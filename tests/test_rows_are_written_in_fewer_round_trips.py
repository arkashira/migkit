"""PostgreSQL's applier: a batch's statements piped, and the runs long
enough to stage copied binary (backlog R19, lever 10).

Measured before: every statement a batch wrote waited for its answer
before the next was sent - on a link with a round trip of R, N statements
took N x R whatever they held - and a staged run went by COPY's text,
which the server reads back into each value. Now the statements are sent
without waiting and their answers read once, where the batch ends, and a
staged run goes binary where every column and value is of a kind sent
exactly; the rest go as before. What each lands is held to what the old
path landed and to the rows given.
"""
import datetime
import decimal
import time
import uuid

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql
from tests.slowlink import SlowLink

pytestmark = needs_docker


def _one(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg(port):
    """One lane: a batch applied side by side splits a run among lanes
    (`test_postgres_sends_a_statement_a_run_not_a_row` has those)."""
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=port, user="postgres",
                  password="test")
    return PostgresEngine(Hop(name="w", engine="postgres", source=ep,
                              target=ep, databases=["postgres"],
                              workers=1))


TYPES = """
drop table if exists public.w, public.side;
create table public.w (id int8 primary key, i4 int4, i2 int2,
    n numeric(30,6), f8 float8, ok bool, t text, vc varchar(30), ch char(5),
    raw bytea, jb jsonb, js json, d date, ts timestamp(6),
    tz timestamptz(6), tm time(6), u uuid);
create table public.side (id int primary key, v text unique);
"""

TZ = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def _row(i):
    """A row of every kind the binary path takes, with the values the old
    bug turned: bytes (empty too), empty strings, NULLs, text of several
    bytes a character, and text that reads as an escape."""
    texts = ["", None, "é😀 ภาษาไทย", "tab\there\nnl\\bs", "\\N", "NULL",
             "'quoted'", "plain"]
    return {
        "id": i, "i4": (-1) ** i * i * 1000, "i2": i % 32767 - 16000,
        "n": [decimal.Decimal("-0.000"), decimal.Decimal("1E+3"),
              decimal.Decimal(i) / 7, None][i % 4],
        "f8": [0.1, -0.0, 1.2345678901234567, 1e300, float("inf"), None,
               float(i) / 3][i % 7],
        "ok": [True, False, None][i % 3],
        "t": texts[i % len(texts)], "vc": texts[(i + 3) % len(texts)],
        "ch": ["", "ab", "abcde", None][i % 4],
        "raw": [b"", bytes([i % 256, 0, 9, 10, 92, 78]), None,
                b"\\x00"][i % 4],
        "jb": [None, '{"k": %d, "s": "é\\t"}' % i, "[]", '"str"', "1.50",
               '{"b": 1, "a": [null, true]}'][i % 6],
        "js": [None, '{"b": 1,  "a": 2}', '"é😀"'][i % 3],
        "d": [datetime.date(2026, 9, 28), None,
              datetime.date(1, 1, 1)][i % 3],
        "ts": [datetime.datetime(2026, 9, 28, 1, 2, 3, i % 1000000), None,
               datetime.datetime(1999, 12, 31, 23, 59, 59, 999999)][i % 3],
        "tz": [datetime.datetime(2026, 9, 28, 1, 2, 3, 7, tzinfo=TZ),
               datetime.datetime(2026, 3, 29, 1, 30,
                                 tzinfo=datetime.timezone.utc), None][i % 3],
        "tm": [datetime.time(23, 59, 59, 999999), None,
               datetime.time(0, 0)][i % 3],
        "u": [uuid.UUID(int=i), None][i % 2],
    }


def _batch(n):
    """Inserts and then updates of `w`, which the batch writes as one
    staged run, and rows of a table with a second unique index, written
    in the batch's order as statements - piped, and sent before the
    staged run's COPY."""
    out = [{"op": "insert", "table": "public.side", "key": {"id": i},
            "values": {"id": i, "v": f"s{i}"}} for i in range(5)]
    out += [{"op": "insert", "table": "public.w", "key": {"id": i},
             "values": _row(i)} for i in range(1, n + 1)]
    out += [{"op": "update", "table": "public.w", "key": {"id": i},
             "values": {**_row(i + 1000), "id": i}}
            for i in range(1, n, 2)]
    return out


DIGEST = ("select md5(string_agg(t::text, '|' order by id)), count(*)"
          " from {} t")


def _way(monkeypatch, way):
    """Every run long enough staged, its COPY by `way`, or none staged
    ("statements")."""
    from migkit.engines.postgres import PostgresEngine
    if way == "statements":
        monkeypatch.setattr(PostgresEngine, "STAGE_FROM", 10 ** 9)
        return
    real = PostgresEngine._cheaper
    monkeypatch.setattr(
        PostgresEngine, "_cheaper",
        lambda self, key, ways: {"stage": way, "upserts": "staged"}.get(
            key[0]) or real(self, key, ways))


def test_binary_text_and_statements_land_the_same(pg_pair, monkeypatch):
    from migkit.engines import postgres
    port = pg_pair["dst"]
    packed, texts, got = [], [], {}
    real_bin, real_text = postgres._copy_binary, postgres._copy_text
    monkeypatch.setattr(postgres, "_copy_binary", lambda *a: (
        lambda d: packed.append(d is not None) or d)(real_bin(*a)))
    monkeypatch.setattr(postgres, "_copy_text", lambda rows: (
        texts.append(len(rows)) or real_text(rows)))
    batch = _batch(1500)
    for way in ("statements", "text", "binary"):
        _one(port, TYPES)
        with monkeypatch.context() as m:
            _way(m, way)
            eng = _pg(port)
            eng.neutral_apply("dst", "postgres", batch)
            # a replay changes nothing
            eng.neutral_apply("dst", "postgres", batch)
        got[way] = [_one(port, DIGEST.format(t))
                    for t in ("public.w", "public.side")]
    assert got["binary"] == got["text"] == got["statements"], got
    assert got["binary"][0].endswith("|1500"), got
    # the staged run of the binary pass went binary, of the text pass as
    # text: once each time the batch was applied
    assert packed == [True] * 2, packed
    assert texts == [1500] * 2, texts
    # and what landed is what was given, not only the same both ways
    rows = [_row(i) for i in range(1, 1501)]
    for i in range(1, 1500, 2):
        rows[i - 1] = {**_row(i + 1000), "id": i}
    want = (sum(r["t"] == "" for r in rows), sum(r["t"] is None
                                                  for r in rows),
            sum(r["raw"] == b"" for r in rows),
            sum(r["raw"] == b"\\x00" for r in rows),
            sum(r["t"] == "é😀 ภาษาไทย" for r in rows),
            sum(r["n"] is not None and r["n"] == 0 for r in rows))
    assert _one(port, "select count(*) filter (where t = ''),"
                      " count(*) filter (where t is null),"
                      " count(*) filter (where raw = ''::bytea),"
                      " count(*) filter (where raw = '\\x5c783030'),"
                      " count(*) filter (where t = 'é😀 ภาษาไทย'),"
                      " count(*) filter (where n = 0)"
                      " from public.w") == "|".join(map(str, want))
    every = ("select i4, i2, n, f8, ok, t, vc, ch, raw, jb::text, js::text,"
             " d, ts, tz, tm, u from public.w where id = {}")
    assert _one(port, every.format(4)) == (
        "4000|-15996|0.000000|Infinity|f|\\N|plain|     |\\x|1.50|"
        '{"b": 1,  "a": 2}|||2026-03-29 01:30:00+00||'
        + str(uuid.UUID(int=4)))
    assert _one(port, every.format(2)) == (
        "2000|-15998|0.285714|1.2345678901234567||é😀 ภาษาไทย|NULL|abcde||"
        '[]|"é😀"|0001-01-01|1999-12-31 23:59:59.999999||00:00:00|'
        + str(uuid.UUID(int=2)))
    # odd rows hold what the update gave them
    assert _one(port, "select i4, jb::text, tz, t = E'tab\\there\\nnl\\\\bs'"
                      " from public.w where id = 1011") == \
        '-2011000|{"k": 2011, "s": "é\\t"}|2026-03-29 01:30:00+00|t'
    assert _one(port, "select jb::text, tz from public.w where id = 6") == \
        "|2026-09-27 19:32:03.000007+00"


def test_a_column_binary_does_not_carry_goes_as_text(pg_pair, monkeypatch):
    """`real`, `interval` and a float into `numeric` are not sent binary:
    the run goes as text, lands as the statements land it, and the cost
    recorded is text's."""
    from migkit.engines import postgres
    port = pg_pair["dst"]
    got = {}
    rows = [{"op": "insert", "table": "public.o", "key": {"id": i},
             "values": {"id": i, "r": i / 3,
                        "iv": f"{i} days 00:00:00.000007", "n": i / 7}}
            for i in range(1200)]
    for way in ("statements", "binary"):
        _one(port, "drop table if exists public.o; create table public.o"
                   " (id int primary key, r real, iv interval, n numeric)")
        with monkeypatch.context() as m:
            _way(m, way)
            eng = _pg(port)
            eng.neutral_apply("dst", "postgres", rows)
        got[way] = _one(port, DIGEST.format("public.o"))
        if way == "binary":
            took = [w for w in eng.measured_ways() if w["work"] == "stage"]
            assert [list(w["seconds_a_row"]) for w in took] == [["text"]], \
                took
    assert got["binary"] == got["statements"], got
    assert postgres._copy_binary(None, ["float4"], [[1.5]]) is None


def test_arrays_land_as_the_old_writer_landed_them(pg_pair):
    """An array column is given its list, which neither COPY here writes:
    the run goes as statements, each list put in as the other driver put
    it, whatever it holds - empty, a NULL element, text with the array's
    own separators and quotes, several bytes a character - and a list into
    a `jsonb` column is the JSON array."""
    import psycopg2
    from psycopg2.extras import execute_values

    from migkit import canon
    port = pg_pair["dst"]
    shape = "(id int primary key, ia int4[], ta text[], ba int8[], j jsonb)"
    _one(port, f"drop table if exists public.arr, public.old;"
               f" create table public.arr {shape};"
               f" create table public.old {shape}")
    texts = ["a,b", 'q"t', "{x}", "é😀", "", None, "NULL", "back\\slash"]

    def row(i):
        return {"id": i, "ia": [[1, None, -3], [], None, [i]][i % 4],
                "ta": [texts, [], None, texts[i % 8:]][i % 4],
                "ba": [[1 << 40, -(1 << 62)], None][i % 2],
                "j": [[1, "x", None], {"k": [i]}, None][i % 3]}
    rows = [row(i) for i in range(1, 1201)]
    for n in (40, 1200):
        _one(port, "truncate public.arr, public.old")
        _pg(port).neutral_apply("dst", "postgres", [
            {"op": "insert", "table": "public.arr", "key": {"id": r["id"]},
             "values": r} for r in rows[:n]])
        names = sorted(rows[0])
        conn = psycopg2.connect(host="127.0.0.1", port=port,
                                user="postgres", password="test",
                                dbname="postgres")
        try:
            with conn.cursor() as cur:
                execute_values(cur, f"insert into public.old"
                                    f" ({', '.join(names)}) values %s",
                               [[r[c] if c in ("ia", "ta", "ba")
                                 else canon.sql_value(r[c]) for c in names]
                                for r in rows[:n]])
            conn.commit()
        finally:
            conn.close()
        got = [_one(port, DIGEST.format(t)) for t in ("public.arr",
                                                      "public.old")]
        assert got[0] == got[1] and got[0].endswith(f"|{n}"), got
    each = "select ia, ta, j::text from public.arr where id = {}"
    assert _one(port, each.format(4)) == (
        '{1,NULL,-3}|{"a,b","q\\"t","{x}",é😀,"",NULL,"NULL",'
        '"back\\\\slash"}|{"k": [4]}')
    assert _one(port, each.format(1)) == '{}|{}|{"k": [1]}'
    assert _one(port, each.format(3)) == \
        '{3}|{é😀,"",NULL,"NULL","back\\\\slash"}|[1, "x", null]'


def test_a_statement_of_more_than_65535_values_lands(pg_pair, monkeypatch):
    """Values bound apart from the statement stop at 65,535 of them; these
    are put into it, so a run of 990 rows of 71 columns - 70,290 values -
    is still one statement."""
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["dst"]
    cols = [f"c{j}" for j in range(70)]
    _one(port, "drop table if exists public.many; create table public.many"
               " (id int primary key, "
               + ", ".join(f"{c} int" for c in cols) + ")")
    sent, real = [], PostgresEngine._in_statements

    def counted(self, conn, head, tail, values):
        sent.append(len(values) * len(values[0]))
        return real(self, conn, head, tail, values)
    monkeypatch.setattr(PostgresEngine, "_in_statements", counted)
    _pg(port).neutral_apply("dst", "postgres", [
        {"op": "insert", "table": "public.many", "key": {"id": i},
         "values": {"id": i, **{c: i * j for j, c in enumerate(cols)}}}
        for i in range(990)])
    assert sent == [70290], sent
    assert _one(port, "select count(*), sum(c69) from public.many") == \
        f"990|{sum(i * 69 for i in range(990))}"


def test_a_batch_waits_for_the_server_once(pg_pair, monkeypatch):
    """Through a link 25 ms each way: 120 changes to a parent and its
    child in turn - a statement each, in the order the batch holds them,
    as a key between two tables asks - took a round trip each before;
    piped, the batch waits once for all of them. The bounds are the
    link's, not the machine's: 120 round trips cannot take less than 6s,
    and the piped batch's allowance is 30 round trips."""
    from migkit.engines.base import Engine
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["dst"]
    batch = [{"op": "insert", "table": f"public.{t}", "key": {"id": i},
              "values": {"id": i, "v": f"{t}{i}"}}
             for i in range(60) for t in ("a", "b")]
    took, got = {}, {}
    with SlowLink(port, 0.025) as link:
        for how in ("each waited", "piped"):
            _one(port, "drop table if exists public.b, public.a;"
                       " create table public.a (id int primary key, v text);"
                       " create table public.b (id int primary key"
                       " references public.a, v text)")
            with monkeypatch.context() as m:
                if how == "each waited":
                    m.setattr(PostgresEngine, "_apply_net",
                              Engine._apply_net)
                eng = _pg(link.port)
                # what the engine asks of a table once, asked first: the
                # time is the batch's
                eng._ordered_tables("dst", "postgres")
                for t in ("public.a", "public.b"):
                    eng._write_rules("dst", "postgres", t)
                began = time.monotonic()
                eng.neutral_apply("dst", "postgres", batch)
                took[how] = time.monotonic() - began
            got[how] = [_one(port, DIGEST.format(t))
                        for t in ("public.a", "public.b")]
    assert got["piped"] == got["each waited"], got
    assert got["piped"][0].endswith("|60"), got
    assert took["each waited"] >= 120 * 0.05, took
    assert took["piped"] < 30 * 0.05, took


def test_a_statement_that_fails_in_a_piped_batch_fails_the_batch(pg_pair):
    """The error is read where the batch ends, not where the statement was
    sent; it is the statement's own, and nothing of the batch lands -
    neither what was sent before it nor after."""
    import psycopg
    port = pg_pair["dst"]
    _one(port, "drop table if exists public.a, public.b;"
               " create table public.a (id int primary key, v text not null);"
               " create table public.b (id int primary key, v text)")
    batch = [{"op": "insert", "table": "public.a", "key": {"id": 1},
              "values": {"id": 1, "v": "x"}},
             {"op": "insert", "table": "public.b", "key": {"id": 1},
              "values": {"id": 1, "v": "y"}},
             {"op": "insert", "table": "public.a", "key": {"id": 2},
              "values": {"id": 2, "v": None}},
             {"op": "insert", "table": "public.b", "key": {"id": 2},
              "values": {"id": 2, "v": "z"}}]
    # and a staged run after it: the stage is not reached
    batch += [{"op": "insert", "table": "public.b", "key": {"id": i},
               "values": {"id": i, "v": "s"}} for i in range(10, 1100)]
    with pytest.raises(psycopg.errors.NotNullViolation):
        _pg(port).neutral_apply("dst", "postgres", batch)
    assert _one(port, "select (select count(*) from public.a)"
                      " + (select count(*) from public.b)") == "0"


def test_counters_move_where_they_stand_in_one_statement_a_row(pg_pair,
                                                               monkeypatch):
    from migkit import canon
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["dst"]
    _one(port, "drop table if exists public.bal; create table public.bal"
               " (id int primary key, n bigint, note text);"
               " insert into public.bal values (1, 100, 'old')")
    asked = []
    real = PostgresEngine._added_row

    def counted(self, *a):
        got = real(self, *a)
        asked.append(got[0].split(" on conflict")[1])
        return got
    monkeypatch.setattr(PostgresEngine, "_added_row", counted)
    _pg(port).neutral_apply("dst", "postgres", [
        {"op": "update", "table": "public.bal", "key": {"id": i},
         "values": {"id": i, "n": canon.Added(5, 5 + i), "note": "new"}}
        for i in (1, 2)])
    # row 1 moved where it stood, row 2 made with what the source ended at
    assert _one(port, "select string_agg(id || ':' || n || ':' || note, ','"
                      " order by id) from public.bal") == \
        "1:105:new,2:7:new"
    assert asked == [' ("id") do update set "n" = t0."n" + %s,'
                     ' "note" = excluded."note"'] * 2, asked
