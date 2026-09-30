"""MySQL written through the server's bulk load, pinned to the rows migkit
prepared (backlog R19, lever 6): the copier's batches straight into the
table, the applier's runs of upserts into a stage and on by one
`insert ... select ... on duplicate key update` - each where the server
takes a load (`local_infile`, never set by migkit) and where it was
measured cheaper than the statements on that server.

What each lands is held to what the statements land and to the rows
given: bytes (empty too), NULL, empty strings, text of several bytes a
character and text that reads as the load's own escapes.
"""
import datetime
import decimal
import json
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MY, PORT = "migkit-test-wp-my", 15991


def my(sql, db="cx"):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B", db], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-p",
                    f"{PORT}:3306", "-e", "MYSQL_ROOT_PASSWORD=test",
                    "-e", "MYSQL_DATABASE=cx", "mysql:8.4",
                    "--local-infile=1"], check=True, capture_output=True)
    try:
        for _ in range(90):
            # over TCP, and the database made: the image's first start
            # answers on its socket before either
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "use cx"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _engine():
    """One lane, so a run is not split among lanes side by side."""
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    return MySQLEngine(Hop(name="wp", engine="mysql", source=ep, target=ep,
                           databases=["cx"], workers=1))


TABLE = """drop table if exists w;
create table w (id bigint primary key, raw varbinary(64), blb blob,
 note varchar(40), empt varchar(10) not null, mb text, amount decimal(20,6),
 at datetime(6), js json, yr year)"""

COLUMNS = [("id", "integer"), ("raw", "bytes"), ("blb", "bytes"),
           ("note", "text"), ("empt", "text"), ("mb", "text"),
           ("amount", "decimal"), ("at", "timestamp"), ("js", "json"),
           ("yr", "integer")]


def _row(i):
    return (i,
            [b"", bytes([i % 256, 0, 9, 10, 92, 78]), None, b"\\N"][i % 4],
            [bytes(range(256)) * 2, b"", None][i % 3],
            [None, "", "tab\there\nnl\\bs", "é😀 ภาษาไทย", "\\N",
             "NULL"][i % 6],
            ["", "x"][i % 2],
            [f"é😀 ภาษาไทย {i}", "", "日本語", None][i % 4],
            (decimal.Decimal(i) / -7).quantize(decimal.Decimal("0.000001")),
            [datetime.datetime(2026, 9, 28, 1, 2, 3, i % 1000000),
             None][i % 2],
            ['{"k": %d, "s": "é\\t"}' % i, None, "[]"][i % 3],
            [0, 1999, 2155, None][i % 4])


def _read(table="w"):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=PORT, user="root",
                           password="test", database="cx",
                           charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            cur.execute(f"select * from {table} order by 1")
            rows = [list(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def _as_given(rows):
    """What the server hands back for the rows `_row` gives: JSON in its
    own spelling, compared as the value it is."""
    out = []
    for r in rows:
        r = list(r)
        r[8] = None if r[8] is None else json.loads(r[8])
        out.append(r)
    return out


def _way(monkeypatch, way):
    """Every choice of a way pinned to `way` ("load"/"staged" or
    "statements")."""
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "_cheaper",
                        lambda self, key, ways: "statements"
                        if way == "statements" else ways[0].name)


def _loads(monkeypatch):
    from migkit.engines import mysql
    real, used = mysql._load, []

    def counted(*a, **k):
        got = real(*a, **k)
        used.append(got)
        return got
    monkeypatch.setattr(mysql, "_load", counted)
    return used


def test_the_load_writes_what_the_statements_write(server, monkeypatch):
    """The copier's writer: 5,000 rows in batches of 1,000 by the load
    and by the statements, read back equal to each other and to the rows
    given."""
    rows = [_row(i) for i in range(1, 5001)]
    got = {}
    for way in ("statements", "load"):
        my(TABLE)
        with monkeypatch.context() as m:
            _way(m, way)
            used = _loads(m)
            eng = _engine()
            for at in range(0, len(rows), 1000):
                eng.neutral_write("dst", "cx", "w", COLUMNS,
                                  rows[at:at + 1000])
        assert used == ([1000] * 5 if way == "load" else []), used
        got[way] = _read()
    assert got["load"] == got["statements"]
    assert _as_given(got["load"]) == _as_given(rows)
    # a YEAR of 0 is 0000, not the 2000 the text "0" loads as
    assert my("select count(*) from w where yr = 0 and id % 4 = 0") == "1250"


def _upserts(n, base=0):
    out = [{"op": "insert", "table": "w", "key": {"id": base + i},
            "values": dict(zip([c for c, _ in COLUMNS], _row(base + i)))}
           for i in range(1, n + 1)]
    out += [{"op": "update", "table": "w", "key": {"id": base + i},
             "values": {"note": "changed", "raw": b"\x00changed"}}
            for i in range(1, n + 1, 3)]
    return out


def test_a_run_of_upserts_is_staged_as_the_statements_write_it(
        server, monkeypatch):
    """The applier's runs: inserts over rows already there, replayed, and
    a table whose columns are all its key - by the stage and by the
    statements, read back equal, and to the rows given."""
    from migkit.engines.mysql import MySQLEngine
    batch = _upserts(3000)
    link = [{"op": "insert", "table": "link", "key": {"a": 1, "b": i},
             "values": {"a": 1, "b": i}} for i in range(1500)]
    got = {}
    for way in ("statements", "staged"):
        my(TABLE + "; drop table if exists link;"
           " create table link (a int, b int, primary key (a, b))")
        # half the rows there before, holding other values
        my("insert into w (id, empt, note) select id, 'old', 'old' from"
           " (select 1 as id union all select 2 union all select 3000) x")
        staged, real = [], MySQLEngine._staged
        with monkeypatch.context() as m:
            _way(m, way)
            m.setattr(MySQLEngine, "_staged", lambda self, *a: staged.append(
                real(self, *a)) or staged[-1])
            eng = _engine()
            for _ in range(2):
                eng.neutral_apply("dst", "cx", batch)
                eng.neutral_apply("dst", "cx", link)
        assert staged == ([True] * 4 if way == "staged" else []), staged
        got[way] = (_read(), _read("link"))
    assert got["staged"] == got["statements"]
    want = [list(_row(i)) for i in range(1, 3001)]
    for r in want[::3]:
        r[1], r[3] = b"\x00changed", "changed"
    assert _as_given(got["staged"][0]) == _as_given(want)
    assert got["staged"][1] == [[1, i] for i in range(1500)]


def test_the_cheaper_way_is_taken_once_both_are_measured(server,
                                                         monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    my(TABLE)
    timed, real = [], MySQLEngine._took
    monkeypatch.setattr(MySQLEngine, "_took",
                        lambda self, key, way, s, n: timed.append(
                            (key, way)) or real(self, key, way, s, n))
    eng = _engine()
    for k in range(3):
        eng.neutral_apply("dst", "cx", _upserts(1100, base=10000 * k)[:1100])
    key = ("upserts", "cx.w", 1024)
    assert [t for t in timed if t[0] == key][:2] == [(key, "staged"),
                                                    (key, "statements")]
    seen = [w for w in eng.measured_ways() if w["work"] == "upserts"]
    assert len(seen) == 1 and set(seen[0]["seconds_a_row"]) == \
        {"staged", "statements"}, seen
    costs = seen[0]["seconds_a_row"]
    # the third run went the way that was cheaper after the first two
    assert timed[-1] == (key, seen[0]["chosen"])
    assert seen[0]["chosen"] == min(costs, key=costs.get)
    assert seen[0]["table"] == "cx.w" and seen[0]["rows_from"] == 1024
    assert my("select count(*) from w") == "3300"


def test_a_server_that_takes_no_load_is_written_by_statements(
        server, monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    my(TABLE)
    my("set global local_infile = 0")
    try:
        called = []
        monkeypatch.setattr(MySQLEngine, "_staged",
                            lambda self, *a: called.append(1))
        used = _loads(monkeypatch)
        eng = _engine()
        eng.neutral_apply("dst", "cx", _upserts(1200)[:1200])
        eng.neutral_write("dst", "cx", "w", COLUMNS,
                          [_row(i) for i in range(5000, 6000)])
        assert called == [] and used == []
        assert eng.measured_ways() == []
        assert my("select count(*) from w") == "2200"
    finally:
        my("set global local_infile = 1")


def test_a_value_the_stage_would_change_stops_as_a_statement_stops(
        server, monkeypatch):
    """A load takes a value too long for its column with a warning and
    goes on: the stage is emptied, and the run is switched to statements
    by itself and said, which refuse it - nothing of the run lands."""
    import pymysql

    from migkit import decide
    from migkit.engines.mysql import MySQLEngine
    my("drop table if exists s; create table s (id int primary key,"
       " v varchar(3))")
    staged, real = [], MySQLEngine._staged

    def counted(self, *a):
        try:
            got = real(self, *a)
        except decide.Blocked as e:
            staged.append(str(e))
            raise
        staged.append(got)
        return got
    monkeypatch.setattr(MySQLEngine, "_staged", counted)
    _way(monkeypatch, "staged")
    rows = [{"op": "insert", "table": "s", "key": {"id": i},
             "values": {"id": i, "v": "abcdef" if i == 700 else "ab"}}
            for i in range(1100)]
    said = []
    eng = _engine()
    eng.lend_log(said.append)
    with pytest.raises(pymysql.err.DataError, match="Data too long"):
        eng.neutral_apply("dst", "cx", rows)
    assert len(staged) == 1 and staged[0].startswith(
        "the bulk load was not taken as it was ("), staged
    assert [m for m in said if "switching to the rows as multi-row"
            " upserts" in m and "could not run (the bulk load was not"
            " taken as it was" in m], said
    assert my("select count(*) from s") == "0"


def test_a_load_with_more_warnings_than_its_answer_counts_is_refused(
        server):
    """The load's own answer carries its warnings in two bytes: 70,000
    of them are counted as 65,535, never as 0."""
    from migkit.engines import mysql
    my("drop table if exists t1; create table t1 (v varchar(1))")
    eng = _engine()
    conn = eng._conn("dst")
    try:
        with conn.cursor() as cur:
            with pytest.raises(mysql._LoadRefused,
                               match="70,000 of 70,000 rows taken, 65,535"
                                     " warnings"):
                mysql._load(cur, "cx", "t1", ["v"], [("ab",)] * 70000,
                            {"v": "varchar"})
    finally:
        conn.rollback()
        conn.close()


def test_counters_move_where_they_stand_in_one_statement_a_row(
        server, monkeypatch):
    from migkit import canon
    from migkit.engines.mysql import MySQLEngine
    my("drop table if exists bal; create table bal (id int primary key,"
       " n bigint, note varchar(10)); insert into bal values (1, 100, 'old')")
    asked, real = [], MySQLEngine._added_row
    monkeypatch.setattr(MySQLEngine, "_added_row",
                        lambda self, *a: asked.append(1) or real(self, *a))
    _engine().neutral_apply("dst", "cx", [
        {"op": "update", "table": "bal", "key": {"id": i},
         "values": {"id": i, "n": canon.Added(5, 5 + i), "note": "new"}}
        for i in (1, 2)])
    assert my("select group_concat(concat_ws(':', id, n, note) order by"
              " id) from bal") == "1:105:new,2:7:new"
    assert asked == [1, 1]
