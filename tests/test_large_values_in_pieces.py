"""Large values, one mechanism for every engine (backlog P0-PARITY,
`migkit.pieces`), held to what it promises without a server: batches cut
by bytes and not by rows, a cursor fetched a few rows at a time, a value
read and written a piece at a time by its row's key, a wide value hashed
where it lies folding exactly as the value itself does, the warehouses'
digest SQL writing a wide value as its length and md5, and every engine's
cell of the coverage matrix a way or a bound.

The SQL engines' mechanism runs here end to end over a SQLite file through
the same `DbapiRows` every SQL Server, Db2, ASE, DuckDB and warehouse engine
reads and writes by: SQLite has `length`, `substr` and `case`, and the test
gives it an md5 and an append, so each step is the real statement on a real
database, only small (`PIECE` and `BATCH_BYTES` made small for the test).
"""
import hashlib
import math
import sqlite3

import pytest

from migkit import canon, model, pieces
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine
from migkit.engines.dbapi import DbapiRows

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.large-values", "redis"), ("move.sized", "sqlite")]

MB = 2 ** 20


# --- batches cut by bytes --------------------------------------------------

def test_a_batch_is_cut_by_bytes_not_rows():
    small, wide = [b"x" * 10], [b"y" * 100]
    rows = [small] * 5 + [wide] + [small] * 3
    got = list(pieces.by_bytes(rows, most_bytes=40))
    # each small row is 11 bytes: three fit in 40, the wide one goes alone
    assert [len(b) for b in got] == [3, 2, 1, 3], got
    for b in got:
        held = sum(pieces.row_bytes(r) for r in b)
        assert held <= 40 or len(b) == 1, b
    assert [r for b in got for r in b] == rows
    assert [len(b) for b in pieces.by_bytes(rows, 10 ** 6, most_rows=2)] \
        == [2, 2, 2, 2, 1]


def test_a_batch_holds_only_its_own_rows():
    pulled = []

    def rows():
        for i in range(100):
            pulled.append(i)
            yield [b"z" * 30]
    first = next(pieces.by_bytes(rows(), most_bytes=100))
    # three rows of 31 bytes, and the fourth read to know the third ended it
    assert len(first) == 3 and len(pulled) == 4, (len(first), pulled)


def test_a_read_asks_for_as_many_rows_as_its_bytes_hold():
    assert pieces.rows_for(None, 500) == 500
    assert pieces.rows_for(10, 500, 64 * MB) == 500
    assert pieces.rows_for(MB, 500, 64 * MB) == 64
    assert pieces.rows_for(100 * MB, 500, 64 * MB) == 1


class _Cursor:
    def __init__(self, rows):
        self.rows, self.asked = list(rows), []

    def fetchmany(self, n):
        self.asked.append(n)
        got, self.rows = self.rows[:n], self.rows[n:]
        return got


def test_a_cursor_is_fetched_a_few_rows_at_a_time_and_ends_at_its_bytes():
    wide = _Cursor([(b"w" * 30,)] * 20)
    got = pieces.fetch_until(wide, most_bytes=100)
    # 31 bytes a row: the fourth brings the read to 100, and it ends there
    assert len(got) == 4 and wide.asked == [1, 1, 1, 1], wide.asked
    narrow = _Cursor([(b"n",)] * 5000)
    got = pieces.fetch_until(narrow, most_bytes=4000)
    # one first, then as many as a quarter of the bytes hold at their width
    assert narrow.asked[:2] == [1, 500], narrow.asked
    assert sum(pieces.row_bytes(r) for r in got) >= 4000 > sum(
        pieces.row_bytes(r) for r in got[:-1])
    every = _Cursor([(b"w" * 30,)] * 10)
    parts = list(pieces.batches(every, 3, most_bytes=100))
    assert [len(p) for p in parts] == [3, 3, 3, 1]
    assert all(sum(pieces.row_bytes(r) for r in p) <= 100 for p in parts)


def test_a_long_string_is_measured_without_a_copy_of_it():
    text = "é" * (3 * MB)
    assert pieces.row_bytes([text]) == len(text.encode()) + 1
    assert pieces.row_bytes(["abc", None, 7, b"\x00"]) == 3 + 1 + 8 + 1 + 1


# --- a value in pieces -----------------------------------------------------

def test_a_value_is_read_a_piece_at_a_time_and_a_short_one_is_noticed():
    value = bytes(range(256)) * 3
    asked = []

    def fetch(at, n):
        asked.append((at, n))
        return value[at - 1:at - 1 + n]
    got = b"".join(pieces.pieces_of(fetch, len(value), piece=100))
    assert got == value and len(asked) == math.ceil(len(value) / 100)
    assert asked[0] == (1, 100) and asked[-1] == (701, 68)

    def shrinks(at, n):
        return b"" if at > 200 else value[at - 1:at - 1 + n]
    assert len(b"".join(pieces.pieces_of(shrinks, len(value), 100))) == 200


def test_a_value_hashed_where_it_lies_folds_as_the_value_does(monkeypatch):
    monkeypatch.setattr(canon, "WIDE", 8)
    value = b"0123456789abcdef"
    there = pieces.Hashed(len(value), hashlib.md5(value).hexdigest().upper())
    assert canon.digest_value("bytes", there) == canon.digest_value(
        "bytes", value)
    rows = [[1, value], [2, b"ab"]]
    hashed = [[1, there], [2, b"ab"]]
    assert canon.fold_rows(["integer", "bytes"], hashed) == canon.fold_rows(
        ["integer", "bytes"], rows)
    # and it says "different" where the bytes differ
    other = pieces.Hashed(len(value), hashlib.md5(b"x" + value[1:])
                          .hexdigest())
    assert canon.fold_rows(["integer", "bytes"], [[1, other], [2, b"ab"]]) \
        != canon.fold_rows(["integer", "bytes"], rows)
    # a text value by its UTF-8, as every server hashes it
    text = "é" * 10
    assert canon.digest_value("text", pieces.Hashed(
        20, hashlib.md5(text.encode()).hexdigest())) == canon.digest_value(
        "text", text)
    # hashed as its pieces pass, the same
    assert pieces.hashed_pieces([value[:5], value[5:11], value[11:]]) == \
        pieces.Hashed(len(value), hashlib.md5(value).hexdigest())
    # a driver that hands the md5 over as bytes
    assert pieces.Hashed(16, hashlib.md5(value).hexdigest().encode()) == \
        pieces.Hashed(16, hashlib.md5(value).hexdigest())


# --- the SQL engines' mechanism over a real database -----------------------

class _Local(DbapiRows, Engine):
    """A DB-API engine over a SQLite file, its large values declared as a
    SQL engine declares them: `length`, `substr`, and an md5 and an append
    this test gives the database. Every statement sent is recorded."""
    CANON_ENGINE = "sqlite"
    PARAMSTYLE = "qmark"
    LIMIT = "limit"
    LARGE_VALUES = pieces.Sql(
        "a test's", length="length({c})", types=("blob", "text"),
        piece="substr({c}, {at}, {n})",
        append="{c} = migkit_append({c}, {v})", appends=("blob",),
        md5="migkit_md5({c})")

    def __init__(self, path):
        super().__init__(Hop(name="lv", engine="sqlite",
                             source=Endpoint(host=path),
                             target=Endpoint(host=path),
                             databases=["main"]))
        self.path, self.said, self.hook = path, [], None

    def _connect(self, side, db):
        conn = sqlite3.connect(self.path)
        conn.create_function("migkit_append", 2,
                             lambda a, b: bytes(a or b"") + bytes(b))
        conn.create_function("migkit_md5", 1,
                             lambda v: None if v is None
                             else hashlib.md5(bytes(v)).hexdigest())

        def said(sql):
            self.said.append(sql)
            if self.hook:
                self.hook(sql)
        conn.set_trace_callback(said)
        return conn

    def _qualified(self, side, db, table):
        return self._q(table)

    def neutral_tables(self, side, db):
        conn = sqlite3.connect(self.path)
        try:
            return [r[0] for r in conn.execute(
                "select name from sqlite_master where type = 'table'")]
        finally:
            conn.close()

    def neutral_columns(self, side, db, table):
        conn = sqlite3.connect(self.path)
        try:
            return [(r[1], r[2]) for r in conn.execute(
                f'pragma table_info("{table}")')]
        finally:
            conn.close()

    def neutral_key(self, side, db, table):
        conn = sqlite3.connect(self.path)
        try:
            return [r[1] for r in conn.execute(
                f'pragma table_info("{table}")') if r[5]]
        finally:
            conn.close()

    def table_facts(self, side, db):
        return {}


COLS = [("id", "integer"), ("b", "bytes"), ("s", "text")]


@pytest.fixture
def local(tmp_path, monkeypatch):
    monkeypatch.setattr(pieces, "PIECE", 16)
    path = str(tmp_path / "lv.db")
    conn = sqlite3.connect(path)
    for t in ("t", "u"):
        conn.execute(f"create table {t} (id integer primary key, b blob,"
                     " s text)")
    conn.executemany("insert into t values (?, ?, ?)",
                     [(1, bytes(range(100)), "a" * 40),
                      (2, b"0123456789", "short"), (3, None, None)])
    conn.commit()
    conn.close()
    return _Local(path)


def _held(path, table):
    conn = sqlite3.connect(path)
    try:
        return [list(r) for r in conn.execute(
            f"select id, b, s from {table} order by id")]
    finally:
        conn.close()


def test_a_value_past_a_piece_is_read_in_pieces_by_its_key(local):
    rows, last = local.neutral_read("src", "main", "t", COLS)
    assert rows == _held(local.path, "t") and last == (3,)
    first = next(s for s in local.said if s.startswith("SELECT")
                 or s.startswith("select"))
    assert 'case when length("b") <= 16 then "b" end' in first, first
    parts = [s for s in local.said if 'substr("b"' in s]
    # only the 100 bytes of row 1, 16 at a time; row 2's 10 came whole
    assert len(parts) == math.ceil(100 / 16), parts


def test_a_value_changed_under_its_read_is_read_again_whole(local):
    """The source written between two pieces: the value's length is not
    what it was, and it is read again - never handed on as the first
    pieces of one value and the last of another."""
    seen = []

    def write_between(sql):
        if 'substr("b"' in sql:
            seen.append(sql)
            if len(seen) == 2:
                conn = sqlite3.connect(local.path)
                conn.execute("update t set b = ? where id = 1",
                             (b"N" * 50,))
                conn.commit()
                conn.close()
    local.hook = write_between
    rows, _ = local.neutral_read("src", "main", "t", COLS)
    assert rows[0][1] == b"N" * 50, rows[0][1][:20]


def test_a_read_ends_at_its_bytes_and_the_next_goes_on(local, monkeypatch):
    monkeypatch.setattr(pieces, "BATCH_BYTES", 250)
    conn = sqlite3.connect(local.path)
    conn.execute("delete from t")
    conn.executemany("insert into t values (?, ?, ?)",
                     [(i, b"v" * 100, None) for i in range(1, 11)])
    conn.commit()
    conn.close()
    rows, last = local.neutral_read("src", "main", "t", COLS, None, 1000)
    # 111 bytes a row once its value is read: the third brings the read
    # past 250
    assert len(rows) == 3 and last == (3,)
    seen, after = [], None
    while True:
        rows, after = local.neutral_read("src", "main", "t", COLS, after,
                                         1000)
        seen += [r[0] for r in rows]
        if not rows or after is None:
            break
    assert seen == list(range(1, 11))
    batches = list(local.neutral_batches("src", "main", "t", COLS, 1000))
    assert [len(b) for b in batches] == [2, 2, 2, 2, 2]


def test_a_value_past_a_piece_is_written_in_pieces(local):
    rows = _held(local.path, "t")
    assert local.neutral_write("dst", "main", "u", COLS, rows) == 3
    assert _held(local.path, "u") == rows
    added = [s for s in local.said if s.lower().startswith("update")]
    # the first 16 bytes with the insert, the other 84 in six statements
    assert len(added) == math.ceil(100 / 16) - 1, added
    # written again, it converges
    local.neutral_write("dst", "main", "u", COLS, rows)
    assert _held(local.path, "u") == rows


def test_a_digest_hashes_a_wide_value_where_it_lies(local, monkeypatch):
    monkeypatch.setattr(canon, "WIDE", 32)
    rows = _held(local.path, "t")
    want = canon.fold_rows([c for _, c in COLS], rows)
    got = local.neutral_digest("src", "main", "t", COLS)
    assert got == (want[0], str(want[1]))
    assert any('migkit_md5("b")' in s for s in local.said)
    # the md5 crossed, not the value: no piece was read for the digest
    assert not any('substr("b"' in s for s in local.said)
    conn = sqlite3.connect(local.path)
    conn.execute("update t set b = ? where id = 1",
                 (bytes(range(99)) + b"\xff",))
    conn.commit()
    conn.close()
    assert local.neutral_digest("src", "main", "t", COLS) != got


def test_without_an_md5_a_wide_value_is_hashed_as_its_pieces_pass(
        local, monkeypatch):
    monkeypatch.setattr(canon, "WIDE", 32)
    monkeypatch.setattr(local, "LARGE_VALUES", pieces.Sql(
        "a test's", length="length({c})", types=("blob", "text"),
        piece="substr({c}, {at}, {n})"))
    rows = _held(local.path, "t")
    want = canon.fold_rows([c for _, c in COLS], rows)
    assert local.neutral_digest("src", "main", "t", COLS) == (
        want[0], str(want[1]))
    assert len([s for s in local.said if 'substr("b"' in s]) == \
        math.ceil(100 / 16)


def test_rows_are_sized_where_they_lie(local):
    got = local.row_sizes("src", "main", ["t"])["t"]
    # 8 for the id, the blob's and the text's lengths, 1 for the row
    assert got["largest"] == 8 + 100 + 40 + 1
    assert got["rows"] == 3 and got["exact"]
    assert any("select max(s), avg(s), count(*)" in s for s in local.said)
    conn = sqlite3.connect(local.path)
    conn.execute("create table n (id integer primary key, k integer)")
    conn.commit()
    conn.close()
    # no column that can hold a large value: no row of it can be wide
    assert local.row_sizes("src", "main", ["n"])["n"]["largest"] == 17


def test_a_sqlite_file_sizes_its_rows_in_the_file(tmp_path):
    from migkit.engines.sqlite import SQLiteEngine
    path = str(tmp_path / "s.db")
    conn = sqlite3.connect(path)
    conn.execute("create table t (id integer primary key, v blob)")
    conn.executemany("insert into t values (?, ?)",
                     [(1, b"x" * 1000), (2, b"y" * 10)])
    conn.commit()
    conn.close()
    eng = SQLiteEngine(Hop(name="s", engine="sqlite",
                           source=Endpoint(host=path),
                           target=Endpoint(host=path), databases=["main"]))
    got = eng.row_sizes("src", "main")["t"]
    # the id's digits (1), the blob's bytes, one for the row
    assert got["largest"] == 1 + 1000 + 1 and got["exact"]


# --- the warehouses' digest SQL --------------------------------------------

def test_duckdbs_digest_writes_a_wide_value_as_its_length_and_md5(
        monkeypatch):
    import duckdb

    from migkit.engines import warehouse_sql as W
    monkeypatch.setattr(canon, "WIDE", 16)
    cols = [("id", "integer", "BIGINT"), ("s", "text", "VARCHAR"),
            ("b", "bytes", "BLOB")]
    con = duckdb.connect()
    con.execute("create table t (id bigint, s varchar, b blob)")
    con.executemany("insert into t values (?, ?, ?)", [
        (1, "short", b"\x00\x01"), (2, "é" * 9, b"\xff" * 17),
        (3, "x" * 40, None), (4, None, b"0123456789abcdef")])

    def q(n):
        return f'"{n}"'
    served = tuple(con.execute(f"select {W.digest_select('duckdb', cols, q)}"
                               " from t").fetchone())
    back = con.execute("select id, s, b from t").fetchall()
    n, total = canon.fold_rows([c for _, c, _ in cols], back)
    assert (int(served[0]), str(served[1])) == (n, str(total))
    # the rendering held to this process's, row by row, wide values and all
    texts = [r[0] for r in con.execute(
        f"select {W.row_expr('duckdb', cols, q)} from t").fetchall()]
    assert W.agree(cols, back, texts)
    assert any(canon.WIDE_MARK in t for t in texts)
    con.execute("update t set s = ? where id = 3", ("x" * 39 + "y",))
    assert tuple(con.execute(f"select {W.digest_select('duckdb', cols, q)}"
                             " from t").fetchone()) != served


def test_a_binary_its_warehouse_cannot_hash_is_folded_here():
    from migkit.engines import warehouse_sql as W
    # Snowflake's MD5 hashes a string: a BINARY that can hold more than
    # `canon.WIDE` is not rendered in its SQL
    assert W.column_text("snowflake", '"B"', "bytes", "BINARY") is None
    assert W.column_text("snowflake", '"B"', "bytes", "BINARY(8388608)") \
        is None
    assert W.column_text("snowflake", '"B"', "bytes", "BINARY(100)")
    # a VARBYTE is 64,000 bytes where no size is declared
    assert W.column_text("redshift", '"b"', "bytes", "varbyte")
    assert W.column_text("redshift", '"b"', "bytes", "VARBYTE(16777216)") \
        is None
    for dialect in ("bigquery", "snowflake"):
        got = W.column_text(dialect, "c", "text", "STRING" if dialect ==
                            "bigquery" else "VARCHAR")
        assert str(canon.WIDE) in got and "MD5" in got.upper(), got


# --- every engine a way or a bound -----------------------------------------

YES = ("mongodb", "mssql", "redis", "kafka", "sqlite", "parquet",
       "clickhouse", "db2", "db2i", "db2zos", "ase", "opensearch",
       "snowflake", "bigquery", "duckdb", "iceberg", "delta", "hetero")


def test_every_engine_has_its_large_values_way_or_its_bound():
    from migkit import decide as d
    from migkit.engines import NAMES, _class_for
    for n in YES:
        assert d.cell(n, "large values") == ("yes", "@LARGE_VALUES"), n
        assert isinstance(_class_for(n).LARGE_VALUES, pieces.Way), n
    for n, bound in (("dynamodb", "400 KB"), ("cassandra", "16 MB"),
                     ("redshift", "16 MB")):
        state, why = d.cell(n, "large values")
        assert state == d.NOT_APPLICABLE and bound in why, (n, why)
    # the 22 engines that had none: each a way or its bound now, but the
    # generic engine, which moves nothing (backlog item 33)
    had_none = set(YES) | {"dynamodb", "cassandra", "redshift"}
    assert len(had_none) == 21 and had_none <= set(NAMES)
    assert all(d.cell(n, "large values")[0] in ("yes", d.NOT_APPLICABLE)
               for n in had_none)
    assert d.cell("generic", "large values")[0] == d.NOT_YET


def test_an_adapter_taken_away_turns_its_cell_stale(monkeypatch):
    from migkit import decide as d
    from migkit.engines.mssql import MSSQLEngine
    from migkit.engines.redis import RedisEngine
    monkeypatch.delattr(MSSQLEngine, "LARGE_VALUES")
    monkeypatch.delattr(RedisEngine, "LARGE_VALUES")
    assert ("mssql", "large values") in d.uncovered()
    assert ("redis", "large values") in d.uncovered()


# --- memory ------------------------------------------------------------------

class _Wide:
    COPY_MEMORY = {"own": 64 * MB, "per_row": 2.0, "batch": 64 * MB}


def test_a_value_carried_in_pieces_holds_a_piece_of_memory():
    sizes = {"k": {"largest": 512 * MB, "mean": 512 * MB, "exact": True}}
    whole, widest = model.worker_memory(_Wide(), sizes)
    assert whole == 64 * MB + 2 * 512 * MB and widest[1] == 512 * MB

    class _Pieces(_Wide):
        CARRIES_IN_PIECES = 8 * MB
    held, _ = model.worker_memory(_Pieces(), sizes)
    assert held == 64 * MB + 2 * 64 * MB
    m = model.Model(worker_memory=whole, widest=widest)
    assert m.too_wide(4096 * MB) is None
    short = m.too_wide(600 * MB)
    assert short and "nothing is cut short" in short


def test_the_plan_says_how_its_large_values_go(tmp_path, monkeypatch):
    from migkit import config
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    class _Eng:
        LARGE_VALUES = pieces.Way("read and written a piece at a time")
        COPY_MEMORY = {"own": 64 * MB, "per_row": 2.0, "batch": 64 * MB}

        def table_facts(self, side, db):
            return {"t": {"rows": 2, "bytes": 200 * MB}}

        def row_sizes(self, side, db, tables=None):
            return {"t": {"largest": 128 * MB, "mean": 100 * MB,
                          "exact": True}}
    hop = Hop(name="lv", engine="postgres", source=Endpoint(host="x"),
              target=Endpoint(host="x"))
    hop.report_dir = lambda db=None: tmp_path
    m = model.build(hop, _Eng(), "app", probe=False)
    assert "large values: read and written a piece at a time" in m.notes


# --- rows of Arrow, messages, keys, documents ------------------------------

def test_arrow_rows_become_values_a_slice_cut_by_bytes_at_a_time():
    import pyarrow as pa
    batch = pa.record_batch([pa.array([1, 2, 3, 4]),
                             pa.array([b"a" * 10, b"b" * 300, b"c" * 10,
                                       b"d" * 10])], names=["id", "v"])
    got = list(pieces.arrow_rows(batch, ["id", "v"], most_bytes=100))
    assert [len(p) for p in got] == [1, 1, 2]
    assert [r for p in got for r in p] == [
        [1, b"a" * 10], [2, b"b" * 300], [3, b"c" * 10], [4, b"d" * 10]]
    assert pieces.arrow_row_bytes(batch) == [19, 309, 19, 19]


def test_a_message_the_target_refuses_stops_the_copy():
    from migkit.engines.kafka import KafkaEngine

    class _Sent:
        def __init__(self, failed):
            self._failed = failed
            self.exception = "MessageSizeTooLarge" if failed else None

        def failed(self):
            return self._failed
    KafkaEngine._refused("t", 0, [(5, _Sent(False)), (6, _Sent(False))])
    with pytest.raises(SystemExit) as e:
        KafkaEngine._refused("t", 0, [(5, _Sent(False)), (6, _Sent(True))])
    assert "offset 6" in str(e.value) and "refused" in str(e.value)
    got = KafkaEngine._sized_for(100 * MB)
    assert got["max_request_size"] > 100 * MB
    assert got["buffer_memory"] >= 2 * got["max_request_size"]
    assert KafkaEngine._sized_for(None)["max_request_size"] == 16 * MB


class _Redis:
    """The few commands of a Redis a key past a piece is carried and
    compared by, over Python's own values."""

    def __init__(self, data=None):
        self.data, self.asked = dict(data or {}), []

    def _log(self, name):
        self.asked.append(name)

    def memory_usage(self, k):
        v = self.data.get(k)
        return None if v is None else 100 + len(repr(v))

    def strlen(self, k):
        return len(self.data.get(k, b""))

    def getrange(self, k, a, b):
        self._log("getrange")
        return self.data[k][a:b + 1]

    def append(self, k, v):
        self._log("append")
        self.data[k] = self.data.get(k, b"") + v

    def hlen(self, k):
        return len(self.data.get(k, {}))

    def hscan(self, k, cursor, count):
        self._log("hscan")
        items = sorted(self.data[k].items())
        got = dict(items[cursor:cursor + count])
        after = cursor + count
        return (0 if after >= len(items) else after), got

    def hset(self, k, mapping):
        self.data.setdefault(k, {}).update(mapping)

    def hmget(self, k, fields):
        return [self.data.get(k, {}).get(f) for f in fields]

    def llen(self, k):
        return len(self.data.get(k, []))

    def lrange(self, k, a, b):
        return self.data[k][a:b + 1]

    def rpush(self, k, *v):
        self.data.setdefault(k, []).extend(v)


def test_a_redis_key_past_a_piece_is_carried_and_compared_in_pieces(
        monkeypatch):
    from migkit.engines.redis import RedisEngine
    monkeypatch.setattr(pieces, "PIECE", 16)
    value = bytes(range(100))
    src = _Redis({b"s": value, b"h": {f"f{i}".encode(): b"v" * i
                                      for i in range(40)},
                  b"l": [b"x%d" % i for i in range(50)]})
    dst = _Redis()
    for key, kind in ((b"s", "string"), (b"h", "hash"), (b"l", "list")):
        RedisEngine._pieces_of(src, dst, key, kind)
        assert dst.data[key] == src.data[key], key
        assert RedisEngine._compare_in_pieces(src, dst, key, kind)
    assert src.asked.count("getrange") == math.ceil(100 / 16)
    assert src.asked.count("hscan") > 1
    # and says "different" where a piece differs
    dst.data[b"s"] = value[:50] + b"!" + value[51:]
    assert not RedisEngine._compare_in_pieces(src, dst, b"s", "string")
    dst.data[b"h"][b"f7"] = b"changed"
    assert not RedisEngine._compare_in_pieces(src, dst, b"h", "hash")
    dst.data[b"l"][30] = b"moved"
    assert not RedisEngine._compare_in_pieces(src, dst, b"l", "list")


def test_a_mongodb_read_ends_at_its_bytes_of_bson(monkeypatch):
    import bson
    from bson.codec_options import CodecOptions
    from bson.raw_bson import RawBSONDocument

    from migkit.engines.mongodb import MongoEngine
    monkeypatch.setattr(pieces, "BATCH_BYTES", 250)
    docs = [{"_id": i, "v": "x" * 80} for i in range(10)]

    class _Found:
        def __init__(self, raw):
            self.raw, self.closed, self.pulled = raw, False, 0

        def sort(self, *a):
            return self

        def limit(self, n):
            self.raw = self.raw[:n]
            return self

        def __iter__(self):
            for d in self.raw:
                self.pulled += 1
                yield d

        def close(self):
            self.closed = True

    class _Coll:
        codec_options = CodecOptions()

        def with_options(self, codec_options):
            assert codec_options.document_class is RawBSONDocument
            return self

        def find(self, flt, projection):
            self.found = _Found([RawBSONDocument(bson.encode(d))
                                 for d in docs])
            return self.found
    coll = _Coll()
    got = MongoEngine._docs_until(None, coll, {}, None, 1000)
    size = len(bson.encode(docs[0]))
    assert len(got) == math.ceil(250 / size) and coll.found.closed
    assert got == docs[:len(got)] and isinstance(got[0], dict)
    raw = MongoEngine._docs_until(None, coll, {}, None, 2, raw=True)
    assert len(raw) == 2 and isinstance(raw[0], RawBSONDocument)
