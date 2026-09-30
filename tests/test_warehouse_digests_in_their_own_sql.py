"""The digest a warehouse answers in its own SQL is the number migkit folds
here (`canon.fold_rows`), and is shown able to say "different".

DuckDB runs in this process, so its rendering is run here on rows no
engine chose: every class, and floats from random bit patterns across the
whole double range, including the bands where the text changes form. The
other dialects' SQL is run in `test_warehouse_exact_loads_run.py`
(BigQuery's emulator; PostgreSQL where Redshift shares its SQL); here the
engines' choice between their SQL and the fold is pinned.
"""
import datetime
import random
import struct
from decimal import Decimal

import pytest

from migkit import canon, rowtext
from migkit.config import Endpoint, Hop
from migkit.engines import warehouse_sql as W

COLS = [("id", "integer", "BIGINT"), ("f", "float", "DOUBLE"),
        ("d", "decimal", "DECIMAL(12,2)"), ("s", "text", "VARCHAR"),
        ("b", "bytes", "BLOB"), ("dt", "date", "DATE"),
        ("ts", "timestamp", "TIMESTAMP"),
        ("tz", "timestamp", "TIMESTAMP WITH TIME ZONE"),
        ("tm", "time", "TIME"), ("ok", "boolean", "BOOLEAN")]

EDGES = [0.0, -0.0, 0.1, 1e20, 1e45, 1e44, -1e45, 1.5e-7, 5e-324, 1e-20,
         9.99e-21, 1.2345678901234567e-15, 0.000012345678901234565,
         -0.000012345678901234565, 1.2345678901234566e17, 1e18, 123.0,
         -7.25, 1.7976931348623157e308, float("nan"), float("inf"),
         float("-inf"), 2.5e-5, 1e16, 3.3333333333333335e30, 1e-4]


def _q(n):
    return '"' + n + '"'


def _table(rows):
    import duckdb
    con = duckdb.connect(config={"TimeZone": "UTC"})
    con.execute("create table t (id bigint, f double, d decimal(12,2),"
                " s varchar, b blob, dt date, ts timestamp, tz timestamptz,"
                " tm time, ok boolean)")
    con.executemany("insert into t values (?,?,?,?,?,?,?,?,?,?)", rows)
    return con


def _rows(n, seed=7):
    rnd = random.Random(seed)
    floats = list(EDGES)
    while len(floats) < n:
        floats.append(struct.unpack("d", struct.pack(
            "Q", rnd.getrandbits(64)))[0])
        floats.append(rnd.uniform(-1e6, 1e6) * 10 ** rnd.randint(-30, 50))
    zone = datetime.timezone(datetime.timedelta(hours=7))
    out = []
    for i, f in enumerate(floats[:n]):
        out.append((
            i, f, Decimal(rnd.randint(-10 ** 6, 10 ** 6)).scaleb(-2)
            if i % 5 else None,
            rnd.choice(["x|y", "é", "", None, "a:b", "N:", "3:abc"]),
            rnd.choice([b"\x00\xff", b"", None]),
            datetime.date(2024, 1, 2) if i % 3 else None,
            datetime.datetime(2024, 1, 2, 3, 4, 5, i % 999999),
            datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=zone),
            datetime.time(3, 4, 5, i % 1000000),
            bool(i % 2) if i % 7 else None))
    return out


def _digest(con):
    return tuple(con.execute(f"select {W.digest_select('duckdb', COLS, _q)}"
                             " from t").fetchone())


def _fold(con):
    back = con.execute("select " + ", ".join(_q(n) for n, _, _ in COLS)
                       + " from t").fetchall()
    n, total = canon.fold_rows([c for _, c, _ in COLS], back)
    return n, str(total)


def test_duckdbs_rendering_is_migkits_on_every_row():
    """6,000 rows: each row's text rendered by DuckDB's SQL is the text this
    process renders from the value DuckDB hands back, and the sums agree.
    Measured on this machine: DuckDB 1.5.5, 6,000 rows, no row apart."""
    con = _table(_rows(6000))
    assert _digest(con) == _fold(con)
    text = W.row_expr("duckdb", COLS, _q)
    apart = []
    for row in con.execute("select " + ", ".join(_q(n) for n, _, _ in COLS)
                           + f", {text} from t").fetchall():
        mine = rowtext.encode([canon.render_value(c, v) for (_, c, _), v
                               in zip(COLS, row[:-1])])
        if mine != row[-1]:
            apart.append((row[0], row[1], row[-1][:80], mine[:80]))
    assert not apart, apart[:3]


@pytest.mark.parametrize("column, value", [
    ("f", 0.30000000000000004), ("f", 1.0000000000000002e30),
    ("f", 1e-21), ("d", Decimal("1.51")), ("s", "x|z"), ("s", None),
    ("b", b"\x00\xfe"), ("dt", datetime.date(2024, 1, 3)),
    ("ts", datetime.datetime(2024, 1, 2, 3, 4, 5, 1)),
    ("tz", datetime.datetime(2024, 1, 2, 3, 4, 5, 123457,
                             tzinfo=datetime.timezone.utc)),
    ("tm", datetime.time(3, 4, 6)), ("ok", None)])
def test_one_value_changed_moves_the_digest(column, value):
    # a row whose every value differs from each changed one
    known = (1000, 0.1, Decimal("1.50"), "x|y", b"\x00\xff",
             datetime.date(2024, 1, 2), datetime.datetime(2024, 1, 2, 3, 4, 5),
             datetime.datetime(2024, 1, 2, 3, 4, 5, 123456,
                               tzinfo=datetime.timezone.utc),
             datetime.time(3, 4, 5), True)
    con = _table(_rows(40, seed=3) + [known])
    before = _digest(con)
    con.execute(f"update t set {column} = ? where id = 1000", (value,))
    assert _digest(con) != before
    assert _digest(con) == _fold(con)


def test_the_digest_follows_canons_hash_and_stops_on_one_it_does_not_know(
        monkeypatch):
    """SHA-256 in the server the day canon's fold moves to it, the same
    number both ways; a fold of another shape turns the server's digest
    off rather than computing a different number."""
    import hashlib
    assert W.canon_hash() == "md5"
    assert "md5(" in W.digest_select("duckdb", COLS, _q)

    def sha_step(total, text):
        h = hashlib.sha256(("" if text is None else str(text)).encode())
        return total + int(h.hexdigest()[:canon.DIGEST_HEX], 16)
    monkeypatch.setattr(canon, "digest_step", sha_step)
    assert W.canon_hash() == "sha256"
    for dialect, want in (("duckdb", "sha256("), ("bigquery", "SHA256("),
                          ("snowflake", "SHA2("), ("redshift", "SHA2(")):
        cols = [("id", "integer", "BIGINT"), ("s", "text", "VARCHAR"
                                               if dialect != "bigquery"
                                               else "STRING")]
        assert want in W.digest_select(dialect, cols, _q), dialect
    con = _table(_rows(200))
    assert _digest(con) == _fold(con)

    def salted(total, text):
        return total + int(hashlib.md5(("salt" + str(text)).encode())
                           .hexdigest()[:canon.DIGEST_HEX], 16)
    monkeypatch.setattr(canon, "digest_step", salted)
    assert W.canon_hash() is None
    assert W.digest_select("duckdb", COLS, _q) is None


def test_what_each_warehouse_renders_and_what_it_leaves_to_the_fold():
    assert W.unrendered("snowflake", [("f", "float", "FLOAT"),
                                      ("n", "decimal", "NUMBER(12,2)"),
                                      ("j", "json", "VARIANT")]) == \
        ["f (FLOAT)", "j (VARIANT)"]
    # a padded CHAR's text depends on who reads it
    assert W.unrendered("redshift", [("c", "text", "character(5)"),
                                     ("v", "text", "character varying(5)")]
                        ) == ["c (character(5))"]
    assert W.unrendered("bigquery", [("f", "float", "FLOAT64"),
                                     ("j", "json", "JSON")]) == \
        ["j (JSON)"]
    # a BigQuery NUMERIC is padded to its declared scale, as the value read
    # through its driver is held
    got = W.column_text("bigquery", "`n`", "decimal", "NUMERIC(12, 2)")
    assert "RPAD" in got and "REPEAT('0', 2)" in got
    assert W.column_text("bigquery", "`n`", "decimal", "NUMERIC") == \
        "CAST(`n` AS STRING)"


# ---- the engines' choice ------------------------------------------------------

def _snowflake():
    from migkit.engines import engine_named
    eng = engine_named("snowflake", Hop(
        name="w", engine="snowflake", source=Endpoint(),
        target=Endpoint(options={"database": "a", "account": "x"}),
        databases=["sales"]))
    return eng


def test_a_warehouse_answers_the_digest_itself_where_it_renders_the_table():
    eng = _snowflake()
    eng.neutral_columns = lambda side, db, table: [("id", "INTEGER"),
                                                   ("name", "TEXT(40)")]
    asked = []

    def rows(side, db, sql, args=None):
        asked.append(" ".join(sql.split()))
        if "limit 64" in sql:
            # the proof: the raw values and the server's text of each row
            return [[1, "a", rowtext.encode(["1", "a"])],
                    [2, None, rowtext.encode(["2", None])]]
        return [[2, "123456"]]
    eng._rows = rows
    got = eng.neutral_digest("dst", "sales", "orders",
                             [("id", "integer"), ("name", "text")],
                             where='"ID" > 0')
    assert got == (2, "123456") and eng.folded_last is False
    assert asked[-1].startswith("select COUNT(*), TO_VARCHAR(COALESCE(SUM("
                                "TO_NUMBER(UPPER(SUBSTR(MD5(")
    assert asked[-1].endswith('from "SALES"."ORDERS" where ("ID" > 0)')
    # asked once a table: the proof is not run again
    eng.neutral_digest("dst", "sales", "orders",
                       [("id", "integer"), ("name", "text")])
    assert sum("limit 64" in a for a in asked) == 1


def test_a_warehouse_that_renders_a_row_differently_is_folded_here():
    """The server's text of a first row is not this process's: its SQL is
    not trusted for the table, which is folded here and said so."""
    eng = _snowflake()
    eng.neutral_columns = lambda side, db, table: [("id", "INTEGER"),
                                                   ("price", "NUMBER(12,2)")]
    def rows(side, db, sql, args=None):
        if "limit 64" in sql:
            # the server printed 1.5 where the declared scale says 1.50
            return [[1, Decimal("1.50"), rowtext.encode(["1", "1.5"])]]
        raise AssertionError("the server's digest was asked for")
    eng._rows = rows
    eng.neutral_batches = lambda side, db, table, columns, size=1000, \
        where=None: iter([[[1, Decimal("1.50")]]])
    eng._unpad = lambda side, db, table, columns, rows: rows
    got = eng.neutral_digest("dst", "sales", "orders",
                             [("id", "integer"), ("price", "decimal")])
    n, total = canon.fold_rows(["integer", "decimal"],
                               [[1, Decimal("1.50")]])
    assert got == (n, str(total)) and eng.folded_last is True


def test_a_float_column_on_snowflake_is_folded_here():
    eng = _snowflake()
    eng.neutral_columns = lambda side, db, table: [("id", "INTEGER"),
                                                   ("f", "FLOAT")]

    def rows(side, db, sql, args=None):
        raise AssertionError("asked the server for a digest it cannot give")
    eng._rows = rows
    eng.neutral_batches = lambda side, db, table, columns, size=1000, \
        where=None: iter([[[1, 0.1]]])
    eng._unpad = lambda side, db, table, columns, rows: rows
    got = eng.neutral_digest("dst", "sales", "orders",
                             [("id", "integer"), ("f", "float")])
    assert got[0] == 1 and eng.folded_last is True


def test_the_report_names_only_the_side_whose_rows_crossed_the_network():
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="h", engine="hetero",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(options={"database": "a", "account": "x"}),
              databases=["sales"],
              options={"source_engine": "postgres",
                       "target_engine": "snowflake"})
    eng = HeteroEngine(hop)
    eng.dst_engine.folded_last = False
    assert "crossed the network" not in eng._where_folded()
    eng.dst_engine.folded_last = True
    said = eng._where_folded()
    assert "snowflake holds a column whose text its SQL does not render" \
        in said, said


def test_a_duckdb_file_answers_its_digest_in_its_own_sql(tmp_path):
    """The DuckDB engine takes the same digest: in its SQL, the number the
    rows folded here give, and a changed value moves it. A column it does
    not render (a UUID) folds the table here instead."""
    import duckdb

    from migkit.engines import engine_named
    from migkit.engines.dbapi import DbapiRows
    path = str(tmp_path / "a.duckdb")
    con = duckdb.connect(path)
    con.execute("create table main.t (id bigint primary key, name varchar,"
                " price decimal(12,2), ratio double, ref uuid)")
    con.execute("insert into main.t select g, 'n|' || g, g / 7, g / 3,"
                " uuid() from range(3000) t(g)")
    con.close()
    hop = Hop(name="d", engine="duckdb", source=Endpoint(host=path),
              target=Endpoint(host=path), databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    eng = engine_named("duckdb", hop)
    cols = [("id", "integer"), ("name", "text"), ("price", "decimal"),
            ("ratio", "float")]
    got = eng.neutral_digest("dst", "main", "t", cols)
    assert eng.folded_last is False
    assert got == DbapiRows.neutral_digest(eng, "dst", "main", "t", cols)
    con = duckdb.connect(path)
    con.execute("update main.t set ratio = ratio + 1e-12 where id = 1500")
    con.close()
    assert eng.neutral_digest("dst", "main", "t", cols) != got
    eng.neutral_digest("dst", "main", "t", cols + [("ref", "text")])
    assert eng.folded_last is True


def test_nine_digits_after_the_second_are_rendered_where_they_are_kept():
    """Snowflake keeps nine digits unless declared fewer, and its reads
    keep them (`nanotime`); its SQL writes the three past the sixth where
    they are not zeros, as `canon` does - a text cut at six would meet a
    value that lost them."""
    nine = W.column_text("snowflake", '"T"', "timestamp", "TIMESTAMP_NTZ")
    assert "FF9" in nine and "RIGHT(" in nine
    assert "FF6" in W.column_text("snowflake", '"T"', "timestamp",
                                  "TIMESTAMP_NTZ(6)")
    assert "FF9" in W.column_text("snowflake", '"T"', "time", "TIME")
    assert "%n" in W.column_text("duckdb", '"t"', "timestamp",
                                 "TIMESTAMP_NS")
    # micro-second engines are written at six
    assert "%E6S" in W.column_text("bigquery", "`t`", "timestamp",
                                   "DATETIME")


def _duck_engine(tmp_path, ddl, fill):
    import duckdb

    from migkit.engines import engine_named
    path = str(tmp_path / "n.duckdb")
    con = duckdb.connect(path)
    con.execute(ddl)
    con.execute(fill)
    con.close()
    hop = Hop(name="n", engine="duckdb", source=Endpoint(host=path),
              target=Endpoint(host=path), databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    return engine_named("duckdb", hop), path


def test_a_nanosecond_and_a_uuid_are_digested_in_duckdbs_sql(tmp_path):
    """A TIMESTAMP_NS read with its nine digits and a UUID as its text: the
    digest in DuckDB's SQL is the fold of the rows read back, and a value
    moved by one nanosecond moves it."""
    import duckdb

    from migkit.engines.dbapi import DbapiRows
    eng, path = _duck_engine(
        tmp_path, "create table main.t (id bigint primary key,"
                  " stamp timestamp_ns, ref uuid)",
        "insert into main.t select g, timestamp_ns '2024-01-01"
        " 00:00:00.123456789' + g * interval '1 second', uuid() from"
        " range(500) t(g)")
    cols = [("id", "integer"), ("stamp", "timestamp"), ("ref", "uuid")]
    got = eng.neutral_digest("dst", "main", "t", cols)
    assert eng.folded_last is False
    assert got == DbapiRows.neutral_digest(eng, "dst", "main", "t", cols)
    con = duckdb.connect(path)
    # 00:04:10.123456789 moved to ...790: the ninth digit alone
    con.execute("update main.t set stamp = timestamp_ns '2024-01-01"
                " 00:04:10.123456790' where id = 250")
    con.close()
    assert eng.neutral_digest("dst", "main", "t", cols) != got


def test_a_decimal_compared_by_value_is_rendered_so_in_every_dialect():
    """`canon.NUMBER`: a decimal's own digits, the zeros after the last
    one and a point left bare taken off - in DuckDB's SQL, run here, as
    `canon` writes it; and the same shape in Snowflake's and Redshift's."""
    import duckdb
    con = duckdb.connect()
    text = W.column_text("duckdb", "d", canon.NUMBER, "DECIMAL(12,4)")
    for v in ("1.5000", "100.0000", "0.0000", "-2.2500", "7.1234", None):
        got = con.execute(f"select {text} from (select CAST(? AS"
                          " DECIMAL(12,4)) d)", (v,)).fetchone()[0]
        want = canon.render_value(canon.NUMBER, Decimal(v)) \
            if v is not None else None
        assert got == want, (v, got, want)
    for dialect in ("snowflake", "redshift"):
        got = W.column_text(dialect, "d", canon.NUMBER, "NUMERIC(12,4)")
        assert "RTRIM(RTRIM(" in got, dialect
    assert W.column_text("bigquery", "d", canon.NUMBER, "NUMERIC") == \
        "CAST(d AS STRING)"


def test_the_leaves_of_a_duckdb_table_are_computed_in_its_sql(tmp_path):
    """A table past the walk's size that differs: its leaves by ranges of
    its integer key, both sides in DuckDB's SQL (`leaves`), and the rows
    that differ named - changed, missing, extra - adding up to the whole
    difference."""
    import duckdb

    from migkit import leaves
    from migkit.engines import engine_named
    src, dst = str(tmp_path / "a.duckdb"), str(tmp_path / "b.duckdb")
    for path in (src, dst):
        con = duckdb.connect(path)
        con.execute("create table main.t (id bigint primary key,"
                    " name varchar, price decimal(12,2))")
        con.execute("insert into main.t select g, 'n|' || g, g / 7 from"
                    " range(25000) t(g)")
        con.close()
    con = duckdb.connect(dst)
    con.execute("update main.t set name = 'changed' where id = 12345")
    con.execute("delete from main.t where id = 777")
    con.execute("insert into main.t values (99999, 'extra', 1)")
    con.close()
    hop = Hop(name="lv", engine="duckdb", source=Endpoint(host=src),
              target=Endpoint(host=dst), databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    eng = engine_named("duckdb", hop)
    got = [(r.status, r.detail) for r in eng.check_data("main")
           if r.check == "data"]
    assert [s for s, _ in got] == ["diff"], got
    detail = got[0][1]
    assert "leaves that differ" in detail, detail
    assert "adds up to the whole difference" in detail, detail
    assert "12345" in detail and "777" in detail and "99999" in detail
    side = leaves.Side(eng._as_pair().dst_engine, "dst", "main", "t",
                       [("id", "integer"), ("name", "text"),
                        ("price", "decimal")], ["id"])
    assert side.sql and side.dialect == "duckdb"


def test_a_keyless_duckdb_table_against_sqlite_is_told_apart_by_value(
        tmp_path):
    """No key: DuckDB's side of the leaves in its SQL, SQLite's hashed in
    this process, the two trees alike - a row on one side and not the
    other named by count."""
    import sqlite3

    import duckdb

    from migkit.engines.hetero import HeteroEngine
    src, dst = str(tmp_path / "a.duckdb"), str(tmp_path / "b.db")
    con = duckdb.connect(src)
    con.execute("create table main.log (msg varchar, n bigint)")
    con.execute("insert into main.log select 'm' || g, g from range(300)"
                " t(g)")
    con.close()
    lite = sqlite3.connect(dst)
    lite.execute("create table log (msg text, n integer)")
    lite.executemany("insert into log values (?, ?)",
                     [(f"m{g}", g) for g in range(300) if g != 7]
                     + [("other", 7)])
    lite.commit()
    lite.close()
    hop = Hop(name="kl", engine="hetero", source=Endpoint(host=src),
              target=Endpoint(host=dst), databases=["main"],
              options={"source_engine": "duckdb",
                       "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: tmp_path
    got = [(r.status, r.detail) for r in HeteroEngine(hop).check_data("main")
           if r.check == "data"]
    assert [s for s, _ in got] == ["diff"], got
    assert "1 row on the source that the target does not hold" in got[0][1]
    assert "1 row on the target that the source does not hold" in got[0][1]
