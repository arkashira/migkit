"""A cross-engine table copied server to server, range by range, with no
row through migkit (backlog R20.13 and R20.14, `migkit.crossmove`): the
compiled mover into PostgreSQL from MySQL and from SQLite, Parquet, Delta
and Iceberg files, and each of MySQL and PostgreSQL rendering the other's
load format.

What is held here, on real servers:
* every class both engines have lands byte for byte - bytes, NULL apart
  from empty, decimals, timestamps with and without a zone, JSON, four-byte
  characters - compared as each server's own bytes, not only by the digest;
* a column a way cannot carry exactly sends the table another way, and the
  move says why;
* a range that lands wrong is found by the digest on both servers, copied
  again by the table copier, and the way left for the rest of the table -
  the digest itself proved able to say "different";
* a move stopped between ranges copies only the ranges not done;
* the hop's row filter and column mapping are applied by the source's own
  SQL and the statement's column lists.
"""
import os

import pytest

from tests import mysql_postgres_pair as xp

pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not xp.docker(),
                                 reason="docker not available")]


@pytest.fixture(scope="module")
def pair():
    xp.up()
    yield
    xp.down()


@pytest.fixture
def db(pair, monkeypatch):
    from migkit import ranges
    xp.fresh()
    # ranges of a few rows, so a small table is copied as a large one is
    monkeypatch.setattr(ranges, "LEAST", 2)
    ranges.active = ranges.Slots(1)
    os.environ.pop("MIGKIT_TABLE_COPY", None)
    monkeypatch.delenv("MIGKIT_FAILPOINT", raising=False)
    yield "cx"
    os.environ.pop("MIGKIT_TABLE_COPY", None)


#: how the move names each way where it says which a table goes
SAID = {"compiled": "the compiled mover",
        "rendered": "the source rendering the target's load format"}


MYSQL_EVERY = r"""
create table every (
  id bigint primary key,
  i_tiny tinyint, i_flag tinyint(1), i_uint int unsigned,
  i_ubig bigint unsigned, i_year year,
  d_dec decimal(20,6), d_wide decimal(38,10),
  f_double double,
  t_varchar varchar(100), t_text longtext, t_enum enum('a','b'),
  t_latin varchar(50) character set latin1,
  b_blob longblob, b_bin varbinary(300),
  dt_date date, dt_datetime datetime(6), dt_ts timestamp(6) null,
  dt_time time(6), j json
) default charset utf8mb4;
insert into every values
(1, -128, 2, 4294967295, 9223372036854775807, 2155,
 -12345678901234.123456, 1234567890123456789012345678.0123456789,
 0.1, 'café 😀𝄞', 'a\\b\tc\nd\re\\N\\.', 'a', 'Ó''Brien',
 x'00010203', x'', '1000-01-01', '1000-01-01 00:00:00.000000',
 '1970-01-01 00:00:01.000000', '00:00:00.000000',
 '{"b": 1, "a": [1.10, 2e3, "x\\ny"], "c": null, "é": "😀"}'),
(2, 127, 0, 0, 0, 1901, 0.000001, -0.0000000001, 1e308, '', '', 'b', '',
 x'', x'5c78', '9999-12-31', '9999-12-31 23:59:59.999999',
 '2038-01-19 03:14:07.999999', '23:59:59.999999', '[]'),
(3, null, null, null, null, null, null, null, null, null, null, null, null,
 null, null, null, null, null, null, null),
(4, 0, 1, 7, 7, 2000, 0, 0, -2.2250738585072014e-308, '\\', 'tab\there',
 'a', 'x', x'5c', x'09', '2024-02-29', '2024-02-29 12:34:56.500000',
 '2024-02-29 12:34:56.500000', '12:00:00.000001', 'null')
"""


def _every_bytes(db):
    xp.ok(xp.my_sql(MYSQL_EVERY))
    xp.ok(xp.my_sql("insert into every (id, b_blob, t_text) select 4 + n,"
                    " unhex(lpad(hex(n), 2, '0')), char(n + 32 using"
                    " utf8mb4) from (with recursive s(n) as (select 0 union"
                    " all select n + 1 from s where n < 255) select n from"
                    " s) g"))
    xp.ok(xp.my_sql("insert into every (id, b_blob) values (300,"
                    " unhex(concat_ws('', " + ", ".join(
                        f"'{b:02X}'" for b in range(256)) + ")))"))
    xp.ok(xp.my_sql("analyze table every"))


#: each class read as the server's own bytes on each side: (MySQL,
#: PostgreSQL); a timestamp with a zone as microseconds since the epoch
AS_BYTES_MY = {
    "text": "hex(convert({c} using utf8mb4))", "bytes": "hex({c})",
    "integer": "cast(({c} + 0) as char)", "decimal": "cast({c} as char)",
    "float": "cast({c} as char)", "date": "date_format({c}, '%Y-%m-%d')",
    "datetime": "date_format({c}, '%Y-%m-%d %H:%i:%s.%f')",
    "instant": "cast(unix_timestamp({c}) * 1000000 as signed)",
    "time": "time_format({c}, '%H:%i:%s.%f')", "json": "cast({c} as char)"}
AS_BYTES_PG = {
    "text": "upper(encode(convert_to({c}, 'UTF8'), 'hex'))",
    "bytes": "upper(encode({c}, 'hex'))", "integer": "{c}::text",
    "decimal": "{c}::text", "float": "{c}::text",
    "date": "to_char({c}, 'YYYY-MM-DD')",
    "datetime": "to_char({c}, 'YYYY-MM-DD HH24:MI:SS.US')",
    "instant": "(extract(epoch from {c}) * 1000000)::bigint::text",
    "time": "to_char({c}, 'HH24:MI:SS.US')", "json": "{c}::text"}

MY_KINDS = {"i_tiny": "integer", "i_flag": "integer", "i_uint": "integer",
            "i_ubig": "integer", "i_year": "integer", "d_dec": "decimal",
            "d_wide": "decimal", "f_double": "float",
            "t_varchar": "text", "t_text": "text", "t_enum": "text",
            "t_latin": "text", "b_blob": "bytes", "b_bin": "bytes",
            "dt_date": "date", "dt_datetime": "datetime",
            "dt_ts": "instant", "dt_time": "time", "j": "json"}


def _as_bytes(kinds, my_table="every", pg_table="every"):
    """{column: ([MySQL's], [PostgreSQL's])} by id."""
    out = {}
    for col, kind in kinds.items():
        a = xp.ok(xp.my_sql(
            "set time_zone = '+00:00'; select id, ifnull("
            + AS_BYTES_MY[kind].format(c=f"`{col}`") + ", '<null>') from"
            f" {my_table} order by id")).splitlines()
        b = xp.ok(xp.pg_sql(
            "select id || chr(9) || coalesce("
            + AS_BYTES_PG[kind].format(c=f'"{col}"') + ", '<null>') from"
            f" {pg_table} order by id")).splitlines()
        out[col] = (a, b)
    return out


def _json_same(a, b):
    from migkit import canon

    def one(line):
        i, _, v = line.partition("\t")
        return i, None if v == "<null>" else canon.render_value("json", v)
    return [one(x) for x in a] == [one(x) for x in b]


def _held_equal(kinds, **kw):
    for col, (a, b) in _as_bytes(kinds, **kw).items():
        if kinds[col] == "json":
            assert _json_same(a, b), (col, a[:3], b[:3])
        elif kinds[col] == "float":
            assert [(x.split("\t")[0], float(x.split("\t")[1]) if not
                     x.endswith("<null>") else None) for x in a] == \
                [(x.split("\t")[0], float(x.split("\t")[1]) if not
                  x.endswith("<null>") else None) for x in b], col
        else:
            assert a == b, (col, [(x, y) for x, y in zip(a, b)
                                  if x != y][:3])


def _move(tmp_path, way, table="every", src="mysql", dst="postgres",
          ck=None, **opts):
    from migkit.engines.hetero import HeteroEngine
    os.environ["MIGKIT_TABLE_COPY"] = way
    eng = HeteroEngine(xp.hop(tmp_path, src, dst, **opts))
    said = []
    eng.create_missing("cx", said.append)
    ck = xp.Checkpoint() if ck is None else ck
    eng.move_table("cx", "", table, 100000, ck, said.append)
    return eng, said, ck


def _taken(eng):
    """The ways the move's units went, as the engine reports what it
    measured (`measured_ways`): a way given up on is not timed."""
    return {w for m in eng.measured_ways()
            if m["work"] in ("range copy", "table copy, whole")
            for w in m["seconds_a_row"]}


def _checked(eng, table="every"):
    got = [r for r in eng.check_data("cx") if r.scope.endswith(table)]
    assert got and all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]


@pytest.mark.parametrize("way", ["compiled", "rendered"])
def test_mysql_into_postgres_lands_every_class_byte_for_byte(db, tmp_path,
                                                             way):
    _every_bytes(db)
    eng, said, ck = _move(tmp_path, way)
    assert _taken(eng) == {way}, (eng.measured_ways(), said)
    assert any(s.startswith(f"{db}.every: {SAID[way]} - ") for s in said), \
        said
    assert len(ck[f"{db}.every"]["ranges"]) > 2, ck
    _checked(eng)
    _held_equal(MY_KINDS)


PG_EVERY = r"""
create table every (
  id bigint primary key, i_small smallint, i_big bigint,
  d_dec numeric(20,6), f_double double precision,
  flag boolean, t_varchar varchar(100), t_text text,
  b_bytea bytea, dt_date date, dt_ts timestamp(6), dt_tstz timestamptz,
  dt_time time(6), j jsonb, u uuid
);
insert into every values
(1, -32768, -9223372036854775808, -12345678901234.123456, 0.1, true,
 'café 😀𝄞', E'a\\b\tc\nd\re\\N', '\x00010203', '1000-01-01',
 '1000-01-01 00:00:00', '1970-01-01 00:00:01+00', '00:00:00',
 '{"b": 1, "a": [1.10, 2e3, "x\ny"], "c": null}',
 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
(2, 32767, 9223372036854775807, 0.000001, 1e308, false, '', '',
 '\x', '9999-12-31', '9999-12-31 23:59:59.999999',
 '2038-01-19 03:14:07.999999+00', '23:59:59.999999', '[]',
 '00000000-0000-0000-0000-00000000000a'),
(3, null, null, null, null, null, null, null, null, null, null, null, null,
 null, null),
(4, 0, 0, 0, -2.2250738585072014e-308, true, E'\\', E'tab\there',
 '\x5c', '2024-02-29', '2024-02-29 12:34:56.5',
 '2024-02-29 12:34:56.5+07', '12:00:00.000001', 'null',
 'ffffffff-ffff-ffff-ffff-ffffffffffff');
insert into every (id, b_bytea, t_text)
  select 4 + n, decode(lpad(to_hex(n), 2, '0'), 'hex'), chr(n + 32)
  from generate_series(0, 255) n;
"""

PG_KINDS = {"i_small": "integer", "i_big": "integer", "d_dec": "decimal",
            "f_double": "float", "t_varchar": "text", "t_text": "text",
            "b_bytea": "bytes", "dt_date": "date",
            "dt_ts": "datetime", "dt_tstz": "instant", "dt_time": "time",
            "j": "json"}


def test_postgres_into_mysql_lands_every_class_byte_for_byte(db, tmp_path):
    xp.ok(xp.pg_sql(PG_EVERY))
    eng, said, ck = _move(tmp_path, "rendered", src="postgres",
                          dst="mysql")
    assert _taken(eng) == {"rendered"}, (eng.measured_ways(), said)
    assert len(ck[f"{db}.every"]["ranges"]) > 2, ck
    _checked(eng)
    _held_equal(PG_KINDS)
    # a boolean lands as MySQL's 0 and 1, and a uuid as its text
    assert xp.ok(xp.my_sql("select id, flag, u from every where id <= 2"
                           " order by id")).splitlines() == [
        "1\t1\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        "2\t0\t00000000-0000-0000-0000-00000000000a"]


def test_a_column_a_way_cannot_carry_sends_the_table_on_and_says_why(
        db, tmp_path):
    # wider than the compiled mover's decimals, past 38 digits: the source
    # renders it, both ways
    xp.ok(xp.my_sql("create table wide (id bigint primary key, d"
                    " decimal(65,30)); insert into wide values (1,"
                    f" {'9' * 35}.{'7' * 29}1), (2, null)"))
    eng, said, ck = _move(tmp_path, "", "wide")
    assert _taken(eng) == {"rendered"}, (eng.measured_ways(), said)
    _checked(eng, "wide")
    xp.ok(xp.my_sql("drop table wide"))
    eng, said, ck = _move(tmp_path, "", "wide", src="postgres",
                          dst="mysql")
    assert _taken(eng) == {"rendered"}, (eng.measured_ways(), said)
    _checked(eng, "wide")
    # a double into a single float on the target is rounded on the way:
    # the table copier takes it, and the move says why
    xp.ok(xp.pg_sql("create table narrow (id bigint primary key, r double"
                    " precision); insert into narrow values (1, 0.5),"
                    " (2, -1.25), (3, null)"))
    xp.ok(xp.my_sql("create table narrow (id bigint primary key, r"
                    " float)"))
    eng, said, ck = _move(tmp_path, "", "narrow", src="postgres",
                          dst="mysql")
    assert _taken(eng) == {"copier"}, (eng.measured_ways(), said)
    assert f"{db}.narrow: the table copier - r is a double, which a float" \
        " holds only rounded; the compiled mover does not join these two" \
        " engines" in said, said
    _checked(eng, "narrow")


def test_a_range_that_lands_wrong_is_copied_again_and_the_way_left(
        db, tmp_path, monkeypatch):
    from migkit import crossmove
    from migkit.engines.hetero import HeteroEngine
    xp.ok(xp.my_sql("create table t (id bigint primary key, v"
                    " varchar(20)); insert into t with recursive s(n) as"
                    " (select 1 union all select n + 1 from s where n < 40)"
                    " select n, concat('v', n) from s; analyze table t"))
    eng = HeteroEngine(xp.hop(tmp_path))
    eng.create_missing("cx", lambda m: None)
    real = crossmove.copy

    def wrong(e, d, s, t, way, sw, dw):
        n = real(e, d, s, t, way, sw, dw)
        xp.ok(xp.pg_sql("update t set v = 'changed' where id = 7"))
        return n
    monkeypatch.setattr(crossmove, "copy", wrong)
    moved, way, fell = eng.copy_range("cx", "t", "public.t", 0, 20,
                                      "compiled")
    assert way == "compiled" and moved == 20
    assert fell == "20 rows on each side, holding different values", fell
    # the table copier put it right
    assert xp.ok(xp.pg_sql("select v from t where id = 7")).strip() == "v7"
    assert crossmove.proved(eng, "cx", "t", "public.t", "`id` <= 20",
                            '"id" <= 20') is None
    # and the parent gives the way up for the table's next ranges
    said = []
    monkeypatch.setenv("MIGKIT_TABLE_COPY", "compiled")
    w = crossmove.TableWay(eng, "cx", "t", "public.t", said.append, "cx.t")
    assert w.pick() == "compiled"
    w.fell("compiled", fell)
    assert w.pick() == "copier"
    assert any("did not prove itself (20 rows on each side, holding"
               " different values)" in s for s in said), said


def test_the_digest_says_different_for_one_byte(db, tmp_path):
    from migkit import crossmove
    xp.ok(xp.my_sql("create table t (id bigint primary key, b varbinary(4),"
                    " s varchar(4)); insert into t values (1, x'00ff', ''),"
                    " (2, null, 'x')"))
    eng, _, _ = _move(tmp_path, "compiled", "t")
    assert crossmove.proved(eng, "cx", "t", "public.t", None, None) is None
    for change, back in (
            ("update t set b = '\\x00fe' where id = 1",
             "update t set b = '\\x00ff' where id = 1"),
            ("update t set s = null where id = 1",
             "update t set s = '' where id = 1"),
            ("delete from t where id = 2",
             "insert into t values (2, null, 'x')")):
        xp.ok(xp.pg_sql(change))
        assert crossmove.proved(eng, "cx", "t", "public.t", None, None), \
            change
        xp.ok(xp.pg_sql(back))
        assert crossmove.proved(eng, "cx", "t", "public.t", None,
                                None) is None


def test_a_move_stopped_between_ranges_copies_only_the_rest(
        db, tmp_path, monkeypatch):
    xp.ok(xp.my_sql("create table t (id bigint primary key, v int); insert"
                    " into t with recursive s(n) as (select 1 union all"
                    " select n + 1 from s where n < 30) select n, n from s;"
                    " analyze table t"))
    monkeypatch.setenv("MIGKIT_FAILPOINT", "range.committed:3:raise")
    ck = xp.Checkpoint()
    with pytest.raises(RuntimeError, match="failpoint range.committed"):
        _move(tmp_path, "compiled", "t", ck=ck)
    st = ck[f"{db}.t"]
    done, total = len(st["ranges_done"]), len(st["ranges"])
    assert 0 < done < total, st
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    from migkit import failpoint
    failpoint._counts.clear()
    eng, said, ck = _move(tmp_path, "compiled", "t", ck=ck)
    copied = [s for s in said if " copied, " in s]
    assert len(copied) == total - done, said
    _checked(eng, "t")


def test_the_row_filter_and_the_column_mapping_are_the_sources_own(
        db, tmp_path):
    xp.ok(xp.my_sql("create table t (id bigint primary key, v int, secret"
                    " varchar(10), label varchar(10)); insert into t with"
                    " recursive s(n) as (select 1 union all select n + 1"
                    " from s where n < 30) select n, n, 'x', concat('l', n)"
                    " from s"))
    for way in ("compiled", "rendered"):
        xp.pg_sql("drop table if exists t")
        eng, said, ck = _move(tmp_path, way, "t", mapping={
            "where": {"t": "`v` % 3 = 0 and `label` like 'l%'"},
            "columns": {"t": {"drop": ["secret"],
                              "rename": {"label": "name"}}}})
        assert _taken(eng) == {way}, (eng.measured_ways(), said)
        assert xp.ok(xp.pg_sql("select count(*), min(v), max(v) from t")
                     ).strip() == "10|3|30", way
        assert xp.ok(xp.pg_sql(
            "select string_agg(column_name, ',' order by column_name) from"
            " information_schema.columns where table_name = 't'")).strip() \
            == "id,name,v", way
        _checked(eng, "t")
    # and from PostgreSQL, its filter in its own SQL
    xp.ok(xp.pg_sql("create table back (id bigint primary key, v int,"
                    " secret text, label text); insert into back select g,"
                    " g, 'x', 'l' || g from generate_series(1, 30) g"))
    eng, said, ck = _move(tmp_path, "rendered", "back", src="postgres",
                          dst="mysql", mapping={
                              "where": {"back": "v % 3 = 0 and label like"
                                                " 'l%'"},
                              "columns": {"back": {
                                  "drop": ["secret"],
                                  "rename": {"label": "name"}}}})
    assert _taken(eng) == {"rendered"}, (eng.measured_ways(), said)
    assert xp.ok(xp.my_sql("select count(*), min(v), max(v) from back")
                 ).split() == ["10", "3", "30"]
    assert xp.ok(xp.my_sql(
        "select group_concat(column_name order by column_name) from"
        " information_schema.columns where table_schema = 'cx' and"
        " table_name = 'back'")).strip() == "id,name,v"
    _checked(eng, "back")


ROWS = ([(i, f"é{i}😀" if i % 5 else "", bytes([i % 256]) * 3, i / 7)
         for i in range(1, 51)] + [(51, None, None, None)])


def _files(tmp_path):
    """The same rows as a SQLite file's table, migkit's Parquet files, a
    Delta table and an Iceberg table: {source engine: (where, table)}."""
    import shutil
    import sqlite3

    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    from pyiceberg.catalog.sql import SqlCatalog

    from migkit.config import Endpoint, Hop
    from migkit.engines.parquet import ParquetEngine
    path = tmp_path / "app.db"
    f = sqlite3.connect(path)
    f.execute("create table t (id integer primary key, v text, b blob,"
              " r real)")
    f.executemany("insert into t values (?, ?, ?, ?)", ROWS)
    f.commit()
    f.close()
    lake = tmp_path / "lake"
    files = ParquetEngine(Hop(name="p", engine="parquet",
                              source=Endpoint(options={"path": str(lake)}),
                              target=Endpoint(options={"path": str(lake)}),
                              databases=["cx"]))
    files.neutral_create("dst", "cx", "pq", [
        ("id", "integer", ()), ("v", "text", ()), ("b", "bytes", ()),
        ("r", "float", ())], ["id"])
    cols = [("id", "integer"), ("v", "text"), ("b", "bytes"),
            ("r", "float")]
    files.neutral_write("dst", "cx", "pq", cols, [list(r) for r in ROWS])
    arrow = pa.table({"id": pa.array([r[0] for r in ROWS], pa.int64()),
                      "v": pa.array([r[1] for r in ROWS], pa.string()),
                      "b": pa.array([r[2] for r in ROWS], pa.binary()),
                      "r": pa.array([r[3] for r in ROWS], pa.float64())})
    # a row written and deleted: its file stays, the log says it is gone
    write_deltalake(str(lake / "cx" / "dt"), arrow)
    write_deltalake(str(lake / "cx" / "dt"), arrow.slice(0, 1).set_column(
        0, "id", pa.array([999], pa.int64())), mode="append")
    DeltaTable(str(lake / "cx" / "dt")).delete("id = 999")
    cat = SqlCatalog("local", uri=f"sqlite:///{tmp_path}/cat.db",
                     warehouse=f"file://{tmp_path}/wh")
    cat.create_namespace("cx")
    it = cat.create_table("cx.it", schema=arrow.schema)
    it.append(arrow)
    shutil.copytree(tmp_path / "wh" / "cx" / "it", lake / "cx" / "it")
    return {"sqlite": (path, ["t"]), "parquet": (lake, ["pq", "dt", "it"])}


def test_sqlite_parquet_delta_and_iceberg_go_into_the_servers(db,
                                                              tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    os.environ["MIGKIT_TABLE_COPY"] = "compiled"
    for src, (where, tables) in _files(tmp_path).items():
        for dst in ("postgres",):
            xp.fresh()
            base = xp.hop(tmp_path, "mysql", dst)
            hop = Hop(name="files", engine="hetero",
                      source=Endpoint(options={"path": str(where)}),
                      target=base.target, databases=["cx"],
                      db_map={"cx": "cx"},
                      options={"source_engine": src, "target_engine": dst})
            hop.report_dir = lambda d=None, _p=tmp_path: _p
            eng = HeteroEngine(hop)
            said = []
            eng.create_missing("cx", said.append)
            for t in tables:
                ck = xp.Checkpoint()
                eng.move_table("cx", "", t, 1000, ck, said.append)
                assert _taken(eng) == {"compiled"}, (src, dst, said)
                assert any(s.startswith(f"cx.{t}: {SAID['compiled']} - ")
                           for s in said), (src, dst, said)
                _checked(eng, t)
                there = (xp.pg_sql(f"select count(*) from {t}")
                         if dst == "postgres" else
                         xp.my_sql(f"select count(*) from {t}"))
                assert xp.ok(there).strip() == str(len(ROWS)), (src, t)
