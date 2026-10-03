"""One row filter, read one way and said in every engine's own words.

A filter written once is asked of the source by the copy, of both sides by
the check, of every change by the tail. If one reader takes other rows
than another, the move carries one set and the check compares a second.
Each translation here is held to one reference - SQL's three-valued
logic in Python - on the same rows, NULLs and missing fields among them:
on paper against the document store's documented rules, and on the
servers themselves.
"""
import re

import pytest

from migkit import rowfilter
from tests import servers
from tests.conftest import needs_docker

PREDICATES = [
    "a <> 'x'", "not (a = 'x')", "not (a in ('x', 'y'))", "a in ('x', null)",
    "not (a in ('x', null))", "b > 3", "not (b > 3)", "b between 2 and 4",
    "not (b between 2 and 4)", "a like 'x%'", "not (a like 'x%')",
    "a is null", "not (a is null)", "a = 'x' or b > 3",
    "not (a = 'x' or b > 3)", "not (a = 'x' and b > 3)",
    "a <> 'x' and not (b <= 2)"]


def _rows(missing=True):
    out, n = [], 0
    for a in ("x", "y", "xz", None) + (("MISSING",) if missing else ()):
        for b in (1, 3, 5, None) + (("MISSING",) if missing else ()):
            n += 1
            row = {"id": n}
            if a != "MISSING":
                row["a"] = a
            if b != "MISSING":
                row["b"] = b
            out.append(row)
    return out


def _taken(pred, rows):
    return {r["id"] for r in rows if rowfilter.evaluate(pred, "postgres", r)}


def _documented(query, doc):
    """A document store's query as its manual says it reads: a missing
    field and a null field match `null`; `$ne`/`$nin` match them too;
    `$gt` and its kind, `$in`, `$regex` match only a value."""
    for k, v in query.items():
        if k == "$and":
            if not all(_documented(x, doc) for x in v):
                return False
        elif k == "$or":
            if not any(_documented(x, doc) for x in v):
                return False
        elif k == "$expr":
            if not v:
                return False
        else:
            has, x = k in doc, doc.get(k)
            for op, arg in v.items():
                if op == "$eq":
                    ok = (x is None) if arg is None else (has and x == arg)
                elif op == "$ne":
                    ok = not ((x is None) if arg is None else
                              (has and x == arg))
                elif op in ("$gt", "$gte", "$lt", "$lte"):
                    ok = has and x is not None and \
                        isinstance(x, str) == isinstance(arg, str) and {
                            "$gt": lambda: x > arg, "$gte": lambda: x >= arg,
                            "$lt": lambda: x < arg,
                            "$lte": lambda: x <= arg}[op]()
                elif op == "$in":
                    ok = has and x in arg
                elif op == "$nin":
                    ok = not (has and x in arg) and not (
                        x is None and None in arg)
                elif op == "$exists":
                    ok = has == arg
                elif op == "$regex":
                    ok = has and isinstance(x, str) and \
                        re.search(arg, x, re.S) is not None
                elif op == "$options":
                    ok = True
                elif op == "$not":
                    ok = not (has and isinstance(x, str)
                              and re.search(arg["$regex"], x, re.S))
                else:
                    raise AssertionError(op)
                if not ok:
                    return False
    return True


@pytest.mark.parametrize("pred", PREDICATES)
def test_the_document_query_takes_the_rows_sql_takes(pred):
    query = rowfilter.to_mongo(pred, "postgres")
    rows = _rows()
    assert {r["id"] for r in rows if _documented(query, r)} == \
        _taken(pred, rows), query


def test_the_reference_keeps_sqls_null():
    row = {"a": None}
    assert not rowfilter.evaluate("a <> 'x'", "postgres", row)
    assert not rowfilter.evaluate("not (a = 'x')", "postgres", row)
    assert rowfilter.evaluate("a is null", "postgres", row)
    assert not rowfilter.evaluate("a not in ('x', null)", "postgres",
                                  {"a": "y"})


def test_a_filter_that_moves_with_the_clock_is_named():
    for pred in ("created_at > now() - interval '30 days'",
                 "d >= current_date", "r < random()"):
        assert rowfilter.problems(pred, "postgres"), pred
    assert rowfilter.problems("id in (select id from other)", "postgres")
    assert rowfilter.problems("created_at > '2026-01-01'", "postgres") == []


def test_what_a_filter_reads_and_what_it_compares_as_text():
    pred = "name = 'abc' and n > 3 and c like 'x%' and s in ('a', 'b')"
    assert rowfilter.columns(pred, "postgres") == ["c", "n", "name", "s"]
    assert rowfilter.text_compared(pred, "postgres") == ["c", "name", "s"]


def test_another_sql_with_the_columns_the_target_names():
    got = rowfilter.to_sql("`order` = 'x' and qty > 2", "mysql", "postgres",
                           {"qty": "quantity"})
    assert got == "\"order\" = 'x' AND quantity > 2"
    with pytest.raises(SystemExit, match="is not SQL"):
        rowfilter.parse("a = = 1", "postgres")


def test_a_document_query_says_what_it_cannot_hold():
    with pytest.raises(SystemExit):
        rowfilter.to_mongo("upper(a) = 'X'", "postgres")
    with pytest.raises(SystemExit):
        rowfilter.to_mongo("a in (select a from t)", "postgres")


# ---- on the servers ------------------------------------------------------

pg = servers.fixture("pg")
my = servers.fixture("mysql")
mongo = servers.fixture("mongo")


def _seed_sql(server, db, quote):
    server.sql(f"create table {quote}t{quote} (id int primary key,"
               f" a varchar(8), b int)", db)
    values = ", ".join(
        f"({r['id']}, {repr(r['a']) if r.get('a') is not None else 'null'},"
        f" {r['b'] if r.get('b') is not None else 'null'})"
        for r in _rows(missing=False))
    server.sql(f"insert into {quote}t{quote} values {values}", db)


@needs_docker
def test_postgres_takes_the_rows_the_reference_takes(pg):
    _seed_sql(pg.src, pg.name, '"')
    rows = _rows(missing=False)
    for pred in PREDICATES:
        got = {int(r[0]) for r in pg.src.sql(
            f"select id from t where {rowfilter.to_sql(pred, 'postgres', 'postgres')}",
            pg.name)}
        assert got == _taken(pred, rows), pred


@needs_docker
def test_mysql_takes_the_rows_the_reference_takes(my):
    _seed_sql(my.src, my.name, "`")
    rows = _rows(missing=False)
    for pred in PREDICATES:
        got = {int(r[0]) for r in my.src.sql(
            f"select id from t where {rowfilter.to_sql(pred, 'postgres', 'mysql')}",
            my.name)}
        assert got == _taken(pred, rows), pred


@needs_docker
def test_the_document_store_takes_the_rows_the_reference_takes(mongo):
    coll = mongo.src.client()[mongo.name]["t"]
    rows = _rows()
    coll.insert_many([dict(r, _id=r["id"]) for r in rows])
    for pred in PREDICATES:
        got = {d["_id"] for d in coll.find(rowfilter.to_mongo(pred,
                                                              "postgres"))}
        assert got == _taken(pred, rows), pred


def _hop_problems(where, engine="postgres"):
    from migkit import hopkeys
    raw = {"engine": engine, "source": {"host": "10.0.0.1", "user": "a"},
           "target": {"host": "10.0.0.2", "user": "a"},
           "mapping": {"where": where}}
    return [(p.where, p.text) for p in hopkeys.check({"hops": {"h": raw}})
            if p.level == "error"]


def test_a_hop_whose_filter_would_take_other_rows_each_time_is_refused():
    assert _hop_problems({"orders": "region = 'TH'"}) == []
    got = _hop_problems({"orders": "created_at > now() - interval '1 day'"})
    assert got and "reads the clock" in got[0][1]
    got = _hop_problems({"orders": "a = = 1"})
    assert got and "is not SQL" in got[0][1]


def test_a_filter_says_the_zone_its_times_are_read_in():
    from migkit.config import build_hop
    rule = {"orders": {"expr": "created_at >= '2026-01-01'",
                       "tz": "+07:00"}}
    assert _hop_problems(rule) == []
    hop = build_hop("h", {"engine": "postgres",
                          "source": {"host": "10.0.0.1", "user": "a"},
                          "target": {"host": "10.0.0.2", "user": "a"},
                          "mapping": {"where": rule}})
    assert hop.row_filter("db", "public", "orders") == \
        "created_at >= '2026-01-01'"
    assert hop.row_filter_zone("db", "public", "orders") == "+07:00"
    assert hop.row_filter_zone("db", "public", "other") is None
    assert _hop_problems({"orders": {"expr": "x = 1",
                                     "tz": "Mars/Base"}})[0][0] == \
        "hops.h.mapping.where.orders.tz"
    assert _hop_problems({"orders": {"tz": "UTC"}})
    assert rowfilter.zone_offset("Asia/Bangkok") == "Asia/Bangkok"


# ---- one owner, read by every reader (backlog P0-SCOPE 7, item 2) ----------

def _hop(engine="postgres", where=None, **options):
    from migkit.config import Endpoint, Hop
    return Hop(name="f", engine=engine, source=Endpoint(host="10.0.0.1"),
               target=Endpoint(host="10.0.0.2"), databases=["appdb"],
               options=dict(options),
               mapping={"where": where} if where else {})


def test_a_table_named_whole_finds_the_rule_its_leaf_answers():
    """`public.orders` given as one part is asked as `orders` too: asked
    only whole, the pair's own name for a table missed a rule written for
    its leaf."""
    hop = _hop(where={"orders": "id > 5"})
    assert hop.row_filter("appdb", "public.orders") == "id > 5"
    assert hop.row_filter("appdb", "public", "orders") == "id > 5"
    assert hop.row_filter("appdb", "public.people") is None


def test_the_pair_says_the_filter_in_the_targets_words_one_way():
    from migkit.engines.hetero import HeteroEngine
    hop = _hop("hetero", where={"orders": "region = 'apac' and qty > 2"},
               source_engine="postgres", target_engine="mysql")
    hop.mapping["columns"] = {"orders": {"rename": {"region": "area"}}}
    eng = HeteroEngine(hop)
    eng.src_engine.neutral_columns = lambda side, db, t: [
        ("region", "text"), ("qty", "integer")]
    here, there = eng._row_scope("appdb", "public.orders")
    assert here == "region = 'apac' and qty > 2"
    assert there == rowfilter.to_sql(here, "postgres", "mysql",
                                     renames={"region": "area"})
    assert "area" in there and "region" not in there


def test_a_document_store_takes_the_filter_as_its_own_query():
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.mongodb import MongoEngine
    hop = _hop("mongodb", where={"orders": "status <> 'gone'"})
    eng = MongoEngine(hop)
    assert eng.filter_dialect() == "postgres"
    assert eng.row_query("appdb", "orders") == rowfilter.to_mongo(
        "status <> 'gone'", "postgres")
    assert eng.row_query("appdb", "people") == {}
    assert eng._filtered("a = 'x'", {"_id": {"$gt": 1}}) == {
        "$and": [{"_id": {"$gt": 1}}, {"a": {"$eq": "x"}}]}
    assert eng._filtered(None, {"_id": 1}) == {"_id": 1}
    # a pair with a document store on one side reads the filter there
    pair = HeteroEngine(_hop("hetero", where={"orders": "qty > 2"},
                             source_engine="postgres",
                             target_engine="mongodb"))
    pair.src_engine.neutral_columns = lambda side, db, t: [("qty",
                                                            "integer")]
    here, there = pair._row_scope("appdb", "public.orders")
    assert rowfilter.to_mongo(there, "postgres") == {"qty": {"$gt": 2}}


def test_what_lies_outside_a_filter_is_one_query_too():
    """The pair asks a target for the rows outside the filter as
    `(filter) IS NOT TRUE`: a document store says it as the documents the
    filter's own query leaves, NULLs among them."""
    q = rowfilter.to_mongo("(a = 'x') is not true", "postgres")
    assert q == {"$nor": [{"a": {"$eq": "x"}}]}
    for row in ({"a": "x"}, {"a": "y"}, {"a": None}, {}):
        assert rowfilter.evaluate("(a = 'x') is not true", "postgres",
                                  row) == (row.get("a") != "x")
    assert rowfilter.evaluate("(a = 'x') is true", "postgres", {"a": "x"})
    assert not rowfilter.evaluate("(a = 'x') is false", "postgres", {})


def test_every_copier_that_reads_the_filter_says_so():
    from migkit.engines.cassandra import CassandraEngine
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.mssql import MSSQLEngine
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    for cls, engine in ((PostgresEngine, "postgres"), (MySQLEngine, "mysql"),
                        (MSSQLEngine, "mssql"), (MongoEngine, "mongodb")):
        assert cls(_hop(engine)).copier_filters(), engine
    assert not CassandraEngine(_hop("cassandra")).copier_filters()
    assert HeteroEngine(_hop("hetero", source_engine="postgres",
                             target_engine="mongodb")).copier_filters()


def test_a_filtered_collection_goes_by_the_copier_not_the_dump():
    from migkit import movers, planner
    hop = _hop("mongodb", where={"orders": "qty > 2"})
    movers.refuse_unpushable_filters(hop, "appdb", "mongodump", "mongodb")
    got = {d.table: d.path for d in planner.plan(
        hop, "appdb", "mongodump", ["orders", "people"], None, choice={})}
    assert got == {"orders": planner.COPIER, "people": planner.BULK}, got
    with pytest.raises(SystemExit, match="row filters"):
        movers.refuse_unpushable_filters(hop, "appdb", "mongosync",
                                         "mongodb")
    with pytest.raises(SystemExit, match="row filters"):
        movers.refuse_unpushable_filters(
            _hop("cassandra", where={"orders": "qty > 2"}), "appdb",
            "builtin", "cassandra")


# ---- the zone a filter's times are read in ------------------------------------

KINDS = {"ts": rowfilter.INSTANT, "d": rowfilter.WALL,
         "u": rowfilter.UNKNOWN}
PRED = "ts >= '2026-01-01 00:00:00' and d < '2026-01-02' and name = 'x'"


def test_a_time_compared_with_a_point_in_time_is_written_as_that_point():
    got = rowfilter.zone_free(PRED, "postgres", "+07:00", KINDS)
    assert "CAST('2025-12-31 17:00:00.000000+00:00' AS TIMESTAMPTZ)" in got
    # a wall clock's digits mean no zone, and text is text
    assert "d < '2026-01-02'" in got and "name = 'x'" in got
    got = rowfilter.zone_free(PRED, "mysql", "+07:00", KINDS)
    assert "UNIX_TIMESTAMP(ts) >= 1767200400.000000" in got
    assert "DATETIMEOFFSET" in rowfilter.zone_free(PRED, "tsql", "+07:00",
                                                   KINDS)
    assert "TO_TIMESTAMP_TZ" in rowfilter.zone_free(PRED, "oracle", "+07:00",
                                                    KINDS)
    got = rowfilter.zone_free("ts between '2026-01-01' and '2026-02-01' or"
                              " ts is null", "mysql", "Asia/Bangkok", KINDS)
    assert got == ("UNIX_TIMESTAMP(ts) BETWEEN 1767200400.000000 AND"
                   " 1769878800.000000 OR ts IS NULL"), got
    # one that says its own offset is that moment, whatever tz says
    assert "2026-01-01 10:00:00.000000+00:00" in rowfilter.zone_free(
        "ts in ('2026-01-01 10:00:00+00:00')", "postgres", "+07:00", KINDS)


def test_what_cannot_be_made_exact_is_refused_with_its_reason():
    for pred, why in (("date(ts) = '2026-01-01'", "session's zone"),
                      ("ts > now()", "session's zone"),
                      ("u > '2026-01-01'", "not measured"),
                      ("ts > 'soon'", "not a time")):
        with pytest.raises(SystemExit, match=why):
            rowfilter.zone_free(pred, "postgres", "+07:00", KINDS)
    with pytest.raises(SystemExit, match="no way"):
        rowfilter.zone_free("ts > '2026-01-01'", "sqlite", "+07:00", KINDS)
    with pytest.raises(SystemExit, match="never happens"):
        rowfilter.instant("2026-03-08 02:30:00", "America/New_York")
    with pytest.raises(SystemExit, match="happens twice"):
        rowfilter.instant("2026-11-01 01:30:00", "America/New_York")
    assert str(rowfilter.instant("2026-07-01 12:00:00",
                                 "America/New_York")) == \
        "2026-07-01 16:00:00+00:00"


def test_every_reader_reads_the_filter_through_its_engine():
    """`Engine.row_filter`: as written without a zone - a hop that says
    none reads as it always did - and zone-free by the engine's own column
    types with one."""
    from migkit.engines.postgres import PostgresEngine
    plain = PostgresEngine(_hop(where={"orders": "ts >= '2026-01-01'"}))
    assert plain.row_filter("appdb", "public", "orders") == \
        "ts >= '2026-01-01'"
    zoned = PostgresEngine(_hop(where={"orders": {
        "expr": "ts >= '2026-01-01' and d >= '2026-01-01'",
        "tz": "+07:00"}}))
    zoned.neutral_columns = lambda side, db, t: [
        ("ts", "timestamp with time zone"), ("d", "date"), ("n", "text")]
    assert zoned.time_kinds("src", "appdb", "public.orders") == {
        "ts": rowfilter.INSTANT, "d": rowfilter.WALL}
    got = zoned.row_filter("appdb", "public", "orders")
    assert "2025-12-31 17:00:00" in got and "d >= '2026-01-01'" in got


def test_text_the_two_sides_collate_otherwise_is_said_before_the_move():
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", where={"orders": "name = 'abc'"}))
    eng.databases = lambda: ["appdb"]
    eng.neutral_tables = lambda side, db: ["orders"]
    facts = {"src": {"name": {"collation": "utf8mb4_bin"}},
             "dst": {"name": {"collation": "utf8mb4_0900_ai_ci"}}}
    eng.column_facts = lambda side, db, t: facts[side]
    got = eng._filter_collation_items()
    assert got and got[0]["level"] == "fail", got
    assert "orders.name" in got[0]["detail"]
    facts["dst"] = facts["src"]
    assert eng._filter_collation_items() == []
    assert MySQLEngine(_hop("mysql"))._filter_collation_items() == []


# ---- the document store, on its server ----------------------------------------

@needs_docker
def test_the_document_store_copies_and_checks_through_the_filter(mongo,
                                                                 tmp_path):
    from migkit.config import Hop
    from migkit.engines.mongodb import MongoEngine
    src = mongo.src.client()[mongo.name]["orders"]
    dst = mongo.dst.client()[mongo.name]["orders"]
    src.insert_many([{"_id": i, "qty": i} for i in range(1, 11)])
    # the target's own document, outside the filter: never emptied
    dst.insert_one({"_id": 100, "qty": 0})
    hop = Hop(name="f", engine="mongodb", source=mongo.src.endpoint(),
              target=mongo.dst.endpoint(), databases=[mongo.name],
              mapping={"where": {"orders": "qty > 6"}})
    hop.report_dir = lambda db=None, _p=tmp_path: _p

    class _Ck(dict):
        def save(self):
            pass
    eng = MongoEngine(hop)
    eng.move_table(mongo.name, "", "orders", 100, _Ck(), lambda m: None)
    assert sorted(d["_id"] for d in dst.find()) == [7, 8, 9, 10, 100]
    got = eng.check_counts(mongo.name)
    assert all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]
    got = eng.check_data(mongo.name)
    assert all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]
    # a document inside the filter gone from the target is found
    dst.delete_one({"_id": 8})
    got = eng.check_counts(mongo.name)
    assert any(r.status == "diff" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]
