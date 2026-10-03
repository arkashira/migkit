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
