"""What a MongoDB database holds beside its documents is read and carried
(backlog P0-PARITY.all), held without a server:

* its indexes read as every engine's are (`neutral_indexes`: name, unique,
  fields, and whether another engine can be given it as it is), made on the
  target by the statement `neutral_create_index_sql` writes and
  `execute_ddl` runs - no other;
* its views - an aggregation pipeline over a collection or another view -
  read (`neutral_views`), left out of the copy, which would have made a
  collection of what they answer, and made on the target once the
  collections are in, a view over a view after the one it reads, one the
  target already has never replaced, one over a collection the hop leaves
  out not made;
* its stored functions (`system.js`), the one stored code it keeps, read
  as routines and carried to the target as they are.

The calls the engine makes are answered by `tests/mongo_model.py`.
"""
import json

import pytest
from bson.code import Code

from migkit import capabilities as caps
from migkit.config import Endpoint, Hop
from migkit.engines.mongodb import MongoEngine
from tests.mongo_model import Server

#: what this module proves, item by engine (`capabilities.proofs`): the
#: indexes are read and made by calls the copy has always made. The views
#: and functions are made by calls (`create_collection` over a view, a
#: write into `system.js`) no server has answered yet, so those two wait
#: in `capabilities.UNPROVED`
PROVES = [("schema.indexes", "mongodb")]


def _eng(tmp_path, src, dst, **hop):
    h = Hop(name="m", engine="mongodb", source=Endpoint(host="s", port=1),
            target=Endpoint(host="t", port=2), databases=["app"], **hop)
    h.report_dir = lambda db=None: tmp_path
    eng = MongoEngine(h)
    eng._client = lambda side: (src if side == "src" else dst).client()
    return eng


@pytest.fixture
def pair():
    return Server(), Server()


def _coll(server, name, db="app", **opts):
    return server.db(db).create_collection(name, **opts)


# ---- indexes -------------------------------------------------------------

def test_an_index_is_read_the_way_every_engines_is(tmp_path, pair):
    src, dst = pair
    c = _coll(src, "orders")
    c.create_index([("sku", 1)], name="sku_1", unique=True)
    c.create_index([("a", 1), ("b", 1)], name="a_b")
    c.create_index([("a", 1), ("b", -1)], name="a_b_desc")
    c.create_index([("at", 1)], name="ttl", expireAfterSeconds=60)
    c.create_index([("x", 1)], name="part", partialFilterExpression={"x": 1})
    c.create_index([("addr.zip", 1)], name="zip")
    c.create_index([("$**", 1)], name="wild")
    c.create_index([("note", "text")], name="text", weights={"note": 1})
    eng = _eng(tmp_path, src, dst)
    got = {n: (u, cols, plain) for n, u, cols, plain in
           eng.neutral_indexes("src", "app", "orders")}
    assert got["sku_1"] == (True, ["sku"], True)
    assert got["a_b"] == (False, ["a", "b"], True)
    # what another engine cannot be given as it is is said so
    for name in ("a_b_desc", "ttl", "part", "wild", "text"):
        assert got[name][2] is False, name
    # a nested path is no column, so the pair holds it back as it does one
    # over a column the hop does not carry
    assert got["zip"] == (False, ["addr.zip"], True)
    assert "_id_" not in got


def test_a_view_has_no_indexes(tmp_path, pair):
    src, dst = pair
    _coll(src, "orders")
    _coll(src, "recent", viewOn="orders", pipeline=[])
    assert _eng(tmp_path, src, dst).neutral_indexes("src", "app",
                                                    "recent") == []


def test_the_statement_that_makes_an_index_is_run_by_execute_ddl(tmp_path,
                                                                 pair):
    src, dst = pair
    eng = _eng(tmp_path, src, dst)
    sql = eng.neutral_create_index_sql("dst", "app", 'we"ird', "ix_a_b", True,
                                       ["a", "b"])
    assert sql == ('db.getCollection("we\\"ird").createIndex('
                   '{"a": 1, "b": 1}, {"name": "ix_a_b", "unique": true})')
    eng.execute_ddl("dst", "app", sql)
    made = dst.db("app").colls['we"ird'].indexes["ix_a_b"]
    assert made["key"] == [("a", 1), ("b", 1)] and made["unique"] is True
    # run again, the same index is the index there
    eng.execute_ddl("dst", "app", sql)


def test_execute_ddl_runs_that_statement_and_nothing_else(tmp_path, pair):
    src, dst = pair
    eng = _eng(tmp_path, src, dst)
    for bad in ("drop table orders",
                'db.getCollection("a").drop()',
                'db.getCollection("a").createIndex({"x": 1}, {}) ; x',
                'db.getCollection("a").createIndex({"x": 1})'):
        with pytest.raises(ValueError):
            eng.execute_ddl("dst", "app", bad)
    # never on a source
    ok = eng.neutral_create_index_sql("src", "app", "a", "i", False, ["x"])
    with pytest.raises(SystemExit):
        eng.execute_ddl("src", "app", ok)
    assert "a" not in src.db("app").colls


def test_an_index_made_by_the_copy_has_the_sources_options(tmp_path, pair):
    """The copy and the pair read an index one way (`_index_specs`): the
    build's own bookkeeping out, the options the server keeps in."""
    src, _ = pair
    c = _coll(src, "orders")
    c.create_index([("at", 1)], name="ttl", expireAfterSeconds=60,
                   background=True)
    got = MongoEngine._index_specs(c)
    assert got == [("ttl", [("at", 1)], {"expireAfterSeconds": 60})]


# ---- views ---------------------------------------------------------------

def _views_world(src):
    _coll(src, "orders")
    _coll(src, "paid", viewOn="orders",
          pipeline=[{"$match": {"status": "paid"}}])
    _coll(src, "paid_big", viewOn="paid",
          pipeline=[{"$match": {"total": {"$gt": 100}}}])


def test_a_view_is_read_as_its_pipeline(tmp_path, pair):
    src, dst = pair
    _views_world(src)
    eng = _eng(tmp_path, src, dst)
    got = dict(eng.neutral_views("src", "app"))
    assert set(got) == {"paid", "paid_big"}
    assert json.loads(got["paid"]) == {
        "viewOn": "orders", "pipeline": [{"$match": {"status": "paid"}}]}


def test_a_view_the_hop_leaves_out_is_not_listed(tmp_path, pair):
    src, dst = pair
    _views_world(src)
    eng = _eng(tmp_path, src, dst, exclude=["app.paid_big"])
    assert [n for n, _ in eng.neutral_views("src", "app")] == ["paid"]
    # or none of that kind, by `objects`
    eng = _eng(tmp_path, src, dst, objects={"views": False})
    assert eng.neutral_views("src", "app") == []


def test_the_copy_lists_collections_and_not_views(tmp_path, pair):
    src, dst = pair
    _views_world(src)
    eng = _eng(tmp_path, src, dst)
    assert eng.list_move_tables("app") == [("", "orders")]


def test_views_are_made_in_the_order_they_read_each_other(tmp_path, pair):
    src, dst = pair
    _views_world(src)
    _coll(dst, "orders", db="app_copy")
    eng = _eng(tmp_path, src, dst, db_map={"app": "app_copy"})
    said = []
    eng.carry_database_objects("app", said.append)
    made = dst.db("app_copy").colls
    assert made["paid"].options["viewOn"] == "orders"
    assert made["paid_big"].options["viewOn"] == "paid"
    assert made["paid"].options["pipeline"] == [{"$match": {
        "status": "paid"}}]
    assert said == ["view paid: made over orders",
                    "view paid_big: made over paid"]
    # again: what is there is left, not made twice
    said.clear()
    eng.carry_database_objects("app", said.append)
    assert said == ["view paid: already on the target",
                    "view paid_big: already on the target"]


def test_a_view_is_made_over_the_name_the_hop_gives_the_collection(tmp_path,
                                                                   pair):
    src, dst = pair
    _coll(src, "orders")
    _coll(src, "paid", viewOn="orders", pipeline=[])
    _coll(dst, "orders_2024")
    eng = _eng(tmp_path, src, dst, mapping={"tables": {"orders":
                                                       "orders_2024"}})
    eng.carry_database_objects("app", lambda m: None)
    assert dst.db("app").colls["paid"].options["viewOn"] == "orders_2024"


def test_a_view_of_the_same_name_that_reads_otherwise_is_not_replaced(
        tmp_path, pair):
    src, dst = pair
    _coll(src, "orders")
    _coll(src, "paid", viewOn="orders", pipeline=[{"$match": {"a": 1}}])
    _coll(dst, "orders")
    _coll(dst, "paid", viewOn="orders", pipeline=[{"$match": {"a": 2}}])
    said = []
    _eng(tmp_path, src, dst).carry_database_objects("app", said.append)
    assert dst.db("app").colls["paid"].options["pipeline"] == [
        {"$match": {"a": 2}}]
    assert "reads otherwise; left as it is" in said[0]


def test_a_view_over_what_the_hop_leaves_out_is_not_made(tmp_path, pair):
    src, dst = pair
    _coll(src, "orders")
    _coll(src, "secret")
    _coll(src, "v", viewOn="secret", pipeline=[])
    said = []
    eng = _eng(tmp_path, src, dst, exclude=["app.secret"])
    eng.carry_database_objects("app", said.append)
    assert "v" not in dst.db("app").colls
    assert said == ["view v: not made - it reads secret, which the hop"
                    " leaves out"]


# ---- stored functions ----------------------------------------------------

def test_stored_functions_are_read_as_routines_and_carried(tmp_path, pair):
    src, dst = pair
    js = src.db("app")["system.js"]
    js.insert_one({"_id": "tax", "value": Code("function (x) { return x; }")})
    js.insert_one({"_id": "fee", "value": Code("function () { return 1; }")})
    eng = _eng(tmp_path, src, dst)
    got = eng.code_routines("src", "app")
    assert [(r.kind, r.name, r.language) for r in got] == [
        ("function", "fee", "javascript"), ("function", "tax", "javascript")]
    assert got[1].body == "function (x) { return x; }"
    said = []
    eng.carry_database_objects("app", said.append)
    held = {d["_id"]: d["value"]
            for d in dst.db("app")["system.js"].docs}
    assert held == {"tax": Code("function (x) { return x; }"),
                    "fee": Code("function () { return 1; }")}
    # a function the target has that reads otherwise is its own
    dst.db("app")["system.js"].docs[0]["value"] = Code("function () {}")
    said.clear()
    eng.carry_database_objects("app", said.append)
    assert any("reads otherwise; left as it is" in m for m in said)
    assert any("already on the target" in m for m in said)


def test_a_function_the_hop_does_not_carry_is_left(tmp_path, pair):
    src, dst = pair
    src.db("app")["system.js"].insert_one(
        {"_id": "tax", "value": Code("function () {}")})
    eng = _eng(tmp_path, src, dst, objects={"functions": False})
    assert eng.code_routines("src", "app") == []


def test_the_matrix_reads_what_the_engine_now_has():
    for item in ("schema.indexes", "schema.views", "schema.routines"):
        assert caps.depth_has("mongodb", item), item
