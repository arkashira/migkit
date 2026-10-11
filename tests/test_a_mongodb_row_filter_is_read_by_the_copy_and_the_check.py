"""The hop's row filter (`mapping.where`) takes the same documents in a
MongoDB copy, in its counts and in its comparison (P0-PARITY.scope).

The filter is written once, in SQL; the engine asks it of the documents as
a document query with SQL's NULL kept (`rowfilter.to_mongo`, held to the
reference on a server in the rowfilter module's own tests): a document
whose field is missing or null is not taken by `n > 1`, nor by `not (n >
1)`. The copy reads the source through it, the counts and the walk read
both sides through it, and a hash of the whole collection is not used to
say anything about the documents it takes.
"""
from migkit.cli import _Checkpoint
from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.row-filter", "mongodb")]

DOCS = [{"_id": 1, "n": 1}, {"_id": 2, "n": 2}, {"_id": 3, "n": 3},
        {"_id": 4, "n": None}, {"_id": 5}]


def _eng(tmp_path, where="n > 1"):
    return engine({"mapping": {"where": {"shop.orders": where}}}, tmp_path)


def test_the_copy_takes_the_documents_the_filter_takes(tmp_path):
    eng, src, dst = _eng(tmp_path)
    src.db("shop").add("orders", DOCS)
    eng.move_table("shop", "", "orders", 2,
                   _Checkpoint(tmp_path / "move.json"), lambda line: None)
    assert [d["_id"] for d in dst.db("shop").colls["orders"].docs] == [2, 3]


def test_the_counts_and_the_walk_read_both_sides_through_it(tmp_path):
    eng, src, dst = _eng(tmp_path)
    src.db("shop").add("orders", DOCS)
    dst.db("shop").add("orders", [{"_id": 2, "n": 2}, {"_id": 3, "n": 3}])
    # the documents it leaves out are not the target's to have
    assert [r.status for r in eng.check_counts("shop")] == ["ok"]
    assert [r.status for r in eng.check_data("shop")] == ["ok"]
    # one it takes, missing, is found
    dst.db("shop").colls["orders"].docs.pop()
    [counts] = eng.check_counts("shop")
    assert counts.status == "diff" and "orders src=2 dst=1" in counts.detail
    [data] = eng.check_data("shop")
    assert data.status == "diff"
    assert data.rows_apart == {"orders": {"missing": 1, "extra": 0,
                                          "changed": 0, "exact": True}}


def test_a_hash_of_the_whole_collection_says_nothing_of_a_filtered_one(
        tmp_path):
    """The two collections differ as wholes - their `dbHash`es are not the
    same - and agree where the filter looks: the answer is the walk's, not
    a hash's."""
    eng, src, dst = _eng(tmp_path)
    src.db("shop").add("orders", DOCS)
    dst.db("shop").add("orders", [{"_id": 2, "n": 2}, {"_id": 3, "n": 3}])
    [got] = eng.check_data("shop")
    assert got.status == "ok" and "dbHash" not in got.detail


def test_a_collection_without_a_filter_is_hashed_whole(tmp_path):
    eng, src, dst = engine({}, tmp_path)
    src.db("shop").add("orders", DOCS)
    dst.db("shop").add("orders", DOCS)
    [got] = eng.check_data("shop")
    assert got.status == "ok" and "dbHash" in got.detail


def test_a_filter_that_takes_nothing_is_not_a_move_that_moved_nothing(
        tmp_path):
    eng, src, dst = _eng(tmp_path, where="n > 100")
    src.db("shop").add("orders", DOCS)
    dst.db("shop").add("orders", [])
    assert eng.moved_nothing("shop") == []
