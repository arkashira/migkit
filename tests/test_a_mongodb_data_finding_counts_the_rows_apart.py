"""A MongoDB data finding counts the documents apart, collection by
collection, and names its collection as the source does; the deep check
finds a target whose newest `_id` runs past the source's (P0-PARITY.verify).

The server is one in memory (`tests/mongo_fake.py`) that gives no
`dbHash` - as a mongos or a user without the right does - so the
comparison walks the documents by their stored bytes: the way migkit
counts a document missing, only on the target, or changed.
"""
from bson.decimal128 import Decimal128
from bson.int64 import Int64

from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.rows-apart", "mongodb"), ("verify.table", "mongodb"),
          ("verify.movement.target-ahead", "mongodb")]


def _pair(tmp_path, src_docs, dst_docs, **hop):
    eng, src, dst = engine(hop, tmp_path)
    src.no_dbhash = dst.no_dbhash = True
    src.db("shop").add("orders", src_docs)
    dst.db("shop").add("orders", dst_docs)
    return eng, src, dst


def test_the_documents_apart_are_counted_one_by_one(tmp_path):
    eng, _, _ = _pair(
        tmp_path,
        [{"_id": 1, "n": 1}, {"_id": 2, "n": 2}, {"_id": 3, "n": 3}],
        [{"_id": 1, "n": 1}, {"_id": 2, "n": 99}, {"_id": 4, "n": 4}])
    [got] = eng.check_data("shop")
    assert got.status == "diff" and got.scope == "shop.orders"
    assert got.table == "orders"
    assert got.rows_apart == {"orders": {"missing": 1, "extra": 1,
                                         "changed": 1, "exact": True}}
    assert got.tables == {"orders": "diff"}
    assert "missing=1 extra=1 changed=1" in got.detail


def test_a_collection_that_agrees_says_so_and_counts_nothing(tmp_path):
    docs = [{"_id": i, "n": i} for i in range(4)]
    eng, _, _ = _pair(tmp_path, docs, docs)
    [got] = eng.check_data("shop")
    assert got.status == "ok" and not got.rows_apart
    assert got.tables == {"orders": "ok"}


def test_a_number_held_in_another_type_is_a_document_apart(tmp_path):
    """5, 5.0 and a 64-bit 5 are equal to a driver and three documents to
    the server: the walk is by the stored bytes."""
    eng, _, _ = _pair(tmp_path, [{"_id": 1, "n": 5}],
                      [{"_id": 1, "n": Int64(5)}])
    [got] = eng.check_data("shop")
    assert got.status == "diff"
    assert got.rows_apart["orders"]["changed"] == 1


def test_the_deep_check_finds_a_target_ahead_of_the_source(tmp_path):
    eng, _, _ = _pair(tmp_path, [{"_id": 1}, {"_id": 2}, {"_id": 3}],
                      [{"_id": 1}, {"_id": 2}, {"_id": 3}, {"_id": 100}])
    got = [r for r in eng.check_deep("shop") if r.scope == "shop boundary"]
    assert [r.status for r in got] == ["diff"], got
    assert "AHEAD of source" in got[0].detail
    assert got[0].category == "movement.target-ahead"


def test_a_target_that_is_only_behind_is_not_ahead(tmp_path):
    eng, _, _ = _pair(tmp_path, [{"_id": 1}, {"_id": 2}, {"_id": 3}],
                      [{"_id": 1}, {"_id": 2}])
    got = [r for r in eng.check_deep("shop") if r.scope == "shop boundary"]
    assert [r.status for r in got] == ["ok"]
    assert "1 behind" in got[0].detail


def test_a_decimal_is_not_the_double_it_prints_as(tmp_path):
    from decimal import Decimal
    eng, _, _ = _pair(tmp_path, [{"_id": 1, "p": Decimal128(Decimal("0.1"))}],
                      [{"_id": 1, "p": 0.1}])
    [got] = eng.check_data("shop")
    assert got.status == "diff" and got.rows_apart["orders"]["changed"] == 1
