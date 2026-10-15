"""A MongoDB standalone keeps no oplog: its changes are followed by
comparing it again and again - each collection's server hash, then the
documents' stored bytes in buckets of their `_id`'s canonical hash, and
only the changed buckets' documents written (backlog LT.R10)."""
import bson
import pytest
from bson.int64 import Int64
from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument

from migkit import changedepth, decide, leaves
from migkit.engines.mongodb import (MongoEngine, bucket_digests,
                                    bucket_writes, changed_buckets,
                                    id_bucket, id_element, id_of)

SPEC = leaves.Spec("hash", bits=4, salt="s1")


def raw(doc):
    return RawBSONDocument(bson.encode(doc))


def docs(*items):
    return [raw(d) for d in items]


def digests(items):
    return bucket_digests(items, SPEC)[0]


MIXED = [{"_id": 1, "a": 1}, {"_id": Int64(1), "a": 1}, {"_id": "1", "a": 1},
         {"_id": ObjectId("0123456789ab0123456789ab"), "a": 1},
         {"_id": {"k": 2}, "a": 1}, {"_id": 2.5, "a": [1, 2]},
         {"_id": None, "a": "x"}]


def test_the_id_element_is_found_in_the_stored_bytes_whatever_its_type():
    for d in MIXED + [{"a": "first", "b": {"c": [1]}, "_id": 7}]:
        el = id_element(bson.encode(d))
        assert el is not None
        assert id_of(el) == d["_id"]
        assert type(id_of(el)) is type(d["_id"])
    assert id_element(bson.encode({"a": 1})) is None


def test_ids_alike_in_value_but_not_in_type_are_told_apart():
    e = [id_element(bson.encode(d)) for d in MIXED[:3]]
    assert len(set(e)) == 3


def test_equal_documents_give_equal_buckets_in_any_order():
    a = digests(docs(*MIXED))
    b = digests(docs(*reversed(MIXED)))
    assert a == b
    assert changed_buckets(a, b) == set()
    assert sum(c for c, _ in a.values()) == len(MIXED)


def test_a_changed_document_changes_only_its_own_bucket():
    before = docs(*MIXED)
    after = docs(*MIXED[:3], {**MIXED[3], "a": 2}, *MIXED[4:])
    changed = changed_buckets(digests(before), digests(after))
    assert changed == {id_bucket(SPEC, id_element(after[3].raw))}


def test_a_number_changed_in_its_type_alone_is_a_change():
    a = digests(docs({"_id": 1, "n": 5}))
    b = digests(docs({"_id": 1, "n": Int64(5)}))
    assert changed_buckets(a, b)


def test_field_order_is_a_change():
    a = digests(docs({"_id": 1, "x": 1, "y": 2}))
    b = digests(docs({"_id": 1, "y": 2, "x": 1}))
    assert changed_buckets(a, b)


def test_an_inserted_and_a_removed_document_change_their_buckets():
    base = docs(*MIXED)
    new = raw({"_id": "new", "a": 1})
    assert changed_buckets(digests(base), digests(base + [new])) == {
        id_bucket(SPEC, id_element(new.raw))}
    gone = base[4]
    assert changed_buckets(digests(base), digests(
        [d for d in base if d is not gone])) == {
        id_bucket(SPEC, id_element(gone.raw))}


def test_a_document_held_twice_is_counted_twice():
    one = raw({"_id": 1, "a": 1})
    a = digests([one])
    b = digests([one, one])
    assert changed_buckets(a, b)


def test_the_writes_make_the_target_the_source():
    def keyed(items):
        return {id_element(d.raw): d.raw for d in items}
    src = keyed(docs({"_id": 1, "a": 2}, {"_id": "n", "a": 1},
                     {"_id": 3, "a": 3}))
    dst = keyed(docs({"_id": 1, "a": 1}, {"_id": Int64(9), "a": 1},
                     {"_id": 3, "a": 3}))
    writes, deletes = bucket_writes(src, dst)
    assert sorted((id_of(k) for k, _ in writes), key=repr) == ["n", 1]
    assert [id_of(k) for k in deletes] == [9]
    assert type(id_of(deletes[0])) is Int64
    assert all(src[k] == r for k, r in writes)


def test_only_the_changes_the_hop_carries_are_written():
    def keyed(items):
        return {id_element(d.raw): d.raw for d in items}
    src = keyed(docs({"_id": 1, "a": 2}, {"_id": 2, "a": 1}))
    dst = keyed(docs({"_id": 1, "a": 1}, {"_id": 3, "a": 1}))
    writes, deletes = bucket_writes(src, dst, {"insert", "update"})
    assert deletes == [] and len(writes) == 2
    writes, deletes = bucket_writes(src, dst, {"update"})
    assert [id_of(k) for k, _ in writes] == [1] and deletes == []
    writes, deletes = bucket_writes(src, dst, {"delete"})
    assert writes == [] and [id_of(k) for k in deletes] == [3]


def test_a_standalone_is_followed_by_the_sweep():
    got = MongoEngine.follow_choice(decide.Facts(
        {"replica set": False, "stream opens": "not a replica set"}))
    assert got.path == "bucket sweep"
    assert "standalone" in got.reason


def test_a_replica_set_is_followed_by_its_stream_never_by_the_sweep():
    got = MongoEngine.follow_choice(decide.Facts(
        {"replica set": True, "stream opens": True}),
        decide.Costs({"bucket sweep": 0.001, "change stream": 50.0}))
    assert got.path == "change stream"
    assert any(r.name == "bucket sweep" and "per range" in why
               for r, why in got.passed)


def test_a_stream_refused_falls_to_the_sweep_and_says_why():
    got = MongoEngine.follow_choice(decide.Facts(
        {"replica set": True, "stream opens": "not authorized"}))
    assert got.path == "bucket sweep"
    assert "not authorized" in got.reason


def test_the_depth_of_a_standalone_says_what_raises_it():
    assert changedepth.mongodb(False, False) == (
        "D1", "convert to a single-node replica set")
    assert changedepth.mongodb(True, False)[0] == "D3"
    assert changedepth.mongodb(True, True) == ("D4", "")


def test_a_sweep_position_is_reached_by_its_time():
    assert MongoEngine.position_reached({"at": "2026-01-02T00:00:01"},
                                        {"at": "2026-01-02T00:00:00"})
    assert not MongoEngine.position_reached({"at": "2026-01-01T00:00:00"},
                                            {"at": "2026-01-02T00:00:00"})
    assert MongoEngine.position_reached({"at": None}, {"at": "x"}) is None


# --- against a standalone server ---------------------------------------------

from tests.test_mongo_health import PORT, _docker, mongod  # noqa: E402,F401


@pytest.mark.docker
@pytest.mark.skipif(not _docker(), reason="docker not available")
def test_a_standalone_source_is_followed_to_the_target(mongod, tmp_path,  # noqa: F811
                                                      monkeypatch):
    import pymongo

    from migkit.config import Endpoint, Hop
    monkeypatch.setenv("MIGKIT_STATE_DIR", str(tmp_path))
    hop = Hop(name="standalone", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=PORT),
              target=Endpoint(host="127.0.0.1", port=PORT),
              db_map={"lt_src": "lt_dst"})
    eng = MongoEngine(hop)
    c = pymongo.MongoClient(port=PORT)
    c.drop_database("lt_src")
    c.drop_database("lt_dst")
    rows = [{"_id": i, "v": i} for i in range(300)] + [
        {"_id": f"s{i}", "v": i} for i in range(50)]
    c.lt_src.c.insert_many(rows)
    c.lt_dst.c.insert_many(rows)
    assert eng.follow_way("lt_src").path == "bucket sweep"
    went, settled = eng._sweep_round("lt_src", True, lambda m: None)
    assert (went, settled) == (0, True)
    c.lt_src.c.update_one({"_id": 7}, {"$set": {"v": Int64(7)}})
    c.lt_src.c.delete_one({"_id": "s3"})
    c.lt_src.c.insert_one({"_id": ObjectId(), "v": -1})
    went, settled = eng._sweep_round("lt_src", True, lambda m: None)
    assert (went, settled) == (3, False)
    assert eng._sweep_round("lt_src", True, lambda m: None) == (0, True)
    a = [bson.encode(d) for d in c.lt_src.c.find(sort=[("_id", 1)])]
    b = [bson.encode(d) for d in c.lt_dst.c.find(sort=[("_id", 1)])]
    assert a == b
    assert set(eng.change_depths().values()) == {
        ("D1", "convert to a single-node replica set")}
