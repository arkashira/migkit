"""A MongoDB follow by change stream (LT.C1): its position kept in the
attempt ledger as well as the position file, so a crash between the two
goes on from the later; each document carried as the bytes the source
stored; a collection dropped or renamed starts a generation of its own,
copied again while the others go on; a target refusing writes stops the
follow, said, never written past; and the way followed - change stream,
oplog or a reseed - picked from the measured facts.
"""
import bson
import pytest
from bson import Binary, Decimal128, ObjectId
from bson.codec_options import CodecOptions, DatetimeConversion
from bson.datetime_ms import DatetimeMS
from bson.errors import InvalidBSON
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS
from pymongo.errors import OperationFailure

from migkit import tailctl
from migkit.engines.mongodb import (WRITES_BLOCKED, MongoEngine,
                                    generation_of, writes_blocked)
from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("tail.follows", "mongodb")]

#: a date past year 9999, which a Python datetime cannot hold
PAST_9999 = DatetimeMS(253402300800000 + 86_400_000)


def _event(op, coll, n, **more):
    return {"_id": {"_data": f"82{n:04x}"}, "operationType": op,
            "ns": {"db": "shop", "coll": coll}, **more}


def _facts(**kw):
    return {"replica set": True, "stream opens": True, "oplog reads": True,
            "position held": True, **kw}


def test_the_stream_is_followed_where_its_position_holds():
    assert MongoEngine.follow_choice(_facts()).path == "change stream"
    refused = MongoEngine.follow_choice(_facts(**{"stream opens": "no"}))
    assert refused.path == "oplog"


def test_a_lost_position_is_reseeded_not_followed_from():
    lost = "the source's oplog now starts later than the saved position"
    way = MongoEngine.follow_choice(_facts(**{"position held": lost}))
    assert way.path == "reseed"
    assert lost in " ".join(w for _, w in way.passed)
    # with no way to follow after it, a reseed is not taken either
    none = MongoEngine.follow_choice(_facts(**{
        "position held": lost, "stream opens": "no", "oplog reads": "no"}))
    assert none.path != "reseed"


def test_a_resume_token_too_old_to_catch_up_is_a_lost_position(
        tmp_path, monkeypatch):
    eng, _, _ = engine(tmp_path=tmp_path)
    path = tailctl.token_path(eng.hop, "shop")
    tailctl.begin(eng, "shop", path, {"_data": "8200"})
    monkeypatch.setattr(eng, "position_lost", lambda *a: None)
    monkeypatch.setattr(eng, "token_age",
                        lambda *a: {"age": 600, "room": 30})
    monkeypatch.setattr(eng, "_oplog_rate", lambda db: 50.0)
    from migkit.decide import HopCosts
    monkeypatch.setattr(HopCosts, "get", lambda self, k: 0.01
                        if k == "change stream entries" else None)
    said = eng._position_held("shop")
    assert said is not True and "300s" in said and "30s" in said
    monkeypatch.setattr(eng, "token_age",
                        lambda *a: {"age": 600, "room": 3600})
    assert eng._position_held("shop") is True


def test_a_crash_between_ledger_and_file_resumes_from_the_ledger(tmp_path):
    eng, _, _ = engine(tmp_path=tmp_path)
    path = tailctl.token_path(eng.hop, "shop")
    env = tailctl.begin(eng, "shop", path, {"_data": "8200"})
    kept = {"generations": {"orders": 2}, "parked": ["audit"],
            "invalidated": None}
    # the ledger took the batch's position, the file never did
    eng._keep_follow("shop", env, {"_data": "8207"}, kept)
    got, held = eng._follow_state("shop", path, True)
    assert got["token"] == {"_data": "8207"} and got["gen"] == 1
    assert tailctl.saved(path) == {"_data": "8207"}
    assert held["generations"] == {"orders": 2}
    assert held["parked"] == ["audit"]


def test_a_ledger_of_another_stream_is_not_resumed_from(tmp_path):
    eng, _, _ = engine(tmp_path=tmp_path)
    path = tailctl.token_path(eng.hop, "shop")
    old = tailctl.begin(eng, "shop", path, {"_data": "8200"})
    eng._keep_follow("shop", {**old, "gen": 5}, {"_data": "82ff"},
                     {"generations": {}, "parked": [], "invalidated": None})
    tailctl.begin(eng, "shop", path, {"_data": "8300"})
    got, held = eng._follow_state("shop", path, True)
    assert got["token"] == {"_data": "8300"}
    assert held["generations"] == {}


def test_a_bson_value_survives_byte_identical(tmp_path):
    eng, _, dst = engine(tmp_path=tmp_path)
    oid = ObjectId()
    doc = {"_id": oid, "price": Decimal128("1.10"),
           "blob": Binary(b"\x00\x01\xff", 0x80),
           "uuid": Binary(b"\x12" * 16, 3), "far": PAST_9999}
    stored = bson.encode(doc)
    with pytest.raises(InvalidBSON):
        bson.decode(stored)     # what a decoded follow would have stopped on
    raw = bson.encode({**_event("insert", "items", 1),
                       "documentKey": {"_id": oid}, "fullDocument": doc})
    ev = bson.decode(raw, codec_options=DEFAULT_RAW_BSON_OPTIONS)
    target = dst.db("shop")
    got = []
    target["items"].replace_one = lambda key, d, upsert=False: got.append(d)
    assert eng._tail_event("shop", target, ev, True) == 1
    assert got[0].raw == stored
    back = bson.decode(got[0].raw, codec_options=CodecOptions(
        datetime_conversion=DatetimeConversion.DATETIME_MS))
    assert back["far"] == PAST_9999 and back["blob"].subtype == 0x80
    assert back["uuid"].subtype == 3 and back["price"] == Decimal128("1.10")


def test_a_rename_reseeds_that_collection_only(tmp_path, monkeypatch):
    eng, src, dst = engine({"options": {"ddl": "apply"}}, tmp_path)
    src.db("shop").add("orders2", [{"_id": 1, "v": "new"}])
    src.db("shop").add("users", [{"_id": 1}])
    dst.db("shop").add("orders", [{"_id": 1, "v": "old"}])
    dst.db("shop").add("users", [{"_id": 1}, {"_id": 2}])
    copied = []

    def move_table(db, sch, tbl, chunk, ck, log):
        copied.append(tbl)
        ck.setdefault(f"{db}.{tbl}", {})["done"] = True
        ck.save()
        dst.db("shop").add(tbl, list(src.db("shop").colls[tbl].docs))
    monkeypatch.setattr(eng, "move_table", move_table)
    ev = _event("rename", "orders", 9, to={"db": "shop", "coll": "orders2"})
    assert generation_of(ev) == ["orders", "orders2"]
    kept = {"generations": {}, "parked": [], "invalidated": None}
    lines = []
    eng._new_generations("shop", ev, kept, True, lines.append)
    assert copied == ["orders2"]
    assert "orders" not in dst.db("shop").list_collection_names()
    assert [d["_id"] for d in dst.db("shop").colls["users"].docs] == [1, 2]
    assert kept["generations"] == {"orders": 1, "orders2": 1}
    # met again after a restart, the same event is not copied twice
    eng._new_generations("shop", ev, kept, True, lines.append)
    assert copied == ["orders2"]
    assert any("generation 1" in line for line in lines)


def test_a_drop_parked_by_the_hop_sets_only_it_aside(tmp_path):
    eng, _, dst = engine({"options": {"ddl": "park"}}, tmp_path)
    dst.db("shop").add("orders", [{"_id": 1}])
    kept = {"generations": {}, "parked": [], "invalidated": None}
    eng._new_generations("shop", _event("drop", "orders", 3), kept, True,
                         lambda line: None)
    assert kept["parked"] == ["orders"]
    assert [d["_id"] for d in dst.db("shop").colls["orders"].docs] == [1]


def test_a_drop_the_hop_stops_at_stops(tmp_path):
    eng, _, _ = engine({"options": {"ddl": "stop"}}, tmp_path)
    kept = {"generations": {}, "parked": [], "invalidated": None}
    with pytest.raises(SystemExit, match="not a row change"):
        eng._new_generations("shop", _event("drop", "orders", 3), kept,
                             True, lambda line: None)


def test_a_target_refusing_writes_is_a_blocker_with_a_next_step(tmp_path):
    eng, _, _ = engine(tmp_path=tmp_path)
    blocked = OperationFailure("User writes blocked", WRITES_BLOCKED)
    assert writes_blocked(blocked)
    stop = eng._write_refused(blocked, "shop.orders")
    assert isinstance(stop, SystemExit)
    assert "does not write past a block" in str(stop)
    assert "Lift the block" in str(stop)
    assert eng._write_refused(OperationFailure("dup", 11000), "x") is None

