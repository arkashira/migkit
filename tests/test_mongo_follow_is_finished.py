"""The MongoDB follow finished (W11F leftovers): the oplog rung carries the
bytes the source stored, so a date past the year 9999 crosses; a change
stream asks for a reshard's events where the server shows them and is
passed over where a sharded cluster would leave them out, the version
named; a reseed drops what the source no longer has, planned first; and the
`_id` edges of a collection are cut through the one place edges are cut.
"""
import bson
import pytest
from bson import Timestamp
from bson.datetime_ms import DatetimeMS
from bson.raw_bson import RawBSONDocument

from migkit import interlock
from migkit.engines.mongodb import (MongoEngine, oplog_entry, oplog_events,
                                    oplog_update)
from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse

PROVES = [("tail.follows", "mongodb")]

PAST_9999 = DatetimeMS(253402300800000 + 86_400_000)


def _stored(entry):
    return RawBSONDocument(bson.encode(entry))


def _entry(op, ns, o, **more):
    return {"ts": Timestamp(10, 1), "op": op, "ns": ns, "o": o, **more}


def test_an_entry_read_as_stored_keeps_a_date_past_9999_as_its_bytes():
    doc = {"_id": 7, "at": PAST_9999, "n": 1}
    stored = _stored(_entry("i", "shop.orders", doc))
    with pytest.raises(Exception):
        bson.decode(stored.raw).get("o")["at"].year
    got = oplog_events(stored, "shop", {})
    assert [e["operationType"] for e in got] == ["insert"]
    assert got[0]["documentKey"] == {"_id": 7}
    kept = got[0]["fullDocument"]
    assert isinstance(kept, RawBSONDocument)
    assert kept.raw == bson.encode(doc)
    assert oplog_entry(stored)["ts"] == Timestamp(10, 1)


def test_a_replace_a_delete_and_a_modify_of_a_stored_entry():
    whole = {"_id": 3, "at": PAST_9999}
    rep = oplog_events(_stored(_entry("u", "shop.orders", whole,
                                      o2={"_id": 3})), "shop", {})[0]
    assert rep["operationType"] == "replace"
    assert rep["fullDocument"].raw == bson.encode(whole)
    assert rep["documentKey"] == {"_id": 3}
    mod = oplog_events(_stored(_entry(
        "u", "shop.orders", {"$v": 1, "$set": {"at": PAST_9999}},
        o2={"_id": 3})), "shop", {})[0]
    assert mod["modifiers"] == {"$set": {"at": PAST_9999}}
    assert bson.encode({"x": mod["modifiers"]["$set"]["at"]}) \
        == bson.encode({"x": PAST_9999})
    gone = oplog_events(_stored(_entry("d", "shop.orders", {"_id": 3})),
                        "shop", {})
    assert gone[0]["operationType"] == "delete"
    assert gone[0]["documentKey"] == {"_id": 3}
    kind, what = oplog_update(RawBSONDocument(bson.encode(whole)))
    assert kind == "replace" and what.raw == bson.encode(whole)


def test_a_transaction_of_stored_entries_is_held_until_it_commits():
    inner = {"op": "i", "ns": "shop.orders",
             "o": {"_id": 1, "at": PAST_9999}}
    lsid = {"id": b"\x01" * 16}
    part = _stored(_entry("c", "admin.$cmd",
                          {"applyOps": [inner], "partialTxn": True},
                          lsid=lsid, txnNumber=5))
    txns = {}
    assert oplog_events(part, "shop", txns) == []
    assert len(txns) == 1
    last = _stored(_entry("c", "admin.$cmd", {"applyOps": [
        {"op": "d", "ns": "shop.orders", "o": {"_id": 2}}]},
        lsid=lsid, txnNumber=5))
    got = oplog_events(last, "shop", txns)
    assert [e["operationType"] for e in got] == ["insert", "delete"]
    assert got[0]["fullDocument"].raw == bson.encode(inner["o"])
    assert txns == {}


def test_a_drop_command_of_a_stored_entry_still_stops_the_follow():
    got = oplog_events(_stored(_entry("c", "shop.$cmd", {"drop": "orders"})),
                       "shop", {})
    assert got == [{"operationType": "drop",
                    "ns": {"db": "shop", "coll": "orders"}}]


def test_the_oplog_is_read_as_stored_on_request(tmp_path):
    eng, src, _ = engine(tmp_path=tmp_path)
    src.db("local").add("oplog.rs", [_entry("i", "shop.orders",
                                            {"_id": 1, "at": PAST_9999})])
    got = eng._oplog("src", {}, stored=True)
    assert isinstance(got[0], RawBSONDocument)
    kept = oplog_events(got[0], "shop", {})[0]["fullDocument"]
    assert kept.raw == bson.encode({"_id": 1, "at": PAST_9999})


def _on(monkeypatch, eng, version, kind="isdbgrid"):
    monkeypatch.setattr(eng, "_server_versions", lambda: (version, "7.0.0"))
    monkeypatch.setattr(eng, "_hello", lambda side: {"msg": kind})


def test_a_stream_asks_for_expanded_events_from_6_0(tmp_path, monkeypatch):
    eng, _, _ = engine(tmp_path=tmp_path)
    _on(monkeypatch, eng, "6.0.3")
    assert eng._expanded_events() == {"show_expanded_events": True}
    _on(monkeypatch, eng, "5.0.9")
    assert eng._expanded_events() == {}
    _on(monkeypatch, eng, "")
    assert eng._expanded_events() == {}


def test_a_sharded_cluster_before_6_0_is_not_followed_by_its_stream(
        tmp_path, monkeypatch):
    eng, _, _ = engine(tmp_path=tmp_path)
    _on(monkeypatch, eng, "5.0.9")
    said = eng._reshards_reported()
    assert said is not True and "5.0" in said and "6.0" in said
    way = MongoEngine.follow_choice({"replica set": True,
                                     "stream opens": True,
                                     "oplog reads": True,
                                     "reshards reported": said})
    assert way.path != "change stream"
    assert any(r.name == "change stream" and "5.0" in why
               for r, why in way.passed)
    for ok in (("6.0.3", "isdbgrid"), ("5.0.9", "rs"), ("4.2.1", "isdbgrid")):
        _on(monkeypatch, eng, ok[0], ok[1])
        assert eng._reshards_reported() is True
    assert MongoEngine.follow_choice({"replica set": True,
                                      "stream opens": True,
                                      "oplog reads": True}).path \
        == "change stream"


def _reseed(tmp_path, monkeypatch, hop_kw=None):
    eng, src, dst = engine(hop_kw, tmp_path)
    src.db("shop").add("users", [{"_id": 1}])
    dst.db("shop").add("users", [{"_id": 1}])
    dst.db("shop").add("old", [{"_id": 9}])
    calls, lines = [], []
    monkeypatch.setattr(interlock, "before_one_way",
                        lambda hop, e, db, action, go, log:
                        calls.append((action, go)))
    monkeypatch.setattr(eng, "change_point", lambda side, db: {"_data": "82"})
    monkeypatch.setattr(eng, "tail_apply", lambda *a: "followed")
    monkeypatch.setattr(eng, "_keep_follow", lambda *a, **k: None)

    def move_table(db, sch, tbl, chunk, ck, log):
        dst.db("shop").add(tbl, list(src.db("shop").colls[tbl].docs))
    monkeypatch.setattr(eng, "move_table", move_table)
    return eng, dst, calls, lines


def test_a_reseed_drops_what_the_source_no_longer_has(tmp_path, monkeypatch):
    eng, dst, calls, lines = _reseed(tmp_path, monkeypatch,
                                     {"options": {"ddl": "apply"}})
    path = tmp_path / "token.json"
    assert eng._reseed_follow("shop", True, path, lines.append) \
        == "followed"
    assert "old" not in dst.db("shop").list_collection_names()
    assert "users" in dst.db("shop").list_collection_names()
    assert calls == [("drop", True)]
    assert any("old" in line and "no longer has" in line for line in lines)


def test_a_reseed_without_go_plans_the_drop_and_writes_nothing(
        tmp_path, monkeypatch):
    eng, dst, calls, lines = _reseed(tmp_path, monkeypatch,
                                     {"options": {"ddl": "apply"}})
    assert eng._reseed_follow("shop", False, tmp_path / "t.json",
                              lines.append) is None
    assert calls == [("drop", False)]
    assert "old" in dst.db("shop").list_collection_names()


def test_a_reseed_leaves_a_collection_the_hop_sets_aside(tmp_path,
                                                         monkeypatch):
    eng, dst, calls, lines = _reseed(tmp_path, monkeypatch,
                                     {"options": {"ddl": "park"}})
    eng._reseed_follow("shop", True, tmp_path / "t.json", lines.append)
    assert "old" in dst.db("shop").list_collection_names()
    assert calls == []
    assert any("left on the target" in line for line in lines)


def test_a_reseed_stops_where_the_hop_stops_at_a_drop(tmp_path, monkeypatch):
    eng, dst, calls, lines = _reseed(tmp_path, monkeypatch,
                                     {"options": {"ddl": "stop"}})
    with pytest.raises(SystemExit) as e:
        eng._reseed_follow("shop", True, tmp_path / "t.json",
                           lines.append)
    assert "old" in str(e.value) and "Nothing was copied or dropped" \
        in str(e.value)
    assert "old" in dst.db("shop").list_collection_names()


class _Buckets:
    name = "orders"

    def __init__(self, counts, ids):
        self.counts, self.ids = counts, ids

    def aggregate(self, pipeline, **kw):
        lo = 0
        for count, top in zip(self.counts, self.ids):
            yield {"_id": {"min": lo, "max": top}, "count": count}
            lo = top + 1


def test_the_id_edges_are_cut_through_the_one_place_edges_are_cut(
        tmp_path, monkeypatch):
    from migkit import ranges
    eng, _, _ = engine(tmp_path=tmp_path)
    seen = []
    real = ranges.cut

    def spy(*a, **kw):
        seen.append((a[1], a[4], a[5], a[6]))
        return real(*a, **kw)
    monkeypatch.setattr(ranges, "cut", spy)
    coll = _Buckets([250, 250, 250, 250], [249, 499, 749, 999])
    edges = eng._id_edges(coll, {"_id": {"$type": 16}}, 1000, 4, 16)
    assert seen == [("mongodb", "_id", 1000, 250)]
    assert edges == [249, 499, 749]
    # too few buckets for the ranges asked: one range, not a guess
    few = _Buckets([500, 500], [499, 999])
    assert eng._id_edges(few, {}, 1000, 8, 16) == []
