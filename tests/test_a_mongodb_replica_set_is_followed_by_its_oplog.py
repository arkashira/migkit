"""A replica set member whose change stream is refused - a server older
than a database's stream, a stream the user may not open, a set run
without majority reads - is followed by its oplog: each entry up to the
point a majority holds, given as the stream's own event and applied by
key, a transaction once it commits, its own position the time of the last
entry applied. The three ways are climbed on the facts and on what each
was measured to cost; a refused question stops as the grant it lacks."""
import json

import pytest
from bson.timestamp import Timestamp
from pymongo.errors import OperationFailure

from migkit import changedepth, decide, tailctl
from migkit.engines import enterprise_connectors as ec
from migkit.engines.mongodb import (MongoEngine, oplog_events, oplog_token,
                                    oplog_ts, oplog_update)
from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse


def ts(i):
    return Timestamp(100, i)


# --- an entry said as the stream's event --------------------------------------

def test_an_update_is_said_as_the_fields_it_set_and_unset():
    assert oplog_update({"$v": 1, "$set": {"a": 1}, "$unset": {"b": True}}) \
        == ("modify", {"$set": {"a": 1}, "$unset": {"b": True}})
    assert oplog_update({"_id": 1, "a": 2}) == ("replace", {"_id": 1, "a": 2})


def test_a_difference_is_translated_down_to_array_elements():
    kind, mods = oplog_update({"$v": 2, "diff": {
        "u": {"a": 1}, "i": {"n": 2}, "d": {"old": False},
        "ssub": {"u": {"x": 3}},
        "sarr": {"a": True, "u1": 9, "s2": {"u": {"y": 1}}}}})
    assert kind == "modify"
    assert mods == {"$set": {"a": 1, "n": 2, "sub.x": 3, "arr.1": 9,
                             "arr.2.y": 1}, "$unset": {"old": ""}}


def test_what_cannot_be_said_as_fields_is_read_again():
    assert oplog_update({"$v": 2, "diff": {"sarr": {"a": True, "l": 1}}}) \
        == ("lookup", None)
    assert oplog_update({"$v": 2, "diff": {"q": 1}}) == ("lookup", None)
    assert oplog_update({"$inc": {"a": 1}}) == ("lookup", None)


def test_only_the_databases_own_row_changes_are_given():
    txns = {}
    assert oplog_events({"op": "n", "ns": "", "o": {"msg": "x"}},
                        "shop", txns) == []
    assert oplog_events({"op": "i", "ns": "other.c", "o": {"_id": 1}},
                        "shop", txns) == []
    assert oplog_events({"op": "i", "ns": "shop.system.views",
                         "o": {"_id": 1}}, "shop", txns) == []
    got = oplog_events({"op": "d", "ns": "shop.orders", "o": {"_id": 7}},
                       "shop", txns)
    assert got == [{"operationType": "delete",
                    "ns": {"db": "shop", "coll": "orders"},
                    "documentKey": {"_id": 7}}]
    got = oplog_events({"op": "u", "ns": "shop.orders", "o2": {"_id": 7},
                        "o": {"$set": {"a": 1}}}, "shop", txns)
    assert got[0]["operationType"] == "update"
    assert got[0]["documentKey"] == {"_id": 7}
    assert got[0]["modifiers"] == {"$set": {"a": 1}}


def _txn(i, ops, **o):
    return {"ts": ts(i), "op": "c", "ns": "admin.$cmd",
            "lsid": {"id": "s1"}, "txnNumber": 3,
            "o": {"applyOps": ops, **o}}


def test_a_transaction_is_given_once_it_commits_in_the_order_written():
    txns = {}
    a = {"op": "i", "ns": "shop.orders", "o": {"_id": 1}}
    b = {"op": "i", "ns": "other.c", "o": {"_id": 2}}
    c = {"op": "d", "ns": "shop.orders", "o": {"_id": 3}}
    assert oplog_events(_txn(1, [a, b], partialTxn=True), "shop", txns) == []
    assert txns
    got = oplog_events(_txn(2, [c]), "shop", txns)
    assert [e["operationType"] for e in got] == ["insert", "delete"]
    assert txns == {}


def test_an_aborted_transaction_is_let_go_and_a_prepared_one_waits():
    txns = {}
    a = {"op": "i", "ns": "shop.orders", "o": {"_id": 1}}
    oplog_events(_txn(1, [a], partialTxn=True), "shop", txns)
    abort = {"op": "c", "ns": "admin.$cmd", "lsid": {"id": "s1"},
             "txnNumber": 3, "o": {"abortTransaction": 1}}
    assert oplog_events(abort, "shop", txns) == [] and txns == {}
    assert oplog_events(_txn(2, [a], prepare=True), "shop", txns) == []
    commit = {**abort, "o": {"commitTransaction": 1}}
    assert [e["documentKey"] for e in oplog_events(commit, "shop", txns)] \
        == [{"_id": 1}]


def test_a_collection_dropped_or_renamed_is_the_event_that_stops():
    txns = {}
    drop = oplog_events({"op": "c", "ns": "shop.$cmd",
                         "o": {"drop": "orders"}}, "shop", txns)
    assert drop == [{"operationType": "drop",
                     "ns": {"db": "shop", "coll": "orders"}}]
    into = oplog_events({"op": "c", "ns": "admin.$cmd",
                         "o": {"renameCollection": "other.a",
                               "to": "shop.b"}}, "shop", txns)
    assert into[0]["operationType"] == "rename"
    assert into[0]["ns"]["coll"] == "b"
    assert oplog_events({"op": "c", "ns": "other.$cmd",
                         "o": {"drop": "x"}}, "shop", txns) == []
    assert oplog_events({"op": "c", "ns": "shop.$cmd",
                         "o": {"create": "new"}}, "shop", txns) == []
    for kind in ("drop", "rename", "dropDatabase", "truncate"):
        assert kind not in MongoEngine.STREAM_OPS


# --- the rung chosen per fact -------------------------------------------------

def _choice(stream, oplog, costs=None):
    return MongoEngine.follow_choice(decide.Facts(
        {"replica set": True, "stream opens": stream, "oplog reads": oplog}),
        costs)


def test_a_stream_that_opens_is_taken_over_a_cheaper_oplog():
    got = _choice(True, True, decide.Costs({"oplog": 0.001,
                                            "change stream": 50.0}))
    assert got.path == "change stream"
    assert any(r.name == "oplog" and "per range" in why
               for r, why in got.passed)


def test_a_stream_refused_falls_to_the_oplog_and_says_why():
    got = _choice("change streams are only supported on 4.0 and later", True)
    assert got.path == "oplog"
    assert "4.0" in got.reason
    assert got.runner.name == "bucket sweep"


def test_an_oplog_refused_falls_to_the_sweep_naming_the_grant():
    said = ("mongodb: reading the oplog was refused - the user needs "
            + MongoEngine.OPLOG_GRANT)
    got = _choice("not authorized", said)
    assert got.path == "bucket sweep"
    assert "local.oplog.rs" in got.reason
    assert "not authorized" in got.reason


def test_between_the_oplog_and_the_sweep_the_measured_cost_decides():
    assert _choice("refused", True, decide.Costs(
        {"oplog": 9.0, "bucket sweep": 0.5})).path == "bucket sweep"
    assert _choice("refused", True, decide.Costs(
        {"oplog": 0.5, "bucket sweep": 9.0})).path == "oplog"


def test_the_oplog_cost_is_its_measured_rate_times_the_writes(tmp_path,
                                                              monkeypatch):
    monkeypatch.setenv("MIGKIT_STATE_DIR", str(tmp_path))
    eng, _, _ = engine(tmp_path=tmp_path)
    assert "oplog" not in eng._follow_costs("shop")
    decide.HopCosts(eng.hop, "follow/").saw("oplog entries", 100, 2.0)
    monkeypatch.setattr(eng, "_oplog_rate", lambda db: 30.0)
    got = eng._follow_costs("shop")["oplog"]
    assert got == pytest.approx(30.0 * eng.SWEEP_EVERY * 0.02, rel=0.05)
    assert "change stream" not in eng._follow_costs("shop")


def test_the_depth_is_said_for_each_of_the_three_ways():
    assert changedepth.mongodb(True, True, "change stream") == ("D4", "")
    assert changedepth.mongodb(True, False, "change stream")[0] == "D3"
    depth, fix = changedepth.mongodb(True, True, "oplog", "refused")
    assert depth == "D3" and "change stream (refused)" in fix
    assert "changeStreamPreAndPostImages" not in fix
    assert "changeStreamPreAndPostImages" in changedepth.mongodb(
        True, False, "oplog")[1]
    depth, fix = changedepth.mongodb(True, False, "bucket sweep", "no grant")
    assert depth == "D1" and "oplog" in fix and "no grant" in fix
    assert changedepth.mongodb(False, True, "bucket sweep") == (
        "D1", "convert to a single-node replica set")
    assert changedepth.mongodb(True, False) == changedepth.mongodb(
        True, False, "change stream")


# --- the position -------------------------------------------------------------

def test_the_position_goes_through_its_file_and_back(tmp_path):
    at = Timestamp(1_700_000_000, 7)
    assert oplog_ts(json.loads(json.dumps(oplog_token(at)))) == at
    eng, _, _ = engine(tmp_path=tmp_path)
    path = tailctl.token_path(eng.hop, "shop")
    tailctl.keep(path, {"id": "x", "token": oplog_token(at)})
    assert oplog_ts(tailctl.saved(path)) == at
    assert eng._tail_position("shop") == at
    assert MongoEngine.position_reached(oplog_token(at), at)
    assert not MongoEngine.position_reached(oplog_token(at),
                                            Timestamp(1_700_000_000, 8))


def test_a_stream_token_is_read_for_its_time_and_a_sweep_has_none():
    token = "82" + (1_700_000_000).to_bytes(4, "big").hex() \
        + (3).to_bytes(4, "big").hex() + "2b0229"
    assert oplog_ts({"_data": token}) == Timestamp(1_700_000_000, 3)
    assert oplog_ts(token) == Timestamp(1_700_000_000, 3)
    assert oplog_ts({"at": "2026-01-01T00:00:00"}) is None
    assert oplog_ts(None) is None


# --- a refused question named as its grant ------------------------------------

def _refusal(command):
    return OperationFailure(f"not authorized on admin to execute command"
                            f" {{ {command}: 1 }}", code=13)


def test_a_refused_list_of_databases_names_its_grant(tmp_path):
    eng, src, _ = engine({"databases": []}, tmp_path)

    def refuse():
        raise _refusal("listDatabases")
    src.list_database_names = refuse
    with pytest.raises(ec.MissingGrant) as got:
        eng.databases()
    assert "readAnyDatabase or clusterMonitor" in str(got.value)


def test_a_refused_collection_statistic_is_no_empty_answer(tmp_path):
    eng, src, _ = engine(tmp_path=tmp_path)
    shop = src.db("shop")
    shop.add("orders", [{"_id": 1}])

    def stats(cmd, *a, **kw):
        raise _refusal("collStats")
    shop.command = stats
    with pytest.raises(ec.MissingGrant) as got:
        eng.table_facts("src", "shop")
    assert "collStats" in str(got.value) and "read role" in str(got.value)

    def a_view(cmd, *a, **kw):
        raise OperationFailure("Namespace shop.orders is a view, not a"
                               " collection", code=166)
    shop.command = a_view
    assert eng.table_facts("src", "shop") == {}


def _member(eng, majority=ts(9)):
    eng._hello = lambda side: {
        "setName": "rs0", "operationTime": majority,
        "lastWrite": {"majorityOpTime": {"ts": majority}}}


def test_a_refused_oplog_read_names_its_grant(tmp_path):
    eng, src, _ = engine(tmp_path=tmp_path)
    _member(eng)
    oplog = src.local.add("oplog.rs", [{"ts": ts(1), "op": "n"}])
    assert eng._oplog_reads("src") is True

    def refuse(*a, **kw):
        raise _refusal("find")
    oplog.find = refuse
    with pytest.raises(ec.MissingGrant) as got:
        eng._oplog("src", {})
    assert "local.oplog.rs" in str(got.value)
    said = eng._oplog_reads("src")
    assert said is not True and "local.oplog.rs" in said
    with pytest.raises(ec.MissingGrant):
        eng.position_lost("src", "shop", oplog_token(ts(1)))


def test_no_majority_point_and_a_router_are_no_oplog_to_follow(tmp_path):
    eng, _, _ = engine(tmp_path=tmp_path)
    eng._hello = lambda side: {"setName": "rs0"}
    assert "majority" in eng._oplog_reads("src")
    eng._hello = lambda side: {"msg": "isdbgrid"}
    assert "router" in eng._oplog_reads("src")
    eng._hello = lambda side: {}
    assert "standalone" in eng._oplog_reads("src")


# --- followed on the fakes ----------------------------------------------------

def _follow(tmp_path, monkeypatch, entries, start=ts(0)):
    """(the oplog follow's `look`, the target's orders, the engine, the
    costs kept): the oplog read from `entries` by their time, the source a
    member whose majority point is the last entry's."""
    monkeypatch.setenv("MIGKIT_STATE_DIR", str(tmp_path))
    eng, src, dst = engine(tmp_path=tmp_path)
    src.db("shop").add("orders", [{"_id": 3, "arr": [1]}])
    held = dst.db("shop").add("orders", [{"_id": 1, "a": 1, "b": 1},
                                         {"_id": 2, "a": 2},
                                         {"_id": 3, "arr": [1, 2]}])
    _member(eng, entries[-1]["ts"])

    def oplog(side, flt, limit=0, sort=1, projection=None):
        bound = flt.get("ts") or {}
        got = [e for e in entries
               if ("$gt" not in bound or e["ts"] > bound["$gt"])
               and ("$gte" not in bound or e["ts"] >= bound["$gte"])
               and ("$lte" not in bound or e["ts"] <= bound["$lte"])]
        got = got[::-1] if sort < 0 else got
        return got[:limit] if limit else got
    eng._oplog = oplog
    timed = []
    monkeypatch.setattr(decide.HopCosts, "saw",
                        lambda self, name, units, seconds:
                        timed.append((name, units)))
    seen = {}
    monkeypatch.setattr(tailctl, "follow", lambda *a: seen.update(
        look=a[6], behind=a[7]))
    path = tailctl.token_path(eng.hop, "shop")
    tailctl.keep(path, {"id": "x", "token": oplog_token(start)})
    eng._oplog_follow("shop", True, path, lambda m: None)
    return seen["look"], held, eng, timed


def _orders(held):
    return {d["_id"]: d for d in held.docs}


def test_entries_are_applied_and_an_open_transaction_holds_the_position(
        tmp_path, monkeypatch):
    entries = [
        {"ts": ts(1), "op": "i", "ns": "shop.orders", "o": {"_id": 4, "a": 4}},
        {"ts": ts(2), "op": "u", "ns": "shop.orders", "o2": {"_id": 1},
         "o": {"$v": 1, "$set": {"a": 10}, "$unset": {"b": True}}},
        _txn(3, [{"op": "i", "ns": "shop.orders", "o": {"_id": 5}}],
             partialTxn=True),
        {"ts": ts(4), "op": "d", "ns": "shop.orders", "o": {"_id": 2}},
        {"ts": ts(5), "op": "u", "ns": "shop.orders", "o2": {"_id": 3},
         "o": {"$v": 2, "diff": {"sarr": {"a": True, "l": 1}}}},
    ]
    look, held, eng, timed = _follow(tmp_path, monkeypatch, entries)
    token, went, settled = look(oplog_token(ts(0)))
    assert (token, went, settled) == (oplog_token(ts(2)), 4, True)
    got = _orders(held)
    assert got[1] == {"_id": 1, "a": 10}
    assert 2 not in got and 5 not in got
    assert got[3] == {"_id": 3, "arr": [1]}
    assert got[4] == {"_id": 4, "a": 4}
    assert timed == [("oplog entries", 5)]

    entries.append(_txn(6, [{"op": "i", "ns": "shop.orders",
                             "o": {"_id": 6}}]))
    _member(eng, ts(6))
    token, went, settled = look(token)
    assert (token, went) == (oplog_token(ts(6)), 2)
    assert {5, 6} <= set(_orders(held))

    # read again from the start, as after a crash: it converges
    before = _orders(held)
    token, _, _ = look(oplog_token(ts(0)))
    assert token == oplog_token(ts(6)) and _orders(held) == before


def test_a_collection_dropped_stops_the_follow(tmp_path, monkeypatch):
    entries = [{"ts": ts(1), "op": "c", "ns": "shop.$cmd",
                "o": {"drop": "orders"}}]
    look, held, _, _ = _follow(tmp_path, monkeypatch, entries)
    with pytest.raises(SystemExit) as got:
        look(oplog_token(ts(0)))
    assert "'drop'" in str(got.value)
    assert set(_orders(held)) == {1, 2, 3}


def test_a_position_the_oplog_no_longer_holds_stops_before_applying(
        tmp_path, monkeypatch):
    entries = [{"ts": Timestamp(200, 1), "op": "i", "ns": "shop.orders",
                "o": {"_id": 9}}]
    look, held, _, _ = _follow(tmp_path, monkeypatch, entries,
                               start=Timestamp(100, 1))
    with pytest.raises(SystemExit) as got:
        look(oplog_token(Timestamp(100, 1)))
    assert "gone" in str(got.value)
    assert 9 not in _orders(held)
