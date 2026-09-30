"""One reader for every source that publishes its changes elsewhere
(backlog ER4, ER5): TiCDC's topic, CockroachDB's changefeed, DSQL's
stream, Couchbase's connector, Cosmos DB's change feed.

Held to what each promises and no more: a change is handed on only once
every lane has said nothing older is still to come, in commit order,
never twice, and a tail restarted from a saved position reads again what
it had not handed on. Each envelope read as its source documents it.
No server: the lanes here are lists, so what is proved is the order and
the positions, and the envelopes' own shapes.
"""
import base64
import json

import pytest

from migkit import changetopic
from migkit.changetopic import (CouchbaseDcp, CosmosFeed, CrdbChangefeed,
                                DsqlCdc, Follower, Mark, TicdcCanalJson,
                                TicdcSimple, hlc, hlc_text)


class Lanes:
    """Lanes of messages held in lists: a lane's position is the index of
    the next message, as a topic partition's offset is."""
    KIND = "test"

    def __init__(self, lanes):
        self.msgs = {name: list(msgs) for name, msgs in lanes.items()}
        self.reads = 0

    def lanes(self):
        return sorted(self.msgs)

    def ends(self):
        return {name: len(m) for name, m in self.msgs.items()}

    def read(self, at, most, seconds):
        self.reads += 1
        out, nxt = [], {}
        for name in self.lanes():
            pos = int(at.get(name) or 0)
            for i in range(pos, len(self.msgs[name])):
                if len(out) >= most:
                    break
                out.append((name, i, None, self.msgs[name][i], None))
                pos = i + 1
            nxt[name] = pos
        caught = all(nxt[n] >= len(self.msgs[n]) for n in self.msgs)
        return out, nxt, caught

    def lost(self, at):
        return None

    def room(self, at):
        return None

    def close(self):
        return None


def canal(kind, ts, rows, table="orders", db="shop", old=None):
    return json.dumps({"database": db, "table": table, "isDdl": False,
                       "type": kind, "pkNames": ["id"], "data": rows,
                       "old": old, "_tidb": {"commitTs": ts}})


def mark(ts):
    return json.dumps({"type": "TIDB_WATERMARK",
                       "_tidb": {"watermarkTs": ts}})


def follower(lanes, **kw):
    return Follower(TicdcCanalJson(), lanes,
                    name=lambda row: row.table, **kw)


def back(table, rows, version):
    """A read back that answers each key as the feed's last change said."""
    return [(r, {"id": int(r.key["id"])},
             None if r.op == "delete" else {"id": int(r.key["id"]),
                                            "at": version})
            for r in rows]


# ---- the order -------------------------------------------------------------

def test_nothing_goes_until_every_lane_has_said_how_far_it_has_come():
    lanes = Lanes({"p0": [canal("INSERT", 20, [{"id": "2"}])],
                   "p1": [canal("INSERT", 10, [{"id": "1"}])]})
    f = follower(lanes, read_back=back)
    token = f.point(fence=lambda: 5)
    token["at"] = {"p0": 0, "p1": 0}
    got, token = f.changes(token, 100)
    # p0 holds a change at 20 and no lane has said anything: p1 may still
    # carry one at 15, which has to go first
    assert got == []
    assert token["at"] == {"p0": 0, "p1": 0}
    lanes.msgs["p0"].append(mark(30))
    lanes.msgs["p1"].append(mark(15))
    got, token = f.changes(token, 100)
    # only what both lanes have passed: 10, and 20 waits for p1
    assert [c["key"] for c in got] == [{"id": 1}]
    assert token["fence"] == 15
    lanes.msgs["p1"].append(mark(40))
    got, token = f.changes(token, 100)
    assert [c["key"] for c in got] == [{"id": 2}]
    assert token["fence"] == 30


def test_changes_go_in_commit_order_whichever_lane_carried_them():
    lanes = Lanes({"p0": [canal("UPDATE", 30, [{"id": "7"}]),
                          canal("INSERT", 12, [{"id": "8"}]), mark(50)],
                   "p1": [canal("INSERT", 20, [{"id": "9"}]), mark(50)]})
    f = follower(lanes, read_back=back)
    token = {"via": "test", "at": {}, "fence": 5, "marks": {}, "seen": {}}
    got, _ = f.changes(token, 100)
    assert [(c["key"]["id"], c["txn"]) for c in got] == [(8, 12), (9, 20),
                                                        (7, 30)]


def test_a_change_delivered_twice_is_handed_on_once():
    twice = canal("INSERT", 20, [{"id": "1"}])
    lanes = Lanes({"p0": [twice, twice, mark(25)]})
    f = follower(lanes, read_back=back)
    got, token = f.changes({"at": {}, "fence": 0, "marks": {}}, 100)
    assert len(got) == 1
    # and again after it was handed on: at or below the fence
    lanes.msgs["p0"] += [twice, mark(30)]
    got, _ = f.changes(token, 100)
    assert got == []


def test_a_change_the_copy_already_read_is_left_out():
    lanes = Lanes({"p0": [canal("INSERT", 9, [{"id": "1"}]),
                          canal("INSERT", 11, [{"id": "2"}]), mark(20)]})
    f = follower(lanes, read_back=back)
    got, _ = f.changes({"at": {}, "fence": 10, "marks": {}}, 100)
    assert [c["key"] for c in got] == [{"id": 2}]


def test_a_restart_reads_again_what_was_held_back():
    lanes = Lanes({"p0": [canal("INSERT", 10, [{"id": "1"}]),
                          canal("INSERT", 40, [{"id": "2"}]), mark(20)],
                   "p1": [mark(50)]})
    f = follower(lanes, read_back=back)
    got, token = f.changes({"at": {}, "fence": 0, "marks": {}}, 100)
    assert [c["key"] for c in got] == [{"id": 1}]
    # the change at 40 is held: the position saved points at it, not
    # past it, so a tail started again from here reads it again
    assert token["at"]["p0"] == 1
    fresh = follower(lanes, read_back=back)
    lanes.msgs["p0"].append(mark(60))
    got, _ = fresh.changes(token, 100)
    assert [c["key"] for c in got] == [{"id": 2}]


def test_the_fence_moves_on_with_no_change_at_all():
    lanes = Lanes({"p0": [mark(100)], "p1": [mark(90)]})
    f = follower(lanes, read_back=back)
    got, token = f.changes({"at": {}, "fence": 5, "marks": {}}, 100)
    assert got == [] and token["fence"] == 90
    assert Follower.reached(token, {"fence": 90}) is True
    assert Follower.reached(token, {"fence": 91}) is False


def test_a_batch_is_never_cut_inside_one_commit():
    rows = [{"id": str(i)} for i in range(5)]
    lanes = Lanes({"p0": [canal("INSERT", 10, rows),
                          canal("INSERT", 11, [{"id": "9"}]), mark(20)]})
    f = follower(lanes, read_back=back)
    got, token = f.changes({"at": {}, "fence": 0, "marks": {}}, 2)
    # five changes of one commit go together, however small the batch
    assert sorted(c["key"]["id"] for c in got) == [0, 1, 2, 3, 4]
    assert token["fence"] == 10
    got, token = f.changes(token, 2)
    assert [c["key"]["id"] for c in got] == [9]


def test_a_feed_that_sends_no_marks_is_said(monkeypatch):
    monkeypatch.setattr(changetopic, "STALL", 0)
    lanes = Lanes({"p0": [canal("INSERT", 10, [{"id": "1"}])]})
    f = follower(lanes, read_back=back)
    with pytest.raises(SystemExit) as e:
        f.changes({"at": {}, "fence": 0, "marks": {}}, 100)
    assert "enable-tidb-extension=true" in str(e.value)


def test_a_stream_with_no_marks_is_released_by_what_it_has_published():
    """DSQL's stream says nothing of how far it has come: `delivered`,
    asked before a read that reaches every lane's end, is the mark."""
    def rec(ts, key):
        return json.dumps({"type": "full", "op": "c", "before": None,
                           "after": {"id": key},
                           "source": {"ts_ns": ts, "txId": f"t{ts}",
                                      "schema": "public", "table": "t",
                                      "db": "postgres"}})
    lanes = Lanes({"s0": [rec(300, 3), rec(100, 1)], "s1": [rec(200, 2)]})
    said = {"now": 150}
    f = Follower(DsqlCdc(), lanes, name=lambda row: row.table,
                 shape=lambda t, row: (dict(row.key), dict(row.values)),
                 delivered=lambda: said["now"])
    got, token = f.changes({"at": {}, "fence": 0, "marks": {}}, 100)
    assert [c["key"]["id"] for c in got] == [1]
    said["now"] = 1000
    got, token = f.changes(token, 100)
    assert [c["key"]["id"] for c in got] == [2, 3]


def test_a_read_back_is_asked_at_the_batchs_own_fence():
    asked = []

    def read(table, rows, version):
        asked.append((table, sorted(r.key["id"] for r in rows), version))
        return back(table, rows, version)
    lanes = Lanes({"p0": [canal("INSERT", 10, [{"id": "1"}]),
                          canal("UPDATE", 12, [{"id": "1"}]),
                          canal("DELETE", 14, [{"id": "2"}]), mark(20)]})
    got, _ = follower(lanes, read_back=read).changes(
        {"at": {}, "fence": 0, "marks": {}}, 100)
    # one read for the batch, each key once, at the last commit it holds
    assert asked == [("orders", ["1", "2"], 14)]
    assert {c["op"] for c in got if c["key"] == {"id": 2}} == {"delete"}
    assert [c["values"] for c in got if c["key"] == {"id": 1}] == [
        {"id": 1, "at": 14}]


# ---- one order per lane ------------------------------------------------------

def dcp(vb, seq, key, body=None, uuid=7, event="mutation"):
    payload = {"event": event, "partition": vb, "key": key, "bySeqno": seq,
               "vBucketUuid": uuid, "bucket": "shop", "scope": "inv",
               "collection": "items",
               "content": base64.b64encode(json.dumps(body).encode())
               .decode() if body is not None else None}
    return json.dumps({"schema": {"type": "struct"}, "payload": payload})


def test_a_vbuckets_changes_go_in_its_order_and_a_redelivery_is_dropped():
    lanes = Lanes({"k0": [dcp(1, 5, "a", {"n": 1}), dcp(1, 6, "a", {"n": 2}),
                          dcp(1, 5, "a", {"n": 1})]})
    f = Follower(CouchbaseDcp(), lanes, name=lambda row: row.table,
                 shape=lambda t, row: (dict(row.key), row.values))
    got, token = f.changes({"at": {}, "seen": {"vb1": [7, 4]}}, 100)
    # the third is the first delivered again, older than what vb1 handed on
    assert [c["values"] for c in got] == [{"n": 1}, {"n": 2}]
    assert token["seen"]["vb1"] == [7, 6]
    assert Follower.reached(token, {"seen": {"vb1": [7, 6]}}) is True
    assert Follower.reached(token, {"seen": {"vb1": [7, 7]}}) is False
    # a vBucket failed over is another incarnation: never reached by the
    # old one's numbers
    assert Follower.reached(token, {"seen": {"vb1": [8, 1]}}) is False


def test_a_vbucket_that_failed_over_is_read_on():
    lanes = Lanes({"k0": [dcp(1, 3, "a", {"n": 9}, uuid=8)]})
    f = Follower(CouchbaseDcp(), lanes, name=lambda row: row.table,
                 shape=lambda t, row: (dict(row.key), row.values))
    got, token = f.changes({"at": {}, "seen": {"vb1": [7, 50]}}, 100)
    # sequence 3 under a new UUID is not below 50 of the old one
    assert [c["values"] for c in got] == [{"n": 9}]
    assert token["seen"]["vb1"] == [8, 3]


# ---- the envelopes, as their sources document them -----------------------

def test_canal_json_names_both_keys_of_an_update_that_moved_the_key():
    got = TicdcCanalJson().decode(None, canal(
        "UPDATE", 42, [{"id": "2", "v": "x"}], old=[{"id": "1", "v": "x"}]))
    assert [(r.op, r.key, r.version) for r in got] == [
        ("upsert", {"id": "2"}, 42), ("delete", {"id": "1"}, 42)]


def test_canal_json_without_the_commit_time_is_refused():
    msg = json.dumps({"database": "shop", "table": "t", "type": "INSERT",
                      "pkNames": ["id"], "data": [{"id": "1"}]})
    with pytest.raises(SystemExit) as e:
        TicdcCanalJson().decode(None, msg)
    assert "enable-tidb-extension=true" in str(e.value)


def test_canal_json_carries_bytes_as_one_character_a_byte():
    assert changetopic.from_text("bytes", "\u0005ÿ\n") == b"\x05\xff\n"


def test_ticdcs_simple_protocol_has_its_own_watermark():
    env = TicdcSimple()
    (m,) = env.decode(None, json.dumps({"type": "WATERMARK",
                                        "commitTs": 99}))
    assert isinstance(m, Mark) and m.version == 99
    (r,) = env.decode(None, json.dumps({
        "type": "DELETE", "database": "shop", "table": "t", "commitTs": 5,
        "old": {"id": "3"}}))
    assert (r.op, r.key, r.version) == ("delete", {"id": "3"}, 5)


def test_a_changefeeds_timestamps_order_as_the_cluster_orders_them():
    a, b = "1701102296662969433.0000000002", "1701102296662969433.0000000010"
    assert hlc(a) < hlc(b) < hlc("1701102296662969434.0000000000")
    assert hlc_text(hlc(b)) == b
    env = CrdbChangefeed()
    (m,) = env.decode(None, json.dumps({"resolved": b}))
    assert m.version == hlc(b)
    (r,) = env.decode(b"[5, \"x\"]", json.dumps({"after": None,
                                                 "updated": a}), "orders")
    assert (r.op, r.table, r.key, r.version) == ("delete", "orders",
                                                 {"0": 5, "1": "x"}, hlc(a))
    with pytest.raises(SystemExit):
        env.decode(b"[1]", json.dumps({"after": {"id": 1}}), "orders")


def test_dsqls_oversized_record_is_put_back_together_and_checked():
    image = json.dumps({"id": 1, "blob": "y" * 50})
    crc = changetopic._crc32c(image.encode())
    main = {"type": "chunked", "op": "c", "before": None, "after": None,
            "source": {"ts_ns": 7, "txId": "a", "schema": "public",
                       "table": "t", "db": "postgres"},
            "chunked": {"after": {"chunk_id": "c1", "total_fragments": 2,
                                  "crc32c": str(crc)}}}
    frags = [{"type": "fragment", "chunk_id": "c1", "index": i,
              "data": image[i * 30:(i + 1) * 30]} for i in (1, 0)]
    env = DsqlCdc()
    assert env.decode(None, json.dumps(frags[0])) == []
    assert env.decode(None, json.dumps(main)) == []
    (r,) = env.decode(None, json.dumps(frags[1]))
    assert r.values == json.loads(image) and r.table == "public.t"
    # the same pieces, one of them changed: refused, nothing applied
    bad = DsqlCdc()
    bad.decode(None, json.dumps(main))
    bad.decode(None, json.dumps(frags[0]))
    wrong = dict(frags[1], data=frags[1]["data"].replace("1", "2"))
    with pytest.raises(SystemExit):
        bad.decode(None, json.dumps(wrong))


def test_a_dsql_delete_names_its_key_or_is_refused():
    env = DsqlCdc()
    src = {"ts_ns": 9, "schema": "public", "table": "t", "db": "postgres"}
    (r,) = env.decode(None, json.dumps({"type": "full", "op": "d",
                                        "before": {"id": 4}, "after": None,
                                        "source": src}))
    assert (r.op, r.key) == ("delete", {"id": 4})
    with pytest.raises(SystemExit):
        env.decode(None, json.dumps({"type": "full", "op": "d",
                                     "before": None, "after": None,
                                     "source": src}))


def test_cosmos_all_versions_and_deletes_names_a_deleted_items_key():
    env = CosmosFeed({"orders": ["region"]})
    (d,) = env.decode(None, {"metadata": {
        "operationType": "delete", "lsn": 12, "crts": 1, "id": "o1",
        "partitionKey": {"region": "eu"}}}, "orders")
    assert (d.op, d.key, d.version) == ("delete", {"id": "o1",
                                                   "region": "eu"}, 12)
    (u,) = env.decode(None, {"current": {"id": "o2", "region": "us",
                                         "n": 1, "_rid": "x", "_ts": 5},
                             "metadata": {"operationType": "replace",
                                          "lsn": 13}}, "orders")
    assert u.values == {"id": "o2", "region": "us", "n": 1}
    assert u.key == {"id": "o2", "region": "us"} and u.version == 13


def test_cosmos_latest_version_is_the_item_as_it_is():
    (u,) = CosmosFeed({"t": []}).decode(None, {"id": "a", "v": 2,
                                               "_lsn": 40, "_etag": "e"},
                                        "t")
    assert (u.op, u.values, u.version) == ("upsert", {"id": "a", "v": 2},
                                           40)


def test_a_cosmos_continuation_is_read_for_its_positions():
    from migkit.engines.cosmosdb import CosmosEngine, lsns
    token = base64.b64encode(json.dumps({"continuation": {"Continuation": [
        {"token": "\"17\"", "range": {"min": "", "max": "7F"}},
        {"token": "\"23\"", "range": {"min": "7F", "max": "FF"}}]}})
        .encode()).decode()
    assert sorted(lsns(token)) == [17, 23]
    assert lsns("\"41\"") == [41]
    want = {"at": {"r": "\"20\""}}
    assert CosmosEngine.position_reached({"at": {"r": token}}, want) \
        is False
    assert CosmosEngine.position_reached({"at": {"r": "\"20\""}}, want) \
        is True
