"""Where a topic's offsets cannot be kept, the offset map is written in the
transaction of the batch it describes (backlog LT.C3).

With `offsets: map`, a partition whose source offsets have gaps (a
compaction hole, a transaction's marker, an aborted write) is copied one
batch to one producer transaction on the target: the batch's messages and
the record of the source and target offsets they took are committed
together or not at all. So the map cannot say a message is somewhere it is
not, or miss one that is there, whatever stops the copy; the copy goes on
from the map rather than from a counter that can be a batch behind; and
the map is read back, whole, to place a consumer group's offsets at a
cutover (`group_positions`).

No broker is reached: the clusters are `tests/kafka_fake.py`'s. They model
transactions as a broker does - an open write stops the last stable offset,
a commit or abort takes an offset on every partition it touched, a producer
with the same id ends what the last left open - but a real broker's
handling of the same calls (and the client's own timing and fencing) is not
proved here.
"""
import json

import pytest

from migkit.engines.kafka import KafkaEngine
from tests import kafka_fake as kf

#: what this module proves, item by engine (`capabilities.proofs`): none
#: of the depth matrix's items is a topic's offsets
PROVES = []

TP = kf.TopicPartition("orders", 0)


@pytest.fixture(autouse=True)
def _kafka(monkeypatch):
    kf.install(monkeypatch)


def _gappy(identical_first=0):
    """A source that cannot be copied offset for offset: `identical_first`
    plain messages, then a transaction and its marker, a hole, a rolled
    back transaction, and more messages."""
    src, dst = kf.Cluster("src"), kf.Cluster("dst")
    src.topic("orders")
    dst.topic("orders")
    if identical_first:
        src.put_many("orders", 0, identical_first, prefix=b"a")
    else:
        src.hole("orders", 0, 1)
    src.transaction("orders", 0, 3, prefix=b"tx")
    src.put_many("orders", 0, 3, prefix=b"b")
    src.hole("orders", 0, 2)
    src.transaction("orders", 0, 2, commit=False, prefix=b"gone")
    src.put_many("orders", 0, 4, prefix=b"c")
    return src, dst


def _map_copy(src, dst, **options):
    return kf.Copy(src, dst, offsets="map", **options)


def _each_source_message_is_where_the_map_says(c, src, dst):
    """Every source message, translated by the map, is the same message
    on the target - exact, none searched for."""
    kept = c.eng._read_offset_map()[("orders", 0)]
    end = kf.Consumer(dst, isolation_level="read_committed") \
        .end_offsets([TP])[TP]
    seen = []
    for off, key, value in src.messages("orders"):
        at, _ = c.eng._map_position(kept, off, end)
        got = dst.log("orders").recs[at]
        assert (got.key, got.value) == (key, value), (off, at)
        assert got.state == "committed"
        seen.append(at)
    assert seen == sorted(set(seen))
    return seen


# ---- the map and the batch are one transaction ---------------------------

def test_a_batch_and_its_map_record_are_one_committed_transaction():
    src, dst = _gappy()
    c = _map_copy(src, dst)
    c.run(chunk=4)
    assert c.partition()["mode"] == "mapped"
    data = dst.log("orders").recs
    ledger = dst.log("__migkit_offset_map").recs
    batches = [r for r in ledger if r.kind == "data" and
               json.loads(r.value)["kind"] == "batch"]
    assert len(batches) >= 3
    for rec in batches:
        body = json.loads(rec.value)
        assert rec.state == "committed" and rec.txn
        landed = [data[d + i] for _, d, n in body["runs"] for i in range(n)]
        assert landed
        # the batch's messages are in the very transaction the map record
        # is in - not an earlier one, not a later one
        assert {r.txn for r in landed} == {rec.txn}
        assert all(r.state == "committed" for r in landed)
    # one transactional id for the topic, a transaction per batch
    assert {r.txn.split("#")[0] for r in batches} == {"migkit-copy.orders"}
    assert len({r.txn for r in batches}) == len(batches)


def test_every_source_message_is_where_the_map_says():
    src, dst = _gappy(identical_first=3)
    c = _map_copy(src, dst)
    c.run(chunk=3)
    seen = _each_source_message_is_where_the_map_says(c, src, dst)
    assert len(seen) == len(src.messages("orders"))
    # not claimed identical: the target holds the markers of its own
    assert dst.log("orders").end > len(seen)
    kept = c.eng._read_offset_map()[("orders", 0)]
    # the three plain messages and the transaction's three are consecutive;
    # its marker is the first offset that holds no message
    assert kept["identical"] == (0, 6)


def test_a_message_the_target_refuses_leaves_neither_it_nor_its_map():
    src, dst = _gappy()
    c = _map_copy(src, dst)
    dst.refuse.add("orders")
    with pytest.raises(SystemExit) as e:
        c.run(chunk=4)
    assert "refused" in str(e.value)
    assert c.partition()["next"] == 0
    ledger = dst.log("__migkit_offset_map").recs
    assert not [r for r in ledger if r.kind == "data" and r.state ==
                "committed" and json.loads(r.value)["kind"] == "batch"]
    assert dst.messages("orders") == []


def test_a_map_record_the_target_refuses_leaves_no_message_either():
    src, dst = _gappy()
    c = _map_copy(src, dst)

    def refuse_the_map(producer, topic, p, rec):
        if topic == "orders":
            dst.refuse.add("__migkit_offset_map")
    dst.hooks["send"].append(refuse_the_map)
    with pytest.raises(SystemExit):
        c.run(chunk=4)
    assert dst.messages("orders") == []
    assert c.partition()["next"] == 0
    assert all(r.state == "aborted" for r in dst.log("orders").recs
               if r.kind == "data")


# ---- a crash leaves both or neither, and the replay is the same ----------

def _whole(src, dst, **options):
    c = _map_copy(src, dst, **options)
    c.run(chunk=3)
    return c


def test_a_crash_before_the_commit_leaves_neither_and_the_replay_is_whole():
    src, dst = _gappy(identical_first=2)
    c = _map_copy(src, dst)
    count = [0]

    def die_on_the_second_commit(producer):
        count[0] += 1
        if count[0] == 2:
            raise kf.Crash()
    dst.hooks["commit"].append(die_on_the_second_commit)
    with pytest.raises(kf.Crash):
        c.run(chunk=3)
    # the second batch's messages and its map record are on the target,
    # open, and no reader of committed records is given either
    open_ = [r for r in dst.log("orders").recs if r.state == "open"]
    assert open_
    assert any(r.state == "open" for r in dst.log("__migkit_offset_map").recs)
    kept = c.eng._read_offset_map()[("orders", 0)]
    assert kept["next"] is not None
    first_batch_only = len(dst.messages("orders"))
    assert first_batch_only < len(src.messages("orders"))

    dst.hooks.clear()
    c.restart()
    c.run(chunk=3)
    # the open transaction was ended by the new producer, the batch was
    # sent again once, and the committed target holds every message once
    assert not [r for r in dst.log("orders").recs if r.state == "open"]
    assert [(k, v) for _, k, v in dst.messages("orders")] == \
        [(k, v) for _, k, v in src.messages("orders")]
    _each_source_message_is_where_the_map_says(c, src, dst)
    # and the map has each source offset exactly once
    runs = c.eng._read_offset_map()[("orders", 0)]["runs"]
    covered = [s + i for s, _, n in runs for i in range(n)]
    assert covered == sorted(set(covered))
    ident = c.eng._read_offset_map()[("orders", 0)]["identical"]
    assert ident == (0, 5)
    assert len(covered) + 5 == len(src.messages("orders"))


def test_a_replay_after_a_crash_yields_the_map_an_uninterrupted_run_does():
    """The same source copied once straight through and once with a stop
    after every batch lands in the same place in the only sense that
    matters: every source message maps to an equal message on a committed
    target, and the committed messages are the same in the same order."""
    src, dst = _gappy(identical_first=1)
    straight_src, straight_dst = _gappy(identical_first=1)
    straight = _map_copy(straight_src, straight_dst)
    straight.run(chunk=3)

    c = _map_copy(src, dst)
    crashes = [0]

    def die(producer):
        crashes[0] += 1
        if crashes[0] % 2 == 1:
            raise kf.Crash()
    dst.hooks["commit"].append(die)
    for _ in range(40):
        try:
            c.run(chunk=3)
            break
        except kf.Crash:
            c.restart()
    assert crashes[0] >= 3
    dst.hooks.clear()
    c.restart()
    c.run(chunk=3)
    assert [(k, v) for _, k, v in dst.messages("orders")] == \
        [(k, v) for _, k, v in straight_dst.messages("orders")]
    _each_source_message_is_where_the_map_says(c, src, dst)
    a = c.eng._read_offset_map()[("orders", 0)]
    b = straight.eng._read_offset_map()[("orders", 0)]
    assert a["next"] == b["next"]
    assert a["identical"] == b["identical"]
    assert sum(n for *_, n in a["runs"]) == sum(n for *_, n in b["runs"])


def test_a_batch_committed_and_not_saved_is_counted_and_not_sent_again():
    src, dst = _gappy()
    c = _map_copy(src, dst)
    real = c.save
    gone = []

    def save():
        # the first position saved after a batch was committed is the one
        # that never reaches disk
        ledger = dst.topics.get("__migkit_offset_map", [kf.Log()])[0]
        batches = [r for r in ledger.recs
                   if r.kind == "data" and r.state == "committed"
                   and json.loads(r.value)["kind"] == "batch"]
        if batches and not gone:
            gone.append(1)
            raise kf.Crash()
        real()
    c.save = save
    with pytest.raises(kf.Crash):
        c.run(chunk=3)
    committed = len(dst.messages("orders"))
    assert committed > 0
    saved_next = json.loads(c.saved)["parts"]["0"]["next"]
    c.save = real
    c.restart()
    c.run(chunk=3)
    assert "committed with their offsets and the position was not saved" in \
        " ".join(c.said)
    assert [(k, v) for _, k, v in dst.messages("orders")] == \
        [(k, v) for _, k, v in src.messages("orders")]
    assert json.loads(c.saved)["parts"]["0"]["next"] > saved_next
    _each_source_message_is_where_the_map_says(c, src, dst)


def test_a_position_saved_past_what_the_map_holds_is_not_trusted():
    src, dst = _gappy()
    c = _whole(src, dst)
    ledger = dst.log("__migkit_offset_map").recs
    last = [r for r in ledger if r.kind == "data"][-1]
    last.state = "aborted"                    # the map lost its last batch
    src.put_many("orders", 0, 1, prefix=b"later")
    c.restart()
    with pytest.raises(SystemExit) as e:
        c.run(chunk=3)
    assert "the map is missing what was copied" in str(e.value)


def test_a_growing_source_extends_the_map_and_leaves_what_was_written():
    src, dst = _gappy()
    c = _whole(src, dst)
    before = c.eng._read_offset_map()[("orders", 0)]["runs"]
    src.transaction("orders", 0, 2, prefix=b"new")
    src.put_many("orders", 0, 3, prefix=b"newer")
    c.run(chunk=3)
    after = c.eng._read_offset_map()[("orders", 0)]["runs"]
    assert after[:len(before) - 1] == before[:-1]
    _each_source_message_is_where_the_map_says(c, src, dst)
    assert [(k, v) for _, k, v in dst.messages("orders")] == \
        [(k, v) for _, k, v in src.messages("orders")]


def test_repeated_resume_writes_nothing_more():
    src, dst = _gappy()
    c = _whole(src, dst)
    log = [(r.offset, r.key, r.state) for r in dst.log("orders").recs]
    ledger = [(r.offset, r.key, r.state)
              for r in dst.log("__migkit_offset_map").recs]
    for _ in range(3):
        c.restart()
        c.run(chunk=3)
    assert [(r.offset, r.key, r.state)
            for r in dst.log("orders").recs] == log
    assert [(r.offset, r.key, r.state)
            for r in dst.log("__migkit_offset_map").recs] == ledger


# ---- what is not claimed --------------------------------------------------

def test_a_client_that_cannot_run_transactions_writes_no_map_and_says_so(
        monkeypatch):
    kf.install(monkeypatch, transactions=False)
    src, dst = _gappy()
    c = _map_copy(src, dst)
    c.run(chunk=4)
    assert c.partition()["mode"] == "plain"
    assert "cannot run transactions" in " ".join(c.said)
    assert not any(p.get("transactional_id") for p in dst.producers)
    ledger = dst.topics.get("__migkit_offset_map")
    batches = [r for r in (ledger[0].recs if ledger else [])
               if r.kind == "data" and json.loads(r.value)["kind"] == "batch"]
    assert batches == []
    assert [(k, v) for _, k, v in dst.messages("orders")] == \
        [(k, v) for _, k, v in src.messages("orders")]


def test_by_default_a_gappy_source_is_copied_without_a_transaction():
    src, dst = _gappy()
    c = kf.Copy(src, dst)
    c.run(chunk=4)
    assert c.partition()["mode"] == "plain"
    assert not any(p.get("transactional_id") for p in dst.producers)
    # the target's log has no marker of its own: nothing but the copy
    assert all(r.kind == "data" for r in dst.log("orders").recs)


def test_a_map_that_disagrees_with_itself_is_not_read():
    out = {}
    rec = {"v": 1, "kind": "batch", "topic": "t", "partition": 0,
           "from": 0, "next": 5, "runs": [[0, 0, 5]]}
    KafkaEngine._take_map_record(out, rec)
    with pytest.raises(SystemExit) as e:
        KafkaEngine._take_map_record(out, {**rec, "from": 3, "next": 8,
                                           "runs": [[3, 9, 5]]})
    assert "in two committed batches" in str(e.value)
    with pytest.raises(SystemExit) as e:
        KafkaEngine._take_map_record({}, {**rec, "v": 2})
    assert "version 2" in str(e.value)


def test_runs_are_exact_for_holes_and_merge_where_both_sides_are_consecutive():
    assert KafkaEngine._runs([(4, 10), (5, 11), (6, 12)]) == [[4, 10, 3]]
    assert KafkaEngine._runs([(4, 10), (5, 11), (8, 12), (9, 13)]) == \
        [[4, 10, 2], [8, 12, 2]]
    assert KafkaEngine._runs([(4, 10), (5, 12)]) == [[4, 10, 1], [5, 12, 1]]
    assert KafkaEngine._runs([]) == []


def test_a_position_on_a_hole_is_the_next_message_past_the_end_unknown():
    entry = {"identical": None, "runs": [(4, 10, 2), (9, 13, 2)],
             "next": 11}
    position = KafkaEngine._map_position
    assert position(entry, 5, 99)[0] == 11
    assert position(entry, 6, 99)[0] == 13       # 6, 7, 8 hold no message
    assert position(entry, 8, 99)[0] == 13
    assert position(entry, 10, 99)[0] == 14
    assert position(entry, 11, 99) is None
    assert position(entry, 2, 99)[0] == 10       # before the first message


# ---- at the cutover -------------------------------------------------------

def test_a_groups_offsets_are_placed_by_the_map_and_not_searched_for():
    src, dst = _gappy(identical_first=2)
    c = _whole(src, dst)
    messages = [o for o, _, _ in src.messages("orders")]
    kept = c.eng._read_offset_map()[("orders", 0)]
    end = len(dst.log("orders").recs)
    wanted = {}
    for name, off in (("on-a-message", messages[6]),
                      ("on-a-hole", 9),
                      ("at-the-start", 0),
                      ("in-the-identical-part", 1)):
        src.groups[name] = {TP: off}
        at, _ = c.eng._map_position(kept, off, end)
        wanted[name] = (off, at)
    rows, unsure, checked = c.eng.group_positions("cluster")
    assert unsure == [] and checked == 4
    got = {r["group"]: (r["source"], r["target"], r["how"]) for r in rows}
    for name, (off, at) in wanted.items():
        assert got[name][:2] == (off, at), name
    assert got["in-the-identical-part"][2] == \
        "identical offsets, kept by the copy"
    assert "offset map" in got["on-a-message"][2]
    # the position of a hole is the next message the group reads
    nxt = min(o for o in messages if o >= 9)
    assert dst.log("orders").recs[wanted["on-a-hole"][1]].value == \
        next(v for o, _, v in src.messages("orders") if o == nxt)


def test_equal_ends_on_a_mapped_partition_are_not_taken_for_the_same_number():
    """The target's markers can add up to the source's holes: the ends are
    equal and the messages are not at the same offsets. The map decides."""
    src, dst = kf.Cluster("src"), kf.Cluster("dst")
    src.topic("orders")
    dst.topic("orders")
    src.put_many("orders", 0, 3)
    src.hole("orders", 0, 2)
    src.put_many("orders", 0, 3, prefix=b"n")
    c = _map_copy(src, dst)
    c.run(chunk=3)
    kept = c.eng._read_offset_map()[("orders", 0)]
    src.groups["g"] = {TP: 5}
    rows, _, _ = c.eng.group_positions("cluster")
    (row,) = rows
    assert row["how"] != "number"
    assert row["target"] == c.eng._map_position(
        kept, 5, len(dst.log("orders").recs))[0]


def test_counts_by_offsets_are_not_compared_for_a_mapped_topic():
    """End minus start counts the target's own markers: arithmetic on it
    would be a finding that is not one. It is said, not compared."""
    src, dst = _gappy()
    c = _whole(src, dst)
    results = c.eng.check_counts("cluster")
    assert [r.status for r in results] == ["warn", "ok"] or \
        [r.status for r in results] == ["warn"]
    assert "offset map" in results[0].detail
    assert not any(r.status == "diff" for r in results)
    data = c.eng.check_data("cluster")
    assert any(r.status == "warn" and "offset map" in r.detail
               for r in data)
    assert not any(r.status in ("diff", "error") for r in data)


def test_a_partition_that_ends_identity_goes_on_in_the_map():
    """Seven consecutive messages, then a marker, with the hop asking for
    the map: the seven are the source's own numbers, the rest are mapped."""
    src, dst = kf.Cluster("src"), kf.Cluster("dst")
    src.topic("orders")
    dst.topic("orders")
    src.put_many("orders", 0, 7)
    src.transaction("orders", 0, 0)                     # a marker alone
    src.put_many("orders", 0, 4, prefix=b"n")
    c = kf.Copy(src, dst, offsets="map")
    c.run(chunk=4)
    assert c.partition()["mode"] == "mapped"
    assert c.partition()["identical_until"] == 7
    kept = c.eng._read_offset_map()[("orders", 0)]
    assert kept["identical"] == (0, 7)
    assert kept["runs"]
    _each_source_message_is_where_the_map_says(c, src, dst)
    assert [m[0] for m in dst.messages("orders")][:7] == list(range(7))
