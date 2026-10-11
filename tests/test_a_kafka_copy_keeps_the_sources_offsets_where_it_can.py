"""A copied Kafka topic carries the source's own offsets where that can be
exact, and says so where it cannot (backlog LT.C2).

A consumer group's committed offset means one message on the cluster it was
committed on. Where the source's offsets run one to one from the partition's
first - no compaction hole, no transaction marker, no aborted write - the
target partition can have the same offsets: its first offset is made the
source's (Kafka has no call that sets a log's first offset; the target is
padded with throwaway records up to it and `DeleteRecords` then moves its
start there) and the copy is not transactional, since a transaction's
marker would take an offset. Every landed offset is read back and held to
the source's.

Where the source has a gap the offsets are not claimed identical: the
partition is copied the other way, with the reason said, and nothing is
padded for it.

No broker is reached: the clusters are `tests/kafka_fake.py`'s, which model
what the offsets depend on (control records, aborted writes, the last
stable offset, `DeleteRecords`, a producer's id taking over a transaction).
What a real broker does with the same requests is not proved here.
"""
import json

import pytest

from migkit.engines.kafka import KafkaEngine
from tests import kafka_fake as kf

#: what this module proves, item by engine (`capabilities.proofs`): none
#: of the depth matrix's items is a topic's offsets
PROVES = []


@pytest.fixture(autouse=True)
def _kafka(monkeypatch):
    kf.install(monkeypatch)


def _clusters(n=0, start=0, partitions=1):
    src, dst = kf.Cluster("src"), kf.Cluster("dst")
    src.topic("orders", partitions)
    dst.topic("orders", partitions)
    for p in range(partitions):
        src.put_many("orders", p, n)
        src.log("orders", p).start = start
    return src, dst


def _same_offsets(src, dst, p=0):
    return dst.messages("orders", p) == src.messages("orders", p)


# ---- the decision ---------------------------------------------------------

FACTS = dict(sbeg=0, dbeg=0, dend=0, gap_free=True)


def _choose(way="auto", **over):
    return KafkaEngine.choose_offsets(way, **{**FACTS, **over})


def test_identical_where_the_target_is_empty_and_the_source_has_no_gap():
    mode, why = _choose()
    assert mode == "identical", why
    assert "checked as it lands" in why
    mode, why = _choose(sbeg=40, gap_free=True)
    assert mode == "identical" and "padded with 40" in why, why


def test_a_source_not_read_is_identical_only_where_nothing_is_padded():
    """Not read means every offset is proved as it lands; with padding
    owed, the source is read first, so None never reaches here with one."""
    assert _choose(gap_free=None)[0] == "identical"


@pytest.mark.parametrize("facts, said", [
    (dict(gap_free=False, gap_why="offset 9 holds no message"),
     "not consecutive: offset 9 holds no message"),
    (dict(dbeg=0, dend=5), "already holds 5 messages"),
    (dict(sbeg=3, dbeg=7, dend=7), "past the source's first 3"),
    (dict(sbeg=3, can_pad=False, pad_why="the target does not allow its"
                                         " records to be deleted"),
     "does not allow its records to be deleted"),
])
def test_anything_else_is_not_claimed_identical_and_says_why(facts, said):
    mode, why = _choose(**facts)
    assert mode == "plain", (mode, why)
    assert said in why
    assert "placed by message" in why


def test_the_gap_falls_back_to_the_map_only_where_the_hop_asks_and_can():
    gap = dict(gap_free=False, gap_why="a marker")
    assert _choose("auto", **gap)[0] == "plain"
    assert _choose("map", transactions=True, **gap)[0] == "mapped"
    mode, why = _choose("map", transactions=False, **gap)
    assert mode == "plain" and "cannot run transactions" in why
    assert _choose("off")[0] == "plain"
    assert _choose("map", transactions=True)[0] == "identical"


# ---- the copy -------------------------------------------------------------

def test_a_gap_free_topic_from_zero_lands_at_the_same_offsets():
    src, dst = _clusters(12)
    c = _copy(src, dst)
    assert _same_offsets(src, dst)
    assert c.partition()["mode"] == "identical"
    assert dst.deleted == [] and dst.log("orders").end == 12
    assert [t for t in dst.topics if t.startswith("__")] == [
        "__migkit_offset_map"]


def _copy(src, dst, **options):
    c = kf.Copy(src, dst, **options)
    c.run()
    return c


def test_a_prefix_the_source_deleted_is_padded_and_cut_so_offsets_agree():
    """The source's log starts at 5 of 12 (retention took the first five):
    the target is padded to 5, cut there, and holds the other seven at
    offsets 5 to 11 - the same as the source's."""
    src, dst = _clusters(12, start=5)
    c = _copy(src, dst)
    log = dst.log("orders")
    assert (log.start, log.end) == (5, 12)
    assert _same_offsets(src, dst)
    assert [m[0] for m in dst.messages("orders")] == list(range(5, 12))
    # the cut is asked once for real; the probe before it deletes nothing
    assert [o for _, o in dst.deleted] == [0, 5]
    assert "padded with 5 records" in " ".join(c.said)


def test_the_padding_is_sent_in_rounds_and_none_of_it_is_left_readable(
        monkeypatch):
    monkeypatch.setattr(KafkaEngine, "PAD_ROUND", 4)
    src, dst = _clusters(10, start=9)
    _copy(src, dst)
    assert dst.messages("orders") == src.messages("orders")
    assert all(k != b"migkit-pad" for _, k, _ in dst.messages("orders"))
    assert dst.log("orders").start == 9


def test_every_partition_of_a_topic_is_decided_on_its_own():
    src, dst = _clusters(8, start=2, partitions=2)
    src.log("orders", 1).recs[4].kind = "control"      # a hole in the second
    c = _copy(src, dst)
    assert c.partition(0)["mode"] == "identical"
    assert c.partition(1)["mode"] == "plain"
    assert _same_offsets(src, dst, 0)
    assert not _same_offsets(src, dst, 1)
    # the one that is not claimed was not padded for
    assert dst.log("orders", 1).start == 0
    assert dst.log("orders", 1).end == len(src.messages("orders", 1))


def test_a_source_with_a_hole_is_not_claimed_identical_and_not_padded():
    src, dst = _clusters(10, start=3)
    src.log("orders").recs[6].kind = "control"          # compacted away
    c = _copy(src, dst)
    assert c.partition()["mode"] == "plain"
    assert "not consecutive" in " ".join(c.said)
    assert dst.log("orders").end == len(src.messages("orders"))
    assert dst.log("orders").start == 0
    # nothing was cut but the probe's no-op
    assert [o for _, o in dst.deleted] == [0]
    # every message arrived, in order, one for one
    assert [(k, v) for _, k, v in dst.messages("orders")] == \
        [(k, v) for _, k, v in src.messages("orders")]


def test_a_source_with_transaction_markers_is_not_claimed_identical():
    src = kf.Cluster("src")
    dst = kf.Cluster("dst")
    src.topic("orders")
    dst.topic("orders")
    src.put_many("orders", 0, 4)
    src.transaction("orders", 0, 3)                     # three and a marker
    src.put_many("orders", 0, 2, prefix=b"after")
    src.log("orders").start = 2
    c = _copy(src, dst)
    assert c.partition()["mode"] == "plain"
    assert "not consecutive" in " ".join(c.said)
    assert not _same_offsets(src, dst)
    assert len(dst.messages("orders")) == len(src.messages("orders"))


def test_a_gap_that_comes_after_nothing_was_padded_ends_identity_there():
    """Nothing to pad, so the source is not read first; each offset is
    proved as it lands. The seven before the marker are identical, the
    marker's offset holds no message, and the map says exactly that - no
    more."""
    src, dst = _clusters(4)
    src.transaction("orders", 0, 3)
    src.put_many("orders", 0, 2, prefix=b"after")
    c = _copy(src, dst)
    assert c.partition()["mode"] == "plain"
    assert c.partition()["identical_until"] == 7
    said = " ".join(c.said)
    assert "identical below 7 and not from there" in said
    assert [m[0] for m in dst.messages("orders")][:7] == list(range(7))
    assert len(dst.messages("orders")) == len(src.messages("orders"))
    # the two after the marker are at 7 and 8 on the target, 8 and 9 on
    # the source
    assert [m[0] for m in dst.messages("orders")][7:] == [7, 8]
    kept = c.eng._read_offset_map()[("orders", 0)]
    assert kept["identical"] == (0, 7)
    # a position below the end of identity is its own number; one past it
    # is not claimed from the map
    assert c.eng._map_position(kept, 3, 10)[0] == 3
    assert c.eng._map_position(kept, 8, 10) is None


def test_a_target_that_holds_messages_is_not_claimed_identical():
    src, dst = _clusters(6)
    dst.put_many("orders", 0, 2, prefix=b"theirs")
    c = _copy(src, dst)
    assert c.partition()["mode"] == "plain"
    assert "already holds 2 messages" in " ".join(c.said)
    assert dst.log("orders").end == 8
    assert dst.deleted == []


def test_a_target_that_will_not_delete_is_not_padded_at_all():
    """A padding that could not be cut would stay on the target for its
    consumers to read: the probe asks first, with a request that deletes
    nothing, and a refusal is a plain copy."""
    src, dst = _clusters(8, start=4)
    dst.deny_delete = True
    c = _copy(src, dst)
    assert c.partition()["mode"] == "plain"
    assert "does not allow its records to be deleted" in " ".join(c.said)
    assert dst.log("orders").end == len(src.messages("orders"))
    assert all(r.key != b"migkit-pad" for r in dst.log("orders").recs)


def test_a_padding_past_the_hops_limit_is_not_made():
    src, dst = _clusters(8, start=6)
    c = _copy(src, dst, offsets_pad_limit=5)
    assert c.partition()["mode"] == "plain"
    assert "past offsets_pad_limit 5" in " ".join(c.said)
    assert all(r.key != b"migkit-pad" for r in dst.log("orders").recs)


def test_offsets_off_copies_as_before():
    src, dst = _clusters(6)
    c = _copy(src, dst, offsets="off")
    assert c.partition()["mode"] == "plain"
    assert not any(t.startswith("__") for t in dst.topics)


def test_the_hop_option_is_a_declared_key():
    from migkit import hopkeys
    for name in ("offsets", "offsets_pad_limit"):
        assert name in hopkeys.HOP_OPTIONS
    assert hopkeys.HOP_OPTIONS["offsets"].choices == ("auto", "map", "off")


# ---- crash, replay -------------------------------------------------------

def _crash_on(cluster, event, nth, after=None):
    """Raise `Crash` on the `nth` time `event` fires (counted from the
    first call after `after()` is true)."""
    seen = [0]

    def hook(*args):
        if after is None or after():
            seen[0] += 1
            if seen[0] == nth:
                raise kf.Crash()
    cluster.hooks[event].append(hook)
    return hook


def test_a_stop_in_the_middle_of_the_padding_goes_on_where_the_target_is(
        monkeypatch):
    monkeypatch.setattr(KafkaEngine, "PAD_ROUND", 3)
    src, dst = _clusters(16, start=10)
    c = kf.Copy(src, dst)
    _crash_on(dst, "flush", 2)
    with pytest.raises(kf.Crash):
        c.run()
    assert dst.log("orders").end == 6               # two rounds of three
    dst.hooks.clear()
    c.restart()
    c.run()
    assert dst.log("orders").start == 10
    assert _same_offsets(src, dst)
    assert [m[0] for m in dst.messages("orders")] == list(range(10, 16))


def test_a_batch_that_landed_and_was_not_saved_is_not_sent_twice():
    src, dst = _clusters(9)
    c = kf.Copy(src, dst)
    # the first batch of three has landed when the process goes; its
    # position was not saved
    _crash_on(dst, "flush", 1, after=lambda: dst.log("orders").end >= 3)
    with pytest.raises(kf.Crash):
        c.run(chunk=3)
    assert c.partition()["next"] == 0 and dst.log("orders").end == 3
    dst.hooks.clear()
    c.restart()
    c.run(chunk=3)
    assert _same_offsets(src, dst)
    assert dst.log("orders").end == 9
    assert c.partition()["next"] == 9 and c.partition()["copied"] == 9


def test_repeated_resume_changes_nothing():
    src, dst = _clusters(7, start=2)
    c = kf.Copy(src, dst)
    c.run()
    first = [(r.offset, r.key, r.value) for r in dst.log("orders").recs]
    for _ in range(3):
        c.restart()
        c.run()
    assert [(r.offset, r.key, r.value)
            for r in dst.log("orders").recs] == first


def test_a_growing_source_goes_on_at_the_same_offsets():
    src, dst = _clusters(5)
    c = kf.Copy(src, dst)
    c.run()
    src.put_many("orders", 0, 4, prefix=b"late")
    c.run()
    assert _same_offsets(src, dst)
    assert c.partition()["mode"] == "identical"


def test_another_writer_on_the_target_stops_the_copy_and_ends_the_claim():
    """A message that lands at another offset than its own is a target
    someone else writes to. Identical offsets hold below it and no further;
    the copy stops rather than go on."""
    src, dst = _clusters(8)
    c = kf.Copy(src, dst)
    once = []

    def intruder(producer, topic, p, rec):
        if topic == "orders" and rec.offset == 3 and not once:
            once.append(1)
            dst.put("orders", 0, b"x", b"foreign")
    dst.hooks["send"].append(intruder)
    with pytest.raises(SystemExit) as e:
        c.run()
    assert "another writer is on the partition" in str(e.value)
    kept = c.eng._read_offset_map()[("orders", 0)]
    assert kept["identical"] == (0, 4)
    # and a later start does not go on over it
    dst.hooks.clear()
    c.restart()
    with pytest.raises(SystemExit):
        c.run()


def test_a_target_that_changed_under_a_resume_is_not_vouched_for():
    src, dst = _clusters(6)
    c = kf.Copy(src, dst)
    c.run()
    src.put_many("orders", 0, 2, prefix=b"more")
    dst.put("orders", 0, b"x", b"foreign")            # lands at 6, a stranger
    c.restart()
    with pytest.raises(SystemExit) as e:
        c.run()
    assert "identical offsets cannot be vouched for" in str(e.value) or \
        "is not the source's" in str(e.value)


# ---- what a consumer group is given --------------------------------------

def test_a_group_behind_the_copy_is_placed_at_its_own_number_and_said():
    src, dst = _clusters(12, start=5)
    c = kf.Copy(src, dst)
    c.run(chunk=7)
    # only part of the topic is on the target when the group is placed
    src.put_many("orders", 0, 4, prefix=b"late")
    tp = kf.TopicPartition("orders", 0)
    src.groups["billing"] = {tp: 8}
    rows, unsure, _ = c.eng.group_positions("cluster")
    assert unsure == []
    (row,) = rows
    assert (row["source"], row["target"]) == (8, 8)
    assert "identical offsets" in row["how"] or row["how"] == "number"


def test_a_group_is_not_moved_by_the_number_where_offsets_differ():
    """A hole at 3 of ten: the target ends one short of the source and
    the same-number shortcut does not apply; the group committed at 7 is
    found by its message, at 6."""
    src, dst = _clusters(10)
    src.log("orders").recs[3].kind = "control"
    c = _copy(src, dst)
    assert c.partition()["mode"] == "plain"
    assert dst.log("orders").end == src.log("orders").end - 1
    assert c.eng._read_offset_map()[("orders", 0)]["identical"] == (0, 3)
    src.groups["billing"] = {kf.TopicPartition("orders", 0): 7}
    rows, unsure, _ = c.eng.group_positions("cluster")
    (row,) = rows
    assert row["how"] != "number"
    assert (row["source"], row["target"]) == (7, 6)


def test_a_target_behind_or_ahead_of_the_copys_position_is_not_vouched_for():
    src, dst = _clusters(6)
    c = kf.Copy(src, dst)
    c.run()
    # the target lost its tail: the copy says it reached 6 and the target
    # ends at 4
    del dst.log("orders").recs[4:]
    c.restart()
    src.put_many("orders", 0, 2, prefix=b"more")
    with pytest.raises(SystemExit) as e:
        c.run()
    assert "identical offsets cannot be vouched for" in str(e.value)
    # and the target far past the whole source
    src2, dst2 = _clusters(6)
    c2 = kf.Copy(src2, dst2)
    c2.run()
    dst2.put_many("orders", 0, 9, prefix=b"stranger")
    c2.restart()
    src2.put_many("orders", 0, 2, prefix=b"more")
    with pytest.raises(SystemExit) as e:
        c2.run()
    assert "identical offsets cannot be vouched for" in str(e.value)


def test_a_plain_copy_still_counts_off_the_targets_own_end_after_a_stop():
    """The way a copy went before offsets were kept, still: a target that
    holds its own messages, a batch that landed and was not saved, and the
    resume sends it once."""
    src, dst = _clusters(9)
    dst.put_many("orders", 0, 2, prefix=b"theirs")
    c = kf.Copy(src, dst)
    _crash_on(dst, "flush", 1, after=lambda: dst.log("orders").end >= 5)
    with pytest.raises(kf.Crash):
        c.run(chunk=3)
    assert c.partition()["next"] == 0 and dst.log("orders").end == 5
    dst.hooks.clear()
    c.restart()
    c.run(chunk=3)
    got = [(k, v) for _, k, v in dst.messages("orders")][2:]
    assert got == [(k, v) for _, k, v in src.messages("orders")]
    assert c.partition()["mode"] == "plain"
