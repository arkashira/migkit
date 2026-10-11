"""A running Kafka tail follows its topics one by one, each in a state of
its own, and a change of a topic goes by the hop's per-topic `ddl`
(backlog P0-PARITY.tail: tail.table-states, tail.added, tail.readded,
tail.ddl).

    live      copied as messages arrive
    joining   added to the running tail: copied up to the source's end
              offsets at that moment (the seam), live from there
    left      out of the scope or deleted on the source: its offsets kept
              for its return, its target topic never touched
    parked    a change the target does not follow yet

A topic put back in the scope, or one that left and returns, is copied
from the offsets it saved - only the messages since - and only if its
target topic is as it was left. A copy resumed past messages the source's
retention took says so: the real client's reset moved such a read to the
log's end, and every message between was passed over without a word.

No broker is reached: the clusters are `tests/kafka_fake.py`'s. Their
logs, offsets, committed reads and the client's reset are modelled; the
real brokers' answers, replication and timing are not proved here.
"""
import json

import pytest

from migkit import topicstates
from tests import kafka_fake as kf

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("tail.table-states", "kafka"), ("tail.added", "kafka"),
          ("tail.readded", "kafka"), ("tail.ddl", "kafka")]


@pytest.fixture(autouse=True)
def _kafka(monkeypatch):
    kf.install(monkeypatch)


class Tail:
    """A tail round after round, its state saved the way the tail saves
    it: only what `save()` was called with survives a stop."""

    def __init__(self, src, dst, where, **options):
        self.src, self.dst, self.where = src, dst, where
        self.options = options
        self.saved = "{}"
        self.said = []
        self.restart()

    def restart(self):
        """A new process: a new engine, a new set, the state as saved."""
        self.eng = kf.engine(self.src, self.dst, where=self.where,
                             **self.options)
        self.state = json.loads(self.saved)
        self.topics = topicstates.TopicSet(self.eng, "cluster", self.state,
                                           self.said.append)

    def save(self):
        self.saved = json.dumps(self.state, sort_keys=True)

    def round(self, scope_changed=False):
        if scope_changed:
            self.topics.file.refresh = lambda: True
        return self.eng._tail_round("cluster", self.topics, self.save,
                                    self.said.append)

    def of(self, topic):
        return self.topics.of(topic)

    def entry(self, topic):
        return self.state[f"topic.{topic}"]


def _clusters(**topics):
    src, dst = kf.Cluster("src"), kf.Cluster("dst")
    for name, (parts, n) in topics.items():
        src.topic(name, parts)
        for p in range(parts):
            src.put_many(name, p, n)
    return src, dst


def _same(src, dst, topic, partitions=1):
    return all(dst.messages(topic, p) == src.messages(topic, p)
               for p in range(partitions))


# ---- the states -----------------------------------------------------------

def test_a_first_round_joins_every_topic_and_leaves_them_live(tmp_path):
    src, dst = _clusters(orders=(1, 6), events=(1, 4))
    tail = Tail(src, dst, tmp_path)
    assert tail.round() == 10
    assert tail.of("orders") == tail.of("events") == topicstates.LIVE
    assert _same(src, dst, "orders") and _same(src, dst, "events")
    assert "joined" in tail.entry("orders")
    assert tail.topics.said() == ""
    # a round with nothing new sends nothing and changes no state
    assert tail.round() == 0


def test_a_copy_that_left_no_state_is_live_and_not_copied_again(tmp_path):
    """The state the copy saves has no word for a topic's state: it is
    live."""
    src, dst = _clusters(orders=(1, 6))
    copy = kf.Copy(src, dst, topic="orders")
    copy.run()
    tail = Tail(src, dst, tmp_path)
    tail.state["topic.orders"] = copy.state
    tail.save()
    tail.restart()
    assert tail.of("orders") == topicstates.LIVE
    assert tail.round() == 0
    assert dst.log("orders").end == 6


def test_the_counts_of_states_are_said_in_a_line(tmp_path):
    src, dst = _clusters(orders=(1, 3), events=(1, 3))
    tail = Tail(src, dst, tmp_path)
    tail.round()
    tail.eng.hop.exclude = ["events"]
    tail.round(scope_changed=True)
    assert tail.topics.counts() == {"live": 1, "left": 1}
    assert tail.topics.said() == "1 left, 1 live"


# ---- a topic added to a running tail --------------------------------------

def test_a_topic_added_to_a_running_tail_joins_at_its_end_offsets_now(
        tmp_path):
    src, dst = _clusters(orders=(1, 5), events=(1, 7))
    tail = Tail(src, dst, tmp_path)
    tail.eng.hop.exclude = ["events"]
    tail.round()
    assert tail.of("events") is None and dst.topics.get("events") is None
    # taken out of the exclude list while the tail runs
    tail.eng.hop.exclude = []
    went = tail.round(scope_changed=True)
    assert went == 7
    assert tail.of("events") == topicstates.LIVE
    assert _same(src, dst, "events") and _same(src, dst, "orders")
    joined = tail.entry("events")["joined"]
    assert json.loads(joined["seam"]) == {"events": {"0": 7}}
    assert any("added to the tail" in m for m in tail.said)


def test_what_arrives_while_a_topic_joins_is_neither_missed_nor_sent_twice(
        tmp_path):
    """Messages written after the seam are the stream's: the topic's copy
    reaches the seam and goes on, and the target ends with every message
    once, in order."""
    src, dst = _clusters(orders=(1, 3))
    tail = Tail(src, dst, tmp_path)
    tail.eng.hop.exclude = ["events"]
    src.topic("events", 1)
    src.put_many("events", 0, 20)
    tail.round()
    tail.eng.hop.exclude = []
    written = []

    def arrive(producer):
        if not written and any(m.startswith("events") for m in tail.said):
            written.append(1)
            src.put_many("events", 0, 5, prefix=b"late")
    dst.hooks["flush"].append(arrive)
    tail.round(scope_changed=True)
    assert written, "the messages were to arrive during the join"
    tail.round()
    assert _same(src, dst, "events")
    assert dst.log("events").end == 25
    assert tail.of("events") == topicstates.LIVE
    assert json.loads(tail.entry("events")["joined"]["seam"]) == {
        "events": {"0": 20}}


def test_a_stop_mid_join_goes_on_from_the_state_it_was_in(tmp_path):
    src, dst = _clusters(orders=(1, 2))
    src.topic("events", 1)
    src.put_many("events", 0, 30)
    tail = Tail(src, dst, tmp_path)
    tail.eng.hop.exclude = ["events"]
    tail.round()
    tail.eng.hop.exclude = []
    flushes = []

    def die(producer):
        flushes.append(1)
        if len(flushes) == 2:
            raise kf.Crash()
    dst.hooks["flush"].append(die)
    with pytest.raises(kf.Crash):
        tail.eng.hop.exclude = []
        tail.round(scope_changed=True)
    # what the stopped process had saved: joining, with its seam
    entry = json.loads(tail.saved)["topic.events"]
    assert entry["state"] == topicstates.JOINING
    assert json.loads(entry["seam"]) == {"events": {"0": 30}}
    dst.hooks["flush"].remove(die)
    tail.restart()
    assert tail.of("events") == topicstates.JOINING
    tail.round()
    assert tail.of("events") == topicstates.LIVE
    assert _same(src, dst, "events")
    assert dst.log("events").end == 30


def test_a_second_resume_changes_nothing(tmp_path):
    src, dst = _clusters(orders=(1, 9))
    tail = Tail(src, dst, tmp_path)
    tail.round()
    for _ in range(3):
        tail.restart()
        assert tail.round() == 0
    assert dst.log("orders").end == 9


def test_a_seam_says_which_side_a_message_is_on(tmp_path):
    src, dst = _clusters(orders=(1, 5))
    eng = kf.engine(src, dst, where=tmp_path)
    mark = eng.snapshot_mark("src", "cluster")
    assert json.loads(mark) == {"orders": {"0": 5}}
    cover = lambda o: eng.mark_covers("src", "cluster", mark,  # noqa: E731
                                      {"topic": "orders", "partition": 0,
                                       "offset": o})
    assert cover(4) is True and cover(5) is False
    assert eng.mark_covers("src", "cluster", mark, {"topic": "x",
                                                    "partition": 0,
                                                    "offset": 0}) is None
    assert eng.mark_covers("src", "cluster", "not a mark", {}) is None


# ---- a topic that left and comes back -------------------------------------

def _left(tmp_path, **options):
    src, dst = _clusters(orders=(1, 4), events=(1, 6))
    tail = Tail(src, dst, tmp_path, **options)
    tail.round()
    tail.eng.hop.exclude = ["events"]
    tail.round(scope_changed=True)
    assert tail.of("events") == topicstates.LEFT
    return src, dst, tail


def test_a_topic_that_left_comes_back_with_only_the_messages_since(tmp_path):
    src, dst, tail = _left(tmp_path)
    assert dst.log("events").end == 6
    src.put_many("events", 0, 5, prefix=b"since")
    assert tail.round() == 0           # away: nothing of it is copied
    tail.eng.hop.exclude = []
    went = tail.round(scope_changed=True)
    assert went == 5
    assert tail.of("events") == topicstates.LIVE
    assert _same(src, dst, "events")
    assert dst.log("events").end == 11
    assert any("only the messages since it left" in m for m in tail.said)
    assert "left" not in tail.entry("events")


def test_a_topic_that_left_while_the_tail_was_stopped_comes_back_too(tmp_path):
    src, dst, tail = _left(tmp_path)
    src.put_many("events", 0, 2, prefix=b"since")
    tail.restart()
    tail.eng.hop.exclude = []
    assert tail.round() == 2
    assert tail.of("events") == topicstates.LIVE and _same(src, dst, "events")


def test_a_target_written_meanwhile_is_not_resumed_over(tmp_path):
    """The copy counts what landed off the target's own end: a message
    written there by someone else would be taken for one the copy sent,
    and a source message skipped."""
    src, dst, tail = _left(tmp_path)
    src.put_many("events", 0, 3, prefix=b"since")
    dst.put("events", 0, b"k", b"someone else's")
    tail.eng.hop.exclude = []
    with pytest.raises(SystemExit) as stop:
        tail.round(scope_changed=True)
    assert "written there since it left" in str(stop.value)
    assert dst.log("events").end == 7        # nothing was copied


def test_a_target_written_meanwhile_parks_the_topic_where_the_hop_says(
        tmp_path):
    src, dst, tail = _left(tmp_path, ddl="park")
    src.put_many("events", 0, 3, prefix=b"since")
    dst.put("events", 0, b"k", b"someone else's")
    src.put_many("orders", 0, 2, prefix=b"more")
    tail.eng.hop.exclude = []
    assert tail.round(scope_changed=True) == 2        # orders goes on
    assert tail.of("events") == topicstates.PARKED
    assert "written there since it left" in tail.entry("events")["why"]
    assert dst.log("events").end == 7
    assert dst.log("orders").end == 6


def test_a_target_that_cannot_be_read_is_not_resumed_over(tmp_path,
                                                          monkeypatch):
    src, dst, tail = _left(tmp_path)
    monkeypatch.setattr(tail.topics, "_target_ends", lambda topic: None)
    tail.eng.hop.exclude = []
    with pytest.raises(SystemExit) as stop:
        tail.round(scope_changed=True)
    assert "could not be read" in str(stop.value)


# ---- a topic deleted or recreated on the source ---------------------------

def test_a_topic_deleted_on_the_source_leaves_and_the_target_keeps_it(
        tmp_path):
    src, dst = _clusters(orders=(1, 4), events=(1, 6))
    tail = Tail(src, dst, tmp_path)
    tail.round()
    del src.topics["events"]
    assert tail.round() == 0
    assert tail.of("events") == topicstates.LEFT
    assert "deleted on the source" in tail.entry("events")["why"]
    assert dst.log("events").end == 6
    assert tail.entry("events")["left"]["target_ends"] == {"0": 6}


def test_a_listing_that_missed_a_topic_for_a_moment_is_not_a_delete(tmp_path):
    """Asked of the topic by name before it is called deleted: a hop whose
    ddl says stop is not stopped by a metadata answer that lacked it."""
    src, dst = _clusters(orders=(1, 4))
    tail = Tail(src, dst, tmp_path, ddl="stop")
    tail.round()
    tail.topics.boundary({}, set(), lambda names: tail.eng._ranges("src",
                                                                   names))
    assert tail.of("orders") == topicstates.LEFT
    assert "taken out of the hop's scope" in tail.entry("orders")["why"]


def test_a_topic_deleted_stops_the_tail_where_the_hop_says_stop(tmp_path):
    src, dst = _clusters(orders=(1, 4))
    tail = Tail(src, dst, tmp_path, ddl="stop")
    tail.round()
    del src.topics["orders"]
    with pytest.raises(SystemExit) as stop:
        tail.round()
    assert "deleted on the source" in str(stop.value)
    assert dst.log("orders").end == 4


def _recreated(tmp_path, **options):
    src, dst = _clusters(orders=(1, 8))
    tail = Tail(src, dst, tmp_path, **options)
    tail.round()
    del src.topics["orders"]
    tail.round()
    assert tail.of("orders") == topicstates.LEFT
    src.topic("orders", 1)
    src.put_many("orders", 0, 3, prefix=b"new")
    return src, dst, tail


def test_a_topic_recreated_shorter_is_not_taken_for_the_one_it_replaced(
        tmp_path):
    """Its end is below the offsets the tail saved: read from them it
    would be passed over whole, as the copy always did."""
    src, dst, tail = _recreated(tmp_path)
    with pytest.raises(SystemExit) as stop:
        tail.round()
    assert "recreated or cut" in str(stop.value)
    assert dst.log("orders").end == 8


def test_a_recreated_topic_is_parked_where_the_hop_says_park(tmp_path):
    src, dst, tail = _recreated(tmp_path, ddl="park")
    assert tail.round() == 0
    assert tail.of("orders") == topicstates.PARKED
    assert tail.topics.following() == []
    # a restart looks again: the cause stands, so it is parked again
    tail.restart()
    tail.round()
    assert tail.of("orders") == topicstates.PARKED


def test_a_recreated_topic_is_followed_from_its_start_if_ddl_skips_truncates(
        tmp_path):
    src, dst, tail = _recreated(tmp_path, ddl={"default": "apply",
                                               "skip": ["truncate"]})
    assert tail.round() == 3
    assert tail.of("orders") == topicstates.LIVE
    assert tail.entry("orders")["resets"] == 1
    # the target kept what it held and took the new log after it
    assert [m[2] for m in dst.messages("orders")][-3:] == [
        b"new0", b"new1", b"new2"]
    assert dst.log("orders").end == 11


# ---- the hop's ddl, per topic ---------------------------------------------

def test_a_topic_made_on_the_source_is_followed_where_the_hop_has_no_ddl(
        tmp_path):
    src, dst = _clusters(orders=(1, 3))
    tail = Tail(src, dst, tmp_path)
    tail.round()
    src.topic("fresh", 2)
    src.put_many("fresh", 0, 2)
    src.put_many("fresh", 1, 2)
    assert tail.round() == 4
    assert tail.of("fresh") == topicstates.LIVE
    assert any("made on the source" in m for m in tail.said)


def test_a_topic_made_stops_the_tail_where_the_hop_says_stop(tmp_path):
    src, dst = _clusters(orders=(1, 3))
    tail = Tail(src, dst, tmp_path, ddl="stop")
    tail.round()
    src.topic("fresh", 1)
    with pytest.raises(SystemExit) as stop:
        tail.round()
    assert "fresh: made on the source while the tail runs" in str(stop.value)
    assert "fresh" not in dst.topics
    # started again, it is a topic added to a tail that started
    tail.restart()
    src.put_many("fresh", 0, 2)
    assert tail.round() == 2 and tail.of("fresh") == topicstates.LIVE


def test_a_topic_made_is_parked_where_the_hop_says_park(tmp_path):
    src, dst = _clusters(orders=(1, 3))
    tail = Tail(src, dst, tmp_path, ddl="park")
    tail.round()
    src.topic("fresh", 1)
    src.put_many("fresh", 0, 4)
    assert tail.round() == 0
    assert tail.of("fresh") == topicstates.PARKED
    assert "fresh" not in dst.topics
    # the hop's scope changing releases it
    tail.eng.hop.exclude = ["nothing"]
    assert tail.round(scope_changed=True) == 4
    assert tail.of("fresh") == topicstates.LIVE


def test_a_topic_made_is_left_out_where_the_hop_skips_creates(tmp_path):
    src, dst = _clusters(orders=(1, 3))
    tail = Tail(src, dst, tmp_path, ddl={"default": "apply",
                                         "skip": ["create"]})
    tail.round()
    src.topic("fresh", 1)
    src.put_many("fresh", 0, 4)
    assert tail.round() == 0
    assert tail.of("fresh") == topicstates.LEFT
    assert tail.round() == 0                      # and stays out
    assert "fresh" not in dst.topics
    tail.restart()                                # until the tail starts again
    assert tail.round() == 4


def test_the_ddl_can_be_said_for_one_topic(tmp_path):
    src, dst = _clusters(orders=(1, 3))
    tail = Tail(src, dst, tmp_path, ddl={"default": "apply",
                                         "billing_*": "stop"})
    tail.round()
    src.topic("alpha", 1)
    src.topic("billing_x", 1)
    with pytest.raises(SystemExit) as stop:
        tail.round()
    assert "billing_x" in str(stop.value)
    # alpha, which the hop's default applies, was taken in before it
    assert tail.of("alpha") == topicstates.JOINING


def test_more_partitions_on_the_source_stop_the_tail_by_default(tmp_path):
    src, dst = _clusters(orders=(2, 3))
    tail = Tail(src, dst, tmp_path)
    tail.round()
    src.topics["orders"].append(kf.Log())
    src.put_many("orders", 2, 2)
    with pytest.raises(SystemExit) as stop:
        tail.round()
    assert "2 partitions on the source became 3" in str(stop.value)
    assert "Raise the target's partitions" in str(stop.value)
    assert dst.log("orders", 0).end == 3


def test_a_target_raised_to_match_has_the_new_partitions_copied(tmp_path):
    src, dst = _clusters(orders=(2, 3))
    tail = Tail(src, dst, tmp_path)
    tail.round()
    src.topics["orders"].append(kf.Log())
    src.put_many("orders", 2, 2)
    dst.topics["orders"].append(kf.Log())
    assert tail.round() == 2
    assert tail.entry("orders")["partitions"] == 3
    assert _same(src, dst, "orders", 3)


def test_more_partitions_park_the_topic_and_only_it(tmp_path):
    src, dst = _clusters(orders=(2, 3), events=(1, 2))
    tail = Tail(src, dst, tmp_path, ddl="apply")
    tail.round()
    src.topics["orders"].append(kf.Log())
    src.put_many("orders", 2, 2)
    src.put_many("events", 0, 2, prefix=b"more")
    assert tail.round() == 2                 # events goes on
    assert tail.of("orders") == topicstates.PARKED
    assert "became 3" in tail.entry("orders")["why"]
    assert tail.of("events") == topicstates.LIVE


def test_a_topic_parked_for_its_partitions_is_followed_once_the_target_has_them(
        tmp_path):
    src, dst = _clusters(orders=(2, 3))
    tail = Tail(src, dst, tmp_path, ddl="apply")
    tail.round()
    src.topics["orders"].append(kf.Log())
    src.put_many("orders", 2, 2)
    assert tail.round() == 0
    assert tail.of("orders") == topicstates.PARKED
    assert tail.round() == 0                  # nothing changed: still parked
    dst.topics["orders"].append(kf.Log())     # the operator raises it
    assert tail.round() == 2
    assert tail.of("orders") == topicstates.LIVE
    assert _same(src, dst, "orders", 3)
    assert "parked_for" not in tail.entry("orders")


def test_a_returning_topic_released_from_park_is_held_to_its_target_again(
        tmp_path):
    src, dst, tail = _left(tmp_path, ddl="park")
    dst.put("events", 0, b"k", b"someone else's")
    tail.eng.hop.exclude = []
    tail.round(scope_changed=True)
    assert tail.of("events") == topicstates.PARKED
    # the tail starts again: the target is still not as it was left, and
    # the topic is not taken for one that is
    tail.restart()
    assert tail.round() == 0
    assert tail.of("events") == topicstates.PARKED
    assert dst.log("events").end == 7


def test_a_recreated_topic_is_parked_where_the_hop_applies_what_it_can(
        tmp_path):
    src, dst, tail = _recreated(tmp_path, ddl="apply")
    assert tail.round() == 0
    assert tail.of("orders") == topicstates.PARKED
    assert dst.log("orders").end == 8


# ---- what retention takes while a topic is not read -----------------------

def test_messages_the_source_deleted_before_they_were_copied_are_counted(
        tmp_path):
    src, dst = _clusters(orders=(1, 10))
    tail = Tail(src, dst, tmp_path)
    tail.round()
    src.put_many("orders", 0, 10, prefix=b"b")
    # the source's retention takes the log up to offset 14, four of the
    # ten the tail had not read: the client would reset to the log's end
    src.log("orders").start = 14
    assert tail.round() == 6
    part = tail.entry("orders")["parts"]["0"]
    assert part["lost"] == 4 and part["next"] == 20
    assert [m[2] for m in dst.messages("orders")][-6:] == [
        b"b4", b"b5", b"b6", b"b7", b"b8", b"b9"]
    assert any("4 messages (10 to 13) left the source's log" in m
               for m in tail.said)


def test_an_identical_partition_keeps_its_offsets_below_where_retention_cut(
        tmp_path):
    src, dst = _clusters(orders=(1, 6))
    tail = Tail(src, dst, tmp_path)
    tail.round()
    assert tail.entry("orders")["parts"]["0"]["mode"] == "identical"
    src.put_many("orders", 0, 6, prefix=b"b")
    src.log("orders").start = 9
    tail.round()
    part = tail.entry("orders")["parts"]["0"]
    assert part["lost"] == 3 and part["mode"] == "plain"
    assert part["identical_until"] == 6
    assert dst.log("orders").end == 9


# ---- round the loop every tail goes round ---------------------------------

def _run(src, dst, where):
    from migkit import tailctl
    eng = kf.engine(src, dst, where=where, follow_until="caught_up")
    said = []
    eng.tail_apply("cluster", True, tailctl.token_path(eng.hop, "cluster"),
                   said.append)
    return said


def test_the_tail_keeps_its_topics_states_in_the_position_it_saves(tmp_path):
    from migkit import tailctl
    src, dst = _clusters(orders=(2, 5))
    said = _run(src, dst, tmp_path)
    assert any("caught up with the source, after 10 messages" in m
               for m in said)
    saved = tailctl.saved(tmp_path / tailctl.TOKEN)
    assert saved["topic.orders"]["state"] == topicstates.LIVE
    assert saved["topic.orders"]["parts"]["1"]["next"] == 5
    # a topic made, and the next run: only it is copied
    src.topic("fresh", 1)
    src.put_many("fresh", 0, 3)
    src.put_many("orders", 0, 1, prefix=b"more")
    said = _run(src, dst, tmp_path)
    assert any("after 4 messages" in m for m in said)
    assert dst.log("fresh").end == 3 and dst.log("orders", 0).end == 6
    assert tailctl.saved(tmp_path / tailctl.TOKEN)["topic.fresh"][
        "state"] == topicstates.LIVE


def test_a_parked_topic_is_behind_the_position_a_drain_waits_for(tmp_path):
    """A parked topic is behind the source, and the drain's position
    (`position_reached`) is not reached while it is."""
    src, dst = _clusters(orders=(1, 3))
    tail = Tail(src, dst, tmp_path, ddl="park")
    tail.round()
    src.topic("fresh", 1)
    src.put_many("fresh", 0, 4)
    tail.round()
    want = tail.eng.log_position("src", "cluster")
    assert want == {"orders": {"0": 3}, "fresh": {"0": 4}}
    assert tail.eng.position_reached(tail.state, want) is False
