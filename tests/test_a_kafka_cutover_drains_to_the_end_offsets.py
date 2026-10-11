"""A Kafka cutover's drain is proved by the source's end offsets: after the
freeze, the position of every partition's end is taken, and the cutover
goes on only once the tail's saved position has copied up to each of them
(backlog P0-PARITY.all, cutover.drain).

    log_position      where each of the hop's topics ends now
    position_reached  whether a tail's saved offsets are at or past those
                      ends - every partition of every topic, a parked or
                      unread one counted
    fence_wait        waits for it (the cutover's own wait)
    drain_report      what is left to wait for, and how far behind each
                      consumer group of the source stands

The end taken includes a transaction still open: the tail passes it only
once that transaction is over, so the drain is never proved while a write
from before the freeze is outstanding.

No broker is reached: the clusters are `tests/kafka_fake.py`'s, which
model the last stable offset and transactions. What the real brokers do
with the same reads, and the cutover's freeze around it, is not proved
here.
"""
import pytest

from migkit import tailctl
from tests import kafka_fake as kf

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.drain", "kafka")]


@pytest.fixture(autouse=True)
def _kafka(monkeypatch):
    kf.install(monkeypatch)


def _clusters():
    src, dst = kf.Cluster("src"), kf.Cluster("dst")
    src.topic("orders", 2)
    src.put_many("orders", 0, 6)
    src.put_many("orders", 1, 4)
    src.topic("events", 1)
    src.put_many("events", 0, 3)
    src.topic("__consumer_offsets", 1)
    src.put_many("__consumer_offsets", 0, 9)
    return src, dst


def _tail(src, dst, where):
    from tests.test_the_kafka_tail_keeps_each_topic_in_a_state import Tail
    return Tail(src, dst, where)


def test_the_position_is_where_each_topic_of_the_hop_ends(tmp_path):
    src, dst = _clusters()
    eng = kf.engine(src, dst, where=tmp_path)
    eng.hop.exclude = ["events"]
    assert eng.log_position("src", "cluster") == {
        "orders": {"0": 6, "1": 4}}
    # asked of the target, it is what the target holds of those topics
    assert eng.log_position("dst", "cluster") == {}


def test_a_tail_has_reached_it_only_when_every_partition_has(tmp_path):
    src, dst = _clusters()
    tail = _tail(src, dst, tmp_path)
    want = tail.eng.log_position("src", "cluster")
    assert tail.eng.position_reached(tail.state, want) is False
    tail.round()
    assert tail.eng.position_reached(tail.state, want) is True
    src.put_many("orders", 1, 1, prefix=b"after")
    later = tail.eng.log_position("src", "cluster")
    assert tail.eng.position_reached(tail.state, later) is False
    tail.round()
    assert tail.eng.position_reached(tail.state, later) is True


def test_a_position_that_is_not_one_is_not_reached_or_missed(tmp_path):
    src, dst = _clusters()
    eng = kf.engine(src, dst, where=tmp_path)
    assert eng.position_reached(None, {"orders": {"0": 1}}) is None
    assert eng.position_reached({}, None) is None
    # nothing to reach is reached; a partition the tail never saw is not
    assert eng.position_reached({}, {}) is True
    assert eng.position_reached({}, {"orders": {"0": 1}}) is False
    assert eng.position_reached(
        {"topic.orders": {"parts": {"0": {"next": 1}}}},
        {"orders": {"0": 1}}) is True


def test_a_transaction_open_at_the_freeze_holds_the_drain_until_it_ends(
        tmp_path):
    src, dst = _clusters()
    open_txn = kf.Producer(src, transactional_id="app")
    open_txn.init_transactions()
    open_txn.begin_transaction()
    open_txn.send("orders", key=b"k", value=b"in flight", partition=0)
    tail = _tail(src, dst, tmp_path)
    tail.round()
    want = tail.eng.log_position("src", "cluster")
    # the end is the log's, the open transaction's message included
    assert want["orders"]["0"] == 7
    assert tail.eng.position_reached(tail.state, want) is False
    tail.round()
    assert tail.eng.position_reached(tail.state, want) is False
    open_txn.commit_transaction()
    # the commit's marker takes an offset of its own
    want = tail.eng.log_position("src", "cluster")
    assert want["orders"]["0"] == 8
    tail.round()
    assert tail.eng.position_reached(tail.state, want) is True
    assert [m[2] for m in dst.messages("orders", 0)][-1] == b"in flight"


def test_an_aborted_transaction_is_passed_without_being_copied(tmp_path):
    src, dst = _clusters()
    src.transaction("orders", 0, 3, commit=False)
    tail = _tail(src, dst, tmp_path)
    tail.round()
    want = tail.eng.log_position("src", "cluster")
    assert want["orders"]["0"] == 10
    assert tail.eng.position_reached(tail.state, want) is True
    assert dst.log("orders", 0).end == 6


def test_the_wait_returns_once_the_saved_position_passes_it(tmp_path,
                                                            monkeypatch):
    src, dst = _clusters()
    eng = kf.engine(src, dst, where=tmp_path)
    at = {"orders": {"0": 6, "1": 4}}
    states = [{"topic.orders": {"parts": {"0": {"next": 2}}}},
              {"topic.orders": {"parts": {"0": {"next": 6},
                                          "1": {"next": 3}}}},
              {"topic.orders": {"parts": {"0": {"next": 6},
                                          "1": {"next": 4}}}}]
    seen = []
    monkeypatch.setattr(tailctl, "saved",
                        lambda path: seen.append(1) or states[
                            min(len(seen) - 1, 2)])
    monkeypatch.setattr(tailctl, "POLL", 0)
    assert eng.fence_wait("cluster", at, timeout=5) is True
    assert len(seen) == 3
    assert eng._fenced_at == at


def test_the_wait_says_false_when_the_tail_does_not_get_there(tmp_path,
                                                              monkeypatch):
    src, dst = _clusters()
    eng = kf.engine(src, dst, where=tmp_path)
    monkeypatch.setattr(tailctl, "saved", lambda path: {
        "topic.orders": {"parts": {"0": {"next": 5}}}})
    assert eng.fence_wait("cluster", {"orders": {"0": 6}},
                          timeout=0.05) is False


def test_the_wait_has_nothing_to_wait_on_without_a_position(tmp_path,
                                                            monkeypatch):
    src, dst = _clusters()
    eng = kf.engine(src, dst, where=tmp_path)
    assert eng.fence_wait("cluster", None) is None
    monkeypatch.setattr(tailctl, "saved", lambda path: None)
    assert eng.fence_wait("cluster", {"orders": {"0": 1}}) is None


def test_the_source_gives_a_position_only_while_a_tail_runs_to_reach_it(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    eng = kf.engine(src, dst, where=tmp_path)
    assert eng.src_lsn("cluster") is None
    monkeypatch.setattr(tailctl, "alive", lambda *a, **k: True)
    assert eng.src_lsn("cluster") == eng.log_position("src", "cluster")
    assert eng.src_lsn("cluster")["orders"] == {"0": 6, "1": 4}


def test_the_report_says_what_is_left_and_how_far_behind_each_group_is(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    tp = kf.TopicPartition
    src.groups["billing"] = {tp("orders", 0): 2, tp("orders", 1): 4}
    src.groups["audit"] = {tp("events", 0): 3}
    monkeypatch.setattr(tailctl, "saved", lambda path: {
        "topic.orders": {"parts": {"0": {"next": 4}, "1": {"next": 4}}},
        "topic.events": {"parts": {"0": {"next": 3}}}})
    eng = kf.engine(src, dst, where=tmp_path)
    at = eng.log_position("src", "cluster")
    said = eng.drain_report("cluster", at)
    assert "2 messages not on the target yet (orders 2)" in said
    # billing has 4 of orders[0] to read and none of orders[1]; audit none
    assert "consumer group lag at the position: audit 0, billing 4" in said


def test_the_report_of_a_drained_topic_says_so(tmp_path, monkeypatch):
    src, dst = _clusters()
    monkeypatch.setattr(tailctl, "saved", lambda path: {
        "topic.orders": {"parts": {"0": {"next": 6}, "1": {"next": 4}}},
        "topic.events": {"parts": {"0": {"next": 3}}}})
    eng = kf.engine(src, dst, where=tmp_path)
    said = eng.drain_report("cluster", eng.log_position("src", "cluster"))
    assert said == "every message up to the position is on the target"


def test_groups_that_cannot_be_read_are_said_not_left_out(tmp_path,
                                                          monkeypatch):
    src, dst = _clusters()
    monkeypatch.setattr(tailctl, "saved", lambda path: {})
    eng = kf.engine(src, dst, where=tmp_path)

    def refuse(side):
        raise PermissionError("GroupAuthorizationFailedError")
    eng._groups = refuse
    said = eng.drain_report("cluster", {"orders": {"0": 6}})
    assert said.endswith("the consumer groups' lag could not be read")
    assert "6 messages not on the target yet" in said


def test_a_drain_that_stopped_says_what_it_was_waiting_for(tmp_path,
                                                           monkeypatch):
    """The cutover's own words for a drain that did not pass its position
    carry the engine's report of what was left, where it makes one."""
    from migkit import cutover
    src, dst = _clusters()
    monkeypatch.setattr(tailctl, "saved", lambda path: {})
    eng = kf.engine(src, dst, where=tmp_path)
    at = eng.log_position("src", "cluster")
    said = cutover._drain_left(eng, "cluster", at)
    assert said.startswith(". Left: 13 messages not on the target yet")

    class Mute:
        pass

    class Broken:
        def drain_report(self, db, pos):
            raise RuntimeError("no")
    assert cutover._drain_left(Mute(), "cluster", at) == ""
    assert cutover._drain_left(Broken(), "cluster", at) == ""
