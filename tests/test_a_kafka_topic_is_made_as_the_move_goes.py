"""The topics the target lacks are made as the source has them - the same
partitions, the same replication (no more than the target has brokers for)
and the settings that decide what a topic means - before the copier fills
any, and the copy of a topic makes it if it is still missing (backlog
P0-PARITY.all: schema.create, move.create-missing).

No broker is reached: the clusters are `tests/kafka_fake.py`'s. The
creating call is the one the copy already made; the replicas of the
source's topic are read with `describe_topics`, a call no broker has been
asked yet, so the two cells are built here and left unproved
(`capabilities.UNPROVED`): what the real brokers answer to it, and to the
create with replicas and settings, is not proved here.
"""
import json

import pytest

from tests import kafka_fake as kf


@pytest.fixture(autouse=True)
def _kafka(monkeypatch):
    kf.install(monkeypatch)


def _clusters():
    src, dst = kf.Cluster("src"), kf.Cluster("dst")
    src.topic("orders", 3)
    src.topic("events", 1)
    src.topic("__consumer_offsets", 1)
    src.topic("_schemas", 1)
    src.replication.update(orders=3, events=2)
    src.topic_settings["orders"] = {
        "cleanup.policy": ("compact", "DYNAMIC_TOPIC_CONFIG"),
        "retention.ms": ("86400000", "DYNAMIC_TOPIC_CONFIG"),
        "segment.bytes": ("1048576", "DYNAMIC_TOPIC_CONFIG")}
    dst.broker_ids = [1, 2, 3]
    return src, dst


def _engine(src, dst, tmp_path, **options):
    return kf.engine(src, dst, real_settings=True, where=tmp_path, **options)


def test_the_topics_the_target_lacks_are_made_as_the_source_has_them(tmp_path):
    src, dst = _clusters()
    said = []
    _engine(src, dst, tmp_path).create_missing("cluster", said.append)
    made = {t.name: t for t in dst.created}
    assert sorted(made) == ["events", "orders"]
    orders = made["orders"]
    assert orders.num_partitions == 3 and orders.replication_factor == 3
    # the settings that decide what a topic means; not the others
    assert orders.topic_configs["cleanup.policy"] == "compact"
    assert orders.topic_configs["retention.ms"] == "86400000"
    assert "segment.bytes" not in orders.topic_configs
    assert made["events"].replication_factor == 2
    assert len(dst.topics["orders"]) == 3
    assert said == ["events: made on the target with 1 partitions",
                    "orders: made on the target with 3 partitions"]


def test_replication_is_held_to_the_brokers_the_target_has(tmp_path):
    src, dst = _clusters()
    dst.broker_ids = [1, 2]
    _engine(src, dst, tmp_path).create_missing("cluster")
    got = {t.name: t.replication_factor for t in dst.created}
    assert got == {"orders": 2, "events": 2}


def test_the_hops_replication_factor_is_the_word_that_stands(tmp_path):
    src, dst = _clusters()
    _engine(src, dst, tmp_path, replication_factor=1).create_missing(
        "cluster")
    assert {t.replication_factor for t in dst.created} == {1}


def test_a_source_that_will_not_say_its_replicas_gets_the_default(tmp_path):
    src, dst = _clusters()
    src.refuse_calls.add("describe_topics")
    _engine(src, dst, tmp_path).create_missing("cluster")
    assert {t.replication_factor for t in dst.created} == {3}


def test_a_topic_the_target_has_is_never_touched(tmp_path):
    src, dst = _clusters()
    dst.topic("orders", 1)
    dst.topic_settings["orders"] = {"cleanup.policy": ("delete", "x")}
    dst.put("orders", 0, b"k", b"kept")
    _engine(src, dst, tmp_path).create_missing("cluster")
    assert [t.name for t in dst.created] == ["events"]
    assert len(dst.topics["orders"]) == 1
    assert dst.messages("orders") == [(0, b"k", b"kept")]
    assert dst.topic_settings["orders"] == {"cleanup.policy": ("delete", "x")}


def test_only_the_topics_the_hop_takes_are_made(tmp_path):
    src, dst = _clusters()
    eng = _engine(src, dst, tmp_path)
    eng.hop.exclude = ["events"]
    eng.create_missing("cluster")
    assert [t.name for t in dst.created] == ["orders"]
    # the cluster's own topics are nobody's to make
    assert "__consumer_offsets" not in dst.topics
    assert "_schemas" not in dst.topics


def test_making_them_twice_makes_them_once(tmp_path):
    src, dst = _clusters()
    eng = _engine(src, dst, tmp_path)
    eng.create_missing("cluster")
    again = []
    eng.create_missing("cluster", again.append)
    assert again == [] and len(dst.created) == 2


def test_settings_that_cannot_be_read_make_nothing(tmp_path):
    """A topic made without its compaction or retention would mean
    something else on the target."""
    src, dst = _clusters()
    src.refuse_calls.add("describe_configs")
    with pytest.raises(SystemExit) as stop:
        _engine(src, dst, tmp_path).create_missing("cluster")
    assert "could not be read on the source" in str(stop.value)
    assert dst.created == [] and "orders" not in dst.topics


def test_a_topic_made_by_someone_between_is_not_a_failure(tmp_path,
                                                          monkeypatch):
    src, dst = _clusters()

    class TopicAlreadyExistsError(Exception):
        pass

    class Racing(kf.Admin):
        def create_topics(self, topics):
            raise TopicAlreadyExistsError("orders")
    eng = _engine(src, dst, tmp_path)
    eng._admin = lambda side: Racing(dst if side == "dst" else src)
    assert eng._made_like_the_source("orders", 3) is None


def test_the_copy_of_a_topic_makes_it_if_it_is_still_missing(tmp_path):
    src, dst = _clusters()
    src.put_many("orders", 0, 4)
    copy = kf.Copy(src, dst, topic="orders")
    copy.eng = _engine(src, dst, tmp_path)
    copy.run()
    assert [t.name for t in dst.created if not t.name.startswith("__")] == [
        "orders"]
    assert dst.log("orders", 0).end == 4


def test_what_would_be_made_is_said_without_making_it(tmp_path):
    src, dst = _clusters()
    eng = _engine(src, dst, tmp_path)
    plan = json.loads(eng.neutral_create_sql("dst", "cluster", "orders", []))
    configs = plan.pop("configs")
    assert plan == {"topic": "orders", "partitions": 3,
                    "replication_factor": 3}
    assert configs["cleanup.policy"] == "compact"
    assert configs["retention.ms"] == "86400000"
    assert dst.created == []
    # a topic the target has is said to be there
    dst.topic("orders")
    assert json.loads(eng.neutral_create_sql(
        "dst", "cluster", "orders", [])) == {"topic": "orders",
                                             "exists": True}
    with pytest.raises(SystemExit):
        eng.neutral_create_sql("dst", "cluster", "nothing", [])


def test_a_topic_made_by_the_statement_is_the_one_it_said(tmp_path):
    src, dst = _clusters()
    eng = _engine(src, dst, tmp_path)
    said = eng.neutral_create("dst", "cluster", "orders", [])
    made = dst.created[0]
    assert (made.name, made.num_partitions, made.replication_factor) == (
        "orders", 3, 3)
    assert json.loads(said)["partitions"] == 3
    # an existing topic is not altered by a second statement
    eng.neutral_create("dst", "cluster", "orders", [])
    assert len(dst.created) == 1
