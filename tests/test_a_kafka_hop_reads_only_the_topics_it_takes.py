"""A Kafka hop takes the topics its `include` and `exclude` say, and the
copy, every check, the consumer groups' positions and the repair all ask
the one scope answer (`Engine.in_scope`, through `KafkaEngine._takes_topic`)
(P0-PARITY.scope).

Two in-memory clusters (`tests/kafka_fake.py`): no broker is reached. What
is proved is the arithmetic of the scope - which topics, which groups and
which positions are read or written - and not the wire protocol, which the
docker tests of this engine hold.

The target differs from the source in a topic the hop leaves out (it lacks
`secret.audit` and has a topic of its own beside it), and a group reads
only that topic: nothing about it is a finding, a plan or a write.
"""
import json

import pytest

from tests import kafka_fake as fake
from tests.kafka_fake import Cluster, TopicPartition

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.listings", "kafka"), ("scope.deep", "kafka"),
          ("scope.copy", "kafka"), ("scope.repair", "kafka"),
          ("scope.objects", "kafka")]

ORDERS = TopicPartition("orders", 0)
AUDIT = TopicPartition("secret.audit", 0)


@pytest.fixture
def pair(monkeypatch, tmp_path):
    fake.install(monkeypatch)
    src, dst = Cluster("src"), Cluster("dst")
    for cluster in (src, dst):
        cluster.topic("orders")
        cluster.put_many("orders", 0, 4)
    src.topic("secret.audit")
    src.put_many("secret.audit", 0, 3)
    # what the target holds of its own, beside the topic it lacks
    dst.topic("secret.own")
    dst.put_many("secret.own", 0, 9)
    src.groups = {"billing": {ORDERS: 3},
                  "auditors": {AUDIT: 2},
                  "both": {ORDERS: 4, AUDIT: 1}}
    eng = fake.engine(src, dst)
    eng.hop.exclude = ["secret.*"]
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng, src, dst


def _noacl(monkeypatch):
    from migkit import users
    monkeypatch.setattr(users, "_kafka_accounts", lambda hop, side: {
        "users": {}, "acls": set(), "unread": {}})


def test_the_copy_lists_only_the_topics_the_hop_takes(pair):
    eng, _, _ = pair
    assert eng.list_move_tables("cluster") == [("", "orders")]


def test_an_include_narrows_the_listing_as_an_exclude_does(pair):
    eng, src, _ = pair
    src.topic("payments")
    eng.hop.exclude = []
    eng.hop.include = ["payments"]
    assert eng.list_move_tables("cluster") == [("", "payments")]


def test_a_topic_the_hop_leaves_out_is_no_finding_in_counts_schema_or_data(
        pair):
    eng, _, _ = pair
    got = {r.scope: r for r in eng.check_counts("cluster")}
    assert got["messages"].status == "ok", got["messages"].detail
    assert set(got["messages"].tables) == {"orders"}
    schema = {r.scope: r for r in eng.check_schema("cluster")}
    assert schema["topics"].status == "ok", schema["topics"].detail
    assert set(schema["topics"].tables) == {"orders"}
    data = eng.check_data("cluster")
    assert [r.status for r in data] == ["ok"], data[0].detail
    assert set(data[0].tables) == {"orders"}


def test_the_same_topic_is_a_finding_once_the_hop_takes_it(pair):
    eng, _, _ = pair
    eng.hop.exclude = []
    got = {r.scope: r for r in eng.check_counts("cluster")}
    assert got["messages"].status == "diff"
    assert "secret.audit missing on target" in got["messages"].detail


def test_a_topic_named_to_the_data_check_is_asked_of_the_scope(pair):
    eng, _, _ = pair
    got = eng.check_data("cluster", table="secret.audit")
    assert [r.status for r in got] == ["error"]
    assert "no partitions were compared" in got[0].detail


def test_the_deep_check_reads_only_the_groups_of_the_topics_it_takes(
        pair, monkeypatch):
    eng, _, _ = pair
    _noacl(monkeypatch)
    got = {r.scope: r for r in eng.check_deep("cluster")}
    # `auditors` reads only a topic the hop leaves out: it is not missing
    assert "billing" in got["groups"].detail
    assert "both" in got["groups"].detail
    assert "auditors" not in got["groups"].detail
    assert "2 consumer groups missing" in got["groups"].detail
    # the positions compared are the taken topic's alone
    assert "secret.audit" not in got["group-offsets"].detail


def test_a_group_with_no_position_at_all_is_still_the_hops(pair, monkeypatch):
    eng, src, _ = pair
    _noacl(monkeypatch)
    src.groups = {"idle": {}}
    got = {r.scope: r for r in eng.check_deep("cluster")}
    assert "idle" in got["groups"].detail


def test_the_repair_plans_only_positions_on_the_topics_the_hop_takes(pair):
    eng, _, _ = pair
    (action,) = eng.repair_plan("cluster", "sequences")
    rows = [json.loads(s) for s in action.statements if s.startswith("{")]
    assert sorted((r["group"], r["topic"]) for r in rows) == [
        ("billing", "orders"), ("both", "orders")]
    assert "auditors" not in action.statements[0]
    assert all(json.loads(s)["topic"] == "orders" for s in action.undo)


def test_the_repair_commits_nothing_on_a_topic_the_hop_leaves_out(pair):
    eng, _, dst = pair
    (action,) = eng.repair_plan("cluster", "sequences")
    eng.apply("cluster", action)
    assert dst.groups == {"billing": {ORDERS: 3}, "both": {ORDERS: 4}}


def test_a_drilldown_of_another_scope_is_not_acted_on(pair):
    """The last check's file was written when the hop took `secret.audit`;
    the repair reads it now, and a position there is the target's own."""
    eng, _, dst = pair
    eng._write_drill("cluster", "groups", missing=[], changed=[
        json.dumps({"group": "auditors", "topic": "secret.audit",
                    "partition": 0, "offset": 2}),
        json.dumps({"group": "billing", "topic": "orders",
                    "partition": 0, "offset": 3})])
    from migkit.engines.base import RepairAction
    eng.apply("cluster", RepairAction("cluster", "sequences"))
    assert dst.groups == {"billing": {ORDERS: 3}}


def test_the_objects_beside_the_topics_follow_the_hops_objects(
        pair, monkeypatch):
    """An ACL on a topic is a grant of an account (`objects: roles`): a hop
    that carries no accounts does not hold the topics to them, one that
    names some holds only theirs, and an ACL on a topic it leaves out is
    nobody's business."""
    from migkit import users
    eng, _, _ = pair
    acls = {("User:app", "*", "READ", "ALLOW", "TOPIC", "orders",
             "LITERAL"),
            ("User:app", "*", "READ", "ALLOW", "TOPIC", "secret.audit",
             "LITERAL"),
            ("User:batch", "*", "WRITE", "ALLOW", "TOPIC", "orders",
             "LITERAL")}
    monkeypatch.setattr(users, "_kafka_accounts", lambda hop, side: {
        "users": {}, "acls": set(acls) if side == "src" else set(),
        "unread": {}})

    def grants():
        (one,) = [r for r in eng.check_deep("cluster")
                  if r.scope == "cluster grants"]
        return one
    both = grants()
    assert both.status == "diff"
    assert "User:app" in both.detail and "User:batch" in both.detail
    assert "secret.audit" not in both.detail
    eng.hop.objects = {"roles": ["User:batch"]}
    only = grants()
    assert "User:batch" in only.detail and "User:app" not in only.detail
    eng.hop.objects = {"roles": False}
    none = grants()
    assert none.status == "ok", none.detail
