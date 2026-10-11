"""What a topic can differ in, each found and told apart (P0-PARITY.verify):
its partition count, its settings, a tombstone where the other side holds
an empty value, messages on the target past the source's newest, the ACLs
on it - and each finding says which topic it is about, how every topic it
compared came out, and how many messages are apart.

Two in-memory clusters (`tests/kafka_fake.py`); no broker is reached. The
calls the engine makes to the cluster are the ones it has always made: what
is proved is how they are read.
"""
import pytest

from tests import kafka_fake as fake
from tests.kafka_fake import Cluster

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.structure.partitions", "kafka"),
          ("verify.structure.collection-options", "kafka"),
          ("verify.structure.objects", "kafka"),
          ("verify.access.table-grants", "kafka"),
          ("verify.value.null-empty", "kafka"),
          ("verify.movement.target-ahead", "kafka"),
          ("verify.rows-apart", "kafka"), ("verify.tables", "kafka"),
          ("verify.table", "kafka")]


@pytest.fixture
def pair(monkeypatch, tmp_path):
    fake.install(monkeypatch)
    src, dst = Cluster("src"), Cluster("dst")
    for cluster in (src, dst):
        cluster.topic("orders", 3)
        cluster.topic("payments")
        for p in range(3):
            cluster.put_many("orders", p, 5)
        cluster.put_many("payments", 0, 4)
    eng = fake.engine(src, dst)
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng, src, dst


def _by_scope(results):
    return {r.scope: r for r in results}


# ---- the partition count --------------------------------------------------

def test_a_partition_count_that_differs_is_a_finding_of_its_own(pair):
    eng, src, dst = pair
    dst.topics["orders"].pop()
    got = _by_scope(eng.check_schema("cluster"))
    assert got["topics"].status == "ok", got["topics"].detail
    one = got["topics partitions"]
    assert one.status == "diff"
    assert "orders partitions src=3 dst=2" in one.detail
    assert one.category == "structure.partitions"
    assert one.table == "orders"
    assert one.tables == {"orders": "diff", "payments": "ok"}


def test_equal_partition_counts_are_said_for_every_topic(pair):
    eng, _, _ = pair
    one = _by_scope(eng.check_schema("cluster"))["topics partitions"]
    assert one.status == "ok" and one.table == ""
    assert one.tables == {"orders": "ok", "payments": "ok"}


def test_a_topic_missing_is_the_topics_finding_and_names_its_table(pair):
    eng, _, dst = pair
    del dst.topics["payments"]
    got = _by_scope(eng.check_schema("cluster"))
    assert got["topics"].status == "diff"
    assert "missing topic payments" in got["topics"].detail
    assert got["topics"].table == "payments"
    assert got["topics"].tables == {"orders": "ok", "payments": "diff"}


# ---- the topics' settings -------------------------------------------------

def test_a_setting_that_differs_is_a_collection_option(pair):
    eng, _, _ = pair
    keys = eng.CRITICAL_CONFIGS

    def configs(side, topics):
        return {t: {k: ("compact" if k == "cleanup.policy"
                        and side == "src" and t == "orders" else "x")
                    for k in keys} for t in topics}
    eng._topic_configs = configs
    one = _by_scope(eng.check_schema("cluster"))["topic-configs"]
    assert one.status == "diff"
    assert "orders cleanup.policy src=compact dst=x" in one.detail
    assert one.category == "structure.collection-options"
    assert one.table == "orders"
    assert one.tables == {"orders": "diff", "payments": "ok"}


def test_settings_that_cannot_be_read_are_an_error_not_a_pass(pair):
    eng, _, _ = pair
    eng._topic_configs = lambda side, topics: None
    one = _by_scope(eng.check_schema("cluster"))["topic-configs"]
    assert one.status == "error"


# ---- the consumer groups --------------------------------------------------

def test_a_consumer_group_missing_is_an_object_beside_the_topics(
        pair, monkeypatch):
    eng, src, _ = pair
    _no_acls(monkeypatch)
    src.groups = {"billing": {}}
    one = _by_scope(eng.check_deep("cluster"))["groups"]
    assert one.status == "diff" and "billing" in one.detail
    assert one.category == "structure.objects"


# ---- a tombstone and an empty value --------------------------------------

def test_a_tombstone_on_one_side_and_an_empty_value_on_the_other_differ(pair):
    eng, src, dst = pair
    src.put("payments", 0, b"k9", None)
    dst.put("payments", 0, b"k9", b"")
    got = _by_scope(eng.check_data("cluster"))
    assert got["tail-sample"].status == "diff"
    one = got["cluster nullempty"]
    assert one.status == "diff"
    assert "payments 1 message" in one.detail
    assert one.category == "value.null-empty"
    assert one.table == "payments"
    assert one.tables["orders"] == "ok" and one.tables["payments"] == "diff"


def test_a_hash_of_the_bytes_does_not_read_a_tombstone_as_an_empty_value(
        pair):
    eng, src, dst = pair
    src.put("payments", 0, b"k9", None)
    dst.put("payments", 0, b"k9", b"")
    tp = fake.TopicPartition("payments", 0)
    a = eng._window_hash(fake.Consumer(src), tp, 0, 5, "src")
    b = eng._window_hash(fake.Consumer(dst), tp, 0, 5, "dst")
    assert a != b


def test_where_a_key_and_a_value_end_is_part_of_what_is_hashed(pair):
    eng, src, dst = pair
    src.put("payments", 0, b"ab", b"c")
    dst.put("payments", 0, b"a", b"bc")
    tp = fake.TopicPartition("payments", 0)
    assert eng._window_hash(fake.Consumer(src), tp, 4, 5, "src") \
        != eng._window_hash(fake.Consumer(dst), tp, 4, 5, "dst")


def test_a_message_that_differs_otherwise_is_no_null_empty_finding(pair):
    eng, src, dst = pair
    src.put("payments", 0, b"k9", b"one")
    dst.put("payments", 0, b"k9", b"two")
    got = _by_scope(eng.check_data("cluster"))
    assert got["tail-sample"].status == "diff"
    assert "cluster nullempty" not in got


# ---- the rows apart, the tables, the table ---------------------------------

def test_a_data_finding_counts_the_messages_apart_by_topic(pair):
    eng, src, dst = pair
    src.put("payments", 0, b"only-src", b"a")
    dst.put("payments", 0, b"only-dst", b"b")
    dst.put("payments", 0, b"only-dst2", b"c")
    one = _by_scope(eng.check_data("cluster"))["tail-sample"]
    assert one.status == "diff"
    assert one.rows_apart == {"payments": {
        "missing": 1, "extra": 2, "changed": 0, "exact": False}}
    assert one.table == "payments"
    assert one.tables == {"orders": "ok", "payments": "diff"}


def test_a_changed_value_is_one_message_changed_not_two_apart(pair):
    eng, src, dst = pair
    src.put("payments", 0, b"k9", b"one")
    dst.put("payments", 0, b"k9", b"two")
    one = _by_scope(eng.check_data("cluster"))["tail-sample"]
    assert one.rows_apart["payments"] == {
        "missing": 0, "extra": 0, "changed": 1, "exact": False}


def test_a_clean_data_check_says_how_every_topic_came_out(pair):
    eng, _, _ = pair
    (one,) = eng.check_data("cluster")
    assert one.status == "ok"
    assert one.tables == {"orders": "ok", "payments": "ok"}


def test_a_count_finding_counts_the_messages_apart_by_topic(pair):
    eng, _, dst = pair
    dst.put_many("payments", 0, 3)
    one = _by_scope(eng.check_counts("cluster"))["messages"]
    assert one.status == "diff"
    assert one.rows_apart == {"payments": {
        "missing": 0, "extra": 3, "changed": 0, "exact": False}}
    assert one.table == "payments"
    assert one.tables == {"orders": "ok", "payments": "diff"}


def test_a_finding_about_several_topics_names_none_as_its_table(pair):
    eng, _, dst = pair
    dst.put_many("payments", 0, 3)
    dst.put_many("orders", 1, 2)
    one = _by_scope(eng.check_counts("cluster"))["messages"]
    assert one.table == ""
    assert set(one.rows_apart) == {"payments", "orders"}


# ---- the target past the source's newest message --------------------------

def _no_acls(monkeypatch):
    from migkit import users
    monkeypatch.setattr(users, "_kafka_accounts", lambda hop, side: {
        "users": {}, "acls": set(), "unread": {}})


def _boundary(eng, monkeypatch):
    _no_acls(monkeypatch)
    return _by_scope(eng.check_deep("cluster"))["cluster boundary"]


def test_a_target_holding_what_the_source_does_is_not_ahead(pair, monkeypatch):
    eng, _, _ = pair
    one = _boundary(eng, monkeypatch)
    assert one.status == "ok", one.detail
    assert one.tables == {"orders": "ok", "payments": "ok"}


def test_messages_written_on_the_target_past_the_sources_newest_are_found(
        pair, monkeypatch):
    eng, _, dst = pair
    dst.put("payments", 0, b"landed", b"on the target", ts=1_700_000_900_000)
    dst.put("payments", 0, b"landed2", b"on the target",
            ts=1_700_000_900_001)
    one = _boundary(eng, monkeypatch)
    assert one.status == "diff"
    assert "payments[0] AHEAD of source by 2 messages" in one.detail
    assert one.category == "movement.target-ahead"
    assert one.table == "payments"
    assert one.rows_apart == {"payments": {
        "missing": 0, "extra": 2, "changed": 0, "exact": False}}


def test_a_copy_applied_twice_leaves_the_target_ahead(pair, monkeypatch):
    eng, src, dst = pair
    dst.put_many("payments", 0, 4)         # the same four, again
    one = _boundary(eng, monkeypatch)
    assert one.status == "diff"
    assert "payments[0] AHEAD" in one.detail


def test_a_target_with_fewer_messages_is_behind_not_ahead(pair, monkeypatch):
    eng, src, _ = pair
    src.put_many("payments", 0, 3, prefix=b"new")
    one = _boundary(eng, monkeypatch)
    # the source's newest is not on the target: it cannot be told, and says so
    assert one.status == "warn"
    assert "payments[0]" in one.detail


def test_a_transaction_marker_at_the_end_does_not_hide_the_newest_message(
        pair, monkeypatch):
    eng, src, dst = pair
    for cluster in (src, dst):
        cluster.transaction("payments", 0, 2)
    dst.put("payments", 0, b"landed", b"x", ts=1_700_000_900_000)
    one = _boundary(eng, monkeypatch)
    assert one.status == "diff"
    assert "payments[0] AHEAD of source by 1 messages" in one.detail


def test_an_empty_source_with_a_target_that_holds_messages_is_ahead(
        pair, monkeypatch):
    eng, src, dst = pair
    src.topic("fresh")
    dst.topic("fresh")
    dst.put_many("fresh", 0, 2)
    one = _boundary(eng, monkeypatch)
    assert one.status == "diff"
    assert "fresh[0] AHEAD of source by 2 messages" in one.detail


def test_an_empty_source_whose_messages_retention_took_cannot_be_told(
        pair, monkeypatch):
    eng, src, dst = pair
    src.topic("aged")
    src.put_many("aged", 0, 3)
    src.log("aged").start = 3              # all of it deleted
    dst.topic("aged")
    dst.put_many("aged", 0, 3)
    one = _boundary(eng, monkeypatch)
    assert one.status == "warn"
    assert "aged[0]" in one.detail


# ---- the ACLs on the topics ------------------------------------------------

def _acls(monkeypatch, src, dst, unread=None):
    from migkit import users

    def accounts(hop, side):
        mine = {"src": src, "dst": dst}[side]
        return {"users": {}, "acls": None if mine is None else set(mine),
                "unread": (unread or {}).get(side, {})}
    monkeypatch.setattr(users, "_kafka_accounts", accounts)


READ = ("User:app", "*", "READ", "ALLOW", "TOPIC", "orders", "LITERAL")


def _grants(eng):
    (one,) = [r for r in eng.check_deep("cluster")
              if r.scope == "cluster grants"]
    return one


def test_an_acl_the_target_lacks_is_a_grant_gap(pair, monkeypatch):
    eng, _, _ = pair
    _acls(monkeypatch, {READ}, set())
    one = _grants(eng)
    assert one.status == "diff"
    assert "User:app allow read on topic orders" in one.detail
    assert one.category == "access.table-grants"
    assert one.table == "orders"
    assert one.tables == {"orders": "diff", "payments": "ok"}


def test_acls_on_both_sides_are_a_pass(pair, monkeypatch):
    eng, _, _ = pair
    _acls(monkeypatch, {READ}, {READ})
    assert _grants(eng).status == "ok"


def test_an_acl_only_the_target_has_is_noted_not_a_difference(
        pair, monkeypatch):
    eng, _, _ = pair
    _acls(monkeypatch, set(), {READ})
    one = _grants(eng)
    assert one.status == "warn"
    assert "the target has 1 of its own" in one.detail


def test_a_prefix_acl_is_held_to_the_topics_it_covers(pair, monkeypatch):
    eng, _, _ = pair
    pre = ("User:app", "*", "READ", "ALLOW", "TOPIC", "pay", "PREFIXED")
    other = ("User:app", "*", "READ", "ALLOW", "TOPIC", "zz", "PREFIXED")
    _acls(monkeypatch, {pre, other}, set())
    one = _grants(eng)
    assert "on topic pay (prefix)" in one.detail
    assert "zz" not in one.detail
    assert one.tables == {"orders": "ok", "payments": "diff"}


def test_a_cluster_that_will_not_list_its_acls_is_an_error_not_a_pass(
        pair, monkeypatch):
    eng, _, _ = pair
    _acls(monkeypatch, {READ}, None,
          unread={"dst": {"acls": "ClusterAuthorizationFailed"}})
    one = _grants(eng)
    assert one.status == "error"
    assert "target: ClusterAuthorizationFailed" in one.detail


def test_a_source_with_no_authorizer_has_no_acls_to_carry(pair, monkeypatch):
    eng, _, _ = pair
    _acls(monkeypatch, None, set(),
          unread={"src": {"acls": "SecurityDisabledError"}})
    assert _grants(eng).status == "ok"
