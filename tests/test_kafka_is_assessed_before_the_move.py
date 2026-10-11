"""What `assess` says of a Kafka pair before anything moves: the brokers'
versions held to each other and to the client library, the pre-flight
that predicts the copy's stops, the work the move leaves to hands, who
else writes the topics, and the room on the target (backlog
P0-PARITY.all: assess.versions, assess.preflight, assess.handwork,
assess.writers, assess.capacity).

No broker is reached: the clusters are `tests/kafka_fake.py`'s, which
answer the admin calls the assessment makes with what a test put on them.
What the real brokers answer is not proved here. The versions, the
pre-flight and the hand-work list read what the admin calls the checks
already made (a broker's and a topic's settings) answer. The producers
a partition holds state for, the brokers' log directories and the
copies of a partition are new calls that no broker has been asked yet:
the cells for the writers and the room (`assess.writers`,
`assess.capacity`) are built here and left unproved.
"""
import pytest

from migkit.engines.base import Engine
from migkit.engines.kafka import KafkaEngine
from tests import kafka_fake as kf

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("assess.versions", "kafka"), ("assess.preflight", "kafka"),
          ("assess.handwork", "kafka")]

SETTING = "inter.broker.protocol.version"
NOW_MS = 1_800_000_000_000


@pytest.fixture(autouse=True)
def _kafka(monkeypatch, tmp_path):
    kf.install(monkeypatch)
    from migkit import users
    monkeypatch.setattr(users, "_kafka_accounts", lambda hop, side: {
        "users": {}, "acls": set(), "quotas": {}, "unread": {}})
    monkeypatch.setattr(KafkaEngine, "_library",
                        staticmethod(lambda: ("3.0.11", (4, 0))))


def _pair(src_version="3.7-IV4", dst_version="3.7-IV4"):
    src, dst = kf.Cluster("src"), kf.Cluster("dst")
    if src_version:
        src.broker_configs[SETTING] = src_version
    if dst_version:
        dst.broker_configs[SETTING] = dst_version
    return src, dst


def _engine(src, dst, tmp_path, **options):
    return kf.engine(src, dst, real_settings=True, where=tmp_path, **options)


def _row(items, text):
    got = [i for i in items if text in i["item"]]
    assert got, (text, [i["item"] for i in items])
    return got[0]


# ---- versions -------------------------------------------------------------

def test_the_version_is_the_brokers_own_setting_on_each_side(tmp_path):
    src, dst = _pair("3.7-IV4", "3.8-IV0")
    eng = _engine(src, dst, tmp_path)
    assert eng._server_versions() == ("3.7", "3.8")
    row = _row(eng.assess(), "server version match")
    assert row["level"] == "warn" and "src 3.7 / dst 3.8" in row["detail"]


def test_the_same_minor_version_on_both_sides_passes(tmp_path):
    src, dst = _pair("3.7-IV4", "3.7-IV1")
    row = _row(_engine(src, dst, tmp_path).assess(), "server version match")
    assert row["level"] == "pass" and "src 3.7 / dst 3.7" in row["detail"]


def test_a_cluster_that_lists_no_setting_is_read_through_the_library(tmp_path):
    """The protocol versions the brokers accept say what the library
    inferred - and the row says it was inferred, not told."""
    src, dst = _pair(None, "3.7-IV4")
    src.api_version = (2, 6, 0)
    eng = _engine(src, dst, tmp_path)
    assert eng._server_versions() == ("2.6", "3.7")
    items = eng.assess()
    row = _row(items, "source brokers' version is inferred")
    assert row["level"] == "warn" and "inferred by the client library" \
        in row["detail"]
    assert not [i for i in items if "target brokers' version is" in i["item"]]


def test_a_version_nobody_would_say_is_unknown_not_clean(tmp_path):
    src, dst = _pair(None, None)
    eng = _engine(src, dst, tmp_path)
    assert eng._server_versions() == (None, None)
    row = _row(eng.assess(), "server version match")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]


def test_a_cluster_that_refuses_its_settings_is_unknown_too(tmp_path):
    src, dst = _pair()
    dst.refuse_calls.add("describe_configs")
    eng = _engine(src, dst, tmp_path)
    assert eng._server_versions() == ("3.7", None)
    row = _row(eng.assess(), "server version match")
    assert row["level"] == "warn" and "dst ?" in row["detail"]


def test_brokers_too_old_for_what_the_copy_carries_fail(tmp_path):
    src, dst = _pair("0.10.2", "3.7-IV4")
    row = _row(_engine(src, dst, tmp_path).assess(),
               "source brokers are too old")
    assert row["level"] == "fail" and "headers" in row["detail"]


def test_a_target_older_than_the_source_is_said(tmp_path):
    src, dst = _pair("3.8-IV0", "2.8-IV1")
    row = _row(_engine(src, dst, tmp_path).assess(),
               "target brokers are older than the source's")
    assert row["level"] == "warn" and "zstd" in row["detail"]


def test_a_broker_newer_than_the_library_knows_is_said(tmp_path, monkeypatch):
    monkeypatch.setattr(KafkaEngine, "_library",
                        staticmethod(lambda: ("2.2.15", (3, 8))))
    src, dst = _pair("3.7-IV4", "4.1-IV0")
    row = _row(_engine(src, dst, tmp_path).assess(),
               "client library and brokers")
    assert row["level"] == "warn"
    assert "target 4.1" in row["detail"] and "up to 3.8" in row["detail"]
    assert "source" not in row["detail"].split("newer than that:")[1]


def test_a_library_that_does_not_say_its_version_is_unknown(tmp_path,
                                                            monkeypatch):
    monkeypatch.setattr(KafkaEngine, "_library",
                        staticmethod(lambda: (None, None)))
    src, dst = _pair()
    row = _row(_engine(src, dst, tmp_path).assess(),
               "client library's version is not known")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]


# ---- the pre-flight -------------------------------------------------------

def test_the_preflight_names_run_before_the_move(tmp_path):
    assert KafkaEngine.PREFLIGHT == ("_partitions_fit", "_message_limits")
    assert KafkaEngine.PREFLIGHT != Engine.PREFLIGHT
    src, dst = _pair()
    src.topic("orders", 6)
    dst.topic("orders", 3)
    src.topic_settings["orders"] = {"max.message.bytes": ("2000000", "x")}
    dst.topic_settings["orders"] = {"max.message.bytes": ("1000000", "x")}
    items = _engine(src, dst, tmp_path)._preflight_items()
    count = _row(items, "partition counts match")
    assert count["level"] == "fail" and "orders 6->3" in count["detail"]
    assert "different partition" in count["detail"]
    size = _row(items, "message size limits")
    assert size["level"] == "fail"
    assert "orders takes 2,000,000 bytes on the source and 1,000,000" \
        in size["detail"]
    assert "[fix]" in size["detail"]


def test_partitions_that_fit_pass_and_a_missing_topic_is_not_a_drift(tmp_path):
    src, dst = _pair()
    src.topic("orders", 3)
    src.topic("events", 4)
    dst.topic("orders", 3)
    got = _engine(src, dst, tmp_path)._partitions_fit("cluster")
    assert got.status == "ok" and got.detail == "all equal"


def test_limits_that_fit_pass(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    dst.topic("orders")
    src.topic_settings["orders"] = {"max.message.bytes": ("1000000", "x")}
    dst.topic_settings["orders"] = {"max.message.bytes": ("5000000", "x")}
    got = _engine(src, dst, tmp_path)._message_limits("cluster")
    assert got.status == "ok" and "1 topics" in got.detail


def test_limits_are_the_topics_not_a_sample_of_its_messages(tmp_path):
    """The target's limit below the source topic's is a stop waiting for
    its first large message, however small the messages seen are."""
    src, dst = _pair()
    src.topic("orders")
    dst.topic("orders")
    src.put("orders", 0, b"k", b"tiny")
    src.topic_settings["orders"] = {"max.message.bytes": ("9000000", "x")}
    got = _engine(src, dst, tmp_path)._message_limits("cluster")
    assert got.status == "diff"


def test_limits_that_cannot_be_read_are_not_a_pass(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    dst.topic("orders")
    dst.refuse_calls.add("describe_configs")
    got = _engine(src, dst, tmp_path)._message_limits("cluster")
    assert got.status == "error" and "unknown, not clean" in got.detail
    # in the assessment an error is a warning, never a pass
    items = _engine(src, dst, tmp_path)._preflight_items()
    assert _row(items, "message size limits")["level"] == "warn"


def test_no_topic_on_both_sides_has_no_limit_to_hold(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    got = _engine(src, dst, tmp_path)._message_limits("cluster")
    assert got.status == "ok" and "no topic is on both sides" in got.detail


def test_a_missing_topic_is_made_not_a_failure(tmp_path):
    src, dst = _pair()
    src.topic("orders", 2)
    row = _row(_engine(src, dst, tmp_path).assess(),
               "every source topic exists")
    assert row["level"] == "pass" and "made as the move goes" in row["detail"]
    assert "orders" in row["detail"]


# ---- the work left to hands -----------------------------------------------

def test_settings_the_copy_does_not_carry_are_counted_and_named(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    src.topic("events")
    src.topic_settings["orders"] = {
        "retention.ms": ("1000", "DYNAMIC_TOPIC_CONFIG"),
        "segment.bytes": ("1048576", "DYNAMIC_TOPIC_CONFIG"),
        "flush.ms": ("500", "DYNAMIC_TOPIC_CONFIG")}
    inv = _engine(src, dst, tmp_path)._handwork()
    rows = inv.rows()
    row = _row(rows, "settings that need a decision")
    assert row["level"] == "warn"
    # retention.ms is carried; segment.bytes and flush.ms are not
    assert "orders: flush.ms, segment.bytes" in row["detail"]
    assert "retention.ms" not in row["detail"]
    assert "events" not in row["detail"]


def test_a_cluster_that_sets_nothing_apart_has_none(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    inv = _engine(src, dst, tmp_path)._handwork()
    row = _row(inv.rows(), "settings that need a decision")
    assert row["level"] == "pass" and row["detail"] == "none"


def test_settings_that_cannot_be_read_are_unknown_not_none(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    src.refuse_calls.add("describe_configs")
    inv = _engine(src, dst, tmp_path)._handwork()
    row = _row(inv.rows(), "settings that need a decision")
    assert row["level"] == "warn" and "UNKNOWN" in row["detail"]
    assert "Not zero" in row["detail"]
    assert inv.unknowns() == 1


def test_the_accounts_the_copy_leaves_are_named(tmp_path, monkeypatch):
    from migkit import users
    acl = ("User:app", "*", "WRITE", "ALLOW", "TOPIC", "orders", "LITERAL")
    monkeypatch.setattr(users, "_kafka_accounts", lambda hop, side: {
        "users": {"app": ["SCRAM-SHA-256"]}, "acls": {acl},
        "quotas": {(("user", "app"),): {"producer_byte_rate": 1000.0}},
        "unread": {}})
    src, dst = _pair()
    src.topic("orders")
    rows = _engine(src, dst, tmp_path)._handwork().rows()
    row = _row(rows, "objects whose contents no data mover copies")
    assert "SCRAM users (carried by migkit users, not by the copy): 1 (app)" \
        in row["detail"]
    assert "ACLs" in row["detail"] and "client quotas" in row["detail"]
    assert "User:app allow write on topic orders" in row["detail"]


def test_accounts_that_cannot_be_read_are_unknown_not_none(tmp_path,
                                                           monkeypatch):
    from migkit import users
    monkeypatch.setattr(users, "_kafka_accounts", lambda hop, side: {
        "users": None, "acls": None, "quotas": {},
        "unread": {"users": "not allowed", "acls": "SecurityDisabledError"}})
    src, dst = _pair()
    src.topic("orders")
    inv = _engine(src, dst, tmp_path)._handwork()
    row = _row(inv.rows(), "objects whose contents no data mover copies")
    assert "UNKNOWN" in row["detail"] and "SecurityDisabledError" \
        in row["detail"]


def test_assess_ends_with_the_manual_work_it_counted(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    src.topic_settings["orders"] = {
        "segment.bytes": ("1048576", "DYNAMIC_TOPIC_CONFIG")}
    items = _engine(src, dst, tmp_path).assess()
    row = _row(items, "items need hands")
    assert row["scope"] == "manual work" and "1 items need hands" \
        in row["item"]


# ---- who else writes (built, its calls not yet asked of a broker) ----------

def test_a_producer_that_wrote_a_moment_ago_beside_the_move_is_named(
        tmp_path, monkeypatch):
    monkeypatch.setattr("time.time", lambda: NOW_MS / 1000)
    src, dst = _pair()
    for c in (src, dst):
        c.topic("orders", 2)
    dst.producer_state[("orders", 1)] = [kf.producer(41, NOW_MS - 20_000),
                                         kf.producer(42, NOW_MS - 3_600_000)]
    eng = _engine(src, dst, tmp_path)
    assert eng.stream_writers("cluster") == [
        ("producer 41 on orders[1]", False)]
    row = _row(eng._writer_items("cluster"),
               "no other replication writes into the target")
    assert row["level"] == "fail" and "producer 41" in row["detail"]


def test_the_producers_of_an_earlier_run_of_migkits_are_not_strangers(
        tmp_path, monkeypatch):
    """The brokers keep a producer's state for a day: one whose last write
    is no later than migkit's own last activity is migkit's."""
    import os
    monkeypatch.setattr("time.time", lambda: NOW_MS / 1000)
    src, dst = _pair()
    for c in (src, dst):
        c.topic("orders")
    dst.producer_state[("orders", 0)] = [kf.producer(41, NOW_MS - 120_000),
                                         kf.producer(42, NOW_MS - 10_000)]
    eng = _engine(src, dst, tmp_path)
    assert [w for w, _ in eng.stream_writers("cluster")] == [
        "producer 41 on orders[0]", "producer 42 on orders[0]"]
    # migkit's copy last saved its checkpoint 150 seconds ago: what was
    # written by 90 seconds ago (a minute's slack) is explained by it
    checkpoint = tmp_path / "move.json"
    checkpoint.write_text("{}")
    os.utime(checkpoint, (NOW_MS / 1000 - 150, NOW_MS / 1000 - 150))
    assert [w for w, _ in eng.stream_writers("cluster")] == [
        "producer 42 on orders[0]"]


def test_the_sources_producers_are_followed_not_failed(tmp_path, monkeypatch):
    monkeypatch.setattr("time.time", lambda: NOW_MS / 1000)
    src, dst = _pair()
    src.topic("orders")
    dst.topic("orders")
    src.producer_state[("orders", 0)] = [kf.producer(7, NOW_MS - 1000)]
    row = _row(_engine(src, dst, tmp_path)._writer_rows(),
               "producers writing the source's topics")
    assert row["level"] == "warn" and "producer 7 on orders[0]" \
        in row["detail"]


def test_producers_that_cannot_be_asked_are_unknown_not_none(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    dst.topic("orders")
    src.no_producer_state = dst.no_producer_state = True
    eng = _engine(src, dst, tmp_path)
    row = _row(eng._writer_rows(), "producers writing the source's topics")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]
    row = _row(eng._writer_items("cluster"),
               "no other replication writes into the target")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]


def test_migkits_own_tail_is_the_target_writer_not_a_stranger(tmp_path,
                                                              monkeypatch):
    src, dst = _pair()
    src.topic("orders")
    dst.topic("orders")
    dst.producer_state[("orders", 0)] = [kf.producer(5, NOW_MS)]
    eng = _engine(src, dst, tmp_path)
    from migkit import tailctl
    monkeypatch.setattr(tailctl, "alive", lambda *a, **k: True)
    assert eng.stream_writers("cluster") == [("migkit's change tail", True)]


def test_who_may_write_is_read_off_the_acls_of_the_hops_topics(tmp_path,
                                                               monkeypatch):
    from migkit import users
    allow = lambda who, op, name, kind="LITERAL", perm="ALLOW": (  # noqa: E731
        who, "*", op, perm, "TOPIC", name, kind)
    acls = {allow("User:app", "WRITE", "orders"),
            allow("User:etl", "ALL", "ord", "PREFIXED"),
            allow("User:other", "WRITE", "billing"),
            allow("User:reader", "READ", "orders"),
            allow("User:banned", "WRITE", "orders", perm="DENY"),
            allow("User:anyone", "IDEMPOTENT_WRITE", "*")}
    monkeypatch.setattr(users, "_kafka_accounts", lambda hop, side: {
        "users": {}, "acls": acls, "unread": {},
        "quotas": {(("user", "app"),): {"producer_byte_rate": 1000.0},
                   (("user", "slow"),): {"consumer_byte_rate": 5.0}}})
    src, dst = _pair()
    src.topic("orders")
    dst.topic("orders")
    rows = _engine(src, dst, tmp_path)._writer_rows()
    row = _row(rows, "who may write the source's topics")
    assert row["detail"].startswith("3 principals: User:anyone, User:app,"
                                    " User:etl"), row["detail"]
    quota = _row(rows, "client quotas on the source")
    assert "producer_byte_rate of user app" in quota["detail"]
    assert "slow" not in quota["detail"]


def test_acls_that_cannot_be_read_are_unknown_not_none(tmp_path, monkeypatch):
    from migkit import users
    monkeypatch.setattr(users, "_kafka_accounts", lambda hop, side: {
        "users": None, "acls": None, "quotas": None,
        "unread": {"acls": "SecurityDisabledError: no authorizer",
                   "quotas": "UnsupportedVersionError"}})
    src, dst = _pair()
    src.topic("orders")
    dst.topic("orders")
    rows = _engine(src, dst, tmp_path)._writer_rows()
    row = _row(rows, "who may write the target's topics")
    assert row["level"] == "warn" and "lets any client write" \
        in row["detail"] and "unknown, not clean" in row["detail"]
    assert _row(rows, "client quotas on the target")["level"] == "warn"


# ---- the room on the target (built, its calls not yet asked of a broker) ---

def test_the_room_is_what_the_brokers_report_free(tmp_path):
    src, dst = _pair()
    dst.log_dirs = [kf.log_dir({}, 4000), kf.log_dir({}, 6000)]
    eng = _engine(src, dst, tmp_path)
    assert eng.free_bytes("dst", "cluster") == 10_000


def test_a_cluster_that_will_not_say_has_no_free_figure(tmp_path):
    src, dst = _pair()
    eng = _engine(src, dst, tmp_path)
    assert eng.free_bytes("dst", "cluster") is None
    dst.log_dirs = [{"log_dirs": [{"log_dir": "/d", "error_code": 0,
                                   "topics": []}]}]
    assert eng.free_bytes("dst", "cluster") is None
    dst.log_dirs = None
    dst.refuse_calls.add("describe_log_dirs")
    assert eng.free_bytes("dst", "cluster") is None


def test_the_topics_need_their_replicas_and_the_target_must_have_the_room(
        tmp_path):
    src, dst = _pair()
    src.topic("orders", 2)
    src.log_dirs = [kf.log_dir({"orders": {0: 400, 1: 600}}, 10 ** 9),
                    # a second broker holds the partitions' replicas
                    kf.log_dir({"orders": {0: 400, 1: 600}}, 10 ** 9)]
    dst.broker_ids = [1, 2, 3]
    eng = _engine(src, dst, tmp_path)
    assert eng._logical_bytes("src", {"orders"}) == 1000
    assert eng._replicas() == 3
    dst.log_dirs = [kf.log_dir({}, 2500)]
    row = _row(eng._room_rows(), "room on the target")
    assert row["level"] == "fail" and "3 replicas" in row["detail"]
    dst.log_dirs = [kf.log_dir({}, 3500)]
    assert _row(eng._room_rows(), "room on the target")["level"] == "warn"
    dst.log_dirs = [kf.log_dir({}, 30_000)]
    assert _row(eng._room_rows(), "room on the target")["level"] == "pass"


def test_a_room_that_cannot_be_measured_is_unknown_not_clean(tmp_path):
    src, dst = _pair()
    src.topic("orders")
    row = _row(_engine(src, dst, tmp_path)._room_rows(), "room on the target")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]
