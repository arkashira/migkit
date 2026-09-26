"""Consumer-group offsets: the check that never ran, and the repair it feeds.

`check --check deep` is the one that looks at consumer groups - the classic
Kafka migration failure is moving the data and not the committed offsets, so
every consumer restarts from earliest or latest and double-processes or drops.
Measured against the client migkit installs, it did not look at anything:

    AttributeError: 'KafkaAdminClient' object has no attribute
                    'list_consumer_groups'

That is a kafka-python 2.x name. On 3.0.11 the methods are `list_groups()`,
which answers with dicts rather than tuples, and `list_group_offsets(group)`,
which answers one level deeper - keyed by group.

With the check running, the repair is the natural next step, and a committed
offset is a counter that has to follow the data, which is what `--kind
sequences` means everywhere else in migkit.

The refusal matters as much as the repair. An offset is a position in one
cluster's log; two clusters only agree on what the number means when their logs
begin and end in the same place. migkit checks that per partition, copies only
where it holds, and names the rest rather than moving a consumer past messages
it never read.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-off-src", "migkit-test-off-dst"
SRC_PORT, DST_PORT = 19411, 19412


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def clusters():
    """Two single-node clusters with the same topic and the same messages,
    so an offset means the same thing on both until a test changes that."""
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run([
            "docker", "run", "-d", "--name", name, "-p", f"{port}:9092",
            "-e", "KAFKA_NODE_ID=1",
            "-e", "KAFKA_PROCESS_ROLES=broker,controller",
            "-e", "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,"
                  "CONTROLLER://0.0.0.0:9093",
            "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}",
            "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,"
                  "PLAINTEXT:PLAINTEXT",
            "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "apache/kafka:3.8.0"], check=True, capture_output=True)
    for port in (SRC_PORT, DST_PORT):
        if not _wait(port):
            pytest.skip("kafka did not come up in this sandbox")
    for port in (SRC_PORT, DST_PORT):
        for _ in range(30):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=f"127.0.0.1:{port}")
                admin.create_topics([NewTopic("orders", num_partitions=2,
                                              replication_factor=1)])
                admin.close()
                break
            except Exception:
                time.sleep(2)
        else:
            pytest.fail(f"could not create the topic on {port}")
    time.sleep(3)
    for port in (SRC_PORT, DST_PORT):
        producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{port}")
        for i in range(100):
            producer.send("orders", key=f"k{i}".encode(),
                          value=f"v{i}".encode(), partition=i % 2)
        producer.flush()
        producer.close()
    yield
    for name in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def _engine(tmp_path):
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="k", engine="kafka",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              db_map={"cluster": "cluster"})
    hop.report_dir = lambda db=None: tmp_path
    return KafkaEngine(hop)


def _commit(port, group, offsets):
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata
    consumer = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}",
                             group_id=group, enable_auto_commit=False)
    tps = {TopicPartition("orders", p): OffsetAndMetadata(o, "", -1)
           for p, o in offsets.items()}
    consumer.assign(list(tps))
    consumer.commit(tps)
    consumer.close()


def _committed(port, group):
    from kafka.admin import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{port}")
    try:
        got = admin.list_group_offsets(group).get(group, {})
    finally:
        admin.close()
    return {tp.partition: meta.offset for tp, meta in got.items()
            if meta.offset >= 0}


def _by_scope(results):
    return {r.scope: r for r in results}


def test_the_client_really_does_lack_the_old_method_names(clusters):
    """Pinned because the whole check was written against them."""
    from kafka.admin import KafkaAdminClient
    assert not hasattr(KafkaAdminClient, "list_consumer_groups")
    assert not hasattr(KafkaAdminClient, "list_consumer_group_offsets")
    assert hasattr(KafkaAdminClient, "list_groups")
    assert hasattr(KafkaAdminClient, "alter_group_offsets")


def test_the_deep_check_runs_and_finds_the_group(clusters, tmp_path):
    _commit(SRC_PORT, "billing", {0: 30, 1: 20})
    got = _by_scope(_engine(tmp_path).check_deep("cluster"))
    assert got["groups"].status == "diff", got["groups"].detail
    assert "billing" in got["groups"].detail
    assert "--kind sequences" in got["groups"].fix_hint
    assert got["group-offsets"].status == "diff", got["group-offsets"].detail
    assert "src=30 dst=None" in got["group-offsets"].detail


def test_the_repair_commits_the_source_offsets_on_the_target(clusters,
                                                             tmp_path):
    _commit(SRC_PORT, "billing", {0: 30, 1: 20})
    eng = _engine(tmp_path)
    eng.check_deep("cluster")
    actions = eng.repair_plan("cluster", "sequences")
    assert len(actions) == 1, actions
    assert "commit 2 offsets" in actions[0].statements[0]
    eng.apply("cluster", actions[0])

    assert _committed(DST_PORT, "billing") == {0: 30, 1: 20}
    after = _by_scope(eng.check_deep("cluster"))
    assert after["groups"].status == "ok", after["groups"].detail
    assert after["group-offsets"].status == "ok", after["group-offsets"].detail


def test_what_the_target_had_before_goes_to_undo(clusters, tmp_path):
    _commit(SRC_PORT, "billing", {0: 30, 1: 20})
    _commit(DST_PORT, "billing", {0: 5, 1: 5})
    eng = _engine(tmp_path)
    eng.check_deep("cluster")
    for action in eng.repair_plan("cluster", "sequences"):
        eng.apply("cluster", action)

    saved = [json.loads(line) for line in
             (tmp_path / "undo" / "group-offsets.jsonl").read_text()
             .splitlines()]
    assert {(r["partition"], r["offset"]) for r in saved} == {(0, 5), (1, 5)}
    assert _committed(DST_PORT, "billing") == {0: 30, 1: 20}


def test_offsets_are_not_copied_between_logs_that_do_not_line_up(clusters,
                                                                 tmp_path):
    """The refusal. Ten more messages on one side and the same number means
    a different message, so copying it moves a consumer past records it
    never read. Where the message the group reads next is not on the
    target at all, there is nowhere to put the group either."""
    from kafka import KafkaProducer
    _commit(SRC_PORT, "billing", {0: 30, 1: 20})
    eng = _engine(tmp_path)
    eng.check_deep("cluster")
    for action in eng.repair_plan("cluster", "sequences"):
        eng.apply("cluster", action)

    producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{DST_PORT}")
    for _ in range(10):
        producer.send("orders", value=b"only on the target", partition=0)
    producer.flush()
    producer.close()
    producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
    for _ in range(3):
        producer.send("orders", value=b"only on the source", partition=0)
    producer.flush()
    producer.close()
    # the next message this group reads is one the target never got
    _commit(SRC_PORT, "billing", {0: 51})
    before = _committed(DST_PORT, "billing")

    got = _by_scope(eng.check_deep("cluster"))
    unsure = got["group-offsets not comparable"]
    assert unsure.status == "warn", unsure.detail
    assert "orders[0]" in unsure.detail, unsure.detail
    assert "do not start and end together" in unsure.detail
    assert "the message it reads next is not on the target" in \
        unsure.detail, unsure.detail
    assert "MirrorMaker2" in unsure.fix_hint or \
        "reset-offsets" in unsure.fix_hint
    # the pass on the rest says what it did not look at
    assert "not comparable" in got["group-offsets"].detail, \
        got["group-offsets"].detail

    assert eng.repair_plan("cluster", "sequences") == []
    assert _committed(DST_PORT, "billing") == before


def test_nothing_to_repair_once_the_two_agree(clusters, tmp_path):
    _commit(SRC_PORT, "billing", {1: 20})
    _commit(DST_PORT, "billing", {1: 20})
    eng = _engine(tmp_path)
    eng.check_deep("cluster")
    assert eng.repair_plan("cluster", "sequences") == []
    assert eng.repair_plan("cluster", "rows") == []


def test_an_unreachable_cluster_is_an_error_not_an_empty_list(tmp_path):
    """No server at all: listing no groups is not the same as there being
    none, and `sync` reads an empty list as nothing to do."""
    from migkit.engines.kafka import KafkaEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="", password="")
    hop = Hop(name="k", engine="kafka", source=ep, target=ep,
              db_map={"cluster": "cluster"})
    hop.report_dir = lambda db=None: tmp_path
    got = KafkaEngine(hop).check_deep("cluster")
    assert got[0].status == "error", [(r.status, r.detail) for r in got]
    assert "not the same as there being none" in got[0].detail


def test_a_group_is_put_at_the_same_message_where_the_logs_do_not_line_up(
        clusters, tmp_path):
    """The target's partition began with seven messages the source never
    had, then the source's twenty, written at the same times as on the
    source, as a mirror keeps them. The same message is seven further on,
    and the group is put there - not at the source's number, which on the
    target is seven messages short, and not refused."""
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    for port in (SRC_PORT, DST_PORT):
        admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{port}")
        admin.create_topics([NewTopic("shifted", num_partitions=1,
                                      replication_factor=1)])
        admin.close()
    time.sleep(2)
    dst = KafkaProducer(bootstrap_servers=f"127.0.0.1:{DST_PORT}")
    for i in range(7):
        dst.send("shifted", key=b"old", value=f"before {i}".encode(),
                 timestamp_ms=1_700_000_000_000 + i)
    dst.flush()
    src = KafkaProducer(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
    for producer in (src, dst):
        for i in range(20):
            producer.send("shifted", key=f"s{i}".encode(),
                          value=f"message {i}".encode(),
                          timestamp_ms=1_700_000_100_000 + i * 1000)
        producer.flush()
        producer.close()
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata
    for port, offset in ((SRC_PORT, 12), (DST_PORT, 12)):
        c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}",
                          group_id="shipping", enable_auto_commit=False)
        tp = TopicPartition("shifted", 0)
        c.assign([tp])
        c.commit({tp: OffsetAndMetadata(offset, "", -1)})
        c.close()
    eng = _engine(tmp_path)
    got = _by_scope(eng.check_deep("cluster"))
    # the same number on the target is a different message: a difference
    assert "shipping shifted[0] src=12 dst=12 (the same message is at 19" \
        in got["group-offsets"].detail, got["group-offsets"].detail
    for action in eng.repair_plan("cluster", "sequences"):
        eng.apply("cluster", action)
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{DST_PORT}",
                      group_id="shipping", enable_auto_commit=False)
    tp = TopicPartition("shifted", 0)
    assert c.committed(tp) == 19
    c.assign([tp])
    c.seek(tp, 19)
    first = next(m for batch in [c.poll(timeout_ms=5000)]
                 for msgs in batch.values() for m in msgs)
    assert (first.key, first.value) == (b"s12", b"message 12")
    c.close()
    after = _by_scope(eng.check_deep("cluster"))
    assert "shipping shifted" not in after["group-offsets"].detail, \
        after["group-offsets"].detail
