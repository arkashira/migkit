"""What Kafka checks do when a partition cannot be read at all.

`_tail_hash` returned the word `unavailable` in the digest's place whenever it
could not reach a partition's offsets. Both sides of a comparison return the
same word, so two partitions nobody could read compared equal and the check
reported "last 200 messages hash-equal". Measured against the cluster this
file builds, with one of its two brokers stopped, that is an everyday path:

    partition with no leader   describe_topics -> leader_id -1, error_code 5
    asking for its offsets     KeyError(TopicPartition(...)) while metadata
                               is stale, KafkaTimeoutError once it is fresh

The delta check had the worse version of it. A partition it could not read was
dropped from the reading of current offsets, and the reading is what gets
written back as the new baseline - so the partition's old baseline was erased,
and when the broker came back the partition looked new and defaulted to
wherever it now was. Everything written in between was never compared by
anyone. The last test here writes messages, takes the broker away, and proves
those messages are still found afterwards.

The cluster is three containers rather than two: a controller of its own, so
stopping a broker leaves the quorum intact and only costs the partitions that
lived there.
"""
import json
import socket
import subprocess
import time

import pytest

NET = "migkit-test-knet"
CTL, B1, B2 = "migkit-test-kctl", "migkit-test-kbr1", "migkit-test-kbr2"
B1_PORT, B2_PORT = 19292, 19293
CLUSTER_ID = "5L6g3nShT-eMCtK--X86sw"


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


def _kafka(*args, container=B1):
    return subprocess.run(
        ["docker", "exec", container, "/opt/kafka/bin/kafka-topics.sh",
         "--bootstrap-server", f"{B1}:9094", *args],
        capture_output=True, text=True)


def _produce(topic, count):
    r = subprocess.run(
        ["docker", "exec", B1, "sh", "-c",
         f"seq 1 {count} | /opt/kafka/bin/kafka-console-producer.sh"
         f" --bootstrap-server {B1}:9094 --topic {topic}"],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-300:]


def _describe(topic):
    return _kafka("--describe", "--topic", topic).stdout


@pytest.fixture(scope="module")
def cluster():
    for n in (CTL, B1, B2):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    subprocess.run(["docker", "network", "rm", NET], capture_output=True)
    subprocess.run(["docker", "network", "create", NET], check=True,
                   capture_output=True)
    subprocess.run([
        "docker", "run", "-d", "--name", CTL, "--network", NET,
        "-e", "KAFKA_NODE_ID=1", "-e", "KAFKA_PROCESS_ROLES=controller",
        "-e", "KAFKA_LISTENERS=CONTROLLER://0.0.0.0:9093",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", f"KAFKA_CONTROLLER_QUORUM_VOTERS=1@{CTL}:9093",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,"
              "PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT",
        "-e", f"CLUSTER_ID={CLUSTER_ID}", "apache/kafka:3.8.0"],
        check=True, capture_output=True)
    for name, node, port in ((B1, 2, B1_PORT), (B2, 3, B2_PORT)):
        subprocess.run([
            "docker", "run", "-d", "--name", name, "--network", NET,
            "-p", f"{port}:9092",
            "-e", f"KAFKA_NODE_ID={node}", "-e", "KAFKA_PROCESS_ROLES=broker",
            "-e", "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,"
                  "INTERNAL://0.0.0.0:9094",
            "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port},"
                  f"INTERNAL://{name}:9094",
            "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
            "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e", f"KAFKA_CONTROLLER_QUORUM_VOTERS=1@{CTL}:9093",
            "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,"
                  "PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT",
            "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=2",
            "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=2",
            "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "-e", f"CLUSTER_ID={CLUSTER_ID}", "apache/kafka:3.8.0"],
            check=True, capture_output=True)
    if not (_wait(B1_PORT) and _wait(B2_PORT)):
        pytest.skip("the kafka cluster did not come up in this sandbox")
    for _ in range(45):
        if _kafka("--list").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.skip("the kafka cluster never answered")
    # one replica each, so stopping a broker takes its partitions away
    assert _kafka("--create", "--topic", "one", "--partitions", "2",
                  "--replication-factor", "1").returncode == 0
    # two replicas, so the same stop leaves it readable but under-replicated
    assert _kafka("--create", "--topic", "two", "--partitions", "2",
                  "--replication-factor", "2").returncode == 0
    _produce("one", 120)
    _produce("two", 120)
    yield
    for n in (CTL, B1, B2):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _engine(tmp_path, port=B1_PORT):
    from migkit.config import Endpoint, Hop
    from migkit.engines.kafka import KafkaEngine
    ep = Endpoint(host="127.0.0.1", port=port, user="", password="")
    hop = Hop(name="k", engine="kafka", source=ep, target=ep,
              db_map={"cluster": "cluster"})
    hop.report_dir = lambda db=None: tmp_path
    return KafkaEngine(hop)


def _stopped_broker():
    """Take one broker away and wait until the cluster admits it."""
    subprocess.run(["docker", "stop", B2], check=True, capture_output=True)
    for _ in range(30):
        if "Leader: none" in _describe("one") or "Leader: -1" in _describe(
                "one"):
            return
        time.sleep(2)
    pytest.fail("the cluster never reported the partitions as leaderless")


def _started_broker():
    subprocess.run(["docker", "start", B2], check=True, capture_output=True)
    for _ in range(40):
        out = _describe("one")
        if out and "Leader: none" not in out and "Leader: -1" not in out:
            return
        time.sleep(2)
    pytest.fail("the broker never rejoined")


@pytest.fixture
def degraded(cluster):
    _stopped_broker()
    yield
    _started_broker()


def test_a_healthy_cluster_compares_every_partition(cluster, tmp_path):
    res = _engine(tmp_path).check_data("cluster")
    assert [r.status for r in res] == ["ok"], [(r.status, r.detail)
                                               for r in res]
    assert "on 4 partitions" in res[0].detail, res[0].detail


def test_a_partition_with_no_leader_is_an_error_not_a_match(degraded,
                                                            tmp_path):
    """The bug itself. Both sides fail the same way, which used to be read as
    the two sides agreeing."""
    res = _engine(tmp_path).check_data("cluster")
    assert res, "a degraded cluster produced no rows at all"
    assert not any(r.status == "ok" for r in res), [
        (r.status, r.detail) for r in res]
    assert not any("hash-equal" in r.detail for r in res), [
        r.detail for r in res]
    bad = [r for r in res if r.status == "error"]
    assert bad, [(r.status, r.detail) for r in res]
    assert "could not be read" in bad[0].detail
    assert "one[" in bad[0].detail, bad[0].detail
    # and it says what the cluster says, not only what the client raised
    assert "has no leader" in bad[0].detail, bad[0].detail
    assert "no leader cannot be read" in bad[0].fix_hint


def test_offline_and_under_replicated_are_told_apart(degraded, tmp_path):
    """Measured, and the reason the leader is what gets checked: an offline
    partition still lists the broker that is gone in its in-sync set, so
    comparing in-sync against replicas does not find it."""
    eng = _engine(tmp_path)
    offline = eng._partition_state("src", "one")
    behind = eng._partition_state("src", "two")
    assert offline, "the stopped broker's partitions were reported as fine"
    assert all("has no leader" in why for why in offline.values()), offline
    assert behind, "the replicated topic was not reported as under-replicated"
    assert all("under-replicated" in why for why in behind.values()), behind

    from kafka.admin import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{B1_PORT}",
                             request_timeout_ms=15000)
    try:
        described = admin.describe_topics(["one"])
    finally:
        admin.close()
    stale = [p for t in described for p in t["partitions"]
             if p["leader_id"] < 0]
    assert stale, described
    for p in stale:
        assert set(p["isr_nodes"]) == set(p["replica_nodes"]), p
        assert p["offline_replicas"], p


def test_a_topic_that_is_not_there_is_not_zero_partitions_matching(cluster,
                                                                   tmp_path):
    res = _engine(tmp_path).check_data("cluster", table="ghost")
    assert [r.status for r in res] == ["error"], [(r.status, r.detail)
                                                  for r in res]
    assert "no partitions on the source" in res[0].detail


def test_the_delta_keeps_its_baseline_over_a_partition_it_cannot_read(
        cluster, tmp_path):
    """The whole point, end to end: messages written before a broker goes
    away are still found after it comes back."""
    eng = _engine(tmp_path)
    state = tmp_path / "delta-offsets.json"
    first = eng.delta_verify("cluster")
    assert first[0].status == "ok", [(r.status, r.detail) for r in first]
    baseline = json.loads(state.read_text())
    assert len(baseline) == 4, baseline

    # everything from here goes to one partition of `one`: the console
    # producer sends a batch to a single partition when the keys are absent
    _produce("one", 40)
    written = json.loads(state.read_text())
    assert written == baseline, "the file moved before a cycle ran"

    _stopped_broker()
    try:
        during = eng.delta_verify("cluster")
        assert during[0].status == "error", [(r.status, r.detail)
                                             for r in during]
        assert "NOT advanced" in during[0].detail
        assert any("could not be read" in r.detail for r in during[1:]), [
            r.detail for r in during]
        assert json.loads(state.read_text()) == baseline, (
            "the baseline moved while a partition could not be read - the"
            " messages on it would never be compared by anyone")
    finally:
        _started_broker()

    after = eng.delta_verify("cluster")
    assert all(r.status == "ok" for r in after), [(r.status, r.detail)
                                                  for r in after]
    moved = json.loads(state.read_text())
    assert moved != baseline, moved
    assert sum(moved.values()) - sum(baseline.values()) == 40, (baseline,
                                                                moved)
