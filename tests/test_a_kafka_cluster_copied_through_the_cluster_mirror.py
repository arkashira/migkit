"""A Kafka cluster copied through the cluster mirror Kafka ships (backlog
F7.mm2), run from Kafka's own image.

The source holds two partitions of keyed messages with headers, and a
transaction that was aborted: the mirror, told to read committed messages
only, carries none of the aborted ones (its Java consumer's default would
copy them as data). The target began with three messages of its own in
one partition, so no number lines up and a group has to be placed by the
message - here through the mirror's own record of where each offset
landed, proved by the message before it is taken.

The copy ends where the source stood when it began, and each partition's
position is kept where migkit's copier keeps it: more written to the
source afterwards is carried by the copier with nothing sent twice. The
mirror's own topics on the target are its bookkeeping, not the
application's, and the check does not count them as extra topics.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.bulk", "kafka")]

pytestmark = needs_docker

SRC, DST = "migkit-test-mirror-src", "migkit-test-mirror-dst"
SRC_PORT, DST_PORT = 16284, 16285


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _broker(name, port):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run([
        "docker", "run", "-d", "--name", name,
        "-p", f"127.0.0.1:{port}:{port}",
        "-e", "KAFKA_NODE_ID=1",
        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", f"KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:{port},"
              "CONTROLLER://0.0.0.0:9093",
        "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,"
              "PLAINTEXT:PLAINTEXT",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
        "apache/kafka:3.8.0"], check=True, capture_output=True)


def _produce(port, start, n):
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=f"127.0.0.1:{port}")
    for i in range(start, start + n):
        p.send("orders", key=f"k{i}".encode(), value=f"v{i}".encode(),
               headers=[("h", f"{i}".encode())], partition=i % 2,
               timestamp_ms=1_700_000_000_000 + i)
    p.flush()
    p.close()


@pytest.fixture(scope="module")
def clusters():
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from migkit import mirror
    if mirror.runner() == "":
        pytest.skip("the cluster mirror cannot run here")
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        _broker(name, port)
    try:
        for port in (SRC_PORT, DST_PORT):
            if not _wait(port):
                pytest.skip("kafka did not come up in this sandbox")
        for port in (SRC_PORT, DST_PORT):
            for _ in range(30):
                try:
                    admin = KafkaAdminClient(
                        bootstrap_servers=f"127.0.0.1:{port}")
                    admin.create_topics([NewTopic("orders", 2, 1)])
                    admin.close()
                    break
                except Exception:  # noqa: BLE001 - not up yet
                    time.sleep(2)
        time.sleep(2)
        p = KafkaProducer(bootstrap_servers=f"127.0.0.1:{DST_PORT}")
        for i in range(3):
            p.send("orders", value=b"the target's own", partition=0,
                   timestamp_ms=1_600_000_000_000)
        p.flush()
        p.close()
        _produce(SRC_PORT, 0, 2000)
        tx = KafkaProducer(bootstrap_servers=f"127.0.0.1:{SRC_PORT}",
                           transactional_id="migkit-test-aborted")
        tx.init_transactions()
        tx.begin_transaction()
        for _ in range(5):
            tx.send("orders", value=b"aborted", partition=1)
        tx.flush()
        tx.abort_transaction()
        tx.close()
        from kafka import KafkaConsumer, TopicPartition
        from kafka.structs import OffsetAndMetadata
        c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{SRC_PORT}",
                          group_id="billing", enable_auto_commit=False)
        tp = TopicPartition("orders", 0)
        c.assign([tp])
        c.commit({tp: OffsetAndMetadata(700, "", -1)})
        c.close()
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path):
    hop = Hop(name="mirror", engine="kafka",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              db_map={"cluster": "cluster"})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _messages(port, partition):
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}",
                      enable_auto_commit=False,
                      isolation_level="read_committed")
    tp = TopicPartition("orders", partition)
    c.assign([tp])
    c.seek_to_beginning(tp)
    out = []
    while True:
        batch = c.poll(timeout_ms=5000)
        if not batch:
            break
        out += [(m.key, m.value, list(m.headers or []), m.timestamp)
                for msgs in batch.values() for m in msgs]
    c.close()
    return out


class _Ck(dict):
    def __init__(self, path):
        super().__init__(json.loads(path.read_text()))
        self.path = path

    def save(self):
        self.path.write_text(json.dumps(self))


def test_the_mirror_carries_every_committed_message_and_no_more(clusters,
                                                                tmp_path):
    from migkit import mirror
    from migkit.engines.kafka import KafkaEngine
    from kafka.admin import KafkaAdminClient
    said = []
    hop = _hop(tmp_path)
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
    before = set(admin.list_topics())
    mirror.move(hop, "cluster", 2, True, said.append)
    # nothing written to the source: no heartbeat, no worker's topics
    assert set(admin.list_topics()) == before, \
        set(admin.list_topics()) ^ before
    admin.close()
    assert any("messages" in str(s) for s in said), said
    for p in (0, 1):
        src = _messages(SRC_PORT, p)
        dst = [m for m in _messages(DST_PORT, p)
               if m[1] != b"the target's own"]
        assert len(src) == 1000, len(src)
        assert dst == src, (p, len(dst), len(src))
        assert not [m for m in dst if m[1] == b"aborted"]
    # each partition's position where the copier keeps its own: after the
    # last committed message - the aborted ones and their marker, past it
    # in partition 1, are not waited for
    state = json.loads((tmp_path / "move.json").read_text())
    parts = state["topic.orders"]["parts"]
    assert {p: parts[p]["next"] for p in parts} == {"0": 1000, "1": 1000}
    eng = KafkaEngine(hop)
    checked = {r.scope: r for r in eng.check_schema("cluster")}
    assert checked["topics"].status == "ok", checked["topics"].detail


def test_the_copier_goes_on_from_where_the_mirror_stopped(clusters,
                                                          tmp_path):
    from migkit import mirror
    from migkit.engines.kafka import KafkaEngine
    hop = _hop(tmp_path)
    mirror.move(hop, "cluster", 2, True, None)
    _produce(SRC_PORT, 2000, 100)
    eng = KafkaEngine(hop)
    ck = _Ck(tmp_path / "move.json")
    eng.move_table("cluster", "", "orders", 1000, ck, lambda m: None)
    for p in (0, 1):
        src = _messages(SRC_PORT, p)
        dst = [m for m in _messages(DST_PORT, p)
               if m[1] != b"the target's own"]
        # nothing sent twice, nothing skipped
        assert dst == src, (p, len(dst), len(src))


def _offset_of(port, partition, value):
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}",
                      enable_auto_commit=False)
    tp = TopicPartition("orders", partition)
    c.assign([tp])
    c.seek_to_beginning(tp)
    try:
        while True:
            batch = c.poll(timeout_ms=5000)
            if not batch:
                return None
            for msgs in batch.values():
                for m in msgs:
                    if m.value == value:
                        return m.offset
    finally:
        c.close()


def test_a_group_is_placed_through_the_mirrors_record(clusters, tmp_path):
    """The mirror writes each batch in a transaction, whose marker takes
    an offset on the target: its record of where each offset landed is
    read as it wrote it, and a group is put at the message itself."""
    from migkit import mirror
    from migkit.engines.kafka import KafkaEngine
    hop = _hop(tmp_path)
    eng = KafkaEngine(hop)
    syncs = mirror.read_table(eng, "syncs")
    first = syncs.get(("orders", 0))
    assert first, syncs
    # its first record is the source's first message, where it landed
    assert first[0][0] == 0
    assert first[0][1] == _offset_of(DST_PORT, 0, b"v0")
    rows, unsure, _ = eng.group_positions("cluster")
    row = next(r for r in rows if r["group"] == "billing"
               and r["partition"] == 0)
    # the source's offset 700 of partition 0 is its 701st even message
    assert row["target"] == _offset_of(DST_PORT, 0, b"v1400"), row
    assert row["target"] >= 703
    assert row["how"] in (
        "by the cluster mirror's record of where it landed, proved by the"
        " message", "by the message it points at"), row
