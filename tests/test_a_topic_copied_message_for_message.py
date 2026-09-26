"""Kafka copies a topic message for message, resumably (backlog 0e).

Each partition's messages go into the same partition on the target, with
their keys, values, headers and times: the times are what a consumer
group's position is translated by afterwards. The topic is made with the
source's partition count and the configs that decide what it means.

The copy is stopped as a crash would stop it: after a batch has reached
the target and before the checkpoint recorded it. Resumed, it counts that
batch off the target's own end rather than sending it twice.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-test-topic-src", "migkit-test-topic-dst"
SRC_PORT, DST_PORT = 15844, 15845


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module")
def brokers():
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", name, "-p", f"{port}:{port}",
             "redpandadata/redpanda:latest", "redpanda", "start",
             "--overprovisioned", "--smp", "1", "--memory", "384M",
             "--reserve-memory", "0M", "--node-id", "0", "--check=false",
             "--kafka-addr", f"PLAINTEXT://0.0.0.0:{port}",
             "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{port}"],
            check=True, capture_output=True)
    try:
        for port in (SRC_PORT, DST_PORT):
            assert _wait(port), "a broker never answered"
        time.sleep(5)
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path):
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="tc", engine="kafka",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              db_map={"cluster": "cluster"})
    hop.report_dir = lambda db=None: tmp_path
    return KafkaEngine(hop)


def _all(port, topic):
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}",
                      enable_auto_commit=False)
    try:
        out = {}
        for p in sorted(c.partitions_for_topic(topic) or []):
            tp = TopicPartition(topic, p)
            c.assign([tp])
            c.seek_to_beginning(tp)
            end = c.end_offsets([tp])[tp]
            got = []
            while len(got) < end:
                batch = c.poll(timeout_ms=5000)
                if not batch:
                    break
                got += [(m.key, m.value, list(m.headers or []), m.timestamp)
                        for msgs in batch.values() for m in msgs]
            out[p] = got
        return out
    finally:
        c.close()


class _Crash(Exception):
    pass


def test_a_topic_arrives_whole_after_a_crash_between_batch_and_checkpoint(
        brokers, tmp_path):
    from kafka import KafkaProducer
    from kafka.admin import ConfigResource, ConfigResourceType, \
        KafkaAdminClient, NewTopic

    from migkit.cli import _Checkpoint
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
    admin.create_topics([NewTopic("orders", num_partitions=3,
                                  replication_factor=1,
                                  topic_configs={"retention.ms":
                                                 "86400000"})])
    admin.close()
    time.sleep(2)
    producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
    for i in range(300):
        producer.send("orders", key=f"k{i % 17}".encode(),
                      value=f"order {i}".encode(), partition=i % 3,
                      headers=[("trace", f"t{i}".encode())],
                      timestamp_ms=1_700_000_000_000 + i)
    producer.flush()
    producer.close()

    eng = _engine(tmp_path)
    assert eng.list_move_tables("cluster") == [("", "orders")]
    saved = {"n": 0}

    class Crashing(_Checkpoint):
        def save(self):
            saved["n"] += 1
            if saved["n"] == 3:
                # the batch is on the target; the checkpoint never hears
                raise _Crash()
            super().save()
    ck = Crashing(tmp_path / "move.json")
    first = []
    with pytest.raises(_Crash):
        eng.move_table("cluster", "", "orders", 40, ck, first.append)
    assert "orders: made on the target with 3 partitions" in first, first
    said = []
    eng.move_table("cluster", "", "orders", 40,
                   _Checkpoint(tmp_path / "move.json"), said.append)
    src, dst = _all(SRC_PORT, "orders"), _all(DST_PORT, "orders")
    # every partition the same messages, in the same order, none twice
    assert {p: len(m) for p, m in dst.items()} == {0: 100, 1: 100, 2: 100}
    assert dst == src
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{DST_PORT}")
    try:
        got = admin.describe_configs([ConfigResource(
            ConfigResourceType.TOPIC, "orders")])
    finally:
        admin.close()
    text = str(got)
    assert "86400000" in text, text


def test_a_topic_copied_before_gets_what_arrived_since(brokers, tmp_path):
    """Run again, a finished topic is not skipped: it goes on from the
    positions the copy saved, and sends nothing twice."""
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    from migkit.cli import _Checkpoint
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
    admin.create_topics([NewTopic("grows", num_partitions=1,
                                  replication_factor=1)])
    admin.close()
    time.sleep(2)

    def send(start, n):
        producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
        for i in range(start, start + n):
            producer.send("grows", key=b"k", value=f"m{i}".encode())
        producer.flush()
        producer.close()
    send(0, 10)
    eng = _engine(tmp_path)
    eng.move_table("cluster", "", "grows", 40,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    send(10, 5)
    said = []
    eng.move_table("cluster", "", "grows", 40,
                   _Checkpoint(tmp_path / "move.json"), said.append)
    assert "grows: done earlier; copying what arrived since" in said, said
    got = [v for _, v, _, _ in _all(DST_PORT, "grows")[0]]
    assert got == [f"m{i}".encode() for i in range(15)], got
