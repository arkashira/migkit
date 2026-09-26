"""A Kafka hop keeps its target following the source, and fences on it
(backlog 0e).

The tail is the topic copier, round after round: each partition goes on
from the position the last round saved, and the first round from where
the copy of the topics ended, so nothing is sent twice. The fence waits
until every partition's saved position has reached the source's end.

A transaction leaves a marker at the end of a partition that no reader of
committed messages is ever given, so the end is never reached by a
message. The position is taken from the consumer there instead: without
that, a fence after a transactional write waited for an offset that never
arrives.
"""
import ctypes
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-test-follow-src", "migkit-test-follow-dst"
SRC_PORT, DST_PORT = 15854, 15855


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
    hop = Hop(name="follow", engine="kafka",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              db_map={"cluster": "cluster"})
    hop.report_dir = lambda db=None: tmp_path
    return KafkaEngine(hop)


def _produce(topic, start, n, **extra):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{SRC_PORT}",
                             **extra)
    if extra.get("transactional_id"):
        producer.init_transactions()
        producer.begin_transaction()
    for i in range(start, start + n):
        producer.send(topic, key=f"k{i % 7}".encode(),
                      value=f"event {i}".encode(), partition=i % 2)
    if extra.get("transactional_id"):
        producer.commit_transaction()
    producer.flush()
    producer.close()


def _all(port, topic):
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}",
                      enable_auto_commit=False,
                      isolation_level="read_committed")
    try:
        out = {}
        for p in sorted(c.partitions_for_topic(topic) or []):
            tp = TopicPartition(topic, p)
            c.assign([tp])
            c.seek_to_beginning(tp)
            got = []
            while True:
                batch = c.poll(timeout_ms=3000)
                if not batch:
                    break
                got += [(m.key, m.value) for msgs in batch.values()
                        for m in msgs]
            out[p] = got
        return out
    finally:
        c.close()


def test_the_tail_goes_on_from_the_copy_and_the_fence_passes(brokers,
                                                             tmp_path):
    from kafka.admin import KafkaAdminClient, NewTopic

    from migkit.cli import _Checkpoint
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
    admin.create_topics([NewTopic("events", num_partitions=2,
                                  replication_factor=1)])
    admin.close()
    time.sleep(2)
    _produce("events", 0, 100)
    eng = _engine(tmp_path)
    eng.move_table("cluster", "", "events", 40,
                   _Checkpoint(tmp_path / "move.json"), lambda m: None)
    assert eng.moved_nothing("cluster") == []

    said, ended, got = [], {}, {}

    def run():
        try:
            eng.tail_apply("cluster", True, tmp_path / "tail-token.json",
                           said.append)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        time.sleep(3)
        _produce("events", 100, 50)
        _produce("events", 150, 10, transactional_id="migkit-test-tx")
        at = eng.src_lsn("cluster")
        got["fence"] = eng.fence_wait("cluster", at, timeout=90)
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    assert at and got["fence"] is True, (at, said)
    src, dst = _all(SRC_PORT, "events"), _all(DST_PORT, "events")
    # every message once, in each partition's order
    assert {p: len(m) for p, m in dst.items()} == {0: 80, 1: 80}
    assert dst == src
    # not running: nothing to fence on
    assert eng.src_lsn("cluster") is None


def test_a_topic_copied_to_nothing_is_named(brokers, tmp_path):
    from kafka.admin import KafkaAdminClient, NewTopic
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{SRC_PORT}")
    admin.create_topics([NewTopic("lost", num_partitions=2,
                                  replication_factor=1)])
    admin.close()
    time.sleep(2)
    _produce("lost", 0, 3)
    assert "lost" in _engine(tmp_path).moved_nothing("cluster")
