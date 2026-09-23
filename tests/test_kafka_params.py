"""Kafka's broker settings, compared like every engine's server settings.

A broker's defaults are what a topic gets when a producer creates it by
writing to it: a partition count, a retention, whether it may be created
at all. A target whose defaults differ turns the same traffic into
different topics, and nothing compared them.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = pytest.mark.docker

SRC, DST = "migkit-test-kparams-src", "migkit-test-kparams-dst"
SRC_PORT, DST_PORT = 15669, 15670


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _broker(name, port, extra=()):
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
        *extra, "apache/kafka:3.8.0"], check=True, capture_output=True)


@pytest.fixture(scope="module")
def brokers():
    try:
        _broker(SRC, SRC_PORT)
        # the target creates topics with 6 partitions where the source
        # makes 1: the difference this check exists to catch
        _broker(DST, DST_PORT, ["-e", "KAFKA_NUM_PARTITIONS=6"])
        for port in (SRC_PORT, DST_PORT):
            if not _wait(port):
                pytest.skip("kafka did not come up in this sandbox")
        time.sleep(5)
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path, dst_port=DST_PORT):
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="k", engine="kafka",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=dst_port, user="",
                              password=""))
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return KafkaEngine(hop)


def test_a_different_broker_default_fails_the_check(brokers, tmp_path):
    got = _engine(tmp_path).check_params("cluster")
    said = " | ".join(f"{r.status} {r.detail}" for r in got)
    assert any(r.status == "diff" for r in got), said
    assert "num.partitions" in said, said


def test_a_broker_it_cannot_read_is_not_a_match(brokers, tmp_path):
    got = _engine(tmp_path, dst_port=1).check_params("cluster")
    assert got and all(r.status != "ok" for r in got), \
        [(r.status, r.detail) for r in got]


def test_a_topic_retention_difference_is_reported_again(brokers, tmp_path):
    """The topic-settings comparison had been skipped without a word: the
    reader was written for the old client's response objects, raised on
    this client's dicts, and the caller dropped the comparison when the
    reader answered None. A retention drift - which decides how long the
    target keeps what it is given - went unreported."""
    from kafka.admin import KafkaAdminClient, NewTopic
    for port, ms in ((SRC_PORT, "86400000"), (DST_PORT, "3600000")):
        admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{port}")
        try:
            admin.create_topics([NewTopic("ledger", num_partitions=1,
                                          replication_factor=1,
                                          topic_configs={"retention.ms": ms})])
        finally:
            admin.close()
    time.sleep(3)
    got = [r for r in _engine(tmp_path).check_schema("cluster")
           if r.scope == "topic-configs"]
    said = " | ".join(f"{r.status} {r.detail}" for r in got)
    assert got and got[0].status == "diff", said
    assert "ledger retention.ms" in said, said
