"""Kafka clusters that ask who is connecting: SASL, as Amazon MSK with
SCRAM, Confluent Cloud and Azure Event Hubs' Kafka endpoint ask for it.

Every client migkit makes for a side - consumer, producer, admin - is
given the side's endpoint options: `security_protocol`, `sasl_mechanism`,
`hosts`, `ssl_cafile`, with the endpoint's user and password as the
account. Measured against a broker that requires SCRAM: topics are listed
and a change is delivered with the right password. The wrong one is said
as a refused sign-in, where the client on its own said only that it could
not reach the cluster.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

RP, PORT = "migkit-test-sasl", 15846


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
def broker():
    subprocess.run(["docker", "rm", "-f", "-v", RP], capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--name", RP, "-p", f"{PORT}:{PORT}",
         "redpandadata/redpanda:latest", "redpanda", "start",
         "--overprovisioned", "--smp", "1", "--memory", "384M",
         "--reserve-memory", "0M", "--node-id", "0", "--check=false",
         "--kafka-addr", f"PLAINTEXT://0.0.0.0:{PORT}",
         "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{PORT}",
         "--set", "redpanda.enable_sasl=true",
         "--set", "redpanda.superusers=[\"migkit\"]"],
        check=True, capture_output=True)
    try:
        assert _wait(PORT)
        for _ in range(30):
            made = subprocess.run(
                ["docker", "exec", RP, "rpk", "acl", "user", "create",
                 "migkit", "-p", "CHANGE_ME-sasl", "--mechanism",
                 "SCRAM-SHA-256"], capture_output=True, text=True)
            if made.returncode == 0:
                break
            time.sleep(2)
        assert made.returncode == 0, made.stderr
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", RP], capture_output=True)


def _engine(tmp_path, password="CHANGE_ME-sasl", **options):
    from migkit.engines.kafka import KafkaEngine
    opts = {"security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "SCRAM-SHA-256",
            "hosts": [f"127.0.0.1:{PORT}"], **options}
    ep = Endpoint(user="migkit", password=password, options=opts)
    hop = Hop(name="sasl", engine="kafka", source=ep, target=ep,
              db_map={"cluster": "cluster"}, databases=["cluster"],
              options={"topic": "delivered.{table}"})
    hop.report_dir = lambda db=None: tmp_path
    return KafkaEngine(hop)


def test_signed_in_it_lists_and_delivers(broker, tmp_path):
    eng = _engine(tmp_path)
    eng.neutral_apply("dst", "app", [{"op": "insert", "table": "t",
                                      "key": {"id": 1},
                                      "values": {"id": 1, "v": "x"}}])
    admin = eng._admin("src")
    try:
        assert "delivered.t" in admin.list_topics()
    finally:
        admin.close()
    consumer = eng._consumer("src")
    try:
        assert "delivered.t" in eng._topics(consumer)
    finally:
        consumer.close()


def test_the_wrong_password_is_refused_not_an_empty_cluster(broker,
                                                           tmp_path):
    eng = _engine(tmp_path, password="CHANGE_ME-wrong")
    with pytest.raises(SystemExit) as e:
        eng._admin("src")
    assert str(e.value).startswith("the source cluster refused the"
                                   " sign-in: Invalid credentials"), e.value


def test_a_protocol_it_does_not_know_is_refused(tmp_path):
    with pytest.raises(SystemExit) as e:
        _engine(tmp_path, security_protocol="KERBEROS")._connection("src")
    assert "security_protocol: KERBEROS is not one of" in str(e.value)
