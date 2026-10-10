"""A Kafka cluster's client quotas are compared, carried and taken back
(R13.kafka).

A target without the source's quotas lets a client that was throttled run
unbounded the moment it is pointed there. Carried as the source holds
them; the target's own are left alone; a rollback puts back what the
target held before, and unsets what it did not hold.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-kquo-src", 15913), ("migkit-test-kquo-dst", 15914)
ADMIN = ("--user", "migkit", "--password", "CHANGE_ME-sasl",
         "--sasl-mechanism", "SCRAM-SHA-256")


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def rpk(side, *args):
    got = subprocess.run(["docker", "exec", side[0], "rpk", *args, *ADMIN],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout


@pytest.fixture(scope="module")
def clusters():
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", name, "-p", f"{port}:{port}",
             "redpandadata/redpanda:latest", "redpanda", "start",
             "--overprovisioned", "--smp", "1", "--memory", "384M",
             "--reserve-memory", "0M", "--node-id", "0", "--check=false",
             "--kafka-addr", f"PLAINTEXT://0.0.0.0:{port}",
             "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{port}",
             "--set", "redpanda.enable_sasl=true",
             "--set", "redpanda.superusers=[\"migkit\"]"],
            check=True, capture_output=True)
    try:
        for name, port in (SRC, DST):
            assert _wait(port)
            for _ in range(30):
                made = subprocess.run(
                    ["docker", "exec", name, "rpk", "acl", "user", "create",
                     "migkit", "-p", "CHANGE_ME-sasl", "--mechanism",
                     "SCRAM-SHA-256"], capture_output=True, text=True)
                if made.returncode == 0:
                    break
                time.sleep(2)
            assert made.returncode == 0, made.stderr
        rpk(SRC, "cluster", "quotas", "alter", "--add",
            "producer_byte_rate=1000", "--name", "client-id=app")
        rpk(SRC, "cluster", "quotas", "alter", "--add",
            "consumer_byte_rate=2000", "--default", "client-id")
        # the target's own, and one the source also sets, differently
        rpk(DST, "cluster", "quotas", "alter", "--add",
            "producer_byte_rate=500", "--name", "client-id=ops")
        rpk(DST, "cluster", "quotas", "alter", "--add",
            "producer_byte_rate=9", "--name", "client-id=app")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path):
    def ep(port):
        return Endpoint(user="migkit", password="CHANGE_ME-sasl", options={
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "SCRAM-SHA-256",
            "hosts": [f"127.0.0.1:{port}"]})
    hop = Hop(name="kquo", engine="kafka", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["cluster"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _held(hop, side):
    from migkit import users
    return users._kafka_accounts(hop, side)["quotas"]


APP = (("client-id", "app"),)
OPS = (("client-id", "ops"),)
ANY = (("client-id", None),)


def test_quotas_the_target_lacks_or_sets_otherwise_are_named(clusters,
                                                             tmp_path):
    from migkit import users
    hop = _hop(tmp_path)
    assert _held(hop, "src")[APP] == {"producer_byte_rate": 1000.0}
    out, _ = users.compare(hop, [].append)
    got = {(tuple(tuple(x) for x in e), k, v)
           for e, k, v in out["quotas_missing"]}
    assert got == {(APP, "producer_byte_rate", 1000.0),
                   (ANY, "consumer_byte_rate", 2000.0)}, out
    assert out["result"] == "gap"


def test_create_carries_them_and_rollback_puts_back_what_was_there(
        clusters, tmp_path):
    from migkit import users
    hop = _hop(tmp_path)
    before = _held(hop, "dst")
    try:
        users.create(hop, apply=True, passwords={}, say=[].append)
    except SystemExit as e:
        # accounts the source has and no password was given for
        assert "quota" not in str(e), e
    after = _held(hop, "dst")
    assert after[APP] == {"producer_byte_rate": 1000.0}, after
    assert after[ANY] == {"consumer_byte_rate": 2000.0}, after
    assert after[OPS] == before[OPS], "the target's own quota is left alone"
    users.rollback(hop, apply=True, say=[].append)
    back = _held(hop, "dst")
    assert back[APP] == {"producer_byte_rate": 9.0}, back
    assert ANY not in back, back
    assert back[OPS] == before[OPS]
