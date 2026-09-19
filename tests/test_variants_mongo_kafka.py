"""Asking the last two engines what they are, instead of reading the address.

`mongodb.assess()` decided a server was Amazon DocumentDB when the hostname
contained `docdb`. A CNAME, a bastion or a connection string with a different
label defeats that, and it answers a question about the address rather than
about the software - which is the reason `variants.py` exists at all.

Both brands here are identified from what the server says. What migkit has not
been able to run against - DocumentDB, MSK, Confluent - has no signature, and
comes back unidentified rather than guessed at.
"""
import socket
import subprocess
import time

import pytest

from migkit import variants as v

MG, RP, AK = ("migkit-test-brand-mg", "migkit-test-brand-rp",
              "migkit-test-brand-ak")
MG_PORT, RP_PORT, AK_PORT = 27065, 9096, 9095


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


def _hop(engine, port, options=None):
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="127.0.0.1", port=port, user="", password="",
                  options=options or {})
    return Hop(name="b", engine=engine, source=ep, target=ep,
               db_map={"cx": "cx"})


# --- what the servers actually reported, kept as strings ------------------

MONGO_BUILDINFO = {"version": "7.0.43", "gitVersion": "ef5a7d34",
                   "javascriptEngine": "mozjs", "allocator": "tcmalloc",
                   "modules": [], "storageEngines": ["wiredTiger"]}
REDPANDA_ID = "redpanda.60bd60c1-e78e-423e-b877-f1caa1fc5835"
KAFKA_ID = "5L6g3nShT-eMCtK--X86sw"


def test_a_mongo_build_names_itself_and_a_thin_one_does_not():
    assert v.identify("mongodb", MONGO_BUILDINFO).name == "mongodb"
    assert v.identify("mongodb", MONGO_BUILDINFO).version == "7.0.43"
    # what a compatible service returns instead: a version and little else.
    # migkit has not run against one, so it says so rather than naming it.
    thin = v.identify("mongodb", {"version": "5.0.0", "ok": 1})
    assert thin.name == "unknown", thin
    assert not thin.identified


def test_redpanda_is_told_apart_from_a_plain_kafka_broker():
    assert v.identify("kafka", {"cluster_id": REDPANDA_ID}).name == "redpanda"
    assert v.identify("kafka", {"cluster_id": KAFKA_ID}).name == "kafka"
    assert v.identify("kafka", {}).name == "unknown"


def test_the_bare_cluster_id_is_recorded_as_naming_less_than_it_seems():
    """MSK, Confluent and a self-hosted broker all report the same shape, so
    `kafka` is the protocol implementation and not the distribution. Saying
    so is the difference between a limit and a wrong answer."""
    why = v.identify("kafka", {"cluster_id": KAFKA_ID}).cannot(
        v.VERSION_IS_REAL)
    assert "MSK, Confluent" in why, why
    assert "rather than the distribution" in why, why


@pytest.fixture(scope="module")
def servers():
    for n in (MG, RP, AK):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"{MG_PORT}:27017", "mongo:7"], check=True,
                   capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", RP, "-p",
                    f"{RP_PORT}:9092", "redpandadata/redpanda:latest",
                    "redpanda", "start", "--overprovisioned", "--smp", "1",
                    "--memory", "1G", "--node-id", "0", "--check=false",
                    "--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
                    "--advertise-kafka-addr",
                    f"PLAINTEXT://127.0.0.1:{RP_PORT}"],
                   check=True, capture_output=True)
    subprocess.run([
        "docker", "run", "-d", "--name", AK, "-p", f"{AK_PORT}:9092",
        "-e", "KAFKA_NODE_ID=1",
        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,"
              "CONTROLLER://0.0.0.0:9093",
        "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{AK_PORT}",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="
              "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
        "apache/kafka:3.8.0"], check=True, capture_output=True)
    assert _wait(MG_PORT) and _wait(RP_PORT) and _wait(AK_PORT)
    time.sleep(12)
    yield
    for n in (MG, RP, AK):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def test_the_mongo_engine_asks_the_server_not_the_hostname(servers):
    """The hostname here says nothing at all, and the brand still comes
    back - which is the whole point of the change."""
    from migkit.engines.mongodb import MongoEngine
    eng = MongoEngine(_hop("mongodb", MG_PORT))
    src, _ = eng._brands()
    assert src.name == "mongodb", src
    assert src.version.startswith("7."), src.version
    assert "gitversion" in src.evidence or "storageengines" in src.evidence

    rows = eng._brand_rows()
    assert any(r["item"] == "source brand" and "mongodb" in r["detail"]
               for r in rows), rows


def test_the_old_hostname_guess_is_gone(servers):
    """It used to add a DocumentDB row for any host with `docdb` in it. The
    host below says `docdb` and the server is plainly MongoDB."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    ep = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="",
                  options={"hosts": f"127.0.0.1:{MG_PORT}"})
    ep.host = "my-docdb-cluster.example"      # a name, not an address
    hop = Hop(name="b", engine="mongodb", source=ep, target=ep,
              db_map={"cx": "cx"})
    eng = MongoEngine(hop)
    assert eng._brands()[0].name == "mongodb"
    text = " ".join(r["item"] + r["detail"] for r in eng._brand_rows())
    assert "DocumentDB" not in text, text


def test_redpanda_and_kafka_are_told_apart_through_the_engine(servers):
    from migkit.engines.kafka import KafkaEngine
    rp = KafkaEngine(_hop("kafka", RP_PORT))._brands()[0]
    ak = KafkaEngine(_hop("kafka", AK_PORT))._brands()[0]
    assert rp.name == "redpanda", rp
    assert ak.name == "kafka", ak
    assert rp.evidence.startswith("cluster_id=redpanda."), rp.evidence
    assert not ak.evidence.startswith("cluster_id=redpanda."), ak.evidence


def test_the_two_brokers_pair_as_a_mismatch(servers):
    """Moving Kafka to Redpanda is a migration someone chooses. What must not
    happen is the report reading as though both sides are the same thing."""
    from migkit.engines.kafka import KafkaEngine
    rp = KafkaEngine(_hop("kafka", RP_PORT))._brands()[0]
    ak = KafkaEngine(_hop("kafka", AK_PORT))._brands()[0]
    why = v.mismatch(ak, rp)
    assert "kafka" in why and "redpanda" in why, why
    assert "different software" in why
