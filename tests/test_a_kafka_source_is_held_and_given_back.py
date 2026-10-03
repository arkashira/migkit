"""A Kafka source held at cutover, read back, and given back
(`freeze.KafkaHold`): every principal's writes to the hop's topics
refused by a deny, which outranks every allow - and, with every topic on
the cluster the hop's, transactional producers kept from committing -
the denies migkit made deleted exactly, and one the operator had made
before left where it was. The read back says when a deny is missing.

The broker checks access (`StandardAuthorizer`) and lets everyone do
what no rule names, so the application writes as itself before and
after, and migkit makes its denies as the same anonymous principal.
"""
import socket
import subprocess
import time

import pytest

from migkit import freeze
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.freeze", "kafka")]

pytestmark = needs_docker

NAME, PORT = "migkit-test-hold-kafka", 16353


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    env = {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://127.0.0.1:{PORT}",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_AUTHORIZER_CLASS_NAME":
            "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        "KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND": "true",
    }
    args = [x for k, v in env.items() for x in ("-e", f"{k}={v}")]
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"127.0.0.1:{PORT}:9092", *args, "apache/kafka:3.8.0"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        time.sleep(5)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


def _admin():
    from kafka.admin import KafkaAdminClient
    return KafkaAdminClient(bootstrap_servers=[f"127.0.0.1:{PORT}"],
                            request_timeout_ms=15000)


def _send(topic, value, **extra):
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=[f"127.0.0.1:{PORT}"], acks="all",
                      max_block_ms=10000, retries=0, **extra)
    try:
        return p.send(topic, value).get(timeout=10)
    finally:
        p.close()


@pytest.fixture
def topics(server, tmp_path):
    from kafka.admin import NewTopic
    admin = _admin()
    try:
        have = set(admin.list_topics())
        for t in ("orders", "payments", "audit"):
            if t in have:
                admin.delete_topics([t])
        time.sleep(2)
        admin.create_topics([NewTopic(t, 1, 1) for t in
                             ("orders", "payments", "audit")])
    finally:
        admin.close()
    time.sleep(2)
    _send("orders", b"seed")
    ep = Endpoint(host="127.0.0.1", port=PORT)
    hop = Hop(name="kh", engine="kafka", source=ep, target=ep,
              databases=["cluster"], exclude=["audit"],
              options={"cutover": True})
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    from migkit.engines.kafka import KafkaEngine
    yield hop, KafkaEngine(hop)


def _denies():
    from kafka.admin import (ACLFilter, ACLOperation, ACLPermissionType,
                             ACLResourcePatternType, ResourcePatternFilter,
                             ResourceType)
    admin = _admin()
    try:
        got, _ = admin.describe_acls(ACLFilter(
            principal=None, host=None, operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.DENY,
            resource_pattern=ResourcePatternFilter(
                ResourceType.ANY, None, ACLResourcePatternType.ANY)))
        return sorted((a.resource_pattern.resource_type.name,
                       a.resource_pattern.resource_name) for a in got)
    finally:
        admin.close()


def test_the_hops_topics_are_refused_read_back_and_given_back(topics):
    from kafka.errors import KafkaError
    hop, eng = topics
    # the excluded topic is the cluster's other: its topics alone, then
    got = freeze.choose(hop, eng, ["cluster"])
    assert got.rung.name == "kafka_topics" and "audit" in got.reason, got
    # a deny the operator made before is left where it is
    admin = _admin()
    try:
        admin.create_acls([freeze._kafka_deny("TOPIC", "orders")])
    finally:
        admin.close()
    said = []
    freeze.freeze_source(hop, eng, ["cluster"], said.append, "kafka_topics")
    for t in ("orders", "payments"):
        with pytest.raises(KafkaError):
            _send(t, b"during")
    _send("audit", b"not the hop's")
    held = freeze.proof(hop, eng, "cluster", "kafka_topics")
    assert all(ok for ok, _ in held), held
    # a deny deleted behind the hold's back reads back as not held
    admin = _admin()
    try:
        admin.delete_acls([freeze._kafka_deny("TOPIC", "payments",
                                              filtered=True)])
    finally:
        admin.close()
    held = freeze.proof(hop, eng, "cluster", "kafka_topics")
    assert (False, "not denied: payments") in held, held
    freeze.thaw_source(hop, eng, ["cluster"], said.append)
    assert _denies() == [("TOPIC", "orders")], "only migkit's own taken"
    _send("payments", b"after")
    admin = _admin()
    try:
        admin.delete_acls([freeze._kafka_deny("TOPIC", "orders",
                                              filtered=True)])
    finally:
        admin.close()
    _send("orders", b"after")


def _transaction(topic, value):
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=[f"127.0.0.1:{PORT}"],
                      transactional_id="shop-app", max_block_ms=10000)
    try:
        p.init_transactions()
        p.begin_transaction()
        p.send(topic, value).get(timeout=10)
        p.commit_transaction()
    finally:
        p.close()


def test_the_whole_cluster_keeps_transactions_from_committing(topics):
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    if not hasattr(KafkaProducer, "init_transactions"):
        pytest.skip("this client has no transactional producer")
    hop, eng = topics
    # a transaction commits before: what refuses it below is the hold
    _transaction("audit", b"before")
    said = []
    freeze.freeze_source(hop, eng, ["cluster"], said.append, "kafka_cluster")
    assert ("TRANSACTIONAL_ID", "*") in _denies()
    with pytest.raises(KafkaError):
        _transaction("audit", b"during")
    freeze.thaw_source(hop, eng, ["cluster"], said.append)
    assert _denies() == []
    _transaction("audit", b"after")
