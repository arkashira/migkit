"""A Kafka cluster's accounts: its SCRAM users and its ACLs.

Measured before: `users` on a kafka hop said the engine was not
supported, so a cluster moved with its topics came up with none of the
permissions its clients had - every consumer refused on the first read,
or, on a target with no authorizer, every client allowed everything.

A SCRAM password is stored salted and cannot be read back, so a user's
name and mechanisms are compared and its password comes from the
passwords file; ACLs are carried as they are and taken back by their
full key, leaving the target's own ACLs alone.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-kacl-src", 15911), ("migkit-test-kacl-dst", 15912)
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
        rpk(SRC, "acl", "user", "create", "app", "-p", "CHANGE_ME-app",
            "--mechanism", "SCRAM-SHA-512")
        rpk(SRC, "acl", "user", "create", "report", "-p", "CHANGE_ME-rep",
            "--mechanism", "SCRAM-SHA-256")
        rpk(SRC, "acl", "create", "--allow-principal", "User:app",
            "--operation", "read,describe", "--topic", "orders")
        rpk(SRC, "acl", "create", "--allow-principal", "User:app",
            "--operation", "read", "--group", "app-",
            "--resource-pattern-type", "prefixed")
        rpk(SRC, "acl", "create", "--deny-principal", "User:report",
            "--operation", "write", "--topic", "orders", "--deny-host",
            "10.0.0.9")
        # the target's own: never matched by what is carried or taken back
        rpk(DST, "acl", "create", "--allow-principal", "User:ops",
            "--operation", "describe", "--cluster")
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
    hop = Hop(name="kacl", engine="kafka", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["cluster"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _acls(side):
    return sorted(line.split() for line in
                  rpk(side, "acl", "list").splitlines()[1:] if line.strip())


def test_users_and_acls_missing_on_the_target_are_named(clusters, tmp_path):
    from migkit import users
    said = []
    out, _ = users.compare(_hop(tmp_path), said.append)
    assert out["missing_on_target"] == ["app", "report"], out
    assert out["source_acls"] == 4, out
    assert len(out["acls_missing"]) == 4, out
    assert out["result"] == "gap"
    text = "\n".join(said)
    assert "User:app allow read on group app- (prefix)" in text, text
    assert "User:report deny write on topic orders from 10.0.0.9" in text


def test_create_carries_them_and_rollback_takes_back_only_those(
        clusters, tmp_path):
    from migkit import users
    before = _acls(DST)
    hop = _hop(tmp_path)
    said = []
    # only app has a password: report is skipped and said, not guessed
    users.create(hop, apply=True, passwords={"app": "CHANGE_ME-app"},
                 say=said.append)
    assert any("skipped report: no password" in m for m in said), said
    out, _ = users.compare(hop, [].append)
    assert out["missing_on_target"] == ["report"], out
    assert out["acls_missing"] == [] and out["mechanisms_differ"] == []
    # the carried password is the one given: the target signs app in
    from migkit.engines.kafka import KafkaEngine
    hop.target.user, hop.target.password = "app", "CHANGE_ME-app"
    hop.target.options["sasl_mechanism"] = "SCRAM-SHA-512"
    KafkaEngine(hop)._admin("dst").close()
    hop.target.user, hop.target.password = "migkit", "CHANGE_ME-sasl"
    hop.target.options["sasl_mechanism"] = "SCRAM-SHA-256"

    users.rollback(hop, apply=True, say=[].append)
    after = _acls(DST)
    assert after == before, (before, after)
    assert "User:ops" in str(after)
    out, _ = users.compare(hop, [].append)
    assert out["missing_on_target"] == ["app", "report"], out


def test_a_target_that_checks_no_acls_is_not_a_pass(clusters, tmp_path,
                                                    monkeypatch):
    from migkit import users
    real = users._kafka_accounts

    def no_authorizer(hop, side):
        got = real(hop, side)
        if side == "dst":
            got["acls"] = None
            got["unread"]["acls"] = "SecurityDisabledError: no authorizer"
        return got
    monkeypatch.setattr(users, "_kafka_accounts", no_authorizer)
    said = []
    out, _ = users.compare(_hop(tmp_path), said.append)
    assert out["result"] == "unknown", out
    assert any("the source holds 4 ACL(s) and the target checks none" in u
               for u in out["unread"]), out["unread"]
    with pytest.raises(SystemExit, match="checks none"):
        users.create(_hop(tmp_path), apply=True, say=[].append)
