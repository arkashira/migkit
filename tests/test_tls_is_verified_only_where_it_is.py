"""A connection is called verified only where it is, on every engine
(audit 2026-10-01, SEC17, SEC18).

Before: every tunnel counted as local, so a forwarder the hop starts with
a command of its own (a cloud's port forward, `kubectl port-forward`)
turned the automatic check off and passed the posture - whatever it did
with the rows; `doctor` had a second idea of "local"; and the posture
covered six engine families, ClickHouse's plain HTTP with its password,
Kafka, OpenSearch, Oracle and Db2 among those it said nothing of.
"""
import pytest

from migkit.config import Endpoint

FAR = "10.0.0.9"


@pytest.fixture
def nothing_asked(monkeypatch):
    from migkit import tls

    def refuse(*a, **k):
        raise AssertionError("a server was asked")
    monkeypatch.setattr(tls, "_probe", refuse)
    return tls


def _through(kind, **options):
    return Endpoint(host="127.0.0.1", port=40001, options={
        "tunnel_to": f"{FAR}:5432", "tunnel_by": kind, **options})


def test_only_an_ssh_tunnel_is_local(nothing_asked):
    tls = nothing_asked
    assert tls.local(_through("ssh"))
    assert tls.posture(_through("ssh"), "postgres")["state"] == "local"
    for ep in (_through("command"), _through(None),
               Endpoint(host="127.0.0.1", port=40001,
                        options={"tunnel": {"command": "kubectl"
                                            " port-forward svc/db"
                                            " {local_port}:5432"}}),
               Endpoint(host="127.0.0.1", port=40001,
                        options={"tunnel": "aws ssm start-session"})):
        assert not tls.local(ep), ep.options
        got = tls.posture(ep, "postgres")
        assert got["state"] == "unknown", got
        assert "SSH tunnel" in got["detail"], got
        # never asked automatically through a tunnel: the near end is this
        # machine's address, not the name on the server's certificate
        assert tls.auto(ep, "postgres") is None
    for spec in ({"ssh": "bastion.example.com"}, "ssh://ec2-user@b.test"):
        assert tls.local(Endpoint(host=FAR, port=5432,
                                  options={"tunnel": spec}))


def test_a_certificate_checked_end_to_end_stays_verified(nothing_asked):
    tls = nothing_asked
    got = tls.posture(_through("command", sslmode="verify-full"),
                      "postgres")
    assert got["state"] == "verified", got
    got = tls.posture(_through("command", sslmode="disable"), "postgres")
    assert got["state"] == "unknown", got


def test_an_opened_tunnel_says_what_carries_it(monkeypatch):
    from migkit import tunnel
    for spec, kind in (({"ssh": "b.example.com"}, "ssh"),
                       ("ssh://u@b.example.com:2222", "ssh"),
                       ({"ssh": "b", "jump": ["j1"]}, "ssh"),
                       ({"command": "socat x"}, "command"),
                       ("cloud-sql-proxy {local_port}", "command")):
        t = tunnel.Tunnel(Endpoint(host=FAR, port=5432), spec, "source")
        assert t.kind() == kind, spec


def test_on_this_machine_is_one_idea(nothing_asked):
    """`doctor` said nothing of a side on 127.0.0.1 by its own test; one
    through a command's tunnel was said to be protected by it."""
    import inspect

    from migkit import cli
    tls = nothing_asked
    assert tls.on_this_machine(Endpoint(host="127.0.0.1", port=5432))
    assert tls.on_this_machine(Endpoint(host="/var/run/postgresql"))
    assert not tls.on_this_machine(_through("ssh"))
    source = inspect.getsource(cli._leg_said)
    assert "tls.on_this_machine(ep)" in source
    assert '"127.0.0.1", "localhost"' not in source

    class Eng:
        ENGINE_FAMILY = "postgres"
        CANON_ENGINE = "postgres"

        def leg_encryption(self, side, db):
            return {"encrypted": False}
    said = cli._leg_said(Eng(), "src", "app", _through("command"))
    assert "cannot vouch" in said and "protects" not in said, said
    said = cli._leg_said(Eng(), "src", "app", _through("ssh"))
    assert "protects" in said, said


#: an endpoint of each engine migkit reaches over a network, as a hop
#: writes it with nothing about TLS, and what the posture must say
BARE = {
    "kafka": "plain", "clickhouse": "plain", "opensearch": "plain",
    "couchbase": "plain", "db2i": "plain", "oracle": "plain",
    "db2": "plain", "db2zos": "plain", "ase": "plain",
    "cosmosdb": "verified", "snowflake": "verified",
    "bigquery": "verified", "dynamodb": "verified", "kinesis": "verified",
    "teradata": "unverified", "sapodp": "verified", "saas": "unknown",
}


def test_every_networked_engine_is_in_the_posture(nothing_asked):
    from migkit.engines import NAMES, _class_for
    tls = nothing_asked
    ep = Endpoint(host=FAR, port=9999)
    for name, state in BARE.items():
        got = tls.posture(ep, name)
        assert got is not None and got["state"] == state, (name, got)
    # every engine that is not files, a stream's sink or a generic reader
    # is covered, by its own family
    left = {n for n in NAMES
            if tls._POSTURE.get(_class_for(n).ENGINE_FAMILY
                                or _class_for(n).CANON_ENGINE) is None}
    assert left <= {"generic", "hetero"}, left


def test_what_each_engine_is_told_is_said(nothing_asked):
    tls = nothing_asked
    cases = [
        ("kafka", {"security_protocol": "SASL_SSL"}, "verified"),
        ("kafka", {"security_protocol": "SASL_PLAINTEXT"}, "plain"),
        ("clickhouse", {"secure": True}, "verified"),
        ("opensearch", {"tls": True}, "verified"),
        ("opensearch", {"tls": True, "verify": False}, "unverified"),
        ("couchbase", {"tls": True, "tls_insecure": True}, "unverified"),
        ("couchbase", {"url": "couchbases://cb.example.com"}, "verified"),
        ("cosmosdb", {"tls": False}, "plain"),
        ("cosmosdb", {"tls_insecure": True}, "unverified"),
        ("db2i", {"tls": True, "tls_ca_file": "/ca.pem"}, "verified"),
        ("dynamodb", {"endpoint_url": "http://10.0.0.3:8000"}, "plain"),
        ("redshift", {"iam": True}, "verified"),
        ("redshift", {"sslmode": "disable"}, "plain"),
        ("teradata", {"sslmode": "verify-full"}, "verified"),
        ("sapodp", {"tls": False}, "plain"),
        ("sapodp", {"tls_insecure": True}, "unverified"),
    ]
    for family, options, state in cases:
        got = tls.posture(Endpoint(host=FAR, port=9999, options=options),
                          family)
        assert got["state"] == state, (family, options, got)
    emulator = tls.posture(Endpoint(host="10.0.0.3", port=8085), "pubsub")
    assert emulator["state"] == "plain"
    assert tls.posture(Endpoint(host=""), "pubsub")["state"] == "verified"
    # a file's path on this machine says nothing, as before
    assert tls.posture(Endpoint(host="/tmp/app.db"), "sqlite") is None
    lake = tls.posture(Endpoint(host="s3://bucket/lake", options={
        "endpoint_url": "http://10.0.0.4:9000"}), "iceberg")
    assert lake["state"] == "plain", lake
    assert tls.posture(Endpoint(host="s3://bucket/lake"),
                       "delta")["state"] == "verified"
    # files on this machine's own disks: nothing crosses a network
    assert tls.posture(Endpoint(host="data/app.db"), "sqlite") is None
