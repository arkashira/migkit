"""A connection is verified wherever the server's certificate verifies, and
`assess` fails a connection to another machine that is not.

Left unset, a PostgreSQL connection was libpq's `prefer`, MySQL's took TLS
with nothing checked, and MongoDB, Redis, SQL Server and Cassandra had no
TLS setting at all - anyone on the path could present a certificate of
their own. Now the server is asked once: where its certificate verifies
against the system's authorities or the hop's own, every connection to it
checks it; where it does not, the hop connects as it always did, and
`assess` and `doctor` say so as a failure.
"""
import datetime
import ipaddress
import subprocess
import time

import pytest

from migkit import tls
from migkit.config import Endpoint, Hop

FAR = "10.0.0.7"


@pytest.fixture(autouse=True)
def fresh():
    tls._SEEN.clear()
    yield
    tls._SEEN.clear()


def _no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("asked the network")
    monkeypatch.setattr(tls, "_probe", refuse)


def test_a_server_on_this_machine_is_never_asked(monkeypatch):
    _no_network(monkeypatch)
    for host in ("127.0.0.1", "localhost", "::1", "/var/run/postgresql"):
        ep = Endpoint(host=host, port=5432)
        assert ep.libpq_tls() == {} and ep.mysql_tls() == {}
        assert ep.redis_tls() == {} and ep.mongo_tls() == {}
        assert tls.posture(ep, "postgres")["state"] == "local"
    tunneled = Endpoint(host="127.0.0.1", port=40001,
                        options={"tunnel_to": f"{FAR}:5432"})
    assert tls.posture(tunneled, "redis")["state"] == "local"


def test_what_the_hop_says_is_what_is_done(monkeypatch):
    _no_network(monkeypatch)
    ep = Endpoint(host=FAR, port=5432, options={"sslmode": "require"})
    assert ep.libpq_tls() == {"sslmode": "require"}
    assert tls.posture(ep, "postgres")["state"] == "unverified"
    ep = Endpoint(host=FAR, port=5432, options={"sslmode": "verify-full",
                                                "sslrootcert": "/ca.pem"})
    assert tls.posture(ep, "postgres")["state"] == "verified"
    ep = Endpoint(host=FAR, port=5432, options={"sslmode": "disable"})
    assert tls.posture(ep, "postgres")["state"] == "plain"
    ep = Endpoint(host=FAR, port=3306, options={"ssl": True})
    assert tls.posture(ep, "mysql")["state"] == "unverified"
    ep = Endpoint(host=FAR, port=3306, options={"ssl_ca": "/ca.pem"})
    assert ep.mysql_tls()["ssl_verify_identity"] is True
    assert tls.posture(ep, "mysql")["state"] == "verified"
    for family in ("mongodb", "redis", "cassandra"):
        on = Endpoint(host=FAR, port=1, options={"tls_ca_file": "/ca.pem"})
        off = Endpoint(host=FAR, port=1, options={"tls": True,
                                                  "tls_insecure": True})
        assert tls.posture(on, family)["state"] == "verified", family
        assert tls.posture(off, family)["state"] == "unverified", family
    ms = Endpoint(host=FAR, port=1433, options={"encrypt": "require"})
    assert ms.mssql_tls() == {"encryption": "require"}
    assert tls.posture(ms, "mssql")["state"] == "unverified"
    assert Endpoint(host=FAR, options={"encrypt": False}).mssql_tls() == \
        {"encryption": "off"}


def test_the_options_reach_each_driver(monkeypatch):
    _no_network(monkeypatch)
    ep = Endpoint(host=FAR, port=27017, user="u", password="p",
                  options={"tls_ca_file": "/c/ca.pem",
                           "tls_cert_file": "/c/me.pem",
                           "tls_crl_file": "/c/crl.pem"})
    assert ep.mongo_tls() == {"tls": "true", "tlsCAFile": "/c/ca.pem",
                              "tlsCertificateKeyFile": "/c/me.pem",
                              "tlsCRLFile": "/c/crl.pem"}
    from migkit.movers import _mongo_uri
    uri = _mongo_uri(ep)
    assert "tls=true" in uri and "tlsCAFile=%2Fc%2Fca.pem" in uri, uri
    # the operator's own say in the URI is theirs
    own = Endpoint(host=FAR, port=27017,
                   options={"uri_options": "tls=true&tlsInsecure=true",
                            "tls_ca_file": "/c/ca.pem"})
    assert own.mongo_tls() == {}
    red = Endpoint(host=FAR, port=6380, options={"tls": True,
                                                 "tls_cert_file": "/me.crt",
                                                 "tls_key_file": "/me.key"})
    assert red.redis_tls() == {"ssl": True, "ssl_cert_reqs": "required",
                               "ssl_check_hostname": True,
                               "ssl_certfile": "/me.crt",
                               "ssl_keyfile": "/me.key"}
    cas = Endpoint(host=FAR, port=9142, options={"tls": True})
    got = cas.cassandra_tls()
    assert got["ssl_context"].check_hostname
    assert got["ssl_options"] == {"server_hostname": FAR}


def _hop(src, dst, engine="redis"):
    return Hop(name="t", engine=engine, source=src, target=dst,
               databases=["0"])


def test_assess_fails_a_far_connection_that_is_not_checked(monkeypatch):
    from migkit.engines.redis import RedisEngine
    monkeypatch.setattr(tls, "_probe",
                        lambda host, port, how, ca: ("unverified", None))
    eng = RedisEngine(_hop(Endpoint(host=FAR, port=6379),
                           Endpoint(host="127.0.0.1", port=6379)))
    rows = {r["item"]: r for r in eng._leg_items()}
    assert rows["source connection NOT verified"]["level"] == "fail"
    assert rows["target connection local"]["level"] == "pass"
    monkeypatch.setattr(tls, "_probe",
                        lambda host, port, how, ca: ("none", None))
    tls._SEEN.clear()
    rows = {r["item"]: r for r in eng._leg_items()}
    assert rows["source connection NOT encrypted"]["level"] == "fail"
    monkeypatch.setattr(tls, "_probe",
                        lambda host, port, how, ca: ("verified", "/ca.pem"))
    tls._SEEN.clear()
    rows = {r["item"]: r for r in eng._leg_items()}
    assert rows["source connection verified"]["level"] == "pass"
    # and the connection the engine makes checks it
    assert Endpoint(host=FAR, port=6379).redis_tls()["ssl_ca_certs"] == \
        "/ca.pem"


def test_doctor_says_it_as_a_failure(monkeypatch):
    from migkit.cli import _leg_said
    monkeypatch.setattr(tls, "_probe",
                        lambda host, port, how, ca: ("unverified", None))

    class Eng:
        ENGINE_FAMILY = "postgres"

        def leg_encryption(self, side, db):
            return {"encrypted": True, "how": "TLSv1.3"}
    said = _leg_said(Eng(), "src", "d", Endpoint(host=FAR, port=5432))
    assert said.startswith("[red]FAIL[/red] connection NOT verified"), said


# ---- real servers -----------------------------------------------------------

PG, RD = "migkit-test-out-tlspg", "migkit-test-out-tlsrd"
PG_PORT, RD_PORT = 16096, 16097


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _certs(where, name="localhost"):
    """An authority, and a certificate for `name` and 127.0.0.1 it signed."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    now = datetime.datetime.now(datetime.timezone.utc)

    def key():
        return ec.generate_private_key(ec.SECP256R1())

    def pem(k):
        return k.private_bytes(serialization.Encoding.PEM,
                               serialization.PrivateFormat.TraditionalOpenSSL,
                               serialization.NoEncryption())
    ca_key = key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,
                                            f"migkit test CA {where.name}")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - datetime.timedelta(days=1))
          .not_valid_after(now + datetime.timedelta(days=2))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None),
                         critical=True)
          .sign(ca_key, hashes.SHA256()))
    srv_key = key()
    srv = (x509.CertificateBuilder()
           .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,
                                                       name)]))
           .issuer_name(ca_name).public_key(srv_key.public_key())
           .serial_number(x509.random_serial_number())
           .not_valid_before(now - datetime.timedelta(days=1))
           .not_valid_after(now + datetime.timedelta(days=2))
           .add_extension(x509.SubjectAlternativeName([
               x509.DNSName(name),
               x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
               critical=False)
           .sign(ca_key, hashes.SHA256()))
    where.mkdir(parents=True, exist_ok=True)
    (where / "ca.crt").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    (where / "server.crt").write_bytes(
        srv.public_bytes(serialization.Encoding.PEM))
    (where / "server.key").write_bytes(pem(srv_key))
    return where / "ca.crt"


@pytest.fixture(scope="module")
def servers(tmp_path_factory):
    if not _docker():
        pytest.skip("docker not available")
    certs = tmp_path_factory.mktemp("certs")
    ca = _certs(certs)
    other = _certs(tmp_path_factory.mktemp("other"))
    for name in (PG, RD):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    try:
        yield from _started(certs, ca, other)
    finally:
        for name in (PG, RD):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _started(certs, ca, other):
    subprocess.run(
        ["docker", "create", "--name", PG, "-p", f"127.0.0.1:{PG_PORT}:5432",
         "-e", "POSTGRES_PASSWORD=test", "postgres:16", "bash", "-c",
         "mkdir -p /tls && cp /certs/* /tls/ && chown postgres /tls/*"
         " && chmod 600 /tls/server.key && exec docker-entrypoint.sh"
         " postgres -c ssl=on -c ssl_cert_file=/tls/server.crt"
         " -c ssl_key_file=/tls/server.key"], check=True,
        capture_output=True)
    subprocess.run(
        ["docker", "create", "--name", RD, "-p", f"127.0.0.1:{RD_PORT}:6379",
         "redis:7", "sh", "-c",
         "mkdir -p /tls && cp /certs/* /tls/ && chmod 644 /tls/*"
         " && exec redis-server --port 0 --tls-port 6379"
         " --tls-cert-file /tls/server.crt --tls-key-file /tls/server.key"
         " --tls-ca-cert-file /tls/ca.crt --tls-auth-clients no"],
        check=True, capture_output=True)
    for name in (PG, RD):
        subprocess.run(["docker", "cp", f"{certs}/.", f"{name}:/certs"],
                       check=True, capture_output=True)
        subprocess.run(["docker", "start", name], check=True,
                       capture_output=True)
    for _ in range(60):
        if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                           "postgres"], capture_output=True).returncode == 0:
            break
        time.sleep(1)
    time.sleep(2)
    yield ca, other


@pytest.fixture
def far(monkeypatch):
    """`localhost`, taken for another machine: the name the certificate
    was made for, reached the way a remote server is."""
    monkeypatch.setattr(tls, "local", lambda ep: False)


@pytest.mark.docker
def test_postgresql_is_verified_where_its_certificate_verifies(
        servers, far, monkeypatch, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    ca, other = servers
    monkeypatch.setattr(tls, "authorities", lambda: [str(ca)])
    ep = Endpoint(host="localhost", port=PG_PORT, user="postgres",
                  password="test")
    assert ep.libpq_tls() == {"sslmode": "verify-full",
                              "sslrootcert": str(ca)}
    hop = Hop(name="tls", engine="postgres", source=ep, target=ep,
              databases=["postgres"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    got = PostgresEngine(hop).leg_encryption("src", "postgres")
    assert got["encrypted"], got
    assert tls.posture(ep, "postgres")["state"] == "verified"
    # the same server under an authority that did not sign it: connected
    # as before, and said as a failure
    tls._SEEN.clear()
    monkeypatch.setattr(tls, "authorities", lambda: [str(other)])
    ep = Endpoint(host="localhost", port=PG_PORT, user="postgres",
                  password="test")
    assert ep.libpq_tls() == {}
    hop = Hop(name="tls", engine="postgres", source=ep, target=ep,
              databases=["postgres"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    eng = PostgresEngine(hop)
    assert eng.leg_encryption("src", "postgres")["encrypted"]
    rows = eng._leg_items()
    assert [r["level"] for r in rows] == ["fail", "fail"], rows
    # and the hop's own authority, named without a mode, is checked
    tls._SEEN.clear()
    ep = Endpoint(host="localhost", port=PG_PORT, user="postgres",
                  password="test", options={"sslrootcert": str(ca)})
    assert ep.libpq_tls()["sslmode"] == "verify-full"


@pytest.mark.docker
def test_redis_is_verified_where_its_certificate_verifies(
        servers, far, monkeypatch, tmp_path):
    from migkit.engines.redis import RedisEngine
    ca, other = servers
    monkeypatch.setattr(tls, "authorities", lambda: [str(ca)])
    ep = Endpoint(host="localhost", port=RD_PORT)
    assert ep.redis_tls()["ssl_ca_certs"] == str(ca)
    eng = RedisEngine(_hop(ep, ep))
    assert eng._client("src").ping()
    assert tls.posture(ep, "redis")["state"] == "verified"
    # under another authority: it speaks TLS and nothing verifies it
    tls._SEEN.clear()
    monkeypatch.setattr(tls, "authorities", lambda: [str(other)])
    assert tls.probe("localhost", RD_PORT, "direct")[0] == "unverified"
    assert tls.posture(Endpoint(host="localhost", port=RD_PORT),
                       "redis")["state"] == "unverified"


@pytest.mark.docker
def test_the_hops_own_authority_is_taken_without_asking(servers,
                                                        monkeypatch):
    """Named by the hop, on this machine too: nothing is probed."""
    from migkit.engines.redis import RedisEngine
    ca, other = servers
    _no_network(monkeypatch)
    ep = Endpoint(host="localhost", port=RD_PORT,
                  options={"tls_ca_file": str(ca)})
    assert RedisEngine(_hop(ep, ep))._client("dst").ping()
    # an authority that did not sign it: refused, not connected unchecked
    wrong = Endpoint(host="localhost", port=RD_PORT,
                     options={"tls_ca_file": str(other)})
    import redis
    with pytest.raises(redis.ConnectionError, match="certificate verify"):
        RedisEngine(_hop(wrong, wrong))._client("dst").ping()
