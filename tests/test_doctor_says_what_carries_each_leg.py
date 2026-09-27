"""`doctor` says whether the connection to each side is encrypted - as
the database itself reports it - and where this machine sits between them.

Asked by the owner: nobody wants a database's rows read on the way. Every
vendor rides on TLS or SSH, and AWS puts its replication instance next to
the target, since a machine far from both sides carries every row across
a wide network twice. Before, `doctor` said neither.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

PG, PORT = "migkit-test-pgssl", 15933


class _Said:
    def __init__(self, got):
        self.got = got

    def leg_encryption(self, side, db):
        return self.got


def test_a_leg_in_clear_is_said():
    from migkit.cli import _leg_said
    far = Endpoint(host="10.0.0.5", port=5432)
    said = _leg_said(_Said({"encrypted": False, "how": ""}), "src", "d", far)
    assert said.startswith("[yellow]not encrypted[/yellow]: every row"), said
    said = _leg_said(_Said({"encrypted": True,
                            "how": "TLSv1.3 TLS_AES_256_GCM_SHA384"}),
                     "src", "d", far)
    assert said == "encrypted: TLSv1.3 TLS_AES_256_GCM_SHA384"
    tunneled = Endpoint(host="127.0.0.1", port=40001,
                        options={"tunnel_to": "10.0.0.5:5432"})
    said = _leg_said(_Said({"encrypted": False, "how": ""}), "src", "d",
                     tunneled)
    assert "carried through the hop's tunnel to 10.0.0.5:5432" in said
    # on this machine nothing crosses a network: nothing to say
    here = Endpoint(host="127.0.0.1", port=5432)
    assert _leg_said(_Said({"encrypted": False, "how": ""}), "src", "d",
                     here) == ""
    # an engine that cannot be asked says nothing rather than guess
    assert _leg_said(_Said(None), "src", "d", far) == ""


def test_a_machine_far_from_both_sides_is_said():
    from migkit.cli import _placement_said
    said = _placement_said({"src": 45.2, "dst": 38.0})
    assert "every row crosses a wide network twice" in said, said
    assert _placement_said({"src": 45.2, "dst": 0.4}) == ""
    assert _placement_said({"src": 45.2}) == ""


@pytest.fixture(scope="module")
def ssl_server():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-p",
                    f"{PORT}:5432", "-e", "POSTGRES_PASSWORD=test",
                    "postgres:16", "-c", "ssl=on", "-c",
                    "ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem",
                    "-c", "ssl_key_file=/etc/ssl/private/"
                    "ssl-cert-snakeoil.key"], check=True,
                   capture_output=True)
    try:
        end = time.time() + 90
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                                 "postgres"], capture_output=True
                                ).returncode == 0
            with socket.socket() as s:
                s.settimeout(1)
                if ok and s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


@needs_docker
def test_postgresql_says_what_its_own_connection_is(ssl_server, tmp_path):
    from migkit.engines.postgres import PostgresEngine

    def eng(**options):
        ep = Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                      password="test", options=options)
        hop = Hop(name="ssl", engine="postgres", source=ep, target=ep,
                  databases=["postgres"])
        hop.report_dir = lambda db=None: tmp_path
        return PostgresEngine(hop)
    got = eng(sslmode="require").leg_encryption("src", "postgres")
    assert got["encrypted"] and got["how"].startswith("TLSv1."), got
    got = eng(sslmode="disable").leg_encryption("src", "postgres")
    assert got == {"encrypted": False, "how": ""}, got


@needs_docker
def test_a_certificate_asked_for_is_checked(ssl_server, tmp_path):
    """Before, no connection migkit opened took a TLS setting: every one
    was libpq's default, TLS where offered and nothing checked. The
    endpoint's `sslmode` and `sslrootcert` reach it now - here a
    certificate made for another name is refused, as it should be."""
    from migkit.engines.postgres import PostgresEngine
    cert = tmp_path / "server.pem"
    cert.write_bytes(subprocess.run(
        ["docker", "exec", PG, "cat", "/etc/ssl/certs/ssl-cert-snakeoil.pem"],
        capture_output=True, check=True).stdout)
    ep = Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                  password="test", options={"sslmode": "verify-full",
                                            "sslrootcert": str(cert)})
    hop = Hop(name="ssl", engine="postgres", source=ep, target=ep,
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    with pytest.raises(Exception, match="does not match host name"):
        PostgresEngine(hop).leg_encryption("src", "postgres")


MY, MY_PORT = "migkit-test-myssl", 15934


@needs_docker
def test_mysql_checks_the_certificate_it_is_given(tmp_path):
    """Before, no MySQL connection took a TLS setting: the client took TLS
    where offered and checked nothing (measured on 8.4), so a certificate
    from anyone was accepted. `ssl_ca` checks it now, and the name on it
    unless `ssl_verify_identity: false`."""
    from migkit.engines.mysql import MySQLEngine
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-p",
                    f"{MY_PORT}:3306", "-e", "MYSQL_ROOT_PASSWORD=test",
                    "mysql:8.4"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            if ok:
                break
            time.sleep(2)
        ca = tmp_path / "ca.pem"
        ca.write_bytes(subprocess.run(
            ["docker", "exec", MY, "cat", "/var/lib/mysql/ca.pem"],
            capture_output=True, check=True).stdout)

        def eng(**options):
            ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                          password="test", options=options)
            hop = Hop(name="myssl", engine="mysql", source=ep, target=ep,
                      databases=["mysql"])
            hop.report_dir = lambda db=None: tmp_path
            return MySQLEngine(hop)
        got = eng().leg_encryption("src", "mysql")
        assert got["encrypted"], got
        # the server's own certificate names no host: refused by name
        with pytest.raises(Exception, match="hostname|match"):
            eng(ssl_ca=str(ca)).leg_encryption("src", "mysql")
        got = eng(ssl_ca=str(ca), ssl_verify_identity=False).leg_encryption(
            "src", "mysql")
        assert got["encrypted"] and got["how"].startswith("TLSv1."), got
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
