"""MySQL: the server is asked for its certificate the way MySQL asks - a
TLS request after the server's greeting - and the row sync migkit runs
beside it asks its vendor nothing.
"""
import http.server
import subprocess
import threading
import time

import pytest

from migkit import tls
from migkit.config import Endpoint, Hop
from tests.test_a_connection_is_verified_where_it_can_be import _certs

MY, PORT, LISTEN = "migkit-test-out-my", 16098, 16099


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


def _sql(text):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=text,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql(tmp_path_factory):
    certs = tmp_path_factory.mktemp("mycerts")
    ca = _certs(certs)
    other = _certs(tmp_path_factory.mktemp("myother"))
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(
        ["docker", "create", "--name", MY, "-p", f"127.0.0.1:{PORT}:3306",
         "-e", "MYSQL_ROOT_PASSWORD=test", "mysql:8.4", "bash", "-c",
         "mkdir -p /tls && cp /certs/* /tls/ && chmod 644 /tls/*"
         " && exec docker-entrypoint.sh mysqld --ssl-ca=/tls/ca.crt"
         " --ssl-cert=/tls/server.crt --ssl-key=/tls/server.key"],
        check=True, capture_output=True)
    try:
        subprocess.run(["docker", "cp", f"{certs}/.", f"{MY}:/certs"],
                       check=True, capture_output=True)
        subprocess.run(["docker", "start", MY], check=True,
                       capture_output=True)
        end = time.time() + 180
        while time.time() < end:
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        _sql("create database a; create database b;"
             " create table a.t (id int primary key, v int);"
             " create table b.t (id int primary key, v int);"
             " insert into a.t values (1, 1), (2, 2);"
             " insert into b.t values (1, 1), (2, 3);")
        yield ca, other
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture(autouse=True)
def fresh():
    tls._SEEN.clear()
    yield
    tls._SEEN.clear()


def test_the_certificate_is_asked_for_after_the_greeting(mysql):
    ca, other = mysql
    assert tls.probe("127.0.0.1", PORT, "mysql", str(ca))[0] == "verified"
    assert tls.probe("localhost", PORT, "mysql", str(other))[0] == \
        "unverified"
    # the same port asked as if it spoke TLS from its first byte: no
    assert tls.probe("127.0.0.1", PORT, "direct", str(ca))[0] == "none"


def test_a_connection_checks_the_certificate_that_verifies(mysql, tmp_path,
                                                           monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    ca, other = mysql
    monkeypatch.setattr(tls, "local", lambda ep: False)
    monkeypatch.setattr(tls, "authorities", lambda: [str(ca)])
    ep = Endpoint(host="localhost", port=PORT, user="root", password="test")
    assert ep.mysql_tls() == {"ssl_ca": str(ca), "ssl_verify_cert": True,
                              "ssl_verify_identity": True}
    hop = Hop(name="my", engine="mysql", source=ep, target=ep,
              databases=["a"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    got = MySQLEngine(hop).leg_encryption("src", "a")
    assert got["encrypted"], got
    tls._SEEN.clear()
    monkeypatch.setattr(tls, "authorities", lambda: [str(other)])
    ep = Endpoint(host="localhost", port=PORT, user="root", password="test")
    assert ep.mysql_tls() == {}
    assert tls.posture(ep, "mysql")["state"] == "unverified"


class _Heard(http.server.BaseHTTPRequestHandler):
    heard = []

    def log_message(self, *a):
        pass

    def do_POST(self):
        _Heard.heard.append(self.path)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"")

    do_GET = do_POST


def test_the_row_sync_asks_its_vendor_nothing(mysql, tmp_path):
    """Measured with its own hook for where it calls (`PERCONA_VERSION_
    CHECK_URL`): started as it was, it called once; started by migkit,
    never."""
    from migkit import util
    if not util.which("pt-table-sync"):
        pytest.skip("the row sync is not installed here")
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", LISTEN), _Heard)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    env = {"PERCONA_VERSION_CHECK_URL": f"http://127.0.0.1:{LISTEN}"}
    dsns = [f"h=127.0.0.1,P={PORT},u=root,p=test,D=a,t=t",
            f"h=127.0.0.1,P={PORT},u=root,p=test,D=b"]
    try:
        # a server it has not checked in the last day is checked: this
        # container is new to it (`/tmp/percona-version-check` keeps the
        # servers and when - measured on this machine, 29 of them checked
        # between 2026-09-25 and 09-27, by migkit's own test runs)
        _Heard.heard.clear()
        got = util.run(["pt-table-sync", "--print", *dsns], env=env,
                       check=False, timeout=120)
        assert "UPDATE" in got.stdout, (got.stdout, got.stderr)
        assert _Heard.heard == [], _Heard.heard
        # the hook is live: without the switch, it calls
        subprocess.run(["pt-table-sync", "--print", *dsns],
                       env=util.tool_env(env), capture_output=True,
                       text=True, timeout=120)
        assert _Heard.heard, "the version check never called"
    finally:
        srv.shutdown()
        srv.server_close()
