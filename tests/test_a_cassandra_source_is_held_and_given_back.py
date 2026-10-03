"""A Cassandra source held at cutover, read back, and given back
(`freeze.CassandraHold`): the application's role loses MODIFY on the
keyspace and its tables, the permission cache is waited out so a session
connected before is refused too, and each grant comes back as it was.
The read back says when a role can still write.
"""
import socket
import subprocess
import time

import pytest

from migkit import freeze
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.freeze", "cassandra")]

pytestmark = needs_docker

NAME, PORT, PW = "migkit-test-hold-cassandra", 16352, "CHANGE_ME-root"


def _session(user="cassandra", password=PW):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    return Cluster(["127.0.0.1"], port=PORT,
                   auth_provider=PlainTextAuthProvider(user, password),
                   connect_timeout=15).connect()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"127.0.0.1:{PORT}:9042", "-e", "MAX_HEAP_SIZE=384M",
                    "-e", "HEAP_NEWSIZE=64M", "-e",
                    "CASSANDRA_AUTHENTICATOR=PasswordAuthenticator", "-e",
                    "CASSANDRA_AUTHORIZER=CassandraAuthorizer", "-e",
                    f"CASSANDRA_PASSWORD={PW}", "bitnami/cassandra:4.1"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 300
        s = None
        while time.time() < end:
            with socket.socket() as sock:
                sock.settimeout(1)
                up = sock.connect_ex(("127.0.0.1", PORT)) == 0
            if up:
                try:
                    s = _session()
                    break
                except Exception:
                    pass
            time.sleep(3)
        if s is None:
            pytest.fail("cassandra never answered")
        yield s
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


@pytest.fixture
def shop(server, tmp_path):
    s = server
    s.execute("drop keyspace if exists shop")
    s.execute("create keyspace shop with replication = {'class':"
              " 'SimpleStrategy', 'replication_factor': 1}")
    s.execute("create table shop.o (id int primary key, tag text)")
    s.execute("drop role if exists app")
    s.execute("create role app with password = 'CHANGE_ME-app' and"
              " login = true")
    s.execute("grant select on keyspace shop to app")
    s.execute("grant modify on keyspace shop to app")
    s.execute("grant modify on table shop.o to app")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="cassandra",
                  password=PW)
    hop = Hop(name="ch", engine="cassandra", source=ep, target=ep,
              databases=["shop"], options={"cutover": {"budget": 60}})
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    from migkit.engines.cassandra import CassandraEngine
    yield hop, CassandraEngine(hop)


def _grants():
    return sorted((r.resource, r.permission) for r in _session().execute(
        "LIST ALL PERMISSIONS OF app NORECURSIVE"))


def test_the_roles_write_is_revoked_waited_out_and_given_back(shop):
    from cassandra import Unauthorized
    hop, eng = shop
    got = freeze.choose(hop, eng, ["shop"])
    assert got.rung.name == "cql_revoke", got
    before = _grants()
    # connected before the hold, its permissions cached
    app = _session("app", "CHANGE_ME-app")
    app.execute("insert into shop.o (id, tag) values (1, 'seed')")
    said = []
    freeze.freeze_source(hop, eng, ["shop"], said.append, "cql_revoke")
    with pytest.raises(Unauthorized):
        app.execute("insert into shop.o (id, tag) values (2, 'during')")
    held = freeze.proof(hop, eng, "shop", "cql_revoke")
    assert all(ok for ok, _ in held), held
    # a grant made behind the hold's back reads back as still writing
    _session().execute("grant modify on keyspace shop to app")
    held = freeze.proof(hop, eng, "shop", "cql_revoke")
    assert (False, "app: may still write <keyspace shop>") in held, held
    _session().execute("revoke modify on keyspace shop from app")
    freeze.thaw_source(hop, eng, ["shop"], said.append)
    assert _grants() == before
    end = time.time() + 15
    while True:
        try:
            app.execute("insert into shop.o (id, tag) values (3, 'after')")
            break
        except Unauthorized:
            # the cache's refusal, until it lets it go
            if time.time() > end:
                raise
            time.sleep(0.5)
    tags = sorted(r.tag for r in _session().execute("select tag from"
                                                    " shop.o"))
    assert tags == ["after", "seed"]
