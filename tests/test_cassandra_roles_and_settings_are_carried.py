"""Cassandra: roles carried with their salted hashes, memberships and
permissions; the server's settings, the keyspace's replication and every
table's options compared; the target's tables recorded before a repair.

Measured before: `users` on a Cassandra hop said the engine was not
supported, and no setting was compared - a target table with a default
time to live expires rows the source keeps, and nothing said so.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

#: the login the cassandra image starts with, the same text as its user
IMAGE_LOGIN = "cassandra"

SRC, DST = ("migkit-test-csauth-src", 15931), ("migkit-test-csauth-dst",
                                               15932)
AUTH = ("sed -i 's/^authenticator:.*/authenticator: PasswordAuthenticator/;"
        " s/^authorizer:.*/authorizer: CassandraAuthorizer/'"
        " /etc/cassandra/cassandra.yaml && exec docker-entrypoint.sh"
        " cassandra -f")


def _session(port, user="cassandra", password=IMAGE_LOGIN, wait=300):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    end = time.time() + wait
    while True:
        try:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", port)) != 0:
                    raise OSError("not listening")
            return Cluster(["127.0.0.1"], port=port, auth_provider=
                           PlainTextAuthProvider(user, password),
                           connect_timeout=10).connect()
        except Exception:
            if time.time() > end:
                raise
            time.sleep(3)


@pytest.fixture(scope="module")
def clusters():
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:9042", "-e", "MAX_HEAP_SIZE=256M", "-e",
                        "HEAP_NEWSIZE=64M", "--entrypoint", "sh",
                        "cassandra:4.1", "-c", AUTH], check=True,
                       capture_output=True)
    try:
        sessions = [_session(port) for _, port in (SRC, DST)]
        for s in sessions:
            s.execute("create keyspace if not exists shop with replication"
                      " = {'class': 'SimpleStrategy', 'replication_factor':"
                      " 1}")
            s.execute("create table if not exists shop.orders (id int"
                      " primary key, v text)")
        yield sessions
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path):
    def ep(port):
        return Endpoint(host="127.0.0.1", port=port, user="cassandra",
                        password=IMAGE_LOGIN)
    hop = Hop(name="cs", engine="cassandra", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def test_roles_cross_with_their_hashes(clusters, tmp_path):
    from migkit import users
    src, dst = clusters
    src.execute("create role if not exists readers")
    src.execute("grant select on keyspace shop to readers")
    src.execute("create role if not exists app with password ="
                " 'CHANGE_ME-app' and login = true")
    src.execute("grant readers to app")
    src.execute("grant modify on table shop.orders to app")
    hop = _hop(tmp_path)
    out, _ = users.compare(hop, [].append)
    assert {"app", "readers"} <= set(out["missing_on_target"]), out
    said = []
    users.create(hop, apply=True, say=said.append)
    # the hash is never printed
    assert not any("$2a$" in m for m in said), said
    out, _ = users.compare(hop, [].append)
    assert out["result"] == "pass", out
    # the carried hash signs app in with the source's password
    s = _session(DST[1], "app", "CHANGE_ME-app", wait=30)
    assert s.execute("select count(*) from shop.orders").one()[0] == 0
    s.cluster.shutdown()
    users.rollback(hop, apply=True, say=[].append)
    left = {r.role for r in dst.execute("select role from"
                                        " system_auth.roles")}
    assert not {"app", "readers"} & left, left


def test_a_table_that_expires_rows_is_named(clusters, tmp_path):
    src, dst = clusters
    dst.execute("alter table shop.orders with default_time_to_live ="
                " 86400")
    from migkit.engines.cassandra import CassandraEngine
    got = CassandraEngine(_hop(tmp_path)).check_params("shop")
    dst.execute("alter table shop.orders with default_time_to_live = 0")
    assert got[0].status == "diff", got[0].detail
    assert "orders.default_time_to_live src=0 dst=86400" in got[0].detail, \
        got[0].detail


def test_the_target_is_recorded_before_a_repair(clusters, tmp_path):
    import json

    from migkit.engines.cassandra import CassandraEngine
    _, dst = clusters
    dst.execute("insert into shop.orders (id, v) values (1, 'x')")
    point = tmp_path / "point"
    point.mkdir()
    CassandraEngine(_hop(tmp_path)).snapshot_state("shop", point)
    got = json.loads((point / "dst-tables.json").read_text())
    assert got["rows"] == {"orders": 1}
    assert "nodetool" not in got["no_snapshot"]
    assert got["settings"]["orders.default_time_to_live"] == "0"


def test_rows_keep_their_time_to_live_and_write_time(clusters, tmp_path):
    """The table copier writes each row as new - its insert names no time
    to live and no write time - so a row set to expire on the source lives
    on the target for good, and a later write loses to the copy's. The
    move's own path reads each column's `TTL()` and `WRITETIME()` and
    writes them back."""
    from migkit.engines.cassandra import CassandraEngine
    src, dst = clusters
    src.execute("create keyspace if not exists timed with replication ="
                " {'class': 'SimpleStrategy', 'replication_factor': 1}")
    dst.execute("create keyspace if not exists timed with replication ="
                " {'class': 'SimpleStrategy', 'replication_factor': 1}")
    src.execute("create table if not exists timed.t (id int, at int,"
                " v text, w text, tags set<text>, primary key (id, at))")
    src.execute("insert into timed.t (id, at, v, w, tags) values"
                " (1, 1, 'a', 'b', {'x'}) using ttl 3600 and timestamp 111")
    src.execute("update timed.t using timestamp 222 set w = 'later' where"
                " id = 1 and at = 1")
    for i in range(2, 400):
        src.execute("insert into timed.t (id, at, v) values (%s, %s, %s)",
                    (i, i, f"v{i}"))
    hop = _hop(tmp_path)
    hop.databases = ["timed"]
    eng = CassandraEngine(hop)
    eng.native_bulk("timed", ["t"], True, [].append)
    got = dst.execute("select v, ttl(v), writetime(v), w, ttl(w),"
                      " writetime(w), tags from timed.t where id = 1 and"
                      " at = 1").one()
    assert got[0] == "a" and 3500 < got[1] <= 3600 and got[2] == 111, got
    assert got[3] == "later" and got[4] is None and got[5] == 222, got
    assert got[6] == {"x"}
    assert dst.execute("select count(*) from timed.t").one()[0] == 399
    # the clustering key and the set came with the definition
    dst.cluster.refresh_table_metadata("timed", "t")
    made = dst.cluster.metadata.keyspaces["timed"].tables["t"]
    assert [c.name for c in made.clustering_key] == ["at"]
