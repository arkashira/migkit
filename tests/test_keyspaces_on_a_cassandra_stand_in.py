"""Amazon Keyspaces as a side of a pair (backlog P0-CLOUDS), its CQL
against Apache Cassandra 4.1 standing in (`tls: false`, a password), which
Keyspaces' own service cannot be here: what differs - TLS only, AWS's
signature, tables made in the background, SingleRegionStrategy, the CDC
stream - is proved against recorded answers
(`test_the_cloud_native_services_as_sides`), and on a real account only.

A keyspace copied with every write at LOCAL_QUORUM and compared; a table
emptied row by row, since Keyspaces takes no TRUNCATE; a changed row a
difference.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

PROVES = [
    ("move.table-copy", "keyspaces"),
    ("schema.create", "keyspaces"),
]

pytestmark = needs_docker

NAME, PORT = "migkit-test-keyspaces", 16458
IMAGE = "cassandra:4.1"
CQL_PASS = "cassandra"


def _session():
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    return Cluster(["127.0.0.1"], port=PORT, auth_provider=PlainTextAuthProvider(
        "cassandra", "cassandra")).connect()


@pytest.fixture(scope="module")
def cluster():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "MAX_HEAP_SIZE=512M", "-e", "HEAP_NEWSIZE=128M", "-p",
                    f"127.0.0.1:{PORT}:9042", IMAGE], check=True,
                   capture_output=True)
    try:
        end = time.time() + 300
        s = None
        while time.time() < end and s is None:
            try:
                s = _session()
            except Exception:
                time.sleep(3)
        assert s is not None
        yield s
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


@pytest.fixture
def ks(cluster):
    for k in ("ks", "ks_copy"):
        cluster.execute(f"drop keyspace if exists {k}")
    cluster.execute("create keyspace ks with replication = {'class':"
                    " 'SimpleStrategy', 'replication_factor': 1}")
    cluster.execute("create table ks.t (id int, n int, note text, tags"
                    " list<int>, primary key (id, n))")
    for i in range(20):
        cluster.execute("insert into ks.t (id, n, note, tags) values"
                        " (%s, %s, %s, %s)", (i % 5, i, f"note {i}", [i, 1]))
    return cluster


def _engine(tmp_path):
    from migkit.engines.keyspaces import KeyspacesEngine
    src = Endpoint(host="127.0.0.1", port=PORT, user="cassandra",
                   password=CQL_PASS, options={"tls": False})
    dst = Endpoint(host="127.0.0.1", port=PORT, user="cassandra",
                   password=CQL_PASS, options={
                       "tls": False, "replication": "{'class':"
                       " 'SimpleStrategy', 'replication_factor': 1}"})
    hop = Hop(name="kspc", engine="keyspaces", source=src, target=dst,
              databases=["ks"], db_map={"ks": "ks_copy"})
    hop.report_dir = lambda db=None: tmp_path
    return KeyspacesEngine(hop)


def _data(eng):
    return {r.scope: r.status for r in eng.check_data("ks")
            if r.check == "data"}


def test_a_keyspace_is_copied_at_local_quorum_and_compared(ks, tmp_path):
    from cassandra import ConsistencyLevel
    from cassandra.cluster import EXEC_PROFILE_DEFAULT

    from migkit.cli import _Checkpoint
    eng = _engine(tmp_path)
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing("ks", lambda m: None)
    for sch, t in eng.list_move_tables("ks"):
        eng.move_table("ks", sch, t, 7, ck, lambda m: None)
    session = eng._session("dst")
    assert session.get_execution_profile(EXEC_PROFILE_DEFAULT) \
        .consistency_level == ConsistencyLevel.LOCAL_QUORUM
    got = _data(eng)
    assert got and set(got.values()) == {"ok"}, got
    ident = eng.server_identity("src", "ks")
    assert ident["id"].startswith("keyspaces:")
    ks.execute("update ks_copy.t set note = 'changed' where id = 1 and n = 6")
    got = _data(_engine(tmp_path))
    assert "diff" in got.values(), got


def test_a_table_is_emptied_row_by_row(ks, tmp_path):
    eng = _engine(tmp_path)
    ks.execute("create keyspace ks_copy with replication = {'class':"
               " 'SimpleStrategy', 'replication_factor': 1}")
    ks.execute("create table ks_copy.t (id int, n int, note text, tags"
               " list<int>, primary key (id, n))")
    for i in range(6):
        ks.execute("insert into ks_copy.t (id, n, note) values (%s, %s,"
                   " 'x')", (i, i))
    assert eng.neutral_empty("dst", "ks", "t") == 6
    assert list(ks.execute("select count(*) from ks_copy.t"))[0][0] == 0
    with pytest.raises(SystemExit, match="does not empty a table on a"):
        eng.neutral_empty("src", "ks", "t")
