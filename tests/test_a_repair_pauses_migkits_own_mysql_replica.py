"""A row repair pauses the MySQL replica migkit set up, and refuses beside
anyone else's (backlog 4).

The repair refuses to write beside a replica applying changes to the same
target: the row that survives is whichever lands last. migkit's own
PostgreSQL subscription and change tail could be paused for it, but a MySQL
replica migkit set up had no name to tell it from anyone else's, so every
repair beside one stopped. It signs in as migkit's own account, which is
the name. It is paused the way the subscription is:
* first it catches up with the source as it is now, because a change
  still in the relay log, applied after the repair, would put back an
  older value
* then its applier stops, while the receiver keeps fetching
* it starts again after the repair, and what the source wrote meanwhile
  is applied on top

Measured against two MySQL 8.4 servers with GTID on.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = pytest.mark.docker

NET = "migkit-test-mypause-net"
SRC, DST = "migkit-test-mypause-src", "migkit-test-mypause-dst"
SRC_PORT, DST_PORT = 15768, 15769
GTID = ["--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--log-bin"]


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(name, port, server_id):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "--network", NET,
                    "-e", "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                    "mysql:8.4", f"--server-id={server_id}", *GTID],
                   check=True, capture_output=True)
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp",
                             "-e", "select 1"],
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module")
def pair():
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    try:
        _up(SRC, SRC_PORT, 1)
        _up(DST, DST_PORT, 2)
        my(SRC, "create database appdb;"
                " create table appdb.t (id int primary key, v int)")
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _replicate(user):
    my(SRC, f"create user if not exists '{user}'@'%' identified by 'test';"
            f" grant replication slave on *.* to '{user}'@'%'")
    my(DST, "stop replica; reset replica all;"
            f" change replication source to source_host='{SRC}',"
            f" source_port=3306, source_user='{user}',"
            " source_password='test', source_auto_position=1,"
            " get_source_public_key=1; start replica;")
    end = time.time() + 60
    while time.time() < end:
        if my(DST, "select count(*) from information_schema.tables"
                   " where table_schema='appdb'") == "1":
            return
        time.sleep(1)
    pytest.fail("the replica never caught up")


def _eng(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="mp", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _threads():
    got = my(DST, "select (select service_state from performance_schema"
                  ".replication_connection_status), (select service_state"
                  " from performance_schema.replication_applier_status)")
    io, sql = got.split("\t")
    return {"receiver": io, "applier": sql}


def test_someone_elses_replica_still_stops_the_repair(pair, tmp_path):
    from migkit import cli
    _replicate("repl")
    eng = _eng(tmp_path)
    writers = eng.stream_writers("appdb")
    assert writers == [(f"the replica applying from {SRC}", False)], writers
    with pytest.raises(SystemExit) as e:
        cli._make_room_for_rows(eng.hop, eng, "appdb",
                                [type("A", (), {"kind": "rows"})()])
    assert "writing to this target" in str(e.value), e.value


def test_migkits_own_replica_is_caught_up_paused_and_resumed(pair,
                                                            tmp_path):
    from migkit import cli
    _replicate("migkit_repl")
    eng = _eng(tmp_path)
    what = f"the replica applying from {SRC}"
    assert eng.stream_writers("appdb") == [(what, True)]
    my(SRC, "insert into appdb.t values (1, 1), (2, 2)")
    paused = cli._make_room_for_rows(eng.hop, eng, "appdb",
                                     [type("A", (), {"kind": "rows"})()])
    assert paused == [what], paused
    # caught up before it stopped: what the source had is on the target
    assert my(DST, "select count(*) from appdb.t") == "2"
    assert _threads() == {"receiver": "ON", "applier": "OFF"}, _threads()
    # what the source writes meanwhile waits, and lands after the resume
    my(SRC, "insert into appdb.t values (3, 3)")
    time.sleep(2)
    assert my(DST, "select count(*) from appdb.t") == "2"
    eng.resume_writer("appdb", what)
    end = time.time() + 30
    while time.time() < end and my(DST, "select count(*) from appdb.t") \
            != "3":
        time.sleep(1)
    assert my(DST, "select count(*) from appdb.t") == "3"
    assert _threads()["applier"] == "ON"
