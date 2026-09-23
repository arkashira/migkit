"""MySQL fences before cutover, the way PostgreSQL already did.

The capability matrix declared MySQL's fence a gap: nothing could prove the
target had caught up with the source before a cutover, so a check taken at
that moment could not tell "still in flight" from "wrong". The server
answers it itself with GTIDs: take the source's executed set, and ask the
target to wait until it has applied that set.

Measured against a real source and replica, both MySQL 8.4 with GTID on:
the fence passes once the replica has applied the writes, times out while
its applier is stopped, and has nothing to fence on when the target is not
replicating at all.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = pytest.mark.docker

NET = "migkit-test-fence-net"
SRC, DST = "migkit-test-fence-src", "migkit-test-fence-dst"
SRC_PORT, DST_PORT = 15663, 15664
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


def _my(name, sql):
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
                             "-ptest", "-e", "select 1"],
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module")
def replica():
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    try:
        _up(SRC, SRC_PORT, 1)
        _up(DST, DST_PORT, 2)
        _my(SRC, "create user 'repl'@'%' identified by 'test';"
                 " grant replication slave on *.* to 'repl'@'%';"
                 " create database appdb;"
                 " create table appdb.t (id int primary key);")
        _my(DST, f"change replication source to source_host='{SRC}',"
                 " source_port=3306, source_user='repl',"
                 " source_password='test', source_auto_position=1,"
                 " get_source_public_key=1; start replica;")
        end = time.time() + 60
        while time.time() < end:
            if _my(DST, "select count(*) from information_schema.tables"
                        " where table_schema='appdb'") == "1":
                break
            time.sleep(1)
        yield
    finally:
        for n in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _engine():
    from migkit.engines.mysql import MySQLEngine
    return MySQLEngine(Hop(
        name="f", engine="mysql",
        source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                        password="test"),
        databases=["appdb"]))


def test_the_fence_passes_once_the_target_has_applied_it(replica):
    eng = _engine()
    _my(SRC, "insert into appdb.t values (1), (2), (3);")
    point = eng.src_lsn("appdb")
    assert point, "GTID is on, so there is a position to fence on"
    assert eng.fence_wait("appdb", point, timeout=60) is True
    assert _my(DST, "select count(*) from appdb.t") == "3"


def test_the_fence_holds_while_the_applier_is_stopped(replica):
    eng = _engine()
    _my(DST, "stop replica sql_thread;")
    try:
        _my(SRC, "insert into appdb.t values (10);")
        point = eng.src_lsn("appdb")
        assert eng.fence_wait("appdb", point, timeout=8) is False
    finally:
        _my(DST, "start replica sql_thread;")
    assert eng.fence_wait("appdb", point, timeout=60) is True


def _checked(eng):
    return {r.scope: r for r in eng.check_data("appdb")}


def test_a_difference_still_arriving_is_confirmed_not_reported(replica,
                                                               tmp_path):
    """The applier is held while rows land on the source, so the checksum
    sees the target behind; it is released a few seconds later, while the
    confirm pass waits on the fence. The verdict is the converged one."""
    import threading
    eng = _engine()
    eng.hop.options["fence_timeout"] = 60
    eng.hop.report_dir = lambda db=None, _p=tmp_path: _p
    _my(SRC, "create table if not exists appdb.w (id int primary key,"
             " v varchar(10));")
    assert eng.fence_wait("appdb", eng.src_lsn("appdb"), timeout=60)
    _my(DST, "stop replica sql_thread;")
    _my(SRC, "insert into appdb.w values (1, 'a'), (2, 'b');")
    threading.Timer(5, lambda: _my(DST, "start replica sql_thread;")).start()
    got = _checked(eng)["appdb.w"]
    assert got.status == "ok", (got.status, got.detail)
    assert "still arriving" in got.detail, got.detail


def test_a_real_difference_survives_the_fence(replica, tmp_path):
    eng = _engine()
    eng.hop.options["fence_timeout"] = 60
    eng.hop.report_dir = lambda db=None, _p=tmp_path: _p
    _my(SRC, "create table if not exists appdb.r (id int primary key,"
             " v varchar(10)); insert into appdb.r values (1, 'a');")
    assert eng.fence_wait("appdb", eng.src_lsn("appdb"), timeout=60)
    # written on the target alone: no amount of waiting heals it
    _my(DST, "update appdb.r set v = 'X' where id = 1;")
    got = _checked(eng)["appdb.r"]
    assert got.status == "diff", (got.status, got.detail)


def test_no_replication_is_nothing_to_fence_on_not_a_pass(replica):
    """None, not True: the caller has to say it could not fence."""
    eng = _engine()
    _my(DST, "stop replica; reset replica all;")
    assert eng.fence_wait("appdb", eng.src_lsn("appdb"), timeout=5) is None
