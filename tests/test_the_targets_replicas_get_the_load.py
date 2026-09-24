"""A MySQL load reaches the target's own replicas.

The loader turns the binlog off for its own sessions unless it is told
otherwise. Measured on 8.4, with a replica following the target: the
replica received the database and the table the move created, and none of
the table's 300,000 rows. Nothing said so, and the target's point-in-time
recovery, which replays the same log, would not have had them either. The
load now writes the binlog wherever the target keeps one.
"""
import socket
import subprocess
import time

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

NET = "migkit-test-downstream-net"
SRC, DST, REP = ("migkit-test-downstream-src", "migkit-test-downstream-dst",
                 "migkit-test-downstream-rep")
SRC_PORT, DST_PORT = 15737, 15738


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(name, *args):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "--network", NET,
                    "-e", "MYSQL_ROOT_PASSWORD=test", *args],
                   check=True, capture_output=True)
    for _ in range(90):
        if subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                           "-ptest", "-h127.0.0.1", "--protocol=tcp", "-e",
                           "select 1"], capture_output=True).returncode == 0:
            return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module")
def estate():
    if not _docker():
        pytest.skip("docker not available")
    if not (movers.which("mydumper") and movers.which("myloader")):
        pytest.skip("the MySQL dump programs are not installed")
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    try:
        _up(SRC, "-p", f"{SRC_PORT}:3306", "mysql:8.4", "--server-id=51")
        _up(DST, "-p", f"{DST_PORT}:3306", "mysql:8.4", "--server-id=52")
        _up(REP, "mysql:8.4", "--server-id=53")
        for port in (SRC_PORT, DST_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        status = my(DST, "show binary log status").split()
        my(REP, "change replication source to SOURCE_HOST = '" + DST + "',"
                " SOURCE_PORT = 3306, SOURCE_USER = 'root',"
                " SOURCE_PASSWORD = 'test', GET_SOURCE_PUBLIC_KEY = 1,"
                f" SOURCE_LOG_FILE = '{status[0]}',"
                f" SOURCE_LOG_POS = {status[1]}; start replica;")
        my(SRC, "create database appdb; create table appdb.t (id int"
                " primary key, v varchar(20)); insert into appdb.t values"
                " (1, 'a'), (2, 'b'), (3, 'c')")
        yield
    finally:
        for name in (SRC, DST, REP):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _hop(tmp_path):
    hop = Hop(name="ds", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def test_a_replica_of_the_target_gets_the_rows(estate, tmp_path):
    movers.mydumper_move(_hop(tmp_path), "appdb", 2, True, lambda m: None)
    assert my(DST, "select count(*) from appdb.t") == "3"
    for _ in range(30):
        got = subprocess.run(["docker", "exec", REP, "mysql", "-uroot",
                              "-ptest", "-N", "-B", "-e",
                              "select count(*) from appdb.t"],
                             capture_output=True, text=True).stdout.strip()
        if got == "3":
            break
        time.sleep(1)
    assert got == "3", got


def test_a_target_without_a_binlog_is_loaded_as_before(tmp_path):
    """A plan against a target that cannot be asked yet: nothing added."""
    hop = Hop(name="n", engine="mysql",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"), databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    if not (movers.which("mydumper") and movers.which("myloader")):
        pytest.skip("the MySQL dump programs are not installed")
    steps = movers.mydumper_move(hop, "appdb", 2, False, None)
    load = [s for s in steps if getattr(s, "argv", None)
            and s.argv[0] == "myloader"][0]
    assert "--enable-binlog" not in load.argv, load.argv
