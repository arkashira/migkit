"""MariaDB's and Percona Server's copy position is measured the way
MySQL's is (AUD.slot.more): with a commit held between the binlog and the
engine, the position a copy's tail starts from either has the insert
after it or returns once a read sees it."""
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

#: image, client program, root-password variable, the settings that hold a
#: commit between the log and the engine, and let go
SERVERS = {
    "mariadb": dict(
        image="mariadb:11.4", client="mariadb", port=16582,
        env="MARIADB_ROOT_PASSWORD",
        hold=("set global binlog_commit_wait_count = 2;"
              " set global binlog_commit_wait_usec = 1500000"),
        free=("set global binlog_commit_wait_count = 0;"
              " set global binlog_commit_wait_usec = 100000")),
    "percona": dict(
        image="percona/percona-server:8.4", client="mysql", port=16583,
        env="MYSQL_ROOT_PASSWORD",
        hold="set global binlog_group_commit_sync_delay = 1500000",
        free="set global binlog_group_commit_sync_delay = 0"),
}


def _sql(name, sql):
    cfg = SERVERS[name]
    got = subprocess.run(
        ["docker", "exec", "-i", f"migkit-test-cp-{name}", cfg["client"],
         "-uroot", "-ptest", "-N", "-B"], input=sql, capture_output=True,
        text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(params=sorted(SERVERS), scope="module")
def server(request):
    name, cfg = request.param, SERVERS[request.param]
    box = f"migkit-test-cp-{name}"
    subprocess.run(["docker", "rm", "-f", "-v", box], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", box, "-e",
                    f"{cfg['env']}=test", "-p",
                    f"127.0.0.1:{cfg['port']}:3306", cfg["image"],
                    "--server-id=7", "--log-bin=binlog",
                    "--binlog-format=ROW"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 240
        while time.time() < end:
            ok = subprocess.run(
                ["docker", "exec", box, cfg["client"], "-uroot", "-ptest",
                 "-h127.0.0.1", "--protocol=tcp", "-e", "select 1"],
                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", cfg["port"])) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail(f"{name} never answered")
        yield name
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", box], capture_output=True)


def test_a_commit_in_flight_is_not_before_the_copys_position(
        server, tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    conf = SERVERS[server]
    _sql(server, "drop database if exists cp; create database cp;"
                 " create table cp.t (id int primary key)")
    ep = Endpoint(host="127.0.0.1", port=conf["port"], user="root",
                  password="test")
    eng = MySQLEngine(Hop(name="cp", engine="mysql", source=ep, target=ep,
                          databases=["cp"]))
    _sql(server, conf["hold"])
    insert = threading.Thread(target=_sql, args=(
        server, "insert into cp.t values (1)"))
    try:
        insert.start()
        time.sleep(0.4)
        stage = _sql(server, "select state from information_schema.processlist"
                             " where info like 'insert into cp.t%'")
        unseen = _sql(server, "select count(*) from cp.t")
        end = eng.change_point("src", "cp")
        point = eng.copy_point("src", "cp")
        seen = _sql(server, "select count(*) from cp.t")
        insert.join(30)
    finally:
        _sql(server, conf["free"])
    if server == "percona":
        # the moment was hit: its snapshot position is the log's end, past
        # an insert no read could see (measured 8.4.11), and the copy's
        # position returned only once the insert could be read
        assert unseen == "0" and stage == "waiting for handler commit", (
            unseen, stage)
        assert seen == "1", (end, point, seen)
    events = _sql(server, f"show binlog events in '{point['log_file']}'"
                          f" from {point['log_pos']}")
    assert seen == "1" or "Write_rows" in events, (point, seen, events)
