"""A cutover whose stream keeps no position migkit can hold it to,
drained by a mark written on the source (`drainmark`): a MySQL replica
without GTIDs, whose file and offset of the source's own log say nothing
once the replica's source changes, so the drain had nothing to wait on
and the preflight refused it.

With `cutover:` on, the mark's table is made on the source before the
clock and a first mark proven to reach the target through the replica;
the freeze is one migkit's own account writes through (per database: the
application's grants revoked, its sessions ended); after it a second mark
is written and waited for, and the replica - which applies in the
source's order - has applied everything the source committed before it.
An application writes all the way through: nothing it was told was
written is lost or doubled. A failure at the drain or at the flip goes
back: the mark's table gone from both sides, the replica put back before
the source takes a write again (it starts from the source's position of
that moment), and every write after it carried.
"""
import json
import socket
import subprocess
import time

import pymysql
import pytest
import yaml
from click.testing import CliRunner

from tests import cutover_harness as harness

pytestmark = pytest.mark.docker

NET = "migkit-test-mark-net"
SRC, DST = ("migkit-test-mark-mysrc", 16355), ("migkit-test-mark-mydst", 16356)
HOP = "mym"


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def _my(node, sql):
    got = subprocess.run(["docker", "exec", "-i", node[0], "mysql",
                          "-uroot", "-ptest", "-N", "-B"],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(node, server_id):
    subprocess.run(["docker", "rm", "-f", "-v", node[0]], capture_output=True)
    # GTIDs off, as the server ships: a replica goes by file and offset
    subprocess.run(["docker", "run", "-d", "--name", node[0], "--network",
                    NET, "-e", "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{node[1]}:3306", "mysql:8.4",
                    f"--server-id={server_id}"],
                   check=True, capture_output=True)


def _ready(node):
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", node[0], "mysql", "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp", "-e",
                             "select 1"], capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", node[1])) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{node[0]} never answered")


@pytest.fixture(scope="module")
def servers():
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    try:
        _up(SRC, 1)
        _up(DST, 2)
        _ready(SRC)
        _ready(DST)
        yield
    finally:
        for node in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", node[0]],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


SCHEMA = """
set session sql_log_bin = 0;
create database app;
create table app.orders (id bigint auto_increment primary key,
                         tag varchar(64) not null);
create user app identified by 'CHANGE_ME';
grant select, insert on app.* to app;
insert into app.orders (tag) with recursive g(n) as (select 1 union all
  select n + 1 from g where n < 200) select concat('seed', n) from g;
"""


def _reset(node):
    _my(node, "stop replica; reset replica all;"
              " set session sql_log_bin = 0;"
              " drop database if exists app;"
              " drop user if exists app, migkit_repl;"
              " reset binary logs and gtids;")


@pytest.fixture
def cut(servers, tmp_path, monkeypatch):
    import migkit.config as cfg
    routes = {f"127.0.0.1:{SRC[1]}": f"{SRC[0]}:3306",
              f"127.0.0.1:{DST[1]}": f"{DST[0]}:3306"}
    harness.install(routes, monkeypatch)
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    for node in (SRC, DST):
        _reset(node)
        _my(node, SCHEMA)
    route = tmp_path / "route"
    route.write_text("src")

    class Cut:
        pass
    c = Cut()
    c.tmp, c.route = tmp_path, route
    c.reports = tmp_path / "reports" / HOP
    conf = tmp_path / "hops.yaml"
    conf.write_text(yaml.safe_dump({"hops": {HOP: {
        "engine": "mysql",
        "source": {"host": "127.0.0.1", "port": SRC[1], "user": "root",
                   "password": "test"},
        "target": {"host": "127.0.0.1", "port": DST[1], "user": "root",
                   "password": "test"},
        "databases": ["app"],
        "options": {"protect_target": True, "fence_timeout": 60,
                    "state": {"mirror": str(tmp_path / "mirror")},
                    "cutover": {"budget": 20, "freeze": "database",
                                "flip": f"printf dst > {route}"}}}}}))
    monkeypatch.setattr(cfg, "CONF", str(conf))

    def connect(where):
        return pymysql.connect(host="127.0.0.1",
                               port=SRC[1] if where == "src" else DST[1],
                               user="app", password="CHANGE_ME",
                               database="app", connect_timeout=3,
                               read_timeout=10, write_timeout=10)
    c.connect = connect
    yield c
    for node in (SRC, DST):
        _reset(node)


def _cli(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _tags(node):
    return _my(node, "select tag from app.orders").split()


def _marks(node):
    return _my(node, "select coalesce(group_concat(table_name), '') from"
                     " information_schema.tables where table_schema = 'app'"
                     " and table_name = 'migkit_cutover_mark'")


def _follow(c):
    got, said = _cli("move", HOP, "--mode", "cdc", "--go")
    assert got.exit_code == 0, said
    running = ("select count(*) from performance_schema"
               ".replication_connection_status c join performance_schema"
               ".replication_applier_status a using (channel_name) where"
               " c.service_state = 'ON' and a.service_state = 'ON'")
    assert harness.wait_for(lambda: _my(DST, running) == "1", 60), said


def _writing(c):
    w = harness.Writer(c.connect, c.route, "app.orders")
    w.start()
    assert harness.wait_for(lambda: len(w.acked()) >= 30, 30), w.errors()[-3:]
    return w


def _nothing_lost_or_twice(w, node):
    have = _tags(node)
    assert len(have) == len(set(have)), "a row written twice"
    assert set(w.acked()) <= set(have), \
        f"{len(set(w.acked()) - set(have))} acknowledged rows missing"


def test_a_replica_without_gtids_is_drained_by_a_mark(cut):
    c = cut
    _follow(c)
    got, said = _cli("move", HOP, "--mode", "cdc", "--drop")
    assert "GTID is off" in said and "the drain: a mark written on the" \
        " source after the freeze" in said, said
    w = _writing(c)
    try:
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code == 0, said
        assert "cut over:" in said, said
        flipped = time.time()
        assert harness.wait_for(lambda: len(w.oks_after(flipped)) >= 20, 30)
    finally:
        w.stop()
    print(f"\ndrained by a mark: {w.outage():.2f}s without an acknowledged"
          " write")
    _nothing_lost_or_twice(w, DST)
    run = [json.loads(x) for x in
           (c.reports / "cutover-runs.jsonl").read_text().splitlines()][-1]
    assert run["state"] == "flipped" and run["drain"] == "mark", run
    token = run["phases"]["drain"]["positions"]["app"].split()[-1]
    # the mark written after the freeze is what the drain found there
    assert _my(DST, "select count(*) from app.migkit_cutover_mark where"
                    f" token = '{token}'") == "1"


def test_a_cutover_drained_by_a_mark_goes_back_whole(cut, monkeypatch):
    from migkit import failpoint
    c = cut
    _follow(c)
    grants = _my(SRC, "show grants for app")
    w = _writing(c)
    try:
        for phase in ("drain", "flip"):
            failpoint._counts.clear()
            monkeypatch.setenv("MIGKIT_FAILPOINT", f"cutover.{phase}:1:raise")
            got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
            assert got.exit_code != 0 and "the cutover was undone" in said, \
                (phase, said)
            back = time.time()
            assert c.route.read_text() == "src", phase
            assert harness.wait_for(lambda: len(w.oks_after(back)) >= 10,
                                    30), (phase, w.errors()[-3:])
            assert _my(SRC, "show grants for app") == grants, phase
            # the mark's table gone from both sides
            assert harness.wait_for(lambda: _marks(DST) == "", 30), phase
            assert _marks(SRC) == "", phase
            # the replica carries what the source took after it came back
            acked = list(w.acked())
            assert harness.wait_for(lambda: set(acked) <= set(_tags(DST)),
                                    60), phase
    finally:
        monkeypatch.delenv("MIGKIT_FAILPOINT", raising=False)
        w.stop()
    _nothing_lost_or_twice(w, SRC)
