"""migkit's MySQL-family replica applies with more than one applier.

Measured, 100,000 one-row transactions queued on the replica before its
applier started: MySQL 8.4 with one applier took 29-55s and with four
14-27s; MariaDB 11.8, which has none by default, took 7.6-9.1s against
4.7-5.2s with four. The plan now gives a target that applies with one
four, where the commit order stays the source's, and says the line that
keeps it past a restart.
"""
import socket
import subprocess
import time
from types import SimpleNamespace

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

NET = "migkit-test-parapply-net"
SRC, DST = "migkit-test-parapply-src", "migkit-test-parapply-dst"
SRC_PORT, DST_PORT = 15777, 15778
IMAGES = {"mysql": ("mysql:8.4", "mysql", "MYSQL_ROOT_PASSWORD", []),
          "mariadb": ("mariadb:11", "mariadb", "MARIADB_ROOT_PASSWORD",
                      ["--log-bin=binlog", "--binlog-format=ROW"])}


def _sql(brand, name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, IMAGES[brand][1],
                          "-uroot", "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _wait(brand, name, port):
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, IMAGES[brand][1],
                             "-uroot", "-ptest", "-h127.0.0.1",
                             "--protocol=tcp", "-e", "select 1"],
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module", params=["mysql", "mariadb"])
def pair(request):
    brand = request.param
    image, _, env, extra = IMAGES[brand]
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    try:
        for name, port, sid in ((SRC, SRC_PORT, 51), (DST, DST_PORT, 52)):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name,
                            "--network", NET, "-e", f"{env}=test", "-p",
                            f"{port}:3306", image, f"--server-id={sid}",
                            # the target keeps a log of its own as well,
                            # with its own writes in it
                            *extra], check=True, capture_output=True)
        _wait(brand, SRC, SRC_PORT)
        _wait(brand, DST, DST_PORT)
        yield brand
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _eng(host="127.0.0.1", port=DST_PORT):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="para", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host=host, port=port, user="root",
                              password="test"),
              databases=["cx"], db_map={"cx": "cy"})
    return MySQLEngine(hop)


#: the variable, what a server that applies with one has, and the statement
KNOB = {"mysql": ("replica_parallel_workers", "1",
                  "set global replica_parallel_workers = 4;"),
        "mariadb": ("slave_parallel_threads", "0",
                    "set global slave_parallel_threads = 4;")}
STOP = {"mysql": "stop replica; reset replica all",
        "mariadb": "stop slave; reset slave all"}


def _single(brand):
    var, one, _ = KNOB[brand]
    _sql(brand, DST, f"{STOP[brand]}; set global {var} = {one}")


@needs_docker
def test_one_applier_becomes_four_and_the_rows_still_arrive(pair):
    brand = pair
    var, one, stmt = KNOB[brand]
    _sql(brand, SRC, "drop database if exists cx; create database cx;"
                     " create table cx.t (id int primary key, v int)")
    _sql(brand, DST, "drop database if exists cy; create database cy;"
                     " create table cy.t (id int primary key, v int)")
    _single(brand)
    eng = _eng()
    plan = eng.replicate_sql("cx", False, "pw")
    start = "start slave;" if brand == "mariadb" else "start replica;"
    assert plan["dst"][-2:] == [stmt, start], plan["dst"]
    assert f"{var} = 4 gives the replica 4 appliers" in plan["note"], \
        plan["note"]
    try:
        for s in plan["src"]:
            _sql(brand, SRC, s)
        for s in plan["dst"]:
            _sql(brand, DST, s.replace("'127.0.0.1'", f"'{SRC}'")
                 .replace(f"= {SRC_PORT}", "= 3306"))
        assert _sql(brand, DST, f"select @@{var}") == "4"
        # one transaction each, the shape a single applier is slowest at
        _sql(brand, SRC, "".join(f"insert into cx.t values ({i}, {i});\n"
                                 for i in range(1, 2001)))
        _sql(brand, SRC, "update cx.t set v = v + 1 where id % 3 = 0;"
                         " delete from cx.t where id % 7 = 0")
        want = _sql(brand, SRC, "select count(*), sum(v) from cx.t")
        for _ in range(90):
            if _sql(brand, DST, "select count(*), sum(v) from cy.t") == want:
                break
            time.sleep(1)
        assert _sql(brand, DST, "select count(*), sum(v) from cy.t") == want
        if brand == "mysql":
            assert _sql(brand, DST, "select count(*) from performance_schema"
                                    ".replication_applier_status_by_worker"
                                    ) == "4"
        # already more than one: the next plan leaves it as it is
        again = _eng().replicate_sql("cx", False, "pw")
        assert stmt not in again["dst"], again["dst"]
        assert "appliers" not in again["note"], again["note"]
        if brand == "mariadb":
            # one while a replica runs cannot be changed; not asked to
            _sql(brand, DST, f"stop slave; set global {var} = 0;"
                             " start slave")
            running = _eng().replicate_sql("cx", False, "pw")
            assert stmt not in running["dst"], running["dst"]
    finally:
        _single(brand)
        _sql(brand, SRC, "drop user if exists 'migkit_repl'@'%'")


@needs_docker
def test_an_order_the_target_does_not_keep_is_not_given_more(pair):
    """Several appliers on MySQL without `replica_preserve_commit_order`
    commit out of the source's order, and a reader of the target could
    see a later transaction before an earlier one."""
    if pair != "mysql":
        pytest.skip("MariaDB's parallel appliers always commit in order")
    _single(pair)
    _sql(pair, DST, "set global replica_preserve_commit_order = 0")
    try:
        assert _eng()._parallel_apply_sql("mysql") == ([], None)
    finally:
        _sql(pair, DST, "set global replica_preserve_commit_order = 1")
    assert _eng()._parallel_apply_sql("mysql")[1] == \
        "replica_parallel_workers = 4"


def test_a_target_that_is_not_there_is_asked_once():
    from migkit.engines.mysql import MySQLEngine
    t0 = time.monotonic()
    assert MySQLEngine(_eng(port=1).hop)._parallel_apply_sql("mysql") == \
        ([], None)
    assert time.monotonic() - t0 < 10


@pytest.mark.parametrize("brand", ["mysql", "mariadb"])
def test_a_managed_target_is_told_the_parameter(monkeypatch, brand):
    from migkit.engines.mysql import MySQLEngine
    eng = _eng(host="db.example.rds.amazonaws.com")
    monkeypatch.setattr(MySQLEngine, "_brands",
                        lambda self: [SimpleNamespace(name=brand)])
    monkeypatch.setattr(MySQLEngine, "_gtid_state",
                        lambda self, b: (False, "gtid OFF"))
    monkeypatch.setattr(MySQLEngine, "_binlog_position",
                        lambda self, side: ("binlog.000001", 4))
    line = f"{KNOB[brand][0]} = 4"
    monkeypatch.setattr(MySQLEngine, "_parallel_apply_sql",
                        lambda self, b: ([KNOB[brand][2]], line))
    plan = eng.replicate_sql("cx", False, "pw")
    assert KNOB[brand][2] not in plan["dst"], plan["dst"]
    assert f"and {line}, or it applies with one" in plan["note"], \
        plan["note"]
