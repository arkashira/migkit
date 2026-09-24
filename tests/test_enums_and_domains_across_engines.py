"""An enum and a domain are compared across engines, not left out.

PostgreSQL's enums and domains are types of the database's own, and the
cross-engine comparison classifies columns by type name. Measured before,
PostgreSQL to MySQL: both columns were left out as types with no
rendering, and a row whose enum was `sad` on one side and `ok` on the other
came out `ok`, with a footnote. An enum now compares as its label and a
domain as the type it is built on.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MY, PG = "migkit-test-enum-my", "migkit-test-enum-pg"
MY_PORT, PG_PORT = 15729, 15730


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


def _my(sql):
    got = _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest", "-D", "cx",
              "-N", "-B", "-e", sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg(sql):
    got = _sh("docker", "exec", PG, "psql", "-U", "postgres", "-d", "cx",
              "-At", "-v", "ON_ERROR_STOP=1", "-c", sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    for n in (MY, PG):
        _sh("docker", "rm", "-f", "-v", n)
    _sh("docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
        "-p", f"{MY_PORT}:3306", "mysql:8.4")
    _sh("docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
        "-p", f"{PG_PORT}:5432", "postgres:16")
    try:
        for _ in range(90):
            if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
                   "-h127.0.0.1", "--protocol=tcp", "-e",
                   "create database if not exists cx").returncode == 0:
                break
            time.sleep(2)
        for _ in range(60):
            if _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
                   "create database cx").returncode == 0:
                break
            time.sleep(2)
        for port in (MY_PORT, PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _pg("create type mood as enum ('ok', 'sad');"
            " create domain posint as int check (value > 0);"
            " create domain small_posint as posint;"
            " create table t (id int primary key, m mood, d small_posint);"
            " insert into t values (1, 'ok', 5), (2, 'sad', 6)")
        _my("create table t (id int primary key, m enum('ok', 'sad'),"
            " d int); insert into t values (1, 'ok', 5), (2, 'ok', 6)")
        yield
    finally:
        for n in (MY, PG):
            _sh("docker", "rm", "-f", "-v", n)


def _check(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="en", engine="hetero",
              options={"source_engine": "postgres",
                       "target_engine": "mysql"},
              source=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"), databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return [r for r in HeteroEngine(hop).check_data("cx")
            if r.check == "data"]


def test_a_differing_enum_is_a_difference(pair, tmp_path):
    got = _check(tmp_path)
    assert [r.status for r in got] == ["diff"], [r.__dict__ for r in got]
    assert "no canonical rendering" not in got[0].detail, got[0].detail


def test_equal_enums_and_domains_are_equal_and_compared(pair, tmp_path):
    _my("update t set m = 'sad' where id = 2")
    got = _check(tmp_path)
    assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]
    # both columns were part of it, not footnoted out of it
    assert "no canonical rendering" not in got[0].detail, got[0].detail
    _my("update t set d = 60 where id = 2")
    got = _check(tmp_path)
    assert [r.status for r in got] == ["diff"], [r.__dict__ for r in got]
