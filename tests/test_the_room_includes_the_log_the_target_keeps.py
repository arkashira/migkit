"""The room a load needs on the target includes the log the target keeps
while loading (backlog 6).

Measured on PostgreSQL 16 with `max_wal_size = 64MB`: a 355 MiB table
loaded in one statement wrote 411 MiB of WAL, and `pg_wal` peaked at
80 MiB, because checkpoints recycle it. The same load with one inactive
slot on the target kept all 416 MiB. A MySQL target that keeps a binlog
keeps all of it until it expires, 30 days by default.
"""
import socket
import subprocess
import time

import pytest

from migkit import planner
from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

PG, MY = "migkit-test-room-pg", "migkit-test-room-my"
PG_PORT, MY_PORT = 15822, 15823


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    for n in (PG, MY):
        _sh("docker", "rm", "-f", "-v", n)
    _sh("docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
        "-p", f"{PG_PORT}:5432", "postgres:16", "-c", "max_wal_size=64MB",
        "-c", "wal_level=logical")
    _sh("docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
        "-p", f"{MY_PORT}:3306", "mysql:8.4")
    try:
        for _ in range(60):
            if _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
                   "select 1").returncode == 0:
                break
            time.sleep(1)
        for _ in range(90):
            if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
                   "-h127.0.0.1", "--protocol=tcp", "-e",
                   "select 1").returncode == 0:
                break
            time.sleep(2)
        for port in (PG_PORT, MY_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        yield
    finally:
        for n in (PG, MY):
            _sh("docker", "rm", "-f", "-v", n)


def _pg():
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                  password="test")
    return PostgresEngine(Hop(name="room", engine="postgres", source=ep,
                              target=ep, databases=["postgres"]))


def _my():
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    return MySQLEngine(Hop(name="room", engine="mysql", source=ep,
                           target=ep, databases=["mysql"]))


GB = 2 ** 30


def test_postgresql_holds_what_checkpoints_leave(servers):
    held, why = _pg().log_kept("postgres", 10 * GB)
    assert held == int(64 * 2 ** 20 * 1.25), held
    assert "recycle" in why, why
    # a load smaller than the bound holds no more than it wrote
    assert _pg().log_kept("postgres", 10 * 2 ** 20)[0] == 10 * 2 ** 20


def test_a_slot_on_the_target_holds_all_of_it(servers):
    _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
        "select pg_create_logical_replication_slot('held', 'pgoutput')")
    try:
        held, why = _pg().log_kept("postgres", 10 * GB)
        assert held == 10 * GB and "1 replication slot" in why, (held, why)
    finally:
        _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
            "select pg_drop_replication_slot('held')")


def test_mysql_keeps_its_binlog_until_it_expires(servers):
    held, why = _my().log_kept("mysql", 10 * GB)
    assert held == 10 * GB and "30 days" in why, (held, why)


def test_the_plan_counts_it_against_the_room(servers):
    decisions = [planner.Decision("public.a", planner.BULK, "")]
    facts = {"public.a": {"rows": 1, "bytes": 6 * GB, "index_bytes": 2 * GB}}
    # 8 GB of tables and indexes fits in 9 GB free; with the WAL a slot
    # keeps, it does not
    fits = planner.size_line(decisions, facts, "postgres", free=9 * GB,
                             kept=lambda grows: _pg().log_kept("postgres",
                                                               grows))
    assert "NOT ENOUGH" not in fits, fits
    assert "plus the 80.0 MB of its WAL it holds at once" in fits, fits
    _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
        "select pg_create_logical_replication_slot('held', 'pgoutput')")
    try:
        short = planner.size_line(decisions, facts, "postgres", free=9 * GB,
                                  kept=lambda grows: _pg().log_kept(
                                      "postgres", grows))
    finally:
        _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
            "select pg_drop_replication_slot('held')")
    assert "NOT ENOUGH" in short and "plus the 8.0 GB of its WAL" in short, \
        short
