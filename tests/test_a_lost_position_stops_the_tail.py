"""A tail whose saved position the source no longer holds stops and says so.

Each change log keeps only so much: a PostgreSQL slot can be dropped or lost
with a rebuilt server, a MySQL binlog is purged on a schedule, a MongoDB
oplog wraps. A tail resuming from a position that is gone has lost every
change after it, and the one thing it must not do is carry on from wherever
the log is now - that is a hole nobody would find until `check`, if anyone
runs it before the cutover.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]

PG, MY, MG = "migkit-test-lost-pg", "migkit-test-lost-my", "migkit-test-lost-mg"
PG_PORT, MY_PORT, MG_PORT = 15678, 15679, 15680


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _exec(name, *cmd):
    return subprocess.run(["docker", "exec", name, *cmd],
                          capture_output=True, text=True)


def _until(probe, what, tries=60):
    for _ in range(tries):
        if probe():
            return
        time.sleep(2)
    pytest.fail(f"{what} never answered")


@pytest.fixture
def pg():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16", "-c", "wal_level=logical"],
                   check=True, capture_output=True)
    try:
        assert _wait(PG_PORT)
        _until(lambda: _exec(PG, "psql", "-U", "postgres", "-c",
                             "select 1").returncode == 0, "postgres")
        _exec(PG, "psql", "-U", "postgres", "-c",
              "create table t (id int primary key, v text)")
        from migkit.engines.postgres import PostgresEngine
        ep = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                      password="test")
        yield PostgresEngine(Hop(name="lost", engine="postgres", source=ep,
                                 target=ep, databases=["postgres"]))
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


def test_a_dropped_slot_is_not_quietly_made_again(pg):
    token = pg.change_point("src", "postgres")
    _exec(PG, "psql", "-U", "postgres", "-c",
          "insert into t values (1, 'written after the position')")
    got = _exec(PG, "psql", "-U", "postgres", "-At", "-c",
                "select pg_drop_replication_slot(slot_name)"
                " from pg_replication_slots")
    assert got.returncode == 0, got.stderr
    with pytest.raises(SystemExit) as e:
        pg.neutral_changes("src", "postgres", token)
    assert "is gone" in str(e.value), e.value
    # and it did not make a fresh one that a retry would read from now
    left = _exec(PG, "psql", "-U", "postgres", "-At", "-c",
                 "select count(*) from pg_replication_slots").stdout.strip()
    assert left == "0", left


def test_a_fresh_start_still_makes_its_slot(pg):
    """No saved position means nothing to have lost."""
    assert pg.change_point("src", "postgres")
    got, _ = pg.neutral_changes("src", "postgres", None)
    assert got == []


@pytest.fixture
def mysql():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8", "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    try:
        assert _wait(MY_PORT)
        # over TCP: the image's first, temporary server answers on the
        # socket only, and is shut down again right after
        _until(lambda: _exec(MY, "mysql", "-uroot", "-ptest", "-h127.0.0.1",
                             "--protocol=tcp", "-e", "select 1"
                             ).returncode == 0, "mysql")
        _exec(MY, "mysql", "-uroot", "-ptest", "-e",
              "create database cx; create table cx.t"
              " (id int primary key, v text)")
        from migkit.engines.mysql import MySQLEngine
        ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                      password="test")
        yield MySQLEngine(Hop(name="lost", engine="mysql", source=ep,
                              target=ep, databases=["cx"]))
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def test_a_purged_binlog_is_named_not_skipped(mysql):
    token = mysql.change_point("src", "cx")

    def my(sql):
        got = _exec(MY, "mysql", "-uroot", "-ptest", "-N", "-e", sql)
        assert got.returncode == 0, got.stderr
        return got.stdout.split()
    my("insert into cx.t values (1, 'written after the position')")
    my("flush binary logs")
    my("flush binary logs")
    last = my("show binary log status")[0]
    my(f"purge binary logs to '{last}'")
    with pytest.raises(SystemExit) as e:
        mysql.neutral_changes("src", "cx", token)
    said = str(e.value)
    assert "no longer has the binlog" in said, said
    assert token["log_file"] in said, said
