"""What a check leaves behind on the server after it has answered.

A verifier reads other people's production tables. If it walks away from one
still inside a transaction, the table it read cannot be altered by anybody
until the process that read it exits - and nothing in migkit's own output
would say so. It was found the way it would be found in the field, by
something else waiting forever:

    connection state after `select count(*)`   idle in transaction
    `drop table` from another session          waits, and goes on waiting
    after the connection closes                DROP TABLE, immediately

That was the generic engine, which borrows reladiff's connection. The sweep
that followed measured every other engine against its own server rather than
reading their code, and they were already clean:

    postgres   one per side left `idle` after a schema check, and twenty
               calls in a row still left one rather than twenty
    mysql      nothing left at all
    sqlite     the file stays writable by other processes
    redis      no client left in `CLIENT LIST`
    mongodb    the driver's pool, idle
    hetero     nothing left on either side

So this file is not a list of known bugs. It pins the property for the
engines where holding a transaction would block somebody: the three that
speak SQL over a connection migkit keeps, and the one that already got it
wrong. The check each test makes is the operator's question rather than a
connection count - can somebody else still take the table?
"""
import pathlib
import socket
import sqlite3
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.util import which
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY1, MY2 = "migkit-test-lock-my1", "migkit-test-lock-my2"
MY1_PORT, MY2_PORT = 13393, 13394


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def _pg_takeable(port, table="t"):
    """Whether another session can take the table outright.

    An exclusive lock is what `alter table` and `drop table` need, so this
    is the thing a left-open transaction would deny. The timeout is what
    stops a failure from hanging the suite the way the original bug did.
    """
    got = psql(port, "set lock_timeout='5s'; begin;"
                     f" lock table {table} in access exclusive mode; commit;")
    return "" if got.returncode == 0 else (got.stderr or got.stdout).strip()


def _pg_in_transaction(port):
    return psql(port, "select count(*) from pg_stat_activity"
                      " where state = 'idle in transaction'").stdout.strip()


def _mysql(container, sql):
    return subprocess.run(
        ["docker", "exec", container, "mysql", "-uroot", "-ptest", "-N", "-B",
         "-h127.0.0.1", "--protocol=tcp", "-e", sql],
        capture_output=True, text=True)


def _mysql_takeable(container, table="t"):
    got = _mysql(container, "set session lock_wait_timeout=5;"
                            f" use app; alter table {table} comment 'probe';")
    return "" if got.returncode == 0 else (got.stderr or got.stdout).strip()


@pytest.fixture(scope="module")
def mysql_pair():
    for name, port in ((MY1, MY1_PORT), (MY2, MY2_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for port in (MY1_PORT, MY2_PORT):
        assert _wait(port)
    for name in (MY1, MY2):
        for _ in range(60):
            if _mysql(name, "select 1").returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{name} never answered")
        _mysql(name, "create database app;"
                     " create table app.t (id bigint primary key, v text);"
                     " insert into app.t values (1,'a'),(2,'b'),(3,'c');")
    yield {"src": MY1_PORT, "dst": MY2_PORT}
    for name in (MY1, MY2):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def _pg_engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="l", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _seed_pg(pg_pair, target_changes=""):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table t (id bigint primary key, v text);"
                   " insert into t select g, 'v'||g from"
                   " generate_series(1,50) g;")
    if target_changes:
        psql(pg_pair["dst"], target_changes)


def test_every_postgres_check_hands_the_tables_back(pg_pair, tmp_path):
    _seed_pg(pg_pair)
    eng = _pg_engine(pg_pair, tmp_path)
    for name in ("check_schema", "check_counts", "check_autoinc",
                 "check_data"):
        got = getattr(eng, name)("postgres")
        # the two sides were seeded identically, so anything but `ok` means
        # the check did not read them - and a test that never made the
        # engine open a connection proves nothing about what it leaves open
        assert [r.status for r in got] == ["ok"] * len(got), [
            (r.status, r.detail) for r in got]
        for side in ("src", "dst"):
            denied = _pg_takeable(pg_pair[side])
            assert not denied, f"after {name}, the {side} table: {denied}"
        assert _pg_in_transaction(pg_pair["src"]) == "0"
        assert _pg_in_transaction(pg_pair["dst"]) == "0"


def test_a_postgres_repair_hands_the_tables_back(pg_pair, tmp_path):
    """The write path holds more than the read path, and a repair that kept
    its transaction open would leave the table it just fixed unalterable."""
    _seed_pg(pg_pair, "update t set v='CHANGED' where id in (5,6);"
                      " delete from t where id=7;"
                      " insert into t values (999,'x');")
    eng = _pg_engine(pg_pair, tmp_path)
    assert [r.status for r in eng.check_data("postgres")] == ["diff"]
    actions = eng.repair_plan("postgres", "rows")
    assert actions, "nothing to repair, so the write path never ran"
    for action in actions:
        eng.apply("postgres", action)
    assert [r.status for r in eng.check_data("postgres")] == ["ok"]
    for side in ("src", "dst"):
        assert not _pg_takeable(pg_pair[side]), side
    assert _pg_in_transaction(pg_pair["dst"]) == "0"


@pytest.mark.skipif(not which("reladiff"),
                    reason="reladiff is not where migkit would look for it")
def test_the_generic_repair_hands_the_tables_back(pg_pair, tmp_path):
    """The engine the measurement in this file's header came from. It has no
    driver of its own and borrows one, which is exactly why its connections
    are nobody else's to close."""
    from migkit.engines.generic import GenericEngine
    _seed_pg(pg_pair, "update t set v='CHANGED' where id=5;"
                      " insert into t values (999,'x');")

    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": ["t"], "key": "id"})
    hop.report_dir = lambda db=None: tmp_path
    eng = GenericEngine(hop)
    assert eng.check_data("-")[0].status == "diff"
    actions = eng.repair_plan("-", "rows")
    assert actions
    for action in actions:
        eng.apply("-", action)
    assert eng.check_data("-")[0].status == "ok"
    for side in ("src", "dst"):
        assert not _pg_takeable(pg_pair[side]), side


@pytest.mark.skipif(not which("reladiff"),
                    reason="reladiff is not where migkit would look for it")
def test_a_refused_repair_hands_the_tables_back_too(pg_pair, tmp_path):
    """A refusal leaves the connection where an exception found it, which is
    the harder half: the rows it had read are still inside a transaction
    unless something closes it on the way out."""
    from migkit.engines.generic import GenericEngine
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table t (id bigint primary key, b bytea);"
                   " insert into t values (1, '\\x0001'::bytea);")
    psql(pg_pair["dst"], "update t set b = '\\xffff'::bytea where id = 1;")

    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": ["t"], "key": "id"})
    hop.report_dir = lambda db=None: tmp_path
    eng = GenericEngine(hop)
    assert eng.check_data("-")[0].status == "diff"
    actions = eng.repair_plan("-", "rows")
    assert actions
    with pytest.raises(SystemExit):
        eng.apply("-", actions[0])
    for side in ("src", "dst"):
        assert not _pg_takeable(pg_pair[side]), side


def test_every_mysql_check_hands_the_tables_back(mysql_pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="l", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=mysql_pair["src"],
                              user="root", password="test"),
              target=Endpoint(host="127.0.0.1", port=mysql_pair["dst"],
                              user="root", password="test"),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    for name in ("check_schema", "check_counts", "check_autoinc",
                 "check_data"):
        got = getattr(eng, name)("app")
        assert [r.status for r in got] == ["ok"] * len(got), [
            (r.status, r.detail) for r in got]
        for container in (MY1, MY2):
            denied = _mysql_takeable(container)
            assert not denied, f"after {name}, {container}: {denied}"
        open_tx = _mysql(MY1, "select count(*) from"
                              " information_schema.innodb_trx").stdout.strip()
        assert open_tx == "0", open_tx


def test_a_sqlite_check_leaves_the_file_writable(tmp_path):
    """SQLite has no server to ask, and a reader that keeps its transaction
    open locks the file for every other process on the machine."""
    from migkit.engines.sqlite import SQLiteEngine
    paths = []
    for name in ("a.db", "b.db"):
        path = tmp_path / name
        con = sqlite3.connect(path)
        con.execute("create table t (id integer primary key, v text)")
        con.executemany("insert into t values (?,?)",
                        [(i, f"v{i}") for i in range(20)])
        con.commit()
        con.close()
        paths.append(path)
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host=str(paths[0]), port=0, user="",
                              password=""),
              target=Endpoint(host=str(paths[1]), port=0, user="",
                              password=""),
              db_map={"main": "main"})
    hop.report_dir = lambda db=None: tmp_path
    eng = SQLiteEngine(hop)
    for name in ("check_schema", "check_counts", "check_data"):
        got = getattr(eng, name)("main")
        assert [r.status for r in got] == ["ok"] * len(got), [
            (r.status, r.detail) for r in got]
        for path in paths:
            other = sqlite3.connect(path, timeout=5)
            try:
                other.execute("insert into t values (999,'probe')")
                other.commit()
                other.execute("delete from t where id=999")
                other.commit()
            except sqlite3.OperationalError as e:
                pytest.fail(f"after {name}, {pathlib.Path(path).name}: {e}")
            finally:
                other.close()
