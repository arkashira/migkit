"""A cross-engine check waits for migkit's own tail before calling a
difference one.

The tail carries changes from the source's log to a target of another
engine. A check taken while it runs saw a change it had read and not yet
applied, and reported the table different. Now, where the tail is running,
the check waits until the tail has read as far as the source's log is at
that moment, and looks again: what converged was still arriving. A
difference made on the target alone still reads `diff`.

Found on the way: the MySQL tail's position did not move past what it left
out - other databases, excluded tables - so on a server busy elsewhere it
never reached the log's end, and a fence waiting for that never passed.
"""
import ctypes
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MY, PG = "migkit-test-xconfirm-my", "migkit-test-xconfirm-pg"
MY_PORT, PG_PORT = 15731, 15732


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def my(sql):
    got = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                          "-N", "-B", "-e", sql], capture_output=True,
                         text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def pg(sql):
    got = subprocess.run(["docker", "exec", PG, "psql", "-U", "postgres",
                          "-d", "cx", "-At", "-c", sql], capture_output=True,
                         text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    for n in (MY, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8.4", "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    try:
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "psql", "-U",
                               "postgres", "-c", "create database cx"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        for port in (MY_PORT, PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        my("create database cx; create database other;"
           " create table cx.t (id int primary key, v varchar(20));"
           " create table other.x (id int primary key);"
           " insert into cx.t values (1, 'a'), (2, 'b')")
        pg("create table t (id int primary key, v varchar(20));"
           " insert into t values (1, 'a'), (2, 'b')")
        yield
    finally:
        for n in (MY, PG):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _eng(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="xc", engine="hetero",
              options={"source_engine": "mysql", "target_engine": "postgres",
                       "fence_timeout": 60},
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_the_position_moves_past_what_is_left_out(pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    eng = _eng(tmp_path).src_engine
    start = eng.change_point("src", "cx")
    my("insert into other.x values (1), (2)")
    now = eng.change_point("src", "cx")
    got, token = eng.neutral_changes("src", "cx", start)
    assert got == [] and token == now, (token, now)
    assert MySQLEngine.position_reached(token, now) is True
    assert MySQLEngine.position_reached(start, now) is False


def test_a_change_the_tail_has_not_applied_is_still_arriving(
        pair, tmp_path, monkeypatch):
    from migkit import tailctl
    from migkit.engines.postgres import PostgresEngine
    eng = _eng(tmp_path)
    real = PostgresEngine.neutral_apply
    held = {"once": False}

    def slow(self, side, db, changes):
        if not held["once"]:
            held["once"] = True
            time.sleep(6)
        return real(self, side, db, changes)

    monkeypatch.setattr(PostgresEngine, "neutral_apply", slow)
    said = []
    thread = threading.Thread(
        target=lambda: eng.tail_apply("cx", True, tmp_path /
                                      "tail-token.json", said.append),
        daemon=True)
    thread.start()
    try:
        for _ in range(30):
            if tailctl.alive(tmp_path) and \
                    (tmp_path / "tail-token.json").exists():
                break
            time.sleep(0.5)
        my("insert into cx.t values (3, 'c')")
        time.sleep(2)   # read by the tail, held in its apply
        got = [r for r in eng.check_data("cx") if r.check == "data"]
        assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]
        assert "still arriving" in got[0].detail, got[0].detail
        # a difference made on the target alone is still one
        pg("update t set v = 'wrong' where id = 1")
        got = [r for r in eng.check_data("cx") if r.check == "data"]
        assert [r.status for r in got] == ["diff"], [r.__dict__ for r in got]
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            thread.join(20)


# --- from a PostgreSQL source ----------------------------------------------

@pytest.fixture
def pg_to_my(pair, pg_pair, tmp_path):
    """The PostgreSQL pair's source, and this module's MySQL as target."""
    from tests.conftest import psql
    psql(pg_pair["src"], "create table public.t (id int primary key,"
                         " v varchar(20)); insert into public.t values"
                         " (1, 'a'), (2, 'b')")
    my("drop database if exists pgx; create database pgx;"
       " create table pgx.t (id int primary key, v varchar(20));"
       " insert into pgx.t values (1, 'a'), (2, 'b')")
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="pgx", engine="hetero",
              options={"source_engine": "postgres",
                       "target_engine": "mysql", "fence_timeout": 60},
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              databases=["postgres"], db_map={"postgres": "pgx"})
    hop.report_dir = lambda db=None: tmp_path
    eng = HeteroEngine(hop)
    yield eng
    try:
        psql(pg_pair["src"], "select pg_drop_replication_slot(slot_name)"
                             " from pg_replication_slots")
    except Exception:
        pass


def test_a_postgres_tail_reaches_the_end_of_a_quiet_log(pg_to_my, pg_pair):
    """Its position stayed at the last change, so on a database with none
    it never reached where the log is now."""
    from tests.conftest import psql
    src = pg_to_my.src_engine
    start = src.change_point("src", "postgres")
    # the log moves on with nothing in it for the tail
    psql(pg_pair["src"], "create table public.noise (id int);"
                         " drop table public.noise")
    now = src.log_position("src", "postgres")
    got, token = src.neutral_changes("src", "postgres", start)
    assert got == [], got
    assert src.position_reached(token, now) is True, (token, now)


def test_a_postgres_change_still_arriving_is_confirmed(
        pg_to_my, pg_pair, tmp_path, monkeypatch):
    from tests.conftest import psql

    from migkit import tailctl
    from migkit.engines.mysql import MySQLEngine
    eng = pg_to_my
    real = MySQLEngine.neutral_apply
    held = {"once": False}

    def slow(self, side, db, changes):
        if not held["once"]:
            held["once"] = True
            time.sleep(6)
        return real(self, side, db, changes)

    monkeypatch.setattr(MySQLEngine, "neutral_apply", slow)
    thread = threading.Thread(
        target=lambda: eng.tail_apply("postgres", True, tmp_path /
                                      "tail-token.json", lambda m: None),
        daemon=True)
    thread.start()
    try:
        for _ in range(30):
            if tailctl.alive(tmp_path) and \
                    (tmp_path / "tail-token.json").exists():
                break
            time.sleep(0.5)
        psql(pg_pair["src"], "insert into public.t values (3, 'c')")
        time.sleep(2)
        got = [r for r in eng.check_data("postgres") if r.check == "data"]
        assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]
        assert "still arriving" in got[0].detail, got[0].detail
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            thread.join(20)
