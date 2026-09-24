"""The change tail stops at a DDL on the source instead of carrying on past it.

A binlog or a logical slot carries rows, not a DDL the tail could read. So
a column added on the source mid-tail arrived as rows the target could not
take, or took without the column. The tail now holds the source's shape as
it starts and reads it again before each batch it would apply: a change
stops the tail before that batch, with the position saved before it, and
says what changed.

The working tables of an online schema change (`_orders_gho`, `_orders_new`
...) are neither: their rows are not carried and their appearance is not a
change. What the swap does to the real table is.
"""
import ctypes
import json
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, PORT = "migkit-test-ddltail-my", 15699


def my(sql):
    got = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                          "-h127.0.0.1", "--protocol=tcp", "-N", "-e", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8", "--binlog-row-metadata=FULL"], check=True,
                   capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            try:
                my("select 1")
                with socket.socket() as s:
                    s.settimeout(2)
                    if s.connect_ex(("127.0.0.1", PORT)) == 0:
                        break
            except AssertionError:
                pass
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _engine(pg_pair, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="ddlt", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["cx"], db_map={"cx": "postgres"},
              options={"source_engine": "mysql",
                       "target_engine": "postgres"})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _tail(eng, path, seconds=6):
    said, done = [], threading.Event()

    def run():
        try:
            eng.tail_apply("cx", True, path, said.append)
        except BaseException as e:
            said.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    done.wait(seconds)
    if not done.is_set():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
        done.wait(20)
    return " | ".join(said)


def _fresh(pg_pair):
    my("drop database if exists cx; create database cx;"
       " create table cx.t (id int primary key, v varchar(20))")
    psql(pg_pair["dst"], "drop table if exists public.t;"
                         " create table public.t (id int primary key,"
                         " v varchar(20))")


@pytest.mark.usefixtures("mysql")
def test_a_column_added_mid_tail_stops_it_before_the_rows(pg_pair, tmp_path):
    eng = _engine(pg_pair, tmp_path)
    _fresh(pg_pair)
    path = tmp_path / "tail-token.json"
    _tail(eng, path, seconds=4)          # a quiet first run: position saved
    before = json.loads(path.read_text())["token"]
    my("insert into cx.t values (1, 'a');"
       " alter table cx.t add column w int;"
       " insert into cx.t values (2, 'b', 7)")
    said = _tail(eng, path)
    assert "exit: SystemExit" in said, said
    assert "t: column w added" in said, said
    # nothing of that batch landed, and the position did not move past it
    assert psql(pg_pair["dst"], "select count(*) from public.t"
                ).stdout.strip() == "0"
    assert json.loads(path.read_text())["token"] == before


@pytest.mark.usefixtures("mysql")
def test_an_online_schema_changes_ghost_table_is_not_a_change(pg_pair,
                                                              tmp_path):
    eng = _engine(pg_pair, tmp_path)
    _fresh(pg_pair)
    path = tmp_path / "tail-token.json"
    _tail(eng, path, seconds=4)
    my("create table cx._t_gho (id int primary key, v varchar(20));"
       " insert into cx._t_gho values (9, 'ghost');"
       " insert into cx.t values (3, 'real')")
    said = _tail(eng, path)
    assert "SystemExit" not in said, said
    assert psql(pg_pair["dst"], "select v from public.t where id = 3"
                ).stdout.strip() == "real"


@pytest.mark.usefixtures("mysql")
def test_once_the_target_matches_it_carries_on_from_where_it_stopped(
        pg_pair, tmp_path):
    eng = _engine(pg_pair, tmp_path)
    _fresh(pg_pair)
    path = tmp_path / "tail-token.json"
    _tail(eng, path, seconds=4)
    my("insert into cx.t values (1, 'a');"
       " alter table cx.t add column w int;"
       " insert into cx.t values (2, 'b', 7)")
    assert "SystemExit" in _tail(eng, path)
    psql(pg_pair["dst"], "alter table public.t add column w int")
    said = _tail(eng, path)
    assert "SystemExit" not in said, said
    got = psql(pg_pair["dst"], "select id||':'||v||':'||coalesce(w, -1)"
                               " from public.t order by id").stdout.split()
    assert got == ["1:a:-1", "2:b:7"], got
