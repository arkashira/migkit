"""Two ways at once through migkit's own tails: MySQL to PostgreSQL and
PostgreSQL to MySQL at the same time (backlog R3).

Each side's changes reach the other and stop there: every transaction a
tail applies begins with its mark (`migkit_origin`), and the tail reading
that side the other way leaves the whole transaction out. Measured before,
20 rows written on each side: the tail into PostgreSQL applied 40 changes
and the one into MySQL 60 - every change came back where it began, and
PostgreSQL's copy of each went round again; it stopped there only because
MySQL does not log an update that changes nothing. The servers' own
replication has a way to stop that (`loops_prevented`); migkit's own tails
had none. A row both sides change is held
to what the change says it was before: `error` stops the tail and names
it, `last_update_wins` keeps the newer by the named column, and each is
written down with both versions.
"""
import ctypes
import json
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, MY_PORT = "migkit-test-twoway-my", 15938


def my(sql, db="app"):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B", db], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8.4", "--binlog-row-metadata=FULL"], check=True,
                   capture_output=True)
    try:
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


TABLE_MY = ("create table t (id int primary key, v varchar(40),"
            " updated_at datetime(6))")
TABLE_PG = ("create table public.t (id int primary key, v varchar(40),"
            " updated_at timestamp(6));"
            " alter table public.t replica identity full")


@pytest.fixture
def sides(mysql, pg_pair):
    my("drop database if exists app; create database app", db="")
    my(TABLE_MY)
    port = pg_pair["dst"]
    psql(port, "select pg_drop_replication_slot(slot_name) from"
               " pg_replication_slots where slot_name like 'migkit_%'")
    psql(port, "select pg_replication_origin_drop(roname) from"
               " pg_replication_origin where roname like"
               " 'migkit\\_twoway\\_%'")
    psql(port, "drop database if exists app")
    assert psql(port, "create database app").returncode == 0
    assert psql(port, TABLE_PG, db="app").returncode == 0
    for i in range(1, 11):
        my(f"insert into t values ({i}, 'v{i}', '2026-01-01 00:00:00')")
        psql(port, f"insert into public.t values ({i}, 'v{i}',"
                   " '2026-01-01 00:00:00')", db="app")
    yield port
    psql(port, "select pg_drop_replication_slot(slot_name) from"
               " pg_replication_slots where slot_name like 'migkit_%'")
    psql(port, "select pg_replication_origin_drop(roname) from"
               " pg_replication_origin where roname like"
               " 'migkit\\_twoway\\_%'")


def _hops(tmp_path, pg_port, **two_way):
    from migkit.engines.hetero import HeteroEngine
    mysql_ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                        password="test")
    pg_ep = Endpoint(host="127.0.0.1", port=pg_port, user="postgres",
                     password="test")
    out = []
    for name, src, dst, se, de in (
            ("m2p", mysql_ep, pg_ep, "mysql", "postgres"),
            ("p2m", pg_ep, mysql_ep, "postgres", "mysql")):
        hop = Hop(name=name, engine="hetero", source=src, target=dst,
                  databases=["app"],
                  options={"source_engine": se, "target_engine": de,
                           "two_way": dict(two_way), "server_id": 4400
                           + len(out)})
        (tmp_path / name).mkdir(exist_ok=True)
        hop.report_dir = lambda db=None, n=name: tmp_path / n
        out.append(HeteroEngine(hop))
    return out


def _run(eng, tmp_path):
    said, ended = [], {}

    def run():
        try:
            eng.tail_apply("app", True,
                           tmp_path / eng.hop.name / "tail-token.json",
                           said.append)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, said, ended


def _stop(*threads):
    for thread in threads:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
    for thread in threads:
        thread.join(timeout=30)


def _rows(pg_port):
    a = my("select id, v from t order by id")
    b = psql(pg_port, "select id || chr(9) || v from public.t order by id",
             db="app").stdout.strip()
    return a, b


def _applied(said):
    got = [int(m.split()[0]) for m in said
           if m.endswith(" changes") and m.split()[0].isdigit()]
    return got[-1] if got else 0


def _wait_equal(pg_port, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        a, b = _rows(pg_port)
        if a == b:
            return a
        time.sleep(1)
    return None


def test_each_change_reaches_the_other_side_once(sides, tmp_path):
    pg_port = sides
    m2p, p2m = _hops(tmp_path, pg_port, on_conflict="error")
    t1, said1, end1 = _run(m2p, tmp_path)
    t2, said2, end2 = _run(p2m, tmp_path)
    try:
        time.sleep(4)
        for i in range(101, 121):
            my(f"insert into t values ({i}, 'mysql {i}', now(6))")
        for i in range(201, 221):
            psql(pg_port, f"insert into public.t values ({i}, 'pg {i}',"
                          " now())", db="app")
        my("update t set v = 'mysql edit' where id = 3")
        psql(pg_port, "update public.t set v = 'pg edit' where id = 4",
             db="app")
        psql(pg_port, "delete from public.t where id = 9", db="app")
        got = _wait_equal(pg_port)
        assert got is not None, (_rows(pg_port), said1[-3:], said2[-3:],
                                 end1, end2)
        # quiet for a while: nothing goes round
        time.sleep(6)
        first = (_applied(said1), _applied(said2))
        time.sleep(4)
        assert (_applied(said1), _applied(said2)) == first, (said1, said2)
    finally:
        _stop(t1, t2)
    assert not end1 or isinstance(end1["e"], KeyboardInterrupt), end1
    assert not end2 or isinstance(end2["e"], KeyboardInterrupt), end2
    # each carried the other side's changes and nothing of its own back
    assert first == (21, 22), (first, said1, said2)
    assert "3\tmysql edit" in got and "4\tpg edit" in got
    assert "9\tv9" not in got
    # the marks are migkit's own: not the application's, not compared
    assert my("select count(*) from migkit_origin") != "0"


def test_a_row_changed_on_both_sides_stops_the_tail(sides, tmp_path):
    pg_port = sides
    m2p, p2m = _hops(tmp_path, pg_port, on_conflict="error")
    for eng in (m2p, p2m):
        eng.tail_start("app", tmp_path / eng.hop.name / "tail-token.json")
    my("update t set v = 'mysql says' where id = 7")
    psql(pg_port, "update public.t set v = 'pg says' where id = 7",
         db="app")
    t1, said1, end1 = _run(m2p, tmp_path)
    t1.join(timeout=60)
    _stop(t1)
    assert isinstance(end1.get("e"), SystemExit), (end1, said1)
    assert "update_origin_differs on public.t" in str(end1["e"]), end1["e"]
    got = [json.loads(line) for line in
           (tmp_path / "m2p" / "conflicts.jsonl").read_text().splitlines()]
    assert got[0]["local"]["v"] == "pg says"
    assert got[0]["remote"]["v"] == "mysql says"
    # nothing of the batch was applied
    assert _rows(pg_port)[1].count("7\tpg says") == 1


def test_the_later_change_wins_where_the_hop_says_so(sides, tmp_path):
    pg_port = sides
    m2p, p2m = _hops(tmp_path, pg_port, on_conflict="last_update_wins",
                     column="updated_at")
    for eng in (m2p, p2m):
        eng.tail_start("app", tmp_path / eng.hop.name / "tail-token.json")
    my("update t set v = 'older', updated_at = '2026-02-01 00:00:00'"
       " where id = 8")
    psql(pg_port, "update public.t set v = 'newer', updated_at ="
                  " '2026-03-01 00:00:00' where id = 8", db="app")
    t1, said1, end1 = _run(m2p, tmp_path)
    t2, said2, end2 = _run(p2m, tmp_path)
    try:
        got = _wait_equal(pg_port)
    finally:
        _stop(t1, t2)
    assert got is not None, (_rows(pg_port), said1, said2, end1, end2)
    assert "8\tnewer" in got, got
    kinds = [json.loads(line)["decision"] for name in ("m2p", "p2m")
             for line in ((tmp_path / name / "conflicts.jsonl").read_text()
                          .splitlines()
                          if (tmp_path / name / "conflicts.jsonl").exists()
                          else [])]
    assert sorted(kinds) == ["apply", "keep_local"], kinds
