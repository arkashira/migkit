"""A row both sides changed, settled column by column between a real
MySQL and a real PostgreSQL, both tails running (backlog P0-DEPTH 2).

What the in-memory sides cannot show: the statements. A column only the
target changed is kept where its row stands (`canon.Kept`) rather than
left out of the upsert, which both servers would refuse on a NOT NULL
column before finding the row; a counter is moved where it stands; a row
one side deleted and the other changed comes back whole; a parked row
waits while every other row goes on, and goes on once decided on both
sides.
"""
import ctypes
import json
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MY, MY_PORT = "migkit-test-cdr-my", 16390
PG, PG_PORT = "migkit-test-cdr-pg", 16391

TABLE_MY = ("create table acct (id int primary key, note varchar(40) not"
            " null, owner varchar(40) not null, balance int not null,"
            " updated_at datetime(6) not null)")
TABLE_PG = ("create table public.acct (id int primary key, note"
            " varchar(40) not null, owner varchar(40) not null, balance int"
            " not null, updated_at timestamp(6) not null);"
            " alter table public.acct replica identity full")


def my(sql, db="app"):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B", db], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def pg(sql, db="app"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG,
                          "psql", "-U", "postgres", "-d", db, "-At", "-c",
                          sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(name, run, ready):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(run, check=True, capture_output=True)
    for _ in range(90):
        if subprocess.run(ready, capture_output=True).returncode == 0:
            return
        time.sleep(2)
    raise AssertionError(f"{name} did not come up")


@pytest.fixture(scope="module")
def servers():
    try:
        _up(MY, ["docker", "run", "-d", "--name", MY, "-e",
                 "MYSQL_ROOT_PASSWORD=test", "-p",
                 f"127.0.0.1:{MY_PORT}:3306", "mysql:8.4",
                 "--binlog-row-metadata=FULL"],
            ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
             "-h127.0.0.1", "--protocol=tcp", "-e", "select 1"])
        _up(PG, ["docker", "run", "-d", "--name", PG, "-e",
                 "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{PG_PORT}:5432",
                 "postgres:16", "-c", "wal_level=logical"],
            ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
             "postgres", "-h", "127.0.0.1", "-c", "select 1"])
        yield
    finally:
        for name in (MY, PG):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def sides(servers):
    my("drop database if exists app; create database app", db="")
    my(TABLE_MY)
    pg("select pg_drop_replication_slot(slot_name) from"
       " pg_replication_slots where slot_name like 'migkit_%'",
       db="postgres")
    pg("select pg_replication_origin_drop(roname) from"
       " pg_replication_origin where roname like 'migkit\\_twoway\\_%'",
       db="postgres")
    pg("drop database if exists app", db="postgres")
    pg("create database app", db="postgres")
    pg(TABLE_PG)
    for i in range(1, 5):
        row = f"({i}, 'n{i}', 'o{i}', 100, '2026-01-01 00:00:00')"
        my(f"insert into acct values {row}")
        pg(f"insert into public.acct values {row}")
    yield


def _hops(tmp_path, **two_way):
    from migkit.engines.hetero import HeteroEngine
    mysql_ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                        password="test")
    pg_ep = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                     password="test")
    out = []
    for i, (name, src, dst, se, de) in enumerate((
            ("m2p", mysql_ep, pg_ep, "mysql", "postgres"),
            ("p2m", pg_ep, mysql_ep, "postgres", "mysql"))):
        tw = json.loads(json.dumps(two_way))
        if i and isinstance(tw.get("on_conflict"), str):
            tw["on_conflict"] = {"overwrite": "ignore",
                                 "ignore": "overwrite"}.get(
                tw["on_conflict"], tw["on_conflict"])
        hop = Hop(name=name, engine="hetero", source=src, target=dst,
                  databases=["app"],
                  options={"source_engine": se, "target_engine": de,
                           "two_way": tw, "server_id": 4660 + i})
        (tmp_path / name).mkdir(exist_ok=True)

        def report_dir(db=None, n=name):
            return tmp_path / n
        hop.report_dir = report_dir
        eng = HeteroEngine(hop)
        eng.tail_start("app", tmp_path / name / "tail-token.json")
        out.append(eng)
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


def _rows():
    a = my("select id, note, owner, balance from acct order by id")
    b = pg("select id || chr(9) || note || chr(9) || owner || chr(9) ||"
           " balance from public.acct order by id")
    return a, b


def _wait(want, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        got = _rows()
        if want(got):
            return got
        time.sleep(1)
    return _rows()


def _records(tmp_path, name):
    path = tmp_path / name / "conflicts.jsonl"
    return [json.loads(x) for x in path.read_text().splitlines()] \
        if path.exists() else []


def test_columns_each_side_changed_are_both_kept(sides, tmp_path):
    """Under `error`: no column changed on both sides, so nothing stops,
    and NOT NULL columns the change keeps where they stand do not refuse
    the upsert."""
    m2p, p2m = _hops(tmp_path, on_conflict="error")
    my("update acct set note = 'from mysql' where id = 1")
    pg("update public.acct set owner = 'from pg' where id = 1")
    t1, said1, end1 = _run(m2p, tmp_path)
    t2, said2, end2 = _run(p2m, tmp_path)
    try:
        got = _wait(lambda r: r[0] == r[1]
                    and "1\tfrom mysql\tfrom pg\t100" in r[0])
    finally:
        _stop(t1, t2)
    assert got[0] == got[1], (got, said1[-3:], said2[-3:], end1, end2)
    assert "1\tfrom mysql\tfrom pg\t100" in got[0], got
    assert not end1 or isinstance(end1["e"], KeyboardInterrupt), end1
    assert not end2 or isinstance(end2["e"], KeyboardInterrupt), end2
    assert {(r["kind"], r["decision"])
            for r in _records(tmp_path, "m2p")} == {
        ("update_origin_differs", "merged")}


def test_each_column_both_changed_is_settled_by_its_rule(sides, tmp_path):
    """The newer note, both sides' additions to the balance, and a row
    deleted on MySQL while PostgreSQL changed it kept whole on both."""
    m2p, p2m = _hops(tmp_path, column="updated_at", on_conflict="newest",
                     tables={"acct": {"columns": {"balance": "delta"}}})
    my("update acct set note = 'mysql', balance = balance + 10,"
       " updated_at = '2026-02-01 00:00:00' where id = 1")
    pg("update public.acct set note = 'pg', balance = balance + 5,"
       " updated_at = '2026-03-01 00:00:00' where id = 1")
    my("delete from acct where id = 2")
    pg("update public.acct set owner = 'still here', updated_at ="
       " '2026-03-01 00:00:00' where id = 2")
    t1, said1, end1 = _run(m2p, tmp_path)
    t2, said2, end2 = _run(p2m, tmp_path)
    want = ["1\tpg\to1\t115", "2\tn2\tstill here\t100"]
    try:
        got = _wait(lambda r: r[0] == r[1]
                    and all(w in r[0] for w in want))
    finally:
        _stop(t1, t2)
    assert got[0] == got[1], (got, said1[-3:], said2[-3:], end1, end2)
    for w in want:
        assert w in got[0], (w, got)
    kinds = {r["kind"] for name in ("m2p", "p2m")
             for r in _records(tmp_path, name)}
    assert {"update_origin_differs", "delete_origin_differs",
            "update_missing"} <= kinds, kinds


def test_a_parked_row_waits_until_decided_on_both_sides(sides, tmp_path):
    m2p, p2m = _hops(tmp_path, on_conflict="park")
    my("update acct set note = 'mysql says' where id = 3")
    pg("update public.acct set note = 'pg says' where id = 3")
    my("update acct set owner = 'flows' where id = 4")
    t1, said1, end1 = _run(m2p, tmp_path)
    t2, said2, end2 = _run(p2m, tmp_path)
    try:
        got = _wait(lambda r: "4\tn4\tflows\t100" in r[1])
        # every other row goes on; the parked one is each side's own
        assert "4\tn4\tflows\t100" in got[1], got
        assert "3\tmysql says" in got[0] and "3\tpg says" in got[1], got
        parked = json.loads((tmp_path / "m2p" / "conflicts-parked.json")
                            .read_text())["rows"]
        assert sum(len(v) for v in parked.values()) == 1, parked
        # decided on both sides; the next batch lets it go
        my("update acct set note = 'decided' where id = 3")
        pg("update public.acct set note = 'decided' where id = 3")
        my("update acct set balance = 101 where id = 4")
        pg("update public.acct set balance = 102 where id = 1")
        _wait(lambda r: "4\tn4\tflows\t101" in r[1]
              and "1\tn1\to1\t102" in r[0])
        my("update acct set owner = 'after' where id = 3")
        got = _wait(lambda r: "3\tdecided\tafter" in r[1])
    finally:
        _stop(t1, t2)
    assert "3\tdecided\tafter" in got[1], (got, said1[-5:], said2[-5:],
                                           end1, end2)
    assert any("its changes go on" in m for m in said1 + said2), (said1,
                                                                  said2)
