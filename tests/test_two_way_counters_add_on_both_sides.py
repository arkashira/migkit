"""Counters under two ways (backlog R3): what each side added is kept.

A balance both sides move at once is no conflict to decide - both
additions happened. Measured before, MySQL and PostgreSQL both ways with
`on_conflict: error`, a balance of 100 moved +10 on MySQL and +5 on
PostgreSQL: the tail stopped on `update_origin_differs`; under
`apply_remote` each side took the other's value: MySQL ended 105 and
PostgreSQL 110. A hop's `two_way.delta` names the counters: a change to one is
applied as what it added, where the target's row stands (`n = n + by`),
so both end 115.

Added twice would be wrong, which a row's final state never is: a batch
the target committed and whose position was not yet saved here has to be
known as applied. Each batch of such a hop is one transaction whose mark
says which batch it was; the tail going on after a stop there asks the
target and goes on after it.

`source_priority` decides the rest of a row by rank, and a counter still
adds where the target's row is kept.
"""
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, MY_PORT = "migkit-test-counters-my", 15951


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


@pytest.fixture
def sides(mysql, pg_pair):
    my("drop database if exists app; create database app", db="")
    my("create table acct (id int primary key, balance int, note"
       " varchar(20))")
    port = pg_pair["dst"]
    psql(port, "select pg_drop_replication_slot(slot_name) from"
               " pg_replication_slots where slot_name like 'migkit_%'")
    psql(port, "select pg_replication_origin_drop(roname) from"
               " pg_replication_origin where roname like"
               " 'migkit\\_twoway\\_%'")
    psql(port, "drop database if exists app")
    assert psql(port, "create database app").returncode == 0
    assert psql(port, "create table public.acct (id int primary key,"
                      " balance int, note varchar(20)); alter table"
                      " public.acct replica identity full",
                db="app").returncode == 0
    for i in range(1, 4):
        my(f"insert into acct values ({i}, 100, 'n{i}')")
        psql(port, f"insert into public.acct values ({i}, 100, 'n{i}')",
             db="app")
    yield port
    psql(port, "select pg_drop_replication_slot(slot_name) from"
               " pg_replication_slots where slot_name like 'migkit_%'")
    psql(port, "select pg_replication_origin_drop(roname) from"
               " pg_replication_origin where roname like"
               " 'migkit\\_twoway\\_%'")


def _hops(tmp_path, pg_port, ranks=None, **two_way):
    from migkit.engines.hetero import HeteroEngine
    mysql_ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                        password="test")
    pg_ep = Endpoint(host="127.0.0.1", port=pg_port, user="postgres",
                     password="test")
    out = []
    for i, (name, src, dst, se, de) in enumerate((
            ("m2p", mysql_ep, pg_ep, "mysql", "postgres"),
            ("p2m", pg_ep, mysql_ep, "postgres", "mysql"))):
        tw = dict(two_way)
        if ranks:
            tw["source_rank"], tw["target_rank"] = ranks[i]
        hop = Hop(name=name, engine="hetero", source=src, target=dst,
                  databases=["app"],
                  options={"source_engine": se, "target_engine": de,
                           "two_way": tw, "server_id": 4500 + i})
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
    import ctypes
    for thread in threads:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
    for thread in threads:
        thread.join(timeout=30)


def _rows(pg_port):
    a = my("select id, balance, note from acct order by id")
    b = psql(pg_port, "select id || chr(9) || balance || chr(9) || note"
                      " from public.acct order by id",
             db="app").stdout.strip()
    return a, b


def _wait(pg_port, want, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        got = _rows(pg_port)
        if got == (want, want):
            return got
        time.sleep(1)
    return _rows(pg_port)


def test_both_sides_additions_are_kept(sides, tmp_path):
    pg_port = sides
    m2p, p2m = _hops(tmp_path, pg_port, on_conflict="error",
                     delta=["balance"])
    for eng in (m2p, p2m):
        eng.tail_start("app", tmp_path / eng.hop.name / "tail-token.json")
    my("update acct set balance = balance + 10 where id = 1")
    psql(pg_port, "update public.acct set balance = balance + 5 where"
                  " id = 1", db="app")
    my("update acct set balance = balance + 1 where id = 2")
    t1, said1, end1 = _run(m2p, tmp_path)
    t2, said2, end2 = _run(p2m, tmp_path)
    try:
        want = "1\t115\tn1\n2\t101\tn2\n3\t100\tn3"
        got = _wait(pg_port, want)
    finally:
        _stop(t1, t2)
    assert got == (want, want), (got, said1[-3:], said2[-3:], end1, end2)
    assert not end1 or isinstance(end1["e"], KeyboardInterrupt), end1
    assert not end2 or isinstance(end2["e"], KeyboardInterrupt), end2


def test_a_batch_committed_before_its_position_was_saved_is_not_added_again(
        sides, tmp_path, monkeypatch):
    pg_port = sides
    m2p, _ = _hops(tmp_path, pg_port, on_conflict="error",
                   delta=["balance"])
    token = tmp_path / "m2p" / "tail-token.json"
    m2p.tail_start("app", token)
    my("update acct set balance = balance + 10 where id = 3")
    # the batch committed on the target, then the tail stopped before its
    # position was saved here - as a kill between the two would
    monkeypatch.setenv("MIGKIT_FAILPOINT", "tail.applied:1:raise")
    from migkit import failpoint
    failpoint._counts.clear()
    said = []
    with pytest.raises(RuntimeError, match="failpoint tail.applied"):
        m2p.tail_apply("app", True, token, said.append)
    assert _rows(pg_port)[1].splitlines()[2] == "3\t110\tn3"
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    t1, said1, end1 = _run(m2p, tmp_path)
    try:
        time.sleep(8)
    finally:
        _stop(t1)
    assert _rows(pg_port)[1].splitlines()[2] == "3\t110\tn3", (said1, end1)
    assert any("had committed batch" in m for m in said1), said1


def test_the_side_ranked_higher_decides_the_rest_and_counters_still_add(
        sides, tmp_path):
    pg_port = sides
    m2p, p2m = _hops(tmp_path, pg_port, ranks=[(2, 1), (1, 2)],
                     on_conflict="source_priority", delta=["balance"])
    for eng in (m2p, p2m):
        eng.tail_start("app", tmp_path / eng.hop.name / "tail-token.json")
    my("update acct set balance = balance + 10, note = 'mysql' where id = 1")
    psql(pg_port, "update public.acct set balance = balance + 5, note ="
                  " 'pg' where id = 1", db="app")
    t1, said1, end1 = _run(m2p, tmp_path)
    t2, said2, end2 = _run(p2m, tmp_path)
    try:
        want = "1\t115\tmysql\n2\t100\tn2\n3\t100\tn3"
        got = _wait(pg_port, want)
    finally:
        _stop(t1, t2)
    assert got == (want, want), (got, said1[-3:], said2[-3:], end1, end2)


def test_a_counter_that_is_not_a_number_is_refused(tmp_path):
    from migkit import twoway

    class Dst:
        CANON_ENGINE = "postgres"

        def neutral_columns(self, side, db, table):
            return [("id", "integer"), ("note", "text")]

    class Pair:
        hop = Hop(name="x", engine="hetero",
                  source=Endpoint(host="h", port=1),
                  target=Endpoint(host="h", port=2), databases=["d"],
                  options={"two_way": {"delta": ["note"]}})
        dst_engine = Dst()
    with pytest.raises(SystemExit, match="note is text, not a number"):
        twoway.resolve(Pair(), "d", [{"op": "update", "table": "t",
                                      "key": {"id": 1},
                                      "values": {"note": "a"}}], print)


def test_source_priority_needs_both_ranks():
    from migkit import twoway
    hop = Hop(name="x", engine="hetero", source=Endpoint(host="h", port=1),
              target=Endpoint(host="h", port=2), databases=["d"],
              options={"two_way": {"on_conflict": "source_priority",
                                   "source_rank": 1}})
    with pytest.raises(SystemExit, match="source_rank and"):
        twoway.policy(hop)


def test_additions_to_one_counter_in_a_batch_are_one_addition():
    from migkit import canon
    from migkit.engines.base import Engine
    got = Engine._collapsed([
        {"op": "update", "table": "t", "key": {"id": 1},
         "values": {"id": 1, "n": canon.Added(10, 110), "s": "a"}},
        {"op": "update", "table": "t", "key": {"id": 1},
         "values": {"id": 1, "n": canon.Added(5, 115), "s": "b"}},
        {"op": "insert", "table": "t", "key": {"id": 2},
         "values": {"id": 2, "n": 7}},
        {"op": "update", "table": "t", "key": {"id": 2},
         "values": {"id": 2, "n": canon.Added(3, 10)}},
    ])
    assert got[0][1][2] == {"id": 1, "n": canon.Added(15, 115), "s": "b"}
    assert got[1][1][2] == {"id": 2, "n": 10}


def test_nothing_changes_for_a_hop_without_counters():
    """Without `delta`, batches are not numbered and lanes stay open."""
    from migkit import twoway
    hop = Hop(name="x", engine="hetero", source=Endpoint(host="h", port=1),
              target=Endpoint(host="h", port=2), databases=["d"],
              options={"two_way": {"on_conflict": "error"}})
    assert not twoway.exact(hop)
    assert twoway.thread_origin(hop) != "x"
