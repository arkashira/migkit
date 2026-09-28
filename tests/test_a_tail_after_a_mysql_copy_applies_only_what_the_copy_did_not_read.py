"""The same on MySQL (backlog R19, lever 8): the table copier's ranges read
side by side in processes of their own, each after the GTIDs the server
had executed, and the tail after the copy leaving out the changes whose
GTID was among them - the target ends equal to the source.

A server with GTIDs on (`gtid_mode` ON), as RDS and Aurora run with them
where replicas are used; with GTIDs off nothing is left out, and that is
the tail as it was.
"""
import json
import random
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MY, PORT = "migkit-test-wm-my", 15980
ROWS = 60_000


def my(sql, db=""):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"] + ([db] if db else []),
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-p",
                    f"127.0.0.1:{PORT}:3306", "-e",
                    "MYSQL_ROOT_PASSWORD=test", "mysql:8.4",
                    "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                    "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _writer(stop, done):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=PORT, user="root",
                           password="test", database="shop", autocommit=True)
    rnd, fresh = random.Random(8), ROWS
    with conn.cursor() as cur:
        while not stop.is_set():
            r = rnd.random()
            if r < 0.8:
                cur.execute("update big set n = n + 1, payload = md5(rand())"
                            " where id = %s", (rnd.randint(1, ROWS),))
            elif r < 0.9:
                cur.execute("delete from big where id = %s",
                            (rnd.randint(1, ROWS),))
            else:
                fresh += 1
                cur.execute("insert into big values (%s, 0, 'new')",
                            (fresh,))
            done[0] += 1
    conn.close()


def _same():
    q = ("set session group_concat_max_len = 1000000000;"
         " select count(*), md5(group_concat(concat_ws('|', id, n, payload)"
         " order by id separator ',')) from big;")
    return my(q, "shop"), my(q, "shop_copy")


@pytest.mark.parametrize("fresh", [False, True],
                         ids=["held open", "started again each time"])
def test_every_change_carries_its_transactions_gtid(server, fresh):
    """A transaction larger than a read carries its GTID on every change,
    whether the reader is held open between reads or started again at
    the transaction's start; and the server's executed set taken between
    two transactions covers the first and not the second."""
    from migkit.engines.mysql import MySQLEngine
    my("drop database if exists gt; create database gt;"
       " create table gt.t (id bigint primary key, v varchar(10))")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")

    def eng():
        return MySQLEngine(Hop(name="wm8gt", engine="mysql", source=ep,
                               target=ep, databases=["gt"]))
    reader = eng()
    token = reader.change_point("src", "gt")
    my("set session cte_max_recursion_depth = 100000; insert into gt.t"
       " with recursive g(n) as (select 1 union all select n + 1 from g"
       " where n < 3000) select n, 'a' from g")
    between = reader.snapshot_mark("src", "gt")
    my("update gt.t set v = 'b' where id = 1")
    got = []
    while True:
        if fresh:
            reader = eng()
        batch, after = reader.neutral_changes("src", "gt", token, limit=700)
        got += batch
        if not batch and after == token:
            break
        token = after
    assert len(got) == 3001, len(got)
    first = {c["txn"] for c in got[:3000]}
    assert len(first) == 1 and None not in first, first
    assert got[-1]["txn"] not in first and got[-1]["txn"]
    covers = MySQLEngine.mark_covers
    assert covers("src", "gt", between, got[0]) is True
    assert covers("src", "gt", between, got[-1]) is False
    now = reader.snapshot_mark("src", "gt")
    assert covers("src", "gt", now, got[-1]) is True


def test_the_tail_leaves_out_what_the_mysql_copy_read(server, tmp_path,
                                                      monkeypatch):
    import ctypes

    from migkit import ranges
    from migkit.cli import _Checkpoint
    from migkit.engines.mysql import MySQLEngine
    table = "create table big (id bigint primary key, n int, payload" \
            " varchar(64))"
    my("drop database if exists shop; drop database if exists shop_copy;"
       " create database shop; create database shop_copy;")
    my(f"{table}; set session cte_max_recursion_depth = 1000000;"
       " insert into big with recursive g(x) as (select 1 union all select"
       f" x + 1 from g where x < {ROWS}) select x, 0, md5(x) from g;"
       " analyze table big", "shop")
    my(table, "shop_copy")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="wm8my", engine="mysql", source=ep, target=ep,
              databases=["shop"], db_map={"shop": "shop_copy"}, workers=4)
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(ranges, "LEAST", 5_000)
    monkeypatch.setattr(ranges, "active", ranges.Slots(2))
    eng = MySQLEngine(hop)
    pair = eng._as_pair()
    token_path = tmp_path / "tail-token.json"
    assert pair.tail_start("shop", token_path)
    stop, done = threading.Event(), [0]
    writer = threading.Thread(target=_writer, args=(stop, done), daemon=True)
    writer.start()
    ck = _Checkpoint(tmp_path / "move.json")
    try:
        eng.move_table("shop", "", "big", 500_000, ck, lambda m: None)
    finally:
        stop.set()
        writer.join(timeout=60)
    st = ck["shop.big"]
    assert len(st["ranges"]) >= 4 and st["done"], st
    assert all(st["ranges_seen"].values()), st["ranges_seen"]
    end = pair.src_engine.change_point("src", "shop")
    lines, finished = [], threading.Event()

    def run():
        try:
            pair.tail_apply("shop", True, token_path, lines.append)
        except BaseException as e:  # noqa: BLE001 - KeyboardInterrupt
            lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            finished.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    while not finished.is_set():
        try:
            at = json.loads(token_path.read_text()).get("token")
        except (OSError, ValueError):
            at = None
        if at and MySQLEngine.position_reached(at, end):
            break
        time.sleep(0.1)
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    finished.wait(timeout=120)
    src, dst = _same()
    assert src == dst, (src, dst, lines[-5:])
    got = json.loads((tmp_path / "tail-accounts.json").read_text())["big"]
    left = got.get("already in what the copy read", 0)
    assert got["read"] == got["applied"] + left, got
    assert left > 0, (got, lines[-5:])
    print(f"\n{done[0]:,} writes during the copy; the tail read"
          f" {got['read']:,}, applied {got['applied']:,}, left out {left:,}")
