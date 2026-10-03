"""CockroachDB as a side of a pair (backlog ER4), against one node of its
own (`cockroachdb/cockroach`, insecure, on this machine only).

Copied by key and written by UPSERT, compared row by row with its values
as the driver reads them - an interval with its months apart from its
days, a decimal at its scale, bytes as bytes. Followed by a changefeed
streamed to migkit's own query, which makes nothing on the cluster: its
changes in commit order at each resolved timestamp, each row read back
`AS OF SYSTEM TIME` that timestamp. The fixture turns the cluster's
rangefeeds on, as the DBA's script says to - migkit never does.
"""
import ctypes
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.fence", "cockroachdb"),
    ("tail.follows", "cockroachdb"),
    ("tail.table-states", "cockroachdb"),
    ("tail.added", "cockroachdb"),
    ("schema.create", "cockroachdb"),
    ("move.table-copy", "cockroachdb"),
    ("cutover.drain", "cockroachdb"),
]

pytestmark = needs_docker

CRDB, PORT = "migkit-test-cockroach", 16341


@pytest.fixture(scope="module")
def cluster():
    subprocess.run(["docker", "rm", "-f", "-v", CRDB], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", CRDB, "-p",
                    f"127.0.0.1:{PORT}:26257",
                    "cockroachdb/cockroach:latest-v25.2",
                    "start-single-node", "--insecure"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            try:
                run(["set cluster setting kv.rangefeed.enabled = true"])
                break
            except Exception:
                time.sleep(1)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", CRDB],
                       capture_output=True)


def run(statements, db="defaultdb"):
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=PORT, user="root",
                            dbname=db, sslmode="disable")
    conn.autocommit = True
    try:
        cur = conn.cursor()
        rows = []
        for s in statements:
            cur.execute(s)
            rows = cur.fetchall() if cur.description else []
        return rows
    finally:
        conn.close()


def _engine(tmp_path):
    from migkit.engines.cockroachdb import CockroachEngine

    def ep():
        return Endpoint(host="127.0.0.1", port=PORT, user="root",
                        password="", options={"sslmode": "disable"})
    hop = Hop(name="crdb", engine="cockroachdb", source=ep(), target=ep(),
              databases=["csrc"], db_map={"csrc": "cdst"})
    hop.report_dir = lambda db=None: tmp_path
    return CockroachEngine(hop)


@pytest.fixture
def shop(cluster):
    run(["drop database if exists csrc cascade",
         "drop database if exists cdst cascade",
         "create database csrc", "create database cdst"])
    run(["create table orders (id int8 primary key, price decimal(12,2),"
         " note string, raw bytes, due interval, made timestamptz,"
         " doc jsonb, rate float8, ref uuid)",
         "insert into orders values (1, 10.50, 'a', b'\\x00\\xff',"
         " '1 mon 2 days 03:00:00', '2024-01-01 00:00:00.123456+00',"
         " '{\"a\": 1.10}', 0.1, gen_random_uuid()),"
         " (2, 0.10, null, null, null, null, null, null, null)",
         "create table lines (region string, seq int8, qty int8,"
         " primary key (region, seq))",
         "insert into lines select 'r' || (g % 3)::string, g, g * 2 from"
         " generate_series(1, 50) g"], db="csrc")
    return "csrc"


def _move(eng, tmp_path):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing("csrc", lambda m: None)
    for sch, t in eng.list_move_tables("csrc"):
        eng.move_table("csrc", sch, t, 20, ck, lambda m: None)


def _data(eng):
    return {r.scope: r.status for r in eng.check_data("csrc")
            if r.check == "data"}


def test_tables_copied_by_upsert_and_compared_value_for_value(shop,
                                                              tmp_path):
    eng = _engine(tmp_path)
    _move(eng, tmp_path)
    got = _data(eng)
    assert len(got) == 2 and set(got.values()) == {"ok"}, got
    # a month is not thirty days: read as its text, the difference shows
    run(["update orders set due = '32 days 03:00:00' where id = 1"],
        db="cdst")
    got = _data(_engine(tmp_path))
    assert "diff" in got.values(), got


def test_a_row_is_read_back_as_it_stood_at_the_resolved_timestamp(
        shop, tmp_path):
    from migkit.changetopic import Row
    eng = _engine(tmp_path)
    at = eng._now("src", "csrc")
    run(["update orders set note = 'later' where id = 1"], db="csrc")
    (r, key, values), = eng._read_back(
        "csrc", "public.orders", [Row(None, "public.orders", "upsert",
                                      {"0": 1}, None, at)], at)
    assert key == {"id": 1} and values["note"] == "a"
    with pytest.raises(SystemExit) as e:
        eng._read_back("csrc", "public.orders",
                       [Row(None, "public.orders", "upsert", {"0": 99},
                            None, at)], at)
    assert "is not the key here" in str(e.value)


def _tail(eng, tmp_path):
    ended = {}

    def go():
        try:
            eng.tail_apply("csrc", True, tmp_path / "tail-token.json",
                           lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=go, daemon=True)
    thread.start()
    return thread, ended


def _stop(thread):
    if thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    thread.join(timeout=30)


def test_a_copy_and_its_changefeed_end_equal(shop, tmp_path):
    eng = _engine(tmp_path)
    eng.tail_start("csrc", tmp_path / "tail-token.json")
    run(["update orders set price = 11.00 where id = 1"], db="csrc")
    _move(eng, tmp_path)
    thread, ended = _tail(eng, tmp_path)
    try:
        run(["insert into orders (id, price) values (3, 1.25)",
             "update orders set id = 20 where id = 2",
             "delete from lines where seq < 5",
             "update orders set raw = b'\\x01', due = '1 year' where"
             " id = 3"], db="csrc")
        time.sleep(2)
        at = eng.src_lsn("csrc")
        assert at is not None
        assert eng.fence_wait("csrc", at, timeout=120) is True
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    got = _data(_engine(tmp_path))
    assert set(got.values()) == {"ok"}, got
    ids = run(["select id from orders order by id"], db="cdst")
    assert [r[0] for r in ids] == [1, 3, 20]


def test_a_table_without_a_key_is_copied_and_not_followed(shop, tmp_path):
    run(["create table bare (v int8)", "insert into bare values (1), (1)"],
        db="csrc")
    eng = _engine(tmp_path)
    _move(eng, tmp_path)
    got = _data(eng)
    assert got.get("csrc.public.bare", got.get("csrc.bare")) == "ok", got
    with pytest.raises(SystemExit) as e:
        _engine(tmp_path).change_point("src", "csrc")
    assert "bare" in str(e.value) and "primary key" in str(e.value)


def test_a_table_that_joins_the_tail_is_followed_after_its_copy(shop,
                                                                tmp_path):
    """The changefeed names its tables when it is opened: a table made on
    the source while the tail ran was copied (`ddl: park`) and then never
    followed - its changes after the copy went nowhere. The feed is let go
    and opened again over the tables the tail takes now
    (`FOLLOWS_LISTED_TABLES`)."""
    from migkit.engines.cockroachdb import CockroachEngine
    eng = _engine(tmp_path)
    eng.hop.options["ddl"] = "park"
    eng.tail_start("csrc", tmp_path / "tail-token.json")
    _move(eng, tmp_path)
    thread, ended = _tail(eng, tmp_path)
    try:
        run(["create table joined (id int8 primary key, v string)",
             "insert into joined values (1, 'copied')"], db="csrc")
        end = time.time() + 120
        while time.time() < end:
            try:
                if run(["select count(*) from joined"], db="cdst")[0][0]:
                    break
            except Exception:  # noqa: BLE001 - not made there yet
                pass
            time.sleep(1)
        run(["insert into joined values (2, 'followed')",
             "update joined set v = 'changed' where id = 1"], db="csrc")
        time.sleep(2)
        at = eng.src_lsn("csrc")
        assert eng.fence_wait("csrc", at, timeout=120) is True
        end = time.time() + 60
        while time.time() < end and run(
                ["select count(*) from joined"], db="cdst")[0][0] < 2:
            time.sleep(1)
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    assert CockroachEngine.FOLLOWS_LISTED_TABLES
    got = run(["select id, v from joined order by id"], db="cdst")
    assert [tuple(r) for r in got] == [(1, "changed"), (2, "followed")]
