"""A SQL Server hop keeps its target following the source through Change
Tracking, and fences on it (backlog 0e).

Change Tracking is SQL Server's record of which rows changed since a
version: the key and the last operation of each row, not every change.
The feed reads each changed row as it is now, which a tail applied by key
converges on; a row that is gone is a delete. Measured against Azure SQL
Edge (SQL Server's engine, the build that runs on arm64): an insert, an
update of it and a delete of another row came back as the inserted row
with its updated value, and a delete.

What a tail cannot do is carry a table the database does not track, or
changes the tracking has already cleaned up: both stop it, named, rather
than leave rows that never arrive. A change that carries only some
columns updates those and keeps the rest.
"""
import ctypes
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-mssql-ct", 15853, "CHANGE_ME-Str0ng!"


def _sql(db, *statements):
    import pymssql
    conn = pymssql.connect(server="127.0.0.1", port=str(PORT), user="sa",
                           password=PW, database=db, autocommit=True,
                           tds_version="7.4", login_timeout=10)
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = cur.fetchall() if cur.description else None
        return out
    finally:
        conn.close()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MS, "-p", f"{PORT}:1433",
                    "-e", "ACCEPT_EULA=1", "-e", f"MSSQL_SA_PASSWORD={PW}",
                    "-e", "MSSQL_MEMORY_LIMIT_MB=1024",
                    "-e", "MSSQL_TELEMETRY_ENABLED=false",
                    "mcr.microsoft.com/azure-sql-edge:latest"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                up = s.connect_ex(("127.0.0.1", PORT)) == 0
            if up:
                try:
                    _sql("master", "select 1")
                    break
                except Exception:
                    pass
            time.sleep(2)
        else:
            pytest.fail("SQL Server never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)


@pytest.fixture
def pair(server, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for db in ("shop", "shop_new"):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
    _sql("master", "alter database shop set change_tracking = on"
                   " (change_retention = 2 days, auto_cleanup = on)")
    table = ("create table dbo.o (id int primary key, region nvarchar(10),"
             " amount decimal(8,2), note nvarchar(max))")
    _sql("shop", table, "alter table dbo.o enable change_tracking")
    _sql("shop_new", table)
    from migkit.engines.mssql import MSSQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    yield MSSQLEngine(Hop(name="ms", engine="mssql", source=ep, target=ep,
                          databases=["shop"], db_map={"shop": "shop_new"}))


def _following(eng, write, check=None):
    """The tail running while `write` changes the source; `check` is asked
    while it still runs."""
    token = eng.hop.report_dir("shop") / "tail-token.json"
    said, ended, got = [], {}, {}

    def run():
        try:
            eng.tail_apply("shop", True, token, said.append)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(4)
    try:
        write()
        for _ in range(60):
            if any(m.endswith("changes") for m in said) or ended:
                break
            time.sleep(0.5)
        if check:
            got["check"] = check()
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    return said, ended, got


def test_the_target_follows_and_the_fence_passes(pair):
    def write():
        _sql("shop", "insert into dbo.o values (1, 'eu', 1.50, 'a'),"
                     " (2, 'us', 2.00, 'b')",
             "update dbo.o set amount = 9.25 where id = 1",
             "delete from dbo.o where id = 2")

    def fenced():
        _sql("shop", "insert into dbo.o values (3, 'ap', 3.00, 'c')")
        at = pair.src_lsn("shop")
        return at, pair.fence_wait("shop", at, timeout=60)
    said, ended, got = _following(pair, write, fenced)
    assert not ended, ended
    at, passed = got["check"]
    assert at is not None and passed is True, got
    assert _sql("shop_new", "select id, region, amount, note from dbo.o"
                            " order by id") == \
        _sql("shop", "select id, region, amount, note from dbo.o order by id")
    assert [r[0] for r in _sql("shop_new", "select id from dbo.o"
                                           " order by id")] == [1, 3]


def test_a_table_it_does_not_track_stops_the_tail(pair):
    _sql("shop", "create table dbo.audit (id int primary key, at datetime2)")
    with pytest.raises(SystemExit) as e:
        pair.tail_start("shop", pair.hop.report_dir("shop")
                        / "tail-token.json")
    assert str(e.value).startswith("Change Tracking does not follow"
                                   " dbo.audit"), e.value


def test_changes_the_tracking_no_longer_keeps_stop_it(pair):
    at = pair.change_point("src", "shop")
    _sql("shop", "insert into dbo.o values (5, 'eu', 5.00, 'e')")
    # tracking taken off and on again keeps nothing from before
    _sql("shop", "alter table dbo.o disable change_tracking",
         "alter table dbo.o enable change_tracking")
    with pytest.raises(SystemExit) as e:
        pair.neutral_changes("src", "shop", at)
    assert str(e.value).startswith("Change Tracking no longer keeps the"
                                   " changes of dbo.o from version"), e.value


def test_a_change_of_some_columns_keeps_the_others(pair):
    _sql("shop_new", "insert into dbo.o values (7, 'eu', 7.00, 'kept')")
    pair.neutral_apply("dst", "shop", [
        {"op": "update", "table": "dbo.o", "key": {"id": 7},
         "values": {"id": 7, "amount": 7.50}},
        {"op": "insert", "table": "dbo.o", "key": {"id": 8},
         "values": {"id": 8, "region": "us", "amount": 8.00,
                    "note": "new"}}])
    # replayed, as a restarted tail replays
    pair.neutral_apply("dst", "shop", [
        {"op": "update", "table": "dbo.o", "key": {"id": 7},
         "values": {"id": 7, "amount": 7.50}}])
    rows = _sql("shop_new", "select id, region, cast(amount as varchar(10)),"
                            " note from dbo.o order by id")
    assert rows == [(7, "eu", "7.50", "kept"), (8, "us", "8.00", "new")]
