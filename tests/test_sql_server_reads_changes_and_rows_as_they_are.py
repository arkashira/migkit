"""SQL Server: Change Tracking read the way Microsoft documents it, and a
row hash that leaves out what every copy writes anew.

Change Tracking was checked and then read, two statements with nothing
between them holding the changes: the retention's cleanup, running in
between, took them, the read returned fewer rows and no error, and the tail
went on past a change it never received. Measured on SQL Edge, with the
table's tracking cleared between the check and the read: an insert made
before it was not among the changes read, and nothing said so; the delta
check reported "0 changed rows, version advanced" over rows it never
verified. Now the check is made inside the reading transaction, before and
after - a SNAPSHOT one where the database allows it - and a read cut short
stops, named.

A `rowversion` is written anew into every row a copy inserts, so a table
copied correctly differed on every row. It is left out of the row hash and
named. Against SQL Edge, which runs on arm64.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-f0v-mssql", 16054, "CHANGE_ME-Str0ng!"


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
    subprocess.run(["docker", "run", "-d", "--name", MS, "-p",
                    f"127.0.0.1:{PORT}:1433", "-e", "ACCEPT_EULA=1", "-e",
                    f"MSSQL_SA_PASSWORD={PW}", "-e",
                    "MSSQL_MEMORY_LIMIT_MB=1024", "-e",
                    "MSSQL_TELEMETRY_ENABLED=false",
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


def _engine():
    from migkit.engines.mssql import MSSQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    return MSSQLEngine(Hop(name="f0v", engine="mssql", source=ep, target=ep,
                           databases=["shop"], db_map={"shop": "shop_new"}))


@pytest.fixture(params=["snapshot", "read committed"])
def tracked(request, server, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for db in ("shop", "shop_new"):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
    _sql("master", "alter database shop set change_tracking = on"
                   " (change_retention = 2 days, auto_cleanup = on)",
         "alter database shop set allow_snapshot_isolation "
         + ("on" if request.param == "snapshot" else "off"))
    table = "create table dbo.o (id int primary key, note nvarchar(20))"
    _sql("shop", table, "alter table dbo.o enable change_tracking")
    _sql("shop_new", table)
    return _engine(), request.param


def _before_the_read(monkeypatch, then):
    """`then(cursor)` once, just before the first statement that reads
    the changes - on whatever connection reads them."""
    from migkit.engines.mssql import MSSQLEngine
    real = MSSQLEngine._run
    fired = {}

    def run(self, cur, sql, args=None):
        if "changetable(changes" in sql and not fired:
            fired["got"] = then(cur)
        return real(self, cur, sql, args)
    monkeypatch.setattr(MSSQLEngine, "_run", run)
    return fired


def _clear_tracking(cur):
    _sql("shop", "alter table dbo.o disable change_tracking",
         "alter table dbo.o enable change_tracking")


def test_changes_cleaned_up_mid_read_are_never_lost_silently(tracked,
                                                             monkeypatch):
    eng, _ = tracked
    at = eng.change_point("src", "shop")
    _sql("shop", "insert into dbo.o values (5, N'e')")
    fired = _before_the_read(monkeypatch, _clear_tracking)
    try:
        changes, _ = eng.neutral_changes("src", "shop", at)
    except SystemExit as e:
        assert str(e).startswith("Change Tracking no longer keeps the"
                                 " changes of dbo.o"), e
    else:
        # the only other acceptable answer: the change was read after all
        assert [c["key"] for c in changes] == [{"id": 5}], changes
    assert fired, "the read never happened"


def test_the_read_is_one_snapshot_where_the_database_allows_it(tracked,
                                                               monkeypatch):
    eng, mode = tracked
    at = eng.change_point("src", "shop")
    _sql("shop", "insert into dbo.o values (6, N'f')")

    def level(cur):
        cur.execute("select transaction_isolation_level from"
                    " sys.dm_exec_sessions where session_id = @@spid")
        return cur.fetchone()[0]
    fired = _before_the_read(monkeypatch, level)
    changes, now = eng.neutral_changes("src", "shop", at)
    # 5 is SNAPSHOT, 2 READ COMMITTED
    assert fired["got"] == (5 if mode == "snapshot" else 2), fired
    assert [c["key"] for c in changes] == [{"id": 6}], changes
    assert now >= at


def test_a_delta_over_changes_the_cleanup_took_is_not_ok(tracked):
    eng, _ = tracked
    first = eng.delta_verify("shop")
    assert first[0].status == "ok" and "baseline" in first[0].detail, first
    # a row only the source has, whose change the cleanup then takes
    _sql("shop", "insert into dbo.o values (7, N'g')")
    _clear_tracking(None)
    got = eng.delta_verify("shop")
    assert got[0].status == "diff", [(r.scope, r.detail) for r in got]
    assert "no longer keeps" in got[0].detail, got[0].detail


def test_a_delta_still_finds_a_changed_row(tracked):
    eng, _ = tracked
    _sql("shop_new", "insert into dbo.o values (8, N'h')")
    _sql("shop", "insert into dbo.o values (8, N'h')")
    assert eng.delta_verify("shop")[0].status == "ok"
    _sql("shop", "update dbo.o set note = N'changed' where id = 8")
    got = eng.delta_verify("shop")
    assert got[0].status == "diff", [(r.scope, r.detail) for r in got]


@pytest.fixture
def rows(server, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for db in ("shop", "shop_new"):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
        _sql(db, "create table dbo.r (id int primary key, v nvarchar(10),"
                 " rv rowversion)")
    # the target's rowversions run ahead of the source's, as a target's do
    # after a load: the same rows, each carrying a different stamp
    _sql("shop_new", "insert into dbo.r (id, v) values (1, N'x'), (2, N'y')",
         "update dbo.r set v = v", "update dbo.r set v = v")
    _sql("shop", "insert into dbo.r (id, v) values (1, N'x'), (2, N'y')")
    stamps = [_sql(d, "select rv from dbo.r order by id")
              for d in ("shop", "shop_new")]
    assert stamps[0] != stamps[1], "the premise failed: the stamps agree"
    return _engine()


def _data(eng):
    got = [r for r in eng.check_data("shop") if r.check == "data"]
    assert len(got) == 1, got
    return got[0]


def test_a_rowversion_is_left_out_and_said(rows):
    r = _data(rows)
    assert r.status == "ok", r.detail
    assert "left out of the row hash" in r.detail \
        and "dbo.r.rv (rowversion)" in r.detail, r.detail


def test_a_row_that_differs_beside_a_rowversion_is_found(rows):
    _sql("shop_new", "update dbo.r set v = N'z' where id = 2")
    r = _data(rows)
    assert r.status == "diff", r.detail
    assert "changed=1" in r.detail, r.detail


def test_columns_in_another_order_hash_alike(server, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for db, cols in (("shop", "id int primary key, a int, b nvarchar(5)"),
                     ("shop_new", "b nvarchar(5), id int primary key, a int")):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
        _sql(db, f"create table dbo.c ({cols})",
             "insert into dbo.c (id, a, b) values (1, 2, N'x')")
    r = _data(_engine())
    assert r.status == "ok", r.detail
    _sql("shop_new", "update dbo.c set a = 3")
    assert _data(_engine()).status == "diff"
