"""SQL Server's check sums a hash of each row as the server writes the row
out (`FOR JSON`), not `BINARY_CHECKSUM(*)`.

`BINARY_CHECKSUM` leaves out every column of a type it cannot compare -
xml, text, ntext, image - and is a 32-bit sum of shifted bytes that
Microsoft itself says can miss a change. Measured before: a table whose
xml column differed on every row passed as equal, counts and checksum
alike, and so did one whose `text` column did. The drilldown already
hashed each row's `FOR JSON`; the checksum now sums the same hash, so the
two cannot disagree. And its logins and users cross with their password
hashes and SIDs. Against SQL Edge, which runs on arm64.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-ms-sums", 15943, "Migkit-Test-9x!"


def _sql(db, *statements):
    import pymssql
    conn = pymssql.connect(server="127.0.0.1", port=PORT, user="sa",
                           password=PW, database=db, autocommit=True)
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
def eng(server, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for db in ("a", "b"):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
    from migkit.engines.mssql import MSSQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    return MSSQLEngine(Hop(name="sums", engine="mssql", source=ep,
                           target=ep, databases=["a"], db_map={"a": "b"}))


@pytest.mark.parametrize("kind, a, b", [
    ("xml", "'<v>1</v>'", "'<v>2</v>'"),
    ("text", "'one'", "'two'"),
])
def test_a_column_the_old_checksum_skipped_is_compared(eng, kind, a, b):
    for db, v in (("a", a), ("b", b)):
        _sql(db, f"create table dbo.t (id int primary key, doc {kind})",
             f"insert into dbo.t values (1, {v}), (2, {v})")
    got = [r for r in eng.check_data("a") if r.check == "data"]
    assert "diff" in [r.status for r in got], [r.detail for r in got]
    # a difference found, not a statement the server refused
    assert not any("refused" in r.detail for r in got), got


def test_equal_tables_still_pass(eng):
    for db in ("a", "b"):
        _sql(db, "create table dbo.t (id int primary key, doc xml,"
                 " n decimal(10,2), s nvarchar(20), b varbinary(8))",
             "insert into dbo.t values (1, '<v>1</v>', 1.50, N'café',"
             " 0x00ff), (2, null, null, null, null)")
    got = [r for r in eng.check_data("a") if r.check == "data"]
    assert got and all(r.status == "ok" for r in got), [r.detail
                                                        for r in got]


def test_a_table_with_no_key_is_compared_as_a_whole(eng):
    """Rows there twice count twice: one row twice against another row
    twice differs, which a fold that cancels would miss."""
    _sql("a", "create table dbo.k (v int, w nvarchar(5))",
         "insert into dbo.k values (1, N'a'), (1, N'a')")
    _sql("b", "create table dbo.k (v int, w nvarchar(5))",
         "insert into dbo.k values (2, N'b'), (2, N'b')")
    got = [r for r in eng.check_data("a") if r.check == "data"]
    assert "diff" in [r.status for r in got], [r.detail for r in got]
    assert not any("refused" in r.detail for r in got), got


MS2, PORT2 = "migkit-test-ms-sums-dst", 15944


@pytest.fixture(scope="module")
def second(server):
    subprocess.run(["docker", "rm", "-f", "-v", MS2], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MS2, "-p",
                    f"{PORT2}:1433", "-e", "ACCEPT_EULA=1", "-e",
                    f"MSSQL_SA_PASSWORD={PW}", "-e",
                    "MSSQL_MEMORY_LIMIT_MB=1024", "-e",
                    "MSSQL_TELEMETRY_ENABLED=false",
                    "mcr.microsoft.com/azure-sql-edge:latest"],
                   check=True, capture_output=True)
    import pymssql
    try:
        end = time.time() + 180
        while time.time() < end:
            try:
                pymssql.connect(server="127.0.0.1", port=PORT2, user="sa",
                                password=PW, login_timeout=5).close()
                break
            except Exception:
                time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MS2],
                       capture_output=True)


def test_logins_and_users_cross_with_their_hashes(second, tmp_path):
    """Before, `users` on a SQL Server hop said the engine was not
    supported. A login now crosses with its password hash and SID, so its
    database user stays joined to it and it signs in with the password it
    had; its roles and permissions follow."""
    import pymssql

    from migkit import users
    for port in (PORT, PORT2):
        conn = pymssql.connect(server="127.0.0.1", port=port, user="sa",
                               password=PW, autocommit=True)
        cur = conn.cursor()
        cur.execute("if db_id('shop') is null create database shop")
        conn.close()
    conn = pymssql.connect(server="127.0.0.1", port=PORT, user="sa",
                           password=PW, database="shop", autocommit=True)
    cur = conn.cursor()
    for st in ("if object_id('dbo.orders') is null create table dbo.orders"
               " (id int primary key)",
               "if suser_id('app') is null create login app with password ="
               " 'CHANGE_ME-app1!', check_policy = off",
               "if user_id('app') is null create user app for login app",
               "alter role db_datareader add member app",
               "grant insert on dbo.orders to app"):
        cur.execute(st)
    conn.close()
    conn = pymssql.connect(server="127.0.0.1", port=PORT2, user="sa",
                           password=PW, database="shop", autocommit=True)
    conn.cursor().execute("if object_id('dbo.orders') is null create table"
                          " dbo.orders (id int primary key)")
    conn.close()
    hop = Hop(name="msu", engine="mssql",
              source=Endpoint(host="127.0.0.1", port=PORT, user="sa",
                              password=PW),
              target=Endpoint(host="127.0.0.1", port=PORT2, user="sa",
                              password=PW), databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    out, _ = users.compare(hop, [].append)
    assert {"login:app", "user:shop.app"} <= set(out["missing_on_target"])
    said = []
    users.create(hop, apply=True, say=said.append)
    assert not any("CHANGE_ME" in m or "0x0200" in m for m in said), said
    out, _ = users.compare(hop, [].append)
    assert out["result"] == "pass", out
    # the carried hash signs app in with the source's password, into the
    # database its user is joined to, with the rights it had
    conn = pymssql.connect(server="127.0.0.1", port=PORT2, user="app",
                           password="CHANGE_ME-app1!", database="shop",
                           autocommit=True)
    cur = conn.cursor()
    cur.execute("insert into dbo.orders values (1)")
    cur.execute("select count(*) from dbo.orders")
    assert cur.fetchone()[0] == 1
    conn.close()
    users.rollback(hop, apply=True, say=[].append)
    out, _ = users.compare(hop, [].append)
    assert "login:app" in out["missing_on_target"]
