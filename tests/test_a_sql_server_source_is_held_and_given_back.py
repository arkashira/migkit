"""A SQL Server source held at cutover, read back, and given back
(`freeze.MSSQLHold`), per database either way.

Read-only: `ALTER DATABASE ... SET READ_ONLY WITH ROLLBACK IMMEDIATE`
rolls back what the application's session held open and ends it, and
nobody writes the database afterwards. Denied: the application's user
denied INSERT, UPDATE and DELETE in the database - over the role that
grants them - its session ended, and each right given back as it was.
Either way the read back says so, and says so when it did not hold.
"""
import socket
import subprocess
import time

import pytest

from migkit import freeze
from migkit.config import Endpoint, Hop
from tests.conftest import mssql_run, needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.freeze", "mssql")]

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-hold-mssql", 16351, "CHANGE_ME-Str0ng!"
APP_PW = "CHANGE_ME-App0nly!"


def _conn(db, user="sa", password=PW, autocommit=True):
    import pymssql
    return pymssql.connect(server="127.0.0.1", port=str(PORT), user=user,
                           password=password, database=db,
                           autocommit=autocommit, tds_version="7.4",
                           login_timeout=10)


def _sql(db, *statements, user="sa", password=PW):
    conn = _conn(db, user, password)
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
    subprocess.run(mssql_run(MS, PORT, PW), check=True, capture_output=True)
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
def shop(server, tmp_path):
    _sql("master", "if db_id('shop') is not null begin alter database shop"
                   " set single_user with rollback immediate; drop database"
                   " shop end", "create database shop",
         "if suser_id('app') is null create login app with password ="
         f" '{APP_PW}', check_policy = off")
    _sql("shop", "create table dbo.o (id int primary key, tag nvarchar(20))",
         "insert into dbo.o values (1, 'seed')",
         "create user app for login app",
         "alter role db_datareader add member app",
         "alter role db_datawriter add member app",
         "grant insert on database::shop to app")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    hop = Hop(name="mh", engine="mssql", source=ep, target=ep,
              databases=["shop"], options={"cutover": True})
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    from migkit.engines.mssql import MSSQLEngine
    yield hop, MSSQLEngine(hop)
    _sql("master", "alter database shop set read_write with rollback"
                   " immediate")


def _rights():
    return sorted(tuple(r) for r in _sql(
        "shop", "select permission_name, state from"
                " sys.database_permissions where class = 0 and"
                " grantee_principal_id = user_id('app')"))


def _tags():
    return sorted(r[0] for r in _sql("shop", "select tag from dbo.o"))


def test_read_only_rolls_back_what_was_open_and_holds_everyone(shop):
    import pymssql
    hop, eng = shop
    got = freeze.choose(hop, eng, ["shop"])
    assert got.rung.name == "mssql_read_only", got
    # the application holds a transaction open across the hold
    held_open = _conn("shop", "app", APP_PW, autocommit=False)
    held_open.cursor().execute("insert into dbo.o values (2, 'in flight')")
    assert freeze.open_work(hop, eng, "shop", "src", None), "seen open"
    said = []
    freeze.freeze_source(hop, eng, ["shop"], said.append, "mssql_read_only")
    with pytest.raises(pymssql.Error):
        held_open.commit()
    with pytest.raises(pymssql.Error):
        _sql("shop", "insert into dbo.o values (3, 'during')", user="app",
             password=APP_PW)
    held = freeze.proof(hop, eng, "shop", "mssql_read_only")
    assert all(ok for ok, _ in held), held
    freeze.thaw_source(hop, eng, ["shop"], said.append)
    _sql("shop", "insert into dbo.o values (4, 'after')", user="app",
         password=APP_PW)
    assert _tags() == ["after", "seed"], "the open one rolled back"
    assert freeze.state(hop, "shop", "src") is None


def test_denied_writes_are_given_back_exactly_and_read_back(shop):
    import pymssql
    hop, eng = shop
    before = _rights()
    assert ("INSERT", "G") in before
    said = []
    freeze.freeze_source(hop, eng, ["shop"], said.append, "mssql_deny")
    with pytest.raises(pymssql.Error):
        _sql("shop", "insert into dbo.o values (5, 'during')", user="app",
             password=APP_PW)
    assert _tags() == ["seed"]
    held = freeze.proof(hop, eng, "shop", "mssql_deny")
    assert all(ok for ok, _ in held), held
    # a deny taken away behind the hold's back reads back as not held
    _sql("shop", "revoke update to app")
    held = freeze.proof(hop, eng, "shop", "mssql_deny")
    assert (False, "app: 2 of 3 writes denied") in held, held
    freeze.thaw_source(hop, eng, ["shop"], said.append)
    assert _rights() == before, "each right as it was"
    _sql("shop", "insert into dbo.o values (6, 'after')", user="app",
         password=APP_PW)
    assert _tags() == ["after", "seed"]
