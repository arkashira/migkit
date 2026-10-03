"""SQL Server's procedures, functions and triggers converted by migkit's
rules and proved against SQL Server itself (backlog R11, as far as the
sandbox allows).

SQL Edge is the SQL Server that runs on this machine's architecture; it
runs T-SQL procedures, functions and triggers, and lacks what CLR backs
(`FORMAT`, `hierarchyid`) and the extended events its coverage tools
read - so a routine is proved here without coverage on the source side,
and its verdict says so. Four servers: the hop's two and the sandbox's
two, so this runs in the machine's second, larger VM
(`with_docker_lock.py --vm migkit`).
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("schema.routines", "mssql")]

pytestmark = [pytest.mark.docker]

MS, SB_MS = "migkit-test-code-ms", "migkit-test-code-sbms"
PG, SB_PG = "migkit-test-code-pg", "migkit-test-code-sbpg"
MS_PORT, SB_MS_PORT, PG_PORT, SB_PG_PORT = 16087, 16088, 16081, 16083
PASSWORD = "Mk1!CHANGE_ME_test"
EDGE = "mcr.microsoft.com/azure-sql-edge:latest"


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


def _ms(sql, port=MS_PORT, db="master"):
    import pymssql
    conn = pymssql.connect(server="127.0.0.1", port=str(port), user="sa",
                           password=PASSWORD, database=db, autocommit=True,
                           login_timeout=10)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def _pg(sql, name=PG, db="ts"):
    got = _sh("docker", "exec", "-i", name, "psql", "-U", "postgres", "-d",
              db, "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SOURCE = [
    "create table dbo.orders (id int identity(1,1) primary key, amount"
    " decimal(10,2) not null, cust nvarchar(20) null)",
    "create table dbo.audit (id int identity(1,1) primary key, order_id int"
    " not null, what nvarchar(60) null)",
    "set identity_insert dbo.orders on; insert into dbo.orders (id, amount,"
    " cust) values (1, 5.00, N'ann'), (2, 20.50, N'bob'), (3, 10.00, null);"
    " set identity_insert dbo.orders off",
    "create function dbo.steps(@x int) returns int as begin declare @y int"
    " set @y = @x + 1 return @y end",
    "create procedure dbo.grade @s int, @g varchar(10) output as begin set"
    " nocount on if @s >= 90 set @g = 'A' else if @s >= 80 set @g = 'B'"
    " else begin set @g = 'C' set @g += '-' end end",
    # the third insert fails on its NOT NULL: the two before it keep
    # their rows, as SQL Server keeps them in a TRY
    "create procedure dbo.add_twice @id int, @err nvarchar(200) output as"
    " begin begin try insert into dbo.orders (amount, cust) values (1.00,"
    " N'x') insert into dbo.audit (order_id, what) values (@id, null)"
    " insert into dbo.audit (order_id, what) values (null, N'no order')"
    " set @err = 'ok' end try begin catch set @err = 'caught' end catch"
    " end",
    "create trigger dbo.orders_ai on dbo.orders after insert as begin set"
    " nocount on insert into dbo.audit (order_id, what) select i.id,"
    " 'amount ' + cast(i.amount as varchar(20)) from inserted i end",
]


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    for n, port in ((MS, MS_PORT), (SB_MS, SB_MS_PORT)):
        _sh("docker", "rm", "-f", "-v", n)
        _sh("docker", "run", "-d", "--name", n, "-e", "ACCEPT_EULA=Y", "-e",
            f"MSSQL_SA_PASSWORD={PASSWORD}", "-p", f"127.0.0.1:{port}:1433",
            EDGE)
    from migkit.storedcode.sandbox import PG_DOCKERFILE, pg_tag
    tag = pg_tag(16)
    if _sh("docker", "image", "inspect", tag).returncode != 0:
        assert _sh("docker", "build", "-t", tag, "-", timeout=900,
                   input=PG_DOCKERFILE.format(v=16)).returncode == 0
    for n, port, image in ((PG, PG_PORT, "postgres:16"),
                           (SB_PG, SB_PG_PORT, tag)):
        _sh("docker", "rm", "-f", "-v", n)
        _sh("docker", "run", "-d", "--name", n, "-e",
            "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432", image)
    try:
        for port in (MS_PORT, SB_MS_PORT):
            for _ in range(90):
                try:
                    _ms("select 1", port)
                    break
                except Exception:
                    time.sleep(2)
        for n in (PG, SB_PG):
            for _ in range(60):
                if _sh("docker", "exec", n, "psql", "-U", "postgres", "-c",
                       "select 1").returncode == 0:
                    break
                time.sleep(1)
        for port in (MS_PORT, SB_MS_PORT, PG_PORT, SB_PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _ms("create database ts")
        for stmt in SOURCE:
            _ms(stmt, db="ts")
        _pg("create database ts", db="postgres")
        _pg("""
            create table orders (id integer generated by default as identity
              primary key, amount numeric(10,2) not null, cust varchar(20));
            create table audit (id integer generated by default as identity
              primary key, order_id integer not null, what varchar(60));
            insert into orders overriding system value values
              (1, 5.00, 'ann'), (2, 20.50, 'bob'), (3, 10.00, null);
            select setval(pg_get_serial_sequence('orders', 'id'), 3);
            """)
        yield
    finally:
        for n in (MS, SB_MS, PG, SB_PG):
            _sh("docker", "rm", "-f", "-v", n)


def _engine(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="ts", engine="hetero",
              options={"source_engine": "mssql", "target_engine": "postgres",
                       "proof_sandbox": {
                           "source": {"host": "127.0.0.1",
                                      "port": SB_MS_PORT, "user": "sa",
                                      "password": PASSWORD},
                           "target": {"host": "127.0.0.1",
                                      "port": SB_PG_PORT,
                                      "user": "postgres",
                                      "password": "test"}},
                       "code_dir": str(tmp_path / "none")},
              source=Endpoint(host="127.0.0.1", port=MS_PORT, user="sa",
                              password=PASSWORD),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["ts"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_tsql_converted_by_the_rules_and_proved_on_sql_server(servers,
                                                              tmp_path):
    eng = _engine(tmp_path)
    got = {n: s for n, s, _ in eng.converted_code("ts")}
    conv = json.loads((tmp_path / "code-conversion.json").read_text())
    # the named sandbox was used (else this says why not)
    assert conv["sandbox"] == "used", conv["sandbox"]
    record = {r["routine"]: r for r in conv["routines"]}
    for name in ("function steps", "procedure grade", "procedure add_twice",
                 "trigger orders_ai"):
        assert record[name]["rung"] == "rules", record[name]
        # SQL Edge measures no coverage: proved, with that said
        assert record[name]["class"] == "proved with caveats", record[name]
        assert "coverage cannot be measured on mssql" in \
            record[name]["evidence"]["reason"], record[name]
    # the TRY kept the rows written before the failure, as SQL Server did
    assert "exit _migkit_try" in got["add_twice"], got["add_twice"]
    # the hop's own SQL Server was only read
    assert _ms("select count(*) from dbo.audit", db="ts") == [(0,)]
    assert _pg("select count(*) from audit") == "0"
