"""SQL Server's values read as the server holds them (type-fidelity G3,
G22), measured on Azure SQL Edge, the SQL Server engine that runs here.

* `datetime2(7)` came back from the driver at six digits: a PostgreSQL
  target holding the value cut at the microsecond compared equal, and a
  move into one said nothing about the seventh digit it dropped;
* `char(10)` came back padded to ten, and read as a difference from a
  PostgreSQL `char(10)` holding the same text, which reads unpadded.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql
from tests.typepair import Checkpoint

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-f0t-ms", 16065, "Migkit-Test-9x!"


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
        _sql("master", "create database cx")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)


def _pair(tmp_path, pg_port):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="f0tms", engine="hetero",
              options={"source_engine": "mssql",
                       "target_engine": "postgres"},
              source=Endpoint(host="127.0.0.1", port=PORT, user="sa",
                              password=PW),
              target=Endpoint(host="127.0.0.1", port=pg_port,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _verdict(eng):
    got = [r for r in eng.check_data("cx")
           if r.check == "data" and r.scope == "cx.t"]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


@pytest.fixture
def tables(server, pg_pair):
    _sql("cx", "if object_id('dbo.t') is not null drop table dbo.t",
         "create table dbo.t (id int primary key, c char(10),"
         " ts datetime2(7))",
         "insert into dbo.t values (1, 'abc', '2024-01-01 00:00:00.1234567'),"
         " (2, 'x', '2024-01-01 00:00:00.5')")
    port = pg_pair["dst"]
    psql(port, "drop database if exists cx")
    assert psql(port, "create database cx").returncode == 0
    return port


def test_padding_is_not_a_difference(tables, tmp_path):
    # the premise: the driver hands the padding over
    assert _sql("cx", "select c from dbo.t where id = 1")[0][0] \
        == "abc       "
    assert psql(tables, "create table t (id int primary key, c char(10));"
                        " insert into t values (1, 'abc'), (2, 'x')",
                db="cx").returncode == 0
    got = _verdict(_pair(tmp_path, tables))
    assert got.status == "ok", got.detail
    assert psql(tables, "update t set c = 'abd' where id = 1",
                db="cx").returncode == 0
    assert _verdict(_pair(tmp_path, tables)).status == "diff"


def test_the_seventh_digit_is_read_and_compared(tables, tmp_path):
    assert psql(tables, "create table t (id int primary key,"
                        " ts timestamp(6)); insert into t values"
                        " (1, '2024-01-01 00:00:00.123456'),"
                        " (2, '2024-01-01 00:00:00.5')",
                db="cx").returncode == 0
    got = _verdict(_pair(tmp_path, tables))
    assert got.status == "diff", got.detail
    assert psql(tables, "drop table t", db="cx").returncode == 0
    with pytest.raises(SystemExit) as refused:
        _pair(tmp_path, tables).move_table("cx", "dbo", "t", 100,
                                           Checkpoint(), lambda m: None)
    said = " ".join(str(refused.value).split())
    assert "ts: 1 row holds more than 6 digits of a second" in said, said
    assert "id 1" in said, said


def _into_sql_server(tmp_path, pg_port):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="f0tms2", engine="hetero",
              options={"source_engine": "postgres",
                       "target_engine": "mssql"},
              source=Endpoint(host="127.0.0.1", port=pg_port,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="sa",
                              password=PW), databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_keys_sql_server_takes_for_one_are_refused(server, pg_pair,
                                                   tmp_path):
    """The database's default collation folds case, and every collation
    there compares `a` and `a ` as one - `_BIN2` too (type-fidelity G8)."""
    port = pg_pair["src"]
    psql(port, "drop database if exists cx")
    assert psql(port, "create database cx").returncode == 0
    assert psql(port, "create table k (pk text primary key, v int);"
                      " insert into k values ('a', 1), ('A', 2), ('a ', 3),"
                      " ('b', 4)", db="cx").returncode == 0
    _sql("cx", "if object_id('dbo.k') is not null drop table dbo.k")
    assert "_CI_" in _sql("cx", "select cast(databasepropertyex(db_name(),"
                                " 'Collation') as nvarchar(128))")[0][0]
    with pytest.raises(SystemExit) as got:
        _into_sql_server(tmp_path, port).move_table(
            "cx", "public", "k", 100, Checkpoint(), lambda m: None)
    said = " ".join(str(got.value).split())
    assert "k.pk: 1 group of keys distinct here that mssql's" in said, said
    assert "'A'" in said and "'a '" in said, said
    _sql("cx", "create table dbo.k (pk nvarchar(10) collate"
               " Latin1_General_BIN2 primary key, v int)")
    with pytest.raises(SystemExit) as got:
        _into_sql_server(tmp_path, port).move_table(
            "cx", "public", "k", 100, Checkpoint(), lambda m: None)
    said = " ".join(str(got.value).split())
    assert "1 group of keys" in said and "'a '" in said, said
    assert "'A'" not in said, said


def test_xml_is_compared_as_the_document_it_is(server, pg_pair, tmp_path):
    """SQL Server writes an `xml` value back its own way - no declaration,
    `<a b="1" />`, no whitespace between elements - where PostgreSQL keeps
    the text; compared as text, every such row read as a difference after
    a move that carried it whole (type-fidelity 8.7)."""
    port = pg_pair["src"]
    psql(port, "drop database if exists cx")
    assert psql(port, "create database cx").returncode == 0
    assert psql(port, "create table x (id int primary key, doc xml);"
                      " insert into x values"
                      " (1, '<?xml version=\"1.0\"?><a  b=\"1\"/>'),"
                      " (2, '<a> <b>t</b> <!-- n --> </a>'), (3, null)",
                db="cx").returncode == 0
    _sql("cx", "if object_id('dbo.x') is not null drop table dbo.x")
    eng = _into_sql_server(tmp_path, port)
    eng.move_table("cx", "public", "x", 100, Checkpoint(), lambda m: None)
    # the premise: SQL Server holds another text of the same documents
    held = _sql("cx", "select cast(doc as nvarchar(max)) from dbo.x"
                      " order by id")
    assert held[0][0] != '<?xml version="1.0"?><a  b="1"/>', held
    assert held[1][0] != '<a> <b>t</b> <!-- n --> </a>', held
    got = [r for r in _into_sql_server(tmp_path, port).check_data("cx")
           if r.check == "data" and r.scope == "cx.x"]
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    _sql("cx", "update dbo.x set doc = '<a><b>u</b><!-- n --></a>'"
               " where id = 2")
    got = [r for r in _into_sql_server(tmp_path, port).check_data("cx")
           if r.check == "data" and r.scope == "cx.x"]
    assert [r.status for r in got] == ["diff"], [r.detail for r in got]
