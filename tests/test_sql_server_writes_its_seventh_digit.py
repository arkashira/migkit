"""A time written into SQL Server keeps every digit it has, to the seventh
(type-fidelity G3, on the write side), measured on Azure SQL Edge.

Before: pymssql sends a Python datetime as its text at three digits
after the second (`'2024-01-01 00:00:00.123'` for 0.123456), so every
time moved into SQL Server lost its fourth to sixth digit - and a
`datetime2(7)`'s seventh, read at nine from DuckDB's `TIMESTAMP_NS`,
went with them. The move's own read-back found the difference only after
the rows were written. Now a time goes as SQL Server's own text of it,
every digit to the seventh, and a `datetime` column is sent no more than
the three it takes.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-ty2-ms", 16218, "Migkit-Test-9x!"


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
                except Exception:  # noqa: BLE001 - not up yet
                    pass
            time.sleep(2)
        else:
            pytest.fail("SQL Server never answered")
        _sql("master", "create database cx")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)


@pytest.fixture
def source(tmp_path):
    import duckdb
    path = tmp_path / "src.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("create schema cx; create table cx.t (id integer primary"
                 " key, ts timestamp, ns timestamp_ns, tm time, old"
                 " timestamp)")
    conn.execute("insert into cx.t values"
                 " (1, '2024-01-01 00:00:00.123456',"
                 " '2024-01-01 00:00:00.1234567', '12:00:00.654321',"
                 " '2024-01-01 00:00:00.5'),"
                 " (2, '1999-12-31 23:59:59.000001',"
                 " '1999-12-31 23:59:59.0000001', '00:00:00.000001',"
                 " '1999-12-31 23:59:59'),"
                 " (3, null, null, null, null)")
    conn.close()
    return path


def _pair(tmp_path, path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="ms7", engine="hetero",
              options={"source_engine": "duckdb", "target_engine": "mssql"},
              source=Endpoint(host="", port=0, options={"path": str(path)}),
              target=Endpoint(host="127.0.0.1", port=PORT, user="sa",
                              password=PW),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _verdict(eng):
    got = [r for r in eng.check_data("cx")
           if r.check == "data" and r.scope.endswith(".t")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def test_every_digit_to_the_seventh_is_written(server, source, tmp_path):
    _sql("cx", "if object_id('dbo.t') is not null drop table dbo.t",
         "create table dbo.t (id int primary key, ts datetime2(6),"
         " ns datetime2(7), tm time(6), old datetime)")
    eng = _pair(tmp_path, source)
    eng.move_table("cx", "", "t", 100, Checkpoint(), [].append)
    held = _sql("cx", "select convert(varchar(27), ts, 121),"
                      " convert(varchar(27), ns, 121),"
                      " convert(varchar(16), tm),"
                      " convert(varchar(23), old, 121)"
                      " from dbo.t order by id")
    assert held[0] == ("2024-01-01 00:00:00.123456",
                       "2024-01-01 00:00:00.1234567", "12:00:00.654321",
                       "2024-01-01 00:00:00.500"), held
    assert held[1] == ("1999-12-31 23:59:59.000001",
                       "1999-12-31 23:59:59.0000001", "00:00:00.000001",
                       "1999-12-31 23:59:59.000"), held
    assert held[2] == (None, None, None, None), held
    got = _verdict(eng)
    assert got.status == "ok", got.detail
    # a hundred nanoseconds off, the one digit the others do not hold
    _sql("cx", "update dbo.t set ns = '2024-01-01 00:00:00.1234568'"
               " where id = 1")
    got = _verdict(eng)
    assert got.status == "diff", got.detail
