"""`assess` says per table how deeply its changes can be read, D0-D4,
and the statement that raises it (LT.R1)."""
import socket
import subprocess
import time

import pytest

from migkit import changedepth
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker


def test_mysql_depth_follows_the_log_settings_and_the_key():
    d = changedepth.mysql
    assert d("ON", "ROW", "FULL", "FULL", True) == ("D4", "")
    assert d("ON", "ROW", "MINIMAL", "FULL", True)[0] == "D3"
    assert d("ON", "ROW", "NOBLOB", "FULL", True)[0] == "D3"
    assert d("ON", "ROW", "FULL", "FULL", False)[0] == "D1"
    assert d("ON", "MIXED", "FULL", "FULL", True)[0] == "D1"
    assert d("ON", "ROW", "FULL", "MINIMAL", True)[0] == "D0"
    depth, fix = d("OFF", "ROW", "FULL", "FULL", True)
    assert depth == "D0" and "log_bin" in fix


def test_postgresql_depth_follows_wal_level_and_replica_identity():
    d = changedepth.postgres
    assert d("replica", "d", True)[0] == "D0"
    assert d("logical", "f", False) == ("D4", "")
    assert d("logical", "d", True)[0] == "D3"
    assert d("logical", "i", False)[0] == "D3"
    assert d("logical", "d", False)[0] == "D1"
    assert d("logical", "n", True)[0] == "D1"


def test_the_lines_group_tables_by_depth_and_name_the_fix():
    got = changedepth.lines({
        "a.t1": ("D4", ""), "a.t2": ("D4", ""),
        "a.t3": ("D1", "give the table a primary key")})
    assert got[0][1].startswith("change reading D4")
    assert got[1][1].startswith("change reading D1")
    assert got[0][0] == "pass" and "a.t1, a.t2" in got[0][2]
    assert got[1][0] == "warn" and "primary key" in got[1][2]
    assert "2 tables" in got[0][1] and "1 table" in got[1][1]


MY, MY_PORT = "migkit-test-lt-r1-my", 16581


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{MY_PORT}:3306", "mysql:8.4",
                    "--server-id=7", "--binlog-row-metadata=FULL",
                    "--binlog-row-image=MINIMAL"],
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
                if ok and s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@needs_docker
def test_a_mysql_assess_names_each_depth(mysql_server):
    from migkit.engines.mysql import MySQLEngine
    sql = ("create database appdb; create table appdb.k (id int primary key);"
           " create table appdb.nokey (v int)")
    subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot", "-ptest"],
                   input=sql, text=True, check=True, capture_output=True)
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    eng = MySQLEngine(Hop(name="ltr1", engine="mysql", source=ep, target=ep,
                          databases=["appdb"]))
    rows = [r for r in eng.assess() if r["item"].startswith("change reading")]
    by = {r["item"].split(" ")[2]: r for r in rows}
    assert set(by) == {"D3", "D1"}, rows
    assert "appdb.k" in by["D3"]["detail"]
    assert "binlog_row_image = 'FULL'" in by["D3"]["detail"]
    assert "appdb.nokey" in by["D1"]["detail"]


@needs_docker
def test_a_postgresql_assess_names_each_depth(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    from tests.conftest import psql
    port = pg_pair["src"]
    psql(port, "create table lt_r1_k (id int primary key);"
               " create table lt_r1_nokey (v int);"
               " create table lt_r1_full (id int primary key);"
               " alter table lt_r1_full replica identity full")
    ep = Endpoint(host="127.0.0.1", port=port, user="postgres",
                  password="test")
    tgt = Endpoint(host="127.0.0.1", port=pg_pair["dst"], user="postgres",
                   password="test")
    eng = PostgresEngine(Hop(name="ltr1", engine="postgres", source=ep,
                             target=tgt, databases=["postgres"]))
    rows = [r for r in eng.assess() if r["item"].startswith("change reading")]
    by = {r["item"].split(" ")[2]: r for r in rows}
    assert "postgres.public.lt_r1_full" in by["D4"]["detail"], rows
    assert "postgres.public.lt_r1_k" in by["D3"]["detail"], rows
    assert "postgres.public.lt_r1_nokey" in by["D1"]["detail"], rows
