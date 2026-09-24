"""`neutral_empty` on the two SQL servers: the target's table emptied and
kept, the source's never touched.

The cross-engine copier empties a target table before it starts it afresh.
Without this on PostgreSQL and MySQL, a copy into either would stop at its
first table; SQLite and MongoDB are covered where they are tested.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _pg(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg_engine(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(Hop(
        name="e", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                        user="postgres", password="test"),
        target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                        user="postgres", password="test"),
        databases=["postgres"]))


def test_pg_the_target_table_is_emptied_and_the_source_is_not(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        _pg(port, "drop table if exists public.t;"
                  " create table public.t (id int primary key);"
                  " insert into public.t values (1), (2), (3)")
    eng = _pg_engine(pg_pair)
    assert eng.neutral_empty("dst", "postgres", "public.t") == 3
    assert _pg(pg_pair["dst"], "select count(*) from public.t") == "0"
    with pytest.raises(SystemExit):
        eng.neutral_empty("src", "postgres", "public.t")
    assert _pg(pg_pair["src"], "select count(*) from public.t") == "3"


SRC, DST = "migkit-test-nempty-src", "migkit-test-nempty-dst"
SRC_PORT, DST_PORT = 15661, 15662


def _my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def my_pair():
    names = (SRC, DST)
    try:
        for n, p in zip(names, (SRC_PORT, DST_PORT)):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                            "mysql:8"], check=True, capture_output=True)
        for n, p in zip(names, (SRC_PORT, DST_PORT)):
            end = time.time() + 180
            while time.time() < end:
                ok = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                     "-ptest", "-h127.0.0.1",
                                     "--protocol=tcp", "-e", "select 1"],
                                    capture_output=True).returncode == 0
                with socket.socket() as s:
                    s.settimeout(2)
                    if ok and s.connect_ex(("127.0.0.1", p)) == 0:
                        break
                time.sleep(2)
            else:
                pytest.fail(f"{n} never answered")
        yield
    finally:
        for n in names:
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def test_mysql_the_target_table_is_emptied_and_the_source_is_not(my_pair):
    from migkit.engines.mysql import MySQLEngine
    for n in (SRC, DST):
        _my(n, "drop database if exists appdb; create database appdb;"
               " create table appdb.t (id int primary key);"
               " insert into appdb.t values (1), (2), (3);")
    eng = MySQLEngine(Hop(
        name="e", engine="mysql",
        source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                        password="test"),
        databases=["appdb"]))
    assert eng.neutral_empty("dst", "appdb", "t") == 3
    assert _my(DST, "select count(*) from appdb.t") == "0"
    with pytest.raises(SystemExit):
        eng.neutral_empty("src", "appdb", "t")
    assert _my(SRC, "select count(*) from appdb.t") == "3"
