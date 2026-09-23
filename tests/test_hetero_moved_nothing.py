"""The cross-engine bulk path notices a move that moved nothing.

Every same-engine bulk path had this guard; the MySQL-to-PostgreSQL one did
not, so `move` printed its success line over an empty target and could only
add that the engine "cannot confirm the rows landed".
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

NAME, PORT = "migkit-test-hmn-my", 15668


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", NAME, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr


@pytest.fixture(scope="module")
def mysql_src():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", NAME, "mysql", "-uroot",
                                 "-ptest", "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


def _engine(pg_pair, mapping=None, pg_port=None):
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(Hop(
        name="h", engine="hetero",
        source=Endpoint(host="127.0.0.1", port=PORT, user="root",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=pg_port or pg_pair["dst"],
                        user="postgres", password="test"),
        databases=["postgres"], mapping=mapping or {},
        options={"source_engine": "mysql", "target_engine": "postgres"}))


def _seed(pg_pair, target_tables):
    _my("drop database if exists postgres; create database postgres;"
        " create table postgres.orders (id int primary key);"
        " insert into postgres.orders values (1);"
        " create table postgres.people (id int primary key);"
        " insert into postgres.people values (1);"
        " create table postgres.idle (id int primary key);")
    for t, rows in target_tables.items():
        psql(pg_pair["dst"], f"create table public.{t} (id int primary key)")
        if rows:
            psql(pg_pair["dst"], f"insert into public.{t} values (1)")


def test_a_table_that_arrived_empty_is_named(mysql_src, pg_pair):
    _seed(pg_pair, {"orders": True, "people": False, "idle": False})
    # idle is empty on the source too, so it is not a failure to fill it
    assert _engine(pg_pair).moved_nothing("postgres") == ["people"]


def test_a_renamed_table_is_looked_for_under_its_new_name(mysql_src,
                                                          pg_pair):
    _seed(pg_pair, {"orders": True, "persons": True})
    got = _engine(pg_pair, {"tables": {"people": "persons"}}) \
        .moved_nothing("postgres")
    assert got == [], got


def test_a_target_it_cannot_ask_is_not_a_clean_bill(mysql_src, pg_pair):
    _seed(pg_pair, {"orders": True})
    """None, not every table: an unreachable target is not an empty one."""
    assert _engine(pg_pair, pg_port=1).moved_nothing("postgres") is None
