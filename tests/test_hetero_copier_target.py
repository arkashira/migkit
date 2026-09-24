"""The MySQL-to-PostgreSQL table copier writes where the hop says, and only
what the source has.

It connected to the target database by the *source's* name, so a hop that
maps `shop` to `shop_new` either failed on a database that does not exist
or, where one of that name did, filled the wrong one. And each chunk
replaced only its own key range, so a target row outside the source's
whole range - left by an earlier load, or deleted on the source since -
survived a move that then called itself complete.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

NAME, PORT = "migkit-test-hct-my", 15676


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", NAME, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


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


class _Ck(dict):
    def save(self):
        pass


def _engine(pg_pair, target_db="shop_new"):
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(Hop(
        name="hct", engine="hetero",
        source=Endpoint(host="127.0.0.1", port=PORT, user="root",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                        user="postgres", password="test"),
        databases=["shop"],
        db_map={"shop": target_db} if target_db != "shop" else {},
        options={"source_engine": "mysql", "target_engine": "postgres"}))


@pytest.mark.parametrize("target_db", ["shop_new", "shop"])
def test_the_copy_lands_where_the_hop_says_and_leaves_no_strays(
        mysql_src, pg_pair, target_db):
    _my("drop database if exists shop; create database shop;"
        " create table shop.orders (id int primary key, v varchar(20));"
        " insert into shop.orders values (1,'a'),(2,'b'),(3,'c'),(4,'d'),"
        " (5,'e');")
    dst = pg_pair["dst"]
    psql(dst, "drop database if exists shop_new")
    psql(dst, "drop database if exists shop")
    assert psql(dst, f"create database {target_db}").returncode == 0
    assert psql(dst, "create table orders (id int primary key,"
                     " v varchar(20))", target_db).returncode == 0
    # an earlier load's leftovers: below, above and inside the range
    assert psql(dst, "insert into orders values (0,'old'), (999,'old'),"
                     " (3,'stale')", target_db).returncode == 0

    eng = _engine(pg_pair, target_db)
    assert eng.my and eng.pg, "this is the MySQL-to-PostgreSQL copier"
    eng.move_table("shop", "", "orders", 2, _Ck(), lambda m: None)

    got = psql(dst, "select id || ':' || v from orders order by id",
               target_db).stdout.split()
    assert got == ["1:a", "2:b", "3:c", "4:d", "5:e"], got
    if target_db != "shop":
        # nothing was written under the source's name
        assert psql(dst, "select 1 from pg_database where datname = 'shop'"
                    ).stdout.strip() == ""


def test_an_empty_source_table_empties_the_target_one(mysql_src, pg_pair):
    _my("drop database if exists shop; create database shop;"
        " create table shop.orders (id int primary key, v varchar(20));")
    dst = pg_pair["dst"]
    psql(dst, "drop database if exists shop_new")
    assert psql(dst, "create database shop_new").returncode == 0
    assert psql(dst, "create table orders (id int primary key,"
                     " v varchar(20))", "shop_new").returncode == 0
    assert psql(dst, "insert into orders values (7,'old')",
                "shop_new").returncode == 0
    _engine(pg_pair).move_table("shop", "", "orders", 2, _Ck(),
                                lambda m: None)
    assert psql(dst, "select count(*) from orders",
                "shop_new").stdout.strip() == "0"


def test_watch_counts_the_mapped_target_and_says_when_it_cannot(
        mysql_src, pg_pair):
    """`watch` counted the target under the source's database name, and a
    count that failed was reported as zero rows - an empty target, which
    is the one thing a load being watched must not be mistaken for."""
    _my("drop database if exists shop; create database shop;"
        " create table shop.orders (id int primary key, v varchar(20));"
        " insert into shop.orders values (1,'a'),(2,'b'),(3,'c');")
    dst = pg_pair["dst"]
    psql(dst, "drop database if exists shop")
    psql(dst, "drop database if exists shop_new")
    assert psql(dst, "create database shop_new").returncode == 0
    assert psql(dst, "create table orders (id int primary key,"
                     " v varchar(20)); insert into orders values (1,'a'),"
                     " (2,'b')", "shop_new").returncode == 0
    got = _engine(pg_pair).watch_sample("shop")
    assert got.get("src_rows") == 3 and got.get("dst_rows") == 2, got
    psql(dst, "drop database shop_new")
    got = _engine(pg_pair).watch_sample("shop")
    assert "error" in got and "dst_rows" not in got, got
