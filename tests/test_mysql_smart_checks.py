"""Port of the smart checks to the mysql engine: a table with no pk/unique is
flagged (CDC drops its updates/deletes), and AUTO_INCREMENT is reported as
usable (won't collide) vs parity (matches source) - the same two-verdict split
the postgres engine gives."""
import socket
import subprocess
import time

import pytest


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-test-mysqlsmart-src", "migkit-test-mysqlsmart-dst"
SRC_PORT, DST_PORT = 13400, 13401


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _ready(name):
    for _ in range(40):
        if subprocess.run(["docker", "exec", name, "mysql", "-uroot", "-ptest",
                           "-e", "select 1"], capture_output=True).returncode == 0:
            time.sleep(1)
            return
        time.sleep(2)


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        _ready(n)
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _sql(name, sql):
    return subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                           "-ptest"], input=sql, capture_output=True, text=True)


def _engine(db="shop"):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=[db])
    return MySQLEngine(hop)


def test_mysql_nopk_and_autoinc_verdicts(pair):
    _sql(SRC, "create database shop;"
              " create table shop.orders(id int auto_increment primary key,"
              " v int);"
              " insert into shop.orders(v) select 1 from"
              " information_schema.tables limit 10;"
              " create table shop.events(a int, b int);")
    _sql(DST, "create database shop;"
              " create table shop.orders(id int auto_increment primary key,"
              " v int);"
              " insert into shop.orders(v) select 1 from"
              " information_schema.tables limit 5;"
              " create table shop.events(a int, b int);")

    eng = _engine()

    deep = eng.check_deep("shop")
    keys = [r for r in deep if r.scope.endswith("keys")]
    assert keys and keys[0].status == "diff", [r.__dict__ for r in deep]
    assert "events" in keys[0].detail

    ai = eng.check_autoinc("shop")
    usable = [r for r in ai if r.scope.endswith("usable")]
    parity = [r for r in ai if r.scope.endswith("parity")]
    assert usable and usable[0].status == "ok", [r.__dict__ for r in ai]
    assert parity and parity[0].status == "diff", [r.__dict__ for r in ai]
    assert "orders" in parity[0].detail


def test_mysql_type_narrowing(pair):
    _sql(SRC, "create database narrowtest;"
              " create table narrowtest.t(id int primary key,"
              " name varchar(100), price decimal(12,4), big bigint,"
              " u int unsigned);")
    _sql(DST, "create database narrowtest;"
              " create table narrowtest.t(id int primary key,"
              " name varchar(50), price decimal(12,2), big int, u int);")

    deep = _engine("narrowtest").check_deep("narrowtest")
    nrw = [r for r in deep if r.scope.endswith("narrowing")]
    assert nrw and nrw[0].status == "diff", [r.__dict__ for r in deep]
    d = nrw[0].detail
    assert "name" in d and "50" in d, d          # varchar shrank
    assert "scale" in d, d                        # decimal scale shrank
    assert "bigint" in d and "int" in d, d        # integer width shrank
    assert "unsigned" in d, d                     # unsigned -> signed


def test_mysql_timeshift(pair):
    srows = ",".join(f"({i},'2026-01-0{i} 07:00:00')" for i in range(1, 7))
    drows = ",".join(f"({i},'2026-01-0{i} 15:00:00')" for i in range(1, 7))
    _sql(SRC, "create database tztest;"
              " create table tztest.ev(id int primary key, ts datetime);"
              f" insert into tztest.ev values {srows};")
    # target stored the same rows 8 hours ahead (non-UTC session on load)
    _sql(DST, "create database tztest;"
              " create table tztest.ev(id int primary key, ts datetime);"
              f" insert into tztest.ev values {drows};")

    deep = _engine("tztest").check_deep("tztest")
    ts = [r for r in deep if r.scope.endswith("timeshift")]
    assert ts and ts[0].status == "diff", [r.__dict__ for r in deep]
    assert "8.0h" in ts[0].detail or "28800" in ts[0].detail, ts[0].detail


def test_mysql_charset_downgrade(pair):
    _sql(SRC, "create database cstest;"
              " create table cstest.t(id int primary key, name varchar(50))"
              " charset=utf8mb4;")
    _sql(DST, "create database cstest;"
              " create table cstest.t(id int primary key, name varchar(50))"
              " charset=latin1;")
    deep = _engine("cstest").check_deep("cstest")
    cs = [r for r in deep if r.scope.endswith("charset")]
    assert cs and cs[0].status == "diff", [r.__dict__ for r in deep]
    assert "downgrade" in cs[0].detail.lower(), cs[0].detail
    assert "t.name" in cs[0].detail, cs[0].detail


def test_mysql_partition_stranded(pair):
    base = ("create table events(id int, ts date, primary key(id,ts))"
            " partition by range (year(ts)) (")
    _sql(SRC, "create database ptest;"
              " use ptest;" + base
              + " partition p2025 values less than (2026),"
                " partition p2026 values less than (2027),"
                " partition pmax values less than maxvalue);"
                " insert into events values (1,'2025-06-01'),(2,'2026-06-01');")
    _sql(DST, "create database ptest;"
              " use ptest;" + base
              + " partition p2025 values less than (2026),"
                " partition pmax values less than maxvalue);"
                " insert into events values (1,'2025-06-01'),(2,'2026-06-01');")
    deep = _engine("ptest").check_deep("ptest")
    p = [r for r in deep if r.scope.endswith("partitions")]
    assert p and p[0].status == "diff", [r.__dict__ for r in deep]
    assert "stranded" in p[0].detail.lower() or "missing" in p[0].detail.lower()
    assert "events" in p[0].detail, p[0].detail


def test_mysql_generated_drift(pair):
    _sql(SRC, "create database gtest;"
              " create table gtest.t(id int primary key, a int,"
              " s int as (a * 2) stored);")
    _sql(DST, "create database gtest;"
              " create table gtest.t(id int primary key, a int,"
              " s int as (a * 3) stored);")
    deep = _engine("gtest").check_deep("gtest")
    g = [r for r in deep if r.scope.endswith("generated")]
    assert g and g[0].status == "diff", [r.__dict__ for r in deep]
    assert "expression differs" in g[0].detail.lower(), g[0].detail
    assert "t.s" in g[0].detail, g[0].detail


def test_mysql_collation_collapse(pair):
    _sql(SRC, "create database coltest;"
              " create table coltest.t(id int primary key,"
              " name varchar(50) collate utf8mb4_bin unique);"
              " insert into coltest.t values (1,'Foo'),(2,'foo');")
    _sql(DST, "create database coltest;"
              " create table coltest.t(id int primary key,"
              " name varchar(50) collate utf8mb4_general_ci unique);"
              " insert into coltest.t values (1,'Foo');")
    deep = _engine("coltest").check_deep("coltest")
    c = [r for r in deep if r.scope.endswith("collation")]
    assert c and c[0].status == "diff", [r.__dict__ for r in deep]
    assert "collapse" in c[0].detail.lower(), c[0].detail
    assert "t.name" in c[0].detail, c[0].detail


def test_mysql_long_transaction_flagged_by_assess(pair):
    import pymysql
    _sql(SRC, "create database lrtx;"
              " create table lrtx.t(id int primary key);")
    # hold an open transaction on the source while assess runs
    conn = pymysql.connect(host="127.0.0.1", port=SRC_PORT, user="root",
                           password="test", database="lrtx")
    try:
        cur = conn.cursor()
        cur.execute("start transaction")
        cur.execute("insert into t values (1)")   # active RW transaction
        items = _engine("lrtx").assess()
    finally:
        conn.rollback()
        conn.close()

    lrt = [i for i in items if "long-running" in i["item"]]
    assert lrt, [i for i in items if i["scope"] == "instance"]
    assert "open" in lrt[0]["detail"], lrt[0]["detail"]
