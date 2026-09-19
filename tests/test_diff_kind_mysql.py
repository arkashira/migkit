"""MySQL names the shape of a difference too - same vocabulary as PostgreSQL.

The point of the shared classifier is that a MySQL finding and a PostgreSQL
finding use the same words, so these assertions are deliberately the same
strings the PostgreSQL tests assert.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-mykind-src", "migkit-test-mykind-dst"
SRC_PORT, DST_PORT = 13408, 13409


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(), reason="docker not available")]


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _ready(name):
    for _ in range(45):
        if subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                           "-ptest", "-e", "select 1"],
                          capture_output=True).returncode == 0:
            time.sleep(1)
            return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        assert _ready(n)
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _sql(name, sql):
    r = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                        "-ptest"], input=sql, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


def _fresh(rows=20):
    for n in (SRC, DST):
        _sql(n, f"""
            drop database if exists shop; create database shop; use shop;
            create table t (id int primary key, payload varchar(40));
            insert into t (id, payload)
            select n, concat('v', n) from (
              with recursive s(n) as (select 1 union all
                                      select n+1 from s where n < {rows})
              select n from s) x;
        """)
        got = _sql(n, "select count(*) from shop.t;").splitlines()[-1]
        assert got == str(rows), f"seed made {got} rows, wanted {rows}"


def _engine():
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["shop"])
    return MySQLEngine(hop)


def test_identical_tables_claim_no_kind(pair):
    _fresh()
    r, _, _ = _engine()._diff_table("shop", "t")
    assert r.status == "ok", r.detail
    assert "kind=" not in r.detail, r.detail


def test_values_changed(pair):
    _fresh()
    _sql(DST, "update shop.t set payload = 'tampered' where id = 3;")
    r, _, _ = _engine()._diff_table("shop", "t")
    assert r.status == "diff"
    assert "kind=values-changed" in r.detail, r.detail


def test_rows_missing(pair):
    _fresh()
    _sql(DST, "delete from shop.t where id = 4;")
    r, _, _ = _engine()._diff_table("shop", "t")
    assert r.status == "diff"
    assert "rows-missing" in r.detail and "by=1" in r.detail, r.detail


def test_rows_extra(pair):
    _fresh()
    _sql(DST, "insert into shop.t values (999, 'ghost');")
    r, _, _ = _engine()._diff_table("shop", "t")
    assert r.status == "diff"
    assert "rows-extra" in r.detail and "by=1" in r.detail, r.detail


def test_rows_replaced(pair):
    _fresh()
    _sql(DST, "delete from shop.t where id = 5;")
    _sql(DST, "insert into shop.t values (500, 'v5');")
    r, _, _ = _engine()._diff_table("shop", "t")
    assert r.status == "diff"
    assert "rows-replaced" in r.detail, r.detail
