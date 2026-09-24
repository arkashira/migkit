"""The three checks PostgreSQL had and MySQL did not.

Each test seeds the actual defect on a real MySQL pair and asserts migkit
names it. Seeds are verified, so an empty table cannot make a check look
like it passed.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-myparity-src", "migkit-test-myparity-dst"
SRC_PORT, DST_PORT = 13406, 13407


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


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
                           "-h127.0.0.1", "--protocol=tcp",
                           "-e", "select 1"],
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


def _sql(name, sql, expect_ok=True):
    r = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                        "-ptest"], input=sql, capture_output=True, text=True)
    if expect_ok:
        assert r.returncode == 0, r.stderr
    return r


def _rows(name, sql):
    return _sql(name, sql).stdout.strip().splitlines()[-1]


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


def _find(results, suffix):
    hits = [r for r in results if r.scope.endswith(suffix)]
    assert hits, f"no {suffix} result in {[r.scope for r in results]}"
    return hits[0]


def test_null_vs_empty_string_flip_is_named(pair):
    ddl = """drop database if exists shop; create database shop; use shop;
             create table t (id int primary key, note varchar(50));"""
    _sql(SRC, ddl + "insert into t values (1, null), (2, 'x');")
    _sql(DST, ddl + "insert into t values (1, ''),   (2, 'x');")
    assert _rows(SRC, "select count(*) from shop.t;") == "2"
    assert _rows(DST, "select count(*) from shop.t;") == "2"

    r = _find(_engine().check_deep("shop"), "nullempty")
    assert r.status == "diff", r.detail
    assert r.category == "value.null-empty"
    assert "t.note" in r.detail, r.detail


def test_matching_null_and_empty_counts_pass(pair):
    ddl = """drop database if exists shop; create database shop; use shop;
             create table t (id int primary key, note varchar(50));"""
    for n in (SRC, DST):
        _sql(n, ddl + "insert into t values (1, null), (2, ''), (3, 'x');")
        assert _rows(n, "select count(*) from shop.t;") == "3"

    r = _find(_engine().check_deep("shop"), "nullempty")
    assert r.status == "ok", r.detail


def test_float_drift_beyond_tolerance_is_named(pair):
    ddl = """drop database if exists shop; create database shop; use shop;
             create table f (id int primary key, v double);"""
    _sql(SRC, ddl + "insert into f values (1, 1.5), (2, 2.5), (3, 1000.125);")
    _sql(DST, ddl + "insert into f values (1, 1.5), (2, 2.5), (3, 1000.5);")
    for n in (SRC, DST):
        assert _rows(n, "select count(*) from shop.f;") == "3"

    r = _find(_engine().check_deep("shop"), "float")
    assert r.status == "diff", r.detail
    assert r.category == "value.precision"
    assert "f.v" in r.detail, r.detail


def test_identical_floats_are_not_flagged(pair):
    ddl = """drop database if exists shop; create database shop; use shop;
             create table f (id int primary key, v double);"""
    for n in (SRC, DST):
        _sql(n, ddl + "insert into f values (1, 0.1), (2, 0.2), (3, 1e12);")
        assert _rows(n, "select count(*) from shop.f;") == "3"

    r = _find(_engine().check_deep("shop"), "float")
    assert r.status == "ok", r.detail


def test_a_check_constraint_not_enforced_on_the_target_is_named(pair):
    """The target keeps the constraint in its catalog and enforces nothing,
    so counting constraints finds both sides equal."""
    base = """drop database if exists shop; create database shop; use shop;
              create table c (id int primary key, qty int,"""
    _sql(SRC, base + " constraint qty_pos check (qty > 0));")
    _sql(DST, base + " constraint qty_pos check (qty > 0) not enforced);")
    # both sides really do have one CHECK constraint
    for n in (SRC, DST):
        assert _rows(n, "select count(*) from information_schema"
                        ".table_constraints where constraint_schema='shop'"
                        " and constraint_type='CHECK';") == "1"
    # and the target really does accept a row the source rejects
    assert _sql(DST, "insert into shop.c values (1, -5);",
                expect_ok=False).returncode == 0
    assert _sql(SRC, "insert into shop.c values (1, -5);",
                expect_ok=False).returncode != 0

    r = _find(_engine().check_deep("shop"), "checks")
    assert r.status == "diff", r.detail
    assert r.category == "structure.unvalidated-constraints"
    assert "NOT ENFORCED" in r.detail, r.detail


def test_matching_enforced_constraints_pass(pair):
    base = """drop database if exists shop; create database shop; use shop;
              create table c (id int primary key, qty int,
              constraint qty_pos check (qty > 0));"""
    for n in (SRC, DST):
        _sql(n, base)
    r = _find(_engine().check_deep("shop"), "checks")
    assert r.status == "ok", r.detail
