"""DEFINER and SQL SECURITY drift, against a real MySQL pair.

`_canon_ddl` strips both before the schema diff runs, on purpose: the mover
rewrites them on every object, and leaving them in would bury every real
schema finding under cosmetic noise. That is the right call for the diff and
the wrong place to stop, so they are reported separately.

Measured before this check existed: a view that was `app@% / DEFINER` on the
source and `dts_migration@% / INVOKER` on the target passed the schema check,
the object check and atlas - all three reported ok.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-own-src", "migkit-test-own-dst"
SRC_PORT, DST_PORT = 13475, 13476


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


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _mysql(name, sql, db="shop"):
    r = subprocess.run(["docker", "exec", name, "mysql", "-uroot", "-ptest",
                        "-N", db, "-e", sql], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


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
        for _ in range(90):
            r = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                "-ptest", "-e", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
        _mysql(n, "create user 'app'@'%' identified by 'x';"
                  " create user 'migrator'@'%' identified by 'y'", db="mysql")
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine(tmp_path, dst_port=DST_PORT):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="o", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=dst_port, user="root",
                              password="test"),
              db_map={"shop": "shop"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _load(src_sql, dst_sql):
    for n, sql in ((SRC, src_sql), (DST, dst_sql)):
        _mysql(n, "drop database if exists shop; create database shop",
               db="mysql")
        _mysql(n, "create table t (id int primary key);" + sql)
        assert int(_mysql(n, "select count(*) from information_schema.views"
                             " where table_schema='shop'")) >= 1, \
            "the seed created no view, so nothing below is being tested"


def _row(tmp_path, dst_port=DST_PORT):
    out = _engine(tmp_path, dst_port)._deep_ownership("shop", "shop")
    assert len(out) == 1, out
    return out[0]


def test_a_rewritten_definer_is_named(pair, tmp_path):
    _load("create definer='app'@'%' sql security definer view v"
          " as select * from t;",
          "create definer='migrator'@'%' sql security definer view v"
          " as select * from t;")
    r = _row(tmp_path)
    assert r.status == "diff", r.detail
    assert "app@%/DEFINER -> migrator@%/DEFINER" in r.detail
    assert "view v" in r.detail


def test_dropping_to_invoker_is_called_out_as_a_privilege_change(pair,
                                                                tmp_path):
    """The dangerous half. The routine now runs with the caller's privileges
    instead of the definer's: it either stops working, or starts working for
    callers who should not be able to run it."""
    _load("create definer='app'@'%' sql security definer view v"
          " as select * from t;",
          "create definer='migrator'@'%' sql security invoker view v"
          " as select * from t;")
    r = _row(tmp_path)
    assert r.status == "diff"
    assert "SQL SECURITY DEFINER to" in r.detail
    assert "caller's privileges" in r.detail


def test_matching_definers_report_ok(pair, tmp_path):
    sql = ("create definer='app'@'%' sql security definer view v"
           " as select * from t;")
    _load(sql, sql)
    r = _row(tmp_path)
    assert r.status == "ok", r.detail
    assert "keep their definer" in r.detail


def test_the_schema_diff_really_is_blind_to_this(pair, tmp_path):
    """The measurement this check exists for. If the schema diff ever starts
    reporting definer drift, this goes red and the two must be reconciled
    rather than both left running."""
    _load("create definer='app'@'%' sql security definer view v"
          " as select * from t;",
          "create definer='migrator'@'%' sql security invoker view v"
          " as select * from t;")
    results = _engine(tmp_path).check_schema("shop")
    assert results, "check_schema returned nothing"
    for r in results:
        assert r.status == "ok", (
            f"the schema check now reports {r.scope}: {r.detail} - reconcile "
            "it with the ownership check instead of reporting twice")


def test_an_object_only_on_one_side_is_not_this_checks_finding(pair,
                                                              tmp_path):
    """That is the object check's job. Saying it twice is how two copies
    drift apart."""
    _load("create definer='app'@'%' sql security definer view v"
          " as select * from t;"
          " create definer='app'@'%' sql security definer view only_src"
          " as select * from t;",
          "create definer='app'@'%' sql security definer view v"
          " as select * from t;")
    r = _row(tmp_path)
    assert r.status == "ok", r.detail
    assert "only_src" not in r.detail


def test_an_unreadable_target_is_unknown_and_not_ok(pair, tmp_path):
    _load("create definer='app'@'%' sql security definer view v"
          " as select * from t;",
          "create definer='app'@'%' sql security definer view v"
          " as select * from t;")
    r = _row(tmp_path, dst_port=1)
    assert r.status == "warn", r.detail
    assert "unknown, not clean" in r.detail
