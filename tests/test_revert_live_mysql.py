"""The generated undo on MySQL, applied to a real server.

Same guarantee as `test_revert_live_pg.py`, through a different differ: the
PostgreSQL path diffs in-process with `results`, MySQL shells out to atlas.
Both directions come from the same pair of live schemas, so both reverts are
exact for structure and silent about nothing.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-revert-src", "migkit-test-revert-dst"
SRC_PORT, DST_PORT = 13485, 13486


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _atlas():
    from shutil import which
    return which("atlas") is not None


pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not _docker(), reason="docker not available"),
    pytest.mark.skipif(not _atlas(), reason="atlas not installed"),
]


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


def _apply(name, path):
    r = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                        "-ptest", "shop"],
                       input=path.read_text(), capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-800:]


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
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="r", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              db_map={"shop": "shop"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _shape(name):
    return _mysql(name, "select group_concat(concat(table_name,':',"
                        "column_name,':',column_type) order by table_name,"
                        " column_name) from information_schema.columns"
                        " where table_schema='shop'")


def _fresh(src_extra="", dst_extra=""):
    for n in (SRC, DST):
        _mysql(n, "drop database if exists shop; create database shop",
               db="mysql")
        _mysql(n, "create table t (id int primary key, name varchar(20));"
                  " insert into t values (1,'a'),(2,'b')")
        assert _mysql(n, "select count(*) from t") == "2"
    if src_extra:
        _mysql(SRC, src_extra)
    if dst_extra:
        _mysql(DST, dst_extra)


def test_an_added_column_round_trips_exactly(pair, tmp_path):
    _fresh(src_extra="alter table t add column extra int")
    before = _shape(DST)
    res = _engine(tmp_path)._atlas("shop")
    fix = tmp_path / "schema-fix.sql"
    rev = tmp_path / "schema-fix.revert.sql"
    assert res.status == "diff", res.detail
    assert fix.exists() and rev.exists(), res.detail
    _apply(DST, fix)
    assert "extra" in _shape(DST)
    _apply(DST, rev)
    assert _shape(DST) == before


def test_a_dropped_column_is_named_as_unrecoverable(pair, tmp_path):
    """The target has a column the source does not, so the repair drops it and
    the undo cannot bring its values back."""
    _fresh(dst_extra="alter table t add column doomed varchar(20);"
                     " update t set doomed = 'keepme'")
    assert _mysql(DST, "select count(*) from t where doomed='keepme'") == "2"
    res = _engine(tmp_path)._atlas("shop")
    rev = tmp_path / "schema-fix.revert.sql"
    assert rev.exists(), res.detail
    head = rev.read_text()
    assert "DROP COLUMN" in head.upper(), head[:400]
    assert "needs a backup, not this file" in head
    assert "take a backup first" in res.detail, res.detail

    _apply(DST, tmp_path / "schema-fix.sql")
    assert _mysql(DST, "select count(*) from information_schema.columns where"
                       " table_schema='shop' and table_name='t'"
                       " and column_name='doomed'") == "0"
    _apply(DST, rev)
    assert _mysql(DST, "select count(*) from information_schema.columns where"
                       " table_schema='shop' and table_name='t'"
                       " and column_name='doomed'") == "1"
    assert _mysql(DST, "select count(*) from t where doomed is not null") \
        == "0"


def test_identical_schemas_leave_no_stale_undo(pair, tmp_path):
    _fresh(src_extra="alter table t add column extra int")
    _engine(tmp_path)._atlas("shop")
    assert (tmp_path / "schema-fix.revert.sql").exists()
    _mysql(DST, "alter table t add column extra int")
    res = _engine(tmp_path)._atlas("shop")
    assert res.status == "ok", res.detail
    assert not (tmp_path / "schema-fix.sql").exists()
    assert not (tmp_path / "schema-fix.revert.sql").exists(), \
        "an undo for changes nobody made was left behind"
