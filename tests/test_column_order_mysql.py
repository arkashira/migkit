"""The same guarantee as `test_column_order_pg.py`, on MySQL.

MySQL never had the defect: `_row_expr` has always read the column list from
`information_schema` and named each column, so the physical order the two
servers happen to store them in cannot reach the hash. That makes this file a
regression guard rather than a bug fix - the property is only worth anything
if it holds on every engine, and an unasserted property is one refactor away
from being gone.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-colorder-src", "migkit-test-colorder-dst"
SRC_PORT, DST_PORT = 13491, 13492


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


def _mysql(name, sql, db=""):
    args = ["docker", "exec", name, "mysql", "-uroot", "-ptest", "-N"]
    if db:
        args += [db]
    r = subprocess.run(args + ["-e", sql], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


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
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="c", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              db_map={"shop": "shop"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


ROWS = 30


def _fresh():
    for n in (SRC, DST):
        _mysql(n, "drop database if exists shop; create database shop")
    _mysql(SRC, "create table t (id int primary key, name varchar(40),"
                " amount decimal(10,2), note varchar(40))", "shop")
    # same columns, different physical order
    _mysql(DST, "create table t (id int primary key, note varchar(40),"
                " name varchar(40), amount decimal(10,2))", "shop")
    for n in (SRC, DST):
        _mysql(n, "insert into t (id, name, amount, note) with recursive"
                  f" s(i) as (select 1 union all select i+1 from s"
                  f" where i < {ROWS})"
                  " select i, concat('n', i), i * 1.5, concat('note', i)"
                  " from s", "shop")
        # a seed that stopped early would let this pass for the wrong reason
        assert _mysql(n, "select count(*) from t", "shop") == str(ROWS)
    orders = [_mysql(n, "select group_concat(column_name order by"
                        " ordinal_position) from information_schema.columns"
                        " where table_schema='shop' and table_name='t'")
              for n in (SRC, DST)]
    assert orders[0] != orders[1], f"the premise failed: both are {orders[0]}"


def _sums(tmp_path):
    eng = _engine(tmp_path)
    expr = eng._row_expr("shop", "t")
    return (eng._checksum("src", "shop", "t", expr),
            eng._checksum("dst", "shop", "t", expr))


def test_a_reordered_target_hashes_the_same(pair, tmp_path):
    _fresh()
    a, b = _sums(tmp_path)
    assert a == b, (a, b)


def test_the_expression_names_its_columns(pair, tmp_path):
    """The reason it holds. If this ever becomes a whole-row shortcut, the
    test above goes red and this one says why."""
    _fresh()
    expr = _engine(tmp_path)._row_expr("shop", "t")
    for c in ("id", "name", "amount", "note"):
        assert f"`{c}`" in expr, expr


def test_a_real_difference_is_still_caught(pair, tmp_path):
    _fresh()
    _mysql(DST, "update t set name = 'tampered' where id = 3", "shop")
    a, b = _sums(tmp_path)
    assert a != b


def test_a_null_is_not_confused_with_an_empty_string(pair, tmp_path):
    _fresh()
    _mysql(SRC, "update t set note = null where id = 5", "shop")
    _mysql(DST, "update t set note = '' where id = 5", "shop")
    a, b = _sums(tmp_path)
    assert a != b
