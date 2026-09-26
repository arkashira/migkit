"""Rows that differ in a table too large to walk, found by halving the key
range on both sides (plan 19).

The walk that names differing rows stops after 20,000 rows a side and says
so. In a table of 60,000 rows, a changed row past that point was never
named. Now a table past the cap, keyed by one integer, is digested in
halves on both engines, and only the halves that disagree are followed
down to ranges small enough to walk. An integer range holds the same rows
in both engines, where a text range would depend on each one's collation.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

pytestmark = [pytest.mark.docker]

MY, MY_PORT = "migkit-test-bisect-my", 15842


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-D", "cx", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair(pg_pair):
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8.4"], check=True, capture_output=True)
    try:
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "create database if not exists cx"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        with socket.socket() as s:
            s.settimeout(5)
            assert s.connect_ex(("127.0.0.1", MY_PORT)) == 0
        _my("set session cte_max_recursion_depth = 100000;"
            " create table t (id int primary key, v varchar(20));"
            " insert into t with recursive g(n) as (select 1 union all"
            " select n + 1 from g where n < 60000) select n, concat('v', n)"
            " from g")
        dst = pg_pair["dst"]
        psql(dst, "drop database if exists cx")
        assert psql(dst, "create database cx").returncode == 0
        assert psql(dst, "create table t (id bigint primary key,"
                         " v varchar(20)); insert into t select g, 'v' || g"
                         " from generate_series(1, 60000) g",
                    db="cx").returncode == 0
        yield dst
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _eng(dst, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="bs", engine="hetero",
              options={"source_engine": "mysql",
                       "target_engine": "postgres"},
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=dst, user="postgres",
                              password="test"), databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_differences_past_the_walks_reach_are_named(pair, tmp_path):
    dst = pair
    # past the 20,000 rows the walk reads: changed, gone, and one extra
    psql(dst, "update t set v = 'changed' where id = 55000;"
              " delete from t where id = 41234;"
              " insert into t values (60001, 'only here')", db="cx")
    got = [r for r in _eng(dst, tmp_path).check_data("cx")
           if r.check == "data"]
    assert [r.status for r in got] == ["diff"], [r.__dict__ for r in got]
    detail = got[0].detail
    assert "1 missing on the target (41234)" in detail, detail
    assert "1 with different values (55000)" in detail, detail
    assert "1 only on the target (60001)" in detail, detail
    assert "found by halving the key range" in detail, detail
    assert "there may be more" not in detail, detail
    # the files sync reads name the same three
    assert (tmp_path / "data-t.changed").read_text().strip() == "[\"55000\"]"
