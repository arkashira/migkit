"""The same comparison of the source's own reads, on MySQL (backlog 24).

The statements come from `performance_schema`'s digests, which keep a
sample of each with its literal values: the sample is what is planned and
run, the normalised digest is what is shown.
"""
import subprocess
import time

import pytest

from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-perf-mysrc", 15805), ("migkit-test-perf-mydst", 15806)


def _sql(name, sql, db=None):
    cmd = ["docker", "exec", "-i", name, "mysql", "-uroot", "-ptest", "-N",
           "-B"] + (["-D", db] if db else [])
    got = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    try:
        for name, port in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"{port}:3306", "mysql:8.4"], check=True,
                           capture_output=True)
        for name, _ in (SRC, DST):
            for _ in range(90):
                if subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                                   "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                   "-e", "select 1"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(2)
            _sql(name, "create database app; use app;"
                       " create table t (id int primary key, k int,"
                       " pad varchar(80), key t_k (k));"
                       " set session cte_max_recursion_depth = 1000000;"
                       " insert into t with recursive s(g) as (select 1"
                       " union all select g + 1 from s where g < 300000)"
                       " select g, g % 50000, repeat('x', 60) from s;"
                       " analyze table t;")
        for _ in range(30):
            _sql(SRC[0], "select count(*) from t where k = 777;"
                         " select max(k) from t;", db="app")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine

    def ep(port):
        return Endpoint(host="127.0.0.1", port=port, user="root",
                        password="test")
    hop = Hop(name="perf", engine="mysql", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def test_the_same_indexes_answer_as_well(pair, tmp_path):
    from migkit import workload
    got = workload.compare(_engine(tmp_path), "app")
    assert got.status == "ok", got.detail
    assert "2 of the source's busiest reads planned on both sides, 2 timed" \
        in got.detail, got.detail


def test_an_index_that_did_not_come_over_is_found_and_timed(pair, tmp_path):
    from migkit import workload
    _sql(DST[0], "alter table t drop index t_k", db="app")
    try:
        got = workload.compare(_engine(tmp_path), "app")
        assert got.status == "warn", got.detail
        assert "reads t whole on the target where the source uses index" \
               " t_k" in got.detail, got.detail
        # the digest is shown, not the sample with its literal
        assert "777" not in got.detail, got.detail
    finally:
        _sql(DST[0], "alter table t add index t_k (k)", db="app")
