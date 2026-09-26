"""A pair of engines verifies only the rows its source changed (plan 18).

The source's change log is read from the last clean position without
moving anything on the source (a MySQL binlog here). The keys it names are
asked of both sides and compared in the one rendering both render to. A
row the target did not follow is named, and the position does not move
until it does. A source whose log is read through the tail's slot is not
read here, and says why.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

pytestmark = [pytest.mark.docker]

MY, MY_PORT = "migkit-test-pairdelta-my", 15841


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
def mysql():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8.4", "--binlog-row-metadata=FULL"], check=True,
                   capture_output=True)
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
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _pair(src, dst, src_ep, dst_ep, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="pd", engine="hetero",
              options={"source_engine": src, "target_engine": dst},
              source=src_ep, target=dst_ep, databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_only_the_changed_rows_are_asked_and_one_is_not_followed(
        mysql, pg_pair, tmp_path):
    dst = pg_pair["dst"]
    psql(dst, "drop database if exists cx")
    assert psql(dst, "create database cx").returncode == 0
    _my("create table t (id int primary key, v varchar(20), n decimal(8,2));"
        " insert into t values (1, 'a', 1.50), (2, 'b', 2.00), (3, 'c', 3)")
    psql(dst, "create table t (id bigint primary key, v varchar(20),"
              " n numeric(8,2)); insert into t values (1, 'a', 1.50),"
              " (2, 'b', 2.00), (3, 'c', 3)", db="cx")
    eng = _pair("mysql", "postgres",
                Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                         password="test"),
                Endpoint(host="127.0.0.1", port=dst, user="postgres",
                         password="test"), tmp_path)
    first = eng.delta_verify("cx")
    assert first[0].detail.startswith("baseline recorded"), first[0].detail
    _my("insert into t values (4, 'd', 4.40); update t set v = 'B' where"
        " id = 2; delete from t where id = 3")
    # the target follows two of the three
    psql(dst, "insert into t values (4, 'd', 4.40); delete from t where"
              " id = 3", db="cx")
    got = {r.scope: r for r in eng.delta_verify("cx")}
    assert got["cx.t"].status == "diff", got["cx.t"].__dict__
    assert got["cx.t"].detail == ("of 3 changed rows: missing=0 extra=0"
                                  " changed=1"), got["cx.t"].detail
    assert "NOT advanced" in got["cx"].detail, got["cx"].detail
    # asked again, since the position did not move; once followed, clean
    psql(dst, "update t set v = 'B' where id = 2", db="cx")
    got = {r.scope: r for r in eng.delta_verify("cx")}
    assert got["cx.t"].status == "ok", got["cx.t"].__dict__
    assert "advanced" in got["cx"].detail and "NOT" not in got["cx"].detail
    # nothing changed since: nothing to ask
    got = eng.delta_verify("cx")
    assert got[0].detail.startswith("0 changed rows"), got[0].detail


def test_a_source_read_through_the_tails_slot_is_not_read_here(pg_pair,
                                                               tmp_path):
    ep = Endpoint(host="127.0.0.1", port=pg_pair["src"], user="postgres",
                  password="test")
    got = _pair("postgres", "mysql", ep,
                Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                         password="test"), tmp_path).delta_verify("cx")
    assert [r.status for r in got] == ["error"], got
    assert "the slot is the tail's" in got[0].detail, got[0].detail
