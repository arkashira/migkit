"""The second reader's groups as a localizer of the verify passes, on a
MySQL to PostgreSQL pair (backlog 0f on R20's rungs).

A table past the row walk's cap (25,000 rows, `DRILL_CAP` 20,000) is
localized by `leaves.drill`. With the leaves already timed on this hop and
the groups not, the groups are the rung tried (`decide`: a rung never
timed goes before one that was): the reader's aggregates per group of
`g`, the rows of the differing group read back and held to the digests.
Where a difference is one the groups' measures cannot see, the rows do
not add up, and the leaves are read instead - nothing is lost either way.
"""
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import verdict
from tests.test_the_report_does_not_name_its_tools import TOOLS

READER = os.environ.get("MIGKIT_SECOND_READER_PYTHON") or str(
    Path(sys.prefix).parent / ".venv-dvt" / "bin" / "python")
MY, MY_PORT = "migkit-test-dvt-xmy", 16042
PG, PG_PORT = "migkit-test-dvt-xpg", 16043
LEAKS = TOOLS + ("dvt", "data-validation", "data_validation")
ROWS = 25_000


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(), reason="docker not available"),
              pytest.mark.skipif(not Path(READER).exists(),
                                 reason="the second reader is not installed")]


def _sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N"], input=sql, capture_output=True,
                         text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg(sql, db="app"):
    got = _sh("docker", "exec", PG, "psql", "-U", "postgres", "-d", db,
              "-v", "ON_ERROR_STOP=1", "-Atc", sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    _sh("docker", "rm", "-f", "-v", MY, PG)
    _sh("docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
        "-p", f"127.0.0.1:{MY_PORT}:3306", "mysql:8")
    _sh("docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
        "-p", f"127.0.0.1:{PG_PORT}:5432", "postgres:16")
    try:
        for _ in range(120):
            if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
                   "-h127.0.0.1", "--protocol=tcp", "-e",
                   "select 1").returncode == 0:
                break
            time.sleep(2)
        for _ in range(60):
            if _sh("docker", "exec", PG, "pg_isready", "-U",
                   "postgres").returncode == 0:
                break
            time.sleep(1)
        for port in (MY_PORT, PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        time.sleep(2)
        _pg("create database app", db="postgres")
        yield
    finally:
        _sh("docker", "rm", "-f", "-v", MY, PG)


def _seed(*changes):
    _my("drop database if exists app; create database app;"
        " create table app.t (id int primary key, g int, n bigint,"
        " s varchar(40), index (g));"
        " set cte_max_recursion_depth = 100000;"
        " insert into app.t with recursive r(i) as (select 1 union all"
        f" select i + 1 from r where i < {ROWS})"
        " select i, i % 10, i * 7, concat('v', i) from r;"
        " analyze table app.t;")
    _pg("drop table if exists t; create table t (id int primary key, g int,"
        " n bigint, s varchar(40)); insert into t select i, i % 10, i * 7,"
        f" 'v' || i from generate_series(1, {ROWS}) i")
    for sql in changes:
        _pg(sql)


def _engine(tmp_path):
    from migkit import decide
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="sr0fx", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["app"],
              options={"source_engine": "mysql",
                       "target_engine": "postgres"})
    hop.report_dir = lambda db=None: tmp_path
    # the leaves timed on this hop, the groups not: the groups are tried
    decide.HopCosts(hop, "verify passes/").saw("localize/leaf reads",
                                               ROWS, 1.0)
    return HeteroEngine(hop)


def _named(eng, kind):
    import json
    return sorted(json.loads(k)[0] for k in eng._read_drill("app", "t",
                                                             kind))


@pytest.mark.usefixtures("pair")
def test_the_differing_group_is_read_back_and_held_to_the_digests(
        tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_SECOND_READER_PYTHON", READER)
    _seed("update t set n = n + 1 where id = 13")
    eng = _engine(tmp_path)
    got = eng.check_data("app")
    data = verdict(got, "app.t")
    assert data.status == "diff", data.detail
    assert _named(eng, "changed") == ["13"], data.detail
    assert "narrowed by g to 1 of about 10 groups" in data.detail, data.detail
    assert "rows read back" in data.detail, data.detail
    assert "adds up to the whole difference" in data.detail, data.detail
    # the second reading of its rows, across engines value by value, read
    # only the group - and held key for key to the row named
    assert "the same 1 rows differ" in data.detail, data.detail
    said = " ".join(f"{r.detail} {r.fix_hint}" for r in got).lower()
    assert not [t for t in LEAKS if t in said], said


@pytest.mark.usefixtures("pair")
def test_what_the_groups_cannot_see_sends_the_leaves_to_it(tmp_path,
                                                          monkeypatch):
    """`v25` to `x25` moves no count, sum or minimum of the group, and a
    length is not compared across engines: group 5 passes, the rows of
    group 3 do not add up to the whole difference, and the leaves find
    both rows."""
    monkeypatch.setenv("MIGKIT_SECOND_READER_PYTHON", READER)
    _seed("update t set n = n + 1 where id = 13",
          "update t set s = 'x25' where id = 25")
    eng = _engine(tmp_path)
    data = verdict(eng.check_data("app"), "app.t")
    assert data.status == "diff", data.detail
    assert _named(eng, "changed") == ["13", "25"], data.detail
    assert "leaves that differ" in data.detail, data.detail
    assert "narrowed by g" not in data.detail, data.detail
