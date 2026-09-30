"""A version down, PostgreSQL 17 into 14 (backlog P0-SCENARIOS): what the
newer source uses that the older target has not is found before anything
is written, a copy of the pages is refused up front with its reason, and
the plan names the case.

Before: the logical copy started, and stopped half way on the first
`CREATE UNIQUE INDEX ... NULLS NOT DISTINCT` the older server could not
parse - or, for a view with `security_invoker`, made it and read with the
owner's rights.
"""
import subprocess
import time

import pytest

from tests.conftest import needs_docker

pytestmark = needs_docker

SRC = ("migkit-test-down-pg-src", 16400, "postgres:17")
DST = ("migkit-test-down-pg-dst", 16401, "postgres:14")


def psql(name, db, sql):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", db, "-v",
                          "ON_ERROR_STOP=1", "-At", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for name, port, image in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{port}:5432", image], check=True,
                       capture_output=True)
    try:
        for name, _, _ in (SRC, DST):
            for _ in range(90):
                ok = subprocess.run(["docker", "exec", name, "pg_isready",
                                     "-U", "postgres", "-h", "127.0.0.1"],
                                    capture_output=True)
                if ok.returncode == 0:
                    break
                time.sleep(1)
            time.sleep(2)
            psql(name, "postgres", "create database app")
        psql(SRC[0], "app", "create table plain (id int primary key,"
                            " v text)")
        yield
    finally:
        for name, _, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path, options=None):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="down-pg", engine="postgres", goal="cutover",
              source=Endpoint(host="127.0.0.1", port=SRC[1],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=DST[1],
                              user="postgres", password="test"),
              databases=["app"], options=options or {})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _eng(hop):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(hop)


def test_what_the_newer_source_uses_is_refused_before_the_move(
        servers, tmp_path):
    from migkit import case, physical, ready
    hop = _hop(tmp_path)
    rows = case.checks(hop, _eng(hop), ["app"], quick=True)
    named = {r["item"]: r for r in rows}["what the two servers are"]
    assert named["detail"].startswith("a version down: 17."), named
    assert " to 14." in named["detail"]
    # nothing the older target lacks is used yet: nothing refused
    fails = [r for r in rows if r["level"] == "fail"]
    assert not fails, fails
    psql(SRC[0], "app", "create table orders (id int primary key,"
                        " code text); create unique index orders_code on"
                        " orders (code) nulls not distinct; create view"
                        " mine with (security_invoker = true) as select *"
                        " from orders")
    rows = case.checks(hop, _eng(hop), ["app"], quick=True)
    by = {r["item"]: r for r in rows}
    nnd = by["unique indexes with NULLS NOT DISTINCT"]
    assert nnd["level"] == "fail" and "public.orders_code" in nnd["detail"]
    assert "has none (it has them from 15)" in nnd["detail"], nnd
    inv = by["views run with the caller's rights (security_invoker)"]
    assert inv["level"] == "fail" and "public.mine" in inv["detail"]
    # the gate says them before the first write, each with its fix
    stops = [str(i) for i in ready.stops(hop, _eng(hop), ["app"], "full",
                                         False)]
    assert any("NULLS NOT DISTINCT" in s for s in stops), stops
    assert any("security_invoker" in s for s in stops), stops
    # a copy of the pages the hop allows: refused up front, and why
    phys = _hop(tmp_path, {"physical": {"data": "/var/lib/postgresql/data",
                                        "run": "sh -c", "stop": "true",
                                        "start": "true"}})
    by = {r["item"]: r for r in case.checks(phys, _eng(phys), ["app"])}
    assert "refused before anything runs" in by["a copy of the pages"][
        "detail"]
    lines = case.plan_lines(phys, _eng(phys), "app")
    assert lines[0].startswith("the case: a version down: 17."), lines
    assert any("a copy of the pages is refused" in ln for ln in lines)
    rung, why = physical.choose(phys, _eng(phys), "full+cdc")
    assert rung is None
    assert "read only by the major version that wrote them" in why, why
