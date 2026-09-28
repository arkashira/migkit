"""A MySQL 8.4 and a PostgreSQL 16 server, and a hop between them either
way, for the tests that hold values of one type to what they are on the
other engine (docs/research/type-fidelity-2026-09-28.md).

One pair for every such test file, started by whichever file runs first in
a session and removed at its end, so no two pairs are up at once.
"""
import socket
import subprocess
import time

import pytest

MY, PG = "migkit-test-f0t-my", "migkit-test-f0t-pg"
MY_PORT, PG_PORT = 16060, 16061


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def my(sql, db="cx"):
    """MySQL's answer to `sql`, tab-separated, or an assertion naming why
    not. Sent on stdin, as UTF-8 (`--default-character-set`: without it
    the client negotiates latin1 and `café` lands as five characters)."""
    got = _sh("docker", "exec", "-i", MY, "mysql", "-uroot", "-ptest",
              "--default-character-set=utf8mb4", "-N", "-B",
              *(["-D", db] if db else []), input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def pg(sql, db="cx"):
    got = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d",
              db, "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def pg_fails(sql, db="cx"):
    """The error PostgreSQL gives for `sql`, which must fail."""
    got = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d",
              db, "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode != 0, got.stdout
    return got.stderr


def my_fails(sql, db="cx"):
    got = _sh("docker", "exec", "-i", MY, "mysql", "-uroot", "-ptest",
              "--default-character-set=utf8mb4", "-N", "-B",
              *(["-D", db] if db else []), input=sql)
    assert got.returncode != 0, got.stdout
    return got.stderr


def _up(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


@pytest.fixture(scope="session")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    for n in (MY, PG):
        _sh("docker", "rm", "-f", "-v", n)
    try:
        _sh("docker", "run", "-d", "--name", MY, "-e",
            "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{MY_PORT}:3306",
            "mysql:8.4")
        _sh("docker", "run", "-d", "--name", PG, "-e",
            "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{PG_PORT}:5432",
            "postgres:16")
        assert _up(MY_PORT) and _up(PG_PORT)
        for _ in range(90):
            if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
                   "-h127.0.0.1", "--protocol=tcp", "-e",
                   "select 1").returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        for _ in range(60):
            if _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
                   "select 1").returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("postgres never answered")
        yield
    finally:
        for n in (MY, PG):
            _sh("docker", "rm", "-f", "-v", n)


@pytest.fixture
def fresh(servers):
    """An empty `cx` database on both servers."""
    my("drop database if exists cx; create database cx", db=None)
    pg("drop database if exists cx with (force)", db="postgres")
    pg("create database cx", db="postgres")
    yield


class Checkpoint(dict):
    """The shape `move` passes in: a dict that can save itself."""

    def save(self):
        pass


def hop(tmp_path, source="postgres", target="mysql", **options):
    """A hop between the pair, `source` -> `target`, reporting under
    `tmp_path`."""
    from migkit.config import Endpoint, Hop
    ends = {"postgres": Endpoint(host="127.0.0.1", port=PG_PORT,
                                 user="postgres", password="test"),
            "mysql": Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test")}
    h = Hop(name="f0t", engine="hetero", source=ends[source],
            target=ends[target], databases=["cx"],
            options={"source_engine": source, "target_engine": target,
                     **options})
    h.report_dir = lambda db=None: tmp_path
    return h


def engine(tmp_path, source="postgres", target="mysql", **options):
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(hop(tmp_path, source, target, **options))


def verdict(eng, table):
    """The one data verdict for `table`."""
    got = [r for r in eng.check_data("cx")
           if r.check == "data" and r.scope == f"cx.{table}"]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def move(eng, table, schema=""):
    """One table through the copier every pair shares, and what it said."""
    said = []
    eng.move_table("cx", schema, table, 1000, Checkpoint(), said.append)
    return said
