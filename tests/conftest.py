import os
import socket
import subprocess
import time

import pytest


def _free(host, port):
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) != 0


def _wait_tcp(host, port, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex((host, port)) == 0:
                return True
        time.sleep(2)
    return False


def _have_docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _mk_docker_mark():
    skip = pytest.mark.skipif(not _have_docker(),
                              reason="docker not available")
    return pytest.mark.docker(skip)


needs_docker = _mk_docker_mark()


@pytest.fixture(scope="session")
def pg_pair():
    if not _have_docker():
        pytest.skip("docker not available")
    names = ["migkit-test-pg-src", "migkit-test-pg-dst"]
    ports = [55432, 55433]
    for n in names:
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    for n, p in zip(names, ports):
        subprocess.run(
            ["docker", "run", "-d", "--name", n, "-e",
             "POSTGRES_PASSWORD=test", "-p", f"{p}:5432", "postgres:16",
             "-c", "wal_level=logical"], check=True, capture_output=True)
    for p in ports:
        assert _wait_tcp("127.0.0.1", p), f"pg on {p} did not come up"
    time.sleep(3)
    yield {"src": ports[0], "dst": ports[1]}
    for n in names:
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def psql(port, sql, db="postgres"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test",
         f"migkit-test-pg-{'src' if port == 55432 else 'dst'}",
         "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
        capture_output=True, text=True)


_WIPE = """
do $$ declare r record; begin
  for r in select tablename from pg_tables where schemaname = 'public'
  loop execute 'drop table if exists public.'||quote_ident(r.tablename)||' cascade'; end loop;
  for r in select sequencename from pg_sequences where schemaname = 'public'
  loop execute 'drop sequence if exists public.'||quote_ident(r.sequencename)||' cascade'; end loop;
end $$;
"""


@pytest.fixture(autouse=True)
def clean_pg(request):
    """Drop all user tables/sequences on both DBs before each pg test, so
    the shared session containers never leak state between tests."""
    if "pg_pair" not in request.fixturenames:
        yield
        return
    ports = request.getfixturevalue("pg_pair")
    for p in (ports["src"], ports["dst"]):
        psql(p, _WIPE)
    yield
