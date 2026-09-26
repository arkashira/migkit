"""The settings two different engines can be held to, and the target's
restore point, on a pair (backlog 0e).

Most of two servers' settings name different things. Two do not: what
the zone names mean, which the fingerprint reads the same way on both
engines, and what the text can hold. A MySQL column in 3-byte UTF-8 holds
no emoji; a PostgreSQL database in SQL_ASCII holds whatever bytes it is
sent and checks none of them.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MY, PG = "migkit-test-set-my", "migkit-test-set-pg"
MY_PORT, PG_PORT = 15818, 15819


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


def _pg(sql, db="postgres"):
    got = _sh("docker", "exec", PG, "psql", "-U", "postgres", "-d", db,
              "-At", "-v", "ON_ERROR_STOP=1", "-c", sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(sql):
    got = _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest", "-D", "cx",
              "-N", "-B", "-e", sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    for n in (MY, PG):
        _sh("docker", "rm", "-f", "-v", n)
    _sh("docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
        "-p", f"{MY_PORT}:3306", "mysql:8.4")
    _sh("docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
        "-p", f"{PG_PORT}:5432", "postgres:16")
    try:
        for _ in range(90):
            if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
                   "-h127.0.0.1", "--protocol=tcp", "-e",
                   "create database if not exists cx").returncode == 0:
                break
            time.sleep(2)
        for _ in range(60):
            if _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
                   "create database cx").returncode == 0:
                break
            time.sleep(2)
        for port in (MY_PORT, PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _pg("create database asc_db encoding 'SQL_ASCII' template template0"
            " locale 'C'")
        _my("create table t (id int primary key, full_text"
            " varchar(10) character set utf8mb4, short_text varchar(10)"
            " character set utf8mb3)")
        _pg("create table t (id bigint primary key, full_text text,"
            " short_text text)", db="cx")
        yield
    finally:
        for n in (MY, PG):
            _sh("docker", "rm", "-f", "-v", n)


def _hop(src, dst, tmp_path, db_map=None):
    from migkit.engines.hetero import HeteroEngine
    ends = {"mysql": Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
            "postgres": Endpoint(host="127.0.0.1", port=PG_PORT,
                                 user="postgres", password="test")}
    hop = Hop(name="set", engine="hetero",
              options={"source_engine": src, "target_engine": dst},
              source=ends[src], target=ends[dst], databases=["cx"],
              db_map=db_map or {})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _said(eng):
    return {r.scope: r for r in eng.check_params("cx")}


def test_a_target_that_holds_more_is_fine(pair, tmp_path):
    got = _said(_hop("mysql", "postgres", tmp_path))
    enc = got["cx text encoding"]
    assert enc.status == "ok", enc.__dict__
    assert "utf8mb3, utf8mb4" in enc.detail and "UTF8" in enc.detail, \
        enc.detail
    zones = got["cx time zone rules"]
    # both engines answer the fingerprint; nothing is skipped as unasked
    assert zones.check == "params" and zones.status != "skip", zones.__dict__


def test_a_target_column_short_of_what_the_source_holds(pair, tmp_path):
    enc = _said(_hop("postgres", "mysql", tmp_path))["cx text encoding"]
    assert enc.status == "diff", enc.__dict__
    assert "only characters inside the Basic Multilingual Plane" in \
        enc.detail, enc.detail


def test_a_target_that_checks_nothing(pair, tmp_path):
    enc = _said(_hop("mysql", "postgres", tmp_path,
                     {"cx": "asc_db"}))["cx text encoding"]
    assert enc.status == "warn", enc.__dict__
    assert "SQL_ASCII" in enc.detail and "checked against no encoding" in \
        enc.detail, enc.detail


def test_the_pair_keeps_the_targets_own_restore_point(pair, tmp_path):
    _hop("mysql", "postgres", tmp_path).snapshot_state("cx", tmp_path)
    kept = (tmp_path / "dst-schema.sql").read_text()
    assert "CREATE TABLE public.t" in kept, kept[:400]
