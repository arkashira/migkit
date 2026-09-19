"""Mover leftovers in a real PostgreSQL source.

This is the only check that looks at the source on its own rather than
comparing it with the target - so nothing on the target can tell you the
finding is there, which is exactly why it was missed for so long.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-leftover"
PORT = 15463


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


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _sql(db, sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", NAME,
                        "psql", "-U", "postgres", "-d", db, "-At",
                        "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pg():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16", "-c", "wal_level=logical"],
                   check=True, capture_output=True)
    assert _wait(PORT)
    for _ in range(45):
        r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", NAME,
                            "psql", "-U", "postgres", "-c", "select 1"],
                           capture_output=True)
        if r.returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never accepted a connection")
    _sql("postgres", "create database srcdb")
    yield
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="l", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"srcdb": "srcdb"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    return eng


def _clean():
    _sql("srcdb", "drop schema if exists __tencentdb__ cascade")
    for pub in _sql("srcdb", "select pubname from pg_publication").splitlines():
        if pub:
            _sql("srcdb", f'drop publication "{pub}"')
    for s in _sql("postgres", "select slot_name from pg_replication_slots"
                              ).splitlines():
        if s:
            _sql("postgres", f"select pg_drop_replication_slot('{s}')")


def _rows(tmp_path):
    return _engine(tmp_path)._mover_leftovers()


def test_a_clean_source_says_so_with_a_count(pg, tmp_path):
    _clean()
    rows = _rows(tmp_path)
    assert len(rows) == 1, rows
    assert rows[0]["level"] == "pass"
    assert "objects examined" in rows[0]["detail"]
    # the count has to be real, or "clean" means "we looked at nothing".
    # A fresh PostgreSQL has pg_catalog, information_schema, public, pg_toast
    # and the plpgsql extension, so four is the floor.
    n = int(rows[0]["detail"].split()[0])
    assert n >= 4, rows[0]["detail"]


def test_a_leftover_schema_is_found_and_the_mover_named(pg, tmp_path):
    _clean()
    _sql("srcdb", "create schema __tencentdb__")
    try:
        rows = _rows(tmp_path)
        warn = [r for r in rows if r["level"] == "warn"]
        assert warn, rows
        assert "Tencent DTS" in warn[0]["detail"]
        assert "schema __tencentdb__" in warn[0]["detail"]
    finally:
        _sql("srcdb", "drop schema __tencentdb__ cascade")


def test_a_replication_slot_is_reported_as_active_harm(pg, tmp_path):
    """Not untidiness. It pins WAL on a source everybody has moved away
    from, and the disk fills up months later."""
    _clean()
    _sql("postgres", "select pg_create_logical_replication_slot("
                     "'dts_leftover_slot', 'pgoutput')")
    try:
        assert _sql("postgres", "select count(*) from pg_replication_slots"
                                " where slot_name='dts_leftover_slot'") == "1"
        rows = _rows(tmp_path)
        fails = [r for r in rows if r["level"] == "fail"]
        assert fails, rows
        assert "dts_leftover_slot" in fails[0]["item"]
        assert "run out of disk" in fails[0]["detail"]
    finally:
        _sql("postgres",
             "select pg_drop_replication_slot('dts_leftover_slot')")


def test_an_application_schema_is_not_called_litter(pg, tmp_path):
    _clean()
    _sql("srcdb", "create schema reporting")
    try:
        rows = _rows(tmp_path)
        assert all(r["level"] == "pass" for r in rows), rows
    finally:
        _sql("srcdb", "drop schema reporting cascade")


def test_assess_includes_the_finding(pg, tmp_path):
    _clean()
    _sql("srcdb", "create schema __tencentdb__")
    try:
        items = _engine(tmp_path).assess()
        hits = [i for i in items if i["scope"] == "source leftovers"]
        assert hits, [i["scope"] for i in items]
        assert any("Tencent DTS" in i["detail"] for i in hits), hits
    finally:
        _sql("srcdb", "drop schema __tencentdb__ cascade")
