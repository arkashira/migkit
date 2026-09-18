"""The change marker, against a real PostgreSQL.

The unit tests defend the rules; this defends the premise. A marker is only
worth anything if it actually moves for every kind of change - and the one
that nearly got missed is TRUNCATE, which moves no tuple counter at all.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-marker"
PORT = 15443


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
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
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
    _sql("postgres", "create database m")
    _sql("m", "create table t (id int primary key, v text);"
              " insert into t select g,'x'||g from generate_series(1,100) g")
    assert _sql("m", "select count(*) from t") == "100"
    yield
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="m", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"m": "m"})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _marker(tmp_path):
    # the statistics collector is not synchronous; give it a moment to land
    time.sleep(1.2)
    m = _engine(tmp_path)._change_marker("src", "m", "public.t")
    assert m, "no marker was produced at all"
    return m


def test_a_quiet_table_keeps_the_same_marker(pg, tmp_path):
    a = _marker(tmp_path)
    b = _marker(tmp_path)
    assert a == b, (a, b)


def test_an_update_moves_it(pg, tmp_path):
    before = _marker(tmp_path)
    _sql("m", "update t set v = 'changed' where id = 5")
    assert _marker(tmp_path) != before


def test_a_delete_moves_it(pg, tmp_path):
    before = _marker(tmp_path)
    _sql("m", "delete from t where id = 6")
    assert _marker(tmp_path) != before


def test_a_truncate_moves_it_even_though_no_counter_does(pg, tmp_path):
    """The case that would have been missed. TRUNCATE increments no tuple
    counter - measured - so a marker built only from those would skip an
    emptied table and report it equal."""
    before = _marker(tmp_path)
    counters_before = _sql("m", "select n_tup_ins||'/'||n_tup_upd||'/'"
                                "||n_tup_del from pg_stat_all_tables"
                                " where relname='t'")
    _sql("m", "truncate t")
    time.sleep(1.2)
    counters_after = _sql("m", "select n_tup_ins||'/'||n_tup_upd||'/'"
                               "||n_tup_del from pg_stat_all_tables"
                               " where relname='t'")
    assert counters_before == counters_after, (
        "the tuple counters moved on TRUNCATE, so this test no longer proves "
        "why relfilenode is in the marker")
    assert _marker(tmp_path) != before


def test_a_statistics_reset_reads_as_changed(pg, tmp_path):
    """It lowers the counters. Comparing for equality catches it; comparing
    for growth would not."""
    _sql("m", "insert into t select g,'z'||g from generate_series(200,260) g")
    before = _marker(tmp_path)
    _sql("m", "select pg_stat_reset()")
    assert _marker(tmp_path) != before


def test_an_old_server_gets_no_marker_at_all(pg, tmp_path):
    """Before PostgreSQL 15 the statistics collector could drop a message,
    so there is nothing here worth trusting."""
    from migkit import unchanged as u
    assert not u.usable_postgres("14.11")
    eng = _engine(tmp_path)
    real = eng._psql("src", "m", "show server_version")
    assert u.usable_postgres(real), f"container is {real}, expected 15+"
