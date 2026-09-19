"""Naming the shape of a difference in the first pass.

A single row checksum says "this table differs" and stops there, so learning
whether rows were edited, replaced or lost costs another pass. Hashing the
primary key alongside the row - in the same scan, since the row is being read
anyway - answers it for free. The three shapes lead to different remedies,
which is why telling them apart early is worth anything at all.

This does not replace the drilldown: that is still how you learn *which*
rows. It removes the need for one in order to learn *what happened*.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-diffkind"
PORT = 15482


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


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def pg():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(PORT)
    time.sleep(3)
    yield
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


def _sql(db, sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", NAME,
                        "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="k", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"srcdb": "dstdb"})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _fresh(rows=20):
    for db in ("srcdb", "dstdb"):
        _sql("postgres", f"drop database if exists {db}")
        _sql("postgres", f"create database {db}")
        _sql(db, "create table t (id int primary key, payload text)")
        _sql(db, f"insert into t select g, 'v'||g"
                 f" from generate_series(1,{rows}) g")
        assert _sql(db, "select count(*) from t") == str(rows)


def _line(tmp_path):
    rc, out = _engine(tmp_path)._data_fast_native("srcdb")
    line = [l for l in out.splitlines() if l.startswith("public.t:")]
    assert line, out
    return rc, line[0]


def test_identical_tables_say_nothing_about_kinds(pg, tmp_path):
    _fresh()
    rc, line = _line(tmp_path)
    assert rc == 0 and ": OK" in line, line
    assert "kind=" not in line, line


def test_a_value_edited_in_place_is_named_values_changed(pg, tmp_path):
    _fresh()
    _sql("dstdb", "update t set payload = 'tampered' where id = 3")
    rc, line = _line(tmp_path)
    assert rc == 1
    assert "kind=values-changed" in line, line


def test_a_deleted_row_is_named_rows_missing_with_a_count(pg, tmp_path):
    _fresh()
    _sql("dstdb", "delete from t where id = 4")
    rc, line = _line(tmp_path)
    assert rc == 1
    assert "kind=rows-missing" in line and "by=1" in line, line


def test_an_added_row_is_named_rows_extra(pg, tmp_path):
    _fresh()
    _sql("dstdb", "insert into t values (999, 'ghost')")
    rc, line = _line(tmp_path)
    assert rc == 1
    assert "kind=rows-extra" in line and "by=1" in line, line


def test_a_swapped_key_is_named_rows_replaced(pg, tmp_path):
    """Same number of rows, different keys - which neither a row count nor a
    single row checksum can tell apart from an edit."""
    _fresh()
    _sql("dstdb", "delete from t where id = 5")
    _sql("dstdb", "insert into t values (500, 'v5')")
    rc, line = _line(tmp_path)
    assert rc == 1
    assert "kind=rows-replaced" in line, line


def test_a_table_with_no_primary_key_claims_no_kind(pg, tmp_path):
    """Without a key there is nothing to reason with, and inventing a verdict
    would be worse than admitting there is none."""
    _fresh()
    for db in ("srcdb", "dstdb"):
        _sql(db, "create table nk (a int, b text)")
        _sql(db, "insert into nk values (1, 'x')")
    _sql("dstdb", "update nk set b = 'y'")
    rc, out = _engine(tmp_path)._data_fast_native("srcdb")
    line = [l for l in out.splitlines() if l.startswith("public.nk:")][0]
    assert rc == 1 and ": DIFF" in line, line
    assert "kind=" not in line, line
