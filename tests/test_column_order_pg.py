"""A different physical column order is not a data difference.

A source that has lived through a `DROP COLUMN` and an `ADD COLUMN` stores its
columns in a different physical order from a target created fresh out of the
same logical schema. Every value matches; `t::text` renders them in attribute
order, so the row hashes do not. migkit used to report that as a difference,
and there is nothing an operator can do about it - the tables are equal.

The mirror of this is in `test_column_order_mysql.py`: the MySQL engine always
named its columns and so never had the defect, and both files assert the same
guarantee so neither engine can lose it quietly.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-colorder"
PORT = 15491


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
                        "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
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
    yield
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)


def _engine(tmp_path, checksum=None):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"srcdb": "dstdb"},
              options={"checksum": checksum} if checksum else {})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


ROWS = 30


def _fresh():
    """Same values on both sides, stored in a different physical order."""
    for db in ("srcdb", "dstdb"):
        _sql("postgres", f"drop database if exists {db}")
        _sql("postgres", f"create database {db}")
    # the source carries the scars of its history
    _sql("srcdb", "create table t (id int primary key, junk text,"
                  " name text, amount numeric);"
                  " alter table t drop column junk;"
                  " alter table t add column note text;")
    # the target was created from the schema as it stands today
    _sql("dstdb", "create table t (id int primary key, note text,"
                  " name text, amount numeric);")
    for db in ("srcdb", "dstdb"):
        _sql(db, f"insert into t (id, name, amount, note)"
                 f" select g, 'n'||g, g * 1.5, 'note'||g"
                 f" from generate_series(1,{ROWS}) g")
        assert _sql(db, "select count(*) from t") == str(ROWS)
    orders = [_sql(db, "select string_agg(attname, ',' order by attnum)"
                       " from pg_attribute where attrelid = 't'::regclass"
                       " and attnum > 0 and not attisdropped")
              for db in ("srcdb", "dstdb")]
    assert orders[0] != orders[1], f"the premise failed: both are {orders[0]}"


def _line(tmp_path, checksum=None):
    rc, out = _engine(tmp_path, checksum)._data_fast_native("srcdb")
    line = [l for l in out.splitlines() if l.startswith("public.t:")]
    assert line, out
    return rc, line[0]


def test_the_whole_row_cast_really_does_disagree(pg):
    """The measurement the fix rests on. Without this, a later "simplify it
    back to t::text" looks harmless."""
    _fresh()
    h = ("select coalesce(sum(('x'||substr(md5(t::text),1,16))"
         "::bit(64)::bigint::numeric), 0) from t t")
    assert _sql("srcdb", h) != _sql("dstdb", h)
    # while the values themselves are equal column by column
    rows = ("select string_agg(id||'/'||name||'/'||amount||'/'||note,"
            " ',' order by id) from t")
    assert _sql("srcdb", rows) == _sql("dstdb", rows)


def test_a_reordered_target_is_not_reported_as_a_difference(pg, tmp_path):
    _fresh()
    rc, line = _line(tmp_path)
    assert rc == 0 and ": OK" in line, line


def test_the_same_holds_for_the_jsonb_checksum(pg, tmp_path):
    _fresh()
    rc, line = _line(tmp_path, checksum="jsonb")
    assert rc == 0 and ": OK" in line, line


def test_a_real_difference_is_still_caught(pg, tmp_path):
    """The fix must not have bought agreement by hashing less."""
    _fresh()
    _sql("dstdb", "update t set name = 'tampered' where id = 3")
    rc, line = _line(tmp_path)
    assert rc == 1 and "kind=values-changed" in line, line


def test_a_missing_row_is_still_caught(pg, tmp_path):
    _fresh()
    _sql("dstdb", "delete from t where id = 4")
    rc, line = _line(tmp_path)
    assert rc == 1 and "kind=rows-missing" in line, line


def test_a_null_is_not_confused_with_an_empty_string(pg, tmp_path):
    """Naming columns changes how NULLs render, so the distinction the text
    cast gave for free has to be re-proved."""
    _fresh()
    _sql("srcdb", "update t set note = null where id = 5")
    _sql("dstdb", "update t set note = '' where id = 5")
    rc, line = _line(tmp_path)
    assert rc == 1 and ": DIFF" in line, line


def test_a_renamed_column_fails_loudly_rather_than_quietly(pg, tmp_path):
    """The expression is named from the source and run on both sides, so a
    column the target does not have is an error - not a hash that silently
    compares equal to something else."""
    _fresh()
    _sql("dstdb", "alter table t rename column note to remark")
    rc, out = _engine(tmp_path)._data_fast_native("srcdb")
    assert rc != 0, out
    assert "public.t" in out and "ERROR" in out.upper(), out


def test_the_drilldown_agrees_with_the_fast_path(pg, tmp_path):
    """Both used to hash differently - the fast path by attribute order, the
    drilldown through to_jsonb. One rule now, so they cannot disagree about
    whether a reordered target is equal."""
    _fresh()
    eng = _engine(tmp_path)
    fast = eng._row_hash_expr("src", "srcdb", "public.t")
    assert "ROW(" in fast and "t::text" not in fast, fast
    results = eng.check_data("srcdb")
    bad = [r for r in results if r.status == "diff"]
    assert not bad, [r.detail for r in bad]
