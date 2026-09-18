"""The generated undo, applied to a real server.

A rollback nobody has run is a rumour. These tests apply the forward script to
a live target, apply the generated undo, and check that the target is back
where it started - and separately, that the undo is honest about the one thing
it cannot restore.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-revert"
PORT = 15489


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


def _apply(db, path):
    """Run a generated script the way an operator would: as a file, stopping
    at the first error."""
    r = subprocess.run(["docker", "exec", "-i", "-e", "PGPASSWORD=test", NAME,
                        "psql", "-U", "postgres", "-d", db,
                        "-v", "ON_ERROR_STOP=1", "-f", "-"],
                       input=path.read_text(), capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-800:]


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


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="r", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"srcdb": "dstdb"})
    hop.report_dir = lambda db=None: tmp_path
    hop._report = tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    return eng


def _shape(db):
    """Enough of the schema to notice a change, in a stable order."""
    return _sql(db, """
        select string_agg(c.relname||':'||a.attname||':'||
                          format_type(a.atttypid, a.atttypmod), ','
                          order by c.relname, a.attname)
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        join pg_attribute a on a.attrelid = c.oid
        where n.nspname = 'public' and c.relkind = 'r'
          and a.attnum > 0 and not a.attisdropped""")


def _fresh(src_extra="", dst_extra=""):
    for db in ("srcdb", "dstdb"):
        # `results` holds its connections open and offers no way to close
        # them, so a previous test's handles are still attached here
        _sql("postgres", "select pg_terminate_backend(pid) from"
                         " pg_stat_activity where datname = %r"
                         " and pid <> pg_backend_pid()" % db)
        _sql("postgres", f"drop database if exists {db}")
        _sql("postgres", f"create database {db}")
        _sql(db, "create table t (id int primary key, name text);"
                 " insert into t values (1,'a'),(2,'b')")
        assert _sql(db, "select count(*) from t") == "2"
    if src_extra:
        _sql("srcdb", src_extra)
    if dst_extra:
        _sql("dstdb", dst_extra)


def _run(tmp_path):
    r = _engine(tmp_path).check_structural_diff("srcdb")
    return r, tmp_path / "structural-fix.sql", \
        tmp_path / "structural-fix.revert.sql"


def test_an_added_column_round_trips_exactly(pg, tmp_path):
    """The target is missing a column, migkit adds it, the undo removes it,
    and the target is byte-for-byte where it began."""
    _fresh(src_extra="alter table t add column extra int")
    before = _shape("dstdb")
    res, fix, rev = _run(tmp_path)
    assert res.status == "diff", res.detail
    assert fix.exists() and rev.exists(), res.detail
    _apply("dstdb", fix)
    assert _shape("dstdb") != before          # the fix did something
    assert "extra" in _shape("dstdb")
    _apply("dstdb", rev)
    assert _shape("dstdb") == before


def test_a_new_table_round_trips_exactly(pg, tmp_path):
    _fresh(src_extra="create table extra_t (id int primary key, v text)")
    before = _shape("dstdb")
    res, fix, rev = _run(tmp_path)
    assert rev.exists(), res.detail
    _apply("dstdb", fix)
    assert "extra_t" in _shape("dstdb")
    _apply("dstdb", rev)
    assert _shape("dstdb") == before


def test_an_additive_fix_says_the_undo_is_complete(pg, tmp_path):
    _fresh(src_extra="alter table t add column extra int")
    res, fix, rev = _run(tmp_path)
    head = rev.read_text()
    assert "returns the target to where it started" in head, head[:400]
    assert "undo was written alongside it" in res.detail
    assert "backup" not in res.detail, res.detail


def test_a_destructive_fix_refuses_to_claim_a_clean_rollback(pg, tmp_path):
    """The target has a column the source does not, so the repair drops it.
    The undo recreates the column - it cannot recreate the values, and it has
    to say so rather than let the reader assume."""
    _fresh(dst_extra="alter table t add column doomed text;"
                     " update t set doomed = 'keepme'")
    assert _sql("dstdb", "select count(*) from t where doomed = 'keepme'") \
        == "2"
    res, fix, rev = _run(tmp_path)
    assert rev.exists(), res.detail
    head = rev.read_text()
    assert "DROP COLUMN" in head.upper()
    assert "needs a backup, not this file" in head
    assert "take a backup first" in res.detail, res.detail

    _apply("dstdb", fix)
    assert _sql("dstdb", "select count(*) from information_schema.columns"
                         " where table_name='t' and column_name='doomed'") \
        == "0"
    _apply("dstdb", rev)
    # the column is back
    assert _sql("dstdb", "select count(*) from information_schema.columns"
                         " where table_name='t' and column_name='doomed'") \
        == "1"
    # and it is empty, exactly as the header warned
    assert _sql("dstdb", "select count(*) from t where doomed is not null") \
        == "0"


def test_no_difference_means_no_revert_file_left_lying_around(pg, tmp_path):
    """A stale revert from a previous run would be an undo for changes that
    were never made."""
    _fresh(src_extra="alter table t add column extra int")
    _, _, rev = _run(tmp_path)
    assert rev.exists()
    _sql("dstdb", "alter table t add column extra int")   # now identical
    res, fix, rev = _run(tmp_path)
    assert res.status == "ok", res.detail
    assert not fix.exists(), fix.read_text()
    assert not rev.exists(), rev.read_text()
    assert not (tmp_path / "structural-fix.locks.txt").exists()
