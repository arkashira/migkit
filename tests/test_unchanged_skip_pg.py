"""Skipping tables that have not moved, end to end.

Two properties, pulling opposite ways. The first run must read everything and
the second must be allowed to read nothing - that is the whole point. But a
table that changed between the two runs has to be read no matter how quiet its
neighbours were, and a skipped table must never be reported in a way that
reads like a fresh verdict.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-skip"
PORT = 15441


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
    yield
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="s", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"srcdb": "dstdb"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    return eng


def _fresh():
    for db in ("srcdb", "dstdb"):
        _sql("postgres", "select pg_terminate_backend(pid) from"
                         " pg_stat_activity where datname = %r"
                         " and pid <> pg_backend_pid()" % db)
        _sql("postgres", f"drop database if exists {db}")
        _sql("postgres", f"create database {db}")
        _sql(db, "create table quiet (id int primary key, v text);"
                 " create table busy (id int primary key, v text);"
                 " insert into quiet select g,'q'||g"
                 " from generate_series(1,50) g;"
                 " insert into busy select g,'b'||g"
                 " from generate_series(1,50) g")
        assert _sql(db, "select count(*) from quiet") == "50"


def _run(tmp_path):
    time.sleep(1.2)          # the statistics collector is not synchronous
    return _engine(tmp_path)._data_fast_native("srcdb")


def test_the_first_run_reads_everything(pg, tmp_path):
    _fresh()
    rc, out = _run(tmp_path)
    assert rc == 0, out
    assert "UNCHANGED" not in out, out
    assert out.count(": OK") == 2, out


def test_the_second_run_skips_what_did_not_move(pg, tmp_path):
    _fresh()
    rc, first = _run(tmp_path)
    assert rc == 0 and "UNCHANGED" not in first, first
    rc, second = _run(tmp_path)
    assert rc == 0, second
    assert second.count("UNCHANGED") == 2, second
    assert "2 of 2 tables unchanged" in second, second


def test_a_table_that_moved_is_read_even_when_its_neighbour_is_quiet(pg,
                                                                    tmp_path):
    _fresh()
    _run(tmp_path)
    _sql("srcdb", "update busy set v = 'moved' where id = 3")
    rc, out = _run(tmp_path)
    busy = [l for l in out.splitlines() if l.startswith("public.busy:")][0]
    quiet = [l for l in out.splitlines() if l.startswith("public.quiet:")][0]
    assert "UNCHANGED" not in busy, busy
    assert "DIFF" in busy, busy
    assert "UNCHANGED" in quiet, quiet
    assert rc == 1


def test_a_skipped_line_never_reads_like_a_fresh_verdict(pg, tmp_path):
    """Nothing was read, so the line must not look like something was."""
    _fresh()
    _run(tmp_path)
    _, out = _run(tmp_path)
    for line in out.splitlines():
        if "UNCHANGED" in line:
            assert ": OK" not in line, line
            assert "checksum=" not in line, line
            assert "no rows read this run" in line, line


def test_a_table_that_differed_is_read_again_next_run(pg, tmp_path):
    """Its proof was dropped, so the next run cannot skip a table already
    known to be wrong."""
    _fresh()
    _run(tmp_path)
    _sql("dstdb", "update busy set v = 'tampered' where id = 7")
    rc, out = _run(tmp_path)
    assert rc == 1 and "DIFF" in out
    # nothing changed between these two runs, yet busy must still be read
    rc2, again = _run(tmp_path)
    busy = [l for l in again.splitlines() if l.startswith("public.busy:")][0]
    assert "UNCHANGED" not in busy, busy
    assert "DIFF" in busy, busy


def test_the_consistent_pass_reads_every_table(pg, tmp_path):
    """The final proof cannot skip - it does not go through this path at
    all, which is stronger than a flag that could be forgotten."""
    _fresh()
    _run(tmp_path)
    _run(tmp_path)               # everything now has a proof on file
    rc, out = _engine(tmp_path)._fast_consistent("srcdb")
    assert "UNCHANGED" not in out, out
    assert out.count("public.quiet") >= 1 and out.count("public.busy") >= 1


def test_a_fully_quiet_run_is_a_pass_not_a_failure(pg, tmp_path):
    """Every table skipped used to exit 1, because failure was inferred from
    the absence of the word OK rather than named."""
    _fresh()
    rc, _ = _run(tmp_path)
    assert rc == 0
    rc, out = _run(tmp_path)
    assert rc == 0, out
    assert out.count("UNCHANGED") == 2, out


def test_the_summary_says_what_was_not_read(pg, tmp_path):
    """A count of tables that quietly omits the skipped ones overstates what
    this run proved."""
    _fresh()
    _engine(tmp_path).check_data("srcdb")
    time.sleep(1.2)
    res = _engine(tmp_path).check_data("srcdb")
    hit = [r for r in res if r.check == "data"][0]
    assert hit.status == "ok", hit.detail
    assert "unchanged since their last proof" in hit.detail, hit.detail


def test_merging_counts_forces_every_table_to_be_read(pg, tmp_path):
    """The row counts come out of this same pass, so a skipped table would
    make the count short by however many rows it holds."""
    _fresh()
    _engine(tmp_path).check_data("srcdb")
    time.sleep(1.2)
    res = _engine(tmp_path).check_data("srcdb", with_counts=True)
    data = [r for r in res if r.check == "data"][0]
    assert "unchanged" not in data.detail, data.detail
    assert "100 rows" in data.detail.replace(",", ""), data.detail
