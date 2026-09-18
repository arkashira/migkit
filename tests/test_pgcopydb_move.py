"""The whole-database copy, through the version-matched pgcopydb container.

The container rather than a local binary, and the reason is measured:
Homebrew's pgcopydb 0.18 is compiled against PostgreSQL 18 and emits
`SET transaction_timeout = 0`, a parameter PostgreSQL 16 has never heard of.
The statement fails, the transaction aborts, every later statement in it is
refused, and a whole-database clone moves zero rows while logging each
rejection - success reported, nothing copied. The published image is compiled
against 16 and states its compatible range.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-pgc-src", "migkit-test-pgc-dst"
SRC_PORT, DST_PORT = 15433, 15434


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _image():
    from migkit import movers
    return movers.pgcopydb_available()


pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not _docker(), reason="docker not available"),
    pytest.mark.skipif(not _image(),
                       reason="dimitri/pgcopydb image not pulled"),
]


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _sql(name, db, sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                        "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "run", "-d", "--name", n,
                        "-e", "POSTGRES_PASSWORD=test", "-p", f"{p}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        for _ in range(45):
            r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", n,
                                "psql", "-U", "postgres", "-c", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
        _sql(n, "postgres", "create database app")
    _sql(SRC, "app", "create table t (id serial primary key, v text,"
                     " n numeric)")
    _sql(SRC, "app", "insert into t (v, n) select md5(g::text), g * 1.5"
                     " from generate_series(1,20000) g")
    _sql(SRC, "app", "create index idx_v on t (v)")
    _sql(SRC, "app", "select lo_from_bytea(0, '\\x48656c6c6f'::bytea)")
    _sql(SRC, "app", "select lo_from_bytea(0, '\\xdeadbeef'::bytea)")
    assert _sql(SRC, "app", "select count(*) from t") == "20000"
    assert _sql(SRC, "app",
                "select count(*) from pg_largeobject_metadata") == "2"
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _hop(tmp_path):
    from migkit.config import Endpoint, Hop
    # One address that works from both sides, which is the whole premise:
    # migkit truncates from here and pgcopydb copies from inside a container
    # sharing the host's network, so they must be dialling the same thing.
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              user="postgres", password="test"),
              db_map={"app": "app"})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def test_a_dry_run_executes_nothing(pair, tmp_path):
    from migkit import movers
    steps = movers.pgcopydb_move(_hop(tmp_path), "app", 4, False, None)
    assert any("dry-run" in s for s in steps), steps
    assert _sql(DST, "app", "select count(*) from information_schema.tables"
                            " where table_schema='public'") == "0"


def test_the_printed_command_does_not_leak_the_password(pair, tmp_path):
    from migkit import movers
    steps = movers.pgcopydb_move(_hop(tmp_path), "app", 4, False, None)
    joined = " ".join(steps)
    assert "PGCOPYDB_SOURCE_PGURI" in joined
    # the password is the word before the @, which is exactly where a
    # split-on-@ redaction leaves it
    assert ":test@" not in joined, joined
    assert "***@" in joined, joined
    # and the endpoints must survive, or the printed command is useless
    assert f"127.0.0.1:{SRC_PORT}" in joined, joined
    assert f"127.0.0.1:{DST_PORT}" in joined, joined


def test_it_copies_the_rows_into_a_schema_that_already_exists(pair, tmp_path):
    """migkit prepares the target schema in its own step, so a mover always
    meets a target that is not empty. That is why this is `copy table-data`
    and not `clone`."""
    from migkit import movers
    _sql(DST, "app", "create table t (id serial primary key, v text,"
                     " n numeric)")
    _sql(DST, "app", "create index idx_v on t (v)")
    assert _sql(DST, "app", "select count(*) from t") == "0"

    movers.pgcopydb_move(_hop(tmp_path), "app", 4, True, None)
    assert _sql(DST, "app", "select count(*) from t") == "20000"


def test_running_it_twice_does_not_double_the_rows(pair, tmp_path):
    """A data-only load that appends leaves every row twice, so the target is
    emptied first - the same truncate the dump path uses, from one place."""
    from migkit import movers
    movers.pgcopydb_move(_hop(tmp_path), "app", 4, True, None)
    assert _sql(DST, "app", "select count(*) from t") == "20000"


def test_migkit_agrees_the_copy_is_equal(pair, tmp_path):
    """The mover's own claim is not the evidence; migkit's checksum is."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              user="postgres", password="test"),
              db_map={"app": "app"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    rc, out = eng._data_fast_native("app")
    assert rc == 0, out
    assert "rows=20000" in out, out


def test_an_unknown_mover_name_refuses(pair, tmp_path):
    """A dispatch that has never been taught a name must stop rather than
    fall through to whatever happens to be first."""
    from migkit import movers
    with pytest.raises(SystemExit, match="no bulk path"):
        movers.run_via("invented", _hop(tmp_path), "app", 2, False, None)


def test_the_logged_command_does_not_leak_the_password_either(pair, tmp_path):
    """`_sh` echoes the argv it is handed, and the argv carries both URIs -
    so the log line has to be built from the redacted form, not the real one.
    """
    from migkit import movers
    seen = []
    movers.pgcopydb_move(_hop(tmp_path), "app", 2, True, seen.append)
    assert seen, "nothing was logged, so this proves nothing"
    joined = " ".join(seen)
    assert ":test@" not in joined, joined
    assert "***@" in joined, joined

