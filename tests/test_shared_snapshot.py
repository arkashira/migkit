"""One instant, read by several connections.

migkit's fast data pass reads tables through a thread pool - one
connection per worker - so table `a` is read at one instant and table `b`
at another. A transaction that moves a row between them in that gap makes
both tables look wrong, on a pair where nothing is.

`--consistent` already answers this by reading every table of a side
inside one repeatable-read transaction, and pays for it by giving up the
parallelism. `pg_export_snapshot` is how both are had at once: one
transaction exports its snapshot, every worker adopts it, and they all
read the same instant while still reading at the same time.

Measured on PostgreSQL 16 with a writer inserting between the export and
the reads:

    workers not sharing the snapshot    a=2  b=2
    workers sharing the snapshot        a=1  b=1

**Not yet wired into the pass.** This is the mechanism and its proof; the
change to `_fast_consistent` that uses it is its own piece of work,
because it rewrites how that script is built.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

PORT = 15555
NAME = "migkit-test-snap"


@pytest.fixture(scope="module")
def snap_db():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    end = time.time() + 120
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", PORT)) == 0:
                break
        time.sleep(1)
    for _ in range(60):
        if subprocess.run(["docker", "exec", NAME, "pg_isready", "-U",
                           "postgres"], capture_output=True).returncode == 0:
            break
        time.sleep(1)
    else:
        pytest.fail("postgres never answered")
    subprocess.run(["docker", "exec", NAME, "psql", "-U", "postgres", "-d",
                    "postgres", "-q", "-c",
                    "create table a (id int); insert into a values (1);"
                    " create table b (id int); insert into b values (1);"],
                   check=True, capture_output=True)
    yield PORT
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


def _engine(port, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sn", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _read(port, table, snapshot=None):
    import psycopg2
    c = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                         password="test", dbname="postgres")
    try:
        cur = c.cursor()
        cur.execute("begin transaction isolation level repeatable read"
                    " read only")
        if snapshot:
            cur.execute(f"set transaction snapshot '{snapshot}'")
        cur.execute(f"select count(*) from {table}")
        return cur.fetchone()[0]
    finally:
        c.rollback()
        c.close()


def test_separate_workers_share_one_instant(snap_db, tmp_path):
    """The whole argument. Two connections, one writer between them, and
    the answer depends entirely on whether they adopted the snapshot."""
    eng = _engine(snap_db, tmp_path)
    conn, snapshot = eng._export_snapshot("src", "postgres")
    assert conn is not None and snapshot, (conn, snapshot)
    try:
        subprocess.run(["docker", "exec", NAME, "psql", "-U", "postgres",
                        "-d", "postgres", "-q", "-c",
                        "insert into a values (99); insert into b values (99)"],
                       check=True, capture_output=True)
        # the control: without it, the two workers see the write
        assert (_read(snap_db, "a"), _read(snap_db, "b")) == (2, 2)
        # with it, both see the instant the snapshot was taken
        assert (_read(snap_db, "a", snapshot),
                _read(snap_db, "b", snapshot)) == (1, 1)
    finally:
        conn.rollback()
        conn.close()
        subprocess.run(["docker", "exec", NAME, "psql", "-U", "postgres",
                        "-d", "postgres", "-q", "-c",
                        "delete from a where id=99; delete from b where id=99"],
                       capture_output=True)


def test_the_snapshot_dies_with_the_transaction_that_exported_it(snap_db,
                                                                   tmp_path):
    """Why the connection is returned rather than discarded: closing it is
    what makes the id useless, so the caller has to hold it."""
    import psycopg2
    eng = _engine(snap_db, tmp_path)
    conn, snapshot = eng._export_snapshot("src", "postgres")
    conn.rollback()
    conn.close()
    with pytest.raises(psycopg2.Error):
        _read(snap_db, "a", snapshot)


def test_the_preamble_adopts_the_snapshot_before_it_tunes(tmp_path):
    """`SET TRANSACTION SNAPSHOT` has to be the first statement of a
    transaction that has not read yet. Ordering the tuning first is not a
    style question - PostgreSQL refuses the snapshot."""
    from migkit.engines.postgres import PostgresEngine
    got = PostgresEngine._snapshot_preamble("00000003-00000005-1", 8)
    assert got[0].startswith("begin transaction isolation level"
                             " repeatable read read only")
    assert got[1] == "set transaction snapshot '00000003-00000005-1';"
    assert "max_parallel_workers_per_gather = 8" in got[2]


def test_that_ordering_is_what_the_server_requires(snap_db, tmp_path):
    """Asserted against the server rather than from memory: the wrong
    order is rejected, so the test above is pinning a real rule."""
    import psycopg2
    eng = _engine(snap_db, tmp_path)
    conn, snapshot = eng._export_snapshot("src", "postgres")
    try:
        c = psycopg2.connect(host="127.0.0.1", port=snap_db, user="postgres",
                             password="test", dbname="postgres")
        cur = c.cursor()
        cur.execute("begin transaction isolation level repeatable read"
                    " read only")
        cur.execute("select count(*) from a")          # a read first
        with pytest.raises(psycopg2.Error) as e:
            cur.execute(f"set transaction snapshot '{snapshot}'")
        assert "snapshot" in str(e.value).lower(), str(e.value)
        c.rollback()
        c.close()
    finally:
        conn.rollback()
        conn.close()


def test_a_server_that_cannot_be_reached_answers_none(tmp_path):
    """"I could not export one" has to be distinguishable from "here is
    one", or a caller would carry `None` into a SQL string."""
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sn", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    assert PostgresEngine(hop)._export_snapshot("src", "x") == (None, None)


def test_no_snapshot_still_produces_a_usable_preamble():
    """The fallback is the transaction migkit already opens, not a broken
    statement with `None` in it."""
    from migkit.engines.postgres import PostgresEngine
    got = PostgresEngine._snapshot_preamble(None, 4)
    assert not any("snapshot" in line for line in got), got
    assert len(got) == 2, got
