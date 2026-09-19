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

`_fast_consistent` now uses it: a side's tables are split across lanes,
every lane adopts the same snapshot, and the fence LSN is asked for by one
lane so there is a single position to prove convergence against. A side
that cannot export falls back to the single script it always ran - an
inconsistent "consistent" pass would be worse than a slow one.
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


def test_the_lanes_all_read_the_same_instant(tmp_path):
    """Splitting a side's tables across connections is only safe because
    every lane runs the same preamble. If one lane omitted it, that lane
    would read a different instant and the mode would be lying."""
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sn", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["x"], workers=3)
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._row_hash_expr = lambda side, db, t: "md5(t::text)"
    got = eng._snapshot_scripts("src", "x",
                                ["public.a", "public.b", "public.c"],
                                "SNAP", 3, 8)
    assert len(got) == 3, got
    for sc in got:
        assert "set transaction snapshot 'SNAP';" in sc, sc
        assert sc.rstrip().endswith("commit;"), sc


def test_only_one_lane_reports_the_fence(tmp_path):
    """The LSN is the fence a convergence proof rests on. Two lanes
    reporting two positions would leave the caller picking one."""
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sn", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["x"], workers=3)
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._row_hash_expr = lambda side, db, t: "md5(t::text)"
    got = eng._snapshot_scripts("src", "x", ["a.a", "b.b", "c.c"], "S", 3, 8)
    assert sum("LSN|" in sc for sc in got) == 1, got


def test_every_table_lands_in_exactly_one_lane(tmp_path):
    """A table in two lanes would be counted twice; a table in none would
    vanish from the verdict - the quieter of the two failures."""
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sn", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["x"], workers=4)
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._row_hash_expr = lambda side, db, t: "md5(t::text)"
    tables = [f"public.t{i}" for i in range(7)]
    got = eng._snapshot_scripts("src", "x", tables, "S", 4, 8)
    joined = "\n".join(got)
    for t in tables:
        assert joined.count(f"'{t}|'") == 1, (t, joined)


def test_more_lanes_than_tables_makes_no_empty_lane(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sn", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["x"], workers=8)
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._row_hash_expr = lambda side, db, t: "md5(t::text)"
    got = eng._snapshot_scripts("src", "x", ["public.only"], "S", 8, 8)
    assert len(got) == 1, got


def test_the_consistent_pass_still_agrees_with_itself(snap_db, tmp_path):
    """The end of it: same database on both sides of the hop, several
    lanes, and every table has to come back OK. A lane reading a different
    instant, or a table counted twice, shows up here."""
    eng = _engine(snap_db, tmp_path)
    subprocess.run(["docker", "exec", NAME, "psql", "-U", "postgres", "-d",
                    "postgres", "-q", "-c",
                    "create table if not exists c (id int);"
                    " create table if not exists d (id int);"
                    " insert into c select generate_series(1,50);"
                    " insert into d select generate_series(1,50);"],
                   check=True, capture_output=True)
    rc, out = eng._fast_consistent("postgres")
    assert rc == 0, out
    assert "consistent snapshot" in out, out
    for table in ("public.a", "public.b", "public.c", "public.d"):
        assert f"{table}: OK" in out, (table, out)
