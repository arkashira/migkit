"""Three range workers load one table of a PostgreSQL target under one
rule for its triggers (P0-DEPTH.triggers): the hold is read once and given
back once (`TableHold`), a trigger on the target counts no firing while
rows load and fires again after, the load's connections run as a replica
and the source's export connection never does, and a user who may not set
`session_replication_role` is refused before a row is written - or loads
once the grant is given (the rung read off the user's privilege, not a
constant).

On the shared PostgreSQL 16 pair, tables named for this module alone."""
import os
import threading

import psycopg
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import pg_pass
from migkit.engines.postgres import PostgresEngine
from tests.conftest import needs_docker, psql

PROVES = [("verify.structure.triggers", "postgres")]

pytestmark = needs_docker

TABLE, LOG, ROLE = "w16e_trig_t", "w16e_trig_log", "w16e_loader"
SCHEMA = (f"create table {TABLE} (id int primary key, v text);"
          f" create table {LOG} (n bigint);"
          " create or replace function w16e_count() returns trigger"
          f" language plpgsql as $$ begin insert into {LOG} values"
          " (new.id); return new; end $$;"
          f" create trigger w16e_count after insert on {TABLE}"
          " for each row execute function w16e_count()")
WORKERS, EACH, WAIT = 3, 100, 60


def _hop(pg_pair, user="postgres"):
    return Hop(name="w16e", engine="postgres",
               source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                               user="postgres", password="test"),
               target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                               user=user, password="test"),
               databases=["postgres"])


@pytest.fixture(autouse=True)
def client_programs(monkeypatch):
    """The target's privilege is asked through the client program; where
    the package keeps it off the path, put it on."""
    for bin_ in ("/opt/homebrew/opt/libpq/bin", "/usr/local/opt/libpq/bin"):
        if os.path.isdir(bin_):
            monkeypatch.setenv("PATH", bin_ + os.pathsep + os.environ["PATH"])


@pytest.fixture
def target(pg_pair):
    dst = pg_pair["dst"]
    psql(dst, f"drop table if exists {TABLE}, {LOG} cascade;"
              f" drop role if exists {ROLE}")
    assert psql(dst, SCHEMA).returncode == 0
    yield pg_pair
    psql(dst, f"drop table if exists {TABLE}, {LOG} cascade;"
              f" drop owned by {ROLE}; drop role if exists {ROLE}")


def _count(port, table):
    return int(psql(port, f"select count(*) from {table}").stdout.strip())


class Spy:
    """Counts the hold's reads of the table's triggers and the window it
    opens and closes, on one engine."""

    def __init__(self, eng):
        self.reads, self.opened, self.closed = 0, 0, 0
        read, window = eng.write_states, eng.load_window

        def states(side, db, table):
            self.reads += 1
            return read(side, db, table)

        def opened(db, log=None, tables=None):
            self.opened += 1
            inner = window(db, log, tables)

            class Counted:
                def __enter__(s):
                    return inner.__enter__()

                def __exit__(s, *exc):
                    self.closed += 1
                    return inner.__exit__(*exc)
            return Counted()
        eng.write_states, eng.load_window = states, opened


def _workers(eng, ids, inside):
    """`WORKERS` threads, each in the table's hold writing its ids through
    the target's write connection; `inside(sessions, roles)` runs once all
    of them have written and are still holding. Returns the errors."""
    wrote = threading.Barrier(WORKERS + 1, timeout=WAIT)
    leave = threading.Barrier(WORKERS + 1, timeout=WAIT)
    errors, sessions, roles = [], [], []

    def work(n):
        try:
            with eng.table_hold("postgres", f"public.{TABLE}") as session:
                sessions.append(session)
                with eng._connect("dst", "postgres", "write") as conn:
                    roles.append(conn.execute(
                        "show session_replication_role").fetchone()[0])
                    for i in ids(n):
                        conn.execute(f"insert into {TABLE} values (%s, %s)",
                                     (i, f"r{n}"))
                    conn.commit()
                wrote.wait()
                leave.wait()
        except BaseException as e:  # noqa: BLE001 - reported to the test
            errors.append(e)
            wrote.abort()
            leave.abort()
    threads = [threading.Thread(target=work, args=(n,), daemon=True)
               for n in range(WORKERS)]
    for t in threads:
        t.start()
    try:
        wrote.wait()
        inside(sessions, roles)
    except threading.BrokenBarrierError:
        pass
    finally:
        try:
            leave.wait()
        except threading.BrokenBarrierError:
            pass
        for t in threads:
            t.join(WAIT)
    return errors


def test_three_workers_hold_once_the_trigger_is_quiet_then_fires_again(
        target):
    eng = PostgresEngine(_hop(target))
    spy = Spy(eng)
    seen = {}

    def inside(sessions, roles):
        seen["rows"] = _count(target["dst"], TABLE)
        seen["fired"] = _count(target["dst"], LOG)
        seen["replica"] = list(eng.__dict__.get("_as_replica", ()))
        seen["roles"] = list(roles)
        seen["sessions"] = list(sessions)
        # the source's export connection, opened while the target is held
        export = pg_pass._Export(eng, "postgres", "pg_current_snapshot")
        try:
            seen["export"] = export.conn.execute(
                "show session_replication_role").fetchone()[0]
        finally:
            export.close()
        src = eng._connect("src", "postgres", "ask")
        try:
            seen["read"] = src.execute(
                "show session_replication_role").fetchone()[0]
        finally:
            src.close()

    errors = _workers(eng, lambda n: range(n * EACH, (n + 1) * EACH), inside)
    assert not errors, errors
    # during: all rows in, no firing, every writer under the one rule
    assert seen["rows"] == WORKERS * EACH
    assert seen["fired"] == 0
    assert seen["roles"] == ["replica"] * WORKERS
    assert seen["replica"] == ["postgres"]
    assert all(s == {"_as_replica": ["postgres"]} for s in seen["sessions"])
    # the source's sessions only read: never a replica
    assert seen["export"] == "origin" and seen["read"] == "origin"
    assert "session_replication_role" not in pg_pass.LOAD_OPTIONS
    # the hold: read once before, once after; opened once, closed once
    assert (spy.reads, spy.opened, spy.closed) == (2, 1, 1)
    assert not eng.__dict__.get("_as_replica")
    # after: the trigger is as it was, and fires for the next write
    with eng._connect("dst", "postgres", "write") as conn:
        assert conn.execute(
            "show session_replication_role").fetchone()[0] == "origin"
        conn.execute(f"insert into {TABLE} values (%s, %s)", (10_000, "after"))
        conn.commit()
    assert _count(target["dst"], LOG) == 1


def test_a_second_pass_of_workers_holds_once_again(target):
    """Held, given back, held again: the table's state is read afresh each
    time, never carried from the first pass."""
    eng = PostgresEngine(_hop(target))
    spy = Spy(eng)
    for first in (0, 1_000):
        errors = _workers(eng, lambda n, f=first: range(
            f + n * EACH, f + (n + 1) * EACH), lambda *_: None)
        assert not errors, errors
    assert (spy.reads, spy.opened, spy.closed) == (4, 2, 2)
    assert _count(target["dst"], TABLE) == 2 * WORKERS * EACH
    assert _count(target["dst"], LOG) == 0


def test_a_user_who_may_not_load_as_a_replica_is_refused_until_granted(
        target):
    dst = target["dst"]
    assert psql(dst, f"create role {ROLE} login password 'test';"
                     f" grant all on {TABLE}, {LOG} to {ROLE}"
                ).returncode == 0
    eng = PostgresEngine(_hop(target, user=ROLE))
    # the table has a trigger and the user may neither set the role nor
    # turn the trigger off: stopped before a row is written, grant named
    with pytest.raises(SystemExit) as refused:
        with eng.table_hold("postgres", f"public.{TABLE}"):
            pytest.fail("the hold was given to a user that may not")
    assert "GRANT SET ON PARAMETER session_replication_role" in str(
        refused.value)
    assert _count(dst, TABLE) == 0 and _count(dst, LOG) == 0
    assert not eng.__dict__.get("_as_replica")
    # with the grant the same user loads as a replica: the rung is read
    # off what the user may do
    assert psql(dst, f"grant set on parameter session_replication_role"
                     f" to {ROLE}").returncode == 0
    eng = PostgresEngine(_hop(target, user=ROLE))
    with eng.table_hold("postgres", f"public.{TABLE}"):
        with eng._connect("dst", "postgres", "write") as conn:
            conn.execute(f"insert into {TABLE} values (1, 'x')")
            conn.commit()
    assert _count(dst, TABLE) == 1 and _count(dst, LOG) == 0
    with psycopg.connect(host="127.0.0.1", port=dst, dbname="postgres",
                         user=ROLE, password="test", autocommit=True) as c:
        c.execute(f"insert into {TABLE} values (2, 'y')")
    assert _count(dst, LOG) == 1
