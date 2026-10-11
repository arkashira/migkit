"""PostgreSQL tells a difference that is still arriving from one that is
wrong (`Engine._resolve_inflight`, `fenced_recheck`; P0-PARITY.verify).

A check taken while the target is catching up finds rows apart that are
only late. Before a table is called different, the engine notes where the
source's log is, waits until the hop's own slot has confirmed flushing past
it (`fence_wait`) and reads the rows apart again, twice: a row that
arrived is healed, one that did not is a real difference. The slot is the
hop's own and by name - another consumer's, caught up, says nothing about
this target - and a fence that never passes (a stream stopped) leaves the
difference standing, never calls it healed.
"""
import threading
import time

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.confirm", "postgres")]

HOP = "confirm_arrives"
SLOT = "migkit_" + HOP


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    ep = lambda side: Endpoint(host="127.0.0.1", port=pg_pair[side],  # noqa
                               user="postgres", password="test")
    hop = Hop(name=HOP, engine="postgres", source=ep("src"),
              target=ep("dst"), databases=["postgres"],
              options={"fence_timeout": 3})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _sql(pg_pair, side, sql):
    got = psql(pg_pair[side], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _setup(pg_pair):
    """The same table on both sides, a slot of the hop's own on the source
    made before the change that is late, and the change made on the source
    only."""
    for side in ("src", "dst"):
        _sql(pg_pair, side, "drop table if exists public.late;"
                            " create table public.late (id int primary key,"
                            " v text);"
                            " insert into public.late values (1, 'a'),"
                            " (2, 'b'), (3, 'c')")
    _sql(pg_pair, "src", f"select pg_create_logical_replication_slot("
                         f"'{SLOT}', 'pgoutput')")
    _sql(pg_pair, "src", "update public.late set v = 'late' where id = 2")


def _teardown(pg_pair):
    _sql(pg_pair, "src", "select pg_drop_replication_slot(slot_name) from"
                         f" pg_replication_slots where slot_name = '{SLOT}'")
    for side in ("src", "dst"):
        _sql(pg_pair, side, "drop table if exists public.late")


def _caught_up(pg_pair):
    """What the stream does when the target has the change: the target
    gets it, and the hop's slot confirms the log up to now."""
    _sql(pg_pair, "dst", "update public.late set v = 'late' where id = 2")
    _sql(pg_pair, "src", f"select pg_replication_slot_advance('{SLOT}',"
                         " pg_current_wal_lsn())")


@needs_docker
def test_a_row_that_arrives_while_the_fence_waits_is_not_a_difference(
        pg_pair, tmp_path):
    eng = _engine(pg_pair, tmp_path)
    _setup(pg_pair)
    try:
        # the check's own pass finds the table different, as the late row
        # has not arrived
        first = eng.check_data("postgres", "public.late")
        assert [r.status for r in first] == ["diff"], first
        arrives = threading.Timer(1.5, _caught_up, args=(pg_pair,))
        arrives.start()
        began = time.monotonic()
        got = eng.check_data("postgres")
        arrives.join()
        data = [r for r in got if r.check == "data"]
        assert [r.status for r in data] == ["ok"], data
        assert "in-flight" in data[0].detail, data[0].detail
        assert "fence lsn=" in data[0].detail and "passed" in \
            data[0].detail, data[0].detail
        # it waited for the stream rather than reading again at once
        assert time.monotonic() - began >= 1.0
    finally:
        _teardown(pg_pair)


@needs_docker
def test_a_row_that_does_not_arrive_stays_a_difference(pg_pair, tmp_path):
    eng = _engine(pg_pair, tmp_path)
    _setup(pg_pair)
    try:
        # the log is confirmed past the change and the target still lacks
        # it: the fence passes and the row is wrong, not late
        _sql(pg_pair, "src", f"select pg_replication_slot_advance("
                             f"'{SLOT}', pg_current_wal_lsn())")
        got = [r for r in eng.check_data("postgres") if r.check == "data"]
        assert [r.status for r in got] == ["diff"], got
        assert "public.late" in got[0].detail
        assert "in-flight" not in got[0].detail
    finally:
        _teardown(pg_pair)


@needs_docker
def test_a_fence_that_never_passes_does_not_call_a_difference_late(
        pg_pair, tmp_path):
    eng = _engine(pg_pair, tmp_path)
    _setup(pg_pair)
    try:
        # the stream stopped: the slot stays where it was, the fence times
        # out (3 s) and the difference stands
        began = time.monotonic()
        got = [r for r in eng.check_data("postgres") if r.check == "data"]
        assert [r.status for r in got] == ["diff"], got
        assert time.monotonic() - began >= 3.0
        # and the keys of the row apart are kept for a repair to read
        assert eng._suspect_keys("postgres", "public.late") == {"2"}
    finally:
        _teardown(pg_pair)
