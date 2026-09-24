"""`check --consistent` holds each side's snapshot for a bounded time.

A consistent pass reads every table of a side under one snapshot, and for
as long as it runs the source cannot clean up after anything newer: vacuum
on PostgreSQL waits for it. `snapshot_limit` (seconds, a hop option) ends
the pass once a side has held its snapshot that long, releases it, and
says so rather than giving a verdict. Two more things were found while
bounding it:
* the lanes that share a snapshot to read in parallel were fed their
  scripts one after another, so they read in turn
* the source's exported snapshot stayed open until the target's side had
  finished as well
"""
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _eng(pg_pair, tmp_path, **options):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sl", engine="postgres", workers=2, options=options,
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _seed(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.a (id int primary key, v text);"
                   " create table public.b (id int primary key, v text);"
                   " insert into public.a select g, md5(g::text)"
                   "  from generate_series(1, 20000) g;"
                   " insert into public.b select g, md5(g::text)"
                   "  from generate_series(1, 20000) g")


def _snapshot_holders(pg_pair):
    return psql(pg_pair["src"], "select count(*) from pg_stat_activity"
                                " where backend_xmin is not null and pid"
                                " <> pg_backend_pid()").stdout.strip()


def test_a_pass_past_its_limit_is_ended_and_says_so(pg_pair, tmp_path,
                                                    monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    _seed(pg_pair)
    eng = _eng(pg_pair, tmp_path, snapshot_limit=1)
    real = PostgresEngine._snapshot_scripts

    def slow(self, side, db, tables, snap, lanes, w):
        # a table that takes longer than the limit to read
        return [sc.replace("commit;", "select pg_sleep(5); commit;")
                for sc in real(self, side, db, tables, snap, lanes, w)]

    monkeypatch.setattr(PostgresEngine, "_snapshot_scripts", slow)
    began = time.time()
    got = [r for r in eng.check_data("postgres", consistent=True)
           if r.check == "data"]
    assert time.time() - began < 5, "the limit did not end the pass"
    assert [r.status for r in got] == ["error"], [r.__dict__ for r in got]
    assert "snapshot_limit" in got[0].detail, got[0].detail
    assert "nothing it read is a verdict" in got[0].detail, got[0].detail
    # and the snapshot is let go: nothing of the pass holds the source back
    for _ in range(20):
        if _snapshot_holders(pg_pair) == "0":
            break
        time.sleep(0.5)
    assert _snapshot_holders(pg_pair) == "0"


def test_without_a_limit_the_pass_is_unchanged(pg_pair, tmp_path):
    _seed(pg_pair)
    got = [r for r in _eng(pg_pair, tmp_path).check_data(
        "postgres", consistent=True) if r.check == "data"]
    assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]


def test_the_lanes_read_at_the_same_time(pg_pair, tmp_path, monkeypatch):
    """Two lanes that each sleep 2s finish in about 2s, not 4."""
    from migkit.engines.postgres import PostgresEngine
    _seed(pg_pair)
    real = PostgresEngine._snapshot_scripts

    def sleepy(self, side, db, tables, snap, lanes, w):
        return [sc.replace("commit;", "select pg_sleep(2); commit;")
                for sc in real(self, side, db, tables, snap, lanes, w)]

    monkeypatch.setattr(PostgresEngine, "_snapshot_scripts", sleepy)
    began = time.time()
    got = [r for r in _eng(pg_pair, tmp_path).check_data(
        "postgres", consistent=True) if r.check == "data"]
    took = time.time() - began
    assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]
    # two sides, two lanes each: 4s when the lanes run together, 8s in turn
    assert took < 6.5, f"{took:.1f}s"


def test_the_source_is_let_go_before_the_target_is_read(pg_pair, tmp_path,
                                                        monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    _seed(pg_pair)
    real = PostgresEngine._snapshot_scripts
    seen = {}

    def watch(self, side, db, tables, snap, lanes, w):
        if side == "dst":
            # the target's lanes are about to run: is the source still held
            seen["holders"] = _snapshot_holders(pg_pair)
        return real(self, side, db, tables, snap, lanes, w)

    monkeypatch.setattr(PostgresEngine, "_snapshot_scripts", watch)
    got = [r for r in _eng(pg_pair, tmp_path).check_data(
        "postgres", consistent=True) if r.check == "data"]
    assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]
    assert seen.get("holders") == "0", seen
