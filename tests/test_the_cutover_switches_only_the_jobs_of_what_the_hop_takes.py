"""The cutover switches only the jobs that write what the hop takes
(backlog P0-SCOPE.6a).

With `cutover: {jobs: move}`, every job the source runs - a MySQL event, a
pg_cron job - was switched off on the source before the freeze and on on
the target at the flip. One that writes only a table the hop leaves out (a
queue the target keeps for itself, a log table the job was told to leave
behind) belongs to whatever writes that table: switched off, the source's
own writer lost it; switched on, it wrote on the target a table the move
never took. Such a job is now left as it is on both sides, and said.

A job is read by its text against the tables and routines of its
database: it is left out only where every table it names is one the hop
leaves out. One that names none, calls a routine or runs a statement built
as text may write anything and stays the hop's, as every job was; a hop
that leaves nothing out reads nothing more than before.
"""
import pytest

from migkit import cutover, jobs
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine
from tests import mysql_pair
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

EVENTS = [("purge_queue", "ENABLED",
           "DELETE FROM `pick_dispatch_queue` WHERE at < now()"),
          ("roll_orders", "ENABLED", "update orders set n = n + 1"),
          ("both", "ENABLED",
           "insert into orders select * from pick_dispatch_queue"),
          ("by_routine", "ENABLED",
           "begin delete from pick_dispatch_queue; call tidy(); end"),
          ("by_text", "ENABLED", "begin prepare s from @q; execute s; end"),
          ("named_nothing", "ENABLED", "do sleep(1)"),
          ("off_already", "DISABLED", "delete from pick_dispatch_queue")]


class _MySQL(Engine):
    """A MySQL side as the jobs read it: its events, tables and routines."""
    CANON_ENGINE = "mysql"

    def __init__(self, hop):
        super().__init__(hop)
        self.asked = []

    def _d(self, side, db):
        return db

    def _all_tables(self, side, db):
        return ["orders", "pick_dispatch_queue"]

    def _q(self, side, sql, args=None, **kw):
        self.asked.append(sql)
        if "information_schema.events" in sql:
            return [(n, s, "app@%", b) for n, s, b in EVENTS]
        if "information_schema.routines" in sql:
            return [("tidy",)]
        if sql.startswith("select version()"):
            return [("8.4.0",)]
        return []


def _hop(tmp_path=None, **scope):
    hop = Hop(name="jb", engine="mysql", source=Endpoint(),
              target=Endpoint(), databases=["shop"], **scope)
    if tmp_path is not None:
        hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def test_a_job_that_writes_only_what_the_hop_leaves_out_is_left_alone():
    hop = _hop(exclude=["pick_dispatch_queue"])
    eng = _MySQL(hop)
    assert jobs.inventory(hop, eng, "shop", "src") == [
        ("roll_orders", True), ("both", True), ("by_routine", True),
        ("by_text", True), ("named_nothing", True)]
    # a job switched off already is not one the cutover would switch
    assert jobs.left_alone(hop, eng, "shop", "src") == ["purge_queue"]


def test_a_hop_that_leaves_nothing_out_takes_every_job_as_before():
    hop = _hop()
    eng = _MySQL(hop)
    got = jobs.inventory(hop, eng, "shop", "src")
    assert [n for n, _ in got] == [n for n, _, _ in EVENTS]
    # and reads nothing more than the events themselves
    assert len(eng.asked) == 1, eng.asked


def test_the_switch_and_the_preflight_leave_it_alone(tmp_path):
    hop = _hop(tmp_path, exclude=["pick_dispatch_queue"])
    eng = _MySQL(hop)
    running = [n for n, on in jobs.inventory(hop, eng, "shop", "src") if on]
    said = []
    assert jobs.switch(hop, eng, "shop", "src", running, False,
                       said.append) == len(running)
    altered = [q for q in eng.asked if q.startswith("alter")]
    assert len(altered) == len(running), altered
    assert not [q for q in altered if "purge_queue" in q], altered
    assert {e["name"] for e in jobs.recorded(hop)} == set(running)
    rows = cutover._job_rows(hop, eng, "shop", {"jobs": "move"})
    left = [r for r in rows if "left as they are" in r[1]]
    assert left and left[0][0] == "ok" and "purge_queue" in left[0][1], rows
    assert not [r for r in rows if "running on the source" in r[1]
                and "purge_queue" in r[1]], rows


class _Postgres(Engine):
    """A PostgreSQL side as the jobs read it, pg_cron installed."""
    CANON_ENGINE = "postgres"

    def _d(self, side, db):
        return db

    def _all_tables(self, side, db):
        return ["public.orders", "audit.event_log"]

    def _psql(self, side, db, sql, **kw):
        if "cron.database_name" in sql:
            return "postgres"
        if "pg_extension" in sql:
            return "1"
        if "from cron.job" in sql:
            return "\n".join("\x1f".join(j) for j in (
                ("1", "trim_log", "t",
                 "delete from audit.event_log where at < now() - interval"
                 " '30 days'"),
                ("2", "refresh", "t", "select refresh_totals()"),
                ("3", "vacuum_orders", "t", "vacuum analyze public.orders")))
        if "pg_proc" in sql:
            return "refresh_totals"
        raise AssertionError(sql)


def test_a_pg_cron_job_is_read_the_same_way():
    hop = _hop(include=["shop.public.*"])
    hop.engine = "postgres"
    eng = _Postgres(hop)
    assert jobs.inventory(hop, eng, "shop", "src") == [
        ("refresh", True), ("vacuum_orders", True)]
    assert jobs.left_alone(hop, eng, "shop", "src") == ["trim_log"]


# -- MySQL, its own events ------------------------------------------------------

@pytest.fixture
def events(my_pair):
    my("src", "drop database if exists jb; create database jb; use jb;"
              " create table pick_dispatch_queue (id int primary key);"
              " create table orders (id int primary key, v int);"
              " create event purge_queue on schedule every 1 day do"
              " delete from pick_dispatch_queue where id < 0;"
              " create event roll_orders on schedule every 1 day do"
              " update orders set v = v where id < 0;")
    yield
    my("src", "drop database if exists jb")


def test_mysql_events_of_a_table_the_hop_leaves_out_stay_on(events,
                                                            tmp_path):
    eng = mysql_pair.engine("jb")
    eng.hop.exclude = ["pick_dispatch_queue"]
    eng.hop.report_dir = lambda db=None, _p=tmp_path: _p
    hop = eng.hop
    assert jobs.inventory(hop, eng, "jb", "src") == [("roll_orders", True)]
    assert jobs.left_alone(hop, eng, "jb", "src") == ["purge_queue"]
    jobs.switch(hop, eng, "jb", "src", ["roll_orders"], False, print)
    got = my("src", "select event_name, status from information_schema"
                    ".events where event_schema = 'jb' order by 1")
    assert got.split() == ["purge_queue", "ENABLED",
                           "roll_orders", "SLAVESIDE_DISABLED"] or \
        got.split() == ["purge_queue", "ENABLED",
                        "roll_orders", "REPLICA_SIDE_DISABLED"], got
    jobs.undo(hop, eng, print, "src")
