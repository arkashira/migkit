"""The counters' plan measures what it reseeds with (backlog
AUD-Z.seq-rate): the head-room above the highest id was the run's margin or
nothing, so `decide.sequence_headroom`'s `measured` rung was held only on
facts a test made up. Now PostgreSQL's and MySQL's `repair_plan` read the
source's counters twice a measured time apart (and against the engine's
last plan of the database, a cutover's gate before the freeze) for each
counter's insert rate, and the cutover hands the writers' restart gap it
measured - its write outage so far, or the last run's recorded one - so a
counter is raised by the rows the source inserts while the writers move.
The docker test keeps a writer inserting on the source through the switch
and a second one writing on the target after it: no id is handed out
twice."""
import threading
import time

import pytest

from migkit import cutover
from migkit.engines import postgres as pg
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine
from tests.conftest import needs_docker, psql
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("sequence_headroom", "measured", "holds"),
    ("sequence_headroom", "measured", "refuses"),
    ("sequence_headroom", "none", "holds"),
]


class _Clock:
    def __init__(self, *at):
        self.at = list(at)

    def __call__(self):
        return self.at.pop(0) if len(self.at) > 1 else self.at[0]


def _pg_fake(reads):
    eng = object.__new__(PostgresEngine)
    reads = list(reads)

    def run(side, db, sql):
        return reads.pop(0) if side == "src" else "public.t_id_seq|1"
    eng._psql = run
    eng._seq_owned = lambda side, db: set()
    eng._seq_col_max = lambda side, db, owned: {}
    eng._noise = lambda: ""
    eng._seq_left_out = lambda side, db, names: set()
    return eng


def _gap(seconds):
    token = pg.RESTART_GAP.set(seconds)
    return token


def test_postgres_a_counter_that_advances_is_raised_by_rate_times_gap(
        monkeypatch):
    # 25 ids in the 2 measured seconds: 12.5 a second; 3.1s of restart
    # gap lands 38.75 rows, so 39 above the later read
    monkeypatch.setattr(pg.time, "monotonic", _Clock(0.0, 2.0))
    eng = _pg_fake(["public.t_id_seq|100", "public.t_id_seq|125"])
    token = _gap(3.1)
    try:
        with watched() as seen:
            plan = eng.repair_plan("app", "sequences")
    finally:
        pg.RESTART_GAP.reset(token)
    stmt = plan[0].statements[0]
    assert "setval('public.t_id_seq', 164, true)" in stmt, stmt
    assert 164 - 125 > 12.5 * 3.1
    assert branch(seen, "measured") == [HOLDS]


def test_postgres_a_quiet_source_is_reseeded_to_its_highest_id(monkeypatch):
    monkeypatch.setattr(pg.time, "monotonic", _Clock(0.0, 2.0))
    eng = _pg_fake(["public.t_id_seq|100", "public.t_id_seq|100"])
    token = _gap(30.0)
    try:
        with watched() as seen:
            plan = eng.repair_plan("app", "sequences")
    finally:
        pg.RESTART_GAP.reset(token)
    assert "setval('public.t_id_seq', 100, true)" in plan[0].statements[0]
    assert branch(seen, "measured") == [REFUSES]
    assert branch(seen, "none") == [HOLDS]


def test_the_last_plans_read_measures_the_rate_across_the_freeze(
        monkeypatch):
    # the gate read 100 at 0s; the counters, after the freeze, 160 at 6s
    # twice: 10 a second, though the source stood still between them
    monkeypatch.setattr(pg.time, "monotonic",
                        _Clock(0.0, 0.0, 6.0, 6.0))
    eng = _pg_fake(["public.t_id_seq|100", "public.t_id_seq|100",
                    "public.t_id_seq|160", "public.t_id_seq|160"])
    eng.repair_plan("app", "sequences")
    token = _gap(2.0)
    try:
        plan = eng.repair_plan("app", "sequences")
    finally:
        pg.RESTART_GAP.reset(token)
    assert "setval('public.t_id_seq', 180, true)" in plan[0].statements[0]


def test_mysql_a_counter_that_advances_is_raised_by_rate_times_gap(
        monkeypatch):
    eng = object.__new__(MySQLEngine)
    reads = [({"t": 100}, {"t": 50}), ({"t": 110}, {"t": 50})]
    eng._counters = lambda db: reads.pop(0)
    eng._d = lambda side, db: db
    monkeypatch.setattr(pg.time, "monotonic", _Clock(0.0, 1.0))
    token = _gap(2.0)
    try:
        plan = eng.repair_plan("app", "sequences")
    finally:
        pg.RESTART_GAP.reset(token)
    assert "auto_increment = 130;" in plan[0].statements[0], plan
    assert plan[0].undo == ["alter table `app`.`t` auto_increment = 50;"]


def test_the_cutover_hands_the_larger_measured_restart_gap(monkeypatch):
    rec = {"clock": {"began": time.time() - 5.0}}
    monkeypatch.setattr(cutover, "last_run", lambda hop: {"outage": 8.25})
    assert cutover._restart_gap(None, rec) == 8.25
    monkeypatch.setattr(cutover, "last_run", lambda hop: None)
    assert 5.0 <= cutover._restart_gap(None, rec) < 6.0
    assert cutover._restart_gap(None, {}) is None

    class _Eng:
        def repair_plan(self, d, kind):
            seen.append(pg.RESTART_GAP.get())
            return []
    seen = []
    monkeypatch.setattr(cutover, "_save", lambda hop, rec: None)
    hop = type("H", (), {"report_dir": lambda self, d=None: None})()
    cutover._counters(hop, _Eng(), "app", {}, {"clock": {}, "counters": {}},
                      lambda m: None)
    assert seen == [None] and pg.RESTART_GAP.get() is None


@needs_docker
def test_writers_inserting_through_the_switch_hand_out_no_id_twice(
        pg_pair, tmp_path):
    from migkit.config import Endpoint, Hop
    for side in ("src", "dst"):
        psql(pg_pair[side], "drop table if exists seqrate;"
                            " create table seqrate (id serial primary key)")
    hop = Hop(name="seqrate", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    eng = PostgresEngine(hop)
    # the writer stops at an instant both sides read on the same clock
    ends = time.time() + 3.0
    writer = threading.Thread(target=psql, args=(
        pg_pair["src"], "do $$ begin while clock_timestamp() <"
        f" to_timestamp({ends}) loop"
        " insert into seqrate default values; commit;"
        " perform pg_sleep(0.01); end loop; end $$"))
    writer.start()
    time.sleep(0.5)
    eng.repair_plan("postgres", "sequences")        # the cutover's gate
    time.sleep(1.0)
    # the source's writers stop where the writer ends: that is the gap
    token = pg.RESTART_GAP.set(max(0.0, ends - time.time()))
    try:
        plan = eng.repair_plan("postgres", "sequences")
    finally:
        pg.RESTART_GAP.reset(token)
    for a in plan:
        eng.apply("postgres", a)
    writer.join()
    psql(pg_pair["dst"], "insert into seqrate select from"
                         " generate_series(1, 50)")
    ids = {s: {int(x) for x in psql(pg_pair[s], "select id from seqrate")
               .stdout.split()} for s in ("src", "dst")}
    assert ids["src"] and len(ids["dst"]) == 50
    assert not ids["src"] & ids["dst"], sorted(ids["src"] & ids["dst"])[:5]
    assert min(ids["dst"]) > max(ids["src"])
