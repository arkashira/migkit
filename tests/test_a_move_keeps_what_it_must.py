"""A move keeps what it must (backlog P0-SAFETY-INTEGRITY).

What a table's move must keep is read before the move from the catalogue
and the engines' facts (`guarantees.read`): its exact columns, NULL apart
from an empty text, a field not there apart from NULL, duplicates where
it has no key, the tables its rows point at, the order its changes need.
What the target cannot keep is a gap, said. A schedule that runs work
side by side is admitted by the decision engine only where it keeps all
of it (`decide.admit`), and the joint plan of a move's tables is held to
it (`jointplan.barriered`, `jointplan.admitted`). Then the faults: a move
with foreign keys stopped at every place it keeps what it has done, and
rows deleted between the stop and the run after, end equal on the
sandbox's servers.
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

from migkit import decide, guarantees, jointplan
from migkit.catalog import Catalog, Column, ForeignKey, Index
from migkit.guarantees import Schedule
from tests.conftest import needs_docker, psql
from tests import servers

ROOT = Path(__file__).resolve().parent.parent


class _Engine:
    def __init__(self, **facts):
        for k, v in facts.items():
            setattr(self, k, v)


def _shop():
    """orders -> customers (on delete cascade), lines -> orders, an
    events table with no key, a text column on each."""
    return Catalog(
        columns={
            "customers": [Column("id", "bigint", "int", False),
                          Column("name", "varchar(40)", "text", True,
                                 collation="en_US")],
            "orders": [Column("id", "bigint", "int", False),
                       Column("customer_id", "bigint", "int", False),
                       Column("note", "text", "text", True)],
            "lines": [Column("order_id", "bigint", "int", False),
                      Column("n", "int", "int", False),
                      Column("sku", "varchar(20)", "text", False)],
            "events": [Column("at", "bigint", "int", True),
                       Column("what", "text", "text", True)]},
        keys={"customers": ("id",), "orders": ("id",),
              "lines": ("order_id", "n")},
        fks={"orders": [ForeignKey("fk_cust", ("customer_id",), "customers",
                                   ("id",), on_delete="CASCADE")],
             "lines": [ForeignKey("fk_order", ("order_id",), "orders",
                                  ("id",))]})


def test_what_a_table_must_keep_is_read_from_its_catalogue():
    got = guarantees.read_hop(_shop(), _Engine(), _Engine())
    assert set(got) == {"customers", "orders", "lines", "events"}
    o = got["orders"]
    assert o.columns == ("id", "customer_id", "note")
    assert o.parents == ("customers",) and o.atomic_with == ("customers",)
    assert o.key == ("id",) and not o.duplicates
    assert o.empty_apart == ("note",)
    assert got["lines"].parents == ("orders",)
    assert got["lines"].atomic_with == ()
    # no key and no unique index: equal rows are each a row
    assert got["events"].duplicates and got["events"].key == ()
    assert got["customers"].collations == (("name", "en_US"),)
    assert all(s.order == "none" and not s.deletes for s in got.values())
    assert all(s.gaps == () for s in got.values())
    assert o.fingerprint == _shop().fingerprint("orders")
    followed = guarantees.read_hop(_shop(), tail=True)
    assert followed["orders"].order == "per key"
    assert followed["events"].order == "as logged"
    assert followed["events"].deletes


def test_a_unique_index_keeps_a_table_with_no_key_from_duplicates():
    cat = _shop()
    cat.indexes["events"] = [Index("u", ("at", "what"), unique=True)]
    got = guarantees.read(cat, "events", tail=True)
    assert not got.duplicates and got.order == "per key"


def test_what_the_target_cannot_keep_is_a_gap_not_a_loss():
    src_cat = _shop()
    dst_cat = Catalog(
        columns={"customers": [Column("id", "bigint", "int", False),
                               Column("name", "varchar(40)", "text", True,
                                      collation="C")],
                 "orders": [Column("id", "bigint", "int", False),
                            Column("customer_id", "bigint", "int", False)],
                 "lines": [Column("order_id", "bigint", "int", False),
                           Column("n", "int", "float", False),
                           Column("sku", "varchar(20)", "text", False)],
                 "events": [Column("at", "bigint", "int", False),
                            Column("what", "text", "text", True)]},
        keys={"events": ("at",)})
    got = guarantees.read_hop(src_cat, _Engine(),
                              _Engine(EMPTY_IS_NULL=True), dst_cat)
    said = {(g.table, g.what): g.why for s in got.values() for g in s.gaps}
    assert "column note is not on the target" in said[("orders", "scope")]
    assert "int on the source and float" in said[("lines", "values")]
    assert "takes NULL on the source" in said[("events", "values")]
    assert "en_US on the source and C" in said[("customers", "encoding")]
    assert "refuses or folds" in said[("events", "duplicates")]
    assert "empty text as NULL" in said[("orders", "null and empty")]
    # a source that stores an empty text as NULL has nothing to lose there
    same = guarantees.read_hop(src_cat, _Engine(EMPTY_IS_NULL=True),
                               _Engine(EMPTY_IS_NULL=True))
    assert all(s.empty_apart == () and not s.gaps for s in same.values())
    # a field not there, carried to a target with only NULL
    docs = guarantees.read(src_cat, "events", _Engine(EXPRESSES_ABSENT=True),
                           _Engine())
    assert [g.what for g in docs.gaps] == ["absent"]
    both = guarantees.read(src_cat, "events", _Engine(EXPRESSES_ABSENT=True),
                           _Engine(EXPRESSES_ABSENT=True))
    assert both.gaps == ()
    # a gap is refused by the admission until it is answered
    ok, why = decide.admit(Schedule(), got)
    assert not ok and "lines: values" in why


def test_a_parent_the_scope_leaves_out_is_a_gap_and_refuses_the_plan():
    got = guarantees.read_hop(_shop(), tables={"orders", "lines"})
    assert set(got) == {"orders", "lines"}
    gap, = got["orders"].gaps
    assert gap.what == "dependencies" and "customers" in gap.why
    assert got["lines"].gaps == ()
    ok, why = decide.admit(Schedule(fks="after"), got)
    assert not ok and "take customers into the scope" in why
    # answered, the gap is kept as said and the schedule admitted
    ok, why = decide.admit(Schedule(fks="after", accepted=(
        ("orders", "dependencies"),)), got)
    assert ok and why == ""


def test_parents_load_first_and_a_cycle_shares_one_wave():
    sem = guarantees.read_hop(_shop())
    waves, cycles = guarantees.barriers(sem)
    assert waves == [("customers", "events"), ("orders",), ("lines",)]
    assert cycles == []
    cat = _shop()
    cat.fks["customers"] = [ForeignKey("fk_last", ("id",), "lines",
                                       ("order_id",))]
    waves, cycles = guarantees.barriers(guarantees.read_hop(cat))
    assert cycles == [("customers", "lines", "orders")]
    assert waves == [("customers", "events", "lines", "orders")]
    ok, why = decide.admit(Schedule(fks="during"), guarantees.read_hop(cat))
    assert not ok and "point at each other" in why
    assert decide.admit(Schedule(fks="after"), guarantees.read_hop(cat))[0]


def test_a_child_started_before_its_parent_is_in_is_not_admitted():
    sem = guarantees.read_hop(_shop())
    early = Schedule(fks="during", at_once=2, times=(
        ("customers", 0.0, 10.0), ("orders", 5.0, 20.0),
        ("lines", 20.0, 30.0), ("events", 0.0, 3.0)))
    ok, why = decide.admit(early, sem)
    assert not ok
    assert "orders starts before customers" in why
    assert "lines starts" not in why
    late = Schedule(fks="during", at_once=2, times=(
        ("customers", 0.0, 10.0), ("orders", 10.0, 20.0),
        ("lines", 20.0, 30.0), ("events", 0.0, 3.0)))
    assert decide.admit(late, sem) == (True, "")
    # keys off for the load: any order keeps them
    assert decide.admit(Schedule(fks="after", at_once=4,
                                 times=early.times), sem) == (True, "")
    # an untimed order: a child before its parent, or beside it
    ok, why = decide.admit(Schedule(fks="during", at_once=1, order=(
        "lines", "orders", "customers")), sem)
    assert not ok and "lines is ordered before orders" in why
    ok, why = decide.admit(Schedule(fks="during", at_once=3, order=(
        "customers", "orders", "lines")), sem)
    assert not ok and "barrier after customers is owed" in why
    assert decide.admit(Schedule(fks="during", at_once=1, order=(
        "customers", "events", "orders", "lines")), sem)[0]


def test_lanes_by_key_over_tied_tables_are_not_admitted():
    """Out-of-order dependent events: lanes cut by a row's key let a
    line's insert pass its order's; lanes cut at commits do not."""
    sem = guarantees.read_hop(_shop(), tail=True)
    ok, why = decide.admit(Schedule(apply="by key"), sem)
    assert not ok
    assert "changes to customers, lines, orders are tied" in why
    assert "events have no key" in why
    ok, why = decide.admit(Schedule(apply="unordered"), sem)
    assert not ok and "orders point at changes theirs" in why
    assert decide.admit(Schedule(apply="by transaction"), sem) == (True, "")
    assert decide.admit(Schedule(apply="in commit order"), sem) == (True, "")
    # copied only, nothing followed: no order to keep among changes
    assert decide.admit(Schedule(apply="by key"),
                        guarantees.read_hop(_shop()))[0]


def test_a_position_moved_early_a_delete_dropped_or_a_shape_ignored_is_refused():
    sem = guarantees.read_hop(_shop())
    for sched, said in (
            (Schedule(checkpoint="before durable"),
             "after the target's commit"),
            (Schedule(deletes="dropped"), "stays on the target"),
            (Schedule(ddl="ignore"), "stop on it or carry it"),
            (Schedule(snapshot="per table"), "read them as of one moment")):
        ok, why = decide.admit(sched, sem)
        assert not ok and said in why, (sched, why)
    for sched in (Schedule(checkpoint="after durable"),
                  Schedule(deletes="carried"), Schedule(deletes="frozen"),
                  Schedule(ddl="stop"), Schedule(snapshot="one"),
                  Schedule(snapshot="per range", caught_up=True)):
        assert decide.admit(sched, sem) == (True, ""), sched
    # a table no key ties may be read as of its own moment
    alone = {"events": sem["events"]}
    assert decide.admit(Schedule(snapshot="per range"), alone)[0]


def test_the_admission_is_one_need_of_the_decision_engine():
    sem = guarantees.read_hop(_shop())
    facts = decide.schedule_facts(Schedule(deletes="dropped"), sem)
    rung = decide.Rung("as scheduled", "the copy",
                       needs=(decide.KEEPS_SEMANTICS,))
    kept, out = decide.prune((rung,), facts)
    assert kept == [] and "stays on the target" in out[0][1]
    assert decide.SCHEDULE_BREAKS in facts.asked


def _jobs():
    way = jointplan.Way("as measured", per_stream=10e6,
                        uses={jointplan.SOURCE: 1})
    return [jointplan.Job("customers", 50e6, 1000, 0.0, 4, (way,)),
            jointplan.Job("orders", 400e6, 8000, 0.0, 8, (way,)),
            jointplan.Job("lines", 300e6, 9000, 0.0, 8, (way,)),
            jointplan.Job("events", 100e6, 3000, 0.0, 1, (way,))]


def test_the_joint_plan_starts_a_child_only_once_its_parent_is_in():
    sem = guarantees.read_hop(_shop())
    limits = jointplan.Limits(caps={jointplan.SOURCE: 200e6}, workers=8)
    order = ("orders", "lines", "customers", "events")
    jobs = _jobs()
    choice = {j.name: j.ways[0] for j in jobs}
    free = jointplan.simulate(jobs, choice, limits, order, tables=4)
    # unbarriered, the largest starts first: refused as it stands
    assert free.placed["orders"].start < free.placed["customers"].copied
    refused = jointplan.admitted(free, sem)
    assert not refused.admitted and not refused.meets
    assert "orders starts before customers" in refused.refused
    held = jointplan.barriered(_jobs(), sem)
    got = jointplan.simulate(held, choice, limits, order, tables=4)
    p = got.placed
    assert p["orders"].start >= p["customers"].copied - 1e-6
    assert p["lines"].start >= p["orders"].copied - 1e-6
    assert jointplan.admitted(got, sem).admitted
    # the plan the move goes by is held so too
    plan = jointplan.admitted(
        jointplan.joint(jointplan.barriered(_jobs(), sem), limits), sem)
    assert plan.admitted, plan.refused
    q = plan.placed
    assert q["orders"].start >= q["customers"].copied - 1e-6
    assert q["lines"].start >= q["orders"].copied - 1e-6
    assert plan.as_evidence()["admitted"] is True
    # keys off for the load: no barrier, the free order admitted
    off = jointplan.barriered(_jobs(), sem, fks="after")
    assert all(j.after == () for j in off)
    assert jointplan.admitted(
        jointplan.simulate(off, choice, limits, order, tables=4), sem,
        fks="after").admitted


def test_a_shape_changed_since_the_read_is_named():
    sem = guarantees.read_hop(_shop())
    cat = _shop()
    cat.columns["orders"].append(Column("total", "numeric", "decimal"))
    del cat.columns["events"]
    assert guarantees.changed_since(sem, cat) == ["events", "orders"]
    assert guarantees.changed_since(sem, _shop()) == []


def test_every_fault_is_proved_or_owed():
    proved, owed = guarantees.SAFETY_PROVED, guarantees.SAFETY_UNPROVED
    assert set(proved) | set(owed) == set(guarantees.FAULTS)
    assert not set(proved) & set(owed)
    here = Path(__file__).read_text()
    for test in proved.values():
        path, _, name = test.partition("::")
        assert path == "tests/" + Path(__file__).name
        assert f"def {name}(" in here, name


# --- the faults, on the sandbox's servers ----------------------------------

def _migkit(tmp, *args, fail=None):
    env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp / "reports"), MIGKIT_MOVER="builtin",
               COLUMNS="200")
    env.pop("MIGKIT_FAILPOINT", None)
    if fail:
        env["MIGKIT_FAILPOINT"] = fail
    return subprocess.run([sys.executable, "-c",
                           "from migkit.cli import main; main()", *args],
                          env=env, cwd=ROOT, capture_output=True, text=True,
                          timeout=900)


def _seed(port):
    got = psql(port, """
        drop table if exists public.lines, public.orders, public.customers,
          public.events cascade;
        create table public.customers (id bigint primary key, name text);
        insert into public.customers select g, case when g % 7 = 0 then ''
          when g % 11 = 0 then null else 'c-' || g end
          from generate_series(1, 20000) g;
        create table public.orders (id bigint primary key,
          customer_id bigint not null references public.customers(id)
            on delete cascade, note text);
        insert into public.orders select g, 1 + g % 20000, 'o-' || g
          from generate_series(1, 80000) g;
        create table public.lines (order_id bigint references
          public.orders(id) on delete cascade, n int, sku text,
          primary key (order_id, n));
        insert into public.lines select 1 + g % 80000, g / 80000, 's' || g
          from generate_series(1, 160000) g;
        create table public.events (at bigint, what text);
        insert into public.events select g % 500, 'e' from
          generate_series(1, 30000) g;
        analyze""")
    assert got.returncode == 0, got.stderr


_Q = {"customers": "id || '|' || coalesce(name, '<null>')",
      "orders": "id || '|' || customer_id || '|' || note",
      "lines": "order_id || '|' || n || '|' || sku",
      "events": "at || '|' || what"}


def _digest(port, t):
    return psql(port, f"select count(*) || ':' || md5(coalesce(string_agg("
                      f"{_Q[t]}, ',' order by {_Q[t]}), '')) from public.{t}"
                ).stdout.strip()


def _same_and_whole(pg_pair):
    got = {t: (_digest(pg_pair["src"], t), _digest(pg_pair["dst"], t))
           for t in _Q}
    assert all(a == b for a, b in got.values()), got
    # no row on the target points at a row it does not have, and both
    # keys the source declares are on the target, checked
    orphans = psql(pg_pair["dst"], """
        select (select count(*) from public.orders o left join
                public.customers c on c.id = o.customer_id
                where c.id is null)
             + (select count(*) from public.lines l left join
                public.orders o on o.id = l.order_id where o.id is null)
        """).stdout.strip()
    assert orphans == "0", orphans
    keys = psql(pg_pair["dst"], "select count(*) from pg_constraint where"
                                " contype = 'f' and convalidated and"
                                " connamespace = 'public'::regnamespace"
                ).stdout.strip()
    assert keys == "2", keys
    # an empty text stays apart from NULL
    nulls = [psql(p, "select count(*) filter (where name = '') || ':' ||"
                     " count(*) filter (where name is null) from"
                     " public.customers").stdout.strip()
             for p in (pg_pair["src"], pg_pair["dst"])]
    assert nulls[0] == nulls[1] and nulls[0] != "0:0", nulls


def _hop(tmp, pg_pair):
    (tmp / "hops.yaml").write_text(
        "hops:\n  keeps:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n")


def _reset(pg_pair, tmp):
    psql(pg_pair["dst"], "drop table if exists public.lines, public.orders,"
                         " public.customers, public.events cascade")
    subprocess.run(["rm", "-rf", str(tmp / "reports")])


#: a crash after a range's commit before its mark, after the mark before
#: the checkpoint, a span of the table with no key half done, a table done
#: before the next
POINTS = [("range.committed", 1), ("range.committed", 4),
          ("range.saved", 2), ("checkpoint.written", 3),
          ("span.committed", 1), ("table.done", 1), ("table.done", 3)]


@needs_docker
@pytest.mark.parametrize("point,at", POINTS,
                         ids=[f"{p}-{n}" for p, n in POINTS])
def test_a_move_with_foreign_keys_stopped_anywhere_ends_equal(
        pg_pair, tmp_path, point, at):
    _seed(pg_pair["src"])
    _reset(pg_pair, tmp_path)
    _hop(tmp_path, pg_pair)
    first = _migkit(tmp_path, "move", "keeps", "--go", "--chunk", "20000",
                    fail=f"{point}:{at}:exit")
    assert first.returncode != 0, (
        f"the move was not stopped at {point}:{at}\n{first.stdout[-2000:]}")
    again = _migkit(tmp_path, "move", "keeps", "--go", "--chunk", "20000")
    assert again.returncode == 0, again.stdout[-3000:] + again.stderr[-2000:]
    _same_and_whole(pg_pair)


@needs_docker
def test_rows_deleted_between_a_stop_and_the_run_after_are_gone(
        pg_pair, tmp_path):
    _seed(pg_pair["src"])
    _reset(pg_pair, tmp_path)
    _hop(tmp_path, pg_pair)
    first = _migkit(tmp_path, "move", "keeps", "--go", "--chunk", "20000",
                    fail="range.saved:3:exit")
    assert first.returncode != 0, first.stdout[-2000:]
    # deletes on both sides of where the copy got to, cascades among them
    got = psql(pg_pair["src"], """
        delete from public.lines where order_id % 9 = 0;
        delete from public.customers where id % 13 = 0;
        delete from public.events where at = 7""")
    assert got.returncode == 0, got.stderr
    again = _migkit(tmp_path, "move", "keeps", "--go", "--chunk", "20000")
    assert again.returncode == 0, again.stdout[-3000:] + again.stderr[-2000:]
    _same_and_whole(pg_pair)


# --- every writer asks admission (lanes, ranges, the joint plan) ----------

class _Lanes:
    """Just enough of an engine for `Engine._lanes_admitted`."""

    def _lane_name(self, table):
        return table.split(".")[-1]


def test_lanes_that_split_tied_tables_go_as_one_with_why():
    from migkit.engines.base import Engine
    groups = {"customers": 1, "orders": 1, "lines": 1, "codes": 2}
    sem = guarantees.tied_of(groups)
    assert sem["orders"].parents == ("customers", "lines")
    assert sem["codes"].self_ref and not sem["orders"].self_ref
    row = ("upsert", {"id": 1}, {})
    whole = [[("public.orders", row), ("public.lines", row)],
             [("public.events", row), ("public.events", row)]]
    assert Engine._lanes_admitted(_Lanes(), whole, groups) is whole
    apart = [[("public.orders", row)], [("public.lines", row)]]
    eng = _Lanes()
    assert Engine._lanes_admitted(eng, apart, groups) is None
    assert "lines, orders are tied" in eng.lanes_refused
    # a table tied to itself spread over two lanes is refused alike
    spread = [[("codes", row)], [("codes", row)]]
    eng = _Lanes()
    assert Engine._lanes_admitted(eng, spread, groups) is None
    assert "codes" in eng.lanes_refused
    assert guarantees.lanes_schedule(spread, sem).apply == "by key"
    assert guarantees.lanes_schedule(whole, sem, _Lanes()._lane_name
                                     ).apply == "by transaction"


def test_ranges_read_apart_are_admitted_only_where_a_tail_catches_up():
    sem = guarantees.read_hop(_shop())
    ok, why = decide.admit(guarantees.copy_schedule(
        4, False, False, "after durable"), sem)
    assert not ok and "read each as of its own moment" in why
    assert decide.admit(guarantees.copy_schedule(
        4, False, True, "after durable"), sem) == (True, "")
    assert decide.admit(guarantees.copy_schedule(
        4, True, False, "after durable"), sem) == (True, "")
    ok, why = decide.admit(guarantees.copy_schedule(
        4, True, True, "before durable"), sem)
    assert not ok and "move it after the target's commit" in why
    # tables tied to nothing are read apart safely
    loose = {"events": sem["events"]}
    assert decide.admit(guarantees.copy_schedule(
        4, False, False, "after durable"), loose)[0]


def test_the_copy_runs_one_table_at_a_time_where_its_ranges_are_refused():
    from migkit import cli
    said = []
    jp = jointplan.Plan(placed={}, makespan=1.0,
                        semantics=guarantees.read_hop(_shop()))
    eng = _Engine()
    assert cli._copy_admitted(None, eng, "db", jp, 4, False,
                              said.append) == 1
    assert said and "one at a time" in said[0]
    assert cli._copy_admitted(None, eng, "db", jp, 4, True,
                              said.append) == 4
    assert cli._copy_admitted(None, eng, "db", None, 4, False,
                              said.append) == 4
    early = _Engine(RANGE_MARK="before durable")
    with pytest.raises(SystemExit, match="not started"):
        cli._copy_admitted(_Engine(name="shop"), _Engine(dst_engine=early),
                           "db", jp, 4, True, said.append)


def test_a_refused_plan_stops_the_move_with_what_to_do(monkeypatch):
    from migkit import cli
    got = {}

    def for_move(hop, m, facts, names, most, d, sem, fks):
        got["fks"] = fks
        return jointplan.admitted(
            jointplan.Plan(placed={}, makespan=1.0, order=("a", "b")),
            {"a": guarantees.Semantics("a", parents=("b",)),
             "b": guarantees.Semantics("b", parents=("a",))}, fks)
    monkeypatch.setattr(jointplan, "for_move", for_move)
    monkeypatch.setattr(jointplan, "semantics_of", lambda *a, **k: {})
    monkeypatch.setattr(jointplan, "account", lambda *a, **k: {})
    eng = _Engine(table_facts=lambda side, d: {},
                  foreign_keys_in_load=lambda d: "during")
    said = []
    with pytest.raises(SystemExit) as stop:
        cli._joint_for_move(_Engine(name="shop"), eng,
                            _Engine(measured=True), "db", ["a", "b"], 4,
                            said.append)
    assert "not started" in str(stop.value)
    assert "point at each other" in str(stop.value)
    assert "take the keys off for the load" in str(stop.value)
    assert got["fks"] == "during"
    # the mover says the keys are off: the same tables load
    eng.foreign_keys_in_load = lambda d: "after"
    jp = cli._joint_for_move(None, eng, _Engine(measured=True), "db",
                             ["a", "b"], 4, said.append)
    assert jp.admitted and jp.as_evidence()["foreign_keys"] == "after"


def test_a_plan_not_meeting_limits_stops_the_move_before_writing(monkeypatch):
    from migkit import cli
    def for_move(*a, **k):
        return jointplan.Plan(placed={}, makespan=7200.0, meets=False,
                              why="the deadline gives 3600s: the longest table sets it",
                              admitted=True)
    monkeypatch.setattr(jointplan, "for_move", for_move)
    monkeypatch.setattr(jointplan, "semantics_of", lambda *a, **k: {})
    monkeypatch.setattr(jointplan, "account", lambda *a, **k: {})
    eng = _Engine(table_facts=lambda side, d: {},
                  foreign_keys_in_load=lambda d: "after")
    said = []
    with pytest.raises(SystemExit) as stop:
        cli._joint_for_move(_Engine(name="shop"), eng,
                            _Engine(measured=True), "db", ["a"], 4,
                            said.append)
    assert "not started" in str(stop.value)
    assert "cannot keep within the hop's limits" in str(stop.value)
    assert "the deadline gives 3600s" in str(stop.value)


def test_the_mover_says_whether_the_keys_hold_during_the_load():
    from migkit.engines.base import Engine
    assert Engine.foreign_keys_in_load(_Engine(), "db") == "during"
    skips = _Engine(dst_engine=_Engine(LOAD_SKIPS_KEYS=True))
    assert Engine.foreign_keys_in_load(skips, "db") == "after"


def test_the_target_catalogue_is_held_to_the_source(monkeypatch):
    from migkit import drift
    src, dst = _shop(), _shop()
    dst.columns["orders"] = [c for c in dst.columns["orders"]
                             if c.name != "note"]
    monkeypatch.setattr(drift, "side_reader", lambda eng, side: side)
    monkeypatch.setattr(drift, "shape", lambda r, side, db: {side: 1})
    monkeypatch.setattr(drift, "catalog_of",
                        lambda got: src if "src" in got else dst)
    sem = jointplan.semantics_of(_Engine(), "db")
    assert [g.said() for g in sem["orders"].gaps] == [
        "orders: scope - column note is not on the target"]
    assert not sem["customers"].gaps


# --- the faults the tail and a change of shape bring -----------------------

_TAIL = """
import sys
from pathlib import Path
from migkit.config import Endpoint, Hop
from migkit.engines.postgres import PostgresEngine
where, name = Path(sys.argv[1]), sys.argv[2]
src, dst = int(sys.argv[3]), int(sys.argv[4])
hop = Hop(name=name, engine="postgres",
          source=Endpoint(host="127.0.0.1", port=src, user="postgres",
                          password="test"),
          target=Endpoint(host="127.0.0.1", port=dst, user="postgres",
                          password="test"),
          databases=["postgres"], workers=4,
          options={"follow_until": "caught_up"})
hop.report_dir = lambda db=None: where
pair = PostgresEngine(hop)._as_pair()
pair.tail_apply("postgres", True, where / "tail-token.json", print)
"""


def _tail(where, name, pg_pair, fail=None):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    env.pop("MIGKIT_FAILPOINT", None)
    if fail:
        env["MIGKIT_FAILPOINT"] = fail
    return subprocess.run([sys.executable, "-c", _TAIL, str(where), name,
                           str(pg_pair["src"]), str(pg_pair["dst"])],
                          env=env, capture_output=True, text=True,
                          timeout=600)


def _tail_ready(pg_pair, tmp_path, name):
    _seed(pg_pair["src"])
    _reset(pg_pair, tmp_path)
    _hop(tmp_path, pg_pair)
    moved = _migkit(tmp_path, "move", "keeps", "--go", "--chunk", "20000")
    assert moved.returncode == 0, moved.stdout[-3000:]
    where = tmp_path / "tail"
    where.mkdir()
    first = _tail(where, name, pg_pair)
    assert first.returncode == 0, first.stdout + first.stderr
    return where


def _no_slot(pg_pair):
    psql(pg_pair["src"], "select pg_drop_replication_slot(slot_name) from"
                         " pg_replication_slots where slot_name like"
                         " 'migkit_%'")


@needs_docker
def test_a_tail_stopped_before_its_position_with_keys_held_ends_equal(
        pg_pair, tmp_path):
    """The target's keys stay on through the tail; parents and their
    children inserted and a parent deleted with its cascade, in batches
    the lanes would cut - stopped after they apply, before the position
    moves, the run after applies each once and leaves no orphan."""
    where = _tail_ready(pg_pair, tmp_path, "keys_held")
    try:
        got = psql(pg_pair["src"], """
            insert into public.customers select g, 'n-' || g
              from generate_series(20001, 23000) g;
            insert into public.orders select g, g - 60000, 'x'
              from generate_series(80001, 83000) g;
            insert into public.lines select g, 9, 'y'
              from generate_series(80001, 83000) g;
            delete from public.customers where id between 100 and 400""")
        assert got.returncode == 0, got.stderr
        stopped = _tail(where, "keys_held", pg_pair, "tail.applied:1:exit")
        assert stopped.returncode != 0, stopped.stdout
        again = _tail(where, "keys_held", pg_pair)
        assert again.returncode == 0, again.stdout + again.stderr
        _same_and_whole(pg_pair)
    finally:
        _no_slot(pg_pair)


@needs_docker
def test_a_change_delivered_twice_in_one_batch_lands_once(pg_pair,
                                                         tmp_path):
    """One batch holding the same rows changed again and again - inserted,
    updated, deleted, inserted again under one key, two equal rows of a
    table with no key - and the whole batch delivered a second time after
    a stop before its position: each row ends as the source has it."""
    where = _tail_ready(pg_pair, tmp_path, "twice")
    try:
        got = psql(pg_pair["src"], """
            begin;
            insert into public.events values (90001, 'a'), (90001, 'a');
            update public.customers set name = 'one' where id = 5;
            update public.customers set name = 'two' where id = 5;
            delete from public.lines where order_id = 6;
            insert into public.lines values (6, 0, 'again');
            update public.lines set sku = 'again2' where order_id = 6;
            commit""")
        assert got.returncode == 0, got.stderr
        stopped = _tail(where, "twice", pg_pair, "tail.applied:1:exit")
        assert stopped.returncode != 0, stopped.stdout
        again = _tail(where, "twice", pg_pair)
        assert again.returncode == 0, again.stdout + again.stderr
        _same_and_whole(pg_pair)
        dup = psql(pg_pair["dst"], "select count(*) from public.events"
                                   " where at = 90001").stdout.strip()
        assert dup == "2", dup
    finally:
        _no_slot(pg_pair)


@needs_docker
def test_a_shape_changed_between_ranges_stops_the_move(pg_pair, tmp_path):
    """A column added to a table after some of its ranges are copied: the
    run after either stops naming the table or carries the new shape -
    never rows read under two shapes said to be equal."""
    _seed(pg_pair["src"])
    _reset(pg_pair, tmp_path)
    _hop(tmp_path, pg_pair)
    first = _migkit(tmp_path, "move", "keeps", "--go", "--chunk", "20000",
                    fail="range.saved:2:exit")
    assert first.returncode != 0, first.stdout[-2000:]
    got = psql(pg_pair["src"], "alter table public.orders add column"
                               " extra int default 7")
    assert got.returncode == 0, got.stderr
    again = _migkit(tmp_path, "move", "keeps", "--go", "--chunk", "20000")
    said = again.stdout + again.stderr
    if again.returncode != 0:
        assert "orders" in said, said[-3000:]
        return
    extra = psql(pg_pair["dst"], "select count(*) filter (where extra = 7)"
                                 " || ':' || count(*) from public.orders"
                 ).stdout.strip()
    assert extra == f"{servers.private_port(0)}:80000", extra
    _same_and_whole(pg_pair)


def _pg_target(monkeypatch, quiet):
    """A PostgreSQL target whose user may do what `quiet` says
    (`movers._pg_quiet_triggers`): "session" or "flag" loads as a replica,
    "none" as the tables are, a SystemExit is a refusal."""
    from migkit import movers
    from migkit.engines.postgres import PostgresEngine

    def said(hop, db, tables):
        if isinstance(quiet, BaseException):
            raise quiet
        return quiet
    monkeypatch.setattr(movers, "_pg_quiet_triggers", said)
    eng = _Engine(hop=None, WRITE_WAYS=PostgresEngine.WRITE_WAYS,
                  _d=lambda side, db: db)
    eng._write_way = lambda db, tables=None: PostgresEngine._write_way(
        eng, db, tables)
    eng._load_skips_keys = lambda db: PostgresEngine._load_skips_keys(eng, db)
    return eng


_CYCLE = {"a": guarantees.Semantics("a", parents=("b",)),
          "b": guarantees.Semantics("b", parents=("a",))}


def test_a_postgres_cycle_loaded_as_a_replica_is_admitted(monkeypatch):
    from migkit.engines.base import Engine
    for quiet in ("session", "flag"):
        dst = _pg_target(monkeypatch, quiet)
        fks = Engine.foreign_keys_in_load(_Engine(dst_engine=dst, hop=None),
                                          "db")
        assert fks == "after", quiet
        jp = jointplan.admitted(
            jointplan.Plan(placed={}, makespan=1.0, order=("a", "b")),
            dict(_CYCLE), fks)
        assert jp.admitted and not jp.refused, quiet
    # a load already inside its replica window says so without asking
    dst = _pg_target(monkeypatch, SystemExit("never asked"))
    dst._as_replica = {"db"}
    assert Engine.foreign_keys_in_load(_Engine(dst_engine=dst, hop=None),
                                       "db") == "after"


def test_a_postgres_cycle_without_the_replica_rung_stops(monkeypatch):
    from migkit.engines.base import Engine
    for quiet in ("none", SystemExit("may not write as a replica")):
        dst = _pg_target(monkeypatch, quiet)
        fks = Engine.foreign_keys_in_load(_Engine(dst_engine=dst, hop=None),
                                          "db")
        assert fks == "during", quiet
        jp = jointplan.admitted(
            jointplan.Plan(placed={}, makespan=1.0, order=("a", "b")),
            dict(_CYCLE), fks)
        assert not jp.admitted
        assert "point at each other" in jp.refused
    # the target not readable: the keys are taken to hold
    dst = _pg_target(monkeypatch, RuntimeError("no route"))
    assert Engine.foreign_keys_in_load(_Engine(dst_engine=dst, hop=None),
                                       "db") == "during"


def test_mysql_says_its_keys_are_skipped_only_where_its_writers_skip_them():
    from migkit.engines.mysql import MySQLEngine

    def answer(v):
        def keys_off(side, db):
            assert side == "dst"
            if isinstance(v, BaseException):
                raise v
            return v
        return _Engine(_keys_off=keys_off)
    assert MySQLEngine._load_skips_keys(answer(True), "db") is True
    assert MySQLEngine._load_skips_keys(answer(False), "db") is False
    assert MySQLEngine._load_skips_keys(answer(OSError("gone")), "db") \
        is False


def test_tied_tables_copied_side_by_side_only_under_a_shared_snapshot():
    from migkit.engines.base import Engine
    tied = {"orders": guarantees.Semantics("orders"),
            "lines": guarantees.Semantics("lines", parents=("orders",))}
    sched = guarantees.copy_schedule(4, False, False, "after durable")
    ok, why = decide.admit(sched, jointplan.Tied(tied), "copy")
    assert not ok and "read each as of its own moment" in why
    ok, why = decide.admit(sched, jointplan.Tied(tied, snapshot=""), "copy")
    assert not ok
    shared = jointplan.Tied(tied, snapshot="one exported snapshot")
    ok, why = decide.admit(sched, shared, "copy")
    assert ok, why
    assert guarantees.shared_read(shared) == "one exported snapshot"
    assert guarantees.shared_read(dict(tied)) == ""
    jp = jointplan.admitted(
        jointplan.Plan(placed={}, makespan=1.0, order=("orders", "lines")),
        shared, "after")
    assert jp.as_evidence()["snapshot"] == "one exported snapshot"
    jp = jointplan.admitted(
        jointplan.Plan(placed={}, makespan=1.0, order=("orders", "lines")),
        jointplan.Tied(tied), "after")
    assert jp.as_evidence()["snapshot"] == "each range as of its own mark"
    class _Source(_Engine):
        export_moment = Engine.export_moment

    class _Exports(_Source):
        def export_moment(self, db):
            return "00000003-1", lambda: None
    assert Engine.shared_snapshot(_Source(), "db") == ""
    assert Engine.shared_snapshot(_Source(SHARED_SNAPSHOT=True), "db")
    # a source that exports a snapshot for other sessions says it shares
    # one between its readers: PostgreSQL does (`PostgresEngine`)
    assert Engine.shared_snapshot(_Exports(), "db")
    from migkit.engines.postgres import PostgresEngine
    assert PostgresEngine.export_moment is not Engine.export_moment


def test_the_cross_engine_tail_says_why_its_lanes_were_refused():
    from migkit.engines.hetero import HeteroEngine
    dst = _Engine()
    tail = _Engine(dst_engine=dst, hop=None)
    said = []
    dst.lanes_refused = "lines, orders are tied"
    HeteroEngine._lanes_said(tail, "db", said.append)
    assert len(said) == 1 and "lines, orders are tied" in said[0]
    assert "applied as one" in said[0]
    assert "lanes_refused" not in dst.__dict__
    # the same reason again is not said twice; none is not said at all
    dst.lanes_refused = "lines, orders are tied"
    HeteroEngine._lanes_said(tail, "db", said.append)
    HeteroEngine._lanes_said(tail, "db", said.append)
    assert len(said) == 1
    dst.lanes_refused = "codes spread over lanes"
    HeteroEngine._lanes_said(tail, "db", said.append)
    assert len(said) == 2 and "codes" in said[1]
