"""A repair chosen by the size of the drift and what each way costs
(backlog P0-DEPTH 1): rows by key, the spans of the key that differ copied
again, or the table copied again whole - one ladder climbed per table
(`repairways`) on the decision engine's cost model, each way recorded so a
rollback puts it back, and every table checked afresh after its repair.

No servers: SQLite files on each side of a pair, which every rung runs
on, and the rule PostgreSQL writes its target by, with its reading of the
target's user given.
"""
import json
import sqlite3

import pytest

from migkit import costmodel, decide, leaves, repairlog, repairways
from migkit.config import Endpoint, Hop
from migkit.engines.base import Result
from migkit.repairways import RANGES, RELOAD, ROWS


@pytest.fixture(autouse=True)
def own_knowledge(tmp_path, monkeypatch):
    """What one test's repairs teach the machine's knowledge is not the
    next test's prior."""
    monkeypatch.setenv("MIGKIT_KNOWLEDGE", str(tmp_path / "knowledge.json"))
    costmodel._KNOWLEDGE.clear()
    yield
    costmodel._KNOWLEDGE.clear()


def _pair(tmp_path, n, **options):
    from migkit.engines.hetero import HeteroEngine
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    for path in (src, dst):
        con = sqlite3.connect(path)
        con.execute("create table t (id integer primary key, v text)")
        con.executemany("insert into t values (?, ?)",
                        [(i, f"v{i}") for i in range(1, n + 1)])
        con.commit()
        con.close()
    hop = Hop(name="rl", engine="hetero",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              databases=["main"],
              options={"source_engine": "sqlite", "target_engine": "sqlite",
                       **options})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop), src, dst


def _sql(path, *statements):
    con = sqlite3.connect(path)
    try:
        for s in statements:
            con.execute(s)
        con.commit()
    finally:
        con.close()


def _rows(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("select id, v from t order by id").fetchall()
    finally:
        con.close()


def _ladder(eng, said=None, copy=True):
    """The check run, then the ladder for the one table it found apart."""
    from migkit import cli
    got = [r for r in eng.check_data("main") if r.scope == "main.t"]
    assert "diff" in [r.status for r in got], [(r.status, r.detail)
                                               for r in got]
    [action] = [a for a in eng.repair_plan("main", "rows")
                if a.kind == "rows"]
    assert action.table == "t", action
    return repairways.Ladder(
        eng, "main", action, log=(said.append if said is not None
                                  else None),
        copy_table=cli._table_copier(eng.hop, eng, "main") if copy
        else None)


def _passed(ladder, way):
    return dict((r.name, why) for r, why in ladder.holding()[1]).get(way)


def _order(ladder):
    return [r.name for r in ladder.order()]


def _kept_costs(tmp_path):
    """The ways the hop kept a fitted cost for (`costmodel.HOP_FILE`)."""
    got = json.loads((tmp_path / costmodel.HOP_FILE).read_text())
    return set(got.get(repairways.PREFIX + repairways.WORK) or {})


# --- the choice, by the size of the drift -----------------------------------

def test_a_few_rows_apart_are_repaired_by_key(tmp_path):
    eng, src, dst = _pair(tmp_path, 5000)
    _sql(dst, "update t set v = 'x' where id in (100, 1100, 2100, 3100,"
              " 4100)")
    ladder = _ladder(eng)
    assert _order(ladder)[0] == ROWS
    # each span of the key holds a thousand rows and one of them differs
    assert "no span of the key differs enough" in _passed(ladder, RANGES)
    assert "target_rows: replace" in _passed(ladder, RELOAD)
    out = ladder.run()
    assert (out.way, out.proof) == (ROWS, "ok"), out
    assert _rows(dst) == _rows(src)
    assert _kept_costs(tmp_path) == {ROWS}


def test_a_span_that_differs_heavily_is_copied_again_and_the_rest_by_key(
        tmp_path):
    eng, src, dst = _pair(tmp_path, 5000)
    _sql(dst, "delete from t where id between 2001 and 3000",
         "update t set v = 'x' where id in (10, 4500, 4999)")
    damaged = _rows(dst)
    ladder = _ladder(eng)
    assert _order(ladder)[0] == RANGES
    spans, left, _ = ladder._spans()
    assert [(p.lo, p.hi, p.rows, p.named) for p in spans] == [
        (2001, 3001, 1000, 1000)], spans
    assert sorted(k for v in left.values() for k in v) == [
        ("10",), ("4500",), ("4999",)], left
    assert ladder.predicted(RANGES).mean < ladder.predicted(ROWS).mean
    # nothing has run here yet: both are predictions, and said so
    assert not ladder.predicted(RANGES).measured
    out = ladder.run()
    assert (out.way, out.proof) == (RANGES, "ok"), out
    assert out.said.startswith("1 spans of id copied again whole"
                               " (1,000 rows"), out.said
    assert out.choice["chosen"]["way"] == ("the spans of the key that"
                                           " differ, copied again whole")
    assert _rows(dst) == _rows(src)
    # the span's record is its pre-image: every row that differed in it,
    # with what the target held there - nothing, for the rows it lacked
    records = repairlog.records(tmp_path / "undo")
    by_size = sorted((len(repairlog.read(p)[1]), p) for p in records)
    assert [n for n, _ in by_size] == [3, 1000], by_size
    head, stmts, upto = repairlog.read(by_size[1][1])
    assert head["form"] == "canon" and head["key"] == ["id"], head
    assert upto == 1000
    assert all(s["op"] == "write" and s["before"] is None
               for s in stmts)
    assert sorted(int(s["key"][0]) for s in stmts) == list(range(2001,
                                                                 3001))
    # each way timed on its own work and kept with the hop
    assert _kept_costs(tmp_path) == {RANGES, ROWS}
    # a rollback puts every row back as the target held it
    for path in repairlog.to_reverse(tmp_path / "undo"):
        repairlog.reverse(path, eng.repair_io("main",
                                              repairlog.read(path)[0]))
    assert _rows(dst) == damaged


def test_most_of_a_table_apart_is_copied_again_whole_where_the_hop_lets_it(
        tmp_path):
    eng, src, dst = _pair(tmp_path, 2000, target_rows="replace")
    _sql(dst, "update t set v = 'x' where id % 10 <> 0")
    damaged = _rows(dst)
    ladder = _ladder(eng)
    assert _order(ladder) == [RELOAD, RANGES, ROWS], _order(ladder)
    out = ladder.run()
    assert (out.way, out.proof) == (RELOAD, "ok"), out
    assert _rows(dst) == _rows(src)
    [path] = repairlog.records(tmp_path / "undo")
    head, [stmt], upto = repairlog.read(path)
    assert head["form"] == "snapshot" and upto == 1
    assert stmt["before"]["rows"] == 2000
    # the digest the table read once the copy landed, not the one planned
    now = repairlog.table_digest(eng.dst_engine, "dst", "main", "t",
                                 head["columns"])
    assert stmt["after"]["digest"] == now != stmt["before"]["digest"]
    snap = tmp_path / "undo" / stmt["before"]["snapshot"]
    assert sum(1 for line in snap.read_text().splitlines() if line) == 2000
    io = eng.repair_io("main", head)
    assert repairlog.reverse(path, io) == (1, 0, [])
    assert _rows(dst) == damaged
    # and again: it is so already
    assert repairlog.reverse(path, io) == (0, 1, [])


def test_a_table_written_since_it_was_copied_whole_is_not_put_back(tmp_path):
    eng, src, dst = _pair(tmp_path, 2000, target_rows="replace")
    _sql(dst, "update t set v = 'x' where id % 10 <> 0")
    assert _ladder(eng).run().way == RELOAD
    _sql(dst, "update t set v = 'written since' where id = 1")
    [path] = repairlog.records(tmp_path / "undo")
    said = []
    got = repairlog.reverse(path, eng.repair_io(
        "main", repairlog.read(path)[0]), said.append)
    assert got == (0, 0, [["t"]]), got
    assert _rows(dst)[0] == (1, "written since")
    assert "changed since the repair" in said[0], said


def test_a_table_is_never_copied_whole_without_target_rows_replace(tmp_path):
    eng, src, dst = _pair(tmp_path, 2000)
    _sql(dst, "update t set v = 'x' where id % 10 <> 0")
    ladder = _ladder(eng)
    assert _order(ladder)[:2] == [RANGES, ROWS]
    assert "target_rows: replace" in _passed(ladder, RELOAD)
    assert ladder.run().way == RANGES
    assert _rows(dst) == _rows(src)


def test_nothing_else_may_write_a_target_a_table_is_copied_whole_into(
        tmp_path):
    from migkit import tailctl
    eng, src, dst = _pair(tmp_path, 2000, target_rows="replace")
    _sql(dst, "update t set v = 'x' where id % 10 <> 0")
    # a tail marked running on another machine counts as running
    (tmp_path / tailctl.PID).write_text("elsewhere.example.com 1")
    ladder = _ladder(eng)
    assert "migkit's change tail writes the target" in _passed(ladder,
                                                               RELOAD)
    assert _order(ladder)[0] == RANGES


def test_a_decision_row_by_row_keeps_a_table_to_rows_by_key(tmp_path):
    eng, src, dst = _pair(tmp_path, 2000, target_rows="replace",
                          newer_wins="v")
    _sql(dst, "update t set v = 'x' where id % 10 <> 0")
    ladder = _ladder(eng)
    for way in (RANGES, RELOAD):
        assert "newer_wins" in _passed(ladder, way), way
    assert _order(ladder)[0] == ROWS


def test_a_way_measured_slow_here_gives_way_to_one_predicted_faster(
        tmp_path):
    eng, src, dst = _pair(tmp_path, 5000)
    _sql(dst, "delete from t where id between 2001 and 3000")
    ladder = _ladder(eng)
    # the spans' way timed here at fifty seconds for a thousand rows,
    # before the choice is made
    ladder.costs.saw(RANGES, 1000, 50.0, unit=ladder._unit(1000))
    got = ladder.predicted(RANGES)
    assert got.measured and got.mean > 10.0, got
    assert not ladder.predicted(ROWS).measured
    assert _order(ladder)[0] == ROWS
    assert ladder.run().way == ROWS
    assert _rows(dst) == _rows(src)


def test_a_table_too_far_apart_to_name_is_planned_and_copied_whole(
        tmp_path, monkeypatch):
    """Where the leaves differ in too many places for the check to name the
    rows, the table used to be left out of the repair's plan altogether:
    nothing listed, nothing done, and the check still said it differed."""
    monkeypatch.setattr(leaves, "MOST_ROWS", 10)
    eng, src, dst = _pair(tmp_path, 3000, target_rows="replace")
    eng.DRILL_CAP = 100
    _sql(dst, "delete from t where id between 1001 and 2000")
    ladder = _ladder(eng)
    kept = eng.repair_leaves("main", "t")
    assert kept["complete"] is False and kept["key"] == ["id"], kept
    assert "too many places" in " ".join(ladder.action.statements)
    assert ladder.named == 0
    assert "did not name the rows" in _passed(ladder, ROWS)
    out = ladder.run()
    assert (out.way, out.proof) == (RELOAD, "ok"), out
    assert _rows(dst) == _rows(src)


def test_a_table_no_way_can_repair_is_said_and_left_to_the_check(
        tmp_path, monkeypatch):
    monkeypatch.setattr(leaves, "MOST_ROWS", 10)
    eng, src, dst = _pair(tmp_path, 3000)
    eng.DRILL_CAP = 100
    _sql(dst, "delete from t where id between 1001 and 2000")
    before = _rows(dst)
    out = _ladder(eng).run()
    assert out.way == "" and out.said.startswith("not repaired"), out
    assert "target_rows: replace" in out.said
    assert out.proof == "diff", out
    assert _rows(dst) == before


# --- a way that fails as it runs, and the check after -----------------------

def test_a_way_that_fails_as_it_runs_is_switched_from_and_said(
        tmp_path, monkeypatch):
    from migkit.engines.hetero import HeteroEngine
    eng, src, dst = _pair(tmp_path, 5000)
    _sql(dst, "delete from t where id between 2001 and 3000")

    def cut(self, *a):
        raise OSError("the connection went away")
    monkeypatch.setattr(HeteroEngine, "copy_range", cut)
    said = []
    ladder = _ladder(eng, said)
    assert _order(ladder)[0] == RANGES
    out = ladder.run()
    assert (out.way, out.proof) == (ROWS, "ok"), out
    assert _rows(dst) == _rows(src)
    assert any("could not run (the connection went away) - switching to"
               " the rows named, by key" in line for line in said), said
    # the span's record says its statements were planned and none landed
    stopped = repairlog.stopped(tmp_path / "undo")
    assert [(upto, planned) for _, upto, planned in stopped] == [(0, 1000)]


def test_what_is_said_of_a_repair_is_the_check_after_it(tmp_path,
                                                         monkeypatch):
    from migkit.engines.hetero import HeteroEngine
    eng, src, dst = _pair(tmp_path, 5000)
    _sql(dst, "update t set v = 'x' where id = 7")
    ladder = _ladder(eng)
    monkeypatch.setattr(HeteroEngine, "check_data",
                        lambda self, db, table=None, stream=None,
                        with_counts=False: [Result("data", "main.t", "diff",
                                                   "1 with different"
                                                   " values")])
    out = ladder.run()
    assert out.proof == "diff"
    assert repairways.said(out).endswith(
        "checked again: still differs (1 with different values)")


def test_a_check_after_that_cannot_run_is_never_taken_for_the_same(
        tmp_path, monkeypatch):
    from migkit.engines.hetero import HeteroEngine
    eng, src, dst = _pair(tmp_path, 5000)
    _sql(dst, "update t set v = 'x' where id = 7")
    ladder = _ladder(eng)

    def gone(self, db, table=None, stream=None, with_counts=False):
        raise OSError("the target went away")
    monkeypatch.setattr(HeteroEngine, "check_data", gone)
    out = ladder.run()
    assert out.proof == "error", out
    assert "the target went away" in out.proof_said
    assert "the same on both sides" not in repairways.said(out)


def test_only_every_verdict_saying_so_is_the_same():
    ok = Result("data", "d.t", "ok", "rows 5 equal")
    diff = Result("data", "d.t", "diff", "1 missing")

    class Target:
        def __init__(self, got):
            self.got = got

        def check_data(self, db, table):
            return self.got
    assert repairways.recheck(Target([ok]), "d", "t") == ("ok",
                                                          "rows 5 equal")
    assert repairways.recheck(Target([ok, diff]), "d", "t")[0] == "diff"
    assert repairways.recheck(Target([]), "d", "t")[0] == "error"
    skipped = Result("data", "d.t", "skip", "not compared")
    assert repairways.recheck(Target([skipped]), "d", "t")[0] == "skip"


# --- the record -------------------------------------------------------------

def test_a_landed_line_is_what_the_statement_left(tmp_path):
    rec = repairlog.Record(tmp_path, "t", "snapshot", [("id", "integer")],
                           ["id"])
    rec.planned("reload", ["t"], {"digest": [1, "a"]}, {"digest": [1, "b"]})
    rec.ready()
    rec.landed(1, {"digest": [1, "c"]})
    rec.applied(1)
    rec.close()
    _, [stmt], upto = repairlog.read(rec.path)
    assert stmt["after"] == {"digest": [1, "c"]} and upto == 1


def test_a_snapshot_is_never_replayed(tmp_path):
    io = repairlog.TableIO(None, "d", "t", [("id", "integer")], ["id"],
                           tmp_path)
    with pytest.raises(SystemExit, match="run the repair again"):
        io.put(("t",), {"digest": [1, "b"]})


# --- the leaves a check keeps -----------------------------------------------

class _Kept:
    def __init__(self):
        self.got = {}

    def _write_drill(self, db, name, **kinds):
        self.got.update(kinds)


def test_the_leaves_kept_say_which_spans_differ_and_how_far():
    pair = _Kept()
    a = leaves.Side(None, "src", "d", "t", [("id", "integer")], ["id"])
    found = leaves.Localized(spec=leaves.Spec("range", base=1, width=1000),
                             sizes={2: (1000, 0), 5: (1000, 999)})
    leaves.kept_leaves(pair, "d", "t", a, found, 9000)
    [line] = pair.got["leaves"]
    assert json.loads(line) == {
        "kind": "range", "base": 1, "width": 1000, "key": ["id"],
        "rows": 9000, "complete": False,
        "leaves": [[2, 1000, 0], [5, 1000, 999]]}
    # a table with no key has no span a repair could copy
    pair = _Kept()
    leaves.kept_leaves(pair, "d", "t", leaves.Side(
        None, "src", "d", "t", [("v", "text")], []), found, 9000)
    assert pair.got == {}


def test_a_drill_that_names_the_rows_clears_the_leaves_kept_before(
        tmp_path):
    from migkit.engines.base import Engine
    eng = Engine(Hop(name="k", engine="postgres",
                     source=Endpoint(host="10.0.0.5", port=1, user="u",
                                     password="CHANGE_ME"),
                     target=Endpoint(host="10.0.0.5", port=2, user="u",
                                     password="CHANGE_ME"),
                     databases=["d"]))
    eng.hop.report_dir = lambda db=None: tmp_path
    (tmp_path / "data-t.leaves").write_text("{}\n")
    eng._drill_clause("d", "t", [("1",)], [], [], [])
    assert not (tmp_path / "data-t.leaves").exists()
    assert (tmp_path / "data-t.missing").read_text() == '["1"]\n'


def test_a_span_is_counted_by_the_key_on_a_side_read_here(tmp_path):
    eng, src, dst = _pair(tmp_path, 3000)
    side = leaves.Side(eng.src_engine, "src", "main", "t",
                       [("id", "integer"), ("v", "text")], ["id"])
    spec = leaves.Spec("range", base=1, width=1000)
    _sql(src, "delete from t where id between 2001 and 2100")
    assert leaves.range_counts(side, spec, {0, 2}) == {0: 1000, 2: 900}


# --- PostgreSQL writes its target by one rule -------------------------------

def _postgres(tmp_path, monkeypatch, quiet):
    from migkit import movers
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="10.0.0.5", port=1, user="app", password="CHANGE_ME")
    eng = PostgresEngine(Hop(name="w", engine="postgres", source=ep,
                             target=ep, databases=["app"]))
    eng.hop.report_dir = lambda db=None: tmp_path

    def asked(hop, db, tables):
        if isinstance(quiet, BaseException):
            raise quiet
        return quiet
    monkeypatch.setattr(movers, "_pg_quiet_triggers", asked)
    return eng


@pytest.mark.parametrize("mode", ["flag", "session"])
def test_a_user_who_may_writes_the_target_as_a_replica(tmp_path, monkeypatch,
                                                       mode):
    eng = _postgres(tmp_path, monkeypatch, mode)
    seen = []
    decide.LISTENERS.append(seen.append)
    try:
        with eng.load_window("app", None, {"public.t"}):
            assert eng.replica_env("app") == {
                "PGOPTIONS": "-c session_replication_role=replica"}
    finally:
        decide.LISTENERS.remove(seen.append)
    assert eng.replica_env("app") == {}
    assert [c.path for c in seen
            if c.work == "app: rows written into the target"] == [
        "as a replica"]


def test_tables_with_no_trigger_are_written_as_they_are(tmp_path,
                                                        monkeypatch):
    eng = _postgres(tmp_path, monkeypatch, "none")
    with eng.load_window("app", None, {"public.t"}):
        assert eng.replica_env("app") == {}


def test_tables_with_triggers_a_user_cannot_quiet_are_not_written(
        tmp_path, monkeypatch):
    eng = _postgres(tmp_path, monkeypatch, SystemExit(
        "app: public.t have triggers on the target - foreign keys among"
        " them - and app can neither turn them off for the load nor load"
        " as a replica."))
    with pytest.raises(SystemExit, match="have triggers on the target"):
        with eng.load_window("app", None, {"public.t"}):
            pytest.fail("the window opened")


def test_a_target_script_in_the_window_runs_as_a_replica(tmp_path,
                                                         monkeypatch):
    from migkit.engines import postgres
    eng = _postgres(tmp_path, monkeypatch, "session")
    ran = []

    class Done:
        returncode, stdout, stderr = 0, "", ""

    def run(argv, input=None, capture_output=True, text=True, env=None):
        ran.append(env["PGOPTIONS"])
        return Done()
    monkeypatch.setattr(postgres.subprocess, "run", run)
    eng._psql_run("dst", "app", "select 1")
    with eng.load_window("app", None, {"public.t"}):
        eng._psql_run("dst", "app", "select 1")
        eng._psql_run("src", "app", "select 1")
    assert ["replica" in opts for opts in ran] == [False, True, False], ran
