"""One ladder a table climbs, for every way it can reach the target
(`tableway`): the move's programs over the database's tables, a program
started for one table, the table copier's own ways - one list of rungs an
engine (`TABLE_WAYS`), one cost model a database, one climb a table,
decided once.

It used to be two choosers of the same thing: the MySQL bulk copy chose a
way per table from the benchmark's rates, then the table copier chose again
for every table it was handed, from costs it kept in another file, and
could dump a table the move had sent to it for its speed; the native
engines chose a third time in a module of their own. No server here: the
facts the needs read are given, and where exact numbers are the point the
cost model is one whose ways cost a start and a rate (`_Rates`).
"""
import json
import time

import pytest

from migkit import costmodel, decide, movers, planner, tableway
from migkit.config import Endpoint, Hop
from migkit.costmodel import Estimate
from migkit.engines.base import Engine
from migkit.engines.mysql import BINARY, MY_BENCH, PASS_BENCH, PLAIN, \
    MySQLEngine
from migkit.tableway import Pick, Way

MB = 2 ** 20


@pytest.fixture(autouse=True)
def _own_knowledge(tmp_path, monkeypatch):
    """Each test's own knowledge base: what one test's models learned is
    no prior of another's, and the tie it tunes is the prior's."""
    monkeypatch.setenv("MIGKIT_KNOWLEDGE", str(tmp_path / "knowledge.json"))
DL, SC, PT, ROWS = movers.DUMP_LOAD, movers.STREAM_COPY, "pass-through", \
    "rows"
STARTS = {w.name: w.startup for w in MySQLEngine.TABLE_WAYS}


def _hop(tmp_path, engine="mysql", options=None, mapping=None, exclude=()):
    hop = Hop(name="w", engine=engine,
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"], options=dict(options or {}),
              mapping=dict(mapping or {}), exclude=list(exclude))
    root = tmp_path / "reports"

    def report_dir(db=None):
        d = root / db if db else root
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


FACTS = {"narrow": {"rows": 500_000, "bytes": 40_000_000, "key": True},
         "wide": {"rows": 500_000, "bytes": 220_000_000, "key": True},
         "tiny": {"rows": 1, "bytes": 16_384, "key": True}}
COLUMNS = ({"narrow": [("id", "int", ""), ("n", "varchar", "")],
            "wide": [("id", "bigint", ""), ("raw", "varbinary", "")],
            "tiny": [("id", "int", "")]}, {})
EVERY = ("mydumper", "myloader", "mysqlsh", "mysql")


class _Rates:
    """A cost model whose ways cost their start and a table's bytes at a
    rate - one for tables of wide rows (`BINARY`, as `MY_BENCH`'s were),
    one for the rest - measured; the model's own interface
    (`costmodel.CostModel`), what it was taught kept."""

    def __init__(self, rates, starts=STARTS):
        self.rates, self.starts = rates, dict(starts)
        self.rungs, self.seen, self.outcomes, self.saved = {}, [], [], 0

    def estimate(self, name, unit=None):
        got = self.rates.get(name)
        if got is None:
            return None
        u = unit or {}
        size, rows = float(u.get("bytes") or 0), int(u.get("rows") or 0)
        shape = BINARY if rows > 1 and size / rows > 200 else PLAIN
        rate = got.get(shape) if isinstance(got, dict) else got
        return Estimate(self.starts.get(name, 0.0) + size / rate, 0.0,
                        costmodel.MEASURED_TRUTH, "the test", 3.0)

    def measured(self, name):
        return name in self.rates

    def risk(self, name):
        return 0.0

    def outcome(self, name, ok, why=""):
        self.outcomes.append((name, ok, str(why)))

    def saw(self, name, units, seconds, unit=None):
        self.seen.append((name, units, seconds))
        return 0.0

    def get(self, name, default=None):
        e = self.estimate(name)
        return e.mean if e is not None else default

    def save(self):
        self.saved += 1


def _costs(monkeypatch, dl, sc, pt, rows, starts=STARTS):
    """The database's cost model, its ways at these rates."""
    got = _Rates({DL: dict(dl), SC: dict(sc), PT: dict(pt),
                  ROWS: dict(rows)}, starts)
    monkeypatch.setattr(tableway, "model",
                        lambda eng, db, work=tableway.WORK, rungs=None: got)
    return got


SLOW = {PLAIN: 0.5e6, BINARY: 0.5e6}
FAST = {PLAIN: 100e6, BINARY: 100e6}


def _my(tmp_path, monkeypatch, local_infile=True, installed=EVERY, **hop):
    """A MySQL engine whose ladder's facts are the real ones, the servers'
    and this machine's answers given."""
    from migkit.engines import mysql
    eng = MySQLEngine(_hop(tmp_path, **hop))
    monkeypatch.setattr(mysql, "which",
                        lambda name: f"/bin/{name}" if name in installed
                        else None)
    monkeypatch.setattr(mysql, "_loading", lambda: object())
    monkeypatch.setattr(movers, "_my_target_loads_files",
                        lambda hop, quick=False: local_infile)
    monkeypatch.setattr(movers, "_disk_free", lambda path: 10 ** 15)
    monkeypatch.setattr(eng, "_pass_columns_of", lambda db: COLUMNS)
    monkeypatch.setattr(eng, "_pass_unfit", lambda db, t: "")
    monkeypatch.setattr(eng, "table_sizes", lambda db, quick=False: FACTS)
    return eng


def _choose(eng, via="mydumper", facts=FACTS, **kw):
    return tableway.choose(eng, "appdb", via, list(facts), facts,
                           fresh=True, **kw)


def _record(hop):
    """The run's decision record: each table's way and why."""
    return decide.recorded(hop)


# ---- the ladder, as data ----------------------------------------------------

def test_the_ladder_is_data_the_fallback_runs(tmp_path):
    """The rungs and a run a rung for one table: what `decide.attempt`
    runs. The move's programs are rungs of it where it runs them; only
    the ones that can carry one table on their own are rungs of the
    copier's."""
    eng = MySQLEngine(_hop(tmp_path))
    rungs, runs = tableway.ladder(eng, batch=True)
    assert [r.name for r in rungs] == [DL, SC, PT, ROWS]
    assert all(isinstance(r, decide.Rung) for r in rungs)
    rungs, runs = tableway.ladder(eng)
    assert [r.name for r in rungs] == [DL, PT, ROWS]
    assert runs[DL] == eng._dump_table and runs[PT] == eng._copy_ranges \
        and runs[ROWS] == eng._copy_ranges
    assert SC not in runs
    from migkit.engines.postgres import PostgresEngine
    pg = PostgresEngine(_hop(tmp_path, engine="postgres"))
    rungs, runs = tableway.ladder(pg, batch=True)
    assert [r.name for r in rungs] == ["ranges", "streaming"]
    assert runs["streaming"] == pg._stream_table
    # a whole PostgreSQL database goes by the program chosen for it, not
    # table by table by speed: no plan of it is refined here
    assert PostgresEngine.TABLE_BATCH == ()
    assert tableway.for_plan(pg.hop, "appdb", "pgdump", ["public.t"]) == {}
    # the native engines' copies climb the same way, in the same module
    import importlib.util
    assert importlib.util.find_spec("migkit.tableways") is None


def test_every_measured_number_seeds_the_cost_model():
    """The benchmark's numbers (`MY_BENCH`, `PASS_BENCH`) predict each way
    until this hop, or another here, measures it: each rung's two rates
    solve for a start, a second a MB and a second a thousand rows, which
    give back the times measured on `MY_BENCH`'s tables."""
    ways = {w.name: w for w in MySQLEngine.TABLE_WAYS}
    assert MY_BENCH == "docker pair, 2026-09-28"
    assert ways[DL].rates[PLAIN] == 18.9e6 and ways[DL].rates[BINARY] == \
        60.8e6 and ways[DL].rates["read"] == 280e6
    assert ways[SC].rates[PLAIN + tableway.DIRECT] == 25.4e6
    assert ways[ROWS].rates == {PLAIN: 10.8e6, BINARY: 36.8e6}
    assert (ways[DL].startup, ways[SC].startup, ways[ROWS].startup) == \
        (0.15, 1.22, 0.45)
    assert (PASS_BENCH[PLAIN], PASS_BENCH[BINARY], PASS_BENCH["startup"]) \
        == (12.6e6, 39.2e6, 0.58)
    seeds = costmodel.seeds()
    narrow = {"bytes": 41.5e6, "rows": 500_000}
    wide = {"bytes": 222e6, "rows": 500_000}
    for w in MySQLEngine.TABLE_WAYS:
        mu, source = seeds[(w.name, "mysql->mysql")]
        assert MY_BENCH in source
        for unit, shape, size in ((narrow, PLAIN, 41.5e6),
                                  (wide, BINARY, 222e6)):
            got = float(costmodel.features(unit) @ mu)
            assert got == pytest.approx(w.startup + size / w.rates[shape],
                                        rel=0.02), (w.name, shape)
    # read and written at once, the straight copy is a way of its own
    mu, _ = seeds[(SC + tableway.DIRECT, "mysql->mysql")]
    assert float(costmodel.features(wide) @ mu) == pytest.approx(
        1.22 + 222e6 / 26.9e6, rel=0.02)
    # the copier's two ways share one start: one program, the table copier
    assert ways[PT].start == ways[ROWS].start == "copier"
    assert ways[ROWS].through and not ways[PT].through
    assert [w.name for w in MySQLEngine.TABLE_WAYS
            if "masked on the way" in w.gives] == [DL]
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    said = " ".join(w.said + " " + w.short
                    for w in MySQLEngine.TABLE_WAYS).lower()
    assert not [t for t in TOOLS + ("mysqlsh", "shell") if t in said], said


def test_the_cost_model_predicts_a_way_on_the_tables_own_shape(tmp_path):
    """No rate a shape: the model costs a way on the table itself, and a
    table of many small rows is costed at its rows where one of few wide
    rows is costed at its bytes - `MY_BENCH`'s two tables, predicted."""
    eng = MySQLEngine(_hop(tmp_path))
    m = tableway.model(eng, "appdb")
    narrow = m.estimate(DL, {"bytes": 41.5e6, "rows": 500_000})
    wide = m.estimate(DL, {"bytes": 222e6, "rows": 500_000})
    assert narrow.truth == wide.truth == costmodel.PREDICTED
    assert narrow.mean == pytest.approx(0.15 + 41.5e6 / 18.9e6, rel=0.05)
    assert wide.mean == pytest.approx(0.15 + 222e6 / 60.8e6, rel=0.05)


# ---- the move's choice, table by table --------------------------------------

def test_each_table_goes_by_the_rung_measured_fastest_for_it(
        tmp_path, monkeypatch):
    eng = _my(tmp_path, monkeypatch)
    _costs(monkeypatch, {PLAIN: 1e6, BINARY: 100e6},
           {PLAIN: 1000e6, BINARY: 1e6}, SLOW, SLOW)
    got = _choose(eng)
    assert got["narrow"].rung == SC, got["narrow"]
    assert got["wide"].rung == DL, got["wide"]
    # what each table adds to a program the move starts once
    assert got["narrow"].seconds[SC] == pytest.approx(0.04, abs=0.01)
    assert got["narrow"].seconds[DL] == pytest.approx(40)
    assert "on this table, against" in got["narrow"].reason


def test_a_rung_that_saves_less_than_it_costs_to_start_is_not_started(
        tmp_path, monkeypatch):
    """Two programs started for one small table is slower than one."""
    eng = _my(tmp_path, monkeypatch)
    start = STARTS[SC]
    # narrow straight across saves half what the straight copy costs to
    # start: 10 s by the dump, 10 - start/2 straight - past the engine's
    # tie, so the climb takes it, and the start-up hands it back
    _costs(monkeypatch, {PLAIN: 4e6, BINARY: 100e6},
           {PLAIN: 40e6 / (10 - start / 2), BINARY: 1e6}, SLOW, SLOW)
    got = _choose(eng)
    assert {c.rung for c in got.values()} == {DL}, got
    assert "rather than starting another program" in got["narrow"].reason
    # saving more than the start, it is started
    _costs(monkeypatch, {PLAIN: 4e6, BINARY: 100e6},
           {PLAIN: 40e6 / (10 - start * 1.5), BINARY: 1e6}, SLOW, SLOW)
    assert _choose(eng)["narrow"].rung == SC


def test_small_tables_do_not_start_the_slower_starting_program(
        tmp_path, monkeypatch):
    """Measured in the docker run: two 2,000-row tables each a few
    milliseconds faster straight across all went that way, and the move
    paid the straight copy's start-up, eight times the dump's."""
    eng = _my(tmp_path, monkeypatch)
    small = {"orders": {"rows": 2000, "bytes": 98_304},
             "people": {"rows": 2000, "bytes": 212_992}}
    monkeypatch.setattr(eng, "table_sizes", lambda db, quick=False: small)
    _costs(monkeypatch, {PLAIN: 18.9e6, BINARY: 60.8e6},
           {PLAIN: 23.2e6, BINARY: 64.9e6}, SLOW, SLOW)
    got = _choose(eng, facts=small)
    assert {c.rung for c in got.values()} == {DL}, got
    assert "start-up costs more than it saves" in got["orders"].reason
    large = {t: {**f, "bytes": f["bytes"] * 10_000}
             for t, f in small.items()}
    got = _choose(eng, facts=large)
    assert {c.rung for c in got.values()} == {SC}, got


def test_a_difference_within_the_engines_tie_goes_by_the_list(
        tmp_path, monkeypatch):
    """Two estimates closer than `decide.TIE` are a tie, as for every
    choice the engine makes: what each leaves behind, then the list's
    order takes it, and says so."""
    eng = _my(tmp_path, monkeypatch)
    _costs(monkeypatch, {PLAIN: 4e6, BINARY: 100e6},
           {PLAIN: 40e6 / (10 * (1 - decide.TIE / 2)), BINARY: 1e6},
           SLOW, SLOW)
    got = _choose(eng)
    assert got["narrow"].rung == DL, got
    assert "a tie" in got["narrow"].reason, got["narrow"].reason


def test_a_rung_that_cannot_run_here_is_never_chosen(tmp_path, monkeypatch):
    """The target's local loads off: neither the straight copy nor the
    rows passed server to server, both of which load by them - asked of
    the target, never set, and said."""
    eng = _my(tmp_path, monkeypatch, local_infile=False)
    _costs(monkeypatch, {PLAIN: 1e6, BINARY: 1e6}, FAST, FAST,
           {PLAIN: 2e6, BINARY: 2e6})
    got = _choose(eng)
    assert {c.rung for c in got.values()} <= {DL, ROWS}, got
    climbed = decide.climb(
        "narrow", tableway.ladder(eng, True)[0],
        eng.table_way_facts("appdb", "mydumper")("narrow", FACTS["narrow"]),
        prove=False)
    why = dict((r.name, w) for r, w in climbed.passed)
    assert "local_infile is off" in why[SC], why
    assert "take a bulk load" in why[PT], why


def test_the_straight_copy_alone_falls_back_only_where_it_cannot_run(
        tmp_path, monkeypatch):
    """Asked to copy straight across, the table copier carries nothing for
    its speed, and every table where the straight copy cannot run."""
    eng = _my(tmp_path, monkeypatch)
    _costs(monkeypatch, FAST, {PLAIN: 1e6, BINARY: 1e6}, FAST, FAST)
    got = _choose(eng, via="mysqlsh")
    assert {c.rung for c in got.values()} == {SC}, got
    eng = _my(tmp_path, monkeypatch, local_infile=False)
    got = _choose(eng, via="mysqlsh")
    assert {c.rung for c in got.values()} == {ROWS}, got


def test_a_masked_table_goes_by_the_dump_whatever_is_faster(tmp_path,
                                                            monkeypatch):
    opts = {"mask_on_write": {"narrow.b": "null"}}
    eng = _my(tmp_path, monkeypatch, options=opts)
    _costs(monkeypatch, {PLAIN: 1e6, BINARY: 1e6}, FAST, FAST, FAST)
    got = _choose(eng)
    assert got["narrow"].rung == DL
    assert "masked on the way" in got["narrow"].reason
    eng = _my(tmp_path, monkeypatch, options=opts,
              installed=("mysqlsh", "mysql"))
    none = _choose(eng)
    assert none["narrow"].rung == "" and "not installed" in \
        none["narrow"].reason
    # the dump started for one table alone masks nothing: a masked table
    # the copier would carry has no way, and says why - large enough to
    # repay starting the dump for it, so only the masking holds it back
    monkeypatch.setattr(MySQLEngine, "STREAM_FROM", 1)
    eng = _my(tmp_path, monkeypatch, options=opts)
    got = tableway.choose(eng, "appdb", "builtin", ["narrow"], FACTS)
    assert got["narrow"].rung == "", got["narrow"]
    assert "applies neither its row filter nor its masking" in \
        got["narrow"].reason


# ---- one decision: the plan, the copy and the record read it ----------------

def test_the_plan_says_the_time_and_keeps_the_way_off_the_console(
        tmp_path, monkeypatch):
    """The owner, 2026-09-30: the choices stay inside. A table measured
    faster table by table is the copier's, and the plan says how long it
    takes; which way and why go to the decision record, and to the
    console only where MIGKIT_EXPLAIN asks."""
    eng = _my(tmp_path, monkeypatch, exclude=["tiny"])
    _costs(monkeypatch, {PLAIN: 1e6, BINARY: 1e6}, {PLAIN: 1e6, BINARY: 1e6},
           SLOW, FAST)
    choice = _choose(eng)
    got = {d.table: d for d in planner.plan(eng.hop, "appdb", "mydumper",
                                            list(FACTS), None, FACTS,
                                            choice=choice)}
    assert got["narrow"].path == planner.COPIER, got
    assert got["narrow"].way == ROWS
    assert "by what was measured or predicted" in got["narrow"].reason
    words = [w.said for w in MySQLEngine.TABLE_WAYS] + [
        w.short for w in MySQLEngine.TABLE_WAYS]
    assert not [w for w in words if w in str(got["narrow"])], got["narrow"]
    assert got["tiny"].path == planner.LEFT
    assert movers.routed_to_copier(eng.hop, "appdb", "mydumper",
                                   list(FACTS), "appdb", choice=choice) == \
        ["appdb.narrow", "appdb.wide"]
    said = []
    monkeypatch.delenv("MIGKIT_EXPLAIN", raising=False)
    tableway.said(eng, choice["narrow"], said.append, "appdb.narrow")
    assert said == []
    monkeypatch.setenv("MIGKIT_EXPLAIN", "1")
    tableway.said(eng, choice["wide"], said.append, "appdb.wide")
    assert said and any("the table copier's rows" in s for s in said), said
    kept = {e["work"]: e for e in _record(eng.hop)}
    assert kept["appdb.narrow"]["way"] == "the table copier's rows"
    assert kept["appdb.narrow"]["reason"] == choice["narrow"].reason


def test_the_copier_after_the_move_takes_the_moves_choice(tmp_path,
                                                         monkeypatch):
    """The move decides once and keeps it: what it measures counts only
    once it is done, so the copier that follows it cannot decide otherwise
    and leave a table it emptied empty."""
    eng = _my(tmp_path, monkeypatch)
    _costs(monkeypatch, {PLAIN: 1e6, BINARY: 1e6}, {PLAIN: 1e6, BINARY: 1e6},
           SLOW, FAST)
    hop = eng.hop
    (hop.report_dir("appdb") / "move-began.json").write_text(
        json.dumps({"epoch": time.time() - 1}))
    choice = _choose(eng)
    tableway.keep(hop, "appdb", choice)
    # the numbers change under it; the move in progress keeps its choice
    _costs(monkeypatch, FAST, {PLAIN: 1e6, BINARY: 1e6}, SLOW, SLOW)
    kept = tableway.choose(eng, "appdb", "mydumper", list(FACTS), FACTS)
    assert {t: c.rung for t, c in kept.items()} == \
        {t: c.rung for t, c in choice.items()}
    assert kept["narrow"].rung == ROWS
    # the move finished: the next plan decides afresh
    time.sleep(0.01)
    (hop.report_dir("appdb") / "copy-position.json").write_text("{}")
    fresh = tableway.choose(eng, "appdb", "mydumper", list(FACTS), FACTS)
    assert fresh["narrow"].rung == DL


def test_a_moves_units_count_once_it_is_done(tmp_path):
    """What the move's programs took, table by table, waits beside the
    move until it has finished and been held to the source; then the
    database's cost model learns it - measured now, not predicted - and
    keeps it with the hop."""
    hop = _hop(tmp_path)
    narrow = costmodel.unit_of(FACTS["narrow"])
    predicted = tableway.model(MySQLEngine(hop), "appdb").estimate(DL, narrow)
    assert predicted.truth == costmodel.PREDICTED
    movers._my_record_rates(hop, "appdb", DL, {"narrow": 40.0}, FACTS)
    m = tableway.model(MySQLEngine(hop), "appdb")
    assert not m.measured(DL), "a move still running taught the choice"
    time.sleep(0.01)
    (hop.report_dir("appdb") / "copy-position.json").write_text("{}")
    m = tableway.model(MySQLEngine(hop), "appdb")
    got = m.estimate(DL, narrow)
    assert m.measured(DL) and got.truth == costmodel.MEASURED_TRUTH
    # 40 s where 2.3 s was predicted: the table's own time moves it
    assert got.mean > 2 * predicted.mean, (got, predicted)
    assert not (hop.report_dir("appdb") / tableway.PENDING).exists()
    kept = json.loads((hop.report_dir() / costmodel.HOP_FILE).read_text())
    assert DL in kept["mysql->mysql table copy"]


def test_overlapping_spans_share_the_rungs_whole_time(tmp_path):
    """Two tables carried side by side for 2 s, one busy 3 s of thread
    time and the other 1: their times are the 2 s shared 3 to 1, not 4 s
    of spans that overlap."""
    hop = _hop(tmp_path)
    movers._my_record_rates(hop, "appdb", DL, {"narrow": 3.0, "wide": 1.0},
                            FACTS, wall=2.0)
    got = json.loads((hop.report_dir("appdb")
                      / tableway.PENDING).read_text())
    assert sorted(got[DL]) == [[500_000, 40e6, 1.5], [500_000, 220e6, 0.5]]
    movers._my_record_rates(hop, "appdb", SC, {"wide": 2.0}, FACTS,
                            suffix=tableway.DIRECT)
    got = json.loads((hop.report_dir("appdb")
                      / tableway.PENDING).read_text())
    assert got[SC + tableway.DIRECT] == [[500_000, 220e6, 2.0]]


def test_the_model_reads_the_ladders_own_choice(tmp_path, monkeypatch):
    """Whether a table's rows pass server to server - what the resource
    model sizes a worker by - is the one climb's answer, not a second
    copy of the pass-through's needs: it holds, and is costed cheapest."""
    eng = _my(tmp_path, monkeypatch)
    _costs(monkeypatch, SLOW, SLOW, FAST, SLOW)
    assert eng.passes_through("appdb", "narrow")
    _costs(monkeypatch, SLOW, SLOW, SLOW, FAST)
    assert not eng.passes_through("appdb", "narrow")
    _costs(monkeypatch, SLOW, SLOW, FAST, SLOW)
    monkeypatch.setattr(eng, "_pass_unfit",
                        lambda db, t: "g (geometry) has no text")
    assert not eng.passes_through("appdb", "narrow")


def test_the_resource_model_reads_the_ladders_measured_fits(tmp_path):
    """The move's phase is predicted from what the table copy measured on
    this hop - the ladder's fits, read for the move's own bytes and rows -
    and none of another engine pair's."""
    from migkit.engines.postgres import PostgresEngine
    hop = _hop(tmp_path, engine="postgres")
    m = tableway.model(PostgresEngine(hop), "appdb")
    m.saw("ranges", 100_000, 4.0, {"bytes": 100 * MB, "rows": 100_000})
    m.save()
    got = tableway.measured(hop, {"bytes": 500 * MB, "rows": 500_000})
    assert len(got) == 1 and got[0] == pytest.approx(25 * MB, rel=0.1)
    assert tableway.measured(_hop(tmp_path), {"bytes": 500 * MB,
                                              "rows": 500_000}) == []


# ---- a table carried, through the runtime fallback --------------------------

class Toy(Engine):
    """An engine whose table ladder is two ways, the first costed cheaper,
    each run recorded."""
    TABLE_WAYS = (
        Way("fast", "the fast way", short="the fast way", run="_fast"),
        Way("slow", "the slow way", short="the slow way", run="_slow"))
    ran = None
    fails = None

    def table_sizes(self, db, quick=False):
        return {"t": {"bytes": 512 * MB, "rows": 1000},
                "u": {"bytes": 512 * MB, "rows": 1000}}

    def _fast(self, job, way):
        self.ran.append(("fast", job.name))
        if self.fails is not None:
            raise self.fails
        return True

    def _slow(self, job, way):
        self.ran.append(("slow", job.name))
        return True


def _toy(tmp_path, monkeypatch=None, fails=None):
    eng = Toy(_hop(tmp_path, engine="postgres"))
    eng.ran, eng.fails = [], fails
    got = None
    if monkeypatch is not None:
        got = _Rates({"fast": 100e6, "slow": 1e6}, {})
        monkeypatch.setattr(tableway, "model",
                            lambda e, db, work=tableway.WORK, rungs=None: got)
    return eng, got


def _carry(eng, tmp_path, name, log):
    job = tableway.job(eng, "appdb", name, f"appdb.{name}", {}, None,
                       log.append)
    job.where = tmp_path / "move.json"
    return tableway.carry(eng, job)


def test_a_way_that_fails_as_it_runs_switches_to_the_next_said(
        tmp_path, monkeypatch):
    """`decide.attempt` runs the ladder: the fast way refused as it runs
    is given up on - for every later table where it says so - the next is
    taken, and one line says the switch; the way each table took and why
    are in the decision record, and the table's time teaches the model."""
    monkeypatch.delenv("MIGKIT_EXPLAIN", raising=False)
    eng, m = _toy(tmp_path, monkeypatch,
                  tableway.Failed("the program exited with 2",
                                  everywhere=True))
    log = []
    got = _carry(eng, tmp_path, "t", log)
    assert got.path == "slow"
    assert eng.ran == [("fast", "t"), ("slow", "t")]
    assert any("the fast way could not run (the program exited with 2)"
               in m_ and "switching to the slow way" in m_ for m_ in log), log
    assert not any(m_.startswith("decision") for m_ in log), log
    ways = [e["way"] for e in _record(eng.hop) if e["work"] == "appdb.t"]
    assert ways == ["the fast way", "the slow way"], ways
    last = [e for e in _record(eng.hop) if e["work"] == "appdb.t"][-1]
    assert "the program exited with 2" in last["reason"], last
    assert [s[0] for s in m.seen] == ["slow"] and m.saved == 1
    assert ("fast", False, "the program exited with 2") in m.outcomes
    # given up for the move: the next table does not start it again
    got = _carry(eng, tmp_path, "u", log)
    assert got.path == "slow" and ("fast", "u") not in eng.ran
    # what each table took is the engine's own report
    assert any(w["work"] == tableway.WORK and "slow" in w["seconds_a_row"]
               for w in eng.measured_ways()), eng.measured_ways()


def test_a_verdict_never_switches(tmp_path, monkeypatch):
    """A way whose answer is a difference stops the copy: another way
    would only hide it."""
    eng, _ = _toy(tmp_path, monkeypatch,
                  SystemExit("copied twice, and the target reads back 3"
                             " rows where 4 were copied"))
    with pytest.raises(SystemExit) as e:
        _carry(eng, tmp_path, "t", [])
    assert "copied twice" in str(e.value)
    assert eng.ran == [("fast", "t")]


def test_the_copier_stands_on_the_moves_choice_where_it_holds(
        tmp_path, monkeypatch):
    """The move's choice, kept, is the way the copier takes - its reason
    the move's - though another way costs less now; where it no longer
    holds, the copier climbs past it and says why."""
    eng, _ = _toy(tmp_path, monkeypatch)
    hop = eng.hop
    (hop.report_dir("appdb") / "move-began.json").write_text(
        json.dumps({"epoch": time.time() - 1}))
    tableway.keep(hop, "appdb", {"t": Pick("t", "slow", "the move's own"
                                                        " reason")})
    got = _carry(eng, tmp_path, "t", [])
    assert got.path == "slow" and eng.ran == [("slow", "t")]
    last = _record(hop)[-1]
    assert (last["work"], last["reason"]) == ("appdb.t",
                                              "the move's own reason")
    tableway._trials(eng, "appdb", tableway.ladder(eng)[0]).give_up(
        "slow", "it stopped on the last table")
    got = _carry(eng, tmp_path, "t", [])
    assert got.path == "fast"
    assert "the way the move chose no longer holds" in \
        _record(hop)[-1]["reason"], _record(hop)[-1]


def test_the_joint_plans_way_through_migkit_is_taken(tmp_path,
                                                     monkeypatch):
    """Where the move's joint plan put a table's rows through migkit -
    weighing every table's ways and the resources they share together -
    the copier takes the way that passes them so, though the table alone
    would go the other."""
    from migkit import jointplan
    eng = _my(tmp_path, monkeypatch)
    _costs(monkeypatch, SLOW, SLOW, FAST, SLOW)
    rungs = tableway.ladder(eng)[0]
    job = tableway.job(eng, "appdb", "narrow", "appdb.narrow", {}, None,
                       None)
    f = eng.table_way_facts("appdb", "builtin")("narrow", FACTS["narrow"])
    monkeypatch.setattr(jointplan, "planned_way",
                        lambda hop, db, t: "through migkit")
    assert tableway._planned(eng, job, rungs, f, ()) == ROWS
    monkeypatch.setattr(jointplan, "planned_way",
                        lambda hop, db, t: "as measured")
    assert tableway._planned(eng, job, rungs, f, ()) is None
    # on PostgreSQL the ranges' own ways: the pipe alone
    from migkit.engines.postgres import PostgresEngine
    pg = PostgresEngine(_hop(tmp_path, engine="postgres"))
    monkeypatch.setattr(pg, "_load_options", lambda db, qt: "")
    for t, plan, ways in (
            ("t1", "through migkit", ["pipe"]),
            ("t2", "as measured", [r.name for r in pg.COPY_WAYS])):
        monkeypatch.setattr(jointplan, "planned_way",
                            lambda hop, db, t, plan=plan: plan)
        got = pg._copy_ways("appdb", f"public.{t}", f'"public"."{t}"',
                            ["id"], lambda m: None)
        try:
            assert [r.name for r in got.trials.rungs] == ways, plan
        finally:
            got.close()


def test_a_program_the_move_runs_hands_its_tables_to_the_copier(
        tmp_path, monkeypatch):
    """The move's program fails as it runs: its tables are the copier's
    after the move, each climbed again without it, and the move's kept
    choice says so for the copier that reads it; a table masked on the way
    has no other way, and the move stops instead."""
    eng = _my(tmp_path, monkeypatch)
    _costs(monkeypatch, FAST, SLOW, SLOW, {PLAIN: 50e6, BINARY: 50e6})
    hop = eng.hop
    (hop.report_dir("appdb") / "move-began.json").write_text(
        json.dumps({"epoch": time.time() - 1}))
    picks = _choose(eng)
    assert {p.rung for p in picks.values()} == {DL}, picks
    said = []

    def dies():
        raise RuntimeError("the load program exited with 1")
    fell = tableway.batch_or_copier(eng, "appdb", DL, ["narrow", "wide"],
                                    dies, said.append, picks=picks,
                                    facts=FACTS)
    assert fell == ["narrow", "wide"]
    assert any("could not run (the load program exited with 1)" in m
               for m in said), said
    kept = tableway.chosen(hop, "appdb")
    assert kept["narrow"].rung == ROWS and kept["wide"].rung == ROWS, kept
    assert kept["tiny"].rung == DL
    assert "the parallel dump and load does not:" in kept["narrow"].reason
    # masked: the copier would carry the values the hop hides
    eng = _my(tmp_path, monkeypatch,
              options={"mask_on_write": {"narrow.b": "null"}})
    with pytest.raises(decide.Exhausted):
        tableway.batch_or_copier(eng, "appdb", DL, ["narrow"], dies,
                                 said.append, masked={"narrow"})


# ---- PostgreSQL: the copier's ways, each measured once ----------------------

def test_postgres_tries_each_way_once_then_takes_the_cheaper(
        tmp_path, monkeypatch):
    """No benchmark predicts PostgreSQL's table ways: each is tried on the
    first table it holds for, in the ladder's order, then the one measured
    cheaper on the table goes."""
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(tmp_path, engine="postgres"))
    monkeypatch.setattr(movers, "pgcopydb_runner", lambda: "local")
    monkeypatch.setattr(eng, "_truncatable", lambda db, qt: True)
    monkeypatch.setattr(eng, "_binary_unfit", lambda db, s, t, c: "")
    monkeypatch.setattr(eng, "_copy_cols", lambda db, s, t: ["id"])
    facts = {"public.big": {"rows": 10 ** 7, "bytes": 1024 * MB},
             "public.small": {"rows": 10, "bytes": 8192}}
    unit = costmodel.unit_of(facts["public.big"])
    m = tableway.model(eng, "appdb")

    def way(t):
        return tableway.choose(eng, "appdb", "builtin", [t], facts)[t]
    assert way("public.big").rung == "ranges"
    m.saw("ranges", 10 ** 7, 20.0, unit)
    assert way("public.big").rung == "streaming"
    m.saw("streaming", 10 ** 7, 10.0, unit)
    got = way("public.big")
    assert got.rung == "streaming" and "on this table" in got.reason
    small = way("public.small")
    assert small.rung == "ranges"
    # too small to repay starting the program: it does not hold at all
    assert "streaming" not in small.seconds and "ranges" in small.seconds
