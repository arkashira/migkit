"""How each table gets to the target: one ladder an engine, one cost model
a database, one climb a table.

Two choosers were built for the same thing on the same day. `move`'s bulk
copy chose per table between the parallel dump and load, the straight copy
and the table copier, from the benchmark's bytes a second and each
program's start-up, kept in one file; the table copier then chose again,
for every table it was handed, between passing the rows server to server,
its own rows and a dump of the one table, on seconds a byte it timed
itself and kept in another. A table the move sent to the copier for its
speed could be dumped after all, by a rule the move had not weighed, and
neither the plan nor the copy said so. The native engines' copies chose a
third time, the same way, in a module of their own.

They are one list now, the engine's `TABLE_WAYS`: every way a table can
reach the target - a program that carries the database's tables at once
(`Way.batch`), a program run on one table, the copier's own ways - each a
`decide.Rung` with the engine's method that carries one table that way
(`Way.run`), the start-up it shares with other rungs (`Way.start`) and the
bytes a second the engine's benchmark measured it at (`Way.rates`), which
`costmodel.seeds` turns into a prediction where nothing was measured. One
cost model a database (`model`, a `costmodel.CostModel`): a way's seconds
for a table from what the table is - its bytes, its rows, its large values
- as measured on this run's tables, the hop's earlier runs and other hops
here, else predicted; a measurement always beats a prediction.

One climb a table. The move decides every table it carries up front,
committed to what is measured or predicted, each program's start-up paid
once (`choose`), and keeps the answer (`keep`) for the copier that follows
it, which takes the kept way where it still holds. A table the copier
carries on its own goes the way the move's joint plan put it where that
plan passes its rows through migkit (`jointplan.planned_way`), else the way
`decide.Trials` gives on the database's model - a way never measured tried
on a small table only, within its budget. Either way it runs through the
decision layer's runtime fallback (`decide.attempt`, `carry`): a way that
fails as it runs is given up on - for every later table where it says so -
and the next taken, said; a program the move runs over many tables that
fails hands them to the copier, each climbed again without it
(`batch_or_copier`). Every choice goes to the run's decision record
(`decide.record`); the console says a table's time, and its way and why
only where `MIGKIT_EXPLAIN` asks.

The native engines' ladders - Cassandra's, OpenSearch's, DynamoDB's - climb
the same way (`each`).
"""
import dataclasses
import json
import threading
import time
from dataclasses import dataclass, field

from . import decide

#: a rung's rates measured where the rows were read and written at once,
#: with no copy on disk between - costed as a way of its own (`costmodel`)
DIRECT = " direct"
#: what a run returns where it carried the table but its time is no way's
#: own - part of it went another way - so no way learns from it
UNTIMED = "untimed"
#: a move's units waiting to count, and the choice the move in progress
#: made, beside the database's reports
PENDING = "table-ways.pending.json"
CHOSEN = "table-ways-chosen.json"
#: what the table ladder's costs are kept under, and the native engines'
WORK = "table copy"
NATIVE = "native table copy"

_LOCK = threading.RLock()


@dataclass(frozen=True)
class Way(decide.Rung):
    """One way a table can reach the target: a `decide.Rung` - what it
    gives, what it needs, what it leaves behind, what starting it costs
    (`startup`) - and beside it how it runs and what it cost where it was
    measured."""
    #: in a few words, where a reason sets it beside another
    short: str = ""
    #: carries the database's tables at once, in the move
    batch: bool = False
    run: str = ""          #: the engine's method carrying one table this way
    start: str = ""        #: the start-up it shares: paid once a move for all
    through: bool = False  #: its rows pass through migkit (`jointplan`)
    #: bytes a second by the shape of a table, and the rates a program is
    #: sized by ("read"), as the engine's benchmark measured them
    rates: dict = field(default_factory=dict, hash=False)


@dataclass(frozen=True)
class Pick:
    """How one table goes, and why: the rung, the sentence the decision
    record keeps, and the seconds each rung that could carry it was
    estimated at (None where nothing measured or predicted it). `rung` is
    "" where no way holds, and the reason says why."""
    table: str
    rung: str
    reason: str
    seconds: dict = field(default_factory=dict)


class Failed(decide.Blocked):
    """A run that did not carry its table, with why - in words an operator
    reads: given up on for the table (`decide.attempt`), and for every
    later table of the move where `everywhere`, and the next rung taken."""

    def __init__(self, why, everywhere=False):
        super().__init__(why)
        self.everywhere = everywhere


# --- the ladder as data ------------------------------------------------------

def ladder(eng, batch=False):
    """(rungs, runs): the engine's table ladder as data. `rungs` are what a
    table climbs - every one where the move runs its programs over the
    database's tables at once (`batch`), else those that carry one table
    on its own; `runs` is {rung name: run(job, choice)} for one table,
    returning True (carried, its time the rung's) or `UNTIMED`, and raising
    `Failed` - or any failure `decide.operational` counts - where it could
    not. What the runtime fallback wraps (`carry`): a failed rung given up,
    the climb made again, the next run."""
    rungs = tuple(w for w in eng.TABLE_WAYS if batch or w.run)
    return rungs, {w.name: getattr(eng, w.run) for w in rungs if w.run}


def way(eng, name):
    """The rung `name` of the engine's ladder."""
    return next(w for w in eng.TABLE_WAYS if w.name == name)


def _engine_class(hop):
    from .engines import ALIASES, _class_for
    return _class_for(ALIASES.get(hop.engine, hop.engine))


def by_the_move(hop, name):
    """Whether rung `name` of the hop's engine's table ladder is carried by
    the move's programs over the database's tables at once, rather than by
    the table copier after it."""
    return any(w.name == name and w.batch
               for w in getattr(_engine_class(hop), "TABLE_WAYS", ()))


def for_plan(hop, db, via, tables):
    """{table: Pick} as a plan reads the move by `via`: the choice the move
    in progress kept, or the one it would make now - {} where the engine's
    table ladder does not hold `via`'s programs."""
    cls = _engine_class(hop)
    if cls is None or via not in getattr(cls, "TABLE_BATCH", ()):
        return {}
    from .engines import get_engine
    return choose(get_engine(hop), db, via, tables, quick=True)


# --- one cost model a database -----------------------------------------------

def _read(path):
    try:
        got = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return got if isinstance(got, dict) else None


def _write(path, value):
    """Written beside itself and put in place whole: a half-written file is
    read as none, never as a smaller one."""
    import os
    import tempfile
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-")
    with os.fdopen(fd, "w") as f:
        json.dump(value, f)
    os.replace(tmp, path)


def pair(eng):
    """The engines a same-engine hop's costs are kept by (`costmodel`)."""
    name = getattr(eng, "CANON_ENGINE", "") or getattr(
        getattr(eng, "hop", None), "engine", "")
    return f"{name}->{name}"


def _prefix(eng):
    return f"{pair(eng)} "


def model(eng, db, work=WORK, rungs=None):
    """The `costmodel.CostModel` the tables of `db` are costed by, one for
    the engine's life: each way's seconds as a function of a table, fitted
    from the tables it carried here, kept with the hop (`costmodel.HOP_FILE`
    under the engines' pair) and learned by every hop of these engines on
    this machine; predicted from migkit's benchmarks where nothing was
    measured. A finished move's own units are taught to it first
    (`pending`)."""
    from .costmodel import CostModel
    with _LOCK:
        kept = eng.__dict__.setdefault("_table_models", {})
        got = kept.get((db, work))
        if got is None:
            got = CostModel(work, eng.TABLE_WAYS if rungs is None else rungs,
                            pair=pair(eng), hop=eng.hop,
                            prefix=_prefix(eng))
            _ripen(eng.hop, db, got)
            kept[(db, work)] = got
        return got


def pending(hop, db, rung, units):
    """Keep what a move measured on `rung` - [(rows, bytes, seconds)], a
    table each - to count once the move is done and proved (`_ripen`)."""
    if not units:
        return
    path = hop.report_dir(db) / PENDING
    got = _read(path) or {}
    got.setdefault(rung, []).extend([[int(r or 0), float(b or 0),
                                      round(float(s), 3)]
                                     for r, b, s in units])
    _write(path, got)


def _ripen(hop, db, m):
    """A finished move's units taught to the model and kept - only once
    the move they were measured in has finished and been held to the
    source (`copy-position.json` is written last): a move that failed
    teaches nothing, and the copier that follows a move decides from what
    the move decided from."""
    waiting = hop.report_dir(db) / PENDING
    done = hop.report_dir(db) / "copy-position.json"
    try:
        ripe = (waiting.exists() and done.exists()
                and done.stat().st_mtime_ns >= waiting.stat().st_mtime_ns)
    except OSError:
        ripe = False
    if not ripe:
        return
    for rung, units in (_read(waiting) or {}).items():
        for rows, size, secs in units:
            m.saw(rung, max(int(rows or 0), 1), float(secs),
                  {"rows": int(rows or 0), "bytes": float(size or 0)})
    m.save()
    waiting.unlink(missing_ok=True)


def measured(hop, unit, got=None):
    """[bytes a second] each way that carries one table was measured at on
    `unit` - its bytes and rows - as the hop kept its ways' fits (`got`, a
    hop's `costmodel.HOP_FILE`; else this hop's own): none of a benchmark's
    or another engine pair's."""
    from .costmodel import HOP_FILE, Fit, features
    cls = _engine_class(hop)
    if cls is None or not float((unit or {}).get("bytes") or 0):
        return []
    if got is None:
        got = _read(hop.report_dir() / HOP_FILE) or {}
    fits = got.get(f"{pair(cls)} {WORK}") or {}
    ways = {w.name for w in getattr(cls, "TABLE_WAYS", ()) if w.run}
    out = []
    for name, d in fits.items():
        f = Fit.from_dict(d)
        if name not in ways or f.n <= 0:
            continue
        secs = f.predict(features(unit))[0]
        if secs > 0:
            out.append(float(unit["bytes"]) / secs)
    return out


class _On:
    """The database's model answering for one table: its unit, and a way's
    rates read and written at once where the table goes so (`DIRECT`).
    What a climb costs its rungs by (`decide.costed`). `shared`: each way's
    start-up is left out, where a move starts a program once for all its
    tables and pays it once (`_regroup`)."""

    def __init__(self, m, unit, direct=(), shared=False):
        self.model, self.unit, self.direct = m, dict(unit or {}), set(direct)
        self.shared = shared

    def _name(self, name):
        return name + DIRECT if name in self.direct else name

    def start(self, name):
        """Seconds the way costs before its first row."""
        e = self.model.estimate(self._name(name), {})
        return e.mean if e is not None else 0.0

    def estimate(self, name, unit=None):
        e = self.model.estimate(self._name(name),
                                self.unit if unit is None else unit)
        if e is None or not self.shared:
            return e
        return dataclasses.replace(e, mean=max(e.mean - self.start(name),
                                               0.0))

    def risk(self, name):
        return self.model.risk(name)

    def measured(self, name):
        return self.model.measured(self._name(name))

    def outcome(self, name, ok, why=""):
        # a table's success is taught with its time (`Trials.took`)
        if not ok:
            self.model.outcome(name, ok, why)

    def get(self, name, default=None):
        e = self.estimate(name)
        return e.mean if e is not None else default


def _unit(fact):
    from .costmodel import unit_of
    return unit_of(fact or {})


def _direct(eng, f):
    """The rungs costed as read and written at once for this table."""
    if not f.get("direct"):
        return ()
    return tuple(w.name for w in eng.TABLE_WAYS
                 if any(str(k).endswith(DIRECT) for k in w.rates))


# --- the choice kept by the move in progress ---------------------------------

def keep(hop, db, picks):
    """The move's choice, for the table copier that follows it."""
    _write(hop.report_dir(db) / CHOSEN, {
        "at": time.time(),
        "tables": {t: [p.rung, p.reason, p.seconds]
                   for t, p in picks.items()}})


def chosen(hop, db):
    """The choice the move in progress made, while it is in progress: the
    move writes it after it began (`move-began.json`) and nothing has
    finished a move since (`copy-position.json`). None otherwise."""
    got = _read(hop.report_dir(db) / CHOSEN)
    began = _read(hop.report_dir(db) / "move-began.json") or {}
    if not got or not began.get("epoch"):
        return None
    if float(got.get("at") or 0) < float(began["epoch"]):
        return None
    done = hop.report_dir(db) / "copy-position.json"
    try:
        if done.exists() and done.stat().st_mtime >= float(got["at"]):
            return None
    except OSError:
        return None
    return {t: Pick(t, *v) for t, v in (got.get("tables") or {}).items()}


# --- what is said, and where -------------------------------------------------
# The owner, 2026-09-30: the choices stay inside. The console says what
# happens to each table - its time, what the operator must do - and one line
# where a way switched (`decide.attempt`); the way each table takes and why
# go to the run's decision record (`decide.record`), and to the console only
# where `MIGKIT_EXPLAIN` asks.

def outcome(pick):
    """What the console says of one table's way: how long it is expected
    to take - never the way."""
    s = (pick.seconds or {}).get(pick.rung) if pick else None
    return (f"{_seconds(s)} by what was measured or predicted"
            if s is not None else "its time is not known yet")


def said(eng, pick, log=None, work=None):
    """One table's way and why in the run's decision record - the way in
    an operator's words - said through `log` only where `MIGKIT_EXPLAIN`
    asks."""
    words = (way(eng, pick.rung).said if pick.rung
             else "no way here")
    decide.record(eng.hop, {"work": work or pick.table, "way": words,
                            "reason": pick.reason,
                            "seconds": pick.seconds},
                  say=log, work=work or pick.table, way=words,
                  reason=pick.reason)


def _seconds(s):
    from .wording import _duration
    return "under a second" if s < 1 else f"about {_duration(s)}"


# --- the climb --------------------------------------------------------------

def _needs(eng, db, name, masked=False, begun=False):
    """What a table asks of the way it goes: masked on the way, its row
    filter applied, a copy begun earlier resumed by its ranges."""
    need = []
    if masked:
        need.append("masked on the way")
    if eng.hop.row_filter(db, *str(name).split(".")):
        need.append("row filter")
    if begun:
        need.append("resume by chunk")
    return tuple(need)


def _given(rungs, given):
    """The rungs, each needing not to have been given up on here."""
    return tuple(dataclasses.replace(r, needs=tuple(r.needs) + (
        decide.Need("not given up on here",
                    lambda f, n=r.name: n not in given,
                    lambda f, n=r.name: given.get(n)),)) for r in rungs)


def _reason(eng, rung, got, costs, masked=False, moved=False):
    """The sentence the decision record keeps for one table's way: its
    time as measured or predicted on this table beside the other ways that
    hold, where anything says one - else the climb's own reason - and why
    each way the ladder lists above it, or costed below it, was passed
    over."""
    ways = {w.name: w for w in eng.TABLE_WAYS}
    order = [w.name for w in eng.TABLE_WAYS]
    w = ways[rung]
    if masked:
        only = [x.short or x.said for x in eng.TABLE_WAYS
                if "masked on the way" in x.gives]
        return (f"{w.said}: masked on the way as the hop asks, which only"
                f" {' and '.join(only)} does")
    mine = costs.estimate(rung)
    passed = []
    for r, why in got.passed:
        if r.name not in ways or not why:
            continue
        e = costs.estimate(r.name)
        above = order.index(r.name) < order.index(rung)
        cheaper = e is not None and mine is not None and e.mean < mine.mean
        line = f"{ways[r.name].short or r.said} does not: {why}"
        if (above or cheaper) and line not in passed:
            passed.append(line)
    if mine is None:
        why = got.reason or ""
        if why.startswith(w.said):
            why = why[len(w.said):].lstrip(": -")
        if any(r.name in ways and order.index(r.name) < order.index(rung)
               for r, _ in got.passed):
            # the climb's reason is those ways' whys, said one by one below
            why = ""
        parts = [x for x in [why] + passed if x]
        return f"{w.said}: " + "; ".join(parts) if parts else w.said
    known = {r.name: costs.estimate(r.name) for r in got.ranked}
    known = {n: e for n, e in known.items() if e is not None}
    others = [f"{ways[n].short or ways[n].said} {e.said()}"
              for n, e in sorted(known.items(), key=lambda x: x[1].mean)
              if n != rung]
    line = (f"{w.said}: {mine.said()} on this table"
            + (f", against {', '.join(others)}" if others else ""))
    fastest = min(known, key=lambda n: known[n].mean)
    if fastest != rung and mine.mean - known[fastest].mean \
            <= decide.TIE * mine.mean:
        line += (" - within the estimates' spread of each other, a tie,"
                 " which what each leaves behind and then the order of the"
                 " ways decide")
    elif fastest != rung and moved:
        line += (" - rather than starting another program for it: the"
                 " start-up costs more than it saves")
    return "; ".join([line] + passed)


def choose(eng, db, via, tables=None, facts=None, quick=False, fresh=False,
           known=None, begun=(), given=None):
    """{table: Pick} for the tables of `db` a move by `via` carries.

    Correctness first, as the climb's needs: a table masked on the way
    needs a way that masks, a table under a row filter one that applies
    it, a table begun earlier (`begun`) one that resumes by its ranges; a
    table the hop excludes or whose columns it maps is left to
    `planner.plan`. Then speed: each table climbs the engine's ladder on
    the database's cost model (`model`), each way costed on the table
    itself - a measurement beating a prediction.

    Where the move runs programs over the database's tables at once
    (`via` in the engine's `TABLE_BATCH`) the climb commits - no way is
    explored in a move that decides every table at once - and each program
    in use costs its start-up once (the model's own for the rung, once for
    the rungs of one `Way.start`): a rung's tables go to another - one in
    use, or one with a shorter start-up - wherever that is cheaper in all.
    The move decides afresh (`fresh`) and keeps its choice for the copier
    that follows it (`keep`); a plan reads the kept choice while a move is
    in progress, and decides afresh otherwise.

    `facts` is the engine's `table_sizes` where the caller has it; `known`
    facts the caller has asked already (a plan asks the target once);
    `given` {rung: why} the rungs given up on, passed over with why."""
    batch = via in eng.TABLE_BATCH
    if batch and not fresh:
        got = chosen(eng.hop, db)
        if got is not None:
            return got
    if facts is None:
        facts = eng.table_sizes(db, quick=quick) or {}
    names = sorted({eng.way_name(t) for t in (tables if tables is not None
                                              else facts)})
    rungs, _ = ladder(eng, batch)
    order = [r.name for r in rungs]
    if given:
        rungs = _given(rungs, given)
    masked = set(eng.masked_on_the_way(db, names))
    facts_for = eng.table_way_facts(db, via, quick=quick, known=known,
                                    sizes=facts)
    m = model(eng, db)
    out, est, opts, info, by, starts = {}, {}, {}, {}, {}, {}
    for t in names:
        parts = str(t).split(".")
        if eng.hop.excluded(db, *parts) or eng.hop.column_rules(db, *parts):
            continue
        fact = facts.get(t) or {}
        f = facts_for(t, fact)
        # in a move, what the table adds to a program started once
        costs = _On(m, _unit(fact), _direct(eng, f), shared=batch)
        got = decide.climb(t, rungs, f, _needs(eng, db, t, t in masked,
                                               t in begun), costs,
                           prove=False, explore=not batch)
        if got.rung is None:
            out[t] = Pick(t, "", got.reason)
            continue
        est[t] = {}
        for r in got.ranked:
            e = costs.estimate(r.name)
            starts.setdefault(r.name, costs.start(r.name))
            est[t][r.name] = None if e is None else e.mean
        opts[t] = [r.name for r in got.ranked]
        info[t] = (got, costs, t in masked)
        by.setdefault(got.path, []).append(t)
    moved = _regroup(eng, by, est, opts, order, starts) if batch else set()
    for rung, ts in by.items():
        for t in ts:
            got, costs, is_masked = info[t]
            seconds = {}
            for r in opts[t]:
                e = costs.estimate(r)
                seconds[r] = None if e is None else round(e.mean, 3)
            out[t] = Pick(t, rung, _reason(eng, rung, got, costs, is_masked,
                                           t in moved), seconds)
    return out


def _starts(ways, in_use, starts):
    """Seconds the programs of the rungs in use cost to start, one start
    for the rungs that share one."""
    groups = {}
    for r in in_use:
        w = ways[r]
        g = w.start or w.name
        groups[g] = max(groups.get(g, 0.0), float(starts.get(r) or 0.0))
    return sum(groups.values())


def _regroup(eng, by, est, opts, order, starts):
    """Each start-up paid once: the tables of one rung go to others - in
    use, or one not started yet - wherever the seconds they add are fewer
    than the start-ups saved. Measured why (`MY_BENCH`): two 2,000-row
    tables each a few milliseconds faster straight across went that way,
    and the move paid the straight copy's 1.22 s start against the dump's
    0.15 s. A table that can go no other way keeps its rung where it is.
    Returns the tables moved."""
    ways = {w.name: w for w in eng.TABLE_WAYS}
    moved = set()
    changed = True
    while changed:
        changed = False
        weight = {r: sum(est[t].get(r) or 0 for t in ts)
                  for r, ts in by.items()}
        best = None
        for r in sorted(by, key=lambda r: weight[r]):
            ts = by[r]
            if any(len(opts[t]) < 2 for t in ts):
                continue
            for o in order:
                if o == r or any(o not in opts[t] or (
                        est[t].get(o) is None and est[t].get(r) is not None)
                        for t in ts if o not in by):
                    continue
                if o in by:
                    dest = {}
                    for t in ts:
                        there = [x for x in by if x != r and x in opts[t]
                                 and not (est[t].get(x) is None
                                          and est[t].get(r) is not None)]
                        if not there:
                            break
                        dest[t] = min(there, key=lambda x: (
                            est[t].get(x) or 0, order.index(x)))
                    if len(dest) < len(ts):
                        continue
                else:
                    dest = {t: o for t in ts}
                after = (set(by) - {r}) | set(dest.values())
                delta = sum((est[t].get(dest[t]) or 0)
                            - (est[t].get(r) or 0) for t in ts) \
                    + _starts(ways, after, starts) \
                    - _starts(ways, set(by), starts)
                if delta < 0 and (best is None or delta < best[0]):
                    best = (delta, r, dest)
        if best:
            _, r, dest = best
            for t, o in dest.items():
                by.setdefault(o, []).append(t)
                moved.add(t)
            del by[r]
            changed = True
    return moved


# --- one table, carried ------------------------------------------------------

#: the trials' facts and needs are one table's at a time
_PICK = threading.Lock()


def _trials(eng, db, rungs, log=None, work=WORK):
    """The `decide.Trials` the tables of `db` the copier carries climb on,
    one for the engine's life: a way never measured is tried on a small
    table only, within the exploring budget, a way that failed for every
    table (`Failed.everywhere`) passed over for the rest - and what each
    table took is what `measured_ways` reports."""
    with _LOCK:
        kept = eng.__dict__.setdefault("_table_trials", {})
        got = kept.get((db, work))
        if got is None:
            try:
                sizes = eng.table_sizes(db) if work == WORK else {}
            except Exception:  # noqa: BLE001 - no size known: no median
                sizes = {}
            ahead = [int(f.get("rows") or 0) for f in (sizes or {}).values()
                     if isinstance(f, dict) and f.get("rows")]
            got = decide.Trials(f"{db}: {work}", rungs,
                                model=model(eng, db, work, rungs), log=log,
                                ahead=ahead or None)
            key = eng._work_key(work, db)
            eng.__dict__.setdefault("_way_cost", {})[key] = got.costs
            eng.__dict__.setdefault("_way_rungs", {})[key] = got.rungs
            eng.__dict__.setdefault("_way_facts", {})[key] = got.facts
            kept[(db, work)] = got
        return got


def _pick(trials, facts, need, unit):
    """The trials' way for one table: its facts and needs, its unit."""
    with _PICK:
        trials.facts, trials.need = facts, need
        return trials.pick(unit)


def planned_through(hop, db, table):
    """Whether the move's joint plan puts `table`'s rows through migkit
    (`jointplan.planned_way`), weighing every table's ways and the
    resources they share together - which a table climbed alone cannot."""
    from . import jointplan
    return jointplan.planned_way(hop, db, table) == "through migkit"


def _planned(eng, job, rungs, facts, need):
    """The rung the move's joint plan puts the table on, where it put it
    through migkit (`planned_through`) and a way of the ladder passes the
    rows so and holds; else None."""
    if not planned_through(eng.hop, job.db, eng.way_name(job.name)):
        return None
    for r in rungs:
        if getattr(r, "through", False) and not decide.prune(
                [r], facts, need)[1]:
            return r.name
    return None


def carry(eng, job):
    """Carry one table by the table ladder, through the decision layer's
    runtime fallback (`decide.attempt`): the way the move in progress
    chose for it, where it still holds; the way the move's joint plan put
    it; else the way the database's trials give (`decide.Trials` on the
    cost model). A way that fails as it runs (`Failed`, a program that
    exits, a connection lost) is given up on - for every later table of
    the move where it says so - and the next the climb gives is taken,
    said; never on a verdict. A table begun earlier stands on the way it
    began, which the decision layer keeps beside the checkpoint
    (`job.where`). Each table's time, where it was not begun earlier and
    went one way whole, teaches the model (`Trials.took`) and is kept with
    the hop. Returns the `Choice` that carried it."""
    rungs, runs = ladder(eng)
    fact = job.fact or {}
    unit = _unit(fact)
    f = eng.table_way_facts(job.db, "builtin")(job.name, fact)
    need = _needs(eng, job.db, job.name,
                  eng.way_name(job.name) in eng.masked_on_the_way(
                      job.db, [eng.way_name(job.name)]),
                  not job.fresh)
    trials = _trials(eng, job.db, rungs, job.log)
    m = trials.model
    view = _On(m, unit)
    kept_pick = (chosen(eng.hop, job.db) or {}).get(eng.way_name(job.name))
    planned, picked = None, None
    if kept_pick is not None and kept_pick.rung:
        keep = kept_pick.rung
    elif job.fresh:
        planned = _planned(eng, job, trials.rungs, f, need)
        if planned is None:
            picked = _pick(trials, f, need, unit)
        keep = planned or (picked.path if picked is not None else "") or ""
    else:
        # a table begun earlier: the way it began (`decide.remembered`)
        keep = None

    def run(choice):
        if kept_pick is not None and choice.path == kept_pick.rung:
            why = kept_pick.reason
        elif planned is not None and choice.path == planned:
            why = (f"{choice.said}: the move's joint plan puts its rows"
                   " through migkit, weighing every table's ways and the"
                   " resources they share together")
        elif picked is not None and choice.path == picked.path:
            why = _reason(eng, choice.path, picked, view)
        elif choice.reason.startswith("the way the last run took, kept"):
            why = f"{choice.said}: {choice.reason}"
        else:
            why = _reason(eng, choice.path, choice, view)
            lost = [w for r, w in choice.passed if kept_pick is not None
                    and r.name == kept_pick.rung]
            if lost:
                why += (f"; the way the move chose no longer holds:"
                        f" {lost[0].split(': ', 1)[-1]}")
        decide.record(eng.hop, dataclasses.replace(
            choice, work=job.key, reason=why), say=job.log)
        began = time.monotonic()
        # the programs a way starts for the table are listed where the
        # database's are (`runner.listing`), so a run after a crash finds
        # one still writing, as it finds a bulk path's
        from . import runner
        with runner.listing(eng.hop.report_dir(job.db)
                            / "running-programs.json"):
            done = runs[choice.path](job, choice)
        took = time.monotonic() - began
        if done is not UNTIMED and job.fresh:
            trials.took(choice.path, max(int(unit.get("rows") or 0), 1),
                        took, unit)
            m.save()
        else:
            m.outcome(choice.path, True)
        return done

    def undo(rung, error):
        if getattr(error, "everywhere", False):
            trials.give_up(rung.name, str(error))
    got, _ = decide.attempt(job.key, trials.rungs, run, f, need, view,
                            where=job.where, keep=keep, log=job.log,
                            undo=undo, prove=False)
    return got


def batch_or_copier(eng, db, name, tables, run, log, masked=(), picks=None,
                    facts=None):
    """The move's program `name` over `tables` (`run()`), through the
    decision layer's runtime fallback: where it fails as it runs, the
    tables are the table copier's after the move, each on the way its own
    climb gives without it - said, never asked, never on a verdict (a
    checksum that differs stops the move as before). A table masked on the
    way (`masked`) falls to the copier too: the copier carries a masked
    table through the pair, which writes it through the hop's transforms
    (`Engine.through_pair`). The move's choice (`picks`, kept for the
    copier) takes each fallen table's new way. Returns the tables that fell
    to the copier, [] where the program carried them."""
    w = way(eng, name)
    copier = decide.Rung(
        "copier", "the table copier, each table on the way its climb gives",
        gives=frozenset(set().union(*(x.gives for x in eng.TABLE_WAYS
                                      if not x.batch))
                        | ({"masked on the way"} if eng.copier_masks()
                           else set())))
    fell, why = [], {}

    def go(choice):
        if choice.path == name:
            return run()
        fell.extend(tables)
        return None

    def undo(rung, error):
        why[rung.name] = decide._first(error)
    decide.attempt(f"{db}: {len(tables)} tables by {w.short or w.said}",
                   (dataclasses.replace(w, needs=()), copier), go,
                   need=("masked on the way",) if set(masked) & set(tables)
                   else (), log=log, undo=undo, prove=False)
    if fell and picks is not None:
        again = choose(eng, db, "builtin", fell, facts, given={
            name: f"{w.short or w.said} failed on the move's tables:"
                  f" {why.get(name, 'it did not finish')}"})
        picks.update(again)
        keep(eng.hop, db, picks)
        for t, p in sorted(again.items()):
            said(eng, p, log, f"{db}.{t}")
    return fell


def job(eng, db, name, key, st, ck, log, **more):
    """What a table's runs share: where it is, its checkpoint entry, the
    log, whether it starts fresh, and the engine's own (`more`)."""
    import types
    try:
        fact = (eng.table_sizes(db) or {}).get(eng.way_name(name)) or {}
    except Exception:  # noqa: BLE001 - no size known: costed as unknown
        fact = {}
    return types.SimpleNamespace(
        db=db, name=name, key=key, st=st, ck=ck, log=log, fact=fact,
        where=getattr(ck, "path", None),
        fresh=not (st.get("ranges_done") or st.get("spans_done")
                   or st.get("last") is not None), **more)


# --- the native engines' ladders ---------------------------------------------

def each(eng, db, tables, rungs, facts_for, run, log):
    """Every table of `tables` by the way climbed for it, for an engine that
    copies a table by its own means and has a program's way beside it
    (Cassandra's bulk loader, OpenSearch's snapshot, DynamoDB's export):
    `rungs` its ladder, `facts_for(t)` the facts its rungs read, `run(name,
    t)` the rows that way copied. The same climb as the table ladder's: the
    trials on the database's cost model - a way never timed tried on a
    table first, then the one measured cheaper - and a way that cannot run
    on a table gives it to the next, said (`decide.attempt`). The last way,
    migkit's own, has none after it: a table no way carried stops the copy
    with every way's reason, never a skip. Returns {table: way name}."""
    trials = _trials(eng, db, rungs, log, NATIVE)
    m = trials.model
    taken = {}
    for t in tables:
        f = facts_for(t)
        picked = _pick(trials, f, (), None)

        def one(choice, t=t):
            decide.record(eng.hop, choice, say=log)
            began = time.monotonic()
            rows = run(choice.rung.name, t)
            trials.took(choice.rung.name, max(1, int(rows or 0)),
                        time.monotonic() - began)
            return rows
        got, _ = decide.attempt(t, trials.rungs, one, f,
                                costs=_On(m, {}), keep=picked.path or "",
                                log=log, prove=False)
        taken[t] = got.path
    m.save()
    return taken


# --- the plan ----------------------------------------------------------------

def plan_for(eng, db, tables, ck=None, log=None):
    """{(schema, table): Pick} for the tables the copier carries on its own
    (`tables`, as the move lists them): the climb `carry` makes when each
    one's turn comes, on what is measured or predicted now - a table begun
    earlier by its ranges, as the checkpoint `ck` says, resumed by them.
    Each way and why goes to the decision record (`said`)."""
    if not eng.TABLE_WAYS or not tables:
        return {}
    names = {(s, t): eng.way_name(eng.move_key(db, s, t)) for s, t in tables}
    begun = set()
    for (s, t), n in names.items():
        st = (ck or {}).get(eng.move_key(db, s, t)) or {}
        if st.get("ranges_done") or st.get("spans_done") \
                or st.get("last") is not None:
            begun.add(n)
    got = choose(eng, db, "builtin", sorted(set(names.values())), quick=True,
                 begun=begun)
    for n, p in sorted(got.items()):
        said(eng, p, log, n)
    return {k: got.get(n) for k, n in names.items()}


def annotate(eng, db, decisions, facts=None, log=None):
    """The plan's decisions with the time each table the copier carries
    for what the bulk copy cannot do is expected to take, and its way and
    why in the decision record; a table the ladder decided already
    (`Decision.way`), or whose columns the pair's copier maps, is as it
    was."""
    from . import planner
    if not eng.TABLE_WAYS:
        return decisions
    copied = [d.table for d in decisions if d.path == planner.COPIER
              and not d.way]
    if not copied:
        return decisions
    got = choose(eng, db, "builtin", copied, facts=facts, quick=True)
    out = []
    for d in decisions:
        p = got.get(eng.way_name(d.table)) if not d.way else None
        if p is not None and p.rung:
            said(eng, p, log, d.table)
            d = dataclasses.replace(d, reason=f"{d.reason}; {outcome(p)}",
                                    way=p.rung)
        out.append(d)
    return out
