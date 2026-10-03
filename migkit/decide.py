"""One way of deciding for every choice migkit makes (backlog P0, decided
2026-09-27).

migkit decided in four places, each in its own way: `movers.pick` took the
first program installed, `planner.plan` chose per table by the hop's rules
and never measured, PostgreSQL's `_verify_way` tried each way once and kept
the cheaper (the only one that measured), and MySQL's `loops_prevented`
asked the servers what would send a change round. The two-way marks of R3
would have been a fifth. A rule learned in one of them held in none of the
others. All four go through this module now, each as one list of rungs
beside the code it chooses for, so a new choice costs one list and nothing
else.

A rung names what it gives (capabilities, in migkit's words), what it
needs (predicates over facts: a version, a grant, a program installed, a
key on the table, a server setting already on), how it is proved (a probe
run before it is trusted) and how it is costed (seconds a unit of work,
measured and kept by the caller - for the run in `Costs`, for the hop
beside its rates in `HopCosts`). The climb, per unit of work, drops every
rung that lacks something the work needs, ranks the rest, proves the top
one and falls a rung on a failed proof, saying why. A strategy for a unit
is composed of parts - how it is read, chunked, written, verified, resumed
and marked - each part a list of rungs, and only the combinations whose
parts fit each other are ranked.

The owner's rule for the order (2026-09-27): what is measured faster ranks
higher; the footprint a rung leaves (a table made, a slot, a process, a
file) breaks a tie and never ranks. A rung never timed is tried before one
that was, in the order its list gives, so each gets its number once - the
rule the verify way already followed. Where nothing is timed the list's
order stands, which is the order the code's own measurements gave it.

The choice is kept beside the work's position (`remember`, next to the
checkpoint or the tail's token), so a restart stands on the same rung; a
kept rung that no longer holds or proves is said and climbed past. No
program is named here: a rung's `said` is what an operator reads, and the
reasons are built from it.

Two things the owner decided on 2026-09-30 are here as well. A rung may
need a change on the source (`Change`): it is offered, never assumed -
the only thing migkit asks (`offered`, `sourcechange`). And a way that
is blocked or fails as it runs is switched from by itself and said, never
asked (`attempt`): the next rung that holds everything the work needs.

And what the owner asked on 2026-09-30 - "really smart, not flat"
(backlog P0-SMART) - is here too, each part where it is measured:
* a rung's cost is a function of the unit with its spread
  (`costmodel.CostModel`, an `Estimate` a unit), fitted from every unit
  it ran, predicted where nothing is measured - from this hop's earlier
  runs, other hops here (the local knowledge base), the resource model,
  migkit's benchmarks - and a measurement always beats a prediction
  (`rank`);
* a rung's chance of failing times the work a failure throws away is in
  its expected time (`costmodel.risked`);
* exploration is safe (`Trials` with a model): a rung never measured is
  tried on a small unit only, never on a deadline's critical path, never
  where its prediction's best is slower than what was measured, and
  within `EXPLORE_BUDGET` of the work's time; a unit that leaves its
  prediction's spread re-climbs the ranges left and says so in a line;
* guarantees are never traded (`guarantees`): where rungs declare them,
  only those reaching the strongest the engines allow are ranked;
* every way of a strategy is weighed (`space`): every combination of the
  parts that fits, bounded by what a combination can at best reach and
  cost, not by guessing; and a move's tables are planned as one schedule
  (`jointplan`);
* every `Choice` carries its runner-up and why not, with the times
  (`Choice.as_evidence`), the facts it read (`factbook`), and is kept in
  the run's decision record (`record`) - which the console shows only
  where `MIGKIT_EXPLAIN=1`: the choices stay inside, the outcomes are
  said.
"""
import contextlib
import dataclasses
import hashlib
import itertools
import json
import math
import os
import time
from dataclasses import dataclass, field

#: the words of a gap are the capability table's
from .capabilities import NOT_APPLICABLE, NOT_YET, STREAM_ONLY  # noqa: E402

#: two measured costs closer than this share of the larger are a tie, and
#: the footprint decides: a smaller difference is within what timing a
#: busy server twice can give, and ranking on it would flip the choice from
#: one run to the next
TIE = 0.05

#: why a unit of work needs each capability, as the reason for passing over
#: a rung that lacks it; `{rung}` is that rung's `said`
WORDS = {
    "carried": "it is carried, which {rung} does not do",
    "left alone": "the hop excludes it",
    "row filter": "its row filter is applied on both sides, which {rung}"
                  " cannot do",
    "column mapping": "its columns are mapped - kept, dropped or renamed -"
                      " which {rung} cannot do",
    "resume by chunk": "a single table resumes by chunk, which {rung}"
                       " cannot do",
    "resume by key": "a stop resumes by key, which {rung} cannot do",
    "keyless": "the table has no key, which {rung} cannot carry",
    "renamed": "the hop gives it another name on the target, which {rung}"
               " cannot write it under",
    "apart": "migkit's own writes must be told apart when read back, which"
             " {rung} cannot do",
    "exact": "no batch may be applied twice, which {rung} cannot promise",
    "byte exact": "the proof must hold byte for byte, which {rung} does"
                  " not",
    "consistent as of": "the rows are read as of one moment, which {rung}"
                        " cannot do",
    "online": "the source keeps taking writes while it is read, which"
              " {rung} cannot allow",
    "masked on the way": "the hop masks its columns on the way, which"
                         " {rung} cannot do",
    "access history": "the target evicts keys by how recently or how often"
                      " they were read, and {rung} does not carry that",
    "certificate checked": "the hop checks the server's certificate, which"
                           " {rung} cannot do",
    "source position": "each message carries its source position, which"
                       " {rung} cannot write",
    "into an older server": "the target refuses the source's serialised"
                            " values, which {rung} writes",
}


def lacking(capability, rung):
    """The reason `rung` is passed over for want of `capability`."""
    return WORDS.get(capability, capability + ", which {rung} cannot do"
                     ).format(rung=rung.said)


def _joined(reasons):
    """Reasons in the order they arose, each once."""
    seen = []
    for r in reasons:
        if r and r not in seen:
            seen.append(r)
    return "; ".join(seen)


class Facts(dict):
    """What the needs and proofs read, each read at most once: a value
    given, or a reader called the first time something asks - so a need
    that asks a server is paid only for a rung still in the running, and
    only once however many rungs ask it."""

    def __init__(self, known=None, **readers):
        super().__init__(known or {})
        self._readers = readers
        #: every fact asked, in the order first asked: what a decision
        #: stood on (`Choice.asked`, `factbook.account`)
        self.asked = []

    def __getitem__(self, key):
        asked = self.__dict__.setdefault("asked", [])
        if key not in asked:
            asked.append(key)
        return super().__getitem__(key)

    def __missing__(self, key):
        if key not in self._readers:
            raise KeyError(key)
        value = self[key] = self._readers[key]()
        return value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def unread(self):
        """The facts a reader could answer that nobody asked."""
        return [k for k in self._readers if k not in self]


@dataclass(frozen=True)
class Need:
    """One thing a rung needs, as a predicate over the facts.

    `holds(facts)` answers whether it is there. `why` is what is said when
    it is not: a sentence (`{rung}` stands for the rung's `said`), a
    callable of the facts, or None for "<rung> needs <what>". A rung's
    needs are asked in the order it lists them and stop at the first that
    does not hold, so the one that asks a server goes last."""
    what: str
    holds: object
    why: object = None

    def missing(self, rung, facts):
        """The reason, or None where the need holds."""
        if self.holds(facts):
            return None
        if callable(self.why):
            return self.why(facts)
        if self.why:
            return self.why.replace("{rung}", rung.said)
        return f"{rung.said} needs {self.what}"


@dataclass(frozen=True)
class Rung:
    """One way of doing one part of the work."""
    name: str                       #: the label the code uses
    said: str                       #: what an operator reads
    gives: frozenset = frozenset()  #: capabilities, `WORDS`' keys
    needs: tuple = ()               #: `Need`s, cheapest first
    prove: object = None            #: facts -> None, or why it failed
    because: str = ""               #: why it is the first choice
    footprint: int = 0              #: what it leaves behind: the tie-breaker
    fits: object = None             #: {part: Rung} -> True, or why not
    #: `Change`s on the source it needs, asked after its needs hold
    changes: tuple = ()
    #: (facts, error) -> None: what it began, undone where it fails as it
    #: runs (`attempt`), so the next rung starts from what was there
    cleanup: object = None
    #: the guarantees it holds, ((guarantee, level), ...) or a dict
    #: (`guarantees.LEVELS`): a choice whose rungs say them ranks only
    #: those reaching the strongest any of them does
    holds: tuple = ()
    #: `guarantees.Lift`s: what it tops up in a strategy it is part of
    lifts: tuple = ()
    #: the resources a byte of it passes and how often, ((resource,
    #: passes), ...) or a dict (`model.Stage.name`): what its cost is
    #: predicted from where it never ran (`costmodel`), and what a
    #: resource's limit being cut slows (`Trials`)
    loads: tuple = ()
    #: seconds before its first byte, and round trips a thousand rows take
    startup: float = 0.0
    trips: float = 0.0

    def __post_init__(self):
        for name in ("holds", "loads"):
            got = getattr(self, name)
            if isinstance(got, dict):
                object.__setattr__(self, name, tuple(sorted(got.items())))

    @property
    def load_map(self):
        return dict(self.loads or ())

    def lacks(self, need):
        """The first needed capability this rung does not give, or None."""
        for cap in need:
            if cap not in self.gives:
                return cap
        return None


@dataclass(frozen=True)
class Choice:
    """The climb's answer for one unit of work, `planner.Decision`-like:
    the way, the reason, and every way passed over with why."""
    work: str
    rung: object                    #: the Rung, or None where none holds
    reason: str
    passed: tuple = ()              #: ((Rung, why), ...)
    ranked: tuple = ()              #: the rungs that hold, in rank order
    #: ((rung name, its cost on the unit: an `Estimate`, seconds a unit,
    #: or None), ...) of the ranked rungs
    costs: tuple = ()
    #: the next rung that holds and why it was not taken
    runner_up: object = None
    why_not: str = ""
    #: ((guarantee, level), ...) the chosen rung reaches
    guarantees: tuple = ()
    #: the facts the climb read, in the order it read them
    asked: tuple = ()

    @property
    def path(self):
        return self.rung.name if self.rung else None

    @property
    def said(self):
        return self.rung.said if self.rung else "no way here"

    def cost(self, name=None):
        """The cost of the chosen rung (or `name`) on the unit, or None."""
        name = name or self.path
        return dict(self.costs).get(name)

    def __str__(self):
        return f"{self.work}: {self.said} - {self.reason}"

    def as_evidence(self):
        """The choice as data - for the evidence export (backlog R23.1)
        and the run's decision record: the way taken and why, its time on
        the unit with its spread (measured or predicted), the guarantees
        it reaches, the runner-up with its time and why not, every way
        passed over with why, and the facts it read. In the operator's
        words: no rung is named by its label, no program by its name."""
        def cost(name):
            got = dict(self.costs).get(name)
            if got is None:
                return None
            if hasattr(got, "as_evidence"):
                return got.as_evidence()
            return {"seconds_a_unit": got, "truth": "measured"}
        from . import factbook
        known = [str(k) for k in self.asked]
        measured = [f"cost of {r.said}" for r in self.ranked
                    if getattr(dict(self.costs).get(r.name), "measured",
                               dict(self.costs).get(r.name) is not None)]
        predicted = [f"cost of {r.said}" for r in self.ranked
                     if getattr(dict(self.costs).get(r.name), "truth", "")
                     == "predicted"]
        return {
            "work": str(self.work),
            "chosen": ({"way": self.rung.said,
                        "predicted": cost(self.rung.name)}
                       if self.rung else None),
            "reason": self.reason,
            "guarantees": dict(self.guarantees),
            "runner_up": ({"way": self.runner_up.said,
                           "predicted": cost(self.runner_up.name),
                           "why_not": self.why_not}
                          if self.runner_up is not None else None),
            "ranked": [{"way": r.said, "predicted": cost(r.name)}
                       for r in self.ranked],
            "passed": [{"way": r.said, "why": why}
                       for r, why in self.passed],
            "facts": factbook.account(known, measured, predicted,
                                      factors=()),
        }


class Costs(dict):
    """Seconds a unit of work took on each rung, for one run: the number
    describes this machine, this link and how busy the servers are now,
    which is why a run's own numbers rank before anything older."""

    def saw(self, name, units, seconds):
        """Remember that `units` of work took `seconds` on `name`."""
        self[name] = float(seconds) / max(int(units or 0), 1)


class HopCosts:
    """The hop's own measurements, kept with its rates
    (`planner.record_rate`) and read back as seconds a unit, so the
    second run chooses from the first run's numbers and never from a
    guess. `prefix` keeps one choice's rungs apart from another's in the
    hop's file."""

    def __init__(self, hop, prefix=""):
        self.hop = hop
        self.prefix = prefix

    def get(self, name, default=None):
        from . import planner
        rate = planner.measured_rate(self.hop, self.prefix + name)
        return 1.0 / rate if rate else default

    def saw(self, name, units, seconds):
        from . import planner
        planner.record_rate(self.hop, self.prefix + name, units, seconds)


class Layered:
    """Costs read from the first mapping that has them - this run's own
    before the hop's kept ones - and each measurement written into
    every one, so a later run starts from what this one saw."""

    def __init__(self, *layers):
        self.layers = [c for c in layers if c is not None]

    def get(self, name, default=None):
        for c in self.layers:
            got = c.get(name)
            if got is not None:
                return got
        return default

    def saw(self, name, units, seconds):
        for c in self.layers:
            c.saw(name, units, seconds)


#: the cost a rung being timed for the first time stands at, so the next
#: unit tries the next rung never timed rather than a second of the same
_TIMING = 1e18


class _InFlight:
    """A work's costs with its rungs being timed now ranked last."""

    def __init__(self, costs, timing):
        self.costs, self.timing = costs, timing

    def get(self, name, default=None):
        got = self.costs.get(name)
        if got is not None:
            return got
        return _TIMING if name in self.timing else default


#: exploring may cost at most this share of the work's time so far: a
#: conservative bandit's budget (Wu et al., 2016). Measured
#: (`bench/smart_decisions.py explore`, 300 tables of 60 ranges of skewed
#: sizes, three ways, 2026-09-30), the table's time over the time with
#: every range on its fastest way: each way tried once and then the least
#: seconds a row, 60.6% longer on average (47.2% median, 117.8% p90); the
#: trials here with nothing predicted, 22.6% (15.3%, 41.9%); with each
#: way predicted to within x1.6, 5.7% (5.2%, 11.3%). A budget of 2% gave
#: 23.7% and 10% gave 21.7% with nothing predicted, the same within 0.2%
#: with a prediction: 5% is the least that loses nothing measurable
EXPLORE_BUDGET = 0.05
#: a unit is small enough to explore on where it holds no more rows than
#: this many times the median unit so far
EXPLORE_SIZE = 1.0


class _InFlightModel:
    """A cost model with the rungs being tried for the first time now
    ranked last, so the next unit tries another."""

    def __init__(self, model, timing, pressure=None):
        self.model, self.timing = model, timing
        self.pressure = pressure or (lambda name: 1.0)

    def estimate(self, name, unit=None):
        if name in self.timing and not self.model.measured(name):
            from .costmodel import Estimate
            return Estimate(_TIMING)
        got = self.model.estimate(name, unit)
        k = self.pressure(name)
        return got.scaled(k) if got is not None and k != 1.0 else got

    def risk(self, name):
        return self.model.risk(name)

    def get(self, name, default=None):
        return self.model.get(name, default)


class Trials:
    """One climb for each unit of a larger work - the ranges of one table -
    on that work's own measurements, from threads copying units side by
    side. A rung never timed is tried on one unit; the units started
    before it finishes go on to the next rung never timed, not to a second
    trial of the same one, and once every rung holding is timed each unit
    takes the cheapest. A rung that failed on this work (`give_up`) is
    passed over for the rest of it, and the reason is said the way any
    other rung's is.

    With a cost model (`model`, a `costmodel.CostModel`; `unit` the
    typical unit) the trials are safe (backlog P0-SMART, item 3): a rung
    is tried only on a unit no larger than the median so far
    (`EXPLORE_SIZE`), never where the work is on a deadline's critical
    path (`critical`), never where even its best is slower than the
    measured best (its prediction's p10 above it: the lead is outside the
    spread), and only while what exploring cost over the best stays within
    `EXPLORE_BUDGET` of the work's time. A unit whose time leaves its
    prediction's spread (`costmodel.SURPRISE`) re-plans what is left: the
    next unit climbs again on what was just learned, and the switch, if
    any, is said in one line through `log` - as is a switch because a
    resource's limit was cut (`limits`, the pace's `sizing.Limit`s: a way
    that loads a resource cut to half its workers is costed at twice its
    time). Nothing here is a second controller: the pace moves the
    workers, the trials move the way."""

    def __init__(self, work, rungs, facts=None, need=(), costs=None,
                 model=None, unit=None, log=None, critical=False,
                 limits=None, ahead=None):
        import dataclasses
        import threading
        self.work, self.need = work, need
        self.facts = facts if facts is not None else Facts()
        self.costs = costs if costs is not None else Costs()
        self.given_up = {}
        self._timing = set()
        self._lock = threading.Lock()
        self.rungs = tuple(dataclasses.replace(r, needs=tuple(r.needs) + (
            Need("not given up on here",
                 lambda f, n=r.name: n not in self.given_up,
                 lambda f, n=r.name: self.given_up.get(n)),))
            for r in rungs)
        self.model, self.unit, self.log = model, unit, log
        self.critical = critical
        self.limits = limits or {}
        self._sizes, self._spent, self._extra = [], 0.0, 0.0
        self._on, self._why = None, ""
        #: the rows of every unit the work has, where known ahead (the
        #: ranges of a table): what a small unit is measured against
        self._median = None
        if ahead:
            got = sorted(int(x) for x in ahead)
            self._median = got[len(got) // 2]
        if model is not None and not getattr(model, "rungs", None):
            model.rungs = {r.name: r for r in rungs}

    # --- the way for the next unit ------------------------------------------

    def pick(self, unit=None):
        """The `Choice` for the next unit (`unit`: its rows and bytes,
        where the caller knows them)."""
        with self._lock:
            if self.model is None:
                got = climb(self.work, self.rungs, self.facts, self.need,
                            _InFlight(self.costs, self._timing))
                if got.rung is not None and \
                        self.costs.get(got.rung.name) is None:
                    self._timing.add(got.rung.name)
                return got
            u = unit if unit is not None else self.unit
            costs = _InFlightModel(self.model, self._timing, self._pressure)
            got = climb(self.work, self.rungs, self.facts, self.need, costs,
                        unit=u, explore=AGAIN if self._may_explore(u)
                        else False)
            if got.rung is not None and not self._measured_best(got, u):
                if not self.model.measured(got.rung.name):
                    self._timing.add(got.rung.name)
            else:
                self._settle(got)
            return got

    def _may_explore(self, unit):
        """Whether this unit may try a way not measured here yet."""
        if self.critical:
            return False
        rows = int((unit or {}).get("rows") or 0)
        if rows:
            median = self._median or (sorted(self._sizes)[
                len(self._sizes) // 2] if self._sizes else None)
            self._sizes.append(rows)
            if median is not None and rows > EXPLORE_SIZE * median:
                return False
        return self._extra <= EXPLORE_BUDGET * max(self._spent, 0.0) \
            or not self._spent

    def _measured_best(self, got, unit):
        """Whether the choice is the best of what was measured (not a
        trial)."""
        c = got.cost()
        return c is not None and _measured(c) and c.mean < _TIMING

    def _pressure(self, name):
        """How much slower `name` goes with the resources it loads cut to
        what their limits allow now."""
        rung = next((r for r in self.rungs if r.name == name), None)
        if rung is None or not self.limits or not rung.loads:
            return 1.0
        from .sizing import stage_of
        k = 1.0
        for resource, limit in self.limits.items():
            stage = stage_of(resource)
            if stage in rung.load_map and limit.value < limit.most:
                k = max(k, limit.most / max(limit.value, 1))
                self._why = (f"{resource} held to {limit.value} of"
                             f" {limit.most} at a time")
        return k

    def _settle(self, got):
        """The way the work stands on now; a change of it said."""
        if got.rung is None:
            return
        was, self._on = self._on, got.rung
        if was is None or was.name == got.rung.name or self.log is None:
            return
        why = self._why or "what was measured since"
        self.log(f"{self.work}: {got.rung.said} for what is left, where"
                 f" {was.said} went before - {why}")
        self._why = ""

    # --- what a unit took -----------------------------------------------------

    def took(self, name, units, seconds, unit=None):
        """A unit done on `name`: `units` of work in `seconds`."""
        with self._lock:
            self.costs.saw(name, units, seconds)
            self._timing.discard(name)
            if self.model is None:
                return
            before = self.model.estimate(name, unit)
            z = self.model.saw(name, units, seconds, unit)
            self._spent += float(seconds or 0)
            if self._on is not None and name != self._on.name:
                best = self.model.estimate(self._on.name, unit)
                if best is not None:
                    self._extra += max(0.0, float(seconds) - best.mean)
            self.model.outcome(name, True)
            from .costmodel import SURPRISE
            if z is not None and abs(z) > SURPRISE and before is not None:
                # the next unit climbs again on what this one taught; the
                # switch, if there is one, is said with this as its why
                rung = next((r for r in self.rungs if r.name == name), None)
                self._why = (f"{rung.said if rung else name} took"
                             f" {seconds:,.1f}s where"
                             f" {before.said()} was predicted")

    def give_up(self, name, why):
        """`name` failed here and is not tried again on this work."""
        with self._lock:
            self.given_up.setdefault(name, why)
            self._timing.discard(name)
            if self.model is not None:
                self.model.outcome(name, False, why)

    def save(self):
        """What the trials learned, kept with the hop and the machine."""
        if self.model is not None:
            self.model.save()


def from_model(model):
    """(costs, facts) of a move's resources (`model.Model`, backlog R22):
    the seconds one stream spends on a megabyte at each resource measured -
    keyed by the resource's name ("source", "target", "link:src",
    "link:dst", "cpu") - and what a rung's needs may ask of them: which one
    binds, what it allows, how many at once carry it, what a worker holds
    of memory and the widest row. A choice between ways that load different
    resources is costed from what was measured here instead of each way
    being tried first."""
    return model.costs(), Facts(model.facts())


def _tied(a, b):
    return abs(a - b) <= TIE * max(abs(a), abs(b))


def _order(entries):
    """[(cost or None, footprint, position, item)] -> [item]: never timed
    first by position, then by cost, a tie decided by the footprint and
    then by position."""
    untried = sorted((e for e in entries if e[0] is None),
                     key=lambda e: e[2])
    tried = sorted((e for e in entries if e[0] is not None),
                   key=lambda e: (e[0], e[2]))
    out = []
    while tried:
        first = tried[0][0]
        tie = [e for e in tried if _tied(e[0], first)]
        out += sorted(tie, key=lambda e: (e[1], e[2]))
        tried = tried[len(tie):]
    return [e[3] for e in untried + out]


def rank(rungs, costs=None, unit=None, explore=True):
    """The rungs in the order the climb tries them. Where `costs` answers
    for a unit (`costmodel.CostModel`), by what each costs on `unit`
    (`_order_estimates`); by one number a rung otherwise, as always."""
    rungs = list(rungs)
    return _ranked(rungs, _priced(rungs, costs, unit), explore)


def _estimating(costs):
    return costs is not None and callable(getattr(costs, "estimate", None))


def costed(rung, costs, unit=None, ranges=1):
    """What `rung` costs on `unit`: an `Estimate` - its chance of failing
    times the work a failure throws away counted in (`costmodel.risked`) -
    where `costs` answers for a unit, seconds a unit where it keeps one
    number, None where nothing is measured or predicted."""
    if costs is None:
        return None
    if not _estimating(costs):
        return costs.get(rung.name)
    got = costs.estimate(rung.name, unit)
    if got is None or not got.mean:
        return got
    p = costs.risk(rung.name) if callable(getattr(costs, "risk", None)) \
        else 0.0
    if p:
        from .costmodel import risked
        from .guarantees import lost
        got = got.scaled(risked(got.mean, p, lost(rung, ranges)) / got.mean)
    return got


def _priced(rungs, costs, unit=None):
    return {r.name: costed(r, costs, unit) for r in rungs}


def _ranked(rungs, priced, explore=True):
    entries = [(priced.get(r.name), r.footprint, i, r)
               for i, r in enumerate(rungs)]
    if any(hasattr(e[0], "mean") for e in entries):
        return _order_estimates(entries, explore)
    return _order(entries)


def _measured(cost):
    """Whether a cost was measured here (a number always was)."""
    if cost is None:
        return False
    return getattr(cost, "measured", True)


def _tied_estimates(a, b):
    """Two costs within `TIE` of each other, or within the noise of one
    unit's time about their lines (`Estimate.noise`): a difference timing
    the units again could reverse. How far a line is stretched from the
    units it was fitted on is not noise: the lower mean is the better
    bet, and exploring is where that spread counts (`Trials`)."""
    share = max(_tie(), getattr(a, "noise", 0.0) or 0.0,
                getattr(b, "noise", 0.0) or 0.0)
    return abs(a.mean - b.mean) <= share * max(abs(a.mean), abs(b.mean))


def _tie():
    """`TIE` as the timing noise measured on this machine has tuned it
    (`tuning`); a number a unit always ties at `TIE` itself."""
    from . import tuning
    try:
        return tuning.value("decide.TIE")
    except Exception:  # noqa: BLE001 - the prior, then
        return TIE


def _by_mean(entries):
    tried = sorted(entries, key=lambda e: (e[0].mean, e[2]))
    out = []
    while tried:
        first = tried[0][0]
        tie = [e for e in tried if _tied_estimates(e[0], first)]
        out += sorted(tie, key=lambda e: (e[1], e[2]))
        tried = [e for e in tried if e not in tie]
    return out


def _order_estimates(entries, explore=True):
    """[(Estimate or None, footprint, position, item)] -> [item]. A rung
    nothing measured or predicted is tried first, in its list's order, as
    always. A measurement always beats a prediction: the measured rungs
    by what they cost, a tie (within the noise) to the smaller footprint;
    a rung only predicted goes before them where it may be faster - its
    p10 below the measured best, so it is worth its trial (`explore`), or
    its p90 below it, so it is faster for sure (committing) - and after
    them otherwise. Where nothing is measured, the predictions rank. A
    rung nothing says anything of goes last where the climb commits.
    `explore` AGAIN - the trials of a work of many units - also times
    again a measured rung whose p10 is below the best and that ran fewer
    than `AGAIN_UNITS` units: its lead is not yet outside the spread."""
    unknown = sorted((e for e in entries if e[0] is None),
                     key=lambda e: e[2])
    measured = _by_mean([e for e in entries
                         if e[0] is not None and e[0].measured])
    predicted = [e for e in entries
                 if e[0] is not None and not e[0].measured]
    if not measured:
        head, tail = _by_mean(predicted), []
    else:
        best = measured[0][0].mean
        ahead = ((lambda e: e[0].low() < best) if explore
                 else (lambda e: e[0].high() < best))
        head = _by_mean([e for e in predicted if ahead(e)])
        tail = _by_mean([e for e in predicted if not ahead(e)])
        if explore == AGAIN:
            again = [e for e in measured[1:] if e[0].n < AGAIN_UNITS
                     and e[0].low() < best]
            head = sorted(head + again, key=lambda e: (e[0].low(), e[2]))
            measured = [e for e in measured if e not in again]
    if not explore and (measured or predicted):
        return [e[3] for e in head + measured + tail + unknown]
    return [e[3] for e in unknown + head + measured + tail]


#: `explore` for the trials of a work of many units (`Trials`)
AGAIN = "again"
#: units a measured rung is timed on before its lead or its lag is taken
#: as it stands
AGAIN_UNITS = 2


def _why_not(chosen, runner, priced):
    """Why `runner`, the next way that holds, was not taken over
    `chosen`, with each one's time where there is one."""
    a, b = priced.get(chosen.name), priced.get(runner.name)
    if a is None and b is None:
        return (f"{chosen.said} comes first in its list, and neither was"
                " measured or predicted here")
    if a is None:
        return (f"{chosen.said} is tried first: nothing has measured or"
                " predicted it here yet")
    if b is None:
        return "nothing has measured or predicted it here yet"
    if hasattr(a, "mean") and hasattr(b, "mean"):
        if _tied_estimates(a, b):
            return (f"within the noise ({b.said()} against {a.said()}),"
                    + (" and it leaves more behind"
                       if runner.footprint > chosen.footprint
                       else " and it comes later in its list"))
        if b.measured:
            return f"measured slower: {b.said()} against {a.said()}"
        return (f"predicted slower: {b.said()} against {a.said()}"
                + ("; the lead is outside its spread"
                   if b.low() >= a.mean else ""))
    if hasattr(a, "mean") or hasattr(b, "mean"):
        return "measured or predicted slower here"
    if _tied(a, b):
        return ("within what timing a busy server twice can give, and it"
                + (" leaves more behind" if runner.footprint
                   > chosen.footprint else " comes later in its list"))
    return (f"measured {b / a:,.1f} times the time a unit of"
            f" {chosen.said} took" if a else "measured slower here")


def _strongest(kept):
    """(the rungs reaching the strongest guarantees any of them does,
    [(rung, why not)]) - only where a rung says its guarantees itself: a
    list that says none is ranked as it always was."""
    from . import guarantees as g
    if not any(g.explicit(r) for r in kept):
        return list(kept), []
    keep, short = g.reaching(kept, lambda r: g.of([r]))
    return keep, [(r, f"{r.said} gives {why}") for r, why in short]


# --- a change on the source a rung needs -------------------------------------
# The owner, 2026-09-30: a faster way that needs a change on the source - a
# plugin put on, a setting, a grant - is offered, never assumed and never
# hidden. The rung declares it (`Rung.changes`), and its probe reads the
# server for where it stands: there already, migkit's user allowed to make
# it, only a DBA, or nobody (a managed service that refuses it) - the
# server's own answer, a cloud's name at most a hint. The operator's
# answer is a fact like any other, under `answer_key`: a rung whose change
# is not there holds only where the answer is yes. `offered` says what the
# answer would buy before it is given; `sourcechange` asks, records, makes
# the change and takes it off again.

PRESENT = "present"        #: there already: nothing to change
POSSIBLE = "possible"      #: migkit's user may make it, and take it off
BY_DBA = "by a DBA"        #: a DBA can make it; migkit's user cannot
IMPOSSIBLE = "impossible"  #: nobody can, on this source

#: an operator's answers to a change offered: migkit makes it, the source
#: is left as it is, or the statements go to the DBA's script
ANSWERS = ("yes", "no", "dba")


@dataclass(frozen=True)
class Stand:
    """Where one change stands on this source, as its probe read it, and
    why - in words an operator reads after the change's own."""
    state: str
    why: str = ""


def answer_key(name):
    """The fact an answer to the change `name` is kept under."""
    return "answer " + name


def _first(e):
    text = str(e).strip() or type(e).__name__
    return text.splitlines()[-1][:300]


def terminated(statement):
    """A statement as a DBA's script writes it: ended by a semicolon, a
    comment left as it is."""
    s = str(statement).rstrip()
    return s if s.endswith(";") or s.startswith(("--", "//")) else s + ";"


@dataclass(frozen=True)
class Change:
    """One change on the source a rung needs: what it is in words, the
    probe that reads where it stands (facts -> `Stand`), the statements
    that make it and take it off again (a tuple, or facts -> [statement]),
    and when migkit takes it off. `speeds` False: the rung cannot run
    without it at all, where a faster way only goes faster."""
    name: str
    said: str
    probe: object
    make: object = ()
    undo: object = ()
    after: str = "after the copy"
    speeds: bool = True

    def stand(self, facts):
        """The probe's answer, asked once and kept in the facts. A probe
        that fails is a change nobody here can be sure of: a DBA's."""
        key = "change " + self.name
        if key not in facts:
            try:
                got = self.probe(facts)
            except Exception as e:  # noqa: BLE001 - said, never guessed
                got = Stand(BY_DBA, "whether it can be made here could not"
                                    f" be read: {_first(e)}")
            facts[key] = got
        return facts[key]

    def answer(self, facts):
        got = facts.get(answer_key(self.name))
        return got if got in ANSWERS else None

    def statements(self, facts, which="make"):
        got = getattr(self, which)
        return list(got(facts) if callable(got) else got or ())

    def holds(self, facts):
        state = self.stand(facts).state
        return state == PRESENT or (state == POSSIBLE
                                    and self.answer(facts) == "yes")

    def missing(self, rung, facts):
        """Why `rung` cannot run for want of this change, or None."""
        if self.holds(facts):
            return None
        stand, answer = self.stand(facts), self.answer(facts)
        head = f"{rung.said} needs {self.said} on the source"
        if stand.state == IMPOSSIBLE:
            return f"{head}, which this source does not allow: {stand.why}"
        stmts = " ".join(terminated(s) for s in self.statements(facts))
        if stand.state == BY_DBA:
            return (f"{head}, which only a DBA can make here: {stand.why}"
                    + (f"; a DBA runs on the source: {stmts}" if stmts
                       else ""))
        if answer == "no":
            return (f"{head}, and the answer is no: the source is left as it"
                    " is")
        if answer == "dba":
            return (f"{head}, handed to the DBA and not made yet"
                    + (f": {stmts}" if stmts else ""))
        return (f"{head}, which migkit may make and take off again"
                f" {self.after} - offered, never assumed: answer it"
                f" (source_changes: {{{self.name}: yes | no | dba}})"
                + (f", or a DBA runs on the source: {stmts}" if stmts
                   else ""))


@dataclass(frozen=True)
class Offer:
    """What a change on the source would buy, before anybody answers: the
    way the climb would take with it, the changes (each possible here and
    not answered), and the `Choice` the climb makes without them."""
    work: str
    rung: object
    changes: tuple
    instead: object


@contextlib.contextmanager
def answering(facts, answers):
    """The facts with `answers` ({change name: answer}) standing in for
    the operator's, put back as they were after."""
    keys = {answer_key(n): v for n, v in answers.items()}
    before = {k: facts[k] for k in keys if k in facts}
    facts.update(keys)
    try:
        yield facts
    finally:
        for k in keys:
            if k in before:
                facts[k] = before[k]
            else:
                facts.pop(k, None)


def offered(work, rungs, facts=None, need=(), costs=None):
    """The `Offer` a change on the source makes, or None: where the climb,
    were every change migkit may make here made, would take a rung that
    needs one not answered yet. Only a rung whose own needs all hold asks
    its changes' probes, so nothing is asked of the server for a rung out
    for another reason; nothing is proved - nothing is run to offer."""
    facts = facts if facts is not None else Facts()
    rungs = tuple(rungs)
    open_ = {}
    for r in rungs:
        if not r.changes or r.lacks(need) is not None:
            continue
        if any(n.missing(r, facts) for n in r.needs):
            continue
        for c in r.changes:
            if c.name not in open_ and c.answer(facts) is None \
                    and c.stand(facts).state == POSSIBLE:
                open_[c.name] = c
    if not open_:
        return None
    with answering(facts, dict.fromkeys(open_, "yes")):
        got = climb(work, rungs, facts, need, costs, prove=False)
    asked = tuple(c for c in (got.rung.changes if got.rung else ())
                  if c.name in open_)
    if not asked:
        return None
    with answering(facts, dict.fromkeys(open_, "no")):
        instead = climb(work, rungs, facts, need, costs, prove=False)
    return Offer(work, got.rung, asked, instead)


def prune(rungs, facts, need=()):
    """([rung that holds], [(rung, why it does not)]): a rung lacking a
    needed capability, or one of whose needs does not hold, is out, with
    the reason in migkit's words - and so is one whose change on the
    source is neither there nor answered yes (`Change`)."""
    kept, out = [], []
    for r in rungs:
        cap = r.lacks(need)
        why = lacking(cap, r) if cap is not None else None
        for n in () if why else r.needs:
            why = n.missing(r, facts)
            if why:
                break
        for c in () if why else r.changes:
            why = c.missing(r, facts)
            if why:
                break
        if why:
            out.append((r, why))
        else:
            kept.append(r)
    return kept, out


#: told of every way chosen, as it is chosen: the run's evidence keeps
#: them with their reasons (`session.Decisions`)
LISTENERS = []


def _told(fn):
    """`fn` - `climb`, `compose` - telling `LISTENERS` what it chose. A
    climb asked in passing (`prove=False`: what a way would be, not the
    way taken) tells nobody, and a listener that fails is not the
    choice's problem."""
    import functools
    import inspect
    signature = inspect.signature(fn)

    @functools.wraps(fn)
    def choosing(*args, **kw):
        got = fn(*args, **kw)
        if LISTENERS and signature.bind(*args, **kw).arguments.get(
                "prove", True) is not False:
            for listen in list(LISTENERS):
                try:
                    listen(got)
                except Exception:  # noqa: BLE001 - the choice stands
                    pass
        return got
    return choosing


@_told
def climb(work, rungs, facts=None, need=(), costs=None, keep=None,
          prove=True, unit=None, explore=True):
    """The `Choice` for one unit of work from its `rungs`.

    `need` is what the work must have (capabilities); `facts` what the
    needs and proofs read (a dict, or `Facts` to read lazily); `costs`
    where the rungs' measurements are (`Costs`, `HopCosts`, any mapping of
    name to seconds a unit - or a `costmodel.CostModel`, which answers
    for `unit` with a spread and predicts what was never measured); `keep`
    the rung a restart stands on (`remembered`), taken first where it
    still holds and proves. Where the rungs say their guarantees, only
    those reaching the strongest any of them does are ranked (`_strongest`).
    `explore` False commits: a rung only predicted ranks above one
    measured only where it is faster even at its p90.

    The reason is why the ways the list prefers were passed over, or the
    chosen rung's own `because` where none was: a way the list puts below
    the chosen one is no reason for it. The runner-up, the costs and the
    facts read are kept on the `Choice` (`as_evidence`)."""
    facts = facts if facts is not None else Facts()
    rungs = tuple(rungs)
    at = {id(r): i for i, r in enumerate(rungs)}
    kept, out = prune(rungs, facts, need)
    kept, short = _strongest(kept)
    out = sorted(out + short, key=lambda x: at[id(x[0])])
    priced = _priced(kept, costs, unit)
    ranked = _ranked(kept, priced, explore)
    order, passed = list(ranked), []
    if keep:
        held = [r for r in ranked if r.name == keep]
        order = held + [r for r in ranked if r.name != keep]
        passed = [(r, f"the way the last run took: {why}")
                  for r, why in out if r.name == keep]
    costs_of = tuple((r.name, priced.get(r.name)) for r in ranked)
    asked = tuple(getattr(facts, "asked", ()) or ())
    for i, r in enumerate(order):
        why = r.prove(facts) if prove and r.prove else None
        if why:
            passed.append((r, f"{r.said} did not prove itself: {why}"))
            continue
        above = [w for o, w in out
                 if at[id(o)] < at[id(r)] and o.name != keep]
        above += [w for _, w in passed]
        if r.name == keep:
            reason = ("the way the last run took, kept so a restart goes"
                      " on the way it began")
        elif above:
            reason = _joined(above)
        else:
            reason = r.because or f"{r.said}: the first way that holds"
            timed = [o for o in ranked if _measured(priced.get(o.name))]
            mine = priced.get(r.name)
            if len(timed) > 1 and r in timed:
                reason += "; measured the cheapest of the ways timed here"
            elif mine is not None and not _measured(mine) and not timed:
                reason += f"; predicted the fastest here, {mine.said()}"
        runner = next(iter(order[i + 1:]), None)
        from . import guarantees as g
        return Choice(work, r, reason, tuple(out + passed), tuple(ranked),
                      costs_of, runner,
                      _why_not(r, runner, priced) if runner else "",
                      tuple(g.named(g.of([r])).items()), asked)
    return Choice(work, None,
                  _joined(w for _, w in out + passed) or "no way was given",
                  tuple(out + passed), tuple(ranked), costs_of, asked=asked)


def remember(where, work, name):
    """Keep that `work` stands on `name` - a rung's name, or a strategy's
    {part: name} - beside the position file `where` (the move's
    checkpoint, the tail's token), in a file of its own, since every engine
    writes its position in a shape of its own. Written whole or not at
    all."""
    _change(_kept_file(where),
            lambda got: got.__setitem__(str(work), name))


def _change(path, apply):
    """One read-modify-write of a kept-ways file at a time, across runs as
    well as threads: the tables of one move copy side by side, a tail and
    a `check` keep theirs beside the same position, and two of them each
    reading the file, adding their own and writing it back would lose one
    of the two (audit ST23)."""
    from . import durable
    durable.change_json(path, apply, kind="state", indent=1)


def remembered(where, work):
    """What `work` stood on last time, or None - and None once the position
    file itself is gone: no position, nothing to go on from."""
    if not os.path.exists(str(where)):
        return None
    return _read(_kept_file(where)).get(str(work)) or None


def forget(where):
    """The ways kept beside `where`, gone with the position."""
    try:
        os.unlink(_kept_file(where))
    except OSError:
        pass


def _kept_file(where):
    root, ext = os.path.splitext(str(where))
    return f"{root}-ways{ext or '.json'}"


def _read(path):
    """The ways kept beside a position, {} where none are kept yet. A file
    that cannot be read whole stops, naming it: read as none, a restart
    climbed another rung than the one its work stands on (audit ST9)."""
    from . import durable
    got = durable.read_state(path, "state")
    return got if isinstance(got, dict) else {}


def choose_kept(where, work, rungs, facts=None, need=(), costs=None):
    """`climb`, standing first on the way the last run took for `work`
    where its position `where` still exists, and keeping the answer
    there - what a tail, a leg or a side calls, so a restart climbs the
    same rung."""
    got = climb(work, rungs, facts, need, costs,
                keep=remembered(where, work))
    if got.rung is not None:
        remember(where, work, got.rung.name)
    return got


# --- a way that fails as it runs: the next one, by itself -------------------
# The owner, 2026-09-30: "if one way is blocked or fails, switch to another
# way by itself - automatically, don't ask, just say it in the log". The
# climb already passes over a way whose needs do not hold and one whose
# proof fails; `attempt` goes on past one that fails as it runs. An
# operational failure - refused, a privilege or a feature missing, a
# program that exited or crashed, a server out of reach, a timeout, a
# proof step that could not run - has what the rung began undone (its own
# `cleanup`, then the caller's `undo`; a range or a batch replaced by key
# is idempotent, so the next rung converges on what it left), is kept
# beside the work's position with the reason and the facts the rung's
# needs read, is said in one line, and the climb goes on to the next rung
# that holds. Every rung the climb offers still holds the work's `need`,
# so a switch never lands on a way that gives less than the hop asks. A
# verdict is never a failure: a difference found, a digest that
# disagrees, a proof that said "different" is the answer, and is raised as
# it is - a false negative is the worst thing migkit can say. The only
# question asked is a change on the source (`offered`): a "no" is kept and
# climbed past like any other way that cannot run.

class Blocked(SystemExit):
    """A way could not run here, and what it began is undone: another way
    may. A SystemExit, so a caller with no other way stops on its words
    exactly as it did before there was one."""


class Verdict(Exception):
    """A way ran and its answer is a difference: never a reason to try
    another way."""


class Exhausted(SystemExit):
    """No way of the work could run: every rung's reason, in one message."""

    def __init__(self, work, reasons):
        self.work, self.reasons = work, list(reasons)
        super().__init__(f"no way of {work} could run here: "
                         + _joined(f"{r.said}: {why}" for r, why in reasons))


#: the modules whose errors are a server's own refusal or a lost
#: connection - a way that could not run, not a fault in migkit
_DRIVERS = ("pymysql", "psycopg", "psycopg2", "pymongo", "pymssql",
            "pyodbc", "mssql_python", "oracledb", "redis", "kafka",
            "confluent_kafka", "cassandra", "clickhouse_connect",
            "clickhouse_driver", "sqlite3", "duckdb", "botocore", "boto3",
            "google", "azure", "opensearchpy", "ibm_db", "snowflake",
            # DuckDB's errors are its compiled module's (`_duckdb`), as
            # Db2's are its DB-API layer's
            "_duckdb", "redshift_connector", "ibm_db_dbi", "grpc")


def operational(e):
    """Whether `e` is a way failing to run - refused, missing, exited,
    unreachable, timed out - rather than a verdict, migkit's own stop
    (SystemExit, said as it is), or a fault in migkit that must be seen."""
    import subprocess
    if isinstance(e, Blocked):
        return True
    if isinstance(e, (Verdict, SystemExit)) or not isinstance(e, Exception):
        return False
    if isinstance(e, RecursionError):
        return False
    if isinstance(e, (OSError, TimeoutError, RuntimeError,
                      subprocess.SubprocessError)):
        return True
    return (type(e).__module__ or "").split(".")[0] in _DRIVERS


def only_blocked(e):
    """`attempt`'s `switch` for a way whose work cannot be put back once
    it is under way: only the failures it raised as `Blocked` switch."""
    return isinstance(e, Blocked)


def _stable(v):
    """A value as the same text from one run to the next: a set's order
    is not, a dict's is kept sorted."""
    if isinstance(v, (set, frozenset)):
        return sorted(_stable(x) for x in v)
    if isinstance(v, dict):
        return sorted((str(k), _stable(x)) for k, x in v.items())
    if isinstance(v, (list, tuple)):
        return [_stable(x) for x in v]
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return repr(v)


class _Reading:
    """The facts, recording what is read of them."""

    def __init__(self, facts):
        self._facts, self.read = facts, {}

    def __getitem__(self, key):
        got = self._facts[key]
        self.read[key] = got
        return got

    def get(self, key, default=None):
        got = self._facts.get(key, default)
        self.read[key] = got
        return got

    def __contains__(self, key):
        return key in self._facts

    def __setitem__(self, key, value):
        self._facts[key] = value

    def pop(self, key, *default):
        return self._facts.pop(key, *default)

    def update(self, *a, **kw):
        return self._facts.update(*a, **kw)

    def __getattr__(self, name):
        return getattr(self._facts, name)


def seen(rung, facts):
    """What the rung's needs and changes read of the facts, as one short
    digest: a rung given up on is tried again once it differs - the fact
    that stopped it has changed."""
    reading = _Reading(facts)
    for n in rung.needs:
        try:
            n.holds(reading)
        except Exception:  # noqa: BLE001 - a need that cannot be read
            pass
    for c in rung.changes:
        try:
            c.holds(reading)
        except Exception:  # noqa: BLE001 - the same
            pass
    text = json.dumps(_stable({"rung": rung.name, **{
        str(k): v for k, v in reading.read.items()}}), default=str)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _up_key(work):
    return f"{work} given up"


def given_up(where, work):
    """{rung name: {"why", "seen"}} this work gave up on beside `where`."""
    if where is None:
        return {}
    got = _read(_kept_file(where)).get(_up_key(work))
    return dict(got) if isinstance(got, dict) else {}


def _keep_given_up(where, work, name, rec):
    _change(_kept_file(where),
            lambda got: got.setdefault(_up_key(work), {}).__setitem__(
                name, rec))


def _cost_words(costs, failed, instead):
    """What the switch costs, where both ways were timed here."""
    if costs is None or instead is None:
        return ""
    a, b = costs.get(failed.name), costs.get(instead.name)
    if a is None or b is None or not a:
        return ""
    return (f"; {instead.said} was measured {b / a:,.1f} times the time a"
            f" unit of {failed.said} took")


def attempt(work, rungs, run, facts=None, need=(), costs=None, where=None,
            keep=None, log=None, undo=None, answer=None, prove=True,
            otherwise=None, switch=operational, record=True):
    """(the `Choice`, what `run` returned): the work done on the first rung
    that runs.

    `run(choice)` does the work on `choice.rung`. An operational failure
    of it (`operational`) undoes what the rung began - `rung.cleanup(facts,
    error)`, then `undo(rung, error)` - keeps the rung given up with its
    reason beside `where` (the work's position file; None keeps it for
    this call only), says one line through `log`, and climbs again. A
    rung given up by an earlier run is passed over while what its needs
    read is unchanged (`seen`); where every rung left was given up
    earlier, the one given up last is tried again - the one the work
    stood on most recently - and that is said. `answer(offer)` is
    asked once, where a change on the source would let a faster way run
    (`offered`) - "yes", "no", "dba", or None for none. `otherwise` is the
    way the caller takes where no rung is left, in words, for the line
    that says the switch. `switch(error)` narrows which failures switch,
    where a way's work cannot be undone once it is under way (a stream
    that ran for an hour, a target replaced by a copy): there only what
    the way itself raises as `Blocked` - it failed before anything it
    cannot put back. What cannot be undone stops everything, with both
    reasons; no rung left stops with every rung's reason (`Exhausted`).
    `record` False reads what `where` keeps and writes nothing to it or
    to `costs`: a dry run climbs the ladder `--go` climbs, the ways given
    up included, and leaves everything as it found it."""
    facts = facts if facts is not None else Facts()
    rungs = tuple(rungs)
    say = log or (lambda m: None)
    if answer is not None:
        off = offered(work, rungs, facts, need, costs)
        if off is not None:
            got = answer(off)
            if got in ANSWERS:
                for c in off.changes:
                    facts[answer_key(c.name)] = got
    if keep is None and where is not None:
        keep = remembered(where, work)
    earlier = given_up(where, work)
    by_name = {r.name: r for r in rungs}
    lifted = set()
    for name in list(earlier):
        r = by_name.get(name)
        if r is None or earlier[name].get("seen") != seen(r, facts):
            earlier.pop(name)
            lifted.add(name)
    now, last_error = {}, None

    def ladder():
        out = {**earlier, **now}
        return tuple(dataclasses.replace(r, needs=tuple(r.needs) + (Need(
            "not given up on here", lambda f, n=r.name: n not in out,
            lambda f, n=r.name: f"{out[n]['why']} (given up on here)"),))
            for r in rungs)
    while True:
        choice = climb(work, ladder(), facts, need, costs, keep=keep,
                       prove=prove)
        if choice.rung is None and earlier and not now:
            # nothing else is left: the way given up last is tried again
            # first - the one the work stood on most recently, whose
            # position a restart goes on from
            last = max(earlier, key=lambda n: earlier[n].get("at") or 0)
            say(f"{work}: every way left was given up by an earlier run;"
                f" trying {by_name[last].said} again")
            earlier.pop(last)
            continue
        if choice.rung is None:
            raise Exhausted(work, choice.passed) from last_error
        rung = choice.rung
        if rung.name in lifted:
            say(f"{work}: {rung.said} is tried again - what stopped it last"
                " time has changed")
            lifted.discard(rung.name)
        try:
            result = run(choice)
        except BaseException as e:
            if not operational(e) or not switch(e):
                raise
            why, last_error = _first(e), e
            if record and callable(getattr(costs, "outcome", None)):
                # its chance of failing, for the next climb here and the
                # next hop of these engines (`costmodel.risk`)
                costs.outcome(rung.name, False, why)
            try:
                if rung.cleanup is not None:
                    rung.cleanup(facts, e)
                if undo is not None:
                    undo(rung, e)
            except BaseException as err:
                raise SystemExit(
                    f"{work}: {rung.said} failed ({why}), and what it began"
                    f" could not be undone ({_first(err)}) - no other way"
                    " is tried over it") from err
            now[rung.name] = {"why": why, "seen": seen(rung, facts),
                              "at": time.time()}
            if where is not None and record:
                _keep_given_up(where, work, rung.name, now[rung.name])
            nxt = climb(work, ladder(), facts, need, costs,
                        prove=False).rung
            say(f"{work}: {rung.said} could not run ({why})"
                + (f" - switching to {nxt.said}"
                   + _cost_words(costs, rung, nxt) if nxt is not None
                   else f" - switching to {otherwise}" if otherwise
                   else " - no other way is left"))
            keep = None
            continue
        if where is not None and record:
            remember(where, work, rung.name)
        if record and callable(getattr(costs, "outcome", None)):
            costs.outcome(rung.name, True)
        return choice, result


# --- a strategy composed of parts -------------------------------------------

PARTS = ("read", "chunk", "write", "verify", "resume", "mark")


@dataclass(frozen=True)
class Part:
    name: str
    rungs: tuple

    @property
    def owns(self):
        """The capabilities any of this part's rungs gives: what the part
        answers for, so a need another part gives is not held against
        it."""
        out = set()
        for r in self.rungs:
            out |= set(r.gives)
        return out


@dataclass(frozen=True)
class Strategy:
    work: str
    parts: dict = field(default_factory=dict)    #: {part name: Rung}
    reasons: dict = field(default_factory=dict)  #: {part name: reason}
    why_not: str = ""                             #: when nothing fits
    #: what the whole strategy costs on the unit (`space`), the guarantees
    #: it reaches ({guarantee: level}), and the runners-up with why not:
    #: ((Strategy, why not), ...)
    estimate: object = None
    guarantees: dict = field(default_factory=dict)
    runners: tuple = ()

    def as_evidence(self):
        """The strategy as data, in the operator's words."""
        def cost(e):
            return e.as_evidence() if hasattr(e, "as_evidence") else e
        return {"work": str(self.work),
                "parts": {p: r.said for p, r in self.parts.items()},
                "reasons": dict(self.reasons), "why_not": self.why_not,
                "predicted": cost(self.estimate),
                "guarantees": dict(self.guarantees),
                "runners_up": [{"parts": {p: r.said
                                          for p, r in s.parts.items()},
                                "predicted": cost(s.estimate),
                                "why_not": why}
                               for s, why in self.runners]}

    def __bool__(self):
        return bool(self.parts)

    @property
    def names(self):
        """{part: rung name}, what `remember` keeps."""
        return {p: r.name for p, r in self.parts.items()}

    @property
    def said(self):
        return ", ".join(f"{p}: {r.said}" for p, r in self.parts.items())


class _Scoped:
    """A part's view of the costs: its rungs are kept as `part/rung`, so
    two parts may each have a rung of the same name."""

    def __init__(self, costs, part):
        self.costs, self.part = costs, part

    def get(self, name, default=None):
        if self.costs is None:
            return default
        return self.costs.get(f"{self.part}/{name}", default)


def _unfit(chosen):
    """Why this combination of rungs does not go together, or None: every
    rung's own rule, over the whole combination."""
    for r in chosen.values():
        if r.fits is None:
            continue
        ok = r.fits(chosen)
        if ok is True:
            continue
        if ok:
            return str(ok)
        return f"{r.said} does not go with the rest of the way"
    return None


@_told
def compose(work, parts, facts=None, need=(), costs=None, keep=None):
    """The `Strategy` for `work`: one rung per part, the parts fitting each
    other, every rung proved.

    Each part's rungs are held to the needs that part answers for; the
    combinations are ranked as `climb` ranks rungs, by the sum of their
    parts' measured costs (a part's rung costed as `part/rung`), never
    timed first, the footprint breaking a tie. `keep` is a strategy's
    `names` to stand on first where it still fits. A need no part gives is
    said before anything is tried."""
    facts = facts if facts is not None else Facts()
    parts = [p if isinstance(p, Part) else Part(*p) for p in parts]
    given = set()
    for p in parts:
        given |= p.owns
    missing = [c for c in need if c not in given]
    if missing:
        return Strategy(work, why_not="no way here gives what it needs: "
                                      + ", ".join(missing))
    ladders, out = [], {}
    for p in parts:
        kept, dropped = prune(p.rungs, facts,
                              tuple(c for c in need if c in p.owns))
        out[p.name] = dropped
        if not kept:
            return Strategy(work, why_not=_joined(w for _, w in dropped))
        ladders.append(rank(kept, _Scoped(costs, p.name)))
    scoped = [_Scoped(costs, p.name) for p in parts]
    entries = []
    for combo in itertools.product(*ladders):
        cs = [s.get(r.name) for s, r in zip(scoped, combo)]
        entries.append((None if None in cs else sum(cs),
                        sum(r.footprint for r in combo),
                        tuple(lad.index(r) for lad, r in zip(ladders, combo)),
                        combo))
    order = _order(entries)
    if keep:
        order.sort(key=lambda c: tuple(r.name for r in c) != tuple(
            keep.get(p.name) for p in parts))
    names = [p.name for p in parts]
    failed, unfit_with, first_unfit = {}, {}, None
    for combo in order:
        chosen = dict(zip(names, combo))
        why = _unfit(chosen)
        if why:
            first_unfit = first_unfit or why
            for n, r in chosen.items():
                unfit_with.setdefault((n, r.name), why)
            continue
        bad = False
        for n, r in chosen.items():
            if (n, r.name) not in failed:
                failed[(n, r.name)] = r.prove(facts) if r.prove else None
            bad = bad or bool(failed[(n, r.name)])
        if bad:
            continue
        reasons = {}
        for p, lad, r in zip(parts, ladders, combo):
            above = [w for o, w in out[p.name]
                     if p.rungs.index(o) < p.rungs.index(r)]
            for o in lad[:lad.index(r)]:
                if failed.get((p.name, o.name)):
                    above.append(f"{o.said} did not prove itself:"
                                 f" {failed[(p.name, o.name)]}")
                elif (p.name, o.name) in unfit_with:
                    above.append(unfit_with[(p.name, o.name)])
                else:
                    above.append(f"the whole way costs less without"
                                 f" {o.said}")
            reasons[p.name] = _joined(above) or r.because or (
                f"{r.said}: the first way that fits")
        return Strategy(work, chosen, reasons)
    proofs = _joined(w for w in failed.values() if w)
    return Strategy(work, why_not=_joined([first_unfit or "", proofs])
                    or "no combination of the parts fits")


# --- every way of a strategy, weighed (backlog P0-SMART, note A) ------------
# `compose` ranks the combinations of the parts and takes the first that
# fits and proves. `space` weighs every one of them: each combination that
# fits, its guarantees computed from its parts (`guarantees.of`) - only
# those reaching the strongest any combination reaches go on - each costed
# on the unit and ranked. The enumeration is a depth-first walk that leaves
# a subtree out only where a bound proves nothing in it can be taken: its
# best possible guarantees are below a combination already found
# (`guarantees.optimistic`). A combination whose cheapest possible cost is
# above the p90 of the best one is counted slower beyond doubt. What the
# walk found of the combinations' fit and guarantees - not their costs,
# which move with every unit - is kept for the same parts and need.

#: the structural walks kept, the oldest dropped past this many
SPACES_KEPT = 256
_SPACES = {}


@dataclass
class Space:
    """Every way a work can go, weighed: the one taken (`best`), the ones
    ranked behind it, the strongest guarantees reached and how many
    combinations were weighed, left out and why."""
    work: str
    best: object
    ranked: list = field(default_factory=list)
    strongest: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)
    weaker: list = field(default_factory=list)

    def said(self):
        """One line for the decision record: how much was weighed."""
        c = self.counts
        return (f"{self.work}: {c.get('combinations', 0):,} combinations,"
                f" {c.get('evaluated', 0):,} weighed ({c.get('unfit', 0):,}"
                f" do not fit, {c.get('weaker', 0):,} give less than the"
                f" strongest, {c.get('bounded', 0):,} left out by a bound,"
                f" {c.get('dominated', 0):,} slower beyond doubt)")

    def as_evidence(self):
        return {"work": str(self.work),
                "chosen": self.best.as_evidence() if self.best else None,
                "strongest": dict(self.strongest),
                "counts": dict(self.counts),
                "weaker": [{"parts": [r.said for r in c], "why": why}
                           for c, why in self.weaker[:20]]}


def _part_cost(costs, part, rung, unit):
    if costs is None:
        return None
    name = f"{part}/{rung.name}"
    if _estimating(costs):
        rungs = getattr(costs, "rungs", None)
        if isinstance(rungs, dict) and name not in rungs:
            rungs[name] = rung
    return costed(dataclasses.replace(rung, name=name), costs, unit)


def _sum_costs(cs):
    """The cost of a combination: None where a part's is unknown, the sum
    otherwise - an `Estimate`'s spreads added as variances."""
    if any(c is None for c in cs):
        return None
    if not any(hasattr(c, "mean") for c in cs):
        return sum(cs)
    from .costmodel import MEASURED_TRUTH, PREDICTED, Estimate
    return Estimate(sum(c.mean for c in cs),
                    math.sqrt(sum(c.sd ** 2 for c in cs)),
                    MEASURED_TRUTH if all(c.measured for c in cs)
                    else PREDICTED, "the parts' own",
                    min(c.n for c in cs), min(c.dof for c in cs),
                    max(c.noise for c in cs))


def _low(c):
    if c is None:
        return 0.0
    return c.low() if hasattr(c, "low") else float(c)


def _high(c):
    if c is None:
        return math.inf
    return c.high() if hasattr(c, "high") else float(c)


def _structure(names, ladders):
    """Every combination that fits with its guarantees, what the walk
    counted, the first reason one did not fit, and {(part, rung name):
    why a combination with it did not fit}."""
    from . import guarantees as g
    key = (tuple(names), tuple(tuple(id(r) for r in lad)
                               for lad in ladders))
    got = _SPACES.get(key)
    if got is not None and all(len(la) == len(lb) and all(
            a is b for a, b in zip(la, lb))
            for la, lb in zip(got[0], ladders)):
        return got[1]
    strong = [sorted(lad, key=lambda r: g.vector(g.of([r])), reverse=True)
              for lad in ladders]
    fits, counts = [], {"evaluated": 0, "unfit": 0, "bounded": 0}
    best = [None]
    unfit_with, first = {}, [None]
    sizes = [len(lad) for lad in ladders]

    def left(i):
        n = 1
        for k in sizes[i:]:
            n *= k
        return n

    def walk(i, chosen):
        if best[0] is not None and i < len(ladders):
            if g.vector(g.optimistic(chosen, strong[i:])) < best[0]:
                counts["bounded"] += left(i)
                return
        if i == len(ladders):
            counts["evaluated"] += 1
            combo = tuple(chosen)
            why = _unfit(dict(zip(names, combo)))
            if why:
                counts["unfit"] += 1
                first[0] = first[0] or why
                for n, r in zip(names, combo):
                    unfit_with.setdefault((n, r.name), why)
                return
            lv = g.of(combo)
            v = g.vector(lv)
            if best[0] is None or v > best[0]:
                best[0] = v
            fits.append((combo, lv))
            return
        for r in strong[i]:
            walk(i + 1, chosen + [r])
    walk(0, [])
    counts["combinations"] = left(0)
    # the combinations in the ladders' own order, as `compose` ranks them
    at = [{id(r): k for k, r in enumerate(lad)} for lad in ladders]
    fits.sort(key=lambda x: tuple(at[i][id(r)] for i, r in
                                  enumerate(x[0])))
    out = (fits, counts, first[0], unfit_with)
    if len(_SPACES) >= SPACES_KEPT:
        _SPACES.pop(next(iter(_SPACES)))
    _SPACES[key] = (tuple(tuple(lad) for lad in ladders), out)
    return out


def space(work, parts, facts=None, need=(), costs=None, keep=None,
          unit=None, explore=True, runners=3):
    """Every way `work` can go, weighed: a `Space` whose `best` is the
    `Strategy` taken - the fastest of the combinations that fit and reach
    the strongest guarantees any combination does, proved - with its cost
    on `unit`, its guarantees and its `runners` runners-up, each with why
    not. `compose`'s arguments, and its rules for what is never measured,
    the footprint and a kept strategy."""
    from . import guarantees as g
    facts = facts if facts is not None else Facts()
    parts = [p if isinstance(p, Part) else Part(*p) for p in parts]
    given = set()
    for p in parts:
        given |= p.owns
    missing = [c for c in need if c not in given]
    if missing:
        return Space(work, Strategy(work, why_not="no way here gives what"
                                    " it needs: " + ", ".join(missing)))
    ladders, out = [], {}
    for p in parts:
        kept, dropped = prune(p.rungs, facts,
                              tuple(c for c in need if c in p.owns))
        out[p.name] = dropped
        if not kept:
            return Space(work, Strategy(work, why_not=_joined(
                w for _, w in dropped)))
        ladders.append(kept)
    names = [p.name for p in parts]
    fits, counts, first_unfit, unfit_with = _structure(names, ladders)
    counts = dict(counts)
    if not fits:
        return Space(work, Strategy(work, why_not=first_unfit or "no"
                                    " combination of the parts fits"),
                     counts=counts)
    top = max(g.vector(lv) for _, lv in fits)
    admissible, weaker = [], []
    for combo, lv in fits:
        if g.vector(lv) >= top:
            admissible.append((combo, lv))
        else:
            weaker.append((combo, lv))
    counts["weaker"] = len(weaker)
    part_cost = [{r.name: _part_cost(costs, n, r, unit) for r in lad}
                 for n, lad in zip(names, ladders)]
    best_high = min((_high(_sum_costs([part_cost[i][r.name] for i, r in
                                       enumerate(c)]))
                     for c, _ in admissible), default=math.inf)
    # a combination whose cheapest is above the best one's dearest is
    # slower beyond doubt: counted as such, and still ranked behind, so
    # the runners-up are what they are
    costed_, dominated = [], 0
    for combo, lv in admissible:
        cs = [part_cost[i][r.name] for i, r in enumerate(combo)]
        if None not in cs and sum(_low(c) for c in cs) > best_high:
            dominated += 1
        costed_.append((combo, lv, _sum_costs(cs)))
    counts["dominated"] = dominated
    entries = [(total, sum(r.footprint for r in combo),
                tuple(lad.index(r) for lad, r in zip(ladders, combo)),
                (combo, lv, total))
               for combo, lv, total in costed_]
    if any(hasattr(e[0], "mean") for e in entries):
        order = _order_estimates(entries, explore)
    else:
        order = _order(entries)
    if keep:
        order.sort(key=lambda x: tuple(r.name for r in x[0]) != tuple(
            keep.get(n) for n in names))
    failed, chosen = {}, None
    for item in order:
        bad = False
        for n, r in zip(names, item[0]):
            if (n, r.name) not in failed:
                failed[(n, r.name)] = r.prove(facts) if r.prove else None
            bad = bad or bool(failed[(n, r.name)])
        if not bad:
            chosen = item
            break
    ranked = [(x[0], x[2]) for x in order]
    strongest = g.named(dict(zip(g.ORDER, top)))
    said_weaker = [(c, g.short(lv, top)) for c, lv in weaker]
    if chosen is None:
        return Space(work, Strategy(work, why_not=_joined(
            w for w in failed.values() if w) or "no combination proved"),
            ranked, strongest, counts, said_weaker)
    combo, lv, total = chosen
    reasons = {}
    for i, (p, lad, r) in enumerate(zip(parts, ladders, combo)):
        above = [w for o, w in out[p.name]
                 if p.rungs.index(o) < p.rungs.index(r)]
        for o in lad[:lad.index(r)]:
            if failed.get((p.name, o.name)):
                above.append(f"{o.said} did not prove itself:"
                             f" {failed[(p.name, o.name)]}")
            elif not any(c[i] is o for c, _ in fits):
                above.append(unfit_with.get((p.name, o.name))
                             or f"{o.said} does not go with the rest")
            elif not any(c[i] is o for c, _ in admissible):
                # the strongest any way with it reaches, and where that
                # falls short
                lv = max((x for c, x in weaker if c[i] is o),
                         key=g.vector)
                above.append(f"{o.said} gives {g.short(lv, top)} with any"
                             " way the other parts can go")
            else:
                above.append(f"the whole way costs less without {o.said}")
        reasons[p.name] = _joined(above) or r.because or (
            f"{r.said}: the first way that fits")
    behind = []
    for c, t in ranked:
        if c is combo or len(behind) >= runners:
            continue
        behind.append((Strategy(work, dict(zip(names, c)), estimate=t,
                                guarantees=g.named(g.of(c))),
                       _strategy_why_not(total, t, combo, c)))
    best = Strategy(work, dict(zip(names, combo)), reasons, estimate=total,
                    guarantees=g.named(lv), runners=tuple(behind))
    for listen in list(LISTENERS):
        try:
            listen(best)
        except Exception:  # noqa: BLE001 - the choice stands
            pass
    return Space(work, best, ranked, strongest, counts, said_weaker)


def _strategy_why_not(a, b, mine, other):
    """Why the combination `other`, costing `b`, was not taken over
    `mine`, costing `a`."""
    if a is None or b is None:
        return "not every part of it was measured or predicted here"
    if hasattr(a, "mean"):
        if _tied_estimates(a, b):
            more = sum(r.footprint for r in other) > sum(
                r.footprint for r in mine)
            return (f"within the noise ({b.said()} against {a.said()})"
                    + (", and it leaves more behind" if more else ""))
        return (f"{'measured' if b.measured else 'predicted'} slower:"
                f" {b.said()} against {a.said()}")
    if _tied(a, b):
        return "within what timing a busy server twice can give"
    return (f"measured {b / a:,.1f} times the time" if a
            else "measured slower here")


# --- the run's decision record (backlog P0-SMART, the owner's note on D) -----
# The owner, 2026-09-30: "the logic and the choices inside - the user
# doesn't need to know them". The console says outcomes: what happens to
# each side, the time with its spread, the guarantees reached, what the
# operator must do or approve, a line where a way switched. Every choice
# in full - the strategies weighed, the runners-up and why not, the facts
# that decided it and the ones not known, predictions against
# measurements - goes to the run's decision record (the hop's
# `decisions.jsonl`, which the run keeps every choice in as it is made,
# `session.Decisions`), to `Choice.as_evidence` for the evidence export,
# and to the console only where `MIGKIT_EXPLAIN=1`.


def explaining():
    """Whether the console shows the decision record as well."""
    return os.environ.get("MIGKIT_EXPLAIN", "").strip().lower() in (
        "1", "yes", "true", "on")


def _record_path(hop):
    from .session import DECISIONS
    return os.path.join(str(hop.report_dir()), DECISIONS)


def record(hop, what, say=None, work=None, way=None, reason=""):
    """One decision in full - a `Choice`, a `Strategy`, a `Space`, a joint
    plan, or a dict - appended to the run's decision record in the run's
    own shape (work, way, reason, what was passed over) with the whole of
    it under "detail", and said through `say` line by line where
    `explaining()`. `hop` None keeps it nowhere."""
    from . import session
    detail = what.as_evidence() if hasattr(what, "as_evidence") \
        else dict(what)
    if hasattr(what, "reason") or hasattr(what, "parts"):
        head = session.choice_record(what)
    else:
        head = {"work": str(work or detail.get("work") or ""),
                "way": way or "", "reason": reason, "passed": []}
    got = {"session": session.current(),
           "at": time.strftime("%Y-%m-%d %H:%M:%S"), **head,
           "detail": detail}
    if hop is not None:
        try:
            path = _record_path(hop)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a") as f:
                f.write(json.dumps(got, default=str) + "\n")
        except (OSError, AttributeError, TypeError):
            pass
    if say is not None and explaining():
        for line in explained(detail):
            say(line)
    return got


def explained(got, indent="  ", most=12):
    """A decision record entry as lines to read, `most` items a level."""
    out = []

    def said(x):
        return f"{x:.3g}" if isinstance(x, float) else str(x)

    def walk(v, depth):
        pad = indent * depth
        items = (list(v.items()) if isinstance(v, dict)
                 else list(enumerate(v)))
        for k, x in items[:most]:
            if x in (None, [], {}, ""):
                continue
            head = f"{pad}{k}:" if isinstance(v, dict) else f"{pad}-"
            if isinstance(x, (dict, list)):
                out.append(head)
                walk(x, depth + 1)
            else:
                out.append(f"{head} {said(x)}")
        if len(items) > most:
            out.append(f"{pad}... and {len(items) - most:,} more")
    walk(got, 0)
    return ["decision:"] + [indent + line for line in out]


def recorded(hop, last=200):
    """The last entries of the run's decision record of `hop`."""
    try:
        with open(_record_path(hop)) as f:
            lines = f.readlines()[-last:]
    except (OSError, AttributeError, TypeError):
        return []
    out = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


# --- every shape has a way, engine by engine --------------------------------
# Declared as `capabilities.GAPS` is. A `yes` names where in the code the
# way is: a dotted name under `migkit`, or `@method` for the engine's own
# class - resolved by the test, a base class's placeholder not counting, so
# a way removed from the code turns its cell stale. A gap says why, or the
# backlog item it waits on. A cell nobody declared is a failure, not a
# guess. `yes` means migkit carries that shape correctly today; how fast
# and how resumably is the rungs' business (a table without a key or with a
# key of text goes by where its rows are stored, by buckets of its rows'
# hash, or in one pass - `ranges.keyless`).

SHAPES = {
    "keyless": "a table without a key",
    "no unique index": "a table with no unique index",
    "wide key": "a key of many columns or of text",
    "too large": "a table too large for one range",
    "large values": "a table with large values",
    "partitioned": "a partitioned table",
    "read-only source": "a source that is read-only or a standby",
    "missing grant": "a user without the grant a program needs",
    "read-only target": "a target that must stay read-only",
    "slow link": "a link too slow for one stream",
    "writes held": "a side whose writes are held and let go again, read"
                   " back - the source at cutover",
}

_BY_KEY_ORDER = ("read by {}, never in key order, so the key's shape does"
                 " not arise")
_NO_PROGRAM = ("no program is driven for this engine; migkit's own reader"
               " needs only to read")

_DBAPI = ("mssql", "oracle", "db2", "db2i", "db2zos", "ase", "redshift",
          "snowflake", "bigquery", "duckdb", "teradata")
#: sources read through an application's own interface, never written
_READ_ONLY = ("sapodp", "saas")
_FILES = ("sqlite", "duckdb", "parquet", "copybook")
#: the generic engine's, for the shapes of a side kept from writers: it
#: compares two databases and writes to neither
_COMPARES_ONLY = ("it compares two databases and writes to neither: nothing"
                  " of migkit's lands on its target, and no cutover drains"
                  " through it")
_NONE = {**{n: (NOT_APPLICABLE, STREAM_ONLY.format(said, n))
            for n, said in (("kinesis", "Kinesis"), ("pubsub", "Pub/Sub"))},
         "generic": (NOT_YET, "33")}
#: the cloud-native services held - a source at cutover, a target kept -
#: by the hold of the API or the wire they speak (`freeze.HOLDS`)
_HELD_AS = (("gaussdb", "PostgresHold"), ("oceanbase", "MySQLHold"),
            ("polardbx", "MySQLHold"), ("dcdb", "MySQLHold"),
            ("oceanbase-oracle", "OracleHold"),
            ("geminidb-cassandra", "CassandraHold"),
            ("geminidb-redis", "RedisHold"), ("geminidb-mongo", "MongoHold"))
#: and those whose writes the cloud's access policies allow
_BY_POLICY = ("spanner", "bigtable", "firestore", "datastore", "keyspaces",
              "neptune", "timestream")
_CLOUD_POLICIES = ("its writes are allowed by the cloud's access policies,"
                   " outside the database, which migkit does not change: its"
                   " operator holds them")

#: {shape: {engine or "*": ("yes", where) | (NOT_YET, item) |
#: (NOT_APPLICABLE, why)}}; "*" is what an engine not named gets
WAYS = {
    "keyless": {
        **_NONE,
        "postgres": ("yes", "@_copy_in_spans"),
        "mysql": ("yes", "@_move_table"),
        "hetero": ("yes", "@_copy_in_spans"),
        **{n: ("yes", "@neutral_batches")
           for n in ("sqlite", "clickhouse", "cassandra", "dynamodb",
                     "opensearch", "parquet", "iceberg", "delta") + _DBAPI},
        "mongodb": (NOT_APPLICABLE, "every document carries an _id"),
        # the engines on another's wire read a keyless table in one pass
        # through the one DB-API reader (ER4)
        **{n: ("yes", "@neutral_batches")
           for n in ("tidb", "cockroachdb", "dsql")},
        "couchbase": (NOT_APPLICABLE, "every document is kept by its key"),
        "cosmosdb": (NOT_APPLICABLE, "every item carries an id within its"
                                     " partition key"),
        # the cloud-native services (P0-CLOUDS part 2): the SQL ones read a
        # keyless table in one pass through their DB-API or MySQL reader;
        # the rest keep every row by a key of their own
        **{n: ("yes", "@neutral_batches")
           for n in ("gaussdb", "oceanbase", "oceanbase-oracle", "polardbx",
                     "dcdb", "keyspaces", "geminidb-cassandra")},
        "spanner": (NOT_APPLICABLE, "every Spanner table has a primary key"),
        "bigtable": (NOT_APPLICABLE, "every row is filed under its row key"),
        "firestore": (NOT_APPLICABLE, "every document is kept by its name"),
        "datastore": (NOT_APPLICABLE, "every entity is kept by its key"),
        "neptune": (NOT_APPLICABLE, "every vertex and edge is kept by its id,"
                                    " and every quad is its own key"),
        "timestream": (NOT_APPLICABLE, "every record is kept by its"
                                       " dimensions, measure name and time"),
        "lindorm": (NOT_APPLICABLE, "every wide table keeps its rows by a"
                                    " primary key"),
        "geminidb-redis": (NOT_APPLICABLE, "every value is under its key"),
        "geminidb-mongo": (NOT_APPLICABLE, "every document carries an _id"),
        "copybook": ("yes", "@position_spans"),
        "teradata": ("yes", "@row_bucket_sql"),
        **{n: ("yes", "@neutral_batches") for n in _READ_ONLY},
        "redis": (NOT_APPLICABLE, "every value is under its key"),
        "kafka": (NOT_APPLICABLE, "a message is addressed by its partition"
                                  " and offset, never by a key"),
    },
    "wide key": {
        **_NONE,
        "postgres": ("yes", "@_copy_in_spans"),
        "mysql": ("yes", "@_move_table"),
        "mongodb": ("yes", "@move_table"),
        "hetero": ("yes", "@_neutral_move"),
        **{n: ("yes", "@neutral_read") for n in ("sqlite", "clickhouse")
           + _DBAPI + ("tidb", "cockroachdb", "dsql")},
        "couchbase": (NOT_APPLICABLE, _BY_KEY_ORDER.format(
            "the key-value range scan")),
        "cosmosdb": (NOT_APPLICABLE, _BY_KEY_ORDER.format("feed ranges")),
        "cassandra": (NOT_APPLICABLE, _BY_KEY_ORDER.format("token ranges")),
        **{n: ("yes", "@neutral_read")
           for n in ("spanner", "gaussdb", "oceanbase", "oceanbase-oracle",
                     "polardbx", "dcdb", "lindorm")},
        "geminidb-mongo": ("yes", "@move_table"),
        **{n: (NOT_APPLICABLE, _BY_KEY_ORDER.format("token ranges"))
           for n in ("keyspaces", "geminidb-cassandra")},
        "bigtable": (NOT_APPLICABLE, "a row key is one string of bytes, read"
                                     " in its own order"),
        "firestore": (NOT_APPLICABLE, "a document's name is one path, read"
                                      " in its own order"),
        "datastore": (NOT_APPLICABLE, "an entity's key is one path, read in"
                                      " its own order"),
        "neptune": (NOT_APPLICABLE, _BY_KEY_ORDER.format(
            "one pass of the graph")),
        "timestream": (NOT_APPLICABLE, _BY_KEY_ORDER.format("time")),
        "geminidb-redis": (NOT_APPLICABLE, "a key is one string, read by the"
                                           " scan's cursor"),
        "dynamodb": (NOT_APPLICABLE, _BY_KEY_ORDER.format("scan segments")),
        "opensearch": (NOT_APPLICABLE, _BY_KEY_ORDER.format("scroll slices")),
        **{n: (NOT_APPLICABLE, _BY_KEY_ORDER.format("a file at a time"))
           for n in ("parquet", "iceberg", "delta")},
        "copybook": (NOT_APPLICABLE, _BY_KEY_ORDER.format(
            "ranges of whole records")),
        "teradata": (NOT_APPLICABLE, _BY_KEY_ORDER.format(
            "its rows' hash buckets, as they lie on the AMPs")),
        "sapodp": (NOT_APPLICABLE, _BY_KEY_ORDER.format(
            "the pages its service hands out")),
        "saas": (NOT_APPLICABLE, _BY_KEY_ORDER.format(
            "the records its program hands over")),
        "redis": (NOT_APPLICABLE, "a key is one string, read by the scan's"
                                  " cursor"),
        "kafka": (NOT_APPLICABLE, "a message's key is carried, never"
                                  " resumed by"),
    },
    "too large": {
        **_NONE,
        **{n: ("yes", "@native_bulk")
           for n in ("cassandra", "dynamodb", "opensearch", "parquet")},
        # read page by page, a page at a time held
        **{n: ("yes", "@neutral_batches") for n in _READ_ONLY},
        "*": ("yes", "@move_table"),
    },
    "large values": {
        **_NONE,
        "postgres": ("yes", "@_large_values"),
        "mysql": ("yes", "@_large_values"),
        "copybook": (NOT_APPLICABLE, "a field is as long as its copybook"
                                     " says, a record 32,760 bytes at most"
                                     " in a variable file"),
        **{n: (NOT_APPLICABLE, "a value arrives whole in the application's"
                               " answer: there is no piece of it to read"
                               " apart") for n in _READ_ONLY},
        "oracle": ("yes", "@_read_in_pieces"),
        # the one way of `pieces` - sized where they lie, batches cut by
        # bytes, read and written in pieces, hashed where they lie - each
        # engine's adapter its `LARGE_VALUES`
        **{n: ("yes", "@LARGE_VALUES") for n in (
            "mongodb", "mssql", "redis", "kafka", "sqlite", "parquet",
            "clickhouse", "db2", "db2i", "db2zos", "ase", "opensearch",
            "snowflake", "bigquery", "duckdb", "iceberg", "delta",
            "hetero")},
        "dynamodb": (NOT_APPLICABLE, "an item is 400 KB at most, whole"
                                     " under the 1 MB a scan page returns"),
        "cassandra": (NOT_APPLICABLE, "a row is written as one mutation, at"
                                      " most half a commit-log segment:"
                                      " 16 MB by default"),
        "redshift": (NOT_APPLICABLE, "a value is 16 MB at most (a VARBYTE"
                                     " or a SUPER) and a VARCHAR 65,535"
                                     " bytes, each read whole in batches"
                                     " cut by bytes"),
        "*": (NOT_YET, "R10"),
    },
    "partitioned": {
        **_NONE,
        "postgres": ("yes", "@check_deep"),
        "mysql": ("yes", "@check_deep"),
        "clickhouse": ("yes", "@_partitioned_by"),
        "kafka": ("yes", "@move_table"),
        "cassandra": (NOT_APPLICABLE, "every table is partitioned by its"
                                      " key; the copy goes by token ranges"),
        "dynamodb": (NOT_APPLICABLE, "every table is partitioned by its"
                                     " key; the copy goes by scan segments"),
        "opensearch": (NOT_APPLICABLE, "an index's shards are its own"
                                       " layout; the scroll reads across"
                                       " them"),
        "mongodb": (NOT_APPLICABLE, "collections are sharded, not"
                                    " partitioned; a sharded source reads"
                                    " as one through its router"),
        "couchbase": (NOT_APPLICABLE, "a bucket's vBuckets are its own"
                                      " layout; the range scan reads across"
                                      " them"),
        "cosmosdb": (NOT_APPLICABLE, "a container's physical partitions are"
                                     " its own layout; it is read feed range"
                                     " by feed range"),
        "spanner": (NOT_APPLICABLE, "Spanner splits a table by its key"
                                    " itself; there are no partitions to"
                                    " carry"),
        "bigtable": (NOT_APPLICABLE, "Bigtable splits a table into tablets"
                                     " by its row keys itself"),
        **{n: (NOT_APPLICABLE, "a collection is spread by the service"
                               " itself; there are no partitions to carry")
           for n in ("firestore", "datastore")},
        **{n: (NOT_APPLICABLE, "every table is partitioned by its key; the"
                               " copy goes by token ranges")
           for n in ("keyspaces", "geminidb-cassandra")},
        "geminidb-mongo": (NOT_APPLICABLE, "collections are sharded, not"
                                           " partitioned; read as one"),
        "neptune": (NOT_APPLICABLE, "a graph is not partitioned; it is read"
                                    " whole"),
        "timestream": (NOT_APPLICABLE, "a table is partitioned by the service"
                                       " by its dimensions; it is read whole"),
        "sqlite": (NOT_APPLICABLE, "SQLite has no partitioned tables"),
        "duckdb": (NOT_APPLICABLE, "DuckDB has no partitioned tables"),
        "copybook": (NOT_APPLICABLE, "a file is not partitioned; one file"
                                     " of several record layouts is several"
                                     " tables, by segment"),
        **{n: (NOT_APPLICABLE, "an application's data is read through its"
                               " interface, which shows no partitions")
           for n in _READ_ONLY},
        "*": (NOT_YET, "R13"),
    },
    "missing grant": {
        **_NONE,
        "postgres": ("yes", "movers._pg_quiet_triggers"),
        "mysql": ("yes", "movers._MyTriggerWindow"),
        # the redo-file reader's grants, named to the DBA; LogMiner
        # otherwise, which needs none of them
        "oracle": ("yes", "dba._ora_redo_files"),
        # the bulk sessions' one grant, named to the DBA; single statements
        # otherwise, which need none of it
        "teradata": ("yes", "dba._teradata"),
        "hetero": (NOT_YET, "0"),
        "mongodb": (NOT_YET, "0"),
        "*": (NOT_APPLICABLE, _NO_PROGRAM),
    },
    # the target kept from the application while migkit moves into it
    # (`protect_target`): the same hold per engine the cutover's freeze
    # stands on, its ways the ones that leave migkit's own account writing
    # (`freeze.KEEPS`) - held, offered as a change of an access policy, or
    # the writes that are not migkit's named
    "read-only target": {
        **_NONE,
        **{n: ("yes", f"freeze.{c}") for n, c in (
            ("postgres", "PostgresHold"), ("mysql", "MySQLHold"),
            ("tidb", "TiDBHold"), ("mongodb", "MongoHold"),
            ("redis", "RedisHold"), ("mssql", "MSSQLHold"),
            ("cassandra", "CassandraHold"), ("kafka", "KafkaHold"),
            ("opensearch", "OpenSearchHold"),
            ("cockroachdb", "PgWireHold"), ("dsql", "PgWireHold"),
            ("clickhouse", "ClickHouseHold"), ("oracle", "OracleHold"),
            ("db2", "Db2Hold"), ("db2zos", "Db2ZosHold"),
            ("db2i", "Db2iHold"), ("ase", "AseHold"),
            ("teradata", "TeradataHold"), ("redshift", "RedshiftHold"),
            ("snowflake", "SnowflakeHold"), ("couchbase", "CouchbaseHold"),
            ("dynamodb", "DynamoDBHold"), ("bigquery", "BigQueryHold"))},
        # a pair's target is kept as its own engine keeps it
        "hetero": ("yes", "freeze.protect"),
        "generic": (NOT_APPLICABLE, _COMPARES_ONLY),
        **{n: (NOT_APPLICABLE, "a file is written by whoever opens it, and"
                               " nothing in it can hold a writer or name"
                               " one") for n in _FILES},
        **{n: (NOT_APPLICABLE, "a lake table is files in a store, written"
                               " by whoever the store lets write, and"
                               " nothing in the table can hold a writer")
           for n in ("iceberg", "delta")},
        "cosmosdb": (NOT_APPLICABLE, "its writes are allowed by the"
                                     " account's keys, which reach every"
                                     " database of the account and cannot"
                                     " be taken from one, and it names no"
                                     " writer of a change"),
        **{n: (NOT_APPLICABLE, "it is read, never written") for n in
           _READ_ONLY},
        **{n: ("yes", f"freeze.{c}") for n, c in _HELD_AS
           + (("lindorm", "LindormHold"),)},
        **{n: (NOT_APPLICABLE, _CLOUD_POLICIES) for n in _BY_POLICY},
        "*": (NOT_YET, "R13"),
    },
    "slow link": {
        **_NONE,
        **{n: (NOT_APPLICABLE, "a file has no link; the store copies its"
                               " own") for n in _FILES},
        "*": ("yes", "tunnel.legs_for"),
    },
    # a source is never written to (`Engine._target_only`), so every copy
    # reads a read-only or standby source
    "read-only source": {
        **_NONE,
        "*": ("yes", "engines.base.Engine._target_only"),
    },
    # one hold per engine (`freeze.Hold`): the cutover's freeze of the
    # source stands on it, and keeping the target from the application can
    "writes held": {
        **_NONE,
        **{n: ("yes", f"freeze.{c}") for n, c in (
            ("postgres", "PostgresHold"), ("mysql", "MySQLHold"),
            ("mongodb", "MongoHold"), ("redis", "RedisHold"),
            ("mssql", "MSSQLHold"), ("cassandra", "CassandraHold"),
            ("kafka", "KafkaHold"), ("opensearch", "OpenSearchHold"),
            ("tidb", "TiDBHold"), ("cockroachdb", "PgWireHold"),
            ("clickhouse", "ClickHouseHold"), ("oracle", "OracleHold"),
            ("db2", "Db2Hold"), ("ase", "AseHold"),
            ("teradata", "TeradataHold"), ("redshift", "RedshiftHold"),
            ("snowflake", "SnowflakeHold"), ("couchbase", "CouchbaseHold"))},
        # a pair's source is held as its own engine holds it
        "hetero": ("yes", "freeze.hold_for"),
        "generic": (NOT_APPLICABLE, _COMPARES_ONLY),
        **{n: (NOT_APPLICABLE, "a file is written by whoever opens it, and"
                               " nothing on it can hold a writer")
           for n in _FILES},
        **{n: (NOT_APPLICABLE, "a lake table is files in a store, written"
                               " by whoever the store lets write, and"
                               " nothing in the table can hold a writer")
           for n in ("iceberg", "delta")},
        "dynamodb": (NOT_APPLICABLE, "its writes are allowed by the"
                                     " account's access policies, outside"
                                     " the data: a table's own policy keeps"
                                     " a target on an answer of yes, and a"
                                     " source is held by its operator"
                                     " (freeze: operator)"),
        "bigquery": (NOT_APPLICABLE, "a role granted on the project writes"
                                     " every dataset, whatever a dataset's"
                                     " access says, so no change migkit"
                                     " makes to the dataset holds every"
                                     " writer of a source"),
        "cosmosdb": (NOT_APPLICABLE, "its writes are allowed by the"
                                     " account's keys, which reach every"
                                     " database of the account and cannot"
                                     " be taken from one"),
        "dsql": (NOT_APPLICABLE, "no session ends another's or sees its"
                                 " transactions, so nothing it still holds"
                                 " open can be ended or read back"),
        **{n: (NOT_APPLICABLE, "its sessions are ended and what they hold"
                               " open is read through the system's own"
                               " operator interfaces, not SQL")
           for n in ("db2i", "db2zos")},
        **{n: (NOT_APPLICABLE, "the application writes it through its own"
                               " interface, which migkit only reads; its"
                               " operator stops the application")
           for n in _READ_ONLY},
        # the cloud-native services held by the hold of the API they speak;
        # those whose writes are allowed by the cloud's access policies
        # are held by their operator
        **{n: ("yes", f"freeze.{c}") for n, c in _HELD_AS},
        **{n: (NOT_APPLICABLE, _CLOUD_POLICIES) for n in _BY_POLICY},
        "lindorm": (NOT_APPLICABLE, "nothing in Lindorm ends another's"
                                    " session or shows what one holds open,"
                                    " so a source's writes are stopped by"
                                    " its operator (freeze: operator) and"
                                    " proved by migkit"),
        "*": (NOT_YET, "F2"),
    },
}
# the copier reads a table whose only index is not unique as one with no
# key: `neutral_key` answers the primary key or nothing
WAYS["no unique index"] = dict(WAYS["keyless"])


def resolves(engine, where):
    """Whether a `yes` cell's way is still in the code: `@method` on the
    engine's own class, or a dotted name under `migkit`. A method an
    engine class only inherits from the base's placeholder, or whose body
    only refuses, is not a way (`capabilities._own`)."""
    import importlib
    import inspect

    from .capabilities import _own
    from .engines import _class_for
    from .engines.base import Engine
    if where.startswith("@"):
        cls = _class_for(engine)
        return cls is not None and _own(cls, where[1:])
    parts = where.split(".")
    for cut in range(len(parts), 0, -1):
        try:
            obj = importlib.import_module("migkit." + ".".join(parts[:cut]))
        except ImportError:
            continue
        owner = None
        for attr in parts[cut:]:
            owner, obj = obj, getattr(obj, attr, None)
            if obj is None:
                return False
        if inspect.isclass(owner) and issubclass(owner, Engine) \
                and owner is not Engine:
            return _own(owner, parts[-1])
        return True
    return False


def cell(engine, shape):
    """(state, detail): "yes" and where, a gap and why, or "undeclared"
    and "stale" for what the test fails on."""
    ways = WAYS.get(shape, {})
    got = ways.get(engine, ways.get("*"))
    if got is None:
        return ("undeclared", "")
    state, detail = got
    if state == "yes" and not resolves(engine, detail):
        return ("stale", detail)
    return state, detail


def coverage():
    """{engine: {shape: "yes" | NOT_YET | NOT_APPLICABLE | "undeclared" |
    "stale"}}: the matrix of shape by engine."""
    from .engines import NAMES
    return {n: {s: cell(n, s)[0] for s in SHAPES} for n in NAMES}


def uncovered():
    """(engine, shape) cells with neither a way nor an honest gap, and
    `yes` cells whose way is gone from the code."""
    return [(n, s) for n, row in coverage().items()
            for s, state in row.items() if state in ("undeclared", "stale")]
