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
    #: the programs on this machine it needs installed: a rung needing one
    #: runs a program migkit drives, not its own code (`_is_external_program`)
    programs: tuple = ()

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
    #: what it leaves behind, in an operator's words - a table made, a
    #: slot, a process, a file - and "nothing" where it leaves nothing:
    #: the words `footprint` stands for, said by `doctor` (`waysaid`)
    leaves: str = ""
    #: it runs a program migkit drives, not migkit's own code: it earns a
    #: rank above the copier only by a run measured here, and is given one
    #: bounded trial where it never ran (`Trials`)
    external: bool = False
    #: migkit's own copier: the way an untimed program is held below
    native_copier: bool = False

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
    #: seconds the unit's run is boxed in where the rung is on its one
    #: trial (`Trials`), 0 where it is not
    trial: float = 0.0

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
#: a program's one trial on a work is boxed in this many times the best
#: measured way's time for the unit, or in `TRIAL_SECONDS` where no way is
#: measured on it yet
TRIAL_TIMES = 4.0
TRIAL_SECONDS = 60.0


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
    workers, the trials move the way.

    A program the ranking holds below migkit's own copier because it never
    ran here (`untried`) is owed one trial on this work all the same, so a
    way is never left unmeasured for want of being run: on a unit exploring
    may take (`_may_explore`), boxed in `TRIAL_TIMES` the best measured
    way's time for that unit (`TRIAL_SECONDS` where nothing is), said in a
    line through `log`. A trial past its box, or failed, is given up on
    for the rest of the work and said; one in time ranks by what it was
    measured at."""

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
        #: {name: seconds}: the programs on their one trial, and its box;
        #: `_trialled` every program that had it
        self._boxes, self._trialled = {}, set()
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
            u = unit if unit is not None else self.unit
            may = self._may_explore(u)
            trial = self._owed(u) if may else None
            if self.model is None:
                got = climb(self.work, self.rungs, self.facts, self.need,
                            _InFlight(self.costs, self._timing), trial=trial)
                if got.rung is not None and \
                        self.costs.get(got.rung.name) is None:
                    self._timing.add(got.rung.name)
                return self._on_trial(got)
            costs = _InFlightModel(self.model, self._timing, self._pressure)
            got = climb(self.work, self.rungs, self.facts, self.need, costs,
                        unit=u, explore=AGAIN if may else False, trial=trial)
            if got.trial:
                self._timing.add(got.rung.name)
                return self._on_trial(got)
            if got.rung is not None and not self._measured_best(got, u):
                if not self.model.measured(got.rung.name):
                    self._timing.add(got.rung.name)
            else:
                got = self._said(got, u)
                self._settle(got, u)
            return got

    def _owed(self, unit):
        """(name, seconds) of the first program never measured on this
        work and owed its one trial, with the box its run is held to, or
        None."""
        costs = self.costs if self.model is None else self.model
        for r in untried(self.rungs, costs, unit):
            if r.name not in self._trialled and r.name not in \
                    self.given_up and r.name not in self._timing:
                return r.name, self._box(unit)
        return None

    def _box(self, unit):
        """The seconds a trial on `unit` is held to: `TRIAL_TIMES` the
        best measured way's time for it, `TRIAL_SECONDS` where none is."""
        best = []
        for r in self.rungs:
            if self.model is not None:
                if self.model.measured(r.name):
                    e = self.model.estimate(r.name, unit)
                    if e is not None and e.mean:
                        best.append(e.mean)
                continue
            got = self.costs.get(r.name)
            rows = int((unit or {}).get("rows") or 0)
            if isinstance(got, (int, float)) and 0 < got < _TIMING and rows:
                best.append(got * rows)
        return TRIAL_TIMES * min(best) if best else TRIAL_SECONDS

    def _on_trial(self, got):
        """`got`, kept and said where it is a program's one trial."""
        if got.trial and got.rung is not None:
            self._boxes[got.rung.name] = got.trial
            self._trialled.add(got.rung.name)
            if self.log is not None:
                self.log(str(got))
        return got

    def _measurements(self, chosen, unit):
        """The ways measured on this work, the chosen first, each with
        what it was measured at on `unit` - or "" where fewer than two
        were: what a choice between them stood on, in words."""
        if chosen is None or self.model is None:
            return ""
        ways = [chosen] + [r for r in self.rungs if r.name != chosen.name]
        got = []
        for r in ways:
            if not self.model.measured(r.name):
                continue
            e = self.model.estimate(r.name, unit)
            if e is not None:
                got.append(f"{r.said} {e.said()}")
        return ", ".join(got) if len(got) > 1 else ""

    def _said(self, got, unit):
        """The choice with the measurements it stood on in its reason."""
        words = self._measurements(got.rung, unit)
        if not words:
            return got
        import dataclasses
        return dataclasses.replace(
            got, reason=f"{got.reason}; measured here a unit: {words}")

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

    def _settle(self, got, unit=None):
        """The way the work stands on now; a change of it said, with the
        measurement it stood on."""
        if got.rung is None:
            return
        was, self._on = self._on, got.rung
        if was is None or was.name == got.rung.name or self.log is None:
            return
        why = (self._why or self._measurements(got.rung, unit)
               or "what was measured since")
        self.log(f"{self.work}: {got.rung.said} for what is left, where"
                 f" {was.said} went before - {why}")
        self._why = ""

    # --- what a unit took -----------------------------------------------------

    def took(self, name, units, seconds, unit=None):
        """A unit done on `name`: `units` of work in `seconds`."""
        with self._lock:
            self.costs.saw(name, units, seconds)
            self._timing.discard(name)
            box = self._boxes.pop(name, None)
            if box is not None and float(seconds or 0) > box:
                self._past_box(name, seconds, box)
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

    def _past_box(self, name, seconds, box):
        """A trial that ran past its box: timed, and given up on for the
        rest of the work, said."""
        rung = next((r for r in self.rungs if r.name == name), None)
        why = (f"its one trial took {float(seconds):,.1f}s, past the"
               f" {box:,.1f}s it was boxed in")
        self.given_up.setdefault(name, why)
        if self.log is not None:
            self.log(f"{self.work}: {rung.said if rung else name} - {why};"
                     " not tried again on this work")

    def give_up(self, name, why):
        """`name` failed here and is not tried again on this work."""
        with self._lock:
            self.given_up.setdefault(name, why)
            self._timing.discard(name)
            if self._boxes.pop(name, None) is not None and \
                    self.log is not None:
                rung = next((r for r in self.rungs if r.name == name), None)
                self.log(f"{self.work}: {rung.said if rung else name} failed"
                         f" its one trial: {why}; not tried again on this"
                         " work")
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
    return _ranked(rungs, _priced(rungs, costs, unit), explore, costs)


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


def _is_external_program(rung):
    """Whether `rung` runs a program migkit drives: it says so itself
    (`Rung.external`), or one of its needs is a program installed here
    (`Need.programs`) - never by its name or its words."""
    return bool(getattr(rung, "external", False)) or any(
        getattr(n, "programs", ()) for n in getattr(rung, "needs", ()))


def _is_native_copier(rung):
    """Whether `rung` is migkit's own copier, as it declares
    (`Rung.native_copier`) - never by its name or its words."""
    return bool(getattr(rung, "native_copier", False))


def _has_measured_run(rung, priced, costs=None):
    """Whether `rung` ran here and was timed: an `Estimate` measured on at
    least one unit, or a number a hop's own rates keep (`Costs`,
    `HopCosts`: a number always was measured). A prediction, however
    fast, never is."""
    from .costmodel import MEASURED_TRUTH
    cost = priced.get(rung.name) if priced else None
    if cost is not None and hasattr(cost, "truth"):
        return cost.truth == MEASURED_TRUTH and cost.n > 0 and cost.mean > 0
    if isinstance(cost, (int, float)):
        return 0 < cost < _TIMING
    if costs is not None and callable(getattr(costs, "measured", None)):
        return bool(costs.measured(rung.name))
    return False


def _enforce_copier_rank(ordered, priced, costs=None, trial=()):
    """`ordered` with a program never measured here held below migkit's
    own copier - except a rung named in `trial`, on its one bounded trial
    (`Trials`), which goes first."""
    if trial:
        first = [r for r in ordered if r.name in trial]
        return first + _enforce_copier_rank(
            [r for r in ordered if r.name not in trial], priced, costs)
    copier_idx = None
    for idx, r in enumerate(ordered):
        if _is_native_copier(r):
            copier_idx = idx
            break
    if copier_idx is None:
        return ordered

    earned = []
    unearned = []
    for r in ordered[:copier_idx]:
        if _is_external_program(r) and not _has_measured_run(r, priced, costs):
            unearned.append(r)
        else:
            earned.append(r)

    if not unearned:
        return ordered

    return earned + [ordered[copier_idx]] + unearned + ordered[copier_idx + 1:]


def _ranked(rungs, priced, explore=True, costs=None, trial=()):
    entries = [(priced.get(r.name), r.footprint, i, r)
               for i, r in enumerate(rungs)]
    if any(hasattr(e[0], "mean") for e in entries):
        ordered = _order_estimates(entries, explore)
    else:
        ordered = _order(entries)
    return _enforce_copier_rank(ordered, priced, costs, trial)


def untried(rungs, costs=None, unit=None):
    """The programs of `rungs` never measured here: each held below the
    copier by the ranking, so each is owed one bounded trial before that
    holds it there for good (`Trials`)."""
    rungs = list(rungs)
    priced = _priced(rungs, costs, unit)
    if not any(_is_native_copier(r) for r in rungs):
        return []
    return [r for r in rungs if _is_external_program(r)
            and not _has_measured_run(r, priced, costs)]


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
    if _is_native_copier(chosen) and _is_external_program(runner) and not _has_measured_run(runner, priced):
        return (f"{runner.said} earns its rank only after a measured"
                " successful run on this hop")
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


def fenced(rungs, *needs):
    """`rungs` with `needs` asked of every one of them first: a ladder's
    shared conditions declared once, in front of each rung's own."""
    import dataclasses
    return tuple(dataclasses.replace(r, needs=tuple(needs) + tuple(r.needs))
                 for r in rungs)


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
          prove=True, unit=None, explore=True, trial=None):
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
    measured only where it is faster even at its p90. `trial` (name,
    seconds) is a program never measured here on its one bounded trial
    (`Trials`): ranked first where it holds, its run boxed in `seconds`;
    (names, seconds) the programs owed one, the first of them that holds
    taking it.

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
    # several names: the programs owed a trial, the first that holds goes
    on_trial = (() if not trial else tuple(trial[0])
                if isinstance(trial[0], (tuple, list)) else (trial[0],))
    ranked = _ranked(kept, priced, explore, costs, on_trial)
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
        elif r.name in on_trial:
            reason = (f"{r.said} never ran here: its one trial, on a small"
                      f" unit and boxed in {trial[1]:,.0f}s, before the"
                      " ranking holds it below the copier")
        else:
            # why the ways above gave way, then how this one won among
            # the ways timed: a refusal above is not the whole reason
            # where the ranking, not the order, chose
            reason = (_joined(above) if above
                      else r.because or f"{r.said}: the first way that holds")
            timed = [o for o in ranked if _measured(priced.get(o.name))]
            mine = priced.get(r.name)
            # the ranking's first timed way is the cheapest; a way chosen
            # below a cheaper one that failed won on holding, not on cost
            if len(timed) > 1 and timed[0] is r:
                reason += "; measured the cheapest of the ways timed here"
            elif mine is not None and not _measured(mine) and not timed:
                reason += f"; predicted the fastest here, {mine.said()}"
        runner = next(iter(order[i + 1:]), None)
        from . import guarantees as g
        return Choice(work, r, reason, tuple(out + passed), tuple(ranked),
                      costs_of, runner,
                      _why_not(r, runner, priced) if runner else "",
                      tuple(g.named(g.of([r])).items()), asked,
                      float(trial[1]) if r.name in on_trial else 0.0)
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
    "spread target": "a target that spreads a table across nodes of its"
                     " own, its rows placed and loaded as it places them",
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
_NONE = {n: (NOT_APPLICABLE, STREAM_ONLY.format(said, n))
         for n, said in (("kinesis", "Kinesis"), ("pubsub", "Pub/Sub"))}
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
            "hetero", "teradata")},
        # the engines added since, and the services on another's wire:
        # the widest value each takes and what it does past it are not yet
        # measured on a server of the kind, and an inherited adapter is
        # not that measurement
        **{n: (NOT_YET, "P0-PARITY.lv.new") for n in (
            "oceanbase", "oceanbase-oracle", "polardbx", "dcdb",
            "gaussdb", "lindorm", "geminidb-redis", "geminidb-mongo",
            "keyspaces", "geminidb-cassandra", "cockroachdb", "dsql",
            "bigtable", "neptune", "timestream")},
        "dynamodb": (NOT_APPLICABLE, "an item is 400 KB at most, whole"
                                     " under the 1 MB a scan page returns"),
        # proved whole on each emulator (tests/test_large_values_on_google_
        # and_aws_native_services.py): the service's own bound keeps a
        # value under a piece, so nothing is read apart
        "firestore": (NOT_APPLICABLE, "a document is 1 MiB at most, read"
                                      " whole in the service's answer"),
        "datastore": (NOT_APPLICABLE, "an entity is 1 MiB at most, read"
                                      " whole in the service's answer"),
        "spanner": (NOT_APPLICABLE, "a column value is 10 MB at most, read"
                                    " whole in the row the service returns"),
        "cosmosdb": (NOT_APPLICABLE, "an item is 2 MB at most, less than"
                                     " a piece: read whole in batches cut"
                                     " by bytes and written in"
                                     " transactional batches cut at the"
                                     " 2 MB a request takes"),
        "couchbase": ("yes", "@LARGE_VALUES"),
        "tidb": ("yes", "@LARGE_VALUES"),
        "cassandra": (NOT_APPLICABLE, "a row is written as one mutation, at"
                                      " most half a commit-log segment:"
                                      " 16 MB by default"),
        "redshift": (NOT_APPLICABLE, "a value is 16 MB at most (a VARBYTE"
                                     " or a SUPER) and a VARCHAR 65,535"
                                     " bytes, each read whole in batches"
                                     " cut by bytes"),
        "*": (NOT_YET, "P0-PARITY.lv.new"),
    },
    "partitioned": {
        **_NONE,
        "postgres": ("yes", "@check_deep"),
        "mysql": ("yes", "@check_deep"),
        # each partition held to its own count on both sides: a partition
        # that arrived empty leaves the table's total plausible
        "mssql": ("yes", "@_deep_partitions"),
        "oracle": ("yes", "@_deep_partitions"),
        "tidb": ("yes", "@_deep_partitions"),
        "teradata": ("yes", "@_deep_partitions"),
        "clickhouse": ("yes", "@_partitioned_by"),
        # partitions and clusters read from the table's metadata, every
        # partition held to its own row count (`INFORMATION_SCHEMA.PARTITIONS`)
        "bigquery": ("yes", "@check_deep"),
        "kafka": ("yes", "@move_table"),
        # read by the code, each partition held to its own count, and owed a
        # test on that engine (`capabilities.UNPROVED`)
        **{n: ("yes", "@_partitions") for n in ("db2", "db2i", "db2zos")},
        "parquet": ("yes", "@_parts"),
        "oceanbase": ("yes", "@_deep_partitions"),
        "polardbx": ("yes", "@_deep_partitions"),
        "dcdb": ("yes", "@_deep_partitions"),
        # partitioned tables exist on these and none is read by its own
        # partitions yet: ASE's and the lakes' are the smaller gaps, the
        # services' and a pair's (read as its engines are) every
        # capability on every engine
        **{n: (NOT_YET, "P0-PARITY.small") for n in (
            "ase", "iceberg", "delta")},
        **{n: (NOT_YET, "P0-PARITY.all") for n in (
            "gaussdb", "oceanbase-oracle", "lindorm", "hetero")},
        "geminidb-redis": (NOT_APPLICABLE, "a keyspace has no tables, so no"
                                           " partitioned ones"),
        "redis": (NOT_APPLICABLE, "a keyspace has no tables, so no"
                                  " partitioned ones"),
        "snowflake": (NOT_APPLICABLE, "a table is cut into micro-partitions"
                                      " by the service itself; none is"
                                      " declared by the application, so"
                                      " none is carried"),
        "redshift": (NOT_APPLICABLE, "a table is laid out by its"
                                     " distribution and sort keys; it has"
                                     " no partitions of its own"),
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
        "cockroachdb": (NOT_APPLICABLE, "a partition only labels rows of"
                                        " one table for a zone to place; the"
                                        " table is read and copied as one,"
                                        " and its zones are the cluster's"
                                        " own, not carried"),
        "dsql": (NOT_APPLICABLE, "a table is split over its storage by the"
                                 " service itself; no partition is declared"
                                 " (`CREATE TABLE` has no `PARTITION BY`),"
                                 " so none is carried"),
        "copybook": (NOT_APPLICABLE, "a file is not partitioned; one file"
                                     " of several record layouts is several"
                                     " tables, by segment"),
        **{n: (NOT_APPLICABLE, "an application's data is read through its"
                               " interface, which shows no partitions")
           for n in _READ_ONLY},
        "*": (NOT_YET, "P0-PARITY.all"),
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
        "hetero": (NOT_YET, "P0-PARITY.small"),
        "mongodb": (NOT_YET, "P0-PARITY.small"),
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
        "*": (NOT_YET, "F2.freeze"),
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
        "*": (NOT_YET, "F2.freeze"),
    },
}
# the copier reads a table whose only index is not unique as one with no
# key: `neutral_key` answers the primary key or nothing
WAYS["no unique index"] = dict(WAYS["keyless"])
#: a reshard's one target that is a cluster (`reshard`): its own
#: distribution read and each row placed and loaded as it places it
#: (`Engine.distribution`); where the cluster places every row itself
#: whatever a client does, there is nothing of it to honour
_KEY_SPANS = ("it spreads a table by spans of its key and every server of"
              " it is a gateway to every span: the copy writes a table in"
              " key order, a batch a span, the order its spans are laid out"
              " in")
_ROUTES = ("the {} places each item it is given by its own key, and no"
           " node of it is addressed by a client")
WAYS["spread target"] = {
    **_NONE,
    # Citus, the nodes' own parts written directly where they are reached
    "postgres": ("yes", "@distribution"),
    # Vitess, through its gate by its vindex
    "mysql": ("yes", "@distribution"),
    "mongodb": ("yes", "@distribution"),
    "cassandra": ("yes", "@distribution"),
    # a pair's target is asked as its own engine is
    "hetero": ("yes", "reshard._spread"),
    "generic": (NOT_APPLICABLE, _COMPARES_ONLY),
    "tidb": (NOT_APPLICABLE, _KEY_SPANS),
    "cockroachdb": (NOT_APPLICABLE, _KEY_SPANS),
    "dsql": (NOT_APPLICABLE, _KEY_SPANS),
    "clickhouse": (NOT_APPLICABLE, "a Distributed table splits every block"
                                   " it is given by its own sharding key and"
                                   " sends each shard its rows"),
    **{n: (NOT_APPLICABLE, _ROUTES.format(said)) for n, said in (
        ("dynamodb", "service"), ("cosmosdb", "service"),
        ("bigquery", "service"), ("snowflake", "service"),
        ("redshift", "cluster's leader"), ("couchbase", "client"),
        ("opensearch", "cluster"), ("kafka", "producer"),
        ("redis", "cluster client"))},
    "*": (NOT_APPLICABLE, "one server holds every row of a table"),
}

#: the generic engine, any SQL source reached through a standard driver: it
#: compares, and each table is compared the way its facts allow
#: (`GenericEngine.compare_rungs`) - key ranges checksummed on both servers
#: where one key column names one row, row digests in this machine
#: otherwise, by key or by the whole row
_GENERIC = {
    "keyless": ("yes", "@_client_digests"),
    "no unique index": ("yes", "@_client_digests"),
    "wide key": ("yes", "@_client_digests"),
    # ranges split again only where they differ; a table the row digests
    # cannot hold here is refused that way, said, and left to the ranges
    "too large": ("yes", "@compare_rungs"),
    # a value is rendered and digested whole, its width in the measured
    # size the row digests are held to
    "large values": ("yes", "@_client_digests"),
    # a standard driver reads a partitioned table as one, and every row is
    # matched whatever partition holds it: a partition that arrived empty
    # is its rows missing, by key
    "partitioned": ("yes", "@compare_rungs"),
    "read-only source": ("yes", "engines.base.Engine._target_only"),
    "missing grant": (NOT_APPLICABLE, "both of its ways only read a table:"
                                      " a grant it lacks is the table's own"
                                      " error, never a slower way"),
    # only the ranges' checksums cross the link
    "slow link": ("yes", "@compare_rungs"),
}
for _shape, _way in _GENERIC.items():
    WAYS[_shape]["generic"] = _way


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


def unowned_gaps():
    """(shape, engine, item) of each not-yet cell whose item is no row of
    `docs/backlog-index.md`, or a row already done: a gap waits on work
    still to do, never on work closed."""
    from .capabilities import backlog_status
    rows = backlog_status()
    return [(s, e, why) for s, ways in WAYS.items()
            for e, (state, why) in ways.items()
            if state == NOT_YET
            and (why not in rows or rows[why].startswith("DONE"))]


def uncovered():
    """(engine, shape) cells with neither a way nor an honest gap, and
    `yes` cells whose way is gone from the code."""
    return [(n, s) for n, row in coverage().items()
            for s, state in row.items() if state in ("undeclared", "stale")]


def probe_width(items, per_item=4):
    """How many probes of waiting work (each holding `per_item` sockets)
    run at once: all of them, held back only by what this process may have
    open - the descriptor limit less a reserve for its own files - never a
    number picked by hand."""
    try:
        import resource
        soft = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        room = (soft - 64) // max(1, per_item) if soft > 0 else items
    except (ImportError, ValueError, OSError):
        room = items
    return max(1, min(items, room))


@dataclass(frozen=True)
class PriorityItem:
    """One task, work unit, probe, investigation or unknown (P0-DECISION-ALL.priority)."""
    name: str
    kind: str = "work"
    mandatory: bool = False
    risk: str = "none"
    critical_path: bool = False
    dependencies: tuple = ()
    unlocks: tuple = ()
    uncertainty: float = 0.0
    cost: float = 1.0
    reversibility: str = "read_only"
    decision_impact: float = 0.0
    scope_size: int = 0
    why: str = ""


#: what an item guards against, worst first: an item guarding against a
#: worse loss goes first whatever it costs - a guarantee is never traded
#: for time
RISKS = ("data_loss", "correctness", "unavailability", "performance", "none")
#: what an item leaves behind, its rung's footprint: the tie-breaker
LEAVES = ("read_only", "reversible", "irreversible")


def _priority_cost(item, costs, unit):
    """(rung, what it costs): what `costs` measured or predicted of the
    item's name, the item's own figure - a prediction - where they say
    nothing."""
    from .costmodel import MEASURED_TRUTH, PREDICTED, Estimate
    rung = Rung(item.name, item.name, footprint=LEAVES.index(
        item.reversibility) if item.reversibility in LEAVES else len(LEAVES))
    got = costed(rung, costs, unit)
    if isinstance(got, (int, float)):
        got = Estimate(float(got), truth=MEASURED_TRUTH, source="this hop",
                       n=1.0)
    if got is None or not got.mean:
        got = Estimate(max(float(item.cost), 0.0), truth=PREDICTED,
                       source="the item's own figure")
    return rung, got


def _priority_value(item, by_name, seen=frozenset()):
    """What doing `item` is worth to the choices after it: the chance it
    changes one (its uncertainty times that choice's weight), and the
    worth of every item that waits on it."""
    own = max(item.uncertainty, 0.0) * max(item.decision_impact, 0.0)
    after = [by_name[n] for n in item.unlocks if n in by_name] + [
        o for o in by_name.values() if item.name in o.dependencies]
    seen = seen | {item.name}
    return own + sum(_priority_value(o, by_name, seen)
                     for o in after if o.name not in seen)


def rank_priority(items, goal="copy", max_cost=None, costs=None, unit=None):
    """`items` (`PriorityItem`) in the order to do them (backlog
    P0-DECISION-ALL.priority): each a rung, costed as every rung is - by
    what `costs` measured of it here (`Costs`, `HopCosts`, a
    `costmodel.CostModel`), its own figure only where nothing was.

    The guarantees order first and are never traded: mandatory evidence,
    then the worse loss guarded against (`RISKS`), then the critical
    path. Within that, the items that settle a choice go by what each
    costs for the choice it settles - a cheap probe that may change a
    decision before a long copy it decides - the rest by what they cost,
    and a tie to what each leaves behind (`LEAVES`). An item never runs
    before what it waits on, and an irreversible one never before every
    mandatory item. `max_cost` keeps the items that fit, mandatory ones
    always."""
    items = list(items)
    by_name = {it.name: it for it in items}
    priced, rungs, value = {}, {}, {}
    for it in items:
        rungs[it.name], priced[it.name] = _priority_cost(it, costs, unit)
        value[it.name] = _priority_value(it, by_name)

    def tier(it):
        return (not it.mandatory,
                RISKS.index(it.risk) if it.risk in RISKS else len(RISKS),
                not (it.critical_path or it.unlocks),
                not value[it.name])

    ordered = []
    for key in sorted({tier(it) for it in items}):
        group = [it for it in items if tier(it) == key]
        per = {it.name: (priced[it.name].scaled(1.0 / value[it.name])
                         if value[it.name] else priced[it.name])
               for it in group}
        ordered += _ranked([rungs[it.name] for it in group], per, False)
    ordered = [by_name[r.name] for r in ordered]

    mandatory = {it.name for it in items if it.mandatory}
    result, done, waiting = [], set(), list(ordered)

    def ready(it):
        wait = {d for d in it.dependencies if d in by_name}
        if it.reversibility == "irreversible":
            wait |= mandatory - {it.name}
        return wait <= done

    while waiting:
        nxt = next((it for it in waiting if ready(it)), None)
        stuck = nxt is None
        nxt = nxt or waiting[0]
        waiting.remove(nxt)
        done.add(nxt.name)
        result.append(dataclasses.replace(nxt, why=_priority_why(
            nxt, priced[nxt.name], value[nxt.name], stuck)))

    if max_cost is None:
        return result
    budgeted, spent = [], 0.0
    for it in result:
        cost = priced[it.name].mean
        if it.mandatory or spent + cost <= max_cost:
            budgeted.append(it)
            spent += cost
    return budgeted


def _priority_why(item, cost, value, stuck):
    reasons = []
    if item.mandatory:
        reasons.append(f"mandatory {item.risk.replace('_', ' ')} guarantee"
                       if item.risk in ("data_loss", "correctness")
                       else "mandatory correctness guarantee")
    elif item.risk in ("data_loss", "correctness", "unavailability"):
        reasons.append(f"{item.risk.replace('_', ' ')} risk")
    if item.critical_path or item.unlocks:
        reasons.append("on critical path")
    if value:
        reasons.append(f"may change a choice worth {value:.2f}")
    reasons.append(f"costs {cost.said()}" if cost.mean else "costs nothing")
    if item.reversibility == "read_only":
        reasons.append("safe read-only probe")
    elif item.reversibility == "irreversible":
        reasons.append("irreversible action held behind its prerequisites"
                       " and every mandatory check")
    if stuck:
        reasons.append("its prerequisites wait on each other: taken in"
                       " rank order")
    return "; ".join(reasons)


# What each task the operator can start asks of the decision engine
# (backlog P0-DECISION-ALL.coverage). A task is routed when the module
# that does its consequential choosing goes through this engine, a gap when
# a choice is still made by a fixed rule (carried to the backlog row), and
# a no-choice when nothing there is a strategy, ordering, resource, retry or
# fallback decision. `task_gaps()` is what the guard test fails on.

TASKS = {
    # task: (family, ("routed", modules...) | ("gap", why) | ("no choice", why))
    "init": ("configuration", ("routed", "guide", "engineat")),
    "doctor": ("discovery", ("routed", "cli")),
    "advise": ("assessment", ("routed", "advisors")),
    "assess": ("assessment", ("routed", "ready")),
    "schema": ("schema", ("routed", "tableway")),
    "check": ("validation", ("routed", "verdict", "tableway")),
    "sync": ("repair", ("routed", "repairways", "verdict")),
    "move": ("move", ("routed", "movers", "ship", "tableway")),
    "watch": ("replication", ("routed", "tailtables", "verdict")),
    "report": ("reporting", ("no choice", "it renders the verdicts already"
               " decided; formatting only")),
    "history": ("reporting", ("no choice", "it lists recorded runs")),
    "rollback": ("cutover", ("routed", "cutover", "freeze")),
    "hops": ("configuration", ("no choice", "it lists configured hops")),
    "setup-target": ("schema", ("routed", "tableway")),
    "repair": ("repair", ("routed", "repairways")),
    "replicate": ("replication", ("routed", "tailtables")),
    "tail": ("replication", ("routed", "tailtables")),
    "convert-schema": ("conversion", ("routed", "tableway")),
    "gen-migration": ("conversion", ("routed", "tableway")),
    "sample-diff": ("validation", ("routed", "verdict")),
    "ui": ("reporting", ("no choice", "it serves the recorded state")),
    "state": ("recovery", ("no choice", "it reads the recorded state")),
    "monitor": ("replication", ("routed", "tailtables")),
    "users": ("security", ("routed", "userways")),
}


def task_gaps():
    """[(task, why)]: tasks the table does not name, routed ones whose
    module does not use this engine, and every declared gap."""
    import ast
    from pathlib import Path
    out = []
    for task, (_family, got) in TASKS.items():
        if got[0] == "gap":
            out.append((task, "gap: " + got[1]))
        elif got[0] == "routed":
            for mod in got[1:]:
                path = Path(__file__).with_name(mod + ".py")
                src = path.read_text() if path.exists() else ""
                if not any(isinstance(n, (ast.Import, ast.ImportFrom))
                           and "decide" in ast.unparse(n)
                           for n in ast.walk(ast.parse(src))) \
                        and mod != "verdict":
                    out.append((task, f"{mod} does not use the engine"))
    return out


#: a tail's proof windows, as multiples of the width it began with: the
#: square root of the batch at its measured rate, as many windows as
#: changes in each. Wider proves a batch in fewer windows, narrower names
#: a difference closer to when it was written; which is cheaper to read
#: back on a hop is measured there (`change_window`)
WINDOW_STEPS = (1, 2, 4, 0.5, 0.25)
#: the prefix a window width's measured verify time is kept under on a hop
WINDOW_COSTS = "change-window:"


def window_rungs(changes, seconds):
    """({rung name: seconds wide}, rungs): the widths a batch of `changes`
    read over `seconds` may be proved in. The first, the square root of
    the batch at its rate, always holds; a narrower one needs the rate
    observed to put at least one change in each window, a wider one the
    batch to span more than one window of it."""
    import math
    base = math.sqrt(changes) / (changes / seconds)
    widths, rungs = {}, []
    for step in WINDOW_STEPS:
        name = f"window x{step:g}"
        width = widths[name] = base * step
        needs = ()
        if step < 1:
            needs = (Need("a change in every window at the rate observed",
                          lambda f, w=width: f["rate"] * w >= 1,
                          "{rung}: fewer than one change a window at the"
                          " rate observed"),)
        elif step > 1:
            needs = (Need("a batch spanning more than one window",
                          lambda f, w=width: f["span"] > w,
                          "{rung}: the batch spans less than one window"),)
        rungs.append(Rung(
            name, f"windows of {step:g} times the batch's square root",
            needs=needs, footprint=int(step != 1),
            because="the square root of the batch at its measured rate:"
                    " as many windows as changes in each"))
    return widths, tuple(rungs)


def change_window(changes, seconds, costs=None):
    """(seconds wide, the `Choice`) of the windows a tail's batch is proved
    in, `changes` read over `seconds`: the width climbed to on `costs` -
    the window verify time a change measured on the hop (`WINDOW_COSTS`),
    each width that holds timed once before the cheapest is kept. (None,
    None) where the batch has no rate to cut by."""
    import math
    if changes <= 0 or seconds <= 0 or not math.isfinite(seconds):
        return None, None
    widths, rungs = window_rungs(changes, seconds)
    got = climb("change windows", rungs,
                Facts({"rate": changes / seconds, "span": seconds}),
                costs=costs)
    return widths[got.path or rungs[0].name], got


def session_patience(durations, predicted=None, deadline=None):
    """(seconds, `Choice`): how long an attempt waits on its server before
    the server's own view of its session is read (backlog
    P0-CONNECTION-PROGRESS). No number is written here: the longest wait
    this operation measured here and its spread above the middle; where
    fewer than two were measured, the cost model's p90 for the unit; where
    none, the read deadline read back from the connection - before which
    nothing is closed, as the transport ends the wait there anyway. None
    where not even that was read: the wait is left to the driver."""
    d = sorted(float(x) for x in durations or () if x is not None and x >= 0)

    def spread():
        longest, middle = d[-1], d[len(d) // 2]
        return longest + (longest - middle)
    rungs = (
        Rung("measured", "the waits this operation measured here",
             needs=(Need("two measured waits", lambda f: len(d) >= 2,
                         "fewer than two waits of this operation were"
                         " measured here"),),
             because="the longest wait measured here and its spread"),
        Rung("predicted", "the predicted wait of the unit",
             needs=(Need("a prediction", lambda f: bool(predicted)
                         and predicted > 0,
                         "no wait was predicted for the unit"),)),
        Rung("deadline", "the read deadline the connection holds",
             needs=(Need("the read deadline", lambda f: bool(deadline)
                         and deadline > 0,
                         "the connection's read deadline was not read"),)))
    got = climb("a wait on the server", rungs, Facts())
    seconds = {"measured": spread() if len(d) >= 2 else None,
               "predicted": predicted, "deadline": deadline}.get(got.path)
    return (float(seconds) if seconds else None), got


def session_action(facts):
    """The `Choice` for a wait the server's view of its session was read
    for (backlog P0-CONNECTION-PROGRESS): close that session only, end
    migkit's own end of a connection the server no longer has, or say what
    it is and leave the wait to the read deadline. `facts`: `kind`
    (`attempts.IDLE`, ...), `idle_seconds` the server counts, `patience`
    (`session_patience`), `idle_reads` in a row, `reads` (the attempt only
    reads: nothing it wrote is left uncertain), `owned` (the session is
    the hop's account's and this attempt's), `unprogressed` (closes of
    this unit not followed by progress) and `can_abandon`."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    once = Need("no earlier close of this unit without progress since",
                lambda f: not f["unprogressed"],
                "this unit's session was closed once already and the"
                " retry made no progress, so it is not closed again")
    rungs = (
        Rung("close", "close that session only", footprint=1, needs=(
            Need("an idle session", lambda f: f["kind"] == "idle",
                 "the server is not idle on it"),
            Need("idle longer than the wait measured",
                 lambda f: f["idle_seconds"] >= f["patience"],
                 lambda f: f"the server was idle {f['idle_seconds']:.0f}s,"
                           f" within the {f['patience']:.0f}s measured"),
            Need("idle on two reads", lambda f: f["idle_reads"] >= 2,
                 "idle on one read only"),
            Need("a read", lambda f: f["reads"],
                 "the attempt writes, and a write closed under it is left"
                 " uncertain"),
            Need("the session owned", lambda f: f["owned"],
                 "the session is not exactly this attempt's"),
            once)),
        Rung("abandon", "end migkit's own end of the connection",
             footprint=1, needs=(
                 Need("a session the server no longer has",
                      lambda f: f["kind"] == "gone",
                      "the server still has the session"),
                 Need("a read", lambda f: f["reads"],
                      "the attempt writes, and its outcome is uncertain"),
                 Need("a way to end it", lambda f: f["can_abandon"],
                      "the connection gives no way to end it"),
                 once)),
        Rung("report", "say it and leave the wait to the read deadline",
             because="nothing safe to close"))
    return climb("a wait the server was asked about", rungs, f)



# --- row parity: which read proves a table's rows alike ----------------------
# Every way below reads every row of both sides - none samples, none can be
# picked silently for less - and folds them into the same proof; what differs
# is what each costs on these servers, measured end to end (seconds a row)
# and kept with the hop, so a table goes the way its own reads were cheapest
# (`Trials` explores a way never timed). Resuming a table from ranges an
# earlier run proved is the one need that is not cost: those ranges are kept.

#: what any way of proving a table's rows alike must give
ROW_PARITY_NEED = ("every row",)

_FOLDS = Need("both servers fold the row digest",
              lambda f: bool(f.get("server digest")),
              "a side's server cannot fold the row digest, so {rung} has"
              " nothing to compare")

ROW_PARITY = (
    Rung("server digest", "each server folding every row into one digest",
         gives=frozenset({"every row"}), needs=(_FOLDS,),
         because="one statement a side, and two numbers back",
         loads={"source": 1, "target": 1}),
    Rung("key-range digest", "the same digest a range of the key at a time,"
                             " each range kept as it is proved",
         gives=frozenset({"every row", "resume by chunk"}),
         needs=(_FOLDS, Need("one integer key",
                             lambda f: bool(f.get("key")),
                             "the table has no single integer key, so its"
                             " ranges are not the same rows on both sides")),
         because="a run stopped midway goes on from the last range proved",
         loads={"source": 1, "target": 1}),
    Rung("row text", "every row brought here as canonical text and hashed",
         gives=frozenset({"every row"}),
         needs=(Need("rows read here",
                     lambda f: bool(f.get("rows read here")),
                     "no reader brings this table's rows here as canonical"
                     " text"),),
         because="a value hashes alike whatever each server renders",
         loads={"source": 1, "target": 1, "link:src": 1, "link:dst": 1,
                "cpu": 1}),
)


def row_parity(work, run, facts=None, need=(), costs=None, log=None,
               where=None):
    """(the `Choice`, what `run` returned) of proving one table's rows
    alike (`ROW_PARITY`, climbed by `attempt`): every row the need, the
    caller's `need` on top ("resume by chunk" where ranges an earlier run
    proved are kept), ranked by `costs` - which `run` tells what each way
    took (`saw(name, rows, seconds)`). A way failing to run switches to
    the next; none left raises `Exhausted` with every way's reason."""
    need = tuple(dict.fromkeys(ROW_PARITY_NEED + tuple(need or ())))
    return attempt(work, ROW_PARITY, run, facts, need=need, costs=costs,
                   where=where, log=log)
def recovery(facts):
    """The `Choice` after a failure (backlog P0-RESILIENCE-ALL): reconcile
    an outcome not known, proceed on one that committed, pause on a side
    whose breaker is open or a credential that renews, throttle, retry,
    replan, isolate the unit, probe, or stop. `facts` (`faults.retried`):
    `class` and `admits` (`faults.CATALOGUE`: the actions the class may
    take), `transient`, `uncertain` (a write whose effect is not known),
    `can_check` (a way to read it), `reconciled` (what that read said),
    `renewable`, `breaker` ("open" or "closed"), `tries_left`. An action
    the class does not admit is never taken, and a write whose effect is
    not known is never repeated before it is reconciled."""
    f = facts if isinstance(facts, Facts) else Facts(facts)

    def admits(action):
        return Need(f"a fault that admits {action}",
                    lambda f: action in (f.get("admits") or ()),
                    lambda f: f"a {f.get('class')} failure is not"
                              f" recovered by {action}")
    left = Need("tries left", lambda f: (f.get("tries_left") or 0) > 0,
                "every try this work may make is spent")
    known = Need("an effect known", lambda f: not f.get("uncertain")
                 or f.get("reconciled") == "nothing",
                 "what the failed write did is not known, so it is not"
                 " repeated")
    rungs = (
        Rung("reconcile", "read whether the work took effect", needs=(
            Need("an effect not known", lambda f: bool(f.get("uncertain")),
                 "the failure's effect is known"),
            Need("not read yet", lambda f: f.get("reconciled") is None,
                 "it was read already"),
            Need("a way to read it", lambda f: bool(f.get("can_check")),
                 "nothing can read whether the write committed, so it"
                 " stops rather than repeat it"))),
        Rung("proceed", "go on: the work took effect", needs=(
            Need("an effect read as committed",
                 lambda f: f.get("reconciled") == "committed",
                 "the work is not known to have taken effect"),)),
        Rung("pause", "wait, then one try", needs=(
            admits("pause"), left,
            Need("a side cooling down or a credential that renews",
                 lambda f: f.get("breaker") == "open" or (
                     f.get("class") == "expired_credential"
                     and f.get("renewable")),
                 "nothing that a wait alone clears"))),
        Rung("throttle", "back off longer with less work at once", needs=(
            admits("throttle"), left, known)),
        Rung("retry", "try again after a backoff", needs=(
            admits("retry"),
            Need("a fault that clears", lambda f: bool(f.get("transient")),
                 "the fault does not clear by itself"),
            left, known)),
        Rung("replan", "take another way for the work", needs=(
            admits("replan"),)),
        Rung("isolate", "set the unit aside, go on with the rest", needs=(
            admits("isolate"),)),
        Rung("probe", "read the evidence again from the servers", needs=(
            admits("probe"),)),
        Rung("stop", "stop this work and say why",
             because="no safe recovery holds"))
    return climb("a failure", rungs, f)
# --- a schedule admitted only where it keeps what the move must keep -------
# (P0-SAFETY-INTEGRITY): one need every schedule that runs work side by
# side is asked through, read from `guarantees.breaks`

#: the fact the need reads: [why] the schedule breaks what the move keeps
SCHEDULE_BREAKS = "schedule breaks"

KEEPS_SEMANTICS = Need(
    "a schedule that keeps what the move must keep",
    lambda f: not f[SCHEDULE_BREAKS],
    lambda f: "; ".join(f[SCHEDULE_BREAKS]))


def schedule_facts(schedule, semantics, known=None):
    """`Facts` a schedule is admitted from: what it breaks, read once."""
    from . import guarantees
    return Facts(known, **{SCHEDULE_BREAKS: lambda: guarantees.breaks(
        schedule, semantics)})


def admit(schedule, semantics, said="the schedule as planned", facts=None):
    """(True, "") where `schedule` keeps every table's `semantics` ({table:
    guarantees.Semantics}), else (False, why with what to do next). An
    empty `semantics` admits only what keeps nothing it could break."""
    f = facts if facts is not None else schedule_facts(schedule, semantics)
    kept, out = prune((Rung("as scheduled", said,
                            needs=(KEEPS_SEMANTICS,)),), f)
    return (True, "") if kept else (False, out[0][1])


# --- how many workers one copy runs (W10D): one owner for concurrency -------

def _count(v):
    try:
        n = int(v or 0)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def worker_budget(facts):
    """(workers, the `Choice`) one copy runs, the one sizing rung every
    copy asks - handed down by `migrate`'s flow, asked by a plain `move`
    itself: the flow's share of the measured budget where it handed one
    down (`budget`), else the start the move's own sizing measured
    (`start`, `sizing.estimate`), else as many as carry the binding stage
    (`little`, Little's law), else one at a time until a measure says
    more. Never more than the most both servers take (`most`) where that
    was measured, cut by what the throttles left (`scale`), never below
    one."""
    f = facts if isinstance(facts, Facts) else Facts(facts)

    def has(key, said, why):
        return Need(said, lambda f: _count(f.get(key)) > 0, why)
    rungs = (
        Rung("the flow's budget", "the share of the measured budget the"
             " flow handed down", needs=(has("budget", "a budget from the"
                                              " flow", "no flow handed a"
                                              " budget down"),)),
        Rung("the measured start", "the start the move's sizing measured",
             needs=(has("start", "a start measured", "nothing sized the"
                        " start"),)),
        Rung("the measured carry", "as many as carry the binding stage",
             needs=(has("little", "the stages measured", "the stages were"
                        " not measured"),)),
        Rung("one at a time", "one worker until a measure says more",
             because="nothing measured says more"))
    choice = climb("the copy's workers", rungs, f)
    key = {"the flow's budget": "budget", "the measured start": "start",
           "the measured carry": "little"}.get(choice.path)
    n = _count(f.get(key)) if key else 1
    most = _count(f.get("most"))
    if most:
        n = min(n, most)
    scale = f.get("scale")
    if key != "budget" and scale:
        n = int(n * float(scale))
    return max(1, n), choice


# --- a continuous hop into a warehouse (LT.P3, S4) --------------------------

#: the share of a batch's interval its landing may take: the rest is the
#: headroom a burst of the tail is absorbed in without falling behind
LANDING_SHARE = 0.5


def _seconds(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x >= 0 else None


def batch_for(rate, startup, per_row, share=LANDING_SHARE):
    """(rows a batch, seconds between batches, lag) a landing keeps up
    with a tail of `rate` changes a second in: the fewest rows whose landing
    (`startup` seconds plus `per_row` a row, both measured) takes no more
    than `share` of the time the tail takes to write them, since the lag a
    row sees is its wait for the batch plus the batch's landing. None where
    no batch keeps up: the landing's own cost a row is at or above what
    the tail gives it."""
    rate, startup, per_row = (_seconds(rate), _seconds(startup),
                              _seconds(per_row))
    if rate is None or startup is None or per_row is None:
        return None
    if rate == 0:
        return 1, None, startup + per_row
    room = share - per_row * rate
    if room <= 0:
        return None
    rows = max(1, math.ceil(startup * rate / room))
    interval = rows / rate
    return rows, interval, interval + startup + per_row * rows


def warehouse_landing(facts):
    """(the `Choice`, (rows, interval, lag)) of how a continuous hop lands
    its batches in a warehouse: the warehouse's streaming insert, staged
    files loaded every interval, or merge statements - each sized from the
    tail's measured rate and that landing's measured start and cost a row
    (`batch_for`), and ranked by the lag a row sees. Every rung lands a
    batch with its line in the ledger in one transaction (or the
    streaming device's own offset, where it keeps one), so none is ranked
    on a weaker promise; one that cannot keep up with the tail is passed
    over, never chosen to fall behind."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    sized = {}
    for name in ("stream", "files", "merge"):
        got = batch_for(f.get("rate"), f.get(f"{name}_startup"),
                        f.get(f"{name}_per_row"))
        if got is not None:
            sized[name] = got

    def keeps_up(name):
        return Need("a batch that keeps up", lambda f: name in sized,
                    lambda f: ("the landing was not measured"
                               if _seconds(f.get(f"{name}_startup")) is None
                               or _seconds(f.get(f"{name}_per_row")) is None
                               or _seconds(f.get("rate")) is None
                               else "its cost a row is more than the tail"
                                    " gives it: it would fall behind"))
    once = Need("each batch landed exactly once",
                lambda f: bool(f.get("ledger")),
                "{rung} needs the ledger written in the batch's"
                " transaction")
    rungs = (
        Rung("stream", "the warehouse's streaming insert",
             needs=(Need("a streaming device keeping its offset",
                         lambda f: bool(f.get("stream_offsets")),
                         "the warehouse offers no streaming insert that"
                         " keeps its offset"), keeps_up("stream")),
             because="the shortest start a batch"),
        Rung("files", "staged files loaded every interval",
             needs=(Need("a stage", lambda f: bool(f.get("stage")),
                         "the target names no stage to load files from"),
                    once, keeps_up("files")),
             because="the lowest cost a row"),
        Rung("merge", "merge statements a batch",
             needs=(once, keeps_up("merge")),
             because="no stage or streaming device needed"))
    costs = Costs({n: lag for n, (_, _, lag) in sized.items()})
    choice = climb("the continuous hop's landing", rungs, f, costs=costs)
    return choice, sized.get(choice.path)


#: the guarantees a feed into a warehouse is held to, strongest first
FEED_PROMISES = ("every row", "deletes", "the source's order", "verified")


def warehouse_feed(facts):
    """The `Choice` between the provider's zero-ETL integration and
    migkit's own continuous hop. A way is ranked only where it keeps every
    promise the hop asks (`FEED_PROMISES` it lists in `promises`): the
    provider's integration is offered for what it states it keeps, and is
    verified by migkit's own check either way, so it never stands on the
    provider's word. Among those kept, the measured lag (seconds) and cost
    (the same unit for both) rank them; an integration not measured yet
    ranks on its documented lag."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    asked = tuple(f.get("promises") or FEED_PROMISES)
    kept = set(f.get("provider_keeps") or ())

    def lag(name):
        x = _seconds(f.get(f"{name}_lag"))
        c = _seconds(f.get(f"{name}_cost"))
        return None if x is None else x * (1 + (c or 0))
    rungs = (
        Rung("provider", "the provider's zero-ETL integration",
             needs=(Need("an integration offered for this pair",
                         lambda f: bool(f.get("offered")),
                         lambda f: f.get("not_offered")
                         or "the provider offers none for this pair"),
                    Need("every promise the hop asks",
                         lambda f: all(p in kept for p in asked),
                         lambda f: "it does not keep "
                         + ", ".join(p for p in asked if p not in kept)),
                    Need("an integration migkit runs",
                         lambda f: bool(f.get("runs")),
                         lambda f: f.get("why_not")
                         or "migkit does not yet run it")),
             because="the provider carries the rows with nothing to run"),
        Rung("own", "migkit's own continuous hop",
             needs=(Need("a change tail on the source",
                         lambda f: bool(f.get("tail")),
                         "the source gives no change tail"),),
             because="every promise kept by migkit's own ledger"))
    costs = Costs({n: v for n in ("provider", "own")
                   if (v := lag(n)) is not None})
    return climb("the feed into the warehouse", rungs, f, costs=costs)
#: how a tail reads its source's log: pushed by the server as it commits,
#: or asked again at a pace the change rate sets (`tailctl.Pace`)
CHANGE_READS = (
    Rung("stream", "the source's log as the server streams it",
         needs=(Need("a reader that streams",
                     lambda f: f.get("stream client") is True,
                     "this machine's database client cannot read a"
                     " streamed log"),
                Need("the right to stream the log",
                     lambda f: f.get("stream granted") is True,
                     lambda f: str(f.get("stream why")
                                   or "the source's user may not open a"
                                      " replication stream")),),
         holds={"consistent": "as of one moment", "idempotent": "converges",
                "resumable": "goes on"},
         because="each change decoded once and pushed as it commits, the"
                 " position confirmed back only once applied",
         leaves="nothing"),
    Rung("poll", "the source's log asked again at a pace the change rate"
         " sets", holds={"consistent": "as of one moment",
                         "idempotent": "converges", "resumable": "goes on"},
         because="every change the log holds is read from the saved"
                 " position, however it is asked",
         leaves="nothing"))


def change_read(facts, costs=None):
    """The `Choice` of how a tail reads its source's log (`CHANGE_READS`)
    from what the grant allows and, where both were measured, the latency
    each gave a batch (`costs`: {"stream": s, "poll": s})."""
    return climb("reading the source's log", CHANGE_READS, facts,
                 costs=costs)
def _edge_fact(name):
    return lambda f: bool(f.get(name))


CHUNK_EDGES = (
    Rung("statistics", "the table's statistics",
         gives=frozenset({"edges"}),
         needs=(Need("statistics fine enough for the ranges",
                     _edge_fact("statistics"),
                     "the table has no statistics fine enough for its"
                     " ranges"),
                Need("statistics that still hold",
                     _edge_fact("statistics fresh"),
                     lambda f: f.get("statistics stale")
                     or "how far the table moved since its statistics is"
                     " not known")),
         because="the server already counted the rows, so no key is read"),
    Rung("sample", "a sample of the key taken now",
         gives=frozenset({"edges"}),
         needs=(Need("a sample the server can take",
                     _edge_fact("sample"),
                     "this engine has no statement for a sample of a"
                     " table's key"),),
         because="a sample of the live rows stands where the statistics"
                 " are stale, coarse or not about the rows asked for",
         loads={"source": 1}, trips=2.0),
    Rung("key order", "the key read in order",
         gives=frozenset({"edges"}),
         because="every row counted, so the ranges hold equal rows",
         loads={"source": 1, "cpu": 1}, trips=2.0),
)


def chunk_edges(work, facts):
    """The `Choice` of how a table's range edges are cut (backlog
    F7.edges): the table's statistics where they are fine enough and the
    table has moved less than half a range since, else a sample of the
    key taken now, else the key read in order - never the key's minimum
    and maximum divided alone."""
    return climb(work, CHUNK_EDGES, facts, need=("edges",))
# --- a Redis tail's live way and two sites both written (LT.C6, LT.C4) ------

def _at_least(have, need):
    return have is not None and need is not None and have >= need


def redis_live(facts):
    """The `Choice` of how a Redis tail follows its source past the copy.

    The replica stream first: migkit is a replica of the source, every
    write comes in the source's order with its offset, a stop resumes from
    the byte it reached and the source's answer to that offset proves no
    write was passed over. It needs an account the source lets replicate,
    one server (a cluster's shards each keep a stream of their own), and
    room on the source for the replica's buffer while the copy runs: the
    headroom measured (`headroom`) at least what the stream needs
    (`stream needs`: the backlog and the replica's buffer limit). Else the
    idle-time sweep: every key the source touched since the last round's
    start read again, and every key the target holds that the source no
    longer has removed - lossless for writes where the source keeps idle
    times (no frequency-based eviction), ordered only by rounds. Else
    nothing: the reason says which fact to change."""
    f = facts if isinstance(facts, Facts) else Facts(facts)

    def room(f):
        if f.get("stream needs") is None:
            return ("the replica stream needs the source's replica buffer"
                    " bounded or its last snapshot timed, and neither was"
                    " measured")
        return (f"the replica stream needs {f.get('stream needs'):,} bytes free on the"
                f" source, and {f.get('headroom') or 0:,} are")
    stream = Rung(
        "replica stream", "followed as a replica of the source, offset by"
        " offset", needs=(
            Need("an account that may replicate",
                 lambda f: f.get("replica grant") is True,
                 "{rung} needs an account the source lets replicate (psync,"
                 " replconf and ping granted)"),
            Need("one server", lambda f: not f.get("cluster"),
                 "{rung} follows one server, and the source is a cluster"),
            Need("room for the replica's buffer",
                 lambda f: _at_least(f.get("headroom"),
                                     f.get("stream needs")), room)),
        prove=lambda f: None if f.get("continued", True) else (
            "the source could not continue from the offset saved, so a"
            " write may have been passed over"),
        because="every write arrives in the source's order and a stop"
                " resumes from the byte it reached")
    sweep = Rung(
        "idle-time sweep", "every key touched since the last round read"
        " again, every key the source no longer has removed", needs=(
            Need("idle times kept", lambda f: f.get("idletime") is True,
                 "{rung} needs the source to keep idle times, and it evicts"
                 " by how often keys are read"),),
        footprint=1)
    return climb("the Redis tail's live way", (stream, sweep), f)


def two_site_rule(facts):
    """The `Choice` of how two Redis sites that both took writes are made
    alike, from the write overlap measured over the window: `overlap` keys
    written on both, `unmergeable` of them neither counters nor sets, and
    `skew_ms` the two clocks' measured skew with the reading lag (None
    where not measured). No key written on both: each site's writes go
    across. Every overlapping key a counter or a set: merged. Else the
    later write by offset time, where the two are further apart than the
    skew; else held and logged. A key whose two writes no rule settles is
    always logged and left as each site has it, never settled silently."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("the two sites' rule", (
        Rung("apart", "each site's writes carried to the other", needs=(
            Need("no key written on both", lambda f: not f.get("overlap"),
                 lambda f: f"{f.get('overlap'):,} keys were written on both"
                 " sites"),)),
        Rung("merged", "counters summed and sets merged member by member",
             needs=(Need("every key written on both mergeable",
                         lambda f: not f.get("unmergeable"),
                         lambda f: f"{f.get('unmergeable'):,} keys written"
                         " on both are neither counters nor sets"),)),
        Rung("merged, else newest", "counters and sets merged, any other"
             " key to the later write where the clocks tell them apart",
             needs=(Need("the clocks' skew measured",
                         lambda f: f.get("skew_ms") is not None,
                         "{rung} needs the two clocks' skew measured"),)),
        Rung("merged, else held", "counters and sets merged, any other key"
             " both wrote held and logged", footprint=1)), f)


# --- a tail batch applied as typed columns or as change records (R19.9) ----

_TAIL_HOLDS = {"consistent": "as of one moment", "idempotent": "converges",
               "resumable": "goes on"}

TAIL_APPLIES = (
    Rung("columns", "the batch as typed columns, written by the target's"
         " column writer",
         needs=(Need("a target that writes columns",
                     lambda f: f.get("writer") is True,
                     "the target has no writer of typed columns"),
                Need("every value typed exactly as columns",
                     lambda f: f.get("typed") is True,
                     lambda f: str(f.get("typed why")
                                   or "the batch's values were not typed")),
                Need("one lane",
                     lambda f: f.get("one lane") is True,
                     "the batch is long enough to go in lanes side by side,"
                     " which the change records do")),
         holds=_TAIL_HOLDS,
         because="one run of columns a table and op, in the changes' order,"
                 " each value of the type it came as",
         leaves="nothing"),
    Rung("records", "the batch as change records", holds=_TAIL_HOLDS,
         because="each change applied by key, in order, as the target's"
                 " own applier collapses them",
         leaves="nothing"))


def tail_apply(facts):
    """The ranked ways a tail batch is applied (`TAIL_APPLIES`), for
    `attempt`: the columns where the target writes them and every value
    went into them exactly, the change records as the fallback."""
    return TAIL_APPLIES


COLUMN_WRITES = (
    Rung("copy", "the batch's columns copied into a stage and moved on in"
         " one statement", footprint=1,
         because="one copy for the rows, whatever their number: two round"
                 " trips, and the rows at the copy's rate"),
    Rung("statements", "the batch's columns as multi-row statements sent"
         " without waiting",
         because="no stage to fill and empty: one round trip piped, and"
                 " the rows at the statements' rate"))


def column_write(facts, costs=None):
    """The `Choice` of how a batch of columns is written (`COLUMN_WRITES`):
    `costs` the seconds each way is forecast to take for this batch - its
    round trips at the target's measured latency and its bytes at the
    rate the way measured here; a way not yet measured is forecast at
    nothing, so each is tried once before the forecasts decide."""
    return climb("writing a batch's columns", COLUMN_WRITES, facts,
                 costs=costs)
#: what a cell can say that is work still owed, of any of the three tables
_OWED = (NOT_YET, "undeclared", "stale", "untested", "claimed")


def still_lacking(engine=None):
    """{engine: [(table, name, state, why)]}: what each engine still
    lacks, read off the code and never written down: a shape of `WAYS`
    with no way (`cell`), a capability its probe does not find
    (`capabilities.matrix`), a depth item the code has no way for or has
    and no test proves (`capabilities.depth_cell`). Each row says the
    backlog row it waits on, or what the check fails on; a cell that says
    the thing does not apply to the engine is not a lack. The depth items
    that read a shape or a capability are listed once, under that table."""
    from . import capabilities
    from .engines import NAMES
    known = capabilities.matrix()
    out = {}
    for n in [engine] if engine else NAMES:
        rows = [("shape", s, *cell(n, s)) for s in SHAPES]
        rows += [("capability", c, state,
                  capabilities.GAPS.get(n, {}).get(c, ("", ""))[1])
                 for c, state in known[n].items()]
        rows += [("depth", i, *capabilities.depth_cell(n, i))
                 for i in capabilities.DEPTH
                 if i not in capabilities.LINKED]
        out[n] = [r for r in rows if r[2] in _OWED]
    return out


def orphan_ways():
    """(shape, engine) of each way declared in `WAYS` for a shape or an
    engine the code does not have: a rung nobody can reach, which no cell
    of the matrix would ever show."""
    from .engines import NAMES
    return [(s, e) for s, ways in WAYS.items() for e in ways
            if s not in SHAPES or (e != "*" and e not in NAMES)]
def redis_seed(facts):
    """The `Choice` of how a Redis tail's full start fills the target.

    From the snapshot the source sends the replica stream: the source as
    of the stream's first offset, so nothing between is read twice or
    missed, and no key is read from the source. It needs room to keep the
    snapshot here while it arrives (`local free` at least `snapshot
    bytes`, the source's measured dataset) and memory for the batches it
    writes (`local memory` at least `memory needs`); it proves itself by
    reading whole - a kind the reader does not know, a value past the
    memory measured or a checksum that does not match is `unread`, and
    the next start knows it. Else every key read again from the source."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("the Redis tail's first fill", (
        Rung("snapshot", "filled from the snapshot the source sent, at its"
             " offset", needs=(
                 Need("room to keep the snapshot",
                      lambda f: _at_least(f.get("local free"),
                                          f.get("snapshot bytes")),
                      lambda f: f"the snapshot needs about"
                      f" {f.get('snapshot bytes') or 0:,} bytes kept here,"
                      f" and {f.get('local free') or 0:,} are free"),
                 Need("memory for its batches",
                      lambda f: _at_least(f.get("local memory"),
                                          f.get("memory needs")),
                      lambda f: "this machine's free memory was not read"
                      if f.get("local memory") is None else
                      f"its batches need {f.get('memory needs') or 0:,}"
                      f" bytes of memory, and {f.get('local memory'):,}"
                      " are free"),
                 Need("every change carried",
                      lambda f: f.get("every change carried", True),
                      "the hop keeps some of the source's changes off the"
                      " target, and a snapshot fill writes every key")),
             prove=lambda f: f.get("unread") or None,
             because="the snapshot is the source at the stream's first"
                     " offset, and no key is read from the source"),
        Rung("scan", "every key read again from the source and written",
             footprint=1)), f)


def phase_order(facts):
    """The `Choice` of the order of one database's phases
    (P0-WORKFLOW-DEPTH.model): the schema whole before the copy, or the
    tables first and the keys and constraints after the rows. Read from
    the dependencies and the engines' semantics: `fk cycle` (True where
    the foreign keys close a cycle - no table order loads them in place,
    None where not read), `target keys` (the target enforces foreign keys
    at all). A cycle not read is not taken as none: the order that holds
    in either case is taken."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("the order of the phases", (
        Rung("schema-whole", "the schema whole, its keys and constraints"
             " with it, before the copy", needs=(
                 Need("no cycle of foreign keys",
                      lambda f: f.get("fk cycle") is False
                      or f.get("target keys") is False,
                      lambda f: "the foreign keys were not read, so a cycle"
                      " among them is not ruled out"
                      if f.get("fk cycle") is None else
                      "the foreign keys close a cycle, which no order of"
                      " tables loads with them in place"),),
             because="every table lands with its constraints already"
                     " checking it, and nothing is added after the rows"),
        Rung("constraints-after", "the tables first, the copy, then the keys"
             " and constraints", footprint=1,
             because="the rows land in any order and the constraints are"
                     " checked whole once they are in")), f)


def phase_step(facts):
    """The `Choice` of what one phase of a workflow does now: enter it
    (`every prerequisite done`, `entry holds`), stay blocked behind a
    prerequisite that failed (`prerequisite failed` - its words), or wait
    for one still owed. Read at every transition, never a fixed order."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("the next transition of a phase", (
        Rung("enter", "entered", needs=(
            Need("every prerequisite done",
                 lambda f: not f.get("prerequisites owed"),
                 lambda f: "it waits for " + str(f.get("prerequisites owed"))),
            Need("its entry criteria",
                 lambda f: not f.get("entry unmet"),
                 lambda f: "its entry criteria do not hold: "
                 + str(f.get("entry unmet")))),
             because="what it needs is done and its entry criteria hold"),
        Rung("blocked", "blocked", needs=(
            Need("a prerequisite that failed, or entry criteria that do"
                 " not hold with nothing left to wait for",
                 lambda f: bool(f.get("prerequisite failed")) or bool(
                     f.get("entry unmet")
                     and not f.get("prerequisites owed"))),),
             because="a phase it needs failed, so what that phase owed it"
                     " is missing"),
        Rung("wait", "waits", footprint=1,
             because="a phase it needs is not done yet")), f)

def range_stall(facts):
    """The `Choice` for a range of a move whose recorded server session was
    read (`attempts.Monitor.unit_health`, backlog P0-ETA-ALL move health):
    replay it from its checkpoint where the server no longer lists the
    session (its one statement's transaction is gone with it, so nothing
    of it is uncertain); close exactly that session and replay where the
    server lists it idle past the keepalive observed on a connection here,
    and it is the hop's account's; else wait - no fixed timeout, only what
    was read. `facts`: `listed` (True, False, or None where the server's
    view could not be read), `state`, `idle_seconds` the server counts,
    `keepalive` (`attempts.keepalive_seconds`, None where not observed),
    `owned`, `open` (the unit has not closed)."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    idle = float(f.get("idle_seconds") or 0.0)
    keep = f.get("keepalive")
    listed = f.get("listed")
    seen = ("the server's view of the session could not be read"
            if listed is None else "the server lists the session"
            if listed else "the server no longer lists the session")
    said = (f"{seen}, {f.get('state') or 'no state'}, idle {idle:.1f}s"
            + (f" against a {float(keep):.1f}s keepalive observed"
               if keep is not None else ", no keepalive observed"))
    still = Need("the unit still open", lambda f: f.get("open"),
                 "the unit has closed: nothing is owed")
    rungs = (
        Rung("replay", "replay the range from its checkpoint", needs=(
            still, Need("a session the server no longer lists",
                        lambda f: f.get("listed") is False,
                        f"not gone: {said}")),
             because=said),
        Rung("close", "close that session only, then replay the range",
             footprint=1, needs=(
                 still,
                 Need("an idle session", lambda f: f.get("listed") is True
                      and f.get("state") == "idle",
                      f"not idle: {said}"),
                 Need("idle past the keepalive observed",
                      lambda f: f.get("keepalive") is not None and idle
                      > float(f["keepalive"]),
                      f"not idle long enough: {said}"),
                 Need("the session owned", lambda f: f.get("owned"),
                      f"the session is not the hop's account's: {said}")),
             because=said),
        Rung("wait", "wait on the range", because=said))
    return climb("a range whose session was read", rungs, f)


def lift_hold(facts):
    """The `Choice` for a hold of writes a run left behind
    (`leftovers.holds`): give it back as its record took it, or leave it.
    Read from `holder gone` (True where the process that took it has
    died, False where it still runs - it may be a cutover writing now -
    None where the record names none), `age` (seconds since the hold was
    taken), `longest step` (seconds the longest step of the hop's last
    run took, None where no run was kept) and `side`. A hold that names
    no process is lifted only where it is older than any step the run
    took; one whose holder still runs is never lifted here."""
    f = facts if isinstance(facts, Facts) else Facts(facts)

    def stale(f):
        age, step = f.get("age"), f.get("longest step")
        return age is not None and step is not None and age > step
    return climb("a hold of writes left behind", (
        Rung("lift", "given back as its record took it", needs=(
            Need("its side named", lambda f: f.get("side") in ("src", "dst"),
                 "the record names no side to give back"),
            Need("its holder gone",
                 lambda f: f.get("holder gone") is True or (
                     f.get("holder gone") is None and stale(f)),
                 lambda f: "the process that took it still runs and may"
                 " be writing" if f.get("holder gone") is False else
                 "the record names no process, and the hold is not older"
                 " than the longest step the run took"
                 if f.get("longest step") is not None else
                 "the record names no process and no run was kept to"
                 " tell a hold left behind from one in use")),
             because="nothing that took it is left to give it back"),
        Rung("wait", "left as it is", footprint=1,
             because="its holder may still be using it")), f)


# ---- how far two servers' settings are read, and how a loaded graph's
# ---- statistics are refreshed (backlog R16b, R16c) --------------------------
def settings_depth(facts):
    """How far a comparison of two vector servers' settings reads.

    `facts`: `collections` (those in scope on either side), `shards` (the
    shards the collections report, both sides together), `peers` (the most
    nodes either server reports in its cluster), `rtt` and `timeout` (the
    round trip measured and the longest a call may take), `shards_answer`
    and `cluster_answer` (None where the server answered, else why it did
    not). A deeper reading is taken where there is more to see - a
    collection split in shards, servers that are a cluster - the server
    answers for it, and its requests, counted from the collections and
    shards measured and priced at the round trip, fit the call's time."""
    def requests(depth):
        n = 2 * facts["collections"]
        if depth >= 1:
            n += 2 * facts["collections"]
        if depth >= 2:
            n += 2 * facts["collections"] + 4
        return n

    def fits(depth):
        return Need(
            "a reading that fits the time a call may take",
            lambda f: requests(depth) * f["rtt"] <= f["timeout"],
            lambda f: f"{requests(depth)} requests at about {f['rtt']:.3f}s"
                      f" would pass the {f['timeout']:.0f}s a call may take")
    rungs = (
        Rung("cluster", "every collection, every shard and the cluster",
             needs=(Need("servers that are a cluster",
                         lambda f: f["peers"] > 1,
                         "the servers run alone: there is no cluster to read"),
                    Need("a server that answers for its cluster",
                         lambda f: f["cluster_answer"] is None,
                         lambda f: "the cluster is not read:"
                                   f" {f['cluster_answer']}"),
                    Need("a server that answers for its shards",
                         lambda f: f["shards_answer"] is None,
                         lambda f: "the shards are not read:"
                                   f" {f['shards_answer']}"),
                    fits(2)),
             because="the servers are a cluster, and where each shard lives"
                     " changes what a search and a write cost"),
        Rung("shards", "every collection and each of its shards",
             needs=(Need("a collection split in shards",
                         lambda f: f["shards"] > 2 * f["collections"],
                         "every collection is one shard: its configuration"
                         " says it all"),
                    Need("a server that answers for its shards",
                         lambda f: f["shards_answer"] is None,
                         lambda f: "the shards are not read:"
                                   f" {f['shards_answer']}"),
                    fits(1)),
             because="a collection split in shards is searched and written"
                     " shard by shard"),
        Rung("collections", "each collection's configuration",
             because="a collection's configuration holds every setting that"
                     " changes its recall, size and speed"))
    return climb("the settings of two servers compared", rungs, facts)


def settle_way(facts):
    """How the statistics of a graph just loaded are refreshed.

    `facts`: `indexes` (how many the target holds), `rows` (the elements
    in it), `loaded` (those this run wrote), `sampling` (the share of an
    index that must change before the server resamples it by itself, or
    None where it could not be read) and `procedures` (the refreshes the
    server lists). Every way waits for the indexes to come online first;
    the server resamples what it finds changed past `sampling` on its own
    schedule, so a load that changed more than that is resampled now."""
    def share(f):
        return 100.0 * f["loaded"] / f["rows"] if f["rows"] else 0.0
    some = Need("an index to resample", lambda f: f["indexes"] > 0,
                "the target holds no index: there are no statistics to"
                " refresh")
    rungs = (
        Rung("resample", "resample every index now",
             needs=(some,
                    Need("a refresh that resamples every index",
                         lambda f: bool({"db.prepareForReplanning",
                                         "db.resampleIndex"}
                                        & set(f["procedures"])),
                         "the server lists no refresh of every index"),
                    Need("a load that changed more than the server's own"
                         " sampling follows",
                         lambda f: f["sampling"] is None
                         or share(f) >= f["sampling"],
                         lambda f: f"the load changed {share(f):.0f}% of the"
                                   f" graph, under the {f['sampling']}% the"
                                   " server resamples by itself")),
             because="the load changed most of the graph, and the server"
                     " samples what it changed only later"),
        Rung("outdated", "resample the indexes the server finds outdated",
             needs=(some,
                    Need("a refresh of the outdated indexes",
                         lambda f: "db.resampleOutdatedIndexes"
                         in f["procedures"],
                         "the server lists no refresh of the outdated"
                         " indexes"),
                    Need("rows loaded", lambda f: f["loaded"] > 0,
                         "nothing was loaded: nothing is outdated")),
             because="a part of the graph changed, and only the indexes the"
                     " server finds outdated need a new sample"),
        Rung("wait", "wait for the indexes to come online",
             because="the server resamples by itself what a small load"
                     " changed"))
    return climb("the statistics of a loaded graph refreshed", rungs, facts)




#: the level `assess` gives each rung of `change_depth_level`
CHANGE_DEPTH_LEVELS = {"whole": "pass", "unread": "pass", "partial": "warn",
                       "none": "fail"}


def change_depth_level(facts):
    """The `Choice` of the level a table's change-reading depth earns
    (`changedepth.SCALE`), from the goal and the depth read (W15J-D0):
    every change whole (D4) passes; a goal that reads no changes (a copy,
    a proof, a one-shot move with the writes stopped) passes at any depth,
    the depth said as a note - its check compares every row once; a goal
    that follows the source's changes warns on a partial depth (D1-D3) and
    fails on none (D0), with the statement that raises it. `facts`:
    `depth` (D0-D4), `goal` (the hop's goal word), `follows` (True where
    the step or goal follows the source's changes, False where it does
    not, None where not said - read as following, the stricter level)."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    depth, goal = f.get("depth"), f.get("goal") or "not said"
    said = f"change reading {depth} on goal {goal}"
    return climb("the level of a change-reading depth", (
        Rung("whole", "every change read whole", needs=(
            Need("every change with its images", lambda f: f.get("depth")
                 == "D4", f"{said}, not every change whole (D4)"),),
             because=f"{said} reads every change whole"),
        Rung("unread", "not needed by the goal", needs=(
            Need("a goal that reads no changes",
                 lambda f: f.get("follows") is False,
                 lambda f: f"{said}: the goal follows the source's changes"
                 if f.get("follows") else f"{said}: whether the goal"
                 " follows the source's changes was not said"),),
             because=f"{said}: the goal reads no changes, so its check"
                     " compares every row once"),
        Rung("partial", "a partial change reading", needs=(
            Need("some change reading", lambda f: f.get("depth") in (
                "D1", "D2", "D3"), f"{said} reads no changes at all"),),
             because=f"{said} reads part of each change"),
        Rung("none", "no change reading for a goal that follows changes",
             footprint=1,
             because=f"{said} reads no changes, and the goal follows"
                     " them")), f)

def session_settings(facts):
    """The `Choice` of the session settings a program the move drives
    sends a server (W15J-PG16): a build sends the settings of the server
    version it was compiled for, and a server older than that refuses the
    statement - in a transaction, every statement after it. Costed from
    what was measured: `server` (the server's `server_version_num`, None
    where it was not read), `build` (the major version the program was
    compiled for, None where it was not read), `sends` (((setting, the
    first `server_version_num` that has it), ...) the build sends), and
    `can drop` (the program can be told to leave a setting out: its
    statements given as text, or an option of its own). Send them all
    where the server has every one; else leave out those it lacks where
    the program can be told; else the program is refused and the range
    copier, which sends only what the server has, carries the work. A
    server or build not read is not taken as a match: a setting is sent
    only where the server is known to have it."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    server = f.get("server")
    sends = tuple(f.get("sends") or ())
    lacks = [n for n, low in sends if server is None or int(server) < low]
    said = (f"the server is {server if server is not None else 'not read'},"
            f" the build was compiled for"
            f" {f.get('build') if f.get('build') is not None else 'a version not read'}"
            + (f"; the server lacks {', '.join(lacks)}" if lacks else
               "; the server has every setting it sends"))
    return climb("the session settings sent the server", (
        Rung("send", "every setting the build sends, sent", needs=(
            Need("a server that has every setting the build sends",
                 lambda f: not lacks, f"not sent as is: {said}"),),
             because=said),
        Rung("drop", "the settings the server lacks left out", footprint=1,
             needs=(Need("a program that can be told to leave one out",
                         lambda f: bool(f.get("can drop")),
                         f"the program cannot be told to leave a setting"
                         f" out: {said}"),),
             because=said),
        Rung("copier", "the range copier instead, which sends only what"
             " the server has", footprint=2, because=said)), f)


# ---- how long the tail waits once its read's link is lost (backlog
# ---- P0-RESILIENCE-ALL) ------------------------------------------------------

def lost_link_wait(reconnects, down=0.0):
    """(seconds, `Choice`): how long the tail waits, once its read's link
    was lost past the read's own tries (`faults.retried`), before it reads
    again from the saved position. No number is written here: the longest
    time a lost link of this loop took to answer again (`reconnects`, each
    measured from the loss to the next read that answered), less what this
    loss has lasted (`down`); where none was measured or this loss has
    outlasted them all, as long again as it has lasted - so the wait grows
    from what was observed, with no fixed ceiling - and a loss that has
    not lasted at all is read again at once."""
    d = tuple(sorted(float(x) for x in reconnects or ()
                     if x is not None and x >= 0))
    down = max(0.0, float(down or 0.0))
    said = ((f"reconnects measured here took "
             + ", ".join(f"{x:.1f}s" for x in d)) if d
            else "no reconnect measured here") + (
        f"; this loss has lasted {down:.1f}s")
    rungs = (
        Rung("measured", "wait out the longest reconnect measured here",
             needs=(Need("a reconnect measured",
                         lambda f: bool(f.get("reconnects")),
                         "no lost link of this loop has come back yet"),
                    Need("a loss not past it",
                         lambda f: f.get("down") < f.get("reconnects")[-1],
                         "this loss has outlasted every reconnect"
                         " measured")),
             because=said),
        Rung("as long again", "wait as long again as the link has been"
             " down", needs=(Need("a loss that has lasted",
                                  lambda f: f.get("down") > 0,
                                  "the link was lost just now"),),
             because=said),
        Rung("at once", "read again now", because=said))
    got = climb("a lost link of the tail", rungs,
                Facts({"reconnects": d, "down": down}))
    seconds = {"measured": (d[-1] - down) if d else 0.0,
               "as long again": down}.get(got.path, 0.0)
    return max(0.0, seconds), got


# ---- Db2's own ways: a table loaded by the server, its statistics, the
# ---- target's rollback point and the rows changed since a clean pass
# ---- (backlog R13-DB2; built on the documentation, not run on a server) -----

def db2_bulk_way(facts):
    """How one table of a Db2 pair is carried: by the server's own load
    reading the source through a cursor, or by the table copier.

    `facts`: `platform` (luw, zos or i), `rows` (the catalogue's count of
    the source table: SYSCAT.TABLES.CARD, SYSIBM.SYSTABLES.CARDF, or None
    where statistics were never gathered), `width` (its average row in
    bytes, None where not read), `empty` (the target table holds no row),
    `mapped` (the hop renames or transforms its columns), `archived` (the
    target archives its log, so a load not logged would leave its table
    space waiting for a backup; None where not read), `alone` (z/OS: the
    target's table space holds this table only), `same_place` (i: both
    sides are one system), `copier_rate` (rows a second the copier was
    measured at in this run, None before) and `load_start` (seconds a load
    of this run took to start, None before). A load needs the rows it
    replaces to be none and the table to be the hop's as it is; where the
    two rates were measured it is taken only where the copier would take
    longer than the load's start alone."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    plat = f.get("platform")
    said = (f"{f.get('rows') if f.get('rows') is not None else 'unknown'}"
            f" rows of about {f.get('width') or '?'} bytes")

    def pays(f):
        rate, start = f.get("copier_rate"), f.get("load_start")
        if not rate or start is None:
            return True
        return f.get("rows") / rate > start
    common = (
        Need("a table the hop takes as it is",
             lambda f: not f.get("mapped"),
             "the hop renames or transforms its columns: the copier"
             " carries what the server's load cannot"),
        Need("a size the catalogue knows",
             lambda f: (f.get("rows") or 0) > 0,
             f"the catalogue says {said}: the copier sizes its own"
             " ranges"),
        Need("an empty target table", lambda f: bool(f.get("empty")),
             "the target table holds rows, and a load only adds to them"),
        Need("both sides on one server",
             lambda f: bool(f.get("same_place")),
             "the two sides are two servers: the server's own load reads"
             " another only with that one's password written into the"
             " command, which the statement history keeps"),
        Need("a load that outruns the copier",
             lambda f: pays(f),
             lambda f: f"the copier at {f.get('copier_rate'):.0f} rows a"
                       f" second finishes before a load starts"
                       f" ({f.get('load_start'):.1f}s)"))
    luw = Need("Db2 for Linux, UNIX and Windows",
               lambda f: f.get("platform") == "luw", "not this platform")
    zos = Need("Db2 for z/OS", lambda f: f.get("platform") == "zos",
               "not this platform")
    ibmi = Need("IBM i", lambda f: f.get("platform") == "i",
                "not this platform")
    rungs = (
        Rung("load", "the server's own load, from a cursor on the source",
             needs=(luw,) + common + (
                 Need("a target whose log is not archived",
                      lambda f: f.get("archived") is False,
                      "the target archives its log (or it was not read):"
                      " a load would leave its table space waiting for a"
                      " backup"),),
             because=f"{said}: the server writes its pages directly"),
        Rung("cross-load", "the load utility reading a cursor (the"
             " cross-loader), logged", needs=(zos,) + common + (
                 Need("a table space holding this table only",
                      lambda f: bool(f.get("alone")),
                      "its table space holds other tables, which a load"
                      " could hold or reset"),
                 Need("no column the target always generates",
                      lambda f: not f.get("always"),
                      "a column is GENERATED ALWAYS: the utility would"
                      " give it the target's value, not the source's"),),
             because=f"{said}: the utility writes the pages, logged"),
        Rung("copy-file", "the system's own copy of the file", needs=(
            ibmi,) + common,
             because=f"{said}: the system copies the members itself"),
        Rung("copier", "the table copier", footprint=1,
             because=f"{said} on {plat}: the copier reads and writes it"
                     " by key ranges"))
    return climb("a Db2 table carried", rungs, f)


def db2_statistics(facts):
    """How a Db2 table's statistics are refreshed after the move.

    `facts`: `rows` (the rows counted now), `card` (what the catalogue
    holds, None where never gathered), `indexes` (how many the table has)
    and `ratio` (the share that can change before statistics stop being
    trusted, `Engine.STALE_STATS_RATIO`). Statistics the catalogue holds
    within that share are left; else a table with indexes is gathered with
    its distribution and its indexes, so the optimiser chooses between
    them on values as they are, and one without on its rows alone."""
    f = facts if isinstance(facts, Facts) else Facts(facts)

    def stale(f):
        card = f.get("card")
        if card is None or card < 0:
            return True
        return abs(f.get("rows") - card) > f.get("ratio") * max(card, 1)
    said = (f"{f.get('rows')} rows, the catalogue says"
            f" {f.get('card') if f.get('card') is not None else 'none'},"
            f" {f.get('indexes')} indexes")
    old = Need("statistics older than the rows", stale,
               f"{said}: the statistics are current")
    return climb("a Db2 table's statistics", (
        Rung("distribution", "statistics with the distribution of values,"
             " and every index", needs=(old, Need(
                 "an index to choose", lambda f: f.get("indexes") > 0,
                 "the table has no index")), because=said),
        Rung("table", "statistics of the table's rows", needs=(old,),
             because=said),
        Rung("current", "left as the catalogue holds them", footprint=1,
             because=said)), f)


def db2_snapshot_mark(facts):
    """What a Db2 target's rollback point holds beside its counters.

    `facts`: `log` (None where the server answered where its log is now,
    else why not). The counters are read together in one unit of work
    at repeatable read on every platform; where the log answers, its
    position is kept with them, so a later read of the log starts where
    the counters stood."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("a Db2 rollback point", (
        Rung("log mark", "the counters read together, and the log's"
             " position they stand at", needs=(Need(
                 "a log that says where it is",
                 lambda f: f.get("log") is None,
                 lambda f: f"the log's position was not read:"
                           f" {f.get('log')}"),),
             because="the counters and the log's position are one"
                     " moment"),
        Rung("together", "the counters read together at repeatable read",
             footprint=1,
             because="the counters are one moment; no log position is"
                     " kept")), f)


def db2_delta_way(facts):
    """How the rows a Db2 source changed since the last clean pass are
    found.

    `facts`: `log` (the source's change log can be read without moving it:
    the reader `Db2Engine.change_log` names), `stamped` (the tables in
    scope with a ROW CHANGE TIMESTAMP column), `tables` (those in scope)
    and `same` (both sides are Db2, whose rows compare as the driver
    hands them). The log names every change, a delete too; a row change
    timestamp names the rows written since, never one deleted, so the
    full check stays owed for deletes."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("the rows a Db2 source changed", (
        Rung("log", "the keys the change log names", needs=(Need(
            "a change log read without being moved",
            lambda f: bool(f.get("log")),
            "no reader of the source's log is installed"),),
             because="the log names every change, deletes too"),
        Rung("stamped", "the rows whose row change timestamp is past the"
             " last clean pass", footprint=1, needs=(
                 Need("a table with a row change timestamp",
                      lambda f: f.get("stamped", 0) > 0,
                      lambda f: f"none of the {f.get('tables')} tables has"
                                " a row change timestamp"),
                 Need("two Db2 sides", lambda f: bool(f.get("same")),
                      "the target is another engine: its rows are"
                      " compared by the full check")),
             because="a row change timestamp names every row written"
                     " since; deletes stay the full check's"),
        Rung("none", "not verified by change", footprint=2,
             because="nothing here names the rows changed: the full check"
                     " compares every row")), f)

# The two engines no hold of migkit's covers yet, said rather than left
# open (W16J). Qdrant lets a write through on the server's API keys, set
# when the server starts, and its one switch that refuses writes refuses
# them on every collection and from every account, migkit's included - so
# it cannot keep a target migkit writes into, and a source is held by its
# operator. Neo4j holds writes by role on its enterprise edition only;
# migkit has no hold of it, so its operator holds them, and the cutover
# proves the hold (`freeze: operator`).
WAYS["read-only target"]["qdrant"] = (
    NOT_APPLICABLE, "its writes are let through by the API keys the server"
    " starts with, and its one switch that refuses writes refuses them on"
    " every collection and from every account, migkit's own included, so"
    " nothing keeps a target from the application and leaves migkit"
    " writing")
WAYS["writes held"]["qdrant"] = (
    NOT_APPLICABLE, "its writes are let through by the API keys the server"
    " starts with, which migkit cannot take from one application, so a"
    " source's writes are stopped by its operator (freeze: operator) and"
    " proved by migkit")
WAYS["read-only target"]["neo4j"] = (
    NOT_APPLICABLE, "its writes are held by role only on its enterprise"
    " edition, through roles migkit does not change, so a target is kept"
    " by its operator; migkit's final check catches a row written there")
WAYS["writes held"]["neo4j"] = (
    NOT_APPLICABLE, "its writes are held by role only on its enterprise"
    " edition, through roles migkit does not change, so a source's writes"
    " are stopped by its operator (freeze: operator) and proved by migkit")

# ---- where the cloud's own sign-in is minted, and when it is minted again
# ---- (backlog 38.real) ------------------------------------------------------

def cloud_sign_in(facts):
    """`Choice` of where a side whose `auth` names the cloud's own sign-in
    gets its token, from what this machine has (`cloudapi.sign_in_facts`):
    the provider's own library and the credentials its chain finds, the
    identity endpoint a hosting service puts in the environment, or the
    instance's metadata endpoint where it answers. No rung holds: the
    reason says what each way lacked, and nothing is minted."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    kind = f.get("kind")
    said = (f"the sign-in is {kind}; library "
            f"{'installed' if f.get('sdk') else 'not installed'}, its"
            f" credentials {'found' if f.get('credentials') else 'not found'}")

    def is_(*kinds):
        return Need(f"a sign-in of {' or '.join(kinds)}",
                    lambda x: x.get("kind") in kinds,
                    f"the side's sign-in is {kind}")
    return climb("where the cloud's own sign-in is minted", (
        Rung("signer", "signed on this machine by the provider's own"
             " library, from the credentials its chain finds",
             needs=(is_("aws_iam"),
                    Need("the provider's library", lambda x: x.get("sdk"),
                         "the provider's library is not installed")),
             because=said),
        Rung("chain", "the access token of the credentials the provider's"
             " own chain finds on this machine",
             needs=(is_("cloudsql_iam", "azure_managed_identity"),
                    Need("the provider's library", lambda x: x.get("sdk"),
                         "the provider's library is not installed"),
                    Need("credentials it finds",
                         lambda x: x.get("credentials"),
                         "its chain finds no credentials here")),
             because=said),
        Rung("identity endpoint", "the identity endpoint the hosting"
             " service gives this process", footprint=1,
             needs=(is_("azure_managed_identity"),
                    Need("an identity endpoint in the environment",
                         lambda x: x.get("identity endpoint"),
                         "the environment names no identity endpoint")),
             because=said),
        Rung("metadata", "the token the instance's metadata endpoint"
             " gives the machine's own identity", footprint=1,
             needs=(is_("cloudsql_iam", "azure_managed_identity"),
                    Need("a metadata endpoint that answers",
                         lambda x: x.get("metadata"),
                         "no metadata endpoint answers here: this machine"
                         " is not one of the cloud's")),
             because=said)), f)


def token_renew(lifetime, mints=()):
    """(seconds before its end a token is minted again, `Choice`): a third
    of its lifetime where minting is quick next to that - the provider's
    documented lifetime is what is read, no number is written - and twice
    the longest mint measured here where that is longer, so a slow signer
    never hands out a token that ends before its connection opens."""
    life = max(0.0, float(lifetime or 0.0))
    took = max((float(x) for x in mints or () if x is not None), default=0.0)
    said = (f"the token lasts {life:.0f}s; the longest mint here took"
            f" {took:.1f}s")
    got = climb("when the cloud's own sign-in is minted again", (
        Rung("measured", "minted again twice the longest mint before its"
             " end", needs=(Need("a mint slower than a sixth of the"
                                 " lifetime", lambda f: 2 * took > life / 3,
                                 "minting is quick next to the lifetime"),),
             because=said),
        Rung("third", "minted again with a third of its lifetime left",
             because=said)), Facts({"lifetime": life, "took": took}))
    return (2 * took if got.path == "measured" else life / 3), got


# ---- how a server only the cloud forwards into is reached (backlog
# ---- R6.cloud) --------------------------------------------------------------

def cloud_forwarder(facts):
    """`Choice` of how a side is reached, from what the endpoint names, what
    this machine has installed and which network answers: directly where
    its port answers from here; else the cloud's own forwarder the
    endpoint names whose client is installed - the database service's own
    connector before a session into a machine of the network, which adds a
    leg - and none where nothing holds, the reason saying what each way
    lacked. `facts`: {"named": the ways the endpoint names, "installed":
    {way: bool}, "answers": whether the port answered directly,
    "private": whether its address is a private network's}."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    named = set(f.get("named") or ())
    have = dict(f.get("installed") or {})
    said = ("its address is "
            + ("a private network's" if f.get("private") else "public")
            + ("; it answers from here" if f.get("answers") else
               "; it does not answer from here"))

    def way(name, words, legs):
        return Rung(name, words, footprint=legs, because=said, needs=(
            Need(f"the endpoint names {words}", lambda x: name in named,
                 f"the endpoint does not name {words}"),
            Need(f"the client of {words} installed",
                 lambda x: bool(have.get(name)),
                 f"the client of {words} is not installed here")))
    return climb("how the side is reached", (
        Rung("direct", "connected directly: the server answers from here",
             because=said, needs=(Need(
                 "a port that answers from here", lambda x: x.get("answers"),
                 "it does not answer from here"),)),
        way("cloudsql", "the database service's own connector", 1),
        way("ssm", "the cloud's own session forwarder", 2),
        way("iap", "the cloud's identity-aware tunnel", 2),
        way("bastion", "the cloud's own bastion tunnel", 2)), f)


def redis_site_rule(facts):
    """The `Choice` of how two Redis sites that both took writes are made
    alike by the rule the hop states (`two_way.on_conflict`, `stated`),
    from the write overlap measured over the window (`overlap`,
    `unmergeable`: keys written on both that are not both counters or both
    sets) and the clocks' measured skew (`skew_ms`). No key written on
    both: each site's writes go across, whatever is stated. `delta` or
    `union` stated: counters summed and sets merged member by member, as
    the conflict-free types of the vendors' active-active documentation
    are; a key of any other type written on both is refused by name.
    `newest` stated: the later write by the server's own clock, where the
    two are further apart than the skew measured; within it, refused by
    name. Nothing stated (or a rule that settles nothing here): every key
    written on both refused by name - never one site's write silently
    over the other's."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    stated = str(f.get("stated") or "").strip().lower()
    return climb("the two sites' stated rule", (
        Rung("apart", "each site's writes carried to the other", needs=(
            Need("no key written on both", lambda f: not f.get("overlap"),
                 lambda f: f"{f.get('overlap'):,} keys were written on both"
                 " sites"),)),
        Rung("merged", "counters summed and sets merged member by member,"
             " as the hop states", needs=(
                 Need("a merge stated", lambda f: stated in ("delta",
                                                             "union"),
                      f"the hop states {stated or 'no rule'}, not delta or"
                      " union"),),
             because="a counter's steps and a set's members added on two"
                     " sites commute, so both sites reach the same value"),
        Rung("newest", "the later write by the server's clock kept on"
             " both, as the hop states", needs=(
                 Need("newest stated", lambda f: stated == "newest",
                      f"the hop states {stated or 'no rule'}, not newest"),
                 Need("the clocks' skew measured",
                      lambda f: f.get("skew_ms") is not None,
                      "the two sites' clocks were not measured against each"
                      " other, so no write can be called the later")),
             because="each write carries its site's clock, and a gap"
                     " wider than the skew measured orders them")), f)


def mongo_change_images(facts):
    """The `Choice` of what a MongoDB change event carries of the document,
    from the server's version (`version`, a tuple) and the collections
    followed that keep their pre- and post-images (`collections`,
    `without images`: those that do not). Where every one keeps them on
    6.0 or later: the document as of the change itself (the post-image,
    required, so an expired one stops the tail at its position rather than
    passing a key with no values) and the document before it where the
    server still has it. Else: the document looked up as it is when the
    event is read, which converges but may arrive newer than its event."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    ver = tuple(f.get("version") or ())
    return climb("what a change event carries", (
        Rung("images", "each update with the document as of the change and"
             " the one before it", needs=(
                 Need("6.0 or later", lambda f: bool(ver) and ver >= (6, 0),
                      "the source predates 6.0, which keeps no document"
                      " images for its change events"),
                 Need("every collection keeps images",
                      lambda f: bool(f.get("collections"))
                      and not f.get("without images"),
                      lambda f: "no collection followed keeps its images"
                      if not f.get("collections") else
                      "not every collection followed keeps its images ("
                      + ", ".join(sorted(f.get("without images"))[:5])
                      + ")")),
             because="the event says the document as that change left it,"
                     " so no later write arrives early"),
        Rung("looked up", "each update with the document as it is when"
             " read", because="any server answers it, and writes by key"
                              " converge on it")), f)
# ---- how long a stopped command's cleanup is given (W16N-BASELINE) --------

def stop_grace(facts):
    """(seconds a command's cleanup is given after SIGTERM before it ends
    as a crash would, `Choice`). A stop runs every `finally`: the
    programs migkit started are asked to end and given a program's stop
    twice over (its SIGTERM, then its SIGKILL); with none running, the one
    statement in flight is given that stop once to answer its cancel. A
    statement measured slower than either is given twice its own time. A
    cleanup still waiting past it waits on something that will not end:
    what a crash leaves, every checkpoint written only after its effect,
    the next run reconciles. `facts`: {"programs": programs running now,
    "grace": the runner's grace for one program's stop - the floor,
    "stopped": the longest the last stop of a program took here, where
    measured, "slowest": the longest one statement has taken here, where
    measured}. The reason names the bound and the fact it came from."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    floor = max(1.0, float(f.get("grace") or 1.0))
    stopped = max(0.0, float(f.get("stopped") or 0.0))
    grace = max(floor, stopped)
    slow = max(0.0, float(f.get("slowest") or 0.0))
    stop_from = (f"a program's last stop measured {stopped:.1f}s"
                 if stopped > floor else
                 f"the {floor:.0f}s floor of a program's stop"
                 + (f" (its last stop measured {stopped:.1f}s)"
                    if stopped else ", no stop measured"))
    slow_said = (f"the slowest statement {slow:.1f}s" if slow
                 else "no statement measured")

    def bound(each):
        if 2 * slow > each:
            return 2 * slow, (f"bound {2 * slow:.0f}s from {slow_said}"
                              f" (twice its time; {stop_from})")
        return each, f"bound {each:.0f}s from {stop_from} ({slow_said})"
    many, many_said = bound(2 * grace)
    one, one_said = bound(grace)
    running = int(f.get("programs") or 0)
    got = climb("how long a stopped command's cleanup is given", (
        Rung("programs", "the programs it started stopped by their own"
             " ladder, SIGTERM then SIGKILL", needs=(Need(
                 "a program running", lambda x: (x.get("programs") or 0) > 0,
                 f"no program is running; {one_said}"),),
             because=f"{running} programs running; {many_said}"),
        Rung("statement", "the statement in flight given its cancel",
             because=f"no program is running; {one_said}")), f)
    return (many if got.path == "programs" else one), got

def ase_bulk_way(facts):
    """How one table of an SAP ASE pair is carried: by the server itself,
    one database's rows inserted into the other's in one statement, or by
    the table copier.

    `facts`: `rows` and `pages` (the source table's rows and data pages
    as the server counts them, `row_count` and `data_pages`, which
    `sp_spaceused` reports; None where not read), `page_size` (the
    server's page in bytes, `@@maxpagesize`), `log_free` (pages free on
    the target database's log segment, None where not read), `empty` (the
    target table holds no row), `mapped` (the hop renames or transforms
    its columns) and `same_place` (both databases are one server's). The
    statement is logged whole, so the target's log must hold every page
    of the table at once; anything else, or a fact not read, is the
    copier's, which commits by key ranges."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    said = (f"{f.get('rows') if f.get('rows') is not None else 'unknown'}"
            f" rows in {f.get('pages') if f.get('pages') is not None else '?'}"
            " pages")

    def room(f):
        pages, free = f.get("pages"), f.get("log_free")
        return pages is not None and free is not None and free >= pages
    return climb("an SAP ASE table carried", (
        Rung("insert-select", "the server's own insert of one database's"
             " rows into the other's, in one logged statement", needs=(
                 Need("a table the hop takes as it is",
                      lambda f: not f.get("mapped"),
                      "the hop renames or transforms its columns: the"
                      " copier carries what one statement cannot"),
                 Need("a size the server knows",
                      lambda f: (f.get("rows") or 0) > 0,
                      f"the server says {said}: the copier sizes its own"
                      " ranges"),
                 Need("an empty target table", lambda f: bool(f.get("empty")),
                      "the target table holds rows, and an insert only adds"
                      " to them"),
                 Need("both databases on one server",
                      lambda f: bool(f.get("same_place")),
                      "the two sides are two servers: one statement reads"
                      " only the databases of its own server"),
                 Need("a target log that holds the whole table", room,
                      lambda f: f"the target's log has"
                                f" {f.get('log_free') if f.get('log_free') is not None else 'an unread number of'}"
                                f" pages free for {said}: one statement"
                                " would fill it")),
             because=f"{said}: the rows never leave the server"),
        Rung("copier", "the table copier", footprint=1,
             because=f"{said}: the copier reads and writes it by key"
                     " ranges")), f)


def ase_statistics(facts):
    """How an SAP ASE table's statistics are refreshed after the move.

    `facts`: `changed` (the share of its rows changed since statistics
    were last updated, in percent, as `datachange` answers it; None where
    never gathered), `gathered` (statistics rows the table has in
    `sysstatistics`), `indexes` (how many it has) and `ratio`
    (`Engine.STALE_STATS_RATIO`). A table with indexes is gathered on
    every column of each (`update index statistics`), one without on its
    rows (`update statistics`); a table changed less than the ratio is
    left."""
    f = facts if isinstance(facts, Facts) else Facts(facts)

    def stale(f):
        if not f.get("gathered") or f.get("changed") is None:
            return True
        return f.get("changed") > f.get("ratio") * 100
    said = (f"{f.get('changed') if f.get('changed') is not None else 'never'}"
            f"% changed since its statistics, {f.get('indexes')} indexes")
    old = Need("statistics older than the rows", stale,
               f"{said}: the statistics are current")
    return climb("an SAP ASE table's statistics", (
        Rung("index", "statistics on every column of each index", needs=(
            old, Need("an index to choose", lambda f: f.get("indexes") > 0,
                      "the table has no index")), because=said),
        Rung("table", "statistics of the table", needs=(old,), because=said),
        Rung("current", "left as the server holds them", footprint=1,
             because=said)), f)


def ase_snapshot_mark(facts):
    """What an SAP ASE target's rollback point holds beside its identity
    counters.

    `facts`: `dbts` (None where the database answered its timestamp,
    `@@dbts`, else why not). The counters are read in one transaction at
    isolation level 3; the database's timestamp, which every change to a
    row moves on, is kept with them where it answers, so a later sweep of
    the rows stamped since starts where the counters stood."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("an SAP ASE rollback point", (
        Rung("timestamp mark", "the counters read together, and the"
             " database timestamp they stand at", needs=(Need(
                 "a database that says its timestamp",
                 lambda f: f.get("dbts") is None,
                 lambda f: f"the database timestamp was not read:"
                           f" {f.get('dbts')}"),),
             because="the counters and the database timestamp are one"
                     " moment"),
        Rung("together", "the counters read together at isolation level 3",
             footprint=1,
             because="the counters are one moment; no timestamp is kept")),
        f)


def ase_delta_way(facts):
    """How the rows an SAP ASE source changed since the last clean pass
    are found.

    `facts`: `stamped` (tables in scope with a `timestamp` column, which
    the server sets from the database timestamp on every insert and
    update), `tables` (those in scope), `same` (both sides are ASE, whose
    rows compare as the driver hands them) and `agent` (the source's
    replication agent is configured and running on the database, as
    `sp_config_rep_agent` says; its log is the replication server's to
    read, licensed apart, so it is said, not read). A timestamp names
    every row written since, never one deleted: the full check stays
    owed for deletes."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("the rows an SAP ASE source changed", (
        Rung("stamped", "the rows whose timestamp is past the last clean"
             " pass", needs=(
                 Need("a table with a timestamp column",
                      lambda f: f.get("stamped", 0) > 0,
                      lambda f: f"none of the {f.get('tables')} tables has"
                                " a timestamp column" + (
                                    " (the replication agent runs, and its"
                                    " log is the replication server's)"
                                    if f.get("agent") else "")),
                 Need("two ASE sides", lambda f: bool(f.get("same")),
                      "the target is another engine: its rows are compared"
                      " by the full check")),
             because="the database timestamp names every row written"
                     " since; deletes stay the full check's"),
        Rung("none", "not verified by change", footprint=1,
             because="nothing here names the rows changed" + (
                 " (the replication agent runs, and its log is the"
                 " replication server's)" if f.get("agent") else "")
             + ": the full check compares every row")), f)

# --- which brand answers, how sure, and what it decides (W17G) -------------

#: the confidence each rung of `brand_confidence` gives the brand
BRAND_CONFIDENCE = {"api": "sure", "named": "sure", "agreed": "sure",
                    "server": "likely", "address": "likely"}


def brand_confidence(facts):
    """`Choice` of how sure migkit is of the brand behind a side, from what
    `variants.identify` read: `server` (the brand a documented sign of the
    server names, or ""), `managed` (whether that brand is a managed
    service), `endpoint` (the brand the address catalogue names) and
    `address` (the service it names, of any family), `named` (the hop's
    `managed.service`), `api` (the brand the provider's own API confirmed)
    and `asked` (every question telling the family's brands apart was
    answered). An absence is never a sign: where no rung holds, the brand
    is not told apart and nothing brand-dependent acts on it."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def agrees(x, key):
        told = [x.get(k) for k in ("api", "named", "server", "endpoint")
                if x.get(k)]
        return bool(x.get(key)) and all(t == x.get(key) for t in told)
    said = (f"the server says {f.get('server') or 'nothing distinctive'},"
            f" the address {f.get('address') or 'names no service'}")
    return climb("how sure the brand answering is", (
        Rung("api", "confirmed by the provider's own API",
             needs=(Need("the provider's API answered for the side",
                         lambda x: agrees(x, "api"),
                         "the provider's API was not reached or does not"
                         " agree"),), because=said),
        Rung("named", "the service the hop names, which nothing read"
             " contradicts",
             needs=(Need("the hop names the service",
                         lambda x: agrees(x, "named"),
                         "the hop names no service, or a read fact"
                         " contradicts it"),), because=said),
        Rung("agreed", "a documented sign of the server, which its address"
             " and every other question asked agree with",
             needs=(Need("a sign of the server", lambda x: agrees(x, "server"),
                         "the server gave no sign of its own"),
                    Need("its address agrees",
                         lambda x: (x.get("endpoint") == x.get("server")
                                    if x.get("managed")
                                    else not x.get("address")),
                         "its address does not say the same"),
                    Need("every telling question answered",
                         lambda x: x.get("managed") or x.get("asked"),
                         "a question that tells its brands apart went"
                         " unanswered")), because=said),
        Rung("server", "a documented sign of the server alone",
             needs=(Need("a sign of the server", lambda x: agrees(x, "server"),
                         "the server gave no sign of its own"),),
             because=said),
        Rung("address", "the address alone, a fact of the endpoint and not"
             " of the server",
             needs=(Need("the address names the brand",
                         lambda x: not x.get("server")
                         and agrees(x, "endpoint"),
                         "the address names no brand of this family"),),
             because=said)), f)


def kafka_sign_in(facts):
    """`Choice` of how a Kafka side signs in, from the mechanisms its
    broker advertised in the SASL handshake (`advertised`, a list, or None
    where it was not read), the one the side's options ask (`asked`), and
    what this machine has: the provider's credentials (`credentials`) and
    the side's user and password (`account`). Amazon MSK's IAM token goes
    first where the broker advertises it; a side whose options name a
    mechanism takes only that one."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def offered(mech):
        def holds(x):
            got = x.get("advertised")
            asked = str(x.get("asked") or "").upper()
            if asked and asked != mech:
                return False
            return mech in [str(m).upper() for m in got] if got is not None \
                else asked == mech
        return Need(f"the broker advertises {mech}", holds,
                    f"the broker does not advertise {mech}, or the side asks"
                    " another")
    account = Need("a user and password", lambda x: x.get("account"),
                   "the side has no user and password")
    said = (f"advertised {', '.join(f.get('advertised') or ()) or 'unread'};"
            f" asked {f.get('asked') or 'none'}")
    return climb("how a Kafka side signs in", (
        Rung("AWS_MSK_IAM", "a token signed for the cluster from the"
             " provider's credentials (presigned connect, over TLS)",
             needs=(offered("AWS_MSK_IAM"),
                    Need("the provider's credentials",
                         lambda x: x.get("credentials"),
                         "this machine has no provider credentials")),
             because=said),
        Rung("SCRAM-SHA-512", "a salted challenge on the side's account,"
             " SHA-512", needs=(offered("SCRAM-SHA-512"), account),
             because=said),
        Rung("SCRAM-SHA-256", "a salted challenge on the side's account,"
             " SHA-256", needs=(offered("SCRAM-SHA-256"), account),
             because=said, footprint=1),
        Rung("PLAIN", "the side's account sent as it is, inside TLS",
             needs=(offered("PLAIN"), account), because=said,
             footprint=2)), f)


def ddl_follow(facts):
    """`Choice` of how a document store's schema changes are seen while
    its changes are followed: the change stream's own DDL events where the
    server emits them (`expanded_events`), else the catalogue (its
    collections, options and indexes) read again at every batch and held
    to the last read - the way that works on every brand, and the one a
    brand not told apart (`told` False) is given."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    return climb("how a schema change is seen while following", (
        Rung("events", "the change stream's own schema-change events",
             needs=(Need("a brand told apart", lambda x: x.get("told"),
                         "the brand is not told apart"),
                    Need("the server emits them",
                         lambda x: x.get("expanded_events"),
                         "the server emits no schema-change events")),
             because=f"brand {f.get('brand') or 'unknown'}"),
        Rung("snapshots", "the catalogue read again at every batch and held"
             " to the last read", footprint=1,
             because="every brand answers its catalogue")), f)

# --- the services on another's wire: what they store, where they say it ----
# (P0-CLOUDS.native, P0-DE.4). A service on MySQL's, PostgreSQL's or an
# Oracle-compatible wire takes its parent's way with large values and
# partitions where the wire is the same; what it changes - the widest value
# its large types store, the catalogue its partitions are kept in - is a
# fact read off the service at `assess`, and the way is the rung those
# facts hold.

#: OceanBase's widest large value (`LONGTEXT`, `LONGBLOB`, `CLOB`, `BLOB`),
#: by its documentation: 48 MB before version 4, 512 MB from it, in both
#: tenant modes
OCEANBASE_LOB = ((4, 512 * 2 ** 20), (0, 48 * 2 ** 20))


def ob_version(text):
    """(major, minor) of OceanBase's own version in what the tenant says of
    itself (`5.7.25-OceanBase-v4.2.1.0`, `OceanBase 3.2.4.0`), or None."""
    import re as _re
    got = _re.search(r"oceanbase[^0-9]*?v?(\d+)\.(\d+)", str(text or ""),
                     _re.I)
    return (int(got.group(1)), int(got.group(2))) if got else None


def oceanbase_value_bound(facts):
    """Which bound holds OceanBase's widest value, by the version the
    tenant gave (`facts["version"]`, `ob_version`): the documented one of
    that version, or none where the version was not read - the packet
    read off the server then bounds a value alone, never a guessed one."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    have = f.get("version")
    rungs = [Rung(f"v{since}", f"{most // 2 ** 20} MB a value, as OceanBase"
                                f" {since}.x and later documents it" if since
                  else f"{most // 2 ** 20} MB a value, as OceanBase before"
                       " 4.x documents it",
                  because=f"the tenant says it runs version {have}",
                  needs=(Need(f"version {since} or later",
                              lambda x, s=since: x.get("version") is not None
                              and tuple(x["version"]) >= (s, 0),
                              f"the tenant's version is before {since}"),))
             for since, most in OCEANBASE_LOB]
    rungs.append(Rung("unread", "no bound of the service's own: the packet"
                                " read off the server bounds a value",
                      because="the tenant's version was not read"))
    return climb("OceanBase's widest value", tuple(rungs), f)


def oceanbase_value_most(choice):
    """The bytes of the bound `oceanbase_value_bound` stood on, or None."""
    name = getattr(choice.rung, "name", "")
    return dict((f"v{s}", m) for s, m in OCEANBASE_LOB).get(name)


def gauss_partition_catalogue(facts):
    """Where a GaussDB side keeps its partitions: its own `pg_partition`
    (`PARTITION BY` as GaussDB and openGauss write it), PostgreSQL's
    declarative one (`pg_partitioned_table`, `pg_inherits`) where the
    server is built on a release that has it, read off the catalogue
    itself (`facts["pg_partition"]`, `facts["pg_partitioned_table"]`);
    none where neither is there."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    return climb("GaussDB's partition catalogue", (
        Rung("own", "GaussDB's own partitions (pg_partition)",
             because="the catalogue has pg_partition",
             needs=(Need("pg_partition in the catalogue",
                         lambda x: bool(x.get("pg_partition")),
                         "the catalogue has no pg_partition"),)),
        Rung("declarative", "PostgreSQL's declarative partitions"
                            " (pg_partitioned_table)",
             because="the catalogue has pg_partitioned_table",
             needs=(Need("pg_partitioned_table in the catalogue",
                         lambda x: bool(x.get("pg_partitioned_table")),
                         "the catalogue has no pg_partitioned_table"),))), f)


# the cells the services above now hold: large values by their parent's
# adapter under the bound each service reads or documents, partitions read
# from the service's own catalogue, and what a service's own quota makes
# whole said as such; run on fakes only - the engine's module says on which
# server it was not run (`UNPROVED_ON`)
WAYS["large values"].update({
    **{n: ("yes", "@LARGE_VALUES") for n in (
        "oceanbase", "oceanbase-oracle", "polardbx", "dcdb", "gaussdb",
        "geminidb-redis", "geminidb-mongo")},
    "keyspaces": (NOT_APPLICABLE, "a row is 1 MB at most, Keyspaces' own"
                                  " quota, written whole as one mutation"),
    "geminidb-cassandra": (NOT_APPLICABLE, "a row is written as one"
                                           " mutation, as on Cassandra's"
                                           " wire, and read whole"),
})
WAYS["partitioned"].update({
    "gaussdb": ("yes", "@_deep_partitions"),
    "oceanbase-oracle": ("yes", "@_deep_partitions"),
})


# ---- the log readers no sandbox here runs (backlog LT.R6, LT.R2, LT.R3,
# ---- LT.C5): each a climb over what its server says ---------------------

def db2_log_way(facts):
    """How a Db2 source's changes are read from its own log
    (`engines.db2_log`). `facts`: `library` (the data server client's
    log-read API loads on this machine), `archived` (the database keeps
    its log past a restart point: archive logging, not circular),
    `captured` and `tables` (of the tables in scope, how many log their
    whole row: `SYSCAT.TABLES.DATACAPTURE` other than 'N'). A table that
    logs its whole row hands its change over from the log; one that logs
    only the bytes an update changed names the row by its record id, and
    the row is read from the source as it is now - which the later
    changes of it, also in the log, converge on."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    base = (
        Need("the log-read API on this machine",
             lambda x: bool(x.get("library")),
             "the source's log cannot be read from this machine: install"
             " the database's data server client (its runtime, with the"
             " log-read API) here, or follow the source another way"),
        Need("a log kept past its restart point",
             lambda x: bool(x.get("archived")),
             "the source's log is circular, so a record is overwritten"
             " once its transaction ends and a reader that falls behind"
             " loses changes: set the database's log archiving on"
             " (LOGARCHMETH1) and take the backup it then asks for"))
    said = (f"{f.get('captured', 0)} of the {f.get('tables', 0)} tables in"
            " scope log their whole row")
    return climb("how a Db2 source's changes are read", (
        Rung("images", "every row change read whole from the log",
             needs=base + (Need(
                 "every table logging its whole row",
                 lambda x: x.get("tables", 0) > 0
                 and x.get("captured", 0) >= x.get("tables", 0),
                 lambda x: f"only {said}"),),
             because=said + ": the log holds each row before and after"),
        Rung("by record", "each changed row named by its record id in the"
             " log, read from the source as it is now", footprint=1,
             needs=base + (Need("a table in scope",
                                lambda x: x.get("tables", 0) > 0,
                                "no table is in scope"),),
             because=said + "; an update of the rest logs only the bytes"
                     " it changed, so the row is read by its record id")),
        f)


def mssql_log_way(facts, costs=None):
    """How a SQL Server source is followed without Change Tracking or
    Change Data Capture (`engines.mssql_log`): its live log read where its
    account may, or its log backups restored, one after the other, onto a
    server migkit owns and read there. `facts`: `live` (the log answered
    the account), `recovery` (the database's recovery model), `standby`
    (the hop names a server migkit owns to restore onto), `chain` (the log
    backups since the position follow on, none missing), and `costs`
    the lag each way was measured at here."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    said = f"the recovery model is {f.get('recovery') or 'unknown'}"
    return climb("how a SQL Server source's log is followed", (
        Rung("live log", "the source's live log, read as it is written",
             needs=(Need("the log readable by the account",
                         lambda x: bool(x.get("live")),
                         "the source's account may not read its live log"
                         " (it needs the server's administrator role)"),),
             because=said + ": the live log names every row change as it"
                     " commits; a position the log has since let go"
                     " stops the tail, named"),
        Rung("log backups", "each log backup restored onto a server"
             " migkit owns, read there", footprint=1,
             needs=(Need("log backups taken",
                         lambda x: str(x.get("recovery") or "").upper()
                         in ("FULL", "BULK_LOGGED"),
                         lambda x: f"{said}, which takes no log backups"),
                    Need("a server migkit owns",
                         lambda x: bool(x.get("standby")),
                         "the hop names no server migkit owns to restore"
                         " the log backups onto (log_standby)"),
                    Need("an unbroken chain", lambda x: x.get("chain")
                         is not False,
                         "a log backup since the position is missing: the"
                         " chain is broken, so the changes in the gap"
                         " cannot be read")),
             because=said + ": the source only takes its log backups, as"
                     " it does anyway; the lag is a backup's interval",
             leaves="a database in standby on the server migkit owns")),
        f, costs=costs or None, prove=False)


def oracle_reader_rungs():
    """The ways an Oracle source's redo is read (`oracle_redo.choice`):
    the redo-file reader beside the redo, then LogMiner - which holds
    unless the rate it mined here is below the source's busiest hour of
    redo (`oracle_logminer.reader_check` measures both), when it would
    fall behind and the climb says so rather than start a tail that never
    catches up."""
    def behind(f):
        m = f.get("rates")
        return bool(m) and m[1] < m[0]

    return (
        Rung("redo-files", "the redo-file reader, beside the redo",
             needs=(Need("to run beside the source's redo",
                         lambda f: f["listening"] or (
                             f["platform"] and f["program"]
                             and f["redo here"]),
                         "{rung} is not run beside this source's redo: the"
                         " source names no redo_reader, and this machine"
                         " cannot read its redo files with the program"),),
             because="it reads the redo files themselves, so the database"
                     " does no mining for it",
             footprint=1),
        Rung("logminer", "migkit's LogMiner reader",
             needs=(Need("mining as fast as the source writes redo",
                         lambda f: not behind(f),
                         lambda f: "LogMiner mined"
                         f" {f['rates'][1] / 2 ** 20:.2f} MB of redo a second"
                         " here, under the"
                         f" {f['rates'][0] / 2 ** 20:.2f} MB a second the"
                         " source wrote at its busiest hour: it would fall"
                         " behind - run the redo-file reader beside the"
                         " redo and name it (redo_reader: host:port)"),),
             because="in every edition, on a managed database, with"
                     " nothing installed"))


# ---- whether a table's rows are written in pieces (W16N-LOB) --------------

WORDS["written in pieces"] = ("a value too large for one statement on the"
                              " target is written in pieces, which {rung}"
                              " does not do")


def written_in_pieces(facts):
    """(whether a table goes to the copier that writes a value too large
    for one statement in pieces, `Choice`). `facts`: {"packet": the
    target's max_allowed_packet as read off it, "widest": the table's
    widest row's large columns in bytes, "exact": whether that is every
    row's maximum read to the end or a bound no row can pass - never a
    sample, which bounds nothing - and "margin": what the rest of a
    statement takes}. A loader escapes a binary value, which can double
    it, so a row past half the packet is not sent whole either. Where the
    packet or the widest row is not known the copier that writes in
    pieces is taken: it carries a value of any size."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    margin = int(f.get("margin") or 0)

    def fits(x):
        return (x.get("widest") is not None and bool(x.get("packet"))
                and 2 * int(x["widest"]) + margin <= int(x["packet"]))
    said = (f"the target's packet {int(f.get('packet') or 0):,} bytes; the"
            " widest row's large columns "
            + (f"{int(f.get('widest')):,} bytes" if f.get("widest")
               is not None else "not known"))
    got = climb("how a table's widest row reaches the target", (
        Rung("whole", "every row sent whole, one statement a batch",
             because=said, needs=(
                 Need("the target's packet read",
                      lambda x: bool(x.get("packet")),
                      "the target's max_allowed_packet could not be read"),
                 Need("the widest row read to the end",
                      lambda x: bool(x.get("exact"))
                      and x.get("widest") is not None,
                      "the widest row was not read to the end within the"
                      " scan's time, and a sample bounds nothing"),
                 Need("the widest row within the packet once escaped", fits,
                      lambda x: f"its widest row, {int(x['widest']):,}"
                      " bytes, may double once escaped, past the target's"
                      f" max_allowed_packet of {int(x['packet']):,}"))),
        Rung("pieces", "a value too large for one statement written in"
             " pieces, inside its row's transaction", because=said)), f)
    return got.path == "pieces", got


# ---- a dump a killed run left, and how a column is hidden (backlog R17b.prog,
# ---- W4.b) ------------------------------------------------------------------
def leftover_dump(facts):
    """The `Choice` for a dump directory a run left (`leftovers.dumps`):
    remove it, or leave it. Read from `owner gone` (True where the process
    recorded beside it has died, False where it still runs - it may be
    writing now - None where no owner is recorded), `age` (seconds since
    it was last written) and `longest step` (seconds the longest step of
    the hop's last run took, None where none was kept). A dump with no
    recorded owner is removed only where it is older than any step the run
    took; one whose owner still runs is never removed here."""
    f = facts if isinstance(facts, Facts) else Facts(facts)

    def stale(f):
        age, step = f.get("age"), f.get("longest step")
        return age is not None and step is not None and age > step
    return climb("a dump directory left behind", (
        Rung("remove", "removed with everything in it", needs=(
            Need("its owner gone",
                 lambda f: f.get("owner gone") is True or (
                     f.get("owner gone") is None and stale(f)),
                 lambda f: "the run that made it still runs and may be"
                 " writing" if f.get("owner gone") is False else
                 "no run is recorded as its owner, and it is not older than"
                 " the longest step a run took"
                 if f.get("longest step") is not None else
                 "no run is recorded as its owner and none was kept to tell"
                 " a dump left behind from one in use"),),
             because="nothing that made it is left to use it"),
        Rung("keep", "left as it is", footprint=1,
             because="the run that made it may still be using it")), f)


def hide_column(facts):
    """The `Choice` for how a column's values are hidden: `fpe` (encrypted
    in their own shape), `token` (a stand-in kept in the user's vault) or
    `hash` (a keyed digest, no way back). Read from `kind` (`digits`,
    `alphabet` for a text in a known set, `text`, `other`), `alphabet` (the
    set's name or characters), `length` (the longest value, in
    characters), `shape` (a reader of the value needs its shape and
    length), `unique` (the column must stay unique), `referenced` (keys
    that refer to it, or that it refers to: the same value must come out
    wherever it stands) and `reversible` (the holder of the key must be
    able to get the value back).

    A value whose shape is read is encrypted in it where its set is known
    and its length leaves a domain big enough to hold (`transforms.
    LEAST_DOMAIN`); a value that must come back, or stay unique or
    joined, and cannot be encrypted in its shape is tokenised; one that
    needs nothing of the three is hashed, which keeps nothing to reverse."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    from . import transforms
    chars = None
    if f.get("kind") in ("digits", "alphabet"):
        try:
            chars = transforms.alphabet(
                "digits" if f.get("kind") == "digits" else f.get("alphabet"))
        except SystemExit:
            chars = None
    said = (f"a {f.get('kind') or 'column of unknown type'}"
            + (", unique" if f.get("unique") else "")
            + (f", {int(f.get('referenced'))} key(s) joined on it"
               if f.get("referenced") else "")
            + (", to come back" if f.get("reversible") else "")
            + (", read by its shape" if f.get("shape") else ""))

    def room(x):
        n = int(x.get("length") or 0)
        return chars is not None and len(chars) ** n \
            >= transforms.LEAST_DOMAIN
    return climb("how a column's values are hidden", (
        Rung("fpe", "encrypted in their own shape and length", because=said,
             needs=(
                 Need("a value read by its shape, or to be unique, joined"
                      " or restored",
                      lambda x: bool(x.get("shape") or x.get("unique")
                                     or x.get("referenced")
                                     or x.get("reversible")),
                      "nothing reads the shape or needs the value back: a"
                      " hash is enough"),
                 Need("a known set of characters", lambda x: chars is not None,
                      "the values are not digits or a known set of"
                      " characters"),
                 Need("a domain big enough to hold", room,
                      lambda x: f"values of {int(x.get('length') or 0)}"
                      " characters leave fewer than"
                      f" {transforms.LEAST_DOMAIN:,} possible values"))),
        Rung("token", "replaced by a token kept in a local vault",
             because=said, needs=(
                 Need("a value to be unique, joined or restored, and not"
                      " read by its shape",
                      lambda x: not x.get("shape") and bool(
                          x.get("unique") or x.get("referenced")
                          or x.get("reversible")),
                      lambda x: "its shape is read and the encryption in"
                      " its shape cannot hold: nothing else keeps it"
                      if x.get("shape") else
                      "the value need not come back, stay unique or join:"
                      " a hash is enough"),)),
        Rung("hash", "a keyed digest, with no way back", because=said)), f)


# --- whether a side's provider is asked what it holds of it (W17F2) ---------

def provider_asked(facts):
    """`Choice` of whether the provider's own API is asked what it holds of
    a side - the brand it confirms, a cluster parameter the plan needs -
    from `managed` (the side is a managed service a provider's API holds),
    `egress` (the egress lets calls to the provider's API out) and
    `refused` (the setting's words where it does not). No rung: the facts
    stay unknown and what reads them says so; the confidence they would
    raise stays where the server's own signs put it."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    return climb("whether the provider's own API is asked of the side", (
        Rung("ask", "the provider's own API asked once, within the"
             " provider client's own timeouts",
             needs=(Need("the side is a managed service",
                         lambda x: x.get("managed") is True,
                         "the side is run by its owner: no provider's API"
                         " holds it"),
                    Need("the egress lets the call out",
                         lambda x: x.get("egress") is True,
                         lambda x: x.get("refused") or "the egress does not"
                         " let calls to the provider's own API out")),
             because="a fact the provider holds of the side, stronger than"
                     " any sign the server gives"),), f)

def slice_recheck(facts):
    """Whether a consolidation's slice verify (`consolidate.slice_verify`)
    compares its sources' slices again: only where a difference is not yet
    agreed by two rounds a caught-up follow stands between, a follow is
    there to catch the target up, and the round before left fewer of them
    than the one before it - differences that do not fall are writes
    outrunning the follow, said as found, never compared without end.
    Else the slices' verdict stands as compared."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("a slice verify's next round", (
        Rung("recheck", "the slices compared again past a caught-up follow",
             needs=(
                 Need("a difference two rounds have not agreed on",
                      lambda x: x["unsettled"] > 0,
                      "every difference found was found alike past a"
                      " caught-up follow"),
                 Need("a follow to catch the target up",
                      lambda x: x["catch_up"] is True,
                      "no follow is there to catch the target up: the"
                      " differences stand as found at the positions read"),
                 Need("differences falling round by round",
                      lambda x: x["before"] is None
                      or x["unsettled"] < x["before"],
                      lambda x: f"{x['unsettled']:,} differences after a"
                                f" round that left {x['before']:,}: writes"
                                " outrun the follow, said as found")),
             because="a difference read while the follow was behind its"
                     " source is compared again once it has caught up"),
        Rung("settle", "the slices' verdict as compared",
             because="every difference is agreed past a caught-up follow")),
        f)



#: the rung of migkit's own move among the providers' operations
MANAGED_OWN_MOVE = "migkit-move"


def managed_against_own(choice, costs):
    """The provider operation `choice` (`scenarios.managed_case`) ranked
    against migkit's own move and its full verify by their durations in
    `costs` ({name: costmodel.Estimate}: the operation's measured or
    documented, migkit's forecast under `MANAGED_OWN_MOVE`). The operation
    stands only where one is eligible and both durations are known and it
    is not the slower; migkit's own move holds wherever it does not."""
    op, mine = costs.get(choice.path), costs.get(MANAGED_OWN_MOVE)
    return climb("a provider operation or migkit's own move", (
        Rung(choice.path or "provider-operation",
             choice.rung.said if choice.rung else "a provider operation",
             needs=(
                 Need("an eligible provider operation",
                      lambda f: f["path"] is not None,
                      "no provider operation is eligible on the facts read"),
                 Need("migkit's own move forecast for the data",
                      lambda f: f["mine"] is not None,
                      "migkit's own move has no forecast for the data, so"
                      " no operation is ranked above it"),
                 Need("the operation's duration no longer than migkit's"
                      " forecast",
                      lambda f: f["op"] is not None and f["mine"] is not None
                      and f["op"].mean <= f["mine"].mean,
                      lambda f: f"the operation's {f['op'].said()} is longer"
                                f" than migkit's own {f['mine'].said()}"
                      if f["op"] is not None and f["mine"] is not None
                      else "the operation's duration is not known")),
             because="the provider's operation is eligible and no slower"
                     " than migkit's own move by the durations read"),
        Rung(MANAGED_OWN_MOVE, "migkit's own move and its full verify",
             because="the first way that holds")),
        Facts({"path": choice.path, "op": op, "mine": mine}))

# ---- how a SQL Server source is followed (MSSQL-x86, W17E) ---------------

def mssql_follow_way(facts, costs=None):
    """How a SQL Server source's changes are followed: through Change Data
    Capture, through Change Tracking, or through its own log
    (`engines.mssql_log`). `facts`: `capture` (the database captures
    changes: `sys.databases.is_cdc_enabled`), `capturing` (what fills the
    change tables runs: the server's Agent is running, or the service runs
    the capture itself), `captured` and `tables` (of the tables in scope,
    how many have a capture instance; how many there are), `tracking`
    (the database tracks changes), `tracked` (how many of the tables in
    scope are tracked), and `log` (the source's own log can be followed
    here: `mssql_log.follow_way` holds). None of them is assumed: a fact
    the server would not give holds no rung.

    Capture is ranked first where it holds: it keeps every change of a
    row, its delete and the order of the changes inside a transaction,
    where tracking keeps only the last operation of each row, read as the
    row is now. Neither is a default: each is taken on what the server
    says, and a database switched from one to the other is followed the
    other way at the next start, from the position its own token names."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def every(have):
        return lambda x: (x.get("tables") or 0) > 0 and (
            x.get(have) or 0) >= (x.get("tables") or 0)
    scope = f"{f.get('tables') or 0} tables in scope"
    return climb("how a SQL Server source's changes are followed", (
        Rung("capture", "Change Data Capture: every change of each row, in"
             " the order it committed",
             needs=(Need("the database capturing changes",
                         lambda x: bool(x.get("capture")),
                         "Change Data Capture is off for the database"
                         " (exec sys.sp_cdc_enable_db, then"
                         " sys.sp_cdc_enable_table for each table)"),
                    Need("what fills the change tables running",
                         lambda x: bool(x.get("capturing")),
                         "the server's Agent is not running, so nothing"
                         " fills the change tables: start it, or follow"
                         " another way"),
                    Need("every table in scope captured", every("captured"),
                         lambda x: f"only {x.get('captured') or 0} of the"
                         f" {scope} have a capture instance")),
             because=scope + ": the change tables hold each change with its"
                     " commit's position, before and after an update"),
        Rung("tracking", "Change Tracking: the last change of each row,"
             " read as the row is now", footprint=1,
             needs=(Need("the database tracking changes",
                         lambda x: bool(x.get("tracking")),
                         "Change Tracking is off for the database"
                         " (alter database ... set change_tracking = on)"),
                    Need("every table in scope tracked", every("tracked"),
                         lambda x: f"only {x.get('tracked') or 0} of the"
                         f" {scope} are tracked (alter table ... enable"
                         " change_tracking)")),
             because=scope + ": a tail applied by key converges on the row"
                     " as it is now"),
        Rung("log", "the source's own log, or its log backups restored",
             footprint=2,
             needs=(Need("the log followable here",
                         lambda x: bool(x.get("log")),
                         "the source's log can be followed no way here"),),
             because="neither capture nor tracking is on; the log names"
                     " every row change as it commits")),
        f, costs=costs or None, prove=False)


def anchor_interval(rate, put_seconds, span):
    """(entries, said) between two anchors of the audit record, taken from
    what was measured of it: `rate` entries a second over `span` seconds
    of the record, and the seconds one anchor took to put (`put_seconds`).
    Anchoring more often leaves less of the record unanchored but spends
    more of the run on puts; the sum of the two over a run of `span`
    seconds is least when the stretch left unanchored is
    sqrt(2 x put x span) seconds (the interval a checkpoint is taken at
    when its cost and the run's length are known), and that stretch at
    the measured rate is the number of entries. Where nothing was
    measured - no rate yet, no anchor put yet - every entry is anchored:
    the interval is never longer than what is known to be safe."""
    if not rate or rate <= 0 or not put_seconds or put_seconds <= 0 \
            or not span or span <= 0:
        return 1, ("every entry: the record's rate or the time an anchor"
                   " takes has not been measured yet")
    stretch = (2 * put_seconds * span) ** 0.5
    n = max(1, int(round(rate * stretch)))
    return n, (f"every {n} entries: {rate:.3g} entries a second over"
               f" {span:.0f}s, an anchor taking {put_seconds:.3g}s, so"
               f" about {stretch:.3g}s of the record is left unanchored")

def estate_eta(facts, costs=None):
    """(the `Choice` of what a server's move in the estate is sized from,
    (low, high) seconds or None) - `migkit estate` (backlog W2).

    `facts`: `size` (the bytes its databases hold, as the server said
    once signed in; None where nobody signed in), `pair` (the kept runs of
    this engine pair on every hop: {"bytes", "seconds"} each, the ledger
    `planner.record_rate` writes) and `engine` (the runs of the same
    source engine to any other target). The bounds are the size over the
    fastest and the slowest rate those runs measured - never a rate of a
    table: where no run of the source's engine was kept, "no history" is
    the answer and no number is said."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def rated(key):
        return [r["bytes"] / r["seconds"] for r in f.get(key) or ()
                if r.get("bytes") and r.get("seconds")]

    def sized(x):
        return x.get("size") is not None

    size = Need("the size the server said", sized,
                "nobody signed in to read the size, so no time is said")
    choice = climb("what a server's move in the estate is sized from", (
        Rung("pair", "the measured runs of this engine pair",
             needs=(size, Need("a measured run of this pair",
                               lambda x: bool(rated("pair")),
                               "no run of this engine pair was kept")),
             because="the same pair's own copies: what this move does"),
        Rung("engine", "the measured runs of the source's engine to"
             " another target", footprint=1,
             needs=(size, Need("a measured run from this engine",
                               lambda x: bool(rated("engine")),
                               "no run from this engine was kept")),
             because="the source's reads are the same; the target's"
                     " writes are another engine's"),
        Rung("none", "no history", footprint=2,
             because="nothing measured: no time is said")),
        f, costs=costs or None, prove=False)
    rates = rated(choice.path) if choice.path in ("pair", "engine") else []
    if not rates:
        return choice, None
    return choice, (f.get("size") / max(rates), f.get("size") / min(rates))


def estate_price(facts, costs=None):
    """(the `Choice` of how a server's move in the estate is priced, the
    cheapest `machine.Advice` at each end of its time or None) - backlog
    W2. `facts`: `bounds` (`estate_eta`'s seconds), `size`, `stages`
    ((fastest, slowest) `machine.Stages` from the measured runs:
    `machine.from_history`) and `types` ({(cloud, region): [Machine]}, the
    catalog's priced types). The price is the catalog's price a second
    times the time the measured rate takes on each type
    (`machine.advise`); without a measured time there is none."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    choice = climb("how a server's move in the estate is priced", (
        Rung("catalog", "the catalog's prices over the measured time",
             needs=(Need("a measured time", lambda x: bool(x.get("bounds")),
                         "no measured time, so no price"),
                    Need("a priced type in the catalog",
                         lambda x: any(x.get("types", {}).values()),
                         "no type in the catalog has a price there"))),
        Rung("none", "not priced", footprint=1,
             because="a price without a measured time would be a guess")),
        f, costs=costs or None, prove=False)
    if choice.path != "catalog":
        return choice, None
    from . import machine
    ends = []
    for stages in f.get("stages"):
        best = None
        for (_, region), types in sorted(f.get("types").items()):
            got = machine.advise(stages, f.get("size"), None, types, region)
            if got.cost is not None and (best is None or got.cost < best[0]
                                         .cost):
                best = (got, region)
        ends.append(best)
    return choice, tuple(ends)

# --- where a hop's transform statement runs (W18C, W4.b) ---------------------

def transform_place(facts, costs=None):
    """`Choice` of where a hop's `transform:` statement runs for a table:
    `in_flight` (over each batch between the read and the write, once a
    batch) or `on_target` (the copy writes the values as read and the
    target's own SQL rewrites them after it) - or no rung, which refuses
    the table before anything is written. Read from `statement` (its
    words), `measured` (it ran over the first batches), `in_flight_s`
    (its seconds a row there, for the table's `rows`), `copy_s` (the
    copy's own measured seconds for the table, which a rewrite on the
    target costs again: every row written once more), `budget_s` (the
    time the hop allows the table, where it says one), `target_runs` (the
    target's SQL says the statement) and `authorized` (the hop lets the
    target be changed after the copy, holding the values as read
    meanwhile).

    In flight holds while the statement keeps up: within what the budget
    leaves after the copy, or - with no budget - within the copy's own
    time, so the copy is not slowed past twice its measured length. The
    costs rank what holds; nothing is chosen from a default."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def room(x):
        copy = float(x.get("copy_s") or 0)
        budget = x.get("budget_s")
        return (float(budget) - copy) if budget is not None else copy

    def pace(x):
        return float(x.get("in_flight_s") or 0) <= room(x)
    said = (f"`{f.get('statement') or '(no statement)'}` measured over"
            f" {int(f.get('sampled') or 0):,} rows of the first batches:"
            f" {float(f.get('in_flight_s') or 0):,.1f}s for"
            f" {int(f.get('rows') or 0):,} rows beside the copy's"
            f" {float(f.get('copy_s') or 0):,.1f}s")
    return climb("where the hop's transform runs", (
        Rung("in_flight", "over each batch as it passes, once a batch",
             because=said, needs=(
                 Need("the statement run over the first batches",
                      lambda x: bool(x.get("measured")),
                      "it was not run over any of the source's rows yet:"
                      " nothing is measured to place it by"),
                 Need("the statement keeping up with the copy", pace,
                      lambda x: f"{float(x.get('in_flight_s') or 0):,.1f}s"
                      f" in flight against {room(x):,.1f}s "
                      + ("the budget leaves after the copy"
                         if x.get("budget_s") is not None else
                         "the copy itself takes")))),
        Rung("on_target", "on the target by its own SQL after the copy",
             footprint=1, because=said, needs=(
                 Need("the target's SQL saying the statement",
                      lambda x: bool(x.get("target_runs")),
                      "the target's SQL has no form of the statement"),
                 Need("the hop letting the target be changed after the copy",
                      lambda x: bool(x.get("authorized")),
                      "the hop does not let the target be changed after the"
                      " copy (transform: on_target: true), which holds the"
                      " values as read there until it is rewritten")))),
        f, costs=costs or None, prove=False)

# ---- the change-topic engines' bulk ways (ER4, ER5) ------------------------
#
# Each is a table's ladder (`tableway.each`): the engine's own bulk way
# where its facts hold, the table copier always after it, so a table no
# bulk way may take is still carried, and a bulk way that fails on a table
# is undone (`cleanup`) before the copier starts it again from nothing.
# Built on fakes only; each engine's module says on which server they
# were not run (`UNPROVED_ON`).

def _empty_target(word):
    return Need("the target's table empty",
                lambda f: f.get("empty") is True,
                lambda f: f"the target's table holds rows: {word} only adds,"
                          " and a row it holds already would stop it")


def _bulk_copier(said="the table copier, by key ranges"):
    return Rung("copier", said, native_copier=True,
                because="every row read and written as the neutral contract"
                        " reads it, resumable by its key")


def crdb_bulk_ways(cleanup=None):
    """A CockroachDB table: the source's `EXPORT` into the stage and the
    target's `IMPORT INTO` from it, where a stage both clusters reach is
    given, the source is CockroachDB, the table has a key (a keyless one's
    hidden row ids are no other cluster's) and the target's is empty; else
    the table copier."""
    return (
        Rung("import", "the clusters' own export into the stage and"
             " import from it",
             needs=(Need("a stage", lambda f: bool(f.get("stage")),
                         "no stage is given (the target's `stage`: a store"
                         " both clusters reach)"),
                    Need("a CockroachDB source", lambda f: bool(f.get("same")),
                         "the source is not CockroachDB: it cannot export"
                         " into the stage"),
                    Need("a key", lambda f: bool(f.get("key")),
                         "the table has no key: its hidden row ids are no"
                         " other cluster's"),
                    _empty_target("an import")),
             because="every value in the clusters' own form and no row"
                     " through migkit; an import that fails is taken back"
                     " by the cluster itself",
             leaves="the exported files in the stage", cleanup=cleanup),
        _bulk_copier())


def tidb_bulk_ways(cleanup=None):
    """A TiDB table: the whole source read in one pass into the server's
    own bulk load (`load data local`, pinned to the rows sent) where the
    target's table is empty; else the table copier. TiDB's own importer is
    no rung: it is a program run beside the cluster reading files on its
    hosts, not a client's way."""
    return (
        Rung("load", "the server's own bulk load, the source read in one"
             " pass", needs=(_empty_target("a load"),),
             because="a batch a statement parsed by the server in bulk, and"
                     " no key range read twice",
             leaves="nothing", cleanup=cleanup),
        _bulk_copier())


def couchbase_bulk_ways(cleanup=None):
    """A Couchbase collection: the server's own bulk importer, from the
    documents written as JSON lines, where it is installed; else the SDK's
    batched upserts by key ranges. An upsert lands on itself, so a load
    that failed half way is written again over, never doubled."""
    return (
        Rung("loader", "the server's own bulk importer, from the documents"
             " as JSON lines",
             needs=(Need("its programs installed",
                         lambda f: bool(f.get("loader")),
                         programs=("cbimport",)),),
             because="the documents sent by the importer's own parallel"
                     " writers, every vBucket at once",
             leaves="the documents' file, removed when it ends",
             external=True, cleanup=cleanup),
        _bulk_copier("the SDK's batched upserts, by key ranges"))


def cosmos_bulk_ways(cleanup=None):
    """A Cosmos DB container: the source read in pages cut by bytes and
    each page's items written as the service's bulk writes - every
    partition key value's items in transactional batches at once - where
    the target's partition key is read; else the table copier."""
    return (
        Rung("bulk", "the service's bulk writes, every partition key's items"
             " in batches at once",
             needs=(Need("the target's partition key read",
                         lambda f: bool(f.get("pk")),
                         "the target container's partition key cannot be"
                         " read"),),
             because="an item written by its id and partition key lands on"
                     " itself: a page written again replaces, never doubles",
             leaves="nothing", cleanup=cleanup),
        _bulk_copier())


# large values on CockroachDB and DSQL: their PostgreSQL-wire expressions
# read a value past what a row carries in pieces by its key, whole, never
# under a sampled maximum (`pieces.ReadPlan`); run on fakes only
WAYS["large values"].update({
    "cockroachdb": ("yes", "@LARGE_VALUES"),
    "dsql": ("yes", "@LARGE_VALUES"),
})

# ---- a reshard's follow: how much of a source is read, and in what order (W7)

def reshard_read(facts):
    """(the `Choice` of how many changes a reshard's follow reads of one
    source next, that number). `facts`: `rate` (changes a second its last
    batch was read and applied onto the targets at, measured; None before
    any), `before` (the rate measured at the read before it), `round`
    (seconds the last round over every source took, measured), `last`
    (what its last read asked for), `full` (the last read came back as
    full as asked: the source is behind), `first` (the reader's own first
    read, taken only where nothing was read).

    Behind and gaining - the last larger read applied faster a change than
    the one before it, or nothing before to compare - it is read twice as
    much, and stops growing the round a larger read gains nothing. Measured
    on a full read and not gaining, it is read as much as the targets apply
    in one round's time: a busy source not held to the others' batches,
    nor the others waiting behind its own. Not behind: what it was read by
    last (the reader's first read before any), a bound the changes fill
    as they come."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def measured(x):
        # a rate read off a short read is the batch's overhead, not what
        # the targets take: only a full read measures it
        return bool(x.get("full")) and (x.get("rate") or 0) > 0 and (
            x.get("round") or 0) > 0

    def gaining(x):
        return x.get("before") is None or (x.get("rate") or 0) > (
            x.get("before") or 0)
    choice = climb("how many changes a source of a reshard is read next", (
        Rung("grow", "twice the last read", needs=(
            Need("the last read full", lambda x: bool(x.get("full"))
                 and (x.get("last") or 0) > 0,
                 "its last read came back short: the source is not"
                 " behind"),
            Need("a larger read gaining", gaining,
                 lambda x: f"the last read applied {x.get('rate') or 0:,.0f}"
                           " changes a second, no more than the"
                           f" {x.get('before') or 0:,.0f} of the smaller one"
                           " before it")),
             because="the source is behind and a larger read was applied"
                     " faster a change"),
        Rung("paced", "the changes the targets apply in one round's time",
             footprint=1, needs=(
                 Need("a rate measured on a full read", measured,
                      "no full read of the source has been applied and"
                      " timed yet"),),
             because="read as much as its measured rate applies while the"
                     " round over every source runs once"),
        Rung("as-asked", "what it was read by last, or the reader's first"
             " read", footprint=2,
             because="the source is not behind: a read is a bound, taken"
                     " as the changes come")), f)
    if choice.path == "grow":
        size = 2 * int(f["last"])
    elif choice.path == "paced":
        size = math.ceil(float(f["rate"]) * float(f["round"]))
    else:
        size = int(f.get("last") or f.get("first") or 1)
    return choice, max(1, size)


def reshard_order(facts):
    """(the `Choice` of the order a reshard's follow reads its sources in
    a round - and so drains them at the cutover - that order). `facts`:
    `took` [seconds each source's last batch took, measured; None where
    not yet], `full` [its last read came back full: behind]. Skewed and
    measured: the sources behind first, the longest batch first among
    them, so the source with the most still to drain starts first and the
    round ends soonest. Nothing measured: the order the group names
    them."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    took, full = list(f.get("took") or []), list(f.get("full") or [])
    n = len(took)
    choice = climb("the order a reshard's sources are read in", (
        Rung("most-behind-first", "the sources behind first, the longest"
             " batch first", needs=(
                 Need("every source's batch timed",
                      lambda x: n > 0 and all(t is not None for t in took),
                      "a source's batch has not been timed yet"),
                 Need("the sources skewed",
                      lambda x: len({(bool(full[i]) if i < len(full)
                                      else False, took[i])
                                     for i in range(n)}) > 1,
                      "every source is as far behind as the others")),
             because="the source with the most still to read starts first,"
                     " so the round over them all ends soonest"),
        Rung("as-named", "the order the group names them", footprint=1,
             because="nothing measured tells the sources apart")), f)
    if choice.path == "most-behind-first":
        order = sorted(range(n), key=lambda i: (
            not (full[i] if i < len(full) else False), -float(took[i]), i))
    else:
        order = list(range(n))
    return choice, order

# --- the warehouses' remaining ways, on what each account answers (W18G) ------

def warehouse_follow_way(facts):
    """How a warehouse source is followed.

    `facts`: `engine`, `tables` (in scope), `tracked` (of them, those that
    track their changes) and `untracked` (their names), `streams` (the
    account read a table's changes when asked - an edition or a setting
    that does not is `refused`, its words), `retention` (seconds of time
    travel the least-kept table holds: all the history a change read has)
    and `gap` (seconds the copy is forecast to take before the tail reads
    from where it began, where measured). A history shorter than the gap
    would be gone before it is read: refused before the copy, the two
    figures said."""
    f = facts if isinstance(facts, Facts) else Facts(facts)

    def hours(s):
        return f"{(s or 0) / 3600:.1f} hours"

    short = Need(
        "history kept past the copy",
        lambda x: x.get("retention") is None or x.get("gap") is None
        or x.get("retention") > x.get("gap"),
        lambda x: f"the tables keep {hours(x.get('retention'))} of change"
                  f" history and the copy is forecast at {hours(x.get('gap'))}:"
                  " where the tail would begin is forgotten before it reads"
                  " it - raise the tables' DATA_RETENTION_TIME_IN_DAYS past"
                  " the copy, or copy fewer tables at a time")
    return climb("following a warehouse source", (
        Rung("changes", "each table's changes read window by window, the"
             " rows as they stood at each window's end", needs=(
                 Need("an account that reads changes",
                      lambda x: bool(x.get("streams")),
                      lambda x: "the account did not read a table's"
                                f" changes ({x.get('refused') or 'refused'})"),
                 Need("every table tracking its changes",
                      lambda x: x.get("tracked", 0) >= x.get("tables", 0),
                      lambda x: f"{x.get('tables', 0) - x.get('tracked', 0)}"
                                " tables track no changes: alter table "
                                + ", ".join(list(x.get("untracked") or [])[:4])
                                + " set change_tracking = true, before the"
                                " copy a tail follows"),
                 short),
             because="the change read and time travel give each window's"
                     " net state, one snapshot of every table"),
        Rung("landing", "followed only where migkit lands the rows: at the"
             " landing's position", footprint=1, needs=(
                 Need("an account without change reads",
                      lambda x: not x.get("streams"),
                      "the account reads changes, so what keeps them from"
                      " being read is said by the first way"),),
             because="the account reads no table's changes: a table is"
                     " followed only as migkit's own landing writes it, by"
                     " the landing's position; a table others write cannot"
                     " be followed"),
        Rung("none", "not followed", footprint=2,
             because="what the account keeps cannot carry a follow from the"
                     " copy's start: see the first way's reason")),
        f, prove=False)


def warehouse_fence_way(facts):
    """How a warehouse holds a writer to its number in migkit's own table.

    `facts`: `engine`, `transactions` (the account runs a transaction of
    several statements: Snowflake's, BigQuery's multi-statement ones) and
    `isolation` (as the account answers it: Redshift runs `serializable`,
    or `snapshot isolation` where a database was made so). The hold is an
    update of the number's row inside the writer's own transaction."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("holding a warehouse writer to its number", (
        Rung("serializable", "the number's row updated in the writer's"
             " transaction, serializable", needs=(
                 Need("serializable isolation",
                      lambda x: str(x.get("isolation") or "").lower()
                      == "serializable",
                      lambda x: f"the database runs {x.get('isolation')}"
                                " isolation" if x.get("isolation") else
                                "the isolation was not answered"),),
             because="two transactions that both update the row cannot both"
                     " commit: a writer under an old number is aborted"),
        Rung("transaction", "the number's row updated in the writer's"
             " transaction, under the table's write lock", footprint=1,
             needs=(Need("transactions of several statements",
                         lambda x: bool(x.get("transactions")),
                         "the account runs no transaction of several"
                         " statements"),),
             because="the update locks the table until the writer commits,"
                     " so no raise passes it, and a row holding another"
                     " number refuses the writer"),
        Rung("none", "no writer held", footprint=2,
             because="nothing holds the number and the writes in one"
                     " transaction here: a standby is refused")),
        f, prove=False)


def warehouse_snapshot_way(facts):
    """How a warehouse target keeps a point a rollback returns to.

    `facts`: `travel_hours` (the least time travel the tables in scope
    keep) and `tables`. A clone at an instant inside time travel is the
    table as it stood then, made without a copy."""
    f = facts if isinstance(facts, Facts) else Facts(facts)
    return climb("a warehouse target's rollback point", (
        Rung("travel", "the instant kept: each table cloned back from it",
             needs=(Need("time travel on every table",
                         lambda x: (x.get("travel_hours") or 0) > 0,
                         "a table keeps no time travel"
                         " (DATA_RETENTION_TIME_IN_DAYS = 0)"),),
             because=f"every table keeps {f.get('travel_hours')} hours of"
                     " time travel: the instant is good that long"),
        Rung("counts", "each table's rows counted; no way back",
             footprint=1,
             because="a table keeps no time travel: the rows are counted,"
                     " and a rollback has nothing to clone back from")),
        f, prove=False)

# ---- the analytic and distributed services' own ways (P0-CLOUDS.more) ------

def mpp_spread(facts, costs=None):
    """How a table made on a service that spreads rows over its nodes is
    spread (AnalyticDB for PostgreSQL's `DISTRIBUTED`, AnalyticDB for
    MySQL's `DISTRIBUTED BY HASH`, a dedicated SQL pool's `DISTRIBUTION`,
    SingleStore's `SHARD KEY`). `facts`: `key` (the table's key columns,
    as the source says them), `rows` (the source's row count, as its
    catalogue or a count says it; None: not read), `nodes` (the target's
    data nodes or segments, as its catalogue says; None: not read),
    `replicated` (the service makes a table whose every node keeps all of
    it), `small` (the rows under which a whole copy on every node is the
    service's own advice; None: it gives none).

    A key spreads by its hash, so a lookup and a join by it stay on one
    node; a table the service advises keeping whole on every node is
    kept so only where both its size and that bound are read; a table
    with no key goes round the nodes evenly. None of them is taken
    without its facts."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    return climb("how a table is spread over the service's nodes", (
        Rung("replicate", "a whole copy on every node",
             needs=(Need("the service keeping a table whole on every node",
                         lambda x: bool(x.get("replicated")),
                         "the service keeps no table whole on every node"),
                    Need("the table's rows read and under the service's"
                         " bound for it",
                         lambda x: x.get("rows") is not None
                         and x.get("small") is not None
                         and x.get("rows") < x.get("small"),
                         lambda x: "the table's rows are not read" if
                         x.get("rows") is None else
                         "the service gives no bound for a table kept whole"
                         if x.get("small") is None else
                         f"{x.get('rows')} rows are over the"
                         f" {x.get('small')} the service advises")),
             because="a small table every node keeps whole joins with no"
                     " rows moved between nodes"),
        Rung("hash", "spread by the hash of its key",
             needs=(Need("a key", lambda x: bool(x.get("key")),
                         "the table has no key to spread it by"),),
             because="rows with one key land on one node, so a lookup or a"
                     " repair by key touches one node"),
        Rung("even", "spread round the nodes evenly", footprint=1,
             because="the table has no key: no column spreads it better"
                     " than an even spread")),
        f, costs=costs or None, prove=False)


def landing_load(facts, costs=None):
    """Whether a table is loaded through the target's own loader from a
    landing store (a dedicated SQL pool's `COPY INTO`, a SingleStore
    pipeline, an AnalyticDB external table over object storage) or by the
    table copier. `facts`: `landing` (the side names its landing store),
    `writer` (this machine can write that store's scheme), `may` (the
    target's account may run the loader: a pipeline, an external table,
    bulk operations - as the target's grants say; None: not read), `rows`
    (the table's rows; None: not read), `empty` (the target table holds
    no rows, so a repeat of an interrupted load cannot double them) and the two ways' costs where they
    were measured (`costs`). The loader is never assumed: each need is a
    fact the hop or the target answered, and where one does not hold the
    table copier takes the table and says why."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    return climb("how a table is loaded into the target", (
        Rung("landing", "the target's own loader from the landing store",
             needs=(Need("a landing store named",
                         lambda x: bool(x.get("landing")),
                         "no landing store is named for the target"
                         " (`stage:`)"),
                    Need("a writer for the landing store here",
                         lambda x: bool(x.get("writer")),
                         lambda x: f"nothing here writes {x.get('landing')}"),
                    Need("the target's account allowed the loader",
                         lambda x: x.get("may") is True,
                         lambda x: "the target's grants were not read" if
                         x.get("may") is None else "the target's account"
                         " may not run the service's loader"),
                    Need("rows to load", lambda x: (x.get("rows") or 0) > 0,
                         "the table holds no rows to land"),
                    Need("the target table empty",
                         lambda x: x.get("empty") is True,
                         "the target table holds rows already: a load"
                         " could write them twice, so they are reconciled"
                         " by key")),
             because="the service's nodes read the landed files in"
                     " parallel, where the table copier writes through one"
                     " front end"),
        Rung("copier", "the table copier, by key ranges, resumably",
             footprint=1, because="the first way that holds")),
        f, costs=costs or None, prove=False)


def service_follow(facts, costs=None):
    """How a source on one of these services is followed, or that no way
    holds and why. `facts`: `engine` (its name), `logical` (the service
    serves logical replication to a client: YugabyteDB from 2024.1.1 with
    its replication commands on), `slots` (replication slots still free;
    None: not read), `binlog` (AnalyticDB for MySQL: of the `tables` in
    scope, how many keep a binary log), `tables`. A service that keeps no
    change log a client reads has no rung: the hop is refused as a
    following source and said why, never followed by a guess."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    scope = f"{f.get('tables') or 0} tables in scope"
    return climb(f"how a {f.get('engine') or 'service'} source is followed", (
        Rung("logical", "the service's logical replication, from a slot",
             needs=(Need("logical replication served",
                         lambda x: bool(x.get("logical")),
                         lambda x: x.get("logical_why") or "the service"
                         " serves no logical replication to a client"),
                    Need("a replication slot free",
                         lambda x: (x.get("slots") or 0) > 0,
                         lambda x: "the free replication slots were not"
                         " read" if x.get("slots") is None else
                         "no replication slot is free")),
             because="the stream carries every committed change in commit"
                     " order, deletes too"),
        Rung("binlog", "each table's own binary log", footprint=1,
             needs=(Need("every table in scope keeping a binary log",
                         lambda x: (x.get("tables") or 0) > 0
                         and (x.get("binlog") or 0) >= x.get("tables"),
                         lambda x: f"only {x.get('binlog') or 0} of the"
                         f" {scope} keep a binary log (alter table ..."
                         " binlog=true)"),),
             because=scope + ": each table's binary log carries its rows'"
                     " changes in order")),
        f, costs=costs or None, prove=False)


# the five services' cells of the shapes, as their wire's contract has them
# (`distmysql.OnMySQLWire`, `pgwire.PgWireEngine`, the warehouse's): a way
# is copied only where it resolves on the service's own class
_SERVICE_MODEL = {"analyticdb-mysql": "polardbx", "singlestore": "polardbx",
                  "analyticdb-postgres": "gaussdb", "yugabytedb": "gaussdb",
                  "synapse": None}
for _shape, _ways in WAYS.items():
    for _engine, _model in _SERVICE_MODEL.items():
        if _engine in _ways:
            continue
        _got = _ways.get(_model) if _model else None
        if _got and _got[0] == "yes" and _got[1].startswith("@"):
            _ways[_engine] = _got
        elif _got and _got[0] == "yes" and _shape in ("read-only target",
                                                      "writes held"):
            _ways[_engine] = _got
        elif _shape in ("keyless", "no unique index", "wide key"):
            _ways[_engine] = ("yes", "@neutral_batches" if _shape != "wide key"
                              else "@neutral_read")
# what the wire's contract has and these services were not read for yet
for _engine, _shapes in {
        "analyticdb-postgres": ("large values", "partitioned"),
        "yugabytedb": ("large values", "partitioned"),
        "synapse": ("large values", "partitioned", "read-only target",
                    "writes held")}.items():
    for _shape in _shapes:
        WAYS[_shape][_engine] = (NOT_YET, "P0-CLOUDS.more")
del _shape, _ways, _engine, _model, _got, _shapes

# ---- a value wider than the target's packet, and the account a grant names
# ---- (backlog W16N-LOB, W16N-DEFECTS) ---------------------------------------
def packet_way(facts):
    """The `Choice` for a table whose widest row the target's packet may
    not carry: "kept" where the packet carries it as it is (whole, or in
    pieces each under the packet - `written_in_pieces`); "raised" where the
    run sets the target's packet to `want` for its length and puts `packet`
    back after, also after a killed run (`leftovers.settings_held`); else
    "refused" before the copy, with the statement the operator can run. No
    statement holds a value wider than the packet - appending past it fails
    on the server - so there is no third way that carries it. `facts`:
    {"widest": the widest row's large columns read to the end, "exact":
    whether it was, "packet": the target's packet, "slack": what a piece's
    statement takes beside its value, "want": the packet that carries the
    row, "ceiling": the most the server takes, "may set": whether the
    target's account may change the global setting, "authorized": the
    hop's answer for the setting (`target_settings`)}."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    widest = f.get("widest")
    said = (f"the widest row's large columns "
            + (f"{int(widest):,} bytes" if widest is not None
               else "not known")
            + f"; the target's packet {int(f.get('packet') or 0):,} bytes")
    read = Need("the widest row read to the end",
                lambda x: bool(x.get("exact")) and x.get("widest") is not None,
                "the widest row was not read to the end, and a sample"
                " bounds nothing")
    return climb("how a row wider than the target's packet arrives", (
        Rung("kept", "the target's packet as it is", because=said, needs=(
            read,
            Need("the widest row within the packet",
                 lambda x: bool(x.get("packet")) and int(x["widest"])
                 + int(x.get("slack") or 0) <= int(x["packet"]),
                 lambda x: f"{int(x['widest']):,} bytes is past the"
                 f" target's max_allowed_packet of"
                 f" {int(x.get('packet') or 0):,}"))),
        Rung("raised", "the target's packet raised for the run and put"
             " back after", because=said, needs=(
                 read,
                 Need("a packet the server takes",
                      lambda x: bool(x.get("want")) and int(x["want"])
                      <= int(x.get("ceiling") or 0)
                      and int(x["want"]) > int(x["widest"]),
                      lambda x: f"the row needs a packet of"
                      f" {int(x.get('want') or 0):,} bytes, past the most"
                      f" the server takes ({int(x.get('ceiling') or 0):,})"),
                 Need("the hop allows the target's setting changed",
                      lambda x: bool(x.get("authorized")),
                      "the hop does not allow the target's settings changed"
                      " (target_settings: {max_allowed_packet: yes})"),
                 Need("the target's account may change it",
                      lambda x: bool(x.get("may set")),
                      "the target's account holds neither SUPER nor"
                      " SYSTEM_VARIABLES_ADMIN"))),
        Rung("refused", "the copy refused before it starts, with the"
             " statement that lets it run", because=said)), f)


def account_carried(facts):
    """The `Choice` for an account a grant on the target names that the
    target lacks: "made" as the source has it, its password from the
    source's hash or the passwords file (`users` plans it); "locked" - a
    login whose password neither gives - made unable to sign in, so the
    grant lands and nobody signs in with a password migkit made up, and
    said; "left" where the hop does not carry accounts (`users.carried`).
    `facts`: {"carried", "planned": the users plan makes it, "login"}."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    carried = Need("the hop carries accounts",
                   lambda x: bool(x.get("carried")),
                   "the hop does not carry users, roles or grants")
    return climb("how an account a grant names reaches the target", (
        Rung("made", "made as the source has it", needs=(
            carried, Need("planned with its password or as a role",
                          lambda x: bool(x.get("planned")),
                          "no password for it on the source or in the"
                          " passwords file"))),
        Rung("locked", "made unable to sign in, its grants given",
             needs=(carried, Need("a login", lambda x: bool(x.get("login")),
                                  "not a login"))),
        Rung("left", "left for the operator: the grant waits for it")), f)

# ---- how a target holds a mover's number (`fencing`, backlog W1) ----------

def fence_place(facts, costs=None):
    """Where migkit's own fence record goes on a target without SQL: in
    the target's own database (`in`), or in migkit's own beside it
    (`beside`) where the account may make that but not the other. `facts`
    (`Engine.fence_facts`): `kept_in` / `kept_beside` (the record is
    already there), `create_in` / `create_beside` (the account may make
    it there: read of its grants, never assumed), `grant_in` (the
    statement an operator runs to let it). A target where neither holds
    holds no number: a standby over it is refused with the statement."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    return climb("where a target keeps the mover's number", (
        Rung("in", "migkit's own record in the target's own database",
             needs=(Need("the record kept or makeable there",
                         lambda x: bool(x.get("kept_in")
                                        or x.get("create_in")),
                         lambda x: "the account may not make migkit's own"
                         " record in the target's database ("
                         + (x.get("grant_in") or "grant it the right to")
                         + ")"),),
             because="the record beside the data it fences"),
        Rung("beside", "migkit's own record in a database of its own"
             " beside the target's", footprint=1,
             needs=(Need("the record kept or makeable beside",
                         lambda x: bool(x.get("kept_beside")
                                        or x.get("create_beside")),
                         lambda x: "nor may it make a database of"
                         " migkit's own beside it ("
                         + (x.get("grant_beside") or x.get("grant_in")
                            or "grant it the right to") + ")"),),
             because="the target's own database may not hold it")),
        f, costs=costs or None, prove=False)


def fence_way(facts, costs=None):
    """The strongest way a target without SQL holds a writer to its
    number (`fencing.WAY_CLASSES`), on `facts`: `ways` (what the target
    speaks: `Engine.fence_facts`), `scripts` (a server-side script may
    run), `transactions` (the broker takes a transactional producer).
    Ranked by who refuses: every rung but the last is the target's own
    atomic compare; `replacing-row` is this process reading the target's
    highest number - weaker, and said."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def speaks(name):
        return Need(f"the target speaks {name}",
                    lambda x: name in (x.get("ways") or ()),
                    f"the target has no {name}")
    return climb("how a target holds the mover's number", (
        Rung("guarded-document", "a document changed only where its number"
             " still matches", needs=(speaks("guarded-document"),),
             because="the server's single-document atomicity compares and"
                     " refuses"),
        Rung("compared-key", "a script comparing and setting in one step",
             needs=(speaks("compared-key"),
                    Need("server-side scripts allowed",
                         lambda x: bool(x.get("scripts")),
                         "the account may not run scripts (allow its"
                         " script commands)")),
             because="the server runs the script whole: nothing between"
                     " the compare and the set"),
        Rung("lightweight-transaction", "a conditional update the replicas"
             " agree on", needs=(speaks("lightweight-transaction"),),
             because="a conditional update is applied only where its"
                     " condition holds at every replica it asks"),
        Rung("producer-epoch", "the broker's producer epoch",
             needs=(speaks("producer-epoch"),
                    Need("transactional producers taken",
                         lambda x: bool(x.get("transactions")),
                         "the broker takes no transactional producer for"
                         " the account (allow its transactional id)")),
             because="a producer under a lower epoch is fenced by the"
                     " broker"),
        Rung("replacing-row", "the highest number read back before each"
             " batch: a weaker hold, this process judges", footprint=2,
             needs=(speaks("replacing-row"),),
             because="no atomic compare here: the target keeps the highest"
                     " number and every batch reads it first")),
        f, costs=costs or None, prove=False)


# ---- how an Oracle table's digest is made (DEC30.sweep) -------------------

def oracle_digest_rungs():
    """The ways an Oracle table's rows are summed into one digest
    (`OracleEngine._digest`): in the server, each row's canonical text
    hashed by `standard_hash` and summed, so no row crosses the network -
    where the database's character set is the UTF-8 every other engine
    hashes, the server is one that has the function (12.1 on), every
    column has an exact rendering in its SQL and a row's text fits what a
    hash takes (`max_string_size`); folded here otherwise, a large value
    read in pieces. Facts: `charset`, `version` (major, None where it
    could not be read - the server is asked and a refusal switches),
    `rendered` (the columns without an exact rendering, by kind), `width`
    and `limit` (bytes of a row's text against what a hash takes). The
    built-in needs no grant, so the account's rights to the crypto
    package are not asked."""
    return (
        Rung("server", "summed in the server, no row read here",
             needs=(Need("the database's character set is UTF-8",
                         lambda f: str(f["charset"]).upper() == "AL32UTF8",
                         lambda f: "the server hashes the bytes it holds,"
                         f" which are in {f['charset']}, not the UTF-8 the"
                         " other side hashes"),
                    Need("a server with standard_hash",
                         lambda f: f["version"] is None
                         or int(f["version"]) >= 12,
                         lambda f: f"the server is release {f['version']},"
                         " which has no standard_hash (12.1 on)"),
                    Need("every column rendered exactly in SQL",
                         lambda f: not f["rendered"],
                         lambda f: "no exact rendering in Oracle's SQL for "
                         + ", ".join(f["rendered"])),
                    Need("a row's text within what a hash takes",
                         lambda f: f["width"] <= f["limit"],
                         lambda f: f"a row's text may take {f['width']}"
                         f" bytes, past the {f['limit']} a hash takes")),
             because="no row crosses the network"),
        Rung("here", "folded here, large values read in pieces",
             footprint=1, because="every value is read whole"))

# ---- how a seed's rows are held in its parts (`ship`, backlog DEC1001) ------

#: the targets that read a Parquet file as it is: a part held in Parquet
#: needs no decoding by a loader there
READS_PARQUET = ("parquet", "iceberg", "delta", "duckdb", "clickhouse",
                 "bigquery", "redshift")


def rows_container(facts, costs=None):
    """How the rows of a seed's part are held on the carrier where the
    pair has no native format of its own: as Parquet, a file a batch,
    or as the batches' rows pickled. Parquet is taken only where a batch
    of the table's own rows was carried through it and read back as
    the very rows - Arrow's types hold every column exactly - and then
    where the target reads Parquet itself, or where the batch measured
    smaller held that way than as pickled rows (the table's row width
    and shape decide, not a default). `facts`: `sample` ({"parquet",
    "pickled": bytes of the same batch, compressed as a part is;
    "rows"}; None: no batch was carried through it), `why` (why not,
    where none was), `target` (the target's engine)."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def smaller(x):
        got = x.get("sample") or {}
        return bool(got) and got["parquet"] < got["pickled"]

    def width(x):
        got = x.get("sample") or {}
        if not got or not got.get("rows"):
            return "no batch of its rows was measured"
        return (f"held as Parquet a row takes"
                f" {got['parquet'] / got['rows']:.0f} bytes against"
                f" {got['pickled'] / got['rows']:.0f} as pickled rows")
    return climb("how a seed's rows are held in its parts", (
        Rung("parquet", "Parquet files, one a batch, each carried through"
             " Arrow and read back as the rows it was given", needs=(
                 Need("a batch of the table's rows carried exactly through"
                      " Arrow's types", lambda x: bool(x.get("sample")),
                      lambda x: x.get("why") or "no batch of its rows was"
                      " read to carry through it"),
                 Need("the target reads Parquet itself, or it holds the"
                      " rows smaller",
                      lambda x: x.get("target") in READS_PARQUET
                      or smaller(x), width)),
             because="its types carry every column of a batch exactly, and"
                     " the target reads the file as it is or the file is"
                     " the smaller"),
        Rung("pickled rows", "each batch's rows pickled as the source's"
             " reader hands them over", because="the first way that holds")),
        f, costs=costs or None, prove=False)


def check_sample(facts, costs=None):
    """Whether a table is first held to a sample of its rows read by
    migkit's own keyed reader (`ranges.sampled`), on `facts`: `asked` (the
    operator asked for a quick signal), `first pass` (a sample read before
    the full keyed check), `rows` (the table's count), `rate` (rows a
    second the keyed reader was measured at on this pair) and `seconds`
    (the time the quick signal may take). The size is what the measured
    reader reads in that time, never past the table. A sample never
    stands for the full check: its answer is said as a sample
    (`verdict.decide`), and the full check stays the outcome."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    rows = int(f.get("rows") or 0)
    rate, seconds = float(f.get("rate") or 0), float(f.get("seconds") or 0)
    size = min(rows, int(rate * seconds))
    f.setdefault("sample size", size)
    return climb("whether a table is held to a sample first", (
        Rung("sample", f"{size:,} rows drawn at random and read by key on"
             " both sides, said as a sample",
             needs=(Need("a quick signal asked or a first pass",
                         lambda x: bool(x.get("asked")
                                        or x.get("first pass")),
                         "no quick signal was asked"),
                    Need("the reader's rate measured",
                         lambda x: bool(rate and seconds),
                         "the keyed reader's rate on this pair is not"
                         " measured yet, so no sample size is known"),
                    Need("a sample smaller than the table",
                         lambda x: 0 < size < rows,
                         "the sample would be the whole table: the full"
                         " check reads it")),
             because="a quick first signal; the full check still decides"),
        Rung("full", "every row compared by the keyed check",
             because="only every row compared can say the two are the"
                     " same")),
        f, costs=costs or None, prove=False)

def connector_port(facts):
    """Where a hop's change-stream runtime answers on this machine, and
    where its reader answers its readings: the ports the hop's stack
    already records, taken again while they are the hop's own (the runtime
    answers there) or free; else ports found free that no other hop's
    stack records. A port is a fact of the hop, never one number for every
    hop: two hops on one machine run two runtimes. `facts`: `recorded`
    (the port the stack names, or None), `mine` (it answers as this hop's
    own runtime), `free` (nothing holds it), `found` (a free port no other
    hop's stack names)."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    return climb("the port of a hop's change-stream runtime", (
        Rung("recorded", "the port the hop's stack already names",
             needs=(Need("a port recorded", lambda x: bool(x.get("recorded")),
                         "the hop's stack names no port yet"),
                    Need("it is the hop's own or free",
                         lambda x: bool(x.get("mine") or x.get("free")),
                         "another program holds the recorded port")),
             because="a restart finds the runtime where it was left"),
        Rung("found", "a port found free that no other hop's stack names",
             needs=(Need("a free port found", lambda x: bool(x.get("found")),
                         "no free port could be found on this machine"),),
             because="the machine gives a free port; another hop's record"
                     " is not taken from it")),
        f, prove=False)


def quiet_way(facts):
    """How a load keeps a PostgreSQL target's triggers from firing on its
    rows, from what the target holds and what its user may do: nothing at
    all where no table it loads has a trigger or a foreign key (the
    session is left as it is), the restore's own disabling for a
    superuser, the load's session as a replica for a user who may set
    `session_replication_role`. Only the load's connections to the target
    are ever asked for it - never an export connection to the source.
    `facts`: `triggers` (the loaded tables that have triggers, foreign
    keys' among them), `superuser`, `settable` (the user may set the
    role)."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    return climb("how a load keeps the target's triggers quiet", (
        Rung("untouched", "no trigger or key on the loaded tables: the"
             " session is left as it is",
             needs=(Need("no triggers among the loaded tables",
                         lambda x: not x.get("triggers"),
                         lambda x: "the target's tables "
                         + ", ".join(sorted(x.get("triggers") or ())[:3])
                         + " have triggers or keys"),),
             because="a rule not needed is not set"),
        Rung("flag", "the restore disables each trigger itself",
             needs=(Need("a superuser", lambda x: bool(x.get("superuser")),
                         "the user is not a superuser"),),
             because="the restore's own switch, as it always has"),
        Rung("session", "the load's session runs as a replica",
             needs=(Need("the user may set the role",
                         lambda x: bool(x.get("settable")),
                         "the user may not set session_replication_role"),),
             because="no change to any table")),
        f, prove=False)

def dialect_bulk(facts, costs=None):
    """How a table is loaded into a SQL source that is a dialect of the
    generic engine (`engines.dbapi.DIALECTS`): by its own bulk way, or by
    the table copier through its driver. `facts`: `way` (the kind of bulk
    way the dialect has: `client` - a statement the driver runs over a file
    on this machine, `landing` - a statement the server runs over an object
    store it reads, `tool` - a statement its own command-line client runs;
    None: it has none, `none_why` says so), `streams` (the driver is loaded
    and takes the client way), `landing` and `writer` (the side names a
    landing store and this machine can write it), `tool` (the client
    program is on this machine), `may` (the target's account may run the
    way; None: not read), `rows` (the table's rows; None: not read) and
    `empty` (the target table holds no rows, so a repeat of an interrupted
    load cannot double them). Every need is a fact the hop, the machine or
    the server answered, never assumed: where one does not hold the table
    copier takes the table and says why."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def only(kind):
        return Need(f"the dialect's {kind} bulk way",
                    lambda x, k=kind: x.get("way") == k,
                    lambda x, k=kind: (x.get("none_why") or "the dialect"
                                       " has no bulk way a client reaches")
                    if not x.get("way") else f"the dialect's bulk way is"
                    f" not a {k} one")

    shared = (Need("the target's account allowed the way",
                   lambda x: x.get("may") is True,
                   lambda x: "the target's grants were not read" if
                   x.get("may") is None else "the target's account may not"
                   " run the dialect's bulk way"),
              Need("rows to load", lambda x: (x.get("rows") or 0) > 0,
                   "the table holds no rows to load"),
              Need("the target table empty",
                   lambda x: x.get("empty") is True,
                   "the target table holds rows already: a load could"
                   " write them twice, so they are reconciled by key"))
    return climb("how a table is loaded into the dialect's target", (
        Rung("client", "the dialect's bulk statement over a file the"
             " driver sends from this machine",
             needs=(only("client"),
                    Need("a driver that takes the way",
                         lambda x: bool(x.get("streams")),
                         lambda x: x.get("streams_why")
                         or "the driver is not loaded")) + shared,
             because="the server reads the rows in one statement, where the"
                     " table copier sends them batch by batch"),
        Rung("landing", "the dialect's bulk statement over the landing"
             " store",
             needs=(only("landing"),
                    Need("a landing store named",
                         lambda x: bool(x.get("landing")),
                         "no landing store is named for the target"
                         " (`stage:`)"),
                    Need("a writer for the landing store here",
                         lambda x: bool(x.get("writer")),
                         lambda x: f"nothing here writes {x.get('landing')}"
                         )) + shared,
             because="the server's nodes read the landed file in parallel,"
                     " where the table copier writes through one front"
                     " end"),
        Rung("tool", "the dialect's bulk statement run by its command-line"
             " client over a file on this machine",
             needs=(only("tool"),
                    Need("the dialect's command-line client here",
                         lambda x: bool(x.get("tool")),
                         "its command-line client is not on this machine"))
             + shared,
             because="the client sends the file in one statement"),
        Rung("copier", "the table copier through the driver, by key ranges,"
             " resumably", footprint=1,
             because="the first way that holds")),
        f, costs=costs or None, prove=False)


# ---- what a SAML identity provider is asked for (backlog DEC1001.gui, W2) ----

def saml_asks(facts):
    """What the view asks of one SAML identity provider, each part a climb
    over the provider's published metadata and what the view holds - none
    a fixed default: {"request": how the sign-in request goes, "logout":
    how a sign-out reaches the provider, "assertion": what an answer may
    carry}. `facts`: `key` (the view holds a key and certificate of its
    own), `wants_signed` (the provider's metadata says it takes only signed
    requests), `logout_to` (the address of its single logout by redirect,
    or empty), `demanded` (the operator demands an encrypted assertion).
    A part with no rung is a blocker, said in its `reason`, never a quiet
    weaker way."""
    f = dict(facts)
    request = climb("how the sign-in request is made", (
        Rung("signed", "the sign-in request signed with the view's own key",
             needs=(Need("the provider takes signed requests",
                         lambda x: bool(x.get("wants_signed")),
                         "the provider's metadata does not ask for signed"
                         " requests"),
                    Need("a key of the view's own",
                         lambda x: bool(x.get("key")),
                         "the view holds no key to sign with")),
             because="the provider's metadata says it takes only signed"
                     " requests"),
        Rung("plain", "the sign-in request unsigned",
             needs=(Need("a provider that does not demand signed requests",
                         lambda x: not x.get("wants_signed"),
                         "the provider's metadata demands signed requests,"
                         " and the view holds no key to sign them"),),
             footprint=1,
             because="the provider's metadata does not ask for signed"
                     " requests")), f, prove=False)
    logout = climb("how a sign-out reaches the identity provider", (
        Rung("redirect", "a signed logout request by the redirect binding,"
             " the provider's session ended with the view's",
             needs=(Need("a single logout address in the provider's"
                         " metadata",
                         lambda x: bool(x.get("logout_to")),
                         "the provider's metadata offers no single logout"
                         " by redirect"),
                    Need("a key of the view's own",
                         lambda x: bool(x.get("key")),
                         "the view holds no key to sign a logout with")),
             because="the provider publishes a single logout and the view"
                     " holds a key to sign the request"),
        Rung("local", "the view's session ended; the provider's is its own",
             footprint=1,
             because="the provider publishes no single logout the view can"
                     " use")), f, prove=False)
    assertion = climb("what an answer from the identity provider may carry",
                      (
        Rung("encrypted", "only an encrypted assertion, opened with the"
             " view's key",
             needs=(Need("an encryption demanded",
                         lambda x: bool(x.get("demanded")),
                         "no encryption is demanded"),
                    Need("a key of the view's own",
                         lambda x: bool(x.get("key")),
                         "the view holds no key to open an assertion"
                         " with")),
             because="the operator demands an encrypted assertion, and an"
                     " unencrypted one is refused"),
        Rung("either", "a signed assertion, plain or encrypted",
             needs=(Need("no encryption demanded",
                         lambda x: not x.get("demanded"),
                         "an encryption is demanded and the view holds no"
                         " key to open one"),),
             footprint=1,
             because="no encryption is demanded: a signed assertion stands,"
                     " opened first where it is encrypted")), f,
        prove=False)
    return {"request": request, "logout": logout, "assertion": assertion}

#: the replica ids a reader may be given: the upper half of the range a
#: server takes, where the ids an operator gives the servers they run
#: rarely are; 0 and 2**32 - 1 are never a replica's
READER_IDS = (2 ** 31, 2 ** 32 - 2)


def _derived_reader_id(identity, taken):
    """The first id the reader's identity (hop, database, purpose) walks
    to that nothing named in `taken` holds: the same identity comes to the
    same id run after run, two identities to two, and a walk over as many
    steps as ids are taken always finds one free."""
    import zlib
    low, high = READER_IDS
    span = high - low + 1
    base = zlib.crc32(str(identity).encode())
    for k in range(len(taken) + 1):
        got = low + (base + k * 2654435761) % span
        if got not in taken:
            return got
    raise AssertionError("unreachable: more steps than ids taken")


def reader_server_id(facts):
    """The replica id a binlog reader attaches under, from what the source
    and this machine say is taken. Two readers on one source under one id:
    the server ends the first when the second attaches, and each then
    takes the other's place again. Returns (the `Choice`, the id or None).

    `facts`: `named` (the hop's `server_id`, None where not set: honoured
    while free and refused, naming who holds it, where not), `recorded`
    (the id this reader was given before, in its run store), `identity`
    (hop, database and purpose the derived id is walked from) and `taken`
    ({id: who holds it}: the source's own `@@server_id`, the replicas it
    lists, the ids other hops on this machine recorded on this source, the
    replica ids the operator named, and an id a reader was ended under)."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def taken(x):
        return dict(x.get("taken") or {})

    def held(key):
        return lambda x: (f"{x.get(key)} is held by "
                          f"{taken(x).get(int(x.get(key)))}")

    unnamed = Need("no id named for the hop",
                   lambda x: x.get("named") in (None, ""),
                   lambda x: f"the hop names server_id {x.get('named')}")
    choice = climb("which replica id the binlog reader attaches under", (
        Rung("named", "the id the hop names",
             needs=(Need("an id named", lambda x: x.get("named")
                         not in (None, ""), "no id is named"),
                    Need("the named id free", lambda x: int(x.get("named"))
                         not in taken(x), held("named"))),
             because="the operator's id, while nothing else holds it"),
        Rung("recorded", "the id this reader attached under before",
             needs=(unnamed,
                    Need("an id recorded", lambda x: x.get("recorded")
                         is not None, "no id is recorded for this reader"),
                    Need("the recorded id free", lambda x: int(
                        x.get("recorded")) not in taken(x),
                        held("recorded"))),
             because="the same id as before: the source sees one reader"),
        Rung("derived", "an id of the reader's own, from the hop, the"
             " database and what it reads for",
             needs=(unnamed,),
             because="free of every id the source and this machine hold")),
        f, prove=False)
    got = {"named": lambda: int(f.get("named")),
           "recorded": lambda: int(f.get("recorded")),
           "derived": lambda: _derived_reader_id(f.get("identity"),
                                                 taken(f))}
    return choice, (got[choice.path]() if choice.path else None)


def tail_state(facts):
    """What a tail that carried nothing lately is doing, so its line says
    why, not silence: the reader is not attached (and why), the source
    wrote elsewhere and nothing on the hop's tables for longer than its
    measured write interval, the source wrote nothing at all, or the tail
    is following and the quiet is still within what the source's pace
    makes ordinary. `facts`: `attached` (False: the source lists no reader
    under the tail's id, or the read failed; None: not readable),
    `why_detached`, `moved` (bytes the source's log moved since the last
    change on the hop's tables; None: not read), `quiet_for` (seconds
    since that change, or since the tail started) and `interval` (the
    measured seconds between the hop's changes; None: none measured yet,
    so any quiet is past it)."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    def past(x):
        gap = x.get("interval")
        return gap is None or (x.get("quiet_for") or 0) > gap
    return climb("what a quiet tail is doing", (
        Rung("detached", "the reader is not attached",
             needs=(Need("the reader not attached",
                         lambda x: x.get("attached") is False,
                         "the reader is attached or cannot be told"),),
             because="nothing reaches the tail until it attaches again"),
        Rung("elsewhere", "quiet: the source writes, none of it on the"
             " hop's tables",
             needs=(Need("the source's log moved",
                         lambda x: (x.get("moved") or 0) > 0,
                         "the source's log did not move"),
                    Need("quiet past the write interval", past,
                         "the quiet is within the measured interval")),
             because="the tail read past every write; none was the hop's"),
        Rung("idle", "quiet: the source wrote nothing",
             needs=(Need("the source's log still",
                         lambda x: x.get("moved") == 0,
                         "the source's log moved or was not read"),
                    Need("quiet past the write interval", past,
                         "the quiet is within the measured interval")),
             because="nothing was written to carry"),
        Rung("following", "following", because="the quiet is ordinary")),
        f, prove=False)

def free_space_source(facts, costs=None):
    """Where the target's free space is read from (backlog 7.free): the
    server's own report, a look at the server's data directory from this
    machine where it is proved to be the server's own, or the managed
    service's provider - and nobody, said so, where none answers. Facts
    (each read only for a rung still in the running): `server` (bytes the
    server reported, None where it did not), `server_why` (why not),
    `directory` (bytes free on the disk under the data directory this
    machine sees, None where it does not), `directory_why`, `provider`
    (bytes the provider's metric says), `provider_why`. A guess is never a
    rung: the capacity gate reads None as "nobody knows" and says so. A
    source after the first that answers is not read."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))

    order = ("server", "directory", "provider")

    def said(key):
        # the first source that answers ends the climb: a later one is
        # never read once an earlier one gave a figure (its reader may run
        # a program on the server)
        before = order[:order.index(key)]

        def holds(x, k=key):
            return all(x.get(b) is None for b in before) and \
                x.get(k) is not None

        def why(x, k=key):
            for b in before:
                if x.get(b) is not None:
                    return f"the {b} answered first"
            return x.get(f"{k}_why") or f"the {k} did not answer"
        return Need(f"a figure from the {key}", holds, why)
    return climb("where the target's free space is read from", (
        Rung("server", "the server's own report of the disk under its"
             " data", needs=(said("server"),),
             because="the server reads its own disk, under the account's"
                     " right"),
        Rung("directory", "the disk under the server's data directory, seen"
             " from this machine and proved the server's own",
             needs=(said("directory"),),
             because="this machine sees the server's own data directory"),
        Rung("provider", "the managed service's own metric of free storage",
             needs=(said("provider"),),
             because="the provider measures the volume the service runs"
                     " on"),
        Rung("none", "nobody says: the plan names no free space",
             footprint=1, because="no source answered, and a guess would"
                                  " be worse than saying so")),
        f, costs=costs or None, prove=False)


#: The releases migkit has run each engine on (backlog 26.versions), as
#: (oldest, newest) - PostgreSQL by its major, the others by (major,
#: minor): the lanes' and the tests' own images -
#: PostgreSQL 14, 16 and 17, MySQL 8.4 and 9.4, MariaDB 10.11 to 11.8. A
#: release outside is one no choice keyed on a version was ever run on.
VERSIONS_SEEN = {"postgres": ((14,), (17,)),
                 "mysql": ((8, 0), (9, 4)),
                 "mariadb": ((10, 11), (11, 8))}

#: What each engine chooses by its server's version, as (since, what): the
#: release from which the choice is taken - the thresholds the engines
#: test (`server_version_num` on PostgreSQL, the banner on MySQL and
#: MariaDB), listed so a version test can say which a server takes
VERSION_KEYS = {
    "postgres": (((10, 0), "identity columns read from the catalogue"),
                 ((11, 0), "procedures told from functions"),
                 ((12, 0), "generated columns read and left to the target"),
                 ((13, 0), "the snapshot read through pg_current_snapshot"),
                 ((14, 0), "a table read in spans of its pages, and large"
                           " transactions streamed to the subscriber"),
                 ((15, 0), "WAL records read in the server, and the"
                           " database's default collation checked"),
                 ((16, 0), "the subscriber applies streamed transactions in"
                           " parallel")),
    "mysql": (((8, 0, 17), "the server's own clone of its data files"),
              ((8, 3), "tagged GTIDs as the change marks")),
    "mariadb": ()}


def version_choices(family, version):
    """[(since, what, taken)] of the engine's version-keyed choices for a
    server at `version` (a tuple; None where it was not read: nothing is
    taken)."""
    have = tuple(version) if version else None
    return [(since, what, have is not None and have >= since)
            for since, what in VERSION_KEYS.get(family, ())]


def version_seen(facts):
    """Whether the server's release is one migkit has run on (backlog
    26.versions): `family` (postgres, mysql, mariadb) and `version` (a
    tuple, None where it was not read). Inside `VERSIONS_SEEN` it is
    taken; a release before or after is refused in words - the choices
    keyed on a version were never run against it, and a server newer than
    any seen may have moved what one of them reads. One not read is taken
    and said so: nothing keyed on a version is chosen for it."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    fam = f.get("family")
    low, high = VERSIONS_SEEN.get(fam, (None, None))

    def text(v):
        return ".".join(str(x) for x in v or ()) or "not read"
    return climb(f"whether migkit has run on this {fam} release", (
        Rung("unread", "the release was not read: nothing keyed on a"
             " version is chosen",
             needs=(Need("the release not read",
                         lambda x: not x.get("version"),
                         "the server said its release"),),
             because="the server did not say its release"),
        Rung("seen", "a release migkit has run on",
             needs=(Need("an engine whose releases are listed",
                         lambda x: low is not None,
                         f"no release of {fam} is listed as run on"),
                    Need("no older than the oldest run on",
                         lambda x: bool(x.get("version")) and tuple(
                             x["version"][:len(low)]) >= low,
                         lambda x: f"release {text(x['version'])} is older"
                         f" than {text(low)}, the oldest migkit has run"
                         f" {fam} on"),
                    Need("no newer than the newest run on",
                         lambda x: bool(x.get("version")) and tuple(
                             x["version"][:len(high)]) <= high,
                         lambda x: f"release {text(x['version'])} is newer"
                         f" than {text(high)}, the newest migkit has run"
                         f" {fam} on: the choices keyed on a version were"
                         " never run against it")),
             because="the version-keyed choices were run on it")),
        f, prove=False)

def _seed_meets(secs, f):
    deadline = f.get("deadline")
    return deadline is None or secs is None or secs <= deadline


def _seed_took(secs):
    from .machine import _took
    return _took(secs)


#: the copy over the link and the seed on media the hop names: the rungs
#: `ship.SEEDS` climbs, owned here so the seed's carrier is one decision
SEED_NETWORK = Rung(
    "network", "the copy over the link",
    needs=(Need("the copy over the link within the deadline",
                lambda f: _seed_meets(f.get("network"), f),
                lambda f: f"it takes about {_seed_took(f.get('network'))},"
                          f" past the deadline of"
                          f" {_seed_took(f.get('deadline'))}"),),
    because="the link carries it within what the hop asks")

SEED_MEDIA = Rung(
    "media", "a seed shipped on media",
    needs=(Need("a carrier the hop names",
                lambda f: bool(f.get("ship")),
                "the hop names no carrier (options ship:)"),
           Need("the seed within the deadline",
                lambda f: _seed_meets(f.get("media"), f),
                lambda f: f"it takes about {_seed_took(f.get('media'))},"
                          f" past the deadline of"
                          f" {_seed_took(f.get('deadline'))}")),
    footprint=1,
    because="the hop names a carrier")


#: the seed carried as a volume snapshot's changed blocks (`blockseed`):
#: a rung of the seed's carriers (`ship.SEEDS["blocks"]`)
SEED_BLOCKS = Rung(
    "blocks", "the changed blocks of a volume snapshot",
    needs=(Need("a snapshot whose parent the target holds",
                lambda f: f.get("blocks ok") is True,
                lambda f: str(f.get("blocks ok") or "no snapshot of the"
                              " source's volume is named")),
           Need("the changed blocks within the deadline",
                lambda f: _seed_meets(f.get("blocks"), f),
                lambda f: f"its changed blocks take about"
                          f" {round(f.get('blocks') or 0)}s, past the"
                          f" deadline of {round(f.get('deadline') or 0)}s")),
    footprint=1,
    because="only the blocks changed since the target's snapshot travel,"
            " and the follow starts from the position recorded at it")


def seed_carrier(facts, costs=None):
    """How the seed reaches the target: the changed blocks of a volume
    snapshot whose parent the target holds (`blockseed`), the seed on media
    the hop names, or the copy over the link (`ship.SEEDS`) - each held to
    the deadline and ranked by the seconds measured for it (`ship.choose`
    reads the hop's and the measured times into them). `facts`:
    `blocks ok` (True, or why no block seed can be carried), `blocks` (its
    changed blocks' seconds at the measured rate), `ship` (the hop names a
    carrier), `media`, `network` (seconds; None where nothing measured)
    and `deadline`. Where nothing is measured the block seed, carrying
    least, goes first, then the carrier the hop names, then the link."""
    f = facts if isinstance(facts, Facts) else Facts(dict(facts or {}))
    for k in ("ship", "network", "media", "deadline", "blocks ok",
              "blocks"):
        f.setdefault(k, None)
    rungs = (SEED_BLOCKS, SEED_MEDIA, SEED_NETWORK) if f.get("ship") else (
        SEED_BLOCKS, SEED_NETWORK, SEED_MEDIA)
    got = {k: f.get(k) for k in ("blocks", "media", "network")}
    measured = {k: v for k, v in got.items() if v is not None}
    return climb("the seed's carrier", rungs, f,
                 costs=dict(costs if costs is not None else measured) or None,
                 prove=False)
