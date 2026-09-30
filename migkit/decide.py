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
"""
import contextlib
import dataclasses
import hashlib
import itertools
import json
import os
import time
from dataclasses import dataclass, field

NOT_YET = "not yet"
NOT_APPLICABLE = "not applicable"

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

    @property
    def path(self):
        return self.rung.name if self.rung else None

    @property
    def said(self):
        return self.rung.said if self.rung else "no way here"

    def __str__(self):
        return f"{self.work}: {self.said} - {self.reason}"


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


class Trials:
    """One climb for each unit of a larger work - the ranges of one table -
    on that work's own measurements, from threads copying units side by
    side. A rung never timed is tried on one unit; the units started
    before it finishes go on to the next rung never timed, not to a second
    trial of the same one, and once every rung holding is timed each unit
    takes the cheapest. A rung that failed on this work (`give_up`) is
    passed over for the rest of it, and the reason is said the way any
    other rung's is."""

    def __init__(self, work, rungs, facts=None, need=(), costs=None):
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

    def pick(self):
        """The `Choice` for the next unit."""
        with self._lock:
            got = climb(self.work, self.rungs, self.facts, self.need,
                        _InFlight(self.costs, self._timing))
            if got.rung is not None and \
                    self.costs.get(got.rung.name) is None:
                self._timing.add(got.rung.name)
            return got

    def took(self, name, units, seconds):
        """A unit done on `name`: `units` of work in `seconds`."""
        with self._lock:
            self.costs.saw(name, units, seconds)
            self._timing.discard(name)

    def give_up(self, name, why):
        """`name` failed here and is not tried again on this work."""
        with self._lock:
            self.given_up.setdefault(name, why)
            self._timing.discard(name)


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


def rank(rungs, costs=None):
    """The rungs in the order the climb tries them."""
    get = (costs or {}).get
    return _order([(get(r.name), r.footprint, i, r)
                   for i, r in enumerate(rungs)])


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


def climb(work, rungs, facts=None, need=(), costs=None, keep=None,
          prove=True):
    """The `Choice` for one unit of work from its `rungs`.

    `need` is what the work must have (capabilities); `facts` what the
    needs and proofs read (a dict, or `Facts` to read lazily); `costs`
    where the rungs' measurements are (`Costs`, `HopCosts`, any mapping of
    name to seconds a unit); `keep` the rung a restart stands on
    (`remembered`), taken first where it still holds and proves.

    The reason is why the ways the list prefers were passed over, or the
    chosen rung's own `because` where none was: a way the list puts below
    the chosen one is no reason for it."""
    facts = facts if facts is not None else Facts()
    rungs = tuple(rungs)
    at = {id(r): i for i, r in enumerate(rungs)}
    kept, out = prune(rungs, facts, need)
    ranked = rank(kept, costs)
    order, passed = list(ranked), []
    if keep:
        held = [r for r in ranked if r.name == keep]
        order = held + [r for r in ranked if r.name != keep]
        passed = [(r, f"the way the last run took: {why}")
                  for r, why in out if r.name == keep]
    for r in order:
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
            timed = [o for o in ranked if (costs or {}).get(o.name)
                     is not None]
            if len(timed) > 1 and r in timed:
                reason += "; measured the cheapest of the ways timed here"
        return Choice(work, r, reason, tuple(out + passed), tuple(ranked))
    return Choice(work, None,
                  _joined(w for _, w in out + passed) or "no way was given",
                  tuple(out + passed), tuple(ranked))


def remember(where, work, name):
    """Keep that `work` stands on `name` - a rung's name, or a strategy's
    {part: name} - beside the position file `where` (the move's
    checkpoint, the tail's token), in a file of its own, since every engine
    writes its position in a shape of its own. Written whole or not at
    all."""
    path = _kept_file(where)
    got = _read(path)
    got[str(work)] = name
    _write(path, got)


def _write(path, got):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(got, f, indent=1)
    os.replace(tmp, path)


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
    try:
        got = json.loads(open(path).read())
    except (OSError, ValueError):
        return {}
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
            "google", "azure", "opensearchpy", "ibm_db", "snowflake")


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
    path = _kept_file(where)
    got = _read(path)
    got.setdefault(_up_key(work), {})[name] = rec
    _write(path, got)


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
            otherwise=None, switch=operational):
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
    reasons; no rung left stops with every rung's reason (`Exhausted`)."""
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
            if where is not None:
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
        if where is not None:
            remember(where, work, rung.name)
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
}

_STREAM_ONLY = ("a stream of changes is where a pair delivers them, not a"
                " hop's engine: it holds no table")
_BY_KEY_ORDER = ("read by {}, never in key order, so the key's shape does"
                 " not arise")
_NO_PROGRAM = ("no program is driven for this engine; migkit's own reader"
               " needs only to read")

_DBAPI = ("mssql", "oracle", "db2", "db2i", "db2zos", "ase", "redshift",
          "snowflake", "bigquery", "duckdb")
_STREAMS = ("kinesis", "pubsub")
_FILES = ("sqlite", "duckdb", "parquet", "copybook")
_NONE = {**{n: (NOT_APPLICABLE, _STREAM_ONLY) for n in _STREAMS},
         "generic": (NOT_YET, "33")}

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
                     "opensearch", "parquet") + _DBAPI},
        "mongodb": (NOT_APPLICABLE, "every document carries an _id"),
        "copybook": ("yes", "@position_spans"),
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
           + _DBAPI},
        "cassandra": (NOT_APPLICABLE, _BY_KEY_ORDER.format("token ranges")),
        "dynamodb": (NOT_APPLICABLE, _BY_KEY_ORDER.format("scan segments")),
        "opensearch": (NOT_APPLICABLE, _BY_KEY_ORDER.format("scroll slices")),
        "parquet": (NOT_APPLICABLE, _BY_KEY_ORDER.format("a file at a time")),
        "copybook": (NOT_APPLICABLE, _BY_KEY_ORDER.format(
            "ranges of whole records")),
        "redis": (NOT_APPLICABLE, "a key is one string, read by the scan's"
                                  " cursor"),
        "kafka": (NOT_APPLICABLE, "a message's key is carried, never"
                                  " resumed by"),
    },
    "too large": {
        **_NONE,
        **{n: ("yes", "@native_bulk")
           for n in ("cassandra", "dynamodb", "opensearch", "parquet")},
        "*": ("yes", "@move_table"),
    },
    "large values": {
        **_NONE,
        "postgres": ("yes", "@_large_values"),
        "mysql": ("yes", "@_large_values"),
        "copybook": (NOT_APPLICABLE, "a field is as long as its copybook"
                                     " says, a record 32,760 bytes at most"
                                     " in a variable file"),
        "oracle": ("yes", "@_read_in_pieces"),
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
        "sqlite": (NOT_APPLICABLE, "SQLite has no partitioned tables"),
        "duckdb": (NOT_APPLICABLE, "DuckDB has no partitioned tables"),
        "copybook": (NOT_APPLICABLE, "a file is not partitioned; one file"
                                     " of several record layouts is several"
                                     " tables, by segment"),
        "*": (NOT_YET, "R13"),
    },
    "missing grant": {
        **_NONE,
        "postgres": ("yes", "movers._pg_quiet_triggers"),
        "mysql": ("yes", "movers._MyTriggerWindow"),
        # the redo-file reader's grants, named to the DBA; LogMiner
        # otherwise, which needs none of them
        "oracle": ("yes", "dba._ora_redo_files"),
        "hetero": (NOT_YET, "0"),
        "mongodb": (NOT_YET, "0"),
        "*": (NOT_APPLICABLE, _NO_PROGRAM),
    },
    "read-only target": {
        **_NONE,
        "postgres": ("yes", "freeze.freeze"),
        "mysql": ("yes", "freeze.freeze"),
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
