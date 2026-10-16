"""Product telemetry: what migkit did, never what it carried (backlog
P0-PRODUCT-TELEMETRY).

An event is one of the typed kinds below (`KINDS`), each field drawn from
a closed set: an engine migkit knows, a command it has, a bucket, a
bounded number, a flag. There is no free text field, so a table's name, a
row's value, a statement, an address, a path or an error's words have
nowhere to go: the producer refuses an unknown field, a string outside its
set and a value of the wrong type (`Rejected`) - exclusion by type, not
redaction after the fact. Nothing is hashed in place of being left out.

Events are kept on this machine first (`Local`, under `spool`): an open
head file, sealed when it is full into an append-only segment - its lines
hash-chained from the segment before, closed by a manifest naming its
count, first and last hour, schema and digest - the oldest dropped past
the number or age the policy keeps. They are written, synced, by a
background thread from a bounded queue; a full queue drops the event and
counts it, so a step never waits on telemetry and a failure here never
changes a run. Nothing leaves on its own: `push`, by hand only, sends the
closed segments to an S3-compatible bucket the operator runs and names,
through the egress gate's `telemetry` kind, signed in by that store's own
credential chain (no key in the hop or in migkit), each segment once
under its digest; `receive` reads them back and checks every event again
(`checked`), so a field outside the schema or a free string is refused at
both ends. `migkit telemetry show` prints the state and exactly what is
kept.

Off, the stricter first (`state`): the organisation's policy file
(`MIGKIT_TELEMETRY_POLICY`, `telemetry: off` or `egress: none`; one that
cannot be read counts as off), `MIGKIT_EGRESS` naming kinds without
`telemetry`, `MIGKIT_TELEMETRY=off`, and `migkit telemetry off`. Off stops
queueing and disposes of the spool: its sealed segments and their pending
push with it.
"""
import atexit
import contextlib
import dataclasses
import hashlib
import json
import math
import os
import queue
import re
import threading
import time
from pathlib import Path

#: 1.1 added the emitters' own codes: a fault class and the action taken
#: for it, the question put to the operator, and the `crash` kind
SCHEMA = 1.1

#: what is collected, why, where it goes and how to stop it: the manual's
#: page and `telemetry show` say these words
DISCLOSURE = (
    ("What is collected", "how migkit behaved: the command, the engines,"
     " the ways chosen and why (as codes), timings and rates in buckets,"
     " workers allocated and used, progress and how far the estimate was"
     " off, error categories, retries and recoveries, how much was verified"
     " and to which guarantee, how a run ended and what the operator"
     " stepped in to do."),
    ("What is never collected", "rows, values, samples, keys or digests of"
     " them, statements, schemas, the names of databases, tables, columns"
     " or accounts, addresses, paths, connection strings, credentials,"
     " logs, stack traces and error messages. Every field is a closed code,"
     " a bucket or a bounded number; anything else is refused where the"
     " event is made."),
    ("Why", "to tell which ways work on which engines and shapes, how good"
     " the estimates are, and where runs fail or need a hand."),
    ("Where it goes", "this machine first: events are kept under the state"
     " directory in sealed, append-only segments of a fixed size, each"
     " chained to the one before, the oldest dropped past the number or age"
     " the organisation's policy keeps (`telemetry_keep`, `telemetry_days`)."
     " Nothing leaves on its own: `migkit telemetry push <bucket>` sends the"
     " sealed segments, once each, to an S3-compatible bucket you run and"
     " name, signed in with that store's own credentials, never a key"
     " migkit carries; `migkit telemetry receive <bucket>` reads them back"
     " and checks every event again. With `MIGKIT_EGRESS=none`, or a policy"
     " saying `telemetry_push: off`, nothing is sent and the push says why."),
    ("Default", "on: events are kept locally; a push happens only when"
     " you run it. `migkit telemetry show` prints exactly what is kept."),
    ("How to turn it off", "`migkit telemetry off`, `MIGKIT_TELEMETRY=off`,"
     " `MIGKIT_EGRESS=none`, or an organisation policy file"
     " (`MIGKIT_TELEMETRY_POLICY`, `/etc/migkit/policy.yaml` by default)"
     " saying `telemetry: off`. Off stops keeping events and deletes those"
     " already kept, the sealed segments not yet pushed included."),
)

FILE_BYTES = 128 * 1024      #: one spool file at most
FILES = 4                    #: spool files kept, the oldest dropped
EVENT_BYTES = 2048           #: one event at most, as written
QUEUED = 512                 #: events waiting for the writer at most
DEFAULT_POLICY = "/etc/migkit/policy.yaml"
#: room the manifest line takes in a sealed segment, so a segment with
#: its manifest stays within `FILE_BYTES`
MANIFEST_BYTES = 512
#: the segment's own format, in its manifest
SEGMENT = 1
#: the schema versions a receiver takes
SCHEMAS = (1, 1.1)
HOUR = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}")
DIGEST = re.compile(r"[0-9a-f]{64}")
#: a version as `migkit --version` prints one, for a receiver of another
VERSION = re.compile(r"[0-9]{1,4}(\.[0-9]{1,4}){0,3}([a-z]{1,5}[0-9]{0,4})?")

COMPLETIONS = ("done", "same", "different", "could not verify",
               "incomplete", "usage", "failed", "refused", "interrupted")
ERRORS = ("none", "timeout", "connection", "permission", "resource",
          "not found", "refused", "usage", "interrupted", "data", "stopped",
          "internal")
PHASES = ("discover", "assess", "configure", "plan", "schema", "move",
          "follow", "verify", "repair", "cutover", "rollback", "report")
RECOVERIES = ("retry", "resume", "fallback", "autotune", "reconcile",
              "gave up")
OUTCOMES = ("recovered", "failed", "blocked")
INTERVENTIONS = ("paused", "resumed", "cancelled", "approved", "overrode",
                 "retried by hand", "repaired by hand", "asked", "answered",
                 "bundled")
#: the phase a command works in, for a stop that no phase announced
PHASE_OF = {"assess": "assess", "advise": "assess", "doctor": "assess",
            "init": "configure", "hops": "configure",
            "convert-schema": "schema", "schema": "schema",
            "setup-target": "schema", "users": "schema",
            "gen-migration": "plan", "move": "move", "migrate": "move",
            "sync": "follow", "tail": "follow", "replicate": "follow",
            "check": "verify", "sample-diff": "verify", "repair": "repair",
            "rollback": "rollback", "report": "report", "history": "report",
            "watch": "report", "monitor": "report", "state": "report",
            "ui": "report"}
UNITS = ("table", "database", "range")
COVERAGE = ("0", "<50%", "<90%", "<99%", "<100%", "100%")


class Rejected(ValueError):
    """An event refused where it is made: the field and why, never the
    value."""


# ---- the closed sets ----------------------------------------------------------

def _version():
    from . import __version__
    return (__version__,)


def _engines():
    from .engines import NAMES
    return tuple(NAMES) + ("other",)


def _commands():
    from . import cli
    return tuple(cli.main.commands) + ("other",)


def _slug(words):
    return re.sub(r"[^a-z0-9]+", "-", str(words).lower()).strip("-")[:48]


_STRATEGIES = None


def strategies():
    """{a way's label in the code: its identifier here}, read off the
    engine's own ladders: the identifier is the way's words as an operator
    reads them, so no program is named, and a label made at run time from
    what a server said is not among them."""
    global _STRATEGIES
    if _STRATEGIES is None:
        from . import export
        _STRATEGIES = {label: _slug(said)
                       for label, said in export._ways().items()}
    return _STRATEGIES


def _strategy_ids():
    return tuple(sorted(set(strategies().values()))) + ("other",)


def _reasons():
    from .decide import WORDS
    return tuple(WORDS) + ("first choice", "fell back", "none holds",
                           "other")


def _guarantees():
    from .guarantees import LEVELS
    return tuple(f"{g}:{level}" for g, levels in LEVELS.items()
                 for level in levels)


def _faults():
    from .faults import CATALOGUE
    return tuple(CATALOGUE)


def _paths():
    from .faults import ACTIONS
    return tuple(ACTIONS)


def _questions():
    from .autonomous import BY_KEY
    return tuple(BY_KEY)


def _bucket_names():
    return tuple(_size_bucket(10 ** k) for k in range(-1, 13))


def _size_bucket(n):
    """A count as its order of magnitude: `0`, `1-9`, `10-99`, ... and
    `>=1e12`."""
    if n is None or n < 1:
        return "0"
    k = min(int(math.log10(n)), 12)
    if k >= 12:
        return ">=1e12"
    return f"1e{k}-1e{k + 1}" if k else "1-9"


def bucket(n):
    """A size, a count or a rate in its order of magnitude."""
    return _size_bucket(n)


def seconds_bucket(s):
    """A duration as a bucket of powers of two seconds."""
    if s is None or s < 1:
        return "<1s"
    k = min(int(math.log2(s)), 20)
    return f"{2 ** k}s+" if k < 20 else ">=12d"


def _seconds_names():
    return ("<1s",) + tuple(f"{2 ** k}s+" for k in range(20)) + (">=12d",)


def coverage(verified, total):
    """How much of the scope was verified, in the bucket it falls in."""
    if not total or not verified:
        return "0"
    part = verified / total
    return ("100%" if part >= 1 else "<100%" if part >= .99
            else "<99%" if part >= .9 else "<90%" if part >= .5 else "<50%")


def error_category(e):
    """An exception as a category, from its type alone: its words, which
    can hold a statement, a name or a value, are never read."""
    from . import verdict
    if e is None:
        return "none"
    if isinstance(e, KeyboardInterrupt):
        return "interrupted"
    if isinstance(e, TimeoutError):
        return "timeout"
    if isinstance(e, PermissionError):
        return "permission"
    if isinstance(e, (FileNotFoundError, LookupError)):
        return "not found"
    if isinstance(e, ConnectionError):
        return "connection"
    if isinstance(e, (MemoryError, RecursionError)):
        return "resource"
    if isinstance(e, verdict.Usage):
        return "usage"
    if isinstance(e, verdict.Refused):
        return "refused"
    if isinstance(e, SystemExit):
        # a number is a verdict's exit; words are migkit stopping itself
        return ("none" if e.code is None or isinstance(e.code, int)
                else "stopped")
    if isinstance(e, (UnicodeError, ArithmeticError, ValueError)):
        return "data"
    if isinstance(e, OSError):
        return "connection"
    return "internal"


def completion(code, e=None):
    """How a command ended, from its exit code (`verdict.EXIT`,
    `verdict.VERDICTS`) and what stopped it."""
    from . import verdict
    if isinstance(e, KeyboardInterrupt):
        return "interrupted"
    if e is not None and not isinstance(e, SystemExit):
        return "failed"
    if isinstance(e, verdict.Usage):
        return "usage"
    if isinstance(e, verdict.Refused):
        return "refused"
    if isinstance(e, SystemExit) and code is None:
        code = e.code
    if code is None or code is False:
        code = 0
    elif type(code) is not int:
        return "failed"
    by_code = {v: k for k, v in verdict.EXIT.items()}
    for v in verdict.VERDICTS.values():
        by_code.setdefault(v.exit, v.word)
    got = by_code.get(code)
    return got if got in COMPLETIONS else "failed"


# ---- the fields an event may have ---------------------------------------------

def one_of(values, doc):
    """A field whose value is one of `values()`."""
    return dataclasses.field(default=None, metadata={
        "type": "one of", "values": values, "doc": doc})


def some_of(values, doc, most=16):
    """A field holding at most `most` distinct values of `values()`."""
    return dataclasses.field(default=(), metadata={
        "type": "some of", "values": values, "doc": doc, "most": most})


def number(most, doc, whole=True):
    """A field holding a number from 0 to `most`."""
    return dataclasses.field(default=None, metadata={
        "type": "whole" if whole else "number", "most": most, "doc": doc})


def flag(doc):
    return dataclasses.field(default=None, metadata={"type": "flag",
                                                     "doc": doc})


#: an opaque id: twelve hex digits of a salted hash, never a name
OPAQUE = re.compile(r"[0-9a-f]{12}")


def opaque(doc):
    """A field holding an id made by `unit_id` and nothing else."""
    return dataclasses.field(default=None, metadata={"type": "opaque",
                                                     "doc": doc})


def _sizes():
    return _bucket_names()


@dataclasses.dataclass(frozen=True)
class Run:
    """One command, at its end."""
    version: str = one_of(_version, "migkit's version")
    command: str = one_of(_commands, "the command")
    engine: str = one_of(_engines, "the hop's engine")
    target_engine: str = one_of(_engines, "the target's engine")
    completion: str = one_of(lambda: COMPLETIONS, "how it ended")
    error: str = one_of(lambda: ERRORS, "what stopped it, by kind")
    took: str = one_of(_seconds_names, "how long it took")
    seconds: float = number(10 ** 7, "how long it took", whole=False)
    writes: bool = flag("it was asked to write (--go, --apply)")
    scheduled: bool = flag("its schedule started it")
    databases: str = one_of(_sizes, "databases in the hop's scope")
    decisions: int = number(10 ** 6, "ways chosen")
    fallbacks: int = number(10 ** 6, "ways given up for another")
    options_set: int = number(1000, "options the operator set")
    dropped: int = number(10 ** 9, "events dropped for a full queue")


@dataclasses.dataclass(frozen=True)
class Decision:
    """One way chosen by the decision engine."""
    strategy: str = one_of(_strategy_ids, "the way chosen")
    reasons: tuple = some_of(_reasons, "why, as codes")
    guarantees: tuple = some_of(_guarantees, "the guarantees it reaches")
    ranked: int = number(1000, "ways that held")
    passed: int = number(1000, "ways passed over")
    fallback: bool = flag("it replaced a way chosen before")
    trial: bool = flag("it ran on a bounded trial")


@dataclasses.dataclass(frozen=True)
class Progress:
    """Where a phase stood, and how good its estimate was."""
    phase: str = one_of(lambda: PHASES, "the phase")
    rows: str = one_of(_sizes, "rows done")
    bytes: str = one_of(_sizes, "bytes done")
    rate: str = one_of(_sizes, "rows a second")
    took: str = one_of(_seconds_names, "how long it has run")
    eta_off: float = number(1000, "actual time over the estimate",
                            whole=False)
    done: str = one_of(lambda: COVERAGE, "how much of it is done")


@dataclasses.dataclass(frozen=True)
class Resources:
    """What a phase was given and what it used."""
    phase: str = one_of(lambda: PHASES, "the phase")
    workers_allocated: int = number(10 ** 5, "workers allocated")
    workers_used: int = number(10 ** 5, "workers that ran")
    connections: int = number(10 ** 5, "connections held")
    memory: str = one_of(_sizes, "bytes of memory the workers hold")
    cpus: int = number(10 ** 4, "processors on this machine")


@dataclasses.dataclass(frozen=True)
class Recovery:
    """A failure met and what was done about it."""
    phase: str = one_of(lambda: PHASES, "the phase")
    error: str = one_of(lambda: ERRORS, "the failure, by kind")
    fault: str = one_of(_faults, "the class of failure")
    path: str = one_of(_paths, "the action the decision engine chose")
    action: str = one_of(lambda: RECOVERIES, "what was done")
    outcome: str = one_of(lambda: OUTCOMES, "how it ended")
    attempts: int = number(10 ** 4, "attempts")


@dataclasses.dataclass(frozen=True)
class Coverage:
    """What a verification proved."""
    phase: str = one_of(lambda: PHASES, "the phase")
    verified: str = one_of(lambda: COVERAGE, "the scope verified")
    guarantees: tuple = some_of(_guarantees, "the guarantees reached")
    completion: str = one_of(lambda: COMPLETIONS, "the verdict")
    units: str = one_of(_sizes, "units compared")


def _statuses():
    from .engines.base import STATUSES
    return tuple(STATUSES)


@dataclasses.dataclass(frozen=True)
class Unit:
    """What a verification proved of one unit: a table, a database or a
    range. The unit is an opaque id, the same on every machine only for
    the install that made it."""
    phase: str = one_of(lambda: PHASES, "the phase")
    scope: str = one_of(lambda: UNITS, "what kind of unit it is")
    unit: str = opaque("the unit, as a salted hash")
    outcome: str = one_of(_statuses, "how the unit compared")
    guarantees: tuple = some_of(_guarantees, "the guarantees it reached")


@dataclasses.dataclass(frozen=True)
class Intervention:
    """The operator stepping in."""
    phase: str = one_of(lambda: PHASES, "the phase")
    did: str = one_of(lambda: INTERVENTIONS, "what they did")
    question: str = one_of(_questions, "the question migkit put to them")


@dataclasses.dataclass(frozen=True)
class Crash:
    """A stop migkit did not expect: its kind and where, never its words;
    the traceback stays in the local evidence folder."""
    command: str = one_of(_commands, "the command")
    phase: str = one_of(lambda: PHASES, "the phase it worked in")
    error: str = one_of(lambda: ERRORS, "what stopped it, by kind")


KINDS = {"run": Run, "decision": Decision, "progress": Progress,
         "resources": Resources, "recovery": Recovery,
         "coverage": Coverage, "unit": Unit, "intervention": Intervention,
         "crash": Crash}


def _check(name, value, meta):
    """The value, or `Rejected` naming the field: the value itself is
    never in the message, since what was refused is what must not go."""
    if value is None or (meta["type"] == "some of" and value == ()):
        return value
    t = meta["type"]
    if t == "flag":
        if type(value) is not bool:
            raise Rejected(f"{name} takes on or off")
        return value
    if t in ("whole", "number"):
        if type(value) is bool or not isinstance(value, (int, float)):
            raise Rejected(f"{name} takes a number")
        if t == "whole" and not float(value).is_integer():
            raise Rejected(f"{name} takes a whole number")
        if not (0 <= value <= meta["most"]) or value != value:
            raise Rejected(f"{name} is outside 0 to {meta['most']}")
        return int(value) if t == "whole" else round(float(value), 3)
    if t == "opaque":
        if type(value) is not str or not OPAQUE.fullmatch(value):
            raise Rejected(f"{name} takes an id made by unit_id")
        return value
    allowed = meta["values"]()
    if t == "one of":
        if type(value) is not str or value not in allowed:
            raise Rejected(f"{name} takes one of its codes, not free text")
        return value
    if not isinstance(value, (tuple, list, set, frozenset)):
        raise Rejected(f"{name} takes a list of its codes")
    got = tuple(sorted(set(value), key=str))
    if len(got) > meta["most"]:
        raise Rejected(f"{name} takes at most {meta['most']} codes")
    for v in got:
        if type(v) is not str or v not in allowed:
            raise Rejected(f"{name} takes only its codes, not free text")
    return got


def make(kind, /, **fields):
    """The event of `kind` as it is kept: {"schema", "kind", "hour",
    fields}; `Rejected` for an unknown kind or field, or a value outside
    its field's set."""
    cls = KINDS.get(kind)
    if cls is None:
        raise Rejected("an event kind telemetry does not know")
    known = {f.name: f for f in dataclasses.fields(cls)}
    unknown = sorted(set(fields) - set(known))
    if unknown:
        # the field's own name is not said: a producer that passed a
        # table's name as a field must not have it repeated in a log
        raise Rejected(f"{kind} has {len(unknown)} field(s) not in its"
                       " schema")
    body = {name: _check(name, fields.get(name), known[name].metadata)
            for name in known}
    event = {"schema": SCHEMA, "kind": kind,
             "hour": time.strftime("%Y-%m-%dT%H", time.gmtime()),
             **{k: list(v) if isinstance(v, tuple) else v
                for k, v in body.items() if v not in (None, ())}}
    if len(json.dumps(event)) > EVENT_BYTES:
        raise Rejected(f"{kind} is larger than {EVENT_BYTES} bytes")
    return event


def schema():
    """[(kind, field, type, what it holds, its codes or bound)], for the
    manual and for `telemetry show`."""
    rows = []
    for kind, cls in KINDS.items():
        for f in dataclasses.fields(cls):
            m = f.metadata
            bound = (", ".join(m["values"]())
                     if m["type"] in ("one of", "some of")
                     and f.name not in ("strategy", "command", "engine",
                                        "target_engine")
                     else f"0 to {m['most']}" if "most" in m
                     and m["type"] != "some of" else "")
            rows.append((kind, f.name, m["type"], m["doc"], bound))
    return rows


# ---- on or off ----------------------------------------------------------------

def spool():
    """Where events are kept: `MIGKIT_TELEMETRY_SPOOL`, else beside the
    state of runs."""
    from . import config, envkeys
    got = envkeys.text("MIGKIT_TELEMETRY_SPOOL")
    return Path(got).expanduser() if got else config.state_home() \
        / "telemetry"


_OFF = ("none", "off", "false", "no", "0", False)


def _policy_doc():
    """The organisation's policy file as a mapping: {} where there is
    none, None where it cannot be read."""
    from . import envkeys
    path = Path(envkeys.text("MIGKIT_TELEMETRY_POLICY")
                or DEFAULT_POLICY).expanduser()
    if not path.exists():
        return {}
    try:
        import yaml
        got = yaml.safe_load(path.read_text()) or {}
        if not isinstance(got, dict):
            raise ValueError
    except Exception:  # noqa: BLE001 - unreadable is the stricter answer
        return None
    return got


def _policy():
    """What the organisation's policy file says: None where there is
    none, else (on, why)."""
    got = _policy_doc()
    if got is None:
        return False, "the organisation's policy file cannot be read"
    off = _OFF
    if (got.get("telemetry") in off or got.get("egress") in off
            or str(got.get("telemetry", "")).lower() in off
            or str(got.get("egress", "")).lower() in off):
        return False, "the organisation's policy turns it off"
    return None


def state():
    """(on, why): the strictest of the policy file, the environment and
    the operator's own setting."""
    from . import egress, envkeys
    got = _policy()
    if got is not None:
        return got
    try:
        allow = egress.settings(None)["allow"]
    except SystemExit:
        return False, "MIGKIT_EGRESS cannot be read"
    if allow is not None and "telemetry" not in allow:
        return False, "MIGKIT_EGRESS does not let telemetry out"
    try:
        if not envkeys.flag("MIGKIT_TELEMETRY"):
            return False, "MIGKIT_TELEMETRY is off"
    except envkeys.Bad:
        return False, "MIGKIT_TELEMETRY cannot be read"
    if (spool() / "off").exists():
        return False, "turned off with `migkit telemetry off`"
    return True, ("on: events are kept on this machine, and nothing"
                  " leaves unless pushed by hand")


# ---- where events go ----------------------------------------------------------

class Sink:
    """Where kept events go: the local store is the only sink; leaving
    this machine is `push`'s alone, asked of the egress gate."""

    def put(self, lines):
        raise TypeError(f"{type(self).__name__} keeps nothing")

    def read(self):
        return []

    def dispose(self):
        return 0


def _chained(prev, lines):
    """The chain's head after `lines`, from `prev`: each line's hash
    commits to the one before, as the audit record's entries do."""
    head = prev
    for line in lines:
        head = hashlib.sha256((head + "\n" + line).encode()).hexdigest()
    return head


def _digest(prev, body):
    """A segment's digest: its bytes and where its chain starts, so two
    segments holding the same events are told apart by their history."""
    return hashlib.sha256((prev + "\n").encode() + body).hexdigest()


def _hour_of(line):
    try:
        got = json.loads(line).get("hour")
    except (ValueError, AttributeError):
        return None
    return got if type(got) is str and HOUR.fullmatch(got) else None


def keeps():
    """(segments kept, days kept or None): the organisation's policy
    (`telemetry_keep`, `telemetry_days`), else the spool's own bound - as
    many sealed segments as leave the store at `FILES` files - and no
    age."""
    doc = _policy_doc() or {}
    count, days = FILES - 1, None
    got = doc.get("telemetry_keep")
    if type(got) is int and got >= 1:
        count = got
    got = doc.get("telemetry_days")
    if type(got) in (int, float) and got > 0:
        days = float(got)
    return count, days


class Local(Sink):
    """The store on this machine: the open head `events-0.jsonl` of at
    most `FILE_BYTES` with its manifest's room, and the sealed segments
    under `segments/`, `segment-<seq>.seg`: a manifest line, then the
    events as they were kept. Every write is synced before it counts;
    a head that would overflow is sealed first, and the oldest segments
    past `keeps()` are dropped."""

    def __init__(self, root=None):
        self.root = Path(root) if root is not None else spool()
        self.held = self.root / "segments"

    def _file(self, i):
        return self.root / f"events-{i}.jsonl"

    @contextlib.contextmanager
    def _locked(self):
        """The store held against another process's writer or push: a
        seal never takes a head another process is appending to."""
        import fcntl
        from . import durable
        if not self.root.exists():
            durable.private_dir(self.root)
        fd = os.open(str(self.root / ".lock"), os.O_RDWR | os.O_CREAT,
                     0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def put(self, lines):
        with self._locked():
            self._put(lines)

    def _put(self, lines):
        from . import durable
        head = self._file(0)
        size = head.stat().st_size if head.exists() else 0
        batch = []
        for line in lines:
            n = len(line.encode()) + 1
            if size + n > FILE_BYTES - MANIFEST_BYTES and (batch or size):
                if batch:
                    durable.append_lines(head, batch)
                    batch = []
                self._seal()
                size = 0
            batch.append(line)
            size += n
        if batch:
            durable.append_lines(head, batch)

    # ---- the sealed segments ----------------------------------------------

    def segments(self):
        """[(seq, path)], oldest first."""
        got = []
        for p in self.held.glob("segment-*.seg"):
            seq = p.stem.split("-", 1)[1]
            if seq.isdigit():
                got.append((int(seq), p))
        return sorted(got)

    @staticmethod
    def manifest(path):
        with open(path, "rb") as f:
            return json.loads(f.readline())

    def seal(self):
        """Close the head into the next segment (`_seal`), the store held;
        the segment's path, or None where nothing was open."""
        with self._locked():
            return self._seal()

    def _seal(self):
        """The head renamed `sealing-<seq>` first, then written whole and
        synced as segment `<seq>`, then removed. Interrupted anywhere, the
        next seal finds the renamed head: its segment there (a write is
        whole or absent) means it was sealed and only the removal is left;
        else it is sealed now. No event is sealed twice or lost."""
        from . import durable
        got = None
        for pending in sorted(self.root.glob("sealing-*.jsonl")):
            got = self._close(pending) or got
        head = self._file(0)
        if head.exists():
            seq = max([n for n, _ in self.segments()]
                      + [int(p.stem.split("-")[1])
                         for p in self.root.glob("sealing-*.jsonl")],
                      default=0) + 1
            pending = self.root / f"sealing-{seq:08d}.jsonl"
            head.replace(pending)
            durable.sync_dir(self.root)
            got = self._close(pending) or got
        return got

    def _close(self, pending):
        from . import durable
        seq = int(pending.stem.split("-")[1])
        path = self.held / f"segment-{seq:08d}.seg"
        lines = []
        for line in pending.read_bytes().decode(
                errors="replace").splitlines():
            try:
                json.loads(line)
            except ValueError:
                continue    # a torn last line: nothing read it as an event
            lines.append(line)
        if lines and not path.exists():
            held = [x for x in self.segments() if x[0] < seq]
            prev = (self.manifest(held[-1][1])["chain"] if held
                    else self.seed())
            body = "".join(line + "\n" for line in lines).encode()
            hours = [h for h in map(_hour_of, lines) if h]
            doc = {"segment": SEGMENT, "seq": seq, "schema": SCHEMA,
                   "count": len(lines), "first": min(hours, default=None),
                   "last": max(hours, default=None), "prev": prev,
                   "chain": _chained(prev, lines),
                   "digest": _digest(prev, body)}
            top = json.dumps(doc, sort_keys=True) + "\n"
            if len(top) > MANIFEST_BYTES:
                raise ValueError("a manifest outgrew its room")
            if not self.held.exists():
                durable.private_dir(self.held)
            durable.write_atomic(path, top.encode() + body)
            durable.sync_dir(self.held)
        pending.unlink(missing_ok=True)
        durable.sync_dir(self.root)
        self.retain()
        return path if lines else None

    def seed(self):
        """Where this store's chain starts: random, made with the store and
        gone with it - no install, host or person in it - so two stores
        that kept the same events never seal the same segment, and a
        bucket two of them push to keeps both."""
        from . import durable
        path = self.held / "seed"
        try:
            got = path.read_text().strip()
            if DIGEST.fullmatch(got):
                return got
        except OSError:
            pass
        got = os.urandom(32).hex()
        durable.write_atomic(path, got)
        return got

    def pushed(self, path):
        return path.with_suffix(".pushed").exists()

    def retain(self):
        """Drop the oldest segments past `keeps()`: how many went that had
        not been pushed is counted (`unsent`)."""
        global _UNSENT
        count, days = keeps()
        held = self.segments()
        old = held[:max(len(held) - count, 0)]
        if days is not None:
            cut = time.strftime("%Y-%m-%dT%H", time.gmtime(
                time.time() - days * 86400))
            for item in held[len(old):]:
                last = self.manifest(item[1]).get("last")
                if last and last < cut:
                    old.append(item)
        for _, p in old:
            if not self.pushed(p):
                _UNSENT += 1
            p.unlink(missing_ok=True)
            p.with_suffix(".pushed").unlink(missing_ok=True)
        return len(old)

    def read(self):
        got = []
        for _, p in self.segments():
            try:
                lines = p.read_text().splitlines()[1:]
            except OSError:
                continue
            got += [json.loads(x) for x in lines if x.strip()]
        try:
            text = self._file(0).read_text()
        except OSError:
            text = ""
        for x in text.splitlines():
            try:
                got.append(json.loads(x))
            except ValueError:
                pass
        return got

    def bytes(self):
        files = [self._file(0)] + [p for _, p in self.segments()]
        return sum(p.stat().st_size for p in files if p.exists())

    def dispose(self):
        gone = 0
        for p in [*self.root.glob("events-*.jsonl"),
                  *self.root.glob("sealing-*.jsonl"),
                  *self.held.glob("segment-*.seg")]:
            p.unlink(missing_ok=True)
            gone += 1
        for p in [*self.held.glob("segment-*.pushed"), self.held / "seed"]:
            p.unlink(missing_ok=True)
        return gone


_UNSENT = 0


def unsent():
    """Sealed segments dropped by retention before they were pushed."""
    return _UNSENT


# ---- the writer ---------------------------------------------------------------

_Q = queue.Queue(maxsize=QUEUED)
_LOCK = threading.Lock()
_WRITER = None
_DROPPED = 0
_DISPOSED = False


def _write():
    while True:
        got = [_Q.get()]
        # one batch at most a queue's worth: what waits in memory is
        # bounded by twice `QUEUED`, however fast the producers are
        while len(got) < QUEUED:
            try:
                got.append(_Q.get_nowait())
            except queue.Empty:
                break
        try:
            lines = [x for x in got if x is not None]
            if lines:
                Local().put(lines)
        except Exception:  # noqa: BLE001 - telemetry never stops a run
            pass
        finally:
            for _ in got:
                _Q.task_done()


def _started():
    global _WRITER
    with _LOCK:
        if _WRITER is None or not _WRITER.is_alive():
            _WRITER = threading.Thread(target=_write, daemon=True,
                                       name="migkit-telemetry")
            _WRITER.start()
            atexit.register(flush)


def dropped():
    return _DROPPED


def record(kind, /, **fields):
    """Keep one event, without waiting: True if it was queued. Off, the
    spool is disposed of (once) and nothing is kept; a full queue drops
    the event and counts it. `Rejected` is the only thing raised: a
    producer handing a field it may not is a bug, not a run's problem."""
    global _DROPPED, _DISPOSED
    event = make(kind, **fields)
    try:
        on, _ = state()
    except Exception:  # noqa: BLE001 - unknown is off
        on = False
    if not on:
        if not _DISPOSED:
            _DISPOSED = True
            try:
                Local().dispose()
            except OSError:
                pass
        return False
    _started()
    try:
        _Q.put_nowait(json.dumps(event, sort_keys=True))
    except queue.Full:
        with _LOCK:
            _DROPPED += 1
        return False
    return True


def flush(timeout=1.0):
    """Wait up to `timeout` seconds for queued events to be kept."""
    end = time.monotonic() + timeout
    while _Q.unfinished_tasks and time.monotonic() < end:
        time.sleep(0.01)
    return not _Q.unfinished_tasks


def turn(on):
    """`migkit telemetry on|off`: off writes the marker and disposes of
    the spool; returns how many spool files were deleted."""
    root = spool()
    if on:
        (root / "off").unlink(missing_ok=True)
        return 0
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    (root / "off").write_text("telemetry turned off by the operator\n")
    flush()
    return Local(root).dispose()


# ---- out by hand, and back ----------------------------------------------------
# `push` and `receive` reach the bucket through the one S3 seam (`anchor`'s
# client, the store's own credential chain) and only through the egress
# gate's `telemetry` kind; a manual act, never one a run takes on its own.

def _where(bucket, endpoint=None):
    """{"bucket", "prefix", "endpoint_url"} of `name`, `name/prefix` or
    `s3://name/prefix`; segments go under `<prefix>/telemetry/`."""
    from . import verdict
    text = str(bucket or "").strip()
    if text.startswith("s3://"):
        text = text[5:]
    name, _, prefix = text.partition("/")
    if not name:
        raise verdict.Usage("telemetry push and receive take the bucket to"
                            " use")
    prefix = prefix.strip("/")
    return {"bucket": name,
            "prefix": (prefix + "/" if prefix else "") + "telemetry/",
            "endpoint_url": endpoint or None}


def _gate(conf):
    """Why the bucket may not be reached, in words, or None: asked before
    any client is made or anything listed."""
    from . import egress
    ok, why = egress.decide(None, "telemetry", conf["endpoint_url"])
    if not ok:
        egress.permit(None, "telemetry", conf["endpoint_url"])
    return None if ok else why


def _client(conf):
    from . import anchor, verdict
    try:
        return anchor._client(conf)
    except anchor.Refused as e:
        raise verdict.Refused(str(e))


def _store_said(e):
    """A store's failure by its code or kind alone: its message can carry
    what the store was asked, so it is not repeated."""
    from . import anchor
    code = anchor._code(e)
    return (f"the store answered {code}" if code else
            f"the store could not be reached ({type(e).__name__})")


def _held(s3, conf, key, data):
    """Whether `key` holds exactly `data`: False where it is absent;
    `Refused` where it holds other bytes."""
    from . import anchor, verdict
    try:
        got = s3.get_object(Bucket=conf["bucket"], Key=key)["Body"].read()
    except Exception as e:  # noqa: BLE001 - only absence is an answer
        if anchor._code(e) in ("NoSuchKey", "404", "NotFound"):
            return False
        raise
    if got != data:
        raise verdict.Refused("the bucket holds other bytes under a sealed"
                              " segment's digest - something else wrote to"
                              " it; nothing more is sent")
    return True


def _put_once(s3, conf, key, data):
    """"sent", or "already" where the key holds this segment: the put is
    conditional (`IfNoneMatch`), so two pushes, or a push resumed after it
    was cut, make one object."""
    from . import anchor
    if _held(s3, conf, key, data):
        return "already"
    try:
        s3.put_object(Bucket=conf["bucket"], Key=key, Body=data,
                      ContentType="application/x-ndjson", IfNoneMatch="*")
        return "sent"
    except Exception as e:  # noqa: BLE001 - one already there is read
        if anchor._code(e) not in ("PreconditionFailed", "412",
                                   "ConditionalRequestConflict", "409"):
            raise
        if _held(s3, conf, key, data):
            return "already"
        raise


def push(bucket, endpoint=None, s3=None):
    """`migkit telemetry push`: the head sealed, then every sealed
    segment not yet pushed, oldest first, under `<digest>.seg`. A segment
    is marked pushed only once the bucket holds it; a failure stops the
    push with the rest left for the next one. The bytes one push sends are
    bounded by the store's own (`keeps()` segments of `FILE_BYTES`).
    Refused (`verdict.Refused`) where telemetry is off, the policy says
    `telemetry_push: off` or the egress does not let telemetry out - and
    then nothing is listed or connected to."""
    from . import anchor, durable, verdict
    conf = _where(bucket, endpoint)
    on, why = state()
    if not on:
        raise verdict.Refused(f"telemetry is off ({why}), so nothing is"
                              " pushed")
    said = (_policy_doc() or {}).get("telemetry_push")
    if said is not None and (said in _OFF or str(said).lower() in _OFF):
        raise verdict.Refused("the organisation's policy says"
                              " telemetry_push: off, so nothing is pushed")
    why = _gate(conf)
    if why:
        raise verdict.Refused(why)
    flush()
    store = Local()
    store.seal()
    todo = [p for _, p in store.segments() if not store.pushed(p)]
    budget = keeps()[0] * FILE_BYTES
    out = {"sent": 0, "already": 0, "bytes": 0, "left": 0, "failed": None}
    if not todo:
        return out
    s3 = s3 or _client(conf)
    for i, path in enumerate(todo):
        data = path.read_bytes()
        if out["bytes"] + len(data) > budget:
            out["left"] = len(todo) - i
            break
        key = f"{conf['prefix']}{store.manifest(path)['digest']}.seg"
        try:
            got = anchor._retry(None, lambda: _put_once(s3, conf, key, data))
        except Exception as e:  # noqa: BLE001 - the rest waits, said
            out["failed"], out["left"] = _store_said(e), len(todo) - i
            break
        durable.write_atomic(path.with_suffix(".pushed"), key)
        out[got] += 1
        out["bytes"] += len(data) if got == "sent" else 0
    return out


def push_lines(out):
    lines = [f"pushed: {out['sent']} segment(s), {out['bytes']} bytes;"
             f" {out['already']} already in the bucket"]
    if out["left"]:
        lines.append(f"left for the next push: {out['left']} segment(s)"
                     + (f" - {out['failed']}" if out["failed"] else
                        " - past the bytes one push sends"))
    return lines


_MANIFEST = ("chain", "count", "digest", "first", "last", "prev", "schema",
             "segment", "seq")


def _manifest_checked(doc):
    bad = Rejected("a segment whose manifest is not one migkit seals")
    if not isinstance(doc, dict) or tuple(sorted(doc)) != _MANIFEST:
        raise bad
    whole = (doc["segment"], doc["seq"], doc["count"])
    if any(type(x) is not int for x in whole) or doc["segment"] != SEGMENT \
            or not 1 <= doc["seq"] < 10 ** 9 \
            or not 0 < doc["count"] <= FILE_BYTES:
        raise bad
    if type(doc["schema"]) not in (int, float) or doc["schema"] \
            not in SCHEMAS:
        raise bad
    for k in ("first", "last"):
        if doc[k] is not None and (type(doc[k]) is not str
                                   or not HOUR.fullmatch(doc[k])):
            raise bad
    for k in ("prev", "chain", "digest"):
        v = doc[k]
        if type(v) is not str or not DIGEST.fullmatch(v):
            raise bad


def checked(event):
    """`event` as the receiver takes it: the producer's own `_check` run
    again on every field, an unknown kind, field or schema refused, the
    hour an hour. Only `version` is read by its shape rather than this
    install's own, since a receiver serves other versions."""
    if not isinstance(event, dict):
        raise Rejected("an event that is not a record")
    kind = event.get("kind")
    cls = KINDS.get(kind) if type(kind) is str else None
    if cls is None:
        raise Rejected("an event kind telemetry does not know")
    got = event.get("schema")
    if type(got) not in (int, float) or got not in SCHEMAS:
        raise Rejected("an event of a schema this receiver does not know")
    hour = event.get("hour")
    if type(hour) is not str or not HOUR.fullmatch(hour):
        raise Rejected("hour takes an hour, not free text")
    known = {f.name: f for f in dataclasses.fields(cls)}
    unknown = set(event) - set(known) - {"schema", "kind", "hour"}
    if unknown:
        raise Rejected(f"{kind} has {len(unknown)} field(s) not in its"
                       " schema")
    for name, f in known.items():
        value = event.get(name)
        if name == "version" and value is not None:
            if type(value) is not str or not VERSION.fullmatch(value):
                raise Rejected("version takes a version, not free text")
            continue
        _check(name, tuple(value) if isinstance(value, list) else value,
               f.metadata)
    return event


def segment_checked(data, name=None):
    """(manifest, events taken, [why each refused event was]) of one
    sealed segment as stored, `name` the digest it is stored under;
    `Rejected` where the segment is not whole as it was sealed: a chain
    that breaks, bytes its digest does not name, another name."""
    try:
        top, _, body = data.partition(b"\n")
        doc = json.loads(top)
        text = body.decode()
    except (ValueError, UnicodeError):
        raise Rejected("a segment whose manifest cannot be read")
    _manifest_checked(doc)
    n = doc["seq"]
    lines = text.splitlines()
    if len(lines) != doc["count"] or _chained(doc["prev"], lines) \
            != doc["chain"]:
        raise Rejected(f"segment {n}'s chain breaks: it was changed after"
                       " it was sealed")
    if _digest(doc["prev"], body) != doc["digest"] or (
            name is not None and name != doc["digest"]):
        raise Rejected(f"segment {n} is not the bytes its digest names: it"
                       " was changed after it was sealed")
    events, bad = [], []
    for line in lines:
        try:
            events.append(checked(json.loads(line)))
        except Rejected as e:
            bad.append(str(e))
        except ValueError:
            bad.append("an event that is not a record")
    return doc, events, bad


def receive(bucket, endpoint=None, s3=None):
    """`migkit telemetry receive`: every segment under the bucket's
    telemetry prefix listed, read and checked (`segment_checked`) - a
    segment that fails is refused whole, an event that fails is refused
    alone - and the events taken counted by kind, engine and way: the
    analysis loop's first table. `gaps` counts segments whose chain names
    one not received. Refused where the egress does not let telemetry
    out, before anything is listed."""
    import collections
    from . import verdict
    conf = _where(bucket, endpoint)
    why = _gate(conf)
    if why:
        raise verdict.Refused(why)
    s3 = s3 or _client(conf)
    keys, token = [], None
    while True:
        kw = {"Bucket": conf["bucket"], "Prefix": conf["prefix"]}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in page.get("Contents", ())
                 if o["Key"].endswith(".seg")]
        token = page.get("NextContinuationToken")
        if not page.get("IsTruncated") or not token:
            break
    out = {"segments": 0, "events": 0, "refused": [], "gaps": 0,
           "rejected": collections.Counter(),
           "table": collections.Counter()}
    chains, prevs = set(), []
    for key in sorted(keys):
        data = s3.get_object(Bucket=conf["bucket"], Key=key)["Body"].read()
        try:
            doc, events, bad = segment_checked(
                data, key[len(conf["prefix"]):-len(".seg")])
        except Rejected as e:
            out["refused"].append(str(e))
            continue
        out["segments"] += 1
        chains.add(doc["chain"])
        if doc["seq"] > 1:
            prevs.append(doc["prev"])
        for e in events:
            out["events"] += 1
            out["table"][(e["kind"], e.get("engine") or "-",
                          e.get("strategy") or "-")] += 1
        out["rejected"].update(bad)
    out["gaps"] = sum(1 for p in prevs if p not in chains)
    return out


def receive_lines(out):
    lines = [f"received: {out['segments']} segment(s), {out['events']}"
             f" event(s) taken; {len(out['refused'])} segment(s) and"
             f" {sum(out['rejected'].values())} event(s) refused;"
             f" {out['gaps']} segment(s) whose one before was not"
             " received"]
    lines += [f"  refused: {why}" for why in out["refused"]]
    lines += [f"  refused {n} event(s): {why}"
              for why, n in sorted(out["rejected"].items())]
    if out["table"]:
        lines.append("kind | engine | way | events")
        lines += [f"{k} | {e} | {s} | {n}" for (k, e, s), n in
                  sorted(out["table"].items(), key=lambda x: (-x[1], x[0]))]
    return lines


# ---- a command, as events -----------------------------------------------------

class Command:
    """One command's telemetry (`session.step`): each way the decision
    engine chooses as a `Decision` (`heard`, a `decide.LISTENERS` entry),
    and the command's end as a `Run` (`ended`). What a work is called -
    a database's or a table's name - stays in memory, to tell a fallback,
    and is never kept."""

    def __init__(self, command, params, hop=None):
        self.command, self.params, self.hop = command, params, hop
        self.began = time.time()
        self.chosen, self.decisions, self.fallbacks = {}, 0, 0
        #: the guarantees the ways chosen in this command reach
        self.reached = set()
        reset_forecasts()
        global _CURRENT
        _CURRENT = self

    def heard(self, got):
        try:
            self._heard(got)
        except Exception:  # noqa: BLE001 - telemetry never stops a run
            pass

    def _heard(self, got):
        parts = getattr(got, "parts", None)
        rungs = (list(parts.values()) if parts is not None
                 else [got.rung])
        names = strategies()
        passed = tuple(getattr(got, "passed", ()) or ())
        before = self.chosen.get(str(got.work))
        label = rungs[0].name if rungs and rungs[0] is not None else None
        self.chosen[str(got.work)] = label
        fell = bool(before and before != label
                    and before in {getattr(r, "name", None)
                                   for r, _ in passed})
        reasons = {"none holds" if label is None else
                   "fell back" if fell else "first choice"
                   if not passed else "other"}
        from .decide import WORDS
        for rung, why in passed:
            for cap in rung_lacks(rung, why, WORDS):
                reasons.add(cap)
        allowed = set(_guarantees())
        reached = {f"{g}:{level}" for g, level in
                   (getattr(got, "guarantees", ()) or ())} & allowed
        self.decisions += 1
        self.fallbacks += fell
        self.reached |= reached
        record("decision",
               strategy=names.get(label, "other") if label else "other",
               reasons=tuple(sorted(reasons))[:16],
               guarantees=tuple(sorted(reached))[:16],
               ranked=min(len(getattr(got, "ranked", ()) or ()), 1000),
               passed=min(len(passed), 1000), fallback=fell,
               trial=bool(getattr(got, "trial", 0)))

    def ended(self, code=None, error=None):
        global _CURRENT
        if _CURRENT is self:
            _CURRENT = None
        try:
            self._ended(code, error)
        except Exception:  # noqa: BLE001 - telemetry never stops a run
            pass

    def _ended(self, code, error):
        took = max(time.time() - self.began, 0.0)
        settled(PHASE_OF.get(self.command, "report"), took)
        engine = getattr(self.hop, "engine", None)
        target = getattr(getattr(self.hop, "target", None), "engine",
                         None) or engine
        engines = set(_engines())
        commands = set(_commands())
        dbs = getattr(self.hop, "databases", None)
        record("run", version=_version()[0],
               command=self.command if self.command in commands
               else "other",
               engine=(engine if engine in engines else "other")
               if engine else None,
               target_engine=(target if target in engines else "other")
               if target else None,
               completion=completion(code, error),
               error=error_category(error),
               took=seconds_bucket(took), seconds=min(took, 10 ** 7),
               writes=bool(self.params.get("go")
                           or self.params.get("do_apply")),
               scheduled=_scheduled(),
               databases=bucket(len(dbs)) if dbs else None,
               decisions=min(self.decisions, 10 ** 6),
               fallbacks=min(self.fallbacks, 10 ** 6),
               options_set=min(sum(1 for v in self.params.values()
                                   if v not in (None, False, "", (), 0)),
                               1000),
               dropped=min(_DROPPED, 10 ** 9))


_CURRENT = None


def _scheduled():
    from . import envkeys
    try:
        return envkeys.flag("MIGKIT_SCHEDULED")
    except envkeys.Bad:
        return False


def rung_lacks(rung, why, words):
    """The capabilities `why` says `rung` was passed over for, as codes:
    each `decide.WORDS` reason is matched against the words it makes, so
    the reason's own text never leaves."""
    from .decide import lacking
    said = str(why)
    return {cap for cap in words if lacking(cap, rung) in said}


# ---- the other kinds, from the code that knows them ---------------------------
# Each emitter takes typed values, makes the event and keeps it; none raises:
# an event refused or a spool that cannot be written never changes a run.

def _emit(what, /, **fields):
    try:
        return record(what, **fields)
    except Exception:  # noqa: BLE001 - telemetry never stops a run
        return False


#: seconds between one phase's progress events; the last one always goes
PROGRESS_EVERY = 30.0
#: one phase of one work (a database's report directory) has its own pace:
#: databases moving side by side must not reset each other's estimate. The
#: key stays in memory and is never kept.
_PACE = {}
#: the seconds a phase was forecast to take, from the plan or an estimate
_PLANNED = {}
#: phases whose `eta_off` has gone in this command
_SETTLED = set()
#: the plan's phase names (`planner.Phase.name`) as the phases they run in
_PLAN_PHASE = {"copy": "move", "index builds": "move",
               "catch-up": "follow", "check": "verify",
               "cutover": "cutover"}


def _pace_key(phase, where):
    return (phase, None if where is None else str(where))


def progressed(phase, rows, total, rate, took, done=False, size=None,
               now=None, where=None):
    """`planner.progress`: where a phase stands, as buckets. The first
    estimate of the whole phase is kept in memory per `where`, and once
    the phase is done `eta_off` is the time it took over that estimate."""
    now = time.monotonic() if now is None else now
    key = _pace_key(phase, where)
    seen = _PACE.get(key)
    left = (max(total - rows, 0) / rate) if rate and total else None
    if seen is None or took < seen["took"]:
        seen = _PACE[key] = {"took": took, "at": None, "whole": None}
    seen["took"] = took
    if seen["whole"] is None and left is not None and not done:
        seen["whole"] = took + left
    if not done and seen["at"] is not None \
            and now - seen["at"] < PROGRESS_EVERY:
        return False
    seen["at"] = now
    off = None
    if done:
        whole = _PACE.pop(key, seen)["whole"]
        off = took / whole if whole else None
        _SETTLED.add(phase)
    return _emit("progress", phase=phase, rows=bucket(rows),
                 bytes=bucket(size) if size is not None else None,
                 rate=bucket(rate or 0), took=seconds_bucket(took),
                 eta_off=min(off, 1000) if off is not None else None,
                 done="100%" if done else coverage(rows, total))


def forecast(phase, seconds):
    """A phase's forecast time, kept in memory until the phase ends
    (`settled`). Several forecasts for one phase add up."""
    try:
        if phase in PHASES and isinstance(seconds, (int, float)) \
                and seconds > 0 and seconds == seconds \
                and seconds != float("inf"):
            _PLANNED[phase] = _PLANNED.get(phase, 0.0) + float(seconds)
    except Exception:  # noqa: BLE001 - telemetry never stops a run
        pass


def forecast_plan(phases):
    """`planner.phase_lines`: the plan's estimates, each added to the
    phase it runs in. A phase the plan does not estimate adds nothing."""
    for p in phases or ():
        if getattr(p, "never", False):
            continue
        phase = _PLAN_PHASE.get(getattr(p, "name", None))
        if phase and getattr(p, "p50", None):
            forecast(phase, p.p50)


def settled(phase, took):
    """A phase over, whatever its kind: `eta_off` is the time it took
    over its forecast. False where it had no forecast or has been said
    (`progressed` says the phases that report their own pace)."""
    planned = _PLANNED.pop(phase, None)
    if not planned or phase in _SETTLED:
        return False
    _SETTLED.add(phase)
    return _emit("progress", phase=phase, took=seconds_bucket(took),
                 eta_off=min(max(took, 0.0) / planned, 1000), done="100%")


def reset_forecasts():
    _PLANNED.clear()
    _SETTLED.clear()


#: the salt that makes a unit's id the same for this install only
_SALT = None


def _salt():
    """Sixteen random bytes kept beside the spool, so that an id means
    the same unit in every run of this install and in no other. Where the
    spool cannot be written the salt lives for this process only."""
    global _SALT
    if _SALT is None:
        path = spool() / "unit.salt"
        try:
            _SALT = bytes.fromhex(path.read_text().strip())
            if len(_SALT) != 16:
                raise ValueError("salt")
        except (OSError, ValueError):
            _SALT = os.urandom(16)
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                             0o600)
                with os.fdopen(fd, "w") as f:
                    f.write(_SALT.hex())
            except OSError:
                pass
    return _SALT


def unit_id(scope, name):
    """The opaque id of a unit: twelve hex digits of a keyed hash of its
    kind and name. The name goes in, only the digits come out."""
    import hashlib
    import hmac
    return hmac.new(_salt(), f"{scope}\0{name}".encode(),
                    hashlib.sha256).hexdigest()[:12]


def resourced(phase, allocated, used, memory=None, connections=None):
    """`model.Model.observe`: the workers the model allotted and the
    workers the phase ran, and the memory they hold."""
    return _emit("resources", phase=phase,
                 workers_allocated=min(allocated, 10 ** 5)
                 if allocated is not None else None,
                 workers_used=min(used, 10 ** 5) if used is not None
                 else None,
                 connections=min(connections, 10 ** 5)
                 if connections is not None else None,
                 memory=bucket(memory) if memory else None,
                 cpus=min(os.cpu_count() or 1, 10 ** 4))


#: how an action `decide.recovery` chose reads as what was done
_DONE = {"reconcile": "reconcile", "replan": "fallback",
         "isolate": "fallback", "stop": "gave up"}


def recovered(fault, path, tries, outcome, error=None):
    """`faults.retried`: a class of failure (`faults.CATALOGUE`), the
    action chosen for it, the tries made and how it ended. The exception
    is read by its type only (`error_category`)."""
    return _emit("recovery", fault=fault, path=path,
                 action=_DONE.get(path, "retry"), outcome=outcome,
                 attempts=min(tries, 10 ** 4),
                 error=error_category(error) if error is not None
                 else None)


#: units named by one check at most; the ones that did not compare first
UNITS_KEPT = 200


def _unit_scope(record):
    """(kind, name) of the unit a record answers for."""
    scope = str(record.get("scope", "")).split()
    name = scope[0] if scope else ""
    if record.get("range"):
        return "range", f"{name}\0{record.get('range')}"
    return ("table" if "." in name.replace("(migra)", "") else
            "database"), name


def verified(records, decision):
    """`verdict.summarize`: how much of the scope a check compared and
    the verdict. The guarantees the ways reached are named only for a
    check that came out the same. Each unit is then kept by its opaque
    id with its outcome and the guarantees its own answer names."""
    total = len(records)
    same = sum(1 for r in records if r.get("status") == "ok")
    reached = (sorted(_CURRENT.reached) if _CURRENT is not None
               and decision.status == "same" else ())
    got = _emit("coverage", phase="verify", verified=coverage(same, total),
                guarantees=tuple(reached)[:16],
                completion=completion(decision.exit),
                units=bucket(total))
    try:
        _units_checked(records)
    except Exception:  # noqa: BLE001 - telemetry never stops a run
        pass
    return got


def _units_checked(records):
    on, _ = state()
    if not on:
        return
    allowed = set(_guarantees())
    statuses = set(_statuses())
    ordered = sorted(records, key=lambda r: r.get("status") == "ok")
    for r in ordered[:UNITS_KEPT]:
        status = r.get("status")
        if status not in statuses:
            continue
        kind, name = _unit_scope(r)
        own = {g for g in (r.get("guarantees") or ())
               if isinstance(g, str)} & allowed
        _emit("unit", phase="verify", scope=kind,
              unit=unit_id(kind, name), outcome=status,
              guarantees=tuple(sorted(own))[:16])


def intervened(did, question=None, phase=None):
    """`autonomous.ask`: the operator was asked, or answered, a question
    of `autonomous.QUESTIONS`, by its key alone."""
    return _emit("intervention", phase=phase, did=did, question=question)


def crashed(command, error):
    """`cli._stopped`: an unexpected stop, by the exception's type and the
    phase its command works in. The traceback is the local debug log's."""
    return _emit("crash", command=command if command in _commands()
                 else "other", phase=PHASE_OF.get(command, "report"),
                 error=error_category(error))


# ---- the library's account (`tooluse`, backlog 0f) -----------------------
# what of the object store's client `push` and `receive` use, read off this
# module

_S3 = ("put_object", "get_object", "list_objects_v2", "IfNoneMatch",
       "ContentType", "ContinuationToken")


def _account():
    from . import tooluse
    tooluse.register(
        "boto3-s3-telemetry", licence="Apache-2.0", release="1.40",
        declared={"s3": frozenset(_S3)}, options={},
        reads=(("telemetry.py",), {("s3", o): o for o in _S3}))


_account()
