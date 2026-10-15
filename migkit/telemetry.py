"""Product telemetry: what migkit did, never what it carried (backlog
P0-PRODUCT-TELEMETRY).

An event is one of the typed kinds below (`KINDS`), each field drawn from
a closed set: an engine migkit knows, a command it has, a bucket, a
bounded number, a flag. There is no free text field, so a table's name, a
row's value, a statement, an address, a path or an error's words have
nowhere to go: the producer refuses an unknown field, a string outside its
set and a value of the wrong type (`Rejected`) - exclusion by type, not
redaction after the fact. Nothing is hashed in place of being left out.

Events are kept on this machine only, in a bounded spool under the state
directory (`spool`): a few files of a fixed size, the oldest dropped. They
are written by a background thread from a bounded queue; a full queue
drops the event and counts it, so a step never waits on telemetry and a
failure here never changes a run. Nothing is sent: no destination is
chosen, and the only `Sink` is the local one. `migkit telemetry show`
prints the state and exactly what is kept.

Off, the stricter first (`state`): the organisation's policy file
(`MIGKIT_TELEMETRY_POLICY`, `telemetry: off` or `egress: none`; one that
cannot be read counts as off), `MIGKIT_EGRESS` naming kinds without
`telemetry`, `MIGKIT_TELEMETRY=off`, and `migkit telemetry off`. Off stops
queueing and disposes of the spool.
"""
import atexit
import dataclasses
import json
import math
import os
import queue
import re
import threading
import time
from pathlib import Path

SCHEMA = 1

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
    ("Where it goes", "nowhere yet: no destination is chosen. Events are"
     " kept on this machine, in a spool of a fixed size under the state"
     " directory, and nothing is sent."),
    ("Default", "on: events are kept locally. `migkit telemetry show`"
     " prints exactly what is kept."),
    ("How to turn it off", "`migkit telemetry off`, `MIGKIT_TELEMETRY=off`,"
     " `MIGKIT_EGRESS=none`, or an organisation policy file"
     " (`MIGKIT_TELEMETRY_POLICY`, `/etc/migkit/policy.yaml` by default)"
     " saying `telemetry: off`. Off stops keeping events and deletes those"
     " already kept."),
)

FILE_BYTES = 128 * 1024      #: one spool file at most
FILES = 4                    #: spool files kept, the oldest dropped
EVENT_BYTES = 2048           #: one event at most, as written
QUEUED = 512                 #: events waiting for the writer at most
DEFAULT_POLICY = "/etc/migkit/policy.yaml"

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
                 "retried by hand", "repaired by hand")
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
    memory: str = one_of(_sizes, "bytes of memory used")
    cpus: int = number(10 ** 4, "processors on this machine")


@dataclasses.dataclass(frozen=True)
class Recovery:
    """A failure met and what was done about it."""
    phase: str = one_of(lambda: PHASES, "the phase")
    error: str = one_of(lambda: ERRORS, "the failure, by kind")
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


@dataclasses.dataclass(frozen=True)
class Intervention:
    """The operator stepping in."""
    phase: str = one_of(lambda: PHASES, "the phase")
    kind: str = one_of(lambda: INTERVENTIONS, "what they did")


KINDS = {"run": Run, "decision": Decision, "progress": Progress,
         "resources": Resources, "recovery": Recovery,
         "coverage": Coverage, "intervention": Intervention}


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


def make(kind, **fields):
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


def _policy():
    """What the organisation's policy file says: None where there is
    none, else (on, why)."""
    from . import envkeys
    path = Path(envkeys.text("MIGKIT_TELEMETRY_POLICY")
                or DEFAULT_POLICY).expanduser()
    if not path.exists():
        return None
    try:
        import yaml
        got = yaml.safe_load(path.read_text()) or {}
        if not isinstance(got, dict):
            raise ValueError
    except Exception:  # noqa: BLE001 - unreadable is the stricter answer
        return False, "the organisation's policy file cannot be read"
    off = ("none", "off", "false", "no", "0", False)
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
    return True, "on: events are kept on this machine, nothing is sent"


# ---- where events go ----------------------------------------------------------

class Sink:
    """Where kept events go. Only the local one exists: sending anywhere
    waits on a destination being chosen."""

    def put(self, lines):
        raise TypeError(f"{type(self).__name__} keeps nothing")

    def read(self):
        return []

    def dispose(self):
        return 0


class Local(Sink):
    """The spool: `FILES` files of `FILE_BYTES` at most, the newest
    `events-0.jsonl`; a write that would overflow it moves each file one
    along and drops the last."""

    def __init__(self, root=None):
        self.root = Path(root) if root is not None else spool()

    def _file(self, i):
        return self.root / f"events-{i}.jsonl"

    def _rotate(self):
        self._file(FILES - 1).unlink(missing_ok=True)
        for i in range(FILES - 2, -1, -1):
            if self._file(i).exists():
                self._file(i).replace(self._file(i + 1))

    def put(self, lines):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        for line in lines:
            data = (line + "\n").encode()
            head = self._file(0)
            if head.exists() and head.stat().st_size + len(data) \
                    > FILE_BYTES:
                self._rotate()
            fd = os.open(head, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                         0o600)
            with os.fdopen(fd, "ab") as f:
                f.write(data)

    def read(self):
        got = []
        for i in range(FILES - 1, -1, -1):
            try:
                text = self._file(i).read_text()
            except OSError:
                continue
            got += [json.loads(x) for x in text.splitlines() if x.strip()]
        return got

    def bytes(self):
        return sum(self._file(i).stat().st_size for i in range(FILES)
                   if self._file(i).exists())

    def dispose(self):
        gone = 0
        for p in self.root.glob("events-*.jsonl"):
            p.unlink(missing_ok=True)
            gone += 1
        return gone


# ---- the writer ---------------------------------------------------------------

_Q = queue.Queue(maxsize=QUEUED)
_LOCK = threading.Lock()
_WRITER = None
_DROPPED = 0
_DISPOSED = False


def _write():
    while True:
        got = [_Q.get()]
        while True:
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


def record(kind, **fields):
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
        record("decision",
               strategy=names.get(label, "other") if label else "other",
               reasons=tuple(sorted(reasons))[:16],
               guarantees=tuple(sorted(reached))[:16],
               ranked=min(len(getattr(got, "ranked", ()) or ()), 1000),
               passed=min(len(passed), 1000), fallback=fell,
               trial=bool(getattr(got, "trial", 0)))

    def ended(self, code=None, error=None):
        try:
            self._ended(code, error)
        except Exception:  # noqa: BLE001 - telemetry never stops a run
            pass

    def _ended(self, code, error):
        took = max(time.time() - self.began, 0.0)
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
