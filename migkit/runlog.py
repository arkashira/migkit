"""The run log: every event of a run as one JSON line, in migkit's words
(audit 2026-10-01, LG5, LG12, LG13).

Measured before it: no run log at all - 230 lines printed to the console,
324 `log(...)` and 246 `say(...)` callbacks, `logging` used only to
silence three libraries - so what a run did could not be read back, the
run id stamped on the evidence was printed nowhere, and the warnings of
the libraries under the engines reached the terminal raw, their own
names in them.

Each run of a command on a hop (`session.step`) writes
`reports/<hop>/runs/<run>.jsonl`, a line an event:

    {"ts": "2026-10-01T02:03:04.567+00:00", "run": "...", "hop": "...",
     "db": "...", "table": "...", "phase": "...", "level": "info",
     "event": "begin", "msg": "...", ...facts}

`ts` in UTC with its zone; `level` one of `LEVELS`; every text through
the one cleaner of what migkit writes (`egress.words`: no secret, no
program named). A library's warning is one of its lines, not the
terminal's (`_Library`). The diagnostics bundle carries the run logs.

The append-only files beside a hop's reports - the record
(`changelog.jsonl`), the digests, the choices, each database's
`commands.log` - are rotated at the start of a run once one is larger
than `ROTATE_BYTES` or began more than `ROTATE_DAYS` ago: its lines move
to a part of their own (`digests.<stamp>.jsonl`) and the file starts
again. The record's new part starts with the head of the chain it
continues - the last entry's hash and the old part's digest - so each
part verifies alone and the parts verify as one chain (`verify_parts`).
The run logs themselves are kept to the newest `KEEP_RUNS`. Readers that
want the last lines read from the end (`last_lines`); readers of a
hop's whole history read every part (`parts`).

What a run added to those files is known without reading them whole: the
size of each is kept when the run begins (`appended`), so the evidence
export at the end of every command - each per-minute "not due" tick of a
schedule included - reads only what the run wrote.
"""
import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from . import durable
from .util import stamp

DIR = "runs"
LEVELS = ("debug", "info", "warn", "error")
FIELDS = ("ts", "run", "hop", "db", "table", "phase", "level", "event",
          "msg")

#: a part is started once the file is this large...
ROTATE_BYTES = 32 * 2 ** 20
#: ...or its first line is this many days old
ROTATE_DAYS = 30
#: run logs kept per hop, the newest
KEEP_RUNS = 500

#: the files beside a hop's reports rotated as a run begins: at the hop's
#: root, and in each database's directory
AT_ROOT = ("changelog.jsonl", "digests.jsonl", "decisions.jsonl")
PER_DB = ("commands.log",)
#: of those, the chained ones: a part begins with the head it continues
CHAINED = ("changelog.jsonl",)
#: the files a run's evidence is read from, whose sizes are kept as it
#: begins (`appended`)
MARKED = ("changelog.jsonl", "digests.jsonl", "decisions.jsonl")

_LOCK = threading.Lock()
#: the runs in progress in this process, innermost last
_STACK = []


class RunLog:
    """One run's log, beside the hop's reports."""

    def __init__(self, hop, run, command=None):
        self.hop, self.run, self.command = hop, run, command
        self.path = hop.report_dir() / DIR / f"{run}.jsonl"
        self.began = time.time()
        #: {file name: its size as the run began}, for `appended`
        self.marks = {}
        self.lock = threading.Lock()

    def write(self, event, msg="", level="info", db=None, table=None,
              phase=None, **facts):
        from . import egress
        if level not in LEVELS:
            raise ValueError(f"{level!r} is not a level of the run log")
        line = {"ts": stamp(), "run": self.run, "hop": self.hop.name,
                "db": db, "table": table, "phase": phase, "level": level,
                "event": str(event), "msg": str(msg or "")}
        for k, v in facts.items():
            if k not in line and v is not None:
                line[k] = v
        text = json.dumps(egress.facts(self.hop, line), default=str)
        with self.lock:
            # a line whole or not at all, readable by this user only; not
            # synced a line - the log is what a run said, not its state
            durable.append_line(self.path, text, sync=False)


def current():
    """The run log of the run in progress here, or None."""
    with _LOCK:
        return _STACK[-1] if _STACK else None


def event(name, msg="", level="info", **kw):
    """An event of the run in progress, where there is one; nothing
    otherwise. Never raises: the log of a run is not the run."""
    got = current()
    if got is None:
        return
    try:
        got.write(name, msg, level=level, **kw)
    except Exception:  # noqa: BLE001 - the run's own answer stands
        pass


def begin(hop, run, command=None):
    """The run log of a new run of `command` on `hop`: the hop's
    append-only files rotated where due, their sizes kept, library
    records routed into it, and its first line written."""
    log = RunLog(hop, run, command)
    root = hop.report_dir()
    try:
        housekeep(root)
    except Exception as e:  # noqa: BLE001 - said; the run goes on
        log.write("housekeeping", "the reports' files could not be"
                  f" rotated ({type(e).__name__})", level="warn")
    for name in MARKED:
        try:
            log.marks[name] = (root / name).stat().st_size
        except OSError:
            log.marks[name] = 0
    try:
        log.marks["verdict.json"] = (root / "verdict.json").stat().st_mtime
    except OSError:
        log.marks["verdict.json"] = None
    with _LOCK:
        _STACK.append(log)
        if len(_STACK) == 1:
            _Library.install()
    event("begin", f"{command or 'a command'} on {hop.name}",
          command=command)
    return log


def end(log, outcome, seconds=None, why=None, code=0):
    """The run's last line - `outcome` "done" (with the exit `code` a
    verdict ended it with), "stopped" or "interrupted" - and the run
    taken off."""
    if log is None:
        return
    try:
        log.write("end", why or outcome,
                  level="error" if outcome == "stopped" else "info",
                  outcome=outcome, exit=code or None,
                  seconds=None if seconds is None else round(seconds, 3))
    except Exception:  # noqa: BLE001 - the run's own answer stands
        pass
    with _LOCK:
        if log in _STACK:
            _STACK.remove(log)
        if not _STACK:
            _Library.remove()


def of(hop, run):
    """The run log of `run` on `hop` in progress here, or None."""
    with _LOCK:
        for got in reversed(_STACK):
            if got.run == run and got.hop.name == hop.name:
                return got
    return None


def appended(hop, run, name):
    """The lines the run `run` added to the hop's file `name` (one of
    `MARKED`), read from where the file stood as the run began - or None
    where the run is not one this process began, and the file must be
    read whole."""
    log = of(hop, run)
    if log is None or name not in log.marks:
        return None
    path = hop.report_dir() / name
    try:
        with open(path, "rb") as f:
            size = f.seek(0, os.SEEK_END)
            start = log.marks[name] if log.marks[name] <= size else 0
            f.seek(start)
            raw = f.read()
    except OSError:
        return []
    out = []
    for line in raw.decode("utf-8", "replace").splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def verdict_written(hop, run):
    """Whether the verdict file changed since the run `run` began; None
    where the run is not one this process began."""
    log = of(hop, run)
    if log is None:
        return None
    try:
        now = (hop.report_dir() / "verdict.json").stat().st_mtime
    except OSError:
        return False
    return log.marks.get("verdict.json") != now


# ---- reading from the end, and the parts of a rotated file ----------------

def last_lines(path, n, chunk=65536):
    """The last `n` lines of a file, read from its end: what a reader of
    the latest entries needs, whatever the file's size."""
    if n <= 0:
        return []
    try:
        with open(path, "rb") as f:
            end = f.seek(0, os.SEEK_END)
            got, at = b"", end
            while at > 0 and got.count(b"\n") <= n:
                step = min(chunk, at)
                at -= step
                f.seek(at)
                got = f.read(step) + got
    except OSError:
        return []
    return got.decode("utf-8", "replace").splitlines()[-n:]


def _part_name(path, when):
    stem, dot, ext = path.name.partition(".")
    return path.with_name(f"{stem}.{when}.{ext}" if dot else f"{stem}.{when}")


def parts(path):
    """The rotated parts of `path`, oldest first, then `path` itself."""
    stem, dot, ext = path.name.partition(".")
    pattern = f"{stem}.*.{ext}" if dot else f"{stem}.*"
    try:
        older = sorted(p for p in path.parent.glob(pattern)
                       if p != path and len(p.name.split(".")) ==
                       len(path.name.split(".")) + 1)
    except OSError:
        older = []
    return older + ([path] if path.exists() else [])


def _began(path):
    """When a file's first line was written, or None where it cannot be
    read: the `ts` or `at` of a JSON line, or a line's leading stamp."""
    try:
        with open(path, "rb") as f:
            first = f.readline().decode("utf-8", "replace").strip()
    except OSError:
        return None
    text = None
    try:
        got = json.loads(first)
        if isinstance(got, dict):
            text = str(got.get("ts") or got.get("at") or "")
    except ValueError:
        text = first[:19]
    if not text:
        return None
    try:
        at = datetime.fromisoformat(text[:32].replace(" ", "T"))
    except ValueError:
        try:
            at = datetime.fromisoformat(text[:19].replace(" ", "T"))
        except ValueError:
            return None
    return at.timestamp()


def due(path, now=None, most_bytes=None, most_days=None):
    """Whether `path` should start a new part."""
    most_bytes = ROTATE_BYTES if most_bytes is None else most_bytes
    most_days = ROTATE_DAYS if most_days is None else most_days
    try:
        size = path.stat().st_size
    except OSError:
        return False
    if size == 0:
        return False
    if size >= most_bytes:
        return True
    began = _began(path)
    now = time.time() if now is None else now
    return began is not None and now - began > most_days * 86400


def rotate(path, chained=False):
    """`path`'s lines moved to a part of their own and the file started
    again; the part's path, or None where there was nothing to move.

    A file its writers open for each line is renamed: a writer that has
    it open finishes its line in the part, and the next opens a new file
    - no line is lost. The record, chained, is copied and emptied in
    place under the lock its writers take (`audit.append`), so no entry
    is written between the two, and starts again with the head it
    continues: the part's name, its entries, the last entry's hash and
    the part's SHA-256, itself an entry of the chain."""
    import fcntl
    when = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    part = _part_name(path, when)
    if not chained:
        try:
            os.rename(path, part)
        except OSError:
            return None
        return part
    with open(path, "r+b") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            raw = f.read()
            if not raw:
                return None
            fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as out:
                out.write(raw)
                out.flush()
                os.fsync(out.fileno())
            f.seek(0)
            f.truncate()
            f.write((json.dumps(_carried(part.name, raw)) + "\n").encode())
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
    return part


def _carried(name, raw):
    """The entry a rotated record starts again with."""
    from . import audit, session
    lines = raw.decode("utf-8", "replace").splitlines()
    head = ""
    for line in reversed(lines):
        try:
            head = json.loads(line).get("hash", "") or ""
            break
        except ValueError:
            continue
    body = {"at": time.strftime("%F %T"), "op": "rotated",
            "session": session.current(), "from": name,
            "entries": len(lines), "head": head,
            "sha256": hashlib.sha256(raw).hexdigest(), "prev": ""}
    body["hash"] = hashlib.sha256(audit._canonical(body).encode()) \
        .hexdigest()
    return body


def verify_parts(path):
    """(entries, chained, broken, parts) of a rotated record read as one:
    each part verified (`audit.verify`), and each part after the first
    starting with the head of the one before - its digest and its last
    hash."""
    from . import audit
    got = parts(path)
    entries = chained = 0
    before = None
    for p in got:
        n, c, broken = audit.verify(p)
        entries, chained = entries + n, chained + c
        if broken:
            return entries, chained, f"{p.name}: {broken}", len(got)
        if before is not None:
            first = (last_first(p) or {})
            raw = before.read_bytes()
            if first.get("op") != "rotated" or first.get("sha256") != \
                    hashlib.sha256(raw).hexdigest() or first.get("from") \
                    != before.name:
                return entries, chained, (f"{p.name} does not continue"
                                          f" {before.name}: one was changed"
                                          " or taken out"), len(got)
        before = p
    return entries, chained, "", len(got)


def last_first(path):
    """The first line of a file as JSON, or None."""
    try:
        with open(path, "rb") as f:
            return json.loads(f.readline() or b"null")
    except (OSError, ValueError):
        return None


def housekeep(root, now=None):
    """The hop's append-only files rotated where due, and its run logs
    kept to the newest `KEEP_RUNS`."""
    rotated = []
    for name in AT_ROOT:
        p = root / name
        if due(p, now):
            rotated.append(rotate(p, chained=name in CHAINED))
    try:
        dbs = [d for d in root.iterdir() if d.is_dir() and d.name != DIR]
    except OSError:
        dbs = []
    for d in dbs:
        for name in PER_DB:
            p = d / name
            if due(p, now):
                rotated.append(rotate(p))
    runs = root / DIR
    try:
        logs = sorted(runs.glob("*.jsonl"), key=lambda p: p.stat().st_mtime)
    except OSError:
        logs = []
    for old in logs[:-KEEP_RUNS] if len(logs) > KEEP_RUNS else []:
        old.unlink(missing_ok=True)
    return [p for p in rotated if p is not None]


# ---- the libraries' own records ----------------------------------------------

class _Library(logging.Handler):
    """A library's warning or error, a line of the run log instead of the
    terminal's: without a handler of migkit's, Python prints them to
    stderr as they come, the library's name first. Python's own
    warnings (`warnings.warn`) come the same way."""

    _ONE = None
    LEVEL = {logging.DEBUG: "debug", logging.INFO: "info",
             logging.WARNING: "warn", logging.ERROR: "error",
             logging.CRITICAL: "error"}

    def emit(self, record):
        try:
            said = record.getMessage()
        except Exception:  # noqa: BLE001 - a record that cannot be read
            said = str(record.msg)
        level = self.LEVEL.get(record.levelno, "warn")
        event("library", said.splitlines()[0][:500] if said else "",
              level=level)

    @classmethod
    def install(cls):
        if cls._ONE is None:
            cls._ONE = cls(logging.WARNING)
            logging.getLogger().addHandler(cls._ONE)
            logging.captureWarnings(True)

    @classmethod
    def remove(cls):
        if cls._ONE is not None:
            logging.getLogger().removeHandler(cls._ONE)
            logging.captureWarnings(False)
            cls._ONE = None
