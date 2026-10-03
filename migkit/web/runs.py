"""Runs started from the view or the API: the command line's own
commands, run whole in a process of their own (backlog W2).

No second logic: a run is `migkit <command> <hop> <options>` exactly as
an operator types it - the same checks, the same guards and approvals,
the same decision engine, the same evidence at the end - run by
`runner` in a process of its own, so a run of hours outlives a restart
of the view, and is stopped the way a terminal stops one (SIGTERM, as
ctrl-c). What the view adds is only what a terminal gives for free:

* who started it - the run carries their name (`MIGKIT_ACTOR`), which
  the hop's record writes beside each entry and approvals take as the
  one who asked, so they cannot approve their own step;
* its id - the run's own (`MIGKIT_RUN_ID`, `session`), so its evidence
  bundle is found from the run's page;
* its output, kept beside the hop's reports (0600), shown with the
  hop's secrets hidden (`redact`) and no program named (`export.words`);
* one run that writes on a hop at a time, from the view - the command's
  own lease still holds against runs from a terminal.

What a person may start is a fixed list (`ACTIONS`): the command and
the options each takes, every value checked against what the option
takes before it becomes an argument - nothing typed reaches a shell or
turns into an option of its own.
"""
import json
import os
import re
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

#: where a hop's runs from the view are kept, under its reports
DIR = "view-runs"
#: a name as the options take it: a database, a table, a state's tag
NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.$@:-]{0,127}$")
#: the checks `check --only` names
ONLY = re.compile(r"^[a-z_]+(,[a-z_]+)*$")
#: the most of a run's output one read returns
READ_MOST = 256 * 1024
#: seconds a run may stand queued before its process says it started
QUEUED_MOST = 60


class Refused(Exception):
    """A run that is not started, with the reason."""

    def __init__(self, why, status=400):
        super().__init__(why)
        self.status = status


@dataclass(frozen=True)
class Option:
    name: str
    kind: str = "name"          #: name, choice, flag, only
    choices: tuple = ()
    what: str = ""
    required: bool = False


@dataclass(frozen=True)
class Action:
    """One thing a person may start: the command it runs, the options it
    takes, whether it writes (then one at a time on the hop), and what it
    does in words."""
    name: str
    what: str
    options: tuple = field(default_factory=tuple)
    need: str = "operator"

    def argv(self, hop_name, params):
        """The command line, every value checked."""
        params = dict(params or {})
        unknown = set(params) - {o.name for o in self.options} - {"action"}
        if unknown:
            raise Refused(f"{self.name} takes no {', '.join(sorted(unknown))}"
                          f" - it takes {', '.join(o.name for o in self.options) or 'nothing'}")
        got = {}
        for o in self.options:
            v = params.get(o.name)
            if v in (None, "", False):
                if o.required:
                    raise Refused(f"{self.name} needs {o.name}: {o.what}")
                continue
            if o.kind == "flag":
                if v is not True:
                    raise Refused(f"{o.name} is true or false")
                got[o.name] = True
            elif o.kind == "choice":
                if v not in o.choices:
                    raise Refused(f"{o.name} is one of {', '.join(o.choices)}")
                got[o.name] = v
            elif o.kind == "only":
                if not isinstance(v, str) or not ONLY.match(v):
                    raise Refused(f"{o.name} is a list of checks: counts,data")
                got[o.name] = v
            else:
                if not isinstance(v, str) or not NAME.match(v):
                    raise Refused(f"{o.name}: {v!r} is not a name this option"
                                  " takes (letters, digits and _ . $ @ : -,"
                                  " not starting with -)")
                got[o.name] = v
        return BUILD[self.name](hop_name, got), got

    def writes(self, got):
        return WRITES[self.name](got)


def _opt(flag, value):
    return [flag, value] if value else []


BUILD = {
    "plan": lambda h, g: (["move", h, "--mode", g.get("mode") or "full"]
                          + _opt("--db", g.get("db"))
                          + _opt("--table", g.get("table"))),
    "assess": lambda h, g: ["assess", h],
    "check": lambda h, g: (["check", h] + _opt("--db", g.get("db"))
                           + _opt("--table", g.get("table"))
                           + _opt("--only", g.get("only"))
                           + (["--deep"] if g.get("deep") else [])),
    "move": lambda h, g: (["move", h, "--mode", g.get("mode") or "full"]
                          + _opt("--db", g.get("db"))
                          + _opt("--table", g.get("table")) + ["--go"]),
    "sync": lambda h, g: (["sync", h] + _opt("--db", g.get("db"))
                          + _opt("--kind", g.get("kind"))
                          + _opt("--mode", g.get("mode"))
                          + _opt("--on-conflict", g.get("on_conflict"))
                          + _opt("--tag", g.get("tag"))
                          + (["--go"] if g.get("go") else
                             ["--apply"] if g.get("apply") else [])),
    "cutover": lambda h, g: (["move", h, "--mode", "cdc", "--drop"]
                             + _opt("--db", g.get("db"))
                             + (["--go"] if g.get("go") else [])),
    "rollback": lambda h, g: (["rollback", h, "--db", g["db"]]
                              + _opt("--state", g.get("state"))
                              + (["--apply"] if g.get("apply") else [])),
    "report": lambda h, g: ["report", h],
}

WRITES = {
    "plan": lambda g: False, "assess": lambda g: False,
    "check": lambda g: False, "report": lambda g: False,
    "move": lambda g: True,
    "sync": lambda g: bool(g.get("go") or g.get("apply")),
    "cutover": lambda g: bool(g.get("go")),
    "rollback": lambda g: bool(g.get("apply")),
}

_DB = Option("db", what="one database of the hop")
_TABLE = Option("table", what="one table, schema.table")
_MODES = ("full", "cdc", "full+cdc")

ACTIONS = {a.name: a for a in (
    Action("plan", "the plan: what the move would do to each side, how long"
                   " it would take with its spread, the case it is - nothing"
                   " is written to either side",
           (Option("mode", "choice", _MODES, "full, cdc or full+cdc"),
            _DB, _TABLE)),
    Action("assess", "readiness: what would stop a move, before anything"
                     " moves"),
    Action("check", "the read-only comparison of the two sides",
           (_DB, _TABLE, Option("only", "only", what="checks, e.g."
                                                     " counts,data"),
            Option("deep", "flag", what="the deep checks too"))),
    Action("move", "the move, as the plan says it",
           (Option("mode", "choice", _MODES, "full, cdc or full+cdc"),
            _DB, _TABLE)),
    Action("sync", "the target made equal to the source: dry run, --apply"
                   " or --go with a restore point",
           (_DB, Option("kind", "choice", ("sequences", "rows", "schema",
                                           "all"), "what is repaired"),
            Option("mode", "choice", ("reconcile", "verify", "seed",
                                      "stream", "migrate"), "the flow"),
            Option("on_conflict", "choice", ("source-wins", "keep-target"),
                   "rows that differ"),
            Option("tag", what="a label for the restore point"),
            Option("apply", "flag", what="execute the repair plan"),
            Option("go", "flag", what="checkpointed, with rollback state"))),
    Action("cutover", "the cutover: the stream torn down and the"
                      " application moved - the hop's approvals first; a"
                      " dry run without go",
           (_DB, Option("go", "flag", what="run it, not only show it"))),
    Action("rollback", "a saved state restored on the target - a dry run"
                       " without apply",
           (Option("db", what="the database", required=True),
            Option("state", what="the state's time or tag, the latest"
                                 " without"),
            Option("apply", "flag", what="restore it"))),
    Action("report", "the hop's evidence exported again"),
)}


def _dir(hop):
    from .. import durable
    return durable.private_dir(hop.report_dir() / DIR)


def _id():
    return time.strftime("%Y%m%dT%H%M%S") + "-" + secrets.token_hex(3)


def _alive(rec):
    from ..tailctl import process_alive
    if not rec.get("pid"):
        return True
    return process_alive(rec.get("host") or "", rec["pid"])


def state_of(rec):
    """A run's state as it stands: `queued`, `running`, `ended`, or
    `lost` - marked running, its process gone without saying how it
    ended."""
    got = rec.get("state") or "queued"
    if got in ("running", "queued") and rec.get("pid") and not _alive(rec):
        return "lost"
    if got == "queued" and not rec.get("pid") \
            and time.time() - float(rec.get("asked") or 0) > QUEUED_MOST:
        # its process never said it started
        return "lost"
    return got


def read(hop, rid):
    from .. import durable
    if not NAME.match(str(rid)):
        return None
    path = _dir(hop) / f"{rid}.json"
    try:
        rec = durable.read_state(path, "cache")
    except SystemExit:
        return None
    if not isinstance(rec, dict):
        return None
    rec["state"] = state_of(rec)
    if rec["state"] == "ended" and isinstance(rec.get("exit"), int):
        # what the exit code means, in the command line's own table
        from ..verdict import exit_codes
        rec["ended_as"] = [{"as": word, "means": means} for word, means in
                           dict(exit_codes()).get(rec["exit"], [])]
    stop = _dir(hop) / f"{rid}.cancel.json"
    if stop.exists():
        rec["cancel"] = durable.read_state(stop, "cache")
    return rec


def listing(hop, most=50):
    out = []
    for p in sorted(_dir(hop).glob("*.json"), reverse=True):
        if p.name.endswith(".cancel.json"):
            continue
        got = read(hop, p.stem)
        if got is not None:
            out.append(got)
        if len(out) >= most:
            break
    return out


def writing(hop):
    """The run from the view that writes on this hop now, or None."""
    for rec in listing(hop):
        if rec.get("writes") and rec["state"] in ("queued", "running"):
            return rec
    return None


def _reap(proc):
    threading.Thread(target=proc.wait, daemon=True).start()


def start(hop, action, params, principal, through):
    """A run started; its record. Refused where the action or a value is
    not one a person may start, or another run writes on the hop."""
    from .. import audit, durable
    spec = ACTIONS.get(action)
    if spec is None:
        raise Refused(f"{action!r} is not a run the view starts - one of"
                      f" {', '.join(ACTIONS)}", 404)
    argv, got = spec.argv(hop.name, params)
    writes = spec.writes(got)
    if writes:
        busy = writing(hop)
        if busy is not None:
            raise Refused(f"a run that writes on {hop.name} is going on"
                          f" ({busy['action']}, started by {busy['by']});"
                          " wait for it, or stop it", 409)
    rid = _id()
    where = _dir(hop)
    rec = {"id": rid, "hop": hop.name, "action": action, "params": got,
           "argv": argv, "writes": writes, "by": principal.id,
           "through": through, "run": rid, "state": "queued",
           "asked": time.time(), "host": socket.gethostname()}
    path = where / f"{rid}.json"
    durable.write_json(path, rec, indent=1)
    log = os.open(str(where / f"{rid}.log"),
                  os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    env = dict(os.environ)
    env.pop("MIGKIT_EXPLAIN", None)
    env.update({"MIGKIT_ACTOR": principal.id, "MIGKIT_RUN_ID": rid,
                "PYTHONUNBUFFERED": "1", "COLUMNS": "140",
                "NO_COLOR": "1"})
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "migkit.web.runner", str(path)],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            env=env, start_new_session=True)
    except OSError as e:
        rec.update(state="ended", exit=None,
                   why=f"it could not be started ({e.strerror or e})")
        durable.write_json(path, rec, indent=1)
        raise Refused(rec["why"], 500)
    finally:
        os.close(log)
    _reap(proc)
    # the runner writes its own pid as it starts; the one here is for
    # this answer only - writing it too would race the runner's own
    rec["pid"] = proc.pid
    audit.append(hop.report_dir() / "changelog.jsonl",
                 {"op": "view-run", "action": action, "run": rid,
                  "argv": ["migkit", *argv], "by": principal.id,
                  "issuer": principal.issuer, "through": through})
    return rec


def cancel(hop, rid, principal, through):
    """A running run asked to stop, as ctrl-c at its terminal would; its
    record, or None where there is no such run."""
    from .. import audit, durable
    rec = read(hop, rid)
    if rec is None:
        return None
    if rec["state"] not in ("queued", "running"):
        raise Refused(f"the run has {rec['state']} already", 409)
    if rec.get("host") and rec["host"] != socket.gethostname():
        raise Refused("the run is on another machine; stop it there", 409)
    durable.write_json(_dir(hop) / f"{rid}.cancel.json",
                       {"by": principal.id, "at": time.time(),
                        "through": through})
    pid = int(rec.get("pid") or 0)
    if pid:
        try:
            os.killpg(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    audit.append(hop.report_dir() / "changelog.jsonl",
                 {"op": "view-run-stop", "run": rid, "by": principal.id,
                  "issuer": principal.issuer, "through": through})
    return read(hop, rid)


def output(hop, rid, offset=0):
    """(text from `offset`, the offset after it) of a run's output, the
    hop's secrets hidden and no program named."""
    from .. import export
    from ..redact import Redactor
    if not NAME.match(str(rid)):
        return "", 0
    path = _dir(hop) / f"{rid}.log"
    try:
        with open(path, "rb") as f:
            f.seek(max(0, int(offset)))
            raw = f.read(READ_MOST)
            end = f.tell()
    except OSError:
        return "", int(offset)
    # a line cut at the end of a read is read whole the next time
    if len(raw) == READ_MOST and b"\n" in raw:
        cut = raw.rfind(b"\n") + 1
        end -= len(raw) - cut
        raw = raw[:cut]
    text = raw.decode("utf-8", "replace")
    if text.strip():
        # the words filter trims what it is given; the lines around the
        # text are the output's own, and the next read goes on after them
        lead = text[:len(text) - len(text.lstrip())]
        trail = text[len(text.rstrip()):]
        text = lead + export.words(Redactor(hop)(text)) + trail
    return text, end


def evidence_of(hop, rec):
    """The run's own evidence bundle's directory name, where it left one."""
    from .. import export
    root = hop.report_dir() / export.DIR / str(rec.get("run") or "")
    return rec.get("run") if rec.get("run") and root.is_dir() else None


def verdict_of(hop, rec):
    """The verdict the run wrote, where it wrote one."""
    try:
        got = json.loads((hop.report_dir() / "verdict.json").read_text())
    except (OSError, ValueError):
        return None
    if isinstance(got, dict) and got.get("session") == rec.get("run"):
        return got.get("status")
    return None
