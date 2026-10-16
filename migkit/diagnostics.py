"""A bundle to report a problem with, holding nothing to hand over.

`MIGKIT_DIAGNOSE=<file>.zip migkit doctor` writes it (an environment
variable, so `doctor` keeps its one flag). In it:

* `about.json`: migkit's version, Python, the platform, and what each
  engine can do here (`capabilities.matrix`)
* `hops.yaml`: the configuration, with every password, token, key and
  notification address replaced, and credentials taken out of any
  address that carries them
* for each hop: the probes it accumulated (`probes.json`: what each one
  measured, when, and how fresh it is, never the value read), its last verdict with every finding's detail removed -
  the detail is where keys and values are - its change log, the ways the
  engine chose and why (`decisions.jsonl`), the logs of its last runs
  (`runs/`, `runlog`) and each database's `commands.log`, what was run

What is removed is removed before anything is written: a secret that
never reaches the file cannot be left in it. One cleaner does it for
every file (`redact`): by the keys that name a secret, by the shape a
secret has wherever it stands (an address's credentials, `password=`,
`IDENTIFIED BY`), and by value - every secret written out in the
configuration and every secret variable of the environment, in each form
it reaches text in. Measured before it had one: `mysql://app:pa/ss@db`
kept its password and `postgres://u:p@ss@h` kept `ss`; the run's own
record of commands, "what someone attaches to a bug report", was left
out.
"""
import json
import os
import platform
import threading
import time
import zipfile
from dataclasses import dataclass, field

HIDDEN = "<removed>"
#: the last lines of a hop's long files, and the last run logs, carried
LINES = 200
RUNS = 20


def scrub(value, key=""):
    """The configuration with its secrets taken out, at any depth: the
    one redactor's (`redact.scrub`), which also finds a password holding
    `/` or `@` in an address - `mysql://app:pa/ss@db` was left whole and
    `postgres://u:p@ss@h` came out `<removed>@ss@h`."""
    from . import redact
    return redact.scrub(value, HIDDEN, key)


def _written(raw):
    """Every secret written out in the configuration - the value of a key
    that names one, and not a reference to where it is kept - for the
    cleaner to find by value in the files carried."""
    from . import addresses
    from .redact import HOLDS_SECRET
    out = []

    def walk(value, key=""):
        if isinstance(value, dict) and key == "notify":
            # a receiver's address is its own password
            walk(value.get("to"), "notify")
        elif isinstance(value, dict):
            for k, v in value.items():
                walk(v, str(k))
        elif isinstance(value, list):
            for v in value:
                walk(v, key)
        elif isinstance(value, str) and value and (
                HOLDS_SECRET.search(key) or key == "notify") \
                and not addresses.is_reference(value):
            out.append(value)
    walk(raw)
    return out


def _verdict(path):
    try:
        got = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    for finding in got.get("findings") or []:
        # the detail names keys and shows values; the rest says what kind
        # of finding it is and where
        finding.pop("detail", None)
        finding.pop("fix_hint", None)
    return got


def _probe_rows(path):
    try:
        got = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return got if isinstance(got, dict) else None


def _last(path, n=LINES):
    from . import runlog
    return runlog.last_lines(path, n)


def write(target, conf, reports):
    """Writes the bundle to `target`; returns the names it holds."""
    import yaml

    from . import __version__, capabilities, runlog
    from .redact import Redactor
    names = []
    try:
        raw = yaml.safe_load(conf.read_text()) or {}
    except (OSError, yaml.YAMLError):
        raw = None
    clean = Redactor(secrets=_written(raw), mask=HIDDEN, userinfo="all",
                     environ=os.environ)

    def put(name, text):
        z.writestr(name, clean(text))
        names.append(name)

    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
        put("about.json", json.dumps({
            "migkit": __version__, "python": platform.python_version(),
            "platform": platform.platform(),
            "written": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "capabilities": capabilities.matrix()}, indent=1))
        if raw is not None:
            put("hops.yaml", yaml.safe_dump(scrub(raw), sort_keys=False))
        hops = (raw or {}).get("hops") or {}
        for hop in sorted(hops):
            where = reports / hop
            seen = _probe_rows(where / PROBES_FILE)
            if seen is not None:
                put(f"{hop}/{PROBES_FILE}", json.dumps(seen, indent=1))
            got = _verdict(where / "verdict.json")
            if got is not None:
                put(f"{hop}/verdict.json", json.dumps(got, indent=1))
            for name in ("changelog.jsonl", "decisions.jsonl"):
                lines = []
                for line in _last(where / name):
                    try:
                        entry = scrub(json.loads(line))
                    except ValueError:
                        continue
                    # who ran it, and where, is not the problem's
                    for k in ("who", "host", "prev", "hash"):
                        entry.pop(k, None)
                    lines.append(json.dumps(entry))
                if lines:
                    put(f"{hop}/{name}", "\n".join(lines) + "\n")
            try:
                runs = sorted((where / runlog.DIR).glob("*.jsonl"),
                              key=lambda p: p.stat().st_mtime)[-RUNS:]
            except OSError:
                runs = []
            for run in runs:
                lines = []
                for line in _last(run, 10 * LINES):
                    try:
                        lines.append(json.dumps(scrub(json.loads(line))))
                    except ValueError:
                        continue
                put(f"{hop}/{runlog.DIR}/{run.name}", "\n".join(lines)
                    + "\n")
            try:
                dbs = sorted(d for d in where.iterdir() if d.is_dir())
            except OSError:
                dbs = []
            for d in dbs:
                log = d / "commands.log"
                if log.exists():
                    put(f"{hop}/{d.name}/commands.log",
                        "\n".join(_last(log)) + "\n")
    return names


# --- the probes of a run -------------------------------------------------------

#: where a hop's accumulated probes are kept, beside its reports
PROBES_FILE = "probes.json"


@dataclass
class Reading:
    """One probe's last answer: what was measured (`kind` of `scope`),
    when, how long it took, and whether it could be read at all."""
    kind: str
    scope: str
    value: object = None
    taken: float = 0.0
    took: float = 0.0
    ok: bool = True
    why: str = ""
    #: something cheap that changes when the answer would (a schema
    #: fingerprint, a server's boot time); a reading whose witness moved
    #: is stale whatever its age
    witness: object = None
    #: seconds between the times this probe's answer was seen to change
    moves: list = field(default_factory=list)
    reads: int = 1
    reused: int = 0
    #: how long the fact itself can hold, from how fast it can change
    #: (`HOLDS`): None until it is changed on purpose (a grant until it
    #: is revoked, a version until an upgrade), 0 for one read (a lag)
    holds: object = None


#: how long a probe of each kind holds by what it measures: a grant, a
#: version, a server setting, a feature switched on (change data capture,
#: a publication) hold until someone changes them, and that change ends
#: them through a witness or a consumer's own bound; a lag, a slot's
#: position, a service's running state hold for the read that took them.
#: The keys are (kind, `where(db, side, name)`), the shape the cutover's
#: own readings have ("open-work" of "db:side").
HOLDS = {"grant": None, "version": None, "setting": None, "feature": None,
         "publication": None, "slot": 0.0, "service": 0.0, "lag": 0.0}


def where(db, side, name=""):
    """The scope of a probe of one database on one side, and of what in it
    (`HOLDS`): "db:side" or "db:side:name" - one spelling for every owner
    that records or reads it."""
    return f"{db}:{side}" + (f":{name}" if name else "")


class ProbeAccumulator:
    """Every probe of a run in one place (audit 2026-10-01, AUD-H): grants,
    versions, settings, sizes, rates. A probe records what it measured and
    when; an owner that needs the same fact asks here before asking the
    server again (`read`), and what is stale is measured again - never
    handed back as if it were new, and never turned into an `ok` over what
    could not be read (`refusals`).

    How long a reading may be used is not a number written here. The
    consumer says how old an answer it can live with (`max_age`); a probe
    seen to change is held to half the shortest time it was seen to take
    between two changes; and a witness that moved ends a reading at once.
    """

    def __init__(self, clock=time.time):
        self._clock = clock
        self._lock = threading.RLock()
        self._seen = {}

    def record(self, kind, scope, value=None, took=0.0, ok=True, why="",
               witness=None, holds=..., paced=False):
        """Keeps one answer, with the time it was taken; returns it.
        `paced`: the answer was measured just now (`read`, `probed`), so a
        change from the last one is a measured pace of the fact - never
        shorter than one read of it. An answer handed in (`paced` False)
        is kept without one: two records close together say how often it
        was written here, not how fast it changes."""
        now = self._clock()
        with self._lock:
            before = self._seen.get((kind, scope))
            moves = list(before.moves) if before else []
            reads = before.reads + 1 if before else 1
            if holds is ...:
                holds = before.holds if before else None
            if paced and before and before.ok and ok and (
                    before.value != value or before.witness != witness):
                moves.append(max(now - before.taken, took, before.took))
            got = Reading(kind, scope, value, now, took, ok,
                          str(why).strip().splitlines()[-1][:160]
                          if str(why).strip() else "", witness, moves, reads,
                          before.reused if before else 0, holds)
            self._seen[(kind, scope)] = got
            return got

    def absorb(self, kind, scope, probes, value=None):
        """The result of a deep check's reads (`util.Probes`) as one
        reading: failed where any planned read was missed, with which."""
        said = probes.words()
        return self.record(kind, scope, value if value is not None else {
            "read": len(probes.read), "planned": len(probes.planned)},
            ok=not said, why=said)

    def get(self, kind, scope):
        with self._lock:
            return self._seen.get((kind, scope))

    def allowed_age(self, got, max_age=None):
        """How old `got` may be and still be used: the consumer's own
        bound, cut to half the shortest interval this probe was measured
        to change in and to how long the fact holds (`HOLDS`) - neither
        below the time the probe took to read. None where nothing bounds
        it (then only its witness and a failed read end it)."""
        pace = min((m for m in got.moves if m > 0), default=None)
        bounds = [b for b in (max_age,
                              max(pace / 2, got.took) if pace else None,
                              max(got.holds, got.took)
                              if got.holds is not None else None)
                  if b is not None]
        return min(bounds) if bounds else None

    def stale(self, kind, scope, max_age=None, witness=None):
        """Why the reading cannot be used now, or "" where it can."""
        got = self.get(kind, scope)
        if got is None:
            return "never measured"
        if not got.ok:
            return "could not be read" + (f" ({got.why})" if got.why else "")
        if witness is not None and got.witness != witness:
            return "what it was taken from has changed since"
        limit = self.allowed_age(got, max_age)
        age = self._clock() - got.taken
        if limit is not None and age > limit:
            return f"{age:.1f}s old, usable for {limit:.1f}s"
        return ""

    def read(self, kind, scope, measure, max_age=None, witness=None):
        """The answer of `measure()`, or the kept one while it is fresh.
        A measure that raises is kept as a failed reading and gives
        None: the older answer is not offered in its place."""
        with self._lock:
            if not self.stale(kind, scope, max_age, witness):
                got = self._seen[(kind, scope)]
                got.reused += 1
                return got.value
        began = self._clock()
        try:
            value = measure()
        except Exception as e:  # noqa: BLE001 - kept as the reason
            self.record(kind, scope, None, self._clock() - began, False,
                        str(e) or type(e).__name__, witness,
                        paced=True)
            return None
        self.record(kind, scope, value, self._clock() - began, True, "",
                    witness, paced=True)
        return value

    def probed(self, kind, scope, measure, witness=None, holds=...,
               keep=None):
        """`measure()` run now, its answer kept as a measured reading
        held as long as its kind holds (`HOLDS`), and handed back as it
        came; one that raises is kept failed, with why, and raised on - an
        owner's probe behaves as it did, and the next owner that needs the
        fact asks `read` before the server. `keep` turns the raw answer
        into the fact kept, so two owners that ask the same thing in
        different shapes keep one."""
        holds = HOLDS.get(kind) if holds is ... else holds
        began = self._clock()
        try:
            value = measure()
        except Exception as e:
            self.record(kind, scope, None, self._clock() - began, False,
                        str(e) or type(e).__name__, witness, holds, True)
            raise
        self.record(kind, scope, keep(value) if keep else value,
                    self._clock() - began, True, "", witness, holds, True)
        return value

    def refusals(self, needs, max_age=None):
        """The facts that stop a decision resting on `needs`, a list of
        (kind, scope): each as 'kind of scope: why', for the one that was
        never measured, failed, or is too old."""
        out = []
        for kind, scope in needs:
            why = self.stale(kind, scope, max_age)
            if why:
                out.append(f"{kind} of {scope}: {why}")
        return out

    def scopes(self, *kinds):
        """[(kind, scope)] of every reading kept, of these kinds."""
        with self._lock:
            return [k for k in sorted(self._seen) if k[0] in kinds]

    def rows(self, max_age=None):
        """Every reading as a dict - never its value, which may hold
        what a report must not: what, where, when, how long, how fresh."""
        now = self._clock()
        with self._lock:
            return [{"kind": g.kind, "scope": g.scope, "ok": g.ok,
                     "why": g.why, "taken": g.taken,
                     "age": round(now - g.taken, 3),
                     "took": round(g.took, 4), "reads": g.reads,
                     "reused": g.reused,
                     "fresh": not self.stale(g.kind, g.scope, max_age)}
                    for g in sorted(self._seen.values(),
                                    key=lambda g: (g.kind, g.scope))]

    def said(self, max_age=None):
        """Lines for `doctor` and a report: each probe, how long ago,
        and the ones that cannot be leaned on said first."""
        rows = self.rows(max_age)
        rows.sort(key=lambda r: (r["fresh"], r["kind"], r["scope"]))
        return [f"{r['kind']} of {r['scope']}: "
                + (f"unreadable - {r['why']}" if not r["ok"] else
                   f"{r['age']:g}s ago, took {r['took']:g}s"
                   + ("" if r["fresh"] else " - stale"))
                for r in rows]

    def save(self, path, max_age=None):
        from . import durable
        durable.write_json(path, {"saved": self._clock(),
                                  "probes": self.rows(max_age)})


_ACCUMULATORS = {}
_ACCUMULATORS_LOCK = threading.Lock()


def accumulator(hop):
    """The one accumulator of a hop in this process: the owners of a run
    read and record in the same place."""
    name = getattr(hop, "name", str(hop))
    with _ACCUMULATORS_LOCK:
        got = _ACCUMULATORS.get(name)
        if got is None:
            got = _ACCUMULATORS[name] = ProbeAccumulator()
        return got


def probed(hop, kind, scope, measure, witness=None, holds=..., keep=None):
    """`ProbeAccumulator.probed` on the hop's own accumulator; a probe with
    no hop to keep it in (an engine built bare) is only run."""
    if hop is None or not getattr(hop, "name", None):
        return measure()
    return accumulator(hop).probed(kind, scope, measure, witness, holds, keep)


def forget(hop):
    with _ACCUMULATORS_LOCK:
        _ACCUMULATORS.pop(getattr(hop, "name", str(hop)), None)


def kept(hop):
    """What a run of this hop left of its probes, for `doctor` in another
    process: the rows saved beside its reports, or []."""
    from . import durable
    try:
        got = durable.read_state(hop.report_dir() / PROBES_FILE, "cache")
    except (OSError, ValueError):
        return []
    return list((got or {}).get("probes") or []) if isinstance(
        got, dict) else []
