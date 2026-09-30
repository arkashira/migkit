"""Numbers set by reasoning, tuned from what runs measure (backlog P0-DEPTH,
item 3, built with P0-SMART's knowledge base).

`canon.WIDE`, `ranges.BUCKET_ROWS`, `throttle.REPLICA_RISE`, the
PostgreSQL digest's memory, the probe floor (`model.PROBE_FROM`) and the
tie (`decide.TIE`) were each set once by reasoning about one machine. Each
is declared here (`TUNABLES`): today's value is its prior, read from the
module that holds it - one place for the number, as before - and a run
that measures what the value should have been says so (`observe`); the
value used from then on (`value`) is the geometric mean of the prior,
weighed as `PRIOR_WEIGHT` runs, and every run's measurement since - per
machine in the knowledge base (`costmodel.Knowledge`), per hop beside its
reports where the hop measured its own. Never further than `SPAN` times
from the prior either way: a run that measured something odd moves the
number, it does not throw it away.

Each declares what measures it (`how`); a module adopts its number by
reading `value(name)` where it read the constant, and says what it
measured with `observe(name, ...)`. Adopted so far: the probe floor and
the tie (the tie floors what two costs within their measured noise are
held to be, `decide._tied_estimates`); the others are declared with what
measures them, for the modules that hold them to adopt.
"""
import json
import math
import os
import threading
from dataclasses import dataclass

#: the prior weighs as much as this many runs' measurements
PRIOR_WEIGHT = 3.0
#: a tuned value stays within this factor of its prior
SPAN = 8.0
#: where a hop keeps what its own runs measured
HOP_FILE = "tuned.json"


def _attr(module, name, owner=None):
    """A reader of today's value, from the module that holds it."""
    def read():
        import importlib
        mod = importlib.import_module("migkit." + module)
        obj = getattr(mod, owner) if owner else mod
        return float(getattr(obj, name))
    return read


@dataclass(frozen=True)
class Tunable:
    name: str
    what: str
    prior: object          #: () -> today's value
    how: str               #: what a run measures it by
    adopted: bool = False  #: whether its module reads `value` yet


TUNABLES = {t.name: t for t in (
    Tunable("model.PROBE_FROM", "the least a database carries for the"
            " resources to be probed before its move",
            _attr("model", "PROBE_FROM"),
            "the bytes the move carried in the time its probes took: below"
            " that, the move takes about as long as the probes would",
            adopted=True),
    Tunable("decide.TIE", "the share within which two measured costs are"
            " a tie", _attr("decide", "TIE"),
            "the relative noise of a unit's time about each way's fitted"
            " line, where a way ran enough units to say",
            adopted=True),
    Tunable("canon.WIDE", "the bytes past which a value is digested where"
            " it lies, not carried whole", _attr("canon", "WIDE"),
            "the largest value both servers digested in less time than"
            " carrying it took"),
    Tunable("ranges.BUCKET_ROWS", "the rows of one bucket of a keyless"
            " table's hash", _attr("ranges", "BUCKET_ROWS"),
            "the rows one leaf scan hashed in the time a range is meant to"
            " take"),
    Tunable("throttle.REPLICA_RISE", "the seconds a replica may fall"
            " further behind while it is read", _attr("throttle",
                                                      "REPLICA_RISE"),
            "the rise in the replica's lag that went with its reads"
            " slowing"),
    Tunable("postgres.DIGEST_MEMORY", "the memory one PostgreSQL digest"
            " may hold", _attr("engines.postgres", "DIGEST_MEMORY",
                               "PostgresEngine"),
            "the most a digest was seen to hold, with room over it"),
)}

_LOCK = threading.Lock()


def _prior(name):
    t = TUNABLES.get(name)
    if t is None:
        raise KeyError(f"no tunable {name!r}")
    return float(t.prior())


def _mean(prior, stats):
    """The geometric mean of the prior (weighed `PRIOR_WEIGHT`) and the
    measurements (their weight and the sum of their logs), within
    `SPAN` of the prior."""
    n = float((stats or {}).get("n") or 0)
    logs = float((stats or {}).get("logs") or 0)
    if n <= 0 or prior <= 0:
        return prior
    got = math.exp((PRIOR_WEIGHT * math.log(prior) + logs)
                   / (PRIOR_WEIGHT + n))
    return min(max(got, prior / SPAN), prior * SPAN)


def _hop_stats(hop, name):
    try:
        got = json.loads((hop.report_dir() / HOP_FILE).read_text())
    except (OSError, ValueError, AttributeError):
        return None
    return ((got or {}).get("params") or {}).get(name)


def value(name, hop=None):
    """`name`'s value here: the hop's own where its runs measured it, this
    machine's where other runs did, its prior otherwise."""
    prior = _prior(name)
    if hop is not None:
        mine = _hop_stats(hop, name)
        if mine and mine.get("n"):
            return _mean(prior, mine)
    from .costmodel import knowledge
    try:
        stats = (knowledge().data().get("params") or {}).get(name)
    except OSError:
        stats = None
    return _mean(prior, stats)


def observe(name, measured, hop=None, weight=1.0):
    """A run measured `measured` as what `name` should have been: kept for
    this machine, and for `hop` where given."""
    if name not in TUNABLES or not measured or measured <= 0:
        return
    add = {"n": float(weight), "logs": float(weight) * math.log(measured)}
    from .costmodel import knowledge

    def apply(got):
        params = got.setdefault("params", {})
        cur = params.setdefault(name, {"n": 0.0, "logs": 0.0})
        cur["n"] += add["n"]
        cur["logs"] += add["logs"]
    try:
        knowledge()._change(apply)
    except OSError:
        pass
    if hop is None:
        return
    with _LOCK:
        try:
            path = hop.report_dir() / HOP_FILE
            try:
                got = json.loads(path.read_text())
            except (OSError, ValueError):
                got = {}
            apply(got)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(got))
            os.replace(tmp, path)
        except (OSError, AttributeError):
            pass


def lines():
    """Every tunable with its value here and its prior, for `doctor`."""
    out = []
    for name, t in TUNABLES.items():
        try:
            prior, now = _prior(name), value(name)
        except Exception:  # noqa: BLE001 - a module not loadable here
            continue
        if abs(now - prior) > 1e-9 * max(prior, 1.0):
            out.append(f"{t.what}: {now:,.4g} here, tuned from {prior:,.4g}"
                       " by what runs here measured")
    return out
