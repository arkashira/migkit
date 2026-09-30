"""What a way costs, as a function of the work, with how sure that is
(backlog P0-SMART, items 1, 2, 6 and learning across hops).

A rung's cost was one number - seconds a unit, the latest measured - so a
way timed on a small table ranked the same on a 500 GB one with large
values over a 50 ms link. Here a rung's seconds for a unit of work are a
linear function of what the unit is (`FEATURES`: its start-up, its bytes,
its rows, its large values, bytes without a key, the round trips its rows
take over the link, and its bytes while the server is busy), fitted
online per rung and engine pair from every unit it ran: a Bayesian linear
regression (a normal-inverse-gamma prior, updated from its sufficient
statistics), so every answer comes with its spread (`Estimate`) - the
posterior predictive's, a Student t - and the spread narrows as units
come in. The errors are relative (each unit weighs by its own size), as
the times of units of very different sizes are.

Where a rung has run nothing here, its cost is predicted (`truth`
PREDICTED) and never taken for a measurement:
* from what this hop measured on earlier runs (`HOP_FILE`) - measured,
  not predicted, only older;
* from what other hops of the same engines measured (the local
  knowledge base, `Knowledge`, which never leaves this machine), its
  spread widened;
* from the resource model (`model.Model`, backlog R22) where the rung
  says which resources a byte of it passes (`Rung.loads`);
* from migkit's own benchmarks (`seeds`: `movers.MY_RUNGS`' numbers of
  `MY_BENCH`, `physical.MEASURED`), its spread wide.
Shape is carried by the features rather than by a bucket a shape: one
fit of seven numbers learns from every unit of a rung, where a bucket
per shape starts every new shape from nothing.

A rung's chance of failing (`risk`) is its history here - tries and
failures by engine pair, version and service - under a prior of one
failure in twenty (a Beta(1, 19)), and enters its expected time with the
work a failure throws away (`risked`).

Measured on simulated units (`bench/smart_decisions.py costs`, 400
choices a scenario, three ways of different start-ups, bytes, rows,
large values and round trips, each timed on four small units of the
first tables with 15% noise, run 2026-09-30) - the mean regret, how much
longer the way chosen takes than the best, by what chose it:

    scenario           one number   one number  least    cost    the
                       a row        a byte      squares  model   prediction
    as timed             30.9%        17.9%     38.5%     1.3%     9.9%
    500x larger         161.6%       136.0%     90.2%     7.2%     8.5%
    large values        177.7%       211.4%    118.7%     7.4%     8.6%
    a 50 ms link       1174.6%      1027.0%    437.2%     4.6%     6.0%
    a busy source        71.3%        58.3%     68.3%     5.8%     6.8%

One number a row is what `Costs` kept. Least squares alone has too few
units to fit seven numbers and nothing for what it never saw; the
prediction alone (its coefficients off by up to x1.6, as the resource
model's are) is right on shapes never timed; the cost model - the
prediction as its prior, the units as its evidence - beats both
everywhere. With the prediction off by x2 at its own wider spread
(`MODEL_SHARE`, as the resource model's one run was): the cost model
1.5%, 10.7%, 12.1%, 9.0% and 10.0% in the same order, the prediction
alone 14.9%, 12.3%, 13.9%, 10.2% and 13.6%.
"""
import json
import math
import os
import threading
import time
from dataclasses import dataclass

MEASURED_TRUTH = "measured"
PREDICTED = "predicted"

FEATURES = ("start", "MB", "k rows", "large-value MB", "keyless MB",
            "round trips", "busy MB")
D = len(FEATURES)
MIB = 2 ** 20

#: the prior on the relative noise of a unit's time: a0, b0 of the
#: inverse-gamma, its mean b0 / (a0 - 1) = 0.04 (a unit's time within
#: ~20% of the line through the others)
A0, B0 = 2.0, 0.04
#: a coefficient nothing says anything about: seconds a feature's unit,
#: its prior spread - wide enough that one unit's time decides it
UNKNOWN_SD = 1e3
#: a coefficient a prediction leaves open beside the ones it gives: a
#: spread as wide as the slowest plausible (a second to start, 20 MB/s,
#: 100k rows/s, 5 MB/s of large values, a round trip a row)
LOOSE_SD = (1.0, 0.05, 0.01, 0.2, 0.05, 1.0, 0.05)
#: how much wider a prior from other hops is than their own spread
ELSEWHERE = 2.0
#: a prediction from a benchmark: its spread, as a share of each
#: coefficient
PRIOR_SHARE = 0.5
#: a prediction from the resource model: as wide as the coefficient itself
#: - its one run against docker named the wrong resource as binding in 4
#: of 11 cases and was off by up to 100% (backlog P0-DEPTH, item 4), so
#: its readings are a prior to be measured past, never the truth
MODEL_SHARE = 1.0
#: the weight this hop's earlier runs keep beside this run's units
EARLIER_WEIGHT = 0.5
#: a unit whose time is this many predicted standard deviations off leaves
#: its spread: the world changed, and what was learned before it is
#: halved (`FORGET`) so what comes after decides
SURPRISE, FORGET = 2.5, 0.5
#: the units a way runs before its noise is measured enough to tune the
#: tie by
TIE_UNITS = 5
#: a failure's prior: one in twenty
FAIL_PRIOR = (1.0, 19.0)
#: the least number of units a rung's own fit needs before its spread is
#: its own and not the prior's
LEAST = 2


def _np():
    import numpy
    return numpy


def features(unit):
    """The feature vector of a unit of work: a dict of its `bytes`,
    `rows`, `lob_bytes`, `keyless`, `rtt` (seconds) and `load` (the share
    of the server busy, 0 to 1)."""
    np = _np()
    u = unit or {}
    b = float(u.get("bytes") or 0) / MIB
    r = float(u.get("rows") or 0) / 1000.0
    return np.array([1.0, b, r, float(u.get("lob_bytes") or 0) / MIB,
                     b if u.get("keyless") else 0.0,
                     float(u.get("rtt") or 0) * r,
                     float(u.get("load") or 0) * b])


def unit_of(fact=None, rows=None, rtt=None, load=None):
    """A unit from a table's facts (`table_facts`, `row_sizes`): its rows
    (or `rows` of them), bytes at its mean row, its large values' share,
    whether it has a key, and the link's round trip and the server's load
    where known."""
    f = fact or {}
    total = int(f.get("rows") or 0)
    n = int(rows if rows is not None else total)
    size = float(f.get("bytes") or 0)
    per = size / total if total else float(f.get("mean") or 0)
    lob = float(f.get("lob_share") or 0)
    return {"rows": n, "bytes": per * n, "lob_bytes": lob * per * n,
            "keyless": f.get("key") is False, "rtt": rtt or f.get("rtt"),
            "load": load if load is not None else f.get("load")}


def _t_factor(dof):
    """How much wider a Student t's quantiles are than the normal's at
    `dof` degrees of freedom (Cornish-Fisher to the second order)."""
    if not dof or dof == math.inf:
        return 1.0
    z = 1.2816
    return 1.0 + (z * z + 1) / (4 * dof) + (5 * z ** 4 + 16 * z * z + 3) / (
        96 * dof * dof)


@dataclass(frozen=True)
class Estimate:
    """Seconds a way takes on a unit: the mean, its spread (standard
    deviation), whether it was measured here or predicted, from what, and
    on how many units."""
    mean: float
    sd: float = 0.0
    truth: str = MEASURED_TRUTH
    source: str = ""
    n: float = 0.0
    dof: float = math.inf
    #: the relative noise of one unit's time about the line (its standard
    #: deviation over its mean): what two ways within it cannot be told
    #: apart by, where the spread above also holds how far the line is
    #: stretched from the units it was fitted on
    noise: float = 0.0

    def low(self):
        """The p10: as fast as it plausibly is."""
        return max(0.0, self.mean - 1.2816 * self.sd * _t_factor(self.dof))

    def high(self):
        """The p90: as slow as it plausibly is."""
        return self.mean + 1.2816 * self.sd * _t_factor(self.dof)

    @property
    def measured(self):
        return self.truth == MEASURED_TRUTH

    def scaled(self, k):
        return Estimate(self.mean * k, self.sd * k, self.truth, self.source,
                        self.n, self.dof, self.noise)

    def said(self):
        """`~12s (8-17s, measured)`."""
        from .wording import _duration
        lo, hi = self.low(), self.high()
        spread = (f"{_duration(lo)}-{_duration(hi)}, "
                  if hi > self.mean * 1.05 else "")
        return f"~{_duration(self.mean)} ({spread}{self.truth})"

    def as_evidence(self):
        return {"mean": self.mean, "p10": self.low(), "p90": self.high(),
                "sd": self.sd, "truth": self.truth, "source": self.source,
                "units": self.n}


class Fit:
    """One rung's line: a normal-inverse-gamma prior (`mu0`, `sd0`) and
    the sufficient statistics of the units it ran, each unit weighed by
    its own size so its error is relative."""

    def __init__(self, mu0=None, sd0=None, source=""):
        np = _np()
        self.mu0 = np.zeros(D) if mu0 is None else np.asarray(mu0, float)
        sd = (np.full(D, UNKNOWN_SD) if sd0 is None
              else np.maximum(np.asarray(sd0, float), 1e-12))
        self.sd0 = sd
        self.informed = sd0 is not None
        self.source = source
        self.xtx = np.zeros((D, D))
        self.xty = np.zeros(D)
        self.yty = 0.0
        self.n = 0.0
        self._post = None

    @property
    def l0(self):
        np = _np()
        return np.diag((B0 / (A0 - 1.0)) / self.sd0 ** 2)

    def posterior(self):
        """(mean, precision, a, b) of the coefficients."""
        if self._post is None:
            np = _np()
            l0 = self.l0
            lam = l0 + self.xtx
            mu = np.linalg.solve(lam, l0 @ self.mu0 + self.xty)
            a = A0 + self.n / 2.0
            b = B0 + 0.5 * max(self.yty + self.mu0 @ l0 @ self.mu0
                               - mu @ lam @ mu, 0.0)
            self._post = (mu, lam, a, b)
        return self._post

    def mean(self, x):
        mu = self.posterior()[0]
        return float(x @ mu)

    def predict(self, x):
        """(mean, standard deviation, degrees of freedom) of a unit's
        seconds at feature vector `x`."""
        np = _np()
        mu, lam, a, b = self.posterior()
        m = float(x @ mu)
        s = max(m, 1e-3)
        var = (b / a) * (s * s + float(x @ np.linalg.solve(lam, x)))
        dof = 2 * a
        if dof > 2:
            var *= dof / (dof - 2)
        return max(m, 0.0), math.sqrt(max(var, 0.0)), dof

    def noise(self):
        """The relative noise of a unit's time about the line."""
        _, _, a, b = self.posterior()
        return math.sqrt(b / a)

    def update(self, x, y, weight=1.0):
        """One unit of `y` seconds at `x`: its surprise (how many predicted
        standard deviations it was off), then learned."""
        np = _np()
        m, sd, _ = self.predict(x)
        z = (y - m) / sd if sd > 0 and (self.n or self.informed) else 0.0
        s = math.sqrt(max(m, 1e-3) * max(y, 1e-3)) if m > 0 else max(y, 1e-3)
        xs, ys = x / s, y / s
        self.xtx = self.xtx + weight * np.outer(xs, xs)
        self.xty = self.xty + weight * xs * ys
        self.yty += weight * ys * ys
        self.n += weight
        self._post = None
        self.last = (xs, ys)
        return z

    def add_row(self, xs, ys, weight=1.0):
        """A unit already weighed by its size (`last`)."""
        np = _np()
        self.xtx = self.xtx + weight * np.outer(xs, xs)
        self.xty = self.xty + weight * xs * ys
        self.yty += weight * ys * ys
        self.n += weight
        self._post = None

    def forget(self, keep):
        """What was learned so far weighs `keep` of what it did."""
        self.xtx = self.xtx * keep
        self.xty = self.xty * keep
        self.yty *= keep
        self.n *= keep
        self._post = None

    def add(self, other, weight=1.0):
        """Another fit's units, as if run here, weighing `weight`."""
        self.xtx = self.xtx + weight * other.xtx
        self.xty = self.xty + weight * other.xty
        self.yty += weight * other.yty
        self.n += weight * other.n
        self._post = None

    def as_prior(self, widen=ELSEWHERE):
        """(mean, spread) of the coefficients, widened, for another hop."""
        np = _np()
        mu, lam, a, b = self.posterior()
        cov = (b / max(a - 1.0, 0.5)) * np.linalg.inv(lam)
        return mu, widen * np.sqrt(np.maximum(np.diag(cov), 0.0))

    def to_dict(self):
        return {"xtx": self.xtx.tolist(), "xty": self.xty.tolist(),
                "yty": self.yty, "n": self.n}

    @classmethod
    def from_dict(cls, d, mu0=None, sd0=None, source=""):
        np = _np()
        f = cls(mu0, sd0, source)
        try:
            f.xtx = np.asarray(d["xtx"], float).reshape(D, D)
            f.xty = np.asarray(d["xty"], float).reshape(D)
            f.yty = float(d["yty"])
            f.n = float(d["n"])
        except (KeyError, TypeError, ValueError):
            return cls(mu0, sd0, source)
        return f


# --- migkit's own benchmarks, the first priors ------------------------------

#: The shapes of `movers.MY_BENCH`'s two tables (the comment above
#: `movers.MY_BENCH`): bytes of data and rows. Each MySQL bulk rung's two
#: measured rates solve for its seconds a MB and a thousand rows: the
#: dump and load, 18.9 MB/s on the narrow table and 60.8 on the wide,
#: is ~0.0080 s a MB and ~3.7 ms a thousand rows - one rate a byte could
#: not say both.
MY_BENCH_SHAPES = {"plain": (41.5e6, 500_000), "binary": (222e6, 500_000)}
_SEEDS = {}
_SEEDS_LOCK = threading.Lock()


def seeds():
    """{(rung, pair): (mean coefficients, source)} from migkit's own
    benchmarks, read from the modules that hold them."""
    with _SEEDS_LOCK:
        if _SEEDS:
            return _SEEDS
        np = _np()
        try:
            from . import movers
            (pb, pr), (bb, br) = (MY_BENCH_SHAPES["plain"],
                                  MY_BENCH_SHAPES["binary"])
            for r in movers.MY_RUNGS:
                plain = r.rates.get(movers.PLAIN)
                binary = r.rates.get(movers.BINARY)
                if not plain or not binary:
                    continue
                tp, tb = pb / plain, bb / binary
                a = (tb - tp) / ((bb - pb) / MIB)
                c = (tp - a * pb / MIB) / (pr / 1000.0)
                mu = np.zeros(D)
                mu[0], mu[1], mu[2] = r.fixed, max(a, 0.0), max(c, 0.0)
                _SEEDS[(r.name, "mysql->mysql")] = (mu, "migkit's benchmark"
                                                    f" ({movers.MY_BENCH})")
        except Exception:  # noqa: BLE001 - a seed is a help, never a need
            pass
        try:
            from . import physical
            for key, (rows, secs) in physical.MEASURED.items():
                name, pair = ((key[0], f"{key[1]}->{key[1]}")
                              if isinstance(key, tuple) else
                              (key, "mysql->mysql" if "clone" in key
                               else "postgres->postgres"))
                mu = np.zeros(D)
                mu[2] = secs / (rows / 1000.0)
                _SEEDS[(name, pair)] = (mu, "migkit's benchmark")
        except Exception:  # noqa: BLE001
            pass
        return _SEEDS


# --- the knowledge base: what every hop here learned ------------------------

def knowledge_path():
    """Where the machine's knowledge is kept: `MIGKIT_KNOWLEDGE`, or the
    user's data directory. Never sent anywhere."""
    got = os.environ.get("MIGKIT_KNOWLEDGE")
    if got:
        return os.path.expanduser(got)
    base = os.environ.get("XDG_DATA_HOME") or os.path.join(
        os.path.expanduser("~"), ".local", "share")
    return os.path.join(base, "migkit", "knowledge.json")


#: what is learned already weighs this much when a run adds its own: an
#: old engine version's numbers fade as new runs come
KEEP_OLD = 0.9


class Knowledge:
    """The local knowledge base: each rung's fit by engine pair, and its
    tries and failures by engine pair, version and service - read once a
    process, written whole or not at all under a lock, so two moves on
    one machine add to it both."""

    def __init__(self, path=None):
        self.path = path or knowledge_path()
        self._got = None
        self._lock = threading.Lock()

    def _read(self):
        try:
            with open(self.path) as f:
                got = json.load(f)
            return got if isinstance(got, dict) else {}
        except (OSError, ValueError):
            return {}

    def data(self):
        if self._got is None:
            self._got = self._read()
        return self._got

    def fit(self, rung, pair):
        got = (self.data().get("fits") or {}).get(f"{rung}|{pair}")
        return Fit.from_dict(got, source="other hops here") if got else None

    def failures(self, rung, pair, version="", service=""):
        """(tries, failures) of `rung` on the engine pair, and on its
        version and service where they are known - the narrowest that has
        any."""
        runs = self.data().get("runs") or {}
        for key in (f"{rung}|{pair}|{version}|{service}",
                    f"{rung}|{pair}|{version}|", f"{rung}|{pair}||"):
            got = runs.get(key)
            if got and got.get("tries"):
                return int(got["tries"]), int(got.get("failed") or 0)
        return 0, 0

    def _change(self, apply):
        import fcntl
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        with self._lock, open(self.path + ".lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            got = self._read()
            apply(got)
            got["at"] = time.time()
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(got, f)
            os.replace(tmp, self.path)
            self._got = got

    def learn(self, rung, pair, fit):
        """A run's units of `rung` added to what the machine knows."""
        if fit is None or fit.n <= 0:
            return

        def apply(got):
            fits = got.setdefault("fits", {})
            key = f"{rung}|{pair}"
            old = Fit.from_dict(fits[key]) if key in fits else Fit()
            old.forget(KEEP_OLD)
            old.add(fit)
            fits[key] = old.to_dict()
        self._change(apply)

    def outcome(self, rung, pair, ok, version="", service="", why=""):
        """One try of `rung`: whether it ran, and why not."""
        self.tries(pair, {rung: [1, 0 if ok else 1, "" if ok else why]},
                   version, service)

    def tries(self, pair, counts, version="", service=""):
        """{rung: [tries, failures, why]} added at once."""
        def apply(got):
            runs = got.setdefault("runs", {})
            for rung, (n, failed, why) in counts.items():
                for key in {f"{rung}|{pair}|{version}|{service}",
                            f"{rung}|{pair}|{version}|",
                            f"{rung}|{pair}||"}:
                    r = runs.setdefault(key, {"tries": 0, "failed": 0})
                    r["tries"] += int(n)
                    if failed:
                        r["failed"] += int(failed)
                        r["why"] = str(why)[:200]
        self._change(apply)


_KNOWLEDGE = {}


def knowledge():
    """The knowledge base at `knowledge_path()`, one a path a process."""
    path = knowledge_path()
    got = _KNOWLEDGE.get(path)
    if got is None:
        got = _KNOWLEDGE[path] = Knowledge(path)
    return got


def risk(tries, failed):
    """The chance of a failure after `failed` of `tries`: the Beta
    posterior's mean under `FAIL_PRIOR`."""
    a, b = FAIL_PRIOR
    return (failed + a) / (tries + a + b)


def risked(seconds, p, lost=0.5, after=None):
    """Expected seconds of a way that fails with chance `p`, a failure
    throwing away `lost` of its work (redone) and the half of it left on
    average going at the next way's pace (`after`, its seconds for the
    whole; the same way's where None)."""
    if not p or seconds is None:
        return seconds
    rest = max(0.0, (after if after is not None else seconds) - seconds)
    return seconds + p * (lost * seconds + rest / 2.0)


# --- one choice's costs -----------------------------------------------------

def _spread(mu, share=PRIOR_SHARE):
    """A prediction's spread: `share` of each coefficient it gives,
    `LOOSE_SD` for each it leaves open."""
    np = _np()
    mu = np.asarray(mu, float)
    return np.where(mu > 0, share * np.abs(mu), np.asarray(LOOSE_SD))


#: where a hop keeps its rungs' fits, beside its rates
HOP_FILE = "costs.json"


class CostModel:
    """The costs of one choice's rungs as functions of the unit: what
    `decide.climb` ranks by and `decide.Trials` explores with. Keeps the
    `Costs` interface (`get`, `saw`) for every caller that reads a number
    a unit.

    `pair` is the engine pair ("mysql->postgres"), `unit` the typical unit
    `get` answers for, `hop` and `prefix` where this hop keeps the fits
    (`HOP_FILE`), `model` the resource model predictions are made from,
    `knowledge` the machine's (None: `knowledge()`; False: none),
    `version` and `service` what a failure is kept by."""

    def __init__(self, work="", rungs=(), pair="", unit=None, hop=None,
                 prefix="", model=None, knowledge=None, version="",
                 service=""):
        self.work, self.pair = work, pair
        self.unit = dict(unit or {})
        self.hop, self.prefix = hop, prefix
        self.model = model
        self._kb = knowledge
        self.version, self.service = version, service
        self.rungs = {r.name: r for r in rungs}
        self.fits = {}
        #: this run's units alone, what is kept and learned from it
        self.own = {}
        #: {rung: [tries, failures, the last failure's why]} not kept yet
        self._tries = {}
        self.surprise = {}
        self._lock = threading.RLock()

    @property
    def kb(self):
        if self._kb is False:
            return None
        return self._kb if self._kb is not None else knowledge()

    def _prior(self, name):
        """(mean, spread, source) of `name`'s coefficients before any unit
        of this hop: other hops' measurements, the resource model's
        prediction, migkit's benchmark - or None where nothing says."""
        kb = self.kb
        got = kb.fit(name, self.pair) if kb is not None else None
        if got is not None and got.n > 0:
            base = self._structural(name)
            if base is not None:
                got = Fit.from_dict(got.to_dict(), base[0], base[1],
                                    "other hops here")
            mu, sd = got.as_prior()
            return mu, sd, "other hops here"
        return self._structural(name)

    def _structural(self, name):
        np = _np()
        rung = self.rungs.get(name)
        loads = dict(getattr(rung, "loads", None) or ())
        stages = getattr(self.model, "stages", None) or {}
        per_mb = 0.0
        for r, passes in loads.items():
            s = stages.get(r)
            if s is not None and s.per_stream:
                per_mb = max(per_mb, passes * MIB / s.per_stream)
        if per_mb:
            mu = np.zeros(D)
            mu[0] = float(getattr(rung, "startup", 0.0) or 0.0)
            mu[1] = per_mb
            mu[5] = float(getattr(rung, "trips", 0.0) or 0.0)
            mu[6] = per_mb
            return mu, _spread(mu, MODEL_SHARE), "the resource model"
        seed = seeds().get((name, self.pair))
        if seed is not None:
            mu, source = seed
            return mu, _spread(mu), source
        return None

    def _earlier(self, name):
        """This hop's own fit of `name` from its earlier runs, or None."""
        if self.hop is None:
            return None
        try:
            got = json.loads((self.hop.report_dir() / HOP_FILE).read_text())
        except (OSError, ValueError, AttributeError):
            return None
        d = (got.get(self.prefix + self.work) or {}).get(name)
        return Fit.from_dict(d) if d else None

    def fit(self, name):
        """`name`'s fit for this run, made the first time it is asked."""
        with self._lock:
            got = self.fits.get(name)
            if got is None:
                prior = self._prior(name)
                got = (Fit(prior[0], prior[1], prior[2]) if prior
                       else Fit())
                earlier = self._earlier(name)
                if earlier is not None and earlier.n > 0:
                    got.add(earlier, EARLIER_WEIGHT)
                    got.source = "this hop's earlier runs"
                self.fits[name] = got
            return got

    def estimate(self, name, unit=None):
        """The `Estimate` of `name` on `unit` (the typical unit where None),
        or None where nothing measured or predicted says anything."""
        f = self.fit(name)
        if f.n <= 0 and not f.informed:
            return None
        u = unit if unit is not None else self.unit
        mean, sd, dof = f.predict(features(u))
        measured = f.n > 0
        return Estimate(mean, sd, MEASURED_TRUTH if measured else PREDICTED,
                        f.source if not measured or f.source.startswith(
                            "this hop") else "this run", f.n, dof,
                        f.noise())

    def get(self, name, default=None):
        """Seconds a unit of the typical unit, for a caller that reads a
        number (`Costs.get`)."""
        e = self.estimate(name)
        if e is None:
            return default
        rows = max(int(self.unit.get("rows") or 1), 1)
        return e.mean / rows

    def saw(self, name, units, seconds, unit=None):
        """`units` rows of work took `seconds` on `name`: learned, and how
        far off the prediction was kept (`surprise`). A unit that leaves
        its spread halves what was learned before it."""
        if seconds is None or seconds < 0:
            return None
        if unit is None:
            unit = self._scaled(units)
        x = features(unit)
        with self._lock:
            f = self.fit(name)
            z = f.update(x, float(seconds))
            self.own.setdefault(name, Fit()).add_row(*f.last)
            if abs(z) > SURPRISE and f.n > LEAST:
                f.forget(FORGET)
            self.surprise[name] = z
        return z

    def _scaled(self, units):
        """The typical unit, at `units` rows."""
        u = dict(self.unit)
        rows = int(u.get("rows") or 0)
        n = int(units or 0)
        if rows and n:
            k = n / rows
            u["bytes"] = float(u.get("bytes") or 0) * k
            u["lob_bytes"] = float(u.get("lob_bytes") or 0) * k
        u["rows"] = n
        return u

    def measured(self, name):
        return self.fit(name).n > 0

    def risk(self, name):
        """`name`'s chance of failing here (`risk`): its history in the
        knowledge base and this run's tries not kept yet."""
        kb = self.kb
        tries, failed = (kb.failures(name, self.pair, self.version,
                                     self.service)
                         if kb is not None and self.pair else (0, 0))
        mine = self._tries.get(name)
        if mine:
            tries, failed = tries + mine[0], failed + mine[1]
        return risk(tries, failed)

    def outcome(self, name, ok, why=""):
        """One try of `name` on a unit, kept until `save`: a unit's
        success is not worth a write to the knowledge base each."""
        with self._lock:
            got = self._tries.setdefault(name, [0, 0, ""])
            got[0] += 1
            if not ok:
                got[1] += 1
                got[2] = str(why)

    def save(self):
        """This run's fits kept with the hop and added to the machine's
        knowledge."""
        with self._lock:
            own = {n: f for n, f in self.own.items() if f.n > 0}
            tries, self._tries = self._tries, {}
        noisy = sorted(f.noise() for f in self.fits.values()
                       if f.n >= TIE_UNITS)
        if noisy and self.kb is not None:
            # how far units of one way spread about its line here: what
            # two costs within are a tie (`tuning`, `decide.TIE`)
            from . import tuning
            tuning.observe("decide.TIE", noisy[len(noisy) // 2])
        kb = self.kb
        if kb is not None and self.pair and tries:
            try:
                kb.tries(self.pair, tries, self.version, self.service)
            except OSError:
                pass
        if not own:
            return
        if self.hop is not None:
            try:
                path = self.hop.report_dir() / HOP_FILE
                try:
                    got = json.loads(path.read_text())
                except (OSError, ValueError):
                    got = {}
                kept = got.setdefault(self.prefix + self.work, {})
                for n, f in own.items():
                    old = Fit.from_dict(kept[n]) if n in kept else Fit()
                    old.forget(KEEP_OLD)
                    old.add(f)
                    kept[n] = old.to_dict()
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".tmp")
                tmp.write_text(json.dumps(got))
                os.replace(tmp, path)
            except (OSError, AttributeError):
                pass
        kb = self.kb
        if kb is not None and self.pair:
            for n, f in own.items():
                try:
                    kb.learn(n, self.pair, f)
                except OSError:
                    pass


#: `bench/smart_decisions.py costs` and `explore`, 2026-09-30: mean regret
#: in percent (the docstring's table), and a table's 60 ranges on the ways
#: `decide.Trials` chose (mean, median, p90 regret in percent)
MEASURED = {
    "choice": {
        "as timed": {"a row": 30.9, "a byte": 17.9, "least squares": 38.5,
                     "cost model": 1.3, "prediction": 9.9},
        "500x larger": {"a row": 161.6, "a byte": 136.0,
                        "least squares": 90.2, "cost model": 7.2,
                        "prediction": 8.5},
        "large values": {"a row": 177.7, "a byte": 211.4,
                         "least squares": 118.7, "cost model": 7.4,
                         "prediction": 8.6},
        "a 50 ms link": {"a row": 1174.6, "a byte": 1027.0,
                         "least squares": 437.2, "cost model": 4.6,
                         "prediction": 6.0},
        "a busy source": {"a row": 71.3, "a byte": 58.3,
                          "least squares": 68.3, "cost model": 5.8,
                          "prediction": 6.8}},
    "choice, the prediction off by x2": {
        "cost model": (1.5, 10.7, 12.1, 9.0, 10.0),
        "prediction": (14.9, 12.3, 13.9, 10.2, 13.6)},
    "trials": {"each once": (60.6, 47.2, 117.8),
               "safe, nothing predicted": (22.6, 15.3, 41.9),
               "safe, predicted": (5.7, 5.2, 11.3)},
}
