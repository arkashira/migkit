import math
import shutil
import subprocess
import sys
import sysconfig
import time
from pathlib import Path
from typing import NamedTuple, Optional

_BASE = Path(__file__).resolve().parent.parent


def _own_script_dirs():
    """Where console scripts installed alongside migkit live - a venv's bin,
    a container's /usr/local/bin, wherever. sys.executable is NOT resolved:
    in a venv it is a symlink to the base interpreter, and following it would
    point at the wrong bin directory."""
    dirs = [sysconfig.get_path("scripts"), str(Path(sys.executable).parent)]
    seen, out = set(), []
    for d in dirs:
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    return out


def _pinned_bin():
    """Where `doctor --install` links the builds it pins (`tools.
    tools_home()/bin`): this user's, so nothing is written where only root
    may, and first, so a pinned build is the one that runs."""
    import os
    return str(Path(os.environ.get("MIGKIT_TOOLS")
                    or Path.home() / ".migkit" / "tools") / "bin")


# the builds migkit pinned first, migkit's own environment next, then a
# source checkout's venv (developing in place), then the paths Homebrew
# keeps off the default PATH.
TOOL_PATHS = [_pinned_bin()] + _own_script_dirs() + [
    str(_BASE / ".venv" / "bin"),
    "/opt/homebrew/opt/libpq/bin", "/opt/homebrew/opt/mysql-client/bin",
    "/opt/homebrew/bin", "/usr/local/bin"]


#: what keeps a program migkit runs from calling its vendor while it runs:
#: the operator never agreed to a connection that leaves the machine with
#: the versions of their servers in it. Measured on 2026-09-28 through a
#: logging proxy and an empty home: the schema differ opened
#: `vercheck.ariga.io:443` on `version` and on `schema diff`, the other
#: differ (4.33) opened `config.liquibase.com:443` on `diff`; with these
#: set, neither opened anything. The online MongoDB sync is told in its
#: own configuration file (`disableTelemetry`), which is where it reads it
NO_CALL_HOME_ENV = {"ATLAS_NO_UPDATE_NOTIFIER": "true",
                    "ATLAS_NO_ANON_TELEMETRY": "true",
                    "ATLAS_NO_UPGRADE_SUGGESTIONS": "true",
                    "LIQUIBASE_ANALYTICS_ENABLED": "false",
                    "DO_NOT_TRACK": "1"}

#: the same for programs that read it from their command line only, by
#: the start of the program's name: every Percona tool checks for updates
#: by default, sending the versions of the operating system, Perl, MySQL
#: and its driver to `v.percona.com` once a day, and prints what it hears
#: on standard output - ahead of the statements a repair reads from it
NO_CALL_HOME_ARGS = {"pt-": ("--no-version-check",)}


def quiet_argv(cmd):
    """`cmd` with the switches that keep its program from calling home,
    right after the program's name; unchanged for a program that has
    none, or a shell string."""
    if isinstance(cmd, str) or not cmd:
        return cmd
    name = str(cmd[0]).rsplit("/", 1)[-1]
    for start, switches in NO_CALL_HOME_ARGS.items():
        if name.startswith(start):
            add = [s for s in switches if s not in cmd]
            return [cmd[0], *add, *cmd[1:]]
    return cmd


def tool_env(extra=None):
    import os
    env = dict(os.environ)
    env["PATH"] = os.pathsep.join(TOOL_PATHS + [env.get("PATH", "")])
    env.update(NO_CALL_HOME_ENV)
    if extra:
        env.update(extra)
    return env


def which(name):
    return shutil.which(name, path=tool_env()["PATH"])


# transient failures that a retry will usually clear: TLS handshake races,
# dropped/reset sockets, pooler hiccups, cross-region blips. NOT auth/syntax/
# constraint errors (those are permanent and must surface immediately).
TRANSIENT = (
    "wrong version number", "lost connection", "connection reset",
    "connection refused", "could not connect", "can't connect",
    "server closed the connection", "broken pipe", "gone away",
    "eof occurred", "timeout expired", "connection timed out",
    "temporary failure", "too many connections", "operationalerror",
    "no route to host", "connection aborted", "reset by peer",
    # a driver's own wait for the server running out: measured, the
    # MongoDB driver's `...:27017: timed out` was read as permanent
    "timed out",
)

#: of those, the ones raised while connecting, before anything was sent:
#: the only failures a statement that writes may be run again after. A
#: connection lost during a write may have applied it - measured, a MySQL
#: DDL script resent after `Lost connection` re-applied its first half
#: (DDL commits as it goes) and failed on `Duplicate column`, hiding the
#: cause (audit 2026-10-01, W11)
CONNECTING = (
    "wrong version number", "connection refused", "could not connect",
    "can't connect", "timeout expired", "too many connections",
    "temporary failure", "no route to host",
    "reading initial communication packet", "reading authorization packet",
    "the database system is starting up",
)


def is_transient(err):
    e = str(err).lower()
    return any(p in e for p in TRANSIENT)


def retryable(err, writes=False):
    """Whether a failure may be tried again: a transient one, and where
    what failed writes, only one while connecting."""
    if not is_transient(err):
        return False
    e = str(err).lower()
    return not writes or any(p in e for p in CONNECTING)


def keepalive(sock, idle=60, interval=10, count=5):
    """Make a dead peer show up as an error instead of an endless wait.

    A long scan over a cross-cloud link can sit on a socket for many minutes.
    If the far end or the tunnel goes away in the meantime nothing is ever
    sent, so the OS never notices and the process waits for good. Probing
    while idle turns that into a normal connection error the retry can handle.
    Returns {"error": why} where a setting could not be made - said by the
    socket's facts (`attempts.transport`), never swallowed - else {}.
    """
    if sock is None:
        return {"error": "no socket to set"}
    import socket as _s
    try:
        sock.setsockopt(_s.SOL_SOCKET, _s.SO_KEEPALIVE, 1)
        for name, val in (("TCP_KEEPIDLE", idle), ("TCP_KEEPALIVE", idle),
                          ("TCP_KEEPINTVL", interval), ("TCP_KEEPCNT", count)):
            opt = getattr(_s, name, None)
            if opt is not None:
                sock.setsockopt(_s.IPPROTO_TCP, opt, val)
    except OSError as e:
        return {"error": f"keepalive not set: {e.strerror or e}"}
    return {}


def _backoff(attempt, base=0.8):
    """Seconds before the next try after `attempt` (from 0) failed."""
    return base * (2 ** attempt)


def with_retry(fn, tries=4, base=0.8, label="", log=None, writes=False,
               side="", **recover):
    """Run fn() under the one recovery policy (`faults.retried`): each
    failure classified by its engine code and effect, and what is done
    chosen by `decide.recovery` - a transient one retried with exponential
    backoff, a permanent one (auth, syntax, constraint) raised at once.
    `writes`: fn changes something, so a failure after it was sent is
    reconciled (`recover["check"]`) or stopped, never repeated on a guess.
    `side`: whose breaker the failures count on. The one retry policy: a
    program `run` starts is retried by it too."""
    from .faults import retried
    return retried(fn, tries=tries, side=side, writes=writes, log=log,
                   label=label, backoff=lambda i: _backoff(i, base),
                   wait=lambda s: time.sleep(s), **recover)


def without_secret(text, secret):
    """`text` with `secret` hidden, and unchanged when there is none.

    The guard is the whole point. `str.replace("", "****")` inserts the
    mask between every character, so a hop authenticating by `trust` or
    `.pgpass` - an empty password, and the safer arrangement - turned the
    plan it was about to run into `****c****r****e****a****t****e****`.
    The more careful the operator's setup, the less readable migkit made
    the output. Hidden by `redact.Redactor`: in each form it reaches text
    in, and with every secret found by its shape besides.
    """
    from .redact import Redactor
    return Redactor(secrets=(secret,), mask="****")(text)


class CommandFailed(RuntimeError):
    """A program `run` started that failed. Its message is the program's
    own error with every secret hidden; the command line is kept on the
    exception (`argv`) for the debug log and never put in the message -
    it carried hosts, users and the SQL, and a traceback printed it."""

    def __init__(self, returncode, stderr, argv):
        from .redact import Redactor
        said = Redactor()(str(stderr or "").strip())
        super().__init__(f"the command failed (exit {returncode})"
                         + (f"\n{said}" if said else ""))
        self.returncode, self.stderr, self.argv = returncode, stderr, argv


class _Exited(Exception):
    """A program that exited non-zero, its error output as its words so
    `faults.classify` reads it; never shown - `run` returns the process."""

    def __init__(self, process):
        super().__init__(str(process.stderr or ""))
        self.process = process


def run(cmd, env=None, input=None, timeout=None, check=True, retries=3,
        writes=False):
    """Run a command, retrying only transient connection failures with
    backoff - the policy and the waits `with_retry` has (`writes`: only a
    failure while connecting). A command that runs out of `timeout` is
    not run again. check=False still returns the (failed) process for
    callers that read returncode themselves (e.g. atlas diff = non-zero on
    diff); a transient failure is retried regardless of check.

    Started, like every program migkit runs, through the one runner
    (`runner.run`): in a session of its own, listed where the run lists
    its programs, its command line in the run's debug log, stopped with
    everything it started when migkit is. Its time running out is
    `subprocess.TimeoutExpired`, and a program not there `OSError`, as
    they always were here."""
    import os

    from . import runner
    shell = ([os.environ.get("COMSPEC") or "cmd.exe", "/c"] if os.name == "nt"
             else ["/bin/sh", "-c"])
    argv = (shell + [cmd] if isinstance(cmd, str)
            else [str(a) for a in cmd])
    from .faults import retried

    def once():
        try:
            got = runner.run(argv, env, input=input, timeout=timeout,
                             check=False, check_flags=False)
        except runner.Transient:
            return "timeout", None
        except runner.Blocked as e:
            return "blocked", e
        if got.returncode:
            raise _Exited(got)
        return "ran", got
    try:
        how, p = retried(once, tries=retries + 1, writes=writes,
                         side=f"run {os.path.basename(argv[0])}",
                         label="a program", backoff=_backoff,
                         wait=lambda s: time.sleep(s))
    except _Exited as e:
        how, p = "ran", e.process
    if how == "timeout":
        raise subprocess.TimeoutExpired(cmd, timeout) from None
    if how == "blocked":
        raise FileNotFoundError(2, str(p), argv[0]) from None
    if p.returncode and check:
        raise CommandFailed(p.returncode, p.stderr, cmd)
    return p


def human_int(n):
    return f"{n:,}"


def human_secs(s):
    """`wording._duration`, the one way a duration is said: five
    formatters said 59.6 s as `1m00s`, `59s`, `60 seconds`, `60s` and
    `60 seconds` (audit 2026-10-01, DU10)."""
    from .wording import _duration
    return _duration(s)


def stamp(when=None):
    """A time as every record says it: UTC, ISO 8601, with its zone -
    `2026-10-01T09:30:00+00:00`. Records said `at` seven ways, almost all
    in the machine's local time with no zone (audit 2026-10-01, DU21), so
    two machines' records of one run did not sort. `when` is seconds
    since the epoch, now where not given."""
    from datetime import datetime, timezone
    at = datetime.now(timezone.utc) if when is None else \
        datetime.fromtimestamp(float(when), timezone.utc)
    return at.isoformat(timespec="seconds")


class ETA(NamedTuple):
    seconds: Optional[float]
    lower_bound: Optional[float]
    upper_bound: Optional[float]
    confidence: float
    formatted: str
    #: measured, conditional, insufficient, stalled, behind or done
    basis: str = ""
    measured_at: Optional[str] = None
    scope: str = ""
    evidence: tuple = ()
    #: the work known to be left: units, rows, bytes, unmeasured units
    known_left: Optional[dict] = None
    unmeasured: int = 0
    #: seconds of work left with no concurrency, beside the wall time
    active_seconds: Optional[float] = None
    undiscovered: bool = False
    stale: bool = False
    revised: str = ""
    next_step: str = ""

    def __str__(self):
        return self.formatted

    def operator_line(self):
        """The estimate as the operator reads it: the time with its range
        and confidence (or why there is none), the last evidence it stands
        on - a revision where there was one - and what to do next."""
        parts = [self.formatted]
        said = [e for e in self.evidence if not e.startswith("stands on")]
        if said and self.basis != "done":
            parts.append(said[-1])
        if self.stale and not self.revised:
            parts.append("measured again after a stale estimate")
        if self.next_step:
            parts.append(self.next_step)
        return "; ".join(parts)

    def as_evidence(self):
        """The estimate as data, for the ledger and the report."""
        return {k: (list(v) if isinstance(v, tuple) else v)
                for k, v in self._asdict().items()}


class CalibratedETA:
    def __init__(self, t0=None, alpha=0.3, warmup_samples=3, clock=None):
        self.clock = clock or time.monotonic
        self.t0 = self.clock() if t0 is None else float(t0)
        self.alpha = float(alpha)
        self.warmup_samples = int(warmup_samples)
        self._last_time = None
        self._last_done = None
        self._last_cost = None
        self._sample_count = 0
        self._smoothed_rate = None
        self._smoothed_cost_rate = None
        self._rates = []
        self._cost_rates = []

    def elapsed(self):
        return self.clock() - self.t0

    def update(self, done, cost=None, timestamp=None):
        now = self.clock() if timestamp is None else float(timestamp)
        done = float(done)
        cost = float(cost) if cost is not None else None

        if self._last_time is None:
            prev_t = self.t0
            prev_done = 0.0
            prev_cost = 0.0 if cost is not None else None
        else:
            prev_t = self._last_time
            prev_done = self._last_done
            prev_cost = self._last_cost

        dt = now - prev_t
        ddone = done - prev_done
        dcost = (cost - prev_cost) if (cost is not None and prev_cost is not None) else None

        if dt <= 0:
            return

        recorded = False
        if ddone > 0:
            r = ddone / dt
            self._rates.append(r)
            if len(self._rates) > 50:
                self._rates.pop(0)
            if self._smoothed_rate is None:
                self._smoothed_rate = r
            else:
                self._smoothed_rate = self.alpha * r + (1.0 - self.alpha) * self._smoothed_rate
            self._last_done = done
            recorded = True

        if dcost is not None and dcost > 0:
            cr = dcost / dt
            self._cost_rates.append(cr)
            if len(self._cost_rates) > 50:
                self._cost_rates.pop(0)
            if self._smoothed_cost_rate is None:
                self._smoothed_cost_rate = cr
            else:
                self._smoothed_cost_rate = self.alpha * cr + (1.0 - self.alpha) * self._smoothed_cost_rate
            self._last_cost = cost
            recorded = True

        if recorded:
            self._sample_count += 1
            self._last_time = now

    record = update

    def eta_detailed(self, done, total, remaining_cost=None, timestamp=None):
        now = self.clock() if timestamp is None else float(timestamp)
        if total is None or total <= 0:
            return ETA(None, None, None, 0.0, "?")

        if done > 0 and (self._last_done is None or done > self._last_done):
            self.update(done=done, timestamp=now)

        if done >= total or (remaining_cost is not None and remaining_cost <= 0):
            return ETA(0.0, 0.0, 0.0, 1.0, human_secs(0))

        if remaining_cost is not None:
            rem = float(remaining_cost)
            if self._smoothed_cost_rate is not None and self._smoothed_cost_rate > 0:
                rate = self._smoothed_cost_rate
                rates = self._cost_rates
            else:
                rate = self._smoothed_rate
                rates = self._rates
        else:
            rem = float(total - done)
            rate = self._smoothed_rate
            rates = self._rates

        if rate is None or rate <= 0:
            elapsed = now - self.t0
            if done > 0 and elapsed > 0:
                rate = done / elapsed
            else:
                rate = None

        if self._sample_count < self.warmup_samples:
            if rate is not None and rate > 0:
                sec = rem / rate
                lb = max(0.0, sec * 0.2)
                ub = sec * 5.0
                conf = round(min(0.4, self._sample_count * 0.15), 2)
                return ETA(sec, lb, ub, conf, "? (warmup)",
                           basis="insufficient")
            return ETA(None, None, None, 0.0, "? (warmup)",
                       basis="insufficient")

        if rate is None or rate <= 0:
            return ETA(None, None, None, 0.0, "?")

        sec = rem / rate
        n = len(rates)
        if n >= 2:
            mean = sum(rates) / n
            var = sum((x - mean) ** 2 for x in rates) / (n - 1)
            std = math.sqrt(var)
            cv = std / mean if mean > 0 else 1.0
        else:
            cv = 0.0
            std = 0.0

        se = std / math.sqrt(max(1, n))
        delta_r = 1.96 * se
        r_low = max(0.05 * rate, rate - delta_r)
        r_high = rate + delta_r
        t_low = rem / r_high
        t_high = rem / r_low
        delta = (t_high - t_low) / 2.0

        lb = max(0.0, sec - delta)
        ub = sec + delta

        fn = min(1.0, n / 6.0)
        fvar = max(0.1, 1.0 - min(0.9, cv))
        fprog = 0.8 + 0.2 * min(1.0, done / total)
        conf = round(min(1.0, max(0.1, fn * fvar * fprog)), 2)

        if conf >= 0.75 or delta < 0.5:
            formatted = human_secs(sec)
        else:
            formatted = f"~{human_secs(sec)} (±{human_secs(delta)})"

        return ETA(sec, lb, ub, conf, formatted)

    def eta(self, done, total, remaining_cost=None, timestamp=None):
        return self.eta_detailed(done, total, remaining_cost=remaining_cost, timestamp=timestamp).formatted


class Timer(CalibratedETA):
    def __init__(self, t0=None, alpha=0.3, warmup_samples=3):
        super().__init__(t0=t0, alpha=alpha, warmup_samples=warmup_samples)


# ---- remaining work and when it ends (backlog P0-ETA-ALL) -------------------

#: the kinds of task a forecast covers, in the words an operator reads
TASK_KINDS = {"move": "the copy", "follow": "the change catch-up",
              "check": "the checks", "verify": "the verification",
              "deep": "the deep comparison"}
#: finished units the recent window holds: older ones describe a server
#: busier, or idler, than it is now
WINDOW = 20
#: finished units measured here before a time is said from them
WARMUP_UNITS = 3
#: an estimate older than this many seconds is stale
STALE_SECONDS = 900.0
#: no unit landing in this many median unit times, with units running,
#: is a stall: smoothing would hide it, so it is said
STALL_UNITS = 4.0
#: the newer half of the window this many times faster or slower than the
#: older half is a workload changing under the estimate
SHIFT = 2.0
#: the latest estimates of one kind of task held to how it really ended
CALIBRATION_LAST = 10
#: estimates scored before their record may lower a confidence
CALIBRATION_LEAST = 3
#: estimates of one task scored at most, spread over its run
SCORED_A_TASK = 10


def kind_of(scope):
    """The kind of task a forecast's scope names (`TASK_KINDS`), or None
    for a scope of its own."""
    for kind, said in TASK_KINDS.items():
        if scope == said or str(scope).startswith(said + " of "):
            return kind
    return None


class Unit(NamedTuple):
    """One unit of work as the durable ledger holds it: a range, a table, a
    database, a batch of changes. Its size is rows (or documents, events)
    and/or bytes; neither known is work not yet measured."""
    name: str
    kind: str = "move"
    rows: Optional[float] = None
    bytes: Optional[float] = None
    done_rows: float = 0.0
    #: pending, running, done or failed (a failed one is owed again)
    state: str = "pending"
    #: the units it waits for
    after: tuple = ()
    #: seconds it took, once done
    seconds: Optional[float] = None
    attempts: int = 1
    #: seconds it waits before it may run again (a backoff, a throttle)
    cooldown: float = 0.0
    ended: Optional[float] = None
    #: seconds since a running unit last made useful progress, as its
    #: attempts say (`attempts.Ledger.units`); None where not watched
    idle: Optional[float] = None
    #: the share of a running unit its closed parts make; None where it
    #: was not cut into parts
    share: Optional[float] = None


def merge_units(listed, measured):
    """The units a task lists (`listed`: their rows and bytes from the
    catalogue) held to what their attempts measured (`measured`: state,
    seconds, attempts, when each ended, the share its closed parts make
    and how long a running one has made no progress), matched by name
    where the measured one is of the task's kind or of none said; a
    measured unit of the task's kind the listing does not hold is work
    found while it ran, and is added. A unit of another kind is another
    task's, and is left out."""
    listed = list(listed or ())
    kinds = {u.kind for u in listed}
    took = {u.name: u for u in measured or ()
            if not u.kind or not kinds or u.kind in kinds}
    out = []
    for u in listed:
        m = took.pop(u.name, None)
        if m is None:
            out.append(u)
            continue
        if m.state == "running" and m.share is not None and u.rows:
            u = u._replace(done_rows=max(u.done_rows, u.rows * m.share))
        out.append(u._replace(
            state=m.state, seconds=m.seconds, attempts=max(u.attempts,
                                                          m.attempts),
            ended=m.ended, idle=m.idle,
            cooldown=max(u.cooldown, m.cooldown),
            done_rows=u.rows if m.state == "done" and u.rows else
            u.done_rows, share=m.share))
    return out + [m for m in took.values() if m.kind]


def _quantile(xs, q):
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * q
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _overlap(units):
    """The most finished units that ran at one time: the concurrency the
    run measured, where nobody says how many workers it has."""
    edges = []
    for u in units:
        if u.ended is not None and u.seconds:
            edges += [(u.ended - u.seconds, 1), (u.ended, -1)]
    most = at = 0
    for _, step in sorted(edges, key=lambda e: (e[0], e[1])):
        at += step
        most = max(most, at)
    return most


def schedule(times, after, workers, cooldown=None):
    """(wall seconds, critical path seconds, active seconds) of running the
    units `times` ({name: seconds}) on `workers` at once, each after the
    units it waits for (`after`, {name: names}). Parallel work is counted
    once: the wall time is the schedule's length, never the sum. The unit
    with the longest path to the end goes first; a wait on a unit not in
    `times` is a wait on finished work; a cycle is broken at its longest
    unit rather than waited on for ever."""
    import heapq
    cooldown = cooldown or {}
    deps = {n: {d for d in after.get(n, ()) if d in times and d != n}
            for n in times}
    below = {n: [m for m in times if n in deps[m]] for n in times}
    tail = {}

    def tail_of(n, path=()):
        if n in tail:
            return tail[n]
        if n in path:
            return 0.0
        tail[n] = times[n] + cooldown.get(n, 0.0) + max(
            (tail_of(m, path + (n,)) for m in below[n]), default=0.0)
        return tail[n]

    for n in times:
        tail_of(n)
    free = [0.0] * max(1, int(workers or 1))
    ends, left = {}, set(times)
    while left:
        ready = [n for n in left if deps[n] <= set(ends)]
        if not ready:
            first = max(left, key=lambda n: (tail[n], n))
            deps[first] = set()
            ready = [first]
        n = max(ready, key=lambda m: (tail[m], m))
        start = max([heapq.heappop(free)] + [ends[d] for d in deps[n]])
        ends[n] = start + cooldown.get(n, 0.0) + times[n]
        heapq.heappush(free, ends[n])
        left.discard(n)
    return (max(ends.values(), default=0.0), max(tail.values(), default=0.0),
            sum(times.values()))


def _estimate_rungs():
    """What an estimate stands on, best first, as the decision engine's
    rungs: this run's own recent units, the rate an earlier run of the
    hop kept, or nothing measured."""
    from .decide import Need, Rung
    return (
        Rung("eta.window", "this run's recent units",
             needs=(Need("finished units measured here",
                         lambda f: f["measured"] >= f["warmup"],
                         why=lambda f: f"{f['measured']} of {f['warmup']}"
                         " units measured here so far"),),
             because="the units this run finished last measure the servers"
                     " and the link as they are now"),
        Rung("eta.kept", "the rate an earlier run of this hop kept",
             needs=(Need("an earlier run's rate",
                         lambda f: f["kept_rate"] is not None,
                         why="no earlier run of this hop kept a rate"),),
             because="an earlier run on the same servers, until this one"
                     " has measured its own"),
        Rung("eta.none", "nothing measured yet",
             because="no rate is measured, so no time is said"),
    )


def forecast(units, kind="move", scope="", workers=None, health=(),
             now=None, kept_rate=None, undiscovered=False, history=None,
             warmup=WARMUP_UNITS):
    """When the remaining work of one task ends, from the work itself.

    `units` are the ledger's units of the task, finished and not; what is
    left is their rows and bytes not yet done, each unit timed at the
    rates finished units measured in the recent window (`WINDOW`: its
    median, its 10th and 90th percentiles for the range), widened by the
    retries those units needed, with cooldowns waited out, and scheduled
    on the workers under the units' dependencies (`schedule`: the critical
    path and the concurrency, parallel work counted once). `health` is
    what each side reports (`throttle.Health`); `kept_rate` rows a second
    an earlier run kept, or a reader of it, asked only where this run has
    not measured enough (`decide.Facts`); `undiscovered` that more units
    may exist than are listed. `history` (`EstimateHistory`) keeps the
    estimate and is read for the finished units, so a resumed run measures
    from where it was, and says a revision or a stale earlier estimate.

    An `ETA` with its range, confidence, when it was measured, evidence,
    scope, the known work left apart from the unmeasured, and what to do
    next - and no time where the evidence holds none: warmup, nothing
    measured, a stall."""
    from .decide import Facts, climb
    now = time.time() if now is None else float(now)
    scope = scope or TASK_KINDS.get(kind, kind)
    units, earlier = list(units), []
    if history is not None:
        # a unit kept as finished and not listed is done work that stands;
        # one listed again as owed is that unit's earlier time, evidence of
        # the rate but not work done
        listed = {u.name: u for u in units}
        for u in history.finished(scope):
            if u.name not in listed:
                units.append(u)
            elif listed[u.name].state != "done":
                earlier.append(u)
    this_run = [u for u in units if u.state == "done"
                and u.seconds and u.seconds > 0]
    finished = sorted(earlier + this_run, key=lambda u: (u.ended or 0.0))
    left = [u for u in units if u.state != "done"]
    window = finished[-WINDOW:]
    row_rates = [u.rows / u.seconds for u in window if u.rows]
    byte_rates = [u.bytes / u.seconds for u in window if u.bytes]
    durations = [u.seconds for u in window]
    evidence = []
    facts = Facts({"measured": len(window), "warmup": int(warmup)},
                  kept_rate=kept_rate if callable(kept_rate)
                  else (lambda: kept_rate))
    choice = climb(f"when {scope} ends", _estimate_rungs(), facts=facts)
    rows_left = sum(max(0.0, u.rows - u.done_rows) for u in left
                    if u.rows is not None)
    bytes_left = sum(u.bytes for u in left
                     if u.bytes is not None and u.rows is None)
    unsized = [u for u in left if u.rows is None and u.bytes is None]
    known = {"units": len(left), "rows": rows_left, "bytes": bytes_left,
             "unmeasured_units": len(unsized)}
    at = stamp(now)

    def made(seconds, lo, hi, conf, words, how, step, active=None):
        got = ETA(seconds, lo, hi, conf, words, basis=how, measured_at=at,
                  scope=scope, evidence=tuple(evidence), known_left=known,
                  unmeasured=len(unsized), active_seconds=active,
                  undiscovered=bool(undiscovered), next_step=step)
        return history.keep(got, now) if history is not None else got

    if not left and not undiscovered:
        evidence.append(f"all {len(units)} units are done")
        return made(0.0, 0.0, 0.0, 1.0, human_secs(0), "done",
                    "nothing is left to wait for")
    evidence.append(f"stands on {choice.said}: {choice.reason}")
    if choice.path == "eta.none":
        evidence.append(f"{len(left)} units left, {len(unsized)} of them"
                        " not yet measured")
        return made(None, None, None, 0.0,
                    f"not enough evidence yet: {len(window)} of {warmup}"
                    " units measured", "insufficient",
                    f"wait for {int(warmup) - len(window)} more units to"
                    " finish; the time is said from them")
    if choice.path == "eta.kept":
        row_rates = [float(facts["kept_rate"])]
        evidence.append(f"an earlier run kept {row_rates[0]:,.0f} rows a"
                        " second; this run has not measured enough of its"
                        " own")

    def took(u, q):
        """Seconds unit `u` takes at the rates' `q` quantile."""
        share = 1.0 - (u.done_rows / u.rows if u.rows else 0.0)
        if u.rows is not None and row_rates:
            return max(0.0, u.rows - u.done_rows) / _quantile(row_rates, q)
        if u.bytes is not None and byte_rates:
            return u.bytes * share / _quantile(byte_rates, q)
        if durations:
            return _quantile(durations, 1.0 - q) * share
        return 0.0

    tries = sum(max(0, u.attempts - 1) for u in window)
    retry = tries / max(1, len(window))
    if tries:
        evidence.append(f"{tries} extra attempts over {len(window)} finished"
                        f" units: each unit left counts {1 + retry:.2f} times"
                        " its own work")
    failed = sum(1 for u in left if u.state == "failed")
    if failed:
        evidence.append(f"{failed} failed units are owed again")
    cool = {u.name: u.cooldown for u in left if u.cooldown}
    if cool:
        evidence.append(f"{len(cool)} units wait out a cooldown"
                        f" ({human_secs(max(cool.values()))} the longest)")
    running = sum(1 for u in left if u.state == "running")
    workers = int(workers or max(running, _overlap(window), 1))
    after = {u.name: tuple(u.after) for u in left}
    timed = {}
    for which, q, factor in (("low", 0.9, 1.0), ("mid", 0.5, 1.0 + retry),
                             ("high", 0.1, 1.0 + 2 * retry)):
        timed[which] = schedule({u.name: took(u, q) * factor for u in left},
                                after, workers, cool)
    wall, critical, active = timed["mid"]
    # a few alike units are still a few: the range is never narrower than
    # what that many samples can say
    few = 1.0 / math.sqrt(max(1, len(window)))
    lo = min(timed["low"][0], wall * (1.0 - few / 2))
    hi = max(timed["high"][0], wall * (1.0 + few))
    evidence.append(f"{len(left)} units left, {workers} at a time:"
                    f" {human_secs(active)} of work, the longest chain"
                    f" {human_secs(critical)}")
    conf = min(1.0, len(window) / 6.0) if choice.path == "eta.window" \
        else 0.25
    if len(row_rates) >= 2:
        mid = _quantile(row_rates, 0.5)
        spread = _quantile(row_rates, 0.9) - _quantile(row_rates, 0.1)
        conf *= max(0.2, 1.0 - min(0.8, spread / mid / 2.0))
    if choice.path == "eta.kept":
        lo, hi = wall * 0.5, wall * 3.0
    cond, step = "", "nothing to do: it goes on by itself"
    if unsized and (row_rates or byte_rates):
        conf *= max(0.2, 1.0 - len(unsized) / len(left))
        cond = (f"if the {len(unsized)} units not yet measured are like"
                " the ones finished, ")
        step = "nothing to do; the range narrows as those units are sized"
    if undiscovered:
        conf *= 0.5
        hi = None
        cond = cond or "for the units found so far, "
        evidence.append("more units may be found than are listed: the time"
                        " covers the listed ones only")
        step = "nothing to do; the time grows as more units are found"
    half = len(window) // 2
    if half >= 2:
        old = _quantile(durations[:half], 0.5)
        new = _quantile(durations[half:], 0.5)
        if old and new and max(old / new, new / old) >= SHIFT:
            conf *= 0.6
            evidence.append(f"units now take {human_secs(new)} against"
                            f" {human_secs(old)} before: the workload is"
                            " changing")
            lo = min(lo, wall * min(1.0, new / old))
            if hi is not None:
                hi = max(hi, wall * max(1.0, old / new))
    for h in health or ():
        why = h.stressed() if h is not None else ""
        if why:
            # a side's own measure of how much of its work went wrong, or
            # the pressure's fixed allowance where it says none
            conf *= float(getattr(h, "trust", 0.7))
            widen = float(getattr(h, "widen", 1.5))
            if hi is not None:
                hi = max(hi * widen, wall * widen)
            evidence.append(f"a side is under pressure ({why}): the work"
                            " is held back and may take longer"
                            + (f", the range opened {widen:.1f} times"
                               if hi is not None else ""))
            step = ("nothing to do; the work slows itself while a side is"
                    " under pressure")
    last = max((u.ended for u in this_run if u.ended), default=None)
    usual = _quantile(durations, 0.5)
    # a unit its attempts say has made no progress for many times the
    # usual unit is stalled, however the others go: the time cannot hold
    # it, and where every running unit is so, no time is said at all
    stuck = sorted((u for u in left if u.state == "running"
                    and u.idle is not None and usual
                    and u.idle > STALL_UNITS * usual),
                   key=lambda u: -u.idle)
    look = ("look at what the stalled units wait on: a lock, a connection"
            " or a side out of room")
    if stuck:
        worst = stuck[0]
        evidence.append(
            f"{len(stuck)} of {running} running units made no progress"
            f" for {human_secs(stuck[-1].idle)} or more, up to"
            f" {worst.idle / usual:.0f} times the usual unit: "
            + ", ".join(u.name for u in stuck[:3])
            + (f" and {len(stuck) - 3} more" if len(stuck) > 3 else ""))
        if len(stuck) >= running:
            return made(None, wall, None, round(min(conf, 0.1), 2),
                        f"stalled: {worst.name} made no progress for"
                        f" {human_secs(worst.idle)}", "stalled", look,
                        active)
        conf *= max(0.1, 1.0 - len(stuck) / running)
        hi = None
        step = look
    if running and last is not None and usual and \
            now - last > STALL_UNITS * usual:
        evidence.append(f"no unit has finished for {human_secs(now - last)},"
                        f" {(now - last) / usual:.0f} times the usual unit")
        return made(None, wall, None, round(min(conf, 0.1), 2),
                    f"stalled: no unit finished for {human_secs(now - last)}",
                    "stalled", "look at what the running units wait on: a"
                    " lock, a connection or a side out of room", active)
    conf, lo, hi = _held_to_record(history, kind, conf, lo, hi, wall,
                                   evidence)
    conf = round(max(0.05, min(1.0, conf)), 2)
    rng = (f"{human_secs(lo)} to {human_secs(hi)}" if hi is not None
           else f"at least {human_secs(lo)}")
    how = "conditional" if cond or choice.path == "eta.kept" else "measured"
    if stuck:
        cond, how = f"if the {len(stuck)} stalled units go on, ", \
            "conditional"
    return made(wall, lo, hi, conf, f"{cond}{human_secs(wall)} ({rng},"
                f" {int(conf * 100)}% confidence)"
                + (f"; {len(stuck)} stalled" if stuck else ""), how, step,
                active)


def _held_to_record(history, kind, conf, lo, hi, wall, evidence):
    """The confidence and range of an estimate held to how the latest
    estimates of its kind of task ended (`EstimateHistory.calibration`):
    where fewer of them ended inside their range than the confidence they
    stated, the confidence is lowered to the share that did and the range
    opened by their median error, and that is said. (conf, lo, hi)."""
    if history is None or wall is None:
        return conf, lo, hi
    try:
        cal = history.calibration(kind)
    except Exception:  # noqa: BLE001 - an unreadable record holds nothing
        return conf, lo, hi
    if not cal or cal["n"] < CALIBRATION_LEAST \
            or cal["coverage"] >= cal["stated"]:
        return conf, lo, hi
    err = cal["error"]
    evidence.append(
        f"the last {cal['n']} estimates of {TASK_KINDS.get(kind, kind)}"
        f" ended inside their range {cal['inside']} times against the"
        f" {cal['stated']:.0%} confidence they said, off by {err:.0%} at"
        " the median: the confidence is lowered to what they kept and the"
        " range opened by that error")
    conf = min(conf, conf * cal["coverage"] / cal["stated"])
    lo = None if lo is None else min(lo, wall * max(0.0, 1.0 - err))
    hi = None if hi is None else max(hi, wall * (1.0 + err))
    return conf, lo, hi


def catch_up(backlog, ingest, applied, scope="", now=None, spread=(),
             history=None):
    """When a change tail catches up: `backlog` changes waiting, arriving
    at `ingest` a second and applied at `applied` a second, both measured
    over one recent window (`spread`: that window's apply rates, for the
    range). Kept apart from the bulk work's estimate, and never a time
    where the apply does not outrun what arrives: the gap then grows or
    holds, and that is what is said."""
    now = time.time() if now is None else float(now)
    scope = scope or TASK_KINDS["follow"]
    ev = []

    def made(seconds, lo, hi, conf, words, how, step):
        got = ETA(seconds, lo, hi, conf, words, basis=how,
                  measured_at=stamp(now), scope=scope, evidence=tuple(ev),
                  known_left={"changes": backlog}, next_step=step)
        return history.keep(got, now) if history is not None else got

    missing = [n for n, v in (("the changes waiting", backlog),
                              ("the rate they arrive at", ingest),
                              ("the rate they are applied at", applied))
               if v is None]
    if missing:
        ev.append("not measured: " + ", ".join(missing))
        return made(None, None, None, 0.0, "not enough evidence yet: "
                    + ", ".join(missing) + " not measured", "insufficient",
                    "let the tail run a little longer; the rates are"
                    " measured as it goes")
    ev.append(f"{backlog:,.0f} changes waiting, {ingest:,.1f} a second"
              f" arrive, {applied:,.1f} a second applied")
    if backlog <= 0:
        return made(0.0, 0.0, 0.0, 1.0, "caught up", "done",
                    "nothing to do: the tail is caught up")
    gain = applied - ingest
    if gain <= 0:
        return made(None, None, None, 0.0,
                    "falling behind: changes arrive as fast as or faster"
                    " than they are applied, so no catch-up time can be"
                    " given", "behind",
                    "give the apply more room (more workers, larger"
                    " batches) or wait for writes on the source to ease")
    rates = [r for r in spread if r and r > 0]
    worst, best = (min(rates), max(rates)) if len(rates) >= 2 \
        else (applied, applied)
    sec = backlog / gain
    lo = backlog / max(gain, best - ingest)
    hi = backlog / (worst - ingest) if worst > ingest else None
    if hi is None:
        ev.append("at the slowest apply rate measured the gap would grow")
    conf = min(1.0, len(rates) / 6.0) if rates else 0.3
    conf, lo, hi = _held_to_record(history, "follow", conf, lo, hi, sec, ev)
    conf = round(conf, 2)
    rng = (f"{human_secs(lo)} to {human_secs(hi)}" if hi is not None
           else f"at least {human_secs(lo)}, unbounded at the slowest rate")
    return made(sec, lo, hi, conf, f"{human_secs(sec)} ({rng},"
                f" {int(conf * 100)}% confidence)", "measured",
                "nothing to do: the tail is catching up")


class EstimateHistory:
    """The estimates said of a run and the units it finished, kept in the
    run's ledger as journal lines (`durable.append_line`): a resumed run,
    or one after a reboot, measures from the units already finished
    rather than from nothing, says an earlier estimate that has gone
    stale, and explains a revision that leaves the earlier range."""

    def __init__(self, path, stale_after=STALE_SECONDS, keep_every=0.0):
        self.path = Path(path)
        self.stale_after = float(stale_after)
        #: an estimate like the last one kept, younger than this many
        #: seconds, is said but not written again: a loop that asks every
        #: second keeps its journal short
        self.keep_every = float(keep_every)

    def _lines(self):
        from . import durable
        return [e for e in durable.read_lines(self.path, "cache")
                if isinstance(e, dict)]

    def done(self, scope, unit, now=None):
        """Keep a finished unit's measurement for `scope`."""
        import json
        from . import durable
        ended = unit.ended if unit.ended is not None else \
            (time.time() if now is None else float(now))
        row = unit._replace(state="done", ended=ended)._asdict()
        row["after"] = list(unit.after)
        durable.append_line(self.path, json.dumps({"scope": scope,
                                                   "unit": row}))

    def finished(self, scope):
        """The finished units kept for `scope`, each by its latest line."""
        got = {}
        for e in self._lines():
            u = e.get("unit")
            if e.get("scope") != scope or not isinstance(u, dict):
                continue
            u = dict(u, after=tuple(u.get("after") or ()))
            try:
                got[u["name"]] = Unit(**u)
            except (TypeError, KeyError):
                continue
        return [u for u in got.values() if u.state == "done"]

    def forget(self, scope, names):
        """Units invalidated since they finished (a changed table, a new
        partition): no longer counted done, so the ledger owes them again;
        the rest of the finished work stands."""
        import json
        from . import durable
        for n in names:
            durable.append_line(self.path, json.dumps(
                {"scope": scope, "unit": Unit(n, state="pending")._asdict()
                 | {"after": []}}))

    def latest(self):
        """{scope: the last estimate kept for it (a dict, with `at`)},
        for the report's time left."""
        got = {}
        for e in self._lines():
            if isinstance(e.get("estimate"), dict) and e.get("scope"):
                got[e["scope"]] = e["estimate"]
        return got

    def last(self, scope):
        """The last estimate kept for `scope` (a dict), or None."""
        got = None
        for e in self._lines():
            if e.get("scope") == scope and isinstance(e.get("estimate"),
                                                      dict):
                got = e["estimate"]
        return got

    def keep(self, eta, now):
        """`eta` held against the earlier estimate - stale where it is old,
        a revision said where its end leaves the earlier range - and kept
        as the latest."""
        import json
        from . import durable
        prev = self.last(eta.scope)
        stale, revised, ev = False, "", list(eta.evidence)
        if prev is not None:
            then = float(prev.get("at") or 0.0)
            if now - then > self.stale_after:
                stale = True
                ev.append(f"the earlier estimate was {human_secs(now - then)}"
                          " old and is measured again")
            plo, phi = prev.get("lower_bound"), prev.get("upper_bound")
            if eta.seconds is not None and plo is not None:
                end = now + eta.seconds
                if end < then + plo or (phi is not None and end > then + phi):
                    was = then + (prev.get("seconds") or plo)
                    revised = (f"revised {'later' if end > was else 'sooner'}"
                               f" by {human_secs(abs(end - was))}, out of the"
                               " earlier range: " + (eta.evidence[-1]
                                                     if eta.evidence else
                                                     "measured since"))
            elif eta.seconds is None and prev.get("seconds") is not None:
                revised = f"no time any more: {eta.formatted}"
            if revised:
                ev.append(revised)
        got = eta._replace(stale=stale, revised=revised, evidence=tuple(ev))
        if prev is not None and not revised and eta.basis != "done" \
                and prev.get("basis") == eta.basis \
                and now - float(prev.get("at") or 0.0) < self.keep_every:
            return got
        durable.append_line(self.path, json.dumps(
            {"scope": eta.scope, "estimate": dict(got.as_evidence(), at=now)},
            default=str))
        if eta.basis == "done":
            self._score(eta.scope, now)
        return got

    def _score(self, scope, now):
        """A task found done: each timed estimate said of it since it was
        last scored held to when it really ended - inside its range or
        not, its error against the time that was left, the confidence it
        stated - kept for its kind (`calibration`). At most
        `SCORED_A_TASK`, spread over the run, so a loop that asked often
        does not outweigh one that asked seldom."""
        import json
        from . import durable
        since, said = None, []
        for e in self._lines():
            if e.get("scope") != scope:
                continue
            if e.get("scored") is not None:
                since, said = float(e.get("upto") or 0.0), []
            est = e.get("estimate")
            if isinstance(est, dict) and est.get("seconds") is not None \
                    and est.get("basis") in ("measured", "conditional") \
                    and (since is None
                         or float(est.get("at") or 0.0) > since):
                said.append(est)
        if not said:
            return
        step = max(1, math.ceil(len(said) / SCORED_A_TASK))
        scored = []
        for est in said[::step][-SCORED_A_TASK:]:
            at = float(est["at"])
            lo, hi = est.get("lower_bound"), est.get("upper_bound")
            inside = (lo is None or now >= at + float(lo)) and (
                hi is None or now <= at + float(hi))
            took = max(1.0, now - at)
            scored.append({
                "inside": bool(inside),
                "error": round(abs(at + float(est["seconds"]) - now) / took,
                               4),
                "stated": float(est.get("confidence") or 0.0)})
        durable.append_line(self.path, json.dumps(
            {"scope": scope, "kind": kind_of(scope), "upto": now,
             "scored": scored}))

    def calibration(self, kind, last=CALIBRATION_LAST):
        """How the latest `last` scored estimates of `kind` held: {"n",
        "inside" (how many ended inside their range), "coverage" (that
        share), "stated" (the confidence they said, on average), "error"
        (their median error against the time that was left)}, or None
        where none is scored."""
        got = []
        for e in self._lines():
            if e.get("kind") == kind and isinstance(e.get("scored"), list):
                got.extend(x for x in e["scored"] if isinstance(x, dict))
        got = got[-int(last):]
        if not got:
            return None
        inside = sum(1 for x in got if x.get("inside"))
        return {"n": len(got), "inside": inside,
                "coverage": inside / len(got),
                "stated": sum(float(x.get("stated") or 0.0) for x in got)
                / len(got),
                "error": _quantile([float(x.get("error") or 0.0)
                                    for x in got], 0.5)}

    def calibrations(self):
        """{kind: `calibration`} for every kind of task scored here, for
        the report."""
        kinds = []
        for e in self._lines():
            k = e.get("kind")
            if k and isinstance(e.get("scored"), list) and k not in kinds:
                kinds.append(k)
        return {k: self.calibration(k) for k in kinds}


class PrivateFile:
    """A file readable by this user only, for as long as a program needs
    it, then gone: where a secret goes instead of a command line, which
    every process listing on the machine can read. Made new under a name
    of its own (`O_EXCL`, mode 0600 from the start, never after), so two
    copies running side by side never share one; in memory where the
    system keeps a directory there (`/dev/shm`), so it never reaches a
    disk."""

    def __init__(self, text, suffix=""):
        self.text, self.suffix, self.path = text, suffix, None

    def __enter__(self):
        import os
        import tempfile
        shm = "/dev/shm"
        where = shm if os.path.isdir(shm) and os.access(shm, os.W_OK) \
            else None
        fd, self.path = tempfile.mkstemp(prefix="migkit-", suffix=self.suffix,
                                         dir=where)
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(self.text)
        return self.path

    def __exit__(self, *exc):
        import os
        try:
            os.unlink(self.path)
        except OSError:
            pass
        return False


def report_name(name):
    """A file or directory name made from a table's name, which the
    database lets hold anything: `/` and `..` wrote outside the report
    directory, and a newline in a table name added a section to the
    filter file a copy program read. Letters, digits, `_`, `.` and `-`
    are kept, the rest become `_`, and a short digest of the name as it
    was keeps two names apart that come out the same (`a/b`, `a_b`)."""
    import hashlib
    import re
    text = str(name)
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", text).lstrip(".") or "_"
    tag = hashlib.sha256(text.encode()).hexdigest()[:8]
    return f"{safe[:80]}-{tag}"


def diff_run_config(url1, table1, url2, table2):
    """The comparison program's run, as its own configuration file reads it
    (`--conf`), so neither address - passwords and all - is on its command
    line."""
    import json
    return ("[run.default]\n"
            f"1 = {{database = {json.dumps(url1)}, table = {json.dumps(table1)}}}\n"
            f"2 = {{database = {json.dumps(url2)}, table = {json.dumps(table2)}}}\n")


# ---- one table name, split and quoted one way -------------------------------

#: each engine's quotes for a name, where they are not `"`
QUOTES = {"mysql": ("`", "`"), "tidb": ("`", "`"), "clickhouse": ("`", "`"),
          "mssql": ("[", "]"), "ase": ("[", "]")}

#: engines whose tables sit straight in a database: a name has no schema
#: part, and a dot in it is the name's own (a MongoDB collection
#: `system.profile`, a topic `orders.v1`)
NO_SCHEMA = ("mysql", "tidb", "clickhouse", "mongodb", "cosmosdb", "redis",
             "kafka", "kinesis", "pubsub", "dynamodb", "cassandra",
             "opensearch", "bigtable", "firestore", "datastore",
             "timestream", "lindorm", "neptune")


def _canonical(engine):
    """The engine whose names `engine`'s are: its alias's, and of an
    engine on another's wire (`engines.WIRE`), that one's - OceanBase's
    tables are named as MySQL's, Keyspaces' as Cassandra's."""
    from .engines import ALIASES, WIRE
    got = ALIASES.get(engine, engine)
    return WIRE.get(got, got)


def _parts(engine, name):
    """The parts of a name, a quoted one unquoted whole - a dot inside
    quotes is the name's own - and, where the engine has schemas, the
    unquoted text split at its dots."""
    opens = {'"': '"'}
    q = QUOTES.get(engine)
    if q:
        opens[q[0]] = q[1]
    flat = engine in NO_SCHEMA
    parts, cur, quoted = [], "", False
    s, i = str(name), 0
    while i < len(s):
        ch = s[i]
        if ch in opens and not cur and not quoted:
            close, j, got = opens[ch], i + 1, ""
            while j < len(s):
                if s[j] == close:
                    if s[j + 1:j + 2] == close:
                        got += close
                        j += 2
                        continue
                    break
                got += s[j]
                j += 1
            if j < len(s):
                cur, quoted, i = got, True, j + 1
                continue
            # never closed: not a quoted name after all
        if ch == "." and (quoted or not flat):
            parts.append(cur)
            cur, quoted = "", False
            i += 1
            continue
        cur += ch
        i += 1
    parts.append(cur)
    return parts


#: the schema a table named without one is in, where the engine reads a
#: bare name in one - PostgreSQL's `regclass` on the search path
DEFAULT_SCHEMA = {"postgres": "public"}


def split_table(engine, name, default=""):
    """(schema, table) of a table's name, plain (`public.orders`) or
    quoted (`public."v1.orders"`, `` `a.b` ``, `[dbo].[x]`), one way for
    every caller. Five ways split it before (audit 2026-10-01, DU14): at
    the first dot, at the last, at every one, the last part only -
    measured with `public."v1.orders"`, the copy left out a table the
    check compared, and a digest named `"public"."v1"."orders"`.

    A dot inside quotes is the name's; unquoted, the first dot ends the
    schema, since a table's name may hold dots and a schema's rarely
    does. An engine with no schemas (`NO_SCHEMA`) keeps an unquoted dot
    as the name's own, and its first part is the database a quoted name
    carries. A name with no schema has `default`."""
    parts = _parts(_canonical(engine), name)
    if len(parts) == 1:
        return default, parts[0]
    return parts[0], ".".join(parts[1:])


def quote_name(engine, name):
    """One name quoted as `engine` quotes it, its quote doubled inside."""
    left, right = QUOTES.get(_canonical(engine), ('"', '"'))
    return left + str(name).replace(right, right + right) + right


def qualify(engine, schema, table):
    """A table's name as the engine's SQL reads it: `"public"."v1.orders"`,
    `` `orders` ``, `[dbo].[orders]` - the schema left out where there
    is none."""
    return ".".join(quote_name(engine, p) for p in (schema, table) if p)


# ---- what a deep check read, and what it could not --------------------------

class Probes:
    """What a check planned to read and what it could not, turned into the
    result's status and words (audit 2026-10-01, LG2).

    A deep check reads one column, one shard, one statistic at a time, and
    a read that failed used to be dropped with a `continue` while the
    result still said `ok`: MongoDB's duplicate-`_id` probe failing read
    "sharded: no dup _id", and MySQL's character-set check over the first
    30 columns said ok over a table of 45. Here each planned read is
    `done` or `failed`; `verdict` makes an `ok` over a part a `warn`
    ("compared 12 of 30 columns; not read: a.b (timeout)") and over
    nothing a `skip`, and gives any other status the same words."""

    def __init__(self, what="columns", planned=()):
        self.what = what
        self.planned = []
        self.read = []
        self.missed = {}
        self.plan(*planned)

    def plan(self, *names):
        for n in names:
            if str(n) not in self.planned:
                self.planned.append(str(n))
        return self

    def done(self, name):
        name = str(name)
        self.plan(name)
        self.missed.pop(name, None)
        if name not in self.read:
            self.read.append(name)

    def failed(self, name, why):
        name = str(name)
        self.plan(name)
        text = str(why).strip().splitlines()
        self.missed[name] = (text[-1] if text
                             else type(why).__name__)[:120]
        if name in self.read:
            self.read.remove(name)

    def reading(self, name):
        """`with probes.reading("a.b"):` - done when the block ends, failed
        with what it raised, which goes no further."""
        return _Reading(self, name)

    def unread(self):
        """{name: why} of every planned read that did not happen: failed,
        or never reached."""
        out = dict(self.missed)
        for n in self.planned:
            if n not in self.read and n not in out:
                out[n] = "not reached"
        return out

    def words(self):
        """"compared 12 of 30 columns; not read: a.b (timeout)", or ""
        where everything planned was read."""
        unread = self.unread()
        if not unread:
            return ""
        shown = [f"{n} ({w})" for n, w in list(unread.items())[:5]]
        more = len(unread) - len(shown)
        return (f"compared {len(self.read)} of {len(self.planned)}"
                f" {self.what}; not read: {', '.join(shown)}"
                + (f" and {more} more" if more > 0 else ""))

    def verdict(self, status, detail=""):
        """(status, detail) with what was not read counted: an `ok` over a
        part is `warn`, over nothing `skip`; any other status stands, its
        detail given the same words."""
        said = self.words()
        if not said:
            return status, detail
        if status == "ok":
            status = "warn" if self.read else "skip"
        return status, "; ".join(p for p in (detail, said) if p)


class _Reading:
    def __init__(self, probes, name):
        self.probes, self.name = probes, name

    def __enter__(self):
        self.probes.plan(self.name)
        return self

    def __exit__(self, kind, err, tb):
        if kind is None:
            self.probes.done(self.name)
            return False
        if not issubclass(kind, Exception):
            return False
        self.probes.failed(self.name, err)
        return True
