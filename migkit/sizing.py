"""How many things a move does at once, worked out rather than asked for.

An operator used to set `workers`, and a number right on one machine was
wrong on the next: too many for a two-CPU production source, too few for a
large target with nothing else to do. What the right number is depends on
everything at once - this machine's CPUs and free memory, how many
connections each server has free and how busy it is, how many CPUs it has,
how much work there is - and then on how fast the rows actually go.

So migkit reads every factor it can (`estimate`), starts from the least of
them, and while the move runs keeps the number where the rows go fastest
(`Pace`): more while more is faster, back at once when a server says it is
under strain. `workers` on a hop is only a ceiling now, for an operator who
wants one.

What the others do, and what was taken from them: pg_restore and pgcopydb
anchor on the server's cores; mydumper and pt-online-schema-change size a
chunk by how long it took; MongoDB's own throughput probing and the .NET
thread pool climb on measured throughput and keep a step only when it paid;
BBR probes upward now and then in case the ceiling moved; gh-ost backs off
on a server's load and replication lag.
"""
import math
import os
import threading
import time

#: memory one worker may hold: a read of the table copier (`READ_BYTES`,
#: 64 MB) and what the driver holds beside it - measured, 36 MB a copier on
#: a million rows - with room over it
WORKER_MEMORY = 128 * 2 ** 20
#: memory left to everything else on this machine
RESERVE_MEMORY = 512 * 2 ** 20
#: connections a worker holds on each server: one reading or writing, and
#: one for the digest or read-back beside it
CONNECTIONS_PER_WORKER = 2
#: the share of a server's free connections a move may take: less of a
#: primary that serves an application, more of a replica or a target that
#: nothing else uses yet
SHARE_PRIMARY, SHARE_OTHER = 0.25, 0.5
#: busy workers a server's CPU is started with: one on a primary source,
#: two on a replica or the target; the pace may go past it while that is
#: faster and the server does not complain
PER_CPU_PRIMARY, PER_CPU_OTHER = 1.0, 2.0
#: a factor nobody can read counts as this many, as `workers` used to
UNREAD = 4


def host():
    """This machine: CPUs it may use (a container's quota counted),
    available memory in bytes, and its load. A value it cannot read is
    None."""
    return {"cpus": _cpus(), "memory": _memory(), "load": _load()}


def _cpus(cgroup="/sys/fs/cgroup"):
    try:
        n = len(os.sched_getaffinity(0))
    except AttributeError:
        n = os.cpu_count() or 1
    # a container's quota: os.cpu_count() reports the host's CPUs inside it
    try:
        with open(f"{cgroup}/cpu.max") as f:
            quota, period = f.read().split()[:2]
        if quota != "max":
            n = min(n, max(1, math.ceil(int(quota) / int(period))))
    except (OSError, ValueError):
        try:
            with open(f"{cgroup}/cpu/cpu.cfs_quota_us") as f:
                quota = int(f.read())
            with open(f"{cgroup}/cpu/cpu.cfs_period_us") as f:
                period = int(f.read())
            if quota > 0 and period > 0:
                n = min(n, max(1, math.ceil(quota / period)))
        except (OSError, ValueError):
            pass
    return n


def _memory():
    try:
        import psutil
        return int(psutil.virtual_memory().available)
    except Exception:  # noqa: BLE001 - not readable here
        return None


def _load():
    try:
        return os.getloadavg()[0]
    except (AttributeError, OSError):
        return None


class Plan:
    """Where a move starts (`start`), how far its pace may go (`most`), and
    each factor's own number, for the log."""

    def __init__(self, start, most, factors):
        self.start, self.most, self.factors = start, most, factors

    def line(self):
        firm = sorted(((n, why) for why, (n, soft) in self.factors.items()
                       if not soft), key=lambda x: x[0])
        soft = sorted(((n, why) for why, (n, s) in self.factors.items()
                       if s), key=lambda x: x[0])
        said = [f"{why} {n}" for n, why in (soft[:2] + firm[:2])]
        return (f"{self.start} at a time to start"
                + (f", up to {self.most} as it goes" if self.most
                   > self.start else "")
                + (f" ({'; '.join(said)})" if said else ""))


def capacity(eng, side, db):
    """What a server says about itself (`Engine.capacity`), {} where it
    says nothing or the question fails."""
    fn = getattr(eng, "capacity", None)
    if fn is None:
        return {}
    try:
        return fn(side, db) or {}
    except Exception:  # noqa: BLE001 - a question never stops a move
        return {}


def estimate(hop, eng, db, units=None, here=None, sides=None):
    """The `Plan` for one database of a move: every factor read, the start
    the least of them, the most the least of the firm ones.

    Firm - never passed: this machine's memory, each server's share of its
    free connections, the work there is, and a ceiling the hop sets. Soft -
    where it starts, and passed only while it is faster and the servers are
    not under strain (`strain`): this machine's and each server's CPUs."""
    here = host() if here is None else here
    sides = sides if sides is not None else {
        s: capacity(eng, s, db) for s in ("src", "dst")}
    f = {}
    if here.get("cpus"):
        # a worker spends much of its time waiting on a server; its own
        # work per row is what the CPUs here bound
        f["this machine's CPUs"] = (2 * int(here["cpus"]), True)
    if here.get("memory"):
        f["this machine's free memory"] = (max(1, int(
            (here["memory"] - RESERVE_MEMORY) // WORKER_MEMORY)), False)
    for side, name in (("src", "the source's"), ("dst", "the target's")):
        c = sides.get(side) or {}
        other = side == "dst" or bool(c.get("replica"))
        if c.get("free_connections") is not None:
            share = SHARE_OTHER if other else SHARE_PRIMARY
            f[f"{name} free connections"] = (max(1, int(
                share * int(c["free_connections"]) // CONNECTIONS_PER_WORKER)),
                False)
        if c.get("cpus"):
            per = PER_CPU_OTHER if other else PER_CPU_PRIMARY
            # half of what already runs there: a session is rarely on a CPU
            # the whole time, and a quiet server's own background work is
            # not the application's load
            f[f"{name} CPUs"] = (max(1, int(int(c["cpus"]) * per
                                            - int(c.get("running") or 0)
                                            // 2)), True)
        else:
            f[f"{name} CPUs, not readable,"] = (UNREAD, True)
    if units:
        f["the work there is"] = (max(1, int(units)), False)
    if getattr(hop, "workers_set", False) and getattr(hop, "workers", None):
        f["the hop's ceiling"] = (max(1, int(hop.workers)), False)
    firm = [n for n, soft in f.values() if not soft]
    soft = [n for n, s in f.values() if s]
    # nothing firm read: no more than twice where it starts
    most = min(firm) if firm else 2 * max(1, min(soft or [UNREAD]))
    start = max(1, min([most] + soft))
    return Plan(start, max(start, most), f)


#: sessions running on a server, per CPU of it, past which it is under
#: strain - its run queue is deeper than its CPUs can take
RUNNING_PER_CPU = 2.0


def strain(eng, db):
    """Why the move should do less at once, from the servers' CPUs: a side
    running more sessions than `RUNNING_PER_CPU` a CPU. "" when neither is,
    or neither says."""
    for side, name in (("src", "source"), ("dst", "target")):
        c = capacity(eng, side, db)
        cpus, running = c.get("cpus"), c.get("running")
        if cpus and running is not None and running >= RUNNING_PER_CPU * cpus:
            return f"{name}: {running} sessions running on {cpus} CPUs"
    return ""


def fit(hop, eng, db, log=None, units=None):
    """`estimate`, and the hop's `workers` set to its start - which every
    path that takes a fixed number (a bulk program's jobs, the check's
    threads) uses. The plan is returned for the paths that pace."""
    plan = estimate(hop, eng, db, units)
    hop.workers = plan.start
    hop.workers_most = plan.most
    if log:
        log(plan.line())
    return plan


class Pace:
    """How many of a move's workers run at once, found while it runs.

    Each finished unit of work reports its weight (rows) and its time; a
    window of them gives a rate. The number is moved one way at a time and
    the rate after compared with the rate before:
    * a step up that paid (5% or more) is kept and followed by another -
      twice as far while it pays 25% or more, never as far as a number that
      has already not paid;
    * a step up that did not pay is taken back to where it was, and that
      number remembered as not worth it, so the next look above goes only
      half the way there;
    * now and then a step down: kept when the rate holds within 5% - as
      fast with fewer is better for both servers - and taken back if not;
    * a server under strain (the `stress` callable's answer) cuts the
      number by 30% at once and holds it two windows.
    Never below one, never past `most`. What it did is in `history`."""

    KEEP, DOUBLE, CUT = 0.05, 0.25, 0.7
    WINDOW, LEAST_DONE, HOLD, LOOK_EVERY, FORGET = 3.0, 4, 2, 8, 32

    def __init__(self, start, most, stress=None, clock=time.monotonic):
        self.most = max(1, int(most))
        self.limit = max(1, min(int(start), self.most))
        self.stress = stress
        self._clock = clock
        self._lock = threading.Lock()
        self._weight, self._n, self._times = 0.0, 0, []
        self._began = clock()
        # the rate at the current number, and where the last move started
        self._ref, self._back, self._move = None, None, None
        self._hold, self._windows, self._growing = 0, 0, True
        self._not_above = self.most + 1
        self.history = [(self.limit, None, "start")]

    def done(self, weight=1, seconds=None):
        with self._lock:
            self._weight += max(0.0, float(weight or 0))
            self._n += 1
            if seconds is not None:
                self._times.append(float(seconds))
                del self._times[:-8]
            now = self._clock()
            took = now - self._began
            typical = (sorted(self._times)[len(self._times) // 2]
                       if self._times else 0.0)
            # while it is still growing, shorter windows: a short copy has
            # few of them, and the first steps up are the ones that pay
            least, window = ((2, self.WINDOW / 3) if self._growing
                             else (self.LEAST_DONE, self.WINDOW))
            if self._n < least or took < max(window, 2 * typical):
                return
            rate = self._weight / took if took > 0 else 0.0
            self._weight, self._n, self._began = 0.0, 0, now
            self._decide(rate)

    def _go(self, n, why, rate, move):
        n = max(1, min(self.most, int(n)))
        if n == self.limit:
            self._move = None
            return
        self._back, self._move = (self.limit, self._ref), move
        self.limit = n
        self.history.append((n, round(rate, 1), why))

    def _decide(self, rate):
        self._windows += 1
        if self._windows % self.FORGET == 0:
            # what was not worth it a while ago may be now
            self._not_above = self.most + 1
        why = ""
        if self.stress is not None:
            try:
                why = self.stress() or ""
            except Exception:  # noqa: BLE001 - a probe never stops a move
                why = ""
        if why:
            self._go(math.floor(self.limit * self.CUT), why, rate, None)
            self._hold, self._ref, self._growing = self.HOLD, None, False
            return
        if self._hold:
            self._hold -= 1
            self._ref = rate if self._ref is None else (self._ref + rate) / 2
            return
        if self._move == "up":
            gain = rate / self._back[1] - 1 if self._back[1] else 1.0
            if gain >= self.KEEP:
                self._ref = rate
                far = (self.limit if self._growing and gain >= self.DOUBLE
                       else max(1, round(self.limit * 0.1)))
                self._go(min(self.limit + far, self._not_above - 1),
                         f"{gain:+.0%} faster, up again", rate, "up")
                return
            self._not_above = min(self._not_above, self.limit)
            self._growing = False
            back, ref = self._back
            self._go(back, f"{gain:+.0%} at {self.limit}, not worth it",
                     rate, None)
            self._ref, self._hold = ref, 1
            return
        if self._move == "down":
            loss = 1 - rate / self._back[1] if self._back[1] else 0.0
            if loss <= self.KEEP:
                self._ref, self._move = rate, None
                return
            back, ref = self._back
            self._go(back, f"{-loss:+.0%} at {self.limit}, back up", rate,
                     None)
            self._ref, self._hold = ref, 1
            return
        self._ref = rate if self._ref is None else (self._ref + rate) / 2
        if len(self.history) == 1 and self.limit < self.most:
            self._go(self.limit + 1, "a step up to measure", rate, "up")
            return
        if self._windows % self.LOOK_EVERY == 0 \
                and self.limit + 1 < self._not_above \
                and self.limit < self.most:
            # half the way to what did not pay, and never more than a
            # quarter again at once: a look is not a leap on a source that
            # serves an application
            room = min(self._not_above - 1, self.most) - self.limit
            self._go(self.limit + max(1, min(room // 2,
                                             round(self.limit * 0.25))),
                     "a look above", rate, "up")
        elif self._windows % self.LOOK_EVERY == self.LOOK_EVERY // 2 \
                and self.limit > 1:
            self._go(self.limit - 1, "a look below", rate, "down")

    def line(self):
        """One line for the log: where it went, if anywhere."""
        seen = [n for n, _, _ in self.history]
        if len(set(seen)) < 2:
            return ""
        return (f"at a time: started at {seen[0]}, ran between {min(seen)}"
                f" and {max(seen)}, ended at {self.limit}")
