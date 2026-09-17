"""Keep verification from becoming the outage it went looking for.

A full-table checksum is a plain SELECT, which is why it is safe to run - and
also why nothing stops it. migkit's own data check ran with `workers` threads
per database, each asking Postgres for 8 parallel workers, against a two-vCPU
Aurora instance that was serving an application at the time. It pegged the CPU
and the instance restarted twice. The check was correct; the load was not.

The fix is not a flag telling the operator to guess a safe concurrency. Two
other projects that verify data at scale reached the same design independently:
Vitess VDiff subscribes its differ to the cluster's lag throttler, and Percona's
pt-table-checksum pauses on `--max-lag` and sizes each chunk to a target
runtime. Both take the signal from the database rather than from a human.

So: ask the server how it is doing, and get out of the way when the answer is
"badly". Three signals, in order of how universally they work:

1. **Our own latency.** If the same shape of query starts taking several times
   its early baseline, something is saturated - us, most likely. Needs no
   server-specific query and works on every engine, including ones where load
   is invisible from SQL.
2. **Session pressure.** Active sessions against the configured maximum.
3. **Replication lag**, when the side has any.

Under stress the gate both sleeps and *narrows*: permits come down, so fewer
tables are in flight, and come back up only after the server looks healthy
again. Everything it did is recorded, because a check that silently took three
times longer is its own kind of failure.
"""
import threading
import time

# How much slower than baseline counts as stress. Chosen wide: normal variance
# between a small and a large table is easily 2x, and a false positive here
# makes every check slower for no reason.
SLOW_FACTOR = 4.0
# Fraction of the server's configured session limit that counts as pressure.
BUSY_RATIO = 0.75
# Replication lag past this many seconds means the target is already behind
# and reading harder will not help it catch up.
LAG_SECONDS = 30.0
# Never sample health more often than this: the probe must not become load.
PROBE_EVERY = 5.0
# Backoff bounds per wait.
MIN_SLEEP, MAX_SLEEP = 0.5, 30.0
# Longest one unit of work will wait for the server to look better before
# going ahead anyway, at minimum concurrency. A permanently busy database
# would otherwise mean the check never runs at all, which is worse than a
# check that runs slowly: the point is to stay out of the way, not to give up.
MAX_WAIT_PER_UNIT = 60.0


class Health:
    """What one side reports about itself. Any field may be None: a managed
    provider that hides a signal must not make the throttle fail closed."""

    def __init__(self, busy_ratio=None, lag_seconds=None, note=""):
        self.busy_ratio = busy_ratio
        self.lag_seconds = lag_seconds
        self.note = note

    def stressed(self):
        if self.busy_ratio is not None and self.busy_ratio >= BUSY_RATIO:
            return f"sessions at {self.busy_ratio:.0%} of the limit"
        if self.lag_seconds is not None and self.lag_seconds >= LAG_SECONDS:
            return f"replication {self.lag_seconds:.0f}s behind"
        return ""


class Throttle:
    """A gate the heavy loop passes through, once per unit of work.

    probe() returns a Health, or None when the engine cannot report one - in
    which case the latency signal carries the whole job.
    """

    def __init__(self, permits, probe=None, clock=time.monotonic,
                 sleeper=time.sleep):
        self.max_permits = max(1, int(permits))
        self.permits = self.max_permits
        self._probe = probe
        self._clock = clock
        self._sleep = sleeper
        self._lock = threading.Condition()
        self._in_flight = 0
        self._baseline = None
        self._recent = []
        self._last_probe_at = None
        self._last_health = None
        self.waits = 0
        self.waited_seconds = 0.0
        self.proceeded_under_stress = 0
        self.narrowed_to = self.max_permits
        self.reasons = {}

    # ---- the signal ----

    def observe(self, seconds):
        """Feed the duration of one unit of work."""
        with self._lock:
            self._recent.append(seconds)
            del self._recent[:-8]
            if self._baseline is None:
                self._baseline = seconds
            else:
                # the baseline tracks the fastest thing we have seen, which is
                # the closest we get to "the server when it is not busy"
                self._baseline = min(self._baseline, seconds)
            self._lock.notify_all()

    def _slow(self):
        if self._baseline is None or len(self._recent) < 3:
            return ""
        recent = sorted(self._recent)[len(self._recent) // 2]
        if recent > max(self._baseline, 0.05) * SLOW_FACTOR:
            return (f"queries {recent / self._baseline:.1f}x slower than"
                    " this run's fastest")
        return ""

    def _health(self):
        """Cached health, resampled at most every PROBE_EVERY seconds."""
        if self._probe is None:
            return None
        now = self._clock()
        if (self._last_probe_at is not None
                and now - self._last_probe_at < PROBE_EVERY):
            return self._last_health
        self._last_probe_at = now
        try:
            self._last_health = self._probe()
        except Exception:
            # a probe that fails must never stop the check it is protecting
            self._last_health = None
        return self._last_health

    def stress(self):
        """Why we should back off, or empty string."""
        h = self._health()
        if h is not None:
            why = h.stressed()
            if why:
                return why
        return self._slow()

    # ---- the gate ----

    def gate(self):
        """Wait until it is reasonable to start another unit of work.

        Bounded: after MAX_WAIT_PER_UNIT the work goes ahead at minimum
        concurrency regardless. Throttling is about not being the heaviest
        thing on the server, not about refusing to verify a busy one.
        """
        waited = 0.0
        sleep_for = MIN_SLEEP
        while True:
            why = self.stress()
            out_of_patience = waited >= MAX_WAIT_PER_UNIT
            with self._lock:
                if why:
                    self.reasons[why] = self.reasons.get(why, 0) + 1
                    # narrow first: fewer things in flight is the real fix,
                    # sleeping is only how we wait for it to take effect
                    self.permits = max(1, self.permits - 1)
                    self.narrowed_to = min(self.narrowed_to, self.permits)
                elif self.permits < self.max_permits and not self._slow():
                    self.permits += 1
                clear = not why or out_of_patience
                if clear and self._in_flight < self.permits:
                    self._in_flight += 1
                    if waited:
                        self.waits += 1
                        self.waited_seconds += waited
                    if why and out_of_patience:
                        self.proceeded_under_stress += 1
                    return
            self._sleep(sleep_for)
            waited += sleep_for
            sleep_for = min(MAX_SLEEP, sleep_for * 2)

    def done(self, seconds=None):
        if seconds is not None:
            self.observe(seconds)
        with self._lock:
            self._in_flight = max(0, self._in_flight - 1)
            self._lock.notify_all()

    def unit(self):
        """`with throttle.unit():` around one table, chunk or query."""
        return _Unit(self)

    # ---- what it did ----

    def summary(self):
        if not self.waits and self.narrowed_to == self.max_permits:
            return {}
        out = {"waits": self.waits,
               "waited_seconds": round(self.waited_seconds, 1),
               "concurrency_from": self.max_permits,
               "concurrency_to": self.narrowed_to,
               "reasons": dict(sorted(self.reasons.items(),
                                      key=lambda kv: -kv[1]))}
        if self.proceeded_under_stress:
            # said out loud: the numbers below were measured while the server
            # was still unhappy, because waiting forever is not an option
            out["proceeded_under_stress"] = self.proceeded_under_stress
        return out

    def line(self):
        s = self.summary()
        if not s:
            return ""
        why = next(iter(s["reasons"]), "")
        return (f"throttled {s['waits']}x ({s['waited_seconds']}s),"
                f" concurrency {s['concurrency_from']} -> {s['concurrency_to']}"
                + (f": {why}" if why else ""))


class _Unit:
    def __init__(self, t):
        self._t = t

    def __enter__(self):
        self._t.gate()
        self._started = self._t._clock()
        return self

    def __exit__(self, *exc):
        self._t.done(self._t._clock() - self._started)
        return False
