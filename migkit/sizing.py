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

The pace used to be one number moved by one signal. Each resource a move
uses now has a limit of its own (`Limit`), moved only by what that resource
says (backlog R22): the source's reads back off on its load or its
replica's lag, the target's writes on its load, its I/O wait or its own
replicas' lag, a link on its round trip growing, and this machine on the
memory it has left - and each gives one back a window when it no longer
complains, as TCP's additive increase and Netflix's concurrency limits do.
The number running is the least of them and of the throughput climb.
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


def _memory(cgroup="/sys/fs/cgroup"):
    """Memory this process may still take: the machine's available memory,
    and inside a container the least of that and its limit less what it
    uses - psutil reads the host's memory from inside a container, and a
    move sized on it was killed at the container's limit instead."""
    try:
        import psutil
        free = int(psutil.virtual_memory().available)
    except Exception:  # noqa: BLE001 - not readable here
        free = None
    ceiling, used = _cgroup_memory(cgroup)
    if ceiling is not None:
        room = max(0, ceiling - (used or 0))
        free = room if free is None else min(free, room)
    return free


def _cgroup_memory(cgroup="/sys/fs/cgroup"):
    """(limit, in use) of this process's container in bytes, (None, None)
    where it has none. In use leaves out the page cache it may reclaim
    (`inactive_file`), as the container's own accounting does before it
    kills anything."""
    def read(name):
        try:
            with open(f"{cgroup}/{name}") as f:
                return f.read().strip()
        except OSError:
            return None

    def stat(name, key):
        text = read(name) or ""
        for line in text.splitlines():
            k, _, v = line.partition(" ")
            if k == key and v.strip().isdigit():
                return int(v)
        return 0
    limit = read("memory.max")
    if limit is not None:
        if not limit.isdigit():
            return None, None
        used = int(read("memory.current") or 0) - stat("memory.stat",
                                                       "inactive_file")
        return int(limit), max(0, used)
    limit = read("memory/memory.limit_in_bytes")
    if limit is None or not limit.isdigit() or int(limit) >= 2 ** 60:
        return None, None
    used = int(read("memory/memory.usage_in_bytes") or 0) - stat(
        "memory/memory.stat", "total_inactive_file")
    return int(limit), max(0, used)


def process_memory():
    """Bytes held by this process and every process it started - the range
    workers and the programs that copy - or None where it cannot be read."""
    try:
        import psutil
        me = psutil.Process(os.getpid())
        total = me.memory_info().rss
        for child in me.children(recursive=True):
            try:
                total += child.memory_info().rss
            except psutil.Error:
                continue
        return int(total)
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


def estimate(hop, eng, db, units=None, here=None, sides=None, model=None):
    """The `Plan` for one database of a move: every factor read, the start
    the least of them, the most the least of the firm ones.

    Firm - never passed: this machine's memory for what one worker holds
    (the widest rows of the move, `model.Model.worker_memory`), each
    server's share of its free connections, the work there is, and a
    ceiling the hop sets. Soft - where it starts, and passed only while it
    is faster and the servers are not under strain (`strain`): this
    machine's and each server's CPUs, or, where the resources were measured
    (`model`), as many as carry the one that binds and not one more
    (Little's law) - a worker waiting on a far link is not a CPU's worth of
    work, and the CPUs' guess started such a move too low."""
    here = host() if here is None else here
    sides = sides if sides is not None else {
        s: capacity(eng, s, db) for s in ("src", "dst")}
    f = {}
    if here.get("cpus"):
        # a worker spends much of its time waiting on a server; its own
        # work per row is what the CPUs here bound
        f["this machine's CPUs"] = (2 * int(here["cpus"]), True)
    held = getattr(model, "worker_memory", None) or WORKER_MEMORY
    if here.get("memory"):
        widest = getattr(model, "widest", None)
        why = ("this machine's free memory" if held <= WORKER_MEMORY
               or not widest else
               f"this machine's free memory for rows of up to"
               f" {_bytes(widest[1])} ({widest[0]})")
        f[why] = (max(1, int((here["memory"] - RESERVE_MEMORY) // held)),
                  False)
    for side, name in (("src", "the source's"), ("dst", "the target's")):
        c = sides.get(side) or {}
        other = side == "dst" or bool(c.get("replica"))
        if c.get("free_connections") is not None:
            f[f"{name} free connections"] = (connections_share(c, side),
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
    little = getattr(model, "little", None)
    if little:
        # measured: the CPUs' guesses stay in the log, the start is the
        # model's
        f[f"what carries {model.binding().said} (Little's law)"] = (
            int(little), True)
        soft = [int(little)]
    # nothing firm read: no more than twice where it starts
    most = min(firm) if firm else 2 * max(1, min(soft or [UNREAD]))
    start = max(1, min([most] + soft))
    return Plan(start, max(start, most), f)


def connections_share(c, side):
    """Workers a server's free connections allow: a share of them - less of
    a primary serving an application than of a replica or the target -
    `CONNECTIONS_PER_WORKER` a worker."""
    other = side == "dst" or bool(c.get("replica"))
    share = SHARE_OTHER if other else SHARE_PRIMARY
    return max(1, int(share * int(c["free_connections"])
                      // CONNECTIONS_PER_WORKER))


def _bytes(n):
    from .wording import human_bytes
    return human_bytes(n)


#: sessions running on a server, per CPU of it, past which it is under
#: strain - its run queue is deeper than its CPUs can take
RUNNING_PER_CPU = 2.0


def strain(eng, db):
    """Why the move should do less at once, from the servers' CPUs: a side
    running more sessions than `RUNNING_PER_CPU` a CPU. "" when neither is,
    or neither says."""
    for side, name in (("src", "source"), ("dst", "target")):
        why = _running(capacity(eng, side, db))
        if why:
            return f"{name}: {why}"
    return ""


def _running(c):
    """More sessions running on a server than its CPUs take, in words."""
    cpus, running = c.get("cpus"), c.get("running")
    if cpus and running is not None and running >= RUNNING_PER_CPU * cpus:
        return f"{running} sessions running on {cpus} CPUs"
    return ""


#: sessions waiting on storage, as a share of those working, past which a
#: side's storage is what it waits on
IO_WAITING = 0.5
#: a link's round trip past this many times its least, and this much more
#: than it, is a link whose queue is filling (Netflix's gradient: the
#: least round trip over the one now, with a tolerance of two)
RTT_GROWN, RTT_MORE = 2.0, 0.02
#: retransmitted segments over those sent in a window past which the
#: links out of this machine are losing packets, counted once enough were
#: sent to say
LOSS, LOSS_SENT = 0.02, 1000


def side_strain(eng, side, db, health=None):
    """Why one side's server should be given less, or "": its own load or
    lag as the throttle reads it (`health`, `throttle.Health`), more
    sessions running than its CPUs take (`strain`), or its sessions waiting
    on storage."""
    h = None
    if health is not None:
        try:
            h = health(eng, side, db)
        except Exception:  # noqa: BLE001 - a probe never stops a move
            h = None
    why = h.stressed() if h is not None else ""
    if why:
        return why
    c = capacity(eng, side, db)
    why = _running(c)
    if why:
        return why
    cpus = c.get("cpus")
    try:
        io = eng.io_wait(side, db) or {}
    except Exception:  # noqa: BLE001
        io = {}
    waiting, active = io.get("io_wait"), io.get("active")
    if waiting and active and waiting >= 2 \
            and waiting >= IO_WAITING * active:
        return f"{waiting} of {active} sessions waiting on storage"
    pending = io.get("io_pending")
    if pending and cpus and pending >= RUNNING_PER_CPU * cpus:
        return f"{pending} reads, writes and flushes queued on storage"
    return ""


class RoundTrip:
    """A link's round trip as a resource's signal: the least seen is the
    link with nothing queued on it, and one grown past `RTT_GROWN` times
    that (and `RTT_MORE` seconds more) asks the number down by the ratio -
    no further than half at once."""

    def __init__(self, host, port, measure=None):
        from .tunnel import _round_trip
        self.host, self.port = host, port
        self._measure = measure or (lambda: _round_trip(host, port, 1, 1.0))
        self.least, self.asked = None, 0

    def __call__(self):
        if self.least is None and self.asked >= 2:
            # it never answered: asked no more, where each window waited
            return None
        self.asked += 1
        now = self._measure()
        if not now:
            return None
        self.least = now if self.least is None else min(self.least, now)
        if now > RTT_GROWN * self.least and now - self.least > RTT_MORE:
            return Strain(f"round trip {now * 1000:.0f} ms,"
                          f" {now / self.least:.1f}x its least",
                          factor=max(0.5, min(1.0, RTT_GROWN * self.least
                                              / now)))
        return None


def retransmits():
    """(segments sent, segments sent again) by this machine's TCP since it
    started, or None where it does not say."""
    try:
        with open("/proc/net/snmp") as f:
            lines = [l.split() for l in f if l.startswith("Tcp:")]
        head, vals = lines[0], lines[1]
        got = dict(zip(head[1:], vals[1:]))
        return int(got["OutSegs"]), int(got["RetransSegs"])
    except (OSError, IndexError, KeyError, ValueError):
        pass
    import re
    import subprocess
    try:
        out = subprocess.run(["netstat", "-s", "-p", "tcp"],
                             capture_output=True, text=True,
                             timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    # the BSDs' own words: "N packets sent", "N data packets (M bytes)
    # retransmitted"
    sent = re.search(r"^\s*(\d+) packets? sent\s*$", out, re.M)
    again = re.search(r"^\s*(\d+) data packets? \([^)]*\) retransmitted",
                      out, re.M)
    if not sent or not again or not int(sent.group(1)):
        return None
    return int(sent.group(1)), int(again.group(1))


class Loss:
    """Packets this machine sent again over those it sent, between two
    windows, as the links' signal."""

    def __init__(self, read=retransmits):
        self._read, self._last = read, None

    def __call__(self):
        now = self._read()
        last, self._last = self._last, now
        if not now or not last:
            return None
        sent, again = now[0] - last[0], now[1] - last[1]
        if sent >= LOSS_SENT and again >= LOSS * sent:
            return f"{again / sent:.1%} of packets sent again"
        return None


def resources(eng, db, hop=None, health=None, model=None):
    """{name: signal} for `Pace`: the source's reads, the target's writes
    and each link, one signal each - so a source that lags cuts the
    number for the source's sake and says so, where one signal for every
    server said only that something was under strain. A link whose round
    trip the model could not measure (`model.sides`) is not asked
    again."""
    out = {"the source's reads":
           lambda: side_strain(eng, "src", db, health),
           "the target's writes":
           lambda: side_strain(eng, "dst", db, health)}
    if hop is not None:
        seen = set()
        known = getattr(model, "sides", None)
        for side, key, ep in (("source", "src", hop.source),
                              ("target", "dst", hop.target)):
            at = (str(ep.host), int(ep.port or 0))
            local = at[0] in ("127.0.0.1", "localhost", "::1") and not (
                ep.options or {}).get("tunnel_to")
            unmeasured = known is not None and (
                known.get(key) or {}).get("rtt") is None
            if local or unmeasured or at in seen or not at[1]:
                continue
            seen.add(at)
            out[f"the link to the {side}"] = RoundTrip(*at)
        if len(out) > 2:
            out["this machine's connections"] = Loss()
    return out


def fit(hop, eng, db, log=None, units=None, model=None):
    """`estimate`, and the hop's `workers` set to its start - which every
    path that takes a fixed number (a bulk program's jobs, the check's
    threads) uses. The plan is returned for the paths that pace.

    Where the resources were measured (`model`), the reads and the writes
    are sized apart as well: `workers_read` for a path that reads the source
    on its own (a dump's jobs), `workers_write` for one that writes the
    target on its own (a load's), each as many as carry its own side and
    no more than that side's connections allow."""
    plan = estimate(hop, eng, db, units, model=model)
    hop.workers = plan.start
    hop.workers_most = plan.most
    hop.workers_read = hop.workers_write = plan.start
    if model is not None and model.little:
        for side, attr, n in (("src", "workers_read", model.read_workers),
                              ("dst", "workers_write", model.write_workers)):
            c = capacity(eng, side, db)
            cap = (connections_share(c, side)
                   if c.get("free_connections") is not None else plan.most)
            setattr(hop, attr, max(1, min(int(n or plan.start), cap,
                                          plan.most)))
    plan.model = model
    if log:
        log(plan.line())
        for line in (model.lines() if model is not None else []):
            log(line)
    return plan


def split(hop, workers):
    """(jobs reading, jobs writing) for a path that reads and then writes -
    a dump and a load: what `fit` sized each side to, `workers` for both
    where nothing was measured."""
    return (int(getattr(hop, "workers_read", 0) or workers),
            int(getattr(hop, "workers_write", 0) or workers))


class Strain:
    """A resource's complaint: why, and how far it asks the number down -
    to `at_most`, or by `factor` (the usual cut where neither is said)."""

    def __init__(self, why, at_most=None, factor=None):
        self.why, self.at_most, self.factor = why, at_most, factor

    def __str__(self):
        return self.why


class Limit:
    """One resource's own ceiling on how many run at once, moved only by
    what that resource says (`signal`: "" or None while it is fine, the
    reason - or a `Strain` saying how far down - when it is not). Cut at
    once when it complains, and given one back every window it does not,
    up to `most` (additive increase, multiplicative decrease: TCP's rule,
    and Netflix's concurrency limits'). `value` starts at `most`: a
    resource that never complains never holds the move back."""

    def __init__(self, name, signal, most):
        self.name, self.signal = name, signal
        self.most = self.value = max(1, int(most))
        self.said, self.cuts = "", 0

    def update(self, current, cut):
        """Asked once a window with the number running now: the reason it
        cut, or ""."""
        try:
            got = self.signal()
        except Exception:  # noqa: BLE001 - a probe never stops a move
            got = None
        if not got:
            self.said = ""
            if self.value < self.most:
                self.value += 1
            return ""
        s = got if isinstance(got, Strain) else Strain(str(got))
        if s.at_most is not None:
            to = int(s.at_most)
        else:
            to = math.floor(current * (s.factor if s.factor is not None
                                       else cut))
        self.value = max(1, min(self.value, to, self.most))
        self.said = f"{self.name}: {s.why}" if self.name else s.why
        self.cuts += 1
        return self.said


#: what the change-stream connector holds by default (its own numbers),
#: kept as the least it is given
STREAM_BATCH, STREAM_QUEUE, STREAM_POLL_MS = 2048, 8192, 500
#: the share of a worker's memory one batch of changes may take
BATCH_SHARE = 16


def stream_buffers(row_bytes=None, rows_per_second=None):
    """What the change-stream connector holds and hands over at once, from
    the rows it carries rather than its defaults.

    Its defaults count changes and not bytes: 8,192 changes queued,
    however large each is, so a table of wide rows can hold gigabytes in a
    queue sized for kilobytes. Here the queue is bounded by the memory a
    worker may hold (`WORKER_MEMORY`) in bytes as well; a batch is
    what fits in a sixteenth of that at the source's average row
    (`row_bytes`, its tables' bytes over their rows), never below the
    connector's own 2,048; and the queue is four batches, the connector's
    own ratio. Where a copy of this hop measured how fast rows went
    (`rows_per_second`), the wait for a batch that is not full is the time
    one takes to fill at that rate, between 50 and 500 ms: a full batch
    never waits, and a slow source's changes are not held half a second
    each."""
    batch = STREAM_BATCH
    if row_bytes and row_bytes > 0:
        batch = max(STREAM_BATCH,
                    int(WORKER_MEMORY // BATCH_SHARE // row_bytes))
    out = {"max.batch.size": batch, "max.queue.size": max(STREAM_QUEUE,
                                                          4 * batch),
           "max.queue.size.in.bytes": WORKER_MEMORY}
    poll = STREAM_POLL_MS
    if rows_per_second and rows_per_second > 0:
        poll = int(min(STREAM_POLL_MS,
                       max(50, 1000 * batch / rows_per_second)))
    out["poll.interval.ms"] = poll
    return {k: str(v) for k, v in out.items()}


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
    * a resource under strain cuts its own limit (`Limit`) by 30% at once,
      or to what it says it can take, and the number with it, held two
      windows; once it no longer complains its limit gives one back a
      window, and the number steps back with it to where the cut found it
      - where one pace for every server waited for its next look above.
      `resources` names them - {name: signal} - and `stress` is the one
      signal of before, for every server at once.
    Never below one, never past `most` or any resource's limit. What it did
    is in `history`, each cut named by its resource.

    `admit(busy)` is asked before another unit starts: whether this
    machine's memory holds one more worker (`memory`, a `Memory`), so a
    move of wide rows waits for room instead of being killed for want of
    it."""

    KEEP, DOUBLE, CUT = 0.05, 0.25, 0.7
    WINDOW, LEAST_DONE, HOLD, LOOK_EVERY, FORGET = 3.0, 4, 2, 8, 32
    #: the weight of the newest window in `recent`: about the last five
    EWMA = 1 / 3

    def __init__(self, start, most, stress=None, clock=time.monotonic,
                 resources=None, memory=None, report=None):
        self.most = max(1, int(most))
        #: called with the pace after each window: where the move keeps
        #: what it has done for `watch` and the metrics
        self.report = report
        self.limit = max(1, min(int(start), self.most))
        self.stress = stress
        signals = dict(resources or {})
        if stress is not None:
            signals.setdefault("", stress)
        if memory is not None:
            signals.setdefault(memory.name, memory.signal)
        self.limits = {n: Limit(n, fn, self.most)
                       for n, fn in signals.items()}
        self.memory = memory
        self._clock = clock
        self._lock = threading.Lock()
        self._weight, self._n, self._times = 0.0, 0, []
        self._began = clock()
        # the rate at the current number, and where the last move started
        self._ref, self._back, self._move = None, None, None
        self._hold, self._windows, self._growing = 0, 0, True
        self._not_above = self.most + 1
        # where the last cut for a resource's sake started from
        self._cut_from = None
        self.history = [(self.limit, None, "start")]
        #: weight a second in the last window, and its moving average - what
        #: the time left is worked out from while the move runs
        self.rate, self.recent = None, None
        #: weight done since the start, when that was, and when the last
        #: unit ended
        self.total, self.started = 0.0, clock()
        self.last = None

    def took(self):
        """Seconds from the start to the last unit's end: the work's own
        time, without what ran after it (the last indexes' builds)."""
        return ((self.last if self.last is not None else self._clock())
                - self.started)

    def ceiling(self):
        """The least of `most` and every resource's limit."""
        return min([self.most] + [x.value for x in self.limits.values()])

    def admit(self, busy):
        """Whether another unit may start beside `busy` running ones."""
        if self.memory is None or busy < 1:
            return True
        return self.memory.room(busy)

    def done(self, weight=1, seconds=None):
        with self._lock:
            self._weight += max(0.0, float(weight or 0))
            self.total += max(0.0, float(weight or 0))
            self._n += 1
            self.last = self._clock()
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
            self.rate = rate
            # one window's rate swings with the ranges that ended in it
            self.recent = rate if self.recent is None else (
                self.EWMA * rate + (1 - self.EWMA) * self.recent)
            self._decide(rate)
        if self.report is not None and self.rate is not None:
            try:
                self.report(self)
            except Exception:  # noqa: BLE001 - a report never stops a move
                pass

    def _go(self, n, why, rate, move):
        n = max(1, min(self.ceiling(), int(n)))
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
        said = [w for w in (x.update(self.limit, self.CUT)
                            for x in self.limits.values()) if w]
        if said:
            self._cut_from = max(self._cut_from or 0, self.limit)
            self._go(min(self.limit, self.ceiling()), "; ".join(said), rate,
                     None)
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
        top = self.ceiling()
        if self._cut_from and self.limit < min(self._cut_from, top):
            # a resource cut it and complains no more: one back a window,
            # as that resource's own limit comes back, to where it was
            self._go(self.limit + 1, "one back, nothing complains", rate,
                     None)
            return
        self._cut_from = None
        if len(self.history) == 1 and self.limit < top:
            self._go(self.limit + 1, "a step up to measure", rate, "up")
            return
        if self._windows % self.LOOK_EVERY == 0 \
                and self.limit + 1 < self._not_above \
                and self.limit < top:
            # half the way to what did not pay, and never more than a
            # quarter again at once: a look is not a leap on a source that
            # serves an application
            room = min(self._not_above - 1, top) - self.limit
            self._go(self.limit + max(1, min(room // 2,
                                             round(self.limit * 0.25))),
                     "a look above", rate, "up")
        elif self._windows % self.LOOK_EVERY == self.LOOK_EVERY // 2 \
                and self.limit > 1:
            self._go(self.limit - 1, "a look below", rate, "down")

    def line(self):
        """One line for the log: where it went, if anywhere, and which
        resource held it back."""
        seen = [n for n, _, _ in self.history]
        if len(set(seen)) < 2:
            return ""
        cut = [x for x in self.limits.values() if x.cuts and x.name]
        return (f"at a time: started at {seen[0]}, ran between {min(seen)}"
                f" and {max(seen)}, ended at {self.limit}"
                + (" - held back by " + ", ".join(
                    f"{x.name} ({x.cuts}x)" for x in cut) if cut else ""))

    def held_by(self):
        """The resources holding the number down now, in words, or ""."""
        return "; ".join(x.said for x in self.limits.values()
                         if x.said and x.value < self.most)


class Memory:
    """This machine's memory as a resource of the move: how many workers of
    `per_worker` bytes it holds (`room`), and the watchdog's signal - fewer
    at once when what is free no longer holds the workers running, and one
    at a time when it falls under `RESERVE_MEMORY` - before the kernel or a
    container's limit kills the move. What one worker holds is the model's
    estimate for the widest rows (`model.Model.worker_memory`), raised to
    what the workers were seen to hold where that is more."""

    name = "this machine's memory"
    #: seconds a reading of this machine's memory is used for
    FRESH = 0.25

    def __init__(self, per_worker, read=None, ours=None, clock=time.monotonic):
        self.per_worker = max(1, int(per_worker or WORKER_MEMORY))
        self._read = read or _memory
        self._ours = ours or process_memory
        self._clock = clock
        self._at, self._got = None, (None, None)
        self.seen = 0
        self.busy = 0

    def _now(self):
        now = self._clock()
        if self._at is None or now - self._at >= self.FRESH:
            self._at, self._got = now, (self._read(), self._ours())
        return self._got

    def each(self):
        """What one worker holds, as far as anyone knows."""
        return max(self.per_worker, self.seen)

    def fits(self, busy=0):
        """Workers this machine's memory holds, `busy` of them running and
        holding what they hold now; None where it cannot be read."""
        free, ours = self._now()
        if free is None:
            return None
        if ours is not None and busy:
            # what the workers were seen to hold, over a floor of what this
            # process holds idle - a worker's share of it is its own
            self.seen = max(self.seen, int(ours // busy))
        return int((free + (ours or 0) - RESERVE_MEMORY) // self.each())

    def room(self, busy):
        self.busy = busy
        n = self.fits(busy)
        return n is None or busy + 1 <= n

    def signal(self):
        free, ours = self._now()
        if free is None:
            return None
        n = self.fits(self.busy)
        if free < RESERVE_MEMORY:
            return Strain(f"{_bytes(max(free, 0))} free, under the"
                          f" {_bytes(RESERVE_MEMORY)} kept for everything"
                          " else", at_most=max(1, min(n or 1, self.busy - 1
                                                      or 1)))
        if n is not None and n < max(self.busy, 1):
            return Strain(f"{_bytes(free)} free, and a worker here holds"
                          f" about {_bytes(self.each())}",
                          at_most=max(1, n))
        return None
