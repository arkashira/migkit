"""One plan for a move's tables, made jointly (backlog P0-SMART, item 4).

Each choice of a move was made alone: a table's way by what that way cost
on its own, the workers by Little's law for the resource that binds, the
tables largest first, each table's indexes built beside the next table's
copy, the check once the whole copy has ended. Each is right alone and
they are not right together: a way that is fastest alone on every table
loads the same resource with every table, so the move waits on that one
resource while another sits idle; a check that waits for the last table
leaves the source idle for the whole index tail.

Here they are one schedule. Every table has the ways that hold for it
(its needs and the strongest guarantees the engines allow, filtered
before it arrives here, `decide.space`), each with what one stream of it
moves a second, what each byte takes of each resource (`uses`), its
start-up, what it bills, its spread and its chance of failing. The move
has its limits (`Limits`): what each resource carries at most, the
workers at once, and where the hop sets them the deadline, the money it
may spend and the share of the source it may take.

The plan is made in four steps, each measured against the per-choice
greedy (`greedy`, what migkit did before: each table's fastest way, the
largest first, as many tables at once as workers, the check after the
move) on simulated moves:

1. The ways, as a mix: starting from each table's fastest-alone way, a
   table is switched to another way wherever that lowers the fluid bound -
   the most any resource carries over its capacity, the workers' stream
   seconds over their number, and the longest table alone - until no switch
   lowers it (a local search, steepest first). Money is a hard limit: over
   it, the tables that save the most money a second lost switch first.
2. The order: each table's copy, its index builds on the target and its
   check, released as soon as what they read is there, simulated as
   flows sharing the resources (max-min fair, `_rates`) - longest first,
   the most of the binding resource first, and the largest first, keeping
   the order whose simulated end comes first.
3. The tables at once (`_at_once`): as many as the workers shares every
   resource from the start but finishes no table early, so every index
   build waits for the end; fewer lets the first tables' builds and checks
   run beside the rest's copy.
4. The workers: the fewest at once whose simulated end is within
   `WORKERS_WITHIN` of the most allowed - more would only wait on the
   resource that binds.

The plan is never later than the greedy but for those fewer workers:
both are simulated on the same model and the greedy's is kept where it
ends sooner.

Measured (`bench/smart_decisions.py schedule`, 200 simulated moves of 5
to 150 tables, lognormal sizes about 200 MB, three ways loading the
resources differently, 4 to 16 workers, a third of them with the source
held to 30%, run 2026-10-01): the joint plan ended 8.7% sooner than the
per-choice greedy on average, 7.7% at the median, 18.4% sooner at the
tenth percentile and 32% at best; 132 of the 195 moves the greedy could
make within their money ended 5% or more sooner; the worst was 1.4%
later, which is the fewer workers it keeps; the other 5 moves the greedy
made spent more money than the hop allowed, and the joint plan kept
within it. Most of the gain is the order, the tables at once and each
table's check released once it is in (8.4% without the mix); the mix of
ways adds the rest where resources differ. Its end was within 3.6% of
the fluid lower bound at the median (29% at the 90th percentile, mostly
moves whose longest table binds); 15 ms at the median to plan, under a
second at most (150 tables).

CP-SAT (OR-Tools 9.10, 8 threads, 5 s a move) was measured beside it on
40 of those moves, each table's copy an optional interval a way with
cumulative resources and workers: its ways and order, simulated on the
same flows with the same choice of tables at once, ended 2.3% later on
average (0.4% at the median, 3.2% sooner at best) - a fixed rate a way is
coarser than the flows it shares - and took 5 s where this takes
milliseconds. It is not used, and not a dependency: the builds that
resolve beside the cloud libraries here are pinned to 9.10, the last
that takes their protobuf (`MEASURED`).
"""
import math
from dataclasses import dataclass, field

#: the resources a byte may pass: the source's reads, the target's
#: writes, each link, this machine's CPUs (`model.Stage.name`)
SOURCE, TARGET = "source", "target"

#: the least number of workers is kept whose simulated end is within this
#: share of the end at the most allowed
WORKERS_WITHIN = 0.02
#: how many standard deviations of a way's rate the p90 end is taken at
P90_SD = 1.2816
#: switches the local search makes at most, per table
SEARCH_PER_TABLE = 3
#: the spread of a way the probes alone measured, no move of it yet: the
#: resource model's one run against docker named the wrong resource as
#: binding in 4 of 11 cases (backlog P0-DEPTH, item 4)
MODEL_SPREAD = 0.5


@dataclass
class Way:
    """One way a table can go, as the scheduler weighs it."""
    name: str
    said: str = ""
    #: bytes a second one stream of it moves, alone
    per_stream: float = 0.0
    #: {resource: times each byte passes it}
    uses: dict = field(default_factory=dict)
    #: seconds before its first byte, once a table
    startup: float = 0.0
    #: money a byte carried costs (egress, bytes billed)
    bills: float = 0.0
    #: its time's spread: the standard deviation over the mean
    spread: float = 0.0
    #: "measured" or "predicted", for the plan's words
    truth: str = ""
    #: its chance of failing on a table, and the share of the table's work
    #: a failure throws away (a range's where it resumes by range)
    risk: float = 0.0
    lost: float = 0.5
    #: the most streams one table takes this way (0: as many as its ranges)
    most: int = 0

    def expected(self, size):
        """Bytes of work a table of `size` bytes costs this way, a failure's
        thrown-away work counted at its chance (P0-SMART item 6)."""
        return size * (1.0 + self.risk * self.lost)


@dataclass
class Job:
    """One table of the move."""
    name: str
    bytes: float
    rows: int = 0
    index_bytes: float = 0.0
    #: the most streams its copy splits into: its ranges
    ranges: int = 1
    ways: tuple = ()
    #: the way its check reads it, or None where it is not checked here
    check: object = None


@dataclass
class Limits:
    """What the move may take."""
    #: {resource: bytes a second all streams together carry}
    caps: dict = field(default_factory=dict)
    workers: int = 4
    deadline: float = None
    money: float = None
    #: bytes a second one index build writes, and how many build at once
    index_rate: float = None
    index_at_once: int = 2
    #: the share of the source's capacity the move may take
    source_share: float = None

    def cap(self, r):
        got = self.caps.get(r)
        if got and r == SOURCE and self.source_share:
            return got * self.source_share
        return got


@dataclass
class Placed:
    """One table in the plan: its way and when each of its steps ends."""
    job: str
    way: str
    start: float = None
    copied: float = None
    indexed: float = None
    checked: float = None
    #: {way: seconds alone} of every way the table could have gone - the
    #: counterfactuals the decision record keeps
    alone: dict = None


@dataclass
class Plan:
    """The schedule: each table's way and times, the end, what binds."""
    placed: dict
    makespan: float
    p90: float = None
    workers: int = 0
    #: the tables copied at once, their ranges sharing the workers
    tables: int = 0
    binding: str = ""
    bound: float = 0.0
    money: float = 0.0
    order: tuple = ()
    how: str = ""
    greedy: float = None
    meets: bool = True
    why: str = ""
    #: "measured" where every way it chose was, "partly predicted" where
    #: one was only predicted
    truth: str = ""

    def ways(self):
        """{table: way name}."""
        return {n: p.way for n, p in self.placed.items()}

    def as_evidence(self):
        """The plan as data, for the evidence export and the run's log."""
        return {"makespan": self.makespan, "p90": self.p90,
                "workers": self.workers, "tables_at_once": self.tables,
                "binding": self.binding,
                "lower_bound": self.bound, "money": self.money,
                "greedy_makespan": self.greedy, "how": self.how,
                "meets_limits": self.meets, "why": self.why,
                "order": list(self.order),
                "truth": self.truth, "tables_at_once": self.tables,
                "tables": {n: {"way": p.way, "start": p.start,
                               "copied": p.copied, "indexed": p.indexed,
                               "checked": p.checked,
                               "alone": p.alone or {}}
                           for n, p in self.placed.items()}}


# --- the flows: what each running step gets of each resource ----------------

class _Task:
    __slots__ = ("job", "kind", "left", "wait", "rate1", "uses", "most",
                 "takes", "prio", "streams", "rate", "after", "started")

    def __init__(self, job, kind, size, wait, rate1, uses, most, takes,
                 prio, after=None):
        self.job, self.kind = job, kind
        self.left, self.wait = float(size), float(wait)
        self.rate1, self.uses = float(rate1), uses
        self.most, self.takes, self.prio = max(1, int(most)), takes, prio
        self.streams, self.rate, self.after = 0, 0.0, after
        self.started = None


def _rates(running, caps):
    """Each running step's bytes a second: as many as its streams carry,
    no resource over its cap, shared max-min fair in proportion to what
    each would take alone (progressive filling)."""
    demand = {}
    for t in running:
        demand[t] = 0.0 if t.wait > 0 else t.streams * t.rate1
        t.rate = 0.0
    active = {t for t in running if demand[t] > 0}
    used = {r: 0.0 for r in caps}
    while active:
        theta, binding = 1.0, None
        for r, cap in caps.items():
            if not cap:
                continue
            want = sum(demand[t] * t.uses.get(r, 0) for t in active)
            if want <= 0:
                continue
            got = (cap - used[r]) / want
            if got < theta:
                theta, binding = max(got, 0.0), r
        if binding is None:
            for t in active:
                t.rate = demand[t]
            return
        frozen = {t for t in active if t.uses.get(binding, 0) > 0}
        for t in active:
            t.rate = theta * demand[t]
        for t in frozen:
            for r in caps:
                used[r] += t.rate * t.uses.get(r, 0)
        active -= frozen


def _tasks(jobs, choice, limits, slow, check_after_all):
    """The steps of every table: its copy, its index builds, its check."""
    out = []
    index_rate = limits.index_rate
    for j in jobs:
        w = choice[j.name]
        k = w.most or j.ranges
        rate1 = w.per_stream / (1.0 + slow * w.spread)
        copy = _Task(j, "copy", w.expected(j.bytes), w.startup, rate1,
                     w.uses, min(k, j.ranges), True, 0)
        out.append(copy)
        last = copy
        if j.index_bytes and index_rate:
            idx = _Task(j, "index", j.index_bytes, 0.0, index_rate,
                        {TARGET: 1}, 1, False, 0, after=copy)
            out.append(idx)
            last = idx
        if j.check is not None:
            c = j.check
            out.append(_Task(j, "check", j.bytes, c.startup,
                             c.per_stream / (1.0 + slow * c.spread),
                             c.uses, min(c.most or j.ranges, j.ranges), True,
                             0, after="all" if check_after_all else last))
    return out


def simulate(jobs, choice, limits, order=None, workers=None, slow=0.0,
             check_after_all=False, tables=None):
    """The `Plan` of `jobs`, each on its way in `choice` ({name: Way}),
    started in `order` (table names, the first first), its steps flowing
    through the resources side by side: as many tables at once as
    `tables` (the workers where None), their ranges sharing the workers -
    one each, the rest a turn each in the order's turn up to its ranges;
    its index builds follow it on the target, `index_at_once` at a time,
    beside the next tables' copies; its check follows those - or, with
    `check_after_all`, waits for every copy and build to end, as a check
    run after the move does. `slow` is how many of each way's standard
    deviations slower it goes (the p90)."""
    workers = int(workers or limits.workers or 1)
    at_once = max(1, min(int(tables or workers), workers))
    order = list(order or [j.name for j in jobs])
    rank = {n: i for i, n in enumerate(order)}
    caps = {r: limits.cap(r) for r in limits.caps if limits.cap(r)}
    tasks = _tasks(jobs, choice, limits, slow, check_after_all)
    for t in tasks:
        t.prio = (rank.get(t.job.name, len(rank)),
                  {"copy": 0, "index": 1, "check": 2}[t.kind])
    tasks.sort(key=lambda t: t.prio)
    placed = {j.name: Placed(j.name, choice[j.name].name) for j in jobs}
    done_copies = 0
    n_copies = sum(1 for t in tasks if t.kind != "check")
    pending, running, now = list(tasks), [], 0.0
    finished = set()
    guard = 0
    while pending or running:
        guard += 1
        if guard > 20 * len(tasks) + 1000:
            break
        idx_free = limits.index_at_once - sum(1 for t in running
                                              if not t.takes)
        # as a move runs them: as many tables at once as there are
        # workers, the ranges of those in flight sharing the workers - one
        # each, the rest a turn each in the order's turn up to its ranges
        holding = [t for t in running if t.takes]
        started = []
        for t in pending:
            if t.after == "all":
                if done_copies < n_copies:
                    continue
            elif t.after is not None and t.after not in finished:
                continue
            if t.takes:
                if len(holding) >= at_once:
                    continue
                holding.append(t)
            else:
                if idx_free <= 0:
                    continue
                idx_free -= 1
                t.streams = 1
            t.started = now
            started.append(t)
        holding.sort(key=lambda t: t.prio)
        for t in holding:
            t.streams = 1
        free = workers - len(holding)
        while free > 0:
            grew = False
            for t in holding:
                if free > 0 and t.streams < t.most:
                    t.streams += 1
                    free -= 1
                    grew = True
            if not grew:
                break
        for t in started:
            pending.remove(t)
            running.append(t)
            p = placed[t.job.name]
            if t.kind == "copy":
                p.start = now
        if not running:
            break
        _rates(running, caps)
        step = math.inf
        for t in running:
            if t.wait > 0:
                step = min(step, t.wait)
            elif t.rate > 0:
                step = min(step, t.left / t.rate)
        if step is math.inf:
            # nothing can move: a resource with no capacity at all
            return Plan(placed, math.inf, workers=workers, how="stuck",
                        why="a resource the ways need carries nothing")
        now += step
        ended = []
        for t in running:
            if t.wait > 0:
                t.wait = max(0.0, t.wait - step)
                continue
            t.left -= t.rate * step
            if t.left <= 1e-6 * max(1.0, t.rate):
                ended.append(t)
        for t in ended:
            running.remove(t)
            finished.add(t)
            p = placed[t.job.name]
            if t.kind == "copy":
                p.copied = now
                done_copies += 1
            elif t.kind == "index":
                p.indexed = now
                done_copies += 1
            else:
                p.checked = now
    return Plan(placed, now, workers=workers, tables=at_once,
                order=tuple(order))


# --- the bounds -------------------------------------------------------------

def loads(jobs, choice, limits, workers=None):
    """{what: seconds it needs at the least}: each resource's bytes over
    its cap, the workers' stream seconds over their number, and each
    table alone - the fluid bound on the end is the largest."""
    workers = int(workers or limits.workers or 1)
    out = {}
    stream_seconds, alone = 0.0, 0.0
    for j in jobs:
        w = choice[j.name]
        size = w.expected(j.bytes)
        for r, n in w.uses.items():
            out[r] = out.get(r, 0.0) + size * n
        if j.index_bytes and limits.index_rate:
            out[TARGET] = out.get(TARGET, 0.0) + j.index_bytes
        if j.check is not None:
            for r, n in j.check.uses.items():
                out[r] = out.get(r, 0.0) + j.bytes * n
            stream_seconds += j.bytes / max(j.check.per_stream, 1e-9)
        if w.per_stream:
            stream_seconds += size / w.per_stream
            k = min(w.most or j.ranges, j.ranges, workers)
            alone = max(alone, w.startup + size / (k * w.per_stream))
    got = {}
    for r, b in out.items():
        cap = limits.cap(r)
        if cap:
            got[r] = b / cap
    got["workers"] = stream_seconds / workers
    got["the longest table"] = alone
    return got


def bound(jobs, choice, limits, workers=None):
    """(the least the end can be, what sets it)."""
    got = loads(jobs, choice, limits, workers)
    what = max(got, key=got.get) if got else ""
    return (got[what] if what else 0.0), what


def money(jobs, choice):
    return sum(choice[j.name].bills * j.bytes for j in jobs)


# --- the per-choice greedy: what migkit did before --------------------------

def alone(job, way, limits):
    """Seconds `job` takes on `way` alone: its start-up and its bytes at as
    many streams as its ranges and the workers allow, no resource over its
    cap."""
    k = min(way.most or job.ranges, job.ranges, limits.workers or 1)
    rate = k * way.per_stream
    for r, n in way.uses.items():
        cap = limits.cap(r)
        if cap and n:
            rate = min(rate, cap / n)
    if rate <= 0:
        return math.inf
    return way.startup + way.expected(job.bytes) / rate


def greedy(jobs, limits):
    """Each table on the way fastest for it alone, the largest first, the
    check after the whole move - each choice made on its own."""
    choice = {j.name: min(j.ways, key=lambda w: (alone(j, w, limits),
                                                 j.ways.index(w)))
              for j in jobs}
    order = [j.name for j in sorted(jobs, key=lambda j: -j.bytes)]
    got = simulate(jobs, choice, limits, order, check_after_all=True)
    got.how = "each choice alone"
    return got, choice


# --- the joint plan ---------------------------------------------------------

def _search(jobs, choice, limits):
    """The mix of ways: switches that lower the fluid bound, steepest
    first, within the money the hop allows."""
    choice = dict(choice)
    if limits.money is not None:
        over = money(jobs, choice) - limits.money
        if over > 0:
            # the tables that save the most money a second lost go first
            moves = []
            for j in jobs:
                w = choice[j.name]
                for o in j.ways:
                    saved = (w.bills - o.bills) * j.bytes
                    if saved <= 0:
                        continue
                    lost = max(alone(j, o, limits) - alone(j, w, limits),
                               1e-9)
                    moves.append((saved / lost, j, o))
            for _, j, o in sorted(moves, key=lambda m: -m[0]):
                if over <= 0:
                    break
                w = choice[j.name]
                saved = (w.bills - o.bills) * j.bytes
                if saved > 0:
                    choice[j.name] = o
                    over -= saved
    fluid = _Fluid(jobs, choice, limits)
    spent = money(jobs, choice)
    best = fluid.value()
    by_size = sorted((j for j in jobs if len(j.ways) > 1),
                     key=lambda j: -j.bytes)
    for _ in range(SEARCH_PER_TABLE * max(1, len(jobs))):
        move = None
        for j in by_size:
            here = choice[j.name]
            for o in j.ways:
                if o is here:
                    continue
                more = (o.bills - here.bills) * j.bytes
                if limits.money is not None \
                        and spent + more > limits.money + 1e-9:
                    continue
                got = fluid.value_if(j, here, o)
                if got < best * (1 - 1e-9) and (move is None
                                                 or got < move[0]):
                    move = (got, j, o, more)
        if move is None:
            break
        best, j, o, more = move
        fluid.switch(j, choice[j.name], o)
        choice[j.name] = o
        spent += more
    return choice


class _Fluid:
    """`loads` kept as the search switches one table at a time: a switch
    is weighed in the resources' count, not the tables'."""

    def __init__(self, jobs, choice, limits):
        self.limits = limits
        self.workers = int(limits.workers or 1)
        self.caps = {r: limits.cap(r) for r in limits.caps if limits.cap(r)}
        self.load = dict.fromkeys(self.caps, 0.0)
        self.streams = 0.0
        self.alone = {}
        for j in jobs:
            self._add(j, choice[j.name], 1)
            if j.index_bytes and limits.index_rate and TARGET in self.caps:
                self.load[TARGET] += j.index_bytes / self.caps[TARGET]
            if j.check is not None:
                for r, n in j.check.uses.items():
                    if r in self.caps:
                        self.load[r] += j.bytes * n / self.caps[r]
                self.streams += j.bytes / max(j.check.per_stream, 1e-9)

    def _alone(self, j, w):
        if not w.per_stream:
            return 0.0
        k = min(w.most or j.ranges, j.ranges, self.workers)
        return w.startup + w.expected(j.bytes) / (k * w.per_stream)

    def _add(self, j, w, sign):
        size = w.expected(j.bytes)
        for r, n in w.uses.items():
            if r in self.caps:
                self.load[r] += sign * size * n / self.caps[r]
        if w.per_stream:
            self.streams += sign * size / w.per_stream
        if sign > 0:
            self.alone[j.name] = self._alone(j, w)

    def value(self):
        return max([self.streams / self.workers, max(self.alone.values(),
                                                     default=0.0)]
                   + list(self.load.values()))

    def switch(self, j, w, o):
        self._add(j, w, -1)
        self._add(j, o, 1)
        self._top = None

    def _longest_but(self, name):
        if getattr(self, "_top", None) is None:
            top = sorted(self.alone.items(), key=lambda kv: -kv[1])[:2]
            self._top = top + [("", 0.0)] * (2 - len(top))
        (a, x), (_, y) = self._top
        return y if a == name else x

    def value_if(self, j, w, o):
        """`value` were `j` switched from `w` to `o`, nothing switched."""
        sw, so = w.expected(j.bytes), o.expected(j.bytes)
        most = self.streams
        if w.per_stream:
            most -= sw / w.per_stream
        if o.per_stream:
            most += so / o.per_stream
        got = max(most / self.workers, self._longest_but(j.name),
                  self._alone(j, o))
        for r, cap in self.caps.items():
            v = self.load[r] + (so * o.uses.get(r, 0)
                                - sw * w.uses.get(r, 0)) / cap
            if v > got:
                got = v
        return got


def _orders(jobs, choice, limits):
    """The orders tried: longest alone first, the most of the binding
    resource first, the largest first."""
    _, what = bound(jobs, choice, limits)
    out = [sorted(jobs, key=lambda j: -alone(j, choice[j.name], limits)),
           sorted(jobs, key=lambda j: -(choice[j.name].uses.get(what, 0)
                                        * j.bytes)),
           sorted(jobs, key=lambda j: -j.bytes)]
    seen, got = set(), []
    for o in out:
        names = tuple(j.name for j in o)
        if names not in seen:
            seen.add(names)
            got.append(list(names))
    return got


def _at_once(workers):
    """The numbers of tables at once the plan weighs: one, a few, and as
    many as the workers - the first lets the first tables' index builds
    run beside the rest's copy, the last shares every resource at once."""
    return sorted({k for k in (1, 2, 4, 8, workers) if 1 <= k <= workers})


def joint(jobs, limits, search=True):
    """The `Plan`: the ways as one mix, the order, the tables at once and
    the workers chosen together, each check released once its table is
    in - and never later than the per-choice greedy simulated on the same
    model, but for the fewer workers it keeps (`WORKERS_WITHIN`)."""
    jobs = [j for j in jobs if j.ways]
    if not jobs:
        return Plan({}, 0.0, workers=0, how="nothing to move")
    base, first = greedy(jobs, limits)
    over_money = (limits.money is not None
                  and money(jobs, first) > limits.money + 1e-9)
    # the mix the search found, and each table's fastest alone where that
    # keeps to the money: a lower bound is not yet a sooner end, so both
    # are simulated under every order
    mixes = [_search(jobs, first, limits)] if search else []
    if not over_money or not mixes:
        mixes.append(dict(first))
    most = int(limits.workers or 1)
    best, choice = None, None
    for mix in mixes:
        # its best order, then the tables at once under that order
        here = None
        for order in _orders(jobs, mix, limits):
            got = simulate(jobs, mix, limits, order)
            if here is None or got.makespan < here.makespan:
                here = got
        for k in _at_once(most):
            got = simulate(jobs, mix, limits, here.order, tables=k)
            if got.makespan < here.makespan:
                here = got
        if best is None or here.makespan < best.makespan:
            best, choice = here, mix
    best.how = "jointly"
    after_all = False
    if base.makespan <= best.makespan and not over_money:
        best, choice, after_all = base, first, True
        best.how = "each choice alone - no joint plan ended sooner"
    # the fewest workers whose end is within reach of the most allowed
    tables = best.tables
    for k in range(1, most):
        got = simulate(jobs, choice, limits, best.order, workers=k,
                       check_after_all=after_all, tables=min(tables, k))
        if got.makespan <= best.makespan * (1 + WORKERS_WITHIN):
            got.how = best.how
            best = got
            break
    best.greedy = base.makespan
    best.bound, best.binding = bound(jobs, choice, limits, best.workers)
    best.money = money(jobs, choice)
    # every way at its p90 at once, and never under the plan's own floor:
    # probes of seconds do not see what a server does over an hour
    from .planner import P90_LEAST
    best.p90 = max(simulate(jobs, choice, limits, best.order, best.workers,
                            slow=P90_SD, check_after_all=after_all,
                            tables=best.tables).makespan,
                   best.makespan * (1 + P90_LEAST))
    best.meets, best.why = _meets(best, limits)
    best.truth = ("partly predicted" if any(
        choice[j.name].truth == "predicted" for j in jobs) else "measured")
    for j in jobs:
        best.placed[j.name].alone = {w.said or w.name: alone(j, w, limits)
                                     for w in j.ways}
    return best


def _meets(plan, limits):
    """(whether the plan keeps within the hop's limits, why not)."""
    from .wording import _duration
    if limits.deadline is not None and plan.makespan > limits.deadline:
        return False, (f"the move needs ~{_duration(plan.makespan)} and the"
                       f" deadline gives {_duration(limits.deadline)}:"
                       f" {_said(plan.binding)} sets it")
    if limits.money is not None and plan.money > limits.money + 1e-9:
        return False, (f"every way here bills more than the {limits.money:g}"
                       " the hop allows")
    return True, ""


def _said(what):
    return {SOURCE: "the source's reads", TARGET: "the target's writes",
            "link:src": "the link to the source",
            "link:dst": "the link to the target", "cpu": "this machine's"
            " CPUs", "workers": "the workers allowed at once",
            "the longest table": "the longest table"}.get(what, what)


def lines(plan):
    """What the plan means for the operator: the end with its spread, what
    sets it, and whether it keeps to the hop's limits - no way named."""
    from .wording import _duration
    if not plan.placed:
        return []
    out = [f"the copy with its index builds and checks: ~"
           f"{_duration(plan.makespan)}"
           + (f", p90 ~{_duration(plan.p90)}" if plan.p90
              and plan.p90 > plan.makespan * 1.05 else "")
           + f", {plan.workers} at a time, set by {_said(plan.binding)}"
           + (f" ({plan.truth})" if plan.truth else "")]
    if plan.greedy and plan.greedy > plan.makespan * 1.01:
        out.append(f"planned as one: ~{_duration(plan.greedy - plan.makespan)}"
                   f" sooner than each choice made alone"
                   f" (~{_duration(plan.greedy)})")
    if not plan.meets:
        out.append(f"NOT WITHIN THE HOP'S LIMITS: {plan.why}")
    return out


# --- from the resource model ------------------------------------------------

def caps_of(model):
    """{resource: bytes a second all streams together carry} from the
    resource model's stages (`model.Stage.total`), the floors
    (`at_least`) kept as caps too - a floor is what was seen carried."""
    out = {}
    for s in (getattr(model, "stages", None) or {}).values():
        if s.total and s.name != "target-check":
            out[s.name] = float(s.total)
    return out


def way_of(model, name="as measured", said="", per_stream=None, uses=None,
           startup=0.0, **kw):
    """The way the resource model measured, as the scheduler weighs it:
    each byte through each stage as often as the stage's steps say, one
    stream at the model's own seconds a byte."""
    if uses is None:
        uses = {}
        for s in (getattr(model, "stages", None) or {}).values():
            if s.name == "target-check" or not s.total:
                continue
            uses[s.name] = len(s.steps or ("copy",))
    if per_stream is None:
        spb = model.seconds_per_byte() if model is not None else None
        per_stream = 1.0 / spb if spb else 0.0
    spread = getattr(model, "spread", 0.0) or 0.0
    if not (getattr(model, "observed", None) or {}).get("seconds_a_row"):
        # the probes alone: a prior, not the truth (`MODEL_SPREAD`)
        spread = max(spread, MODEL_SPREAD)
    return Way(name, said, per_stream, uses, startup, spread=spread,
               truth="measured", **kw)


def way_of_strategy(strategy, unit_bytes, name=None):
    """A strategy `decide.space` weighed, as the scheduler weighs it: one
    stream's bytes a second from what it costs on a unit of `unit_bytes`,
    the resources its parts load (the most passes any part makes of
    each), their start-ups, its spread - so the plan picks among every
    way that reaches the strongest guarantees, not only the first."""
    est = getattr(strategy, "estimate", None)
    mean = getattr(est, "mean", est)
    if not mean or not unit_bytes:
        return None
    uses, startup = {}, 0.0
    for r in strategy.parts.values():
        for res, n in (getattr(r, "loads", ()) or ()):
            uses[res] = max(uses.get(res, 0), n)
        startup += float(getattr(r, "startup", 0.0) or 0.0)
    work = max(float(mean) - startup, 1e-9)
    return Way(name or "; ".join(r.name for r in strategy.parts.values()),
               strategy.said if strategy.parts else "",
               float(unit_bytes) / work, uses, startup,
               spread=(getattr(est, "sd", 0.0) / mean if mean else 0.0),
               truth=getattr(est, "truth", "measured"))


def ways_of_space(space, unit_bytes, most=3):
    """The best `most` strategies of a `decide.Space` that reach its
    strongest guarantees, as ways (`way_of_strategy`)."""
    out = []
    best = getattr(space, "best", None)
    for s in ([best] if best else []) + [s for s, _ in
                                         getattr(best, "runners", ())]:
        w = way_of_strategy(s, unit_bytes)
        if w is not None:
            out.append(w)
        if len(out) >= most:
            break
    return tuple(out)


def jobs_of(facts, ways, ranges_least=None, check=None):
    """[Job] from the source's catalogue (`table_facts`): each table's
    bytes, rows, index bytes and ranges, on the ways `ways` gives - a
    callable of (table, fact) or one tuple for every table."""
    from . import ranges as _ranges
    least = ranges_least or _ranges.LEAST
    out = []
    for t, f in (facts or {}).items():
        if not isinstance(f, dict):
            continue
        size = float(f.get("bytes") or 0)
        rows = int(f.get("rows") or 0)
        if size <= 0:
            continue
        n = max(1, rows // least) if f.get("key", True) else 1
        got = ways(t, f) if callable(ways) else ways
        if not got:
            continue
        out.append(Job(t, size, rows, float(f.get("index_bytes") or 0), n,
                       tuple(got), check))
    return out


#: what one writer stream is taken to hold of the target's memory where
#: the hop limits it (`limits: target_memory`): the target's own share
#: for a session's sorts and an index build's (PostgreSQL's
#: maintenance_work_mem and MySQL's sort buffer both default to 64 MB)
TARGET_STREAM_MEMORY = 64 * 2 ** 20
#: connections a writer stream holds on the target (`sizing`)
_CONNECTIONS = 2


def share_limit(v, what):
    """A share written 0.3 or 30%."""
    text = str(v).strip()
    try:
        got = float(text[:-1]) / 100 if text.endswith("%") else float(text)
    except ValueError:
        raise SystemExit(f"limits: {what} {v!r} is not a share (0.3 or"
                         " 30%)") from None
    if not 0 < got <= 1:
        raise SystemExit(f"limits: {what} {v!r} is not between 0 and 100%")
    return got


def _number(v, what):
    try:
        got = float(v)
    except (TypeError, ValueError):
        raise SystemExit(f"limits: {what} {v!r} is not a number") from None
    if got < 0:
        raise SystemExit(f"limits: {what} {v!r} is below zero")
    return got


def _bytes(v, what):
    """Bytes written as a number or with a unit (8GB, 512MB)."""
    import re
    text = str(v).strip().upper()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([KMGT]?)I?B?", text)
    if not m:
        raise SystemExit(f"limits: {what} {v!r} is not a size (8GB)")
    return float(m.group(1)) * 1024 ** "_KMGT".index(m.group(2) or "_")


def limits_of(hop, model=None, workers=None):
    """The `Limits` of one database's move of `hop`: the resource model's
    caps, the workers `sizing` allows at most, and the hop's own limits -
    `deadline:` and `limits:` (`hopkeys.LIMITS`), each a hard limit. A
    limit written wrong stops the plan, said."""
    from . import machine
    opts = getattr(hop, "options", None) or {}
    lim = opts.get("limits") or {}
    most = int(workers or getattr(hop, "workers_most", 0)
               or getattr(hop, "workers", 0) or 4)
    if lim.get("target_connections") is not None:
        most = min(most, max(1, int(_number(lim.get("target_connections"),
                                            "target_connections")
                                    // _CONNECTIONS)))
    if lim.get("target_memory") is not None:
        most = min(most, max(1, int(_bytes(lim.get("target_memory"),
                                           "target_memory")
                                    // TARGET_STREAM_MEMORY)))
    money = None
    if lim.get("money") is not None:
        money = _number(lim.get("money"), "money")
    if lim.get("bytes_billed") is not None:
        price = opts.get("transfer_price_per_gb")
        billed = _bytes(lim.get("bytes_billed"), "bytes_billed")
        as_money = billed / 2 ** 30 * float(price) if price else None
        money = (as_money if money is None else
                 min(money, as_money) if as_money is not None else money)
    target = (getattr(model, "stages", None) or {}).get(TARGET)
    return Limits(caps=caps_of(model) if model is not None else {},
                  workers=most, deadline=machine.deadline(hop),
                  money=money,
                  index_rate=(target.per_stream if target is not None
                              else None),
                  source_share=(share_limit(lim.get("source_load"),
                                       "source_load")
                                if lim.get("source_load") is not None
                                else None))


def outage_line(hop, phases):
    """Whether the cutover's predicted outage keeps within the hop's
    `limits: outage` (or `cutover:`'s budget), in words - "" where the hop
    sets neither or the plan has no cutover time."""
    opts = getattr(hop, "options", None) or {}
    lim = opts.get("limits") or {}
    budget = lim.get("outage")
    if budget is None:
        conf = opts.get("cutover")
        budget = conf.get("budget") if isinstance(conf, dict) else None
    if budget is None:
        return ""
    budget = _number(budget, "outage")
    cut = next((p for p in phases or () if p.name == "cutover"), None)
    if cut is None or cut.p50 is None:
        return ""
    from .wording import _duration
    worst = cut.p90 or cut.p50
    if worst <= budget:
        return (f"the cutover's outage ~{_duration(cut.p50)} (p90"
                f" ~{_duration(worst)}) keeps within the {budget:g}s the"
                " hop allows")
    return (f"NOT WITHIN THE HOP'S LIMITS: the cutover's outage may reach"
            f" ~{_duration(worst)} (p90) against the {budget:g}s the hop"
            " allows - a quieter hour on the source, or the cutover's"
            " check narrowed to what changed, would shorten it")


def plan_move(model, facts, limits, ways=None, check=None):
    """The joint plan of one database's move from its resource model and
    catalogue, or None where the model measured no rate to plan from."""
    if model is None or not getattr(model, "measured", False):
        return None
    w = ways or (way_of(model),)
    if not callable(w) and not any(x.per_stream for x in w):
        return None
    jobs = jobs_of(facts, w, check=check)
    if not jobs:
        return None
    if not limits.caps:
        limits.caps = caps_of(model)
    return joint(jobs, limits)


def _per_stream(model, names):
    """Bytes a second one stream gets through the stages `names`: in each
    step its slowest of them, the steps one after the other - or the
    longer where the model overlaps them."""
    stages = [s for n, s in (model.stages or {}).items()
              if n in names and s.per_stream]
    steps = []
    for s in stages:
        for step in s.steps:
            if step not in steps:
                steps.append(step)
    each = [max([1.0 / s.per_stream for s in stages if step in s.steps]
                or [0.0]) for step in steps]
    if not each:
        return 0.0
    secs = max(each) if model.overlapped else sum(each)
    return 1.0 / secs if secs else 0.0


def ways_of(model):
    """The ways a table's rows can go, as the resource model measured
    them: the path it measured (`way_of`), and where that path is server
    to server, the path through migkit beside it - its links and this
    machine's CPUs as the probes measured them, predicted."""
    here = way_of(model, name="as measured", said=model.path or "")
    if "server to server" not in (model.path or ""):
        return (here,)
    names = ("source", "link:src", "cpu", "link:dst", "target",
             "target-check")
    rate = _per_stream(model, names)
    if not rate:
        return (here,)
    uses = {n: len(s.steps or ("copy",)) for n, s in model.stages.items()
            if n in names and n != "target-check" and s.total}
    pipe = Way("through migkit", "its rows through migkit", rate, uses,
               spread=max(model.spread or 0.0, 0.2), truth="predicted")
    return (here, pipe)


def check_of(model):
    """The way a table's check reads it after the copy: both sides read
    and digested there, one stream at the slower of the two."""
    src = (model.stages or {}).get(SOURCE)
    dst = (model.stages or {}).get("target-check") or (
        model.stages or {}).get(TARGET)
    rates = [s.per_stream for s in (src, dst) if s is not None
             and s.per_stream]
    if not rates:
        return None
    return Way("check", "both sides read and digested", min(rates),
               {SOURCE: 1, TARGET: 1}, truth="measured")


#: the joint plan kept beside the database's reports, for the move that
#: follows its plan and for `watch`
PLAN_FILE = "plan.json"


def for_move(hop, model, facts, tables=None, workers=None, db=None):
    """The joint plan of one database's move, kept beside its reports
    (`PLAN_FILE`), or None where the model measured nothing to plan from.
    `tables` limits it to the tables the move carries."""
    if model is None or not getattr(model, "measured", False):
        return None
    got = {t: f for t, f in (facts or {}).items()
           if tables is None or t in tables}
    lim = limits_of(hop, model, workers)
    plan = plan_move(model, got, lim, ways_of(model), check_of(model))
    if plan is not None and db is not None:
        try:
            import json
            import os
            path = os.path.join(str(hop.report_dir(db)), PLAN_FILE)
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(plan.as_evidence(), f, default=str)
            os.replace(tmp, path)
        except (OSError, AttributeError, TypeError):
            pass
    return plan


def account(hop, model, facts, plan=None):
    """What the plan of a move stood on (`factbook.account`): the facts
    known from the hop and the catalogue, the ones measured by the
    resource model's probes or earlier runs, the ones predicted, and the
    plan's factors nobody knew - each one that would change it."""
    from . import factbook, machine
    opts = getattr(hop, "options", None) or {}
    known, measured, predicted = {"engine"}, set(), set()
    if getattr(hop, "engine", "") == "hetero" or opts.get("source_engine"):
        known.add("pair")
    for f in (facts or {}).values():
        if isinstance(f, dict):
            for k in ("bytes", "rows", "key", "index_bytes", "partitions"):
                if f.get(k) is not None:
                    known.add(k)
    if model is not None:
        if model.widest:
            known.add("widest")
        for side in (model.sides or {}).values():
            for k in ("free_connections", "cpus", "replica"):
                if side.get(k) is not None:
                    known.add(k)
            if side.get("rtt") is not None:
                measured.add("rtt")
            if side.get("loss") is not None:
                measured.add("loss")
            if side.get("running") is not None:
                known.add("load")
        for s in (model.stages or {}).values():
            from .model import TRUTHS
            # measured now, on an earlier run or on the rehearsal
            which = measured if s.truth in TRUTHS[:3] else predicted
            which.add("costs")
            if s.name.startswith("link") and s.truth == "measured":
                measured.add("bandwidth")
            if s.name == "cpu":
                known.add("machine_cpus")
        if model.arrival:
            measured.add("change rate")
        if model.worker_memory:
            known.add("machine_memory")
    if plan is not None and any(
            t.get("way") == "through migkit"
            for t in (plan.as_evidence().get("tables") or {}).values()):
        predicted.add("costs")
    if machine.deadline(hop) is not None:
        known.add("deadline")
    lim = opts.get("limits") or {}
    for k, v in (("source_load", lim.get("source_load")),
                 ("money", lim.get("money")),
                 ("outage", lim.get("outage"))):
        if v is not None:
            known.add(k)
    if opts.get("transfer_price_per_gb") is not None:
        known.add("egress_price")
    return factbook.account(known, measured, predicted)


def kept(hop, db):
    """The joint plan kept for `db`, as data, or None."""
    import json
    import os
    try:
        with open(os.path.join(str(hop.report_dir(db)), PLAN_FILE)) as f:
            return json.load(f)
    except (OSError, ValueError, AttributeError, TypeError):
        return None


def order_of(hop, db, names):
    """`names` in the order the kept plan starts them - or None where no
    plan is kept, or the one kept was made for other tables."""
    got = kept(hop, db)
    if not got or not got.get("order"):
        return None
    at = {n: i for i, n in enumerate(got["order"])}
    if any(n not in at for n in names):
        return None
    return sorted(names, key=lambda n: at[n])


def planned_way(hop, db, table):
    """The way the kept plan puts `table` on ("as measured", "through
    migkit"), or None: for the copier choosing a table's way, which
    stands on the plan's mix where it has no measurement of its own."""
    got = kept(hop, db) or {}
    t = (got.get("tables") or {}).get(table)
    return t.get("way") if t else None


#: `bench/smart_decisions.py schedule`, 2026-10-01: what the docstring
#: gives, ratios of the ends (below one is sooner)
MEASURED = {
    "moves": 200, "within money": 195,
    "joint/greedy": {"mean": 0.913, "median": 0.923, "p10": 0.816,
                     "best": 0.679, "worst": 1.014},
    "without the mix/greedy mean": 0.916,
    "5% or more sooner": 132, "greedy over the money": 5,
    "joint/lower bound": {"median": 1.036, "p90": 1.294},
    "ms to plan": {"median": 14.6, "max": 810.5},
    "cpsat/joint (40 moves, 5 s each)": {"mean": 1.023, "median": 1.004,
                                         "best": 0.968, "seconds": 5.04},
}
