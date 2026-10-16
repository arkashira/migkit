"""A large table copied as ranges of its key, side by side.

A table copier went through one table one range at a time, and a move is
as long as its largest table: measured on a 300,000-row database, one
table of 200,000 rows set the time whatever else ran beside it. A table
with a single integer key is split into ranges of equal rows - not equal
spans of the key, which on sparse keys left most ranges empty, each still
paying for its statements - and the ranges are copied by as many workers
as the move has. Every range is copied, read back and checkpointed on its
own, so a run started again copies only the ranges not done.

The workers are the move's, shared by every table in flight: a table
copied beside others does not multiply them.
"""
import contextlib
import contextvars
import threading
from dataclasses import dataclass, field

_NOLOCK = contextlib.nullcontext()

#: the least rows a range holds when a table is split for workers
LEAST = 50_000

_current_pool_var = contextvars.ContextVar("current_read_pool", default=None)
_current_coordinator_var = contextvars.ContextVar("current_coordinator", default=None)
_active_lease_var = contextvars.ContextVar("active_lease", default=None)


def current_pool():
    return _current_pool_var.get()


def current_coordinator():
    return _current_coordinator_var.get()


class _Lease:
    """A slot held by one unit of work. `borrowed`: it rides on the slot
    of the unit that started it and waits for it (`parent_lease`), so it
    takes none of its own; `lent`: its own slot is ridden on by such a
    unit now. The two were one flag: a borrowed lease read as already
    lent, so the third level down - a check of a table inside the check
    of a database inside its unit - waited for a slot its own caller held,
    and with one worker the command waited on itself for good."""

    def __init__(self, slots, borrowed=False, parent_lease=None):
        self.slots = slots
        self.root = slots.root
        self.borrowed = borrowed
        self.lent = False
        self.parent_lease = parent_lease


class ReadPool:
    def __init__(self, hop, engine=None, db=None, workers=None, parent=None, coordinator=None):
        import concurrent.futures as cf
        from . import sizing

        if parent is None:
            parent = current_pool() or getattr(engine, "_active_read_pool", None) or getattr(hop, "_read_pool", None)
        if coordinator is None and parent is not None:
            coordinator = getattr(parent, "slots", None)
        if coordinator is None:
            coordinator = current_coordinator() or getattr(engine, "_slot_coordinator", None) or getattr(hop, "_slot_coordinator", None)

        self.parent = parent
        self.coordinator = coordinator

        plan = None
        if hop is not None and engine is not None and db is not None:
            try:
                plan = sizing.estimate(hop, engine, db)
            except Exception:
                plan = None

        plan_most = plan.most if plan is not None else (int(workers) if workers else 4)
        plan_start = plan.start if plan is not None else min(2, plan_most)

        most = min(plan_most, max(1, int(workers))) if workers else plan_most
        if coordinator is not None:
            most = min(most, coordinator.workers)
        elif parent is not None and getattr(parent, "slots", None) is not None:
            most = min(most, parent.slots.workers)

        resources = {}
        if engine is not None and db is not None and hop is not None:
            try:
                resources = sizing.resources(engine, db, hop)
            except Exception:
                resources = {}
        if engine is not None and db is not None:
            # what the failure catalogue reads of each side, one signal of
            # the pace's own (`sizing.catalogue`): the pace alone moves
            # the width, read once a window, not once a unit
            resources.setdefault(sizing.CATALOGUE,
                                 sizing.catalogue(engine, db))
        self.pace = sizing.Pace(min(plan_start, most), most,
                               resources=resources,
                               memory=sizing.Memory(sizing.WORKER_MEMORY))

        parent_slots = coordinator or (parent.slots if parent is not None else None)
        self.slots = Slots(most, pace=self.pace, parent=parent_slots)
        self.pool = cf.ThreadPoolExecutor(max_workers=most)

    def submit(self, fn, *args, **kwargs):
        import time

        ctx = contextvars.copy_context()

        def read():
            lease = self.slots._take()
            token = _active_lease_var.set(lease)
            pool_token = _current_pool_var.set(self)
            coord_token = _current_coordinator_var.set(self.slots)
            began = time.monotonic()
            weight = 0
            try:
                result = fn(*args, **kwargs)
                weight = _weight(result)
                return result
            finally:
                try:
                    _current_coordinator_var.reset(coord_token)
                    _current_pool_var.reset(pool_token)
                    _active_lease_var.reset(token)
                finally:
                    self.slots._give(lease)
                    if self.pace is not None:
                        self.pace.done(weight, time.monotonic() - began)

        return self.pool.submit(ctx.run, read)

    def shutdown(self, wait=True, *, cancel_futures=False):
        return self.pool.shutdown(wait=wait, cancel_futures=cancel_futures)

    def __enter__(self):
        self._pool_token = _current_pool_var.set(self)
        self._coord_token = _current_coordinator_var.set(self.slots)
        return self

    def __exit__(self, *exc):
        try:
            self.shutdown(wait=True, cancel_futures=exc[0] is not None)
        finally:
            if hasattr(self, "_coord_token"):
                _current_coordinator_var.reset(self._coord_token)
            if hasattr(self, "_pool_token"):
                _current_pool_var.reset(self._pool_token)
        return False


class Slots:
    """How many ranges the whole move copies at once: at most `workers`,
    and as many of them as the move's pace (`sizing.Pace`) finds fastest,
    told by every range that finishes how many rows it moved in how
    long."""

    def __init__(self, workers, pace=None, parent=None):
        self.workers = max(1, int(workers))
        self.parent = parent
        self.root = parent.root if parent is not None else self
        if parent is not None:
            self.workers = min(self.workers, self.root.workers)
        self.pace = pace if pace is not None else (parent.pace if parent is not None else None)
        self._cv = self.root._cv if parent is not None else threading.Condition()
        self._busy = 0

    def _take(self, parent_lease=None):
        if parent_lease is None:
            parent_lease = _active_lease_var.get()
        if parent_lease is not None and getattr(parent_lease, "root", None) is not self.root:
            parent_lease = None

        with self._cv:
            while True:
                # the caller waits on this unit holding its slot: the unit
                # runs on it, at any depth, before a slot of its own is
                # counted - asked first, so a full count never makes a
                # unit wait for the slot its own caller holds
                if parent_lease is not None and not parent_lease.lent:
                    parent_lease.lent = True
                    return _Lease(self, borrowed=True, parent_lease=parent_lease)

                if self._busy >= self.workers:
                    self._cv.wait(0.1)
                    continue

                root = self.root
                root_limit = (min(root.workers, root.pace.limit)
                              if root.pace else root.workers)
                can_admit = root.pace.admit(root._busy) if root.pace else True

                if root._busy < root_limit and can_admit:
                    if self is not root:
                        root._busy += 1
                    self._busy += 1
                    return _Lease(self, borrowed=False)

                self._cv.wait(0.1)

    def _give(self, lease=None):
        with self._cv:
            if lease is not None and lease.borrowed:
                if lease.parent_lease is not None:
                    lease.parent_lease.lent = False
            else:
                self._busy -= 1
                if self is not self.root:
                    self.root._busy -= 1
            self._cv.notify_all()

    def each(self, items, fn):
        """fn(item) for every item, as many at a time as there are slots.
        One that fails lets those in flight finish, starts no more, and is
        raised."""
        import concurrent.futures as cf
        import time
        take = items.take if isinstance(items, Pending) else None
        items = [] if take else list(items)

        def one(item):
            lease = self._take()
            token = _active_lease_var.set(lease)
            coord_token = _current_coordinator_var.set(self)
            began = time.monotonic()
            weight = 0
            try:
                got = fn(item)
                weight = _weight(got)
                return got
            finally:
                try:
                    _current_coordinator_var.reset(coord_token)
                    _active_lease_var.reset(token)
                finally:
                    self._give(lease)
                    if self.pace is not None:
                        self.pace.done(weight, time.monotonic() - began)
        if take is not None:
            return self._each_taken(take, one)
        if self.workers < 2 or len(items) < 2:
            for item in items:
                one(item)
            return
        failed = None
        pool = cf.ThreadPoolExecutor(min(self.workers, len(items)))
        try:
            futs = [pool.submit(contextvars.copy_context().run, one, item) for item in items]
            for f in cf.as_completed(futs):
                if f.cancelled():
                    continue
                if f.exception() is not None and failed is None:
                    failed = f.exception()
                    for g in futs:
                        g.cancel()
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
        if failed is not None:
            raise failed


    def _each_taken(self, take, one):
        """`each` over a plan's ranges taken one at a time (`Pending`): no
        more handed out than there are slots, so a range not yet taken is
        still the plan's to cut again (`refine`) as the copy measures it."""
        import concurrent.futures as cf
        if self.workers < 2:
            while (item := take()) is not None:
                one(item)
            return
        failed, running = None, set()
        pool = cf.ThreadPoolExecutor(self.workers)

        def more():
            while failed is None and len(running) < self.workers:
                item = take()
                if item is None:
                    return
                running.add(pool.submit(contextvars.copy_context().run, one,
                                        item))
        try:
            more()
            while running:
                ended, _ = cf.wait(running, return_when=cf.FIRST_COMPLETED)
                for f in ended:
                    running.discard(f)
                    if f.exception() is not None and failed is None:
                        failed = f.exception()
                more()
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
        if failed is not None:
            raise failed

    def each_process(self, items, fn, done, begin=None):
        """fn(item) in a process of its own for every item, as many at once
        as the move's slots allow, and done(item, result) here as each
        finishes - so what finished is recorded even if a later one fails;
        begin(item) here as each is handed over. One that fails starts no
        more, lets those running finish, and is raised.

        The process is `python -m migkit.ranges`, handed `fn` and the item
        on its input: the standard library's own process pools start by
        importing the program that started them, which ran a script that
        called migkit a second time in every process."""
        # a process a slot, kept for every range that slot copies: one
        # started for each range paid its start, its imports and its
        # connections again, and more, smaller ranges - what lets the pace
        # move in small steps - were slower for it (measured, a million
        # rows: 7.7s in eight ranges, 11.2s in sixteen)
        mine, kept = threading.local(), []

        def one(item):
            worker = getattr(mine, "worker", None)
            if worker is None or not worker.alive():
                worker = mine.worker = _Worker()
                kept.append(worker)
            if begin is not None:
                begin(item)
            got = worker.run(fn, item)
            done(item, got)
            return got
        try:
            self.each(items, one)
        finally:
            for worker in kept:
                worker.close()


def _weight(got):
    """The rows a finished range reports, however its copier says it: a
    count, a count first in a pair, or a tally; one where it says none."""
    if isinstance(got, bool):
        return 1
    if isinstance(got, int):
        return got
    if isinstance(got, tuple) and got and isinstance(got[0], int):
        return got[0]
    n = getattr(got, "n", None)
    return n if isinstance(n, int) else 1


class WorkerGone(RuntimeError):
    """A worker process that stopped without answering."""


class _Worker:
    """A `python -m migkit.ranges` process that copies range after range:
    each asked as a length and a pickled `(fn, item)` on its input, each
    answered as a length and a pickled `(ok, result or exception)` on its
    output. What it prints goes to a file, for the error if it dies.

    Asked and answered apart (`send`, `receive`) where the asker has other
    work meanwhile: the tail's reader decodes the next batch while the one
    before it is applied (`hetero._ReadProcess`)."""

    def __init__(self, what="a copy process"):
        import subprocess
        import sys
        import tempfile
        self.what = what
        self.said = tempfile.TemporaryFile()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "migkit.ranges"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.said)

    def alive(self):
        return self.proc.poll() is None

    def run(self, fn, item):
        self.send(fn, item)
        return self.receive()

    def send(self, fn, item):
        import pickle
        import struct
        data = pickle.dumps((fn, item))
        try:
            self.proc.stdin.write(struct.pack(">Q", len(data)) + data)
            self.proc.stdin.flush()
        except Exception:  # noqa: BLE001 - it died before it was asked
            self._gone()

    def receive(self):
        import pickle
        import struct
        try:
            head = self.proc.stdout.read(8)
            body = (self.proc.stdout.read(struct.unpack(">Q", head)[0])
                    if len(head) == 8 else b"")
            ok, got = pickle.loads(body)
        except Exception:  # noqa: BLE001 - it died before it answered
            self._gone()
        if not ok:
            raise got
        return got

    def _gone(self):
        self.proc.kill()
        self.proc.wait()
        self.said.seek(0)
        last = self.said.read().decode(errors="replace").strip()
        raise WorkerGone(
            f"{self.what} stopped without an answer: "
            + (last.splitlines()[-1] if last else
               f"exit code {self.proc.returncode}"))

    def close(self, wait=True):
        """Its input closed and its end awaited; `wait=False` stops it
        where it is, for an answer nobody will read."""
        try:
            if not wait:
                self.proc.kill()
            self.proc.stdin.close()
            self.proc.wait(timeout=30)
        except Exception:  # noqa: BLE001 - it is going anyway
            self.proc.kill()
        self.said.close()


def _in_process(fn, item):
    """fn(item) in a new interpreter; its result, or its exception
    raised here."""
    worker = _Worker()
    try:
        return worker.run(fn, item)
    finally:
        worker.close()


def _child():
    import pickle
    import struct
    import sys
    into, out = sys.stdin.buffer, sys.stdout.buffer
    # what the copy prints is not the answer
    sys.stdout = sys.stderr
    while True:
        head = into.read(8)
        if len(head) < 8:
            return
        fn, item = pickle.loads(into.read(struct.unpack(">Q", head)[0]))
        try:
            answer = (True, fn(item))
        except BaseException as e:  # noqa: BLE001 - carried back whole
            try:
                pickle.dumps(e)
            except Exception:  # noqa: BLE001 - said, if it cannot travel
                e = RuntimeError(f"{type(e).__name__}: {e}")
            answer = (False, e)
        data = pickle.dumps(answer)
        out.write(struct.pack(">Q", len(data)) + data)
        out.flush()


#: the slots of the move in progress; one at a time where none is
active = Slots(1)


def ident(v):
    """How a range's start is named in a checkpoint entry: the number as
    its digits, any other key (text, a composite's tuple, None for the open
    first end) as JSON, so a key read back from the file names the range it
    named when written."""
    import json
    return json.dumps(v, separators=(",", ":"), default=str)


def _h(v):
    """A key as a hashable: the list a composite key came back from JSON
    as, as the tuple it was."""
    return tuple(_h(x) for x in v) if isinstance(v, (list, tuple)) else v


def _plain(v):
    """A key as the checkpoint keeps it: what JSON holds as it is, anything
    else (a date, a decimal, a uuid) as the text the server wrote it in."""
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        raise ValueError("a binary key is not written into a statement here")
    return str(v)


def plan(st, lo, hi, edges_fn, save, table=None, lit=None):
    """The ranges of a table, planned once and kept in its checkpoint
    entry `st` so a run started again copies the same ranges: [(after,
    upto)] still to copy. Rows the source gained past either end since the
    plan was made get a range of their own; a checkpoint written before
    ranges (`last`, one range at a time) counts what it had done.

    A key that is not a whole number passes `lo` and `hi` as None: its
    ranges are cut at the edges alone, the first open below and the last
    open above (`plan_keyed`), and read with `pred`.

    `table` is the table as the source's change log names it, kept with
    the process copying it, for the tail (`already_read`)."""
    if lo is None and hi is None:
        return plan_keyed(st, edges_fn, save, table, lit or _lit)
    return _plan_whole(st, lo, hi, edges_fn, save, table)


def plan_keyed(st, edges_fn, save, table=None, lit=None):
    """`plan` for a key of any type - text, dates, uuids, a composite's
    tuples - in the order the server gave its edges. The ranges are the
    edges' (after, upto] with the first open below and the last open
    above, so the rows the source gains past either end belong to a range
    already and no minimum or maximum is read. Refused when an edge cannot
    be written into a statement (`lit`), before a single range is begun."""
    import os
    import socket
    lit = lit or _lit
    if table is not None:
        st["table"] = table
    st["copying"] = {"host": socket.gethostname(), "pid": os.getpid()}
    _DONE.pop(id(st), None)
    if "ranges" not in st:
        edges = []
        for e in edges_fn():
            e = _plain(e)
            if not edges or edges[-1] != e:
                edges.append(e)
        for e in edges:
            for v in (e if isinstance(e, list) else (e,)):
                lit(v)
        st["ranges"] = [list(r) for r in split_keyed(edges)]
        st["ranges_done"] = []
        st["keyed"] = True
        save()
    done = {_h(x) for x in st["ranges_done"]}
    return [(_h(r[0]), _h(r[1])) for r in st["ranges"]
            if _h(r[0]) not in done]


def _plan_whole(st, lo, hi, edges_fn, save, table=None):
    import os
    import socket
    if table is not None:
        st["table"] = table
    st["copying"] = {"host": socket.gethostname(), "pid": os.getpid()}
    _DONE.pop(id(st), None)
    if "ranges" not in st:
        st["ranges"] = [list(r) for r in split(lo, hi, edges_fn())]
        st["ranges_done"] = [a for a, u in st["ranges"]
                             if "last" in st and u <= st["last"]]
        save()
    if refine(st):
        save()
    rs = st["ranges"]
    if rs and hi > rs[-1][1]:
        rs.append([rs[-1][1], hi])
    if rs and lo - 1 < rs[0][0]:
        rs.insert(0, [lo - 1, rs[0][0]])
    done = {_h(x) for x in st["ranges_done"]}
    return Pending(st, [tuple(r) for r in rs if _h(r[0]) not in done],
                   _owner(save))


class Pending(list):
    """The ranges `plan` left to copy. Handed to `Slots.each`, they are
    taken one at a time (`take`), each read off the entry's plan as it is
    taken, so the ranges `finished` cut again while the copy runs
    (`refine`) are copied as they now stand; a range once taken is never
    cut again. Read whole as a list, the plan is fixed from then on: what
    was read is what is copied."""

    def __init__(self, st, rows, owner=None):
        super().__init__(rows)
        self.st, self.frozen = st, False
        self.lock = owner._lock if owner is not None else threading.RLock()
        self.taken, self._done, self._dn, self._n = set(), set(), None, 0
        self._rs, self._i = None, 0
        _WEIGHED.pop(id(st), None)
        if len(_PENDING) > 10000:
            _PENDING.clear()
        _PENDING[id(st)] = self

    def __iter__(self):
        with self.lock:
            self.frozen = True
            done = {_h(x) for x in self.st["ranges_done"]}
            return iter([tuple(r) for r in self.st["ranges"]
                         if _h(r[0]) not in done])

    def take(self):
        """The next range neither done nor taken, now marked taken; None
        where none is left."""
        with self.lock:
            rs, dn = self.st["ranges"], self.st["ranges_done"]
            if dn is not self._dn:
                self._dn, self._done, self._n = dn, set(), 0
            while self._n < len(dn):
                self._done.add(_h(dn[self._n]))
                self._n += 1
            if rs is not self._rs:
                self._rs, self._i = rs, 0
            while self._i < len(rs):
                r = rs[self._i]
                self._i += 1
                h = _h(r[0])
                if h not in self._done and h not in self.taken:
                    self.taken.add(h)
                    return tuple(r)
            return None


#: {id(entry): its `Pending`} - the ranges a copy has taken, which
#: `finished` leaves as they are when it cuts the rest again
_PENDING = {}

#: how far from the planned rows a range the finished ranges hold on
#: average before the ranges not yet taken are cut again, and the factor a
#: pending range's measured rows must pass to be split (`refine`)
OFF_BY = 2

#: {id(entry): finished ranges counted when the plan was last weighed}:
#: weighed again once that count doubled, so a plan of n ranges is
#: weighed about log2(n) times
_WEIGHED = {}


def _weigh(st):
    """Whether the finished ranges' rows say the plan is off by more than
    `OFF_BY`, checked once the count of measured ranges doubled since the
    last check."""
    held, every = st.get("ranges_rows") or {}, int(st.get("every") or 0)
    n = len(held)
    if every < 1 or n < 1 or n < 2 * _WEIGHED.get(id(st), 0.5):
        return False
    if len(_WEIGHED) > 10000:
        _WEIGHED.clear()
    _WEIGHED[id(st)] = n
    mean = sum(held.values()) / n
    return mean > OFF_BY * every or mean * OFF_BY < every


#: {id(entry, its done ranges' starts, how many ranges from the
#: first are done, its ranges)} - where `finished` goes on from
_DONE = {}


def _owner(save):
    """The checkpoint `save` writes, where it is one: its lock and its
    count of changes (`cli._Checkpoint`)."""
    owner = getattr(save, "__self__", None)
    return owner if hasattr(owner, "changed") else None


def _journaled(owner, st, event):
    """Whether `event` went into the checkpoint's journal - where it keeps
    one (`cli._Checkpoint.journals`) - rather than a whole save."""
    if owner is None or not callable(getattr(owner, "journals", None)) \
            or not owner.journals():
        return False
    owner.journal(st, event)
    return True


def _saved(save, owner, gen):
    if owner is None:
        save()
    else:
        owner.save(upto=gen)


def _reach(st, after):
    """How far every range from the start is done, `after` just finished:
    gone on from where the last range of the entry left it. It rebuilt
    the set of done ranges and walked from the first range for every one
    that finished - measured in-process, 20,000 ranges finishing in
    order: 3.41 s of it, against 0.025 s going on from where it was."""
    rs = st["ranges"]
    got = _DONE.get(id(st))
    if got is None or got[0] is not st or got[3] is not rs \
            or got[4] != len(rs) \
            or len(got[1]) + 1 != len(st["ranges_done"]):
        # the plan grown at an end, or ranges marked elsewhere: from the
        # start again
        done, i = {_h(x) for x in st["ranges_done"]}, 0
    else:
        done, i = got[1], got[2]
        done.add(_h(after))
    while i < len(rs) and _h(rs[i][0]) in done:
        i += 1
    if len(_DONE) > 10000:
        _DONE.clear()
    _DONE[id(st)] = (st, done, i, rs, len(rs))
    return rs[i - 1][1] if i else None


#: (id of a table's checkpoint, a range's start) -> (that checkpoint, what
#: closes the range's unit once it holds the range done) (`each_owned`,
#: `finished`). The checkpoint is kept with it, so an id reused by a later
#: checkpoint never reaches an earlier table's closure, and `each_owned`
#: pops every entry of its table as it ends, the ranges that raised too.
_LANDING = {}


def unit_of(table, after):
    """The attempt ledger's name for the range of `table` after `after`:
    `fleet.unit_name` where the key is a whole number, so a mover taking
    the hop over reads it; the start as the checkpoint writes it
    (`ident`) for a text key or the open first end."""
    from . import fleet
    if isinstance(after, int) and not isinstance(after, bool):
        return fleet.unit_name(table, after)
    return f"{table} k:{ident(after)}"


def each_owned(slots, eng, table, st, todo, one, log=None, settle=None):
    """`slots.each(todo, one)` with every range a unit of `eng`'s attempt
    ledger (backlog P0-ETA-ALL, P0-CONNECTION-PROGRESS): opened before the
    statement that reads it, closed done only once its checkpoint holds it
    (`finished`) - a range proved beside the next one's load closes then,
    not when `one` returns - and failed, owed again, where it raised; a
    failed range's recorded session is read for move health
    (`attempts.Monitor.unit_health`). After each range the table's
    remaining time is said from the ranges it ran
    (`attempts.Ledger.parts_as_units`, `util.forecast`). A range whose
    checkpoint never held it once all are through is closed failed -
    after `settle()`, where given: the proofs still running beside the
    loads (the last range's, or those cut short by a stop) land first, so
    a range its checkpoint holds is never told owed. A failure of
    `settle` is its caller's to raise (it waits on it again). Without a
    ledger, `slots.each(todo, one)` as it is."""
    import threading
    from . import attempts
    from .util import TASK_KINDS
    mon = attempts.monitor_of(eng)
    if mon is None:
        return slots.each(todo, one)
    led = mon.ledger
    parts = len(st.get("ranges") or ()) or None
    left = {}
    mine = set()
    gate = threading.Lock()

    def landed(name):
        def close():
            with gate:
                if left.pop(name, None) is None:
                    return
            led.unit_closed(name, True, at=mon.wall())
            _said_left(led, table, slots, log)
        return close

    def owned(rng):
        after = rng[0]
        name = unit_of(table, after)
        led.unit_opened(name, "move", "range", table, parts, at=mon.wall())
        key = (id(st), ident(after))
        with gate:
            left[name] = after
            mine.add(key)
        _LANDING[key] = (st, landed(name))
        try:
            with eng.attempt_unit(TASK_KINDS.get("move", "move"), name,
                                  after):
                got = one(rng)
        except BaseException as e:
            _LANDING.pop(key, None)
            with gate:
                left.pop(name, None)
            try:
                if not led.unit_record(name).get("stalls"):
                    mon.unit_health(name)
            except Exception:  # noqa: BLE001 - the failure is what is raised
                pass
            led.unit_closed(name, False, type(e).__name__, at=mon.wall())
            raise
        if after in (st.get("ranges_done") or ()):
            landed(name)()
        return got
    try:
        return slots.each(todo, owned)
    finally:
        if settle is not None:
            try:
                settle()
            except BaseException:  # noqa: BLE001 - raised by its caller
                pass
        with gate:
            cut = list(left)
            left.clear()
            gone, _ = list(mine), mine.clear()
        for key in gone:
            # nothing of this table's is left to close a later one's unit
            _LANDING.pop(key, None)
        for name in cut:
            # `one` returned and the checkpoint never held the range: owed
            led.unit_closed(name, False, "not proved", at=mon.wall())


def _said_left(led, table, slots, log):
    """The time `table`'s ranges not yet done take, from the ranges it
    ran: said once a range closes, with how it was reached."""
    if log is None:
        return
    from .util import forecast
    # the ranges not yet opened are owed like the rest
    units = led.owed_parts(table)
    done = sum(1 for u in units if u.state == "done")
    parts = len(units)
    owed = parts - done
    if not owed:
        return
    eta = forecast(units, scope=f"{table}'s ranges",
                   workers=min(slots.workers, owed), warmup=1)
    if eta.seconds is None:
        return
    log(f"{table}: {done} of {parts} ranges done, about {eta.seconds:.2f}s"
        f" left ({eta.lower_bound:.2f}s to {eta.upper_bound:.2f}s), timed"
        " from the ranges run")


def finished(st, after, save, rows=None):
    """A range copied: marked, and `last` moved to how far every range
    from the start is done - under the checkpoint's lock, where ranges of
    one table finish side by side, and saved by one save for every range
    that finished while another was writing (`_Checkpoint.save`)."""
    from . import failpoint
    failpoint.hit("range.committed")
    owner = _owner(save)
    plan = _PENDING.get(id(st))
    plan = plan if plan is not None and plan.st is st else None
    lock = owner._lock if owner is not None else (
        plan.lock if plan is not None else _NOLOCK)
    with lock:
        st["ranges_done"].append(after)
        if rows is not None:
            measured(st, after, rows)
        reach = _reach(st, after)
        if reach is not None:
            st["last"] = reach
        gen = owner.changed() if owner is not None else None
        # the ranges not yet taken cut again where the measured rows say
        # the plan is off, and the new plan saved whole before any of them
        # is taken: a journal line of a range the saved plan does not have
        # would read, after a stop, as the old range done
        if plan is not None and not plan.frozen and _weigh(st) \
                and refine(st, keep=plan.taken):
            save()
            journaled = True
        else:
            # a line of the checkpoint's journal, not the whole file again
            # (`cli._Checkpoint.journal`, audit ST17)
            journaled = _journaled(owner, st, {"done": after,
                                               "last": st.get("last")})
    if not journaled:
        _saved(save, owner, gen)
    # the range's unit closes once its checkpoint holds it (`each_owned`)
    landing = _LANDING.pop((id(st), ident(after)), None)
    if landing is not None and landing[0] is st:
        landing[1]()
    failpoint.hit("range.saved")


def started(st, after, mark, save):
    """A range about to be read: the source's mark taken just before it
    (`Engine.snapshot_mark`; None where it gave none), saved before the
    read begins - so a range with nothing kept has not been read. Not
    begun where this process no longer holds the lease it copies under
    (`lease.check_held`)."""
    from . import lease
    lease.check_held()
    owner = _owner(save)
    with owner._lock if owner is not None else _NOLOCK:
        st.setdefault("ranges_seen", {})[ident(after)] = mark
        gen = owner.changed() if owner is not None else None
        journaled = _journaled(owner, st, {"seen": [after, mark]})
    if not journaled:
        _saved(save, owner, gen)


def begun_ranges(st, was=None, gone=()):
    """The ranges of the entry `st` (their `ident`s) whose writes may be
    on the target, so a copy empties each before it copies it again: the
    ones `started` saw begin, and - where `was` (the entry's `copying` as
    it was read, before this run's plan named this process) is a mover of
    another machine or one this process took the hop over from (`gone`,
    `lease.taken_from`) - every range not done. A taker reads the entry
    from the machines' store, which trails the departed mover by its last
    push: a range begun after it holds documents on the target and is not
    in `ranges_seen`, which the checkpoint's journal gives back named by
    `ident` as `started` wrote it."""
    import socket
    seen = set((st.get("ranges_seen") or {}).keys())
    if not isinstance(was, dict) or not was.get("host"):
        return seen
    who = f"{was['host']}:{was.get('pid')}"
    if was["host"] == socket.gethostname() and who not in set(gone or ()):
        return seen
    done = {_h(x) for x in st.get("ranges_done") or ()}
    return seen | {ident(r[0]) for r in st.get("ranges") or ()
                   if _h(r[0]) not in done}

def already_read(st, values, covers, ahead=False, place=None):
    """Whether a change to the rows keyed `values` is in what the table
    copier read of the table whose checkpoint entry is `st`, or will be -
    so the tail leaves it out instead of applying it.

    A range is copied as it is when read, and every change after that
    read is applied; a change before it is superseded by the read. So a
    change is left out where every value it touches - its row's key, and
    the new one where it moves the key - lies in a range that

    * began its read under a mark that covers the change (`covers(mark)`
      True, `Engine.mark_covers`): what the read wrote holds the change
      or a later state of the row, a delete included; or
    * has not begun, where `ahead` says the table's copy is running now
      and the change was visible before `st` was read: the read comes
      after it.

    Anything else is applied, as before: a table copied with no ranges
    (no key, copied whole or in one pass), a value outside the plan (a
    row the source gained past its ends), a range read under no mark or
    one that cannot say, a range not begun whose copy is not known to be
    running - a copy that stopped may never come back to it, and a table
    copied again some other way is not what this entry says.

    `place`, `placer(st)` made once for an entry the tail asks about
    change after change."""
    places = (place or placer(st))(values)
    if not places:
        return False
    for where, mark in places:
        if where in ("read", "reading"):
            if mark is None or covers(mark) is not True:
                return False
        elif where != "todo" or not ahead:
            return False
    return True


def place(st, values):
    """Where each of `values` - a change's keys - lies in the ranges of the
    checkpoint entry `st`: [(where, the mark its range was begun under)],
    `where` one of `read` (its range copied), `reading` (begun and not
    done), `todo` (not begun) or `outside` (no range of the plan holds it,
    or its key is not a whole number); [] where the entry has no ranges.
    A table whose copy is done has no range left to begin: one not begun
    is `outside` then."""
    return placer(st)(values)


def placer(st):
    """`place` for the entry `st` as it stands now, its plan indexed once:
    the set of ranges done was built again for every change the tail
    asked about - 24 us a change at 2,000 ranges, 83 us at 20,000 (audit
    SP3). Held to the entry as it was when made: made again once it has
    moved."""
    import bisect
    rs = st.get("ranges")
    if not rs:
        return lambda values: []
    seen = st.get("ranges_seen") or {}
    done = {_h(x) for x in st.get("ranges_done") or ()}
    if not all(isinstance(x, int) and not isinstance(x, bool)
               for r in rs for x in r if x is not None):
        # a change's key is placed among the plan's by the number it is;
        # text and composite keys sort by the server's collation, which
        # this side cannot repeat, so each is applied
        return lambda values: [("outside", None) for _ in values or ()]
    starts = [float("-inf") if r[0] is None else r[0] for r in rs]
    finished = bool(st.get("done"))
    return lambda values: _placed(rs, starts, seen, done, finished,
                                  values, bisect)


def _placed(rs, starts, seen, done, finished, values, bisect):
    if not values:
        return []
    out = []
    for v in values:
        if not isinstance(v, int) or isinstance(v, bool):
            out.append(("outside", None))
            continue
        # asked for every change the tail reads: found by halves, the
        # plan kept in key order
        i = bisect.bisect_left(starts, v) - 1
        if i < 0 or (rs[i][1] is not None and v > rs[i][1]):
            out.append(("outside", None))
            continue
        at = ident(rs[i][0])
        if at in seen:
            out.append(("read" if _h(rs[i][0]) in done else "reading",
                        seen[at]))
        elif finished or _h(rs[i][0]) in done:
            out.append(("outside", None))
        else:
            out.append(("todo", None))
    return out


def spans_to_copy(st, spans, there, count_of, restart, log, label):
    """The places a table with no key is copied from, planned once and
    kept in its checkpoint entry `st`, and [(start, where)] still to copy.

    A span is copied in one transaction and marked done once it has
    committed, so a stop leaves at most one span unaccounted for: its rows
    on the target, or not. The target's count settles which - what the
    done spans put there, or that and the next span's rows. Anything else
    and nothing says which rows are whose: `restart()` empties the table
    and it starts over, and says so."""
    if "spans" not in st:
        st["spans"] = [[start, where] for start, where in spans]
        st["spans_done"], st["span_tally"] = [], {}
        restart()
        return [tuple(x) for x in st["spans"]]
    done = set(st["spans_done"])
    known = sum(int(n) for n, _ in st["span_tally"].values())
    have = there()
    todo = [tuple(x) for x in st["spans"] if x[0] not in done]
    if have != known:
        nxt = todo[0] if todo else None
        rows = count_of(nxt[1]) if nxt else None
        if nxt and have == known + rows:
            # it committed and the checkpoint never heard: counted in, by
            # what the source holds there
            st["spans_done"].append(nxt[0])
            st["span_tally"][str(nxt[0])] = [rows, None]
            todo = todo[1:]
        else:
            log(f"{label}: the target holds {have:,} rows where the copy"
                f" had accounted for {known:,}, and a table with no key"
                " does not say which are whose - starting it over")
            for k in ("spans", "spans_done", "span_tally"):
                st.pop(k, None)
            return spans_to_copy(st, spans, there, count_of, restart, log,
                                 label)
    return todo


def span_copied(st, start, n, total, save):
    """A span committed: its rows and their fold kept, so the whole table
    can be held to what passed."""
    from . import failpoint
    failpoint.hit("span.committed")
    st["spans_done"].append(start)
    st["span_tally"][str(start)] = [int(n), None if total is None
                                    else str(total)]
    save()


def spans_total(st):
    """(rows, fold) of every span copied, or None where a span's fold is
    not known - one counted in after a stop."""
    n, total = 0, 0
    for rows, fold in st["span_tally"].values():
        if fold is None:
            return None
        n += int(rows)
        total += int(fold)
    return n, total


# --- a table with no key: where it is copied from, and how it goes on ------
# Backlog P0-KEYLESS. A table with no key has nothing to resume by but where
# its source stores its rows (PostgreSQL's pages) - on MySQL, SQL Server and
# the rest a stop started it over. It goes in buckets of its rows' hash now:
# the same buckets the check reads it by (`leaves.Spec("row")`, the top bits
# of each row's canonical hash, a node of the check's tree), found by the
# server on each side, so a bucket not finished is emptied on the target by
# the same hash and copied again, and a table that does not add up is held
# to its buckets one by one and only those that differ are copied again.
# Where the source hashes a row only by its own function (SQL Server's), the
# buckets are the source's alone and the target's count places a stop, as it
# does for pages (`spans_to_copy`).

#: rows a bucket holds on average. Each bucket is one scan of the whole
#: table on the source, its rows found by their hash - so a table in n
#: buckets is hashed n times there, and a stop costs one bucket instead of
#: the table. Set, not measured: the leaves' one-scan tree hashed 1M rows of
#: five columns in 3.68s on a 2-CPU PostgreSQL (`leaves`), so 2^4 buckets of
#: a 16M-row table add about a minute a bucket on such a server
BUCKET_ROWS = 1_000_000
#: 2^6 buckets at most
MOST_BUCKET_BITS = 6


def bucket_rows(hop=None):
    from . import tuning
    return int(tuning.value("ranges.BUCKET_ROWS", hop))


def bucket_bits(rows, hop=None):
    """Bits of the row hash a table's buckets are cut by: about
    `BUCKET_ROWS` a bucket, at least two buckets."""
    limit = bucket_rows(hop)
    n = max(2, -(-int(rows or 0) // limit))
    return min(MOST_BUCKET_BITS, (n - 1).bit_length())


def _keyless_parts(hop=None):
    from . import decide
    R, N = decide.Rung, decide.Need
    limit = bucket_rows(hop)
    large = N("a table large enough to repay a scan a bucket",
              lambda f: (f.get("rows") or 0) >= 2 * limit,
              "the table is small enough that starting it over costs less"
              " than reading it once a bucket")
    return (
        decide.Part("chunk", (
            R("pages", "spans of where the source stores its rows",
              gives=frozenset({"keyless", "resume by chunk"}),
              needs=(N("the source reads a span of its stored rows",
                       lambda f: len(f.get("pages") or ()) >= 2,
                       "the source has no stored position to read a span of"
                       " its rows by, or the table is one span"),),
              because="a span is read by a scan of just its pages; no row"
                      " is hashed to find it"),
            R("buckets", "buckets of each row's hash, found alike on both"
                         " sides", gives=frozenset({"keyless",
                                                    "resume by chunk"}),
              needs=(large,
                     N("the source hashes its rows where they are",
                       lambda f: bool(f.get("hashes")),
                       "the source renders its rows only in this process,"
                       " so a bucket would be the whole table read here")),
              because="a bucket is found by the row's own hash, the one the"
                      " check reads the table by"),
            R("server buckets", "buckets of the source's own hash of each"
                                " row", gives=frozenset({"keyless",
                                                         "resume by chunk"}),
              needs=(large,
                     N("the source's own hash of a row",
                       lambda f: bool(f.get("server hash")),
                       "the source has no hash of a row of its own")),
              because="a bucket is found by the source's own hash of the"
                      " row"),
            R("one pass", "the whole table in one pass",
              gives=frozenset({"keyless"}),
              because="nothing to resume by, and a restart starts over"),
        )),
        decide.Part("resume", (
            R("by bucket", "a bucket a stop left unfinished emptied on the"
                           " target by the same hash and copied again",
              needs=(N("the target finds a bucket's rows by the same hash",
                       lambda f: bool(f.get("target hashes")),
                       "the target cannot find a bucket's rows by the"
                       " source's hash"),),
              fits=lambda ch: ch["chunk"].name == "buckets"
              or "only a bucket of the row's canonical hash is the same"
                 " rows on the target",
              because="a stop costs the bucket it was in, and nothing needs"
                      " counting"),
            R("by count", "the target's count places a stop between two"
                          " spans", fits=lambda ch: ch["chunk"].name
              in ("pages", "buckets", "server buckets")
              or "a table in one pass has no span to place a stop between",
              because="a span commits whole, so the target's count says"
                      " whether the one in flight landed"),
            R("over", "started over", fits=lambda ch: ch["chunk"].name
              == "one pass" or "a table copied in parts goes on from them",
              because="nothing to go on from"),
        )),
    )


KEYLESS = None


def keyless_parts():
    """The parts a table with no key is copied by: how it is cut, and how a
    stop is gone on from."""
    global KEYLESS
    if KEYLESS is None:
        KEYLESS = _keyless_parts()
    return KEYLESS


def bucket_pairs(sc, dc):
    """(source columns, target columns) a bucket is found by on each side:
    those of one class on both, rendered alike in SQL (`leaves.
    bucket_columns`) - so a row's hash is the same number on both."""
    from . import leaves
    pairs = [(s, d) for s, d in zip(sc, dc) if s[1] == d[1]
             and leaves.bucket_columns([s])]
    return [s for s, _ in pairs], [d for _, d in pairs]


@dataclass
class Keyless:
    """How one table with no key is copied: the strategy, its spans -
    [(start, where on the source, where on the target or None)] - and the
    buckets' spec where it goes by the rows' hash."""
    strategy: object
    spans: list = field(default_factory=list)
    spec: object = None
    #: the checkpoint's spans were another way's, and are dropped: what
    #: the target holds of the table is not this way's to go on from
    restart: bool = False

    @property
    def chunk(self):
        return self.strategy.parts["chunk"].name if self.strategy else None

    @property
    def by_bucket(self):
        return bool(self.strategy) and \
            self.strategy.parts["resume"].name == "by bucket"


def keyless(work, st, src, dst, db, src_t, dst_t, sc, dc, rows, pages,
            salt, costs=None):
    """The `Keyless` way for one table (`keyless_parts`, composed by
    `decide.compose`), kept in its checkpoint entry `st` so a restart goes
    on the way it began. `pages()` is the source's `position_spans`, asked
    only where that way is still in the running; `rows` about how many the
    table holds; `salt` the hash's, kept with the spans."""
    from . import decide, leaves
    sb, db_cols = bucket_pairs(sc, dc)
    a = leaves.Side(src, "src", db, src_t, sb)
    b = leaves.Side(dst, "dst", db, dst_t, db_cols)
    probe = leaves.Spec("row", bits=1, salt=salt)
    facts = decide.Facts(
        {"rows": rows}, pages=pages,
        hashes=lambda: leaves.bucket_where(a, probe, 0) is not None,
        **{"target hashes": lambda: leaves.bucket_where(b, probe, 0)
           is not None,
           "server hash": lambda: src.row_bucket_sql(
               "src", db, src_t, sc, 2) is not None})
    need = ("keyless", "resume by chunk") \
        if (rows or 0) >= 2 * BUCKET_ROWS else ("keyless",)
    # spans a checkpoint holds from before the way was kept beside them
    # were a copy by pages
    kept = st.get("keyless") or ({"chunk": "pages", "resume": "by count"}
                                 if "spans" in st else None)
    strat = decide.compose(work, keyless_parts(), facts, need, costs,
                           keep=kept)
    out = Keyless(strat)
    if not strat:
        return out
    if "spans" in st and strat.names != kept:
        for k in ("spans", "spans_done", "span_tally", "spans_begun",
                  "bucket_spec"):
            st.pop(k, None)
        out.restart = True
    st["keyless"] = strat.names
    if "spans" in st:
        # planned once, gone on from as the checkpoint has it
        spec = st.get("bucket_spec")
        out.spec = leaves.Spec.of(spec) if spec else None
        out.spans = [tuple(x) + (None,) * (3 - len(x)) for x in st["spans"]]
        return out
    chunk = strat.parts["chunk"].name
    if chunk == "pages":
        out.spans = [(s, w, None) for s, w in facts["pages"]]
    elif chunk in ("buckets", "server buckets"):
        bits = bucket_bits(rows)
        if chunk == "buckets":
            out.spec = leaves.Spec("row", bits=bits, salt=salt)
            there = out.by_bucket
            out.spans = [(i, leaves.bucket_where(a, out.spec, i),
                          leaves.bucket_where(b, out.spec, i) if there
                          else None) for i in range(1 << bits)]
            st["bucket_spec"] = out.spec.as_dict()
        else:
            expr = src.row_bucket_sql("src", db, src_t, sc, 1 << bits)
            out.spans = [(i, f"({expr}) = {i}", None)
                         for i in range(1 << bits)]
    return out


def buckets_to_copy(st, spans, clear, log, label):
    """[(start, where, where on the target)] of a table's buckets still to
    copy, planned once and kept in its checkpoint entry `st`. A bucket is
    marked begun before its first row is written and done once its last
    has: one begun and not done when a run starts again is emptied on the
    target (`clear(where on the target)`) - by the same hash the source's
    rows were found by - before it is copied again. A bucket never begun
    holds nothing there: the table was emptied when the copy began."""
    if "spans" not in st:
        st["spans"] = [list(s) for s in spans]
        st["spans_done"], st["span_tally"], st["spans_begun"] = [], {}, []
    done = set(st["spans_done"])
    todo = [tuple(x) for x in st["spans"] if x[0] not in done]
    for start, _, there in todo:
        if start in st.get("spans_begun", []):
            gone = clear(there)
            log(f"{label}: bucket {start} was begun and not finished; emptied"
                f" {gone or 0:,} of its rows on the target to copy it again")
    return todo


def bucket_begun(st, start, save):
    """A bucket's first rows are about to be written."""
    begun = st.setdefault("spans_begun", [])
    if start not in begun:
        begun.append(start)
        save()


def step(rows, chunk, workers):
    """Rows per range: no more than `chunk`, and small enough that the
    workers each get a share of a table, but not below `LEAST`."""
    rows, chunk = max(int(rows or 0), 0), max(int(chunk), 1)
    share = -(-rows // max(workers * 2, 1)) if rows else chunk
    return max(min(chunk, max(share, LEAST)), 1)


def wanted(rows, chunk, workers):
    """How many ranges `rows` rows are cut into at `step(rows, chunk,
    workers)` rows each: the count a copy cuts and a check that reads the
    same rows by ranges asks, so the two cut alike."""
    rows = max(int(rows or 0), 0)
    return max(1, -(-rows // step(rows, chunk, workers)))

def bounds_sql(quote, table, key, every, where=None):
    """The statement answering the key at every `every`-th row in key
    order, in SQL both PostgreSQL and MySQL take: the upper edge of each
    range but the last."""
    k = _quoted(quote, key)
    return (f"select {k} from (select {k}, row_number() over (order by {k})"
            f" as migkit_rn from {table}"
            + (f" where ({where})" if where else "")
            + f") migkit_keys where migkit_rn % {int(every)} = 0"
            f" order by {k}")


def thin(bounds, rows, every):
    """The range edges a table's histogram gives: its equi-depth bucket
    bounds (`pg_stats.histogram_bounds`, each bucket about the same number
    of rows) thinned to the `ceil(rows / every)` ranges the table needs.
    None where the histogram is coarser than that, or empty - the caller
    then reads the key itself (`bounds_sql`).

    An edge is only where one range ends and the next begins, so a stale
    or sampled histogram makes ranges uneven, never a row left out: the
    first and last ranges are the table's own minimum and maximum."""
    need = -(-int(rows) // max(int(every), 1))
    got = sorted({int(b) for b in bounds or ()})
    buckets = len(got) - 1
    if need < 2 or buckets < need:
        return None
    return sorted({got[i * buckets // need] for i in range(1, need)})


def split(lo, hi, edges):
    """[(after, upto)] covering (lo - 1, hi] with the edges between."""
    cuts = [e for e in sorted(set(int(e) for e in edges)) if lo <= e < hi]
    out, last = [], lo - 1
    for e in cuts:
        out.append((last, e))
        last = e
    out.append((last, hi))
    return out


# --- chunk edges from the statistics (backlog F7.edges) ----------------------

#: sample points an edge stands on per range: a range of N rows has its
#: edge off by about N / sqrt(points), refined as the ranges are copied
#: (`refine`)
SAMPLE_PER_RANGE = 16


def _keys(key):
    return [key] if isinstance(key, str) else list(key)


def _quoted(quote, key):
    return ", ".join(quote(k) for k in _keys(key))


def kind_of(engine):
    """The statistics dialect of a source engine, from the class it is or
    derives from; None where migkit has no statement for its statistics."""
    for cls in type(engine).__mro__:
        name = cls.__name__.lower()
        for word, kind in (("cockroach", "cockroachdb"),
                           ("postgres", "postgresql"), ("mysql", "mysql"),
                           ("mssql", "mssql"), ("sqlserver", "mssql"),
                           ("oracle", "oracle")):
            if word in name:
                return kind
    return None


def _ordered(values):
    """Integers in numeric order; any other key in the order the server
    gave it, which is the key's collation, not Python's."""
    out, seen = [], set()
    for v in values:
        t = tuple(v) if isinstance(v, (list, tuple)) else v
        if t not in seen:
            seen.add(t)
            out.append(t)
    if all(isinstance(v, int) and not isinstance(v, bool) for v in out):
        return sorted(out)
    return out


def quantile_edges(values, need):
    """The `need - 1` edges that cut `values` - keys in key order, a
    sample's or a statistics' bounds - into `need` ranges of about equal
    rows; None where there are fewer values than ranges."""
    got = list(values)
    if all(isinstance(v, int) and not isinstance(v, bool) for v in got):
        got.sort()
    if need < 2 or len(got) < need:
        return None
    cut = _ordered(got[i * len(got) // need] for i in range(1, need))
    return cut or None


def cumulative_edges(points, need):
    """Edges from a histogram's (key, share of the rows at or below it):
    the first key reaching each i / need of the rows. Equal-depth bounds
    and a histogram with a heavy value come out alike. None where the
    histogram is coarser than the ranges."""
    pts = [(v, float(c)) for v, c in points if c is not None]
    if need < 2 or len(pts) < need:
        return None
    top = pts[-1][1] or 1.0
    out = []
    for i in range(1, need):
        goal = top * i / need
        for v, c in pts:
            if c >= goal:
                out.append(v)
                break
    out = _ordered(out)
    return out if len(out) + 1 >= max(need // 2, 2) else None


@dataclass
class Stats:
    """What a table's statistics say of a key: `points` [(key, share of
    rows at or below)], `modified` the rows written since they were taken
    (None where the engine does not say), `touched` whether the table was
    written since at all (None where not known), `analyzed_ago` the seconds
    since the statistics were taken on the server's clock and `rows` the
    rows the statistics counted and `floor` the rows known to have moved
    though the engine does not count them since the statistics (each None
    where the engine does not say)."""
    points: list
    modified: int = None
    touched: bool = None
    analyzed_ago: float = None
    rows: int = None
    floor: int = None


def _lit(value):
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value)
    if "\\" in text or "\x00" in text:
        raise ValueError("a key value with a backslash or a NUL is not"
                         " written into a statement here")
    return "'" + text.replace("'", "''") + "'"


def statistics_sql(kind, table, key, names=None):
    """The statements that read a key's histogram and what changed since,
    for a dialect, as (histogram, drift) - None where it has none."""
    col = _keys(key)[0]
    schema, name = names if names else (None, None)
    if kind == "postgresql":
        return ("select u.v, u.ord, (select n_mod_since_analyze from"
                " pg_stat_all_tables where relid = c.oid),"
                " format_type(a.atttypid, a.atttypmod), (select extract("
                "epoch from now() - greatest(last_analyze,"
                " last_autoanalyze)) from pg_stat_all_tables where relid ="
                " c.oid) from pg_stats s"
                " join pg_class c on c.relname = s.tablename join"
                " pg_namespace n on n.oid = c.relnamespace and n.nspname ="
                " s.schemaname join pg_attribute a on a.attrelid = c.oid and"
                " a.attname = s.attname cross join lateral"
                " unnest(s.histogram_bounds::text::text[]) with ordinality"
                f" as u(v, ord) where c.oid = {_lit(table)}::regclass and"
                f" s.attname = {_lit(col)} order by u.ord", None)
    if kind == "mysql" and schema:
        where = (f"where SCHEMA_NAME = {_lit(schema)} and TABLE_NAME ="
                 f" {_lit(name)} and COLUMN_NAME = {_lit(col)}")
        return (f"select cast(HISTOGRAM as char) from"
                f" information_schema.COLUMN_STATISTICS {where}",
                f"select (t.UPDATE_TIME > c.HISTOGRAM->>'$.\"last-updated\"')"
                f" from information_schema.TABLES t join"
                f" information_schema.COLUMN_STATISTICS c on"
                f" c.SCHEMA_NAME = t.TABLE_SCHEMA and c.TABLE_NAME ="
                f" t.TABLE_NAME where t.TABLE_SCHEMA = {_lit(schema)} and"
                f" t.TABLE_NAME = {_lit(name)} and c.COLUMN_NAME ="
                f" {_lit(col)}")
    if kind == "cockroachdb":
        return ("select histogram_id, row_count, extract(epoch from now() -"
                " created) from [show statistics for table "
                f"{table}] where {_lit(col)} = any(column_names) and"
                " array_length(column_names, 1) = 1 and histogram_id is not"
                " null order by created desc limit 1",
                None)
    if kind == "mssql":
        obj = str(table).replace("'", "''")
        return ("select s.stats_id, h.range_high_key, h.range_rows +"
                " h.equal_rows, p.modification_counter from sys.stats s"
                " join sys.stats_columns sc on sc.object_id = s.object_id"
                " and sc.stats_id = s.stats_id and sc.stats_column_id = 1"
                " join sys.columns c on c.object_id = sc.object_id and"
                " c.column_id = sc.column_id cross apply"
                " sys.dm_db_stats_histogram(s.object_id, s.stats_id) h"
                " cross apply sys.dm_db_stats_properties(s.object_id,"
                " s.stats_id) p where s.object_id = object_id(N'" + obj
                + f"') and c.name = {_lit(col)} order by s.stats_id,"
                " h.step_number", None)
    if kind == "oracle" and schema:
        return ("select h.endpoint_value, h.endpoint_number, (select"
                " m.inserts + m.updates + m.deletes from"
                " all_tab_modifications m where m.table_owner = h.owner and"
                " m.table_name = h.table_name and m.partition_name is null)"
                " from all_tab_histograms h where h.owner ="
                f" {_lit(schema)} and h.table_name = {_lit(name)} and"
                f" h.column_name = {_lit(col)} order by h.endpoint_number",
                None)
    return None


def _text(value):
    """A histogram bound as the key it is: the engine writes a text bound
    as `base64:type<N>:<data>`, which is decoded; refused (the statistics
    not used) where it is not text."""
    import base64
    if isinstance(value, str) and value.startswith("base64:type"):
        return base64.b64decode(value.split(":", 2)[2],
                                validate=True).decode("utf-8")
    return value


def read_statistics(kind, ask, table, key, names=None):
    """The `Stats` of a table's key as its engine holds them, or None where
    there are none or the engine gives no way to read them. Reading them
    never fails a copy."""
    import json
    import re
    sqls = statistics_sql(kind, table, key, names)
    if not sqls or len(_keys(key)) != 1:
        return None
    try:
        rows = ask(sqls[0]) or []
        if not rows:
            return None
        if kind == "postgresql":
            whole = re.fullmatch(r"(small|big)?int(eger)?\d*", rows[0][3])
            last = max(int(r[1]) for r in rows) - 1
            points = [(int(r[0]) if whole else r[0],
                       (int(r[1]) - 1) / last if last else 1.0)
                      for r in rows]
            ago = rows[0][4] if len(rows[0]) > 4 else None
            return Stats(points, modified=None if rows[0][2] is None
                         else int(rows[0][2]),
                         analyzed_ago=None if ago in (None, "")
                         else float(ago))
        if kind == "cockroachdb":
            hid, counted, ago = rows[0][:3]
            hist = ask(f"select upper_bound, range_rows + equal_rows from"
                       f" [show histogram {int(hid)}] order by upper_bound")
            running, points = 0, []
            for bound, n in hist or ():
                running += int(n)
                points.append((_plain(bound), running))
            return Stats(points, analyzed_ago=None if ago is None
                         else float(ago), rows=None if counted is None
                         else int(counted))
        if kind == "mysql":
            hist = json.loads(rows[0][0])
            points = []
            for b in hist.get("buckets") or ():
                points.append((_text(b[1]), b[2]) if len(b) > 2
                              else (_text(b[0]), b[1]))
            touched = None
            if sqls[1]:
                got = ask(sqls[1])
                if got and got[0][0] is not None:
                    touched = bool(int(got[0][0]))
            floor = None
            if names and names[0]:
                from . import unchanged
                got = ask(unchanged.COUNTERS["mysql"].modified_sql(
                    names[0], names[1]))
                if got and got[0][0] is not None:
                    floor = int(got[0][0])
            return Stats(points, touched=touched, floor=floor)
        if kind == "mssql":
            first = rows[0][0]
            rows = [r for r in rows if r[0] == first]
            running, points = 0, []
            for r in rows:
                running += int(r[2])
                points.append((r[1], running))
            return Stats(points, modified=int(rows[0][3]))
        points = [(r[0], r[1]) for r in rows]
        return Stats(points, modified=None if rows[0][2] is None
                     else int(rows[0][2]))
    except Exception:  # noqa: BLE001 - no statistics is not a failure
        return None


def sample_sql(kind, quote, table, key, rows, want, where=None):
    """The statement taking about `want` of a table's `rows` keys, in key
    order, from the live rows; None where the dialect has none or the
    sample would be the whole table."""
    if kind is None or rows <= want:
        return None
    k = _quoted(quote, key)
    share = want / max(rows, 1)
    cond = f" where ({where})" if where else ""
    more = f" and ({where})" if where else ""
    if kind == "postgresql":
        return (f"select {k} from {table} tablesample bernoulli"
                f" ({share * 100:.8f}){cond} order by {k}")
    if kind == "cockroachdb":
        return (f"select {k} from {table} where random() < {share:.10f}"
                f"{more} order by {k}")
    if kind == "mysql":
        return (f"select {k} from {table} where rand() < {share:.10f}"
                f"{more} order by {k}")
    if kind == "mssql":
        return (f"select {k} from {table} where abs(checksum(newid())) %"
                f" {max(round(1 / share), 1)} = 0{more} order by {k}")
    if kind == "oracle":
        return (f"select {k} from {table} sample ({share * 100:.8f}){cond}"
                f" order by {k}")
    return None


def cut(ask, kind, quote, table, key, rows, every, where=None, names=None,
        stats=None, into=None, log=None, label=None, wait=None):
    """The range edges of a table - keys in key order - cut the way
    `decide.CHUNK_EDGES` picks on what is known of it (`cut_with`), the
    three ways read through SQL: the engine's statistics (`stats` an
    engine's own reader returning a `Stats`, else `read_statistics`), a
    sample of the live key, the key in order. The keys may be of any type;
    the edges come back in the order the server gave them, which is the
    key's collation. `into` is the table's checkpoint entry, which keeps
    the rows a range holds and the way the edges came from for `refine`."""
    every = max(int(every), 1)
    need = -(-int(rows) // every)
    read = None if where else (stats or (
        lambda: read_statistics(kind, ask, table, key, names)))
    sql = sample_sql(kind, quote, table, key, int(rows),
                     need * SAMPLE_PER_RANGE, where)
    return cut_with(
        rows, every, lambda: [_pick(r, key) for r in ask(bounds_sql(
            quote, table, key, every, where))],
        read=read, sample=(lambda: [_pick(r, key) for r in ask(sql)])
        if sql else None, into=into, log=log, label=label or str(table),
        wait=wait)


#: seconds after an analyze in which a table's modification counter can
#: still hold writes made before it: the counter is flushed by the
#: sessions that wrote, once a second at most, and measured trailing an
#: analyze by about a second (backlog W12J)
COUNTER_LAG = 2.0


def cut_with(rows, every, order, read=None, sample=None, into=None,
             log=None, label="the table", wait=None):
    """The one place a table's range edges are chosen, for any source: the
    statistics `read()` returns (a `Stats`; None where there are none or a
    row filter narrows the rows) while the table moved less than half a
    range since, else the keys `sample()` returns in key order (None where
    the source cannot sample), else the edges `order()` reads off the key
    in order. A way that cannot run hands on to the next
    (`decide.CHUNK_EDGES`); the edges are never the key's minimum and
    maximum divided.

    A counter that trails an analyze (`COUNTER_LAG`) is read again once the
    lag has passed when it says more than half a range moved, so a table
    analyzed a moment ago is not called stale on writes the analyze already
    counted; a counter still high then is believed."""
    from . import decide
    every = max(int(every), 1)
    need = -(-int(rows) // every)
    say = log or (lambda m: None)
    half = every // 2
    got = read() if read else None
    facts = {}
    if (got is not None and got.modified is not None and got.modified > half
            and got.analyzed_ago is not None
            and got.analyzed_ago < COUNTER_LAG):
        pause = COUNTER_LAG - got.analyzed_ago
        say(f"{label}: its statistics were taken {got.analyzed_ago:.1f}s"
            f" ago and the counter of rows moved since may still hold"
            f" writes made before; read again in {pause:.1f}s")
        if wait is None:
            import time
            wait = time.sleep
        wait(pause)
        facts["statistics settled"] = True
        got = read() or got
    cand = None
    if got is not None and got.points:
        cand = cumulative_edges(got.points, need)
    if cand:
        facts["statistics"] = True
        moved = got.modified
        if moved is None and got.rows is not None:
            moved = abs(int(rows) - got.rows)
        if got.touched is False or (moved is not None and moved <= half):
            facts["statistics fresh"] = True
        elif moved is not None:
            facts["statistics stale"] = (
                f"the table moved {moved:,} rows since its"
                f" statistics, more than half a range ({half:,})")
        elif got.floor is not None and got.floor > half:
            facts["statistics stale"] = (
                f"the table moved at least {got.floor:,} rows since its"
                f" statistics, more than half a range ({half:,})")
        elif got.touched:
            facts["statistics stale"] = ("the table was written since its"
                                         " statistics were taken")
    facts["sample"] = sample is not None

    def run(choice):
        if choice.path == "statistics":
            return cand
        if choice.path == "sample":
            keys = sample()
            edges = quantile_edges(keys, need)
            if edges is None:
                raise decide.Blocked(f"the sample held {len(keys):,} keys,"
                                     f" fewer than the {need:,} ranges")
            return edges
        return order()
    choice, edges = decide.attempt(
        f"the range edges of {label}", decide.CHUNK_EDGES, run, facts,
        need=("edges",), log=say, record=False)
    say(f"{label}: range edges from {choice.said}"
        + (f" ({facts['statistics stale']}, so they are not trusted)"
           if choice.path != "statistics" and "statistics stale" in facts
           else ""))
    if into is not None:
        into["every"] = every
        into["edges_from"] = choice.path
    return _ordered(edges)


def _pick(row, key):
    """A key from a row: the value, or the tuple of a composite key's."""
    if len(_keys(key)) > 1:
        return tuple(row[:len(_keys(key))])
    return row[0]


def range_where(quote, key, after, upto, lit=_lit):
    """The condition for the keys after `after` up to and including `upto`
    in key order (None for an open end), written as the plain comparisons
    every engine takes - a composite key as its lexicographic expansion,
    which holds for strings and uuids as for integers."""
    cols = [quote(k) for k in _keys(key)]

    def beyond(point):
        vals = point if isinstance(point, (list, tuple)) else (point,)
        terms = []
        for i, v in enumerate(vals):
            eq = [f"{cols[j]} = {lit(vals[j])}" for j in range(i)]
            terms.append("(" + " and ".join(eq + [f"{cols[i]} > {lit(v)}"])
                         + ")")
        return "(" + " or ".join(terms) + ")"
    parts = []
    if after is not None:
        parts.append(beyond(after))
    if upto is not None:
        parts.append("not " + beyond(upto))
    return " and ".join(parts) or "1 = 1"


def split_keyed(edges):
    """[(after, upto)] over the edges of any key type, in the order given:
    None is an open end - the first range has no lower bound and the last
    no upper - so a key that is not a number needs no minimum or maximum
    to be read first."""
    out, last = [], None
    for e in edges:
        out.append((last, e))
        last = e
    out.append((last, None))
    return out


def pred(quote, key, rng, where=None, lit=None):
    """The condition for one range `(after, upto]` of `plan` or
    `plan_keyed`, for any key: plain comparisons on a whole-number key, the
    lexicographic expansion (`range_where`) on text, dates, uuids and a
    composite; an open end is no bound. `where` is the table's row filter,
    kept beside it."""
    after, upto = rng
    cols = _keys(key)
    whole = all(v is None or (isinstance(v, int) and not isinstance(v, bool))
                for v in (after, upto))
    if len(cols) == 1 and whole:
        c = quote(cols[0])
        bounds = ([f"{c} > {after}"] if after is not None else []) \
            + ([f"{c} <= {upto}"] if upto is not None else [])
        out = " and ".join(bounds) or "1 = 1"
    else:
        out = range_where(quote, key, after, upto, lit or _lit)
    return f"({out}) and ({where})" if where else out


def describe(key, rng):
    """A range in words for a line of the log, whatever its key."""
    after, upto = rng
    name = ", ".join(_keys(key))
    if isinstance(after, int) and isinstance(upto, int):
        return f"{name} {after + 1:,} to {upto:,}"
    if after is None and upto is None:
        return f"{name}, every key"
    if after is None:
        return f"{name} up to {upto}"
    if upto is None:
        return f"{name} after {after}"
    return f"{name} after {after} up to {upto}"


def layout(lo, hi, edges):
    """[[after, upto]] for a table's checkpoint entry from edges of any key
    type - what a copier that keeps its own plan (`claims.copy`, the
    mongodb engine) writes in place of `split`/`split_keyed` by hand:
    whole-number keys between `lo` and `hi`, any other key from the edges
    alone with its ends open."""
    if lo is None and hi is None:
        return [list(r) for r in split_keyed(
            [_plain(e) for e in edges])]
    return [list(r) for r in split(lo, hi, edges)]


def measured(st, after, rows):
    """The rows a range held, kept beside the plan for `refine`; nothing
    kept where the copier did not count them."""
    try:
        n = int(rows)
    except (TypeError, ValueError):
        return
    st.setdefault("ranges_rows", {})[ident(after)] = n


def refine(st, keep=()):
    """The ranges not yet begun cut again from what the finished ones held:
    a pending range the finished ranges' density says holds more than
    `OFF_BY` shares is split into shares, and neighbours whose rows add to
    no more than one share are merged. Only integer ranges, and only ones
    neither done, begun nor taken by a copy running now (`keep`, their
    starts), so no row is read twice or left out - an edge is where one
    range ends and the next begins. True where the plan changed."""
    rs, every = st.get("ranges"), int(st.get("every") or 0)
    held = st.get("ranges_rows") or {}
    if not rs or every < 1 or not held:
        return False
    if not all(isinstance(x, int) for r in rs for x in r):
        return False
    spans = {str(r[0]): r[1] - r[0] for r in rs}
    rows = sum(v for k, v in held.items() if k in spans)
    width = sum(spans[k] for k in held if k in spans)
    if rows < 1 or width < 1:
        return False
    density = rows / width
    done = set(st.get("ranges_done") or ())
    begun = set((st.get("ranges_seen") or {}).keys())

    def pending(r):
        return (r[0] not in done and str(r[0]) not in begun
                and _h(r[0]) not in keep)

    out = []
    for r in rs:
        guess = (r[1] - r[0]) * density
        if pending(r) and guess > OFF_BY * every:
            shares = min(-(-int(guess) // every), r[1] - r[0])
            edges = [r[0] + (r[1] - r[0]) * i // shares
                     for i in range(1, shares)]
            last = r[0]
            for e in edges + [r[1]]:
                if e > last:
                    out.append([last, e])
                    last = e
        elif (out and pending(r) and pending(out[-1])
              and (r[1] - out[-1][0]) * density <= every):
            out[-1] = [out[-1][0], r[1]]
        else:
            out.append(list(r))
    if out == [list(r) for r in rs]:
        return False
    st["ranges"] = out
    _DONE.pop(id(st), None)
    return True


if __name__ == "__main__":
    _child()



def sampled(eng, db, table, columns, key, size, seed=None, dst=None,
            dst_db=None, dst_table=None, page=None):
    """A check of `size` rows drawn at random from the source's keys and
    read by key on both sides (`Engine.neutral_rows_by_key`) - migkit's
    own keyed reader, no second program. The keys are walked page by page
    (`neutral_read` of the key columns only, `page` keys a page: the
    reader's own size where not given) and drawn by reservoir, so every
    key is as likely; `seed` makes the draw again. A record for
    `verdict.decide`: `diff` naming the keys that differ, else `ok` with
    `sampled` - which the verdict says as "no difference in the sample",
    never as the table's "same": a row outside the sample is not seen.
    `dst` is the target's engine where it is another (cross-engine)."""
    import random

    from . import canon
    dst = dst or eng
    dst_db, dst_table = dst_db or db, dst_table or table
    names = [n for n, _ in columns]
    key = list(key)
    kcols = [(n, c) for n, c in columns if n in key]
    rnd = random.Random(seed)
    drawn, seen, after = [], 0, None
    while size > 0:
        rows, after = eng.neutral_read("src", db, table, kcols, after=after,
                                       limit=page or 1000)
        for r in rows:
            seen += 1
            if len(drawn) < size:
                drawn.append(tuple(r))
            else:
                j = rnd.randrange(seen)
                if j < size:
                    drawn[j] = tuple(r)
        if not rows or after is None:
            break
    mine = eng.neutral_rows_by_key("src", db, table, columns, key, drawn)
    theirs = dst.neutral_rows_by_key("dst", dst_db, dst_table, columns, key,
                                     drawn)
    classes = [c for _, c in columns]

    def text(row):
        return canon.fold_rows(classes, [row]) if row is not None else None
    differ = sorted(k for k in set(mine) | set(theirs)
                    if text(mine.get(k)) != text(theirs.get(k)))
    out = {"scope": f"{db}.{table}", "check": "sampled rows",
           "sampled": {"rows": len(drawn), "of": seen, "seed": seed,
                       "columns": names}}
    if differ:
        out.update(status="diff", detail=f"{len(differ)} of {len(drawn)}"
                   " sampled rows differ: " + ", ".join(map(str,
                                                           differ[:5])))
    else:
        out.update(status="ok", detail=f"no difference in a sample of"
                   f" {len(drawn):,} of {seen:,} rows")
    return out
