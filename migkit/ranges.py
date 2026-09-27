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
import threading

#: the least rows a range holds when a table is split for workers
LEAST = 50_000


class Slots:
    """How many ranges the whole move copies at once: at most `workers`,
    and as many of them as the move's pace (`sizing.Pace`) finds fastest,
    told by every range that finishes how many rows it moved in how
    long."""

    def __init__(self, workers, pace=None):
        self.workers = max(1, int(workers))
        self.pace = pace
        self._cv = threading.Condition()
        self._busy = 0

    def _take(self):
        with self._cv:
            while self._busy >= (min(self.workers, self.pace.limit)
                                 if self.pace else self.workers):
                self._cv.wait(0.5)
            self._busy += 1

    def _give(self):
        with self._cv:
            self._busy -= 1
            self._cv.notify_all()

    def each(self, items, fn):
        """fn(item) for every item, as many at a time as there are slots.
        One that fails lets those in flight finish, starts no more, and is
        raised."""
        import concurrent.futures as cf
        import time
        items = list(items)

        def one(item):
            self._take()
            began = time.monotonic()
            try:
                got = fn(item)
            finally:
                self._give()
            if self.pace is not None:
                self.pace.done(_weight(got), time.monotonic() - began)
            return got
        if self.workers < 2 or len(items) < 2:
            # in this thread, and still within the slots: a table of one
            # range copied beside a split one counts against them too
            for item in items:
                one(item)
            return
        failed = None
        pool = cf.ThreadPoolExecutor(min(self.workers, len(items)))
        try:
            futs = [pool.submit(one, item) for item in items]
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


    def each_process(self, items, fn, done):
        """fn(item) in a process of its own for every item, as many at once
        as the move's slots allow, and done(item, result) here as each
        finishes - so what finished is recorded even if a later one fails.
        One that fails starts no more, lets those running finish, and is
        raised.

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


class _Worker:
    """A `python -m migkit.ranges` process that copies range after range:
    each asked as a length and a pickled `(fn, item)` on its input, each
    answered as a length and a pickled `(ok, result or exception)` on its
    output. What it prints goes to a file, for the error if it dies."""

    def __init__(self):
        import subprocess
        import sys
        import tempfile
        self.said = tempfile.TemporaryFile()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "migkit.ranges"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.said)

    def alive(self):
        return self.proc.poll() is None

    def run(self, fn, item):
        import pickle
        import struct
        data = pickle.dumps((fn, item))
        try:
            self.proc.stdin.write(struct.pack(">Q", len(data)) + data)
            self.proc.stdin.flush()
            head = self.proc.stdout.read(8)
            body = (self.proc.stdout.read(struct.unpack(">Q", head)[0])
                    if len(head) == 8 else b"")
            ok, got = pickle.loads(body)
        except Exception:  # noqa: BLE001 - it died before it answered
            self.proc.kill()
            self.proc.wait()
            self.said.seek(0)
            last = self.said.read().decode(errors="replace").strip()
            raise RuntimeError(
                "a copy process stopped without an answer: "
                + (last.splitlines()[-1] if last else
                   f"exit code {self.proc.returncode}"))
        if not ok:
            raise got
        return got

    def close(self):
        try:
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


def plan(st, lo, hi, edges_fn, save):
    """The ranges of a table, planned once and kept in its checkpoint
    entry `st` so a run started again copies the same ranges: [(after,
    upto)] still to copy. Rows the source gained past either end since the
    plan was made get a range of their own; a checkpoint written before
    ranges (`last`, one range at a time) counts what it had done."""
    if "ranges" not in st:
        st["ranges"] = [list(r) for r in split(lo, hi, edges_fn())]
        st["ranges_done"] = [a for a, u in st["ranges"]
                             if "last" in st and u <= st["last"]]
        save()
    rs = st["ranges"]
    if rs and hi > rs[-1][1]:
        rs.append([rs[-1][1], hi])
    if rs and lo - 1 < rs[0][0]:
        rs.insert(0, [lo - 1, rs[0][0]])
    done = set(st["ranges_done"])
    return [tuple(r) for r in rs if r[0] not in done]


def finished(st, after, save):
    """A range copied: marked, and `last` moved to how far every range
    from the start is done."""
    from . import failpoint
    failpoint.hit("range.committed")
    st["ranges_done"].append(after)
    done = set(st["ranges_done"])
    reach = None
    for a, u in st["ranges"]:
        if a not in done:
            break
        reach = u
    if reach is not None:
        st["last"] = reach
    save()
    failpoint.hit("range.saved")


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


def step(rows, chunk, workers):
    """Rows per range: no more than `chunk`, and small enough that the
    workers each get a share of a table, but not below `LEAST`."""
    rows, chunk = max(int(rows or 0), 0), max(int(chunk), 1)
    share = -(-rows // max(workers * 2, 1)) if rows else chunk
    return max(min(chunk, max(share, LEAST)), 1)


def bounds_sql(quote, table, key, every, where=None):
    """The statement answering the key at every `every`-th row in key
    order, in SQL both PostgreSQL and MySQL take: the upper edge of each
    range but the last."""
    k = quote(key)
    return (f"select {k} from (select {k}, row_number() over (order by {k})"
            f" as migkit_rn from {table}"
            + (f" where ({where})" if where else "")
            + f") migkit_keys where migkit_rn % {int(every)} = 0"
            f" order by {k}")


def split(lo, hi, edges):
    """[(after, upto)] covering (lo - 1, hi] with the edges between."""
    cuts = [e for e in sorted(set(int(e) for e in edges)) if lo <= e < hi]
    out, last = [], lo - 1
    for e in cuts:
        out.append((last, e))
        last = e
    out.append((last, hi))
    return out


if __name__ == "__main__":
    _child()

