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
from dataclasses import dataclass, field

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
        # and only where this machine's memory holds one more worker
        # (`sizing.Memory`): one of the widest rows waits for room, where it
        # was started beside the others and the move was killed for it
        with self._cv:
            while self._busy >= (min(self.workers, self.pace.limit)
                                 if self.pace else self.workers) or (
                    self.pace is not None
                    and not self.pace.admit(self._busy)):
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


def plan(st, lo, hi, edges_fn, save, table=None):
    """The ranges of a table, planned once and kept in its checkpoint
    entry `st` so a run started again copies the same ranges: [(after,
    upto)] still to copy. Rows the source gained past either end since the
    plan was made get a range of their own; a checkpoint written before
    ranges (`last`, one range at a time) counts what it had done.

    `table` is the table as the source's change log names it, kept with
    the process copying it, for the tail (`already_read`)."""
    import os
    import socket
    if table is not None:
        st["table"] = table
    st["copying"] = {"host": socket.gethostname(), "pid": os.getpid()}
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


def started(st, after, mark, save):
    """A range about to be read: the source's mark taken just before it
    (`Engine.snapshot_mark`; None where it gave none), saved before the
    read begins - so a range with nothing kept has not been read."""
    st.setdefault("ranges_seen", {})[str(after)] = mark
    save()


def already_read(st, values, covers, ahead=False):
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
    copied again some other way is not what this entry says."""
    import bisect
    rs = st.get("ranges")
    if not rs or not values:
        return False
    seen = st.get("ranges_seen") or {}
    for v in values:
        if not isinstance(v, int) or isinstance(v, bool):
            return False
        # asked for every change the tail reads: found by halves, the
        # plan kept in key order
        i = bisect.bisect_left(rs, v, key=lambda r: r[0]) - 1
        if i < 0 or v > rs[i][1]:
            return False
        at = str(rs[i][0])
        if at in seen:
            if seen[at] is None or covers(seen[at]) is not True:
                return False
        elif not ahead or st.get("done") \
                or rs[i][0] in (st.get("ranges_done") or ()):
            return False
    return True


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


def bucket_bits(rows):
    """Bits of the row hash a table's buckets are cut by: about
    `BUCKET_ROWS` a bucket, at least two buckets."""
    n = max(2, -(-int(rows or 0) // BUCKET_ROWS))
    return min(MOST_BUCKET_BITS, (n - 1).bit_length())


def _keyless_parts():
    from . import decide
    R, N = decide.Rung, decide.Need
    large = N("a table large enough to repay a scan a bucket",
              lambda f: (f.get("rows") or 0) >= 2 * BUCKET_ROWS,
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

