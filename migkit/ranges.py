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
    """How many ranges the whole move copies at once."""

    def __init__(self, workers):
        self.workers = max(1, int(workers))
        self._sem = threading.BoundedSemaphore(self.workers)

    def each(self, items, fn):
        """fn(item) for every item, as many at a time as there are slots.
        One that fails lets those in flight finish, starts no more, and is
        raised."""
        import concurrent.futures as cf
        items = list(items)

        def one(item):
            with self._sem:
                fn(item)
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
