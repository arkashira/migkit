"""Make a long verify restartable.

A 488-million-row table is one checksum query. It is fast - a parallel
aggregate, no sort - but it is also all or nothing: lose the connection at 90%
and the next run starts from zero. Vitess hit this in VDiff v1 and fixed it in
v2 by streaming with a `LastPK` and checkpointing per table; Percona's
pt-table-checksum has `--resume` for the same reason.

migkit's checksum happens to be the easy case. It is a commutative sum of
per-row md5 values accumulated as `numeric`, so:

    checksum(whole table) == sum(checksum(range) for range in any partition)

Partial progress is therefore *meaningful* rather than just a position marker.
Each completed primary-key range can be stored with its own (rows, sum), and a
resumed run adds the ranges it still owes. Order does not matter, which means
ranges can also be done in parallel and out of sequence.

The correctness hazard is resuming into a table that has changed underneath
the stored partials: adding today's range sums to yesterday's gives a total
that matches neither side. Every table therefore carries a fingerprint of the
things that would invalidate its partials - the checksum expression and the
range boundaries - and a mismatch discards the partials instead of trusting
them.
"""
import hashlib
import json
import os
import tempfile
import threading

FORMAT_VERSION = 1


def _key(lo, hi):
    return f"{'' if lo is None else lo}..{'' if hi is None else hi}"


def fingerprint(expr, ranges):
    """Identity of a table's partial work.

    Includes the range boundaries: re-chunking a table differently makes the
    old partials unusable, because they cover different rows.
    """
    payload = json.dumps([expr, [[_key(a, b)] for a, b in ranges]],
                         separators=(",", ":"), sort_keys=True,
                         default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class Checkpoint:
    """Per-database record of completed ranges, safe to reopen and resume.

    Written atomically: a crash during a save leaves the previous file intact
    rather than a truncated one, because a corrupt checkpoint is worse than no
    checkpoint - it would silently drop ranges from the total.
    """

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self._data = {"format_version": FORMAT_VERSION, "tables": {}}
        if path and os.path.exists(path):
            try:
                loaded = json.loads(open(path).read())
                if loaded.get("format_version") == FORMAT_VERSION:
                    self._data = loaded
            except (ValueError, OSError):
                pass   # unreadable checkpoint = start over, never crash

    # ---- resume decisions ----

    def chunk_for(self, table, proposed):
        """Chunk size to use for this table.

        A table with partials keeps the size it was planned with: re-chunking
        would change the range boundaries, which invalidates every partial and
        would make an adaptive size defeat resumability instead of helping it.
        """
        with self._lock:
            entry = self._data["tables"].get(table)
            if entry and entry.get("done") and entry.get("chunk"):
                return int(entry["chunk"])
            self._data["tables"].setdefault(
                table, {"fingerprint": "", "done": {}})["chunk"] = int(proposed)
            return int(proposed)

    def begin(self, table, expr, ranges):
        """Register the plan for a table; returns the ranges still to do.

        Discards stored partials whose fingerprint no longer matches, so a
        resumed run can never mix work from two different table states.
        """
        fp = fingerprint(expr, ranges)
        with self._lock:
            entry = self._data["tables"].get(table)
            if not entry or entry.get("fingerprint") != fp:
                entry = {"fingerprint": fp, "done": {},
                         "chunk": (entry or {}).get("chunk")}
                self._data["tables"][table] = entry
            done = entry["done"]
        return [(lo, hi) for lo, hi in ranges if _key(lo, hi) not in done]

    def resumed(self, table):
        """How many ranges were already complete when this run started."""
        e = self._data["tables"].get(table) or {}
        return len(e.get("done") or {})

    # ---- accumulating ----

    def record(self, table, lo, hi, rows, checksum):
        with self._lock:
            entry = self._data["tables"].setdefault(
                table, {"fingerprint": "", "done": {}})
            entry["done"][_key(lo, hi)] = [int(rows), str(checksum)]
            self._flush()

    def total(self, table, combine="sum"):
        """(rows, checksum) over every recorded range.

        `combine` names how the engine's checksum composes. Both operations
        in use are commutative and associative, which is the property that
        makes chunking equivalent to one pass:

          sum  PostgreSQL adds per-row md5 values as numeric
          xor  MySQL folds them with BIT_XOR

        XOR has one wrinkle worth knowing: two identical rows cancel. That
        cannot hide a difference here because row counts are compared
        alongside the checksum, and a cancelled pair changes the count.
        """
        e = self._data["tables"].get(table) or {}
        rows, acc = 0, 0
        for r, c in (e.get("done") or {}).values():
            rows += int(r)
            acc = acc + int(c) if combine == "sum" else acc ^ int(c)
        return rows, str(acc)

    def clear(self, table):
        """Forget a table's partials - used once it is fully verified, so the
        file does not grow into a record of every table ever checked."""
        with self._lock:
            self._data["tables"].pop(table, None)
            self._flush()

    def _flush(self):
        if not self.path:
            return
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=d or ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(self._data, f, separators=(",", ":"))
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise


# What one chunk should take. Long enough that per-query overhead is noise,
# short enough that a crash loses little and a cancel is responsive. Percona's
# pt-table-checksum sizes chunks to a target runtime for the same reasons.
TARGET_SECONDS = 10.0
# Bounds on the learned size. The floor stops a slow table from being split
# into millions of tiny queries; the ceiling stops a fast one from becoming a
# single query again, which is what made a long verify unrestartable.
MIN_CHUNK, MAX_CHUNK = 50_000, 20_000_000
# Assumed rate before anything has been measured. Deliberately low: guessing
# small costs a few extra round trips, guessing large costs a ten-minute query
# against a database someone is using.
DEFAULT_ROWS_PER_SECOND = 200_000


class Rate:
    """Rows per second, learned from the chunks that have already run.

    Kept per run rather than persisted: the number describes the machine, the
    network and how busy the server is right now, none of which survive until
    the next run in a form worth trusting.
    """

    def __init__(self, rows_per_second=DEFAULT_ROWS_PER_SECOND):
        self.value = float(rows_per_second)
        self.samples = 0

    def observe(self, rows, seconds):
        if rows <= 0 or seconds <= 0:
            return
        seen = rows / seconds
        # exponential moving average: react to a server that has got busier
        # without letting one unlucky chunk redefine the plan
        self.value = seen if self.samples == 0 else self.value * 0.7 + seen * 0.3
        self.samples += 1

    def chunk_rows(self, target_seconds=TARGET_SECONDS):
        # the ceiling is applied last on purpose: "never one giant query" is
        # the guarantee that makes a long verify restartable, so it wins over
        # the floor if the two are ever set inconsistently
        return min(MAX_CHUNK, max(MIN_CHUNK, int(self.value * target_seconds)))


def plan_ranges(lo, hi, chunk):
    """Split the keyspace into half-open ranges of `chunk` keys.

    The ranges cover everything, not just [lo, hi]: the first is open at the
    bottom and the last is open at the top. Boundaries come from the source,
    and the target is free to hold keys outside them - a row below the
    source's minimum or above its maximum is exactly the kind of thing worth
    catching, so it must fall inside a range rather than between two.
    """
    if lo is None or hi is None or chunk <= 0 or hi < lo:
        return [(None, None)]
    cuts, start, hi = [], int(lo), int(hi)
    while start <= hi:
        start += int(chunk)
        if start <= hi:
            cuts.append(start)
    bounds = [None] + cuts + [None]
    return [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]


def where(column, lo, hi):
    """SQL predicate for one range. Quoting is the caller's business."""
    if lo is None and hi is None:
        return ""
    if hi is None:
        return f"{column} >= {lo}"
    if lo is None:
        return f"{column} < {hi}"
    return f"{column} >= {lo} and {column} < {hi}"
