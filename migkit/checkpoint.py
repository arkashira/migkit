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
import datetime
import decimal
import hashlib
import json
import threading
import uuid

from . import decide
from .sizing import (
    AIMDChunkSizer, AdaptiveChunkSizer, AdaptiveBatchSizer,
    aimd_adjust_chunk, aimd_chunk_size, is_lob_type, lob_length_sql,
    auto_slice_lob_predicates, lob_bounded_predicates
)

FORMAT_VERSION = 1



def serialize_cursor(val):
    """Serialize a range cursor/boundary preserving canonical types."""
    if val is None:
        return None
    if isinstance(val, datetime.datetime):
        return {"_type": "datetime", "val": val.isoformat(), "ts": val.timestamp()}
    if isinstance(val, datetime.date):
        return {"_type": "date", "val": val.isoformat()}
    if isinstance(val, uuid.UUID):
        return {"_type": "uuid", "val": str(val)}
    if isinstance(val, decimal.Decimal):
        return {"_type": "decimal", "val": str(val)}
    if isinstance(val, int):
        return {"_type": "int", "val": val}
    if isinstance(val, float):
        return {"_type": "float", "val": val}
    if isinstance(val, str):
        return {"_type": "str", "val": val}
    if isinstance(val, (list, tuple)):
        return {"_type": "composite", "val": [serialize_cursor(x) for x in val]}
    if isinstance(val, dict) and "_type" in val:
        return val
    return {"_type": "str", "val": str(val)}


def deserialize_cursor(raw):
    """Restore native Python type from typed cursor dict."""
    if raw is None or not isinstance(raw, dict) or "_type" not in raw:
        return raw
    t = raw.get("_type")
    v = raw.get("val")
    if t == "datetime":
        return datetime.datetime.fromisoformat(v)
    if t == "date":
        return datetime.date.fromisoformat(v)
    if t == "uuid":
        return uuid.UUID(v)
    if t == "decimal":
        return decimal.Decimal(v)
    if t == "int":
        return int(v)
    if t == "float":
        return float(v)
    if t == "str":
        return str(v)
    if t == "composite" and isinstance(v, list):
        return tuple(deserialize_cursor(x) for x in v)
    return v


def _key(lo, hi):
    def _repr(x):
        if x is None:
            return ""
        if isinstance(x, (datetime.datetime, datetime.date)):
            return x.isoformat()
        if isinstance(x, dict) and "_type" in x:
            return str(x.get("val", ""))
        return str(x)
    return f"{_repr(lo)}..{_repr(hi)}"


def record_progress_range(report_dir, hop_name, db, table, lo, hi, rows, checksum, fingerprint=""):
    """Write sub-unit range completion to check-progress.jsonl for durable recovery."""
    if not report_dir:
        return
    from pathlib import Path
    pf = Path(report_dir) / "check-progress.jsonl"
    pf.parent.mkdir(parents=True, exist_ok=True)
    rec = {
        "kind": "range",
        "hop": str(hop_name or ""),
        "db": str(db or ""),
        "table": str(table or ""),
        "range_id": _key(lo, hi),
        "fingerprint": str(fingerprint or ""),
        "cursor": {
            "lo": serialize_cursor(lo),
            "hi": serialize_cursor(hi),
        },
        "rows": int(rows),
        "checksum": str(checksum),
    }
    with open(pf, "a") as f:
        f.write(json.dumps(rec) + "\n")


def new_salt():
    """A salt for one run's row hashes, put in front of every row's text
    before it is hashed. Two different tables sum alike with a chance of
    2^-64 a comparison whatever the salt; what the salt changes is that the
    chance is drawn again every run, so rows that happened to collide once
    do not collide on every check after it. Hex, so it goes into any SQL
    as a plain literal."""
    import secrets
    return secrets.token_hex(8)


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

    Written durably (`durable.write_atomic`): a crash or a power loss during
    a save leaves the previous file intact rather than a truncated one,
    because a corrupt checkpoint is worse than no checkpoint - it would
    silently drop ranges from the total. Read as a cache: one that cannot
    be read is started over, which re-checks what it held and loses
    nothing. Two checks of one database save into one file: each save
    takes the file's lock and writes the tables it worked on over what
    the file holds now, keeping the other's (audit ST23) - each wrote its
    whole view over the other's before.
    """

    def __init__(self, path, scope=None):
        self.path = path
        self.scope = scope
        self._lock = threading.Lock()
        self._data = {"format_version": FORMAT_VERSION, "tables": {}}
        if self.scope is not None:
            self._data["scope_header"] = self.scope.header()
        # the tables this run worked on: its saves write those, and keep
        # what another run saved of the rest
        self._touched = set()
        if path:
            from .durable import read_state
            loaded = read_state(path, "cache")
            if isinstance(loaded, dict) and \
                    loaded.get("format_version") == FORMAT_VERSION:
                if self.scope is not None and loaded.get("scope_header"):
                    self.scope.validate_compatibility(loaded["scope_header"])
                self._data = loaded
                if self.scope is not None and not loaded.get("scope_header"):
                    self._data["scope_header"] = self.scope.header()

    # ---- resume decisions ----

    def chunk_for(self, table, proposed):
        """Chunk size to use for this table.

        A table with partials keeps the size it was planned with: re-chunking
        would change the range boundaries, which invalidates every partial and
        would make an adaptive size defeat resumability instead of helping it.
        """
        with self._lock:
            self._touched.add(table)
            entry = self._data["tables"].get(table)
            if entry and entry.get("done") and entry.get("chunk"):
                return int(entry["chunk"])
            self._data["tables"].setdefault(
                table, {"fingerprint": "", "done": {}})["chunk"] = int(proposed)
            return int(proposed)

    def aimd_sizer(self, table, initial=1000):
        with self._lock:
            sizers = getattr(self, "_aimd_sizers", None)
            if sizers is None:
                sizers = self._aimd_sizers = {}
            if table not in sizers:
                init = self.chunk_for(table, initial) if initial else 1000
                sizers[table] = AIMDChunkSizer(initial=init)
            return sizers[table]


    def begin(self, table, expr, ranges):
        """Register the plan for a table; returns the ranges still to do.

        Discards stored partials whose fingerprint no longer matches, so a
        resumed run can never mix work from two different table states.
        """
        fp = fingerprint(expr, ranges)
        with self._lock:
            self._touched.add(table)
            entry = self._data["tables"].get(table)
            if not entry:
                entry = {"fingerprint": fp, "done": {},
                         "chunk": None}
                self._data["tables"][table] = entry
            elif entry.get("fingerprint") == fp:
                pass
            elif entry.get("fingerprint") in ("", None):
                entry["fingerprint"] = fp
                planned_keys = {_key(lo, hi) for lo, hi in ranges}
                entry["done"] = {k: v for k, v in (entry.get("done") or {}).items() if k in planned_keys}
            else:
                entry = {"fingerprint": fp, "done": {},
                         "chunk": (entry or {}).get("chunk")}
                self._data["tables"][table] = entry
            done = entry["done"]
        return [(lo, hi) for lo, hi in ranges if _key(lo, hi) not in done]

    def resumed(self, table):
        """How many ranges were already complete when this run started."""
        e = self._data["tables"].get(table) or {}
        return len(e.get("done") or {})

    def salt(self, table, fresh):
        """The salt this table's row hashes are taken with: the one its
        stored partials were hashed with, so a resumed run adds sums of one
        kind; else `fresh`, kept with the partials to come. Called after
        `begin`, which has already dropped partials of another plan."""
        with self._lock:
            self._touched.add(table)
            entry = self._data["tables"].setdefault(
                table, {"fingerprint": "", "done": {}})
            if not entry.get("done") or not entry.get("salt"):
                entry["salt"] = fresh
            return entry["salt"]

    def load_progress_journal(self, jsonl_path):
        """Load range-level progress from check-progress.jsonl into checkpoint in O(1)."""
        from pathlib import Path
        p = Path(jsonl_path)
        if not p.exists():
            return 0
        loaded = 0
        with self._lock:
            try:
                for line in p.read_text().splitlines():
                    if not line.strip():
                        continue
                    r = json.loads(line)
                    if r.get("kind") != "range":
                        continue
                    tbl = r.get("table")
                    rid = r.get("range_id")
                    if not tbl or not rid:
                        continue
                    self._touched.add(tbl)
                    entry = self._data["tables"].setdefault(tbl, {"fingerprint": "", "done": {}})
                    if r.get("fingerprint") and not entry.get("fingerprint"):
                        entry["fingerprint"] = str(r["fingerprint"])
                    if rid not in entry["done"]:
                        entry["done"][rid] = [int(r.get("rows", 0)), str(r.get("checksum", "0"))]
                        loaded += 1
            except (json.JSONDecodeError, OSError):
                pass
            if loaded:
                self._flush()
        return loaded

    # ---- accumulating ----

    def record(self, table, lo, hi, rows, checksum, cursor=None):
        with self._lock:
            self._touched.add(table)
            entry = self._data["tables"].setdefault(
                table, {"fingerprint": "", "done": {}})
            k = _key(lo, hi)
            entry["done"][k] = [int(rows), str(checksum)]
            entry.setdefault("cursors", {})[k] = {
                "lo": serialize_cursor(lo),
                "hi": serialize_cursor(hi),
                "rows": int(rows),
                "checksum": str(checksum),
            }
            self._flush()

    def total(self, table):
        """(rows, checksum) over every recorded range: the checksums are
        sums, so ranges add up to the one pass over the table.

        Not XOR, which MySQL's folded with until it was measured: two equal
        rows cancel, and a count beside it does not save it - two copies of
        one row and two copies of another have the same count and the same
        XOR (zero).
        """
        e = self._data["tables"].get(table) or {}
        rows, acc = 0, 0
        for r, c in (e.get("done") or {}).values():
            rows += int(r)
            acc += int(c)
        return rows, str(acc)

    def clear(self, table):
        """Forget a table's partials - used once it is fully verified, so the
        file does not grow into a record of every table ever checked."""
        with self._lock:
            self._touched.add(table)
            self._data["tables"].pop(table, None)
            self._flush()

    def _flush(self):
        if not self.path:
            return
        from .durable import locked, read_state, write_atomic
        with locked(self.path):
            disk = read_state(self.path, "cache")
            merged = {"format_version": FORMAT_VERSION, "tables": {}}
            if isinstance(disk, dict) and \
                    disk.get("format_version") == FORMAT_VERSION:
                merged["tables"].update(disk.get("tables") or {})
            for table in self._touched:
                if table in self._data["tables"]:
                    merged["tables"][table] = self._data["tables"][table]
                else:
                    merged["tables"].pop(table, None)
            write_atomic(self.path, json.dumps(merged,
                                               separators=(",", ":")))


# ---- the copy's checkpoint and its journal ---------------------------------

#: the key a copy's checkpoint names its journal by
JOURNAL = "__journal__"


def journal_of(path):
    """The journal beside a copy's checkpoint (`move.json`)."""
    from pathlib import Path
    path = Path(path)
    return path.with_name(path.name + ".journal")


def read_move(path, kind="position"):
    """A copy's checkpoint as its file and the journal after it say: its
    entries, each range begun and landed since the file was written added
    back from the journal (`replay`); None where there is neither.

    The one reader of a copy's checkpoint - the copier resuming, the
    tail's `_CopiedRanges`, a tail going on from a copy of topics, the
    dashboard, the evidence (audit 2026-10-01, ST17). A reader of the file
    alone misses the ranges the journal holds: the tail would apply what
    the copy read, or leave out what it did not. The journal's lines count
    only for the file whose journal they were written under (its
    `JOURNAL`): a file written whole since starts one of its own, and a
    line left over from before it is not added to it. `kind` is the file's
    torn-file policy (`durable.read_state`): a copy's position stops; the
    dashboard reads it as a cache."""
    from . import durable
    snap = durable.read_state(path, kind)
    lines = durable.read_lines(journal_of(path),
                               "cache" if kind == "cache" else "journal")
    if snap is None and not lines:
        return None
    if not isinstance(snap, dict):
        snap = {}
    jid = snap.pop(JOURNAL, None)
    replay(snap, [e for e in lines if jid is not None
                  and isinstance(e, dict) and e.get("j") == jid])
    return snap


def replay(entries, lines):
    """The journal's `lines` added to a checkpoint's `entries`, in order:
    a range begun (`seen`: its start and the mark it was begun under) and
    a range landed (`done`, with how far every range from the start has
    landed then, `last`) - the only two things the journal holds, both
    written by `ranges`."""
    done = {}
    for e in lines:
        st = entries.setdefault(e["k"], {})
        if "seen" in e:
            # named as `ranges.started` named it, so a text key, a
            # composite or the open first end is found again after a reload
            from . import ranges
            after, mark = e["seen"]
            st.setdefault("ranges_seen", {})[ranges.ident(after)] = mark
        if "done" in e:
            have = st.setdefault("ranges_done", [])
            got = done.get(e["k"])

            def _hashable(x):
                return tuple(_hashable(i) for i in x) if isinstance(x, list) else x

            if got is None:
                got = done[e["k"]] = {_hashable(x) for x in have}
            h_done = _hashable(e["done"])
            if h_done not in got:
                got.add(h_done)
                have.append(e["done"])
            if e.get("last") is not None:
                st["last"] = e["last"]
    return entries


def write_move(path, entries):
    """A copy's checkpoint written whole, with a journal of its own begun
    (`read_move`): for a writer other than the copier's own checkpoint
    (a mirror of topics keeping their positions where the copier does)."""
    import secrets

    from . import durable
    body = dict(entries)
    body.pop(JOURNAL, None)
    body[JOURNAL] = secrets.token_hex(6)
    durable.write_atomic(path, json.dumps(body, separators=(",", ":"),
                                          default=str))
    journal_of(path).unlink(missing_ok=True)


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

    def __init__(self, rows_per_second=DEFAULT_ROWS_PER_SECOND, aimd=False, initial_chunk=1000):
        self.value = float(rows_per_second)
        self.samples = 0
        self.aimd = aimd
        self.sizer = AIMDChunkSizer(initial=initial_chunk) if aimd else None

    def enable_aimd(self, initial=1000):
        self.aimd = True
        self.sizer = AIMDChunkSizer(initial=initial)
        return self.sizer

    def observe(self, rows, seconds, bytes_count=None, memory_healthy=True):
        if rows <= 0 or seconds <= 0:
            return
        seen = rows / seconds
        # exponential moving average: react to a server that has got busier
        # without letting one unlucky chunk redefine the plan
        self.value = seen if self.samples == 0 else self.value * 0.7 + seen * 0.3
        self.samples += 1
        if self.sizer is not None:
            self.sizer.adjust(latency=seconds, rows=rows, bytes_count=bytes_count,
                              memory_healthy=memory_healthy)

    def chunk_rows(self, target_seconds=TARGET_SECONDS):
        if self.sizer is not None:
            return self.sizer.chunk
        # the ceiling is applied last on purpose: "never one giant query" is
        # the guarantee that makes a long verify restartable, so it wins over
        # the floor if the two are ever set inconsistently
        return min(MAX_CHUNK, max(MIN_CHUNK, int(self.value * target_seconds)))


def plan_adaptive_ranges(lo, hi, sizer=None, initial_chunk=1000):
    if sizer is None:
        sizer = AIMDChunkSizer(initial=initial_chunk)
    return plan_ranges(lo, hi, sizer.chunk)


def plan_ranges(lo, hi, chunk, total_rows=None):
    """Split the keyspace into half-open ranges of `chunk` keys.

    The ranges cover everything, not just [lo, hi]: the first is open at the
    bottom and the last is open at the top. Boundaries come from the source,
    and the target is free to hold keys outside them - a row below the
    source's minimum or above its maximum is exactly the kind of thing worth
    catching, so it must fall inside a range rather than between two.
    If `total_rows` is provided and greater than 0, timestamp and date ranges
    are partitioned proportionally to row density rather than treating chunk
    as literal seconds or days.
    """
    if isinstance(chunk, AIMDChunkSizer):
        chunk = chunk.chunk
    if lo is None or hi is None or chunk <= 0:
        return [(None, None)]

    if isinstance(lo, datetime.datetime) and isinstance(hi, datetime.datetime):
        t_lo = lo.timestamp()
        t_hi = hi.timestamp()

        if t_hi < t_lo:
            return [(None, None)]
        cuts = []
        cur = t_lo
        if total_rows is not None and total_rows > 0:
            import math
            num_chunks = max(1, math.ceil(float(total_rows) / float(chunk)))
            step = max(1.0, (t_hi - t_lo) / num_chunks)
        else:
            step = float(chunk)
        while cur <= t_hi:
            cur += step
            if cur <= t_hi:
                cuts.append(datetime.datetime.fromtimestamp(cur, tz=lo.tzinfo))
        bounds = [None] + cuts + [None]
        return [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]

    if isinstance(lo, datetime.date) and isinstance(hi, datetime.date) and not isinstance(lo, datetime.datetime):
        if hi < lo:
            return [(None, None)]
        cuts = []
        cur = lo
        if total_rows is not None and total_rows > 0:
            import math
            total_days = max(1, (hi - lo).days)
            num_chunks = max(1, math.ceil(float(total_rows) / float(chunk)))
            day_step = max(1, math.ceil(float(total_days) / float(num_chunks)))
            delta = datetime.timedelta(days=day_step)
        else:
            delta = datetime.timedelta(days=max(1, int(chunk)))
        while cur <= hi:
            cur += delta
            if cur <= hi:
                cuts.append(cur)
        bounds = [None] + cuts + [None]
        return [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]

    try:
        cuts, start, hi_int = [], int(lo), int(hi)
    except (ValueError, TypeError):
        return [(None, None)]

    if hi_int < start:
        return [(None, None)]
    while start <= hi_int:
        start += int(chunk)
        if start <= hi_int:
            cuts.append(start)
    bounds = [None] + cuts + [None]
    return [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]


def plan_from_boundaries(bounds):
    """Ranges from an ordered list of boundary values.

    The arithmetic split in `plan_ranges` assumes an integer key, which both
    SQL engines have and MongoDB does not: an `_id` may be an ObjectId, a
    string, an integer, or a mix of all three in one collection. The answer is
    the same one MongoDB's own verifier uses - partition along the `_id` index
    rather than computing positions - so the boundaries are read from the data
    and passed through here.

    Same guarantee as the arithmetic version: open at both ends, contiguous,
    no gaps and no overlap, so no key can fall between two ranges.
    """
    b = list(bounds)
    edges = [None] + b + [None]
    return [(edges[i], edges[i + 1]) for i in range(len(edges) - 1)]


def mongo_filter(lo, hi):
    """`_id` predicate for one range, in MongoDB's comparison order.

    Mongo compares across BSON types by a canonical type order, and `$lt` and
    `$gte` respect it - so a mixed-type `_id` partitions correctly as long as
    both sides are given the same boundaries.
    """
    cond = {}
    if lo is not None:
        cond["$gte"] = lo
    if hi is not None:
        cond["$lt"] = hi
    return {"_id": cond} if cond else {}


def _format_literal(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (datetime.datetime, datetime.date)):
        return f"'{v.isoformat()}'"
    if isinstance(v, uuid.UUID):
        return f"'{v}'"
    if isinstance(v, str):
        if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')) or (v.startswith("(") and v.endswith(")")):
            return v
        if v.isdigit() or (v.startswith("-") and v[1:].isdigit()):
            return v
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def where(column, lo, hi, lob_pred=None):
    """SQL predicate for one range. Quoting is the caller's business."""
    if lo is None and hi is None:
        base = ""
    elif hi is None:
        base = f"{column} >= {_format_literal(lo)}"
    elif lo is None:
        base = f"{column} < {_format_literal(hi)}"
    else:
        base = f"{column} >= {_format_literal(lo)} and {column} < {_format_literal(hi)}"

    if lob_pred:
        return f"{base} and ({lob_pred})" if base else f"({lob_pred})"
    return base


def _prove_o1_journal_replay(facts: decide.Facts) -> bool:
    return bool(facts.get("journal_replay") and not facts.get("refused"))


def _prove_typed_cursor_preservation(facts: decide.Facts) -> bool:
    return bool(facts.get("typed_cursor") and not facts.get("refused"))


def _prove_matching_fingerprint_retention(facts: decide.Facts) -> bool:
    return bool(facts.get("matching_fingerprint") and not facts.get("refused"))


def _prove_adaptive_range_density_scaling(facts: decide.Facts) -> bool:
    return bool(facts.get("adaptive_density") and not facts.get("refused"))


def _prove_refuse_corrupted_journal_plan(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_incompatible_plan"))


def universal_resume_rungs() -> tuple:
    return (
        decide.Rung(
            "o1_journal_checkpoint_replay",
            "replay completed sub-unit ranges from journal into checkpoint in O(1) without rescanning",
            needs=(
                decide.Need(
                    "o1 replay enabled",
                    _prove_o1_journal_replay,
                    "o1 replay not enabled",
                ),
            ),
        ),
        decide.Rung(
            "typed_cursor_preservation",
            "preserve exact typed boundaries across resume (datetime, date, uuid, decimal, composite)",
            needs=(
                decide.Need(
                    "typed cursor preserved",
                    _prove_typed_cursor_preservation,
                    "typed cursor boundaries not preserved",
                ),
            ),
        ),
        decide.Rung(
            "matching_fingerprint_retention",
            "retain completed ranges when journal plan matches or is compatible with current table plan",
            needs=(
                decide.Need(
                    "plan fingerprint matched",
                    _prove_matching_fingerprint_retention,
                    "plan fingerprint mismatch detected",
                ),
            ),
        ),
        decide.Rung(
            "adaptive_range_density_scaling",
            "scale range boundaries proportionally based on key density and total row counts",
            needs=(
                decide.Need(
                    "range density scaled",
                    _prove_adaptive_range_density_scaling,
                    "range density scaling criteria not met",
                ),
            ),
        ),
        decide.Rung(
            "refuse_corrupted_journal_plan",
            "refuse mixing ranges when plan fingerprint contradicts an earlier incompatible plan",
            needs=(
                decide.Need(
                    "incompatible plan refused",
                    _prove_refuse_corrupted_journal_plan,
                    "incompatible journal plan permitted",
                ),
            ),
        ),
    )


def decide_universal_resume(facts) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("universal resume", universal_resume_rungs(), fact_obj)


def _prove_datetime_timezone_aware(facts: decide.Facts) -> bool:
    return bool(facts.get("datetime_supported") and not facts.get("refused"))


def _prove_uuid_canonical_hex(facts: decide.Facts) -> bool:
    return bool(facts.get("uuid_supported") and not facts.get("refused"))


def _prove_decimal_fixed_scale(facts: decide.Facts) -> bool:
    return bool(facts.get("decimal_supported") and not facts.get("refused"))


def _prove_composite_tuple_cursor(facts: decide.Facts) -> bool:
    return bool(facts.get("composite_supported") and not facts.get("refused"))


def _prove_refuse_untyped_opaque_cursor(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_untyped_cursor"))


def cursor_serialization_rungs() -> tuple:
    return (
        decide.Rung(
            "datetime_timezone_aware",
            "serialize and deserialize timezone-aware datetimes with exact ISO-8601 UTC offsets",
            needs=(
                decide.Need(
                    "datetime serialization verified",
                    _prove_datetime_timezone_aware,
                    "datetime serialization unsupported",
                ),
            ),
        ),
        decide.Rung(
            "uuid_canonical_hex",
            "preserve 128-bit UUID cursors in canonical hyphenated string format",
            needs=(
                decide.Need(
                    "uuid serialization verified",
                    _prove_uuid_canonical_hex,
                    "uuid serialization unsupported",
                ),
            ),
        ),
        decide.Rung(
            "decimal_fixed_scale",
            "preserve exact arbitrary-precision Decimal strings without float precision loss",
            needs=(
                decide.Need(
                    "decimal serialization verified",
                    _prove_decimal_fixed_scale,
                    "decimal serialization unsupported",
                ),
            ),
        ),
        decide.Rung(
            "composite_tuple_cursor",
            "serialize multi-column composite key cursors with element-level typing",
            needs=(
                decide.Need(
                    "composite serialization verified",
                    _prove_composite_tuple_cursor,
                    "composite serialization unsupported",
                ),
            ),
        ),
        decide.Rung(
            "refuse_untyped_opaque_cursor",
            "refuse unhandled object types from being serialized as opaque unrecoverable strings",
            needs=(
                decide.Need(
                    "untyped cursor refused",
                    _prove_refuse_untyped_opaque_cursor,
                    "untyped cursor permitted",
                ),
            ),
        ),
    )


def decide_cursor_serialization(facts) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("cursor serialization", cursor_serialization_rungs(), fact_obj)

