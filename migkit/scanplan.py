"""The aggregates checks read of a table, planned as one set of reads
(backlog P0-VERIFY-EXECUTION).

The deep checks read a table once for every column and every check, on
each side in turn: NULL against empty text, the float drift, the
replacement characters - a wide table of a billion rows read once per
text column, then again per float column, while the other side waited.
Here every check hands its aggregates (`Aggregate`: SQL that folds the
whole table into a value) to one plan, and the plan reads each table:

* the same expression asked by two checks is read once;
* a table's aggregates are fused into one read a side - bounded, one row
  back, at most `MOST_EXPRS` expressions a read - or one read a key range
  where the key cuts the table into ranges that cover it (`covering`);
  where a fused read fails, each aggregate is read on its own, so a
  failing column is named alone and the others still count. Which of
  these runs is a climb of `READS` (`decide.attempt`), ranked by what
  each was measured to cost end to end, seconds a row read, with "every
  row" the need: no way of reading less than the whole table is on the
  ladder, and none can be picked silently;
* both sides and every table are read side by side under the run's own
  slots (`ranges.current_coordinator`, the check's `ReadPool`), borrowed
  rather than multiplied: nothing here opens a pool of its own;
* a value read where both sides' change markers prove the table has not
  moved since (`Evidence`, `unchanged.unchanged`) is not read again, and
  never where the proof goes into a cutover.

A read that fails stays failed (`Got.failed`): never a value of zero.
"""
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from . import decide

#: the most expressions one read carries: a fused read past it is cut
#: into reads of this many, each still one row back
MOST_EXPRS = 512

#: how each value folds across the ranges a table is read in
FOLDS = {"sum": lambda a, b: a + b, "max": max, "min": min}


@dataclass(frozen=True)
class Aggregate:
    """One check's question of one table: `exprs` fold the whole table
    into values, each folded across ranges by its `folds` entry ("sum"
    where none is said)."""
    check: str
    table: str
    name: str
    exprs: tuple
    folds: tuple = ()

    def fold(self, expr):
        i = self.exprs.index(expr)
        return self.folds[i] if i < len(self.folds) else "sum"


_N, _R = decide.Need, decide.Rung
_SIDES = {"source": 1, "target": 1}

READS = (
    _R("ranges", "every aggregate of the table in one read a key range,"
                 " the ranges side by side",
       gives=frozenset({"every row", "one read", "side by side"}),
       needs=(_N("the key cut into ranges that cover the table",
                 lambda f: (f.get("ranges") or 1) > 1,
                 "the table has no key cut into ranges that cover it"),),
       because="one read carries every check of the table, and the ranges"
               " keep both servers busy at once", loads=_SIDES),
    _R("fused", "every aggregate of the table in one read a side",
       gives=frozenset({"every row", "one read"}),
       because="one read carries every check of the table", loads=_SIDES),
    _R("apart", "each aggregate in a read of its own",
       gives=frozenset({"every row"}),
       needs=(_N("a fused read failed here",
                 lambda f: bool(f.get("a fused read failed")),
                 "no fused read of the table failed, and {rung} reads it"
                 " once for every aggregate"),),
       because="a column a read fails on is named alone, and the others"
               " still count", loads={"source": 2, "target": 2}),
)

#: what any way of reading must give: the whole table, never a sample
NEED = ("every row",)


def covering(parts):
    """`parts` ((lo, hi), ...) where they cover the whole key space - open
    at both ends and each starting where the one before ended - else the
    one range of the whole table."""
    parts = list(parts or ())
    if (len(parts) > 1 and parts[0][0] is None and parts[-1][1] is None
            and all(a[1] is not None and a[1] == b[0]
                    for a, b in zip(parts, parts[1:]))):
        return parts
    return [(None, None)]


class Got:
    """What the plan read: each value by (side, table, expr), each failure
    by (table, expr) with its exception, the rows each side holds, and
    the way each table was read."""

    def __init__(self, aggregates):
        self.aggregates = list(aggregates)
        self.values, self.failed, self.rows = {}, {}, {}
        self.ways, self.reused = {}, 0

    def of(self, check):
        return [a for a in self.aggregates if a.check == check]

    def value(self, side, agg):
        """The aggregate's folded values on `side`, or None where a read
        of it failed or never happened."""
        got = tuple(self.values.get((side, agg.table, e)) for e in agg.exprs)
        return None if None in got else got

    def failure(self, agg):
        """The exception a read of `agg` failed with, or None."""
        for e in agg.exprs:
            if (agg.table, e) in self.failed:
                return self.failed[(agg.table, e)]
        return None

    def failures(self, check):
        return {f"{a.table}.{a.name}": self.failure(a)
                for a in self.of(check) if self.failure(a) is not None}


class Evidence:
    """The values read where both sides carried a change marker, kept with
    the markers: reused only while both markers are exactly as they were
    and the scope is the one they were read under."""

    def __init__(self, path):
        self.path, self.data = path, {}
        try:
            got = json.loads(path.read_text()) if path and path.exists() \
                else {}
            self.data = got if isinstance(got, dict) else {}
        except (OSError, ValueError):
            # an unreadable record proves nothing
            self.data = {}

    def reusable(self, table, markers, scope):
        from . import unchanged
        kept = self.data.get(table)
        if not isinstance(kept, dict) or kept.get("scope") != scope:
            return {}
        if not all(unchanged.unchanged((kept.get("markers") or {}).get(s),
                                       markers.get(s)) for s in markers):
            return {}
        return kept.get("values") or {}

    def keep(self, table, markers, scope, values):
        if not all(markers.values()):
            self.data.pop(table, None)
            return
        self.data[table] = {"markers": markers, "scope": scope,
                            "values": values}

    def save(self):
        from . import durable
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        durable.write_atomic(self.path, json.dumps(self.data, sort_keys=True,
                                                   default=str))


def _chunks(exprs, most=MOST_EXPRS):
    return [tuple(exprs[i:i + most]) for i in range(0, len(exprs), most)]


def _number(v):
    """A value as a number JSON keeps: a whole decimal as an integer, so a
    sum of lengths stays exact; any other as itself or a float."""
    import decimal
    if v is None:
        return 0
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return v
    if isinstance(v, decimal.Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    try:
        return int(v)
    except (TypeError, ValueError):
        return float(v)


def _slots(eng, db, hop, units):
    """Slots for `units` reads under the run's own budget: a child of the
    slots the caller runs under where there are some, else slots paced as
    a move's are (`sizing`)."""
    from . import ranges, sizing
    try:
        plan = sizing.estimate(hop, eng, db, units=units)
    except Exception:  # noqa: BLE001 - no estimate reads one at a time
        plan = sizing.Plan(1, 1, {})
    parent = ranges.current_coordinator()
    if parent is not None:
        return ranges.Slots(plan.most, parent=parent)
    try:
        held = sizing.resources(eng, db, hop)
    except Exception:  # noqa: BLE001 - unknown resources hold nothing back
        held = {}
    return ranges.Slots(plan.most, pace=sizing.Pace(
        min(plan.start, plan.most), plan.most, resources=held,
        memory=sizing.Memory(sizing.WORKER_MEMORY)))


def together(items, fn, most):
    """[fn(item) for item in items], in that order, as many at once as
    `most` and the run's own slots allow: a child of the slots the caller
    runs under where there are some (`ranges.current_coordinator`),
    borrowed rather than multiplied; else slots of `most` of their own,
    which reads nested in these do not count against: with no run's
    budget to share, a side's reads are not held to the two sides'.
    The first failure is raised once those in flight have finished."""
    from . import ranges
    items = list(items)
    got = [None] * len(items)
    parent = ranges.current_coordinator()
    slots = ranges.Slots(max(1, min(int(most or 1), len(items) or 1)),
                         parent=parent)

    def one(i):
        if parent is not None:
            got[i] = fn(items[i])
            return
        token = ranges._current_coordinator_var.set(None)
        try:
            got[i] = fn(items[i])
        finally:
            ranges._current_coordinator_var.reset(token)
    slots.each(range(len(items)), one)
    return got


def cuts(rows, most):
    """How many key ranges a table of `rows` rows is read in: no more than
    the plan reads at once (`most`), and none holding fewer than a range
    of a copy does (`ranges.LEAST`); one where its rows are not known."""
    from . import ranges
    try:
        rows = int(rows)
    except (TypeError, ValueError):
        return 1
    return max(1, min(int(most or 1), rows // ranges.LEAST))


def spread(points, n):
    """`n - 1` of the ascending `points` evenly apart, each once: the
    bounds that cut what the points sample into `n` ranges of near equal
    rows."""
    points = list(points)
    if n < 2 or not points:
        return []
    picked = [points[min(len(points) - 1, len(points) * i // n)]
              for i in range(1, n)]
    return list(dict.fromkeys(picked))


def bounded(column, bounds, nullable=True):
    """The predicates of the ranges of `column` cut at the ascending
    `bounds` (SQL literals), open at both ends and each starting where the
    one before ended (`covering`): every row in exactly one of them on
    either side, whatever each side holds. The first also takes a key
    that is NULL where `nullable`, which no range of values holds. [""],
    the whole table, where there is nothing to cut at."""
    from . import checkpoint
    bounds = list(dict.fromkeys(bounds))
    if not bounds:
        return [""]
    edges = [None] + bounds + [None]
    preds = [checkpoint.where(column, lo, hi)
             for lo, hi in covering(list(zip(edges, edges[1:])))]
    if nullable and preds[0]:
        preds[0] = f"({preds[0]} or {column} is null)"
    return preds


def plan_most(eng, db):
    """How many reads the run's plan holds at once for `db`
    (`sizing.estimate`): the most key ranges a table is cut into."""
    from . import sizing
    try:
        return max(1, int(sizing.estimate(eng.hop, eng, db).most))
    except Exception:  # noqa: BLE001 - no plan reads the table whole
        return 1


def for_engine(eng, db, aggregates, *, source, query, parts=None,
               progress="deep-scan-progress.json", count="count(*)"):
    """`run` as an engine's deep checks call it: the engine's change
    markers (`_change_marker`) and cutover hold, the hop's scope and its
    kept costs, the progress and the evidence under the database's report
    directory. Where the caller cuts no ranges and the engine says how its
    tables are cut (`scan_cuts(db, table, most)`, from its own statistics),
    each table is cut into at most as many ranges as the plan reads at
    once (`plan_most`); a table the engine cannot cut is read whole."""
    from . import config
    hop = eng.hop
    cutter = getattr(eng, "scan_cuts", None)
    if parts is None and callable(cutter):
        most, cut, guard = plan_most(eng, db), {}, threading.Lock()

        def parts(table):
            with guard:
                if table in cut:
                    return cut[table]
            try:
                got = list(cutter(db, table, most) or [""])
            except Exception:  # noqa: BLE001 - uncut is read whole
                got = [""]
            with guard:
                # more ranges than the plan holds is a cut it did not ask
                # for, and fewer of them would not cover the table
                return cut.setdefault(table, got if len(got) <= most
                                      else [""])
    marker = getattr(eng, "_change_marker", None)
    scope = getattr(hop, "scope_fingerprint", None)
    real = isinstance(hop, config.Hop)
    return run(
        eng, db, aggregates, source=source, query=query, parts=parts,
        markers=(lambda side, t: marker(side, db, t)) if marker else None,
        held=eng._held_for_cutover(db) if marker else "",
        scope=scope() if callable(scope) else None,
        costs=decide.Layered(decide.Costs(), decide.HopCosts(
            hop, "scan/")) if real else None,
        progress=hop.report_dir(db) / progress,
        evidence=Evidence(hop.report_dir(db) / "scan-evidence.json")
        if marker else None, count=count)


def run(eng, db, aggregates, *, source, query, parts=None, markers=None,
        held="", scope=None, costs=None, progress=None, evidence=None,
        log=None, sides=("src", "dst"), count="count(*)"):
    """`Got` for `aggregates` over `sides` of `db`.

    `source(side, table)` is what a read selects from; `query(side, sql)`
    its rows; `parts(table)` [(predicate, ...)] the predicates of the key
    ranges that cover the table (`covering`), [""] for the whole of it;
    `markers(side, table)` the side's change marker, or None where it has
    none; `held` why nothing kept is reused (a cutover's proof); `scope`
    what the values are read under; `costs` where each way's measured
    seconds a row are kept (`decide.Costs`, `HopCosts`); `progress` the
    file that says how far it is; `evidence` an `Evidence`; `count` how
    the side counts its rows (`count_big(*)` where `count(*)` overflows)."""
    hop = getattr(eng, "hop", None)
    got = Got(aggregates)
    tables = {}
    for agg in got.aggregates:
        exprs = tables.setdefault(agg.table, {})
        for e in agg.exprs:
            exprs.setdefault(e, agg.fold(e))
    if not tables:
        return got
    costs = costs if costs is not None else decide.Costs()
    lock = threading.Lock()
    state = {"planned": 0, "completed": 0, "table": "", "side": ""}

    def say(status):
        if progress is None:
            return
        from . import durable
        with lock:
            line = dict(state, phase="scan", status=status,
                        reused=got.reused, ways=dict(got.ways),
                        resume_supported=bool(evidence is not None))
        progress.parent.mkdir(parents=True, exist_ok=True)
        durable.write_atomic(progress, json.dumps(line, default=str))

    def one_table(table):
        from . import attempts
        with attempts.work_unit(eng, "deep", f"{db}.{table}"):
            table_once(table)

    def table_once(table):
        exprs = tables[table]
        wanted = list(exprs)
        mark = {s: (markers(s, table) if markers else None) for s in sides}
        sig = scope or ""
        kept = ({} if held or evidence is None
                else evidence.reusable(table, mark, sig))
        for s in sides:
            for e in list(wanted):
                if e in (kept.get(s) or {}) and all(
                        e in (kept.get(o) or {}) for o in sides):
                    got.values[(s, table, e)] = kept[s][e]
        left = [e for e in wanted
                if any((s, table, e) not in got.values for s in sides)]
        with lock:
            got.reused += len(wanted) - len(left)
        if not left:
            got.ways[table] = "kept"
            return
        preds = list(parts(table)) if parts else [""]
        facts = decide.Facts({"ranges": len(preds), "expressions": len(left)})

        def read(choice):
            name = choice.rung.name
            if name == "apart":
                units = [(s, (e,), "") for s in sides for e in left]
            else:
                cut = preds if name == "ranges" else [""]
                units = [(s, chunk, p) for s in sides
                         for chunk in _chunks(left) for p in cut]
            sums, failed, rows = {}, {}, {}
            with lock:
                state["planned"] += len(units)
            at = {u: i for i, u in enumerate(units)}

            def unit(u):
                # each range's worker opens its own part of the table
                from . import attempts
                with attempts.work_unit(
                        eng, "deep", f"{db}.{table} {at[u] + 1}/{len(units)}",
                        level="range", parent=f"{db}.{table}",
                        parts=len(units)):
                    return unit_read(u)

            def unit_read(u):
                side, chunk, pred = u
                with lock:
                    state["table"], state["side"] = table, side
                sql = (f"select {count}, {', '.join(chunk)} from"
                       f" {source(side, table)}"
                       + (f" where {pred}" if pred else ""))
                try:
                    answer = query(side, sql)
                    if not answer:
                        raise RuntimeError(f"a read of {table} returned no"
                                           " row")
                    row = answer[0]
                    if len(row) < len(chunk) + 1:
                        raise RuntimeError(
                            f"a read of {table} returned {len(row) - 1} of"
                            f" its {len(chunk)} values")
                except Exception as e:
                    if name != "apart":
                        facts["a fused read failed"] = True
                        raise
                    with lock:
                        for x in chunk:
                            failed[(side, x)] = e
                    return 0
                with lock:
                    rows[(side, pred)] = int(row[0] or 0)
                    for x, v in zip(chunk, row[1:]):
                        k, v = (side, x), _number(v)
                        sums[k] = FOLDS[exprs[x]](sums[k], v) \
                            if k in sums else v
                    state["completed"] += 1
                say("running")
                return int(row[0] or 0)

            began = time.monotonic()
            _slots(eng, db, hop, len(units)).each(units, unit)
            seen = sum(rows.values()) or 1
            if not failed:
                costs.saw(name, seen, time.monotonic() - began)
            return sums, failed, rows

        choice, (sums, failed, rows) = decide.attempt(
            f"{db}.{table}: the checks' aggregates", READS, read, facts,
            need=NEED, costs=costs, log=log)
        with lock:
            got.ways[table] = choice.rung.name
            for (side, e), v in sums.items():
                got.values[(side, table, e)] = v
            for (side, e), err in failed.items():
                got.failed.setdefault((table, e), err)
                got.values.pop((side, table, e), None)
            for s in sides:
                got.rows[(s, table)] = sum(n for (x, _), n in rows.items()
                                           if x == s)
        if evidence is not None and not held:
            values = {s: {e: got.values[(s, table, e)] for e in wanted
                          if (s, table, e) in got.values} for s in sides}
            if all(len(values[s]) == len(wanted) for s in sides):
                evidence.keep(table, mark, sig, values)
            else:
                evidence.keep(table, {s: None for s in sides}, sig, {})

    try:
        _slots(eng, db, hop, len(tables)).each(sorted(tables), one_table)
    except BaseException:
        say("incomplete")
        raise
    if evidence is not None:
        evidence.save()
    say("incomplete" if got.failed else "complete")
    return got


def canon_text(value):
    """One value as canonical text: alike wherever two servers hold the
    same value, whatever each renders - a number by its value not its
    scale, bytes as hex, a time in ISO form, NULL as a byte no text
    holds."""
    import datetime
    import decimal
    if value is None:
        return "\x00"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            return str(value)
        got = format(value.normalize(), "f")
        return "0" if got in ("-0", "0") else got
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    return str(value)


def row_canon(row):
    """A row as one canonical text, its values in their order."""
    return "\x1f".join(canon_text(v) for v in row)


@dataclass
class RowText:
    """What `row_text` read: the rows of each side, each range's digest a
    side, and the first row the two sides hold apart - (the range it is
    in, its key, what each side holds of it) - or None where every row is
    alike. `earlier` is how many of its ranges stand on a check stopped
    before this one (`RangeTexts`), read at `earlier_at` (wall time) - the
    guarantee such a range gives is that read's, not this one's."""
    rows: dict = field(default_factory=dict)
    digests: list = field(default_factory=list)
    first: tuple = None
    earlier: int = 0
    earlier_at: float = None

    def earlier_said(self):
        """"N of M ranges from the check stopped at <time>", or "" where
        every range was read by this check."""
        if not self.earlier:
            return ""
        import datetime
        when = (datetime.datetime.fromtimestamp(
            self.earlier_at, datetime.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC") if self.earlier_at else
            "an earlier time")
        return (f"{self.earlier} of {len(self.digests)} ranges from the"
                f" check stopped at {when}")

    @property
    def alike(self):
        return self.first is None and len(set(self.rows.values())) <= 1


class RangeTexts:
    """The key ranges of each table a row text check has proved, kept in
    one file (`durable`) as each range is proved, so a check stopped half
    way goes on from the ranges it owes and never falls back to one read
    of the whole table. A range is kept for the check it was read in (the
    table and what it is read under, `scope`): a table whose every range
    is proved has its ranges forgotten, so the next check reads them all
    again rather than standing on rows read before it began."""

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        try:
            self.kept = json.loads(path.read_text()) if path else {}
        except (OSError, ValueError):
            self.kept = {}

    def of(self, table, scope=""):
        """{range predicate: (counts, digests, first)} of `table` proved by
        the check stopped earlier under the same `scope`."""
        got = self.kept.get(table) or {}
        if got.get("scope") != (scope or ""):
            return {}
        out = {}
        for pred, r in (got.get("ranges") or {}).items():
            first = r.get("first")
            out[pred] = (dict(r["rows"]), dict(r["digest"]),
                         (tuple(first[0]), dict(first[1])) if first
                         else None)
        return out

    def proved(self, table, scope, pred, result):
        """Range `pred` of `table` proved: kept on the disk before the
        next range is owed."""
        count, digest, first = result
        with self._lock:
            got = self.kept.get(table) or {}
            if got.get("scope") != (scope or ""):
                got = {"scope": scope or "", "ranges": {}}
            got["ranges"][pred] = {
                "rows": count, "digest": digest,
                "first": [list(first[0]), first[1]] if first else None}
            got["at"] = time.time()
            self.kept[table] = got
            self._save()

    def proved_at(self, table, scope=""):
        """The wall time the check stopped earlier last proved a range of
        `table` under `scope`, or None."""
        got = self.kept.get(table) or {}
        return got.get("at") if got.get("scope") == (scope or "") else None

    def finished(self, table):
        """Every range of `table` proved: nothing of it is kept for the
        next check."""
        with self._lock:
            if self.kept.pop(table, None) is not None:
                self._save()

    def _save(self):
        if not self.path:
            return
        from . import durable
        durable.write_atomic(self.path, json.dumps(self.kept))


def row_text(rows, preds=("",), key_width=1, most=1, sides=("src", "dst"),
             eng=None, table="", scope="", kept=None):
    """Every row of both `sides` brought here as canonical text and
    compared, a key range at a time (`preds`, from `bounded`), as many
    ranges side by side as `most` and the run's slots allow (`together`).
    `rows(side, pred)` yields a side's rows of the range in key order, the
    key's `key_width` values first (0: the whole row is its own key, read
    in its own order). Each range is hashed a side and its rows walked
    together, so the first key the two hold apart is named - not just
    that the table differs.

    Where `table` is named, each range is a unit of `eng`'s ledger (one
    `attempts.work_unit` a range, its parent the table) and, with `kept`
    (`RangeTexts`), each range proved is kept as it is: a check run again
    after a stop reads only the ranges not proved, in their own units, and
    folds the kept ones in their place."""
    import hashlib
    import itertools
    from . import attempts
    preds = list(preds) or [""]
    done = kept.of(table, scope) if kept is not None and table else {}
    at = {p: i for i, p in enumerate(preds)}

    def one(pred):
        if pred in done:
            return done[pred]
        if not table:
            return read(pred)
        with attempts.work_unit(eng, "deep",
                                f"{table} {at[pred] + 1}/{len(preds)}",
                                level="range", parent=table,
                                parts=len(preds)):
            got = read(pred)
        if kept is not None:
            kept.proved(table, scope, pred, got)
        return got

    def read(pred):
        digest = {s: hashlib.blake2b(digest_size=16) for s in sides}
        count = dict.fromkeys(sides, 0)
        first = None
        for pair in itertools.zip_longest(*(iter(rows(s, pred))
                                            for s in sides)):
            texts = []
            for s, r in zip(sides, pair):
                if r is None:
                    texts.append(None)
                    continue
                t = row_canon(r)
                digest[s].update(t.encode("utf-8", "surrogatepass") + b"\n")
                count[s] += 1
                texts.append(t)
            if first is None and texts[0] != texts[1]:
                held = next(r for r in pair if r is not None)
                key = tuple(canon_text(v) for v in (
                    held[:key_width] if key_width else held))
                first = (key, {s: ("absent" if t is None else "differs")
                               for s, t in zip(sides, texts)})
        return count, {s: d.hexdigest() for s, d in digest.items()}, first

    got = RowText(rows=dict.fromkeys(sides, 0),
                  earlier=sum(1 for p in preds if p in done),
                  earlier_at=kept.proved_at(table, scope) if done else None)
    for i, (count, digest, first) in enumerate(together(preds, one, most)):
        for s in sides:
            got.rows[s] += count[s]
        got.digests.append(digest)
        if got.first is None and first is not None:
            got.first = (preds[i] or "whole table",) + first
    if kept is not None and table:
        kept.finished(table)
    return got


def fold_digests(got, side):
    """One digest of `side` over every range `row_text` read, in range
    order: alike on both sides only where every range is."""
    import hashlib
    whole = hashlib.blake2b(digest_size=16)
    for d in got.digests:
        whole.update(d[side].encode())
    return whole.hexdigest()


def row_text_line(table, got):
    """A table's OK/DIFF line from what `row_text` read: the rows and the
    digest folded over every range, or the first key held apart."""
    import hashlib
    if got.alike:
        whole = hashlib.blake2b(digest_size=16)
        for d in got.digests:
            whole.update(next(iter(d.values())).encode())
        n = next(iter(got.rows.values()), 0)
        line = f"{table}: OK rows={n} checksum={whole.hexdigest()}"
        return line + (f" ({got.earlier_said()})" if got.earlier else "")
    if got.first is None:
        rows = " ".join(f"{s}={n}" for s, n in got.rows.items())
        return f"{table}: DIFF {rows}" + (
            f" ({got.earlier_said()})" if got.earlier else "")
    where, key, held = got.first
    said = ", ".join(f"{s} {h}" for s, h in held.items())
    rows = " ".join(f"{s}={n}" for s, n in got.rows.items())
    return (f"{table}: DIFF {rows} first key={'|'.join(key)} ({said})"
            f" where {where}"
            + (f" ({got.earlier_said()})" if got.earlier else ""))


REPLACEMENT_CHAR = "\ufffd"


@dataclass
class FusedPushdownResult:
    """One slice's verification totals from a single fused query."""
    rows: int
    data_hash: object
    charset_anomalies: int
    null_empty_diffs: int
    extra: tuple = ()

    def __iter__(self):
        return iter((self.rows, self.data_hash, self.charset_anomalies, self.null_empty_diffs) + self.extra)

    def __getitem__(self, idx):
        return (self.rows, self.data_hash, self.charset_anomalies, self.null_empty_diffs, *self.extra)[idx]


def _quote_ident(engine, name, quote=True):
    if not quote:
        return name
    eng = str(engine).lower()
    if eng == "mysql":
        return name if name.startswith("`") and name.endswith("`") else f"`{name}`"
    return name if name.startswith('"') and name.endswith('"') else f'"{name}"'


def fused_pushdown_query(engine, table, hash_expr=None, *,
                         hash_algo="xor",
                         hash_agg=None,
                         charset_columns=(),
                         null_empty_conditions=(),
                         null_empty_columns=(),
                         where="",
                         aliases=False,
                         quote_identifiers=True,
                         count_expr=None):
    eng = str(engine).lower()
    cols = []
    cnt = count_expr or ("COUNT(*)" if aliases else "count(*)")
    cols.append(f"{cnt} as total_rows" if aliases else cnt)

    if hash_agg is not None:
        h_part = str(hash_agg)
    elif hash_expr is not None:
        if hash_algo == "xor":
            if eng == "mysql":
                if not any(fn in str(hash_expr).lower() for fn in ("conv(", "cast(", "crc32(")):
                    int_expr = f"cast(conv(substring(md5({hash_expr}), 1, 16), 16, 10) as unsigned)"
                else:
                    int_expr = hash_expr
                h_part = f"coalesce(BIT_XOR({int_expr}), 0)"
            else:
                h_part = f"coalesce(bit_xor({hash_expr}), 0)"
        else:
            h_part = f"coalesce(sum({hash_expr}), 0)"
    else:
        h_part = "BIT_XOR(HASH_EXPR)" if hash_algo == "xor" else "sum(HASH_EXPR)"
    cols.append(f"{h_part} as data_hash" if aliases else h_part)

    if charset_columns:
        preds = [_quote_ident(eng, c, quote_identifiers) for c in charset_columns]
        cs_cond = " OR ".join(f"{c} LIKE '%{REPLACEMENT_CHAR}%'" for c in preds)
        cs_expr = f"SUM(CASE WHEN {cs_cond} THEN 1 ELSE 0 END)"
        cols.append(f"{cs_expr} as charset_anomalies" if aliases else cs_expr)

    nec_preds = []
    if null_empty_conditions:
        nec_preds = list(null_empty_conditions)
    elif null_empty_columns:
        cols_list = list(null_empty_columns)
        if cols_list and isinstance(cols_list[0], (list, tuple)):
            for pair in cols_list:
                if len(pair) >= 2:
                    qa = _quote_ident(eng, pair[0], quote_identifiers)
                    qb = _quote_ident(eng, pair[1], quote_identifiers)
                    nec_preds.append(f"{qa} IS NULL AND {qb} = ''")
                elif len(pair) == 1:
                    qc = _quote_ident(eng, pair[0], quote_identifiers)
                    nec_preds.append(f"{qc} IS NULL OR {qc} = ''")
        elif len(cols_list) == 2:
            qa = _quote_ident(eng, cols_list[0], quote_identifiers)
            qb = _quote_ident(eng, cols_list[1], quote_identifiers)
            nec_preds.append(f"{qa} IS NULL AND {qb} = ''")
        else:
            pairs = [cols_list[i:i + 2] for i in range(0, len(cols_list), 2)]
            for pair in pairs:
                if len(pair) == 2:
                    qa = _quote_ident(eng, pair[0], quote_identifiers)
                    qb = _quote_ident(eng, pair[1], quote_identifiers)
                    nec_preds.append(f"{qa} IS NULL AND {qb} = ''")
                else:
                    qc = _quote_ident(eng, pair[0], quote_identifiers)
                    nec_preds.append(f"{qc} IS NULL OR {qc} = ''")

    for i, cond in enumerate(nec_preds):
        ne_expr = f"SUM(CASE WHEN {cond} THEN 1 ELSE 0 END)"
        if aliases:
            ne_expr += " as null_empty_diffs" if i == 0 else f" as null_empty_diffs_{i}"
        cols.append(ne_expr)

    sql = f"SELECT {', '.join(cols)} FROM {table}"
    if where:
        w = where.strip()
        sql += f" {w}" if w.lower().startswith("where ") else f" WHERE {w}"
    return sql


def fused_pushdown_verify(eng, db, table, *, query,
                          source=None,
                          text_columns=(),
                          hash_expr=None,
                          hash_algo="xor",
                          hash_agg=None,
                          null_empty_conditions=(),
                          null_empty_columns=(),
                          parts=None,
                          sides=("src", "dst"),
                          quote_identifiers=True,
                          keyless=False,
                          keyless_mode="auto",
                          total_blocks=None,
                          total_rows=None,
                          total_pages=None,
                          chunk_size=None):
    from .engines.base import Result
    dialect = getattr(eng, "CANON_ENGINE", "mysql")
    if source is None:
        source = lambda side, t: t
    if keyless and parts is None:
        from . import keyless as kl
        dialect_name = str(dialect).lower()
        if dialect_name in ("postgres", "pg", "postgresql"):
            u = total_blocks if total_blocks is not None else (total_rows or 1000)
            m = "ctid" if keyless_mode == "auto" else keyless_mode
        elif keyless_mode == "page":
            u = total_pages if total_pages is not None else (total_rows or 1000)
            m = "page"
        else:
            u = total_rows or 1000
            m = "rowid" if keyless_mode == "auto" else keyless_mode
        kl_slices = kl.keyless_slices(dialect_name, u, chunk_size=chunk_size, mode=m)
        slices = [s.predicate for s in kl_slices]
    else:
        slices = list(parts) if parts is not None else [""]
    if not slices:
        slices = [""]

    slice_results = {s: [] for s in sides}
    for pred in slices:
        for s in sides:
            src_table = source(s, table)
            sql = fused_pushdown_query(
                dialect, src_table, hash_expr=hash_expr, hash_algo=hash_algo,
                hash_agg=hash_agg, charset_columns=text_columns,
                null_empty_conditions=null_empty_conditions,
                null_empty_columns=null_empty_columns, where=pred,
                quote_identifiers=quote_identifiers)
            answer = query(s, sql)
            if not answer:
                raise RuntimeError(f"fused query returned no rows for {table} on {s}")
            row = answer[0]
            if isinstance(row, str):
                row = row.split("|")
            rows = int(row[0] or 0)
            dhash = row[1] if len(row) > 1 and row[1] is not None else 0
            cs_anom = int(row[2] or 0) if len(row) > 2 else 0
            ne_diff = int(row[3] or 0) if len(row) > 3 else 0
            extra = tuple(row[4:]) if len(row) > 4 else ()
            slice_results[s].append(FusedPushdownResult(rows, dhash, cs_anom, ne_diff, extra))

    if len(sides) == 1:
        s = sides[0]
        total_rows = sum(r.rows for r in slice_results[s])
        return Result("deep", f"{db}.{table} fused", "ok",
                      f"{table}: rows={total_rows} clean", category="verify.fused")

    totals = {}
    for s in ("src", "dst"):
        s_rows = sum(r.rows for r in slice_results[s])
        s_cs = sum(r.charset_anomalies for r in slice_results[s])
        s_ne = sum(r.null_empty_diffs for r in slice_results[s])
        if hash_algo == "xor":
            s_hash = 0
            for r in slice_results[s]:
                try:
                    s_hash ^= int(r.data_hash or 0)
                except (ValueError, TypeError):
                    s_hash = r.data_hash
        else:
            s_hash = 0
            for r in slice_results[s]:
                try:
                    s_hash += int(r.data_hash or 0)
                except (ValueError, TypeError):
                    s_hash = r.data_hash
        totals[s] = (s_rows, s_hash, s_cs, s_ne)

    src_r, src_h, src_cs, src_ne = totals["src"]
    dst_r, dst_h, dst_cs, dst_ne = totals["dst"]

    if src_r != dst_r:
        return Result("deep", f"{db}.{table} fused", "diff",
                      f"{table}: row count mismatch: src={src_r} dst={dst_r}",
                      category="verify.fused")
    if src_h != dst_h:
        return Result("deep", f"{db}.{table} fused", "diff",
                      f"{table}: data hash mismatch: src={src_h} dst={dst_h}",
                      category="verify.fused")
    if dst_cs > src_cs:
        return Result("deep", f"{db}.{table} fused", "diff",
                      f"{table}: charset corruption: {dst_cs - src_cs} excess replacement characters on target",
                      category="verify.fused")
    if src_ne != dst_ne:
        return Result("deep", f"{db}.{table} fused", "diff",
                      f"{table}: null/empty drift: src={src_ne} dst={dst_ne}",
                      category="verify.fused")

    return Result("deep", f"{db}.{table} fused", "ok",
                  f"{table}: OK rows={src_r} hash={src_h} clean",
                  category="verify.fused")


@dataclass
class AdaptiveMerkleResult:
    table: str
    total_rows: int
    mismatched_keys: list
    identical_buckets: int
    divergent_buckets: int
    drilldown_rows_examined: int
    status: str
    detail: str
    certified_complete: bool = False


def adaptive_merkle_bucket_query(engine, table, key_col, hash_expr=None, *,
                                 num_buckets=1024, lo=None, hi=None, width=None,
                                 where="", aliases=False, quote_identifiers=True):
    eng = str(engine).lower()
    k = _quote_ident(eng, key_col, quote_identifiers)
    if lo is not None and hi is not None:
        span = hi - lo + 1
        w = width or max(1, -(-span // num_buckets))
        b_expr = f"(({k} - {lo}) DIV {w})" if eng == "mysql" else f"div({k} - {lo}, {w})"
    elif width is not None:
        base = lo if lo is not None else 0
        b_expr = f"(({k} - {base}) DIV {width})" if eng == "mysql" else f"div({k} - {base}, {width})"
    else:
        if eng == "mysql":
            b_expr = f"(conv(substr(md5(cast({k} as char)), 1, 8), 16, 10) % {num_buckets})"
        else:
            b_expr = f"(abs(('x' || substr(md5(cast({k} as text)), 1, 8))::bit(32)::bigint) % {num_buckets})"

    h_expr = k if hash_expr is None else hash_expr
    if eng == "mysql":
        h_part = h_expr if h_expr.upper().startswith("BIT_XOR(") else f"BIT_XOR({h_expr})"
    else:
        h_part = h_expr if h_expr.lower().startswith("bit_xor(") else f"bit_xor({h_expr})"

    cols = []
    cols.append(f"{b_expr} as bucket_id" if aliases else b_expr)
    cols.append("COUNT(*) as row_count" if aliases else "COUNT(*)")
    cols.append(f"{h_part} as bucket_hash" if aliases else h_part)

    sql = f"SELECT {', '.join(cols)} FROM {table}"
    conds = []
    if lo is not None and hi is not None:
        conds.append(f"{k} >= {lo} AND {k} <= {hi}")
    if where:
        w = where.strip()
        if w.lower().startswith("where "):
            w = w[6:].strip()
        if w:
            conds.append(w)
    if conds:
        sql += " WHERE " + " AND ".join(f"({c})" for c in conds)
    sql += f" GROUP BY {b_expr} ORDER BY bucket_id" if aliases else f" GROUP BY {b_expr}"
    return sql


def adaptive_merkle_bucket_verify(eng, db, table, key_col, *,
                                  lo=None, hi=None, num_buckets=1024,
                                  hash_expr=None, query=None, row_query=None,
                                  source=None, sides=("src", "dst"),
                                  leaf_threshold=16, max_depth=8,
                                  quote_identifiers=True):
    from . import leaves
    dialect = getattr(eng, "CANON_ENGINE", "mysql")
    if source is None:
        source = lambda side, t: t
    src_table = source("src", table)
    dst_table = source("dst", table)

    def _q_fn(side, q_lo, q_hi, q_nb):
        tbl = src_table if side == "src" else dst_table
        sql = adaptive_merkle_bucket_query(
            dialect, tbl, key_col, hash_expr=hash_expr, num_buckets=q_nb,
            lo=q_lo, hi=q_hi, quote_identifiers=quote_identifiers)
        ans = query(side, sql)
        out = []
        for r in ans:
            if isinstance(r, str):
                r = r.split("|")
            b_id = int(r[0])
            cnt = int(r[1])
            h = int(r[2]) if r[2] is not None and str(r[2]).strip() != "" else 0
            out.append((b_id, cnt, h))
        return out

    def _row_fn(side, r_lo, r_hi):
        if row_query is not None:
            ans = row_query(side, r_lo, r_hi)
        else:
            tbl = src_table if side == "src" else dst_table
            k = _quote_ident(dialect, key_col, quote_identifiers)
            h = hash_expr or k
            sql = f"SELECT {k}, {h} FROM {tbl} WHERE {k} >= {r_lo} AND {k} <= {r_hi} ORDER BY {k}"
            ans = query(side, sql)
        out = []
        for r in ans:
            if isinstance(r, str):
                r = r.split("|")
            out.append((int(r[0]), int(r[1]) if r[1] is not None else 0))
        return out

    differ = leaves.AdaptiveBucketDiffer(
        key_col=key_col, lo=lo, hi=hi, num_buckets=num_buckets,
        hash_expr=hash_expr, leaf_threshold=leaf_threshold,
        max_depth=max_depth, engine=dialect)
    return differ.run(_q_fn, row_fn=_row_fn, table=table, sides=sides)


def keyless_slice_query(engine, table, slice_predicate, **kwargs):
    from . import keyless
    return keyless.keyless_slice_query(engine, table, slice_predicate, **kwargs)


def keyless_slice_verify(eng, db, table, **kwargs):
    from . import keyless
    return keyless.keyless_slice_verify(eng, db, table, **kwargs)


def _prove_single_query_pushdown(f: Dict[str, Any]) -> bool:
    return bool(f.get("fused", True)) and int(f.get("total_rows", 0)) <= int(f.get("single_query_threshold", 500000)) and len(f.get("parts", []) or []) <= 1


def _prove_charset_anomaly_detection(f: Dict[str, Any]) -> bool:
    return bool(f.get("has_text_columns", False)) or bool(f.get("text_columns", ()))


def _prove_null_empty_drift_fencing(f: Dict[str, Any]) -> bool:
    return bool(f.get("has_null_empty_conditions", False)) or bool(f.get("null_empty_conditions", ())) or bool(f.get("null_empty_columns", ()))


def _prove_partitioned_range_pushdown(f: Dict[str, Any]) -> bool:
    return bool(f.get("partitioned", False)) or len(f.get("parts", []) or []) > 1 or int(f.get("total_rows", 0)) > int(f.get("single_query_threshold", 500000))


def _prove_refuse_unpushable_aggregation(f: Dict[str, Any]) -> bool:
    return bool(f.get("unpushable", False)) or not bool(f.get("can_aggregate", True))


def fused_pushdown_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "single_query_fused_pushdown",
            "execute fused aggregation in a single query across row count, hash, and value anomalies",
            needs=(
                decide.Need(
                    "table fits single pushdown query envelope",
                    _prove_single_query_pushdown,
                    "table requires partitioned range slicing or exceeds threshold",
                ),
            ),
        ),
        decide.Rung(
            "charset_anomaly_detection",
            "fuse replacement character detection into pushdown query for text columns",
            needs=(
                decide.Need(
                    "text columns present for charset screening",
                    _prove_charset_anomaly_detection,
                    "no text columns requiring charset screening",
                ),
            ),
        ),
        decide.Rung(
            "null_empty_drift_fencing",
            "fuse null versus empty string differentiation into pushdown verification",
            needs=(
                decide.Need(
                    "null/empty conditions specified",
                    _prove_null_empty_drift_fencing,
                    "no null/empty conditions present",
                ),
            ),
        ),
        decide.Rung(
            "partitioned_range_pushdown",
            "execute fused pushdown verification partitioned across key or block slices",
            needs=(
                decide.Need(
                    "partitioned range slicing active",
                    _prove_partitioned_range_pushdown,
                    "table fits single pushdown query",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unpushable_aggregation",
            "refuse pushdown verification when dialect lacks required aggregate functions",
            needs=(
                decide.Need(
                    "refuse unpushable pushdown aggregation",
                    _prove_refuse_unpushable_aggregation,
                    "aggregation pushdown fully supported by engine",
                ),
            ),
        ),
    )


def decide_fused_pushdown(facts: Dict[str, Any]) -> decide.Choice:
    return decide.climb("fused pushdown verification", fused_pushdown_rungs(), facts=decide.Facts(facts))



