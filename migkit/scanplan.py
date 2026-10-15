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
    alike."""
    rows: dict = field(default_factory=dict)
    digests: list = field(default_factory=list)
    first: tuple = None

    @property
    def alike(self):
        return self.first is None and len(set(self.rows.values())) <= 1


def row_text(rows, preds=("",), key_width=1, most=1, sides=("src", "dst")):
    """Every row of both `sides` brought here as canonical text and
    compared, a key range at a time (`preds`, from `bounded`), as many
    ranges side by side as `most` and the run's slots allow (`together`).
    `rows(side, pred)` yields a side's rows of the range in key order, the
    key's `key_width` values first (0: the whole row is its own key, read
    in its own order). Each range is hashed a side and its rows walked
    together, so the first key the two hold apart is named - not just
    that the table differs."""
    import hashlib
    import itertools
    preds = list(preds) or [""]

    def one(pred):
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

    got = RowText(rows=dict.fromkeys(sides, 0))
    for i, (count, digest, first) in enumerate(together(preds, one, most)):
        for s in sides:
            got.rows[s] += count[s]
        got.digests.append(digest)
        if got.first is None and first is not None:
            got.first = (preds[i] or "whole table",) + first
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
        return f"{table}: OK rows={n} checksum={whole.hexdigest()}"
    if got.first is None:
        rows = " ".join(f"{s}={n}" for s, n in got.rows.items())
        return f"{table}: DIFF {rows}"
    where, key, held = got.first
    said = ", ".join(f"{s} {h}" for s, h in held.items())
    rows = " ".join(f"{s}={n}" for s, n in got.rows.items())
    return (f"{table}: DIFF {rows} first key={'|'.join(key)} ({said})"
            f" where {where}")
