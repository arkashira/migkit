"""How a batch of changes is applied (backlog W9): collapsed change by
change, or its keys collapsed as columns and each table's net rows handed
to the engine's bulk way as one run of each kind.

The second is climbed to through `decide` only where the batches applied
change by change go slower than the source changes - measured, the rate
the rows way applied at against the changes the source committed a second
of its own clock (`txn_ts`) - and kept only while it measures cheaper a
change here. Either way the batch is the one transaction its session
holds, the tables bound by keys go change by change in the source's order
first, and the statements written and the rows in them are the same: the
columns way takes a batch only where its changes collapse as columns to
exactly what the rows way leaves of them (`Plan.of`), and says why where
they do not."""

import heapq
import itertools
import math
import threading
import time
from collections import deque

from . import backfill_race, canon, decide, partial_images

WORK = "applying changes"
#: the batches each rate is measured over
WINDOW = 8
#: the types a key column is collapsed as a column of: those whose
#: equality is their `repr`'s, which is how the rows way tells rows apart
#: (`base._ident`) - not 1 and 1.0, not two spellings of one decimal
KEY_TYPES = (int, str, bytes)
#: the values a change carries that are not the value itself - an addition,
#: a value kept where it stands, a field not there - which merge into what
#: an earlier change left (`canon.merged_values`), so are not taken last
MARKED = (canon.Added, type(canon.ABSENT))


class Pace:
    """What the source changes at and what each way applies at, over the
    last `WINDOW` batches of one engine."""

    def __init__(self):
        self._lock = threading.Lock()
        self._source = deque(maxlen=WINDOW)
        self._ways = {}
        self._last = None
        #: the way the last batch took, and the switches already said
        self.on, self.said = None, set()

    def arrived(self, changes):
        """A batch read: the changes it holds against the source's commit
        times on them, from the last batch's last commit - or, for the
        first, between its own first and last."""
        first, last = _stamped(changes), _stamped(reversed(changes))
        if last is None:
            return
        with self._lock:
            prev = self._last
            self._last = last[1] if prev is None else max(prev, last[1])
            if prev is not None and last[1] > prev:
                self._source.append((len(changes), last[1] - prev))
            elif prev is None and last[1] > first[1]:
                at = len(changes) - 1 - last[0]
                self._source.append((at - first[0], last[1] - first[1]))

    def applied(self, way, changes, seconds):
        with self._lock:
            self._ways.setdefault(way, deque(maxlen=WINDOW)).append(
                (changes, seconds))

    def source_rate(self):
        """Changes a second the source made, or None where unmeasured."""
        return _rate(self._source)

    def rate(self, way):
        """Changes a second `way` applied, or None where it never ran."""
        return _rate(self._ways.get(way, ()))

    def behind(self):
        """(whether the rows way applies slower than the source changes,
        what was measured, in words)."""
        source, rows = self.source_rate(), self.rate("rows")
        if source is None:
            return False, ("the rate the source changes at is not measured:"
                           " its changes carry no commit time")
        if rows is None:
            return False, "the rows way has not been timed here yet"
        said = (f"applied change by change at {rows:,.0f} changes a second"
                f" while the source changes at {source:,.0f}")
        return rows < source, said


def _stamped(changes):
    """(position, commit time) of the first change in `changes` that
    carries its transaction's commit time, or None."""
    for i, c in enumerate(changes):
        ts = c.get("txn_ts")
        if ts is not None:
            return i, float(ts)
    return None


def _rate(window):
    n = sum(c for c, _ in window)
    s = sum(t for _, t in window)
    return n / s if s > 0 and n else None


def _behind(facts):
    return facts["behind"][0]


def _planned(facts):
    plan = facts["plan"]
    return None if isinstance(plan, Plan) else plan


WAYS = (
    decide.Rung("rows", "the batch collapsed change by change",
                because="each change merged into the row it leaves"),
    decide.Rung(
        "columns", "the batch's keys collapsed as columns and each table's"
                   " rows written as one run of each kind",
        needs=(decide.Need("the source outpacing the rows way", _behind,
                           lambda f: "the batches keep up: "
                                     + f["behind"][1]),),
        prove=_planned,
        because="the rows applied slower than the source changes"),
)


class Plan:
    """A batch collapsed as columns: the rows of the tables bound by keys
    change by change, as the rows way has them, then - where the engine
    names those tables - each table's deletes and then its upserts, a run
    each, in the order `_net_rows` gives them; where it cannot, every row
    in the order it was first touched, as `_collapsed` gives them.

    `rows` is [(table, kind, [(first touched, table, kind, key, values)])]
    grouped, else those rows in the order first touched."""

    def __init__(self, bound, rows, grouped, keys):
        self.bound, self.rows, self.grouped = bound, rows, grouped
        self.keys = keys

    @classmethod
    def of(cls, engine, side, db, changes):
        """The plan, or why the batch is not collapsed as columns."""
        split = engine._net_split(side, db, changes)
        bound, loose = split if split is not None else ((), changes)
        try:
            tables = _collapse(loose)
        except (KeyError, TypeError, AttributeError, ValueError) as e:
            return f"a change the columns cannot read ({type(e).__name__})"
        if isinstance(tables, str):
            return tables
        bound = [row for c in bound for row in engine._collapsed([c])]
        keys = {t["table"]: (t["key"], t["names"]) for t in tables}
        if split is None:
            return cls(bound, list(heapq.merge(*(t["rows"]
                                                 for t in tables))),
                       False, keys)
        groups = []
        for kind in ("delete", "upsert"):
            mine = [(t["table"], [r for r in t["rows"] if r[2] == kind])
                    for t in tables]
            groups += sorted(((t, kind, rs) for t, rs in mine if rs),
                             key=lambda g: g[2][0][0])
        return cls(bound, groups, True, keys)

    def net_rows(self):
        """The rows as `_net_rows` gives them: what the rows way writes."""
        rows = (self.rows if not self.grouped
                else [r for _, _, rs in self.rows for r in rs])
        return self.bound + [(r[1], (r[2], r[3], r[4])) for r in rows]

    def write(self, engine, side, db):
        """The rows written: a table's run of each kind straight to the
        engine's bulk way (`_apply_run`), which is the run `_apply_net`
        would have made of it - or, for an engine with a way of its own
        through the rows (a pipe), through that."""
        from .engines.base import Engine
        if not self.grouped or type(engine)._apply_net \
                is not Engine._apply_net:
            engine._apply_net(side, db, self.net_rows())
            return
        if self.bound:
            engine._apply_net(side, db, self.bound)
        for table, kind, rows in self.rows:
            key, names = self.keys[table]
            shape = (table, kind, tuple(sorted(key)),
                     tuple(sorted(names if kind == "upsert" else key)),
                     False)
            engine._apply_run(side, db, shape, [(r[3], r[4]) for r in rows])


#: tables a batch is split into by one pass over it for each; past it, one
#: pass that sorts every change
SPLIT_PASSES = 16


def _collapse(changes):
    """[{table, key, names, rows: [(first touched, table, kind, key,
    values)]}] for a batch's changes - what `_collapsed` leaves of each
    row, worked out a table at a time with its keys as columns - or why
    the batch is not one the columns can collapse alike."""
    tables = [c["table"] for c in changes]
    named = dict.fromkeys(tables)
    if len(named) == 1:
        by = {tables[0]: range(len(changes))} if changes else {}
    elif len(named) <= SPLIT_PASSES:
        by = {t: [i for i, x in enumerate(tables) if x == t] for t in named}
    else:
        by = {}
        for i, t in enumerate(tables):
            by.setdefault(t, []).append(i)
    out = []
    for table, at in by.items():
        got = _table(table, changes if isinstance(at, range)
                     else [changes[i] for i in at], at)
        if isinstance(got, str):
            return got
        out.append(got)
    return out


def _table(table, changes, at):
    """One table's changes collapsed as columns: each key's first touch
    and last change found by maps built from the key columns at once, not
    by merging change after change. Measured, 100,000 changes of three
    tables with their keys grouped by a columnar library took as long as
    the rows way - the changes come as rows, and making its columns of
    them cost what the grouping saved - so the maps are built from the
    columns as they are."""
    ops = [c.get("op") for c in changes]
    if set(ops) - {"insert", "update", "delete"}:
        return f"a change to {table} is not an insert, update or delete"
    named = {tuple(c["key"]) for c in changes}
    if len(named) != 1 or not next(iter(named)):
        return f"the changes to {table} are keyed on different columns"
    key, = named
    writes = [j for j, op in enumerate(ops) if op != "delete"]
    values = [changes[j].get("values") or {} for j in writes]
    shapes = {tuple(v) for v in values}
    if len(shapes) > 1:
        # a change carrying some of the columns is merged over the one
        # before it, which only the rows way does
        return f"the changes to {table} carry different columns"
    shape = next(iter(shapes)) if shapes else ()
    names = tuple(sorted({*key, *shape}))
    vals_flat = list(itertools.chain.from_iterable(map(dict.values, values)))
    if any(map(isinstance, vals_flat, itertools.repeat(MARKED))):
        return (f"a change to {table} adds to a value or keeps one where it"
                " stands")
    if any(isinstance(v, str) and v in partial_images.KNOWN_PLACEHOLDERS for v in vals_flat):
        return f"a change to {table} carries an unchanged LOB/TOAST placeholder"
    cols = [[c["key"][k] for c in changes] for k in key]
    moved = set()
    for k in key:
        if k in shape:
            old = cols[key.index(k)]
            moved.update(j for j, v in zip(writes, values) if v[k] != old[j])
    n = len(changes)
    if not moved:
        # an event a change: (change, None)
        ev = list(zip(range(n), itertools.repeat(None)))
    else:
        # an update that moved its key leaves its old address and arrives
        # at the new one: two events, (change, False) then (change, True)
        ev = [e for j in range(n) for e in
              (((j, False), (j, True)) if j in moved else ((j, None),))]
        cols = [[(changes[j]["values"] if f else changes[j]["key"])[k]
                 for j, f in ev] for k in key]
    for col in cols:
        types = set(map(type, col))
        if len(types) != 1 or not types <= set(KEY_TYPES):
            return (f"a key of {table} is not one type of whole number,"
                    " text or bytes throughout")
    keys = cols[0] if len(cols) == 1 else list(zip(*cols))
    m = len(keys)
    last = dict(zip(keys, range(m)))
    first = dict(zip(reversed(keys), range(m - 1, -1, -1)))
    rows = []
    for k in dict.fromkeys(keys):
        j, f = ev[last[k]]
        c = changes[j]
        s = first[k]
        s = 2 * at[ev[s][0]] + (ev[s][1] is True)
        if f is False or (f is None and ops[j] == "delete"):
            rows.append((s, table, "delete", dict(c["key"]), {}))
        elif f:
            v = c["values"]
            rows.append((s, table, "upsert", {x: v[x] for x in key},
                         dict(v)))
        else:
            rows.append((s, table, "upsert", dict(c["key"]),
                         dict(c.get("values") or {})))
    return {"table": table, "key": key, "names": names, "rows": rows}


_PACES = threading.Lock()


def state(engine):
    """The engine's `Pace`: the rates its batches were measured at and the
    way the last one took."""
    with _PACES:
        return engine.__dict__.setdefault("_apply_pace", Pace())


def _bucket(n):
    """The batches costed together: those of one power of ten of
    changes."""
    return 10 ** int(math.log10(max(int(n), 1)))


def apply(engine, side, db, changes):
    """Apply one batch the way measured cheaper here (`WAYS`), in the
    transaction the caller holds. The `Choice` back."""
    pace = state(engine)
    pace.arrived(changes)
    facts = decide.Facts({"behind": pace.behind()},
                         plan=lambda: Plan.of(engine, side, db, changes))
    began = time.perf_counter()
    with pace._lock:
        trials = engine._trials((WORK, "", _bucket(len(changes))), WAYS,
                                facts)
        choice = trials.pick({"rows": len(changes)})
    way = choice.path or "rows"
    if way == "columns":
        facts["plan"].write(engine, side, db)
    else:
        began = time.perf_counter()
        engine._apply_net(side, db, engine._net_rows(side, db, changes))
    took = time.perf_counter() - began
    trials.took(way, len(changes), took)
    pace.applied(way, len(changes), took)
    _say(engine, pace, choice, way)
    return choice


def _say(engine, pace, choice, way):
    """A switch of way said through the log lent (`lend_log`), once for
    each reason it switched on."""
    was, pace.on = pace.on, way
    if was is None or was == way:
        return
    behind, measured = pace.behind()
    if way == "columns":
        why, seen = measured, "behind"
    elif behind:
        why = seen = choice.reason
    else:
        why, seen = "the batches keep up: " + measured, "kept up"
    log = engine.__dict__.get("_lent_log")
    if (was, way, seen) in pace.said or log is None:
        return
    pace.said.add((was, way, seen))
    before = next(r.said for r in WAYS if r.name == was)
    log(f"{WORK}: {choice.said} from this batch on, where {before} went"
        f" before - {why}")
