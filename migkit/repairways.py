"""The way a table's rows are repaired, chosen by how far it drifted and
what each way costs here (backlog P0-DEPTH 1).

A repair was one way for every table: the rows the check named, by key,
a batch at a time - a table ten rows apart and one ten million apart got
the same statements. It is a ladder now, climbed per table through the
one decision engine (`decide.attempt`, `Engine._attempt`):

* **rows by key** (`ROWS`): the engine's own row repair, as it was - each
  row the check named copied from the source or removed.
* **spans copied again** (`RANGES`): where the table has one integer key,
  each span of it that differs heavily is replaced by the source's the way
  the move copies a range - emptied and copied under one transaction a
  span on PostgreSQL and MySQL (`Engine.ONE_TRANSACTION_A_RANGE`), emptied
  and then copied batch by batch across two engines - and the rows named
  outside those spans go by key. The check's leaves say which spans
  differ and how many rows each side holds in them (`leaves.kept_leaves`);
  where the check named the rows, they fall into spans of the width the
  leaves use, each counted on both sides by the key's index. Where the
  leaves differ in too many places for their rows to be named, this is
  the way a repair has short of the whole table.
* **the table copied again whole** (`RELOAD`): by the fastest way the
  move's own table ladder offers (`Engine.move_table`), only where the hop
  lets a copy replace what a table holds (`target_rows: replace`) and
  nothing else writes the target - no tail or replication into it, no
  way back, no cutover begun.

**What each guarantees** (`GUARANTEES`, and `Rung.holds` in the decision
engine's words). All three are exact - what they leave is the source's
rows, and a fresh check of the table follows every repair and says so or
says what is left; a repair is never said to have worked without it -
and idempotent: run again, each ends where it ended. Each is put back by
a rollback through the one statement record (`repairlog`): the rows way
records every row it writes or removes with what the target held; the
spans way reads each span on both sides before it lands and records
every row that differs in it - its pre-image, key for key; the reload
keeps every row the table held beside its record (a snapshot) with the
table's digest before and after, and a rollback puts the snapshot back
only where the table still reads as the reload left it. A target whose
rows migkit cannot put back (`ROWS_PUT_BACK`) is repaired by rows alone,
as before.

**Chosen by predicted cost** (`RepairCosts`, the decision engine's cost
model, `costmodel.CostModel`). Each way is costed on the work it would do
here - the rows named; the rows in the spans copied, and the rows named
outside them by key; the table's rows - from a line fitted to every unit
it ran on this hop and, where it never ran, on other hops of the same
engines (the machine's knowledge). A way nothing has measured is
predicted from the move's own measured rate - the fastest table way's
bytes a second, else the fastest copy's rows a second - times what the
way does besides copying (`PRIOR`), its spread wide; a measurement always
beats it. The decision's record says which numbers were measured and
which predicted; the plan shows the outcome: what the target is to go
through, about how long, and how a rollback puts it back. A decision a
hop makes row by row (`on_conflict: keep-target`, `newer_wins`) and a
row filter keep a table to rows by key: a span or a table copied whole
would decide nothing and filter nothing.

**One rule for the target's triggers and foreign keys.** The spans and
the reload write inside the target engine's load window, the move's own
(`Engine.load_window`); the rows way's engines open the same window, and
a rollback puts rows back inside it too.
"""
import math
import time
from dataclasses import dataclass, field

from . import costmodel, decide

#: the ways, by names the machine's knowledge keeps their costs under
ROWS, RANGES, RELOAD = "repair rows", "repair spans", "repair table"
#: the work their costs are kept for, beside the hop's (`costmodel.HOP_FILE`)
WORK = "a table's repair"
PREFIX = "repair/"
#: the kinds of row a check names
KINDS = ("missing", "changed", "extra")

#: what a row of each way costs against a row the move copies in bulk,
#: where the way has never run here or on another hop of these engines -
#: set, not measured, and the prior's spread is half of it: a row by key
#: is looked up on both sides and written on its own share of a statement
#: (10), a row of a span is read on both sides for the record before it
#: is copied (3), a row of the table is read once for the snapshot before
#: it is copied (2)
PRIOR = {ROWS: 10.0, RANGES: 3.0, RELOAD: 2.0}
#: seconds a row copied in bulk where the hop has never copied a table -
#: set, not measured
BULK = 20e-6
#: bytes a row, for a table whose size the catalogue does not give -
#: set, not measured
ROW_BYTES = 200
#: rows one span holds at most: neighbouring spans that differ are joined
#: up to it, so a span's transaction stays bounded
SPAN_MOST = 200_000
#: rows read at once for a span's record or a table's snapshot
READ = 5000

#: what each way guarantees, in the words the plan says them in
GUARANTEES = {
    ROWS: {"exact": True, "idempotent": True,
           "reversible": "each row it writes or removes is recorded with"
                         " what the target held there"},
    RANGES: {"exact": True, "idempotent": True,
             "reversible": "each span is read on both sides before it is"
                           " replaced, and every row that differs in it is"
                           " recorded with what the target held"},
    RELOAD: {"exact": True, "idempotent": True,
             "reversible": "every row the table held is kept beside the"
                           " record, and put back only where the table"
                           " still reads as the copy left it"},
}
#: the same, in the decision engine's levels (`guarantees.LEVELS`): each
#: is proved by the check after it and converges run again - alike, so
#: the cost alone ranks them
HOLDS = {"correct": "proved", "idempotent": "converges"}


@dataclass(frozen=True)
class Sides:
    """One table as a repair reads and writes it: each side's engine, table
    and columns `[(name, class)]` in the pair's order, and the key by the
    source's names and by the target's."""
    src: object
    src_t: str
    sc: list
    dst: object
    dst_t: str
    dc: list
    key: list
    dkey: list


@dataclass
class Span:
    """[lo, hi) of the key, the rows the larger side holds in it, and the
    rows named in it - None where not every row in it was named."""
    lo: int
    hi: int
    rows: int
    named: object = None


@dataclass
class Outcome:
    """What one table's repair did, and what the check after it said."""
    table: str
    way: str = ROWS
    said: str = ""
    reason: str = ""
    #: {way: the cost predicted or measured before it ran, as evidence}
    predicted: dict = field(default_factory=dict)
    units: int = 0
    seconds: float = 0.0
    proof: str = ""
    proof_said: str = ""
    #: the decision in full (`decide.Choice.as_evidence`)
    choice: dict = field(default_factory=dict)

    def record(self):
        """The decision and its result, for the changelog and evidence -
        each way by what it does, never by its label."""
        said = {r.name: r.said for r in RUNGS}
        return {"table": self.table, "way": said.get(self.way, ""),
                "reason": self.reason,
                "predicted": {said.get(k, k): v
                              for k, v in self.predicted.items()},
                "units": self.units, "seconds": round(self.seconds, 3),
                "recheck": self.proof, "recheck_said": self.proof_said,
                "decision": self.choice}


_N = decide.Need

#: the ways, cheapest first where nothing is measured
RUNGS = (
    decide.Rung(
        ROWS, "the rows named, by key", holds=HOLDS,
        needs=(_N("rows named", lambda f: f["named"] > 0,
                  "the check did not name the rows that differ - it found"
                  " them in too many places to name"),)),
    decide.Rung(
        RANGES, "the spans of the key that differ, copied again whole",
        holds=HOLDS,
        needs=(_N("one integer key", lambda f: f["int key"],
                  "the table's key is not one integer column, so its rows"
                  " fall in no span of it"),
               _N("no decision row by row", lambda f: not f["per row"],
                  lambda f: f["per row"]),
               _N("no row filter", lambda f: not f["filter"],
                  "the hop filters the table's rows, and a span copied"
                  " whole would not keep to the filter"),
               _N("a span copied the move's way", lambda f: not f["range"],
                  lambda f: f["range"]),
               _N("a span that differs enough", lambda f: bool(f["spans"]),
                  lambda f: f["spans why"]))),
    decide.Rung(
        RELOAD, "the table copied again whole", holds=HOLDS, footprint=1,
        needs=(_N("no decision row by row", lambda f: not f["per row"],
                  lambda f: f["per row"]),
               _N("no row filter", lambda f: not f["filter"],
                  "the hop filters the table's rows, and a table copied"
                  " whole would not keep to the filter"),
               _N("replacing allowed", lambda f: f["replace"],
                  "the hop does not let a copy replace what a table holds"
                  " (target_rows: replace)"),
               _N("nothing else writes the target", lambda f: not
                  f["writers"],
                  lambda f: f"{f['writers']} writes the target, and a table"
                            " copied whole would take what it wrote"),
               _N("the table's copier", lambda f: f["copier"],
                  lambda f: f["copier why"]),
               _N("the table's size known", lambda f: f["rows"] is not None,
                  "the catalogue does not say how many rows the table"
                  " holds, so what copying it costs cannot be told"))),
)


class RepairCosts(costmodel.CostModel):
    """The ways' costs for one table: the decision engine's cost model,
    each way asked on the work it would do here (`Ladder.work`) - the
    spans way on its spans and on the rows named outside them by key -
    and, for a way nothing has measured, a prior from the move's own
    measured rate (`PRIOR`)."""

    def __init__(self, ladder):
        s = ladder.sides
        super().__init__(
            work=WORK, rungs=RUNGS,
            pair=f"{s.src.CANON_ENGINE}->{s.dst.CANON_ENGINE}",
            hop=ladder.target.hop, prefix=PREFIX)
        self.ladder = ladder

    def _structural(self, name):
        np = costmodel._np()
        mu = np.zeros(costmodel.D)
        # seconds a thousand rows, `features`' third
        mu[2] = PRIOR[name] * self.ladder._bulk() * 1000.0
        return mu, costmodel._spread(mu), "the move's own rate"

    def estimate(self, name, unit=None):
        """`name` on `unit` where one is asked for; else on the work it
        would do here, its parts' times added and their spreads with
        them - measured only where every part was."""
        if unit is not None:
            return super().estimate(name, unit)
        parts = [(w, u) for w, u in self.ladder.work(name) if u["rows"]]
        if not parts:
            return None
        got = [super(RepairCosts, self).estimate(w, u) for w, u in parts]
        if any(e is None for e in got):
            return None
        return costmodel.Estimate(
            sum(e.mean for e in got),
            math.sqrt(sum(e.sd * e.sd for e in got)),
            costmodel.MEASURED_TRUTH if all(e.measured for e in got)
            else costmodel.PREDICTED,
            "; ".join(sorted({e.source for e in got if e.source})),
            min(e.n for e in got), min(e.dof for e in got),
            max(e.noise for e in got))

    def marginal(self, name, unit):
        """Seconds `unit` adds to `name`'s work, its start paid once."""
        a = super().estimate(name, unit)
        b = super().estimate(name, {**unit, "rows": 0, "bytes": 0})
        if a is None or b is None:
            return None
        return max(a.mean - b.mean, 0.0)


class Ladder:
    """One table's repair (`action`, a `RepairAction` naming its `table`):
    its drift, the facts the ways' needs read - each read once, and only
    for a way still in the running - what each way would cost, and the
    way taken. `copy_table(sch, table, log)` copies a table the move's
    way, where the caller can: the reload's copy."""

    def __init__(self, eng, db, action, log=None, copy_table=None):
        self.eng, self.db, self.action = eng, db, action
        self.target = eng.columns_pair() if action.by == "pair" else eng
        self.table = action.table
        self.say = log or (lambda m: None)
        self.copy_table = copy_table
        self.undo_dir = self.target.hop.report_dir(db) / "undo"
        self.found = self.sides = self.kept = None
        self.why_not = ""
        try:
            self.found = self.target.repair_found(db, self.table)
            if not getattr(self.target, "ROWS_PUT_BACK", True):
                # nothing but its rows by key could be undone here
                self.why_not = ("this target's rows are put back by its own"
                                " record alone, row by row")
            elif self.found is not None:
                self.sides = self.target.repair_sides(db, self.table)
                self.kept = self.target.repair_leaves(db, self.table)
        except Exception as e:  # noqa: BLE001 - the rows way, said why
            if not decide.operational(e):
                raise
            self.why_not = decide._first(e)
        if not self.why_not and (self.found is None or self.sides is None):
            self.why_not = ("the two sides of the table cannot be lined up"
                            " by its key")
        self.facts = self._facts() if self.ready else None
        self.costs = RepairCosts(self) if self.ready else None

    @property
    def ready(self):
        return not self.why_not

    @property
    def named(self):
        return sum(len((self.found or {}).get(k) or ()) for k in KINDS)

    # --- the facts ---------------------------------------------------------

    def _facts(self):
        from . import interlock
        hop = self.target.hop
        return decide.Facts(
            {"named": self.named,
             "per row": self._per_row(),
             "replace": interlock.choice(hop) == "replace"},
            **{"int key": self._int_key,
               "filter": lambda: bool(hop.row_filter(
                   self.db, *str(self.sides.src_t).split("."))),
               "range": self._range_ready,
               "spans": lambda: self._spans()[0],
               "spans why": lambda: self._spans()[2],
               "writers": self._writers,
               "copier": lambda: self._move() is not None,
               "copier why": lambda: self.__dict__.get(
                   "_move_why", "the move has no copier for this table"),
               "rows": lambda: self._size()[0]})

    def _per_row(self):
        """Why the hop decides this table row by row, or ""."""
        t = self.target
        if (t.hop.options or {}).get("on_conflict") == "keep-target":
            return ("the hop keeps the rows the target changed (on_conflict:"
                    " keep-target), which only a repair row by row can tell"
                    " apart")
        col = t.newer_wins_column()
        if col:
            return (f"the hop keeps the newer row by {col} (newer_wins),"
                    " which only a repair row by row can tell apart")
        return ""

    def _int_key(self):
        s = self.sides
        return (len(s.key) == 1 and len(s.dkey) == 1
                and dict(s.sc).get(s.key[0]) == "integer"
                and dict(s.dc).get(s.dkey[0]) == "integer")

    def _range_ready(self):
        """Why a span cannot be copied the move's way here, or ""."""
        try:
            return self.target.repair_range_ready(self.db, self.sides) or ""
        except Exception as e:  # noqa: BLE001 - said as the reason
            if not decide.operational(e):
                raise
            return ("whether a span can be copied could not be read: "
                    + decide._first(e))

    def _writers(self):
        """What else writes the target, in words, or ""."""
        from . import cutover
        t = self.target
        opts = t.hop.options or {}
        out = []
        try:
            out += [w for w, _ in t.stream_writers(self.db)]
        except Exception as e:  # noqa: BLE001 - not ruled out, then
            if not decide.operational(e):
                raise
            out.append(f"a writer migkit could not rule out"
                       f" ({decide._first(e)})")
        if opts.get("two_way") or opts.get("reverse") \
                or str(opts.get("topology") or "") == "two_way":
            out.append("the way back to the source")
        rec = cutover.record(t.hop)
        if rec and rec.get("state") in ("running", "undoing", "flipped"):
            out.append("the application, since the cutover began")
        return ", ".join(out)

    def _size(self):
        """(rows, bytes) of the source's table as its catalogue says them,
        or its rows as the check's kept leaves counted them; None for what
        neither says."""
        if "_size_got" not in self.__dict__:
            s = self.sides
            rows = size = None
            try:
                got = s.src.table_facts("src", self.db) or {}
                fact = (got.get(s.src_t) or got.get(
                    str(s.src_t).split(".")[-1]) or {})
                rows, size = fact.get("rows"), fact.get("bytes")
            except Exception as e:  # noqa: BLE001 - not known, then
                if not decide.operational(e):
                    raise
            if rows is None and self.kept and self.kept.get("rows"):
                rows = int(self.kept["rows"])
            self.__dict__["_size_got"] = (
                None if rows is None else int(rows),
                None if size is None else int(size))
        return self.__dict__["_size_got"]

    def _row_bytes(self):
        rows, size = self._size()
        return (size / rows) if rows and size else ROW_BYTES

    def _move(self):
        """(schema, table) as the move copies it, or None, why said in
        `_move_why`."""
        if "_move_got" in self.__dict__:
            return self.__dict__["_move_got"]
        got = None
        leaf = str(self.table).split(".")[-1]
        try:
            listed = self.eng.list_move_tables(self.db)
        except Exception as e:  # noqa: BLE001 - said as the reason
            if not decide.operational(e):
                raise
            listed = []
            self.__dict__["_move_why"] = ("the move's tables could not be"
                                          " listed: " + decide._first(e))
        exact = [(sch, t) for sch, t in listed
                 if f"{sch}.{t}" == str(self.table) or t == self.table]
        pick = exact or [(sch, t) for sch, t in listed if t == leaf]
        if len(pick) == 1:
            got = pick[0]
        elif pick:
            self.__dict__["_move_why"] = (f"{leaf} is in more than one"
                                          " schema, and which of them the"
                                          " move copies cannot be told")
        self.__dict__["_move_got"] = got
        return got

    # --- what the work is, and what it costs --------------------------------

    def _bulk(self):
        """Seconds a row the move copies in bulk: this table's bytes a row
        over the fastest table way's bytes a second as the table ladder
        measured them for a table this size (`planner._bytes_rates`),
        else the fastest whole copy's rows a second, else `BULK`."""
        if "_bulk_got" in self.__dict__:
            return self.__dict__["_bulk_got"]
        from . import movers, planner
        hop = self.target.hop
        got = BULK
        rows, size = self._size()
        rates = (planner._bytes_rates(hop, {"bytes": size, "rows": rows or 0})
                 if size else None)
        if rates and rates[0]:
            got = self._row_bytes() / rates[0][-1]
        else:
            seen = [r for r in (planner.measured_rate(hop, via)
                                for via in movers.VIAS) if r]
            if seen:
                got = 1.0 / max(seen)
        self.__dict__["_bulk_got"] = got
        return got

    def _unit(self, rows):
        return {"rows": int(rows), "bytes": int(rows) * self._row_bytes(),
                "keyless": False}

    def work(self, way):
        """[(way, unit)] of what `way` would do here: the spans way is its
        spans, and the rows named outside them by key."""
        if way == ROWS:
            return [(ROWS, self._unit(self.named))]
        if way == RANGES:
            spans, left, _ = self._spans()
            return [(RANGES, self._unit(sum(p.rows for p in spans))),
                    (ROWS, self._unit(sum(len(v) for v in left.values())))]
        rows = self._size()[0]
        return [(RELOAD, self._unit(rows))] if rows is not None else []

    def predicted(self, way):
        """What `way` would take here (`costmodel.Estimate`), or None."""
        return self.costs.estimate(way)

    # --- the spans ---------------------------------------------------------

    def _spans(self):
        """(spans to copy again, the rows named outside them by kind, why
        there is none) - read once."""
        if "_spans_got" not in self.__dict__:
            try:
                self.__dict__["_spans_got"] = self._spans_read()
            except Exception as e:  # noqa: BLE001 - said as the reason
                if not decide.operational(e):
                    raise
                self.__dict__["_spans_got"] = (
                    [], self.found, "the spans of the key that differ could"
                    f" not be read: {decide._first(e)}")
        return self.__dict__["_spans_got"]

    def _spans_read(self):
        from . import leaves
        s = self.sides
        keys = sorted({int(k[0]) for kind in KINDS
                       for k in (self.found.get(kind) or ())
                       if k and k[0] is not None})
        kept = self.kept if (self.kept and self.kept.get("kind") == "range"
                             and self.kept.get("key") == list(s.key)) \
            else None
        if kept:
            base, width = int(kept["base"]), max(1, int(kept["width"]))
            sizes = {int(b): max(int(a), int(z))
                     for b, a, z in kept.get("leaves") or ()}
            complete = bool(kept.get("complete"))
        elif keys:
            a = leaves.Side(s.src, "src", self.db, s.src_t, list(s.sc),
                            list(s.key))
            b = leaves.Side(s.dst, "dst", self.db, s.dst_t, list(s.dc),
                            list(s.dkey))
            lohi = leaves.key_range(a, b)
            if lohi is None:
                return [], self.found, "neither side holds a row"
            spec = leaves.plan(self._size()[0] or len(keys), "range", *lohi)
            base, width = spec.base, spec.width
            want = {(k - base) // width for k in keys}
            ca, cb = leaves.both(leaves.range_counts, a, b, spec, want)
            sizes = {x: max(ca.get(x, 0), cb.get(x, 0)) for x in want}
            complete = True
        else:
            return [], self.found, ("the check named no row and kept no"
                                    " span of the key that differs")
        named = {}
        for k in keys:
            x = (k - base) // width
            named[x] = named.get(x, 0) + 1
        # a span is copied again where that costs less than its rows named
        # by key - each way's start paid once, not a span; a span the check
        # could not account for is copied whole: the rows named in it are
        # not all the rows that differ there
        heavy = []
        for x, rows in sizes.items():
            span = self.costs.marginal(RANGES, self._unit(max(rows, 1)))
            by_key = self.costs.marginal(ROWS, self._unit(named.get(x, 0)))
            if not complete or (span is not None and by_key is not None
                                and span < by_key):
                heavy.append(x)
        heavy.sort()
        if not heavy:
            return [], self.found, (
                "no span of the key differs enough: each holds more rows"
                " than copying it again saves over the rows named in it")
        runs, run = [], None
        for x in heavy:
            rows = max(sizes.get(x, 0), 1)
            if run and x == run[1] + 1 and run[2] + rows <= SPAN_MOST:
                run = [run[0], x, run[2] + rows, run[3] + named.get(x, 0)]
                continue
            if run:
                runs.append(run)
            run = [x, x, rows, named.get(x, 0)]
        runs.append(run)
        spans = [Span(base + a * width, base + (z + 1) * width, rows,
                      n if complete else None) for a, z, rows, n in runs]
        inside = set(heavy)
        left = {kind: [k for k in (self.found.get(kind) or ())
                       if k and k[0] is not None
                       and (int(k[0]) - base) // width not in inside]
                for kind in KINDS}
        return spans, left, ""

    # --- the choice --------------------------------------------------------

    def holding(self):
        """The ways whose needs hold here, and those passed over with why."""
        return decide.prune(RUNGS, self.facts)

    def order(self):
        """The ways in the order the climb takes them: those that hold by
        what each is predicted or measured to cost on its work here, then
        those that do not."""
        kept, out = self.holding()
        return decide.rank(kept, self.costs) + [r for r, _ in out]

    def foreseen(self):
        """The plan's line: the way that would be taken, what it does to
        the target, about how long, and how a rollback puts it back."""
        if not self.ready:
            return (f"{self.table}: {self.named:,} rows to be repaired by"
                    f" key ({self.why_not})")
        got = decide.climb(f"{self.db}.{self.table}: the repair", RUNGS,
                           self.facts, costs=self.costs, prove=False)
        if got.rung is None:
            return (f"{self.table}: no way of repairing it holds here -"
                    f" {got.reason}")
        p = self.predicted(got.path)
        return (f"{self.table}: {self._what(got.path)}"
                + (f", about {p.said()}" if p is not None else "")
                + f"; {GUARANTEES[got.path]['reversible']}")

    def _what(self, way, done=False):
        """What `way` does to the target, in the plan's words."""
        s = self.sides
        verb = "copied" if done else "to be copied"
        if way == ROWS:
            return (f"{self.named:,} rows "
                    f"{'repaired' if done else 'to be repaired'} by key")
        if way == RANGES:
            spans, left, _ = self._spans()
            rows = sum(p.rows for p in spans)
            n = sum(len(v) for v in left.values())
            how = ("each in one transaction"
                   if getattr(self.target, "ONE_TRANSACTION_A_RANGE", False)
                   else "each emptied, then copied a batch at a time")
            return (f"{len(spans):,} spans of {s.key[0]} {verb} again whole"
                    f" ({rows:,} rows, {how})"
                    + (f", and {n:,} rows by key" if n else ""))
        rows = self._size()[0]
        return (f"the table {verb} again whole"
                + (f" ({rows:,} rows)" if rows is not None else "")
                + " by the move's fastest way (target_rows: replace)")

    # --- the repair --------------------------------------------------------

    def run(self, prove=True):
        """The table repaired by the way climbed for it, switched from by
        itself where one fails as it runs; then, where `prove`, checked
        afresh. Returns the `Outcome`."""
        out = Outcome(self.table)
        began = time.monotonic()
        if not self.ready:
            self.eng.apply(self.db, self.action)
            out.said = f"{self.named:,} rows repaired by key"
            out.reason = self.why_not
        elif not self.holding()[0]:
            # said for this table, and the next one repaired: the check
            # after it says what is left
            out.way = ""
            out.said = out.reason = (
                "not repaired - no way of repairing it holds here: "
                + "; ".join(f"{r.said}: {why}"
                            for r, why in self.holding()[1]))
        else:
            # what was known when the way was chosen, before its own time
            # is kept
            for r in self.holding()[0]:
                p = self.predicted(r.name)
                if p is not None:
                    out.predicted[r.name] = p.as_evidence()
            try:
                got, units = self.target._attempt(
                    f"{self.db}.{self.table}: the repair", RUNGS,
                    self._run, self.facts, log=self.say, costs=self.costs)
            finally:
                self.costs.save()
            out.way, out.reason, out.units = got.path, got.reason, units
            out.choice = got.as_evidence()
            out.said = self._what(got.path, done=True)
        out.seconds = time.monotonic() - began
        if prove:
            out.proof, out.proof_said = recheck(self.target, self.db,
                                                self.table)
        return out

    def _run(self, choice):
        """One way of the climb, its time kept with the cost model."""
        if choice.path == RANGES:
            return self._copy_spans()
        began = time.monotonic()
        if choice.path == ROWS:
            self._by_key(None)
            units = self.named
        else:
            units = self._reload()
        self.costs.saw(choice.path, units, time.monotonic() - began,
                       unit=self._unit(units))
        return units

    def _by_key(self, only):
        """The engine's own repair of the rows named - of those among them
        in `only` (canonical key texts) where given: the one set each
        engine's row path reads its keys through (`_repair_only`)."""
        t = self.target
        if only is not None:
            t.__dict__["_repair_only"] = {tuple(k) for k in only}
        try:
            self.eng.apply(self.db, self.action)
        finally:
            t.__dict__.pop("_repair_only", None)

    def _copy_spans(self):
        """Every span that differs copied again, each span's rows that
        differ recorded before it lands (its pre-image); then the rows
        named outside them by key. The spans are timed as the spans way,
        the rows by key as the rows way. Returns the rows done."""
        from . import repairlog
        s = self.sides
        spans, left, _ = self._spans()
        began = time.monotonic()
        record = repairlog.Record(self.undo_dir, s.dst_t, "canon", s.dc,
                                  s.dkey)
        rows = 0
        try:
            with self.target.load_window(self.db, self.say, {s.dst_t}):
                for p in spans:
                    self._pre_image(record, p)
                    record.ready()
                    self.target.repair_range(self.db, s, p.lo, p.hi,
                                             self.say)
                    record.applied(record.n)
                    rows += p.rows
        finally:
            record.close()
            if not record.n:
                record.path.unlink(missing_ok=True)
        self.costs.saw(RANGES, rows, time.monotonic() - began,
                       unit=self._unit(rows))
        n = sum(len(v) for v in left.values())
        if n:
            began = time.monotonic()
            self._by_key([k for v in left.values() for k in v])
            self.costs.saw(ROWS, n, time.monotonic() - began,
                           unit=self._unit(n))
        return rows + n

    def _pre_image(self, record, span):
        """Every row of `span` that differs, recorded with what the target
        holds (before) and what the source does (after): both sides read in
        key order and merged, `READ` rows at a time."""
        from . import canon, repairlog
        s = self.sides
        src = _span_rows(s.src, "src", self.db, s.src_t, s.sc, s.key[0],
                         span)
        dst = _span_rows(s.dst, "dst", self.db, s.dst_t, s.dc, s.dkey[0],
                         span)
        cls = dict(s.dc)[s.dkey[0]]
        a, b = next(src, None), next(dst, None)
        while a is not None or b is not None:
            if b is None or (a is not None and a[0] < b[0]):
                k, before, after = a[0], None, a[1]
                a = next(src, None)
            elif a is None or b[0] < a[0]:
                k, before, after = b[0], b[1], None
                b = next(dst, None)
            else:
                k, before, after = a[0], b[1], a[1]
                a, b = next(src, None), next(dst, None)
            was = (repairlog.canon_rows(s.dc, [before])[0]
                   if before is not None else None)
            will = (repairlog.canon_rows(s.sc, [after])[0]
                    if after is not None else None)
            if was == will:
                continue
            record.planned("write" if will is not None else "delete",
                           (canon.render_value(cls, k),), was, will)

    def _reload(self):
        """The table copied again whole by the move's own copier, its rows
        kept first (the snapshot) and its digest read before and after it.
        Returns the table's rows."""
        from . import repairlog
        if self.copy_table is None:
            raise decide.Blocked("no copy of a table runs here")
        s = self.sides
        record = repairlog.Record(self.undo_dir, s.dst_t, "snapshot", s.dc,
                                  s.dkey)
        try:
            snap = self.undo_dir / repairlog.snapshot_name(record.path)
            before = repairlog.table_digest(s.dst, "dst", self.db, s.dst_t,
                                            s.dc)
            kept = repairlog.snapshot(s.dst, self.db, s.dst_t, s.dc, s.dkey,
                                      snap)
            intended = repairlog.table_digest(s.src, "src", self.db,
                                              s.src_t, s.sc)
            record.planned("reload", (str(s.dst_t),),
                           {"digest": before, "snapshot": snap.name,
                            "rows": kept}, {"digest": intended})
            record.ready()
            sch, tbl = self._move()
            self.copy_table(sch, tbl, self.say)
            record.landed(1, {"digest": repairlog.table_digest(
                s.dst, "dst", self.db, s.dst_t, s.dc)})
            record.applied(1)
        finally:
            record.close()
        return self._size()[0] or 0


def _span_rows(eng, side, db, table, cols, key, span):
    """(key as an integer, row) of one side's rows in `span`, in key
    order, `READ` at a time."""
    q = eng._quote_ident(key)
    where = f"{q} >= {int(span.lo)} and {q} < {int(span.hi)}"
    at = [n for n, _ in cols].index(key)
    after = None
    while True:
        rows, after = eng.neutral_read(side, db, table, cols, after, READ,
                                       where=where)
        for r in rows:
            yield int(r[at]), r
        if not rows or after is None:
            return


def recheck(target, db, table):
    """(verdict, what it said) of a fresh check of one table: "ok" only
    where every data verdict about it says so."""
    try:
        got = target.check_data(db, table)
    except Exception as e:  # noqa: BLE001 - said, never taken for ok
        if not decide.operational(e):
            raise
        return "error", f"the check after the repair could not run:" \
                        f" {decide._first(e)}"
    data = [r for r in got if getattr(r, "check", "") == "data"] or got
    if not data:
        return "error", "the check after the repair said nothing of the" \
                        " table"
    for status in ("diff", "error"):
        worst = next((r for r in data if r.status == status), None)
        if worst is not None:
            return status, worst.detail
    if any(r.status == "ok" for r in data) and all(
            r.status in ("ok", "warn") for r in data):
        return "ok", next(r.detail for r in data if r.status == "ok")
    first = data[0]
    return first.status, first.detail


def said(outcome):
    """The console's line for one table's repair: what happened, and what
    the check after it said."""
    line = f"{outcome.table}: {outcome.said}"
    if not outcome.proof:
        return line
    if outcome.proof == "ok":
        return f"{line} - checked again: the same on both sides"
    if outcome.proof == "diff":
        return f"{line} - checked again: still differs ({outcome.proof_said})"
    return (f"{line} - checked again: {outcome.proof}"
            f" ({outcome.proof_said})")
