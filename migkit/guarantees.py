"""What a way guarantees, and what a whole strategy of ways does (backlog
P0-SMART, the owner's note C of 2026-09-30: "guarantees never traded").

Correctness, consistency, idempotency, exactly-once delivery and going on
after a stop are not preferences a faster way may give up. Each is a
ladder of levels (`LEVELS`, weakest first); a rung says the level it
holds of each (`Rung.holds`, or read from what it gives, `derived`); a
strategy - a rung for each part of the work - holds, of each guarantee,
the weakest of what its parts hold (the meet of a product of chains), and
then what migkit tops up: a part may lift a guarantee to a level where
the strategy already holds what the lift stands on (`Lift`) - a digest
of every row after a bulk program that proves nothing lifts correctness
to "proved"; a repair by key needs writes that converge. Lifts only
raise, so their fixed point is reached in at most one pass a lift.

The strongest the engines allow is the lexicographic best of every
strategy that fits (`strongest`), the guarantees in `ORDER` - a stronger
proof first, then consistency, then a re-run converging, then exactly
once, then going on after a stop. Only strategies that reach it are
costed (`reaching`); a faster one that does not is excluded, and said.

How much a stop throws away (a range's work, or half the table's) is not
a guarantee but a cost, and enters a way's expected time with its chance
of failing (`lost`, `costmodel.risked`): both "goes on" by range and by
table converge; by range loses less.
"""
from dataclasses import dataclass

#: each guarantee's levels, weakest first
LEVELS = {
    "correct": ("unproved", "counted", "proved", "byte exact"),
    "consistent": ("none", "per range", "as of one moment"),
    "idempotent": ("no", "converges"),
    "exactly once": ("at least once", "exactly once"),
    "resumable": ("starts over", "goes on"),
}
#: the order a stronger guarantee counts in, the first the most
ORDER = ("correct", "consistent", "idempotent", "exactly once", "resumable")

#: what migkit's capabilities (`decide.WORDS`) already say of a guarantee
DERIVED = {"byte exact": ("correct", "byte exact"),
           "exact": ("exactly once", "exactly once"),
           "consistent as of": ("consistent", "as of one moment"),
           "resume by key": ("resumable", "goes on"),
           "resume by chunk": ("resumable", "goes on")}

#: how each level is said, where the plan names what a strategy reaches
WORDS = {("correct", "proved"): "every row proved",
         ("correct", "byte exact"): "every row proved byte for byte",
         ("correct", "counted"): "rows counted, not proved",
         ("consistent", "as of one moment"): "read as of one moment",
         ("consistent", "per range"): "each range read as of its own"
                                      " moment",
         ("idempotent", "converges"): "a re-run converges",
         ("exactly once", "exactly once"): "no batch applied twice",
         ("resumable", "goes on"): "goes on after a stop"}


def level(dim, name):
    """The level's rank on its ladder."""
    try:
        return LEVELS[dim].index(name)
    except (KeyError, ValueError):
        raise ValueError(f"{dim}: no level {name!r}") from None


@dataclass(frozen=True)
class Lift:
    """A part that raises `dim` to `to` in any strategy that already holds
    each level `stands_on` names ((dim, level), ...)."""
    dim: str
    to: str
    stands_on: tuple = ()

    def applies(self, got):
        return all(got[d] >= level(d, lv) for d, lv in self.stands_on)


def declared(rung):
    """{dim: rank} the rung says itself (`holds`), then what its
    capabilities say (`DERIVED`) where it says nothing of that one."""
    out = {}
    for cap in getattr(rung, "gives", ()) or ():
        got = DERIVED.get(cap)
        if got is not None:
            d, lv = got
            out[d] = max(out.get(d, 0), level(d, lv))
    for d, lv in getattr(rung, "holds", ()) or ():
        out[d] = level(d, lv)
    return out


def explicit(rung):
    """Whether the rung says its guarantees itself."""
    return bool(getattr(rung, "holds", ()) or getattr(rung, "lifts", ()))


def of(rungs):
    """{dim: rank} a strategy of `rungs` holds: of each guarantee the
    weakest any part holds (a part that says nothing of one leaves it to
    the others; none saying anything is its weakest level), then every
    lift whose ground holds, to a fixed point."""
    rungs = list(rungs)
    said = [declared(r) for r in rungs]
    got = {}
    for d in ORDER:
        levels = [s[d] for s in said if d in s]
        got[d] = min(levels) if levels else 0
    lifts = [x for r in rungs for x in getattr(r, "lifts", ()) or ()]
    changed = True
    while changed:
        changed = False
        for x in lifts:
            to = level(x.dim, x.to)
            if got[x.dim] < to and x.applies(got):
                got[x.dim] = to
                changed = True
    return got


def vector(got):
    """The levels in `ORDER`, what strategies are compared by."""
    return tuple(got[d] for d in ORDER)


def strongest(candidates):
    """The lexicographically strongest vector among `candidates`
    ({dim: rank} each), or None where there are none."""
    vs = [vector(c) for c in candidates]
    return max(vs) if vs else None


def reaching(items, levels_of):
    """(the items whose guarantees reach the strongest any of them does,
    [(item, the first guarantee it falls short on)] of the rest)."""
    items = list(items)
    got = [(i, levels_of(i)) for i in items]
    best = strongest(g for _, g in got)
    keep, out = [], []
    for i, g in got:
        if vector(g) >= best:
            keep.append(i)
        else:
            out.append((i, short(g, best)))
    return keep, out


def short(got, best):
    """The first guarantee `got` holds less of than `best`, in words."""
    for k, d in enumerate(ORDER):
        if got[d] < best[k]:
            return (f"{LEVELS[d][got[d]]} where {LEVELS[d][best[k]]} is"
                    f" reachable here ({d})")
    return ""


def said(got):
    """What a strategy reaches, in the plan's words."""
    out = [WORDS[(d, LEVELS[d][got[d]])] for d in ORDER
           if (d, LEVELS[d][got[d]]) in WORDS]
    return ", ".join(out) or "nothing beyond the rows carried"


def named(got):
    """{dim: level name}, for the evidence."""
    return {d: LEVELS[d][got[d]] for d in ORDER}


def lost(rung, ranges=1):
    """The share of a unit's work a failure throws away: a range's where
    the rung goes on by range, half of it where it starts over."""
    got = declared(rung)
    if got.get("resumable", 0) >= level("resumable", "goes on"):
        return 0.5 / max(int(ranges or 1), 1)
    return 0.5


def optimistic(parts_chosen, parts_left):
    """{dim: rank} no strategy completing `parts_chosen` (rungs) with one
    rung from each of `parts_left` (lists of rungs) can pass: the chosen
    parts' weakest, the parts left at their strongest, every lift of any
    of them - a bound the strategy space is pruned by
    (`decide.space`)."""
    said_ = [declared(r) for r in parts_chosen]
    got = {}
    for d in ORDER:
        levels = [s[d] for s in said_ if d in s]
        for part in parts_left:
            best = [declared(r).get(d) for r in part]
            if all(b is not None for b in best) and best:
                levels.append(max(best))
        got[d] = min(levels) if levels else len(LEVELS[d]) - 1
    lifts = [x for r in parts_chosen for x in getattr(r, "lifts", ()) or ()]
    lifts += [x for part in parts_left for r in part
              for x in getattr(r, "lifts", ()) or ()]
    for x in lifts:
        got[x.dim] = max(got[x.dim], level(x.dim, x.to))
    return got


# --- what a move must keep, read before it runs (P0-SAFETY-INTEGRITY) --------
#
# A guarantee above is what a way holds; what a table's move must keep is
# what the source has and the target can hold of it: its exact columns,
# each value's class, NULL apart from an empty text, a field not there
# apart from NULL, its collation, duplicates where it has no key, the
# tables its rows point at, the rows a delete changes with it, the order
# its changes need and its shape when read. Each is read from the
# catalogue (`catalog.Catalog`) and the engines' own facts, never guessed;
# what the target cannot keep is a `Gap`, said, never carried lossily.
# A schedule (`Schedule`) is admitted only where it keeps them
# (`decide.admit`): a child table after its parents where the keys hold
# during the load, the tables a key ties read as of one moment, changes
# a key or a transaction ties applied in their order, a position moved
# only after what it stands for is on the target's disk.

#: what a table's move must keep, in the order it is said
KEEPS = ("scope", "values", "null and empty", "absent", "encoding",
         "duplicates", "dependencies", "atomicity", "order", "deletes",
         "schema")

#: delete and update actions that change another table's rows with it
_CASCADES = ("cascade", "set null", "set default")

_TEXT_WORDS = ("char", "text", "string", "clob")


@dataclass(frozen=True)
class Gap:
    """What `table`'s move cannot keep (`what`, one of `KEEPS`), and why."""
    table: str
    what: str
    why: str

    def said(self):
        return f"{self.table}: {self.what} - {self.why}"


@dataclass(frozen=True)
class Semantics:
    """What one table's move must keep, read before the move."""
    table: str
    #: the exact columns, in the source's order
    columns: tuple = ()
    #: ((column, its class, or its declared type where no class is read))
    classes: tuple = ()
    #: the text columns where an empty text and NULL are two values
    empty_apart: tuple = ()
    #: whether the source says "not there" apart from NULL
    absent: bool = False
    #: ((column, collation), ...) the source declares
    collations: tuple = ()
    #: the primary key, () where it has none
    key: tuple = ()
    #: no key and no unique index: equal rows are each a row of their own
    duplicates: bool = False
    #: the tables its foreign keys point at, itself left out
    parents: tuple = ()
    #: it points at itself: a row may need another of its own first
    self_ref: bool = False
    #: the parents whose delete or update changes its rows in the same
    #: transaction
    atomic_with: tuple = ()
    #: the order its changes need: "per key", "as logged" (no key: a
    #: change finds its row by every value), "none" where only copied
    order: str = "none"
    #: changes followed: a delete on the source is one on the target
    deletes: bool = False
    #: the catalogue's fingerprint of it when read: evidence read against
    #: another shape is not evidence of this one
    fingerprint: str = ""
    gaps: tuple = ()

    def needs_order(self):
        return self.order != "none"


def _texty(col):
    t = f"{col.cls or ''} {col.declared or ''}".lower()
    return any(w in t for w in _TEXT_WORDS)


def _fact(engine, name):
    return bool(getattr(engine, name, False)) if engine is not None \
        else False


def read(cat, table, src=None, dst=None, dst_cat=None, tables=None,
         tail=False):
    """The `Semantics` of `table` from the source's catalogue `cat`, the
    two engines' facts and, where read, the target's catalogue `dst_cat`;
    `tables` is the hop's scope, `tail` whether its changes are followed."""
    cols = list(cat.columns.get(table) or [])
    key = tuple(cat.keys.get(table) or ())
    unique = any(i.unique for i in cat.indexes.get(table) or [])
    fks = list(cat.fks.get(table) or [])
    gaps = []
    parents = tuple(sorted({f.ref_table for f in fks
                            if f.ref_table != table}))
    atomic = tuple(sorted({f.ref_table for f in fks
                           if f.ref_table != table and (
                               (f.on_delete or "").lower() in _CASCADES
                               or (f.on_update or "").lower() in _CASCADES)}))
    for p in parents:
        if tables is not None and p not in tables:
            gaps.append(Gap(table, "dependencies",
                            f"its rows point at {p}, which the hop leaves"
                            " out: they would land without the rows they"
                            f" need - take {p} into the scope or leave"
                            f" {table} out"))
    empty_apart = ()
    if not _fact(src, "EMPTY_IS_NULL"):
        empty_apart = tuple(c.name for c in cols if _texty(c))
    if empty_apart and _fact(dst, "EMPTY_IS_NULL"):
        gaps.append(Gap(table, "null and empty",
                        f"{', '.join(empty_apart)} hold an empty text apart"
                        " from NULL, and the target stores an empty text as"
                        " NULL - say which value an empty text becomes"))
    absent = _fact(src, "EXPRESSES_ABSENT")
    if absent and dst is not None and not _fact(dst, "EXPRESSES_ABSENT"):
        gaps.append(Gap(table, "absent",
                        "a field that is not there and one that is NULL"
                        " are two on the source and one on the target -"
                        " carry the field's absence in a column of its own"
                        " or accept it as NULL"))
    duplicates = not key and not unique
    there = dst_cat.columns.get(table) if dst_cat is not None else None
    if dst_cat is not None and there is None:
        gaps.append(Gap(table, "scope", "the target has no such table"))
    if there is not None:
        now = {c.name: c for c in there}
        for c in cols:
            d = now.get(c.name)
            if d is None:
                gaps.append(Gap(table, "scope",
                                f"column {c.name} is not on the target"))
                continue
            if c.cls and d.cls and c.cls != d.cls:
                gaps.append(Gap(table, "values",
                                f"column {c.name} is {c.cls} on the source"
                                f" and {d.cls} on the target"))
            if c.nullable and not d.nullable:
                gaps.append(Gap(table, "values",
                                f"column {c.name} takes NULL on the source"
                                " and not on the target"))
            if (c.collation and d.collation and _texty(c)
                    and c.collation.lower() != d.collation.lower()):
                gaps.append(Gap(table, "encoding",
                                f"column {c.name} compares by {c.collation}"
                                f" on the source and {d.collation} on the"
                                " target: equal and ordered text differ"))
        tkey = tuple(dst_cat.keys.get(table) or ())
        tunique = any(i.unique for i in dst_cat.indexes.get(table) or [])
        if duplicates and (tkey or tunique):
            gaps.append(Gap(table, "duplicates",
                            "it has no key and may hold equal rows, and the"
                            " target's key or unique index refuses or folds"
                            " them - drop that index for the move or give"
                            " the source a key"))
    order = "none"
    if tail:
        order = "per key" if key or unique else "as logged"
    return Semantics(
        table=table, columns=tuple(c.name for c in cols),
        classes=tuple((c.name, c.cls or str(c.declared or "")) for c in cols),
        empty_apart=empty_apart, absent=absent,
        collations=tuple((c.name, c.collation) for c in cols
                         if c.collation),
        key=key, duplicates=duplicates, parents=parents,
        self_ref=any(f.ref_table == table for f in fks), atomic_with=atomic,
        order=order, deletes=bool(tail),
        fingerprint=cat.fingerprint(table) if cols else "",
        gaps=tuple(gaps))


def read_hop(cat, src=None, dst=None, dst_cat=None, tables=None,
             tail=False):
    """{table: Semantics} of every table of `cat` the hop's scope
    (`tables`, every one where None) takes."""
    names = [t for t in cat.tables if tables is None or t in tables]
    scope = set(names) if tables is not None else None
    return {t: read(cat, t, src, dst, dst_cat, scope, tail) for t in names}


def changed_since(semantics, cat):
    """The tables whose shape in `cat` is not the one their semantics were
    read from: what was proved of them is not proof of them now."""
    return sorted(t for t, s in semantics.items()
                  if s.fingerprint and (t not in cat.tables
                                        or cat.fingerprint(t)
                                        != s.fingerprint))


def barriers(semantics):
    """([wave], [cycle]): the tables in waves, each table in a wave after
    every wave holding a table it points at; tables pointing at each other
    round a cycle share one wave. Each wave and cycle a sorted tuple."""
    names = sorted(semantics)
    parents = {t: {p for p in semantics[t].parents if p in semantics}
               for t in names}
    # strongly connected parts (Tarjan), so a cycle is one node
    index, low, stack, on, parts = {}, {}, [], set(), []

    def visit(v):
        index[v] = low[v] = len(index)
        stack.append(v)
        on.add(v)
        for w in sorted(parents[v]):
            if w not in index:
                visit(w)
                low[v] = min(low[v], low[w])
            elif w in on:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            part = []
            while True:
                w = stack.pop()
                on.discard(w)
                part.append(w)
                if w == v:
                    break
            parts.append(tuple(sorted(part)))

    for v in names:
        if v not in index:
            visit(v)
    of = {t: p for p in parts for t in p}
    depth = {}

    def level_of(p):
        if p not in depth:
            ups = {of[u] for t in p for u in parents[t]} - {p}
            depth[p] = 1 + max((level_of(u) for u in ups), default=-1)
        return depth[p]

    waves = {}
    for p in parts:
        waves.setdefault(level_of(p), []).extend(p)
    cycles = [p for p in parts if len(p) > 1]
    return ([tuple(sorted(waves[k])) for k in sorted(waves)],
            sorted(cycles))


@dataclass(frozen=True)
class Schedule:
    """How a move's work runs side by side: what a schedule decides, each
    None where this schedule does not decide it."""
    #: the tables in the order they start
    order: tuple = ()
    #: ((table, start, copied), ...) where the schedule is timed
    times: tuple = ()
    #: the tables copied at once
    at_once: int = 1
    #: "during": the target's foreign keys hold while rows load; "after":
    #: taken off for the load and put back once every table is in
    fks: str = None
    #: "one": every table read as of one moment or position; "per table",
    #: "per range": each as of its own
    snapshot: str = None
    #: a tail from a position before every read carries what changed
    #: while the tables were read, deletes too
    caught_up: bool = False
    #: "after durable": a position or a range's mark moves once what it
    #: stands for is on the target's disk; "before durable": earlier
    checkpoint: str = None
    #: how followed changes are applied: "in commit order", "by
    #: transaction" (lanes cut at commits, a transaction whole in one),
    #: "by key" (lanes by a row's key), "unordered"
    apply: str = None
    #: what a delete on the source while the move runs becomes: "carried",
    #: "frozen" (the source takes no writes), "dropped"
    deletes: str = None
    #: what a change of shape while the move runs does: "stop", "carry",
    #: "ignore"
    ddl: str = None
    #: (table, what) gaps the operator answered: kept as said, not lost
    accepted: tuple = ()

    def timed(self):
        return {t: (start, end) for t, start, end in self.times or ()}


def _linked(semantics):
    out = {t for t, s in semantics.items()
           if any(p in semantics for p in s.parents)}
    return out | {p for t in out for p in semantics[t].parents
                  if p in semantics}


def _out_of_order(schedule, semantics, cycles):
    in_cycle = {t: c for c in cycles for t in c}
    out = []
    at = schedule.timed()
    pos = {t: i for i, t in enumerate(schedule.order or ())}
    for t, s in sorted(semantics.items()):
        for p in s.parents:
            if p in in_cycle.get(t, ()):
                continue
            if at and t in at and p in at:
                start, done = at[t][0], at[p][1]
                if start is not None and done is not None \
                        and start + 1e-9 < done:
                    out.append(f"{t} starts before {p}, which its rows"
                               " point at, is in, and the target's keys"
                               f" hold - load it after {p}")
            elif not at and t in pos and p in pos and pos[t] < pos[p]:
                out.append(f"{t} is ordered before {p}, which its rows"
                           " point at, and the target's keys hold - load"
                           f" it after {p}")
            elif not at and t in pos and p in pos \
                    and (schedule.at_once or 1) > 1:
                out.append(f"{t} may load beside {p}, which its rows point"
                           " at, and the target's keys hold - a barrier"
                           f" after {p} is owed")
    return out


def breaks(schedule, semantics):
    """[why] the schedule does not keep what `semantics` ({table:
    Semantics}) says the move must keep, each saying what to do next -
    [] where it keeps all of it."""
    out = []
    accepted = {tuple(a) for a in schedule.accepted or ()}
    for s in semantics.values():
        for g in s.gaps:
            if (g.table, g.what) not in accepted:
                out.append(g.said())
    linked = _linked(semantics)
    if schedule.fks == "during":
        _, cycles = barriers(semantics)
        for c in cycles:
            out.append(f"{', '.join(c)} point at each other and the"
                       " target's keys hold during the load: no order"
                       " loads them - take the keys off for the load")
        out += _out_of_order(schedule, semantics, cycles)
    if schedule.snapshot in ("per table", "per range") and linked \
            and not schedule.caught_up:
        out.append(f"{', '.join(sorted(linked))} are tied by foreign keys"
                   " and read each as of its own moment: a child read"
                   " later than its parent points at rows the parent's"
                   " read did not see - read them as of one moment, or"
                   " follow the changes from a position before every read")
    if schedule.checkpoint == "before durable":
        out.append("the position moves before what it stands for is on the"
                   " target's disk: a stop between them loses changes -"
                   " move it after the target's commit")
    if schedule.apply in ("by key", "unordered"):
        tied = sorted(t for t in linked if semantics[t].needs_order())
        if tied:
            out.append(f"changes to {', '.join(tied)} are tied by foreign"
                       f" keys and applied {schedule.apply}: a child's"
                       " insert can pass its parent's - cut lanes at"
                       " commits so a transaction applies whole")
        loose = sorted(t for t, s in semantics.items()
                       if s.order == "as logged")
        if loose:
            out.append(f"{', '.join(loose)} have no key: a change finds its"
                       " row by every value, so two changes to equal rows"
                       f" applied {schedule.apply} can change the wrong one"
                       " - apply them in commit order")
        atomic = sorted(t for t, s in semantics.items() if s.atomic_with)
        if atomic and schedule.apply == "unordered":
            out.append(f"a delete or update of the rows {', '.join(atomic)}"
                       " point at changes theirs in the same transaction:"
                       " applied unordered the target holds half of it -"
                       " apply by transaction")
    if schedule.deletes == "dropped":
        out.append("a row deleted on the source while the move runs stays"
                   " on the target - follow the changes or stop the"
                   " source's writes for the move")
    if schedule.ddl == "ignore":
        out.append("a change of shape while the move runs would be"
                   " carried into rows read under the old shape - stop on"
                   " it or carry it")
    return out


#: the faults a move is tried against (`failpoint`), each with what must
#: hold after it: the run after ends equal, or the move stops saying why
FAULTS = {
    "crash after commit, before the mark": "the run after finds the range"
    " on the target and does not apply it twice",
    "crash after the mark, before the checkpoint": "the run after goes on"
    " from the mark and ends equal",
    "crash before the tail's position moves": "the changes after the"
    " position are applied once each",
    "duplicate delivery": "a change applied twice leaves the row as once",
    "out-of-order dependent events": "a schedule that would apply a"
    " child's change before its parent's is not admitted",
    "concurrent delete during copy": "a row deleted while the table is read"
    " is not on the target at the end",
    "DDL during copy": "the move stops saying which table changed shape",
    "partial batch": "a batch stopped half way is applied whole or not at"
    " all",
}
_SAFE = "tests/test_a_move_keeps_what_it_must.py::"
#: the faults a test injects, by the test that does
SAFETY_PROVED = {
    "crash after commit, before the mark":
        _SAFE + "test_a_move_with_foreign_keys_stopped_anywhere_ends_equal",
    "crash after the mark, before the checkpoint":
        _SAFE + "test_a_move_with_foreign_keys_stopped_anywhere_ends_equal",
    "partial batch":
        _SAFE + "test_a_move_with_foreign_keys_stopped_anywhere_ends_equal",
    "out-of-order dependent events":
        _SAFE + "test_lanes_by_key_over_tied_tables_are_not_admitted",
    "concurrent delete during copy":
        _SAFE + "test_rows_deleted_between_a_stop_and_the_run_after_are_gone",
}
#: the faults whose test is owed, and why
SAFETY_UNPROVED = {
    "crash before the tail's position moves": "proved per engine by the"
    " tail's own crash tests; not yet with foreign keys across tables",
    "duplicate delivery": "the tail's numbered batches skip a batch seen"
    " twice; a change delivered twice inside one batch is not injected yet",
    "DDL during copy": "the tail stops on a change of shape; a change of"
    " shape between a copy's ranges is not injected yet",
}
