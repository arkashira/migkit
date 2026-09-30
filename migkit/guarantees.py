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
