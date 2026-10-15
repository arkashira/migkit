"""The routing and partition math of many-node topologies (backlog
R3.topo), decided in `scenarios.topology_case`: whether the key spaces of
many sources are apart (`overlaps`), where one source's row goes under a
routing rule (`Route`), whether ranges cover a key's domain once
(`range_gaps`), and the proofs that the parts equal the whole: a union
(`prove_union`), a partition (`prove_partition`), nodes converged
(`prove_converged`), each source's changes in its order (`out_of_order`)
and no change come back round (`echoes`).

Rows are compared as {key: digest}: a key a tuple of its texts, a digest
whatever the verify hashed the row to.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from math import gcd

from .reshard import hash_place


@dataclass(frozen=True)
class KeySpace:
    """The keys a source writes of one table: a closed range `lo`..`hi`
    (None unbounded), the numbers `offset` modulo `step` in it (an
    auto-increment's offset and increment; step 1 every number), or the
    listed `values` only."""
    lo: object = None
    hi: object = None
    offset: int = 0
    step: int = 1
    values: frozenset = None

    @classmethod
    def of(cls, spec):
        """A key space from the hop's words: {"range": [lo, hi]},
        {"stride": [offset, step]}, {"values": [...]}, any of them
        together, or a KeySpace already."""
        if isinstance(spec, cls):
            return spec
        spec = dict(spec or {})
        lo, hi = (list(spec.get("range") or ()) + [None, None])[:2]
        offset, step = (list(spec.get("stride") or ()) + [0, 1])[:2]
        values = spec.get("values")
        return cls(lo, hi, int(offset), max(int(step), 1),
                   frozenset(values) if values is not None else None)

    def _in_range(self, v):
        return ((self.lo is None or v >= self.lo)
                and (self.hi is None or v <= self.hi))

    def holds(self, v):
        """Whether the key `v` is in the space."""
        if self.values is not None and v not in self.values:
            return False
        if not self._in_range(v):
            return False
        if self.step > 1:
            return isinstance(v, int) and v % self.step == self.offset % self.step
        return True

    def bounded(self):
        return self.values is not None or (self.lo is not None
                                           and self.hi is not None)


def _crt(a1, m1, a2, m2):
    """(the least x >= 0 with x = a1 mod m1 and x = a2 mod m2, the period)
    or None where no number is both."""
    g = gcd(m1, m2)
    if (a2 - a1) % g:
        return None
    lcm = m1 // g * m2
    m = m2 // g
    k = ((a2 - a1) // g * pow(m1 // g, -1, m)) % m if m > 1 else 0
    return (a1 + m1 * k) % lcm, lcm


def common(a, b):
    """A key both spaces hold, or None where they are apart. Listed values
    are tried against the other space; ranges and strides are solved:
    the strides' common progression (`_crt`) has its first member at or
    above both lows checked against both highs."""
    a, b = KeySpace.of(a), KeySpace.of(b)
    if a.values is not None or b.values is not None:
        listed, other = (a, b) if a.values is not None else (b, a)
        for v in sorted(listed.values, key=repr):
            if listed.holds(v) and other.holds(v):
                return v
        return None
    lows = [x for x in (a.lo, b.lo) if x is not None]
    highs = [x for x in (a.hi, b.hi) if x is not None]
    lo, hi = (max(lows) if lows else None), (min(highs) if highs else None)
    if lo is not None and hi is not None and lo > hi:
        return None
    if a.step == 1 and b.step == 1:
        if lo is not None:
            return lo
        return hi if hi is not None else 0
    got = _crt(a.offset % a.step, a.step, b.offset % b.step, b.step)
    if got is None:
        return None
    first, period = got
    start = int(lo) if lo is not None else first
    v = first + -(-(start - first) // period) * period if start > first else first
    if lo is None and hi is not None and v > hi:
        v -= -(-(v - int(hi)) // period) * period
    return v if hi is None or v <= hi else None


def overlaps(spaces, tables=()):
    """[(table, source a, source b, a key both write)] of every pair of
    sources whose key spaces of a table meet. `spaces` is {source: {table:
    spec}}; a source that names no space for a table writes every key of
    it, so it meets every other source's space of that table."""
    out = []
    names = sorted(spaces)
    every = sorted(set(tables) | {t for s in names for t in spaces[s] or {}})
    for t in every:
        for i, x in enumerate(names):
            for y in names[i + 1:]:
                k = common((spaces[x] or {}).get(t), (spaces[y] or {}).get(t))
                if k is not None:
                    out.append((t, x, y, k))
    return out


@dataclass(frozen=True)
class Route:
    """How one source's rows are fanned out to its targets: by `table`
    ({table: target}), by key `range` (sorted first keys, target i taking
    keys from bounds[i-1] up to below bounds[i]; one more target than
    bounds), or by `hash` of the key into `targets` (`reshard.hash_place`,
    the same placement a shard split uses)."""
    by: str
    targets: tuple
    tables: dict = field(default_factory=dict)
    bounds: tuple = ()

    def place(self, table, key):
        """The target the row of `table` with `key` (a tuple of texts)
        goes to, or None where the rule places it nowhere."""
        if self.by == "table":
            return self.tables.get(table)
        if self.by == "range":
            if len(self.bounds) + 1 != len(self.targets):
                return None
            return self.targets[bisect.bisect_right(list(self.bounds), key[0])]
        if self.by == "hash":
            return self.targets[hash_place(key, len(self.targets))]
        return None


def range_gaps(bounds, targets):
    """Why the range rule does not place every key exactly once, or ""."""
    bounds = list(bounds)
    if len(bounds) + 1 != len(targets):
        return (f"{len(bounds)} bounds cut {len(bounds) + 1} ranges for"
                f" {len(targets)} targets")
    for a, b in zip(bounds, bounds[1:]):
        if not a < b:
            return f"the bounds {a!r} and {b!r} are not in rising order"
    return ""


@dataclass
class Proof:
    """What a verify of parts against a whole found, key by key: rows in
    no part, in more than one, in a part the rule does not send them to,
    in a part but not the whole, and held with another digest."""
    missing: list = field(default_factory=list)
    twice: list = field(default_factory=list)
    misplaced: list = field(default_factory=list)
    extra: list = field(default_factory=list)
    differs: list = field(default_factory=list)

    @property
    def ok(self):
        return not (self.missing or self.twice or self.misplaced
                    or self.extra or self.differs)

    def words(self, limit=3):
        out = []
        for name in ("missing", "twice", "misplaced", "extra", "differs"):
            got = getattr(self, name)
            if got:
                out.append(f"{len(got)} {name}: "
                           + ", ".join(repr(x) for x in got[:limit]))
        return "; ".join(out) or "every row in its place once"


def prove_partition(whole, parts, route=None):
    """Whether the parts are the whole cut once: `whole` {table: {key:
    digest}}, `parts` {target: {table: {key: digest}}}. With a `route`,
    a row found in another target than the rule's is misplaced."""
    p = Proof()
    held = {}
    for target, tables in sorted(parts.items()):
        for t, rows in (tables or {}).items():
            for k, d in rows.items():
                held.setdefault((t, k), []).append((target, d))
    for t, rows in sorted(whole.items()):
        for k, d in rows.items():
            got = held.pop((t, k), [])
            if not got:
                p.missing.append((t, k))
                continue
            if len(got) > 1:
                p.twice.append((t, k, tuple(x for x, _ in got)))
            target, digest = got[0]
            if route is not None and route.place(t, k) not in (x for x, _ in got):
                p.misplaced.append((t, k, target))
            if any(x != d for _, x in got):
                p.differs.append((t, k))
    p.extra = sorted(((t, k) for t, k in held), key=repr)
    return p


def prove_union(sources, target, settled=()):
    """Whether the target is the union of the sources: every source's row
    in it with that source's digest, and nothing else. A key two sources
    both hold is `twice` unless it is in `settled` (a conflict rule chose
    one), where the target's row must be one of theirs."""
    p = Proof()
    settled = set(settled)
    want = {}
    for s, tables in sorted(sources.items()):
        for t, rows in (tables or {}).items():
            for k, d in rows.items():
                want.setdefault((t, k), []).append((s, d))
    have = {(t, k): d for t, rows in target.items() for k, d in rows.items()}
    for (t, k), got in sorted(want.items(), key=repr):
        if len(got) > 1 and (t, k) not in settled:
            p.twice.append((t, k, tuple(s for s, _ in got)))
        if (t, k) not in have:
            p.missing.append((t, k))
        elif have[(t, k)] not in {d for _, d in got}:
            p.differs.append((t, k))
    p.extra = sorted((key for key in have if key not in want), key=repr)
    return p


def prove_converged(nodes):
    """Whether every node holds the same rows: a Proof against the first
    node, a row another lacks `missing`, one it alone holds `extra`."""
    p = Proof()
    names = sorted(nodes)
    if not names:
        return p
    keys = lambda n: {(t, k): d for t, rows in nodes[n].items()
                      for k, d in rows.items()}
    base = keys(names[0])
    for n in names[1:]:
        other = keys(n)
        p.missing += [(n,) + key for key in sorted(set(base) - set(other), key=repr)]
        p.extra += [(n,) + key for key in sorted(set(other) - set(base), key=repr)]
        p.differs += [(n,) + key for key in sorted(set(base) & set(other), key=repr)
                      if base[key] != other[key]]
    return p


def out_of_order(applied):
    """[(source, position before, position after)] where a target applied
    a source's change after one that came later in that source's log.
    `applied` is the target's order: [(source, position), ...]."""
    last, out = {}, []
    for source, pos in applied:
        if source in last and pos < last[source]:
            out.append((source, last[source], pos))
        last[source] = max(pos, last.get(source, pos))
    return out


def mesh_links(nodes):
    """Every directed link a full mesh needs: each node to every other."""
    nodes = sorted(nodes)
    return [(a, b) for a in nodes for b in nodes if a != b]


def missing_links(nodes, links):
    """The mesh's links not set up, sorted."""
    have = {tuple(x) for x in links}
    return [x for x in mesh_links(nodes) if x not in have]


def echoes(applied):
    """The changes that came round: [(node, origin, change)] of every
    change a node applied that began on that node, or that a node got
    from a sender which was not its origin (a forwarding, which a mesh
    never needs). `applied` is [(node, sender, origin, change), ...]."""
    return [(n, o, c) for n, sender, o, c in applied
            if o == n or sender != o]
