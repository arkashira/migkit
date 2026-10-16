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
    auto-increment's offset and increment; step 1 every number), the
    listed `values` only, or the text keys led by `prefix` (a
    consolidation's `keys: prefix`: its source's value and the
    separator)."""
    lo: object = None
    hi: object = None
    offset: int = 0
    step: int = 1
    values: frozenset = None
    prefix: str = None

    @classmethod
    def of(cls, spec):
        """A key space from the hop's words: {"range": [lo, hi]},
        {"stride": [offset, step]}, {"values": [...]}, {"prefix": text},
        any of them together, or a KeySpace already."""
        if isinstance(spec, cls):
            return spec
        spec = dict(spec or {})
        lo, hi = (list(spec.get("range") or ()) + [None, None])[:2]
        offset, step = (list(spec.get("stride") or ()) + [0, 1])[:2]
        values = spec.get("values")
        prefix = spec.get("prefix")
        return cls(lo, hi, int(offset), max(int(step), 1),
                   frozenset(values) if values is not None else None,
                   str(prefix) if prefix is not None else None)

    def _in_range(self, v):
        return ((self.lo is None or v >= self.lo)
                and (self.hi is None or v <= self.hi))

    def holds(self, v):
        """Whether the key `v` is in the space."""
        if self.values is not None and v not in self.values:
            return False
        if self.prefix is not None and not str(v).startswith(self.prefix):
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
    if a.prefix is not None or b.prefix is not None:
        return _common_prefix(a, b)
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


def _common_prefix(a, b):
    """A text key two spaces hold where one leads its keys by a prefix:
    two prefixes meet only where one begins the other (the longer is then
    a key of both); a listed value is tried as it is; a range is tried at
    the prefix itself and at its bounds that begin with it - a key past
    the prefix and below a bound that does not begin with it sorts with
    the prefix."""
    led, other = (a, b) if a.prefix is not None else (b, a)
    if other.prefix is not None:
        longer = max(led.prefix, other.prefix, key=len)
        tries = [longer] if longer.startswith(min(
            led.prefix, other.prefix, key=len)) else []
    elif other.values is not None:
        tries = sorted(other.values, key=repr)
    else:
        tries = [led.prefix] + [x for x in (other.lo, other.hi)
                                if x is not None
                                and str(x).startswith(led.prefix)]
    for v in tries:
        try:
            if led.holds(v) and other.holds(v):
                return v
        except TypeError:   # a whole-number range is no text key's
            continue
    return None


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


# ---- the facts of a topology, read off the hops --------------------------------

def site(endpoint):
    """The name of the machine an endpoint of a hop reaches: its host and
    port, so two hops that name the same server name the same site."""
    host = getattr(endpoint, "host", "") or ""
    port = int(getattr(endpoint, "port", 0) or 0)
    return f"{host}:{port}" if port else host


@dataclass(frozen=True)
class Layout:
    """The topology the hop configuration draws: its `sites`, the
    directed `links` between them (each one hop of the configuration,
    `hops` of them), and the rate each link last measured on its own
    copies (rows per second, None where it has not been measured)."""
    sites: tuple
    links: tuple
    hops: int
    kind: str
    speeds: dict = field(default_factory=dict)

    def speed(self, a, b):
        return self.speeds.get((a, b))

    def slowest(self):
        """((a, b), rate) of the slowest measured link, or None."""
        known = [(k, v) for k, v in sorted(self.speeds.items())
                 if v is not None]
        return min(known, key=lambda x: x[1]) if known else None


def shape(links):
    """The kind of many-node topology a set of directed links draws -
    `many-to-one`, `one-to-many`, `mesh` - or "" where it draws none:
    sources and targets are told apart by which side of a link a site
    stands on."""
    sources = {a for a, _ in links}
    targets = {b for _, b in links}
    nodes = sources | targets
    if len(nodes) >= 3 and sources == targets == nodes:
        return "mesh"
    if sources & targets:
        return ""
    if len(targets) == 1 and len(sources) >= 2:
        return "many-to-one"
    if len(sources) == 1 and len(targets) >= 2:
        return "one-to-many"
    return ""


def layout(hops, speed=None):
    """The `Layout` of `hops` (the configuration's `config.Hop`s that
    belong to one topology): sites and links from their endpoints, the
    kind from the links, link rates from `speed(hop)` - the planner's
    measured rate of the hop's last copies where not given."""
    if speed is None:
        from .planner import link_speed as speed
    hops = list(hops)
    links = tuple(dict.fromkeys((site(h.source), site(h.target))
                                for h in hops))
    sites = tuple(sorted({x for pair in links for x in pair}))
    rates = {}
    for h in hops:
        rates[(site(h.source), site(h.target))] = speed(h)
    return Layout(sites, links, len(hops), shape(links), rates)


#: the hop options the topology reads by name (declared in `hopkeys`):
#: {table: key space} of the hop's source, the conflict rule where sources
#: meet, and the fan-out's routing rule
SPACES_KEY, CONFLICT_KEY, ROUTING_KEY = "key_spaces", "conflict_rule", \
    "routing"


def _said(hop, name):
    """What the hop says under `name`, as `hopkeys` reads it; None where
    the hop says nothing or the key is not one a hop can carry."""
    from . import hopkeys
    options = getattr(hop, "options", None)
    if not isinstance(options, dict) or name not in hopkeys.HOP_OPTIONS:
        return None
    return hopkeys.opt(options, name)


def _agreed(hops, name, what):
    """The one value every hop that names `name` gives; a refusal naming
    the hops where two of them give different ones - a topology decided on
    one of two rules would silently ignore the other's."""
    seen = {}
    for h in hops:
        got = _said(h, name)
        if got not in (None, "", {}):
            seen.setdefault(repr(got), (got, []))[1].append(h.name)
    if len(seen) > 1:
        raise SystemExit(
            f"{what}: the hops of one topology say different things - "
            + "; ".join(f"{', '.join(names)}: {got!r}"
                        for got, names in seen.values()))
    return next(iter(seen.values()))[0] if seen else None


def spaces_of(hops):
    """{site: {table: spec}} of the key spaces the hops say their sources
    write (`key_spaces`); two hops from one source that give a table two
    spaces are refused."""
    out = {}
    for h in hops:
        got = _said(h, SPACES_KEY)
        if not got:
            continue
        mine = out.setdefault(site(h.source), {})
        for table, spec in dict(got).items():
            try:
                KeySpace.of(spec)
            except (TypeError, ValueError):
                raise SystemExit(
                    f"{h.name}: {SPACES_KEY} of {table} is not a key space"
                    " (range, stride or values)") from None
            if table in mine and mine[table] != spec:
                raise SystemExit(
                    f"{h.name}: the key space of {table} written by"
                    f" {site(h.source)} differs from what another hop from"
                    " the same source says")
            mine[table] = spec
    return out


def route_of(hops):
    """The fan-out's routing rule the hops say (`routing`), their tables
    joined: {"by", "tables", "bounds"} or {} where none says one. Hops that
    name different rules, bounds or a table's target twice differ."""
    out = {}
    for h in hops:
        got = _said(h, ROUTING_KEY)
        if not got:
            continue
        got = dict(got)
        for k in ("by", "bounds"):
            if got.get(k) and out.get(k) not in (None, got[k]):
                raise SystemExit(f"{h.name}: {ROUTING_KEY} {k} differs from"
                                 " what another hop of the topology says")
            if got.get(k):
                out[k] = got[k]
        for table, target in dict(got.get("tables") or {}).items():
            tables = out.setdefault("tables", {})
            if tables.setdefault(table, target) != target:
                raise SystemExit(f"{h.name}: {ROUTING_KEY} sends {table} to"
                                 f" {target}, another hop to"
                                 f" {tables[table]}")
    return out


def facts_of(hops, tables, spaces=None, conflict=None, route=None,
             marks=None, speed=None):
    """(the topology's decision facts, its `Layout`) read from the hops:
    kind, sources, targets and links come from the hops' endpoints; each
    source's key space, the conflict rule and a fan-out's routing come from
    the hops' own keys (`key_spaces`, `conflict_rule`, `routing`), what is
    passed in standing over what they say; each link's rate is the
    measured one (`Layout.speeds`) and rides in the facts, with the links
    that have none. Only the tables' keys, and which nodes mark migkit's
    writes, are not in the hops."""
    from . import scenarios
    hops = list(hops)
    lay = layout(hops, speed)
    sources = sorted({a for a, _ in lay.links})
    targets = sorted({b for _, b in lay.links})
    if lay.kind == "mesh":
        sources = targets = list(lay.sites)
    said = spaces_of(hops)
    for src, mine in dict(spaces or {}).items():
        said[src] = {**said.get(src, {}), **dict(mine or {})}
    facts = scenarios.topology_facts(
        lay.kind, sources, targets, tables, said,
        conflict or _agreed(hops, CONFLICT_KEY, "conflict_rule"),
        route or route_of(hops), marks, lay.links)
    facts["speeds"] = {k: v for k, v in sorted(lay.speeds.items())
                       if v is not None}
    facts["unmeasured"] = tuple(k for k in lay.links
                                if lay.speeds.get(k) is None)
    return facts, lay
