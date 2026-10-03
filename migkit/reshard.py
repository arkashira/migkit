"""Resharding (backlog W7): rows of N sources placed on M targets by one
function - a database split, many merged into one, a new shard key - each
row once, proved, and followed so the move ends with a cutover, not a stop.

    hops:
      orders-a:
        goal: reshard
        source: postgres://app@10.0.0.1/shop      # an old shard
        target: postgres://app@10.0.0.11/shop     # a new one
        options:
          reshard:
            with: [orders-b, orders-c]   # the group: every source and target they name
            by: hash                     # auto, hash, range, list, lookup or target
            key: {orders: customer_id}   # the shard key; a table not named, its own key
            shared: [countries]          # tables every target takes whole
            ranges: {orders-a: 1000000, orders-b: null}  # by: range, upper bounds
            values: {orders-a: [eu, uk], orders-b: [us]} # by: list
            lookup: {acme: orders-a, globex: orders-b}   # by: lookup
            default: orders-b            # where a value list or lookup leaves out goes

**A group** (`group`) is the hops of one such move, followed through each
one's `with`: a reshard's, a split's parts (`split`) - one source, a
target a part, each part's rule its row filter - or a consolidation's
sources (`consolidate`) - many sources, one target, each source's rows
under its own value. Its sources are the distinct sources its hops read,
its targets the distinct targets they write, each named by the first hop
by name that says it, so every hop of the group finds one order - hash
places a row on the same target whichever hop runs. Its state is kept
under its first hop by name (`Group.report_dir`).

**One function places every row** (`Function`, its way climbed per table,
`table_ways`): `hash` (the shard key's canonical text, an 8-byte BLAKE2b,
modulo the targets), `range` (upper bounds, exclusive, one a target hop,
`null` the last), `list` and `lookup` (a value's target, `default` for one
not named), a split part's rule (its row filter, asked of the source), a
consolidation's one target, or the target cluster's own distribution read
and honoured (`Spread`, `Engine.distribution`: Citus's distribution column
and shard placements, MongoDB's shard key and chunks, Cassandra's
partitioner token, Vitess's vindex). `auto` takes the target's own where
the group has one target that spreads the table, hash otherwise.

**Before the move** (`problems`): every source row placed on the sources -
a row no target takes, a key two sources hold, a table with no key to
place by - refused with its keys before anything is written.

**The copy**: a reshard's (`copy`) makes and empties each target's tables
once, then writes every source's rows to the target their key places them
on, a batch a target (`consolidate.write_ways`); a table the target
cluster spreads goes in batches a node, to the node owning them where
migkit reaches it, through the cluster's router otherwise (`node_ways`);
units are handed out across machines through claims where the hop shares
its tables. A split's parts and a consolidation's sources are each copied
by their own hop's move (`copies`), before their changes are followed.

**The follow** (`follow`): the one follow loop (`tailctl.run_loop`) over
every source's change log, each change placed by the same function. A
batch is planned whole (`plan_batch`): a row whose key moved it is removed
from every target it left before it is written on the one it reaches -
every target's removals first, then every target's writes - so no row is
on two targets at once; each target's writes of a source's batch are one
transaction under the batch's number (`marks`), so a target that
committed a batch before a stop is not written it again. A key that moves
between sources is asked of every source as it is now (`_held_elsewhere`).

**Proved** (`prove`, `judge`): every target read by key and digest - each
key on the one target its key places it on, as its source has it, no key
on two targets, the targets together the sources; a split's and a
consolidation's by their own proofs (`proofs`).

**The cutover** (`cut_over`): the cutover's own path over the whole group
at once (`Cluster`, `freeze.GroupHold`, `leg`): every source frozen, the
follow drained past every source's position taken after the freeze, the
proof across every target, every target's counters, the flip.
"""
import dataclasses
import hashlib
import time
from dataclasses import dataclass, field

GOAL = "reshard"
OPTION = "reshard"
BYS = ("auto", "hash", "range", "list", "lookup", "target")
#: the goals whose hops make a group (`group`): each but reshard moved once
#: by its own hop, and followed here
KINDS = ("reshard", "split", "consolidate")
EXAMPLES = 3
#: changes a read of the follow asks for, and the most it grows to while
#: it is behind - the tail's own (`HeteroEngine.TAIL_BATCH`)
BATCH, BATCH_MOST = 1000, 16000
#: what a function places a row of a table every target takes whole on
ALL = -1
#: a row the source no longer holds when it is asked where it goes now
GONE = "gone"


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def _names(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    return [str(value)]


def settings(hop):
    """The hop's `reshard:`, read and held together, for a hop whose goal
    is reshard; None for any other. What needs the other hops is held by
    `group`."""
    if (getattr(hop, "goal", "") or "") != GOAL:
        return None
    raw = (hop.options or {}).get("reshard") or {}
    if not isinstance(raw, dict):
        raise SystemExit("reshard: is a mapping - with, by, key, shared,"
                         " ranges, values, lookup, default")
    by = str(raw.get("by") or "auto").lower()
    if by not in BYS:
        raise SystemExit(f"reshard: by: {by} - one of {', '.join(BYS)}")
    key = raw.get("key") or {}
    if not isinstance(key, dict):
        raise SystemExit("reshard: key is {table: column or [columns]}")
    ranges = raw.get("ranges")
    values = raw.get("values")
    lookup = raw.get("lookup")
    wrong = []
    for name, got, kind in (("ranges", ranges, "range"),
                            ("values", values, "list"),
                            ("lookup", lookup, "lookup")):
        if got is not None and not isinstance(got, dict):
            wrong.append(f"{name} is a mapping")
        elif got is not None and by != kind:
            wrong.append(f"{name} is read only by by: {kind}, and this hop"
                         f" says by: {by}")
        elif got is None and by == kind:
            wrong.append(f"by: {kind} places rows by {name}, and none is"
                         " given")
    seen = {}
    for hop_name, listed in (values.items() if isinstance(values, dict)
                             else ()):
        for v in _names(listed):
            if v in seen and seen[v] != str(hop_name):
                wrong.append(f"values: {v} is listed for both {seen[v]} and"
                             f" {hop_name}")
            seen.setdefault(v, str(hop_name))
    if isinstance(ranges, dict):
        open_ = [str(h) for h, b in ranges.items() if b is None]
        if len(open_) > 1:
            wrong.append("ranges: only the last range is open (null) -"
                         f" {', '.join(open_)} all are")
    if wrong:
        raise SystemExit("reshard: " + "; ".join(wrong))
    return {"with": _names(raw.get("with")), "by": by,
            "key": {str(t): _names(c) for t, c in key.items()},
            "shared": _names(raw.get("shared")),
            "ranges": ({str(h): b for h, b in ranges.items()}
                       if isinstance(ranges, dict) else None),
            "values": ({str(h): _names(v) for h, v in values.items()}
                       if isinstance(values, dict) else None),
            "lookup": ({str(v): str(h) for v, h in lookup.items()}
                       if isinstance(lookup, dict) else None),
            "default": (str(raw["default"]) if raw.get("default")
                        is not None else None)}


def kind_of(hop):
    """The group a hop is one of - reshard, split or consolidate - or
    None."""
    goal = getattr(hop, "goal", "") or ""
    return goal if goal in KINDS else None


def follows(hop):
    """Whether this hop's changes are followed by its group (`follow`) - a
    split's part, a consolidation's source - rather than by a tail of its
    own: a row a rule moves between parts is removed from one before it is
    written on the other, which tails of their own could not order."""
    return kind_of(hop) in ("split", "consolidate")


# --- the group -----------------------------------------------------------------------

@dataclass
class Side:
    """One source or target of a group: the hop it is read or written
    through, the database of the source it is read from - or, for a
    target, whose rows it is written - and a consolidation's source's
    value in the tenant column."""
    name: str
    hop: object
    db: str
    value: object = None


def _where(ep):
    return (str(ep.host or ""), int(ep.port or 0))


def _target_of(hop, db):
    """A target as a group tells targets apart: the server, and the
    database it writes."""
    return (_where(hop.target), str(hop.target_db(db)))


@dataclass
class Group:
    """The hops of one reshard, split or consolidation, and what they
    name."""
    kind: str
    lead: object
    hops: list
    sources: list
    targets: list
    conf: dict
    _pairs: dict = field(default_factory=dict)

    @property
    def db(self):
        """The database the group's state is kept under: its first
        source's."""
        return self.sources[0].db

    def index(self, hop_name):
        """The target a hop of the group writes, by its index."""
        hop = next((h for h in self.hops if h.name == hop_name), None)
        if hop is None:
            return None
        want = _target_of(hop, self.db)
        for j, t in enumerate(self.targets):
            if _target_of(t.hop, t.db) == want:
                return j
        return None

    def pair_hop(self, i, j):
        """The hop source `i` is carried to target `j` by, one writer a
        batch (so a batch is one transaction its mark can say), named apart
        so its reports, its slot and its batch marks are its own: a
        reshard's rules between the two servers, a split part's own rules,
        a consolidation source's own."""
        src, dst = self.sources[i], self.targets[j]
        if self.kind == "split":
            base = dst.hop
        elif self.kind == "consolidate":
            # its row filter is the group's to ask (`_rule_places`), of the
            # source's own keys, before they are shifted: asked again by the
            # pair, of a shifted key, every row would be out of it
            base = dataclasses.replace(src.hop, mapping={
                k: v for k, v in (src.hop.mapping or {}).items()
                if k != "where"})
        else:
            base = dataclasses.replace(self.lead,
                                       db_map=dict(dst.hop.db_map or {}))
        return dataclasses.replace(
            base, name=f"{self.lead.name}-s{i + 1}t{j + 1}",
            source=src.hop.source, target=dst.hop.target, workers=1,
            options=dict(base.options or {}))

    def pair(self, i, j):
        got = self._pairs.get((i, j))
        if got is None:
            from .engines import get_engine
            got = self._pairs[(i, j)] = _pair(get_engine(self.pair_hop(i,
                                                                       j)))
            _arm(self, got)
        return got

    def reader(self, i):
        return self.pair(i, 0).src_engine

    def writer(self, i, j):
        return self.pair(i, j).dst_engine

    def report_dir(self):
        d = self.lead.report_dir(self.db) / "group"
        d.mkdir(parents=True, exist_ok=True)
        return d


def _pair(eng):
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


def _closure(hop, have, with_of, goal):
    """(every hop the group holds, by name; the reasons it does not):
    the hops named, and the hops they name."""
    out, wrong, todo = {hop.name: hop}, [], [hop]
    while todo:
        h = todo.pop()
        try:
            named = with_of(h)
        except SystemExit as e:
            wrong.append(f"{h.name}: {e}")
            continue
        for name in named:
            if name in out:
                continue
            other = have.get(name)
            if other is None:
                wrong.append(f"with: {name} is not a hop the configuration"
                             " has")
                continue
            if (getattr(other, "goal", "") or "") != goal:
                wrong.append(f"with: {name}'s goal is not {goal}")
                continue
            out[name] = other
            todo.append(other)
    return [out[n] for n in sorted(out)], wrong


def _siblings(hop, named, siblings):
    from . import config
    if not named:
        return {}, []
    if siblings is not None:
        return siblings, []
    try:
        return config.load_hops(), []
    except SystemExit as e:
        return {}, [f"the hops it names could not be read: {e}"]


def group(hop, db, siblings=None):
    """The `Group` this hop is one of, for its database `db`, or
    SystemExit naming every reason its hops do not hold together."""
    kind = kind_of(hop)
    if kind == "split":
        return _split_group(hop, db, siblings)
    if kind == "consolidate":
        return _consolidate_group(hop, db, siblings)
    return _reshard_group(hop, db, siblings)


def _split_group(hop, db, siblings):
    """A split's parts: its one source, a target a part, each held to the
    parts it names (`split.parts`)."""
    from . import split
    conf = split.settings(hop)
    have, wrong = _siblings(hop, conf["with"], siblings)
    hops, more = _closure(hop, have, lambda h: split.settings(h)["with"],
                          "split")
    wrong += more
    for h in hops:
        try:
            split.parts(h, have)
        except SystemExit as e:
            wrong.append(str(e))
    if wrong:
        raise SystemExit(f"{hop.name}: the split does not hold together - "
                         + "; ".join(dict.fromkeys(wrong)))
    targets = [Side(h.name, h, db) for h in hops]
    return Group("split", hops[0], hops, [Side(hops[0].name, hops[0], db)],
                 targets, conf)


def _consolidate_group(hop, db, siblings):
    """A consolidation's sources into the one target database `db` goes
    into: every hop of it, each of its databases going there."""
    from . import consolidate
    conf = consolidate.settings(hop)
    have, wrong = _siblings(hop, conf["with"], siblings)
    hops, more = _closure(hop, have,
                          lambda h: consolidate.settings(h)["with"],
                          "consolidate")
    wrong += more
    there = _target_of(hop, db)
    found = {}
    for h in hops:
        try:
            got = consolidate.sources(h, list(h.databases or [db]), have)
        except SystemExit as e:
            wrong.append(str(e))
            continue
        for s in got:
            if _target_of(s.hop, s.db) == there:
                found.setdefault(s.name, Side(s.name, s.hop, s.db, s.value))
    if not found and not wrong:
        wrong.append(f"no source of it goes into {hop.target_db(db)}")
    if wrong:
        raise SystemExit(f"{hop.name}: the consolidation does not hold"
                         " together - " + "; ".join(dict.fromkeys(wrong)))
    sources = [found[n] for n in sorted(found)]
    lead = sources[0].hop
    return Group("consolidate", lead, hops, sources,
                 [Side(str(lead.target_db(sources[0].db)), lead,
                       sources[0].db)], conf)


def _alike(conf):
    """What every hop of a reshard says alike: all of it but `with`."""
    return {k: v for k, v in conf.items() if k != "with"}


def _reshard_group(hop, db, siblings):
    conf = settings(hop)
    have, wrong = _siblings(hop, conf["with"], siblings)
    hops, more = _closure(hop, have, lambda h: settings(h)["with"], GOAL)
    wrong += more
    mine = _alike(conf)
    for h in hops:
        if h is hop:
            continue
        try:
            theirs = settings(h)
        except SystemExit:
            continue
        if _alike(theirs) != mine:
            wrong.append(f"{h.name} places rows otherwise than {hop.name}"
                         " - every hop of a reshard says by, key, shared,"
                         " ranges, values, lookup and default alike")
        if (h.engine, (h.options or {}).get("source_engine"),
                (h.options or {}).get("target_engine")) != (
                hop.engine, (hop.options or {}).get("source_engine"),
                (hop.options or {}).get("target_engine")):
            wrong.append(f"{h.name} joins other engines than {hop.name}")
        if sorted(h.databases or []) != sorted(hop.databases or []):
            wrong.append(f"{h.name} carries other databases than"
                         f" {hop.name}")
    sources, targets, seen_s, seen_t = [], [], set(), set()
    for h in hops:
        if _where(h.source) not in seen_s:
            seen_s.add(_where(h.source))
            sources.append(Side(h.name, h, db))
        if _target_of(h, db) not in seen_t:
            seen_t.add(_target_of(h, db))
            targets.append(Side(h.name, h, db))
    for s in sources:
        for t in targets:
            if (_where(s.hop.source), str(db)) == _target_of(t.hop, db):
                wrong.append(f"{t.name} writes the database {s.name} reads:"
                             " a reshard writes new shards, never its own"
                             " sources")
    named = {h.name for h in hops}
    for h in list(conf["ranges"] or {}) + list(conf["values"] or {}) + list(
            (conf["lookup"] or {}).values()) + ([conf["default"]]
                                                if conf["default"] else []):
        if h not in named:
            wrong.append(f"{h} is named where rows go, and is not a hop of"
                         " the group")
    if conf["by"] == "target" and len(targets) > 1:
        wrong.append("by: target places rows by the one target cluster's"
                     " own distribution, and the group writes"
                     f" {len(targets)} targets")
    if wrong:
        raise SystemExit(f"{hop.name}: the reshard does not hold together -"
                         " " + "; ".join(dict.fromkeys(wrong)))
    return Group(GOAL, hops[0], hops, sources, targets, conf)


# --- the function --------------------------------------------------------------------

def _text(cls, value):
    """A value as the canonical text every side renders it to - typed
    first where a change log handed it over as text (`twoway._text`), so
    a key read by the copy and the same key in a change place alike."""
    from . import twoway
    return twoway._text(cls, value)


def hash_place(texts, n):
    """The target of a key under `hash`: its texts, an 8-byte BLAKE2b,
    modulo `n`. The same in every process and every run - Python's own
    `hash` is salted per process."""
    h = hashlib.blake2b(digest_size=8)
    for t in texts:
        h.update(b"\x00" if t is None else
                 b"\x01" + str(t).encode("utf-8", "surrogatepass"))
        h.update(b"\x1f")
    return int.from_bytes(h.digest(), "big") % max(int(n), 1)


_NUMBERS = ("integer", "decimal", "float")


def _ordered(cls, text):
    """A key's first text as `range` compares it: a number where the
    column is one, the text otherwise (ISO dates and times order as
    text)."""
    from decimal import Decimal, InvalidOperation
    if text is None:
        return None
    if cls in _NUMBERS:
        try:
            return (0, Decimal(str(text)))
        except (InvalidOperation, ValueError):
            return (1, str(text))
    return (1, str(text))


@dataclass
class Function:
    """How one table's rows are placed. `place(texts)`: the target index
    of a row whose shard key renders to `texts`, `ALL` for a table its
    targets take whole, None for a row placed nowhere - or, for a split
    part's rule, None always: a rule is SQL, asked of the source
    (`_rule_places`). `within`, the targets that hold the table."""
    way: str
    key: list
    said: str
    within: list
    classes: dict = field(default_factory=dict)
    pk: list = field(default_factory=list)
    columns: list = field(default_factory=list)
    #: the first source that holds the table: a table its targets take
    #: whole is read from it alone
    home: int = 0
    #: range: [(upper, index)] in order, the open one last (upper None)
    bounds: list = field(default_factory=list)
    #: list and lookup: {value text: index}, and the index of `default`
    table: dict = field(default_factory=dict)
    default: object = None
    #: a split's rules: {target index: its row filter, or None for all}
    rules: dict = field(default_factory=dict)
    #: a split's rows no part takes stay on the source (`rest: stays`)
    stays: bool = False
    #: the one target cluster's word on how it spreads the table (`Spread`)
    spread: object = None

    def place(self, texts):
        if self.way == "whole":
            return ALL
        if self.way in ("one", "owned"):
            return 0
        if self.way == "rule" or texts is None:
            return None
        if self.way == "hash":
            return hash_place(texts, len(self.within))
        first = texts[0] if texts else None
        if self.way == "range":
            got = _ordered(self.classes.get(self.key[0]), first)
            if got is None:
                return None
            for upper, j in self.bounds:
                if upper is None or got < upper:
                    return j
            return None
        if first is None:
            return self.default
        return self.table.get(str(first), self.default)


def table_ways():
    """The ways a table's rows are placed, in the order they are climbed:
    its targets take it whole, a split part's rule, a consolidation's one
    target, the target cluster's own distribution, the hop's own
    function, a hash of the key."""
    from .decide import Need, Rung
    return (
        Rung("whole", "every target takes it whole",
             frozenset({"placed"}),
             needs=(Need("a table every target takes",
                         lambda f: f["shared"] or f["reference"],
                         "the hop does not list it in shared, nor the"
                         " target keep it on every node"),),
             because="the hop lists it in shared, or the target cluster"
                     " keeps it on every node"),
        Rung("rule", "the rows each target's row filter takes",
             frozenset({"placed"}),
             needs=(Need("a split, or a source read through a row filter",
                         lambda f: f["group"] == "split" or f["filtered"],
                         "the group is not a split, and the hop filters"
                         " no row of it"),),
             because="each target takes the rows its row filter selects"),
        Rung("one", "the one target, under each source's value",
             frozenset({"placed"}),
             needs=(Need("a consolidation",
                         lambda f: f["group"] == "consolidate",
                         "the group is not a consolidation"),),
             because="every source's rows go into one target"),
        Rung("owned", "the target cluster's own distribution",
             frozenset({"placed"}),
             needs=(Need("by: target or auto",
                         lambda f: f["shard by"] in ("target", "auto"),
                         "the hop places rows by its own function"),
                    Need("one target", lambda f: f["shard targets"] == 1,
                         "the group writes more than one target"),
                    Need("a target that spreads the table",
                         lambda f: f["distributed"],
                         "the target does not spread the table across"
                         " nodes of its own")),
             because="the target spreads the table across its nodes, and"
                     " each row goes where the target itself would put it"),
        Rung("range", "the hop's ranges of the shard key",
             frozenset({"placed"}),
             needs=(Need("by: range", lambda f: f["shard by"] == "range",
                         "the hop does not place rows by ranges"),),
             because="the hop places rows by ranges"),
        Rung("list", "the hop's lists of shard key values",
             frozenset({"placed"}),
             needs=(Need("by: list or lookup",
                         lambda f: f["shard by"] in ("list", "lookup"),
                         "the hop does not place rows by listed values"),),
             because="the hop names each value's target"),
        Rung("hash", "a hash of the shard key", frozenset({"placed"}),
             needs=(Need("by: hash or auto",
                         lambda f: f["shard by"] in ("hash", "auto"),
                         "the hop places rows by its own function"),),
             because="every target takes an even share"))


def _named_in(names, table):
    leaf = str(table).split(".")[-1]
    return str(table) in names or leaf in names


def shard_key(g, table, pk):
    """The columns a table's rows are placed by: the hop's `key` for it,
    else its own key."""
    keys = (g.conf.get("key") or {}) if g.kind == GOAL else {}
    for name in (str(table), str(table).split(".")[-1]):
        if name in keys:
            return list(keys[name])
    return list(pk or [])


def _classes(eng, side, db, table):
    from . import canon
    return {n: canon.type_class(eng.CANON_ENGINE, t)
            for n, t in eng.neutral_columns(side, db, table)}


def _target_table(pair, db, table):
    """The target's name for a source table of `db`, as the copy names
    it."""
    dst = pair.dst_engine
    there = [] if dst.target_missing(db) else dst.neutral_tables("dst", db)
    got, _, _, _ = pair.match_tables([table], there, pair._renamer(db))
    return got[0][1] if got else pair._leaf(pair._rename(table, db))


def _spread(g, table):
    """What the one target says of how it spreads `table` (`Spread`), or
    None - asked of its own engine (`Engine.distribution`); a target that
    cannot say spreads nothing migkit knows of."""
    if len(g.targets) != 1:
        return None
    pair = g.pair(0, 0)
    db = g.sources[0].db
    try:
        return pair.dst_engine.distribution("dst", db,
                                            _target_table(pair, db, table))
    except Exception:  # noqa: BLE001 - not known: spread by no one
        return None


def _shared(g, table):
    """Whether every target of the group is to hold `table` whole: a
    reshard's or a split's `shared`."""
    if g.kind == "consolidate":
        return False
    return _named_in(g.conf.get("shared") or [], table)


def function(g, table, home=0, record=False):
    """(the `Function` placing `table`'s rows, the `decide.Choice` it was
    taken by); the Function None where no way holds - the choice says
    why."""
    from . import decide
    reader = g.reader(home)
    db = g.sources[home].db
    pk = list(reader.neutral_key("src", db, table) or [])
    spread = {}

    def spread_of():
        if "got" not in spread:
            spread["got"] = (_spread(g, table) if g.kind == GOAL
                             else None)
        return spread["got"]
    facts = decide.Facts(
        {"group": g.kind,
         "shard by": g.conf.get("by", "") if g.kind == GOAL else "",
         "shard targets": len(g.targets), "shared": _shared(g, table),
         # a consolidation's source read through its row filter: a row
         # that leaves it is taken off the one target
         "filtered": g.kind == "consolidate" and any(
             _where_of(g, i, table) for i in range(len(g.sources)))},
        distributed=lambda: bool(spread_of() is not None
                                 and not spread_of().reference),
        reference=lambda: bool(spread_of() is not None
                               and spread_of().reference))
    choice = decide.climb(f"{db}.{table}: where its rows go", table_ways(),
                          facts, ("placed",))
    if record and choice.rung is not None:
        decide.record(g.lead, choice)
    if choice.rung is None:
        return None, choice
    way = choice.rung.name
    classes = _classes(reader, "src", db, table)
    every = list(range(len(g.targets)))
    out = Function(way, shard_key(g, table, pk), choice.rung.said, every,
                   classes, pk, list(classes), home)
    if g.kind == "split":
        from . import split
        parts = [split.Part(t.hop) for t in g.targets]
        out.within = [j for j, p in enumerate(parts)
                      if p.carries(g.targets[j].db, table)]
        out.rules = {j: parts[j].rule(g.targets[j].db, table)
                     for j in out.within}
        out.stays = (g.conf.get("rest") == "stays")
    elif way == "rule":
        # a consolidation's: each source's own filter, asked of that source
        # (`_rule_places`); a row out of it is not carried
        out.stays = True
    if way == "owned":
        sp = spread_of()
        # the cluster names its columns as the target has them: read
        # back through the hop's mapping to the source's
        _, back = g.pair(home, 0)._mapped_types(
            db, table, dict(reader.neutral_columns("src", db, table)))
        out.key = [back.get(c, c) for c in sp.columns]
        out.spread, out.said = sp, sp.said
    elif way == "range":
        cls = classes.get(out.key[0]) if out.key else None
        bounds = [(None if upper is None else
                   _ordered(cls, _text(cls, upper)), g.index(name))
                  for name, upper in g.conf["ranges"].items()]
        bounds.sort(key=lambda b: (b[0] is None, b[0] or (0, 0)))
        out.bounds = bounds
    elif way == "list":
        cls = classes.get(out.key[0]) if out.key else None
        for name, listed in (g.conf.get("values") or {}).items():
            for v in listed:
                out.table[str(_text(cls, v))] = g.index(name)
        for v, name in (g.conf.get("lookup") or {}).items():
            out.table[str(_text(cls, v))] = g.index(name)
        out.default = (g.index(g.conf["default"]) if g.conf.get("default")
                       else None)
    return out, choice


# --- the tables and their rows -------------------------------------------------------

def _takes(g, i, table):
    """Whether the group carries `table` of source `i`, by the scope of the
    hop that carries it: a reshard's first hop, any part of a split, a
    consolidation's source's own hop."""
    s = g.sources[i]
    engine = g.reader(i).CANON_ENGINE
    if g.kind == "split":
        return any(t.hop.takes_table(t.db, table, engine) for t in g.targets)
    hop = s.hop if g.kind == "consolidate" else g.lead
    return hop.takes_table(s.db, table, engine)


def _tables(g):
    """{source table: the index of the first source holding it} of every
    table the group carries - the sources hold one schema, and a table
    one of them lacks is read where it is."""
    out = {}
    for i, s in enumerate(g.sources):
        for t in g.reader(i).neutral_tables("src", s.db):
            if _takes(g, i, t):
                out.setdefault(t, i)
    return out


def _where_of(g, i, table):
    """The row filter source `i`'s rows of `table` are read through: the
    hop's own (`mapping.where`), none for a split - each part's rule is
    its placement."""
    if g.kind == "split":
        return None
    s = g.sources[i]
    hop = s.hop if g.kind == "consolidate" else g.lead
    return hop.row_filter(s.db, *hop.parts_of(table))


def _sources_of(g, fn):
    """The sources a table's rows are read from: every one, but for a
    table its targets take whole the first that holds it - the others
    keep copies of the same table, not rows of their own."""
    if fn.way == "whole":
        return [fn.home]
    return list(range(len(g.sources)))


def _texts(fn, values):
    """The shard key's texts of a row given as {column: value}, or None
    where the row does not carry every column of it."""
    if not fn.key or not all(k in values for k in fn.key):
        return None
    return tuple(_text(fn.classes.get(k), values[k]) for k in fn.key)


def _keys(ks):
    return ", ".join("(" + ", ".join(map(str, k)) + ")" for k in ks)


def _shown(keys):
    return _keys(sorted(keys, key=repr)[:EXAMPLES])


@dataclass
class Placed:
    """One table's rows counted against the function on the sources."""
    table: str
    said: str
    rows: int = 0
    per_target: list = field(default_factory=list)
    nowhere: int = 0
    shown_nowhere: list = field(default_factory=list)
    twice: int = 0
    shown_twice: list = field(default_factory=list)
    wrong: list = field(default_factory=list)


def placement(g):
    """[Placed] of every table of a reshard: each source's rows - their key
    and their shard key - placed by the function, read only; and what
    stops it before a row is written: a table no way places, no key to
    place by, a shard key the table does not have, a row the function
    places nowhere, a key two sources hold."""
    out = []
    for t, home in sorted(_tables(g).items()):
        fn, choice = function(g, t, home)
        if fn is None:
            out.append(Placed(t, "", wrong=[f"{t}: no way places its rows -"
                                            f" {choice.reason}"]))
            continue
        p = Placed(t, fn.said, per_target=[0] * len(g.targets))
        lacks = [k for k in fn.key if k not in fn.classes]
        if lacks:
            p.wrong.append(f"{t}: the shard key {', '.join(lacks)} is not a"
                           " column of it - reshard: {key: {table:"
                           " column}}")
            out.append(p)
            continue
        if not fn.pk and fn.way != "whole":
            p.wrong.append(f"{t}: no key, so a row cannot be placed once and"
                           " proved there - give it one, or reshard: {shared:"
                           f" [{t}]}} where every target is to hold it whole")
            out.append(p)
            continue
        read = list(dict.fromkeys(fn.pk + list(fn.key)))
        seen = {}
        for i in _sources_of(g, fn):
            r, db = g.reader(i), g.sources[i].db
            if t not in r.neutral_tables("src", db):
                continue
            cls = _classes(r, "src", db, t)
            for rows_ in r._every_row("src", db, t,
                                      [(c, cls.get(c)) for c in read],
                                      _where_of(g, i, t)):
                for row in rows_:
                    values = dict(zip(read, row))
                    p.rows += 1
                    key = tuple(_text(cls.get(c), values[c]) for c in fn.pk)
                    if fn.way != "whole" and len(g.sources) > 1:
                        if key in seen and seen[key] != i:
                            p.twice += 1
                            if len(p.shown_twice) < EXAMPLES:
                                p.shown_twice.append(key)
                        seen.setdefault(key, i)
                    j = fn.place(_texts(fn, values))
                    if j is None:
                        p.nowhere += 1
                        if len(p.shown_nowhere) < EXAMPLES:
                            p.shown_nowhere.append(key)
                    elif j == ALL:
                        p.per_target = [n + 1 for n in p.per_target]
                    else:
                        p.per_target[j] += 1
        if p.nowhere:
            p.wrong.append(f"{t}: {p.nowhere:,} of its {p.rows:,} rows no"
                           f" target takes ({fn.said}; by key:"
                           f" {_keys(p.shown_nowhere)}) - name a target for"
                           " their values, or reshard: {default: <hop>}")
        if p.twice:
            p.wrong.append(f"{t}: {p.twice:,} keys more than one source holds"
                           f" (by key: {_keys(p.shown_twice)}) - a row is"
                           " placed once, and two sources' rows of one key"
                           " would be one row on the target")
        out.append(p)
    return out


def _ledger_path(g):
    return g.report_dir() / "copy.json"


def _ledger(g):
    from . import durable
    return durable.read_state(_ledger_path(g), "state") or {}


def _save_ledger(g, got):
    from . import durable
    durable.write_json(_ledger_path(g), got, indent=1)


def problems(hop, eng, db):
    """[what stops this reshard before a row is written]: a group that does
    not hold together, and every table's rows placed on the sources
    (`placement`). Once the group's copy is done and its changes are
    followed, the rows are not counted again: the follow places each
    change, and the cutover proves every row."""
    try:
        g = group(hop, db)
    except SystemExit as e:
        return [str(e)]
    if _ledger(g).get("done"):
        return []
    try:
        placed = placement(g)
    except SystemExit as e:
        return [str(e)]
    except Exception as e:  # noqa: BLE001 - asked of the sources, said
        return [f"{db}: where each row goes could not be counted on the"
                f" sources: {_last(e)}"]
    return [w for p in placed for w in p.wrong]


def rows(hop, eng, db):
    """assess rows of the reshard: where every table's rows go."""
    try:
        g = group(hop, db)
        placed = placement(g)
    except Exception as e:  # noqa: BLE001 - the gate says it by problems
        return [{"level": "warn", "scope": "the reshard", "item": db,
                 "detail": f"could not be counted: {_last(e)}"}]
    names = [t.name for t in g.targets]
    out = [{"level": "pass", "scope": "the reshard", "item": db,
            "detail": f"{len(g.sources)} sources onto {len(g.targets)}"
                      f" targets ({', '.join(names)}): "
                      f"{sum(p.rows for p in placed):,} rows in"
                      f" {len(placed)} tables placed by the function"}]
    for p in placed:
        if p.wrong or not p.said:
            continue
        spread = ", ".join(f"{n} {c:,}" for n, c in zip(names,
                                                         p.per_target))
        out.append({"level": "pass", "scope": "the reshard",
                    "item": f"{db}.{p.table}",
                    "detail": f"{p.said}: {spread}"})
    return out


def plan_lines(hop, eng, db):
    """The plan's lines of a reshard, or []."""
    got = settings(hop)
    if got is None or not db:
        return []
    try:
        g = group(hop, db)
    except SystemExit as e:
        return [f"  a reshard that does not hold together: {e}"]
    return [f"  a reshard: {db} of {len(g.sources)} sources"
            f" ({', '.join(s.name for s in g.sources)}) onto"
            f" {len(g.targets)} targets"
            f" ({', '.join(t.name for t in g.targets)}), each row by"
            f" {got['by']} of its shard key, counted before the move,"
            " followed change by change - a key that moves its row removed"
            " from one target before it is written on the other - and"
            " proved on every target and across them at the cutover"]


def follow_lines(hop, db):
    """The plan's line of a split's or a consolidation's follow, or []."""
    if not follows(hop) or not db:
        return []
    try:
        g = group(hop, db)
    except SystemExit as e:
        return [f"  its changes cannot be followed as a group: {e}"]
    return [f"  its changes followed with its group's ({len(g.sources)}"
            f" sources onto {len(g.targets)} targets) in one loop: a row a"
            " rule moves removed from the target it left before it is"
            " written on the one it reaches, and the group cut over at"
            " once"]


# --- writing ------------------------------------------------------------------------

def node_ways():
    """The ways a batch of a table the target cluster spreads is written:
    to the node that owns its rows, where migkit reaches the node; through
    the cluster's own router, a batch a node, otherwise - every write by
    key, so a batch the first left half done converges."""
    from .decide import Need, Rung
    return (Rung("node", "to the node that owns them",
                 frozenset({"written"}),
                 needs=(Need("a node migkit can address",
                             lambda f: f["node address"],
                             "the target names no node migkit can write to"
                             " directly"),),
                 because="the cluster's router is not in the way of the"
                         " rows"),
            Rung("router", "through the cluster's router, a batch a node",
                 frozenset({"written"}), footprint=1,
                 because="the router places each row where the cluster"
                         " keeps it"))


def _write(g, i, j, table, cols, rows_, log):
    """A batch of rows written to target `j`, by the target's bulk write or
    its plainest (`consolidate.write_ways`)."""
    from . import consolidate, decide
    dst, db = g.writer(i, j), g.sources[i].db

    def run(choice, rows_=rows_):
        write = (dst.neutral_write if choice.rung.name == "bulk"
                 else dst.neutral_rewrite)
        for a in range(0, len(rows_), consolidate.BATCH):
            write("dst", db, table, cols, rows_[a:a + consolidate.BATCH])
        return len(rows_)
    return decide.attempt(f"{table} on {g.targets[j].name}",
                          consolidate.write_ways(), run, log=log)[1]


def _node_pair(g, i, k, spread):
    """The pair writing source `i`'s rows to part `k`'s node of the one
    target's cluster, or None where the cluster names no address for it."""
    at = spread.address(k) if spread.address is not None else None
    if at is None:
        return None
    key = (i, "node", tuple(at))
    got = g._pairs.get(key)
    if got is None:
        from .engines import get_engine
        base = g.pair_hop(i, 0)
        host, port = at
        h = dataclasses.replace(
            base, name=f"{g.lead.name}-s{i + 1}n{k + 1}",
            target=dataclasses.replace(base.target, host=host,
                                       port=int(port)))
        got = g._pairs[key] = _pair(get_engine(h))
        _arm(g, got)
    return got


def _write_spread(g, i, table, fn, src_cols, dst_cols, rows_, log, where):
    """A batch of a table the target cluster spreads: its rows grouped by
    the part that owns each (`Spread.owners`, asked of the cluster in one
    statement), each group written to its node directly, or through the
    cluster's router where the node cannot be written from here
    (`node_ways`, `decide.attempt`)."""
    from . import decide
    names = [n for n, _ in src_cols]
    at = [names.index(k) for k in fn.key]
    owners = fn.spread.owners([tuple(_text(fn.classes.get(k), r[x])
                                     for k, x in zip(fn.key, at))
                               for r in rows_])
    groups = {}
    for r, k in zip(rows_, owners):
        groups.setdefault(k, []).append(r)
    dst_names = [n for n, _ in dst_cols]
    pk = [dst_names[names.index(k)] for k in fn.pk if k in names]
    db = g.sources[i].db
    n = 0
    for k, part in groups.items():
        node = _node_pair(g, i, k, fn.spread) if k is not None else None

        def run(choice, part=part, k=k, node=node):
            if choice.rung.name == "router":
                return _write(g, i, 0, _target_table(g.pair(i, 0), db,
                                                     table),
                              dst_cols, part, log)
            if not pk or len(pk) != len(fn.pk):
                raise decide.Blocked("the table has no key to write its"
                                     " rows to a node by")
            # by key, as a change is applied: a node's own part of a table
            # is not one its catalogue lists to every client, so the key
            # comes with the rows
            recs = [{"op": "insert", "table": fn.spread.parts[k][1],
                     "key": {c: v for c, v in zip(dst_names, r)
                             if c in pk},
                     "values": dict(zip(dst_names, r))} for r in part]
            node.dst_engine.neutral_apply("dst", db, recs)
            return len(part)
        said = (fn.spread.parts[k][0] if k is not None
                else "a node the cluster did not name")
        facts = decide.Facts({"node address": node is not None})
        n += decide.attempt(f"{table}: a batch for {said}", node_ways(), run,
                            facts, where=where, log=log)[1]
    return n


def _columns(g, i, table):
    """(the columns source `i` is read by, [(target table, its columns,
    notes)] of every target): the carried columns are the same on every
    target - the group's targets hold one schema - or the copy stops
    here."""
    out, read = [], None
    db = g.sources[i].db
    for j in range(len(g.targets)):
        pair = g.pair(i, j)
        dst_t = _target_table(pair, db, table)
        sc, dc, notes = pair._move_columns(table, dst_t, db)
        if read is None:
            read = sc
        elif [n for n, _ in sc] != [n for n, _ in read]:
            raise SystemExit(f"{table}: {g.targets[j].name} holds other"
                             f" columns of it than {g.targets[0].name} -"
                             " the targets of a reshard hold one schema."
                             " Nothing more was written")
        out.append((dst_t, dc, notes))
    return read, out


def _carry(g, i, table, fn, log, where):
    """One table of source `i`: read through the hop's row filter, every
    row written to the target its key places it on, a batch a target."""
    read, targets = _columns(g, i, table)
    for n_ in targets[0][2]:
        log(f"{table}: {n_}")
    names = [n for n, _ in read]
    lost = [k for k in fn.key if k not in names]
    if lost:
        raise SystemExit(f"{table}: the shard key {', '.join(lost)} is not"
                         " carried, so a row cannot be placed by it."
                         " Nothing more was written")
    at = [names.index(k) for k in fn.key]
    n = 0
    for rows_ in g.reader(i)._every_row("src", g.sources[i].db, table, read,
                                        _where_of(g, i, table)):
        if fn.way == "owned":
            n += _write_spread(g, i, table, fn, read, targets[0][1], rows_,
                               log, where)
            continue
        groups = {}
        for r in rows_:
            j = fn.place(tuple(_text(fn.classes.get(k), r[x])
                               for k, x in zip(fn.key, at)))
            if j is None:
                raise SystemExit(
                    f"{table}: a row no target takes arrived since the rows"
                    f" were counted ({fn.said}, by its shard key"
                    f" {tuple(r[x] for x in at)}) - name a target for its"
                    " value, or reshard: {default: <hop>}. The tables"
                    " written so far are kept, and the next run goes on")
            for t in (fn.within if j == ALL else (j,)):
                groups.setdefault(t, []).append(r)
        for j, part in groups.items():
            dst_t, dc, _ = targets[j]
            n += _write(g, i, j, dst_t, dc, part, log)
    return n


# --- the copy ----------------------------------------------------------------------

def _arm(g, pair):
    """`pair`'s writes held to the claim the group's first hop holds now,
    where one is armed (`fencing`): a token of its own a pair, since each
    target's row is raised the first time that target is written."""
    from . import fencing
    tok = fencing.armed(g.lead)
    if tok is not None:
        fencing.arm(pair.hop, fencing.token_of(tok.plain()))


def _disarm(g):
    from . import fencing
    for pair in g._pairs.values():
        fencing.disarm(pair.hop)


def _unit(g, key, run, log):
    """`run()` done once across the machines of the hop. Where they share
    its tables (`share_tables`), through a claim of the unit whole
    (`claims.whole`), each target's writes held to the claim's number - a
    machine that lost the claim is refused by every target it writes -
    and waited for while another machine holds it; on one machine, run
    here."""
    from . import shared
    if not shared.together(g.lead):
        run()
        return
    from . import claims, ha
    store = shared.store_for(g.lead, log)
    claims.way(g.lead, g.db, key, store, lambda: False)
    table = claims.Table(store, claims.path(g.lead, g.db, key), ha.mover(),
                         ha.term())

    def fenced():
        for pair in list(g._pairs.values()):
            _arm(g, pair)
        try:
            return run() or 0
        finally:
            _disarm(g)
    said = False
    while True:
        rec = table.read() or {}
        st = (rec.get("state") or {}).get("0") or {}
        if st.get("state") == claims.DONE:
            return
        if claims.whole(g.lead, g.db, key, store, fenced, log):
            return
        if not said:
            log(f"{key}: another machine of the hop holds it; waiting for it"
                " to be done, or for its claim to lapse")
            said = True
        time.sleep(1)


def _prepare(g, tables, log):
    """Every target's tables made where it lacks them, its rows of another
    run's or another writer's held to the hop's `target_rows`, and the
    tables in scope emptied - within the hop's row filter - so each holds
    the group's rows and nothing else."""
    from . import interlock
    gone = 0
    for j in range(len(g.targets)):
        for i in sorted(set(tables.values())):
            g.pair(i, j).create_missing(g.sources[i].db, log)
        pair, db = g.pair(0, j), g.sources[0].db
        named = [tuple(str(t).rpartition(".")[::2]) for t in tables]
        kept = interlock.before_copy(pair.hop, pair, db, named, True, log)
        if kept:
            raise SystemExit(
                f"{g.targets[j].name}: target_rows: keep leaves"
                f" {', '.join('.'.join(p for p in k if p) for k in sorted(kept))}"
                " as they are, and a reshard places and proves every row of"
                " every table - target_rows: replace, or empty them. Nothing"
                " was written")
        for t in sorted(tables):
            dst_t = _target_table(pair, db, t)
            _, dst_where = pair._row_scope(db, t)
            n = pair.dst_engine.neutral_empty(
                "dst", db, dst_t, **({"where": dst_where} if dst_where
                                     else {}))
            if n:
                gone += n
                log(f"{dst_t} on {g.targets[j].name}: {n:,} rows removed"
                    " before the rows are placed")
    return gone


def _finish(g, log):
    """Once every unit is in: the indexes of the tables the copy made, each
    target's counters past its rows, and the proof - a target that is not
    what the function places there stops it."""
    for key, pair in sorted((k, p) for k, p in g._pairs.items()
                            if len(k) == 2):
        pair.finish_created(g.sources[key[0]].db, log)
    for j in range(len(g.targets)):
        said = g.writer(0, j).settle_target(g.sources[0].db,
                                            from_source=False)
        if said:
            log(f"{g.targets[j].name}: {said}")
    results = prove(g)
    for r in results:
        log(f"{r.scope}: {r.status.upper()} {r.detail}")
    bad = [r for r in results if r.status not in ("ok", "skip")]
    if bad:
        raise SystemExit(f"{g.db}: the reshard is not what its function"
                         " places - " + "; ".join(f"{r.scope}: {r.detail}"
                                                  for r in bad[:4]))
    return len(results)


def copy(g, go, log):
    """Every source's rows onto the targets their keys place them on: each
    table's way climbed and recorded, the targets prepared once, each
    (source, table) carried as a unit - kept in the ledger, so a run that
    stopped goes on after the units it finished - and the whole proved.
    A plan without `go`."""
    import contextlib
    tables = _tables(g)
    fns = {}
    for t in sorted(tables):
        fn, choice = function(g, t, tables[t], record=go)
        if fn is None:
            raise SystemExit(f"{g.db}.{t}: nothing was written - no way"
                             f" places its rows: {choice.reason}")
        fns[t] = fn
        log(f"{t}: {fn.said} - {choice.reason}")
    if not go:
        log(f"[{g.db}] dry-run: {len(fns)} tables of {len(g.sources)}"
            f" sources onto {len(g.targets)} targets - each target's tables"
            " emptied, every row written to the target its key places it"
            " on, and every target proved")
        return
    if (g.lead.options or {}).get("protect_target"):
        # every target kept from the application's writes before a row is
        # copied, each its own way, as one (`freeze.GroupHold`)
        from . import freeze
        freeze.protect(g.lead, Cluster(g), g.db, log)
    ledger = _ledger(g)
    where = _ledger_path(g)
    if not ledger.get("prepared"):
        _unit(g, "reshard-prepare", lambda: _prepare(g, tables, log), log)
        ledger["prepared"] = True
        _save_ledger(g, ledger)
    units = ledger.setdefault("units", {})
    held = {}
    with contextlib.ExitStack() as windows:
        for i, s in enumerate(g.sources):
            for j in range(len(g.targets)):
                windows.enter_context(g.pair(i, j).load_window(s.db, log))
        for t in sorted(fns):
            for i in _sources_of(g, fns[t]):
                if i not in held:
                    held[i] = set(g.reader(i).neutral_tables(
                        "src", g.sources[i].db))
                if t not in held[i]:
                    continue
                name = f"{i + 1}/{t}"
                if name in units:
                    log(f"{t} of {g.sources[i].name}: carried by an earlier"
                        " run")
                    continue
                got = {}

                def run(i=i, t=t, got=got):
                    got["rows"] = _carry(g, i, t, fns[t], log, where)
                    return got["rows"]
                _unit(g, f"reshard-{i + 1}-{t}", run, log)
                units[name] = {"rows": got.get("rows"),
                               "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
                _save_ledger(g, ledger)
                if "rows" in got:
                    log(f"{t} of {g.sources[i].name}: {got['rows']:,} rows"
                        " placed")
    _unit(g, "reshard-finish", lambda: _finish(g, log), log)
    ledger["done"] = True
    _save_ledger(g, ledger)
    log(f"[{g.db}] {len(g.sources)} sources placed onto {len(g.targets)}"
        " targets and proved")


def copies(g):
    """[(hop, database)] whose own `move --mode full` copies a split's or a
    consolidation's group - each part, each source - before the group's
    changes are followed; none for a reshard, which copies itself
    (`copy`)."""
    if g.kind == "split":
        return [(t.hop.name, t.db) for t in g.targets]
    if g.kind == "consolidate":
        return list(dict.fromkeys((s.hop.name, s.db) for s in g.sources))
    return []


def _token_path(g, i):
    return g.report_dir() / f"source-{i + 1}" / "position.json"


def _mark_path(g, i, j):
    return g.report_dir() / f"source-{i + 1}" / f"target-{j + 1}.json"


def begin(g, go, log):
    """Each source's change position, taken before anything of the group is
    copied (`copy_point`) - so nothing changed while the rows are read is
    lost - or kept where an earlier run took one: older, it only replays
    changes the targets take by key."""
    from . import tailctl
    for i, s in enumerate(g.sources):
        path = _token_path(g, i)
        if tailctl.saved(path) is not None:
            continue
        if not go:
            log(f"{s.name}: its change position is taken with --go, before"
                " the copy reads a row")
            continue
        g.pair(i, 0)._can_tail(s.db)
        reader = g.reader(i)
        point = reader.copy_point("src", s.db)
        tailctl.begin(reader, s.db, path, point)
        log(f"{s.name}: changes followed from {str(point)[:60]}, taken"
            " before the copy")


def move(hop, eng, dbs, mode, table, go, log):
    """`move` of a reshard hop: the copy (`--mode full`), the follow
    (`--mode cdc`), or both with the positions taken first (`full+cdc`) -
    for the whole group, whichever of its hops is run."""
    if table:
        raise SystemExit(f"{hop.name}: a reshard is carried whole - every"
                         " row is placed and proved across every target."
                         " Leave out --table")
    if mode in ("cdc", "full+cdc") and len(dbs) > 1:
        raise SystemExit(f"{hop.name}: the follow of a group runs one"
                         " database a process - --db one of"
                         f" {', '.join(dbs)}")
    for d in dbs:
        g = group(hop, d)
        log(f"[{d}] {len(g.sources)} sources"
            f" ({', '.join(s.name for s in g.sources)}) onto"
            f" {len(g.targets)} targets"
            f" ({', '.join(t.name for t in g.targets)})")
        if mode in ("full", "full+cdc"):
            if mode == "full+cdc":
                begin(g, go, log)
            copy(g, go, log)
        if mode in ("cdc", "full+cdc"):
            follow(g, go, log)


# --- the follow ----------------------------------------------------------------------

def _ident(table, key):
    return (str(table), tuple(sorted((str(k), repr(v))
                                     for k, v in key.items())))


def _before(fn, c):
    """The targets a row may be on before its change: where its old shard
    key places it - read from the change's key, or from the image of the
    row the log keeps - every target of a table they all hold, and None
    where neither says, or where a rule places it (SQL, asked of the
    source now, not of the row as it was): then every target but the
    one it goes to has it taken off by key."""
    if fn.way == "whole":
        return set(fn.within)
    if fn.way in ("one", "owned"):
        return {0}
    if fn.way == "rule":
        return None
    old = {**(c.get("before") or {}), **(c.get("key") or {})}
    texts = _texts(fn, old)
    if texts is None:
        return None
    j = fn.place(texts)
    if j is None:
        return set()
    return set(fn.within) if j == ALL else {j}


class Unplaced(SystemExit):
    """A change whose row the function places on no target."""


@dataclass
class Planned:
    """One source's batch as the targets take it: `out`, {target: deletes}
    of rows whose key moved them off it - applied first, on every target;
    `into`, {target: changes} - each target's one transaction, in the
    order the source first touched each row; `asked`, the rows another
    source may hold now (a key that moves between sources); `partial`, the
    rows written where they were not, whose change does not carry every
    column; `rule`, the rows a split part's rule is to be asked about;
    `moved`, the rows a key moved across targets."""
    out: dict
    into: dict
    asked: list = field(default_factory=list)
    partial: list = field(default_factory=list)
    rule: list = field(default_factory=list)
    moved: int = 0


def plan_batch(changes, fns, n, held=None, filled=None, placed=None):
    """One source's batch of row changes, in its log's order, as `Planned`
    for `n` targets.

    Each row is followed through the batch to its last state: where it was
    before the batch (`_before`, from its first change) and where its last
    state places it. A row whose key moved it is removed from every target
    it may have been on but the one it goes to - before any target is
    written - and written there; a row removed is removed where it was.
    Rows go in the order the source first touched them, the order an
    engine applies a batch it collapses by key.

    `held`, {row: its values} of rows another source holds now: the row is
    that source's - written as it has it, taken off every other target.
    `filled`, {row: its values now, or None}, completes a row written
    where it was not from what the source holds now - a row gone since is
    left to its own delete, which follows in the log. `placed`, {row: the
    targets whose rule takes it now, or GONE} of a split's rows."""
    held = held or {}
    filled = filled or {}
    placed = placed or {}
    state, order = {}, []

    def touch(table, key, fn, first, before):
        k = _ident(table, key)
        st = state.get(k)
        if st is None:
            st = state[k] = {"table": table, "key": dict(key), "fn": fn,
                             "first": first, "before": before,
                             "final": None, "values": None}
            order.append(k)
        return st

    def written(st, key, values):
        was = st["values"] if st["final"] == "up" else {}
        st["values"] = {**was, **key, **(values or {})}
        st["final"] = "up"

    for c in changes:
        t = c["table"]
        fn = fns.get(t)
        if fn is None:
            continue
        op, key, values = c["op"], dict(c["key"]), c.get("values") or {}
        if op == "insert":
            written(touch(t, key, fn, "insert", set()), key, values)
        elif op == "update":
            new = {p: values.get(p, v) for p, v in key.items()}
            if new != key:
                # the row's own key moved: the old row goes where it was,
                # a new one comes where its key places it
                st = touch(t, key, fn, "update", _before(fn, c))
                st["final"], st["values"] = "del", None
                written(touch(t, new, fn, "insert", set()), new, values)
            else:
                written(touch(t, key, fn, "update", _before(fn, c)), key,
                        values)
        elif op == "delete":
            st = touch(t, key, fn, "delete", _before(fn, c))
            st["final"], st["values"] = "del", None
    asked, partial, rule = [], [], []
    for k in order:
        st = state[k]
        if st["first"] == "insert" or st["final"] == "del":
            asked.append((st["table"], st["key"]))
        if k in held:
            # another source holds the row now: where this source's copy
            # of it sits is not known here, and its own is the row
            st["before"] = None
            if st["final"] == "del":
                st["final"], st["values"] = "up", dict(held[k])
        cols = st["fn"].columns
        if st["final"] == "up" and cols and not set(cols) <= set(
                st["values"]):
            if k in filled:
                if filled[k] is None:
                    st["final"] = GONE
                else:
                    st["values"] = {**filled[k], **st["values"]}
            else:
                partial.append((st["table"], st["key"]))
        if st["final"] == "up" and st["fn"].way == "rule":
            if k not in placed:
                rule.append((st["table"], st["key"]))
            elif placed[k] == GONE:
                st["final"] = GONE
    out = {j: [] for j in range(n)}
    into = {j: [] for j in range(n)}
    moved = 0
    for k in order:
        st = state[k]
        fn, t, key = st["fn"], st["table"], st["key"]
        everywhere = set(fn.within)
        before = everywhere if st["before"] is None else st["before"]
        if st["final"] == "up":
            if fn.way == "rule":
                if k not in placed:
                    continue
                to = set(placed[k]) & everywhere
                if not to and not fn.stays:
                    raise Unplaced(
                        f"{t}: a change leaves its row in no part's rule (by"
                        f" key {_keys([tuple(key.values())])}) - the follow"
                        " stops before it, its position kept: make the"
                        " rules take every row, or split: {rest: stays}")
            else:
                j = fn.place(_texts(fn, st["values"]))
                if j is None:
                    raise Unplaced(
                        f"{t}: a change places its row on no target"
                        f" ({fn.said}, by key {_keys([tuple(key.values())])})"
                        " - the follow stops before it, its position kept:"
                        " name a target for the value (reshard: default)"
                        " and run it again")
                to = everywhere if j == ALL else {j}
            leaving = before - to
            if leaving and st["before"]:
                moved += 1
            for x in sorted(leaving):
                out[x].append({"op": "delete", "table": t, "key": key})
            op = "insert" if st["first"] == "insert" else "update"
            for x in sorted(to):
                into[x].append({"op": op, "table": t, "key": key,
                                "values": st["values"]})
        elif st["final"] == "del":
            for x in sorted(before):
                into[x].append({"op": "delete", "table": t, "key": key})
    return Planned(out, into, asked, partial, rule, moved)


def _by_key(g, k, fn, table, keys, where=None):
    """{row: its values} of the rows source `k` holds now of `keys`, read
    by key through `where`."""
    reader, db = g.reader(k), g.sources[k].db
    if table not in reader.neutral_tables("src", db):
        return {}
    cls = _classes(reader, "src", db, table)
    cols = [(c, cls.get(c)) for c in (fn.columns or fn.pk) if c in cls]
    got = reader.neutral_rows_by_key(
        "src", db, table, cols, fn.pk,
        [tuple(key[p] for p in fn.pk) for key in keys], where=where)
    out = {}
    for key in keys:
        row = got.get(tuple(_text(cls.get(p), key[p]) for p in fn.pk))
        if row is not None:
            out[_ident(table, key)] = dict(zip([c for c, _ in cols], row))
    return out


def _held_elsewhere(g, i, asked, fns):
    """{row: its values} of the rows `asked` that a source other than `i`
    holds now: a key that moves between sources arrives as a delete in one
    log and an insert in another, in no order the two logs share - the
    source holding it now is the one whose copy the targets keep."""
    by_table = {}
    for t, key in asked:
        by_table.setdefault(t, []).append(key)
    out = {}
    for t, keys in by_table.items():
        for k in range(len(g.sources)):
            if k == i:
                continue
            for ident, values in _by_key(g, k, fns[t], t, keys,
                                         _where_of(g, k, t)).items():
                out.setdefault(ident, values)
    return out


def _now(g, i, partial, fns):
    """{row: its values now, or None where source `i` no longer has it}."""
    by_table = {}
    for t, key in partial:
        by_table.setdefault(t, []).append(key)
    out = {}
    for t, keys in by_table.items():
        got = _by_key(g, i, fns[t], t, keys, _where_of(g, i, t))
        for key in keys:
            out[_ident(t, key)] = got.get(_ident(t, key))
    return out


def _rule_places(g, i, asked, fns):
    """{row: the targets whose part's rule takes it now, or GONE where the
    source no longer holds it} - each rule is SQL, asked of the source by
    key the way the tail asks a row filter (`HeteroEngine._in_scope`):
    the row's state now converges, a row that crossed a rule and back in
    one batch is where its rule has it."""
    by_table = {}
    for t, key in asked:
        by_table.setdefault(t, []).append(key)
    out = {}
    for t, keys in by_table.items():
        fn = fns[t]
        there = _by_key(g, i, fn, t, keys)
        # a split's parts' rules; a consolidation's source's own filter
        rules = (fn.rules if g.kind == "split"
                 else {0: _where_of(g, i, t)})
        taken = {j: (there if not rule else
                     _by_key(g, i, fn, t, keys, rule))
                 for j, rule in rules.items()}
        for key in keys:
            ident = _ident(t, key)
            out[ident] = (GONE if ident not in there else
                          {j for j, got in taken.items() if ident in got})
    return out


def _same(a, b):
    import json
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(
        b, sort_keys=True, default=str)


def _shape(g, i, plan, log):
    """Each target's changes of source `i` as its target holds them: a
    consolidation's under the source's value, its keys shifted as its copy
    shifted them, a key past its range or one another source holds
    refused (`consolidate.follow_change`); every other group's as they
    are."""
    if g.kind != "consolidate":
        return plan
    from . import consolidate
    them = [consolidate.Source(x.hop, x.db, x.value) for x in g.sources]
    shapes = g.__dict__.setdefault("_shapes", {})
    if i not in shapes:
        shapes[i] = consolidate.follow_plan(them[i], them)
    plan.into = {j: consolidate.follow_changes(shapes[i], g.pair(i, j),
                                               them[i], recs)
                 for j, recs in plan.into.items()}
    plan.out = {j: consolidate.follow_changes(shapes[i], g.pair(i, j),
                                              them[i], recs)
                for j, recs in plan.out.items()}
    return plan


def _apply(g, i, changes, fn_of, token, run, log):
    """One batch of source `i`, planned whole (`plan_batch`) and applied:
    every target's removals of rows a key moved off it first, then each
    target's changes in one transaction under the batch's number where the
    target keeps one (`marks`) - a target that committed this very batch
    before a stop is not written again. (changes applied, rows moved,
    whether a numbered batch was written)."""
    import json

    from . import canon, drift, failpoint, fencing
    db = g.sources[i].db
    rows_ = []
    for c in changes:
        fn = fn_of(c["table"])
        if canon.is_ddl(c):
            if fn is None:
                continue
            raise SystemExit(
                f"{db}.{c['table']}: {c.get('kind')} on {g.sources[i].name}"
                " - the group's follow stops at a change of a table's shape"
                " or of its whole contents, its position kept before it:"
                " make the change on every target, then move again (--mode"
                " full+cdc)")
        if fn is None or drift.transient(c["table"]):
            continue
        if fn.way == "whole" and fn.home != i:
            # a table its targets take whole is carried from the first
            # source that holds it; the others hold copies of it
            continue
        rows_.append({k: v for k, v in c.items()
                      if k not in canon.FROM_THE_LOG})
    if not rows_:
        return 0, 0, False
    fns = {c["table"]: fn_of(c["table"]) for c in rows_}
    n = len(g.targets)
    plan = plan_batch(rows_, fns, n)
    held = (_held_elsewhere(g, i, plan.asked, fns)
            if g.kind == GOAL and len(g.sources) > 1 and plan.asked else {})
    filled = _now(g, i, plan.partial, fns) if plan.partial else {}
    placed = _rule_places(g, i, plan.rule, fns) if plan.rule else {}
    if held or filled or placed:
        plan = plan_batch(rows_, fns, n, held, filled, placed)
    plan = _shape(g, i, plan, log)
    for j in range(n):
        if plan.out[j]:
            g.writer(i, j).__dict__.pop("_batch_seen", None)
            g.pair(i, j).carry(db, plan.out[j])
    if any(plan.out.values()):
        failpoint.hit("reshard.moved_out")
    numbered = False
    for j in range(n):
        if not plan.into[j]:
            continue
        if _same(run["ahead"].get((i, j)), token):
            log(f"{g.targets[j].name} had committed this batch of"
                f" {g.sources[i].name} before the stop: not written again")
            continue
        w = g.writer(i, j)
        if run["numbered"].get((i, j)):
            w._batch_seen = json.dumps(
                {"token": token, "batch": run["batch"][i] + 1,
                 "run": run["run"][i], **fencing.marked(g.pair(i, j).hop)},
                default=str)
            numbered = True
        try:
            g.pair(i, j).carry(db, plan.into[j])
        finally:
            w.__dict__.pop("_batch_seen", None)
        failpoint.hit("reshard.applied")
    return len(rows_), plan.moved, numbered


def _functions(g):
    """{table: its Function} of every table the group carries, or
    SystemExit naming a table no way places."""
    fns = {}
    for t, home in _tables(g).items():
        fn, choice = function(g, t, home)
        if fn is None:
            raise SystemExit(f"{g.db}.{t}: no way places its rows -"
                             f" {choice.reason}")
        fns[t] = fn
    return fns


def follow(g, go, log):
    """Every source's changes onto the targets, by the same function, on
    the one follow loop (`tailctl.run_loop`) until it is stopped - ctrl-c,
    `tail.stop`, the cutover - or caught up where the hop's `follow_until`
    says so: a batch of each source in turn, each planned whole and
    applied (`_apply`), its position kept once every target has it. A
    connection lost is read again from the kept positions, which the
    targets take by key or their marks settle."""
    import contextlib
    import secrets

    from . import marks, tailctl, twoway
    from .engines.hetero import HeteroEngine
    where = g.report_dir()
    if go and tailctl.alive(where, g.lead):
        raise SystemExit(f"{g.db}: the group's follow already runs - one"
                         " follow a group")
    fns = _functions(g)
    leaves = {}
    for t in fns:
        leaves.setdefault(str(t).split(".")[-1], []).append(t)

    def fn_of(table):
        got = fns.get(table)
        leaf = str(table).split(".")[-1]
        if got is None and len(leaves.get(leaf, [])) == 1:
            got = fns[leaves[leaf][0]]
        return got
    n_src, n_dst = len(g.sources), len(g.targets)
    paths = [_token_path(g, i) for i in range(n_src)]
    envs = []
    for i, s in enumerate(g.sources):
        g.pair(i, 0)._can_tail(s.db)
        reader = g.reader(i)
        env = tailctl.position(paths[i])
        if env is None and go:
            env = tailctl.begin(reader, s.db, paths[i],
                                reader.copy_point("src", s.db))
            log(f"{s.name}: no position was kept - followed from now, and"
                " what changed before it is the copy's")
        elif env is not None:
            tailctl.same_source(reader, s.db, paths[i], env.get("token"))
        envs.append(env)
    if not go:
        log(f"[{g.db}] dry-run: the changes of {n_src} sources followed onto"
            f" {n_dst} targets by the function of each table; a key that"
            " moves its row removed from the target it left before it is"
            " written on the one it reaches")
        return
    batch, base = [], []
    for i in range(n_src):
        env = envs[i]
        if env.get("batch") is None:
            # a number no earlier run's marks can hold, kept before
            # anything is applied (`HeteroEngine._saved_batch`)
            b = secrets.randbits(48)
            env = envs[i] = tailctl.keep(paths[i], {**env, "batch": b,
                                                    "run": b})
        batch.append(int(env["batch"]))
        base.append(env.get("run", env["batch"]))
    numbered, ahead = {}, {}
    for i, s in enumerate(g.sources):
        for j in range(n_dst):
            numbered[(i, j)] = marks.settle_exact(
                g.pair(i, j), s.db, _mark_path(g, i, j), log) is not None
            if not numbered[(i, j)]:
                continue
            try:
                got = twoway.committed_ahead(g.writer(i, j), s.db, batch[i],
                                             "table", run=base[i])
            except Exception:  # noqa: BLE001 - not known: written again
                got = None
            if got is not None:
                ahead[(i, j)] = got[0]
    run = {"batch": batch, "run": base, "numbered": numbered,
           "ahead": ahead}
    st = {"saved": [e.get("token") for e in envs],
          "read_at": [e.get("token") for e in envs],
          "carried": [[] for _ in range(n_src)], "limit": [BATCH] * n_src,
          "fence": [None] * n_src, "moved": 0}
    cut_ok = [bool(getattr(g.reader(i), "READ_ENDS_INSIDE_TRANSACTIONS",
                           False)) for i in range(n_src)]

    def look(token):
        went, settled = 0, True
        for i, s in enumerate(g.sources):
            reader = g.reader(i)
            got, read_to = reader.neutral_changes(
                "src", s.db, st["read_at"][i], limit=st["limit"][i])
            fewer = len(got) < st["limit"][i]
            st["limit"][i] = (BATCH if fewer
                              else min(st["limit"][i] * 2, BATCH_MOST))
            changes, st["carried"][i] = st["carried"][i] + got, []
            st["read_at"][i] = at = read_to
            cut = HeteroEngine._commit_cut(changes) if cut_ok[i] else None
            if cut is not None:
                changes, st["carried"][i] = changes[:cut], changes[cut:]
                at = changes[-1]["txn_at"]
            applied, m, marked = _apply(g, i, changes, fn_of, at, run, log)
            for key in [k for k in ahead if k[0] == i]:
                ahead.pop(key)
            if applied or not _same(at, st["saved"][i]):
                batch[i] += 1 if marked else 0
                envs[i] = tailctl.moved(paths[i], envs[i], at,
                                        batch=batch[i])
                st["saved"][i] = at
            went += applied
            st["moved"] += m
            settled = settled and fewer and not st["carried"][i]
        if went:
            log(f"{went:,} changes"
                + (f", {st['moved']:,} rows moved across targets so far"
                   if st["moved"] else ""))
        return list(st["saved"]), went, settled

    def reached(token):
        out = True
        for i, s in enumerate(g.sources):
            reader = g.reader(i)
            if st["fence"][i] is None:
                st["fence"][i] = reader.log_position("src", s.db)
            if st["fence"][i] is None:
                return None
            got = reader.position_reached(st["saved"][i], st["fence"][i])
            if got is None:
                return None
            out = out and bool(got)
        return out

    def lost(e, token):
        st["read_at"] = list(st["saved"])
        st["carried"] = [[] for _ in range(n_src)]
        return list(st["saved"])
    secrets_ = tuple(x for s in g.sources for x in (s.hop.source.password,))
    secrets_ += tuple(t.hop.target.password for t in g.targets)
    try:
        with contextlib.ExitStack() as windows:
            for i, s in enumerate(g.sources):
                for j in range(n_dst):
                    windows.enter_context(g.pair(i, j).load_window(s.db,
                                                                   log))
            log(f"following {n_src} sources onto {n_dst} targets")
            tailctl.run_loop(where, g.db, look, log, "changes", hop=g.lead,
                             go=True, token=list(st["saved"]),
                             reached=reached, lost=lost,
                             secrets=tuple(x for x in secrets_ if x))
    finally:
        for i in range(n_src):
            try:
                g.reader(i).release_changes()
            except Exception:  # noqa: BLE001 - let go on the way out
                pass


# --- the proof ---------------------------------------------------------------------

def judge(want, have):
    """What the targets hold against what the function places there.

    `want`, {key: (target index, ALL or None, the row's digest)} of every
    source row; `have`, [{key: digest}] of each target. Returns ([{rows,
    missing, extra, changed}] a target, keys on more than one target that
    every target is not to hold, keys the function places nowhere, keys on
    no target). A key on a target the function does not place it on is
    that target's extra and the target it belongs to's missing; a row as
    the source does not have it, changed."""
    per = []
    for j, got in enumerate(have):
        expect = {k: d for k, (p, d) in want.items() if p == j or p == ALL}
        per.append({"rows": len(expect),
                    "missing": [k for k in expect if k not in got],
                    "extra": [k for k in got if k not in expect],
                    "changed": [k for k in expect
                                if k in got and got[k] != expect[k]]})
    where = {}
    for j, got in enumerate(have):
        for k in got:
            where.setdefault(k, []).append(j)
    twice = [k for k, js in where.items()
             if len(js) > 1 and want.get(k, (None, None))[0] != ALL]
    nowhere = [k for k, (p, _) in want.items() if p is None]
    lost = [k for k, (p, _) in want.items()
            if p is not None and k not in where]
    return per, twice, nowhere, lost


def _there(pair, db):
    dst = pair.dst_engine
    return set() if dst.target_missing(db) else set(dst.neutral_tables(
        "dst", db))


def _prove_table(g, t, fn):
    """[Result] of one table: a line a target and one across them."""
    from collections import Counter

    from . import consolidate
    from .engines.base import Result
    out, cols = [], []
    db = g.sources[fn.home].db
    for j in range(len(g.targets)):
        pair = g.pair(fn.home, j)
        dst_t = _target_table(pair, db, t)
        if dst_t not in _there(pair, db):
            out.append(Result("reshard", f"{db}.{t} on {g.targets[j].name}",
                              "diff", "not on the target"))
            continue
        sc, dc, notes = pair._comparable_columns(
            db, pair.src_engine, t, pair.dst_engine, dst_t)
        cols.append((j, pair, dst_t, sc, dc, notes))
    if out:
        return out
    sc = cols[0][3]
    names = [n for n, _ in sc]
    for j, _, _, sc_j, _, _ in cols[1:]:
        if [n for n, _ in sc_j] != names:
            return [Result("reshard", f"{db}.{t}", "diff",
                           f"{g.targets[j].name} holds other columns of it"
                           f" than {g.targets[0].name}")]
    at = [names.index(k) for k in fn.pk if k in names]
    pair0 = cols[0][1]
    if not fn.pk or len(at) != len(fn.pk):
        if fn.way != "whole":
            return [Result("reshard", f"{db}.{t}", "error",
                           "no key compared to place its rows by")]
        # a table every target holds whole, without a key: as a multiset
        # of rows, so a row there twice on one side is not one row
        a = Counter()
        for rows_ in g.reader(fn.home)._every_row(
                "src", db, t, sc, _where_of(g, fn.home, t)):
            a.update(consolidate._hash(pair0._row_text(sc, r))
                     for r in rows_)
        for j, pair, dst_t, _, dc, _ in cols:
            b = Counter()
            for rows_ in pair.dst_engine._every_row("dst", db, dst_t, dc):
                b.update(consolidate._hash(pair._row_text(dc, r))
                         for r in rows_)
            gone, more = sum((a - b).values()), sum((b - a).values())
            said = ([f"{gone:,} rows missing"] if gone else []) + (
                [f"{more:,} rows the source does not have"] if more else [])
            out.append(Result("reshard", f"{db}.{t} on {g.targets[j].name}",
                              "diff" if said else "ok",
                              "; ".join(said) or f"{sum(a.values()):,} rows,"
                              " whole, as the source has them"))
        return out
    want, dup = {}, []
    for i in _sources_of(g, fn):
        reader, sdb = g.reader(i), g.sources[i].db
        if t not in reader.neutral_tables("src", sdb):
            continue
        for rows_ in reader._every_row("src", sdb, t, sc,
                                       _where_of(g, i, t)):
            for r in rows_:
                texts = pair0._row_text(sc, r)
                key = tuple(texts[x] for x in at)
                if key in want:
                    dup.append(key)
                want[key] = (fn.place(_texts(fn, dict(zip(names, r)))),
                             consolidate._hash(texts))
    have = []
    for j, pair, dst_t, _, dc, _ in cols:
        got = {}
        for rows_ in pair.dst_engine._every_row("dst", db, dst_t, dc):
            for r in rows_:
                texts = pair._row_text(dc, r)
                got[tuple(texts[x] for x in at)] = consolidate._hash(texts)
        have.append(got)
    per, twice, nowhere, lost = judge(want, have)
    for (j, _, _, _, _, notes), d in zip(cols, per):
        said = []
        if d["missing"]:
            said.append(f"{len(d['missing']):,} rows its key places here are"
                        f" not here ({_shown(d['missing'])})")
        if d["extra"]:
            said.append(f"{len(d['extra']):,} rows here that the function"
                        " places on another target, or no source has"
                        f" ({_shown(d['extra'])})")
        if d["changed"]:
            said.append(f"{len(d['changed']):,} rows differ from the"
                        f" source's ({_shown(d['changed'])})")
        out.append(Result(
            "reshard", f"{db}.{t} on {g.targets[j].name}",
            "diff" if said else "ok", "; ".join(said) or
            f"{d['rows']:,} rows, each as its source has it"
            + (f" ({'; '.join(notes)})" if notes else ""), "",
            "" if not said else f"migkit move {g.lead.name} --mode full"
                                " --go"))
    said = []
    if dup and fn.way != "whole":
        said.append(f"{len(dup):,} keys more than one source holds"
                    f" ({_shown(dup)})")
    if twice:
        said.append(f"{len(twice):,} keys on more than one target"
                    f" ({_shown(twice)})")
    if nowhere:
        said.append(f"{len(nowhere):,} source rows the function places on"
                    f" no target ({_shown(nowhere)})")
    if lost:
        said.append(f"{len(lost):,} source rows on no target"
                    f" ({_shown(lost)})")
    out.append(Result(
        "reshard", f"{db}.{t} across the targets", "diff" if said else "ok",
        "; ".join(said) or
        (f"{len(want):,} rows, on every one of the {len(fn.within)}"
         " targets" if fn.way == "whole" else
         f"{len(want):,} rows of {len(g.sources)} sources, each on exactly"
         f" one of the {len(g.targets)} targets - {fn.said}")))
    if fn.way == "owned":
        out += _prove_parts(g, t, fn)
    return out


def _prove_parts(g, t, fn):
    """[Result] of a table the target cluster spreads: each part read on
    its own node, where migkit reaches it, and every row there one the
    cluster's function places in that part - a row a direct write put in
    the wrong part is one the cluster's router would never find. A part
    no address names was only written through the router, which places
    each row itself."""
    from .engines.base import Result
    sp, db = fn.spread, g.sources[fn.home].db
    bad, read = [], 0
    cols = [(c, fn.classes.get(c)) for c in dict.fromkeys(fn.pk + fn.key)]
    for k, (_, part) in enumerate(sp.parts):
        node = _node_pair(g, fn.home, k, sp)
        if node is None:
            continue
        for rows_ in node.dst_engine._every_row("dst", db, part, cols):
            values = [dict(zip([c for c, _ in cols], r)) for r in rows_]
            owners = sp.owners([_texts(fn, v) for v in values])
            for v, got in zip(values, owners):
                read += 1
                if got != k:
                    bad.append(tuple(_text(fn.classes.get(p), v[p])
                                     for p in fn.pk))
    if not read:
        return [Result("reshard", f"{db}.{t} parts", "skip",
                       "no part of it is on a node migkit reaches: every row"
                       " went through the cluster's router, which places"
                       " it")]
    if bad:
        return [Result("reshard", f"{db}.{t} parts", "diff",
                       f"{len(bad):,} rows sit in a part the cluster's own"
                       f" function does not place them in ({_shown(bad)})")]
    return [Result("reshard", f"{db}.{t} parts", "ok",
                   f"{read:,} rows read on their nodes, each in the part the"
                   " cluster places it in")]


def prove(g):
    """[Result] of every table of a reshard: a line a target - each key
    there the one the function places there, as its source has it - and
    one across them: no key on two targets, no key two sources hold, every
    source row on a target."""
    from .engines.base import Result
    out = []
    for t, home in sorted(_tables(g).items()):
        fn, choice = function(g, t, home)
        if fn is None:
            out.append(Result("reshard", f"{g.db}.{t}", "error",
                              f"no way places its rows: {choice.reason}"))
            continue
        try:
            out += _prove_table(g, t, fn)
        except Exception as e:  # noqa: BLE001 - said as the table's error
            out.append(Result("reshard", f"{g.db}.{t}", "error",
                              f"could not be read: {_last(e)}"))
    return out


def proofs(g):
    """[Result] of the whole group: a reshard's own proof; a split's
    placement (`split.prove`) and every part's rows held to the source
    through its rule (its own check); every consolidation source's rows
    under its value (`consolidate.prove`)."""
    from .engines import get_engine
    if g.kind == GOAL:
        return prove(g)
    out, seen = [], set()
    if g.kind == "split":
        from . import split
        out += split.prove(g.lead, get_engine(g.lead), g.db)
        for t in g.targets:
            out += get_engine(t.hop).check_data(t.db)
        return out
    from . import consolidate
    for name, db in copies(g):
        hop = next(h for h in g.hops if h.name == name)
        for r in consolidate.prove(hop, get_engine(hop), db):
            if (r.scope, r.status, r.detail) not in seen:
                seen.add((r.scope, r.status, r.detail))
                out.append(r)
    return out


def check(hop, eng, db):
    """`check` of a reshard hop: the proof of the whole group."""
    from .engines.base import Result
    try:
        g = group(hop, db)
    except SystemExit as e:
        return [Result("reshard", db, "error", str(e))]
    return prove(g)


# --- the cutover -------------------------------------------------------------------

class _Sides:
    """One side of a group as the cutover's freeze and its target's hold
    see it (`freeze.GroupHold`): each source - or each target - with the
    hop it is held under, its engine and its database."""
    CANON_ENGINE = "group"

    def __init__(self, g, side):
        self.group, self.side = g, side

    @property
    def parts(self):
        g, out = self.group, []
        if self.side == "src":
            for i, s in enumerate(g.sources):
                pair = g.pair(i, 0)
                out.append((s.name, pair.hop, pair, s.db))
        elif g.kind == GOAL:
            for j, t in enumerate(g.targets):
                pair = g.pair(0, j)
                out.append((t.name, pair.hop, pair, g.sources[0].db))
        else:
            from .engines import get_engine
            # a split's part, a consolidation's source: the target is kept
            # under the hop whose own move kept it (`protect_target`)
            for name, db in copies(g):
                hop = next(h for h in g.hops if h.name == name)
                out.append((f"{name}/{db}", hop, get_engine(hop), db))
        return out


class Cluster:
    """A group as the cutover's one engine (`cutover.run`): its sources
    frozen together (`freeze.GroupHold`), its final check the group's
    proof (`proofs`), its counters every target's own."""
    REPLICATES_THE_SERVER = False

    def __init__(self, g):
        self.group, self.hop = g, g.lead
        self.src_engine = _Sides(g, "src")
        self.dst_engine = _Sides(g, "dst")

    def databases(self):
        return [self.group.db]

    def maps_columns(self):
        return False

    def check_data(self, db, table=None, stream=None, with_counts=False):
        return proofs(self.group)

    def repair_plan(self, db, kind):
        """Each target's counters past its own rows, as its own engine
        raises them; `by` names the target an action is for."""
        out = []
        for j in range(len(self.group.targets)):
            for a in self.group.pair(0, j).repair_plan(
                    self.group.sources[0].db, kind):
                if a.kind != "sequences":
                    continue
                a.by = str(j)
                a.note = f"{self.group.targets[j].name}: {a.note}"
                out.append(a)
        return out

    def apply(self, db, action):
        return self.group.pair(0, int(action.by or 0)).apply(
            self.group.sources[0].db, action)


class _Fence:
    """The follow's drain, as the cutover asks it: every source's position
    now, and the follow waited for until its kept position of each has
    passed it."""
    CANON_ENGINE = "group"

    def __init__(self, g):
        self.group = g

    def src_lsn(self, db):
        from . import tailctl
        g = self.group
        if not tailctl.alive(g.report_dir(), g.lead):
            return None
        out = {}
        for i, s in enumerate(g.sources):
            pos = g.reader(i).log_position("src", s.db)
            if pos is None:
                return None
            out[s.name] = pos
        return out

    def fence_wait(self, db, pos, timeout=300):
        from . import tailctl
        g = self.group
        if not pos:
            return None
        began = time.monotonic()
        while time.monotonic() - began < timeout:
            reached = []
            for i, s in enumerate(g.sources):
                have = tailctl.saved(_token_path(g, i))
                got = (g.reader(i).position_reached(have, pos[s.name])
                       if have is not None else False)
                if got is None:
                    return None
                reached.append(got)
            if all(reached):
                return True
            time.sleep(tailctl.POLL)
        return False


def leg(g, say):
    """The group's follow as the cutover drains it, pauses it and ends it
    (`cutover.Leg`)."""
    from . import cutover, tailctl
    where = g.report_dir()

    def down(d):
        if not tailctl.pause(where, 60):
            raise SystemExit(f"{d}: the group's follow did not pause")
        return []

    def up(d):
        tailctl.resume(where)

    def end(d):
        if not tailctl.alive(where, g.lead):
            return
        tailctl.stop(where)
        tailctl.resume(where)
        stop = time.monotonic() + 60
        while tailctl.alive(where, g.lead) and time.monotonic() < stop:
            time.sleep(tailctl.POLL)
        if tailctl.alive(where, g.lead):
            raise SystemExit(f"{d}: the group's follow did not end")
        say(f"  {d}: the group's follow ended")
    if not tailctl.alive(where, g.lead):
        return cutover.Leg("none", why="no follow of the group runs: start"
                                       " it with --mode cdc")
    return cutover.Leg("tail", fence=_Fence(g), down=down, up=up, end=end)


def cut_over(hop, dbs, go, say, approve):
    """The cutover of a whole group at once, on the cutover's own path
    (`cutover.run`): every source frozen, the follow drained past every
    source's position taken after the freeze, the group's proof, each
    target's counters, the flip - undone on any failure."""
    from . import cutover
    if len(dbs) != 1:
        raise SystemExit(f"{hop.name}: a group is cut over one database at"
                         " a time - --db one of " + ", ".join(dbs))
    if (hop.options or {}).get("reverse") or (hop.options or {}).get(
            "two_way"):
        raise SystemExit(f"{hop.name}: a group has no stream back - its"
                         " targets hold other rows than each of its"
                         " sources")
    g = group(hop, dbs[0])
    return cutover.run(g.lead, Cluster(g), [g.db], go, leg(g, say), say,
                       approve)
