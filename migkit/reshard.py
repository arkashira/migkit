"""Resharding (backlog W7): the rows of N sources placed on M targets by
one shard function - a split, a merge, a new shard key - each row once,
proved, and followed so the reshard ends with a cutover, not a stop.

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

**The group** (`group`) is the hops named, followed through each one's
`with`: its sources are the distinct sources they read, its targets the
distinct targets they write, each named by the first hop (by name) that
says it - so every hop of the group finds the same order, and hash places
a row on the same target whichever hop runs. Its state - positions, the
copy's ledger, the follow's files - is kept under its first hop by name.

**One function places every row** (`Function`): `hash` (the canonical text
of the shard key, an 8-byte BLAKE2b, modulo the targets), `range` (upper
bounds, exclusive, one per target hop, `null` the last), `list` and
`lookup` (a value's target, `default` for one not named), or `target` -
the target cluster's own distribution read and honoured (`Spread`: Citus's
distribution column and shard placements, MongoDB's shard key and chunks,
Cassandra's partitioner token, Vitess's vindex). `auto` takes the target's
own where the group has one target that distributes the table, hash
otherwise. How each table is placed is climbed as a way (`table_ways`).

**Before the move** (`problems`): every source row placed by the function
on the sources - a row no target takes, a key two sources hold, a table
with no key to place by - refused with the keys, before anything is written.

**The copy** (`copy`): each target's tables made and emptied once, then
each source's tables read and every row written to the target its key
places it on, a batch a target (`write_ways`); a table the target cluster
spreads goes in batches a node, to the node that owns them where it can be
reached, through the cluster's router otherwise (`node_ways`). Units are
handed out across machines through claims where the hop shares its
tables (`claims`).

**The follow** (`follow`): one loop over every source's change log, each
change routed by the same function. A batch is planned whole
(`plan_batch`): a row whose shard key moved is removed from the target it
left before it is written on the one it reaches - every target's removals
first, then every target's writes - so no row is on two targets at once;
each target's writes of a source's batch are one transaction, numbered by
its mark (`marks`), so after a stop a target that committed the batch is
not written again. A key that moves between sources is asked of every
source as it is now (`_held_elsewhere`).

**Proved** (`prove`, `judge`): every target read by key and digest - each
key on exactly the target its key places it on, as the source has it, no
key on two targets, and the targets together the sources.

**The cutover** (`cut_over`): the cutover's own path, run over the whole
group at once (`Cluster`, `freeze.GroupHold`, `leg`): every source frozen,
the follow drained past every source's position taken after the freeze,
the proof across every target, every target's counters, the flip.
"""
import dataclasses
import hashlib
import json
import time
from dataclasses import dataclass, field

GOAL = "reshard"
OPTION = "reshard"
BYS = ("auto", "hash", "range", "list", "lookup", "target")
EXAMPLES = 3
#: changes a read of the follow asks for, and the most it grows to while
#: it is behind - the tail's own (`HeteroEngine.TAIL_BATCH`)
BATCH, BATCH_MOST = 1000, 16000
#: what `plan_batch` places a change of a table every target takes whole on
ALL = -1


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
    for hop_name, listed in (values or {}).items() \
            if isinstance(values, dict) else ():
        for v in _names(listed):
            if v in seen and seen[v] != str(hop_name):
                wrong.append(f"values: {v} is listed for both {seen[v]} and"
                             f" {hop_name}")
            seen.setdefault(v, str(hop_name))
    if isinstance(ranges, dict):
        open_ = [h for h, b in ranges.items() if b is None]
        if len(open_) > 1:
            wrong.append("ranges: only the last range is open (null) -"
                         f" {', '.join(map(str, open_))} all are")
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


def _alike(conf):
    """What every hop of a group says alike: all of it but `with`."""
    return {k: v for k, v in conf.items() if k != "with"}


# --- the group -----------------------------------------------------------------------

@dataclass
class Side:
    """One source or target of the group: the first hop by name that reads
    or writes it, and the server it is."""
    name: str
    hop: object

    @property
    def endpoint(self):
        return self.hop.source if self.kind == "src" else self.hop.target

    kind: str = "src"


def _where(ep):
    return (str(ep.host or ""), int(ep.port or 0))


def _target_of(hop):
    """A target as the group tells targets apart: the server, and the
    database names its hop writes into."""
    return (_where(hop.target),
            tuple(sorted((str(k), str(v))
                         for k, v in (hop.db_map or {}).items())))


@dataclass
class Group:
    """The hops of one reshard and what they name."""
    lead: object
    hops: list
    sources: list
    targets: list
    conf: dict
    _pairs: dict = field(default_factory=dict)

    def index(self, hop_name):
        """The target a hop of the group writes, by its index."""
        hop = next((h for h in self.hops if h.name == hop_name), None)
        if hop is None:
            return None
        want = _target_of(hop)
        for j, t in enumerate(self.targets):
            if _target_of(t.hop) == want:
                return j
        return None

    def pair_hop(self, i, j):
        """The hop source `i` is carried to target `j` by: the lead's rules
        between their servers, one writer a batch (so a batch is one
        transaction its mark can say), named apart so its reports, its
        slot and its batch marks are its own."""
        base = self.lead
        src, dst = self.sources[i].hop, self.targets[j].hop
        return dataclasses.replace(
            base, name=f"{base.name}-s{i + 1}t{j + 1}", source=src.source,
            target=dst.target, db_map=dict(dst.db_map or {}), workers=1,
            options=dict(base.options or {}))

    def pair(self, i, j):
        got = self._pairs.get((i, j))
        if got is None:
            from .engines import get_engine
            got = self._pairs[(i, j)] = _pair(get_engine(self.pair_hop(i,
                                                                       j)))
        return got

    def reader(self, i):
        return self.pair(i, 0).src_engine

    def writer(self, i, j):
        return self.pair(i, j).dst_engine

    def report_dir(self, db):
        d = self.lead.report_dir(db) / "reshard"
        d.mkdir(parents=True, exist_ok=True)
        return d


def _pair(eng):
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


def _closure(hop, have):
    """(every hop the group holds, by name; the reasons it does not)."""
    out, wrong, todo = {hop.name: hop}, [], [hop]
    while todo:
        h = todo.pop()
        try:
            mine = settings(h)
        except SystemExit as e:
            wrong.append(f"{h.name}: {e}")
            continue
        for name in mine["with"]:
            if name in out:
                continue
            other = have.get(name)
            if other is None:
                wrong.append(f"with: {name} is not a hop the configuration"
                             " has")
                continue
            if (getattr(other, "goal", "") or "") != GOAL:
                wrong.append(f"with: {name}'s goal is not reshard")
                continue
            out[name] = other
            todo.append(other)
    return [out[n] for n in sorted(out)], wrong


def group(hop, siblings=None):
    """The `Group` this hop is one of, or SystemExit naming every reason
    its hops do not hold together."""
    from . import config
    conf = settings(hop)
    wrong = []
    have = {}
    if conf["with"]:
        try:
            have = siblings if siblings is not None else config.load_hops()
        except SystemExit as e:
            wrong.append(f"the hops it names could not be read: {e}")
    hops, more = _closure(hop, have)
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
            sources.append(Side(h.name, h, "src"))
        if _target_of(h) not in seen_t:
            seen_t.add(_target_of(h))
            targets.append(Side(h.name, h, "dst"))
    for s in sources:
        for t in targets:
            if _where(s.hop.source) == _where(t.hop.target) and not (
                    t.hop.db_map or {}):
                wrong.append(f"{t.name} writes the server {s.name} reads,"
                             " into the same databases: a reshard writes"
                             " new shards, never its own sources")
    g = Group(hops[0] if hops else hop, hops, sources, targets, conf)
    named = {h.name for h in hops}
    for h in list((conf["ranges"] or {})) + list(
            (conf["values"] or {})) + list((conf["lookup"] or {}).values()) \
            + ([conf["default"]] if conf["default"] else []):
        if h not in named:
            wrong.append(f"{h} is named where rows go, and is not a hop of"
                         " the group")
    if conf["by"] == "target" and len(targets) > 1:
        wrong.append(f"by: target places rows by the one target cluster's"
                     f" own distribution, and the group writes"
                     f" {len(targets)} targets")
    if wrong:
        raise SystemExit(f"{hop.name}: the reshard does not hold together -"
                         + " " + "; ".join(dict.fromkeys(wrong)))
    return g


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
    """How one table's rows are placed: `place(texts)` the target index of
    a row whose shard key renders to `texts`, `ALL` for a table every
    target takes whole, None for a row the function places nowhere."""
    way: str
    key: list
    said: str
    n: int = 1
    classes: dict = field(default_factory=dict)
    #: range: [(upper, index)] in order, the open one last (upper None)
    bounds: list = field(default_factory=list)
    #: list and lookup: {value text: index}, and the index of `default`
    table: dict = field(default_factory=dict)
    default: object = None
    #: target: what the one target cluster says of how it spreads the
    #: table (`Spread`)
    spread: object = None

    def place(self, texts):
        if self.way == "whole":
            return ALL
        if self.way in ("hash", "owned"):
            if self.way == "owned" or self.n == 1:
                return 0
            return hash_place(texts, self.n)
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
    every target takes it whole, the target cluster's own distribution,
    the hop's own function, a hash of the key."""
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


def _leaf(table):
    return str(table).split(".")[-1]


def _named_in(names, table):
    return str(table) in names or _leaf(table) in names


def shard_key(g, db, table, pk):
    """The columns a table's rows are placed by: the hop's `key` for it,
    else its own key."""
    for name in (str(table), _leaf(table)):
        if name in g.conf["key"]:
            return list(g.conf["key"][name])
    return list(pk or [])


def _classes(eng, side, db, table):
    from . import canon
    return {n: canon.type_class(eng.CANON_ENGINE, t)
            for n, t in eng.neutral_columns(side, db, table)}


def _spread(g, db, table):
    """What the one target says of how it spreads `table` (`Spread`), or
    None - asked of its own engine; a target that cannot say spreads
    nothing migkit knows of."""
    if len(g.targets) != 1:
        return None
    pair = g.pair(0, 0)
    ask = getattr(pair.dst_engine, "distribution", None)
    if ask is None:
        return None
    there = _target_table(pair, db, table)
    try:
        return ask("dst", db, there)
    except Exception:  # noqa: BLE001 - not known: spread by no one
        return None


def _target_table(pair, db, table):
    """The target's name for a source table, as the copy names it."""
    dst = pair.dst_engine
    there = [] if dst.target_missing(db) else dst.neutral_tables("dst", db)
    got, _, _, _ = pair.match_tables([table], there, pair._rename)
    return got[0][1] if got else pair._leaf(pair._rename(table))


def function(g, db, table, record=False):
    """(the `Function` placing `table`'s rows, the `decide.Choice` it was
    taken by); the Function None where no way holds - the choice says
    why."""
    from . import decide
    reader = g.reader(0)
    pk = list(reader.neutral_key("src", db, table) or [])
    spread = {}

    def spread_of():
        if "got" not in spread:
            spread["got"] = _spread(g, db, table)
        return spread["got"]
    facts = decide.Facts(
        {"shard by": g.conf["by"], "shard targets": len(g.targets),
         "shared": _named_in(g.conf["shared"], table)},
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
    if way == "owned":
        sp = spread_of()
        key = list(sp.columns)
        return Function("owned", key, f"{sp.said}", 1, classes,
                        spread=sp), choice
    key = shard_key(g, db, table, pk)
    out = Function(way, key, choice.rung.said, len(g.targets), classes)
    if way == "whole":
        out.said = choice.rung.said
        return out, choice
    if way == "range":
        bounds = []
        for name, upper in g.conf["ranges"].items():
            j = g.index(name)
            bounds.append((None if upper is None else _ordered(
                classes.get(key[0]) if key else None,
                _text(classes.get(key[0]) if key else None, upper)), j))
        bounds.sort(key=lambda b: (b[0] is None, b[0] or (0, 0)))
        out.bounds = bounds
    elif way == "list":
        cls = classes.get(key[0]) if key else None
        table_ = {}
        if g.conf["values"] is not None:
            for name, listed in g.conf["values"].items():
                for v in listed:
                    table_[str(_text(cls, v))] = g.index(name)
        for v, name in (g.conf["lookup"] or {}).items():
            table_[str(_text(cls, v))] = g.index(name)
        out.table = table_
        out.default = (g.index(g.conf["default"]) if g.conf["default"]
                       else None)
    return out, choice


# --- the tables and their rows -------------------------------------------------------

def _tables(g, db):
    """{source table: the index of the first source holding it} of every
    table the reshard carries - the sources hold one schema, and a table
    one of them lacks is read where it is."""
    out = {}
    for i in range(len(g.sources)):
        for t in g.reader(i).neutral_tables("src", db):
            if g.lead.excluded(db, *str(t).split(".")):
                continue
            out.setdefault(t, i)
    return out


def _where_of(g, db, table):
    return g.lead.row_filter(db, *str(table).split("."))


def _sources_of(g, fn, home):
    """The sources a table's rows are read from: every one, but for a
    table every target takes whole the first that holds it - the others
    keep copies of the same table, not rows of their own."""
    if fn.way == "whole":
        return [home]
    return list(range(len(g.sources)))


def _texts(fn, values):
    """The shard key's texts of a row given as {column: value}, or None
    where the row does not carry every column of it."""
    if not all(k in values for k in fn.key):
        return None
    return tuple(_text(fn.classes.get(k), values[k]) for k in fn.key)


def _keys(ks):
    return ", ".join("(" + ", ".join(map(str, k)) + ")" for k in ks)


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


def placement(g, db):
    """[Placed] of every table: each source's rows - their key and their
    shard key - placed by the function, read only; and what stops the
    reshard before a row is written: a table no way places, no key to
    place by, a shard key the table does not have, a row the function
    places nowhere, a key two sources hold."""
    out = []
    for t, home in sorted(_tables(g, db).items()):
        fn, choice = function(g, db, t)
        if fn is None:
            out.append(Placed(t, "", wrong=[f"{t}: no way places its rows -"
                                            f" {choice.reason}"]))
            continue
        p = Placed(t, fn.said, per_target=[0] * len(g.targets))
        reader = g.reader(home)
        pk = list(reader.neutral_key("src", db, t) or [])
        cols = dict(reader.neutral_columns("src", db, t))
        lacks = [k for k in fn.key if k not in cols]
        if lacks:
            p.wrong.append(f"{t}: the shard key {', '.join(lacks)} is not a"
                           " column of it - reshard: {key: {table:"
                           " column}}")
            out.append(p)
            continue
        if not pk and fn.way != "whole":
            p.wrong.append(f"{t}: no key, so a row cannot be placed once and"
                           " proved there - give it one, or reshard: {shared:"
                           f" [{t}]}} where every target is to hold it whole")
            out.append(p)
            continue
        read = list(dict.fromkeys(pk + list(fn.key)))
        seen = {}
        for i in _sources_of(g, fn, home):
            r = g.reader(i)
            if t not in r.neutral_tables("src", db):
                continue
            cls = _classes(r, "src", db, t)
            for rows_ in r._every_row("src", db, t,
                                      [(c, cls.get(c)) for c in read],
                                      _where_of(g, db, t)):
                for row in rows_:
                    values = dict(zip(read, row))
                    p.rows += 1
                    key = tuple(_text(cls.get(c), values[c]) for c in pk)
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


def _ledger_path(g, db):
    return g.report_dir(db) / "copy.json"


def _ledger(g, db):
    try:
        return json.loads(_ledger_path(g, db).read_text())
    except (OSError, ValueError):
        return {}


def _save_ledger(g, db, got):
    path = _ledger_path(g, db)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(got, indent=1, default=str))
    tmp.replace(path)


def problems(hop, eng, db):
    """[what stops this reshard before a row is written]: a group that does
    not hold together, and every table's rows placed on the sources
    (`placement`). Once the group's copy is done and its changes are
    followed, the rows are not counted again: the follow places each
    change, and the cutover proves every row."""
    try:
        g = group(hop)
    except SystemExit as e:
        return [str(e)]
    if _ledger(g, db).get("done"):
        return []
    try:
        placed = placement(g, db)
    except SystemExit as e:
        return [str(e)]
    except Exception as e:  # noqa: BLE001 - asked of the sources, said
        return [f"{db}: where each row goes could not be counted on the"
                f" sources: {_last(e)}"]
    return [w for p in placed for w in p.wrong]


def rows(hop, eng, db):
    """assess rows of the reshard: where every table's rows go."""
    try:
        g = group(hop)
        placed = placement(g, db)
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
        g = group(hop)
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


def _write(g, db, i, j, table, cols, rows_, log):
    """A batch of rows written to target `j`, by the target's bulk write or
    its plainest (`consolidate.write_ways`)."""
    from . import consolidate, decide
    dst = g.writer(i, j)

    def run(choice, rows_=rows_):
        write = (dst.neutral_write if choice.rung.name == "bulk"
                 else dst.neutral_rewrite)
        for a in range(0, len(rows_), consolidate.BATCH):
            write("dst", db, table, cols, rows_[a:a + consolidate.BATCH])
        return len(rows_)
    return decide.attempt(f"{table} on {g.targets[j].name}",
                          consolidate.write_ways(), run, log=log)[1]


def _node_pair(g, i, k, spread):
    """The pair writing source `i`'s rows to node part `k` of the one
    target's cluster, or None where the cluster names no address for it."""
    at = spread.address(k)
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
    return got


def _write_spread(g, db, i, table, fn, src_cols, dst_cols, rows_, log,
                  where):
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
    src_pk = list(g.reader(i).neutral_key("src", db, table) or [])
    pk = [dst_names[names.index(k)] for k in src_pk if k in names]
    n = 0
    for k, part in groups.items():
        node = _node_pair(g, i, k, fn.spread) if k is not None else None

        def run(choice, part=part, k=k, node=node):
            if choice.rung.name == "router":
                return _write(g, db, i, 0, _target_table(g.pair(i, 0), db,
                                                         table),
                              dst_cols, part, log)
            if not pk or len(pk) != len(src_pk):
                raise decide.Blocked("the table has no key to write its"
                                     " rows to a node by")
            # by key, as a change is applied: a node's own part of a table
            # is not one its catalogue lists to every client, so the key
            # comes with the rows
            recs = [{"op": "insert", "table": fn.spread.parts[k].table,
                     "key": {c: v for c, v in zip(dst_names, r)
                             if c in pk},
                     "values": dict(zip(dst_names, r))} for r in part]
            node.dst_engine.neutral_apply("dst", db, recs)
            return len(part)
        part_name = (fn.spread.parts[k].node if k is not None
                     else "a node the cluster did not name")
        facts = decide.Facts({"node address": node is not None})
        n += decide.attempt(f"{table}: a batch for {part_name}",
                            node_ways(), run, facts, where=where,
                            log=log)[1]
    return n


def _columns(g, db, i, table):
    """(the columns source `i` is read by, [(target table, its columns,
    notes)] of every target): the carried columns are the same on every
    target - the group's targets hold one schema - or the reshard stops
    here."""
    out, read = [], None
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


def _carry(g, db, i, table, fn, log, where):
    """One table of source `i`: read through the hop's row filter, every
    row written to the target its key places it on, a batch a target."""
    read, targets = _columns(g, db, i, table)
    for n_ in targets[0][2]:
        log(f"{table}: {n_}")
    names = [n for n, _ in read]
    lost = [k for k in fn.key if k not in names]
    if lost:
        raise SystemExit(f"{table}: the shard key {', '.join(lost)} is not"
                         " carried, so a row cannot be placed by it."
                         " Nothing more was written")
    at = [names.index(k) for k in fn.key]
    reader = g.reader(i)
    n = 0
    for rows_ in reader._every_row("src", db, table, read,
                                   _where_of(g, db, table)):
        if fn.way == "owned":
            n += _write_spread(g, db, i, table, fn, read, targets[0][1],
                               rows_, log, where)
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
            for t in (range(len(g.targets)) if j == ALL else (j,)):
                groups.setdefault(t, []).append(r)
        for j, part in groups.items():
            dst_t, dc, _ = targets[j]
            n += _write(g, db, i, j, dst_t, dc, part, log)
    return n


# --- the copy ----------------------------------------------------------------------

def _arm(g, pair):
    """`pair`'s writes held to the claim the lead holds now, where one is
    armed (`fencing`): a token of its own a target, since each target's
    row is raised the first time that target is written."""
    from . import fencing
    tok = fencing.armed(g.lead)
    if tok is not None:
        fencing.arm(pair.hop, fencing.token_of(tok.plain()))


def _disarm(g):
    from . import fencing
    for pair in g._pairs.values():
        fencing.disarm(pair.hop)


def _unit(g, db, key, run, log):
    """`run()` done once across the machines of the hop. Where they share
    its tables (`share_tables`), through a claim of the unit whole
    (`claims.whole`), each target's writes held to the claim's number - a
    machine that lost the claim is refused by every target it writes - and
    waited for while another machine holds it; on one machine, run here."""
    from . import shared
    if not shared.together(g.lead):
        run()
        return
    from . import claims, ha
    store = shared.store_for(g.lead, log)
    claims.way(g.lead, db, key, store, lambda: False)
    table = claims.Table(store, claims.path(g.lead, db, key), ha.mover(),
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
        if claims.whole(g.lead, db, key, store, fenced, log):
            return
        if not said:
            log(f"{key}: another machine of the hop holds it; waiting for it"
                " to be done, or for its claim to lapse")
            said = True
        time.sleep(1)


def _prepare(g, db, tables, log):
    """Every target's tables made where it lacks them, its rows of another
    run's or another writer's held to the hop's `target_rows`, and the
    tables in scope emptied - within the hop's row filter - so each holds
    the group's rows and nothing else."""
    from . import interlock
    gone = 0
    for j in range(len(g.targets)):
        for i in sorted(set(tables.values())):
            g.pair(i, j).create_missing(db, log)
        pair = g.pair(0, j)
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


def _finish(g, db, log):
    """Once every unit is in: the indexes of the tables the copy made, each
    target's counters past its rows, and the proof - a target that is not
    what the function places there stops it."""
    for (i, j), pair in sorted((k, p) for k, p in g._pairs.items()
                               if len(k) == 2 and isinstance(k[1], int)):
        pair.finish_created(db, log)
    for j in range(len(g.targets)):
        said = g.writer(0, j).settle_target(db, from_source=False)
        if said:
            log(f"{g.targets[j].name}: {said}")
    results = prove(g, db)
    for r in results:
        log(f"{r.scope}: {r.status.upper()} {r.detail}")
    bad = [r for r in results if r.status not in ("ok", "skip")]
    if bad:
        raise SystemExit(f"{db}: the reshard is not what its function"
                         " places - " + "; ".join(f"{r.scope}: {r.detail}"
                                                  for r in bad[:4]))
    return len(results)


def copy(g, db, go, log):
    """Every source's rows onto the targets their keys place them on: each
    table's way climbed and recorded, the targets prepared once, each
    (source, table) carried as a unit - kept in the ledger, so a run that
    stopped goes on after the units it finished - and the whole proved.
    A plan without `go`."""
    import contextlib
    tables = _tables(g, db)
    fns = {}
    for t in sorted(tables):
        fn, choice = function(g, db, t, record=go)
        if fn is None:
            raise SystemExit(f"{db}.{t}: nothing was written - no way places"
                             f" its rows: {choice.reason}")
        fn.home = tables[t]
        fns[t] = fn
        log(f"{t}: {fn.said} - {choice.reason}")
    if not go:
        log(f"[{db}] dry-run: {len(fns)} tables of {len(g.sources)} sources"
            f" onto {len(g.targets)} targets - each target's tables emptied,"
            " every row written to the target its key places it on, and"
            " every target proved")
        return
    ledger = _ledger(g, db)
    where = _ledger_path(g, db)
    if not ledger.get("prepared"):
        _unit(g, db, "reshard-prepare",
              lambda: _prepare(g, db, tables, log), log)
        ledger["prepared"] = True
        _save_ledger(g, db, ledger)
    units = ledger.setdefault("units", {})
    held = {}
    with contextlib.ExitStack() as windows:
        for i in range(len(g.sources)):
            for j in range(len(g.targets)):
                windows.enter_context(g.pair(i, j).load_window(db, log))
        for t in sorted(fns):
            for i in _sources_of(g, fns[t], tables[t]):
                if i not in held:
                    held[i] = set(g.reader(i).neutral_tables("src", db))
                if t not in held[i]:
                    continue
                name = f"{i + 1}/{t}"
                if name in units:
                    log(f"{t} of {g.sources[i].name}: carried by an earlier"
                        " run")
                    continue
                got = {}

                def run(i=i, t=t, got=got):
                    got["rows"] = _carry(g, db, i, t, fns[t], log, where)
                    return got["rows"]
                _unit(g, db, f"reshard-{i + 1}-{t}", run, log)
                units[name] = {"rows": got.get("rows"),
                               "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
                _save_ledger(g, db, ledger)
                if "rows" in got:
                    log(f"{t} of {g.sources[i].name}: {got['rows']:,} rows"
                        " placed")
    _unit(g, db, "reshard-finish", lambda: _finish(g, db, log), log)
    ledger["done"] = True
    _save_ledger(g, db, ledger)
    log(f"[{db}] {len(g.sources)} sources placed onto {len(g.targets)}"
        " targets and proved")


def _token_path(g, db, i):
    return g.report_dir(db) / f"source-{i + 1}" / "position.json"


def _mark_path(g, db, i, j):
    return g.report_dir(db) / f"source-{i + 1}" / f"target-{j + 1}.json"


def begin(g, db, go, log):
    """Each source's change position, taken before the copy reads it
    (`copy_point`) - so nothing changed while the rows are read is lost -
    or kept where an earlier run took one: older, it only replays changes
    the targets take by key."""
    from . import tailctl
    for i in range(len(g.sources)):
        path = _token_path(g, db, i)
        if tailctl.saved(path) is not None:
            continue
        if not go:
            log(f"{g.sources[i].name}: its change position is taken with"
                " --go, before the copy reads a row")
            continue
        g.pair(i, 0)._can_tail(db)
        reader = g.reader(i)
        point = reader.copy_point("src", db)
        tailctl.begin(reader, db, path, point)
        log(f"{g.sources[i].name}: changes followed from"
            f" {str(point)[:60]}, taken before the copy")


def move(hop, eng, dbs, mode, table, go, log):
    """`move` of a reshard hop: the copy (`--mode full`), the follow
    (`--mode cdc`), or both with the positions taken first (`full+cdc`) -
    for the whole group, whichever of its hops is run."""
    if table:
        raise SystemExit(f"{hop.name}: a reshard is carried whole - every"
                         " row is placed and proved across every target."
                         " Leave out --table")
    g = group(hop)
    if mode in ("cdc", "full+cdc") and len(dbs) > 1:
        raise SystemExit(f"{hop.name}: the follow of a reshard runs one"
                         " database a process - --db one of"
                         f" {', '.join(dbs)}")
    for d in dbs:
        log(f"[{d}] {len(g.sources)} sources"
            f" ({', '.join(s.name for s in g.sources)}) onto"
            f" {len(g.targets)} targets"
            f" ({', '.join(t.name for t in g.targets)})")
        if mode in ("full", "full+cdc"):
            if mode == "full+cdc":
                begin(g, d, go, log)
            copy(g, d, go, log)
        if mode in ("cdc", "full+cdc"):
            follow(g, d, go, log)


# --- the follow ----------------------------------------------------------------------

def _ident(table, key):
    return (str(table), tuple(sorted((str(k), repr(v))
                                     for k, v in key.items())))


def _before(fn, c, n):
    """The targets a row may be on before its change: where its old shard
    key places it - read from the change's key, or from the image of the
    row the log keeps - every target of a table they all hold, and None
    where the log keeps neither: then every target but the one it goes
    to has it taken off by key."""
    if fn.way == "whole":
        return set(range(n))
    old = {**(c.get("before") or {}), **(c.get("key") or {})}
    texts = _texts(fn, old)
    if texts is None:
        return None
    j = fn.place(texts)
    if j is None:
        return set()
    return set(range(n)) if j == ALL else {j}


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
    column; `moved`, the rows whose key moved them across targets."""
    out: dict
    into: dict
    asked: list = field(default_factory=list)
    partial: list = field(default_factory=list)
    moved: int = 0


def plan_batch(changes, fns, n, held=None, filled=None):
    """One source's batch of row changes (in its log's order) as `Planned`.

    Each row is followed through the batch to its last state: where it was
    before the batch (`_before`, taken from its first change) and where
    its last state places it. A row whose key moved it is removed from
    every target it may have been on but the one it goes to - before any
    target is written - and written there; a row removed is removed where
    it was. Rows go in the order the source first touched them, the order
    an engine applies a batch it collapses by key.

    `held`, {row: its values} of rows another source holds now: the row
    is that source's - written as it has it, and taken off every other
    target. `filled`, {row: its values now, or None}, completes a row
    written where it was not from what the source holds now - a row gone
    since is left to its own delete, which follows in the log."""
    held = held or {}
    filled = filled or {}
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
                st = touch(t, key, fn, "update", _before(fn, c, n))
                st["final"], st["values"] = "del", None
                written(touch(t, new, fn, "insert", set()), new, values)
            else:
                written(touch(t, key, fn, "update", _before(fn, c, n)), key,
                        values)
        elif op == "delete":
            st = touch(t, key, fn, "delete", _before(fn, c, n))
            st["final"], st["values"] = "del", None
    asked, partial = [], []
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
                    st["final"] = "gone"
                else:
                    st["values"] = {**filled[k], **st["values"]}
            else:
                partial.append((st["table"], st["key"]))
    everywhere = set(range(n))
    out = {j: [] for j in range(n)}
    into = {j: [] for j in range(n)}
    moved = 0
    for k in order:
        st = state[k]
        fn, t, key = st["fn"], st["table"], st["key"]
        before = everywhere if st["before"] is None else st["before"]
        if st["final"] == "up":
            j = fn.place(_texts(fn, st["values"]))
            if j is None:
                raise Unplaced(
                    f"{t}: a change places its row on no target ({fn.said},"
                    f" by key {_keys([tuple(key.values())])}) - the follow"
                    " stops before it, its position kept: name a target for"
                    " the value (reshard: default) and run it again")
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
    return Planned(out, into, asked, partial, moved)


def _by_key(g, db, k, fn, table, keys):
    """{row: its values} of the rows source `k` holds now of `keys`, read
    by key through the hop's row filter."""
    reader = g.reader(k)
    if table not in reader.neutral_tables("src", db):
        return {}
    cls = _classes(reader, "src", db, table)
    cols = [(c, cls.get(c)) for c in fn.columns if c in cls]
    got = reader.neutral_rows_by_key(
        "src", db, table, cols, fn.pk,
        [tuple(key[p] for p in fn.pk) for key in keys],
        where=_where_of(g, db, table))
    out = {}
    for key in keys:
        canon_key = tuple(_text(cls.get(p), key[p]) for p in fn.pk)
        row = got.get(canon_key)
        if row is not None:
            out[_ident(table, key)] = dict(zip([c for c, _ in cols], row))
    return out


def _held_elsewhere(g, db, i, asked, fns):
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
            for ident, values in _by_key(g, db, k, fns[t], t,
                                         keys).items():
                out.setdefault(ident, values)
    return out


def _now(g, db, i, partial, fns):
    """{row: its values now, or None where source `i` no longer has it}."""
    by_table = {}
    for t, key in partial:
        by_table.setdefault(t, []).append(key)
    out = {}
    for t, keys in by_table.items():
        got = _by_key(g, db, i, fns[t], t, keys)
        for key in keys:
            ident = _ident(t, key)
            out[ident] = got.get(ident)
    return out


def _same(a, b):
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(
        b, sort_keys=True, default=str)


def _apply(g, db, i, changes, fn_of, token, run, log):
    """One batch of source `i`, planned whole (`plan_batch`) and applied:
    every target's removals of rows a key moved off it first, then each
    target's changes in one transaction under the batch's number where the
    target keeps one (`marks`) - a target that committed this very batch
    before a stop is not written again. (changes applied, rows moved,
    whether a numbered batch was written)."""
    from . import canon, drift, failpoint, fencing
    rows_ = []
    for c in changes:
        fn = fn_of(c["table"])
        if canon.is_ddl(c):
            if fn is None:
                continue
            raise SystemExit(
                f"{db}.{c['table']}: {c.get('kind')} on"
                f" {g.sources[i].name} - the reshard's follow stops at a"
                " change of a table's shape or of its whole contents, its"
                " position kept before it: make the change on every target,"
                " then move again (--mode full+cdc)")
        if fn is None or drift.transient(c["table"]):
            continue
        if fn.way == "whole" and fn.home != i:
            # a table every target takes whole is carried from the first
            # source that holds it; the others hold copies of it
            continue
        rows_.append({k: v for k, v in c.items()
                      if k not in canon.FROM_THE_LOG})
    if not rows_:
        return 0, 0, False
    fns = {c["table"]: fn_of(c["table"]) for c in rows_}
    n = len(g.targets)
    plan = plan_batch(rows_, fns, n)
    held = (_held_elsewhere(g, db, i, plan.asked, fns)
            if len(g.sources) > 1 and plan.asked else {})
    filled = _now(g, db, i, plan.partial, fns) if plan.partial else {}
    if held or filled:
        plan = plan_batch(rows_, fns, n, held, filled)
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
                 **fencing.marked(g.pair(i, j).hop)}, default=str)
            numbered = True
        try:
            g.pair(i, j).carry(db, plan.into[j])
        finally:
            w.__dict__.pop("_batch_seen", None)
        failpoint.hit("reshard.applied")
    return len(rows_), plan.moved, numbered


def _save_position(path, token, batch):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"token": token, "batch": batch},
                              default=str))
    tmp.replace(path)


def follow(g, db, go, log):
    """Every source's changes onto the targets, by the same function, until
    stopped - ctrl-c, `tail.stop`, the cutover - or caught up where the
    hop's `follow_until` says so. One loop for the group: a batch a source
    in turn, each planned whole and applied (`_apply`), its position saved
    after every target has it. A connection lost is read again from the
    saved positions, which the targets take by key."""
    import contextlib
    import secrets

    from . import marks, tailctl
    from .engines.hetero import HeteroEngine
    from .util import is_transient
    where = g.report_dir(db)
    if go and tailctl.alive(where):
        raise SystemExit(f"{db}: the reshard's follow already runs here -"
                         " one follow a group")
    tables = _tables(g, db)
    fns = {}
    for t, home in tables.items():
        fn, choice = function(g, db, t)
        if fn is None:
            raise SystemExit(f"{db}.{t}: no way places its rows -"
                             f" {choice.reason}")
        fn.home = home
        fns[t] = fn
    leaves = {}
    for t in fns:
        leaves.setdefault(_leaf(t), []).append(t)

    def fn_of(table):
        got = fns.get(table)
        if got is None and len(leaves.get(_leaf(table), [])) == 1:
            got = fns[leaves[_leaf(table)][0]]
        return got
    n_src, n_dst = len(g.sources), len(g.targets)
    paths = [_token_path(g, db, i) for i in range(n_src)]
    tokens = []
    for i in range(n_src):
        g.pair(i, 0)._can_tail(db)
        reader = g.reader(i)
        tok = tailctl.saved(paths[i])
        if tok is None and go:
            tailctl.begin(reader, db, paths[i],
                          reader.copy_point("src", db))
            tok = tailctl.saved(paths[i])
            log(f"{g.sources[i].name}: no position was kept - followed from"
                " now, and what changed before it is the copy's")
        elif tok is not None:
            tailctl.same_source(reader, db, paths[i], tok)
        tokens.append(tok)
    if not go:
        log(f"[{db}] dry-run: the changes of {n_src} sources followed onto"
            f" {n_dst} targets by the function of each table; a key that"
            " moves its row removed from the target it left before it is"
            " written on the one it reaches")
        return
    batch = []
    for i in range(n_src):
        got = marks.saved(paths[i]).get("batch")
        if got is None:
            # a number no earlier run's marks can hold, written before
            # anything is applied (`HeteroEngine._saved_batch`)
            got = secrets.randbits(48)
            _save_position(paths[i], tokens[i], got)
        batch.append(int(got))
    numbered, ahead = {}, {}
    for i in range(n_src):
        for j in range(n_dst):
            numbered[(i, j)] = marks.settle_exact(
                g.pair(i, j), db, _mark_path(g, db, i, j), log) is not None
            if not numbered[(i, j)]:
                continue
            try:
                got = g.writer(i, j).origin_seen("dst", db)
            except Exception:  # noqa: BLE001 - not known: written again
                got = None
            if got and int(got.get("batch", -1)) == batch[i] + 1:
                ahead[(i, j)] = got.get("token")
    run = {"batch": batch, "numbered": numbered, "ahead": ahead}
    saved, read_at = list(tokens), list(tokens)
    carried = [[] for _ in range(n_src)]
    limit = [BATCH] * n_src
    cut_ok = [bool(getattr(g.reader(i), "READ_ENDS_INSIDE_TRANSACTIONS",
                           False)) for i in range(n_src)]
    until = tailctl.until(g.lead)
    fence = [None] * n_src
    seen = moved = lost = 0
    caught_up = time.time()
    log(f"following {n_src} sources onto {n_dst} targets"
        + (", until caught up" if until == "caught_up"
           else ", ctrl-c to stop"))
    try:
        with contextlib.ExitStack() as stack:
            stack.enter_context(tailctl.Running(where))
            for i in range(n_src):
                for j in range(n_dst):
                    stack.enter_context(g.pair(i, j).load_window(db, log))
            while True:
                tailctl.hold_if_asked(where, log)
                if tailctl.stop_asked(where):
                    log(f"stopped as asked after {seen:,} changes; every"
                        " source's position is saved")
                    break
                try:
                    went, short = 0, []
                    for i in range(n_src):
                        reader = g.reader(i)
                        got, read_to = reader.neutral_changes(
                            "src", db, read_at[i], limit=limit[i])
                        fewer = len(got) < limit[i]
                        limit[i] = (BATCH if fewer
                                    else min(limit[i] * 2, BATCH_MOST))
                        changes, carried[i] = carried[i] + got, []
                        read_at[i] = token = read_to
                        cut = (HeteroEngine._commit_cut(changes)
                               if cut_ok[i] else None)
                        if cut is not None:
                            changes, carried[i] = changes[:cut], \
                                changes[cut:]
                            token = changes[-1]["txn_at"]
                        applied, m, marked = _apply(g, db, i, changes,
                                                    fn_of, token, run, log)
                        for key in [k for k in ahead if k[0] == i]:
                            ahead.pop(key)
                        if applied or not _same(token, saved[i]):
                            batch[i] += 1 if marked else 0
                            _save_position(paths[i], token, batch[i])
                            saved[i] = token
                        went += applied
                        moved += m
                        short.append(fewer and not carried[i])
                    seen += went
                    if went:
                        log(f"{seen:,} changes"
                            + (f", {moved:,} rows moved across targets"
                               if moved else ""))
                    if all(short):
                        caught_up = time.time()
                    tailctl.beat(where, caught_up, seen)
                    if until == "caught_up" and all(short):
                        for i in range(n_src):
                            if fence[i] is None:
                                fence[i] = g.reader(i).log_position("src",
                                                                    db)
                        reached = [g.reader(i).position_reached(
                            saved[i], fence[i]) if fence[i] is not None
                            else None for i in range(n_src)]
                        if all(r or (r is None and short[x])
                               for x, r in enumerate(reached)):
                            log(f"caught up with every source, after"
                                f" {seen:,} changes - ended, as"
                                " follow_until says; the next run goes on"
                                " from there")
                            break
                    if not went:
                        time.sleep(1)
                except Exception as e:  # noqa: BLE001 - classified here
                    if not is_transient(e):
                        raise
                    read_at = list(saved)
                    carried = [[] for _ in range(n_src)]
                    lost += 1
                    wait = min(60, 2 ** min(lost, 6))
                    log(f"lost a connection ({_last(e)[:100]}); reading"
                        f" again in {wait}s from every source's saved"
                        " position")
                    tailctl.beat(where, caught_up, seen)
                    time.sleep(wait)
                    continue
                lost = 0
    except KeyboardInterrupt:
        log(f"stopped after {seen:,} changes; rerun to resume")
    finally:
        for i in range(n_src):
            try:
                g.reader(i).release_changes()
            except Exception:  # noqa: BLE001 - let go on the way out
                pass


# --- the proof ---------------------------------------------------------------------

def _shown(keys):
    return _keys(sorted(keys, key=repr)[:EXAMPLES])


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


def _prove_table(g, db, t, home, fn):
    """[Result] of one table: a line a target and one across them."""
    from collections import Counter

    from . import consolidate
    from .engines.base import Result
    out, cols = [], []
    for j in range(len(g.targets)):
        pair = g.pair(home, j)
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
    pk = list(g.reader(home).neutral_key("src", db, t) or [])
    at = [names.index(k) for k in pk if k in names]
    pair0 = cols[0][1]
    where = _where_of(g, db, t)
    if not pk or len(at) != len(pk):
        if fn.way != "whole":
            return [Result("reshard", f"{db}.{t}", "error",
                           "no key compared to place its rows by")]
        # a table every target holds whole, without a key: as a multiset
        # of rows, so a row there twice on one side is not one row
        a = Counter()
        for rows_ in g.reader(home)._every_row("src", db, t, sc, where):
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
    for i in _sources_of(g, fn, home):
        reader = g.reader(i)
        if t not in reader.neutral_tables("src", db):
            continue
        for rows_ in reader._every_row("src", db, t, sc, where):
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
        (f"{len(want):,} rows, on every one of the {len(g.targets)}"
         " targets" if fn.way == "whole" else
         f"{len(want):,} rows of {len(g.sources)} sources, each on exactly"
         f" one of the {len(g.targets)} targets - {fn.said}")))
    if fn.way == "owned":
        out += _prove_parts(g, db, t, home, fn, pk)
    return out


def _prove_parts(g, db, t, home, fn, pk):
    """[Result] of a table the target cluster spreads: each part read on
    its own node, where migkit reaches it, and every row there one the
    cluster's function places in that part - a row a direct write put in
    the wrong part is one the cluster's router would never find. A part
    no address names was only written through the router, which places
    each row itself."""
    from .engines.base import Result
    sp = fn.spread
    out, bad, read = [], [], 0
    for k, part in enumerate(sp.parts):
        node = _node_pair(g, home, k, sp)
        if node is None:
            continue
        cls = _classes(g.reader(home), "src", db, t)
        cols = [(c, cls.get(c)) for c in dict.fromkeys(pk + list(fn.key))]
        for rows_ in node.dst_engine._every_row("dst", db, part.table, cols):
            values = [dict(zip([c for c, _ in cols], r)) for r in rows_]
            owners = sp.owners([_texts(fn, v) for v in values])
            for v, got in zip(values, owners):
                read += 1
                if got != k:
                    bad.append(tuple(_text(cls.get(p), v[p]) for p in pk))
    if not read and not bad:
        return [Result("reshard", f"{db}.{t} parts", "skip",
                       "no part of it is on a node migkit reaches: every row"
                       " went through the cluster's router, which places"
                       " it")]
    if bad:
        out.append(Result("reshard", f"{db}.{t} parts", "diff",
                          f"{len(bad):,} rows sit in a part the cluster's"
                          f" own function does not place them in"
                          f" ({_shown(bad)})"))
    else:
        out.append(Result("reshard", f"{db}.{t} parts", "ok",
                          f"{read:,} rows read on their nodes, each in the"
                          " part the cluster places it in"))
    return out


def prove(g, db):
    """[Result] of every table: a line a target - each key there the one
    the function places there, as its source has it - and one across
    them: no key on two targets, no key two sources hold, every source
    row on a target."""
    from .engines.base import Result
    out = []
    for t, home in sorted(_tables(g, db).items()):
        fn, choice = function(g, db, t)
        if fn is None:
            out.append(Result("reshard", f"{db}.{t}", "error",
                              f"no way places its rows: {choice.reason}"))
            continue
        try:
            out += _prove_table(g, db, t, home, fn)
        except Exception as e:  # noqa: BLE001 - said as the table's error
            out.append(Result("reshard", f"{db}.{t}", "error",
                              f"could not be read: {_last(e)}"))
    return out


def check(hop, eng, db):
    """`check` of a reshard hop: the proof of the whole group."""
    from .engines.base import Result
    try:
        g = group(hop)
    except SystemExit as e:
        return [Result("reshard", db, "error", str(e))]
    return prove(g, db)


# --- the cutover -------------------------------------------------------------------

class _Sides:
    """One side of the group as the cutover's freeze sees it: every source
    (or target) with its own hop and engine, each held its own way
    (`freeze.GroupHold`)."""
    CANON_ENGINE = "group"

    def __init__(self, g, side):
        self.group, self.side = g, side

    @property
    def parts(self):
        """[(name, hop, engine)] of each source or target."""
        out = []
        if self.side == "src":
            for i, s in enumerate(self.group.sources):
                out.append((s.name, _side_hop(self.group, "src", i),
                            self.group.pair(i, 0)))
        else:
            for j, t in enumerate(self.group.targets):
                out.append((t.name, _side_hop(self.group, "dst", j),
                            self.group.pair(0, j)))
        return out


def _side_hop(g, side, k):
    """The hop a source or target of the group is frozen under: its pair's,
    so its records are its own and never the group's (`freeze._save`)."""
    return g.pair(k, 0).hop if side == "src" else g.pair(0, k).hop


class Cluster:
    """The group as the cutover's one engine (`cutover.run`): its sources
    frozen together (`freeze.GroupHold`), its final check the proof across
    every target, its counters every target's own."""
    REPLICATES_THE_SERVER = False

    def __init__(self, g):
        self.group, self.hop = g, g.lead
        self.src_engine = _Sides(g, "src")
        self.dst_engine = _Sides(g, "dst")

    def databases(self):
        return list(self.group.lead.databases or [])

    def maps_columns(self):
        return False

    def check_data(self, db, table=None, stream=None, with_counts=False):
        return prove(self.group, db)

    def repair_plan(self, db, kind):
        """Each target's counters past its own rows, as its own engine
        raises them; `by` names the target an action is for."""
        out = []
        for j in range(len(self.group.targets)):
            for a in self.group.pair(0, j).repair_plan(db, kind):
                if a.kind != "sequences":
                    continue
                a.by = str(j)
                a.note = f"{self.group.targets[j].name}: {a.note}"
                out.append(a)
        return out

    def apply(self, db, action):
        j = int(action.by or 0)
        return self.group.pair(0, j).apply(db, action)


class _Fence:
    """The follow's drain, as the cutover asks it: every source's position
    now, and the follow waited for until its saved position of each has
    passed it."""
    CANON_ENGINE = "group"

    def __init__(self, g):
        self.group = g

    def src_lsn(self, db):
        from . import tailctl
        if not tailctl.alive(self.group.report_dir(db)):
            return None
        out = {}
        for i, s in enumerate(self.group.sources):
            pos = self.group.reader(i).log_position("src", db)
            if pos is None:
                return None
            out[s.name] = pos
        return out

    def fence_wait(self, db, pos, timeout=300):
        from . import tailctl
        if not pos:
            return None
        began = time.monotonic()
        while time.monotonic() - began < timeout:
            reached = []
            for i, s in enumerate(self.group.sources):
                have = tailctl.saved(_token_path(self.group, db, i))
                got = (self.group.reader(i).position_reached(have,
                                                            pos[s.name])
                       if have is not None else False)
                if got is None:
                    return None
                reached.append(got)
            if all(reached):
                return True
            time.sleep(1)
        return False


def leg(g, dbs, say):
    """The follow as the cutover drains it, pauses it and ends it
    (`cutover.Leg`)."""
    from . import cutover, tailctl

    def down(d):
        if not tailctl.pause(g.report_dir(d), 60):
            raise SystemExit(f"{d}: the reshard's follow did not pause")
        return []

    def up(d):
        tailctl.resume(g.report_dir(d))

    def end(d):
        where = g.report_dir(d)
        if not tailctl.alive(where):
            return
        tailctl.stop(where)
        tailctl.resume(where)
        stop = time.monotonic() + 60
        while tailctl.alive(where) and time.monotonic() < stop:
            time.sleep(0.5)
        if tailctl.alive(where):
            raise SystemExit(f"{d}: the reshard's follow did not end")
        say(f"  {d}: the reshard's follow ended")
    alive = [d for d in dbs if tailctl.alive(g.report_dir(d))]
    if not alive:
        return cutover.Leg("none", why="no follow of the reshard runs: start"
                                       " it with --mode cdc")
    return cutover.Leg("tail", fence=_Fence(g), down=down, up=up, end=end)


def cut_over(hop, eng, dbs, go, say, approve):
    """The cutover of the whole group at once, on the cutover's own path
    (`cutover.run`): every source frozen, the follow drained past every
    source's position taken after the freeze, the proof across every
    target, each target's counters, the flip - undone on any failure."""
    from . import cutover
    g = group(hop)
    if (hop.options or {}).get("reverse") or (hop.options or {}).get(
            "two_way"):
        raise SystemExit(f"{hop.name}: a reshard has no stream back - its"
                         " targets are other shards than its sources")
    return cutover.run(g.lead, Cluster(g), dbs, go, leg(g, dbs, say), say,
                       approve)
