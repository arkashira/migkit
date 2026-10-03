"""Many databases or tenants into one (backlog P0-SCENARIOS): each
source's rows carried into one target under a column the hop names,
filled with the source's own value, its keys kept apart from every other
source's, and proved in the target source by source.

    hops:
      shop-eu:
        goal: consolidate
        source: postgres://app@10.0.0.1/shop
        target: postgres://app@10.0.0.9/shop
        options:
          consolidate:
            column: tenant_id        # the column each source's rows are held under
            value: eu                # this source's value (values: {db: value} for several)
            keys: tenant             # refuse (the default), tenant, offset or remap
            with: [shop-us]          # the other hops writing the same target
      shop-us:
        goal: consolidate
        source: postgres://app@10.0.0.2/shop
        target: postgres://app@10.0.0.9/shop
        options:
          consolidate: {column: tenant_id, value: us, keys: tenant, with: [shop-eu]}

**The sources** (`sources`) are this hop's databases and those of the
hops it names (`with`), each with its value: two sources of one value, a
hop named that is not one, or one writing another target is refused. One
hop may consolidate several of its own databases into one
(`db_map: {a: all, b: all}`, `values: {a: .., b: ..}`).

**Keys that collide are found before the move** (`collisions`): every
source's keys of every table read and put side by side, and a key two
sources hold is said by name with the sources that hold it. What happens
to them is the hop's rule, chosen per table as a way (`KEY_WAYS`):
`refuse` - the move stops on them; `tenant` - the target's key leads with
the tenant column, so a key is one source's; `offset` - each source's
integer keys shifted by its own offset (`offsets: {value: N}`), and every
column referring to them by the same; `remap` - the offsets worked out by
migkit, source by source, a stride apart that holds ten times the largest
key, kept (`consolidate-keys.json`) so a later run shifts the same way.

**Counters merged**: once the rows are in, every counter on the target is
raised past every source's keys (`settle_target`), and the proof asks it.

**Proved by tenant** (`prove`): every source's rows read and shifted as
they were written, the target's rows of its value read, both compared by
key; the target holding rows of a value no source has is said; each
table's counter past its rows.
"""
import json
import time
from dataclasses import dataclass, field

GOAL = "consolidate"
OPTION = "consolidate"
RULES = ("refuse", "tenant", "offset", "remap")
BATCH = 1000
EXAMPLES = 3
#: what `remap` leaves each source above its largest key, as a multiple:
#: its keys may grow tenfold before the stride is reached
ROOM = 10


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def settings(hop):
    """The hop's `consolidate:`, read, for a hop whose goal is
    consolidate; None for any other."""
    if (getattr(hop, "goal", "") or "") != GOAL:
        return None
    raw = (hop.options or {}).get("consolidate") or {}
    if not isinstance(raw, dict):
        raise SystemExit("consolidate: is a mapping - column, value or"
                         " values, keys, offsets, with")
    column = str(raw.get("column") or "").strip()
    rule = str(raw.get("keys") or "refuse").lower()
    if rule not in RULES:
        raise SystemExit(f"consolidate: keys: {rule} - one of"
                         f" {', '.join(RULES)}")
    offsets = {}
    for v, n in (raw.get("offsets") or {}).items():
        try:
            offsets[str(v)] = int(n)
        except (TypeError, ValueError):
            raise SystemExit(f"consolidate: offsets: {v}: {n!r} is not a"
                             " whole number")
    values = {str(k): v for k, v in (raw.get("values") or {}).items()}
    return {"column": column, "value": raw.get("value"), "values": values,
            "keys": rule, "offsets": offsets,
            "with": [str(h) for h in raw.get("with") or []]}


@dataclass(frozen=True)
class Source:
    """One source of the target: a hop, one of its databases, and its
    value in the tenant column."""
    hop: object
    db: str
    value: object

    @property
    def name(self):
        return f"{self.hop.name}/{self.db}"


def value_of(hop, db, got=None):
    """This source's value: `values[db]`, else `value`, else the database's
    own name."""
    got = got or settings(hop)
    if db in got["values"]:
        return got["values"][db]
    if got["value"] is not None:
        return got["value"]
    return db


def _where(hop):
    return (hop.target.host, hop.target.port)


def sources(hop, dbs, siblings=None):
    """([Source] of the group writing the target `hop` writes, this hop's
    first), or SystemExit naming every reason the group does not hold
    together."""
    from . import config
    got = settings(hop)
    wrong = []
    if not got["column"]:
        wrong.append("consolidate: column - the column each source's rows"
                     " are held under - is not said")
    out = [Source(hop, d, value_of(hop, d, got)) for d in dbs]
    if got["with"]:
        try:
            have = siblings if siblings is not None else config.load_hops()
        except SystemExit as e:
            have = {}
            wrong.append(f"the hops it names could not be read: {e}")
        for name in got["with"]:
            other = have.get(name)
            if other is None:
                wrong.append(f"with: {name} is not a hop the configuration"
                             " has")
                continue
            theirs = settings(other)
            if theirs is None:
                wrong.append(f"with: {name}'s goal is not consolidate")
                continue
            if theirs["column"] != got["column"] or theirs["keys"] != \
                    got["keys"]:
                wrong.append(f"with: {name} holds its rows under"
                             f" {theirs['column']!r} by keys:"
                             f" {theirs['keys']}, this hop under"
                             f" {got['column']!r} by keys: {got['keys']} -"
                             " say both alike")
            if _where(other) != _where(hop):
                wrong.append(f"with: {name} writes another server")
            for d in other.databases or []:
                out.append(Source(other, str(d), value_of(other, str(d),
                                                          theirs)))
    seen = {}
    for s in out:
        key = (s.hop.target_db(s.db), str(s.value))
        if key in seen:
            wrong.append(f"{seen[key]} and {s.name} both go into"
                         f" {key[0]} as {s.value!r}: each source needs a"
                         " value of its own")
        seen.setdefault(key, s.name)
    if wrong:
        raise SystemExit(f"{hop.name}: the consolidation does not hold"
                         " together - " + "; ".join(wrong))
    return out


def _pair(eng):
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


def _engine(s, eng):
    """The engine of a source: this hop's own, or a sibling's built."""
    if s.hop is getattr(eng, "hop", None):
        return eng
    from .engines import get_engine
    return get_engine(s.hop)


def group(hop, eng, db, siblings=None):
    """The `Source`s going into the same target database as `db` of this
    hop."""
    target = hop.target_db(db)
    return [s for s in sources(hop, list(hop.databases or [db]), siblings)
            if s.hop.target_db(s.db) == target]


# --- the keys -----------------------------------------------------------------------

def _classes(e, side, db, table):
    from . import canon
    return {n: canon.type_class(e.CANON_ENGINE, t)
            for n, t in e.neutral_columns(side, db, table)}


def _tables(hop, pair, db):
    src = pair.src_engine
    return [t for t in src.neutral_tables("src", db)
            if hop.takes_table(db, t)]


def _key_texts(s, eng, table):
    """{key text: None} of one source's table, through its row filter."""
    from . import canon
    pair = _pair(_engine(s, eng))
    src = pair.src_engine
    key = list(src.neutral_key("src", s.db, table) or [])
    if not key:
        return key, None
    cls = _classes(src, "src", s.db, table)
    read = [(k, cls.get(k)) for k in key]
    where = s.hop.row_filter(s.db, *str(table).split("."))
    out = set()
    for rows in src._every_row("src", s.db, table, read, where):
        for r in rows:
            out.add(tuple(canon.render_value(cls.get(k), v)
                          for k, v in zip(key, r)))
    return key, out


def collisions(hop, eng, db, group_=None):
    """{table: (key, [(key text, [source names])])} of every key two
    sources of the group hold in one table - read on each source."""
    group_ = group_ if group_ is not None else group(hop, eng, db)
    pair = _pair(eng)
    out = {}
    for t in _tables(hop, pair, db):
        held, key = {}, None
        for s in group_:
            try:
                k, texts = _key_texts(s, eng, t)
            except Exception:  # noqa: BLE001 - a source without the table
                continue
            key = key or k
            for x in texts or ():
                held.setdefault(x, []).append(s.name)
        both = [(x, names) for x, names in held.items() if len(names) > 1]
        if both:
            out[t] = (key, sorted(both))
    return out


def _integer_key(pair, db, table):
    """The table's one key column where it is a whole number, or None."""
    src = pair.src_engine
    key = list(src.neutral_key("src", db, table) or [])
    if len(key) != 1:
        return None
    return key[0] if _classes(src, "src", db, table).get(key[0]) == \
        "integer" else None


def _stride(hop, eng, group_, tables):
    """`remap`'s stride: a power of ten at least `ROOM` times the largest
    key any source holds in any table - worked out once and kept beside
    the group, so a later run shifts every key as the first did."""
    path = hop.report_dir() / "consolidate-keys.json"
    try:
        kept = json.loads(path.read_text())
    except (OSError, ValueError):
        kept = {}
    if kept.get("stride"):
        return int(kept["stride"]), kept
    top = 1
    for s in group_:
        pair = _pair(_engine(s, eng))
        for t in tables:
            col = _integer_key(pair, s.db, t)
            if col is None:
                continue
            k, texts = _key_texts(s, eng, t)
            for x in texts or ():
                try:
                    top = max(top, abs(int(x[0])))
                except (TypeError, ValueError):
                    continue
    stride = 10
    while stride < top * ROOM:
        stride *= 10
    order = sorted(str(s.value) for s in group_)
    kept = {"stride": stride, "order": order,
            "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    path.write_text(json.dumps(kept, indent=1))
    return stride, kept


def offset_of(hop, eng, s, group_, tables):
    """The offset every key of source `s` is shifted by: the hop's own
    under `offset`, the stride times its place under `remap`, 0 under
    `refuse` and `tenant`."""
    got = settings(hop)
    if got["keys"] == "offset":
        if str(s.value) not in got["offsets"]:
            raise SystemExit(f"consolidate: offsets names no offset for"
                             f" {s.value!r} ({s.name})")
        return got["offsets"][str(s.value)]
    if got["keys"] == "remap":
        stride, kept = _stride(hop, eng, group_, tables)
        order = kept.get("order") or []
        if str(s.value) not in order:
            order.append(str(s.value))
            kept["order"] = order
            (hop.report_dir() / "consolidate-keys.json").write_text(
                json.dumps(kept, indent=1))
        return order.index(str(s.value)) * stride
    return 0


def _key_facts(hop, pair, db, table, clash, rule):
    from .decide import Facts
    return Facts({"rule": rule, "clash": bool(clash)},
                 integer=lambda: _integer_key(pair, db, table) is not None)


def key_ways():
    """How a table's keys are kept apart on the target, as ways: the
    tenant column leading the key (every source's key is its own, nothing
    shifted), each source's integer keys shifted by its offset (the
    target's key as the application's is, the numbers not), or the keys
    as they are where no two sources hold one."""
    from .decide import Need, Rung
    return (
        Rung("tenant", "the tenant column leading the target's key",
             frozenset({"keys apart"}),
             needs=(Need("keys: tenant", lambda f: f["rule"] == "tenant",
                         "the hop's rule is not tenant"),),
             because="every source's key is its own under its value"),
        Rung("offset", "each source's keys shifted by its own offset",
             frozenset({"keys apart"}),
             needs=(Need("keys: offset or remap",
                         lambda f: f["rule"] in ("offset", "remap"),
                         "the hop's rule shifts no key"),
                    Need("a whole-number key",
                         lambda f: f["integer"],
                         "the table's key is not one whole number, and"
                         " only a whole number is shifted")),
             footprint=1,
             because="the keys stay one column, each source's in its own"
                     " range"),
        Rung("apart", "the keys as they are",
             frozenset({"keys apart"}),
             needs=(Need("no key two sources hold",
                         lambda f: not f["clash"],
                         "sources hold the same keys in it"),),
             footprint=2,
             because="no two sources hold one key"))


def key_way(hop, pair, db, table, clash):
    """The `decide.Choice` of how `table`'s keys are kept apart; its rung
    None where none holds - the refusal says why each could not."""
    from . import decide
    rule = settings(hop)["keys"]
    return decide.climb(f"{db}.{table}: keys apart", key_ways(),
                        _key_facts(hop, pair, db, table, clash, rule),
                        ("keys apart",))


# --- the plan of one table -----------------------------------------------------------

@dataclass
class TablePlan:
    table: str
    target: str
    key: list
    way: str
    #: {column: parent table} of the columns shifted with their parent's
    shifted: dict = field(default_factory=dict)
    #: the `decide.Choice` the way was taken by
    choice: object = None


def _fk_shifts(pair, db, table, ways):
    """{column: parent} of the columns of `table` that refer to a table
    whose keys are shifted: they are shifted by the same offset."""
    out = {}
    for child, cc, parent, pc in pair.src_engine.foreign_keys("src", db):
        if child != table or ways.get(parent) != "offset":
            continue
        if len(cc) != 1:
            raise SystemExit(f"{table} refers to {parent} by"
                             f" {', '.join(cc)}: only a one-column reference"
                             " to a shifted key is shifted with it")
        out[cc[0]] = parent
    return out


def table_plans(hop, eng, db, group_=None):
    """([TablePlan], [why a table cannot be carried]) of `db`."""
    group_ = group_ if group_ is not None else group(hop, eng, db)
    pair = _pair(eng)
    clash = collisions(hop, eng, db, group_)
    there = [] if pair.dst_engine.target_missing(db) else \
        pair.dst_engine.neutral_tables("dst", db)
    pairs, _, _, _ = pair.match_tables(_tables(hop, pair, db), there,
                                       pair._renamer(db))
    names = dict(pairs)
    ways, wrong, chosen = {}, [], {}
    for t in _tables(hop, pair, db):
        c = key_way(hop, pair, db, t, clash.get(t))
        if c.rung is None:
            key, both = clash.get(t, ([], []))
            shown = "; ".join(f"({', '.join(map(str, k))}) in"
                              f" {' and '.join(n)}"
                              for k, n in both[:EXAMPLES])
            wrong.append(f"{t}: {len(both):,} keys held by more than one"
                         f" source ({shown}) - {c.reason}")
            continue
        ways[t], chosen[t] = c.path, c
    out = []
    for t, w in ways.items():
        try:
            shifted = _fk_shifts(pair, db, t, ways)
        except SystemExit as e:
            wrong.append(str(e))
            continue
        out.append(TablePlan(t, names.get(t)
                             or pair._leaf(pair._rename(t, db)),
                             list(pair.src_engine.neutral_key("src", db, t)
                                  or []), w, shifted, chosen[t]))
    return out, wrong


def problems(hop, eng, db):
    """[what stops this consolidation before a row is read]: the group
    that does not hold together, keys two sources hold that the rule does
    not keep apart, a target table without the tenant column or keyed
    otherwise than the rule needs, shifted keys that meet or pass their
    column's range."""
    try:
        group_ = group(hop, eng, db)
        plans, wrong = table_plans(hop, eng, db, group_)
    except SystemExit as e:
        return [str(e)]
    got = settings(hop)
    pair = _pair(eng)
    dst = pair.dst_engine
    col = got["column"]
    there = set() if dst.target_missing(db) else set(
        dst.neutral_tables("dst", db))
    for p in plans:
        if p.target not in there:
            continue
        cols = dict(dst.neutral_columns("dst", db, p.target))
        if col not in cols:
            wrong.append(f"{p.target} on the target has no column {col}:"
                         " add it, or drop the table and migkit makes it")
            continue
        if p.way == "tenant":
            key = list(dst.neutral_key("dst", db, p.target) or [])
            if col not in key:
                wrong.append(f"{p.target} on the target is keyed by"
                             f" ({', '.join(key)}) and keys: tenant needs"
                             f" {col} in it")
    for p in plans:
        if p.way == "offset":
            wrong += _shifted_apart(hop, eng, db, p, group_, there)
    return wrong


def _shifted_apart(hop, eng, db, p, group_, there):
    """[why] the shifted keys of `p` are not apart: two sources' ranges,
    each shifted by its offset, that meet - `remap`'s stride outgrown, or
    offsets the hop chose too close - and a shifted key past the target
    column's range."""
    from . import canon
    dst = _pair(eng).dst_engine
    spans, wrong = [], []
    for s in group_:
        off = offset_of(hop, eng, s, group_, [p.table])
        _, texts = _key_texts(s, eng, p.table)
        keys = [int(x[0]) for x in texts or ()]
        if keys:
            spans.append((min(keys) + off, max(keys) + off, s.name, off))
    spans.sort()
    for a, b in zip(spans, spans[1:]):
        if b[0] <= a[1]:
            wrong.append(f"{p.table}: {a[2]}'s keys shifted by {a[3]:,}"
                         f" reach {a[1]:,} and {b[2]}'s shifted by"
                         f" {b[3]:,} start at {b[0]:,} - the ranges meet:"
                         " offsets further apart (or, under remap, a new"
                         " stride: consolidate-keys.json removed before any"
                         " row is written)")
    if p.target in there and spans:
        declared = dict(dst.neutral_columns("dst", db, p.target)).get(
            p.key[0])
        rng = canon.INT_RANGES.get(dst.CANON_ENGINE, {}).get(
            canon._split_declared(declared)[0]) if declared else None
        top = max(s[1] for s in spans)
        if rng and top > rng[1]:
            wrong.append(f"{p.target}.{p.key[0]} ({declared}) holds at most"
                         f" {rng[1]:,}, and the shifted keys reach {top:,}")
    return wrong


# --- the move ----------------------------------------------------------------------

def _tenant_column(value):
    """(name, class, numbers, rules) of the tenant column a table migkit
    makes holds: a whole number for a whole-number value, text else."""
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer", ()
    return "text", (64,)


def _make(hop, pair, db, p, value, log):
    """The target's table, made where it is not there: the source's
    columns, the tenant column, and the key the way needs."""
    got = settings(hop)
    columns, key = pair._target_shape(db, p.table, p.target)
    cls, numbers = _tenant_column(value)
    columns = list(columns) + [(got["column"], cls, numbers,
                                {"null": False})]
    if p.way == "tenant" and key:
        key = [got["column"]] + list(key)
    ddl = pair.dst_engine.neutral_create("dst", db, p.target, columns, key)
    log(f"{p.target}: made on the target with {got['column']} - {ddl}")


def _literal(dst, value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return dst._quote_literal(str(value))


def tenant_where(dst, column, value):
    """The target's predicate for one source's rows."""
    return f"{dst._quote_ident(column)} = {_literal(dst, value)}"


def _shift(row, names, p, offsets):
    out = list(row)
    for i, n in enumerate(names):
        if out[i] is None:
            continue
        if p.way == "offset" and n in p.key:
            out[i] = int(out[i]) + offsets[p.table]
        elif n in p.shifted:
            out[i] = int(out[i]) + offsets[p.shifted[n]]
    return out


def write_ways():
    """The ways a batch is written: the target's own bulk path, or the
    plainest one it has (`neutral_rewrite`) where that fails as it runs -
    both write by key, so a batch the first left half done converges."""
    from .decide import Rung
    return (Rung("bulk", "the target's own bulk write",
                 frozenset({"written"}), because="the fastest write the"
                                                 " target has"),
            Rung("plain", "the plainest write the target has",
                 frozenset({"written"}), footprint=1,
                 because="where the bulk write cannot run"))


def _order(tables, edges):
    after = {t: {pt for c, _, pt, _ in edges if c == t and pt != t}
             for t in tables}
    out, left = [], sorted(tables)
    while left:
        ready = [t for t in left if not (after[t] & set(left))]
        nxt = ready[0] if ready else left[0]
        out.append(nxt)
        left.remove(nxt)
    return out


def _done_path(hop, db):
    return hop.report_dir(db) / "consolidate-done.json"


def move(hop, eng, dbs, mode, table, go, log):
    """Each of this hop's sources into the target under its value: its
    rows there replaced, shifted as the plan says, written, the counters
    raised after, and proved; a plan without `go`."""
    from . import decide
    if mode != "full":
        raise SystemExit(f"{hop.name}: a consolidation is carried once"
                         " (--mode full): each source's changes followed"
                         " into one target under its value is not a way"
                         " migkit has. Nothing has been started")
    if table:
        raise SystemExit(f"{hop.name}: a consolidation is carried whole -"
                         " its keys are shifted and proved across every"
                         " table. Leave out --table")
    pair = _pair(eng)
    src, dst = pair.src_engine, pair.dst_engine
    got = settings(hop)
    for d in dbs:
        group_ = group(hop, eng, d)
        plans, wrong = table_plans(hop, eng, d, group_)
        if wrong:
            raise SystemExit(f"{d}: nothing was written - "
                             + "; ".join(wrong))
        me = next(s for s in group_ if s.hop is hop and s.db == d)
        tables = [p.table for p in plans]
        offsets = {t: offset_of(hop, eng, me, group_, [t]) for t in tables}
        edges = list(src.foreign_keys("src", d))
        log(f"[{d}] into {hop.target_db(d)} as {got['column']} ="
            f" {me.value!r}, beside {len(group_) - 1} other sources")
        for p in plans:
            log(f"{p.table}: {p.choice.said} - {p.choice.reason}"
                + (f" (+{offsets[p.table]:,})" if p.way == "offset" else ""))
            if go:
                decide.record(hop, p.choice)
        if not go:
            log(f"[{d}] dry-run: {len(plans)} tables; the target's rows of"
                f" {me.value!r} replaced, every other source's left as"
                " they are")
            continue
        made = dst.prepare_target(d)
        if made:
            log(made)
        there = set() if dst.target_missing(d) else set(
            dst.neutral_tables("dst", d))
        for p in plans:
            if p.target not in there:
                _make(hop, pair, d, p, me.value, log)
        try:
            done = json.loads(_done_path(hop, d).read_text())
        except (OSError, ValueError):
            done = {}
        order = _order(tables, edges)
        where = tenant_where(dst, got["column"], me.value)
        by = {p.table: p for p in plans}
        with pair.load_window(d, log, {by[t].target for t in order}):
            for t in reversed(order):
                if done.get(t, {}).get("value") == str(me.value):
                    continue
                gone = dst.neutral_empty("dst", d, by[t].target, where)
                if gone:
                    log(f"{by[t].target}: {gone:,} rows of {me.value!r}"
                        " removed before they are written again")
            for t in order:
                if done.get(t, {}).get("value") == str(me.value):
                    log(f"{t}: carried by an earlier run")
                    continue
                n = _carry(hop, pair, d, by[t], me, offsets, log)
                done[t] = {"value": str(me.value), "rows": n,
                           "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
                _done_path(hop, d).write_text(json.dumps(done, indent=1))
        said = dst.settle_target(d, from_source=False)
        if said:
            log(said)
        results = prove(hop, eng, d)
        for r in results:
            log(f"{r.scope}: {r.status.upper()} {r.detail}")
        bad = [r for r in results if r.status not in ("ok", "skip")]
        if bad:
            raise SystemExit(f"{d}: the consolidation is not what it was to"
                             " hold - " + "; ".join(f"{r.scope}: {r.detail}"
                                                    for r in bad[:4]))
        _done_path(hop, d).unlink(missing_ok=True)
        log(f"[{d}] carried and proved under {got['column']} ="
            f" {me.value!r}")


def _carry(hop, pair, db, p, me, offsets, log):
    """One table of one source: read through its row filter, shifted,
    the tenant value added, written a batch at a time."""
    from . import decide
    got = settings(hop)
    src, dst = pair.src_engine, pair.dst_engine
    src_cols, dst_cols, notes = pair._move_columns(p.table, p.target, db)
    for n in notes:
        log(f"{p.table}: {n}")
    names = [n for n, _ in src_cols]
    cls, _ = _tenant_column(me.value)
    columns = list(dst_cols) + [(got["column"], cls)]
    where = hop.row_filter(db, *str(p.table).split("."))
    n = 0
    for rows in src._every_row("src", db, p.table, src_cols, where):
        batch = [_shift(r, names, p, offsets) + [me.value] for r in rows]

        def run(choice, batch=batch):
            write = (dst.neutral_write if choice.rung.name == "bulk"
                     else dst.neutral_rewrite)
            for i in range(0, len(batch), BATCH):
                write("dst", db, p.target, columns, batch[i:i + BATCH])
            return len(batch)
        _, wrote = decide.attempt(f"{p.target} as {me.value!r}",
                                  write_ways(), run, log=log)
        n += wrote
    log(f"{p.table}: {n:,} rows written as {me.value!r}")
    return n


# --- the proof ---------------------------------------------------------------------

def _hash(texts):
    import hashlib
    h = hashlib.sha256()
    for t in texts:
        h.update(b"\x00" if t is None else b"\x01" + str(t).encode(
            "utf-8", "surrogatepass"))
        h.update(b"\x1f")
    return h.digest()[:16]


def prove(hop, eng, db):
    """[Result] of `db`'s tables in the target, source by source: this
    hop's own rows as they were written - shifted, its value - held to the
    target's rows of its value by key; the target's rows of a value no
    source of the group has; each counter past the rows."""
    from .engines.base import Result
    got = settings(hop)
    try:
        group_ = group(hop, eng, db)
        plans, wrong = table_plans(hop, eng, db, group_)
    except SystemExit as e:
        return [Result("consolidate", db, "error", str(e))]
    if wrong:
        return [Result("consolidate", db, "error", "; ".join(wrong))]
    pair = _pair(eng)
    src, dst = pair.src_engine, pair.dst_engine
    mine = [s for s in group_ if s.hop is hop and s.db == db]
    values = [s.value for s in group_]
    out = []
    for p in plans:
        try:
            sc, dc, notes = pair._comparable_columns(db, src, p.table, dst,
                                                     p.target)
        except Exception as e:  # noqa: BLE001 - said as the table's error
            out.append(Result("consolidate", f"{db}.{p.table}", "error",
                              f"could not be read: {_last(e)}"))
            continue
        notes = _without(notes, got["column"])
        names = [n for n, _ in sc]
        key = [k for k in p.key if k in names]
        at = [names.index(k) for k in key]
        for s in mine:
            offsets = {t.table: offset_of(hop, eng, s, group_, [t.table])
                       for t in plans}
            from collections import Counter
            where = hop.row_filter(db, *str(p.table).split("."))
            # by key; a table without one as a multiset of rows, so a row
            # there twice on one side is not one row
            a, b = ({}, {}) if key else (Counter(), Counter())
            for rows in src._every_row("src", db, p.table, sc, where):
                for r in rows:
                    texts = pair._row_text(sc, _shift(r, names, p, offsets))
                    if key:
                        a[tuple(texts[i] for i in at)] = _hash(texts)
                    else:
                        a[_hash(texts)] += 1
            for rows in dst._every_row("dst", db, p.target, dc,
                                       tenant_where(dst, got["column"],
                                                    s.value)):
                for r in rows:
                    texts = pair._row_text(dc, list(r))
                    if key:
                        b[tuple(texts[i] for i in at)] = _hash(texts)
                    else:
                        b[_hash(texts)] += 1
            if key:
                missing = [k for k in a if k not in b]
                extra = [k for k in b if k not in a]
                changed = [k for k in a if k in b and a[k] != b[k]]
            else:
                missing = list((a - b).elements())
                extra = list((b - a).elements())
                changed = []
            said = []
            if missing:
                said.append(f"{len(missing):,} rows missing"
                            + (f" ({_keys(missing)})" if key else ""))
            if extra:
                said.append(f"{len(extra):,} rows the source does not have"
                            + (f" ({_keys(extra)})" if key else ""))
            if changed:
                said.append(f"{len(changed):,} rows differ ({_keys(changed)})")
            # no brackets: the console reads them as its own markup
            scope = f"{db}.{p.table} {got['column']}={s.value}"
            if said:
                out.append(Result("consolidate", scope, "diff",
                                  "; ".join(said) + f" - under"
                                  f" {got['column']} = {s.value!r}", "",
                                  f"migkit move {hop.name} --mode full"
                                  " --go"))
            else:
                out.append(Result(
                    "consolidate", scope, "ok",
                    f"{(len(a) if key else sum(a.values())):,} rows as the"
                    " source's"
                    + (f", keys shifted by {offsets[p.table]:,}"
                       if p.way == "offset" and offsets[p.table] else "")
                    + (f" ({'; '.join(notes)})" if notes else "")))
        out.append(_strangers(dst, db, p, got["column"], values))
    out += _counters(dst, db)
    return out


def _without(notes, column):
    """The comparison's notes without the tenant column, which only the
    target has on purpose."""
    head = "columns only on the target, not compared: "
    out = []
    for n in notes:
        if n.startswith(head):
            rest = [c for c in n[len(head):].split(", ") if c != column]
            if rest:
                out.append(head + ", ".join(rest))
            continue
        out.append(n)
    return out


def _keys(ks):
    return ", ".join("(" + ", ".join(map(str, k)) + ")"
                     if isinstance(k, tuple) else "a row"
                     for k in ks[:EXAMPLES])


def _strangers(dst, db, p, column, values):
    """One Result: the target's rows of `p` under a value no source of the
    group has, counted."""
    from .engines.base import Result
    known = " and ".join(f"{dst._quote_ident(column)} <> {_literal(dst, v)}"
                         for v in values)
    pred = f"({dst._quote_ident(column)} is null or ({known}))"
    n = 0
    try:
        for rows in dst._every_row("dst", db, p.target,
                                   [(column, "text")], pred):
            n += len(rows)
    except Exception as e:  # noqa: BLE001 - said as its error
        return Result("consolidate", f"{db}.{p.table} others", "error",
                      f"could not be counted: {_last(e)}")
    if n:
        return Result("consolidate", f"{db}.{p.table} others", "diff",
                      f"{n:,} rows on the target are under a {column} no"
                      " source of the consolidation has")
    return Result("consolidate", f"{db}.{p.table} others", "ok",
                  f"every row on the target is under one of the"
                  f" {len(values)} sources' values")


def _counters(dst, db):
    """[Result]: every counter on the target past the rows every source
    wrote (`sequences_behind`, where the target's engine can say)."""
    from .engines.base import Result
    ask = getattr(dst, "sequences_behind", None)
    if ask is None:
        if dst.CANON_ENGINE == "mysql":
            return [_mysql_counters(dst, db)]
        return [Result("consolidate", f"{db} counters", "skip",
                       "the target's engine cannot be asked whether its"
                       " counters are past the rows - not asked")]
    try:
        behind = ask(db)
    except Exception as e:  # noqa: BLE001 - said as its error
        return [Result("consolidate", f"{db} counters", "error",
                       f"could not be read: {_last(e)}")]
    if behind:
        return [Result("consolidate", f"{db} counters", "diff",
                       f"{len(behind)} counters would hand out a key a"
                       " source's rows hold: " + ", ".join(
                           str(b[0]) for b in behind[:4]), "",
                       "migkit sync --kind sequences")]
    return [Result("consolidate", f"{db} counters", "ok",
                   "every counter past every source's rows")]


def _mysql_counters(dst, db):
    """One Result: the target's AUTO_INCREMENT counters of the tables the
    hop takes past their rows - a table it leaves out keeps the counter
    its own writer gives it (`Engine.in_scope`)."""
    from .engines.base import Result
    ddb = dst._d("dst", db)
    rows = [r for r in dst._q(
        "dst", "select c.table_name, c.column_name, t.auto_increment from"
               " information_schema.columns c join information_schema.tables"
               " t on t.table_schema = c.table_schema and t.table_name ="
               " c.table_name where c.table_schema = %s and c.extra like %s",
        (ddb, "%auto_increment%")) if dst.in_scope(db, r[0])]
    behind = []
    for t, c, nxt in rows:
        top = dst._q("dst", f"select max({dst._quote_ident(c)}) from"
                            f" {dst._quote_ident(ddb)}."
                            f"{dst._quote_ident(t)}")[0][0]
        if top is not None and nxt is not None and int(nxt) <= int(top):
            behind.append(f"{t}.{c}")
    if behind:
        return Result("consolidate", f"{db} counters", "diff",
                      "AUTO_INCREMENT at or below the rows in "
                      + ", ".join(behind[:4]))
    return Result("consolidate", f"{db} counters", "ok",
                  f"{len(rows)} AUTO_INCREMENT counters past every"
                  " source's rows")


def check(hop, eng, db):
    """`check` of a consolidation hop: the proof."""
    return prove(hop, eng, db)


def plan_lines(hop, eng, db):
    """The plan's line of a consolidation, or []."""
    got = settings(hop)
    if got is None or not db:
        return []
    try:
        group_ = group(hop, eng, db)
    except SystemExit as e:
        return [f"  a consolidation that does not hold together: {e}"]
    return [f"  a consolidation: {len(group_)} sources into"
            f" {hop.target_db(db)}, each under {got['column']}"
            f" ({', '.join(repr(s.value) for s in group_[:5])}), keys kept"
            f" apart by {got['keys']}; every source's rows proved by its"
            " value"]
