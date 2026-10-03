"""Did the source's shape change while it was being moved.

A DDL on the source in the middle of a move is invisible until the next
schema check: the rows that moved before it and after it belong to two
different tables, and the check that follows compares them as one. The
paid services watch for it in their change stream. Where migkit reads no
stream, it reads the catalogue before the move and again after, through
the neutral contract every engine that moves data already speaks, and
says what changed.
"""

import functools
import re

#: the working tables of an online schema change: gh-ost's ghost, changelog
#: and deleted tables, pt-online-schema-change's new and old. They come and
#: go while the tool works; what matters is the real table's shape once it
#: swaps them in, and that is what a shape comparison sees.
ONLINE_SCHEMA_CHANGE = re.compile(r"^_.+_(gho|ghc|del|new|old)$")

#: working tables no application names its own: pg_repack's log and copy
#: of the table it rewrites, and their indexes and sequences, in the schema
#: it keeps for itself (`repack.log_16384`, `repack.table_16384`); the
#: server's own copy of a table an ALTER rebuilds (`#sql-...`) - all MySQL
#: Shell's load leaves in a schema besides its view placeholders, which
#: carry the view's own name, when it adds the indexes it deferred; Vitess's
#: shadow and retired tables (`_vt_hld_...`, `_vt_HOLD_...`,
#: `_<uuid>_<time>_vrepl`); Spirit's sentinel
OWN_NAMES = (re.compile(r"^repack\.(log|table|index)_\d+(_.*)?$"),
             re.compile(r"^#sql"),
             re.compile(r"^_vt_(hld|prg|evc|drp|vrp|hold|purge|evac|drop)_",
                        re.I),
             re.compile(r"^_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}"
                        r"_[0-9a-f]{12}_\d{14}_vrepl$"),
             re.compile(r"^_spirit_sentinel$"))

#: names an application could also choose, a working table only beside the
#: table it is a copy of - the group is that table's name: gh-ost's and
#: pt-online-schema-change's (above), Spirit's checkpoint, Facebook's
#: OnlineSchemaChange, and LHM's new and archived copies
BESIDE_ITS_TABLE = (re.compile(r"^_(.+)_(?:gho|ghc|del|new|old|chkpnt)$"),
                    re.compile(r"^_(.+)_\d{14}_del$"),
                    re.compile(r"^__osc_(?:new|chg|old)_(.+)$"),
                    re.compile(r"^lhmn_(.+)$"),
                    re.compile(r"^lhma_(?:\d+_)+(.+)$"))

#: the triggers those tools put on the table they copy, which leave with
#: them: pg_repack's, pt-online-schema-change's, Facebook's and LHM's
TRANSIENT_TRIGGERS = re.compile(
    r"^(z_)?repack_trigger$|^pt_osc_.+_(ins|upd|del)$"
    r"|^__osc_(ins|upd|del)_.+$|^lhmt_(ins|upd|del)_.+$")


def transient(table, among=None):
    """True for a table an online schema change is working in, or one that
    is another tool's own state (`leftovers.bookkeeping`, the rule
    `Hop.excluded` asks too - one rule for gh-ost's changelog, not two).

    `among`, the names of the tables on the same side, is what lets a name
    an application could also have chosen count: `_orders_new` is a working
    table beside `orders` and an application's own without it. Without
    `among` those names are read as they always were - gh-ost's and
    pt-online-schema-change's by their name alone - and the rest only where
    no application would choose it. A set is used as it is: the caller
    that asks for every table of a side builds it once (`transient_among`).
    """
    name = str(table)
    if among is None:
        return _transient_alone(name)
    if _own(name):
        return True
    leaf = name.split(".")[-1]
    schema = name.rpartition(".")[0]
    others = among if isinstance(among, (set, frozenset)) else \
        {str(t) for t in among}
    for p in BESIDE_ITS_TABLE:
        got = p.match(leaf)
        if got and (got.group(1) in others
                    or (schema and f"{schema}.{got.group(1)}" in others)):
            return True
    return False


@functools.lru_cache(maxsize=4096)
def _own(name):
    from .leftovers import bookkeeping
    leaf = name.split(".")[-1]
    if any(p.match(name) or p.match(leaf) for p in OWN_NAMES):
        return True
    # asked as a table of some database: the database a hop names is the
    # operator's choice, what lies in it is asked of
    return bool(bookkeeping("-", *name.split(".")[-2:]))


@functools.lru_cache(maxsize=4096)
def _transient_alone(name):
    """`transient` without the side's other tables: asked for every row
    change the tail reads, so each name is worked out once."""
    return _own(name) or bool(ONLINE_SCHEMA_CHANGE.match(name.split(".")[-1]))


def maybe_transient(table):
    """Whether the name alone could be an online schema change's working
    table - asked before reading what else the side holds."""
    name = str(table)
    leaf = name.split(".")[-1]
    return any(p.match(name) or p.match(leaf)
               for p in OWN_NAMES + BESIDE_ITS_TABLE)


def transient_among(tables):
    """The tables of one side that an online schema change is working in.
    The side's names are gathered once: gathered per table, 5,000 tables
    took 509 ms and a move asks this several times a table (measured,
    2026-10-01); once, the same answer in a few."""
    tables = [str(t) for t in tables]
    among = frozenset(tables)
    return {t for t in tables if transient(t, among)}


def transient_trigger(name):
    """True for a trigger an online schema change put on the table it is
    copying."""
    return bool(TRANSIENT_TRIGGERS.match(str(name).split(".")[-1]))


# ---- a table's shape ---------------------------------------------------------
#
# A shape is {table: [(column, definition), ...]}: what every reader of
# one takes apart - a column's name and a text - and what a JSON file
# beside a run keeps as it is. Underneath it is the side's `catalog.Catalog`
# (an engine's `shape_catalog`), carried whole: a column's definition is
# its declared type with every number, followed after `PART` by the rest
# the server declares of it (nullability, default, collation, identity or
# computed expression), and the first column's also carries, after
# `TABLE`, the table's key, indexes, foreign keys and the rest the server
# says of it (its checks and triggers). `catalog_of` reads a shape back
# into the catalogue, so the one comparer and the one explanation are the
# catalogue's (`Catalog.fingerprint`, `Catalog.explain`), whether a shape
# was read now or saved by an earlier run. An engine that reads only
# (column, type) - `column_catalog` - is compared by that.

#: before the rest of a column's definition
PART = "\x1f"
#: before the table's own parts, on its first column
TABLE = "\x1e"


def pairs(cat):
    """`catalog.Catalog` -> {table: [(column, definition)]}, everything it
    holds kept (`catalog_of` reads it back)."""
    import json
    out = {}
    for t in cat.tables:
        cols = []
        for c in cat.columns.get(t) or []:
            text = str(c.declared)
            rest = [bool(c.nullable), c.default, c.collation, c.extra or ""]
            if rest != [True, None, None, ""]:
                text += PART + json.dumps(rest)
            cols.append((str(c.name), text))
        whole = {"key": list(cat.keys.get(t) or ()),
                 "indexes": [[i.name, list(i.columns), bool(i.unique),
                              i.definition or ""]
                             for i in cat.indexes.get(t) or []],
                 "fks": [[f.name, list(f.columns), f.ref_table,
                          list(f.ref_columns), f.on_delete, f.on_update]
                         for f in cat.fks.get(t) or []],
                 "others": sorted(str(o) for o in cat.others.get(t) or [])}
        if cols and any(whole.values()):
            name, first = cols[0]
            cols[0] = (name, first + TABLE + json.dumps(
                whole, sort_keys=True, separators=(",", ":")))
        out[t] = cols
    return out


def catalog_of(shape):
    """A shape (`pairs`, or an engine's plain (column, type) pairs) as the
    `catalog.Catalog` it was made from."""
    import json

    from .catalog import Catalog, Column, ForeignKey, Index
    cat = Catalog()
    for t, cols in (shape or {}).items():
        columns = []
        for name, text in cols or []:
            text = str(text)
            own, _, whole = text.partition(TABLE)
            declared, _, rest = own.partition(PART)
            nullable, default, collation, extra = (
                json.loads(rest) if rest else [True, None, None, ""])
            columns.append(Column(str(name), declared, "", nullable, default,
                                  collation, extra))
            if whole:
                got = json.loads(whole)
                cat.keys[t] = tuple(got.get("key") or ())
                cat.indexes[t] = [Index(i[0], tuple(i[1]), bool(i[2]), i[3])
                                  for i in got.get("indexes") or []]
                cat.fks[t] = [ForeignKey(f[0], tuple(f[1]), f[2],
                                         tuple(f[3]), f[4], f[5])
                              for f in got.get("fks") or []]
                cat.others[t] = list(got.get("others") or [])
        cat.columns[t] = columns
        cat.tables.append(t)
    return cat


def reader(engine):
    """The engine that reads the source's whole column catalogue in one
    query, or None. A cross-engine hop reads it through its source's
    engine; a document store's shape is a scan of every document, which
    nothing that only watches may cost."""
    src = getattr(engine, "src_engine", None) or engine
    return src if (hasattr(src, "shape_catalog")
                   or hasattr(src, "column_catalog")) else None


def side_reader(engine, side):
    """`reader` for either side: a pair's target read through its target's
    engine."""
    if side == "src":
        return reader(engine)
    dst = getattr(engine, "dst_engine", None) or engine
    return dst if (hasattr(dst, "shape_catalog")
                   or hasattr(dst, "column_catalog")) else None


#: the two sides' shapes as a move left them (`kept_at_move`)
AT_MOVE = "schema-at-move.json"


def kept_at_move(hop, engine, db):
    """Both sides' shapes as the move of `db` left them, kept beside its
    reports: the baseline a later check holds each side to, so it can say
    what changed since the move - a schema snapshot at each change, as a
    schema tool keeps one at each migration it runs. Nothing is kept for
    a side that cannot be read."""
    import json
    import time
    got = {"at": time.time()}
    for side in ("src", "dst"):
        r = side_reader(engine, side)
        got[side] = shape(r, side, db) if r is not None else None
    if got["src"] is None and got["dst"] is None:
        return
    if hasattr(hop, "scope_fingerprint"):
        got["scope_fingerprint"] = hop.scope_fingerprint()
    path = hop.report_dir(db) / AT_MOVE
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(got, default=list))
    tmp.replace(path)


def since_the_move(hop, engine, db):
    """[(side, when the move left it, lines)] of what changed in each
    side's schema since the move left it (`kept_at_move`) - [] where
    nothing did, no move was kept, or it was kept under another scope (its
    tables are not this scope's)."""
    import json
    import time
    try:
        kept = json.loads((hop.report_dir(db) / AT_MOVE).read_text())
    except (OSError, ValueError):
        return []
    if hasattr(hop, "scope_changed") and hop.scope_changed(kept):
        return []
    when = time.strftime("%Y-%m-%d %H:%M",
                         time.localtime(float(kept.get("at") or 0)))
    out = []
    for side in ("src", "dst"):
        before = kept.get(side)
        r = side_reader(engine, side)
        if before is None or r is None:
            continue
        before = {t: [tuple(c) for c in cols] for t, cols in before.items()}
        said = changes(before, shape(r, side, db))
        if said:
            out.append((side, when, said))
    return out


def shape(engine, side, db):
    """{table: [(column, definition), ...]} as the engine sees it now, or
    None when it cannot be read - never an exception: a move is not
    stopped by the thing that watches it. An engine that reads its whole
    catalogue in one pass (`shape_catalog`) is asked that way, else one
    that reads its columns so (`column_catalog`); the rest are read table
    by table.
    """
    try:
        if hasattr(engine, "shape_catalog"):
            got = pairs(engine.shape_catalog(side, db))
        elif hasattr(engine, "column_catalog"):
            got = engine.column_catalog(side, db)
        else:
            got = {t: sorted((str(n), str(ty)) for n, ty in
                             engine.neutral_columns(side, db, t))
                   for t in engine.neutral_tables(side, db)}
    except Exception:
        return None
    return in_scope(engine, db, got)


def in_scope(engine, db, got):
    """A shape without the tables the hop leaves alone - not moved, so
    their DDL is not the move's business - nor the scaffolding of an
    online schema change."""
    working = transient_among(got)
    return {t: cols for t, cols in got.items()
            if engine.hop.takes_table(db, t)
            and t not in working}


def said(shape):
    """A shape as lines a person reads: each column with the rest of its
    definition, then each table's key, indexes, foreign keys and the
    rest."""
    cat = catalog_of(shape)
    out = []
    for t in sorted(cat.tables):
        for c in cat.columns.get(t) or []:
            rest = [x for x in (
                "" if c.nullable else "not null",
                f"default {c.default}" if c.default is not None else "",
                f"collate {c.collation}" if c.collation else "", c.extra)
                if x]
            out.append(f"{t}.{c.name} {', '.join([c.declared] + rest)}\n")
        if cat.keys.get(t):
            out.append(f"{t}: key ({', '.join(cat.keys[t])})\n")
        out += [f"{t}: index {i.name} ({', '.join(i.columns)})"
                f"{' unique' if i.unique else ''} {i.definition}".rstrip()
                + "\n" for i in cat.indexes.get(t) or []]
        out += [f"{t}: foreign key {f.name} ({', '.join(f.columns)}) ->"
                f" {f.ref_table} ({', '.join(f.ref_columns)})\n"
                for f in cat.fks.get(t) or []]
        out += [f"{t}: {o}\n" for o in sorted(cat.others.get(t) or [])]
    return "".join(out)


def table_changes(cols_a, cols_b, engine=None, added="added",
                  dropped="dropped"):
    """What differs between one table's two shapes, in words - the
    catalogue's own explanation (`Catalog.explain`): columns `added` and
    `dropped`, each column's type (a narrower one named with why), its
    nullability, default, collation and expression, and the table's key,
    indexes, foreign keys, checks and triggers. The one comparer of two
    tables: drift over time, and a schema check across a hop's two sides
    (`added` and `dropped` then in that check's words)."""
    a, b = catalog_of({"t": cols_a}), catalog_of({"t": cols_b})
    if a.fingerprint("t") == b.fingerprint("t"):
        return []
    return a.explain(b, "t", engine, added, dropped)


def changes(before, after, engine=None):
    """What differs between two shapes, one line per table, in words.
    `engine`, where the two are one engine's, names a narrowing with why
    (`verdict.narrowing`)."""
    if before is None or after is None:
        return []
    out = []
    for t in sorted(set(before) - set(after)):
        out.append(f"{t}: dropped")
    for t in sorted(set(after) - set(before)):
        out.append(f"{t}: created")
    for t in sorted(set(before) & set(after)):
        parts = table_changes(before[t], after[t], engine)
        if parts:
            out.append(f"{t}: {', '.join(parts)}")
    return out


def changed_tables(before, after):
    """The tables whose shape differs between two shapes: altered,
    created or dropped - anything the server says of a table, by its
    fingerprint (`Catalog.fingerprint`)."""
    if before is None or after is None:
        return set()
    a, b = catalog_of(before).shape(), catalog_of(after).shape()
    return a.changed(b)
