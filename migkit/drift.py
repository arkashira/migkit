"""Did the source's shape change while it was being moved.

A DDL on the source in the middle of a move is invisible until the next
schema check: the rows that moved before it and after it belong to two
different tables, and the check that follows compares them as one. The
paid services watch for it in their change stream. Where migkit reads no
stream, it reads the catalogue before the move and again after, through
the neutral contract every engine that moves data already speaks, and
says what changed.
"""


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
    """True for a table an online schema change is working in.

    `among`, the names of the tables on the same side, is what lets a name
    an application could also have chosen count: `_orders_new` is a working
    table beside `orders` and an application's own without it. Without
    `among` those names are read as they always were - gh-ost's and
    pt-online-schema-change's by their name alone - and the rest only where
    no application would choose it.
    """
    name = str(table)
    leaf = name.split(".")[-1]
    if any(p.match(name) or p.match(leaf) for p in OWN_NAMES):
        return True
    if among is None:
        return bool(ONLINE_SCHEMA_CHANGE.match(leaf))
    schema = name.rpartition(".")[0]
    others = {str(t) for t in among}
    for p in BESIDE_ITS_TABLE:
        got = p.match(leaf)
        if got and (got.group(1) in others
                    or (schema and f"{schema}.{got.group(1)}" in others)):
            return True
    return False


def maybe_transient(table):
    """Whether the name alone could be an online schema change's working
    table - asked before reading what else the side holds."""
    name = str(table)
    leaf = name.split(".")[-1]
    return any(p.match(name) or p.match(leaf)
               for p in OWN_NAMES + BESIDE_ITS_TABLE)


def transient_among(tables):
    """The tables of one side that an online schema change is working in."""
    tables = [str(t) for t in tables]
    return {t for t in tables if transient(t, tables)}


def transient_trigger(name):
    """True for a trigger an online schema change put on the table it is
    copying."""
    return bool(TRANSIENT_TRIGGERS.match(str(name).split(".")[-1]))


def reader(engine):
    """The engine that reads the source's whole column catalogue in one
    query, or None. A cross-engine hop reads it through its source's
    engine; a document store's shape is a scan of every document, which
    nothing that only watches may cost."""
    src = getattr(engine, "src_engine", None) or engine
    return src if hasattr(src, "column_catalog") else None


def shape(engine, side, db):
    """{table: [(column, type), ...]} as the engine sees it now, or None
    when it cannot be read - never an exception: a move is not stopped by
    the thing that watches it. An engine that can read its whole column
    catalogue in one query (`column_catalog`) is asked that way; the rest
    are read table by table.
    """
    try:
        whole = getattr(engine, "column_catalog", None)
        if whole is not None:
            got = whole(side, db)
        else:
            got = {t: sorted((str(n), str(ty)) for n, ty in
                             engine.neutral_columns(side, db, t))
                   for t in engine.neutral_tables(side, db)}
    except Exception:
        return None
    # a table the hop leaves alone is not moved, so its DDL is not the
    # move's business; nor is the scaffolding of an online schema change
    working = transient_among(got)
    return {t: cols for t, cols in got.items()
            if not engine.hop.excluded(db, *str(t).split("."))
            and t not in working}


def changes(before, after):
    """What differs between two shapes, one line per table, in words."""
    if before is None or after is None:
        return []
    out = []
    for t in sorted(set(before) - set(after)):
        out.append(f"{t}: dropped")
    for t in sorted(set(after) - set(before)):
        out.append(f"{t}: created")
    for t in sorted(set(before) & set(after)):
        a, b = dict(before[t]), dict(after[t])
        parts = [f"column {c} added" for c in sorted(set(b) - set(a))]
        parts += [f"column {c} dropped" for c in sorted(set(a) - set(b))]
        parts += [f"column {c} {a[c]} -> {b[c]}"
                  for c in sorted(set(a) & set(b)) if a[c] != b[c]]
        if parts:
            out.append(f"{t}: {', '.join(parts)}")
    return out


def changed_tables(before, after):
    """The tables whose shape differs between two shapes: altered,
    created or dropped."""
    if before is None or after is None:
        return set()
    return {t for t in set(before) | set(after)
            if dict(before.get(t) or []) != dict(after.get(t) or [])
            or (t in before) != (t in after)}
