"""Two ways at once through migkit's own tails (backlog R3).

Two hops of one pair of databases, each with `two_way` in its options, A
to B and B to A: each side's changes carried to the other, what migkit
applied never carried back, and a row both sides changed decided by the
hop's policy - or the tail stopped on it.

    two_way:
      on_conflict: error            # apply_remote, keep_local,
                                    # last_update_wins, source_priority
      column: updated_at            # what last_update_wins compares
      source_rank: 2                # source_priority, and the tie of
      target_rank: 1                #   last_update_wins: the higher wins
      delta: [balance, stock]       # counters: both sides' changes added

How migkit's own writes are told apart: every transaction a two-way tail
applies writes first a row of a table of its own on that side
(`migkit_origin`, a row per applying thread, its count moved), and the
tail reading that side the other way sees that row first in the
transaction and leaves the whole transaction out. The table is written in
the application's database, so it is made only where a hop asks for two
ways. Where the servers' own replication can run both ways instead
(`loops_prevented`), that needs no table.

A conflict is told from the target's row as it is now, held to what the
change says the row was before it (the full row a binlog keeps, or a
PostgreSQL table with REPLICA IDENTITY FULL): a row the target changed
too is `update_origin_differs` or `delete_origin_differs`; a row it made
too, with other values, `insert_exists`; one it has not got,
`update_missing`. Without a before image only the last three are seen,
and said so once. Every conflict is written to `conflicts.jsonl` with both
versions of the row, whatever was decided.

A counter (`delta`) is not decided: what each side added is kept. A row
whose differences are only in its counters is no conflict at all - the
change is applied with each counter moved by what the target moved it
since (its value now less the change's before image), and so is every
later change of that row in the batch. Where other columns conflict too,
those are decided by the policy, and where the policy keeps the target's
row, the change still goes on as the counters alone.
"""
import datetime
import json
import threading

from . import canon

TABLE = "migkit_origin"
POLICIES = ("error", "apply_remote", "keep_local", "last_update_wins",
            "source_priority")
#: the classes a counter can be
NUMERIC = ("integer", "decimal", "float")


def settings(hop):
    got = (hop.options or {}).get("two_way")
    if not got:
        return None
    return got if isinstance(got, dict) else {}


def policy(hop):
    """(policy, column) of a two-way hop; refused where it is not one
    migkit knows, or last_update_wins names no column."""
    got = settings(hop) or {}
    name = str(got.get("on_conflict", "error"))
    if name not in POLICIES:
        raise SystemExit(f"two_way.on_conflict: {name} is not one of"
                         f" {', '.join(POLICIES)}")
    column = got.get("column")
    if name == "last_update_wins" and not column:
        raise SystemExit("two_way.on_conflict: last_update_wins compares a"
                         " column each row carries its last change's time"
                         " in - name it as two_way.column")
    if name == "source_priority" and _ranks(hop) is None:
        raise SystemExit("two_way.on_conflict: source_priority keeps the"
                         " row of the side ranked higher - give both as"
                         " two_way.source_rank and two_way.target_rank,"
                         " apart")
    return name, column


def _ranks(hop):
    """(source rank, target rank), or None where the hop ranks neither or
    ranks both alike."""
    got = settings(hop) or {}
    try:
        a, b = int(got["source_rank"]), int(got["target_rank"])
    except (KeyError, TypeError, ValueError):
        return None
    return None if a == b else (a, b)


def deltas(hop):
    got = (settings(hop) or {}).get("delta") or []
    if isinstance(got, str):
        got = [got]
    return [str(x) for x in got]


def refuse_unless_able(src, dst):
    """Both engines must mark what they apply and skip what is marked:
    one that cannot sends every change back where it came from."""
    for eng, role in ((src, "source"), (dst, "target")):
        if not getattr(eng, "READS_ORIGIN_MARK", False) or \
                not callable(getattr(eng, "origin_mark", None)):
            raise SystemExit(
                f"two_way: the {role} ({type(eng).__name__[:-6].lower()})"
                " cannot mark what migkit applies or leave it out when"
                " read, so every change would come back to where it began."
                " Two ways run through migkit between PostgreSQL and MySQL"
                " (or MariaDB) sides")


def thread_origin(hop):
    """The row of `migkit_origin` this applying thread moves: one a
    thread, so lanes applying side by side do not wait on one row - and
    one for the hop where it counts (`exact`), whose batches are one
    transaction each."""
    if exact(hop):
        return hop.name
    return f"{hop.name}:{threading.get_ident() % 1024}"


def exact(hop):
    """Whether a batch applied twice would be wrong: counters (`delta`)
    add, so a batch the target committed and the tail had not yet saved
    the position of must not be applied again. Such a hop's batch is one
    transaction, and says in its mark which batch it was (`batch_seen`);
    the tail going on after a stop asks the target (`committed_ahead`)."""
    return settings(hop) is not None and bool(deltas(hop))


def batch_seen(engine):
    """The text the mark of the batch being applied carries: its
    position and its number, set by the tail before it applies."""
    return engine.__dict__.get("_batch_seen")


def seen_of(text):
    if not text:
        return None
    try:
        got = json.loads(text)
    except ValueError:
        return None
    return got if isinstance(got, dict) and "batch" in got else None


def committed_ahead(dst, db, batch):
    """(position, number) of a batch the target committed after the one
    the tail saved last, or None: the target's mark is written in the
    batch's own transaction, so it is there exactly when the batch is."""
    got = dst.origin_seen("dst", db)
    if got and int(got.get("batch", -1)) == batch + 1:
        return got["token"], batch + 1
    return None


def _typed(cls, value):
    """A value a change log handed over as text, as the class it is: a
    PostgreSQL slot's timestamp is the string it printed, and a MySQL
    target's the datetime its driver made - rendered apart, they differed
    where nothing had changed (measured)."""
    import datetime
    import decimal
    if not isinstance(value, str):
        return value
    try:
        if cls == "timestamp":
            return datetime.datetime.fromisoformat(value)
        if cls == "date":
            return datetime.date.fromisoformat(value)
        if cls == "time":
            return datetime.time.fromisoformat(value)
        if cls == "decimal":
            return decimal.Decimal(value)
        if cls == "integer":
            return int(value)
        if cls == "float":
            return float(value)
        if cls == "boolean":
            return value.lower() in ("t", "true", "1", "y", "yes", "on")
    except (ValueError, decimal.InvalidOperation):
        return value
    return value


def _text(cls, value):
    if value is None:
        return None
    try:
        return canon.render_value(cls, _typed(cls, value))
    except Exception:  # noqa: BLE001 - compared as it prints
        return str(value)


def _differs(classes, a, b):
    """Whether two rows differ on the columns both have, as the check
    renders them."""
    names = [n for n in a if n in b and n in classes]
    return [n for n in names
            if _text(classes[n], a[n]) != _text(classes[n], b[n])]


def _ident(c):
    return (c["table"], tuple(sorted((k, repr(v))
                                     for k, v in c["key"].items())))


def _number(cls, value):
    if value is None:
        return None
    return _typed(cls, value if isinstance(value, str) else str(value))


def resolve(pair, db, changes, log):
    """The changes to apply after each is held to the target's row: those
    a conflict decides against are left out, a row's counters moved by
    what the target added to them, and a conflict under `error` stops the
    tail before anything of the batch is applied."""
    name, column = policy(pair.hop)
    counters = deltas(pair.hop)
    ranks = _ranks(pair.hop)
    dst = pair.dst_engine
    first, order = {}, []
    for c in changes:
        ident = _ident(c)
        if ident not in first:
            first[ident] = c
            order.append(ident)
    by_table = {}
    for ident in order:
        by_table.setdefault(ident[0], []).append(first[ident])
    lost, kept_back, only_counters, classes_of = [], set(), set(), {}
    blind = False
    for table, firsts in by_table.items():
        columns = [(n, canon.type_class(dst.CANON_ENGINE, t) or "text")
                   for n, t in dst.neutral_columns("dst", db, table)]
        classes = dict(columns)
        for n in counters:
            if n in classes and classes[n] not in NUMERIC:
                raise SystemExit(
                    f"two_way.delta: {table}.{n} is {classes[n]}, not a"
                    " number - a counter is added to on both sides, so it"
                    " has to be one")
        key = sorted(firsts[0]["key"])
        keys = [tuple(c["key"][k] for k in key) for c in firsts]
        now = dst.neutral_rows_by_key("dst", db, table, columns, key, keys)
        names = [n for n, _ in columns]
        for c in firsts:
            at = tuple(_text(classes.get(k, "text"), c["key"][k])
                       for k in key)
            row = now.get(at)
            here = dict(zip(names, row)) if row is not None else None
            kind = None
            if c["op"] == "insert" and here is not None \
                    and _differs(classes, c["values"], here):
                kind = "insert_exists"
            elif c["op"] == "update" and here is None:
                kind = "update_missing"
            elif c["op"] in ("update", "delete") and here is not None:
                if c["op"] == "update" and not _differs(
                        classes, c["values"], here):
                    # already as the change leaves it: applied before a
                    # stop, and read again from the saved position
                    kind = None
                elif c.get("before") is None:
                    blind = True
                elif _differs(classes, c["before"], here):
                    kind = (f"{c['op']}_origin_differs")
            ident = _ident(c)
            if kind == "update_origin_differs" and counters and not [
                    n for n in _differs(classes, c["before"], here)
                    if n not in counters]:
                # only what both sides count moved: added, not decided
                _record(pair, db, table, c, here, "counters_moved", name,
                        "added")
                continue
            if kind is None:
                continue
            decision = _decide(name, column, c, here, classes, ranks)
            _record(pair, db, table, c, here, kind, name, decision)
            if decision == "stop":
                raise SystemExit(
                    f"two_way: {kind} on {table} {c['key']}: the target's"
                    " row changed there too, and on_conflict is error."
                    f" Both versions are in conflicts.jsonl; decide the row"
                    " on both sides, then run the tail again")
            if decision == "keep_local":
                if counters and c["op"] == "update" and here is not None:
                    # the target's row kept, but what the source added to
                    # its counters still added
                    only_counters.add(ident)
                else:
                    kept_back.add(ident)
                lost.append(kind)
        for c in changes:
            if c["table"] == table:
                classes_of[_ident(c)] = classes
    if blind and not getattr(pair, "_said_blind", False):
        pair._said_blind = True
        log("two_way: the source keeps no whole previous row for some"
            " changes, so a row the target changed too is found only when"
            " it is missing or made twice - keep full row images on the"
            " source (binlog_row_image FULL, REPLICA IDENTITY FULL)")
    if lost:
        log(f"two_way: {len(lost)} change(s) left out, the target's row"
            " kept (keep_local, last_update_wins or source_priority)")
    out = []
    for c in changes:
        ident = _ident(c)
        if ident in kept_back:
            continue
        if counters and c["op"] == "update":
            c = _as_added(c, counters, classes_of.get(ident, {}), log, pair)
            if ident in only_counters:
                values = {n: v for n, v in c["values"].items()
                          if isinstance(v, canon.Added)}
                if not values:
                    continue
                c = dict(c, values=values)
        out.append(c)
    return out


def _as_added(c, counters, classes, log, pair):
    """An update with each counter it moved as what it added (after less
    before), applied where the target's row stands (`canon.Added`). A
    change with no whole previous row cannot say what it added: written
    as it ends, and said once."""
    before = c.get("before")
    values = dict(c.get("values") or {})
    moved = False
    for n in counters:
        if n not in values or values[n] is None:
            continue
        if before is None or before.get(n) is None:
            if not getattr(pair, "_said_counter_blind", False):
                pair._said_counter_blind = True
                log("two_way.delta: a change without the row as it was"
                    " before cannot say what it added to a counter, so it"
                    " is written as it ends - keep full row images on the"
                    " source")
            continue
        cls = classes.get(n, "decimal")
        to = _number(cls, values[n])
        by = to - _number(cls, before[n])
        values[n] = canon.Added(by, to)
        moved = True
    return dict(c, values=values) if moved else c


def _decide(name, column, change, here, classes, ranks=None):
    if name == "error":
        return "stop"
    if name == "apply_remote":
        return "apply"
    if name == "keep_local":
        return "keep_local"
    if name == "source_priority":
        return "apply" if ranks[0] > ranks[1] else "keep_local"
    # last_update_wins: the newer of the two by the named column; on a tie
    # the side ranked higher, the incoming change where neither is ranked,
    # and the incoming change where either side has no value
    theirs = (change.get("values") or {}).get(column)
    mine = (here or {}).get(column)
    if theirs is None or mine is None:
        return "apply"
    cls = classes.get(column, "text")
    try:
        a, b = _typed(cls, theirs), _typed(cls, mine)
        newer, tie = a > b, a == b
    except TypeError:
        a, b = _text(cls, theirs), _text(cls, mine)
        newer, tie = a > b, a == b
    if tie:
        return "apply" if ranks is None or ranks[0] > ranks[1] \
            else "keep_local"
    return "apply" if newer else "keep_local"


def _record(pair, db, table, change, here, kind, name, decision):
    path = pair.hop.report_dir(db) / "conflicts.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps({
            "at": datetime.datetime.now().isoformat(timespec="seconds"),
            "hop": pair.hop.name, "table": table, "kind": kind,
            "policy": name, "decision": decision, "key": change["key"],
            "local": here, "remote": change.get("values"),
            "remote_before": change.get("before"), "op": change["op"]},
            default=str) + "\n")
