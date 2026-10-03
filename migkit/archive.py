"""Old rows moved to cheaper storage (backlog P0-SCENARIOS): the rows a
rule picks carried to an archive, proved there, and removed from the
source only once proved and only where the hop allows it.

    hops:
      orders-archive:
        goal: archive
        source: postgres://app@10.0.0.1/shop
        target: parquet:///archive/shop          # any engine migkit writes
        exclude: [customers]                     # the tables it leaves
        mapping:
          where: {orders: "created < '2024-01-01'"}   # the rows it archives
        options:
          archive: {remove: true}                # removed from the source
          source_changes: {archived_rows: yes}   # the source's own leave

**Picked by rule, added to**: every table the hop carries is archived by
its row filter (`mapping.where`, the one place a row is picked by); a
table with none is refused - an archive of every row is a copy. What the
archive holds already is kept: a row already there as the source has it
is left, a new one written, and one the archive holds otherwise is never
overwritten - it is refused by key. Each batch is read back and compared
as it is written.

**Removed only once proved, only where allowed** (`remove`): with
`archive: {remove: true}`, and the source's own answer for the change by
its name (`source_changes: {archived_rows: yes}`, or asked at a terminal
and recorded - never a word given for every change), behind the hop's
approvals (`source-setting`): each row is read again on both sides just
before it goes, and removed only where the archive holds it as the source
has it now; a row changed since it was archived stays and is named. A
row other rows still name on the source - a key's parent whose children
stay - is never removed. Children first, a batch at a time, each batch's
keys and each row's digest kept (`archive-ledger.jsonl`) before it is
removed, so a run stopped half way goes on from what is left.

**Reversible**: `archive: {restore: true}` puts every removed row back
from the archive into the source, under the same answer and approvals.

**Proved** (`prove`): every row the ledger says was removed on the
archive with its digest, and every row the rule still picks on the
source in the archive as the source has it.
"""
import dataclasses
import hashlib
import json
import time

GOAL = "archive"
OPTION = "archive"
CHANGE = "archived_rows"
BATCH = 1000
EXAMPLES = 3


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def settings(hop):
    """{"remove", "restore", "batch"} of a hop whose goal is archive; None
    for any other."""
    if (getattr(hop, "goal", "") or "") != GOAL:
        return None
    raw = (hop.options or {}).get("archive") or {}
    if not isinstance(raw, dict):
        raise SystemExit("archive: is a mapping - remove, restore, batch")
    try:
        batch = int(raw.get("batch") or BATCH)
    except (TypeError, ValueError):
        raise SystemExit(f"archive: batch: {raw.get('batch')!r} is not a"
                         " number of rows")
    if raw.get("remove") and raw.get("restore"):
        raise SystemExit("archive: remove and restore both - one of the two")
    return {"remove": bool(raw.get("remove")),
            "restore": bool(raw.get("restore")), "batch": max(batch, 1)}


def _pair(eng):
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


def tables(hop, eng, db):
    """[(source table, its rule)] of the tables the hop archives."""
    src = _pair(eng).src_engine
    return [(t, hop.row_filter(db, *str(t).split(".")))
            for t in src.neutral_tables("src", db)
            if hop.takes_table(db, t)]


def _source_as_target(hop, eng):
    """The source's own engine, on a copy of the hop whose target is the
    source: every write migkit makes goes through a writer that only
    writes a target, and this is the one it writes the source through -
    for the archive's rows alone, behind their answer and approvals."""
    from .engines import engine_named
    name = (hop.options or {}).get("source_engine") if hop.engine == \
        "hetero" else hop.engine
    back = dataclasses.replace(
        hop, name=f"{hop.name}-source", engine=name, target=hop.source,
        db_map={}, goal="",
        options={k: v for k, v in (hop.options or {}).items()
                 if k not in ("physical", "cutover", "reverse", "topology",
                              "two_way", "protect_target",
                              "source_engine", "target_engine")})
    e = engine_named(name, back)
    if not hasattr(e, "_apply_deletes") or not hasattr(e, "neutral_write"):
        raise SystemExit(f"removing rows from a {e.CANON_ENGINE} source is"
                         " not a way migkit has: the archive is carried and"
                         " proved, and the rows stay")
    return e


def problems(hop, eng, db):
    """[what stops this archive before a row is read]: a table carried
    with no rule, one with no key (a row is written, proved and removed
    by its key), a removal the source's engine cannot make."""
    try:
        got = settings(hop)
        picked = tables(hop, eng, db)
    except SystemExit as e:
        return [str(e)]
    src = _pair(eng).src_engine
    wrong = []
    for t, rule in picked:
        if not rule:
            wrong.append(f"{t}: the hop carries it and no rule picks its"
                         " rows (mapping.where) - an archive of every row"
                         " is a copy: give it a rule, or exclude it")
        if not src.neutral_key("src", db, t):
            wrong.append(f"{t}: no key, and an archived row is written,"
                         " proved and removed by its key")
        if (got["remove"] or got["restore"]) and hop.masks(db, t):
            # the archive holds the masked values: removed from the source,
            # the real ones would be held nowhere, and put back they would
            # come back masked
            wrong.append(f"{t}: the hop masks its columns, and the archive"
                         " holds them masked - removing the rows would"
                         " leave their values nowhere: archive it without"
                         " masking, or keep the rows on the source")
    if got["remove"] or got["restore"]:
        try:
            _source_as_target(hop, eng)
        except SystemExit as e:
            wrong.append(str(e))
    if got["remove"]:
        try:
            for t, parent, n in _still_named(hop, eng, db):
                wrong.append(f"{parent}: {n:,} of the rows its rule picks"
                             f" are named by rows of {t} that stay on the"
                             " source - removing them would leave those"
                             f" naming nothing: archive {t}'s rows with"
                             " them, or narrow the rule")
        except Exception as e:  # noqa: BLE001 - unknown, refused
            wrong.append(f"whether other rows still name the rows to remove"
                         f" could not be counted: {_last(e)}")
    return wrong


def _still_named(hop, eng, db):
    """[(child, parent, rows)] of the rows the rule picks in `parent`
    that rows of `child` the hop does not remove still name - asked of
    the source, read only."""
    src = _pair(eng).src_engine
    q = src._quote_ident
    rules = dict(tables(hop, eng, db))
    out = []
    for child, cc, parent, pc in src.foreign_keys("src", db):
        pr = rules.get(parent)
        if not pr:
            continue
        cr = rules.get(child)
        on = " and ".join(f"c.{q(a)} = p.{q(b)}" for a, b in zip(cc, pc))
        stays = f" and not ({cr})" if cr else ""
        n = int(src.run_rule(
            "src", db, f"select count(*) from {src._qualified('src', db, parent)}"
                       f" p where ({pr}) and exists (select 1 from"
                       f" {src._qualified('src', db, child)} c where {on}"
                       f"{stays})")[0][0] or 0)
        if n:
            out.append((child, parent, n))
    return out


# --- the rows, as text --------------------------------------------------------------

def _hash(texts):
    h = hashlib.sha256()
    for t in texts:
        h.update(b"\x00" if t is None else b"\x01" + str(t).encode(
            "utf-8", "surrogatepass"))
        h.update(b"\x1f")
    return h.hexdigest()[:32]


@dataclasses.dataclass
class Table:
    """One archived table as both sides read it."""
    table: str
    target: str
    rule: str
    key: list
    src_cols: list
    dst_cols: list
    notes: list

    def names(self):
        return [n for n, _ in self.src_cols]

    def at(self):
        names = self.names()
        return [names.index(k) for k in self.key]


def _table(pair, db, t, rule, there):
    got, _, _, _ = pair.match_tables([t], there, pair._renamer(db))
    target = got[0][1] if got else pair._leaf(pair._rename(t, db))
    key = list(pair.src_engine.neutral_key("src", db, t) or [])
    if not got:
        return Table(t, target, rule, key, [], [], [])
    sc, dc, notes = pair._comparable_columns(db, pair.src_engine, t,
                                             pair.dst_engine, target)
    return Table(t, target, rule, key, sc, dc, notes)


def _texts(pair, cols, row):
    return pair._row_text(cols, list(row))


def _keyed(pair, tb, rows, side="src"):
    """{key text: (row hash, row)} of rows read on one side."""
    cols = tb.src_cols if side == "src" else tb.dst_cols
    at = tb.at()
    out = {}
    for r in rows:
        texts = _texts(pair, cols, r)
        out[tuple(texts[i] for i in at)] = (_hash(texts), r)
    return out


def _dst_key(tb):
    names = tb.names()
    return [tb.dst_cols[names.index(k)][0] for k in tb.key]


def _there(pair, db, tb, key_rows):
    """The target's rows of `key_rows` (source rows, the key among them),
    by key text."""
    at = tb.at()
    keys = [tuple(r[i] for i in at) for r in key_rows]
    got = {}
    for i in range(0, len(keys), BATCH):
        got.update(pair.dst_engine.neutral_rows_by_key(
            "dst", db, tb.target, tb.dst_cols, _dst_key(tb),
            keys[i:i + BATCH]))
    return {k: (_hash(_texts(pair, tb.dst_cols, r)), r)
            for k, r in got.items()}


# --- the move ----------------------------------------------------------------------

def move(hop, eng, dbs, mode, table, go, log, ask=None):
    """The rows each rule picks carried to the archive and proved, then -
    where the hop allows it - removed from the source, or put back; a plan
    without `go`."""
    if mode != "full":
        raise SystemExit(f"{hop.name}: an archive is carried by its rule"
                         " (--mode full), run again to take the rows the"
                         " rule picks since. Nothing has been started")
    got = settings(hop)
    pair = _pair(eng)
    for d in dbs:
        wrong = problems(hop, eng, d)
        if wrong:
            raise SystemExit(f"{d}: nothing was written - "
                             + "; ".join(wrong))
        picked = [(t, r) for t, r in tables(hop, eng, d)
                  if not table or str(t) == table
                  or str(t).endswith("." + table)]
        if got["restore"]:
            restore(hop, eng, d, go, log, ask)
            continue
        log(f"[{d}] the archive: {len(picked)} tables by their rules")
        for t, rule in picked:
            log(f"{t}: where {rule}")
        if not go:
            log(f"[{d}] dry-run: the rows each rule picks added to the"
                " archive and proved"
                + ("; then removed from the source once proved, where the"
                   " source's answer allows it" if got["remove"] else ""))
            if got["remove"]:
                _offer(hop, d, [t for t, _ in picked], False, ask, log)
            continue
        made = pair.dst_engine.prepare_target(d)
        if made:
            log(made)
        for t, rule in picked:
            _copy(hop, pair, d, t, rule, got["batch"], log)
        bad = [r for r in prove(hop, eng, d) if r.status not in ("ok",
                                                                 "skip")]
        if bad:
            raise SystemExit(f"{d}: the archive is not what it was to hold,"
                             " nothing was removed - "
                             + "; ".join(f"{r.scope}: {r.detail}"
                                         for r in bad[:4]))
        log(f"[{d}] every row the rules pick is in the archive, proved")
        if got["remove"]:
            remove(hop, eng, d, [t for t, _ in picked], go, log, ask)


def _copy(hop, pair, db, t, rule, batch, log):
    """One table's picked rows into the archive: new ones written, the
    ones there already left, each batch read back; a row the archive holds
    otherwise refused by key, never overwritten."""
    from . import consolidate, decide
    dst = pair.dst_engine
    there = [] if dst.target_missing(db) else dst.neutral_tables("dst", db)
    tb = _table(pair, db, t, rule, there)
    if not tb.src_cols:
        pair._create_target(db, t, tb.target, log)
        tb = _table(pair, db, t, rule, dst.neutral_tables("dst", db))
    lost = [n for n in tb.notes
            if not n.startswith("columns only on the target")]
    if lost:
        # a column the archive cannot hold, or cannot be compared by, is
        # one a removed row would lose for good
        raise SystemExit(f"{t}: {'; '.join(lost)} - the archive would not"
                         " hold every column of a row it proves, and a row"
                         " removed from the source would lose them. Nothing"
                         " was written")
    wrote, kept, held = 0, 0, []
    for rows in pair.src_engine._every_row("src", db, t, tb.src_cols, rule):
        for i in range(0, len(rows), batch):
            part = rows[i:i + batch]
            mine = _keyed(pair, tb, part)
            theirs = _there(pair, db, tb, part)
            new = [r for k, (h, r) in mine.items() if k not in theirs]
            held += [k for k, (h, _) in mine.items()
                     if k in theirs and theirs[k][0] != h]
            kept += sum(1 for k, (h, _) in mine.items()
                        if k in theirs and theirs[k][0] == h)
            if new:
                def run(choice, new=new):
                    write = (dst.neutral_write if choice.rung.name == "bulk"
                             else dst.neutral_rewrite)
                    write("dst", db, tb.target, tb.dst_cols, new)
                    return len(new)
                decide.attempt(f"{tb.target}: archived rows",
                               consolidate.write_ways(), run, log=log)
                back = _there(pair, db, tb, new)
                wrong = [k for k, (h, _) in _keyed(pair, tb, new).items()
                         if back.get(k, (None,))[0] != h]
                if wrong:
                    raise SystemExit(
                        f"{tb.target}: {len(wrong):,} rows written to the"
                        " archive read back otherwise"
                        f" ({_keys(wrong)}); nothing was removed")
                wrote += len(new)
    if held:
        raise SystemExit(f"{tb.target}: the archive holds {len(held):,} of"
                         " the rows the rule picks otherwise than the source"
                         f" has them ({_keys(held)}) - an archived row is"
                         " never overwritten: find which is right, then"
                         " move again. Nothing was removed")
    log(f"{t}: {wrote:,} rows added to the archive, {kept:,} there"
        " already as the source has them")


def _keys(ks):
    return ", ".join("(" + ", ".join(map(str, k)) + ")"
                     for k in list(ks)[:EXAMPLES])


# --- off the source, and back ------------------------------------------------------

def _offer(hop, db, names, go, ask, log, restoring=False):
    """The source's answer for its archived rows (`sourcechange`)."""
    from . import sourcechange
    d = sourcechange.DEFINITIONS[CHANGE]
    ctx = sourcechange.Ctx(db=db, tables=list(names))
    lines = ["removing the archived rows is a change on the source -"
             " offered, never assumed:" if not restoring else
             "putting the removed rows back is a change on the source:"]
    lines += [f"  on the source: {s}" for s in (
        d.taking_off(ctx) if restoring else d.statements(ctx))]
    lines.append(f"  without it: {d.without}")
    return sourcechange.settle_named(hop, CHANGE, lines, go, ask, log)


def _ledger_path(hop, db):
    return hop.report_dir(db) / "archive-ledger.jsonl"


def ledger(hop, db):
    """The ledger's lines, oldest first."""
    try:
        text = _ledger_path(hop, db).read_text()
    except OSError:
        return []
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _append(hop, db, entry):
    import os
    path = _ledger_path(hop, db)
    with path.open("a") as f:
        f.write(json.dumps(entry, default=str) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _remove_ways():
    """The ways a batch leaves the source: in one statement a batch, or a
    row at a time where that fails as it runs (a lock it waits on too
    long, a statement too large)."""
    from .decide import Rung
    return (Rung("batch", "the batch removed at once",
                 frozenset({"removed"}), because="one statement a batch"),
            Rung("rows", "the batch removed a row at a time",
                 frozenset({"removed"}), footprint=1,
                 because="where the batch at once cannot run"))


def _order(names, edges):
    """Children before the tables they name: a parent's row goes after
    every row naming it."""
    before = {t: {c for c, _, p, _ in edges if p == t and c != t}
              for t in names}
    out, left = [], sorted(names)
    while left:
        ready = [t for t in left if not (before[t] & set(left))]
        nxt = ready[0] if ready else left[0]
        out.append(nxt)
        left.remove(nxt)
    return out


def remove(hop, eng, db, names, go, log, ask=None):
    """The archived rows removed from the source: allowed by the source's
    answer and the hop's approvals, each row read again on both sides
    just before it goes and removed only where the archive holds it as
    the source has it now, its key and digest kept in the ledger first."""
    from . import approvals, decide, sourcechange
    answer = _offer(hop, db, names, go, ask, log)
    if answer != "yes":
        log("the archived rows stay on the source" + (
            f" (answered {answer})" if answer else ""))
        return 0
    approvals.require(hop, sourcechange.STEP, db, log, detail={
        "change": CHANGE, "tables": list(names)})
    pair = _pair(eng)
    src = pair.src_engine
    back = _source_as_target(hop, eng)
    order = _order(list(names), list(src.foreign_keys("src", db)))
    rules = dict(tables(hop, eng, db))
    batch = settings(hop)["batch"]
    there = pair.dst_engine.neutral_tables("dst", db)
    total, stayed = 0, []
    for t in order:
        tb = _table(pair, db, t, rules[t], there)
        for rows in src._every_row("src", db, t, tb.src_cols, tb.rule):
            for i in range(0, len(rows), batch):
                part = rows[i:i + batch]
                mine = _keyed(pair, tb, part)
                theirs = _there(pair, db, tb, part)
                go_ = {k: h for k, (h, _) in mine.items()
                       if theirs.get(k, (None,))[0] == h}
                stayed += [(t, k) for k in mine if k not in go_]
                if not go_:
                    continue
                keys = [dict(zip(tb.key, [r[j] for j in tb.at()]))
                        for k, (h, r) in mine.items() if k in go_]
                entry = {"id": f"{t}-{int(time.time() * 1000)}",
                         "change": CHANGE, "table": t, "db": db,
                         "state": "removing", "rows": len(keys),
                         "keys": [list(k) for k in go_],
                         "digests": list(go_.values()),
                         "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
                _append(hop, db, entry)

                def run(choice, keys=keys):
                    if choice.rung.name == "batch":
                        back._apply_deletes("dst", db, t, keys)
                    else:
                        for k in keys:
                            back._apply_delete("dst", db, t, k)
                    return len(keys)
                decide.attempt(f"{t}: archived rows off the source",
                               _remove_ways(), run, log=log)
                left = src.neutral_rows_by_key(
                    "src", db, t, tb.src_cols, tb.key,
                    [tuple(k[c] for c in tb.key) for k in keys])
                if left:
                    raise SystemExit(f"{t}: {len(left):,} rows are still on"
                                     " the source after their removal"
                                     f" ({_keys(left)}); the ledger has the"
                                     " batch")
                _append(hop, db, {**entry, "state": "removed",
                                  "at": time.strftime("%Y-%m-%dT%H:%M:%S")})
                sourcechange.written(hop, {**entry, "state": "removed",
                                           "keys": None, "digests": None})
                total += len(keys)
    if stayed:
        log(f"{len(stayed):,} rows the rules pick stay on the source: the"
            " archive does not hold them as the source has them now"
            f" ({'; '.join(f'{t} {k}' for t, k in stayed[:EXAMPLES])}) -"
            " the next run archives them as they are")
    log(f"[{db}] {total:,} archived rows removed from the source, each"
        " proved in the archive first")
    return total


def restore(hop, eng, db, go, log, ask=None):
    """Every row the ledger says was removed put back from the archive into
    the source, under the same answer and approvals - the way back."""
    from . import approvals, sourcechange
    removed = {}
    for e in ledger(hop, db):
        if e.get("state") == "removed":
            removed.setdefault(e["table"], []).append(e)
        elif e.get("state") == "restored":
            removed.pop(e["table"], None)
    if not removed:
        log(f"[{db}] nothing removed to put back")
        return 0
    answer = _offer(hop, db, sorted(removed), go, ask, log, restoring=True)
    if not go:
        log(f"[{db}] dry-run: {sum(e['rows'] for v in removed.values() for e in v):,}"
            " rows would be put back")
        return 0
    if answer != "yes":
        log("nothing put back" + (f" (answered {answer})" if answer else ""))
        return 0
    approvals.require(hop, sourcechange.STEP, db, log, detail={
        "change": CHANGE, "restore": sorted(removed)})
    pair = _pair(eng)
    back = _source_as_target(hop, eng)
    rules = dict(tables(hop, eng, db))
    there = pair.dst_engine.neutral_tables("dst", db)
    total = 0
    for t in reversed(_order(list(removed), list(
            pair.src_engine.foreign_keys("src", db)))):
        tb = _table(pair, db, t, rules.get(t), there)
        cls = dict(tb.src_cols)
        from . import canon
        for e in removed[t]:
            keys = [tuple(canon.from_text(cls[c], v)
                          for c, v in zip(tb.key, k)) for k in e["keys"]]
            got = {}
            for i in range(0, len(keys), BATCH):
                got.update(pair.dst_engine.neutral_rows_by_key(
                    "dst", db, tb.target, tb.dst_cols, _dst_key(tb),
                    keys[i:i + BATCH]))
            if len(got) != len(keys):
                raise SystemExit(f"{t}: the archive holds {len(got):,} of"
                                 f" the {len(keys):,} rows to put back")
            rows = list(got.values())
            back.neutral_write("dst", db, t, tb.src_cols, rows)
            _append(hop, db, {**e, "state": "restored",
                              "at": time.strftime("%Y-%m-%dT%H:%M:%S")})
            total += len(rows)
    log(f"[{db}] {total:,} rows put back on the source from the archive")
    return total


# --- the proof ---------------------------------------------------------------------

def prove(hop, eng, db):
    """[Result] of the archive of `db`: every row removed on the archive
    with its digest (the ledger's), and every row the rule still picks on
    the source in the archive as the source has it."""
    from . import canon
    from .engines.base import Result
    pair = _pair(eng)
    dst = pair.dst_engine
    there = [] if dst.target_missing(db) else dst.neutral_tables("dst", db)
    out = []
    gone = {}
    for e in ledger(hop, db):
        if e.get("state") == "removed":
            for k, h in zip(e.get("keys") or [], e.get("digests") or []):
                gone.setdefault(e["table"], {})[tuple(k)] = h
        elif e.get("state") == "restored":
            for k in e.get("keys") or []:
                gone.get(e["table"], {}).pop(tuple(k), None)
    for t, rule in tables(hop, eng, db):
        tb = _table(pair, db, t, rule, there)
        if not tb.src_cols:
            out.append(Result("archive", f"{db}.{t}", "diff",
                              "not in the archive yet", "",
                              f"migkit move {hop.name} --mode full --go"))
            continue
        missing, changed, n = [], [], 0
        for rows in pair.src_engine._every_row("src", db, t, tb.src_cols,
                                               rule):
            mine = _keyed(pair, tb, rows)
            theirs = _there(pair, db, tb, rows)
            for k, (h, _) in mine.items():
                n += 1
                if k not in theirs:
                    missing.append(k)
                elif theirs[k][0] != h:
                    changed.append(k)
        lost, moved = [], []
        removed = gone.get(t, {})
        if removed:
            cls = dict(tb.src_cols)
            keys = list(removed)
            for i in range(0, len(keys), BATCH):
                part = keys[i:i + BATCH]
                got = dst.neutral_rows_by_key(
                    "dst", db, tb.target, tb.dst_cols, _dst_key(tb),
                    [tuple(canon.from_text(cls[c], v)
                           for c, v in zip(tb.key, k)) for k in part])
                for k in part:
                    row = got.get(k)
                    if row is None:
                        lost.append(k)
                    elif _hash(_texts(pair, tb.dst_cols, row)) != removed[k]:
                        moved.append(k)
        said = []
        if missing:
            said.append(f"{len(missing):,} rows the rule picks are not in"
                        f" the archive ({_keys(missing)})")
        if changed:
            said.append(f"{len(changed):,} rows differ from the source's"
                        f" ({_keys(changed)})")
        if lost:
            said.append(f"{len(lost):,} rows removed from the source are"
                        f" gone from the archive ({_keys(lost)})")
        if moved:
            said.append(f"{len(moved):,} rows removed from the source are"
                        " in the archive otherwise than when they were"
                        f" removed ({_keys(moved)})")
        if said:
            out.append(Result("archive", f"{db}.{t}", "diff",
                              "; ".join(said), "",
                              f"migkit move {hop.name} --mode full --go"))
        else:
            out.append(Result("archive", f"{db}.{t}", "ok",
                              f"{n:,} rows the rule picks on the source in"
                              " the archive as they are"
                              + (f"; {len(removed):,} removed from the"
                                 " source, each in the archive with its"
                                 " digest" if removed else "")))
    return out


def check(hop, eng, db):
    """`check` of an archive hop: the proof."""
    return prove(hop, eng, db)


def plan_lines(hop, eng, db):
    """The plan's line of an archive, or []."""
    got = settings(hop)
    if got is None or not db:
        return []
    try:
        picked = tables(hop, eng, db)
    except Exception as e:  # noqa: BLE001 - said, the plan goes on
        return [f"  an archive: its tables could not be read: {_last(e)}"]
    return [f"  an archive: {len(picked)} tables by their rules into the"
            " archive, proved there"
            + ("; then removed from the source, row by row once proved,"
               " where the source's answer and the hop's approvals allow"
               if got["remove"] else "; the source keeps its rows")
            + ("; the removed rows put back from the archive"
               if got["restore"] else "")]
