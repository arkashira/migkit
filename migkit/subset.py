"""Copies for testing (backlog R23, 5): the rows chosen and every row they
refer to, masked where the hop says, into a staging or developer database,
for any pair of engines migkit copies between, proved row by row.

    hops:
      orders-dev:
        goal: subset
        source: postgres://app@db.example.com/orders
        target: postgres://dev@dev-db.example.com/orders
        options:
          subset:
            start:
              orders: "created_at >= '2026-09-01'"   # a condition, in the source's SQL
              customers: 10%          # a share of the rows, the same ones every run
              products: 500           # the first rows by key
            children: false           # the rows that refer to those taken come too
            most: 1000000             # refused past this many rows
          mask_on_write:
            customers.email: constant masked@example.com
            customers.phone: null

**Complete by construction.** From the rows chosen every foreign key is
followed to the row it names, and from there on, until nothing new
arrives: every reference in the copy resolves inside the copy. With
`children: true` the rows that refer to a row taken come too, and their
own references are followed in turn. A reference that cannot be followed
- to a table the hop leaves out, through a column its mapping drops -
stops the plan before anything is written, named.

**Repeatable.** A share is taken by a hash of each row's key, not by
chance: the same rule over the same source takes the same rows every run,
so a test that failed on one copy fails on the next.

**Masked on the way.** The hop's `mask_on_write` (null, constant) is
applied to the rows before they are written, on any pair - never on a
key or a column a reference runs through (the copy would no longer line
up) and never as a random value (nothing could be proved of it); the rule
is held as the parallel dump holds it (`movers.mask_plan`).

**Proved.** Each table's keys are kept (`subset-keys/`), and after the
write every row is read back from the target by them and compared, value
for value, with the source's as it was read - its masked columns with what
the rule writes - the target holding none but them, and every reference
in the copy is proved to resolve there. `check` asks the same of the
source as it is now: a copy for testing is as of its copy, and a source
that moved on since is said, not hidden.

The target database holds the subset and nothing else: every table in the
hop's scope is emptied before the subset's rows are written, under the
interlock's word on rows it did not write (`target_rows`).
"""
import hashlib
import json
import time
from collections import deque
from dataclasses import dataclass, field

GOAL = "subset"
#: rows a subset holds at most where the hop does not say
MOST = 1_000_000
#: rows read, or values asked for, at once
BATCH = 1000
#: the share rule's buckets: a key hashing below share x SHARE_OF is taken
SHARE_OF = 1_000_000


def settings(hop):
    """{"start": {table: rule}, "children", "most"} for a hop whose goal is
    `subset`; None for any other."""
    if (getattr(hop, "goal", "") or "") != GOAL:
        return None
    raw = (hop.options or {}).get("subset") or {}
    if not isinstance(raw, dict):
        raw = {}
    start = raw.get("start") or {}
    try:
        most = int(raw.get("most") or MOST)
    except (TypeError, ValueError):
        raise SystemExit(f"subset: most is a number of rows, not"
                         f" {raw.get('most')!r}")
    return {"start": dict(start) if isinstance(start, dict) else {},
            "children": bool(raw.get("children")), "most": most}


@dataclass(frozen=True)
class Rule:
    """How the rows of one table are chosen: `where` (a condition in the
    source's SQL), `share` (a percentage), `rows` (the first by key) or
    `all`."""
    table: str
    kind: str
    value: object = None

    def __str__(self):
        return {"where": f"where {self.value}",
                "share": f"{self.value:g}% of its rows by their keys",
                "rows": f"its first {self.value:,} rows by key",
                "all": "every row"}[self.kind]


def rule(table, said):
    """The `Rule` a hop's `start` entry says, or SystemExit."""
    if said is True or str(said).strip().lower() in ("all", "true", "*"):
        return Rule(table, "all")
    if isinstance(said, int) and not isinstance(said, bool):
        if said <= 0:
            raise SystemExit(f"subset: {table}: {said} rows - more than 0")
        return Rule(table, "rows", said)
    text = str(said).strip()
    if text.endswith("%"):
        try:
            share = float(text[:-1])
        except ValueError:
            share = -1.0
        if not 0 < share <= 100:
            raise SystemExit(f"subset: {table}: {text} - a share is more than"
                             " 0% and no more than 100%")
        return Rule(table, "share", share)
    if text.isdigit():
        return rule(table, int(text))
    if not text:
        raise SystemExit(f"subset: {table}: no rule - a condition, a share"
                         " like 10% or a number of rows")
    return Rule(table, "where", text)


def _pair(eng):
    """The hop as a pair of engines: the copier written for any pair reads
    and writes a same-engine hop too (`Engine._as_pair`)."""
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


def _named(table, tables):
    """The source's table a rule's name stands for, by the suffix rule
    every name in a hop is matched by; None where none, or more than one,
    does."""
    table = str(table)
    got = [t for t in tables if str(t) == table
           or str(t).endswith("." + table)]
    return got[0] if len(got) == 1 else None


# --- the plan ------------------------------------------------------------------

@dataclass
class Plan:
    """What one database's subset holds: per table, its columns as read,
    its key, and its rows by the canonical text of their key (a table with
    no key by the text of the whole row); how each table came in."""
    db: str
    columns: dict = field(default_factory=dict)
    keys: dict = field(default_factory=dict)
    rows: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)
    why: dict = field(default_factory=dict)

    def total(self):
        return sum(len(r) for r in self.rows.values())


def _classes(src, db, table):
    from . import canon
    return [(n, canon.type_class(src.CANON_ENGINE, typ))
            for n, typ in src.neutral_columns("src", db, table)]


def _ident(plan, table, row):
    """The canonical text a row is known by in the plan: its key's, or the
    whole row's where the table has no key."""
    from . import canon
    cols = plan.columns[table]
    names = [n for n, _ in cols]
    key = plan.keys[table] or names
    cls = dict(cols)
    return tuple(canon.render_value(cls[k], row[names.index(k)])
                 for k in key)


def _values(plan, table, row, columns):
    """The canonical text of `columns` of a row, or None where one is NULL -
    a reference through a NULL names nothing."""
    from . import canon
    cols = plan.columns[table]
    names = [n for n, _ in cols]
    cls = dict(cols)
    out = []
    for c in columns:
        v = row[names.index(c)]
        if v is None:
            return None
        out.append(canon.render_value(cls[c], v))
    return tuple(out)


def _raw(plan, table, row, columns):
    names = [n for n, _ in plan.columns[table]]
    return tuple(row[names.index(c)] for c in columns)


def _scan(src, db, table, cols, where=None, stop=None):
    """The rows of `table` in key order, `BATCH` a read; `stop` rows at
    most."""
    got, after = 0, None
    while True:
        rows, last = src.neutral_read("src", db, table, cols, after, BATCH,
                                      **({"where": where} if where else {}))
        for r in rows:
            yield r
            got += 1
            if stop is not None and got >= stop:
                return
        if last is None or not rows:
            return
        after = last


def _bucket(ident):
    digest = hashlib.sha256("\x1f".join(
        "\x00" if v is None else str(v) for v in ident).encode()).digest()
    return int.from_bytes(digest[:8], "big") % SHARE_OF


def _add(plan, table, rows, most):
    """The rows not in the plan yet, added; refused past `most`."""
    have = plan.rows.setdefault(table, {})
    new = []
    for r in rows:
        k = _ident(plan, table, r)
        if k not in have:
            have[k] = list(r)
            new.append(list(r))
    if plan.total() > most:
        raise SystemExit(
            f"{plan.db}: the subset would hold more than {most:,} rows"
            f" ({table} alone {len(have):,}), and the hop allows {most:,}"
            " (subset: most). Choose fewer rows to start from, or raise"
            " the bound; nothing was written")
    return new


def outline(hop, eng, db):
    """(the `Plan` of `db`'s subset without its rows - the tables it may
    reach, their columns and keys, the references between them - and the
    rules it starts from), reading the catalogue alone; SystemExit with
    every reason a rule or a reference cannot be followed."""
    got = settings(hop)
    if got is None:
        raise SystemExit(f"{hop.name}: its goal is not subset")
    pair = _pair(eng)
    src = pair.src_engine
    tables = [t for t in src.neutral_tables("src", db)
              if not hop.excluded(db, *str(t).split("."))]
    rules, wrong = [], []
    for name, said in sorted(got["start"].items()):
        t = _named(name, tables)
        if t is None:
            wrong.append(f"{name}: no such table on the source in {db}, or"
                         " more than one of that name")
            continue
        rules.append(rule(t, said))
    everything = list(src.neutral_tables("src", db))
    edges = []
    for child, ccols, parent, pcols in src.foreign_keys("src", db):
        if child not in tables:
            continue
        if parent not in tables:
            wrong.append(f"{child} refers to {parent}"
                         + (", which the hop leaves out" if parent in
                            everything else ", which is not in the copy")
                         + ": the copy could not be complete")
            continue
        edges.append((child, list(ccols), parent, list(pcols)))
    if wrong:
        raise SystemExit(f"{db}: the subset cannot be taken, nothing was"
                         " written: " + "; ".join(wrong))
    p = Plan(db, edges=edges)
    need = {r.table for r in rules} | {e[0] for e in edges} | \
        {e[2] for e in edges}
    for t in sorted(need):
        p.columns[t] = _classes(src, db, t)
        p.keys[t] = list(src.neutral_key("src", db, t) or [])
        names = {n for n, _ in p.columns[t]}
        for c, cc, pt, pc in edges:
            for col in (cc if c == t else pc if pt == t else ()):
                if col not in names:
                    raise SystemExit(f"{db}: {c} refers to {pt} through"
                                     f" {col}, which cannot be read on the"
                                     " source; nothing was written")
    return p, rules


def problems(hop, eng, db):
    """[what stops this subset before a row is read]: a rule or a reference
    that cannot be followed, a masking rule that cannot be applied as
    asked - what `ready` says before anything runs."""
    try:
        p, _ = outline(hop, eng, db)
        masks(hop, _pair(eng), db, p)
    except SystemExit as e:
        return [str(e)]
    return []


def plan(hop, eng, db, log=None):
    """The `Plan` of `db`'s subset: the rows each rule chooses, then the
    closure over the foreign keys. Read only; refused, with every reason,
    where a rule or a reference cannot be followed."""
    got = settings(hop)
    say = log or (lambda _: None)
    p, rules = outline(hop, eng, db)
    src = _pair(eng).src_engine
    edges = p.edges
    most = got["most"]
    queue = deque()
    for r in rules:
        cols, key = p.columns[r.table], p.keys[r.table]
        if r.kind == "where":
            rows = list(_scan(src, db, r.table, cols, where=r.value))
        elif r.kind == "rows":
            rows = list(_scan(src, db, r.table, cols, stop=r.value))
        elif r.kind == "all":
            rows = list(_scan(src, db, r.table, cols))
        else:
            cut = r.value / 100.0 * SHARE_OF
            rows = [row for row in _scan(src, db, r.table, cols)
                    if _bucket(_ident(p, r.table, row)) < cut]
        new = _add(p, r.table, rows, most)
        p.why.setdefault(r.table, []).append(str(r))
        say(f"{r.table}: {len(new):,} rows chosen - {r}"
            + ("" if key else " (no key: rows are told apart by all their"
               " values)"))
        if new:
            queue.append((r.table, new))
    asked = {}
    while queue:
        t, new = queue.popleft()
        for c, cc, pt, pc in edges:
            if c == t:
                # each reference to the row it names
                want = {}
                for row in new:
                    v = _values(p, t, row, cc)
                    if v is not None:
                        want.setdefault(v, _raw(p, t, row, cc))
                seen = asked.setdefault((pt, tuple(pc)), set())
                ask = [raw for v, raw in want.items() if v not in seen]
                seen.update(want)
                got_rows = []
                for i in range(0, len(ask), BATCH):
                    got_rows += list(src.neutral_rows_by_key(
                        "src", db, pt, p.columns[pt], pc,
                        ask[i:i + BATCH]).values())
                added = _add(p, pt, got_rows, most)
                if added:
                    p.why.setdefault(pt, []).append(f"referred to by {c}")
                    queue.append((pt, added))
            if pt == t and got["children"]:
                # the rows that refer to it
                want = {}
                for row in new:
                    v = _values(p, t, row, pc)
                    if v is not None:
                        want.setdefault(v, _raw(p, t, row, pc))
                seen = asked.setdefault(("children", c, tuple(cc)), set())
                ask = [raw for v, raw in want.items() if v not in seen]
                seen.update(want)
                got_rows = src.neutral_rows_matching(
                    "src", db, c, p.columns[c], cc, ask) if ask else []
                added = _add(p, c, got_rows, most)
                if added:
                    p.why.setdefault(c, []).append(f"refers to {pt}")
                    queue.append((c, added))
    for t in sorted(p.rows):
        said = "; ".join(dict.fromkeys(p.why.get(t) or []))
        say(f"{t}: {len(p.rows[t]):,} rows in the subset ({said})")
    return p


# --- masking -------------------------------------------------------------------

def masks(hop, pair, db, plan_):
    """{table: {column: (kind, value)}} of the hop's `mask_on_write` for the
    subset's tables, held to the rules the parallel dump holds it to
    (`movers.mask_plan`): SystemExit naming every rule that cannot be
    applied as asked."""
    from . import movers
    rules = movers.mask_rules(hop, db)
    if not rules:
        return {}
    keys, columns = {}, {}
    leaf = {str(t).split(".")[-1]: t for t in plan_.columns}
    src = pair.src_engine
    tables = src.neutral_tables("src", db)
    for name in rules:
        # a rule's table the subset does not take is asked of the source
        # all the same: a rule naming a column it lacks hides nothing
        t = leaf.get(name) or _named(name, tables)
        if t is None:
            continue
        cols = plan_.columns.get(t) or _classes(src, db, t)
        columns[name] = {n for n, _ in cols}
        keys[name] = set(plan_.keys.get(t) or src.neutral_key("src", db, t)
                         or [])
    for c, cc, pt, pc in plan_.edges:
        keys.setdefault(str(c).split(".")[-1], set()).update(cc)
        keys.setdefault(str(pt).split(".")[-1], set()).update(pc)
    _, refused = movers.mask_plan(hop, db, keys, columns)
    if refused:
        raise SystemExit(f"{db}: nothing was written - "
                         + "; ".join(f"{n}: {why}" for n, why in refused))
    return {leaf[t]: cols for t, cols in rules.items() if t in leaf}


def _masked(row, names, rules):
    if not rules:
        return row
    out = list(row)
    for col, (kind, value) in rules.items():
        if col in names:
            out[names.index(col)] = None if kind == "null" else value
    return out


# --- the move ------------------------------------------------------------------

def _order(tables, edges):
    """Parents before the tables that refer to them; a cycle in the order
    it is met."""
    after = {t: {pt for c, _, pt, _ in edges if c == t and pt != t}
             for t in tables}
    out, left = [], list(sorted(tables))
    while left:
        ready = [t for t in left if not (after[t] & set(left))]
        nxt = ready[0] if ready else left[0]
        out.append(nxt)
        left.remove(nxt)
    return out


def _as_tuple(t):
    t = str(t)
    return tuple(t.split(".", 1)) if "." in t else ("", t)


def _targets(pair, db):
    """{source table: its name on the target}, by the hop's renames."""
    pairs, _, _, _ = pair.match_tables(
        pair.src_engine.neutral_tables("src", db),
        pair.dst_engine.neutral_tables("dst", db), pair._rename)
    return dict(pairs)


def move(hop, eng, dbs, mode, table, go, log):
    """The subset of each database into the target, proved; a plan without
    `go`."""
    from . import interlock
    if mode != "full":
        raise SystemExit(f"{hop.name}: a copy for testing is taken once"
                         " (--mode full) and not followed - its rows are"
                         " masked and chosen, and a change of the source's"
                         " would carry neither. Nothing has been started")
    if table:
        raise SystemExit(f"{hop.name}: a subset is taken whole - its tables"
                         " refer to each other, and one of them alone would"
                         " not be complete. Leave out --table")
    pair = _pair(eng)
    for d in dbs:
        log(f"[{d}] the subset: the rows chosen, then every row they refer"
            " to")
        p = plan(hop, eng, d, log)
        rules = masks(hop, pair, d, p)
        for t, cols in rules.items():
            log(f"{t}: masked on the way - " + ", ".join(
                f"{c} {'null' if k == 'null' else 'a constant'}"
                for c, (k, _) in sorted(cols.items())))
        if not go:
            log(f"[{d}] dry-run: {p.total():,} rows in"
                f" {len(p.rows)} tables would be written; the target's other"
                " tables in scope emptied")
            continue
        scope = eng.list_move_tables(d)
        kept = interlock.before_copy(hop, eng, d, scope, go, log)
        eng.create_missing(d, log)
        targets = _targets(pair, d)
        written = {}
        leaves = {str(t[-1]) for t in scope}
        order = _order(list(targets), p.edges)
        missing = [t for t in p.rows if t not in targets]
        if missing:
            raise SystemExit(f"{d}: {', '.join(missing)} has no table on the"
                             " target to write into; nothing was written")
        # the window of the engine that writes: the pair's own target
        with pair.load_window(d, log, leaves):
            # emptied first, what refers to others before what it refers
            # to, then written the other way round: no reference on the
            # target is left pointing at a row that is not there yet
            for s in reversed(order):
                if _as_tuple(s) in kept or hop.excluded(d, *str(s).split(
                        ".")):
                    continue
                gone = pair.dst_engine.neutral_empty("dst", d, targets[s])
                if gone and s not in p.rows:
                    log(f"{s}: emptied ({gone:,} rows) - not in the subset")
            for t in order:
                if t not in p.rows:
                    continue
                if _as_tuple(t) in kept:
                    log(f"{t}: kept as the target holds it (target_rows:"
                        " keep); its rows are not written")
                    continue
                written[t] = _write(pair, d, t, targets[t], p,
                                    rules.get(t) or {}, log)
        eng.finish_created(d, log)
        _keep(hop, d, p, rules, targets, kept)
        results = prove(hop, eng, d, p)
        for r in results:
            log(f"{r.scope}: {r.status.upper()} {r.detail}")
        bad = [r for r in results if r.status not in ("ok", "skip")]
        if bad:
            raise SystemExit(f"{d}: the copy is not what it was to hold - "
                             + "; ".join(f"{r.scope}: {r.detail}"
                                         for r in bad[:4]))
        log(f"[{d}] {sum(written.values()):,} rows written and proved")


def _write(pair, db, src_t, dst_t, p, rules, log):
    """One table's rows, into the table emptied before, written as the
    target's columns take them; how many."""
    src_cols, dst_cols, notes = pair._move_columns(src_t, dst_t, db)
    for n in notes:
        log(f"{src_t}: {n}")
    names = [n for n, _ in p.columns[src_t]]
    at = [names.index(n) for n, _ in src_cols]
    rows = [_masked(r, names, rules) for r in p.rows[src_t].values()]
    rows = [[r[i] for i in at] for r in rows]
    done = 0
    for i in range(0, len(rows), BATCH * 5):
        done += pair.dst_engine.neutral_write("dst", db, dst_t, dst_cols,
                                              rows[i:i + BATCH * 5]) or 0
    log(f"{src_t}: {len(rows):,} rows written")
    return len(rows)


# --- what is kept, and the proof ----------------------------------------------

def _dir(hop, db):
    d = hop.report_dir(db) / "subset-keys"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _file(table):
    return "".join(ch if ch.isalnum() or ch in "._-" else "_"
                   for ch in str(table)) + ".jsonl"


def keep_value(v):
    """A value as JSON holds it and gives it back as it was."""
    import datetime
    import decimal
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return {"f": repr(v)}
    if isinstance(v, decimal.Decimal):
        return {"d": str(v)}
    if isinstance(v, datetime.datetime):
        return {"t": v.isoformat()}
    if isinstance(v, datetime.date):
        return {"date": v.isoformat()}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"b": bytes(v).hex()}
    return {"s": str(v)}


def back_value(v):
    import datetime
    import decimal
    if not isinstance(v, dict):
        return v
    if "f" in v:
        return float(v["f"])
    if "d" in v:
        return decimal.Decimal(v["d"])
    if "t" in v:
        return datetime.datetime.fromisoformat(v["t"])
    if "date" in v:
        return datetime.date.fromisoformat(v["date"])
    if "b" in v:
        return bytes.fromhex(v["b"])
    return v.get("s")


def _keep(hop, db, p, rules, targets, kept):
    """Each table's keys (a keyless table's rows) kept for the proof, and
    what the subset is, in `subset.json`."""
    where = _dir(hop, db)
    for f in where.glob("*.jsonl"):
        f.unlink()
    rec = {"at": time.strftime("%Y-%m-%dT%H:%M:%S"), "tables": {},
           "edges": p.edges}
    for t, rows in p.rows.items():
        if _as_tuple(t) in kept:
            continue
        key = p.keys[t]
        names = [n for n, _ in p.columns[t]]
        with (where / _file(t)).open("w") as f:
            for r in rows.values():
                got = ([r[names.index(k)] for k in key] if key else
                       _masked(r, names, rules.get(t) or {}))
                f.write(json.dumps([keep_value(v) for v in got]) + "\n")
        rec["tables"][t] = {"rows": len(rows), "key": key,
                            "columns": names,
                            "target": targets.get(t), "file": _file(t),
                            "masked": {c: list(m) for c, m in
                                       (rules.get(t) or {}).items()},
                            "why": list(dict.fromkeys(p.why.get(t) or []))}
    (hop.report_dir(db) / "subset.json").write_text(
        json.dumps(rec, indent=1, default=str))


def _record(hop, db):
    try:
        return json.loads((hop.report_dir(db) / "subset.json").read_text())
    except (OSError, ValueError):
        return None


def _kept_rows(hop, db, entry):
    try:
        lines = (_dir(hop, db) / entry["file"]).read_text().splitlines()
    except OSError:
        return []
    return [[back_value(v) for v in json.loads(line)] for line in lines
            if line.strip()]


def _count(eng, side, db, table, cols):
    n, after = 0, None
    while True:
        rows, last = eng.neutral_read(side, db, table, cols, after, 5000)
        n += len(rows)
        if last is None or not rows:
            return n
        after = last


def prove(hop, eng, db, p=None):
    """[Result] of the subset of `db` on the target: each table holds its
    rows and none but them, each row as the source's (`p`, what the copy
    read; the source as it is now without it), its masked columns as the
    rule writes them - and every reference resolves in the copy."""
    from .engines.base import Result
    rec = _record(hop, db)
    if rec is None:
        return [Result("subset", db, "error",
                       "no subset of this database has been copied yet:"
                       f" migkit move {hop.name} --mode full --go")]
    pair = _pair(eng)
    src, dst = pair.src_engine, pair.dst_engine
    out, held = [], {}
    for t, entry in sorted(rec["tables"].items()):
        dst_t = entry.get("target")
        key = entry.get("key") or []
        masked = {c: tuple(m) for c, m in (entry.get("masked") or {}).items()}
        try:
            sc, dc, _ = pair._comparable_columns(db, src, t, dst, dst_t)
        except Exception as e:  # noqa: BLE001 - said as the table's error
            out.append(Result("subset", f"{db}.{t}", "error",
                              f"could not be read: {_last(e)}"))
            continue
        names = [n for n, _ in sc]
        to_dst = dict(zip(names, [n for n, _ in dc]))
        kept = _kept_rows(hop, db, entry)
        if key and not set(key) <= set(names):
            out.append(Result("subset", f"{db}.{t}", "error",
                              "its key is not among the columns both sides"
                              " render, so its rows cannot be proved"))
            continue
        if key:
            if p is not None:
                cols = [n for n, _ in p.columns[t]]
                a = {}
                for r in p.rows[t].values():
                    r = _masked(r, cols, masked)
                    row = [r[cols.index(n)] for n in names]
                    a[pair._key_of(sc, key, row)] = row
            else:
                a = {}
                for i in range(0, len(kept), BATCH):
                    a.update(src.neutral_rows_by_key(
                        "src", db, t, sc, key, kept[i:i + BATCH]))
                a = {k: _masked(r, names, masked) for k, r in a.items()}
            b = {}
            for i in range(0, len(kept), BATCH):
                b.update(dst.neutral_rows_by_key(
                    "dst", db, dst_t, dc, [to_dst[k] for k in key],
                    kept[i:i + BATCH]))
            missing, extra, changed = pair._rows_differ(sc, dc, a, b)
            gone = len(kept) - len(a)
            held[t] = list(b.values())
        else:
            # no key to read the source by: held to the rows the copy read,
            # as kept - masked as they were written
            cols = entry.get("columns") or []
            want = _texts(sc, [[r[cols.index(n)] if n in cols else None
                                for n in names] for r in kept])
            got = list(_scan_all(dst, db, dst_t, dc))
            have = _texts(dc, got)
            missing = list((want - have).elements())
            extra = list((have - want).elements())
            changed, gone = [], 0
            held[t] = got
        there = _count(dst, "dst", db, dst_t, dc)
        problems = []
        if missing:
            problems.append(f"{len(missing):,} rows missing on the target")
        if changed:
            problems.append(f"{len(changed):,} rows differ from the"
                            " source's" + (" as it is now" if p is None
                                           else ""))
        if extra:
            problems.append(f"{len(extra):,} rows the subset does not hold")
        if gone:
            problems.append(f"{gone:,} of its rows are gone from the source"
                            " since the copy")
        if there != len(kept):
            problems.append(f"the target holds {there:,} rows and the"
                            f" subset {len(kept):,}")
        if problems:
            out.append(Result(
                "subset", f"{db}.{t}", "diff", "; ".join(problems)
                + (" - a copy for testing is as of its copy: move again to"
                   " take it afresh" if p is None else ""), "",
                f"migkit move {hop.name} --mode full --go"))
        else:
            out.append(Result(
                "subset", f"{db}.{t}", "ok",
                f"{len(kept):,} rows, each as the source's"
                + (f", {', '.join(sorted(masked))} masked as the hop says"
                   if masked else "")))
    out.append(_references(pair, db, rec, held))
    return out


def _scan_all(eng, db, table, cols):
    after = None
    while True:
        rows, last = eng.neutral_read("dst", db, table, cols, after, 5000)
        yield from rows
        if last is None or not rows:
            return
        after = last


def _texts(cols, rows):
    from collections import Counter

    from . import canon
    return Counter(tuple(canon.render_value(c, v)
                         for (_, c), v in zip(cols, r)) for r in rows)


def _references(pair, db, rec, held):
    """One Result: every reference among the subset's rows on the target
    resolves to a row of the subset."""
    from . import canon
    from .engines.base import Result
    src, dst = pair.src_engine, pair.dst_engine
    loose, asked = [], 0
    for c, cc, pt, pc in rec.get("edges") or []:
        if c not in held or pt not in held:
            continue
        ce, pe = rec["tables"][c], rec["tables"][pt]
        try:
            csc, cdc, _ = pair._comparable_columns(db, src, c, dst,
                                                   ce["target"])
            psc, pdc, _ = pair._comparable_columns(db, src, pt, dst,
                                                   pe["target"])
        except Exception:  # noqa: BLE001 - that reference not asked
            continue
        cnames, pnames = [n for n, _ in csc], [n for n, _ in psc]
        if not (set(cc) <= set(cnames) and set(pc) <= set(pnames)):
            continue
        crows, prow = held[c], held[pt]
        ccls, pcls = dict(cdc), dict(pdc)
        cmap = dict(zip(cnames, [n for n, _ in cdc]))
        pmap = dict(zip(pnames, [n for n, _ in pdc]))
        have = set()
        for r in prow:
            have.add(tuple(canon.render_value(pcls[pmap[k]],
                                              r[pnames.index(k)])
                           for k in pc))
        for r in crows:
            v = [r[cnames.index(k)] for k in cc]
            if any(x is None for x in v):
                continue
            asked += 1
            got = tuple(canon.render_value(ccls[cmap[k]], x)
                        for k, x in zip(cc, v))
            if got not in have:
                loose.append(f"{c} ({', '.join(cc)}) = {', '.join(got)}"
                             f" names no row of {pt}")
    if loose:
        return Result("subset", f"{db} references", "diff",
                      f"{len(loose):,} references in the copy name no row"
                      f" of it: {'; '.join(loose[:4])}")
    return Result("subset", f"{db} references", "ok",
                  f"all {asked:,} references among the subset's rows"
                  " resolve inside it")


def check(hop, eng, db):
    """`check` of a subset hop: the proof, against the source as it is
    now."""
    return prove(hop, eng, db)


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:120]
