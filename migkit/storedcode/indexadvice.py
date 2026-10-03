"""Indexes for the target, measured on the target's own planner before
they are advised (leapfrog research P15, backlog CV.P15).

The workload comparison advises the index the source reads a table by,
where the target reads that table whole (`workload.advise`). That is the
source's design, not the translated workload's: a statement the
translator rewrote may want another. With the hop option `index_advice`,
each translated statement the target planned proposes the indexes it
could use - its equality columns (a filter or a join), then one column
it ranges over or orders by, as an index adviser proposes them - and each
candidate is made on the target hypothetically: the hypothetical-index
extension gives the planner an index that exists for this session only,
written nowhere (no catalogue, no disk, no lock). Each statement is then
planned again, and a candidate is advised only where the planner chooses
it and its estimated cost of a statement falls to half or less; the
benefit is those falls weighed by how often each statement runs, the
largest few are advised, each with the planner's costs before and after,
the statements it serves and the index's estimated size, and the writes
on record to its table (an index each of them pays for). Nothing is
created on the target: the statements go to `for-the-target.sql` with
the rest of the advice.

The target must run the extension already - migkit installs nothing on a
hop's server - and be PostgreSQL; elsewhere nothing more than the
source's own index is advised, and it is said why. Partitioning is not
advised: no released version of the extension plans a partitioning that
is not there, and a partitioning advised without its plan measured would
be a guess.
"""
import re

from sqlglot import exp

#: the planner's estimate must fall to this share of what it was
FALL = 0.5
#: indexes advised at most, and candidates measured at most
MOST = 5
CANDIDATES = 40
#: columns in one candidate, at most
WIDEST = 3
#: the extension's functions this module calls
_MAKE, _DROP, _SIZE, _RESET = ("hypopg_create_index", "hypopg_drop_index",
                               "hypopg_relation_size", "hypopg_reset")


def turned_on(hop):
    return bool((hop.options or {}).get("index_advice"))


def _tables(tree):
    return {str(t.alias_or_name).lower(): t.name for t in
            tree.find_all(exp.Table)}


def candidates(sql):
    """[(table, (column, ...))] a statement could use, in the target's
    SQL: its equality columns per table, then each one range or order
    column after them; each single equality column on its own too."""
    from .translate import Refused, parse
    try:
        tree = parse(sql, "postgres")
    except Refused:
        return []
    tables = _tables(tree)
    only = list(dict.fromkeys(tables.values()))

    def owner(col):
        if col.table:
            return tables.get(str(col.table).lower())
        return only[0] if len(only) == 1 else None
    eq, rng = {}, {}
    for node in tree.find_all(exp.EQ, exp.In, exp.GT, exp.GTE, exp.LT,
                              exp.LTE, exp.Between, exp.Like):
        cols = [c for c in (node.this, node.args.get("expression"))
                if isinstance(c, exp.Column)]
        for col in cols:
            t = owner(col)
            if t is None:
                continue
            into = eq if isinstance(node, (exp.EQ, exp.In)) else rng
            if isinstance(node, exp.Like):
                pattern = node.expression
                if not (isinstance(pattern, exp.Literal) and
                        not str(pattern.this).startswith(("%", "_"))):
                    continue
            into.setdefault(t, [])
            if col.name not in into[t]:
                into[t].append(col.name)
    for o in tree.find_all(exp.Ordered):
        col = o.this
        if isinstance(col, exp.Column) and owner(col):
            rng.setdefault(owner(col), [])
            if col.name not in rng[owner(col)]:
                rng[owner(col)].append(col.name)
    out = []
    for t in dict.fromkeys(list(eq) + list(rng)):
        e = eq.get(t, [])[:WIDEST]
        for c in e:
            out.append((t, (c,)))
        if len(e) > 1:
            out.append((t, tuple(e)))
        for r in rng.get(t, [])[:2]:
            if r not in e and len(e) < WIDEST:
                out.append((t, tuple(e) + (r,)))
    return list(dict.fromkeys(out))


def _cost(cur, sql):
    """(the planner's total cost, [the indexes its plan uses])."""
    import json
    generic = ", generic_plan" if re.search(r"\$\d", sql) else ""
    cur.execute(f"explain (format json{generic}) {sql}")
    got = cur.fetchone()[0]
    plan = (got if isinstance(got, list) else json.loads(got))[0]["Plan"]
    used = []

    def walk(node):
        if node.get("Index Name"):
            used.append(node["Index Name"])
        for child in node.get("Plans") or []:
            walk(child)
    walk(plan)
    return float(plan["Total Cost"]), used


def _q(name):
    return '"' + str(name).replace('"', '""') + '"'


def advise(dst, db, statements):
    """([the statement making each index advised], [what is said of each,
    or why nothing is]) for `statements` [(the target's SQL, how often it
    runs, "read" or "write")] planned on the target."""
    if getattr(dst, "CANON_ENGINE", "") != "postgres":
        return [], []
    conn = dst._conn("dst", dst._d("dst", db))
    try:
        conn.set_session(readonly=True)
        cur = conn.cursor()
        cur.execute("set statement_timeout = 5000")
        cur.execute("select 1 from pg_extension where extname = 'hypopg'")
        if cur.fetchone() is None:
            return [], ["no other index is measured: the target runs no"
                        " hypothetical-index extension, and migkit installs"
                        " nothing on a hop's server"]
        base, cands = {}, {}
        for k, (sql, calls, kind) in enumerate(statements):
            if kind != "read":
                continue
            try:
                base[k] = _cost(cur, sql)[0]
            except Exception:  # noqa: BLE001 - not planned, not measured
                conn.rollback()
                cur = conn.cursor()
                continue
            for c in candidates(sql):
                cands.setdefault(c, []).append(k)
        writes = {}
        for sql, calls, kind in statements:
            if kind == "write":
                for t in re.findall(r"(?i)\b(?:into|update|from)\s+"
                                    r"(\"[^\"]+\"|[\w.]+)", sql)[:1]:
                    leaf = t.strip('"').split(".")[-1].lower()
                    writes[leaf] = writes.get(leaf, 0) + int(calls or 1)
        found = []
        for (table, cols), served in list(cands.items())[:CANDIDATES]:
            made = f"create index on {_q_table(table)} (" \
                + ", ".join(_q(c) for c in cols) + ")"
            try:
                cur.execute(f"select indexrelid, indexname from {_MAKE}(%s)",
                            (made,))
                oid, name = cur.fetchone()
                cur.execute(f"select {_SIZE}(%s)", (oid,))
                size = int(cur.fetchone()[0] or 0)
            except Exception:  # noqa: BLE001 - a candidate it cannot make
                conn.rollback()
                cur = conn.cursor()
                continue
            gain, fell = 0.0, []
            for k in served:
                sql, calls, _ = statements[k]
                try:
                    after, used = _cost(cur, sql)
                except Exception:  # noqa: BLE001 - not planned with it
                    conn.rollback()
                    cur = conn.cursor()
                    continue
                if name in used and after <= FALL * base[k]:
                    gain += (base[k] - after) * int(calls or 1)
                    fell.append((base[k], after, int(calls or 1)))
            try:
                cur.execute(f"select {_DROP}(%s)", (oid,))
            except Exception:  # noqa: BLE001 - reset below all the same
                conn.rollback()
                cur = conn.cursor()
            if fell:
                found.append((gain, table, cols, fell, size))
        try:
            cur.execute(f"select {_RESET}()")
        except Exception:  # noqa: BLE001 - the session ends with it
            pass
    finally:
        conn.rollback()
        conn.close()
    found.sort(key=lambda x: -x[0])
    made, said = [], []
    for gain, table, cols, fell, size in found[:MOST]:
        name = f"{table.split('.')[-1]}_{'_'.join(cols)}_idx"[:63]
        made.append(f"create index {_q(name)} on {_q_table(table)} ("
                    + ", ".join(_q(c) for c in cols) + ")")
        before = sum(b for b, _, _ in fell)
        after = sum(a for _, a, _ in fell)
        runs = sum(n for _, _, n in fell)
        w = writes.get(table.split(".")[-1].lower(), 0)
        said.append(f"{table} ({', '.join(cols)}): the target's planner"
                    f" estimates {before:,.0f} before and {after:,.0f} with"
                    f" it for {len(fell)} statements run {runs:,} times"
                    f" (about {size / 1e6:,.1f} MB"
                    + (f"; {w:,} writes on record to the table pay for it"
                       if w else "") + ")")
    return made, said


def _q_table(table):
    return ".".join(_q(p) for p in str(table).split("."))


#: the hypothetical-index extension's functions, as its release documents
#: them, and the token in this file that shows each used
_HYPOTHETICAL = {
    "hypopg_create_index": "_MAKE", "hypopg_drop_index": "_DROP",
    "hypopg_relation_size": "_SIZE", "hypopg_reset": "_RESET",
    **{f: f for f in ("hypopg_list_indexes", "hypopg_get_indexdef",
                      "hypopg_hide_index", "hypopg_unhide_index",
                      "hypopg_hidden_indexes", "hypopg_unhide_all_indexes",
                      "hypopg_reset_index")}}


def _register():
    """The hypothetical-index extension's account: what is used is read
    off this file; what is not says why."""
    from .. import tooluse
    if "hypopg" in tooluse.REGISTRY:
        return
    unused = tooluse.UNUSED
    kept = ("an index the target already has is left as it is: which the"
            " workload's plans use is read off the plans themselves")
    tooluse.register(
        "hypopg", licence="PostgreSQL", release="1.4.1",
        declared={"": frozenset(_HYPOTHETICAL)},
        reads=(("storedcode/indexadvice.py::advise",),
               {("", f): t for f, t in _HYPOTHETICAL.items()}),
        options={
            ("", "hypopg_list_indexes"): (
                unused, "each index made is known by the number it was"
                        " made with, and dropped by it"),
            ("", "hypopg_get_indexdef"): (
                unused, "the statement advised is migkit's own, written"
                        " from the candidate it made"),
            ("", "hypopg_hide_index"): (unused, kept),
            ("", "hypopg_unhide_index"): (unused, "see hypopg_hide_index"),
            ("", "hypopg_hidden_indexes"): (unused,
                                            "see hypopg_hide_index"),
            ("", "hypopg_unhide_all_indexes"): (unused,
                                                "see hypopg_hide_index"),
            ("", "hypopg_reset_index"): (
                unused, "the older name of the reset, which is called by"
                        " its own")})


_register()

