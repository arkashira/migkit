"""Getting the indexes out of the way of a bulk load, and putting them back.

An index on a table being loaded is maintained one row at a time. Built after
the rows are already there, it is a sort - which is a different amount of
work, not a smaller amount of the same work.

Measured on PostgreSQL 16, 300,000 rows into a table with three secondary
indexes, on the same two-CPU machine:

    indexes present during the load     1.241s
    load into a bare table              0.230s
    build the three indexes afterwards  0.422s
                                        -----
                                        0.652s   (1.9x)

The gap widens with more indexes and more rows, because per-row maintenance
scales with both while a bulk build amortises the sort.

**The dangerous part is not the speed, it is the window.** Between the drop
and the rebuild the target has no indexes, and if the process dies there the
table is left slow and - worse - a unique index that was enforcing something
is simply gone. So the order here is fixed and not negotiable:

1. read the definitions and **write them to disk**
2. only then drop
3. load
4. rebuild, whether or not the load succeeded

Step 1 before step 2 is what makes step 4 possible from a different process,
an hour later, by a person. The file is the point.

**Constraints are never touched.** A primary key or a unique constraint is
enforcing something, not just making a query fast, and dropping one changes
what the database will accept - possibly while a foreign key elsewhere depends
on it. Only indexes that back no constraint are moved out of the way, which is
the majority of them and all of the ones that cost the most to maintain.
"""


def plan(rows):
    """(to_drop, ddl) from (name, definition, is_constraint) triples or 7-tuples.

    An index whose definition could not be read is left alone: dropping
    something migkit cannot recreate is the one mistake this file must never
    make.
    """
    to_drop, ddl = [], {}
    for item in rows:
        if len(item) == 7:
            name, definition = item[2], item[3]
            is_uniq, is_pk, is_con = item[4], item[5], item[6]
            if is_uniq or is_pk or is_con:
                continue
        elif len(item) == 3:
            name, definition, is_con = item
            if is_con:
                continue
        else:
            continue
        if not name or not definition:
            continue
        to_drop.append(str(name))
        ddl[str(name)] = str(definition)
    return sorted(to_drop), ddl


def saved(path, ddl):
    """Write the definitions where a human can find them, and confirm it.

    Returns True only when the file is on disk and reads back with every
    definition in it. A caller that gets False must not drop anything.
    """
    import json
    import os
    import pathlib
    import tempfile
    if not ddl:
        return True
    path = pathlib.Path(path)
    try:
        # inside the guard: a caller that gets an exception here instead of
        # False may not handle it, and the whole contract is "could not save,
        # so do not drop"
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    except Exception:
        return False
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(ddl, f, indent=1, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False
    try:
        back = json.loads(path.read_text())
    except Exception:
        return False
    return back == ddl


def restore_from(path):
    """Definitions written by an earlier run, for rebuilding by hand."""
    import json
    import pathlib
    p = pathlib.Path(path)
    if not p.exists():
        return {}
    try:
        got = json.loads(p.read_text())
        return got if isinstance(got, dict) else {}
    except Exception:
        return {}


def summary(dropped, rebuilt, failed):
    """One line for the log, naming anything that did not come back."""
    if not dropped:
        return "no secondary indexes to move out of the way"
    if failed:
        return (f"{len(rebuilt)} of {len(dropped)} indexes rebuilt;"
                f" STILL MISSING: {', '.join(sorted(failed))}"
                f" - the definitions are on disk, recreate them before the"
                f" target is used")
    return f"{len(dropped)} indexes dropped for the load and rebuilt after"


INDEXES_SET_ASIDE = "dropped-indexes"

PG_INDEX_SQL = """
select string_agg(concat_ws(chr(31), n.nspname, c.relname, i.relname,
                            pg_get_indexdef(ix.indexrelid),
                            ix.indisunique, ix.indisprimary,
                            con.conname is not null),
                  chr(30) order by n.nspname, c.relname, i.relname)
  from pg_index ix
  join pg_class i on i.oid = ix.indexrelid
  join pg_class c on c.oid = ix.indrelid
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_constraint con on con.conindid = ix.indexrelid
 where n.nspname not in ('pg_catalog', 'information_schema')
   and c.relkind in ('r', 'm', 'p')"""

MYSQL_INDEX_SQL = """
select concat_ws(char(31), table_schema, table_name, index_name,
                 non_unique = 0, index_name = 'PRIMARY',
                 group_concat(column_name order by seq_in_index separator ','))
  from information_schema.statistics
 where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
 group by table_schema, table_name, index_name, non_unique
 order by table_schema, table_name, index_name"""


def index_rows(raw):
    """(schema, table, name, definition, is_unique, is_primary, is_constraint)
    of each index `PG_INDEX_SQL` read."""
    out = []
    for rec in (raw or "").rstrip("\n").split(chr(30)):
        parts = rec.split(chr(31))
        if len(parts) != 7 or not parts[3]:
            continue
        sch, tbl, name, definition, uniq, pk, con = parts
        out.append((
            sch, tbl, name, definition,
            uniq.strip() in ("t", "true", "True", "1"),
            pk.strip() in ("t", "true", "True", "1"),
            con.strip() in ("t", "true", "True", "1"),
        ))
    return out


def mysql_index_rows(raw):
    """(schema, table, name, is_unique, is_primary, columns)
    of each index `MYSQL_INDEX_SQL` read."""
    out = []
    for line in (raw or "").splitlines():
        for rec in line.split(chr(30)):
            parts = rec.split(chr(31))
            if len(parts) != 6:
                continue
            sch, tbl, name, uniq, pk, cols = parts
            out.append((
                sch, tbl, name,
                uniq.strip() in ("1", "t", "true", "True"),
                pk.strip() in ("1", "t", "true", "True") or name.upper() == "PRIMARY",
                [c.strip() for c in cols.split(",") if c.strip()],
            ))
    return out


def plan_secondary_indexes(rows, tables=None, engine="postgres"):
    """(to_drop, plan) for secondary non-unique indexes.

    Filters out primary keys, unique constraints, and unique indexes.
    """
    to_drop, plan = [], {}
    from .engines.postgres import _ident

    for item in rows:
        if len(item) == 7:
            sch, tbl, name, definition, is_uniq, is_pk, is_con = item
        elif len(item) == 6:
            sch, tbl, name, is_uniq, is_pk, cols = item
            definition = (f"CREATE INDEX `{name}` ON `{sch}`.`{tbl}` ("
                          + ", ".join(f"`{c}`" for c in cols) + ")")
            is_con = False
        elif len(item) == 3:
            name, definition, is_con = item
            sch, tbl = "public", ""
            is_uniq, is_pk = False, False
        else:
            continue

        if is_pk or is_uniq or is_con:
            continue
        if not name or not definition:
            continue

        if tables is not None:
            if (sch, tbl) not in tables and tbl not in tables and (sch, name) not in tables:
                continue

        if engine == "postgres":
            key = f"{_ident(sch)}.{_ident(name)}"
            on = f"{_ident(sch)}.{_ident(tbl)}" if tbl else ""
        else:
            key = f"`{sch}`.`{name}`"
            on = f"`{sch}`.`{tbl}`" if tbl else ""

        to_drop.append(key)
        plan[key] = {
            "schema": sch,
            "table": tbl,
            "name": name,
            "def": definition,
            "on": on,
        }
    return sorted(to_drop), plan


def concurrent_index_sql(definition, engine="postgres"):
    """Convert an index creation statement to its online concurrent form."""
    import re
    clean = definition.strip().rstrip(";")
    if engine == "postgres":
        if re.search(r"\bCONCURRENTLY\b", clean, re.I):
            return clean
        m = re.match(r"^(\s*create\s+(?:unique\s+)?index)\s+(.+)$", clean, re.I)
        if m:
            return f"{m.group(1)} CONCURRENTLY {m.group(2)}"
        return clean
    elif engine == "mysql":
        if re.search(r"\bALGORITHM\s*=", clean, re.I):
            return clean
        if re.match(r"^\s*alter\s+table\b", clean, re.I):
            return f"{clean}, ALGORITHM=INPLACE, LOCK=NONE"
        if re.match(r"^\s*create\s+(?:unique\s+)?index\b", clean, re.I):
            return f"{clean} ALGORITHM=INPLACE LOCK=NONE"
        return clean
    return clean


def drop_index_sql(schema, name, table=None, engine="postgres"):
    """Generate statement to drop an index."""
    from .engines.postgres import _ident
    if engine == "postgres":
        sch = _ident(schema) if schema else ""
        nm = _ident(name)
        target = f"{sch}.{nm}" if sch else nm
        return f"DROP INDEX CONCURRENTLY IF EXISTS {target}"
    elif engine == "mysql":
        sch = f"`{schema}`." if schema else ""
        tbl = f"`{table}`" if table else ""
        nm = f"`{name}`"
        if tbl:
            return f"ALTER TABLE {sch}{tbl} DROP INDEX {nm}"
        return f"DROP INDEX {nm} ON {sch}{tbl}"
    return f"DROP INDEX IF EXISTS {name}"


def rebuild_indexes_parallel(definitions, run, max_workers=None,
                             engine="postgres", log=None):
    """Rebuild indexes post-load via parallel background workers."""
    if not definitions:
        return [], []
    import concurrent.futures
    import os
    if max_workers is None:
        max_workers = min(len(definitions), max(1, os.cpu_count() or 4))
    items = list(definitions.items())
    rebuilt, failed = [], []

    def _worker(item):
        key, val = item
        raw_def = val["def"] if isinstance(val, dict) else val
        sql = concurrent_index_sql(raw_def, engine=engine)
        try:
            run(sql)
            return key, sql, None
        except Exception as e:
            return key, sql, _words(e)

    workers = min(max_workers, len(items)) if max_workers > 0 else 1
    if workers > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            for key, sql, err in pool.map(_worker, items):
                if err is None:
                    rebuilt.append(key)
                else:
                    failed.append((sql, err))
    else:
        for item in items:
            key, sql, err = _worker(item)
            if err is None:
                rebuilt.append(key)
            else:
                failed.append((sql, err))

    return rebuilt, failed


class NotRebuilt(SystemExit):
    """What a load dropped did not all rebuild."""

    def __init__(self, failed, where):
        self.failed, self.where = list(failed), where
        super().__init__(
            f"{len(self.failed)} indexes did not rebuild on the target:"
            + "".join(f"\n  {sql} -> {why}" for sql, why in self.failed)
            + f"\nThe definitions are kept in {where}; rebuild them before"
              " the target is used.")


class IndexWindow:
    """The target's secondary non-unique indexes taken off for a load
    and rebuilt after via parallel background workers.
    """
    _held = set()

    def __init__(self, hop, db, log=None, tables=None, run=None,
                 max_workers=4, engine="postgres", hold=False):
        from .setaside import SetAside
        from .util import split_table
        self.hop, self.db, self.log, self.hold = hop, db, log, hold
        self.max_workers = max_workers
        self.engine = engine
        self.tables = (None if tables is None else
                       {tuple(t) if isinstance(t, tuple) else
                        split_table(engine, str(t), "public" if engine == "postgres" else "")
                        for t in tables})
        if run is None:
            if engine == "postgres":
                from .movers import _pg_psql
                def run(sql):
                    return _pg_psql(hop, db, sql)
            elif engine == "mysql":
                from .movers import _mysql_sh
                def run(sql):
                    return _mysql_sh(hop, db, sql)
        self.run = run
        self.record = SetAside(hop, db, INDEXES_SET_ASIDE)
        self.dropped = {}

    def __enter__(self):
        from . import decide
        sql = PG_INDEX_SQL if self.engine == "postgres" else MYSQL_INDEX_SQL
        try:
            raw = self.run(sql)
            rows = (index_rows(raw) if self.engine == "postgres"
                    else mysql_index_rows(raw))
        except Exception as e:
            raise decide.Blocked(
                "the target's indexes could not be read, so none were"
                f" taken off: {_words(e)}") from e
        to_drop, plan = plan_secondary_indexes(rows, self.tables, engine=self.engine)
        left, stale = self.record.left_behind()
        for path in sorted(p for p in IndexWindow._held
                           if p.parent == self.record.dir):
            IndexWindow._held.discard(path)
            left = {**restore_from(path), **left}
            stale.append(path)
        if not plan and not left:
            for path in stale:
                path.unlink(missing_ok=True)
            return self
        if not self.record.save({**plan, **left}, stale):
            raise decide.Blocked(
                "the index definitions could not be saved to"
                f" {self.hop.report_dir(self.db)}, so none were taken off")
        self.dropped = dict(left)
        for key in to_drop:
            idx = plan[key]
            drop_sql = drop_index_sql(idx["schema"], idx["name"],
                                      table=idx.get("table"), engine=self.engine)
            try:
                self.run(drop_sql)
            except Exception as e:
                why = f"{drop_sql} -> {_words(e)}"
                self._rebuild()
                raise decide.Blocked(
                    f"an index could not be taken off: {why}; every"
                    " index taken off before it is rebuilt") from e
            self.dropped[key] = left.get(key, idx)
        if self.log and plan:
            self.log(f"{len(plan)} secondary indexes dropped for the load;"
                     f" definitions saved to {self.record.path}")
        return self

    def __exit__(self, kind, *exc):
        if self.hold and kind is not None and self.dropped:
            IndexWindow._held.add(self.record.path)
            if self.log:
                self.log(f"{len(self.dropped)} indexes stay off the"
                         " target for the load that goes on from this one;"
                         f" their definitions are kept in"
                         f" {self.record.path}")
            return False
        self._rebuild()
        return False

    def _rebuild(self):
        if not self.dropped:
            self.record.done()
            return
        rebuilt, failed = rebuild_indexes_parallel(
            self.dropped, self.run, max_workers=self.max_workers,
            engine=self.engine, log=self.log)
        if failed:
            IndexWindow._held.add(self.record.path)
            raise NotRebuilt(failed, self.record.path)
        if self.log:
            self.log(f"{len(rebuilt)} secondary indexes rebuilt")
        self.dropped = {}
        self.record.done()


# ---- foreign keys: taken off a target for a load, put back after -----------

#: A load's set-aside record of the foreign keys it took off
#: (`setaside.SetAside`).
FKS_SET_ASIDE = "dropped-fks"

#: The target's foreign keys outside the catalogues, one record per key
#: (`chr(30)` between records, `chr(31)` between fields, so a name holding
#: a newline stays one name): schema, table, name, validated, definition,
#: referenced schema, referenced table. A key a partition inherits from
#: its table (`coninhcount`) is not one: the server refuses to drop it, and
#: it goes and comes back with its table's.
PG_FK_SQL = """
select string_agg(concat_ws(chr(31), cn.nspname, c.relname, k.conname,
                            k.convalidated, pg_get_constraintdef(k.oid),
                            rn.nspname, r.relname),
                  chr(30) order by cn.nspname, c.relname, k.conname)
  from pg_constraint k
  join pg_class c on c.oid = k.conrelid
  join pg_namespace cn on cn.oid = c.relnamespace
  join pg_class r on r.oid = k.confrelid
  join pg_namespace rn on rn.oid = r.relnamespace
 where k.contype = 'f' and k.coninhcount = 0
   and cn.nspname not in ('pg_catalog', 'information_schema')"""


class NotPutBack(SystemExit):
    """What a load took off the target did not all go back. The move
    stops here: the target accepts rows its own keys would refuse. Each
    statement is said with the server's words; the definitions stay on
    disk for the next load, or a person, to put back."""

    def __init__(self, failed, where):
        self.failed, self.where = list(failed), where
        super().__init__(
            f"{len(self.failed)} foreign keys did not go back on the target:"
            + "".join(f"\n  {sql} -> {why}" for sql, why in self.failed)
            + f"\nThe definitions are kept in {where}; add them back before"
              " the target is used.")


def fk_rows(raw):
    """(schema, table, name, validated, definition, ref schema, ref table)
    of each key `PG_FK_SQL` read."""
    out = []
    for rec in (raw or "").rstrip("\n").split(chr(30)):
        parts = rec.split(chr(31))
        if len(parts) != 7 or not parts[4]:
            continue
        sch, tbl, name, valid, definition, rsch, rtbl = parts
        out.append((sch, tbl, name, valid.strip() in ("t", "true"),
                    definition, rsch, rtbl))
    return out


def fk_key(schema, table, name):
    """One key as the target's SQL names it, the same for a key saved and
    a key found there."""
    from .engines.postgres import _ident
    return f"{_ident(schema)}.{_ident(table)}.{_ident(name)}"


def fk_plan(rows, tables=None):
    """{key: {"on", "name", "def", "valid"}} of the keys a load into
    `tables` ((schema, table) pairs; None for every table) takes off: those
    on a table it fills and those pointing into one - both refuse the
    load's rows in an order the source never had to keep."""
    from .engines.postgres import _ident
    out = {}
    for sch, tbl, name, valid, definition, rsch, rtbl in rows:
        if tables is not None and (sch, tbl) not in tables \
                and (rsch, rtbl) not in tables:
            continue
        out[fk_key(sch, tbl, name)] = {
            "on": f"{_ident(sch)}.{_ident(tbl)}", "name": _ident(name),
            "def": definition, "valid": valid}
    return out


def fk_put_back(key, fk, present):
    """The statements that put `fk` back on a target holding `present`
    ({key: validated}): added as not yet valid - which takes no lock that
    stops the application's writes - and then validated, where the source
    held it valid. A key already there and valid needs nothing."""
    alter = f"alter table {fk['on']}"
    out = []
    if key not in present:
        late = fk["valid"] and not fk["def"].upper().endswith(" NOT VALID")
        out.append(f"{alter} add constraint {fk['name']} {fk['def']}"
                   + (" not valid" if late else ""))
        if late:
            out.append(f"{alter} validate constraint {fk['name']}")
    elif fk["valid"] and not present[key]:
        out.append(f"{alter} validate constraint {fk['name']}")
    return out


def fk_endpoints(key, fk):
    """Return ((child_schema, child_table), (parent_schema, parent_table))."""
    import re
    if "table" in fk and "ref_table" in fk:
        child = fk["table"] if isinstance(fk["table"], tuple) else (None, fk["table"])
        parent = fk["ref_table"] if isinstance(fk["ref_table"], tuple) else (None, fk["ref_table"])
        return child, parent

    on = fk.get("on", "")
    on_parts = [p.strip('"`[] ') for p in on.split(".") if p.strip()]
    child = (on_parts[-2], on_parts[-1]) if len(on_parts) >= 2 else (None, on_parts[0] if on_parts else "")

    definition = fk.get("def", "")
    m = re.search(r"\breferences\s+([^\s(]+)", definition, re.I)
    if m:
        ref = m.group(1).strip()
        ref_parts = [p.strip('"`[] ') for p in ref.split(".") if p.strip()]
        parent = (ref_parts[-2], ref_parts[-1]) if len(ref_parts) >= 2 else (child[0], ref_parts[0])
    else:
        parent = (None, None)
    return child, parent


def topological_fk_sort(fks):
    """Sort foreign keys into topological dependency generations.

    Parents precede children in validation order. Returns a list of
    generations (lists of keys), where all keys in a generation are
    independent and may be validated concurrently.
    """
    if not fks:
        return []

    endpoints = {}
    table_to_child_fks = {}
    for key, fk in fks.items():
        child, parent = fk_endpoints(key, fk)
        endpoints[key] = (child, parent)
        table_to_child_fks.setdefault(child, set()).add(key)

    deps = {key: set() for key in fks}
    dependents = {key: set() for key in fks}

    for b_key, (cb, pb) in endpoints.items():
        if pb and pb in table_to_child_fks:
            for a_key in table_to_child_fks[pb]:
                if a_key != b_key:
                    deps[b_key].add(a_key)
                    dependents[a_key].add(b_key)

    in_degrees = {k: len(v) for k, v in deps.items()}
    generations = []
    ready = sorted([k for k, deg in in_degrees.items() if deg == 0])
    visited = set()

    while ready:
        generations.append(ready)
        visited.update(ready)
        next_ready = []
        for a_key in ready:
            for b_key in dependents[a_key]:
                in_degrees[b_key] -= 1
                if in_degrees[b_key] == 0:
                    next_ready.append(b_key)
        ready = sorted(next_ready)

    remaining = sorted([k for k in fks if k not in visited])
    if remaining:
        generations.append(remaining)

    return generations


def topological_validate_fks(dropped, run, max_workers=4, present=None,
                             log=None):
    """Validate foreign keys in topological generations using parallel workers."""
    if not dropped:
        return [], []
    import concurrent.futures
    present = present or {}
    generations = topological_fk_sort(dropped)
    validated, failed = [], []

    for gen in generations:
        to_validate = []
        for key in gen:
            fk = dropped[key]
            if fk.get("valid"):
                if key in present and present[key]:
                    continue
                to_validate.append((key, fk))
        if not to_validate:
            continue

        def _val_one(item):
            key, fk = item
            sql = f"alter table {fk['on']} validate constraint {fk['name']}"
            try:
                run(sql)
                return key, sql, None
            except Exception as e:
                return key, sql, _words(e)

        workers = min(max_workers, len(to_validate)) if max_workers > 0 else 1
        if workers > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                for key, sql, err in pool.map(_val_one, to_validate):
                    if err is None:
                        validated.append(key)
                    else:
                        failed.append((sql, err))
        else:
            for item in to_validate:
                key, sql, err = _val_one(item)
                if err is None:
                    validated.append(key)
                else:
                    failed.append((sql, err))
        if failed:
            break

    return validated, failed


def restore_foreign_keys_non_blocking(dropped, run, max_workers=None,
                                      present=None, log=None):
    """Restore foreign keys: ADD CONSTRAINT ... NOT VALID, then topological VALIDATE."""
    if not dropped:
        return [], []
    import os
    if max_workers is None:
        max_workers = min(len(dropped), max(1, os.cpu_count() or 4))
    present = present or {}
    added, failed = [], []

    for key in sorted(dropped):
        fk = dropped[key]
        if key not in present:
            alter = f"alter table {fk['on']}"
            late = fk["valid"] and not fk["def"].upper().endswith(" NOT VALID")
            sql = f"{alter} add constraint {fk['name']} {fk['def']}" + (
                " not valid" if late else "")
            try:
                run(sql)
                added.append(key)
            except Exception as e:
                failed.append((sql, _words(e)))
                return added, failed

    validated, val_failed = topological_validate_fks(
        dropped, run, max_workers=max_workers, present=present, log=log)
    if val_failed:
        failed.extend(val_failed)
    return added + validated, failed


def _words(e):
    lines = [x for x in str(e).splitlines() if x.strip()]
    return lines[-1][:200] if lines else type(e).__name__


class ForeignKeyWindow:
    """The target's foreign keys taken off for a load and put back after,
    on the one record every take-off keeps (`setaside.SetAside`).

    The definitions reach disk before any key is dropped. A key that cannot
    be read, saved or dropped leaves the window shut, with what was dropped
    put back and the reason raised (`decide.Blocked`) - another way may
    load with the keys in place. On the way out every key goes back, the
    keys a load that died left off with them; one that does not is the
    move's end (`NotPutBack`), never a line in the log.

    `hold`: a load that dies inside the window leaves its keys off and its
    record in place for the load that goes on from it - put back over
    half a copy, a key is refused on rows the rest of the copy would
    have matched, and that refusal took the place of the copy's own
    error. The record stops the cutover until the keys are back
    (`cutover`), and the next window - in this process or a later one -
    takes them over and puts them back at its end.

    `tables`: names, or (schema, table) pairs as the target has them.
    `run(sql)` sends one statement to the target and returns what it
    printed; by default the target's own client (`movers._pg_psql`).
    """

    #: the records a window held for the load that goes on, this process's
    #: own: alive while it runs, so no record of a dead load
    _held = set()

    def __init__(self, hop, db, log=None, tables=None, run=None,
                 hold=False, topological=False, max_workers=1):
        from .setaside import SetAside
        from .util import split_table
        self.hop, self.db, self.log, self.hold = hop, db, log, hold
        self.topological = topological
        self.max_workers = max_workers
        self.tables = (None if tables is None else
                       {tuple(t) if isinstance(t, tuple) else
                        split_table("postgres", str(t), "public")
                        for t in tables})
        if run is None:
            from .movers import _pg_psql

            def run(sql):
                return _pg_psql(hop, db, sql)
        self.run = run
        self.record = SetAside(hop, db, FKS_SET_ASIDE)
        self.dropped = {}

    def __enter__(self):
        from . import decide
        try:
            rows = fk_rows(self.run(PG_FK_SQL))
        except Exception as e:
            raise decide.Blocked(
                "the target's foreign keys could not be read, so none were"
                f" taken off: {_words(e)}") from e
        plan = fk_plan(rows, self.tables)
        present = {fk_key(s, t, n): v for s, t, n, v, *_ in rows}
        left, stale = self.record.left_behind()
        # what a load of this process that died in its window held off
        for path in sorted(p for p in ForeignKeyWindow._held
                           if p.parent == self.record.dir):
            ForeignKeyWindow._held.discard(path)
            left = {**restore_from(path), **left}
            stale.append(path)
        left = {k: fk for k, fk in left.items()
                if isinstance(fk, dict) and fk_put_back(k, fk, present)}
        if not plan and not left:
            # a dead load's keys the target has back: its record is done
            for path in stale:
                path.unlink(missing_ok=True)
            return self
        # a key both the dead load set aside and the target holds now - put
        # back half way, added and never validated - goes back as the dead
        # load saved it: valid where the source held it so. The target's
        # present state (not yet valid) would have kept it so for good
        if not self.record.save({**plan, **left}, stale):
            raise decide.Blocked(
                "the foreign keys' definitions could not be saved to"
                f" {self.hop.report_dir(self.db)}, so none were taken off")
        self.dropped = dict(left)
        if left and self.log:
            self.log(f"{len(left)} foreign keys an earlier load took off and"
                     " did not put back go back at the end of this one")
        for key, fk in plan.items():
            sql = f"alter table {fk['on']} drop constraint {fk['name']}"
            try:
                self.run(sql)
            except Exception as e:
                why = f"{sql} -> {_words(e)}"
                self._put_back()
                raise decide.Blocked(
                    f"a foreign key could not be taken off: {why}; every"
                    " key taken off before it is back") from e
            self.dropped[key] = left.get(key, fk)
        if self.log and plan:
            self.log(f"{len(plan)} foreign keys taken off for the load;"
                     f" definitions saved to {self.record.path}")
        return self

    def __exit__(self, kind, *exc):
        if self.hold and kind is not None and self.dropped:
            ForeignKeyWindow._held.add(self.record.path)
            if self.log:
                self.log(f"{len(self.dropped)} foreign keys stay off the"
                         " target for the load that goes on from this one;"
                         f" their definitions are kept in"
                         f" {self.record.path}, and the cutover waits for"
                         " them")
            return False
        self._put_back()
        return False            # never swallow the load's own exception

    def _put_back(self):
        """Every key set aside back on the target, or `NotPutBack` naming
        each statement it refused; the record goes only when all are."""
        if not self.dropped:
            self.record.done()
            return
        try:
            present = {fk_key(s, t, n): v for s, t, n, v, *_ in
                       fk_rows(self.run(PG_FK_SQL))}
        except Exception as e:
            raise NotPutBack([("reading the target's foreign keys",
                               _words(e))], self.record.path) from e
        failed = []
        if self.topological:
            _, failed = restore_foreign_keys_non_blocking(
                self.dropped, self.run, max_workers=self.max_workers,
                present=present, log=self.log)
        else:
            for key in sorted(self.dropped):
                for sql in fk_put_back(key, self.dropped[key], present):
                    try:
                        self.run(sql)
                    except Exception as e:
                        failed.append((sql, _words(e)))
                        break
        if failed:
            # what went back is not owed again; the rest waits for the
            # next window of this process, as for one that died in its load
            ForeignKeyWindow._held.add(self.record.path)
            raise NotPutBack(failed, self.record.path)
        if self.log:
            self.log(f"{len(self.dropped)} foreign keys put back")
        self.dropped = {}
        self.record.done()


class DeferredIndexLifecycle:
    """Unified lifecycle manager for deferred indexes and non-blocking FKs."""

    def __init__(self, hop, db=None, log=None, tables=None, run=None,
                 max_workers=None, engine="postgres", hold=False):
        if db is None:
            db = getattr(hop, "target_db", None) or getattr(hop, "db", None) or ""
        if max_workers is None:
            opts = getattr(hop, "options", None) or {}
            max_workers = (
                opts.get("workers")
                or getattr(hop, "workers", None)
                or getattr(hop, "workers_most", None)
                or 4
            )
        self.hop, self.db, self.log, self.hold = hop, db, log, hold
        self.max_workers = max_workers
        self.engine = engine
        self.tables = tables
        self.run = run
        self.ix_window = IndexWindow(
            hop, db, log=log, tables=tables, run=run,
            max_workers=max_workers, engine=engine, hold=hold)
        self.fk_window = ForeignKeyWindow(
            hop, db, log=log, tables=tables, run=run,
            hold=hold, topological=True, max_workers=max_workers)

    def drop_non_unique_indexes(self):
        self.fk_window.__enter__()
        self.ix_window.__enter__()
        return list(self.ix_window.dropped.keys())

    def rebuild_indexes(self):
        self.ix_window._rebuild()
        return list(self.ix_window.dropped.keys())

    def restore_foreign_keys(self):
        self.fk_window._put_back()
        return list(self.fk_window.dropped.keys())

    def __enter__(self):
        self.fk_window.__enter__()
        self.ix_window.__enter__()
        return self

    def __exit__(self, kind, *exc):
        ix_exc = None
        try:
            self.ix_window.__exit__(kind, *exc)
        except Exception as e:
            ix_exc = e
        try:
            self.fk_window.__exit__(kind, *exc)
        except Exception as e:
            if ix_exc:
                raise ix_exc from e
            raise
        if ix_exc:
            raise ix_exc
        return False
