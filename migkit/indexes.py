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
    """(to_drop, ddl) from (name, definition, is_constraint) triples.

    An index whose definition could not be read is left alone: dropping
    something migkit cannot recreate is the one mistake this file must never
    make.
    """
    to_drop, ddl = [], {}
    for name, definition, is_constraint in rows:
        if is_constraint:
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


# ---- foreign keys: taken off a target for a load, put back after -----------

#: A load's set-aside record of the foreign keys it took off
#: (`setaside.SetAside`).
FKS_SET_ASIDE = "dropped-fks"

#: The target's foreign keys outside the catalogues, one record per key
#: (`chr(30)` between records, `chr(31)` between fields, so a name holding
#: a newline stays one name): schema, table, name, validated, definition,
#: referenced schema, referenced table.
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
 where k.contype = 'f'
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

    `run(sql)` sends one statement to the target and returns what it
    printed; by default the target's own client (`movers._pg_psql`).
    """

    def __init__(self, hop, db, log=None, tables=None, run=None):
        from .setaside import SetAside
        from .util import split_table
        self.hop, self.db, self.log = hop, db, log
        self.tables = (None if tables is None else
                       {split_table("postgres", str(t), "public")
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
        left = {k: fk for k, fk in left.items()
                if isinstance(fk, dict) and fk_put_back(k, fk, present)}
        if not plan and not left:
            # a dead load's keys the target has back: its record is done
            for path in stale:
                path.unlink(missing_ok=True)
            return self
        if not self.record.save({**left, **plan}, stale):
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
            self.dropped[key] = fk
        if self.log and plan:
            self.log(f"{len(plan)} foreign keys taken off for the load;"
                     f" definitions saved to {self.record.path}")
        return self

    def __exit__(self, *exc):
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
        for key in sorted(self.dropped):
            for sql in fk_put_back(key, self.dropped[key], present):
                try:
                    self.run(sql)
                except Exception as e:
                    failed.append((sql, _words(e)))
                    break
        if failed:
            raise NotPutBack(failed, self.record.path)
        if self.log:
            self.log(f"{len(self.dropped)} foreign keys put back")
        self.dropped = {}
        self.record.done()
