"""Best-tool movers.

migkit does not compete with the battle-tested bulk movers, it drives
them: pg_dump/pg_restore parallel jobs, mydumper/myloader, pgloader,
mongodump/mongorestore. The builtin chunked copy stays the fallback
(and the only mode with per-chunk crash resume).

The managed streaming pipeline below is selected by migkit itself when an
engine has no native CDC path; callers do not choose it and do not need to
know what it runs underneath. Third-party components used there are named
only in the generated compose file. See NOTICE for attribution.
"""
import functools
import re
import subprocess
from urllib.parse import quote

from .util import run, tool_env, which

VIAS = ("auto", "builtin", "pgdump", "pgcopydb", "mydumper",
        "pgloader", "mongodump")


def pick(engine, table=""):
    """Fastest installed tool for whole-db moves, builtin for single tables
    (chunk resume matters more than raw speed there)."""
    if table:
        return "builtin"
    if engine == "postgres":
        # the version-matched container first: it overlaps copy, index and
        # constraint work, builds indexes after the rows land, and needs no
        # intermediate directory - none of which the dump path does
        if pgcopydb_available():
            return "pgcopydb"
        if which("pg_dump") and which("pg_restore"):
            return "pgdump"
    if engine == "mysql" and which("mydumper") and which("myloader"):
        return "mydumper"
    if engine == "hetero" and which("pgloader"):
        return "pgloader"
    if engine == "mongodb" and which("mongodump") and which("mongorestore"):
        return "mongodump"
    return "builtin"


def chosen(engine, table=""):
    """Which mover to use. Not a user-facing decision.

    Picking the fastest installed tool for an engine is knowledge migkit
    already has; making the operator supply it just moves migkit's homework
    onto them, and gets it wrong when the machine changes. MIGKIT_MOVER
    overrides it for debugging - deliberately an environment variable and not
    a flag, so it stays out of the command surface.
    """
    import os
    forced = os.environ.get("MIGKIT_MOVER", "").strip()
    if forced:
        if forced not in VIAS:
            raise SystemExit(f"MIGKIT_MOVER={forced} is not one of"
                             f" {', '.join(VIAS)}")
        if forced != "builtin" and not supported(engine, forced):
            raise SystemExit(f"MIGKIT_MOVER={forced} does not apply to"
                             f" a {engine} hop")
        return forced
    return pick(engine, table)


#: Movers that can be given a row predicate per table, measured rather
#: than assumed. mydumper takes one section per table in a defaults file
#: (verified: 2 of 3 rows dumped where a rule applied). `pg_dump` 18.6 and
#: `pgcopydb` 0.18 have `-t`, `-T`, `--exclude-table-data`, `--filter` and
#: `--filters` between them and **not one row predicate** - their filtering
#: is table-level throughout.
ROW_FILTER_MOVERS = ("mydumper",)


def refuse_unpushable_filters(hop, db, via):
    """Stop a move whose row filters the chosen mover cannot honour.

    The quiet failure this prevents is the expensive one. A mover that
    ignores the filter copies every row; the check, reading the same
    mapping, then compares the filtered source against a target holding
    everything and reports the difference forever. The move looks like it
    worked and the verification never goes green, which is the worst of
    both - so it refuses before anything is copied, and names the tables.
    """
    rules = (getattr(hop, "mapping", None) or {}).get("where") or {}
    if not rules or via in ROW_FILTER_MOVERS:
        return
    mine = sorted(k for k in rules
                  if len([p for p in str(k).split(".") if p]) < 2
                  or str(k).split(".")[0] == db)
    if not mine:
        return
    why = {
        "pgdump": "pg_dump 18.6 filters by table (-t, -T,"
                  " --exclude-table-data, --filter) and never by row",
        "pgcopydb": "pgcopydb 0.18's --filters selects tables, not rows",
    }.get(via, f"the {via} mover applies no row predicate")
    raise SystemExit(
        f"the hop maps row filters onto {', '.join(mine)}, and {why}."
        " Moving anyway would copy every row and leave `check` comparing"
        " a filtered source against a full target for good. Drop the"
        " filters, or narrow the source with a view the hop points at"
        " instead."
    )


def supported(engine, via):
    return {"pgdump": engine == "postgres",
            "pgcopydb": engine == "postgres",
            "mydumper": engine == "mysql",
            "pgloader": engine == "hetero",
            "mongodump": engine == "mongodb"}.get(via, True)


def stream_supported(engine):
    """Internal: can migkit stand up its managed streaming pipeline for this
    engine. Never an operator choice; `move --mode cdc` falls back to it
    when the engine has no native CDC path."""
    return engine in ("postgres", "mysql", "hetero")


def _sh(cmd, env=None, log=None):
    if log:
        log("$ " + " ".join(str(c) for c in cmd))
    p = subprocess.run(cmd, env=tool_env(env), text=True,
                       capture_output=True)
    if p.returncode:
        raise RuntimeError((p.stderr or p.stdout)[-500:])
    return p


#: The target's user tables, one per line. This used to assemble the whole
#: `truncate` statement with `string_agg`, which put "which tables count as
#: the application's" in SQL while "which tables the hop excludes" stayed in
#: Python - so the two never met and the excluded ones were emptied.
PG_USER_TABLES_SQL = (
    "select format('%I.%I', n.nspname, c.relname)"
    " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
    " where c.relkind = 'r'"
    " and n.nspname not in ('pg_catalog','information_schema')"
    " and n.nspname not like 'pg\\_%'"
    " and n.nspname not like '\\_\\_%'"
    " and c.relname not like 'migkit\\_%'"
    " order by 1")

#: Everything a `truncate ... cascade` of `:names` would empty as well,
#: through a foreign key at any depth. Leaving a table out of the statement
#: is not the same as leaving its rows alone: PostgreSQL follows references
#: into tables nobody named, and announces it as a NOTICE *while it is doing
#: it*. Asked first, the same catalogue answers in time to stop.
PG_CASCADE_REACH_SQL = """
with recursive reached(oid) as (
    select c.oid from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
     where format('%I.%I', n.nspname, c.relname)
           = any (string_to_array({names}, chr(31)))
    union
    select con.conrelid from pg_constraint con
      join reached r on con.confrelid = r.oid
     where con.contype = 'f'
)
select format('%I.%I', n.nspname, c.relname)
  from reached r
  join pg_class c on c.oid = r.oid
  join pg_namespace n on n.oid = c.relnamespace
"""


def _unresolved_note(why):
    """The plan line when the source's tables could not be listed."""
    return ("# could not list the source's tables, so the hop's exclude list"
            f" cannot be pushed down: {why}")


def _unresolved_exclusion(db, why):
    """Stop a move whose exclusions could not be resolved - before anything.

    The emptying step keeps the tables the hop excludes. A dump that could
    not be told to skip them carries them anyway, and the load then puts
    the source's rows on top of the target's own - into exactly the tables
    the setting exists to protect. Every bulk path raises this one, so they
    stop at the same point and say the same thing.
    """
    return SystemExit(
        f"{db}: the hop excludes tables and the source's table list could not"
        f" be read ({why}), so the copy cannot be told to skip them. Nothing"
        " has been changed on the target.")


def _pg_literal(text):
    """`text` as a PostgreSQL string literal.

    `psql -c` hands the string to the server untouched - it does **not**
    interpolate `:'var'`, so a query written that way reaches the server with
    the colon still in it and fails on a syntax error. The value therefore has
    to arrive already quoted, and a table name is allowed to contain the quote
    character: `create table "it's"` is legal, and `format('%I.%I', ...)`
    renders it back with the apostrophe intact.
    """
    return "'" + str(text).replace("'", "''") + "'"


def _truncate_step(hop, db=""):
    """The plan line for the step that empties the target.

    One wording for every bulk path that empties the target before loading
    it, and it has to track what the emptying does: a plan that says "all
    user tables" beside a plan that says the excluded ones are skipped
    describes a move that empties a table and never refills it.
    """
    where = f" {db}" if db else ""
    if getattr(hop, "exclude", None):
        return (f"# empty the target's{where} user tables, except the ones"
                " the hop excludes (generated from catalog)")
    return (f"# empty all the target's{where} user tables"
            " (generated from catalog)")


def _pg_truncate_target(hop, db, log=None):
    """Empty the user tables a data-only load is about to fill.

    One copy, used by both PostgreSQL bulk paths: a data-only load that
    appends instead of replacing produces a target with every row twice, and
    two versions of "which tables count as the application's" would eventually
    disagree about which ones got emptied.

    **What the hop excludes is not emptied.** `exclude` is documented as
    protecting tables whose rows are written on the target rather than
    carried from the source. The dump already skips them - so emptying them
    here deleted precisely the rows the setting promises to keep, and left
    nothing to put back. Measured on a target holding two rows no source had:

        # 1 tables the hop excludes are not dumped at all
        audit_log before: 2 rows
        audit_log after:  0 rows

    and the move then reported `public.audit_log` as a table the copy had
    failed to fill, which sent the reader looking at the wrong end of it.

    The set is resolved through the same `excluded_tables()` the dump and
    `check` use, so all three empty, carry and verify the same tables.
    """
    t = hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    env_t = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}

    def psql(*args):
        return _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
                    "-d", ddb, "-X"] + list(args), env_t)

    names = [ln for ln in psql("-At", "-c", PG_USER_TABLES_SQL
                               ).stdout.splitlines() if ln.strip()]
    skip = set(excluded_tables(hop, db, names))
    keep = [n for n in names if n not in skip]
    if not keep:
        return ""
    if skip:
        reached = {ln for ln in psql("-At", "-c", PG_CASCADE_REACH_SQL.format(
            names=_pg_literal(chr(31).join(keep)))).stdout.splitlines()
            if ln.strip()}
        caught = sorted(skip & reached)
        if caught:
            raise SystemExit(
                f"{', '.join(caught)} would be emptied anyway: the table"
                " references one of the tables this move replaces, and"
                " emptying a table empties everything pointing at it.\n"
                "  the hop excludes it, which says its rows are written here"
                " and not carried, so they cannot be put back afterwards.\n"
                "  either it is not target-owned after all - take it out of"
                " the hop's exclude list and let the move carry it - or the"
                " reference has to go before the move can run.")
    stmt = "truncate table " + ", ".join(keep) + " cascade"
    _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
         "-d", ddb, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c", stmt],
        env_t, log)
    return stmt


PG_INDEX_SQL = """
    select n.nspname||'.'||tb.relname||chr(31)||i.relname||chr(31)
           ||pg_get_indexdef(x.indexrelid)||chr(31)
           ||(c.conname is not null)::text
      from pg_index x
      join pg_class i on i.oid = x.indexrelid
      join pg_class tb on tb.oid = x.indrelid
      join pg_namespace n on n.oid = tb.relnamespace
      left join pg_constraint c on c.conindid = x.indexrelid
     where n.nspname not in ('pg_catalog','information_schema')
       and n.nspname not like 'pg\\_%' and n.nspname not like '\\_\\_%'
       and tb.relname not like 'migkit\\_%'
       and x.indisvalid
     order by 1"""


def _pg_psql(hop, db, sql, log=None):
    t = hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    env = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    return _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
                "-d", ddb, "-X", "-At", "-v", "ON_ERROR_STOP=1",
                "-c", sql], env, log).stdout


def _outside_exclusion(hop, db, rows, log=None, qualifier="public"):
    """The index `rows` - table first - on tables the hop does not exclude.

    A table the hop excludes is one the target owns: its rows are written
    there, often by the application while the move runs. Dropping its
    indexes for the load's sake was measured to cost it for good. The table
    kept a unique index built with `CREATE UNIQUE INDEX`, the application
    wrote a second row with the same key while the window was open, and the
    rebuild then failed:

        REBUILD FAILED for audit_ref_u: Key (ref)=(r7) is duplicated.
        2 of 3 indexes rebuilt; STILL MISSING: audit_ref_u

    - the one table migkit was told not to touch, left without the
    constraint that would have refused the duplicate, and holding it. Its
    load gains nothing from the drop either, because nothing is loaded into
    it. Which tables count as excluded is `excluded_tables()`'s answer, not
    a second reading here.
    """
    owned = {t for t in {r[0] for r in rows}
             if excluded_tables(hop, db, [t], qualifier)}
    kept = [r for r in rows if r[0] not in owned]
    if owned and log:
        log(f"{len(rows) - len(kept)} indexes on {len(owned)} tables the hop"
            " excludes were left in place")
    return kept


class _IndexWindow:
    """Drop the target's secondary indexes for a load and put them back.

    Measured worth on PostgreSQL 16: 300,000 rows into a table with three
    secondary indexes took 1.241s with the indexes in place and 0.652s as a
    bare load plus a bulk build - 1.9x, on two CPUs.

    Used as a context manager so the rebuild happens on the way out whether
    the load worked or raised. The definitions reach disk before anything is
    dropped; if they cannot, nothing is dropped and the load simply runs the
    slower way.
    """

    def __init__(self, hop, db, workers, log):
        self.hop, self.db, self.workers, self.log = hop, db, workers, log
        self.dropped, self.ddl = [], {}

    def __enter__(self):
        from . import indexes as _ix
        try:
            raw = _pg_psql(self.hop, self.db, PG_INDEX_SQL)
        except Exception:
            return self
        found = []
        for line in (raw or "").splitlines():
            parts = line.split("\x1f")
            if len(parts) == 4:
                found.append((parts[0].strip(), parts[1].strip(),
                              parts[2].strip(), parts[3].strip() == "true"))
        rows = [r[1:] for r in
                _outside_exclusion(self.hop, self.db, found, self.log)]
        drop, ddl = _ix.plan(rows)
        if not drop:
            return self
        where = self.hop.report_dir(self.db) / "dropped-indexes.json"
        if not _ix.saved(where, ddl):
            if self.log:
                self.log("could not save the index definitions, so none were"
                         " dropped - the load runs with them in place")
            return self
        for name in drop:
            try:
                _pg_psql(self.hop, self.db, f'drop index "{name}"')
                self.dropped.append(name)
                self.ddl[name] = ddl[name]
            except Exception as e:
                if self.log:
                    self.log(f"could not drop {name}, leaving it:"
                             f" {str(e).splitlines()[-1][:80]}")
        if self.log and self.dropped:
            self.log(f"{len(self.dropped)} secondary indexes dropped for the"
                     f" load; definitions saved to {where}")
        return self

    def __exit__(self, *exc):
        from . import indexes as _ix
        rebuilt, failed = [], []
        for name in self.dropped:
            try:
                _pg_psql(self.hop, self.db, self.ddl[name])
                rebuilt.append(name)
            except Exception as e:
                failed.append(name)
                if self.log:
                    self.log(f"REBUILD FAILED for {name}:"
                             f" {str(e).splitlines()[-1][:100]}")
        if self.log:
            self.log(_ix.summary(self.dropped, rebuilt, failed))
        return False            # never swallow the load's own exception


def pgdump_move(hop, db, workers, go, log):
    """Dump, then empty the target, then restore - in that order.

    The dump is a directory on disk (`-Fd`), so nothing forces the target to
    be emptied before it exists. Emptying it first was measured to cost the
    target everything when the source could not be reached:

        move failed: ... Is the server running on that host and accepting
        TCP/IP connections?
        target orders: 2 rows before the move, 0 after

    - a move that failed and still deleted what it had come to replace.
    """
    s, t = hop.source, hop.target
    outdir = hop.report_dir(db) / "pgdump"
    # the same resolution pgcopydb and `check` use, so all three exclude
    # exactly the same tables rather than three readings of one pattern
    skip, unresolved = [], ""
    if getattr(hop, "exclude", None):
        try:
            from .engines.postgres import PostgresEngine
            skip = excluded_tables(hop, db,
                                   PostgresEngine(hop).neutral_tables("src",
                                                                      db))
        except Exception as e:
            unresolved = str(e).splitlines()[-1][:70] if str(e) \
                else type(e).__name__
    skip_args = [a for name in skip for a in ("-T", name)]
    steps = [
        f"pg_dump -h {s.host} -p {s.port} -U {s.user} -d {db} -Fd"
        f" -j {workers} --data-only -f {outdir}"
        + ("".join(f" -T {n}" for n in skip) if skip else ""),
        _truncate_step(hop, db),
        f"pg_restore -h {t.host} -p {t.port} -U {t.user}"
        f" -d {hop.target_db(db)}"
        f" --data-only --disable-triggers -j {workers} {outdir}",
    ]
    if unresolved:
        steps.insert(1, _unresolved_note(unresolved))
    elif skip:
        steps.insert(1, f"# {len(skip)} tables the hop excludes are not"
                        " dumped at all")
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)
    env_t = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    import shutil
    shutil.rmtree(outdir, ignore_errors=True)
    _pgdump_dump(hop, db, s, workers, outdir, log, skip_args)
    _pg_truncate_target(hop, db, log)
    with _IndexWindow(hop, db, workers, log):
        _pgdump_restore(hop, db, workers, outdir, env_t, log)
    shutil.rmtree(outdir, ignore_errors=True)
    return steps


def _pgdump_dump(hop, db, s, workers, outdir, log, skip_args=()):
    _sh(["pg_dump", "-h", s.host, "-p", str(s.port), "-U", s.user,
         "-d", db, "-Fd", "-j", str(workers), "--data-only",
         "-f", str(outdir), *skip_args],
        {"PGPASSWORD": s.password, "PGCONNECT_TIMEOUT": "15"}, log)


def _pgdump_restore(hop, db, workers, outdir, env_t, log):
    """Restore into the *target's* name for the database.

    It used the source's name, while the emptying and the index window
    already used `target_db()` - so a hop whose `db_map` renames the
    database emptied the right one and loaded into another.
    """
    t = hop.target
    try:
        _sh(["pg_restore", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", hop.target_db(db), "--data-only", "--disable-triggers",
             "-j", str(workers), str(outdir)], env_t, log)
    except RuntimeError as e:
        # newer pg_dump emits SETs older servers reject; pg_restore exits 1
        # on those even when all rows landed - migkit check is the judge
        if "errors ignored on restore" not in str(e):
            raise
        if log:
            log("pg_restore ignored version-mismatch SET statements,"
                " data restored - verify with migkit check")


MY_INDEX_SQL = """
    select s.table_name, s.index_name,
           group_concat(concat('`', s.column_name, '`')
                        order by s.seq_in_index),
           max(s.non_unique) = 0,
           max(case when k.constraint_name is not null
                         and s.seq_in_index = 1 then 1 else 0 end)
      from information_schema.statistics s
      left join information_schema.key_column_usage k
        on k.table_schema = s.table_schema
       and k.table_name = s.table_name
       and k.column_name = s.column_name
       and k.referenced_table_name is not null
     where s.table_schema = %s
       and s.index_name <> 'PRIMARY'
       and s.table_name not like 'migkit%%'
     group by s.table_name, s.index_name"""


class _MyIndexWindow:
    """The MySQL half of moving indexes out of a bulk load's way.

    Measured on MySQL 8, 200,000 rows into a table with three secondary
    indexes: 1.150s with them in place against 0.303s + 0.511s as a bare load
    plus one ALTER - 1.41x. Less than PostgreSQL's 1.9x on the same shape, and
    still worth having.

    Two things are protected here that have no PostgreSQL equivalent. A unique
    index is enforcing something, as everywhere. And **an index that backs a
    foreign key cannot be dropped at all** - InnoDB refuses with "needed in a
    foreign key constraint" - so those are left alone rather than attempted
    and logged as failures.

    Same order as the PostgreSQL window, for the same reason: the definitions
    reach disk before anything is dropped, and the rebuild happens on the way
    out whether the load worked or raised.
    """

    def __init__(self, engine, hop, db, workers, log):
        self.eng, self.hop, self.db = engine, hop, db
        self.workers, self.log = workers, log
        self.dropped, self.ddl = [], {}

    def __enter__(self):
        from . import indexes as _ix
        ddb = self.hop.target_db(self.db) if hasattr(self.hop, "target_db") \
            else self.db
        try:
            rows = self.eng._q("dst", MY_INDEX_SQL, (ddb,))
        except Exception:
            return self
        rows = _outside_exclusion(self.hop, self.db, list(rows), self.log,
                                  qualifier=self.db)
        triples = []
        for tbl, name, cols, is_unique, backs_fk in rows:
            key = f"{tbl}.{name}"
            if int(backs_fk or 0):
                # InnoDB needs an index whose *leftmost* column is the
                # referencing one, and refuses to drop it: "needed in a
                # foreign key constraint". An index that merely contains the
                # column further along does not satisfy the key and can go -
                # excluding those too left a free win on the table.
                #
                # The prediction is a shortcut, not the safety net: the drop
                # is attempted inside a try, so if InnoDB disagrees the index
                # simply stays and the load runs with it.
                continue
            ddl = (f"ALTER TABLE `{ddb}`.`{tbl}` ADD INDEX `{name}`"
                   f" ({cols})")
            triples.append((key, ddl, bool(is_unique)))
        drop, ddl = _ix.plan(triples)
        if not drop:
            return self
        where = self.hop.report_dir(self.db) / "dropped-indexes.json"
        if not _ix.saved(where, ddl):
            if self.log:
                self.log("could not save the index definitions, so none were"
                         " dropped - the load runs with them in place")
            return self
        for key in drop:
            tbl, name = key.split(".", 1)
            try:
                self.eng._q("dst", f"ALTER TABLE `{ddb}`.`{tbl}`"
                                   f" DROP INDEX `{name}`")
                self.dropped.append(key)
                self.ddl[key] = ddl[key]
            except Exception as e:
                if self.log:
                    self.log(f"could not drop {key}, leaving it:"
                             f" {str(e)[:80]}")
        if self.log and self.dropped:
            self.log(f"{len(self.dropped)} secondary indexes dropped for the"
                     f" load; definitions saved to {where}")
        return self

    def __exit__(self, *exc):
        from . import indexes as _ix
        rebuilt, failed = [], []
        for key in self.dropped:
            try:
                self.eng._q("dst", self.ddl[key])
                rebuilt.append(key)
            except Exception as e:
                failed.append(key)
                if self.log:
                    self.log(f"REBUILD FAILED for {key}: {str(e)[:100]}")
        if self.log:
            self.log(_ix.summary(self.dropped, rebuilt, failed))
        return False


def excluded_tables(hop, db, tables, qualifier="public"):
    """The hop's excluded tables as concrete `schema.table` names.

    One resolution for every mover, and the reason is that the alternative
    is two. `hop.exclude` is right-anchored fnmatch - `audit_log`,
    `public.audit_log`, `appdb.public.*`. `pg_dump -T` has its own pattern
    language and pgcopydb's filter file has none at all, so handing either
    of them the raw pattern means the dump excludes one set, the other
    mover excludes a second, and `check` - which asks `hop.excluded()` -
    uses a third. Resolving here through the same `hop.excluded()` the
    check uses is what keeps the three answers identical.

    `qualifier` is what an unqualified name is prefixed with. PostgreSQL
    tables arrive as `schema.table` already; MySQL's arrive bare, and
    Debezium wants them as `database.table`, so the caller says which.
    """
    if not getattr(hop, "exclude", None):
        return []
    out = []
    for ident in sorted(tables):
        parts = [p for p in str(ident).split(".") if p]
        if hop.excluded(db, *parts):
            out.append(".".join(parts) if len(parts) > 1
                       else f"{qualifier}.{parts[0]}")
    return out


def pgcopydb_filters(hop, db, tables):
    """`[exclude-table]` entries for what this hop already excludes, or None.

    `hop.exclude` is patterns - `audit_log`, `public.audit_log`,
    `appdb.public.*` - and pgcopydb wants concrete `schema.table` names. So
    the patterns are resolved here against the tables the source actually
    has, using `hop.excluded()` rather than a second reading of the same
    rules. A pattern that matches nothing produces nothing, which is the
    right answer for a filter file.

    Verified against pgcopydb 0.18 on a live source with three tables and
    `public.audit_log` excluded: `pgcopydb list tables --filters` returned
    two, and the same command without the file returned three.

    Until now `exclude` was honoured by the check and ignored by the mover,
    so an excluded table was copied and then never looked at. Filtering it
    here means it is not carried at all.
    """
    out = excluded_tables(hop, db, tables)
    if not out:
        return None
    return "[exclude-table]\n" + "\n".join(out) + "\n"


def mydumper_defaults(hop, db):
    """The hop's row filters as a mydumper defaults file, or None.

    mydumper's `--where` is one predicate for the whole dump; the per-table
    form lives in a config file, one section per table. Verified against
    mydumper v1.0.5 on a live pair - a rule on `orders` dumped 2 of its 3
    rows and left `people`, which no rule names, at all 2:

        [`appdb`.`orders`]
        where = region = 'apac'

        appdb.orders  INSERT INTO `orders` VALUES(1,"apac"),(3,"apac");
        appdb.people  INSERT INTO `people` VALUES(1,"a"),(2,"b");

    Pushing the predicate down here rather than filtering afterwards means
    the rows never cross the wire, and it is the same predicate the check
    compares with - one mapping, read by both.

    A rule naming another database is skipped rather than applied to this
    one: `where` keys are right-anchored like every other name in the hop,
    so `orders` means this database's, and `other.orders` does not.
    """
    rules = (getattr(hop, "mapping", None) or {}).get("where") or {}
    lines = ["[mydumper]"]
    for key, predicate in sorted(rules.items()):
        parts = [p for p in str(key).split(".") if p]
        if len(parts) > 1 and parts[0] != db:
            continue
        lines.append(f"[`{db}`.`{parts[-1]}`]")
        lines.append(f"where = {predicate}")
    return "\n".join(lines) + "\n" if len(lines) > 1 else None


@functools.lru_cache(maxsize=None)
def _long_options(program):
    """The long options this installed build of `program` really has.

    Read from the option column of its own `--help`, not from the whole
    text: mydumper 1.0.5 names `--overwrite-tables` inside the description
    of `--overwrite-unsafe`, and the binary answers `Unknown option
    --overwrite-tables`. Measured, the column holds 136 options for mydumper
    and 99 for myloader, and that one is in neither.
    """
    p = subprocess.run([program, "--help"], env=tool_env(None), text=True,
                       capture_output=True)
    return frozenset(re.findall(
        r"(?m)^\s+(?:-\w,\s+)?(--[a-z0-9][a-z0-9-]*)(?=\s{2,}|$)",
        (p.stdout or "") + (p.stderr or "")))


def tool_flag(program, *spellings):
    """The first of `spellings` the installed build accepts.

    Flags get renamed between releases, and the old spelling does not
    degrade: mydumper's `--trx-consistency-only` became `--trx-tables`,
    myloader's `--purge-mode` is gone outright, and handed either one the
    program stops at option parsing. So the spelling is asked of the binary
    that is about to run rather than remembered from whichever build was
    installed when the line was written.
    """
    have = _long_options(program)
    for s in spellings:
        if s in have:
            return s
    raise SystemExit(
        "the installed bulk copy program accepts none of"
        f" {', '.join(spellings)}, which this move needs -"
        " `migkit doctor --install` puts a version in place that does")


def _my_truncate_target(hop, db, log=None):
    """Empty the target's tables before a data-only MySQL load.

    The loader used to do this itself, as `--purge-mode TRUNCATE`. The
    build installed here has no such option; its modes moved to
    `--drop-table`, and measured on a data-only dump neither
    `--drop-table=TRUNCATE` nor `--drop-table=DELETE` empties anything - the
    load appends, and the rows the target already had stay beside the new
    copy. So the target is emptied here, as the PostgreSQL paths already do
    it, and through the same `excluded_tables()`.

    `foreign_key_checks = 0` for the session doing it, because MySQL will
    not truncate a table another table references - it refuses outright:

        ERROR 1701 (42000): Cannot truncate a table referenced in a
        foreign key constraint

    With the checks off it empties exactly the tables it is given, and no
    others. Measured: an excluded table referencing a replaced one kept both
    its rows - which is why this path needs no counterpart to the PostgreSQL
    cascade refusal. One connection throughout, because the setting belongs
    to the session and `_q` opens a new one per call.
    """
    from .engines.mysql import MySQLEngine
    eng = MySQLEngine(hop)
    names = eng._all_tables("dst", db)
    skip = set(excluded_tables(hop, db, names, qualifier=db))
    keep = [n for n in names if f"{db}.{n}" not in skip]
    if not keep:
        return []
    ddb = eng._quote_ident(eng._d("dst", db))
    stmts = ["set foreign_key_checks = 0"] + [
        f"truncate table {ddb}.{eng._quote_ident(n)}" for n in keep]
    if log:
        log("$ " + "; ".join(stmts))
    conn = eng._conn("dst")
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt)
    finally:
        conn.close()
    return keep


def _mydumper_commands(hop, db, workers, outdir, cnf=None, omit=None):
    """The dump and load command lines, built once for the plan and the run.

    Neither carries a password. It used to go on as `-p<secret>`, attached,
    which this build does not accept: the characters after `-p` were read
    as more short options - `-ptest` is `-p -t -e -s -t`, and the run died
    on `Error parsing option -t`, naming a flag nobody typed. And `_sh`
    logs a command line as given, so every run printed the source password
    before it failed; measured with a distinctive one, a single `move --go`
    put it on the console once. Both programs read `MYSQL_PWD` - measured,
    and a wrong one exits 1 - the way the PostgreSQL paths read
    `PGPASSWORD`, so the secret reaches neither argv, the log, nor the
    process list.

    `-B` on the loader is the *target's* name for the database, which the
    hop's `db_map` may make different from the source's.
    """
    from .engines.mysql import MySQLEngine
    s, t = hop.source, hop.target
    dump = ["mydumper", "-h", s.host, "-P", str(s.port), "-u", s.user,
            "-B", db, "-o", str(outdir), "--threads", str(workers),
            "--no-schemas",
            tool_flag("mydumper", "--trx-tables", "--trx-consistency-only")]
    if cnf:
        dump += ["--defaults-file", str(cnf)]
    if omit:
        dump += [tool_flag("mydumper", "--omit-from-file"), str(omit)]
    load = ["myloader", "-h", t.host, "-P", str(t.port), "-u", t.user,
            "-B", MySQLEngine(hop)._d("dst", db), "-d", str(outdir),
            "--threads", str(workers)]
    return dump, load


def mydumper_move(hop, db, workers, go, log):
    """Dump, then empty the target, then load - in that order.

    The target is emptied only once a complete dump is on disk. Emptying it
    first, and then finding the source unreachable, hands back a target
    with nothing in it and nothing to load.
    """
    outdir = hop.report_dir(db) / "mydumper"
    filters = mydumper_defaults(hop, db)
    # beside the dump directory, not inside it: myloader is pointed at
    # that directory and has no reason to meet a file it does not read
    cnf = hop.report_dir(db) / "mydumper-filters.cnf"
    omit = hop.report_dir(db) / "omit-tables.txt"
    skip, unresolved = [], ""
    if getattr(hop, "exclude", None):
        from .engines.mysql import MySQLEngine
        try:
            skip = excluded_tables(hop, db, MySQLEngine(hop)._all_tables(
                "src", db), qualifier=db)
        except Exception as e:
            unresolved = str(e).strip().splitlines()[0][:100] if str(e) \
                else type(e).__name__
    dump, load = _mydumper_commands(hop, db, workers, outdir,
                                    cnf if filters else None,
                                    omit if skip else None)
    steps = [" ".join(dump)
             + ("  # row filters from the hop's mapping" if filters else ""),
             _truncate_step(hop, db),
             " ".join(load)]
    if skip:
        steps.insert(1, f"# {len(skip)} tables the hop excludes are not"
                        " dumped at all")
    if unresolved:
        steps.insert(1, _unresolved_note(unresolved))
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)
    import shutil
    shutil.rmtree(outdir, ignore_errors=True)
    if filters:
        cnf.write_text(filters)
        cnf.chmod(0o600)
    if skip:
        omit.write_text("".join(f"{n}\n" for n in skip))
    _sh(dump, {"MYSQL_PWD": hop.source.password}, log)
    from .engines.mysql import MySQLEngine
    _my_truncate_target(hop, db, log)
    with _MyIndexWindow(MySQLEngine(hop), hop, db, workers, log):
        _sh(load, {"MYSQL_PWD": hop.target.password}, log)
    shutil.rmtree(outdir, ignore_errors=True)
    return steps


def pgloader_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    loadfile = hop.report_dir(db) / "pgloader.load"
    body = f"""LOAD DATABASE
  FROM mysql://{s.user}:{quote(s.password, safe='')}@{s.host}:{s.port}/{db}
  INTO postgresql://{t.user}:{quote(t.password, safe='')}@{t.host}:{t.port}/{db}
WITH data only, workers = {workers}, concurrency = {min(workers, 4)},
     on error stop
ALTER SCHEMA '{db}' RENAME TO 'public';
"""
    loadfile.write_text(body)
    loadfile.chmod(0o600)
    steps = [f"pgloader {loadfile}   # data only, mysql -> postgres"]
    if not go:
        return steps + ["# dry-run, review the load file then add --go"]
    _sh(["pgloader", str(loadfile)], None, log)
    return steps


def _mongo_uri(ep):
    auth = (f"{quote(ep.user, safe='')}:{quote(ep.password, safe='')}@"
            if ep.user else "")
    hosts = ep.options.get("hosts") or f"{ep.host}:{ep.port}"
    uri = f"mongodb://{auth}{hosts}/"
    extra = ep.options.get("uri_options", "")
    if extra:
        uri += "?" + extra
    return uri


def mongodump_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    steps = [
        f"mongodump --uri=<src> --db={db} --archive"
        f" | mongorestore --uri=<dst> --archive --drop"
        f" --nsInclude='{db}.*' --numParallelCollections={workers}",
    ]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    dump = subprocess.Popen(
        ["mongodump", f"--uri={_mongo_uri(s)}", f"--db={db}", "--archive",
         "--quiet"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=tool_env())
    restore = subprocess.Popen(
        ["mongorestore", f"--uri={_mongo_uri(t)}", "--archive", "--drop",
         f"--nsInclude={db}.*", f"--numParallelCollections={workers}"],
        stdin=dump.stdout, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env=tool_env())
    dump.stdout.close()
    _, err_r = restore.communicate()
    _, err_d = dump.communicate()
    if dump.returncode or restore.returncode:
        raise RuntimeError((err_d + err_r).decode()[-500:])
    if log:
        log(f"{db}: mongodump | mongorestore complete")
    return steps



#: What the loading connection asks the server for, so that triggers on the
#: target do not rewrite the rows as they land.
#:
#: Measured, with a real `migkit move --mode full --go`: a `BEFORE INSERT`
#: trigger setting `updated_at := now()` on the target turned rows carrying
#: `2001-01-01` and `2002-02-02` into today's timestamp, and migkit printed
#: `bulk copy complete`. With this on the target URI the same move landed
#: `2001-01-01` and `2002-02-02` untouched.
#:
#: A **connection** option rather than `ALTER TABLE ... DISABLE TRIGGER`,
#: deliberately: disabling triggers is a change to the target that outlives
#: a crash, and a target left with disabled triggers is the exact failure
#: `check --deep` already reports. This lasts as long as the connection and
#: not one moment longer. It is what migkit's repair path has always done;
#: the mover was the inconsistent one.
QUIET_TRIGGERS = "?options=-c%20session_replication_role%3Dreplica"

PGCOPYDB_IMAGE = "dimitri/pgcopydb:latest"
_PGCOPYDB_OK = None


def pgcopydb_available():
    """Whether the version-matched pgcopydb container can be run here.

    Deliberately the image and not a local binary. Homebrew's pgcopydb 0.18 is
    compiled against PostgreSQL 18 and emits `SET transaction_timeout = 0` on
    the target; PostgreSQL 16 has never heard of that parameter, so the
    statement fails, the transaction aborts, and every later statement in it is
    refused - measured, a whole-database clone moved zero rows while reporting
    each rejection separately. The published image is compiled against 16 and
    states its compatible range, which is the difference between a tool that
    works and one that reports success while moving nothing.
    """
    return bool(pgcopydb_runner())


_PGCOPYDB_HOW = None


def pgcopydb_runner():
    """How to run pgcopydb here: the image, the local binary, or not at all.

    The image is preferred because its build matches the server range it
    states. The local binary is accepted for `copy table-data` because the
    failure the image exists to avoid was re-measured and is narrower than
    it looked: Homebrew's 0.18, built against PostgreSQL 18, does emit `SET
    transaction_timeout = 0` and PostgreSQL 16 does reject it - the error
    appears in the server log - and yet 50,000 and then 2,000,000 rows
    arrived through `copy table-data` all the same. `clone` is the
    subcommand that dies on it, and migkit does not use `clone`.

    That is worth measuring against: on this machine pgcopydb moved a
    555 MB table in 4.1 s against the dump path's 11.7 s, and 2,093 MB of
    large objects in 18.2 s against 93.6 s. What makes taking the binary
    safe rather than hopeful is the guard that runs afterwards: a move that
    reports success and leaves the target empty is refused by
    `Engine.moved_nothing`, so the failure this note describes cannot pass
    as a completed migration.
    """
    global _PGCOPYDB_HOW
    if _PGCOPYDB_HOW is not None:
        return _PGCOPYDB_HOW
    how = ""
    if which("docker"):
        try:
            p = run(["docker", "image", "inspect", PGCOPYDB_IMAGE],
                    check=False, timeout=20)
            if p.returncode == 0:
                how = "image"
        except Exception:
            how = ""
    if not how and which("pgcopydb"):
        how = "local"
    # cached: `pick` is called per table, and starting a docker client each
    # time to ask the same question would cost more than the answer is worth
    _PGCOPYDB_HOW = how
    return how


def pgcopydb_move(hop, db, workers, go, log):
    """Parallel data-only copy through pgcopydb, source straight to target.

    `copy table-data` and not `clone`, and the difference matters. `clone`
    restores the schema as well and refuses a target that already has objects
    - measured, it exits with `clone process has terminated`. migkit prepares
    the target schema in its own step before any data moves, so by the time a
    mover runs the target is never empty, and `clone` can never be the right
    subcommand here.

    What that leaves, against `pg_dump -Fd -j | pg_restore`: the tables are
    copied in parallel straight from source to target with no intermediate
    directory to write out and read back. What it does *not* leave, precisely
    because the schema is already in place: the indexes exist, so they are
    maintained during the load rather than built after it - which is the
    larger of the two wins and belongs to `clone` alone.

    So this is faster than the dump path, and not by as much as pgcopydb is
    capable of. Saying which is better than implying the rest.

    The network is the host's by default, so the endpoints resolve exactly as
    they do for migkit itself. `MIGKIT_PGCOPYDB_NETWORK` overrides it - an
    environment variable rather than a flag, so it stays out of the command
    surface, the same way `MIGKIT_MOVER` does.

    **The target has to be empty.** Unlike `pgdump_move`, which truncates the
    user tables it is about to fill, `clone` restores the schema as well and
    refuses to run over objects that already exist - measured, it exits with
    `clone process has terminated`. That refusal is the right behaviour and is
    left as it is: `--drop-if-exists` would make a second run destroy a target
    somebody may have been using, which is not a decision a mover should make
    on its own.
    """
    import os
    from urllib.parse import quote
    s, t = hop.source, hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    src = (f"postgresql://{s.user}:{quote(s.password or '', safe='')}"
           f"@{s.host}:{s.port}/{db}")
    dst = (f"postgresql://{t.user}:{quote(t.password or '', safe='')}"
           f"@{t.host}:{t.port}/{ddb}{QUIET_TRIGGERS}")
    net = os.environ.get("MIGKIT_PGCOPYDB_NETWORK", "host")
    how = pgcopydb_runner()
    # Excluded tables, resolved against what the source actually has.
    # Asking the source is what turns a pattern into the concrete
    # `schema.table` names pgcopydb wants; a source that cannot be reached
    # yet leaves the filter off rather than guessing, and says so in the
    # steps instead of quietly copying what the hop excludes.
    filters_path, filters_note = None, ""
    if getattr(hop, "exclude", None):
        try:
            from .engines.postgres import PostgresEngine
            tables = PostgresEngine(hop).neutral_tables("src", db)
            text = pgcopydb_filters(hop, db, tables)
        except Exception as e:
            text = None
            filters_note = ("# could not list the source's tables, so the"
                            f" hop's exclude list is not pushed down: "
                            f"{str(e).splitlines()[-1][:70]}")
        if text:
            filters_path = hop.report_dir(db) / "pgcopydb-filters.ini"
            filters_path.write_text(text)
            filters_note = (f"# {text.count(chr(10)) - 1} tables excluded by"
                            " the hop are filtered out at the source")
    if how == "local":
        # its own directory per run. pgcopydb keeps its state - including the
        # exported snapshot - under /tmp/pgcopydb by default, so a second run
        # finds the first one's snapshot and dies on it: measured,
        # `FATAL Failed to use given --snapshot "00000003-00000048-1"`. The
        # container path never hit this because each container brought its
        # own filesystem; the binary shares the host's.
        import tempfile
        work = tempfile.mkdtemp(prefix="migkit-pgcopydb-")
        cmd = ["pgcopydb", "copy", "table-data",
               "--table-jobs", str(workers), "--dir", work,
               "--source", src, "--target", dst]
        if filters_path:
            cmd += ["--filters", str(filters_path)]
    else:
        cmd = ["docker", "run", "--rm", "--network", net,
               "-e", f"PGCOPYDB_SOURCE_PGURI={src}",
               "-e", f"PGCOPYDB_TARGET_PGURI={dst}"]
        if filters_path:
            cmd += ["-v", f"{filters_path}:/tmp/migkit-filters.ini:ro"]
        cmd += [PGCOPYDB_IMAGE, "pgcopydb", "copy", "table-data",
                "--table-jobs", str(workers)]
        if filters_path:
            cmd += ["--filters", "/tmp/migkit-filters.ini"]
    # the password sits *before* the @, so splitting there and keeping the
    # front half kept the secret and threw the host away - the printed steps
    # are what an operator pastes into a ticket
    import re as _re
    shown = [_re.sub(r"(://[^:/@]+:)[^@]*@", r"\1***@", c) for c in cmd]
    steps = [_truncate_step(hop),
             "# parallel table copy, source to target, no intermediate file"]
    if filters_note:
        steps.append(filters_note)
    steps.append(" ".join(shown))
    if not go:
        return steps + ["# dry-run, add --go to execute"]

    # Having the image is not the same as being able to reach the databases
    # from inside it: the container has its own network view, and on a laptop
    # `--network host` is the VM's host, not this one. `ping` is the tool's
    # own answer to that question, and asking it first turns a failed move
    # into a fallback rather than an outage.
    if how == "local":
        # `ping` does not take `--dir` - it answers with its usage if you
        # give it one, which reads as a connectivity failure and sends the
        # move down the fallback path for no reason
        ping = ["pgcopydb", "ping", "--source", src, "--target", dst]
    else:
        ping = cmd[:cmd.index(PGCOPYDB_IMAGE) + 1] + ["pgcopydb", "ping"]
    try:
        _sh(ping)
    except Exception as e:
        if log:
            where = ("from its container"
                     f" (network={net})" if how != "local" else "")
            log(f"pgcopydb cannot reach both endpoints {where}:"
                f" {str(e).splitlines()[-1][:120]}")
            log("falling back to the pg_dump path")
        return pgdump_move(hop, db, workers, go, log)

    _pg_truncate_target(hop, db, log)
    # logged through the redacted form, never the raw argv: `_sh` would echo
    # the command as given, and the command carries both passwords
    if log:
        log("$ " + " ".join(shown[1:]))
    with _IndexWindow(hop, db, workers, log):
        _sh(cmd)
    return steps


def follow_selected():
    """Which PostgreSQL CDC path `move --mode cdc` should take.

    An environment variable rather than a flag, the way `MIGKIT_MOVER` is,
    because it is not a choice about *what* to do - both paths replicate the
    same changes - but about which one this network allows. Unset keeps the
    path every existing hop already takes.
    """
    import os
    want = os.environ.get("MIGKIT_CDC", "").strip().lower()
    if not want:
        return ""
    if want not in ("native", "follow"):
        raise SystemExit(
            f"MIGKIT_CDC={want} is not one of native, follow."
            " native = the database's own replication, which needs the"
            " target to dial the source; follow = a leg driven from where"
            " migkit runs, which connects out to both sides instead")
    return want


def follow_dir(hop, db):
    """Where the CDC leg keeps its state, and why it is not a temp dir.

    pgcopydb confirms flush to the *source* as soon as the receive side has
    written a change to this directory - so the source releases the WAL for
    changes the target has not applied yet, and this directory becomes the
    only copy. Measured on 0.18 with apply held back: the slot's
    `confirmed_flush_lsn` had advanced to the head with 56 bytes of WAL
    retained, the target held 0 rows of 200, and everything the source could
    tell you said "caught up". Put it under the hop's report directory,
    where it survives a reboot and a `/tmp` sweep.
    """
    return hop.report_dir(db) / "follow"


def pgcopydb_follow(hop, db, go, log=None, timeout=None):
    """Carry changes to the target from where migkit runs, and stop at a
    position that can be named.

    `CREATE SUBSCRIPTION` makes the *target* dial the source, which plenty
    of migrations cannot do at all, and leaves the source's password in
    `pg_subscription` when it can. This runs on the operator's machine,
    opens both connections itself, and writes no credential anywhere.

    Three things measured on pgcopydb 0.18 that a bare `shell out to
    follow` would get wrong, and that migkit does here so the operator
    never meets them:

    * **Apply is off by default.** Without `stream sentinel set apply`
      nothing is ever written to the target; the log says only "Waiting
      until the pgcopydb sentinel apply is enabled" and the run looks
      healthy. The sentinel lives in `--dir`, not on the source - checked:
      the source gained no schema and no table, only a publication, which
      is the same object the native path already creates.
    * **It reports progress the target does not have.** `replay_lsn` came
      back equal to `write_lsn` - fully replayed - while the target held
      zero rows. So the run is not judged by pgcopydb's own sentinel.
    * **`--endpos` really does end it**, which is the thing no other CDC
      path here offers: a stop at a position, rather than a stop when
      somebody notices. Measured ~2 minutes from setting it to exit.

    What it is *not* judged by either is `applied_lsn >= endpos`. The
    target's origin records the LSN of the last applied *transaction*, and
    an endpos is a WAL position that is usually past it - measured, endpos
    0/156D578 against a final origin of 0/1567D28, on a run that copied
    every row correctly. Waiting for that comparison would never return.
    The end of the run is the process exiting; the proof is `migkit check`.
    """
    import os
    import subprocess as _sp

    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    origin, slot = eng.follow_origin(db), eng.follow_slot(db)
    d = follow_dir(hop, db)
    s, t = hop.source, hop.target
    src = (f"postgresql://{s.user}:{quote(s.password or '', safe='')}"
           f"@{s.host}:{s.port}/{db}")
    dst = (f"postgresql://{t.user}:{quote(t.password or '', safe='')}"
           f"@{t.host}:{t.port}/{hop.target_db(db)}")
    shown = (f"pgcopydb follow --dir {d} --slot-name {slot}"
             f" --create-slot --origin {origin} --plugin pgoutput"
             " --not-consistent")
    steps = [
        shown,
        f"pgcopydb stream sentinel set apply --dir {d}"
        "   (without this nothing is ever applied)",
        f"pgcopydb stream sentinel set endpos --current --dir {d}"
        "   (the provable stop)",
        f"state kept in {d} - the source releases WAL as soon as pgcopydb"
        " has written it there, so that directory is the only copy until"
        " the target has it",
        f"changes from before the slot {slot} existed are not carried;"
        " the bulk copy and migkit check are what cover those",
    ]
    if not go:
        return steps
    if pgcopydb_runner() != "local":
        raise SystemExit(
            "this CDC path needs a component that is not installed"
            " on this machine: it runs for the length of the catch-up and"
            f" keeps its state in {d}. migkit doctor --install puts it in"
            " place, or use MIGKIT_CDC=native, which needs no component"
            " here but does need the target to reach the source")
    d.mkdir(parents=True, exist_ok=True)
    env = tool_env({"PGCOPYDB_SOURCE_PGURI": src, "PGCOPYDB_TARGET_PGURI": dst})
    out = d / "follow.log"
    if log:
        log(shown)
    with out.open("ab") as fh:
        proc = _sp.Popen(
            ["pgcopydb", "follow", "--dir", str(d), "--slot-name", slot,
             "--create-slot", "--origin", origin, "--plugin", "pgoutput",
             "--not-consistent"], stdout=fh, stderr=fh, env=env)
    try:
        _follow_sentinel(d, env, proc, log)
        limit = timeout or int(os.environ.get("MIGKIT_FOLLOW_TIMEOUT", "1800"))
        try:
            proc.wait(timeout=limit)
        except _sp.TimeoutExpired:
            raise SystemExit(
                f"the CDC leg did not reach its end position in {limit}s."
                f" It is still running; its log is {out}. Nothing has been"
                " marked as caught up - raise MIGKIT_FOLLOW_TIMEOUT if the"
                " backlog is simply larger than that")
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except _sp.TimeoutExpired:
                proc.kill()
    if proc.returncode:
        raise SystemExit(f"the CDC leg exited {proc.returncode} before it"
                         f" finished catching up; its log is {out}")
    steps.append(f"target applied up to {eng.applied_lsn(db) or 'nothing'}"
                 " - run migkit check to prove the rows, an LSN is not a"
                 " row count")
    return steps


def follow_teardown(hop, db, go, log=None):
    """Take back what the CDC leg left on both servers.

    The slot is the one that matters: an abandoned logical slot pins WAL
    until the source runs out of disk, which is a source outage caused by
    the migration tooling (E5). pgcopydb also creates a publication named
    after the slot, and the origin lives on the target - all three are
    named by migkit, so all three can be named here.
    """
    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    origin, slot = eng.follow_origin(db), eng.follow_slot(db)
    plan = [
        ("src", f"select pg_drop_replication_slot('{slot}')"
                f" from pg_replication_slots where slot_name = '{slot}'"),
        ("src", f"drop publication if exists {slot}"),
        ("dst", f"select pg_replication_origin_drop('{origin}')"
                " from pg_replication_origin"
                f" where roname = '{origin}'"),
    ]
    steps = [f"{side}: {sql}" for side, sql in plan]
    if not go:
        return steps
    for side, sql in plan:
        d = db if side == "src" else hop.target_db(db)
        try:
            eng._psql(side, d, sql)
        except RuntimeError as e:
            # each of the three is independently absent on a leg that never
            # ran, or that was torn down already; saying which one refused
            # beats stopping before the slot is dropped
            steps.append(f"{side}: could not run `{sql[:40]}...`:"
                         f" {str(e).splitlines()[-1][:120]}")
        else:
            if log:
                log(f"{side}: {sql}")
    left = follow_dir(hop, db)
    steps.append(f"state directory left in place: {left} - it is the record"
                 " of what was carried, and removing it is the operator's"
                 " call")
    return steps


def _follow_sentinel(d, env, proc, log=None):
    """Turn the apply on and name the stop, once the run has made its
    sentinel. `follow` creates it at startup, so this cannot be done before
    the process exists - which is exactly why it is migkit's job and not a
    line in a runbook somebody forgets."""
    import subprocess as _sp
    import time as _t
    end = _t.monotonic() + 120
    while _t.monotonic() < end:
        if proc.poll() is not None:
            raise SystemExit("the CDC leg exited before it was told to"
                             " apply anything, so nothing reached the"
                             f" target; its log is {d / 'follow.log'}")
        got = _sp.run(["pgcopydb", "stream", "sentinel", "get", "--dir",
                       str(d)], capture_output=True, text=True, env=env)
        if got.returncode == 0 and "apply" in got.stdout:
            break
        _t.sleep(2)
    else:
        raise SystemExit("the CDC leg never became ready to be told to apply;"
                         f" its log is {d / 'follow.log'}")
    for args, why in ((["set", "apply"], "apply"),
                      (["set", "endpos", "--current"], "end position")):
        p = _sp.run(["pgcopydb", "stream", "sentinel"] + args + ["--dir",
                    str(d)], capture_output=True, text=True, env=env)
        if p.returncode:
            raise SystemExit(f"could not set the {why} on the CDC leg:"
                             f" {(p.stderr or p.stdout)[-200:]}")
        if log:
            log(f"pgcopydb stream sentinel {' '.join(args)}")


def stream_codegen(hop, dbs, engine):
    """Write the managed streaming pipeline for this hop: a single-broker
    log plus a connector runtime with a source and an upsert sink, sized for
    one hop and ready to start. migkit reimplements none of it - it wires
    proven components together and owns the lifecycle, so the operator drives
    everything through migkit."""
    out = hop.report_dir() / "stream"
    out.mkdir(parents=True, exist_ok=True)
    s, t = hop.source, hop.target
    src_is_mysql = engine in ("mysql", "hetero")
    connector_class = ("io.debezium.connector.mysql.MySqlConnector"
                       if src_is_mysql
                       else "io.debezium.connector.postgresql.PostgresConnector")
    name = f"migkit-{hop.name}"
    source = {
        "name": f"{name}-source",
        "config": {
            "connector.class": connector_class,
            "database.hostname": s.host,
            "database.port": str(s.port),
            "database.user": s.user,
            "database.password": s.password,
            "topic.prefix": name,
            "snapshot.mode": "initial",
            # A signal channel, so one table can be snapshotted again
            # without stopping the stream. Debezium's `source` channel
            # reads the request from a table it expects to find in the
            # source database; migkit writes to the source nowhere, and
            # the broker is already in this compose file, so the Kafka
            # channel buys the same capability without breaking that.
            "signal.enabled.channels": "kafka",
            "signal.kafka.topic": f"{name}-signal",
            "signal.kafka.bootstrap.servers": "redpanda:9092",
            "signal.kafka.groupId": f"{name}-signal",
        },
    }
    if src_is_mysql:
        source["config"].update({
            "database.include.list": ",".join(dbs),
            "database.server.id": "184054",
            "schema.history.internal.kafka.bootstrap.servers":
                "redpanda:9092",
            "schema.history.internal.kafka.topic": f"{name}-history",
        })
    else:
        source["config"].update({
            "database.dbname": dbs[0] if dbs else "postgres",
            "plugin.name": "pgoutput",
            "slot.name": name.replace("-", "_"),
        })
    # what the hop already excludes, kept out of the stream as well. The
    # same resolution the bulk movers and `check` use, so a table nobody
    # wants is not carried by the CDC leg either.
    if getattr(hop, "exclude", None):
        try:
            from .engines.mysql import MySQLEngine
            from .engines.postgres import PostgresEngine
            one = dbs[0] if dbs else "postgres"
            if src_is_mysql:
                tables = MySQLEngine(hop).neutral_tables("src", one)
                skip = debezium_exclude(hop, one, tables, one)
            else:
                tables = PostgresEngine(hop).neutral_tables("src", one)
                skip = debezium_exclude(hop, one, tables, "public")
        except Exception:
            # a source that cannot be listed leaves the stream unfiltered
            # rather than guessing at names; the README says so
            skip = None
        if skip:
            source["config"]["table.exclude.list"] = skip

    dst_is_pg = engine in ("postgres", "hetero")
    jdbc = (f"jdbc:postgresql://{t.host}:{t.port}/" if dst_is_pg
            else f"jdbc:mysql://{t.host}:{t.port}/")
    sink = {
        "name": f"{name}-sink",
        "config": {
            "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
            "topics.regex": f"{name}\\..*",
            "connection.url": jdbc + (dbs[0] if dbs else ""),
            "connection.username": t.user,
            "connection.password": t.password,
            "insert.mode": "upsert",
            "delete.enabled": "true",
            "primary.key.mode": "record_key",
            "schema.evolution": "basic",
        },
    }
    compose = """services:
  redpanda:
    image: redpandadata/redpanda:latest
    command: redpanda start --overprovisioned --smp 1 --memory 1G
      --node-id 0 --kafka-addr PLAINTEXT://0.0.0.0:9092
      --advertise-kafka-addr PLAINTEXT://redpanda:9092
  connect:
    image: quay.io/debezium/connect:3.0
    depends_on: [redpanda]
    ports: ["8083:8083"]
    environment:
      BOOTSTRAP_SERVERS: redpanda:9092
      GROUP_ID: migkit
      CONFIG_STORAGE_TOPIC: migkit_connect_configs
      OFFSET_STORAGE_TOPIC: migkit_connect_offsets
      STATUS_STORAGE_TOPIC: migkit_connect_statuses
"""
    readme = f"""# Streaming pipeline for hop {hop.name} (generated by migkit)

migkit owns this stack. Drive it with migkit, not by hand:

  start      migkit move {hop.name} --mode cdc --go
  progress   migkit watch {hop.name}
  verify     migkit check {hop.name}
             migkit watch {hop.name} --verify --delta
  tear down  migkit move {hop.name} --mode cdc --drop --go

These files hold credentials from hops.yaml - keep this directory private
(chmod 700, never commit). Tearing down also drops the replication slot /
binlog reader left on the source.

Third-party components are pinned in docker-compose.yml; see NOTICE for
licences and attribution.
"""
    import json as _json
    (out / "docker-compose.yml").write_text(compose)
    (out / "source-connector.json").write_text(
        _json.dumps(source, indent=2) + "\n")
    (out / "sink-connector.json").write_text(
        _json.dumps(sink, indent=2) + "\n")
    (out / "README.md").write_text(readme)
    for f in out.iterdir():
        f.chmod(0o600)
    out.chmod(0o700)
    return out


def _connect_api(method, path, body=None, port=8083, timeout=15):
    import json as _json
    import urllib.request
    data = _json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://localhost:{port}{path}", data=data, method=method,
        headers={"Content-Type": "application/json",
                 "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (_json.loads(raw) if raw else None)


def debezium_exclude(hop, db, tables, qualifier):
    """`table.exclude.list` for the connector, or None.

    Both connectors were asked what they accept rather than assumed: a
    config validate against Debezium 3.9 lists `table.exclude.list` and
    `table.include.list` for PostgreSQL and MySQL alike, so the hop's
    deny list maps straight across with no inversion.

    The values are **regular expressions** matched against the whole
    qualified name, so the dots are escaped - an unescaped `.` would make
    `public.orders` also match `publicXorders`, and quietly stop streaming
    a table nobody excluded.
    """
    names = excluded_tables(hop, db, tables, qualifier)
    if not names:
        return None
    return ",".join(n.replace(".", "\\.") for n in names)


def resnapshot_message(hop_name, tables, kind="blocking"):
    """The (topic, key, value) that asks a running connector to read a
    table again from scratch.

    Debezium calls this an ad-hoc snapshot. The key has to be the
    connector's `topic.prefix` - the signal topic is shared, and that is
    how a connector tells its own requests from another's.

    **Blocking, not incremental, and that is measured rather than
    preferred.** An incremental snapshot reads the table in chunks
    alongside the stream, which sounds strictly better, and Debezium
    refuses to run one here:

        Requested 'INCREMENTAL' snapshot of data collections
          '[public.orders]'
        Action execute-snapshot failed ...
        DebeziumException: Incremental snapshot is not properly
          configured, either sinalling data collection is not provided
          or connector-specific snapshotting not set

    The chunking is bracketed by watermark rows that Debezium writes into
    a signalling table **in the source database**, so incremental cannot
    work without write access to the source. migkit does not have it and
    should not ask for it. The blocking snapshot needs no such table, and
    on the same pipeline it ran: 50 rows re-emitted, the topic's high
    watermark moving 50 -> 100, `snapshot=BLOCKING snapshot_completed=true`.

    What blocking costs is honest and worth stating where the operator
    sees it: streaming pauses while the table is re-read. Pass
    `kind="incremental"` to ask for the other one anyway - it works the
    moment a signalling table exists and `signal.data.collection` names
    it, which is a decision about the source, not about migkit.

    Returns the pieces rather than sending them, so the caller can be
    tested without a broker.
    """
    prefix = f"migkit-{hop_name}"
    return (
        f"{prefix}-signal",
        prefix,
        {"type": "execute-snapshot",
         "data": {"data-collections": list(tables),
                  "type": kind.upper()}},
    )


def send_resnapshot(out, hop_name, tables, kind="blocking", log=None):
    """Put the request on the signal topic, through the compose file.

    The broker publishes no port to the host - only the connector's REST
    API on 8083 is exposed - so the message goes in the way migkit already
    drives the rest of this stack, with `docker compose exec`. Opening 9092
    to the host to avoid one exec would widen the pipeline's surface for
    the convenience of the tool, which is the wrong trade.
    """
    topic, key, value = resnapshot_message(hop_name, tables, kind)
    import json as _json
    line = f"{key}\t{_json.dumps(value)}\n"
    cmd = ["docker", "compose", "-f", str(out / "docker-compose.yml"),
           "exec", "-T", "redpanda",
           "rpk", "topic", "produce", topic, "-f", "%k\\t%v\\n"]
    if log:
        log(f"signalling {kind} snapshot of {', '.join(tables)}")
    p = run(cmd, check=False, input=line)
    if p.returncode:
        raise RuntimeError(
            f"could not reach the signal topic: {p.stderr[-200:]}")
    return topic


def stream_up(out, log=None):
    _sh(["docker", "compose", "-f", str(out / "docker-compose.yml"),
         "up", "-d"], log=log)


def stream_down(out, log=None):
    _sh(["docker", "compose", "-f", str(out / "docker-compose.yml"),
         "down", "-v"], log=log)


def stream_wait(port=8083, timeout=180, log=None):
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            code, _ = _connect_api("GET", "/", port=port, timeout=5)
            if code == 200:
                return True
        except Exception:
            pass
        if log:
            log("waiting for the connector runtime on :%d ..." % port)
        time.sleep(4)
    return False


def stream_register(out, port=8083, log=None):
    import json as _json
    import urllib.error
    for f in ("source-connector.json", "sink-connector.json"):
        cfg = _json.loads((out / f).read_text())
        name = cfg["name"]
        try:
            _connect_api("POST", "/connectors", cfg, port)
            if log:
                log(f"registered {name}")
        except urllib.error.HTTPError as e:
            if e.code == 409:  # already exists -> update config
                _connect_api("PUT", f"/connectors/{name}/config",
                             cfg["config"], port)
                if log:
                    log(f"updated {name}")
            else:
                raise


def stream_status(name, port=8083):
    """Summarize a connector + its tasks: RUNNING/FAILED per component."""
    try:
        _, body = _connect_api("GET", f"/connectors/{name}/status", port=port)
    except Exception as e:
        return f"{name}: unreachable ({str(e).splitlines()[-1][:50]})"
    conn = body.get("connector", {}).get("state", "?")
    tasks = [t.get("state", "?") for t in body.get("tasks", [])]
    return f"{name}: connector={conn} tasks={','.join(tasks) or 'none'}"


def run_via(via, hop, db, workers, go, log):
    fns = {"pgdump": pgdump_move, "pgcopydb": pgcopydb_move,
           "mydumper": mydumper_move, "pgloader": pgloader_move,
           "mongodump": mongodump_move}
    if via not in fns:
        # a mover nobody has taught this function about must stop here rather
        # than fall through to whatever happens to be first
        raise SystemExit(f"no bulk path named {via!r}")
    tools = {"pgdump": ("pg_dump", "pg_restore"),
             "pgcopydb": ("docker",),
             "mydumper": ("mydumper", "myloader"),
             "pgloader": ("pgloader",),
             "mongodump": ("mongodump", "mongorestore")}[via]
    missing = [t for t in tools if not which(t)]
    if missing:
        raise SystemExit(f"the {via} bulk path needs {', '.join(missing)}"
                         " installed - run: migkit doctor --install")
    if via == "pgcopydb" and not pgcopydb_available():
        raise SystemExit("this bulk path needs a component that is not"
                         " available on this machine - migkit doctor says"
                         " what is missing and migkit doctor --install"
                         " puts it in place")
    return fns[via](hop, db, workers, go, log)
