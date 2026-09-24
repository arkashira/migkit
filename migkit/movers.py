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
import json
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


def fitted(hop, engine, via):
    """`via`, or the table copier where `via` cannot carry what this hop
    asks of it - with the reason, in words, or None.

    The one-pass cross-engine load is chosen for every cross-engine hop
    once it is installed, and the load file migkit writes for it reads
    MySQL into PostgreSQL, the whole database, with nothing else: no
    exclude list, no table or column mapping, no row filter. For any
    other pair it reads the wrong server as the wrong kind; on a hop with
    an exclude list it loads the tables the target owns. The table copier
    carries all of those, so the decision is made here, from the hop.
    """
    if via != "pgloader":
        return via, None
    from .engines import ALIASES
    opts = hop.options or {}
    pair = tuple(ALIASES.get(n, n) for n in (
        opts.get("source_engine", "mysql"),
        opts.get("target_engine", "postgres")))
    mapping = getattr(hop, "mapping", None) or {}
    if pair != ("mysql", "postgres"):
        why = "the one-pass load reads MySQL into PostgreSQL only"
    elif getattr(hop, "exclude", None):
        why = ("the one-pass load has no way to leave out the tables the"
               " hop excludes")
    elif any(mapping.get(k) for k in ("tables", "columns", "where")):
        why = ("the one-pass load reads no table, column or row mapping,"
               " and the hop has one")
    else:
        return via, None
    return "builtin", why


#: Movers that can be given a row predicate per table, measured rather
#: than assumed. mydumper takes one section per table in a defaults file
#: (verified: 2 of 3 rows dumped where a rule applied). `pg_dump` 18.6 and
#: `pgcopydb` 0.18 have `-t`, `-T`, `--exclude-table-data` and `--filter` /
#: `--filters` between them and **not one row predicate** - their filtering
#: is table-level throughout.
ROW_FILTER_MOVERS = ("mydumper",)

#: Bulk paths that can leave a table out, so a table with a row filter can
#: be carried by the table copier instead.
ROUTING_MOVERS = ("pgdump", "pgcopydb")

#: Engines whose table copier (`move_table`) applies the row filter on
#: both ends - reading only what it selects, replacing only what it
#: selects on the target. The pair's copier does it in each side's own
#: SQL, for a hop between engines and for every engine that copies
#: through the pair; a side that speaks no SQL is refused by it.
FILTERING_COPIERS = ("postgres", "mysql", "hetero", "sqlite")


def _routes_any(hop, db):
    """Whether a table of `db` may leave the bulk path: excluded, under a
    row filter, or with its columns mapped. The table list is asked for
    only then; `routed_to_copier` decides table by table."""
    return bool(getattr(hop, "exclude", None) or _filtered_here(hop, db)
                or (getattr(hop, "mapping", None) or {}).get("columns"))


def _filtered_here(hop, db):
    """The hop's row-filter keys that may apply to database `db`.

    Left out only when a key names another database outright. A two-part
    key is `database.table` on MySQL and `schema.table` on PostgreSQL, and
    `row_filter` matches both by suffix. This used to keep a two-part key
    only when its first part was `db`, so `public.orders` applied to no
    database at all. The bulk path then neither refused the filter nor
    routed the table to the copier, and dumped every row of a table the
    hop filters. A key is now dropped only when its first part is one of
    the hop's other databases. Callers that hold the table list
    (`routed_to_copier`) still match each table exactly.
    """
    from .engines import ALIASES
    rules = (getattr(hop, "mapping", None) or {}).get("where") or {}
    # where tables have no schema, the first of two parts is a database
    engine = ALIASES.get(hop.engine, hop.engine)
    if engine == "hetero":
        engine = (hop.options or {}).get("source_engine", "")
    two_is_database = engine in ("mysql", "mongodb", "sqlite")
    known = set(getattr(hop, "databases", None) or []) | set(
        getattr(hop, "db_map", None) or {})
    out = []
    for key in rules:
        parts = [p for p in str(key).split(".") if p]
        if len(parts) < 2 or parts[0] == db:
            out.append(key)
        elif (len(parts) == 2 and not two_is_database
              and parts[0] not in known):
            out.append(key)
    return sorted(out)


def routed_to_copier(hop, db, via, tables, qualifier="public"):
    """The tables a bulk path cannot filter, carried by the table copier.

    `via` copies whole tables; the copier reads and replaces only the rows
    a filter selects. So a table with a row filter is left out of the bulk
    copy and moved on its own, and the rest still go the fast way - where
    this used to refuse the whole database. One answer for the dump, the
    plan and the move, so none of them can disagree about which tables took
    which path. Nothing is routed for a path that applies the filter itself.
    """
    from . import planner
    return [d.table for d in planner.plan(hop, db, via, tables, qualifier)
            if d.path == planner.COPIER]


def refuse_unpushable_filters(hop, db, via, engine=None):
    """Stop a move whose row filters nothing on its path can apply.

    A filter is never silently dropped. It is applied by the bulk copy
    (`ROW_FILTER_MOVERS`), or its tables are routed to a table copier that
    applies it (`routed_to_copier`), or the move stops here - before
    anything is copied, because a path that ignores the filter copies every
    row and leaves the check comparing a filtered source against a full
    target for good.
    """
    mine = _filtered_here(hop, db)
    if not mine or via in ROW_FILTER_MOVERS:
        return
    if engine in FILTERING_COPIERS and via in ROUTING_MOVERS + ("builtin",):
        return
    raise SystemExit(
        f"the hop maps row filters onto {', '.join(mine)}, and nothing on"
        f" this {engine or 'engine'} path can apply one: neither its bulk"
        " copy nor its table copier takes a row predicate. Moving anyway"
        " would copy every row and leave `check` comparing a filtered"
        " source against a full target for good. Drop the filters, or"
        " narrow the source with a view the hop points at instead.")


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


#: the programs the bulk paths drive. Their names never reach the operator:
#: the command lines go to the run's debug log, and what a failing one said
#: is passed on with its name taken out.
DRIVEN = ("pg_dump", "pg_restore", "pgcopydb", "psql", "mydumper",
          "myloader", "pgloader", "mongodump", "mongorestore")
#: environment variables whose values are secrets, for the debug log
_SECRET_ENV = re.compile(r"PASS|PWD|SECRET|TOKEN", re.I)
#: (DebugLog, secrets) for the run in progress, set by `run_via`
_DEBUG = None


def _debug(cmd, env=None):
    """Write a command line to the run's debug log, secrets removed."""
    if _DEBUG is not None:
        secrets = [v for k, v in (env or {}).items() if _SECRET_ENV.search(k)]
        _DEBUG[0].command(cmd, secrets + list(_DEBUG[1]))


def _sh(cmd, env=None, log=None, progress=None):
    """Run one program, and keep its command line off the screen.

    This printed `$ <command line>` to the operator for every program it
    ran, and let a failing program's own message out as it was
    (`pg_restore: error: ...`). The command line now goes to the run's
    debug log with every secret removed, and a failure keeps the
    database's words and loses the program's name. What the operator reads
    while the step runs is the phase line its caller logs.
    """
    from . import wording
    _debug(cmd, env)
    if progress is None:
        p = subprocess.run(cmd, env=tool_env(env), text=True,
                           capture_output=True)
        out, err = p.stdout, p.stderr
    else:
        # read what the program says as it says it, and turn the lines that
        # mark progress into migkit's own; the rest is kept only for the
        # error message if it fails
        # one stream, so a program that fills the other pipe cannot stall
        # while this one is being read
        p = subprocess.Popen(cmd, env=tool_env(env), text=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        tail = []
        for line in p.stdout:
            said = progress(line)
            if said and log:
                log(said)
            tail.append(line)
            del tail[:-40]
        p.wait()
        out = err = "".join(tail)
    if p.returncode:
        # a reader that parsed the program's log knows its error messages;
        # the raw tail of a machine log is not something to show anyone
        explain = getattr(progress, "failure", None)
        said = ((explain() if explain else None) or err or out)[-500:]
        raise RuntimeError(wording.without_programs(said, DRIVEN))
    return subprocess.CompletedProcess(cmd, p.returncode, out, err)


def _tables_done(pattern, verb, total=None):
    """A progress reader for a program that names each table as it
    reaches it: `pattern` has a `table` group. Answers migkit's line for a
    matching line - `public.orders: read (3 of 12 tables)` - and None for
    everything else."""
    seen = []
    rx = re.compile(pattern)

    def read(line):
        m = rx.search(line)
        if not m:
            return None
        seen.append(m.group("table"))
        of = f" of {total:,}" if total else ""
        return f"{m.group('table')}: {verb} ({len(seen):,}{of} tables)"
    return read


class _JsonLog:
    """A progress reader for a program that logs one JSON object per line.

    Read by field rather than by the wording of a message, which is what a
    log written for machines is for. `on_event` answers migkit's line for
    an event, or None. Error messages are kept for the failure text.
    """

    def __init__(self, on_event):
        self.on_event = on_event
        self.errors = []

    def __call__(self, line):
        line = line.strip()
        if not line.startswith("{"):
            return None
        try:
            event = json.loads(line)
        except ValueError:
            return None
        # measured: a successful load logs a GLib assertion at ERROR with
        # fatal true, and carries on; it is not the program's own error
        if (event.get("level") in ("ERROR", "CRITICAL")
                and event.get("domain") != "GLib" and event.get("message")):
            self.errors.append(str(event["message"]))
        return self.on_event(event)

    def failure(self):
        return "; ".join(self.errors[-3:]) or None


def _my_dump_progress():
    """The MySQL dump's `dump_table_progress` events, once per table."""
    seen = []

    def on(event):
        if event.get("event") != "dump_table_progress":
            return None
        name = f"{event.get('db')}.{event.get('table')}"
        if name in seen:
            return None
        seen.append(name)
        total = str(event.get("tables_total") or "")
        of = f" of {int(total):,}" if total.isdigit() else ""
        return f"{name}: reading ({len(seen):,}{of} tables)"
    return _JsonLog(on)


def _my_load_progress(summary):
    """The MySQL load's `restore_data_progress` events, once per table,
    and its `restore_completed` counts into `summary`."""
    seen = []

    def on(event):
        kind = event.get("event")
        if kind == "restore_completed":
            summary.update(event)
            return None
        if kind != "restore_data_progress":
            return None
        name = f"{event.get('db')}.{event.get('table')}"
        if name in seen:
            return None
        seen.append(name)
        return f"{name}: loading ({len(seen):,} tables)"
    return _JsonLog(on)


#: what the PostgreSQL dump and load say, with `-v`, as they reach each
#: table
PG_DUMP_TABLE = r'dumping contents of table "(?P<table>[^"]+)"'
PG_RESTORE_TABLE = r'processing data for table "(?P<table>[^"]+)"'


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


def _routed_note(routed):
    """The plan line for tables the table copier carries instead."""
    return (f"# {len(routed)} tables with a row filter are copied table by"
            " table after the bulk copy, the filter applied on both sides:"
            f" {', '.join(routed[:6])}" + (" ..." if len(routed) > 6 else ""))


def _unresolved_exclusion(db, why):
    """Stop a move whose exclusions could not be resolved - before anything.

    The emptying step keeps the tables the hop excludes. A dump that could
    not be told to skip them carries them anyway, and the load then puts
    the source's rows on top of the target's own - into exactly the tables
    the setting exists to protect. Every bulk path raises this one, so they
    stop at the same point and say the same thing.
    """
    return SystemExit(
        f"{db}: the hop excludes tables or filters their rows, and the"
        f" source's table list could not be read ({why}), so the copy cannot"
        " be told to skip them. Nothing has been changed on the target.")


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


#: every index on the target, as `schema.name` - what a load that died
#: dropped is only rebuilt where it is not there now
PG_INDEX_NAMES_SQL = """
    select n.nspname||'.'||i.relname
      from pg_index x
      join pg_class i on i.oid = x.indexrelid
      join pg_namespace n on n.oid = i.relnamespace"""


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
        from .setaside import SetAside
        self.hop, self.db, self.workers, self.log = hop, db, workers, log
        self.dropped, self.ddl = [], {}
        self.record = SetAside(hop, db, "dropped-indexes")

    def __enter__(self):
        from . import indexes as _ix
        try:
            raw = _pg_psql(self.hop, self.db, PG_INDEX_SQL)
            there = set(_pg_psql(self.hop, self.db, PG_INDEX_NAMES_SQL
                                 ).split())
        except Exception:
            return self
        found = []
        for line in (raw or "").splitlines():
            parts = line.split("\x1f")
            if len(parts) == 4:
                found.append((parts[0].strip(), parts[1].strip(),
                              parts[2].strip(), parts[3].strip() == "true"))
        # each index by its schema too: the same name in two schemas is two
        # indexes, and the bare name dropped whichever the search path
        # found first - the other's definition overwrote it, and one of
        # them was never rebuilt
        rows = [(f"{t.split('.', 1)[0]}.{name}", definition, isc)
                for t, name, definition, isc in
                _outside_exclusion(self.hop, self.db, found, self.log)]
        drop, ddl = _ix.plan(rows)
        # what a load that died dropped and the target still lacks: built
        # at the end of this one, with the rest
        left, stale = self.record.left_behind(there)
        if not drop and not left:
            return self
        if not self.record.save({**left, **ddl}, stale):
            if self.log:
                self.log("could not save the index definitions, so none were"
                         " dropped - the load runs with them in place")
            return self
        where = self.record.path
        self.dropped, self.ddl = list(left), dict(left)
        if left and self.log:
            self.log(f"{len(left)} indexes an earlier load dropped and did"
                     " not rebuild are built at the end of this one")
        for name in drop:
            sch, _, idx = name.partition(".")
            quoted = ".".join('"' + x.replace('"', '""') + '"'
                              for x in (sch, idx))
            try:
                _pg_psql(self.hop, self.db, f"drop index {quoted}")
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
        if not failed:
            self.record.done()
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
    skip, routed, unresolved = [], [], ""
    if _routes_any(hop, db):
        try:
            from .engines.postgres import PostgresEngine
            tables = PostgresEngine(hop)._all_tables("src", db)
            skip = excluded_tables(hop, db, tables)
            routed = [n for n in routed_to_copier(hop, db, "pgdump", tables)
                      if n not in skip]
        except Exception as e:
            unresolved = str(e).splitlines()[-1][:70] if str(e) \
                else type(e).__name__
    from .wording import Step, phase
    left_out = skip + routed
    dump = Step(
        phase("dump", workers=workers, left_out=len(skip),
              routed=len(routed)),
        ["pg_dump", "-h", s.host, "-p", s.port, "-U", s.user, "-d", db,
         "-Fd", "-j", workers, "--data-only", "-v", "-f", outdir,
         *[a for name in left_out for a in ("-T", name)]])
    load = Step(
        phase("load", workers=workers),
        ["pg_restore", "-h", t.host, "-p", t.port, "-U", t.user,
         "-d", hop.target_db(db), "--data-only", "--disable-triggers",
         "-v", "-j", workers, outdir])
    steps = [dump, _truncate_step(hop, db), load]
    note = _create_note(lambda: _pg_missing_tables(hop, db)[0])
    if note:
        steps.insert(1, note)
    if unresolved:
        steps.insert(1, _unresolved_note(unresolved))
    else:
        if routed:
            steps.insert(1, _routed_note(routed))
        if skip:
            steps.insert(1, f"# {len(skip)} tables the hop excludes are not"
                            " dumped at all")
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)
    env_t = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    from .engines.postgres import PostgresEngine
    quiet = _pg_quiet_triggers(
        hop, db, [n for n in PostgresEngine(hop)._all_tables("src", db)
                  if n not in left_out])
    if quiet != "flag":
        load.argv.remove("--disable-triggers")
    if quiet == "session":
        env_t["PGOPTIONS"] = "-c session_replication_role=replica"
    import shutil
    shutil.rmtree(outdir, ignore_errors=True)
    if log:
        log(dump)
    _sh(dump.argv, {"PGPASSWORD": s.password, "PGCONNECT_TIMEOUT": "15"},
        log, progress=_tables_done(PG_DUMP_TABLE, "read"))
    _pg_create_missing(hop, db, log)
    _pg_truncate_target(hop, db, log)
    with _IndexWindow(hop, db, workers, log):
        _pgdump_restore(load, env_t, log)
    _pg_finish_created(hop, db, log)
    shutil.rmtree(outdir, ignore_errors=True)
    return steps


class _RestoreLog:
    """Reads a restore's output as it runs: the tables it names, every
    statement the target refused, and how many refusals it counted.

    A newer dump writes session settings an older server does not have
    (`SET transaction_timeout = 0` against PostgreSQL 16), and the restore
    exits 1 over them with every row in place. Those, and only those, are
    tolerated. This used to tolerate any restore that ended in `errors
    ignored on restore`, which is how every failure of it ends: measured
    onto a target without the tables, all 12 statements failed - the
    settings, and every `COPY` with `relation "public.parent" does not
    exist` - and the move said `bulk copy complete` with nothing loaded.
    """

    #: the two ways it says the target refused a statement; its other
    #: error lines are context (`from TOC entry ...`), and in a parallel
    #: restore they arrive interleaved with the refusals they belong to
    REFUSED = re.compile(r'error: (?:could not execute query|COPY failed for'
                         r' table "[^"]*"): (?P<what>ERROR:.*)')
    IGNORED = re.compile(r"errors ignored on restore: (?P<n>\d+)")

    def __init__(self):
        self.tables = _tables_done(PG_RESTORE_TABLE, "loaded")
        self.refused = []
        self.ignored = None

    def __call__(self, line):
        m = self.REFUSED.search(line)
        if m:
            self.refused.append(" ".join(m.group("what").split()))
            return None
        m = self.IGNORED.search(line)
        if m:
            self.ignored = int(m.group("n"))
            return None
        return self.tables(line)

    @staticmethod
    def _a_setting(what):
        # raised only by SET, SHOW and set_config: a setting this server
        # does not have, whichever statement it came in
        return "unrecognized configuration parameter" in what

    @staticmethod
    def _there_already(what):
        return "already exists" in what

    def real(self, existing_ok=False):
        """What the target refused besides settings it does not have - and,
        with `existing_ok`, besides objects it already has. When the
        restore counted more refusals than were read, the difference counts
        as real too - never as tolerated."""
        out = [w for w in self.refused if not self._a_setting(w)
               and not (existing_ok and self._there_already(w))]
        settings = len(self.refused) - len(out)
        if self.ignored is not None and self.ignored > settings + len(out):
            out.append(f"{self.ignored - settings - len(out)} more the"
                       " restore counted and did not say")
        return out


def _restore(argv, env, log, what, existing_ok=False):
    """Run a restore, tolerating only the settings an older server lacks
    (`_RestoreLog`) - and, with `existing_ok`, the objects the target
    already has, which a step that creates only what is missing leaves as
    they are. True when it had to tolerate some."""
    from . import wording
    reader = _RestoreLog()
    try:
        _sh(argv, env, log, progress=reader)
    except RuntimeError as e:
        real = reader.real(existing_ok)
        if "errors ignored on restore" not in str(e) or real:
            if not real:
                raise
            said = list(dict.fromkeys(real))
            raise RuntimeError(wording.without_programs(
                f"the target refused {len(real)} statements of {what}: "
                + "; ".join(said[:4]) + (" ..." if len(said) > 4 else ""),
                DRIVEN)) from e
        return True
    return False


def _pgdump_restore(load, env_t, log):
    """Run the load step, into the *target's* name for the database.

    It used the source's name, while the emptying and the index window
    already used `target_db()` - so a hop whose `db_map` renames the
    database emptied the right one and loaded into another. The name is
    now in the step itself, built once with the plan.
    """
    if log:
        log(load)
    if _restore(load.argv, env_t, log, "the load") and log:
        log("the target rejected settings a newer source version"
            " writes, and nothing else; migkit check confirms the rows")


def _within(ask, seconds=8):
    """`ask()`'s answer, or None when it fails or takes longer than
    `seconds`. For what a dry run would like to say but need not: a target
    that is not reachable yet is retried for a minute and more by the
    engine's own connection, and a plan should not wait for that."""
    import threading
    got = {}

    def run():
        try:
            got["answer"] = ask()
        except Exception:
            got["answer"] = None
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(seconds)
    return got.get("answer")


def _create_note(missing):
    """The plan's line for the tables the target lacks: how many, when the
    target can be asked (`missing` is how to ask); said without a number
    when it cannot; nothing when none are missing."""
    got = _within(missing)
    n = None if got is None else len(got)
    if n == 0:
        return None
    return ("# create the tables the target does not have yet, from the"
            " source's definition" + (f": {n}" if n else ""))


def _pg_pattern(name):
    """`schema.table` as a dump's table pattern that matches it and
    nothing else: each part quoted, so case is kept and no character in
    it is read as a wildcard."""
    sch, _, tbl = str(name).partition(".")
    return ".".join('"' + p.replace('"', '""') + '"' for p in (sch, tbl))


def _pg_missing_tables(hop, db):
    """The tables in scope the target does not have, and whether it has
    no tables at all."""
    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    have = set(eng._all_tables("dst", db))
    # a table whose columns the hop maps is built by the pair's copier,
    # under the names and without the columns the mapping says
    want = [t for t in eng._all_tables("src", db)
            if not hop.excluded(db, *t.split(".", 1))
            and not eng.through_pair(db, t)]
    return [t for t in want if t not in have], not have


def _pg_schema(hop, db, section, tables, whole, log=None):
    """One section of the source's schema, restored onto the target.

    `whole` is a target with no tables at all: everything the source's
    section holds - its types, functions, sequences, tables, views - but
    the tables the hop excludes. Otherwise only `tables`, whose types and
    functions the target has to have already; a definition it cannot take
    stops the move with the server's words. Without owners or grants: the
    roles are `migkit users`' business, and `check` names the grants that
    differ.
    """
    from .engines.postgres import PostgresEngine
    s, t = hop.source, hop.target
    if whole:
        eng = PostgresEngine(hop)
        every = eng._all_tables("src", db)
        skip = excluded_tables(hop, db, every) + [
            n for n in every if eng.through_pair(db, n)]
        pick = [a for n in skip for a in ("-T", _pg_pattern(n))]
    else:
        pick = [a for n in tables for a in ("-t", _pg_pattern(n))]
    out = hop.report_dir(db) / f"schema-{section}.dump"
    dump = ["pg_dump", "-h", s.host, "-p", str(s.port), "-U", s.user,
            "-d", db, "-Fc", "--schema-only", f"--section={section}",
            *pick, "-f", str(out)]
    load = ["pg_restore", "-h", t.host, "-p", str(t.port), "-U", t.user,
            "-d", hop.target_db(db), f"--section={section}", "--no-owner",
            "--no-privileges", str(out)]
    try:
        _sh(dump, {"PGPASSWORD": s.password, "PGCONNECT_TIMEOUT": "15"}, log)
        # a target with no tables can still have the schemas, types or
        # extensions someone made ready for it; those are left as they are
        _restore(load, {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"},
                 log, "the schema", existing_ok=True)
    finally:
        out.unlink(missing_ok=True)


def _pg_created_path(hop, db):
    return hop.report_dir(db) / "created-tables.json"


def _pg_create_missing(hop, db, log=None):
    """Create on the target the tables it lacks, from the source's
    definition, before a data-only load; their keys, indexes and
    constraints come after it (`_pg_finish_created`), which is also the
    faster order.

    Measured onto a target without the tables: the load was refused on
    every one, and before `_restore` read its refusals, the move said
    complete. What was created is recorded before anything is created, so
    a load that fails still has its keys added on the next run, when the
    tables are no longer missing.
    """
    import json
    missing, whole = _pg_missing_tables(hop, db)
    if not missing:
        return
    record = _pg_created_path(hop, db)
    record.write_text(json.dumps({"tables": missing, "whole": whole}))
    if log:
        from .wording import phase
        log(phase("create", tables=len(missing)))
    _pg_schema(hop, db, "pre-data", missing, whole, log)


def _pg_finish_created(hop, db, log=None):
    """The keys, indexes and constraints of the tables `_pg_create_missing`
    made, once their rows are in."""
    import json
    record = _pg_created_path(hop, db)
    try:
        made = json.loads(record.read_text())
    except (OSError, ValueError):
        return
    if log:
        from .wording import phase
        log(phase("finish-created", tables=len(made["tables"])))
    _pg_schema(hop, db, "post-data", made["tables"], made["whole"], log)
    record.unlink()


#: Every foreign key on the target, as the table it references and the
#: table it is on, where those are two tables.
PG_REFERENCES_SQL = """
    select format('%I.%I', rn.nspname, r.relname) || chr(31)
           || format('%I.%I', cn.nspname, c.relname)
      from pg_constraint k
      join pg_class r on r.oid = k.confrelid
      join pg_namespace rn on rn.oid = r.relnamespace
      join pg_class c on c.oid = k.conrelid
      join pg_namespace cn on cn.oid = c.relnamespace
     where k.contype = 'f' and k.confrelid <> k.conrelid"""


def _pg_references_into(hop, db, routed=()):
    """The target's foreign keys into a table the streaming copy would
    empty - every table in scope but the ones routed to the table copier -
    as `referenced <- referencing`. [] when there are none, or when the
    target cannot be asked (the copy then says for itself)."""
    try:
        raw = _pg_psql(hop, db, PG_REFERENCES_SQL)
    except Exception:
        return []
    out = []
    for line in raw.splitlines():
        ref, _, by = line.partition(chr(31))
        if not by or ref in routed:
            continue
        if hop.excluded(db, *ref.replace('"', "").split(".", 1)):
            continue
        out.append(f"{ref} <- {by}")
    return sorted(set(out))


#: The target's triggers of every kind - the foreign keys' own included -
#: on tables outside the catalogues, as `schema.table`.
PG_TRIGGERED_SQL = """
    select distinct n.nspname || '.' || c.relname
      from pg_trigger g
      join pg_class c on c.oid = g.tgrelid
      join pg_namespace n on n.oid = c.relnamespace
     where n.nspname not in ('pg_catalog', 'information_schema')
       and n.nspname not like 'pg\\_%' and c.relname not like 'migkit\\_%'
     order by 1"""


def _pg_quiet_triggers(hop, db, tables):
    """How the load keeps the target's triggers from firing, from what the
    target's user may do.

    `flag` - a superuser: the restore disables every trigger itself, as it
    always has. `session` - allowed `session_replication_role`, which a
    managed service's admin user is: the load's own connections run as a
    replica, with no change to any table. `none` - neither, and nothing to
    keep quiet among `tables`. Anything else stops the move before the
    target is emptied: measured as a plain owner, the restore's own attempt
    was refused on the foreign keys' system triggers (`permission denied:
    "RI_ConstraintTrigger_c_16399" is a system trigger`) and the child
    table's rows were then refused on its foreign key - 100 of 100 rows
    missing - which the load used to report as complete.
    """
    t = hop.target
    ask = ["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
           "-d", hop.target_db(db), "-X", "-At", "-v", "ON_ERROR_STOP=1"]
    env = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    if _sh(ask + ["-c", "select rolsuper from pg_roles"
                        " where rolname = current_user"],
           env).stdout.strip() == "t":
        return "flag"
    try:
        _sh(ask + ["-c", "set session_replication_role = replica"], env)
        return "session"
    except RuntimeError:
        pass
    triggered = set(_sh(ask + ["-c", PG_TRIGGERED_SQL], env
                        ).stdout.split())
    # by `schema.table` or by the bare name, which is all some callers have
    # - a bare name matches it in every schema, which can only refuse more
    hit = sorted(t for t in triggered
                 if tables is None or t in tables
                 or t.rpartition(".")[2] in tables)
    if not hit:
        return "none"
    raise SystemExit(
        f"{db}: {', '.join(hit[:5])}" + (" ..." if len(hit) > 5 else "")
        + f" have triggers on the target - foreign keys among them - and"
        f" {t.user} can neither turn them off for the load nor load as a"
        " replica. Loading anyway would either be refused row by row or"
        " let the triggers rewrite what arrives. Nothing has been written."
        f" Grant it with `GRANT SET ON PARAMETER session_replication_role"
        f" TO {t.user}` (PostgreSQL 15 and later), or move as the managed"
        " service's admin user.")


def _pg_carry_sequences(hop, db, log=None):
    """Set the target's sequences from the source's after a copy that
    carries rows and not sequences.

    Measured, a streaming copy of 52 rows: the target's sequence stayed at
    1, and the application's first insert after cutover failed on
    `duplicate key ... Key (id)=(1) already exists`. The copy program's own
    sequence command, run on its own, read 0 sequences and reset the
    target's. The engine's sequence repair decides the value instead - the
    source's, never below a row the target holds - and refuses a target
    already ahead of the source, which it says.
    """
    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    for action in eng.repair_plan(db, "sequences"):
        if action.statements:
            if log:
                from .wording import phase
                log(phase("sequences"))
            _pg_psql(hop, db, "\n".join(action.statements))
        if log and action.note:
            log(action.note)


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


TRIGGERS_SET_ASIDE = "dropped-triggers"


def triggers_set_aside(hop, db):
    """The triggers migkit took off this database's target for a load and
    has not put back: [(file, its process still running, {name: saved
    definition} or None where the file cannot be read)] - one file per
    load (`setaside`)."""
    from .setaside import SetAside
    return SetAside(hop, db, TRIGGERS_SET_ASIDE).records()


def _trigger_parts(text):
    """sql_mode, statement, charset, collation, from how they were saved."""
    lines = text.split("\n")
    return [lines[0], "\n".join(lines[1:-2]), lines[-2], lines[-1]]


class _MyTriggerWindow:
    """Take the target's triggers off the tables a MySQL load writes, and
    put them back afterwards.

    MySQL has no way to keep a trigger from firing for one session, which
    is what PostgreSQL's replica role does. Measured on 8.4, a `BEFORE
    INSERT` trigger setting `updated_at = now()` on the target: rows the
    source dated 2001 and 2002 landed dated the day of the move, through the
    bulk load and through the table copier both, and the move said
    complete. So the triggers go for the load, their definitions saved to
    disk first, and come back on the way out whether the load worked or
    not.

    A trigger defined by another account comes back only for a user allowed
    to set its definer. Where the load's user is not, nothing is dropped and
    the move stops before loading, naming them - rather than taking away
    what it cannot put back.

    A change tail holds the window for as long as it runs, which is days,
    and a process killed that long is not unusual. What a run that died
    took off goes back at the end of the next load into the database, and
    `check` names it until then.
    """

    def __init__(self, hop, db, log=None, tables=None):
        from .setaside import SetAside
        self.hop, self.db, self.log = hop, db, log
        # a trigger's table has no schema here; a pair's names may
        self.tables = (None if tables is None
                       else {str(t).rpartition(".")[2] for t in tables})
        self.defs, self.dropped = {}, []
        self.record = SetAside(hop, db, TRIGGERS_SET_ASIDE)

    def __enter__(self):
        from .engines.mysql import MySQLEngine
        eng = MySQLEngine(self.hop)
        self.eng, self.tdb = eng, eng._d("dst", self.db)
        on = eng._q("dst", "select trigger_name, event_object_table, definer"
                           " from information_schema.triggers"
                           " where trigger_schema = %s", (self.tdb,))
        there = {str(r[0]) for r in on}
        rows = [r for r in on
                if not self.hop.excluded(self.db, str(r[1]))
                and (self.tables is None or str(r[1]) in self.tables)]
        # what a load that died took off: back at the end of this one,
        # whichever tables this one writes. Held by a running one: off, as
        # this load needs them, and that one puts them back.
        left, stale = self.record.left_behind(there)
        adopted = {n: _trigger_parts(t) for n, t in left.items()}
        if not rows and not adopted:
            for path in stale:
                path.unlink(missing_ok=True)
            return self
        me = str(eng._q("dst", "select current_user()")[0][0])
        grants = " ".join(str(g[0]) for g in eng._q("dst", "show grants"))
        may = any(p in grants for p in ("SET_USER_ID", "SUPER",
                                        "ALL PRIVILEGES ON *.*"))
        foreign = [f"{n} on {t} (defined by {d})" for n, t, d in rows
                   if str(d) != me and not may]
        if foreign:
            raise SystemExit(
                f"{self.db}: " + ", ".join(foreign[:5])
                + (" ..." if len(foreign) > 5 else "")
                + " would fire for every row the load writes, and could"
                f" rewrite them; {me} cannot create them again as their"
                " definer after taking them off. Nothing has been loaded."
                " Load as an account allowed to set a definer (SET_USER_ID),"
                " or take them off and put them back yourself.")
        q = eng._quote_ident
        for name, table, _ in rows:
            got = eng._q("dst", f"show create trigger {q(self.tdb)}"
                                f".{q(name)}")[0]
            # Trigger, sql_mode, SQL Original Statement, charset, collation
            self.defs[str(name)] = [str(got[1]), str(got[2]), str(got[3]),
                                    str(got[4])]
        self.defs.update(adopted)
        if not self.record.save({k: "\n".join(v)
                                 for k, v in self.defs.items()}, stale):
            raise SystemExit(
                f"{self.db}: the target's triggers could not be saved before"
                " taking them off for the load, so nothing was loaded")
        self.dropped = list(adopted)
        for name, _, _ in rows:
            eng._q("dst", f"drop trigger if exists {q(self.tdb)}"
                          f".{q(str(name))}")
            self.dropped.append(str(name))
        if self.log:
            if adopted:
                self.log(f"{len(adopted)} triggers an earlier load took off"
                         " and did not put back go back at the end of this"
                         " one: " + ", ".join(sorted(adopted)[:5]))
            self.log(f"{len(self.dropped) - len(adopted)} triggers taken off"
                     f" for the load; definitions saved to"
                     f" {self.record.path}")
        return self

    def __exit__(self, exc_type, *exc):
        failed = []
        for name in self.dropped:
            mode, stmt, charset, collation = self.defs[name]
            conn = self.eng._conn("dst")
            try:
                with conn.cursor() as cur:
                    cur.execute(f"use {self.eng._quote_ident(self.tdb)}")
                    cur.execute("set session sql_mode = %s", (mode,))
                    cur.execute(f"set names {charset} collate {collation}")
                    cur.execute(stmt)
            except Exception as e:
                # 1359: put back already, by hand
                if getattr(e, "args", [None])[0] != 1359:
                    failed.append(name)
                    if self.log:
                        self.log(f"TRIGGER NOT PUT BACK: {name}:"
                                 f" {str(e).splitlines()[-1][:100]}")
            finally:
                conn.close()
        if self.dropped and self.log:
            self.log(f"{len(self.dropped) - len(failed)} of"
                     f" {len(self.dropped)} triggers put back")
        if not failed:
            self.record.done()
        if failed and exc_type is None:
            raise SystemExit(
                f"{self.db}: {len(failed)} triggers were not put back:"
                f" {', '.join(failed)}. Their definitions are in"
                f" {self.record.path}")
        return False


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
        from .setaside import SetAside
        self.eng, self.hop, self.db = engine, hop, db
        self.workers, self.log = workers, log
        self.dropped, self.ddl = [], {}
        self.record = SetAside(hop, db, "dropped-indexes")

    def __enter__(self):
        from . import indexes as _ix
        ddb = self.hop.target_db(self.db) if hasattr(self.hop, "target_db") \
            else self.db
        try:
            rows = self.eng._q("dst", MY_INDEX_SQL, (ddb,))
            there = {f"{t}.{n}" for t, n in self.eng._q(
                "dst", "select distinct table_name, index_name from"
                       " information_schema.statistics"
                       " where table_schema = %s", (ddb,))}
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
        # what a load that died dropped and the target still lacks: built
        # at the end of this one, with the rest
        left, stale = self.record.left_behind(there)
        if not drop and not left:
            return self
        if not self.record.save({**left, **ddl}, stale):
            if self.log:
                self.log("could not save the index definitions, so none were"
                         " dropped - the load runs with them in place")
            return self
        where = self.record.path
        self.dropped, self.ddl = list(left), dict(left)
        if left and self.log:
            self.log(f"{len(left)} indexes an earlier load dropped and did"
                     " not rebuild are built at the end of this one")
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
        if not failed:
            self.record.done()
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


def pgcopydb_filters(hop, db, tables, also=()):
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
    # tables left to the table copier (`routed_to_copier`) are left out of
    # the bulk copy the same way
    out += [n for n in also if n not in out]
    if not out:
        return None
    return "[exclude-table]\n" + "\n".join(out) + "\n"


#: the schema comparison's config: both URLs read from the environment
SCHEMA_DIFF_CONFIG = """variable "from" {
  type    = string
  default = getenv("MIGKIT_SCHEMA_FROM")
}
variable "to" {
  type    = string
  default = getenv("MIGKIT_SCHEMA_TO")
}
env "migkit" {
  url = var.from
  src = var.to
}
"""


def schema_diff(frm, to, excludes=(), timeout=180):
    """The DDL that takes database URL `frm` to `to`, with the URLs handed
    over in the environment. On the command line, their passwords were
    readable by every process listing on the machine. Measured: the
    comparison reads both through a config file of its own and finds the
    same difference, and a wrong password is still refused."""
    import os
    import shutil
    import tempfile
    d = tempfile.mkdtemp(prefix="migkit-")
    try:
        cfg = os.path.join(d, "schema.hcl")
        with open(cfg, "w") as f:
            f.write(SCHEMA_DIFF_CONFIG)
        return run(["atlas", "schema", "diff", "--config", f"file://{cfg}",
                    "--env", "migkit", "--from", "env://url",
                    "--to", "env://src",
                    *[a for e in excludes for a in ("--exclude", e)]],
                   env={"MIGKIT_SCHEMA_FROM": frm, "MIGKIT_SCHEMA_TO": to},
                   check=False, timeout=timeout)
    finally:
        shutil.rmtree(d, ignore_errors=True)


class client_defaults:
    """A defaults file per endpoint, holding its password, for a MySQL
    client program that takes its connection as a DSN (`F=` names the
    file): the password on its command line is readable by every process
    listing on the machine. Measured: the table sync reads `[client]
    password` from `F=` and produces the same statements. Private to this
    user, and gone when the block ends."""

    def __init__(self, *endpoints):
        self.endpoints, self.dir = endpoints, None

    def __enter__(self):
        import os
        import tempfile
        self.dir = tempfile.mkdtemp(prefix="migkit-")
        paths = []
        for i, ep in enumerate(self.endpoints):
            path = os.path.join(self.dir, f"{i}.cnf")
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write("[client]\npassword=" + str(ep.password or "")
                        + "\n")
            paths.append(path)
        return paths

    def __exit__(self, *exc):
        import shutil
        shutil.rmtree(self.dir, ignore_errors=True)
        return False


def mydumper_session(hop, db):
    """The dump's defaults file: its session's sql_mode, then the hop's
    row filters.

    The dump writes its session's sql_mode at the head of every data file,
    and the load runs under it. Left to itself it takes the source's mode
    without its strict part. Measured on 8.4, onto a target column of the
    wrong type, length or kind: `'z'` landed as `0`, `'12345678901'` as
    `'12345'`, `'2026-13-45'` as `0000-00-00`, and a date the source held,
    `2026-00-15`, as `0000-00-00` too - and the move said complete. The
    mode every write of migkit's runs under (`WRITE_SQL_MODE`) stops on the
    first such row instead, and keeps the source's own values.
    """
    from .engines.mysql import MySQLEngine
    return ("[mydumper_session_variables]\nsql_mode ="
            f" '{MySQLEngine.WRITE_SQL_MODE}'\n"
            + (mydumper_defaults(hop, db) or ""))


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
    # the option column: the flag, then the column gap, the end of the
    # line, or its `=VALUE`. Measured, the MongoDB tools spell theirs in
    # camelCase with `=<value>` (`--bypassDocumentValidation`), and the old
    # lower-case, gap-only pattern found 6 of mongodump's 37 flags
    return frozenset(re.findall(
        r"(?m)^\s+(?:-\w,\s+)?(--[a-zA-Z0-9][a-zA-Z0-9-]*)(?=\s{2,}|$|=|\[=)",
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
        from .wording import phase
        log(phase("empty", tables=len(keep), left_out=len(skip)))
    conn = eng._conn("dst")
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt)
    finally:
        conn.close()
    return keep


def _my_missing_tables(hop, db):
    """The tables in scope the target does not have yet, and whether the
    database itself is missing there."""
    from .engines.mysql import MySQLEngine
    eng = MySQLEngine(hop)
    have = set(eng._all_tables("dst", db))
    # a table whose columns the hop maps is built by the pair's copier,
    # under the names and without the columns the mapping says
    want = [t for t in eng._tables("src", db)
            if t not in have and not eng.through_pair(db, t)]
    gone = not have and not eng._q(
        "dst", "select 1 from information_schema.schemata"
               " where schema_name = %s", (eng._d("dst", db),))
    return want, gone


def _my_create_missing(hop, db, log=None):
    """Create on the target the tables it does not have, from the source's
    own definition, before a data-only load.

    The load carries rows, not tables. Measured, onto a target without
    them: the move emptied nothing, loaded nothing, and stopped on
    `ERROR 1146: Table 'appdb.small' doesn't exist` - where the
    cross-engine copier creates what is missing. A table the
    target already has is left exactly as it is; the database is created,
    in the source's character set and collation, only when it is not there.

    One session, with foreign key checks off so that a table may reference
    one created after it, and the sql_mode a dump of the schema would use,
    so that a definition the source accepted is accepted here.
    """
    from .engines.mysql import MySQLEngine
    eng = MySQLEngine(hop)
    want, gone = _my_missing_tables(hop, db)
    if not want and not gone:
        return []
    ddb = eng._quote_ident(eng._d("dst", db))
    stmts = ["set foreign_key_checks = 0",
             "set session sql_mode = 'NO_AUTO_VALUE_ON_ZERO'"]
    if gone:
        cs = eng._q("src", "select default_character_set_name,"
                           " default_collation_name from"
                           " information_schema.schemata"
                           " where schema_name = %s", (db,))
        stmts.append(f"create database if not exists {ddb}"
                     + (f" character set {cs[0][0]} collate {cs[0][1]}"
                        if cs else ""))
    src = eng._quote_ident(db)
    stmts.append(f"use {ddb}")
    # a MariaDB table kept with its history is a table there and nothing
    # else here: MySQL has no system versioning, so it is made as a plain
    # one - the rows it holds now arrive, the history does not, and that
    # is said (assess names it before the move)
    keeps_history = eng._brands()[1].name == "mariadb"
    plain = []
    for t in want:
        got = eng._q("src", f"show create table {src}.{eng._quote_ident(t)}")
        ddl = got[0][1]
        if not keeps_history and re.search(r"\bWITH SYSTEM VERSIONING\b",
                                           ddl, re.I):
            ddl = re.sub(r"\s+WITH(OUT)? SYSTEM VERSIONING\b", "", ddl,
                         flags=re.I)
            plain.append(t)
        stmts.append(ddl)
    if log:
        from .wording import phase
        log(phase("create", tables=len(want)))
        for t in plain:
            log(f"{t}: made without its history - the target keeps none;"
                " its rows as they are now are carried")
    conn = eng._conn("dst")
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt)
    finally:
        conn.close()
    return want


#: One row per column of every foreign key inside one database, in key
#: order. Grouped in Python rather than with `group_concat`, which would
#: need a separator no column name can contain.
MY_FK_COLUMNS_SQL = """
    select constraint_name, table_name, referenced_table_name,
           column_name, referenced_column_name
      from information_schema.key_column_usage
     where table_schema = %s and referenced_table_schema = %s
       and referenced_table_name is not null
     order by constraint_name, table_name, ordinal_position"""


def _my_orphans_left(hop, db, log):
    """Say which rows of an excluded table now point at nothing.

    The excluded table keeps its rows, which is the point of excluding it.
    But a row of it can reference a row only the target had: the load
    replaces the referenced table with the source's rows, that one does not
    come back, and the reference is left pointing at nothing. `check` will
    never see it - the hop excludes the table it is in - so the move is the
    only step that can say so. PostgreSQL needs no counterpart: there, an
    excluded table that references a replaced one stops the move before
    anything is emptied.

    Returns `(table, referenced, count)` triples, or None when the
    catalogue could not be read - which is said, not taken as "none".
    """
    from .engines.mysql import MySQLEngine
    eng = MySQLEngine(hop)
    ddb = eng._d("dst", db)
    q = eng._quote_ident
    try:
        rows = eng._q("dst", MY_FK_COLUMNS_SQL, (ddb, ddb))
    except Exception as e:
        if log:
            log("could not look for rows left pointing at nothing:"
                f" {str(e).strip()[:100]}")
        return None
    keys = {}
    for name, table, parent, col, pcol in rows:
        keys.setdefault((name, table, parent), []).append((col, pcol))
    found = []
    for (name, table, parent), cols in sorted(keys.items()):
        if not excluded_tables(hop, db, [table], db) \
                or excluded_tables(hop, db, [parent], db):
            continue
        on = " and ".join(f"c.{q(a)} = p.{q(b)}" for a, b in cols)
        filled = " and ".join(f"c.{q(a)} is not null" for a, _ in cols)
        sql = (f"select count(*) from {q(ddb)}.{q(table)} c"
               f" left join {q(ddb)}.{q(parent)} p on {on}"
               f" where {filled} and p.{q(cols[0][1])} is null")
        try:
            n = int(eng._q("dst", sql)[0][0])
        except Exception as e:
            if log:
                log(f"could not look for rows of {table} left pointing at"
                    f" nothing: {str(e).strip()[:100]}")
            continue
        if n:
            found.append((table, parent, n))
            if log:
                log(f"{table}: {n} {'row points' if n == 1 else 'rows point'}"
                    f" at {parent} rows that no longer exist - they were only"
                    f" on the target. The hop excludes {table}, so the check"
                    " will not look at it; this is the only place it is"
                    " said")
    return found


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
    if _my_target_logs(hop):
        # the loader turns the binlog off for its own sessions unless told
        # otherwise. Measured on 8.4: a replica of the target received the
        # tables the move created and none of their 300,000 rows, and said
        # nothing - and a point-in-time restore of the target, which
        # replays the same log, would not have them either
        load.append(tool_flag("myloader", "--enable-binlog"))
    # one JSON object per event, table by table - what the progress lines
    # are read from. A build without it just gives no per-table lines.
    for cmd, program in ((dump, "mydumper"), (load, "myloader")):
        if "--machine-log-json" in _long_options(program):
            cmd += ["--machine-log-json", "-v", "3"]
    return dump, load


def _my_target_logs(hop):
    """Whether the target writes a binlog, which its replicas and its
    point-in-time recovery read. Asked quickly: a plan against a target
    not reachable yet says it would not, and the run asks again."""
    from .engines.mysql import MySQLEngine

    def ask():
        eng = MySQLEngine(hop)
        conn = eng._conn("dst", retry=False)
        try:
            with conn.cursor() as cur:
                cur.execute("select @@log_bin")
                return str(cur.fetchone()[0]) == "1"
        finally:
            conn.close()
    return bool(_within(ask))


def mydumper_move(hop, db, workers, go, log):
    """Dump, then empty the target, then load - in that order.

    The target is emptied only once a complete dump is on disk. Emptying it
    first, and then finding the source unreachable, hands back a target
    with nothing in it and nothing to load.
    """
    outdir = hop.report_dir(db) / "mydumper"
    settings = mydumper_session(hop, db)
    # beside the dump directory, not inside it: myloader is pointed at
    # that directory and has no reason to meet a file it does not read
    cnf = hop.report_dir(db) / "mydumper-filters.cnf"
    omit = hop.report_dir(db) / "omit-tables.txt"
    skip, routed, unresolved = [], [], ""
    if _routes_any(hop, db):
        from .engines.mysql import MySQLEngine
        try:
            every = MySQLEngine(hop)._all_tables("src", db)
            skip = excluded_tables(hop, db, every, qualifier=db)
            # the table copier carries these after the load (the dump
            # applies row filters itself, so only mapped columns route)
            routed = [n for n in routed_to_copier(hop, db, "mydumper", every,
                                                  qualifier=db)
                      if n not in skip]
        except Exception as e:
            unresolved = str(e).strip().splitlines()[0][:100] if str(e) \
                else type(e).__name__
    from .wording import Step, phase
    dump, load = _mydumper_commands(hop, db, workers, outdir, cnf,
                                    omit if skip or routed else None)
    dump = Step(phase("dump", workers=workers, left_out=len(skip),
                      routed=len(routed),
                      row_filters=len(_filtered_here(hop, db))
                      if mydumper_defaults(hop, db) else 0), dump)
    load = Step(phase("load", workers=workers), load)
    steps = [dump, _truncate_step(hop, db), load]
    note = _create_note(lambda: _my_missing_tables(hop, db)[0])
    if note:
        steps.insert(1, note)
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
    cnf.write_text(settings)
    cnf.chmod(0o600)
    if skip or routed:
        omit.write_text("".join(f"{n}\n" for n in skip + routed))
    if log:
        log(dump)
    _sh(dump.argv, {"MYSQL_PWD": hop.source.password}, log,
        progress=_my_dump_progress())
    from .engines.mysql import MySQLEngine
    _my_create_missing(hop, db, log)
    _my_truncate_target(hop, db, log)
    summary = {}
    with _MyTriggerWindow(hop, db, log), \
            _MyIndexWindow(MySQLEngine(hop), hop, db, workers, log):
        if log:
            log(load)
        _sh(load.argv, {"MYSQL_PWD": hop.target.password}, log,
            progress=_my_load_progress(summary))
    errors = str(summary.get("errors") or "0")
    if errors.isdigit() and int(errors):
        # the load's own count, which an exit code of 0 does not rule out
        raise RuntimeError(f"the load counted {int(errors):,} errors")
    if skip:
        _my_orphans_left(hop, db, log)
    _my_dump_position(hop, db, outdir)
    shutil.rmtree(outdir, ignore_errors=True)
    return steps


def _my_dump_position(hop, db, outdir):
    """Keep the position the dump was a snapshot at, for a replica that
    follows this copy (`replicate_sql`). The dump records it in its
    metadata, commented out:

        [source]
        # executed_gtid_set = "c95d739d-...:1-12"
        # SOURCE_LOG_FILE = "binlog.000002"
        # SOURCE_LOG_POS = 2087
    """
    import json
    try:
        text = (outdir / "metadata").read_text()
    except OSError:
        return
    got = dict(re.findall(r"(?m)^#?\s*(SOURCE_LOG_FILE|SOURCE_LOG_POS|"
                          r"executed_gtid_set)\s*=\s*\"?([^\"\n]*)\"?\s*$",
                          text))
    if "SOURCE_LOG_FILE" in got and "SOURCE_LOG_POS" in got:
        (hop.report_dir(db) / "dump-position.json").write_text(json.dumps({
            "log_file": got["SOURCE_LOG_FILE"].strip(),
            "log_pos": int(got["SOURCE_LOG_POS"]),
            "gtid_set": got.get("executed_gtid_set", "").strip()}))


def pgloader_move(hop, db, workers, go, log):
    """MySQL to PostgreSQL in one pass, through a load file.

    The load file carries both passwords, so it lives only for the run:
    written with owner-only permissions, and removed afterwards whether
    the load worked or not. It named the source's database on the target
    side too, so a hop whose `db_map` renames the database loaded into the
    wrong one.
    """
    from .wording import Step, phase
    s, t = hop.source, hop.target
    loadfile = hop.report_dir(db) / "pgloader.load"
    body = f"""LOAD DATABASE
  FROM mysql://{s.user}:{quote(s.password, safe='')}@{s.host}:{s.port}/{db}
  INTO postgresql://{t.user}:{quote(t.password, safe='')}@{t.host}:{t.port}/{hop.target_db(db)}
WITH data only, workers = {workers}, concurrency = {min(workers, 4)},
     on error stop
ALTER SCHEMA '{db}' RENAME TO 'public';
"""
    copy = Step(phase("stream-copy", workers=workers),
                ["pgloader", loadfile])
    steps = [copy]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    loadfile.write_text(body)
    loadfile.chmod(0o600)
    try:
        if log:
            log(copy)
        _sh(copy.argv, None, log)
    finally:
        loadfile.unlink(missing_ok=True)
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
    """Dump piped straight into restore, one collection set per database.

    The restore drops each collection it is about to load. It read no
    exclude list, so a collection the hop excludes - one the target owns -
    was dropped and replaced with the source's copy whenever the source had
    one by that name. Excluded collections are now left out of the dump,
    and out of the restore as well, so the drop cannot reach them. And the
    restore wrote to the source's database name whatever the hop's
    `db_map` said.
    """
    from .wording import Step, phase
    s, t = hop.source, hop.target
    skip, unresolved = [], ""
    if getattr(hop, "exclude", None):
        try:
            from .engines.mongodb import MongoEngine
            names = MongoEngine(hop)._client("src")[db].list_collection_names()
            skip = sorted(n for n in names if hop.excluded(db, n))
        except Exception as e:
            unresolved = (str(e).strip().splitlines()[0][:100] if str(e)
                          else type(e).__name__)
    tdb = hop.target_db(db)
    dump = ["mongodump", f"--uri={_mongo_uri(s)}", f"--db={db}", "--archive",
            "--quiet", *[f"--excludeCollection={n}" for n in skip]]
    # the collection is made with its validator before its documents go
    # in, and a document written before that validator existed - which the
    # source still holds - was refused, while the program exited 0
    restore = ["mongorestore", f"--uri={_mongo_uri(t)}", "--archive",
               "--drop", "--bypassDocumentValidation", f"--nsInclude={db}.*",
               *[f"--nsExclude={db}.{n}" for n in skip],
               *([f"--nsFrom={db}.*", f"--nsTo={tdb}.*"] if tdb != db
                 else []),
               f"--numParallelCollections={workers}"]
    copy = Step(phase("stream-copy", workers=workers, left_out=len(skip)),
                dump + ["|"] + restore)
    steps = [copy]
    if unresolved:
        steps.append(_unresolved_note(unresolved))
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)
    if log:
        log(copy)
    _debug(copy.argv)
    dump = subprocess.Popen(dump, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=tool_env())
    restore = subprocess.Popen(restore, stdin=dump.stdout,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE, env=tool_env())
    dump.stdout.close()
    # what the load says as it finishes each collection, in migkit's words,
    # and how many documents it could not load - which it reports and then
    # exits 0 over
    loaded = _tables_done(MONGO_RESTORED, "loaded")
    tail, failed = [], []
    for raw in restore.stderr:
        line = raw.decode(errors="replace")
        said = loaded(line)
        if said and log:
            log(said)
        m = re.search(MONGO_RESTORED, line)
        if m and int(m.group("failed")):
            failed.append(f"{m.group('table')} {m.group('failed')} of"
                          f" {int(m.group('docs')) + int(m.group('failed'))}")
        tail.append(line)
        del tail[:-40]
    restore.wait()
    _, err_d = dump.communicate()
    if dump.returncode or restore.returncode:
        from .wording import without_programs
        raise RuntimeError(without_programs(
            (err_d.decode(errors="replace") + "".join(tail))[-500:], DRIVEN))
    if failed:
        raise RuntimeError(
            "documents that did not load: " + ", ".join(failed[:6])
            + (" ..." if len(failed) > 6 else ""))
    if log:
        log(f"{db}: copied")
    return steps


#: what the MongoDB load says as it finishes each collection
#: (measured on 100.16: "finished restoring `app.orders` (1 document,
#: 1 failure)" - the first count is what landed, the namespace in backticks)
MONGO_RESTORED = (r"finished restoring `?(?P<table>[^`\s]+)`? \((?P<docs>\d+)"
                  r" documents?, (?P<failed>\d+) failures?\)")



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
    # no password in either connection string: they went on the program's
    # command line, where any process listing on the machine read them
    # while it ran. The passwords go in a password file only this user can
    # read, for the run's length.
    src = (f"postgresql://{quote(s.user or '', safe='')}"
           f"@{s.host}:{s.port}/{db}")
    dst = (f"postgresql://{quote(t.user or '', safe='')}"
           f"@{t.host}:{t.port}/{ddb}{QUIET_TRIGGERS}")
    passfile = hop.report_dir(db) / "streaming.pgpass"
    net = os.environ.get("MIGKIT_PGCOPYDB_NETWORK", "host")
    how = pgcopydb_runner()
    # Excluded tables, resolved against what the source actually has.
    # Asking the source is what turns a pattern into the concrete
    # `schema.table` names pgcopydb wants; a source that cannot be reached
    # yet leaves the filter off rather than guessing, and says so in the
    # steps instead of quietly copying what the hop excludes.
    filters_path, filters_note, unresolved, routed = None, "", "", []
    if _routes_any(hop, db):
        try:
            from .engines.postgres import PostgresEngine
            tables = PostgresEngine(hop)._all_tables("src", db)
            routed = routed_to_copier(hop, db, "pgcopydb", tables)
            text = pgcopydb_filters(hop, db, tables, also=routed)
        except Exception as e:
            text = None
            unresolved = (str(e).splitlines()[-1][:70] if str(e)
                          else type(e).__name__)
            filters_note = _unresolved_note(unresolved)
        if text:
            filters_path = hop.report_dir(db) / "pgcopydb-filters.ini"
            filters_path.write_text(text)
            filters_note = (f"# {text.count(chr(10)) - 1 - len(routed)}"
                            " tables excluded by the hop are filtered out at"
                            " the source")
            if routed:
                filters_note += "\n" + _routed_note(routed)
    uris = {}
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
        # the container still takes them in its environment: whether it
        # can read a file mounted from here depends on the user it runs as,
        # which this machine cannot measure without the image. Named on
        # the command line and valued from this process's environment
        # (`-e NAME`), which docker passes through - measured - so the
        # passwords are not on a command line anyone can list.
        def full(ep, name, tail=""):
            return (f"postgresql://{quote(ep.user or '', safe='')}"
                    f":{quote(ep.password or '', safe='')}"
                    f"@{ep.host}:{ep.port}/{name}{tail}")
        uris = {"PGCOPYDB_SOURCE_PGURI": full(s, db),
                "PGCOPYDB_TARGET_PGURI": full(t, ddb, QUIET_TRIGGERS)}
        cmd = ["docker", "run", "--rm", "--network", net,
               "-e", "PGCOPYDB_SOURCE_PGURI", "-e", "PGCOPYDB_TARGET_PGURI"]
        if filters_path:
            cmd += ["-v", f"{filters_path}:/tmp/migkit-filters.ini:ro"]
        cmd += [PGCOPYDB_IMAGE, "pgcopydb", "copy", "table-data",
                "--table-jobs", str(workers)]
        if filters_path:
            cmd += ["--filters", "/tmp/migkit-filters.ini"]
    # the plan says what happens; the command stays on the step and goes
    # only to the run's debug log
    from .wording import Step, phase
    # the run has to know; a plan says what it can find out quickly
    blocked = (_pg_references_into(hop, db, routed) if go else
               _within(lambda: _pg_references_into(hop, db, routed)) or [])
    if blocked:
        # measured: the streaming copy empties each table it loads on its
        # own, one at a time, and the target refuses to empty a table
        # another one references - `cannot truncate a table referenced in
        # a foreign key constraint` - so on such a target it can only fail
        if log:
            log(f"the target's foreign keys ({', '.join(blocked[:3])}"
                + (" ..." if len(blocked) > 3 else "") + ") keep the"
                " streaming copy from emptying the tables it loads, so this"
                " goes through a local copy instead")
        return pgdump_move(hop, db, workers, go, log)
    copy = Step(phase("stream-copy", workers=workers), cmd)
    steps = [_truncate_step(hop)]
    note = _create_note(lambda: _pg_missing_tables(hop, db)[0])
    if note:
        steps.insert(0, note)
    if filters_note:
        steps.append(filters_note)
    steps.append(copy)
    steps.append("# set the target's sequences from the source's")
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)

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
    _pgpass(passfile, hop, db, ddb)
    env = {"PGPASSFILE": str(passfile), **uris}
    try:
        _sh(ping, env)
    except Exception as e:
        passfile.unlink(missing_ok=True)
        if log:
            where = ("from its container"
                     f" (network={net})" if how != "local" else "")
            log(f"the streaming copy cannot reach both databases {where}:"
                f" {str(e).splitlines()[-1][:120]}")
            log("copying through a local dump instead")
        return pgdump_move(hop, db, workers, go, log)

    try:
        _pg_create_missing(hop, db, log)
        _pg_truncate_target(hop, db, log)
        with _IndexWindow(hop, db, workers, log):
            if log:
                log(copy)
            _sh(copy.argv, env)
    finally:
        passfile.unlink(missing_ok=True)
    _pg_finish_created(hop, db, log)
    _pg_carry_sequences(hop, db, log)
    return steps


def _pgpass(path, hop, db, ddb):
    """A password file holding the source's and the target's passwords,
    readable by this user only. `:` and `\\` in a field are escaped, as
    the file's format asks."""
    def field(v):
        return str(v).replace("\\", "\\\\").replace(":", "\\:")
    s, t = hop.source, hop.target
    lines = [":".join(field(x) for x in (e.host, e.port, name, e.user,
                                         e.password or ""))
             for e, name in ((s, db), (t, ddb))]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(mode=0o600)
    path.chmod(0o600)
    path.write_text("\n".join(lines) + "\n")


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
            from .wording import without_programs
            raise SystemExit(f"could not set the {why} on the CDC leg:"
                             + without_programs(
                                 " " + (p.stderr or p.stdout)[-200:],
                                 DRIVEN))
        if log:
            log(f"change stream {why} set")


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
        if _gtid_on(hop):
            # re-reading a table then interleaves with the stream instead of
            # pausing it, and needs nothing written to the source: the
            # watermarks come from the server's executed GTID set
            source["config"]["read.only"] = "true"
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
                tables = MySQLEngine(hop)._all_tables("src", one)
                skip = debezium_exclude(hop, one, tables, one)
            else:
                tables = PostgresEngine(hop)._all_tables("src", one)
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
    sees it: streaming pauses while the table is re-read. The exception is
    a MySQL source running GTID. There the connector takes its watermarks
    from the executed GTID set (`read.only=true`), and incremental needs no
    table. Measured on this pipeline: 50 rows re-read, the stream running
    throughout, and a key changed before its chunk carried the new value.
    `stream_codegen` turns it on where the source allows, and the repair
    asks for `kind="incremental"` wherever the pipeline has it.

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


def _gtid_on(hop):
    """Whether a MySQL source replicates by GTID - what the read-only
    re-snapshot takes its watermarks from. Anything short of a plain `ON`,
    or a server that cannot be asked, is no."""
    from .engines.mysql import MySQLEngine
    try:
        conn = MySQLEngine(hop)._conn("src", retry=False)
        try:
            with conn.cursor() as cur:
                cur.execute("select @@gtid_mode")
                got = cur.fetchone()
        finally:
            conn.close()
    except Exception:
        return False
    return bool(got) and str(got[0]).upper() == "ON"


def stream_reads_again_in_place(out):
    """Whether the pipeline in `out` can re-read a table without pausing
    the stream, from the connector configuration migkit wrote there."""
    import json as _json
    try:
        cfg = _json.loads((out / "source-connector.json").read_text())
    except (OSError, ValueError):
        return False
    return str(cfg.get("config", {}).get("read.only", "")).lower() == "true"


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
        # the names went out through the variables here, where the static
        # scan of messages could not see them
        raise SystemExit("this bulk path needs programs this machine does"
                         " not have - migkit doctor --install puts them in"
                         " place")
    if via == "pgcopydb" and not pgcopydb_available():
        raise SystemExit("this bulk path needs a component that is not"
                         " available on this machine - migkit doctor says"
                         " what is missing and migkit doctor --install"
                         " puts it in place")
    global _DEBUG
    from .wording import DebugLog
    _DEBUG = (DebugLog(hop.report_dir(db) / "commands.log"),
              (hop.source.password, hop.target.password))
    try:
        return fns[via](hop, db, workers, go, log)
    finally:
        _DEBUG = None
