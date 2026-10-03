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
import math
import re
import threading
import time
from urllib.parse import quote

from . import envkeys, leftovers, runner, toolversion, tooluse
from .runner import run
from .config import scoped as _scoped
from .tools import IMAGES, PROGRAM
from .tools import names as tool_names
from .tooluse import TOPPED, UNUSED, USED
from .util import PrivateFile, report_name, tool_env, which  # noqa: F401

VIAS = ("auto", "builtin", "pgdump", "pgcopydb", "mydumper", "mysqlsh",
        "pgloader", "mongodump", "mongosync", "riotx", "native", "pgpull",
        "pgsubscribe", "redisshake", "mirror")


def _native(engine):
    """Whether the engine copies a table by its own means - server to
    server, or file to file - with no row through this process."""
    from .engines import _class_for
    cls = _class_for(engine)
    return cls is not None and callable(getattr(cls, "native_bulk", None))


def _moves(via):
    from .decide import Need
    return Need("an engine it moves", lambda f: supported(f["engine"], via),
                "{rung} does not move this engine's hops")


def _installed(*programs):
    from .decide import Need
    return Need("its programs installed",
                lambda f: all(which(p) for p in programs))


def _ladder():
    """The ways a whole database moves, in the order the measurements
    gave: the first that holds is taken (`decide.climb`). Ranked by the
    list and not by a hop's rates: a whole run's rate at one size does not
    carry to another, and a database is not moved twice to learn which
    way is faster - the benchmark (item 28) ranks them."""
    from . import legs
    from .decide import Need, Rung
    return legs.rungs() + (
        # the version-matched container first: it overlaps copy, index
        # and constraint work, builds indexes after the rows land, and
        # needs no intermediate directory - none of which the dump path
        # does
        Rung("pgcopydb", "the streaming copy",
             needs=(_moves("pgcopydb"),
                    Need("its version-matched build",
                         lambda f: pgcopydb_available())),
             because="copy, index and constraint work overlap, and indexes"
                     " are built after the rows land"),
        Rung("pgdump", "the dump and restore in parallel",
             needs=(_moves("pgdump"), _installed("pg_dump", "pg_restore"))),
        # the MySQL bulk copy: each table by the rung the engine's table
        # ladder gives it (`tableway.choose`), the straight copy alone where
        # the dump and load programs are not here
        Rung("mydumper", "the parallel dump and load",
             needs=(_moves("mydumper"),
                    _installed("mydumper", "myloader"))),
        Rung("mysqlsh", "the straight copy",
             needs=(_moves("mysqlsh"), _installed("mysqlsh"))),
        Rung("pgloader", "the one-pass load",
             needs=(_moves("pgloader"), _installed("pgloader"))),
        # online, and consistent as of its commit - a dump of a source
        # still taking writes is neither; `fitted` falls back where the
        # hop or the servers cannot take it
        Rung("mongosync", "the online sync",
             gives=frozenset({"online", "consistent as of"}),
             needs=(_moves("mongosync"), _installed("mongosync")),
             because="online, and consistent as of its commit"),
        Rung("mongodump", "the dump and restore",
             needs=(_moves("mongodump"),
                    _installed("mongodump", "mongorestore"))),
        # Redis's own tool, where its terms allow it: `fitted` gives way
        # to migkit's copier for any other target
        Rung("riotx", "the Redis products' own copy",
             needs=(_moves("riotx"), _installed("riotx")),
             because="many keys at once, and a cluster's nodes side by"
                     " side"),
        # the replica protocol's copy of a keyspace, and Kafka's own
        # cluster mirror: each ranked against migkit's copier on the hop's
        # measured rate (`bulkways`), where the list's order only starts it
        Rung("redisshake", "the keyspace copy by the replica protocol",
             needs=(_moves("redisshake"), _installed("redis-shake")),
             because="one snapshot of the whole keyspace as of the moment"
                     " the source forked"),
        Rung("mirror", "the cluster mirror",
             needs=(_moves("mirror"),
                    Need("its runner here", lambda f: bool(_mirror_runner()),
                         "the cluster mirror is not installed here")),
             because="Kafka's own Java clients, many partitions at once"),
        Rung("native", "the engine's own means", needs=(_moves("native"),)),
        # a single table goes by the table copier: resuming by chunk
        # matters more than raw speed there
        Rung("builtin", "the table copier",
             gives=frozenset({"resume by chunk", "row filter",
                              "column mapping"})),
    )


#: what a stop costs each bulk way, as the plan says it before it starts:
#: a way not named starts over, and every table it had copied is copied
#: again (DTS says it as "resumable dump status"; nobody here did)
AFTER_A_STOP = {
    "builtin": "a stop goes on from the ranges it had finished",
    "pgcopydb": "a stop goes on from the tables it had finished - a table"
                " cut in parts, a copy run in a container, or one begun"
                " under another scope starts over",
    "mysqlsh": "a stop goes on loading from the dump it kept, where the"
               " source has not been written since; otherwise it starts"
               " over",
}


def after_a_stop(via):
    """What a stop costs the bulk way `via`, in words (`AFTER_A_STOP`)."""
    return AFTER_A_STOP.get(via, "a stop starts it over: every table it"
                                 " had copied is copied again")


def pick(engine, table="", hop=None, go=False):
    """Fastest installed tool for whole-db moves, builtin for single tables
    (chunk resume matters more than raw speed there)."""
    return choice(engine, table, hop, go).path


def choice(engine, table="", hop=None, go=False):
    """`pick`'s whole answer, the reason with it. With the `hop`, the ways
    that need its leave and ask its servers are weighed too; a way that
    proves itself only by acting on a server is proved when the move goes
    (`go`), and a dry run names it on its needs alone."""
    from . import legs, sourcechange
    from .decide import Facts, climb
    facts = Facts({"engine": engine, "hop": hop, "go": go,
                   "db": legs._first_db(hop) if hop is not None else None},
                  legs=lambda: legs.facts(hop),
                  rtt=lambda: legs.round_trips(hop))
    ladder = _ladder()
    if hop is not None:
        # a way that needs a change on the source takes the hop's answer
        # to it (`sourcechange`): offered, never assumed
        sourcechange.put_answers(hop, facts, ladder)
    return climb(table or engine, ladder, facts,
                 ("resume by chunk",) if table else ())


def chosen(engine, table="", hop=None, go=False):
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
    return pick(engine, table, hop, go)


def for_hop(hop, engine, table="", go=False):
    """(via, why the ways above it were passed over, or "") for a move of
    `hop`: the ways that take the rows from server to server are climbed
    only where the hop allows them, and otherwise the choice is `chosen`'s
    exactly as it always was - one call for the move, `assess`, `doctor`
    and the plan."""
    import os

    from . import legs
    if legs.allowed(hop) and not os.environ.get("MIGKIT_MOVER", "").strip():
        got = choice(engine, table, hop, go)
        return got.path, got.reason
    return chosen(engine, table), ""


def fitted(hop, engine, via):
    """`via`, or the table copier where `via` cannot carry what this hop
    asks of it - with the reason, in words, or None.

    The one-pass cross-engine load is chosen for every cross-engine hop
    once it is installed, and the load file migkit writes for it reads
    MySQL into PostgreSQL, the whole database, with nothing else: no
    exclude or include list, no table or column mapping, no row filter.
    For any other pair it reads the wrong server as the wrong kind; on a
    hop that leaves tables out it loads the tables the target owns. The
    table copier carries all of those, so the decision is made here, from
    the hop.

    `via` stands first with what it needs of the hop (`_FITS`), and the
    ways below it in `_ladder` are the fallback: one ladder for the pick
    and the fit, so the two cannot disagree on what comes next.
    """
    from . import bulkways
    if bulkways.measured(engine):
        return bulkways.fitted(hop, engine, via)
    if via not in _FITS:
        return via, None
    import dataclasses

    from .decide import Facts, climb
    from .engines import ALIASES
    opts = hop.options or {}
    facts = Facts(
        {"engine": engine, "hop": hop},
        pair=lambda: tuple(ALIASES.get(n, n) for n in (
            opts.get("source_engine", "mysql"),
            opts.get("target_engine", "postgres"))),
        mapping=lambda: getattr(hop, "mapping", None) or {},
        # asked of the servers, once, and only for the online sync
        unfit=lambda: _mongosync_unfit(hop),
        # asked of the target, and only for the Redis products' copy
        terms=lambda: _riotx_unfit(hop))
    ladder = _ladder()
    at = [r.name for r in ladder].index(via)
    got = climb(hop.name, (dataclasses.replace(ladder[at], needs=_FITS[via]),)
                + ladder[at + 1:], facts)
    if got.path == via:
        return via, None
    return got.path, next(why for r, why in got.passed if r.name == via)


def _fits():
    from .decide import Need
    one_pass = (
        Need("MySQL into PostgreSQL",
             lambda f: f["pair"] == ("mysql", "postgres"),
             "the one-pass load reads MySQL into PostgreSQL only"),
        # an include as much as an exclude: asked of `exclude` alone, a hop
        # taking some tables got the whole database, and the target's step
        # that empties it judged `public.orders` against `appdb.orders` -
        # an included table left full, and the load appended to it
        Need("nothing left out", lambda f: not _scoped(f["hop"]),
             "the one-pass load has no way to leave out the tables the hop"
             " excludes, or to take only the ones it includes"),
        Need("no mapping", lambda f: not any(
            f["mapping"].get(k) for k in ("tables", "columns", "where")),
            "the one-pass load reads no table, column or row mapping, and"
            " the hop has one"))
    return {"pgloader": one_pass,
            "mongosync": (Need("a hop it can carry",
                               lambda f: f["unfit"] is None,
                               lambda f: f["unfit"]),),
            "riotx": (Need("a target its terms allow",
                           lambda f: f["terms"] is None,
                           lambda f: f["terms"]),)}


#: what a way needs of the hop beyond its engine and its programs, asked
#: by `fitted` of the way the pick took
_FITS = _fits()


#: Movers that can be given a row predicate per table, measured rather
#: than assumed. mydumper takes one section per table in a defaults file
#: (verified: 2 of 3 rows dumped where a rule applied). `pg_dump` 18.6 and
#: `pgcopydb` 0.18 have `-t`, `-T`, `--exclude-table-data` and `--filter` /
#: `--filters` between them and **not one row predicate** - their filtering
#: is table-level throughout. The straight copy takes a `where` per table
#: too, and both carry it into their checksum.
ROW_FILTER_MOVERS = ("mydumper", "mysqlsh")

#: Bulk paths that can leave a table out, so a table with a row filter can
#: be carried by the table copier instead.
ROUTING_MOVERS = ("pgdump", "pgcopydb", "native", "pgpull", "pgsubscribe")

#: Bulk paths that write a table under the name the hop gives it on the
#: target (`mapping.tables`); on every other one a renamed table is left
#: out of the bulk copy and carried by the copier that renames it - the
#: rest loaded it under the source's name
RENAMING_MOVERS = ("mongodump",)

#: Bulk paths that carry only a table with a key: a temporary
#: subscription publishes the tables it copies, and a table without a key
#: in a publication makes the source refuse its application's updates and
#: deletes while it is there
NEEDS_KEYS = ("pgsubscribe",)

#: Bulk paths that write a local copy of the source and load it back: the
#: disk of the machine they run on writes every byte and reads it again
LOCAL_COPY = ("pgdump", "mydumper", "mongodump")

#: Engines whose table copier (`move_table`) applies the row filter on
#: both ends - reading only what it selects, replacing only what it
#: selects on the target. The pair's copier does it in each side's own
#: SQL, for a hop between engines and for every engine that copies
#: through the pair; a side that speaks no SQL is refused by it.
FILTERING_COPIERS = ("postgres", "mysql", "hetero", "sqlite")


def _routes_any(hop, db):
    """Whether a table of `db` may leave the bulk path: excluded, under a
    row filter, with its columns mapped, or renamed (`hop.table_map()`) -
    a rename was not asked, so a bulk path never listed its tables and
    loaded the renamed one under the source's name. The table list is
    asked for only then; `routed_to_copier` decides table by table."""
    return bool(_scoped(hop) or _filtered_here(hop, db)
                or (getattr(hop, "mapping", None) or {}).get("columns")
                or renames_any(hop))


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


def routed_to_copier(hop, db, via, tables, qualifier="public", choice=None,
                     facts=None, roots=None):
    """The tables a bulk path cannot filter, carried by the table copier.

    `via` copies whole tables; the copier reads and replaces only the rows
    a filter selects. So a table with a row filter is left out of the bulk
    copy and moved on its own, and the rest still go the fast way - where
    this used to refuse the whole database. One answer for the dump, the
    plan and the move, so none of them can disagree about which tables took
    which path. Nothing is routed for a path that applies the filter itself.
    `choice` is the table ladder's per-table choice where the caller made
    it already (`tableway.choose`); `roots` the source's partitions' roots
    (`_pg_roots`), so a partition goes where its table goes.
    """
    from . import planner
    if facts is None and via in NEEDS_KEYS:
        # which tables have a key is a fact of the source's
        from .engines import get_engine
        facts = get_engine(hop).table_facts("src", db)
    return [d.table for d in planner.plan(hop, db, via, tables, qualifier,
                                          facts, choice=choice, roots=roots)
            if d.path == planner.COPIER]


def refuse_unpushable_filters(hop, db, via, engine=None):
    """Stop a move whose row filters nothing on its path can apply.

    A filter is never silently dropped. It is applied by the bulk copy
    (`ROW_FILTER_MOVERS`), or its tables are routed to a table copier that
    applies it (`routed_to_copier`), or the move stops here - before
    anything is copied, because a path that ignores the filter copies every
    row and leaves the check comparing a filtered source against a full
    target for good.

    Masking on the way is held to the same rule: only the parallel dump
    masks (`mask_plan`), so any other path would copy the very values the
    hop asks to hide, and stops here instead.
    """
    masks = mask_rules(hop, db)
    if masks and via != "mydumper":
        raise SystemExit(
            f"the hop masks {', '.join(sorted(masks))} on the way, and only"
            " the parallel dump of a MySQL hop can: this path would copy the"
            " values as they are. Nothing has been copied. Install it with"
            " migkit doctor --install, or drop the masking rule")
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
            "pgpull": engine == "postgres",
            "pgsubscribe": engine == "postgres",
            "mydumper": engine == "mysql",
            "mysqlsh": engine == "mysql",
            "pgloader": engine == "hetero",
            "mongodump": engine == "mongodb",
            "mongosync": engine == "mongodb",
            "riotx": engine == "redis",
            "redisshake": engine == "redis",
            "mirror": engine == "kafka",
            "native": _native(engine)}.get(via, True)


def qualifier_for(hop):
    """What a table name with no schema is qualified by in a plan: the
    schema PostgreSQL puts it in, and nothing elsewhere."""
    from .engines import ALIASES
    return ("public" if ALIASES.get(hop.engine, hop.engine) == "postgres"
            else None)


def native_move(hop, db, workers, go, log):
    """The tables the plan sends the fast way, copied by the engine's own
    means (`native_bulk`); the rest are left to the table copier, as every
    routing bulk path leaves them."""
    from . import planner
    from .engines import get_engine
    eng = get_engine(hop)
    plan = planner.plan(hop, db, "native", eng.neutral_tables("src", db),
                        qualifier_for(hop), eng.table_facts("src", db))
    tables = [d.table for d in plan if d.path == planner.BULK]
    # a table routed for its row filter alone keeps the source's shape: the
    # engine makes it as it makes the rest, and the copier fills it - not
    # one it renames, which the engine would make under the source's name
    shaped = [d.table for d in plan if d.path == planner.COPIER
              and not hop.column_rules(db, *_asked(hop, d.table))
              and not _renamed(hop, db, _asked(hop, d.table))]
    return eng.native_bulk(db, tables, go, log, shape_only=shaped) or []


def stream_supported(engine):
    """Internal: can migkit stand up its managed streaming pipeline for this
    engine. Never an operator choice; `move --mode cdc` falls back to it
    when the engine has no native CDC path."""
    return engine in ("postgres", "mysql", "hetero")


#: the programs the bulk paths drive, by every name each goes by, read off
#: the registry (`tools.PROGRAM`). Their names never reach the operator:
#: the command lines go to the run's debug log, and what a failing one
#: said is passed on with its name taken out (`runner`)
DRIVEN = tuple(sorted(tool_names()))


def _program_role(program):
    """What a report calls a program: the dump's, the load's, the copy's
    - never its name (`tools.PROGRAM`)."""
    from .tools import ROLES
    return ROLES.get(str(program).rsplit("/", 1)[-1], "copy program")


def _still_running(path):
    """[(pid, program, since)] a run that is gone listed and that still
    run (`runner.still_running`): a listing whose process is gone, or is
    now another process under the same number, is dropped."""
    return runner.still_running(path)


def _sh(cmd, env=None, log=None, progress=None, what="", stall=None,
        timeout=None, retries=0):
    """Run one program through the runner, and keep its command line off
    the screen.

    This printed `$ <command line>` to the operator for every program it
    ran, and let a failing program's own message out as it was
    (`pg_restore: error: ...`). The command line now goes to the run's
    debug log with every secret removed, and a failure keeps the
    database's words and loses the program's name (`runner.classify`).
    What the operator reads while the step runs is the phase line its
    caller logs. With a progress reader the program's two streams are read
    as one, so a program that fills the other cannot stall while this one
    is read; the reader's lines are migkit's, the rest is kept only for
    the words of a failure.
    """
    return runner.run(cmd, env, progress=progress, log=log,
                      merge=progress is not None, what=what, stall=stall,
                      timeout=timeout, retries=retries)


#: {table: bytes} of the database the running bulk path carries, from the
#: source's catalogue; set by `run_via` so every progress reader counts
#: against it
_SIZES = None


def _source_sizes(hop, db):
    """{table: bytes} the move carries, the hop's exclusions left out; {}
    where the catalogue says nothing."""
    from .engines import get_engine
    try:
        eng = get_engine(hop)
        facts = eng.table_facts("src", db)
    except Exception:  # noqa: BLE001 - progress without sizes, then
        return {}
    # a partition is carried where its table is (`Engine.in_scope`)
    return {name: int(f.get("bytes") or 0) for name, f in facts.items()
            if isinstance(f, dict) and f.get("bytes")
            and eng.in_scope(db, name)}


def _tables_done(pattern, verb, total=None):
    """A progress reader for a program that names each table as it
    reaches it: `pattern` has a `table` group. Answers migkit's line for a
    matching line - `public.orders: read (3 of 12 tables, 1.2 GB of 4.8 GB
    (25%), 40.0 MB/s, about 1m30s left)` - and None for everything else."""
    from .wording import Tally
    tally = Tally(_SIZES, total)
    rx = re.compile(pattern)

    def read(line):
        m = rx.search(line)
        if not m:
            return None
        so_far = tally.reached(m.group("table"))
        if so_far is None:
            return None
        return f"{m.group('table')}: {verb} ({so_far})"
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


#: what the PostgreSQL dump and load say, with `-v`, as they reach each
#: table
PG_DUMP_TABLE = r'dumping contents of table "(?P<table>[^"]+)"'
PG_RESTORE_TABLE = r'processing data for table "(?P<table>[^"]+)"'


#: The target's user tables, partitioned ones too, one per line: the name
#: as the server quotes it, which the statement is built from, then its
#: schema and its own name, which it is matched by. This used to assemble
#: the whole `truncate` statement with `string_agg`, which put "which
#: tables count as the application's" in SQL while "which tables the hop
#: excludes" stayed in Python - so the two never met and the excluded ones
#: were emptied.
PG_USER_TABLES_SQL = (
    "select format('%I.%I', n.nspname, c.relname)"
    "||chr(31)||n.nspname||chr(31)||c.relname"
    " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
    " where c.relkind in ('r', 'p')"
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
        f"{db}: the hop excludes tables, filters their rows or renames"
        f" them, and the source's table list could not be read ({why}), so"
        " the copy cannot be told to skip them. Nothing has been changed"
        " on the target.")


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
    # a table only the target has is never emptied, and a filtered one
    # only of the rows its filter selects (`_pg_truncate_target`)
    rows = ("; of a table the hop filters, only the rows its filter"
            " selects" if _filtered_here(hop, db) else "")
    if _scoped(hop):
        return (f"# empty the target's{where} user tables the source has"
                " too, except the ones the hop excludes or does not take"
                f" (generated from catalog){rows}")
    return (f"# empty all the target's{where} user tables the source has"
            f" too (generated from catalog){rows}")


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

    **Only what the move carries is emptied** (`_pg_carried`): it was every
    target table the hop did not exclude, so a table the source does not
    have at all - the target's own, never named in `exclude` - was emptied
    and nothing refilled it, and a renamed table was emptied under the
    name the load did not fill. A partition goes as its table goes, on the
    target as on the source.

    **A table under a row filter is not emptied whole**: the move carries
    only the rows the filter selects, so those are deleted and the rest -
    the target's own - stay, as the table copier keeps them. One
    transaction with the truncate, so a delete the target refuses empties
    nothing. Where the pair's copier carries it (`through_pair`), the
    filter may name columns the target calls otherwise, and that copier
    replaces the rows it selects itself.

    **A table the hop carries only some changes of** (`mapping.ops`) is not
    emptied over the rows the target holds of it - those are what its
    contract keeps (`_refuse_kept_rows`).
    """
    from .util import split_table
    t = hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    env_t = {**t.libpq_env(),
             "PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}

    def psql(*args):
        return _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
                    "-d", ddb, "-X"] + list(args), env_t)

    carried = _pg_carried(hop, db)
    there = {}
    for ln in psql("-At", "-c", PG_USER_TABLES_SQL).stdout.splitlines():
        quoted, sch, tbl = (ln.split("\x1f") + ["", ""])[:3]
        if quoted.strip():
            there[(sch, tbl)] = quoted
    # the target's partitions by its own tree, which need not be the
    # source's: one the source lacks is still its table's
    tree = _pg_target_tree(hop, db) if there else {}
    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    from .config import OPS
    keep, filtered, spared, some = [], {}, {}, {}
    for key, quoted in sorted(there.items(), key=lambda kv: kv[1]):
        own = key in carried
        asked = carried.get(key) or carried.get(tree.get(key))
        if asked is None:
            spared[quoted] = None
            continue
        rf = hop.row_filter(db, *asked)
        if hop.carries(db, *asked) != frozenset(OPS):
            some[quoted] = rf
        if rf:
            # the rows outside the filter are the target's own
            spared[quoted] = rf
            if own and not eng.through_pair(db, ".".join(asked)):
                filtered[quoted] = rf
            continue
        keep.append(quoted)
    if not keep and not filtered:
        return ""
    _refuse_kept_rows([n for n in keep + sorted(filtered) if n in some
                       and _pg_holds_own_rows(psql, n, some[n], True)])
    if keep and spared:
        reached = {ln for ln in psql("-At", "-c", PG_CASCADE_REACH_SQL.format(
            names=_pg_literal(chr(31).join(keep)))).stdout.splitlines()
            if ln.strip()}
        # what the hop excludes is refused whether or not it holds rows
        # now - the target writes them while the move runs; any other
        # table only where emptying it would lose some
        by_name = {there[k]: ".".join(v) for k, v in tree.items()
                   if k in there}
        owned = set(excluded_tables(hop, db, [n for n in spared
                                              if n in reached],
                                    roots=by_name))
        caught = [n for n in sorted(spared) if n in reached
                  and (n in owned or _pg_holds_own_rows(psql, n, spared[n]))]
        if caught:
            # in words for each kind: excluded, filtered, the target's own
            filters = any(spared[n] for n in caught if n not in owned)
            alone = any(spared[n] is None for n in caught if n not in owned)
            raise SystemExit(
                f"{', '.join(caught)} would be emptied anyway: the table"
                " references one of the tables this move replaces, and"
                " emptying a table empties everything pointing at it.\n"
                + ("  the hop excludes it, which says its rows are written"
                   " here and not carried, so they cannot be put back"
                   " afterwards.\n  either it is not target-owned after all"
                   " - take it out of the hop's exclude list and let the"
                   " move carry it - or the reference has to go before the"
                   " move can run.\n" if owned & set(caught) else "")
                + ("  the hop carries only the rows its filter selects into"
                   " it, and the rows outside the filter are the target's"
                   " own, so they cannot be put back afterwards. Either the"
                   " filter goes, or the reference has to go before the"
                   " move can run.\n" if filters else "")
                + ("  the source has no such table, so the move does not"
                   " carry it and its rows are the target's own, which"
                   " cannot be put back afterwards. Either the reference"
                   " goes before the move can run, or the hop leaves out"
                   " the table it points at." if alone else ""))
    stmts = (["truncate table " + ", ".join(keep) + " cascade"] if keep
             else []) + [f"delete from {n} where {rf}"
                         for n, rf in sorted(filtered.items())]
    stmt = "; ".join(stmts)
    # one statement string, one transaction: a delete the target refuses
    # undoes the truncate before it
    _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
         "-d", ddb, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c", stmt],
        env_t, log)
    if filtered and log:
        log(f"{len(filtered)} tables the hop filters keep their rows outside"
            " the filter: only the rows it selects were removed")
    _pg_clear_carried_objects(hop, db, env_t, log)
    return stmt


def _refuse_kept_rows(tables):
    """Stop before the target is emptied where a table the hop carries
    only some changes of (`Hop.carries`, `mapping.ops`) holds rows there
    the emptying would take: the target keeps what the source deletes, or
    the rows as they were before the source changed them, and a load that
    empties the table first would lose exactly those. A table the target
    holds none of is filled as the copy of the source it then is."""
    if tables:
        raise SystemExit(
            f"{', '.join(tables[:6])}" + (" ..." if len(tables) > 6 else "")
            + ": the hop carries only some of their changes (mapping.ops),"
            " so the rows the target holds of them are kept as they are - a"
            " row the source deleted, or changed, stays as it was there -"
            " and emptying them for the load would take those rows."
            " Nothing has been emptied. Move them while the target holds"
            " none of their rows, or follow only their changes"
            " (`migkit move --mode cdc`)")


def _pg_holds_own_rows(psql, name, rf, inside=False):
    """Whether target table `name` holds rows the move would lose by
    emptying it: any at all where the move does not carry it (`rf` None),
    any outside its row filter where it does - or, `inside`, any the
    filter selects (every one without a filter), which a table whose
    changes the hop carries only some of keeps. Asked, a table that holds
    none costs nothing; one that cannot be asked is taken to hold some."""
    where = ((f" where {rf}" if rf else "") if inside else
             f" where ({rf}) is not true" if rf else "")
    try:
        got = psql("-At", "-c", f"select exists(select 1 from {name}{where})"
                   ).stdout.strip()
    except Exception:  # noqa: BLE001 - not known, so not risked
        return True
    return got != "f"


def _pg_carried(hop, db):
    """{(schema, table) on the target: the parts the hop's rules are asked
    with} of every table of `db` the move carries: the source's tables the
    hop takes, each under the name the hop gives it on the target - a
    partition as its table is taken (`_pg_roots`), and only through its
    table where that is renamed. One reading for the step that empties
    the target and for the window that sets its indexes aside: both reached
    every target table the hop did not exclude, so one the source does not
    have was emptied, and a renamed one looked for under its old name."""
    from .engines import ALIASES
    from .util import split_table
    source = ((hop.options or {}).get("source_engine", "")
              if hop.engine == "hetero" else hop.engine)
    same = ALIASES.get(source, source) == "postgres"
    if same:
        from .engines.postgres import PostgresEngine
        eng = PostgresEngine(hop)
        names = eng._all_tables("src", db)
        roots = _pg_roots(hop, db, eng=eng)
    else:
        # the one-pass load from another engine lands every table in
        # `public`, under its name in lower case (`pgloader_move`)
        from .engines import get_engine
        names = get_engine(hop).src_engine.neutral_tables("src", db)
        roots = {}
    out = {}
    for name in names:
        asked = _asked(hop, name, roots)
        if hop.excluded(db, *asked):
            continue
        if name in roots:
            if _renamed(hop, db, asked):
                # filled through its table, under that table's new name
                continue
            out[split_table("postgres", name, "public")] = asked
            continue
        sch = asked[0] if len(asked) > 1 else "public"
        there = str(hop.target_table_in(db, *asked))
        out[split_table("postgres", there if same else there.lower(),
                        sch)] = asked
    return out


def _pg_clear_carried_objects(hop, db, env_t, log=None):
    """The target's large objects that the load carries again, under the
    same oids, removed first - as the tables it fills are emptied. Left,
    the load stopped on each of them (measured: `duplicate key value
    violates unique constraint "pg_largeobject_metadata_oid_index"`, the
    target already holding what an earlier move had carried). One the
    source does not have is left as it is. A source of another engine
    has none to carry: asking a MySQL source for them stopped the one-pass
    load before it began (measured, `received invalid response to SSL
    negotiation`, psql speaking to MySQL's port)."""
    from .engines import ALIASES
    source = (hop.options or {}).get("source_engine", hop.engine)
    if ALIASES.get(source, source) != "postgres":
        return
    s, t = hop.source, hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    got = _sh(["psql", "-h", s.host, "-p", str(s.port), "-U", s.user,
               "-d", db, "-X", "-At", "-c",
               "select coalesce(string_agg(oid::text, ','), '') from"
               " pg_largeobject_metadata"],
              {**s.libpq_env(), "PGPASSWORD": s.password,
               "PGCONNECT_TIMEOUT": "15"}).stdout.strip()
    if not got:
        return
    _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user, "-d", ddb,
         "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c",
         "select count(lo_unlink(oid)) from pg_largeobject_metadata where"
         f" oid = any('{{{got}}}'::oid[])"], env_t, log)


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
    env = {**t.libpq_env(),
           "PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    return _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
                "-d", ddb, "-X", "-At", "-v", "ON_ERROR_STOP=1",
                "-c", sql], env, log).stdout


def _outside_exclusion(hop, db, rows, log=None, qualifier="public",
                       fills=None):
    """The index `rows` - table first - on tables the hop does not exclude,
    and - where `fills` says which tables the load fills (`_pg_fills`,
    `_my_carried`) - only on those: a table the source does not have is the
    target's own as much as an excluded one, and its indexes were dropped
    for a load that put nothing in it.

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
    alone = {t for t in {r[0] for r in rows} - owned
             if fills is not None and not fills(t)}
    kept = [r for r in rows if r[0] not in owned | alone]
    if owned and log:
        log(f"{sum(r[0] in owned for r in rows)} indexes on {len(owned)}"
            " tables the hop excludes were left in place")
    if alone and log:
        log(f"{sum(r[0] in alone for r in rows)} indexes on {len(alone)}"
            " tables the move does not fill were left in place")
    return kept


def _pg_target_tree(hop, db):
    """{(schema, table): (schema, table) of its root} of the target's
    partitions (`_pg_roots` on the target): a partition the source does
    not have is still part of its table there."""
    from .util import split_table
    return {split_table("postgres", k, "public"):
            split_table("postgres", v, "public")
            for k, v in _pg_roots(hop, db, "dst").items()}


def _pg_fills(hop, db):
    """`fills(name)`: whether the move fills the target table `name`
    (`schema.table`) - one it carries (`_pg_carried`), or a partition of
    one by the target's own tree."""
    from .util import split_table
    carried = _pg_carried(hop, db)
    tree = _pg_target_tree(hop, db)

    def fills(name):
        key = split_table("postgres", str(name), "public")
        return key in carried or tree.get(key) in carried
    return fills


def _rebuild(names, build, workers, log):
    """(rebuilt, failed): every index `build` makes, as many at once as
    the move has workers - each on a session of its own, so the server
    builds them side by side, where one after another left all but one
    of the target's cores idle for the length of the rebuild."""
    import concurrent.futures as cf
    rebuilt, failed = [], []

    def one(name):
        try:
            build(name)
            return name, None
        except Exception as e:  # noqa: BLE001 - said, and the rest go on
            return name, e
    with cf.ThreadPoolExecutor(max(1, min(int(workers or 1),
                                          len(names) or 1))) as pool:
        for name, err in pool.map(one, list(names)):
            if err is None:
                rebuilt.append(name)
                continue
            failed.append(name)
            if log:
                log(f"REBUILD FAILED for {name}:"
                    f" {str(err).splitlines()[-1][:100]}")
    return rebuilt, failed


#: one window at a time takes over what a dead load set aside
_SET_ASIDE_CLAIM = threading.Lock()


class Beside:
    """Index builds of a finished table run while the next tables copy.

    A table's copier set its secondary indexes aside and built them on its
    way out, holding its place among the tables copied at once until the
    last index was built - so the next table waited for another table's
    index builds, and the target's cores the builds left idle went
    unused. Handed here, a table's builds run on a thread of their own,
    one table's at a time, and the copier goes on to the next table; the
    copy of the database waits for them before it ends (`wait`), and a
    build that failed is said and left to the record the window keeps, as
    it was when it ran inline. A move stopped meanwhile leaves the
    indexes to that record: the next run builds them.
    """

    def __init__(self, log=None):
        from concurrent.futures import ThreadPoolExecutor
        self.log = log
        self._pool = ThreadPoolExecutor(1, thread_name_prefix="migkit-index")
        self._futs = []

    def run(self, fn):
        self._futs.append(self._pool.submit(fn))

    def wait(self):
        self._pool.shutdown(wait=True)
        failed = None
        for f in self._futs:
            if f.exception() is not None and failed is None:
                failed = f.exception()
        if failed is not None:
            raise failed


#: the builds of the copy in progress, seen from every thread copying it
_BESIDE = {"active": None}


class builds_beside:
    """The context a database's tables are copied in with their index
    builds beside the copy (`Beside`)."""

    def __init__(self, log=None):
        self.log = log
        self.beside = None
        self._before = None

    def __enter__(self):
        self._before = _BESIDE["active"]
        self.beside = _BESIDE["active"] = Beside(self.log)
        return self.beside

    def __exit__(self, *exc):
        try:
            self.beside.wait()
        finally:
            _BESIDE["active"] = self._before
        return False


def _pg_build_settings(hop, db, at_once):
    """The session settings each index build on a PostgreSQL target runs
    under, from what the target says of itself - never a server setting.
    `max_parallel_maintenance_workers`: the target's CPUs shared among the
    builds running at once, the leader counted, where its CPUs can be read
    (`capacity`); `maintenance_work_mem`: a share of `shared_buffers` - the
    memory the server was sized with - for each build at once, never below
    what the session already has and at most 2 GB. "" where neither can
    be told."""
    from .engines.postgres import PostgresEngine
    try:
        got = _pg_psql(hop, db, "select pg_size_bytes(current_setting("
                                "'maintenance_work_mem')) || '|' ||"
                                " pg_size_bytes(current_setting("
                                "'shared_buffers'))").strip()
        mwm, shared = (int(x) for x in got.split("|"))
        cpus = PostgresEngine(hop).capacity("dst", db).get("cpus")
    except Exception:  # noqa: BLE001 - the server's own defaults, then
        return ""
    at_once = max(1, int(at_once))
    out = []
    share = min(2 * 2 ** 30, shared // (2 * at_once))
    if share > mwm:
        out.append(f"set maintenance_work_mem = '{share // 1024}kB'")
    if cpus:
        out.append("set max_parallel_maintenance_workers ="
                   f" {max(0, int(cpus) // at_once - 1)}")
    return "; ".join(out) + ("; " if out else "")


def _before_last(sql, anchor, text):
    at = sql.rindex(anchor)
    return sql[:at] + text + sql[at:]


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

    def __init__(self, hop, db, workers, log, table=None, only_left=False):
        from .setaside import SetAside
        self.hop, self.db, self.workers, self.log = hop, db, workers, log
        self.table, self.only_left = table, only_left
        self.dropped, self.ddl = [], {}
        self.record = SetAside(hop, db, "dropped-indexes")

    def __enter__(self):
        from . import indexes as _ix
        sql = PG_INDEX_SQL
        if self.table:
            sch, tbl = self.table
            sql = _before_last(sql, "order by 1",
                               f"and n.nspname = {_pg_literal(sch)} and"
                               f" tb.relname = {_pg_literal(tbl)}\n     ")
        try:
            raw = ("" if self.only_left
                   else _pg_psql(self.hop, self.db, sql))
            there = set(_pg_psql(self.hop, self.db, PG_INDEX_NAMES_SQL
                                 ).split())
            # the whole database's load: only the tables it fills
            fills = (_pg_fills(self.hop, self.db)
                     if raw and not self.table else None)
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
                _outside_exclusion(self.hop, self.db, found, self.log,
                                   fills=fills)]
        if self.table:
            # one table's copy: an index that makes its rows unique stays -
            # a copier may find rows by it, and a load that let a duplicate
            # in would find out only at the rebuild
            rows = [r for r in rows
                    if not r[1].upper().startswith("CREATE UNIQUE ")]
        drop, ddl = _ix.plan(rows)
        # what a load that died dropped and the target still lacks: built
        # at the end of this one, with the rest. Claimed under a lock: two
        # tables copied side by side each found the dead load's record and
        # both built its indexes, and the second build failed on the first
        with _SET_ASIDE_CLAIM:
            left, stale = self.record.left_behind(there)
            if not drop and not left:
                return self
            if not self.record.save({**left, **ddl}, stale):
                if self.log:
                    self.log("could not save the index definitions, so none"
                             " were dropped - the load runs with them in"
                             " place")
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
        # one table's: built beside the next tables' copy where the
        # database's copy runs them there (`Beside`)
        beside = _BESIDE["active"]
        if beside is not None and self.table and self.dropped:
            beside.run(self._put_back)
            return False
        self._put_back()
        return False            # never swallow the load's own exception

    def _put_back(self):
        """Every index set aside built again, each build under the
        session settings the target's size gives (`_pg_build_settings`)."""
        from . import indexes as _ix
        settings = (_pg_build_settings(self.hop, self.db,
                                       min(int(self.workers or 1),
                                           len(self.dropped)))
                    if self.dropped else "")
        rebuilt, failed = _rebuild(
            self.dropped, lambda name: _pg_psql(self.hop, self.db,
                                                settings + self.ddl[name]),
            self.workers, self.log)
        # a table's own window, or one that only looked for what a dead
        # load left, says something only where it did something
        if self.log and (self.dropped or not (self.table or self.only_left)):
            self.log(_ix.summary(self.dropped, rebuilt, failed))
        if not failed:
            self.record.done()


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
    # exactly the same tables rather than three readings of one pattern.
    # Each partition is named beside its table: `-T` of a partitioned
    # table leaves out its own rows, which it has none of, and not its
    # partitions'
    skip, routed, unresolved, seqs = [], [], "", []
    if _routes_any(hop, db):
        try:
            from .engines.postgres import PostgresEngine
            tables = PostgresEngine(hop)._all_tables("src", db)
            roots = _pg_roots(hop, db)
            skip = excluded_tables(hop, db, tables, roots=roots)
            routed = [n for n in routed_to_copier(hop, db, "pgdump", tables,
                                                  roots=roots)
                      if n not in skip]
            seqs = _pg_sequences_left_out(hop, db)
        except Exception as e:
            unresolved = str(e).splitlines()[-1][:70] if str(e) \
                else type(e).__name__
    from .sizing import split
    from .wording import Step, phase
    left_out = skip + routed
    # each side as many jobs as carry it (`sizing.fit`)
    reads, writes = split(hop, workers)
    dump = Step(
        phase("dump", workers=reads, left_out=len(skip),
              routed=len(routed)),
        ["pg_dump", "-h", s.host, "-p", s.port, "-U", s.user, "-d", db,
         "-Fd", "-j", reads, "--data-only", "-v", "-f", outdir,
         # quoted, as the catalogue names it: unquoted, the dump folds a
         # name to lower case and leaves `"AuditLog"` in
         *[a for name in left_out + seqs for a in ("-T", _pg_pattern(name))],
         *leftovers.pg_dump_args()])
    load = Step(
        phase("load", workers=writes),
        ["pg_restore", "-h", t.host, "-p", t.port, "-U", t.user,
         "-d", hop.target_db(db), "--data-only", "--disable-triggers",
         "-v", "-j", writes, outdir])
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
                            " dumped at all"
                         + (f", nor the {len(seqs)} sequences that go with"
                            " them" if seqs else ""))
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)
    env_t = {**t.libpq_env(),
             "PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    from .engines.postgres import PostgresEngine
    quiet = _pg_quiet_triggers(
        hop, db, [n for n in PostgresEngine(hop)._all_tables("src", db)
                  if n not in left_out])
    if quiet != "flag":
        load.argv.remove("--disable-triggers")
    if quiet == "session":
        env_t["PGOPTIONS"] = "-c session_replication_role=replica"
    with _LocalCopy(outdir):
        if log:
            log(dump)
        _sh(dump.argv, {**s.libpq_env(), "PGPASSWORD": s.password,
                        "PGCONNECT_TIMEOUT": "15"},
            log, progress=_tables_done(PG_DUMP_TABLE, "read"))
        _pg_create_missing(hop, db, log)
        _pg_truncate_target(hop, db, log)
        with _IndexWindow(hop, db, workers, log):
            _pgdump_restore(load, env_t, log)
    _pg_finish_created(hop, db, log, workers)
    return steps


class _LocalCopy:
    """The local copy of the source a dump-and-load path makes: private
    while it exists, and gone when the path ends, whichever way it ends.

    It is the source's data, all of it, in files. It was removed only
    after a load that succeeded: a load that failed or was stopped left
    it in the report directory, readable by anyone who could read that,
    until the next run happened to start by removing it. Now it is made
    readable by this user only before anything is written into it, and
    removed on the way out."""

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        import os
        import shutil
        shutil.rmtree(self.path, ignore_errors=True)
        self.path.mkdir(parents=True)
        os.chmod(self.path, 0o700)
        return self.path

    def __exit__(self, *exc):
        import shutil
        shutil.rmtree(self.path, ignore_errors=True)
        return False


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
    it is read as a wildcard - the dump folds an unquoted pattern to lower
    case, and `-T public.AuditLog` left `"AuditLog"` in. Split the one way
    names are (`util.split_table`): a name the catalogue quoted
    (`public."AuditLog"`) is unquoted first, and one with no schema is in
    `public`."""
    from .util import split_table
    sch, tbl = split_table("postgres", str(name), "public")
    return ".".join('"' + p.replace('"', '""') + '"' for p in (sch, tbl))


def _pg_missing_tables(hop, db):
    """The tables in scope the target does not have, and whether it has
    no tables at all."""
    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    have = set(eng._all_tables("dst", db))
    # a table whose columns the hop maps, or that it renames, is built by
    # the pair's copier, under the names and without the columns the
    # mapping says - made here, it stood under the source's name; and a
    # partition goes as its table goes (`in_scope`, `_ruled`)
    want = [t for t in eng._all_tables("src", db) if eng.in_scope(db, t)
            and not eng.through_pair(db, ".".join(eng._ruled(db, t)))]
    return [t for t in want if t not in have], not have


def _pg_schema(hop, db, section, tables, whole, log=None, workers=1):
    """One section of the source's schema, restored onto the target.

    `whole` is a target with no tables at all: everything the source's
    section holds - its types, functions, sequences, tables, views - but
    the tables the hop excludes. Otherwise only `tables`, whose types and
    functions the target has to have already; a definition it cannot take
    stops the move with the server's words. Without owners or grants: the
    roles are `migkit users`' business, and `check` names the grants that
    differ. `workers`: the post-data section's keys and indexes built that
    many at once, as the move's sizing gives them, where they were built
    one at a time.

    What depends on a table the target is not given is left out with it
    (`_pg_depending`): the tables the hop leaves out - a partition as its
    table is (`partition_roots`) - and, where the target has tables, only
    those it lacks; and the tables the pair's copier makes later under
    the name the hop gives them (`through_pair`). A view over one made
    the restore refuse the whole schema; a foreign key into one stopped
    the move once the rows were in. So are the views, routines and
    triggers the hop's `objects` does not take (`objects_left_out`), and
    what depends on them. Each is said, a foreign key with the table it
    points at, and kept in the run's decision record.
    """
    from .engines.postgres import PostgresEngine
    s, t = hop.source, hop.target
    eng = PostgresEngine(hop)
    every = eng._all_tables("src", db)
    roots = _pg_roots(hop, db, eng=eng)
    skip = excluded_tables(hop, db, every, roots=roots)
    # built by the pair's copier after the load, under the name the hop
    # gives them (`through_pair`)
    paired = [n for n in every if n not in skip
              and eng.through_pair(db, roots.get(n, n))]
    if whole:
        gone = skip + paired
    else:
        # a table left out that the target has is the target's own, and
        # what refers to it finds it there
        have = set(eng._all_tables("dst", db))
        gone = [n for n in skip if n not in have] + paired
    # the objects the hop's `objects` does not take, and what reads them
    objects = eng.objects_left_out("src", db)
    gone += [n for k in ("view", "matview") for n in objects.get(k, ())]
    routines = [n for k in ("function", "procedure")
                for n in objects.get(k, ())]
    triggers = list(objects.get("trigger", ()))
    left = (_pg_depending(hop, db, gone, routines, triggers)
            if gone or routines or triggers else [])
    views = [x["view"] for x in left if x["view"]]
    if whole:
        pick = [a for n in skip + paired + views
                for a in ("-T", _pg_pattern(n))]
    else:
        pick = [a for n in tables for a in ("-t", _pg_pattern(n))]
    out = hop.report_dir(db) / f"schema-{section}.dump"
    listed = out.with_suffix(".list")
    dump = ["pg_dump", "-h", s.host, "-p", str(s.port), "-U", s.user,
            "-d", db, "-Fc", "--schema-only", f"--section={section}",
            *pick, *(leftovers.pg_dump_args() if whole else []),
            "-f", str(out)]
    load = ["pg_restore", "-h", t.host, "-p", str(t.port), "-U", t.user,
            "-d", hop.target_db(db), f"--section={section}", "--no-owner",
            "--no-privileges",
            *(["-j", str(int(workers))] if section == "post-data"
              and int(workers or 1) > 1 else []), str(out)]
    env_s = {**s.libpq_env(), "PGPASSWORD": s.password,
             "PGCONNECT_TIMEOUT": "15"}
    try:
        _sh(dump, env_s, log)
        # what depends on a table the target is not given is left out of
        # the restore by its own entry in the dump's list: a foreign key
        # into one stopped the move after its rows were in, a function
        # reading one before they were
        entries = {x["entry"]: x for x in left if not x["view"]}
        dropped = []
        if entries:
            text, dropped = _toc_without(
                _sh(["pg_restore", "-l", str(out)], env_s).stdout, entries)
            if dropped:
                listed.write_text(text)
                load[-1:-1] = ["-L", str(listed)]
        _schema_left_out(hop, db, section,
                         [x for x in left if x["view"]] if section ==
                         "pre-data" and whole else [],
                         [entries[k] for k in dict.fromkeys(dropped)], log)
        # a target with no tables can still have the schemas, types or
        # extensions someone made ready for it; those are left as they are
        _restore(load, {**t.libpq_env(),
                        "PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"},
                 log, "the schema", existing_ok=True)
    finally:
        out.unlink(missing_ok=True)
        listed.unlink(missing_ok=True)


#: What depends on the relations `{names}` lists, the routines
#: `{routines}` lists (`schema.name`) and the triggers `{triggers}` lists
#: (`schema.table.trigger`), at any depth: a view over one (by its rule), a
#: function whose body reads one, a trigger calling such a function, a
#: foreign key into one, a policy or rule naming one - as the class and
#: number the dump's own list names each entry by, what PostgreSQL calls
#: it, the view's name where it is one, the table a foreign key
#: references, and whether it is one of those listed (1) or reads one (0)
PG_DEPENDING_SQL = """
with recursive gone(classid, objid, own) as (
    select * from (
        select 'pg_class'::regclass::oid, c.oid, true from pg_class c
          join pg_namespace n on n.oid = c.relnamespace
         where n.nspname || '.' || c.relname
               = any (string_to_array({names}, chr(31)))
        union all
        select 'pg_proc'::regclass::oid, p.oid, true from pg_proc p
          join pg_namespace n on n.oid = p.pronamespace
         where n.nspname || '.' || p.proname
               = any (string_to_array({routines}, chr(31)))
        union all
        select 'pg_trigger'::regclass::oid, t.oid, true from pg_trigger t
          join pg_class c on c.oid = t.tgrelid
          join pg_namespace n on n.oid = c.relnamespace
         where n.nspname || '.' || c.relname || '.' || t.tgname
               = any (string_to_array({triggers}, chr(31)))) listed
    union
    select case when v.oid is null then d.classid
                else 'pg_class'::regclass::oid end,
           coalesce(v.oid, d.objid), false
      from gone g
      join pg_depend d on d.refclassid = g.classid and d.refobjid = g.objid
                      and d.deptype = 'n'
      left join pg_rewrite r on d.classid = 'pg_rewrite'::regclass
                            and r.oid = d.objid
      left join pg_class v on v.oid = r.ev_class and r.rulename = '_RETURN'
                          and v.relkind in ('v', 'm')
     where d.classid in ('pg_rewrite'::regclass, 'pg_proc'::regclass,
                         'pg_trigger'::regclass, 'pg_constraint'::regclass,
                         'pg_policy'::regclass)
       and coalesce(v.oid, 0::oid) <> g.objid
)
select distinct on (g.classid, g.objid) g.classid || ' ' || g.objid
       || chr(31) || coalesce(pg_describe_object(g.classid, g.objid, 0), '')
       || chr(31) || coalesce((select n.nspname || '.' || c.relname
                                 from pg_class c
                                 join pg_namespace n
                                   on n.oid = c.relnamespace
                                where g.classid = 'pg_class'::regclass
                                  and c.oid = g.objid
                                  and c.relkind in ('v', 'm')), '')
       || chr(31) || coalesce((select k.confrelid::regclass::text
                                 from pg_constraint k
                                where g.classid = 'pg_constraint'::regclass
                                  and k.oid = g.objid
                                  and k.contype = 'f'), '')
       || chr(31) || g.own::int
  from gone g
 where g.classid <> 'pg_class'::regclass
    or exists (select 1 from pg_class c where c.oid = g.objid
                                          and c.relkind in ('v', 'm'))
 order by g.classid, g.objid, g.own desc"""


def _pg_depending(hop, db, gone, routines=(), triggers=()):
    """[{"entry", "said", "view", "references", "own"}] of the source's
    objects the target is not given: what depends on the relations `gone`
    (`schema.table`) - tables the hop leaves out, or the pair's copier
    makes later under another name, and views its `objects` does not
    take - and the `routines` and `triggers` its `objects` does not take,
    with what depends on those. `own` is one the hop does not take itself;
    `entry` the class and number the dump's list names it by
    (`_toc_without`), `view` a view's name where it is one (left out of
    the dump by name, its comments with it), and `references` the table a
    foreign key points at."""
    from .engines.postgres import PostgresEngine
    got = PostgresEngine(hop)._psql("src", db, PG_DEPENDING_SQL.format(
        names=_pg_literal(chr(31).join(gone)),
        routines=_pg_literal(chr(31).join(routines)),
        triggers=_pg_literal(chr(31).join(triggers))))
    out = []
    for line in got.splitlines():
        entry, said, view, refs, own = (line.split("\x1f")
                                        + ["", "", "", ""])[:5]
        if entry:
            out.append({"entry": entry, "said": said, "view": view,
                        "references": refs, "own": own == "1"})
    return out


#: one entry of a dump's list: its number, then the class and number of
#: the object it makes
_TOC_ENTRY = re.compile(r"^\s*\d+;\s+(?P<cls>\d+)\s+(?P<oid>\d+)\s")


def _toc_without(listing, entries):
    """(the dump's list with `entries` - "class number" - commented out,
    the ones it held). The restore reads only the lines left: `;` begins
    a comment there, which is how its own documentation leaves an entry
    out."""
    out, dropped = [], []
    for line in listing.splitlines():
        m = _TOC_ENTRY.match(line)
        key = f"{m.group('cls')} {m.group('oid')}" if m else None
        if key in entries:
            dropped.append(key)
            line = ";" + line
        out.append(line)
    return "\n".join(out) + "\n", dropped


def _schema_left_out(hop, db, section, views, entries, log=None):
    """Say what the schema step left out of the target, and why: in the
    run's words and in its decision record, which is part of the
    evidence - a foreign key named with the table it points at, so the
    reader knows which reference the target does not enforce."""
    if not views and not entries:
        return
    from . import decide
    every = list(views) + list(entries)
    groups = (
        ([x for x in every if x["references"] and not x["own"]],
         "foreign keys are not made on the target - each refers to a table"
         " the hop does not take, or one the table copier makes later under"
         " another name"),
        ([x for x in every if x["own"]],
         "objects the hop does not take are not made there"),
        ([x for x in every if not x["references"] and not x["own"]],
         "objects that read a table or an object the target is not given"
         " are not made there"))
    said = [f"{x['said']} -> {x['references']}" if x["references"]
            else x["said"] for group, _ in groups for x in group]
    if log:
        for group, words in groups:
            if group:
                named = [f"{x['said']} -> {x['references']}"
                         if x["references"] else x["said"] for x in group]
                log(f"{db}: {len(group)} {words}: " + "; ".join(named[:8])
                    + (" ..." if len(group) > 8 else ""))
    decide.record(hop, {"work": f"the schema of {db} ({section})",
                        "left out": said},
                  work=f"the schema of {db}", way="left out of the schema",
                  reason="the hop does not take them, or they depend on what"
                         " the target is not given")


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


def _pg_finish_created(hop, db, log=None, workers=1):
    """The keys, indexes and constraints of the tables `_pg_create_missing`
    made, once their rows are in, `workers` at once."""
    import json
    record = _pg_created_path(hop, db)
    try:
        made = json.loads(record.read_text())
    except (OSError, ValueError):
        return
    if log:
        from .wording import phase
        log(phase("finish-created", tables=len(made["tables"])))
    _pg_schema(hop, db, "post-data", made["tables"], made["whole"], log,
               workers)
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
    empty - every table it fills (`_pg_fills`) but the ones routed to the
    table copier - as `referenced <- referencing`. [] when there are none,
    or when the target cannot be asked (the copy then says for itself).
    Each name split the one way names are: the catalogue quotes a name
    with case, and its quotes were taken for part of it."""
    from .util import split_table
    try:
        raw = _pg_psql(hop, db, PG_REFERENCES_SQL)
        fills = _pg_fills(hop, db)
    except Exception:
        return []
    away = {split_table("postgres", str(n), "public") for n in routed}
    out = []
    for line in raw.splitlines():
        ref, _, by = line.partition(chr(31))
        if not by or split_table("postgres", ref, "public") in away \
                or not fills(ref):
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
    env = {**t.libpq_env(),
           "PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
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


def _pg_sequences_left_out(hop, db):
    """The source's sequences the hop does not take, as `schema.sequence`
    (`PostgresEngine._seq_left_out`): a dump of the data alone still writes
    the value of a serial's sequence whose table it was told to leave out -
    it dumps a sequence its table owns with that table's flags and its own
    - so the counter of a table the target writes itself was set back to
    the source's. Left out of the dump by name; the carry after it leaves
    them too (`_pg_carry_sequences`)."""
    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    names = [line.rpartition("|")[0] for line in
             eng._psql("src", db, eng.SEQ_Q).splitlines() if "|" in line]
    return sorted(eng._seq_left_out("src", db, names))


def _pg_carry_sequences(hop, db, log=None):
    """Set the target's sequences from the source's after a copy that
    carries rows and not sequences.

    Measured, a streaming copy of 52 rows: the target's sequence stayed at
    1, and the application's first insert after cutover failed on
    `duplicate key ... Key (id)=(1) already exists`. The copy program's own
    sequence command, run on its own, read 0 sequences and reset the
    target's. The engine's sequence repair decides the value instead - the
    source's, never below a row the target holds, never below where the
    target's counter stands, and only of the tables the hop takes - and
    refuses a target already ahead of the source, which it says.
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
        # the whole database's load writes only the tables it carries: a
        # table the source does not have kept its triggers off for a load
        # that never reached it, while its own application wrote to it
        from .engines import ALIASES
        whole = (self.tables is None and on and ALIASES.get(
            self.hop.engine, self.hop.engine) == "mysql")
        tables = (set(_my_carried(self.hop, self.db, eng)) if whole
                  else self.tables)
        rows = [r for r in on
                if eng.in_scope(self.db, str(r[1]))
                and (tables is None or str(r[1]) in tables)]
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

    def __init__(self, engine, hop, db, workers, log, table=None,
                 only_left=False):
        from .setaside import SetAside
        self.eng, self.hop, self.db = engine, hop, db
        self.workers, self.log = workers, log
        self.table, self.only_left = table, only_left
        self.dropped, self.ddl = [], {}
        self.record = SetAside(hop, db, "dropped-indexes")

    def __enter__(self):
        from . import indexes as _ix
        ddb = self.hop.target_db(self.db) if hasattr(self.hop, "target_db") \
            else self.db
        try:
            rows = ([] if self.only_left
                    else self.eng._q("dst", MY_INDEX_SQL, (ddb,)))
            there = {f"{t}.{n}" for t, n in self.eng._q(
                "dst", "select distinct table_name, index_name from"
                       " information_schema.statistics"
                       " where table_schema = %s", (ddb,))}
            # the whole database's load: only the tables it fills
            filled = (set(_my_carried(self.hop, self.db, self.eng))
                      if rows and not self.table else None)
        except Exception:
            return self
        rows = _outside_exclusion(
            self.hop, self.db, list(rows), self.log, qualifier=self.db,
            fills=None if filled is None else (lambda t: t in filled))
        if self.table:
            # one table's copy, and its unique indexes stay (as PostgreSQL's)
            rows = [r for r in rows
                    if r[0] == self.table and not int(r[3] or 0)]
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
        with _SET_ASIDE_CLAIM:
            left, stale = self.record.left_behind(there)
            if not drop and not left:
                return self
            if not self.record.save({**left, **ddl}, stale):
                if self.log:
                    self.log("could not save the index definitions, so none"
                             " were dropped - the load runs with them in"
                             " place")
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
        # one table's: built beside the next tables' copy where the
        # database's copy runs them there (`Beside`)
        beside = _BESIDE["active"]
        if beside is not None and self.table and self.dropped:
            beside.run(self._put_back)
            return False
        self._put_back()
        return False

    def _put_back(self):
        from . import indexes as _ix
        # a table's indexes in one statement: InnoDB reads the table once
        # for all of them, where an ALTER each read it once each and waited
        # on the one before for the table's lock
        tables = {}
        for key in self.dropped:
            head, _, part = self.ddl[key].partition(" ADD INDEX ")
            tables.setdefault(head, []).append((key, part))
        together, apart = _rebuild(
            list(tables), lambda head: self.eng._q(
                "dst", head + " " + ", ".join(
                    f"ADD INDEX {part}" for _, part in tables[head])),
            self.workers, None)
        rebuilt = [key for head in together for key, _ in tables[head]]
        # where the one statement failed, one at a time, to say which
        more, failed = _rebuild(
            [key for head in apart for key, _ in tables[head]],
            lambda key: self.eng._q("dst", self.ddl[key]),
            self.workers, self.log)
        rebuilt += more
        if self.log and (self.dropped or not (self.table or self.only_left)):
            self.log(_ix.summary(self.dropped, rebuilt, failed))
        if not failed:
            self.record.done()


def excluded_tables(hop, db, tables, qualifier="public", roots=None):
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

    Another tool's bookkeeping is among them whether or not the hop
    excludes anything (`leftovers.bookkeeping`, through `hop.excluded()`):
    it is not the application's to carry.

    `roots` is {partition: its root} of a PostgreSQL side (`_pg_roots`): a
    partition is left out exactly where its table is, which is what the
    hop's patterns name. Each name is split the one way names are
    (`util.split_table`), so `public."Audit.Log"` keeps its dot; it comes
    back as it was given.
    """
    if not hasattr(hop, "excluded"):
        return []
    from .util import split_table
    out = []
    for ident in sorted(tables):
        if hop.excluded(db, *_asked(hop, ident, roots)):
            schema, _ = split_table(_names_engine(hop), str(ident))
            out.append(str(ident) if schema else f"{qualifier}.{ident}")
    return out


def _names_engine(hop):
    """The engine whose names the hop's tables carry (`Hop._names_engine`):
    the source's, where a pair has an engine of its own."""
    got = getattr(hop, "_names_engine", None)
    return got() if callable(got) else str(getattr(hop, "engine", ""))


def _asked(hop, name, roots=None):
    """The parts every question the hop answers about table `name` is
    asked with - left out or taken, its row filter, its columns, its name
    on the target: its root's where it is a partition (`roots`, from
    `_pg_roots`), its own otherwise, split the one way names are
    (`util.split_table`). Each list split it at every dot, so a quoted
    name's own dot made it two parts, and a partition matched none of the
    rules written for its table."""
    from .util import split_table
    name = str((roots or {}).get(name, name))
    return [p for p in split_table(_names_engine(hop), name) if p]


def rules_by_name(hop):
    """Whether the hop has a rule a partition's table's name answers and
    its own may not: a scope (`config.scoped`), a row filter, a column
    rule or a rename. Without one, a partition goes as itself, and its
    root is never asked for."""
    mapping = getattr(hop, "mapping", None) or {}
    return bool(_scoped(hop) or mapping.get("where")
                or mapping.get("columns") or renames_any(hop))


def _pg_roots(hop, db, side="src", eng=None):
    """{partition: its root} of one side's PostgreSQL database, each as
    `schema.table` the way the move's lists name them, read from the one
    reading of partitions (`PostgresEngine.partition_roots`): what every
    list of a move hands `_asked`, so a partition is taken, left out,
    filtered, mapped and renamed as its table is. {} for a hop with no
    rule a root answers (`rules_by_name`)."""
    if not rules_by_name(hop):
        return {}
    if eng is None:
        from .engines.postgres import PostgresEngine
        eng = PostgresEngine(hop)
    out = {}
    for table, root in eng.partition_roots(side, db).items():
        a, b = ".".join(eng._split(table)), ".".join(eng._split(root))
        if a != b:
            out[a] = b
    return out


def pgcopydb_filters(hop, db, tables, also=(), roots=None):
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
    here means it is not carried at all. A partition is named on its own
    beside its table (`roots`): the filter file reads names, not trees.
    """
    out = excluded_tables(hop, db, tables, roots=roots)
    # tables left to the table copier (`routed_to_copier`) are left out of
    # the bulk copy the same way
    out += [n for n in also if n not in out]
    if not out:
        return None
    # a name with a line break in it would be two lines of the file, the
    # second of them anything - a section of its own where it starts `[`
    _filter_names_hold(out)
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
        return runner.run(
            ["atlas", "schema", "diff", "--config", f"file://{cfg}",
             "--env", "migkit", "--from", "env://url", "--to", "env://src",
             *[a for e in excludes for a in ("--exclude", e)]],
            {"MIGKIT_SCHEMA_FROM": frm, "MIGKIT_SCHEMA_TO": to},
            check=False, timeout=timeout, what="the schema comparison",
            retries=3)
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


def mydumper_session(hop, db, sections=None, tables=None):
    """The dump's defaults file: its session's sql_mode, then the hop's
    row filters of the tables it dumps (`tables`) and the lines migkit
    adds per table (`sections`: the chunk size, the masking).

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
            + (mydumper_defaults(hop, db, sections, tables) or ""))


def mydumper_defaults(hop, db, sections=None, tables=None):
    """The hop's row filters as a mydumper defaults file, or None.

    `sections` is {table: [line]} more to say about a table - its chunk
    size (`my_chunk_rows`), its masking (`mask_plan`) - written into the
    same section as its filter, one section a table.

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

    Each table's section holds the one filter the hop gives it
    (`hop.row_filter`), the most particular key that names it, asked for
    every table the dump takes (`tables`). Read off the keys, a table both
    `orders` and `appdb.orders` named had two `where =` lines in its
    section, and which the dump went by was its own business - not the
    one the check compares with. Without `tables`, the tables the keys
    name in this database.
    """
    if tables is None:
        rules = (getattr(hop, "mapping", None) or {}).get("where") or {}
        tables = set()
        for key in rules:
            parts = [p for p in str(key).split(".") if p]
            if len(parts) == 1 or parts[0] == db:
                tables.add(parts[-1])
    per = {}
    for table in sorted(tables):
        predicate = hop.row_filter(db, table)
        if predicate:
            per.setdefault(table, []).append(f"where = {predicate}")
    for table, more in (sections or {}).items():
        per.setdefault(table, []).extend(more)
    lines = ["[mydumper]"]
    for table in sorted(per):
        lines.append(f"[`{db}`.`{table}`]")
        lines += per[table]
    return "\n".join(lines) + "\n" if len(lines) > 1 else None


def scope_alternatives(hop, db, with_db):
    """(the expressions of the names the hop's `exclude` takes in database
    `db`, those its `include` takes - or None where it takes the whole
    database), each a part of one larger expression (`redisshake.glob_body`)
    read the way `Hop.excluded` reads the pattern. `with_db`: a name is
    `db.table`, as the parallel dump matches it; otherwise a name inside
    the database, as the online sync matches a collection's.

    For a program that reads patterns, so a table made under one while it
    runs is taken or left out as the hop says: a list resolved before it
    started missed it - and the online sync runs for days."""
    from .config import name_matches, pattern_after
    from .redisshake import glob_body
    head = re.escape(f"{db}.")

    def parts(p):
        if with_db:
            return [glob_body(p), head + glob_body(p)]
        # a pattern written with the database, read after it
        return [glob_body(p)] + [glob_body(r) for r in
                                 pattern_after(p, f"{db}.") if r]
    out = sorted({x for p in hop.exclude or () for x in parts(str(p))})
    pats = [str(p) for p in getattr(hop, "include", None) or ()
            if str(p).strip()]
    if not pats or any(name_matches(db, p, hop.folds_case()) for p in pats):
        return out, None
    return out, sorted({x for p in pats for x in parts(p)})


def _mydumper_regex(hop, db):
    """The hop's scope as the parallel dump's one `--regex` over
    `db.table`, or None where it leaves nothing out: no name `exclude`
    takes, and - where `include` narrows the database - only a name it
    takes."""
    out, taken = scope_alternatives(hop, db, True)
    if not out and taken is None:
        return None
    return ("(?s)^" + (f"(?!(?:{'|'.join(out)})$)" if out else "")
            + ("" if taken is None else
               f"(?:{'|'.join(taken) or NOTHING})$"))


#: the long options an installed build really has, asked once per build
#: (`toolversion.options`), and the pieces of that question the flag checks
#: read: the subcommand of a command line (`runner._subcommand`) and the
#: option column of a `--help` text
_long_options = toolversion.options
_options_of = toolversion.options_of
_option_column = toolversion.option_column
_subcommand = runner._subcommand


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


def _my_truncate_target(hop, db, log=None, spare=()):
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

    `spare` are tables a stopped load is going on with (`_shell_resumable`):
    what it loaded is kept, and it loads the rest.

    Only what the move carries is emptied (`_my_carried`), under the name
    the hop gives it there: every target table the hop did not exclude
    was, so one the source does not have - the target's own - was emptied
    and never refilled. A table under a row filter is not emptied whole:
    the dump carries only the rows the filter selects, so only those are
    deleted, and the rest - the target's own - stay, as the table copier
    keeps them. Where the pair's copier carries it (`through_pair`), that
    copier replaces the rows it selects itself.
    """
    from .engines.mysql import MySQLEngine
    eng = MySQLEngine(hop)
    names = eng._all_tables("dst", db)
    skip = set(excluded_tables(hop, db, names, qualifier=db))
    carried = _my_carried(hop, db, eng)
    from .config import OPS
    keep, filtered, alone, some = [], {}, [], {}
    for n in names:
        src = carried.get(n)
        if src is None:
            if f"{db}.{n}" not in skip:
                alone.append(n)
            continue
        if n in set(spare):
            continue
        rf = hop.row_filter(db, *_asked(hop, src))
        if hop.carries(db, *_asked(hop, src)) != frozenset(OPS):
            some[n] = rf
        if rf:
            if not eng.through_pair(db, src):
                filtered[n] = rf
            continue
        keep.append(n)
    if not keep and not filtered:
        return []
    ddb = eng._quote_ident(eng._d("dst", db))
    _refuse_kept_rows([n for n in keep + sorted(filtered) if n in some and
                       eng._q("dst", f"select 1 from {ddb}"
                                     f".{eng._quote_ident(n)}"
                              + (f" where {some[n]}" if some[n] else "")
                              + " limit 1")])
    stmts = ["set foreign_key_checks = 0"] + [
        f"truncate table {ddb}.{eng._quote_ident(n)}" for n in keep] + [
        f"delete from {ddb}.{eng._quote_ident(n)} where {rf}"
        for n, rf in sorted(filtered.items())]
    if log:
        from .wording import phase
        log(phase("empty", tables=len(keep), left_out=len(skip)))
        if alone:
            log(f"{len(alone)} tables only the target has are not emptied:"
                " the source has none of them, so nothing would refill"
                " them: " + ", ".join(alone[:6])
                + (" ..." if len(alone) > 6 else ""))
        if filtered:
            log(f"{len(filtered)} tables the hop filters keep their rows"
                " outside the filter: only the rows it selects are removed")
    conn = eng._conn("dst")
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt)
        # a delete is kept only once committed; a truncate commits itself
        conn.commit()
    finally:
        conn.close()
    return keep


def _my_carried(hop, db, eng=None):
    """{table on the target: the source's} of every table of `db` the move
    carries, under the name the hop gives it there (`mapping.tables`): what
    the step that empties the target and the windows that take its indexes
    and triggers off reach. They reached every target table the hop did
    not exclude - one only the target has was emptied and never refilled,
    a renamed one was looked for under its old name."""
    from .engines.mysql import MySQLEngine
    eng = eng or MySQLEngine(hop)
    out = {}
    for t in eng._all_tables("src", db):
        if eng.in_scope(db, t):
            out[str(hop.target_table_in(db, *_asked(hop, t))
                    ).rpartition(".")[2]] = t
    return out


def _my_missing_tables(hop, db):
    """The tables in scope the target does not have yet, and whether the
    database itself is missing there."""
    from .engines.mysql import MySQLEngine
    eng = MySQLEngine(hop)
    have = set(eng._all_tables("dst", db))
    # a table whose columns the hop maps, or that it renames, is built by
    # the pair's copier, under the names and without the columns the
    # mapping says - made here, it stood under the source's name
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
    from . import tableengine
    # a foreign key into a table the target is not given - one the hop
    # leaves out that the target lacks, or one the pair's copier makes
    # later under another name - points at nothing there, or at the wrong
    # table: made with the checks off it was taken, and refused the
    # application's first row after the move
    have = set(eng._all_tables("dst", db))
    gone = {t for t in eng._all_tables("src", db)
            if (not eng.in_scope(db, t) and t not in have)
            or eng.through_pair(db, t)}
    unmade = []
    for t in want:
        got = eng._q("src", f"show create table {src}.{eng._quote_ident(t)}")
        # in the engine the table changes to, where it changes one
        ddl = tableengine.schema(hop, eng, db, t, got[0][1])
        if not keeps_history and re.search(r"\bWITH SYSTEM VERSIONING\b",
                                           ddl, re.I):
            ddl = re.sub(r"\s+WITH(OUT)? SYSTEM VERSIONING\b", "", ddl,
                         flags=re.I)
            plain.append(t)
        ddl, keys = _my_without_keys(ddl, gone)
        unmade += [f"{k} on {t} -> {p}" for k, p in keys]
        stmts.append(ddl)
    if log:
        from .wording import phase
        log(phase("create", tables=len(want)))
        for t in plain:
            log(f"{t}: made without its history - the target keeps none;"
                " its rows as they are now are carried")
    if unmade:
        if log:
            log(f"{db}: {len(unmade)} foreign keys are not made on the target"
                " - each refers to a table the hop does not take, or one the"
                " table copier makes later under another name: "
                + "; ".join(unmade[:8]) + (" ..." if len(unmade) > 8 else ""))
        from . import decide
        decide.record(hop, {"work": f"the schema of {db}",
                            "left out": unmade},
                      work=f"the schema of {db}",
                      way="left out of the schema",
                      reason="they depend on tables the target is not given")
    conn = eng._conn("dst")
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt)
    finally:
        conn.close()
    return want


#: one foreign key of a `SHOW CREATE TABLE`, a line of its own: its name and
#: the table it references, in this database or named with another
_MY_FOREIGN_KEY = re.compile(
    r"^\s*CONSTRAINT\s+`(?P<name>(?:[^`]|``)+)`\s+FOREIGN KEY\b.*?"
    r"\bREFERENCES\s+(?:`(?P<db>(?:[^`]|``)+)`\.)?`(?P<parent>(?:[^`]|``)+)`",
    re.I)


def _my_without_keys(ddl, gone):
    """(the `CREATE TABLE` `ddl` without its foreign keys into the tables
    `gone` of its own database, [(key, table it referenced)]). The element
    before the closing parenthesis loses the comma the one after it took
    away."""
    out, keys = [], []
    for line in str(ddl).split("\n"):
        m = _MY_FOREIGN_KEY.match(line)
        parent = m.group("parent").replace("``", "`") if m else None
        if m and not m.group("db") and parent in gone:
            keys.append((m.group("name").replace("``", "`"), parent))
            continue
        if keys and line.startswith(")") and out:
            out[-1] = out[-1].rstrip().removesuffix(",")
        out.append(line)
    return ("\n".join(out) if keys else ddl), keys


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


def _my_tls(ep, program):
    """The endpoint's TLS as the program's flags (dump and load) or
    connection keys (the straight copy), from what `Endpoint.mysql_tls`
    decides for migkit's own connections - the automatic answer included:
    a server whose certificate verifies against the system's authorities
    is checked by the programs too, where they were given only what the
    hop wrote and connected with whatever the server offered."""
    o = getattr(ep, "options", None) or {}
    got = ep.mysql_tls() if hasattr(ep, "mysql_tls") else {}
    ca = got.get("ssl_ca")
    cert, key = o.get("ssl_cert"), o.get("ssl_key")
    identity = bool(got.get("ssl_verify_identity", True))
    if program == "mysqlsh":
        if ca:
            out = {"ssl-mode": "VERIFY_IDENTITY" if identity
                   else "VERIFY_CA", "ssl-ca": str(ca)}
        elif got.get("ssl") is not None or o.get("ssl"):
            out = {"ssl-mode": "REQUIRED"}
        else:
            return {}
        if cert:
            out["ssl-cert"] = str(cert)
        if key:
            out["ssl-key"] = str(key)
        return out
    if ca:
        # the dump and load programs know two modes: REQUIRED checks
        # nothing, so a named authority is held to the server's name too
        out = ["--ssl-mode", "VERIFY_IDENTITY", "--ca", str(ca)]
    elif got.get("ssl") is not None or o.get("ssl"):
        out = ["--ssl"]
    else:
        return []
    return (out + (["--cert", str(cert)] if cert else [])
            + (["--key", str(key)] if key else []))


def _mydumper_commands(hop, db, workers, outdir, cnf=None, omit=None,
                       logs=None, compress=False, regex=None):
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

    Both sides checksum every table (`--checksum-all`): the dump takes the
    source's at the dump, the load the target's after it, and migkit reads
    each pair (`_MyLoadLog`) and holds the move to them beside its own
    digest. Measured on 1.0.5: without `--checksum-all` on the load as well,
    its checksum step compares nothing - a dumped checksum edited to a
    wrong value loaded with no warning - and with it the load logs each
    table's expected and actual value. `--checksum warn` rather than
    `fail`: `fail` stops at the first table and names only it, `warn` lets
    migkit name every one and fail the move itself.

    `--max-threads-per-table` is the whole of `workers`, so one large table
    is read and loaded by every worker (the default is 4, whatever the
    sizing says); `--rows` per table sits in the defaults file
    (`my_chunk_rows`). `logs` is whether the target writes a binlog
    (`_my_target_logs`), asked once by the caller.

    `regex` is the hop's scope as the dump reads a pattern
    (`_mydumper_regex`), given where the installed build takes one: the
    omitted list is resolved before the dump starts, and a table made
    under an excluded pattern while it ran was dumped.
    """
    from .engines.mysql import MySQLEngine
    from .sizing import split
    s, t = hop.source, hop.target
    # each side as many threads as carry it (`sizing.fit`)
    reads, writes = split(hop, workers)
    dump = (["mydumper", "-h", s.host, "-P", str(s.port), "-u", s.user]
            + _my_tls(s, "mydumper")
            + ["-B", db, "-o", str(outdir), "--threads", str(reads),
               "--max-threads-per-table", str(reads), "--checksum-all"])
    if compress:
        dump += [tool_flag("mydumper", "--compress"), "zstd"]
    dump += ["--no-schemas",
             tool_flag("mydumper", "--trx-tables", "--trx-consistency-only")]
    if cnf:
        dump += ["--defaults-file", str(cnf)]
    if omit:
        dump += [tool_flag("mydumper", "--omit-from-file"), str(omit)]
    if regex and "--regex" in _long_options("mydumper"):
        dump += ["--regex", regex]
    load = (["myloader", "-h", t.host, "-P", str(t.port), "-u", t.user]
            + _my_tls(t, "myloader")
            + ["-B", MySQLEngine(hop)._d("dst", db), "-d", str(outdir),
               "--threads", str(writes), "--max-threads-per-table",
               str(writes), "--checksum-all",
               tool_flag("myloader", "--checksum"), "warn"])
    if (hop.options or {}).get("target_has_replicas", True) is not False \
            and (_my_target_logs(hop) if logs is None else logs):
        # the loader turns the binlog off for its own sessions unless told
        # otherwise. Measured on 8.4: a replica of the target received the
        # tables the move created and none of their 300,000 rows, and said
        # nothing - and a point-in-time restore of the target, which
        # replays the same log, would not have them either. Left off only
        # where the hop states its target has no replicas
        load.append(tool_flag("myloader", "--enable-binlog"))
    # one JSON object per event, table by table - what the progress lines
    # are read from. A build without it just gives no per-table lines.
    for cmd, program in ((dump, "mydumper"), (load, "myloader")):
        if "--machine-log-json" in _long_options(program):
            cmd += ["--machine-log-json", "-v", "3"]
    return dump, load


def mydumper_table(hop, db, table, workers, rows, log=None):
    """One table through the parallel dump and load: the target's table
    emptied, the source's dumped in chunks for `workers` threads, loaded.
    Raises where either program fails; the caller proves the rest.

    What the table copier's own pass-through is timed against, table by
    table (`MySQLEngine.TABLE_WAYS`)."""
    import shutil

    from .engines.mysql import MySQLEngine
    outdir = hop.report_dir(db) / f"dump-table-{report_name(table)}"
    shutil.rmtree(outdir, ignore_errors=True)
    dump, load = _mydumper_commands(
        hop, db, workers, outdir, logs=_my_target_logs(hop, quick=False))
    from . import ranges
    per = max(ranges.LEAST, int(rows or 0) // max(1, int(workers)))
    dump += ["-T", f"{db}.{table}", "--rows", str(per)]
    eng = MySQLEngine(hop)
    conn = eng._conn("dst")
    try:
        with conn.cursor() as cur:
            cur.execute("set foreign_key_checks = 0")
            cur.execute(f"truncate table `{eng._d('dst', db)}`"
                        f".`{table.replace('`', '``')}`")
        conn.commit()
    finally:
        conn.close()
    try:
        with _LocalCopy(outdir):
            _sh(dump, {"MYSQL_PWD": hop.source.password}, log)
            _sh(load, {"MYSQL_PWD": hop.target.password}, log)
    finally:
        shutil.rmtree(outdir, ignore_errors=True)


def _my_ask(hop, side, sql, args=None):
    """One question to a side, on a connection of its own that is not
    retried: the plan asks it through `_within`, the run directly."""
    from .engines.mysql import MySQLEngine
    conn = MySQLEngine(hop)._conn(side, retry=False)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()
    finally:
        conn.close()


def _my_target_logs(hop, quick=True):
    """Whether the target writes a binlog, which its replicas and its
    point-in-time recovery read. Asked quickly for a plan: a target not
    reachable yet is said not to, and the run asks again, directly."""
    def ask():
        return str(_my_ask(hop, "dst", "select @@log_bin")[0][0]) == "1"
    return bool(_within(ask)) if quick else ask()


def _my_target_says(hop, db):
    """What a plan asks the target, in one go (`_within` once, not once a
    question): whether it writes a binlog, whether it takes local loads,
    and the tables it lacks."""
    row = _my_ask(hop, "dst", "select @@log_bin, @@global.local_infile")[0]
    return {"logs": str(row[0]) == "1",
            "loads": str(row[1]).lower() in ("1", "on"),
            "missing": _my_missing_tables(hop, db)[0]}


# --- the MySQL bulk copy -------------------------------------------------------
#
# Three programs' ways to carry a MySQL table, each used whole: the parallel
# dump and load (chunked by rows inside a table, checksummed on both sides,
# masked on the way where the hop asks), the straight copy (read and written
# at once in chunks by bytes, checksummed), and migkit's own table copier.
# Which carries a table is the engine's one table ladder's to say
# (`MySQLEngine.TABLE_WAYS`, climbed by `tableway.choose`), from rates
# measured on each rung - the benchmark's (`MY_BENCH`) until the hop has its
# own; this is the batch that runs the programs over the tables it gives
# them.

#: the ladder's rungs the move's programs carry (`MySQLEngine.TABLE_WAYS`)
DUMP_LOAD, STREAM_COPY = "dump-load", "stream-copy"


#: What migkit asks of each MySQL bulk program, feature by feature, in the
#: tool-use registry of backlog 0f (`tooluse`): used, topped up (used, and
#: migkit does more with it than the program does alone) or unused, each
#: with its reason. Kept beside the builders it describes, and the tests
#: hold the command lines to it. A feature is the program's flag, or what
#: its defaults file or its utility is given, by the program's own name.
def _features(account):
    return {("", feature): got for feature, got in account.items()}


tooluse.register("mydumper", options=_features({
    "--threads": (USED, "from migkit's sizing of both servers and this"
                        " machine, never set by hand"),
    "--max-threads-per-table": (
        USED, "the whole of the sizing, so one large table is read by every"
              " worker rather than four"),
    "rows (per table)": (
        TOPPED, "in the defaults file, one size a table from its rows and"
                " bytes and the rate this hop is read at (`my_chunk_rows`),"
                " where the flag is one size for every table. Measured on a"
                " local link no faster than the program's own cutting (5.60"
                " s against 4.58 s for both tables, within the runs' spread"
                " on each alone): what it sets is where a chunk starts on"
                " the link this hop is read over"),
    "--checksum-all": (
        TOPPED, "each table's checksum at the dump held to the load's and"
                " folded into the move's verdict, beside migkit's own digest"
                " and never instead of it; measured within the runs' spread"
                " (5.40 s against 5.60 s for both tables)"),
    "--compress": (USED, "zstd, where the dump would not fit the disk:"
                         " measured slower where it fits, 6.07 s against"
                         " 5.60 s (MY_BENCH)"),
    "masking (per column)": (
        USED, "null and constant, from the hop's mask_on_write, on columns"
              " outside every key, on a hop that sends nothing back; the"
              " masked columns are held to the rule after the load"
              " (`mask_plan`)"),
    "--defaults-file": (USED, "the session's sql_mode, and per table its"
                              " filter, chunk size and masking"),
    "--omit-from-file": (USED, "the tables the hop excludes and those"
                               " another rung carries"),
    "--regex": (USED, "the hop's exclude and include as one pattern over"
                      " database.table (`_mydumper_regex`), so a table made"
                      " under one while the dump runs is taken or left out"
                      " as the hop says"),
    "--tables-list": (
        UNUSED, "the tables other rungs carry are left out by name instead,"
                " so a table made on the source during the move is still"
                " carried"),
    "--trx-tables": (USED, "one consistent snapshot of transactional"
                           " tables"),
    "--machine-log-json": (
        TOPPED, "read event by event into migkit's progress lines, each"
                " table's checksum pair and each table's time for the next"
                " choice"),
    "--no-schemas": (USED, "migkit makes the tables the target lacks from"
                           " the source's own definition"),
    "--where": (UNUSED, "one predicate for every table; the per-table where"
                        " of the defaults file is used"),
    "--stream": (UNUSED, "the target is emptied only once a whole dump is"
                         " on disk"),
    "--source-data": (UNUSED, "the dump's metadata holds the position"
                              " without it (`_my_dump_position`)"),
    "--split-partitions": (UNUSED, "not yet: a partitioned table is chunked"
                                   " by rows like the rest"),
}))
tooluse.register("myloader", options=_features({
    "--threads": (USED, "from migkit's sizing"),
    "--max-threads-per-table": (USED, "the whole of the sizing"),
    "--checksum-all": (TOPPED, "without it on the load, its checksum step"
                               " compares nothing (measured)"),
    "--checksum": (TOPPED, "warn, so every table's pair is read and migkit"
                           " fails the move naming each"),
    "--enable-binlog": (USED, "where the target writes a binlog, so its"
                              " replicas and point-in-time recovery get the"
                              " rows"),
    "--optimize-keys": (
        UNUSED, "not yet: it acts only where the load makes the tables, and"
                " migkit makes them from the source's definition"
                " (`_my_create_missing`) and moves their secondary indexes"
                " out of the load's way itself - which also strips what the"
                " target cannot keep (MariaDB's system versioning). Measured"
                " where the load makes them: both tables 4.07-4.52 s against"
                " 4.41-5.44 s by the index window (MY_BENCH), within the"
                " runs' spread, and the benchmark's window rebuilt one table"
                " after another where the move's rebuilds them side by"
                " side"),
    "--disable-redo-log": (UNUSED, "it changes a server setting, which"
                                   " migkit does not do"),
    "--drop-table": (UNUSED, "on a data-only dump neither TRUNCATE nor"
                             " DELETE empties anything (measured); migkit"
                             " empties the target itself"),
    "--resume": (UNUSED, "a stopped load is loaded again from an emptied"
                         " target; the straight copy's resumable load"
                         " carries the long moves"),
}))
tooluse.register("mysqlsh", options=_features({
    "util.copy_schemas": (USED, "read and written at once, no file between,"
                                " where this machine cannot hold the dump"
                                " (`_shell_direct`), after its dry run"),
    "util.dump_schemas + util.load_dump": (
        TOPPED, "the way it runs wherever this machine holds the dump -"
                " measured faster than reading and writing at once (6.19 s"
                " against 11.41 s, MY_BENCH) - with the load's progress"
                " kept, and a stopped move going on from it only while the"
                " source's binlog is where the dump left it"
                " (`_shell_resumable`)"),
    "threads": (USED, "from migkit's sizing"),
    "bytesPerChunk": (USED, "from the rate this hop is read at, no more than"
                            " the largest table split across the workers"
                            " (`_shell_chunk`)"),
    "checksum": (TOPPED, "every chunk's, a table that differs folded into"
                         " the move's verdict beside migkit's digest"),
    "compression": (USED, "zstd, for the dump a resumable load reads"),
    "where": (USED, "the hop's row filter per table"),
    "schema": (USED, "the target's name for the database (db_map)"),
    "dryRun": (USED, "before the target is emptied: the source can be read"
                     " and the target takes the load"),
    "deferTableIndexes": (
        UNUSED, "it acts only where the copy runs the tables' DDL; migkit"
                " makes the tables and moves their secondary indexes out of"
                " the load's way itself. Measured where it runs the DDL: all"
                " deferred 5.40 s against none 5.67 s for both tables"
                " (MY_BENCH)"),
    "waitDumpTimeout": (UNUSED, "loading while the dump is written empties"
                                " the target before the dump is whole; the"
                                " straight copy loads while it reads"
                                " instead, after its dry run"),
    "local_infile": (USED, "asked of the target, never set: the rung is not"
                           " used where it is off"),
    "maxRate": (UNUSED, "not yet: the throttle paces migkit's own copier,"
                        " and has no rate to hand this one"),
    "skipBinlog": (UNUSED, "the target's binlog is kept for its replicas"),
    "analyzeTables": (UNUSED, "migkit settles the target's statistics after"
                              " every bulk copy"),
    "util.checkForServerUpgrade": (
        TOPPED, "run against the source for the version the upgrade goes"
                " to, each problem it finds an assess row in migkit's words"
                " beside migkit's own checks of what that version changes -"
                " its settings and defaults read from both servers"
                " (`upgrade.checker`)"),
    "targetVersion": (USED, "the new version, as the target or the hop's"
                            " upgrade settings say it"),
    "outputFormat": (USED, "JSON, read problem by problem"),
    "configPath": (UNUSED, "the source's configuration file is not on this"
                           " machine; its settings are read from the server"
                           " instead"),
    "include": (UNUSED, "every check it has is run"),
    "exclude": (UNUSED, "every check it has is run"),
    "list": (UNUSED, "its checks are run, not listed"),
    "checkTimeout": (UNUSED, "the whole run is bounded by migkit's own"
                             " wait of ten minutes"),
}))


def _my_target_loads_files(hop, quick=False):
    """Whether the target takes `LOAD DATA LOCAL` (`local_infile`), which
    the straight copy loads by - asked, never set: turning it on is a
    change to the target's server that is the operator's to make. None
    where a plan could not ask in time."""
    def ask():
        got = _my_ask(hop, "dst", "select @@global.local_infile")
        return str(got[0][0]).lower() in ("1", "on")
    return _within(ask) if quick else ask()


def _read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def _write_json(path, value):
    """Written beside itself and put in place whole: a half-written file is
    read as none, never as a smaller one."""
    import os
    import tempfile
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-")
    with os.fdopen(fd, "w") as f:
        json.dump(value, f)
    os.replace(tmp, path)


def _my_record_rates(hop, db, rung, spans, facts, wall=None, suffix=""):
    """Keep what this move measured on `rung`, table by table, to count once
    the move is done (`tableway.pending`): `spans` is {table: seconds}, the
    rows and bytes are the catalogue's; `suffix` names the way it ran
    (`tableway.DIRECT`: read and written at once).

    `wall` is the rung's whole time, start-up and checksums in it, as the
    benchmark's rates are: the spans only share it out. Tables carried side
    by side overlap, so their spans add up to more than the time they took
    together, and a rate taken from the spans alone would say the rung is
    faster than it was."""
    total = sum(s for s in spans.values() if s and s > 0)
    if wall and total:
        spans = {t: wall * s / total for t, s in spans.items()
                 if s and s > 0}
    units = []
    for table, seconds in sorted(spans.items()):
        fact = (facts or {}).get(table) or {}
        size = int(fact.get("bytes") or 0)
        if not size or not seconds or seconds <= 0:
            continue
        units.append((int(fact.get("rows") or 0), size, seconds))
    from . import tableway
    tableway.pending(hop, db, rung + suffix, units)


def _disk_free(path):
    import shutil
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return None


def _shell_direct(hop, db, facts):
    """Whether the straight copy reads and writes at once rather than
    through a dump on disk: only where this machine cannot hold the dump
    - the database's size, which its compressed dump stays under. Through
    the dump it is faster (`MY_BENCH`) and a stopped load goes on."""
    size = sum(int((f or {}).get("bytes") or 0)
               for f in (facts or {}).values() if isinstance(f, dict))
    free = _disk_free(hop.report_dir(db))
    return bool(size and free is not None and size > free)


def _took(s):
    from .wording import _duration
    return _duration(s)


#: how long one chunk of the dump should take to read: the dump doubles a
#: chunk read in under a second and halves one that took over two, so it
#: starts between them
CHUNK_SECONDS = 1.5
#: the fewest rows a chunk is worth: below it the query costs more than
#: the rows
CHUNK_FLOOR = 1_000


def my_chunk_rows(fact, read_rate, workers):
    """`MIN:START:MAX` rows for the dump to cut one table by, or None where
    one chunk is right.

    START is the rows one worker reads in `CHUNK_SECONDS` at the rate the
    dump reads this hop's tables (`read_rate`, bytes a second over every
    worker - the link's share of it), no more than the table split once
    across the workers so every worker has a part of a table that is
    large. MIN and MAX are a tenth and ten times it, the room the dump's
    own doubling and halving is given."""
    rows = int((fact or {}).get("rows") or 0)
    size = int((fact or {}).get("bytes") or 0)
    workers = max(1, int(workers or 1))
    if rows < 2 * CHUNK_FLOOR * workers:
        return None
    per_row = size / rows if size else 100.0
    start = int(read_rate / workers * CHUNK_SECONDS / max(per_row, 1.0))
    start = max(CHUNK_FLOOR, min(start, rows // workers))
    return f"{max(CHUNK_FLOOR, start // 10)}:{start}:{start * 10}"


# --- masking on the way ----------------------------------------------------

#: what a write-side masking rule may ask for, and whether each is exact:
#: every value it writes is known, so the target can be held to it after
#: the load. A random value is known to nobody - it cannot be checked, and
#: a second run writes another
MASKS = {"null": True, "constant": True, "random": False}


def mask_rules(hop, db):
    """{table: {column: (kind, value)}} from the hop's `mask_on_write`,
    for `db`: `table.column` or `db.table.column` to `null`, `constant
    <text>` or `random`. The display option `mask` is not one of them: it
    hides values where a person reads them and leaves what is written as
    it is."""
    raw = (getattr(hop, "options", None) or {}).get("mask_on_write") or {}
    out = {}
    for key, rule in sorted(raw.items()):
        parts = [p for p in str(key).split(".") if p]
        if len(parts) == 3 and parts[0] != db or len(parts) < 2:
            continue
        table, column = parts[-2], parts[-1]
        text = "" if rule is None else str(rule).strip()
        kind, _, value = text.partition(" ")
        if ":" in kind:
            kind, _, rest = kind.partition(":")
            value = (rest + " " + value).strip()
        out.setdefault(table, {})[column] = (kind.lower(), value.strip())
    return out


MY_KEY_COLUMNS_SQL = """
    select table_name, column_name from information_schema.statistics
     where table_schema = %s and non_unique = 0
    union
    select table_name, column_name from information_schema.key_column_usage
     where table_schema = %s and referenced_table_name is not null
    union
    select referenced_table_name, referenced_column_name
      from information_schema.key_column_usage
     where referenced_table_schema = %s and referenced_table_name is not null"""


def _my_mask_facts(hop, db):
    """({table: {column}} of every column in a primary or unique key, or in
    a foreign key on either end, and {table: {column}} of every column) on
    the source: what `mask_plan` holds a rule to."""
    keys, columns = {}, {}
    for t, c in _my_ask(hop, "src", MY_KEY_COLUMNS_SQL, (db, db, db)):
        keys.setdefault(str(t), set()).add(str(c))
    for t, c in _my_ask(hop, "src", "select table_name, column_name from"
                                    " information_schema.columns"
                                    " where table_schema = %s", (db,)):
        columns.setdefault(str(t), set()).add(str(c))
    return keys, columns


def _streams_back(hop):
    opts = getattr(hop, "options", None) or {}
    topo = str(opts.get("topology") or "").lower()
    return bool(opts.get("reverse") or opts.get("two_way")
                or topo in ("two-way", "two_way", "bidirectional"))


def mask_plan(hop, db, keys, columns=None):
    """(sections, refused) for the hop's masking rules on `db`: the dump's
    defaults-file lines per table, and [(table.column, why)] for every rule
    that cannot be applied as asked.

    A rule is applied only where it is exact (`MASKS`) and safe to run
    again and to undo: not on a key - a masked key is not the row's key
    any more, and a second run, the check and every table that references
    it line rows up by it - and not on a hop that streams the target's
    changes back, which would write masked values over the source's real
    ones. `keys` and `columns` are `_my_mask_facts`' ({table: {column}});
    with `columns` known, a rule naming a column the source does not have
    is refused rather than left to mask nothing - a typo there hides
    nothing and says it did. `columns` is None where a plan could not
    ask."""
    sections, refused = {}, []
    back = _streams_back(hop)
    for table, rules in mask_rules(hop, db).items():
        for column, (kind, value) in sorted(rules.items()):
            name = f"{table}.{column}"
            if kind not in MASKS:
                refused.append((name, f"`{kind or '(nothing)'}` is not a"
                                " masking rule: null, constant <text> or"
                                " random"))
            elif not MASKS[kind]:
                refused.append((name, "a random value cannot be held to"
                                " anything after the load, and a second run"
                                " writes another"))
            elif kind == "constant" and ("'" in value or "\n" in value
                                         or not value):
                refused.append((name, "a constant must be text without"
                                " quotes or line breaks"))
            elif column in (keys or {}).get(table, set()):
                refused.append((name, "it is part of a key: a masked key is"
                                " not the row's key any more, and a second"
                                " run, the check and every table that"
                                " references it line rows up by it"))
            elif back:
                refused.append((name, "the hop streams the target's changes"
                                " back to the source, which would write"
                                " masked values over the real ones"))
            elif columns is not None and column not in columns.get(table,
                                                                   ()):
                refused.append((name, "the source has no such column"))
            else:
                line = (f"`{column}` = null" if kind == "null"
                        else f"`{column}` = constant '{value}'")
                sections.setdefault(table, []).append(line)
    return sections, refused


def _masked_record(hop, db):
    return hop.report_dir(db) / "bulk-masked.json"


def masked_tables(hop, db):
    """`db.table` of every table the move in progress masked on the way
    and held to the rule (`_my_masked_held`): not a copy of its source, so
    not compared whole. Read from what the move wrote once it had held
    them, never from the rules alone - a table a rule names and nothing
    masked is compared like any other."""
    got = _read_json(_masked_record(hop, db)) or {}
    began = _read_json(hop.report_dir(db) / "move-began.json") or {}
    if not got or float(got.get("at") or 0) < float(began.get("epoch")
                                                     or 0):
        return set()
    return {f"{db}.{t}" for t in got.get("tables") or ()}


def _my_masked_held(hop, db, rules):
    """[problem] for the masked tables: each holds as many rows as its
    source does (through the row filter), and every masked column holds
    exactly what the rule writes. Asked of the servers, so it can say a
    table is not what the rule makes it - a column left unmasked, a row
    missing."""
    from .engines.mysql import MySQLEngine
    eng = MySQLEngine(hop)
    q = eng._quote_ident
    ddb = eng._d("dst", db)
    out = []
    for table, cols in sorted(rules.items()):
        where = hop.row_filter(db, table)
        src = eng._q("src", f"select count(*) from {q(db)}.{q(table)}"
                            + (f" where {where}" if where else ""))[0][0]
        dst = eng._q("dst", f"select count(*) from {q(ddb)}.{q(table)}")[0][0]
        if int(src) != int(dst):
            out.append(f"{table}: {int(dst):,} rows where the source has"
                       f" {int(src):,}")
        for column, (kind, value) in sorted(cols.items()):
            n = eng._q("dst", f"select count(*) from {q(ddb)}.{q(table)}"
                              f" where not ({q(column)} <=> %s)",
                       (None if kind == "null" else value,))[0][0]
            if int(n):
                out.append(f"{table}.{column}: {int(n):,} rows not masked"
                           " as the hop asks")
    return out


# --- what the dump and the load say, table by table -------------------------

def _my_ts(text):
    """Seconds from the programs' JSON `ts`. Measured on 1.0.5: the
    fraction is microseconds without their leading zeros - `.95765`
    follows `.142985`'s second and means .095765, and read as a decimal it
    put a chunk before the one that started it."""
    m = re.match(r"(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(?:\.(\d+))?",
                 str(text or ""))
    if not m:
        return None
    y, mo, d, h, mi, s = (int(x) for x in m.groups()[:6])
    return (time.mktime((y, mo, d, h, mi, s, 0, 0, -1))
            + int(m.group(7) or 0) / 1e6)


class _Spans:
    """How long each table took, from events that mark where a thread
    started a piece of it: a piece ends where the same thread says
    anything next, or where the run ends."""

    def __init__(self):
        self.open, self.spans = {}, {}

    def start(self, thread, table, at):
        self.stop(thread, at)
        self.open[thread] = (table, at)

    def stop(self, thread, at):
        got = self.open.pop(thread, None)
        if got and at is not None and got[1] is not None:
            lo, hi = self.spans.get(got[0], (got[1], at))
            self.spans[got[0]] = (min(lo, got[1]), max(hi, at))

    def end(self, at):
        for thread in list(self.open):
            self.stop(thread, at)

    def seconds(self):
        return {t: hi - lo for t, (lo, hi) in self.spans.items()}


class _MyDumpLog(_JsonLog):
    """The dump's `dump_table_progress` events: migkit's line once per
    table, and how long each table's reading took (`spans`)."""

    def __init__(self):
        from .wording import Tally
        self.tally = Tally(_SIZES)
        self.spans = _Spans()
        super().__init__(self._on)

    def _on(self, event):
        at = _my_ts(event.get("ts"))
        thread = event.get("thread_id") or event.get("thread")
        kind = event.get("event")
        if kind in ("dump_completed", "dump_finished"):
            self.spans.end(at)
            return None
        if kind != "dump_table_progress":
            if thread:
                self.spans.stop(thread, at)
            return None
        table = str(event.get("table") or "")
        self.spans.start(thread, table, at)
        name = f"{event.get('db')}.{table}"
        total = str(event.get("tables_total") or "")
        if total.isdigit():
            self.tally.total = int(total)
        so_far = self.tally.reached(name)
        return f"{name}: reading ({so_far})" if so_far else None


class _MyLoadLog(_JsonLog):
    """The load's events: migkit's line once per table, how long each
    table's loading took, each table's checksum pair (`sums`: {table:
    (at the dump, after the load)}), and the `restore_completed` counts
    (`summary`)."""

    def __init__(self):
        from .wording import Tally
        self.tally = Tally(_SIZES)
        self.spans = _Spans()
        self.sums, self.summary = {}, {}
        super().__init__(self._on)

    def _on(self, event):
        at = _my_ts(event.get("ts"))
        thread = event.get("thread_id") or event.get("thread")
        kind = event.get("event")
        if kind == "restore_completed":
            self.spans.end(at)
            self.summary.update(event)
            return None
        if kind == "checksum_run":
            self.spans.end(at)
            return None
        if kind == "checksum_check" and event.get("checksum_type") \
                == "table_data" and event.get("status") in ("finished",
                                                           "failed"):
            self.sums[str(event.get("table"))] = (
                str(event.get("expected_checksum")),
                str(event.get("actual_checksum")))
            return None
        if kind != "restore_data_progress":
            if thread:
                self.spans.stop(thread, at)
            return None
        table = str(event.get("table") or "")
        self.spans.start(thread, table, at)
        name = f"{event.get('db')}.{table}"
        so_far = self.tally.reached(name)
        return f"{name}: loading ({so_far})" if so_far else None


def _my_load_progress(summary):
    """The MySQL load's `restore_data_progress` events, once per table,
    and its `restore_completed` counts into `summary`."""
    reader = _MyLoadLog()
    reader.summary = summary
    return reader


def _my_dumped(outdir):
    """{table: rows} the dump's metadata counted, per table."""
    try:
        text = (outdir / "metadata").read_text()
    except OSError:
        return {}
    out = {}
    for m in re.finditer(r"(?ms)^\[`[^`]*`\.`([^`]*)`\]\s*$(.*?)(?=^\[|\Z)",
                         text):
        rows = re.search(r"(?m)^rows\s*=\s*(\d+)", m.group(2))
        if rows:
            out[m.group(1)] = int(rows.group(1))
    return out


# --- the straight copy, driven through its own Python ---------------------

#: The program run inside the straight copy's own Python, which is where its
#: API lives (no package carries it). The secrets reach it through its
#: environment, never argv or a file; it says how it ended on a line of its
#: own, and its own messages go to its error stream, read by `_ShellLog`.
SHELL_RUN = '''import json
import os


def said(**kw):
    print("MIGKIT " + json.dumps(kw), flush=True)


spec = json.loads(os.environ["MIGKIT_SPEC"])


def side(name):
    ep = dict(spec[name])
    ep["password"] = os.environ.get("MIGKIT_" + name.upper() + "_PASSWORD",
                                    "")
    return ep


try:
    what = spec["what"]
    shell.connect(side("target" if what == "load" else "source"))
    if what == "copy":
        util.copy_schemas([spec["db"]], side("target"), spec["options"])
    elif what == "dump":
        util.dump_schemas([spec["db"]], spec["dir"], spec["options"])
    else:
        util.load_dump(spec["dir"], spec["options"])
    said(ok=True)
except Exception as e:
    said(ok=False, error=str(e))
'''

#: the straight copy's messages, in its own words: colour codes, a
#: checksum that did not match, the load's warning count, a row a load
#: refused, and the position the copy was consistent at
_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_SHELL_SUM = re.compile(r"Checksum verification failed for: `(?P<db>[^`]+)`"
                        r"\.`(?P<table>[^`]+)`")
_SHELL_WARNED = re.compile(r"(?P<n>\d+) warnings were reported during the"
                           r" load")
_SHELL_REFUSED = re.compile(r"LOAD DATA error for `[^`]+`\.`(?P<table>[^`]+)`"
                            r".*?error (?P<code>\d+): (?P<text>.*)$")
_SHELL_AT = re.compile(r"^\s*(?P<key>Binlog_file|Binlog_position|"
                       r"Executed_GTID_set):\s*(?P<value>\S*)\s*$")
_SHELL_ROWS = re.compile(r"Rows written: (?P<n>\d+)")


class _ShellLog:
    """Reads the straight copy's output as it runs: the ending it says on
    its own line, the tables whose checksum did not match, the load's
    warnings and the first rows it refused, and the binlog position the
    copy was consistent at. Nothing it prints reaches the operator."""

    def __init__(self):
        self.result, self.sums, self.warned = None, set(), 0
        self.refused, self.position, self.rows = [], {}, None

    def __call__(self, line):
        line = _ANSI.sub("", line).rstrip("\n")
        if line.startswith("MIGKIT "):
            try:
                self.result = json.loads(line[7:])
            except ValueError:
                pass
            return None
        m = _SHELL_SUM.search(line)
        if m:
            self.sums.add(m.group("table"))
        m = _SHELL_WARNED.search(line)
        if m:
            self.warned = max(self.warned, int(m.group("n")))
        m = _SHELL_REFUSED.search(line)
        if m and len(self.refused) < 3:
            self.refused.append(f"{m.group('table')}: ERROR"
                                f" {m.group('code')}: {m.group('text')}")
        m = _SHELL_AT.match(line)
        if m:
            self.position[m.group("key")] = m.group("value")
        m = _SHELL_ROWS.search(line)
        if m:
            self.rows = int(m.group("n"))
        return None

    def failure(self):
        said = (self.result or {}).get("error") or ""
        return "; ".join([s for s in [said] + self.refused if s]) or None


def _shell_endpoint(ep):
    out = {"scheme": "mysql", "host": ep.host, "port": int(ep.port),
           "user": ep.user}
    out.update(_my_tls(ep, "mysqlsh"))
    return out


#: bytes a chunk of the straight copy holds, when nothing measured says
#: otherwise: its own default
SHELL_CHUNK = 64 * 2 ** 20
#: chunks each worker is given of the largest table: measured, 16 MB
#: chunks (about four a worker of the 222 MB table) copied both tables in
#: 7.25 s against 9.89 s at the default 64 MB (`MY_BENCH`)
SHELL_CHUNKS_PER_WORKER = 4
#: the smallest chunk worth a query of its own
SHELL_CHUNK_FLOOR = 4 * 2 ** 20


def _shell_chunk(read_rate, workers, largest):
    """Bytes per chunk: what one worker reads in `CHUNK_SECONDS`, no more
    than the largest table split into `SHELL_CHUNKS_PER_WORKER` a worker,
    and within `SHELL_CHUNK_FLOOR` and four times the program's default."""
    workers = max(1, int(workers or 1))
    size = int(read_rate / workers * CHUNK_SECONDS) \
        if read_rate else SHELL_CHUNK
    if largest:
        size = min(size, int(largest) // (workers * SHELL_CHUNKS_PER_WORKER))
    return max(SHELL_CHUNK_FLOOR, min(size, 4 * SHELL_CHUNK))


def _shell_spec(hop, db, what, tables, workers, chunk, where=None,
                folder=None, progress=None):
    """What the straight copy is asked to do, without a secret in it."""
    from .engines.mysql import MySQLEngine
    ddb = MySQLEngine(hop)._d("dst", db)
    opts = {"threads": int(workers), "showProgress": False,
            "checksum": True}
    if what in ("copy", "dump"):
        opts.update({"dataOnly": True, "consistent": True,
                     "bytesPerChunk": str(int(chunk)),
                     "includeTables": [f"{db}.{t}" for t in tables]})
        if where:
            opts["where"] = {f"{db}.{t}": w for t, w in where.items()}
    if what == "dump":
        opts["compression"] = "zstd"
    if what == "load":
        opts.update({"progressFile": str(progress), "resetProgress": False,
                     "loadDdl": False, "ignoreExistingObjects": True})
    if what in ("copy", "load") and ddb != db:
        opts["schema"] = ddb
    if what == "copy":
        opts["ignoreExistingObjects"] = True
    spec = {"what": what, "db": db, "options": opts,
            "source": _shell_endpoint(hop.source),
            "target": _shell_endpoint(hop.target)}
    if folder:
        spec["dir"] = str(folder)
    return spec


def _shell_argv(script):
    return ["mysqlsh", "--file", str(script)]


def _shell_run(hop, db, spec, script, log=None):
    """Run the straight copy once, as `spec` says. Returns its `_ShellLog`;
    raises with the database's words when it failed for any reason but a
    checksum that did not match, which the caller folds into the move's
    verdict."""
    import os
    import tempfile
    reader = _ShellLog()
    with tempfile.TemporaryDirectory(prefix="migkit-") as home:
        env = {"MIGKIT_SPEC": json.dumps(spec),
               "MIGKIT_SOURCE_PASSWORD": str(hop.source.password or ""),
               "MIGKIT_TARGET_PASSWORD": str(hop.target.password or ""),
               # its history, log and settings in a place of its own that
               # goes with the run, not under this user's home
               "MYSQLSH_USER_CONFIG_HOME": home}
        os.chmod(home, 0o700)
        _sh(_shell_argv(script), env, log, progress=reader)
    result = reader.result or {}
    if result.get("ok"):
        return reader
    if reader.sums and "checksum" in str(result.get("error", "")).lower():
        return reader
    raise RuntimeError(reader.failure() or "the straight copy stopped"
                       " without saying why")


def _shell_position(reader):
    """The binlog position the straight copy was consistent at, in the
    shape `_my_dump_position` keeps; None where it said none."""
    p = reader.position
    if p.get("Binlog_file") and str(p.get("Binlog_position", "")).isdigit():
        return {"log_file": p["Binlog_file"],
                "log_pos": int(p["Binlog_position"]),
                "gtid_set": p.get("Executed_GTID_set", "")}
    return None


def _shell_resume_record(hop, db):
    return hop.report_dir(db) / "bulk-resume.json"


def _my_source_at(hop):
    """Where the source's binlog is now, file and position - to tell
    whether a dump taken earlier is still the source's state."""
    try:
        got = _my_ask(hop, "src", "show binary log status")
    except Exception:  # noqa: BLE001 - before 8.2 it had its old name
        got = _my_ask(hop, "src", "show master status")
    return ({"log_file": str(got[0][0]), "log_pos": int(got[0][1])}
            if got else None)


def _shell_dumped_at(folder):
    """The binlog position the straight copy's dump was consistent at, from
    its own metadata (`@.json`), in `_my_dump_position`'s shape."""
    at = ((_read_json(folder / "@.json") or {}).get("source") or {}).get(
        "binlog") or {}
    if not at.get("file") or not str(at.get("position", "")).isdigit():
        return None
    return {"log_file": str(at["file"]), "log_pos": int(at["position"]),
            "gtid_set": str(at.get("gtidExecuted") or "")}


def _shell_resumable(hop, db, tables, folder):
    """The kept record of a dump whose load stopped, when the next move can
    go on from it: the dump finished, the same tables under the same scope,
    and the source not written to since - its binlog where the dump's
    snapshot left it. None otherwise: a dump older than the source's last
    write would load yesterday's rows over today's, and one taken under
    another row filter, columns or names (`Hop.scope_changed`) or other
    values masked as they are written (`mask_rules`) would load what the
    rules of then took into tables this hop fills otherwise."""
    rec = _read_json(_shell_resume_record(hop, db))
    if not rec or sorted(rec.get("tables") or []) != sorted(tables):
        return None
    if hop.scope_changed(rec) or ("masked" in rec and rec["masked"] != (
            json.loads(json.dumps(mask_rules(hop, db))))):
        return None
    if not (folder / "@.done.json").exists():
        return None
    dumped = rec.get("position") or {}
    try:
        now = _my_source_at(hop)
    except Exception:  # noqa: BLE001 - not known, so not resumed
        return None
    return rec if now and dumped.get("log_file") == now["log_file"] \
        and dumped.get("log_pos") == now["log_pos"] else None


def mydumper_move(hop, db, workers, go, log, only=None):
    """The MySQL bulk copy: each table by the rung the engine's one table
    ladder gives it (`tableway.choose`), the parallel dump and load and the
    straight copy one after the other, the table copier after both
    (`routed_to_copier`). The choice is kept for the copier that follows
    (`tableway.keep`), which takes each table the way chosen for it.

    A program that fails as it runs hands its tables to the copier after
    the move, each on the way its own climb gives without it, said
    (`tableway.batch_or_copier`); a checksum that differs is a verdict and
    stops the move as it always did.

    Dump, then empty the target, then load - in that order. The target is
    emptied only once a complete dump is on disk, and once the straight
    copy has shown it can read the source (its dry run): emptying it
    first, and then finding the source unreachable, hands back a target
    with nothing in it and nothing to load.

    Each rung's own checksums are compared table by table - the dump's
    against the load's, the straight copy's chunk by chunk - and a table
    either says differs stops the move here, whatever the digest that
    follows would say (`_held_to_the_source` runs it after, as for every
    bulk path: the programs' checksums are a second reading beside it,
    never instead of it). `only` limits the move to one rung
    (`mysqlsh_move`).
    """
    from .wording import Step, phase
    root = hop.report_dir(db)
    outdir = root / "mydumper"
    shell_dir = root / "straight-copy"
    script = root / "straight-copy.py"
    # beside the dump directory, not inside it: myloader is pointed at
    # that directory and has no reason to meet a file it does not read
    cnf = root / "mydumper-filters.cnf"
    omit = root / "omit-tables.txt"
    from . import tableway
    from .engines.mysql import PLAIN, MySQLEngine
    eng = MySQLEngine(hop)  # the table ladder's own
    via = "mysqlsh" if only == STREAM_COPY else "mydumper"
    unresolved = ""
    # a plan asks each server once, and not for long; the run asks directly
    if go:
        try:
            facts = eng.table_sizes(db)
        except Exception as e:
            why = (str(e).strip().splitlines()[0][:100] if str(e).strip()
                   else type(e).__name__)
            if _routes_any(hop, db) or mask_rules(hop, db):
                raise _unresolved_exclusion(db, why)
            raise RuntimeError(f"the source's tables could not be read:"
                               f" {why}. Nothing has been changed on the"
                               " target")
        logs = _my_target_logs(hop, quick=False)
        known = {}
        missing = lambda: _my_missing_tables(hop, db)[0]  # noqa: E731
    else:
        facts = eng.table_sizes(db, quick=True)
        said = _within(lambda: _my_target_says(hop, db)) or {}
        logs = said.get("logs")
        known = {"local_infile": said.get("loads")}
        missing = lambda: said.get("missing")  # noqa: E731
    every = sorted(facts) if facts is not None else None
    skip, routed, choice = [], [], {}
    if every is not None:
        skip = excluded_tables(hop, db, every, qualifier=db)
        choice = tableway.choose(eng, db, via, every, facts, quick=not go,
                                 fresh=True, known=known)
        if go:
            tableway.keep(hop, db, choice)
        for t, p in sorted(choice.items()):
            tableway.said(eng, p, log, f"{db}.{t}")
        # the table copier carries these after the load: mapped columns,
        # and the tables it is measured faster on (the dump applies row
        # filters itself)
        routed = [n for n in routed_to_copier(hop, db, via, every,
                                              qualifier=db, choice=choice)
                  if n not in skip]
    elif _routes_any(hop, db) or mask_rules(hop, db):
        unresolved = "no answer in time"
    leaf = {f"{db}.{t}": t for t in (every or [])}
    elsewhere = {leaf.get(n, n.rsplit(".", 1)[-1]) for n in skip + routed}
    shell_tables = [t for t in every or () if t not in elsewhere
                    and choice.get(t) and choice[t].rung == STREAM_COPY]
    dump_tables = (None if every is None else
                   [t for t in every if t not in elsewhere
                    and t not in shell_tables])
    rates = {w.name: w.rates for w in eng.TABLE_WAYS}
    # masking on the way: exact rules on columns outside every key, on a
    # hop that sends nothing back; anything else stops the move before it
    # writes, rather than copy the values the hop asked to hide
    sections, refused, rules = {}, [], mask_rules(hop, db)
    if rules:
        keys, columns = (_within(lambda: _my_mask_facts(hop, db))
                         or ({}, None)) if not go else _my_mask_facts(hop, db)
        sections, refused = mask_plan(hop, db, keys, columns)
        for t in rules:
            c = choice.get(t)
            if every is not None and t not in every:
                # a rule on a name the source does not have masks nothing,
                # and the operator believes something is hidden
                refused.append((t, "the source has no such table"))
            elif c is not None and c.rung != DUMP_LOAD \
                    or f"{db}.{t}" in routed:
                refused.append((t, "only the parallel dump masks on the way,"
                                " and this table cannot go by it"))
    read_rate = rates[DUMP_LOAD].get("read") or rates[DUMP_LOAD][PLAIN]
    for t in dump_tables or ():
        spec = my_chunk_rows((facts or {}).get(t), read_rate, workers)
        if spec:
            sections.setdefault(t, []).append(f"rows = {spec}")
    # each dumped table's own filter, the one the check compares with
    settings = mydumper_session(hop, db, sections, tables=dump_tables)
    size = sum(int(((facts or {}).get(t) or {}).get("bytes") or 0)
               for t in dump_tables or ())
    import shutil
    free = _disk_free(root)
    # compressed where the dump would not fit beside a margin; written
    # plain otherwise, which is faster (MY_BENCH)
    compress = bool(free is not None and size and size * 1.5 > free)
    dump, load = _mydumper_commands(
        hop, db, workers, outdir, cnf,
        omit if skip or routed or shell_tables else None, logs=bool(logs),
        compress=compress, regex=_mydumper_regex(hop, db))
    # each side as many threads as carry it (`sizing.fit`)
    from .sizing import split
    reads, writes = split(hop, workers)
    dump = Step(phase("dump", workers=reads, left_out=len(skip),
                      routed=len(routed),
                      row_filters=len(_filtered_here(hop, db))
                      if mydumper_defaults(hop, db) else 0), dump)
    load = Step(phase("load", workers=writes), load)
    # through a dump on disk, its load's progress kept for a stopped move
    # to go on from - measured faster than reading and writing at once too
    # (MY_BENCH); at once only where this machine cannot hold the dump
    resumable = not _shell_direct(hop, db, facts)
    where = {t: hop.row_filter(db, t) for t in shell_tables
             if hop.row_filter(db, t)}
    largest = max([int(((facts or {}).get(t) or {}).get("bytes") or 0)
                   for t in shell_tables] or [0])
    chunk = _shell_chunk(rates[STREAM_COPY].get("read"), workers, largest)
    shell_said = (phase("dump", tables=len(shell_tables), workers=workers,
                        row_filters=len(where)) + "; "
                  + phase("load", workers=workers) if resumable else
                  phase("stream-copy", tables=len(shell_tables),
                        workers=workers, row_filters=len(where)))
    shell = Step(shell_said, _shell_argv(script)) if shell_tables else None
    steps = []
    if dump_tables is None or dump_tables:
        steps.append(dump)
    steps.append(_truncate_step(hop, db))
    if dump_tables is None or dump_tables:
        steps.append(load)
    if shell:
        steps.append(shell)
    note = _create_note(missing)
    if note:
        steps.insert(1, note)
    if skip:
        steps.insert(1, f"# {len(skip)} tables the hop excludes are not"
                        " dumped at all")
    if unresolved:
        steps.insert(1, _unresolved_note(unresolved))
    for name, why in refused:
        steps.insert(1, f"# {name}: cannot be masked on the way as the hop"
                        f" asks - {why}. The move stops before it writes")
    if rules and not refused:
        steps.insert(1, f"# {len(rules)} tables are masked on the way: the"
                        " target is not a copy of the source in those"
                        " columns, so the move holds them to the masking"
                        " rule and counts their rows instead of comparing"
                        " them; the check compares whole rows and shows"
                        " them as different")
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)
    if refused:
        raise SystemExit(
            f"{db}: the hop asks to mask "
            + "; ".join(f"{n} ({why})" for n, why in refused)
            + ". Nothing has been written: fix the rule, or drop it to copy"
              " the values as they are")
    if any(not c.rung for c in choice.values()):
        raise SystemExit(
            f"{db}: " + "; ".join(f"{t}: {c.reason}" for t, c in
                                  sorted(choice.items()) if not c.rung))
    _masked_record(hop, db).unlink(missing_ok=True)
    cnf.write_text(settings)
    cnf.chmod(0o600)
    if skip or routed or shell_tables:
        omit.write_text("".join(f"{n}\n" for n in
                                skip + routed
                                + [f"{db}.{t}" for t in shell_tables]))
    import contextlib
    sums, spans, took = {}, {}, {}
    shell_reader, resumed = None, None
    fallen = []

    def or_copier(rung, tables, run):
        """`run()`, or its tables handed to the copier after the move."""
        fell = tableway.batch_or_copier(eng, db, rung, tables, run, log,
                                        masked=rules, picks=choice,
                                        facts=facts)
        fallen.extend(fell)
        return not fell
    with contextlib.ExitStack() as stack:
        dumped = {}
        if dump_tables:
            stack.enter_context(_LocalCopy(outdir))
            if log:
                log(dump)
            reading = _MyDumpLog()
            began = time.monotonic()
            if or_copier(DUMP_LOAD, dump_tables, lambda: _sh(
                    dump.argv, {"MYSQL_PWD": hop.source.password}, log,
                    progress=reading)):
                took[DUMP_LOAD] = time.monotonic() - began
                spans[DUMP_LOAD] = reading.spans.seconds()
                dumped = _my_dumped(outdir)
            else:
                dump_tables = []
        if shell_tables:
            script.write_text(SHELL_RUN)
            script.chmod(0o600)
            if resumable:
                resumed = _shell_resumable(hop, db, shell_tables, shell_dir)
                if resumed is None:
                    # kept when the load stops, for the next move to go on
                    # from (`_shell_resumable`); private, like every local
                    # copy of the source, and gone once the load is done
                    shutil.rmtree(shell_dir, ignore_errors=True)
                    (root / "straight-copy.progress").unlink(missing_ok=True)
                    shell_dir.mkdir(parents=True)
                    shell_dir.chmod(0o700)
                    if log:
                        log(phase("dump", workers=workers,
                                  tables=len(shell_tables)))
                    began = time.monotonic()
                    if or_copier(STREAM_COPY, shell_tables,
                                 lambda: _shell_run(hop, db, _shell_spec(
                                     hop, db, "dump", shell_tables, workers,
                                     chunk, where, folder=shell_dir), script,
                                     log)):
                        took[STREAM_COPY] = time.monotonic() - began
                        _write_json(_shell_resume_record(hop, db), {
                            "tables": sorted(shell_tables),
                            "position": _shell_dumped_at(shell_dir),
                            "scope_fingerprint": hop.scope_fingerprint(),
                            "masked": mask_rules(hop, db)})
                    else:
                        shell_tables = []
                elif log:
                    log(f"{len(shell_tables)} tables go on loading from the"
                        " copy an earlier move made and did not finish:"
                        " the source has not been written to since")
            else:
                # before anything is emptied: the source can be read, the
                # tables are there, the target takes the load
                spec = _shell_spec(hop, db, "copy", shell_tables, workers,
                                   chunk, where)
                spec["options"]["dryRun"] = True
                if not or_copier(STREAM_COPY, shell_tables,
                                 lambda: _shell_run(hop, db, spec, script,
                                                    None)):
                    shell_tables = []
        _my_create_missing(hop, db, log)
        _my_truncate_target(hop, db, log,
                            spare=shell_tables if resumed else ())
        loading = _MyLoadLog()
        with _MyTriggerWindow(hop, db, log), \
                _MyIndexWindow(MySQLEngine(hop), hop, db, workers, log):
            if dump_tables:
                if log:
                    log(load)
                began = time.monotonic()
                if or_copier(DUMP_LOAD, dump_tables, lambda: _sh(
                        load.argv, {"MYSQL_PWD": hop.target.password}, log,
                        progress=loading)):
                    took[DUMP_LOAD] = took.get(DUMP_LOAD, 0) + (
                        time.monotonic() - began)
                    load_spans = loading.spans.seconds()
                    spans[DUMP_LOAD] = {
                        t: spans[DUMP_LOAD].get(t, 0) + load_spans.get(t, 0)
                        for t in set(spans[DUMP_LOAD]) | set(load_spans)}
                    sums = loading.sums
                else:
                    dump_tables, dumped = [], {}
                    loading = _MyLoadLog()
            if shell_tables:
                if log:
                    log(shell)
                began = time.monotonic()
                if resumable:
                    spec = _shell_spec(
                        hop, db, "load", shell_tables, workers, chunk,
                        folder=shell_dir,
                        progress=root / "straight-copy.progress")
                else:
                    spec = _shell_spec(hop, db, "copy", shell_tables,
                                       workers, chunk, where)
                got = []
                if or_copier(STREAM_COPY, shell_tables, lambda: got.append(
                        _shell_run(hop, db, spec, script, log))):
                    shell_reader = got[0]
                else:
                    shell_tables = []
                took[STREAM_COPY] = took.get(STREAM_COPY, 0) + (
                    time.monotonic() - began)
                if resumable and shell_reader is not None:
                    # the dump's own count, per table and exact
                    counted = ((_read_json(shell_dir / "@.done.json") or {})
                               .get("tableRows") or {}).get(db) or {}
                    if counted:
                        shell_reader.rows = sum(int(n) for n in
                                                counted.values())
        errors = str(loading.summary.get("errors") or "0")
        if errors.isdigit() and int(errors):
            # the load's own count, which an exit code of 0 does not rule
            # out
            raise RuntimeError(f"the load counted {int(errors):,} errors")
        # a filtered table keeps the target's own rows outside its filter
        # (`_my_truncate_target`), and the load sums the whole table: held
        # by the digest that follows, which reads both sides through it
        own = {t for t in sums if hop.row_filter(db, t)}
        differ = sorted(
            [t for t, (a, b) in sums.items() if a != b and t not in rules
             and t not in own]
            + list(shell_reader.sums if shell_reader else ()))
        if differ:
            raise SystemExit(
                f"{db}: the copy's own checksum of {len(differ)} tables"
                " differs from the source's: " + ", ".join(differ[:8])
                + (" ..." if len(differ) > 8 else "")
                + ". Nothing has been marked as moved"
                + (f"; the load reported {shell_reader.warned:,} warnings,"
                   " the likely cause" if shell_reader
                   and shell_reader.warned else "")
                + ". A source written to while it was copied differs by"
                  " what changed - move with --mode full+cdc for that;"
                  " otherwise `migkit sync` puts the rows right")
        not_compared = sorted(t for t in dumped if t not in sums
                              and t not in rules)
        if dump_tables and not_compared and log:
            log(f"{len(not_compared)} tables had no checksum from the copy"
                " to compare; the digest that follows compares them")
        if rules:
            held = {t: c for t, c in rules.items()
                    if t in (dump_tables or ())}
            wrong = _my_masked_held(hop, db, held)
            if wrong:
                raise SystemExit(f"{db}: the tables masked on the way are not"
                                 " what the hop's rule makes them: "
                                 + "; ".join(wrong[:6]))
            _write_json(_masked_record(hop, db),
                        {"at": time.time(), "tables": sorted(held)})
        if skip:
            _my_orphans_left(hop, db, log)
        # the position the copy is a snapshot at, for a replica that follows
        # it - only where one rung carried every table: two rungs are two
        # snapshots, and neither position is the other's
        if dump_tables and not shell_tables and not fallen:
            _my_dump_position(hop, db, outdir)
        elif shell_tables and not dump_tables and not fallen:
            at = (_shell_dumped_at(shell_dir) if resumable
                  else _shell_position(shell_reader))
            if at:
                (root / "dump-position.json").write_text(json.dumps(at))
        if resumable:
            _shell_resume_record(hop, db).unlink(missing_ok=True)
            (root / "straight-copy.progress").unlink(missing_ok=True)
            shutil.rmtree(shell_dir, ignore_errors=True)
        if dump_tables:
            _my_record_rates(hop, db, DUMP_LOAD, spans.get(DUMP_LOAD) or {},
                             facts, wall=took.get(DUMP_LOAD))
            if took.get(DUMP_LOAD) and log:
                n = sum(dumped.values())
                log(f"dumped and loaded {n:,} rows in"
                    f" {_took(took[DUMP_LOAD])}"
                    + (f" ({int(n / took[DUMP_LOAD]):,} rows/s)"
                       if took[DUMP_LOAD] > 0 else ""))
                for t, secs in sorted((spans.get(DUMP_LOAD) or {}).items()):
                    if dumped.get(t) and secs > 0:
                        log(f"{db}.{t}: {dumped[t]:,} rows,"
                            f" {int(dumped[t] / secs):,} rows/s")
        if shell_tables and took.get(STREAM_COPY) and shell_reader:
            if log and shell_reader and shell_reader.rows is not None:
                log(f"copied {shell_reader.rows:,} rows"
                    f" {'through a dump' if resumable else 'straight across'}"
                    " in"
                    f" {_took(took[STREAM_COPY])}"
                    + (f" ({int(shell_reader.rows / took[STREAM_COPY]):,}"
                       " rows/s)" if took[STREAM_COPY] > 0 else ""))
            # it says nothing table by table: its time is shared out by
            # size; a load that went on from an earlier dump is part of a
            # copy, and teaches nothing
            if resumed is None:
                _my_record_rates(hop, db, STREAM_COPY, {
                    t: float((facts.get(t) or {}).get("bytes") or 0)
                    for t in shell_tables}, facts, wall=took[STREAM_COPY],
                    suffix="" if resumable else tableway.DIRECT)
    return steps


#: the MySQL bulk copy by the straight copy alone
mysqlsh_move = functools.partial(mydumper_move, only=STREAM_COPY)


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

    The load file carries both passwords, so it lives only for the run: a
    file of this user's made new under a name of its own (`PrivateFile`),
    gone however the load ends - it was written into the reports and then
    narrowed, and a stopped run left it there. It named the source's
    database on the target side too, so a hop whose `db_map` renames the
    database loaded into the wrong one.

    Its connections take TLS without checking a certificate
    (`sslmode=require` is the most its PostgreSQL side is given, its MySQL
    side none), so a hop whose TLS checks one - asked, or found to verify
    (`tls.auto`) - or that asks for TLS on its MySQL side is not carried
    this way (`runner.Blocked`, another way is taken): it connected with
    no TLS at all while `doctor` said verified.
    """
    from .wording import Step, phase
    s, t = hop.source, hop.target
    copy = Step(phase("stream-copy", workers=workers),
                ["pgloader", "<private load file>"])
    steps = [_truncate_step(hop, db), copy]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    pg_tls = t.libpq_tls()
    my_tls = s.mysql_tls() if hasattr(s, "mysql_tls") else {}
    if (str(pg_tls.get("sslmode") or "").startswith("verify")
            or my_tls.get("ssl_ca") or my_tls.get("ssl") is not None):
        raise runner.Blocked(
            "the hop's TLS checks the servers' certificates, or asks for"
            " TLS on the source, which this way of copying cannot say;"
            " nothing has been copied by it")
    tail = ("?sslmode=require" if pg_tls.get("sslmode") not in (
        None, "", "disable", "allow", "prefer") else "")
    body = f"""LOAD DATABASE
  FROM mysql://{s.user}:{quote(s.password, safe='')}@{s.host}:{s.port}/{db}
  INTO postgresql://{t.user}:{quote(t.password, safe='')}@{t.host}:{t.port}/{hop.target_db(db)}{tail}
WITH data only, workers = {workers}, concurrency = {min(workers, 4)},
     on error stop
ALTER SCHEMA '{db}' RENAME TO 'public';
"""
    # a data-only load appends: run again, it doubled every row or stopped
    # on the first key already there. Emptied first, as the other bulk
    # paths into PostgreSQL empty it, so a second run replaces the first
    _pg_truncate_target(hop, db, log)
    if log:
        log(copy)
    with PrivateFile(body, ".load") as loadfile:
        done = _sh(["pgloader", loadfile], None, log)
    # it says an error in its log and exits 0: measured, 3.6.10 could not
    # sign in to MySQL 8.4 (`The value of SEQUENCE ... is not of type
    # (SIMPLE-ARRAY (UNSIGNED-BYTE 8))`), loaded nothing and ended as if
    # it had loaded everything
    failed = [ln for ln in f"{done.stdout}\n{done.stderr}".splitlines()
              if " ERROR " in ln or " FATAL " in ln]
    if failed:
        raise runner.classify(1, "\n".join(failed), secrets=(
            s.password, t.password))
    return steps


def _mongo_uri(ep, secret=False):
    """The address a MongoDB program is given. Without its password unless
    `secret` says so: a command line is readable by every process listing
    on the machine, and the dump and load programs took theirs whole - the
    guard against it looked at the call and not at the variable the
    command line was built in. The password goes in a private file
    (`_MongoSecrets`)."""
    auth = ((f"{quote(ep.user, safe='')}:{quote(ep.password, safe='')}@"
             if secret or not ep.password
             else f"{quote(ep.user, safe='')}@")
            if ep.user else "")
    hosts = ep.options.get("hosts") or f"{ep.host}:{ep.port}"
    uri = f"mongodb://{auth}{hosts}/"
    extra = ep.options.get("uri_options", "")
    # the connection's TLS, the same the driver makes (`Endpoint.mongo_tls`)
    tls = ep.mongo_tls() if hasattr(ep, "mongo_tls") else {}
    if tls:
        from urllib.parse import urlencode
        extra = "&".join(p for p in (extra, urlencode(tls)) if p)
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

    A collection the hop renames (`mapping.tables`) is restored under its
    new name, its own pair of names ahead of the database's - the restore
    takes the first pair a name matches: it was restored under the
    source's name, and the one the hop names stayed empty.
    """
    from .wording import Step, phase
    s, t = hop.source, hop.target
    skip, renamed, unresolved = [], {}, ""
    if _scoped(hop) or renames_any(hop):
        try:
            names, skip = _mongo_left_out(hop, db)
            renamed = {n: hop.target_table_in(db, n) for n in names
                       if n not in skip and _renamed(hop, db, [n])}
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
               *[a for n, there in renamed.items()
                 for a in (f"--nsFrom={_ns(db, n)}",
                           f"--nsTo={_ns(tdb, there)}")],
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
    with _MongoSecrets(s, t) as files:
        return _mongo_piped(db, dump + files["src"], restore + files["dst"],
                            log, steps, (s.password, t.password))


def _ns(db, name):
    """`db.name` as the restore reads a namespace it renames: its own
    wildcard and variable characters (`*`, `$`) and its escape taken as
    themselves."""
    def bare(x):
        return re.sub(r"([\\*$])", r"\\\1", str(x))
    return f"{bare(db)}.{bare(name)}"


def _mongo_left_out(hop, db):
    """(the source database's collections, the ones the hop leaves out),
    each asked the one scope question (`Engine.takes`): a collection by
    its name, a view by its name and the hop's `objects` - a view the
    hop's `objects` leaves out was dumped and restored, and synced."""
    from .engines.mongodb import MongoEngine
    eng = MongoEngine(hop)
    got = list(eng._client("src")[db].list_collections())
    names = sorted(str(c["name"]) for c in got)
    views = {str(c["name"]) for c in got if c.get("type") == "view"}
    return names, [n for n in names
                   if not eng.takes(db, "view" if n in views else "table", n)]


class _MongoSecrets:
    """Each side's password in a file of its own, readable by this user
    only, for as long as the programs run: `{side: ["--config=<file>"]}`,
    empty for a side with no password - each a `PrivateFile`, made new
    under a name of its own. Measured with the Database Tools 100.16:
    `--config` holding `password:` beside a `--uri` without one signed in,
    and a wrong one there exited 1."""

    def __init__(self, src, dst):
        self.eps = {"src": src, "dst": dst}
        self.files = []

    def __enter__(self):
        import yaml
        out = {}
        for side, ep in self.eps.items():
            if not ep.password:
                out[side] = []
                continue
            f = PrivateFile(yaml.safe_dump({"password": ep.password}),
                            ".yaml")
            self.files.append(f)
            out[side] = [f"--config={f.__enter__()}"]
        return out

    def __exit__(self, *exc):
        for f in self.files:
            f.__exit__(*exc)
        return False


def _mongosync_unfit(hop):
    """Why the online sync cannot carry this hop, or None.

    It keeps each database's name and each collection's, needs both sides
    to be a replica set or sharded cluster of MongoDB 6.0 or later, and
    reads no row filter.
    Asked of the servers, once; one that cannot be asked is a reason.

    And it is licensed for MongoDB Atlas and Enterprise Advanced
    deployments only: a hop runs it where it says it has one
    (`mongodb_entitlement: atlas | enterprise`), and the open path
    otherwise."""
    if not mongodb_entitled(hop):
        return ("the online sync is licensed for MongoDB Atlas and"
                " Enterprise Advanced only, and the hop declares neither"
                " (mongodb_entitlement: atlas or enterprise)")
    if any(hop.target_db(d) != d for d in (hop.databases or [])) or any(
            k != v for k, v in (hop.db_map or {}).items()):
        return "the online sync keeps each database's name, and the hop maps one"
    if renames_any(hop):
        # it wrote a renamed collection under the source's name, and the
        # one the hop names on the target stayed empty
        return ("the online sync keeps each collection's name, and the hop"
                " renames one")
    from .engines.mongodb import MongoEngine
    eng = MongoEngine(hop)
    for side, label in (("src", "source"), ("dst", "target")):
        try:
            admin = eng._client(side).admin
            hello = admin.command("hello")
            version = admin.command("buildInfo").get("versionArray") or [0]
        except Exception as e:  # noqa: BLE001 - said, as the reason
            return f"the {label} could not be asked ({type(e).__name__})"
        if not (hello.get("setName") or hello.get("msg") == "isdbgrid"):
            return f"the {label} is not a replica set or sharded cluster"
        if int(version[0]) < 6:
            return f"the {label} is older than MongoDB 6.0"
    return None


#: what a hop says to declare the MongoDB licence the online sync's terms
#: ask for
ENTITLEMENTS = {"atlas": "MongoDB Atlas", "enterprise": "Enterprise Advanced",
                "enterprise-advanced": "Enterprise Advanced"}


def mongodb_entitled(hop):
    """The MongoDB licence the hop declares, or ''."""
    said = str((getattr(hop, "options", None) or {}).get(
        "mongodb_entitlement") or "").strip().lower()
    return ENTITLEMENTS.get(said, "")


#: the names Redis Cloud's databases are reached by: a target there is one
#: of Redis's own products whatever else is known of it
REDIS_CLOUD = (".redislabs.com", ".redis-cloud.com", ".rlrcp.com")
#: what a target endpoint says to declare itself one of Redis's products
#: where its name cannot: `redis_product: community | cloud | software`
REDIS_PRODUCTS = ("community", "cloud", "software")


def _riotx_unfit(hop):
    """Why Redis's own copy tool may not carry this hop, or None.

    Its licence (BSL 1.1, Redis Ltd.) grants production use only with
    Redis Community Edition, Redis Cloud and Redis Software, so the target
    must be one: Redis itself, not Valkey, KeyDB or Dragonfly (the server
    says which, `variants`), and reached as Redis Cloud by its name or
    declared one of the three by the hop (`redis_product`) - a managed
    service elsewhere runs the same Redis and is not Redis's product. And
    it takes one key pattern where the hop may leave several out."""
    t = hop.target
    said = str((t.options or {}).get("redis_product") or "").strip().lower()
    cloud = str(t.host or "").lower().endswith(REDIS_CLOUD)
    if not (cloud or said in REDIS_PRODUCTS):
        return ("the Redis products' own copy tool is licensed only into"
                " Redis Community Edition, Redis Cloud or Redis Software,"
                " and the target is not known to be one (redis_product:"
                " community, cloud or software on the target, where it is)")
    if _scoped(hop):
        return ("the hop leaves keys out, and the Redis products' own copy"
                " tool takes one pattern of keys to carry")
    from .engines.redis import RedisEngine
    try:
        brand = RedisEngine(hop)._brands()[1].name
    except Exception as e:  # noqa: BLE001 - said, as the reason
        return f"the target could not be asked what it is ({type(e).__name__})"
    if brand and brand != "redis":
        return (f"the target runs {brand}, not Redis, and the Redis"
                " products' own copy tool is licensed only into Redis")
    return None


def riotx_move(hop, db, workers, go, log):
    """A Redis keyspace into one of Redis's own products through their
    replication tool (`replicate`, scan mode): every key's value, type and
    expiry, a cluster's every node on either side, many at once.

    Its terms are the operator's to accept (`tools.TERMS`), and the target
    is held to them (`_riotx_unfit`). Both addresses and passwords go in
    its environment, which it reads for every option (`RIOT_SOURCE_*`,
    `RIOT_TARGET_*`), not on a command line anyone can list. What the
    target held of the database is removed first, as migkit's own copier
    does, and `check` compares the two after. Not run on this machine:
    installing it is accepting its terms, which is the operator's to do."""
    from .engines.redis import RedisEngine
    from .wording import Step, phase
    eng = RedisEngine(hop)
    argv = ["riotx", "replicate", "--mode", "scan",
            "--threads", str(max(1, int(workers)))]
    env = {}
    for side, ep, word in (("src", hop.source, "SOURCE"),
                           ("dst", hop.target, "TARGET")):
        tls = ep.redis_tls()
        scheme = "rediss" if tls else "redis"
        env[f"RIOT_{word}_URI"] = (f"{scheme}://{ep.host}:{int(ep.port)}"
                                   f"/{int(db)}")
        if ep.user:
            env[f"RIOT_{word}_USER"] = ep.user
        if ep.password:
            env[f"RIOT_{word}_PASS"] = ep.password
        if tls:
            env[f"RIOT_{word}_TLS"] = "true"
            env[f"RIOT_{word}_TLS_VERIFY"] = (
                "NONE" if tls.get("ssl_cert_reqs") == "none" else "FULL")
            if tls.get("ssl_ca_certs"):
                argv += [f"--{word.lower()}-cacert", tls["ssl_ca_certs"]]
        if go and eng._cluster(side):
            env[f"RIOT_{word}_CLUSTER"] = "true"
    step = Step(phase("keyspace", workers=workers), argv)
    if not go:
        return [step, "# dry-run, add --go to execute"]
    gone = eng.empty_target(db)
    if gone and log:
        log(f"db{db}: emptied {gone:,} keys the target held before the copy")
    if log:
        log(step)
    _sh(argv, env, log)
    return [step]


def _free_port():
    import socket as _socket
    with _socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _sync_api(port, method, path, body=None, timeout=15):
    import urllib.request
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v1/{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read() or b"{}")


def mongosync_move(hop, db, workers, go, log):
    """MongoDB to MongoDB through the cluster-to-cluster sync: an online
    copy that keeps applying the source's changes while it runs, committed
    when it has caught up, and consistent as of that commit.

    Measured with 1.21 between two 7.0 replica sets. Started with
    `preExistingDestinationData` and the hop's database as its only
    namespace, it wrote nothing to the source: no database or collection
    appeared there, and it asked for no user on the source (without that
    option it refused to start until it could turn on write blocking
    there). It keeps its own bookkeeping in `__mdb_internal_mongosync` on
    the target. 3,000 documents arrived, and so did an insert and an update
    made while it ran; NumberLong and Decimal128 stayed as they were.

    Both addresses, passwords and all, go in its configuration file, which
    is readable by this user only and removed when it ends."""
    from .wording import Step, phase
    skip, unresolved = [], ""
    if _scoped(hop):
        try:
            skip = _mongo_left_out(hop, db)[1]
        except Exception as e:  # noqa: BLE001 - said, and refused below
            unresolved = (str(e).strip().splitlines()[0][:100] if str(e)
                          else type(e).__name__)
    port = _free_port()
    start = {"source": "cluster0", "destination": "cluster1",
             "preExistingDestinationData": True}
    start.update(mongosync_filters(hop, db, skip))
    sync = Step(phase("sync", left_out=len(skip)),
                ["mongosync", "--config", "<private file>", "--port", port])
    steps = [sync]
    if unresolved:
        steps.append(_unresolved_note(unresolved))
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)
    import yaml

    from .engines.mongodb import MongoEngine
    from .util import PrivateFile
    eng = MongoEngine(hop)
    # what the target holds of these collections is not this copy's, and
    # the sync refuses to start over one that exists even empty (measured:
    # "namespace conflict(s) with existing data") - dropped, on the target
    # only, as the other bulk path drops them; it builds their indexes
    target = eng._client("dst")[db]
    there = set(target.list_collection_names())
    for name in eng._client("src")[db].list_collection_names():
        if name not in skip and name in there:
            target.drop_collection(name)
    # its logs and metrics in the hop's reports, and nothing sent to its
    # vendor: telemetry is on unless it is turned off, and measured, its
    # metrics went into whatever directory it was started from
    here = hop.report_dir(db)
    conf = yaml.safe_dump({
        "cluster0": _mongo_uri(hop.source, secret=True),
        "cluster1": _mongo_uri(hop.target, secret=True),
        "port": port, "logPath": str(here / "sync-logs"),
        "metricsLogPath": str(here / "sync-metrics"),
        "disableTelemetry": True})
    if log:
        log(sync)
    with PrivateFile(conf, ".yaml") as path:
        import subprocess
        proc = runner.start(["mongosync", "--config", path],
                            stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE, cwd=here)
        # what it says is read as it says it: a pipe nobody reads fills in
        # an hours-long sync and holds the program up
        said = runner._Reader(proc.popen.stderr, None, False)
        with runner.stopping(), proc:
            try:
                _mongosync_run(port, proc, start, db, log,
                               said=lambda: runner.said(
                                   "".join(said.lines)))
            finally:
                # committed, it has nothing more to do and does not stop;
                # stopped part way, it is stopped with what it started
                proc.stop(grace=30)
    return steps


#: the online sync's first build that reads a pattern of collections
#: (`collectionsRegex`); an older one is told the names resolved at its
#: start
MONGOSYNC_PATTERNS = "1.6"

#: an expression that takes no name, in every dialect the programs read
NOTHING = r"\b\B"


def _taken_by(alternatives, name):
    """Whether `name` is one the expressions `alternatives` take."""
    return bool(alternatives) and re.fullmatch(
        "(?s:" + "|".join(alternatives) + ")", name) is not None


def mongosync_filters(hop, db, skip):
    """The online sync's `includeNamespaces` and `excludeNamespaces` for
    database `db`: the hop's patterns as the sync's own patterns of
    collections (`scope_alternatives`), where the installed build reads
    them, with the collections left out for another reason - migkit's
    own, another tool's - named in them; the names resolved at its start
    (`skip`) otherwise.

    The sync runs online until it is committed, days on a large source.
    Told only those names, it synced a collection made under an excluded
    pattern after it started - into a target that may own one by that
    name - and every collection made outside an included one."""
    out = {"includeNamespaces": [{"database": db}]}
    if not _scoped(hop) or toolversion.older(program_version("mongosync"),
                                             MONGOSYNC_PATTERNS):
        if skip:
            out["excludeNamespaces"] = [{"database": db,
                                         "collections": list(skip)}]
        return out
    left, taken = scope_alternatives(hop, db, False)
    left = left + [re.escape(n) for n in skip if not _taken_by(left, n)
                   and (taken is None or _taken_by(taken, n))]
    if taken is not None:
        out["includeNamespaces"] = [{"database": db, "collectionsRegex": {
            "pattern": "^(?s:" + ("|".join(taken) or NOTHING) + ")$",
            "options": ""}}]
    if left:
        out["excludeNamespaces"] = [{"database": db, "collectionsRegex": {
            "pattern": "^(?s:" + "|".join(left) + ")$", "options": ""}}]
    return out


#: seconds a sync that can commit may go on not saying its lag before the
#: commit is refused
LAG_UNSAID_WAIT = 60
#: seconds behind the source at which the commit is made
COMMIT_LAG = 1


def commit_ready(progress):
    """(whether to commit now, the lag it read) from one `/progress`.

    Only on `canCommit` and the overall lag, `lag.overallLagSeconds`. The
    top-level `lagTimeSeconds` this read before is deprecated since 1.21
    and absent from 1.22, where `(absent or 0) <= 1` read as caught up and
    committed while changes were still arriving. A lag it does not report
    is None - never 0."""
    lag = (progress.get("lag") or {}).get("overallLagSeconds")
    if isinstance(lag, bool) or not isinstance(lag, (int, float)):
        lag = None
    return (bool(progress.get("canCommit")) and lag is not None
            and lag <= COMMIT_LAG), lag


def _mongosync_run(port, proc, start, db, log, said=lambda: ""):
    """The sync started, followed and committed through its own API;
    `said()` is what the program said, in migkit's words, for a start
    that fails."""
    import time as _t
    end = _t.time() + 60
    while True:
        try:
            _sync_api(port, "GET", "progress", timeout=5)
            break
        except OSError:
            if proc.poll() is None and _t.time() > end:
                proc.stop()
            if proc.poll() is not None:
                raise runner.Failed("the online sync did not start: "
                                    + (said() or "it said nothing")[-300:])
            _t.sleep(0.5)
    got = _sync_api(port, "POST", "start", start)
    if not got.get("success"):
        raise RuntimeError(f"the online sync refused to start:"
                           f" {got.get('errorDescription') or got}")
    said, last = None, _t.time()
    unsaid = None
    while True:
        p = _sync_api(port, "GET", "progress").get("progress") or {}
        if proc.poll() is not None:
            raise RuntimeError("the online sync stopped before it"
                               " could commit")
        ready, lag = commit_ready(p)
        if ready:
            break
        if p.get("canCommit") and lag is None:
            # it says it can commit and not how far behind it is: a build
            # that stopped reporting the lag this reads, where committing
            # would stop the copy with changes still to apply
            unsaid = unsaid or _t.time()
            if _t.time() - unsaid > LAG_UNSAID_WAIT:
                raise RuntimeError(
                    "the online sync says it can commit and does not say"
                    " how far behind the source it is; not committed, so"
                    " no change is left behind - the copy it made stays,"
                    " and the check says what it holds")
        else:
            unsaid = None
        copy = p.get("collectionCopy") or {}
        done, total = (copy.get("estimatedCopiedBytes"),
                       copy.get("estimatedTotalBytes"))
        if log and total and _t.time() - last > 10:
            from .wording import human_bytes
            now = f"{db}: {human_bytes(done or 0)} of {human_bytes(total)}"
            if now != said:
                log(now)
                said, last = now, _t.time()
        _t.sleep(1)
    got = _sync_api(port, "POST", "commit", {})
    if not got.get("success"):
        raise RuntimeError(f"the online sync would not commit:"
                           f" {got.get('errorDescription') or got}")
    while (_sync_api(port, "GET", "progress").get("progress") or {}
           ).get("state") != "COMMITTED":
        if proc.poll() is not None:
            raise RuntimeError("the online sync stopped while committing")
        _t.sleep(1)
    if log:
        log(f"{db}: copied, and committed with nothing left to apply")


def _mongo_piped(db, dump, restore, log, steps, secrets=()):
    """The dump piped straight into the load (`runner.pipe`): what the load
    says as it finishes each collection, in migkit's words, and how many
    documents it could not load - which it reports and then exits 0
    over."""
    loaded = _tables_done(MONGO_RESTORED, "loaded")
    failed = []

    def read(line):
        m = re.search(MONGO_RESTORED, line)
        if m and int(m.group("failed")):
            failed.append(f"{m.group('table')} {m.group('failed')} of"
                          f" {int(m.group('docs')) + int(m.group('failed'))}")
        return loaded(line)
    runner.pipe(dump, restore, progress=read, log=log, what="the copy",
                secrets=secrets)
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

PGCOPYDB_IMAGE = IMAGES["pgcopydb"]
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
#: seconds an answer of `pgcopydb_runner` stands: a long `sync --serve`
#: sees an image pulled, or a build installed, after it started
PGCOPYDB_HOW_FOR = 300


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
    now = time.monotonic()
    if _PGCOPYDB_HOW is not None and now - _PGCOPYDB_HOW[1] \
            < PGCOPYDB_HOW_FOR:
        return _PGCOPYDB_HOW[0]
    how = ""
    if which("docker"):
        try:
            p = runner.run(["docker", "image", "inspect", PGCOPYDB_IMAGE],
                           check=False, timeout=20)
            if p.returncode == 0:
                how = "image"
        except Exception:  # noqa: BLE001 - no docker to ask
            how = ""
    if not how and which("pgcopydb"):
        how = "local"
    # kept a while: `pick` is called per table, and starting a docker
    # client each time to ask the same question would cost more than the
    # answer is worth
    _PGCOPYDB_HOW = (how, now)
    return how


def _pgcopydb_options(*sub):
    """The long options of the build the streaming copy runs, for its
    subcommand `sub`: the local build's own help, or the image's, asked
    once per image."""
    if pgcopydb_runner() == "local":
        return _long_options("pgcopydb", *sub)
    return _image_options(PGCOPYDB_IMAGE, tuple(sub))


@functools.lru_cache(maxsize=None)
def _image_options(image, sub):
    try:
        p = runner.container(image, ["pgcopydb", *sub, "--help"],
                             network=None, check=False, timeout=60,
                             init=False)
    except Exception:  # noqa: BLE001 - an image that cannot be asked
        return frozenset()
    return _option_column(p.stdout or "")


#: a part of a split table is not worth making under this many seconds of
#: one stream: each part is a query of its own on the source
PART_SECONDS = 10


class _Split:
    """How the streaming copy cuts its largest tables: `threshold` bytes
    and `parts` at most per table, or neither; `way` is "split" or "whole"
    (what the run's rate is recorded under) and `why` says so."""

    def __init__(self, way, why, threshold=None, parts=None, tables=()):
        self.way, self.why = way, why
        self.threshold, self.parts, self.tables = threshold, parts, tables

    def argv(self):
        if self.way != "split":
            return []
        return ["--split-tables-larger-than", str(int(self.threshold)),
                "--split-max-parts", str(int(self.parts))]

    def keep(self, begun):
        """Kept beside the record that a copy began (`decide.remember`),
        so a copy stopped part way goes on cut the way it began - or, cut
        in parts, starts over.

        Measured on 0.18: a copy that cut a 1,500,000-row table in four
        parts, stopped with two of them done, then run again with
        `--resume --not-consistent`, exited 0 with 750,000 rows in the
        table - the same cut, another cut and no cut alike. The parts not
        done were taken for done and never copied, and nothing said so. A
        copy stopped before any part of the table was done went on whole
        (1,500,000). Only the whole database's comparison with the source
        would have found it, so a split copy is never gone on from."""
        from .decide import remember
        remember(begun, "split", {"way": self.way,
                                  "threshold": self.threshold,
                                  "parts": self.parts,
                                  "tables": list(self.tables)})

    @classmethod
    def resumed(cls, begun):
        """The way a stopped copy began, or None where it kept none (a
        copy begun before the split was chosen, which went whole)."""
        from .decide import remembered
        got = remembered(begun, "split")
        if not isinstance(got, dict):
            return cls("whole", "tables go whole, as the stopped copy"
                                " began") if begun.exists() else None
        way = got.get("way", "whole")
        return cls(way, "tables go " + ("in parts" if way == "split"
                                        else "whole")
                   + ", as the stopped copy began", got.get("threshold"),
                   got.get("parts"), tuple(got.get("tables") or ()))


def _copy_split(hop, workers, sizes, have=frozenset()):
    """Whether, and how, the streaming copy splits a table across streams.

    One table copied by one stream is the copy's floor: at four streams, a
    table holding half the database keeps one of them busy long after the
    other three are done. The split (`--split-tables-larger-than`) gives a
    large table several streams of its own. Nothing decides that on its
    own: the threshold here comes from what this copy carries, from the
    source's catalogue (`sizes`), and from how fast one stream went over
    this link last time.

    * a table is a candidate only where it is more than an even share of
      the copy (`total / workers`): a copy whose tables are all within one
      share keeps every stream busy whole
    * the threshold is half a share. Measured on 0.18, a 54 MB table: over
      40 MB -> 3 parts, 20 -> 4, 15 -> 5, 10 -> 7, 5 -> 12: `ceil(size /
      threshold) + 1`, the first part being the rows whose key is NULL and
      the last range open-ended, twice the others. Half a share puts the
      largest part within one share
    * not below `PART_SECONDS` of what one stream carried last time
      (`planner.variants`): a part shorter than that is mostly its query
    * and the two ways are rungs of one ladder (`decide`), ranked by the
      rate each reached on this hop or a rehearsal of it: the split first
      where neither was timed, the other once where one was, then the
      faster - a tie within 5% going whole, which asks the source fewer
      queries. On this machine, both servers sharing two CPUs, whole was
      faster. Through `pgcopydb_move`, two tables of 57 MB with an index
      each, four streams, 5 rounds alternated: the copy itself 1.20 to
      1.71 s whole (median 1.45) against 1.48 to 2.63 s in 5 parts each
      (median 1.63); the whole move, indexes rebuilt, 3.61 to 4.59 s
      against 3.47 to 6.24 s. Bare, three tables of 54, 54 and 13 MB at
      four streams: whole 1.71 to 1.99 s against 1.80 to 3.68 s; one table
      of 326 MB at two streams: 3.67 to 4.45 s against 4.40 to 6.24 s.
      Each more stream there is another backend on a CPU already busy;
      servers with CPUs to spare are where its author credits it with its
      speed, which this machine cannot measure - hence the measurement per
      hop and no rule.
    """
    from . import planner
    from .decide import Facts, Need, Rung, climb
    from .wording import human_bytes
    total = sum(sizes.values())
    share = total / workers if workers else 0
    big = {t: b for t, b in sizes.items() if share and b > share}
    measured = planner.variants(hop, "pgcopydb")
    one = [m["stream"] for m in measured.values() if m.get("stream")]
    threshold = max([share / 2] + [s * PART_SECONDS for s in one])
    split = Rung("split", "in parts", footprint=1, needs=(
        Need("streams to split across", lambda f: workers >= 2,
             "one stream"),
        Need("the tables' sizes", lambda f: total > 0,
             "the source's catalogue gave no sizes to cut them by"),
        Need("the split in the build",
             lambda f: {"--split-tables-larger-than",
                        "--split-max-parts"} <= set(have),
             "the copy installed cannot split one"),
        Need("a table larger than a share", lambda f: bool(big),
             f"none is more than an even share of the copy"
             f" ({human_bytes(share)})"),
        Need("parts worth their query",
             lambda f: threshold < max(big.values()),
             lambda f: f"no part would be longer than {PART_SECONDS}s of"
                       f" one stream at the {human_bytes(max(one or [0]))}/s"
                       " one reached here")))
    whole = Rung("whole", "whole")
    costs = {w: 1 / m["rate"] for w, m in measured.items() if m.get("rate")}
    got = climb("the copy's tables", (split, whole), Facts(), costs=costs)

    def rate(way):
        m = measured[way]
        return (f"{human_bytes(m['rate'])}/s"
                + (f" on {', '.join(m['whose'])}" if m.get("whose") else ""))
    timed = [w for w in ("split", "whole") if w in costs]
    if got.path == "whole":
        held = split in got.ranked
        if not held:
            return _Split("whole", f"tables go whole: {got.reason}")
        if timed == ["split"]:
            return _Split("whole", "tables go whole this time, to measure"
                                   f" it: in parts went {rate('split')}")
        if costs["whole"] > costs["split"]:
            return _Split("whole", "tables go whole: in parts measured"
                                   f" {rate('split')} against"
                                   f" {rate('whole')} whole, too close to"
                                   " call, and whole asks the source fewer"
                                   " queries")
        return _Split("whole", "tables go whole: measured faster here -"
                               f" {rate('whole')} whole against"
                               f" {rate('split')} in parts")
    if len(timed) == 2:
        why = (f"measured faster here - {rate('split')} in parts against"
               f" {rate('whole')} whole")
    elif timed:
        why = (f"to measure it: whole tables went {rate('whole')}, and"
               f" {len(big)} tables are more than an even share"
               f" ({human_bytes(share)})")
    else:
        why = (f"{len(big)} tables are each more than an even share"
               f" ({human_bytes(share)}) of the copy")
    parts = int(math.ceil(max(big.values()) / threshold)) + 1
    return _Split("split", f"tables over {human_bytes(threshold)} go in up"
                           f" to {parts} parts: {why}", threshold, parts,
                  tuple(sorted(t for t, b in big.items() if b > threshold)))


def _copy_catalogue(work, sizes=None):
    """What the copy's own catalogue says it did: {"tables": {table:
    {"bytes", "seconds", "parts", "done"}}, "streams": [bytes a second]}.

    Its own reader is broken in this build - measured, `list progress
    --json` stops on `no such column: bytes` in its catalogue query, and
    `--summary` wants a summary file the table copy never writes - so the
    same table, `summary`, one row per COPY with its bytes and
    milliseconds, is read here, read-only. `sizes` turns the bytes COPY
    sent into bytes on disk, the unit the split threshold is in: measured,
    a 54 MB table sent 40 MB."""
    import sqlite3
    from pathlib import Path
    path = Path(work) / "schema" / "source.db"
    if not path.exists():
        return {"tables": {}, "streams": []}
    try:
        c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
    except sqlite3.Error:
        return {"tables": {}, "streams": []}
    try:
        names = {oid: str(q).replace('"', "") for oid, q in
                 c.execute("select oid, qname from s_table")}
        counts = dict(c.execute("select oid, max(partcount)"
                                " from s_table_part group by oid"))
        rows = c.execute(
            "select tableoid, bytes, duration, done_time_epoch from summary"
            " where tableoid is not null and indexoid is null"
            " and conoid is null").fetchall()
    except sqlite3.Error:
        return {"tables": {}, "streams": []}
    finally:
        c.close()
    tables, sent = {}, {}
    for oid, carried, ms, done in rows:
        name = names.get(oid, str(oid))
        t = tables.setdefault(name, {"bytes": 0, "seconds": 0.0, "done": 0,
                                     "parts": counts.get(oid) or 1})
        if done is not None:
            t["done"] += 1
            t["bytes"] += int(carried or 0)
            t["seconds"] += (ms or 0) / 1000
    for name, t in tables.items():
        t["done"] = t["done"] >= t["parts"]
        disk = (sizes or {}).get(name)
        sent[name] = (disk / t["bytes"]) if disk and t["bytes"] else 1.0
    streams = []
    for oid, carried, ms, done in rows:
        name = names.get(oid, str(oid))
        if done is not None and carried and ms:
            streams.append(carried * sent[name] / (ms / 1000))
    return {"tables": tables, "streams": sorted(streams)}


class _CopyProgress:
    """The streaming copy's progress in migkit's words, table by table as
    its catalogue marks them done, while it runs. Nothing is said where
    there is no catalogue to read (the image keeps its own)."""

    EVERY = 2.0

    def __init__(self, work, log):
        self.work, self.log = work, log
        self.stop = threading.Event()
        self.thread = None

    def _look(self, tally):
        got = _copy_catalogue(self.work)["tables"]
        for name in sorted(got):
            if got[name]["done"]:
                so_far = tally.reached(name)
                if so_far and self.log:
                    self.log(f"{name}: copied ({so_far})")

    def __enter__(self):
        if self.work is None or self.log is None:
            return self
        from .wording import Tally
        self.tally = Tally(_SIZES)

        def loop():
            while not self.stop.wait(self.EVERY):
                try:
                    self._look(self.tally)
                except Exception:  # noqa: BLE001 - progress never stops a copy
                    pass
        self.thread = threading.Thread(target=loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, kind, *exc):
        if self.thread is None:
            return False
        self.stop.set()
        self.thread.join(5)
        if kind is None:
            try:
                self._look(self.tally)
            except Exception:  # noqa: BLE001
                pass
        return False


def _record_copy(hop, work, way, seconds, sizes):
    """The finished copy's rate, the way it went (`way`), and what one of
    its streams carried a second - for the next run's `_copy_split`."""
    from . import planner
    got = _copy_catalogue(work, sizes)
    tables = got["tables"]
    carried = sum((sizes or {}).get(n) or t["bytes"]
                  for n, t in tables.items())
    streams = got["streams"]
    stream = streams[len(streams) // 2] if streams else None
    planner.record_variant(hop, "pgcopydb", way, carried, seconds, stream,
                           {n: [t["bytes"], round(t["seconds"], 3),
                                t["parts"]] for n, t in tables.items()})


def _copy_args(workers, split, resume, filters, snapshot=None):
    """The table copy's options past its subcommand and its two databases:
    the streams, the split, going on from a stopped copy, the filters, and
    the snapshot a caller holds open for it to read under. One builder for
    the whole database's copy and one table's (`pgcopydb_table`)."""
    out = ["--table-jobs", str(workers)] + split.argv()
    if resume:
        out += ["--resume", "--not-consistent"]
    if filters:
        out += ["--filters", str(filters)]
    if snapshot:
        out += ["--snapshot", str(snapshot)]
    return out


#: what the copy says when this machine has no message queue left for its
#: workers, and what that means
QUEUES_GONE = ("this machine has no message queue left for the copy's"
               " workers: copies stopped by force before this one left"
               " theirs behind. `ipcs -q` lists them; `ipcrm -q <id>`"
               " removes one no running process uses. Nothing was copied")


def pgcopydb_move(hop, db, workers, go, log):
    """Parallel data-only copy through pgcopydb, source straight to target.

    `copy table-data` and not `clone`, and the difference matters. `clone`
    restores the schema as well and refuses a target that already has objects
    - measured, it exits with `clone process has terminated`. migkit prepares
    the target schema in its own step before any data moves, so by the time a
    mover runs the target is never empty, and `clone` can never be the right
    subcommand here. Measured again with the build installed here (compiled
    for PostgreSQL 18) into an empty 16: its schema step stopped on
    `unrecognized configuration parameter "transaction_timeout"`, 1.4 s in,
    nothing copied.

    What that leaves, against `pg_dump -Fd -j | pg_restore`: the tables are
    copied in parallel straight from source to target with no intermediate
    directory to write out and read back, the largest of them split across
    streams where that is faster (`_copy_split`). The indexes the target
    has are set aside for the load and rebuilt after it, as many at a time
    as the sizing gives (`_IndexWindow`) - what `clone`'s index jobs would
    do, on a target that already has its schema.

    The copy carries rows and no large objects - measured, a source with
    201 of them left the target with none - so they follow it, carried by
    migkit under the same numbers and compared first
    (`carry_database_objects`). The copy's own large-object step is not
    used: with this build into PostgreSQL 16 its first statement on the
    target was refused (`transaction_timeout`), its transaction aborted,
    and it then waited with nothing to do until it was killed, 338 s on.

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
    s, t = hop.source, hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    # no password in either connection string: they went on the program's
    # command line, where any process listing on the machine read them
    # while it ran. The passwords go in a password file only this user can
    # read, for the run's length; the hop's TLS goes in the addresses
    src, dst = _pg_uri(s, db), _pg_uri(t, ddb, quiet_triggers=True)
    net = envkeys.text("MIGKIT_PGCOPYDB_NETWORK")
    how = pgcopydb_runner()
    # what the copy carries and how many large objects follow it, asked
    # while the plan asks the rest: a plan waits for none of it for long
    facts = (None if go else _ask_soon(
        lambda: (_source_sizes(hop, db), _pg_large_objects(hop, db))))
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
            roots = _pg_roots(hop, db)
            routed = routed_to_copier(hop, db, "pgcopydb", tables,
                                      roots=roots)
            text = pgcopydb_filters(hop, db, tables, also=routed,
                                    roots=roots)
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
    resume = stopped_split = other_scope = False
    # the blocking questions first, so the ones asked beside them have had
    # as long as these took
    blocked = (_pg_references_into(hop, db, routed) if go else
               _within(lambda: _pg_references_into(hop, db, routed)) or [])
    note = _create_note(lambda: _pg_missing_tables(hop, db)[0])
    if go:
        sizes = (_SIZES if _SIZES is not None
                 else _source_sizes(hop, db))
        objects = None
    else:
        sizes, objects = facts(8) or (None, None)
    carried = {n: b for n, b in (sizes or {}).items() if n not in routed}
    split = _copy_split(hop, workers, carried,
                        _pgcopydb_options("copy", "table-data")
                        if how else frozenset())
    work = begun = None
    if how == "local":
        # its own directory for the hop's database, kept under its reports
        # while a copy is under way. pgcopydb keeps its state - including
        # the exported snapshot - under /tmp/pgcopydb by default, so a second
        # run found the first one's snapshot and died on it: measured,
        # `FATAL Failed to use given --snapshot "00000003-00000048-1"`. Kept
        # here instead, a copy that was stopped goes on from the tables it
        # had finished (`--resume`), where it used to start over. The rest
        # comes from a new snapshot (`--not-consistent`): the snapshot the
        # first run copied from ended with it, and the rows copied before
        # and after it can disagree across tables - which is why the move
        # compares the whole result with the source before it is complete.
        work, begun = _streaming_work(hop, db)
        resume = go and begun.exists()
        if resume:
            # a stopped copy goes on cut the way it began - and one that
            # cut a table in parts does not go on at all (`_Split.keep`)
            kept = _Split.resumed(begun) or split
            stopped_split = kept.way == "split"
            # nor one begun under another scope: what it finished was
            # chosen, filtered and named by the scope of then, and going
            # on skips emptying the tables now taken
            from .decide import remembered
            other_scope = hop.scope_changed(remembered(begun, "scope"))
            resume = not stopped_split and not other_scope
            split = kept if resume else split
        args = ["copy", "table-data", "--dir", str(work), "--source", src,
                "--target", dst,
                *_copy_args(workers, split, resume, filters_path)]
    else:
        # the container takes the addresses, passwords and all, from an
        # environment file of this user's the docker client reads
        # (`runner.container`): whether it could read a password file
        # mounted from here depends on the user it runs as, which this
        # machine cannot measure without the image
        args = ["copy", "table-data",
                *_copy_args(workers, split, False, filters_path and
                            "/tmp/migkit-filters.ini")]
    stream = _Streaming(hop, db, ddb, how, net, filters_path)
    # the plan says what happens; the command stays on the step and goes
    # only to the run's debug log
    from .wording import Step, phase
    if blocked:
        # measured: the streaming copy empties each table it loads on its
        # own, one at a time, and the target refuses to empty a table
        # another one references - `cannot truncate a table referenced in
        # a foreign key constraint` - so on such a target it can only fail.
        # The dump and load that carry it instead are recorded as
        # themselves (`Ran`), never as this way's time
        if log:
            log(f"the target's foreign keys ({', '.join(blocked[:3])}"
                + (" ..." if len(blocked) > 3 else "") + ") keep the"
                " streaming copy from emptying the tables it loads, so this"
                " goes through a local copy instead")
        return Ran(pgdump_move(hop, db, workers, go, log), "pgdump")
    copy = Step(phase("stream-copy", workers=workers,
                      split=len(split.tables)), stream.argv(args))
    steps = [_truncate_step(hop)]
    if note:
        steps.insert(0, note)
    if filters_note:
        steps.append(filters_note)
    steps.append(copy)
    steps.append(f"# {split.why}")
    steps.append("# set the target's sequences from the source's")
    if objects:
        steps.append(f"# carry the source's {objects:,} large objects"
                     " after the tables, under the same numbers")
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    if unresolved:
        raise _unresolved_exclusion(db, unresolved)

    # Having the image is not the same as being able to reach the databases
    # from inside it: the container has its own network view, and on a laptop
    # `--network host` is the VM's host, not this one. `ping` is the tool's
    # own answer to that question, and asking it first turns a failed move
    # into a fallback rather than an outage - the dump and load, recorded as
    # themselves. A refusal of the database's is not that: every way would
    # meet it, and it stops the move (`runner.Refused`)
    try:
        # `ping` does not take `--dir` - it answers with its usage if you
        # give it one, which reads as a connectivity failure and sends the
        # move down the fallback path for no reason
        stream.run(["ping"] + (["--source", src, "--target", dst]
                               if how == "local" else []),
                   what="the streaming copy's first reach")
    except (RuntimeError, runner.Blocked) as e:
        if log:
            where = ("from its container"
                     f" (network={net})" if how != "local" else "")
            log(f"the streaming copy cannot reach both databases {where}:"
                f" {str(e).splitlines()[-1][:120]}")
            log("copying through a local dump instead")
        return Ran(pgdump_move(hop, db, workers, go, log), "pgdump")

    took = None
    _pg_create_missing(hop, db, log)
    if resume:
        # what the stopped copy put in is what it goes on from
        if log:
            log("an earlier streaming copy of this database stopped part"
                " way; going on from the tables it finished")
    else:
        if stopped_split and log:
            log("an earlier streaming copy of this database stopped part"
                " way through a table it was copying in parts, and a"
                " copy cut that way cannot be gone on from without"
                " losing rows; it starts over")
        elif other_scope and log:
            log("an earlier streaming copy of this database stopped part"
                " way under another scope than the hop's now, and what it"
                " finished was taken under that one; it starts over")
        _pg_truncate_target(hop, db, log)
        if how == "local":
            import shutil
            from .decide import remember
            shutil.rmtree(work, ignore_errors=True)
            work.mkdir(parents=True)
            begun.write_text(time.strftime("%Y-%m-%dT%H:%M:%S"))
            split.keep(begun)
            # the scope it begins under, beside the record that it began
            remember(begun, "scope", hop.scope_fingerprint())
    with _IndexWindow(hop, db, workers, log):
        if log:
            log(copy)
            log(split.why)
        began = time.monotonic()
        try:
            with _CopyProgress(work if how == "local" else None, log):
                stream.run(args, what="the streaming copy")
        except RuntimeError as e:
            if "message queue" in str(e):
                raise runner.Failed(QUEUES_GONE) from None
            raise
        took = time.monotonic() - began
    if how == "local":
        # a resumed copy's time is not the whole copy's, so only a copy
        # from the start is a rate
        if not resume:
            _record_copy(hop, work, split.way, took, carried)
        # finished: nothing to go on from next time
        import shutil
        shutil.rmtree(work, ignore_errors=True)
        from .decide import forget
        forget(begun)
        begun.unlink(missing_ok=True)
    _pg_finish_created(hop, db, log, workers)
    _pg_carry_sequences(hop, db, log)
    from .engines.postgres import PostgresEngine
    PostgresEngine(hop).carry_database_objects(db, log)
    return steps


class Ran(list):
    """A bulk path's steps, and the way that ran them where it was another
    than the one asked for: a streaming copy that could not reach both
    databases is carried by the dump and load, and its time is theirs -
    kept under the streaming copy's name, it corrupted the rates every
    later choice is made on."""

    def __init__(self, steps, via):
        super().__init__(steps or [])
        self.via = getattr(steps, "via", via)


def _pg_uri(ep, name, secret=False, quiet_triggers=False):
    """A PostgreSQL address as a program is given it: the user, the
    database, the hop's TLS as libpq reads it in the address
    (`Endpoint.libpq_tls`, the automatic answer included - the streaming
    copy connected with no `sslmode` at all while `doctor` said verified),
    and the password only where `secret` asks for it: an address on a
    command line takes none, and its password goes in a file of this
    user's. `quiet_triggers`: the loading connection keeps the target's
    triggers off its rows (`QUIET_TRIGGERS`)."""
    from urllib.parse import urlencode
    auth = quote(ep.user or "", safe="")
    if secret:
        auth += ":" + quote(ep.password or "", safe="")
    query = dict(ep.libpq_tls())
    if quiet_triggers:
        query["options"] = "-c session_replication_role=replica"
    host = f"[{ep.host}]" if ":" in str(ep.host) else ep.host
    tail = urlencode(query, quote_via=quote)
    return (f"postgresql://{auth}@{host}:{ep.port}/{quote(str(name), safe='')}"
            + (f"?{tail}" if tail else ""))


def _pg_tls_files(ep):
    """The files the hop's TLS names for `ep` (its authority, its client
    certificate and key, its revocation list): what a container is given
    read-only, at the same paths."""
    return [v for k, v in ep.libpq_tls().items()
            if k in ("sslrootcert", "sslcert", "sslkey", "sslcrl")]


class _Streaming:
    """The streaming copy of one database as it runs here: its local build
    with the passwords in a password file of this user's, or its image
    with the addresses in an environment file of this user's - either made
    new for each run under a name of its own, and gone with it."""

    def __init__(self, hop, db, ddb, how, net, filters=None):
        self.hop, self.db, self.ddb = hop, db, ddb
        self.how, self.net, self.filters = how, net, filters

    def argv(self, args):
        """The command line as the plan and the debug log show it."""
        if self.how == "local":
            return ["pgcopydb", *args]
        return (["docker", "run", "--rm", "--network", self.net,
                 "--env-file", "<private environment file>"]
                + (["-v", f"{self.filters}:/tmp/migkit-filters.ini:ro"]
                   if self.filters else [])
                + [PGCOPYDB_IMAGE, "pgcopydb", *args])

    def run(self, args, what="", stall=None, log=None, progress=None):
        s, t = self.hop.source, self.hop.target
        if self.how == "local":
            with PrivateFile(_pgpass_text(self.hop, self.db, self.ddb),
                             ".pgpass") as passfile:
                return _sh(["pgcopydb", *args], {"PGPASSFILE": passfile},
                           log, progress)
        mounts = [(p, p) for ep in (s, t) for p in _pg_tls_files(ep)]
        if self.filters:
            mounts.append((str(self.filters), "/tmp/migkit-filters.ini"))
        return runner.container(
            PGCOPYDB_IMAGE, ["pgcopydb", *args],
            secrets={"PGCOPYDB_SOURCE_PGURI": _pg_uri(s, self.db,
                                                      secret=True),
                     "PGCOPYDB_TARGET_PGURI": _pg_uri(
                         t, self.ddb, secret=True, quiet_triggers=True)},
            mounts=mounts, network=self.net, what=what, stall=stall,
            log=log, progress=progress)


def _ask_soon(ask):
    """`ask()` started now; the answer is had by calling the result with
    how many seconds more to wait for it (None when it failed or is not in
    yet). What a plan asks beside its other questions, so the waits
    overlap instead of adding up."""
    got = {}

    def go():
        try:
            got["answer"] = ask()
        except Exception:  # noqa: BLE001 - a plan says less, then
            got["answer"] = None
    t = threading.Thread(target=go, daemon=True)
    t.start()

    def answer(seconds=8):
        t.join(seconds)
        return got.get("answer")
    return answer


def _pg_large_objects(hop, db):
    """How many large objects the source holds."""
    from .engines.postgres import PostgresEngine
    got = PostgresEngine(hop)._psql(
        "src", db, "select count(*) from pg_largeobject_metadata").strip()
    return int(got) if got.isdigit() else None


#: the subcommands of the streaming copy whose options are accounted for:
#: the ones migkit runs, and `clone` and `copy blobs`, which hold the rest
#: of what the program can do
PGCOPYDB_COMMANDS = ("clone", "copy table-data", "copy blobs", "follow",
                     "stream sentinel get", "stream sentinel set apply",
                     "stream sentinel set endpos", "ping", "compare schema",
                     "compare data", "list progress")

_CLONE = ("only a whole-database clone takes it, and migkit does not clone:"
          " it creates the tables the target lacks itself before any copy,"
          " so the target is never empty when the copy starts - and"
          " measured, this build (compiled for PostgreSQL 18) stopped a"
          " clone into 16 at its schema step on `transaction_timeout`, 1.4 s"
          " in, nothing copied")
_BLOBS = ("the large objects are carried by migkit after the table copy,"
          " under the same numbers and compared first"
          " (`carry_database_objects`): measured, this build's own"
          " large-object copy into PostgreSQL 16 had its first statement"
          " refused (`transaction_timeout`) and then waited with nothing to"
          " do until it was killed, 338 s on")
_SERVER = ("the two databases, without their passwords: those go in a"
           " password file only this user can read, for the run")
_DIR = ("its working state under the hop's report directory, so a stopped"
        " copy goes on from what it finished and never meets another run's"
        " snapshot (measured, a shared directory died on `Failed to use"
        " given --snapshot`)")
_SCHEMA = ("migkit restores the source's schema itself, the pre- and"
           " post-data sections without owners or grants (`_pg_schema`),"
           " and compares extensions, collations and settings in its"
           " checks")
_ROLES = ("users and their grants are carried by `migkit users`, by"
          " password hash where the source lets it read one")
_STREAM = ("the change stream is started on its own (`follow`), so its end"
           " can be named once the copy is proved")
_SNAPSHOT = ("the leg starts from its own slot's position, taken before"
             " the copy it follows, and applies every later change again"
             " by key; the copy is proved by `check`, not by sharing its"
             " snapshot")
_FRESH = ("a fresh copy starts in a directory migkit has emptied itself;"
          " measured, a leftover directory without this or --resume fails"
          " on `invalid snapshot identifier`, and this would allow what"
          " emptying it already does")
_ONE_FIELD = ("one field of the reading; the whole reading comes at once"
              " with --json")
_COORDINATOR = ("the leg is reached through its directory on this machine,"
                " not over the network")


def _pgcopydb_account():
    o = {}

    def put(commands, option, state, why):
        for c in commands:
            o[(c, option)] = (state, why)
    put(("copy table-data", "follow", "ping", "compare schema",
         "compare data"), "--source", USED, _SERVER)
    put(("copy table-data", "follow", "ping", "compare schema",
         "compare data"), "--target", USED, _SERVER)
    put(("stream sentinel set endpos", "list progress"), "--source", UNUSED,
        "the source's address reaches the leg through its environment,"
        " password and all, and never through a command line")
    put(("copy table-data", "follow", "compare schema", "compare data"),
        "--dir", USED, _DIR)
    put(("copy table-data",), "--table-jobs", USED,
        "as many streams as migkit's sizing gives the move (`sizing.fit`):"
        " this machine, both servers' free connections and CPUs, and the"
        " work there is")
    put(("copy table-data",), "--split-tables-larger-than", USED,
        "half an even share of what the copy carries, from the source's"
        " catalogue, not under ten seconds of one stream as measured here,"
        " and only where splitting went faster here (`_copy_split`)")
    put(("copy table-data",), "--split-max-parts", USED,
        "enough parts for the largest table's parts to fit one share each"
        " (`_copy_split`)")
    put(("copy table-data", "follow"), "--filters", USED,
        "the tables the hop excludes, and on the copy the ones routed to"
        " the table copier, resolved against what the source has")
    put(("copy table-data",), "--resume", USED,
        "a copy that was stopped goes on from the tables it finished,"
        " matched to migkit's own record that it began - only one that"
        " copied its tables whole: going on from one that cut a table in"
        " parts lost half the table's rows and exited 0 (measured)")
    put(("copy table-data", "follow"), "--not-consistent", USED,
        "the rest of a stopped copy, and a stream, from a new snapshot:"
        " the move compares the whole result with the source before it is"
        " complete")
    put(("copy table-data", "follow"), "--restart", UNUSED, _FRESH)
    put(("copy table-data",), "--snapshot", USED,
        "one table copied whole under the snapshot migkit holds open for"
        " it, so the digest of the source reads exactly what was copied"
        " (`pgcopydb_table`)")
    put(("follow",), "--snapshot", UNUSED, _SNAPSHOT)
    put(("follow",), "--resume", UNUSED,
        "not yet: a leg is started once for a catch-up and torn down after"
        " it, so there is no stopped leg to go on from")
    put(("follow",), "--slot-name", USED,
        "a slot named after the hop and the database, so teardown can name"
        " it and drop it; after a copy of the source's pages, the slot made"
        " before that copy (`physical.follow_from`)")
    put(("follow",), "--create-slot", USED,
        "the leg makes its slot when it starts, except after a copy of the"
        " source's pages, whose slot was made before it; teardown drops"
        " either, since an abandoned slot pins the source's log")
    put(("follow",), "--origin", USED,
        "an origin on the target named after the hop, which records how far"
        " it applied - migkit's reading of the leg's progress; after a copy"
        " of the source's pages, made by migkit at the position the copy"
        " holds, so the leg applies only what commits after it")
    put(("follow",), "--plugin", USED,
        "pgoutput, which every stock server has (measured: postgres:16"
        " carries pgoutput and test_decoding only)")
    put(("follow",), "--wal2json-numeric-as-string", UNUSED,
        "the plugin is pgoutput, which every stock server has; wal2json is"
        " not in a stock server")
    put(("follow",), "--endpos", TOPPED,
        "the end is set on the running leg once it is ready (`set endpos"
        " --current`), the source's position at that moment")
    put(("copy blobs",), "--large-objects-jobs", TOPPED, _BLOBS)
    for f in ("--source", "--target", "--dir", "--drop-if-exists",
              "--restart", "--resume", "--not-consistent", "--snapshot"):
        put(("copy blobs",), f, UNUSED, _BLOBS)
    put(("stream sentinel get",), "--json", USED,
        "the leg's positions for the lag `/metrics` shows"
        " (`follow_lag`); its end position is read from nowhere, measured"
        " wrong in this build's JSON (the start given as the end)")
    for f in ("--startpos", "--endpos", "--apply", "--write-lsn",
              "--flush-lsn", "--transform-lsn", "--replay-lsn"):
        put(("stream sentinel get",), f, UNUSED, _ONE_FIELD)
    put(("stream sentinel get", "stream sentinel set apply",
         "stream sentinel set endpos"), "--host", UNUSED, _COORDINATOR)
    put(("stream sentinel get", "stream sentinel set apply",
         "stream sentinel set endpos"), "--port", UNUSED, _COORDINATOR)
    put(("stream sentinel set endpos",), "--current", USED,
        "the stop is the source's position when the leg is told, which"
        " `--endpos` could not know when the leg started")
    put(("compare schema", "compare data"), "--all-databases",
        UNUSED, "migkit takes the hop's databases one by one, each with its"
        " own plan, copy and check")
    put(("compare data",), "--json", UNUSED,
        "not yet: the second reading is taken from its table's difference"
        " marker, which it has always printed")
    put(("list progress",), "--summary", TOPPED,
        "measured broken in this build: it wants a summary file the table"
        " copy never writes; migkit reads the per-table timings from the"
        " copy's own catalogue instead (`_copy_catalogue`)")
    put(("list progress",), "--json", TOPPED,
        "measured broken in this build (`no such column: bytes` in its own"
        " catalogue query); migkit reads the same catalogue itself, for the"
        " progress lines and the next run's rates")
    put(("list progress",), "--dir", TOPPED,
        "the catalogue is read in the copy's own directory by migkit")
    # the whole-database clone: what migkit does instead, or why not
    put(("clone",), "--index-jobs", TOPPED,
        "migkit sets the target's secondary indexes aside for the load and"
        " rebuilds them after it, as many at a time as the sizing gives"
        " (`_IndexWindow`, measured 1.9x on PostgreSQL 16)")
    put(("clone",), "--restore-jobs", TOPPED,
        "the schema's sections are restored by migkit's own step and the"
        " indexes rebuilt by `_IndexWindow`, sized as the move is")
    put(("clone",), "--large-objects-jobs", TOPPED, _BLOBS)
    put(("clone",), "--skip-large-objects", TOPPED, _BLOBS)
    put(("clone",), "--estimate-table-sizes", TOPPED,
        "the sizes come from migkit's own read of the catalogue"
        " (`table_facts`), once for the plan, the split and the progress")
    for f in ("--skip-extensions", "--skip-ext-comments", "--skip-collations",
              "--skip-db-properties", "--no-owner", "--no-acl",
              "--no-comments", "--no-tablespaces"):
        put(("clone",), f, TOPPED, _SCHEMA)
    put(("clone",), "--roles", TOPPED, _ROLES)
    put(("clone",), "--no-role-passwords", TOPPED, _ROLES)
    put(("clone",), "--skip-vacuum", TOPPED,
        "migkit refreshes the target's statistics itself after every load"
        " (`settle_target`)")
    put(("clone",), "--skip-analyze", TOPPED,
        "migkit refreshes the target's statistics itself after every load"
        " (`settle_target`)")
    for f in ("--follow", "--plugin", "--slot-name", "--create-slot",
              "--origin", "--endpos"):
        put(("clone",), f, UNUSED, _STREAM)
    for f in ("--source", "--target", "--dir", "--table-jobs",
              "--split-tables-larger-than", "--split-max-parts", "--filters",
              "--restart", "--resume", "--not-consistent", "--snapshot",
              "--drop-if-exists", "--fail-fast", "--use-copy-binary",
              "--requirements", "--skip-split-by-ctid", "--all-databases",
              "--wal2json-numeric-as-string"):
        o.setdefault(("clone", f), (UNUSED, _CLONE))
    return o


def _pgcopydb_installed():
    if not which("pgcopydb"):
        return None
    return {c: _long_options("pgcopydb", *c.split())
            for c in PGCOPYDB_COMMANDS}


tooluse.register("pgcopydb", options=_pgcopydb_account(),
                 installed=_pgcopydb_installed)


def pgcopydb_table(hop, db, table, snapshot, workers, size, log=None):
    """One table through the streaming copy, read under `snapshot` - an
    export the caller holds open, so the caller's digest of the source
    reads exactly what was copied - and cut across `workers` streams the
    way the whole database's copy is (`_copy_split`, with the table's
    `size` bytes as the whole): in parts where that went faster here, the
    parts no shorter than ten seconds of one stream. The table is emptied
    by the copy itself. Raises where the copy fails; the caller proves the
    rest.

    Tables are copied side by side, each in a directory of its own named
    from the table (`report_name`, so a name with `/` or `..` in it stays
    inside the reports) and with a password file of its own
    (`_Streaming`): one fixed file for all of them was removed under the
    others' connections by the first to finish.

    What the table copier's own pass-through is timed against, table by
    table (`PostgresEngine.TABLE_WAYS`)."""
    import shutil
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    how = pgcopydb_runner()
    if not how:
        raise runner.Blocked("the streaming copy is not installed here")
    _filter_names_hold([table])
    work = hop.report_dir(db) / f"streaming-table-{report_name(table)}"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    filters = work / "filters.ini"
    filters.write_text(f"[include-only-table]\n{table}\n")
    split = _copy_split(hop, int(workers), {table: int(size or 0)},
                        _pgcopydb_options("copy", "table-data"))
    if log:
        log(f"{table}: {split.why}")
    net = envkeys.text("MIGKIT_PGCOPYDB_NETWORK")
    if how == "local":
        args = ["copy", "table-data", "--dir", str(work / "state"),
                "--source", _pg_uri(hop.source, db), "--target",
                _pg_uri(hop.target, ddb, quiet_triggers=True),
                *_copy_args(workers, split, False, filters, snapshot)]
    else:
        args = ["copy", "table-data",
                *_copy_args(workers, split, False, "/tmp/migkit-filters.ini",
                            snapshot)]
    try:
        _Streaming(hop, db, ddb, how, net, filters).run(
            args, what=f"{table}: the streaming copy", log=log)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _filter_names_hold(names):
    """Refuses a table the streaming copy's filter file cannot name: one
    whose name holds a line break, which the file reads as a line of its
    own - a section of its own, where it starts with `[`."""
    bad = [n for n in names if "\n" in str(n) or "\r" in str(n)]
    if bad:
        raise runner.Blocked(
            f"{bad[0]!r}: its name holds a line break, which the streaming"
            " copy's list of tables cannot say; the table copier carries it")


def _streaming_work(hop, db):
    """(the streaming copy's working directory, the file saying a copy
    into it began and has not finished) for the hop's database."""
    base = hop.report_dir(db)
    return base / "streaming-work", base / "streaming-work.begun"


def _pgpass_text(hop, db, ddb):
    """A password file's lines for the source's and the target's
    passwords. `:` and `\\` in a field are escaped, as the file's format
    asks."""
    def field(v):
        return str(v).replace("\\", "\\\\").replace(":", "\\:")
    s, t = hop.source, hop.target
    return "\n".join(":".join(field(x) for x in (e.host, e.port, name,
                                                 e.user, e.password or ""))
                     for e, name in ((s, db), (t, ddb))) + "\n"


def _pgpass(path, hop, db, ddb):
    """The password file at `path`, readable by this user only from the
    moment it exists - made 0600, never made and then narrowed."""
    import os
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    if hasattr(os, "fchmod"):
        os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(_pgpass_text(hop, db, ddb))


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


def stream_ways():
    """The ways a hop's changes follow its source (`move --mode cdc`), as
    rungs of one ladder (`decide`) - migkit's own change tail and the
    programs it wraps side by side.

    The list's order is the order the dispatch took before it was a
    ladder, so a hop that asks for nothing takes the way it always took:
    PostgreSQL its servers' own replication, unless the network keeps the
    target from dialling the source (`MIGKIT_CDC=follow`) and the leg run
    from here connects out to both instead; another engine its change
    tail, then its servers' replication, then the streaming pipeline.

    A way's measured cost (seconds a change took to reach the target,
    kept with the hop's rates under `cdc/`) ranks it among the ways that
    hold, the cheaper first. A way the dispatch placed behind another
    competes with it only once it has a cost of its own on this hop:
    `decide` tries a way never timed first, and trying a way of following
    a source is not free - a slot, a replication set up on the target,
    a pipeline of containers - so none is started only to be timed. The
    leg run from here records its cost (`pgcopydb_follow`); the tail and
    the pipeline do not yet, so until they do they rank as the list does.
    """
    from .decide import Need, Rung
    pg = Need("a PostgreSQL source",
              lambda f: f["engine"] == "postgres" and f["repl"],
              "{rung} reads a PostgreSQL source's log only")

    def unless_measured(way, holds, why):
        # behind the way before it, unless timed here itself
        return Need(f"{way} measured, or the way before it unable to run",
                    lambda f: holds(f) or f["costs"].get(way) is not None,
                    why)
    # what the hop asks of its tables that the servers' own replication,
    # which applies whole rows under the source's names, cannot do: its
    # tables are then followed by migkit's own tail (`by_pair`), which
    # reads the mapping and the scope in one place
    as_they_are = Need("the hop's tables carried as they are",
                       lambda f: not f.get("own_refused"),
                       lambda f: f["own_refused"])
    return (
        Rung("follow", "the change leg run from here",
             needs=(pg, Need("the target unable to dial the source",
                             lambda f: follow_selected() == "follow",
                             "the target can dial the source, so its own"
                             " replication carries the changes"),
                    as_they_are),
             because="the target cannot dial the source, and the leg"
                     " connects out to both from here",
             footprint=1),
        Rung("subscription", "the servers' own replication",
             needs=(pg, Need("the target able to dial the source",
                             lambda f: follow_selected() != "follow",
                             "the target cannot dial the source"
                             " (MIGKIT_CDC=follow)"),
                    as_they_are,
                    # asked last: it reads the source's publication
                    Need("a source that takes tables into its publication"
                         " without losing their changes",
                         lambda f: not f.get("publication_risk"),
                         lambda f: f["publication_risk"])),
             because="the servers' own replication, nothing running here"),
        Rung("tail", "migkit's own change tail",
             needs=(Need("a change tail",
                         lambda f: f["tail"] or f.get("by_pair"),
                         "{rung} does not read this engine's log"),
                    Need("its driver",
                         lambda f: f["ready"] or f.get("by_pair"),
                         "the change tail needs a driver this machine does"
                         " not have"),
                    unless_measured(
                        "tail", lambda f: not (f["engine"] == "postgres"
                                               and f["repl"])
                        or bool(f.get("publication_risk"))
                        or bool(f.get("by_pair")),
                        "a PostgreSQL source's changes go by its servers'"
                        " replication or the leg run from here")),
             because="read and applied by migkit, by key, with every batch"
                     " accounted"),
        Rung("replica", "the servers' own replication",
             needs=(Need("replication between the servers",
                         lambda f: f["repl"] and f["engine"] != "postgres",
                         "{rung} is not set up for this engine"),
                    unless_measured(
                        "replica", lambda f: not f["ready"],
                        "the change tail runs here, and applies by key"
                        " where a replica stops on a row the copy already"
                        " carried"),
                    as_they_are)),
        # its log is a source-available broker: started only once its
        # terms are accepted on this machine, and taken down whatever
        # they are, since a pipeline already running is the operator's
        Rung("pipeline", PIPELINE_SAID,
             needs=(Need("an engine it streams",
                         lambda f: stream_supported(f["engine"]),
                         "{rung} does not stream this engine"),
                    unless_measured(
                        "pipeline", lambda f: not f["ready"],
                        "the change tail runs here"),
                    Need("its log's terms accepted",
                         lambda f: f.get("drop") or f["broker"][0],
                         lambda f: f"{PIPELINE_SAID}: {f['broker'][1]}")),
             footprint=2),
    )


#: the pipeline rung in an operator's words
PIPELINE_SAID = "migkit's streaming pipeline"


def stream_way(hop, engine, repl, tail, ready, drop=False, risk=None,
               refused=None, by_pair=None):
    """The `decide.Choice` of the way this hop's changes follow its source,
    from what the engine has (`repl`: its servers' replication, `tail`:
    migkit's change tail, `ready`: the tail's driver installed) and what
    each way was measured to cost on this hop; `drop` where the way is
    being taken down rather than set up. `risk()`, where given, says why
    the servers' own replication would lose a table's changes here
    (`PostgresEngine.publication_add_risk`), read only if that way is
    still in the running; `refused()` why it would carry the hop's tables
    wrongly, and `by_pair()` whether migkit's own tail then follows them
    (`cli._own_refused`)."""
    from .decide import climb
    facts = stream_facts(hop, engine, repl, tail, ready, drop, risk,
                         refused, by_pair)
    return climb(hop.name, stream_ways(), facts, costs=facts["costs"])


def stream_facts(hop, engine, repl, tail, ready, drop=False, risk=None,
                 refused=None, by_pair=None):
    """What `stream_ways`' needs read, as `stream_way` climbs them and as
    `decide.attempt` runs them (`cli._cdc`): one reading of the hop."""
    from .decide import Facts, HopCosts
    return Facts({"engine": engine, "repl": bool(repl), "tail": bool(tail),
                  "ready": bool(ready), "costs": HopCosts(hop, "cdc/"),
                  "drop": bool(drop)},
                 broker=lambda: tooluse.allowed("redpanda", hop),
                 publication_risk=risk or (lambda: ""),
                 own_refused=refused or (lambda: ""),
                 by_pair=by_pair or (lambda: False))


def _target_writes(hop, db):
    """Rows written into the target's tables since its statistics began:
    what a change leg applied, taken as the difference across the leg.
    None where the target cannot be asked."""
    try:
        got = _pg_psql(hop, db, "select coalesce(sum(n_tup_ins + n_tup_upd"
                                " + n_tup_del), 0) from pg_stat_user_tables")
        return int(got.strip())
    except Exception:  # noqa: BLE001 - no number, then no cost
        return None


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

    A catch-up with an end: the end is the source's position when the leg
    is ready. Where the hop's `follow_until` is `stopped` the leg runs on
    instead until asked - ctrl-c, a service manager's stop, `tail.stop`
    (`_follow_until_asked`) - and the end is named then, so it still ends
    where it can say it did (P3).

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

    After a copy of the source's pages (`physical.follow_from`) the leg
    goes on from that copy's position, never from now: it reads the slot
    made before the copy instead of making one, and its origin is made
    on the target at the position the restored target proved, as the
    subscription's is (`physical.pg_continue`). The program keeps an
    origin that exists and applies only what commits after its progress
    (its own messages: `Replication origin ... already exists at LSN`,
    `Setting up previous LSN from replication origin ... progress`); a
    run after the first finds the origin moved on and leaves it there.
    Where the seed cannot be gone on from this way, `follow_from` refuses
    with the reason.
    """
    import os

    from . import physical
    from .engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    seed = physical.follow_from(hop, eng, db)
    origin = eng.follow_origin(db)
    slot = seed["slot"] if seed else eng.follow_slot(db)
    d = follow_dir(hop, db)
    s, t = hop.source, hop.target
    # passwords and the hop's TLS in the addresses, which reach the leg in
    # its environment, never on its command line
    src = _pg_uri(s, db, secret=True)
    dst = _pg_uri(t, hop.target_db(db), secret=True)
    argv = ["pgcopydb", "follow", "--dir", str(d), "--slot-name", slot,
            *([] if seed else ["--create-slot"]), "--origin", origin,
            "--plugin", "pgoutput", "--not-consistent"]
    steps = [
        f"read the changes through slot {slot} and apply them on the"
        f" target under origin {origin}",
        "tell the leg to apply as soon as it is ready"
        "   (without this nothing is ever applied)",
        ("keep it applying until asked to end (follow_until: stopped),"
         " then end it at the source's position of that moment"
         if _follow_runs(hop) else
         "end it at the source's position of that moment"
         "   (the provable stop)"),
        f"state kept in {d} - the source releases WAL as soon as the leg"
        " has written it there, so that directory is the only copy until"
        " the target has it",
        f"changes from before the slot {slot} existed are not carried;"
        " the bulk copy and migkit check are what cover those",
    ]
    if seed:
        steps[0] = (f"read the changes through slot {slot}, made before the"
                    f" copy of {seed['at']}, and apply on the target under"
                    f" origin {origin} only what commits after"
                    f" {seed['lsn']}, the position the copy holds")
        steps[4] = (f"origin {origin} made on the target at {seed['lsn']}"
                    " where it is new: the copy holds everything before"
                    " that position, and nothing before it is applied"
                    " again")
    # the tables the hop excludes are the target's own, and a stream that
    # carried their source's changes would overwrite what the move left
    # alone; and a table the hop filters, renames or carries only some
    # changes of cannot be carried by a stream that applies whole rows
    # under their own names
    if _routes_any(hop, db) or hop.table_map() or \
            (hop.mapping or {}).get("ops"):
        tables = (eng._all_tables("src", db) if go else
                  _within(lambda: eng._all_tables("src", db)))
        if tables is None:
            steps.append("the tables the hop excludes are resolved against"
                         " the source when the leg starts")
        else:
            # a partition goes as its table goes (`_pg_roots`)
            roots = _pg_roots(hop, db, eng=eng)
            refused = stream_cannot_carry(
                hop, db, sorted({roots.get(t, t) for t in tables}))
            if refused:
                raise SystemExit(cannot_carry_words(refused))
            text = pgcopydb_filters(hop, db, tables, roots=roots)
            if text:
                ini = hop.report_dir(db) / "follow-filters.ini"
                ini.write_text(text)
                argv += ["--filters", str(ini)]
                steps.append(f"{text.count(chr(10)) - 1} tables the hop"
                             " excludes are left out of the stream")
    from .wording import Step
    # the command stays on the step, for the debug log and the tests
    steps[0] = Step(steps[0], argv)
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
    if seed:
        _seed_origin(eng, db, origin, seed["lsn"])
    env = {"PGCOPYDB_SOURCE_PGURI": src, "PGCOPYDB_TARGET_PGURI": dst}
    out = d / "follow.log"
    if log:
        log(steps[0])
    # what the target had taken before the leg, for its cost: the rows it
    # writes over the time from being told to apply to its end
    written = _target_writes(hop, db)
    # the catch-up with an end, as it always was; or, where the hop's
    # `follow_until` is stopped, a leg that runs until asked to end
    running = _follow_runs(hop) and timeout is None
    import subprocess
    with out.open("ab") as fh:
        proc = runner.start(argv, env, stdin=subprocess.DEVNULL, stdout=fh,
                            stderr=fh, secrets=(s.password, t.password))
    limit = timeout or int(os.environ.get("MIGKIT_FOLLOW_TIMEOUT", "1800"))
    with runner.stopping(), proc:
        try:
            _follow_sentinel(d, env, proc, log, end=not running)
            applying = time.monotonic()
            if running:
                _follow_until_asked(hop, db, d, env, proc, log)
            try:
                proc.wait(timeout=limit)
            except subprocess.TimeoutExpired:
                raise SystemExit(
                    f"the CDC leg did not reach its end position in"
                    f" {limit}s, and was stopped there; its log is {out}."
                    " Nothing has been marked as caught up - raise"
                    " MIGKIT_FOLLOW_TIMEOUT if the backlog is simply larger"
                    " than that") from None
        finally:
            if proc.poll() is None:
                proc.stop(grace=30)
    if proc.returncode:
        raise SystemExit(f"the CDC leg exited {proc.returncode} before it"
                         f" finished catching up; its log is {out}")
    after = _target_writes(hop, db)
    if written is not None and after is not None and after > written:
        # seconds a change took to reach the target at a named position,
        # the wait for the end position included: what this way costs
        # beside the others (`stream_way`)
        from .decide import HopCosts
        HopCosts(hop, "cdc/").saw("follow", after - written,
                                  time.monotonic() - applying)
    steps.append(f"target applied up to {eng.applied_lsn(db) or 'nothing'}"
                 " - run migkit check to prove the rows, an LSN is not a"
                 " row count")
    return steps


def _seed_origin(eng, db, origin, lsn):
    """The leg's origin on the target at `lsn` - made where it is not
    there, and advanced only where it holds no progress yet: a leg that
    ran before moved it on, and it is never set back."""
    name = origin.replace("'", "''")
    # the engine maps the target's name for this database itself; mapping
    # it here as well asked a database the hop's `db_map` never named
    eng._psql("dst", db, f"select pg_replication_origin_create('{name}')"
                          " where not exists (select 1 from"
                          " pg_replication_origin where roname ="
                          f" '{name}')")
    eng._psql("dst", db, f"select pg_replication_origin_advance('{name}',"
                          f" '{lsn}') where pg_replication_origin_progress("
                          f"'{name}', false) is null")


def follow_teardown(hop, db, go, log=None):
    """Take back what the CDC leg left on both servers.

    The slot is the one that matters: an abandoned logical slot pins WAL
    until the source runs out of disk, which is a source outage caused by
    the migration tooling (E5). pgcopydb also creates a publication named
    after the slot, and the origin lives on the target - all three are
    named by migkit, so all three can be named here. A hop that may be
    seeded from a copy of the source's pages takes back the slot made
    before that copy and the publication named after it as well, where
    no subscription holds the slot (`physical.follow_from`).
    """
    from . import physical
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
    seed = physical.seed_slot(eng, db)
    if physical.settings(hop) and seed not in eng._target_feeds(db):
        plan[2:2] = [
            ("src", f"select pg_drop_replication_slot('{seed}') from"
                    f" pg_replication_slots where slot_name = '{seed}'"
                    " and not active"),
            ("src", f"drop publication if exists {seed}")]
    steps = [f"{side}: {sql}" for side, sql in plan]
    if not go:
        return steps
    for side, sql in plan:
        d = db if side == "src" else hop.target_db(db)
        try:
            eng._psql(side, d, sql)
        except RuntimeError as e:
            # each is independently absent on a leg that never ran, or that
            # was torn down already; saying which one refused beats
            # stopping before the slot is dropped
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


def _follow_runs(hop):
    """Whether the change leg run from here runs until asked to end, not
    to the position of the moment it starts (P3): only where the hop's
    `follow_until` says `stopped` - a leg with no word on it catches up
    and ends, as it always did."""
    from . import tailctl
    return (hop.options or {}).get("follow_until") is not None \
        and tailctl.until(hop) == "stopped"


def _follow_until_asked(hop, db, d, env, proc, log=None):
    """A leg left applying until someone asks it to end - ctrl-c, a
    service manager's stop, or `tail.stop` beside the hop's position
    (`tailctl`) - and then ended as a catch-up is: its end named at the
    source's position of that moment, so what was written before the
    request reaches the target. Marked running while it waits, for
    `/metrics` and for a repair to see it; returns once the end is
    named, for the caller to wait for the leg to reach it."""
    import signal
    import time as _t

    from . import tailctl
    where = hop.report_dir(db)
    asked = []

    def ask(*_):
        asked.append(True)
    try:
        before = signal.signal(signal.SIGTERM, ask)
    except ValueError:
        before = None
    try:
        with tailctl.Running(where):
            if log:
                log("the change leg applies until it is asked to end:"
                    f" ctrl-c, or `tail.stop` in {where}")
            try:
                while proc.poll() is None and not asked \
                        and not tailctl.stop_asked(where):
                    _t.sleep(2)
            except KeyboardInterrupt:
                asked.append(True)
            if proc.poll() is not None:
                return
            if log:
                log("asked to end: the leg applies up to the source's"
                    " position of now, then stops")
            _follow_end(d, env, log)
    finally:
        if before is not None:
            signal.signal(signal.SIGTERM, before)


def _follow_end(d, env, log=None):
    """Name the stop of a running leg: the source's position of this
    moment (`set endpos --current`). The leg applies up to it and exits."""
    _sentinel_set(d, env, ["set", "endpos", "--current"], "end position",
                  log)


def _sentinel(d, env, timeout=30):
    """The running leg's sentinel as its own JSON says it (`sentinel get
    --json`, the reading `follow_lag` takes too), or None where it has
    none yet."""
    got = runner.run(["pgcopydb", "stream", "sentinel", "get", "--json",
                      "--dir", str(d)], env, check=False, timeout=timeout,
                     what="the change leg's state")
    try:
        reading = json.loads(got.stdout or "")
    except ValueError:
        return None
    return reading if got.returncode == 0 and isinstance(reading, dict) \
        else None


def _follow_sentinel(d, env, proc, log=None, end=True):
    """Turn the apply on and name the stop, once the run has made its
    sentinel. `follow` creates it at startup, so this cannot be done before
    the process exists - which is exactly why it is migkit's job and not a
    line in a runbook somebody forgets. `end` False leaves the stop to be
    named later (`_follow_end`), for a leg that runs until asked. Ready is
    read by field from its JSON, where the text it prints for people was
    searched for the word `apply`."""
    import time as _t
    ready_by = _t.monotonic() + 120
    while _t.monotonic() < ready_by:
        if proc.poll() is not None:
            raise SystemExit("the CDC leg exited before it was told to"
                             " apply anything, so nothing reached the"
                             f" target; its log is {d / 'follow.log'}")
        reading = _sentinel(d, env)
        if reading is not None and "apply" in reading:
            break
        _t.sleep(2)
    else:
        raise SystemExit("the CDC leg never became ready to be told to apply;"
                         f" its log is {d / 'follow.log'}")
    _sentinel_set(d, env, ["set", "apply"], "apply", log)
    if end:
        _follow_end(d, env, log)


def _sentinel_set(d, env, args, why, log=None):
    """One setting of a running leg's sentinel, or a stop that says which
    could not be set."""
    p = runner.run(["pgcopydb", "stream", "sentinel"] + args
                   + ["--dir", str(d)], env, check=False, timeout=60,
                   what=f"the change leg's {why}")
    if p.returncode:
        raise SystemExit(f"could not set the {why} on the CDC leg: "
                         + runner.said((p.stderr or "") + (p.stdout or ""))
                         [-200:])
    if log:
        log(f"change stream {why} set")


def stream_cannot_carry(hop, db, tables, columns_left_out=False,
                        ops_alike=False):
    """[(table, why)] for the tables of `db` a change stream that applies
    whole rows under their own names would carry wrongly - every one the
    hop does not exclude and filters, renames, maps the columns of, or
    carries only some changes of (`mapping.ops`).

    Such a stream used to carry them anyway, and the check could not see
    it: `check` applies a row filter to both sides, so every row outside
    it that the stream wrote onto the target went unread. `columns_left_out`
    says the stream can leave a table's columns out (`column.include.list`
    on the pipeline), which answers a kept or dropped column and nothing
    else; `ops_alike` that it can leave the same changes out of every
    table (`skipped.operations`), which answers a hop whose tables all
    carry the same ones."""
    from .config import OPS
    out, alike = [], None
    for ident in tables:
        parts = [p for p in str(ident).split(".") if p]
        if hop.excluded(db, *parts):
            continue
        name = ".".join(parts)
        cols = hop.column_rules(db, *parts)
        carried = hop.carries(db, *parts)
        if alike is None:
            alike = carried
        if hop.row_filter(db, *parts):
            out.append((name, "its rows are filtered"))
        elif _renamed(hop, db, parts):
            out.append((name, "it is renamed on the target"))
        elif cols.get("rename"):
            out.append((name, "its columns are renamed"))
        elif (cols.get("keep") or cols.get("drop")) and not columns_left_out:
            out.append((name, "its columns are kept or dropped"))
        elif carried != frozenset(OPS) and not ops_alike:
            out.append((name, "it carries only " + ", ".join(
                f"{o}s" for o in OPS if o in carried)))
        elif ops_alike and carried != alike:
            out.append((name, "it carries other changes than the hop's"
                              " other tables"))
    return out


def renames_any(hop):
    """Whether a rule of the hop's `mapping.tables` gives a table another
    name: one naming a table by its own name (`orders: orders`, or the
    last parts of it) renames nothing, and a hop with only such rules
    goes every way it went without them."""
    for k, v in ((getattr(hop, "mapping", None) or {}).get("tables")
                 or {}).items():
        to = str(v).split(".")
        if to != str(k).split(".")[-len(to):]:
            return True
    return False


def _renamed(hop, db, parts):
    """Whether the hop gives this table another name on the target: what
    `target_table` answers, held against its own name and each of the
    trailing names it answers to (a mapping of `orders` to `orders`
    renames nothing). As whole names, not split at their dots: a table
    `v1.orders` or a collection `fs.chunks` that no rule names read as
    renamed, its parts counted against the dots of its own name."""
    own = [str(p) for p in (db, *parts) if p not in (None, "")]
    got = str(hop.target_table(db, *parts))
    return all(got != ".".join(own[i:]) for i in range(len(own)))


def cannot_carry_words(refused):
    """The refusal `stream_cannot_carry`'s answer becomes."""
    said = "; ".join(f"{t} ({why})" for t, why in refused[:5])
    return ("this change stream applies whole rows under the source's names"
            f" and cannot carry what the hop asks for {said}"
            + (f" and {len(refused) - 5} more" if len(refused) > 5 else "")
            + ". Nothing has been started. Carried anyway, those rows would"
            " land on the target unchanged, and a check that reads through"
            " the hop's filters and names would not see them")


def _lsn(text):
    """A PostgreSQL position (`0/350411C0`) as a number of bytes."""
    hi, _, lo = str(text or "").partition("/")
    try:
        return (int(hi, 16) << 32) + int(lo, 16)
    except ValueError:
        return None


def follow_lag(hop, db):
    """The CDC leg's reading of itself, for `/metrics`, or None where no
    leg runs here: {"apply": whether it is applying, "held": bytes it has
    received and not applied, by its own account, "behind": bytes of the
    source's log it has not received}.

    Read through the leg's own sentinel with no password anywhere: the
    reader wants the source's address only to match its catalogue, and
    measured, it answers with none in the address. Two things about the
    reading, both measured on 0.18:
    * its JSON gives the start position as the end position (the text
      form said 0/0 for an end nobody had set), so the end is not read
    * its `replay_lsn` is its own claim: it matched `write_lsn` while the
      target held no row - which is why the target's origin, not this, is
      what the move is judged by (`pgcopydb_follow`). `held` is that claim,
      and says so in its metric's help
    """
    import os
    d = follow_dir(hop, db)
    try:
        pid = int((d / "pgcopydb.pid").read_text().split()[0])
        os.kill(pid, 0)
    except (OSError, ValueError, IndexError):
        return None
    try:
        reading = _sentinel(d, {"PGCOPYDB_SOURCE_PGURI":
                                _pg_uri(hop.source, db)}, timeout=15)
    except (OSError, runner.Failed, runner.Blocked):
        return None
    if reading is None:
        return None
    write, replay = _lsn(reading.get("write_lsn")), _lsn(
        reading.get("replay_lsn"))
    out = {"apply": bool(reading.get("apply")), "held": None,
           "behind": None}
    if write is not None and replay:
        out["held"] = max(write - replay, 0)
    try:
        from .engines.postgres import PostgresEngine
        now = _lsn(PostgresEngine(hop).log_position("src", db))
    except Exception:  # noqa: BLE001 - the source's side goes unsaid
        now = None
    if now is not None and write is not None:
        out["behind"] = max(now - write, 0)
    return out


#: the topic the connector puts each transaction's beginning and end on,
#: under its prefix; the sink reads every table's topic and not this one
TRANSACTION_TOPIC = "transaction"


def _stream_facts(hop, dbs, src_is_mysql):
    """(tables by database, their average row in bytes, rows a second a
    copy of this hop went at) for the pipeline's configuration; what
    cannot be read is None."""
    from . import planner
    from .engines.mysql import MySQLEngine
    from .engines.postgres import PostgresEngine
    eng = (MySQLEngine if src_is_mysql else PostgresEngine)(hop)
    tables, rows, size = {}, 0, 0
    # the names only where the hop has something to match against them;
    # the sizes only for as long as a plan waits for anything
    named = bool(_scoped(hop) or hop.mapping)
    for one in dbs or ["postgres"]:
        try:
            tables[one] = eng._all_tables("src", one) if named else []
        except Exception:  # noqa: BLE001 - a source that cannot be listed
            tables[one] = None
        facts = _within(lambda one=one: eng.table_facts("src", one)) or {}
        for name, f in facts.items():
            if not hop.takes_table(one, name):
                continue
            if f.get("rows") and f.get("bytes"):
                rows += int(f["rows"])
                size += int(f["bytes"])
    rate = None
    for path in ("pgcopydb", "pgdump", "mydumper", "builtin"):
        rate = planner.measured_rate(hop, path)
        if rate:
            break
    return tables, (size / rows if rows else None), rate


def _stream_columns_taken(hop, dbs, tables, src_is_mysql):
    """`column.include.list` for a hop that keeps or drops columns, or
    None: as the connector matches them - `schema.table.column` on
    PostgreSQL, `database.table.column` on MySQL - a table it keeps
    columns of takes those by name, one it drops columns of takes every
    column but those, and every other table every column.

    It was the columns left out, worked out once from the source's
    columns of the moment: a column added later to a table the hop keeps
    columns of was not on the list, and the stream carried it. The
    connector takes one of the two lists; the kept ones are still matched
    against the source's, a name it does not have refused."""
    from .engines.mysql import MySQLEngine
    from .engines.postgres import PostgresEngine
    eng = (MySQLEngine if src_is_mysql else PostgresEngine)(hop)
    out, ruled = [], []
    for one in dbs or ["postgres"]:
        catalogue = None
        for ident in tables.get(one) or ():
            parts = [p for p in str(ident).split(".") if p]
            if hop.excluded(one, *parts):
                continue
            rules = hop.column_rules(one, *parts)
            if not (rules.get("keep") or rules.get("drop")):
                continue
            table = _regex_name(".".join([one] + parts if src_is_mysql
                                         else parts))
            drop = list(rules.get("drop") or [])
            if rules.get("keep"):
                if catalogue is None:
                    catalogue = eng.column_catalog("src", one)
                cols = [c for c, _ in catalogue.get(ident) or []]
                if not cols:
                    raise SystemExit(
                        f"{ident}: the columns the hop keeps could not be"
                        " matched against the source's, so the stream"
                        " cannot be told which to take. Nothing has been"
                        " started")
                keep = [c for c in rules["keep"] if c not in drop]
                out.append(f"{table}\\.(?:"
                           + "|".join(_regex_name(c) for c in keep) + ")")
            else:
                out.append(f"{table}\\.(?!(?:"
                           + "|".join(_regex_name(c) for c in drop)
                           + ")$)[^.]+")
            ruled.append(table)
    if not out:
        return None
    out.append("(?!(?:" + "|".join(ruled) + ")\\.[^.]+$).+")
    return ",".join(out)


def _regex_name(name):
    """A name as a regular expression the connector reads that matches it
    and nothing else: every character but a letter, a digit and `_`
    escaped. A comma ends an entry of the connector's lists, so a name
    holding one is refused."""
    if "," in str(name):
        raise SystemExit(f"{name}: a name holding a comma cannot be put on"
                         f" {PIPELINE_SAID}'s lists, which a comma divides."
                         " Nothing has been started")
    return "".join(ch if ch.isalnum() or ch == "_" else "\\" + ch
                   for ch in str(name))


def _shell_regex(pattern):
    """A hop's shell pattern (`config.name_matches`) as a regular
    expression the connector reads: `*` any run, `?` one character, a
    `[...]` class as itself, the rest as it is (`_regex_name`)."""
    from .config import _shell_parts
    out = []
    for part in _shell_parts(pattern):
        if part == "*":
            out.append(".*")
        elif part == "?":
            out.append(".")
        elif len(part) > 1:
            body = part[1:-1]
            if body.startswith("!"):
                body = "^" + body[1:]
            out.append("[" + body.replace("\\", "\\\\") + "]")
        else:
            out.append(_regex_name(part))
    return "".join(out)


def _connector_takes(hop, db, patterns, mysql, include=False):
    """A regular expression over the connector's name of a table -
    `database.table` on MySQL, `schema.table` on PostgreSQL, whose
    connector reads one database - for the tables of `db` that `patterns`
    name as the hop reads them: right-anchored, so a pattern names a table
    by its whole name or any end of it (`Hop.excluded`), and an `include`
    pattern by its schema's or its database's too (`Hop.included`); one
    written with the database is read after it (`config.pattern_after`).
    None where they name none; `.*` where one names the whole database."""
    from .config import has_wildcard, name_matches, pattern_after

    def fits(r, dots):
        # whether a pattern can name a name of that many dots
        return has_wildcard(r) or r.count(".") == dots
    alts = []
    for p in patterns:
        p = str(p)
        if name_matches(db, p):
            alts = [".*"]
            break
        after = [r for r in pattern_after(p, f"{db}.") if r]
        if mysql:
            # `table`, named with its database or without
            alts += [_shell_regex(r) for r in after + [p] if fits(r, 0)]
            continue
        if include:
            # its schema, named with the database or without
            alts += [f"{_shell_regex(r)}\\..+" for r in after + [p]
                     if fits(r, 0)]
        # `schema.table`, with the database or without; `table`
        alts += [_shell_regex(r) for r in after + [p] if fits(r, 1)]
        if fits(p, 0):
            alts.append(f"[^.]+\\.{_shell_regex(p)}")
    if not alts:
        return None
    alts = list(dict.fromkeys(alts))
    body = alts[0] if len(alts) == 1 else "(?:" + "|".join(alts) + ")"
    return f"{_regex_name(db)}\\.{body}" if mysql else body


def debezium_tables(hop, dbs, tables, mysql):
    """The connector's table list for a hop that leaves anything out, by
    the hop's own patterns, in every database it reads: where the hop
    says what it takes, `table.include.list` of those, with what it
    leaves out as a lookahead before each - the connector takes one of
    the two lists; else `table.exclude.list` of what it leaves out. Both
    hold, beside the patterns, the tables `hop.excluded` leaves out by a
    rule of its own now (another tool's bookkeeping, migkit's own:
    `debezium_exclude`) - by name, which is what they are known by.

    It was the excluded tables of the first database alone, read once:
    the second database's were streamed, and a table made later outside
    what the hop takes, or under a pattern it leaves out, was streamed
    and made on the target by the sink."""
    out = []
    taking = []
    for one in dbs:
        listed = tables.get(one)
        gone = _connector_takes(hop, one, hop.exclude or [], mysql)
        take = (_connector_takes(hop, one, hop.include, mysql, include=True)
                if hop.include else ".*")
        if take is None:
            # a database the hop takes nothing of
            continue
        named = []
        if listed is not None:
            got = debezium_exclude(hop, one, listed,
                                   one if mysql else "public")
            # by name only those the patterns do not already leave out,
            # and only among what they take
            for n in got.split(",") if got else []:
                plain = n.replace("\\.", ".")
                if (gone and re.fullmatch(gone, plain)) or \
                        not re.fullmatch(take, plain):
                    continue
                named.append(n)
        leave = ([gone] if gone else []) + named
        if not hop.include:
            out += leave
            continue
        taking.append((f"(?!(?:{'|'.join(leave)})$)" if leave else "")
                      + take)
    if hop.include:
        # a database it takes nothing of is left out whole
        return {"table.include.list": ",".join(taking) or "(?!)"}
    return {"table.exclude.list": ",".join(out)} if out else {}


def _skipped_operations(hop, tables):
    """`skipped.operations`: the changes the hop's `mapping.ops` leaves
    out, where every table it carries leaves out the same
    (`stream_cannot_carry` refuses a hop where not) - a delete left out
    leaves a truncate out too, which takes away what a delete would -
    else none."""
    from .config import OPS
    seen = {hop.carries(one, *hop.parts_of(t))
            for one, listed in tables.items() for t in listed or ()
            if hop.takes_table(one, t)}
    if len(seen) != 1:
        return "none"
    carried = next(iter(seen))
    out = [w for o, w in zip(OPS, "cud") if o not in carried]
    if "delete" not in carried:
        out.append("t")
    return ",".join(out) or "none"


def value_modes(mysql):
    """How the connector hands each kind of value on, chosen so nothing
    reaches the target other than the source held it - the forms `canon`
    compares, where every connector has a lossy answer beside the exact
    one:
    * times: to the microsecond, as each column holds them (`adaptive`,
      and on MySQL `adaptive_time_microseconds`, its own default) - never
      `connect`, which keeps milliseconds and cuts the rest. `canon`
      renders a time to the microsecond and past it where a value carries
      more (`FRACTION`)
    * decimals: `precise`, the exact value - `double` rounds past fifteen
      digits, and `string` asks the sink to write text into a number.
      `canon` compares a decimal by its value (`NUMBER`)
    * bytes: `bytes`, not base64 or hex text for the sink to decode
    * MySQL's unsigned 64-bit integers: `precise` - the default `long`
      holds up to 2^63 - 1, and a larger value wraps negative; `canon`
      holds a `bigint unsigned` to 0..2^64 - 1
    * MySQL's dates: `enable.time.adjuster` off. On, the connector reads a
      year below 100 as a two-digit year and moves it into 1970-2069 (its
      documented rule); MySQL stores a four-digit year, and the value as
      stored is what `canon` compares
    * PostgreSQL's intervals: `string`, the ISO 8601 text - the default
      counts a month as 30 days, and `1 mon` is not `30 days` on the
      target. `canon` compares an interval by the engine's own text
      (`OWN`); how the sink writes that text into an interval column is not
      measured here, and `check` compares what lands
    Pinned, not left to defaults, so a release that changes one does not
    change what lands."""
    out = {"decimal.handling.mode": "precise",
           "binary.handling.mode": "bytes"}
    if mysql:
        out.update({"time.precision.mode": "adaptive_time_microseconds",
                    "bigint.unsigned.handling.mode": "precise",
                    "enable.time.adjuster": "false"})
    else:
        out.update({"time.precision.mode": "adaptive",
                    "interval.handling.mode": "string"})
    return out


def stream_codegen(hop, dbs, engine):
    """Write the managed streaming pipeline for this hop: a single-broker
    log plus a connector runtime with a source and an upsert sink, sized for
    one hop and ready to start. migkit reimplements none of it - it wires
    proven components together and owns the lifecycle, so the operator drives
    everything through migkit.

    What the connector is asked beyond its defaults, and why - the full
    account, option by option, is `DEBEZIUM` below (`tooluse`):
    * transaction boundaries on every change and a topic of their own, so
      a reader of the log can keep a source transaction whole; the sink
      reads every table's topic and not that one
    * truncates carried: its default leaves a source `TRUNCATE` out, and the
      target kept every row of a table the source had emptied
    * its buffers from the rows it carries (`sizing.stream_buffers`),
      bounded in bytes as well as in changes; the sink writes a batch as
      large as the one it is handed
    * a failure stops it (`fail`), pinned: the other answers skip or warn,
      and a change skipped is a row the target never gets
    * decimals, times and bytes in the forms `canon` compares - exact
      decimals, times to the microsecond, bytes as bytes - pinned, so a
      later default does not change what lands
    * a heartbeat as often as the runtime keeps its place (60 s), so a slot
      whose tables are quiet still lets the source's log go
    * snapshot progress on a notification topic, read back in migkit's
      words (`stream_progress`)
    * the tables by the hop's own patterns (`debezium_tables`): what it
      takes on the include list, what it leaves out as a lookahead before
      it or on the exclude list - so a table made later is taken or left
      by the rule that names it, in every database of the hop
    * the columns the hop keeps taken by name (`column.include.list`), so
      a column added later to a table it keeps columns of stays out; a
      table the hop filters or renames is refused (`stream_cannot_carry`)
      - the pipeline would carry it whole under its own name
    * the changes the hop does not carry left out where every table
      carries the same (`skipped.operations`), and refused where not
    * one database, written into the hop's name for it on the target: the
      sink writes into the one its address names, and every database's
      tables went into the first's
    """
    from . import sizing
    if len(dbs or ()) > 1:
        raise SystemExit(
            f"the hop moves {len(dbs)} databases ({', '.join(dbs[:4])}), and"
            f" {PIPELINE_SAID} writes into the one database its sink is"
            " given: every database's tables would land in the first's."
            " Nothing has been started - follow one database a hop, or"
            " with migkit's own change tail")
    out = hop.report_dir() / "stream"
    out.mkdir(parents=True, exist_ok=True)
    s, t = hop.source, hop.target
    src_is_mysql = engine in ("mysql", "hetero")
    connector_class = ("io.debezium.connector.mysql.MySqlConnector"
                       if src_is_mysql
                       else "io.debezium.connector.postgresql.PostgresConnector")
    name = f"migkit-{hop.name}"
    tables, row_bytes, rate = _stream_facts(hop, dbs, src_is_mysql)
    refused = []
    for one, listed in tables.items():
        if listed is None and (hop.mapping or {}):
            raise SystemExit(
                f"the source's tables in {one} could not be listed, so what"
                " the hop maps cannot be matched against them. Nothing has"
                " been started")
        refused += stream_cannot_carry(hop, one, listed or (),
                                       columns_left_out=True,
                                       ops_alike=True)
    if refused:
        raise SystemExit(cannot_carry_words(refused))
    buffers = sizing.stream_buffers(row_bytes, rate)
    source = {
        "name": f"{name}-source",
        "config": {
            "connector.class": connector_class,
            "database.hostname": s.host,
            "database.port": str(s.port),
            "database.user": s.user,
            # the password from the runtime's environment (`connect.env`),
            # never in the connector's configuration, which its API hands
            # anyone who asks and its configuration topic keeps
            "database.password": "${env:MIGKIT_SOURCE_PASSWORD}",
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
            "notification.enabled.channels": "sink",
            "notification.sink.topic.name": f"{name}-notifications",
            "provide.transaction.metadata": "true",
            "topic.transaction": TRANSACTION_TOPIC,
            "skipped.operations": _skipped_operations(hop, tables),
            "event.processing.failure.handling.mode": "fail",
            "heartbeat.interval.ms": "60000",
            **value_modes(src_is_mysql),
            **buffers,
        },
    }
    if src_is_mysql:
        source["config"].update({
            "database.include.list": ",".join(dbs),
            "database.server.id": "184054",
            "schema.history.internal.kafka.bootstrap.servers":
                "redpanda:9092",
            "schema.history.internal.kafka.topic": f"{name}-history",
            "snapshot.locking.mode": "minimal",
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
        if _pg_snapshot_watermarks(hop, dbs[0] if dbs else "postgres"):
            # the same, from the source's own snapshot of transactions in
            # flight rather than from rows written into it
            source["config"]["read.only"] = "true"
    # what the hop takes and leaves out, by its own patterns: the same
    # reading `check` makes (`Hop.included`, `Hop.excluded`), so a table
    # nobody wants is not carried by the CDC leg either - one made later
    # included
    if _scoped(hop):
        source["config"].update(debezium_tables(hop, dbs or ["postgres"],
                                                tables, src_is_mysql))
    taken = _stream_columns_taken(hop, dbs, tables, src_is_mysql)
    if taken:
        source["config"]["column.include.list"] = taken
    # the hop's TLS, as each connector takes it; the files it names are
    # given to the runtime read-only, at the same paths
    tls_src, files = _debezium_tls(s, src_is_mysql)
    source["config"].update(tls_src)

    dst_is_pg = engine in ("postgres", "hetero")
    jdbc = (f"jdbc:postgresql://{t.host}:{t.port}/" if dst_is_pg
            else f"jdbc:mysql://{t.host}:{t.port}/")
    query, more = _jdbc_tls(t, dst_is_pg)
    files += more
    sink = {
        "name": f"{name}-sink",
        "config": {
            "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
            # every table's topic, and not the transactions' one beside them
            "topics.regex": f"{name}\\.(?!{TRANSACTION_TOPIC}$).*",
            # the database the hop maps the source's to on the target
            "connection.url": jdbc + (hop.target_db(dbs[0]) if dbs else "")
            + query,
            "connection.username": t.user,
            "connection.password": "${env:MIGKIT_TARGET_PASSWORD}",
            "insert.mode": "upsert",
            "delete.enabled": "true",
            "truncate.enabled": "true",
            "primary.key.mode": "record_key",
            "schema.evolution": "basic",
            "batch.size": buffers["max.batch.size"],
        },
    }
    compose = f"""services:
  redpanda:
    image: {IMAGES["redpanda"]}
    command: redpanda start --overprovisioned --smp 1 --memory 1G
      --node-id 0 --kafka-addr PLAINTEXT://0.0.0.0:9092
      --advertise-kafka-addr PLAINTEXT://redpanda:9092
  connect:
    image: {IMAGES["debezium"]}
    depends_on: [redpanda]
    ports: ["127.0.0.1:8083:8083", "127.0.0.1:8778:8778"]
    environment:
      BOOTSTRAP_SERVERS: redpanda:9092
      GROUP_ID: migkit
      CONFIG_STORAGE_TOPIC: migkit_connect_configs
      OFFSET_STORAGE_TOPIC: migkit_connect_offsets
      STATUS_STORAGE_TOPIC: migkit_connect_statuses
      # its readings over HTTP on this machine only, which `/metrics`
      # carries out under migkit's names (`stream_readings`)
      ENABLE_JOLOKIA: "true"
      # the connectors' passwords read from this environment
      CONNECT_CONFIG_PROVIDERS: env
      CONNECT_CONFIG_PROVIDERS_ENV_CLASS: {ENV_PROVIDER}
    env_file: [connect.env]
""" + ("    volumes:\n" + "".join(f"      - {json.dumps(f + ':' + f + ':ro')}\n"
                                   for f in sorted(set(files)))
       if files else "")
    readme = f"""# Streaming pipeline for hop {hop.name} (generated by migkit)

migkit owns this stack. Drive it with migkit, not by hand:

  start      migkit move {hop.name} --mode cdc --go
  progress   migkit watch {hop.name}
  verify     migkit check {hop.name}
             migkit watch {hop.name} --verify --delta
  tear down  migkit move {hop.name} --mode cdc --drop --go

connect.env holds the databases' passwords from hops.yaml; the connectors
read them from there and carry none. Keep this directory private (chmod
700, never commit). Tearing down also drops the replication slot /
binlog reader left on the source.

Third-party components are pinned in docker-compose.yml; see NOTICE for
licences and attribution.
"""
    import json as _json
    (out / "docker-compose.yml").write_text(compose)
    _write_private(out / "connect.env", _env_file({
        "MIGKIT_SOURCE_PASSWORD": s.password or "",
        "MIGKIT_TARGET_PASSWORD": t.password or ""}))
    (out / "source-connector.json").write_text(
        _json.dumps(source, indent=2) + "\n")
    (out / "sink-connector.json").write_text(
        _json.dumps(sink, indent=2) + "\n")
    (out / "README.md").write_text(readme)
    for f in out.iterdir():
        f.chmod(0o600)
    out.chmod(0o700)
    return out


#: the connector runtime's provider that reads `${env:NAME}` from its own
#: environment (Kafka 3.5 and later; the runtime's image is 3.0 on Kafka
#: 3.8), which reads it off `connect.env`
ENV_PROVIDER = "org.apache.kafka.common.config.provider.EnvVarConfigProvider"


def _debezium_tls(ep, mysql):
    """({field: value}, [file]): the side's TLS as the source connector
    takes it - PostgreSQL's `database.sslmode` and its files, the ones
    `Endpoint.libpq_tls` decides for migkit's own connections; MySQL's
    `database.ssl.mode` from `Endpoint.mysql_tls`. Both with the automatic
    answer: the pipeline connected with no TLS at all while `doctor` said
    verified. MySQL's connector checks a certificate against Java's own
    store of authorities, which a hop's own authority file is not."""
    if not mysql:
        got = ep.libpq_tls()
        out = {f"database.{k}": v for k, v in got.items()
               if k in ("sslmode", "sslrootcert", "sslcert", "sslkey")}
        return out, [v for k, v in got.items()
                     if k in ("sslrootcert", "sslcert", "sslkey")]
    got = ep.mysql_tls() if hasattr(ep, "mysql_tls") else {}
    if got.get("ssl_ca"):
        return {"database.ssl.mode": "verify_identity"
                if got.get("ssl_verify_identity", True)
                else "verify_ca"}, []
    if got.get("ssl") is not None:
        return {"database.ssl.mode": "required"}, []
    return {}, []


def _jdbc_tls(ep, pg):
    """(the query of the sink's JDBC address, [file]): the target's TLS as
    each driver reads it in the address - PostgreSQL's `sslmode` and its
    files, MySQL's `sslMode`."""
    from urllib.parse import urlencode
    if pg:
        got = {k: v for k, v in ep.libpq_tls().items()
               if k in ("sslmode", "sslrootcert", "sslcert", "sslkey")}
        return ("?" + urlencode(got) if got else "",
                [v for k, v in got.items() if k != "sslmode"])
    got = ep.mysql_tls() if hasattr(ep, "mysql_tls") else {}
    if got.get("ssl_ca"):
        return ("?sslMode=" + ("VERIFY_IDENTITY"
                               if got.get("ssl_verify_identity", True)
                               else "VERIFY_CA"), [])
    if got.get("ssl") is not None:
        return "?sslMode=REQUIRED", []
    return "", []


def _env_file(values):
    """An environment file's lines, each value taken as it is written: in
    single quotes, or in double quotes with what they read escaped where
    the value holds a single quote."""
    out = []
    for k, v in values.items():
        v = str(v)
        if "'" not in v:
            out.append(f"{k}='{v}'")
        else:
            v = v.replace("\\", "\\\\").replace('"', '\\"').replace(
                "$", "$$")
            out.append(f'{k}="{v}"')
    return "\n".join(out) + "\n"


def _write_private(path, text):
    """`text` as the file `path`, readable by this user only from the
    moment it exists."""
    import os
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    if hasattr(os, "fchmod"):
        os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


#: the connectors the pipeline runs, as its option account names them
DEBEZIUM_COMMANDS = ("postgres", "mysql", "jdbc-sink", "signals")
#: what every connector takes from its runtime as well as its own
_CONNECT = ("transforms", "predicates")
#: the signals a connector reads on its channel
_SIGNALS = ("execute-snapshot", "pause-snapshot", "resume-snapshot",
            "stop-snapshot", "log")

_RELATIONAL = (
    "connector.class", "tasks.max", "topic.prefix", "database.hostname",
    "database.port", "database.user", "database.password",
    "table.include.list", "table.exclude.list", "column.include.list",
    "column.exclude.list", "skip.messages.without.change",
    "column.truncate.to.", "column.mask.with.", "column.mask.hash.",
    "column.propagate.source.type", "datatype.propagate.source.type",
    "message.key.columns", "time.precision.mode", "decimal.handling.mode",
    "binary.handling.mode", "tombstones.on.delete",
    "schema.name.adjustment.mode", "field.name.adjustment.mode",
    "converters", "snapshot.mode", "snapshot.include.collection.list",
    "snapshot.select.statement.overrides", "snapshot.delay.ms",
    "snapshot.fetch.size", "snapshot.max.threads", "snapshot.lock.timeout.ms",
    "snapshot.locking.mode", "snapshot.query.mode",
    "snapshot.tables.order.by.row.count",
    "event.processing.failure.handling.mode", "max.batch.size",
    "max.queue.size", "max.queue.size.in.bytes", "poll.interval.ms",
    "heartbeat.interval.ms", "heartbeat.action.query",
    "provide.transaction.metadata", "skipped.operations",
    "signal.data.collection", "signal.enabled.channels", "signal.kafka.",
    "notification.enabled.channels", "notification.sink.topic.name",
    "incremental.snapshot.chunk.size",
    "incremental.snapshot.watermarking.strategy", "topic.naming.strategy",
    "topic.delimiter", "topic.cache.size", "topic.heartbeat.prefix",
    "topic.transaction", "custom.metric.tags", "errors.max.retries",
    "database.query.timeout.ms", "read.only", "post.processors",
    "retriable.restart.connector.wait.ms", "database.initial.statements")
_POSTGRES = (
    "plugin.name", "slot.name", "slot.drop.on.stop", "slot.stream.params",
    "slot.max.retries", "slot.retry.delay.ms", "publication.name",
    "publication.autocreate.mode", "replica.identity.autoset.values",
    "database.dbname", "database.sslmode", "database.sslcert",
    "database.sslpassword", "database.sslkey", "database.sslrootcert",
    "database.sslfactory", "database.tcpKeepAlive", "schema.include.list",
    "schema.exclude.list", "hstore.handling.mode", "interval.handling.mode",
    "money.fraction.digits", "message.prefix.include.list",
    "message.prefix.exclude.list", "status.update.interval.ms",
    "xmin.fetch.interval.ms", "flush.lsn.source",
    "unavailable.value.placeholder", "include.unknown.datatypes")
_MYSQL = (
    "database.server.id", "database.include.list", "database.exclude.list",
    "database.ssl.mode", "database.ssl.keystore",
    "database.ssl.keystore.password", "database.ssl.truststore",
    "database.ssl.truststore.password", "include.schema.changes",
    "include.schema.comments", "include.query",
    "bigint.unsigned.handling.mode",
    "event.deserialization.failure.handling.mode",
    "inconsistent.schema.handling.mode", "enable.time.adjuster",
    "gtid.source.includes", "gtid.source.excludes", "connect.timeout.ms",
    "connect.keep.alive", "connect.keep.alive.interval.ms",
    "table.ignore.builtin", "binlog.buffer.size",
    "min.row.count.to.stream.results", "database.jdbc.driver",
    "use.nongraceful.disconnect", "schema.history.internal.")
_SINK = (
    "connector.class", "tasks.max", "topics", "topics.regex",
    "connection.url", "connection.username", "connection.password",
    "connection.pool.min_size", "connection.pool.max_size",
    "connection.pool.acquire_increment", "connection.pool.timeout",
    "insert.mode", "delete.enabled", "truncate.enabled", "primary.key.mode",
    "primary.key.fields", "schema.evolution", "quote.identifiers",
    "collection.name.format", "collection.naming.strategy",
    "column.naming.strategy", "database.time_zone", "use.time.zone",
    "batch.size", "use.reduction.buffer", "field.include.list",
    "field.exclude.list", "flush.max.retries", "flush.retry.delay.ms",
    "dialect.postgres.postgis.schema", "dialect.sqlserver.identity.insert",
    "hibernate.")
_DEBEZIUM_FAMILIES = ("column.truncate.to.", "column.mask.with.",
                      "column.mask.hash.", "signal.kafka.",
                      "schema.history.internal.", "hibernate.")

_DEFAULT = "left at its default, which is the right answer here:"
_MASK = ("the target is to hold the source's values: masking, hashing or"
         " cutting one short at the source puts a different value in the"
         " target, which the check then reports. The hop's `mask` is what a"
         " person reads in a report, not what the target holds")


def _debezium_account():
    o = {}

    def put(commands, option, state, why):
        for c in commands:
            o[(c, option)] = (state, why)
    both, pg, my, sink = ("postgres", "mysql"), ("postgres",), ("mysql",), (
        "jdbc-sink",)
    for f in ("connector.class", "database.hostname", "database.port",
              "database.user", "database.password", "topic.prefix"):
        put(both, f, USED, "where and as whom the connector reads the source,"
            " and the prefix the sink and the signals find its topics by")
    put(both, "tasks.max", UNUSED, _DEFAULT + " a source connector reads one"
        " change log in one task whatever this says")
    put(both, "table.exclude.list", USED, "a hop that says only what it"
        " leaves out: its patterns as anchored expressions"
        " (`debezium_tables`), so a table made later under one stays out,"
        " and the tables another rule of `hop.excluded` leaves out now by"
        " name")
    put(both, "table.include.list", USED, "a hop that says what it takes:"
        " its include patterns, in every database it reads, with what it"
        " leaves out as a lookahead before them - the connector takes one"
        " of the two lists, and the two are not the same list (a table made"
        " later outside what the hop takes was streamed)")
    put(both, "column.exclude.list", UNUSED, "the connector takes one of"
        " the two lists, and a list of what is left out, read once, let a"
        " column added later to a table the hop keeps columns of through;"
        " the include list says it by name")
    put(both, "column.include.list", USED, "the columns the hop carries:"
        " the ones it keeps by name, every one but those it drops, every"
        " column of the other tables (`_stream_columns_taken`)")
    put(both, "skip.messages.without.change", UNUSED, "a change to a column"
        " the hop keeps out still moves nothing on the target, applied or"
        " not; left at its default")
    for f in ("column.truncate.to.", "column.mask.with.",
              "column.mask.hash."):
        put(both, f, UNUSED, _MASK)
    for f in ("column.propagate.source.type",
              "datatype.propagate.source.type"):
        put(both, f, UNUSED, "the sink builds a table the target lacks from"
            " the change's own schema; migkit makes the target's tables"
            " before the stream starts, from the source's definition")
    put(both, "message.key.columns", UNUSED, "a change is addressed by the"
        " table's primary key, which is what the sink upserts by; a table"
        " without one is refused by migkit's own paths too")
    for f in ("time.precision.mode", "decimal.handling.mode",
              "binary.handling.mode"):
        put(both, f, USED, "the exact form of each value, as `canon`"
            " compares it, never a lossy one (`value_modes`): times to the"
            " microsecond (never `connect`, which keeps milliseconds),"
            " decimals exact, bytes as bytes")
    put(both, "tombstones.on.delete", UNUSED, _DEFAULT + " the sink applies"
        " a delete from the delete itself, and the tombstone lets the log"
        " compact what a deleted key held")
    for f in ("schema.name.adjustment.mode", "field.name.adjustment.mode"):
        put(both, f, UNUSED, _DEFAULT + " names are kept as the source has"
            " them")
    put(both, "converters", UNUSED, "no value is converted on the way; a"
        " type the connector cannot carry is a difference `check` reports")
    put(both, "snapshot.mode", USED, "`initial`: the rows first, then every"
        " change from the position the read began at")
    put(both, "snapshot.include.collection.list", UNUSED, "every table the"
        " stream carries is read first; the exclude list already leaves the"
        " rest out of both")
    put(both, "snapshot.select.statement.overrides", UNUSED, "a row filter"
        " at the first read only would let every later change of a row"
        " outside it through the stream, and a check that reads through the"
        " filter would never see those rows; a hop that filters rows is"
        " refused by the pipeline instead (`stream_cannot_carry`)")
    for f in ("snapshot.delay.ms", "snapshot.fetch.size",
              "snapshot.max.threads", "snapshot.lock.timeout.ms",
              "snapshot.query.mode", "snapshot.tables.order.by.row.count"):
        put(both, f, UNUSED, "not yet: the first read of the pipeline runs"
            " as the connector reads by default, and migkit's own copy is"
            " the path sized per table")
    put(my, "snapshot.locking.mode", USED, "pinned to `minimal`: `none` is"
        " safe only where no schema change runs during the first read, and"
        " nothing watches the source's schema while the connector reads it")
    put(pg, "snapshot.locking.mode", UNUSED, _DEFAULT + " the first read"
        " takes locks that stop a schema change, never a write")
    put(both, "event.processing.failure.handling.mode", USED, "pinned to"
        " `fail`: the other answers warn or skip, and a change skipped is a"
        " row the target never gets")
    for f in ("max.batch.size", "max.queue.size", "max.queue.size.in.bytes",
              "poll.interval.ms"):
        put(both, f, USED, "from the rows it carries (`sizing.stream_"
            "buffers`): a batch a sixteenth of a worker's memory at the"
            " source's average row, the queue four of them and bounded in"
            " bytes, the wait for a batch what one takes to fill at the rate"
            " measured")
    put(both, "heartbeat.interval.ms", USED, "as often as the runtime keeps"
        " its place (60 s): a slot whose tables are quiet still lets the"
        " source's log go")
    put(both, "heartbeat.action.query", UNUSED, "it writes to the source,"
        " and migkit writes to a source nowhere")
    put(both, "provide.transaction.metadata", USED, "each change carries its"
        " transaction and each transaction's end is on a topic of its own,"
        " so a reader of the log can keep one whole")
    put(both, "topic.transaction", USED, "named, so the sink's topic pattern"
        " can leave that topic out")
    put(both, "skipped.operations", USED, "`none`: its default leaves a"
        " source `TRUNCATE` out, and the target kept every row the source"
        " had emptied. Where every table the hop carries leaves the same"
        " changes out (`mapping.ops`), those - and a truncate with a delete"
        " (`_skipped_operations`); a hop whose tables leave out different"
        " ones is refused (`stream_cannot_carry`)")
    put(both, "signal.enabled.channels", USED, "the log's own topic, so a"
        " table can be read again without a signalling table in the source")
    put(both, "signal.kafka.", USED, "the signal topic, the broker, and the"
        " group the connector reads its signals as")
    put(both, "signal.data.collection", UNUSED, "it names a table in the"
        " source to write signals into, and migkit writes to a source"
        " nowhere")
    put(both, "notification.enabled.channels", USED, "the log's own topic,"
        " read back as the reads' progress in migkit's words"
        " (`stream_progress`)")
    put(both, "notification.sink.topic.name", USED, "a topic the sink's"
        " pattern does not match")
    put(both, "incremental.snapshot.chunk.size", UNUSED, "not yet: a re-read"
        " beside the stream is held while the source is under strain"
        " (`stream_hold`), and its chunk left at the connector's size")
    put(both, "incremental.snapshot.watermarking.strategy", UNUSED, "the"
        " watermarks come from the source's own positions (`read.only`),"
        " never from rows written into it")
    put(both, "read.only", USED, "where the source can give a re-read its"
        " watermarks without a write - GTID on MySQL, `pg_current_snapshot`"
        " on PostgreSQL 13 and later (measured: not in the PostgreSQL"
        " connector's advertised options, and it works)")
    for f in ("topic.naming.strategy", "topic.delimiter", "topic.cache.size",
              "topic.heartbeat.prefix"):
        put(both, f, UNUSED, _DEFAULT + " the sink finds a table's topic by"
            " the prefix and the default dot")
    put(both, "custom.metric.tags", UNUSED, "the pipeline's progress is read"
        " by migkit from its notifications and its sink, not scraped")
    put(both, "errors.max.retries", UNUSED, _DEFAULT + " a connector that"
        " fails stops, and `stream_status` says so")
    put(both, "database.query.timeout.ms", UNUSED, _DEFAULT + " the"
        " connector's own queries are catalogue reads")
    put(both, "post.processors", UNUSED, "no change is rewritten on its way;"
        " what the source wrote is what the target gets")
    put(both, "retriable.restart.connector.wait.ms", UNUSED, _DEFAULT +
        " the runtime restarts it after a connection it can retry")
    put(both, "database.initial.statements", UNUSED, "a session setting on"
        " the source is a change to how the source runs, which migkit does"
        " not make")
    put(pg, "plugin.name", USED, "pgoutput, which every stock server has")
    put(pg, "slot.name", USED, "named after the hop, so teardown can name it"
        " and drop it")
    put(pg, "slot.drop.on.stop", UNUSED, "teardown drops the slot when the"
        " stream is done with; dropped on every stop, a restart would lose"
        " the changes made while it was down")
    for f in ("slot.stream.params", "slot.max.retries", "slot.retry.delay.ms",
              "status.update.interval.ms", "xmin.fetch.interval.ms",
              "flush.lsn.source"):
        put(pg, f, UNUSED, _DEFAULT + " the slot is read and confirmed the"
            " way the connector reads it by default")
    for f in ("publication.name", "publication.autocreate.mode"):
        put(pg, f, UNUSED, _DEFAULT + " the connector names and makes its"
            " publication, which teardown drops with the slot")
    put(pg, "replica.identity.autoset.values", UNUSED, "it changes a table's"
        " replica identity on the source, which migkit does not change")
    put(pg, "database.dbname", USED, "the hop's first database")
    for f in ("database.sslmode", "database.sslcert", "database.sslkey",
              "database.sslrootcert"):
        put(pg, f, USED, "the hop's TLS as migkit's own connections take"
            " it, the automatic answer included (`_debezium_tls`); the"
            " files it names given to the runtime read-only")
    put(pg, "database.sslpassword", UNUSED, "a client key is read as the"
        " hop gives it, unprotected, as migkit's own connections read it")
    put(pg, "database.sslfactory", UNUSED, _DEFAULT + " the driver's own"
        " checks, which the mode and the authority file steer")
    put(pg, "database.tcpKeepAlive", UNUSED, "not yet: the connector's"
        " heartbeat keeps the connection in use")
    for f in ("schema.include.list", "schema.exclude.list"):
        put(pg, f, UNUSED, "the hop's exclusions reach it table by table on"
            " the table exclude list")
    put(pg, "hstore.handling.mode", UNUSED, _DEFAULT + " a map, as the sink"
        " writes one back")
    put(pg, "interval.handling.mode", USED, "`string`, the ISO 8601 text:"
        " the default counts a month as 30 days, and a month is not 30 days"
        " on the target (`value_modes`)")
    put(pg, "money.fraction.digits", UNUSED, _DEFAULT + " two digits, as"
        " the money type keeps")
    for f in ("message.prefix.include.list", "message.prefix.exclude.list"):
        put(pg, f, UNUSED, "logical decoding messages are not rows; the sink"
            " has nothing to apply from them")
    put(pg, "unavailable.value.placeholder", UNUSED, _DEFAULT + " a value"
        " the change does not carry is marked, and the sink leaves the"
        " target's value as it is")
    put(pg, "include.unknown.datatypes", UNUSED, "a type the connector does"
        " not know stays out, and `check` reports the column as different")
    put(my, "database.server.id", USED, "one id for the pipeline's reader,"
        " apart from migkit's own reader's")
    put(my, "database.include.list", USED, "the databases the hop moves,"
        " and no other")
    put(my, "database.exclude.list", UNUSED, "the hop names its databases;"
        " the include list says the same")
    put(my, "database.ssl.mode", USED, "the hop's TLS as migkit's own"
        " connections take it, the automatic answer included"
        " (`_debezium_tls`)")
    for f in ("database.ssl.keystore", "database.ssl.keystore.password",
              "database.ssl.truststore", "database.ssl.truststore.password"):
        put(my, f, UNUSED, "not yet: the connector checks a certificate"
            " against its runtime's own authorities, and the hop names an"
            " authority as a file of another form")
    for f in ("database.jdbc.driver", "connect.timeout.ms",
              "connect.keep.alive", "connect.keep.alive.interval.ms",
              "use.nongraceful.disconnect"):
        put(my, f, UNUSED, "not yet: the pipeline connects the way the"
            " connector does by default")
    for f in ("include.schema.changes", "include.schema.comments",
              "include.query"):
        put(my, f, UNUSED, _DEFAULT + " schema changes go on the prefix's"
            " own topic, which the sink's pattern does not match")
    put(my, "bigint.unsigned.handling.mode", USED, "`precise`: the default"
        " holds up to 2^63 - 1, and a larger unsigned value wraps negative"
        " (`value_modes`)")
    put(my, "event.deserialization.failure.handling.mode", UNUSED, _DEFAULT
        + " `fail`: a change that cannot be read stops the stream")
    put(my, "inconsistent.schema.handling.mode", UNUSED, _DEFAULT + " `fail`:"
        " a change to a table the history does not know stops the stream")
    put(my, "enable.time.adjuster", USED, "off: on, a year below 100 is"
        " read as a two-digit year and moved into 1970-2069, where MySQL"
        " stores the year as written (`value_modes`)")
    for f in ("gtid.source.includes", "gtid.source.excludes"):
        put(my, f, UNUSED, "every source server's transactions are read; the"
            " hop's databases are what is filtered")
    put(my, "table.ignore.builtin", UNUSED, _DEFAULT + " the server's own"
        " tables are left out")
    put(my, "binlog.buffer.size", UNUSED, "not yet: its read-ahead buffer"
        " is not measured here")
    put(my, "min.row.count.to.stream.results", UNUSED, _DEFAULT + " a large"
        " table is streamed rather than held whole")
    put(my, "schema.history.internal.", USED, "the history of the source's"
        " schema, on the pipeline's own broker")
    put(sink, "connector.class", USED, "the sink that applies each change by"
        " key")
    put(sink, "tasks.max", UNUSED, _DEFAULT + " one task applies the changes"
        " in the order the log holds them")
    put(sink, "topics.regex", USED, "every table's topic under the prefix,"
        " and not the transactions' one beside them")
    put(sink, "topics", UNUSED, "the tables are matched by the pattern, so a"
        " table added on the source is carried too")
    for f in ("connection.url", "connection.username", "connection.password"):
        put(sink, f, USED, "where and as whom the sink writes the target")
    for f in ("connection.pool.min_size", "connection.pool.max_size",
              "connection.pool.acquire_increment", "connection.pool.timeout"):
        put(sink, f, UNUSED, _DEFAULT + " one task writes on one connection")
    put(sink, "insert.mode", USED, "`upsert`: a change applied twice leaves"
        " the row once")
    put(sink, "delete.enabled", USED, "a delete on the source deletes on the"
        " target")
    put(sink, "truncate.enabled", USED, "a truncate on the source empties the"
        " table on the target, now that the source carries it")
    put(sink, "primary.key.mode", USED, "the change's key, which is the"
        " table's primary key")
    put(sink, "primary.key.fields", UNUSED, "the key comes whole from the"
        " change's own key")
    put(sink, "schema.evolution", USED, "`basic`: a column added on the"
        " source is added on the target")
    put(sink, "quote.identifiers", UNUSED, _DEFAULT + " names are written as"
        " the target folds them")
    for f in ("collection.name.format", "collection.naming.strategy",
              "column.naming.strategy"):
        put(sink, f, UNUSED, _DEFAULT + " a table is written under the name"
            " the source gives it; a hop that renames one is refused")
    for f in ("database.time_zone", "use.time.zone"):
        put(sink, f, UNUSED, _DEFAULT + " times are written in UTC, as"
            " `canon` compares them")
    put(sink, "batch.size", USED, "as large as a batch the source connector"
        " hands over (`sizing.stream_buffers`); its default is 500")
    put(sink, "use.reduction.buffer", UNUSED, "not yet: several changes to"
        " one key in a batch are each applied, in order")
    for f in ("field.include.list", "field.exclude.list"):
        put(sink, f, UNUSED, "the columns are left out at the source, where"
            " they are never read")
    for f in ("flush.max.retries", "flush.retry.delay.ms"):
        put(sink, f, UNUSED, _DEFAULT + " a write that keeps failing stops the"
            " sink, and `stream_status` says so")
    for f in ("dialect.postgres.postgis.schema",
              "dialect.sqlserver.identity.insert"):
        put(sink, f, UNUSED, "not a target this pipeline writes: PostgreSQL"
            " without PostGIS types, or MySQL")
    for c in both + sink:
        put((c,), "transforms", UNUSED, "the sink reads the change envelope"
            " as it is, deletes included: flattening it or routing it is for"
            " a sink that cannot, and masking a value on the way puts a"
            " value in the target the source never held")
        put((c,), "predicates", UNUSED, "no transform is applied, so there"
            " is nothing for a predicate to choose for")
    sig = ("signals",)
    put(sig, "execute-snapshot", USED, "a table read again through the"
        " stream, where a repair would otherwise race it (`send_resnapshot`)")
    put(sig, "pause-snapshot", USED, "a re-read beside the stream held"
        " while the source is under strain (`stream_hold`)")
    put(sig, "resume-snapshot", USED, "the held re-read let go once the"
        " source is no longer under strain (`stream_hold`)")
    put(sig, "stop-snapshot", UNUSED, "a re-read migkit asks for is the"
        " repair itself; stopped halfway it leaves the rows it had not read"
        " as they were, which only another re-read puts right - so migkit"
        " holds one and lets it go, and never stops one")
    put(sig, "log", UNUSED, "what the connector would write to its own log"
        " is read by no one; its notifications are read back instead")
    put(sink, "hibernate.", UNUSED, "the sink's own persistence settings are"
        " left as it ships them")
    return o


tooluse.register(
    "debezium", options=_debezium_account(),
    declared={"postgres": frozenset(_RELATIONAL + _POSTGRES + _CONNECT),
              "mysql": frozenset(_RELATIONAL + _MYSQL + _CONNECT),
              "jdbc-sink": frozenset(_SINK + _CONNECT),
              "signals": frozenset(_SIGNALS)},
    families=_DEBEZIUM_FAMILIES)
# the broker the generated pipeline pulls: source-available, its terms
# accepted where every closed program's are (`tools.TERMS`, `doctor
# --install`), and the pipeline not started until they are (`stream_ways`)
tooluse.register("redpanda", options={})


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
    return signal_message(hop_name, "execute-snapshot", tables, kind)


#: the signals the connector takes on its channel that migkit sends
SIGNALS = ("execute-snapshot", "pause-snapshot", "resume-snapshot")


def signal_message(hop_name, signal, tables=(), kind="blocking"):
    """The (topic, key, value) of one signal to the hop's connector: a
    re-read of `tables` (`execute-snapshot`), or holding and letting go of
    a re-read under way (`pause-snapshot`, `resume-snapshot` - which only
    a re-read beside the stream obeys: a blocking one holds the stream
    itself). The key is the connector's `topic.prefix`, which is how it
    tells its own requests on the shared topic from another's."""
    if signal not in SIGNALS:
        raise ValueError(f"{signal!r} is not one of {', '.join(SIGNALS)}")
    prefix = f"migkit-{hop_name}"
    value = {"type": signal, "data": {}}
    if signal == "execute-snapshot":
        value["data"] = {"data-collections": list(tables),
                         "type": kind.upper()}
    return f"{prefix}-signal", prefix, value


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


def _pg_snapshot_watermarks(hop, db):
    """Whether a PostgreSQL source can give the read-only re-read its
    watermarks: `pg_current_snapshot()`, from version 13.

    Measured with the connector shipped here (3.0.8) against PostgreSQL 16,
    no signalling table anywhere: without `read.only` the incremental
    re-read was refused (`Incremental snapshot is not properly
    configured`); with it, it read the 50 rows again beside the stream -
    `will end at position [50]`, then `finished` - the connector RUNNING
    throughout and nothing written to the source. A row updated before
    its chunk came out as the new value in the stream and in the re-read
    (`49 -> -1 -> -1`). The option is not in the connector's advertised
    configuration, which is why it is measured rather than looked up.

    One attempt: a source that cannot be asked keeps the pause."""
    from migkit.engines.postgres import PostgresEngine
    return (PostgresEngine(hop)._server_version("src", db) or 0) >= 130000


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
    if log:
        log(f"signalling {kind} snapshot of {', '.join(tables)}")
    return send_signal(out, hop_name, "execute-snapshot", tables, kind)


def send_signal(out, hop_name, signal, tables=(), kind="blocking"):
    """Put one signal on the hop's signal topic, through the compose file
    (`send_resnapshot` says why not through a port)."""
    topic, key, value = signal_message(hop_name, signal, tables, kind)
    import json as _json
    line = f"{key}\t{_json.dumps(value)}\n"
    cmd = ["docker", "compose", "-f", str(out / "docker-compose.yml"),
           "exec", "-T", "redpanda",
           "rpk", "topic", "produce", topic, "-f", "%k\\t%v\\n"]
    # sent once: a signal sent twice asks for the same re-read twice
    p = run(cmd, check=False, input=line, timeout=60, what="the signal")
    if p.returncode:
        raise runner.Failed("could not reach the signal topic: "
                            + runner.said(p.stderr or p.stdout)[-200:])
    return topic


#: what the connector's notifications say, in migkit's words
_READS = {"Initial Snapshot": "the first read of the source",
          "Incremental Snapshot": "the re-read beside the stream",
          "Blocking Snapshot": "the re-read that holds the stream"}
_HAPPENED = {"STARTED": "started", "COMPLETED": "finished",
             "ABORTED": "was stopped", "SKIPPED": "was not needed",
             "PAUSED": "is held", "RESUMED": "goes on"}


def snapshot_words(note):
    """One notification from the connector, as a line in migkit's words,
    or None for one that says nothing an operator acts on."""
    what = _READS.get(str(note.get("aggregate_type") or ""))
    if what is None:
        return None
    kind = str(note.get("type") or "")
    extra = note.get("additional_data") or {}
    table = (extra.get("data_collection") or extra.get("scanned_collection")
             or extra.get("current_collection_in_progress") or "")
    if kind == "TABLE_SCAN_COMPLETED":
        rows = str(extra.get("total_rows_scanned") or "")
        status = str(extra.get("status") or "SUCCEEDED").lower()
        return (f"{what}: {table} read"
                + (f", {int(rows):,} rows" if rows.isdigit() else "")
                + ("" if status == "succeeded" else f" ({status})"))
    if kind == "IN_PROGRESS":
        return f"{what}: reading {table}" if table else None
    said = _HAPPENED.get(kind)
    return f"{what} {said}" if said else None


def _notifications(out, hop_name):
    """The connector's notifications so far, oldest first, read off their
    topic through the compose file; [] where there are none or the broker
    cannot be reached."""
    import json as _json
    topic = f"migkit-{hop_name}-notifications"
    cmd = ["docker", "compose", "-f", str(out / "docker-compose.yml"),
           "exec", "-T", "redpanda", "rpk", "topic", "consume", topic,
           "--offset", ":end", "--format", "%v\\n"]
    try:
        p = run(cmd, check=False, timeout=30, retries=3,
                what="the stream's notifications")
    except Exception:  # noqa: BLE001 - progress, never a failure
        return []
    got = []
    for line in (p.stdout or "").splitlines():
        try:
            got.append(_json.loads(line))
        except ValueError:
            continue
    return got


def stream_progress(out, hop_name, since=0):
    """([line], how many notifications there were) - what the connector
    reported of its reads after the first `since`, in migkit's words."""
    notes = _notifications(out, hop_name)
    return ([w for w in (snapshot_words(n) for n in notes[since:]) if w],
            len(notes))


def rereading(notes):
    """Whether a re-read beside the stream is under way, by the last thing
    the connector said about one."""
    last = None
    for n in notes:
        if n.get("aggregate_type") == "Incremental Snapshot":
            last = str(n.get("type") or "")
    return last in ("STARTED", "IN_PROGRESS", "TABLE_SCAN_COMPLETED",
                    "RESUMED")


def stream_hold(out, hop_name, strained, held, log=None):
    """Hold a re-read beside the stream while the source is under strain,
    and let it go on once it is not; returns whether it is held now.

    A re-read beside the stream reads a whole table from the source in
    chunks, on top of the application's load, for as long as it takes, and
    the connector reads on regardless. migkit asks the source how it is
    (`sizing.strain`) and holds the re-read (`pause-snapshot`) while the
    source runs more sessions than its CPUs take - the way gh-ost holds its
    copy on a replica's lag - and lets it go on (`resume-snapshot`) once
    it does not. `strained` is that answer ("" when it is not), `held`
    whether migkit holds it already. Nothing is sent where no re-read is
    under way."""
    if strained and not held:
        if not rereading(_notifications(out, hop_name)):
            return False
        send_signal(out, hop_name, "pause-snapshot")
        if log:
            log(f"the re-read beside the stream is held: {strained}")
        return True
    if held and not strained:
        send_signal(out, hop_name, "resume-snapshot")
        if log:
            log("the re-read beside the stream goes on: the source is no"
                " longer under strain")
        return False
    return held


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


def refused_config(cfg, port=8083):
    """What the connector runtime refuses in `cfg`, asked before it is
    registered: [(option, why)], [] where it takes all of it. The runtime
    validates against the build it runs, which is the build that matters -
    an option a later release renamed is refused here rather than ignored
    by a connector that then runs without it."""
    from .wording import without_programs
    cls = str(cfg["config"].get("connector.class") or "")
    if not cls:
        return []
    _, got = _connect_api("PUT", f"/connector-plugins/{cls}/config/validate",
                          cfg["config"], port)
    out = []
    for c in (got or {}).get("configs") or []:
        value = c.get("value") or {}
        for why in value.get("errors") or []:
            out.append((value.get("name"),
                        without_programs(str(why), ("debezium",))))
    return out


def stream_register(out, port=8083, log=None):
    import json as _json
    import urllib.error
    for f in ("source-connector.json", "sink-connector.json"):
        cfg = _json.loads((out / f).read_text())
        name = cfg["name"]
        refused = refused_config(cfg, port)
        if refused:
            raise SystemExit(
                "the change stream's configuration was refused before it"
                " started: " + "; ".join(f"{k}: {why}" for k, why in
                                         refused[:5])
                + ". Nothing has been registered")
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


def stream_parts(hop_name, port=8083, timeout=2):
    """[(part, running)] of the hop's pipeline: its source and sink
    connectors and each of their tasks, as the pipeline's API says - for
    `/metrics`; nothing where the API does not answer."""
    out = []
    for side in ("source", "sink"):
        name = f"migkit-{hop_name}-{side}"
        try:
            _, body = _connect_api("GET", f"/connectors/{name}/status",
                                   port=port, timeout=timeout)
        except Exception:  # noqa: BLE001 - a scrape never fails on one
            continue
        out.append((side, (body.get("connector") or {}).get("state")
                    == "RUNNING"))
        for i, t in enumerate(body.get("tasks") or []):
            out.append((f"{side} task {i}", t.get("state") == "RUNNING"))
    return out


def stream_readings(attributes, port, timeout=2, server=None):
    """{attribute: value} of the stream reader's own metrics - its MBeans
    read over HTTP through the agent its image carries (`ENABLE_JOLOKIA`
    in the pipeline's compose), summed over the reader's contexts where
    it has more than one; {} where nothing answers. The image's 3.0
    entrypoint, read for this, starts that agent on port 8778 when
    `ENABLE_JOLOKIA` is `true`; no pipeline was run to see it answer.
    `server` keeps the readings of one hop's connector only - its topic
    prefix, `migkit-<hop>`, which each MBean names as `server=`: the
    agent answers for every connector the machine runs, and a hop given
    another's readings was told it was behind when that one was."""
    import json as _json
    import urllib.request
    pattern = (f"debezium.*:type=connector-metrics,server={server},*"
               if server else "debezium.*:type=connector-metrics,*")
    url = (f"http://localhost:{port}/jolokia/read/"
           + quote(pattern, safe=""))
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            got = _json.loads(r.read())
    except Exception:  # noqa: BLE001 - a scrape never fails on one
        return {}
    values = got.get("value") if isinstance(got, dict) else None
    out = {}
    for name, bean in (values or {}).items():
        if server is not None and f"server={server}" not in \
                str(name).partition(":")[2].split(","):
            continue
        for attr, v in (bean or {}).items():
            if attr not in attributes or v is None:
                continue
            if isinstance(v, bool):
                v = int(v)
            if isinstance(v, (int, float)) and v >= 0:
                out[attr] = (max(out.get(attr, 0), v)
                             if attr == "MilliSecondsBehindSource"
                             else out.get(attr, 0) + v)
    return out


def stream_status(name, port=8083):
    """Summarize a connector + its tasks: RUNNING/FAILED per component."""
    try:
        _, body = _connect_api("GET", f"/connectors/{name}/status", port=port)
    except Exception as e:
        return f"{name}: unreachable ({str(e).splitlines()[-1][:50]})"
    conn = body.get("connector", {}).get("state", "?")
    tasks = [t.get("state", "?") for t in body.get("tasks", [])]
    return f"{name}: connector={conn} tasks={','.join(tasks) or 'none'}"


def _movers():
    from . import legs
    return {"pgdump": pgdump_move, "pgcopydb": pgcopydb_move,
            "mydumper": mydumper_move, "mysqlsh": mysqlsh_move,
            "pgloader": pgloader_move, "mongodump": mongodump_move,
            "mongosync": mongosync_move, "riotx": riotx_move,
            "native": native_move, "pgpull": legs.pull_move,
            "pgsubscribe": legs.subscribe_move,
            "redisshake": _redisshake_move, "mirror": _mirror_move}


def _redisshake_move(hop, db, workers, go, log):
    from . import redisshake
    return redisshake.move(hop, db, workers, go, log)


def _mirror_move(hop, db, workers, go, log):
    from . import mirror
    return mirror.move(hop, db, workers, go, log)


def _mirror_runner():
    from . import mirror
    return mirror.runner()


#: the programs each bulk path runs, in the order it runs them
PROGRAMS = {"pgdump": ("pg_dump", "pg_restore"),
            "pgcopydb": ("docker",),
            "mydumper": ("mydumper", "myloader"),
            "mysqlsh": ("mysqlsh",),
            "pgloader": ("pgloader",),
            "mongodump": ("mongodump", "mongorestore"),
            "mongosync": ("mongosync",),
            "riotx": ("riotx",),
            "native": (), "pgpull": (), "pgsubscribe": (),
            "redisshake": ("redis-shake",),
            "mirror": ("connect-mirror-maker.sh",)}
#: the programs a bulk path runs besides those, where they are installed:
#: the MySQL bulk copy carries the tables chosen for the straight copy by it
ALSO = {"mydumper": ("mysqlsh",)}
#: how a report names them: never by the program's own name
ROLES = ("dump program", "load program")
#: the builds each program was measured with here (backlog 46), as the
#: registry has them (`tools.PROGRAM`)
MEASURED = {n: p.measured for n, p in PROGRAM.items() if p.measured}


def programs(via):
    """The programs `via` runs on this machine. The streaming copy runs its
    local build where there is one and its image otherwise
    (`pgcopydb_runner`), so which program it is depends on the machine."""
    if via == "pgcopydb":
        return ("pgcopydb",) if pgcopydb_runner() == "local" else ("docker",)
    if via == "mirror":
        return (("docker",) if _mirror_runner() == "image"
                else ("connect-mirror-maker.sh",))
    return PROGRAMS.get(via, ())


def program_version(program):
    """The installed build's version, as it prints it - asked once per
    build (`toolversion.version`)."""
    return toolversion.version(program)


def options_missing(hop, db, via):
    """{program: [option]} for every long option the bulk path's own
    command lines pass a program whose installed build does not have it.

    The command lines are the dry run's, so they are the ones the move
    would run. Each has changed underneath migkit already - a flag
    renamed, another removed - and the program stopped at option parsing,
    after migkit had started. `tests/test_wrapped_flags_exist.py` holds
    them against the builds installed where the tests run; this holds
    them against the build installed where the move runs."""
    out = {}
    wrapped = [p for p in programs(via) if p in MEASURED]
    if not wrapped or not all(which(p) for p in wrapped):
        return out
    wrapped += [p for p in ALSO.get(via, ()) if p in MEASURED and which(p)]
    for step in _movers()[via](hop, db, 1, False, None) or ():
        argv = getattr(step, "argv", None) or []
        # a piped step carries two programs
        parts, cur = [], []
        for a in argv:
            if a == "|":
                parts.append(cur)
                cur = []
            else:
                cur.append(a)
        parts.append(cur)
        for part in parts:
            if not part:
                continue
            program = str(part[0]).rsplit("/", 1)[-1]
            if program not in wrapped:
                continue
            flags = {str(a).split("=", 1)[0] for a in part
                     if str(a).startswith("--")}
            gone = sorted(flags - _long_options(program,
                                                *_subcommand(part)))
            if gone:
                out.setdefault(program, [])
                out[program] += [f for f in gone if f not in out[program]]
    return out


def _role(via, program):
    runs = programs(via)
    if program not in runs or len(runs) == 1:
        return _program_role(program)
    return ROLES[min(runs.index(program), len(ROLES) - 1)]


def bulk_path_report(hop, db, via):
    """[(level, item, detail)] for the programs `via` runs: the build
    installed, whether it is one migkit was measured with, and whether it
    takes every option migkit passes it. No program is named."""
    rows = []
    try:
        missing, refused = options_missing(hop, db, via), ""
    except SystemExit as e:
        # a flag the move chooses between spellings of, and this build has
        # none of them (`tool_flag`)
        missing, refused = {}, str(e)
    for program in programs(via) + ALSO.get(via, ()):
        if program not in MEASURED or not which(program):
            continue
        role = _role(via, program)
        have = program_version(program) or "unknown"
        measured = MEASURED[program]
        seen = ("the build migkit was measured with" if have == measured
                else f"not the build migkit was measured with ({measured})")
        if refused:
            rows.append(("fail", f"the {role}, {have}",
                         f"{seen}; {refused}"))
        elif missing.get(program):
            rows.append(("fail", f"the {role}, {have}",
                         f"{seen}; it does not take"
                         f" {', '.join(missing[program])}, which the move"
                         " passes it, so it would stop at option parsing"
                         " - migkit doctor --install puts a build that"
                         " does in place"))
        else:
            rows.append(("pass", f"the {role}, {have}",
                         f"{seen}; it takes every option the move passes"
                         " it"))
    return rows


def run_via(via, hop, db, workers, go, log):
    """The bulk path `via` over database `db`: the steps it says, and with
    `go` what it ran - its programs run through the runner (`runner`),
    each listed in the database's list of running programs while it runs,
    its command line in the run's debug log, `kill` taken as ctrl-c while
    it runs. Returns `Ran`: the way that ran them, which is another where
    the one asked for handed its work on (`pgcopydb_move`).

    Each program's options are held to its installed build on the very
    command line about to start (`runner.flags_refused`), not on a
    rehearsal of the whole path run first: the rehearsal asked the source
    again for everything the path asks, on every move. Only a build other
    than the one measured is rehearsed as well, first, so that nothing is
    written before it is refused."""
    fns = _movers()
    if via not in fns:
        # a mover nobody has taught this function about must stop here rather
        # than fall through to whatever happens to be first
        raise SystemExit(f"no bulk path named {via!r}")
    tools = programs(via)
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
    running = hop.report_dir(db) / "running-programs.json"
    if go:
        left = runner.still_running(running, log)
        if left:
            pid, program, since = left[0]
            raise SystemExit(
                f"the {_program_role(program)} an earlier move of {db}"
                f" started is still running (process {pid}, since"
                f" {time.strftime('%H:%M:%S', time.localtime(since or 0))}):"
                " the run that started it was stopped without stopping it,"
                " and it is still writing. A move started beside it would"
                " empty the tables under it. Wait for it to finish, or stop"
                f" it (`kill {pid}`), then run the move again. Nothing has"
                " been written by this run")
    odd = [p for p in programs(via) + ALSO.get(via, ())
           if go and p in MEASURED and which(p)
           and program_version(p) != MEASURED[p]]
    if odd:
        # a build other than the one measured is held to what the move
        # passes it before anything is written, through a rehearsal of
        # the path's command lines; the measured build takes them all
        # (`tests/test_wrapped_flags_exist.py`), and is not rehearsed
        refused = options_missing(hop, db, via)
        if refused:
            raise SystemExit(
                "the installed "
                + " and ".join(f"{_role(via, p)} ({program_version(p)})"
                               for p in refused)
                + " does not take options the move passes it: "
                + "; ".join(", ".join(v) for v in refused.values())
                + ". Nothing has been written. migkit doctor --install"
                " puts a build that does in place")
    global _SIZES
    from contextlib import ExitStack

    from .wording import DebugLog
    _SIZES = _source_sizes(hop, db) if go else None
    try:
        with ExitStack() as stack:
            stack.enter_context(runner.recording(
                DebugLog(hop.report_dir(db) / "commands.log"),
                (hop.source.password, hop.target.password)))
            if go:
                stack.enter_context(runner.listing(running))
                # stopped by a service manager or `kill`: taken as ctrl-c,
                # so the programs this run started are stopped with it
                stack.enter_context(runner.stopping())
            got = fns[via](hop, db, workers, go, log)
    finally:
        _SIZES = None
    return got if isinstance(got, Ran) else Ran(got, via)
