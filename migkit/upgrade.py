"""A major-version upgrade as a goal (backlog R23, 7).

`goal: upgrade` is one engine at a newer version with the least downtime.
The source stays where it is and as it is; the target runs, or will run,
the new version; the move is the same move, follow and cutover any hop
makes, and the cutover's outage is the only downtime. What is particular
to an upgrade is decided and checked here.

**The way, by what was measured.** migkit never upgrades the source where
it runs: that is a change of the source's server, which migkit does not
make (the only writes to a source are a cutover's, and they are undone).
So the choice is between the ways that leave the source as it is, each a
`decide` rung (`ways`, `choose`):

* *a copy of the pages upgraded on the target* - the target seeded at the
  source's own version from a copy of its pages or a cloud's own copy
  (`physical`, `cloudseed`), upgraded in place there by the new version's
  own upgrade - PostgreSQL's, run where the target's data is; MySQL's,
  which a server of the new version runs as it first starts on the old
  data; a cloud's, asked of its API - and followed from the position the
  copy holds, which the upgrade does not move. Indexes arrive built,
  nothing is decoded, and the upgrade's own step is timed;
* *a logical move into the new version* - the target already running it,
  filled by the copy and followed as any cutover hop.

Both end in the same cutover. Which holds is the target's own version and
the hop's `physical:` settings; where both could, the costs this hop and
its rehearsal measured rank them (`physical.choose` weighs the copy of the
pages against the logical copy the same way). What an upgrade done in
place on the source would have cost is said beside it (`in_place_line`),
from the in-place step measured on the target: the downtime the operator
avoids.

**Checked first** (`checks`), each with its fix, before anything is
written - read from both servers' own catalogues wherever they can say it,
a table of the engine's history only where they cannot:

* the versions: one engine, the target newer, a jump the way allows;
* what the new version no longer has: settings the source sets that it
  does not know (`REMOVED_SETTINGS` says what replaced each), types a
  table still holds, functions the source's own code still calls;
* what it changed by default: every setting the source leaves at its
  default whose default the new version changed, compared default for
  default - not from a list;
* the extensions, each offered by the target at the source's version or
  one it can be updated to;
* the collations: a collation whose library differs sorts text otherwise -
  indexes carried as pages are rebuilt on the target (`after`), and text
  ordered by it may come back in another order;
* plan regressions: the source's own statements planned, timed and held
  to the same answers on both versions (`workload`), once the target holds
  the rows;
* for MySQL, its own upgrade checker, where the build here has it, every
  error and warning in migkit's words.
"""
import json
import re
import shlex
import time

from . import decide

GOAL = "upgrade"
#: the path the in-place step's time is kept under with the hop's rates
IN_PLACE = "upgrade in place"


def settings(hop):
    """The hop's `physical: {upgrade: {...}}` for a hop whose goal is
    upgrade ({} where it says none); None for any other goal."""
    if (getattr(hop, "goal", "") or "") != GOAL:
        return None
    phys = (hop.options or {}).get("physical")
    got = phys.get("upgrade") if isinstance(phys, dict) else None
    return dict(got) if isinstance(got, dict) else {}


def major(version):
    """The major version of a version text, as a tuple two versions are
    compared by: from 10 on the first number (PostgreSQL 16, SQL Server
    16), below it the first two (MySQL 8.0 and 8.4, PostgreSQL 9.6,
    MongoDB 7.0). None where the text holds no number."""
    nums = [int(x) for x in re.findall(r"\d+", str(version or ""))[:3]]
    if not nums:
        return None
    return tuple(nums[:1]) if nums[0] >= 10 else tuple(nums[:2])


def _series(version):
    nums = [int(x) for x in re.findall(r"\d+", str(version or ""))[:3]]
    return tuple(nums + [0] * (3 - len(nums))) if nums else None


def _side(eng, side):
    return getattr(eng, "src_engine" if side == "src" else "dst_engine",
                   None) or eng


def _engine(eng):
    return getattr(_side(eng, "src"), "CANON_ENGINE", "") or ""


def versions(hop, eng, db):
    """(the source's version, the target's) as each server says it
    (`interlock.identity`); a side that does not answer is None."""
    from . import interlock
    out = []
    for side in ("src", "dst"):
        try:
            out.append(interlock.identity(hop, eng, side, db).get("version"))
        except Exception:  # noqa: BLE001 - not known, said so
            out.append(None)
    return tuple(out)


def new_version(hop, eng, db, have=None):
    """The version the target runs once the upgrade is done: its own where
    it is newer than the source already (a logical move), else what the
    hop's upgrade settings say or its new binaries answer. None where
    nothing says."""
    src, dst = have or versions(hop, eng, db)
    if src and dst and major(dst) and major(src) and major(dst) > major(src):
        return dst
    up = settings(hop) or {}
    if up.get("version"):
        return str(up["version"])
    from . import physical
    opts = physical.settings(hop) or {}
    if up.get("bin") and opts.get("run"):
        prog = "postgres" if _engine(eng) == "postgres" else "mysqld"
        got = physical._on_target(
            opts, f"{shlex.quote(str(up['bin']).rstrip('/') + '/' + prog)}"
                  " --version", timeout=60)
        found = re.search(r"(\d+\.\d+(?:\.\d+)?)", got.stdout or "")
        if got.returncode == 0 and found:
            return found.group(1)
    return None


# --- the checks ----------------------------------------------------------------

def _row(level, item, detail, fix=""):
    return {"level": level, "scope": "upgrade", "item": item,
            "detail": detail + (f"  [fix] {fix}" if fix and level != "pass"
                                else "")}


def checks(hop, eng, db, quick=False):
    """assess rows of everything the upgrade of `db` needs known first;
    `quick` leaves out the source's statements compared on both
    versions, which take up to a minute and a half."""
    src_v, dst_v = versions(hop, eng, db)
    new = new_version(hop, eng, db, (src_v, dst_v))
    out = []
    if not src_v or not new:
        return [_row("warn", "versions",
                     f"the source says {src_v or 'nothing'} and the new"
                     f" version is {new or 'not known'} - unknown, not"
                     " clean", "the hop's upgrade settings name the new"
                     " version (physical: {upgrade: {version: ...}}), or"
                     " the target runs it")]
    a, b = major(src_v), major(new)
    if b <= a:
        return [_row("fail", "versions",
                     f"the source runs {src_v} and the new version is {new}:"
                     " an upgrade goes to a newer major version",
                     "point the hop's target at a server of the new version,"
                     " or name it in physical: {upgrade: {version: ...}}")]
    from . import physical
    # the target at the source's version, or not made yet by a cloud's
    # copy: upgraded after a copy of the pages, its new version's own
    # catalogue read only once it runs it
    seeded = (major(dst_v) == a) if dst_v else bool(physical.settings(hop))
    out.append(_row("pass", "versions",
                    f"{src_v} to {new}"
                    + (" - the target is upgraded after a copy of the"
                       " source's pages, and what only the new version's"
                       " own catalogue says is read once it runs" if seeded
                       else "")))
    ask = {"postgres": _pg_checks, "mysql": _my_checks,
           "mongodb": _mongo_checks, "mssql": _mssql_checks}.get(
        _engine(eng))
    if ask is None:
        out.append(_row("warn", "what the new version changes",
                        f"no version checks for {hop.engine} yet: the move,"
                        " the check and the comparison of the source's"
                        " statements run as on any hop"))
        return out
    for one in ask(hop, eng, db, src_v, new, seeded, bool(dst_v)):
        # each asked on its own: one that cannot be read leaves the rest
        try:
            out += one()
        except Exception as e:  # noqa: BLE001 - said, the rest stands
            out.append(_row("warn", "what the new version changes",
                            f"could not be read: {_last(e)} - unknown, not"
                            " clean"))
    if dst_v and not seeded and not quick:
        out.append(_plans(hop, eng, db))
    return out


def _plans(hop, eng, db):
    """The source's statements on both versions (`workload`), once the
    target holds the rows."""
    from . import workload
    dst = _side(eng, "dst")
    try:
        empty = not dst.rows_present(db, dst.neutral_tables("dst", db))
    except Exception:  # noqa: BLE001 - asked, and not known
        empty = None
    if empty:
        return _row("pass", "plan regressions",
                    "compared once the target holds the rows: migkit check"
                    f" {hop.name} plans, times and holds to the same answers"
                    " the source's own statements on both versions")
    got = workload.compare(eng, db)
    level = {"ok": "pass", "warn": "warn"}.get(got.status, "pass")
    return _row(level, "plan regressions", got.detail, got.fix_hint)


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def _num(text):
    """An extension's or a server's version as numbers, for ordering."""
    return tuple(int(x) for x in re.findall(r"\d+", str(text or "")))


#: settings the new version no longer has, and what took each one's
#: place, from each release's notes - said beside a setting the source
#: still sets
REMOVED_SETTINGS = {
    "postgres": {
        "checkpoint_segments": "max_wal_size (9.5)",
        "min_parallel_relation_size": "min_parallel_table_scan_size (10)",
        "sql_inheritance": "nothing: inheritance is always on (10)",
        "replacement_sort_tuples": "nothing (11)",
        "default_with_oids": "nothing: tables have no OIDs (12)",
        "wal_keep_segments": "wal_keep_size (13)",
        "operator_precedence_warning": "nothing (14)",
        "vacuum_cleanup_index_scale_factor": "nothing (14)",
        "stats_temp_directory": "nothing: statistics live in shared memory"
                                " (15)",
        "force_parallel_mode": "debug_parallel_query (16)",
        "vacuum_defer_cleanup_age": "hot_standby_feedback or a replication"
                                    " slot (16)",
        "promote_trigger_file": "pg_ctl promote or pg_promote() (16)",
        "old_snapshot_threshold": "nothing (17)",
        "db_user_namespace": "nothing (17)",
        "trace_recovery_messages": "nothing (17)"},
    "mysql": {
        "tx_isolation": "transaction_isolation (8.0)",
        "tx_read_only": "transaction_read_only (8.0)",
        "log_warnings": "log_error_verbosity (8.0)",
        "query_cache_size": "nothing: the query cache is gone (8.0)",
        "query_cache_type": "nothing: the query cache is gone (8.0)",
        "innodb_file_format": "nothing (8.0)",
        "innodb_large_prefix": "nothing (8.0)",
        "expire_logs_days": "binlog_expire_logs_seconds (8.4)",
        "default_authentication_plugin": "authentication_policy (8.4)",
        "master_info_repository": "nothing: kept in tables (8.4)",
        "relay_log_info_repository": "nothing: kept in tables (8.4)",
        "binlog_transaction_dependency_tracking": "nothing (8.4)"},
}

#: functions the source's own code may still call, by the major version
#: that took them away, from each release's notes
REMOVED_FUNCTIONS = {
    "postgres": {
        (10,): ("pg_current_xlog_location", "pg_current_xlog_insert_location",
                "pg_current_xlog_flush_location",
                "pg_last_xlog_receive_location",
                "pg_last_xlog_replay_location", "pg_xlog_location_diff",
                "pg_xlogfile_name", "pg_xlogfile_name_offset",
                "pg_switch_xlog", "pg_is_xlog_replay_paused",
                "pg_xlog_replay_pause", "pg_xlog_replay_resume"),
        (15,): ("pg_start_backup", "pg_stop_backup"),
    },
    "mysql": {
        (8, 0): ("password", "old_password", "encode", "decode", "encrypt",
                 "des_encrypt", "des_decrypt", "glength", "astext",
                 "geomfromtext", "pointfromtext"),
    },
}

#: types a table may hold that the new version, or its own upgrade, does
#: not take: (the major version from which, only the in-place upgrade
#: refuses it, why)
PG_TYPES = {
    "abstime": ((12,), False, "the type is gone"),
    "reltime": ((12,), False, "the type is gone"),
    "tinterval": ((12,), False, "the type is gone"),
    "unknown": ((10,), False, "a column may no longer be of it"),
    "sql_identifier": ((12,), True, "its storage changed"),
    "aclitem": ((16,), True, "its storage changed"),
    "regproc": ((1,), True, "it holds object numbers the upgrade does not"
                            " keep"),
    "regprocedure": ((1,), True, "it holds object numbers the upgrade does"
                                 " not keep"),
    "regoper": ((1,), True, "it holds object numbers the upgrade does not"
                            " keep"),
    "regoperator": ((1,), True, "it holds object numbers the upgrade does"
                                " not keep"),
    "regconfig": ((1,), True, "it holds object numbers the upgrade does not"
                              " keep"),
    "regdictionary": ((1,), True, "it holds object numbers the upgrade does"
                                  " not keep"),
    "regnamespace": ((1,), True, "it holds object numbers the upgrade does"
                                 " not keep"),
    "regcollation": ((1,), True, "it holds object numbers the upgrade does"
                                 " not keep"),
}


def _removed_between(engine, old, new):
    """The functions the major versions after `old` up to `new` took
    away."""
    out = []
    for at, names in (REMOVED_FUNCTIONS.get(engine) or {}).items():
        if old < at <= new:
            out += list(names)
    return out


# --- PostgreSQL ------------------------------------------------------------------

def _pg(eng, side, db, sql):
    e = _side(eng, side)
    return [r.split("\x1f") for r in e._psql(side, db, sql).splitlines()
            if r]


def _pg_checks(hop, eng, db, src_v, new, seeded, reached):
    """The checks of a PostgreSQL upgrade, each a callable: what needs the
    new version's catalogue only where the target runs it, what needs the
    target's host only where it answers."""
    old, nw = major(src_v), major(new)
    out = [lambda: _pg_types(eng, db, old, nw, seeded),
           lambda: _pg_code(eng, db, old, nw),
           lambda: _pg_defaults_elsewhere(eng, db, old, nw, seeded)]
    if seeded:
        out.append(lambda: [_row(
            "pass", "settings and extensions",
            "the new version's own defaults and the extensions it offers"
            " are read once the target runs it; before anything is stopped"
            " its own check holds the copy to them - every library an"
            " extension needs present")])
    else:
        out += [lambda: _pg_settings(eng, db, seeded, nw),
                lambda: _pg_extensions(eng, db, seeded)]
    if reached:
        out.append(lambda: [_pg_collations(eng, db, seeded)])
    return out


def _pg_settings(eng, db, seeded, nw):
    """Removed and changed defaults, from both servers' `pg_settings`: the
    compiled-in default (`boot_val`) of each - where the target runs the
    source's version still, the new one's cannot be read, and is compared
    once it runs."""
    q = ("select name || chr(31) || coalesce(boot_val, '') || chr(31)"
         " || source || chr(31) || context || chr(31)"
         " || coalesce(setting, '') from pg_settings")
    src = {r[0]: r for r in _pg(eng, "src", db, q) if len(r) == 5}
    if seeded:
        return [_row("pass", "settings",
                     "the new version's own defaults are compared once the"
                     " target runs it: migkit assess after the upgrade")]
    dst = {r[0]: r for r in _pg(eng, "dst", db, q) if len(r) == 5}
    gone, changed = [], []
    for name, (_, boot, source, context, value) in sorted(src.items()):
        if context == "internal":
            continue
        if name not in dst:
            if source not in ("default", "override", "client", "session"):
                instead = REMOVED_SETTINGS["postgres"].get(name)
                gone.append(f"{name} = {value}"
                            + (f" (now {instead})" if instead else ""))
            continue
        if source == "default" and boot != dst[name][1]:
            changed.append(f"{name}: {boot or 'empty'} by default before,"
                           f" {dst[name][1] or 'empty'} now")
    out = []
    if gone:
        out.append(_row("warn", "settings the new version does not have",
                        f"{len(gone)} set on the source: " + "; ".join(
                            gone[:8]) + (" ..." if len(gone) > 8 else ""),
                        "leave them out of the target's configuration - a"
                        " server that reads a setting it does not know"
                        " does not start - and set what replaced each"))
    if changed:
        out.append(_row("warn", "defaults the new version changed",
                        f"{len(changed)} the source leaves at their"
                        " default: " + "; ".join(changed[:8])
                        + (" ..." if len(changed) > 8 else ""),
                        "set each on the target to the source's value to"
                        " keep its behaviour, or keep the new default once"
                        " the source's statements are measured on it"))
    if not gone and not changed:
        out.append(_row("pass", "settings",
                        f"{len(src)} compared: none the source sets is gone,"
                        " no default it relies on changed"))
    return out


def _pg_extensions(eng, db, seeded):
    """Each extension the source uses, offered by the target at its
    version or one it is updated to after."""
    have = {r[0]: r[1] for r in _pg(eng, "src", db,
                                    "select extname || chr(31) || extversion"
                                    " from pg_extension") if len(r) == 2}
    offered = {}
    for r in _pg(eng, "dst", db, "select name || chr(31) || version from"
                                 " pg_available_extension_versions"):
        if len(r) == 2:
            offered.setdefault(r[0], []).append(r[1])
    default = {r[0]: r[1] for r in _pg(eng, "dst", db,
                                       "select name || chr(31) ||"
                                       " coalesce(default_version, '')"
                                       " from pg_available_extensions")
               if len(r) == 2}
    missing, older, later, same = [], [], [], []
    for ext, v in sorted(have.items()):
        if ext == "plpgsql":
            continue
        there = offered.get(ext) or []
        if not there:
            missing.append(ext)
        elif v in there:
            if _num(default.get(ext)) > _num(v):
                later.append(f"{ext} {v} (updated to {default[ext]} after)")
            else:
                same.append(f"{ext} {v}")
        elif max(there, key=_num) and _num(max(there, key=_num)) < _num(v):
            older.append(f"{ext}: the source runs {v}, the target offers up"
                         f" to {max(there, key=_num)}")
        else:
            later.append(f"{ext} {v} - not offered as it is; created at"
                         f" {default.get(ext) or max(there, key=_num)}")
    out = []
    if missing:
        out.append(_row("fail", "extensions",
                        "the target does not offer "
                        + ", ".join(missing) + " at all",
                        "install each one's package for the new version on"
                        " the target's server first, or drop it on the"
                        " source if nothing uses it"))
    if older:
        out.append(_row("fail", "extensions", "; ".join(older),
                        "an extension does not go back a version: install"
                        " the source's version or later for the new server"))
    if later:
        out.append(_row("warn" if seeded else "pass", "extensions",
                        "; ".join(later),
                        "migkit updates each on the target after the"
                        " upgrade (ALTER EXTENSION ... UPDATE)"))
    if not (missing or older or later):
        out.append(_row("pass", "extensions",
                        (", ".join(same) or "none but the built-in ones")
                        + " - offered as the source runs them"))
    return out


def _pg_types(eng, db, old, nw, seeded):
    names = ", ".join(f"'{t}'" for t in PG_TYPES)
    rows = _pg(eng, "src", db, f"""
        select n.nspname || '.' || c.relname || '.' || a.attname || chr(31)
               || t.typname
          from pg_attribute a
          join pg_class c on c.oid = a.attrelid
          join pg_namespace n on n.oid = c.relnamespace
          join pg_type t on t.oid = a.atttypid
         where a.attnum > 0 and not a.attisdropped
           and c.relkind in ('r', 'm', 'p')
           and n.nspname not in ('pg_catalog', 'information_schema')
           and n.nspname not like 'pg\\_%'
           and t.typname in ({names})""")
    bad = []
    for col, typ in (r for r in rows if len(r) == 2):
        at, in_place, why = PG_TYPES[typ]
        if nw >= at and (seeded or not in_place) and (
                not in_place or old < at or at == (1,)):
            bad.append(f"{col} ({typ}: {why})")
    if old < (12,) <= nw:
        for (name,) in _pg(eng, "src", db,
                           "select n.nspname || '.' || c.relname from"
                           " pg_class c join pg_namespace n on n.oid ="
                           " c.relnamespace where c.relhasoids and"
                           " n.nspname not in ('pg_catalog',"
                           " 'information_schema')"):
            bad.append(f"{name} (WITH OIDS: tables have none from 12)")
    if old < (14,) <= nw:
        for (name,) in _pg(eng, "src", db,
                           "select oprname from pg_operator where oprright"
                           " = 0 and oid >= 16384"):
            bad.append(f"operator {name} (a postfix operator: none from"
                       " 14)")
    if not bad:
        return [_row("pass", "types", "no table holds a type the new"
                                      " version or its upgrade refuses")]
    return [_row("fail", "types", "; ".join(bad[:8])
                  + (" ..." if len(bad) > 8 else ""),
                  "change each column's type on the source (or drop what"
                  " holds it) before the upgrade, or move it logically"
                  " into a target that already runs the new version")]


def _pg_code(eng, db, old, nw):
    gone = _removed_between("postgres", old, nw)
    if not gone:
        return []
    pattern = "|".join(gone)
    rows = _pg(eng, "src", db, f"""
        select 'function ' || n.nspname || '.' || p.proname
          from pg_proc p join pg_namespace n on n.oid = p.pronamespace
         where n.nspname not in ('pg_catalog', 'information_schema')
           and p.prosrc ~* '\\m({pattern})\\M'
        union all
        select 'view ' || n.nspname || '.' || c.relname
          from pg_class c join pg_namespace n on n.oid = c.relnamespace
         where c.relkind in ('v', 'm')
           and n.nspname not in ('pg_catalog', 'information_schema')
           and pg_get_viewdef(c.oid) ~* '\\m({pattern})\\M'""")
    found = [r[0] for r in rows if r]
    if not found:
        return [_row("pass", "code", "the source's own functions and views"
                                     " call nothing the new version took"
                                     " away")]
    return [_row("fail", "code",
                  f"{len(found)} call what the new version no longer has ("
                  + ", ".join(gone[:4]) + (" ..." if len(gone) > 4 else "")
                  + "): " + ", ".join(found[:6]),
                  "change each to the new name before the cutover; the"
                  " target refuses to create it as it is")]


def _pg_defaults_elsewhere(eng, db, old, nw, seeded):
    """What changed by default outside `pg_settings`: who may create in
    the public schema (15), the search path maintenance runs with (17),
    and passwords hashed the old way (14)."""
    out = []
    if old < (15,) <= nw and not seeded:
        q = "select has_schema_privilege('public', 'public', 'CREATE')"
        try:
            src = _pg(eng, "src", db, q)[0][0]
            dst = _pg(eng, "dst", db, q)[0][0]
        except Exception:  # noqa: BLE001 - asked, and not known
            src = dst = None
        if src == "t" and dst == "f":
            out.append(_row(
                "warn", "defaults the new version changed",
                "on the source anyone may create in the public schema; a"
                " database made by the new version lets only its owner",
                "grant create on schema public to the roles that create"
                " there, in each target database"))
    if old < (17,) <= nw:
        rows = _pg(eng, "src", db, """
            select distinct n.nspname || '.' || p.proname
              from pg_depend d
              join pg_proc p on p.oid = d.refobjid
              join pg_namespace n on n.oid = p.pronamespace
             where d.classid = 'pg_class'::regclass
               and d.refclassid = 'pg_proc'::regclass
               and d.objid in (select indexrelid from pg_index
                               union select oid from pg_class
                                where relkind = 'm')
               and n.nspname not in ('pg_catalog', 'information_schema')
               and not exists (select 1 from unnest(p.proconfig) c
                                where c like 'search_path=%')""")
        found = [r[0] for r in rows if r]
        if found:
            out.append(_row(
                "warn", "code",
                f"{len(found)} functions an index or a materialized view"
                " runs have no search path of their own: "
                + ", ".join(found[:6]) + " - from 17 maintenance runs them"
                " with pg_catalog and pg_temp alone, and one that names a"
                " table without its schema fails in REINDEX, VACUUM and"
                " REFRESH", "ALTER FUNCTION ... SET search_path = <its"
                " schemas> for each, on the source before the move"))
    if old < (14,) <= nw:
        try:
            n = int(_pg(eng, "src", db, "select count(*) from pg_authid"
                                        " where rolpassword like 'md5%'")
                    [0][0])
        except Exception:  # noqa: BLE001 - only a superuser may ask
            n = None
        if n:
            out.append(_row(
                "warn", "defaults the new version changed",
                f"{n} roles keep passwords hashed as md5, and the new"
                " version hashes with scram-sha-256: a server whose"
                " pg_hba.conf asks for scram-sha-256 refuses them",
                "set those roles' passwords again once the target hashes"
                " with scram-sha-256"))
    return out


def pg_collations(eng, side, db):
    """{collation: the version its library says now} of the collations
    the database's columns use, and its default's."""
    rows = _pg(eng, side, db, """
        select c.collname || chr(31)
               || coalesce(pg_collation_actual_version(c.oid), '')
          from pg_collation c
         where c.collprovider <> 'd'
           and (c.oid in (select distinct a.attcollation
                            from pg_attribute a
                            join pg_class t on t.oid = a.attrelid
                            join pg_namespace n on n.oid = t.relnamespace
                           where a.attnum > 0 and a.attcollation <> 0
                             and n.nspname not in ('pg_catalog',
                                                   'information_schema'))
                or c.collname = (select datcollate from pg_database
                                  where datname = current_database()))""")
    return {r[0]: r[1] for r in rows if len(r) == 2 and r[1]}


def _pg_collations(eng, db, seeded):
    a, b = pg_collations(eng, "src", db), pg_collations(eng, "dst", db)
    moved = [f"{c} ({a[c]} on the source, {b[c]} on the target)"
             for c in sorted(a) if c in b and a[c] != b[c]]
    if not moved:
        return _row("pass", "collations",
                    f"{len(a)} used, the same library version on both"
                    " sides" if a else "none but C, which never changes")
    return _row("warn", "collations",
                "text sorts otherwise on the target: " + "; ".join(moved[:6]),
                "migkit rebuilds the target's indexes on those columns"
                " after a copy of the pages; text ordered by them may come"
                " back in another order - check what the application sorts"
                if seeded else
                "the target builds its indexes itself; text ordered by"
                " those collations may come back in another order - check"
                " what the application sorts")


# --- MySQL -------------------------------------------------------------------------

#: a server's own, which two servers always hold apart
MY_PER_SERVER = ("server_id", "server_uuid", "hostname", "datadir", "basedir",
                 "pid_file", "socket", "port", "log_error", "general_log_file",
                 "slow_query_log_file", "tmpdir", "version",
                 "version_comment", "version_compile_os",
                 "version_compile_machine", "version_compile_zlib",
                 "innodb_version", "protocol_version", "report_host",
                 "character_sets_dir", "plugin_dir", "lc_messages_dir",
                 "relay_log", "relay_log_index", "log_bin_basename",
                 "log_bin_index", "relay_log_basename", "ssl_ca", "ssl_cert",
                 "ssl_key", "gtid_executed", "gtid_purged", "server_id_bits",
                 "innodb_buffer_pool_size", "innodb_log_group_home_dir",
                 "innodb_data_home_dir", "secure_file_priv", "admin_address",
                 "mysqlx_socket", "mysqlx_port", "large_files_support")


def _my(eng, side, sql, args=None):
    e = _side(eng, side)
    return e._q(side, sql, args) if args is not None else e._q(side, sql)


def _my_vars(eng, side):
    """{name: (value, compiled default or None)}: `variables_info` says
    which a server holds at its default (8.0 on); before it, None."""
    try:
        rows = _my(eng, side, "select g.variable_name, g.variable_value,"
                              " i.variable_source from performance_schema"
                              ".global_variables g join performance_schema"
                              ".variables_info i using (variable_name)")
        return {str(n).lower(): (str(v), str(s) == "COMPILED")
                for n, v, s in rows}
    except Exception:  # noqa: BLE001 - a server before 8.0
        rows = _my(eng, side, "select variable_name, variable_value from"
                              " performance_schema.global_variables")
        return {str(n).lower(): (str(v), None) for n, v in rows}


def _my_checks(hop, eng, db, src_v, new, seeded, reached):
    """The checks of a MySQL upgrade, each a callable, as `_pg_checks`."""
    old, nw = major(src_v), major(new)
    out = []
    if seeded and not _my_jump_ok(old, nw):
        out.append(lambda: [_row(
            "fail", "versions",
            f"MySQL upgrades in place one release series at a time (5.7 to"
            f" 8.0, 8.0 to 8.4, 8.4 to 9), and this is {src_v} to {new}",
            "move logically into a target that already runs the new"
            " version, or upgrade the copy in steps")])
    if not seeded:
        out += [lambda: _my_settings(eng),
                lambda: _my_reserved(eng, db)]
    out += [lambda: _my_code(eng, db, old, nw),
            lambda: _my_auth(eng, nw),
            lambda: checker(hop, eng, db, new)]
    return out


def _my_settings(eng):
    """Removed and changed defaults, from both servers' own variables and
    which of them each holds at its compiled default (`_my_vars`)."""
    src, dst = _my_vars(eng, "src"), _my_vars(eng, "dst")
    gone, changed = [], []
    for name, (value, default) in sorted(src.items()):
        if name in MY_PER_SERVER:
            continue
        if name not in dst:
            if default is not True:
                instead = REMOVED_SETTINGS["mysql"].get(name)
                gone.append(f"{name} = {value}"
                            + (f" (now {instead})" if instead else ""))
            continue
        there, their_default = dst[name]
        if default and their_default and value != there:
            changed.append(f"{name}: {value} by default before, {there} now")
    out = []
    if gone:
        out.append(_row("warn", "settings the new version does not have",
                        f"{len(gone)}: " + "; ".join(gone[:8])
                        + (" ..." if len(gone) > 8 else ""),
                        "leave them out of the target's configuration - a"
                        " server that reads an option it does not know does"
                        " not start - and set what replaced each"))
    if changed:
        out.append(_row("warn", "defaults the new version changed",
                        f"{len(changed)} the source leaves at their"
                        " default: " + "; ".join(changed[:8])
                        + (" ..." if len(changed) > 8 else ""),
                        "set each on the target to the source's value to"
                        " keep its behaviour, or keep the new default once"
                        " the source's statements are measured on it"))
    if not gone and not changed:
        out.append(_row("pass", "settings",
                        f"{len(src)} compared: none the source sets is gone,"
                        " no default it relies on changed"))
    return out


def _my_jump_ok(old, new):
    if old == new:
        return True
    return (old, new) in (((5, 7), (8, 0)), ((8, 0), (8, 4))) or (
        old == (8, 0) and new[:1] == (8,) and new <= (8, 4)) or (
        old == (8, 4) and new[:1] == (9,))


def _my_reserved(eng, db):
    """Names the source's objects carry that the new version reserves."""
    try:
        new = {str(w).upper() for (w,) in _my(
            eng, "dst", "select word from information_schema.keywords"
                        " where reserved = 1")}
    except Exception:  # noqa: BLE001 - a target before 8.0 says nothing
        return []
    try:
        was = {str(w).upper() for (w,) in _my(
            eng, "src", "select word from information_schema.keywords"
                        " where reserved = 1")}
    except Exception:  # noqa: BLE001 - a source before 8.0
        was = set()
    name = _side(eng, "src")._d("src", db)
    rows = _my(eng, "src", """
        select table_name from information_schema.tables
         where table_schema = %s
        union select column_name from information_schema.columns
         where table_schema = %s
        union select routine_name from information_schema.routines
         where routine_schema = %s
        union select trigger_name from information_schema.triggers
         where trigger_schema = %s""", (name, name, name, name))
    hit = sorted({str(n) for (n,) in rows if str(n).upper() in new - was})
    if not hit:
        return [_row("pass", "reserved words",
                     "none of the source's names is a word the new version"
                     " reserves")]
    return [_row("warn", "reserved words",
                 f"{len(hit)} names are words the new version reserves: "
                 + ", ".join(hit[:10]),
                 "quote each in backticks wherever the application's"
                 " statements name it, or rename it")]


def _my_code(eng, db, old, nw):
    gone = _removed_between("mysql", old, nw)
    if not gone:
        return []
    name = _side(eng, "src")._d("src", db)
    rows = _my(eng, "src", """
        select concat('routine ', routine_name), routine_definition
          from information_schema.routines where routine_schema = %s
        union all
        select concat('view ', table_name), view_definition
          from information_schema.views where table_schema = %s
        union all
        select concat('trigger ', trigger_name), action_statement
          from information_schema.triggers where trigger_schema = %s""",
               (name, name, name))
    pattern = re.compile(r"\b(" + "|".join(gone) + r")\s*\(", re.I)
    found = [str(n) for n, body in rows if pattern.search(str(body or ""))]
    if not found:
        return [_row("pass", "code", "the source's own routines, views and"
                                     " triggers call nothing the new"
                                     " version took away")]
    return [_row("fail", "code", f"{len(found)} call what the new version"
                                 " no longer has: " + ", ".join(found[:6]),
                 "change each before the cutover; the target refuses to"
                 " create it as it is")]


def _my_auth(eng, nw):
    try:
        rows = _my(eng, "src", "select user, host from mysql.user where"
                               " plugin = 'mysql_native_password'")
    except Exception:  # noqa: BLE001 - not every user may read it
        return []
    users = [f"'{u}'@'{h}'" for u, h in rows]
    if not users or nw < (8, 4):
        return []
    gone = nw >= (9, 0)
    return [_row("fail" if gone else "warn", "sign-in",
                 f"{len(users)} users sign in with mysql_native_password,"
                 + (" which the new version does not have" if gone else
                    " which 8.4 turns off by default") + ": "
                 + ", ".join(users[:6]),
                 "ALTER USER ... IDENTIFIED WITH caching_sha2_password BY"
                 " '<password>' for each (their clients must speak it)"
                 + ("" if gone else ", or turn mysql_native_password on"
                    " on the target"))]


def checker(hop, eng, db, new):
    """MySQL's own upgrade checker, run against the source for the new
    version where the build here has it: each problem it finds as a row,
    in migkit's words. [] where it is not installed."""
    from . import tools
    from .movers import DRIVEN
    from .wording import without_programs
    if not tools.which("mysqlsh"):
        return []
    ep = hop.source
    js = ("util.checkForServerUpgrade(null, {targetVersion: "
          f"{json.dumps(str(new))}, outputFormat: 'JSON'}})")
    import subprocess
    try:
        got = subprocess.run(
            ["mysqlsh", "--js", "--passwords-from-stdin", "--uri",
             f"{ep.user}@{ep.host}:{ep.port}", "-e", js],
            input=f"{ep.password}\n", capture_output=True, text=True,
            timeout=600)
    except (OSError, subprocess.TimeoutExpired) as e:
        return [_row("warn", "the version's own checks",
                     f"could not be run: {_last(e)}")]
    text = got.stdout or ""
    try:
        report = json.loads(text[text.index("{"):text.rindex("}") + 1])
    except ValueError:
        return [_row("warn", "the version's own checks",
                     "gave nothing to read: " + without_programs(
                         _last(got.stderr or text or "no output"), DRIVEN))]
    out = []
    for check in report.get("checksPerformed") or []:
        for p in check.get("detectedProblems") or []:
            level = {"error": "fail", "warning": "warn"}.get(
                str(p.get("level", "")).lower(), "pass")
            said = " ".join(x for x in (str(p.get("dbObject") or ""),
                                        str(p.get("description") or ""))
                            if x)
            out.append(_row(level, str(check.get("title") or check.get(
                "id") or "the version's own checks"),
                without_programs(said, DRIVEN),
                without_programs(str(check.get("description") or ""),
                                 DRIVEN)))
    if not out:
        out.append(_row("pass", "the version's own checks",
                        f"{len(report.get('checksPerformed') or [])} run"
                        " against the source for the new version, nothing"
                        " found"))
    return out


# --- MongoDB and SQL Server -----------------------------------------------------

def _mongo_checks(hop, eng, db, src_v, new, seeded, reached):
    return [lambda: _mongo(hop, eng, db, new)]


def _mongo(hop, eng, db, new):
    e = _side(eng, "src")
    client = e._client("src")
    out = []
    try:
        fcv = client.admin.command({"getParameter": 1,
                                    "featureCompatibilityVersion": 1})
        fcv = (fcv.get("featureCompatibilityVersion") or {}).get("version")
    except Exception:  # noqa: BLE001 - not every user may ask
        fcv = None
    if fcv:
        out.append(_row("pass", "compatibility", f"the source's feature"
                                                 f" compatibility is {fcv}"))
    if major(new) >= (5, 0):
        haystack = []
        d = client[e._d("src", db)]
        for name in d.list_collection_names():
            if name.startswith("system."):
                continue
            for ix in d[name].list_indexes():
                if "geoHaystack" in [str(v) for v in
                                     dict(ix.get("key") or {}).values()]:
                    haystack.append(f"{name}.{ix.get('name')}")
        if haystack:
            out.append(_row("fail", "indexes",
                            "geoHaystack indexes, which the new version does"
                            " not have: " + ", ".join(haystack[:6]),
                            "replace each with a 2d index on the source, or"
                            " leave it to be made anew on the target"))
    return out or [_row("pass", "what the new version changes",
                        "nothing found that the new version refuses")]


def _mssql_checks(hop, eng, db, src_v, new, seeded, reached):
    return [lambda: _mssql(hop, eng, db, new)]


def _mssql(hop, eng, db, new):
    e = _side(eng, "src")
    out = []
    (level,) = e._rows("src", db, "select compatibility_level from"
                                  " sys.databases where name = db_name()")[0]
    own = major(new)[0] * 10
    if int(level) < own:
        out.append(_row("warn", "compatibility level",
                        f"the database runs at compatibility level {level};"
                        f" one made on the new version runs at {own}, with"
                        " another cardinality estimator",
                        f"set it on the target to {level} first (ALTER"
                        " DATABASE ... SET COMPATIBILITY_LEVEL ="
                        f" {level}), and raise it once the source's"
                        " statements are measured on both"))
    try:
        used = e._rows("src", db, "select rtrim(instance_name), cntr_value"
                                  " from sys.dm_os_performance_counters"
                                  " where object_name like '%Deprecated"
                                  " Features%' and cntr_value > 0")
    except Exception:  # noqa: BLE001 - VIEW SERVER STATE is not given
        used = []
    if used:
        out.append(_row("warn", "deprecated features",
                        f"{len(used)} deprecated features used since the"
                        " source started: " + ", ".join(
                            f"{n} ({int(c):,})" for n, c in used[:6]),
                        "each may be gone in a later version: change what"
                        " uses it before the one after this"))
    return out or [_row("pass", "what the new version changes",
                        "nothing found that the new version refuses")]


# --- the way -----------------------------------------------------------------------

SEEDED = "a copy of the pages upgraded on the target"
LOGICAL = "a logical move into the new version"


def _facts(hop, eng, db):
    from . import physical

    def seed():
        rung, why = physical.choose(hop, eng, "full+cdc")
        return rung, why

    def have():
        return versions(hop, eng, db)
    return decide.Facts(seed=seed, versions=have,
                        settings=lambda: settings(hop) or {})


def ways():
    """The two ways, as the decision engine reads them."""
    return (
        decide.Rung(
            "seeded", SEEDED, footprint=2,
            needs=(decide.Need(
                "a copy of the pages the hop allows, upgraded as it says",
                lambda f: f["seed"][0] is not None and bool(f["settings"]),
                lambda f: ("the hop does not say how the target is upgraded"
                           " after a copy of the pages (physical: {upgrade:"
                           " {...}})" if not f["settings"] else
                           f"a copy of the pages is not taken: "
                           f"{f['seed'][1] or 'the hop allows none'}")),),
            because="the pages arrive with their indexes built and nothing"
                    " decoded, and the upgrade runs on the target"),
        decide.Rung(
            "logical", LOGICAL, footprint=1,
            needs=(decide.Need(
                "a target that runs a newer version than the source",
                lambda f: bool(f["versions"][0] and f["versions"][1])
                and major(f["versions"][1]) > major(f["versions"][0]),
                lambda f: (f"the target runs {f['versions'][1] or '?'} and"
                           f" the source {f['versions'][0] or '?'}; a"
                           " logical move goes into a server that already"
                           " runs the new version")),),
            because="the target already runs the new version"),
    )


def costs(hop):
    """Seconds a row each way took on this hop or its rehearsal: the
    physical rungs' runs for the copy of the pages, the fastest logical
    copy's for the logical move (`physical.costs`)."""
    from . import physical
    got = physical.costs(hop)
    out = {}
    seeds = [v for k, v in got.items() if k != physical.LOGICAL.name]
    if seeds:
        out["seeded"] = min(seeds)
    if physical.LOGICAL.name in got:
        out["logical"] = got[physical.LOGICAL.name]
    return out


def choose(hop, eng, db):
    """The `decide.Choice` of the way this upgrade goes."""
    return decide.climb("the upgrade", ways(), _facts(hop, eng, db),
                        costs=costs(hop))


def in_place_line(hop):
    """What an upgrade in place on the source itself would cost in
    downtime, from the in-place step measured on this hop's target or its
    rehearsal's - or ""."""
    from . import planner
    runs = planner.runs(hop, IN_PLACE)
    whose = ""
    if not runs:
        got, names = planner._elsewhere(hop, IN_PLACE)
        runs, whose = got, ", ".join(names[:1])
    if not runs:
        return ""
    last = runs[-1]
    return (f"the same upgrade in place on the source would stop it for"
            f" ~{planner._took(last['seconds'])} (the upgrade's own step,"
            f" measured on the target{' of ' + whose if whose else ''},"
            f" {str(last.get('at', ''))[:10]}); moved and followed, the"
            " downtime is the cutover's alone")


def plan_lines(hop, eng, db):
    """The plan's lines of an upgrade: the way and why, and what the
    in-place upgrade of the source would cost."""
    got = choose(hop, eng, db)
    out = [f"upgrade: {got.said} - {got.reason}"]
    for rung, why in got.passed:
        out.append(f"  not {rung.said}: {why}")
    said = in_place_line(hop)
    if said:
        out.append(f"  {said}")
    return out


# --- the step on the target ------------------------------------------------------

def _took_step(hop, eng, dbs, began):
    from . import physical, planner
    took = time.time() - began
    planner.record_rate(hop, IN_PLACE, max(physical._rows(eng, dbs), 1),
                        took)
    return took


#: how the new version's own upgrade carries the data directory, fastest
#: first: files linked, not copied (the copy it runs on is the target's
#: own, used by nothing else); cloned where the file system clones; copied
LINK = decide.Rung("link", "the files linked into the new version's"
                           " directory")
CLONE = decide.Rung("clone", "the files cloned by the file system")
COPY = decide.Rung("copy", "the files copied")


#: the programs the in-place step runs, kept out of what an operator reads
PROGRAMS = ("pg_upgrade", "initdb")
#: what names a place in a configuration file, which the new data
#: directory's own says
PLACES = ("data_directory", "config_file", "hba_file", "ident_file",
          "external_pid_file")


def _quiet(text):
    from . import physical
    from .movers import DRIVEN
    from .wording import without_programs
    return without_programs(str(text), tuple(DRIVEN) + physical.PROGRAMS
                            + PROGRAMS)


def _pg_initdb(hop, eng, new):
    """The command line that makes the new version's data directory, where
    it is not there yet, as the source's was made - its encoding, locale
    and locale provider, its page checksums, the same first user - with
    the old directory's sign-in rules and every setting of its
    configuration the new version knows (`postgres --describe-config`)
    carried into it: the new version's own upgrade carries the data and
    none of the configuration."""
    q = shlex.quote
    row = eng._psql("src", "postgres",
                    "select pg_encoding_to_char(encoding) || chr(31) ||"
                    " datcollate || chr(31) || datctype || chr(31) ||"
                    " (select rolname from pg_roles where oid = 10) ||"
                    " chr(31) || current_setting('data_checksums') || chr(31)"
                    " || current_setting('server_version_num') from"
                    " pg_database where datname = 'template1'")
    enc, collate, ctype, first, sums, num = (row.split("\x1f")
                                             + [""] * 6)[:6]
    flags = ["-U", q(first), "--encoding", q(enc), "--lc-collate",
             q(collate), "--lc-ctype", q(ctype)]
    if int(num or 0) >= 150000:
        column = "datlocale" if int(num) >= 170000 else "daticulocale"
        got = eng._psql("src", "postgres",
                        f"select datlocprovider || chr(31) ||"
                        f" coalesce({column}, '') from pg_database where"
                        " datname = 'template1'")
        provider, _, locale = got.partition("\x1f")
        if provider == "i":
            flags += ["--locale-provider", "icu", "--icu-locale",
                      q(locale)]
        elif provider == "b":
            flags += ["--locale-provider", "builtin", "--builtin-locale",
                      q(locale)]
    if sums == "on":
        flags.append("--data-checksums")
    elif major(new) >= (18,):
        # the new version checks pages by default, and its upgrade takes
        # only a directory made as the old one was
        flags.append("--no-data-checksums")
    places = "|".join(PLACES)
    made = " ".join(['"$NB/initdb"', "--pgdata", '"$N"'] + flags)
    return (' if [ ! -f "$N/PG_VERSION" ]; then mkdir -p "$N"; '
            + made + ' >/dev/null;'
            ' for f in pg_hba.conf pg_ident.conf; do'
            ' if [ -f "$O/$f" ]; then cp "$O/$f" "$N/$f"; fi; done;'
            ' "$NB/postgres" --describe-config | cut -f1 > "$W/known";'
            ' for f in postgresql.conf postgresql.auto.conf; do'
            ' if [ -f "$O/$f" ]; then awk -v skip="^(' + places + ')$"'
            " 'NR==FNR{k[tolower($1)]=1;next} {l=$0; sub(/^[ \\t]+/,\"\",l)}"
            " l ~ /^[A-Za-z_.]+[ \\t]*=/ {n=l; sub(/[ \\t]*=.*/,\"\",n);"
            " n=tolower(n); if ((n in k) && n !~ skip) print $0}'"
            ' "$W/known" "$O/$f" >> "$N/postgresql.conf"; fi; done; fi;')


def pg_in_place(hop, eng, rec, dbs, log):
    """The target, a copy of the source's pages at its version, upgraded in
    place by the new version's own upgrade: checked first with the way its
    files are carried chosen (`LINK`, `CLONE`, `COPY`, each proved by the
    upgrade's own check), the old server stopped, upgraded, the new one
    started. Recorded in `rec`; timed."""
    from . import physical
    up = settings(hop) or {}
    opts = physical.settings(hop) or {}
    lack = [k for k in ("bin", "old_bin", "data", "start") if not up.get(k)]
    if lack:
        raise SystemExit(
            "the copy of the pages is at the source's version and the hop"
            " does not say how to upgrade it: physical: {upgrade: {...}}"
            f" needs {', '.join(lack)} - the new version's programs, the"
            " old one's, the new data directory and how its server"
            " starts")
    new = new_version(hop, eng, dbs[0] if dbs else "")
    q = shlex.quote
    # each way of carrying the files, as the new version's upgrade is told
    file_ways = {"link": "--link", "clone": "--clone", "copy": "--copy"}
    head = ("set -e; W=$(mktemp -d); cd \"$W\";"
            f" O={q(str(opts['data']))}; N={q(str(up['data']))};"
            f" OB={q(str(up['old_bin']))}; NB={q(str(up['bin']))};"
            + _pg_initdb(hop, eng, new or ""))
    first = eng._psql("src", "postgres",
                      "select rolname from pg_roles where oid = 10").strip()
    base = " ".join(['"$NB/pg_upgrade"', "--old-bindir", '"$OB"',
                     "--new-bindir", '"$NB"', "--old-datadir", '"$O"',
                     "--new-datadir", '"$N"', "--username", q(first)])

    def proved(way):
        def prove(_facts):
            got = physical._on_target(
                opts, head + " " + base + " " + " ".join(
                    ["--check", file_ways[way]]))
            if got.returncode == 0:
                return None
            return _quiet(physical._tail_of(got, 3))
        return prove
    ladder = [decide.Rung(r.name, r.said, prove=proved(r.name))
              for r in (LINK, CLONE, COPY)]
    # stopped first: the check then starts the old server itself, where a
    # server still running beside it is one it would have to reach
    physical._local(opts["stop"], "stop")
    got = decide.climb("the upgrade's files", ladder,
                       costs=decide.HopCosts(hop, "upgrade files "))
    if got.rung is None:
        raise SystemExit(
            "the new version's own upgrade refuses this copy, and nothing"
            " was changed on the source; the target's old server is left"
            " stopped with the copy as it was: " + _quiet(got.reason))
    log(f"the new version's own check passed; {got.rung.said} ({got.reason})")
    began = time.time()
    ran = physical._on_target(opts, head + " " + base + " " + " ".join(
        [file_ways[got.rung.name], "--jobs",
         '"$(nproc 2>/dev/null || echo 2)"']))
    if ran.returncode != 0:
        raise SystemExit(
            "the new version's own upgrade failed on the target, and the"
            " source is as it was; the target's old server is left stopped"
            " with the copy as it was. What it said: "
            + _quiet(physical._tail_of(ran)))
    physical._local(up["start"], "upgrade start")
    physical._wait_for(lambda: eng._psql("dst", "postgres", "select 1"),
                       "the target's new server")
    took = _took_step(hop, eng, dbs, began)
    decide.HopCosts(hop, "upgrade files ").saw(got.rung.name, 1, took)
    now = eng._psql("dst", "postgres",
                    "select current_setting('server_version') || chr(31) ||"
                    " system_identifier from pg_control_system()")
    version, _, system = now.partition("\x1f")
    if new and major(version) != major(new):
        raise SystemExit(f"the target's server answers {version} after the"
                         f" upgrade, and the new version is {new}")
    rec["upgraded"] = {"version": version, "system": system.strip(),
                       "seconds": round(took, 1), "files": got.rung.name,
                       "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    physical._save_seed(hop, rec)
    log(f"the target runs {version} after {took:,.0f}s of the upgrade's own"
        " step - what an upgrade in place on the source would stop it for")
    after(hop, eng, dbs, log)


def my_in_place(hop, eng, rec, dbs, log):
    """The target, the source's clone at its version, stopped cleanly and
    started by the new version's server on the same data, which upgrades
    it as it starts; the clone's position stands. Recorded; timed."""
    from . import physical
    up = settings(hop) or {}
    lack = [k for k in ("stop", "start") if not up.get(k)]
    if lack:
        raise SystemExit(
            "the clone is at the source's version and the hop does not say"
            " how to upgrade it: physical: {upgrade: {...}} needs "
            + ", ".join(lack) + " - how the old server stops and how the"
            " new version's server starts on the same data")
    began = time.time()
    # a clean, slow shutdown: the new version reads the files as they are
    eng._q("dst", "SET GLOBAL innodb_fast_shutdown = 0")
    physical._local(up["stop"], "upgrade stop")
    physical._local(up["start"], "upgrade start")
    version = physical._wait_for(
        lambda: str(eng._q("dst", "select @@version")[0][0]),
        "the target's new server")
    took = _took_step(hop, eng, dbs, began)
    new = up.get("version")
    if new and major(version) != major(new):
        raise SystemExit(f"the target's server answers {version} after the"
                         f" upgrade, and the new version is {new}")
    rec["upgraded"] = {"version": version, "seconds": round(took, 1),
                       "uuid": str(eng._q("dst", "select @@server_uuid")
                                   [0][0]),
                       "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    physical._save_seed(hop, rec)
    log(f"the target runs {version} after {took:,.0f}s of the upgrade's own"
        " step - what an upgrade in place on the source would stop it for")


def cloud_in_place(hop, eng, rec, cloud, dbs, log):
    """The copy a cloud made, upgraded by the same cloud's API to the
    version the hop names; the copy's position stands. Recorded; timed."""
    from . import physical
    up = settings(hop) or {}
    if not up.get("version"):
        raise SystemExit(
            "the cloud's copy is at the source's version and the hop does"
            " not name the version to upgrade it to: physical: {upgrade:"
            " {version: ...}}")
    began = time.time()
    cloud.upgrade(rec["target"], str(up["version"]), up, log)
    took = _took_step(hop, eng, dbs, began)
    rec["upgraded"] = {"version": str(up["version"]),
                       "seconds": round(took, 1),
                       "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    physical._save_seed(hop, rec)
    log(f"{rec['target'].get('id')} runs {up['version']} after"
        f" {took:,.0f}s of the cloud's own upgrade")
    if physical._engine_of(hop) == "postgres":
        after(hop, eng, dbs, log)


def after(hop, eng, dbs, log):
    """What an upgrade in place leaves for the target, done there before
    anything is applied: each extension updated to the new server's
    version, and every index on text sorted by a library whose version
    moved under it rebuilt, then that version recorded."""
    for d in dbs:
        for (ext,) in _pg(eng, "dst", d, """
                select e.extname from pg_extension e
                  join pg_available_extensions a on a.name = e.extname
                 where a.default_version is not null
                   and a.default_version <> e.extversion"""):
            eng._psql("dst", d, f'alter extension "{ext}" update')
            log(f"{d}: extension {ext} updated for the new version")
        rows = _pg(eng, "dst", d, """
            select distinct format('%I.%I', n.nspname, i.relname)
              from pg_index x
              join pg_class i on i.oid = x.indexrelid
              join pg_namespace n on n.oid = i.relnamespace
              join pg_collation c on c.oid = any(x.indcollation::oid[])
             where n.nspname not in ('pg_catalog', 'information_schema')
               and c.collversion is not null
               and c.collversion
                   is distinct from pg_collation_actual_version(c.oid)""")
        moved = [r[0] for r in rows if r]
        for index in moved:
            eng._psql("dst", d, f"reindex index {index}")
        if moved:
            log(f"{d}: {len(moved)} indexes on text rebuilt - the"
                " collation library under them is another version")
            for (coll,) in _pg(eng, "dst", d, """
                    select format('%I.%I', n.nspname, c.collname)
                      from pg_collation c
                      join pg_namespace n on n.oid = c.collnamespace
                     where c.collversion is not null
                       and c.collversion is distinct from
                           pg_collation_actual_version(c.oid)"""):
                eng._psql("dst", d, f"alter collation {coll} refresh"
                                    " version")
        _database_collation(eng, d, log)


def _database_collation(eng, d, log):
    """The database's own default collation (recorded from 15 on): where
    its library moved, every index sorted by the default rebuilt, then the
    version recorded."""
    try:
        moved = _pg(eng, "dst", d, """
            select (datcollversion is distinct from
                    pg_database_collation_actual_version(oid))::int
                   || chr(31) || quote_ident(datname)
              from pg_database
             where datname = current_database()
               and datcollversion is not null""")
    except Exception:  # noqa: BLE001 - before 15 the database keeps none
        return
    if not moved or moved[0][0] != "1":
        return
    rows = _pg(eng, "dst", d, """
        select distinct format('%I.%I', n.nspname, i.relname)
          from pg_index x
          join pg_class i on i.oid = x.indexrelid
          join pg_namespace n on n.oid = i.relnamespace
         where n.nspname not in ('pg_catalog', 'information_schema')
           and 100 = any(x.indcollation::oid[])""")
    for (index,) in rows:
        eng._psql("dst", d, f"reindex index {index}")
    eng._psql("dst", d, f"alter database {moved[0][1]} refresh collation"
                        " version")
    log(f"{d}: {len(rows)} indexes sorted by the database's default"
        " collation rebuilt - its library is another version here")


def upgraded(rec):
    """Whether the seed `rec` records was upgraded already."""
    return bool((rec or {}).get("upgraded"))


# --- the tools it drives ----------------------------------------------------------

def _account():
    from .tooluse import UNUSED
    return {
        ("", "--check"): None, ("", "--link"): None, ("", "--clone"): None,
        ("", "--copy"): None, ("", "--jobs"): None,
        ("", "--old-bindir"): None, ("", "--new-bindir"): None,
        ("", "--old-datadir"): None, ("", "--new-datadir"): None,
        ("", "--username"): None,
        ("", "--copy-file-range"): (
            UNUSED, "not yet: a fourth way of carrying the files, tried"
                    " after the three the ladder has"),
        ("", "--swap"): (UNUSED, "not yet: the newest way of carrying the"
                                 " files, from version 18 only"),
        ("", "--retain"): (UNUSED, "its logs are read from its output and"
                                   " its working directory is its own"),
        ("", "--old-options"): (UNUSED, "the old server is started as its"
                                        " configuration says"),
        ("", "--new-options"): (UNUSED, "the new server is started as its"
                                        " configuration says"),
        ("", "--old-port"): (UNUSED, "its own port is free on the target's"
                                     " host, where nothing else runs"),
        ("", "--new-port"): (UNUSED, "its own port is free on the target's"
                                     " host, where nothing else runs"),
        ("", "--socketdir"): (UNUSED, "its working directory is a new one"
                                      " each run, where its sockets go"),
        ("", "--no-sync"): (UNUSED, "a directory not written to disk may"
                                    " be lost with the host, and the copy"
                                    " would be made again"),
        ("", "--verbose"): (UNUSED, "its failure is said from the last"
                                    " lines it prints"),
        ("", "--no-statistics"): (UNUSED, "the statistics it carries from"
                                          " version 18 are kept; the target"
                                          " is analysed after in any case"),
        ("", "--set-char-signedness"): (UNUSED, "the copy and the target"
                                                " run on one platform")}


def _initdb_account():
    from .tooluse import UNUSED
    return {
        ("", "--pgdata"): None, ("", "-U"): None, ("", "--encoding"): None,
        ("", "--lc-collate"): None, ("", "--lc-ctype"): None,
        ("", "--locale-provider"): None, ("", "--icu-locale"): None,
        ("", "--builtin-locale"): None, ("", "--data-checksums"): None,
        ("", "--no-data-checksums"): None,
        ("", "--locale"): (UNUSED, "the collation and the character type"
                                   " are given one by one, as the source"
                                   " has each"),
        ("", "--lc-messages"): (UNUSED, "a message's language changes"
                                        " nothing an upgrade carries"),
        ("", "--lc-monetary"): (UNUSED, "set by the configuration carried"
                                        " from the old directory"),
        ("", "--lc-numeric"): (UNUSED, "set by the configuration carried"
                                       " from the old directory"),
        ("", "--lc-time"): (UNUSED, "set by the configuration carried from"
                                    " the old directory"),
        ("", "--auth"): (UNUSED, "the old directory's sign-in rules are"
                                 " carried whole"),
        ("", "--auth-host"): (UNUSED, "see --auth"),
        ("", "--auth-local"): (UNUSED, "see --auth"),
        ("", "--pwfile"): (UNUSED, "the old server's users come with the"
                                   " upgrade, their passwords with them"),
        ("", "--pwprompt"): (UNUSED, "see --pwfile"),
        ("", "--waldir"): (UNUSED, "the new directory keeps its log where"
                                   " the operator's layout puts it"),
        ("", "--wal-segsize"): (UNUSED, "the new version's own size, which"
                                        " its upgrade takes"),
        ("", "--allow-group-access"): (UNUSED, "the directory is read by"
                                              " its server's user alone"),
        ("", "--icu-rules"): (UNUSED, "no rules are added to the source's"
                                      " own collation"),
        ("", "--text-search-config"): (UNUSED, "the old databases' own"
                                               " come with the upgrade"),
        ("", "--set"): (UNUSED, "settings are carried from the old"
                                " directory's configuration instead"),
        ("", "--no-sync"): (UNUSED, "a directory not written to disk may be"
                                    " lost with the host"),
        ("", "--no-instructions"): (UNUSED, "its output is not read"),
        ("", "--no-clean"): (UNUSED, "a failed start leaves nothing to"
                                     " keep: the next run makes it again"),
        ("", "--debug"): (UNUSED, "its failure is said from its output"),
        ("", "--discard-caches"): (UNUSED, "a setting for testing the"
                                           " server itself")}


def _register():
    from . import tooluse
    got = _account()
    tokens = {("", o): o for (_, o), v in got.items() if v is None}
    tooluse.register(
        "pg_upgrade", licence="PostgreSQL",
        declared={"": frozenset(o for _, o in got)},
        reads=(["upgrade.py::pg_in_place"], tokens),
        options={k: v for k, v in got.items() if v is not None})
    made = _initdb_account()
    tooluse.register(
        "initdb", licence="PostgreSQL",
        declared={"": frozenset(o for _, o in made)},
        reads=(["upgrade.py::_pg_initdb"],
               {("", o): o for (_, o), v in made.items() if v is None}),
        options={k: v for k, v in made.items() if v is not None})


_register()
