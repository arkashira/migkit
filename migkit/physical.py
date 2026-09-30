"""Physical where the source allows it, logical everywhere else (backlog
R19 lever 1): the target seeded from a copy of the source's pages, then
carried on logically from the exact position that copy ends at.

A logical copy decodes every row, sends it, encodes it again and builds
every index a second time on the target. A physical copy sends the pages:
indexes arrive built, nothing is decoded, and what proves the bulk is the
pages' own checksums (PostgreSQL verifies each page as it reads it where
the cluster has checksums, and the copy is checked file by file against
its manifest before the target starts). Measured on this machine (colima,
2 CPUs, 4 GB; both servers in containers on one bridge network), the same
source each way onto an empty target, a writer inserting about 70 rows a
second on the source throughout (`bench/physical_seed.py`, 2026-09-30):

    PostgreSQL 16, 1.84 GB (9.8 M rows, 3 tables, each a key and an index)
      a copy of the pages, end to end: the target
        stopped, copied, proved, started, recovered       15.7 s
      the copy of the pages alone, sent as they are       10.0 s
      the same, compressed on the source (zstd 1)         10.1 s
      a subscription copying the rows, the schema
        made beforehand and not timed                     18.8 s
      migkit's logical copy (tables straight from
        source to target, indexes after, compared)        50.8 s
    MySQL 8.4, 1.17 GB (7.15 M rows, 3 tables, each a key and an index)
      the target cloning itself, end to end: cloned,
        restarted into it, its position proved              5.3 s
      migkit's logical copy (dump and load in parallel,
        indexes dropped and rebuilt, compared)             77.7 s

Both logical copies ended saying the writer's table differs - the rows
written while it was read, which is what `--mode full+cdc` carries; the
physical ones are exact at their position and the changes after it are
the tail's.

**Exact, not converging.** The changes after the copy are read from a
position the copy itself says it holds, never from a guess and never from
"before the copy, replayed by key":

* PostgreSQL: a logical slot is made on every database *before* the copy,
  so it holds every change the copy might not. The restored target is
  started once; crash recovery replays the WAL the copy streamed and stops
  at the last whole record, and `pg_last_wal_replay_lsn()` says where that
  was - the end of the last record the target holds. A transaction whose
  commit record starts at or after it is decoded by the slot, one that
  starts before it is not, and every record before it is in the target, so
  nothing is applied twice and nothing is missed. The subscription's origin
  is advanced to that LSN before it is enabled. The WAL the copy streamed
  runs past the backup's own end - measured on 16 under writes: the
  manifest's End-LSN 0/4004F78, the target's recovery ended at 0/5000000,
  since the backup ends with a switch of segment and the stream stops only
  after it - which is why the backup's end is a lower bound checked, not
  the position used.
* MySQL: the clone records the binlog file, position and GTID set it holds
  (`performance_schema.clone_status`); with GTIDs on, the position is
  proved before the tail trusts it - the transactions the source's log
  holds from there on share none with the clone's set, and together they
  cover everything the source had executed when asked.
* A cloud's copy (`cloudseed`): the provider's own seed position -
  `aurora_volume_logical_start_lsn()`, the restored instance's replay end,
  its GTID set - and the same continuation.

**Chosen, not assumed.** Each way is a `decide.Rung` with what it gives,
what it needs, how it is proved and what it cost on this hop (`rungs`),
climbed by `decide.climb` with the logical copy as one more rung where the
hop measured it. A rung is taken only where every need holds - the hop
allows it (`physical:` in its options), the same major version on both
sides, the privilege, an empty target, a hop that carries the whole server
(a copy of the pages cannot leave a table out, rename a database or filter
a row) - and where it proves itself, and not where this hop measured it
slower than a logical copy. A managed source that gives no physical
access (`clouds` names it) is refused, with the reason, and the cloud's
own copy named instead. A hop without `physical:` moves exactly as
before.

The option:

    options:
      physical:
        # PostgreSQL - the target's data directory is filled from the copy
        data: /var/lib/postgresql/16/main
        run: ssh postgres@10.0.0.2      # runs one shell command line where
                                        # `data` is, passing this machine's
                                        # standard input through
        stop: ssh 10.0.0.2 sudo systemctl stop postgresql@16-main
        start: ssh 10.0.0.2 sudo systemctl start postgresql@16-main
        source: 10.0.0.1:5432           # the source as the target reaches
                                        # it (default: the hop's)
        # MySQL - the target clones itself from the source
        restart: ssh 10.0.0.2 sudo systemctl restart mysqld
        source_plugin: install          # put the clone plugin on the
                                        # source, and take it off after
        # a cloud's own copy instead (`cloudseed`)
        cloud: aws
"""
import json
import os
import shlex
import subprocess
import time
from dataclasses import dataclass

from . import decide

#: the programs a physical copy runs, hidden from what an operator reads
PROGRAMS = ("pg_basebackup", "pg_verifybackup", "psql")

#: why a unit of work needs what only a physical rung gives, as
#: `decide.WORDS` says it for the capabilities every rung may give
decide.WORDS.setdefault("whole server", "the whole server is carried as it"
                                        " is, which {rung} does not do")
decide.WORDS.setdefault("indexes built", "indexes must arrive built, which"
                                         " {rung} does not do")
decide.WORDS.setdefault("pages proved", "the pages must be proved by their"
                                        " own checksums, which {rung} does"
                                        " not do")

GIVES = frozenset({"whole server", "indexes built", "exact",
                   "consistent as of", "online", "keyless", "large values"})

#: seconds a restored target is given to accept connections: recovery
#: replays what was written during the copy before it opens
START_WAIT = 600


def settings(hop):
    """The hop's `physical` option, or None where it allows no physical
    seed - and then nothing here runs and the move is what it was."""
    got = (hop.options or {}).get("physical")
    return got if isinstance(got, dict) and got else None


def _engine_of(hop):
    from .engines import ALIASES
    return ALIASES.get(hop.engine, hop.engine)


def _first_line(e):
    text = str(e).strip() or type(e).__name__
    return text.splitlines()[-1][:300]


def _quiet(text):
    from .movers import DRIVEN
    from .wording import without_programs
    return without_programs(str(text), tuple(DRIVEN) + PROGRAMS)


# --- rungs and needs, as the decision engine reads them -------------------

class Unread:
    """A fact that could not be asked. A need that reads it does not hold:
    refused with the reason, never guessed."""

    def __init__(self, why):
        self.why = why

    def __bool__(self):
        return False

    def __repr__(self):
        return f"Unread({self.why!r})"


def Need(what, reads, holds, why=None, when=None):
    """A `decide.Need` over the facts named in `reads`: any of them that
    could not be read and it does not hold, with the reading's own failure
    as the reason. `when(facts)` false and the need does not apply at all -
    and asks nothing (going on from the copy needs a change log, a copy
    alone does not)."""
    def unread(facts):
        if when is not None and not when(facts):
            return None
        for key in reads:
            got = facts[key]
            if isinstance(got, Unread):
                return got
        return None

    def ok(facts):
        if when is not None and not when(facts):
            return True
        return unread(facts) is None and bool(holds(facts))

    def said(facts):
        got = unread(facts)
        if got is not None:
            return f"{what} is needed, and {got.why}"
        text = why(facts) if callable(why) else why
        return text or f"{what} is needed"
    return decide.Need(what, ok, said)


@dataclass(frozen=True)
class Rung(decide.Rung):
    """One way of seeding the target: a `decide.Rung` - what it gives, what
    it needs (cheapest first), the probe that proves it before it is
    trusted, what it leaves behind - and the engine and the cloud it is
    for. What it cost is the hop's own number for it
    (`decide.HopCosts`), kept by `planner.record_rate` under its name."""
    engine: str = ""
    cloud: str = ""

    def refused(self, facts):
        """The first need that does not hold, in words, or None."""
        _, out = decide.prune([self], facts)
        return out[0][1] if out else None


class Facts(dict):
    """What the needs and proofs read, each asked of the servers once and
    only when a need asks: a rung already refused asks nothing more. A fact
    whose asking failed is `Unread`, which no need takes for a yes."""

    def __init__(self, hop, eng, mode):
        super().__init__()
        self.hop, self.eng, self.mode = hop, eng, mode

    def __missing__(self, key):
        try:
            got = getattr(self, "_" + key)()
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said, not hidden
            got = Unread(f"{key.replace('_', ' ')} could not be read:"
                         f" {_quiet(_first_line(e))}")
        self[key] = got
        return got

    def _options(self):
        return settings(self.hop) or {}

    def _covers(self):
        """True where a copy of the whole server is what this hop asks
        for, or the reason it is not."""
        hop = self.hop
        if hop.exclude:
            return (f"the hop excludes {', '.join(map(str, hop.exclude))},"
                    " and a copy of the pages carries every table")
        mapping = hop.mapping or {}
        if mapping.get("where"):
            return ("the hop filters rows, and a copy of the pages carries"
                    " every row")
        if mapping.get("columns") or mapping.get("tables"):
            return ("the hop maps tables or columns, and a copy of the pages"
                    " carries them under the source's names")
        renamed = {k: v for k, v in (hop.db_map or {}).items() if k != v}
        if renamed:
            pairs = ", ".join(f"{a} to {b}" for a, b in renamed.items())
            return (f"the hop renames {pairs}, and a copy of the pages"
                    " carries each database under its own name")
        # a database with tables the hop does not follow would arrive as it
        # was at the copy and never change again - the maintenance database
        # too, which a hop naming none leaves out
        have, followed = self["src_databases"], self["followed"]
        for got in (have, followed):
            if isinstance(got, Unread):
                raise RuntimeError(got.why)
        left = sorted(set(have) - set(followed))
        if left:
            return (f"the source also holds tables in {', '.join(left)},"
                    " which the hop does not name, and a copy of the pages"
                    " carries every database of the server")
        return True

    def _followed(self):
        return self.eng.databases()

    def _slots_wanted(self):
        # a logical slot for every database followed, and the copy's own
        # stream of the log for its length
        return (len(self.eng.databases()) if self.mode == "full+cdc"
                else 0) + 1

    def _target_signs_in(self):
        """Whether the hop's target user is one of the source's: the copy
        carries the source's users, and the target's own are gone."""
        return self.hop.target.user in self["src_users"]

    def _managed_src(self):
        """The managed service the source is, in words, or "" for a server
        run by its owner - read from its address and its own answers
        (`clouds.of`, the one place that tells them apart)."""
        from . import clouds
        where = clouds.of(self.eng, "src", self.hop.source)
        return where.label if clouds.managed(where) else ""

    def _our_seed(self):
        """Whether the target is the copy an earlier run of this hop made
        (`seeded`), which a move goes on from rather than refusing as a
        target that is not empty."""
        rec = seeded(self.hop)
        return bool(rec) and self._is_ours(rec)

    def _is_ours(self, rec):
        return False


def _said_count(names, most=4):
    names = list(names)
    head = ", ".join(names[:most])
    return head + (f" and {len(names) - most} more" if len(names) > most
                   else "")


def _hop_allows(said):
    return Need("the hop to allow it", ("options",),
                lambda f: bool(f["options"]) and not f["options"].get("cloud"),
                lambda f: (f"the hop asks for {f['options'].get('cloud')}'s"
                           " own copy instead" if f["options"].get("cloud")
                           else f"{said} is taken only where the hop allows"
                                " it (options: physical)"))


def _hop_says(keys, what):
    return Need(f"the hop to say {what}", ("options",),
                lambda f: all(f["options"].get(k) for k in keys),
                lambda f: ("the hop's physical option needs "
                           + ", ".join(k for k in keys
                                       if not f["options"].get(k))
                           + f" - {what}"))


_WHOLE = Need("to carry the whole server", ("covers",),
              lambda f: f["covers"] is True, lambda f: f["covers"])

_EMPTY = Need("an empty target", ("dst_tables",),
              # or the copy an earlier run of this hop made, gone on from
              lambda f: not f["dst_tables"] or f["our_seed"] is True,
              lambda f: (f"the target holds {len(f['dst_tables'])} tables"
                         f" ({_said_count(f['dst_tables'])}): a copy of the"
                         " pages replaces the whole server, so it is taken"
                         " only onto an empty one"))

_SIGNS_IN = Need("the hop's target user among the source's", (
    "src_users",), lambda f: f["target_signs_in"],
    lambda f: (f"the copy carries the source's users and none of the"
               f" target's, and {f.hop.target.user} is not one of the"
               " source's - the hop could not sign in to the target after"
               " it; sign in to the target as a user of the source's"))


# --- PostgreSQL facts --------------------------------------------------------

class PgFacts(Facts):
    def _one(self, side, db, sql):
        return self.eng._psql(side, db, sql).strip()

    def _is_ours(self, rec):
        return rec.get("rung") == "physical-pages" and _pg_is_the_seed(
            self.eng, rec)

    def _src_version(self):
        return int(self._one("src", "postgres",
                             "show server_version_num"))

    def _dst_version(self):
        return int(self._one("dst", "postgres",
                             "show server_version_num"))

    def _src_databases(self):
        """The source's databases that hold a table of the user's."""
        names = self._one("src", "postgres",
                          "select datname from pg_database where"
                          " datallowconn and not datistemplate order by 1")
        return [d for d in names.splitlines() if d and self._tables(
            "src", d)]

    def _tables(self, side, db):
        from .engines.postgres import PostgresEngine
        got = self.eng._psql(side, db, PostgresEngine.USER_TABLES)
        return [t for t in got.splitlines() if t]

    def _dst_tables(self):
        names = self._one("dst", "postgres",
                          "select datname from pg_database where"
                          " datallowconn and not datistemplate order by 1")
        out = []
        for d in names.splitlines():
            if d:
                out += [f"{d}.{t}" for t in self._tables("dst", d)]
        return out

    def _src_subscriptions(self):
        return int(self._one("src", "postgres",
                             "select count(*) from pg_subscription"))

    def _unlogged(self):
        """Unlogged tables of the databases followed: their rows are not
        in the log, so a copy of the pages carries them empty."""
        out = []
        for d in self["followed"]:
            got = self._one("src", d, "select n.nspname || '.' || c.relname"
                                      " from pg_class c join pg_namespace n"
                                      " on n.oid = c.relnamespace where"
                                      " c.relkind in ('r', 'p') and"
                                      " c.relpersistence = 'u' order by 1")
            out += [f"{d}.{t}" for t in got.splitlines() if t]
        return out

    def _tablespaces(self):
        return [t for t in self._one(
            "src", "postgres", "select spcname from pg_tablespace where"
                               " spcname not in ('pg_default', 'pg_global')"
                               " order by 1").splitlines() if t]

    def _replication_role(self):
        return self._one("src", "postgres",
                         "select rolreplication or rolsuper from pg_roles"
                         " where rolname = current_user") == "t"

    def _sender_room(self):
        return int(self._one(
            "src", "postgres",
            "select current_setting('max_wal_senders')::int"
            " - (select count(*) from pg_stat_replication)"))

    def _slot_room(self):
        return int(self._one(
            "src", "postgres",
            "select current_setting('max_replication_slots')::int"
            " - (select count(*) from pg_replication_slots)"))

    def _wal_level(self):
        return self._one("src", "postgres", "show wal_level")

    def _src_users(self):
        return set(self._one("src", "postgres",
                             "select rolname from pg_roles where rolcanlogin"
                             ).splitlines())

    def _src_system(self):
        return self._one("src", "postgres",
                         "select system_identifier from pg_control_system()")

    def _checksums(self):
        return self._one("src", "postgres", "show data_checksums")


def _pg_needs():
    said = PAGES
    return (
        _hop_allows(said),
        _hop_says(("data", "run", "stop", "start"),
                  "where the target's data directory is and how its"
                  " server is stopped and started"),
        _WHOLE,
        Need("a source that gives a replication connection",
             ("managed_src",), lambda f: not f["managed_src"],
             lambda f: (f"the source is {f['managed_src']}, which gives no"
                        " replication connection to its storage: its own"
                        " snapshot or clone is the way there (physical:"
                        " {cloud: ...})")),
        Need("the same major version on both sides",
             ("src_version", "dst_version"),
             lambda f: f["src_version"] // 10000 == f["dst_version"] // 10000,
             lambda f: (f"the source is PostgreSQL"
                        f" {f['src_version'] // 10000} and the target"
                        f" {f['dst_version'] // 10000}; pages are read only"
                        " by the major version that wrote them")),
        _EMPTY,
        Need("no unlogged table", ("unlogged",), lambda f: not f["unlogged"],
             lambda f: (f"{_said_count(f['unlogged'])} are unlogged: their"
                        " rows are not in the log, and a copy of the pages"
                        " carries them empty")),
        Need("every tablespace in the data directory", ("tablespaces",),
             lambda f: not f["tablespaces"],
             lambda f: (f"the source keeps tablespaces"
                        f" {', '.join(f['tablespaces'])} outside its data"
                        " directory, and the copy would write them to the"
                        " same paths on the target's host")),
        Need("no subscription on the source", ("src_subscriptions",),
             lambda f: not f["src_subscriptions"],
             lambda f: (f"the source subscribes to {f['src_subscriptions']}"
                        " publications, and a copy of its pages would"
                        " subscribe too - a second reader of another"
                        " server's slot, taking its changes from the"
                        " source")),
        Need("a user that may open a replication connection",
             ("replication_role",), lambda f: f["replication_role"],
             lambda f: (f"{f.hop.source.user} has neither REPLICATION nor"
                        " superuser on the source; a DBA grants it:"
                        f" alter role {f.hop.source.user} replication;")),
        Need("room for its connections on the source",
             ("sender_room", "slot_room", "slots_wanted"),
             lambda f: f["sender_room"] >= 2
             and f["slot_room"] >= f["slots_wanted"],
             lambda f: (f"the copy streams its log on a connection of its own"
                        f" and holds {f['slots_wanted']} slots; the source has"
                        f" room for {max(f['sender_room'], 0)} more"
                        f" connections (max_wal_senders) and"
                        f" {max(f['slot_room'], 0)} more slots"
                        " (max_replication_slots)")),
        Need("logical decoding on the source to go on from the copy",
             ("wal_level",), lambda f: f["wal_level"] == "logical",
             lambda f: (f"wal_level is {f['wal_level']} on the source, and"
                        " the changes after the copy are read through a"
                        " logical slot"), when=_following),
        _SIGNS_IN,
    )


def _following(facts):
    return facts.mode == "full+cdc"


def _pg_prove(facts):
    """The copy's own path tried before anything is stopped: the program
    on the target's host is the source's major version, and a replication
    connection from there reaches this very source (its system identifier
    is the source's)."""
    opts, hop = facts["options"], facts.hop
    host, port = _source_address(hop, opts)
    script = ("IFS= read -r PGPASSWORD; export PGPASSWORD;"
              " pg_basebackup --version &&"
              " psql " + shlex.quote(
                  f"host={host} port={port} user={hop.source.user}"
                  " dbname=replication replication=true connect_timeout=15")
              + " -XAtw -c IDENTIFY_SYSTEM")
    got = _on_target(opts, script, hop.source.password, timeout=120)
    if got.returncode != 0:
        return ("the target's host could not open a replication connection"
                f" to the source: {_quiet(_tail_of(got))}")
    lines = [ln for ln in got.stdout.splitlines() if ln.strip()]
    if not lines:
        return "the target's host said nothing when asked"
    import re
    ver = re.search(r"(\d+)(?:\.\d+)?", lines[0])
    major = int(ver.group(1)) if ver else None
    src = facts["src_version"]
    if major is None or major != src // 10000:
        return (f"the copy program on the target's host is version {major},"
                f" the source {src // 10000}")
    system = lines[-1].split("|")[0]
    if system != facts["src_system"]:
        return (f"the replication connection from the target's host reached"
                f" a server whose system identifier is {system}, not the"
                f" source's ({facts['src_system']}) - physical: source must"
                " name this source as the target's host reaches it")
    return None


# --- MySQL facts -------------------------------------------------------------

_MY_SYSTEM = ("mysql", "sys", "performance_schema", "information_schema")


class MyFacts(Facts):
    def _q(self, side, sql):
        return self.eng._q(side, sql)

    def _is_ours(self, rec):
        return rec.get("rung") == "physical-clone" and _my_is_the_seed(
            self.eng, rec)

    def _about(self, side):
        row = self._q(side, "select @@version, @@version_compile_os,"
                            " @@version_compile_machine, @@innodb_page_size,"
                            " @@innodb_data_file_path,"
                            " @@max_allowed_packet")[0]
        return {"version": str(row[0]), "os": str(row[1]),
                "machine": str(row[2]), "page": int(row[3]),
                "files": str(row[4]), "packet": int(row[5])}

    def _src(self):
        return self._about("src")

    def _dst(self):
        return self._about("dst")

    def _src_databases(self):
        rows = self._q("src", "select distinct table_schema from"
                              " information_schema.tables where table_type ="
                              " 'BASE TABLE' and table_schema not in"
                              " ('mysql', 'sys', 'performance_schema',"
                              " 'information_schema')")
        return sorted(str(r[0]) for r in rows)

    def _dst_tables(self):
        rows = self._q("dst", "select concat(table_schema, '.', table_name)"
                              " from information_schema.tables where"
                              " table_schema not in ('mysql', 'sys',"
                              " 'performance_schema', 'information_schema')"
                              " order by 1")
        return [str(r[0]) for r in rows]

    def _not_innodb(self):
        rows = self._q("src", "select concat(table_schema, '.', table_name,"
                              " ' (', engine, ')') from"
                              " information_schema.tables where table_type ="
                              " 'BASE TABLE' and engine <> 'InnoDB' and"
                              " table_schema not in ('mysql', 'sys',"
                              " 'performance_schema', 'information_schema')"
                              " order by 1")
        return [str(r[0]) for r in rows]

    def _plugin(self, side):
        rows = self._q(side, "select plugin_status from"
                             " information_schema.plugins where plugin_name"
                             " = 'clone'")
        return str(rows[0][0]) if rows else ""

    def _src_plugin(self):
        return self._plugin("src")

    def _dst_plugin(self):
        return self._plugin("dst")

    def _grants(self, side):
        rows = self._q(side, "show grants for current_user()")
        return " ".join(str(r[0]) for r in rows).upper()

    def _src_backup_admin(self):
        return _granted(self._grants("src"), "BACKUP_ADMIN")

    def _dst_clone_admin(self):
        return _granted(self._grants("dst"), "CLONE_ADMIN")

    def _log_bin(self):
        return str(self._q("src", "select @@log_bin")[0][0]) in ("1", "ON")

    def _gtid_mode(self):
        return str(self._q("src", "select @@gtid_mode")[0][0]).upper()

    def _src_users(self):
        rows = self._q("src", "select distinct user from mysql.user")
        return {str(r[0]) for r in rows}


def _granted(grants, privilege):
    """A dynamic privilege is listed by name wherever it is held: `GRANT
    ALL ON *.*` is shown with every dynamic privilege spelled out beside
    it, so nothing is inferred from ALL."""
    import re
    return bool(re.search(rf"\b{privilege}\b", grants))


def _series(version):
    """(major, minor, patch) of a MySQL version string."""
    import re
    got = re.match(r"(\d+)\.(\d+)\.(\d+)", version or "")
    return tuple(int(x) for x in got.groups()) if got else (0, 0, 0)


def _clone_versions_ok(src, dst):
    """Clone runs between identical versions, and from 8.0.37 between
    releases of one series (8.0.37 to 8.0.39, 8.4.0 to 8.4.2)."""
    a, b = _series(src), _series(dst)
    if a == b:
        return True
    return a[:2] == b[:2] and min(a, b) >= (8, 0, 37)


def _my_needs():
    said = CLONE
    return (
        _hop_allows(said),
        _WHOLE,
        Need("a source that allows a clone", ("managed_src",),
             lambda f: not f["managed_src"],
             lambda f: (f"the source is {f['managed_src']}, which does not"
                        " let a server clone it: its own snapshot or clone is"
                        " the way there (physical: {cloud: ...})")),
        Need("MySQL 8.0.17 or later", ("src",),
             lambda f: "mariadb" not in f["src"]["version"].lower()
             and _series(f["src"]["version"]) >= (8, 0, 17),
             lambda f: (f"the source is {f['src']['version']}, and a server"
                        " clones another only from MySQL 8.0.17")),
        Need("the same version and platform on both sides", ("src", "dst"),
             lambda f: _clone_versions_ok(f["src"]["version"],
                                          f["dst"]["version"])
             and (f["src"]["os"], f["src"]["machine"])
             == (f["dst"]["os"], f["dst"]["machine"]),
             lambda f: (f"the source is {f['src']['version']} on"
                        f" {f['src']['os']} {f['src']['machine']} and the"
                        f" target {f['dst']['version']} on {f['dst']['os']}"
                        f" {f['dst']['machine']}; a clone needs the same")),
        Need("the same page size and system tablespace", ("src", "dst"),
             lambda f: (f["src"]["page"], f["src"]["files"])
             == (f["dst"]["page"], f["dst"]["files"]),
             lambda f: ("innodb_page_size or innodb_data_file_path differ"
                        " between the two sides, and the clone copies the"
                        " files as they are")),
        Need("room for its packets", ("src", "dst"),
             lambda f: min(f["src"]["packet"], f["dst"]["packet"])
             >= 2 * 1024 * 1024,
             "max_allowed_packet is under 2 MB on a side, and the clone"
             " sends its data in packets of that size"),
        Need("every table in InnoDB", ("not_innodb",),
             lambda f: not f["not_innodb"],
             lambda f: (f"{_said_count(f['not_innodb'])} are not InnoDB, and"
                        " a clone carries only InnoDB's data - those tables"
                        " would arrive empty")),
        _EMPTY,
        Need("BACKUP_ADMIN on the source", ("src_backup_admin",),
             lambda f: f["src_backup_admin"],
             lambda f: (f"{f.hop.source.user} lacks BACKUP_ADMIN on the"
                        " source; a DBA grants it: grant backup_admin on *.*"
                        f" to {f.hop.source.user};")),
        Need("CLONE_ADMIN on the target", ("dst_clone_admin",),
             lambda f: f["dst_clone_admin"],
             lambda f: (f"{f.hop.target.user} lacks CLONE_ADMIN on the"
                        " target: grant clone_admin on *.* to"
                        f" {f.hop.target.user};")),
        Need("the clone plugin on the source", ("src_plugin", "options"),
             lambda f: f["src_plugin"] == "ACTIVE"
             or f["options"].get("source_plugin") == "install",
             lambda f: ("the clone plugin is not on the source, and putting"
                        " it there changes the source's settings, which"
                        " migkit does only where the hop allows it"
                        " (physical: {source_plugin: install}, taken off"
                        " again after the copy). Or a DBA runs on the"
                        " source: INSTALL PLUGIN clone SONAME"
                        " 'mysql_clone.so';")),
        Need("the binlog on the source to go on from the copy", ("log_bin",),
             lambda f: f["log_bin"],
             "the binlog is off on the source, and the changes after the"
             " copy are read from it", when=_following),
        _SIGNS_IN,
    )


# --- the rungs ---------------------------------------------------------------

PAGES = "a copy of the source's pages"
CLONE = "the target cloning itself from the source"


def _rungs():
    from . import cloudseed
    local = (
        Rung("physical-pages", PAGES, gives=GIVES | {"pages proved"},
             needs=_pg_needs(), prove=_pg_prove, footprint=1,
             because="a copy of the pages carries the indexes built and"
                     " decodes nothing", engine="postgres"),
        Rung("physical-clone", CLONE, gives=GIVES, needs=_my_needs(),
             footprint=1,
             because="a clone carries the pages and the indexes built and"
                     " decodes nothing", engine="mysql"),
    )
    return local + cloudseed.rungs()


def rungs(engine=None):
    """Every physical rung, or the ones for `engine`, in the order they
    are tried when none has been measured - each a `decide.Rung`."""
    return [r for r in _rungs() if engine in (None, r.engine)]


def facts_for(hop, eng, mode):
    from . import cloudseed
    if (settings(hop) or {}).get("cloud"):
        return cloudseed.facts_for(hop, eng, mode)
    return (PgFacts if _engine_of(hop) == "postgres" else MyFacts)(
        hop, eng, mode)


#: the logical copy, as one more rung of the seed's climb: it holds
#: wherever the physical ones do not, and stands in the climb only where
#: this hop measured it, so a physical rung measured slower is passed over
#: for it by the decision engine's own ranking
LOGICAL = Rung("logical", "a logical copy",
               because="measured the faster of the two on this hop")


def _logical_rate(hop):
    """Rows a second of the fastest logical copy this hop measured, and
    which, or (None, None)."""
    from . import movers, planner
    # the bulk paths a whole move records its rate under (`cli._move`),
    # and nothing else the hop's rates keep: a part's way measured under
    # a physical rung, another choice's rungs
    rates = {p: planner.measured_rate(hop, p) for p in movers.VIAS}
    rates = {p: r for p, r in rates.items() if r}
    if not rates:
        return None, None
    best = max(rates, key=rates.get)
    return rates[best], best


def costs(hop):
    """Seconds a row each way took on this hop: the physical rungs' own
    runs (`decide.HopCosts`) and the fastest logical copy's."""
    from . import planner
    out = {}
    for r in _rungs():
        rate = planner.measured_rate(hop, r.name)
        if rate:
            out[r.name] = 1.0 / rate
    rate, _ = _logical_rate(hop)
    if rate:
        out[LOGICAL.name] = 1.0 / rate
    return out


def choose(hop, eng, mode):
    """(the rung that carries this move, why) - or (None, every reason the
    rungs gave). (None, "") where the hop allows no physical seed.

    `decide.climb` over this engine's rungs for what the hop asks (the
    local copy, or the cloud it names), with the logical copy as a rung
    where the hop measured one: a rung never measured is tried before one
    that was, so each gets its number once; after that the cheaper a row
    ranks first."""
    if not settings(hop) or mode not in ("full", "full+cdc"):
        return None, ""
    engine = _engine_of(hop)
    want = str(settings(hop).get("cloud") or "").lower()
    candidates = [r for r in rungs(engine) if r.cloud == want]
    if not candidates:
        return None, ("a physical seed"
                      + (f" through {want}" if want else "")
                      + f" is not built for {hop.engine}; the logical copy"
                        " carries it")
    cost = costs(hop)
    if LOGICAL.name in cost:
        candidates.append(LOGICAL)
    got = decide.climb("the seed", candidates, facts_for(hop, eng, mode),
                       costs=cost)
    if got.rung is None or got.rung is LOGICAL:
        return None, _why_not(hop, got)
    return got.rung, got.reason


def _why_not(hop, got):
    """What passed every physical rung over, in words: each rung's own
    reason, and where the logical copy won on measurement, the numbers."""
    from . import planner
    said = [f"{r.said}: {why}" for r, why in got.passed
            if r is not LOGICAL]
    if got.rung is LOGICAL:
        rate, _ = _logical_rate(hop)
        for r in got.ranked:
            mine = planner.measured_rate(hop, r.name)
            if r is not LOGICAL and mine:
                said.append(f"{r.said} was measured slower on this hop -"
                            f" {mine:,.0f} rows a second against"
                            f" {rate:,.0f} for a logical copy")
    return "; ".join(said)


# --- running a command where the target is -----------------------------------

def _source_address(hop, opts):
    """(host, port) of the source as the target's host reaches it."""
    got = str(opts.get("source") or "").strip()
    if not got:
        return hop.source.host, int(hop.source.port)
    host, _, port = got.rpartition(":")
    if not host or not port.isdigit():
        raise SystemExit(f"physical: source is {got!r}; it names the source"
                         " as host:port, as the target's host reaches it")
    return host, int(port)


def _on_target(opts, script, secret="", timeout=None):
    """`script`, a shell command line, run where the target's data
    directory is, with `secret` as its first line of input - never on a
    command line, where the host's process list would show it."""
    argv = shlex.split(str(opts["run"])) + [script]
    try:
        return subprocess.run(argv, input=f"{secret}\n", capture_output=True,
                              text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        return subprocess.CompletedProcess(argv, 124, e.stdout or "",
                                           f"no answer in {timeout}s")
    except OSError as e:
        return subprocess.CompletedProcess(argv, 127, "", str(e))


def _local(command, what, timeout=600):
    """One of the hop's own commands (`stop`, `start`, `restart`), run
    here as written; a failure stops the move with its own words."""
    try:
        got = subprocess.run(shlex.split(str(command)), capture_output=True,
                             text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise SystemExit(f"the hop's {what} command failed: {e}")
    if got.returncode != 0:
        raise SystemExit(f"the hop's {what} command failed"
                         f" (exit {got.returncode}): {_tail_of(got)}")
    return got


def _tail_of(proc, lines=6):
    text = "\n".join(x for x in ((proc.stderr or "") + "\n"
                                 + (proc.stdout or "")).splitlines()
                     if x.strip())
    return "\n".join(text.splitlines()[-lines:])


# --- records -----------------------------------------------------------------

def _seed_path(hop):
    return hop.report_dir() / "physical-seed.json"


def seeded(hop):
    """What the last physical seed of this hop recorded, or None."""
    try:
        return json.loads(_seed_path(hop).read_text())
    except (OSError, ValueError):
        return None


def _save_seed(hop, record):
    path = _seed_path(hop)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(record, indent=1))
    os.replace(tmp, path)


def _record_positions(hop, dbs, before, exact):
    """Each database's copy record, as a finished copy leaves it: when,
    and the position its changes are read from (`cli._record_copy`)."""
    for d in dbs:
        where = hop.report_dir(d)
        rec = {"at": time.strftime("%Y-%m-%d %H:%M:%S"), "before": before,
               "exact": exact, "physical": True}
        (where / "copy-position.json").write_text(json.dumps(rec))
        # a position a stream already started from is a stream's, once
        used = where / "copy-position.used.json"
        if used.exists():
            used.unlink()


# --- PostgreSQL: the copy ----------------------------------------------------

def seed_slot(eng, db):
    """The slot made before the copy for one database, held by the
    subscription that continues it (and dropped with it at cutover)."""
    import re
    raw = f"migkit_seed_{eng.hop.name}_{db}".lower()
    return re.sub(r"[^a-z0-9_]", "_", raw)[:63]


def _pg_slots_before(hop, eng, dbs, log):
    """A publication and a slot on every database, before the copy, so the
    slot holds every change the copy may not. {db: slot}.

    Made and returned before the copy starts, so its consistent point is
    before the copy's first checkpoint and so before where the target's
    recovery ends. Made the one way every slot a copy follows is made
    (`_slot_with_its_snapshot`, with the plugin the subscription reads):
    over the replication protocol, then waiting until every commit before
    its position is visible to a read. A copy of the pages would not need
    the wait - the target holds what replaying the log gives it, commit
    record by commit record, whether or not a read could see a commit yet -
    but the slot is the same slot a logical copy would follow, and one way
    of making it keeps one set of rules for it: a commit a synchronous
    standby holds out of sight past the wait stops the move here, before
    anything is stopped or copied, on both paths alike."""
    made = {}
    try:
        for d in dbs:
            for stmt in eng.replicate_sql(d, copy_data=False)["src"]:
                eng.apply_replication_stmt("src", d, stmt)
            slot = seed_slot(eng, d)
            have = eng._psql("src", d, "select plugin from"
                                       " pg_replication_slots where"
                                       f" slot_name = '{slot}'").strip()
            if have and have != "pgoutput":
                raise SystemExit(f"a slot named {slot} exists on the source"
                                 f" with the {have} plugin; it is not this"
                                 " hop's - drop it or rename the hop")
            if not have:
                eng._slot_with_its_snapshot("src", d, slot,
                                            plugin="pgoutput")
            made[d] = slot
            log(f"{d}: slot {slot} made before the copy")
    except BaseException:
        _pg_drop_slots(eng, made)
        raise
    return made


def _pg_drop_slots(eng, slots):
    for d, slot in (slots or {}).items():
        try:
            eng._psql("src", d, "select pg_drop_replication_slot(slot_name)"
                                " from pg_replication_slots where slot_name"
                                f" = '{slot}' and not active")
        except Exception:  # noqa: BLE001 - said by the caller's failure
            pass


#: how the pages travel, as rungs of their own: sent as they are, or
#: compressed by the source and unpacked where they land. A link that
#: carries bytes faster than the source's cores compress them gains
#: nothing by compressing, a slow or far one does - measured, 1.8 GB
#: between two containers on one bridge, the same script but for the
#: compression: 10.0 s as they are, 10.1 s compressed (the module's table).
PLAIN = decide.Rung("as they are", "the pages sent as they are")
ZSTD = decide.Rung(
    "zstd", "the pages compressed on the source",
    needs=(decide.Need("PostgreSQL 15 on the source",
                       lambda f: f["version"] >= 150000,
                       "the source compresses what it sends only from"
                       " PostgreSQL 15"),))
#: a round trip to the source under this many seconds is a local link,
#: where the pages go as they are first
NEAR = 0.002


def compression(hop, version, rtt=None):
    """(the rung the pages travel by, why): each way tried once on this
    hop, then the one measured faster (`decide.HopCosts`, kept beside the
    hop's rates under `physical-pages `). Untried, a near source sends them
    as they are and a far one compresses them."""
    if rtt is None:
        from .tunnel import _round_trip
        rtt = _round_trip(hop.source.host, hop.source.port)
    ways = (PLAIN, ZSTD) if rtt is not None and rtt < NEAR else (ZSTD, PLAIN)
    got = decide.climb("the pages", ways, {"version": version},
                       costs=decide.HopCosts(hop, "physical-pages "))
    return got.rung, got.reason


def _pg_script(hop, opts, compressed):
    """The shell command line that empties the target's data directory,
    copies the source's pages into it and proves the copy.

    `--checkpoint=fast` writes now what the source's next checkpoint would
    write anyway; a spread one waits out up to a whole checkpoint interval
    before the first page is read. Compressed, the source sends each page
    through zstd and the target's host unpacks it. The manifest's SHA-256
    is checked file by file before the server starts, and the WAL the copy
    needs is parsed through (`pg_verifybackup`)."""
    host, port = _source_address(hop, opts)
    q = shlex.quote
    comp = " --compress=server-zstd:level=1" if compressed else ""
    return (
        "IFS= read -r PGPASSWORD; export PGPASSWORD;"
        f" D={q(str(opts['data']))};"
        # only a data directory, or nothing at all, is emptied
        ' if [ -n "$(ls -A "$D" 2>/dev/null)" ] && [ ! -f "$D/PG_VERSION" ];'
        ' then echo "$D holds files and no PG_VERSION: not a data'
        ' directory, left alone" >&2; exit 3; fi;'
        ' mkdir -p "$D" && find "$D" -mindepth 1 -delete &&'
        f" pg_basebackup -h {q(host)} -p {int(port)} -U"
        f" {q(hop.source.user)} -w -D \"$D\" -Fp -Xstream"
        " --checkpoint=fast --manifest-checksums=SHA256" + comp + " &&"
        ' pg_verifybackup -q "$D" && echo verified &&'
        ' grep -oE \'"(Start|End)-LSN": "[0-9A-F/]+"\' "$D/backup_manifest"'
        ' && du -sk "$D"')


def _pg_copy(hop, eng, opts, version, log):
    """Stop the target, copy the source's pages into its data directory,
    prove them, start it. {"start", "end", "kb", "way"} the copy reported;
    the way the pages travelled is timed and kept for the next choice."""
    way, why = compression(hop, version)
    _local(opts["stop"], "stop")
    log("the target's server stopped; copying the source's pages into its"
        f" data directory, {way.said} ({why})")
    began = time.time()
    got = _on_target(opts, _pg_script(hop, opts, way is ZSTD),
                     hop.source.password)
    if got.returncode != 0 or "verified" not in got.stdout:
        raise SystemExit(
            "the copy of the source's pages failed, and the target's server"
            " is left stopped with its data directory as the copy left it -"
            " nothing was written to the source but the slots, which are"
            f" dropped. What it said: {_quiet(_tail_of(got))}")
    import re
    lsn = dict(re.findall(r'"(Start|End)-LSN": "([0-9A-F/]+)"', got.stdout))
    kb = re.search(r"^(\d+)\s", got.stdout, re.M)
    out = {"start": lsn.get("Start"), "end": lsn.get("End"),
           "kb": int(kb.group(1)) if kb else None, "way": way.name}
    if out["kb"]:
        decide.HopCosts(hop, "physical-pages ").saw(
            way.name, out["kb"], time.time() - began)
    log(f"the copy is proved against its manifest ({out['kb'] or 0:,} kB,"
        f" the source's log from {out['start']} to {out['end']})")
    _local(opts["start"], "start")
    _wait_for(lambda: eng._psql("dst", "postgres", "select 1"),
              "the target's server")
    return out


def _wait_for(ask, what, most=None):
    most = most or int(os.environ.get("MIGKIT_SEED_START_WAIT", START_WAIT))
    end, last = time.time() + most, None
    while time.time() < end:
        try:
            return ask()
        except (Exception, SystemExit) as e:  # noqa: BLE001 - asked again
            last = e
            time.sleep(2)
    raise SystemExit(f"{what} did not accept a connection within {most}s"
                     f" after the copy: {_quiet(_first_line(last))}. The"
                     " copy carries the source's users - the hop signs in"
                     " to the target as one of them, with the source's"
                     " password")


def lsn_value(lsn):
    """An LSN as the 64-bit number its two halves make."""
    hi, _, lo = str(lsn).partition("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def replay_end(eng, copied=None, system=None):
    """The LSN the restored target's recovery ended at, and the checks
    that hold it: {"lsn", "redo", "system"}.

    `pg_last_wal_replay_lsn()` is the end of the last record the recovery
    applied, kept in the server's memory until it restarts - so it is read
    once, right after the first start, and recorded. Around it:

    * the copy's own end (`copied`, the manifest's End-LSN) is at or before
      it: recovery reached the point the copy is consistent at
    * the end-of-recovery checkpoint the target wrote is at or after it
    * the target's system identifier is the source's: this server is a
      copy of that one

    A restarted target answers NULL, and a seed position is never guessed:
    the copy is made again."""
    got = eng._psql("dst", "postgres",
                    "select coalesce(pg_last_wal_replay_lsn()::text, '')"
                    " || '|' || (select redo_lsn::text from"
                    " pg_control_checkpoint()) || '|' || (select"
                    " system_identifier::text from pg_control_system())"
                    ).strip()
    lsn, redo, sysid = (got.split("|") + ["", "", ""])[:3]
    return check_replay_end(lsn, redo, sysid, copied, system)


def check_replay_end(lsn, redo, sysid, copied=None, system=None):
    if not lsn:
        raise SystemExit(
            "the target does not say where its recovery ended: it was"
            " restarted after the copy, or it was not started from one. The"
            " position the changes go on from is never guessed - move again,"
            " which copies again")
    if system and sysid != system:
        raise SystemExit(
            f"the target's system identifier is {sysid} and the source's"
            f" {system}: the target is not a copy of this source")
    if copied and lsn_value(lsn) < lsn_value(copied):
        raise SystemExit(
            f"the target's recovery ended at {lsn}, before the copy's own"
            f" end at {copied}: it is not consistent, and nothing goes on"
            " from it")
    if redo and lsn_value(redo) < lsn_value(lsn):
        raise SystemExit(
            f"the target's last checkpoint ({redo}) is before the end of its"
            f" recovery ({lsn}): the two do not describe one recovery")
    return {"lsn": lsn, "redo": redo, "system": sysid}


def _pg_is_the_seed(eng, rec):
    """Whether the target is the copy `rec` describes: its system
    identifier is the source's and the slots it goes on from are there."""
    try:
        sysid = eng._psql("dst", "postgres", "select system_identifier from"
                                             " pg_control_system()").strip()
    except Exception:  # noqa: BLE001 - not reachable: not this seed
        return False
    return sysid == rec.get("system")


def pg_continue(hop, eng, dbs, rec, go, log):
    """A subscription per database on the slot made before the copy, its
    origin advanced to where the target's recovery ended, then enabled -
    so the first transaction applied is the first the target lacks."""
    opts = settings(hop) or {}
    via = _source_address(hop, opts) if opts.get("source") else None
    for d in dbs:
        slot = (rec.get("slots") or {}).get(d)
        if not slot:
            log(f"{d}: no slot was made before the copy, so no change after"
                " it can be carried - it holds the copy only")
            continue
        sql = eng.replicate_sql(d, copy_data=False, slot=slot,
                                at=rec["lsn"], via=via)
        if go and _subscribed(eng, d):
            # a second run: the origin has moved on from the copy, and
            # setting it back would be refused while the apply holds it
            log(f"{d}: already going on from the copy -"
                f" {eng.replication_status(d, sql['status'])}")
            continue
        from .util import without_secret
        for stmt in sql["dst"]:
            log(f"{d} dst: {without_secret(stmt, hop.source.password)}")
            if go:
                eng.apply_replication_stmt("dst", d, stmt)
        if go:
            log(f"{d}: {eng.replication_status(d, sql['status'])}")


def _subscribed(eng, db):
    """Whether this hop's subscription is on the target's database."""
    got = eng._psql("dst", db,
                    "select count(*) from pg_subscription where subname ="
                    f" '{eng._repl_name().lower()}' and subdbid = (select oid"
                    " from pg_database where datname = current_database())")
    return got.strip() not in ("", "0")


def _rows(eng, dbs):
    total = 0
    for d in dbs:
        try:
            total += sum(int(f.get("rows") or 0)
                         for f in eng.table_facts("src", d).values())
        except Exception:  # noqa: BLE001 - no estimate: no rate
            pass
    return total


def _pg_carry(hop, eng, rung, db, mode, go, log):
    opts = settings(hop)
    dbs = eng.databases()
    follow = mode == "full+cdc"
    rec = seeded(hop)
    if rec and rec.get("rung") == rung.name and _pg_is_the_seed(eng, rec) \
            and (not follow or rec.get("slots")):
        log(f"the target is the copy made on {rec['at']} (its system"
            f" identifier is the source's), recovered to {rec['lsn']};"
            " going on from it")
    else:
        where = _source_address(hop, opts)
        first = f"a slot on {', '.join(dbs)} first, then " if follow else ""
        log(f"plan: {first}the target's server stopped, its data directory"
            f" ({opts['data']}) emptied and filled with the pages of"
            f" {where[0]}:{where[1]}, checked against the copy's manifest,"
            " the server started - with the source's users and the"
            " settings the source keeps in its data directory")
        if not go:
            log("dry-run: nothing stopped, copied or made")
            return
        facts = facts_for(hop, eng, mode)
        began = time.time()
        slots = _pg_slots_before(hop, eng, dbs, log) if follow else {}
        try:
            copied = _pg_copy(hop, eng, opts, facts["src_version"], log)
            end = replay_end(eng, copied.get("end"), facts["src_system"])
        except BaseException:
            _pg_drop_slots(eng, slots)
            raise
        took = time.time() - began
        rec = {"rung": rung.name, "at": time.strftime("%Y-%m-%d %H:%M:%S"),
               "seconds": round(took, 1), "slots": slots,
               "copied_from": copied.get("start"),
               "copied_to": copied.get("end"), "kb": copied.get("kb"),
               "pages": copied.get("way"),
               "page_checksums": facts["checksums"] == "on", **end}
        _save_seed(hop, rec)
        from . import planner
        planner.record_rate(hop, rung.name, _rows(eng, dbs), took)
        log(f"the target recovered to {end['lsn']} (the copy ended at"
            f" {copied.get('end')}), in {took:,.0f}s"
            + ("; every page's checksum was verified as it was read"
               if rec["page_checksums"] else
               "; the source has no page checksums, so the pages are proved"
               " by the manifest and by migkit's check after"))
    if follow:
        pg_continue(hop, eng, dbs, rec, go, log)
    elif go:
        # a later stream is told the copy's time; with no slot made before
        # it, what changed since is carried by nothing, and it says so
        _record_positions(hop, dbs, None, {"lsn": rec["lsn"]})


# --- MySQL: the clone --------------------------------------------------------

def _esc(text):
    return str(text).replace("\\", "\\\\").replace("'", "\\'")


def clone_sql(hop, opts):
    """The statement the target runs to clone itself from the source."""
    host, port = _source_address(hop, opts)
    return (f"CLONE INSTANCE FROM '{_esc(hop.source.user)}'@'{_esc(host)}'"
            f":{int(port)} IDENTIFIED BY '{_esc(hop.source.password)}'")


def _my_plugin(eng, side, log):
    eng._q(side, "INSTALL PLUGIN clone SONAME 'mysql_clone.so'")
    log("the clone plugin put on the"
        f" {'source' if side == 'src' else 'target'}")


def _my_clone(hop, eng, opts, facts, log):
    """Run the clone on the target and wait for it to come back.

    The statement returns only when the data is copied; the target then
    restarts itself where a supervisor runs it, and says `3707` where none
    does - measured on 8.4 in a container: "Clone shutting down server as
    RESTART failed", the server gone and the clone opened only by the next
    start. The hop's `restart` command is run then. Either way the server
    that answers is waited for until it is the clone (`_opened_clone`)."""
    import pymysql
    host, port = _source_address(hop, opts)
    eng._q("dst", "SET GLOBAL clone_valid_donor_list ="
                  f" '{_esc(host)}:{int(port)}'")
    conn = eng._conn("dst")
    restart = False
    try:
        # a clone of a large server runs for as long as it takes
        conn._read_timeout = None
        with conn.cursor() as cur:
            cur.execute(clone_sql(hop, opts))
    except pymysql.err.MySQLError as e:
        code = e.args[0] if e.args else None
        if code == 3707:
            restart = True
        elif code not in (2013, 2006):
            raise SystemExit("the target could not clone the source:"
                             f" {_quiet(e.args[-1] if e.args else e)}")
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - the server went away under it
            pass
    if not restart:
        # restarting by itself, where something supervises it; a server
        # that went down and stays down is restarted as the hop says
        try:
            return _wait_for(lambda: _opened_clone(eng),
                             "the target's server", most=60)
        except SystemExit:
            restart = True
    if not opts.get("restart"):
        raise SystemExit(
            "the clone is complete and the target must be restarted to open"
            " it, and nothing restarts it by itself. The hop says how"
            " (physical: {restart: ...}); restart it and move again, which"
            " goes on from the clone")
    _local(opts["restart"], "restart")
    return _wait_for(lambda: _opened_clone(eng), "the target's server")


def _clone_status(eng):
    rows = eng._q("dst", "select state, error_no, error_message,"
                         " binlog_file, binlog_position, gtid_executed,"
                         # started after the clone began: the server that
                         # answers is the clone, not the one it replaced.
                         # Not after it ended - measured on 8.4, the end
                         # is written by the restarted server as it opens
                         # the clone, a second after it started
                         " begin_time < now(3) - interval (select"
                         " variable_value from"
                         " performance_schema.global_status where"
                         " variable_name = 'Uptime') second from"
                         " performance_schema.clone_status")
    if not rows:
        raise RuntimeError("no clone recorded on the target yet")
    state, err, msg, bfile, bpos, gtids, opened = rows[0]
    return {"state": str(state), "error": int(err or 0),
            "message": str(msg or ""), "log_file": str(bfile or ""),
            "log_pos": int(bpos or 0),
            "gtids": "".join(str(gtids or "").split()),
            "opened": bool(opened)}


def _opened_clone(eng):
    got = _clone_status(eng)
    if got["state"] == "Completed" and not got["opened"]:
        raise RuntimeError("the clone is complete and the server has not"
                           " restarted into it yet")
    return got


# GTID sets, as the servers print them: `uuid:1-5:7,uuid2:3` (and, from
# 8.4, `uuid:tag:1-3`). Held as {source: [(first, last), ...]} merged.

def gtid_set(text):
    out = {}
    for part in str(text or "").replace("\n", "").split(","):
        part = part.strip()
        if not part:
            continue
        bits = part.split(":")
        key = bits[0].lower()
        for bit in bits[1:]:
            if not bit[:1].isdigit():
                key = f"{bits[0].lower()}:{bit.lower()}"
                continue
            lo, _, hi = bit.partition("-")
            out.setdefault(key, []).append((int(lo), int(hi or lo)))
    return {k: _merged(v) for k, v in out.items()}


def _merged(spans):
    out = []
    for lo, hi in sorted(spans):
        if out and lo <= out[-1][1] + 1:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def gtid_add(s, gtid):
    key, _, n = str(gtid).lower().rpartition(":")
    got = dict(s)
    got[key] = _merged(got.get(key, []) + [(int(n), int(n))])
    return got


def gtid_minus(a, b):
    out = {}
    for key, spans in a.items():
        left = []
        for lo, hi in spans:
            cur = [(lo, hi)]
            for blo, bhi in b.get(key, []):
                nxt = []
                for clo, chi in cur:
                    if bhi < clo or blo > chi:
                        nxt.append((clo, chi))
                        continue
                    if clo < blo:
                        nxt.append((clo, blo - 1))
                    if chi > bhi:
                        nxt.append((bhi + 1, chi))
                cur = nxt
            left += cur
        if left:
            out[key] = _merged(left)
    return out


def gtid_and(a, b):
    return gtid_minus(a, gtid_minus(a, b))


def gtid_text(s):
    return ",".join(f"{k}:" + ":".join(f"{lo}-{hi}" if hi != lo else str(lo)
                                       for lo, hi in v)
                    for k, v in sorted(s.items()) if v)


def _gtids_from(eng, token):
    """The GTIDs the source's binlog holds from `token` to its end now."""
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.event import GtidEvent
    ep = eng.hop.source
    stream = BinLogStreamReader(
        connection_settings={"host": ep.host, "port": ep.port,
                             "user": ep.user, "passwd": ep.password},
        server_id=int((eng.hop.options or {}).get("server_id", 4379)) + 1,
        blocking=False, resume_stream=True, log_file=token["log_file"],
        log_pos=int(token["log_pos"]), only_events=[GtidEvent])
    got = {}
    try:
        for ev in stream:
            got = gtid_add(got, ev.gtid)
    finally:
        stream.close()
    return got


def position_proof(eng, token, cloned, read_after=None):
    """Why a tail from `token` would apply a transaction twice or miss one
    after a copy holding the GTID set `cloned`, or None.

    The source's executed set is read first, the log from `token` to its
    end after: what the log holds from there must share nothing with the
    copy (nothing twice), and the copy and the log together must hold
    everything the source had executed (nothing missed). A transaction
    committed between the two reads is in the log and not in the executed
    set, which the second test allows."""
    now = gtid_set(eng._q("src", "select @@global.gtid_executed")[0][0])
    after = (read_after or _gtids_from)(eng, token)
    have = gtid_set(cloned) if isinstance(cloned, str) else cloned
    twice = gtid_and(after, have)
    if twice:
        return (f"the source's log from {token['log_file']}:"
                f"{token['log_pos']} holds {gtid_text(twice)}, which the copy"
                " already has - it would be applied twice")
    missed = gtid_minus(gtid_minus(now, have), after)
    if missed:
        return (f"{gtid_text(missed)} ran on the source and is neither in"
                f" the copy nor in its log from {token['log_file']}:"
                f"{token['log_pos']} - it would be missed")
    return None


def _my_carry(hop, eng, rung, db, mode, go, log):
    opts = settings(hop)
    dbs = eng.databases()
    facts = facts_for(hop, eng, mode)
    rec = seeded(hop)
    if rec and rec.get("rung") == rung.name and _my_is_the_seed(eng, rec):
        log(f"the target is the clone made on {rec['at']} at"
            f" {rec['log_file']}:{rec['log_pos']}; going on from it")
    else:
        host, port = _source_address(hop, opts)
        log(f"plan: the target clones itself from {host}:{port} - its data"
            " replaced by the source's, its users by the source's - and"
            " restarts; the changes after it are read from the position the"
            " clone records")
        if not go:
            log("dry-run: nothing cloned")
            return
        installed = False
        if facts["src_plugin"] != "ACTIVE":
            from . import approvals
            approvals.require(hop, "source-setting", db, log)
            _my_plugin(eng, "src", log)
            installed = True
        try:
            if facts["dst_plugin"] != "ACTIVE":
                _my_plugin(eng, "dst", log)
            began = time.time()
            status = _my_clone(hop, eng, opts, facts, log)
            took = time.time() - began
        finally:
            if installed:
                # the source's settings as they were
                eng._q("src", "UNINSTALL PLUGIN clone")
                log("the clone plugin taken off the source again")
        if status["state"] != "Completed" or status["error"]:
            raise SystemExit(f"the clone did not complete: {status['state']}"
                             f" {status['error']} {_quiet(status['message'])}")
        token = {"log_file": status["log_file"],
                 "log_pos": status["log_pos"]}
        if facts["gtid_mode"] == "ON":
            have = str(eng._q("dst", "select @@global.gtid_executed")[0][0])
            if gtid_set(have) != gtid_set(status["gtids"]):
                raise SystemExit(
                    f"the target holds {have} and the clone recorded"
                    f" {status['gtids']}: they do not describe one copy")
            why = position_proof(eng, token, status["gtids"])
            if why:
                raise SystemExit(f"the clone's position is not exact: {why}")
            log(f"the clone's position {token['log_file']}:"
                f"{token['log_pos']} is proved against the source's"
                " executed set")
        rec = {"rung": rung.name, "at": time.strftime("%Y-%m-%d %H:%M:%S"),
               "seconds": round(took, 1), **token,
               "gtids": status["gtids"]}
        _save_seed(hop, rec)
        from . import planner
        planner.record_rate(hop, rung.name, _rows(eng, dbs), took)
        log(f"the target is the source as of {token['log_file']}:"
            f"{token['log_pos']}, in {took:,.0f}s")
    token = {"log_file": rec["log_file"], "log_pos": rec["log_pos"]}
    _record_positions(hop, dbs, token, token)


def _my_is_the_seed(eng, rec):
    try:
        got = _clone_status(eng)
    except Exception:  # noqa: BLE001 - no clone there
        return False
    return (got["state"] == "Completed" and got["log_file"] == rec.get(
        "log_file") and got["log_pos"] == rec.get("log_pos"))


# --- the move ----------------------------------------------------------------

def carry(hop, eng, db, mode, go, log):
    """Seed the target by the physical rung this hop allows and needs, and
    set up what carries on from it. True where it did (the move is this
    one's), False where the logical copy is to run - with the reason said
    when the hop asked for a physical seed and none was taken."""
    rung, why = choose(hop, eng, mode)
    if rung is None:
        if why:
            log(f"the physical copy is not taken: {why}")
        return False
    log(f"{rung.said}: {why}")
    if rung.cloud:
        from . import cloudseed
        cloudseed.carry(hop, eng, rung, db, mode, go, log)
    elif rung.engine == "postgres":
        _pg_carry(hop, eng, rung, db, mode, go, log)
    else:
        _my_carry(hop, eng, rung, db, mode, go, log)
    return True
