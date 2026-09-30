"""Keep the application from writing to the target until cutover - and, at
the cutover of a hop that asks for it, from writing to the source.

A target that takes writes from the application before the cutover drifts
from the source in ways no verification can put right afterwards. Tencent's
DTS offers this as one switch, `IsDstReadOnly`. On PostgreSQL the obvious
single switch does not work, as measured:

* `ALTER ROLE app SET default_transaction_read_only = on` stopped a *new*
  session of `app`. A session already connected kept writing. Its setting
  still read `off`, and its insert went in. The application can also turn
  the setting off itself.
* Revoking the role's write privileges took effect on the connected
  session's very next statement: `permission denied for table t`.
* A role that owns its table could grant the privilege back to itself and
  write again, so revoking is no boundary for an owner.

So the freeze is decided per role, from what the catalogue says about it:
* the write privileges it holds directly are revoked, and recorded first
  so `thaw` gives back exactly those
* a role that can still write after that - because it owns the table, or
  inherits the privilege, or it is granted to everyone - also gets
  `default_transaction_read_only` in this database, and its open sessions
  are ended, since the setting reaches only sessions that start afterwards

What it did is said per role. The account migkit itself writes with is
never frozen. MySQL has its own measurements and its own rules, further
down.

Turned on by the hop option `protect_target: true`. `app_roles` names the
roles; without it they are the login roles present on both sides, which
are the application's own accounts, carried across by `migkit users`.

The source is frozen only by the cutover of a hop that turns `cutover:` on
(`migkit.cutover`), with the same record-first rule, and more: there the
freeze is the start of the write outage and has to be complete, not a
guard. Its rules are at the end of this file.
"""
import json
import time

WRITES = ("INSERT", "UPDATE", "DELETE", "TRUNCATE")


def _q(ident):
    return '"' + str(ident).replace('"', '""') + '"'


def _lit(text):
    return "'" + str(text).replace("'", "''") + "'"


def _state_path(hop, db, side="dst"):
    return hop.report_dir(db) / ("freeze.json" if side == "dst"
                                 else "freeze-src.json")


def state(hop, db, side="dst"):
    """What a freeze on this database recorded, or None."""
    try:
        return json.loads(_state_path(hop, db, side).read_text())
    except (OSError, ValueError):
        return None


def _save(hop, db, record, side="dst"):
    path = _state_path(hop, db, side)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(record, indent=1))
    tmp.replace(path)


def app_roles(hop, eng, db, side="dst"):
    """The roles whose writes the freeze stops.

    On the target, the login roles present on both sides: the
    application's accounts, carried across. On the source, every login
    role that is not migkit's, the system's or a superuser's - an account
    the application writes with need not have been carried yet, and it
    writes all the same. A superuser is not in the list because nothing a
    freeze can take stops one; `bypass` names them instead."""
    named = (hop.options or {}).get("app_roles")
    # migkit's own accounts, its replica's among them: ending that one's
    # session at the source's freeze cut the stream the drain waits on
    ours = {hop.target.user, hop.source.user,
            getattr(eng, "REPL_USER", None)}
    if named:
        return [r for r in named if r not in ours]
    kind, eng = side_engine(hop, eng, side)
    if kind == "mysql":
        from .users import _mysql_sysuser
        q = "select distinct user from mysql.user order by 1"
        if side == "src":
            return [r[0] for r in eng._q("src", q)
                    if r[0] not in ours and r[0]
                    and not _mysql_sysuser(r[0])]
        src = {r[0] for r in eng._q("src", q)}
        return [r[0] for r in eng._q("dst", q)
                if r[0] in src and r[0] not in ours
                and not _mysql_sysuser(r[0])]
    from .users import _pg_sysrole
    if side == "src":
        got = eng._psql("src", db, "select rolname from pg_roles where"
                                   " rolcanlogin and not rolsuper"
                                   " order by 1")
        return [r for r in got.split() if r not in ours
                and not _pg_sysrole(r)]
    q = "select rolname from pg_roles where rolcanlogin order by 1"
    src = set(eng._psql("src", db, q).split())
    dst = eng._psql("dst", db, q).split()
    return [r for r in dst
            if r in src and r not in ours and not _pg_sysrole(r)]


def _direct_writes(eng, db, role, side="dst"):
    """[(schema.table, privilege)] granted to `role` itself in this db."""
    got = eng._psql(side, db,
                    "select n.nspname||'.'||c.relname||'|'||a.privilege_type"
                    " from pg_class c join pg_namespace n"
                    " on n.oid = c.relnamespace, aclexplode(c.relacl) a"
                    f" where a.grantee = {_lit(role)}::regrole"
                    " and c.relowner <> a.grantee"
                    " and c.relkind in ('r', 'p')"
                    " and a.privilege_type in ('INSERT', 'UPDATE', 'DELETE',"
                    " 'TRUNCATE') and n.nspname not in ('pg_catalog',"
                    " 'information_schema') order by 1")
    return [tuple(line.split("|", 1)) for line in got.splitlines() if line]


def _still_writes(eng, db, role, side="dst"):
    """Tables `role` can still write to, whatever the reason."""
    got = eng._psql(side, db,
                    "select n.nspname||'.'||c.relname from pg_class c"
                    " join pg_namespace n on n.oid = c.relnamespace"
                    " where c.relkind in ('r', 'p') and n.nspname not in"
                    " ('pg_catalog', 'information_schema')"
                    f" and has_table_privilege({_lit(role)}, c.oid,"
                    " 'INSERT, UPDATE, DELETE, TRUNCATE') order by 1")
    return [line for line in got.splitlines() if line]


def _setting(eng, db, role, side="dst"):
    """The role's own `default_transaction_read_only` in this database, or
    None when it has none."""
    got = eng._psql(side, db,
                    "select s.cfg from pg_db_role_setting d,"
                    " unnest(d.setconfig) s(cfg)"
                    f" where d.setrole = {_lit(role)}::regrole"
                    " and d.setdatabase = (select oid from pg_database"
                    " where datname = current_database())"
                    " and s.cfg like 'default_transaction_read_only=%'")
    return got.split("=", 1)[1] if got.strip() else None


def _engine(hop):
    from .engines import ALIASES
    return ALIASES.get(hop.engine, hop.engine)


def side_engine(hop, eng, side):
    """(what engine it is, the engine) answering for one side: a pair of
    two engines keeps one per side, and a MySQL source into PostgreSQL is
    frozen as MySQL."""
    own = getattr(eng, "src_engine" if side == "src" else "dst_engine", None)
    e = own if own is not None else eng
    from .engines import ALIASES
    # a service on another's API is held as itself (`Engine.PAIR_ENGINE`)
    kind = (getattr(e, "PAIR_ENGINE", "") or getattr(e, "CANON_ENGINE", "")
            or _engine(hop))
    return ALIASES.get(kind, kind), e


def supported(hop):
    return _engine(hop) in ("postgres", "mysql")


def freeze(hop, eng, db, say, side="dst"):
    """Stop the application's roles writing to this database.

    Idempotent: a role frozen already is checked again, and anything it
    could newly write to - a table the move just created, a grant made
    since - is frozen too, the record growing rather than being replaced.
    """
    kind, eng = side_engine(hop, eng, side)
    if kind == "mysql":
        return _my_freeze(hop, eng, db, say, side)
    dbname = eng._d(side, db)
    record = state(hop, db, side) or {"db": dbname, "roles": {}}
    for role in app_roles(hop, eng, db, side):
        entry = record["roles"].setdefault(
            role, {"revoked": [], "setting": None, "set": False})
        revoked = [tuple(x) for x in entry["revoked"]]
        todo = [g for g in _direct_writes(eng, db, role, side)
                if g not in revoked]
        entry["revoked"] = [list(g) for g in revoked + todo]
        # recorded before it is done: a run that stops half way leaves a
        # record `thaw` can finish from
        _save(hop, db, record, side)
        for table, priv in todo:
            sch, _, tbl = table.partition(".")
            eng._psql(side, db, f"revoke {priv} on {_q(sch)}.{_q(tbl)}"
                                f" from {_q(role)}")
        left = _still_writes(eng, db, role, side)
        how = [f"{len(todo)} write grants revoked"] if todo else []
        if left:
            if not entry["set"]:
                entry["setting"] = _setting(eng, db, role, side)
                entry["set"] = True
                _save(hop, db, record, side)
            eng._psql(side, db, f"alter role {_q(role)} in database"
                                f" {_q(dbname)} set"
                                " default_transaction_read_only = on")
            ended = eng._psql(side, db,
                              "select count(pg_terminate_backend(pid))"
                              " from pg_stat_activity"
                              f" where usename = {_lit(role)}"
                              " and datname = current_database()"
                              " and pid <> pg_backend_pid()").strip()
            how.append(f"still able to write to {len(left)} tables"
                       f" ({', '.join(left[:3])}{' ...' if len(left) > 3 else ''})"
                       " as their owner or through another role, so it is"
                       " read-only by default here and its"
                       f" {ended or 0} open sessions were ended")
        say(f"{dbname}: {role} - " + ("; ".join(how) if how
                                        else "nothing to freeze"))
    _save(hop, db, record, side)
    return record


def thaw(hop, eng, db, say, side="dst"):
    """Give back exactly what `freeze` - or a hold of the side's
    (`Hold.hold`) - took, from its record, and forget it."""
    record = state(hop, db, side)
    if not record:
        return None
    hold_for(hop, eng, side).release(db, record, say)
    _state_path(hop, db, side).unlink(missing_ok=True)
    return record


def _pg_thaw(hop, eng, db, record, say, side):
    dbname = record.get("db") or eng._d(side, db)
    for role, entry in record["roles"].items():
        for table, priv in entry["revoked"]:
            sch, _, tbl = table.partition(".")
            eng._psql(side, db, f"grant {priv} on"
                                f" {_q(sch)}.{_q(tbl)} to {_q(role)}")
        if entry.get("set"):
            before = entry.get("setting")
            eng._psql(side, db,
                      f"alter role {_q(role)} in database"
                      f" {_q(dbname)} "
                      + (f"set default_transaction_read_only = {before}"
                         if before is not None
                         else "reset default_transaction_read_only"))
        if entry.get("connect"):
            eng._psql(side, db, f"grant connect on database"
                                f" {_q(dbname)} to {_q(role)}")
        say(f"{dbname}: {role} - writes given back"
            f" ({len(entry['revoked'])} grants)")
    _pg_connect_back(eng, db, dbname, record, side)


# --- MySQL ----------------------------------------------------------------
# Measured on 8.4 before choosing:
# * a table-level revoke stopped a connected session on its next statement
# * a database-level revoke did not: a session that had already chosen the
#   database kept inserting, and only new sessions were refused
# * `read_only` stopped the application and left an administrator writing,
#   but it is one switch for the whole server - every database, every
#   account - so it is not what a per-database hop may reach for
# So each account's grants on this database are revoked (recorded first),
# and its open sessions are ended so the database-level ones reach it.
# Privileges it holds server-wide, or through a role, cannot be taken back
# for this database alone without taking them everywhere: those are said,
# not touched.

_MY_WRITES = ("Insert", "Update", "Delete")


def _my_q(ident):
    return "`" + str(ident).replace("`", "``") + "`"


def _my_account(user, host):
    return f"{_lit(user)}@{_lit(host)}"


def _my_grants(eng, dbname, user, side="dst"):
    """[[host, table or None, privilege]] of write grants on this database,
    database-wide ones with table None."""
    out = []
    for host, *flags in eng._q(
            side, "select host, insert_priv, update_priv, delete_priv"
                  " from mysql.db where db = %s and user = %s",
            (dbname, user)):
        out += [[host, None, p.upper()]
                for p, flag in zip(_MY_WRITES, flags) if flag == "Y"]
    for host, table, privs in eng._q(
            side, "select host, table_name, table_priv"
                  " from mysql.tables_priv where db = %s and user = %s",
            (dbname, user)):
        have = {x.strip().lower() for x in str(privs).split(",")}
        out += [[host, table, p.upper()] for p in _MY_WRITES
                if p.lower() in have]
    return out


def _my_wider(eng, user, side="dst"):
    """Where this account can write that a per-database freeze cannot
    reach: server-wide privileges, and roles granted to it."""
    said = []
    got = eng._q(side, "select count(*) from mysql.user where user = %s"
                       " and 'Y' in (insert_priv, update_priv,"
                       " delete_priv)", (user,))
    if got and int(got[0][0]):
        said.append("holds write privileges on every database")
    try:
        roles = [r[0] for r in eng._q(
            side, "select distinct from_user from mysql.role_edges"
                  " where to_user = %s", (user,))]
    except Exception:  # noqa: BLE001 - MariaDB keeps roles elsewhere
        roles = []
    if roles:
        said.append(f"is granted the roles {', '.join(roles)}")
    return said


def _my_sessions(eng, users, side, db=None):
    """[(id, user)] of the open sessions of these accounts, but migkit's."""
    if not users:
        return []
    marks = ", ".join(["%s"] * len(users))
    return [(int(i), u) for i, u in eng._q(
        side, "select id, user from information_schema.processlist"
              f" where user in ({marks}) and id <> connection_id()",
        tuple(users))]


def _my_hold(hop, eng, side, sql):
    """A grant or revoke of the hold on one side, kept out of that side's
    binary log wherever the log is replayed on the other side: always on
    the source, whose replica would take the source's freeze onto the
    target - where the target's own hold had already taken that grant,
    so the replica stops on it (1141) and the drain with it - and on a
    target whose log flows back to the source (a stream back at cutover,
    or streams both ways), whose hold would become the source's."""
    top = str((hop.options or {}).get("topology", "") or "").lower()
    back = str((hop.options or {}).get("reverse", "") or "").lower()
    local = side == "src" or top == "two_way" or back == "at_cutover"
    if not local:
        return eng._q(side, sql)
    conn = eng._conn(side)
    try:
        with conn.cursor() as cur:
            cur.execute("set session sql_log_bin = 0")
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _my_freeze(hop, eng, db, say, side="dst"):
    dbname = eng._d(side, db)
    record = state(hop, db, side) or {"db": dbname, "roles": {}}
    for user in app_roles(hop, eng, db, side):
        entry = record["roles"].setdefault(user, {"revoked": []})
        todo = [g for g in _my_grants(eng, dbname, user, side)
                if g not in entry["revoked"]]
        entry["revoked"] += todo
        _save(hop, db, record, side)
        for host, table, priv in todo:
            where = (f"{_my_q(dbname)}.{_my_q(table)}" if table
                     else f"{_my_q(dbname)}.*")
            _my_hold(hop, eng, side, f"revoke {priv} on {where} from"
                                     f" {_my_account(user, host)}")
        ended = 0
        if todo or side == "src":
            # a database-level revoke reaches a session only when it next
            # chooses the database; ending the session is what makes it now.
            # On a source at cutover every session goes: one with a
            # transaction open would otherwise commit what it wrote before
            # the revoke, after the position the drain waits for
            ended = _my_kill(eng, side, _my_sessions(eng, [user], side))
        how = ([f"{len(todo)} write grants revoked, {ended} open sessions"
                " ended"] if todo or ended else [])
        wider = _my_wider(eng, user, side)
        if wider:
            how.append(f"still able to write here: it {' and '.join(wider)},"
                       " which cannot be taken for this database alone -"
                       " left as it is")
        say(f"{dbname}: {user} - " + ("; ".join(how) if how
                                        else "nothing to freeze"))
    _save(hop, db, record, side)
    return record


def _my_kill(eng, side, sessions, wait=10):
    """End these sessions and wait until the server has let them go:
    `KILL` only marks a session, and one still rolling back holds its
    transaction open where a proof looks for it."""
    ended = 0
    for pid, _ in sessions:
        try:
            eng._q(side, f"kill {int(pid)}")
            ended += 1
        except Exception:  # noqa: BLE001 - it ended on its own
            pass
    ids = [int(pid) for pid, _ in sessions]
    if not ids:
        return ended
    marks = ", ".join(["%s"] * len(ids))
    end = time.monotonic() + wait
    while time.monotonic() < end:
        got = eng._q(side, "select count(*) from information_schema"
                           f".processlist where id in ({marks})", tuple(ids))
        if not got or int(got[0][0]) == 0:
            break
        time.sleep(0.1)
    return ended


def _my_thaw(hop, eng, db, record, say, side="dst"):
    dbname = record.get("db") or eng._d(side, db)
    for user, entry in record["roles"].items():
        for host, table, priv in entry["revoked"]:
            where = (f"{_my_q(dbname)}.{_my_q(table)}" if table
                     else f"{_my_q(dbname)}.*")
            _my_hold(hop, eng, side, f"grant {priv} on {where} to"
                                     f" {_my_account(user, host)}")
        say(f"{dbname}: {user} - writes given back"
            f" ({len(entry['revoked'])} grants)")


# --- a side's writes held, and let go -----------------------------------------
# The freeze that starts the write outage. It has to stop every new write,
# end every write already in flight, and be read back: a freeze that stops
# new writes and lets an open transaction commit afterwards is how a
# "clean" cutover loses a row - the commit lands after the position the
# drain waited for, and the stream is torn down without it.
#
# One primitive per engine does it (`Hold`), for either side: the
# cutover's freeze of the source stands on it, and keeping the target
# from the application can. Each engine's hold says the ways it holds by
# (`Hold.rungs`), holds (`hold`), gives back exactly what it took
# (`release`), reads back that the writes are held (`held`), names who
# still writes through it (`exempt`), and counts the server's writes and
# what it holds open (`writes`, `open`). Everything is recorded before it
# is done, per database and for the server, and `release` gives back
# exactly that. Which engines have one is declared for `decide.coverage`
# (shape "writes held").
#
# Chosen from what the side allows, never asked (`choose`):
# * the hop's databases are every database on the server, and migkit may
#   change the server's settings: the server-wide switch as well - on
#   PostgreSQL `default_transaction_read_only` through ALTER SYSTEM, on
#   MySQL `super_read_only`, which stops even an administrator
# * always, per database: the application's accounts as on the target
#   (grants revoked, recorded first), and on PostgreSQL the right to
#   connect taken from the roles that could still write after that (an
#   owner), and every open session of the application ended, so what it
#   had not committed is rolled back rather than committed late

def _server_path(hop, side="src"):
    return hop.report_dir() / f"freeze-{side}-server.json"


def server_state(hop, side="src"):
    try:
        return json.loads(_server_path(hop, side).read_text())
    except (OSError, ValueError):
        return None


def _server_save(hop, record, side="src"):
    path = _server_path(hop, side)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(record, indent=1))
    tmp.replace(path)


def source_frozen(hop, dbs):
    """True where anything of a source freeze is still recorded."""
    return bool(server_state(hop)) or any(state(hop, d, "src") for d in dbs)


# --- which way: a ladder of what the side allows ------------------------------
# Every way of holding a side's writes is a rung of one ladder (`decide`),
# per engine, and the climb takes the first whose needs hold on this
# server: it is all the hop's, migkit's account may make the change, the
# version has it. The hop's `freeze:` narrows the ladder to the whole
# server or the hop's databases alone, and a drain by a mark written on
# the source (`drainmark`) to the ways migkit's own account still writes
# through. The order is what each way holds - a switch that stops the
# server's administrators as well before the application's accounts -
# and never time: a way that stops more writers is not traded for one a
# few milliseconds quicker (the owner: guarantees are never traded).

#: what a way gives (`decide.Rung.gives`), as the climb says it lacks one
SERVER = "the whole server frozen"
DATABASE = "the hop's databases alone frozen"
#: migkit's own account still writes through it: what a drain by a mark
#: on the source needs (`drainmark`)
OURS = "migkit's own account writing through it"

#: the scope each way is, as the hop's `freeze:` and the records name it
SCOPE = {"pg_server": "server", "pg_database": "database",
         "my_server": "server", "my_database": "database",
         "mongo_block": "server", "mongo_fsync": "server",
         "mongo_database": "database",
         "redis_pause": "server", "redis_accounts": "database",
         "mssql_read_only": "database", "mssql_deny": "database",
         "cql_revoke": "database",
         "kafka_cluster": "server", "kafka_topics": "database",
         "os_cluster": "server", "os_indices": "database"}


def _read(fn):
    """A fact read from the server, None where it cannot be: a need that
    reads None does not hold, and says it could not be read."""
    def reader():
        try:
            return fn()
        except Exception:  # noqa: BLE001 - not readable is not "yes"
            return None
    return reader


def _alone(what="databases"):
    from .decide import Need

    def why(f):
        if f["others"] is None:
            return f"the server's {what} could not be listed"
        return (f"the server holds {what} the hop does not move"
                f" ({', '.join(f['others'][:3])})")
    return Need(f"every one of the server's {what} the hop's",
                lambda f: f["others"] == [], why)


def _is(fact, what, why):
    """A need that holds where the fact read True; one that could not be
    read is said as that."""
    from .decide import Need
    return Need(what, lambda f: f[fact] is True,
                lambda f: why if f[fact] is not None else
                f"{what}: it could not be read from the server")


def _at_least(fact, want, words):
    from .decide import Need

    def why(f):
        got = f[fact]
        if got is None:
            return f"{words}, and the server's version could not be read"
        return f"{words}, and the server runs {'.'.join(map(str, got))}"
    return Need(f"version {'.'.join(map(str, want))} or later",
                lambda f: f[fact] is not None and tuple(f[fact]) >= want,
                why)


class Hold:
    """Writes held on one side of a hop, and let go again: the one
    primitive per engine the cutover's freeze of the source stands on,
    and keeping the target from the application can.

    rungs()             the ways it holds by, in the order tried
    facts(dbs)          what their needs read, each asked once
    hold(dbs, way, say) held, each change recorded before it is made
    release(db, record, say), release_server(rec, say)
                        exactly what `hold` took, from its records
    held(db, way)       [(held, what)] read back from the server
    exempt(db, way)     who still writes through it, as lines; a line that
                        says "per-database freeze" is one the way cannot
                        reach
    writes(db)          the server's running count of writes, or None
    open(db, older)     what it cannot end cleanly: prepared and long
                        transactions, as lines
    cannot(db)          what migkit's account lacks for it, as lines
    """

    def __init__(self, hop, eng, side="src"):
        self.hop, self.side = hop, side
        self.kind, self.eng = side_engine(hop, eng, side)

    @property
    def me(self):
        """The account migkit signs in to this side with."""
        return (self.hop.source if self.side == "src" else
                self.hop.target).user

    def rungs(self):
        return ()

    def facts(self, dbs):
        from .decide import Facts
        return Facts(others=_read(lambda: self.others(list(dbs))),
                     **self.readers(list(dbs)))

    def readers(self, dbs):
        return {}

    def others(self, dbs):
        return None

    def hold(self, dbs, way, say):
        raise SystemExit(f"a {self.hop.engine} side is not held by migkit")

    def release(self, db, record, say):
        pass

    def release_server(self, rec, say):
        pass

    def held(self, db, way):
        return [(False, f"{self.hop.engine}: no hold of migkit's to read"
                        " back")]

    def exempt(self, db, way):
        return []

    def writes(self, db):
        return None

    def open(self, db, older_than):
        return []

    def cannot(self, db):
        return []


def hold_for(hop, eng, side="src"):
    """The hold of one side, as its engine does it (`HOLDS`)."""
    kind = side_engine(hop, eng, side)[0]
    return HOLDS.get(kind, Hold)(hop, eng, side)


def not_held(kind):
    """Why migkit holds no writes of this engine, in words: the gap
    `decide` keeps for it (shape "writes held")."""
    from .decide import NOT_APPLICABLE, cell
    state_, why = cell(kind, "writes held")
    return why if state_ == NOT_APPLICABLE else \
        "migkit holds no writes of it yet"


def ways(hop, eng, dbs, side="src"):
    """(facts, rungs) of the ways this side's writes may be held."""
    h = hold_for(hop, eng, side)
    return h.facts(dbs), h.rungs()


def choose(hop, eng, dbs, how="auto", need=(), side="src"):
    """The `decide.Choice` of the way a side is held: `how` is the hop's
    `freeze:` (auto, server, database), `need` what else the way must give
    (`OURS`, for a drain by a mark on the source)."""
    from . import decide
    facts, rungs = ways(hop, eng, dbs, side)
    want = tuple(need) + ((SERVER,) if how == "server" else
                          (DATABASE,) if how == "database" else ())
    kind = side_engine(hop, eng, side)[0]
    return decide.climb(f"holding the {kind} {'source' if side == 'src' else 'target'}'s"
                        " writes", rungs, facts, want)


def scope(hop, eng, dbs):
    """("server", why) where the whole server may be frozen - every
    database on it is the hop's, and migkit's account may make the
    change - or ("database", why not): the way the ladder takes."""
    got = choose(hop, eng, dbs)
    if got.rung is None:
        return "database", got.reason
    return SCOPE.get(got.rung.name, "database"), got.reason


def _way(hop, eng, dbs, how, side="src"):
    """The way `how` stands for: a way's own name, or the scope the
    ladder narrows to (`choose`)."""
    if how in SCOPE:
        return how
    got = choose(hop, eng, dbs, how, side=side)
    if got.rung is None:
        raise SystemExit(f"the {'source' if side == 'src' else 'target'} is"
                         f" not held by migkit here: {got.reason}")
    return got.rung.name


def _budget(hop):
    from .cutover import settings
    conf = settings(hop)
    return conf["budget"] if conf else None


def _named(hop):
    got = (hop.options or {}).get("app_roles")
    return [str(x) for x in got] if got else None


def freeze_source(hop, eng, dbs, say, how):
    """Stop every write of the application to the source, `how` being a
    way's name (`choose`) or the scope the ladder narrows to ("server",
    "database"). Returns when the application's sessions are gone;
    `proof` is what shows it held."""
    hold_for(hop, eng, "src").hold(list(dbs), _way(hop, eng, dbs, how),
                                   say)


def thaw_source(hop, eng, dbs, say):
    """Give the source back exactly what `freeze_source` took: the
    databases' records first, then the server's switch - but for a way
    whose switch keeps the databases' own settings from changing (a
    cluster made read-only refuses index settings), which is lifted
    first."""
    rec = server_state(hop)
    first = bool(rec) and rec.get("way") in _SERVER_FIRST
    if first:
        _thaw_server(hop, eng, rec, say)
    for d in dbs:
        thaw(hop, eng, d, say, side="src")
    if rec and not first:
        _thaw_server(hop, eng, rec, say)


def _thaw_server(hop, eng, rec, say, side="src"):
    hold_for(hop, eng, side).release_server(rec, say)
    _server_path(hop, side).unlink(missing_ok=True)


def proof(hop, eng, db, how):
    """[(held, what)] read back from the source after `freeze_source`:
    nothing held means the freeze is not proven and the cutover goes
    back. `how` is the way (`choose`) or its scope."""
    way = how if how in SCOPE else (
        (state(hop, db, "src") or {}).get("way")
        or (server_state(hop) or {}).get("way") or how)
    return hold_for(hop, eng, "src").held(db, way)


def open_work(hop, eng, db, side, older_than):
    """What is left open on a side that a freeze cannot end cleanly:
    prepared transactions (they survive a killed session, a restart and
    every timeout, and a PostgreSQL slot cannot be made while one is
    open), and write transactions older than `older_than` seconds (None:
    any). Each as a line; [] where there is none."""
    return hold_for(hop, eng, side).open(db, older_than)


def cannot(hop, eng, db):
    """What migkit's account on the source lacks to freeze it per
    database and give it back, as lines; [] where it lacks nothing. Asked
    before anything is written: a freeze that revokes half and cannot end
    a session holds the application out and proves nothing."""
    try:
        return hold_for(hop, eng, "src").cannot(db)
    except Exception as e:  # noqa: BLE001 - not readable: said as that
        return [f"what migkit's account may do on the source could not be"
                f" read ({str(e).strip()[:120]})"]


def writes_seen(hop, eng, db, side="src"):
    """The server's running count of rows written in this database, or
    None where it keeps none that can be read: compared across a few
    seconds, it moving is something still writing. PostgreSQL counts in
    `pg_stat_database`, each session reporting at least once a second
    while it works; MySQL per table in `performance_schema`."""
    try:
        return hold_for(hop, eng, side).writes(db)
    except Exception:  # noqa: BLE001 - not readable: no count, not "quiet"
        return None


def _held_still(hop, eng, db, side="src", window=1.0):
    """(held, what) of the server's own count of writes over `window`
    seconds after a hold (`writes_seen`): the read back of a way whose
    server keeps no session to look for - a write the hold did not reach
    moves the count. No count is not proof."""
    first = writes_seen(hop, eng, db, side)
    time.sleep(window)
    last = writes_seen(hop, eng, db, side)
    if first is None or last is None:
        return (False, "the server's count of writes could not be read")
    if last != first:
        return (False, f"{last - first} writes in {window:g}s after the"
                       " freeze")
    return (True, f"no write in {window:g}s after the freeze")


def bypass(hop, eng, db, way=None):
    """Who can still write through the freeze, as lines for the preflight:
    superusers on PostgreSQL, and on MySQL, for a per-database freeze, the
    accounts whose write privileges are server-wide or come through a
    role; and what each other engine's way (`way`) leaves - a line that
    says "per-database freeze" is one the way cannot reach."""
    return hold_for(hop, eng, "src").exempt(db, way)


# PostgreSQL

class PostgresHold(Hold):
    def rungs(self):
        from .decide import Rung
        return (
            Rung("pg_server", "the server's read-only default for every"
                 " new transaction, with the application's grants and"
                 " sessions per database", gives=frozenset({SERVER, OURS}),
                 needs=(_alone(), _is(
                     "alter system", "migkit's account may change the"
                     " server's settings", "migkit's account may not change"
                     " the server's settings (ALTER SYSTEM)")),
                 because="every database on the server is the hop's"),
            Rung("pg_database", "the application's grants revoked and its"
                 " sessions ended, per database",
                 gives=frozenset({DATABASE, OURS}), because="per database"))

    def readers(self, dbs):
        return {"alter system": _read(lambda: _pg_may_alter(
            self.eng, dbs[0], self.side))}

    def others(self, dbs):
        mine = {self.eng._d(self.side, d) for d in dbs}
        return [d for d in self.eng._psql(
            self.side, dbs[0], "select datname from pg_database where"
                               " datallowconn and not datistemplate"
                               " order by 1").split() if d not in mine]

    def hold(self, dbs, way, say):
        if SCOPE[way] == "server":
            _pg_server_freeze(self.hop, self.eng, dbs, say, self.side)
        for d in dbs:
            _pg_source_freeze(self.hop, self.eng, d, say, self.side)

    def release(self, db, record, say):
        _pg_thaw(self.hop, self.eng, db, record, say, self.side)

    def release_server(self, rec, say):
        _pg_server_thaw(self.hop, self.eng, rec, say, self.side)

    def held(self, db, way):
        return _pg_proof(self.hop, self.eng, db, SCOPE.get(way, way),
                         self.side)

    def exempt(self, db, way):
        eng, side = self.eng, self.side
        got = eng._psql(side, db, "select rolname from pg_roles where"
                                  " rolsuper and rolcanlogin order by 1")
        out = [f"{r} is a superuser: nothing short of stopping it keeps it"
               " from writing" for r in got.split() if r != self.me]
        got = eng._psql(side, db,
                        "select coalesce(r.rolname, 'every role') from"
                        " pg_db_role_setting s left join pg_roles r on"
                        " r.oid = s.setrole, unnest(s.setconfig) c(cfg)"
                        " where c.cfg = 'default_transaction_read_only=off'"
                        " and s.setdatabase in (0, (select oid from"
                        " pg_database where datname = current_database()))")
        out += [f"{r} is set writable by default here, which a server-wide"
                " read-only default does not reach" for r in got.split()
                if r != self.me]
        return out

    def writes(self, db):
        got = self.eng._psql(self.side, db,
                             "select tup_inserted + tup_updated +"
                             " tup_deleted from pg_stat_database"
                             " where datname = current_database()")
        return int(got.strip())

    def open(self, db, older_than):
        eng, side = self.eng, self.side
        got = eng._psql(side, db, "select gid||' ('||owner||', prepared '||"
                                  "to_char(prepared, 'HH24:MI:SS')||')' from"
                                  " pg_prepared_xacts where database ="
                                  " current_database()")
        out = [f"prepared transaction {g}" for g in got.splitlines() if g]
        age = "" if older_than is None else \
            f" and xact_start < now() - interval '{float(older_than)} seconds'"
        got = eng._psql(side, db,
                        "select usename||' pid '||pid||', open '||"
                        "round(extract(epoch from now() - xact_start))||'s'"
                        " from pg_stat_activity where datname ="
                        " current_database() and backend_xid is not null"
                        " and backend_type = 'client backend'"
                        " and pid <> pg_backend_pid()" + age)
        return out + [f"write transaction {g}" for g in got.splitlines()
                      if g]

    def cannot(self, db):
        eng, side = self.eng, self.side
        out = []
        sup, owner, signal = eng._psql(
            side, db, "select r.rolsuper::text||' '||(d.datdba ="
                      " r.oid)::text||' '||pg_has_role(r.oid,"
                      " 'pg_signal_backend', 'member')::text from"
                      " pg_roles r, pg_database d where r.rolname ="
                      " current_user and d.datname = current_database()"
                      ).split()
        if sup == "true":
            return []
        if signal != "true":
            out.append("migkit's account on the source cannot end the"
                       " application's sessions (neither superuser nor"
                       " a member of pg_signal_backend)")
        if owner != "true":
            out.append("migkit's account on the source does not own the"
                       " database, so it cannot take the right to"
                       " connect from a role that owns its tables")
        n = eng._psql(
            side, db, "select count(*) from pg_class c join"
                      " pg_namespace n on n.oid = c.relnamespace where"
                      " c.relkind in ('r', 'p') and n.nspname not in"
                      " ('pg_catalog', 'information_schema') and"
                      " c.relowner <> (select oid from pg_roles where"
                      " rolname = current_user) and not"
                      " has_table_privilege(c.oid, 'INSERT WITH GRANT"
                      " OPTION')").strip()
        if n not in ("", "0"):
            out.append(f"migkit's account on the source can neither"
                       f" revoke nor give back the grants on {n} tables"
                       " it does not own")
        return out


def _pg_may_alter(eng, db, side="src"):
    if eng._psql(side, db, "select rolsuper from pg_roles"
                           " where rolname = current_user").strip() == "t":
        return True
    if _pg_version(eng, db, side) < 150000:
        return False
    return eng._psql(side, db,
                     "select coalesce(has_parameter_privilege("
                     "'default_transaction_read_only', 'ALTER SYSTEM'),"
                     " false)").strip() == "t"


def _pg_version(eng, db, side="src"):
    try:
        return int(eng._psql(side, db, "show server_version_num").strip())
    except (RuntimeError, ValueError):
        return 0


def _pg_source_freeze(hop, eng, db, say, side="src"):
    """`freeze` on this side, then the right to connect taken from
    whatever role can still write, and every application session ended."""
    dbname = eng._d(side, db)
    # the server's clock, not this machine's: the proof asks the server
    # which sessions it opened before this
    at = eng._psql(side, db, "select clock_timestamp()::text").strip()
    record = freeze(hop, eng, db, say, side=side)
    record.setdefault("at", at)
    connect = record.setdefault("connect", {})
    roles = list(record["roles"])
    owners = [r for r in roles if _still_writes(eng, db, r, side)]
    if owners:
        # a role that owns its tables writes whatever is revoked, and the
        # read-only default is one `begin read write` away: kept out of the
        # database instead. Where everyone may connect, that is PUBLIC's
        # right, so every other login role is given it by name first
        if _pg_public_connects(eng, db, side):
            keep = [r for r in eng._psql(
                side, db, "select rolname from pg_roles where rolcanlogin"
                          " and not rolsuper order by 1").split()
                    if r not in roles]
            given = [r for r in keep
                     if not _pg_connects_by_name(eng, db, r, side)]
            connect["granted"] = sorted(set(connect.get("granted", []))
                                        | set(given))
            connect["public"] = True
            _save(hop, db, record, side)
            for r in given:
                eng._psql(side, db, f"grant connect on database"
                                    f" {_q(dbname)} to {_q(r)}")
            eng._psql(side, db, f"revoke connect on database"
                                f" {_q(dbname)} from public")
        for r in owners:
            if _pg_connects_by_name(eng, db, r, side):
                record["roles"][r]["connect"] = True
                _save(hop, db, record, side)
                eng._psql(side, db, f"revoke connect on"
                                    f" database {_q(dbname)} from {_q(r)}")
            say(f"{dbname}: {r} - kept from connecting: it can write as"
                " the owner whatever is revoked")
    _save(hop, db, record, side)
    ended = _pg_end_sessions(eng, db, roles, side)
    say(f"{dbname}: {ended} open sessions of the application ended - what"
        " they had not committed is rolled back, not committed after the"
        " freeze")
    return record


def _pg_public_connects(eng, db, side="src"):
    return eng._psql(
        side, db,
        "select coalesce(bool_or(a.grantee = 0 and a.privilege_type ="
        " 'CONNECT'), false) or bool_or(d.datacl is null) from pg_database d"
        " left join lateral aclexplode(d.datacl) a on true"
        " where d.datname = current_database()").strip() == "t"


def _pg_connects_by_name(eng, db, role, side="src"):
    return eng._psql(
        side, db,
        "select exists(select 1 from pg_database d, aclexplode(d.datacl) a"
        " where d.datname = current_database() and a.privilege_type ="
        f" 'CONNECT' and a.grantee = {_lit(role)}::regrole)").strip() == "t"


def _pg_connect_back(eng, db, dbname, record, side):
    connect = record.get("connect") or {}
    if connect.get("public"):
        eng._psql(side, db, f"grant connect on database"
                            f" {_q(dbname)} to public")
    for r in connect.get("granted", []):
        eng._psql(side, db, f"revoke connect on database"
                            f" {_q(dbname)} from {_q(r)}")


def _pg_end_sessions(eng, db, roles, side="src", wait=10):
    """End every session of these roles in this database and wait until
    the server has let them go: `pg_terminate_backend` only signals."""
    if not roles:
        return 0
    names = ", ".join(_lit(r) for r in roles)
    who = (f" from pg_stat_activity where usename in ({names})"
           " and datname = current_database() and pid <> pg_backend_pid()")
    ended = int(eng._psql(side, db, "select count(pg_terminate_backend(pid))"
                          + who).strip() or 0)
    end = time.monotonic() + wait
    while time.monotonic() < end:
        if eng._psql(side, db, "select count(*)" + who).strip() == "0":
            break
        time.sleep(0.1)
    return ended


def _pg_server_freeze(hop, eng, dbs, say, side="src"):
    """`default_transaction_read_only` for the whole server, migkit's own
    role exempted in each database so its reads, the stream back and the
    undo still write. What was in the server's own file for the setting
    is recorded first.

    Measured on PostgreSQL 16 with the default on: an application session
    was refused its insert, and the same session's `begin read write` let
    it in - which is why the grants go as well; `create subscription`, the
    stream back, was refused without the exemption; and a subscription's
    apply worker wrote regardless, 0.02s after the row was written on the
    other side, with no apply error, also after the worker restarted -
    so the old source still follows the new primary while frozen."""
    first = dbs[0]
    rec = server_state(hop, side) or {}
    if "before" not in rec:
        rec["before"] = eng._psql(
            side, first,
            "select setting from pg_file_settings where name ="
            " 'default_transaction_read_only' and sourcefile like"
            " '%postgresql.auto.conf' order by seqno desc limit 1").strip()
        rec["ours"] = {}
        rec["engine"] = "postgres"
        _server_save(hop, rec, side)
    me = (hop.source if side == "src" else hop.target).user
    for d in dbs:
        if d not in rec["ours"]:
            rec["ours"][d] = _setting(eng, d, me, side)
            _server_save(hop, rec, side)
        eng._psql(side, d, f"alter role {_q(me)} in database"
                           f" {_q(eng._d(side, d))} set"
                           " default_transaction_read_only = off")
    eng._psql(side, first, "alter system set default_transaction_read_only"
                           " = on")
    eng._psql(side, first, "select pg_reload_conf()")
    rec["on"] = True
    _server_save(hop, rec, side)
    say(f"{'source' if side == 'src' else 'target'}: every new transaction"
        " on the server is read-only (default_transaction_read_only),"
        " migkit's own role excepted")


def _pg_server_thaw(hop, eng, rec, say, side="src"):
    dbs = list(rec.get("ours") or {})
    first = dbs[0] if dbs else "postgres"
    before = rec.get("before")
    eng._psql(side, first, "alter system "
              + (f"set default_transaction_read_only = {_lit(before)}"
                 if before else "reset default_transaction_read_only"))
    eng._psql(side, first, "select pg_reload_conf()")
    me = (hop.source if side == "src" else hop.target).user
    for d, was in (rec.get("ours") or {}).items():
        eng._psql(side, d, f"alter role {_q(me)} in database"
                           f" {_q(eng._d(side, d))} "
                  + (f"set default_transaction_read_only = {was}"
                     if was is not None
                     else "reset default_transaction_read_only"))
    say(f"{'source' if side == 'src' else 'target'}: the server's read-only"
        " default taken off again")


def _pg_proof(hop, eng, db, how, side="src"):
    out = []
    rec = state(hop, db, side) or {"roles": {}}
    roles = list(rec["roles"])
    names = ", ".join(_lit(r) for r in roles) or "''"
    # opened before the freeze: one opened since is refused a write, or a
    # connection, by what the freeze took
    left = eng._psql(side, db, "select count(*) from pg_stat_activity"
                               f" where usename in ({names}) and datname ="
                               " current_database() and backend_start <"
                               f" {_lit(rec.get('at') or 'now')}::timestamptz"
                     ).strip()
    out.append((left == "0", f"{left} sessions of the application's roles"
                              " left from before the freeze"))
    for r in roles:
        can = eng._psql(side, db, f"select has_database_privilege({_lit(r)},"
                                  " current_database(), 'CONNECT')").strip()
        writes = _still_writes(eng, db, r, side)
        out.append((can != "t" or not writes,
                    f"{r}: " + ("cannot connect" if can != "t" else
                                "can connect and write to "
                                f"{len(writes)} tables" if writes else
                                "can connect, writes to nothing")))
    writing = eng._psql(
        side, db, "select coalesce(string_agg(usename||' pid '||pid, ', '),"
                  " '') from pg_stat_activity where datname ="
                  " current_database() and backend_type = 'client backend'"
                  " and backend_xid is not null"
                  " and pid <> pg_backend_pid()").strip()
    out.append((not writing, f"writing now: {writing}" if writing
                else "no session holds a write transaction"))
    prepared = eng._psql(side, db, "select count(*) from pg_prepared_xacts"
                                   " where database = current_database()"
                         ).strip()
    out.append((prepared == "0", f"{prepared} prepared transactions"))
    if how == "server":
        got = eng._psql(side, db,
                        "select setting from pg_file_settings where name ="
                        " 'default_transaction_read_only' and applied"
                        " order by seqno desc limit 1").strip()
        out.append((got == "on", "the server's default is read-only"
                    if got == "on" else "the server's default is not"
                    " read-only"))
    return out


# MySQL

class MySQLHold(Hold):
    def rungs(self):
        from .decide import Rung
        return (
            Rung("my_server", "the server read-only for every account, its"
                 " administrators too", gives=frozenset({SERVER}),
                 needs=(_alone(), _is(
                     "read only switch", "migkit's account may switch the"
                     " server read-only", "migkit's account may not change"
                     " the server's read_only")),
                 because="every database on the server is the hop's"),
            Rung("my_database", "the application's grants revoked and its"
                 " sessions ended, per database",
                 gives=frozenset({DATABASE, OURS}), because="per database"))

    def readers(self, dbs):
        return {"read only switch": _read(lambda: _my_may_switch(
            self.eng, self.side))}

    def others(self, dbs):
        have = {r[0] for r in self.eng._q(
            self.side, "select schema_name from information_schema.schemata")}
        return sorted(have - {self.eng._d(self.side, d) for d in dbs}
                      - {"mysql", "sys", "information_schema",
                         "performance_schema"})

    def hold(self, dbs, way, say):
        if SCOPE[way] == "server":
            _my_server_freeze(self.hop, self.eng, say, self.side)
        for d in dbs:
            if SCOPE[way] == "server":
                # nothing to revoke under super_read_only - and a revoke
                # would be refused by it: the sessions are ended, so what
                # they hold open is rolled back now rather than failing at
                # its commit
                _my_end_sessions(self.hop, self.eng, d, say, self.side)
            else:
                _my_freeze(self.hop, self.eng, d, say, side=self.side)

    def release(self, db, record, say):
        _my_thaw(self.hop, self.eng, db, record, say, self.side)

    def release_server(self, rec, say):
        _my_server_thaw(self.hop, self.eng, rec, say, self.side)

    def held(self, db, way):
        return _my_proof(self.hop, self.eng, db, SCOPE.get(way, way),
                         self.side)

    def exempt(self, db, way):
        out = []
        for u in app_roles(self.hop, self.eng, db, self.side):
            wider = _my_wider(self.eng, u, self.side)
            if wider:
                out.append(f"{u} {' and '.join(wider)}: a per-database"
                           " freeze does not reach that")
        return out

    def writes(self, db):
        got = self.eng._q(self.side,
                          "select coalesce(sum(count_write), 0) from"
                          " performance_schema"
                          ".table_io_waits_summary_by_table"
                          " where object_schema = %s",
                          (self.eng._d(self.side, db),))
        return int(got[0][0])

    def open(self, db, older_than):
        eng, side = self.eng, self.side
        out = []
        try:
            got = eng._q(side, "xa recover")
            out += [f"prepared XA transaction {r[3]!r}" for r in got]
        except Exception as e:  # noqa: BLE001 - no right to ask
            out.append(f"XA RECOVER could not be read ({e})")
        age = "" if older_than is None else \
            f" and t.trx_started < now() - interval {float(older_than)} second"
        got = eng._q(side,
                     "select p.user, t.trx_mysql_thread_id,"
                     " timestampdiff(second, t.trx_started, now())"
                     " from information_schema.innodb_trx t join"
                     " information_schema.processlist p"
                     " on p.id = t.trx_mysql_thread_id"
                     " where t.trx_rows_modified > 0 and p.user <> %s"
                     " and p.user not in ('system user', 'event_scheduler')"
                     + age, (self.me,))
        return out + [f"write transaction {u} thread {i}, open {s}s"
                      for u, i, s in got]

    def cannot(self, db):
        out = []
        grants = " ".join(str(r[0]).upper() for r in self.eng._q(
            self.side, "show grants for current_user()"))
        wide = "ALL PRIVILEGES ON *.*" in grants
        if not (wide or "SUPER" in grants or "CONNECTION_ADMIN" in grants):
            out.append("migkit's account on the source cannot end other"
                       " accounts' sessions (CONNECTION_ADMIN)")
        if "WITH GRANT OPTION" not in grants:
            out.append("migkit's account on the source cannot revoke or"
                       " give back the application's grants (GRANT"
                       " OPTION)")
        return out


def _my_may_switch(eng, side="src"):
    grants = " ".join(str(r[0]).upper() for r in eng._q(
        side, "show grants for current_user()"))
    return any(p in grants for p in ("ALL PRIVILEGES ON *.*", "SUPER",
                                     "SYSTEM_VARIABLES_ADMIN"))


def _my_brand(eng, side):
    return eng._brands()[0 if side == "src" else 1].name


def _my_end_sessions(hop, eng, db, say, side="src"):
    """The application's sessions on a side the whole server of which is
    read-only, ended; its accounts recorded, with nothing revoked."""
    record = state(hop, db, side) or {"db": db, "roles": {}}
    users = app_roles(hop, eng, db, side)
    for user in users:
        record["roles"].setdefault(user, {"revoked": []})
    _save(hop, db, record, side)
    ended = _my_kill(eng, side, _my_sessions(eng, users, side))
    say(f"{db}: {ended} open sessions of the application ended - what they"
        " had not committed is rolled back")
    return record


def _my_server_freeze(hop, eng, say, side="src"):
    """`super_read_only`: no account writes, an administrator neither -
    only the server's own replication applies. Persisted, so a restart in
    the rollback window cannot make the old source writable; what was set
    and persisted before is recorded first."""
    rec = server_state(hop, side) or {}
    brand = _my_brand(eng, side)
    if "before" not in rec:
        if brand == "mariadb":
            got = eng._q(side, "select @@global.read_only")
            rec["before"] = {"read_only": int(got[0][0])}
        else:
            got = eng._q(side, "select @@global.read_only,"
                               " @@global.super_read_only")
            rec["before"] = {"read_only": int(got[0][0]),
                             "super_read_only": int(got[0][1])}
            rec["persisted"] = sorted(r[0] for r in eng._q(
                side, "select variable_name from"
                      " performance_schema.persisted_variables where"
                      " variable_name in ('read_only', 'super_read_only')"))
        rec["engine"] = "mysql"
        rec["brand"] = brand
        _server_save(hop, rec, side)
    if brand == "mariadb":
        eng._q(side, "set global read_only = 1")
    else:
        eng._q(side, "set persist super_read_only = on")
    rec["on"] = True
    _server_save(hop, rec, side)
    say(f"{'source' if side == 'src' else 'target'}: the whole server is"
        " read-only ("
        + ("read_only" if brand == "mariadb" else "super_read_only")
        + "), its own replication excepted")


def _my_server_thaw(hop, eng, rec, say, side="src"):
    before = rec.get("before") or {}
    if rec.get("brand") == "mariadb":
        eng._q(side, f"set global read_only = {int(before.get('read_only', 0))}")
    else:
        persisted = set(rec.get("persisted") or [])
        eng._q(side, "set global super_read_only ="
                     f" {int(before.get('super_read_only', 0))}")
        eng._q(side, f"set global read_only = {int(before.get('read_only', 0))}")
        for name in ("super_read_only", "read_only"):
            if name in persisted:
                eng._q(side, f"set persist {name} = {int(before.get(name, 0))}")
            else:
                try:
                    eng._q(side, f"reset persist if exists {name}")
                except Exception:  # noqa: BLE001 - nothing persisted
                    pass
    say(f"{'source' if side == 'src' else 'target'}: writable again, as it"
        " was before the freeze")


def _my_proof(hop, eng, db, how, side="src"):
    out = []
    rec = state(hop, db, side) or {"roles": {}}
    users = list(rec["roles"])
    if how == "server":
        var = ("read_only" if _my_brand(eng, side) == "mariadb"
               else "super_read_only")
        got = eng._q(side, f"select @@global.{var}")
        on = bool(got and int(got[0][0]))
        out.append((on, f"{var} is {'on' if on else 'off'}"))
    for u in users:
        left = _my_grants(eng, eng._d(side, db), u, side)
        wider = _my_wider(eng, u, side)
        out.append((how == "server" or not (left or wider),
                    f"{u}: " + ("no write grant on the database" if not left
                                else f"{len(left)} write grants left")
                    + (f", {' and '.join(wider)}" if wider else "")))
    xa = open_work(hop, eng, db, side, None)
    out.append((not xa, "; ".join(xa) if xa else
                "no prepared XA or open write transaction"))
    return out


# MongoDB
# The whole server: `setUserWriteBlockMode` (6.0 on), which every account
# obeys but one holding `bypassWriteBlockingMode` - migkit's own, which
# its tail needs, and any other `bypass` names - and before 6.0
# `fsyncLock`, which stops every write, the server's own too. Per
# database: the application's accounts lose the roles that write there
# for `read` on it, recorded first, and their sessions are ended, which
# aborts what they held open. Measured on 7.0 with the block on: an
# account with `readWrite` was refused (371, `User writes blocked`), `root`
# wrote, and a transaction the application had opened before could not
# commit - it was aborted.

MONGO_SYSTEM = ("admin", "local", "config")
_MONGO_WRITE_ROLES = ("readWrite", "dbOwner")


class MongoHold(Hold):
    def rungs(self):
        from .decide import Need, Rung
        return (
            Rung("mongo_block", "users' writes blocked on the server",
                 gives=frozenset({SERVER}),
                 needs=(_alone(), _at_least(
                     "version", (6, 0), "a block on users' writes needs"
                     " 6.0"), Need(
                     "the right to block users' writes",
                     lambda f: "setUserWriteBlockMode" in (f["actions"]
                                                           or ()),
                     "migkit's account may not block the server's"
                     " writes")),
                 because="every database on the server is the hop's"),
            Rung("mongo_fsync", "the server locked, its own writes too",
                 gives=frozenset({SERVER}),
                 needs=(_alone(), Need(
                     "the right to lock the server",
                     lambda f: {"fsync", "unlock"} <= set(f["actions"]
                                                          or ()),
                     "migkit's account may not block the server's"
                     " writes")),
                 because="every database on the server is the hop's;"
                         " before 6.0 it has no write block, so it is"
                         " locked (fsyncLock)"),
            Rung("mongo_database", "the application's accounts given read"
                 " for the roles that write here, their sessions ended",
                 gives=frozenset({DATABASE, OURS}), because="per database"))

    def readers(self, dbs):
        c = self.eng._client(self.side)
        return {"version": _read(lambda: tuple(c.admin.command(
                    "buildInfo").get("versionArray", [0, 0])[:2])),
                "actions": _read(lambda: _mongo_actions(c))}

    def others(self, dbs):
        return sorted(set(self.eng._client(self.side).list_database_names())
                      - set(MONGO_SYSTEM)
                      - {self.eng._d(self.side, d) for d in dbs})

    def hold(self, dbs, way, say):
        how = SCOPE[way]
        # the sessions first: the block waits for every transaction open
        # when it is asked, measured on 7.0 at 87.7s for one the
        # application held - its whole lifetime limit, all of it outage
        for d in dbs:
            _mongo_freeze(self.hop, self.eng, d, say, how, self.side)
        if how == "server":
            _mongo_server_freeze(self.hop, self.eng, say,
                                 "block" if way == "mongo_block"
                                 else "fsync", self.side)

    def release(self, db, record, say):
        _mongo_thaw(self.hop, self.eng, db, record, say, self.side)

    def release_server(self, rec, say):
        _mongo_server_thaw(self.hop, self.eng, rec, say, self.side)

    def held(self, db, way):
        return _mongo_proof(self.hop, self.eng, db, SCOPE.get(way, way),
                            self.side)

    def exempt(self, db, way):
        out = []
        for u, d, roles, bypass in _mongo_users(self.hop, self.eng,
                                                self.side):
            wide = [r["role"] for r in roles if r.get("role") in (
                "readWriteAnyDatabase", "root", "restore", "__system")]
            if bypass:
                out.append(f"{u}@{d} may write through a blocked server"
                           " (bypassWriteBlockingMode)")
            elif wide:
                out.append(f"{u}@{d} writes to every database"
                           f" ({', '.join(wide)}): a per-database freeze"
                           " does not reach that")
        return out

    def writes(self, db):
        got = self.eng._client(self.side).admin.command("top").get(
            "totals", {})
        name = self.eng._d(self.side, db)
        return sum(int((v.get(op) or {}).get("count", 0))
                   for ns, v in got.items()
                   if isinstance(v, dict) and ns.startswith(f"{name}.")
                   for op in ("insert", "update", "remove"))

    def open(self, db, older_than):
        return _mongo_open(self.eng, db, older_than, self.side)


def _mongo_actions(c):
    status = c.admin.command({"connectionStatus": 1, "showPrivileges": True})
    return sorted({a for p in (status.get("authInfo") or {}).get(
        "authenticatedUserPrivileges", [])
        if (p.get("resource") or {}).get("cluster")
        for a in p.get("actions", [])})


def _mongo_users(hop, eng, side="src"):
    """[(user, its database, roles, whether it may write through a
    blocked server)] of every account on the side, but migkit's and the
    server's own. The privileges are asked one account at a time: the
    server gives them only to a query naming the account (measured on 7.0,
    `usersInfo: {forAllDBs: true}` with `showPrivileges` is refused)."""
    c = eng._client(side)
    me = (hop.source if side == "src" else hop.target).user
    got = c.admin.command({"usersInfo": {"forAllDBs": True}})
    out = []
    for u in got.get("users", []):
        if u["user"] == me or u["db"] in ("local", "config") \
                or u["user"].startswith("__"):
            continue
        one = c[u["db"]].command({"usersInfo": u["user"],
                                  "showPrivileges": True})["users"][0]
        out.append((u["user"], u["db"], u.get("roles", []),
                    any("bypassWriteBlockingMode" in p.get("actions", [])
                        for p in one.get("inheritedPrivileges", []))))
    return out


def _mongo_end_sessions(eng, users, side="src"):
    """The sessions these accounts hold, ended - which aborts a
    transaction open in one - and the operations they run without one.
    Found through `$currentOp` and ended by id: the pattern that names
    accounts (`killAllSessionsByPattern` with `users`) needs a privilege
    even `root` does not hold (measured on 7.0: `Not authorized to
    impersonate`)."""
    if not users:
        return
    names = {(u, d) for u, d in users}
    c = eng._client(side)
    lsids, ops = [], []
    for op in c.admin.aggregate([{"$currentOp": {
            "allUsers": True, "idleSessions": True}}]):
        who = {(x.get("user"), x.get("db"))
               for x in op.get("effectiveUsers") or []}
        if not who & names:
            continue
        if (op.get("lsid") or {}).get("id") is not None:
            lsids.append({"id": op["lsid"]["id"]})
        elif op.get("opid") is not None:
            ops.append(op["opid"])
    if lsids:
        c.admin.command("killSessions", lsids)
    for opid in ops:
        c.admin.command("killOp", op=opid)


def _mongo_server_freeze(hop, eng, say, method, side="src"):
    """`method` is the way the ladder took (`MongoHold.rungs`): the block
    where the version has it and migkit's account may set it, the lock
    otherwise - a 6.0 server whose block migkit may not set is locked,
    where the version alone had chosen a block it was refused."""
    c = eng._client(side)
    rec = server_state(hop, side) or {}
    if "method" not in rec:
        rec.update({"engine": "mongodb", "method": method,
                    "way": "mongo_block" if method == "block"
                    else "mongo_fsync"})
        _server_save(hop, rec, side)
    if rec["method"] == "block":
        c.admin.command({"setUserWriteBlockMode": 1, "global": True})
    elif not rec.get("on"):
        c.admin.command("fsync", lock=True)
    rec["on"] = True
    _server_save(hop, rec, side)
    say(f"{'source' if side == 'src' else 'target'}: every account's writes"
        " blocked on the server"
        + (" (setUserWriteBlockMode)" if rec["method"] == "block"
           else " (fsyncLock)"))


def _mongo_server_thaw(hop, eng, rec, say, side="src"):
    c = eng._client(side)
    if rec.get("method") == "block":
        c.admin.command({"setUserWriteBlockMode": 1, "global": False})
    elif rec.get("on"):
        c.admin.command("fsyncUnlock")
    say(f"{'source' if side == 'src' else 'target'}: writes given back to"
        " the server")


def _mongo_freeze(hop, eng, db, say, how, side="src"):
    """The application's accounts on this database: per database, the
    roles that write here swapped for `read` on it; either way, their
    sessions ended."""
    name = eng._d(side, db)
    record = state(hop, db, side) or {"db": name, "roles": {}}
    users = [(u, d, roles) for u, d, roles, bypass
             in _mongo_users(hop, eng, side) if not bypass]
    ended = []
    for user, udb, roles in users:
        key = f"{udb}.{user}"
        todo = [r for r in roles if r.get("db") == name
                and r.get("role") in _MONGO_WRITE_ROLES]
        if how == "server" or todo or key in record["roles"]:
            ended.append((user, udb))
        if how == "server" or not todo:
            record["roles"].setdefault(key, {"user": user, "db": udb,
                                             "revoked": [], "read": False})
            continue
        entry = record["roles"].setdefault(key, {"user": user, "db": udb,
                                                 "revoked": [], "read": False})
        todo = [r for r in todo if r not in entry["revoked"]]
        reads = any(r.get("db") == name and r.get("role") == "read"
                    for r in roles)
        entry["revoked"] += todo
        entry["read"] = entry["read"] or (bool(todo) and not reads)
        # recorded before it is done, as everywhere here
        _save(hop, db, record, side)
        c = eng._client(side)[udb]
        if todo:
            c.command("revokeRolesFromUser", user, roles=todo)
            if entry["read"]:
                c.command("grantRolesToUser", user,
                          roles=[{"role": "read", "db": name}])
    _save(hop, db, record, side)
    _mongo_end_sessions(eng, ended, side)
    say(f"{name}: the sessions of {len(ended)} accounts ended"
        + ("" if how == "server" else ", the roles that write here"
           " swapped for read"))
    return record


def _mongo_thaw(hop, eng, db, record, say, side):
    name = record.get("db") or eng._d(side, db)
    for entry in record["roles"].values():
        c = eng._client(side)[entry["db"]]
        if entry["revoked"]:
            c.command("grantRolesToUser", entry["user"],
                      roles=entry["revoked"])
        if entry.get("read"):
            c.command("revokeRolesFromUser", entry["user"],
                      roles=[{"role": "read", "db": name}])
    say(f"{name}: the accounts' roles given back")


def _mongo_blocked(eng, side="src"):
    """Whether the server blocks users' writes now, as it keeps it."""
    c = eng._client(side)
    rec = c.config["user_writes_critical_sections"].find_one({})
    return bool(rec and any(v is True for k, v in rec.items()
                            if "write" in k.lower()))


def _mongo_open(eng, db, older_than, side="src"):
    """Open multi-document transactions on the server, as lines."""
    c = eng._client(side)
    got = c.admin.aggregate([
        {"$currentOp": {"allUsers": True, "idleSessions": True}},
        {"$match": {"transaction": {"$exists": True}}}])
    out = []
    for op in got:
        secs = int((op.get("transaction") or {}).get("timeOpenMicros", 0)
                   ) / 1e6
        if older_than is not None and secs < older_than:
            continue
        who = ",".join(u.get("user", "?") for u in
                       op.get("effectiveUsers") or []) or "?"
        out.append(f"write transaction {who}, open {secs:.0f}s")
    return out


def _mongo_proof(hop, eng, db, how, side="src"):
    out = []
    rec = server_state(hop, side) or {}
    if how == "server" and rec.get("method") == "block":
        on = _mongo_blocked(eng, side)
        out.append((on, "users' writes are blocked on the server" if on
                    else "users' writes are not blocked on the server"))
    elif how == "server":
        on = bool(eng._client(side).admin.command("currentOp").get(
            "fsyncLock"))
        out.append((on, "the server is locked" if on
                    else "the server is not locked"))
    held = state(hop, db, side) or {"roles": {}}
    name = eng._d(side, db)
    writers = [f"{u}@{d}" for u, d, roles, bypass
               in _mongo_users(hop, eng, side)
               if not bypass and how != "server"
               and any(r.get("db") == name and r.get("role")
                       in _MONGO_WRITE_ROLES for r in roles)]
    out.append((not writers, f"still writing here: {', '.join(writers)}"
                if writers else f"{len(held['roles'])} accounts hold no"
                " write here"))
    left = _mongo_open(eng, db, None, side)
    out.append((not left, "; ".join(left[:3]) if left
                else "no open transaction"))
    return out


# Redis
# The whole server: `CLIENT PAUSE WRITE` (6.2 on) holds every client's
# write commands - an administrator's too, never the server's own
# replication - and the connections open before it are ended, so nothing
# the application sent before the hold is applied after the position
# the drain waits for. Held, not refused: a write sent while it is on
# waits. It lives in the server's memory only (a restart lifts it) and
# lasts until migkit lifts it (`CLIENT UNPAUSE`), its own limit set a year
# out. Not where a stream back is asked: the stream back applies on the
# old source as writes, which a server holding its writes holds too.
# Per account: every account the server keeps but the replication one
# loses its write commands (`-@write`), its rules recorded whole first
# (`ACL LIST`) and put back whole - migkit's own among them, since
# nothing migkit does to a source at cutover is a write: it reads
# positions, and ends and makes replicas, which the server's replication
# applies whatever an account may do. Kept across a restart where the
# server keeps its accounts in a file of their own (`aclfile`), and read
# back by asking the server whether each account could write
# (`ACL DRYRUN`, 7.0).

PAUSED_FOR_MS = 365 * 24 * 3600 * 1000


def _acl_words(line):
    """The words of one `ACL LIST` line, a selector in parentheses kept
    as one word, as `ACL SETUSER` takes it back."""
    out, held = [], None
    for w in str(line).split(" "):
        if held is not None:
            held.append(w)
            if w.endswith(")"):
                out.append(" ".join(held))
                held = None
        elif w.startswith("(") and not w.endswith(")"):
            held = [w]
        elif w:
            out.append(w)
    if held:
        out.append(" ".join(held))
    return out


class RedisHold(Hold):
    def rungs(self):
        from .decide import Need, Rung
        return (
            Rung("redis_pause", "every client's writes held on the server,"
                 " the connections open before it ended",
                 gives=frozenset({SERVER}),
                 needs=(_alone(), _at_least(
                     "version", (6, 2), "holding writes alone needs 6.2"),
                     Need("no stream back to it",
                          lambda f: not f["stream back"],
                          "the stream back applies on the old source as"
                          " writes, which a server holding its writes"
                          " holds as well")),
                 because="every database on the server is the hop's"),
            Rung("redis_accounts", "every account's writes refused, the"
                 " connections open before it ended",
                 gives=frozenset({DATABASE}),
                 needs=(_at_least(
                     "version", (7, 0), "an account's writes are read back"
                     " as refused from 7.0 (ACL DRYRUN)"), _is(
                     "acl", "accounts on the server", "the server keeps"
                     " no accounts (ACL)")),
                 because="per account"))

    def readers(self, dbs):
        return {"version": _read(self.version),
                "acl": _read(lambda: self.eng._acl(self.side)),
                "stream back": lambda: self.side == "src" and str(
                    (self.hop.options or {}).get("reverse", "") or ""
                ).lower() == "at_cutover"}

    def c(self):
        return self.eng._client(self.side)

    def version(self):
        got = str(self.c().info("server").get("redis_version") or "")
        return tuple(int(x) for x in got.split(".")[:2])

    def others(self, dbs):
        mine = {str(d) for d in dbs}
        return sorted(k[2:] for k in self.c().info("keyspace")
                      if k[2:] not in mine)

    def rules(self):
        """{account: its rules} as the server lists them."""
        out = {}
        for line in self.c().execute_command("ACL", "LIST"):
            words = _acl_words(line)
            if len(words) >= 2 and words[0] == "user":
                out[words[1]] = words[2:]
        return out

    def accounts(self):
        rules = self.rules()
        return [u for u in (_named(self.hop) or sorted(rules))
                if u in rules and u != self.eng.REPL_USER]

    @staticmethod
    def clients(c, users=None, before=None):
        """[(id, account)] of the clients but this one and the server's
        own replication - of `users` where given, and connected more than
        `before` seconds ago where given."""
        me = int(c.client_id())
        out = []
        for cl in c.client_list():
            flags = str(cl.get("flags") or "")
            if int(cl["id"]) == me or "S" in flags or "M" in flags:
                continue
            if users is not None and cl.get("user") not in users:
                continue
            if before is not None and int(cl.get("age") or 0) < before:
                continue
            out.append((int(cl["id"]), cl.get("user")))
        return out

    @staticmethod
    def end(c, clients):
        ended = 0
        for i, _ in clients:
            try:
                ended += int(c.execute_command("CLIENT", "KILL", "ID", i,
                                               "SKIPME", "yes") or 0)
            except Exception:  # noqa: BLE001 - it went on its own
                pass
        return ended

    def kept_in_a_file(self):
        """Whether the server keeps its accounts in a file of their own."""
        try:
            got = self.c().config_get("aclfile")
        except Exception:  # noqa: BLE001 - not an answer: no file
            return False
        return bool((got or {}).get("aclfile"))

    def hold(self, dbs, way, say):
        c = self.c()
        rec = server_state(self.hop, self.side) or {
            "engine": "redis", "way": way, "at": time.time(), "rules": {}}
        if way == "redis_pause":
            _server_save(self.hop, rec, self.side)
            c.execute_command("CLIENT", "PAUSE", PAUSED_FOR_MS, "WRITE")
            rec["on"] = True
            _server_save(self.hop, rec, self.side)
            ended = self.end(c, self.clients(c))
            say(f"every client's writes held on the server; {ended}"
                " connections open before it ended - what they had sent is"
                " not applied")
            return
        rules = self.rules()
        accounts = self.accounts()
        for u in accounts:
            rec["rules"].setdefault(u, rules[u])
        rec["file"] = self.kept_in_a_file()
        # recorded before it is done, as everywhere here
        _server_save(self.hop, rec, self.side)
        for u in accounts:
            c.execute_command("ACL", "SETUSER", u, "-@write")
        if rec["file"]:
            c.execute_command("ACL", "SAVE")
        ended = self.end(c, self.clients(c, set(accounts)))
        say(f"{len(accounts)} accounts refused every write; {ended}"
            " connections open before it ended")

    def release_server(self, rec, say):
        c = self.c()
        if rec.get("way") == "redis_pause":
            if rec.get("on"):
                c.execute_command("CLIENT", "UNPAUSE")
            say("its clients' writes go through again")
            return
        for u, rules in (rec.get("rules") or {}).items():
            c.execute_command("ACL", "SETUSER", u, "reset", *rules)
        if rec.get("file"):
            c.execute_command("ACL", "SAVE")
        say(f"{len(rec.get('rules') or {})} accounts given back their"
            " rules as they were")

    def writes(self, db):
        """The server's running count of write commands carried out."""
        c = self.c()
        writes = {str(n).lower() for n, info in c.command().items()
                  if "write" in [str(f) for f in (info.get("flags") or [])]}
        return sum(int(v.get("calls", 0)) for k, v in
                   c.info("commandstats").items()
                   if k.startswith("cmdstat_")
                   and k[8:].split("|")[0].lower() in writes)

    def held(self, db, way):
        c = self.c()
        rec = server_state(self.hop, self.side) or {}
        since = max(0, int(time.time() - float(rec.get("at")
                                               or time.time())))
        out = []
        if way == "redis_pause":
            held = c.info("clients").get("paused_actions")
            if held is not None:
                # the server says what it holds from 7.4; before, the
                # count of writes standing still is what shows it
                out.append(("write" in str(held) or "all" in str(held),
                            f"the server holds: {held or 'nothing'}"))
            left = self.clients(c, before=since + 1)
        else:
            accounts = list(rec.get("rules") or {})
            writers = [u for u in accounts if str(c.execute_command(
                "ACL", "DRYRUN", u, "SET", "migkit:probe", "1")) == "OK"]
            out.append((not writers, f"still able to write:"
                        f" {', '.join(writers)}" if writers else
                        f"{len(accounts)} accounts refused a write"))
            left = self.clients(c, set(accounts), before=since + 1)
        out.append((not left, f"{len(left)} connections left from before"
                              " the freeze"))
        out.append(_held_still(self.hop, self.eng, db, self.side))
        return out

    def exempt(self, db, way):
        if way == "redis_pause":
            return ["a restart lifts the hold on its writes: the server"
                    " keeps it in memory only"]
        if not self.kept_in_a_file():
            return ["a restart gives its accounts back their writes: it"
                    " keeps them in its configuration, which migkit does"
                    " not rewrite"]
        return []


# SQL Server
# Per database, either way. Read-only first: `ALTER DATABASE ... SET
# READ_ONLY WITH ROLLBACK IMMEDIATE` rolls back what every other session
# holds open and ends it, and nobody writes a read-only database, an
# administrator neither - a setting of the database, so it outlives a
# restart. Not for a database in an availability group, which takes no
# such switch. Otherwise the application's users are denied INSERT,
# UPDATE and DELETE in the database - a deny outranks every grant but an
# owner's - what each held at the database's scope recorded first and
# put back exactly (a grant, a grant with its option, or nothing), and
# their sessions ended, which rolls back what they held open. The change
# tracking a follow reads is read the same in a read-only database.

def _ms_lit(text):
    return "N'" + str(text).replace("'", "''") + "'"


class MSSQLHold(Hold):
    def rungs(self):
        from .decide import Rung
        return (
            Rung("mssql_read_only", "the database read-only for everyone,"
                 " what was open rolled back", gives=frozenset({DATABASE}),
                 needs=(_is("alter", "migkit's account may alter the"
                            " database", "migkit's account may not alter"
                            " the database (ALTER)"),
                        _is("alone in its group", "a database of its own",
                            "the database is in an availability group,"
                            " which takes no read-only switch")),
                 because="read-only stops every account on it"),
            Rung("mssql_deny", "the application's users denied every write"
                 " in the database, their sessions ended",
                 gives=frozenset({DATABASE, OURS}),
                 needs=(_is("control", "migkit's account may deny and give"
                            " back", "migkit's account may not deny or"
                            " give back the users' rights (CONTROL)"),
                        _is("kill", "migkit's account may end sessions",
                            "migkit's account may not end other sessions"
                            " (ALTER ANY CONNECTION)")),
                 because="per user"))

    def readers(self, dbs):
        return {
            "alter": _read(lambda: all(self.perm(d, "ALTER") for d in dbs)),
            "alone in its group": _read(lambda: all(self.alone(d)
                                                    for d in dbs)),
            "control": _read(lambda: all(self.perm(d, "CONTROL")
                                         for d in dbs)),
            "kill": _read(lambda: int(self.one(
                dbs[0], "select has_perms_by_name(null, null,"
                        " 'ALTER ANY CONNECTION')") or 0) == 1)}

    def name(self, db):
        return self.eng._d(self.side, db)

    def run(self, db, *sql):
        """Statements outside any transaction: `ALTER DATABASE` refuses
        one, and a deny left in an open one holds nobody. `db` "master" is
        the server's own."""
        conn = self.eng._connect(self.side, db)
        try:
            conn.autocommit(True)
            cur = conn.cursor()
            for s in sql:
                cur.execute(s)
        finally:
            conn.close()

    def one(self, db, sql):
        got = self.eng._rows(self.side, db, sql)
        return got[0][0] if got else None

    def switch(self, db, sql):
        """A switch of the whole database, which ends every other session
        in it - migkit's own change tail's among them, whose query in
        flight the driver reports as a dead process, not a lost
        connection, and the tail stops on it. So the tail is held between
        its batches while the switch runs (`tailctl.pause`), and let go
        after - one held already (the cutover holds its tail from the
        flip on) is left held."""
        from . import tailctl
        where = self.hop.report_dir(db)
        held = not (where / tailctl.PAUSE).exists() \
            and tailctl.alive(where) and tailctl.pause(where, 30)
        try:
            self.run("master", sql)
        finally:
            if held:
                tailctl.resume(where)

    def perm(self, db, perm):
        return int(self.one(db, "select has_perms_by_name("
                                f"{_ms_lit(self.name(db))}, 'DATABASE',"
                                f" '{perm}')") or 0) == 1

    def alone(self, db):
        return self.one(db, "select case when replica_id is null then 1"
                            " else 0 end from sys.databases where name ="
                            f" {_ms_lit(self.name(db))}") == 1

    def users(self, db):
        """The application's users in the database: every one but
        migkit's, the database's own and the system's."""
        got = [r[0] for r in self.eng._rows(
            self.side, db, "select name from sys.database_principals where"
                           " type in ('S', 'U', 'G', 'E', 'X') and"
                           " principal_id > 4 and name not like '##%' and"
                           " sid is not null and sid <> suser_sid() and"
                           " name <> user_name() order by 1")]
        named = _named(self.hop)
        return [u for u in named if u in got] if named else got

    def sessions(self, db, users, before=None):
        if not users:
            return []
        names = ", ".join(_ms_lit(u) for u in users)
        return [int(r[0]) for r in self.eng._rows(
            self.side, db,
            "select s.session_id from sys.dm_exec_sessions s join"
            " sys.database_principals p on p.sid = s.security_id where"
            f" p.name in ({names}) and s.database_id = db_id() and"
            " s.session_id <> @@spid and s.is_user_process = 1"
            + (f" and s.login_time < cast({_ms_lit(before)} as datetime2)"
               if before else ""))]

    def end(self, db, users, wait=10):
        ids = self.sessions(db, users)
        for i in ids:
            try:
                self.run(db, f"kill {int(i)}")
            except Exception:  # noqa: BLE001 - it ended on its own
                pass
        stop = time.monotonic() + wait
        while ids and time.monotonic() < stop:
            if not self.sessions(db, users):
                break
            time.sleep(0.1)
        return len(ids)

    def hold(self, dbs, way, say):
        q = self.eng._q
        for d in dbs:
            name = self.name(d)
            record = state(self.hop, d, self.side) or {
                "db": name, "way": way, "roles": {}}
            record.setdefault("at", str(self.one(
                d, "select convert(nvarchar(30), sysdatetime(), 121)")))
            if way == "mssql_read_only":
                if "was" not in record:
                    record["was"] = int(self.one(
                        d, "select cast(is_read_only as int) from"
                           f" sys.databases where name = {_ms_lit(name)}")
                        or 0)
                _save(self.hop, d, record, self.side)
                if not record["was"]:
                    self.switch(d, f"alter database {q(name)} set"
                                   " read_only with rollback immediate")
                say(f"{name}: read-only for every account; what the other"
                    " sessions held open is rolled back and they are"
                    " ended")
                continue
            users = self.users(d)
            for u in users:
                if u not in record["roles"]:
                    record["roles"][u] = {"prior": {
                        p: s for p, s in self.eng._rows(
                            self.side, d,
                            "select permission_name, state from"
                            " sys.database_permissions where class = 0 and"
                            f" grantee_principal_id = user_id({_ms_lit(u)})"
                            " and permission_name in ('INSERT', 'UPDATE',"
                            " 'DELETE')")}}
                    _save(self.hop, d, record, self.side)
                self.run(d, f"deny insert, update, delete to {q(u)}")
            ended = self.end(d, users)
            say(f"{name}: {len(users)} users denied every write, {ended} of"
                " their sessions ended - what they had not committed is"
                " rolled back")

    def release(self, db, record, say):
        q = self.eng._q
        name = record.get("db") or self.name(db)
        if record.get("way") == "mssql_read_only":
            if not record.get("was"):
                self.switch(db, f"alter database {q(name)} set"
                                " read_write with rollback immediate")
            say(f"{name}: writable again")
            return
        for u, entry in (record.get("roles") or {}).items():
            prior = entry.get("prior") or {}
            stmts = []
            for p in ("INSERT", "UPDATE", "DELETE"):
                was = prior.get(p)
                if was == "D":
                    continue
                stmts.append(f"revoke {p.lower()} to {q(u)}" if not was else
                             f"grant {p.lower()} to {q(u)}"
                             + (" with grant option" if was == "W" else ""))
            self.run(db, *stmts)
        say(f"{name}: {len(record.get('roles') or {})} users given back"
            " their rights as they were")

    def owns(self, db, user):
        """Whether `user` owns the database (db_owner), or else a schema
        in it - what a deny does not reach."""
        member = int(self.one(
            db, "select count(*) from sys.database_role_members m join"
                " sys.database_principals r on r.principal_id ="
                " m.role_principal_id where r.name = 'db_owner' and"
                f" m.member_principal_id = user_id({_ms_lit(user)})") or 0)
        if member:
            return "owns the database (db_owner)"
        owns = [r[0] for r in self.eng._rows(
            self.side, db, "select name from sys.schemas where principal_id"
                           f" = user_id({_ms_lit(user)})")]
        return f"owns the schema {owns[0]}" if owns else ""

    def held(self, db, way):
        record = state(self.hop, db, self.side) or {"roles": {}}
        out = []
        if way == "mssql_read_only":
            on = self.one(db, "select cast(is_read_only as int) from"
                              " sys.databases where name ="
                              f" {_ms_lit(self.name(db))}")
            out.append((on == 1, "the database is read-only" if on == 1
                        else "the database is not read-only"))
        else:
            for u in record["roles"]:
                denied = int(self.one(
                    db, "select count(*) from sys.database_permissions"
                        " where class = 0 and state = 'D' and"
                        f" grantee_principal_id = user_id({_ms_lit(u)})"
                        " and permission_name in ('INSERT', 'UPDATE',"
                        " 'DELETE')") or 0)
                owner = self.owns(db, u)
                out.append((denied == 3 and not owner,
                            f"{u}: {denied} of 3 writes denied"
                            + (f", and it {owner}" if owner else "")))
            left = self.sessions(db, list(record["roles"]),
                                 before=record.get("at"))
            out.append((not left, f"{len(left)} sessions of the"
                                  " application's users left from before"
                                  " the freeze"))
        left = self.open(db, None)
        out.append((not left, "; ".join(left[:3]) if left else
                    "no prepared or open write transaction"))
        return out

    def open(self, db, older_than):
        out = []
        n = int(self.one(db, "select count(*) from"
                             " sys.dm_tran_database_transactions dt join"
                             " sys.dm_tran_active_transactions t on"
                             " t.transaction_id = dt.transaction_id where"
                             " dt.database_id = db_id() and"
                             " t.transaction_state = 5") or 0)
        if n:
            out.append(f"prepared transaction: {n} waiting for their"
                       " coordinator's word")
        age = "" if older_than is None else (
            " and dt.database_transaction_begin_time < dateadd(second,"
            f" -{int(older_than)}, getdate())")
        got = self.eng._rows(
            self.side, db,
            "select coalesce(s.login_name, '?'), st.session_id,"
            " datediff(second, dt.database_transaction_begin_time,"
            " getdate()) from sys.dm_tran_database_transactions dt join"
            " sys.dm_tran_session_transactions st on st.transaction_id ="
            " dt.transaction_id left join sys.dm_exec_sessions s on"
            " s.session_id = st.session_id where dt.database_id = db_id()"
            " and dt.database_transaction_log_record_count > 0 and"
            " st.session_id <> @@spid" + age)
        return out + [f"write transaction {u} session {i}, open {s}s"
                      for u, i, s in got]

    def writes(self, db):
        return int(self.one(db, "select coalesce(sum(leaf_insert_count +"
                                " leaf_update_count + leaf_delete_count), 0)"
                                " from sys.dm_db_index_operational_stats("
                                "db_id(), null, null, null)") or 0)

    def exempt(self, db, way):
        if way == "mssql_read_only":
            return []
        out = []
        for u in self.users(db):
            owner = self.owns(db, u)
            if owner:
                out.append(f"{u} {owner}: a deny does not reach an owner -"
                           " a per-database freeze does not reach that")
        return out


# Cassandra and ScyllaDB
# The application's roles lose MODIFY on the keyspace and on its tables
# where they hold it themselves, recorded first and granted back exactly.
# A connected session keeps what the server's permission cache holds for
# up to its validity (`permissions_validity`, read from the cluster), so
# the hold waits that out before the drain takes its position, and a
# cache longer than the budget is a way that does not hold. There is no
# session to end and no transaction to roll back: a write either landed
# before the grant went or is refused. A superuser, and a role that holds
# MODIFY on every keyspace or through another role, is not reached by a
# revoke on this keyspace: named, not touched.

def _cql_q(name):
    return '"' + str(name).replace('"', '""') + '"'


def _cql_seconds(text):
    """A duration as the server writes it, in seconds: `2000ms`, `2s`, or
    a bare number of milliseconds (`..._in_ms`, ScyllaDB's)."""
    t = str(text).strip().strip('"').lower()
    if t.endswith("ms"):
        return float(t[:-2]) / 1000
    if t.endswith("s"):
        return float(t[:-1])
    return float(t) / 1000


def _cql_resource(resource):
    """`<keyspace ks>` or `<table ks.t>` as a statement names it."""
    kind, _, name = resource.strip("<>").partition(" ")
    if kind == "table":
        ks, _, t = name.partition(".")
        return f"TABLE {_cql_q(ks)}.{_cql_q(t)}"
    return f"KEYSPACE {_cql_q(name)}"


class CassandraHold(Hold):
    #: what a cache that cannot be read is waited for: ScyllaDB's default,
    #: the larger of the two
    CACHE = 10.0

    def rungs(self):
        from .decide import Need, Rung
        return (
            Rung("cql_revoke", "the application's roles' right to write"
                 " the keyspace revoked, the permission cache waited out",
                 gives=frozenset({DATABASE, OURS}),
                 needs=(_is("authorizer", "roles checked for their"
                            " permissions", "the cluster checks no"
                            " permissions (no authorizer)"),
                        _is("authorize", "migkit's role may grant and"
                            " revoke", "migkit's role may not grant or"
                            " revoke on the keyspace (AUTHORIZE)"),
                        Need("a permission cache shorter than the budget",
                             lambda f: f["cache"] is not None
                             and f["budget"] is not None
                             and f["cache"] < f["budget"],
                             lambda f: (
                                 "the cluster's permission cache could not"
                                 " be read" if f["cache"] is None else
                                 "a revoke reaches a connected session only"
                                 " once the permission cache has let the"
                                 f" grant go, {f['cache']:g}s, past the"
                                 f" {f['budget'] or 0:g}s budget"))),
                 because="per role"),)

    def readers(self, dbs):
        return {"authorizer": _read(self.authorizer),
                "authorize": _read(lambda: all(self.may_authorize(d)
                                               for d in dbs)),
                "cache": _read(self.cache),
                "budget": lambda: _budget(self.hop)}

    def rows(self, q):
        return list(self.eng._session(self.side).execute(q))

    def ks(self, db):
        return self.eng._ks(self.side, db)

    def authorizer(self):
        for q in ("select value from system_views.settings where name ="
                  " 'authorizer'",
                  "select value from system.config where name ="
                  " 'authorizer'"):
            try:
                got = self.rows(q)
            except Exception:  # noqa: BLE001 - the other server's table
                continue
            if got and got[0][0]:
                return "allowall" not in str(got[0][0]).lower()
        return None

    def may_authorize(self, db):
        for r in self.rows("LIST ROLES"):
            if r.role == self.me and r.super:
                return True
        return bool(self.rows(f"LIST AUTHORIZE ON KEYSPACE"
                              f" {_cql_q(self.ks(db))} OF {_cql_q(self.me)}"))

    def cache(self):
        for q in ("select value from system_views.settings where name ="
                  " 'permissions_validity'",
                  "select value from system_views.settings where name ="
                  " 'permissions_validity_in_ms'",
                  "select value from system.config where name ="
                  " 'permissions_validity_in_ms'"):
            try:
                got = self.rows(q)
            except Exception:  # noqa: BLE001 - the other server's table
                continue
            if got and got[0][0] not in (None, ""):
                return _cql_seconds(got[0][0])
        return None

    def roles(self):
        got = [r.role for r in self.rows("LIST ROLES")
               if r.login and not r.super and r.role != self.me]
        named = _named(self.hop)
        return [r for r in named if r in got] if named else got

    def modify(self, role, ks, direct=True):
        """[(resource, the role holding it)] of MODIFY reaching `ks`."""
        q = f"LIST MODIFY OF {_cql_q(role)}" + (" NORECURSIVE" if direct
                                                 else "")
        return [(r.resource, r.role) for r in self.rows(q)
                if r.resource in (f"<keyspace {ks}>", "<all keyspaces>")
                or r.resource.startswith(f"<table {ks}.")]

    def hold(self, dbs, way, say):
        s = self.eng._session(self.side)
        roles = self.roles()
        for d in dbs:
            ks = self.ks(d)
            record = state(self.hop, d, self.side) or {"db": ks, "way": way,
                                                       "roles": {}}
            n = 0
            for r in roles:
                entry = record["roles"].setdefault(r, {"revoked": []})
                todo = [res for res, _ in self.modify(r, ks)
                        if res != "<all keyspaces>"
                        and res not in entry["revoked"]]
                entry["revoked"] += todo
                _save(self.hop, d, record, self.side)
                for res in todo:
                    s.execute(f"REVOKE MODIFY ON {_cql_resource(res)} FROM"
                              f" {_cql_q(r)}")
                n += len(todo)
            record["revoked_at"] = time.time()
            _save(self.hop, d, record, self.side)
            say(f"{ks}: {n} grants to write revoked from {len(roles)}"
                " roles")
        wait = self.cache()
        wait = self.CACHE if wait is None else wait
        # a connected session keeps the grant it cached until then
        time.sleep(wait + 0.2)
        say(f"the permission cache's {wait:g}s waited out")

    def release(self, db, record, say):
        s = self.eng._session(self.side)
        n = 0
        for r, entry in (record.get("roles") or {}).items():
            for res in entry.get("revoked") or []:
                s.execute(f"GRANT MODIFY ON {_cql_resource(res)} TO"
                          f" {_cql_q(r)}")
                n += 1
        say(f"{record.get('db') or db}: {n} grants to write given back")

    def held(self, db, way):
        record = state(self.hop, db, self.side) or {"roles": {}}
        ks = self.ks(db)
        out = []
        for r in record["roles"]:
            left = sorted({res for res, _ in self.modify(r, ks, False)})
            out.append((not left, f"{r}: " + (f"may still write {left[0]}"
                                              if left else "writes nothing"
                                              " here")))
        cache = self.cache()
        need = self.CACHE if cache is None else cache
        since = time.time() - float(record.get("revoked_at") or 0)
        out.append((since >= need, f"the revoke {since:.1f}s old, the"
                                   f" permission cache holds {need:g}s"))
        return out

    def exempt(self, db, way):
        out = [f"{r.role} is a superuser: nothing short of stopping it keeps"
               " it from writing" for r in self.rows("LIST ROLES")
               if r.super and r.login and r.role != self.me]
        ks = self.ks(db)
        for r in self.roles():
            wide = [(res, holder) for res, holder in self.modify(r, ks,
                                                                 False)
                    if res == "<all keyspaces>" or holder != r]
            if wide:
                res, holder = wide[0]
                out.append(f"{r} may write {res}"
                           + (f" through {holder}" if holder != r else "")
                           + ": a per-database freeze does not reach that")
        return out


# Kafka
# Every principal's writes to the hop's topics refused by a deny, which
# outranks every allow (`User:*`, WRITE, each topic by its name), the
# denies migkit made recorded first and deleted exactly. With every topic
# on the cluster the hop's, the transactional producers are kept from
# committing too (WRITE on every transactional id): a transaction the
# application opened before the hold would otherwise commit after the
# position the drain waits for. A broker's super users pass every
# access check: named, not touched. Read back as the cluster lists the
# denies, and by the topics' ends standing still.

def _kafka_deny(kind, name, filtered=False):
    from kafka.admin import (ACL, ACLFilter, ACLOperation,
                             ACLPermissionType, ACLResourcePatternType,
                             ResourcePattern, ResourcePatternFilter,
                             ResourceType)
    rtype = getattr(ResourceType, kind)
    if filtered:
        return ACLFilter(principal="User:*", host="*",
                         operation=ACLOperation.WRITE,
                         permission_type=ACLPermissionType.DENY,
                         resource_pattern=ResourcePatternFilter(
                             rtype, name, ACLResourcePatternType.LITERAL))
    return ACL(principal="User:*", host="*", operation=ACLOperation.WRITE,
               permission_type=ACLPermissionType.DENY,
               resource_pattern=ResourcePattern(
                   rtype, name, ACLResourcePatternType.LITERAL))


class KafkaHold(Hold):
    def rungs(self):
        from .decide import Rung
        return (
            Rung("kafka_cluster", "every principal's writes to the hop's"
                 " topics refused, and every transactional producer on the"
                 " cluster kept from committing", gives=frozenset({SERVER}),
                 needs=(_alone("topics"), _is(
                     "authorizer", "access checked on the cluster", "the"
                     " cluster checks no access (no authorizer)")),
                 because="every topic on the cluster is the hop's"),
            Rung("kafka_topics", "every principal's writes to the hop's"
                 " topics refused", gives=frozenset({DATABASE}),
                 needs=(_is("authorizer", "access checked on the cluster",
                            "the cluster checks no access (no"
                            " authorizer)"),),
                 because="per topic"))

    def readers(self, dbs):
        return {"authorizer": _read(self.authorizer)}

    def topics(self, dbs):
        """The hop's topics, as this side names them: the source's own
        names, which the copy keeps."""
        mine = sorted({t for d in dbs for _, t in
                       self.eng.list_move_tables(d)})
        if self.side == "src":
            return mine
        c = self.eng._consumer(self.side)
        try:
            there = set(c.topics())
        finally:
            c.close()
        return [t for t in mine if t in there]

    def others(self, dbs):
        mine = set(self.topics(dbs))
        c = self.eng._consumer(self.side)
        try:
            return sorted(t for t in c.topics()
                          if not t.startswith(self.eng.INTERNAL)
                          and t not in mine)
        finally:
            c.close()

    def authorizer(self):
        from kafka.admin import (ACLFilter, ACLOperation, ACLPermissionType,
                                 ACLResourcePatternType,
                                 ResourcePatternFilter, ResourceType)
        from kafka.errors import SecurityDisabledError
        admin = self.eng._admin(self.side)
        try:
            admin.describe_acls(ACLFilter(
                principal=None, host=None, operation=ACLOperation.ANY,
                permission_type=ACLPermissionType.ANY,
                resource_pattern=ResourcePatternFilter(
                    ResourceType.ANY, None, ACLResourcePatternType.ANY)))
            return True
        except SecurityDisabledError:
            return False
        finally:
            admin.close()

    @staticmethod
    def there(admin, kind, name):
        got, _ = admin.describe_acls(_kafka_deny(kind, name, filtered=True))
        return bool(got)

    def hold(self, dbs, way, say):
        admin = self.eng._admin(self.side)
        try:
            rec = server_state(self.hop, self.side) or {
                "engine": "kafka", "way": way, "made": []}
            want = [["TOPIC", t] for t in self.topics(dbs)]
            if way == "kafka_cluster":
                want.append(["TRANSACTIONAL_ID", "*"])
            todo = [x for x in want if x not in rec["made"]
                    and not self.there(admin, *x)]
            rec["made"] += todo
            # recorded before it is made: a release deletes exactly these
            _server_save(self.hop, rec, self.side)
            if todo:
                got = admin.create_acls([_kafka_deny(*x) for x in todo])
                failed = (got or {}).get("failed") or []
                if failed:
                    raise RuntimeError("the cluster refused a deny:"
                                       f" {failed[0]}")
            say("every principal refused writes on"
                f" {sum(1 for k, _ in rec['made'] if k == 'TOPIC')} topics"
                + ("; transactional producers kept from committing"
                   if way == "kafka_cluster" else ""))
        finally:
            admin.close()

    def release_server(self, rec, say):
        made = rec.get("made") or []
        if made:
            admin = self.eng._admin(self.side)
            try:
                admin.delete_acls([_kafka_deny(*x, filtered=True)
                                   for x in made])
            finally:
                admin.close()
        say(f"{len(made)} denies taken away")

    def writes(self, db):
        from kafka import TopicPartition
        c = self.eng._consumer(self.side)
        try:
            tps = [TopicPartition(t, p) for t in self.topics([db])
                   for p in self.eng._partitions(c, t)]
            return sum((c.end_offsets(tps) if tps else {}).values())
        finally:
            c.close()

    def held(self, db, way):
        rec = server_state(self.hop, self.side) or {}
        admin = self.eng._admin(self.side)
        try:
            missing = [n for k, n in rec.get("made") or []
                       if not self.there(admin, k, n)]
        finally:
            admin.close()
        return [(not missing, f"not denied: {', '.join(missing[:3])}"
                 if missing else f"{len(rec.get('made') or [])} denies on"
                                 " the cluster's record"),
                _held_still(self.hop, self.eng, db, self.side)]

    def exempt(self, db, way):
        from kafka.admin import ConfigResource, ConfigResourceType
        admin = self.eng._admin(self.side)
        try:
            first = list(admin._client.cluster.brokers())[0]
            node = str(first.node_id if hasattr(first, "node_id")
                       else first.nodeId)
            got = self.eng._described(admin.describe_configs(
                [ConfigResource(ConfigResourceType.BROKER, node)]))
        finally:
            admin.close()
        names = [p.strip() for p in str((got.get(node) or {}).get(
            "super.users") or "").split(";")
            if p.strip() and p.strip() not in ("None", "null")]
        return [f"{p} is one of the cluster's super users: nothing short of"
                " stopping it keeps it from writing" for p in names]


# OpenSearch
# Each of the hop's indices closed to writes by its own block
# (`_block/write`), which the cluster acknowledges only once the writes in
# flight on every shard have ended - so no write lands after the drain's
# position - what each index had set recorded first and set back. With
# every index on the cluster the hop's, the whole cluster read-only as
# well (`cluster.blocks.read_only`), which takes in an index made during
# the cutover; it is lifted first on the way back, since index settings
# cannot be changed under it. A block holds every account, an
# administrator's too.

class OpenSearchHold(Hold):
    def rungs(self):
        from .decide import Rung
        return (
            Rung("os_cluster", "the whole cluster read-only, once the hop's"
                 " indices' writes in flight have ended",
                 gives=frozenset({SERVER}), needs=(_alone("indices"),),
                 because="every index on the cluster is the hop's"),
            Rung("os_indices", "the hop's indices closed to writes, their"
                 " writes in flight ended first",
                 gives=frozenset({DATABASE}), because="per index"))

    def call(self, method, path, body=None):
        return self.eng._call(self.side, method, path, body)

    def indices(self, db):
        return [self.eng._index(self.side, db, t)
                for t in self.eng.neutral_tables(self.side, db)]

    def others(self, dbs):
        pre = tuple(self.eng._prefix(self.side, d) for d in dbs)
        got = self.call("GET", "/_cat/indices?format=json&h=index")
        return sorted(i["index"] for i in got or []
                      if not i["index"].startswith(".")
                      and not i["index"].startswith(pre))

    def setting(self, index):
        got = self.call("GET", f"/{index}/_settings/index.blocks.write"
                               "?flat_settings=true")
        return ((got or {}).get(index) or {}).get("settings", {}).get(
            "index.blocks.write")

    def cluster_block(self):
        got = self.call("GET", "/_cluster/settings?flat_settings=true")
        return {k: (got.get(k) or {}).get("cluster.blocks.read_only")
                for k in ("persistent", "transient")}

    def hold(self, dbs, way, say):
        n = 0
        for d in dbs:
            record = state(self.hop, d, self.side) or {
                "db": d, "way": way, "indices": {}}
            for index in self.indices(d):
                if index not in record["indices"]:
                    record["indices"][index] = self.setting(index)
                    _save(self.hop, d, record, self.side)
                got = self.call("PUT", f"/{index}/_block/write") or {}
                if not (got.get("acknowledged")
                        and got.get("shards_acknowledged")):
                    raise RuntimeError(f"{index}: the write block was not"
                                       " acknowledged by every shard")
                n += 1
        if way == "os_cluster":
            rec = server_state(self.hop, self.side) or {
                "engine": "opensearch", "way": way}
            if "before" not in rec:
                rec["before"] = self.cluster_block()
                _server_save(self.hop, rec, self.side)
            self.call("PUT", "/_cluster/settings",
                      {"persistent": {"cluster.blocks.read_only": True}})
            rec["on"] = True
            _server_save(self.hop, rec, self.side)
        say(f"{n} indices closed to writes"
            + ("; the whole cluster read-only" if way == "os_cluster"
               else ""))

    def release(self, db, record, say):
        for index, was in (record.get("indices") or {}).items():
            self.call("PUT", f"/{index}/_settings",
                      {"index.blocks.write": was})
        say(f"{record.get('db') or db}: {len(record.get('indices') or {})}"
            " indices open to writes as they were")

    def release_server(self, rec, say):
        if rec.get("on"):
            self.call("PUT", "/_cluster/settings", {"persistent": {
                "cluster.blocks.read_only": (rec.get("before") or {}).get(
                    "persistent")}})
        say("the cluster writable again")

    def writes(self, db):
        total = 0
        for index in self.indices(db):
            got = self.call("GET", f"/{index}/_stats/indexing") or {}
            done = (((got.get("_all") or {}).get("primaries") or {})
                    .get("indexing") or {})
            total += int(done.get("index_total") or 0) + int(
                done.get("delete_total") or 0)
        return total

    def held(self, db, way):
        record = state(self.hop, db, self.side) or {"indices": {}}
        writable = [i for i in record["indices"]
                    if str(self.setting(i)).lower() != "true"]
        out = [(not writable, f"open to writes: {', '.join(writable[:3])}"
                if writable else f"{len(record['indices'])} indices closed"
                                 " to writes")]
        if way == "os_cluster":
            now = self.cluster_block()
            on = str(now["transient"] if now["transient"] is not None
                     else now["persistent"]).lower() == "true"
            out.append((on, "the cluster is read-only" if on else
                        "the cluster is not read-only"))
        out.append(_held_still(self.hop, self.eng, db, self.side))
        return out


#: the hold each engine has (`Hold`); the engines not here are held by
#: their operator, and `decide.WAYS["writes held"]` says why
HOLDS = {"postgres": PostgresHold, "mysql": MySQLHold, "mongodb": MongoHold,
         "redis": RedisHold, "mssql": MSSQLHold, "cassandra": CassandraHold,
         "kafka": KafkaHold, "opensearch": OpenSearchHold,
         # services held as the API they speak is (P0-CLOUDS): GaussDB's
         # roles as PostgreSQL's, GeminiDB's APIs as their engines'; each
         # way's needs are read off the service before it is taken
         "gaussdb": PostgresHold, "geminidb-cassandra": CassandraHold,
         "geminidb-redis": RedisHold, "geminidb-mongo": MongoHold}
#: the sources the cutover freezes itself; any other is frozen by its
#: operator (`freeze: operator`) and proved by migkit
FREEZES = tuple(HOLDS)
#: the ways whose server switch is lifted before the databases' records
_SERVER_FIRST = ("os_cluster",)
