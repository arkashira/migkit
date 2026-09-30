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
    kind = getattr(e, "CANON_ENGINE", "") or _engine(hop)
    return ALIASES.get(kind, kind), e


def supported(hop):
    return _engine(hop) in ("postgres", "mysql")


#: the sources the cutover freezes itself; any other is frozen by its
#: operator (`freeze: operator`) and proved by migkit
FREEZES = ("postgres", "mysql", "mongodb")


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
    """Give back exactly what `freeze` took, and forget it."""
    record = state(hop, db, side)
    if not record:
        return None
    kind, eng = side_engine(hop, eng, side)
    if kind == "mysql":
        return _my_thaw(hop, eng, db, record, say, side)
    if kind == "mongodb":
        return _mongo_thaw(hop, eng, db, record, say, side)
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
    _state_path(hop, db, side).unlink()
    return record


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


def _my_end_sessions(hop, eng, db, say):
    """The application's sessions on a source the whole server of which is
    read-only, ended; its accounts recorded, with nothing revoked."""
    record = state(hop, db, "src") or {"db": db, "roles": {}}
    users = app_roles(hop, eng, db, "src")
    for user in users:
        record["roles"].setdefault(user, {"revoked": []})
    _save(hop, db, record, "src")
    ended = _my_kill(eng, "src", _my_sessions(eng, users, "src"))
    say(f"{db}: {ended} open sessions of the application ended - what they"
        " had not committed is rolled back")
    return record


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
    _state_path(hop, db, side).unlink()
    return record


# --- the source, at cutover -----------------------------------------------
# The freeze that starts the write outage. It has to stop every new write,
# end every write already in flight, and be read back: a freeze that stops
# new writes and lets an open transaction commit afterwards is how a
# "clean" cutover loses a row - the commit lands after the position the
# drain waited for, and the stream is torn down without it.
#
# Chosen from what the source allows, never asked:
# * the hop's databases are every database on the server, and migkit may
#   change the server's settings: the server-wide switch as well - on
#   PostgreSQL `default_transaction_read_only` through ALTER SYSTEM, on
#   MySQL `super_read_only`, which stops even an administrator
# * always, per database: the application's accounts as on the target
#   (grants revoked, recorded first), and on PostgreSQL the right to
#   connect taken from the roles that could still write after that (an
#   owner), and every open session of the application ended, so what it
#   had not committed is rolled back rather than committed late
#
# Everything is recorded before it is done, per database and for the
# server, and `thaw_source` gives back exactly that.

def _server_path(hop):
    return hop.report_dir() / "freeze-src-server.json"


def server_state(hop):
    try:
        return json.loads(_server_path(hop).read_text())
    except (OSError, ValueError):
        return None


def _server_save(hop, record):
    path = _server_path(hop)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(record, indent=1))
    tmp.replace(path)


def source_frozen(hop, dbs):
    """True where anything of a source freeze is still recorded."""
    return bool(server_state(hop)) or any(state(hop, d, "src") for d in dbs)


def scope(hop, eng, dbs):
    """("server", why) where the whole server may be switched to
    read-only - every database on it is the hop's, and migkit's account
    may change the setting - or ("database", why not)."""
    kind, eng = side_engine(hop, eng, "src")
    try:
        if kind == "mongodb":
            return _mongo_scope(hop, eng, dbs)
        if kind == "mysql":
            have = {r[0] for r in eng._q(
                "src", "select schema_name from information_schema.schemata")}
            others = sorted(have - set(dbs)
                            - {"mysql", "sys", "information_schema",
                               "performance_schema"})
            if others:
                return "database", (f"the server holds databases the hop"
                                    f" does not move ({', '.join(others[:3])})")
            grants = " ".join(str(r[0]).upper() for r in eng._q(
                "src", "show grants for current_user()"))
            if not any(p in grants for p in (
                    "ALL PRIVILEGES ON *.*", "SUPER",
                    "SYSTEM_VARIABLES_ADMIN")):
                return "database", ("migkit's account may not change the"
                                    " server's read_only")
            return "server", "every database on the server is the hop's"
        if kind == "postgres":
            first = dbs[0]
            others = [d for d in eng._psql(
                "src", first, "select datname from pg_database where"
                              " datallowconn and not datistemplate"
                              " order by 1").split() if d not in dbs]
            if others:
                return "database", (f"the server holds databases the hop"
                                    f" does not move ({', '.join(others[:3])})")
            may = eng._psql("src", first,
                            "select rolsuper from pg_roles"
                            " where rolname = current_user").strip()
            if may != "t":
                may = eng._psql(
                    "src", first,
                    "select coalesce(has_parameter_privilege("
                    "'default_transaction_read_only', 'ALTER SYSTEM'),"
                    " false)").strip() if _pg_version(eng, first) >= 150000 \
                    else "f"
            if may != "t":
                return "database", ("migkit's account may not change the"
                                    " server's settings (ALTER SYSTEM)")
            return "server", "every database on the server is the hop's"
    except Exception as e:  # noqa: BLE001 - asking failed: the narrower one
        return "database", f"the server could not be asked ({e})"
    return "database", "per database"


def _pg_version(eng, db, side="src"):
    try:
        return int(eng._psql(side, db, "show server_version_num").strip())
    except (RuntimeError, ValueError):
        return 0


def freeze_source(hop, eng, dbs, say, how):
    """Stop every write of the application to the source, `how` being
    "server" or "database" (`scope`). Returns when the application's
    sessions are gone; `proof` is what shows it held."""
    kind, eng = side_engine(hop, eng, "src")
    if kind == "mongodb":
        # the sessions first: the block waits for every transaction open
        # when it is asked, measured on 7.0 at 87.7s for one the
        # application held - its whole lifetime limit, all of it outage
        for d in dbs:
            _mongo_freeze(hop, eng, d, say, how)
        if how == "server":
            _mongo_server_freeze(hop, eng, say)
        return
    if how == "server":
        if kind == "mysql":
            _my_server_freeze(hop, eng, say)
        elif kind == "postgres":
            _pg_server_freeze(hop, eng, dbs, say)
    for d in dbs:
        if kind == "mysql" and how == "server":
            # nothing to revoke under super_read_only - and a revoke would
            # be refused by it: the sessions are ended, so what they hold
            # open is rolled back now rather than failing at its commit
            _my_end_sessions(hop, eng, d, say)
        elif kind == "mysql":
            _my_freeze(hop, eng, d, say, side="src")
        elif kind == "postgres":
            _pg_source_freeze(hop, eng, d, say)
        else:
            raise SystemExit(f"a {hop.engine} source is not frozen by"
                             " migkit")


def thaw_source(hop, eng, dbs, say):
    """Give the source back exactly what `freeze_source` took."""
    for d in dbs:
        thaw(hop, eng, d, say, side="src")
    rec = server_state(hop)
    if not rec:
        return
    kind, eng = side_engine(hop, eng, "src")
    if kind == "mysql":
        _my_server_thaw(hop, eng, rec, say)
    elif kind == "postgres":
        _pg_server_thaw(hop, eng, rec, say)
    elif kind == "mongodb":
        _mongo_server_thaw(hop, eng, rec, say)
    _server_path(hop).unlink(missing_ok=True)


# PostgreSQL

def _pg_source_freeze(hop, eng, db, say):
    """`freeze` on the source's side, then the right to connect taken from
    whatever role can still write, and every application session ended."""
    dbname = db
    # the server's clock, not this machine's: the proof asks the server
    # which sessions it opened before this
    at = eng._psql("src", db, "select clock_timestamp()::text").strip()
    record = freeze(hop, eng, db, say, side="src")
    record.setdefault("at", at)
    connect = record.setdefault("connect", {})
    roles = list(record["roles"])
    owners = [r for r in roles if _still_writes(eng, db, r, "src")]
    if owners:
        # a role that owns its tables writes whatever is revoked, and the
        # read-only default is one `begin read write` away: kept out of the
        # database instead. Where everyone may connect, that is PUBLIC's
        # right, so every other login role is given it by name first
        if _pg_public_connects(eng, db):
            keep = [r for r in eng._psql(
                "src", db, "select rolname from pg_roles where rolcanlogin"
                           " and not rolsuper order by 1").split()
                    if r not in roles]
            given = [r for r in keep if not _pg_connects_by_name(eng, db, r)]
            connect["granted"] = sorted(set(connect.get("granted", []))
                                        | set(given))
            connect["public"] = True
            _save(hop, db, record, "src")
            for r in given:
                eng._psql("src", db, f"grant connect on database"
                                     f" {_q(dbname)} to {_q(r)}")
            eng._psql("src", db, f"revoke connect on database"
                                 f" {_q(dbname)} from public")
        for r in owners:
            if _pg_connects_by_name(eng, db, r):
                record["roles"][r]["connect"] = True
                _save(hop, db, record, "src")
                eng._psql("src", db, f"revoke connect on"
                                     f" database {_q(dbname)} from {_q(r)}")
            say(f"{dbname}: {r} - kept from connecting: it can write as"
                " the owner whatever is revoked")
    _save(hop, db, record, "src")
    ended = _pg_end_sessions(eng, db, roles)
    say(f"{dbname}: {ended} open sessions of the application ended - what"
        " they had not committed is rolled back, not committed after the"
        " freeze")
    return record


def _pg_public_connects(eng, db):
    return eng._psql(
        "src", db,
        "select coalesce(bool_or(a.grantee = 0 and a.privilege_type ="
        " 'CONNECT'), false) or bool_or(d.datacl is null) from pg_database d"
        " left join lateral aclexplode(d.datacl) a on true"
        " where d.datname = current_database()").strip() == "t"


def _pg_connects_by_name(eng, db, role):
    return eng._psql(
        "src", db,
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


def _pg_end_sessions(eng, db, roles, wait=10):
    """End every session of these roles in this database and wait until
    the server has let them go: `pg_terminate_backend` only signals."""
    if not roles:
        return 0
    names = ", ".join(_lit(r) for r in roles)
    who = (f" from pg_stat_activity where usename in ({names})"
           " and datname = current_database() and pid <> pg_backend_pid()")
    ended = int(eng._psql("src", db, "select count(pg_terminate_backend(pid))"
                          + who).strip() or 0)
    end = time.monotonic() + wait
    while time.monotonic() < end:
        if eng._psql("src", db, "select count(*)" + who).strip() == "0":
            break
        time.sleep(0.1)
    return ended


def _pg_server_freeze(hop, eng, dbs, say):
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
    rec = server_state(hop) or {}
    if "before" not in rec:
        rec["before"] = eng._psql(
            "src", first,
            "select setting from pg_file_settings where name ="
            " 'default_transaction_read_only' and sourcefile like"
            " '%postgresql.auto.conf' order by seqno desc limit 1").strip()
        rec["ours"] = {}
        rec["engine"] = "postgres"
        _server_save(hop, rec)
    me = hop.source.user
    for d in dbs:
        if d not in rec["ours"]:
            rec["ours"][d] = _setting(eng, d, me, "src")
            _server_save(hop, rec)
        eng._psql("src", d, f"alter role {_q(me)} in database {_q(d)} set"
                            " default_transaction_read_only = off")
    eng._psql("src", first, "alter system set default_transaction_read_only"
                            " = on")
    eng._psql("src", first, "select pg_reload_conf()")
    rec["on"] = True
    _server_save(hop, rec)
    say("source: every new transaction on the server is read-only"
        " (default_transaction_read_only), migkit's own role excepted")


def _pg_server_thaw(hop, eng, rec, say):
    dbs = list(rec.get("ours") or {})
    first = dbs[0] if dbs else "postgres"
    before = rec.get("before")
    eng._psql("src", first, "alter system "
              + (f"set default_transaction_read_only = {_lit(before)}"
                 if before else "reset default_transaction_read_only"))
    eng._psql("src", first, "select pg_reload_conf()")
    me = hop.source.user
    for d, was in (rec.get("ours") or {}).items():
        eng._psql("src", d, f"alter role {_q(me)} in database {_q(d)} "
                  + (f"set default_transaction_read_only = {was}"
                     if was is not None
                     else "reset default_transaction_read_only"))
    say("source: the server's read-only default taken off again")


# MySQL

def _my_server_freeze(hop, eng, say):
    """`super_read_only`: no account writes, an administrator neither -
    only the server's own replication applies. Persisted, so a restart in
    the rollback window cannot make the old source writable; what was set
    and persisted before is recorded first."""
    rec = server_state(hop) or {}
    brand = eng._brands()[0].name
    if "before" not in rec:
        if brand == "mariadb":
            got = eng._q("src", "select @@global.read_only")
            rec["before"] = {"read_only": int(got[0][0])}
        else:
            got = eng._q("src", "select @@global.read_only,"
                                " @@global.super_read_only")
            rec["before"] = {"read_only": int(got[0][0]),
                             "super_read_only": int(got[0][1])}
            rec["persisted"] = sorted(r[0] for r in eng._q(
                "src", "select variable_name from"
                       " performance_schema.persisted_variables where"
                       " variable_name in ('read_only', 'super_read_only')"))
        rec["engine"] = "mysql"
        rec["brand"] = brand
        _server_save(hop, rec)
    if brand == "mariadb":
        eng._q("src", "set global read_only = 1")
    else:
        eng._q("src", "set persist super_read_only = on")
    rec["on"] = True
    _server_save(hop, rec)
    say("source: the whole server is read-only ("
        + ("read_only" if brand == "mariadb" else "super_read_only")
        + "), its own replication excepted")


def _my_server_thaw(hop, eng, rec, say):
    before = rec.get("before") or {}
    if rec.get("brand") == "mariadb":
        eng._q("src", f"set global read_only = {int(before.get('read_only', 0))}")
    else:
        persisted = set(rec.get("persisted") or [])
        eng._q("src", "set global super_read_only ="
                      f" {int(before.get('super_read_only', 0))}")
        eng._q("src", f"set global read_only = {int(before.get('read_only', 0))}")
        for name in ("super_read_only", "read_only"):
            if name in persisted:
                eng._q("src", f"set persist {name} = {int(before.get(name, 0))}")
            else:
                try:
                    eng._q("src", f"reset persist if exists {name}")
                except Exception:  # noqa: BLE001 - nothing persisted
                    pass
    say("source: writable again, as it was before the freeze")


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


def _mongo_scope(hop, eng, dbs):
    c = eng._client("src")
    others = sorted(set(c.list_database_names()) - set(MONGO_SYSTEM)
                    - set(dbs))
    if others:
        return "database", (f"the server holds databases the hop does not"
                            f" move ({', '.join(others[:3])})")
    status = c.admin.command({"connectionStatus": 1, "showPrivileges": True})
    actions = {a for p in (status.get("authInfo") or {}).get(
        "authenticatedUserPrivileges", [])
        if (p.get("resource") or {}).get("cluster")
        for a in p.get("actions", [])}
    version = tuple(c.admin.command("buildInfo").get("versionArray",
                                                     [0, 0])[:2])
    if version >= (6, 0) and "setUserWriteBlockMode" in actions:
        return "server", "every database on the server is the hop's"
    if {"fsync", "unlock"} <= actions:
        return "server", ("every database on the server is the hop's; before"
                          " 6.0 it has no write block, so it is locked"
                          " (fsyncLock)")
    return "database", "migkit's account may not block the server's writes"


def _mongo_users(hop, eng):
    """[(user, its database, roles, whether it may write through a
    blocked server)] of every account on the source, but migkit's and the
    server's own. The privileges are asked one account at a time: the
    server gives them only to a query naming the account (measured on 7.0,
    `usersInfo: {forAllDBs: true}` with `showPrivileges` is refused)."""
    c = eng._client("src")
    got = c.admin.command({"usersInfo": {"forAllDBs": True}})
    out = []
    for u in got.get("users", []):
        if u["user"] == hop.source.user or u["db"] in ("local", "config") \
                or u["user"].startswith("__"):
            continue
        one = c[u["db"]].command({"usersInfo": u["user"],
                                  "showPrivileges": True})["users"][0]
        out.append((u["user"], u["db"], u.get("roles", []),
                    any("bypassWriteBlockingMode" in p.get("actions", [])
                        for p in one.get("inheritedPrivileges", []))))
    return out


def _mongo_end_sessions(eng, users):
    """The sessions these accounts hold, ended - which aborts a
    transaction open in one - and the operations they run without one.
    Found through `$currentOp` and ended by id: the pattern that names
    accounts (`killAllSessionsByPattern` with `users`) needs a privilege
    even `root` does not hold (measured on 7.0: `Not authorized to
    impersonate`)."""
    if not users:
        return
    names = {(u, d) for u, d in users}
    c = eng._client("src")
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


def _mongo_server_freeze(hop, eng, say):
    c = eng._client("src")
    rec = server_state(hop) or {}
    if "method" not in rec:
        version = tuple(c.admin.command("buildInfo").get(
            "versionArray", [0, 0])[:2])
        rec.update({"engine": "mongodb",
                    "method": "block" if version >= (6, 0) else "fsync"})
        _server_save(hop, rec)
    if rec["method"] == "block":
        c.admin.command({"setUserWriteBlockMode": 1, "global": True})
    elif not rec.get("on"):
        c.admin.command("fsync", lock=True)
    rec["on"] = True
    _server_save(hop, rec)
    say("source: every account's writes blocked on the server"
        + (" (setUserWriteBlockMode)" if rec["method"] == "block"
           else " (fsyncLock)"))


def _mongo_server_thaw(hop, eng, rec, say):
    c = eng._client("src")
    if rec.get("method") == "block":
        c.admin.command({"setUserWriteBlockMode": 1, "global": False})
    elif rec.get("on"):
        c.admin.command("fsyncUnlock")
    say("source: writes given back to the server")


def _mongo_freeze(hop, eng, db, say, how):
    """The application's accounts on this database: per database, the
    roles that write here swapped for `read` on it; either way, their
    sessions ended."""
    record = state(hop, db, "src") or {"db": db, "roles": {}}
    users = [(u, d, roles) for u, d, roles, bypass in _mongo_users(hop, eng)
             if not bypass]
    ended = []
    for user, udb, roles in users:
        key = f"{udb}.{user}"
        todo = [r for r in roles if r.get("db") == db
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
        reads = any(r.get("db") == db and r.get("role") == "read"
                    for r in roles)
        entry["revoked"] += todo
        entry["read"] = entry["read"] or (bool(todo) and not reads)
        # recorded before it is done, as everywhere here
        _save(hop, db, record, "src")
        c = eng._client("src")[udb]
        if todo:
            c.command("revokeRolesFromUser", user, roles=todo)
            if entry["read"]:
                c.command("grantRolesToUser", user,
                          roles=[{"role": "read", "db": db}])
    _save(hop, db, record, "src")
    _mongo_end_sessions(eng, ended)
    say(f"{db}: the sessions of {len(ended)} accounts ended"
        + ("" if how == "server" else ", the roles that write here"
           " swapped for read"))
    return record


def _mongo_thaw(hop, eng, db, record, say, side):
    for entry in record["roles"].values():
        c = eng._client(side)[entry["db"]]
        if entry["revoked"]:
            c.command("grantRolesToUser", entry["user"],
                      roles=entry["revoked"])
        if entry.get("read"):
            c.command("revokeRolesFromUser", entry["user"],
                      roles=[{"role": "read", "db": db}])
    say(f"{db}: the accounts' roles given back")
    _state_path(hop, db, side).unlink()
    return record


def _mongo_blocked(eng):
    """Whether the server blocks users' writes now, as it keeps it."""
    c = eng._client("src")
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


# --- what shows the freeze held --------------------------------------------

def proof(hop, eng, db, how):
    """[(held, what)] read back from the source after `freeze_source`:
    nothing held means the freeze is not proven and the cutover goes
    back."""
    kind, eng = side_engine(hop, eng, "src")
    if kind == "postgres":
        return _pg_proof(hop, eng, db, how)
    if kind == "mysql":
        return _my_proof(hop, eng, db, how)
    if kind == "mongodb":
        return _mongo_proof(hop, eng, db, how)
    return [(False, f"{hop.engine}: no freeze of migkit's to read back")]


def _mongo_proof(hop, eng, db, how):
    out = []
    rec = server_state(hop) or {}
    if how == "server" and rec.get("method") == "block":
        on = _mongo_blocked(eng)
        out.append((on, "users' writes are blocked on the server" if on
                    else "users' writes are not blocked on the server"))
    elif how == "server":
        on = bool(eng._client("src").admin.command("currentOp").get(
            "fsyncLock"))
        out.append((on, "the server is locked" if on
                    else "the server is not locked"))
    held = state(hop, db, "src") or {"roles": {}}
    writers = [f"{u}@{d}" for u, d, roles, bypass in _mongo_users(hop, eng)
               if not bypass and how != "server"
               and any(r.get("db") == db and r.get("role")
                       in _MONGO_WRITE_ROLES for r in roles)]
    out.append((not writers, f"still writing here: {', '.join(writers)}"
                if writers else f"{len(held['roles'])} accounts hold no"
                " write here"))
    left = _mongo_open(eng, db, None)
    out.append((not left, "; ".join(left[:3]) if left
                else "no open transaction"))
    return out


def _pg_proof(hop, eng, db, how):
    out = []
    rec = state(hop, db, "src") or {"roles": {}}
    roles = list(rec["roles"])
    names = ", ".join(_lit(r) for r in roles) or "''"
    # opened before the freeze: one opened since is refused a write, or a
    # connection, by what the freeze took
    left = eng._psql("src", db, "select count(*) from pg_stat_activity"
                                f" where usename in ({names}) and datname ="
                                " current_database() and backend_start <"
                                f" {_lit(rec.get('at') or 'now')}::timestamptz"
                     ).strip()
    out.append((left == "0", f"{left} sessions of the application's roles"
                              " left from before the freeze"))
    for r in roles:
        can = eng._psql("src", db, f"select has_database_privilege({_lit(r)},"
                                   " current_database(), 'CONNECT')").strip()
        writes = _still_writes(eng, db, r, "src")
        out.append((can != "t" or not writes,
                    f"{r}: " + ("cannot connect" if can != "t" else
                                "can connect and write to "
                                f"{len(writes)} tables" if writes else
                                "can connect, writes to nothing")))
    writing = eng._psql(
        "src", db, "select coalesce(string_agg(usename||' pid '||pid, ', '),"
                   " '') from pg_stat_activity where datname ="
                   " current_database() and backend_type = 'client backend'"
                   " and backend_xid is not null"
                   " and pid <> pg_backend_pid()").strip()
    out.append((not writing, f"writing now: {writing}" if writing
                else "no session holds a write transaction"))
    prepared = eng._psql("src", db, "select count(*) from pg_prepared_xacts"
                                    " where database = current_database()"
                         ).strip()
    out.append((prepared == "0", f"{prepared} prepared transactions"))
    if how == "server":
        got = eng._psql("src", db,
                        "select setting from pg_file_settings where name ="
                        " 'default_transaction_read_only' and applied"
                        " order by seqno desc limit 1").strip()
        out.append((got == "on", "the server's default is read-only"
                    if got == "on" else "the server's default is not"
                    " read-only"))
    return out


def _my_proof(hop, eng, db, how):
    out = []
    rec = state(hop, db, "src") or {"roles": {}}
    users = list(rec["roles"])
    if how == "server":
        brand = eng._brands()[0].name
        var = "read_only" if brand == "mariadb" else "super_read_only"
        got = eng._q("src", f"select @@global.{var}")
        on = bool(got and int(got[0][0]))
        out.append((on, f"{var} is {'on' if on else 'off'}"))
    for u in users:
        left = _my_grants(eng, db, u, "src")
        wider = _my_wider(eng, u, "src")
        out.append((how == "server" or not (left or wider),
                    f"{u}: " + ("no write grant on the database" if not left
                                else f"{len(left)} write grants left")
                    + (f", {' and '.join(wider)}" if wider else "")))
    xa = open_work(hop, eng, db, "src", None)
    out.append((not xa, "; ".join(xa) if xa else
                "no prepared XA or open write transaction"))
    return out


def open_work(hop, eng, db, side, older_than):
    """What is left open on a side that a freeze cannot end cleanly:
    prepared transactions (they survive a killed session, a restart and
    every timeout, and a PostgreSQL slot cannot be made while one is
    open), and write transactions older than `older_than` seconds (None:
    any). Each as a line; [] where there is none."""
    kind, eng = side_engine(hop, eng, side)
    out = []
    if kind == "mongodb":
        return _mongo_open(eng, db, older_than, side)
    if kind == "postgres":
        got = eng._psql(side, db, "select gid||' ('||owner||', prepared '||"
                                  "to_char(prepared, 'HH24:MI:SS')||')' from"
                                  " pg_prepared_xacts where database ="
                                  " current_database()")
        out += [f"prepared transaction {g}" for g in got.splitlines() if g]
        age = "" if older_than is None else \
            f" and xact_start < now() - interval '{float(older_than)} seconds'"
        got = eng._psql(side, db,
                        "select usename||' pid '||pid||', open '||"
                        "round(extract(epoch from now() - xact_start))||'s'"
                        " from pg_stat_activity where datname ="
                        " current_database() and backend_xid is not null"
                        " and backend_type = 'client backend'"
                        " and pid <> pg_backend_pid()" + age)
        out += [f"write transaction {g}" for g in got.splitlines() if g]
    elif kind == "mysql":
        try:
            got = eng._q(side, "xa recover")
            out += [f"prepared XA transaction {r[3]!r}" for r in got]
        except Exception as e:  # noqa: BLE001 - no right to ask
            out.append(f"XA RECOVER could not be read ({e})")
        age = "" if older_than is None else \
            f" and t.trx_started < now() - interval {float(older_than)} second"
        me = (hop.source if side == "src" else hop.target).user
        got = eng._q(side,
                     "select p.user, t.trx_mysql_thread_id,"
                     " timestampdiff(second, t.trx_started, now())"
                     " from information_schema.innodb_trx t join"
                     " information_schema.processlist p"
                     " on p.id = t.trx_mysql_thread_id"
                     " where t.trx_rows_modified > 0 and p.user <> %s"
                     " and p.user not in ('system user', 'event_scheduler')"
                     + age, (me,))
        out += [f"write transaction {u} thread {i}, open {s}s"
                for u, i, s in got]
    return out


def cannot(hop, eng, db):
    """What migkit's account on the source lacks to freeze it per
    database and give it back, as lines; [] where it lacks nothing. Asked
    before anything is written: a freeze that revokes half and cannot end
    a session holds the application out and proves nothing."""
    kind, eng = side_engine(hop, eng, "src")
    out = []
    try:
        if kind == "postgres":
            sup, owner, signal = eng._psql(
                "src", db, "select r.rolsuper::text||' '||(d.datdba ="
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
                "src", db, "select count(*) from pg_class c join"
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
        elif kind == "mysql":
            grants = " ".join(str(r[0]).upper() for r in eng._q(
                "src", "show grants for current_user()"))
            wide = "ALL PRIVILEGES ON *.*" in grants
            if not (wide or "SUPER" in grants
                    or "CONNECTION_ADMIN" in grants):
                out.append("migkit's account on the source cannot end other"
                           " accounts' sessions (CONNECTION_ADMIN)")
            if "WITH GRANT OPTION" not in grants:
                out.append("migkit's account on the source cannot revoke or"
                           " give back the application's grants (GRANT"
                           " OPTION)")
    except Exception as e:  # noqa: BLE001 - not readable: said as that
        out.append(f"what migkit's account may do on the source could not"
                   f" be read ({str(e).strip()[:120]})")
    return out


def writes_seen(hop, eng, db, side="src"):
    """The server's running count of rows written in this database, or
    None where it keeps none that can be read: compared across a few
    seconds, it moving is something still writing. PostgreSQL counts in
    `pg_stat_database`, each session reporting at least once a second
    while it works; MySQL per table in `performance_schema`."""
    kind, eng = side_engine(hop, eng, side)
    try:
        if kind == "mongodb":
            got = eng._client(side).admin.command("top").get("totals", {})
            return sum(int((v.get(op) or {}).get("count", 0))
                       for ns, v in got.items()
                       if isinstance(v, dict) and ns.startswith(f"{db}.")
                       for op in ("insert", "update", "remove"))
        if kind == "postgres":
            got = eng._psql(side, db, "select tup_inserted + tup_updated +"
                                      " tup_deleted from pg_stat_database"
                                      " where datname = current_database()")
            return int(got.strip())
        if kind == "mysql":
            got = eng._q(side, "select coalesce(sum(count_write), 0) from"
                               " performance_schema"
                               ".table_io_waits_summary_by_table"
                               " where object_schema = %s",
                         (eng._d(side, db),))
            return int(got[0][0])
    except Exception:  # noqa: BLE001 - not readable: no count, not "quiet"
        return None
    return None


def bypass(hop, eng, db):
    """Who can still write through the freeze, as lines for the preflight:
    superusers on PostgreSQL, and on MySQL, for a per-database freeze, the
    accounts whose write privileges are server-wide or come through a
    role."""
    kind, eng = side_engine(hop, eng, "src")
    out = []
    if kind == "mongodb":
        for u, d, roles, bypass in _mongo_users(hop, eng):
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
    if kind == "postgres":
        got = eng._psql("src", db, "select rolname from pg_roles where"
                                   " rolsuper and rolcanlogin order by 1")
        out += [f"{r} is a superuser: nothing short of stopping it keeps it"
                " from writing" for r in got.split()
                if r != hop.source.user]
        got = eng._psql("src", db,
                        "select coalesce(r.rolname, 'every role') from"
                        " pg_db_role_setting s left join pg_roles r on"
                        " r.oid = s.setrole, unnest(s.setconfig) c(cfg)"
                        " where c.cfg = 'default_transaction_read_only=off'"
                        " and s.setdatabase in (0, (select oid from"
                        " pg_database where datname = current_database()))")
        out += [f"{r} is set writable by default here, which a server-wide"
                " read-only default does not reach" for r in got.split()
                if r != hop.source.user]
    elif kind == "mysql":
        for u in app_roles(hop, eng, db, "src"):
            wider = _my_wider(eng, u, "src")
            if wider:
                out.append(f"{u} {' and '.join(wider)}: a per-database"
                           " freeze does not reach that")
    return out
