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

Every other engine's target is kept by the same primitive the cutover's
freeze of a source stands on (`Hold`), the way climbed from what the
target allows among the ways that leave migkit's own account writing, per
database (`protect`, the section "the target kept from the application"
below): the application's rights taken where the engine grants rights,
a change of its access policy offered where the engine's writes are
allowed outside it, and, where nothing can hold a writer and leave
migkit's, the writes that are not migkit's named before the cutover.

The source is frozen only by the cutover of a hop that turns `cutover:` on
(`migkit.cutover`), with the same record-first rule, and more: there the
freeze is the start of the write outage and has to be complete, not a
guard. Its rules are at the end of this file.
"""
import dataclasses
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
    application's accounts, carried across - or, where the source is
    another engine, whose accounts are not the target's, every login role
    of the target's but migkit's and the system's. On the source, every
    login role that is not migkit's, the system's or a superuser's - an
    account the application writes with need not have been carried yet,
    and it writes all the same. A superuser is not in the list because
    nothing a freeze can take stops one; `bypass` names them instead."""
    named = (hop.options or {}).get("app_roles")
    # migkit's own accounts, its replica's among them: ending that one's
    # session at the source's freeze cut the stream the drain waits on
    ours = {hop.target.user, hop.source.user,
            getattr(eng, "REPL_USER", None)}
    if named:
        return [r for r in named if r not in ours]
    kind, mine = side_engine(hop, eng, side)
    src_kind, src = side_engine(hop, eng, "src")
    both = side == "dst" and src_kind == kind
    if kind in _MYSQL_KINDS:
        from .users import _mysql_sysuser
        q = "select distinct user from mysql.user order by 1"
        if side == "src":
            return [r[0] for r in mine._q("src", q)
                    if r[0] not in ours and r[0]
                    and not _mysql_sysuser(r[0])]
        there = {r[0] for r in src._q("src", q)} if both else None
        return [r[0] for r in mine._q("dst", q)
                if (there is None or r[0] in there) and r[0]
                and r[0] not in ours and not _mysql_sysuser(r[0])]
    from .users import _pg_sysrole
    if side == "src":
        got = mine._psql("src", db, "select rolname from pg_roles where"
                                    " rolcanlogin and not rolsuper"
                                    " order by 1")
        return [r for r in got.split() if r not in ours
                and not _pg_sysrole(r)]
    q = "select rolname from pg_roles where rolcanlogin order by 1"
    there = set(src._psql("src", db, q).split()) if both else None
    dst = mine._psql("dst", db, q).split()
    return [r for r in dst if (there is None or r in there)
            and r not in ours and not _pg_sysrole(r)]


#: the engines whose accounts are MySQL's: TiDB keeps MySQL's grant tables
_MYSQL_KINDS = ("mysql", "tidb")


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


def freeze(hop, eng, db, say, side="dst"):
    """Stop the application's roles writing to this database: on the
    target, the hop's `protect_target`, the way its engine allows
    (`protect`); on a source, the per-role part of the cutover's freeze.

    Idempotent: a role frozen already is checked again, and anything it
    could newly write to - a table the move just created, a grant made
    since - is frozen too, the record growing rather than being replaced.
    """
    if side == "dst":
        return protect(hop, eng, db, say)
    kind, eng = side_engine(hop, eng, side)
    if kind in _MYSQL_KINDS:
        return _my_freeze(hop, eng, db, say, side)
    return _pg_freeze(hop, eng, db, say, side)


def _pg_freeze(hop, eng, db, say, side="dst"):
    """The application's roles on one PostgreSQL database: their direct
    write grants revoked, and a role that can still write after that made
    read-only by default here, its sessions ended."""
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
    (`Hold.hold`) - took, from its record, and forget it. A target held
    by a change to the whole server's accounts or access lists (`Hold.keep`
    recorded it for the server) has that given back with the first of its
    databases."""
    record = state(hop, db, side)
    if not record:
        return None
    h = hold_for(hop, eng, side)
    h.release(db, record, say)
    _state_path(hop, db, side).unlink(missing_ok=True)
    if side == "dst" and record.get("server"):
        rec = server_state(hop, side)
        if rec:
            h.release_server(rec, say)
            _server_path(hop, side).unlink(missing_ok=True)
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
        if entry.get("limit") is not None:
            eng._psql(side, db, f"alter role {_q(role)} connection limit"
                                f" {int(entry['limit'])}")
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


#: the procedure a managed MySQL gives its admin to end another account's
#: session where it holds no CONNECTION_ADMIN (RDS and Aurora); the
#: managed-service catalogue adds the others it names (`_kill_procedures`)
KILL_PROCEDURES = ("mysql.rds_kill",)


def _kill_procedures():
    from . import managed
    try:
        more = [managed._procedure(s.admin.get("kill"))
                for s in managed.services().values()
                if s.family == "mysql"]
    except Exception:  # noqa: BLE001 - the catalogue unread: ours stand
        more = []
    return tuple(dict.fromkeys(list(KILL_PROCEDURES)
                               + [m for m in more if m]))


def _my_kill_way(eng, side="src"):
    """How migkit's account ends another account's session here: `kill`
    where it holds CONNECTION_ADMIN or SUPER, else a procedure of the
    service's that the server has and lists to it (`KILL_PROCEDURES`),
    else None - read from the server, once a run."""
    kept = eng.__dict__.setdefault("_migkit_kill_way", {})
    if side in kept:
        return kept[side]
    grants = " ".join(str(r[0]).upper() for r in eng._q(
        side, "show grants for current_user()"))
    got = None
    if any(p in grants for p in ("ALL PRIVILEGES ON *.*", "SUPER",
                                 "CONNECTION_ADMIN")):
        got = "kill"
    else:
        for proc in _kill_procedures():
            schema, _, name = proc.rpartition(".")
            n = eng._q(side, "select count(*) from information_schema"
                             ".routines where routine_schema = %s and"
                             " routine_name = %s", (schema or "mysql", name))
            if n and int(n[0][0]):
                got = proc
                break
    if got is None:
        # a service whose admin ends another's session with KILL through a
        # privilege of its own (the catalogue says which): taken at its
        # word, and `_my_kill` refuses to go on past a session that stays
        from . import managed
        try:
            svc = managed.side(eng.hop, eng, "source" if side == "src"
                               else "target").service
            if str(svc.admin.get("kill") or "").lower().startswith("kill"):
                got = "kill"
        except Exception:  # noqa: BLE001 - no service to take the word of
            pass
    kept[side] = got
    return got


def _my_kill(eng, side, sessions, wait=10):
    """End these sessions and wait until the server has let them go:
    `KILL` only marks a session, and one still rolling back holds its
    transaction open where a proof looks for it. Where migkit's account
    may not kill another's session, the service's own procedure does
    (`_my_kill_way`); TiDB's own word for it (`KILL TIDB`) ends a session
    on whichever of its servers holds it."""
    ended = 0
    how = _my_kill_way(eng, side) if sessions else None
    verb = "kill tidb" if getattr(eng, "CANON_ENGINE", "") == "tidb" \
        else "kill"
    for pid, _ in sessions:
        try:
            eng._q(side, f"call {how}({int(pid)})"
                   if how and how != "kill" else f"{verb} {int(pid)}")
            ended += 1
        except Exception:  # noqa: BLE001 - it ended on its own
            pass
    ids = [int(pid) for pid, _ in sessions]
    if not ids:
        return ended
    marks = ", ".join(["%s"] * len(ids))
    end = time.monotonic() + wait
    left = 0
    while time.monotonic() < end:
        got = eng._q(side, "select count(*) from information_schema"
                           f".processlist where id in ({marks})", tuple(ids))
        left = int(got[0][0]) if got else 0
        if not left:
            break
        time.sleep(0.1)
    if left and ended < len(ids):
        # a session the server would not end goes on writing past a
        # revoke that reaches it only when it next chooses the database:
        # nothing after this could prove the source held
        raise SystemExit(f"{left} of the application's sessions could not"
                         " be ended - migkit's account may not end them")
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
#: what every way of keeping a target gives: the hop's databases alone -
#: the server's settings are not migkit's to change on a target either -
#: and migkit's own account writing on, since it moves into it
KEEP = (DATABASE, OURS)

#: the scope each way is, as the hop's `freeze:` and the records name it
SCOPE = {"pg_server": "server", "pg_database": "database",
         "pg_role_limit": "database",
         "my_server": "server", "my_param_read_only": "server",
         "my_database": "database",
         "mongo_block": "server", "mongo_fsync": "server",
         "mongo_database": "database",
         "redis_pause": "server", "redis_accounts": "database",
         "mssql_read_only": "database", "mssql_deny": "database",
         "cql_revoke": "database",
         "kafka_cluster": "server", "kafka_topics": "database",
         "os_cluster": "server", "os_indices": "database",
         # the ways a target is kept by (`Hold.keep_rungs`)
         "kafka_super": "database", "kafka_principals": "database",
         "os_named": "database", "pgw_grants": "database",
         "tidb_server": "server",
         "ch_grants": "database", "ora_grants": "database",
         "db2_grants": "database", "ase_grants": "database",
         "td_grants": "database", "rs_grants": "database",
         "sf_grants": "database", "cb_roles": "database",
         "ddb_policy": "database", "bq_access": "database"}


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
    cannot(db, way)     what migkit's account lacks for it, as lines

    and, keeping a target from the application (`protect`):

    keep_rungs()        the ways a target is kept by: the hop's databases
                        alone, migkit's own account writing on (`KEEP`)
    keep(db, way, say)  kept, each change recorded before it is made
    kept(db, record)    [(held, what)] read back: nothing of the
                        application's writes here, or the writes that are
                        not migkit's, named
    offer(db)           the change to the target's access policy a way
                        needs an answer to, as lines; [] where none
    """

    #: how it keeps a target from the application (`protect`): "held" -
    #: its ways leave migkit's own account writing; "offered" - by a change
    #: to the target's access policy, made only once the operator answers
    #: yes; "named" - nothing holds a writer and leaves migkit's, so the
    #: writes that are not migkit's are named before the cutover; "" -
    #: neither, and `decide.WAYS["read-only target"]` says why
    TARGET = ""

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

    def cannot(self, db, way=None):
        return []

    def keep_rungs(self):
        return tuple(r for r in self.rungs() if r.lacks(KEEP) is None)

    def keep(self, db, way, say):
        self.hold([db], way, say)

    def kept(self, db, record):
        return self.held(db, record.get("way"))

    def offer(self, db):
        return []


def hold_for(hop, eng, side="src"):
    """The hold of one side, as its engine does it: a source's as the
    cutover freezes it (`HOLDS`), a target's as `protect_target` keeps it
    (`KEEPS`)."""
    kind = side_engine(hop, eng, side)[0]
    return (HOLDS if side == "src" else KEEPS).get(kind, Hold)(hop, eng, side)


def not_held(kind):
    """Why migkit holds no writes of this engine, in words: the gap
    `decide` keeps for it (shape "writes held")."""
    from .decide import NOT_APPLICABLE, cell
    state_, why = cell(kind, "writes held")
    return why if state_ == NOT_APPLICABLE else \
        "migkit holds no writes of it yet"


def ways(hop, eng, dbs, side="src"):
    """(facts, rungs) of the ways this side's writes may be held: a
    source's as the cutover freezes it, a target's as it is kept from the
    application (`Hold.keep_rungs`)."""
    h = hold_for(hop, eng, side)
    return h.facts(dbs), (h.rungs() if side == "src" else h.keep_rungs())


def choose(hop, eng, dbs, how="auto", need=(), side="src"):
    """The `decide.Choice` of the way a side is held: `how` is the hop's
    `freeze:` (auto, server, database), `need` what else the way must give
    (`OURS`, for a drain by a mark on the source)."""
    from . import decide, sourcechange
    facts, rungs = ways(hop, eng, dbs, side)
    # a way that needs a change on the source holds where the hop, or an
    # operator at a terminal before, answered yes to it (`sourcechange`)
    sourcechange.put_answers(hop, facts, rungs)
    want = tuple(need) + ((SERVER,) if how == "server" else
                          (DATABASE,) if how == "database" else ())
    kind = side_engine(hop, eng, side)[0]
    return decide.climb(f"holding the {kind} {'source' if side == 'src' else 'target'}'s"
                        " writes", rungs, facts, want)


def offer(hop, eng, dbs, how="auto", need=(), side="src"):
    """(the `decide.Offer` a change on the source would make in the way a
    side is held, its facts), or (None, facts): said in the preflight
    with what it buys, never made unasked (`sourcechange`)."""
    from . import decide, sourcechange
    facts, rungs = ways(hop, eng, dbs, side)
    sourcechange.put_answers(hop, facts, rungs)
    want = tuple(need) + ((SERVER,) if how == "server" else
                          (DATABASE,) if how == "database" else ())
    kind = side_engine(hop, eng, side)[0]
    return decide.offered(f"holding the {kind} source's writes", rungs,
                          facts, want), facts


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


def cannot(hop, eng, db, way=None):
    """What migkit's account on the source lacks to freeze it the way
    `way` does (per database, where None) and give it back, as lines; []
    where it lacks nothing. Asked before anything is written: a freeze
    that revokes half and cannot end a session holds the application out
    and proves nothing."""
    try:
        return hold_for(hop, eng, "src").cannot(db, way)
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


# --- the target kept from the application (protect_target) -------------------
# The same primitive per engine (`Hold`), on the target: its ladder the
# ways that leave migkit's own account writing, per database
# (`Hold.keep_rungs`), climbed from what the target allows. A way that
# changes an access policy kept outside the data (DynamoDB's table
# policies, BigQuery's dataset access) is offered and made only once the
# operator answers yes - at a terminal under --go, or ahead in the hop
# (`target_access`). A target nothing can hold while migkit writes into it
# has the writes that are not migkit's named instead (`Hold.kept`), and the
# cutover's preflight says them as drift. Where neither is possible,
# `protect_target` stops the move before anything is copied - or, where
# the hop's goal turned it on and the hop did not, says why and moves as
# before: the goal keeps the target "where the engine allows it".
#
# Taken before the copy and again after it (`protect` is idempotent), so
# a table the copy made and a grant a default or future grant gave the
# application on it are taken in; given back when the stream is torn down
# at cutover, by `rollback --apply`, and put on again by a cutover undone.

#: the hop option a change of the target's access policy is answered under
ACCESS = "target_access"
ACCESS_FILE = "target-access.json"
QUESTION = "change the target's access for this? yes / no"
#: the ways that hold nothing and name the writes that are not migkit's:
#: what they name is drift, and stops a cutover's gate
NAMED = ("os_named",)


def target_kind(hop):
    """The engine of the hop's target, no server asked: a pair's own
    `target_engine`."""
    from .engines import ALIASES
    kind = _engine(hop)
    if kind == "hetero":
        kind = str((hop.options or {}).get("target_engine") or "postgres")
    return ALIASES.get(kind, kind)


def supported(hop):
    """Whether migkit keeps the hop's target from the application by
    itself - held, or its writes named - so that a goal may turn
    `protect_target` on (`hopkeys.goal_options`). A target kept only by a
    change of its access policy waits on an answer, which no goal gives."""
    return getattr(KEEPS.get(target_kind(hop)), "TARGET", "") in (
        "held", "named")


def from_goal(hop):
    """Whether `protect_target` is on because the hop's goal turned it
    on, not the hop itself (`hopkeys.apply_goal` keeps which)."""
    return "protect_target" in (vars(hop).get("_from_goal") or ())


def not_kept(kind):
    """Why migkit does not keep a target of this engine, in words: the gap
    `decide` keeps for it (shape "read-only target")."""
    from .decide import NOT_APPLICABLE, cell
    got, why = cell(kind, "read-only target")
    return why if got == NOT_APPLICABLE else "migkit does not keep it yet"


def answered(hop):
    """The answer to a change of the target's access policy - "yes",
    "no" - or None: the hop's own (`target_access`) over one given at a
    terminal, which is kept for the hop's later runs."""
    from .sourcechange import answer_word
    raw = (hop.options or {}).get(ACCESS)
    if raw is not None:
        return answer_word(raw)
    try:
        got = json.loads((hop.report_dir() / ACCESS_FILE).read_text())
    except (OSError, ValueError):
        return None
    return answer_word(got.get("answer")) if isinstance(got, dict) else None


def _answered_at_a_terminal(hop, answer):
    import getpass

    from . import audit
    try:
        who = getpass.getuser()
    except Exception:  # noqa: BLE001 - nobody to name
        who = ""
    path = hop.report_dir() / ACCESS_FILE
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"answer": answer, "by": who,
                               "at": time.strftime("%Y-%m-%d %H:%M:%S")}))
    tmp.replace(path)
    audit.append(hop.report_dir() / "changelog.jsonl",
                 {"op": "target-access-answer", "answer": answer})


def keeping(hop, eng, dbs):
    """The `decide.Choice` of the way the target of `dbs` is kept."""
    return choose(hop, eng, dbs, "database", need=(OURS,), side="dst")


def _not_kept_why(h, got):
    if h.TARGET:
        return ("the target is not kept from the application's writes"
                f" here: {got.reason}")
    return ("keeping the application from writing to the target is not"
            f" available for {h.kind} targets: {not_kept(h.kind)}")


def protect(hop, eng, db, say, ask=None):
    """The target of `db` kept from the application's writes while migkit
    moves into it (`protect_target`): the way climbed from what the target
    allows (`keeping`), every change recorded before it is made. Called
    again, it takes in what was made since. `ask(question)` - a terminal
    under --go - answers a change of the target's access policy offered
    and not answered yet. Returns the record; None where nothing keeps
    the target and the hop's goal, not the hop, asked for it."""
    from .sourcechange import answer_word
    h = hold_for(hop, eng, "dst")
    if h.TARGET == "offered" and ask is not None and answered(hop) is None:
        lines = h.offer(db)
        if lines:
            for line in lines:
                say(line)
            got = answer_word(ask(QUESTION)) or "no"
            _answered_at_a_terminal(hop, got)
            say(f"answered {got}, recorded for this hop's later runs")
    got = keeping(hop, eng, [db])
    if got.rung is None:
        why = _not_kept_why(h, got)
        before = state(hop, db, "dst")
        if before is not None:
            # taken before, and not to be taken again now: what it holds
            # stays, and the read back says what it does not
            say(f"{db}: {why} - what was kept before stays kept")
            return before
        if from_goal(hop):
            say(f"{db}: {why} - moved into as before: the hop's goal keeps"
                " the target where it allows it")
            return None
        raise SystemExit(f"{db}: protect_target is set, and {why} - nothing"
                         " has been copied. Remove the option to move"
                         " without it.")
    way = got.rung.name
    h.keep(db, way, say)
    record = state(hop, db, "dst")
    if record is None:
        # a way that changes the whole server's accounts or access lists
        # keeps its record for the server (`server_state`); this one says
        # the database is kept by it, and `thaw` gives it back
        record = {"db": db, "server": True}
    record["way"] = way
    _save(hop, db, record, "dst")
    return record


def not_protected(hop, eng, db):
    """Why the hop's own `protect_target` would stop a move into `db`
    before anything is copied - nothing here keeps the target, and the hop
    itself asked for it - or None: not asked, asked by its goal alone,
    kept already, or a way that keeps it. Reads only."""
    if not (hop.options or {}).get("protect_target") or from_goal(hop) \
            or state(hop, db, "dst") is not None:
        return None
    h = hold_for(hop, eng, "dst")
    if h.TARGET == "offered" and answered(hop) is None:
        # not answered yet: --go at a terminal asks it (`protect`)
        return None
    try:
        got = keeping(hop, eng, [db])
    except Exception as e:  # noqa: BLE001 - not readable: said as that
        return ("whether the target can be kept from the application could"
                f" not be read ({str(e).strip()[:120]})")
    if got.rung is not None:
        return None
    return _not_kept_why(h, got)


def protect_plan(hop, eng, dbs):
    """What `protect_target` does on the target, as the dry run says it:
    the way it takes, the change of an access policy it would offer, or
    why --go stops. Reads only."""
    out = []
    h = hold_for(hop, eng, "dst")
    for d in dbs:
        got = keeping(hop, eng, [d])
        if got.rung is not None:
            out.append(f"{d}: the target kept from the application's"
                       f" writes: {got.rung.said} ({got.reason})")
            continue
        if h.TARGET == "offered" and answered(hop) is None:
            out += h.offer(d)
            out.append("dry-run: --go at a terminal asks it")
        why = _not_kept_why(h, got)
        out.append(f"{d}: {why} - " + (
            "moved into as before: the hop's goal keeps the target where"
            " it allows it" if from_goal(hop) else
            "with --go the move stops before anything is copied"))
    return out


def kept_rows(hop, eng, d, gate):
    """[(level, what)] for the cutover's preflight: the target's hold read
    back from the server (`Hold.kept`) - a line not held is a warning, a
    write named that is not migkit's is drift, `gate`'s level - or why
    nothing holds it."""
    if not (hop.options or {}).get("protect_target"):
        return [("warn", f"{d}: the target is not held from the"
                         " application's writes (protect_target); the final"
                         " check is what catches a row written there")]
    record = state(hop, d, "dst")
    if not record:
        return [("warn", f"{d}: protect_target is set and nothing holds the"
                         " target: the application may have written there")]
    try:
        got = hold_for(hop, eng, "dst").kept(d, record)
    except Exception as e:  # noqa: BLE001 - said as the reason
        return [("warn", f"{d}: the target's hold could not be read back"
                         f" ({str(e).strip()[:160]})")]
    named = record.get("way") in NAMED
    return [("ok" if ok else gate if named else "warn",
             f"{d}: the target: {what}") for ok, what in got]


def kept_line(hop, d):
    """What `doctor` says of a target `protect_target` keeps, from its
    record - so it is said even when the target cannot be reached - or
    None."""
    record = state(hop, d, "dst")
    if not record:
        return None
    if record.get("way") in NAMED:
        return (f"{d}: the writes that are not migkit's are named before the"
                f" cutover, counted since {record.get('since') or '?'}")
    who = sorted(record.get("roles") or ())
    return (f"{d} is kept from the application's writes"
            + (f" - {', '.join(who)}" if who else ""))


# PostgreSQL

def _keeps_out():
    """A way that takes the right to connect from a role that writes as
    its tables' owner needs the database to be migkit's account's to take
    it from (a superuser's, or its owner's)."""
    from .decide import Need
    return Need(
        "migkit's account may keep a role that owns its tables out of the"
        " database",
        lambda f: f.get("owner roles") is not True
        or f.get("connect revoke", True) is True,
        lambda f: "a role of the application writes as the owner of its"
        " tables whatever is revoked, and the database is not migkit's"
        " account's to take the right to connect from"
        if f.get("connect revoke") is not None else
        "whether migkit's account may take the right to connect could not"
        " be read")


_KEEPS_OUT = _keeps_out()


class PostgresHold(Hold):
    # A managed server gives its admin no superuser: no ALTER SYSTEM, and
    # a database its admin did not make is not its own, so the right to
    # connect cannot be taken from a role that writes as the owner of its
    # tables. What such an admin may do is change the application's roles
    # (CREATEROLE; from 16, with the admin option the maker of a role
    # holds): a connection limit of none keeps an owner out of every
    # database at once, which is the hop's only where the role connects
    # to nothing else. Each is read from the server, never from its name.
    TARGET = "held"

    def keep_rungs(self):
        from .decide import Rung
        # the target's own way, measured first (the module's head): a
        # role's grants, and a role that can still write after that made
        # read-only by default here, its sessions ended - its right to
        # connect left, so it reads on; none of the source's needs, which
        # are the freeze's at cutover
        return (Rung("pg_database", "the application's grants revoked, a"
                     " role that can still write read-only by default here"
                     " and its sessions ended", gives=frozenset(KEEP),
                     because="per database"),)

    def keep(self, db, way, say):
        _pg_freeze(self.hop, self.eng, db, say, self.side)

    def kept(self, db, record):
        out = []
        for role, entry in (record.get("roles") or {}).items():
            left = _still_writes(self.eng, db, role, self.side)
            if not left:
                out.append((True, f"{role} writes to nothing here"))
            elif entry.get("set") and _setting(
                    self.eng, db, role, self.side) == "on":
                # held by a default the role may switch off itself: said
                # as not held, never as held
                out.append((False, f"{role} can write to {len(left)} tables"
                                   " as their owner or through another"
                                   " role; it is read-only by default here,"
                                   " which it may switch off itself"))
            else:
                out.append((False, f"{role} can write to {len(left)} tables"
                                   f" ({', '.join(left[:3])})"))
        return out or [(True, "no login role of the application's is on"
                              " the target")]

    def rungs(self):
        from .decide import Need, Rung
        return (
            Rung("pg_server", "the server's read-only default for every"
                 " new transaction, with the application's grants and"
                 " sessions per database", gives=frozenset({SERVER, OURS}),
                 needs=(_alone(), _is(
                     "alter system", "migkit's account may change the"
                     " server's settings", "migkit's account may not change"
                     " the server's settings (ALTER SYSTEM)"), _KEEPS_OUT),
                 because="every database on the server is the hop's"),
            Rung("pg_database", "the application's grants revoked and its"
                 " sessions ended, per database",
                 gives=frozenset({DATABASE, OURS}), needs=(_KEEPS_OUT,),
                 because="per database"),
            Rung("pg_role_limit", "the application's grants revoked, a role"
                 " that owns its tables kept from connecting (a connection"
                 " limit of none), its sessions ended",
                 gives=frozenset({DATABASE, OURS}),
                 needs=(Need("migkit's account may change the application's"
                             " roles", lambda f: f.get("alter roles") is True,
                             lambda f: "migkit's account may not change the"
                             " application's roles (CREATEROLE, and from 16"
                             " the admin option on each)"
                             if f.get("alter roles") is not None else
                             "whether migkit's account may change the"
                             " application's roles could not be read"),
                        Need("the application's roles in no other database",
                             lambda f: f.get("roles elsewhere") == [],
                             lambda f: "a connection limit keeps a role out"
                             " of every database, and the application's"
                             " roles are in "
                             + ", ".join(f.get("roles elsewhere") or
                                         ["databases that could not be"
                                          " listed"])
                             + " as well")),
                 because="a role that writes as its tables' owner is kept"
                         " out by its connection limit where the database"
                         " is not migkit's account's to take the right to"
                         " connect from"))

    def readers(self, dbs):
        roles = lambda: [r for d in dbs for r in app_roles(  # noqa: E731
            self.hop, self.eng, d, self.side)]
        return {"alter system": _read(lambda: _pg_may_alter(
                    self.eng, dbs[0], self.side)),
                "owner roles": _read(lambda: any(
                    _pg_writes_past_grants(self.eng, d, r, self.side)
                    for d in dbs for r in app_roles(self.hop, self.eng, d,
                                                    self.side))),
                "connect revoke": _read(lambda: all(
                    _pg_may_revoke_connect(self.eng, d, self.side)
                    for d in dbs)),
                "alter roles": _read(lambda: _pg_may_alter_roles(
                    self.eng, dbs[0], sorted(set(roles())), self.side)),
                "roles elsewhere": _read(lambda: _pg_roles_elsewhere(
                    self.eng, dbs, sorted(set(roles())), self.side))}

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
            _pg_source_freeze(self.hop, self.eng, d, say, self.side,
                              limit=way == "pg_role_limit")

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

    def cannot(self, db, way=None):
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
        # another role's transaction is shown only to a member of it or
        # of pg_read_all_stats: to anyone else an open one reads as none,
        # and the proof would pass over it
        stats = eng._psql(side, db, "select pg_has_role(current_user,"
                                    " 'pg_read_all_stats', 'USAGE')::text"
                          ).strip()
        if stats != "true":
            out.append("migkit's account on the source cannot see other"
                       " roles' transactions (neither superuser nor a"
                       " member of pg_read_all_stats or pg_monitor), so an"
                       " open one would read as none")
        # a connection limit keeps an owner out instead (`pg_role_limit`)
        if owner != "true" and way != "pg_role_limit":
            out.append("migkit's account on the source does not own the"
                       " database, so it cannot take the right to"
                       " connect from a role that owns its tables")
        # a managed admin that keeps the owners out by their connection
        # limit revokes only the grants other roles hold: a table of an
        # owner of its own, granted to nobody, is nothing it must give back
        granted = (" and not pg_has_role(current_user, c.relowner, 'USAGE')"
                   " and exists (select 1 from aclexplode(c.relacl) a where"
                   " a.grantee not in (0, c.relowner) and a.privilege_type"
                   " in ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE'))"
                   if way == "pg_role_limit" else "")
        n = eng._psql(
            side, db, "select count(*) from pg_class c join"
                      " pg_namespace n on n.oid = c.relnamespace where"
                      " c.relkind in ('r', 'p') and n.nspname not in"
                      " ('pg_catalog', 'information_schema') and"
                      " c.relowner <> (select oid from pg_roles where"
                      " rolname = current_user) and not"
                      " has_table_privilege(c.oid, 'INSERT WITH GRANT"
                      " OPTION')" + granted).strip()
        if n not in ("", "0"):
            out.append(f"migkit's account on the source can neither"
                       f" revoke nor give back the grants on {n} tables"
                       " it does not own")
        return out


def _pg_names(roles):
    return ", ".join(_lit(r) for r in roles) or "''"


def _pg_writes_past_grants(eng, db, role, side="src"):
    """Whether `role` writes a table here whatever its own grants: as the
    owner (or a member of the owner), or through a grant to everyone or to
    another role it is a member of - what revoking its grants leaves it."""
    got = eng._psql(
        side, db,
        "select count(*) from pg_class c join pg_namespace n on n.oid ="
        " c.relnamespace where c.relkind in ('r', 'p') and n.nspname not in"
        " ('pg_catalog', 'information_schema') and"
        f" (pg_has_role({_lit(role)}, c.relowner, 'USAGE') or exists"
        " (select 1 from aclexplode(c.relacl) a where a.privilege_type in"
        " ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE') and a.grantee <>"
        f" {_lit(role)}::regrole and (a.grantee = 0 or pg_has_role("
        f"{_lit(role)}, a.grantee, 'USAGE'))))").strip()
    return got not in ("", "0")


def _pg_may_revoke_connect(eng, db, side="src"):
    """Whether migkit's account may take the right to connect to this
    database from a role: a superuser, or its owner (or a member of it)."""
    return eng._psql(side, db,
                     "select r.rolsuper or pg_has_role(current_user,"
                     " d.datdba, 'USAGE') from pg_roles r, pg_database d"
                     " where r.rolname = current_user and d.datname ="
                     " current_database()").strip() == "t"


def _pg_may_alter_roles(eng, db, roles, side="src"):
    """Whether migkit's account may change these roles' connection limit:
    a superuser; a role that may create roles - which from 16 changes
    only a role it holds the admin option on (its maker holds it)."""
    sup, create, version = eng._psql(
        side, db, "select rolsuper::text||' '||rolcreaterole::text||' '||"
                  "current_setting('server_version_num') from pg_roles"
                  " where rolname = current_user").split()
    if sup == "true":
        return True
    if create != "true":
        return False
    supers = eng._psql(side, db, "select count(*) from pg_roles where"
                                 f" rolsuper and rolname in ({_pg_names(roles)})"
                       ).strip()
    if supers not in ("", "0"):
        return False
    if int(version) < 160000 or not roles:
        return True
    return eng._psql(side, db,
                     "select coalesce(bool_and(pg_has_role(current_user,"
                     " r, 'MEMBER WITH ADMIN OPTION')), true) from unnest("
                     f"array[{_pg_names(roles)}]::text[]) r").strip() == "t"


def _pg_roles_elsewhere(eng, dbs, roles, side="src"):
    """The server's other databases the application's roles are in - one
    they own, hold the right to connect to by name, or have a session in
    now: where a connection limit on them would reach as well."""
    if not roles:
        return []
    mine = ", ".join(_lit(eng._d(side, d)) for d in dbs)
    names = _pg_names(roles)
    got = eng._psql(
        side, dbs[0],
        "select coalesce(string_agg(d.datname, ' ' order by d.datname), '')"
        " from pg_database d where d.datallowconn and not d.datistemplate"
        f" and d.datname not in ({mine}) and (pg_get_userbyid(d.datdba) in"
        f" ({names}) or exists (select 1 from pg_stat_activity s where"
        f" s.datname = d.datname and s.usename in ({names})) or exists"
        " (select 1 from aclexplode(d.datacl) a where a.privilege_type ="
        " 'CONNECT' and a.grantee <> 0 and pg_get_userbyid(a.grantee) in"
        f" ({names})))").strip()
    return got.split()


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


def _pg_source_freeze(hop, eng, db, say, side="src", limit=False):
    """`freeze` on this side, then whatever role can still write kept out
    - the right to connect taken from it, or with `limit` its connection
    limit set to none (recorded first) - and every application session
    ended."""
    dbname = eng._d(side, db)
    # the server's clock, not this machine's: the proof asks the server
    # which sessions it opened before this
    at = eng._psql(side, db, "select clock_timestamp()::text").strip()
    record = _pg_freeze(hop, eng, db, say, side=side)
    record.setdefault("at", at)
    connect = record.setdefault("connect", {})
    roles = list(record["roles"])
    owners = [r for r in roles if _still_writes(eng, db, r, side)]
    if owners and limit:
        for r in owners:
            entry = record["roles"][r]
            if "limit" not in entry:
                entry["limit"] = int(eng._psql(
                    side, db, "select rolconnlimit from pg_roles where"
                              f" rolname = {_lit(r)}").strip())
                _save(hop, db, record, side)
            eng._psql(side, db, f"alter role {_q(r)} connection limit 0")
            say(f"{dbname}: {r} - kept from connecting (a connection limit"
                " of none, it was"
                f" {'none set' if entry['limit'] < 0 else entry['limit']}):"
                " it can write as the owner whatever is revoked")
    elif owners:
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
        # kept out by the right to connect, or by a connection limit of
        # none (`pg_role_limit`)
        can = eng._psql(side, db, f"select has_database_privilege({_lit(r)},"
                                  " current_database(), 'CONNECT') and"
                                  " (select rolconnlimit from pg_roles"
                                  f" where rolname = {_lit(r)}) <> 0"
                        ).strip()
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
# A managed server gives its admin no SUPER and no SYSTEM_VARIABLES_ADMIN:
# the whole server is not switched read-only by migkit's account, and on
# some another account's session is ended only through the service's own
# procedure (`mysql.rds_kill`). Its provider's parameter group can switch
# `read_only`, which stops every account without SUPER - the admin, the
# application, migkit's own - and not the server's replication: offered
# as a change on the source (`sourcechange`, `param_read_only`), made
# through the provider's API behind the hop's approvals and taken off at
# the thaw, never assumed. Each is read from the server or the provider's
# API, never from the address.

def _ends_sessions():
    from .decide import Need
    return Need(
        "a way to end the application's sessions",
        lambda f: f.get("kill", "kill") is not None,
        "migkit's account may not end other accounts' sessions: it holds"
        " no CONNECTION_ADMIN, and the server has no procedure of its"
        " service's that does it")


_ENDS = _ends_sessions()


def _param_ctx(f):
    from . import sourcechange as sc
    got = f.get("param read_only")
    before = (got.get("before") if isinstance(got, dict) else None) or "0"
    return sc.Ctx(where=f.get("where"), label="the source",
                  before=str(before))


def _param_stand(f):
    """Where the source's `read_only` stands through its provider's API
    (`_my_param`), as a change on the source."""
    from .decide import BY_DBA, IMPOSSIBLE, POSSIBLE, PRESENT, Stand
    got = f.get("param read_only")
    if not isinstance(got, dict):
        return Stand(IMPOSSIBLE, "the provider's API could not be asked")
    return Stand({"present": PRESENT, "possible": POSSIBLE,
                  "dba": BY_DBA}.get(got.get("state"), IMPOSSIBLE),
                 got.get("why", ""))


def _param_change():
    from . import sourcechange
    got = globals().get("_PARAM_CHANGE")
    if got is None:
        got = globals()["_PARAM_CHANGE"] = sourcechange.change(
            "param_read_only", _param_stand, _param_ctx,
            after="when the cutover gives the source back")
    return got


def _my_param(hop, eng, side="src"):
    """{"state", "why", "before"} of the source's `read_only` as its
    provider's API says it: modifiable on its own parameter group
    ("possible"), on one other instances share ("dba": a DBA gives it a
    group of its own first), not at all ("impossible"), or on already
    ("present")."""
    from . import managed
    if side != "src":
        return {"state": "impossible", "why": "only the source is frozen"
                                              " through its provider"}
    api, conf = managed.api_conf(hop, eng, "source")
    if api is None:
        return {"state": "impossible", "why": conf}
    inst = managed.adapter(hop, eng, "source").instance(names=("read_only",))
    p = inst.parameters.get("read_only")
    if p is None:
        return {"state": "impossible", "why": "its parameters list no"
                                              " read_only"}
    before = str(p.value if p.value is not None else "0")
    if before.upper() in ("1", "ON", "TRUE"):
        return {"state": "present", "why": "read_only is on already",
                "before": before}
    if p.modifiable is False:
        return {"state": "impossible", "why": "its provider does not let"
                                              " read_only be changed"}
    if inst.group_shared:
        return {"state": "dba", "why": f"its parameter group {inst.group} is"
                " shared with other instances, which would be made"
                " read-only too: a DBA gives it a group of its own",
                "before": before}
    return {"state": "possible", "before": before,
            "why": f"read_only in its own parameter group"
                   f" {inst.group or ''}".rstrip()}


def _my_param_read(hop, eng, side="src"):
    """`_my_param`, a provider that cannot be asked said as the reason."""
    try:
        return _my_param(hop, eng, side)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said, not guessed
        text = (str(e).strip().splitlines() or [type(e).__name__])[-1]
        return {"state": "impossible", "why": "the provider's API could not"
                                              f" be asked: {text[:160]}"}


def _my_param_runner(eng, side, adapter, make, undo, before, say):
    """`run(statement)` for the change `param_read_only`: its make and
    undo lines carried out through the provider's API; any other
    statement - one of an earlier change a killed run left
    (`sourcechange.undo_left`) - run on the server."""
    calls = {s: (lambda: adapter.set_parameters({"read_only": "1"}, say))
             for s in make}
    calls.update({s: (lambda: adapter.set_parameters(
        {"read_only": before}, say)) for s in undo})

    def run(statement):
        fn = calls.get(statement)
        return fn() if fn is not None else eng._q(side, statement)
    return run


def _my_param_freeze(hop, eng, say, side="src", wait=None):
    """`read_only` on through the source's provider's parameter group:
    recorded first, made as a change on the source behind the hop's
    approvals (`sourcechange.put_on`), and waited for until the server
    reads it back on - the provider applies it in its own time, which the
    cutover's clock counts."""
    from . import managed, sourcechange
    rec = server_state(hop, side) or {}
    adapter = managed.adapter(hop, eng, "source")
    if "before" not in rec:
        got = _my_param(hop, eng, side)
        rec.update({"engine": "mysql", "way": "my_param_read_only",
                    "before": {"read_only": str(got.get("before") or "0")}})
        _server_save(hop, rec, side)
    before = rec["before"]["read_only"]
    change = _param_change()
    facts = {"where": managed.side(hop, eng, "source").where,
             "param read_only": {"before": before}}
    run = _my_param_runner(eng, side, adapter, change.statements(facts),
                           change.statements(facts, "undo"), before, say)
    if not rec.get("entry"):
        entry = sourcechange.put_on(hop, change, facts, run, None, say)
        rec["entry"] = entry["id"]
        _server_save(hop, rec, side)
    end = time.monotonic() + float(wait if wait is not None else
                                   (_budget(hop) or 60))
    while True:
        on = str(eng._q(side, "select @@global.read_only")[0][0])
        if on in ("1", "ON"):
            break
        if time.monotonic() > end:
            raise SystemExit("the provider made read_only 1, and the server"
                             " still reads it off")
        time.sleep(0.5)
    rec["on"] = True
    _server_save(hop, rec, side)
    say(f"{'source' if side == 'src' else 'target'}: the whole server is"
        " read-only through its provider's parameter group (read_only), its"
        " own replication excepted")


def _my_param_thaw(hop, eng, rec, say, side="src"):
    """`read_only` given back as it was, through the provider, from the
    change's own record (`sourcechange.take_off`)."""
    from . import managed, sourcechange
    entry = sourcechange.entry(hop, rec.get("entry"))
    if entry is None:
        return
    before = str((rec.get("before") or {}).get("read_only") or "0")
    run = _my_param_runner(eng, side, managed.adapter(hop, eng, "source"),
                           entry.get("make") or [], entry.get("undo") or [],
                           before, say)
    if not sourcechange.take_off(hop, entry, run, say):
        raise SystemExit("read_only could not be given back through the"
                         " provider; it is recorded, and the next run takes"
                         " it off")
    say(f"{'source' if side == 'src' else 'target'}: writable again, its"
        " parameter group as it was before the freeze")


class MySQLHold(Hold):
    TARGET = "held"

    def keep(self, db, way, say):
        # `_my_freeze` says itself what a per-database revoke cannot reach
        self.hold([db], way, say)

    def kept(self, db, record):
        out = []
        name = self.eng._d(self.side, db)
        for u in record.get("roles") or {}:
            left = _my_grants(self.eng, name, u, self.side)
            wider = _my_wider(self.eng, u, self.side)
            out.append((not (left or wider), f"{u}: " + (
                "no write grant on the database" if not left
                else f"{len(left)} write grants left")
                + (f", and it {' and '.join(wider)}" if wider else "")))
        return out or [(True, "no account of the application's is on the"
                              " target")]

    def rungs(self):
        from .decide import Need, Rung
        return (
            Rung("my_server", "the server read-only for every account, its"
                 " administrators too", gives=frozenset({SERVER}),
                 needs=(_alone(), _is(
                     "read only switch", "migkit's account may switch the"
                     " server read-only", "migkit's account may not change"
                     " the server's read_only"), _ENDS),
                 because="every database on the server is the hop's"),
            Rung("my_param_read_only", "the server read-only through its"
                 " provider's parameter group for every account without"
                 " SUPER - its admin too - the application's sessions"
                 " ended", gives=frozenset({SERVER}),
                 needs=(_alone(), _ENDS), changes=(_param_change(),),
                 because="every database on the server is the hop's, and"
                         " its account may not switch it read-only itself:"
                         " its provider's parameter group does, where the"
                         " operator answers yes"),
            Rung("my_database", "the application's grants revoked and its"
                 " sessions ended, per database",
                 gives=frozenset({DATABASE, OURS}),
                 needs=(_ENDS, Need(
                     "the right to revoke and give back grants",
                     lambda f: f.get("grant option", True) is not False,
                     "migkit's account may not revoke or give back the"
                     " application's grants (it holds no GRANT OPTION)")),
                 because="per database"))

    def readers(self, dbs):
        from . import managed
        which = "source" if self.side == "src" else "target"
        return {"read only switch": _read(lambda: _my_may_switch(
                    self.eng, self.side)),
                "kill": _read(lambda: _my_kill_way(self.eng, self.side)),
                "grant option": _read(lambda: "WITH GRANT OPTION" in " ".join(
                    str(r[0]).upper() for r in self.eng._q(
                        self.side, "show grants for current_user()"))),
                "where": _read(lambda: managed.side(self.hop, self.eng,
                                                    which).where),
                "param read_only": lambda: _my_param_read(
                    self.hop, self.eng, self.side)}

    def others(self, dbs):
        have = {r[0] for r in self.eng._q(
            self.side, "select schema_name from information_schema.schemata")}
        return sorted(have - {self.eng._d(self.side, d) for d in dbs}
                      - {"mysql", "sys", "information_schema",
                         "performance_schema"})

    def hold(self, dbs, way, say):
        if way == "my_param_read_only":
            _my_param_freeze(self.hop, self.eng, say, self.side)
        elif SCOPE[way] == "server":
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
        if rec.get("way") == "my_param_read_only":
            _my_param_thaw(self.hop, self.eng, rec, say, self.side)
            return
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

    def cannot(self, db, way=None):
        out = []
        grants = " ".join(str(r[0]).upper() for r in self.eng._q(
            self.side, "show grants for current_user()"))
        if _my_kill_way(self.eng, self.side) is None:
            out.append("migkit's account on the source cannot end other"
                       " accounts' sessions (CONNECTION_ADMIN, or a"
                       " procedure of its service's that does it)")
        # a server made read-only revokes nothing
        if "WITH GRANT OPTION" not in grants and SCOPE.get(way) != "server":
            out.append("migkit's account on the source cannot revoke or"
                       " give back the application's grants (GRANT"
                       " OPTION)")
        return out


def _my_may_switch(eng, side="src"):
    grants = " ".join(str(r[0]).upper() for r in eng._q(
        side, "show grants for current_user()"))
    return any(p in grants for p in ("ALL PRIVILEGES ON *.*", "SUPER",
                                     "SYSTEM_VARIABLES_ADMIN"))


# TiDB
# MySQL's grant tables and its per-database way, with TiDB's own word for
# ending a session (`_my_kill`). Its switch for the whole cluster is its
# own, `tidb_super_read_only` (6.2 on), which refuses every account's
# writes, an administrator's too: MySQL's `super_read_only` is one TiDB
# keeps to be read and holds nothing by (TiDB's own reference). What is
# held open is its own table of transactions (`cluster_tidb_trx`, 5.4 on),
# every server of the cluster's at once, one that has written holding
# keys in its buffer; TiDB takes no prepared transaction of a client's
# (no XA).

def _tidb_version(eng, side="src"):
    """(major, minor) of the TiDB a side runs, from `version()` -
    `8.0.11-TiDB-v7.5.0`."""
    got = str(eng._q(side, "select version()")[0][0])
    tail = got.split("TiDB-v", 1)[-1] if "TiDB-v" in got else ""
    nums = [int(x) for x in tail.split("-")[0].split(".")[:2] if x.isdigit()]
    return tuple(nums) if len(nums) == 2 else None


class TiDBHold(MySQLHold):
    def rungs(self):
        from .decide import Rung
        return (
            Rung("tidb_server", "the whole cluster read-only for every"
                 " account, its administrators too (tidb_super_read_only),"
                 " the application's sessions ended",
                 gives=frozenset({SERVER}),
                 needs=(_alone(), _at_least(
                     "version", (6, 2), "a read-only switch that holds"
                     " needs 6.2"), _is(
                     "read only switch", "migkit's account may switch the"
                     " cluster read-only", "migkit's account may not change"
                     " the cluster's settings (SUPER or"
                     " SYSTEM_VARIABLES_ADMIN)"), _ENDS),
                 because="every database on the cluster is the hop's"),
        ) + tuple(r for r in super().rungs() if r.name == "my_database")

    def readers(self, dbs):
        return {**super().readers(dbs),
                "version": _read(lambda: _tidb_version(self.eng, self.side))}

    def hold(self, dbs, way, say):
        if way != "tidb_server":
            return super().hold(dbs, way, say)
        rec = server_state(self.hop, self.side) or {}
        if "before" not in rec:
            got = self.eng._q(self.side,
                              "select @@global.tidb_super_read_only")
            rec.update({"engine": "tidb", "way": way,
                        "before": int(got[0][0]) if got else 0})
            _server_save(self.hop, rec, self.side)
        self.eng._q(self.side, "set global tidb_super_read_only = on")
        rec["on"] = True
        _server_save(self.hop, rec, self.side)
        say(f"{'source' if self.side == 'src' else 'target'}: the whole"
            " cluster is read-only (tidb_super_read_only)")
        for d in dbs:
            _my_end_sessions(self.hop, self.eng, d, say, self.side)

    def release_server(self, rec, say):
        if rec.get("way") != "tidb_server":
            return super().release_server(rec, say)
        if rec.get("on"):
            self.eng._q(self.side, "set global tidb_super_read_only ="
                                   f" {int(rec.get('before') or 0)}")
        say(f"{'source' if self.side == 'src' else 'target'}: writable"
            " again, as it was before the freeze")

    def held(self, db, way):
        if way != "tidb_server":
            return super().held(db, way)
        got = self.eng._q(self.side, "select @@global.tidb_super_read_only")
        on = bool(got and int(got[0][0]))
        left = self.open(db, None)
        return [(on, f"tidb_super_read_only is {'on' if on else 'off'}"),
                (not left, "; ".join(left[:3]) if left else
                 "no transaction holding writes")]

    def writes(self, db):
        # no running count of writes a table that TiDB keeps: the open
        # transactions are what is read
        return None

    def open(self, db, older_than):
        age = "" if older_than is None else \
            f" and start_time < now() - interval {float(older_than)} second"
        got = self.eng._q(
            self.side, "select user, session_id, timestampdiff(second,"
                       " start_time, now()) from"
                       " information_schema.cluster_tidb_trx where"
                       " mem_buffer_keys > 0 and (user is null or user <>"
                       " %s)" + age, (self.me,))
        return [f"write transaction {u} session {i}, open {s}s"
                for u, i, s in got]


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
        # read_only where the provider's parameter group set it: an
        # account without SUPER is what it stops (`my_param_read_only`)
        var = ("read_only" if (server_state(hop, side) or {}).get("way")
               == "my_param_read_only" or _my_brand(eng, side) == "mariadb"
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
#
# A MongoDB-compatible service keeps some of these commands from its admin
# or does not have them at all - a write block, a lock, ending a session
# by its id - so the commands the server lists (`listCommands`) decide
# the way as well as the account's rights: a service that lists no write
# block and no lock is held per database, its operations ended one by one
# where it ends no session. A server that does not answer the list is
# asked as before.

MONGO_SYSTEM = ("admin", "local", "config")
_MONGO_WRITE_ROLES = ("readWrite", "dbOwner")
#: the roles that write to every database, which a role taken on one does
#: not reach
_MONGO_WIDE = ("readWriteAnyDatabase", "root", "restore", "__system")


def _lists(*names):
    """A need that the server lists every one of `names` - or listed
    nothing to ask (`commands` None), as before the list was read."""
    from .decide import Need
    return Need(f"the server's {' and '.join(names)}",
                lambda f: f.get("commands") is None
                or set(names) <= set(f.get("commands") or ()),
                lambda f: "the server has no "
                + ", ".join(n for n in names
                            if n not in set(f.get("commands") or ()))
                + " (its service keeps it from every account)")


class MongoHold(Hold):
    def rungs(self):
        from .decide import Need, Rung
        return (
            Rung("mongo_block", "users' writes blocked on the server",
                 gives=frozenset({SERVER}),
                 needs=(_alone(), _at_least(
                     "version", (6, 0), "a block on users' writes needs"
                     " 6.0"), _lists("setUserWriteBlockMode"), Need(
                     "the right to block users' writes",
                     lambda f: "setUserWriteBlockMode" in (f["actions"]
                                                           or ()),
                     "migkit's account may not block the server's"
                     " writes")),
                 because="every database on the server is the hop's"),
            Rung("mongo_fsync", "the server locked, its own writes too",
                 gives=frozenset({SERVER}),
                 needs=(_alone(), _lists("fsync", "fsyncUnlock"), Need(
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
                 gives=frozenset({DATABASE, OURS}),
                 # a server that checks nobody lets anyone who reaches it
                 # write, whatever an account's roles say: a role taken
                 # there holds no one, and the read back of the roles
                 # would say held
                 needs=(_lists("grantRolesToUser", "revokeRolesFromUser"),
                        _is("auth", "the server checks who signs in",
                            "the server checks no accounts (authorization"
                            " off): anyone who reaches it writes, whatever"
                            " an account's roles say")),
                 because="per database"))

    TARGET = "held"

    def readers(self, dbs):
        c = self.eng._client(self.side)
        return {"version": _read(lambda: tuple(c.admin.command(
                    "buildInfo").get("versionArray", [0, 0])[:2])),
                "actions": _read(lambda: _mongo_actions(c)),
                "commands": _read(lambda: _mongo_commands(c)),
                "auth": _read(lambda: _mongo_auth(c))}

    def kept(self, db, record):
        name = self.eng._d(self.side, db)
        out = []
        for u, d, roles, bypass in _mongo_users(self.hop, self.eng,
                                                self.side):
            here = [r["role"] for r in roles if r.get("db") == name
                    and r.get("role") in _MONGO_WRITE_ROLES]
            wide = [r["role"] for r in roles if r.get("role") in _MONGO_WIDE]
            if here or wide or bypass:
                out.append((False, f"{u}@{d} still writes here ("
                            + ", ".join(here + wide or
                                        ["bypassWriteBlockingMode"]) + ")"))
        held = len(record.get("roles") or {})
        return out or [(True, f"{held} accounts hold no write here")]

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
            wide = [r["role"] for r in roles if r.get("role") in _MONGO_WIDE]
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


def _mongo_auth(c):
    """Whether the server checks who signs in: its own options say so, or
    it refuses to show them to an account that may not ask, which is that
    check. A service that does not have the command at all, and took
    migkit's sign-in, checks it."""
    from pymongo.errors import OperationFailure
    try:
        got = c.admin.command("getCmdLineOpts")
    except OperationFailure as e:
        if e.code == 13:
            return True
        status = c.admin.command("connectionStatus")
        return bool((status.get("authInfo") or {}).get("authenticatedUsers"))
    sec = (got.get("parsed") or {}).get("security") or {}
    return (sec.get("authorization") == "enabled" or bool(sec.get("keyFile"))
            or str(sec.get("clusterAuthMode") or "") in (
                "keyFile", "sendKeyFile", "sendX509", "x509"))


def _mongo_actions(c):
    status = c.admin.command({"connectionStatus": 1, "showPrivileges": True})
    return sorted({a for p in (status.get("authInfo") or {}).get(
        "authenticatedUserPrivileges", [])
        if (p.get("resource") or {}).get("cluster")
        for a in p.get("actions", [])})


def _mongo_commands(c):
    """The commands the server lists, or None where it lists none."""
    got = c.admin.command("listCommands").get("commands")
    return sorted(got) if got else None


def _mongo_users(hop, eng, side="src"):
    """[(user, its database, roles, whether it may write through a
    blocked server)] of every account on the side, but migkit's and the
    server's own. The privileges are asked one account at a time: the
    server gives them only to a query naming the account (measured on 7.0,
    `usersInfo: {forAllDBs: true}` with `showPrivileges` is refused). A
    service that keeps every account in `admin` and refuses `forAllDBs`
    is asked there; one that shows no privileges gives no account a
    bypass."""
    from pymongo.errors import OperationFailure
    c = eng._client(side)
    me = (hop.source if side == "src" else hop.target).user
    try:
        got = c.admin.command({"usersInfo": {"forAllDBs": True}})
    except OperationFailure:
        got = c.admin.command({"usersInfo": 1})
    out = []
    for u in got.get("users", []):
        if u["user"] == me or u.get("db", "admin") in ("local", "config") \
                or u["user"].startswith("__"):
            continue
        udb = u.get("db", "admin")
        try:
            one = c[udb].command({"usersInfo": u["user"],
                                  "showPrivileges": True})["users"][0]
        except OperationFailure:
            one = {}
        out.append((u["user"], udb, u.get("roles", []),
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
    from pymongo.errors import OperationFailure
    names = {(u, d) for u, d in users}
    c = eng._client(side)
    try:
        listed = _mongo_commands(c)
    except OperationFailure:
        listed = None
    # a service without the aggregation stage answers the command, and one
    # that ends no session by its id has its operations ended one by one
    by_session = listed is None or "killSessions" in listed
    try:
        seen = list(c.admin.aggregate([{"$currentOp": {
            "allUsers": True, "idleSessions": True}}]))
    except OperationFailure:
        seen = c.admin.command("currentOp", {"$all": True}).get("inprog")
    lsids, ops = [], []
    for op in seen or []:
        who = {(x.get("user"), x.get("db"))
               for x in op.get("effectiveUsers") or []}
        if not who & names:
            continue
        if by_session and (op.get("lsid") or {}).get("id") is not None:
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


def _one_server():
    """A Redis side that is one server: a cluster keeps its accounts and
    its pause node by node, where migkit's hold reaches the one node it
    signs in to - a hold of one node read back as the whole."""
    return _is("one server", "one server, not a cluster", "a cluster keeps"
               " its accounts and its pause node by node, and migkit's"
               " hold reaches the node it signs in to")


class RedisHold(Hold):
    def rungs(self):
        from .decide import Need, Rung
        return (
            Rung("redis_pause", "every client's writes held on the server,"
                 " the connections open before it ended",
                 gives=frozenset({SERVER}),
                 needs=(_alone(), _one_server(), _at_least(
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
                 needs=(_one_server(), _at_least(
                     "version", (7, 0), "an account's writes are read back"
                     " as refused from 7.0 (ACL DRYRUN)"), _is(
                     "acl", "accounts on the server", "the server keeps"
                     " no accounts (ACL)")),
                 because="per account"))

    TARGET = "held"

    def keep_rungs(self):
        # on a target migkit writes on through its own account, which only
        # an account of its own - not the server's default one, which the
        # application's connections may sign in as too - can be told apart
        r = self.rungs()[1]
        return (dataclasses.replace(
            r, said="every account's writes but migkit's refused, the"
                    " connections open before it ended",
            gives=frozenset(KEEP), needs=r.needs + (_is(
                "own account", "an account of migkit's own", "migkit signs"
                " in as the server's default account, which the"
                " application's connections may use as well: give migkit"
                " an account of its own (ACL SETUSER) for the others to be"
                " held"),)),)

    def readers(self, dbs):
        return {"version": _read(self.version),
                "acl": _read(lambda: self.eng._acl(self.side)),
                "one server": _read(lambda: not self.eng._cluster(
                    self.side)),
                "own account": lambda: self.side == "src" or (
                    self.me or "default") != "default",
                "stream back": lambda: self.side == "src" and str(
                    (self.hop.options or {}).get("reverse", "") or ""
                ).lower() == "at_cutover"}

    def kept(self, db, record):
        c = self.c()
        writers = [u for u in self.accounts() if str(c.execute_command(
            "ACL", "DRYRUN", u, "SET", "migkit:probe", "1")) == "OK"]
        rec = server_state(self.hop, self.side) or {}
        since = max(0, int(time.time() - float(rec.get("at")
                                               or time.time())))
        left = self.clients(c, set(rec.get("rules") or {}),
                            before=since + 1)
        return [(not writers, f"still able to write: {', '.join(writers)}"
                 if writers else f"{len(rec.get('rules') or {})} accounts"
                                 " refused a write"),
                (not left, f"{len(left)} connections left from before the"
                           " hold")]

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
        """The accounts held: on a source every one but the replication
        one - nothing migkit does to a source at cutover is a write - and
        on a target migkit's own left writing too."""
        rules = self.rules()
        mine = (self.me or "default") if self.side == "dst" else None
        return [u for u in (_named(self.hop) or sorted(rules))
                if u in rules and u not in (self.eng.REPL_USER, mine)]

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

    TARGET = "held"

    def kept(self, db, record):
        out = []
        # every user of the application's there now, one made since the
        # hold among them
        for u in self.users(db):
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
        return out or [(True, "no user of the application's is in the"
                              " database")]


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

    TARGET = "held"

    def keep_rungs(self):
        # a target is kept before the copy, outside any outage: the cache
        # is waited out whatever its length, and no budget is held to it
        r = self.rungs()[0]
        return (dataclasses.replace(r, needs=r.needs[:2]),)

    def kept(self, db, record):
        ks = self.ks(db)
        out = []
        for r in self.roles():
            left = sorted({res for res, _ in self.modify(r, ks, False)})
            out.append((not left, f"{r}: " + (f"may still write {left[0]}"
                                              if left else "writes nothing"
                                              " here")))
        return out or [(True, "no role of the application's signs in")]

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

def _kafka_deny(kind, name, principal="User:*", filtered=False):
    """A deny of WRITE on one resource by its name, for `principal` -
    every principal by default; a filter that finds it where `filtered`."""
    from kafka.admin import (ACL, ACLFilter, ACLOperation,
                             ACLPermissionType, ACLResourcePatternType,
                             ResourcePattern, ResourcePatternFilter,
                             ResourceType)
    rtype = getattr(ResourceType, kind)
    if filtered:
        return ACLFilter(principal=principal, host="*",
                         operation=ACLOperation.WRITE,
                         permission_type=ACLPermissionType.DENY,
                         resource_pattern=ResourcePatternFilter(
                             rtype, name, ACLResourcePatternType.LITERAL))
    return ACL(principal=principal, host="*", operation=ACLOperation.WRITE,
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
        out = {"authorizer": _read(self.authorizer)}
        if self.side == "dst":
            out.update({
                "super": _read(lambda: self.principal() is not None
                               and self.principal() in self.super_users()),
                "principal": lambda: self.principal() is not None,
                "closed": _read(self.closed),
                "named allows": _read(lambda: "User:*" not in self.allowed(
                    self.topics(dbs, every=True)))})
        return out

    def topics(self, dbs, every=False):
        """The hop's topics, as this side names them: the source's own
        names, which the copy keeps - on a target those it holds already,
        or `every` one of them, made or not."""
        mine = sorted({t for d in dbs for _, t in
                       self.eng.list_move_tables(d)})
        if self.side == "src" or every:
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
    def there(admin, kind, name, principal="User:*"):
        got, _ = admin.describe_acls(_kafka_deny(kind, name, principal,
                                                 filtered=True))
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

    def broker(self):
        """The first broker's configuration, as the cluster describes it."""
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
        return got.get(node) or {}

    def super_users(self):
        return [p.strip() for p in str(self.broker().get("super.users")
                                       or "").split(";")
                if p.strip() and p.strip() not in ("None", "null")]

    def exempt(self, db, way):
        mine = self.principal() if self.side == "dst" else None
        return [f"{p} is one of the cluster's super users: nothing short of"
                " stopping it keeps it from writing"
                for p in self.super_users() if p != mine]

    # the target: kept from the application while migkit writes into it.
    # A deny for every principal refuses migkit's own as well, but for a
    # super user, which passes every check; else each principal an allow
    # lets write the hop's topics is denied by name, on a cluster that
    # refuses a principal no ACL names and where no allow names every
    # principal. A deny on a topic's name holds before the copy makes it.

    TARGET = "held"

    def keep_rungs(self):
        from .decide import Rung
        checked = _is("authorizer", "access checked on the cluster", "the"
                      " cluster checks no access (no authorizer)")
        return (
            Rung("kafka_super", "every principal's writes to the hop's"
                 " topics refused - migkit's own passes, as one of the"
                 " cluster's super users", gives=frozenset(KEEP),
                 needs=(checked, _is(
                     "super", "migkit signs in as one of the cluster's super"
                     " users", "migkit does not sign in as one of the"
                     " cluster's super users, which alone pass a deny for"
                     " every principal")),
                 because="a deny outranks every allow, and a super user"
                         " passes it"),
            Rung("kafka_principals", "the writes of each principal an allow"
                 " lets write the hop's topics refused by name, migkit's own"
                 " left", gives=frozenset(KEEP),
                 needs=(checked, _is(
                     "principal", "migkit signs in as a principal of its own",
                     "migkit signs in with no SASL account of its own"
                     " (anonymously, by a certificate or by IAM), so its"
                     " principal cannot be told from the application's"),
                     _is("closed", "the cluster refuses a principal no ACL"
                         " names", "the cluster lets any principal write a"
                         " topic no ACL names"
                         " (allow.everyone.if.no.acl.found)"),
                     _is("named allows", "every allow on the hop's topics"
                         " names its principal", "an allow for every"
                         " principal (User:*) reaches the hop's topics: only"
                         " a deny for every principal outranks it, and that"
                         " refuses migkit's own")),
                 because="per principal"))

    def principal(self):
        """migkit's principal on this side as the cluster names it - its
        SASL account - or None where it signs in otherwise."""
        ep = self.hop.source if self.side == "src" else self.hop.target
        o = ep.options or {}
        proto = str(o.get("security_protocol") or "").upper()
        mech = str(o.get("sasl_mechanism") or "PLAIN").upper()
        if proto.startswith("SASL") and mech != "AWS_MSK_IAM" and ep.user:
            return f"User:{ep.user}"
        return None

    def allowed(self, topics):
        """{principal} an allow lets write any of `topics`: a literal, a
        prefixed or an every-topic one."""
        from kafka.admin import (ACLFilter, ACLOperation, ACLPermissionType,
                                 ACLResourcePatternType,
                                 ResourcePatternFilter, ResourceType)
        out = set()
        admin = self.eng._admin(self.side)
        try:
            for t in topics:
                got, _ = admin.describe_acls(ACLFilter(
                    principal=None, host=None, operation=ACLOperation.ANY,
                    permission_type=ACLPermissionType.ALLOW,
                    resource_pattern=ResourcePatternFilter(
                        ResourceType.TOPIC, t,
                        ACLResourcePatternType.MATCH)))
                out |= {a.principal for a in got if a.operation in (
                    ACLOperation.WRITE, ACLOperation.ALL)}
        finally:
            admin.close()
        return out

    def closed(self):
        got = self.broker().get("allow.everyone.if.no.acl.found")
        return None if got is None else str(got).strip().lower() == "false"

    def keep(self, db, way, say):
        topics = self.topics([db], every=True)
        if way == "kafka_super":
            want = [["TOPIC", t] for t in topics]
        else:
            skip = {self.principal(), "User:*", *self.super_users()}
            want = [["TOPIC", t, p] for t in topics
                    for p in sorted(self.allowed([t])) if p not in skip]
        admin = self.eng._admin(self.side)
        try:
            rec = server_state(self.hop, self.side) or {
                "engine": "kafka", "way": way, "made": []}
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
        finally:
            admin.close()
        say(f"{db}: " + (f"every principal refused writes on {len(topics)}"
                         " topics, migkit's own passing as a super user"
                         if way == "kafka_super" else
                         f"{len({x[2] for x in want})} principals refused"
                         f" writes on {len(topics)} topics by name"))
        for line in self.exempt(db, way):
            say(f"{db}: still able to write here: {line}")

    def kept(self, db, record):
        rec = server_state(self.hop, self.side) or {}
        made = rec.get("made") or []
        admin = self.eng._admin(self.side)
        try:
            missing = [x for x in made if not self.there(admin, *x)]
        finally:
            admin.close()
        out = [(not missing, "not denied: " + ", ".join(
            f"{x[2] + ' on ' if len(x) > 2 else ''}{x[1]}"
            for x in missing[:3]) if missing else
            f"{len(made)} denies on the cluster's record")]
        if rec.get("way") == "kafka_principals":
            topics = self.topics([db], every=True)
            denied = {x[2] for x in made if len(x) > 2}
            loose = sorted(self.allowed(topics) - denied - {
                self.principal(), *self.super_users()})
            out.append((not loose, f"allowed to write and not denied:"
                                   f" {', '.join(loose[:3])}" if loose else
                        "every principal allowed to write is denied"))
        return out


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
#
# So a target is not held: a block would refuse migkit's own writes, and
# the cluster grants nothing by account that could be taken from the
# application alone (a security plugin's roles allow, they never deny).
# Its writes are named instead: every operation on a shard takes the next
# sequence number, which no restart takes back, so the operations each of
# the hop's indices has taken since the target was protected - its
# primaries' highest numbers - less the ones migkit's own requests were
# acknowledged for (`counted`, by every process writing it) are writes
# that are not migkit's. An operation migkit sent and was not told of
# counts against the application, never the other way round; an index
# made again since starts its numbers again and is named by its new id.

#: where migkit's own acknowledged operations into a target whose writes
#: are named are counted, a line a request (`counted`)
OURS_FILE = "named-writes.jsonl"


def counted(hop, counts):
    """migkit's own operations the target acknowledged ({index: n}),
    appended where the hop counts them - a line a request, one write, so a
    tail in another process adds its own beside the copy's. Nothing is
    written where the target's writes are not named."""
    counts = {k: int(v) for k, v in (counts or {}).items() if v}
    if not counts:
        return
    path = _ours_path(hop)
    if not path.exists():
        return
    with open(path, "a") as f:
        f.write(json.dumps(counts) + "\n")


def _ours_path(hop):
    """The count's file, found without making the hop's directory: a hop
    whose target's writes are not named leaves nothing behind."""
    from . import config
    if "report_dir" in vars(hop):
        return hop.report_dir() / OURS_FILE
    return config.REPORTS / str(hop.name) / OURS_FILE


def _ours(hop):
    out = {}
    try:
        lines = _ours_path(hop).read_text().splitlines()
    except OSError:
        return out
    for line in lines:
        try:
            got = json.loads(line)
        except ValueError:
            continue
        for k, v in got.items():
            out[k] = out.get(k, 0) + int(v)
    return out


class OpenSearchHold(Hold):
    TARGET = "named"

    def keep_rungs(self):
        from .decide import Rung
        return (Rung(
            "os_named", "the writes that are not migkit's named: each"
            " index's own count of its operations, less migkit's",
            gives=frozenset(KEEP), needs=(_is(
                "numbered", "every shard's operations numbered", "the"
                " cluster keeps no numbers of its shards' operations"),),
            because="a write block refuses migkit's own writes as well, and"
                    " the cluster grants nothing that could be taken from"
                    " the application alone"),)

    def ops(self, db):
        """{index: [its id, operations its primaries have taken]} of every
        index under the hop's prefix on this side."""
        pre = self.eng._prefix(self.side, db)
        ids = {i["index"]: i.get("uuid") for i in self.call(
            "GET", f"/_cat/indices/{pre}*?format=json&h=index,uuid") or []}
        got = self.call("GET", f"/{pre}*/_stats/docs?level=shards") or {}
        out = {}
        for index, body in (got.get("indices") or {}).items():
            n = 0
            for copies in (body.get("shards") or {}).values():
                for c in copies:
                    if (c.get("routing") or {}).get("primary"):
                        n += int((c.get("seq_no") or {}).get(
                            "max_seq_no", -1)) + 1
            out[index] = [ids.get(index), n]
        return out

    def numbered(self, db):
        got = self.call("GET", "/") or {}
        major = str((got.get("version") or {}).get("number") or "0")
        # sequence numbers came with 6.0 of the engine OpenSearch forked
        return (got.get("version") or {}).get("distribution") == \
            "opensearch" or int(major.split(".")[0]) >= 6

    def keep(self, db, way, say):
        first = state(self.hop, db, self.side) is None
        record = state(self.hop, db, self.side) or {
            "db": db, "way": way, "base": {},
            "since": time.strftime("%Y-%m-%d %H:%M:%S")}
        # read before migkit's own are counted: one of them landing in
        # between counts against the application, never for it
        for index, (uid, n) in self.ops(db).items():
            if index not in record["base"]:
                # one there at the first call counts from what it held; one
                # made since starts at nothing, whatever the copy wrote
                record["base"][index] = [uid, n if first else 0]
        _save(self.hop, db, record, self.side)
        path = self.hop.report_dir() / OURS_FILE
        if not path.exists():
            path.touch()
        say(f"{db}: nothing on the cluster holds a writer and leaves"
            " migkit's own, so the writes that are not migkit's are named"
            f" before the cutover: {len(record['base'])} indices counted"
            f" from {record['since']}")

    def kept(self, db, record):
        now = self.ops(db)
        ours = _ours(self.hop)
        base = record.get("base") or {}
        out = []
        for index in sorted(set(now) | set(base)):
            uid, n = now.get(index, [None, 0])
            was_uid, was = base.get(index, [uid, 0])
            if index in base and index not in now:
                out.append((False, f"{index}: gone from the target since"
                                   f" {record.get('since')}"))
            elif was_uid and uid and was_uid != uid:
                out.append((False, f"{index}: made again since"
                                   f" {record.get('since')}: its numbers"
                                   " started again, and what it took before"
                                   " is not counted"))
            elif n - was - ours.get(index, 0) > 0:
                out.append((False, f"{index}: {n - was - ours.get(index, 0)}"
                                   " writes that are not migkit's since"
                                   f" {record.get('since')}"))
        return out or [(True, "no write that is not migkit's on"
                              f" {len(now)} indices since"
                              f" {record.get('since')}")]

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
        if record.get("way") in NAMED:
            # nothing was taken; the count of migkit's own goes with the
            # last database named
            if not any((state(self.hop, d, self.side) or {}).get("way")
                       in NAMED for d in (self.hop.databases or [])
                       if d != db):
                (self.hop.report_dir() / OURS_FILE).unlink(missing_ok=True)
            say(f"{db}: its writes no longer named")
            return
        for index, was in (record.get("indices") or {}).items():
            self.call("PUT", f"/{index}/_settings",
                      {"index.blocks.write": was})
        say(f"{record.get('db') or db}: {len(record.get('indices') or {})}"
            " indices open to writes as they were")

    def readers(self, dbs):
        return {"numbered": _read(lambda: self.numbered(dbs[0]))}

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


# --- the holds of the engines that grant rights --------------------------------
# An engine that grants rights is held the way PostgreSQL's application is:
# each of the application's accounts loses the rights to write it holds on
# the hop's database - recorded first, given back exactly - and its open
# sessions are ended where migkit's account may, so nothing it had begun
# is written after. What a revoke on one database cannot reach is named
# and left (`exempt`): on a source, a way in that it leaves - an owner, a
# grant to everyone that migkit's own writes rest on, a role migkit's
# account holds - says "a per-database freeze does not reach that", which
# the cutover's preflight stops on, and an account of the server's own
# that nothing short of stopping it holds is said. One logic (`_Grants`),
# an adapter an engine: its accounts, the grants each holds here, the
# statements that take and give one back, what an account can still
# write, its sessions, and what other sessions hold open. A source's hold
# (`SOURCE`) ends every session of the accounts it holds - one with a
# transaction open would otherwise commit what it wrote before the revoke
# after the position the drain waits for - and reads back that none is
# left from before it and that nothing is held open; its rung needs both
# to be in migkit's account's reach. An engine whose sessions and open
# work cannot be read so does not freeze a source, and
# `decide.WAYS["writes held"]` says why.

#: what a way into the database a per-database freeze leaves says, as the
#: cutover's preflight reads it (`bypass`)
LEAVES = "a per-database freeze does not reach that"


def _autocommits(conn):
    got = getattr(conn, "autocommit", False)
    return got is True


def _source_needs():
    """What a source's freeze by rights needs beyond the target's: a way to
    end the application's sessions, and their open work in sight - read
    back as none where it cannot be seen, it would pass for none."""
    from .decide import Need
    return (
        Need("a way to end the application's sessions",
             lambda f: f.get("ends") is True,
             lambda f: "migkit's account may not end the application's"
             " sessions here" if f.get("ends") is not None else
             "whether migkit's account may end the application's sessions"
             " could not be read"),
        Need("the application's open work in sight",
             lambda f: f.get("sees") is True,
             lambda f: "migkit's account cannot see what other sessions"
             " hold open here, so an open write would read as none"
             if f.get("sees") is not None else "whether migkit's account"
             " sees other sessions' open work could not be read"))


def _tried(fn):
    """True where `fn()` runs, False where the server refuses it."""
    try:
        fn()
        return True
    except Exception:  # noqa: BLE001 - refused: not in reach
        return False


class _Grants(Hold):
    TARGET = "held"
    #: the engine's hold freezes a source at cutover too (`HOLDS`)
    SOURCE = False
    #: the way's name and what it is, in words; what it ends on a source
    WAY, SAID = "", ""
    ENDS = "every session of theirs ended"

    def rungs(self):
        from .decide import Rung
        if not self.SOURCE:
            return ()
        return (Rung(self.WAY, f"{self.SAID}; on a source, {self.ENDS}",
                     gives=frozenset(KEEP),
                     needs=self.needs() + _source_needs(),
                     because="per account"),)

    def keep_rungs(self):
        from .decide import Rung
        return (Rung(self.WAY, self.SAID, gives=frozenset(KEEP),
                     needs=self.needs(), because="per account"),)

    def needs(self):
        return ()

    def readers(self, dbs):
        if self.side != "src":
            return {}
        return {"ends": _read(lambda: self.may_end(dbs[0])),
                "sees": _read(lambda: self.may_see(dbs[0]))}

    def name(self, db):
        return self.eng._d(self.side, db)

    def rows(self, db, sql, args=()):
        return self.eng._rows(self.side, db, sql, tuple(args) or None)

    def run(self, db, *stmts):
        """Statements that change rights, each as the engine takes it and
        committed where the driver does not commit them itself."""
        conn = self.eng._connect(self.side, db)
        try:
            cur = conn.cursor()
            for s in stmts:
                cur.execute(s)
            if not _autocommits(conn):
                conn.commit()
        finally:
            conn.close()

    # the adapter
    def accounts(self, db):
        """The application's accounts here: every one that holds a right
        to write here but migkit's own, the system's, and those `wider`
        names."""
        return []

    def grants(self, db, who):
        """The write grants `who` holds here, each as a list the record
        keeps and `take`/`give` read."""
        return []

    def take(self, db, who, grant):
        return []

    def give(self, db, who, grant):
        return []

    def still(self, db, who):
        """What `who` can still write here, as words; [] where nothing."""
        return [self.said(g) for g in self.grants(db, who)]

    def said(self, grant):
        return " ".join(str(x) for x in grant if x not in (None, ""))

    def end(self, db, who):
        """`who`'s open sessions ended: how many, 0 where none or where the
        engine gives migkit no way to."""
        return 0

    def wider(self, db):
        """[(line, whether it is a way in a per-database freeze leaves)]:
        what a revoke on this database does not reach."""
        return []

    def more(self, db, who, entry, save):
        """Anything else that keeps `who` from writing here where its
        grants do not - recorded in `entry` (`save()`) before it is done;
        what it did, in words, or ""."""
        return ""

    def less(self, db, who, entry):
        """What `more` did, undone from `entry`."""

    # a source's freeze
    def clock(self, db):
        """The server's time now, as `left` compares a session's start
        with it; None where the engine proves by its open work alone."""
        return None

    def left(self, db, who, at):
        """(held, what) of the sessions of the accounts `who` opened before
        `at`, or None where the engine proves by its open work alone."""
        return None

    def stop(self, db):
        """What other sessions hold open here, ended where the engine ends
        it apart from a session; what was ended, in words, or ""."""
        return ""

    def may_end(self, db):
        return False

    def may_see(self, db):
        return False

    def holders(self, db):
        """The accounts held, or that a hold would take."""
        rec = state(self.hop, db, self.side) or {}
        return sorted(set(rec.get("roles") or {}) | set(self.accounts(db)))

    def hold(self, dbs, way, say):
        src = self.side == "src"
        for d in dbs:
            record = state(self.hop, d, self.side) or {
                "db": self.name(d), "way": way, "roles": {}}
            if src and "at" not in record:
                # the server's clock, not this machine's: the proof asks
                # the server which sessions it opened before this
                record["at"] = self.clock(d)
            for who in self.accounts(d):
                entry = record["roles"].setdefault(who, {"revoked": []})
                todo = [g for g in self.grants(d, who)
                        if g not in entry["revoked"]]
                entry["revoked"] += todo
                # recorded before it is done: a run that stops half way
                # leaves a record `thaw` can finish from
                _save(self.hop, d, record, self.side)
                for g in todo:
                    self.run(d, *self.take(d, who, g))
                extra = self.more(d, who, entry,
                                  lambda: _save(self.hop, d, record,
                                                self.side))
                # on a source every session goes, whatever was revoked
                ended = self.end(d, who) if todo or extra or src else 0
                how = ([f"{len(todo)} write grants revoked"] if todo
                       else []) + ([extra] if extra else []) + (
                    [f"{ended} open sessions ended"] if ended else [])
                say(f"{record['db']}: {who} - " + ("; ".join(how) if how
                                                   else "nothing to take"))
            _save(self.hop, d, record, self.side)
            stopped = self.stop(d) if src else ""
            if stopped:
                say(f"{record['db']}: {stopped}")

    def keep(self, db, way, say):
        self.hold([db], way, say)
        for line in self.exempt(db, way):
            say(f"{db}: still able to write here: {line}")

    def release(self, db, record, say):
        name = record.get("db") or self.name(db)
        for who, entry in (record.get("roles") or {}).items():
            self.less(db, who, entry)
            for g in entry.get("revoked") or []:
                self.run(db, *self.give(db, who, g))
            say(f"{name}: {who} - writes given back"
                f" ({len(entry.get('revoked') or [])} grants)")

    def kept(self, db, record):
        out = []
        # the accounts held, and any that has come to write here since
        for who in sorted(set(record.get("roles") or {})
                          | set(self.accounts(db))):
            left = self.still(db, who)
            out.append((not left, f"{who}: " + (
                f"can still write {', '.join(left[:3])}"
                + (" ..." if len(left) > 3 else "") if left
                else "writes nothing here")))
        return out or [(True, "no account of the application's writes"
                              " here")]

    def held(self, db, way):
        record = state(self.hop, db, self.side) or {"roles": {}}
        out = self.kept(db, record)
        got = self.left(db, sorted(record.get("roles") or {}),
                        record.get("at"))
        if got is not None:
            out.append(got)
        work = self.open(db, None)
        out.append((not work, "; ".join(work[:3]) if work else
                    "nothing held open by another session"))
        return out

    def exempt(self, db, way):
        src = self.side == "src"
        return [line + (f" - {LEAVES}" if src and leaves else "")
                for line, leaves in self.wider(db)]

    def named_only(self, got):
        """`got` narrowed to the hop's `app_roles`, where it names them."""
        named = _named(self.hop)
        return [a for a in got if a in named] if named else list(got)


# PostgreSQL's wire: CockroachDB and Aurora DSQL keep `information_schema`'s
# table privileges and take PostgreSQL's GRANT and REVOKE. An owner of a
# table writes it whatever is revoked, and so does a role of the server's
# own (CockroachDB's admin): named. CockroachDB ends a session by its id
# and lists every open transaction to its admin, so it freezes a source
# too; DSQL lets no session end another's or see its transactions, and
# keeps a target only.

class PgWireHold(_Grants):
    WAY = "pgw_grants"
    SAID = ("the application's roles' write grants revoked, their"
            " sessions ended where the server lets migkit")
    SOURCE = True
    SYSTEM = ("pg_catalog", "information_schema", "crdb_internal",
              "pg_extension", "sys")

    def mine(self, db):
        return str(self.rows(db, "select current_user")[0][0])

    def privileges(self, db):
        from .engines.dbapi import MARK
        marks = ", ".join([MARK] * len(self.SYSTEM))
        return self.rows(
            db, "select grantee, table_schema, table_name, privilege_type,"
                " is_grantable from information_schema.table_privileges"
                " where table_catalog = current_database() and table_schema"
                f" not in ({marks}) and privilege_type in ('INSERT',"
                " 'UPDATE', 'DELETE', 'ALL') order by 1, 2, 3, 4",
            self.SYSTEM)

    def owners(self, db):
        from .engines.dbapi import MARK
        marks = ", ".join([MARK] * len(self.SYSTEM))
        return {(s, t): o for s, t, o in self.rows(
            db, "select schemaname, tablename, tableowner from pg_tables"
                f" where schemaname not in ({marks})", self.SYSTEM)}

    def servers(self, db):
        """The roles of the server's own that write everything here: its
        superusers, and CockroachDB's admin and root."""
        own = {"admin", "root"} if self.kind == "cockroachdb" else set()
        try:
            return own | {str(r[0]) for r in self.rows(
                db, "select rolname from pg_roles where rolsuper")}
        except Exception:  # noqa: BLE001 - a catalogue without the column
            return own

    def member(self, db, me, role):
        from .engines.dbapi import MARK
        try:
            got = self.rows(db, f"select pg_has_role({MARK}, {MARK},"
                                " 'member')", (me, role))
            return bool(got and got[0][0])
        except Exception:  # noqa: BLE001 - asked as it can be
            return False

    def accounts(self, db):
        me = self.mine(db)
        owners = self.owners(db)
        skip = {me, "PUBLIC", "public"} | self.servers(db)
        got = sorted({g for g, s, t, _, _ in self.privileges(db)
                      if g not in skip and owners.get((s, t)) != g
                      and not self.member(db, me, g)})
        return self.named_only(got)

    def grants(self, db, who):
        return [[s, t, p, str(gr).upper() == "YES"]
                for g, s, t, p, gr in self.privileges(db) if g == who]

    def take(self, db, who, grant):
        s, t, p, _ = grant
        return [f"revoke {p.lower()} on table {_q(s)}.{_q(t)} from {_q(who)}"]

    def give(self, db, who, grant):
        s, t, p, opt = grant
        return [f"grant {p.lower()} on table {_q(s)}.{_q(t)} to {_q(who)}"
                + (" with grant option" if opt else "")]

    def said(self, grant):
        return f"{grant[0]}.{grant[1]} ({grant[2].lower()})"

    def end(self, db, who):
        if self.kind != "cockroachdb":
            return 0
        from .engines.dbapi import MARK
        ids = [r[0] for r in self.rows(
            db, "select session_id from crdb_internal.cluster_sessions"
                f" where user_name = {MARK} and session_id <> (select"
                " session_id from [show session_id])", (who,))]
        n = 0
        for i in ids:
            try:
                self.run(db, f"cancel session {_lit(i)}")
                n += 1
            except Exception:  # noqa: BLE001 - it ended on its own
                pass
        return n

    def wider(self, db):
        me = self.mine(db)
        owners = self.owners(db)
        servers = self.servers(db)
        out = [(f"{o} owns {s}.{t}: an owner writes whatever is revoked",
                True) for (s, t), o in sorted(owners.items())
               if o != me and o not in servers][:5]
        out += [(f"{r} is the server's own: nothing short of stopping it"
                 " keeps it from writing", False)
                for r in sorted(servers) if r != me]
        privs = self.privileges(db)
        if any(g in ("PUBLIC", "public") for g, *_ in privs):
            out.append(("PUBLIC may write tables here, and migkit's own role"
                        " is one of PUBLIC: named, not taken", True))
        out += [(f"{g} writes here, and migkit's own role is a member of it:"
                 " it is not taken", True)
                for g in sorted({g for g, *_ in privs})
                if g not in servers | {me, "PUBLIC", "public"}
                and self.member(db, me, g)]
        return out

    # a source's freeze, CockroachDB's: every session of the accounts held
    # cancelled, and every open transaction of theirs, and a prepared one,
    # read back from the cluster's own tables, which show its admin all
    def admin(self, db):
        got = self.rows(db, "select pg_has_role(current_user, 'admin',"
                            " 'member')")
        return bool(got and got[0][0])

    def may_end(self, db):
        return self.kind == "cockroachdb" and self.admin(db)

    def may_see(self, db):
        return self.kind == "cockroachdb" and self.admin(db)

    def clock(self, db):
        return str(self.rows(db, "select now()::text")[0][0])

    def left(self, db, who, at):
        from .engines.dbapi import MARK
        if not who:
            return (True, "no session of the application's to end")
        n = int(self.rows(
            db, "select count(*) from crdb_internal.cluster_sessions where"
                f" user_name in ({', '.join([MARK] * len(who))}) and"
                f" session_start < {MARK}::timestamptz",
            (*who, at))[0][0])
        return (n == 0, f"{n} sessions of the application's roles left from"
                        " before the freeze")

    def open(self, db, older_than):
        from .engines.dbapi import MARK
        who = self.holders(db)
        out = []
        try:
            out += [f"prepared transaction {g}" for (g,) in self.rows(
                db, "select gid from pg_prepared_xacts")]
        except Exception:  # noqa: BLE001 - a version that prepares none
            pass
        if not who:
            return out
        age = "" if older_than is None else \
            f" and t.start < now() - interval '{float(older_than)} seconds'"
        got = self.rows(
            db, "select s.user_name, t.id, extract(epoch from now() -"
                " t.start)::int from crdb_internal.cluster_transactions t"
                " join crdb_internal.cluster_sessions s on s.session_id ="
                f" t.session_id where s.user_name in"
                f" ({', '.join([MARK] * len(who))})" + age, tuple(who))
        return out + [f"transaction {u} {i}, open {s}s" for u, i, s in got]


# ClickHouse
# Each of the application's users and the roles they hold but migkit's own
# account does not loses the right to insert, update, delete and truncate
# in the database: one revoke on `db.*`, which takes the database's and its
# tables' grants and, against a grant on every database, leaves a partial
# revoke - recorded first, given back exactly, a pre-existing partial
# revoke left as it was. A user still able to write after that - through a
# role migkit's own holds - is made read-only (`readonly`, which it cannot
# lift itself), for every database: said. Its running queries are killed.
# A user kept in the server's configuration files is not changed by an
# access statement: named. On a source, what is held open is the writes
# running and the mutations not yet done - a mutation applies after the
# statement that asked for it has returned, so one queued before the
# freeze would change rows after it: read back, never killed (killing one
# loses the change it was asked for).

_CH_WRITE = ("INSERT", "ALTER UPDATE", "ALTER DELETE", "TRUNCATE")
#: the access types that cover each write, its own among them
_CH_COVER = {"INSERT": ("INSERT", "ALL"),
             "ALTER UPDATE": ("ALTER UPDATE", "ALTER TABLE", "ALTER", "ALL"),
             "ALTER DELETE": ("ALTER DELETE", "ALTER TABLE", "ALTER", "ALL"),
             "TRUNCATE": ("TRUNCATE", "ALL")}
#: where a user is kept that an access statement may change
_CH_CHANGEABLE = ("local_directory", "replicated", "memory")
#: the kinds of query that write
_CH_WRITING = ("Insert", "Alter", "Delete")


def _ch_lit(text):
    return "'" + str(text).replace("\\", "\\\\").replace("'", "\\'") + "'"


class ClickHouseHold(_Grants):
    WAY = "ch_grants"
    SAID = ("the application's users' and roles' writes in the database"
            " revoked, a user still writing through a role of migkit's made"
            " read-only, its queries killed")
    SOURCE = True
    ENDS = "their running queries killed"

    def q(self, name):
        return self.eng._q(name)

    def rows(self, db, sql, args=()):
        return self.eng._rows(self.side, None, sql, dict(args or {}))

    def run(self, db, *stmts):
        client = self.eng._client(self.side)
        try:
            for s in stmts:
                client.command(s)
        finally:
            client.close()

    def mine(self):
        return self.me or "default"

    def held_roles(self, who):
        """Every role `who` holds, through another role too."""
        edges = self.rows(None, "select user_name, role_name,"
                                " granted_role_name from system.role_grants")
        out, todo = set(), {who}
        while todo:
            n = todo.pop()
            for u, r, g in edges:
                if n in (u, r) and g not in out:
                    out.add(g)
                    todo.add(g)
        return out

    def users(self):
        return {str(n): str(s) for n, s in self.rows(
            None, "select name, storage from system.users")}

    def accounts(self, db):
        users = self.users()
        me = self.mine()
        ours = self.held_roles(me)
        app = [u for u, where in users.items()
               if u != me and where in _CH_CHANGEABLE]
        app = self.named_only(app)
        roles = sorted({r for u in app for r in self.held_roles(u)} - ours)
        # the roles first: a user that writes through one is held by its
        # revoke, and made read-only only where a role of migkit's own
        # leaves it writing
        return roles + sorted(app)

    def grants(self, db, who):
        types = sorted({t for c in _CH_COVER.values() for t in c})
        got = self.rows(
            None, "select access_type, database, table, column,"
                  " grant_option, is_partial_revoke from system.grants where"
                  " (user_name = {w:String} or role_name = {w:String}) and"
                  " access_type in {t:Array(String)} and (database ="
                  " {d:String} or database is null)",
            {"w": who, "t": types, "d": self.name(db)})
        partial = sorted({str(a) for a, d, t, c, o, p in got
                          if p and d == self.name(db) and t is None})
        return [[str(a), d, t, c, bool(o), partial]
                for a, d, t, c, o, p in got if not p]

    def covered(self, access):
        return [w for w in _CH_WRITE if access in _CH_COVER[w]]

    def scope(self, db, grant):
        _, d, t, c, _, _ = grant
        where = f"{self.q(self.name(db))}." + (self.q(t) if t else "*")
        return where, f"({self.q(c)})" if c else ""

    def take(self, db, who, grant):
        where, col = self.scope(db, grant)
        writes = self.covered(grant[0])
        return [f"revoke {', '.join(w + col for w in writes)} on {where}"
                f" from {self.q(who)}"] if writes else []

    def give(self, db, who, grant):
        access, d, t, c, opt, partial = grant
        where, col = self.scope(db, grant)
        if d is None:
            # a grant on every database: the partial revoke this database
            # was given is taken off, one there before left as it was
            writes = [w for w in self.covered(access)
                      if not any(p in _CH_COVER[w] for p in partial)]
            return [f"grant {', '.join(writes)} on {where} to"
                    f" {self.q(who)}" + (" with grant option" if opt
                                         else "")] if writes else []
        return [f"grant {access}{col} on {where} to {self.q(who)}"
                + (" with grant option" if opt else "")]

    def still(self, db, who):
        if who in self.users() and self.read_only(who):
            return []
        name = self.name(db)
        holders = [who] + sorted(self.held_roles(who))
        types = sorted({t for c in _CH_COVER.values() for t in c})
        got = self.rows(
            None, "select user_name, role_name, access_type, database,"
                  " table, is_partial_revoke from system.grants where"
                  " (user_name in {h:Array(String)} or role_name in"
                  " {h:Array(String)}) and access_type in {t:Array(String)}"
                  " and (database = {d:String} or database is null)",
            {"h": holders, "t": types, "d": name})
        left = []
        for w in _CH_WRITE:
            for h in holders:
                mine = [r for r in got if h in (r[0], r[1])
                        and r[2] in _CH_COVER[w]]
                here = [r for r in mine if not r[5] and r[3] == name]
                off = [r for r in mine if r[5] and r[3] == name
                       and r[4] is None]
                every = [r for r in mine if not r[5] and r[3] is None]
                if here or (every and not off):
                    left.append(f"{w.lower()} ({h})")
                    break
        return left

    def read_only(self, who):
        got = self.rows(None, "select value from"
                              " system.settings_profile_elements where"
                              " user_name = {w:String} and setting_name ="
                              " 'readonly'", {"w": who})
        return bool(got) and str(got[0][0]) not in ("0", "", "None")

    def more(self, db, who, entry, save):
        if who not in self.users() or not self.still(db, who):
            return ""
        if "readonly" not in entry:
            got = self.rows(None, "select value from"
                                  " system.settings_profile_elements where"
                                  " user_name = {w:String} and setting_name"
                                  " = 'readonly'", {"w": who})
            entry["readonly"] = str(got[0][0]) if got else None
            save()
        self.run(db, f"alter user {self.q(who)} "
                     + ("modify" if entry["readonly"] is not None else "add")
                     + " settings readonly = 1")
        return ("still able to write through a role migkit's account holds"
                " too, so it is read-only - on every database")

    def less(self, db, who, entry):
        if "readonly" not in entry:
            return
        was = entry["readonly"]
        self.run(db, f"alter user {self.q(who)} "
                     + (f"modify settings readonly = {int(was)}"
                        if was is not None else "drop settings readonly"))

    def end(self, db, who):
        if who not in self.users():
            return 0
        got = self.rows(None, "select count() from system.processes where"
                              " user = {w:String}", {"w": who})
        self.run(db, f"kill query where user = {_ch_lit(who)} sync")
        return int(got[0][0]) if got else 0

    def said(self, grant):
        return f"{grant[0].lower()} on {grant[2] or '*'}"

    def wider(self, db):
        me = self.mine()
        out = [(f"{u} is kept in the server's configuration files, which an"
                " access statement does not change", True)
               for u, where in sorted(self.users().items())
               if u != me and where not in _CH_CHANGEABLE
               and self.still(db, u)]
        for r in sorted(self.held_roles(me)):
            if self.still(db, r):
                out.append((f"the role {r} writes here and migkit's own"
                            " account holds it: it is not taken", True))
        return out

    def may_end(self, db):
        got = self.rows(None, "select count() from system.grants where"
                              " (user_name = currentUser() or role_name in"
                              " (select role_name from system.current_roles))"
                              " and access_type in ('KILL QUERY', 'ALL') and"
                              " is_partial_revoke = 0")
        return bool(got) and int(got[0][0]) > 0

    def may_see(self, db):
        return _tried(lambda: self.rows(None, "select count() from"
                                              " system.mutations"))

    def left(self, db, who, at):
        if not who:
            return (True, "no session of the application's to end")
        got = self.rows(None, "select count() from system.processes where"
                              " user in {w:Array(String)} and query_kind in"
                              " {k:Array(String)}",
                        {"w": who, "k": list(_CH_WRITING)})
        n = int(got[0][0]) if got else 0
        return (n == 0, f"{n} writes of the application's users running")

    def open(self, db, older_than):
        name = self.name(db)
        out = [f"mutation {m} on {t}, not done: {str(c)[:60]}"
               for t, m, c in self.rows(
                   None, "select table, mutation_id, command from"
                         " system.mutations where database = {d:String} and"
                         " not is_done", {"d": name})]
        age = "" if older_than is None else \
            f" and elapsed > {float(older_than)}"
        out += [f"write {q} of {u}, running {e:.0f}s"
                for u, q, e in self.rows(
                    None, "select user, query_id, elapsed from"
                          " system.processes where user <> currentUser() and"
                          " query_kind in {k:Array(String)}" + age,
                    {"k": list(_CH_WRITING)})]
        return out


# Oracle
# A schema's tables are written by their owner, and by every account a
# grant names: on a table or a view, and on a procedure, function or
# package of the owner's that runs with its rights (definer's rights) -
# what a revoke on the tables alone leaves. The grants the owner made are
# revoked, recorded first, granted back exactly; a grant its grantee
# passed on to others is left, and named - revoking it would take theirs
# too. The owner itself, where it is not migkit's own account, is locked
# and its sessions killed: nothing revoked keeps an owner from its own
# tables, and a lock reaches only sessions that start after it. A system
# privilege (INSERT ANY TABLE and the like) reaches every schema: named.
# On a source, every session of the accounts held is killed, which rolls
# back what each held open, and the transactions still open
# (`v$transaction`) and in doubt (`dba_2pc_pending`) are read back.

class OracleHold(_Grants):
    WAY = "ora_grants"
    SAID = ("the grants on the schema's tables and on the owner's code"
            " that runs with its rights revoked from the application's"
            " accounts, the schema's owner locked, their sessions killed")
    SOURCE = True

    def owner(self, db):
        return self.eng._owner(self.side, db)

    def mine(self, db):
        return str(self.rows(db, "select user from dual")[0][0])

    def ours(self, db):
        """Roles migkit's own session holds, through others too."""
        return {str(r[0]) for r in self.rows(db, "select role from"
                                                 " session_roles")}

    def privileges(self, db):
        """[(grantee, object, privilege, grantable, grantor)] of the
        writes into the schema."""
        from .engines.dbapi import MARK
        return self.rows(
            db, "select grantee, table_name, privilege, grantable, grantor"
                f" from all_tab_privs where table_schema = {MARK} and"
                " ((privilege in ('INSERT', 'UPDATE', 'DELETE') and type in"
                " ('TABLE', 'VIEW')) or (privilege = 'EXECUTE' and type in"
                " ('PROCEDURE', 'FUNCTION', 'PACKAGE') and table_name in"
                " (select object_name from all_procedures where owner ="
                f" {MARK} and authid = 'DEFINER'))) order by 1, 2, 3",
            (self.owner(db), self.owner(db)))

    def maintained(self, db):
        try:
            return {str(r[0]) for r in self.rows(
                db, "select username from all_users where oracle_maintained"
                    " = 'Y'")}
        except Exception:  # noqa: BLE001 - before 12c: by name
            return {"SYS", "SYSTEM"}

    def passed_on(self, db, who):
        return [r for r in self.privileges(db) if r[4] == who]

    def accounts(self, db):
        owner, me = self.owner(db), self.mine(db)
        skip = {me, "PUBLIC"} | self.ours(db) | self.maintained(db)
        got = sorted({str(g) for g, t, p, gr, by in self.privileges(db)
                      if g not in skip and by == owner
                      and not self.passed_on(db, g)})
        if owner != me and owner not in skip:
            got = sorted(set(got) | {owner})
        return self.named_only(got)

    def grants(self, db, who):
        owner = self.owner(db)
        return [[t, p, gr == "YES"] for g, t, p, gr, by
                in self.privileges(db) if g == who and by == owner]

    def take(self, db, who, grant):
        t, p, _ = grant
        return [f"revoke {p.lower()} on {_q(self.owner(db))}.{_q(t)} from"
                f" {_q(who)}"]

    def give(self, db, who, grant):
        t, p, opt = grant
        return [f"grant {p.lower()} on {_q(self.owner(db))}.{_q(t)} to"
                f" {_q(who)}" + (" with grant option" if opt else "")]

    def said(self, grant):
        return f"{grant[0]} ({grant[1].lower()})"

    def status(self, db, who):
        """The account's state as the server keeps it (OPEN, LOCKED, ...),
        or None where migkit's account may not read it."""
        from .engines.dbapi import MARK
        try:
            got = self.rows(db, "select account_status from dba_users where"
                                f" username = {MARK}", (who,))
        except Exception:  # noqa: BLE001 - not readable by migkit's account
            return None
        return str(got[0][0]) if got else None

    def more(self, db, who, entry, save):
        if who != self.owner(db):
            return ""
        if "status" not in entry:
            was = self.status(db, who)
            if was is None:
                # never locked where what to give back is not known
                return ("the schema's owner: its account's state could not"
                        " be read, so it is not locked - as the owner it"
                        " still writes")
            entry["status"] = was
            save()
        try:
            self.run(db, f"alter user {_q(who)} account lock")
        except Exception as e:  # noqa: BLE001 - said, and read back
            return ("the schema's owner: it could not be locked ("
                    + (str(e).strip().splitlines() or [""])[0][:120]
                    + ") - as the owner it still writes")
        return "the schema's owner, locked: nothing revoked keeps an owner" \
               " from its own tables"

    def less(self, db, who, entry):
        if "status" in entry and "LOCKED" not in str(entry["status"] or ""):
            self.run(db, f"alter user {_q(who)} account unlock")

    def end(self, db, who):
        from .engines.dbapi import MARK
        try:
            got = self.rows(db, "select sid, serial# from v$session where"
                                f" username = {MARK}", (who,))
        except Exception:  # noqa: BLE001 - not readable by migkit's account
            return 0
        n = 0
        for sid, serial in got:
            try:
                self.run(db, f"alter system kill session '{int(sid)},"
                             f"{int(serial)}' immediate")
                n += 1
            except Exception:  # noqa: BLE001 - it ended on its own
                pass
        return n

    def still(self, db, who):
        if who == self.owner(db):
            from .engines.dbapi import MARK
            status = self.status(db, who)
            if "LOCKED" not in str(status or ""):
                return ["its own tables, as their owner (not locked)"]
            try:
                live = self.rows(db, "select count(*) from v$session where"
                                     f" username = {MARK}", (who,))
            except Exception:  # noqa: BLE001 - not readable: not "none"
                return ["through any session it held open: they could not"
                        " be read"]
            n = int(live[0][0]) if live else 0
            return [f"through {n} sessions open from before"] if n else []
        return [self.said(g) for g in self.grants(db, who)]

    def wider(self, db):
        from .engines.dbapi import MARK
        owner, me = self.owner(db), self.mine(db)
        out = []
        if owner == me:
            out.append((f"{owner} owns the schema's tables and is the account"
                        " migkit writes with: an application signing in as"
                        " it is not held", True))
        privs = self.privileges(db)
        if any(g == "PUBLIC" for g, *_ in privs):
            out.append(("PUBLIC may write the schema, and migkit's own account"
                        " is one of PUBLIC: named, not taken", True))
        passed = sorted({by for g, t, p, gr, by in privs if by != owner})
        out += [(f"{who} granted its rights here on to others: revoking them"
                 " would take theirs too, so they are left", True)
                for who in passed]
        ours = self.ours(db)
        out += [(f"the role {g} writes here and migkit's own session holds"
                 " it: it is not taken", True)
                for g in sorted({g for g, *_ in privs} & ours)]
        try:
            anys = sorted({str(r[0]) for r in self.rows(
                db, "select grantee from dba_sys_privs where privilege in"
                    " ('INSERT ANY TABLE', 'UPDATE ANY TABLE', 'DELETE ANY"
                    f" TABLE') and grantee <> {MARK}", (me,))})
        except Exception:  # noqa: BLE001 - not readable by migkit's account
            anys = []
        out += [(f"{g} writes every schema's tables (a system privilege)",
                 False) for g in anys if g not in self.maintained(db)]
        return out

    def may_end(self, db):
        got = self.rows(db, "select count(*) from session_privs where"
                            " privilege = 'ALTER SYSTEM'")
        return bool(got) and int(got[0][0]) > 0

    def may_see(self, db):
        return all(_tried(lambda q=q: self.rows(db, q)) for q in (
            "select count(*) from v$session",
            "select count(*) from v$transaction",
            "select count(*) from dba_2pc_pending"))

    def clock(self, db):
        return str(self.rows(db, "select to_char(sysdate, 'YYYY-MM-DD"
                                 " HH24:MI:SS') from dual")[0][0])

    def left(self, db, who, at):
        from .engines.dbapi import MARK
        if not who:
            return (True, "no session of the application's to end")
        n = int(self.rows(
            db, "select count(*) from v$session where username in"
                f" ({', '.join([MARK] * len(who))}) and logon_time <"
                f" to_date({MARK}, 'YYYY-MM-DD HH24:MI:SS')",
            (*who, at))[0][0])
        return (n == 0, f"{n} sessions of the application's accounts left"
                        " from before the freeze")

    def open(self, db, older_than):
        from .engines.dbapi import MARK
        out = [f"prepared transaction {t} ({s})" for t, s in self.rows(
            db, "select local_tran_id, state from dba_2pc_pending")]
        age = "" if older_than is None else \
            f" and t.start_date < sysdate - {float(older_than)} / 86400"
        got = self.rows(
            db, "select s.username, s.sid, round((sysdate - t.start_date) *"
                " 86400) from v$transaction t join v$session s on s.taddr ="
                f" t.addr where s.username <> {MARK}" + age,
            (self.mine(db),))
        return out + [f"write transaction {u} session {i}, open {s}s"
                      for u, i, s in got]


# Db2 for Linux, UNIX and Windows; for z/OS and for i below, with their own
# catalogues. Table privileges revoked from the grantee - a user, a group
# or a role - and granted back with the option they had; CONTROL, which
# holds them all, too. On Linux, UNIX and Windows the packages whose
# static SQL touches the schema's tables and the schema's routines are
# taken as well: static SQL runs with its package's owner's rights, which
# a revoke from the user does not reach - EXECUTE on them is. PUBLIC is
# taken only where migkit's own account writes without it (it owns every
# table, or holds DATAACCESS); else named. On a source the application's
# connections are forced off, which rolls back their units of work, and
# the units of work still open that wrote are read back.

class Db2Hold(_Grants):
    WAY = "db2_grants"
    SAID = ("the application's table privileges revoked, CONTROL among"
            " them, and EXECUTE on the packages and routines that write the"
            " schema, their connections forced off where migkit's account"
            " may")
    SOURCE = True
    PRIVS = (("CONTROL", "controlauth"), ("INSERT", "insertauth"),
             ("UPDATE", "updateauth"), ("DELETE", "deleteauth"))
    KINDS = {"U": "user", "G": "group", "R": "role"}
    ROUTINES = {"P": "procedure", "F": "function"}

    def schema(self, db):
        return self.eng._owner(self.side, db)

    def mine(self, db):
        return str(self.rows(db, "select current user from"
                                 " sysibm.sysdummy1")[0][0]).strip()

    def ours(self, db):
        from .engines.dbapi import MARK
        try:
            return {str(r[0]).strip() for r in self.rows(
                db, "select rolename from syscat.roleauth where grantee ="
                    f" {MARK}", (self.mine(db),))}
        except Exception:  # noqa: BLE001 - a catalogue without roles
            return set()

    def privileges(self, db):
        """[(grantee, its kind, object kind, object, privilege, Y or G,
        a routine's kind)]."""
        from .engines.dbapi import MARK
        schema = self.schema(db)
        cols = ", ".join(c for _, c in self.PRIVS)
        out = []
        for g, kind, t, *flags in self.rows(
                db, f"select grantee, granteetype, tabname, {cols} from"
                    f" syscat.tabauth where tabschema = {MARK}", (schema,)):
            out += [(str(g).strip(), str(kind).strip(), "TABLE",
                     str(t).strip(), p, str(f).strip(), "")
                    for (p, _), f in zip(self.PRIVS, flags)
                    if str(f).strip() in ("Y", "G")]
        for g, kind, ps, pn, f in self.rows(
                db, "select a.grantee, a.granteetype, a.pkgschema,"
                    " a.pkgname, a.executeauth from syscat.packageauth a"
                    " where a.executeauth in ('Y', 'G') and exists (select 1"
                    " from syscat.packagedep d where d.pkgschema ="
                    " a.pkgschema and d.pkgname = a.pkgname and d.btype in"
                    f" ('T', 'V') and d.bschema = {MARK})", (schema,)):
            out.append((str(g).strip(), str(kind).strip(), "PACKAGE",
                        f"{str(ps).strip()}.{str(pn).strip()}", "EXECUTE",
                        str(f).strip(), ""))
        for g, kind, rn, f, rt in self.rows(
                db, "select a.grantee, a.granteetype, a.specificname,"
                    " a.executeauth, a.routinetype from syscat.routineauth a"
                    f" where a.schema = {MARK} and a.specificname is not null"
                    " and a.executeauth in ('Y', 'G') and a.routinetype in"
                    " ('P', 'F')", (schema,)):
            out.append((str(g).strip(), str(kind).strip(), "ROUTINE",
                        str(rn).strip(), "EXECUTE", str(f).strip(),
                        str(rt).strip()))
        return out

    def public_taken(self, db):
        """Whether migkit's own account writes every table here without
        PUBLIC: it owns them all, or holds DATAACCESS."""
        from .engines.dbapi import MARK
        me = self.mine(db)
        got = self.rows(db, "select count(*) from syscat.tables where"
                            f" tabschema = {MARK} and type = 'T' and owner"
                            f" <> {MARK}", (self.schema(db), me))
        if got and int(got[0][0]) == 0:
            return True
        got = self.rows(db, "select count(*) from syscat.dbauth where"
                            f" grantee = {MARK} and dataaccessauth = 'Y'",
                        (me,))
        return bool(got and int(got[0][0]))

    def accounts(self, db):
        skip = {self.mine(db)} | self.ours(db)
        if not self.public_taken(db):
            skip.add("PUBLIC")
        return self.named_only(sorted({p[0] for p in self.privileges(db)
                                       if p[0] not in skip}))

    def grants(self, db, who):
        # one privilege granted by several grantors is one row each, and
        # one revoke takes them all
        got = {}
        for g, kind, ok, name, p, f, rt in self.privileges(db):
            if g == who:
                key = (ok, name, kind, p, rt)
                got[key] = got.get(key, False) or f == "G"
        return [[ok, name, kind, p, opt, rt]
                for (ok, name, kind, p, rt), opt in sorted(got.items())]

    def target(self, kind, who):
        if who == "PUBLIC":
            return "public"
        return f"{self.KINDS.get(kind, 'user')} {_q(who)}"

    def on(self, db, grant):
        ok, name, _, _, _, rt = grant
        if ok == "TABLE":
            return f"table {_q(self.schema(db))}.{_q(name)}"
        sch, _, n = name.partition(".")
        if ok == "PACKAGE":
            return f"package {_q(sch)}.{_q(n)}"
        return (f"specific {self.ROUTINES.get(rt, 'procedure')}"
                f" {_q(self.schema(db))}.{_q(name)}")

    def take(self, db, who, grant):
        return [f"revoke {grant[3].lower()} on {self.on(db, grant)} from"
                f" {self.target(grant[2], who)}"]

    def give(self, db, who, grant):
        p, opt = grant[3], grant[4]
        return [f"grant {p.lower()} on {self.on(db, grant)} to"
                f" {self.target(grant[2], who)}"
                + (" with grant option" if opt and p != "CONTROL" else "")]

    def said(self, grant):
        return f"{grant[1]} ({grant[3].lower()})"

    def end(self, db, who):
        from .engines.dbapi import MARK
        try:
            handles = [int(r[0]) for r in self.rows(
                db, "select application_handle from"
                    " table(mon_get_connection(null, -2)) where"
                    f" session_auth_id = {MARK} and application_handle <>"
                    " mon_get_application_handle()", (who,))]
            if handles:
                self.run(db, "call sysproc.admin_cmd('force application ("
                             + ", ".join(map(str, handles)) + ")')")
            return len(handles)
        except Exception:  # noqa: BLE001 - migkit's account may not
            return 0

    def wider(self, db):
        from .engines.dbapi import MARK
        me = self.mine(db)
        out = []
        privs = self.privileges(db)
        if "PUBLIC" not in self.accounts(db) and any(
                p[0] == "PUBLIC" for p in privs):
            out.append(("PUBLIC may write the schema, and migkit's own"
                        " account writes through it: named, not taken",
                        True))
        out += [(f"the role {r} writes here and migkit's own account holds"
                 " it: it is not taken", True)
                for r in sorted({p[0] for p in privs} & self.ours(db))]
        try:
            out += [(f"{str(g).strip()} holds DATAACCESS or DBADM: it writes"
                     " every table", True) for (g,) in self.rows(
                        db, "select grantee from syscat.dbauth where"
                            " (dataaccessauth = 'Y' or dbadmauth = 'Y') and"
                            f" grantee <> {MARK}", (me,))]
        except Exception:  # noqa: BLE001 - not readable by migkit's account
            pass
        if self.side == "src":
            out.append(("a transaction an XA manager left in doubt is not"
                        " read here: the database's manager lists those"
                        " (LIST INDOUBT TRANSACTIONS)", False))
        return out

    def may_end(self, db):
        """FORCE APPLICATION is SYSADM's, SYSCTRL's or SYSMAINT's."""
        got = self.rows(db, "select count(*) from table(sysproc"
                            ".auth_list_authorities_for_authid(current"
                            " user, 'U')) where authority in ('SYSADM',"
                            " 'SYSCTRL', 'SYSMAINT') and 'Y' in (d_user,"
                            " d_group, d_public, role_user, role_group,"
                            " role_public)")
        return bool(got) and int(got[0][0]) > 0

    def may_see(self, db):
        return _tried(lambda: self.rows(
            db, "select count(*) from table(mon_get_unit_of_work(null, -2))"))

    def clock(self, db):
        return str(self.rows(db, "select varchar(current timestamp) from"
                                 " sysibm.sysdummy1")[0][0]).strip()

    def left(self, db, who, at):
        from .engines.dbapi import MARK
        if not who:
            return (True, "no connection of the application's to force off")
        n = int(self.rows(
            db, "select count(*) from table(mon_get_connection(null, -2))"
                f" where session_auth_id in ({', '.join([MARK] * len(who))})"
                f" and connection_start_time < timestamp({MARK})",
            (*who, at))[0][0])
        return (n == 0, f"{n} connections of the application's accounts"
                        " left from before the freeze")

    def open(self, db, older_than):
        age = "" if older_than is None else (
            " and uow_start_time < current timestamp -"
            f" {int(older_than)} seconds")
        got = self.rows(
            db, "select session_auth_id, application_handle,"
                " timestampdiff(2, char(current timestamp - uow_start_time))"
                " from table(mon_get_unit_of_work(null, -2)) where"
                " uow_log_space_used > 0 and application_handle <>"
                " mon_get_application_handle()" + age)
        return [f"write transaction {str(u).strip()} application {h},"
                f" open {s}s" for u, h, s in got]


class Db2ZosHold(Db2Hold):
    """Db2 for z/OS: `SYSIBM.SYSTABAUTH`, a grant's revoke by every
    grantor (`BY ALL`), and no CONTROL. Its threads are ended and their
    open work read through the subsystem's operator commands, not SQL: it
    keeps a target, and does not freeze a source."""
    SOURCE = False
    PRIVS = (("INSERT", "insertauth"), ("UPDATE", "updateauth"),
             ("DELETE", "deleteauth"))
    KINDS = {"": "", "L": "role"}

    def mine(self, db):
        return str(self.rows(db, "select user from"
                                 " sysibm.sysdummy1")[0][0]).strip()

    def ours(self, db):
        return set()

    def privileges(self, db):
        from .engines.dbapi import MARK
        cols = ", ".join(c for _, c in self.PRIVS)
        out = []
        for g, kind, t, *flags in self.rows(
                db, f"select grantee, granteetype, ttname, {cols} from"
                    f" sysibm.systabauth where tcreator = {MARK} and"
                    " granteetype in (' ', 'L')", (self.schema(db),)):
            out += [(str(g).strip(), str(kind).strip(), "TABLE",
                     str(t).strip(), p, str(f).strip(), "")
                    for (p, _), f in zip(self.PRIVS, flags)
                    if str(f).strip() in ("Y", "G")]
        return out

    def public_taken(self, db):
        return False

    def target(self, kind, who):
        if who == "PUBLIC":
            return "public"
        return f"role {_q(who)}" if kind == "L" else _q(who)

    def take(self, db, who, grant):
        return [f"revoke {grant[3].lower()} on {self.on(db, grant)} from"
                f" {self.target(grant[2], who)} by all"]

    def end(self, db, who):
        return 0

    def wider(self, db):
        out = []
        if any(p[0] == "PUBLIC" for p in self.privileges(db)):
            out.append(("PUBLIC may write tables here, and migkit's own"
                        " account writes through it: named, not taken",
                        True))
        out.append(("an authority over the whole subsystem (SYSADM,"
                    " DATAACCESS), and static SQL bound by another, write"
                    " whatever is revoked from the application's account",
                    False))
        return out


class Db2iHold(Db2Hold):
    """Db2 for i: `QSYS2.SYSTABAUTH`, its privileges by name, PUBLIC's
    default authority among them. Its jobs are ended and their pending
    changes read through its work management, not SQL: it keeps a target,
    and does not freeze a source."""
    SOURCE = False

    def mine(self, db):
        return str(self.rows(db, "select current user from"
                                 " sysibm.sysdummy1")[0][0]).strip()

    def ours(self, db):
        return set()

    def privileges(self, db):
        from .engines.dbapi import MARK
        return [(str(g).strip(), "", "TABLE", str(t).strip(),
                 str(p).strip(),
                 "G" if str(gr).strip().upper() == "YES" else "Y", "")
                for g, t, p, gr in self.rows(
                    db, "select grantee, table_name, privilege_type,"
                        " is_grantable from qsys2.systabauth where"
                        f" table_schema = {MARK} and privilege_type in"
                        " ('INSERT', 'UPDATE', 'DELETE')",
                    (self.schema(db),))]

    def public_taken(self, db):
        from .engines.dbapi import MARK
        got = self.rows(db, "select count(*) from qsys2.systables where"
                            f" table_schema = {MARK} and table_type in ('T',"
                            f" 'P') and table_owner <> {MARK}",
                        (self.schema(db), self.mine(db)))
        return bool(got) and int(got[0][0]) == 0

    def target(self, kind, who):
        return "public" if who == "PUBLIC" else _q(who)

    def end(self, db, who):
        return 0

    def wider(self, db):
        out = []
        if "PUBLIC" not in self.accounts(db) and any(
                p[0] == "PUBLIC" for p in self.privileges(db)):
            out.append(("PUBLIC may write tables here, and migkit's own"
                        " account writes through it: named, not taken",
                        True))
        out.append(("a user with *ALLOBJ, the tables' owner and a program"
                    " that adopts its owner's authority write whatever is"
                    " revoked", False))
        return out


# SAP ASE
# Object permissions in the database (`sysprotects`: insert 195, delete
# 196, update 197) revoked from the users and groups that hold them, and
# EXECUTE on the procedures of the database, whose statements run with
# their owner's rights through an ownership chain; granted back with their
# grant option. Their sessions in the database are killed where migkit's
# login holds sa_role. A table's owner and the database's owner write
# whatever is revoked: named, by name. On a source, the transactions
# still open in the database (`master..systransactions`) are read back.

class AseHold(_Grants):
    WAY = "ase_grants"
    SAID = ("the application's insert, update, delete and execute"
            " permissions revoked, their sessions in the database killed"
            " where migkit's login may")
    SOURCE = True
    ACTIONS = {195: "insert", 196: "delete", 197: "update", 224: "execute"}

    def run(self, db, *stmts):
        conn = self.eng._connect(self.side, db)
        try:
            # a permission is not changed inside a transaction ASE has to
            # hold open
            conn.autocommit = True
            cur = conn.cursor()
            for s in stmts:
                cur.execute(s)
        finally:
            conn.close()

    def mine(self, db):
        return str(self.rows(db, "select user_name()")[0][0])

    def privileges(self, db):
        return [(str(g), str(owner), str(t), self.ACTIONS[int(a)],
                 int(kind) == 0) for t, owner, g, a, kind in self.rows(
            db, "select o.name, user_name(o.uid), user_name(p.uid),"
                " p.action, p.protecttype from sysprotects p join sysobjects"
                " o on o.id = p.id where ((o.type = 'U' and p.action in"
                " (195, 196, 197)) or (o.type = 'P' and p.action = 224))"
                " and p.protecttype in (0, 1)")
            if g is not None]

    def accounts(self, db):
        skip = {self.mine(db), "dbo", "public"}
        return self.named_only(sorted({g for g, *_ in self.privileges(db)
                                       if g not in skip}))

    def grants(self, db, who):
        return [[owner, t, a, opt] for g, owner, t, a, opt
                in self.privileges(db) if g == who]

    def take(self, db, who, grant):
        owner, t, a, _ = grant
        return [f"revoke {a} on {_q(owner)}.{_q(t)} from {_q(who)}"]

    def give(self, db, who, grant):
        owner, t, a, opt = grant
        return [f"grant {a} on {_q(owner)}.{_q(t)} to {_q(who)}"
                + (" with grant option" if opt else "")]

    def said(self, grant):
        return f"{grant[0]}.{grant[1]} ({grant[2]})"

    def end(self, db, who):
        from .engines.dbapi import MARK
        try:
            spids = [int(r[0]) for r in self.rows(
                db, "select spid from master..sysprocesses where suid ="
                    f" (select suid from sysusers where name = {MARK}) and"
                    " dbid = db_id() and spid <> @@spid", (who,))]
            for s in spids:
                self.run(db, f"kill {s}")
            return len(spids)
        except Exception:  # noqa: BLE001 - migkit's login may not
            return 0

    def wider(self, db):
        me = self.mine(db)
        owners = sorted({str(r[0]) for r in self.rows(
            db, "select distinct user_name(uid) from sysobjects where type"
                " in ('U', 'P')")} - {me, "dbo"})
        out = [(f"{o} owns tables here: an owner writes whatever is"
                " revoked", True) for o in owners]
        if any(g == "public" for g, *_ in self.privileges(db)):
            out.append(("public may write tables here, and migkit's own"
                        " login is one of public: named, not taken", True))
        got = self.rows(db, "select suser_name(suid) from sysusers where"
                            " name = 'dbo'")
        dbo = str(got[0][0]) if got and got[0][0] else ""
        if dbo and dbo != self.me:
            out.append((f"{dbo} owns the database (dbo): it writes whatever"
                        " is revoked", False))
        return out

    def may_end(self, db):
        got = self.rows(db, "select proc_role('sa_role')")
        return bool(got) and int(got[0][0] or 0) > 0

    def may_see(self, db):
        return _tried(lambda: self.rows(
            db, "select count(*) from master..systransactions"))

    def clock(self, db):
        return str(self.rows(db, "select convert(char(8), getdate(), 112) +"
                                 " ' ' + convert(char(8), getdate(),"
                                 " 108)")[0][0])

    def left(self, db, who, at):
        from .engines.dbapi import MARK
        if not who:
            return (True, "no session of the application's to end")
        n = int(self.rows(
            db, "select count(*) from master..sysprocesses where suid in"
                " (select suid from sysusers where name in"
                f" ({', '.join([MARK] * len(who))})) and dbid = db_id() and"
                f" loggedindatetime < convert(datetime, {MARK})",
            (*who, at))[0][0])
        return (n == 0, f"{n} sessions of the application's users left from"
                        " before the freeze")

    def open(self, db, older_than):
        age = "" if older_than is None else \
            f" and datediff(ss, starttime, getdate()) > {int(older_than)}"
        got = self.rows(
            db, "select spid, datediff(ss, starttime, getdate()) from"
                " master..systransactions where dbid = db_id() and spid <>"
                " @@spid" + age)
        return [f"write transaction session {s}, open {n}s" for s, n in got]


# Teradata
# Rights on the database and its tables (`DBC.AllRightsV`: I, U, D)
# revoked from the users that hold them, granted back with their grant
# option. A role's rights reach every user it is granted to, migkit's own
# perhaps, and the database's owner writes it by implicit right: named. On
# a source every session of the users held is aborted (`AbortSessions`,
# logged off), which rolls back what each held open, and none left from
# before the freeze is read back.

class TeradataHold(_Grants):
    WAY = "td_grants"
    SAID = ("the application's insert, update and delete rights on the"
            " database and its tables revoked")
    SOURCE = True
    ENDS = "every session of theirs aborted"
    RIGHTS = {"I": "insert", "U": "update", "D": "delete"}
    SYSTEM = ("DBC", "SYSLIB", "SYSUDTLIB", "SYSSPATIAL", "SYSBAR",
              "TD_SYSFNLIB", "TD_SYSXML", "SYSTEMFE", "SYSADMIN",
              "CRASHDUMPS", "TDPUSER", "PUBLIC", "ALL")

    def mine(self, db):
        return str(self.rows(db, "select user")[0][0]).strip()

    def privileges(self, db):
        from .engines.dbapi import MARK
        return [(str(u).strip(), str(t).strip(),
                 self.RIGHTS[str(r).strip()], str(g).strip() == "Y")
                for u, t, r, g in self.rows(
                    db, "select UserName, TableName, AccessRight,"
                        " GrantAuthority from DBC.AllRightsV where"
                        f" DatabaseName = {MARK} and trim(AccessRight) in"
                        " ('I', 'U', 'D')", (self.name(db),))]

    def accounts(self, db):
        skip = {self.mine(db).upper()} | set(self.SYSTEM)
        return self.named_only(sorted({u for u, *_ in self.privileges(db)
                                       if u.upper() not in skip}))

    def grants(self, db, who):
        return [[t, r, opt] for u, t, r, opt in self.privileges(db)
                if u == who]

    def scope(self, db, t):
        return _q(self.name(db)) + ("" if t.lower() == "all"
                                    else f".{_q(t)}")

    def take(self, db, who, grant):
        t, r, _ = grant
        return [f"revoke {r} on {self.scope(db, t)} from {_q(who)}"]

    def give(self, db, who, grant):
        t, r, opt = grant
        return [f"grant {r} on {self.scope(db, t)} to {_q(who)}"
                + (" with grant option" if opt else "")]

    def said(self, grant):
        return f"{grant[0]} ({grant[1]})"

    def end(self, db, who):
        if self.side != "src":
            return 0
        from .engines.dbapi import MARK
        got = self.rows(db, "select SYSLIB.AbortSessions(-1, "
                            f"{MARK}, 0, 'Y', 'Y')", (who,))
        return int(got[0][0] or 0) if got else 0

    def wider(self, db):
        from .engines.dbapi import MARK
        me = self.mine(db)
        out = []
        try:
            roles = sorted({str(r[0]).strip() for r in self.rows(
                db, "select RoleName from DBC.AllRoleRightsV where"
                    f" DatabaseName = {MARK} and trim(AccessRight) in ('I',"
                    " 'U', 'D')", (self.name(db),))})
        except Exception:  # noqa: BLE001 - not readable by migkit's account
            roles = []
        out += [(f"the role {r} writes here, for every user it is granted"
                 " to: named, not taken", True) for r in roles]
        got = self.rows(db, "select OwnerName from DBC.DatabasesV where"
                            f" DatabaseName = {MARK}", (self.name(db),))
        owner = str(got[0][0]).strip() if got else ""
        if owner and owner.upper() not in {me.upper()} | set(self.SYSTEM):
            out.append((f"{owner} owns the database and writes it by"
                        " implicit right", True))
        return out

    def may_end(self, db):
        got = self.rows(db, "select count(*) from DBC.FunctionsV where"
                            " DatabaseName = 'SYSLIB' and FunctionName ="
                            " 'AbortSessions'")
        return bool(got) and int(got[0][0]) > 0

    def may_see(self, db):
        return _tried(lambda: self.rows(
            db, "select count(*) from DBC.SessionInfoV"))

    def clock(self, db):
        d, t = self.rows(db, "select cast(current_date as char(10)),"
                             " extract(hour from current_time) * 10000 +"
                             " extract(minute from current_time) * 100 +"
                             " extract(second from current_time)")[0]
        return f"{str(d).strip()}|{float(t)}"

    def left(self, db, who, at):
        from .engines.dbapi import MARK
        if not who:
            return (True, "no session of the application's to abort")
        day, _, hhmmss = str(at or "").partition("|")
        marks = ", ".join([MARK] * len(who))
        n = int(self.rows(
            db, "select count(*) from DBC.SessionInfoV where UserName in"
                f" ({marks}) and (LogonDate < cast({MARK} as date) or"
                f" (LogonDate = cast({MARK} as date) and LogonTime <"
                f" {MARK}))", (*who, day, day, float(hhmmss or 0)))[0][0])
        return (n == 0, f"{n} sessions of the application's users left from"
                        " before the freeze")


# Amazon Redshift
# Table privileges in the hop's schema revoked from the users, groups and
# roles that hold them (`svv_relation_privileges`), granted back with
# their option; a user's sessions ended. A table's owner writes whatever
# is revoked, and a superuser writes everything: named. On a source, a
# transaction of another user holding a lock that writes is read back.

class RedshiftHold(_Grants):
    WAY = "rs_grants"
    SAID = ("the application's users', groups' and roles' write privileges"
            " on the schema's tables revoked, their sessions ended")
    SOURCE = True

    def mine(self, db):
        return str(self.rows(db, "select current_user")[0][0])

    def ours(self, db):
        from .engines.dbapi import MARK
        try:
            return {str(r[0]) for r in self.rows(
                db, "select role_name from svv_user_grants where user_name"
                    f" = {MARK}", (self.mine(db),))}
        except Exception:  # noqa: BLE001 - a cluster without roles
            return set()

    def privileges(self, db):
        from .engines.dbapi import MARK
        return [(str(n), str(k), str(t), str(p), str(o).lower() in (
            "t", "true", "1")) for t, p, n, k, o in self.rows(
            db, "select relation_name, privilege_type, identity_name,"
                " identity_type, admin_option from svv_relation_privileges"
                f" where namespace_name = {MARK} and privilege_type in"
                " ('INSERT', 'UPDATE', 'DELETE')", (self.name(db),))]

    def supers(self, db):
        return {str(r[0]) for r in self.rows(
            db, "select usename from pg_user where usesuper")}

    def accounts(self, db):
        skip = {self.mine(db)} | self.ours(db) | self.supers(db)
        return self.named_only(sorted({n for n, k, *_ in
                                       self.privileges(db)
                                       if n not in skip}))

    def grants(self, db, who):
        return [[k, t, p, opt] for n, k, t, p, opt in self.privileges(db)
                if n == who]

    def target(self, kind, who):
        return {"group": f"group {_q(who)}", "role": f"role {_q(who)}"}.get(
            kind, _q(who))

    def take(self, db, who, grant):
        k, t, p, _ = grant
        return [f"revoke {p.lower()} on {_q(self.name(db))}.{_q(t)} from"
                f" {self.target(k, who)}"]

    def give(self, db, who, grant):
        k, t, p, opt = grant
        return [f"grant {p.lower()} on {_q(self.name(db))}.{_q(t)} to"
                f" {self.target(k, who)}"
                + (" with grant option" if opt and k == "user" else "")]

    def said(self, grant):
        return f"{grant[1]} ({grant[2].lower()})"

    def end(self, db, who):
        from .engines.dbapi import MARK
        try:
            got = self.rows(db, "select count(pg_terminate_backend(process))"
                                f" from stv_sessions where user_name = {MARK}"
                                " and process <> pg_backend_pid()", (who,))
            return int(got[0][0]) if got else 0
        except Exception:  # noqa: BLE001 - migkit's account may not
            return 0

    def wider(self, db):
        from .engines.dbapi import MARK
        me = self.mine(db)
        out = [(f"{o} owns tables here: an owner writes whatever is"
                " revoked", True) for (o,) in self.rows(
                   db, "select distinct tableowner from pg_tables where"
                       f" schemaname = {MARK} and tableowner <> {MARK}",
                   (self.name(db), me))]
        out += [(f"{s} is a superuser: nothing short of stopping it keeps it"
                 " from writing", False) for s in sorted(self.supers(db))
                if s != me]
        out += [(f"the role {r} writes here and migkit's own account holds"
                 " it: it is not taken", True)
                for r in sorted({n for n, k, *_ in self.privileges(db)
                                 if k == "role"} & self.ours(db))]
        return out

    def may_end(self, db):
        # another user's session is ended, and its transactions seen, by
        # a superuser alone
        return self.mine(db) in self.supers(db)

    def may_see(self, db):
        return self.may_end(db)

    def clock(self, db):
        return str(self.rows(db, "select getdate()::varchar")[0][0])

    def left(self, db, who, at):
        from .engines.dbapi import MARK
        if not who:
            return (True, "no session of the application's to end")
        n = int(self.rows(
            db, "select count(*) from stv_sessions where user_name in"
                f" ({', '.join([MARK] * len(who))}) and starttime <"
                f" {MARK}::timestamp", (*who, at))[0][0])
        return (n == 0, f"{n} sessions of the application's users left from"
                        " before the freeze")

    def open(self, db, older_than):
        age = "" if older_than is None else \
            f" and txn_start < getdate() - interval '{int(older_than)} seconds'"
        got = self.rows(
            db, "select distinct txn_owner, pid, datediff(s, txn_start,"
                " getdate()) from svv_transactions where txn_owner <>"
                " current_user and lock_mode <> 'AccessShareLock'" + age)
        return [f"write transaction {str(u).strip()} pid {p}, open {s}s"
                for u, p, s in got]


# Snowflake
# Roles hold privileges. The write privileges on the schema's tables
# (`information_schema.table_privileges`) and the future grants that give
# them to every table made in it are revoked from each role migkit's own
# session does not hold, recorded first, granted back with their option.
# A statement checks its role's privileges as it runs: there is no session
# to end. The role that owns a table writes it whatever is revoked: named.
# On a source, a transaction that writes the schema holds its locks until
# it ends: the account's locks (`SHOW LOCKS IN ACCOUNT`) are what is held
# open, and each transaction holding one on the schema is aborted.

class SnowflakeHold(_Grants):
    WAY = "sf_grants"
    SAID = ("the write privileges on the schema's tables, and its future"
            " grants of them, revoked from the application's roles")
    SOURCE = True
    ENDS = "every transaction holding the schema's tables aborted"
    WRITE = ("INSERT", "UPDATE", "DELETE", "TRUNCATE")

    def schema(self, db):
        return self.eng._owner(self.side, db)

    def database(self):
        return self.eng._database(self.side)

    def where(self, db):
        return f"{_q(self.database())}.{_q(self.schema(db))}"

    def ours(self, db):
        got = self.rows(db, "select current_available_roles()")
        raw = got[0][0] if got else "[]"
        return set(json.loads(raw) if isinstance(raw, str) else raw or [])

    def privileges(self, db):
        from .engines.dbapi import MARK
        return [(str(g), str(t), str(p), str(gr).upper() == "YES")
                for g, t, p, gr in self.rows(
                    db, "select grantee, table_name, privilege_type,"
                        f" is_grantable from {_q(self.database())}"
                        ".information_schema.table_privileges where"
                        f" table_schema = {MARK} and privilege_type in"
                        " ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',"
                        " 'OWNERSHIP')", (self.schema(db),))]

    def future(self, db):
        """[(role, privilege, grant option)] of the schema's future grants
        on tables that write."""
        out = []
        for r in self.rows(db, f"show future grants in schema"
                               f" {self.where(db)}"):
            priv, on, to, grantee, opt = (str(r[1]), str(r[2]), str(r[4]),
                                          str(r[5]), r[6])
            if on.upper() == "TABLE" and to.upper() == "ROLE" and \
                    priv.upper() in self.WRITE:
                out.append((grantee, priv.upper(),
                            str(opt).lower() in ("true", "1")))
        return out

    def accounts(self, db):
        ours = self.ours(db)
        got = {g for g, t, p, _ in self.privileges(db) if p != "OWNERSHIP"}
        got |= {g for g, _, _ in self.future(db)}
        return self.named_only(sorted(got - ours))

    def grants(self, db, who):
        return [["table", t, p, opt] for g, t, p, opt in self.privileges(db)
                if g == who and p != "OWNERSHIP"] + [
            ["future", None, p, opt] for g, p, opt in self.future(db)
            if g == who]

    def on(self, db, grant):
        if grant[0] == "future":
            return f"future tables in schema {self.where(db)}"
        return f"table {self.where(db)}.{_q(grant[1])}"

    def take(self, db, who, grant):
        return [f"revoke {grant[2].lower()} on {self.on(db, grant)} from role"
                f" {_q(who)}"]

    def give(self, db, who, grant):
        return [f"grant {grant[2].lower()} on {self.on(db, grant)} to role"
                f" {_q(who)}" + (" with grant option" if grant[3] else "")]

    def said(self, grant):
        return ("every table made from now" if grant[0] == "future"
                else grant[1]) + f" ({grant[2].lower()})"

    def wider(self, db):
        ours = self.ours(db)
        privs = self.privileges(db)
        out = [(f"the role {g} owns {t}: an owner writes whatever is"
                " revoked", True) for g, t, p, _ in privs
               if p == "OWNERSHIP" and g not in ours][:5]
        out += [(f"the role {g} writes here, and migkit's own session holds"
                 " it: it is not taken", True) for g in sorted(
                    {g for g, t, p, _ in privs if p != "OWNERSHIP"} & ours)]
        return out

    def locks(self, db):
        """[(transaction, its start, resource)] of the locks held on the
        schema's tables, as the account lists them."""
        pre = f"{self.database()}.{self.schema(db)}.".upper()
        out = []
        for r in self.rows(db, "show locks in account"):
            resource, txn, began = str(r[0]), r[2], r[3]
            if resource.upper().startswith(pre):
                out.append((txn, began, resource))
        return out

    def may_end(self, db):
        return _tried(lambda: self.rows(db, "show locks in account"))

    def may_see(self, db):
        return self.may_end(db)

    def stop(self, db):
        txns = sorted({str(t) for t, _, _ in self.locks(db)})
        for t in txns:
            self.rows(db, f"select system$abort_transaction({int(t)})")
        return (f"{len(txns)} transactions holding the schema's tables"
                " aborted") if txns else ""

    def open(self, db, older_than):
        return [f"write transaction {t} on {res}, since {began}"
                for t, began, res in self.locks(db)]


# Couchbase
# Each local user's roles that write the hop's bucket - by name, never one
# for every bucket - swapped for the roles that read it, the user's whole
# list recorded first and put back whole. A role a user takes from a group,
# a role on every bucket and an administrator's write whatever one user
# loses: named. An open connection takes the user's roles as they now
# are. On a source, a transaction still open (`system:transactions`) is
# what is held open; a single write is done or not done.

_CB_WRITE = ("data_writer", "bucket_full_access", "data_backup",
             "query_insert", "query_update", "query_delete",
             "mobile_sync_gateway", "bucket_admin")
#: what each that reads as well is swapped for
_CB_READ = {"data_writer": "data_reader", "bucket_full_access": "data_reader",
            "data_backup": "data_reader", "query_update": "query_select",
            "query_delete": "query_select"}
_CB_WIDE = ("admin", "cluster_admin", "ro_admin", "security_admin")


def _cb_role(r):
    """A role as the cluster takes it back: `name[bucket:scope:coll]`."""
    parts = [r.get(k) for k in ("bucket_name", "scope_name",
                                "collection_name") if r.get(k)]
    return r["role"] + (f"[{':'.join(parts)}]" if parts else "")


class CouchbaseHold(Hold):
    WAY = "cb_roles"
    SAID = ("the application's users' roles that write the bucket swapped"
            " for the ones that read it")
    TARGET = "held"

    def rungs(self):
        from .decide import Rung
        return (Rung(self.WAY, self.SAID, gives=frozenset(KEEP),
                     needs=(_is("sees", "the cluster's open transactions in"
                                " sight", "migkit's user cannot list the"
                                " cluster's open transactions"
                                " (system:transactions)"),),
                     because="per user"),)

    def keep_rungs(self):
        from .decide import Rung
        return (Rung(self.WAY, self.SAID, gives=frozenset(KEEP),
                     because="per user"),)

    def readers(self, dbs):
        if self.side != "src":
            return {}
        return {"sees": _read(lambda: _tried(self.transactions))}

    def bucket(self, db):
        return self.eng._d(self.side, db)

    def call(self, method, path, form=None):
        import base64 as b64
        import ssl
        import urllib.parse
        import urllib.request
        ep = self.eng._ep(self.side)
        tls = bool(ep.options.get("tls"))
        port = ep.options.get("mgmt_port") or (18091 if tls else 8091)
        data = urllib.parse.urlencode(form).encode() if form is not None \
            else None
        req = urllib.request.Request(
            f"{'https' if tls else 'http'}://{ep.host}:{port}{path}",
            data=data, method=method, headers={
                "Authorization": "Basic " + b64.b64encode(
                    f"{ep.user}:{ep.password}".encode()).decode(),
                **({"Content-Type": "application/x-www-form-urlencoded"}
                   if data is not None else {})})
        ctx = None
        if tls:
            ctx = ssl.create_default_context(
                cafile=ep.options.get("tls_ca_file") or None)
            if ep.options.get("tls_insecure"):
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=15, context=ctx) as got:
            raw = got.read()
        return json.loads(raw) if raw.strip() else {}

    def users(self):
        """{user: its record} of the cluster's own users but migkit's."""
        me = self.eng._ep(self.side).user
        got = self.call("GET", "/settings/rbac/users/local")
        return {u["id"]: u for u in got or [] if u.get("id") != me}

    @staticmethod
    def direct(r):
        return any((o or {}).get("type") == "user"
                   for o in r.get("origins") or [{"type": "user"}])

    def writes(self, r, bucket):
        return r.get("role") in _CB_WRITE and r.get("bucket_name") == bucket

    def hold(self, dbs, way, say):
        for d in dbs:
            b = self.bucket(d)
            record = state(self.hop, d, self.side) or {
                "db": b, "way": way, "roles": {}}
            for uid, u in sorted(self.users().items()):
                if _named(self.hop) and uid not in _named(self.hop):
                    continue
                roles = [r for r in u.get("roles") or [] if self.direct(r)]
                todo = [r for r in roles if self.writes(r, b)]
                if not todo or uid in record["roles"]:
                    continue
                # the whole user as the cluster takes it back: a PUT of a
                # user replaces it, its groups among the rest
                record["roles"][uid] = {"was": [_cb_role(r) for r in roles],
                                        "name": u.get("name") or "",
                                        "groups": list(u.get("groups")
                                                       or [])}
                # recorded before it is done, as everywhere here
                _save(self.hop, d, record, self.side)
                keep = [_cb_role(r) for r in roles if r not in todo]
                keep += [_cb_role({**r, "role": _CB_READ[r["role"]]})
                         for r in todo if r["role"] in _CB_READ]
                self.call("PUT", f"/settings/rbac/users/local/{uid}",
                          {"name": u.get("name") or "",
                           "groups": ",".join(u.get("groups") or []),
                           "roles": ",".join(dict.fromkeys(keep))})
                say(f"{b}: {uid} - {len(todo)} roles that write swapped for"
                    " the ones that read")
            _save(self.hop, d, record, self.side)

    def keep(self, db, way, say):
        self.hold([db], way, say)
        for line in self.exempt(db, way):
            say(f"{db}: still able to write here: {line}")

    def release(self, db, record, say):
        for uid, entry in (record.get("roles") or {}).items():
            self.call("PUT", f"/settings/rbac/users/local/{uid}",
                      {"name": entry.get("name") or "",
                       "groups": ",".join(entry.get("groups") or []),
                       "roles": ",".join(entry.get("was") or [])})
        say(f"{record.get('db') or db}: {len(record.get('roles') or {})}"
            " users given back their roles as they were")

    def kept(self, db, record):
        b = self.bucket(db)
        out = []
        for uid, u in sorted(self.users().items()):
            left = [_cb_role(r) for r in u.get("roles") or []
                    if self.writes(r, b) or r.get("role") in _CB_WIDE
                    or (r.get("role") in _CB_WRITE
                        and r.get("bucket_name") == "*")]
            if left:
                out.append((False, f"{uid} still writes here"
                                   f" ({', '.join(left[:3])})"))
        return out or [(True, f"{len(record.get('roles') or {})} users hold"
                              " no role that writes the bucket")]

    def transactions(self):
        """[(id, its state)] of the transactions open on the cluster."""
        got = self.eng._cluster(self.side).query(
            "select t.id, t.state from system:transactions as t")
        return [(r.get("id"), r.get("state")) for r in got.rows()]

    def open(self, db, older_than):
        return [f"transaction {i} open ({s})"
                for i, s in self.transactions()]

    def held(self, db, way):
        record = state(self.hop, db, self.side) or {"roles": {}}
        out = self.kept(db, record)
        work = self.open(db, None)
        out.append((not work, "; ".join(work[:3]) if work else
                    "no transaction open on the cluster"))
        return out

    def exempt(self, db, way):
        b = self.bucket(db)
        out = []
        src = self.side == "src"
        for uid, u in sorted(self.users().items()):
            for r in u.get("roles") or []:
                if r.get("role") in _CB_WIDE:
                    out.append(f"{uid} is an administrator ({r['role']}):"
                               " nothing short of stopping it keeps it from"
                               " writing")
                elif r.get("role") in _CB_WRITE and r.get("bucket_name") \
                        == "*":
                    out.append(f"{uid} writes every bucket ({_cb_role(r)}):"
                               " taking it would take every bucket's"
                               + (f" - {LEAVES}" if src else ""))
                elif self.writes(r, b) and not self.direct(r):
                    out.append(f"{uid} writes here through a group: the"
                               " group's roles are not taken"
                               + (f" - {LEAVES}" if src else ""))
                else:
                    continue
                break
        return out


# --- targets kept by a change to their access policy, offered ----------------
# DynamoDB's and BigQuery's writes are allowed by policies kept outside the
# data. A change to them is offered, never assumed (as a change on the
# source is, `sourcechange`): the dry run says it, --go at a terminal asks
# it, `target_access: yes | no` answers it ahead; made only on yes,
# recorded first, taken off at cutover with the rest.

def _answered_yes():
    from .decide import Need
    return Need("an answer of yes to the change of the target's access",
                lambda f: f["answer"] == "yes",
                lambda f: (
                    "the answer to the change of the target's access is no"
                    if f["answer"] == "no" else
                    "it needs a change to the target's access policy -"
                    " offered, never assumed: answer it (target_access: yes"
                    " | no), or --go at a terminal asks it"))


# DynamoDB
# A deny in each of the hop's tables' own resource policy: every write
# action, for every principal but migkit's own and the service's own
# replication, what the policy held before kept beside it and put back
# whole. A deny in a resource policy outranks every allow an identity
# holds. Whoever may change the table's policy can take it off: named.

_DDB_WRITES = ("dynamodb:PutItem", "dynamodb:UpdateItem",
               "dynamodb:DeleteItem", "dynamodb:BatchWriteItem",
               "dynamodb:PartiQLInsert", "dynamodb:PartiQLUpdate",
               "dynamodb:PartiQLDelete")
_DDB_SID = "MigkitKeepsTheTargetFromTheApplication"


class DynamoDBHold(Hold):
    TARGET = "offered"
    WAY = "ddb_policy"

    def keep_rungs(self):
        from .decide import Rung
        return (Rung(self.WAY, "every principal's writes but migkit's"
                               " denied in each table's own policy",
                     gives=frozenset(KEEP),
                     needs=(_answered_yes(), _is(
                         "me", "migkit's own principal known", "migkit's"
                         " own principal could not be told (sts)"), _is(
                         "policies", "tables that keep a policy of their"
                         " own", "the service keeps no policy on a table"
                         " (resource policies)")),
                     because="a deny in a table's own policy outranks every"
                             " allow"),)

    def readers(self, dbs):
        return {"answer": lambda: answered(self.hop),
                "me": _read(lambda: bool(self.principals())),
                "policies": _read(lambda: self.policies(dbs))}

    def client(self):
        return self.eng._client(self.side)

    def sts(self):
        import boto3
        ep = self.hop.source if self.side == "src" else self.hop.target
        kw = {"region_name": ep.options.get("region") or "us-east-1"}
        if ep.user:
            kw.update(aws_access_key_id=ep.user,
                      aws_secret_access_key=ep.password)
        return boto3.client("sts", **kw)

    def principals(self):
        """The ARNs migkit's own calls are made as, as a policy's
        `aws:PrincipalArn` names them: an assumed role by its role, under
        any path."""
        arn = str(self.sts().get_caller_identity()["Arn"])
        head, _, rest = arn.partition(":assumed-role/")
        if rest:
            account = head.rsplit(":", 1)[-1]
            role = rest.split("/")[0]
            return [f"arn:aws:iam::{account}:role/{role}",
                    f"arn:aws:iam::{account}:role/*/{role}"]
        return [arn]

    def tables(self, db):
        """{name on the target: its ARN} of the hop's tables there now."""
        c = self.client()
        out = {}
        for t in self.eng.neutral_tables(self.side, db):
            name = self.eng._name(self.side, db, t)
            out[name] = c.describe_table(TableName=name)["Table"]["TableArn"]
        return out

    def policy(self, arn):
        """(the table's policy, its revision) or (None, None)."""
        try:
            got = self.client().get_resource_policy(ResourceArn=arn)
        except Exception as e:  # noqa: BLE001 - classified here
            if "PolicyNotFound" in type(e).__name__ or "PolicyNotFound" in \
                    str(e):
                return None, None
            raise
        return got.get("Policy"), got.get("RevisionId")

    def policies(self, dbs):
        for d in dbs:
            for arn in self.tables(d).values():
                self.policy(arn)
                return True
        # no table of the hop's there yet: nothing to put a deny on until
        # the copy makes them, and the hold is taken again after it
        return True

    def statement(self, arn):
        return {"Sid": _DDB_SID, "Effect": "Deny", "Principal": "*",
                "Action": list(_DDB_WRITES), "Resource": arn,
                "Condition": {
                    "ArnNotLike": {"aws:PrincipalArn": self.principals()},
                    "Bool": {"aws:PrincipalIsAWSService": "false"}}}

    def offer(self, db):
        try:
            n = len(self.tables(db))
            who = ", ".join(self.principals())
        except Exception as e:  # noqa: BLE001 - said as it is
            n, who = None, f"(it could not be read: {str(e)[:80]})"
        return [
            "the target's writes are allowed by its tables' access policies,"
            " outside the data: keeping the application from them is a"
            " change to those policies - offered, never assumed:",
            f"  on each of the hop's tables ({'?' if n is None else n} there"
            " now, and each the copy makes): a statement in the table's own"
            " policy denying every principal but migkit's own"
            f" ({who}) and the service's replication its writes - what the"
            " policy held before kept and put back whole at cutover",
            "  yes - migkit adds it and takes it off at cutover or on"
            " rollback --apply; no - the target is left as it is, and the"
            " move with protect_target stops before anything is copied",
            f"  answered ahead in the hop: {ACCESS}: yes | no"]

    def keep(self, db, way, say):
        record = state(self.hop, db, self.side) or {"db": db, "way": way,
                                                    "tables": {}}
        c = self.client()
        n = 0
        for name, arn in sorted(self.tables(db).items()):
            if name not in record["tables"]:
                before, _ = self.policy(arn)
                record["tables"][name] = {"arn": arn, "before": before}
                # recorded before it is done, as everywhere here
                _save(self.hop, db, record, self.side)
            before, rev = self.policy(arn)
            doc = json.loads(before) if before else {
                "Version": "2012-10-17", "Statement": []}
            doc["Statement"] = [s for s in doc.get("Statement") or []
                                if s.get("Sid") != _DDB_SID] + [
                self.statement(arn)]
            c.put_resource_policy(ResourceArn=arn, Policy=json.dumps(doc),
                                  ExpectedRevisionId=rev or "NO_POLICY")
            n += 1
        _save(self.hop, db, record, self.side)
        say(f"{db}: every principal's writes but migkit's denied in {n}"
            " tables' own policies")
        for line in self.exempt(db, way):
            say(f"{db}: still able to write here: {line}")

    def release(self, db, record, say):
        c = self.client()
        for name, t in (record.get("tables") or {}).items():
            if t.get("before"):
                c.put_resource_policy(ResourceArn=t["arn"],
                                      Policy=t["before"])
            else:
                try:
                    c.delete_resource_policy(ResourceArn=t["arn"])
                except Exception as e:  # noqa: BLE001 - gone already
                    if "PolicyNotFound" not in type(e).__name__ and \
                            "PolicyNotFound" not in str(e):
                        raise
        say(f"{db}: {len(record.get('tables') or {})} tables' policies as"
            " they were")

    def kept(self, db, record):
        out = []
        for name, arn in sorted(self.tables(db).items()):
            got, _ = self.policy(arn)
            ours = [s for s in (json.loads(got).get("Statement") or [])
                    if s.get("Sid") == _DDB_SID] if got else []
            out.append((bool(ours) and ours[0] == self.statement(arn),
                        f"{name}: " + ("every principal's writes but"
                                       " migkit's denied" if ours
                                       else "no deny in its policy")))
        return out or [(True, "no table of the hop's on the target yet")]

    def exempt(self, db, way):
        return ["a principal allowed to change a table's policy can take"
                " the deny off it"]


# BigQuery
# A dataset's access entries: every entry that writes - WRITER, OWNER and
# the BigQuery roles that edit or own - for anyone but migkit's own
# account, removed, a READER put in its place where it had none, and
# migkit's own account given a WRITER of its own where it wrote only as
# one of the project's; recorded first, and on release exactly what was
# taken put back and what was added taken away. A role granted on the
# project reaches every dataset, whatever a dataset's access says: named.

_BQ_WRITE = ("WRITER", "OWNER", "roles/bigquery.dataEditor",
             "roles/bigquery.dataOwner", "roles/bigquery.admin")


class BigQueryHold(Hold):
    TARGET = "offered"
    WAY = "bq_access"

    def keep_rungs(self):
        from .decide import Rung
        return (Rung(self.WAY, "every entry that writes the dataset but"
                               " migkit's own made one that reads",
                     gives=frozenset(KEEP),
                     needs=(_answered_yes(), _is(
                         "me", "migkit's own account known", "migkit signs"
                         " in without an account a dataset's access can"
                         " name (a service account)")),
                     because="per entry"),)

    def readers(self, dbs):
        return {"answer": lambda: answered(self.hop),
                "me": _read(lambda: bool(self.mine()))}

    def client(self):
        return self.eng._client(self.side)

    def mine(self):
        """The service account migkit signs in as, or None - a machine's
        own account before it has signed in says only "default"."""
        creds = getattr(self.client(), "_credentials", None)
        got = getattr(creds, "service_account_email", None)
        return got if got and "@" in str(got) else None

    def ref(self, db):
        return f"{self.eng._database(self.side)}.{self.eng._d(self.side, db)}"

    def ours(self, e):
        me = self.mine()
        return e.entity_id in (me, f"serviceAccount:{me}")

    def writer(self, e):
        return e.role in _BQ_WRITE and e.entity_type in (
            "userByEmail", "groupByEmail", "domain", "specialGroup",
            "iamMember") and not self.ours(e)

    def offer(self, db):
        return [
            "the target's writes are allowed by its dataset's access, kept"
            " outside the data: keeping the application from them is a"
            " change to that access - offered, never assumed:",
            f"  on {self.ref(db)}: every entry that writes, but migkit's own"
            " account's, made one that reads; migkit's own account given a"
            " writer entry of its own where it wrote as one of the"
            " project's - all of it given back as it was at cutover",
            "  yes - migkit changes it and gives it back at cutover or on"
            " rollback --apply; no - the target is left as it is, and the"
            " move with protect_target stops before anything is copied",
            f"  answered ahead in the hop: {ACCESS}: yes | no"]

    def keep(self, db, way, say):
        from google.cloud.bigquery import AccessEntry
        c = self.client()
        ds = c.get_dataset(self.ref(db))
        record = state(self.hop, db, self.side) or {
            "db": db, "way": way, "taken": [], "added": []}
        entries = list(ds.access_entries)
        taken = [e for e in entries if self.writer(e)]
        added = []
        have = {(e.role, e.entity_type, str(e.entity_id)) for e in entries}
        for e in taken:
            if ("READER", e.entity_type, str(e.entity_id)) not in have:
                added.append(AccessEntry("READER", e.entity_type,
                                         e.entity_id))
        me = self.mine()
        if not any(self.ours(e) and e.role in _BQ_WRITE for e in entries):
            added.append(AccessEntry("WRITER", "userByEmail", me))
        record["taken"] += [e.to_api_repr() for e in taken]
        record["added"] += [e.to_api_repr() for e in added]
        # recorded before it is done, as everywhere here
        _save(self.hop, db, record, self.side)
        ds.access_entries = [e for e in entries if e not in taken] + added
        c.update_dataset(ds, ["access_entries"])
        say(f"{db}: {len(taken)} entries that write the dataset made ones"
            " that read")
        for line in self.exempt(db, way):
            say(f"{db}: still able to write here: {line}")

    def release(self, db, record, say):
        from google.cloud.bigquery import AccessEntry
        c = self.client()
        ds = c.get_dataset(self.ref(db))
        added = [AccessEntry.from_api_repr(x) for x in
                 record.get("added") or []]
        taken = [AccessEntry.from_api_repr(x) for x in
                 record.get("taken") or []]
        now = [e for e in ds.access_entries if e not in added]
        ds.access_entries = now + [e for e in taken if e not in now]
        c.update_dataset(ds, ["access_entries"])
        say(f"{db}: the dataset's access as it was ({len(taken)} entries"
            " given back)")

    def kept(self, db, record):
        ds = self.client().get_dataset(self.ref(db))
        left = [f"{e.entity_type} {e.entity_id} ({e.role})"
                for e in ds.access_entries if self.writer(e)]
        return [(not left, "still writing: " + ", ".join(left[:3]) if left
                 else "no entry of the dataset's writes but migkit's")]

    def exempt(self, db, way):
        return ["an account granted a BigQuery role that writes on the"
                " project writes every dataset, whatever a dataset's access"
                " says"]


#: the hold each engine has (`Hold`); the engines not here are held by
#: their operator, and `decide.WAYS["writes held"]` says why
HOLDS = {"postgres": PostgresHold, "mysql": MySQLHold, "mongodb": MongoHold,
         "redis": RedisHold, "mssql": MSSQLHold, "cassandra": CassandraHold,
         "kafka": KafkaHold, "opensearch": OpenSearchHold,
         "tidb": TiDBHold, "cockroachdb": PgWireHold,
         "clickhouse": ClickHouseHold, "oracle": OracleHold, "db2": Db2Hold,
         "ase": AseHold, "teradata": TeradataHold, "redshift": RedshiftHold,
         "snowflake": SnowflakeHold, "couchbase": CouchbaseHold}
#: the sources the cutover freezes itself; any other is frozen by its
#: operator (`freeze: operator`) and proved by migkit
FREEZES = tuple(HOLDS)
#: the hold each engine keeps a target by (`protect`): every source's, and
#: those kept for targets alone; an engine not here is not kept, and
#: `decide.WAYS["read-only target"]` says why
KEEPS = {**HOLDS, "dsql": PgWireHold, "db2zos": Db2ZosHold,
         "db2i": Db2iHold, "dynamodb": DynamoDBHold,
         "bigquery": BigQueryHold}
#: the ways whose server switch is lifted before the databases' records
_SERVER_FIRST = ("os_cluster",)
