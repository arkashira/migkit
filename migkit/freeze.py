"""Keep the application from writing to the target until cutover.

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
"""
import json

WRITES = ("INSERT", "UPDATE", "DELETE", "TRUNCATE")


def _q(ident):
    return '"' + str(ident).replace('"', '""') + '"'


def _lit(text):
    return "'" + str(text).replace("'", "''") + "'"


def _state_path(hop, db):
    return hop.report_dir(db) / "freeze.json"


def state(hop, db):
    """What a freeze on this database recorded, or None."""
    try:
        return json.loads(_state_path(hop, db).read_text())
    except (OSError, ValueError):
        return None


def _save(hop, db, record):
    path = _state_path(hop, db)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(record, indent=1))
    tmp.replace(path)


def app_roles(hop, eng, db):
    """The roles whose writes the freeze stops."""
    named = (hop.options or {}).get("app_roles")
    ours = {hop.target.user, hop.source.user}
    if named:
        return [r for r in named if r not in ours]
    if _engine(hop) == "mysql":
        from .users import _mysql_sysuser
        q = "select distinct user from mysql.user order by 1"
        src = {r[0] for r in eng._q("src", q)}
        return [r[0] for r in eng._q("dst", q)
                if r[0] in src and r[0] not in ours
                and not _mysql_sysuser(r[0])]
    from .users import _pg_sysrole
    q = "select rolname from pg_roles where rolcanlogin order by 1"
    src = set(eng._psql("src", db, q).split())
    dst = eng._psql("dst", db, q).split()
    return [r for r in dst
            if r in src and r not in ours and not _pg_sysrole(r)]


def _direct_writes(eng, db, role):
    """[(schema.table, privilege)] granted to `role` itself in this db."""
    got = eng._psql("dst", db,
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


def _still_writes(eng, db, role):
    """Tables `role` can still write to, whatever the reason."""
    got = eng._psql("dst", db,
                    "select n.nspname||'.'||c.relname from pg_class c"
                    " join pg_namespace n on n.oid = c.relnamespace"
                    " where c.relkind in ('r', 'p') and n.nspname not in"
                    " ('pg_catalog', 'information_schema')"
                    f" and has_table_privilege({_lit(role)}, c.oid,"
                    " 'INSERT, UPDATE, DELETE, TRUNCATE') order by 1")
    return [line for line in got.splitlines() if line]


def _setting(eng, db, role):
    """The role's own `default_transaction_read_only` in this database, or
    None when it has none."""
    got = eng._psql("dst", db,
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


def supported(hop):
    return _engine(hop) in ("postgres", "mysql")


def freeze(hop, eng, db, say):
    """Stop the application's roles writing to this target database.

    Idempotent: a role frozen already is checked again, and anything it
    could newly write to - a table the move just created, a grant made
    since - is frozen too, the record growing rather than being replaced.
    """
    if _engine(hop) == "mysql":
        return _my_freeze(hop, eng, db, say)
    dbname = eng._d("dst", db)
    record = state(hop, db) or {"db": dbname, "roles": {}}
    for role in app_roles(hop, eng, db):
        entry = record["roles"].setdefault(
            role, {"revoked": [], "setting": None, "set": False})
        revoked = [tuple(x) for x in entry["revoked"]]
        todo = [g for g in _direct_writes(eng, db, role) if g not in revoked]
        entry["revoked"] = [list(g) for g in revoked + todo]
        # recorded before it is done: a run that stops half way leaves a
        # record `thaw` can finish from
        _save(hop, db, record)
        for table, priv in todo:
            sch, _, tbl = table.partition(".")
            eng._psql("dst", db, f"revoke {priv} on {_q(sch)}.{_q(tbl)}"
                                 f" from {_q(role)}")
        left = _still_writes(eng, db, role)
        how = [f"{len(todo)} write grants revoked"] if todo else []
        if left:
            if not entry["set"]:
                entry["setting"] = _setting(eng, db, role)
                entry["set"] = True
                _save(hop, db, record)
            eng._psql("dst", db, f"alter role {_q(role)} in database"
                                 f" {_q(dbname)} set"
                                 " default_transaction_read_only = on")
            ended = eng._psql("dst", db,
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
    _save(hop, db, record)
    return record


def thaw(hop, eng, db, say):
    """Give back exactly what `freeze` took, and forget it."""
    record = state(hop, db)
    if not record:
        return None
    if _engine(hop) == "mysql":
        return _my_thaw(hop, eng, db, record, say)
    dbname = record.get("db") or eng._d("dst", db)
    for role, entry in record["roles"].items():
        for table, priv in entry["revoked"]:
            sch, _, tbl = table.partition(".")
            eng._psql("dst", db, f"grant {priv} on {_q(sch)}.{_q(tbl)}"
                                 f" to {_q(role)}")
        if entry.get("set"):
            before = entry.get("setting")
            eng._psql("dst", db,
                      f"alter role {_q(role)} in database {_q(dbname)} "
                      + (f"set default_transaction_read_only = {before}"
                         if before is not None
                         else "reset default_transaction_read_only"))
        say(f"{dbname}: {role} - writes given back"
            f" ({len(entry['revoked'])} grants)")
    _state_path(hop, db).unlink()
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


def _my_grants(eng, dbname, user):
    """[[host, table or None, privilege]] of write grants on this database,
    database-wide ones with table None."""
    out = []
    for host, *flags in eng._q(
            "dst", "select host, insert_priv, update_priv, delete_priv"
                   " from mysql.db where db = %s and user = %s",
            (dbname, user)):
        out += [[host, None, p.upper()]
                for p, flag in zip(_MY_WRITES, flags) if flag == "Y"]
    for host, table, privs in eng._q(
            "dst", "select host, table_name, table_priv"
                   " from mysql.tables_priv where db = %s and user = %s",
            (dbname, user)):
        have = {x.strip().lower() for x in str(privs).split(",")}
        out += [[host, table, p.upper()] for p in _MY_WRITES
                if p.lower() in have]
    return out


def _my_wider(eng, user):
    """Where this account can write that a per-database freeze cannot
    reach: server-wide privileges, and roles granted to it."""
    said = []
    got = eng._q("dst", "select count(*) from mysql.user where user = %s"
                        " and 'Y' in (insert_priv, update_priv,"
                        " delete_priv)", (user,))
    if got and int(got[0][0]):
        said.append("holds write privileges on every database")
    roles = [r[0] for r in eng._q(
        "dst", "select distinct from_user from mysql.role_edges"
               " where to_user = %s", (user,))]
    if roles:
        said.append(f"is granted the roles {', '.join(roles)}")
    return said


def _my_freeze(hop, eng, db, say):
    dbname = eng._d("dst", db)
    record = state(hop, db) or {"db": dbname, "roles": {}}
    for user in app_roles(hop, eng, db):
        entry = record["roles"].setdefault(user, {"revoked": []})
        todo = [g for g in _my_grants(eng, dbname, user)
                if g not in entry["revoked"]]
        entry["revoked"] += todo
        _save(hop, db, record)
        for host, table, priv in todo:
            where = (f"{_my_q(dbname)}.{_my_q(table)}" if table
                     else f"{_my_q(dbname)}.*")
            eng._q("dst", f"revoke {priv} on {where} from"
                          f" {_my_account(user, host)}")
        ended = 0
        if todo:
            # a database-level revoke reaches a session only when it next
            # chooses the database; ending the session is what makes it now
            for (pid,) in eng._q(
                    "dst", "select id from information_schema.processlist"
                           " where user = %s and id <> connection_id()",
                    (user,)):
                eng._q("dst", f"kill {int(pid)}")
                ended += 1
        how = ([f"{len(todo)} write grants revoked, {ended} open sessions"
                " ended"] if todo else [])
        wider = _my_wider(eng, user)
        if wider:
            how.append(f"still able to write here: it {' and '.join(wider)},"
                       " which cannot be taken for this database alone -"
                       " left as it is")
        say(f"{dbname}: {user} - " + ("; ".join(how) if how
                                        else "nothing to freeze"))
    _save(hop, db, record)
    return record


def _my_thaw(hop, eng, db, record, say):
    dbname = record.get("db") or eng._d("dst", db)
    for user, entry in record["roles"].items():
        for host, table, priv in entry["revoked"]:
            where = (f"{_my_q(dbname)}.{_my_q(table)}" if table
                     else f"{_my_q(dbname)}.*")
            eng._q("dst", f"grant {priv} on {where} to"
                          f" {_my_account(user, host)}")
        say(f"{dbname}: {user} - writes given back"
            f" ({len(entry['revoked'])} grants)")
    _state_path(hop, db).unlink()
    return record
