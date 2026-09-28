"""Login/user sync source -> target, keeping the same password.

mysql    : password hash copied from mysql.user (CREATE USER ... IDENTIFIED WITH
           <plugin> AS <hash>) -> the user keeps the exact same password and the
           plaintext is never read or shown. Grants replayed from SHOW GRANTS.
postgres : password hashes are not readable on managed sources (RDS locks
           pg_authid), so create takes a passwords file (yaml `role: password`);
           attributes and role memberships are copied from the source.

Cloud system accounts (AWS_*, rds*, tencent*, mysql internal) are excluded on
both sides. Never writes to the source. create/rollback are dry-run unless
apply=True. Every apply writes a created-record under the hop's report dir and
rollback drops only users listed in that record.
"""
import json
import datetime
import hashlib
import os

from .config import get_hop

MYSQL_SYS = {'mysql.sys', 'mysql.session', 'mysql.infoschema', 'root',
             'rdsadmin', 'rds_superuser_role', 'rdswriteforwarduser',
             'tencentroot', 'tencentdba'}
PG_SYS = {'rdsadmin', 'rdstopmgr', 'rds_superuser', 'rdsrepladmin',
          'rdswriteforwarduser', 'root', 'postgres'}


def _h(x):
    b = x if isinstance(x, bytes) else str(x or "").encode()
    return hashlib.md5(b, usedforsecurity=False).hexdigest()[:10]


def _mysql_sysuser(u):
    return u in MYSQL_SYS or u.startswith("AWS_")


def _pg_sysrole(r):
    if r.startswith("pg_"):
        return True
    return r in PG_SYS or r.startswith("tencentdb") or r.startswith("rds")


def _retry(fn, tries=4, wait=5):
    """Cross-cloud links drop a connection now and then, usually while a checksum
    run is saturating the same tunnel. One timeout must not cost the whole users
    step, so try again before giving up."""
    import time
    last = None
    for n in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if n < tries - 1:
                print(f"   cannot connect ({str(e).strip().splitlines()[-1][:70]}) "
                      f"retry {n + 2}/{tries} in {wait}s")
                time.sleep(wait)
    raise last


def _mysql_conn(ep):
    import pymysql
    return _retry(lambda: pymysql.connect(
        host=ep.host, port=ep.port, user=ep.user,
        password=ep.password, connect_timeout=15, autocommit=True,
        **ep.mysql_tls()))


def _pg_conn(ep):
    import psycopg2

    def _open():
        c = psycopg2.connect(host=ep.host, port=ep.port, user=ep.user,
                             password=ep.password, dbname="postgres",
                             connect_timeout=15, **ep.libpq_tls())
        c.autocommit = True
        return c
    return _retry(_open)


def _mysql_users(ep):
    c = _mysql_conn(ep)
    cur = c.cursor()
    cur.execute("select user, host, plugin, authentication_string from mysql.user")
    d = {(u, h): (p, a) for u, h, p, a in cur.fetchall() if not _mysql_sysuser(u)}
    c.close()
    return d


def _pg_hashes(ep):
    """rolname -> password hash, when the source allows reading pg_authid.
    RDS/Aurora blocks it (returns {}); a PG where we are a real superuser does
    not, so the hash can be copied to the target without knowing the password."""
    try:
        c = _pg_conn(ep)
        cur = c.cursor()
        cur.execute("select rolname, rolpassword from pg_authid where rolpassword is not null")
        d = {n: h for n, h in cur.fetchall() if not _pg_sysrole(n)}
        c.close()
        return d
    except Exception:
        return {}


def _pg_roles(ep):
    c = _pg_conn(ep)
    cur = c.cursor()
    cur.execute("""select r.rolname, r.rolcanlogin, r.rolcreatedb, r.rolcreaterole,
                          r.rolconnlimit, r.rolsuper, r.rolreplication, r.rolbypassrls,
                          r.rolinherit, r.rolvaliduntil,
                          array(select b.rolname from pg_auth_members m
                                join pg_roles b on m.roleid=b.oid where m.member=r.oid)
                   from pg_roles r""")
    d = {}
    for n, lg, cd, cr, cl, su, rep, byp, inh, valid, mo in cur.fetchall():
        if _pg_sysrole(n):
            continue
        d[n] = {"login": lg, "createdb": cd, "createrole": cr, "connlimit": cl,
                "superuser": su, "replication": rep, "bypassrls": byp, "inherit": inh,
                "valid_until": str(valid) if valid else None,
                "member_of": [m for m in mo if m.startswith("pg_") or not _pg_sysrole(m)]}
    c.close()
    return d


def compare(hop, say=print):
    eng = hop.engine
    if eng == "mysql":
        s = _mysql_users(hop.source)
        t = _mysql_users(hop.target)
        skeys = {f"{u}@{h}" for u, h in s}
        tkeys = {f"{u}@{h}" for u, h in t}
        pw = sorted(f"{u}@{h}" for (u, h) in s if (u, h) in t
                    and _h(s[(u, h)][1]) != _h(t[(u, h)][1]))
    elif eng == "postgres":
        s = _pg_roles(hop.source)
        t = _pg_roles(hop.target)
        skeys, tkeys, pw = set(s), set(t), []
    elif eng in ("mongodb", "mongo"):
        s = _mongo_users(hop.source)
        t = _mongo_users(hop.target)
        skeys = {f"{d}.{u}" for d, u in s}
        tkeys = {f"{d}.{u}" for d, u in t}
        pw = []
    elif eng == "kafka":
        s = _kafka_accounts(hop, "src")
        t = _kafka_accounts(hop, "dst")
        skeys, tkeys = set(s["users"] or {}), set(t["users"] or {})
        pw = []
    elif eng == "clickhouse":
        s = _ch_access(hop, "src")
        t = _ch_access(hop, "dst")
        skeys, tkeys = set(s), set(t)
        pw = []
    elif eng == "cassandra":
        s = _cs_roles(hop, "src")
        t = _cs_roles(hop, "dst")
        skeys, tkeys = set(s), set(t)
        # the salted hash itself: one carried by `create` is the same one
        pw = sorted(r for r in s if r in t and s[r]["hash"] != t[r]["hash"])
    elif eng == "mssql":
        s = _ms_principals(hop, "src")
        t = _ms_principals(hop, "dst")
        skeys, tkeys = set(s), set(t)
        pw = sorted(k for k in s if k in t and s[k].get("hash")
                    and s[k].get("hash") != t[k].get("hash"))
    elif eng == "redis":
        s = _redis_users(hop.source)
        t = _redis_users(hop.target)
        skeys, tkeys = set(s), set(t)
        # ACL LIST shows each password as its SHA-256 (`#...`), so a
        # password is compared, and later carried, without being read
        pw = sorted(u for u in s if u in t
                    and s[u]["hashes"] != t[u]["hashes"])
    else:
        from .capabilities import require
        require(eng, "users")
        raise SystemExit(f"users: engine {eng} not supported")
    missing = sorted(skeys - tkeys)
    extra = sorted(tkeys - skeys)
    out = {"check": "users", "hop": hop.name, "engine": eng,
           "generated": datetime.date.today().isoformat(),
           "excluded": "cloud system accounts (AWS_*, rds*, tencent*, mysql internal)",
           "source_users": len(skeys), "target_users": len(tkeys),
           "missing_on_target": missing, "extra_on_target": extra,
           "password_differs": pw,
           "result": "pass" if not missing and not pw else "gap"}
    say(f"hop={hop.name} engine={eng}  source={len(skeys)} target={len(tkeys)}")
    say(f"  missing on target ({len(missing)}): {missing}")
    say(f"  extra on target ({len(extra)}): {extra}")
    if eng == "redis":
        rd = sorted(u for u in s if u in t and s[u]["rules"] != t[u]["rules"])
        out["rules_differ"] = rd
        out["password_differs"] = pw
        out["result"] = "pass" if not missing and not pw and not rd \
            else "gap"
        say(f"  rules differ ({len(rd)}): {rd}")
        say(f"  password differs ({len(pw)}): {pw}")
    elif eng == "kafka":
        _kafka_compared(out, s, t, missing, say)
    elif eng == "mssql":
        gd = sorted(k for k in s if k in t and (
            set(s[k]["roles"]) - set(t[k]["roles"])
            or set(s[k]["grants"]) - set(t[k]["grants"])))
        out.update(grants_differ=gd, password_differs=pw)
        out["result"] = "pass" if not (missing or pw or gd) else "gap"
        say(f"  password differs ({len(pw)}): {pw}")
        say(f"  roles or permissions missing on target ({len(gd)}): {gd}")
    elif eng == "cassandra":
        ad = sorted(r for r in s if r in t and any(
            s[r][a] != t[r][a] for a in ("login", "superuser",
                                         "member_of")))
        gd = sorted(r for r in s if r in t
                    and set(s[r]["grants"]) - set(t[r]["grants"]))
        out.update(attributes_differ=ad, grants_differ=gd)
        out["result"] = "pass" if not (missing or pw or ad or gd) \
            else "gap"
        say(f"  password differs ({len(pw)}): {pw}")
        say(f"  login, superuser or membership differ ({len(ad)}): {ad}")
        say(f"  permissions missing on target ({len(gd)}): {gd}")
    elif eng == "clickhouse":
        gd = sorted(k for k in s if k in t
                    and set(s[k]["grants"]) - set(t[k]["grants"]))
        out["grants_differ"] = gd
        out["result"] = "pass" if not missing and not gd else "gap"
        say(f"  grants missing on target ({len(gd)}): {gd}")
        say("  note: a password is carried as its hash where the source"
            " shows it (display_secrets_in_show_and_select); otherwise"
            " create sets the one given in the passwords file")
    elif eng in ("mongodb", "mongo"):
        # only roles the source has and the target lacks; a target superset
        # is not a gap
        rd = sorted(f"{d}.{u}" for (d, u) in s
                    if (d, u) in t and _mongo_role_gap(s[(d, u)], t[(d, u)]))
        out["roles_differ"] = rd
        out["result"] = "pass" if not missing and not rd else "gap"
        say(f"  roles differ ({len(rd)}): {rd}")
        say("  note: passwords cannot be copied from the source (SCRAM, and DocumentDB blocks system.users)"
            " -> create sets new ones")
    elif eng == "mysql":
        say(f"  password differs ({len(pw)}): {pw}")
    else:
        sh = _pg_hashes(hop.source)
        if sh:
            th = _pg_hashes(hop.target)
            pw = sorted(r for r, h in sh.items() if r in th and th[r] != h)
            out["password_differs"] = pw
            out["result"] = "pass" if not missing and not pw else "gap"
            say(f"  password differs ({len(pw)}): {pw}   (hash copied from pg_authid)")
        else:
            say("  password compare: source hides pg_authid (managed) - names/attributes only;"
                " create needs --passwords")
    jf = hop.report_dir() / "users.json"
    json.dump(out, open(jf, "w"), indent=2, ensure_ascii=False)
    say(f"  json: {jf}")
    return out, s


# --- kafka -----------------------------------------------------------------
#: an ACL as compared and recorded: principal, host, operation, permission,
#: resource type, resource name, pattern type - names, not the wire numbers
ACL_FIELDS = ("principal", "host", "operation", "permission", "resource",
              "name", "pattern")


def _kafka_admin(hop, side):
    from .engines.kafka import KafkaEngine
    return KafkaEngine(hop)._admin(side)


def _kafka_accounts(hop, side):
    """{"users": {name: [mechanism, ...]}, "acls": {key, ...}} of one
    cluster, each None with the reason in "unread" when the cluster did
    not say: a cluster that checks no ACLs refuses to list them, and one
    that signs in some other way (IAM, mTLS) may have no SCRAM list.

    A SCRAM password is stored salted and cannot be read back, so what
    is compared of a user is its name and the mechanisms it signs in with.
    """
    from kafka.admin import (ACLFilter, ACLOperation, ACLPermissionType,
                             ACLResourcePatternType, ResourcePatternFilter,
                             ResourceType)
    admin = _kafka_admin(hop, side)
    out, unread = {"users": None, "acls": None}, {}
    try:
        try:
            got = admin.describe_user_scram_credentials()
            out["users"] = {
                u: sorted(i["mechanism"].name.replace("_", "-")
                          for i in v.get("credential_infos") or [])
                for u, v in got.items() if not v.get("error")}
        except Exception as e:
            unread["users"] = f"{type(e).__name__}: {str(e)[:120]}"
        try:
            acls, _ = admin.describe_acls(ACLFilter(
                None, None, ACLOperation.ANY, ACLPermissionType.ANY,
                ResourcePatternFilter(ResourceType.ANY, None,
                                      ACLResourcePatternType.ANY)))
            out["acls"] = {_acl_key(a) for a in acls}
        except Exception as e:
            unread["acls"] = f"{type(e).__name__}: {str(e)[:120]}"
    finally:
        admin.close()
    out["unread"] = unread
    return out


def _acl_key(acl):
    from kafka.admin import (ACLOperation, ACLPermissionType,
                             ACLResourcePatternType, ResourceType)
    rp = acl.resource_pattern
    return (acl.principal, acl.host, ACLOperation(acl.operation).name,
            ACLPermissionType(acl.permission_type).name,
            ResourceType(rp.resource_type).name, rp.resource_name,
            ACLResourcePatternType(rp.pattern_type).name)


def _acl_of(key):
    from kafka.admin import (ACL, ACLOperation, ACLPermissionType,
                             ACLResourcePatternType, ResourcePattern,
                             ResourceType)
    principal, host, op, perm, rtype, name, ptype = key
    return ACL(principal, host, ACLOperation[op], ACLPermissionType[perm],
               ResourcePattern(ResourceType[rtype], name,
                               ACLResourcePatternType[ptype]))


def _acl_said(key):
    principal, host, op, perm, rtype, name, ptype = key
    where = "" if host == "*" else f" from {host}"
    return (f"{principal} {perm.lower()} {op.lower()} on {rtype.lower()}"
            f" {name}{' (prefix)' if ptype == 'PREFIXED' else ''}{where}")


def _kafka_compared(out, s, t, missing, say):
    """The users' mechanisms and the ACLs, on top of the user names.
    What a side would not list is a result of its own, never a pass."""
    su, tu = s["users"] or {}, t["users"] or {}
    mech = sorted(u for u in su if u in tu and set(su[u]) - set(tu[u]))
    sa, ta = s["acls"], t["acls"]
    gone = sorted(sa - ta) if sa and ta is not None else []
    out["mechanisms_differ"] = mech
    out["acls_missing"] = [list(k) for k in gone]
    out["source_acls"] = len(sa or ())
    unread = [f"{side} {what}: {why}"
              for side, acc in (("source", s), ("target", t))
              for what, why in acc["unread"].items()
              if not (side == "source" and what == "acls"
                      and "SecurityDisabled" in why)]
    if sa and ta is None:
        unread.append(f"the source holds {len(sa)} ACL(s) and the target"
                      " checks none (no authorizer): every client there may"
                      " do anything")
    out["unread"] = unread
    if unread:
        out["result"] = "unknown"
    else:
        out["result"] = "pass" if not missing and not mech and not gone \
            else "gap"
    say(f"  mechanisms differ ({len(mech)}): {mech}")
    say(f"  ACLs missing on target ({len(gone)} of {len(sa or ())}):")
    for k in gone:
        say(f"    {_acl_said(k)}")
    for u in unread:
        say(f"  NOT COMPARED - {u}")
    say("  note: a SCRAM password is stored salted and cannot be read back;"
        " create sets the one given in the passwords file")


def _kafka_create(hop, apply, passwords, say):
    """Add the SCRAM users and ACLs the target lacks. A user's password
    comes from the passwords file (`user: password`) - the source cannot
    give it - and a user with none is skipped and said. ACLs are carried
    as they are, principal and pattern included, whether or not their
    principal is a SCRAM user (mTLS and IAM principals have ACLs too)."""
    from kafka.admin import ScramMechanism, UserScramCredentialUpsertion
    out, s = compare(hop, say)
    if out["result"] == "unknown":
        raise SystemExit("users: " + "; ".join(out["unread"]))
    su = s["users"] or {}
    wanted = [(u, m) for u in out["missing_on_target"] for m in su[u]]
    have = _kafka_accounts(hop, "dst")["users"] or {}
    wanted += [(u, m) for u in out["mechanisms_differ"] for m in su[u]
               if m not in have.get(u, [])]
    users = [(u, m) for u, m in wanted if (passwords or {}).get(u)]
    skipped = sorted({u for u, _ in wanted if not (passwords or {}).get(u)})
    acls = [tuple(k) for k in out["acls_missing"]]
    if not users and not acls:
        say("  nothing to create" + (f"; skipped {skipped}: no password"
                                     " given" if skipped else ""))
        return
    say(f">> kafka: create {len(users)} SCRAM credential(s), {len(acls)}"
        " ACL(s)" + ("" if apply else "  (nothing done; add --apply)"))
    for u, m in users:
        say(f"   user {u} ({m})")
    for k in acls:
        say(f"   acl {_acl_said(k)}")
    for u in skipped:
        say(f"   skipped {u}: no password in the passwords file, and the"
            " source cannot give one")
    if not apply:
        return
    admin = _kafka_admin(hop, "dst")
    made_users, made_acls, failed = [], [], []
    try:
        if users:
            said = admin.alter_user_scram_credentials([
                UserScramCredentialUpsertion(
                    u, ScramMechanism[m.replace("-", "_")], passwords[u])
                for u, m in users])
            for u, m in users:
                if said.get(u):
                    failed.append(f"user {u} ({m}): {said[u]}")
                else:
                    made_users.append([u, m])
        if acls:
            said = admin.create_acls([_acl_of(k) for k in acls])
            made = {_acl_key(a) for a in said["succeeded"]}
            made_acls = [list(k) for k in acls if k in made]
            failed += [f"acl: {type(e).__name__ if isinstance(e, type) else e}"
                       for e in said["failed"]]
    finally:
        admin.close()
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "kafka", "users": made_users, "acls": made_acls,
                 "failed": failed, "skipped": skipped})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    for f in failed:
        say(f"   FAILED {f}")
    say(f"  created {len(made_users)} credential(s), {len(made_acls)}"
        f" ACL(s); recorded at {rec} (undo with users {hop.name} rollback)")
    if failed:
        raise SystemExit(f"users: {len(failed)} could not be created")


def _kafka_rollback(hop, apply, say):
    """Remove exactly what `create` added: each ACL by its full key, so an
    ACL the target had of its own is never matched, and each credential
    by user and mechanism."""
    from kafka.admin import ScramMechanism, UserScramCredentialDeletion
    rec = hop.report_dir() / "user-sync-created.json"
    runs = [e for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "kafka"]
    users = [tuple(u) for e in runs for u in e.get("users", [])]
    acls = [tuple(k) for e in runs for k in e.get("acls", [])]
    if not users and not acls:
        say("  no kafka users or ACLs were created by us, nothing to undo")
        return
    for u, m in users:
        say(f"   {'delete' if apply else 'would delete'} user {u} ({m})")
    for k in acls:
        say(f"   {'delete' if apply else 'would delete'} acl {_acl_said(k)}")
    if not apply:
        return
    admin = _kafka_admin(hop, "dst")
    try:
        if acls:
            admin.delete_acls([_acl_of(k) for k in acls])
        if users:
            admin.alter_user_scram_credentials([
                UserScramCredentialDeletion(
                    u, ScramMechanism[m.replace("-", "_")])
                for u, m in users])
    finally:
        admin.close()
    rec.rename(str(rec) + ".rolled-back")


# --- sql server ------------------------------------------------------------
#: logins every server has, or the provider's on a managed one
MS_SYS = ("sa", "public", "sysadmin", "rdsa", "rdsadmin")


def _ms_conn(ep, db="master"):
    import pymssql
    return pymssql.connect(server=ep.host, port=int(ep.port or 1433),
                           user=ep.user, password=ep.password, database=db,
                           autocommit=True, login_timeout=15,
                           **ep.mssql_tls())


def _ms_rows(ep, db, sql):
    conn = _ms_conn(ep, db)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def _ms_system(name):
    return (name in MS_SYS or name.startswith(("##", "NT ", "BUILTIN\\"))
            or name.upper().startswith("NT "))


def _ms_principals(hop, side):
    """{"login:<name>" | "user:<db>.<name>": {...}} - each SQL login with
    its password hash and SID (the same SID is what keeps a database's user
    joined to its login), its server roles; each database user of the
    hop's databases with its login, roles and permissions."""
    ep = hop.source if side == "src" else hop.target
    out = {}
    for name, sid, pw, off, db_default, kind in _ms_rows(
            ep, "master", "select p.name, p.sid, l.password_hash,"
                          " p.is_disabled, p.default_database_name, p.type"
                          " from sys.server_principals p left join"
                          " sys.sql_logins l on l.principal_id ="
                          " p.principal_id where p.type in ('S', 'U', 'G')"):
        if _ms_system(name):
            continue
        out[f"login:{name}"] = {
            "kind": kind, "sid": bytes(sid).hex() if sid else "",
            "hash": bytes(pw).hex() if pw else "", "disabled": bool(off),
            "default_db": db_default, "roles": [], "grants": []}
    for role, member in _ms_rows(
            ep, "master", "select r.name, m.name from"
                          " sys.server_role_members rm join"
                          " sys.server_principals r on r.principal_id ="
                          " rm.role_principal_id join sys.server_principals"
                          " m on m.principal_id = rm.member_principal_id"):
        if f"login:{member}" in out:
            out[f"login:{member}"]["roles"].append(role)
    for db in hop.databases or []:
        name_db = hop.target_db(db) if side == "dst" else db
        users = {}
        for name, kind, sid, schema in _ms_rows(
                ep, name_db, "select name, type, sid, default_schema_name"
                             " from sys.database_principals where type in"
                             " ('S', 'U', 'G') and principal_id > 4"):
            if _ms_system(name) or name in ("dbo", "guest"):
                continue
            login = next((k[6:] for k, v in out.items()
                          if k.startswith("login:") and v["sid"] ==
                          (bytes(sid).hex() if sid else "")), None)
            users[name] = {"kind": kind, "login": login,
                           "schema": schema or "dbo", "roles": [],
                           "grants": []}
        for role, member in _ms_rows(
                ep, name_db, "select r.name, m.name from"
                             " sys.database_role_members rm join"
                             " sys.database_principals r on r.principal_id"
                             " = rm.role_principal_id join"
                             " sys.database_principals m on m.principal_id"
                             " = rm.member_principal_id"):
            if member in users:
                users[member]["roles"].append(role)
        for who, state, perm, cls, sch, obj in _ms_rows(
                ep, name_db, "select g.name, p.state_desc,"
                             " p.permission_name, p.class_desc,"
                             " coalesce(schema_name(o.schema_id),"
                             " schema_name(p.major_id)), o.name from"
                             " sys.database_permissions p join"
                             " sys.database_principals g on g.principal_id"
                             " = p.grantee_principal_id left join"
                             " sys.objects o on p.class = 1 and o.object_id"
                             " = p.major_id where p.permission_name <>"
                             " 'CONNECT'"):
            if who not in users:
                continue
            on = (f"[{sch}].[{obj}]" if cls == "OBJECT_OR_COLUMN" and obj
                  else f"SCHEMA::[{sch}]" if cls == "SCHEMA" else "")
            users[who]["grants"].append(
                f"{'DENY' if state == 'DENY' else 'GRANT'} {perm}"
                + (f" ON {on}" if on else ""))
        for name, v in users.items():
            v["roles"].sort()
            v["grants"].sort()
            out[f"user:{db}.{name}"] = v
    return out


def _ms_create(hop, apply, passwords, say):
    """Each missing login made with the source's password hash and SID
    (`PASSWORD = 0x... HASHED`), so a database user carried beside it is
    joined to it and signs in with the password it had; its server roles;
    then each database's users, their roles and their permissions."""
    out, s = compare(hop, say)
    stmts, made = [], []
    missing = out["missing_on_target"]
    for key in sorted(k for k in missing if k.startswith("login:")):
        v, name = s[key], key[6:]
        if v["kind"] == "S":
            if not v["hash"]:
                say(f"   skipped {name}: its password hash is not readable")
                continue
            head = (f"CREATE LOGIN [{name}] WITH PASSWORD = 0x{v['hash']}"
                    f" HASHED, SID = 0x{v['sid']}, CHECK_POLICY = OFF")
            if v["default_db"]:
                head += f", DEFAULT_DATABASE = [{v['default_db']}]"
        else:
            head = f"CREATE LOGIN [{name}] FROM WINDOWS"
        stmts.append(("master", key, head))
        if v["disabled"]:
            stmts.append(("master", None, f"ALTER LOGIN [{name}] DISABLE"))
        for role in v["roles"]:
            stmts.append(("master", None,
                          f"ALTER SERVER ROLE [{role}] ADD MEMBER [{name}]"))
    for key in sorted(k for k in set(missing) | set(out.get(
            "grants_differ", [])) if k.startswith("user:")):
        v = s[key]
        db, name = key[5:].split(".", 1)
        target = hop.target_db(db)
        if key in missing:
            how = (f"FOR LOGIN [{v['login']}]" if v["login"]
                   else "WITHOUT LOGIN")
            stmts.append((target, key, f"CREATE USER [{name}] {how} WITH"
                                       f" DEFAULT_SCHEMA = [{v['schema']}]"))
        for role in v["roles"]:
            stmts.append((target, None,
                          f"ALTER ROLE [{role}] ADD MEMBER [{name}]"))
        for g in v["grants"]:
            stmts.append((target, None, f"{g} TO [{name}]"))
    if not stmts:
        say("  nothing to create")
        return
    say(f">> sqlserver: {len(stmts)} statement(s)"
        + ("" if apply else "  (nothing done; add --apply)"))
    for db, _, st in stmts:
        say(f"   [{db}] " + (st.split(" WITH PASSWORD")[0]
                              + " WITH PASSWORD = <hash> HASHED ..."
                              if "HASHED" in st else st))
    if not apply:
        return
    failed = []
    for db, key, st in stmts:
        try:
            conn = _ms_conn(hop.target, db)
            try:
                conn.cursor().execute(st)
            finally:
                conn.close()
            if key:
                made.append(key)
        except Exception as e:  # noqa: BLE001 - recorded and said
            failed.append(f"{st.split(' WITH PASSWORD')[0][:80]}:"
                          f" {str(e).splitlines()[0][:120]}")
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "mssql", "created": made, "failed": failed})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    for f in failed:
        say(f"   FAILED {f}")
    say(f"  created {len(made)}; recorded at {rec}"
        f" (undo with users {hop.name} rollback)")
    if failed:
        raise SystemExit(f"users: {len(failed)} statement(s) failed")


def _ms_rollback(hop, apply, say):
    rec = hop.report_dir() / "user-sync-created.json"
    made = [k for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "mssql" for k in e.get("created", [])]
    if not made:
        say("  no sqlserver logins or users were created by us, nothing to"
            " undo")
        return
    # users before the logins they are joined to
    for key in sorted(made, key=lambda k: not k.startswith("user:")):
        if key.startswith("user:"):
            db, name = key[5:].split(".", 1)
            db, st = hop.target_db(db), f"DROP USER IF EXISTS [{name}]"
        else:
            db, st = "master", f"DROP LOGIN [{key[6:]}]"
        if not apply:
            say(f"   would run [{db}] {st}")
            continue
        conn = _ms_conn(hop.target, db)
        try:
            conn.cursor().execute(st)
        finally:
            conn.close()
        say(f"   [{db}] {st}")
    if apply:
        rec.rename(str(rec) + ".rolled-back")


# --- cassandra -------------------------------------------------------------
#: the role every cluster starts with, the provider's on a managed one
CS_SYS = ("cassandra",)


def _cs_engine(hop):
    from .engines.cassandra import CassandraEngine
    return CassandraEngine(hop)


def _cs_roles(hop, side):
    """{role: {login, superuser, member_of, hash, grants}} from the
    cluster's own auth tables: the salted hash is carried as it is, so a
    role keeps its password without anyone knowing it."""
    s = _cs_engine(hop)._session(side)
    out = {}
    for r in s.execute("select role, can_login, is_superuser, member_of,"
                       " salted_hash from system_auth.roles"):
        if r.role in CS_SYS:
            continue
        out[r.role] = {"login": bool(r.can_login),
                       "superuser": bool(r.is_superuser),
                       "member_of": sorted(r.member_of or []),
                       "hash": r.salted_hash, "grants": []}
    for r in s.execute("select role, resource, permissions from"
                       " system_auth.role_permissions"):
        if r.role in out:
            out[r.role]["grants"] += [f"{p} {r.resource}"
                                      for p in sorted(r.permissions or [])]
    for v in out.values():
        v["grants"].sort()
    return out


def _cs_resource(hop, resource):
    """A resource as a GRANT names it, the keyspace as the target calls
    it; None for one migkit does not carry (functions, beans)."""
    parts = str(resource).split("/")
    q = '"{}"'.format
    if parts[0] == "data":
        if len(parts) == 1:
            return "ALL KEYSPACES"
        ks = hop.target_db(parts[1]) if parts[1] in (hop.databases or []) \
            else parts[1]
        return (f"KEYSPACE {q(ks)}" if len(parts) == 2
                else f"TABLE {q(ks)}.{q(parts[2])}")
    if parts[0] == "roles":
        return "ALL ROLES" if len(parts) == 1 else f"ROLE {q(parts[1])}"
    return None


def _cs_create(hop, apply, say):
    """Each missing role made with the source's salted hash, login and
    superuser flags; then memberships; then permissions - by the
    statements the cluster takes, the keyspaces as the target calls
    them."""
    out, s = compare(hop, say)
    stmts, skipped, made = [], [], []
    q = '"{}"'.format
    for r in out["missing_on_target"]:
        with_ = [f"LOGIN = {str(s[r]['login']).lower()}",
                 f"SUPERUSER = {str(s[r]['superuser']).lower()}"]
        if s[r]["hash"]:
            with_.insert(0, "HASHED PASSWORD = '"
                         + s[r]["hash"].replace("'", "''") + "'")
        stmts.append((r, f"CREATE ROLE {q(r)} WITH " + " AND ".join(with_)))
    for r in out["missing_on_target"] + out.get("attributes_differ", []):
        for parent in s[r]["member_of"]:
            stmts.append((None, f"GRANT {q(parent)} TO {q(r)}"))
    for r in out["missing_on_target"] + out.get("grants_differ", []):
        for g in s[r]["grants"]:
            perm, resource = g.split(" ", 1)
            on = _cs_resource(hop, resource)
            if on is None:
                skipped.append(f"{r}: {g}")
                continue
            stmts.append((None, f"GRANT {perm} ON {on} TO {q(r)}"))
    if not stmts:
        say("  nothing to create")
        return
    say(f">> cassandra: {len(stmts)} statement(s)"
        + ("" if apply else "  (nothing done; add --apply)"))
    for _, st in stmts:
        say("   " + (st.split(" WITH ")[0] + " WITH HASHED PASSWORD ..."
                     if "HASHED PASSWORD" in st else st))
    for x in skipped:
        say(f"   skipped {x}: not a keyspace, table or role")
    if not apply:
        return
    session, failed = _cs_engine(hop)._session("dst"), []
    for role, st in stmts:
        try:
            session.execute(st)
            if role:
                made.append(role)
        except Exception as e:  # noqa: BLE001 - recorded and said
            failed.append(f"{st.split(' WITH ')[0]}:"
                          f" {str(e).splitlines()[0][:120]}")
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "cassandra", "created": made, "failed": failed,
                 "skipped": skipped})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    for f in failed:
        say(f"   FAILED {f}")
    say(f"  created {len(made)}; recorded at {rec}"
        f" (undo with users {hop.name} rollback)")
    if failed:
        raise SystemExit(f"users: {len(failed)} statement(s) failed")


def _cs_rollback(hop, apply, say):
    rec = hop.report_dir() / "user-sync-created.json"
    made = [r for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "cassandra" for r in e.get("created", [])]
    if not made:
        say("  no cassandra roles were created by us, nothing to undo")
        return
    session = _cs_engine(hop)._session("dst") if apply else None
    for r in made:
        if not apply:
            say(f"   would drop role {r}")
            continue
        session.execute('DROP ROLE IF EXISTS "{}"'.format(r))
        say(f"   dropped role {r}")
    if apply:
        rec.rename(str(rec) + ".rolled-back")


# --- clickhouse ------------------------------------------------------------
#: the account every server has, defined in its own configuration
CH_SYS = ("default",)


def _ch_engine(hop):
    from .engines.clickhouse import ClickHouseEngine
    return ClickHouseEngine(hop)


def _ch_access(hop, side):
    """{"user:<name>" | "role:<name>": {"create": statement, "grants":
    [statement, ...]}} from the server's own `SHOW CREATE` and `SHOW
    GRANTS`, asked to show secrets: where the server allows it
    (`display_secrets_in_show_and_select`), a password comes back as its
    hash and is carried without anyone knowing it."""
    eng = _ch_engine(hop)
    client = eng._client(side)
    shown = {"format_display_secrets_in_show_and_select": 1}
    out = {}
    try:
        for kind, table in (("role", "system.roles"),
                            ("user", "system.users")):
            for (name,) in client.query(
                    f"select name from {table} order by name").result_rows:
                if kind == "user" and name in CH_SYS:
                    continue
                q = eng._q(name)
                try:
                    create = client.command(f"show create {kind} {q}",
                                            settings=shown)
                except Exception:
                    create = client.command(f"show create {kind} {q}")
                grants = [r[0] for r in client.query(
                    f"show grants for {q}").result_rows]
                out[f"{kind}:{name}"] = {"create": str(create),
                                         "grants": sorted(grants)}
    finally:
        client.close()
    return out


def _ch_literal(text):
    return "'" + str(text).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _ch_create(hop, apply, passwords, say):
    """Roles first, then users, then every grant - each as the source's
    own statement. A user whose password the source did not show is
    given the one in the passwords file, or skipped and said."""
    import re
    out, s = compare(hop, say)
    passwords = passwords or {}
    todo, skipped = [], []
    for key in sorted(out["missing_on_target"],
                      key=lambda k: (not k.startswith("role:"), k)):
        kind, name = key.split(":", 1)
        stmt = s[key]["create"]
        m = re.search(r"IDENTIFIED WITH (\w+)(?! BY)(?=\s|$)", stmt)
        if kind == "user" and m and m.group(1) not in (
                "no_password", "ldap", "kerberos", "ssl_certificate",
                "ssh_key", "http"):
            if not passwords.get(name):
                skipped.append(name)
                continue
            stmt = (stmt[:m.end()] + f" BY {_ch_literal(passwords[name])}"
                    + stmt[m.end():])
        todo.append((key, stmt, s[key]["grants"]))
    for key in out.get("grants_differ", []):
        todo.append((key, None, [g for g in s[key]["grants"]]))
    if not todo:
        say("  nothing to create" + (f"; skipped {skipped}: no password"
                                     " given" if skipped else ""))
        return
    say(f">> clickhouse: {len(todo)} account(s) to create or grant"
        + ("" if apply else "  (nothing done; add --apply)"))
    for key, stmt, grants in todo:
        say(f"   {key}: {'grants only' if stmt is None else 'create'},"
            f" {len(grants)} grant(s)")
    for name in skipped:
        say(f"   skipped user {name}: its password is not shown by the"
            " source and none is in the passwords file")
    if not apply:
        return
    client = _ch_engine(hop)._client("dst")
    made, failed = [], []
    try:
        for key, stmt, grants in todo:
            try:
                if stmt is not None:
                    client.command(stmt)
                    made.append(key)
                for g in grants:
                    client.command(g)
            except Exception as e:  # noqa: BLE001 - recorded and said
                failed.append(f"{key}: {str(e).splitlines()[0][:120]}")
    finally:
        client.close()
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "clickhouse", "created": made, "failed": failed,
                 "skipped": skipped})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    for f in failed:
        say(f"   FAILED {f}")
    say(f"  created {len(made)}; recorded at {rec}"
        f" (undo with users {hop.name} rollback)")
    if failed:
        raise SystemExit(f"users: {len(failed)} could not be created")


def _ch_rollback(hop, apply, say):
    rec = hop.report_dir() / "user-sync-created.json"
    made = [k for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "clickhouse" for k in e.get("created", [])]
    if not made:
        say("  no clickhouse accounts were created by us, nothing to undo")
        return
    eng = _ch_engine(hop)
    client = eng._client("dst") if apply else None
    try:
        # users before the roles they were given
        for key in sorted(made, key=lambda k: (k.startswith("role:"), k)):
            kind, name = key.split(":", 1)
            if not apply:
                say(f"   would drop {kind} {name}")
                continue
            client.command(f"drop {kind} if exists {eng._q(name)}")
            say(f"   dropped {kind} {name}")
    finally:
        if client:
            client.close()
    if apply:
        rec.rename(str(rec) + ".rolled-back")


# --- redis -----------------------------------------------------------------
#: the account every Redis has, which the provider owns on a managed one
REDIS_SYS = ("default",)


def _redis_users(ep):
    """user -> {rules, hashes}, from `ACL LIST`.

    Each line is `user <name> <rule> <rule> ...`. The password rules
    (`#<sha256>`, `nopass`) are kept apart from the rest, so a user whose
    permissions match and whose password does not is told apart from one
    whose permissions differ.
    """
    client = _redis_client(ep)
    out = {}
    for line in client.acl_list():
        parts = line.split()
        if len(parts) < 2 or parts[0] != "user" or parts[1] in REDIS_SYS:
            continue
        rules = parts[2:]
        hashes = sorted(r for r in rules if r.startswith("#")
                        or r == "nopass")
        out[parts[1]] = {"rules": sorted(r for r in rules
                                         if r not in hashes),
                         "hashes": hashes, "line": rules}
    client.close()
    return out


# --- mongodb ---------------------------------------------------------------
# provider-created accounts that exist on one side only, not app accounts
MONGO_SYS = ("mongouser", "rwuser", "serviceadmin", "__system")
# roles that already cover every other role
MONGO_SUPER_ROLES = {"root", "__system"}
# provider-specific roles the target cannot create and does not need
MONGO_VENDOR_ROLES = {"index_stats", "restoreoplog", "readAnyDatabase_tencent"}


def _mongo_role_gap(src_roles, dst_roles):
    """Roles the source has and the target genuinely lacks.

    Provider-only roles are dropped, and a target that already has root covers
    the rest; otherwise this chases a gap that can never close."""
    if any(r in MONGO_SUPER_ROLES for r, _ in dst_roles):
        return set()
    ignore = MONGO_VENDOR_ROLES | {
        x.strip() for x in os.environ.get("MONGO_IGNORE_ROLES", "").split(",")
        if x.strip()}
    return {(r, d) for r, d in set(src_roles) - set(dst_roles) if r not in ignore}


def _mongo_sysuser(name):
    n = str(name)
    if n in MONGO_SYS or n.startswith(("cmgo-", "dds-", "mongo-")):
        return True
    extra = os.environ.get("MONGO_IGNORE_USERS", "")
    return n in {x.strip() for x in extra.split(",") if x.strip()}


def _mongo_client(ep):
    from urllib.parse import quote
    from pymongo import MongoClient
    # replica sets use options.hosts, single servers use host+port
    hosts = ep.options.get("hosts") or f"{ep.host}:{ep.port}"
    opts = ep.options.get("uri_options", "") or ""
    tls = ep.mongo_tls()
    if tls:
        from urllib.parse import urlencode
        opts = "&".join(p for p in (opts, urlencode(tls)) if p)
    uri = (f"mongodb://{quote(str(ep.user))}:{quote(str(ep.password))}"
           f"@{hosts}/?{opts}")
    return _retry(lambda: MongoClient(uri, serverSelectionTimeoutMS=15000))


def _mongo_users(ep):
    """(db, user) -> roles

    usersInfo needs viewUser, which some providers withhold while still allowing
    a direct read of admin.system.users. Try both so either side works."""
    c = _mongo_client(ep)
    out = {}
    try:
        for x in c.admin.command("usersInfo", {"forAllDBs": True})["users"]:
            out[(x["db"], x["user"])] = sorted(
                (r["role"], r["db"]) for r in x.get("roles", []))
    except Exception:
        for d in c.admin["system.users"].find():
            out[(d.get("db"), d.get("user"))] = sorted(
                (r["role"], r["db"]) for r in d.get("roles", []))
    c.close()
    return {k: v for k, v in out.items() if not _mongo_sysuser(k[1])}


def _mongo_create(hop, missing, role_diff, src, passwords, apply, say):
    """Create the users missing on the target.

    The original passwords cannot be copied: the source stores SCRAM (one-way)
    and DocumentDB refuses direct writes to system.users, so new ones are set at
    creation. They come from (1) a file passed with --passwords, (2) the
    MONGO_PW_<user> variable, or (3) a generated value written to a gitignored
    file so it can be moved into the secret store."""
    import secrets as _s
    made, secretsmap, skipped = [], {}, []
    c = _mongo_client(hop.target) if apply else None
    for db, user in missing:
        pw = (passwords or {}).get(user) or os.environ.get(f"MONGO_PW_{user}", "")
        if not pw:
            pw = _s.token_urlsafe(18)
            secretsmap[user] = pw
        roles = [{"role": r, "db": d} for r, d in src[(db, user)]]
        if not apply:
            say(f"   would create {db}.{user} roles={[r['role'] for r in roles]}")
            made.append({"db": db, "user": user, "roles": roles})
            continue
        try:
            c[db].command("createUser", user, pwd=pw, roles=roles)
            say(f"   created {db}.{user} roles={[r['role'] for r in roles]}")
            made.append({"db": db, "user": user, "roles": roles})
        except Exception as e:
            skipped.append(f"{db}.{user}: {str(e)[:80]}")
            say(f"   FAILED {db}.{user}: {str(e)[:90]}")
    tgt_now = _mongo_users(hop.target)
    for db, user in role_diff:
        gap = _mongo_role_gap(src[(db, user)], tgt_now.get((db, user), []))
        want = [{"role": r, "db": d} for r, d in sorted(gap)]
        if not want:
            continue
        if not apply:
            say(f"   would grant {db}.{user} -> {[r['role'] for r in want]}")
            continue
        try:
            c[db].command("grantRolesToUser", user, roles=want)
            say(f"   granted {db}.{user} {[r['role'] for r in want]}")
        except Exception as e:
            skipped.append(f"{db}.{user} roles: {str(e)[:80]}")
            say(f"   FAILED roles {db}.{user}: {str(e)[:90]}")
    if c:
        c.close()
    if secretsmap:
        f = hop.report_dir() / "mongo-new-passwords.txt"
        f.write_text("\n".join(f"{u}\t{p}" for u, p in secretsmap.items()) + "\n")
        try:
            os.chmod(f, 0o600)
        except OSError:
            pass
        say(f"   set new passwords for {len(secretsmap)} account(s) (the originals cannot be copied)")
        say(f"   passwords in {f} - move them into the app secret store, then delete the file")
    return made, skipped


def _plan(hop, out, s, passwords):
    plan, skipped, secrets = [], [], {}
    if hop.engine == "mysql":
        c = _mysql_conn(hop.source)
        cur = c.cursor()
        for key in out["missing_on_target"]:
            u, h = key.rsplit("@", 1)
            plugin, auth = s[(u, h)]
            if not auth:
                skipped.append((key, "source has empty auth string"))
                continue
            hexs = auth.encode("latin1").hex() if isinstance(auth, str) else auth.hex()
            stmts = [f"CREATE USER '{u}'@'{h}' IDENTIFIED WITH '{plugin}' AS 0x{hexs}"]
            show = [f"CREATE USER '{u}'@'{h}' IDENTIFIED WITH '{plugin}' AS 0x<hash>  (same password as source)"]
            cur.execute("show grants for %s@%s", (u, h))
            for (g,) in cur.fetchall():
                if not g.startswith("GRANT PROXY"):
                    stmts.append(g)
                    show.append(g[:110])
            plan.append((key, stmts, show))
        c.close()
    else:
        hashes = _pg_hashes(hop.source)
        for r in out["missing_on_target"]:
            a = s[r]
            secret, how = None, None
            if r in hashes:
                secret, how = hashes[r], "hash from source (same password)"
            elif r in passwords:
                secret, how = passwords[r], "from --passwords file"
            elif a["login"]:
                skipped.append((r, "no hash readable on source and no --passwords entry"))
                continue
            opts = ["LOGIN" if a["login"] else "NOLOGIN"]
            if a.get("superuser"):
                opts.append("SUPERUSER")
            if a["createdb"]:
                opts.append("CREATEDB")
            if a["createrole"]:
                opts.append("CREATEROLE")
            if a.get("replication"):
                opts.append("REPLICATION")
            if a.get("bypassrls"):
                opts.append("BYPASSRLS")
            if a.get("inherit") is False:
                opts.append("NOINHERIT")
            if a["connlimit"] not in (None, -1):
                opts.append(f"CONNECTION LIMIT {a['connlimit']}")
            if a.get("valid_until"):
                opts.append(f"VALID UNTIL '{a['valid_until']}'")
            if secret is not None:
                secrets[r] = secret
            if secret is None:
                stmts = [f'CREATE ROLE "{r}" {" ".join(opts)}']
                show = [f'CREATE ROLE "{r}" {" ".join(opts)}  (no password: nologin role)']
            else:
                stmts = [f'CREATE ROLE "{r}" {" ".join(opts)} PASSWORD %s']
                show = [f'CREATE ROLE "{r}" {" ".join(opts)} PASSWORD <{how}>']
            for m in a["member_of"]:
                stmts.append(f'GRANT "{m}" TO "{r}"')
                show.append(f'GRANT "{m}" TO "{r}"')
            plan.append((r, stmts, show))
    return plan, skipped, secrets


def _redis_client(ep):
    import redis
    return redis.Redis(host=ep.host, port=ep.port, username=ep.user or None,
                       password=ep.password or None, socket_timeout=15,
                       decode_responses=True, **ep.redis_tls())


def _redis_create(hop, apply, say):
    """Create the users the target lacks, exactly as the source has them.

    `ACL SETUSER` takes a password as its SHA-256 (`#...`), which is what
    `ACL LIST` shows, so each user lands with the password it had without
    anyone knowing it. Users already on the target are left as they are:
    their differences are reported, not overwritten.
    """
    out, s = compare(hop, say)
    missing = out["missing_on_target"]
    if not missing:
        say("  nothing to create; every source user is on the target")
        return
    say(f">> redis: create {len(missing)} user(s), passwords carried as"
        " their hashes" + ("" if apply else "  (nothing done; add --apply)"))
    for u in missing:
        shown = [r for r in s[u]["line"] if not r.startswith("#")]
        say(f"   {u}: {' '.join(shown)}")
    if not apply:
        return
    client = _redis_client(hop.target)
    made = []
    try:
        for u in missing:
            client.execute_command("ACL", "SETUSER", u, "reset",
                                   *s[u]["line"])
            made.append(u)
    finally:
        client.close()
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "redis", "created": made})
    json.dump(prev, open(rec, "w"), indent=2)
    say(f"  created {len(made)}; recorded at {rec}"
        f" (undo with users {hop.name} rollback)")


def _redis_rollback(hop, apply, say):
    rec = hop.report_dir() / "user-sync-created.json"
    made = [u for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "redis" for u in e.get("created", [])]
    if not made:
        say("  no redis users were created by us, nothing to undo")
        return
    client = _redis_client(hop.target) if apply else None
    try:
        for u in made:
            if not apply:
                say(f"   would delete {u}")
                continue
            client.execute_command("ACL", "DELUSER", u)
            say(f"   deleted {u}")
    finally:
        if client:
            client.close()


def create(hop, apply=False, passwords=None, say=print):
    if hop.engine == "redis":
        return _redis_create(hop, apply, say)
    if hop.engine == "kafka":
        return _kafka_create(hop, apply, passwords, say)
    if hop.engine == "clickhouse":
        return _ch_create(hop, apply, passwords, say)
    if hop.engine == "cassandra":
        return _cs_create(hop, apply, say)
    if hop.engine == "mssql":
        return _ms_create(hop, apply, passwords, say)
    if hop.engine in ("mongodb", "mongo"):
        out, s = compare(hop, say)
        t = _mongo_users(hop.target)
        missing = [k for k in s if k not in t]
        role_diff = [k for k in s if k in t and _mongo_role_gap(s[k], t[k])]
        if not missing and not role_diff:
            say("  nothing to create; users and roles already match")
            return
        say(f">> mongo: create {len(missing)} account(s), adjust roles on {len(role_diff)}"
            + ("" if apply else "  (nothing done; add --apply)"))
        made, skipped = _mongo_create(hop, missing, role_diff, s, passwords, apply, say)
        if not apply:
            say("  (dry-run: nothing created, so nothing recorded)")
            return
        rec = hop.report_dir() / "user-sync-created.json"
        prev = json.loads(rec.read_text()) if rec.exists() else []
        prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                     "engine": "mongodb", "created": made, "skipped": skipped})
        json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
        say(f"  recorded at {rec} (undo with users {hop.name} rollback)")
        return

    passwords = passwords or {}
    out, s = compare(hop, say)
    if not out["missing_on_target"]:
        say("nothing to create - target already has every non-system source user")
        return
    plan, skipped, secrets = _plan(hop, out, s, passwords)
    say(f"\nplan: create {len(plan)} users on TARGET ({'APPLY' if apply else 'dry-run'})")
    for key, _, show in plan:
        say(f"  -- {key}")
        for ln in show:
            say(f"     {ln}")
    for key, why in skipped:
        say(f"  skipped {key}: {why}")
    if not apply:
        say("\ndry-run only. Add --apply to execute on the target.")
        return
    conn = _mysql_conn(hop.target) if hop.engine == "mysql" else _pg_conn(hop.target)
    cur = conn.cursor()
    created, failed = [], []
    for key, stmts, _ in plan:
        try:
            for st in stmts:
                if hop.engine == "postgres" and "PASSWORD %s" in st:
                    cur.execute(st, (secrets.get(key, passwords.get(key)),))
                else:
                    cur.execute(st)
            created.append(key)
            say(f"  created {key}")
        except Exception as e:
            failed.append({"user": key, "error": f"{type(e).__name__}: {str(e)[:100]}"})
            say(f"  FAILED {key}: {type(e).__name__}: {str(e)[:100]}")
    conn.close()
    rf = hop.report_dir() / "user-sync-created.json"
    # append every run rather than overwrite: several creates must all be undoable
    prev = []
    if rf.exists():
        try:
            prev = json.load(open(rf)).get("created", [])
        except Exception:
            prev = []
    allc = list(dict.fromkeys(prev + created))
    rec = {"hop": hop.name, "engine": hop.engine,
           "applied": datetime.date.today().isoformat(),
           "created": allc, "created_this_run": created, "failed": failed}
    json.dump(rec, open(rf, "w"), indent=2, ensure_ascii=False)
    say(f"\ncreated={len(created)} failed={len(failed)}  record: {rf}")
    say(f"next: migkit users {hop.name} verify")


def mongo_setpw(hop, passwords, apply=False, say=print):
    """Overwrite passwords for users that already exist on the target.

    Used once the real passwords arrive from the secret store: MongoDB
    passwords cannot be carried to DocumentDB, so creation sets temporary ones."""
    if not passwords:
        say("  a password file is required: --passwords <file.yaml> (user: password)")
        return 1
    have = _mongo_users(hop.target)
    names = {u for _, u in have}
    c = _mongo_client(hop.target) if apply else None
    done = miss = 0
    for user, pw in passwords.items():
        if user not in names:
            say(f"   skip {user}: no such account on the target")
            miss += 1
            continue
        db = next(d for d, u in have if u == user)
        if not apply:
            say(f"   would set password: {db}.{user}")
            done += 1
            continue
        try:
            c[db].command("updateUser", user, pwd=str(pw))
            say(f"   password set for {db}.{user}")
            done += 1
        except Exception as e:
            miss += 1
            say(f"   FAILED {db}.{user}: {str(e)[:90]}")
    if c:
        c.close()
    say(f"  {done} set, {miss} not set"
        + ("" if apply else "  (dry-run; add --apply)"))
    return 1 if miss else 0


def _mongo_rollback(hop, apply, say):
    rec = hop.report_dir() / "user-sync-created.json"
    if not rec.exists():
        say("  nothing recorded as created, nothing to undo")
        return
    entries = [e for e in json.loads(rec.read_text())
               if e.get("engine") == "mongodb"]
    users = [(u["db"], u["user"]) for e in entries for u in e.get("created", [])]
    if not users:
        say("  no mongo users were created by us")
        return
    c = _mongo_client(hop.target) if apply else None
    for db, user in users:
        if not apply:
            say(f"   would drop {db}.{user}")
            continue
        try:
            c[db].command("dropUser", user)
            say(f"   dropped {db}.{user}")
        except Exception as e:
            say(f"   FAILED drop {db}.{user}: {str(e)[:80]}")
    if c:
        c.close()


def rollback(hop, apply=False, say=print):
    if hop.engine == "redis":
        return _redis_rollback(hop, apply, say)
    if hop.engine == "kafka":
        return _kafka_rollback(hop, apply, say)
    if hop.engine == "clickhouse":
        return _ch_rollback(hop, apply, say)
    if hop.engine == "cassandra":
        return _cs_rollback(hop, apply, say)
    if hop.engine == "mssql":
        return _ms_rollback(hop, apply, say)
    if hop.engine in ("mongodb", "mongo"):
        return _mongo_rollback(hop, apply, say)
    rf = hop.report_dir() / "user-sync-created.json"
    if not rf.exists():
        say(f"no created-record at {rf} - nothing this tool created here")
        return
    rec = json.load(open(rf))
    if not rec["created"]:
        say("record has no created users")
        return
    say(f"rollback plan ({'APPLY' if apply else 'dry-run'}): drop {len(rec['created'])} users created on {rec['applied']}")
    for key in rec["created"]:
        say(f"  DROP {key}")
    if not apply:
        say("dry-run only. Add --apply to execute.")
        return
    conn = _mysql_conn(hop.target) if rec["engine"] == "mysql" else _pg_conn(hop.target)
    cur = conn.cursor()
    for key in rec["created"]:
        try:
            if rec["engine"] == "mysql":
                u, h = key.rsplit("@", 1)
                cur.execute(f"DROP USER IF EXISTS '{u}'@'{h}'")
            else:
                try:
                    cur.execute(f'DROP ROLE IF EXISTS "{key}"')
                except Exception:
                    # PG refuses to drop a role that still holds privileges -> revoke across every db first
                    conn.rollback()
                    for db in (hop.databases or []):
                        try:
                            c2 = _pg_conn({**hop.target, "database": db})
                            c2.autocommit = True
                            k2 = c2.cursor()
                            k2.execute("select nspname from pg_namespace where nspname not like 'pg\\_%'"
                                       " and nspname <> 'information_schema'")
                            for (ns,) in k2.fetchall():
                                for obj in ("TABLES", "SEQUENCES", "FUNCTIONS"):
                                    try:
                                        k2.execute(f'REVOKE ALL ON ALL {obj} IN SCHEMA "{ns}" FROM "{key}"')
                                    except Exception:
                                        pass
                                try:
                                    k2.execute(f'REVOKE ALL ON SCHEMA "{ns}" FROM "{key}"')
                                except Exception:
                                    pass
                            try:
                                k2.execute(f'REVOKE ALL ON DATABASE "{db}" FROM "{key}"')
                            except Exception:
                                pass
                            c2.close()
                        except Exception:
                            pass
                    cur.execute(f'DROP ROLE IF EXISTS "{key}"')
            say(f"  dropped {key}")
        except Exception as e:
            say(f"  FAILED {key}: {type(e).__name__}: {str(e)[:100]}")
    conn.close()
    rf.rename(str(rf) + ".rolled-back")


def run(hop_name, mode, apply=False, pw_file="", say=print):
    hop = get_hop(hop_name)
    passwords = {}
    if pw_file:
        import yaml as _y
        passwords = _y.safe_load(open(pw_file)) or {}
    if mode == "create":
        create(hop, apply, passwords, say)
    elif mode == "setpw":
        if hop.engine not in ("mongodb", "mongo"):
            raise SystemExit("setpw: mongodb only (other engines can copy the hash)")
        raise SystemExit(mongo_setpw(hop, passwords, apply, say))
    elif mode == "rollback":
        rollback(hop, apply, say)
    else:
        out, _ = compare(hop, say)
        if mode == "verify":
            if out["result"] == "pass":
                say("VERIFY: pass")
            else:
                say("VERIFY: gap found")
                raise SystemExit(1)
