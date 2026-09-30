"""Login/user sync source -> target, keeping the same password.

mysql    : password hash copied from mysql.user (CREATE USER ... IDENTIFIED WITH
           <plugin> AS <hash>) -> the user keeps the exact same password and the
           plaintext is never read or shown. Grants replayed from SHOW GRANTS.
postgres : password hashes are not readable on managed sources (RDS locks
           pg_authid), so create takes a passwords file (yaml `role: password`);
           attributes and role memberships are copied from the source.

Cloud system accounts (`SYSTEM`) are excluded on both sides. Never writes to
the source. create/rollback are dry-run unless apply=True. Every apply writes
a record of each statement it ran under the hop's report dir, and rollback
undoes only what that record lists; create ends by comparing again, and a
gap it left is an error.

A name read off the source reaches a statement run on the target only
through that engine's own quoting, and a value only as a bound parameter:
a source role called `x" SUPERUSER --` is a role of that name on the target,
never a superuser (`tests/test_hostile_account_names_stay_names.py`).
"""
import json
import datetime
import hashlib
import os

from .config import get_hop

#: accounts and roles a server makes for itself, or its provider makes on a
#: managed one: never carried, never compared - (whole names, prefixes).
#: The one list `users`, the grant checks, `assess` and `freeze` ask
#: (`system_principal`, `system_principals_sql`)
SYSTEM = {
    "mysql": (frozenset({"mysql.sys", "mysql.session", "mysql.infoschema",
                         "mariadb.sys", "root"}),
              ("AWS_", "rds", "tencent", "cloudsql", "azure_")),
    "postgres": (frozenset({"postgres", "root"}),
                 ("pg_", "rds", "tencentdb", "cloudsql", "azure",
                  "alloydb")),
    "mssql": (frozenset({"sa", "public", "sysadmin", "rdsa", "rdsadmin",
                         "dbo", "guest", "sys", "INFORMATION_SCHEMA"}),
              ("##", "NT ", "BUILTIN\\")),
}
#: the whole names alone, as the MySQL grant check reads them
MYSQL_SYS = set(SYSTEM["mysql"][0]) | {
    "rdsadmin", "rds_superuser_role", "rdswriteforwarduser", "tencentroot",
    "tencentdba"}


def system_principal(engine, name):
    """Whether `name` is an account the server or its provider keeps for
    itself on `engine` (`SYSTEM`)."""
    whole, prefixes = SYSTEM.get(engine, (frozenset(), ()))
    n = str(name)
    if n in whole or n.startswith(prefixes):
        return True
    return engine == "mssql" and n.upper().startswith(("NT ", "BUILTIN\\"))


def system_principals_sql(engine, column):
    """`system_principal` as a SQL condition on `column`, for a catalogue
    query to leave them out where they are read: true for a system one."""
    whole, prefixes = SYSTEM.get(engine, (frozenset(), ()))

    def lit(text):
        return "'" + str(text).replace("'", "''") + "'"

    def like(prefix):
        return lit(prefix.replace("\\", "\\\\").replace("%", "\\%")
                   .replace("_", "\\_") + "%")
    parts = [f"{column} in ({', '.join(lit(n) for n in sorted(whole))})"]
    parts += [f"{column} like {like(p)}" for p in prefixes]
    return "(" + " or ".join(parts) + ")"


def _h(x):
    b = x if isinstance(x, bytes) else str(x or "").encode()
    return hashlib.md5(b, usedforsecurity=False).hexdigest()[:10]


def _mysql_sysuser(u):
    return system_principal("mysql", u)


def _pg_sysrole(r):
    return system_principal("postgres", r)


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


def _pg_conn(ep, dbname="postgres"):
    import psycopg2

    def _open():
        c = psycopg2.connect(host=ep.host, port=ep.port, user=ep.user,
                             password=ep.password, dbname=dbname,
                             connect_timeout=15, **ep.libpq_tls())
        c.autocommit = True
        return c
    return _retry(_open)


# --- quoting ---------------------------------------------------------------
#
# A statement is (text, args): every name in `text` through the engine's own
# quoting, every value in `args`. The PostgreSQL and MySQL drivers format
# `text` with `args` whenever args are given - an empty tuple too - so a
# `%` a name quoted for them holds is doubled, and their statements always
# run with their tuple. The other engines' statements run without args,
# so their text is not formatted and their quoting is the engine's alone.

def _pg_ident(name):
    """A PostgreSQL identifier, quoted as the engine quotes one: any
    character but NUL inside double quotes, a double quote doubled."""
    from .engines.postgres import PostgresEngine
    return PostgresEngine._quote_ident(name).replace("%", "%%")


def _ms_ident(name):
    """A SQL Server identifier in brackets, a closing bracket doubled -
    the engine's own quoting (`MSSQLEngine._q`)."""
    from .engines.mssql import MSSQLEngine
    return MSSQLEngine._q(name)


def _cs_ident(name):
    """A CQL identifier in double quotes, one doubled inside."""
    from .engines.cassandra import CassandraEngine
    return CassandraEngine._q(name)


def _ora_ident(name):
    """An Oracle identifier in double quotes. Oracle has no way to write a
    double quote or a NUL inside one, so a name holding either cannot be
    an Oracle name at all, and is refused rather than written."""
    text = str(name)
    if '"' in text or "\x00" in text:
        raise ValueError(f"not an Oracle name: {text!r}")
    return f'"{text}"'


def _hex(value):
    """Bytes (or a latin-1 string) as the hex digits of a literal: nothing
    but 0-9 and a-f reaches the statement."""
    b = value.encode("latin1") if isinstance(value, str) else bytes(value)
    return b.hex()


def _run_steps(cur, steps, say):
    """Each step (`_step`) run on its own - one that fails is recorded and
    the rest go on - into the ledger the record keeps: what ran, whether
    it took, and the statement that takes it back where one does."""
    ledger = []
    for st in steps:
        entry = {"key": st["key"], "sql": st["shown"], "undo": st["undo"],
                 "made": st["made"]}
        args = list(st["args"])
        if "secret" in st:
            args.insert(st.get("secret_at", len(args)), st["secret"])
        try:
            cur.execute(st["sql"], tuple(args))
            entry["ok"] = True
        except Exception as e:  # noqa: BLE001 - recorded and said
            entry["ok"] = False
            entry["error"] = f"{type(e).__name__}: {str(e).strip()[:160]}"
            say(f"   FAILED {st['shown']}: {entry['error']}")
        ledger.append(entry)
    return ledger


def _record(hop, engine, ledger, **more):
    """The ledger of one apply, appended to the hop's record of what
    `create` did - every run kept, so each can be undone."""
    rec = hop.report_dir() / "user-sync-created.json"
    prev = []
    if rec.exists():
        got = json.loads(rec.read_text())
        # the record's first shape, one dict per hop, kept as the run it was
        prev = got if isinstance(got, list) else [
            {"engine": got.get("engine"), "at": got.get("applied"),
             "created": got.get("created", []), "failed": got.get("failed")}]
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": engine,
                 "created": [e["key"] for e in ledger
                             if e["ok"] and e.get("made")],
                 "steps": ledger,
                 "failed": [f"{e['sql']}: {e['error']}" for e in ledger
                            if not e["ok"]], **more})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    return rec


# --- MySQL -----------------------------------------------------------------
#: what an account is created with besides its password, as mysql.user
#: keeps it
MY_ATTRS = ("account_locked", "password_expired", "ssl_type", "max_questions",
            "max_updates", "max_connections", "max_user_connections")


def _text(v):
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", "replace")
    return None if v is None else str(v)


def _mysql_users(ep):
    """{(user, host): {"plugin", "auth", "attrs", "ssl", "role",
    "default_roles", "grants"}} of every account that is not the server's
    own, read on one connection: mysql.user whole (its columns differ
    between MySQL and MariaDB, so each is taken by name), the default
    roles, and each account's own `SHOW GRANTS` - the server's rendering,
    role grants among them."""
    c = _mysql_conn(ep)
    try:
        from .variants import identify
        cur = c.cursor()
        cur.execute("select version(), @@version_comment")
        v, comment = cur.fetchone()
        # the brand as `variants` names it, the one place it is told
        maria = identify("mysql", {"version": _text(v), "version_comment":
                                   _text(comment)}).name == "mariadb"
        cur.execute("select * from mysql.user")
        names = [d[0].lower() for d in cur.description]
        out = {}
        for row in cur.fetchall():
            r = dict(zip(names, row))
            u, h = _text(r.get("user")), _text(r.get("host"))
            if system_principal("mysql", u):
                continue
            auth = r.get("authentication_string")
            if auth is None:
                auth = r.get("password")
            out[(u, h)] = {
                "plugin": _text(r.get("plugin")) or "",
                "auth": bytes(auth) if isinstance(auth, (bytes, bytearray))
                else _text(auth) or "",
                "attrs": {a: _text(r.get(a)) for a in MY_ATTRS if a in r},
                "ssl": {k: _text(r.get(k)) or "" for k in (
                    "ssl_cipher", "x509_issuer", "x509_subject")},
                "role": maria and _text(r.get("is_role")) == "Y",
                "default_roles": ([_text(r.get("default_role"))]
                                  if maria and r.get("default_role")
                                  else []),
                "maria": maria, "grants": []}
        if not maria:
            try:
                cur.execute("select user, host, default_role_user,"
                            " default_role_host from mysql.default_roles")
                for u, h, ru, rh in cur.fetchall():
                    key = (_text(u), _text(h))
                    if key in out:
                        out[key]["default_roles"].append(
                            (_text(ru), _text(rh)))
            except Exception:  # noqa: BLE001 - before 8.0: no roles
                pass
        for (u, h), v in out.items():
            v["default_roles"].sort()
            if v["role"]:
                cur.execute("show grants for %s", (u,))
            else:
                cur.execute("show grants for %s@%s", (u, h))
            # the USAGE line holds no privilege (before 5.7 it held the
            # password's hash): what an account holds is the rest
            v["grants"] = sorted(
                g for g in (_text(g) for (g,) in cur.fetchall())
                if not g.startswith("GRANT USAGE ON *.* TO "))
        return out
    finally:
        c.close()


def _pg_hashes(ep):
    """rolname -> password hash, or None where this account may not read
    them: RDS/Aurora block pg_authid; a PG where we are a real superuser
    does not, so the hash can be copied to the target without knowing the
    password. None is never read as "no passwords": it is "not
    compared"."""
    try:
        c = _pg_conn(ep)
    except Exception:  # noqa: BLE001 - said as not compared
        return None
    try:
        cur = c.cursor()
        cur.execute("select rolname, rolpassword from pg_authid where"
                    " rolpassword is not null")
        return {n: h for n, h in cur.fetchall() if not _pg_sysrole(n)}
    except Exception:  # noqa: BLE001 - not readable here
        return None
    finally:
        c.close()


#: a role's attributes, as pg_roles keeps them and CREATE ROLE sets them
PG_ATTRS = ("login", "superuser", "createdb", "createrole", "replication",
            "bypassrls", "inherit", "connlimit", "valid_until")


def _pg_roles(ep):
    """{role: attributes and `member_of` - the roles it is a member of,
    each with ` WITH ADMIN` where it may grant it on}."""
    c = _pg_conn(ep)
    try:
        cur = c.cursor()
        cur.execute("""select r.rolname, r.rolcanlogin, r.rolcreatedb,
                              r.rolcreaterole, r.rolconnlimit, r.rolsuper,
                              r.rolreplication, r.rolbypassrls, r.rolinherit,
                              r.rolvaliduntil,
                              array(select b.rolname || case when
                                           m.admin_option then ' WITH ADMIN'
                                           else '' end
                                    from pg_auth_members m join pg_roles b
                                      on m.roleid = b.oid
                                    where m.member = r.oid)
                       from pg_roles r""")
        d = {}
        for n, lg, cd, cr, cl, su, rep, byp, inh, valid, mo in cur.fetchall():
            if _pg_sysrole(n):
                continue
            d[n] = {"login": lg, "createdb": cd, "createrole": cr,
                    "connlimit": cl, "superuser": su, "replication": rep,
                    "bypassrls": byp, "inherit": inh,
                    "valid_until": str(valid) if valid else None,
                    "member_of": sorted(
                        m for m in mo if m.startswith("pg_")
                        or not _pg_sysrole(m.split(" WITH ")[0]))}
        return d
    finally:
        c.close()


# --- the one compare -------------------------------------------------------

def _grant_gaps(s, t, part="grants"):
    """(keys on both sides whose target lacks one of the source's `part`,
    keys whose target holds one the source does not)."""
    both = [k for k in s if k in t]
    short = sorted(k for k in both
                   if set(s[k].get(part) or ()) - set(t[k].get(part) or ()))
    extra = sorted(k for k in both
                   if set(t[k].get(part) or ()) - set(s[k].get(part) or ()))
    return short, extra


def _attr_gaps(s, t, attrs):
    return sorted(k for k in s if k in t and any(
        s[k].get(a) != t[k].get(a) for a in attrs))


def compare(hop, say=print, sides=None):
    """(result, source's accounts); the target's put in `sides["dst"]`
    where the caller gives a `sides` to keep them (`_compare`), so what
    carries the gap does not read the target again. The result has one
    shape for every engine: `missing_on_target`, `extra_on_target`,
    `attributes_differ`, `grants_missing`, `grants_extra`,
    `password_differs` (None where passwords are not compared) and
    `not_compared`; `result` is `pass`, `gap`, or `unknown` where
    something that could differ was not read. The engines it branches on
    are the ones `users` covers (`capabilities._users_engines` reads them
    here)."""
    eng = hop.engine
    notes, not_compared = [], []
    ad, gm, gx, pw = [], [], [], []
    if eng == "mysql":
        s = _mysql_users(hop.source)
        t = _mysql_users(hop.target)
        skeys = {f"{u}@{h}" for u, h in s}
        tkeys = {f"{u}@{h}" for u, h in t}
        both = [k for k in s if k in t]
        pw = sorted(f"{u}@{h}" for (u, h) in both
                    if _h(s[(u, h)]["auth"]) != _h(t[(u, h)]["auth"]))
        ad = sorted(f"{u}@{h}" for (u, h) in both if any(
            s[(u, h)][p] != t[(u, h)][p]
            for p in ("attrs", "ssl", "default_roles", "plugin")))
        gm, gx = [[f"{u}@{h}" for u, h in keys]
                  for keys in _grant_gaps(s, t)]
    elif eng == "postgres":
        s = _pg_roles(hop.source)
        t = _pg_roles(hop.target)
        skeys, tkeys = set(s), set(t)
        ad = _attr_gaps(s, t, PG_ATTRS)
        gm, gx = _grant_gaps(s, t, "member_of")
        sh = _pg_hashes(hop.source)
        th = _pg_hashes(hop.target) if sh else None
        if sh is None:
            pw = None
            notes.append("passwords not compared: the source hides"
                         " pg_authid (managed) - names, attributes and"
                         " memberships only; create needs --passwords")
        elif th is None and any(r in t for r in sh):
            pw = None
            not_compared.append("passwords: the source's hashes were read"
                                " and the target hides pg_authid, so"
                                " whether they match is not known")
        else:
            th = th or {}
            pw = sorted(r for r, h in sh.items() if r in th and th[r] != h)
    elif eng in ("mongodb", "mongo"):
        s = _mongo_users(hop.source)
        t = _mongo_users(hop.target)
        skeys = {f"{d}.{u}" for d, u in s}
        tkeys = {f"{d}.{u}" for d, u in t}
        pw = None
        # only roles the source has and the target lacks; a target superset
        # is not a gap
        gm = sorted(f"{d}.{u}" for (d, u) in s
                    if (d, u) in t and _mongo_role_gap(s[(d, u)], t[(d, u)]))
        notes.append("passwords cannot be copied from the source (SCRAM, and"
                     " DocumentDB blocks system.users) -> create sets new"
                     " ones")
    elif eng == "kafka":
        s = _kafka_accounts(hop, "src")
        t = _kafka_accounts(hop, "dst")
        skeys, tkeys = set(s["users"] or {}), set(t["users"] or {})
        pw = None
        notes.append("a SCRAM password is stored salted and cannot be read"
                     " back; create sets the one given in the passwords"
                     " file")
    elif eng == "clickhouse":
        s = _ch_access(hop, "src")
        t = _ch_access(hop, "dst")
        skeys, tkeys = set(s), set(t)
        pw = None
        gm, gx = _grant_gaps(s, t)
        notes.append("a password is carried as its hash where the source"
                     " shows it (display_secrets_in_show_and_select);"
                     " otherwise create sets the one given in the passwords"
                     " file")
    elif eng in ("opensearch", "elasticsearch"):
        s = _os_security(hop, "src")
        t = _os_security(hop, "dst")
        skeys, tkeys = set(s or {}), set(t or {})
        pw = None
        notes.append("the security API shows no password, not even as its"
                     " hash; create sets the one given in the passwords"
                     " file")
    elif eng == "cassandra":
        s = _cs_roles(hop, "src")
        t = _cs_roles(hop, "dst")
        skeys, tkeys = set(s), set(t)
        # the salted hash itself: one carried by `create` is the same one
        pw = sorted(r for r in s if r in t and s[r]["hash"] != t[r]["hash"])
        ad = _attr_gaps(s, t, ("login", "superuser", "member_of"))
        gm, gx = _grant_gaps(s, t)
    elif eng == "mssql":
        s = _ms_principals(hop, "src")
        t = _ms_principals(hop, "dst")
        skeys, tkeys = set(s), set(t)
        pw = sorted(k for k in s if k in t and s[k].get("hash")
                    and s[k].get("hash") != t[k].get("hash"))
        ad = _attr_gaps(s, t, MS_ATTRS)
        gm, gx = [sorted(set(a) | set(b)) for a, b in
                  zip(_grant_gaps(s, t, "roles"), _grant_gaps(s, t))]
    elif eng == "db2i":
        s = _ibmi_accounts(hop, "src")
        t = _ibmi_accounts(hop, "dst")
        skeys, tkeys = set(s), set(t)
        pw = None
        ad = _attr_gaps(s, t, ("status", "class", "special", "groups"))
        gm, gx = _grant_gaps(s, t)
        notes.append("IBM i shows no password or its hash; a profile is"
                     " created by its security officer with one of their"
                     " choosing")
    elif eng == "teradata":
        s = _td_accounts(hop, "src")
        t = _td_accounts(hop, "dst")
        skeys, tkeys = set(s), set(t)
        pw = None
        gm, gx = [sorted(set(a) | set(b)) for a, b in
                  zip(_grant_gaps(s, t, "roles"), _grant_gaps(s, t))]
        notes.append("Teradata shows no password or its hash; a user is"
                     " created with the one the passwords file gives it")
    elif eng == "redis":
        s = _redis_users(hop.source)
        t = _redis_users(hop.target)
        skeys, tkeys = set(s), set(t)
        # ACL LIST shows each password as its SHA-256 (`#...`), so a
        # password is compared, and later carried, without being read
        pw = sorted(u for u in s if u in t
                    and s[u]["hashes"] != t[u]["hashes"])
        # a user's rules are what it may do: its grants
        gm = sorted(u for u in s if u in t and s[u]["rules"] != t[u]["rules"])
    elif eng == "oracle":
        s = _ora_principals(hop, "src")
        t = _ora_principals(hop, "dst")
        skeys, tkeys = set(s), set(t)
        # compared only where both hashes could be read: one unreadable
        # is not one that differs, and is said below
        pw = sorted(k for k in s if k in t and s[k].get("hash")
                    and t[k].get("hash") and s[k]["hash"] != t[k]["hash"])
        gm = _ora_short(s, t)
        gx = _ora_short(t, s)
        hidden = sorted(k for k in s if k.startswith("user:")
                        and not s[k].get("hash"))
        if hidden:
            notes.append(f"password hash not readable with this account"
                         f" ({len(hidden)}): {hidden[:8]} - create sets the"
                         " one in the passwords file for these")
        blind = sorted(k for k in s if k in t and s[k].get("hash")
                       and not t[k].get("hash"))
        if blind:
            not_compared.append(
                f"passwords of {len(blind)} user(s): the target hides their"
                f" hashes ({blind[:8]})")
    else:
        from .capabilities import require
        require(eng, "users")
        raise SystemExit(f"users: engine {eng} not supported")
    missing = sorted(skeys - tkeys)
    extra = sorted(tkeys - skeys)
    out = {"check": "users", "hop": hop.name, "engine": eng,
           "generated": datetime.date.today().isoformat(),
           "excluded": "system and provider accounts (AWS_*, rds*,"
                       " tencent*, cloudsql*, azure*, the server's own)",
           "source_users": len(skeys), "target_users": len(tkeys),
           "missing_on_target": missing, "extra_on_target": extra,
           "attributes_differ": ad, "grants_missing": gm,
           "grants_extra": gx, "password_differs": pw,
           "not_compared": not_compared}
    out["result"] = ("unknown" if not_compared else "gap"
                     if missing or ad or gm or pw else "pass")
    say(f"hop={hop.name} engine={eng}  source={len(skeys)} target={len(tkeys)}")
    if eng == "kafka":
        _kafka_compared(out, s, t, missing, say)
    elif eng in ("opensearch", "elasticsearch"):
        _os_compared(hop, out, s, t, say)
    _said(out, say)
    for n in notes:
        say(f"  note: {n}")
    jf = hop.report_dir() / "users.json"
    json.dump(out, open(jf, "w"), indent=2, ensure_ascii=False)
    say(f"  json: {jf}")
    if sides is not None:
        sides["dst"] = t
    return out, s


def _said(out, say):
    """The result in one set of words for every engine."""
    for key, words in (("missing_on_target", "missing on target"),
                       ("extra_on_target", "extra on target"),
                       ("attributes_differ", "attributes differ"),
                       ("grants_missing", "grants or memberships missing on"
                                          " target"),
                       ("grants_extra", "grants or memberships only on"
                                        " target"),
                       ("password_differs", "password differs")):
        got = out.get(key)
        if got is None:
            say(f"  {words}: not compared")
        else:
            say(f"  {words} ({len(got)}): {got}")
    for why in out.get("not_compared") or []:
        say(f"  NOT COMPARED - {why}")


def _compare(hop, say=print):
    """(result, source's accounts, target's accounts) - `compare` with
    both sides kept."""
    sides = {}
    out, s = compare(hop, say, sides)
    return out, s, sides["dst"]


def _after(hop, say, apply, given=()):
    """What `create` ends with once it applied anything: the same compare
    again, and a gap it left an error rather than a line in its output.
    `given`: accounts this create gave the passwords file's password - by
    the operator's choice not the source's, so their passwords differing
    is said, not a gap."""
    if not apply:
        return
    say("  compared again after create:")
    out, _, _ = _compare(hop, say)
    chosen = sorted(set(out.get("password_differs") or ()) & set(given))
    if chosen:
        say(f"  set from the passwords file, not the source's ({len(chosen)}):"
            f" {chosen}")
        out["password_differs"] = [k for k in out["password_differs"]
                                   if k not in chosen]
    left = [f"{k.replace('_', ' ')}: {', '.join(map(str, v[:6]))}"
            for k, v in out.items()
            if k in ("missing_on_target", "attributes_differ",
                     "grants_missing", "password_differs") and v]
    left += out.get("not_compared") or []
    if left:
        raise SystemExit("users: the target still differs from the source"
                         " after create - " + "; ".join(left))


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
    """The users' mechanisms (their attributes) and the ACLs (their
    grants), on top of the user names. What a side would not list is a
    result of its own, never a pass."""
    su, tu = s["users"] or {}, t["users"] or {}
    mech = sorted(u for u in su if u in tu and set(su[u]) - set(tu[u]))
    sa, ta = s["acls"], t["acls"]
    gone = sorted(sa - ta) if sa and ta is not None else []
    extra = sorted(ta - sa) if ta and sa is not None else []
    out["attributes_differ"] = mech
    out["acls_missing"] = [list(k) for k in gone]
    out["grants_missing"] = [_acl_said(k) for k in gone]
    out["grants_extra"] = [_acl_said(k) for k in extra]
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
    out["not_compared"] = unread
    out["result"] = ("unknown" if unread else "gap"
                     if missing or mech or gone else "pass")


def _kafka_create(hop, apply, passwords, say):
    """Add the SCRAM users and ACLs the target lacks. A user's password
    comes from the passwords file (`user: password`) - the source cannot
    give it - and a user with none is skipped and said. ACLs are carried
    as they are, principal and pattern included, whether or not their
    principal is a SCRAM user (mTLS and IAM principals have ACLs too)."""
    from kafka.admin import ScramMechanism, UserScramCredentialUpsertion
    out, s, t = _compare(hop, say)
    if out["result"] == "unknown":
        raise SystemExit("users: " + "; ".join(out["not_compared"]))
    su = s["users"] or {}
    wanted = [(u, m) for u in out["missing_on_target"] for m in su[u]]
    have = t["users"] or {}
    wanted += [(u, m) for u in out["attributes_differ"] for m in su[u]
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
#: what is compared and carried of a login (its SID - the same SID is what
#: keeps a database's user joined to it -, whether it is disabled, its
#: default database and password policy) and of a database user (the login
#: it is joined to, its default schema)
MS_ATTRS = ("sid", "disabled", "default_db", "policy", "expiration",
            "login", "schema")

#: a securable's class (`sys.database_permissions.class`) as GRANT names
#: its kind; the database itself (0) and an object or column (1) are
#: written apart, a database principal (4) by the principal's own kind
MS_SECURABLES = {3: "SCHEMA", 5: "ASSEMBLY", 6: "TYPE",
                 10: "XML SCHEMA COLLECTION", 23: "FULLTEXT CATALOG",
                 24: "SYMMETRIC KEY", 25: "CERTIFICATE",
                 26: "ASYMMETRIC KEY"}
#: the classes whose securable lives in a schema
MS_IN_SCHEMA = (1, 6, 10)

#: each permission granted in a database, with its securable's names: the
#: object's schema and name and the column (`minor_id`) for class 1, the
#: principal's kind for class 4
MS_PERMISSIONS = (
    "select g.name, p.state_desc, p.permission_name, p.class,"
    " case p.class when 1 then object_schema_name(p.major_id)"
    " when 3 then schema_name(p.major_id)"
    " when 6 then (select schema_name(t.schema_id) from sys.types t where"
    " t.user_type_id = p.major_id)"
    " when 10 then (select schema_name(x.schema_id) from"
    " sys.xml_schema_collections x where x.xml_collection_id = p.major_id)"
    " end,"
    " case p.class when 1 then object_name(p.major_id)"
    " when 4 then (select d.name from sys.database_principals d where"
    " d.principal_id = p.major_id)"
    " when 5 then (select a.name from sys.assemblies a where a.assembly_id ="
    " p.major_id)"
    " when 6 then (select t.name from sys.types t where t.user_type_id ="
    " p.major_id)"
    " when 10 then (select x.name from sys.xml_schema_collections x where"
    " x.xml_collection_id = p.major_id)"
    " when 23 then (select f.name from sys.fulltext_catalogs f where"
    " f.fulltext_catalog_id = p.major_id)"
    " when 24 then (select k.name from sys.symmetric_keys k where"
    " k.symmetric_key_id = p.major_id)"
    " when 25 then (select c.name from sys.certificates c where"
    " c.certificate_id = p.major_id)"
    " when 26 then (select k.name from sys.asymmetric_keys k where"
    " k.asym_key_id = p.major_id)"
    " end,"
    " case when p.class = 1 and p.minor_id > 0 then col_name(p.major_id,"
    " p.minor_id) end,"
    " case when p.class = 4 then (select d.type from sys.database_principals"
    " d where d.principal_id = p.major_id) end,"
    " p.class_desc, p.major_id, p.minor_id"
    " from sys.database_permissions p join sys.database_principals g on"
    " g.principal_id = p.grantee_principal_id"
    " where not (p.class = 0 and p.permission_name = 'CONNECT')")


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
    return system_principal("mssql", name)


def _ms_securable(cls, sch, obj, col, ptype):
    """The securable of one permission as GRANT names it: "" for the
    database, `OBJECT::[s].[t] ([c])` for a column, `USER::[u]` or
    `ROLE::[r]` for a principal, `CERTIFICATE::[c]` and the rest by their
    class. None where its name could not be read or its class has no word
    here - such a permission is compared and listed, never granted."""
    q = _ms_ident
    if cls == 0:
        return ""
    if obj is None or (cls in MS_IN_SCHEMA and sch is None):
        return None
    if cls == 1:
        return (f"OBJECT::{q(sch)}.{q(obj)}"
                + (f" ({q(col)})" if col is not None else ""))
    if cls == 3:
        return f"SCHEMA::{q(obj if obj is not None else sch)}"
    if cls == 4:
        kind = {"R": "ROLE", "A": "APPLICATION ROLE"}.get(
            str(ptype or "").strip(), "USER")
        return f"{kind}::{q(obj)}"
    word = MS_SECURABLES.get(cls)
    if word is None:
        return None
    return (f"{word}::{q(sch)}.{q(obj)}" if cls in MS_IN_SCHEMA
            else f"{word}::{q(obj)}")


def _ms_permission(state, perm, on):
    """One permission as the GRANT or DENY that gives it, without its
    grantee: `WITH GRANT OPTION` kept after it. None for one that cannot
    be written (`_ms_securable`), or whose permission is not one of the
    server's words."""
    import re
    if on is None or not re.fullmatch(r"[A-Z][A-Z ]*", str(perm)):
        return None
    head = (f"{'DENY' if state == 'DENY' else 'GRANT'} {perm}"
            + (f" ON {on}" if on else ""))
    return head + (" WITH GRANT OPTION"
                   if state == "GRANT_WITH_GRANT_OPTION" else "")


def _ms_principals(hop, side):
    """{"login:<name>" | "user:<db>.<name>" | "role:<db>.<name>": {...}} -
    each SQL or Windows login with its password hash, SID, password policy
    and server roles; each database user and role of the hop's databases
    with its login, default schema, the roles it is a member of and every
    permission it holds, rendered by its class with the column a column
    grant is on and its grant option (`_ms_permission`)."""
    ep = hop.source if side == "src" else hop.target
    back = {hop.target_db(d): d for d in (hop.databases or [])} \
        if side == "dst" else {}
    out = {}
    sids = {}
    for name, sid, pw, off, db_default, kind, policy, expiry in _ms_rows(
            ep, "master", "select p.name, p.sid, l.password_hash,"
                          " p.is_disabled, p.default_database_name, p.type,"
                          " l.is_policy_checked, l.is_expiration_checked"
                          " from sys.server_principals p left join"
                          " sys.sql_logins l on l.principal_id ="
                          " p.principal_id where p.type in ('S', 'U', 'G')"):
        sid = bytes(sid).hex() if sid else ""
        sids[sid] = name
        if _ms_system(name):
            continue
        out[f"login:{name}"] = {
            "name": name, "kind": str(kind).strip(), "sid": sid,
            "hash": bytes(pw).hex() if pw else "", "disabled": bool(off),
            "default_db": back.get(db_default, db_default),
            "policy": None if policy is None else bool(policy),
            "expiration": None if expiry is None else bool(expiry),
            "roles": [], "grants": []}
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
        found = {}
        for name, kind, sid, schema in _ms_rows(
                ep, name_db, "select name, type, sid, default_schema_name"
                             " from sys.database_principals where type in"
                             " ('S', 'U', 'G', 'R') and principal_id > 4"
                             " and is_fixed_role = 0 and name <> 'public'"):
            if _ms_system(name):
                continue
            kind = str(kind).strip()
            sid = bytes(sid).hex() if sid else ""
            found[name] = {
                "name": name, "db": db, "kind": kind,
                "login": sids.get(sid) if kind != "R" else None,
                "schema": (schema or "dbo") if kind != "R" else None,
                "roles": [], "grants": [], "unwritten": []}
        for role, member in _ms_rows(
                ep, name_db, "select r.name, m.name from"
                             " sys.database_role_members rm join"
                             " sys.database_principals r on r.principal_id"
                             " = rm.role_principal_id join"
                             " sys.database_principals m on m.principal_id"
                             " = rm.member_principal_id"):
            if member in found:
                found[member]["roles"].append(role)
        for (who, state, perm, cls, sch, obj, col, ptype, cdesc, major,
             minor) in _ms_rows(ep, name_db, MS_PERMISSIONS):
            if who not in found:
                continue
            said = _ms_permission(state, perm,
                                  _ms_securable(cls, sch, obj, col, ptype))
            if said is None:
                # compared by what it is; never granted
                said = (f"{state} {perm} ON {cdesc} {sch or ''}.{obj or ''}"
                        f" #{major}.{minor}")
                found[who]["unwritten"].append(said)
            found[who]["grants"].append(said)
        for name, v in found.items():
            v["roles"].sort()
            v["grants"].sort()
            kind = "role" if v["kind"] == "R" else "user"
            out[f"{kind}:{db}.{name}"] = v
    return out


def _ms_grant_to(grant, who):
    """A permission `_ms_permission` rendered, given to `who`: the grantee
    before `WITH GRANT OPTION`."""
    tail = " WITH GRANT OPTION"
    if grant.endswith(tail):
        return f"{grant[:-len(tail)]} TO {who}{tail}"
    return f"{grant} TO {who}"


def _ms_revoke(grant, who):
    """The statement that takes a permission `_ms_grant_to` gave back: a
    REVOKE removes a GRANT and a DENY alike, and one given with its grant
    option goes with what was granted on from it."""
    tail = " WITH GRANT OPTION"
    cascade = grant.endswith(tail)
    body = grant[:-len(tail)] if cascade else grant
    body = body.split(" ", 1)[1]
    return f"REVOKE {body} FROM {who}" + (" CASCADE" if cascade else "")


def _ms_plan(hop, out, s, t):
    """Each statement `_ms_create` runs, in the order the server takes
    them: the logins, their server roles; then in each database its roles,
    its users, who is a member of which role, and the permissions. Each is
    {"db", "key", "sql", "shown", "undo", "made"}."""
    import re
    q = _ms_ident
    steps, skipped = [], []
    missing = set(out["missing_on_target"])
    differ = set(out["attributes_differ"]) | set(out["grants_missing"])

    def there(db):
        return hop.target_db(db) if db in (hop.databases or []) else db

    def step(db, sql, key=None, undo=None, shown=None, made=False):
        steps.append({"db": db, "key": key, "sql": sql,
                      "shown": shown or sql, "undo": undo, "made": made})

    for key in sorted(k for k in missing if k.startswith("login:")):
        v = s[key]
        name = q(v["name"])
        if v["kind"] == "S":
            if not v["hash"] or not re.fullmatch(r"[0-9a-f]+", v["hash"]) \
                    or not re.fullmatch(r"[0-9a-f]*", v["sid"]):
                skipped.append(f"{key}: its password hash is not readable")
                continue
            opts = [f"SID = 0x{v['sid']}"] if v["sid"] else []
            if v["default_db"]:
                opts.append(f"DEFAULT_DATABASE = {q(there(v['default_db']))}")
            opts.append("CHECK_POLICY ="
                        f" {'OFF' if v['policy'] is False else 'ON'}")
            opts.append("CHECK_EXPIRATION ="
                        f" {'ON' if v['expiration'] else 'OFF'}")
            head = f"CREATE LOGIN {name} WITH PASSWORD = 0x{v['hash']} HASHED"
            step("master", head + ", " + ", ".join(opts), key,
                 ["master", f"DROP LOGIN {name}"],
                 f"CREATE LOGIN {name} WITH PASSWORD = <hash> HASHED, "
                 + ", ".join(opts), made=True)
        else:
            head = f"CREATE LOGIN {name} FROM WINDOWS"
            if v["default_db"]:
                head += f" WITH DEFAULT_DATABASE = {q(there(v['default_db']))}"
            step("master", head, key, ["master", f"DROP LOGIN {name}"],
                 made=True)
        if v["disabled"]:
            step("master", f"ALTER LOGIN {name} DISABLE")
    for key in sorted(k for k in (missing | differ)
                      if k.startswith("login:")):
        if key in skipped or any(x.startswith(key + ":") for x in skipped):
            continue
        v, have = s[key], t.get(key, {})
        name = q(v["name"])
        for role in sorted(set(v["roles"]) - set(have.get("roles", ()))):
            step("master", f"ALTER SERVER ROLE {q(role)} ADD MEMBER {name}",
                 undo=None if key in missing else
                 ["master", f"ALTER SERVER ROLE {q(role)} DROP MEMBER"
                            f" {name}"])
        if key not in missing and have:
            if v["disabled"] != have.get("disabled"):
                now, was = (("DISABLE", "ENABLE") if v["disabled"]
                            else ("ENABLE", "DISABLE"))
                step("master", f"ALTER LOGIN {name} {now}",
                     undo=["master", f"ALTER LOGIN {name} {was}"])
            if v["kind"] == "S" and (v["policy"], v["expiration"]) != (
                    have.get("policy"), have.get("expiration")):
                step("master", f"ALTER LOGIN {name} WITH CHECK_POLICY ="
                               f" {'OFF' if v['policy'] is False else 'ON'},"
                               " CHECK_EXPIRATION ="
                               f" {'ON' if v['expiration'] else 'OFF'}")
            if v["sid"] != have.get("sid"):
                skipped.append(f"{key}: the target's login has another SID,"
                               " so the users joined to it on the source"
                               " are not joined to it there - drop and"
                               " create it again with the source's SID")
    principals = [k for k in s if k.startswith(("user:", "role:"))]
    for db in hop.databases or []:
        target = hop.target_db(db)
        here = sorted(k for k in principals if s[k]["db"] == db)
        # roles first: a user is made a member of one, and a role of another
        for key in sorted(here, key=lambda k: not k.startswith("role:")):
            if key not in missing:
                continue
            v = s[key]
            name = q(v["name"])
            if key.startswith("role:"):
                step(target, f"CREATE ROLE {name}", key,
                     [target, f"DROP ROLE IF EXISTS {name}"], made=True)
                continue
            how = (f"FOR LOGIN {q(v['login'])}" if v["login"]
                   else "WITHOUT LOGIN")
            step(target, f"CREATE USER {name} {how} WITH DEFAULT_SCHEMA ="
                         f" {q(v['schema'])}", key,
                 [target, f"DROP USER IF EXISTS {name}"], made=True)
        for key in here:
            if key not in missing and key not in differ:
                continue
            v, have = s[key], t.get(key, {})
            name = q(v["name"])
            if key not in missing and have and key.startswith("user:"):
                if v["schema"] != have.get("schema"):
                    step(target, f"ALTER USER {name} WITH DEFAULT_SCHEMA ="
                                 f" {q(v['schema'])}",
                         undo=[target, f"ALTER USER {name} WITH"
                                       f" DEFAULT_SCHEMA ="
                                       f" {q(have.get('schema') or 'dbo')}"])
                if v["login"] and v["login"] != have.get("login"):
                    step(target, f"ALTER USER {name} WITH LOGIN ="
                                 f" {q(v['login'])}")
            for role in sorted(set(v["roles"]) - set(have.get("roles", ()))):
                step(target, f"ALTER ROLE {q(role)} ADD MEMBER {name}",
                     undo=None if key in missing else
                     [target, f"ALTER ROLE {q(role)} DROP MEMBER {name}"])
            for g in sorted(set(v["grants"]) - set(have.get("grants", ()))):
                if g in v["unwritten"]:
                    skipped.append(f"{key}: {g} - its securable has no"
                                   " GRANT form here")
                    continue
                step(target, _ms_grant_to(g, name),
                     undo=None if key in missing else
                     [target, _ms_revoke(g, name)])
    return steps, skipped


def _ms_create(hop, apply, passwords, say):
    """Each missing login made with the source's password hash, SID and
    password policy (`PASSWORD = 0x... HASHED`), so a database user carried
    beside it is joined to it and signs in with the password it had; its
    server roles; then each database's roles and users, their memberships
    and their permissions - a column's on that column, one with its grant
    option with it (`_ms_plan`). Every statement goes into the record, with
    the one that undoes it."""
    out, s, t = _compare(hop, say)
    steps, skipped = _ms_plan(hop, out, s, t)
    for x in skipped:
        say(f"   skipped {x}")
    if not steps:
        say("  nothing to create")
        return
    say(f">> sqlserver: {len(steps)} statement(s)"
        + ("" if apply else "  (nothing done; add --apply)"))
    for st in steps:
        say(f"   [{st['db']}] {st['shown']}")
    if not apply:
        return
    ledger = []
    for st in steps:
        entry = {"key": st["key"], "db": st["db"], "sql": st["shown"],
                 "undo": st["undo"], "made": st["made"]}
        try:
            conn = _ms_conn(hop.target, st["db"])
            try:
                conn.cursor().execute(st["sql"])
            finally:
                conn.close()
            entry["ok"] = True
        except Exception as e:  # noqa: BLE001 - recorded and said
            entry["ok"] = False
            entry["error"] = str(e).splitlines()[0][:160]
            say(f"   FAILED [{st['db']}] {st['shown'][:80]}: {entry['error']}")
        ledger.append(entry)
    rec = _record(hop, "mssql", ledger, skipped=skipped)
    made = sum(1 for e in ledger if e["ok"] and e["made"])
    say(f"  created {made}, ran {sum(e['ok'] for e in ledger)} of"
        f" {len(ledger)}; recorded at {rec} (undo with users {hop.name}"
        " rollback)")
    if any(not e["ok"] for e in ledger):
        raise SystemExit(f"users: {sum(not e['ok'] for e in ledger)}"
                         " statement(s) failed")


def _ms_rollback(hop, apply, say):
    """Each statement `_ms_create` ran that has an undo, undone last
    first: a permission revoked, a membership dropped, then the users and
    roles, then the logins they were joined to."""
    rec = hop.report_dir() / "user-sync-created.json"
    runs = [e for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "mssql"]
    undo = [tuple(st["undo"]) for e in runs
            for st in (e.get("steps") or []) if st.get("ok") and
            st.get("undo")]
    # a record from before the ledger: what it created, users first
    for e in runs:
        if e.get("steps") is None:
            for key in sorted(e.get("created", []),
                              key=lambda k: k.startswith("user:")):
                if key.startswith("user:"):
                    db, name = key[5:].split(".", 1)
                    undo.append((hop.target_db(db), "DROP USER IF EXISTS"
                                                    f" {_ms_ident(name)}"))
                else:
                    undo.append(("master", f"DROP LOGIN {_ms_ident(key[6:])}"))
    if not undo:
        say("  no sqlserver logins or users were created by us, nothing to"
            " undo")
        return
    failed = []
    for db, st in reversed(undo):
        if not apply:
            say(f"   would run [{db}] {st}")
            continue
        try:
            conn = _ms_conn(hop.target, db)
            try:
                conn.cursor().execute(st)
            finally:
                conn.close()
            say(f"   [{db}] {st}")
        except Exception as e:  # noqa: BLE001 - said
            failed.append(st)
            say(f"   FAILED [{db}] {st}: {str(e).splitlines()[0][:120]}")
    if apply and not failed:
        rec.rename(str(rec) + ".rolled-back")


# --- IBM i -----------------------------------------------------------------

def _ibmi_engine(hop):
    from .engines.db2i import Db2iEngine
    return Db2iEngine(hop)


def _ibmi_accounts(hop, side):
    """{"profile:<name>" | "*PUBLIC": {...}} - each user profile IBM did
    not ship (created by `*IBM`, or named Q...), its class, special
    authorities, group and supplemental groups and status; and each one's
    object authority to every table of the hop's schemas, *PUBLIC's among
    them. IBM i shows no password or its hash, so none is compared."""
    from .engines.dbapi import MARK
    eng = _ibmi_engine(hop)
    db0 = (hop.databases or [""])[0]
    out = {}
    for name, status, klass, special, group, extra in eng._rows(
            side, db0, "select authorization_name, status, user_class_name,"
                       " special_authorities, group_profile_name,"
                       " supplemental_group_list from qsys2.user_info where"
                       " user_creator <> '*IBM' and authorization_name not"
                       " like 'Q%'"):
        out[f"profile:{str(name).strip()}"] = {
            "status": str(status or "").strip(),
            "class": str(klass or "").strip(),
            "special": sorted(str(special or "").split()),
            "groups": sorted(([str(group).strip()] if group and str(
                group).strip() not in ("", "*NONE") else [])
                + str(extra or "").split()),
            "grants": []}
    out.setdefault("*PUBLIC", {"status": "", "class": "", "special": [],
                               "groups": [], "grants": []})
    for db in hop.databases or []:
        schema = hop.target_db(db) if side == "dst" else db
        for obj, who, auth in eng._rows(
                side, db, "select system_object_name, authorization_name,"
                          " object_authority from qsys2.object_privileges"
                          " where object_type = '*FILE' and"
                          " system_object_schema = (select"
                          " system_schema_name from qsys2.sysschemas where"
                          f" schema_name = {MARK})",
                (schema.upper() if schema == schema.lower() else schema,)):
            key = "*PUBLIC" if str(who).strip() == "*PUBLIC" \
                else f"profile:{str(who).strip()}"
            if key in out:
                out[key]["grants"].append(
                    f"{str(auth).strip()} ON {db}.{str(obj).strip()}")
    for v in out.values():
        v["grants"].sort()
    return out


def _ibmi_create(hop, apply, say):
    """Each object authority a profile has on the source and lacks on the
    target, granted with GRTOBJAUT run through `QSYS2.QCMDEXC`; a profile
    missing on the target is listed for its security officer to create -
    IBM i shows no password to carry, and a profile's special authorities
    are theirs to decide."""
    import re
    out, s, t = _compare(hop, say)
    for key in out["missing_on_target"]:
        say(f"   create by hand (CRTUSRPRF): {key[8:]}"
            f" class {s[key]['class']}"
            + (f", special {' '.join(s[key]['special'])}"
               if s[key]["special"] else ""))
    # a CL command is one string: each name in it is a system name - what
    # an IBM i name is made of, never a parenthesis, a blank or a quote -
    # or the authority is not granted from here
    word = re.compile(r"[A-Z0-9_$#@*][A-Z0-9_$#@.*]{0,127}")
    cmds, odd = [], []
    for key, v in sorted(s.items()):
        if key not in t:
            continue
        who = "*PUBLIC" if key == "*PUBLIC" else key[8:]
        for g in sorted(set(v["grants"]) - set(t[key]["grants"])):
            auth, _, obj = g.partition(" ON ")
            db, _, name = obj.partition(".")
            lib = hop.target_db(db).upper()
            if not all(word.fullmatch(x) for x in (lib, name, who, auth)):
                odd.append(f"{g} to {who}")
                continue
            cmds.append((key, g, f"GRTOBJAUT OBJ({lib}/{name})"
                                 f" OBJTYPE(*FILE) USER({who}) AUT({auth})"))
    for x in odd:
        say(f"   not granted from here, a name in it is not a system name:"
            f" {x}")
    if not cmds:
        say("  no object authority to grant")
        return
    say(f">> ibm i: {len(cmds)} authority(ies)"
        + ("" if apply else "  (nothing done; add --apply)"))
    for _, _, cmd in cmds:
        say(f"   {cmd}")
    if not apply:
        return
    eng = _ibmi_engine(hop)
    db0 = (hop.databases or [""])[0]
    made, failed = [], []
    for key, g, cmd in cmds:
        try:
            conn = eng._connect("dst", db0)
            try:
                conn.cursor().execute("call qsys2.qcmdexc(?)", (cmd,))
                conn.commit()
            finally:
                conn.close()
            made.append(cmd)
        except Exception as e:  # noqa: BLE001 - recorded and said
            failed.append(f"{cmd}: {str(e).splitlines()[0][:120]}")
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "db2i", "created": made, "failed": failed})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    for f in failed:
        say(f"   FAILED {f}")
    say(f"  granted {len(made)}; recorded at {rec}"
        f" (undo with users {hop.name} rollback)")
    if failed:
        raise SystemExit(f"users: {len(failed)} authority(ies) not granted")


def _ibmi_rollback(hop, apply, say):
    """Each authority `_ibmi_create` granted, revoked (RVKOBJAUT)."""
    rec = hop.report_dir() / "user-sync-created.json"
    made = [k for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "db2i" for k in e.get("created", [])]
    if not made:
        say("  no IBM i authority was granted by us, nothing to undo")
        return
    eng = _ibmi_engine(hop)
    db0 = (hop.databases or [""])[0]
    for cmd in made:
        undo = cmd.replace("GRTOBJAUT", "RVKOBJAUT", 1)
        if not apply:
            say(f"   would run {undo}")
            continue
        conn = eng._connect("dst", db0)
        try:
            conn.cursor().execute("call qsys2.qcmdexc(?)", (undo,))
            conn.commit()
        finally:
            conn.close()
        say(f"   {undo}")
    if apply:
        rec.rename(str(rec) + ".rolled-back")


# --- Teradata ----------------------------------------------------------------
#
# The users and roles that hold a right on the hop's databases, the roles
# each is a member of, and each right as the GRANT that gives it - read from
# DBC's views of every right (`AllRightsV`, `AllRoleRightsV`, `RoleMembersV`).
# A right on the target is compared under the source's database name
# (`db_map`). Teradata shows no password or its hash: a user is created with
# the one the passwords file gives, and one without is listed, not made.

#: DBC's access right codes, as GRANT says each; a right with no code here
#: is compared, and listed rather than granted
TD_RIGHTS = {"R": "SELECT", "I": "INSERT", "U": "UPDATE", "D": "DELETE",
             "E": "EXECUTE", "EF": "EXECUTE FUNCTION",
             "PE": "EXECUTE PROCEDURE", "CT": "CREATE TABLE",
             "DT": "DROP TABLE", "CV": "CREATE VIEW", "DV": "DROP VIEW",
             "CM": "CREATE MACRO", "DM": "DROP MACRO",
             "PC": "CREATE PROCEDURE", "PD": "DROP PROCEDURE",
             "CF": "CREATE FUNCTION", "DF": "DROP FUNCTION",
             "CG": "CREATE TRIGGER", "DG": "DROP TRIGGER", "IX": "INDEX",
             "RF": "REFERENCES", "ST": "STATISTICS", "SH": "SHOW",
             "DP": "DUMP", "RS": "RESTORE", "CP": "CHECKPOINT"}
#: users and roles the system makes for itself
TD_SYS = {"DBC", "SYSLIB", "SYSUDTLIB", "SYSSPATIAL", "SYSBAR", "SYSJDBC",
          "SYSXML", "TD_SYSFNLIB", "TD_SYSXML", "TD_SYSGPL", "TD_SERVER_DB",
          "TDPUSER", "SYSTEMFE", "SYSADMIN", "SYS_CALENDAR", "TDSTATS",
          "TDQCD", "TDMAPS", "DBCMNGR", "CRASHDUMPS", "SQLJ", "TDWM",
          "LOCKLOGSHREDDER", "EXTERNAL_AP", "PUBLIC"}


def _td_engine(hop):
    from .engines.teradata import TeradataEngine
    return TeradataEngine(hop)


def _td_rows(hop, side, sql, args=()):
    eng = _td_engine(hop)
    return eng._rows(side, (hop.databases or [""])[0], sql, args)


def _td_q(name):
    return '"' + str(name).replace('"', '""') + '"'


def _td_grant(code, db, table, column):
    """A right as the GRANT that gives it, on the source's database name;
    None for a code GRANT has no word for here."""
    word = TD_RIGHTS.get(str(code).strip())
    if word is None:
        return None
    on = _td_q(db) + ("" if str(table).strip() in ("All", "") else
                      "." + _td_q(str(table).strip()))
    col = str(column or "").strip()
    if col and col != "All" and word in ("SELECT", "UPDATE", "REFERENCES"):
        word += f" ({_td_q(col)})"
    return f"GRANT {word} ON {on}"


def _td_accounts(hop, side):
    """{"user:<name>" | "role:<name>": {"grants", "roles", "codes"}} of every
    user and role holding a right on the hop's databases, with the roles
    each user is a member of."""
    dbs = {(hop.target_db(d) if side == "dst" else d).upper(): d
           for d in hop.databases or []}
    if not dbs:
        raise SystemExit("users: a Teradata hop names its databases")
    marks = ", ".join("?" for _ in dbs)
    out = {}

    def entry(kind, name):
        return out.setdefault(f"{kind}:{name}", {"grants": [], "roles": [],
                                                 "unmapped": []})
    for who, db, table, col, code in _td_rows(
            hop, side, "select UserName, DatabaseName, TableName, ColumnName,"
                       " AccessRight from DBC.AllRightsV where"
                       f" upper(DatabaseName) in ({marks})", tuple(dbs)):
        who = str(who).strip()
        if who.upper() in TD_SYS or who.upper() in dbs:
            continue
        g = _td_grant(code, dbs[str(db).strip().upper()], table, col)
        e = entry("user", who)
        (e["grants"] if g else e["unmapped"]).append(
            g or f"{str(code).strip()} ON {str(db).strip()}")
    for role, db, table, col, code in _td_rows(
            hop, side, "select RoleName, DatabaseName, TableName,"
                       " ColumnName, AccessRight from DBC.AllRoleRightsV"
                       f" where upper(DatabaseName) in ({marks})",
            tuple(dbs)):
        g = _td_grant(code, dbs[str(db).strip().upper()], table, col)
        e = entry("role", str(role).strip())
        (e["grants"] if g else e["unmapped"]).append(
            g or f"{str(code).strip()} ON {str(db).strip()}")
    roles = {k[5:] for k in out if k.startswith("role:")}
    for role, grantee in _td_rows(hop, side, "select RoleName, Grantee from"
                                             " DBC.RoleMembersV"):
        role, grantee = str(role).strip(), str(grantee).strip()
        if role in roles and grantee.upper() not in TD_SYS:
            entry("user", grantee)["roles"].append(role)
    for v in out.values():
        v["grants"] = sorted(set(v["grants"]))
        v["roles"] = sorted(set(v["roles"]))
        v["unmapped"] = sorted(set(v["unmapped"]))
    return out


def _td_create(hop, apply, passwords, say):
    """Each role missing on the target made, each user made with the
    password the passwords file gives it (Teradata shows none to carry),
    then each role membership and each right the target lacks granted."""
    out, s, t = _compare(hop, say)
    stmts = []
    for key in sorted(k for k in out["missing_on_target"]
                      if k.startswith("role:")):
        stmts.append((key, f"CREATE ROLE {_td_q(key[5:])}"))
    for key in sorted(k for k in out["missing_on_target"]
                      if k.startswith("user:")):
        name = key[5:]
        pw = (passwords or {}).get(name)
        if not pw:
            say(f"   skipped user {name}: Teradata shows no password, and"
                " the passwords file gives none")
            continue
        stmts.append((key, f"CREATE USER {_td_q(name)} AS PERM = 0,"
                           f" PASSWORD = {_td_q(pw)}"))
    for key, v in sorted(s.items()):
        have = t.get(key, {"grants": [], "roles": []})
        who = _td_q(key.split(":", 1)[1])
        for role in sorted(set(v["roles"]) - set(have["roles"])):
            stmts.append((None, f"GRANT {_td_q(role)} TO {who}"))
        for g in sorted(set(v["grants"]) - set(have["grants"])):
            word, _, on = g[len("GRANT "):].partition(" ON ")
            db, dot, rest = on.partition(".")
            mapped = _td_q(hop.target_db(db.strip('"'))) + dot + rest
            stmts.append((None, f"GRANT {word} ON {mapped} TO {who}"))
        for u in v.get("unmapped", []):
            say(f"   not granted, no GRANT word for it here: {u} to {who}")
    if not stmts:
        say("  nothing to create")
        return
    say(f">> teradata: {len(stmts)} statement(s)"
        + ("" if apply else "  (nothing done; add --apply)"))
    for _, st in stmts:
        say("   " + (st.split(", PASSWORD")[0] + ", PASSWORD = <from the"
                     " passwords file>" if "PASSWORD" in st else st))
    if not apply:
        return
    eng = _td_engine(hop)
    db0 = (hop.databases or [""])[0]
    made, failed = [], []
    for key, st in stmts:
        try:
            eng.neutral_create_code("dst", db0, st)
            if key:
                made.append(key)
        except Exception as e:  # noqa: BLE001 - recorded and said
            failed.append(f"{st.split(', PASSWORD')[0][:80]}:"
                          f" {str(e).splitlines()[0][:120]}")
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "teradata", "created": made, "failed": failed})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    for f in failed:
        say(f"   FAILED {f}")
    say(f"  created {len(made)}; recorded at {rec}"
        f" (undo with users {hop.name} rollback)")
    if failed:
        raise SystemExit(f"users: {len(failed)} statement(s) failed")


def _td_rollback(hop, apply, say):
    """Each user and role `_td_create` made, dropped - the users first."""
    rec = hop.report_dir() / "user-sync-created.json"
    made = [k for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "teradata" for k in e.get("created", [])]
    if not made:
        say("  no teradata users or roles were created by us, nothing to"
            " undo")
        return
    eng = _td_engine(hop)
    db0 = (hop.databases or [""])[0]
    for key in sorted(made, key=lambda k: not k.startswith("user:")):
        kind, name = key.split(":", 1)
        st = f"DROP {'USER' if kind == 'user' else 'ROLE'} {_td_q(name)}"
        if not apply:
            say(f"   would run {st}")
            continue
        eng.neutral_create_code("dst", db0, st)
        say(f"   {st}")
    if apply:
        rec.rename(str(rec) + ".rolled-back")


# --- oracle ----------------------------------------------------------------
#
# Users and roles are the pluggable database's: every one the database did
# not make for itself (`oracle_maintained = 'N'`). A user the hop moves as a
# schema is the same user on both sides under the hop's `db_map`, and so
# is an object a grant names. A password is carried as the hash the server
# keeps, where the account may read it; the plain password never passes.


def _ora_engine(hop):
    from .engines.oracle import OracleEngine
    return OracleEngine(hop)


def _ora_rows(hop, side, sql, args=None):
    eng = _ora_engine(hop)
    return eng._rows(side, (hop.databases or [""])[0], sql, args)


def _ora_said(hop, side, name):
    """A target's name as the source calls it: the schema the hop maps
    `app` to is `app` on both sides."""
    if side != "dst" or not name:
        return name
    back = {str(v).upper(): str(k).upper()
            for k, v in (hop.db_map or {}).items()}
    return back.get(str(name).upper(), name)


def _ora_there(hop, name):
    """A source's name as the target calls it."""
    return {str(k).upper(): str(v).upper()
            for k, v in (hop.db_map or {}).items()}.get(str(name).upper(),
                                                          name)


def _ora_hash(hop, side, name):
    """The password's hash as the server keeps it (`S:...;T:...`), or ""
    where this account may not read it. Measured on 23.26: `sys.user$`
    refuses even SYSTEM unless it was given READ on it (ORA-41900), and a
    user's DDL holds the hash for SYS alone - an account with the export
    roles gets it without one."""
    import re
    try:
        got = _ora_rows(hop, side, "select spare4 from sys.user$ where name"
                                   " = :1", (name,))
        if got and got[0][0]:
            return str(got[0][0])
    except Exception:  # noqa: BLE001 - not this way
        pass
    try:
        got = _ora_rows(hop, side, "select dbms_metadata.get_ddl('USER',"
                                   " :1) from dual", (name,))
        m = re.search(r"IDENTIFIED BY VALUES '([^']+)'",
                      str(got[0][0]) if got else "")
        return m.group(1) if m else ""
    except Exception:  # noqa: BLE001 - not readable
        return ""


def _ora_principals(hop, side):
    """{"user:NAME" | "role:NAME": {...}}: each user with its password's
    hash, lock, profile, default tablespace and quotas, each role, and what
    each was granted - roles, system privileges and object privileges, an
    object by the name the source gives it."""
    out = {}
    for name, status, profile, space in _ora_rows(
            hop, side, "select username, account_status, profile,"
                       " default_tablespace from dba_users where"
                       " oracle_maintained = 'N'"):
        out[f"user:{_ora_said(hop, side, name)}"] = {
            "kind": "user", "name": name,
            "locked": "LOCKED" in str(status), "profile": profile,
            "tablespace": space, "hash": _ora_hash(hop, side, name),
            "roles": [], "privileges": [], "grants": [], "quotas": []}
    for (name,) in _ora_rows(hop, side, "select role from dba_roles where"
                                        " oracle_maintained = 'N'"):
        out[f"role:{name}"] = {"kind": "role", "name": name, "roles": [],
                               "privileges": [], "grants": []}
    by_name = {v["name"]: k for k, v in out.items()}
    for who, role, admin in _ora_rows(hop, side, "select grantee,"
                                                 " granted_role, admin_option"
                                                 " from dba_role_privs"):
        if who in by_name:
            out[by_name[who]]["roles"].append(
                role + (" WITH ADMIN OPTION" if admin == "YES" else ""))
    for who, priv, admin in _ora_rows(hop, side, "select grantee, privilege,"
                                                 " admin_option from"
                                                 " dba_sys_privs"):
        if who in by_name:
            out[by_name[who]]["privileges"].append(
                priv + (" WITH ADMIN OPTION" if admin == "YES" else ""))
    for who, owner, obj, priv, grantable in _ora_rows(
            hop, side, "select grantee, owner, table_name, privilege,"
                       " grantable from dba_tab_privs where grantee in"
                       " (select username from dba_users where"
                       " oracle_maintained = 'N' union all select role"
                       " from dba_roles where oracle_maintained = 'N')"):
        if who in by_name:
            out[by_name[who]]["grants"].append(
                f"{priv} ON {_ora_ident(_ora_said(hop, side, owner))}"
                f".{_ora_ident(obj)}"
                + (" WITH GRANT OPTION" if grantable == "YES" else ""))
    for who, space, most in _ora_rows(hop, side, "select username,"
                                                 " tablespace_name, max_bytes"
                                                 " from dba_ts_quotas"):
        if who in by_name:
            out[by_name[who]]["quotas"].append(
                f"{'UNLIMITED' if int(most) < 0 else int(most)} ON"
                f" {_ora_ident(space)}")
    for v in out.values():
        for part in ("roles", "privileges", "grants", "quotas"):
            if part in v:
                v[part].sort()
    return out


def _ora_short(s, t):
    """Keys on both sides where the target lacks a role, privilege, grant
    or quota the source has."""
    return sorted(k for k in s if k in t and any(
        set(s[k].get(p, [])) - set(t[k].get(p, []))
        for p in ("roles", "privileges", "grants", "quotas")))


#: a privilege's words, as Oracle names every system and object privilege
_ORA_WORDS = r"[A-Z][A-Z ]*[A-Z]"


def _ora_create(hop, apply, passwords, say):
    """Each missing role, then each missing user - with the source's
    password hash (`IDENTIFIED BY VALUES`), or the one in the passwords
    file where the hash is not readable - its lock, profile and tablespace
    where the target has them, its quotas; then every role, privilege and
    grant the target lacks, an object by the target's own schema. Every
    name through `_ora_ident`; a privilege only if it is Oracle's words."""
    import re
    out, s, t = _compare(hop, say)
    spaces = {n for (n,) in _ora_rows(hop, "dst", "select tablespace_name"
                                                  " from dba_tablespaces")}
    profiles = {n for (n,) in _ora_rows(hop, "dst", "select distinct"
                                                    " profile from"
                                                    " dba_profiles")}
    q = _ora_ident
    stmts, skipped = [], []
    missing = out["missing_on_target"]
    for key in sorted(k for k in missing if k.startswith("role:")):
        stmts.append((key, f"CREATE ROLE {q(s[key]['name'])}"))
    for key in sorted(k for k in missing if k.startswith("user:")):
        v = s[key]
        name = _ora_there(hop, v["name"])
        secret = (passwords or {}).get(v["name"]) or \
            (passwords or {}).get(v["name"].lower())
        if v["hash"] and re.fullmatch(r"[0-9A-Za-z:;]+", v["hash"]):
            how = f"IDENTIFIED BY VALUES '{v['hash']}'"
        elif secret and '"' not in str(secret):
            how = f"IDENTIFIED BY {q(secret)}"
        else:
            skipped.append(key)
            say(f"   skipped {key}: its password hash is not readable here"
                " and the passwords file has none for it that Oracle takes"
                " (no double quote)")
            continue
        head = f"CREATE USER {q(name)} {how}"
        if v["tablespace"] in spaces:
            head += f" DEFAULT TABLESPACE {q(v['tablespace'])}"
        if v["profile"] in profiles:
            head += f" PROFILE {q(v['profile'])}"
        if v["locked"]:
            head += " ACCOUNT LOCK"
        stmts.append((key, head))
    for key in sorted(set(missing) | set(out["grants_missing"])):
        if key in skipped or key not in s:
            continue
        v = s[key]
        who = q(_ora_there(hop, v["name"]))
        have = t.get(key, {})
        for quota in v.get("quotas", []):
            if quota not in have.get("quotas", []):
                stmts.append((None, f"ALTER USER {who} QUOTA {quota}"))
        for role in v["roles"]:
            if role not in have.get("roles", []):
                bare, _, admin = role.partition(" WITH ")
                stmts.append((None, f"GRANT {q(bare)} TO {who}"
                              + (f" WITH {admin}" if admin else "")))
        for priv in v["privileges"]:
            if priv not in have.get("privileges", []):
                bare, _, admin = priv.partition(" WITH ")
                if not re.fullmatch(_ORA_WORDS, bare):
                    say(f"   not granted, not a privilege's name: {priv}")
                    continue
                stmts.append((None, f"GRANT {bare} TO {who}"
                              + (f" WITH {admin}" if admin else "")))
        for grant in v["grants"]:
            if grant not in have.get("grants", []):
                word, _, on = grant.partition(" ON ")
                owner, _, rest = on.partition(".")
                if not re.fullmatch(_ORA_WORDS, word):
                    say(f"   not granted, not a privilege's name: {grant}")
                    continue
                there = f"{q(_ora_there(hop, owner[1:-1]))}.{rest}"
                stmts.append((None, f"GRANT {word} ON {there} TO {who}"))
    if not stmts:
        say("  nothing to create")
        return
    say(f">> oracle: {len(stmts)} statement(s)"
        + ("" if apply else "  (nothing done; add --apply)"))
    for _, st in stmts:
        say("   " + (st.split(" IDENTIFIED BY ")[0]
                     + " IDENTIFIED BY <hash or password> ..."
                     if " IDENTIFIED BY " in st else st))
    if not apply:
        return
    eng = _ora_engine(hop)
    made, failed = [], []
    for key, st in stmts:
        try:
            conn = eng._connect("dst", (hop.databases or [""])[0])
            try:
                conn.cursor().execute(st)
            finally:
                conn.close()
            if key:
                made.append(key)
        except Exception as e:  # noqa: BLE001 - recorded and said
            failed.append(f"{st.split(' IDENTIFIED BY ')[0][:80]}:"
                          f" {str(e).splitlines()[0][:120]}")
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "oracle", "created": made, "failed": failed})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    for f in failed:
        say(f"   FAILED {f}")
    say(f"  created {len(made)}; recorded at {rec}"
        f" (undo with users {hop.name} rollback)")
    if failed:
        raise SystemExit(f"users: {len(failed)} statement(s) failed")


def _ora_rollback(hop, apply, say):
    """Drops what `create` made and nothing else: users first, then roles.
    A user who owns objects by now is not dropped with them - the drop is
    refused, and said."""
    rec = hop.report_dir() / "user-sync-created.json"
    made = [k for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "oracle" for k in e.get("created", [])]
    if not made:
        say("  no oracle users or roles were created by us, nothing to"
            " undo")
        return
    eng = _ora_engine(hop)
    failed = []
    for key in sorted(made, key=lambda k: not k.startswith("user:")):
        kind, name = key.split(":", 1)
        st = (f"DROP USER {_ora_ident(_ora_there(hop, name))}"
              if kind == "user" else f"DROP ROLE {_ora_ident(name)}")
        if not apply:
            say(f"   would run {st}")
            continue
        try:
            conn = eng._connect("dst", (hop.databases or [""])[0])
            try:
                conn.cursor().execute(st)
            finally:
                conn.close()
            say(f"   {st}")
        except Exception as e:  # noqa: BLE001 - said
            failed.append(key)
            say(f"   FAILED {st}: {str(e).splitlines()[0][:120]}")
    if apply and not failed:
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
    q = _cs_ident
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
    import re
    out, s = compare(hop, say)
    stmts, skipped, made = [], [], []
    q = _cs_ident
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
    for r in out["missing_on_target"] + out["grants_missing"]:
        for g in s[r]["grants"]:
            perm, resource = g.split(" ", 1)
            on = _cs_resource(hop, resource)
            if on is None or not re.fullmatch(r"[A-Z]+", perm):
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
        session.execute(f"DROP ROLE IF EXISTS {_cs_ident(r)}")
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
    for key in out["grants_missing"]:
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


# --- opensearch --------------------------------------------------------------
#: where the security plugin answers, and what it keeps of each kind
OS_API = "/_plugins/_security/api"
OS_KINDS = (("role", "roles", ("cluster_permissions", "index_permissions",
                               "tenant_permissions", "description")),
            ("user", "internalusers", ("backend_roles", "attributes",
                                       "opendistro_security_roles",
                                       "description")),
            ("mapping", "rolesmapping", ("backend_roles", "hosts", "users",
                                         "and_backend_roles",
                                         "description")))


#: Elasticsearch keeps the same three in its own security API, each in a
#: shape of its own (`_security/role`, `_security/user`,
#: `_security/role_mapping`), and marks what it keeps for itself
#: `metadata._reserved`
ES_KINDS = (("role", "/_security/role", ("cluster", "indices",
                                         "applications", "run_as",
                                         "description")),
            ("user", "/_security/user", ("roles", "full_name", "email",
                                         "enabled")),
            ("mapping", "/_security/role_mapping", ("roles", "rules",
                                                    "enabled")))


def _os_engine(hop):
    from .engines.opensearch import OpenSearchEngine
    return OpenSearchEngine(hop)


def _es_side(hop, side):
    """Whether the side's server is Elasticsearch, as it says itself
    (`variants`): its security API is its own, not the plugin's."""
    b = _os_engine(hop)._brands()[0 if side == "src" else 1]
    return b.name == "elasticsearch"


def _os_paths(hop, side):
    """{kind: the path its definitions are read and written at} on one
    side, in its own API."""
    if _es_side(hop, side):
        return {kind: path for kind, path, _ in ES_KINDS}
    return {kind: f"{OS_API}/{path}" for kind, path, _ in OS_KINDS}


def _os_patterns(hop, rule):
    """A role's index permissions with the source's index names as the
    target calls them, where the hop gives a database another name there -
    OpenSearch's `index_patterns`, Elasticsearch's `names`."""
    eng = _os_engine(hop)
    moved = [(eng._prefix("src", d), eng._prefix("dst", d))
             for d in (hop.databases or [])]
    out = json.loads(json.dumps(rule))
    for group, field in (("index_permissions", "index_patterns"),
                         ("indices", "names")):
        for perm in out.get(group) or []:
            pats = []
            for p in perm.get(field) or []:
                for a, b in moved:
                    if a != b and str(p).startswith(a):
                        p = b + str(p)[len(a):]
                        break
                pats.append(p)
            perm[field] = pats
    return out


def _os_security(hop, side):
    """{"role:<name>" | "user:<name>" | "mapping:<role>": definition} from
    the security plugin's own REST API, what the cluster keeps for itself
    (reserved, static, hidden) left out; None where the cluster answers no
    security API - the plugin absent or turned off. The API shows no
    password, not even as its hash, so none is compared or carried but the
    one the passwords file gives."""
    eng = _os_engine(hop)
    out = {}
    es = _es_side(hop, side)
    kinds = ES_KINDS if es else tuple((k, f"{OS_API}/{p}", keep)
                                      for k, p, keep in OS_KINDS)
    for kind, path, keep in kinds:
        try:
            got = eng._call(side, "GET", path)
        except RuntimeError as e:
            code = str(e).split(":", 2)[1].strip()[:3] if ":" in str(e) \
                else ""
            if code in ("400", "404", "405") or (
                    es and "security must be explicitly enabled"
                    in str(e).lower()):
                return None
            where = "source" if side == "src" else "target"
            raise SystemExit(f"users: the {where}'s security configuration"
                             f" cannot be read ({str(e)[:120]}): the account"
                             " needs the security plugin's REST API")
        for name, d in (got or {}).items():
            if d.get("reserved") or d.get("static") or d.get("hidden") \
                    or (d.get("metadata") or {}).get("_reserved"):
                continue
            entry = {k: d[k] for k in keep if d.get(k) not in (None, "",
                                                                [], {})}
            for k, v in list(entry.items()):
                if isinstance(v, list) and all(isinstance(x, str)
                                               for x in v):
                    entry[k] = sorted(v)
            out[f"{kind}:{name}"] = entry
    return out


def _os_name(name):
    """A role's, user's or mapping's name as one segment of the API's
    path: a `/`, `?`, `#` or `..` in it stays in the name and never
    reaches another endpoint."""
    from urllib.parse import quote
    return quote(str(name), safe="")


def _os_pointer(name):
    """A name as one step of a JSON Patch path (RFC 6901)."""
    return str(name).replace("~", "~0").replace("/", "~1")


def _os_compared(hop, out, s, t, say):
    """What differs besides what is missing, and whether each side runs the
    plugin at all."""
    out["plugin"] = {"source": "absent" if s is None else "present",
                     "target": "absent" if t is None else "present"}
    s, t = s or {}, t or {}
    want = {k: (_os_patterns(hop, v) if k.startswith("role:") else v)
            for k, v in s.items()}
    dd = sorted(k for k in want if k in t and want[k] != t[k])
    # a role's, a user's or a mapping's definition is what it may do and
    # who holds it: its attributes, in the one result shape
    out["attributes_differ"] = dd
    apart = _es_side(hop, "src") != _es_side(hop, "dst")
    out["apis_differ"] = apart
    gap = out["missing_on_target"] or dd or apart or (
        out["plugin"]["target"] == "absent" and s)
    out["result"] = "gap" if gap else "pass"
    if apart and s:
        say("  one side is Elasticsearch and the other OpenSearch: each"
            " keeps its roles, users and mappings in a shape of its own,"
            " so they are made on the target by hand, from the list above")
    if out["plugin"]["source"] == "absent":
        say("  the source answers no security API (the plugin is absent or"
            " off): nothing to carry")
    if out["plugin"]["target"] == "absent" and s:
        say("  the target answers no security API: turn its security plugin"
            " on before these can be carried")


def _os_create(hop, apply, passwords, say):
    """Roles first, then users, then who holds which role - each as the
    source defines it, the index names as the target calls them. A user is
    made with the password the passwords file gives it, or skipped and
    said. A definition the target already had is replaced, and kept in the
    record so a rollback puts it back."""
    out, s, t = _compare(hop, say)
    if out["plugin"]["target"] == "absent":
        if s:
            raise SystemExit("users: the target answers no security API, so"
                             " nothing can be carried to it")
        say("  nothing to create")
        return
    if out.get("apis_differ") and s:
        raise SystemExit("users: one side is Elasticsearch and the other"
                         " OpenSearch, which keep roles, users and mappings"
                         " in shapes of their own - make them on the target"
                         " by hand, from what compare lists")
    t = t or {}
    passwords = passwords or {}
    todo, skipped = [], []
    order = {"role": 0, "user": 1, "mapping": 2}
    for key in sorted(set(out["missing_on_target"])
                      | set(out["attributes_differ"]),
                      key=lambda k: (order[k.split(":", 1)[0]], k)):
        kind, name = key.split(":", 1)
        body = (_os_patterns(hop, s[key]) if kind == "role"
                else dict(s[key]))
        if kind == "user":
            if not passwords.get(name):
                skipped.append(name)
                continue
            body["password"] = passwords[name]
        todo.append((key, body))
    if not todo:
        say("  nothing to create" + (f"; skipped {skipped}: no password"
                                     " given" if skipped else ""))
        return
    say(f">> opensearch: {len(todo)} definition(s) to create or replace"
        + ("" if apply else "  (nothing done; add --apply)"))
    for key, _ in todo:
        say(f"   {key}: {'replace' if key in t else 'create'}")
    for name in skipped:
        say(f"   skipped user {name}: the security API shows no password"
            " and none is in the passwords file")
    if not apply:
        return
    eng = _os_engine(hop)
    paths = _os_paths(hop, "dst")
    made, replaced, failed = [], {}, []
    for key, body in todo:
        kind, name = key.split(":", 1)
        try:
            eng._call("dst", "PUT", f"{paths[kind]}/{_os_name(name)}",
                      body)
        except Exception as e:  # noqa: BLE001 - recorded and said
            failed.append(f"{key}: {str(e).splitlines()[0][:120]}")
            continue
        if key in t:
            replaced[key] = t[key]
        else:
            made.append(key)
    rec = hop.report_dir() / "user-sync-created.json"
    prev = json.loads(rec.read_text()) if rec.exists() else []
    prev.append({"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "engine": "opensearch", "created": made,
                 "replaced": replaced, "failed": failed,
                 "skipped": skipped})
    json.dump(prev, open(rec, "w"), indent=2, ensure_ascii=False)
    for f in failed:
        say(f"   FAILED {f}")
    say(f"  created {len(made)}, replaced {len(replaced)}; recorded at {rec}"
        f" (undo with users {hop.name} rollback)")
    if failed:
        raise SystemExit(f"users: {len(failed)} could not be created")


def _os_rollback(hop, apply, say):
    rec = hop.report_dir() / "user-sync-created.json"
    runs = [e for e in (json.loads(rec.read_text()) if rec.exists() else [])
            if e.get("engine") == "opensearch"]
    made = [k for e in runs for k in e.get("created", [])]
    # the earliest record of a definition is what the target had first
    replaced = {}
    for e in runs:
        for k, v in (e.get("replaced") or {}).items():
            replaced.setdefault(k, v)
    if not made and not replaced:
        say("  no opensearch definitions were created by us, nothing to"
            " undo")
        return
    eng = _os_engine(hop)
    paths = _os_paths(hop, "dst")
    es = _es_side(hop, "dst")
    order = {"mapping": 0, "user": 1, "role": 2}
    # who holds a role before the users, users before the roles
    for key in sorted(made, key=lambda k: (order[k.split(":", 1)[0]], k)):
        kind, name = key.split(":", 1)
        if not apply:
            say(f"   would delete {kind} {name}")
            continue
        eng._call("dst", "DELETE", f"{paths[kind]}/{_os_name(name)}",
                  missing_ok=True)
        say(f"   deleted {kind} {name}")
    for key, body in sorted(replaced.items()):
        kind, name = key.split(":", 1)
        if kind == "user":
            say(f"   {key} was replaced and keeps the password it was given:"
                " the one it had is not shown by the security API")
            body = {k: v for k, v in body.items()}
            if not apply:
                continue
            if es:
                # Elasticsearch updates a user without a password and
                # keeps the one it has
                eng._call("dst", "PUT", f"{paths[kind]}/{_os_name(name)}",
                          body)
                continue
            ops = [{"op": "add", "path": f"/{_os_pointer(name)}"
                                         f"/{_os_pointer(k)}", "value": v}
                   for k, v in body.items()]
            eng._call("dst", "PATCH", paths[kind], ops)
            continue
        if not apply:
            say(f"   would put back {kind} {name}")
            continue
        eng._call("dst", "PUT", f"{paths[kind]}/{_os_name(name)}", body)
        say(f"   put back {kind} {name}")
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


def _step(key, sql, args=(), shown=None, undo=None, made=False):
    """One statement of a PostgreSQL or MySQL plan: `sql` with every name
    quoted and every `%` of it doubled, `args` bound, `shown` what the
    operator is shown (a password never), `undo` the (sql, args) that
    takes it back where one does, `made` for a statement that makes an
    account."""
    return {"key": key, "sql": sql, "args": list(args),
            "shown": shown or sql.replace("%%", "%"),
            "undo": [undo[0], list(undo[1])] if undo else None,
            "made": made}


def _outside(text, word):
    """Where `word` first stands in `text` outside back quotes, quotes and
    parentheses, or -1: `GRANT SELECT (`a ON b`) ON `x`.* TO ...` holds
    ` ON ` three times and only the last is the statement's."""
    depth, quote = 0, None
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == quote:
                quote = None
        elif ch in "`'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and text.startswith(word, i):
            return i
        i += 1
    return -1


def _my_revoke(line):
    """The REVOKE that takes back one `SHOW GRANTS` line replayed on an
    account that was already there, or None where the line cannot be read
    that certainly."""
    if not line.startswith("GRANT "):
        return None
    body = line[len("GRANT "):]
    grant_option = body.endswith(" WITH GRANT OPTION")
    body = body[:-len(" WITH GRANT OPTION")] if grant_option else body
    body = body[:-len(" WITH ADMIN OPTION")] \
        if body.endswith(" WITH ADMIN OPTION") else body
    to = _outside(body, " TO ")
    if to < 0:
        return None
    what, who = body[:to], body[to + len(" TO "):]
    on = _outside(what, " ON ")
    if on < 0:
        # a role granted
        return f"REVOKE {what} FROM {who}"
    privs = what[:on] + (", GRANT OPTION" if grant_option else "")
    return f"REVOKE {privs} ON {what[on + len(' ON '):]} FROM {who}"


#: what `ALTER USER` / `CREATE USER` calls each limit mysql.user keeps
MY_LIMITS = (("max_questions", "MAX_QUERIES_PER_HOUR"),
             ("max_updates", "MAX_UPDATES_PER_HOUR"),
             ("max_connections", "MAX_CONNECTIONS_PER_HOUR"),
             ("max_user_connections", "MAX_USER_CONNECTIONS"))


def _my_options(v, lock=True):
    """(text, args) of an account's TLS requirement, its limits and its
    lock, as `CREATE USER` and `ALTER USER` take them: the issuer, subject
    and cipher bound, the limits whole numbers."""
    a, text, args = v["attrs"], [], []
    ssl = str(a.get("ssl_type") or "").upper()
    if ssl == "ANY":
        text.append("REQUIRE SSL")
    elif ssl == "X509":
        text.append("REQUIRE X509")
    elif ssl == "SPECIFIED":
        parts = []
        for col, word in (("x509_issuer", "ISSUER"),
                          ("x509_subject", "SUBJECT"),
                          ("ssl_cipher", "CIPHER")):
            if v["ssl"].get(col):
                parts.append(f"{word} %s")
                args.append(v["ssl"][col])
        text.append("REQUIRE " + " AND ".join(parts) if parts
                    else "REQUIRE SSL")
    elif ssl == "":
        text.append("REQUIRE NONE")
    limits = [f"{word} {int(a.get(col) or 0)}" for col, word in MY_LIMITS
              if col in a]
    if limits:
        text.append("WITH " + " ".join(limits))
    if lock and "account_locked" in a:
        text.append("ACCOUNT LOCK" if a["account_locked"] == "Y"
                    else "ACCOUNT UNLOCK")
    return " ".join(text), args


def _mysql_plugins(ep):
    """The authentication plugins the server signs accounts in with now,
    or None where it will not say. `mysql_native_password` is off by
    default from 8.4 and gone in 9.0: a hash made by it cannot be carried
    there, whatever the source holds."""
    try:
        c = _mysql_conn(ep)
    except Exception:  # noqa: BLE001 - not known, then
        return None
    try:
        cur = c.cursor()
        cur.execute("select plugin_name from information_schema.plugins"
                    " where plugin_type = 'AUTHENTICATION' and"
                    " plugin_status = 'ACTIVE'")
        return {_text(p) for (p,) in cur.fetchall()}
    except Exception:  # noqa: BLE001 - not known, then
        return None
    finally:
        c.close()


def _my_plan(out, s, t, plugins=None, passwords=None):
    """(steps, skipped) that bring the target's accounts level with the
    source's: every missing account made first - a role as a locked
    account with no password, which is what MySQL 8 makes of `CREATE
    ROLE` -, then each one's grants as the source's server renders them
    (role grants among them), then default roles, then the attributes of
    accounts on both sides. An account whose plugin the target does not
    sign in with (`plugins`, `_mysql_plugins`) is made with its password
    from the passwords file, bound, under the target's own plugin - or
    skipped, and said."""
    import re
    passwords = passwords or {}
    steps, skipped = [], []
    missing = {k for k in s if f"{k[0]}@{k[1]}" in
               set(out["missing_on_target"])}
    for key in sorted(missing):
        u, h = key
        v, name = s[key], f"{u}@{h}"
        drop = ("DROP USER IF EXISTS %s@%s", (u, h))
        if v["role"]:
            steps.append(_step(name, "CREATE ROLE %s", (u,),
                               undo=("DROP ROLE IF EXISTS %s", (u,)),
                               made=True))
            continue
        plugin = v["plugin"]
        if plugin and not re.fullmatch(r"[A-Za-z0-9_]+", plugin):
            skipped.append((name, f"its authentication plugin {plugin!r}"
                                  " is not a plugin's name"))
            continue
        opts, args = _my_options(v)
        said = opts.replace("%s", "<bound>")
        expired = v["attrs"].get("password_expired") == "Y"
        secret = None
        if not v["auth"]:
            if v["attrs"].get("account_locked") != "Y":
                skipped.append((name, "no password on the source, and it"
                                      " may sign in: not made from here"))
                continue
            sql = f"CREATE USER %s@%s {opts}"
            shown = f"CREATE USER '{u}'@'{h}' {said} (a role: no password)"
        elif plugins is not None and plugin and plugin not in plugins:
            secret = passwords.get(u) or passwords.get(name)
            if not secret:
                skipped.append((name, f"the target does not sign in with"
                                      f" {plugin}, so its password cannot"
                                      " be carried as its hash, and the"
                                      " passwords file has none for it"))
                continue
            sql = f"CREATE USER %s@%s IDENTIFIED BY %s {opts}"
            shown = (f"CREATE USER '{u}'@'{h}' IDENTIFIED BY <from the"
                     f" passwords file> {said} (the target has no {plugin})")
        else:
            how = f"IDENTIFIED WITH {plugin} AS 0x{_hex(v['auth'])}" \
                if plugin else f"IDENTIFIED BY PASSWORD 0x{_hex(v['auth'])}"
            sql = f"CREATE USER %s@%s {how} {opts}"
            shown = (f"CREATE USER '{u}'@'{h}' IDENTIFIED WITH {plugin} AS"
                     f" 0x<hash> {said} (the source's password)")
        if expired:
            sql += " PASSWORD EXPIRE"
            shown += " PASSWORD EXPIRE"
        steps.append(_step(name, sql, (u, h) + tuple(args), shown,
                           undo=drop, made=True))
        if secret is not None:
            # bound in its place when it runs; never shown or recorded
            steps[-1]["secret_at"] = 2
            steps[-1]["secret"] = secret
            steps[-1]["given"] = True
    made = {f"{u}@{h}" for u, h in missing}
    short = set(out["grants_missing"])
    for key in sorted(s):
        name = f"{key[0]}@{key[1]}"
        if name not in made and name not in short:
            continue
        have = set(t.get(key, {}).get("grants") or ())
        for line in s[key]["grants"]:
            if line in have:
                continue
            undo = None if name in made else _my_revoke(line)
            steps.append(_step(name, line.replace("%", "%%"),
                               undo=(undo.replace("%", "%%"), ())
                               if undo else None))
    for key in sorted(s):
        name = f"{key[0]}@{key[1]}"
        want = s[key]["default_roles"]
        had = t.get(key, {}).get("default_roles") or []
        if not want or (name not in made and want == had):
            continue
        u, h = key
        if s[key]["maria"]:
            steps.append(_step(name, "SET DEFAULT ROLE %s FOR %s@%s",
                               (want[0], u, h)))
            continue
        roles = ", ".join("%s@%s" for _ in want)
        undo = None
        if name not in made:
            undo = (("ALTER USER %s@%s DEFAULT ROLE "
                     + ", ".join("%s@%s" for _ in had),
                     (u, h) + tuple(x for r in had for x in r))
                    if had else ("ALTER USER %s@%s DEFAULT ROLE NONE",
                                 (u, h)))
        steps.append(_step(name, f"ALTER USER %s@%s DEFAULT ROLE {roles}",
                           (u, h) + tuple(x for r in want for x in r),
                           undo=undo))
    for name in out["attributes_differ"]:
        key = next(k for k in s if f"{k[0]}@{k[1]}" == name)
        v, have = s[key], t[key]
        u, h = key
        if (v["attrs"], v["ssl"]) != (have["attrs"], have["ssl"]):
            opts, args = _my_options(v)
            back, bargs = _my_options(have)
            steps.append(_step(name, f"ALTER USER %s@%s {opts}",
                               (u, h) + tuple(args),
                               undo=(f"ALTER USER %s@%s {back}",
                                     (u, h) + tuple(bargs))))
            if v["attrs"].get("password_expired") == "Y" and \
                    have["attrs"].get("password_expired") != "Y":
                steps.append(_step(name, "ALTER USER %s@%s PASSWORD EXPIRE",
                                   (u, h)))
        if v["plugin"] != have["plugin"]:
            skipped.append((name, "signs in with another plugin on the"
                                  " target: its password there is the"
                                  " target's own, not changed from here"))
    return steps, skipped


def _pg_options(a, only=None):
    """(text, args) of a role's attributes as CREATE ROLE and ALTER ROLE
    take them: all of them, or those named in `only`; the date bound."""
    flags = (("login", "LOGIN", "NOLOGIN"),
             ("superuser", "SUPERUSER", "NOSUPERUSER"),
             ("createdb", "CREATEDB", "NOCREATEDB"),
             ("createrole", "CREATEROLE", "NOCREATEROLE"),
             ("replication", "REPLICATION", "NOREPLICATION"),
             ("bypassrls", "BYPASSRLS", "NOBYPASSRLS"),
             ("inherit", "INHERIT", "NOINHERIT"))
    text, args = [], []
    for attr, yes, no in flags:
        if only is None or attr in only:
            text.append(yes if a.get(attr) else no)
    if only is None or "connlimit" in only:
        limit = a.get("connlimit")
        text.append(f"CONNECTION LIMIT"
                    f" {-1 if limit is None else int(limit)}")
    if only is None or "valid_until" in only:
        text.append("VALID UNTIL %s")
        args.append(a.get("valid_until") or "infinity")
    return " ".join(text), args


def _pg_plan(out, s, t, hashes, passwords):
    """(steps, skipped): every missing role made first, with
    none of its memberships - so no role waits on another -, then each
    membership the target lacks, then the attributes of roles on both
    sides. A login role is made with the source's password hash where the
    source shows it, else the passwords file's; one with neither is
    skipped and said."""
    steps, skipped = [], []
    missing = set(out["missing_on_target"])
    made = set()
    q = _pg_ident
    for r in sorted(missing):
        a = s[r]
        secret, how = None, None
        if hashes and r in hashes:
            secret, how = hashes[r], "hash from source (same password)"
        elif r in passwords:
            secret, how = passwords[r], "from --passwords file"
        elif a["login"]:
            skipped.append((r, "no hash readable on source and no"
                               " --passwords entry"))
            continue
        # defaults left out: a managed target refuses to be told even
        # NOREPLICATION by a role that is not a superuser
        only = [k for k in PG_ATTRS if k not in ("connlimit", "valid_until")
                and bool(a.get(k)) != (k == "inherit")]
        if a["connlimit"] not in (None, -1):
            only.append("connlimit")
        if a.get("valid_until"):
            only.append("valid_until")
        opts, args = _pg_options(a, only)
        sql = f"CREATE ROLE {q(r)} {opts}".rstrip()
        shown = sql.replace("%%", "%").replace(
            "VALID UNTIL %s", f"VALID UNTIL '{a.get('valid_until')}'")
        if secret is not None:
            sql += " PASSWORD %s"
            shown += f" PASSWORD <{how}>"
        else:
            shown += "  (no password: nologin role)"
        steps.append(_step(r, sql, args, shown,
                           undo=(f"DROP ROLE IF EXISTS {q(r)}", ()),
                           made=True))
        if secret is not None:
            # bound last when it runs; never in what is shown or recorded
            steps[-1]["secret"] = secret
            steps[-1]["given"] = r not in (hashes or {})
        made.add(r)
    for r in sorted(set(s)):
        if r not in made and r not in out["grants_missing"]:
            continue
        have = set(t.get(r, {}).get("member_of") or ())
        for m in s[r]["member_of"]:
            if m in have:
                continue
            role, admin = m.split(" WITH ADMIN")[0], " WITH ADMIN" in m
            if not admin and f"{role} WITH ADMIN" in have:
                # a member here who may grant it on, and not on the source
                steps.append(_step(
                    r, f"REVOKE ADMIN OPTION FOR {q(role)} FROM {q(r)}",
                    undo=(f"GRANT {q(role)} TO {q(r)} WITH ADMIN OPTION",
                          ())))
                continue
            # taken back as it was given: the admin option alone where the
            # membership was there before it
            back = (f"REVOKE ADMIN OPTION FOR {q(role)} FROM {q(r)}"
                    if admin and role in have
                    else f"REVOKE {q(role)} FROM {q(r)}")
            steps.append(_step(
                r, f"GRANT {q(role)} TO {q(r)}"
                   + (" WITH ADMIN OPTION" if admin else ""),
                undo=None if r in made else (back, ())))
    for r in out["attributes_differ"]:
        a, b = s[r], t[r]
        only = [k for k in PG_ATTRS if a.get(k) != b.get(k)]
        opts, args = _pg_options(a, only)
        back, bargs = _pg_options(b, only)
        steps.append(_step(r, f"ALTER ROLE {q(r)} {opts}", args,
                           undo=(f"ALTER ROLE {q(r)} {back}", bargs)))
    return steps, skipped


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
    """Carry what the target lacks, and - once anything was applied -
    compare again: a gap `create` left (an account skipped for want of a
    password, a statement the target refused) ends it with an error
    naming it, never with a clean exit over a target that still differs
    (`_after`)."""
    ways = {"redis": lambda: _redis_create(hop, apply, say),
            "kafka": lambda: _kafka_create(hop, apply, passwords, say),
            "clickhouse": lambda: _ch_create(hop, apply, passwords, say),
            "cassandra": lambda: _cs_create(hop, apply, say),
            "opensearch": lambda: _os_create(hop, apply, passwords, say),
            "elasticsearch": lambda: _os_create(hop, apply, passwords, say),
            "mssql": lambda: _ms_create(hop, apply, passwords, say),
            "db2i": lambda: _ibmi_create(hop, apply, say),
            "teradata": lambda: _td_create(hop, apply, passwords, say),
            "oracle": lambda: _ora_create(hop, apply, passwords, say),
            "mongodb": lambda: _mongo_create_all(hop, apply, passwords, say),
            "mongo": lambda: _mongo_create_all(hop, apply, passwords, say)}
    way = ways.get(hop.engine) or (
        lambda: _sql_create(hop, apply, passwords or {}, say))
    _after(hop, say, apply, way() or ())


def _mongo_create_all(hop, apply, passwords, say):
    out, s, t = _compare(hop, say)
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


def _sql_create(hop, apply, passwords, say):
    """PostgreSQL and MySQL: the plan (`_pg_plan`, `_my_plan`) shown, and
    applied on one connection, each statement on its own into the ledger
    the record keeps."""
    out, s, t = _compare(hop, say)
    if hop.engine == "mysql":
        steps, skipped = _my_plan(
            out, s, t, _mysql_plugins(hop.target) if out["missing_on_target"]
            else None, passwords)
    else:
        hashes = _pg_hashes(hop.source) if out["missing_on_target"] else {}
        steps, skipped = _pg_plan(out, s, t, hashes or {}, passwords)
    say(f"\nplan: {len(steps)} statement(s) on TARGET"
        f" ({'APPLY' if apply else 'dry-run'})")
    key = None
    for st in steps:
        if st["key"] != key:
            key = st["key"]
            say(f"  -- {key}")
        say(f"     {st['shown'][:160]}")
    for name, why in skipped:
        say(f"  skipped {name}: {why}")
    if not steps:
        say("nothing to create - the target already has every non-system"
            " source account as the source has it")
        return
    if not apply:
        say("\ndry-run only. Add --apply to execute on the target.")
        return
    conn = _mysql_conn(hop.target) if hop.engine == "mysql" \
        else _pg_conn(hop.target)
    try:
        ledger = _run_steps(conn.cursor(), steps, say)
    finally:
        conn.close()
    rec = _record(hop, hop.engine, ledger,
                  skipped=[f"{n}: {why}" for n, why in skipped])
    failed = [e for e in ledger if not e["ok"]]
    say(f"\nran={len(ledger) - len(failed)} failed={len(failed)}"
        f"  record: {rec}")
    if failed:
        raise SystemExit(f"users: {len(failed)} statement(s) failed, each"
                         f" named above and in {rec}")
    # the accounts given the passwords file's password (`_after`)
    return {st["key"] for st in steps if "secret" in st
            and st.get("given")}


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
    if hop.engine in ("opensearch", "elasticsearch"):
        return _os_rollback(hop, apply, say)
    if hop.engine == "mssql":
        return _ms_rollback(hop, apply, say)
    if hop.engine == "db2i":
        return _ibmi_rollback(hop, apply, say)
    if hop.engine == "teradata":
        return _td_rollback(hop, apply, say)
    if hop.engine == "oracle":
        return _ora_rollback(hop, apply, say)
    if hop.engine in ("mongodb", "mongo"):
        return _mongo_rollback(hop, apply, say)
    return _sql_rollback(hop, apply, say)


def _sql_rollback(hop, apply, say):
    """PostgreSQL and MySQL: every statement a `create` ran that has an
    undo, undone last first - a membership or grant given to an account
    that was already there revoked, an attribute set back, an account it
    made dropped. A role PostgreSQL will not drop while it holds a
    privilege has them revoked in every database of the hop first."""
    rf = hop.report_dir() / "user-sync-created.json"
    if not rf.exists():
        say(f"no created-record at {rf} - nothing this tool created here")
        return
    got = json.loads(rf.read_text())
    runs = got if isinstance(got, list) else [
        {"engine": got.get("engine"), "created": got.get("created", [])}]
    undo = []
    for run in runs:
        if run.get("engine") != hop.engine:
            continue
        if "steps" in run:
            undo += [(st["key"], st["undo"], st.get("made"))
                     for st in run["steps"] if st.get("ok") and st["undo"]]
            continue
        # a record from before the ledger: the accounts it made
        for key in run.get("created", []):
            if hop.engine == "mysql":
                u, h = key.rsplit("@", 1)
                undo.append((key, ["DROP USER IF EXISTS %s@%s", [u, h]],
                             True))
            else:
                undo.append((key, [f"DROP ROLE IF EXISTS {_pg_ident(key)}",
                                   []], True))
    if not undo:
        say("record has nothing to undo")
        return
    say(f"rollback plan ({'APPLY' if apply else 'dry-run'}): {len(undo)}"
        " statement(s), last first")
    for key, (sql, args), _ in reversed(undo):
        say(f"  {key}: {sql.replace('%%', '%')}"
            + (f"  {args}" if args else ""))
    if not apply:
        say("dry-run only. Add --apply to execute.")
        return
    conn = _mysql_conn(hop.target) if hop.engine == "mysql" \
        else _pg_conn(hop.target)
    failed = []
    try:
        cur = conn.cursor()
        for key, (sql, args), made in reversed(undo):
            try:
                try:
                    cur.execute(sql, tuple(args))
                except Exception:
                    if hop.engine != "postgres" or not made:
                        raise
                    _pg_revoke_everywhere(hop, key)
                    cur.execute(sql, tuple(args))
                say(f"  undone {key}: {sql.replace('%%', '%')[:100]}")
            except Exception as e:  # noqa: BLE001 - said, the rest go on
                failed.append(key)
                say(f"  FAILED {key}: {type(e).__name__}: {str(e)[:100]}")
    finally:
        conn.close()
    if not failed:
        rf.rename(str(rf) + ".rolled-back")


def _pg_revoke_everywhere(hop, role):
    """What `role` holds in each of the hop's databases on the target,
    revoked - PostgreSQL refuses to drop a role that still holds one."""
    q = _pg_ident
    for db in hop.databases or []:
        there = hop.target_db(db)
        try:
            c2 = _pg_conn(hop.target, dbname=there)
        except Exception:  # noqa: BLE001 - the drop says what is left
            continue
        try:
            k2 = c2.cursor()
            k2.execute("select nspname from pg_namespace where nspname not"
                       " like 'pg\\_%%' and nspname <> 'information_schema'",
                       ())
            for (ns,) in k2.fetchall():
                for obj in ("TABLES", "SEQUENCES", "FUNCTIONS"):
                    try:
                        k2.execute(f"REVOKE ALL ON ALL {obj} IN SCHEMA"
                                   f" {q(ns)} FROM {q(role)}", ())
                    except Exception:  # noqa: BLE001 - best effort
                        pass
                try:
                    k2.execute(f"REVOKE ALL ON SCHEMA {q(ns)} FROM"
                               f" {q(role)}", ())
                except Exception:  # noqa: BLE001 - best effort
                    pass
            try:
                k2.execute(f"REVOKE ALL ON DATABASE {q(there)} FROM"
                           f" {q(role)}", ())
            except Exception:  # noqa: BLE001 - best effort
                pass
        finally:
            c2.close()


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
            from .verdict import VERDICTS
            if out["result"] == "pass":
                say("VERIFY: pass")
            elif out["result"] == "unknown":
                # nothing found and not everything compared: never a pass,
                # and not a gap it did not see (`verdict.VERDICTS`)
                say("VERIFY: not everything could be compared")
                raise SystemExit(VERDICTS["incomplete"].exit)
            else:
                say("VERIFY: gap found")
                raise SystemExit(VERDICTS["different"].exit)
