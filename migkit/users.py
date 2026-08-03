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

from .config import get_hop

MYSQL_SYS = {'mysql.sys', 'mysql.session', 'mysql.infoschema', 'root',
             'rdsadmin', 'rds_superuser_role', 'rdswriteforwarduser',
             'tencentroot', 'tencentdba'}
PG_SYS = {'rdsadmin', 'rdstopmgr', 'rds_superuser', 'rdsrepladmin',
          'rdswriteforwarduser', 'root', 'postgres'}


def _h(x):
    b = x if isinstance(x, bytes) else str(x or "").encode()
    return hashlib.md5(b).hexdigest()[:10]


def _mysql_sysuser(u):
    return u in MYSQL_SYS or u.startswith("AWS_")


def _pg_sysrole(r):
    if r.startswith("pg_"):
        return True
    return r in PG_SYS or r.startswith("tencentdb") or r.startswith("rds")


def _mysql_conn(ep):
    import pymysql
    return pymysql.connect(host=ep.host, port=ep.port, user=ep.user,
                           password=ep.password, connect_timeout=15, autocommit=True)


def _pg_conn(ep):
    import psycopg2
    c = psycopg2.connect(host=ep.host, port=ep.port, user=ep.user,
                         password=ep.password, dbname="postgres", connect_timeout=15)
    c.autocommit = True
    return c


def _mysql_users(ep):
    c = _mysql_conn(ep)
    cur = c.cursor()
    cur.execute("select user, host, plugin, authentication_string from mysql.user")
    d = {(u, h): (p, a) for u, h, p, a in cur.fetchall() if not _mysql_sysuser(u)}
    c.close()
    return d


def _pg_hashes(ep):
    """rolname -> password hash, ถ้า source ยอมให้อ่าน pg_authid.
    RDS/Aurora บล็อกไว้ (คืน {}) แต่ PG ที่เราเป็น superuser จริงอ่านได้
    -> ก็อป hash ไปสร้างที่ปลายทางได้เลย ไม่ต้องรู้ password ตัวจริง"""
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
    else:
        raise SystemExit("users: mysql / postgres only (mongo needs the plaintext to recreate)")
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
    if eng == "mysql":
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


def create(hop, apply=False, passwords=None, say=print):
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
    # สะสมไว้ทุกรอบ ไม่ทับของเดิม - create หลายรอบแล้ว rollback ต้องถอนได้ครบ
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


def rollback(hop, apply=False, say=print):
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
                    # PG กัน drop ถ้า role ยังถือสิทธิ์อยู่ -> ถอนสิทธิ์ทุก db ก่อน
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
