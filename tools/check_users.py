#!/usr/bin/env python3
"""Compare DB users/roles between a hop's source and target (login-parity check).
DTS/DMS migrate data+schema but NOT users/grants -> target often has 0 app users.
Usage: check_users.py <hop_name>
  pg    : pg_authid password-hash compare when permitted, else pg_roles (names only)
  mysql : mysql.user + authentication_string hash match
  mongo : admin usersInfo (needs privilege)
Read-only. Also writes users-<hop>.json to $COMPARE_OUT (default reports/) so runs
can be diffed against each other and dropped into the result folder as-is.
"""
import sys, os, os, json, datetime, yaml, hashlib
HOPS = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"]

def _h(x):  # stable short hash for password-compare without printing the secret
    b = x if isinstance(x, bytes) else str(x or "").encode()
    return hashlib.md5(b).hexdigest()[:10]

def pg_users(cfg):
    import psycopg2
    c = psycopg2.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                         password=cfg["password"], dbname="postgres", connect_timeout=15)
    cur = c.cursor()
    try:
        cur.execute("""select rolname, coalesce(rolpassword,'') from pg_authid
                       where rolcanlogin and rolname not like 'pg_%'
                       and rolname not in ('rdsadmin','rdstopmgr','rds_superuser','rdsrepladmin')""")
        d = {r[0]: _h(r[1]) for r in cur.fetchall()}
        mode = "pg_authid (password hash compared)"
    except Exception:
        c.rollback()
        cur.execute("""select rolname from pg_roles
                       where rolcanlogin and rolname not like 'pg_%'
                       and rolname not like 'tencentdb%'
                       and rolname not in ('rdsadmin','rdstopmgr','rds_superuser','rdsrepladmin','root')""")
        d = {r[0]: "-" for r in cur.fetchall()}
        mode = "pg_roles (names only; password check needs superuser)"
    c.close(); return d, mode

def mysql_users(cfg):
    import pymysql
    c = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                        password=cfg["password"], connect_timeout=15)
    cur = c.cursor()
    cur.execute("""select concat(user,'@',host), authentication_string from mysql.user
                   where user not in ('mysql.sys','mysql.session','mysql.infoschema','root',
                   'rdsadmin','tencentroot','tencentdba')""")
    d = {u: _h(a) for u, a in cur.fetchall()}
    c.close(); return d, "mysql.user (password hash compared)"

def mongo_users(cfg, is_src):
    from pymongo import MongoClient
    import warnings; warnings.filterwarnings("ignore")
    if is_src and cfg.get("host"):
        uri = f"mongodb://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/?{cfg['uri_options']}"
    else:
        host = cfg.get("hosts") or f"{cfg['host']}:{cfg['port']}"
        uri = f"mongodb://{cfg['user']}:{cfg['password']}@{host}/?{cfg['uri_options']}"
    cl = MongoClient(uri, serverSelectionTimeoutMS=8000)
    r = cl.admin.command("usersInfo", {"forAllDBs": True})
    cl.close()
    return {f"{u['user']}@{u['db']}": "-" for u in r.get("users", [])}, "usersInfo (names only)"

def main(hop_name):
    hop = HOPS[hop_name]; eng = hop["engine"]
    if eng == "mongodb":
        src, smode = mongo_users(hop["source"], True); dst, dmode = mongo_users(hop["target"], False)
    else:
        fn = {"postgres": pg_users, "mysql": mysql_users}[eng]
        src, smode = fn(hop["source"]); dst, dmode = fn(hop["target"])
    missing = sorted(set(src) - set(dst))
    extra = sorted(set(dst) - set(src))
    pwdiff = sorted(u for u in set(src) & set(dst) if src[u] != dst[u] and "-" not in (src[u], dst[u]))
    print(f"hop={hop_name} engine={eng}  source_users={len(src)}  target_users={len(dst)}")
    print(f"  MISSING on target ({len(missing)}): {missing}")
    print(f"  extra on target ({len(extra)}): {extra}")
    print(f"  present but password differs ({len(pwdiff)}): {pwdiff}")
    if not missing and not pwdiff:
        print("  OK: all source users present on target with matching password")
    out = {
        "check": "users", "hop": hop_name, "engine": eng,
        "generated": datetime.date.today().isoformat(),
        "compare_method": {"source": smode, "target": dmode},
        "source_users": len(src), "target_users": len(dst),
        "missing_on_target": missing, "extra_on_target": extra,
        "password_differs": pwdiff,
        "result": "pass" if not missing and not pwdiff else "gap",
    }
    od = os.environ.get("COMPARE_OUT", "reports"); os.makedirs(od, exist_ok=True)
    jf = f"{od}/users-{hop_name}.json"
    json.dump(out, open(jf, "w"), indent=2, ensure_ascii=False)
    print(f"  json: {jf}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: check_users.py <hop_name>  (hops:", ", ".join(HOPS), ")"); sys.exit(1)
    try:
        main(sys.argv[1])
    except Exception as _e:
        print(f"ERROR: {type(_e).__name__}: {str(_e)[:120]}"); sys.exit(1)
