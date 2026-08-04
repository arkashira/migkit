#!/usr/bin/env python3
"""Compare table + sequence grants (PostgreSQL) between a hop's source and target.
Missing SEQUENCE USAGE/SELECT/UPDATE -> INSERT fails "permission denied for sequence" even
with table INSERT. --gen writes GRANT (apply) + REVOKE (undo) .sql per db.
Usage: check_grants.py <hop_name> [--gen] [--db X]
Read-only unless you run the generated GRANT .sql yourself. (grants apply to target;
role must already exist or GRANT errors.)
"""
import sys, os, yaml, time, os, json, datetime
HOPS = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"]
OUT = os.environ.get("GRANTS_OUT", "reports/grants")  # relative to migkit/ cwd

def grants(cfg, db):
    import psycopg2
    c = psycopg2.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                         password=cfg["password"], dbname=db, connect_timeout=15)
    c.autocommit = True; cur = c.cursor()
    # raw ACL, not information_schema.role_table_grants: that view only shows
    # grants whose grantee the connected user is a member of, so a plain user on
    # the target cannot see group_* grants and they look missing when they exist
    cur.execute("""select pg_get_userbyid(a.grantee), c.relname, a.privilege_type
                   from pg_class c
                   join pg_namespace n on n.oid = c.relnamespace,
                        aclexplode(c.relacl) a
                   where c.relkind in ('r','p','v','m','f') and n.nspname='public'
                     and pg_get_userbyid(a.grantee) <> 'PUBLIC'""")
    tg = set(cur.fetchall())
    # role_usage_grants only exposes USAGE; SELECT/UPDATE come from relacl
    cur.execute("""select pg_get_userbyid(a.grantee), c.relname, a.privilege_type
                   from pg_class c
                   join pg_namespace n on n.oid = c.relnamespace,
                        aclexplode(c.relacl) a
                   where c.relkind='S' and n.nspname='public'
                     and pg_get_userbyid(a.grantee) <> 'PUBLIC'""")
    sg = set(cur.fetchall())
    cur.execute("select rolname from pg_roles")
    roles = set(x[0] for x in cur.fetchall())
    c.close(); return tg, sg, roles

def main(hop_name, gen, only_db):
    global OUT
    OUT = os.path.join('reports', 'grants', hop_name)
    hop = HOPS[hop_name]
    if hop["engine"] != "postgres":
        print("check_grants: postgres only"); return
    dbs = [only_db] if only_db else hop["databases"]
    if gen: os.makedirs(OUT, exist_ok=True)
    jout = {"check": "grants", "hop": hop_name,
            "generated": datetime.date.today().isoformat(), "databases": {}}
    # the mover's own bookkeeping tables exist on one side only, so their
    # grants can never apply and do not mean an app permission is missing
    noise = (hop.get("options") or {}).get("noise_prefix", "")
    # cloud-provider roles exist on their own side only, set in config.conf
    ignore = {r.strip() for r in os.environ.get("GRANTS_IGNORE_ROLES", "").split(",") if r.strip()}
    ignore |= {r.strip() for r in ((hop.get("options") or {}).get("ignore_roles") or "").split(",") if r.strip()}

    def _app(rows):
        return {r for r in rows
                if not (noise and r[1].startswith(noise)) and r[0] not in ignore}

    for db in dbs:
        stg, ssg, _ = grants(hop["source"], db)
        dtg, dsg, droles = grants(hop["target"], db)
        stg, ssg, dtg, dsg = _app(stg), _app(ssg), _app(dtg), _app(dsg)
        mtg = sorted(stg - dtg); msg = sorted(ssg - dsg)
        gtees = sorted(set(g for g, _, _ in mtg) | set(g for g, _, _ in msg))
        can = [g for g in gtees if g in droles]; need = [g for g in gtees if g not in droles]
        print(f"{db}: table-grants missing={len(mtg)} seq-grants missing={len(msg)}")
        print(f"   grantees present on target (grant-able now): {can}")
        print(f"   grantees NOT on target (create role first): {need}")
        bycnt = {}
        for g, _, _ in mtg: bycnt[g] = bycnt.get(g, 0) + 1
        jout["databases"][db] = {
            "table_grants_missing": len(mtg), "sequence_grants_missing": len(msg),
            "missing_by_grantee": bycnt,
            "grantable_now": can, "need_role_created_first": need,
            "result": "pass" if not mtg and not msg else "gap",
        }
        if gen:
            gsql, rsql = [], []
            for g, t, p in mtg:
                if g in droles:
                    gsql.append(f'GRANT {p} ON "public"."{t}" TO "{g}";')
                    rsql.append(f'REVOKE {p} ON "public"."{t}" FROM "{g}";')
            for g, o, p in msg:
                if g in droles:
                    gsql.append(f'GRANT {p} ON SEQUENCE "public"."{o}" TO "{g}";')
                    rsql.append(f'REVOKE {p} ON SEQUENCE "public"."{o}" FROM "{g}";')
            gf = f"{OUT}/grants-{db}.sql"; uf = f"{OUT}/grants-{db}-undo.sql"
            open(gf, "w").write("\n".join(gsql) + "\n")
            open(uf, "w").write("\n".join(rsql) + "\n")
            print(f"   wrote {gf} ({len(gsql)} GRANTs) + undo {uf}")
    od = os.environ.get("COMPARE_OUT", "reports"); os.makedirs(od, exist_ok=True)
    jf = f"{od}/grants-{hop_name}.json"
    json.dump(jout, open(jf, "w"), indent=2, ensure_ascii=False)
    print(f"json: {jf}")

if __name__ == "__main__":
    a = sys.argv[1:]
    if not a:
        print("usage: check_grants.py <hop_name> [--gen] [--db X]"); sys.exit(1)
    hop = a[0]; gen = "--gen" in a
    only = a[a.index("--db")+1] if "--db" in a else None
    try:
        main(hop, gen, only)
    except Exception as _e:
        print(f"ERROR: {type(_e).__name__}: {str(_e)[:120]}"); sys.exit(1)
