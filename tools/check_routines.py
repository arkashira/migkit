#!/usr/bin/env python3
"""Compare views, stored procedures/functions, and triggers between a hop's source
and target, per database. (Objects migkit's default check does not separately count.)
Usage: check_routines.py <hop_name>   (mysql / postgres)
Read-only.
"""
import sys, os, yaml
HOPS = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"]
SYS_MY = {'__tencentdb__', 'information_schema', 'mysql', 'performance_schema', 'sys'}

def mysql_objs(cfg):
    import pymysql
    c = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                        password=cfg["password"], connect_timeout=15); cur = c.cursor()
    cur.execute("select table_schema,count(*) from information_schema.views group by 1")
    v = {s: n for s, n in cur.fetchall() if s not in SYS_MY}
    cur.execute("select routine_schema,routine_type,count(*) from information_schema.routines group by 1,2")
    r = {}
    for s, t, n in cur.fetchall():
        if s not in SYS_MY: r.setdefault(s, {})[t] = n
    cur.execute("select trigger_schema,count(*) from information_schema.triggers group by 1")
    tg = {s: n for s, n in cur.fetchall() if s not in SYS_MY}
    c.close(); return v, r, tg

def pg_objs(cfg, db):
    import psycopg2
    c = psycopg2.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                         password=cfg["password"], dbname=db, connect_timeout=15); cur = c.cursor()
    cur.execute("select count(*) from information_schema.views where table_schema='public'")
    v = cur.fetchone()[0]
    cur.execute("select routine_type,count(*) from information_schema.routines where routine_schema='public' group by 1")
    r = dict(cur.fetchall())
    cur.execute("""select count(*) from information_schema.triggers where trigger_schema='public'""")
    tg = cur.fetchone()[0]
    c.close(); return v, r, tg

def main(hop_name):
    hop = HOPS[hop_name]; eng = hop["engine"]
    print(f"hop={hop_name} engine={eng}")
    if eng == "mysql":
        sv, sr, st = mysql_objs(hop["source"]); dv, dr, dt = mysql_objs(hop["target"])
        dbs = sorted(set(sv) | set(dv) | set(sr) | set(dr) | set(st) | set(dt))
        for db in dbs:
            s_ = sr.get(db, {}); d_ = dr.get(db, {})
            eq = sv.get(db, 0) == dv.get(db, 0) and s_ == d_ and st.get(db, 0) == dt.get(db, 0)
            print(f"  {db:16} views {sv.get(db,0)}/{dv.get(db,0)}  "
                  f"procs {s_.get('PROCEDURE',0)}/{d_.get('PROCEDURE',0)}  "
                  f"funcs {s_.get('FUNCTION',0)}/{d_.get('FUNCTION',0)}  "
                  f"triggers {st.get(db,0)}/{dt.get(db,0)}  {'OK' if eq else 'DIFF'}")
    elif eng == "postgres":
        for db in hop["databases"]:
            try:
                sv, sr, st = pg_objs(hop["source"], db); dv, dr, dt = pg_objs(hop["target"], db)
                eq = sv == dv and sr == dr and st == dt
                print(f"  {db:20} views {sv}/{dv}  routines {sum(sr.values())}/{sum(dr.values())}  "
                      f"triggers {st}/{dt}  {'OK' if eq else 'DIFF'}")
            except Exception as e:
                print(f"  {db:20} ERROR {type(e).__name__}")
    else:
        print("  routines check: mysql/postgres only")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: check_routines.py <hop_name>"); sys.exit(1)
    try:
        main(sys.argv[1])
    except Exception as _e:
        print(f"ERROR: {type(_e).__name__}: {str(_e)[:120]}"); sys.exit(1)
