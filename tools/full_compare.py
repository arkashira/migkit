#!/usr/bin/env python3
"""Full source-vs-target comparison as one JSON per db, {src,dst} style (like params.json).
Covers: params, record_counts, data_checksums, sequences, objects, constraints, grants.
Read-only (SELECT / md5 checksums). Not part of migkit.
Usage: full_compare.py [hop_name]   (default: rds-to-tencent; postgres hops only)"""
import os, sys, yaml, json, datetime, psycopg2

OUT = os.environ.get("COMPARE_OUT", "reports")  # relative to migkit/ cwd
os.makedirs(OUT, exist_ok=True)
HOP_NAME = sys.argv[1] if len(sys.argv) > 1 else "rds-to-tencent"
HOP  = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"][HOP_NAME]
SRC, DST = HOP["source"], HOP["target"]
DBS  = HOP["databases"]
EXCL = set(HOP.get("exclude", []))              # e.g. oms_mkp_uat.public.pick_dispatch_queue

def conn(cfg, db):
    return psycopg2.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                            password=cfg["password"], dbname=db, connect_timeout=15)

def q(cur, sql):
    cur.execute(sql); return cur.fetchall()

NOISE = "__"   # DTS internal schemas: __tencentdb__dts_*, __dst__resume__* (bookkeeping, not user data)

def user_tables(cur):
    return [(s, t) for s, t in q(cur, """select schemaname,tablename from pg_tables
            where schemaname not in ('pg_catalog','information_schema') order by 1,2""")
            if not s.startswith(NOISE)]

def counts(cur, tbls):
    d = {}
    for s, t in tbls:
        try:
            cur.execute(f'select count(*) from "{s}"."{t}"'); d[f"{s}.{t}"] = cur.fetchone()[0]
        except Exception as e:
            d[f"{s}.{t}"] = f"ERR:{type(e).__name__}"
    return d

def checksums(cur, tbls):
    d = {}
    for s, t in tbls:
        # order-independent content hash: md5 of each row's text, aggregated in md5 order
        try:
            cur.execute(f'select md5(coalesce(string_agg(rm,\'\' order by rm),\'\')) '
                        f'from (select md5(x::text) rm from "{s}"."{t}" x) z')
            d[f"{s}.{t}"] = cur.fetchone()[0]
        except Exception as e:
            d[f"{s}.{t}"] = f"ERR:{type(e).__name__}"
    return d

def unlogged(cur):
    return sorted(f"{s}.{t}" for s, t in q(cur, """select n.nspname,c.relname from pg_class c
        join pg_namespace n on n.oid=c.relnamespace where c.relpersistence='u'
        and n.nspname not in ('pg_catalog','information_schema')""") if not s.startswith(NOISE))

def sequences(cur):
    d = {}
    for s, n, last in q(cur, "select schemaname,sequencename,last_value from pg_sequences"):
        if s.startswith(NOISE): continue
        d[f"{s}.{n}"] = last
    return d

def objects(cur):
    rows = q(cur, """select case relkind when 'r' then 'table' when 'v' then 'view'
              when 'm' then 'matview' when 'i' then 'index' when 'S' then 'sequence'
              when 'p' then 'partitioned_table' else relkind end, n.nspname||'.'||c.relname
              from pg_class c join pg_namespace n on n.oid=c.relnamespace
              where n.nspname not in ('pg_catalog','information_schema','pg_toast')
              order by 1,2""")
    d = {}
    for kind, name in rows:
        if name.split(".", 1)[0].startswith(NOISE): continue
        d.setdefault(kind, []).append(name)
    return d

def constraints(cur):
    rows = q(cur, """select conname, con.convalidated, n.nspname||'.'||rel.relname
             from pg_constraint con join pg_class rel on rel.oid=con.conrelid
             join pg_namespace n on n.oid=rel.relnamespace
             where con.contype in ('c','f') and n.nspname not in ('pg_catalog','information_schema')""")
    return {f"{tbl}.{name}": bool(v) for name, v, tbl in rows if not tbl.split(".", 1)[0].startswith(NOISE)}

def grants(cur):
    rows = q(cur, """select grantee, table_schema||'.'||table_name, privilege_type
             from information_schema.role_table_grants
             where table_schema not in ('pg_catalog','information_schema')""")
    return set(f"{g}|{t}|{p}" for g, t, p in rows if not t.split(".", 1)[0].startswith(NOISE))

def excluded(db, tbl):  # tbl = 'schema.table'
    return f"{db}.{tbl}" in EXCL

def side(cfg, db, want_checksums):
    c = conn(cfg, db); c.autocommit = True; cur = c.cursor()   # autocommit: one bad table can't poison the rest
    tbls = user_tables(cur)
    out = {"tables": tbls, "counts": counts(cur, tbls), "sequences": sequences(cur),
           "objects": objects(cur), "constraints": constraints(cur), "grants": grants(cur),
           "unlogged": unlogged(cur)}
    out["checksums"] = checksums(cur, tbls) if want_checksums else {}
    c.close(); return out

def merge_kv(sd, dd, db, kind):
    keys = sorted(set(sd) | set(dd)); res = {}
    for k in keys:
        sv, dv = sd.get(k), dd.get(k)
        row = {"src": sv, "dst": dv, "match": (sv == dv)}
        if kind in ("counts", "checksums") and excluded(db, k):
            row["excluded"] = "target-owned (live rows on target); not compared"
            row["match"] = None
        res[k] = row
    return res

def build(db):
    print(f"[{db}] source (tunnel)…")
    s = side(SRC, db, True)
    print(f"[{db}] target (.34)…")
    d = side(DST, db, True)
    rc = merge_kv(s["counts"], d["counts"], db, "counts")
    dc = merge_kv(s["checksums"], d["checksums"], db, "checksums")
    sq = merge_kv(s["sequences"], d["sequences"], db, "seq")
    cn = merge_kv(s["constraints"], d["constraints"], db, "con")
    # objects: presence by type
    obj = {}
    for kind in sorted(set(s["objects"]) | set(d["objects"])):
        ss, dd_ = set(s["objects"].get(kind, [])), set(d["objects"].get(kind, []))
        obj[kind] = {"src_count": len(ss), "dst_count": len(dd_),
                     "missing_on_dst": sorted(ss - dd_), "extra_on_dst": sorted(dd_ - ss)}
    # grants: set diff
    gr = {"src_count": len(s["grants"]), "dst_count": len(d["grants"]),
          "missing_on_dst": sorted(s["grants"] - d["grants"]),
          "extra_on_dst":   sorted(d["grants"] - s["grants"])}
    # params from migkit's fresh dump
    try:
        params = json.load(open(f"reports/{HOP_NAME}/{db}/params.json"))
    except Exception:
        params = {}
    def ndiff(m):   # count real diffs (match False), ignoring excluded
        return sum(1 for v in m.values() if v.get("match") is False)
    doc = {
        "database": db,
        "generated": datetime.date.today().isoformat(),
        "source": f"{SRC['host']}:{SRC['port']}",
        "target": f"{DST['host']}:{DST['port']}",
        "summary": {
            "tables": len(s["tables"]),
            "record_counts_diff": ndiff(rc),
            "data_checksums_diff": ndiff(dc),
            "sequences_diff": ndiff(sq),
            "constraints_validated_diff": ndiff(cn),
            "grants_missing_on_dst": len(gr["missing_on_dst"]),
            "grants_extra_on_dst": len(gr["extra_on_dst"]),
            "params_total": len(params),
        },
        "unlogged_tables": {"src": s.get("unlogged", []), "dst": d.get("unlogged", []),
                            "note": "unlogged tables write no WAL, so DTS logical decoding does NOT replicate their rows; a data/count diff on these is expected"},
        "record_counts": rc,
        "data_checksums": dc,
        "sequences": sq,
        "constraints_validated": cn,
        "objects": obj,
        "grants": gr,
        "params": params,
    }
    path = f"{OUT}/compare-{db}.json"
    json.dump(doc, open(path, "w"), indent=2, default=str)
    S = doc["summary"]
    print(f"  wrote {path}")
    print(f"    tables={S['tables']} counts_diff={S['record_counts_diff']} "
          f"data_diff={S['data_checksums_diff']} seq_diff={S['sequences_diff']} "
          f"con_diff={S['constraints_validated_diff']} grants_missing={S['grants_missing_on_dst']}")
    return doc["summary"]

if __name__ == "__main__":
    import sys, os
    which = sys.argv[1:] or DBS
    allsum = {}
    for db in which:
        try:
            allsum[db] = build(db)
        except Exception as e:
            print(f"  [{db}] FAIL {type(e).__name__}: {str(e)[:100]}")
            allsum[db] = {"error": f"{type(e).__name__}: {str(e)[:100]}"}
    json.dump(allsum, open(f"{OUT}/compare-summary.json", "w"), indent=2)
    print(f"\nsummary -> {OUT}/compare-summary.json")
