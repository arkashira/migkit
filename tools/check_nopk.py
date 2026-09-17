#!/usr/bin/env python3
"""Check the tables the mover cannot: those with no key to pair rows by.

DMS matches rows across sides by key, so a table with no usable primary/unique
key is skipped entirely and has no proof of equality. It is also the group whose
updates and deletes CDC drops, which makes it the riskiest one.

Compared here with an order-independent aggregate (bit_xor of per-row crc32),
which needs no key. Slower than a keyed compare, but these tables are small.

  check_nopk.py <hop> [--db X] [--json]

Read-only on both sides. Exits 1 when a table differs.
"""
import json
import os
import sys
import time

import yaml

HOPS = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"]
SKIP_DBS = {"mysql", "sys", "information_schema", "performance_schema"}


def conn(cfg, db=None):
    import pymysql
    return pymysql.connect(host=cfg["host"], port=int(cfg.get("port", 3306)),
                           user=cfg["user"], password=cfg["password"],
                           database=db, charset="utf8mb4", connect_timeout=15,
                           read_timeout=int(os.environ.get("MIGKIT_READ_TIMEOUT",
                                                           "3600")))


def nopk_tables(cur, db):
    """Tables the mover cannot match row for row by key.

    Not only tables with no key at all: also tables whose unique key allows
    NULL in every column, because NULLs do not compare. DMS reports those as
    "No eligible primary/unique key" and skips them the same way."""
    cur.execute("""select t.table_name from information_schema.tables t
                   where t.table_schema=%s and t.table_type='BASE TABLE'
                     and not exists (
                       select 1 from information_schema.statistics s
                       join information_schema.columns c
                         on c.table_schema=s.table_schema
                        and c.table_name=s.table_name
                        and c.column_name=s.column_name
                       where s.table_schema=t.table_schema
                         and s.table_name=t.table_name
                         and s.non_unique=0
                       group by s.index_name
                       having sum(c.is_nullable='YES')=0)""", (db,))
    return [r[0] for r in cur.fetchall()]


def columns(cur, db, table):
    cur.execute("""select column_name, data_type from information_schema.columns
                   where table_schema=%s and table_name=%s
                   order by ordinal_position""", (db, table))
    return cur.fetchall()


def fingerprint(cur, db, table, cols):
    """Row count plus an order-independent aggregate of every row.

    xor is commutative, so the two sides compare without sorting. Floats are
    rounded first because the sides may store different precision."""
    parts = []
    for name, dtype in cols:
        col = f"`{name}`"
        if dtype in ("float", "double"):
            col = f"round({col}, 6)"
        elif dtype in ("datetime", "timestamp"):
            col = f"date_format({col}, '%%Y-%%m-%%d %%H:%%i:%%s')"
        parts.append(f"ifnull(cast({col} as char), '\\0')")
    expr = "concat_ws('\\x1f', " + ", ".join(parts) + ")"
    cur.execute(f"select count(*), coalesce(bit_xor(crc32({expr})), 0) "
                f"from `{db}`.`{table}`")
    return cur.fetchone()


def main(hop_name, only_db, want_json):
    hop = HOPS[hop_name]
    if hop["engine"] != "mysql":
        print("check_nopk: mysql only")
        return 0
    s = conn(hop["source"])
    t = conn(hop["target"])
    sc, tc = s.cursor(), t.cursor()
    if only_db:
        dbs = [only_db]
    elif hop.get("databases"):
        dbs = list(hop["databases"])
    else:
        sc.execute("show databases")
        dbs = [r[0] for r in sc.fetchall()
               if r[0] not in SKIP_DBS and not r[0].startswith("__")]
    excl = {x.strip() for x in os.environ.get("MIGKIT_EXCLUDE", "").split(",")
            if x.strip()}
    out = {"check": "nopk", "hop": hop_name, "databases": {}}
    bad = total = 0
    for db in dbs:
        if db in excl:
            continue
        tables = [x for x in nopk_tables(sc, db)
                  if f"{db}.{x}" not in excl and x not in excl]
        if not tables:
            continue
        print(f"{db}: {len(tables)} table(s) without a primary key")
        rows = {}
        for tbl in tables:
            total += 1
            cols = columns(sc, db, tbl)
            tcols = columns(tc, db, tbl)
            if [c[0] for c in cols] != [c[0] for c in tcols]:
                print(f"   {tbl}: column count differs src={len(cols)} dst={len(tcols)}")
                rows[tbl] = {"result": "diff", "why": "columns differ"}
                bad += 1
                continue
            t0 = time.time()
            try:
                sn, sx = fingerprint(sc, db, tbl, cols)
                tn, tx = fingerprint(tc, db, tbl, cols)
            except Exception as e:
                print(f"   {tbl}: unreadable {str(e)[:80]}")
                rows[tbl] = {"result": "error", "why": str(e)[:120]}
                bad += 1
                continue
            same = (sn == tn and sx == tx)
            rows[tbl] = {"result": "ok" if same else "diff",
                         "src_rows": sn, "dst_rows": tn,
                         "src_fp": sx, "dst_fp": tx,
                         "seconds": round(time.time() - t0, 1)}
            if same:
                print(f"   {tbl}: match, {sn} rows ({time.time() - t0:.1f}s)")
            else:
                bad += 1
                why = (f"row counts differ src={sn} dst={tn}" if sn != tn
                       else f"same {sn} rows but different contents")
                print(f"   {tbl}: differs - {why}")
        out["databases"][db] = rows
    s.close()
    t.close()
    out["tables_checked"] = total
    out["tables_diff"] = bad
    out["result"] = "pass" if not bad else "gap"
    print(f"\n{total} tables checked, {bad} differ")
    if not total:
        print("  (no table lacks a primary key - no blind spot)")
    p = os.path.join("reports", f"nopk-{hop_name}.json")
    os.makedirs("reports", exist_ok=True)
    json.dump(out, open(p, "w"), indent=2, ensure_ascii=False, default=str)
    print(f"json: {p}")
    if want_json:
        print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: check_nopk.py <hop> [--db X] [--json]")
        sys.exit(2)
    a = sys.argv[1:]
    db = a[a.index("--db") + 1] if "--db" in a else ""
    sys.exit(main(a[0], db, "--json" in a))
