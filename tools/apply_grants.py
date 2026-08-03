#!/usr/bin/env python3
"""Apply the GRANT files that check_grants.py --gen produced, on the TARGET.

Reads reports/grants/grants-<db>.sql and runs each statement against that
database on the hop's target. The matching -undo.sql (REVOKE) stays on disk, so
anything applied here can be taken back. Read-only on the source, and it only
ever touches privileges - never data.

Usage: apply_grants.py <hop>
"""
import sys, os, glob, yaml

HOPS = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"]


def main(hop_name):
    hop = HOPS[hop_name]
    tgt = hop["target"]
    if hop.get("engine") != "postgres":
        print(f"  {hop_name}: mysql/mongo grants ride along with 'migkit users create --apply' - nothing to apply here")
        return 0
    files = sorted(glob.glob(os.path.join("reports", "grants", hop_name, "grants-*.sql")))
    files = [f for f in files if not f.endswith("-undo.sql")]
    if not files:
        print("  no grant files - run ./repair.sh grants <hop> first"); return 1
    total = failed = 0
    for f in files:
        db = os.path.basename(f)[len("grants-"):-len(".sql")]
        stmts = [s.strip() for s in open(f).read().split(";") if s.strip()]
        if not stmts:
            continue
        try:
            if hop["engine"] == "postgres":
                import psycopg2
                cn = psycopg2.connect(host=tgt["host"], port=tgt.get("port", 5432),
                                      user=tgt["user"], password=tgt["password"],
                                      dbname=db, connect_timeout=10)
            else:
                import pymysql
                cn = pymysql.connect(host=tgt["host"], port=tgt.get("port", 3306),
                                     user=tgt["user"], password=tgt["password"],
                                     database=db, connect_timeout=10)
            cn.autocommit = True
            cur = cn.cursor()
        except Exception as e:
            print(f"  {db}: cannot connect to target: {type(e).__name__}: {str(e)[:80]}")
            failed += len(stmts); continue
        ok = bad = 0
        for st in stmts:
            try:
                cur.execute(st); ok += 1
            except Exception as e:
                bad += 1
                print(f"    FAILED: {st[:70]} -> {type(e).__name__}: {str(e)[:60]}")
        cn.close()
        total += ok; failed += bad
        print(f"  {db}: applied {ok} grants" + (f", {bad} failed" if bad else ""))
    print(f"applied={total} failed={failed}  undo: reports/grants/grants-<db>-undo.sql")
    return 1 if failed else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: apply_grants.py <hop>"); sys.exit(2)
    sys.exit(main(sys.argv[1]))
