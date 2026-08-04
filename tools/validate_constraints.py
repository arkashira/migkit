#!/usr/bin/env python3
"""Validate the check constraints and foreign keys left NOT VALID on the target.

Movers create constraints NOT VALID so the load runs fast, which means they are
enforced for new writes but the existing rows were never scanned. Two effects:
old rows may already violate them, and the planner distrusts a constraint that
was never validated. A target in that state is not equal to its source even when
every row matches.

VALIDATE CONSTRAINT only reads and sets a flag; it changes no data, and it fails
without changing anything if a row really does violate. Safe to run, but it takes
a light lock while scanning, so do large tables outside busy hours.

  validate_constraints.py <hop> [--apply] [--db X]
"""
import os
import sys
import time

import yaml

HOPS = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"]


def conn(cfg, db):
    import psycopg2
    c = psycopg2.connect(host=cfg["host"], port=cfg.get("port", 5432),
                         user=cfg["user"], password=cfg["password"],
                         dbname=db, connect_timeout=15)
    c.autocommit = True
    return c


def pending(cur):
    cur.execute("""select n.nspname, c.conrelid::regclass::text, c.conname, c.contype,
                          coalesce(pg_total_relation_size(c.conrelid), 0)
                   from pg_constraint c
                   join pg_namespace n on n.oid = c.connamespace
                   where not c.convalidated and c.contype in ('c','f')
                     and n.nspname not like 'pg\\_%'
                   order by 5 desc""")
    return cur.fetchall()


def main(hop_name, apply, only_db):
    hop = HOPS[hop_name]
    if hop["engine"] != "postgres":
        print("validate_constraints: postgres เท่านั้น")
        return 0
    dbs = [only_db] if only_db else (hop.get("databases") or [])
    total = done = failed = 0
    for db in dbs:
        c = conn(hop["target"], db)
        cur = c.cursor()
        rows = pending(cur)
        total += len(rows)
        if not rows:
            print(f"{db}: constraint ทุกตัว validate แล้ว")
            c.close()
            continue
        kind = {"c": "check", "f": "foreign key"}
        print(f"{db}: ยังไม่ validate {len(rows)} ตัว")
        for schema, table, name, ctype, size in rows:
            gb = size / 1024 ** 3
            label = f"{table}.{name} ({kind.get(ctype, ctype)}, ตาราง {gb:.1f} GB)"
            if not apply:
                print(f"   จะสั่ง: alter table {table} validate constraint \"{name}\""
                      f"   [{gb:.1f} GB]")
                continue
            t0 = time.time()
            try:
                cur.execute(f'alter table {table} validate constraint "{name}"')
                print(f"   validate แล้ว {label} ใช้ {time.time() - t0:.1f} วิ")
                done += 1
            except Exception as e:
                failed += 1
                print(f"   ไม่ผ่าน {label}: {str(e).strip().splitlines()[0][:110]}")
                print("      แปลว่ามีแถวเดิมที่ผิดกติกาจริง ต้องแก้ข้อมูลก่อน")
        c.close()
    if not apply:
        print(f"\nรวม {total} ตัวที่ยังไม่ validate (ยังไม่ลงมือ ใส่ --apply)")
        print("ย้อนกลับ: ไม่ต้อง - VALIDATE ไม่แก้ข้อมูล แค่ติดธงว่าตรวจแล้ว")
    else:
        print(f"\nสำเร็จ {done} ตัว ไม่ผ่าน {failed} ตัว")
    return 1 if failed else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: validate_constraints.py <hop> [--apply] [--db X]")
        sys.exit(2)
    a = sys.argv[1:]
    db = a[a.index("--db") + 1] if "--db" in a else ""
    sys.exit(main(a[0], "--apply" in a, db))
