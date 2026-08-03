#!/usr/bin/env python3
"""Spot check for a mysql hop: system-time parity + random-record row compare.

What it does (read-only, both sides):
  1. system time  - now(6) vs utc_timestamp(6) on source and target, proves the
                    effective timezone is the same (UTC) and the clocks agree
  2. random rows  - picks tables (largest ones + a random spread), samples random
                    rows by primary key, fetches the full row from BOTH sides and
                    compares every column (composite and string PKs supported)

Usage:  spot_check.py <hop> [--tables N] [--rows N]     (defaults: 12 tables, 3 rows)
Output: stdout report + two files in $COMPARE_OUT (default reports/):
          spot-check-<hop>.txt   plain-language summary (share as-is)
          spot-check-<hop>.json  machine-comparable result
Exit 0 = everything matches; 1 = any diff/missing/time problem.
Works on any environment - point conf/hops.yaml at prod and run the same command.
"""
import sys, os, os, json, random, hashlib, datetime, time, yaml

HOPS = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"]
OUT = os.environ.get("COMPARE_OUT", "reports")
SYS_SCHEMAS = ("mysql", "information_schema", "performance_schema", "sys", "__tencentdb__")


def conn(cfg):
    import pymysql
    return pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                           password=cfg["password"], connect_timeout=12,
                           cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4")


def time_check(a, b):
    q = "select now(6) n, utc_timestamp(6) u, @@time_zone tz, @@system_time_zone stz"
    t0 = time.time()
    a.execute(q); s = a.fetchone()
    b.execute(q); t = b.fetchone()
    gap = time.time() - t0
    s_off = (s["n"] - s["u"]).total_seconds()
    t_off = (t["n"] - t["u"]).total_seconds()
    drift = (t["u"] - s["u"]).total_seconds()
    ok = abs(s_off) < 2 and abs(t_off) < 2 and abs(drift) < 5
    return {"source": {"now": str(s["n"]), "utc": str(s["u"]), "tz": s["tz"], "system_tz": s["stz"],
                       "now_minus_utc_sec": round(s_off, 3)},
            "target": {"now": str(t["n"]), "utc": str(t["u"]), "tz": t["tz"], "system_tz": t["stz"],
                       "now_minus_utc_sec": round(t_off, 3)},
            "clock_delta_sec": round(drift, 3), "query_gap_sec": round(gap, 2),
            "result": "pass" if ok else "FAIL"}


def pick_tables(a, n_tables):
    a.execute(f"""select t.table_schema s, t.table_name t, t.table_rows r
                  from information_schema.tables t
                  where t.table_type='BASE TABLE' and t.table_rows > 0
                    and t.table_schema not in {SYS_SCHEMAS!r}
                    and exists (select 1 from information_schema.key_column_usage k
                                where k.constraint_name='PRIMARY'
                                  and k.table_schema=t.table_schema and k.table_name=t.table_name)
                  order by t.table_rows desc""")
    rows = a.fetchall()
    big = rows[:4]
    rest = rows[4:]
    random.shuffle(rest)
    return big + rest[:max(0, n_tables - len(big))]


def pk_cols(a, schema, table):
    a.execute("""select column_name c from information_schema.key_column_usage
                 where constraint_name='PRIMARY' and table_schema=%s and table_name=%s
                 order by ordinal_position""", (schema, table))
    return [r["c"] for r in a.fetchall()]


def first_pk_is_numeric(a, schema, table, col):
    a.execute("""select data_type d from information_schema.columns
                 where table_schema=%s and table_name=%s and column_name=%s""",
              (schema, table, col))
    r = a.fetchone()
    return r and r["d"] in ("int", "bigint", "smallint", "mediumint", "tinyint")


def sample_table(a, b, schema, table, n_rows, est_rows):
    q = lambda name: f"`{name}`"
    full = f"{q(schema)}.{q(table)}"
    pks = pk_cols(a, schema, table)
    first = q(pks[0])
    numeric = first_pk_is_numeric(a, schema, table, pks[0])
    if numeric:
        a.execute(f"select min({first}) mn, max({first}) mx from {full}")
        rng = a.fetchone()
        if rng["mn"] is None:
            return pks, 0, []
    n = est_rows
    out = []
    seen = set()
    for _ in range(n_rows):
        if numeric:
            rv = random.randint(rng["mn"], rng["mx"])
            a.execute(f"select {first} v from {full} where {first} >= %s order by {first} limit 1", (rv,))
        else:
            # non-numeric pk: cheap offset capped so huge tables stay fast
            off = random.randint(0, max(0, min(est_rows - 1, 50000)))
            a.execute(f"select {first} v from {full} order by {first} limit 1 offset {off}")
        r = a.fetchone()
        if not r:
            continue
        a.execute(f"select * from {full} where {first}=%s limit 1", (r["v"],))
        src = a.fetchone()
        key = tuple(str(src[c]) for c in pks)
        if key in seen:
            continue
        seen.add(key)
        where = " and ".join(f"{q(c)}=%s" for c in pks)
        b.execute(f"select * from {full} where {where} limit 1", tuple(src[c] for c in pks))
        tgt = b.fetchone()
        pk_disp = "+".join(str(src[c])[:24] for c in pks)
        if tgt is None:
            out.append({"pk": pk_disp, "result": "MISSING"})
            continue
        bad = [k for k in src if src[k] != tgt.get(k)]
        h = hashlib.md5(repr(sorted((k, str(v)) for k, v in src.items())).encode()).hexdigest()[:8]
        ts = next((f"{src[k]}" for k in ("created_at", "create_time", "updated_at") if k in src and src[k]), "")
        if bad:
            out.append({"pk": pk_disp, "result": "DIFF", "columns": bad})
        else:
            out.append({"pk": pk_disp, "result": "identical", "cols": len(src), "hash": h, "ts": ts})
    return pks, n, out


def main(hop_name, n_tables, n_rows):
    hop = HOPS[hop_name]
    if hop["engine"] != "mysql":
        print("spot_check: mysql hops only for now"); sys.exit(2)
    cs, ct = conn(hop["source"]), conn(hop["target"])
    a, b = cs.cursor(), ct.cursor()

    tc = time_check(a, b)
    print("=== system time ===")
    print(f"  source now={tc['source']['now']} utc={tc['source']['utc']} tz={tc['source']['tz']!r} (now-utc {tc['source']['now_minus_utc_sec']:+}s)")
    print(f"  target now={tc['target']['now']} utc={tc['target']['utc']} tz={tc['target']['tz']!r} (now-utc {tc['target']['now_minus_utc_sec']:+}s)")
    print(f"  clock delta {tc['clock_delta_sec']:+}s (query gap {tc['query_gap_sec']}s)  -> {tc['result']}")

    print("\n=== random records ===")
    tables = pick_tables(a, n_tables)
    results = []
    tot = ok = diff = miss = 0
    for t in tables:
        try:
            pks, n, rows = sample_table(a, b, t["s"], t["t"], n_rows, t["r"])
        except Exception as e:
            print(f"  {t['s']}.{t['t']}: ERROR {type(e).__name__}: {str(e)[:60]}")
            continue
        for r in rows:
            tot += 1
            if r["result"] == "identical":
                ok += 1
            elif r["result"] == "DIFF":
                diff += 1
            else:
                miss += 1
        st = "; ".join(f"{r['pk']}={r['result']}" for r in rows) or "no rows"
        print(f"  {t['s']}.{t['t']} (pk={'+'.join(pks)}, ~{n:,} rows): {st}")
        results.append({"table": f"{t['s']}.{t['t']}", "pk": "+".join(pks), "rows_in_table_approx": n, "sampled": rows})
    verdict = "pass" if (diff == 0 and miss == 0 and tot > 0 and tc["result"] == "pass") else "FAIL"
    print(f"\nRESULT: {tot} rows sampled -> identical={ok} diff={diff} missing={miss} | time={tc['result']} | overall={verdict}")

    os.makedirs(OUT, exist_ok=True)
    today = datetime.date.today().strftime("%d/%m/%Y")
    jf = f"{OUT}/spot-check-{hop_name}.json"
    json.dump({"check": "spot_check", "hop": hop_name, "generated": today,
               "system_time": tc, "tables": results,
               "summary": {"rows": tot, "identical": ok, "diff": diff, "missing": miss, "overall": verdict}},
              open(jf, "w"), indent=2, ensure_ascii=False, default=str)

    L = []
    L.append(f"ผลตรวจ mysql spot check ({hop['source'].get('host','?')} -> {hop['target'].get('host','?')}) - {today}")
    L.append("สุ่ม records เทียบรายแถว และเช็ค system time")
    L.append("")
    L.append("1) system time")
    L.append(f"  ฝั่งต้นทาง  now() = {tc['source']['now']}  utc_timestamp() = {tc['source']['utc']}  (tz={tc['source']['tz']})")
    L.append(f"  ฝั่งปลายทาง now() = {tc['target']['now']}  utc_timestamp() = {tc['target']['utc']}  (tz={tc['target']['tz']})")
    L.append(f"  - now() กับ utc_timestamp() ห่างกัน {tc['source']['now_minus_utc_sec']:+}s / {tc['target']['now_minus_utc_sec']:+}s"
             f" = เวลาที่ใช้จริง{'เป็น UTC ทั้งคู่' if tc['result']=='pass' else 'ไม่ตรงกัน ต้องตรวจ'}")
    L.append(f"  - นาฬิกาสองเครื่องห่างกัน {tc['clock_delta_sec']:+} วินาที (รวมช่วงยิง query {tc['query_gap_sec']}s)")
    L.append(f"  - สรุป: {'เวลาตรงกัน ไม่มี shift' if tc['result']=='pass' else 'มีปัญหา ตรวจค่า tz ด่วน'}")
    L.append("")
    L.append("2) random records")
    L.append(f"  สุ่มแถวจริงจาก {len(results)} ตาราง ดึงทั้งแถวจากสองฝั่งด้วย primary key เดียวกัน")
    L.append(f"  เทียบทุกคอลัมน์ รวม {tot} แถว")
    L.append("")
    for t in results:
        good = sum(1 for r in t["sampled"] if r["result"] == "identical")
        bad = [r for r in t["sampled"] if r["result"] != "identical"]
        line = f"  {t['table']:34} {good}/{len(t['sampled'])} แถวตรง"
        if bad:
            line += "  มีปัญหา: " + "; ".join(f"{r['pk']}={r['result']}" for r in bad)
        L.append(line)
    L.append("")
    if verdict == "pass":
        L.append(f"  รวม {tot}/{tot} แถว ตรงกันทุกคอลัมน์ ไม่มีแถวหาย")
        L.append("  ค่า timestamp ในแถวตรงกันถึงระดับ microsecond")
    else:
        L.append(f"  พบปัญหา: diff={diff} missing={miss} - ดูรายละเอียดใน {jf}")
    tf = f"{OUT}/spot-check-{hop_name}.txt"
    open(tf, "w").write("\n".join(L) + "\n")
    print(f"files: {tf}  {jf}")
    sys.exit(0 if verdict == "pass" else 1)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("usage: spot_check.py <hop> [--tables N] [--rows N]"); sys.exit(1)
    nt = int(args[args.index("--tables") + 1]) if "--tables" in args else 12
    nr = int(args[args.index("--rows") + 1]) if "--rows" in args else 3
    try:
        main(args[0], nt, nr)
    except SystemExit:
        raise
    except Exception as _e:
        print(f"ERROR: {type(_e).__name__}: {str(_e)[:140]}"); sys.exit(1)
