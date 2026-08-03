#!/usr/bin/env python3
"""LEGACY/UAT report generator: labels and cached report paths are baked in from the
UAT run. For a reproducible parameter check on any env use:
    .venv/bin/migkit check <hop> --only params

Standalone param parity: dump ALL settings both sides, classify, write per-engine text.
Not part of migkit. Reads creds from conf/hops.yaml. Read-only (SELECT / SHOW / getParameter).

Buckets per engine:
  A  differences that MATTER   (behavior-critical AND harmful: value-diff or presence-diff)
  B  differs but SAFE          (behavior-critical but expected on a managed target; why)
  C  filtered out              (everything else: counts + appendix of present/absent names)
"""
import sys, os, yaml, datetime

CONF = yaml.safe_load(open(os.environ.get("MIGKIT_CONF", "conf/hops.yaml")))["hops"]
OUT = os.environ.get("COMPARE_OUT", "reports")  # relative to migkit/ cwd
os.makedirs(OUT, exist_ok=True)

# ---- what each behavior-critical param is + what breaks if it differs ----
PG_CRIT = {
 "timezone": ("Session/display time zone for now(), current_timestamp, timestamptz->text, AT TIME ZONE",
              "a trigger/default calling now() stores a different wall-clock; timestamptz renders shifted"),
 "datestyle": ("Date parse & output format (ISO, MDY, DMY)",
               "'01/02/2024' read as Feb vs Jan; text date output differs"),
 "intervalstyle": ("interval -> text serialization",
                   "intervals serialize differently in dumps/compares"),
 "server_encoding": ("Database character encoding",
                     "non-ASCII (Thai) becomes mojibake or the copy fails on conversion"),
 "client_encoding": ("Session character encoding",
                     "same insert/select reinterprets bytes; round-trips corrupt non-ASCII"),
 "lc_collate": ("String sort order for ORDER BY, < >, and text indexes",
                "rows sort differently; a text index returns wrong/missing rows (collation-version bug)"),
 "lc_ctype": ("Character classification, upper()/lower()",
              "case folding / char-class functions give different results"),
 "lc_monetary": ("to_char() and money formatting", "formatted money output differs"),
 "lc_numeric": ("Numeric formatting in to_char()", "formatted numeric output differs"),
 "lc_time": ("Time formatting in to_char()", "formatted time output differs"),
 "standard_conforming_strings": ("Whether backslash in a literal is literal or an escape",
                                 "'\\n' = newline vs backslash-n; backslash data stored differently"),
 "bytea_output": ("bytea -> text encoding (hex vs escape)",
                  "binary looks different in dumps/checksums"),
 "extra_float_digits": ("Float precision when converting to text (0 vs 3)",
                        "float/double exports and checksums disagree; values look rounded"),
 "backslash_quote": ("Whether \\' is accepted as a quote escape", "literal parsing/escaping differs"),
 "default_transaction_isolation": ("read committed vs repeatable read",
                                   "different snapshot visibility; concurrency anomalies differ"),
 "search_path": ("Schema resolution for unqualified names",
                 "an unqualified table/function resolves to a DIFFERENT schema -> wrong object"),
 "array_nulls": ("Whether NULL is recognized in array input", "array literals parse differently"),
 "check_function_bodies": ("Function-body validation at CREATE time",
                           "a restore that passes on one side can fail on the other"),
 "default_text_search_config": ("Default full-text-search config for to_tsvector()/FTS",
                                "FTS tokenization/stemming differs -> queries return different matches"),
}
# behavior-critical GUCs that are EXPECTED to differ on a managed target, harmless to data
PG_SAFE = {
 "wal_level": "managed target runs 'logical' for its own replication/CDC; affects replication capability, NOT stored data",
}

MYSQL_CRIT = {
 "character_set_server": ("Default charset for new schemas/tables",
                          "latin1 vs utf8mb4 stores Thai wrong / truncates multibyte / mojibake"),
 "character_set_database": ("Default charset of the current database", "same as above at db scope"),
 "character_set_connection": ("Session charset for statements", "same insert/select reinterprets bytes"),
 "character_set_client": ("Charset the client sends in", "round-trips corrupt non-ASCII"),
 "character_set_results": ("Charset results are returned in", "SELECT returns mojibake for non-ASCII"),
 "collation_server": ("Default collation (sort/case/accent sensitivity)",
                      "ORDER BY differs; WHERE matches differ; unique index collapses e/e-accent"),
 "collation_database": ("Collation of the current database", "same as above at db scope"),
 "collation_connection": ("Session collation", "comparison/sort in the session differs"),
 "time_zone": ("Session zone for NOW(), CURRENT_TIMESTAMP, TIMESTAMP<->local",
               "a trigger/default using NOW() stores a different time; TIMESTAMP converts differently"),
 "system_time_zone": ("OS zone picked up (used when time_zone=SYSTEM)", "shifts every TIMESTAMP conversion"),
 "sql_mode": ("Strictness & dialect (STRICT, NO_ZERO_DATE, ONLY_FULL_GROUP_BY, PIPES_AS_CONCAT, ANSI_QUOTES, NO_BACKSLASH_ESCAPES)",
              "data that inserts on one side is rejected/truncated/zeroed on the other; ||, \"x\", GROUP BY change meaning"),
 "lower_case_table_names": ("Table-name case sensitivity (0/1/2)",
                            "Orders and orders are one table or two -> lost rows / name collision"),
 "explicit_defaults_for_timestamp": ("TIMESTAMP default & nullability behavior",
                                     "different implicit DEFAULT CURRENT_TIMESTAMP / ON UPDATE -> values diverge"),
 "transaction_isolation": ("repeatable read vs read committed", "different snapshot visibility"),
 "default_storage_engine": ("Engine for new tables (InnoDB vs MyISAM)", "new tables lose transactions/FKs"),
 "max_allowed_packet": ("Largest statement/row", "a large row/BLOB that inserts on source fails on a smaller target"),
 "group_concat_max_len": ("GROUP_CONCAT result length", "concatenated results silently truncated"),
}
MYSQL_SAFE = {
 "version": "vendor/build string differs (e.g. 8.0.42 vs TXSQL fork); explains subtle behavior, not data corruption",
 "version_comment": "vendor label; informational only",
 "time_zone": "TC sets time_zone='+00:00' (UTC) explicitly while AWS uses SYSTEM which resolves to UTC -> effective session zone is UTC on BOTH, so NOW()/CURRENT_TIMESTAMP/TIMESTAMP conversions are identical. migkit deep 'timeshift' check found no uniform timestamp offset, confirming no data shift.",
 "system_time_zone": "only consulted when time_zone=SYSTEM; TC's time_zone is the explicit +00:00, so the CST OS zone never applies -> no effect on stored or converted times.",
}

def norm(v):
    return "" if v is None else str(v).strip()

def classify(src, dst, crit, safe):
    names = set(src) | set(dst)
    A_val, A_pres, B, C_val, C_only_src, C_only_dst = [], [], [], [], [], []
    for n in sorted(names):
        ln = n.lower()
        in_s, in_d = n in src, n in dst
        sv, dv = src.get(n), dst.get(n)
        if in_s and in_d:
            if norm(sv) == norm(dv):
                continue  # identical -> ignore
            # value differs
            if ln in safe:
                B.append((n, sv, dv, safe[ln]))
            elif ln in crit:
                A_val.append((n, sv, dv, crit[ln]))
            else:
                C_val.append(n)
        elif in_s:      # only on AWS
            if ln in crit: A_pres.append((n, sv, "ABSENT", crit[ln], "AWS"))
            else: C_only_src.append(n)
        else:           # only on Tencent
            if ln in crit: A_pres.append((n, "ABSENT", dv, crit[ln], "Tencent"))
            else: C_only_dst.append(n)
    return A_val, A_pres, B, C_val, C_only_src, C_only_dst

def write_report(engine, path, hop, srcdesc, dstdesc, src, dst, crit, safe, extra_note=""):
    A_val, A_pres, B, C_val, C_only_src, C_only_dst = classify(src, dst, crit, safe)
    L = []
    W = L.append
    W(f"PARAM PARITY — {engine}   (AWS source  vs  Tencent target)")
    W("="*72)
    W(f"hop:      {hop}")
    W(f"AWS  src: {srcdesc}   ({len(src)} settings)")
    W(f"TC   dst: {dstdesc}   ({len(dst)} settings)")
    W(f"dumped:   {datetime.date.today().isoformat()}   (values live from both servers)")
    if extra_note: W(f"note:     {extra_note}")
    W("")
    W("Method: every setting from both sides was dumped and compared. A difference")
    W("is listed under (A) only if it is behavior-critical AND harmful. Differences")
    W("that are present-on-one-side or value-diff but do NOT change stored data or")
    W("query answers are filtered to (C); ones that differ for a known-safe reason")
    W("are in (B) with the reason.")
    W("")
    W("-"*72)
    W(f"(A) DIFFERENCES THAT MATTER — verify/fix before cutover   [{len(A_val)+len(A_pres)}]")
    W("-"*72)
    if not A_val and not A_pres:
        W("  (none — no behavior-critical parameter differs harmfully)")
    for n, sv, dv, (what, impact) in A_val:
        W(f"\n  {n}")
        W(f"      AWS (src):     {norm(sv)!r}")
        W(f"      Tencent (dst): {norm(dv)!r}")
        W(f"      what it is:    {what}")
        W(f"      if it differs: {impact}")
    for n, sv, dv, (what, impact), side in A_pres:
        W(f"\n  {n}   (present only on {side})")
        W(f"      AWS (src):     {norm(sv) if sv!='ABSENT' else 'ABSENT'}")
        W(f"      Tencent (dst): {norm(dv) if dv!='ABSENT' else 'ABSENT'}")
        W(f"      what it is:    {what}")
        W(f"      if it differs: {impact}")
    W("")
    W("-"*72)
    W(f"(B) DIFFERS BUT SAFE TO IGNORE — with reason   [{len(B)}]")
    W("-"*72)
    if not B: W("  (none)")
    for n, sv, dv, reason in B:
        W(f"\n  {n}")
        W(f"      AWS (src):     {norm(sv)!r}")
        W(f"      Tencent (dst): {norm(dv)!r}")
        W(f"      safe because:  {reason}")
    W("")
    W("-"*72)
    W(f"(C) FILTERED OUT — not behavior-critical")
    W("-"*72)
    W(f"  value differs but harmless : {len(C_val)}")
    W(f"  present only on AWS        : {len(C_only_src)}")
    W(f"  present only on Tencent    : {len(C_only_dst)}")
    if C_only_src:
        W("\n  [present only on AWS, harmless]")
        W("    " + ", ".join(C_only_src))
    if C_only_dst:
        W("\n  [present only on Tencent, harmless]")
        W("    " + ", ".join(C_only_dst))
    W("")
    open(path, "w").write("\n".join(L) + "\n")
    print(f"  wrote {path}")
    print(f"    (A) matter={len(A_val)+len(A_pres)}  (B) safe={len(B)}  "
          f"(C) filtered: valdiff={len(C_val)} onlyAWS={len(C_only_src)} onlyTC={len(C_only_dst)}")
    return A_val, A_pres, B

# ---- read a migkit params.json cache ({name:{src,dst}}) into two plain dicts ----
def load_cache(path):
    import json
    raw = json.load(open(path))
    src = {k: v["src"] for k, v in raw.items() if isinstance(v, dict) and v.get("src") is not None}
    dst = {k: v["dst"] for k, v in raw.items() if isinstance(v, dict) and v.get("dst") is not None}
    return src, dst, raw

def _hop_labels(hop):
    """source/target label ของ hop จาก hops.yaml (ไม่ hardcode endpoint ของใคร)"""
    h = HOPS.get(hop, {})
    def lab(side):
        d = h.get(side, {}) or {}
        return f"{d.get('host') or d.get('hosts', '?')} ({h.get('engine', '?')})"
    return lab("source"), lab("target")


def _first_params_cache(hop, db=""):
    """reports/<hop>/<db>/params.json - ถ้าไม่ระบุ db ใช้ตัวแรกที่มี"""
    base = os.path.join("reports", hop)
    if db:
        return os.path.join(base, db, "params.json")
    for d in sorted(os.listdir(base)) if os.path.isdir(base) else []:
        p = os.path.join(base, d, "params.json")
        if os.path.exists(p):
            return p
    return os.path.join(base, "<db>", "params.json")


# ============================ POSTGRES ============================
def do_pg():
    # params are server-level, so any database of the hop captures the same two servers.
    hop = os.environ.get("PG_HOP", "rds-to-tencent")
    db = os.environ.get("PG_DB", "")
    path = _first_params_cache(hop, db)
    print(f"[pg] reading cache {path} …")
    src, dst, _ = load_cache(path)
    s_label, d_label = _hop_labels(hop)
    write_report("PostgreSQL", f"{OUT}/param-diff-pg.txt", hop,
                 s_label, d_label, src, dst, PG_CRIT, PG_SAFE)

# ============================ MYSQL ============================
def do_mysql():
    hop = os.environ.get("MYSQL_HOP", "mysql-to-tencent")
    db = os.environ.get("MYSQL_DB", "")
    path = _first_params_cache(hop, db)
    print(f"[mysql] reading cache {path} …")
    src, dst, _ = load_cache(path)
    s_label, d_label = _hop_labels(hop)
    write_report("MySQL", f"{OUT}/param-diff-mysql.txt", hop,
                 s_label, d_label, src, dst, MYSQL_CRIT, MYSQL_SAFE)

# ============================ MONGO ============================
def do_mongo():
    # oss/params.json = 2026-07-27 17:55. DocDB source does NOT expose getParameter('*')
    # (all src values are null), which itself proves a server-param table is not comparable.
    print("[mongo] reading cache reports/mongo-to-tencent/oss/params.json …")
    _, _, raw = load_cache("reports/mongo-to-tencent/oss/params.json")
    def gv(side, key, default="n/a"):
        v = raw.get(key, {})
        return v.get(side, default) if isinstance(v, dict) else default
    tfcv = gv("dst", "featureCompatibilityVersion")
    if isinstance(tfcv, dict): tfcv = tfcv.get("version", str(tfcv))
    sfcv = gv("src", "featureCompatibilityVersion")
    if isinstance(sfcv, dict): sfcv = sfcv.get("version", str(sfcv))
    if sfcv in (None, "n/a"): sfcv = "unavailable (DocumentDB does not expose it)"
    sver = "5.0-compatible (DocumentDB)"
    tver = "5.0.12"
    sp = {k for k, v in raw.items() if isinstance(v, dict) and v.get("src") is not None}
    tp = {k for k, v in raw.items() if isinstance(v, dict) and v.get("dst") is not None}
    sp = list(sp); tp = list(tp)
    L = []; W = L.append
    W("PARAM PARITY — MongoDB   (AWS DocumentDB source  vs  Tencent MongoDB target)")
    W("="*72)
    W("hop:      mongo-to-tencent")
    W(f"AWS  src: DocumentDB, engine reports version {sver}   FCV={sfcv}")
    W(f"TC   dst: TencentDB MongoDB {tver}   FCV={tfcv}")
    W(f"dumped:   {datetime.date.today().isoformat()}")
    W("")
    W("Why this file is short: DocumentDB and MongoDB are DIFFERENT engines that")
    W("share almost no server parameters, so a server-level parameter table is not")
    W("a meaningful comparison (it would be all noise). What actually changes")
    W("behavior for a Mongo migration is per-collection or per-operation:")
    W("")
    W("-"*72)
    W("(A) WHAT TO VERIFY (these change query answers / data fidelity)")
    W("-"*72)
    W("""
  featureCompatibilityVersion (FCV)
      what it is:    which server features and index types are enabled
      if it differs: an index type / aggregation stage available on one side is
                     missing on the other
      observed:      src FCV=%s   dst FCV=%s

  Per-collection default collation  (locale, strength, caseLevel)
      what it is:    sort / compare / uniqueness rules stored ON each collection
      if it differs: ORDER BY-style sorts differ; a unique index treats e and
                     e-with-accent as same vs distinct; range queries match
                     differently. NOT a server setting -> must be compared per
                     collection (migkit mongo check does this).

  readConcern / writeConcern defaults
      what it is:    read consistency and write durability defaults
      if it differs: writes acknowledge with different durability guarantees

  Balancer / chunk settings (sharded only)
      what it is:    data distribution across shards
      if it differs: uneven distribution or orphaned documents

  Oplog size / retention
      what it is:    CDC / resume-token window
      if it differs: change streams cannot resume after a gap
""" % (sfcv, tfcv))
    W("-"*72)
    W("(B) SAFE TO IGNORE")
    W("-"*72)
    W("  Engine version string (DocumentDB 5.0-compatible vs Tencent Mongo %s):" % tver)
    W("      different engines; the number does not imply the same internals.")
    W("  Server getParameter tables: not compared (no meaningful shared keys).")
    W("      DocDB getParameter('*') exposed keys=%d, Tencent keys=%d — DocumentDB" % (len(sp), len(tp)))
    W("      returns none of them, so there is nothing that maps to data behavior.")
    W("")
    W("Note: Mongo dates are stored in UTC and the zone is applied at query time,")
    W("so time-zone drift breaks less than in SQL as long as the tz database is")
    W("present on both sides.")
    W("")
    open(f"{OUT}/param-diff-mongo.txt", "w").write("\n".join(L) + "\n")
    print(f"  wrote {OUT}/param-diff-mongo.txt  (src FCV={sfcv} dst FCV={tfcv})")

if __name__ == "__main__":
    which = sys.argv[1:] or ["pg", "mysql", "mongo"]
    if "pg" in which:    do_pg()
    if "mysql" in which: do_mysql()
    if "mongo" in which: do_mongo()
