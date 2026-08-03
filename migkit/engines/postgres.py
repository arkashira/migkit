import json
import os
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ..config import REPORTS
from ..util import run, tool_env, which
from .base import Engine, RepairAction, Result

PGDC_ROOT = REPORTS / "pgdc"

INVENTORY_SQL = """
with ns as (
  select oid, nspname from pg_namespace
  where nspname not in ('pg_catalog','information_schema')
    and nspname not like 'pg\\_%'
    and nspname not like '\\_\\_%'
)
select case c.relkind when 'v' then 'view' when 'm' then 'matview'
    when 'S' then 'sequence'
    when 'i' then case when i.indisvalid then 'index' else 'invalid-index' end
    when 'I' then case when i.indisvalid then 'index' else 'invalid-index' end
    else 'table' end
  ||'|'||ns.nspname||'.'||c.relname
from pg_class c
join ns on ns.oid = c.relnamespace
left join pg_index i on i.indexrelid = c.oid
where c.relkind in ('r','p','v','m','S','i','I')
  and c.relname not like 'migkit\\_%'
union all
select case p.prokind when 'p' then 'procedure' else 'function' end
  ||'|'||ns.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')'
from pg_proc p join ns on ns.oid = p.pronamespace
where p.prokind in ('f','p')
union all
select 'trigger|'||ns.nspname||'.'||c.relname||'.'||t.tgname
from pg_trigger t
join pg_class c on c.oid = t.tgrelid
join ns on ns.oid = c.relnamespace
where not t.tgisinternal
union all
select case con.contype when 'p' then 'pk' when 'f' then 'fk'
    when 'u' then 'unique' else 'check' end
  ||'|'||ns.nspname||'.'||rel.relname||'.'||con.conname
from pg_constraint con
join pg_class rel on rel.oid = con.conrelid
join ns on ns.oid = con.connamespace
where con.contype in ('p','f','u','c')
union all
select 'extension|'||extname from pg_extension
"""


class PostgresEngine(Engine):
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = True

    USER_TABLES = ("select n.nspname||'.'||c.relname from pg_class c"
                   " join pg_namespace n on n.oid = c.relnamespace"
                   " where c.relkind = 'r'"
                   " and n.nspname not in ('pg_catalog','information_schema')"
                   " and n.nspname not like 'pg\\_%'"
                   " and n.nspname not like '\\_\\_%'"
                   " and c.relname not like 'migkit\\_%' order by 1")

    def _d(self, side, db):
        """Physical db name for a side: source as given, target through the
        hop's db_map so a migration can land in a differently-named db
        (identity when unmapped). Maintenance db 'postgres' never maps."""
        if side == "dst" and db != "postgres":
            return self.hop.target_db(db)
        return db

    def _psql(self, side, db, sql):
        ep = self.hop.source if side == "src" else self.hop.target
        env = {"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15",
               "PGOPTIONS": "-c TimeZone=UTC -c DateStyle=ISO -c statement_timeout=0"}
        p = run(["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
                 "-d", self._d(side, db), "-X", "-At", "-q", "-v",
                 "ON_ERROR_STOP=1", "-c", sql],
                env=env)
        return p.stdout.rstrip("\n")


    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        out = self._psql("src", "postgres",
                         "select datname from pg_database where not datistemplate"
                         " and datname not in ('postgres','rdsadmin') order by 1")
        return [l for l in out.splitlines() if l]

    def _report(self, db):
        return PGDC_ROOT / self.hop.name / db

    def _dump_schema_native(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        p = run(["pg_dump", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
                 "-d", self._d(side, db), "--schema-only", "--no-owner",
                 "--no-privileges", "--no-security-labels", "--no-tablespaces",
                 "--exclude-schema", self.hop.options.get("exclude_schema", "__*"),
                 "--exclude-table", "*.migkit_changelog*"],
                env={"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15"})
        noise = self.hop.options.get("noise_prefix", "")
        keep = []
        for l in p.stdout.splitlines():
            if (l.startswith(("--", "SET ", "\\restrict", "\\unrestrict",
                              "SELECT pg_catalog.set_config")) or not l.strip()):
                continue
            if noise and (f"EVENT TRIGGER {noise}" in l
                          or f"PUBLICATION {noise}" in l):
                continue
            keep.append(l)
        pats = self._ignore_patterns()
        if pats:
            keep = [l for l in keep if not any(pt.search(l) for pt in pats)]
        return "\n".join(keep)

    def check_schema(self, db):
        import difflib
        src = self._dump_schema_native("src", db)
        dst = self._dump_schema_native("dst", db)
        d = self._report(db)
        d.mkdir(parents=True, exist_ok=True)
        (d / "schema-src.sql").write_text(src + "\n")
        (d / "schema-dst.sql").write_text(dst + "\n")
        changed = [l for l in difflib.unified_diff(
                       src.splitlines(), dst.splitlines(), "src", "dst",
                       lineterm="")
                   if l[:1] in "+-" and not l.startswith(("+++", "---"))]
        if changed:
            (d / "schema.diff").write_text("\n".join(changed) + "\n")
            status, line = "diff", f"{len(changed)} changed lines"
        else:
            (d / "schema.diff").unlink(missing_ok=True)
            status, line = "ok", "schema identical (native pg_dump diff)"
        res = [Result("schema", db, status, line, str(d / "schema.diff"),
                      "review diff, apply missing DDL from schema-src.sql")]
        if which("migra"):
            s, t = self.hop.source, self.hop.target
            surl = f"postgresql://{s.user}:{s.password}@{s.host}:{s.port}/{db}"
            turl = (f"postgresql://{t.user}:{t.password}"
                    f"@{t.host}:{t.port}/{self._d('dst', db)}")
            p = subprocess.run(["migra", "--unsafe", turl, surl],
                               capture_output=True, text=True, env=tool_env())
            if p.stdout.strip():
                path = self._report(db) / "migra-fix.sql"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(p.stdout)
                res.append(Result("schema", f"{db} (migra)", "diff",
                                  "migra generated fix DDL", str(path),
                                  "review then apply migra-fix.sql on target"))
        res.append(self.check_objects(db))
        if which("liquibase") and self.hop.options.get("liquibase", True):
            lb = self.check_liquibase(db)
            if lb:
                res.append(lb)
        if which("atlas") and self.hop.options.get("atlas", True):
            at = self.check_atlas(db)
            if at:
                res.append(at)
        return self._atlas_authoritative(res)

    def check_atlas(self, db):
        from urllib.parse import quote
        s, t = self.hop.source, self.hop.target
        su = (f"postgres://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}?sslmode=prefer")
        tu = (f"postgres://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{self._d('dst', db)}?sslmode=prefer")
        try:
            p = run(["atlas", "schema", "diff", "--from", tu, "--to", su,
                     "--exclude", "__*",
                     "--exclude", "*.migkit_changelog"],
                    check=False, timeout=180)
        except Exception:
            return None
        if p.returncode != 0:
            return None
        text = p.stdout.strip()
        if not text or "Schemas are synced" in text:
            return Result("schema", f"{db} (atlas)", "ok", "atlas diff clean")
        out = self._report(db) / "atlas-fix.sql"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n")
        return Result("schema", f"{db} (atlas)", "diff",
                      f"atlas generated {len(text.splitlines())} lines of fix DDL",
                      str(out), "review then apply atlas-fix.sql on target")

    def check_liquibase(self, db):
        s, t = self.hop.source, self.hop.target
        out = self._report(db) / "liquibase-diff.txt"
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            p = run(["liquibase", "diff",
                     f"--url=jdbc:postgresql://{t.host}:{t.port}/"
                     f"{self._d('dst', db)}?sslmode=prefer",
                     f"--username={t.user}", f"--password={t.password}",
                     f"--referenceUrl=jdbc:postgresql://{s.host}:{s.port}/{db}"
                     f"?sslmode=prefer",
                     f"--referenceUsername={s.user}",
                     f"--referencePassword={s.password}"],
                    check=False, timeout=180)
        except Exception:
            return None
        if p.returncode != 0:
            return None
        out.write_text(p.stdout)
        noise = self.hop.options.get("noise_prefix", "")
        bad = [l.strip() for l in p.stdout.splitlines()
               if (l.startswith("Missing") or l.startswith("Unexpected")
                   or l.startswith("Changed"))
               and not l.rstrip().endswith("NONE")
               and "__" not in l and "migkit_changelog" not in l
               and (not noise or noise not in l)]
        if bad:
            return Result("schema", f"{db} (liquibase)", "diff",
                          "; ".join(bad[:6]), str(out),
                          "see liquibase-diff.txt for the full object list")
        return Result("schema", f"{db} (liquibase)", "ok",
                      "liquibase diff clean")

    def check_objects(self, db):
        sides = {}
        for side in ("src", "dst"):
            m = {}
            for line in self._psql(side, db, INVENTORY_SQL).splitlines():
                t, _, name = line.partition("|")
                m.setdefault(t, set()).add(name)
            sides[side] = m
        inv = {}
        for t in sorted(set(sides["src"]) | set(sides["dst"])):
            s = sides["src"].get(t, set())
            d = sides["dst"].get(t, set())
            inv[t] = {"src": len(s), "dst": len(d),
                      "missing": sorted(s - d)[:50],
                      "extra": sorted(d - s)[:50]}
        out = self.hop.report_dir(db) / "objects.json"
        out.write_text(json.dumps(inv, indent=1))
        bad = {t: v for t, v in inv.items()
               if t != "invalid-index" and (v["missing"] or v["extra"])}
        note = ""
        iv = inv.get("invalid-index")
        if iv and iv["src"]:
            note = (f"; note: {iv['src']} invalid index on source"
                    f" ({', '.join(iv['missing'][:3])}), drop and rebuild"
                    " there, pg_dump and most movers skip it")
        if bad:
            parts = []
            for t, v in bad.items():
                p = f"{t} {v['src']}/{v['dst']}"
                if v["missing"]:
                    p += " missing: " + ", ".join(v["missing"][:3])
                if v["extra"]:
                    p += " extra: " + ", ".join(v["extra"][:3])
                parts.append(p)
            return Result("schema", f"{db} objects", "diff",
                          "; ".join(parts) + note, str(out),
                          "create missing objects on target from schema-src.sql")
        total = sum(v["src"] for t, v in inv.items() if t != "invalid-index")
        return Result("schema", f"{db} objects", "ok",
                      f"{total} objects in {len(inv)} types,"
                      f" all present on target{note}")

    PARAM_CRITICAL = ("TimeZone", "client_encoding", "server_encoding",
                      "lc_collate", "lc_ctype", "lc_monetary", "lc_numeric",
                      "lc_time", "DateStyle", "IntervalStyle",
                      "standard_conforming_strings", "bytea_output",
                      "default_transaction_isolation", "extra_float_digits",
                      "wal_level", "integer_datetimes", "backslash_quote",
                      "check_function_bodies", "array_nulls", "search_path",
                      "default_text_search_config")

    def check_params(self, db):
        q = ("select name||chr(31)||coalesce(setting,'') from pg_settings"
             " order by name")

        def pull(side):
            return dict(l.split("\x1f", 1) for l in
                        self._psql(side, db, q).splitlines() if "\x1f" in l)

        return self._param_result(
            db, pull("src"), pull("dst"), self.PARAM_CRITICAL,
            "align the behavior-critical GUCs on the target parameter group"
            " before cutover")

    SEQ_Q = ("select schemaname||'.'||sequencename||'|'||coalesce(last_value,0)"
             " from pg_sequences where schemaname not like '\\_\\_%'"
             " and sequencename not like 'migkit\\_%' order by 1")

    # map every serial/identity sequence to the column it feeds, via the
    # dependency catalog, so we can prove nextval clears that column's max.
    SEQ_OWNED_Q = (
        "select sn.nspname||'.'||s.relname||'|'||ns.nspname||'|'||t.relname"
        "||'|'||a.attname"
        " from pg_class s"
        " join pg_namespace sn on sn.oid = s.relnamespace"
        " join pg_depend d on d.objid = s.oid"
        "  and d.classid = 'pg_class'::regclass"
        "  and d.refclassid = 'pg_class'::regclass and d.deptype in ('a','i')"
        " join pg_class t on t.oid = d.refobjid"
        " join pg_namespace ns on ns.oid = t.relnamespace"
        " join pg_attribute a on a.attrelid = d.refobjid"
        "  and a.attnum = d.refobjsubid"
        " where s.relkind = 'S' and sn.nspname not like '\\_\\_%'"
        " and s.relname not like 'migkit\\_%'")

    def _seq_owned(self, side, db):
        """seqkey (schema.sequence) -> (schema, table, column) it feeds."""
        owned = {}
        for l in self._psql(side, db, self.SEQ_OWNED_Q).splitlines():
            p = l.split("|")
            if len(p) == 4:
                owned[p[0]] = (p[1], p[2], p[3])
        return owned

    def _seq_col_max(self, side, db, owned):
        """seqkey -> max(owned column) on `side`, so we know the floor the
        sequence must clear to avoid a duplicate-key collision on insert."""
        if not owned:
            return {}
        parts = [f"select '{k}|'||coalesce(max(\"{c}\"),0)"
                 f' from "{s}"."{t}"' for k, (s, t, c) in owned.items()]
        out = {}
        for i in range(0, len(parts), 200):
            chunk = " union all ".join(parts[i:i + 200])
            for l in self._psql(side, db, chunk).splitlines():
                if "|" in l:
                    key, _, v = l.rpartition("|")
                    out[key] = int(v)
        return out

    def _seq_next(self, side, db, owned):
        """seqkey -> the value nextval() would return WITHOUT consuming it.
        A never-called sequence returns last_value itself; once called it
        returns last_value + increment. Getting this right is the difference
        between last_value==max (safe, nextval clears it) and a real
        collision, so we read is_called rather than assume."""
        if not owned:
            return {}
        inc = {}
        for l in self._psql(side, db,
                            "select schemaname||'.'||sequencename||'|'"
                            "||increment_by from pg_sequences").splitlines():
            if "|" in l:
                k, _, v = l.rpartition("|")
                inc[k] = int(v)
        parts = []
        for k in owned:
            sch, name = k.split(".", 1)
            parts.append(f"select '{k}'||e'\\t'||last_value||e'\\t'||is_called"
                         f' from "{sch}"."{name}"')
        out = {}
        for i in range(0, len(parts), 200):
            chunk = " union all ".join(parts[i:i + 200])
            for l in self._psql(side, db, chunk).splitlines():
                p = l.split("\t")
                if len(p) == 3:
                    last, called = int(p[1]), p[2] in ("t", "true")
                    out[p[0]] = last + inc.get(p[0], 1) if called else last
        return out

    def _keep_tbl(self, db, t):
        """False if the 'schema.table' is excluded for this db, so it is
        neither verified nor repaired (protects target-owned tables)."""
        sch, _, tbl = t.partition(".")
        return not self.hop.excluded(db, sch, tbl)

    def check_counts(self, db):
        st = [t for t in self._psql("src", db, self.USER_TABLES).splitlines()
              if t and self._keep_tbl(db, t)]
        dt = set(t for t in self._psql("dst", db, self.USER_TABLES).splitlines()
                 if t and self._keep_tbl(db, t))
        bad = [f"{t} missing on target" for t in st if t not in dt]
        bad += [f"{t} extra on target"
                for t in sorted(dt - set(st))]

        def cnt(side, t):
            sch, tbl = t.split(".", 1)
            return int(self._psql(side, db,
                                  f'select count(*) from "{sch}"."{tbl}"') or 0)

        common = [t for t in st if t in dt]
        total = 0
        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            futs = {t: (pool.submit(cnt, "src", t), pool.submit(cnt, "dst", t))
                    for t in common}
            for t in common:
                a, b = futs[t][0].result(), futs[t][1].result()
                total += a
                if a != b:
                    bad.append(f"{t} src={a} dst={b}")
        if bad:
            return [Result("counts", db, "diff", "; ".join(bad[:10]), "",
                           "missing rows show up in check data, fix there")]
        return [Result("counts", db, "ok",
                       f"{len(common)} tables, {total:,} rows both sides")]

    def check_autoinc(self, db):
        """Two verdicts per database. USABLE is the one that matters at
        cutover: every serial/identity sequence on the target must sit above
        its column's max, or the first insert duplicate-keys (the single most
        common migration outage). PARITY is the softer 1-to-1 check that the
        target continues from the same value the source stopped at."""
        src = dict(l.rsplit("|", 1) for l in
                   self._psql("src", db, self.SEQ_Q).splitlines() if l)
        dst = dict(l.rsplit("|", 1) for l in
                   self._psql("dst", db, self.SEQ_Q).splitlines() if l)
        owned = self._seq_owned("dst", db)
        dmax = self._seq_col_max("dst", db, owned)
        dnext = self._seq_next("dst", db, owned)
        collide = []
        for k, (s, t, c) in sorted(owned.items()):
            nxt = dnext.get(k, 0)
            mx = dmax.get(k, 0)
            if mx > 0 and nxt <= mx:
                collide.append(f"{k}: nextval={nxt} <= max({t}.{c})={mx}")
        res = []
        if collide:
            res.append(Result("autoinc", f"{db} usable", "diff",
                              "sequences WILL collide on next insert: "
                              + "; ".join(collide[:8]), "",
                              f"migkit sync {self.hop.name} --db {db} --kind"
                              " sequences --apply  (fix BEFORE cutover)"))
        else:
            res.append(Result("autoinc", f"{db} usable", "ok",
                              f"{len(owned)} owned sequences all clear their"
                              " column max, no collision"
                              if owned else "no owned sequences"))
        parity = [f"{n} src={v} dst={dst.get(n, 'MISSING')}"
                  for n, v in sorted(src.items()) if dst.get(n) != v]
        if parity:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(parity[:8]), "",
                              f"migkit sync {self.hop.name} --db {db} --kind"
                              " sequences --apply"))
        else:
            res.append(Result("autoinc", f"{db} parity", "ok",
                              f"{len(src)} sequences match source last_value"))
        return res

    def _data_fast_native(self, db, stream=None):
        """Per-table checksum on both sides in parallel: commutative
        sum-of-md5 as a Postgres parallel aggregate (no sort, no lock beyond
        a plain SELECT). Emits the same OK/DIFF/ERROR lines the slice-mode
        drilldown and counts merge consume."""
        tables = [t for t in
                  self._psql("src", db, self.USER_TABLES).splitlines()
                  if t and self._keep_tbl(db, t)]
        w = int(self.hop.options.get("checksum_workers", 8))
        h = self._row_hash_expr()

        def csum(side, t):
            sch, tbl = t.split(".", 1)
            return self._psql(side, db,
                f"set max_parallel_workers_per_gather = {w};"
                f" select count(*)||'|'||coalesce(sum(('x'||substr({h},1,16))"
                f'::bit(64)::bigint::numeric), 0) from "{sch}"."{tbl}" t')

        def one(t):
            try:
                a, b = csum("src", t), csum("dst", t)
            except RuntimeError as e:
                return f"{t}: ERROR {str(e).splitlines()[-1][:80]}"
            if a == b:
                return f"{t}: OK rows={a.split('|')[0]} checksum={a.split('|', 1)[1]}"
            return f"{t}: DIFF src={a} dst={b}"

        lines, rc = [], 0
        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            for line in pool.map(one, tables):
                lines.append(line)
                if stream:
                    stream(line)
                if ": OK" not in line:
                    rc = 1
        return rc, "\n".join(lines)

    @staticmethod
    def _parse_fast(out):
        import re as _re
        rows_src = rows_dst = n = 0
        bad = []
        for line in out.splitlines():
            m = _re.match(r"(\S+): OK rows=(\d+)", line)
            if m:
                n += 1
                rows_src += int(m.group(2))
                rows_dst += int(m.group(2))
                continue
            m = _re.match(r"(\S+): DIFF src=(\d+)\|\S+ dst=(\d+)\|\S+", line)
            if m:
                n += 1
                a, b = int(m.group(2)), int(m.group(3))
                rows_src += a
                rows_dst += b
                if a != b:
                    bad.append(f"{m.group(1)} src={a} dst={b}")
        return n, rows_src, rows_dst, bad

    def _counts_from_fast(self, db, out):
        n, rows_src, rows_dst, bad = self._parse_fast(out)
        st = set(self._psql("src", db, self.USER_TABLES).splitlines())
        dt = set(self._psql("dst", db, self.USER_TABLES).splitlines())
        bad += [f"{t} missing on target" for t in sorted(st - dt)]
        bad += [f"{t} extra on target" for t in sorted(dt - st)]
        if bad:
            return Result("counts", db, "diff", "; ".join(bad[:10]), "",
                          "missing rows show up in check data, fix there")
        return Result("counts", db, "ok",
                      f"{n} tables, rows {rows_src:,}=={rows_dst:,}"
                      " (from the checksum pass, no extra scan)")

    def _drilldown_native(self, db, table):
        """Find the differing pks of one table (whole-table for normal
        sizes, PK-index slices for big single-int-pk tables so pgsql_tmp
        never fills). Writes data-<table>.missing/.extra/.changed."""
        cols = self._pk_cols_of(db, table)
        if not cols:
            return None
        sch, tbl = table.split(".", 1)
        qt = f'"{sch}"."{tbl}"'
        pkexpr = "concat_ws(e'\\t', " + ", ".join(
            f'"{c}"::text' for c in cols) + ")"

        def fetch(side, where=""):
            out = {}
            for l in self._psql(side, db,
                                f"select {pkexpr}||'|'||md5(to_jsonb(t)::text)"
                                f" from {qt} t {where}").splitlines():
                k, _, hsh = l.rpartition("|")
                out[k] = hsh
            return out

        n = int(self._psql("src", db, f"select count(*) from {qt}") or 0)
        intpk = self._int_pk(db, sch, tbl)
        missing, extra, changed = [], [], []
        if n > self.hop.slice and intpk:
            mm = self._psql("src", db,
                            f'select coalesce(min("{intpk}"),0)||\'|\'||'
                            f'coalesce(max("{intpk}"),0) from {qt}')
            lo, hi = (int(x) for x in mm.split("|"))
            step = max(1, (hi - lo) // max(1, n // self.hop.slice) + 1)
            for a in range(lo, hi + 1, step):
                w = f'where "{intpk}" >= {a} and "{intpk}" < {a + step}'
                s, dd = fetch("src", w), fetch("dst", w)
                missing += [k for k in s if k not in dd]
                extra += [k for k in dd if k not in s]
                changed += [k for k in s if k in dd and s[k] != dd[k]]
        else:
            s, dd = fetch("src"), fetch("dst")
            missing = [k for k in s if k not in dd]
            extra = [k for k in dd if k not in s]
            changed = [k for k in s if k in dd and s[k] != dd[k]]
        self._write_pk_files(db, table, sorted(missing), sorted(extra),
                             sorted(changed))
        return len(missing), len(extra), len(changed)

    def check_data(self, db, table=None, stream=None, with_counts=False,
                   consistent=False):
        if table:
            r = self._drilldown_native(db, table)
            status = "ok" if r and not any(r) else "diff" if r else "error"
            detail = (f"missing={r[0]} extra={r[1]} changed={r[2]}"
                      if r else "no primary key for row-level compare")
            return [Result("data", f"{db} {table}", status, detail,
                           str(self._report(db)),
                           f"migkit sync {self.hop.name} --db {db} --kind rows")]
        if consistent:
            rc, out = self._fast_consistent(db)
            if stream:
                for line in out.splitlines():
                    stream(line)
        else:
            rc, out = self._data_fast_native(db, stream=stream)
        ev = self.hop.report_dir(db) / "data-evidence.txt"
        ev.write_text(out + "\n")
        pre = [self._counts_from_fast(db, out)] if with_counts else []
        mode = "consistent snapshot, " if consistent else ""
        if rc == 0:
            import re as _re
            rows = sum(int(m) for m in _re.findall(r"rows=(\d+)", out))
            n = out.count(": OK")
            return pre + [Result("data", db, "ok",
                                 f"{mode}{n} tables, {rows:,} rows,"
                                 f" checksums equal both sides", str(ev))]
        bad = [l.split(":")[0] for l in out.splitlines() if ": DIFF" in l]
        err = [l.split(":")[0] for l in out.splitlines() if ": ERROR" in l]
        for t in bad:
            self._drilldown_native(db, t)
        if bad:
            still, healed, how = self._resolve_inflight(db, bad, stream)
            if not still and not err:
                return pre + [Result("data", db, "ok",
                                     f"{mode}all diffs proven in-flight"
                                     " replication: " + "; ".join(how),
                                     str(ev))]
            bad = still
        detail = ""
        if bad:
            detail = f"tables differ: {', '.join(bad)} (pk-level files written)"
            fps = []
            for t in bad[:5]:
                cols = self._column_fingerprint(db, t)
                if cols:
                    fps.append(f"{t} -> {', '.join(cols[:6])}")
            if fps:
                detail += "; drift localized to columns: " + "; ".join(fps)
        if err:
            detail += f" errors: {', '.join(err)}"
        return pre + [Result("data", db, "diff" if bad else "error", detail,
                             str(self._report(db)),
                             f"migkit sync {self.hop.name} --db {db} --kind rows")]

    def _ignore_patterns(self):
        import re as _re
        from ..config import CONF
        pats = []
        for d in (Path(CONF).parent, PGDC_ROOT / "conf"):
            f = d / f"{self.hop.name}.schema-ignore"
            if f.exists():
                pats += [p for p in f.read_text().splitlines() if p.strip()]
        return [_re.compile(p) for p in pats]

    def check_deep(self, db):
        res = []
        rpt = self.hop.report_dir(db)

        # a table with no pk/unique is the quiet trap: DMS and GoldenGate drop
        # its UPDATE/DELETE during CDC and duplicate it on full+CDC, and it
        # can't be verified or repaired by key. Surface it before it bites.
        nopk = [l for l in self._psql("src", db,
                "select n.nspname||'.'||c.relname from pg_class c"
                " join pg_namespace n on n.oid = c.relnamespace"
                " where c.relkind = 'r'"
                " and n.nspname not in ('pg_catalog','information_schema')"
                " and n.nspname not like 'pg\\_%'"
                " and n.nspname not like '\\_\\_%'"
                " and c.relname not like 'migkit\\_%'"
                " and not exists (select 1 from pg_index i"
                "  where i.indrelid = c.oid"
                "  and (i.indisprimary or i.indisunique))"
                " order by 1").splitlines() if l]
        if nopk:
            res.append(Result("deep", f"{db} keys", "diff",
                              f"{len(nopk)} tables have no pk/unique"
                              " (CDC drops their updates/deletes, dups on"
                              f" reload, unverifiable): {', '.join(nopk[:5])}",
                              "", "add a primary key or unique index, or set"
                                  " replica identity full, before migrating"))
        else:
            res.append(Result("deep", f"{db} keys", "ok",
                              "every table has a pk or unique index"))

        # orphans only hide behind NOT VALID fks (pg enforces validated ones)
        fks = [l.split("|") for l in self._psql("dst", db, """
            select c.conname
              ||'|'||c.conrelid::regclass||'|'||c.confrelid::regclass
              ||'|'||(select string_agg(quote_ident(a.attname), ','
                                        order by x.ord)
                      from unnest(c.conkey) with ordinality x(attnum, ord)
                      join pg_attribute a on a.attrelid = c.conrelid
                       and a.attnum = x.attnum)
              ||'|'||(select string_agg(quote_ident(a.attname), ','
                                        order by x.ord)
                      from unnest(c.confkey) with ordinality x(attnum, ord)
                      join pg_attribute a on a.attrelid = c.confrelid
                       and a.attnum = x.attnum)
            from pg_constraint c
            join pg_namespace n on n.oid = c.connamespace
            where c.contype = 'f' and not c.convalidated
              and n.nspname not like '\\_\\_%'""").splitlines() if l]
        orphans = []
        for name, child, parent, ckeys, pkeys in fks:
            cc = ", ".join(f"c.{k}" for k in ckeys.split(","))
            pc = ", ".join(f"p.{k}" for k in pkeys.split(","))
            n = self._psql("dst", db,
                           f"select count(*) from {child} c"
                           f" where ({cc}) is not null and not exists"
                           f" (select 1 from {parent} p where ({pc}) = ({cc}))")
            if n != "0":
                orphans.append(f"{child}.{name}: {n} orphan rows")
        if orphans:
            res.append(Result("deep", f"{db} fk", "diff",
                              "; ".join(orphans[:5]), "",
                              "fix orphans, then alter table ..."
                              " validate constraint on target"))
        else:
            res.append(Result("deep", f"{db} fk", "ok",
                              f"{len(fks)} NOT VALID fks scanned, 0 orphans;"
                              " validate them before cutover" if fks
                              else "all fk constraints validated, no orphans"
                                   " possible"))

        # NOT VALID check constraints enforce new writes but never scanned the
        # existing rows, and the planner distrusts them - a load-time speed
        # hack left unfinished. The fk orphan scan above covers foreign keys;
        # check constraints are the blind spot.
        nvc = [l for l in self._psql("dst", db,
               "select conrelid::regclass::text||'.'||conname"
               " from pg_constraint c"
               " join pg_namespace n on n.oid = c.connamespace"
               " where c.contype = 'c' and not c.convalidated"
               " and n.nspname not like '\\_\\_%'").splitlines() if l]
        if nvc:
            res.append(Result("deep", f"{db} checks", "diff",
                              f"{len(nvc)} check constraints NOT VALIDATED"
                              " (existing rows unchecked, planner distrusts): "
                              + "; ".join(nvc[:5]), "",
                              "alter table ... validate constraint ... on"
                              " target after confirming no violations"))
        else:
            res.append(Result("deep", f"{db} checks", "ok",
                              "all check constraints validated"))

        # some tooling drops DEFERRABLE / INITIALLY DEFERRED when copying a
        # schema; code that relies on deferred checks (bulk reorder inside one
        # transaction) then fails with a constraint violation that never
        # happened on the source. Diff the deferral flags per constraint.
        dfq = ("select conrelid::regclass::text||'.'||conname||'|'"
               "||condeferrable||'|'||condeferred from pg_constraint c"
               " join pg_namespace n on n.oid = c.connamespace"
               " where c.contype in ('p','u','f','c') and c.conrelid <> 0"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'")
        sdf = {l.split("|", 1)[0]: l.split("|", 1)[1] for l in
               self._psql("src", db, dfq).splitlines() if "|" in l}
        ddf = {l.split("|", 1)[0]: l.split("|", 1)[1] for l in
               self._psql("dst", db, dfq).splitlines() if "|" in l}
        defer = [f"{k}: src deferrable/deferred={sdf[k]} dst={ddf[k]}"
                 for k in sorted(sdf) if k in ddf and sdf[k] != ddf[k]]
        if defer:
            res.append(Result("deep", f"{db} deferrable", "diff",
                              f"{len(defer)} constraints changed deferral: "
                              + "; ".join(defer[:5]), "",
                              "alter table ... alter constraint ... deferrable"
                              " initially deferred to match source"))
        else:
            res.append(Result("deep", f"{db} deferrable", "ok",
                              "constraint deferral flags match"))

        # row-level security is a silent-data-loss trap: a non-owner /
        # non-BYPASSRLS role (which a dump or even migkit itself may connect
        # as) sees only policy-permitted rows, so counts and checksums can be
        # a filtered subset with no error. And RLS enabled with zero policies
        # is default-deny - the table looks empty to everyone but the owner.
        rls = [l.split("|") for l in self._psql("src", db,
               "select n.nspname||'.'||c.relname||'|'||"
               "(select count(*) from pg_policies p"
               " where p.schemaname = n.nspname and p.tablename = c.relname)"
               " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
               " where c.relkind = 'r' and c.relrowsecurity"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'").splitlines() if l]
        if rls:
            bypass = self._psql("src", db,
                                "select case when rolsuper or rolbypassrls"
                                " then 'y' else 'n' end from pg_roles"
                                " where rolname = current_user") == "y"
            deny = [r[0] for r in rls if r[1] == "0"]
            msgs = []
            if deny:
                msgs.append(f"{len(deny)} RLS tables have ZERO policies"
                            " (default-deny, read as empty by non-owners): "
                            + ", ".join(deny[:4]))
            if not bypass:
                msgs.append("migkit's source role is subject to RLS on"
                            f" {len(rls)} tables - counts/checksums there may"
                            " be a filtered subset, not the full data")
            res.append(Result("deep", f"{db} rls", "diff" if msgs else "ok",
                              "; ".join(msgs) if msgs
                              else f"{len(rls)} RLS tables, all have policies"
                                   " and migkit reads with a bypass role", "",
                              "verify RLS tables with an owner/BYPASSRLS role;"
                              " recreate missing policies on target"
                              if msgs else ""))
        else:
            res.append(Result("deep", f"{db} rls", "ok",
                              "no row-level security in use"))

        dis = [l for l in self._psql("dst", db,
               "select n.nspname||'.'||c.relname||'.'||t.tgname"
               " from pg_trigger t"
               " join pg_class c on c.oid = t.tgrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " where not t.tgisinternal and t.tgenabled = 'D'")
               .splitlines() if l]
        res.append(Result("deep", f"{db} triggers",
                          "diff" if dis else "ok",
                          "disabled on target: " + ", ".join(dis[:5]) if dis
                          else "no disabled triggers on target", "",
                          "alter table ... enable trigger before cutover"
                          if dis else ""))

        colq = ("select table_schema||'.'||table_name||'.'||column_name"
                "||'|'||coalesce(data_type,'')||'|'||is_nullable"
                "||'|'||coalesce(column_default,'')"
                "||'|'||coalesce(character_maximum_length::text,'')"
                "||'|'||coalesce(numeric_precision::text,'')"
                "||'|'||coalesce(numeric_scale::text,'')"
                " from information_schema.columns"
                " where table_schema not in ('pg_catalog',"
                "'information_schema')"
                " and table_schema not like 'pg\\_%'"
                " and table_schema not like '\\_\\_%'"
                " and table_name not like 'migkit\\_%' order by 1")
        sc = {l.split("|", 1)[0]: l for l in
              self._psql("src", db, colq).splitlines() if l}
        dc = {l.split("|", 1)[0]: l for l in
              self._psql("dst", db, colq).splitlines() if l}
        ignores = self._ignore_patterns()
        drift = [f"src {sc[k]}\ndst {dc[k]}" for k in sorted(sc)
                 if k in dc and sc[k] != dc[k]
                 and not any(p.search(sc[k]) or p.search(dc[k])
                             for p in ignores)]
        if drift:
            out = rpt / "deep-columns.diff"
            out.write_text("\n\n".join(drift) + "\n")
            heads = [d.splitlines()[0].split("|")[0][4:] for d in drift]
            res.append(Result("deep", f"{db} columns", "diff",
                              f"{len(drift)} columns drift"
                              f" (type/null/default/precision):"
                              f" {', '.join(heads[:4])}", str(out),
                              "see deep-columns.diff, align target DDL"))
        else:
            res.append(Result("deep", f"{db} columns", "ok",
                              f"{len(sc)} columns compared, type/null/"
                              "default/precision identical"))

        # narrowing is the dangerous subset of drift: a target column that
        # holds fewer characters, less numeric scale/precision, or a smaller
        # integer than the source silently truncates, rounds, or overflows
        # values (DMS caps unlimited text at varchar(8000); scale loss eats
        # money). Called out on its own, at higher severity than cosmetic drift.
        CHAR_T = ("character varying", "character", "text")
        INTW = {"smallint": 2, "integer": 4, "bigint": 8}
        narrow = []
        for k in sorted(sc):
            if k not in dc:
                continue
            sp, dp = sc[k].split("|"), dc[k].split("|")
            if len(sp) < 7 or len(dp) < 7:
                continue
            st, dt = sp[1], dp[1]
            scm, dcm, spr, ssc = sp[4], dp[4], sp[5], sp[6]
            dpr, dsc = dp[5], dp[6]
            why = None
            if st in CHAR_T and dcm and (not scm or int(dcm) < int(scm)):
                why = f"char {scm or 'unlimited'} -> {dcm}"
            elif st in ("numeric", "decimal") and ssc and dsc \
                    and int(dsc) < int(ssc):
                why = f"numeric scale {ssc} -> {dsc} (rounds)"
            elif st in ("numeric", "decimal") and spr and dpr \
                    and int(dpr) < int(spr):
                why = f"numeric precision {spr} -> {dpr} (overflow)"
            elif INTW.get(st, 0) > INTW.get(dt, 99):
                why = f"{st} -> {dt} (overflow)"
            if why:
                narrow.append(f"{k}: {why}")
        if narrow:
            res.append(Result("deep", f"{db} narrowing", "diff",
                              f"{len(narrow)} target columns NARROWER than"
                              " source (silent truncation/overflow risk): "
                              + "; ".join(narrow[:6]), "",
                              "widen the target column to match source before"
                              " loading, or values are cut/rounded/overflowed"))
        else:
            res.append(Result("deep", f"{db} narrowing", "ok",
                              "no target column narrower than source"))

        # a constant, per-row offset on a timestamp column is the fingerprint
        # of a timezone conversion bug (a mover applying a non-UTC session),
        # not random corruption. Both sides are read with TimeZone=UTC pinned,
        # so a correct instant compares as delta 0; a systematic shift shows
        # the same non-zero delta on every sampled row.
        tsq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
               " from pg_attribute a"
               " join pg_class c on c.oid = a.attrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_type ty on ty.oid = a.atttypid"
               " where c.relkind = 'r' and a.attnum > 0"
               " and not a.attisdropped"
               " and ty.typname in ('timestamp','timestamptz')"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               " and c.relname not like 'migkit\\_%' order by 1")
        shifts = []
        for tc in [l for l in self._psql("src", db, tsq).splitlines() if l][:10]:
            tbl, col = tc.split("|", 1)
            pks = self._pk_cols_of(db, tbl)
            if len(pks) != 1:
                continue
            sch2, t2 = tbl.split(".", 1)
            pk = pks[0]
            q = (f'select "{pk}"::text||e\'\\t\'||extract(epoch from "{col}")'
                 f' from "{sch2}"."{t2}" where "{col}" is not null'
                 f' order by "{pk}" limit 200')
            try:
                sm = dict(l.split("\t") for l in
                          self._psql("src", db, q).splitlines() if "\t" in l)
                dm = dict(l.split("\t") for l in
                          self._psql("dst", db, q).splitlines() if "\t" in l)
            except (RuntimeError, ValueError):
                continue
            deltas = [float(dm[k]) - float(sm[k]) for k in sm if k in dm]
            if len(deltas) < 3:
                continue
            avg = sum(deltas) / len(deltas)
            if max(deltas) - min(deltas) < 1 and abs(avg) >= 1:
                secs = round(avg)
                shifts.append(f"{tbl}.{col}: every row shifted"
                              f" {secs}s (~{secs / 3600:.1f}h)")
        if shifts:
            res.append(Result("deep", f"{db} timeshift", "diff",
                              "uniform timezone offset (systematic, not"
                              " row-level corruption): " + "; ".join(shifts[:5]),
                              "", "target stored a non-UTC wall clock; re-load"
                              " with the source session timezone or convert"
                              " the column"))
        else:
            res.append(Result("deep", f"{db} timeshift", "ok",
                              "no uniform timestamp offset detected"))

        # NULL vs empty-string: Oracle stores '' as NULL, Postgres keeps them
        # distinct, so a migration can silently flip IS NULL semantics and
        # unique behavior. Per text column, compare the null-count and the
        # empty-count on each side; a swap is the fingerprint (and it survives
        # a row checksum only because both are "present").
        txtq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
                " from pg_attribute a"
                " join pg_class c on c.oid = a.attrelid"
                " join pg_namespace n on n.oid = c.relnamespace"
                " join pg_type ty on ty.oid = a.atttypid"
                " where c.relkind = 'r' and a.attnum > 0"
                " and not a.attisdropped"
                " and ty.typname in ('text','varchar','bpchar')"
                " and n.nspname not in ('pg_catalog','information_schema')"
                " and n.nspname not like 'pg\\_%'"
                " and n.nspname not like '\\_\\_%'"
                " and c.relname not like 'migkit\\_%' order by 1")
        neq = []
        for tc in [l for l in self._psql("src", db, txtq).splitlines() if l][:30]:
            tbl, col = tc.split("|", 1)
            sch2, t2 = tbl.split(".", 1)
            q = (f'select count(*) filter (where "{col}" is null)||\'|\'||'
                 f'count(*) filter (where "{col}" = \'\') from "{sch2}"."{t2}"')
            try:
                sv, dv = self._psql("src", db, q), self._psql("dst", db, q)
            except RuntimeError:
                continue
            if sv != dv and "|" in sv and "|" in dv:
                neq.append(f"{tbl}.{col}: src null/empty={sv} dst={dv}")
        if neq:
            res.append(Result("deep", f"{db} nullempty", "diff",
                              f"{len(neq)} text columns differ in NULL vs"
                              " empty-string split (semantic flip): "
                              + "; ".join(neq[:5]), "",
                              "normalize with NULLIF(col,'') / COALESCE per"
                              " column intent; decide which side is canonical"))
        else:
            res.append(Result("deep", f"{db} nullempty", "ok",
                              "NULL vs empty-string consistent on text columns"))

        # charset corruption: a lossy transcode on load leaves U+FFFD
        # replacement characters, so any excess on the target over the source
        # is silent damage; and a SQL_ASCII target validates nothing, letting
        # bad bytes through. (utf8mb4/latin1 traps are mysql-specific.)
        enc = []
        se = self._psql("src", db, "select pg_encoding_to_char(encoding)"
                        " from pg_database where datname = current_database()")
        de = self._psql("dst", db, "select pg_encoding_to_char(encoding)"
                        " from pg_database where datname = current_database()")
        if de == "SQL_ASCII" and se != "SQL_ASCII":
            enc.append(f"target database is SQL_ASCII (no encoding"
                       f" validation); source is {se}")
        elif se != de:
            enc.append(f"server_encoding differs: src={se} dst={de}")
        for tc in [l for l in self._psql("src", db, txtq).splitlines() if l][:30]:
            tbl, col = tc.split("|", 1)
            sch2, t2 = tbl.split(".", 1)
            q = ('select coalesce(sum(char_length("' + col + '")'
                 ' - char_length(replace("' + col + '", U&\'\\FFFD\','
                 " ''))),0) from \"" + sch2 + '"."' + t2 + '"')
            try:
                sv = int(self._psql("src", db, q) or 0)
                dv = int(self._psql("dst", db, q) or 0)
            except (RuntimeError, ValueError):
                continue
            if dv > sv:
                enc.append(f"{tbl}.{col}: {dv - sv} extra U+FFFD replacement"
                           " chars on target")
        if enc:
            res.append(Result("deep", f"{db} encoding", "diff",
                              "; ".join(enc[:6]), "",
                              "re-load affected rows with a non-lossy client"
                              " encoding; never use a SQL_ASCII target"))
        else:
            res.append(Result("deep", f"{db} encoding", "ok",
                              "encodings match, no replacement-char excess"))

        # partitioned tables: a mover can land rows in the DEFAULT catch-all
        # partition, miss a partition bound entirely, or even change the
        # partition key - all of which reroute or strand data silently.
        parts = [l.split("|", 1) for l in self._psql("src", db,
                 "select c.relnamespace::regnamespace||'.'||c.relname"
                 "||'|'||pg_get_partkeydef(c.oid)"
                 " from pg_partitioned_table p"
                 " join pg_class c on c.oid = p.partrelid"
                 " where c.relnamespace::regnamespace::text not like 'pg\\_%'"
                 " and c.relnamespace::regnamespace::text not like"
                 " '\\_\\_%'").splitlines() if "|" in l]
        pbad = []
        for tbl, keydef in parts:
            dkey = self._psql("dst", db,
                              f"select pg_get_partkeydef('{tbl}'::regclass)")
            if not dkey:
                pbad.append(f"{tbl}: not partitioned on target (was {keydef})")
                continue
            if dkey != keydef:
                pbad.append(f"{tbl}: partition key differs"
                            f" src({keydef}) dst({dkey})")
                continue
            bq = ("select coalesce(pg_get_expr(ch.relpartbound, ch.oid),'')"
                  " from pg_inherits i join pg_class ch on ch.oid = i.inhrelid"
                  f" where i.inhparent = '{tbl}'::regclass")
            sb = set(self._psql("src", db, bq).splitlines()) - {""}
            dbnd = set(self._psql("dst", db, bq).splitlines()) - {""}
            miss = sb - dbnd
            if miss:
                pbad.append(f"{tbl}: {len(miss)} partition bound(s) missing on"
                            f" target: {', '.join(sorted(miss)[:2])}")
            dflt = self._psql("dst", db,
                              "select c.relnamespace::regnamespace||'.'"
                              "||c.relname from pg_inherits i"
                              " join pg_class c on c.oid = i.inhrelid"
                              f" where i.inhparent = '{tbl}'::regclass"
                              " and pg_get_expr(c.relpartbound, c.oid)"
                              " = 'DEFAULT'")
            if dflt:
                n = self._psql("dst", db, f"select count(*) from {dflt}")
                if n and int(n) > 0:
                    pbad.append(f"{tbl}: {n} rows stranded in default"
                                f" partition ({dflt})")
        if not parts:
            res.append(Result("deep", f"{db} partitions", "ok",
                              "no partitioned tables"))
        elif pbad:
            res.append(Result("deep", f"{db} partitions", "diff",
                              "; ".join(pbad[:6]), "",
                              "recreate the missing partitions and move rows"
                              " out of the default before cutover"))
        else:
            res.append(Result("deep", f"{db} partitions", "ok",
                              f"{len(parts)} partitioned tables, schemes and"
                              " bounds match, default empty"))

        # generated columns: a bulk load can write a literal into a stored
        # generated column (so it no longer equals its expression), the
        # expression can drift, or a column generated on the source can arrive
        # plain on the target (then a mover happily inserts rotting literals).
        import re as _re
        genq = ("select c.relnamespace::regnamespace||'.'||c.relname"
                "||'|'||a.attname||'|'||a.attgenerated::text"
                "||'|'||coalesce(pg_get_expr(d.adbin, d.adrelid),'')"
                " from pg_attribute a"
                " join pg_class c on c.oid = a.attrelid"
                " left join pg_attrdef d on d.adrelid = a.attrelid"
                "  and d.adnum = a.attnum"
                " where a.attnum > 0 and not a.attisdropped"
                "  and a.attgenerated <> '' and c.relkind = 'r'"
                " and c.relnamespace::regnamespace::text"
                "  not in ('pg_catalog','information_schema')"
                " and c.relnamespace::regnamespace::text not like 'pg\\_%'"
                " and c.relnamespace::regnamespace::text not like '\\_\\_%'")

        def _genmap(side):
            out = {}
            for l in self._psql(side, db, genq).splitlines():
                p = l.split("|", 3)
                if len(p) == 4:
                    out[p[0] + "|" + p[1]] = (p[2], p[3])
            return out

        def _norm(e):
            return _re.sub(r"\s+", "", e).lower()
        sg, dg = _genmap("src"), _genmap("dst")
        gbad = []
        for k, (gen, expr) in sorted(sg.items()):
            tbl, col = k.split("|")
            if k not in dg:
                gbad.append(f"{tbl}.{col}: generated on source,"
                            " plain/missing on target")
                continue
            dexpr = dg[k][1]
            if _norm(expr) != _norm(dexpr):
                gbad.append(f"{tbl}.{col}: generation expression differs")
                continue
            if gen == "s":
                sch2, t2 = tbl.split(".", 1)
                try:
                    n = self._psql("dst", db, f'select count(*) from'
                                   f' "{sch2}"."{t2}" where "{col}"'
                                   f" is distinct from ({dexpr})")
                    if n and int(n) > 0:
                        gbad.append(f"{tbl}.{col}: {n} rows where the stored"
                                    " value != its expression")
                except (RuntimeError, ValueError):
                    pass
        for k in sorted(dg):
            if k not in sg:
                tbl, col = k.split("|")
                gbad.append(f"{tbl}.{col}: generated on target but plain on"
                            " source")
        if gbad:
            res.append(Result("deep", f"{db} generated", "diff",
                              "; ".join(gbad[:6]), "",
                              "align the generation expression / storage on"
                              " target and re-derive the column"))
        else:
            res.append(Result("deep", f"{db} generated", "ok",
                              f"{len(sg)} generated columns match"
                              if sg else "no generated columns"))

        # collation: a unique/pk text column whose target collation is
        # case/accent-insensitive (or just different) can COLLAPSE distinct
        # source rows into duplicates on load - silent data loss. And a
        # glibc/ICU version drift silently corrupts existing btree unique
        # indexes (wrong results, duplicate admission).
        uq = ("select c.relnamespace::regnamespace||'.'||c.relname"
              "||'|'||a.attname||'|'||coalesce(co.collname::text,'default')"
              " from pg_constraint con"
              " join pg_class c on c.oid = con.conrelid"
              " join pg_attribute a on a.attrelid = con.conrelid"
              "  and a.attnum = con.conkey[1]"
              " left join pg_collation co on co.oid = a.attcollation"
              " where con.contype in ('p','u')"
              " and array_length(con.conkey,1) = 1"
              " and a.atttypid in ('text'::regtype,'varchar'::regtype,"
              "'bpchar'::regtype)"
              " and c.relnamespace::regnamespace::text"
              "  not in ('pg_catalog','information_schema')"
              " and c.relnamespace::regnamespace::text not like 'pg\\_%'"
              " and c.relnamespace::regnamespace::text not like '\\_\\_%'")

        def _uqmap(side):
            out = {}
            for l in self._psql(side, db, uq).splitlines():
                p = l.split("|")
                if len(p) == 3:
                    out[p[0] + "|" + p[1]] = p[2]
            return out
        su, du = _uqmap("src"), _uqmap("dst")
        cbad = []
        for k, scoll in sorted(su.items()):
            dcoll = du.get(k)
            if not dcoll or dcoll == scoll:
                continue
            tbl, col = k.split("|")
            sch2, t2 = tbl.split(".", 1)
            detail = (f"{tbl}.{col}: unique-key collation src({scoll})"
                      f" != dst({dcoll})")
            if dcoll != "default":
                try:
                    n = self._psql("src", db,
                                   f'select count(*) from (select 1 from'
                                   f' "{sch2}"."{t2}" group by "{col}"'
                                   f' collate "{dcoll}" having count(*) > 1) x')
                    if n and int(n) > 0:
                        detail += (f"; {n} source groups COLLAPSE to duplicates"
                                   " under the target collation (data loss)")
                except RuntimeError:
                    detail += "; collapse untestable (collation not on source)"
            cbad.append(detail)
        try:
            v = self._psql("dst", db,
                           "select count(*) from pg_depend d"
                           " where d.refclassid = 'pg_collation'::regclass"
                           " and d.refobjversion <> ''"
                           " and d.refobjversion <>"
                           " pg_collation_actual_version(d.refobjid)")
            if v and int(v) > 0:
                cbad.append(f"{v} target objects built under a stale collation"
                            " version (glibc/ICU drift) - unique indexes may be"
                            " corrupt; reindex then refresh collation version")
        except RuntimeError:
            pass
        if cbad:
            res.append(Result("deep", f"{db} collation", "diff",
                              "; ".join(cbad[:6]), "",
                              "match the unique-key collation to source (or"
                              " dedup first); reindex on version drift"))
        else:
            res.append(Result("deep", f"{db} collation", "ok",
                              "unique-key collations match, no version drift"))

        # float columns: FLOAT/DOUBLE are IEEE approximations, so a mover that
        # changes precision (float4->float8 widening, a text round-trip) drifts
        # them. Compare per-row with a relative tolerance, so genuine drift is
        # caught without false-flagging a bit-identical copy. This is a
        # diagnostic layer; the row checksum stays exact.
        tol = float(self.hop.options.get("float_tolerance", 1e-9))
        fcols = [l for l in self._psql("src", db,
                 "select n.nspname||'.'||c.relname||'|'||a.attname"
                 " from pg_attribute a join pg_class c on c.oid = a.attrelid"
                 " join pg_namespace n on n.oid = c.relnamespace"
                 " join pg_type ty on ty.oid = a.atttypid"
                 " where c.relkind = 'r' and a.attnum > 0"
                 " and not a.attisdropped and ty.typname in ('float4','float8')"
                 " and n.nspname not in ('pg_catalog','information_schema')"
                 " and n.nspname not like 'pg\\_%'"
                 " and n.nspname not like '\\_\\_%'"
                 " and c.relname not like 'migkit\\_%'").splitlines() if l]
        fbad = []
        for tc in fcols[:10]:
            tbl, col = tc.split("|", 1)
            pks = self._pk_cols_of(db, tbl)
            if len(pks) != 1:
                continue
            sch2, t2 = tbl.split(".", 1)
            pk = pks[0]
            q = (f'select "{pk}"::text||e\'\\t\'||"{col}" from "{sch2}"."{t2}"'
                 f' where "{col}" is not null order by "{pk}" limit 500')
            try:
                sm = dict(l.split("\t") for l in
                          self._psql("src", db, q).splitlines() if "\t" in l)
                dm = dict(l.split("\t") for l in
                          self._psql("dst", db, q).splitlines() if "\t" in l)
            except (RuntimeError, ValueError):
                continue
            worst, nd = 0.0, 0
            for k in sm:
                if k not in dm:
                    continue
                try:
                    a, b = float(sm[k]), float(dm[k])
                except ValueError:
                    continue
                d = abs(a - b)
                if d > tol * max(abs(a), abs(b), 1.0):
                    nd += 1
                    worst = max(worst, d)
            if nd:
                fbad.append(f"{tbl}.{col}: {nd} values drift beyond tolerance"
                            f" (max {worst:g})")
        if fbad:
            res.append(Result("deep", f"{db} float", "diff",
                              "; ".join(fbad[:5]), "",
                              "float precision changed on target (widening or"
                              " round-trip); use numeric for exact columns, or"
                              " set options.float_tolerance to accept it"))
        else:
            res.append(Result("deep", f"{db} float", "ok",
                              "float columns within tolerance"
                              if fcols else "no float columns"))

        # exotic types can render differently across builds; sample and
        # compare their actual text so a checksum can't hide it
        exq = ("select n.nspname||'.'||c.relname||'|'||a.attname"
               " from pg_attribute a"
               " join pg_class c on c.oid = a.attrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_type ty on ty.oid = a.atttypid"
               " where c.relkind = 'r' and a.attnum > 0"
               " and not a.attisdropped"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               " and c.relname not like 'migkit\\_%'"
               " and (ty.typtype in ('e','c','d','r','m') or ty.typname in"
               " ('money','point','polygon','path','circle','box','line',"
               "'lseg','tsvector','tsquery','xml','interval','bit','varbit'))")
        exotic = [l.split("|") for l in
                  self._psql("src", db, exq).splitlines() if l][:20]

        def _esc(v):
            return "'" + v.replace("'", "''") + "'"

        drifts = []
        audited = 0
        for tbl, col in exotic:
            pks = self._pk_cols_of(db, tbl)
            if not pks:
                continue
            sch2, t2 = tbl.split(".", 1)
            pkexpr = ("concat_ws(e'\\t', "
                      + ", ".join(f'"{p}"::text' for p in pks) + ")")
            q = (f'select {pkexpr}||\'|\'||"{col}"::text'
                 f' from "{sch2}"."{t2}" where "{col}" is not null limit 5')
            try:
                a = dict(l.split("|", 1) for l in
                         self._psql("src", db, q).splitlines() if "|" in l)
            except RuntimeError:
                continue
            if not a:
                continue
            audited += 1
            if len(pks) == 1:
                where = (f'"{pks[0]}"::text in ('
                         + ", ".join(_esc(k) for k in a) + ")")
            else:
                tup = ", ".join(f'"{p}"::text' for p in pks)
                vals = ", ".join(
                    "(" + ", ".join(_esc(x) for x in k.split("\t")) + ")"
                    for k in a)
                where = f"({tup}) in ({vals})"
            qd = (f'select {pkexpr}||\'|\'||"{col}"::text'
                  f' from "{sch2}"."{t2}" where {where}')
            try:
                b = dict(l.split("|", 1) for l in
                         self._psql("dst", db, qd).splitlines() if "|" in l)
            except RuntimeError:
                drifts.append(f"{tbl}.{col}: unreadable on target")
                continue
            for k, v in a.items():
                if k in b and b[k] != v:
                    drifts.append(f"{tbl}.{col}: renders differently"
                                  f" (src {v[:40]!r} dst {b[k][:40]!r})")
                    break
        res.append(Result("deep", f"{db} render",
                          "diff" if drifts else "ok",
                          "; ".join(drifts[:5]) if drifts
                          else (f"{audited} exotic-typed columns sampled,"
                                " rendering identical" if audited
                                else "no exotic-typed columns"), "",
                          "check type definitions/versions on target;"
                          " consider options.checksum: jsonb" if drifts
                          else ""))

        mvs = [l for l in self._psql("src", db,
               "select n.nspname||'.'||c.relname from pg_class c"
               " join pg_namespace n on n.oid = c.relnamespace"
               " where c.relkind = 'm'"
               " and n.nspname not like '\\_\\_%'").splitlines() if l]
        stale = []
        for m in mvs:
            try:
                pop = self._psql("dst", db, "select relispopulated"
                                 f" from pg_class where oid = '{m}'::regclass")
            except RuntimeError:
                stale.append(f"{m}: missing on target")
                continue
            if pop != "t":
                stale.append(f"{m}: not populated on target")
                continue
            # count misses a stale mv with the same size; checksum content
            q = ("select count(*)||'|'||coalesce(sum(('x'||substr("
                 "md5(t::text),1,16))::bit(64)::bigint::numeric), 0)"
                 f" from {m} t")
            a = self._psql("src", db, q)
            b = self._psql("dst", db, q)
            if a != b:
                stale.append(f"{m}: rows|checksum src={a} dst={b},"
                             " stale on target")
        res.append(Result("deep", f"{db} matviews",
                          "diff" if stale else "ok",
                          "; ".join(stale[:5]) if stale
                          else f"{len(mvs)} matviews populated and equal"
                          if mvs else "no matviews", "",
                          "refresh materialized view ... on target"
                          if stale else ""))

        gq = ("select grantee||'|'||table_schema||'.'||table_name"
              "||'|'||privilege_type"
              " from information_schema.table_privileges"
              " where table_schema not in ('pg_catalog',"
              "'information_schema')"
              " and table_schema not like '\\_\\_%'"
              " and grantee not in ('PUBLIC')"
              " and grantee not like 'pg\\_%' and grantee not like 'rds%'"
              " and grantee not like '%tencent%'"
              " and table_name not like 'migkit\\_%'")
        ga = set(self._psql("src", db, gq).splitlines()) - {""}
        gb = set(self._psql("dst", db, gq).splitlines()) - {""}
        droles = set(self._psql("dst", db,
                                "select rolname from pg_roles").splitlines())
        sroles = set(self._psql("src", db,
                                "select rolname from pg_roles").splitlines())
        miss = sorted(g for g in ga - gb if g.split("|")[0] in droles)
        extra = sorted(g for g in gb - ga if g.split("|")[0] in sroles)
        if miss or extra:
            res.append(Result("deep", f"{db} grants", "diff",
                              (f"{len(miss)} grants missing on target"
                               + (": " + "; ".join(
                                   g.replace("|", " ") for g in miss[:3])
                                  if miss else "")
                               + (f"; {len(extra)} extra" if extra else "")),
                              "", "re-grant on target (pg_dump drops grants"
                                  " with --no-privileges)"))
        else:
            res.append(Result("deep", f"{db} grants", "ok",
                              f"{len(ga)} table grants match"
                              " (roles present both sides)"))

        # a SERIAL/identity column depends on a sequence with its OWN acl; the
        # classic trap is granting the table but not its sequence, so the app's
        # inserts fail with "permission denied for sequence". Table grants
        # above miss it entirely - diff sequence grants separately.
        sgq = ("select pg_get_userbyid(a.grantee)||'|'||n.nspname||'.'"
               "||c.relname||'|'||a.privilege_type from pg_class c"
               " join pg_namespace n on n.oid = c.relnamespace"
               " cross join lateral aclexplode(c.relacl) a"
               " where c.relkind = 'S'"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               " and c.relname not like 'migkit\\_%'")
        sga = set(self._psql("src", db, sgq).splitlines()) - {""}
        sgb = set(self._psql("dst", db, sgq).splitlines()) - {""}
        smiss = sorted(g for g in sga - sgb if g.split("|")[0] in droles)
        if smiss:
            res.append(Result("deep", f"{db} seq-grants", "diff",
                              f"{len(smiss)} sequence grants missing on target"
                              " (inserts will hit 'permission denied for"
                              " sequence'): "
                              + "; ".join(g.replace("|", " ")
                                          for g in smiss[:4]), "",
                              "grant usage/select/update on the sequence to the"
                              " app role on target"))
        else:
            res.append(Result("deep", f"{db} seq-grants", "ok",
                              f"{len(sga)} sequence grants match"
                              if sga else "no explicit sequence grants"))

        # a missing or version-mismatched extension breaks its functions and
        # can fail the restore outright; the mover copies data, not CREATE
        # EXTENSION. Diff the installed set (plpgsql is always present).
        exq = ("select extname||' '||extversion from pg_extension"
               " where extname <> 'plpgsql'")
        se = set(self._psql("src", db, exq).splitlines()) - {""}
        de = set(self._psql("dst", db, exq).splitlines()) - {""}
        dn = {e.split(" ")[0] for e in de}
        miss = sorted(e.split(" ")[0] for e in se if e.split(" ")[0] not in dn)
        vers = sorted(e for e in se if e.split(" ")[0] in dn and e not in de)
        if miss or vers:
            det = []
            if miss:
                det.append(f"{len(miss)} missing on target: "
                           + ", ".join(miss[:5]))
            if vers:
                det.append("version mismatch: " + ", ".join(vers[:3]))
            res.append(Result("deep", f"{db} extensions", "diff",
                              "; ".join(det), "",
                              "create extension ... on target (and install its"
                              " shared library) before the app depends on it"))
        else:
            res.append(Result("deep", f"{db} extensions", "ok",
                              f"{len(se)} extensions match" if se
                              else "no non-default extensions"))

        pkq = ("select n.nspname||'|'||c.relname||'|'||a.attname"
               " from pg_index i"
               " join pg_class c on c.oid = i.indrelid"
               " join pg_namespace n on n.oid = c.relnamespace"
               " join pg_attribute a on a.attrelid = i.indrelid"
               " and a.attnum = i.indkey[0]"
               " where i.indisprimary and i.indnatts = 1"
               " and a.atttypid in ('smallint'::regtype::oid,"
               "'integer'::regtype::oid,'bigint'::regtype::oid)"
               " and c.relkind = 'r'"
               " and n.nspname not in ('pg_catalog','information_schema')"
               " and n.nspname not like 'pg\\_%'"
               " and n.nspname not like '\\_\\_%'"
               " and c.relname not like 'migkit\\_%' order by 1")
        spk = [l.split("|") for l in
               self._psql("src", db, pkq).splitlines() if l]
        dpk = {tuple(l.split("|")[:2]) for l in
               self._psql("dst", db, pkq).splitlines() if l}
        both = [(s, t, c) for s, t, c in spk if (s, t) in dpk]

        def maxes(side, triples):
            out = {}
            for i in range(0, len(triples), 200):
                parts = [f"select '{s}.{t}|'||coalesce(max(\"{c}\"), 0)"
                         f' from "{s}"."{t}"'
                         for s, t, c in triples[i:i + 200]]
                for l in self._psql(side, db,
                                    " union all ".join(parts)).splitlines():
                    k, _, v = l.rpartition("|")
                    out[k] = int(v)
            return out

        if both:
            am, bm = maxes("src", both), maxes("dst", both)
            ahead = [f"{k} src_max={am[k]} dst_max={bm[k]}"
                     for k in sorted(am) if bm.get(k, 0) > am[k]]
            behind = [f"{k} src_max={am[k]} dst_max={bm[k]}"
                      for k in sorted(am) if bm.get(k, 0) < am[k]]
            if ahead:
                res.append(Result("deep", f"{db} boundary", "diff",
                                  f"target max(pk) AHEAD of source on"
                                  f" {len(ahead)}: {'; '.join(ahead[:4])}",
                                  "", "writes landing on target or"
                                      " double-apply, find the writer"
                                      " before cutover"))
            else:
                note = (f"; {len(behind)} behind (replication lag):"
                        f" {'; '.join(behind[:3])}" if behind else "")
                res.append(Result("deep", f"{db} boundary", "ok",
                                  f"max(pk) checked on {len(both)} tables,"
                                  f" none ahead of source{note}"))
        else:
            res.append(Result("deep", f"{db} boundary", "ok",
                              "no single-int-pk tables to boundary-check"))
        return res

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            q = ("select schemaname||'.'||sequencename||'|'||coalesce(last_value,0)"
                 " from pg_sequences"
                 " where schemaname not like '\\_\\_%'"
                 " and sequencename not like 'migkit\\_%'")
            src = dict(l.rsplit("|", 1) for l in
                       self._psql("src", db, q).splitlines() if l)
            dst = dict(l.rsplit("|", 1) for l in
                       self._psql("dst", db, q).splitlines() if l)
            owned = self._seq_owned("dst", db)
            dmax = self._seq_col_max("dst", db, owned)
            stmts, undo, refuse = [], [], []
            for name, v in sorted(src.items()):
                cur = dst.get(name)
                mx = dmax.get(name, 0)
                sv = int(v or 0)
                # target column already past source last_value = someone
                # wrote to the target; do not paper over it, surface the writer
                if name in owned and mx > sv:
                    refuse.append(f"{name}: target max={mx} > source"
                                  f" last={sv} (writes landed on target?)")
                    continue
                # GREATEST(source, target max) can never sit below a live row,
                # so nextval is guaranteed to clear the column with no gap
                target = max(sv, mx)
                if target == 0 or (cur is not None and int(cur) == target):
                    continue
                stmts.append(f"select setval('{name}', {target}, true);"
                             f"  -- src={sv} dstmax={mx}"
                             f" dst now {cur if cur is not None else 'MISSING'}")
                if cur == "0":
                    undo.append(f"select setval('{name}', 1, false);")
                elif cur is not None:
                    undo.append(f"select setval('{name}', {cur}, true);")
            same = sum(1 for n, v in src.items() if dst.get(n) == v)
            note = f"{len(stmts)} sequences set, {same} already equal"
            if refuse:
                note += (f"; REFUSED {len(refuse)} (target ahead of source): "
                         + "; ".join(refuse[:4]))
            if stmts or refuse:
                actions.append(RepairAction(db, "sequences", stmts, undo, note))
        if kind in ("rows", "all"):
            rpt = self._report(db)
            tables = set()
            if rpt.exists():
                for f in rpt.glob("data-*.missing"):
                    tables.add(f.name[len("data-"):-len(".missing")])
                for f in rpt.glob("data-*.extra"):
                    tables.add(f.name[len("data-"):-len(".extra")])
                for f in rpt.glob("data-*.changed"):
                    tables.add(f.name[len("data-"):-len(".changed")])
            for t in sorted(tables):
                if not self._keep_tbl(db, t):
                    continue
                counts = []
                for kind_ in ("missing", "extra", "changed"):
                    f = rpt / f"data-{t}.{kind_}"
                    n = sum(1 for _ in f.open()) if f.exists() else 0
                    if n:
                        counts.append(f"{kind_}={n}")
                actions.append(RepairAction(
                    db, "rows", [f"resync-rows {t}"], [],
                    f"{t}: {', '.join(counts) or 'no pk files'},"
                    " deleted rows saved to undo before recopy"))
        if kind in ("schema", "all"):
            act = self._schema_repair_action(db)
            if act:
                actions.append(act)
        return actions

    def _schema_repair_action(self, db):
        """DDL that makes the target's objects (columns, indexes, PK/FK,
        views, functions, procedures, triggers, sequences-as-objects) match
        the source, generated by atlas - the authoritative schema differ -
        with the reverse DDL captured as undo. Data is untouched; this only
        aligns structure."""
        if not hasattr(self, "migration_pair"):
            return None
        fwd, undo = self.migration_pair(db)
        if not fwd:
            return None
        return RepairAction(db, "schema", fwd.splitlines(),
                            (undo or "").splitlines(),
                            "atlas-generated DDL to align target objects to"
                            " source (review before --apply; runs in one"
                            " transaction, reverse DDL saved to undo)")

    def apply(self, db, action):
        if action.kind == "sequences":
            self._psql("dst", db,
                       "\n".join(s.split("  --")[0] for s in action.statements))
        elif action.kind == "schema":
            # one psql call: multi-statement + $$-quoted bodies apply intact
            blob = "begin;\n" + "\n".join(action.statements) + "\ncommit;"
            self._psql("dst", db, blob)
        else:
            for stmt in action.statements:
                self._repair_rows_native(db, stmt.split(" ", 1)[1])

    def _psql_run(self, side, db, script):
        """Run a multi-statement psql script from stdin so client-side
        \\copy works (needed for the temp-pk join repair)."""
        ep = self.hop.source if side == "src" else self.hop.target
        env = tool_env({"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15",
                        "PGOPTIONS": "-c statement_timeout=0"})
        p = subprocess.run(
            ["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
             "-d", self._d(side, db), "-X", "-q", "-v", "ON_ERROR_STOP=1"],
            input=script, capture_output=True, text=True, env=env)
        if p.returncode:
            raise RuntimeError((p.stdout + p.stderr)[-400:])
        return p.stdout

    def _repair_rows_native(self, db, t):
        """Set-based row repair: pull the rows to (re)copy from source into a
        temp file via a temp-pk join, save the target rows they overwrite as
        undo, delete extra/changed on target, then copy the source rows in.
        session_replication_role=replica keeps FKs/triggers quiet, with a
        fallback when the target refuses it."""
        d = self._report(db)
        sch, tbl = t.split(".", 1)
        qt = f'"{sch}"."{tbl}"'
        pks = self._pk_cols_of(db, t)
        if not pks:
            return
        cols_decl = ", ".join(f"c{i + 1} text" for i in range(len(pks)))
        cond = " and ".join(f't."{pks[i]}"::text = p.c{i + 1}'
                            for i in range(len(pks)))

        def read(kind):
            f = d / f"data-{t}.{kind}"
            return f.read_text() if f.exists() else ""

        # on_conflict=keep-target preserves rows the target changed itself:
        # only fix missing (insert) and extra (delete), never overwrite a
        # differing row. source-wins (default) reconciles everything.
        keep = self.hop.options.get("on_conflict") == "keep-target"
        chg = "" if keep else read("changed")
        copy_pks = read("missing") + chg
        del_pks = read("extra") + chg
        work = Path(tempfile.mkdtemp())
        undo = d / "undo"
        undo.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        try:
            (work / "copy.pks").write_text(copy_pks)
            (work / "del.pks").write_text(del_pks)
            if copy_pks.strip():
                self._psql_run("src", db,
                    f"create temp table _pk ({cols_decl});\n"
                    f"\\copy _pk from '{work}/copy.pks'\n"
                    f"\\copy (select t.* from {qt} t join _pk p on {cond})"
                    f" to '{work}/rows.out'\n")
            body = (f"create temp table _pk ({cols_decl});\n"
                    f"\\copy _pk from '{work}/del.pks'\n"
                    f"\\copy (select t.* from {qt} t join _pk p on {cond})"
                    f" to '{undo}/{ts}-{tbl}.rows'\n"
                    f"delete from {qt} t using _pk p where {cond};\n")
            if (work / "rows.out").exists():
                body += f"\\copy {qt} from '{work}/rows.out'\n"
            try:
                self._psql_run("dst", db,
                               "set session_replication_role = replica;\n" + body)
            except RuntimeError as e:
                if "session_replication_role" not in str(e):
                    raise
                self._psql_run("dst", db, body)
            with (undo / "manifest.txt").open("a") as m:
                m.write(f"{ts} {t}: re-copy {undo}/{ts}-{tbl}.rows to undo\n")
        finally:
            import shutil as _sh
            _sh.rmtree(work, ignore_errors=True)

    def setup_target_plan(self, db):
        s, t = self.hop.source, self.hop.target
        tdb = self._d("dst", db)
        return [
            f"pg_dumpall -h {s.host} -p {s.port} -U {s.user} --globals-only"
            f" > globals.sql   # review roles, then apply on target",
            f"pg_dump -h {s.host} -p {s.port} -U {s.user} -d {db} -Fc --schema-only"
            f" -f {db}.schema.dump",
            f"createdb -h {t.host} -p {t.port} -U {t.user} {tdb}"
            f"   # match encoding/locale with source",
            f"pg_restore -h {t.host} -p {t.port} -U {t.user} -d {tdb} --no-owner"
            f" {db}.schema.dump",
            f"-- drop or disable FK constraints and triggers on target before full load,"
            f" keep PKs (script them first: they are your rollback)",
            f"-- then start the migration service in data-only mode into existing tables",
        ]

    def snapshot_state(self, db, state_dir, kind="all"):
        seqs = self._psql("dst", db,
                          "select schemaname||'.'||sequencename||'|'||"
                          "coalesce(last_value,1)||'|'||(last_value is not null)"
                          " from pg_sequences"
                          " where schemaname not like '\\_\\_%'")
        (state_dir / "dst-sequences.txt").write_text((seqs + "\n") if seqs else "")
        # a sequence-only repair rolls back from the sequence snapshot alone;
        # skip the schema dump, which is slow (and can stall) on big partitioned
        # databases and buys nothing here.
        if kind == "sequences":
            return
        ep = self.hop.target
        p = run(["pg_dump", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
                 "-d", self._d("dst", db), "--schema-only", "--no-owner",
                 "--no-privileges"],
                env={"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15"})
        (state_dir / "dst-schema.sql").write_text(p.stdout)

    def assess(self):
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope,
                          "item": item, "detail": str(detail)})

        sv = self._psql("src", "postgres", "show server_version")
        dv = self._psql("dst", "postgres", "show server_version")
        add("pass" if sv.split(".")[0] == dv.split(".")[0] else "warn",
            "instance", "server version match", f"src {sv} / dst {dv}")

        # read replica (pg_is_in_recovery=t) rejects writes incl SELECT FOR
        # UPDATE (25006): fatal as a target, ok-for-checks as a source
        src_ro = self._in_recovery("src", "postgres")
        dst_ro = self._in_recovery("dst", "postgres")
        add("warn" if src_ro else "pass", "instance",
            "source endpoint role",
            "READ REPLICA (read-only) - ok for checks, but point at the"
            " writer/cluster endpoint for replication and the LSN fence"
            if src_ro else "primary / writer (writable)")
        add("fail" if dst_ro else "pass", "instance",
            "target endpoint is writable (not a read replica)",
            "READ REPLICA (read-only) - cannot migrate or repair into it,"
            " and apps get SQLSTATE 25006 on SELECT FOR UPDATE; use the"
            " writer/cluster endpoint" if dst_ro else "primary / writer")

        wal = self._psql("src", "postgres", "show wal_level")
        add("pass" if wal == "logical" else "fail", "instance",
            "wal_level=logical on source (required for CDC)", wal)
        slots = self._psql("src", "postgres",
                           "select count(*)||' used / '||"
                           "current_setting('max_replication_slots')||' max'"
                           " from pg_replication_slots")
        add("pass", "instance", "replication slots", slots)

        # a lagging or abandoned slot pins WAL and can silently fill the source
        # disk (source outage mid-migration). wal_status is pg13+, so probe it
        # and fall back cleanly on older majors.
        try:
            rows = [l for l in self._psql("src", "postgres",
                    "select slot_name||'|'||active||'|'"
                    "||coalesce(wal_status,'?')||'|'||coalesce("
                    "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),"
                    " restart_lsn)),'?') from pg_replication_slots"
                    ).splitlines() if l]
            for r in rows:
                name, active, status, retained = r.split("|")
                if status == "lost":
                    add("fail", "instance", f"replication slot '{name}'",
                        f"WAL LOST - replication broken, retained {retained}")
                elif active in ("f", "false"):
                    add("warn", "instance", f"replication slot '{name}'",
                        "INACTIVE (no consumer) - pins WAL, retained"
                        f" {retained}; drop it if abandoned")
                elif status in ("unreserved", "extended"):
                    add("warn", "instance", f"replication slot '{name}'",
                        f"wal_status {status}, retained {retained} -"
                        " approaching the retention limit")
            if rows:
                mk = self._psql("src", "postgres",
                                "show max_slot_wal_keep_size")
                if mk.strip() in ("-1", "-1B"):
                    add("warn", "instance", "max_slot_wal_keep_size",
                        "-1 (unbounded): an inactive slot can fill the disk;"
                        " set a limit so a stalled consumer can't take the"
                        " source down")
        except RuntimeError:
            pass
        lrt = self._psql("src", "postgres",
                         "select count(*) from pg_stat_activity where"
                         " xact_start is not null"
                         " and now() - xact_start > interval '10 minutes'")
        add("pass" if lrt == "0" else "warn", "instance",
            "transactions open longer than 10 minutes", lrt)

        # pg_authid has the real hash (pg_roles masks it); needs superuser,
        # fall back to name-only when blocked
        roles_q = ("select rolname||'|'||coalesce(rolpassword,'')"
                   " from pg_authid"
                   " where rolcanlogin"
                   " and rolname not like 'pg\\_%'"
                   " and rolname not like 'rds%'"
                   " and rolname not like '%tencent%' order by 1")
        try:
            src_pairs = dict(l.split("|", 1) for l in
                             self._psql("src", "postgres", roles_q).splitlines()
                             if l)
            dst_pairs = dict(l.split("|", 1) for l in
                             self._psql("dst", "postgres", roles_q).splitlines()
                             if l)
        except RuntimeError:
            # no pg_authid access (managed service): fall back to name-only
            names_q = roles_q.replace(
                "||'|'||coalesce(rolpassword,'')", "")
            names_q = ("select rolname from pg_roles r"
                       " where rolcanlogin"
                       " and r.rolname not like 'pg\\_%'"
                       " and r.rolname not like 'rds%'"
                       " and r.rolname not like '%tencent%' order by 1")
            src_pairs = {n: "" for n in
                         self._psql("src", "postgres", names_q).splitlines()}
            dst_pairs = {n: "" for n in
                         self._psql("dst", "postgres", names_q).splitlines()}
        sa, da = set(src_pairs), set(dst_pairs)
        miss = sorted(sa - da)
        add("pass" if not miss else "warn", "instance",
            "login roles present on target",
            f"{len(sa)} src / {len(da)} dst"
            + (f", missing: {', '.join(miss[:5])}" if miss else ""))
        # role on both sides but hash differs = DTS carried the user, not the
        # password -> apps can't authenticate on target
        drift = sorted(n for n in (sa & da)
                       if src_pairs[n] and dst_pairs[n]
                       and src_pairs[n] != dst_pairs[n])
        if any(src_pairs.values()):
            add("pass" if not drift else "fail", "instance",
                "role passwords match source (login will work on target)",
                "all match" if not drift
                else f"password differs for: {', '.join(drift[:5])}"
                     " -> reset on target or apps cannot log in")

        avail = set(self._psql("dst", "postgres",
                               "select name from pg_available_extensions")
                    .splitlines())
        for db in self.databases():
            try:
                exts = set(self._psql("src", db,
                                      "select extname from pg_extension")
                           .splitlines())
                gap = sorted(exts - avail)
                add("pass" if not gap else "fail", db,
                    "extensions available on target",
                    ", ".join(gap) if gap else f"{len(exts)} ok")
                nopk = self._psql("src", db, """
                    select coalesce(string_agg(n.nspname||'.'||c.relname, ', '
                      order by c.relname), '')
                    from pg_class c
                    join pg_namespace n on n.oid = c.relnamespace
                    where c.relkind = 'r'
                      and n.nspname not in ('pg_catalog','information_schema')
                      and n.nspname not like 'pg\\_%'
                      and n.nspname not like '\\_\\_%'
                      and not exists (select 1 from pg_index i
                        where i.indrelid = c.oid and i.indisprimary)""")
                add("pass" if not nopk else "warn", db,
                    "tables without primary key (no CDC updates,"
                    " checksum-only verify)", nopk or "none")
                unlogged = self._psql("src", db,
                    "select count(*) from pg_class c"
                    " join pg_namespace n on n.oid = c.relnamespace"
                    " where c.relkind = 'r' and c.relpersistence = 'u'"
                    " and n.nspname not in ('pg_catalog','information_schema')"
                    " and n.nspname not like 'pg\\_%'"
                    " and n.nspname not like '\\_\\_%'")
                add("pass" if unlogged == "0" else "warn", db,
                    "unlogged tables (no WAL, movers skip their changes)",
                    unlogged)
                inv = self._psql("src", db,
                                 "select count(*) from pg_index"
                                 " where not indisvalid")
                add("pass" if inv == "0" else "warn", db,
                    "invalid indexes on source", inv)
                enc_q = ("select pg_encoding_to_char(encoding)||' '||datcollate"
                         f" from pg_database where datname = '{db}'")
                se = self._psql("src", "postgres", enc_q)
                try:
                    de = self._psql("dst", "postgres", enc_q)
                except RuntimeError:
                    de = ""
                if not de:
                    add("warn", db, "database exists on target", "missing")
                else:
                    add("pass" if se == de else "fail", db,
                        "encoding and collation match", f"src {se} / dst {de}")
            except RuntimeError as e:
                add("fail", db, "assess queries", str(e).splitlines()[-1][:120])
        return items

    def _pk_cols_of(self, db, table):
        sch, tbl = table.split(".", 1)
        cols = self._psql("src", db,
            "select a.attname from pg_index i"
            " join pg_attribute a on a.attrelid = i.indrelid"
            " and a.attnum = any(i.indkey)"
            f" where i.indrelid = '\"{sch}\".\"{tbl}\"'::regclass"
            " and i.indisprimary"
            " order by array_position(i.indkey, a.attnum)").splitlines()
        return [c for c in cols if c]

    def _compare_pks(self, db, table, keys):
        """Row-level compare of the given pk keys (tab-joined when
        composite). Returns (missing, extra, changed) or None when the
        table has no pk."""
        cols = self._pk_cols_of(db, table)
        if not cols:
            return None
        sch, tbl = table.split(".", 1)

        def esc(v):
            return "'" + v.replace("'", "''") + "'"

        def fetch(side):
            out = {}
            klist = sorted(keys)
            for i in range(0, len(klist), 500):
                chunk = klist[i:i + 500]
                if len(cols) == 1:
                    inlist = ", ".join(esc(k) for k in chunk)
                    where = f'"{cols[0]}"::text in ({inlist})'
                    pk = f'"{cols[0]}"::text'
                else:
                    tup = ", ".join(f'"{c}"::text' for c in cols)
                    vals = ", ".join(
                        "(" + ", ".join(esc(p) for p in k.split("\t")) + ")"
                        for k in chunk)
                    where = f"({tup}) in ({vals})"
                    pk = f"concat_ws(e'\\t', {tup})"
                q = (f"select {pk}||'|'||md5(to_jsonb(t)::text)"
                     f' from "{sch}"."{tbl}" t where {where}')
                for line in self._psql(side, db, q).splitlines():
                    k, _, h = line.rpartition("|")
                    out[k] = h
            return out

        src, dst = fetch("src"), fetch("dst")
        missing = sorted(k for k in keys if k in src and k not in dst)
        extra = sorted(k for k in keys if k in dst and k not in src)
        changed = sorted(k for k in keys
                         if k in src and k in dst and src[k] != dst[k])
        return missing, extra, changed

    def _write_pk_files(self, db, table, missing, extra, changed):
        d = self._report(db)
        d.mkdir(parents=True, exist_ok=True)
        for kind, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            f = d / f"data-{table}.{kind}"
            if rows:
                f.write_text("\n".join(rows) + "\n")
            elif f.exists():
                f.unlink()

    def settle_recheck(self, db, table):
        d = self._report(db)
        keys = set()
        for k in ("missing", "extra", "changed"):
            f = d / f"data-{table}.{k}"
            if f.exists():
                keys |= set(f.read_text().splitlines())
        keys.discard("")
        if not keys or len(keys) > 20000:
            return None
        cmp = self._compare_pks(db, table, keys)
        if cmp is None:
            return None
        missing, extra, changed = cmp
        self._write_pk_files(db, table, missing, extra, changed)
        return len(missing), len(extra), len(changed)

    # --- consistency by design: any consumer (incl. DTS) holds a slot on
    # the source, so "target applied past LSN X" is observable there ---

    def _psql_script(self, side, db, sql):
        ep = self.hop.source if side == "src" else self.hop.target
        env = tool_env({"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15",
                        "PGOPTIONS": "-c TimeZone=UTC -c DateStyle=ISO"
                                     " -c statement_timeout=0"
                                     " -c extra_float_digits=3"})
        return subprocess.Popen(
            ["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
             "-d", self._d(side, db), "-X", "-At", "-q", "-v",
             "ON_ERROR_STOP=1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, env=env), sql

    def _row_hash_expr(self):
        # to_jsonb canonicalizes rendering (ISO timestamps); ::text is faster
        if self.hop.options.get("checksum", "text") == "jsonb":
            return "md5(to_jsonb(t)::text)"
        return "md5(t::text)"

    def _fast_consistent(self, db):
        """Whole-database checksum inside ONE repeatable-read read-only
        transaction per side: no intra-db skew (every table of a side is
        the same instant), and the src LSN captured in-snapshot gives the
        fence for convergence proofs."""
        st = [l for l in self._psql("src", db,
                                    self.USER_TABLES).splitlines() if l]
        dt = set(l for l in self._psql("dst", db,
                                       self.USER_TABLES).splitlines() if l)
        both = [t for t in st if t in dt]
        w = int(self.hop.options.get("checksum_workers", 8))
        h = self._row_hash_expr()

        def script(side):
            lines = ["begin transaction isolation level repeatable read"
                     " read only;",
                     f"set local max_parallel_workers_per_gather = {w};",
                     "select 'LSN|'||case when pg_is_in_recovery() then"
                     " 'standby (read replica, no fence)' else"
                     " pg_current_wal_lsn()::text end;"]
            for t in both:
                sch, tbl = t.split(".", 1)
                lines.append(
                    f"select '{t}|'||count(*)||'|'||coalesce(sum(('x'||"
                    f"substr({h},1,16))::bit(64)::bigint::numeric), 0)"
                    f' from "{sch}"."{tbl}" t;')
            lines.append("commit;")
            return "\n".join(lines)

        procs = {}
        for side in ("src", "dst"):
            p, sql = self._psql_script(side, db, script(side))
            procs[side] = (p, sql)
        outs = {}
        for side, (p, sql) in procs.items():
            stdout, stderr = p.communicate(sql)
            if p.returncode:
                return 1, f"consistent pass failed on {side}: {stderr[-300:]}"
            outs[side] = stdout

        def parse(text):
            lsn, rows = "", {}
            for line in text.splitlines():
                name, _, rest = line.partition("|")
                if name == "LSN":
                    lsn = rest
                elif name:
                    rows[name] = rest
            return lsn, rows

        src_lsn, src_rows = parse(outs["src"])
        dst_lsn, dst_rows = parse(outs["dst"])
        out = [f"consistent snapshot: one repeatable-read txn per side,"
               f" src lsn={src_lsn} dst lsn={dst_lsn}"]
        rc = 0
        for t in both:
            a, b = src_rows.get(t, ""), dst_rows.get(t, "")
            if a == b:
                out.append(f"{t}: OK rows={a.split('|')[0]}"
                           f" checksum={a.split('|', 1)[1]}")
            else:
                rc = 1
                out.append(f"{t}: DIFF src={a} dst={b}")
        for t in st:
            if t not in dt:
                rc = 1
                out.append(f"{t}: ERROR missing on target")
        return rc, "\n".join(out)

    def _in_recovery(self, side, db="postgres"):
        """True if this endpoint is a standby / read replica (read-only).
        Aurora and RDS readers answer pg_is_in_recovery() = t and reject
        every write, including SELECT ... FOR UPDATE (SQLSTATE 25006)."""
        try:
            return self._psql(side, db, "select pg_is_in_recovery()") == "t"
        except RuntimeError:
            return False

    def src_lsn(self, db):
        # WAL funcs are unavailable during recovery; no source LSN, no fence
        if self._in_recovery("src", db):
            return None
        return self._psql("src", db, "select pg_current_wal_lsn()")

    def fence_wait(self, db, lsn, timeout=300):
        """Block until every active replication consumer of this db has
        confirmed flushing past `lsn`. True = fence passed, False = timed
        out, None = no slot visible / no source LSN (cannot fence)."""
        if lsn is None:
            return None
        if self._psql("src", db,
                      "select count(*) from pg_replication_slots"
                      f" where database = '{db}' and active") == "0":
            return None
        q = ("select coalesce(min(confirmed_flush_lsn), '0/0'::pg_lsn)"
             f" >= '{lsn}'::pg_lsn from pg_replication_slots"
             f" where database = '{db}' and active")
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            if self._psql("src", db, q) == "t":
                return True
            time.sleep(2)
        return False

    def fenced_recheck(self, db, table, keys):
        """Convergence proof for suspect rows: capture src LSN, wait for
        the fence, re-compare. Two rounds ride out rows that stay hot;
        what survives is a real diff, not replication in flight.
        Returns (missing, extra, changed, proof) or None if unfenceable."""
        proof = []
        for rnd in (1, 2):
            lsn = self.src_lsn(db)
            ok = self.fence_wait(db, lsn, timeout=int(
                self.hop.options.get("fence_timeout", 300)))
            if ok is None:
                return None
            proof.append(f"round {rnd}: fence lsn={lsn}"
                         f" {'passed' if ok else 'TIMEOUT'}")
            cmp = self._compare_pks(db, table, keys)
            if cmp is None:
                return None
            missing, extra, changed = cmp
            if not (missing or extra or changed):
                self._write_pk_files(db, table, [], [], [])
                return [], [], [], proof
            keys = set(missing) | set(extra) | set(changed)
            if not ok:
                break
        self._write_pk_files(db, table, missing, extra, changed)
        return missing, extra, changed, proof

    def _resolve_inflight(self, db, bad, stream=None):
        """Split DIFF tables into real diffs vs in-flight replication,
        deterministically when a fence is available, by sleep-settle as
        the fallback."""
        still, healed, how = [], [], []
        settle = int(self.hop.options.get("settle", 0))
        slept = False
        for t in bad:
            d = self._report(db)
            keys = set()
            for k in ("missing", "extra", "changed"):
                f = d / f"data-{t}.{k}"
                if f.exists():
                    keys |= set(f.read_text().splitlines())
            keys.discard("")
            if not keys or len(keys) > 20000:
                still.append(t)
                continue
            r = self.fenced_recheck(db, t, keys)
            if r is not None:
                missing, extra, changed, proof = r
                if missing or extra or changed:
                    still.append(t)
                else:
                    healed.append(t)
                    how.append(f"{t}: {'; '.join(proof)}")
                if stream:
                    stream(f"{t}: fence {'REAL DIFF' if t in still else 'converged'}")
                continue
            if not settle:
                still.append(t)
                continue
            if not slept:
                time.sleep(settle)
                slept = True
            s = self.settle_recheck(db, t)
            if s is None or any(s):
                still.append(t)
            else:
                healed.append(t)
                how.append(f"{t}: settled after {settle}s (no fence visible)")
        return still, healed, how

    def _column_fingerprint(self, db, table):
        """One scan, one aggregate per column: which columns actually
        differ. Localizes drift (e.g. only updated_at differs = timezone
        rendering, not data loss) before any row-level work."""
        sch, tbl = table.split(".", 1)
        cols = [l for l in self._psql("src", db,
                "select attname from pg_attribute"
                f""" where attrelid = '"{sch}"."{tbl}"'::regclass"""
                " and attnum > 0 and not attisdropped"
                " and attgenerated = '' order by attnum").splitlines() if l]
        if not cols:
            return []
        expr = ", ".join(
            f"coalesce(sum(('x'||substr(md5(coalesce(\"{c}\"::text,"
            f" chr(1))),1,16))::bit(64)::bigint::numeric), 0)"
            for c in cols)
        q = f'select {expr} from "{sch}"."{tbl}"'
        try:
            a = self._psql("src", db, q).split("|")
            b = self._psql("dst", db, q).split("|")
        except RuntimeError:
            return ["(column fingerprint failed, column sets may differ)"]
        diff = [c for c, x, y in zip(cols, a, b) if x != y]
        out = self.hop.report_dir(db) / f"data-{table}.columns"
        if diff:
            out.write_text("columns differing between src and dst:\n"
                           + "\n".join(diff) + "\n")
        elif out.exists():
            out.unlink()
        return diff

    # --- delta verify: a source slot records which rows changed; each cycle
    # verifies only those pks and advances only when clean (idempotent) ---

    def _delta_slot(self, db):
        import re as _re
        name = f"migkit_delta_{self.hop.name}_{db}"
        return _re.sub(r"[^a-z0-9_]", "_", name.lower())[:63]

    def delta_setup(self, db):
        slot = self._delta_slot(db)
        have = self._psql("src", db,
                          "select 1 from pg_replication_slots"
                          f" where slot_name = '{slot}'")
        if have:
            return False
        self._psql("src", db,
                   "select pg_create_logical_replication_slot("
                   f"'{slot}', 'test_decoding')")
        return True

    def delta_teardown(self, db):
        slot = self._delta_slot(db)
        self._psql("src", db,
                   "select pg_drop_replication_slot(slot_name)"
                   " from pg_replication_slots"
                   f" where slot_name = '{slot}'")

    @staticmethod
    def _parse_decoding(lines, pk_of):
        """Parse test_decoding rows into {table: set(pk_key)}.
        pk_of(table) -> ordered pk column list or None."""
        import re as _re
        field_re = _re.compile(r"(\w+)\[[^\]]*\]:('(?:[^']|'')*'|[^ ]+)")
        head_re = _re.compile(
            r"table ([^:]+): (INSERT|UPDATE|DELETE): (.*)")
        touched = {}
        nopk = set()
        last_lsn = ""
        for lsn, data in lines:
            last_lsn = lsn or last_lsn
            m = head_re.match(data)
            if not m:
                continue
            table = m.group(1).replace('"', "")
            pks = pk_of(table)
            if not pks:
                nopk.add(table)
                continue
            vals = {}
            for col, val in field_re.findall(m.group(3)):
                if val.startswith("'"):
                    val = val[1:-1].replace("''", "'")
                vals[col] = val
            if all(p in vals for p in pks):
                touched.setdefault(table, set()).add(
                    "\t".join(vals[p] for p in pks))
        return touched, nopk, last_lsn

    def delta_verify(self, db, limit=20000, log=None):
        slot = self._delta_slot(db)
        if self._in_recovery("src", db):
            return [Result("delta", db, "error",
                           "source is a read replica (pg_is_in_recovery=t):"
                           " logical slots live on the primary. Point the"
                           " hop's source at the writer/cluster endpoint")]
        if self.delta_setup(db):
            return [Result("delta", db, "ok",
                           f"slot {slot} created, changes are tracked"
                           " from this point on")]
        out = self._psql("src", db,
                         "select lsn||' '||data from"
                         f" pg_logical_slot_peek_changes('{slot}',"
                         f" null, {limit})")
        lines = [l.split(" ", 1) for l in out.splitlines() if " " in l]
        pk_cache = {}

        def pk_of(t):
            if t not in pk_cache:
                try:
                    pk_cache[t] = self._pk_cols_of(db, t)
                except RuntimeError:
                    pk_cache[t] = None
            return pk_cache[t]

        touched, nopk, last_lsn = self._parse_decoding(lines, pk_of)
        n_changes = sum(len(v) for v in touched.values())
        if not touched and not nopk:
            return [Result("delta", db, "ok",
                           "0 changes since last verified point")]
        res = []
        clean = True
        for t, keys in sorted(touched.items()):
            cmp = self._compare_pks(db, t, keys)
            if cmp is None:
                res.append(Result("delta", f"{db}.{t}", "error",
                                  "pk lookup failed"))
                clean = False
                continue
            missing, extra, changed = cmp
            if missing or extra or changed:
                clean = False
                self._write_pk_files(db, t, missing, extra, changed)
                res.append(Result(
                    "delta", f"{db}.{t}", "diff",
                    f"of {len(keys)} touched rows: missing={len(missing)}"
                    f" extra={len(extra)} changed={len(changed)}",
                    str(self._report(db)),
                    f"migkit sync {self.hop.name} --db {db} --kind rows"))
            else:
                res.append(Result("delta", f"{db}.{t}", "ok",
                                  f"{len(keys)} touched rows verified"
                                  " equal on both sides"))
            if log:
                log(f"{t}: {len(keys)} touched, "
                    + ("clean" if not (missing or extra or changed)
                       else "DIFF"))
        for t in sorted(nopk):
            res.append(Result("delta", f"{db}.{t}", "skip",
                              "no pk, cannot delta-verify"))
        if clean and last_lsn:
            self._psql("src", db,
                       "select pg_replication_slot_advance("
                       f"'{slot}', '{last_lsn}'::pg_lsn)")
            note = "slot advanced"
        else:
            note = "slot NOT advanced, window replays next cycle"
        if len(lines) >= limit:
            note += f"; window truncated at {limit} changes, more pending"
        res.insert(0, Result("delta", db,
                             "ok" if clean else "diff",
                             f"{n_changes} changed rows across"
                             f" {len(touched)} tables, {note}"))
        return res

    def list_move_tables(self, db):
        out = self._psql("src", db,
            "select n.nspname||'|'||c.relname from pg_class c"
            " join pg_namespace n on n.oid = c.relnamespace"
            " where c.relkind = 'r'"
            " and n.nspname not in ('pg_catalog','information_schema')"
            " and n.nspname not like 'pg\\_%'"
            " and n.nspname not like '\\_\\_%' order by 1")
        return [tuple(x.split("|")) for x in out.splitlines() if x]

    def _int_pk(self, db, sch, tbl):
        rows = self._psql("src", db,
            "select a.attname||'|'||a.atttypid::regtype from pg_index i"
            " join pg_attribute a on a.attrelid = i.indrelid"
            " and a.attnum = any(i.indkey)"
            f" where i.indrelid = '\"{sch}\".\"{tbl}\"'::regclass"
            " and i.indisprimary").splitlines()
        if len(rows) == 1:
            col, typ = rows[0].split("|")
            if typ in ("smallint", "integer", "bigint"):
                return col
        return None

    def _copy_pipe(self, db, select_sql, qt, pre_sql=""):
        s, t = self.hop.source, self.hop.target
        env_s = tool_env({"PGPASSWORD": s.password, "PGCONNECT_TIMEOUT": "15"})
        env_t = tool_env({"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"})
        out = subprocess.Popen(
            ["psql", "-h", s.host, "-p", str(s.port), "-U", s.user, "-d", db,
             "-X", "-q", "-v", "ON_ERROR_STOP=1",
             "-c", f"\\copy ({select_sql}) to stdout"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env_s)
        cmds = ["-c", pre_sql] if pre_sql else []
        inp = subprocess.Popen(
            ["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", self._d("dst", db),
             "-X", "-q", "-v", "ON_ERROR_STOP=1", "-1", *cmds,
             "-c", f"\\copy {qt} from stdin"],
            stdin=out.stdout, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env_t)
        out.stdout.close()
        _, err_i = inp.communicate()
        _, err_o = out.communicate()
        if out.returncode or inp.returncode:
            raise RuntimeError((err_o + err_i).decode()[-300:])

    def move_table(self, db, sch, tbl, chunk, ck, log):
        sch = sch or "public"
        key = f"{sch}.{tbl}"
        qt = f'"{sch}"."{tbl}"'
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        pk = self._int_pk(db, sch, tbl)
        if not pk:
            log(f"{key}: no single int pk, single-shot copy")
            self._copy_pipe(db, f"select * from {qt}", qt,
                            f"truncate {qt}")
            st["done"] = True
            return
        mm = self._psql("src", db,
                        f'select coalesce(min("{pk}"), 0)||\'|\'||'
                        f'coalesce(max("{pk}"), 0) from {qt}')
        lo, hi = (int(x) for x in mm.split("|"))
        last = st.get("last", lo - 1)
        while last < hi:
            nxt = min(last + chunk, hi)
            pred = f'"{pk}" > {last} and "{pk}" <= {nxt}'
            self._copy_pipe(db, f"select * from {qt} where {pred}", qt,
                            f"delete from {qt} where {pred}")
            last = nxt
            st["last"] = last
            ck.save()
            log(f"{key}: up to {pk}={last:,} of {hi:,}")
        st["done"] = True
        ck.save()

    def replicate_sql(self, db, copy_data=True):
        s = self.hop.source
        name = "migkit_" + self.hop.name.replace("-", "_")
        conn = (f"host={s.host} port={s.port} dbname={db}"
                f" user={s.user} password={s.password}")
        return {
            "src": [f"create publication {name} for all tables;"],
            "dst": [f"create subscription {name} connection '{conn}'"
                    f" publication {name} with (copy_data ="
                    f" {'true' if copy_data else 'false'});"],
            "drop_src": [f"drop publication if exists {name};"],
            "drop_dst": [f"drop subscription if exists {name};"],
            "status": "select subname, received_lsn, latest_end_lsn,"
                      " latest_end_time from pg_stat_subscription",
        }

    def migration_pair(self, db):
        from urllib.parse import quote
        if not which("atlas"):
            return None, None
        s, t = self.hop.source, self.hop.target
        su = (f"postgres://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}?sslmode=prefer")
        tu = (f"postgres://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{self._d('dst', db)}?sslmode=prefer")

        def diff(a, b):
            p = run(["atlas", "schema", "diff", "--from", a, "--to", b,
                     "--exclude", "__*",
                     "--exclude", "*.migkit_changelog"],
                    check=False, timeout=180)
            text = p.stdout.strip()
            if p.returncode or "Schemas are synced" in text:
                return ""
            return text
        return diff(tu, su), diff(su, tu)

    def fetch_sample_df(self, side, db, table, limit):
        import io as _io

        import pandas as pd
        sch, tbl = table.split(".", 1) if "." in table else ("public", table)
        ep = self.hop.source if side == "src" else self.hop.target
        env = tool_env({"PGPASSWORD": ep.password})
        p = subprocess.run(
            ["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
             "-d", self._d(side, db), "-X", "-q", "-v", "ON_ERROR_STOP=1",
             "-c", f"\\copy (select * from \"{sch}\".\"{tbl}\""
                   f" limit {limit}) to stdout (format csv, header)"],
            capture_output=True, text=True, env=env)
        if p.returncode:
            raise RuntimeError(p.stderr[-200:])
        return pd.read_csv(_io.StringIO(p.stdout))

    def watch_sample(self, db):
        sample = {"db": db, "ts": time.time()}
        q = ("select coalesce(sum(n_live_tup),0) from pg_stat_user_tables"
             " where schemaname not like '\\_\\_%'")
        try:
            sample["src_rows"] = int(self._psql("src", db, q) or 0)
            sample["dst_rows"] = int(self._psql("dst", db, q) or 0)
        except RuntimeError as e:
            sample["error"] = str(e).splitlines()[-1]
            return sample
        try:
            # per-table live counts on the source so a cutover verdict can
            # name which tables a queue/worker is still writing to
            rows = self._psql("src", db,
                              "select schemaname||'.'||relname||'|'||n_live_tup"
                              " from pg_stat_user_tables"
                              " where schemaname not like '\\_\\_%'").splitlines()
            sample["src_tables"] = {l.rsplit("|", 1)[0]: int(l.rsplit("|", 1)[1])
                                    for l in rows if "|" in l}
        except (RuntimeError, ValueError):
            pass
        try:
            slots = self._psql("src", db,
                               "select slot_name||' active='||active||' lag='||"
                               "coalesce(pg_size_pretty(pg_wal_lsn_diff("
                               "pg_current_wal_lsn(), confirmed_flush_lsn)),'?')"
                               " from pg_replication_slots")
            sample["replication_slots"] = slots.splitlines() if slots else []
            conns = self._psql("src", db,
                               "select application_name||' '||client_addr||' '||state"
                               " from pg_stat_replication")
            sample["replication_conns"] = conns.splitlines() if conns else []
        except RuntimeError:
            pass
        return sample
