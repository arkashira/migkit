import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

from ..config import REPORTS
from ..util import run, tool_env, which
from .base import Engine, RepairAction, Result

CHECKER = Path(os.environ.get("PG_DIFF_CHECKER",
                              Path(__file__).resolve().parent.parent
                              / "scripts" / "pg"))
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

    def __init__(self, hop):
        super().__init__(hop)
        self._conf = None

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

    def checker_conf(self):
        if self._conf:
            return self._conf
        s, t = self.hop.source, self.hop.target
        conf_dir = PGDC_ROOT / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        name = self.hop.name
        body = (
            f"SRC_HOST={s.host}\nSRC_PORT={s.port}\nSRC_USER={s.user}\n"
            f"SRC_PASS='{s.password}'\n"
            f"DST_HOST={t.host}\nDST_PORT={t.port}\nDST_USER={t.user}\n"
            f"DST_PASS='{t.password}'\n"
            f"DBS='{' '.join(self.hop.databases)}'\n"
            f"CHUNK=50000\nDRILL_MAX_ROWS=2000000\n"
        )
        path = conf_dir / f"{name}.conf"
        path.write_text(body)
        path.chmod(0o600)
        self._conf = name
        return name

    def _env(self):
        return {"PGDC_CONF_DIR": str(PGDC_ROOT / "conf"),
                "PGDC_REPORT_DIR": str(PGDC_ROOT),
                "PGDC_NOISE_PREFIX": self.hop.options.get("noise_prefix", ""),
                "PGDC_EXCLUDE_SCHEMA": self.hop.options.get(
                    "exclude_schema", "__*"),
                "WORKERS": str(self.hop.options.get("checksum_workers", 8)),
                "BIG_ROWS": str(self.hop.big_rows),
                "SLICE": str(self.hop.slice),
                "DB_MAP": " ".join(f"{k}:{v}"
                                   for k, v in self.hop.db_map.items())}

    def _script(self, script, *args, stream=None):
        conf = self.checker_conf()
        cmd = [str(CHECKER / script), conf, *args]
        env = tool_env(self._env())
        if stream:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True, env=env)
            lines = []
            for line in p.stdout:
                line = line.rstrip("\n")
                lines.append(line)
                stream(line)
            p.wait()
            return p.returncode, "\n".join(lines)
        p = subprocess.run(cmd, capture_output=True, text=True, env=env)
        return p.returncode, (p.stdout + p.stderr).strip()

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        out = self._psql("src", "postgres",
                         "select datname from pg_database where not datistemplate"
                         " and datname not in ('postgres','rdsadmin') order by 1")
        return [l for l in out.splitlines() if l]

    def _report(self, db):
        return PGDC_ROOT / self.hop.name / db

    def check_schema(self, db):
        rc, out = self._script("check-schema.sh", db)
        line = out.splitlines()[-1] if out else ""
        status = "ok" if rc == 0 else "diff"
        res = [Result("schema", db, status, line, str(self._report(db) / "schema.diff"),
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
        return res

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

    def check_counts(self, db):
        rc, out = self._script("check-counts.sh", db)
        status = "ok" if rc == 0 else "diff"
        return [Result("counts", db, status, out.splitlines()[-1] if out else "",
                       str(self._report(db) / "counts.diff"),
                       "missing rows show up in check data, fix there")]

    def check_autoinc(self, db):
        rc, out = self._script("check-sequences.sh", db)
        status = "ok" if rc == 0 else "diff"
        return [Result("autoinc", db, status, out.splitlines()[0] if out else "",
                       str(self._report(db) / "sequences.diff"),
                       f"migkit sync {self.hop.name} --db {db} --kind sequences")]

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

    def check_data(self, db, table=None, stream=None, with_counts=False,
                   consistent=False):
        if table:
            rc, out = self._script("check-data.sh", db, table, stream=stream)
            status = "ok" if rc == 0 else "diff"
            return [Result("data", f"{db} {table}", status,
                           out.splitlines()[-1] if out else "",
                           str(self._report(db)),
                           f"migkit sync {self.hop.name} --db {db} --kind rows")]
        if consistent:
            rc, out = self._fast_consistent(db)
            if stream:
                for line in out.splitlines():
                    stream(line)
        else:
            rc, out = self._script("check-data-fast.sh", db, stream=stream)
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
            self._script("check-data.sh", db, t, stream=stream)
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
        pats = []
        for d in (CHECKER / "conf", PGDC_ROOT / "conf"):
            f = d / f"{self.hop.name}.schema-ignore"
            if f.exists():
                pats += [p for p in f.read_text().splitlines() if p.strip()]
        return [_re.compile(p) for p in pats]

    def check_deep(self, db):
        res = []
        rpt = self.hop.report_dir(db)

        # orphans can only hide behind NOT VALID constraints (pg enforces
        # validated ones), so scan just those
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

        # render audit: exotic types are where ::text equality can lie
        # across builds/versions; compare the actual rendering of sampled
        # rows so the lie surfaces instead of hiding inside a checksum
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
            # count alone misses a stale mv with the same row count, so
            # checksum the content (matviews are small derived sets)
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
            stmts, undo = [], []
            for name, v in sorted(src.items()):
                cur = dst.get(name)
                if v == "0" or cur == v:
                    continue
                stmts.append(f"select setval('{name}', {v}, true);"
                             f"  -- dst now {cur if cur is not None else 'MISSING'}")
                if cur == "0":
                    undo.append(f"select setval('{name}', 1, false);")
                elif cur is not None:
                    undo.append(f"select setval('{name}', {cur}, true);")
            same = sum(1 for n, v in src.items() if dst.get(n) == v)
            if stmts:
                actions.append(RepairAction(
                    db, "sequences", stmts, undo,
                    f"{len(stmts)} sequences differ, {same} already equal"))
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
                counts = []
                for kind_ in ("missing", "extra", "changed"):
                    f = rpt / f"data-{t}.{kind_}"
                    n = sum(1 for _ in f.open()) if f.exists() else 0
                    if n:
                        counts.append(f"{kind_}={n}")
                actions.append(RepairAction(
                    db, "rows",
                    [f"{CHECKER / 'fix-data.sh'} {self.hop.name} {db} {t}"],
                    [], f"{t}: {', '.join(counts) or 'no pk files'},"
                        " deleted rows saved to undo before recopy"))
        return actions

    def apply(self, db, action):
        if action.kind == "sequences":
            self._psql("dst", db,
                       "\n".join(s.split("  --")[0] for s in action.statements))
        else:
            for cmd in action.statements:
                run(cmd, env=self._env())

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

    def snapshot_state(self, db, state_dir):
        seqs = self._psql("dst", db,
                          "select schemaname||'.'||sequencename||'|'||"
                          "coalesce(last_value,1)||'|'||(last_value is not null)"
                          " from pg_sequences"
                          " where schemaname not like '\\_\\_%'")
        (state_dir / "dst-sequences.txt").write_text((seqs + "\n") if seqs else "")
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

        # endpoint role: a read replica answers pg_is_in_recovery()=t and
        # rejects every write incl. SELECT ... FOR UPDATE (SQLSTATE 25006).
        # target on a reader = migration and repair cannot write; source on
        # a reader = fine for checks but the LSN fence and logical slots
        # need the primary. This is the "app pointed at the reader endpoint"
        # class of outage, surfaced before it bites.
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
        lrt = self._psql("src", "postgres",
                         "select count(*) from pg_stat_activity where"
                         " xact_start is not null"
                         " and now() - xact_start > interval '10 minutes'")
        add("pass" if lrt == "0" else "warn", "instance",
            "transactions open longer than 10 minutes", lrt)

        # pg_authid carries the real password hash (pg_roles masks it as
        # ********); needs superuser, so fall back to name-only when blocked
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
        # credential drift: role exists on both but the stored hash differs.
        # this is the DTS failure mode where the user is carried over but the
        # password is not, so applications cannot authenticate on the target.
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

    # --- consistency by design ---------------------------------------
    # the settle heuristic guesses; these prove. Every consumer of the
    # source (our subscription or an opaque mover like DTS) holds a slot
    # on the source, so "target applied past LSN X" is observable from
    # the source side alone.

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
        # to_jsonb canonicalizes rendering (ISO timestamps regardless of
        # DateStyle, sorted jsonb keys); plain ::text is faster
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
        # WAL functions are unavailable during recovery (and Aurora readers
        # reject the replay-lsn variant too); no source LSN => no fence
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

    # --- delta verify: O(changes) continuous verification -------------
    # a dedicated logical slot on the source records which rows changed;
    # each cycle verifies only those pks on both sides. The slot is only
    # advanced after a clean verify, so a crash or a diff replays the
    # same window: idempotent by construction.

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
