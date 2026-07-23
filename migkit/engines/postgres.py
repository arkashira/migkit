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

    def __init__(self, hop):
        super().__init__(hop)
        self._conf = None

    def _psql(self, side, db, sql):
        ep = self.hop.source if side == "src" else self.hop.target
        env = {"PGPASSWORD": ep.password, "PGCONNECT_TIMEOUT": "15",
               "PGOPTIONS": "-c TimeZone=UTC -c DateStyle=ISO -c statement_timeout=0"}
        p = run(["psql", "-h", ep.host, "-p", str(ep.port), "-U", ep.user,
                 "-d", db, "-X", "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
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
                "SLICE": str(self.hop.slice)}

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
            turl = f"postgresql://{t.user}:{t.password}@{t.host}:{t.port}/{db}"
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
              f"@{t.host}:{t.port}/{db}?sslmode=prefer")
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
                     f"--url=jdbc:postgresql://{t.host}:{t.port}/{db}?sslmode=prefer",
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
                       f"migkit repair {self.hop.name} --db {db} --kind sequences")]

    def check_data(self, db, table=None, stream=None):
        if table:
            rc, out = self._script("check-data.sh", db, table, stream=stream)
            status = "ok" if rc == 0 else "diff"
            return [Result("data", f"{db} {table}", status,
                           out.splitlines()[-1] if out else "",
                           str(self._report(db)),
                           f"migkit repair {self.hop.name} --db {db} --kind rows")]
        rc, out = self._script("check-data-fast.sh", db, stream=stream)
        ev = self.hop.report_dir(db) / "data-evidence.txt"
        ev.write_text(out + "\n")
        if rc == 0:
            import re as _re
            rows = sum(int(m) for m in _re.findall(r"rows=(\d+)", out))
            n = out.count(": OK")
            return [Result("data", db, "ok",
                           f"{n} tables, {rows:,} rows, checksums equal"
                           f" both sides", str(ev))]
        bad = [l.split(":")[0] for l in out.splitlines() if ": DIFF" in l]
        err = [l.split(":")[0] for l in out.splitlines() if ": ERROR" in l]
        for t in bad:
            self._script("check-data.sh", db, t, stream=stream)
        settle = int(self.hop.options.get("settle", 0))
        if bad and settle:
            time.sleep(settle)
            still, healed = [], []
            for t in bad:
                r = self.settle_recheck(db, t)
                if r is None or any(r):
                    still.append(t)
                else:
                    healed.append(t)
            if not still:
                return [Result("data", db, "ok",
                               f"all diffs were in-flight replication,"
                               f" settled and re-verified equal after"
                               f" {settle}s ({', '.join(healed)})")]
            bad = still
        detail = ""
        if bad:
            detail = f"tables differ: {', '.join(bad)} (pk-level files written)"
        if err:
            detail += f" errors: {', '.join(err)}"
        return [Result("data", db, "diff" if bad else "error", detail,
                       str(self._report(db)),
                       f"migkit repair {self.hop.name} --db {db} --kind rows")]

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
        return [
            f"pg_dumpall -h {s.host} -p {s.port} -U {s.user} --globals-only"
            f" > globals.sql   # review roles, then apply on target",
            f"pg_dump -h {s.host} -p {s.port} -U {s.user} -d {db} -Fc --schema-only"
            f" -f {db}.schema.dump",
            f"createdb -h {t.host} -p {t.port} -U {t.user} {db}"
            f"   # match encoding/locale with source",
            f"pg_restore -h {t.host} -p {t.port} -U {t.user} -d {db} --no-owner"
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
                 "-d", db, "--schema-only", "--no-owner", "--no-privileges"],
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

    def settle_recheck(self, db, table):
        d = self._report(db)
        files = {k: d / f"data-{table}.{k}"
                 for k in ("missing", "extra", "changed")}
        keys = set()
        for f in files.values():
            if f.exists():
                keys |= set(f.read_text().splitlines())
        if not keys or len(keys) > 20000:
            return None
        sch, tbl = table.split(".", 1)
        cols = self._psql("src", db,
            "select a.attname from pg_index i"
            " join pg_attribute a on a.attrelid = i.indrelid"
            " and a.attnum = any(i.indkey)"
            f" where i.indrelid = '\"{sch}\".\"{tbl}\"'::regclass"
            " and i.indisprimary"
            " order by array_position(i.indkey, a.attnum)").splitlines()
        cols = [c for c in cols if c]
        if not cols:
            return None

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
                sch, tbl = table.split(".", 1)
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
        for kind, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            f = files[kind]
            if rows:
                f.write_text("\n".join(rows) + "\n")
            elif f.exists():
                f.unlink()
        return len(missing), len(extra), len(changed)

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
            ["psql", "-h", t.host, "-p", str(t.port), "-U", t.user, "-d", db,
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

    LEDGER_DDL = ("create table if not exists public.migkit_changelog ("
                  "id bigint generated by default as identity primary key,"
                  " ran_at timestamptz default now(), author text,"
                  " op text, scope text, detail text, undo_ref text)")

    def record_ledger(self, db, entry):
        try:
            self._psql("dst", db, self.LEDGER_DDL)
            def q(v):
                return "null" if v is None else "'" + str(v).replace("'", "''") + "'"
            self._psql("dst", db,
                "insert into public.migkit_changelog"
                " (author, op, scope, detail, undo_ref) values ("
                f"{q(entry.get('author', 'migkit'))}, {q(entry.get('op'))},"
                f" {q(entry.get('scope') or entry.get('db'))},"
                f" {q(entry.get('detail') or entry.get('note'))},"
                f" {q(entry.get('undo_ref'))})")
            return True
        except RuntimeError:
            return False

    def read_ledger(self, db):
        try:
            out = self._psql("dst", db,
                "select ran_at||'|'||author||'|'||op||'|'||"
                "coalesce(scope,'')||'|'||coalesce(detail,'')"
                " from public.migkit_changelog order by id")
            return [l.split("|", 4) for l in out.splitlines() if l]
        except RuntimeError:
            return []

    def migration_pair(self, db):
        from urllib.parse import quote
        if not which("atlas"):
            return None, None
        s, t = self.hop.source, self.hop.target
        su = (f"postgres://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}?sslmode=prefer")
        tu = (f"postgres://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{db}?sslmode=prefer")

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
             "-d", db, "-X", "-q", "-v", "ON_ERROR_STOP=1",
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
