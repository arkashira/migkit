import difflib
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from ..util import run, which
from .base import Engine, RepairAction, Result

SKIP_DBS = {"mysql", "sys", "performance_schema", "information_schema"}
NULL_TOKEN = "~null~"


class MySQLEngine(Engine):
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = True

    def _conn(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            import pymysql
        except ImportError:
            raise SystemExit("pip install 'migkit[mysql]' for mysql support")
        return pymysql.connect(host=ep.host, port=ep.port, user=ep.user,
                               password=ep.password, charset="utf8mb4")

    def _q(self, side, sql, args=None, fresh=False):
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                if fresh:
                    try:
                        cur.execute("set session information_schema_stats_expiry = 0")
                    except Exception:
                        pass
                cur.execute(sql, args)
                return cur.fetchall()
        finally:
            conn.close()

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        rows = self._q("src", "show databases")
        return sorted(r[0] for r in rows if r[0] not in SKIP_DBS
                      and not r[0].startswith("__"))

    def _dump_schema(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        if not which("mysqldump"):
            raise SystemExit("mysqldump not found, run bootstrap.sh")
        p = run(["mysqldump", "-h", ep.host, "-P", str(ep.port), "-u", ep.user,
                 f"-p{ep.password}", "--no-data", "--routines", "--triggers",
                 "--events", "--skip-comments", "--skip-dump-date",
                 "--column-statistics=0",
                 f"--ignore-table={db}.migkit_changelog", db], check=False)
        if p.returncode != 0:
            raise RuntimeError(p.stderr.strip())
        text = re.sub(r" AUTO_INCREMENT=\d+", "", p.stdout)
        text = re.sub(r"DEFINER=`[^`]*`@`[^`]*`", "", text)
        lines = [l for l in text.splitlines()
                 if not l.startswith("--")
                 and "GTID_PURGED" not in l
                 and "SQL_LOG_BIN" not in l
                 and l.strip()]
        return "\n".join(lines) + "\n"

    def check_schema(self, db):
        d = self.hop.report_dir(db)
        src = self._dump_schema("src", db)
        dst = self._dump_schema("dst", db)
        (d / "schema-src.sql").write_text(src)
        (d / "schema-dst.sql").write_text(dst)
        diff = list(difflib.unified_diff(src.splitlines(), dst.splitlines(),
                                         "src", "dst", lineterm=""))
        changed = [l for l in diff if l[:1] in "+-"
                   and not l.startswith(("+++", "---"))]
        res = []
        if not changed:
            res.append(Result("schema", db, "ok"))
        else:
            (d / "schema.diff").write_text("\n".join(diff))
            sample = "; ".join(sorted(set(l.strip() for l in changed))[:3])
            res.append(Result("schema", db, "diff",
                              f"{len(changed)} changed lines, e.g. {sample}",
                              str(d / "schema.diff"),
                              "apply missing DDL from schema-src.sql on target"))
        res.append(self.check_objects(db))
        if which("atlas") and self.hop.options.get("atlas", True):
            at = self._atlas(db)
            if at:
                res.append(at)
        return res

    def check_objects(self, db):
        queries = {
            "table": "select table_name from information_schema.tables"
                     " where table_schema=%s and table_type='BASE TABLE'",
            "view": "select table_name from information_schema.tables"
                    " where table_schema=%s and table_type='VIEW'",
            "function": "select routine_name from information_schema.routines"
                        " where routine_schema=%s and routine_type='FUNCTION'",
            "procedure": "select routine_name from information_schema.routines"
                         " where routine_schema=%s and routine_type='PROCEDURE'",
            "trigger": "select trigger_name from information_schema.triggers"
                       " where trigger_schema=%s",
            "event": "select event_name from information_schema.events"
                     " where event_schema=%s",
            "index": "select concat(table_name, '.', index_name)"
                     " from information_schema.statistics"
                     " where table_schema=%s group by table_name, index_name",
        }
        inv = {}
        for typ, sql in queries.items():
            a = {r[0] for r in self._q("src", sql, (db,))}
            b = {r[0] for r in self._q("dst", sql, (db,))}
            inv[typ] = {"src": len(a), "dst": len(b),
                        "missing": sorted(a - b)[:50],
                        "extra": sorted(b - a)[:50]}
        out = self.hop.report_dir(db) / "objects.json"
        out.write_text(json.dumps(inv, indent=1))
        bad = {t: v for t, v in inv.items() if v["missing"] or v["extra"]}
        if bad:
            parts = []
            for t, v in bad.items():
                m = f"{t} {v['src']}/{v['dst']}"
                if v["missing"]:
                    m += " missing: " + ", ".join(v["missing"][:3])
                if v["extra"]:
                    m += " extra: " + ", ".join(v["extra"][:3])
                parts.append(m)
            return Result("schema", f"{db} objects", "diff", "; ".join(parts),
                          str(out),
                          "create missing objects on target from schema-src.sql")
        total = sum(v["src"] for v in inv.values())
        return Result("schema", f"{db} objects", "ok",
                      f"{total} objects in {len(inv)} types,"
                      " all present on target")

    def snapshot_state(self, db, state_dir):
        q = ("select table_name, auto_increment from information_schema.tables"
             " where table_schema=%s and auto_increment is not null")
        rows = self._q("dst", q, (db,), fresh=True)
        (state_dir / "dst-autoinc.txt").write_text(
            "".join(f"{t}|{v}\n" for t, v in sorted(rows)))
        (state_dir / "dst-schema.sql").write_text(self._dump_schema("dst", db))

    def _atlas(self, db):
        from urllib.parse import quote
        s, t = self.hop.source, self.hop.target
        su = (f"mysql://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}")
        tu = (f"mysql://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{db}")
        try:
            p = run(["atlas", "schema", "diff", "--from", tu, "--to", su,
                     "--exclude", "migkit_changelog"],
                    check=False, timeout=180)
        except Exception:
            return None
        if p.returncode != 0:
            return None
        text = p.stdout.strip()
        if not text or "Schemas are synced" in text:
            return Result("schema", f"{db} (atlas)", "ok", "atlas diff clean")
        out = self.hop.report_dir(db) / "atlas-fix.sql"
        out.write_text(text + "\n")
        return Result("schema", f"{db} (atlas)", "diff",
                      f"atlas generated {len(text.splitlines())} lines of fix DDL",
                      str(out), "review then apply atlas-fix.sql on target")

    def _tables(self, side, db):
        rows = self._q(side, "select table_name from information_schema.tables"
                             " where table_schema=%s and table_type='BASE TABLE'"
                             " and table_name not like 'migkit%%'"
                             " order by 1", (db,))
        return [r[0] for r in rows]

    def _pk_cols(self, db, t):
        rows = self._q("src",
                       "select column_name from information_schema.key_column_usage"
                       " where table_schema=%s and table_name=%s"
                       " and constraint_name='PRIMARY' order by ordinal_position",
                       (db, t))
        return [r[0] for r in rows]

    def _cols(self, db, t):
        rows = self._q("src", "select column_name from information_schema.columns"
                              " where table_schema=%s and table_name=%s"
                              " order by ordinal_position", (db, t))
        return [r[0] for r in rows]

    def _row_expr(self, db, t):
        cols = ", ".join(f"ifnull(cast(`{c}` as char), '{NULL_TOKEN}')"
                         for c in self._cols(db, t))
        return f"concat_ws('#', {cols})"

    def check_counts(self, db):
        st, dt = set(self._tables("src", db)), set(self._tables("dst", db))
        res = []
        if st - dt:
            res.append(Result("counts", db, "diff",
                              f"missing tables on target: {sorted(st - dt)}"))
        if dt - st:
            res.append(Result("counts", db, "diff",
                              f"extra tables on target: {sorted(dt - st)}"))
        bad = []
        total_a = total_b = 0

        def cnt(side, t):
            return self._q(side, f"select count(*) from `{db}`.`{t}`")[0][0]

        common = sorted(st & dt)
        with ThreadPoolExecutor(max_workers=max(2, self.hop.workers)) as pool:
            futs = {t: (pool.submit(cnt, "src", t), pool.submit(cnt, "dst", t))
                    for t in common}
            for t in common:
                a, b = futs[t][0].result(), futs[t][1].result()
                total_a += a
                total_b += b
                if a != b:
                    bad.append(f"{t} src={a} dst={b}")
        if bad:
            res.append(Result("counts", db, "diff", "; ".join(bad)))
        return res or [Result("counts", db, "ok",
                              f"{len(common)} tables, rows"
                              f" {total_a:,}=={total_b:,}")]

    def check_autoinc(self, db):
        q = ("select table_name, auto_increment from information_schema.tables"
             " where table_schema=%s and auto_increment is not null")
        src = dict(self._q("src", q, (db,), fresh=True))
        dst = dict(self._q("dst", q, (db,), fresh=True))
        bad = [f"{t} src={v} dst={dst.get(t)}" for t, v in sorted(src.items())
               if dst.get(t) != v]
        if bad:
            return [Result("autoinc", db, "diff", "; ".join(bad), "",
                           f"migkit sync {self.hop.name} --db {db} --kind sequences")]
        return [Result("autoinc", db, "ok",
                       f"{len(src)} counters, values match")]

    def check_data(self, db, table=None, stream=None, with_counts=False):
        st, dt = set(self._tables("src", db)), set(self._tables("dst", db))
        tables = [table] if table else sorted(st & dt)
        res = []
        rows_a = rows_b = 0
        bad_counts = []
        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            futs = {pool.submit(self._diff_table, db, t): t for t in tables}
            for fu in as_completed(futs):
                r, ra, rb = fu.result()
                if stream:
                    stream(f"{futs[fu]}: {r.status}")
                res.append(r)
                rows_a += ra
                rows_b += rb
                if ra != rb:
                    bad_counts.append(f"{futs[fu]} src={ra} dst={rb}")
        res = sorted(res, key=lambda r: r.scope)
        if with_counts:
            bad_counts += [f"{t} missing on target" for t in sorted(st - dt)]
            bad_counts += [f"{t} extra on target" for t in sorted(dt - st)]
            if bad_counts:
                cres = Result("counts", db, "diff",
                              "; ".join(bad_counts[:10]))
            else:
                cres = Result("counts", db, "ok",
                              f"{len(tables)} tables, rows"
                              f" {rows_a:,}=={rows_b:,}"
                              " (from the checksum pass, no extra scan)")
            res.insert(0, cres)
        return res

    def _checksum(self, side, db, t, expr, where=""):
        q = (f"select count(*), coalesce(bit_xor(crc32({expr})), 0),"
             f" coalesce(bit_xor(conv(substring(md5({expr}), 1, 8), 16, 10)), 0)"
             f" from `{db}`.`{t}` {where}")
        return tuple(self._q(side, q)[0])

    def _reladiff_url(self, side, db):
        from urllib.parse import quote
        ep = self.hop.source if side == "src" else self.hop.target
        return (f"mysql://{ep.user}:{quote(ep.password, safe='')}"
                f"@{ep.host}:{ep.port}/{db}")

    def _reladiff_table(self, db, t, pks):
        cmd = ["reladiff", self._reladiff_url("src", db), t,
               self._reladiff_url("dst", db), t, "--stats",
               "-j", str(self.hop.workers), "-c", "%"]
        for k in pks:
            cmd += ["-k", k]
        try:
            p = run(cmd, check=False, timeout=3600)
        except Exception:
            return None, "", 0, 0
        text = p.stdout + p.stderr
        if p.returncode != 0 or "ERROR" in text:
            return None, "", 0, 0
        import re as _re
        m = _re.search(r"(\d+) rows in table A.*?(\d+) rows in table B",
                       text, _re.S)
        ra, rb = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
        rows = f"rows {ra}=={rb}, " if m else ""
        if "0 rows are different" in text and "0 rows exclusive" in text:
            return "ok", f"{rows}hashdiff 0 differences (reladiff)", ra, rb
        return "diff", "", ra, rb

    def _diff_table(self, db, t):
        scope = f"{db}.{t}"
        expr = self._row_expr(db, t)
        pks = self._pk_cols(db, t)

        # builtin single-aggregate checksum is the fast default; set
        # options.reladiff: true on the hop to put reladiff first again
        if pks and which("reladiff") and self.hop.options.get("reladiff",
                                                              False):
            verdict, detail, ra, rb = self._reladiff_table(db, t, pks)
            if verdict == "ok":
                return Result("data", scope, "ok", detail), ra, rb
            if verdict == "diff":
                return self._drilldown(db, t, pks, expr, [""])

        ranges = [""]
        if len(pks) == 1:
            n = self._q("src", f"select count(*) from `{db}`.`{t}`")[0][0]
            if n > self.hop.slice:
                mm = self._q("src", f"select min(`{pks[0]}`), max(`{pks[0]}`)"
                                    f" from `{db}`.`{t}`")[0]
                if mm[0] is not None and str(mm[0]).lstrip("-").isdigit():
                    lo, hi = int(mm[0]), int(mm[1])
                    parts = max(2, n // self.hop.slice + 1)
                    step = max(1, (hi - lo + 1) // parts + 1)
                    ranges = [f"where `{pks[0]}` >= {a} and `{pks[0]}` < {a + step}"
                              for a in range(lo, hi + 1, step)]

        bad_ranges = []
        rows_a = rows_b = xor_a = xor_b = 0
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = {}
            for w in ranges:
                fa = pool.submit(self._checksum, "src", db, t, expr, w)
                fb = pool.submit(self._checksum, "dst", db, t, expr, w)
                futs[w] = (fa, fb)
            for w, (fa, fb) in futs.items():
                ra, rb = fa.result(), fb.result()
                rows_a += ra[0]
                rows_b += rb[0]
                xor_a ^= int(ra[2] or 0)
                xor_b ^= int(rb[2] or 0)
                if ra != rb:
                    bad_ranges.append(w)
        if not bad_ranges:
            return Result("data", scope, "ok",
                          f"rows {rows_a:,}=={rows_b:,}, checksum"
                          f" {xor_a:x}=={xor_b:x} ({len(ranges)} chunks)"), \
                rows_a, rows_b
        if not pks:
            return Result("data", scope, "diff",
                          "checksum differs, no pk for row drilldown", "",
                          "recopy whole table with dump/load"), rows_a, rows_b
        return self._drilldown(db, t, pks, expr, bad_ranges)

    def _drilldown(self, db, t, pks, expr, ranges):
        scope = f"{db}.{t}"
        pkexpr = "concat_ws('\\t', " + ", ".join(
            f"cast(`{c}` as char)" for c in pks) + ")"
        src, dst = {}, {}
        for w in ranges:
            q = f"select {pkexpr}, md5({expr}) from `{db}`.`{t}` {w}"
            src.update(dict(self._q("src", q)))
            dst.update(dict(self._q("dst", q)))
        missing = sorted(k for k in src if k not in dst)
        extra = sorted(k for k in dst if k not in src)
        changed = sorted(k for k in src if k in dst and src[k] != dst[k])
        d = self.hop.report_dir(db)
        for name, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            p = d / f"data-{t}.{name}"
            if rows:
                p.write_text("\n".join(rows) + "\n")
            elif p.exists():
                p.unlink()
        if not (missing or extra or changed):
            # checksum flagged a difference but the per-pk pass found none:
            # the rows settled between the two reads = in-flight CDC lag,
            # not a real diff
            return Result("data", scope, "ok",
                          "checksum flicker settled (in-flight replication),"
                          " 0 rows actually differ"), len(src), len(dst)
        return Result("data", scope, "diff",
                      f"missing={len(missing)} extra={len(extra)}"
                      f" changed={len(changed)}", str(d),
                      f"migkit sync {self.hop.name} --db {db} --kind rows"
                      " --apply"), len(src), len(dst)

    def check_deep(self, db):
        res = []

        # loads run with foreign_key_checks=0 (ours included), so orphans
        # are possible on target even though mysql normally enforces fks
        rows = self._q("dst",
                       "select constraint_name, table_name, column_name,"
                       " referenced_table_name, referenced_column_name"
                       " from information_schema.key_column_usage"
                       " where table_schema=%s"
                       " and referenced_table_name is not null"
                       " order by constraint_name, ordinal_position", (db,))
        fks = {}
        for con, t, c, rt, rc in rows:
            fk = fks.setdefault((con, t, rt), ([], []))
            fk[0].append(c)
            fk[1].append(rc)
        orphans = []
        for (con, t, rt), (cols, rcols) in sorted(fks.items()):
            nn = " and ".join(f"c.`{c}` is not null" for c in cols)
            join = " and ".join(f"p.`{r}` = c.`{c}`"
                                for c, r in zip(cols, rcols))
            n = self._q("dst", f"select count(*) from `{db}`.`{t}` c"
                               f" where {nn} and not exists"
                               f" (select 1 from `{db}`.`{rt}` p"
                               f" where {join})")[0][0]
            if n:
                orphans.append(f"{t}.{con}: {n} orphan rows")
        res.append(Result("deep", f"{db} fk", "diff" if orphans else "ok",
                          "; ".join(orphans[:5]) if orphans
                          else f"{len(fks)} fks scanned, 0 orphan rows", "",
                          "reload the child rows or delete orphans"
                          if orphans else ""))

        colq = ("select concat(table_name, '.', column_name), column_type,"
                " is_nullable, coalesce(column_default, ''),"
                " coalesce(character_set_name, ''),"
                " coalesce(collation_name, ''), extra"
                " from information_schema.columns where table_schema=%s"
                " and table_name not like 'migkit%%' order by 1")
        sc = {r[0]: r[1:] for r in self._q("src", colq, (db,))}
        dc = {r[0]: r[1:] for r in self._q("dst", colq, (db,))}
        drift = [f"{k}: src={sc[k]} dst={dc[k]}" for k in sorted(sc)
                 if k in dc and sc[k] != dc[k]]
        if drift:
            out = self.hop.report_dir(db) / "deep-columns.diff"
            out.write_text("\n".join(drift) + "\n")
            res.append(Result("deep", f"{db} columns", "diff",
                              f"{len(drift)} columns drift (type/null/"
                              f"default/charset): "
                              + "; ".join(d.split(":")[0] for d in drift[:4]),
                              str(out), "align target DDL, charset drift"
                                        " corrupts comparisons and apps"))
        else:
            res.append(Result("deep", f"{db} columns", "ok",
                              f"{len(sc)} columns compared, type/null/"
                              "default/charset/collation identical"))

        pkq = ("select k.table_name, k.column_name"
               " from information_schema.key_column_usage k"
               " join information_schema.columns c"
               " on c.table_schema = k.table_schema"
               " and c.table_name = k.table_name"
               " and c.column_name = k.column_name"
               " where k.table_schema=%s and k.constraint_name='PRIMARY'"
               " and c.data_type in ('tinyint','smallint','mediumint',"
               "'int','bigint')"
               " and 1 = (select count(*)"
               " from information_schema.key_column_usage k2"
               " where k2.table_schema = k.table_schema"
               " and k2.table_name = k.table_name"
               " and k2.constraint_name='PRIMARY')"
               " order by 1")
        spk = self._q("src", pkq, (db,))
        dpk = {r[0] for r in self._q("dst", pkq, (db,))}
        both = [(t, c) for t, c in spk if t in dpk]

        def maxes(side):
            out = {}
            for i in range(0, len(both), 200):
                q = " union all ".join(
                    f"select '{t}', coalesce(max(`{c}`), 0)"
                    f" from `{db}`.`{t}`" for t, c in both[i:i + 200])
                out.update({r[0]: int(r[1]) for r in self._q(side, q)})
            return out

        if both:
            am, bm = maxes("src"), maxes("dst")
            ahead = [f"{k} src_max={am[k]} dst_max={bm[k]}"
                     for k in sorted(am) if bm.get(k, 0) > am[k]]
            behind = [k for k in sorted(am) if bm.get(k, 0) < am[k]]
            if ahead:
                res.append(Result("deep", f"{db} boundary", "diff",
                                  f"target max(pk) AHEAD of source on"
                                  f" {len(ahead)}: {'; '.join(ahead[:4])}",
                                  "", "writes landing on target or"
                                      " double-apply, find the writer"
                                      " before cutover"))
            else:
                note = (f"; {len(behind)} behind (replication lag)"
                        if behind else "")
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
            q = ("select table_name, auto_increment from information_schema.tables"
                 " where table_schema=%s and auto_increment is not null")
            src = dict(self._q("src", q, (db,), fresh=True))
            dst = dict(self._q("dst", q, (db,), fresh=True))
            stmts = [f"alter table `{db}`.`{t}` auto_increment = {v};"
                     f"  -- dst now {dst.get(t, 'MISSING')}"
                     for t, v in sorted(src.items()) if dst.get(t) != v]
            undo = [f"alter table `{db}`.`{t}` auto_increment = {dst[t]};"
                    for t in sorted(src) if t in dst and dst.get(t) != src[t]]
            same = sum(1 for t, v in src.items() if dst.get(t) == v)
            if stmts:
                actions.append(RepairAction(
                    db, "sequences", stmts, undo,
                    f"{len(stmts)} counters differ, {same} already equal"))
        if kind in ("rows", "all"):
            d = self.hop.report_dir(db)
            tables = sorted({f.name.split(".")[0][len("data-"):]
                             for f in d.glob("data-*.missing")}
                            | {f.name.split(".")[0][len("data-"):]
                               for f in d.glob("data-*.extra")}
                            | {f.name.split(".")[0][len("data-"):]
                               for f in d.glob("data-*.changed")})
            for t in tables:
                counts = []
                for k in ("missing", "extra", "changed"):
                    f = d / f"data-{t}.{k}"
                    if f.exists():
                        counts.append(f"{k}={sum(1 for _ in f.open())}")
                stmts = [f"resync pks for {t} ({', '.join(counts)})"]
                if which("pt-table-sync"):
                    s, tg = self.hop.source, self.hop.target
                    p = run(["pt-table-sync", "--print",
                             f"h={s.host},P={s.port},u={s.user},"
                             f"p={s.password},D={db},t={t}",
                             f"h={tg.host},P={tg.port},u={tg.user},"
                             f"p={tg.password}"], check=False, timeout=300)
                    sql = [l for l in p.stdout.splitlines()
                           if l and not l.startswith("#")]
                    if sql:
                        stmts += [f"  {l}" for l in sql[:10]]
                        if len(sql) > 10:
                            stmts.append(f"  ... {len(sql) - 10} more (pt-table-sync)")
                actions.append(RepairAction(
                    db, "rows", stmts,
                    [], f"{t}: delete extra/changed on target (saved to undo"
                        " first), reinsert from source"))
        return actions

    def apply(self, db, action):
        if action.kind == "sequences":
            conn = self._conn("dst")
            try:
                with conn.cursor() as cur:
                    for s in action.statements:
                        cur.execute(s.split("  --")[0])
                conn.commit()
            finally:
                conn.close()
            return
        t = action.statements[0].split()[3]
        self._apply_rows(db, t, getattr(self, "_undo_dir", None))

    def _apply_rows(self, db, t, undo_dir=None):
        d = self.hop.report_dir(db)
        pks = self._pk_cols(db, t)
        cols = self._cols(db, t)

        def read(kind):
            f = d / f"data-{t}.{kind}"
            return [l.split("\t") for l in
                    f.read_text().splitlines()] if f.exists() else []

        missing, extra, changed = read("missing"), read("extra"), read("changed")
        to_delete = extra + changed
        to_copy = missing + changed
        cond = " and ".join(f"cast(`{c}` as char) = %s" for c in pks)
        collist = ", ".join(f"`{c}`" for c in cols)
        ph = ", ".join(["%s"] * len(cols))

        undo = Path(undo_dir) if undo_dir else d / "undo"
        undo.mkdir(parents=True, exist_ok=True)
        # complete reversible undo: for every pk repair will touch, record its
        # pre-repair target state. absent (missing) -> restore deletes it;
        # present (extra/changed) -> restore re-inserts the saved old row.
        sconn, dconn = self._conn("src"), self._conn("dst")
        touched = missing + extra + changed
        entries = []
        try:
            with dconn.cursor() as cur:
                cur.execute("set foreign_key_checks = 0")
                for pk in touched:
                    cur.execute(f"select {collist} from `{db}`.`{t}`"
                                f" where {cond}", pk)
                    got = cur.fetchall()
                    entries.append({"pk": pk, "cols": cols,
                                    "old": [list(r) for r in got] or None})
                (undo / f"rows-{t}.jsonl").write_text(
                    "".join(json.dumps(e, default=str) + "\n"
                            for e in entries))
                for pk in to_delete:
                    cur.execute(f"delete from `{db}`.`{t}` where {cond}", pk)
                with sconn.cursor() as scur:
                    for pk in to_copy:
                        scur.execute(f"select {collist} from `{db}`.`{t}`"
                                     f" where {cond}", pk)
                        rows = scur.fetchall()
                        if rows:
                            cur.executemany(
                                f"insert into `{db}`.`{t}` ({collist})"
                                f" values ({ph})", rows)
            dconn.commit()
        finally:
            sconn.close()
            dconn.close()

    def restore_rows(self, db, undo_dir):
        """Replay a complete row undo: for each touched pk, delete whatever is
        there now, then re-insert the saved pre-repair row (or leave absent)."""
        undo_dir = Path(undo_dir)
        files = sorted(undo_dir.glob("rows-*.jsonl"))
        if not files:
            return 0
        n = 0
        dconn = self._conn("dst")
        try:
            with dconn.cursor() as cur:
                cur.execute("set foreign_key_checks = 0")
                for f in files:
                    t = f.name[len("rows-"):-len(".jsonl")]
                    pks = self._pk_cols(db, t)
                    for line in f.read_text().splitlines():
                        e = json.loads(line)
                        cols = e["cols"]
                        cond = " and ".join(
                            f"cast(`{c}` as char) = %s" for c in pks)
                        cur.execute(f"delete from `{db}`.`{t}` where {cond}",
                                    e["pk"])
                        if e["old"]:
                            collist = ", ".join(f"`{c}`" for c in cols)
                            ph = ", ".join(["%s"] * len(cols))
                            cur.executemany(
                                f"insert into `{db}`.`{t}` ({collist})"
                                f" values ({ph})", e["old"])
                        n += 1
            dconn.commit()
        finally:
            dconn.close()
        return n

    def assess(self):
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope,
                          "item": item, "detail": str(detail)})

        def var(side, name):
            r = self._q(side, f"show variables like '{name}'")
            return r[0][1] if r else "?"

        sv = self._q("src", "select version()")[0][0]
        dv = self._q("dst", "select version()")[0][0]
        add("pass" if sv.split(".")[:2] == dv.split(".")[:2] else "warn",
            "instance", "server version match", f"src {sv} / dst {dv}")
        for name, want, lvl in (("log_bin", "ON", "fail"),
                                ("binlog_format", "ROW", "fail"),
                                ("binlog_row_image", "FULL", "warn")):
            v = var("src", name)
            add("pass" if v == want else lvl, "instance",
                f"{name}={want} on source (CDC requirement)", v)
        ret = var("src", "binlog_expire_logs_seconds")
        try:
            ok = int(ret) >= 86400
        except ValueError:
            ok = False
        add("pass" if ok else "warn", "instance",
            "binlog retention at least 24h", ret)
        for db in self.databases():
            rows = self._q("src",
                "select t.table_name from information_schema.tables t"
                " left join information_schema.key_column_usage k"
                " on k.table_schema = t.table_schema"
                " and k.table_name = t.table_name"
                " and k.constraint_name = 'PRIMARY'"
                " where t.table_schema = %s and t.table_type = 'BASE TABLE'"
                " and k.column_name is null group by t.table_name", (db,))
            nopk = ", ".join(r[0] for r in rows)
            add("pass" if not nopk else "warn", db,
                "tables without primary key", nopk or "none")
            eng_rows = self._q("src",
                "select count(*) from information_schema.tables"
                " where table_schema = %s and table_type = 'BASE TABLE'"
                " and engine <> 'InnoDB'", (db,))
            add("pass" if eng_rows[0][0] == 0 else "warn", db,
                "non-InnoDB tables", eng_rows[0][0])
            cs = self._q("src", "select default_character_set_name,"
                         " default_collation_name from"
                         " information_schema.schemata"
                         " where schema_name = %s", (db,))
            cd = self._q("dst", "select default_character_set_name,"
                         " default_collation_name from"
                         " information_schema.schemata"
                         " where schema_name = %s", (db,))
            if not cd:
                add("warn", db, "database exists on target", "missing")
            else:
                add("pass" if cs == cd else "warn", db,
                    "charset and collation match",
                    f"src {cs[0]} / dst {cd[0]}")

        try:
            uq = ("select concat(user,'@',host),"
                  " authentication_string from mysql.user"
                  " where user not in ('mysql.sys','mysql.session',"
                  "'mysql.infoschema','root') and user not like 'rds%'")
            su = dict(self._q("src", uq))
            du = dict(self._q("dst", uq))
            miss = sorted(set(su) - set(du))
            add("pass" if not miss else "warn", "instance",
                "user accounts present on target",
                f"{len(su)} src / {len(du)} dst"
                + (f", missing: {', '.join(miss[:5])}" if miss else ""))
            drift = sorted(u for u in (set(su) & set(du))
                           if su[u] and du[u] and su[u] != du[u])
            add("pass" if not drift else "fail", "instance",
                "account passwords match source",
                "all match" if not drift
                else f"password differs for: {', '.join(drift[:5])}"
                     " -> apps cannot log in on target")
        except Exception:
            add("warn", "instance", "cannot read mysql.user",
                "grant select on mysql.user to compare accounts")
        return items

    def list_move_tables(self, db):
        return [("", t) for t in self._tables("src", db)]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        t = tbl if tbl else sch
        key = f"{db}.{t}"
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        cols = self._cols(db, t)
        collist = ", ".join(f"`{c}`" for c in cols)
        ph = ", ".join(["%s"] * len(cols))
        pks = self._pk_cols(db, t)
        intpk = None
        if len(pks) == 1:
            r = self._q("src", "select data_type from information_schema.columns"
                        " where table_schema=%s and table_name=%s"
                        " and column_name=%s", (db, t, pks[0]))
            if r and r[0][0] in ("tinyint", "smallint", "mediumint",
                                 "int", "bigint"):
                intpk = pks[0]
        sconn, dconn = self._conn("src"), self._conn("dst")
        try:
            with dconn.cursor() as dcur, sconn.cursor() as scur:
                dcur.execute("set foreign_key_checks = 0")
                if not intpk:
                    log(f"{key}: no single int pk, single-shot copy")
                    dcur.execute(f"truncate `{db}`.`{t}`")
                    scur.execute(f"select {collist} from `{db}`.`{t}`")
                    while True:
                        rows = scur.fetchmany(5000)
                        if not rows:
                            break
                        dcur.executemany(
                            f"insert into `{db}`.`{t}` ({collist})"
                            f" values ({ph})", rows)
                    dconn.commit()
                    st["done"] = True
                    ck.save()
                    return
                mm = self._q("src", f"select coalesce(min(`{intpk}`), 0),"
                             f" coalesce(max(`{intpk}`), 0)"
                             f" from `{db}`.`{t}`")[0]
                lo, hi = int(mm[0]), int(mm[1])
                last = st.get("last", lo - 1)
                while last < hi:
                    nxt = min(last + chunk, hi)
                    dcur.execute(f"delete from `{db}`.`{t}`"
                                 f" where `{intpk}` > %s and `{intpk}` <= %s",
                                 (last, nxt))
                    scur.execute(f"select {collist} from `{db}`.`{t}`"
                                 f" where `{intpk}` > %s and `{intpk}` <= %s",
                                 (last, nxt))
                    while True:
                        rows = scur.fetchmany(5000)
                        if not rows:
                            break
                        dcur.executemany(
                            f"insert into `{db}`.`{t}` ({collist})"
                            f" values ({ph})", rows)
                    dconn.commit()
                    last = nxt
                    st["last"] = last
                    ck.save()
                    log(f"{key}: up to {intpk}={last:,} of {hi:,}")
                st["done"] = True
                ck.save()
        finally:
            sconn.close()
            dconn.close()

    def replicate_sql(self, db, copy_data=True):
        s, t = self.hop.source, self.hop.target
        pos = self._q("src", "show binary log status") or             self._q("src", "show master status")
        coords = f"file {pos[0][0]} pos {pos[0][1]}" if pos else "unknown"
        gtid = self._q("src", "show variables like 'gtid_mode'")
        gtid_on = gtid and gtid[0][1] == "ON"
        src_cmds = [
            "create user if not exists 'migkit_repl'@'%'"
            " identified by 'CHANGE_ME';",
            "grant replication slave on *.* to 'migkit_repl'@'%';",
        ]
        if "rds.amazonaws.com" in (t.host or ""):
            dst_cmds = [
                f"call mysql.rds_set_external_source ('{s.host}', {s.port},"
                " 'migkit_repl', 'CHANGE_ME',"
                + (f" '{pos[0][0]}', {pos[0][1]}," if pos else " '', 4,")
                + " 0);",
                "call mysql.rds_start_replication;",
            ]
        else:
            auto = "SOURCE_AUTO_POSITION = 1" if gtid_on else                 (f"SOURCE_LOG_FILE = '{pos[0][0]}',"
                 f" SOURCE_LOG_POS = {pos[0][1]}" if pos else "")
            dst_cmds = [
                f"change replication source to SOURCE_HOST = '{s.host}',"
                f" SOURCE_PORT = {s.port}, SOURCE_USER = 'migkit_repl',"
                f" SOURCE_PASSWORD = 'CHANGE_ME',"
                f" GET_SOURCE_PUBLIC_KEY = 1, {auto};",
                "start replica;",
            ]
        return {"src": src_cmds, "dst": dst_cmds,
                "drop_src": ["drop user if exists 'migkit_repl'@'%';"],
                "drop_dst": ["stop replica;", "reset replica all;"],
                "status": "show replica status",
                "note": f"binlog now at {coords},"
                        f" gtid {'ON' if gtid_on else 'OFF'},"
                        " run move first then replicate from these coords"}

    def setup_target_plan(self, db):
        s, t = self.hop.source, self.hop.target
        return [
            f"mysqldump -h {s.host} -u {s.user} -p --no-data --routines --triggers"
            f" --events {db} > {db}.schema.sql",
            f"mysql -h {t.host} -u {t.user} -p -e 'create database `{db}`"
            f" character set utf8mb4'  # match source charset/collation",
            f"mysql -h {t.host} -u {t.user} -p {db} < {db}.schema.sql",
            "-- set foreign_key_checks=0 on the load session or drop FKs until cutover",
            "-- then start the migration service full load + binlog replication into existing tables",
        ]

    def migration_pair(self, db):
        from urllib.parse import quote
        if not which("atlas"):
            return None, None
        s, t = self.hop.source, self.hop.target
        su = (f"mysql://{s.user}:{quote(s.password, safe='')}"
              f"@{s.host}:{s.port}/{db}")
        tu = (f"mysql://{t.user}:{quote(t.password, safe='')}"
              f"@{t.host}:{t.port}/{db}")

        def diff(a, b):
            p = run(["atlas", "schema", "diff", "--from", a, "--to", b,
                     "--exclude", "migkit_changelog"],
                    check=False, timeout=180)
            text = p.stdout.strip()
            if p.returncode or "Schemas are synced" in text:
                return ""
            return text
        return diff(tu, su), diff(su, tu)

    def fetch_sample_df(self, side, db, table, limit):
        import pandas as pd
        t = table.split(".", 1)[-1]
        cols = self._cols(db, t)
        rows = self._q(side, f"select * from `{db}`.`{t}` limit {limit}")
        return pd.DataFrame(rows, columns=cols)

    def record_ledger(self, db, entry):
        try:
            conn = self._conn("dst")
            with conn.cursor() as cur:
                cur.execute(f"create table if not exists `{db}`.migkit_changelog"
                            " (id bigint auto_increment primary key,"
                            " ran_at datetime default current_timestamp,"
                            " author varchar(64), op varchar(64),"
                            " scope varchar(255), detail text, undo_ref text)")
                cur.execute(f"insert into `{db}`.migkit_changelog"
                            " (author, op, scope, detail, undo_ref)"
                            " values (%s, %s, %s, %s, %s)",
                            (entry.get("author", "migkit"), entry.get("op"),
                             entry.get("scope") or entry.get("db"),
                             entry.get("detail") or entry.get("note"),
                             entry.get("undo_ref")))
            conn.commit()
            conn.close()
            return True
        except Exception:
            return False

    def read_ledger(self, db):
        try:
            rows = self._q("dst", f"select ran_at, author, op, scope, detail"
                           f" from `{db}`.migkit_changelog order by id")
            return [[str(r[0]), r[1] or "", r[2] or "", r[3] or "", r[4] or ""]
                    for r in rows]
        except Exception:
            return []

    def watch_sample(self, db):
        import time
        q = ("select coalesce(sum(table_rows), 0) from information_schema.tables"
             " where table_schema=%s")
        return {"db": db, "ts": time.time(),
                "src_rows": int(self._q("src", q, (db,))[0][0]),
                "dst_rows": int(self._q("dst", q, (db,))[0][0])}
