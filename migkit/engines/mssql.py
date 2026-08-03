import time

from ..util import run, which
from .base import Engine, RepairAction, Result

SKIP_DBS = {"master", "tempdb", "model", "msdb"}


class MSSQLEngine(Engine):
    checks = ("schema", "counts", "autoinc", "data")

    def _q(self, side, db, sql):
        ep = self.hop.source if side == "src" else self.hop.target
        if not which("sqlcmd"):
            raise SystemExit("sqlcmd not found, brew install sqlcmd")
        p = run(["sqlcmd", "-S", f"{ep.host},{ep.port}", "-U", ep.user,
                 "-P", ep.password, "-d", db, "-C", "-h", "-1", "-W",
                 "-s", "|", "-Q", f"set nocount on; {sql}"])
        return [l.split("|") for l in p.stdout.splitlines() if l.strip()]

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        rows = self._q("src", "master",
                       "select name from sys.databases where name not in"
                       " ('master','tempdb','model','msdb') order by 1")
        return [r[0] for r in rows]

    def check_params(self, db):
        def pull(side):
            rows = self._q(side, "master",
                           "select name, cast(value_in_use as varchar(64))"
                           " from sys.configurations order by name")
            return {r[0].strip(): r[1].strip() for r in rows if len(r) >= 2}
        return self._param_result(
            db, pull("src"), pull("dst"), (),
            "align sp_configure / server settings on the target"
            " (server collation compared separately)")

    def _objects(self, side, db):
        rows = self._q(side, db,
                       "select s.name+'.'+o.name, o.type_desc,"
                       " convert(varchar(64), hashbytes('SHA2_256',"
                       " isnull(object_definition(o.object_id),'')), 2)"
                       " from sys.objects o join sys.schemas s on s.schema_id=o.schema_id"
                       " where o.is_ms_shipped=0 and o.type in"
                       " ('U','V','P','FN','TF','IF','TR','PK','F','UQ','C','D')"
                       " order by 1,2")
        return {(r[0], r[1]): r[2] for r in rows}

    def _indexes(self, side, db):
        rows = self._q(side, db,
                       "select s.name+'.'+t.name+'.'+i.name, i.type_desc,"
                       " i.is_unique from sys.indexes i"
                       " join sys.tables t on t.object_id=i.object_id"
                       " join sys.schemas s on s.schema_id=t.schema_id"
                       " where i.name is not null order by 1")
        return {r[0]: (r[1], r[2]) for r in rows}

    def check_schema(self, db):
        so, do = self._objects("src", db), self._objects("dst", db)
        si, di = self._indexes("src", db), self._indexes("dst", db)
        bad = []
        for k in sorted(set(so) | set(do)):
            if k not in do:
                bad.append(f"missing {k[1]} {k[0]}")
            elif k not in so:
                bad.append(f"extra {k[1]} {k[0]}")
            elif so[k] != do[k]:
                bad.append(f"definition differs {k[1]} {k[0]}")
        for k in sorted(set(si) | set(di)):
            if si.get(k) != di.get(k):
                bad.append(f"index differs {k}")
        d = self.hop.report_dir(db)
        (d / "schema-src.txt").write_text("\n".join(f"{k} {v}" for k, v in sorted(so.items())))
        (d / "schema-dst.txt").write_text("\n".join(f"{k} {v}" for k, v in sorted(do.items())))
        if bad:
            (d / "schema.diff").write_text("\n".join(bad))
            return [Result("schema", db, "diff", "; ".join(bad[:8]),
                           str(d / "schema.diff"),
                           "script objects from source (mssql-scripter) and apply")]
        return [Result("schema", db, "ok", f"{len(so)} objects, {len(si)} indexes")]

    def check_counts(self, db):
        q = ("select s.name+'.'+t.name, sum(p.rows) from sys.tables t"
             " join sys.schemas s on s.schema_id=t.schema_id"
             " join sys.partitions p on p.object_id=t.object_id and p.index_id in (0,1)"
             " group by s.name, t.name order by 1")
        src = {r[0]: r[1] for r in self._q("src", db, q)}
        dst = {r[0]: r[1] for r in self._q("dst", db, q)}
        bad = [f"{t} src={src.get(t)} dst={dst.get(t)}"
               for t in sorted(set(src) | set(dst)) if src.get(t) != dst.get(t)]
        if bad:
            return [Result("counts", db, "diff", "; ".join(bad[:10]))]
        total = sum(int(v or 0) for v in src.values())
        return [Result("counts", db, "ok",
                       f"{len(src)} tables, rows {total:,} both sides")]

    def check_autoinc(self, db):
        """usable = will IDENTITY collide on the next insert (a load that used
        IDENTITY_INSERT without a follow-up DBCC CHECKIDENT RESEED leaves the
        seed behind max(col); SQL Server does not auto-clamp it); parity = the
        seed matches the source."""
        q = ("select s.name+'.'+t.name, c.name,"
             " cast(isnull(ic.last_value,0) as bigint)"
             " from sys.identity_columns ic"
             " join sys.tables t on t.object_id=ic.object_id"
             " join sys.schemas s on s.schema_id=t.schema_id"
             " join sys.columns c on c.object_id=ic.object_id"
             " and c.column_id=ic.column_id order by 1")
        src = {r[0]: r[2] for r in self._q("src", db, q)}
        drows = self._q("dst", db, q)
        dcol = {r[0]: r[1] for r in drows}
        dcur = {r[0]: r[2] for r in drows}
        collide = []
        for tbl, col in sorted(dcol.items()):
            try:
                mx = int(self._q("dst", db, f"select isnull(max([{col}]),0)"
                                 f" from {tbl} with (nolock)")[0][0] or 0)
            except (RuntimeError, IndexError, ValueError):
                continue
            cur = int(dcur.get(tbl, 0) or 0)
            if mx > 0 and cur < mx:
                collide.append(f"{tbl}: IDENT_CURRENT={cur} < max({col})={mx}")
        res = []
        if collide:
            res.append(Result("autoinc", f"{db} usable", "diff",
                              "IDENTITY will collide on next insert: "
                              + "; ".join(collide[:8]), "",
                              f"migkit sync {self.hop.name} --db {db} --kind"
                              " sequences  (DBCC CHECKIDENT RESEED)"))
        else:
            res.append(Result("autoinc", f"{db} usable", "ok",
                              f"{len(dcol)} identity tables clear their column"
                              " max, no collision" if dcol
                              else "no identity tables"))
        parity = [f"{t} src={v} dst={dcur.get(t)}"
                  for t, v in sorted(src.items()) if dcur.get(t) != v]
        if parity:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(parity[:8]), "",
                              f"migkit sync {self.hop.name} --db {db} --kind"
                              " sequences"))
        else:
            res.append(Result("autoinc", f"{db} parity", "ok",
                              f"{len(src)} identity seeds match source"))
        return res

    counts_from_data = True

    DRILL_MAX_ROWS = 2_000_000

    def _pk_cols(self, db, t):
        sch, tbl = t.split(".", 1)
        rows = self._q("src", db,
                       "select c.name from sys.index_columns ic"
                       " join sys.indexes i on i.object_id=ic.object_id"
                       " and i.index_id=ic.index_id and i.is_primary_key=1"
                       " join sys.columns c on c.object_id=ic.object_id"
                       " and c.column_id=ic.column_id"
                       f" where ic.object_id=object_id('{sch}.{tbl}')"
                       " order by ic.key_ordinal")
        return [r[0] for r in rows]

    def _drilldown(self, db, t):
        """Row-level compare via canonical FOR JSON rendering: SQL Server
        serializes the row itself, no hand-rolled cast rules to get wrong."""
        pks = self._pk_cols(db, t)
        if not pks:
            return None
        n = int(self._q("src", db,
                        f"select count_big(*) from {t} with (nolock)")[0][0]
                or 0)
        if n > self.DRILL_MAX_ROWS:
            return None
        pkexpr = "+'\t'+".join(f"cast(t.{c} as varchar(100))" for c in pks)
        q = (f"select {pkexpr}, convert(varchar(64), hashbytes('SHA2_256',"
             " (select t.* for json path, include_null_values,"
             " without_array_wrapper)), 2)"
             f" from {t} t with (nolock)")
        src = {r[0]: r[1] for r in self._q("src", db, q)}
        dst = {r[0]: r[1] for r in self._q("dst", db, q)}
        missing = sorted(k for k in src if k not in dst)
        extra = sorted(k for k in dst if k not in src)
        changed = sorted(k for k in src
                         if k in dst and src[k] != dst[k])
        d = self.hop.report_dir(db)
        for kind, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            f = d / f"data-{t}.{kind}"
            if rows:
                f.write_text("\n".join(rows) + "\n")
            elif f.exists():
                f.unlink()
        return len(missing), len(extra), len(changed)

    def check_data(self, db, table=None, stream=None, with_counts=False):
        q = ("select s.name+'.'+t.name from sys.tables t"
             " join sys.schemas s on s.schema_id=t.schema_id order by 1")
        st = [r[0] for r in self._q("src", db, q)]
        dt = {r[0] for r in self._q("dst", db, q)}
        tables = [table] if table else [t for t in st if t in dt]
        bad = []
        rows_a = rows_b = 0
        bad_counts = []
        for t in tables:
            cq = ("select count_big(*), isnull(sum(cast(binary_checksum(*)"
                  f" as bigint)),0) from {t} with (nolock)")
            try:
                a = self._q("src", db, cq)[0]
                b = self._q("dst", db, cq)[0]
            except RuntimeError as e:
                bad.append(f"{t} error {e}")
                continue
            rows_a += int(a[0] or 0)
            rows_b += int(b[0] or 0)
            if a[0] != b[0]:
                bad_counts.append(f"{t} src={a[0]} dst={b[0]}")
            if stream:
                stream(f"{t}: {'ok' if a == b else 'DIFF'}")
            if a != b:
                drill = self._drilldown(db, t)
                if drill == (0, 0, 0):
                    continue  # settled between the two reads = in-flight
                if drill:
                    m, e, c = drill
                    bad.append(f"{t} missing={m} extra={e} changed={c}"
                               " (pk files written)")
                else:
                    bad.append(f"{t} src={a} dst={b}")
        res = []
        if with_counts:
            bad_counts += [f"{t} missing on target" for t in st
                           if t not in dt]
            if bad_counts:
                res.append(Result("counts", db, "diff",
                                  "; ".join(bad_counts[:10])))
            else:
                res.append(Result("counts", db, "ok",
                                  f"{len(tables)} tables, rows"
                                  f" {rows_a:,}=={rows_b:,}"
                                  " (from the checksum pass, no extra scan)"))
        if bad:
            res.append(Result("data", db, "diff", "; ".join(bad[:10]), "",
                              "pk-level diffs in data-*.missing/extra/"
                              "changed; repair via tablediff -f fix.sql,"
                              " review, then apply"))
        else:
            res.append(Result("data", db, "ok",
                              f"{len(tables)} tables, counts and checksums"
                              " equal both sides (binary_checksum + FOR"
                              " JSON hash drilldown)"))
        return res

    def check_deep(self, db):
        res = []
        # no pk/unique = CDC drops its updates/deletes and it can't be verified
        # or repaired by key (same trap as the other engines)
        nopk = [r[0] for r in self._q("src", db,
                "select s.name+'.'+t.name from sys.tables t"
                " join sys.schemas s on s.schema_id=t.schema_id"
                " where not exists (select 1 from sys.indexes i"
                " where i.object_id=t.object_id"
                " and (i.is_primary_key=1 or i.is_unique=1))")]
        if nopk:
            res.append(Result("deep", f"{db} keys", "diff",
                              f"{len(nopk)} tables have no pk/unique (CDC drops"
                              " their updates/deletes, unverifiable by key): "
                              + ", ".join(nopk[:5]), "",
                              "add a primary key or unique index before"
                              " migrating"))
        else:
            res.append(Result("deep", f"{db} keys", "ok",
                              "every table has a pk or unique index"))
        # movers load with constraints/triggers disabled and often forget
        # to re-enable or re-validate: is_disabled and is_not_trusted are
        # the sql server analog of postgres NOT VALID
        rows = self._q("dst", db,
                       "select s.name+'.'+t.name+'.'+fk.name,"
                       " fk.is_disabled, fk.is_not_trusted"
                       " from sys.foreign_keys fk"
                       " join sys.tables t on t.object_id=fk.parent_object_id"
                       " join sys.schemas s on s.schema_id=t.schema_id")
        disabled = [r[0] for r in rows if r[1] == "1"]
        untrusted = [r[0] for r in rows if r[1] == "0" and r[2] == "1"]
        bad = ([f"disabled: {', '.join(disabled[:4])}"] if disabled else []) \
            + ([f"not trusted (loaded WITH NOCHECK):"
                f" {', '.join(untrusted[:4])}"] if untrusted else [])
        res.append(Result("deep", f"{db} fk", "diff" if bad else "ok",
                          "; ".join(bad) if bad
                          else f"{len(rows)} fks enabled and trusted", "",
                          "alter table ... with check check constraint ..."
                          if bad else ""))
        trg = [r[0] for r in self._q("dst", db,
               "select s.name+'.'+t.name+'.'+tr.name from sys.triggers tr"
               " join sys.tables t on t.object_id=tr.parent_id"
               " join sys.schemas s on s.schema_id=t.schema_id"
               " where tr.is_disabled=1")]
        res.append(Result("deep", f"{db} triggers",
                          "diff" if trg else "ok",
                          "disabled on target: " + ", ".join(trg[:5]) if trg
                          else "no disabled triggers on target", "",
                          "enable trigger ... on ..." if trg else ""))
        colq = ("select table_schema+'.'+table_name+'.'+column_name+'|'+"
                "data_type+'|'+is_nullable+'|'+isnull(column_default,'')+'|'+"
                "isnull(cast(character_maximum_length as varchar),'')+'|'+"
                "isnull(cast(numeric_precision as varchar),'')+'|'+"
                "isnull(collation_name,'')"
                " from information_schema.columns order by 1")
        sc = {r[0].split("|", 1)[0]: r[0] for r in self._q("src", db, colq)}
        dc = {r[0].split("|", 1)[0]: r[0] for r in self._q("dst", db, colq)}
        drift = [k for k in sorted(sc) if k in dc and sc[k] != dc[k]]
        if drift:
            out = self.hop.report_dir(db) / "deep-columns.diff"
            out.write_text("\n".join(f"src {sc[k]}\ndst {dc[k]}"
                                     for k in drift) + "\n")
            res.append(Result("deep", f"{db} columns", "diff",
                              f"{len(drift)} columns drift: "
                              + ", ".join(drift[:4]), str(out),
                              "align target DDL (type/null/default/"
                              "collation)"))
        else:
            res.append(Result("deep", f"{db} columns", "ok",
                              f"{len(sc)} columns compared, identical"))
        pk_rows = self._q("src", db,
                          "select s.name+'.'+t.name, c.name"
                          " from sys.tables t"
                          " join sys.schemas s on s.schema_id=t.schema_id"
                          " join sys.index_columns ic on"
                          " ic.object_id=t.object_id"
                          " join sys.indexes i on i.object_id=ic.object_id"
                          " and i.index_id=ic.index_id and i.is_primary_key=1"
                          " join sys.columns c on c.object_id=ic.object_id"
                          " and c.column_id=ic.column_id"
                          " join sys.types ty on ty.user_type_id="
                          "c.user_type_id and ty.name in"
                          " ('int','bigint','smallint','tinyint')"
                          " where 1=(select count(*) from sys.index_columns"
                          " ic2 join sys.indexes i2 on"
                          " i2.object_id=ic2.object_id and"
                          " i2.index_id=ic2.index_id and i2.is_primary_key=1"
                          " where ic2.object_id=t.object_id)")
        ahead, behind, n = [], [], 0
        for t, c in pk_rows:
            n += 1
            try:
                a = int(self._q("src", db, f"select isnull(max({c}),0)"
                                f" from {t} with (nolock)")[0][0] or 0)
                b = int(self._q("dst", db, f"select isnull(max({c}),0)"
                                f" from {t} with (nolock)")[0][0] or 0)
            except RuntimeError:
                continue
            if b > a:
                ahead.append(f"{t} src_max={a} dst_max={b}")
            elif b < a:
                behind.append(t)
        if ahead:
            res.append(Result("deep", f"{db} boundary", "diff",
                              f"target max(pk) AHEAD of source on"
                              f" {len(ahead)}: {'; '.join(ahead[:4])}", "",
                              "writes landing on target or double-apply,"
                              " find the writer before cutover"))
        else:
            note = (f"; {len(behind)} behind (replication lag)"
                    if behind else "")
            res.append(Result("deep", f"{db} boundary", "ok",
                              f"max(pk) checked on {n} tables,"
                              f" none ahead of source{note}"))
        return res

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            src = dict(self._q("src", db,
                               "select s.name+'.'+t.name,"
                               " cast(ident_current(s.name+'.'+t.name) as bigint)"
                               " from sys.tables t join sys.schemas s"
                               " on s.schema_id=t.schema_id where"
                               " objectproperty(t.object_id,'TableHasIdentity')=1"))
            dst = dict(self._q("dst", db,
                               "select s.name+'.'+t.name,"
                               " cast(ident_current(s.name+'.'+t.name) as bigint)"
                               " from sys.tables t join sys.schemas s"
                               " on s.schema_id=t.schema_id where"
                               " objectproperty(t.object_id,'TableHasIdentity')=1"))
            stmts = [f"dbcc checkident ('{t}', reseed, {v});"
                     f"  -- dst now {dst.get(t, 'MISSING')}"
                     for t, v in sorted(src.items()) if dst.get(t) != v]
            undo = [f"dbcc checkident ('{t}', reseed, {dst[t]});"
                    for t in sorted(src) if t in dst and dst.get(t) != src[t]]
            same = sum(1 for t, v in src.items() if dst.get(t) == v)
            if stmts:
                actions.append(RepairAction(
                    db, "sequences", stmts, undo,
                    f"{len(stmts)} identities differ, {same} already equal"))
        if kind in ("rows", "all"):
            actions.append(RepairAction(
                db, "rows", ["tablediff -sourceserver ... -destinationserver ..."
                             " -f fix.sql  # generates repair T-SQL"],
                [], "use the tablediff utility, it emits repair sql you can review"))
        return actions

    def apply(self, db, action):
        if action.kind != "sequences":
            raise SystemExit("mssql row repair is manual, see plan notes")
        self._q("dst", db,
                " ".join(s.split("  --")[0] for s in action.statements))

    def setup_target_plan(self, db):
        return [
            "python -m pip install mssql-scripter",
            f"mssql-scripter -S <src> -d {db} --schema-and-data=schema > {db}.schema.sql",
            f"sqlcmd -S <dst> -Q \"create database [{db}]\"",
            f"sqlcmd -S <dst> -d {db} -i {db}.schema.sql",
            "-- disable FK/triggers on target during load, then start the migration service",
        ]

    def delta_verify(self, db, limit=20000, log=None):
        """SQL Server Change Tracking (the native mechanism): CHANGETABLE
        lists rows changed since a version; re-verify those tables and
        advance the version only on a clean cycle. Requires CT enabled."""
        import json
        state = self.hop.report_dir(db) / "delta-ctver.json"
        on = self._q("src", db, "select count(*) from"
                     " sys.change_tracking_databases where database_id = db_id()")
        if not on or on[0][0] != "1":
            return [Result("delta", db, "error",
                           "Change Tracking not enabled on source; run: alter"
                           f" database [{db}] set change_tracking = on"
                           " (change_retention = 2 days, auto_cleanup = on),"
                           " then per table: alter table ... enable change_tracking")]
        cur = self._q("src", db, "select change_tracking_current_version()")[0][0]
        if not state.exists():
            state.write_text(json.dumps({"ver": cur}))
            return [Result("delta", db, "ok", f"baseline CT version {cur}")]
        last = json.loads(state.read_text()).get("ver")
        tabs = self._q("src", db,
                       "select s.name+'.'+t.name from"
                       " sys.change_tracking_tables ct"
                       " join sys.tables t on t.object_id = ct.object_id"
                       " join sys.schemas s on s.schema_id = t.schema_id")
        res, clean, total = [], True, 0
        for row in tabs:
            tbl = row[0]
            c = self._q("src", db,
                        f"select count(*) from changetable(changes {tbl},"
                        f" {last}) ct")
            n = int(c[0][0]) if c and c[0][0].lstrip("-").isdigit() else 0
            if n == 0:
                continue
            total += n
            r = self._drilldown(db, tbl)
            ok = r == (0, 0, 0)
            clean = clean and ok
            res.append(Result("delta", f"{db}.{tbl}", "ok" if ok else "diff",
                              f"{n} rows changed since v{last}, table"
                              f" {'verified equal' if ok else 'DIFFERS'}"))
            if log:
                log(f"{tbl}: {n} changed, {'ok' if ok else 'DIFF'}")
        if clean:
            state.write_text(json.dumps({"ver": cur}))
        res.insert(0, Result("delta", db, "ok" if clean else "diff",
                             f"{total} changed rows across {len(res)} tables"
                             f" since v{last}, version"
                             f" {'advanced' if clean else 'NOT advanced'}"))
        return res

    def watch_sample(self, db):
        q = "select sum(p.rows) from sys.partitions p where p.index_id in (0,1)"
        return {"db": db, "ts": time.time(),
                "src_rows": int(self._q("src", db, q)[0][0] or 0),
                "dst_rows": int(self._q("dst", db, q)[0][0] or 0)}
