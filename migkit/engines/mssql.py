import time

from ..util import run, which
from .base import Engine, RepairAction, Result
from .dbapi import MARK, DbapiRows

SKIP_DBS = {"master", "tempdb", "model", "msdb"}


class MSSQLEngine(DbapiRows, Engine):
    checks = ("schema", "counts", "autoinc", "data")

    def _cmd(self, side, db, sql):
        ep = self.hop.source if side == "src" else self.hop.target
        if not which("sqlcmd"):
            raise SystemExit("the SQL Server client is not installed on"
                             " this machine: migkit doctor --install")
        # the password in the environment, which the client reads when
        # `-P` is absent (without either it stops to prompt for one). The
        # certificate a server makes for itself can carry a negative serial
        # number, which the client refuses to parse since Go 1.23 - measured
        # on SQL Edge: `x509: negative serial number`, and no check ran. It
        # is trusted as it is (`-C`) either way. `-b`: without it a failed
        # statement exits 0 and its message comes back as rows - measured,
        # `Msg 8134 ... Divide by zero` with exit code 0 - which both sides
        # of a comparison can print alike.
        p = run(["sqlcmd", "-S", f"{ep.host},{ep.port}", "-U", ep.user,
                 "-d", self._d(side, db), "-C", "-b", "-h", "-1", "-W",
                 "-s", "|", "-Q", f"set nocount on; {sql}"],
                env={"SQLCMDPASSWORD": ep.password,
                     "GODEBUG": "x509negativeserial=1"}, check=False)
        if p.returncode:
            # the server's message is on stdout, where the rows would be
            said = " ".join((p.stdout + p.stderr).split())
            raise RuntimeError(f"the {'source' if side == 'src' else 'target'}"
                               f" refused a statement: {said[-300:]}")
        return [l.split("|") for l in p.stdout.splitlines() if l.strip()]

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        rows = self._cmd("src", "master",
                       "select name from sys.databases where name not in"
                       " ('master','tempdb','model','msdb') order by 1")
        return [r[0] for r in rows]

    def check_params(self, db):
        def pull(side):
            rows = self._cmd(side, "master",
                           "select name, cast(value_in_use as varchar(64))"
                           " from sys.configurations order by name")
            return {r[0].strip(): r[1].strip() for r in rows if len(r) >= 2}
        return self._param_result(
            db, pull("src"), pull("dst"), (),
            "align sp_configure / server settings on the target"
            " (server collation compared separately)")

    #: the columns of a key constraint's index, in key order
    _KEY_COLUMNS = ("stuff((select ',' + c.name from sys.index_columns ic"
                    " join sys.columns c on c.object_id = ic.object_id and"
                    " c.column_id = ic.column_id where ic.object_id ="
                    " {k}.parent_object_id and ic.index_id ="
                    " {k}.unique_index_id order by ic.key_ordinal"
                    " for xml path('')), 1, 1, '')")

    def _objects(self, side, db):
        """{(name, kind): definition hash}. A constraint whose name the
        server made up is named by what it is instead - measured on SQL
        Edge, the same `primary key` made in two databases was called
        `PK__o__3213E83F720CC3A2` in one and `PK__o__3213E83FA19E373F` in
        the other, and read as one missing and one extra."""
        key_cols = self._KEY_COLUMNS.format(k="kc")
        rows = self._cmd(side, db,
                       "select s.name + '.' + case"
                       " when kc.is_system_named = 1 then"
                       " object_name(o.parent_object_id) + '.' + o.type_desc"
                       f" + '(' + {key_cols} + ')' collate database_default"
                       " when fk.is_system_named = 1 then"
                       " object_name(o.parent_object_id) + '.' + o.type_desc"
                       " + '(' + stuff((select ',' + col_name(f.parent_object_id,"
                       " f.parent_column_id) from sys.foreign_key_columns f"
                       " where f.constraint_object_id = o.object_id order by"
                       " f.constraint_column_id for xml path('')), 1, 1, '')"
                       " + ')->' + object_name(fk.referenced_object_id)"
                       " collate database_default"
                       " when dc.is_system_named = 1 then"
                       " object_name(o.parent_object_id) + '.' + o.type_desc"
                       " + '(' + col_name(dc.parent_object_id,"
                       " dc.parent_column_id) + ')' collate database_default"
                       " when cc.is_system_named = 1 then"
                       " object_name(o.parent_object_id) + '.' + o.type_desc"
                       " + '(' + convert(varchar(16), hashbytes('SHA2_256',"
                       " isnull(cc.definition, '')), 2) + ')'"
                       " collate database_default"
                       " else o.name collate database_default end,"
                       " o.type_desc,"
                       " convert(varchar(64), hashbytes('SHA2_256',"
                       " isnull(object_definition(o.object_id),'')), 2)"
                       " from sys.objects o join sys.schemas s on s.schema_id=o.schema_id"
                       " left join sys.key_constraints kc on kc.object_id = o.object_id"
                       " left join sys.foreign_keys fk on fk.object_id = o.object_id"
                       " left join sys.default_constraints dc on dc.object_id = o.object_id"
                       " left join sys.check_constraints cc on cc.object_id = o.object_id"
                       " where o.is_ms_shipped=0 and o.type in"
                       " ('U','V','P','FN','TF','IF','TR','PK','F','UQ','C','D')"
                       " order by 1,2")
        return {(r[0], r[1]): r[2] for r in rows}

    def _indexes(self, side, db):
        """{name: (kind, unique)}, an index behind a constraint the server
        named by the constraint's kind and columns, as `_objects` names
        it."""
        key_cols = self._KEY_COLUMNS.format(k="kc")
        rows = self._cmd(side, db,
                       "select s.name+'.'+t.name+'.'+case when"
                       " kc.is_system_named = 1 then kc.type_desc + '('"
                       f" + {key_cols} + ')' collate database_default"
                       " else i.name collate database_default end,"
                       " i.type_desc,"
                       " i.is_unique from sys.indexes i"
                       " join sys.tables t on t.object_id=i.object_id"
                       " join sys.schemas s on s.schema_id=t.schema_id"
                       " left join sys.key_constraints kc on"
                       " kc.parent_object_id = i.object_id and"
                       " kc.unique_index_id = i.index_id"
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
        src = {r[0]: r[1] for r in self._cmd("src", db, q)}
        dst = {r[0]: r[1] for r in self._cmd("dst", db, q)}
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
        src = {r[0]: r[2] for r in self._cmd("src", db, q)}
        drows = self._cmd("dst", db, q)
        dcol = {r[0]: r[1] for r in drows}
        dcur = {r[0]: r[2] for r in drows}
        collide = []
        for tbl, col in sorted(dcol.items()):
            try:
                mx = int(self._cmd("dst", db, f"select isnull(max([{col}]),0)"
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
        rows = self._cmd("src", db,
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
        n = int(self._cmd("src", db,
                        f"select count_big(*) from {t} with (nolock)")[0][0]
                or 0)
        if n > self.DRILL_MAX_ROWS:
            return None
        pkexpr = "+'\t'+".join(f"cast(t.{c} as varchar(100))" for c in pks)
        q = (f"select {pkexpr}, convert(varchar(64), hashbytes('SHA2_256',"
             " (select t.* for json path, include_null_values,"
             " without_array_wrapper)), 2)"
             f" from {t} t with (nolock)")
        src = {r[0]: r[1] for r in self._cmd("src", db, q)}
        dst = {r[0]: r[1] for r in self._cmd("dst", db, q)}
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
        st = [r[0] for r in self._cmd("src", db, q)]
        dt = {r[0] for r in self._cmd("dst", db, q)}
        tables = [table] if table else [t for t in st if t in dt]
        bad = []
        rows_a = rows_b = 0
        bad_counts = []
        for t in tables:
            cq = ("select count_big(*), isnull(sum(cast(binary_checksum(*)"
                  f" as bigint)),0) from {t} with (nolock)")
            try:
                a = self._cmd("src", db, cq)[0]
                b = self._cmd("dst", db, cq)[0]
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
        nopk = [r[0] for r in self._cmd("src", db,
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
                              "every table on the source has a pk or unique"
                              " index"))
        # movers load with constraints/triggers disabled and often forget
        # to re-enable or re-validate: is_disabled and is_not_trusted are
        # the sql server analog of postgres NOT VALID
        rows = self._cmd("dst", db,
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
        trg = [r[0] for r in self._cmd("dst", db,
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
        sc = {r[0].split("|", 1)[0]: r[0] for r in self._cmd("src", db, colq)}
        dc = {r[0].split("|", 1)[0]: r[0] for r in self._cmd("dst", db, colq)}
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
        pk_rows = self._cmd("src", db,
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
                a = int(self._cmd("src", db, f"select isnull(max({c}),0)"
                                f" from {t} with (nolock)")[0][0] or 0)
                b = int(self._cmd("dst", db, f"select isnull(max({c}),0)"
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
            src = dict(self._cmd("src", db,
                               "select s.name+'.'+t.name,"
                               " cast(ident_current(s.name+'.'+t.name) as bigint)"
                               " from sys.tables t join sys.schemas s"
                               " on s.schema_id=t.schema_id where"
                               " objectproperty(t.object_id,'TableHasIdentity')=1"))
            dst = dict(self._cmd("dst", db,
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
        self._cmd("dst", db,
                " ".join(s.split("  --")[0] for s in action.statements))

    def snapshot_state(self, db, state_dir, kind="all"):
        """What the target held before a repair: each identity column's
        current value, which a rollback reseeds, and - unless only those
        are repaired - the definitions of its views, functions, procedures
        and triggers as the server keeps them."""
        rows = self._rows(
            "dst", db,
            "select s.name + '.' + t.name,"
            " cast(ic.last_value as bigint) from sys.identity_columns ic"
            " join sys.tables t on t.object_id = ic.object_id"
            " join sys.schemas s on s.schema_id = t.schema_id order by 1")
        # a table no row ever went into has no value: nothing to reseed
        (state_dir / "dst-identity.txt").write_text("".join(
            f"{t}|{v}\n" for t, v in rows if v is not None))
        if kind == "sequences":
            return
        defs = self._rows(
            "dst", db,
            "select s.name + '.' + o.name, o.type_desc, m.definition"
            " from sys.sql_modules m join sys.objects o"
            " on o.object_id = m.object_id join sys.schemas s"
            " on s.schema_id = o.schema_id order by 1")
        (state_dir / "dst-schema.sql").write_text("".join(
            f"-- {name} ({what.lower()})\n{body}\ngo\n"
            for name, what, body in defs if body is not None))

    def restore_sequences(self, db, state_dir):
        """The statements that put each identity back where the snapshot
        found it."""
        path = state_dir / "dst-identity.txt"
        if not path.exists():
            return []
        out = []
        for line in path.read_text().splitlines():
            if line:
                table, value = line.rsplit("|", 1)
                named = ".".join(self._q(p) for p in table.split(".", 1))
                literal = named.replace("'", "''")
                out.append(f"dbcc checkident ('{literal}', reseed,"
                           f" {int(value)});")
        return out

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
        on = self._cmd("src", db, "select count(*) from"
                     " sys.change_tracking_databases where database_id = db_id()")
        if not on or on[0][0] != "1":
            return [Result("delta", db, "error",
                           "Change Tracking not enabled on source; run: alter"
                           f" database [{db}] set change_tracking = on"
                           " (change_retention = 2 days, auto_cleanup = on),"
                           " then per table: alter table ... enable change_tracking")]
        cur = self._cmd("src", db, "select change_tracking_current_version()")[0][0]
        if not state.exists():
            state.write_text(json.dumps({"ver": cur}))
            return [Result("delta", db, "ok", f"baseline CT version {cur}")]
        last = json.loads(state.read_text()).get("ver")
        tabs = self._cmd("src", db,
                       "select s.name+'.'+t.name from"
                       " sys.change_tracking_tables ct"
                       " join sys.tables t on t.object_id = ct.object_id"
                       " join sys.schemas s on s.schema_id = t.schema_id")
        res, clean, total = [], True, 0
        for row in tabs:
            tbl = row[0]
            c = self._cmd("src", db,
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
                "src_rows": int(self._cmd("src", db, q)[0][0] or 0),
                "dst_rows": int(self._cmd("dst", db, q)[0][0] or 0)}

    # ---- the neutral layer: SQL Server as one side of a pair of engines --
    #
    # Read through the driver and rendered here, as MongoDB and SQLite
    # are: the canonical text comes from `canon.render_value`, the one
    # renderer every in-process engine shares, not from a fourth SQL
    # rendering written for T-SQL. Written 2026-09-25 without a server to
    # run it against (none runs on this machine's architecture); the
    # comparison it feeds is the one the other engines are measured by.

    CANON_ENGINE = "mssql"
    SQL_DIALECT = "tsql"
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " a T-SQL one nobody has run")

    #: the declared type as the catalog holds it, with the numbers a
    #: counterpart needs: precision and scale, a length in characters
    DECLARED = ("type_name({c}.system_type_id) + case"
                " when type_name({c}.system_type_id) in ('decimal', 'numeric')"
                " then '(' + cast({c}.precision as varchar(4)) + ','"
                " + cast({c}.scale as varchar(4)) + ')'"
                " when type_name({c}.system_type_id) in ('varchar', 'char',"
                " 'varbinary', 'binary') then '(' + case when {c}.max_length"
                " = -1 then 'max' else cast({c}.max_length as varchar(6))"
                " end + ')'"
                " when type_name({c}.system_type_id) in ('nvarchar', 'nchar')"
                " then '(' + case when {c}.max_length = -1 then 'max' else"
                " cast({c}.max_length / 2 as varchar(6)) end + ')'"
                " when type_name({c}.system_type_id) in ('datetime2', 'time')"
                " then '(' + cast({c}.scale as varchar(2)) + ')'"
                " else '' end")

    PARAMSTYLE = "pyformat"
    LIMIT = "top"

    def _connect(self, side, db):
        import pymssql
        ep = self.hop.source if side == "src" else self.hop.target
        return pymssql.connect(server=ep.host, port=str(ep.port),
                               user=ep.user, password=ep.password,
                               database=self._d(side, db), login_timeout=15,
                               charset="UTF-8", tds_version="7.4")

    def _q(self, name):
        return "[" + str(name).replace("]", "]]") + "]"

    _bracket = _q

    def neutral_tables(self, side, db):
        return [t for (t,) in self._rows(
            side, db, "select s.name + '.' + t.name from sys.tables t join"
                      " sys.schemas s on s.schema_id = t.schema_id where"
                      " t.is_ms_shipped = 0 order by 1")
            if not self.hop.excluded(db, *t.split(".", 1))]

    def neutral_columns(self, side, db, table):
        return [(n, t) for n, t in self._rows(
            side, db, f"select c.name, {self.DECLARED.format(c='c')} from"
                      f" sys.columns c where c.object_id = object_id({MARK})"
                      " order by c.column_id",
            (self._qualified(side, db, table),))]

    def neutral_key(self, side, db, table):
        return [n for (n,) in self._rows(
            side, db, "select c.name from sys.index_columns ic"
                      " join sys.indexes i on i.object_id = ic.object_id"
                      " and i.index_id = ic.index_id and i.is_primary_key = 1"
                      " join sys.columns c on c.object_id = ic.object_id"
                      " and c.column_id = ic.column_id"
                      f" where ic.object_id = object_id({MARK})"
                      " order by ic.key_ordinal",
            (self._qualified(side, db, table),))]

    def _before_insert(self, cur, side, db, table):
        """Explicit values go into an identity column only with identity
        insert on, for the table and the session."""
        quoted = self._qualified(side, db, table)
        self._run(cur, "select count(*) from sys.identity_columns where"
                       f" object_id = object_id({MARK})", (quoted,))
        if not cur.fetchone()[0]:
            return None
        self._run(cur, f"set identity_insert {quoted} on")
        return f"set identity_insert {quoted} off"

    def _create_name(self, side, db, table):
        parts = str(table).split(".", 1)
        return ".".join(self._q(p) for p in
                        (parts if len(parts) == 2 else ["dbo"] + parts))

    def settle_target(self, db, from_source=True):
        """Statistics for every table the load wrote, which the optimiser
        otherwise builds on the first queries, while they wait."""
        self._target_only("dst", "refresh statistics")
        conn = self._connect("dst", db)
        try:
            conn.cursor().execute("exec sp_updatestats")
            conn.commit()
        finally:
            conn.close()
        return "refreshed the target's statistics"

    def neutral_views(self, side, db):
        """The select each view is defined as - the text after its `AS`,
        which is what another engine is given."""
        import sqlglot
        from sqlglot import exp
        out = []
        for name, definition in self._rows(
                side, db, "select s.name + '.' + v.name, m.definition from"
                          " sys.views v join sys.schemas s on s.schema_id ="
                          " v.schema_id join sys.sql_modules m on"
                          " m.object_id = v.object_id where v.is_ms_shipped"
                          " = 0 order by 1"):
            if self.hop.excluded(db, *name.split(".", 1)):
                continue
            try:
                tree = sqlglot.parse_one(definition, read="tsql")
                body = tree.expression if isinstance(tree, exp.Create) \
                    else None
                sql = body.sql("tsql") if body is not None else definition
            except Exception:  # noqa: BLE001 - handed on as written
                sql = definition
            out.append((name, sql))
        return out

    def neutral_functions(self, side, db):
        """Scalar functions whose body is `BEGIN RETURN <expression> END`;
        the rest - procedures, table-valued functions, bodies of
        statements - are listed with no expression, to be named."""
        import re
        import sqlglot
        from sqlglot import exp
        found = self._rows(
            side, db, "select o.object_id, o.name, o.type, m.definition from"
                      " sys.objects o join sys.sql_modules m on m.object_id"
                      " = o.object_id where o.is_ms_shipped = 0 and o.type"
                      " in ('FN', 'IF', 'TF', 'P') order by o.name")
        out = []
        for oid, name, kind, definition in found:
            params, returns = [], None
            for pname, declared, pid in self._rows(
                    side, db, f"select p.name, {self.DECLARED.format(c='p')},"
                              " p.parameter_id from sys.parameters p where"
                              f" p.object_id = {MARK} order by"
                              " p.parameter_id",
                    (oid,)):
                if pid == 0:
                    returns = declared
                else:
                    params.append((str(pname).lstrip("@"), declared))
            one = None
            m = re.search(r"\bas\s+begin\s+return\b(.*)\bend\s*;?\s*$",
                          str(definition or ""), re.I | re.S)
            if kind.strip() == "FN" and m and ";" not in \
                    m.group(1).strip().rstrip(";"):
                try:
                    tree = sqlglot.parse_one(m.group(1).strip().rstrip(";"),
                                             read="tsql")
                    for p in list(tree.find_all(exp.Parameter)):
                        p.replace(exp.column(p.name))
                    one = tree.sql("tsql")
                except Exception:  # noqa: BLE001 - named, not carried
                    one = None
            out.append((name, params, returns, one))
        return out

    def neutral_function_sql(self, name, params, returns, body):
        import sqlglot
        from sqlglot import exp
        tree = sqlglot.parse_one(body, read="tsql")
        names = {p for p, _ in params}
        for c in list(tree.find_all(exp.Column)):
            if c.name in names and not c.table:
                c.replace(exp.Parameter(this=exp.var(c.name)))
        args = ", ".join(f"@{p} {t}" for p, t in params)
        return (f"create function {self._bracket(name)}({args}) returns"
                f" {returns} as begin return {tree.sql('tsql')} end")

    # ---- the change feed: Change Tracking --------------------------------
    #
    # SQL Server's own record of which rows changed since a version, the
    # mechanism `delta_verify` reads too. It keeps the key and the last
    # operation of each row, not every change: a row changed five times
    # comes back once, and is read as it is now, which a tail applied by key
    # converges on.

    def _tracked(self, side, db):
        """The tables Change Tracking follows, as `neutral_tables` names
        them."""
        return {t for (t,) in self._rows(
            side, db, "select s.name + '.' + t.name from"
                      " sys.change_tracking_tables c join sys.tables t on"
                      " t.object_id = c.object_id join sys.schemas s on"
                      " s.schema_id = t.schema_id")}

    def change_point(self, side, db):
        """The database's Change Tracking version now. Refused where the
        database does not track, or where a table the hop moves is not
        tracked: its changes would be carried by nothing, and nothing would
        say so."""
        got = self._rows(side, db, "select change_tracking_current_version()")
        at = got[0][0] if got else None
        if at is None:
            raise SystemExit(
                f"Change Tracking is off for {db}, and it is the change log"
                " migkit reads here: alter database ... set change_tracking"
                " = on, then alter table ... enable change_tracking for"
                " each table")
        self._all_tracked(side, db)
        return int(at)

    def _all_tracked(self, side, db):
        """Stops where a table the hop moves is not tracked - asked with
        every read, since a table can be made after the tail starts."""
        missed = sorted(set(self.neutral_tables(side, db))
                        - self._tracked(side, db))
        if missed:
            raise SystemExit(
                f"Change Tracking does not follow {', '.join(missed[:6])}"
                + (f" and {len(missed) - 6} more" if len(missed) > 6 else "")
                + ", so their changes would be carried by nothing: alter"
                " table ... enable change_tracking for each")

    def log_position(self, side, db):
        got = self._rows(side, db, "select change_tracking_current_version()")
        return int(got[0][0]) if got and got[0][0] is not None else None

    @staticmethod
    def position_reached(have, want):
        if have is None or want is None:
            return None
        return int(have) >= int(want)

    def stream_identity(self, side, db):
        """The server and the database, by when it was made: a version
        from another database's tracking means nothing here."""
        got = self._rows(side, db, "select @@servername, create_date from"
                                   " sys.databases where database_id ="
                                   " db_id()")
        if not got:
            return None
        return {"server": str(got[0][0]), "database created": str(got[0][1])}

    def position_lost(self, side, db, token):
        """A table whose tracking no longer reaches back to `token`: what
        changed there before its oldest kept version is gone, and a tail
        from `token` would skip it."""
        if token is None:
            return None
        gone = [t for t, low in self._rows(
            side, db, "select s.name + '.' + t.name,"
                      " change_tracking_min_valid_version(t.object_id) from"
                      " sys.change_tracking_tables c join sys.tables t on"
                      " t.object_id = c.object_id join sys.schemas s on"
                      " s.schema_id = t.schema_id")
            if low is not None and int(low) > int(token)]
        if not gone:
            return None
        return (f"Change Tracking no longer keeps the changes of"
                f" {', '.join(sorted(gone)[:6])} from version {token}: its"
                " retention has cleaned them up")

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each row changed since `token`, as it is now: a delete where it
        is gone. The version is read before the rows, so a change made
        while they are read is read again next time rather than skipped."""
        from .. import canon
        if token is None:
            return [], self.change_point(side, db)
        lost = self.position_lost(side, db, token)
        if lost:
            raise SystemExit(lost)
        self._all_tracked(side, db)
        now = self.log_position(side, db)
        found = []
        for t in sorted(self._tracked(side, db)):
            if self.hop.excluded(db, *t.split(".", 1)):
                continue
            key = self.neutral_key(side, db, t)
            cols = [n for n, _ in self.neutral_columns(side, db, t)]
            quoted = self._qualified(side, db, t)
            rows = self._rows(
                side, db,
                "select ct.sys_change_version, ct.sys_change_operation, "
                + ", ".join(f"ct.{self._q(k)}" for k in key) + ", "
                + ", ".join(f"r.{self._q(c)}" for c in cols)
                + f", case when r.{self._q(key[0])} is null then 0 else 1"
                f" end from changetable(changes {quoted}, {MARK}) ct"
                f" left join {quoted} r on "
                + " and ".join(f"r.{self._q(k)} = ct.{self._q(k)}"
                               for k in key)
                + " order by ct.sys_change_version", (int(token),))
            for r in rows:
                version, op = r[0], str(r[1]).strip()
                ident = dict(zip(key, r[2:2 + len(key)]))
                values = dict(zip(cols, r[2 + len(key):-1]))
                if op == "D" or not r[-1]:
                    found.append((version, canon.change("delete", t, ident)))
                else:
                    found.append((version, canon.change(
                        "insert" if op == "I" else "update", t, ident,
                        values)))
        found.sort(key=lambda x: x[0])
        return [c for _, c in found], now

    # the same-engine hop keeps its target following through the pair's
    # tail, which reads the feed above and applies by key

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_seed(self, db, token_path, point):
        return self._as_pair().tail_seed(db, token_path, point)

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)
