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
        q = ("select s.name+'.'+t.name, cast(ident_current(s.name+'.'+t.name) as bigint)"
             " from sys.tables t join sys.schemas s on s.schema_id=t.schema_id"
             " where objectproperty(t.object_id,'TableHasIdentity')=1 order by 1")
        src = dict(self._q("src", db, q))
        dst = dict(self._q("dst", db, q))
        bad = [f"{t} src={v} dst={dst.get(t)}" for t, v in sorted(src.items())
               if dst.get(t) != v]
        if bad:
            return [Result("autoinc", db, "diff", "; ".join(bad), "",
                           f"migkit sync {self.hop.name} --db {db} --kind sequences")]
        return [Result("autoinc", db, "ok",
                       f"{len(src)} identity tables, values match")]

    def check_data(self, db, table=None, stream=None):
        q = ("select s.name+'.'+t.name from sys.tables t"
             " join sys.schemas s on s.schema_id=t.schema_id order by 1")
        tables = [table] if table else [r[0] for r in self._q("src", db, q)]
        bad = []
        for t in tables:
            cq = (f"select count_big(*), isnull(sum(cast(binary_checksum(*) as bigint)),0)"
                  f" from {t} with (nolock)")
            try:
                a = self._q("src", db, cq)[0]
                b = self._q("dst", db, cq)[0]
            except RuntimeError as e:
                bad.append(f"{t} error {e}")
                continue
            if stream:
                stream(f"{t}: {'ok' if a == b else 'DIFF'}")
            if a != b:
                bad.append(f"{t} src={a} dst={b}")
        if bad:
            return [Result("data", db, "diff", "; ".join(bad[:10]), "",
                           "binary_checksum is coarse: confirm with reladiff"
                           " (mssql support) or tablediff utility before repair")]
        return [Result("data", db, "ok",
                       f"{len(tables)} tables, counts and binary_checksum"
                       " equal both sides")]

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

    def watch_sample(self, db):
        q = "select sum(p.rows) from sys.partitions p where p.index_id in (0,1)"
        return {"db": db, "ts": time.time(),
                "src_rows": int(self._q("src", db, q)[0][0] or 0),
                "dst_rows": int(self._q("dst", db, q)[0][0] or 0)}
