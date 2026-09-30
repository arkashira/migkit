import time

from .. import canon, decide
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
        row, _ = self._row_json(db, t)
        q = (f"select {pkexpr}, convert(varchar(64), hashbytes('SHA2_256',"
             f" {row}), 2) from {t} t with (nolock)")
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

    #: the tasks waiting for a CPU against the CPUs the server schedules on -
    #: SQL Server's saturation, where active sessions against a limit of
    #: 32,767 connections say nothing - and, where the database read is a
    #: readable secondary of an availability group, its redo still to do
    #: over the rate it redoes at: its own distance behind the primary
    HEALTH_SQL = (
        "select (select isnull(sum(runnable_tasks_count), 0) from"
        " sys.dm_os_schedulers where status = 'VISIBLE ONLINE'),"
        " (select count(*) from sys.dm_os_schedulers"
        " where status = 'VISIBLE ONLINE'),"
        " (select max(case when redo_rate > 0 then redo_queue_size * 1.0"
        " / redo_rate else 0 end) from sys.dm_hadr_database_replica_states"
        " where is_local = 1 and is_primary_replica = 0"
        " and database_id = db_id())")

    def row_bucket_sql(self, side, db, table, columns, n):
        """The server's own checksum of the whole row, in [0, n): a heap -
        a table with no key, which SQL Server keeps no position of a client
        can read a span by - is copied in buckets of it
        (`ranges.keyless`). `binary_checksum(*)` passes over the columns it
        cannot hash (text, ntext, image, xml): two rows alike in all the
        others share a bucket, which is all a bucket needs."""
        return (f"(cast(binary_checksum(*) as bigint) + 2147483648)"
                f" % {int(n)}")

    def _health(self, side, db=None):
        """What this side says about its own load, for the throttle
        (`HEALTH_SQL`). None where the server will not say (no VIEW SERVER
        STATE): a hidden signal is not an idle server."""
        from ..throttle import Health
        try:
            got = self._rows(side, db or self.databases()[0],
                             self.HEALTH_SQL)[0]
        except Exception:  # noqa: BLE001 - no reading, then
            return None
        runnable, cpus, lag = got
        busy = float(runnable or 0) / max(float(cpus or 0), 1.0)
        return Health(busy_ratio=busy,
                      lag_seconds=None if lag is None else float(lag),
                      replica=lag is not None)

    def check_data(self, db, table=None, stream=None, with_counts=False):
        from ..throttle import Throttle
        # a table's two row-hash sums at a time, the source's load asked
        # between them (backlog C4)
        gate = Throttle(1, probe=lambda: self._health("src", db))
        q = ("select s.name+'.'+t.name from sys.tables t"
             " join sys.schemas s on s.schema_id=t.schema_id order by 1")
        st = [r[0] for r in self._cmd("src", db, q)]
        dt = {r[0] for r in self._cmd("dst", db, q)}
        tables = [table] if table else [t for t in st if t in dt]
        bad, left = [], []
        rows_a = rows_b = 0
        bad_counts = []
        salt = self.digest_salt()
        for t in tables:
            # each row's hash as the server writes the row out, the same
            # the drilldown compares, summed as a decimal so a row there
            # twice counts twice. It was BINARY_CHECKSUM(*), which leaves
            # out xml, text, ntext and image columns - measured, a table
            # whose xml differed on every row passed as equal. 64 bits of
            # it, behind the run's salt (`checkpoint.new_salt`)
            try:
                row, out = self._row_json(db, t)
            except Exception as e:  # noqa: BLE001 - the table is named
                bad.append(f"{t} error {e}")
                continue
            left += out
            cq = ("select count_big(*), isnull(sum(h), 0) from (select"
                  " cast(convert(bigint, substring(hashbytes('SHA2_256',"
                  f" N'{salt}' + {row}), 1, 8)) as decimal(38, 0)) h"
                  f" from {t} t) rows_hashed")
            try:
                with gate.unit():
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
                    # The sums differed and no row does: a write between
                    # the reads, if they now agree - asked, not assumed.
                    # Still different, nothing found where, and it is said.
                    if self._cmd("src", db, cq)[0] != \
                            self._cmd("dst", db, cq)[0]:
                        bad.append(f"{t} differs, not localized: the sums"
                                   " still differ and no row differs by"
                                   " key")
                    continue
                if drill:
                    # confirmed before it is called different: where a tail
                    # follows the source, waited on and asked again
                    _, healed, _ = self._resolve_inflight(db, [t], stream)
                    if t in healed:
                        continue
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
        said = ("; left out of the row hash, as every copy writes them"
                f" anew: {', '.join(left[:8])}"
                + (f" and {len(left) - 8} more" if len(left) > 8 else "")
                if left else "")
        self._last_throttle = gate.summary()
        note = gate.line()
        if note and stream:
            stream(f"# {note}")
        if bad:
            res.append(Result("data", db, "diff", "; ".join(bad[:10]) + said,
                              "", "pk-level diffs in data-*.missing/extra/"
                              f"changed; migkit sync {self.hop.name} --db"
                              f" {db} --kind rows shows the repair, --apply"
                              " runs it"))
        else:
            res.append(Result("data", db, "ok",
                              f"{len(tables)} tables, counts and every"
                              " row's hash equal both sides" + said))
        return res

    #: CLR types a row hash reads through their own rendering: `FOR JSON`
    #: refuses them outright (Msg 13604), which left every table holding
    #: one reported as an error and never compared. A shape as its SRID and
    #: its well-known binary with Z and M; a hierarchy node as its path
    #: the types a value of which the row hash takes as its length and hash
    #: where it is wider than `canon.WIDE`; `datalength` reads the length
    #: the server keeps, and `hashbytes` its bytes
    WIDE_TYPES = ("varbinary", "varchar", "nvarchar")

    CLR_RENDERED = {
        "geography": "cast({c}.STSrid as varchar(11)) + ':'"
                     " + convert(varchar(max), {c}.AsBinaryZM(), 2)",
        "geometry": "cast({c}.STSrid as varchar(11)) + ':'"
                    " + convert(varchar(max), {c}.AsBinaryZM(), 2)",
        "hierarchyid": "{c}.ToString()",
    }

    def _row_json(self, db, t):
        """(the row of `t` as JSON, aliased `t`; the columns left out).

        Named, not `t.*`: in name order read from the source and run
        alike on both sides, so a target whose columns stand in another
        order hashes the same, and a HIDDEN column (a temporal table's
        period) is compared rather than skipped; each column under a name
        of its own, since `FOR JSON PATH` turns a name holding a dot into a
        nested object. A `rowversion` is left out and named: the server
        writes a new one into every row a copy inserts, so it differs on
        every row of a table that was copied correctly. CLR types go
        through `CLR_RENDERED`; one of an assembly nobody here knows,
        through its own bytes.
        """
        cols = self._rows(
            "src", db, "select c.name, ty.name, ty.is_assembly_type,"
                       " c.system_type_id from sys.columns c join sys.types"
                       " ty on ty.user_type_id = c.user_type_id where"
                       f" c.object_id = object_id({MARK}) order by c.name",
            (self._qualified("src", db, t),))
        parts, left = [], []
        for name, kind, assembly, system in cols:
            c = f"t.{self._q(name)}"
            if int(system) == 189:
                left.append(f"{t}.{name} (rowversion)")
                continue
            if assembly and kind in self.CLR_RENDERED:
                c = self.CLR_RENDERED[kind].format(c=c)
            elif assembly:
                c = f"cast({c} as varbinary(max))"
            elif kind in self.WIDE_TYPES:
                # a value wider than `canon.WIDE` as its length and hash,
                # hashed where it lies instead of written into the row's
                # JSON a third larger (backlog R22); any other as before
                wide = (f"concat('{canon.WIDE_MARK}', datalength({c}), ':',"
                        f" convert(varchar(64), hashbytes('SHA2_256', {c}),"
                        " 2))")
                if kind == "varbinary":
                    # of the column's own type, so a value that is not wide
                    # is written as it always was
                    wide = f"cast({wide} as varbinary(max))"
                c = f"iif(datalength({c}) > {canon.WIDE}, {wide}, {c})"
            parts.append(f"{c} as [c{len(parts)}]")
        if not parts:
            return "N'{}'", left
        return (f"(select {', '.join(parts)} for json path,"
                " include_null_values, without_array_wrapper)", left)

    # --- the confirm pass (base `fenced_recheck`) ---------------------------

    def _compare_pks(self, db, table, keys):
        """(missing, extra, changed) among these keys, as the drilldown
        writes them - each row's hash read again from both sides, now."""
        pks = self._pk_cols(db, table)
        if not pks:
            return None
        pkexpr = "+'\t'+".join(f"cast(t.{c} as varchar(100))" for c in pks)
        row, _ = self._row_json(db, table)
        keys = sorted(keys)
        got = {"src": {}, "dst": {}}
        for i in range(0, len(keys), 500):
            part = ", ".join("'" + k.replace("'", "''") + "'"
                             for k in keys[i:i + 500])
            q = (f"select {pkexpr}, convert(varchar(64), hashbytes("
                 f"'SHA2_256', {row}), 2)"
                 f" from {table} t where {pkexpr} in ({part})")
            for side in got:
                got[side].update({r[0]: r[1] for r in
                                  self._cmd(side, db, q)})
        a, b = got["src"], got["dst"]
        return (sorted(k for k in a if k not in b),
                sorted(k for k in b if k not in a),
                sorted(k for k in a if k in b and a[k] != b[k]))

    def _write_pk_files(self, db, table, missing, extra, changed):
        d = self.hop.report_dir(db)
        for kind, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            f = d / f"data-{table}.{kind}"
            if rows:
                f.write_text("\n".join(rows) + "\n")
            elif f.exists():
                f.unlink()

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
            actions += self._rows_found_plan(db)
        return actions

    def _found(self, db, t):
        """{missing, changed, extra: [key as canonical text]} of one table,
        from the drilldown's files: each line the key's columns as the
        server writes them as text, tab-joined (`_drilldown`)."""
        from .. import repairlog
        d = self.hop.report_dir(db)
        pks = self._pk_cols(db, t)
        sc, _, _ = self._comparable_columns(db, self, t, self, t)
        out = {}
        for kind in ("missing", "changed", "extra"):
            f = d / f"data-{t}.{kind}"
            lines = [x for x in f.read_text().splitlines() if x] \
                if f.exists() else []
            out[kind] = [list(k) for k in repairlog.canon_keys(
                sc, pks, [x.split("\t") for x in lines])]
        return out

    def _rows_found_plan(self, db):
        """The rows the last check found, one action a table, repaired
        through the neutral contract (`_apply_rows`) - the base's plan, over
        this engine's own drilldown files."""
        d = self.hop.report_dir(db)
        tables = sorted({f.name[len("data-"):].rsplit(".", 1)[0]
                         for k in ("missing", "changed", "extra")
                         for f in d.glob(f"data-*.{k}")})
        out = []
        for t in tables:
            n = {k: sum(1 for x in (d / f"data-{t}.{k}").read_text()
                        .splitlines() if x)
                 if (d / f"data-{t}.{k}").exists() else 0
                 for k in ("missing", "changed", "extra")}
            if not any(n.values()):
                continue
            out.append(RepairAction(
                f"{db}.{t}", "rows",
                [f"copy {n['missing'] + n['changed']} rows from the source,"
                 f" delete {n['extra']} rows the source does not have"],
                [], f"{n['missing']} missing, {n['changed']} changed,"
                    f" {n['extra']} extra; every target row this overwrites"
                    " or removes is written to the undo file first"))
        return out

    def apply(self, db, action):
        if action.kind == "rows":
            t = action.scope.split(".", 1)[1]
            sc, dc, _ = self._comparable_columns(db, self, t, self, t)
            return self._apply_rows(db, t, self, t, sc, self, t, dc,
                                    found=self._found(db, t))
        if action.kind != "sequences":
            raise SystemExit(f"mssql cannot apply a {action.kind!r} action")
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
            from .. import sourcechange as sc
            # the same words as the DBA's script and every other refusal
            return [Result("delta", db, "error",
                           sc.requirement("change_tracking", "off",
                                          sc.Ctx(db=db))
                           + " Until it is on, `migkit check` compares"
                             " every row rather than the ones changed")]
        if not state.exists():
            cur = self._cmd("src", db,
                            "select change_tracking_current_version()")[0][0]
            state.write_text(json.dumps({"ver": cur}))
            return [Result("delta", db, "ok", f"baseline CT version {cur}")]
        last = json.loads(state.read_text()).get("ver")
        tabs = [r[0] for r in self._cmd(
            "src", db, "select s.name+'.'+t.name from"
                       " sys.change_tracking_tables ct"
                       " join sys.tables t on t.object_id = ct.object_id"
                       " join sys.schemas s on s.schema_id = t.schema_id")]

        def read(c):
            out = {}
            for tbl in tabs:
                self._run(c, "select count(*) from changetable(changes"
                             f" {tbl}, {MARK}) ct", (int(last),))
                out[tbl] = int(c.fetchone()[0])
            return out
        try:
            changed, cur = self._ct_read("src", db, last, read)
        except SystemExit as e:
            # the changes since the last clean cycle are gone: nothing can
            # re-verify them, and the next cycle must not start after them
            state.unlink(missing_ok=True)
            return [Result("delta", db, "diff",
                           f"{e} - what changed since v{last} cannot be"
                           " verified again, so 'in sync' would be a lie",
                           "", "run a full check to re-baseline; raise the"
                           " change retention to cover the time between"
                           " checks")]
        res, clean, total = [], True, 0
        for tbl in tabs:
            n = changed[tbl]
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
                               charset="UTF-8", tds_version="7.4",
                               **ep.mssql_tls())

    def _q(self, name):
        return "[" + str(name).replace("]", "]]") + "]"

    _bracket = _q

    def neutral_tables(self, side, db):
        return [t for (t,) in self._rows(
            side, db, "select s.name + '.' + t.name from sys.tables t join"
                      " sys.schemas s on s.schema_id = t.schema_id where"
                      " t.is_ms_shipped = 0 order by 1")
            if not self.hop.excluded(db, *t.split(".", 1))]

    def column_facts(self, side, db, table):
        """Whether each column takes NULL, and the collation its text is
        compared by - the database's own for a table migkit builds here,
        `SQL_Latin1_General_CP1_CI_AS` unless someone chose otherwise,
        which takes `a` and `A` for one key."""
        if table is None:
            got = self._rows(side, db, "select cast(databasepropertyex("
                                       "db_name(), 'Collation') as"
                                       " nvarchar(128))")
            return {None: {"null": True, "collation": got[0][0]}}
        return {str(n): {"null": bool(null), "collation": co}
                for n, null, co in self._rows(
                    side, db, "select c.name, c.is_nullable,"
                              " c.collation_name from sys.columns c where"
                              f" c.object_id = object_id({MARK})",
                    (self._qualified(side, db, table),))}

    def key_merges(self, side, db, columns, batches, examples=3):
        """Asked of this server's own collation, as MySQL's is: the keys go
        into a temporary table whose columns compare as the target's do,
        and the server groups them. Under every collation, `_BIN2` too,
        SQL Server compares two strings with the trailing spaces of the
        shorter made up, so `a` and `a ` are one key whatever the column
        says."""
        if all(co is None for _, co in columns):
            return None
        q = self._q
        defs = ", ".join(
            f"{q(n)} nvarchar(4000) collate {co or 'Latin1_General_BIN2'}"
            for n, co in columns)
        cols = ", ".join(q(n) for n, _ in columns)
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(f"create table #migkit_key_merges ({defs})")
            insert = ("insert into #migkit_key_merges values ("
                      + ", ".join(["%s"] * len(columns)) + ")")
            for rows in batches:
                if rows:
                    cur.executemany(insert, rows)
            cur.execute(f"select count(*) from (select 1 as one from"
                        f" #migkit_key_merges group by {cols} having"
                        " count(*) > 1) g")
            groups = int(cur.fetchone()[0])
            shown = []
            if groups:
                cur.execute(f"select top ({int(examples)}) {cols} from"
                            f" #migkit_key_merges group by {cols} having"
                            " count(*) > 1")
                for rep in cur.fetchall():
                    # every row, not `distinct`: that compares by the
                    # collation too, and gave back one of the group
                    cur.execute(f"select {cols} from"
                                " #migkit_key_merges where " + " and ".join(
                                    f"{q(n)} = %s" for n, _ in columns),
                                tuple(rep))
                    shown.append([tuple(r) for r in cur.fetchall()])
            cur.execute("drop table #migkit_key_merges")
        finally:
            conn.close()
        return groups, shown

    #: `char(10)` holding 'abc' comes back 'abc       ' (the test's own
    #: premise, on Azure SQL Edge) and read as a difference from
    #: PostgreSQL's `char(10)`, which reads 'abc'
    PADDED = ("char", "nchar")

    def _read_as(self, quoted, declared):
        """A `geography` or a `geometry` as its SRID and its well-known
        binary with Z and M (`geo`), the same rendering the row hash reads
        it by (`CLR_RENDERED`): the driver hands back SQL Server's own
        serialisation of it, which no other engine reads."""
        from .. import canon, geo
        name = canon._split_declared(declared)[0]
        if name not in ("geography", "geometry"):
            return None
        return (f"case when {quoted} is null then null else "
                + self.CLR_RENDERED[name].format(c=quoted) + " end",
                geo.of)

    def _nanos_text(self, quoted, declared):
        """A `datetime2` or `time` of seven digits as the server writes
        it: the driver hands back six."""
        from .. import canon
        name, nums = canon._split_declared(declared)
        if name in ("datetime2", "time") and (nums[0] if nums else 7) > 6:
            return (f"convert(varchar(27), {quoted}, 121)"
                    if name == "datetime2" else
                    f"convert(varchar(16), {quoted})")
        return None

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

    def table_facts(self, side, db):
        """{table: rows, bytes, index bytes, key} from the catalogue in one
        query: the rows the heap or clustered index holds and the pages in
        use, which the plan estimates from and a copy records its rate
        by."""
        pages = ("(select sum(a.used_pages) * 8192 from sys.partitions p"
                 " join sys.allocation_units a on a.container_id ="
                 " p.partition_id where p.object_id = t.object_id and"
                 " p.index_id {})")
        out = {}
        for name, rows, data, index, key in self._rows(
                side, db, "select s.name + '.' + t.name, (select"
                          " sum(p.rows) from sys.partitions p where"
                          " p.object_id = t.object_id and p.index_id in"
                          f" (0, 1)), {pages.format('in (0, 1)')},"
                          f" {pages.format('> 1')}, case when exists (select"
                          " 1 from sys.indexes i where i.object_id ="
                          " t.object_id and i.is_primary_key = 1) then 1"
                          " else 0 end from sys.tables t join sys.schemas s"
                          " on s.schema_id = t.schema_id where"
                          " t.is_ms_shipped = 0"):
            out[name] = {"rows": None if rows is None else int(rows),
                         "bytes": None if data is None else int(data),
                         "index_bytes": int(index or 0), "key": bool(key)}
        return out

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

    def code_routines(self, side, db):
        """Every function, procedure and trigger written in the database,
        with the statement that makes each as the server keeps it."""
        from ..storedcode.routines import Param, Routine
        kinds = {"FN": "function", "IF": "function", "TF": "function",
                 "P": "procedure", "TR": "trigger"}
        found = self._rows(
            side, db, "select o.object_id, o.name, o.type, m.definition,"
                      " s.name from sys.objects o join sys.sql_modules m on"
                      " m.object_id = o.object_id join sys.schemas s on"
                      " s.schema_id = o.schema_id where o.is_ms_shipped = 0"
                      " and o.type in ('FN', 'IF', 'TF', 'P', 'TR')"
                      " order by o.name")
        params = {}
        for oid, pname, declared, pid, out in self._rows(
                side, db, f"select p.object_id, p.name,"
                          f" {self.DECLARED.format(c='p')}, p.parameter_id,"
                          " p.is_output from sys.parameters p join"
                          " sys.objects o on o.object_id = p.object_id where"
                          " o.is_ms_shipped = 0 order by p.object_id,"
                          " p.parameter_id"):
            params.setdefault(oid, []).append((str(pname).lstrip("@"),
                                               str(declared), int(pid),
                                               bool(out)))
        triggers = {}
        for oid, table, instead, event in self._rows(
                side, db, "select t.object_id, s.name + '.' +"
                          " object_name(t.parent_id), t.is_instead_of_trigger,"
                          " e.type_desc from sys.triggers t join"
                          " sys.trigger_events e on e.object_id = t.object_id"
                          " join sys.objects o on o.object_id = t.parent_id"
                          " join sys.schemas s on s.schema_id = o.schema_id"
                          " where t.parent_class = 1"):
            one = triggers.setdefault(oid, [str(table), bool(instead), []])
            one[2].append(str(event).upper())
        out = []
        for oid, name, kind, definition, schema in found:
            k = kinds[str(kind).strip()]
            mine = params.get(oid, [])
            returns = next((t for n, t, pid, _ in mine if pid == 0), None)
            r = Routine(kind=k, name=str(name), definition=definition,
                        language="tsql",
                        params=[Param(n, t, "inout" if o else "in")
                                for n, t, pid, o in mine if pid > 0],
                        returns=returns if k == "function" else None,
                        returns_set=str(kind).strip() in ("IF", "TF"),
                        settings={"schema": str(schema)})
            if str(kind).strip() in ("IF", "TF"):
                r.returns = "TABLE"
            if k == "trigger":
                table, instead, events = triggers.get(oid, [None, False,
                                                            []])
                r.table, r.events = table, tuple(events)
                r.timing = "INSTEAD OF" if instead else "AFTER"
                r.level = "statement"
                if table and self.hop.excluded(db, *table.split(".", 1)):
                    continue
            try:
                from ..storedcode.tsql_pg import body_of
                r.body = body_of(definition or "", k)[0]
            except Exception:  # noqa: BLE001 - read from its definition
                r.body = definition
            out.append(r)
        return out

    def table_ddl(self, side, db, table):
        """The statements that make this table as it is - each column's
        type, collation, identity, default and NULL rule, its key and its
        unique and check constraints - for a copy on another SQL Server
        (the proof's sandbox). Foreign keys are `foreign_keys`'."""
        quoted = self._qualified(side, db, table)
        sch = str(table).split(".", 1)[0] if "." in str(table) else "dbo"
        cols = []
        for name, declared, nullable, collation, ident, seed, step, dflt, \
                computed in self._rows(
                    side, db, f"select c.name, {self.DECLARED.format(c='c')},"
                              " c.is_nullable, c.collation_name,"
                              " c.is_identity, cast(ic.seed_value as"
                              " decimal(38, 0)), cast(ic.increment_value as"
                              " decimal(38, 0)), d.definition,"
                              " cc.definition from sys.columns c left join"
                              " sys.identity_columns ic on ic.object_id ="
                              " c.object_id and ic.column_id = c.column_id"
                              " left join sys.default_constraints d on"
                              " d.parent_object_id = c.object_id and"
                              " d.parent_column_id = c.column_id left join"
                              " sys.computed_columns cc on cc.object_id ="
                              " c.object_id and cc.column_id = c.column_id"
                              f" where c.object_id = object_id({MARK})"
                              " order by c.column_id", (quoted,)):
            if computed:
                cols.append(f"{self._q(name)} as {computed}")
                continue
            one = f"{self._q(name)} {declared}"
            if collation:
                one += f" collate {collation}"
            if ident:
                one += f" identity({seed}, {step})"
            one += " null" if nullable else " not null"
            if dflt:
                one += f" default {dflt}"
            cols.append(one)
        keys = {}
        for kname, kind, col in self._rows(
                side, db, "select i.name, case when i.is_primary_key = 1"
                          " then 'primary key' else 'unique' end, c.name"
                          " from sys.indexes i join sys.index_columns ic on"
                          " ic.object_id = i.object_id and ic.index_id ="
                          " i.index_id join sys.columns c on c.object_id ="
                          " ic.object_id and c.column_id = ic.column_id"
                          f" where i.object_id = object_id({MARK}) and"
                          " (i.is_primary_key = 1 or i.is_unique_constraint"
                          " = 1) order by i.index_id, ic.key_ordinal",
                (quoted,)):
            keys.setdefault((kname, kind), []).append(self._q(col))
        for (_, kind), kcols in keys.items():
            cols.append(f"{kind} ({', '.join(kcols)})")
        for (check,) in self._rows(
                side, db, "select definition from sys.check_constraints"
                          f" where parent_object_id = object_id({MARK})",
                (quoted,)):
            cols.append(f"check {check}")
        return (f"if schema_id('{sch}') is null exec('create schema"
                f" {self._q(sch)}');\n"
                f"create table {self._create_name(side, db, table)}"
                f" ({', '.join(cols)})")

    def foreign_keys(self, side, db):
        got = {}
        for fk, child, col, parent, pcol in self._rows(
                side, db, "select f.name, s.name + '.' + object_name("
                          "f.parent_object_id), cp.name, rs.name + '.' +"
                          " object_name(f.referenced_object_id), cr.name"
                          " from sys.foreign_keys f join"
                          " sys.foreign_key_columns k on"
                          " k.constraint_object_id = f.object_id join"
                          " sys.columns cp on cp.object_id ="
                          " k.parent_object_id and cp.column_id ="
                          " k.parent_column_id join sys.columns cr on"
                          " cr.object_id = k.referenced_object_id and"
                          " cr.column_id = k.referenced_column_id join"
                          " sys.objects o on o.object_id ="
                          " f.parent_object_id join sys.schemas s on"
                          " s.schema_id = o.schema_id join sys.objects ro on"
                          " ro.object_id = f.referenced_object_id join"
                          " sys.schemas rs on rs.schema_id = ro.schema_id"
                          " order by f.name, k.constraint_column_id"):
            one = got.setdefault((fk, child, parent), ([], []))
            one[0].append(str(col))
            one[1].append(str(pcol))
        return [(str(c), a, str(p), b) for (_, c, p), (a, b) in got.items()]

    def unique_keys(self, side, db):
        got = {}
        for table, index, col in self._rows(
                side, db, "select s.name + '.' + t.name, i.name, c.name from"
                          " sys.indexes i join sys.tables t on t.object_id ="
                          " i.object_id join sys.schemas s on s.schema_id ="
                          " t.schema_id join sys.index_columns ic on"
                          " ic.object_id = i.object_id and ic.index_id ="
                          " i.index_id join sys.columns c on c.object_id ="
                          " ic.object_id and c.column_id = ic.column_id"
                          " where i.is_unique = 1 order by 1,"
                          " i.is_primary_key desc, i.name, ic.key_ordinal"):
            got.setdefault(str(table), {}).setdefault(str(index), []).append(
                str(col))
        return {t: list(k.values()) for t, k in got.items()}

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

    # ---- rows in through SQL Server's own bulk load ------------------------
    #
    # The TDS bulk load, through the driver Microsoft publishes for Python:
    # rows - or Arrow batches, where a SQL Server source is read without its
    # values becoming Python objects - streamed into the load's packets by
    # the driver's compiled core. The load commits on a connection of its
    # own, which no transaction of migkit's can undo, so it never loads the
    # table itself: it loads a stage (`_Stage`), and the stage is promoted
    # into the table in one transaction of migkit's - the rows with the
    # batch's keys deleted, the stage inserted, the stage emptied, committed
    # together. A batch lands whole or not at all, and run again it finds
    # its keys and replaces them. The promote is an insert like any other:
    # the table's constraints are checked and stay trusted, its triggers
    # fire, which a load straight into the table would skip.

    #: what a batch costs each way into SQL Server: (seconds each time,
    #: seconds a row), which `write_way` starts from until each way is
    #: timed here. Measured on SQL Edge beside migkit (a 2-core VM),
    #: seven columns a row (bigint key, nvarchar, decimal, datetime2(7),
    #: varchar, varbinary, uniqueidentifier): 500,000 rows in batches of
    #: 50,000 took 269.6s by statements (1,855 rows/s) and 7.55s through
    #: the bulk load, staged and promoted (66,196 rows/s, 36 times as
    #: fast); read from SQL Server as Arrow and loaded as read
    #: (`native_bulk`), 6.97s (71,696 rows/s). A batch of 1, 100 and 1,000
    #: rows took 0.019s, 0.069s and 0.562s by statements and 0.051s,
    #: 0.048s and 0.067s through the load (a connection of its own and a
    #: stage made each time): statements are cheaper up to about 57 rows.
    #: The full 2022 server, x86 under Rosetta on 4 cores: 305.3s by
    #: statements (1,638 rows/s), 5.83s through the load (85,750 rows/s),
    #: 3.58s read as Arrow and loaded as read (139,613 rows/s). Measured
    #: before a seven-digit time crossed the load as text (`_as_text`),
    #: which that table's datetime2(7) now does; `bench/sql_server_write_
    #: ways.py` produces them again.
    WRITE_COSTS = {"statements": (0.018, 0.00054),
                   "bulk load": (0.048, 0.000015)}

    #: the declared types the bulk load carries as the server holds them,
    #: from the values the table copier reads: each column of a table of
    #: every type loaded into a stage and held to the source by its bytes
    #: (`cast(... as varbinary(max))`) and by the server's rendering of the
    #: row (`FOR JSON`), on SQL Edge. Refused: `datetimeoffset` (the load
    #: stops, `returned invalid data for column`), and a char column of a
    #: UTF-8 collation (`String length (30 characters) exceeds schema size
    #: (20 characters)` for 4 characters in a varchar(20)). `datetime2(7)`
    #: and `time(7)` lost their seventh digit to the load's own conversion
    #: of a value, so they cross as the server's text of it (`_as_text`).
    #: A column of any other type is written by statements.
    BULK_TYPES = frozenset({
        "bigint", "int", "smallint", "tinyint", "bit", "decimal", "numeric",
        "money", "smallmoney", "float", "real", "date", "datetime",
        "smalldatetime", "datetime2", "time", "char", "varchar", "text",
        "nchar", "nvarchar", "ntext", "xml", "binary", "varbinary", "image",
        "uniqueidentifier"})
    #: read from a SQL Server source as Arrow and loaded as it was read
    #: (`native_bulk`): the same types, measured the same way, but for two
    #: whose Arrow form is not what the server holds - `datetime2(7)` comes
    #: back as microseconds, the seventh digit gone, and `datetimeoffset` at
    #: UTC with its offset gone. Those are read as the text the server
    #: writes them in (style 121, its ODBC canonical form) into a text
    #: column of the stage, and turned back by the server in the promote -
    #: measured exact to the byte: {declared type: (the read, the stage's
    #: type, the turn back)}. A table with a column of any other type is
    #: copied table by table.
    ARROW_TYPES = BULK_TYPES
    CARRIED = {
        "datetime2(7)": ("convert(varchar(27), {}, 121)", "varchar(27)",
                         "convert(datetime2(7), {}, 121)"),
        "datetimeoffset": ("convert(varchar(34), {}, 121)", "varchar(34)",
                           "convert(datetimeoffset(7), {}, 121)"),
    }

    #: the ways a batch goes in, chosen per table and size of batch by
    #: what each cost (`Engine._cheaper`), starting from `WRITE_COSTS`
    WRITE_WAYS = (
        decide.Rung("bulk load", "SQL Server's bulk load, staged and"
                                 " promoted",
                    # a stage in tempdb while the batch is loaded
                    footprint=1),
        decide.Rung("statements", "statements"),
    )
    #: the ways a table goes from one SQL Server to another in a bulk move
    #: (`native_bulk`), in the order measured fastest
    NATIVE_RUNGS = (
        decide.Rung(
            "arrow", "read as Arrow and loaded by SQL Server's bulk load",
            gives=frozenset({"carried", "keyless"}),
            needs=(decide.Need("its driver", lambda f: f["driver"] is None,
                               lambda f: f["driver"]),
                   decide.Need("the table on the target",
                               lambda f: f["there"],
                               "the target has no such table yet, and the"
                               " copier makes it"),
                   decide.Need("types it is proved on",
                               lambda f: f["unproved"] is None,
                               lambda f: f["unproved"])),
            footprint=1),
        decide.Rung("copier", "table by table",
                    gives=frozenset({"carried", "keyless"})),
    )

    @staticmethod
    def _bulk_unusable():
        """Why the bulk load's driver cannot be used on this machine, or
        None. It is a dependency, and still a compiled one: a wheel built
        for a newer C library than the machine's installs and then fails
        to load."""
        try:
            import mssql_py_core  # noqa: F401
            import mssql_python  # noqa: F401
        except ImportError:
            return "the bulk load's driver does not load on this machine"
        return None

    def _bulk_connect(self, side, db):
        """A connection through the bulk load's driver, encrypted as the
        endpoint's `encrypt` says (`Endpoint.mssql_tls`: `off`, `request`,
        and `require`, which is also what it is where nothing is said -
        this driver encrypts unless told not to). The server's certificate
        is trusted as it is, as the checks' client trusts it (`-C`) and as
        `assess` reports for every SQL Server leg: the driver refuses one
        the server made for itself."""
        import mssql_python
        ep = self.hop.source if side == "src" else self.hop.target
        encrypt = {"off": "no", "request": "optional"}.get(
            ep.mssql_tls().get("encryption"), "yes")

        def braced(v):
            return "{" + str(v).replace("}", "}}") + "}"
        return mssql_python.connect(
            f"Server={ep.host},{ep.port or 1433};"
            f"Database={braced(self._d(side, db))};UID={braced(ep.user)};"
            f"PWD={braced(ep.password)};Encrypt={encrypt};"
            "TrustServerCertificate=yes", autocommit=False, timeout=15)

    def _shape(self, side, db, table):
        """{"declared": {column: declared type}, "key": [column],
        "identity": column or None, "unwritable": {column}, "utf8":
        {column}} - a computed column or a row version takes no value; a
        char column of a UTF-8 collation is one the bulk load refuses.
        Asked once a table."""
        shapes = self.__dict__.setdefault("_shapes", {})
        at = (side, db, str(table))
        if at not in shapes:
            quoted = self._qualified(side, db, table)
            flags = self._rows(
                side, db, "select c.name, c.is_identity, c.is_computed,"
                          " type_name(c.system_type_id), c.collation_name"
                          " from sys.columns c where c.object_id ="
                          f" object_id({MARK})", (quoted,))
            declared = dict(self.neutral_columns(side, db, table))
            if not declared:
                # not there yet: asked again once the copier has made it
                return {"declared": {}, "key": [], "identity": None,
                        "unwritable": set(), "utf8": set()}
            shapes[at] = {
                "declared": declared,
                "key": self.neutral_key(side, db, table),
                "identity": next((f[0] for f in flags if f[1]), None),
                "unwritable": {f[0] for f in flags
                               if f[2] or f[3] == "timestamp"},
                "utf8": {f[0] for f in flags
                         if "_utf8" in str(f[4] or "").lower()}}
        return shapes[at]

    def _unproved(self, shape, names, arrow=False):
        """Why the bulk load is not used for these columns of a table of
        `shape` - a type it is not proved on (`BULK_TYPES`, or read as
        Arrow `ARROW_TYPES` and `CARRIED`), or a UTF-8 collation - or
        None."""
        def proved(n):
            declared = str(shape["declared"].get(n, "")).lower()
            if n in shape["utf8"]:
                return False
            if arrow and declared in self.CARRIED:
                return True
            return self._base(declared) in (self.ARROW_TYPES if arrow
                                             else self.BULK_TYPES)
        odd = sorted(n for n in names if not proved(n))
        if not odd:
            return None
        return (f"{', '.join(odd[:4])} " + ("is" if len(odd) == 1 else "are")
                + " of a type the bulk load is not proved on")

    @staticmethod
    def _base(declared):
        return str(declared or "").split("(")[0].strip().lower()

    def write_way(self, side, db, table, columns, rows):
        """("statements" | "bulk load", why) for a batch of `rows` rows
        into `table`: the bulk load only where its driver loads here, it
        has not refused a value of this table before and every column
        written is of a type it is proved on - and then whichever way did
        a row in less time for batches of this size (`Engine._cheaper`),
        from what each was measured to cost (`WRITE_COSTS`) until each is
        timed here."""
        why = (self._bulk_unusable()
               or self.__dict__.get("_refused", {}).get((side, db,
                                                         str(table)))
               or self._unproved(self._shape(side, db, table),
                                 [n for n, _ in columns]))
        if why:
            return "statements", why
        key = self._work_key("rows", str(table), rows)
        way = self._cheaper(key, self.WRITE_WAYS, {
            w: (fixed + rows * row) / max(rows, 1)
            for w, (fixed, row) in self.WRITE_COSTS.items()})
        return way, (f"the way a row cost less for batches of about"
                     f" {key[2]:,} rows")

    def neutral_write(self, side, db, table, columns, rows):
        """Rows in by statements or through the bulk load, whichever
        `write_way` finds cheaper for this batch. Either replaces the rows
        with the same key in one transaction, so a batch written again
        converges.

        A value the bulk load will not take into its stage - one another
        engine hands over in a form it does not convert - has touched
        nothing but the stage: the batch goes by statements, and so does
        the rest of the table. What the promote refuses, the table itself
        refuses, and is said as it would be by statements."""
        if not rows:
            return 0
        self._target_only(side, "write rows")
        way, _ = self.write_way(side, db, table, columns, len(rows))
        began = time.monotonic()
        if way == "bulk load":
            from .. import canon
            names = [n for n, _ in columns]
            texts = self._as_text(side, db, table, names)
            try:
                staged = canon.sql_rows([c for _, c in columns], rows,
                                        engine="mssql")
                if texts:
                    at = [(i, texts[n][0]) for i, n in enumerate(names)
                          if n in texts]
                    staged = [list(r) for r in staged]
                    for r in staged:
                        for i, text in at:
                            r[i] = text(r[i])
                with _Stage(self, side, db, table, names,
                            {n: t[1:] for n, t in texts.items()}) as stage:
                    stage.load(staged)
                    stage.promote()
            except _NotLoaded as e:
                self.__dict__.setdefault("_refused", {})[
                    (side, db, str(table))] = (
                    f"the bulk load refused a value of it: {e}")
                way, began = "statements", time.monotonic()
        if way == "statements":
            super().neutral_write(side, db, table, columns, rows)
        key = self._work_key("rows", str(table), len(rows))
        if key in self.__dict__.get("_way_rungs", {}):
            # timed where the way was chosen by what it cost
            self._took(key, way, time.monotonic() - began, len(rows))
        return len(rows)

    def _as_text(self, side, db, table, names):
        """{column: (value to text, the stage's type, the turn back)} for
        the columns of more than six digits after the second: a value
        handed over keeps its seventh digit (`nanotime`), which the bulk
        load's own conversion of a datetime would drop - so it crosses as
        the server's text of it and the server turns it back in the
        promote, as `CARRIED` does for a read."""
        declared = self._shape(side, db, table)["declared"]
        out = {}
        for n in names:
            d = str(declared.get(n, "")).lower()
            if not self._nanos_text(self._q(n), d):
                continue
            if self._base(d) == "datetime2":
                out[n] = (_time_text, "varchar(27)",
                          f"convert({d}, {{}}, 121)")
            else:
                out[n] = (_time_text, "varchar(16)", f"convert({d}, {{}})")
        return out

    def neutral_rewrite(self, side, db, table, columns, rows):
        """Written again by statements: a batch that read back different
        is not given to the bulk load a second time."""
        return super().neutral_write(side, db, table, columns, rows)

    #: what one staged batch of a table copied by `native_bulk` holds at
    #: most - rows, and bytes as Arrow holds them: one promote, so one
    #: transaction on the target, carries that much
    NATIVE_ROWS = 500_000
    NATIVE_BYTES = 64 * 2 ** 20

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table read from the source as Arrow batches and loaded
        through the bulk load, a batch staged and promoted at a time - no
        value becomes a Python object on the way. A table the target lacks,
        or with a column of a type the Arrow read is not proved on, is
        copied table by table instead, where its batches still go by
        whichever way costs less. A table a row filter keeps for the table
        copier (`shape_only`) is made by the copier, the way it makes the
        rest here. Measured: 500,000 rows in 6.97s on SQL Edge and 3.58s
        on the 2022 server, against 269.6s and 305.3s by statements
        (`WRITE_COSTS`)."""
        steps = [f"{t}: read in batches and loaded by SQL Server's bulk load,"
                 " each batch staged and promoted in one transaction"
                 for t in tables]
        if not go or not tables:
            return steps
        pair = None
        for t in tables:
            got = decide.climb(t, self.NATIVE_RUNGS, self._native_facts(db, t))
            why = got.reason
            if got.path == "arrow":
                try:
                    self._native_table(db, t, log)
                    continue
                except _NotLoaded as e:
                    why = f"the bulk load refused a value of it: {e}"
            if log:
                log(f"{t}: copied table by table - {why}")
            if pair is None:
                pair = self._as_pair()
            sch, _, tbl = str(t).rpartition(".")
            pair.move_table(db, sch, tbl, pair.READ_ROWS, _Unkept(), log
                            or (lambda m: None))
        return steps

    def _native_facts(self, db, table):
        """What `NATIVE_RUNGS` read for one table, each asked only when a
        need gets to it."""
        def unproved():
            source, target = (self._shape("src", db, table),
                              self._shape("dst", db, table))
            names = [n for n in source["declared"]
                     if n in target["declared"]
                     and n not in target["unwritable"]]
            return (self._unproved(source, names, arrow=True)
                    or self._unproved(target, names, arrow=True))
        return decide.Facts(
            driver=self._bulk_unusable,
            there=lambda: str(table).lower() in {
                t.lower() for t in self.neutral_tables("dst", db)},
            unproved=unproved)

    def _native_table(self, db, table, log):
        """One table, emptied on the target and filled from the source's
        read as it arrives: Arrow batches of up to `NATIVE_ROWS` rows or
        `NATIVE_BYTES`, each staged and promoted. In the source's key
        order, which is the target's too where both keep the key the same
        way."""
        import pyarrow as pa
        target = self._shape("dst", db, table)
        source = self._shape("src", db, table)
        names = [n for n in source["declared"]
                 if n in target["declared"] and n not in target["unwritable"]]
        carried = {n: self.CARRIED[str(source["declared"][n]).lower()]
                   for n in names
                   if str(source["declared"][n]).lower() in self.CARRIED}
        key = source["key"]
        order = (" order by " + ", ".join(self._q(k) for k in key)
                 if key and all(k in names for k in key) else "")
        began, moved = time.monotonic(), 0
        gone = self.neutral_empty("dst", db, table)
        if gone and log:
            log(f"{table}: emptied {gone:,} rows the target held before the"
                " copy")
        src = self._bulk_connect("src", db)
        try:
            cur = src.cursor()
            cur.execute("select " + ", ".join(
                f"{carried[n][0].format(self._q(n))} as {self._q(n)}"
                if n in carried else self._q(n) for n in names)
                + f" from {self._qualified('src', db, table)}{order}")
            with _Stage(self, "dst", db, table, names,
                        {n: c[1:] for n, c in carried.items()}) as stage:
                held, rows, size = [], 0, 0
                for batch in cur.arrow_reader(batch_size=10_000):
                    held.append(batch)
                    rows += batch.num_rows
                    size += batch.nbytes
                    if rows >= self.NATIVE_ROWS or size >= self.NATIVE_BYTES:
                        stage.load_arrow(pa.Table.from_batches(held))
                        stage.promote()
                        moved += rows
                        held, rows, size = [], 0, 0
                        if log:
                            log(f"{table}: {moved:,} rows")
                if held:
                    stage.load_arrow(pa.Table.from_batches(held))
                    stage.promote()
                    moved += rows
        finally:
            src.close()
        took = time.monotonic() - began
        if log:
            log(f"{table}: copied, {moved:,} rows in {took:.1f}s")
        return moved

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
            from .. import sourcechange as sc
            raise SystemExit(
                f"Change Tracking is off for {db}, and it is the change log"
                " migkit reads here - "
                + sc.requirement("change_tracking", "off", sc.Ctx(db=db))
                + ". Then each table: alter table ... enable change_tracking")
        self._all_tracked(side, db)
        return int(at)

    def _all_tracked(self, side, db):
        """Stops where a table the hop moves is not tracked - asked with
        every read, since a table can be made after the tail starts."""
        missed = sorted(set(self.neutral_tables(side, db))
                        - self._tracked(side, db))
        if missed:
            from .. import sourcechange as sc
            tables = [tuple(t.split(".", 1)) if "." in t else ("dbo", t)
                      for t in missed[:6]]
            raise SystemExit(
                f"Change Tracking does not follow {', '.join(missed[:6])}"
                + (f" and {len(missed) - 6} more" if len(missed) > 6 else "")
                + ", so their changes would be carried by nothing - "
                + sc.requirement("table_tracking",
                                 f"{len(missed)} not tracked",
                                 sc.Ctx(db=db, tables=tables)))

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

    def server_identity(self, side, db):
        """`@@SERVERNAME` with the database's creation (`stream_identity`),
        the machine it runs on, its version, the database's size, and -
        where this login may read the server's state - when it started."""
        name, version, size = self._rows(
            side, db, "select cast(serverproperty('MachineName') as"
                      " nvarchar(128)), cast(serverproperty('ProductVersion')"
                      " as nvarchar(128)), (select sum(cast(size as bigint))"
                      " * 8192 from sys.database_files)")[0]
        out = {"name": str(name or ""), "version": str(version or ""),
               "bytes": int(size or 0)}
        try:
            out["running"] = str(self._rows(
                side, db, "select sqlserver_start_time from"
                          " sys.dm_os_sys_info")[0][0])
        except Exception:  # noqa: BLE001 - VIEW SERVER STATE is not given
            pass
        got = self.stream_identity(side, db)
        if got:
            out["id"] = got
        return out

    def position_lost(self, side, db, token):
        """A table whose tracking no longer reaches back to `token`: what
        changed there before its oldest kept version is gone, and a tail
        from `token` would skip it."""
        if token is None:
            return None
        conn = self._connect(side, db)
        try:
            return self._lost_since(conn.cursor(), token)
        finally:
            conn.close()

    #: each tracked table and the oldest version its tracking still keeps
    _MIN_VALID = ("select s.name + '.' + t.name,"
                  " change_tracking_min_valid_version(t.object_id) from"
                  " sys.change_tracking_tables c join sys.tables t on"
                  " t.object_id = c.object_id join sys.schemas s on"
                  " s.schema_id = t.schema_id")

    def _lost_since(self, cur, token):
        """`position_lost`, asked on a cursor already open - inside the
        transaction that reads the changes, where it has to be asked."""
        self._run(cur, self._MIN_VALID)
        gone = [t for t, low in cur.fetchall()
                if low is not None and int(low) > int(token)]
        if not gone:
            return None
        return (f"Change Tracking no longer keeps the changes of"
                f" {', '.join(sorted(gone)[:6])} from version {token}: its"
                " retention has cleaned them up")

    def _ct_read(self, side, db, token, read):
        """(`read(cursor)`, the version to read from next time): what
        Change Tracking holds since `token`, read the way Microsoft
        documents it has to be.

        The version to go on from is taken first, so a change made while
        the rows are read is read again next time rather than skipped.
        Then, in one transaction - SNAPSHOT where the database allows it,
        so every table is read as of one moment and a cleanup running
        meanwhile cannot take rows out from under the read - each table's
        oldest kept version is checked against `token`, the changes are
        read, and the check is made again. The check before and after is
        what an unguarded read lacked: the retention's cleanup, running
        between a check and the read, removed the changes the check had
        just said were there, the read returned fewer rows and no error,
        and the tail went on past what it never received. Measured on SQL
        Edge with the tracking of a table cleared between the check and
        the read: an insert made before it was not among the changes, and
        nothing said so. Either check failing stops, named.
        """
        conn = self._connect(side, db)
        try:
            conn.autocommit(True)
            cur = conn.cursor()
            self._run(cur, "select change_tracking_current_version(),"
                           " snapshot_isolation_state from sys.databases"
                           " where database_id = db_id()")
            now, snap = cur.fetchone()
            if snap == 1:
                self._run(cur, "set transaction isolation level snapshot")
            self._run(cur, "begin transaction")
            try:
                lost = self._lost_since(cur, token)
                got = None if lost else read(cur)
                lost = lost or self._lost_since(cur, token)
            except Exception:
                # the read failed: a table whose tracking was taken away
                # fails a snapshot read - said as what it is, if so
                try:
                    self._run(cur, "if @@trancount > 0 rollback")
                    lost = self._lost_since(cur, token)
                except Exception:  # noqa: BLE001 - the first error stands
                    lost = None
                if lost:
                    raise SystemExit(lost)
                raise
            self._run(cur, "commit")
            if lost:
                raise SystemExit(lost)
            return got, int(now) if now is not None else None
        finally:
            conn.close()

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each row changed since `token`, as it is now: a delete where it
        is gone. Read through `_ct_read`, which stops rather than hand on
        a read the retention has cut short."""
        from .. import canon
        if token is None:
            return [], self.change_point(side, db)
        self._all_tracked(side, db)
        plans = []
        for t in sorted(self._tracked(side, db)):
            if self.hop.excluded(db, *t.split(".", 1)):
                continue
            key = self.neutral_key(side, db, t)
            cols = [n for n, _ in self.neutral_columns(side, db, t)]
            quoted = self._qualified(side, db, t)
            plans.append((t, key, cols,
                "select ct.sys_change_version, ct.sys_change_operation, "
                + ", ".join(f"ct.{self._q(k)}" for k in key) + ", "
                + ", ".join(f"r.{self._q(c)}" for c in cols)
                + f", case when r.{self._q(key[0])} is null then 0 else 1"
                f" end from changetable(changes {quoted}, {MARK}) ct"
                f" left join {quoted} r on "
                + " and ".join(f"r.{self._q(k)} = ct.{self._q(k)}"
                               for k in key)
                + " order by ct.sys_change_version"))

        def read(cur):
            out = []
            for t, key, cols, sql in plans:
                self._run(cur, sql, (int(token),))
                out.append((t, key, cols, cur.fetchall()))
            return out
        got, now = self._ct_read(side, db, token, read)
        found = []
        for t, key, cols, rows in got:
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


class _Stage:
    """Where a bulk load lands before it is promoted: a global temporary
    table in tempdb, shaped as the columns written. `select top 0 ... into`,
    joined to itself by `union all`, takes each column's type, length and
    collation from the table and leaves its identity, defaults and
    constraints behind - the stage holds values, it makes none. Global, so
    the load's own connection can write it; named for itself, so tables
    and ranges loaded side by side never share one; dropped when done with,
    and by the server with the session that made it where migkit is killed
    first. Nothing is left in the target's database."""

    #: seconds one load may take; a stalled server is an error, not a hang
    TIMEOUT = 1800

    def __init__(self, eng, side, db, table, names, carried=None):
        import uuid
        shape = eng._shape(side, db, table)
        self.q = eng._q
        self.names = list(names)
        #: {column: (the stage's type for it, what turns it back)} for a
        #: column loaded as another type than the table's (`CARRIED`)
        self.carried = dict(carried or {})
        self.target = eng._qualified(side, db, table)
        self.name = "##migkit_stage_" + uuid.uuid4().hex
        self.identity = shape["identity"] in self.names
        self.key = (shape["key"] if shape["key"]
                    and all(k in self.names for k in shape["key"]) else [])
        self.conn = eng._bulk_connect(side, db)
        try:
            cols = ", ".join(
                f"cast(null as {self.carried[n][0]}) as {self.q(n)}"
                if n in self.carried else self.q(n) for n in self.names)
            self.conn.cursor().execute(
                f"select top 0 {cols} into {self.name} from {self.target}"
                f" union all select top 0 {cols} from {self.target}")
            # committed before the load looks for it: uncommitted, the
            # load's connection waits on this session's lock for good
            self.conn.commit()
        except BaseException:
            self.conn.close()
            raise

    def _back(self, name, alias=""):
        """A stage column as the table's value."""
        at = f"{alias}{self.q(name)}"
        return self.carried[name][1].format(at) if name in self.carried \
            else at

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        try:
            self.conn.rollback()
            self.conn.cursor().execute(f"drop table if exists {self.name}")
            self.conn.commit()
        except Exception:  # noqa: BLE001 - the session's end drops it too
            pass
        finally:
            self.conn.close()

    def _options(self):
        # the load's own defaults are the wrong way round for a copy: a
        # NULL would take the column's default, an identity would number
        # the rows itself. The stage has neither; this keeps it so should
        # it ever be made another way. Nobody else writes the stage, so it
        # is locked whole, which lets the load log least.
        return {"keep_nulls": True, "keep_identity": True,
                "table_lock": True, "timeout": self.TIMEOUT}

    def load(self, rows):
        """Rows as values, each a sequence in the order of `names`."""
        rows = [r if isinstance(r, tuple) else tuple(r) for r in rows]
        try:
            got = self.conn.cursor().bulkcopy(self.name, rows,
                                              **self._options())
        except Exception as e:
            raise _NotLoaded(_said(e)) from e
        self._landed(got, len(rows))

    def load_arrow(self, table):
        try:
            got = self.conn.cursor().bulkcopy_arrow(self.name, table,
                                                    **self._options())
        except Exception as e:
            raise _NotLoaded(_said(e)) from e
        self._landed(got, table.num_rows)

    def _landed(self, got, n):
        copied = (got or {}).get("rows_copied")
        if copied is None or int(copied) != n:
            raise _NotLoaded(f"it put {copied} of {n:,} rows into its stage")

    def promote(self):
        """The stage into the table, in one transaction: the rows with the
        stage's keys deleted, the stage inserted, the stage emptied. A table
        with no key is appended to, as a statement would append to it."""
        q = self.q
        cur = self.conn.cursor()
        try:
            cur.execute("set xact_abort on")
            if self.identity:
                cur.execute(f"set identity_insert {self.target} on")
            if self.key:
                cur.execute(f"delete t from {self.target} t where exists"
                            f" (select 1 from {self.name} s where "
                            + " and ".join(f"{self._back(k, 's.')} ="
                                           f" t.{q(k)}" for k in self.key)
                            + ")")
            cur.execute(f"insert into {self.target}"
                        f" ({', '.join(q(n) for n in self.names)}) select"
                        f" {', '.join(self._back(n) for n in self.names)}"
                        f" from {self.name}")
            cur.execute(f"truncate table {self.name}")
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        finally:
            if self.identity:
                try:
                    cur.execute(f"set identity_insert {self.target} off")
                except Exception:  # noqa: BLE001 - a lost session holds
                    pass           # nothing on, and the first error stands


class _NotLoaded(Exception):
    """The bulk load did not fill its stage: nothing reached the table."""


def _time_text(v):
    """A datetime or a time as SQL Server writes one of seven digits after
    the second (style 121), from the six a Python value holds and the
    digits past them it carries (`nanotime`), as the statements send it
    (`canon.mssql_time_text`). A value with a zone is not one a column
    without a zone is written from here."""
    import datetime

    from .. import canon
    if v is None or isinstance(v, str):
        return v
    if getattr(v, "tzinfo", None) is not None:
        raise _NotLoaded("a time with a zone, for a column without one")
    if not isinstance(v, (datetime.datetime, datetime.time)):
        raise _NotLoaded(f"a {type(v).__name__} for a time column")
    return canon.mssql_time_text(v)


def _said(e):
    """What the server or the driver said, on one line, without the
    driver's own framing and without its name or its methods'."""
    from ..wording import without_programs
    text = " ".join(str(e).split()).split("DDBC Error: ", 1)[-1]
    return without_programs(text, ("mssql_python", "mssql_py_core",
                                   "mssql-python", "bulkcopy_arrow()",
                                   "bulkcopy()", "bulkcopy"))[:240]


class _Unkept(dict):
    """A checkpoint for a table copied inside `native_bulk`, which starts
    the table over on a run again rather than resuming it."""

    def save(self):
        pass
