import time

from .. import canon, decide, pieces
from .base import Engine, RepairAction, Result, clauses, rows_counted
from .dbapi import MARK, DbapiRows

#: the server's own databases, and the one a managed server keeps
#: for its provider
SKIP_DBS = frozenset({"master", "tempdb", "model", "msdb", "rdsadmin"})


def plan_tables(xml):
    """{table: "index <name>" or None where it is read whole} of an
    estimated plan as `SHOWPLAN_XML` gives it: a scan of the table or of
    its clustered index reads it whole; a seek, or a scan of another
    index, reaches it through that index."""
    import xml.etree.ElementTree as ET
    out = {}
    for op in ET.fromstring(xml).iter():
        if not str(op.tag).endswith("}RelOp"):
            continue
        how = op.get("PhysicalOp", "")
        for child in op:
            if not str(child.tag).endswith(("}IndexScan", "}TableScan")):
                continue
            obj = next((o for o in child if str(o.tag).endswith("}Object")),
                       None)
            if obj is None:
                continue
            table = obj.get("Table", "").strip("[]")
            index = obj.get("Index", "").strip("[]")
            out[table] = (None if how in ("Table Scan", "Clustered Index Scan")
                          else f"index {index}" if index
                          else out.get(table))
    return out


def _without_parameters(text):
    """A statement the Query Store keeps with its parameters declared in
    front of it - `(@P1 int,@P2 nvarchar(50))select ...` - without that
    declaration; any other as it is."""
    if not text.startswith("(@"):
        return text
    depth = 0
    for i, ch in enumerate(text):
        depth += {"(": 1, ")": -1}.get(ch, 0)
        if depth == 0:
            return text[i + 1:].strip()
    return text


class MSSQLEngine(DbapiRows, Engine):
    checks = ("schema", "counts", "autoinc", "data")
    CHARSETS = True
    #: a change tracking version only says where a read begins: one of its
    #: own reads from an older one again, while the retention keeps it
    REREADS_FROM_A_POSITION = True

    def _kept(self, side, db):
        """The connection this thread keeps to one side's database: every
        catalogue and check statement goes through it, where each was a
        client process and a sign-in of its own (W15)."""
        import os
        import threading
        kept = self.__dict__.setdefault("_kept_conns", {})
        # a process's own: a child forked with its parent's would share
        # the parent's socket
        at = (side, self._d(side, db), os.getpid(), threading.get_ident())
        conn = kept.get(at)
        if conn is None:
            conn = self._connect(side, db)
            conn.autocommit(True)
            kept[at] = conn
        return conn

    def _forget(self, side, db):
        import os
        import threading
        at = (side, self._d(side, db), os.getpid(), threading.get_ident())
        conn = self.__dict__.get("_kept_conns", {}).pop(at, None)
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - it is gone either way
                pass

    @staticmethod
    def _cell(v):
        """A value as the rows of `_cmd` carry it: text, NULL as `NULL`,
        a flag as 1 or 0, bytes as the server writes a binary."""
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, (bytes, bytearray)):
            return "0x" + bytes(v).hex().upper()
        return str(v)

    def _cmd(self, side, db, sql):
        """Each row of `sql` as a list of texts (`_cell`), run on the
        side's kept connection. A statement the server refuses is an error
        naming the side, never rows: the client program this replaced
        exited 0 with `Msg 8134 ... Divide by zero` printed as a row, which
        both sides of a comparison can print alike; and it split every row
        at `|`, so a value holding one was read as several - the deep
        check's column comparison read a column's name and nothing of its
        definition."""
        return [[self._cell(v) for v in r]
                for r in self._kept_rows(side, db, sql)]

    def _kept_rows(self, side, db, sql, args=None):
        """The rows of `sql` as the driver gives them, `args` bound at its
        MARKs, on the side's kept connection; one that dropped is opened
        again once."""
        where = "source" if side == "src" else "target"
        for attempt in (0, 1):
            conn = self._kept(side, db)
            try:
                cur = conn.cursor()
                self._run(cur, sql, args)
                return [list(r) for r in cur.fetchall()] \
                    if cur.description else []
            except Exception as e:  # noqa: BLE001 - said, with the side
                said = " ".join(str(e).split())
                lost = any(w in said.lower() for w in (
                    "connection", "closed", "dbprocess is dead",
                    "not connected"))
                self._forget(side, db)
                if attempt == 0 and lost:
                    continue
                raise RuntimeError(f"the {where} refused a statement:"
                                   f" {said[-300:]}") from None

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        rows = self._cmd("src", "master",
                         "select name from sys.databases order by 1")
        return [r[0] for r in rows if r[0] not in SKIP_DBS
                and not self.hop.excluded(r[0])]

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

    #: the declared type as a column's whole definition takes it: DECLARED,
    #: with a `datetimeoffset`'s digits after the second too
    _SHAPE_DECLARED = ("case when type_name(c.system_type_id) ="
                       " 'datetimeoffset' then 'datetimeoffset(' +"
                       " cast(c.scale as varchar(2)) + ')' else {d} end")

    def _shape_reads(self):
        """The six statements `shape_catalog` reads a side's catalogue in,
        as (statement, its order, how many columns): every column named,
        and every column an order stands on selected as well - a column's
        place in its table, in its index, in its foreign key - so
        `shape_mark` digests exactly what is read, places included."""
        declared = self._SHAPE_DECLARED.format(d=self.DECLARED.format(c="c"))
        return (
            (f"select s.name + '.' + t.name c0, c.name c1, {declared} c2,"
             " c.is_nullable c3, c.collation_name c4, ic.seed_value c5,"
             " ic.increment_value c6, dc.definition c7, cc.definition c8,"
             " cc.is_persisted c9, c.is_sparse c10, c.is_rowguidcol c11,"
             " case when c.user_type_id <> c.system_type_id then"
             " type_name(c.user_type_id) end c12, c.column_id c13"
             " from sys.tables t join sys.schemas s on s.schema_id ="
             " t.schema_id join sys.columns c on c.object_id = t.object_id"
             " left join sys.identity_columns ic on ic.object_id ="
             " c.object_id and ic.column_id = c.column_id"
             " left join sys.default_constraints dc on"
             " dc.parent_object_id = c.object_id and dc.parent_column_id"
             " = c.column_id left join sys.computed_columns cc on"
             " cc.object_id = c.object_id and cc.column_id = c.column_id"
             " where t.is_ms_shipped = 0", "1, c.column_id", 14),
            ("select s.name + '.' + t.name c0, ic.index_id c1, c.name c2,"
             " ic.is_descending_key c3, ic.is_included_column c4,"
             " ic.key_ordinal c5 from"
             " sys.index_columns ic join sys.tables t on t.object_id ="
             " ic.object_id join sys.schemas s on s.schema_id ="
             " t.schema_id join sys.columns c on c.object_id ="
             " ic.object_id and c.column_id = ic.column_id where"
             " t.is_ms_shipped = 0", "1, 2, ic.is_included_column,"
             " ic.key_ordinal, c.name", 6),
            ("select s.name + '.' + t.name c0, i.index_id c1, i.name c2,"
             " coalesce(kc.is_system_named, 0) c3, lower(i.type_desc) c4,"
             " i.is_primary_key c5, i.is_unique_constraint c6,"
             " i.is_unique c7,"
             " case when i.has_filter = 1 then i.filter_definition end c8"
             " from sys.indexes i join sys.tables t on t.object_id ="
             " i.object_id join sys.schemas s on s.schema_id ="
             " t.schema_id left join sys.key_constraints kc on"
             " kc.parent_object_id = i.object_id and kc.unique_index_id"
             " = i.index_id where t.is_ms_shipped = 0 and i.type > 0",
             "", 9),
            ("select f.constraint_object_id c0, col_name(f.parent_object_id,"
             " f.parent_column_id) c1, col_name(f.referenced_object_id,"
             " f.referenced_column_id) c2, f.constraint_column_id c3"
             " from sys.foreign_key_columns f", "1, f.constraint_column_id",
             4),
            ("select s.name + '.' + t.name c0, fk.object_id c1, fk.name c2,"
             " fk.is_system_named c3, schema_name(rt.schema_id) + '.' +"
             " rt.name c4, lower(fk.delete_referential_action_desc) c5,"
             " lower(fk.update_referential_action_desc) c6"
             " from sys.foreign_keys fk join sys.tables t on"
             " t.object_id = fk.parent_object_id join sys.schemas s"
             " on s.schema_id = t.schema_id join sys.tables rt on"
             " rt.object_id = fk.referenced_object_id where"
             " t.is_ms_shipped = 0", "", 7),
            ("select s.name + '.' + t.name c0, 'check' c1, cc.name c2,"
             " cc.is_system_named c3, cc.definition c4, col_name("
             "cc.parent_object_id, cc.parent_column_id) c5"
             " from sys.check_constraints cc join sys.tables t on"
             " t.object_id = cc.parent_object_id join sys.schemas s on"
             " s.schema_id = t.schema_id where t.is_ms_shipped = 0"
             " union all select s.name + '.' + t.name, 'trigger',"
             " tr.name, 0, convert(varchar(64), hashbytes('SHA2_256',"
             " isnull(object_definition(tr.object_id), '')), 2),"
             " case when tr.is_instead_of_trigger = 1 then 'instead of'"
             " else 'after' end"
             " from sys.triggers tr join sys.tables t on t.object_id ="
             " tr.parent_id join sys.schemas s on s.schema_id ="
             " t.schema_id where t.is_ms_shipped = 0", "", 6))

    def shape_catalog(self, side, db):
        """The side's `catalog.Catalog`, read in six statements on the kept
        connection whatever the number of tables (`_shape_reads`): each
        column with its declared type and every number, nullability,
        collation, default and identity, computed expression or alias type
        (`extra`); each table's key, its indexes by their columns, order,
        included columns and filter, its foreign keys with their actions,
        and its checks and triggers (`others`). A constraint the server
        named itself is said by what it is, not by that name: the same key
        made in two databases is named differently in each
        (`PK__o__3213E83F720CC3A2` and `PK__o__3213E83FA19E373F`, measured
        on SQL Edge)."""
        from ..catalog import Catalog, Column, ForeignKey, Index
        reads = self._shape_reads()

        def read(n):
            sql, order, _ = reads[n]
            return self._kept_rows(side, db, sql + (f" order by {order}"
                                                    if order else ""))
        cat = Catalog()
        for (table, name, typ, null, coll, seed, step, default, computed,
             persisted, sparse, guid, alias, _place) in read(0):
            extra = " ".join(x for x in (
                f"identity({seed}, {step})" if seed is not None else "",
                (f"computed {computed}" + (" persisted" if persisted else ""))
                if computed is not None else "",
                "sparse" if sparse else "", "rowguidcol" if guid else "",
                f"type {alias}" if alias else "") if x)
            cat.columns.setdefault(str(table), []).append(
                Column(str(name), str(typ), "", bool(null), default, coll,
                       extra))
        cols = {}
        for table, index, name, desc, included, _place in read(1):
            got = cols.setdefault((str(table), index), ([], []))
            got[1 if included else 0].append(
                str(name) + (" desc" if desc and not included else ""))
        for (table, index, name, own, how, primary, constraint, unique,
             where) in read(2):
            table = str(table)
            keyed, inc = cols.get((table, index), ([], []))
            if primary:
                cat.keys[table] = tuple(k.removesuffix(" desc")
                                        for k in keyed)
                cat.others.setdefault(table, []).append(
                    f"primary key {how} ({', '.join(keyed)})")
                continue
            cat.indexes.setdefault(table, []).append(Index(
                "" if own else str(name), tuple(keyed), bool(unique),
                " ".join(x for x in (
                    str(how), "constraint" if constraint else "",
                    f"include ({', '.join(inc)})" if inc else "",
                    f"where {where}" if where else "") if x)))
        refs = {}
        for fk, name, ref, _place in read(3):
            got = refs.setdefault(fk, ([], []))
            got[0].append(str(name))
            got[1].append(str(ref))
        for table, fk, name, own, ref, on_delete, on_update in read(4):
            mine, theirs = refs.get(fk, ([], []))
            cat.fks.setdefault(str(table), []).append(ForeignKey(
                "" if own else str(name), tuple(mine), str(ref),
                tuple(theirs), str(on_delete).replace("_", " "),
                str(on_update).replace("_", " ")))
        for table, kind, name, own, a, b in read(5):
            if kind == "check":
                said = (f"check{'' if own else ' ' + str(name)}"
                        + (f" on {b}" if b else "") + f" {a}")
            else:
                said = f"trigger {name} {b} {str(a)[:16].lower()}"
            cat.others.setdefault(str(table), []).append(said)
        cat.tables = sorted(set(cat.columns) | set(cat.others)
                            | set(cat.indexes) | set(cat.fks))
        return cat

    def shape_mark(self, side, db):
        """One digest of everything `shape_catalog` reads, computed by the
        server over the same six statements (`_shape_reads`) in one round
        trip: each statement's rows counted, and each row - every value as
        its length and its text, a missing one apart - hashed on its own
        (SHA-256, its first 56 bits) and the hashes summed, so the digest
        holds however many rows there are and whatever order the server
        reads them in; the places a row stands at are among its values.
        The same catalogue gives the same digest, a change to what drift
        compares another - what a tail asks every batch before it reads
        the whole shape again (`hetero._shape_now`, audit SP8). A server
        that cannot hash a row this long refuses, and the shape is read
        whole."""
        def value(n):
            text = f"convert(nvarchar(max), q.c{n})"
            return (f"isnull(convert(nvarchar(20), datalength({text}))"
                    f" + N':' + {text}, N'-')")
        parts = []
        for sql, _, width in self._shape_reads():
            row = " + ".join(value(n) for n in range(width))
            parts.append(
                "(select convert(varchar(20), count_big(*)) + ':' +"
                " isnull(convert(varchar(48), sum(convert(decimal(38, 0),"
                " convert(bigint, substring(hashbytes('SHA2_256',"
                f" {row}), 1, 7))))), '0') from ({sql}) q)")
        got = self._kept_rows(side, db, "select " + ", ".join(parts))
        return "/".join(str(v) for v in got[0]) if got else None

    def column_catalog(self, side, db):
        """`shape_catalog` as the (column, definition) pairs a shape is
        kept in (`drift.pairs`) - what the cutover's schema gate reads."""
        from .. import drift
        return drift.pairs(self.shape_catalog(side, db))

    def _objects(self, side, db):
        """{(schema.name, kind): definition hash} of the views, routines,
        sequences and synonyms - each by the text the server keeps of it; a
        sequence by its type and numbers, a synonym by what it names. A
        table, its keys, indexes, constraints and triggers are compared by
        their whole definitions instead (`shape_catalog`)."""
        rows = self._cmd(side, db,
                         "select s.name + '.' + o.name, o.type_desc,"
                         " convert(varchar(64), hashbytes('SHA2_256',"
                         " coalesce(object_definition(o.object_id),"
                         " (select type_name(q.user_type_id) + ' '"
                         " + convert(nvarchar(64), q.start_value) + ' '"
                         " + convert(nvarchar(64), q.increment) + ' '"
                         " + convert(nvarchar(64), q.minimum_value) + ' '"
                         " + convert(nvarchar(64), q.maximum_value) + ' '"
                         " + convert(nvarchar(4), q.is_cycling) + ' '"
                         " + convert(nvarchar(16), isnull(q.cache_size, 0))"
                         " from sys.sequences q where q.object_id ="
                         " o.object_id),"
                         " (select y.base_object_name from sys.synonyms y"
                         " where y.object_id = o.object_id), '')), 2),"
                         " s.name, o.name"
                         " from sys.objects o join sys.schemas s on"
                         " s.schema_id = o.schema_id where o.is_ms_shipped = 0"
                         " and o.type in ('V', 'P', 'FN', 'TF', 'IF', 'SO',"
                         " 'SN') order by 1, 2")
        return {(r[0], r[1]): r[2] for r in rows
                if not self.hop.excluded(db, r[3], r[4])}

    def check_schema(self, db):
        """Every table compared whole on both sides, by the one comparer
        (`catalog.Catalog.explain`): its columns by their declared types
        with length, precision and scale - a narrower target column named
        with why (`verdict.narrowing`) - nullability, collation, default,
        identity or expression; its key, its indexes by their columns, its
        foreign keys, checks and triggers (`shape_catalog`). Views,
        routines, sequences and synonyms by their definitions. It compared
        a hash of `object_definition`, which is NULL for a table, a key and
        a foreign key: a dropped, narrowed or nullability-changed column
        read ok."""
        from .. import drift
        src, dst = self.shape_catalog("src", db), self.shape_catalog("dst",
                                                                     db)
        keep = [set(drift.in_scope(self, db, dict.fromkeys(c.tables)))
                for c in (src, dst)]
        so, do = self._objects("src", db), self._objects("dst", db)
        bad = [f"missing table {t}" for t in sorted(keep[0] - keep[1])]
        bad += [f"extra table {t}" for t in sorted(keep[1] - keep[0])]
        for t in sorted(keep[0] & keep[1]):
            if src.fingerprint(t) == dst.fingerprint(t):
                continue
            said = src.explain(dst, t, self.CANON_ENGINE,
                               added="only on the target",
                               dropped="missing on the target")
            bad.append(f"{t}: {', '.join(said)}")
        for k in sorted(set(so) | set(do)):
            if k not in do:
                bad.append(f"missing {k[1]} {k[0]}")
            elif k not in so:
                bad.append(f"extra {k[1]} {k[0]}")
            elif so[k] != do[k]:
                bad.append(f"definition differs {k[1]} {k[0]}")
        d = self.hop.report_dir(db)
        for side, cat, mine, objs in (("src", src, keep[0], so),
                                      ("dst", dst, keep[1], do)):
            (d / f"schema-{side}.txt").write_text(
                drift.said({t: v for t, v in drift.pairs(cat).items()
                            if t in mine})
                + "".join(f"{k[1]} {k[0]} {v}\n"
                          for k, v in sorted(objs.items())))
        columns = sum(len(src.columns.get(t) or []) for t in keep[0])
        if bad:
            (d / "schema.diff").write_text("\n".join(bad) + "\n")
            return [Result("schema", db, "diff", "; ".join(bad[:8]),
                           str(d / "schema.diff"),
                           "make the target's tables, keys, indexes and"
                           " code the source's before loading: each"
                           " difference is in schema.diff")]
        return [Result("schema", db, "ok",
                       f"{len(keep[0])} tables, {columns} columns with their"
                       " keys, indexes, constraints and triggers, and"
                       f" {len(so)} views, routines, sequences and synonyms"
                       " the same on both sides")]

    def check_counts(self, db):
        q = ("select s.name+'.'+t.name, sum(p.rows) from sys.tables t"
             " join sys.schemas s on s.schema_id=t.schema_id"
             " join sys.partitions p on p.object_id=t.object_id and p.index_id in (0,1)"
             " group by s.name, t.name order by 1")
        src = {r[0]: r[1] for r in
               self._scoped_rows(db, self._cmd("src", db, q))}
        dst = {r[0]: r[1] for r in
               self._scoped_rows(db, self._cmd("dst", db, q))}
        bad = [f"{t} src={src.get(t)} dst={dst.get(t)}"
               for t in sorted(set(src) | set(dst)) if src.get(t) != dst.get(t)]
        if bad:
            return [Result("counts", db, "diff", clauses(bad))]
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
        # the identities of the tables the hop takes: one it leaves out
        # keeps the seed its own writer gives it
        src = {r[0]: r[2] for r in self._cmd("src", db, q)
               if self.in_scope(db, r[0])}
        drows = [r for r in self._cmd("dst", db, q)
                 if self.in_scope(db, r[0])]
        dcol = {r[0]: r[1] for r in drows}
        dcur = {r[0]: r[2] for r in drows}
        collide = []
        for tbl, col in sorted(dcol.items()):
            try:
                mx = int(self._cmd(
                    "dst", db, f"select isnull(max({self._q(col)}),0) from"
                               f" {self._qualified('dst', db, tbl)} with"
                               " (nolock)")[0][0] or 0)
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
        rows = self._kept_rows(
            "src", db, "select c.name from sys.index_columns ic"
                       " join sys.indexes i on i.object_id=ic.object_id"
                       " and i.index_id=ic.index_id and i.is_primary_key=1"
                       " join sys.columns c on c.object_id=ic.object_id"
                       " and c.column_id=ic.column_id"
                       f" where ic.object_id=object_id({MARK})"
                       " order by ic.key_ordinal",
            (self._qualified("src", db, t),))
        return [str(r[0]) for r in rows]

    def _drilldown(self, db, t):
        """Row-level compare via canonical FOR JSON rendering: SQL Server
        serializes the row itself, no hand-rolled cast rules to get wrong."""
        pks = self._pk_cols(db, t)
        if not pks:
            return None
        named = self._qualified("src", db, t)
        n = int(self._cmd("src", db, f"select count_big(*) from {named}"
                                     " with (nolock)")[0][0] or 0)
        if n > self.DRILL_MAX_ROWS:
            return None
        pkexpr = "+'\t'+".join(f"cast(t.{self._q(c)} as varchar(100))"
                               for c in pks)
        row, _ = self._row_json(db, t)
        q = (f"select {pkexpr}, convert(varchar(64), hashbytes('SHA2_256',"
             f" {row}), 2) from {named} t with (nolock)")
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
        # the tables the hop takes (`in_scope`), on the source; one the
        # target lacks is a difference, not a table passed over
        st = [r[0] for r in self._cmd("src", db, q)
              if self.in_scope(db, r[0])]
        dt = {r[0] for r in self._cmd("dst", db, q)}
        tables = [table] if table else [t for t in st if t in dt]
        bad, left = [], []
        outcomes = {} if table else {t: "diff" for t in st if t not in dt}
        apart = {}
        if not table:
            bad += [f"{t} missing on target" for t in st if t not in dt]
        rows_a = rows_b = 0
        bad_counts, settled = [], []
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
                outcomes[t] = "error"
                continue
            left += out
            cq = ("select count_big(*), isnull(sum(h), 0) from (select"
                  " cast(convert(bigint, substring(hashbytes('SHA2_256',"
                  f" N'{salt}' + {row}), 1, 8)) as decimal(38, 0)) h"
                  f" from {self._qualified('src', db, t)} t) rows_hashed")
            try:
                with gate.unit():
                    a = self._cmd("src", db, cq)[0]
                    b = self._cmd("dst", db, cq)[0]
            except RuntimeError as e:
                bad.append(f"{t} error {e}")
                outcomes[t] = "error"
                continue
            outcomes[t] = "ok"
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
                    # Still different, nothing found where, and it is said;
                    # agreeing, the table's answer is this check's own `ok`
                    if self._cmd("src", db, cq)[0] != \
                            self._cmd("dst", db, cq)[0]:
                        bad.append(f"{t} differs, not localized: the sums"
                                   " still differ and no row differs by"
                                   " key")
                        outcomes[t] = "diff"
                    else:
                        settled.append(t)
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
                    apart.update(rows_counted(t, m, e, c))
                else:
                    bad.append(f"{t} src={a} dst={b}")
                outcomes[t] = "diff"
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
            res.append(Result("data", db, "diff", clauses(bad) + said,
                              "", "pk-level diffs in data-*.missing/extra/"
                              f"changed; migkit sync {self.hop.name} --db"
                              f" {db} --kind rows shows the repair, --apply"
                              " runs it", tables=outcomes,
                              rows_apart=apart))
        else:
            again = (f"; {len(settled)} read while rows changed, every row"
                     " equal by key and the sums equal when read again: "
                     + ", ".join(settled[:8]) if settled else "")
            res.append(Result("data", db, "ok",
                              f"{len(tables)} tables, counts and every"
                              " row's hash equal both sides" + again + said,
                              tables=outcomes))
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
        pkexpr = "+'\t'+".join(f"cast(t.{self._q(c)} as varchar(100))"
                               for c in pks)
        row, _ = self._row_json(db, table)
        keys = sorted(keys)
        got = {"src": {}, "dst": {}}
        for i in range(0, len(keys), 500):
            part = ", ".join("'" + k.replace("'", "''") + "'"
                             for k in keys[i:i + 500])
            q = (f"select {pkexpr}, convert(varchar(64), hashbytes("
                 f"'SHA2_256', {row}), 2)"
                 f" from {self._qualified('src', db, table)} t where"
                 f" {pkexpr} in ({part})")
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
        nopk = [r[0] for r in self._scoped_rows(db, self._cmd("src", db,
                "select s.name+'.'+t.name from sys.tables t"
                " join sys.schemas s on s.schema_id=t.schema_id"
                " where not exists (select 1 from sys.indexes i"
                " where i.object_id=t.object_id"
                " and (i.is_primary_key=1 or i.is_unique=1))"))]
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
        # each after the table it is on, which decides its scope
        rows = self._scoped_rows(db, self._cmd("dst", db,
                       "select s.name+'.'+t.name+'.'+fk.name,"
                       " fk.is_disabled, fk.is_not_trusted, s.name+'.'+t.name"
                       " from sys.foreign_keys fk"
                       " join sys.tables t on t.object_id=fk.parent_object_id"
                       " join sys.schemas s on s.schema_id=t.schema_id"), 3)
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
        trg = [r[0] for r in self._scoped_rows(db, self._cmd("dst", db,
               "select s.name+'.'+t.name+'.'+tr.name, s.name+'.'+t.name"
               " from sys.triggers tr"
               " join sys.tables t on t.object_id=tr.parent_id"
               " join sys.schemas s on s.schema_id=t.schema_id"
               " where tr.is_disabled=1"), 1)]
        res.append(Result("deep", f"{db} triggers",
                          "diff" if trg else "ok",
                          "disabled on target: " + ", ".join(trg[:5]) if trg
                          else "no disabled triggers on target", "",
                          "enable trigger ... on ..." if trg else ""))
        colq = ("select table_schema+'.'+table_name+'.'+column_name+'|'+"
                "data_type+'|'+is_nullable+'|'+isnull(column_default,'')+'|'+"
                "isnull(cast(character_maximum_length as varchar),'')+'|'+"
                "isnull(cast(numeric_precision as varchar),'')+'|'+"
                "isnull(collation_name,''),"
                " table_schema+'.'+table_name"
                " from information_schema.columns order by 1")
        sc = {r[0].split("|", 1)[0]: r[0] for r in
              self._scoped_rows(db, self._cmd("src", db, colq), 1)}
        dc = {r[0].split("|", 1)[0]: r[0] for r in
              self._scoped_rows(db, self._cmd("dst", db, colq), 1)}
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
        for t, c in self._scoped_rows(db, pk_rows):
            n += 1
            try:
                q = (f"select isnull(max({self._q(c)}),0) from"
                     f" {self._qualified('src', db, t)} with (nolock)")
                a = int(self._cmd("src", db, q)[0][0] or 0)
                b = int(self._cmd("dst", db, q)[0][0] or 0)
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
        from .. import workload
        res.append(workload.compare(self, db))
        res.append(self._deep_partitions(db))
        res.append(self._mojibake(db))
        return res

    # ---- partitions ------------------------------------------------------

    def partition_scheme(self, side, db):
        """{table: {"m": method, "e": the partitioning column and which
        side a boundary value belongs to, "p": {partition: its bound}}} of
        every partitioned table in scope. A partition has a number and no
        name, so `p1`, `p2` ...: the number is its place in the function's
        order, and its bound the boundary value that closes it (`<= v` on
        a function that puts a boundary on its left) or opens it (`>= v`).
        The partition every other value lands in is the open end."""
        rows = self._cmd(side, db, (
            "select s.name + '.' + t.name, pf.name, pf.type_desc,"
            " pf.boundary_value_on_right, c.name, p.partition_number,"
            " pf.fanout from sys.partitions p join sys.tables t on"
            " t.object_id = p.object_id join sys.schemas s on s.schema_id ="
            " t.schema_id join sys.indexes i on i.object_id = p.object_id"
            " and i.index_id = p.index_id join sys.partition_schemes ps on"
            " ps.data_space_id = i.data_space_id join sys.partition_functions"
            " pf on pf.function_id = ps.function_id join sys.index_columns ic"
            " on ic.object_id = i.object_id and ic.index_id = i.index_id and"
            " ic.partition_ordinal = 1 join sys.columns c on c.object_id ="
            " ic.object_id and c.column_id = ic.column_id where i.index_id in"
            " (0, 1) order by 1, p.partition_number"))
        bounds = {}
        for fn, at, value in self._cmd(side, db, (
                "select pf.name, prv.boundary_id, convert(nvarchar(4000),"
                " prv.value, 126) from sys.partition_functions pf join"
                " sys.partition_range_values prv on prv.function_id ="
                " pf.function_id order by 1, 2")):
            bounds[(fn, int(at))] = value
        out = {}
        for table, fn, kind, right, col, number, fanout in rows:
            if not self.in_scope(db, table):
                continue
            right, number, fanout = (str(right) in ("1", "True")), \
                int(number), int(fanout)
            one = out.setdefault(table, {
                "m": f"{kind} {'RIGHT' if right else 'LEFT'}", "e": col,
                "f": fn, "p": {}})
            if right:
                bound = (f">= {bounds[(fn, number - 1)]}" if number > 1
                         else "MINVALUE")
            else:
                bound = (f"<= {bounds[(fn, number)]}" if number < fanout
                         else "MAXVALUE")
            one["p"][f"p{number}"] = bound
        return out

    def _partition_rows(self, side, db, table, parts, scheme):
        """{partition: {"rows", "digest", "bound", "catchall"}} of one
        table on one side: every partition counted by `$PARTITION` over its
        function and column, in one statement. Counted and not digested -
        the digest is left None, and the check says so."""
        one = scheme[table]
        function = one["f"]
        at = f"$PARTITION.{self._q(function)}({self._q(one['e'])})"
        got = {int(n): int(c) for n, c in self._cmd(side, db, (
            f"select {at}, count_big(*) from "
            f"{self._qualified(side, db, table)} t group by {at}"))}
        out = {}
        for name, bound in parts.items():
            out[name] = {"rows": got.get(int(name[1:]), 0), "digest": None,
                         "bound": bound,
                         "catchall": bound in ("MAXVALUE", "MINVALUE")}
        return out

    def _deep_partitions(self, db):
        """Every partitioned table's function and column on both sides, and
        every partition held to its own count (`Engine._partitions_verdict`)
        - a partition that arrived empty leaves the table's total
        plausible."""
        try:
            sp = self.partition_scheme("src", db)
            dp = self.partition_scheme("dst", db)
            names = {side: {r[0] for r in self._cmd(side, db, (
                "select s.name + '.' + t.name from sys.tables t join"
                " sys.schemas s on s.schema_id = t.schema_id"))}
                for side in ("src", "dst")}
            scheme, tables = [], {}
            for t, si in sorted(sp.items()):
                di = dp.get(t)
                if not di:
                    scheme.append(
                        f"{t}: partitioned on source, not on target"
                        if t in names["dst"] else
                        f"{t}: missing on target (partitioned by"
                        f" {si['m']} on the source)")
                    continue
                if (si["m"], si["e"]) != (di["m"], di["e"]):
                    scheme.append(f"{t}: partition scheme differs"
                                  f" src({si['m']} {si['e']})"
                                  f" dst({di['m']} {di['e']})")
                tables[t] = (
                    self._partition_rows("src", db, t, si["p"], sp),
                    self._partition_rows("dst", db, t, di["p"], dp))
            for t, di in sorted(dp.items()):
                if t not in sp and t in names["src"]:
                    scheme.append(f"{t}: partitioned on target ({di['m']}"
                                  f" {di['e']}), not on source")
            return self._partitions_verdict(db, tables, scheme, "open-ended")
        except Exception as e:  # noqa: BLE001 - said, and the rest runs
            return Result("deep", f"{db} partitions", "error",
                          "could not read the partitions:"
                          f" {str(e).splitlines()[-1][:120]}")

    # --- the source's own statements, from its Query Store (R23, 4) -----

    #: statements of migkit's own checks, left out of the application's
    MIGKIT_MARKS = ("sys.", "information_schema", "hashbytes(",
                    "checksum_agg(", "serverproperty(", "@@")

    def workload_reads(self, db, n):
        """[(label, statement, runnable, mean ms, calls)] - the source's
        busiest reads by total time, from its Query Store - or why there
        are none. A statement the application sent with parameters is kept
        as the server holds it, `(@P1 int)select ...`: planned where the
        parameters can be declared, never run."""
        return self._workload(db, n, ("select", "with"))

    def workload_writes(self, db, n):
        """The source's busiest inserts, updates, deletes and merges, as
        `workload_reads` gives its reads - their mean time the Query Store's
        (a write is never run on the source)."""
        return self._workload(db, n, ("insert", "update", "delete",
                                      "merge"))

    def _workload(self, db, n, verbs):
        state = self._rows("src", db, "select actual_state_desc from"
                                      " sys.database_query_store_options")
        if not state or str(state[0][0]).upper() not in ("READ_WRITE",
                                                          "READ_ONLY"):
            return ("the source keeps no statement statistics here (its"
                    " Query Store is off in this database)")
        rows = self._rows("src", db, f"""
            select top ({int(n) * 3}) qt.query_sql_text,
                   sum(rs.avg_duration * rs.count_executions)
                       / nullif(sum(rs.count_executions), 0) / 1000.0,
                   sum(rs.count_executions)
              from sys.query_store_query_text qt
              join sys.query_store_query q
                on q.query_text_id = qt.query_text_id
              join sys.query_store_plan p on p.query_id = q.query_id
              join sys.query_store_runtime_stats rs on rs.plan_id = p.plan_id
             group by qt.query_sql_text
             order by sum(rs.avg_duration * rs.count_executions) desc""")
        out = []
        for text, mean, calls in rows:
            body = _without_parameters(" ".join(str(text or "").split()))
            low = body.lower()
            if not low.startswith(verbs) or any(m in low for m in
                                                self.MIGKIT_MARKS):
                continue
            out.append((body[:70], body, "@" not in body,
                        float(mean) if mean is not None else None,
                        int(calls or 1)))
            if len(out) >= n:
                break
        return out

    def workload_plan(self, side, db, sql):
        """{table: "index <name>" or None where the plan reads it whole},
        from the estimated plan (`SHOWPLAN_XML`): nothing is run."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute("SET SHOWPLAN_XML ON")
            try:
                cur.execute(sql)
                got = cur.fetchone()[0]
            finally:
                cur.execute("SET SHOWPLAN_XML OFF")
        finally:
            conn.close()
        return plan_tables(got)

    def workload_time(self, side, db, sql):
        """The statement's time in ms, its rows read and let go; it waits
        on no lock for more than five seconds."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute("SET LOCK_TIMEOUT 5000")
            began = time.perf_counter()
            cur.execute(sql)
            while cur.fetchmany(10000):
                pass
            return (time.perf_counter() - began) * 1000.0
        finally:
            conn.close()

    def workload_rows(self, side, db, sql, most):
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute("SET LOCK_TIMEOUT 5000")
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchmany(int(most))]
        finally:
            conn.close()

    def workload_time_write(self, side, db, sql):
        """A write's time in ms on the target, inside a transaction rolled
        back; a lock it would wait on for more than a second stops it."""
        import pymssql
        self._target_only(side, "time a write")
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute("SET LOCK_TIMEOUT 1000")
            began = time.perf_counter()
            try:
                cur.execute(sql)
            except pymssql.OperationalError as e:
                if "1222" in str(e):
                    return None
                raise
            return (time.perf_counter() - began) * 1000.0
        finally:
            conn.rollback()
            conn.close()

    def planner_settings(self, side, db):
        """What the optimizer's choices turn on: the database's
        compatibility level - the cardinality estimator it runs - its
        scoped configurations, and the server's parallelism."""
        out = {}
        for (level,) in self._rows(side, db, "select compatibility_level"
                                             " from sys.databases where"
                                             " name = db_name()"):
            out["compatibility_level"] = str(level)
        for name, value in self._rows(
                side, db, "select name, cast(value as nvarchar(64)) from"
                          " sys.database_scoped_configurations where name"
                          " in ('LEGACY_CARDINALITY_ESTIMATION', 'MAXDOP',"
                          " 'PARAMETER_SNIFFING', 'QUERY_OPTIMIZER_HOTFIXES')"):
            out[str(name).lower()] = str(value)
        for name, value in self._rows(
                side, db, "select name, cast(value_in_use as nvarchar(64))"
                          " from sys.configurations where name in ('max"
                          " degree of parallelism', 'cost threshold for"
                          " parallelism')"):
            out[str(name)] = str(value)
        return out

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            q = ("select s.name+'.'+t.name,"
                 " cast(ident_current(s.name+'.'+t.name) as bigint)"
                 " from sys.tables t join sys.schemas s"
                 " on s.schema_id=t.schema_id where"
                 " objectproperty(t.object_id,'TableHasIdentity')=1")
            # the identities of the tables the hop takes only: one it
            # leaves out keeps the seed its own writer gives it
            src, dst = ({t: v for t, v in self._cmd(side, db, q)
                         if self.in_scope(db, t)} for side in ("src", "dst"))

            def lit(t):
                # the table's quoted name, as a literal: a quote in a name
                # stays in the name
                return self._qualified("dst", db, t).replace("'", "''")

            def ahead(t):
                # never lowered: a seed past the source's has handed out
                # the keys up to it, and reseeded back, the next insert
                # takes one of them again
                return dst.get(t) not in (None, "NULL") \
                    and src[t] != "NULL" and int(dst[t]) > int(src[t])
            change = [t for t, v in sorted(src.items())
                      if dst.get(t) != v and v != "NULL" and not ahead(t)]
            stmts = [f"dbcc checkident ('{lit(t)}', reseed, {int(src[t])});"
                     f"  -- dst now {dst.get(t, 'MISSING')}" for t in change]
            undo = [f"dbcc checkident ('{lit(t)}', reseed, {int(dst[t])});"
                    for t in change if t in dst and dst[t] != "NULL"]
            same = sum(1 for t, v in src.items() if dst.get(t) == v)
            past = [f"{t}: target {dst[t]} > source {src[t]}"
                    for t in sorted(src) if ahead(t)]
            if stmts or past:
                actions.append(RepairAction(
                    db, "sequences", stmts, undo,
                    f"{len(stmts)} identities differ, {same} already equal"
                    + (f"; left alone {len(past)} (the target's seed is past"
                       " it already, never lowered): " + "; ".join(past[:4])
                       if past else "")))
        if kind in ("rows", "all"):
            actions += self._rows_found_plan(db)
            # after the rows, never before: a row copied again takes the
            # source's text, which is where the broken text lives
            text = self._mojibake_repair(db)
            if text:
                actions.append(text)
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
        # a drilldown of a table the hop has since left out is said, never
        # acted on
        tables, said = self._drills_in_scope(db, tables)
        out = [said] if said else []
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
                    " or removes is written to the undo file first",
                table=t))
        return out

    def repair_found(self, db, table):
        """The keys the last check named for `table` (`_found`)."""
        return self._found(db, table)

    def apply(self, db, action):
        if action.kind == "text":
            return self._apply_text(db, action)
        if action.kind == "rows":
            t = action.scope.split(".", 1)[1]
            sc, dc, _ = self._comparable_columns(db, self, t, self, t)
            # written as a load writes (`load_window`)
            with self.load_window(db, None, {t}):
                return self._apply_rows(db, t, self, t, sc, self, t, dc,
                                        found=self._found(db, t))
        if action.kind != "sequences":
            raise SystemExit(f"mssql cannot apply a {action.kind!r} action")
        # seeds left alone and nothing to reseed send nothing
        if action.statements:
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
            f"script the schema of {db} from the source: its tables, keys,"
            " indexes, views and routines",
            f"create database {self._q(self.hop.target_db(db))} on the"
            " target",
            f"run that script on the target, in"
            f" {self._q(self.hop.target_db(db))}",
            "-- disable FK/triggers on target during load, then start the"
            " migration service",
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
                             f" {self._qualified('src', db, tbl)}, {MARK})"
                             " ct", (int(last),))
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
            if r is None:
                # not compared row by row is not a difference found, nor
                # one ruled out: the version stays where it was
                res.append(Result("delta", f"{db}.{tbl}", "skip",
                                  f"{n} rows changed since v{last}, not"
                                  " compared again row by row: the table"
                                  " has no primary key or holds more than"
                                  f" {self.DRILL_MAX_ROWS:,} rows"))
                continue
            res.append(Result("delta", f"{db}.{tbl}", "ok" if ok else "diff",
                              f"{n} rows changed since v{last}, table"
                              f" {'verified equal' if ok else 'DIFFERS'}"))
            if log:
                log(f"{tbl}: {n} changed, {'ok' if ok else 'DIFF'}")
        if clean:
            state.write_text(json.dumps({"ver": cur}))
        found = any(r.status == "diff" for r in res)
        res.insert(0, Result("delta", db, "diff" if found else "ok" if clean
                             else "warn",
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

    #: large values (`pieces`): a value's bytes as the server keeps them,
    #: `datalength` - read off the value's root, not the value; a byte
    #: value past a piece read by `substring` of it, and a value of a
    #: `(max)` type written a piece a statement through `.write`: the
    #: driver writes a parameter into the statement's own text, a byte
    #: value as hex twice its size. A byte value's md5 by `hashbytes`,
    #: which takes any length from SQL Server 2016; a large table sized over
    #: a sample of its pages
    LARGE_VALUES = pieces.Sql(
        "sized by the server, a byte value read and every (max) value"
        " written a piece at a time, byte values hashed where they lie",
        length="datalength({c})",
        types=("varbinary", "varchar", "nvarchar", "text", "ntext", "image",
               "xml", "binary"),
        piece="substring({c}, {at}, {n})",
        append="{c}.write({v}, null, null)",
        appends=("varbinary(max)", "varchar(max)", "nvarchar(max)"),
        md5="convert(varchar(32), hashbytes('MD5', {c}), 2)",
        sample="tablesample system ({pct} percent)")

    def _connect(self, side, db):
        import pymssql
        ep = self.hop.source if side == "src" else self.hop.target
        return pymssql.connect(server=ep.host, port=str(ep.port),
                               user=ep.user, password=ep.password,
                               database=self._d(side, db), login_timeout=15,
                               charset="UTF-8", tds_version="7.4",
                               **ep.mssql_tls())

    @staticmethod
    def _q(name):
        return "[" + str(name).replace("]", "]]") + "]"

    _bracket = _q

    def neutral_tables(self, side, db):
        return [t for (t,) in self._rows(
            side, db, "select s.name + '.' + t.name from sys.tables t join"
                      " sys.schemas s on s.schema_id = t.schema_id where"
                      " t.is_ms_shipped = 0 order by 1")
            if self.in_scope(db, t)]

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
            if not self.in_scope(db, name):
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
                if table and not self.in_scope(db, table):
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
    #: what each cost (`Engine._cheaper`), starting from `WRITE_COSTS`;
    #: the bulk load only where its driver loads here, the table has not
    #: refused it this run and every column is of a type it is proved on
    WRITE_WAYS = (
        decide.Rung("bulk load", "SQL Server's bulk load, staged and"
                                 " promoted",
                    # the stage takes the batch whole; a value past a
                    # piece goes in a piece a statement (`LARGE_VALUES`)
                    needs=(decide.Need("no value to write in pieces",
                                       lambda f: not f["past a piece"],
                                       "{rung} writes no value in pieces"),
                           decide.Need("its driver",
                                       lambda f: f["driver"] is None,
                                       lambda f: f["driver"]),
                           decide.Need("a table that has not refused it",
                                       lambda f: f["refused"] is None,
                                       lambda f: f["refused"]),
                           decide.Need("types it is proved on",
                                       lambda f: f["unproved"] is None,
                                       lambda f: f["unproved"])),
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
        None. It brings Microsoft's ODBC driver, which is not open source,
        so it is installed only once its terms are accepted (`tools.TERMS`,
        `migkit doctor --install`); and it is a compiled one: a wheel built
        for a newer C library than the machine's installs and then fails
        to load."""
        from ..tools import MSSQL_BULK, driver_present
        if not driver_present(MSSQL_BULK):
            return ("the bulk load's driver is not installed here - `migkit"
                    " doctor --install` puts it in place once its terms are"
                    " accepted")
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
        try:
            return mssql_python.connect(
                f"Server={ep.host},{ep.port or 1433};"
                f"Database={braced(self._d(side, db))};UID={braced(ep.user)};"
                f"PWD={braced(ep.password)};Encrypt={encrypt};"
                "TrustServerCertificate=yes", autocommit=False, timeout=15)
        except Exception as e:
            if not decide.operational(e):
                raise
            # the statements' own connection may still be made: another
            # way may carry the rows
            from ..util import without_secret
            raise decide.Blocked(
                "the bulk load could not connect to the"
                f" {'source' if side == 'src' else 'target'}: "
                + without_secret(_said(e), ep.password)) from e

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

    def _write_ladder(self, side, db, table, columns, rows, wide=False):
        """(facts, costs, key) of a batch of `rows` rows into `table`
        climbing `WRITE_WAYS`: the costs - what each way was measured to
        cost (`WRITE_COSTS`) until each is timed here, for batches of this
        size - only where both ways hold, and the batch is then timed.
        `wide`: the batch holds a value past a piece (`LARGE_VALUES`)."""
        facts = decide.Facts(
            **{"past a piece": lambda: wide},
            driver=self._bulk_unusable,
            refused=lambda: self.__dict__.get("_refused", {}).get(
                (side, db, str(table))),
            unproved=lambda: self._unproved(self._shape(side, db, table),
                                            [n for n, _ in columns]))
        held, _ = decide.prune(self.WRITE_WAYS, facts)
        if len(held) < 2:
            return facts, None, None
        key = self._work_key("rows", str(table), rows)
        return facts, self._way_costs(key, self.WRITE_WAYS, {
            w: (fixed + rows * row) / max(rows, 1)
            for w, (fixed, row) in self.WRITE_COSTS.items()}, facts), key

    def write_way(self, side, db, table, columns, rows):
        """("statements" | "bulk load", why) for a batch of `rows` rows
        into `table`, climbed through `WRITE_WAYS`: the bulk load only
        where its driver loads here, it has not refused this table this
        run and every column written is of a type it is proved on - and
        then whichever way did a row in less time for batches of this size
        (`_write_ladder`)."""
        facts, costs, key = self._write_ladder(side, db, table, columns,
                                               rows)
        got = decide.climb(str(table), self.WRITE_WAYS, facts, costs=costs)
        if costs is None:
            return got.path, got.reason
        return got.path, (f"the way a row cost less for batches of about"
                          f" {key[2]:,} rows")

    def neutral_write(self, side, db, table, columns, rows):
        """Rows in by statements or through the bulk load, whichever
        `write_way` finds cheaper for this batch. Either replaces the rows
        with the same key in one transaction, so a batch written again
        converges.

        The bulk load that cannot run - a value its stage will not take,
        one another engine hands over in a form it does not convert, or
        its own connection or stage refused - has touched nothing but the
        stage: the batch is switched to statements by itself and said
        (`decide.attempt`), and so is the rest of the table this run. What
        the promote refuses, the table itself refuses, and is said as it
        would be by statements: never switched from."""
        if not rows:
            return 0
        self._target_only(side, "write rows")
        facts, costs, key = self._write_ladder(side, db, table, columns,
                                               len(rows),
                                               pieces.wider_than(rows))
        by_statements = super().neutral_write

        def run(choice):
            began = time.monotonic()
            if choice.path == "bulk load":
                self._bulk_write(side, db, table, columns, rows)
            else:
                by_statements(side, db, table, columns, rows)
            if costs is not None:
                # timed where the way was chosen by what it cost
                self._took(key, choice.path, time.monotonic() - began,
                           len(rows))

        def undo(rung, error):
            self.__dict__.setdefault("_refused", {})[
                (side, db, str(table))] = decide._first(error)
        self._attempt(str(table), self.WRITE_WAYS, run, facts, costs=costs,
                      undo=undo, switch=decide.only_blocked)
        return len(rows)

    def _bulk_write(self, side, db, table, columns, rows):
        """One batch through the bulk load, staged and promoted. What stops
        it before the promote is `decide.Blocked`: nothing has reached the
        table."""
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
            stage = _Stage(self, side, db, table, names,
                           {n: t[1:] for n, t in texts.items()})
        except _NotLoaded as e:
            raise decide.Blocked(_refused_value(e)) from e
        with stage:
            try:
                stage.load(staged)
            except _NotLoaded as e:
                raise decide.Blocked(_refused_value(e)) from e
            stage.promote()

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
        copied table by table, where its batches still go by whichever way
        costs less; so is one the bulk load cannot finish - a value its
        stage refuses, its connection or its stage refused - switched to by
        itself and said (`decide.attempt`): the copier empties the table
        before it writes, so what the load had promoted is not doubled. A
        table a row filter keeps for the table copier (`shape_only`) is
        made by the copier, the way it makes the rest here. Measured:
        500,000 rows in 6.97s on SQL Edge and 3.58s on the 2022 server,
        against 269.6s and 305.3s by statements (`WRITE_COSTS`)."""
        steps = [f"{t}: read in batches and loaded by SQL Server's bulk load,"
                 " each batch staged and promoted in one transaction"
                 for t in tables]
        if not go or not tables:
            return steps
        said = log or (lambda m: None)
        pair = []

        def run(choice, t):
            if choice.path == "arrow":
                try:
                    return self._native_table(db, t, log)
                except _NotLoaded as e:
                    raise decide.Blocked(_refused_value(e)) from e
            said(f"{t}: copied table by table - {choice.reason}")
            if not pair:
                pair.append(self._as_pair())
            sch, _, tbl = str(t).rpartition(".")
            pair[0].move_table(db, sch, tbl, pair[0].READ_ROWS, _Unkept(),
                               said)
        for t in tables:
            self._attempt(t, self.NATIVE_RUNGS,
                          lambda choice, t=t: run(choice, t),
                          self._native_facts(db, t), log=log,
                          switch=decide.only_blocked)
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
            if not self.in_scope(db, t):
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
                # the version is the transaction's that changed the row
                # last: what the tail holds to a copy's marks
                # (`mark_covers`)
                if op == "D" or not r[-1]:
                    found.append((version, canon.change(
                        "delete", t, ident, txn=int(version))))
                else:
                    found.append((version, canon.change(
                        "insert" if op == "I" else "update", t, ident,
                        values, txn=int(version))))
        found.sort(key=lambda x: x[0])
        # the changes of one version are one transaction's, and a read
        # from a version goes on after it
        out, group = [], []
        for i, (version, c) in enumerate(found):
            group.append(c)
            if i + 1 == len(found) or found[i + 1][0] != version:
                canon.ended(group, 0, at=int(version))
                out += group
                group = []
        return out, now

    def snapshot_mark(self, side, db):
        """The database's change tracking version now, after the database
        it was read in (`database_id` and its creation time on this
        server): every transaction at or below it has committed, and a
        read begun after it sees it. None where the database tracks no
        changes."""
        on = self._marked_on(side, db)
        try:
            got = self.run_rule(side, db, "select"
                                " change_tracking_current_version()")
        except Exception:  # noqa: BLE001 - no tracking, no mark
            return None
        if not on or not got or got[0][0] is None:
            return None
        return f"{on}|{int(got[0][0])}"

    def _marked_on(self, side, db):
        """The database a mark belongs to - its server, its id there and
        when it was made - asked once: a version means nothing in another
        database."""
        known = self.__dict__.setdefault("_marks_on", {})
        if (side, db) not in known:
            try:
                got = self.run_rule(side, db, "select @@servername, db_id(),"
                                    " convert(varchar(30), (select"
                                    " create_date from sys.databases where"
                                    " database_id = db_id()), 126)")
                known[(side, db)] = ("/".join(str(x) for x in got[0])
                                     if got else None)
            except Exception:  # noqa: BLE001 - no mark, then
                known[(side, db)] = None
        return known[(side, db)]

    def mark_covers(self, side, db, mark, change):
        """Whether the change's version (`txn`, the transaction that last
        changed the row) is at or below the mark's, in the same
        database."""
        version = change.get("txn")
        if not isinstance(mark, str) or not isinstance(version, int) \
                or isinstance(version, bool):
            return None
        on, _, at = mark.rpartition("|")
        if not on or on != self._marked_on(side, db):
            return None
        try:
            return version <= int(at)
        except ValueError:
            return None

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
        except BaseException as e:
            self.conn.close()
            if isinstance(e, Exception) and decide.operational(e):
                raise decide.Blocked(
                    f"its stage could not be made: {_said(e)}") from e
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


def _refused_value(e):
    """Why the bulk load gave up on a table, from its `_NotLoaded`."""
    return f"the bulk load refused a value of it: {e}"


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
