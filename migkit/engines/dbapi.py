"""Rows read and written through a DB-API driver, rendered in this process.

The neutral contract for SQL engines migkit has no canonical SQL rendering
for - SQL Server, Oracle, Db2, SAP ASE: rows come back through the driver
as values and are compared through `canon.render_value`, the renderer
every in-process engine shares. What differs between those engines is
small and is what an engine sets here: how it quotes a name, how it marks
a parameter, how it limits a read. Everything else is written once.
"""
from .base import NeutralCopier

#: where a parameter goes, before the driver's own mark replaces it
MARK = "\x00?\x00"


class DbapiRows(NeutralCopier):
    #: the driver's parameter style: "pyformat" (%s), "numeric" (:1) or
    #: "qmark" (?)
    PARAMSTYLE = "qmark"
    #: how a read is limited: "top" (select top (n) ...), "fetch"
    #: (... fetch first n rows only) or "limit" (... limit n)
    LIMIT = "fetch"
    #: rows per statement when deleting by key before a write
    DELETE_BATCH = 500
    #: fixed-length character types, whose values come back padded with
    #: spaces to the column's length: read without them, as PostgreSQL's
    #: `char(n)` reads and as every other engine's is compared
    PADDED = ()

    # ---- what an engine provides ---------------------------------------

    def _connect(self, side, db):
        raise NotImplementedError

    def _q(self, name):
        return '"' + str(name).replace('"', '""') + '"'

    def _quote_ident(self, name):
        return self._q(name)

    def _qualified(self, side, db, table):
        return ".".join(self._q(p) for p in str(table).split(".", 1))

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _nanos_text(self, quoted, declared):
        """The expression reading a column of this declared type as the
        server's own text of it with every digit after the second, where
        it holds more than six - Python's types hold six, and the driver
        cuts the rest (`nanotime`). None where it holds six or fewer."""
        return None

    def _wide_times(self, side, db, table):
        """{column: the expression reading it whole} for a table's
        columns of more than six digits after the second. Asked once per
        table, with which of its columns are `PADDED`."""
        known = self.__dict__.setdefault("_wide_times_of", {})
        at = (side, db, str(table))
        pads = self.__dict__.setdefault("_padded_of", {})
        if at not in known and not self.PADDED and \
                type(self)._nanos_text is DbapiRows._nanos_text:
            # an engine with neither: nothing to ask its catalogue about
            known[at], pads[at] = {}, set()
        if at not in known:
            padded = {p.lower() for p in self.PADDED}
            wide, pad = {}, set()
            columns = self.neutral_columns(side, db, table)
            for name, typ in columns:
                got = self._nanos_text(self._q(name), typ)
                if got:
                    wide[name] = got
                if str(typ).split("(")[0].strip().lower() in padded:
                    pad.add(name)
            if not columns:
                # a table not there yet is asked again once it is
                pads[at] = pad
                return wide
            known[at], pads[at] = wide, pad
        return known[at]

    def _whole_times(self, side, db, table, names, rows):
        """`rows` with every column `_wide_times` read as text turned back
        into its value, all its digits kept, and a padded character value
        without its padding."""
        from .. import nanotime
        wide = self._wide_times(side, db, table)
        padded = self.__dict__["_padded_of"][(side, db, str(table))]
        at = [i for i, n in enumerate(names) if n in wide]
        pad = [i for i, n in enumerate(names) if n in padded]
        for r in rows:
            for i in at:
                r[i] = nanotime.parse(r[i])
            for i in pad:
                if isinstance(r[i], str):
                    r[i] = r[i].rstrip(" ")
        return rows

    def _before_insert(self, cur, side, db, table):
        """Anything the engine needs said before explicit values go into
        a table (SQL Server's identity insert); returns what undoes it."""
        return None

    # ---- statements -----------------------------------------------------

    def _sql(self, sql, has_args):
        """The statement with each MARK as the driver writes a parameter.
        A driver that reads `%` as its own doubles every other one."""
        if self.PARAMSTYLE == "pyformat":
            if has_args:
                sql = sql.replace("%", "%%")
            return sql.replace(MARK, "%s")
        if self.PARAMSTYLE == "numeric":
            parts = sql.split(MARK)
            return "".join(p + (f":{i + 1}" if i < len(parts) - 1 else "")
                           for i, p in enumerate(parts))
        return sql.replace(MARK, "?")

    def _run(self, cur, sql, args=None):
        args = tuple(args or ())
        if args:
            cur.execute(self._sql(sql, True), args)
        else:
            cur.execute(self._sql(sql, False))

    def _rows(self, side, db, sql, args=None):
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._run(cur, sql, args)
            return [list(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def _after(self, key, after):
        """`(a, b) > (x, y)` spelled out, for engines that have no row
        comparison."""
        from .. import canon
        ors, args = [], []
        for i in range(len(key)):
            ands = [f"{self._q(k)} = {MARK}" for k in key[:i]]
            ands.append(f"{self._q(key[i])} > {MARK}")
            ors.append("(" + " and ".join(ands) + ")")
            args += [canon.sql_value(v) for v in after[:i + 1]]
        return "(" + " or ".join(ors) + ")", args

    def _select(self, cols, source, cond, order, limit):
        if limit and self.LIMIT == "top":
            return f"select top ({int(limit)}) {cols} from {source}" \
                   f"{cond}{order}"
        if limit and self.LIMIT == "limit":
            return f"select {cols} from {source}{cond}{order}" \
                   f" limit {int(limit)}"
        return (f"select {cols} from {source}{cond}{order}"
                + (f" fetch first {int(limit)} rows only" if limit else ""))

    def _read_query(self, side, db, table, columns, after, limit, where):
        names = [n for n, _ in columns]
        key = self.neutral_key(side, db, table)
        resume, args = ("", [])
        if key and after is not None:
            resume, args = self._after(key, after)
        parts = [p for p in (resume, f"({where})" if where else "") if p]
        cond = (" where " + " and ".join(parts)) if parts else ""
        order = (" order by " + ", ".join(self._q(k) for k in key)
                 if key else "")
        wide = self._wide_times(side, db, table)
        return (self._select(", ".join(wide.get(n, self._q(n))
                                       for n in names),
                             self._qualified(side, db, table), cond, order,
                             limit if key else None),
                args, key, names)

    # ---- the neutral contract -------------------------------------------

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        sql, args, key, names = self._read_query(side, db, table, columns,
                                                 after, limit, where)
        rows = self._whole_times(side, db, table, names,
                                 self._rows(side, db, sql, args))
        if not rows or not key or not all(k in names for k in key):
            return (rows, None)
        return (rows, tuple(rows[-1][names.index(k)] for k in key))

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        sql, args, _, names = self._read_query(side, db, table, columns,
                                               None, None, where)
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._run(cur, sql, args)
            while True:
                rows = cur.fetchmany(size)
                if not rows:
                    break
                yield self._whole_times(side, db, table, names,
                                        [list(r) for r in rows])
        finally:
            conn.close()

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        from .. import canon
        if not key or not keys:
            return {}
        keys = list(keys)
        one = "(" + " and ".join(f"{self._q(k)} = {MARK}" for k in key) + ")"
        found = []
        wide = self._wide_times(side, db, table)
        cols = ", ".join(wide.get(n, self._q(n)) for n, _ in columns)
        for i in range(0, len(keys), self.DELETE_BATCH):
            part = keys[i:i + self.DELETE_BATCH]
            cond = " or ".join([one] * len(part))
            if where:
                cond = f"({cond}) and ({where})"
            found += self._rows(side, db, f"select {cols} from"
                                          f" {self._qualified(side, db, table)}"
                                          f" where {cond}",
                                [canon.sql_value(v) for k in part for v in k])
        return self._by_key_map(columns, key, self._whole_times(
            side, db, table, [n for n, _ in columns], found))

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded here, over rows the driver hands back as values, with the
        renderer and the arithmetic every engine's digest is held to."""
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns,
                                          size=5000, where=where):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        return (n, str(total))

    def neutral_write(self, side, db, table, columns, rows):
        """Rows with the same key are deleted and inserted again in one
        transaction, which converges on a restart as an upsert does."""
        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        quoted = self._qualified(side, db, table)
        key = self.neutral_key(side, db, table)
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            undo = self._before_insert(cur, side, db, table)
            if key and all(k in names for k in key):
                at = [names.index(k) for k in key]
                one = "(" + " and ".join(f"{self._q(k)} = {MARK}"
                                         for k in key) + ")"
                for i in range(0, len(rows), self.DELETE_BATCH):
                    part = rows[i:i + self.DELETE_BATCH]
                    self._run(cur, f"delete from {quoted} where "
                                   + " or ".join([one] * len(part)),
                              [canon.sql_value(r[j]) for r in part
                               for j in at])
            insert = self._sql(
                f"insert into {quoted}"
                f" ({', '.join(self._q(n) for n in names)}) values"
                f" ({', '.join([MARK] * len(names))})", True)
            cur.executemany(insert, [tuple(canon.sql_value(v) for v in r)
                                     for r in rows])
            if undo:
                self._run(cur, undo)
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    # ---- applying a change stream ---------------------------------------
    #
    # A batch of changes is one transaction on one connection (the base's
    # `_apply_session`). A row is updated by its key and inserted where no
    # row was there: a change can carry only the columns it changed, and
    # deleting the row to insert it again would empty the others. Replayed,
    # the same statements find the row and leave it as it is.

    def _open_writer(self, side, db):
        self._target_only(side, "apply changes")
        return self._connect(side, db)

    def _where_key(self, key):
        from .. import canon
        return (" and ".join(f"{self._q(k)} = {MARK}" for k in key),
                [canon.sql_value(v) for v in key.values()])

    def _upsert_one(self, cur, side, db, table, key, values):
        from .. import canon
        quoted = self._qualified(side, db, table)
        where, kargs = self._where_key(key)
        sets = [n for n in values if n not in key]
        if sets:
            self._run(cur, f"update {quoted} set "
                           + ", ".join(f"{self._q(n)} = {MARK}"
                                       for n in sets) + f" where {where}",
                      [canon.sql_value(values[n]) for n in sets] + kargs)
            if cur.rowcount is not None and cur.rowcount > 0:
                return
        if not sets or cur.rowcount is None or cur.rowcount < 0:
            # nothing to set, or a driver that does not count: asked
            self._run(cur, f"select count(*) from {quoted} where {where}",
                      kargs)
            if cur.fetchone()[0]:
                return
        row = {**key, **values}
        self._run(cur, f"insert into {quoted}"
                       f" ({', '.join(self._q(n) for n in row)}) values"
                       f" ({', '.join([MARK] * len(row))})",
                  [canon.sql_value(v) for v in row.values()])

    def _apply_upserts(self, side, db, table, rows):
        with self._writer(side, db) as conn:
            cur = conn.cursor()
            undo = self._before_insert(cur, side, db, table)
            for key, values in rows:
                self._upsert_one(cur, side, db, table, key, values)
            if undo:
                self._run(cur, undo)

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        with self._writer(side, db) as conn:
            cur = conn.cursor()
            for key in keys:
                where, kargs = self._where_key(key)
                self._run(cur, f"delete from"
                               f" {self._qualified(side, db, table)} where"
                               f" {where}", kargs)

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._run(cur, f"delete from {self._qualified(side, db, table)}"
                           + (f" where ({where})" if where else ""))
            gone = cur.rowcount
            if gone is None or gone < 0:
                # DuckDB answers a delete with a row holding the count and
                # leaves rowcount at -1: "emptied -1 rows" was said
                got = cur.fetchone() if cur.description else None
                gone = int(got[0]) if got else 0
            conn.commit()
        finally:
            conn.close()
        return gone

    def _create_name(self, side, db, table):
        return ".".join(self._q(p) for p in str(table).split(".", 1))

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        defs = [f"{self._q(col[0])}"
                f" {canon.ddl_type(self.CANON_ENGINE, col[1], col[2],
                                   col[0] in key)}"
                + self._column_tail(col[3] if len(col) > 3 else None,
                                    self.CANON_ENGINE)
                for col in columns]
        if key:
            defs.append("primary key (" + ", ".join(self._q(k) for k in key)
                        + ")")
        return f"create table {self._create_name(side, db, table)} (" \
               + ", ".join(defs) + ")"

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a table")
        if self._leaf_name(table) in {self._leaf_name(t) for t in
                                      self.neutral_tables(side, db)}:
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        self.neutral_create_code(side, db, ddl)
        return ddl

    @staticmethod
    def _leaf_name(table):
        return str(table).split(".")[-1].lower()

    def neutral_create_code(self, side, db, statement):
        self._target_only(side, "create objects")
        conn = self._connect(side, db)
        try:
            self._run(conn.cursor(), statement)
            conn.commit()
        finally:
            conn.close()

    def run_rule(self, side, db, sql):
        """The operator's SQL, in a transaction that is always rolled back:
        undoing is the guard where a session cannot be made read-only."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return [list(r) for r in cur.fetchall()]
        finally:
            conn.rollback()
            conn.close()


class FoldsToCapitals:
    """For engines that fold an unquoted name to capitals (Oracle, Db2): a
    name in capitals is said in small letters, and a name in small letters
    is written in capitals - which is how `orders` on one side meets
    `ORDERS` on the other. A name of mixed case is kept as it is."""

    @staticmethod
    def _said(name):
        return name.lower() if name == name.upper() else name

    def _q(self, name):
        name = str(name)
        stored = name.upper() if name == name.lower() else name
        return '"' + stored.replace('"', '""') + '"'

    def _owner(self, side, db):
        name = self._d(side, db)
        return name.upper() if name == name.lower() else name

    def _qualified(self, side, db, table):
        return f"{self._q(self._owner(side, db))}.{self._q(table)}"

    def _create_name(self, side, db, table):
        return self._qualified(side, db, str(table).split(".")[-1])

    def _unpad(self, side, db, table, columns, rows):
        """Values as the column declares them: a fixed-length character
        column without its padding, a decimal at its scale, and at no scale
        an integer."""
        from decimal import Decimal

        from .. import canon
        declared = dict(self.neutral_columns(side, db, table))
        fix = []
        for i, (n, _) in enumerate(columns):
            t = declared.get(n, "")
            base = t.split("(")[0].strip().upper()
            if base in self.PADDED:
                fix.append((i, lambda v: v.rstrip(" ")
                            if isinstance(v, str) else v))
            elif base in self.SCALED and len(canon.params(t)) == 2:
                scale = canon.params(t)[1]
                step = Decimal(1).scaleb(-scale)
                fix.append((i, (lambda v: int(v) if v is not None else v)
                            if scale == 0 else
                            (lambda v, step=step: Decimal(v).quantize(step)
                             if v is not None else v)))
        for r in rows:
            for i, f in fix:
                r[i] = f(r[i])
        return rows

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        rows, last = super().neutral_read(side, db, table, columns, after,
                                          limit, where)
        return self._unpad(side, db, table, columns, rows), last

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        for batch in super().neutral_batches(side, db, table, columns, size,
                                             where):
            yield self._unpad(side, db, table, columns, batch)
