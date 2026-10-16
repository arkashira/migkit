"""Rows read and written through a DB-API driver, rendered in this process.

The neutral contract for SQL engines migkit has no canonical SQL rendering
for - SQL Server, Oracle, Db2, SAP ASE: rows come back through the driver
as values and are compared through `canon.render_value`, the renderer
every in-process engine shares. What differs between those engines is
small and is what an engine sets here: how it quotes a name, how it marks
a parameter, how it limits a read. Everything else is written once.
"""
import re
from dataclasses import dataclass

from .base import NeutralCopier
from .generic import GenericEngine

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
    #: what this engine does with its large values (`pieces.Sql`): sized,
    #: read and written in pieces, and hashed where they lie by its own
    #: expressions; None where it has none of its own
    LARGE_VALUES = None

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

    def _read_as(self, quoted, declared):
        """(expression, parse) reading a column of this declared type as a
        text the driver hands over whole and `parse` turns into the value
        - an interval's three parts, a shape's SRID and binary - where
        the driver's own reading of it loses part; None elsewhere."""
        return None

    def _wide_times(self, side, db, table):
        """{column: the expression reading it whole} for a table's
        columns of more than six digits after the second. Asked once per
        table, with which of its columns are `PADDED`."""
        known = self.__dict__.setdefault("_wide_times_of", {})
        at = (side, db, str(table))
        pads = self.__dict__.setdefault("_padded_of", {})
        parsers = self.__dict__.setdefault("_parsers_of", {})
        if at not in known and not self.PADDED and \
                type(self)._nanos_text is DbapiRows._nanos_text and \
                type(self)._read_as is DbapiRows._read_as:
            # an engine with neither: nothing to ask its catalogue about
            known[at], pads[at], parsers[at] = {}, set(), {}
        if at not in known:
            padded = {p.lower() for p in self.PADDED}
            wide, pad, parse = {}, set(), {}
            columns = self.neutral_columns(side, db, table)
            for name, typ in columns:
                got = self._nanos_text(self._q(name), typ)
                if got:
                    wide[name] = got
                other = self._read_as(self._q(name), typ)
                if other:
                    wide[name], parse[name] = other
                if str(typ).split("(")[0].strip().lower() in padded:
                    pad.add(name)
            parsers[at] = parse
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
        parse = self.__dict__.get("_parsers_of", {}).get(
            (side, db, str(table)), {})
        at = [(i, parse.get(n, nanotime.parse))
              for i, n in enumerate(names) if n in wide]
        pad = [i for i, n in enumerate(names) if n in padded]
        for r in rows:
            for i, fn in at:
                r[i] = fn(r[i]) if r[i] is not None else None
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
            args += [canon.write_value(self.CANON_ENGINE, None, v)
                     for v in after[:i + 1]]
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

    def _read_plan(self, side, db, table, columns, key=None):
        """How a read of `columns` carries their large values: split out
        and read in pieces (`pieces.ReadPlan`), or - inside a digest folded
        here - hashed where they lie (`pieces.DigestPlan`). `key` is the
        table's, where the caller has asked it already. The select list of
        `_read_query`, and what a read's rows are finished by: an engine
        that writes its own `_read_query` and has no `LARGE_VALUES` reads
        the plain list, which its plan finishes as it is."""
        from .. import pieces
        wide = self._wide_times(side, db, table)
        plan = (pieces.DigestPlan if pieces.is_hashing(self)
                else pieces.ReadPlan)
        return plan(self, side, db, table, columns, wide, key)

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
        plan = self._read_plan(side, db, table, columns, key)
        return (self._select(plan.select, self._qualified(side, db, table),
                             cond, order, limit if key else None),
                args, key, names)

    def _rows_until(self, side, db, sql, args, size):
        """The statement's rows up to the one that brings them to
        `pieces.BATCH_BYTES` (`pieces.fetch_until`)."""
        from .. import pieces
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._run(cur, sql, args)
            return pieces.fetch_until(cur, size)
        finally:
            conn.close()

    # ---- the neutral contract -------------------------------------------

    def neutral_read(self, side, db, table, columns, after=None, limit=None,
                     where=None):
        """In key order, ending at `limit` rows or at the row that brings
        the read to `pieces.BATCH_BYTES`, whichever is first: the next read
        goes on from its last row. A table with no key is read whole, as
        the contract says."""
        sql, args, key, names = self._read_query(side, db, table, columns,
                                                 after, limit, where)
        plan = self._read_plan(side, db, table, columns, key)
        keyed = bool(key) and all(k in names for k in key)
        raw = (self._rows_until(side, db, sql, args, plan.size) if keyed
               else self._rows(side, db, sql, args))
        rows = self._whole_times(side, db, table, names, plan.finish(raw))
        if not rows or not keyed:
            return (rows, None)
        return (rows, tuple(rows[-1][names.index(k)] for k in key))

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        """The whole table in one pass, in batches of `size` rows or
        `pieces.BATCH_BYTES` (`pieces.batches`)."""
        from .. import pieces
        sql, args, key, names = self._read_query(side, db, table, columns,
                                                 None, None, where)
        plan = self._read_plan(side, db, table, columns, key)
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._run(cur, sql, args)
            for rows in pieces.batches(cur, size, plan.size):
                yield self._whole_times(side, db, table, names,
                                        plan.finish(rows))
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
        plan = self._read_plan(side, db, table, columns)
        for i in range(0, len(keys), self.DELETE_BATCH):
            part = keys[i:i + self.DELETE_BATCH]
            cond = " or ".join([one] * len(part))
            if where:
                cond = f"({cond}) and ({where})"
            found += plan.finish(self._rows(
                side, db, f"select {plan.select} from"
                          f" {self._qualified(side, db, table)} where {cond}",
                [canon.write_value(self.CANON_ENGINE, None, v)
                 for k in part for v in k]))
        return self._by_key_map(columns, key, self._whole_times(
            side, db, table, [n for n, _ in columns], found))

    def neutral_rows_matching(self, side, db, table, columns, match, values):
        """Every row whose `match` columns hold one of `values`, asked of
        the server as a read by key asks, `DELETE_BATCH` values a
        statement."""
        from .. import canon
        if not match or not values:
            return []
        values = list(values)
        one = "(" + " and ".join(f"{self._q(k)} = {MARK}"
                                 for k in match) + ")"
        wide = self._wide_times(side, db, table)
        cols = ", ".join(wide.get(n, self._q(n)) for n, _ in columns)
        found = []
        for i in range(0, len(values), self.DELETE_BATCH):
            part = values[i:i + self.DELETE_BATCH]
            found += self._rows(
                side, db, f"select {cols} from"
                          f" {self._qualified(side, db, table)} where "
                          + " or ".join([one] * len(part)),
                [canon.sql_value(v) for k in part for v in k])
        return self._whole_times(side, db, table, [n for n, _ in columns],
                                 found)

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded here, over rows the driver hands back as values, with the
        renderer and the arithmetic every engine's digest is held to: in
        batches cut by bytes, each value wider than `canon.WIDE` that the
        server hashes read as its length and md5 (`pieces.hashing`)."""
        from .. import canon, pieces
        classes = [c for _, c in columns]
        total, n = 0, 0
        with pieces.hashing(self):
            for batch in self.neutral_batches(side, db, table, columns,
                                              size=5000, where=where):
                k, total = canon.fold_rows(classes, batch, total)
                n += k
        return (n, str(total))

    def row_sizes(self, side, db, tables=None):
        """Measured by the server where this engine says how
        (`LARGE_VALUES`, `pieces.sql_row_sizes`); from a sample of rows
        read here otherwise (`Engine.row_sizes`)."""
        from .. import pieces
        if isinstance(self.LARGE_VALUES, pieces.Sql):
            return pieces.sql_row_sizes(self, side, db, tables)
        return super().row_sizes(side, db, tables)

    def neutral_write(self, side, db, table, columns, rows):
        """Rows with the same key are deleted and inserted again in one
        transaction, which converges on a restart as an upsert does. A row
        holding a value past `pieces.PIECE` in a column this engine adds to
        is inserted with its first piece and filled by the rest, in the
        same transaction (`pieces.written_in_pieces`)."""
        from .. import canon, pieces
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
                              [canon.write_value(self.CANON_ENGINE,
                                                 columns[j][1], r[j])
                               for r in part for j in at])
            landed = len(rows)
            if pieces.wider_than(rows):
                rows = pieces.written_in_pieces(self, cur, side, db, table,
                                                columns, key, rows)
            marks = self._value_marks(side, db, table, names)
            if rows and marks:
                # a statement for each way the batch's values are placed:
                # one, unless a column takes some through an expression of
                # its own (a sql_variant of its base type)
                by = {}
                for r in rows:
                    got = [self._bound(m, c, v)
                           for m, (_, c), v in zip(marks, columns, r)]
                    by.setdefault(tuple(p for p, _ in got), []).append(
                        tuple(a for _, a in got))
                for places, part in by.items():
                    cur.executemany(self._sql(
                        f"insert into {quoted}"
                        f" ({', '.join(self._q(n) for n in names)}) values"
                        f" ({', '.join(places)})", True), part)
            elif rows:
                insert = self._sql(
                    f"insert into {quoted}"
                    f" ({', '.join(self._q(n) for n in names)}) values"
                    f" ({', '.join([MARK] * len(names))})", True)
                cur.executemany(insert, [tuple(
                    canon.write_value(self.CANON_ENGINE, c, v)
                    for (_, c), v in zip(columns, r)) for r in rows])
            if undo:
                self._run(cur, undo)
            conn.commit()
        finally:
            conn.close()
        return landed

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

    def _value_marks(self, side, db, table, names):
        """[how each of `names` takes a value written into `table`, or
        None], or None where every one is a plain parameter: f(value)
        giving (its place in the statement, `{}` where the parameter goes;
        what is bound there), or None from f where the value goes in as a
        parameter after all. An engine whose types take some values only
        through an expression of their own says so here: SQL Server's
        `sql_variant` of its base type, its shapes through their
        constructors (`commercial.mssql_mark`)."""
        return None

    def _bound(self, mark, cls, value):
        """(where one value goes in a statement, what is bound there):
        through its column's own expression (`_value_marks`) where that
        takes it, as a parameter `canon.write_value` writes otherwise."""
        from .. import canon
        got = mark(value) if mark is not None and value is not None \
            else None
        if got is None:
            return MARK, canon.write_value(self.CANON_ENGINE, cls, value)
        return got[0].format(MARK), got[1]

    def _where_key(self, key):
        from .. import canon
        return (" and ".join(f"{self._q(k)} = {MARK}" for k in key),
                [canon.write_value(self.CANON_ENGINE, None, v)
                 for v in key.values()])

    def _upsert_one(self, cur, side, db, table, key, values):
        quoted = self._qualified(side, db, table)
        where, kargs = self._where_key(key)
        sets = [n for n in values if n not in key]
        marks = dict(zip(sets, self._value_marks(side, db, table, sets)
                         or ()))
        if sets:
            placed = [self._bound(marks.get(n), None, values[n])
                      for n in sets]
            self._run(cur, f"update {quoted} set "
                           + ", ".join(f"{self._q(n)} = {p}"
                                       for n, (p, _) in zip(sets, placed))
                           + f" where {where}",
                      [a for _, a in placed] + kargs)
            if cur.rowcount is not None and cur.rowcount > 0:
                return
        if not sets or cur.rowcount is None or cur.rowcount < 0:
            # nothing to set, or a driver that does not count: asked
            self._run(cur, f"select count(*) from {quoted} where {where}",
                      kargs)
            if cur.fetchone()[0]:
                return
        row = {**key, **values}
        marks = dict(zip(row, self._value_marks(side, db, table, list(row))
                         or ()))
        placed = [self._bound(marks.get(n), None, v) for n, v in row.items()]
        self._run(cur, f"insert into {quoted}"
                       f" ({', '.join(self._q(n) for n in row)}) values"
                       f" ({', '.join(p for p, _ in placed)})",
                  [a for _, a in placed])

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

    def neutral_read(self, side, db, table, columns, after=None, limit=None,
                     where=None):
        rows, last = super().neutral_read(side, db, table, columns, after,
                                          limit, where)
        return self._unpad(side, db, table, columns, rows), last

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        for batch in super().neutral_batches(side, db, table, columns, size,
                                             where):
            yield self._unpad(side, db, table, columns, batch)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        # read back as a read reads them: a write checked by its keys
        # otherwise finds 0.10 written and 0.1 there
        got = super().neutral_rows_by_key(side, db, table, columns, key,
                                          keys, where)
        return self._by_key_map(columns, key, self._unpad(
            side, db, table, columns, [list(r) for r in got.values()]))

    def neutral_rows_matching(self, side, db, table, columns, match, values):
        return self._unpad(side, db, table, columns, [
            list(r) for r in super().neutral_rows_matching(
                side, db, table, columns, match, values)])


# ---- the other SQL sources: dialects of the generic engine -----------------
#
# Netezza, SAP HANA, Informix, Progress OpenEdge, Vertica, Exasol and
# Databricks SQL answer the standard driver interface and a catalogue of
# their own. Each is a row of `DIALECTS` - how it quotes and folds a name,
# pages a read, names its types, asks its catalogue, and loads a table in
# bulk where a client reaches a way - read by the one connection and the one
# engine below; the generic engine's copy, follow, fence, compare and scope
# run on them unchanged. A driver is imported when a side is opened, and the
# one missing is named.

#: the declared types every dialect shares, by name without its size
STANDARD_TYPES = {
    "int": "integer", "integer": "integer", "smallint": "integer",
    "bigint": "integer", "tinyint": "integer", "serial": "integer",
    "numeric": "decimal", "decimal": "decimal", "dec": "decimal",
    "number": "decimal", "money": "decimal",
    "float": "float", "double": "float", "double precision": "float",
    "real": "float", "smallfloat": "float",
    "char": "text", "character": "text", "varchar": "text",
    "nchar": "text", "nvarchar": "text", "varchar2": "text",
    "text": "text", "string": "text", "clob": "text", "nclob": "text",
    "long varchar": "text", "character varying": "text",
    "boolean": "boolean", "bool": "boolean", "bit": "boolean",
    "timestamp": "timestamp", "datetime": "timestamp",
    "timestamp without time zone": "timestamp", "timestamp_ntz": "timestamp",
    "timestamp with time zone": "timestamptz", "timestamptz": "timestamptz",
    "timestamp_ltz": "timestamptz", "date": "date"}


@dataclass(frozen=True)
class Bulk:
    """A dialect's bulk way: `kind` is `client`, `landing` or `tool`
    (`decide.dialect_bulk`); `sql` the statement, over `{table}`,
    `{columns}` and `{where}` - the file on this machine (`client`, `tool`)
    or the landed object's address (`landing`)."""
    kind: str
    sql: str
    tool: str = ""


@dataclass(frozen=True)
class Dialect:
    name: str
    title: str
    #: the driver's module, and what installs it
    driver: str
    pip: str
    port: int
    #: opens the driver's connection: f(module, host, port, user, password,
    #: database)
    connect: object
    #: how a name is written: its quote, and how an unquoted name folds
    #: (`upper`, or "" where it is kept as written)
    quote_mark: str = '"'
    fold: str = ""
    #: how a read is limited: `limit` (a trailing LIMIT n OFFSET m), `first`
    #: (SELECT FIRST n) or `top` (SELECT TOP n)
    paging: str = "limit"
    types: tuple = ()
    #: the catalogue's questions, `{schema}` and `{table}` as literals: the
    #: base tables, a table's columns as (name, declared type, datetime
    #: precision, numeric precision, numeric scale), the columns the table's
    #: primary key holds, the session's schema, and whether the account may
    #: run the bulk way (a count above zero)
    tables_sql: str = ""
    columns_sql: str = ""
    unique_sql: str = ""
    schema_sql: str = ""
    schema_default: str = ""
    may_sql: str = ""
    system_schemas: tuple = ()
    bulk: object = None
    #: why a dialect has no bulk way a client reaches
    no_bulk: str = ""

    # ---- names, paging and types, as `conn.dialect` -----------------------

    def stored(self, name):
        """The name as the catalogue holds it: an unquoted lower-case name
        folds to capitals where the dialect folds."""
        name = str(name)
        return name.upper() if self.fold == "upper" and \
            name == name.lower() else name

    def quote(self, name):
        mark = self.quote_mark
        return mark + self.stored(name).replace(mark, mark * 2) + mark

    def offset_limit(self, offset=None, limit=None):
        """The trailing clause of a read limited to `limit` rows after
        `offset`; "" for a dialect that limits in front (`page`)."""
        if self.paging != "limit":
            return ""
        return (f"LIMIT {int(limit)}" if limit is not None else "") + \
            (f" OFFSET {int(offset)}" if offset else "")

    def page(self, select, offset=None, limit=None):
        """`select` ("SELECT cols FROM t ...") limited in this dialect's
        way."""
        if limit is None and not offset:
            return select
        if self.paging == "limit":
            return f"{select} {self.offset_limit(offset, limit)}"
        word = "FIRST" if self.paging == "first" else "TOP"
        skip = f"SKIP {int(offset)} " if offset and self.paging == "first" \
            else ""
        head, _, rest = select.partition(" ")
        return f"{head} {skip}{word} {int(limit)} {rest}"

    def kind_of(self, declared):
        table = {**STANDARD_TYPES, **dict(self.types)}
        base = re.sub(r"\(.*", "", str(declared)).strip().lower()
        return table.get(base)

    def parse_type(self, path, name, declared, dt_prec=None, num_prec=None,
                   num_scale=None):
        """The column's type as the classes every dialect shares."""
        from sqeleton.abcs import database_types as t
        kind = self.kind_of(declared)
        sized = re.search(r"\((\d+)(?:\s*,\s*(-?\d+))?\)", str(declared))
        if sized:
            num_prec = num_prec if num_prec is not None else int(sized[1])
            if num_scale is None and sized[2] is not None:
                num_scale = int(sized[2])
        if kind == "integer":
            return t.Integer()
        if kind == "decimal":
            return t.Decimal(precision=num_scale or 0)
        if kind == "float":
            return t.Float(precision=15)
        if kind == "text":
            return t.Text()
        if kind == "boolean":
            return t.Boolean()
        if kind in ("timestamp", "timestamptz", "date"):
            cls = {"timestamp": t.Timestamp, "timestamptz": t.TimestampTZ,
                   "date": t.Date}[kind]
            return cls(precision=dt_prec if dt_prec is not None else 6,
                       rounds=False)
        return t.UnknownColType(str(declared))

    # ---- opening a side ----------------------------------------------------

    def load_driver(self):
        """The driver's module, or a stop naming the one missing."""
        import importlib
        try:
            return importlib.import_module(self.driver)
        except ImportError:
            raise SystemExit(
                f"{self.title} needs its Python driver, `{self.pip}`, and"
                f" it is not installed: pip install {self.pip}. Nothing was"
                " read") from None

    def open(self, url, side):
        """A `DialectConnection` on the address `url`."""
        from urllib.parse import unquote, urlsplit
        module = self.load_driver()
        parts = urlsplit(url)
        try:
            raw = self.connect(module, parts.hostname or "",
                               parts.port or self.port,
                               unquote(parts.username or ""),
                               unquote(parts.password or ""),
                               (parts.path or "").lstrip("/"))
        except Exception as e:  # noqa: BLE001 - said, not raised raw
            raise SystemExit(
                f"cannot open the {side} {self.title} address:"
                f" {(str(e).splitlines() or [''])[-1][:160]}")
        return DialectConnection(self, raw)


def _literal(value):
    return "'" + str(value).replace("'", "''") + "'"


class DialectConnection:
    """A driver's connection answering what the generic engine asks of one:
    statements, the catalogue's tables and columns, a table's name, and the
    dialect that writes a name, a type and a page."""

    def __init__(self, dialect, raw):
        self.dialect = dialect
        self._conn = raw
        self._schema = None

    @property
    def default_schema(self):
        if self._schema is None:
            self._schema = self.dialect.schema_default
            if self.dialect.schema_sql:
                try:
                    got = self.query(self.dialect.schema_sql, list)
                    self._schema = str(got[0][0]).strip() if got and \
                        got[0][0] else self._schema
                except Exception:  # noqa: BLE001 - the dialect's default
                    pass
        return self._schema or None

    def parse_table_name(self, name):
        return tuple(p.strip(self.dialect.quote_mark)
                     for p in str(name).split("."))

    def query(self, sql, kind=None):
        """The statement's rows (`list`), or None for one that returns
        none; a statement that writes is committed by `commit`."""
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            if kind is list or cur.description:
                return [tuple(r) for r in cur.fetchall()]
            return None
        finally:
            cur.close()

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    def _names(self, path):
        schema, table = (path if len(path) > 1 else (None, path[0]))[-2:]
        return {"schema": _literal(self.dialect.stored(schema))
                if schema else "NULL",
                "table": _literal(self.dialect.stored(table))}

    def select_table_schema(self, path):
        return self.dialect.columns_sql.format(**self._names(path))

    def query_table_schema(self, path):
        """{column: (name, declared type, datetime precision, numeric
        precision, numeric scale)}, in the table's column order."""
        return {str(r[0]): tuple(r[:5]) for r in
                self.query(self.select_table_schema(path), list)}

    def _process_table_schema(self, path, raw):
        return {row[0]: self.dialect.parse_type(path, *row)
                for row in raw.values()}

    def query_table_unique_columns(self, path):
        if not self.dialect.unique_sql:
            return []
        return [str(r[0]) for r in self.query(
            self.dialect.unique_sql.format(**self._names(path)), list)]


def _hdb(m, host, port, user, password, db):
    return m.connect(address=host, port=port, user=user, password=password)


def _nz(m, host, port, user, password, db):
    return m.connect(user=user, password=password, host=host, port=port,
                     database=db)


def _vertica(m, host, port, user, password, db):
    return m.connect(host=host, port=port, user=user, password=password,
                     database=db)


def _exasol(m, host, port, user, password, db):
    return m.connect(dsn=f"{host}:{port}", user=user, password=password,
                     schema=db or "")


def _informix(m, host, port, user, password, db):
    return m.connect(f"SERVER={db.split('/')[0]};DATABASE="
                     f"{db.split('/')[-1]};HOST={host};SERVICE={port};"
                     f"UID={user};PWD={password};")


def _openedge(m, host, port, user, password, db):
    return m.connect(f"DRIVER={{Progress OpenEdge Wire Protocol}};HOST="
                     f"{host};PORT={port};DB={db};UID={user};PWD={password}")


def _databricks(m, host, port, user, password, db):
    return m.connect(server_hostname=host, http_path="/" + db,
                     access_token=password)


#: the seven dialects, one row each
DIALECTS = {d.name: d for d in (
    Dialect(
        "netezza", "Netezza", "nzpy", "nzpy", 5480, _nz, fold="upper",
        types=(("byteint", "integer"), ("nvarchar", "text")),
        tables_sql="SELECT schema, tablename FROM _V_TABLE WHERE objtype ="
                   " 'TABLE'",
        columns_sql="SELECT attname, format_type, NULL, NULL, NULL FROM"
                    " _V_RELATION_COLUMN WHERE schema = {schema} AND name ="
                    " {table} ORDER BY attnum",
        unique_sql="SELECT attname FROM _V_RELATION_KEYDATA WHERE schema ="
                   " {schema} AND relation = {table} AND contype = 'p'",
        schema_sql="SELECT CURRENT_SCHEMA",
        system_schemas=("definition_schema", "information_schema"),
        bulk=Bulk("client", "INSERT INTO {table} ({columns}) SELECT * FROM"
                            " EXTERNAL '{where}' USING (REMOTESOURCE"
                            " 'python' DELIMITER ',' NULLVALUE '\\N'"
                            " ESCAPECHAR '\\')")),
    Dialect(
        "saphana", "SAP HANA", "hdbcli.dbapi", "hdbcli", 30015, _hdb,
        fold="upper", types=(("seconddate", "timestamp"),
                             ("nvarchar", "text"), ("alphanum", "text")),
        tables_sql="SELECT schema_name, table_name FROM SYS.TABLES WHERE"
                   " is_user_defined_type = 'FALSE' AND is_temporary ="
                   " 'FALSE'",
        columns_sql="SELECT column_name, data_type_name, NULL, length, scale"
                    " FROM SYS.TABLE_COLUMNS WHERE schema_name = {schema} AND"
                    " table_name = {table} ORDER BY position",
        unique_sql="SELECT column_name FROM SYS.CONSTRAINTS WHERE schema_name"
                   " = {schema} AND table_name = {table} AND is_primary_key ="
                   " 'TRUE'",
        schema_sql="SELECT CURRENT_SCHEMA FROM DUMMY",
        may_sql="SELECT COUNT(*) FROM SYS.EFFECTIVE_PRIVILEGES WHERE"
                " user_name = CURRENT_USER AND privilege = 'IMPORT'",
        system_schemas=("sys", "_sys_bi", "_sys_repo", "_sys_statistics",
                        "_sys_xs", "hdb_catalog", "sap_hana_ems"),
        bulk=Bulk("landing", "IMPORT FROM CSV FILE '{where}' INTO {table}"
                             " WITH RECORD DELIMITED BY '\\n' FIELD"
                             " DELIMITED BY ',' OPTIONALLY ENCLOSED BY '\"'"
                             " ERROR LOG '{where}.err'")),
    Dialect(
        "informix", "Informix", "IfxPyDbi", "IfxPy", 9088, _informix,
        paging="first",
        types=(("0", "text"), ("1", "integer"), ("2", "integer"),
               ("3", "float"), ("4", "float"), ("5", "decimal"),
               ("6", "integer"), ("7", "date"), ("8", "decimal"),
               ("10", "timestamp"), ("13", "text"), ("15", "text"),
               ("16", "text"), ("17", "integer"), ("18", "integer"),
               ("45", "boolean"), ("52", "integer"), ("53", "integer")),
        tables_sql="SELECT TRIM(owner), tabname FROM systables WHERE tabtype"
                   " = 'T' AND tabid >= 100",
        columns_sql="SELECT c.colname, MOD(c.coltype, 256), NULL,"
                    " c.collength, NULL FROM syscolumns c JOIN systables t ON"
                    " t.tabid = c.tabid WHERE TRIM(t.owner) = {schema} AND"
                    " t.tabname = {table} ORDER BY c.colno",
        unique_sql="SELECT c.colname FROM sysconstraints k JOIN systables t"
                   " ON t.tabid = k.tabid JOIN sysindexes i ON i.tabid ="
                   " k.tabid AND i.idxname = k.idxname JOIN syscolumns c ON"
                   " c.tabid = i.tabid AND c.colno = ABS(i.part1) WHERE"
                   " k.constrtype = 'P' AND TRIM(t.owner) = {schema} AND"
                   " t.tabname = {table}",
        may_sql="SELECT COUNT(*) FROM sysusers WHERE usertype IN ('D', 'R')"
                " AND username = USER",
        system_schemas=("informix",),
        bulk=Bulk("tool", "LOAD FROM '{where}' DELIMITER ',' INSERT INTO"
                          " {table} ({columns})", tool="dbaccess")),
    Dialect(
        "progress", "Progress OpenEdge", "pyodbc", "pyodbc", 20931,
        _openedge, fold="upper", paging="top", schema_default="PUB",
        tables_sql="SELECT owner, tbl FROM sysprogress.systables WHERE"
                   " tbltype = 'T'",
        columns_sql="SELECT col, coltype, NULL, width, scale FROM"
                    " sysprogress.syscolumns WHERE owner = {schema} AND tbl ="
                    " {table} ORDER BY id",
        system_schemas=("sysprogress",),
        no_bulk="OpenEdge SQL has no bulk loader a client reaches"),
    Dialect(
        "vertica", "Vertica", "vertica_python", "vertica-python", 5433,
        _vertica, types=(("long varchar", "text"), ("int8", "integer"),
                         ("float8", "float"), ("numeric", "decimal")),
        tables_sql="SELECT table_schema, table_name FROM v_catalog.tables",
        columns_sql="SELECT column_name, data_type, datetime_precision,"
                    " numeric_precision, numeric_scale FROM"
                    " v_catalog.columns WHERE table_schema = {schema} AND"
                    " table_name = {table} ORDER BY ordinal_position",
        unique_sql="SELECT column_name FROM v_catalog.primary_keys WHERE"
                   " table_schema = {schema} AND table_name = {table}",
        schema_sql="SELECT CURRENT_SCHEMA()",
        may_sql="SELECT COUNT(*) FROM v_catalog.table_privileges WHERE"
                " grantee = CURRENT_USER() AND privileges_description LIKE"
                " '%INSERT%'",
        system_schemas=("v_catalog", "v_monitor", "v_internal"),
        bulk=Bulk("client", "COPY {table} ({columns}) FROM LOCAL '{where}'"
                            " DELIMITER ',' NULL '\\N' ENCLOSED BY '\"'"
                            " REJECTMAX 0 ABORT ON ERROR")),
    Dialect(
        "exasol", "Exasol", "pyexasol.db2", "pyexasol", 8563, _exasol,
        fold="upper", types=(("decimal", "decimal"),
                             ("timestamp with local time zone",
                              "timestamptz")),
        tables_sql="SELECT table_schema, table_name FROM SYS.EXA_ALL_TABLES",
        columns_sql="SELECT column_name, column_type, NULL, column_num_prec,"
                    " column_num_scale FROM SYS.EXA_ALL_COLUMNS WHERE"
                    " column_schema = {schema} AND column_table = {table}"
                    " ORDER BY column_ordinal_position",
        unique_sql="SELECT column_name FROM SYS.EXA_ALL_CONSTRAINT_COLUMNS"
                   " WHERE constraint_schema = {schema} AND constraint_table"
                   " = {table} AND constraint_type = 'PRIMARY KEY'",
        schema_sql="SELECT CURRENT_SCHEMA",
        may_sql="SELECT COUNT(*) FROM SYS.EXA_ALL_OBJ_PRIVS WHERE grantee ="
                " CURRENT_USER AND privilege = 'INSERT'",
        system_schemas=("sys", "exa_statistics"),
        bulk=Bulk("landing", "IMPORT INTO {table} ({columns}) FROM CSV AT"
                             " '{where}' COLUMN SEPARATOR = ',' COLUMN"
                             " DELIMITER = '\"' NULL = '\\N' REJECT LIMIT 0")),
    Dialect(
        "databricks", "Databricks", "databricks.sql", "databricks-sql-connector",
        443, _databricks, quote_mark="`",
        types=(("tinyint", "integer"), ("timestamp_ntz", "timestamp"),
               ("string", "text")),
        tables_sql="SELECT table_schema, table_name FROM"
                   " information_schema.tables WHERE table_type IN"
                   " ('MANAGED', 'EXTERNAL', 'BASE TABLE')",
        columns_sql="SELECT column_name, data_type, datetime_precision,"
                    " numeric_precision, numeric_scale FROM"
                    " information_schema.columns WHERE table_schema ="
                    " {schema} AND table_name = {table} ORDER BY"
                    " ordinal_position",
        unique_sql="SELECT k.column_name FROM"
                   " information_schema.key_column_usage k JOIN"
                   " information_schema.table_constraints c ON"
                   " c.constraint_name = k.constraint_name AND"
                   " c.table_schema = k.table_schema AND c.table_name ="
                   " k.table_name WHERE c.constraint_type = 'PRIMARY KEY' AND"
                   " k.table_schema = {schema} AND k.table_name = {table}",
        schema_sql="SELECT current_schema()",
        may_sql="SELECT COUNT(*) FROM information_schema.table_privileges"
                " WHERE grantee = current_user() AND privilege_type ="
                " 'MODIFY'",
        system_schemas=("information_schema",),
        bulk=Bulk("landing", "COPY INTO {table} FROM '{where}' FILEFORMAT ="
                             " CSV FORMAT_OPTIONS ('nullValue' = '\\\\N',"
                             " 'quote' = '\"', 'escape' = '\"')")),
)}


#: why the dialects' ways are owed a run (P0-PARITY.small, W5): each is
#: written from its vendor's documented catalogue and statements and run
#: against a driver stand-in, never a real server
UNPROVED_ON = ("built; not run on a real Netezza, SAP HANA, Informix,"
               " OpenEdge, Vertica, Exasol or Databricks server")
#: the cells built here and run against no such server
UNPROVED_ITEMS = (
    "tail.quiet "
    "assess.engine-checks",
    "move.table-copy",
    "scope.copy",
    "scope.deep",
    "scope.listings",
    "scope.names",
    "scope.objects",
    "scope.ops",
    "scope.repair",
    "scope.row-filter",
    "scope.transforms",
    "tail.follows",
    "verify.confirm",
    "verify.delta",
    "verify.fence",
    "verify.repair",
    "verify.structure.foreign-keys",
    "verify.structure.indexes",
    "verify.structure.keys",
    "verify.structure.objects",
    "verify.value.null-empty",
)


class DialectEngine(GenericEngine):
    """A SQL source that is a dialect of the generic engine: its connection
    is the driver's, its names, types, pages and catalogue are the row of
    `DIALECTS` its subclass names, and a table goes in by its bulk way where
    the climb (`decide.dialect_bulk`) finds one that holds."""
    #: the dialect's row in `DIALECTS`
    DIALECT = ""

    def tail_where(self, db, token):
        """What the tail's quiet line stands on (`tailctl.where_from`): the
        tail's position and the source's now (`log_position`). The tail reads the tables'
        changes by a column it keeps, not a log: the source keeps no count
        of its writes to read and no reader of the tail's to list - both
        are said unread, never assumed."""
        from .. import tailctl
        return tailctl.where_from(self, db, token)

    def _dialect(self):
        return DIALECTS[self.DIALECT]

    @property
    def CATALOGUE_TABLES(self):  # noqa: N802 - the generic engine's name
        return self._dialect().tables_sql

    @property
    def SYSTEM_SCHEMAS(self):  # noqa: N802 - the generic engine's name
        return GenericEngine.SYSTEM_SCHEMAS | frozenset(
            self._dialect().system_schemas)

    def _connect(self, side):
        if not hasattr(self, "_conns"):
            self._conns = {}
        if side not in self._conns:
            self._conns[side] = self._dialect().open(self._url(side), side)
        return self._conns[side]

    def _apply_rows_borrowed(self, db, action):
        """A repair writes through the query builder of the library the
        generic engine borrows its connection from, which a dialect's own
        connection does not carry: refused with the reason, nothing
        written, never half done."""
        raise SystemExit(
            f"{db}: a repair of rows on {self._dialect().title} is not"
            " written yet: the rows that differ are reported, and nothing"
            " was changed")

    # ---- the bulk way ----------------------------------------------------

    def _landing(self):
        return (self.hop.target.options or {}).get("stage") or ""

    def _count(self, side, table):
        conn = self._connect(side)
        return int(conn.query(f"SELECT count(*) FROM {self._from(conn, table)}",
                              list)[0][0])

    def _may_bulk(self):
        """Whether the target's account may run the bulk way: True, False,
        or None where the dialect's grants were not read."""
        sql = self._dialect().may_sql
        if not sql:
            return None
        try:
            return bool(self._connect("dst").query(sql, list)[0][0])
        except Exception:  # noqa: BLE001 - not read: said as not read
            return None

    def bulk_way(self, table, log=None):
        """The `Choice` for loading `table`, every need a fact asked of the
        hop, this machine and the servers."""
        from .. import decide
        from ..util import which
        from . import analyticdb
        row = self._dialect()
        way = row.bulk.kind if row.bulk else None
        land = self._landing() if way == "landing" else ""
        facts = {"way": way, "none_why": row.no_bulk, "landing": land,
                 "writer": bool(land) and analyticdb.landing_writer(land),
                 "streams": False, "streams_why": None,
                 "tool": bool(row.bulk and row.bulk.tool
                              and which(row.bulk.tool)),
                 "may": None, "rows": None, "empty": None}
        if way == "client":
            # the client way is the driver's own: it takes it where its
            # module loads on this machine, and says why where it does not
            try:
                row.load_driver()
                facts["streams"] = True
            except SystemExit as e:
                facts["streams_why"] = str(e).split(". Nothing", 1)[0]
        if way and (way != "landing" or facts["writer"]) and \
                (way != "tool" or facts["tool"]) and \
                (way != "client" or facts["streams"]):
            facts["may"] = self._may_bulk()
            if facts["may"]:
                facts["rows"] = self._count("src", table)
                facts["empty"] = not self._count("dst", self._on("dst", table))
        choice = decide.dialect_bulk(facts)
        decide.record(self.hop, choice, log, work=f"the load of {table}",
                      way=choice.said, reason=choice.reason)
        return choice

    def _bulk_statement(self, table, columns, where):
        conn = self._connect("dst")
        return self._dialect().bulk.sql.format(
            table=self._from(conn, self._on("dst", table)), where=where,
            columns=", ".join(conn.dialect.quote(c) for c in columns))

    def _source_batches(self, table, columns, size):
        """The source's rows of `table`, its columns in `columns`' order and
        masked as the target holds them, `size` rows a fetch from one
        cursor: never the whole table in memory."""
        conn = self._connect("src")
        cols = ", ".join(conn.dialect.quote(c) for c in columns)
        pred = self._filter(table)
        cur = conn._conn.cursor()
        try:
            try:
                cur.execute(f"SELECT {cols} FROM {self._from(conn, self._on('src', table))}"
                            + (f" WHERE {pred}" if pred else ""))
            except Exception as e:  # noqa: BLE001 - said as its grant
                self._grant_refused("src", "reading rows", table, e)
                raise SystemExit(f"cannot read {table} on the source:"
                                 f" {str(e).splitlines()[-1][:160]}")
            while True:
                rows = cur.fetchmany(size)
                if not rows:
                    return
                yield self._masked(table, list(columns),
                                   [tuple(r) for r in rows])
        finally:
            cur.close()

    def _land_source(self, table, columns, kind, chunk=None):
        """The source's rows as one CSV: an object in the landing store
        (`landing`: its address), a file on this machine otherwise; and
        the rows written, counted as each batch of the source's cursor is
        (`chunk` rows a fetch, the copier's batch otherwise)."""
        import csv
        import tempfile

        from . import analyticdb
        size = max(1, int(chunk or self.BATCH_ROWS))
        wrote = [0]

        def rows():
            for batch in self._source_batches(table, columns, size):
                for r in batch:
                    wrote[0] += 1
                    yield r
        if kind == "landing":
            where = analyticdb.land_csv(self._landing(), table.replace(
                ".", "_"), rows(), self.hop.target.options or {})
            return where, wrote[0]
        with tempfile.NamedTemporaryFile(
                "w", newline="", suffix=".csv", delete=False) as fh:
            out = csv.writer(fh, lineterminator="\n")
            for r in rows():
                out.writerow([analyticdb.landing_text(v) for v in r])
        return fh.name, wrote[0]

    def _run_bulk(self, kind, statement):
        conn = self._connect("dst")
        if kind == "tool":
            import subprocess
            tool = self._dialect().bulk.tool
            done = subprocess.run([tool, self._url("dst").rsplit("/", 1)[-1],
                                   "-"], input=statement, text=True,
                                  capture_output=True)
            if done.returncode:
                raise SystemExit("the bulk load failed:"
                                 f" {(done.stderr or '').strip()[-160:]}")
            return
        conn.query(statement)
        conn.commit()

    def move_table(self, db, sch, tbl, chunk, ck, log):
        """The table by the dialect's bulk way where the climb takes one,
        the generic copier otherwise. The bulk way loads an empty table in
        one statement and the rows landed are counted against the source's
        before the table is marked done: a load cut off leaves the target
        not empty, so the next run's copier reconciles it by key and never
        loads the rows twice."""
        import os
        table = f"{sch}.{tbl}" if sch else tbl
        choice = self.bulk_way(table, log)
        if choice.path in (None, "copier"):
            return super().move_table(db, sch, tbl, chunk, ck, log)
        st = ck.setdefault(self.move_key(db, sch, tbl), {})
        if st.get("done"):
            return None
        columns = self._copy_columns(table)
        where, rows = self._land_source(table, columns, choice.path, chunk)
        try:
            self._run_bulk(choice.path, self._bulk_statement(
                table, columns, where))
            landed = self._count("dst", self._on("dst", table))
        finally:
            if choice.path != "landing":
                os.unlink(where)
            self._close()
        if landed != rows:
            raise SystemExit(f"{table}: the bulk load left {landed} rows on"
                             f" the target for {rows} on the source; the"
                             " table is not marked done and the next run"
                             " reconciles it by key")
        st["done"], st["rows"] = True, rows
        ck.save()
        if log:
            log(f"{table}: {rows} rows loaded by the {choice.path} bulk way")
        return None


class NetezzaEngine(DialectEngine):
    DIALECT = "netezza"


class SapHanaEngine(DialectEngine):
    DIALECT = "saphana"


class InformixEngine(DialectEngine):
    DIALECT = "informix"


class ProgressEngine(DialectEngine):
    DIALECT = "progress"


class VerticaEngine(DialectEngine):
    DIALECT = "vertica"


class ExasolEngine(DialectEngine):
    DIALECT = "exasol"


class DatabricksEngine(DialectEngine):
    DIALECT = "databricks"
