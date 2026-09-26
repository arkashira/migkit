import difflib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from ..util import keepalive as _keepalive, run, which, with_retry
from .base import Engine, RepairAction, Result

SKIP_DBS = {"mysql", "sys", "performance_schema", "information_schema"}


class MySQLEngine(Engine):
    ENGINE_FAMILY = "mysql"
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = True

    #: The sql_mode every write to the target runs under, whatever the
    #: server's own is: strict, so a value the column cannot hold stops the
    #: write instead of being cut to fit, and without `NO_ZERO_DATE` and
    #: `NO_ZERO_IN_DATE`, so a zero date the source holds lands as it is.
    #: `NO_AUTO_VALUE_ON_ZERO` keeps a key of 0 a 0. Measured on a target
    #: whose own mode was lax (`NO_ENGINE_SUBSTITUTION`, a managed 5.7's
    #: default): the table copier wrote `'12345678901'` into a varchar(5)
    #: as `'12345'` and said nothing.
    WRITE_SQL_MODE = "NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES"

    def _conn(self, side, retry=True):
        ep = self.hop.source if side == "src" else self.hop.target
        try:
            import pymysql
        except ImportError:
            raise SystemExit("pip install 'migkit[mysql]' for mysql support")
        # read_timeout + TCP keepalive: without them a network blip on a
        # cross-cloud link leaves the socket open forever and a long scan
        # (count on a hundred-GB table) hangs the whole run with no error.
        import os
        rt = int(os.environ.get("MIGKIT_READ_TIMEOUT", "3600"))

        def _open():
            # the greeting is read under the read timeout, so a server that
            # does not speak MySQL - measured, a PostgreSQL port - held the
            # connect for the whole hour of it; the handshake gets the
            # connect's 15 seconds and the queries after it the hour
            c = pymysql.connect(host=ep.host, port=ep.port, user=ep.user,
                                password=ep.password, charset="utf8mb4",
                                connect_timeout=15,
                                read_timeout=15, write_timeout=rt,
                                init_command=(
                                    "set session sql_mode ="
                                    f" '{self.WRITE_SQL_MODE}'"
                                    if side == "dst" else None))
            c._read_timeout = rt
            _keepalive(getattr(c, "_sock", None))
            return c
        # a probe that only decides an option asks once: retrying a server
        # that is not there only delays saying "no"
        return (with_retry(_open, label=f"mysql connect {side}") if retry
                else _open())

    CANON_ENGINE = "mysql"
    OWN_PATHS_READ_COLUMN_MAPPING = False

    def neutral_tables(self, side, db):
        return self._tables(side, db)

    def run_rule(self, side, db, sql):
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.execute(f"use {self._my_ident(self._d(side, db))}")
                # read-only: a rule that tries to write fails, and the
                # source is not written to whatever the rule says
                cur.execute("start transaction read only")
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.rollback()
            conn.close()

    @staticmethod
    def _my_ident(name):
        return "`" + str(name).replace("`", "``") + "`"

    #: the account migkit's own replica signs in with (`replicate_sql`) -
    #: what tells it apart from a replica somebody else set up
    REPL_USER = "migkit_repl"

    def stream_writers(self, db):
        """Adds the target's replica applier, when its SQL thread runs. The
        one migkit set up signs in as its own account and can be paused for
        a repair; any other cannot."""
        out = super().stream_writers(db)
        for row in self._q_named("dst", "show replica status"):
            sql = self._repl_field(row, self._REPL_FIELDS["sql"])
            host = self._repl_field(row, self._REPL_FIELDS["host"])
            user = self._repl_field(row, self._REPL_FIELDS["user"])
            if str(sql).lower() == "yes":
                out.append((f"the replica applying from {host}",
                            str(user) == self.REPL_USER))
        return out

    def pause_writer(self, db, what, window):
        """migkit's own replica, paused for a repair.

        First it catches up with the source as it is now, so that nothing
        the repair is about to write is still on its way: a change still
        in the relay log, applied after the repair, would put back a value
        older than the one the repair read. Then its applier stops. The
        receiver keeps fetching, so what the source writes meanwhile waits
        in the relay log and is applied on top of the repair when the
        applier starts again."""
        if not what.startswith("the replica applying from "):
            return super().pause_writer(db, what, window)
        if not dict(self.stream_writers(db)).get(what):
            raise ValueError(f"{what} is not migkit's")
        if self._replica_caught_up(db, window) is not True:
            return False
        mariadb = self._brands()[1].name == "mariadb"
        self._q("dst", "stop slave sql_thread" if mariadb
                else "stop replica sql_thread")
        return True

    def resume_writer(self, db, what):
        if not what.startswith("the replica applying from "):
            return super().resume_writer(db, what)
        mariadb = self._brands()[1].name == "mariadb"
        self._q("dst", "start slave sql_thread" if mariadb
                else "start replica sql_thread")

    def _replica_caught_up(self, db, window):
        """Wait until the target has applied what the source has written
        now: by GTID where the source runs it, by its binary log file and
        position otherwise. True, False on time out, None where there is
        nothing to wait on."""
        got = self.fence_wait(db, self.src_lsn(db), timeout=window)
        if got is not None:
            return got
        pos = self._binlog_position("src")
        if not pos:
            return None
        ask = ("select master_pos_wait(%s, %s, 5)"
               if self._brands()[1].name == "mariadb"
               else "select source_pos_wait(%s, %s, 5)")
        began = time.monotonic()
        while time.monotonic() - began < window:
            r = self._q("dst", ask, (pos[0], int(pos[1])))
            if r and r[0][0] is not None and int(r[0][0]) >= 0:
                return True
            if r and r[0][0] is None:
                # the applier is not running, or this is no replica
                return None
        return False

    def table_facts(self, side, db):
        """InnoDB's own row estimate and whether a primary key exists."""
        out = {}
        for name, rows, key, size, idx in self._q(
                side, "select t.table_name, t.table_rows,"
                      " exists(select 1 from information_schema"
                      ".table_constraints c where c.table_schema ="
                      " t.table_schema and c.table_name = t.table_name"
                      " and c.constraint_type = 'PRIMARY KEY'),"
                      " t.data_length, t.index_length"
                      " from information_schema.tables t"
                      " where t.table_schema = %s"
                      " and t.table_type in ('BASE TABLE', 'SYSTEM VERSIONED')",
                (self._d(side, db),)):
            out[name] = {"rows": None if rows is None else int(rows),
                         "key": bool(key),
                         "bytes": None if size is None else int(size),
                         "index_bytes": None if idx is None else int(idx)}
        return out

    def column_catalog(self, side, db):
        """{table: [(column, type), ...]} in one query - what a move reads
        before and after itself to notice a DDL on the source."""
        out = {}
        for t, c, ty in self._q(side, "select table_name, column_name,"
                                      " column_type from"
                                      " information_schema.columns"
                                      " where table_schema = %s",
                                (self._d(side, db),)):
            out.setdefault(str(t), []).append((str(c), str(ty)))
        return {t: sorted(cols) for t, cols in out.items()}

    def neutral_columns(self, side, db, table):
        rows = self._q(side, "select column_name, column_type"
                             " from information_schema.columns"
                             " where table_schema=%s and table_name=%s"
                             " order by ordinal_position",
                       (self._d(side, db), table))
        return [(r[0], r[1]) for r in rows]

    def neutral_key(self, side, db, table):
        return [r[0] for r in self._q(side, self.PK_SQL,
                                      (self._d(side, db), table))]

    def _read_query(self, side, db, table, columns, after, limit, where):
        """(sql, args, key, names) for `neutral_read` and
        `neutral_batches`: one statement shape for both."""
        names = [n for n, _ in columns]
        cols = ", ".join(f"`{n}`" for n in names)
        key = self.neutral_key(side, db, table)
        resume, args = "", []
        if key and after is not None:
            places = ", ".join(["%s"] * len(key))
            keys = ", ".join(f"`{k}`" for k in key)
            resume = f"({keys}) > ({places})"
            args = list(after)
        where = self._where(where, resume, bool(args), percent=True)
        order = (" order by " + ", ".join(f"`{k}`" for k in key)) if key else ""
        cap = f" limit {int(limit)}" if key and limit else ""
        return (f"select {cols} from `{self._d(side, db)}`.`{table}`"
                f"{where}{order}{cap}", args or None, key, names)

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        """The whole table in one pass, `size` rows at a time, for a table
        with no key to resume from - read through a cursor the server keeps,
        so no more than a batch is ever held here."""
        import pymysql
        sql, args, _, _ = self._read_query(side, db, table, columns, None,
                                           None, where)
        conn = self._conn(side)
        try:
            with conn.cursor(pymysql.cursors.SSCursor) as cur:
                cur.execute(sql, args)
                while True:
                    rows = cur.fetchmany(size)
                    if not rows:
                        break
                    yield [list(r) for r in rows]
        finally:
            conn.close()

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        sql, args, key, names = self._read_query(side, db, table, columns,
                                                 after, limit, where)
        rows = [list(r) for r in self._q(side, sql, args)]
        if not rows or not key:
            return (rows, None)
        idx = [names.index(k) for k in key if k in names]
        if len(idx) != len(key):
            return (rows, None)
        return (rows, tuple(rows[-1][i] for i in idx))

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        sql, args = self._by_key_query(
            f"`{self._d(side, db)}`.`{table}`", columns, key, list(keys),
            lambda n: f"`{n}`", "%s",
            where.replace("%", "%%") if where else None)
        rows = [list(r) for r in self._q(side, sql, args)]
        return self._by_key_map(columns, key, rows)

    def neutral_write(self, side, db, table, columns, rows):
        if not rows:
            return 0
        from .. import canon
        names = [n for n, _ in columns]
        cols = ", ".join(f"`{n}`" for n in names)
        key = set(self.neutral_key(side, db, table))
        sets = ", ".join(f"`{n}` = values(`{n}`)"
                         for n in names if n not in key)
        # `on duplicate key update` rather than `replace into`: REPLACE
        # deletes the old row first, which fires delete triggers and drops
        # any column the incoming row does not carry
        tail = f" on duplicate key update {sets}" if sets else ""
        place = "(" + ", ".join(["%s"] * len(names)) + ")"
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    f"insert into `{self._d(side, db)}`.`{table}` ({cols})"
                    f" values {place}{tail}",
                    [tuple(canon.sql_value(v) for v in r) for r in rows])
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                gone = cur.execute(
                    f"delete from {self._quote_ident(self._d(side, db))}"
                    f".{self._quote_ident(table)}" + self._where(where))
            conn.commit()
        finally:
            conn.close()
        return gone

    SQL_DIALECT = "mysql"

    #: types whose default is written as a bare number
    NUMERIC_TYPES = {"tinyint", "smallint", "mediumint", "int", "integer",
                     "bigint", "decimal", "numeric", "float", "double",
                     "real"}

    def neutral_column_rules(self, side, db, table):
        rows = self._q(side, "select column_name, is_nullable,"
                             " column_default, extra, data_type"
                             " from information_schema.columns"
                             " where table_schema=%s and table_name=%s",
                       (self._d(side, db), table))
        out = {}
        for name, null, default, extra, typ in rows:
            extra = str(extra or "").lower()
            if default is None:
                shown = None
            elif "default_generated" in extra:
                # an expression, which the catalogue keeps as written
                shown = str(default)
            elif str(typ).lower() in self.NUMERIC_TYPES:
                shown = str(default)
            else:
                # a literal, which the catalogue keeps without its quotes
                shown = "'" + str(default).replace("'", "''") + "'"
            out[str(name)] = {"null": str(null).upper() == "YES",
                              "default": shown,
                              "identity": "auto_increment" in extra}
        return out

    def default_works(self, side, db, expr, typ=None):
        try:
            self._q(side, f"select ({expr})")
            return True
        except Exception:
            return False

    def neutral_indexes(self, side, db, table):
        rows = self._q(side, "select index_name, non_unique, column_name,"
                             " sub_part, expression"
                             " from information_schema.statistics"
                             " where table_schema=%s and table_name=%s"
                             " and index_name <> 'PRIMARY'"
                             " order by index_name, seq_in_index",
                       (self._d(side, db), table))
        got = {}
        for name, non_unique, col, part, expr in rows:
            one = got.setdefault(str(name), [not non_unique, [], True])
            if expr is not None or col is None or part is not None:
                one[2] = False
            if col is not None:
                one[1].append(str(col))
        return [(n, u, c, ok) for n, (u, c, ok) in sorted(got.items())]

    def execute_ddl(self, side, db, sql):
        self._target_only(side, "change a schema")
        self._q(side, sql)

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        return (f"create {'unique ' if unique else ''}index `{name}`"
                f" on `{self._d(side, db)}`.`{table}` ("
                + ", ".join(f"`{c}`" for c in columns) + ")")

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        defs = [f"`{col[0]}` {canon.ddl_type('mysql', col[1], col[2])}"
                + self._column_tail(col[3] if len(col) > 3 else None,
                                    "mysql")
                for col in columns]
        if key:
            defs.append("primary key (" + ", ".join(f"`{k}`" for k in key)
                        + ")")
        return (f"create table `{self._d(side, db)}`.`{table}` ("
                + ", ".join(defs) + ")")

    def neutral_create(self, side, db, table, columns, key=()):
        exists = self._q(side, "select count(*) from information_schema"
                               ".tables where table_schema=%s"
                               " and table_name=%s",
                         (self._d(side, db), table))[0][0]
        if exists:
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        finally:
            conn.close()
        return ddl

    def local_table(self, table):
        """The last component: this engine has no schemas to qualify with."""
        return str(table).split(".")[-1]

    def _unwritable_columns(self, side, db, table):
        """MySQL refuses a value for a generated column too, with its own
        wording - measured on 8: `ERROR 3105 (HY000): The value specified
        for generated column 'total' in table 't' is not allowed.` Omitting
        it stored `total=20.00` from `price * qty`, the same shape as
        PostgreSQL.

        Both kinds are excluded. A VIRTUAL column is not stored at all, so
        writing to it is refused just as flatly as to a STORED one, and
        `generation_expression` is non-empty for both. Cached per table.
        """
        cache = self.__dict__.setdefault("_generated_cache", {})
        target = self._d(side, db)
        ident = (side, target, table)
        if ident not in cache:
            rows = self._q(side,
                           "select column_name from information_schema.columns"
                           " where table_schema = %s and table_name = %s"
                           " and generation_expression <> ''",
                           (target, table))
            cache[ident] = {r[0] for r in rows}
        return cache[ident]

    #: Rows in one applied statement (`Engine._apply_each`).
    APPLY_ROWS = 1000

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_upserts(self, side, db, table, rows):
        from .. import canon
        table = self.local_table(table)
        key = rows[0][0]
        full = [{**k, **v} for k, v in rows]
        names = [n for n in sorted(full[0])
                 if n not in self._unwritable_columns(side, db, table)]
        cols = ", ".join(f"`{n}`" for n in names)
        sets = ", ".join(f"`{n}` = values(`{n}`)"
                         for n in names if n not in key)
        # a row already there with nothing but its key to write is left
        # as it is, as `on conflict do nothing` does
        tail = (f" on duplicate key update {sets}" if sets else
                " on duplicate key update " + ", ".join(
                    f"`{k}` = `{k}`" for k in sorted(key)))
        mark = "(" + ", ".join(["%s"] * len(names)) + ")"
        with self._writer(side, db) as conn:
            with conn.cursor() as cur:
                for at in range(0, len(full), self.APPLY_ROWS):
                    part = full[at:at + self.APPLY_ROWS]
                    cur.execute(
                        f"insert into `{self._d(side, db)}`.`{table}`"
                        f" ({cols}) values "
                        + ", ".join([mark] * len(part)) + tail,
                        [canon.sql_value(r[n]) for r in part
                         for n in names])

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def _apply_deletes(self, side, db, table, keys):
        from .. import canon
        table = self.local_table(table)
        names = sorted(keys[0])
        cols = ", ".join(f"`{n}`" for n in names)
        mark = "(" + ", ".join(["%s"] * len(names)) + ")"
        with self._writer(side, db) as conn:
            with conn.cursor() as cur:
                for at in range(0, len(keys), self.APPLY_ROWS):
                    part = keys[at:at + self.APPLY_ROWS]
                    cur.execute(
                        f"delete from `{self._d(side, db)}`.`{table}`"
                        f" where ({cols}) in ("
                        + ", ".join([mark] * len(part)) + ")",
                        [canon.sql_value(k[n]) for k in part
                         for n in names])

    def _open_writer(self, side, db):
        self._target_only(side, "apply changes")
        return self._conn(side)

    def binlog_names(self, db, table, values):
        """Binlog row values keyed by column name rather than by position.

        `binlog_row_metadata` defaults to MINIMAL, and at MINIMAL the binlog
        carries no column names at all - the reader hands back
        `UNKNOWN_COL0`, `UNKNOWN_COL1` and so on. Setting it to FULL fixes
        that on a server you control, and a managed MySQL often will not let
        you, so the mapping has to exist either way.

        The index is the column's ordinal position, which is what
        `information_schema` orders by, so the two line up.
        """
        if not any(str(k).startswith("UNKNOWN_COL") for k in values):
            return values
        real = self._cols(db, table)
        out = {}
        for k, v in values.items():
            if str(k).startswith("UNKNOWN_COL"):
                i = int(str(k)[11:])
                if i >= len(real):
                    raise SystemExit(
                        f"{table}: the binlog has more columns than"
                        " information_schema does, which means the table was"
                        " altered after this event was written - replaying it"
                        " would put values in the wrong columns")
                out[real[i]] = v
            else:
                out[k] = v
        return out

    #: binlog events that carry rows the reader cannot open, and the
    #: setting that writes them: MariaDB's compressed row events. MySQL's
    #: compressed transaction is read (`binlog_payload`); it is listed for
    #: the reader that cannot, should one be given no way to
    COMPRESSED_ROWS = {0x28: "binlog_transaction_compression",
                       **{t: "log_bin_compress" for t in range(166, 172)}}

    @staticmethod
    def _row_events(stream):
        """The stream's events, with each compressed transaction's opened
        in its place (`binlog_payload`)."""
        from ..binlog_payload import TransactionPayloadEvent
        for ev in stream:
            if isinstance(ev, TransactionPayloadEvent):
                yield from (e for e in ev.events if hasattr(e, "rows"))
            else:
                yield ev

    @classmethod
    def _compressed_stop(cls, token, event_type):
        """Measured, MySQL 8.4 with `binlog_transaction_compression = ON`
        and MariaDB 11 with `log_bin_compress = ON`: an insert, an update
        and a delete were written compressed, and the reader skipped all
        three - no change returned, the position not moved, nothing said.
        A tail on it would have called itself caught up for ever."""
        raise SystemExit(cls._compressed_said(token, event_type, "applied"))

    @classmethod
    def _compressed_said(cls, token, event_type, done):
        name = cls.COMPRESSED_ROWS[event_type]
        return (
            f"the source writes rows into its binlog compressed ({name}),"
            " and migkit's binlog reader cannot read them; reading on would"
            f" skip them without a trace. Nothing after"
            f" {token.get('log_file')}, position {token.get('log_pos')}, was"
            f" {done}. On the source: set global {name} = OFF (in the"
            " parameter group on a managed service)"
            + (", and in any application session that turns it on for"
               " itself" if name == "binlog_transaction_compression" else "")
            + ". What was already written compressed stays unreadable, so"
            " move again with --mode full+cdc once it is off")

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Row changes out of the binlog, as neutral records.

        Non-blocking on purpose: this returns what is there now and a token
        to come back with, rather than holding the connection open. A caller
        that wants to follow calls it again; one that wants a bounded catch-up
        gets a bounded one.

        A table with no primary key is skipped and named. Without a key there
        is no way to address the row on the target, and applying an UPDATE by
        matching every column would hit every duplicate of it.
        """
        import pymysql
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.event import NotImplementedEvent
        from pymysqlreplication.row_event import (DeleteRowsEvent,
                                                  UpdateRowsEvent,
                                                  WriteRowsEvent)

        from .. import canon
        ep = self.hop.source if side == "src" else self.hop.target
        for name, why in (
                ("binlog_row_metadata",
                 "without it the binlog carries no column names and no"
                 " charset per column, so the reader cannot tell a"
                 " varbinary from a varchar - measured, it tries to UTF-8"
                 " decode the binary and raises. The library's way around"
                 " that replaces the bytes it cannot decode, which is worse"
                 " than stopping"),
                ("binlog_row_image",
                 "without it an UPDATE's before image carries only the key,"
                 " so a column that was not part of the change arrives as"
                 " missing rather than unchanged")):
            got = self._q(side, f"select @@{name}")
            value = str(got[0][0]) if got else "?"
            if value.upper() != "FULL":
                raise SystemExit(
                    f"{name} is {value} on this server, and the tail needs"
                    f" FULL: {why}.\n"
                    f"    set global {name} = 'FULL';   -- self-managed\n"
                    f"    {name}=FULL                   -- parameter group")
        from ..binlog_payload import register
        token = dict(token or {}) or self.change_point(side, db)
        start = dict(token)
        stream = BinLogStreamReader(
            connection_settings={"host": ep.host, "port": ep.port,
                                 "user": ep.user, "passwd": ep.password},
            server_id=int(self.hop.options.get("server_id", 4379)),
            blocking=False, resume_stream=True,
            log_file=token.get("log_file"), log_pos=token.get("log_pos"),
            only_schemas=[self._d(side, db)],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent,
                         NotImplementedEvent, register()],
            filter_non_implemented_events=False)
        # each table's key once a batch: asked for every event, on a
        # connection of its own, it held the reader to about 160 rows a
        # second - one-row transactions at 472 a second left the tail 38
        # seconds behind when the writer stopped (`bench/run.py`)
        out, skipped, keys_of = [], set(), {}
        try:
            for ev in self._row_events(stream):
                if isinstance(ev, NotImplementedEvent):
                    if ev.event_type in self.COMPRESSED_ROWS:
                        # nothing of this batch is handed back, so the
                        # tail stays where it was asked to read from
                        self._compressed_stop(start, ev.event_type)
                    continue
                table = ev.table
                if self.hop.excluded(db, table):
                    # the target owns it: the move left it alone, so the
                    # tail does too - and a keyless one does not stop it
                    continue
                if table not in keys_of:
                    keys_of[table] = self._pk_cols(self._d(side, db), table)
                keys = keys_of[table]
                if not keys:
                    skipped.add(table)
                    continue
                named = self._d(side, db)
                for row in ev.rows:
                    if isinstance(ev, WriteRowsEvent):
                        vals = self.binlog_names(named, table, row["values"])
                        out.append(canon.change(
                            "insert", table,
                            {k: vals[k] for k in keys}, vals))
                    elif isinstance(ev, UpdateRowsEvent):
                        before = self.binlog_names(named, table,
                                                   row["before_values"])
                        after = self.binlog_names(named, table,
                                                  row["after_values"])
                        # the key from the *before* image: an UPDATE that
                        # moved the primary key has to find the old row
                        out.append(canon.change(
                            "update", table,
                            {k: before[k] for k in keys}, after))
                    else:
                        vals = self.binlog_names(named, table, row["values"])
                        out.append(canon.change(
                            "delete", table, {k: vals[k] for k in keys}))
                token = {"log_file": stream.log_file,
                         "log_pos": stream.log_pos}
                if len(out) >= limit:
                    break
            else:
                # read to the end of the log: everything up to here has been
                # seen, including what was left out - other databases, tables
                # the hop excludes - so the position moves past it too. It
                # stayed at the last row of this database, and a fence waiting
                # for the tail to reach the log's end never saw it get there
                # on a server busy elsewhere.
                if stream.log_file and stream.log_pos:
                    token = {"log_file": stream.log_file,
                             "log_pos": stream.log_pos}
        except pymysql.err.OperationalError as e:
            # measured on 8.4 after `purge binary logs`: 1236, "Could not
            # find first log file name in binary log index file"
            if not (e.args and e.args[0] == 1236):
                raise
            raise SystemExit(
                f"the source no longer has the binlog this tail had read up"
                f" to ({token.get('log_file')}, position"
                f" {token.get('log_pos')}): {e.args[-1]}."
                " The changes after it went with it, so starting again from"
                " now would skip them without a trace. Move again with"
                " --mode full+cdc, and keep the binlog longer than the tail"
                " may be stopped for (binlog_expire_logs_seconds, or"
                " `binlog retention hours` on RDS)")
        finally:
            stream.close()
        if skipped:
            raise SystemExit(
                f"no primary key on {', '.join(sorted(skipped))} - a change"
                " to a keyless table cannot be addressed on the target, and"
                " applying it by matching every column would hit every"
                " duplicate. Add a key, or exclude the table")
        return out, token

    def _binlog_position(self, side):
        """(file, position) the binlog is at now, or None when it is off.

        MySQL 8.4 renamed the statement and removed the old spelling; 8.0,
        Aurora and MariaDB only have the old one. Asking with `or` meant the
        new spelling's syntax error on an 8.0 server stopped the tail before
        the old one was ever tried, so only that error moves on to the next.
        """
        import pymysql
        for q in ("show binary log status", "show master status"):
            try:
                got = self._q(side, q)
            except pymysql.err.ProgrammingError as e:
                if e.args and e.args[0] == 1064:
                    continue
                raise
            if got:
                return got[0][0], int(got[0][1])
            return None
        return None

    CHANGE_POINT_READS_ONLY = True

    def load_window(self, db, log=None, tables=None):
        """The target's triggers, off for the load (`_MyTriggerWindow`)."""
        from ..movers import _MyTriggerWindow
        return _MyTriggerWindow(self.hop, db, log, tables)

    def log_position(self, side, db):
        return self.change_point(side, db)

    @staticmethod
    def position_reached(have, want):
        """Binlog positions compare by file, then by offset in it."""
        def at(pos):
            f = str(pos["log_file"])
            seq = f.rsplit(".", 1)[-1]
            return (int(seq) if seq.isdigit() else 0, int(pos["log_pos"]))
        try:
            return at(have) >= at(want)
        except (KeyError, TypeError, ValueError):
            return None

    def stream_room(self, side, db, token):
        """Seconds until the binlog file the tail is reading may be purged.

        A file is purged once it has not been written for
        `binlog_expire_logs_seconds`, and it stops being written when the
        next one starts. Measured on 8.4: each file opens with a format
        event carrying the time it started, and the time on the one after
        the tail's is the time the tail's file was last written. The
        purge runs when a file rotates, so the time given is the soonest.

        None where nothing purges (`binlog_expire_logs_auto_purge` off, or
        no expiry) and on a managed server, whose retention is its own
        setting and not this variable."""
        import pymysql
        ep = self.hop.source if side == "src" else self.hop.target
        if "rds.amazonaws.com" in (ep.host or "") or not token:
            return None
        try:
            expire = int(self._q(side, "select"
                                 " @@binlog_expire_logs_seconds")[0][0])
        except pymysql.err.MySQLError:
            # MariaDB before 10.6
            expire = int(float(self._q(side, "select @@expire_logs_days"
                                       )[0][0]) * 86400)
        try:
            if not int(self._q(side, "select"
                               " @@binlog_expire_logs_auto_purge")[0][0]):
                return None
        except pymysql.err.MySQLError:
            pass
        if expire <= 0:
            return None
        files = [r[0] for r in self._q(side, "show binary logs")]
        at = token.get("log_file")
        if at not in files:
            return {"seconds": 0}
        if at == files[-1]:
            return {"seconds": expire}
        closed = self._binlog_started(side, files[files.index(at) + 1])
        if closed is None:
            return None
        return {"seconds": max(int(closed + expire - time.time()), 0)}

    def _binlog_started(self, side, name):
        """The time on the format event a binlog file opens with."""
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.event import FormatDescriptionEvent
        ep = self.hop.source if side == "src" else self.hop.target
        stream = BinLogStreamReader(
            connection_settings={"host": ep.host, "port": ep.port,
                                 "user": ep.user, "passwd": ep.password},
            server_id=int(self.hop.options.get("server_id", 4379)),
            blocking=False, resume_stream=True, log_file=name, log_pos=4,
            only_events=[FormatDescriptionEvent])
        try:
            for ev in stream:
                if ev.timestamp and stream.log_file == name:
                    return ev.timestamp
        finally:
            stream.close()
        return None

    def change_point(self, side, db):
        """Where the binlog is now, as the token `neutral_changes` resumes
        from."""
        pos = self._binlog_position(side)
        if not pos:
            raise SystemExit(
                "the binlog is off on this server, so there is no change"
                " log to read - turn on log_bin, or move without CDC")
        return {"log_file": pos[0], "log_pos": pos[1]}

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        row = canon.row_expr("mysql", columns)
        r = self._q(side, f"select count(*),"
                          f" {canon.digest_expr('mysql', row)}"
                          f" from `{self._d(side, db)}`.`{table}`"
                          + self._where(where))
        return (int(r[0][0]), str(r[0][1]))

    def purge_backlog(self, side):
        """InnoDB's history list length - the undo not yet purged, which is
        what an open snapshot holds back - or None where it cannot be
        read."""
        try:
            got = self._q(side, "select count from information_schema"
                                ".innodb_metrics"
                                " where name = 'trx_rseg_history_len'")
        except Exception:
            return None
        return int(got[0][0]) if got else None

    def _brand_probes(self):
        """`version()` and `@@version_comment` per side, in one round trip.

        Two fields because the forks split across them: MariaDB puts its name
        in both (measured: `11.8.9-MariaDB-ubu2404` /
        `mariadb.org binary distribution`), TiDB and Vitess put theirs in the
        version string, and Percona only shows up in the comment.
        """
        def one(side):
            try:
                r = self._q(side, "select version(), @@version_comment")
            except Exception:
                return {}
            if not r:
                return {}
            return {"version": r[0][0], "version_comment": r[0][1]}
        return (one("src"), one("dst"))

    def _q(self, side, sql, args=None, fresh=False):
        # retry the connect+query as a unit so a TLS/socket blip clears
        def once():
            conn = self._conn(side)
            try:
                with conn.cursor() as cur:
                    if fresh:
                        try:
                            cur.execute("set session"
                                        " information_schema_stats_expiry = 0")
                        except Exception:
                            pass
                    cur.execute(sql, args)
                    return cur.fetchall()
            finally:
                conn.close()
        return with_retry(once, label=f"mysql query {side}")

    def _q_named(self, side, sql):
        """`_q`, keeping the column names.

        `SHOW REPLICA STATUS` answers one row of about fifty columns whose
        order is not a contract and whose names differ between MySQL and
        MariaDB. Reading it by index would be guessing twice.
        """
        def once():
            conn = self._conn(side)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cols = [d[0] for d in (cur.description or ())]
                    return [dict(zip(cols, r)) for r in cur.fetchall()]
            finally:
                conn.close()
        return with_retry(once, label=f"mysql named query {side}")

    def _rows_utc(self, side, sql):
        """Read with the session time_zone pinned to +00:00 so UNIX_TIMESTAMP
        on a DATETIME is comparable across sides regardless of server tz -
        the mysql equivalent of the postgres UTC pin used for tz-shift audit."""
        def once():
            conn = self._conn(side)
            try:
                with conn.cursor() as cur:
                    try:
                        cur.execute("set time_zone = '+00:00'")
                    except Exception:
                        pass
                    cur.execute(sql)
                    return cur.fetchall()
            finally:
                conn.close()
        return with_retry(once, label=f"mysql utc {side}")

    def _d(self, side, db):
        """Resolve the physical db name for a side. Source keeps the given
        name; target goes through the hop's db_map so a migration can land
        in a differently-named database (identity when unmapped)."""
        return self.hop.target_db(db) if side == "dst" else db

    @staticmethod
    def _canon_ddl(text):
        """Normalize a mysqldump so cosmetic mover artifacts don't read as
        schema drift: strip per-table AUTO_INCREMENT counters and DEFINER
        clauses, and collapse "CHARACTER SET x COLLATE y" to the bare
        "COLLATE y" (a collation already implies its charset, so the two
        forms are identical - DTS emits one, the source the other). Real
        charset/collation differences still surface via the COLLATE name."""
        text = re.sub(r" AUTO_INCREMENT=\d+", "", text)
        text = re.sub(r"DEFINER=`[^`]*`@`[^`]*`", "", text)
        text = re.sub(r"CHARACTER SET \w+ COLLATE", "COLLATE", text)
        # DTS downgrades routines to SQL SECURITY INVOKER; atlas treats this
        # as non-structural, so drop the clause to agree (raw dumps keep it)
        text = re.sub(r"\s*SQL SECURITY (DEFINER|INVOKER)", "", text)
        return text

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        rows = self._q("src", "show databases")
        return sorted(r[0] for r in rows if r[0] not in SKIP_DBS
                      and not r[0].startswith("__")
                      and not self.hop.excluded(r[0]))

    def _dump_schema(self, side, db, physical=None):
        ep = self.hop.source if side == "src" else self.hop.target
        if not which("mysqldump"):
            raise SystemExit("the MySQL client is not installed on this"
                             " machine: migkit doctor --install")
        pdb = physical or self._d(side, db)
        left = [f"--ignore-table={pdb}.{t}"
                for t in sorted(self._left_out_of_schema(db, side))]
        # the password in the environment: on the command line every
        # process listing on the machine could read it
        p = run(["mysqldump", "-h", ep.host, "-P", str(ep.port), "-u", ep.user,
                 "--no-data", "--routines", "--triggers",
                 "--events", "--skip-comments", "--skip-dump-date",
                 "--column-statistics=0",
                 f"--ignore-table={pdb}.migkit_changelog", *left, pdb],
                check=False, env={"MYSQL_PWD": ep.password})
        if p.returncode != 0:
            raise RuntimeError(p.stderr.strip())
        text = self._canon_ddl(p.stdout)
        lines = [l for l in text.splitlines()
                 if not l.startswith("--")
                 and "GTID_PURGED" not in l
                 and "SQL_LOG_BIN" not in l
                 and l.strip()]
        return "\n".join(lines) + "\n"

    def _left_out_of_schema(self, db, side):
        """The tables on this side the whole-database schema comparers
        leave out: those the hop excludes, which are the target's own, and
        those whose columns the hop maps, which the pair machinery compares
        through the mapping (`_mapped_schema`)."""
        out = {t for t in self._all_tables(side, db)
               if self.hop.excluded(db, t)}
        for t in self.mapped_tables(db):
            leaf = t.rpartition(".")[2]
            out.add(self.hop.target_table(db, leaf).rpartition(".")[2]
                    if side == "dst" else leaf)
        return out

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
            (d / "schema.diff").unlink(missing_ok=True)  # clear stale evidence
            res.append(Result("schema", db, "ok"))
        else:
            (d / "schema.diff").write_text("\n".join(diff))
            sample = "; ".join(sorted(set(l.strip() for l in changed))[:3])
            r = Result("schema", db, "diff",
                       f"{len(changed)} changed lines, e.g. {sample}",
                       str(d / "schema.diff"),
                       "apply missing DDL from schema-src.sql on target")
            # a table on one side only is never cosmetic, whatever another
            # comparer read: measured, the schema-aware one did not see a
            # MariaDB system-versioned table at all and called the two
            # schemas the same
            r.whole_table = any(l[1:].lstrip().upper().startswith(
                "CREATE TABLE") for l in changed)
            res.append(r)
        res.append(self.check_objects(db))
        if which("atlas") and self.hop.options.get("atlas", True):
            at = self._atlas(db)
            if at:
                res.append(at)
        # after the demotion, not before: the schema-aware comparison left
        # these tables out, so its reading is no authority over them
        return self._atlas_authoritative(res) + self._mapped_schema(db)

    def check_objects(self, db):
        queries = {
            "table": "select table_name from information_schema.tables"
                     " where table_schema=%s and table_type in ('BASE TABLE', 'SYSTEM VERSIONED')",
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
        # a mapped table's indexes are over the columns the mapping names
        left = {side: self._left_out_of_schema(db, side)
                for side in ("src", "dst")}

        def kept(side, typ, name):
            return not (typ == "index"
                        and str(name).split(".", 1)[0] in left[side])
        for typ, sql in queries.items():
            a = {r[0] for r in self._q("src", sql, (db,))
                 if kept("src", typ, r[0])}
            b = {r[0] for r in self._q("dst", sql, (self._d("dst", db),))
                 if kept("dst", typ, r[0])}
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

    def snapshot_state(self, db, state_dir, kind="all"):
        q = ("select table_name, auto_increment from information_schema.tables"
             " where table_schema=%s and auto_increment is not null")
        rows = self._q("dst", q, (self._d("dst", db),), fresh=True)
        (state_dir / "dst-autoinc.txt").write_text(
            "".join(f"{t}|{v}\n" for t, v in sorted(rows)))
        # sequence-only repair rolls back from the auto_increment snapshot alone
        if kind == "sequences":
            return
        (state_dir / "dst-schema.sql").write_text(self._dump_schema("dst", db))

    def _schema_urls(self, db):
        """(source, target) as the schema comparison's URLs."""
        from urllib.parse import quote
        s, t = self.hop.source, self.hop.target
        return (f"mysql://{s.user}:{quote(s.password, safe='')}"
                f"@{s.host}:{s.port}/{db}",
                f"mysql://{t.user}:{quote(t.password, safe='')}"
                f"@{t.host}:{t.port}/{self._d('dst', db)}")

    def _schema_excludes(self, db):
        """What the schema comparison leaves out: migkit's own table, and
        the tables the pair compares through the mapping."""
        return ["migkit_changelog"] + [
            name for side in ("src", "dst")
            for name in sorted(self._left_out_of_schema(db, side))]

    def _atlas(self, db):
        from ..movers import schema_diff
        su, tu = self._schema_urls(db)
        excludes = self._schema_excludes(db)
        try:
            p = schema_diff(tu, su, excludes)
        except Exception:
            return None
        if p.returncode != 0:
            return None
        text = p.stdout.strip()
        if not text or "Schemas are synced" in text:
            # both files, not just the fix: an undo left behind after the
            # schemas converged is a rollback for changes nobody made
            for stale in ("schema-fix.sql", "schema-fix.revert.sql"):
                (self.hop.report_dir(db) / stale).unlink(missing_ok=True)
            return Result("schema", f"{db} {self.AUTHORITY_SCOPE}", "ok",
                          "the two schemas match, compared as schemas"
                          " rather than as text")
        out = self.hop.report_dir(db) / "schema-fix.sql"
        out.write_text(text + "\n")
        detail = (f"{len(text.splitlines())} lines of DDL would bring the"
                  " target's schema up to the source's")
        # The same diff run the other way is the undo of exactly these
        # statements, and it has to be taken now: once the fix is applied the
        # two schemas no longer describe where the target came from.
        from .. import revert as _revert
        rev = self.hop.report_dir(db) / "schema-fix.revert.sql"
        try:
            rp = schema_diff(su, tu, excludes)
            rtext = rp.stdout.strip() if rp.returncode == 0 else ""
        except Exception:
            rtext = ""
        body = _revert.script(text, rtext, "schema-fix.sql")
        if body:
            rev.write_text(body)
            detail += "; " + _revert.summary(text, rtext)
        else:
            # no undo is a fact worth stating, not a blank to fill in later
            rev.unlink(missing_ok=True)
            detail += "; no undo could be generated - take a backup first"
        detail += self._backfill_clause(db, text)
        return Result("schema", f"{db} {self.AUTHORITY_SCOPE}", "diff", detail,
                      str(out), "review schema-fix.sql, then apply it to"
                      " the target; schema-fix.revert.sql undoes it")

    def _all_tables(self, side, db):
        """Every base table on a side, the hop's exclusions included.

        The bulk path needs the excluded ones by name - to tell the dump to
        skip them and to leave them alone when the target is emptied - and
        `_tables` has already dropped them by the time it answers. One
        catalogue query for both, so the two lists cannot disagree about
        what counts as a table.
        """
        rows = self._q(side, "select table_name from information_schema.tables"
                             " where table_schema=%s and table_type in ('BASE TABLE', 'SYSTEM VERSIONED')"
                             " and table_name not like 'migkit%%'"
                             " order by 1", (self._d(side, db),))
        return [r[0] for r in rows]

    def _tables(self, side, db):
        return [t for t in self._all_tables(side, db)
                if not self.hop.excluded(db, t)]

    #: a primary key's columns, only the ones the table shows: MariaDB adds
    #: a system-versioned table's hidden `row_end` to its key, and a check
    #: that selected it stopped on `Unknown column 'row_end'`
    PK_SQL = ("select k.column_name from information_schema.key_column_usage k"
              " join information_schema.columns c"
              " on c.table_schema = k.table_schema"
              " and c.table_name = k.table_name"
              " and c.column_name = k.column_name"
              " where k.table_schema=%s and k.table_name=%s"
              " and k.constraint_name='PRIMARY' order by k.ordinal_position")

    def _pk_cols(self, db, t):
        return [r[0] for r in self._q("src", self.PK_SQL, (db, t))]

    def _cols(self, db, t):
        rows = self._q("src", "select column_name from information_schema.columns"
                              " where table_schema=%s and table_name=%s"
                              " order by ordinal_position", (db, t))
        return [r[0] for r in rows]

    def _row_expr(self, db, t):
        """The row as one string, encoded so distinct rows cannot collide.

        This used to be `concat_ws('#', ...)` with `~null~` standing in for
        NULL, and both halves of that were wrong in the same way. Measured on
        MySQL 8: ('x#y','z') and ('x','y#z') both rendered `x#y#z` and hashed
        to CRC32 3898531935, and a NULL and the literal `~null~` did the same.
        A source holding the first of each against a target holding the second
        was reported `rows 2==2, checksum 810ced44==810ced44` - a verifier
        certifying a difference as equality. See `migkit.rowtext`.
        """
        from .. import rowtext
        return rowtext.mysql_row(self._cols(db, t))

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
            return self._q(side,
                           f"select count(*) from {self._scope(side, db, t)}"
                           )[0][0]

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
                outside = self._outside_filter(db, t)
                if outside:
                    bad.append(f"{t} dst holds {outside} rows the hop's row"
                               " filter excludes - the move does not put"
                               " them there")
        if bad:
            res.append(Result("counts", db, "diff", "; ".join(bad)))
        return res or [Result("counts", db, "ok",
                              f"{len(common)} tables, rows"
                              f" {total_a:,}=={total_b:,}")]

    PARAM_CRITICAL = ("time_zone", "system_time_zone", "character_set_server",
                      "character_set_database", "collation_server",
                      "collation_database", "collation_connection", "sql_mode",
                      "lower_case_table_names", "default_storage_engine",
                      "transaction_isolation", "explicit_defaults_for_timestamp",
                      "character_set_connection", "character_set_client",
                      "character_set_results", "max_allowed_packet",
                      "group_concat_max_len", "version")

    def check_params(self, db):
        def pull(side):
            return {r[0]: str(r[1]) for r in
                    self._q(side, "show global variables")}
        return self._param_result(
            db, pull("src"), pull("dst"), self.PARAM_CRITICAL,
            "align the behavior-critical variables on the target parameter"
            " group before cutover")

    def check_autoinc(self, db):
        """usable = will AUTO_INCREMENT collide with an existing row on the
        next insert (the DMS trap - InnoDB 8 usually clamps it up, but a
        pre-8.0 restart, tablespace import, or a stray id=0 row can leave it
        behind); parity = does the counter match the source."""
        ddb = self._d("dst", db)
        q = ("select table_name, auto_increment from information_schema.tables"
             " where table_schema=%s and auto_increment is not null")
        src = dict(self._q("src", q, (db,), fresh=True))
        dst = dict(self._q("dst", q, (ddb,), fresh=True))
        acol = dict(self._q("dst",
                            "select table_name, column_name"
                            " from information_schema.columns"
                            " where table_schema=%s and extra like %s",
                            (ddb, "%auto_increment%"), fresh=True))
        collide = []
        for t, nextv in sorted(dst.items()):
            col = acol.get(t)
            if not col or nextv is None:
                continue
            mx = self._q("dst", f"select coalesce(max(`{col}`),0)"
                                f" from `{ddb}`.`{t}`", fresh=True)[0][0]
            if int(mx) > 0 and int(nextv) <= int(mx):
                collide.append(f"{t}: auto_increment={nextv}"
                               f" <= max({col})={mx}")
        res = []
        if collide:
            res.append(Result("autoinc", f"{db} usable", "diff",
                              "AUTO_INCREMENT will collide on next insert: "
                              + "; ".join(collide[:8]), "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind sequences"))
        else:
            res.append(Result("autoinc", f"{db} usable", "ok",
                              f"{len(acol)} auto_increment tables clear their"
                              " column max, no collision" if acol
                              else "no auto_increment tables"))
        parity = [f"{t} src={v} dst={dst.get(t)}"
                  for t, v in sorted(src.items()) if dst.get(t) != v]
        if parity:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(parity[:8]), "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind sequences"))
        else:
            res.append(Result("autoinc", f"{db} parity", "ok",
                              f"{len(src)} counters match source"))
        return res

    def _health(self, side):
        """What this side says about its own load, for the throttle.

        `Threads_running` against `max_connections` is the cheapest honest
        load signal MySQL offers. Anything unreadable comes back as None
        rather than as "fine", so a server that hides a signal is not
        mistaken for an idle one.
        """
        from ..throttle import Health
        try:
            running = float(self._q(
                side, "show global status like 'Threads_running'")[0][1])
            limit = float(self._q(
                side, "show global variables like 'max_connections'")[0][1])
            busy = running / max(limit, 1.0)
        except Exception:
            return None
        # Where this side is itself a replica - a reader the check was
        # pointed at to spare the primary - its lag is the other signal: a
        # heavy read there makes it fall behind. By name, through
        # `_q_named`, since the columns differ between MySQL and MariaDB.
        lag = None
        try:
            rows = self._q_named(side, "show replica status")
        except Exception:
            rows = []
        if rows:
            got = self._repl_field(rows[0], self._REPL_FIELDS["lag"])
            try:
                lag = None if got is None else float(got)
            except (TypeError, ValueError):
                lag = None
        return Health(busy_ratio=busy, lag_seconds=lag)

    def check_data(self, db, table=None, stream=None, with_counts=False):
        if table and self.through_pair(db, table):
            return self.columns_pair().check_data(db, table, stream)
        got = self._own_check_data(db, table, stream, with_counts)
        return got if table else got + self._mapped_data(db, stream)

    def _own_check_data(self, db, table=None, stream=None, with_counts=False):
        st, dt = set(self._tables("src", db)), set(self._tables("dst", db))
        tables = [table] if table else sorted(
            t for t in st & dt if not self.through_pair(db, t))
        res = []
        rows_a = rows_b = 0
        bad_counts = []
        # Same reason as the PostgreSQL path: a checksum is only a SELECT, so
        # nothing else stops this loop from being the heaviest thing on a
        # server that is also serving an application.
        from ..throttle import Throttle
        gate = Throttle(self.hop.workers, probe=lambda: self._health("src"))

        def guarded(d, tbl):
            with gate.unit():
                return self._diff_table(d, tbl)

        per = {}
        with ThreadPoolExecutor(max_workers=self.hop.workers) as pool:
            futs = {pool.submit(guarded, db, t): t for t in tables}
            for fu in as_completed(futs):
                r, ra, rb = fu.result()
                if stream:
                    stream(f"{futs[fu]}: {r.status}")
                per[futs[fu]] = (r, ra, rb)
        # A table that differs is confirmed before it is called different:
        # where the target replicates from the source, wait until it has
        # applied what the source had, and look again. What converged was
        # still arriving; what did not is a real difference. Without a
        # replica to fence on, nothing changes here.
        bad = [t for t, (r, _, _) in per.items() if r.status == "diff"]
        if bad:
            _, healed, how = self._resolve_inflight(db, bad, stream)
            proof = dict(h.split(": ", 1) for h in how)
            for t in healed:
                r, ra, rb = self._diff_table(db, t)
                if r.status == "ok":
                    r = Result("data", r.scope, "ok",
                               f"{r.detail}; the difference was still"
                               f" arriving ({proof.get(t, 'confirmed')})",
                               r.report)
                per[t] = (r, ra, rb)
        for t, (r, ra, rb) in per.items():
            res.append(r)
            rows_a += ra
            rows_b += rb
            if ra != rb:
                bad_counts.append(f"{t} src={ra} dst={rb}")
        res = sorted(res, key=lambda r: r.scope)
        self._last_throttle = gate.summary()
        note = gate.line()
        if note and stream:
            stream(f"# {note}")
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

    def _key_expr(self, db, t):
        """Expression hashing only the primary key, or None without one.

        Rides along in the same scan as the row hash: the row is already being
        read and hashing a key costs far less than hashing a whole row, which
        is what makes naming the shape of a difference effectively free.
        """
        pks = self._pk_cols(db, t)
        if not pks:
            return None
        from .. import rowtext
        # `\x02` joined and `\x01` stood in for NULL here, which is the same
        # ambiguity as the row hash had, just with rarer characters
        return rowtext.mysql_row(pks)

    def _scope(self, side, db, t):
        """What a check reads of table `t` on one side, as a FROM item
        aliased `t`: the whole table, or the rows the hop's row filter
        selects. The PostgreSQL engine has the same method for the same
        reason - the filter the move applies was never applied by any check,
        so a filtered move was reported as missing rows for good. Every read
        a check makes of a table goes through here."""
        qt = f"`{self._d(side, db)}`.`{t}`"
        pred = (self.hop.row_filter(db, t)
                if hasattr(self.hop, "row_filter") else None)
        return f"(select * from {qt} where {pred}) t" if pred else f"{qt} t"

    def _outside_filter(self, db, t):
        """Target rows the hop's row filter excludes, or None without one.
        The move puts no row outside the filter on the target, so any count
        above zero is said; `is not true` counts a NULL predicate too."""
        pred = (self.hop.row_filter(db, t)
                if hasattr(self.hop, "row_filter") else None)
        if not pred:
            return None
        return int(self._q("dst", f"select count(*) from"
                                  f" `{self._d('dst', db)}`.`{t}`"
                                  f" where ({pred}) is not true")[0][0])

    def _checksum(self, side, db, t, expr, where="", key_expr=None):
        cols = ["count(*)", "coalesce(bit_xor(crc32(" + expr + ")), 0)",
                "coalesce(bit_xor(conv(substring(md5(" + expr + "), 1, 8),"
                " 16, 10)), 0)"]
        if key_expr:
            cols.append("coalesce(bit_xor(conv(substring(md5(" + key_expr
                        + "), 1, 8), 16, 10)), 0)")
        q = (f"select {', '.join(cols)}"
             f" from {self._scope(side, db, t)} {where}")
        return tuple(self._q(side, q)[0])

    def _reladiff_url(self, side, db):
        from urllib.parse import quote
        ep = self.hop.source if side == "src" else self.hop.target
        return (f"mysql://{ep.user}:{quote(ep.password, safe='')}"
                f"@{ep.host}:{ep.port}/{self._d(side, db)}")

    def _reladiff_table(self, db, t, pks):
        from ..util import PrivateFile, diff_run_config
        # both addresses in a private file: they carry the passwords, and
        # were the program's arguments
        conf = PrivateFile(diff_run_config(
            self._reladiff_url("src", db), t, self._reladiff_url("dst", db),
            t), ".toml")
        cmd = ["reladiff", "--conf", None, "--stats",
               "-j", str(self.hop.workers), "-c", "%"]
        for k in pks:
            cmd += ["-k", k]
        pred = (self.hop.row_filter(db, t)
                if hasattr(self.hop, "row_filter") else None)
        if pred:
            cmd += ["--where", pred]
        try:
            with conf as path:
                cmd[2] = path
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

        # builtin checksum is the fast default; options.reladiff:true swaps in
        if pks and which("reladiff") and self.hop.options.get("reladiff",
                                                              False):
            verdict, detail, ra, rb = self._reladiff_table(db, t, pks)
            if verdict == "ok":
                return Result("data", scope, "ok", detail), ra, rb
            if verdict == "diff":
                return self._drilldown(db, t, pks, expr, [""])

        # Ranges come from the shared planner rather than being built here, so
        # both engines get the same guarantee: the ranges cover the whole
        # keyspace, open at both ends. The version that used to live here
        # started at min(pk) on the source, which silently skipped any target
        # row with a smaller key - exactly the kind of difference worth
        # catching.
        from .. import checkpoint as _cp
        pk_ranges, col = [(None, None)], None
        if len(pks) == 1:
            n = self._q("src", f"select count(*) from"
                               f" {self._scope('src', db, t)}")[0][0]
            if n > self.hop.slice:
                mm = self._q("src", f"select min(`{pks[0]}`), max(`{pks[0]}`)"
                                    f" from {self._scope('src', db, t)}")[0]
                if mm[0] is not None and str(mm[0]).lstrip("-").isdigit():
                    col = pks[0]
                    cp_path = str(self.hop.report_dir(db) / "checkpoint.json")
                    cp = _cp.Checkpoint(cp_path)
                    chunk = cp.chunk_for(scope, max(1, self.hop.slice))
                    pk_ranges = _cp.plan_ranges(int(mm[0]), int(mm[1]), chunk)

        key_expr = self._key_expr(db, t)

        def clause(lo, hi):
            w = _cp.where(f"`{col}`", lo, hi) if col else ""
            return f"where {w}" if w else ""

        ranges = [clause(lo, hi) for lo, hi in pk_ranges]
        cp = (_cp.Checkpoint(str(self.hop.report_dir(db) / "checkpoint.json"))
              if col else _cp.Checkpoint(None))
        todo = cp.begin(scope, expr, pk_ranges) if col else pk_ranges
        done_before = cp.resumed(scope) if col else 0

        bad_ranges, kinds = [], set()
        rows_a = rows_b = xor_a = xor_b = 0
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = {}
            for lo, hi in todo:
                w = clause(lo, hi)
                fa = pool.submit(self._checksum, "src", db, t, expr, w,
                                 key_expr)
                fb = pool.submit(self._checksum, "dst", db, t, expr, w,
                                 key_expr)
                futs[(lo, hi)] = (fa, fb)
            for (lo, hi), (fa, fb) in futs.items():
                ra, rb = fa.result(), fb.result()
                if ra != rb:
                    bad_ranges.append(clause(lo, hi))
                    if key_expr and len(ra) > 3:
                        from ..verdict import difference_kind
                        k = difference_kind(ra[0], ra[3], rb[0], rb[3])
                        if k:
                            kinds.add(k)
                    if col:
                        cp.clear(scope)
                    continue
                if col:
                    cp.record(scope, lo, hi, ra[0], int(ra[2] or 0))
                else:
                    rows_a += ra[0]
                    rows_b += rb[0]
                    xor_a ^= int(ra[2] or 0)
                    xor_b ^= int(rb[2] or 0)
        if not bad_ranges:
            if col:
                rows_a, xor_s = cp.total(scope, combine="xor")
                rows_b, xor_a = rows_a, int(xor_s)
                xor_b = xor_a
                cp.clear(scope)
            resumed = (f", resumed {done_before}/{len(pk_ranges)}"
                       if done_before else "")
            return Result("data", scope, "ok",
                          f"rows {rows_a:,}=={rows_b:,}, checksum"
                          f" {xor_a:x}=={xor_b:x}"
                          f" ({len(pk_ranges)} chunks{resumed})"), \
                rows_a, rows_b
        if not pks:
            return Result("data", scope, "diff",
                          "checksum differs, no pk for row drilldown", "",
                          "recopy whole table with dump/load"), rows_a, rows_b
        r, ra, rb = self._drilldown(db, t, pks, expr, bad_ranges)
        if kinds and r.status != "ok":
            r.detail += " kind=" + ",".join(sorted(kinds))
        return r, ra, rb

    def target_mark(self, db):
        """Where the target's binlog is as the move begins: a row written
        after this is in the log from here on, and one only the target has
        that is not there was there before the move."""
        pos = self._binlog_position("dst")
        return ({"log_file": pos[0], "log_pos": int(pos[1])} if pos
                else {})

    #: how far into the target's log `who_wrote` reads, at most
    WHO_WROTE_EVENTS = 200_000

    def who_wrote(self, db, table, keys, began):
        """Of the rows only the target has, which the target's own binlog
        shows written since the move began - by this server's sessions or
        by a replica's - and which it does not, which the target held
        before. Where the log from that point is gone, it says it cannot
        tell rather than guess."""
        from .. import rowtext
        mark = began or {}
        if not keys or not mark.get("log_file"):
            return ""
        files = [r[0] for r in self._q("dst", "show binary logs")]
        if mark["log_file"] not in files:
            return ("who wrote them cannot be told: the target's binlog"
                    " from the move's start is gone")
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.row_event import (UpdateRowsEvent,
                                                  WriteRowsEvent)
        wanted = set(keys)
        # the target's own key: `_pk_cols` asks the source
        pks = [r[0] for r in self._q("dst", self.PK_SQL,
                                     (self._d("dst", db), table))]
        ep = self.hop.target
        stream = BinLogStreamReader(
            connection_settings={"host": ep.host, "port": ep.port,
                                 "user": ep.user, "passwd": ep.password},
            server_id=int(self.hop.options.get("server_id", 4379)) + 1,
            blocking=False, resume_stream=True,
            log_file=mark["log_file"], log_pos=int(mark["log_pos"]),
            only_schemas=[self._d("dst", db)], only_tables=[table],
            only_events=[WriteRowsEvent, UpdateRowsEvent])
        me = int(self._q("dst", "select @@server_id")[0][0])
        found, by_me, others, first, last, seen = set(), 0, set(), None, \
            None, 0
        try:
            for ev in stream:
                seen += 1
                if seen > self.WHO_WROTE_EVENTS:
                    break
                for row in ev.rows:
                    vals = row.get("after_values") or row.get("values") or {}
                    key = rowtext.encode([vals.get(k) for k in pks])
                    if key in wanted and key not in found:
                        found.add(key)
                        if ev.packet.server_id == me:
                            by_me += 1
                        else:
                            others.add(ev.packet.server_id)
                        first = first or ev.timestamp
                        last = ev.timestamp
        finally:
            stream.close()
        import time as _t
        said = []
        before = len(wanted) - len(found)
        if before and seen <= self.WHO_WROTE_EVENTS:
            said.append(f"{before} not written since the move of"
                        f" {mark.get('at', '')} began - the target held them"
                        " before it and was not emptied of them")
        if found:
            said.append(
                f"{len(found)} written since it began"
                + (f", {by_me} by the target's own sessions" if by_me else "")
                + (f", {len(found) - by_me} arriving from server"
                   f" {', '.join(map(str, sorted(others)))}" if others else "")
                + f", between {_t.strftime('%H:%M:%S', _t.localtime(first))}"
                  f" and {_t.strftime('%H:%M:%S', _t.localtime(last))}")
        if seen > self.WHO_WROTE_EVENTS:
            said.append(f"read the first {self.WHO_WROTE_EVENTS:,} changes"
                        " since, and stopped there")
        return "; ".join(said)

    def _drilldown(self, db, t, pks, expr, ranges):
        scope = f"{db}.{t}"
        from .. import rowtext
        pkexpr = rowtext.mysql_row(pks)
        src, dst = {}, {}
        for w in ranges:
            src.update(dict(self._q("src",
                f"select {pkexpr}, md5({expr})"
                f" from {self._scope('src', db, t)} {w}")))
            dst.update(dict(self._q("dst",
                f"select {pkexpr}, md5({expr})"
                f" from {self._scope('dst', db, t)} {w}")))
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
            # checksum differed but per-pk found none = in-flight CDC lag
            return Result("data", scope, "ok",
                          "checksum flicker settled (in-flight replication),"
                          " 0 rows actually differ"), len(src), len(dst)
        detail = (f"missing={len(missing)} extra={len(extra)}"
                  f" changed={len(changed)}")
        fp = self._column_fingerprint(db, t)
        if fp:
            detail += f"; drift localized to columns: {', '.join(fp[:6])}"
        if extra:
            try:
                whose = self.who_wrote(db, t, extra, self._move_began(db))
            except Exception:  # noqa: BLE001 - an extra, not the finding
                whose = ""
            if whose:
                detail += f"; of the rows only the target has: {whose}"
        return Result("data", scope, "diff", detail, str(d),
                      f"migkit sync {self.hop.name} --db {db} --kind rows"
                      " --apply"), len(src), len(dst)

    def _column_fingerprint(self, db, t):
        """One scan per side, one aggregate per column: which columns
        actually differ before any row-level work."""
        try:
            cols = self._cols(db, t)
        except Exception as e:
            return self._fingerprint_failed(e)
        if not cols:
            return self._fingerprint_failed(
                f"the source lists no columns for {t}")
        # One value per hash, so there is no separator to be confused by -
        # but a NULL and the literal that stood in for it still collided, and
        # the encoding is meant to be the same everywhere
        from .. import rowtext
        expr = ", ".join(
            f"coalesce(bit_xor(conv(substring(md5("
            f"{rowtext.mysql_row([c])}), 1, 8), 16, 10)), 0)"
            for c in cols)
        try:
            a = self._q("src", f"select {expr} from `{db}`.`{t}`")[0]
            b = self._q("dst",
                        f"select {expr} from `{self._d('dst', db)}`.`{t}`")[0]
        except Exception as e:
            return self._fingerprint_failed(e)
        diff = [c for c, x, y in zip(cols, a, b) if x != y]
        out = self.hop.report_dir(db) / f"data-{t}.columns"
        if diff:
            out.write_text("columns differing between src and dst:\n"
                           + "\n".join(diff) + "\n")
        elif out.exists():
            out.unlink()
        return diff

    def _compare_pks(self, db, t, keys):
        pks = self._pk_cols(db, t)
        if not pks:
            return None
        expr = self._row_expr(db, t)
        from .. import rowtext
        pkexpr = rowtext.mysql_row(pks)
        def fetch(side):
            out = {}
            klist = sorted(keys)
            for i in range(0, len(klist), 500):
                chunk = klist[i:i + 500]
                if len(pks) == 1:
                    where = (f"cast(`{pks[0]}` as char) in ("
                             + ", ".join(["%s"] * len(chunk)) + ")")
                    args = [rowtext.parse(k)[0] for k in chunk]
                else:
                    tup = ("(" + ", ".join(f"cast(`{c}` as char)"
                                           for c in pks) + ")")
                    one = "(" + ", ".join(["%s"] * len(pks)) + ")"
                    where = tup + " in (" + ", ".join([one] * len(chunk)) + ")"
                    # decoded by the same module that encoded it; splitting
                    # on a tab here is what a tab inside a key value broke
                    args = [x for k in chunk for x in rowtext.parse(k)]
                q = (f"select {pkexpr}, md5({expr})"
                     f" from `{self._d(side, db)}`.`{t}` where {where}")
                out.update({r[0]: r[1] for r in self._q(side, q, args)})
            return out
        src, dst = fetch("src"), fetch("dst")
        missing = sorted(k for k in keys if k in src and k not in dst)
        extra = sorted(k for k in keys if k in dst and k not in src)
        changed = sorted(k for k in keys
                         if k in src and k in dst and src[k] != dst[k])
        return missing, extra, changed

    def _write_pk_files(self, db, t, missing, extra, changed):
        d = self.hop.report_dir(db)
        for kind, rows in (("missing", missing), ("extra", extra),
                           ("changed", changed)):
            f = d / f"data-{t}.{kind}"
            if rows:
                f.write_text("\n".join(rows) + "\n")
            elif f.exists():
                f.unlink()

    # delta verify: re-check only pks touched in the binlog since the saved
    # position, which advances only on a clean verify (idempotent)
    def delta_verify(self, db, limit=20000, log=None):
        try:
            from pymysqlreplication import BinLogStreamReader
            from pymysqlreplication.event import NotImplementedEvent
            from pymysqlreplication.row_event import (DeleteRowsEvent,
                                                      UpdateRowsEvent,
                                                      WriteRowsEvent)
        except ImportError:
            return [Result("delta", db, "error",
                           "pip install mysql-replication for delta verify")]
        state = self.hop.report_dir(db) / "delta-pos.json"
        if not state.exists():
            try:
                pos = self._binlog_position("src")
            except Exception:
                pos = None
            if not pos:
                return [Result("delta", db, "error",
                               "cannot read binlog position on source")]
            state.write_text(json.dumps({"log_file": pos[0],
                                         "log_pos": pos[1]}))
            return [Result("delta", db, "ok",
                           f"baseline {pos[0]}:{pos[1]} recorded,"
                           " changes are tracked from this point on")]
        from .. import rowtext
        from ..binlog_payload import register
        ck = json.loads(state.read_text())
        s = self.hop.source
        stream = BinLogStreamReader(
            connection_settings={"host": s.host, "port": s.port,
                                 "user": s.user, "passwd": s.password},
            server_id=int(self.hop.options.get("server_id", 4379)) + 1,
            resume_stream=True, blocking=False,
            log_file=ck.get("log_file"), log_pos=ck.get("log_pos"),
            only_schemas=[db],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent,
                         NotImplementedEvent, register()],
            filter_non_implemented_events=False)
        touched = {}
        nopk = set()
        n = 0
        try:
            for ev in self._row_events(stream):
                if isinstance(ev, NotImplementedEvent):
                    # the rows of a compressed transaction are not read,
                    # and the delta said "0 changes" over them: the
                    # position stays where it was, and the error says why
                    if ev.event_type in self.COMPRESSED_ROWS:
                        return [Result("delta", db, "error",
                                       self._compressed_said(
                                           ck, ev.event_type, "verified"))]
                    continue
                t = ev.table
                if self.hop.excluded(db, t):
                    # the target's own: nothing else compares it either
                    continue
                pks = self._pk_cols(db, t)
                if not pks:
                    nopk.add(t)
                    continue
                real = self._cols(db, t)

                def fix(vals):
                    if not any(k.startswith("UNKNOWN_COL") for k in vals):
                        return vals
                    return {real[int(k[11:])]: v for k, v in vals.items()}

                for row in ev.rows:
                    for vals in filter(None, (row.get("values"),
                                              row.get("after_values"),
                                              row.get("before_values"))):
                        v = fix(vals)
                        if all(p in v for p in pks):
                            # the same encoding the check writes, so one
                            # drilldown file never holds two forms and the
                            # repair's reader has one thing to read
                            touched.setdefault(t, set()).add(
                                rowtext.encode([v[p] for p in pks]))
                    n += 1
                if n >= limit:
                    break
            end = {"log_file": stream.log_file, "log_pos": stream.log_pos}
        finally:
            stream.close()
        if not touched and not nopk:
            state.write_text(json.dumps(end))
            return [Result("delta", db, "ok",
                           "0 changes since last verified position")]
        res = []
        clean = True
        for t, keys in sorted(touched.items()):
            # a mapped table is compared by the pair, through the mapping,
            # and the pair keeps its own drilldown
            pair = self.through_pair(db, t)
            cmp = self._delta_compare(db, t, keys)
            if cmp is None:
                res.append(Result("delta", f"{db}.{t}", "error",
                                  "pk lookup failed"))
                clean = False
                continue
            missing, extra, changed = cmp
            if missing or extra or changed:
                clean = False
                if not pair:
                    self._write_pk_files(db, t, missing, extra, changed)
                res.append(Result(
                    "delta", f"{db}.{t}", "diff",
                    f"of {len(keys)} touched rows: missing={len(missing)}"
                    f" extra={len(extra)} changed={len(changed)}",
                    str(self.hop.report_dir(db)),
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
        if clean:
            state.write_text(json.dumps(end))
            note = "position advanced"
        else:
            note = "position NOT advanced, window replays next cycle"
        if n >= limit:
            note += f"; window truncated at {limit} events, more pending"
        res.insert(0, Result("delta", db, "ok" if clean else "diff",
                             f"{sum(len(v) for v in touched.values())}"
                             f" changed rows across {len(touched)} tables,"
                             f" {note}"))
        return res

    #: Types whose values behave like the LOBs a mover has a mode for.
    LOB_TYPES = ("blob", "tinyblob", "mediumblob", "longblob",
                 "text", "tinytext", "mediumtext", "longtext",
                 "json", "varbinary")

    def _lob_check(self, db):
        """The biggest value per large column, on both sides.

        The ceiling here is the server's own `max_allowed_packet` - a single
        row larger than it cannot be sent at all, and the default is small
        enough to matter (measured on MySQL 8: 67,108,864). What the check
        is really for is the comparison: a target whose biggest value is
        smaller than the source's is what a mover truncating past its LOB
        limit leaves behind, and the row counts agree throughout.
        """
        ddb = self._d("dst", db)
        try:
            cols = [(r[0], r[1]) for r in self._q(
                "src", "select table_name, column_name"
                       " from information_schema.columns"
                       " where table_schema = %s and data_type in ("
                       + ", ".join(["%s"] * len(self.LOB_TYPES)) + ")"
                       " order by table_name, column_name",
                       (db, *self.LOB_TYPES))]
            limit = int(self._q("dst", "select @@max_allowed_packet")[0][0])
        except Exception as e:
            return Result("deep", f"{db} lobs", "error",
                          "could not size the large columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        findings = []
        for table, column in cols:
            q = "select coalesce(max(length(`{}`)), 0) from `{}`.`{}`"
            try:
                src = int(self._q("src", q.format(column, db, table))[0][0])
            except Exception:
                continue
            try:
                dst = int(self._q("dst", q.format(column, ddb, table))[0][0])
            except Exception:
                dst = None
            findings.append((table, column, src, dst, limit))
        return self._lob_result(
            db, findings, "server's max_allowed_packet",
            "raise the mover's LOB size limit above the biggest value, or"
            " move those tables with a path that does not truncate")

    #: room around the largest row for the rest of its statement: its
    #: other columns, the INSERT text, and the small rows a loader packs in
    #: beside it (a loader closes a statement once it passes about 1 MB)
    PACKET_MARGIN = 2 * 1024 * 1024
    #: the most `max_allowed_packet` takes
    PACKET_CEILING = 1024 ** 3

    def _file_sizes(self, db):
        """{table: bytes on disk} for tables in a file of their own, the
        partitions summed. What no row of the table can be larger than,
        read from the file itself rather than from statistics that lag the
        rows. {} where the server does not say."""
        for view in ("innodb_tablespaces", "innodb_sys_tablespaces"):
            try:
                rows = self._q("src", f"select name, file_size from"
                                      f" information_schema.{view}"
                                      " where name like %s", (f"{db}/%",))
                break
            except Exception:
                continue
        else:
            return {}
        out = {}
        for name, size in rows:
            table = re.split(r"#[pP]#", str(name).split("/", 1)[1])[0]
            if size:
                out[table] = out.get(table, 0) + int(size)
        return out

    def _row_sizes(self, db, fits_under, deadline):
        """[(table, kind, bytes)] for each table in scope with large
        columns: "bounded" where the table's whole file is under
        `fits_under`, so no row of it can be larger; "measured", the
        largest row's large columns, read before `deadline`; "unknown"
        where the time ran out first."""
        cols = {}
        for t, c in self._q("src", "select table_name, column_name"
                                   " from information_schema.columns"
                                   " where table_schema = %s and data_type"
                                   " in (" + ", ".join(["%s"] * len(
                                       self.LOB_TYPES)) + ")"
                                   " order by table_name, ordinal_position",
                            (db, *self.LOB_TYPES)):
            if not self.hop.excluded(db, str(t)):
                cols.setdefault(str(t), []).append(str(c))
        files = self._file_sizes(db) if cols else {}
        # a compressed table's file is no bound on its rows: measured, an
        # 8 MiB value in a ROW_FORMAT=COMPRESSED table left a 73,728-byte
        # file. Page and column compression are read as rows too.
        if files:
            for (t,) in self._q(
                    "src", "select table_name from information_schema.tables"
                           " where table_schema = %s and (row_format ="
                           " 'Compressed' or lower(create_options) like"
                           " '%%compress%%') union select table_name from"
                           " information_schema.columns where table_schema"
                           " = %s and lower(column_type) like"
                           " '%%compressed%%'", (db, db)):
                files.pop(str(t), None)
        mariadb = self._brands()[0].name == "mariadb"
        out = []
        # the largest files first: they are the likeliest to hold the row
        # that does not fit, and the time may run out before the rest
        for t in sorted(cols, key=lambda t: -files.get(t, 1 << 62)):
            if files.get(t) and files[t] < fits_under:
                out.append((t, "bounded", files[t]))
                continue
            left = deadline - time.monotonic()
            if left <= 0:
                out.append((t, "unknown", None))
                continue
            size = "+".join(f"coalesce(length(`{c.replace('`', '``')}`), 0)"
                            for c in cols[t])
            table = f"`{db.replace('`', '``')}`.`{t.replace('`', '``')}`"
            sql = (f"set statement max_statement_time = {left:.3f} for"
                   f" select max({size}) from {table}" if mariadb else
                   f"select /*+ MAX_EXECUTION_TIME({max(int(left * 1000), 1)})"
                   f" */ max({size}) from {table}")
            try:
                got = self._q("src", sql)
            except Exception as e:
                if "interrupted" not in str(e).lower() and \
                        "execution time" not in str(e).lower():
                    raise
                out.append((t, "unknown", None))
                continue
            out.append((t, "measured", int(got[0][0] or 0)))
        return out

    def _packet_to_set(self, largest):
        """The target's `max_allowed_packet` that carries a row whose large
        columns are `largest` bytes: twice that, since a loader escapes
        binary values and a value of zero bytes comes out twice as long
        (measured: an 8 MiB value of zero bytes stopped a load a 9 MiB
        packet carried in letters), plus the rest of its statement, in
        whole MiB, at most the ceiling."""
        mib = 1024 * 1024
        want = 2 * largest + self.PACKET_MARGIN
        return min(-(-want // mib) * mib, self.PACKET_CEILING)

    def _packet_items(self):
        """Does the largest row on the source fit the target's
        `max_allowed_packet`.

        Measured on 8.4: a row whose value was 8 MiB, loaded into a target
        with a 4 MiB packet, stopped the copy with `Lost connection` and
        nothing else - no word of the packet, the table emptied for the
        load and left empty. The largest row is read per table, only its
        large columns and only where the table's file on disk is big
        enough to hold such a row, within `lob_scan_seconds` (default 60)
        for the whole source; what the time did not reach is said.
        """
        item = "largest row against the target's max_allowed_packet"
        try:
            limit = int(self._q("dst", "select @@global.max_allowed_packet"
                                )[0][0])
        except Exception as e:
            return [{"level": "warn", "scope": "instance", "item": item,
                     "detail": "could not read the target's"
                               f" max_allowed_packet: {str(e)[-90:]} -"
                               " unknown, not clean"}]
        deadline = time.monotonic() + float(
            self.hop.options.get("lob_scan_seconds", 60))
        how = ("set persist max_allowed_packet = {} on the target (in the"
               " parameter group on a managed service)")
        items = []
        for db in self.databases():
            def add(level, detail):
                items.append({"level": level, "scope": db, "item": item,
                              "detail": detail})
            try:
                sizes = self._row_sizes(
                    db, (limit - self.PACKET_MARGIN) / 2, deadline)
            except Exception as e:
                add("warn", f"could not size the rows: {str(e)[-90:]} -"
                            " unknown, not clean")
                continue
            if not sizes:
                continue
            measured = sorted(((n, t) for t, k, n in sizes
                               if k == "measured"), reverse=True)
            unknown = [t for t, k, _ in sizes if k == "unknown"]
            largest = measured[0][0] if measured else 0
            over = [(n, t) for n, t in measured if n + 65536 > limit]
            tight = [(n, t) for n, t in measured
                     if (n, t) not in over
                     and 2 * n + self.PACKET_MARGIN > limit]
            said = ", ".join(f"{t} {n:,} bytes"
                             for n, t in (over or tight)[:4])
            if over:
                add("fail", f"{said}: larger than the target's"
                            f" max_allowed_packet of {limit:,}, and the"
                            " copy stops on such a row with a lost"
                            " connection - "
                            + how.format(self._packet_to_set(largest)))
            elif tight:
                add("warn", f"{said}: within the target's"
                            f" max_allowed_packet of {limit:,}, but a"
                            " binary value can double once escaped for"
                            " the load - "
                            + how.format(self._packet_to_set(largest)))
            if unknown:
                add("warn", f"{len(unknown)} tables with large columns not"
                            f" measured within lob_scan_seconds:"
                            f" {', '.join(unknown[:5])}"
                            + (" ..." if len(unknown) > 5 else "")
                            + " - unknown, not clean")
            if not (over or tight or unknown):
                add("pass", f"{len(sizes)} tables with large columns; the"
                            " largest row"
                            + (f" measured is {largest:,} bytes" if measured
                               else "s are bounded by their files on disk")
                            + f", within the target's {limit:,} even"
                              " escaped")
        return items

    def why_it_stopped(self, message):
        """A load that loses its connection says nothing else; a row
        larger than the target's packet is the cause measured, so it is
        looked for."""
        if "lost connection" not in str(message).lower():
            return ""
        try:
            items = self._packet_items()
        except Exception:
            return ""
        hit = [i for i in items if i["level"] in ("fail", "warn")
               and "max_allowed_packet =" in i["detail"]]
        return (f"The likely cause, in {hit[0]['scope']}: {hit[0]['detail']}."
                if hit else "")

    def rows_present(self, db, tables):
        ddb = self._d("dst", db)
        got = set()
        for name in tables:
            _, _, tbl = name.rpartition(".")
            try:
                if self._q("dst", f"select 1 from `{ddb}`.`{tbl}` limit 1"):
                    got.add(name)
            except Exception:
                continue
        return got

    def moved_nothing(self, db):
        """Which tables the source has rows in and the target does not."""
        ddb = self._d("dst", db)
        try:
            names = [r[0] for r in self._q(
                "src", "select table_name from information_schema.tables"
                       " where table_schema = %s and table_type in ('BASE TABLE', 'SYSTEM VERSIONED')"
                       " order by 1", (db,))]
            there = set(self._all_tables("dst", db))
        except Exception:
            return None
        empty = []
        for t in names:
            try:
                has_src = bool(self._q("src", "select 1 from"
                                       f" {self._scope('src', db, t)} limit 1"))
                # a table the target does not have received nothing: asking
                # it anyway failed, and the guard gave up on the whole move
                has_dst = t in there and bool(self._q(
                    "dst", "select 1 from"
                           f" {self._scope('dst', db, t)} limit 1"))
            except Exception:
                return None
            if has_src and not has_dst:
                empty.append(t)
        return empty

    def settle_target(self, db, from_source=True):
        """`ANALYZE TABLE` every table that was just loaded.

        Measured on MySQL 8: after loading 50,000 rows the stored estimate
        read 19, and one ANALYZE moved it to 50,456. InnoDB's automatic
        recalculation gets there eventually; a migration should not hand
        over a database that is waiting for it.
        """
        ddb = self._d("dst", db)
        try:
            tables = [r[0] for r in self._q(
                "dst", "select table_name from information_schema.tables"
                       " where table_schema = %s and table_type in ('BASE TABLE', 'SYSTEM VERSIONED')"
                       " order by 1", (ddb,))]
        except Exception as e:
            return f"could not list {ddb} to analyze: {str(e)[:80]}"
        if not tables:
            return None
        done = 0
        for t in tables:
            try:
                self._q("dst", f"analyze table `{ddb}`.`{t}`")
                done += 1
            except Exception:
                continue
        return f"analyzed {done}/{len(tables)} tables on the target"

    def _planner_stats(self, db):
        """What this engine can and cannot prove about the target's
        statistics.

        The two cheap signals were both measured on MySQL 8 and neither can
        carry the check on its own:

            right after loading 50,000 rows
                information_schema.tables.table_rows   0
                mysql.innodb_table_stats.n_rows        19
            after ANALYZE TABLE                        50,456
            after an UPDATE, and a `flush tables`
                information_schema.tables.update_time  unchanged

        So `n_rows` is wrong by three orders of magnitude while the load is
        settling, and `update_time` does not advance when the data is
        written - a check built on it would call stale statistics fresh,
        which is the failure this project refuses to ship. InnoDB's
        automatic recalculation did correct itself here (19 -> 50,456 ->
        50,507 against 50,000 real rows), which is why this is a gap worth
        stating rather than an alarm worth raising.

        Proving it properly needs the target's real row count, which the
        counts pass already has - joining the two is the work this leaves
        open.
        """
        return Result("deep", f"{db} statistics", "skip",
                      "migkit cannot prove this on MySQL yet: measured,"
                      " innodb_table_stats.n_rows read 19 for a table"
                      " holding 50,000 rows while the load settled, and"
                      " information_schema update_time did not move when"
                      " the table was written - a verdict from either would"
                      " be guesswork")

    def _quote_ident(self, name):
        return "`" + str(name).replace("`", "``") + "`"

    def _quote_literal(self, value):
        """MySQL reads a backslash inside a string as an escape unless the
        session sets NO_BACKSLASH_ESCAPES, which the default sql_mode does
        not. Doubling the quote alone - correct everywhere else - would turn
        a value holding `\\n` into a newline on the way in."""
        return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

    def _qualified(self, side, db, table):
        return (self._quote_ident(self._d(side, db)) + "."
                + self._quote_ident(table))

    def _scalar(self, side, db, sql):
        rows = self._q(side, sql)
        return [None if v is None else str(v) for v in rows[0]] if rows \
            else None

    def _zone_fingerprints(self, side, db):
        """The same reading as PostgreSQL's, which is why the two engines
        produce the same fingerprint for the same zone.

        `ifnull` rather than bare `convert_tz`: `concat_ws` drops NULLs, so
        a zone that answers nothing would otherwise fingerprint the same as
        a zone with one fewer probe."""
        readings = ", ".join(
            f"ifnull(convert_tz('{p}','UTC',Name),'?')" for p in self.TZ_PROBES)
        rows = self._q(side, "select Name, md5(concat_ws(','"
                             f", {readings})) from mysql.time_zone_name"
                             " order by Name")
        return {r[0]: r[1] for r in rows}

    def _duplicate_keys(self, db, reason=""):
        """migkit has no measured trigger for this on MySQL, and says so
        rather than either hunting blindly or implying the database is safe.

        The two states that make a PostgreSQL unique index stop enforcing
        were both measured absent here: collations are compiled into the
        server (286 of 286 `IS_COMPILED=Yes`), and a failed index build
        rolls back completely under atomic DDL.

        The MySQL-shaped candidate is `unique_checks = 0`, which mysqldump
        writes into the file it hands you - InnoDB is documented as being
        allowed to skip the check when the index page is not in the buffer
        pool. Tried on 8.4: the duplicate was **still** rejected with error
        1062, because a small index is cached. So the risk is real in the
        documentation and unreproduced in this sandbox, which is not the
        same as absent - and inventing a trigger from it would mean
        scanning every table on a guess.
        """
        return self._duplicate_hunt_result(
            db, [], 0, 0, reason,
            "decide which copy survives, delete the rest, then rebuild the"
            " index") if reason else Result(
            "deep", f"{db} duplicate keys", "skip",
            "no measured way for a MySQL unique index to stop enforcing:"
            " collations are compiled in and a failed index build rolls"
            " back - and unique_checks=0, which a logical dump sets, still"
            " rejected a duplicate here (error 1062) because the index was"
            " cached. Unreproduced is not absent, so migkit claims nothing"
            " either way")

    TEXT_TYPES = ("char", "varchar", "tinytext", "text", "mediumtext",
                  "longtext")

    def _mojibake(self, db):
        """The same sampling as PostgreSQL, through a driver instead of a
        client: `length <> char_length` is the same "has a non-ASCII
        character" test, and the counting and the verdict are the base's."""
        findings, scanned = [], 0
        try:
            cols = {}
            place = ",".join(["%s"] * len(self.TEXT_TYPES))
            for t, c in self._q("src",
                                "select c.table_name, c.column_name"
                                " from information_schema.columns c"
                                " join information_schema.tables t"
                                " on t.table_schema = c.table_schema"
                                " and t.table_name = c.table_name"
                                " where c.table_schema = %s"
                                " and t.table_type in ('BASE TABLE', 'SYSTEM VERSIONED')"
                                " and c.table_name not like 'migkit%%'"
                                f" and c.data_type in ({place})"
                                " order by 1, 2",
                                (db,) + self.TEXT_TYPES):
                cols.setdefault(t, []).append(c)
            for table, columns in sorted(cols.items()):
                quoted = ["`" + c.replace("`", "``") + "`" for c in columns]
                where = " or ".join(f"length({q}) <> char_length({q})"
                                    for q in quoted)
                rows = self._q("src",
                               f"select {', '.join(quoted)} from"
                               f" `{db.replace('`', '``')}`."
                               f"`{table.replace('`', '``')}`"
                               f" where {where}"
                               f" limit {self.MOJIBAKE_SAMPLE}")
                found, seen = self._mojibake_tally(table, columns, rows)
                findings += found
                scanned += seen
        except Exception as e:
            return Result("deep", f"{db} mojibake", "error",
                          "could not sample the source's text columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._mojibake_result(
            db, findings, scanned,
            "convert the column once on the source before moving it, and"
            " re-check afterwards - a second pass over already-repaired text"
            " breaks it again")

    def _mojibake_repair(self, db):
        """The row-by-row repair the mojibake check asks for, as PostgreSQL
        has it: only the values that re-encode to valid UTF-8 are touched,
        each update matching the old value as well as the key, and the
        genuinely accented text next to them is left alone.

        It reads the **target**: migkit writes to the source nowhere. The
        decision of which values to repair is the base's
        (`_mojibake_updates`); this supplies the rows. A table without a
        primary key cannot address a row, and a broken key column is not
        rewritten - both are named rather than attempted."""
        ddb = self._d("dst", db)
        place = ",".join(["%s"] * len(self.TEXT_TYPES))
        cols = {}
        for t, c in self._q("dst", "select c.table_name, c.column_name"
                                   " from information_schema.columns c"
                                   " join information_schema.tables t"
                                   " on t.table_schema = c.table_schema"
                                   " and t.table_name = c.table_name"
                                   " where c.table_schema = %s and"
                                   " t.table_type in ('BASE TABLE',"
                                   " 'SYSTEM VERSIONED')"
                                   " and c.table_name not like 'migkit%%'"
                                   f" and c.data_type in ({place})"
                                   " order by 1, 2",
                            (ddb,) + self.TEXT_TYPES):
            if not self.hop.excluded(db, str(t)):
                cols.setdefault(str(t), []).append(str(c))
        stmts, undo, touched, refused = [], [], set(), []
        repaired = clean = skipped = left = 0
        q = self._quote_ident
        for table, columns in sorted(cols.items()):
            key = [str(r[0]) for r in self._q("dst", self.PK_SQL,
                                              (ddb, table))]
            if not key:
                refused.append((table, "no primary key, so no row can be"
                                       " addressed"))
                continue
            for c in columns:
                if c in key:
                    refused.append((f"{table}.{c}", "the key itself -"
                                    " rewriting it would move rows other"
                                    " tables point at"))
            want = [c for c in columns if c not in key]
            if not want:
                continue
            where = " or ".join(f"length({q(c)}) <> char_length({q(c)})"
                                for c in want)
            rows = [list(r) for r in self._q(
                "dst", f"select {', '.join(q(c) for c in key + want)}"
                       f" from {q(ddb)}.{q(table)} where {where}"
                       f" limit {self.MOJIBAKE_REPAIR_CAP + 1}")]
            s, u, t, rep, cl, sk, lf = self._mojibake_updates(
                db, table, key, want, rows)
            stmts += s
            undo += u
            touched |= t
            repaired += rep
            clean += cl
            skipped += sk
            left += lf
        if not repaired:
            return None
        enabled = self._repair_text_enabled()
        note = self._mojibake_repair_note(repaired, touched, clean, skipped,
                                          left, refused, enabled)
        return RepairAction(db, "text", stmts if enabled else [],
                            undo if enabled else [], note)

    def _collation_versions(self, db):
        """MySQL cannot have this problem, and that is a design difference
        worth stating rather than a check worth faking.

        PostgreSQL borrows its sort order from the operating system, so a
        C library upgrade changes it under every text index. MySQL compiles
        its collations into the server: measured on 8.4, all **286** rows of
        `information_schema.collations` report `IS_COMPILED = Yes`, and the
        table has no version column at all - CHARACTER_SET_NAME,
        COLLATION_NAME, ID, IS_COMPILED, IS_DEFAULT, PAD_ATTRIBUTE, SORTLEN
        and nothing else. Upgrading the OS underneath it changes nothing.

        The MySQL-shaped version of this risk is the *server* changing the
        default for new objects - 5.7's `utf8mb4_general_ci` against 8.0's
        `utf8mb4_0900_ai_ci`, which also differ in padding (measured: PAD
        SPACE against NO PAD, so `'a '` and `'a'` compare equal under one
        and not the other). That lands as a per-column collation difference
        between the two sides, which `{db} collation` already compares and
        tests for collapse.
        """
        return Result("deep", f"{db} collation versions", "skip",
                      "MySQL compiles its collations into the server"
                      " (measured: 286 of 286 report IS_COMPILED=Yes, and"
                      " information_schema.collations has no version column),"
                      " so no OS upgrade can re-sort an index underneath it"
                      " - the version-shaped risk here is the server default"
                      " changing for new objects, which the collation check"
                      " compares per column")

    def _invalid_indexes(self, db):
        """PostgreSQL's half-built index has no MySQL equivalent, and that
        is a measured answer rather than an assumed one.

        On MySQL 8, `ALTER TABLE ... ADD UNIQUE INDEX` over duplicate rows
        fails with 1062 and leaves **nothing** behind: `information_schema.
        statistics` for the schema came back empty, and `innodb_indexes`
        held only `GEN_CLUST_INDEX`. The control matters as much as the
        result - a valid index created afterwards did appear in the same
        query, so the empty answer was the absence of an index and not a
        query that reads nothing. Atomic DDL, doing what it says.

        The one state MySQL does have is a secondary index InnoDB has
        marked corrupt, and no catalog column exposes it - it surfaces only
        as error 1712 when something touches the index. Unknown, not zero.
        """
        return Result("deep", f"{db} indexes", "skip",
                      "no half-built index state on MySQL: measured, a failed"
                      " ADD UNIQUE INDEX rolled back completely (atomic DDL)"
                      " while a valid index created next did show up in the"
                      " same query - and a corrupt index is exposed only as"
                      " error 1712, with no catalog column to read")


    # --- the source's own reads, planned and timed on both sides (G1) ----

    #: statements of migkit's own checks, left out of the application's
    MIGKIT_MARKS = ("information_schema", "performance_schema", "bit_xor(",
                    "md5(", "`mysql`.",
                    # a client reading its settings, not the application
                    "select @@")

    def workload_reads(self, db, n):
        """[(label, statement, runnable)] - the source's busiest reads by
        total time, from `performance_schema`'s digests. The label is the
        digest's normalised text, never the sample's literal values."""
        rows = self._q("src", "select DIGEST_TEXT, QUERY_SAMPLE_TEXT from"
                              " performance_schema"
                              ".events_statements_summary_by_digest"
                              " where SCHEMA_NAME = %s and DIGEST_TEXT"
                              " like 'SELECT %%'"
                              " order by SUM_TIMER_WAIT desc limit %s",
                       (self._d("src", db), int(n) * 3))
        out = []
        for digest, sample in rows:
            text = f"{digest} {sample}".lower()
            if any(m in text for m in self.MIGKIT_MARKS):
                continue
            sample = sample or ""
            # a sample cut at the server's text limit is not a statement
            whole = bool(sample) and not sample.rstrip().endswith("...") \
                and "?" not in sample
            out.append((" ".join(str(digest).split())[:70], sample, whole))
            if len(out) >= n:
                break
        return out

    def _read_only(self, side, db):
        conn = self._conn(side)
        cur = conn.cursor()
        cur.execute(f"use {self._my_ident(self._d(side, db))}")
        cur.execute("set session max_execution_time = 5000")
        cur.execute("start transaction read only")
        return conn, cur

    def workload_plan(self, side, db, sql):
        """{table: "index <name>" or None where the plan reads it whole}."""
        import json as _json
        conn, cur = self._read_only(side, db)
        try:
            cur.execute(f"explain format=json {sql}")
            plan = _json.loads(cur.fetchone()[0])
        finally:
            conn.rollback()
            conn.close()
        out = {}

        def walk(node):
            if isinstance(node, dict):
                t = node.get("table")
                if isinstance(t, dict) and t.get("table_name"):
                    out[t["table_name"]] = (
                        None if t.get("access_type") == "ALL"
                        else f"index {t.get('key')}" if t.get("key")
                        else out.get(t["table_name"]))
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)
        walk(plan)
        return out

    def workload_time(self, side, db, sql):
        """The statement's time in ms, from `EXPLAIN ANALYZE`, run
        read-only; no rows come back."""
        import re
        conn, cur = self._read_only(side, db)
        try:
            cur.execute(f"explain analyze {sql}")
            text = cur.fetchone()[0]
        finally:
            conn.rollback()
            conn.close()
        m = re.search(r"actual time=[\d.]+\.\.([\d.]+)", text)
        if m:
            return float(m.group(1))
        # answered before execution (`max` over an index): nothing ran
        return 0.0 if "before execution" in text else None

    def stream_identity(self, side, db):
        """The server's own id: a binlog file and offset mean something in
        one server's log only."""
        got = self._q(side, "select @@server_uuid")
        return {"server": str(got[0][0])} if got else None

    def log_kept(self, db, grows):
        """A binlog is kept whole until it expires, however long the load
        took to write it."""
        on, expire = self._q("dst", "select @@log_bin,"
                                    " @@binlog_expire_logs_seconds")[0]
        if not int(on):
            return (0, "none: the target keeps no binlog")
        days = int(expire) / 86400
        return (grows, "all of it, for"
                       f" {days:g} days (binlog_expire_logs_seconds)"
                if expire else "all of it, until someone purges it")

    def text_encodings(self, side, db):
        """Per column: a database's default says nothing about a column
        declared in another character set, which is where a 3-byte UTF-8
        column sits inside a utf8mb4 database."""
        got = {str(c): int(n) for c, n in self._q(
            side, "select character_set_name, count(*) from"
                  " information_schema.columns where table_schema = %s and"
                  " character_set_name is not null group by 1",
            (self._d(side, db),))}
        if got:
            return got
        rows = self._q(side, "select default_character_set_name from"
                             " information_schema.schemata where"
                             " schema_name = %s", (self._d(side, db),))
        return {str(rows[0][0]): 0} if rows else None

    def neutral_create_code(self, side, db, statement):
        if side != "dst":
            raise RuntimeError("migkit does not write to the source")
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.execute(f"use {self._my_ident(self._d(side, db))}")
                cur.execute(statement)
            conn.commit()
        finally:
            conn.close()

    def neutral_views(self, side, db):
        return [(n, d) for n, d in self._q(
            side, "select table_name, view_definition from"
                  " information_schema.views where table_schema = %s"
                  " order by 1", (self._d(side, db),))
            if not self.hop.excluded(db, n)]

    def neutral_functions(self, side, db):
        name = self._d(side, db)
        out = []
        for fn, kind, body, returns in self._q(
                side, "select routine_name, routine_type,"
                      " routine_definition, dtd_identifier from"
                      " information_schema.routines where routine_schema"
                      " = %s order by 1", (name,)):
            text = " ".join(str(body or "").split())
            one = kind == "FUNCTION" and text.lower().startswith("return ")
            params = [(p, t) for p, t in self._q(
                side, "select parameter_name, dtd_identifier from"
                      " information_schema.parameters where specific_schema"
                      " = %s and specific_name = %s and ordinal_position > 0"
                      " order by ordinal_position", (name, fn))]
            out.append((fn, params, returns, text[7:] if one else None))
        return out

    def code_definition(self, side, db, kind, name):
        what = {"view": "view", "function": "function",
                "procedure": "procedure"}.get(kind)
        if not what:
            return None
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.execute(f"use {self._my_ident(self._d(side, db))}")
                cur.execute(f"show create {what} {self._my_ident(name)}")
                row = cur.fetchone()
        finally:
            conn.close()
        # the statement is the column after the name (and, for a view, is
        # the second of four)
        return next((c for c in row[1:] if isinstance(c, str)
                     and c.lower().lstrip().startswith("create")), None)

    def neutral_function_sql(self, name, params, returns, body):
        # a server writing a binary log refuses a function that declares
        # nothing about what it reads; this one reads, at most
        return (f"create function {self._quote_ident(name)}"
                f"({', '.join(f'{self._quote_ident(p)} {t}' for p, t in params)})"
                f" returns {returns} reads sql data return {body}")

    def zone_evidence(self, side, db):
        """(the server's UTC offset in minutes, its UTC clock, [(table.column,
        latest value)]) for the `datetime` columns an index leads - read
        by the index, so each is one probe (`zones`)."""
        name = self._d(side, db)
        offset, now = self._q(side, "select timestampdiff(minute,"
                                    " utc_timestamp(), now()),"
                                    " utc_timestamp(6)")[0]
        cols = self._q(side, "select s.table_name, s.column_name from"
                             " information_schema.statistics s join"
                             " information_schema.columns c on"
                             " c.table_schema = s.table_schema and"
                             " c.table_name = s.table_name and"
                             " c.column_name = s.column_name"
                             " where s.table_schema = %s and"
                             " s.seq_in_index = 1 and c.data_type ="
                             " 'datetime' group by s.table_name,"
                             " s.column_name limit 30", (name,))
        heads = []
        for t, c in cols:
            if self.hop.excluded(db, t):
                continue
            latest = self._q(side, f"select max(`{c}`) from"
                                   f" {self._my_ident(name)}.`{t}`")[0][0]
            heads.append((f"{t}.{c}", latest))
        return int(offset), now, heads

    def check_deep(self, db):
        from .. import workload, zones
        res = [self._planner_stats(db), workload.compare(self, db),
               zones.infer(self, db),
               self._lob_check(db),
               self._invalid_indexes(db),
               self._collation_versions(db),
               self._mojibake(db),
               self._duplicate_keys(db),
               self._temporal_meaning(db),
               self._time_zone_rules(db),
               self._capacity_gaps(db)]
        ddb = self._d("dst", db)

        # no pk/unique = CDC drops its updates/deletes and it can't be verified
        # or repaired by key (the same trap postgres has, without the InnoDB
        # guardrails). Check the source tables that are about to be migrated.
        nopk = [r[0] for r in self._q("src",
                "select t.table_name from information_schema.tables t"
                " where t.table_schema=%s and t.table_type in ('BASE TABLE', 'SYSTEM VERSIONED')"
                " and t.table_name not like 'migkit%%'"
                " and not exists (select 1 from"
                " information_schema.table_constraints tc"
                " where tc.table_schema=t.table_schema"
                " and tc.table_name=t.table_name"
                " and tc.constraint_type in ('PRIMARY KEY','UNIQUE'))", (db,))]
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

        res.append(self._fk_orphans(db))

        colq = ("select concat(table_name, '.', column_name), column_type,"
                " is_nullable, coalesce(column_default, ''),"
                " coalesce(character_set_name, ''),"
                " coalesce(collation_name, ''), extra"
                " from information_schema.columns where table_schema=%s"
                " and table_name not like 'migkit%%' order by 1")
        sc = {r[0]: r[1:] for r in self._q("src", colq, (db,))}
        dc = {r[0]: r[1:] for r in self._q("dst", colq, (ddb,))}
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

        # narrowing = silent truncation/overflow: a target column shorter than
        # the source (fewer chars, less decimal scale/precision, smaller int,
        # or unsigned turned signed) quietly cuts or wraps values.
        CHAR_T = ("char", "varchar", "tinytext", "text", "mediumtext",
                  "longtext")
        INTW = {"tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4,
                "bigint": 8}
        nq = ("select concat(table_name,'.',column_name), data_type,"
              " coalesce(character_maximum_length,0),"
              " coalesce(numeric_precision,0), coalesce(numeric_scale,0),"
              " column_type from information_schema.columns"
              " where table_schema=%s and table_name not like 'migkit%%'")
        scn = {r[0]: r[1:] for r in self._q("src", nq, (db,))}
        dcn = {r[0]: r[1:] for r in self._q("dst", nq, (ddb,))}
        narrow = []
        for k in sorted(scn):
            if k not in dcn:
                continue
            st, scm, spr, ssc, sct = scn[k]
            dt, dcm, dpr, dsc, dct = dcn[k]
            why = None
            if st in CHAR_T and scm and dcm and int(dcm) < int(scm):
                why = f"char {scm} -> {dcm}"
            elif st == "decimal" and ssc and int(dsc) < int(ssc):
                why = f"decimal scale {ssc} -> {dsc} (rounds)"
            elif st == "decimal" and spr and int(dpr) < int(spr):
                why = f"decimal precision {spr} -> {dpr} (overflow)"
            elif INTW.get(st, 0) > INTW.get(dt, 99):
                why = f"{st} -> {dt} (overflow)"
            elif "unsigned" in sct and "unsigned" not in dct \
                    and st in INTW:
                why = f"unsigned -> signed ({sct} -> {dct})"
            if why:
                narrow.append(f"{k}: {why}")
        if narrow:
            res.append(Result("deep", f"{db} narrowing", "diff",
                              f"{len(narrow)} target columns NARROWER than"
                              " source (silent truncation/overflow risk): "
                              + "; ".join(narrow[:6]), "",
                              "widen the target column to match source before"
                              " loading, or values are cut/rounded/overflowed"))
        else:
            res.append(Result("deep", f"{db} narrowing", "ok",
                              "no target column narrower than source"))

        # a constant per-row offset on a datetime/timestamp column is a tz
        # conversion bug (mover applied a non-UTC session), not row corruption.
        # Both sides read with time_zone pinned to +00:00, so a faithful copy
        # compares as delta 0 and a systematic shift shows the same delta.
        tcols = self._q("src",
                        "select table_name, column_name"
                        " from information_schema.columns where table_schema=%s"
                        " and data_type in ('datetime','timestamp')"
                        " and table_name not like 'migkit%%'", (db,))
        shifts = []
        for t, col in tcols[:10]:
            pk = self._pk_cols(db, t)
            if len(pk) != 1:
                continue
            p = pk[0]

            def rows(side, dbn):
                q = (f"select `{p}`, unix_timestamp(`{col}`)"
                     f" from `{dbn}`.`{t}` where `{col}` is not null"
                     f" order by `{p}` limit 200")
                return {str(r[0]): r[1] for r in self._rows_utc(side, q)
                        if r[1] is not None}
            sm, dm = rows("src", db), rows("dst", ddb)
            deltas = [float(dm[k]) - float(sm[k]) for k in sm if k in dm]
            if len(deltas) < 3:
                continue
            avg = sum(deltas) / len(deltas)
            if max(deltas) - min(deltas) < 1 and abs(avg) >= 1:
                secs = round(avg)
                shifts.append(f"{t}.{col}: every row shifted"
                              f" {secs}s (~{secs / 3600:.1f}h)")
        if shifts:
            res.append(Result("deep", f"{db} timeshift", "diff",
                              "uniform timezone offset (systematic, not"
                              " row-level corruption): " + "; ".join(shifts[:5]),
                              "", "target stored a non-UTC wall clock; re-load"
                              " with the source session timezone"))
        else:
            res.append(Result("deep", f"{db} timeshift", "ok",
                              "no uniform timestamp offset detected"))

        # charset corruption: a text column downgraded from utf8mb4 to a
        # narrower charset (utf8mb3/latin1) on the target truncates or drops
        # 4-byte characters (emoji, astral CJK), and a lossy transcode leaves
        # U+FFFD replacement characters - both silent.
        csq = ("select table_name, column_name, character_set_name"
               " from information_schema.columns where table_schema=%s"
               " and character_set_name is not null"
               " and table_name not like 'migkit%%'")
        scs = {(r[0], r[1]): r[2] for r in self._q("src", csq, (db,))}
        dcs = {(r[0], r[1]): r[2] for r in self._q("dst", csq, (ddb,))}
        WIDTH = {"utf8mb4": 4, "utf8mb3": 3, "utf8": 3, "latin1": 1,
                 "latin2": 1, "ascii": 1}
        csbad = []
        for k, s2 in sorted(scs.items()):
            d2 = dcs.get(k)
            if d2 and d2 != s2:
                note = f"{k[0]}.{k[1]}: charset {s2} -> {d2}"
                if WIDTH.get(d2, 9) < WIDTH.get(s2, 0):
                    note += " (DOWNGRADE, drops/truncates multibyte chars)"
                csbad.append(note)
        tcols = self._q("src",
                        "select distinct table_name, column_name"
                        " from information_schema.columns where table_schema=%s"
                        " and data_type in ('char','varchar','text','tinytext',"
                        "'mediumtext','longtext') and table_name not like"
                        " 'migkit%%'", (db,))
        for t, col in tcols[:30]:
            q = ("select coalesce(sum(char_length(convert(`{c}` using utf8mb4))"
                 " - char_length(replace(convert(`{c}` using utf8mb4),"
                 " _utf8mb4 0xEFBFBD, ''))),0) from `{d}`.`{t}`")
            try:
                sv = int(self._q("src", q.format(c=col, d=db, t=t))[0][0])
                dv = int(self._q("dst", q.format(c=col, d=ddb, t=t))[0][0])
            except Exception:
                continue
            if dv > sv:
                csbad.append(f"{t}.{col}: {dv - sv} extra U+FFFD replacement"
                             " chars on target")
        if csbad:
            res.append(Result("deep", f"{db} charset", "diff",
                              "; ".join(csbad[:6]), "",
                              "keep the target column utf8mb4 and re-load the"
                              " affected rows over a utf8mb4 connection"))
        else:
            res.append(Result("deep", f"{db} charset", "ok",
                              "text column charsets match, no replacement-char"
                              " excess"))

        # partitioned tables (mysql): a missing partition sends rows to the
        # MAXVALUE catch-all, a changed method/expression reroutes them.
        def _parts(side, dbn):
            out = {}
            for t, meth, expr, desc in self._q(side,
                    "select table_name, partition_method,"
                    " coalesce(partition_expression,''), partition_description"
                    " from information_schema.partitions where table_schema=%s"
                    " and partition_name is not null", (dbn,)):
                e = out.setdefault(t, {"m": meth, "e": expr, "b": set()})
                e["b"].add(desc)
            return out
        sp, dp = _parts("src", db), _parts("dst", ddb)
        pbad = []
        for t, si in sorted(sp.items()):
            di = dp.get(t)
            if not di:
                pbad.append(f"{t}: partitioned on source, not on target")
                continue
            if (si["m"], si["e"]) != (di["m"], di["e"]):
                pbad.append(f"{t}: partition scheme differs"
                            f" src({si['m']} {si['e']}) dst({di['m']} {di['e']})")
                continue
            miss = si["b"] - di["b"]
            if miss:
                pbad.append(f"{t}: {len(miss)} partition bound(s) missing on"
                            f" target: {', '.join(sorted(miss)[:2])}")
            if "MAXVALUE" in di["b"]:
                mv = self._q("dst", "select partition_name from"
                             " information_schema.partitions where"
                             " table_schema=%s and table_name=%s and"
                             " partition_description='MAXVALUE'", (ddb, t))
                if mv:
                    n = self._q("dst", f"select count(*) from `{ddb}`.`{t}`"
                                f" partition (`{mv[0][0]}`)")[0][0]
                    if n > 0:
                        pbad.append(f"{t}: {n} rows stranded in MAXVALUE"
                                    f" partition ({mv[0][0]})")
        if not sp:
            res.append(Result("deep", f"{db} partitions", "ok",
                              "no partitioned tables"))
        elif pbad:
            res.append(Result("deep", f"{db} partitions", "diff",
                              "; ".join(pbad[:6]), "",
                              "recreate the missing partitions and move rows"
                              " out of MAXVALUE before cutover"))
        else:
            res.append(Result("deep", f"{db} partitions", "ok",
                              f"{len(sp)} partitioned tables, schemes and"
                              " bounds match"))

        # generated/computed columns (mysql)
        genq = ("select table_name, column_name, extra,"
                " coalesce(generation_expression,'')"
                " from information_schema.columns where table_schema=%s"
                " and generation_expression <> '' and generation_expression"
                " is not null and table_name not like 'migkit%%'")
        sg = {(r[0], r[1]): (r[2], r[3]) for r in self._q("src", genq, (db,))}
        dg = {(r[0], r[1]): (r[2], r[3]) for r in self._q("dst", genq, (ddb,))}

        def _n(e):
            return re.sub(r"\s+", "", e).lower()
        gbad = []
        for k, (extra, expr) in sorted(sg.items()):
            t, col = k
            if k not in dg:
                gbad.append(f"{t}.{col}: generated on source,"
                            " plain/missing on target")
                continue
            if _n(expr) != _n(dg[k][1]):
                gbad.append(f"{t}.{col}: generation expression differs")
                continue
            if "STORED" in (extra or "").upper():
                try:
                    n = self._q("dst", f"select count(*) from `{ddb}`.`{t}`"
                                f" where not (`{col}` <=> ({dg[k][1]}))")[0][0]
                    if n > 0:
                        gbad.append(f"{t}.{col}: {n} rows where the stored"
                                    " value != its expression")
                except Exception:
                    pass
        for k in sorted(dg):
            if k not in sg:
                gbad.append(f"{k[0]}.{k[1]}: generated on target but plain"
                            " on source")
        res.append(Result("deep", f"{db} generated",
                          "diff" if gbad else "ok",
                          "; ".join(gbad[:6]) if gbad
                          else (f"{len(sg)} generated columns match"
                                if sg else "no generated columns"), "",
                          "align the generation expression/storage and"
                          " re-derive the column" if gbad else ""))

        # collation unique-collapse (mysql): a unique/pk text column on a
        # case/accent-insensitive target collation collapses distinct source
        # rows into duplicates on load.
        uqq = ("select k.table_name, k.column_name, c.collation_name"
               " from information_schema.table_constraints tc"
               " join information_schema.key_column_usage k"
               "  on k.table_schema=tc.table_schema"
               "  and k.table_name=tc.table_name"
               "  and k.constraint_name=tc.constraint_name"
               " join information_schema.columns c"
               "  on c.table_schema=k.table_schema"
               "  and c.table_name=k.table_name"
               "  and c.column_name=k.column_name"
               " where tc.table_schema=%s and tc.constraint_type"
               "  in ('PRIMARY KEY','UNIQUE')"
               "  and c.data_type in ('char','varchar','text')"
               "  and c.collation_name is not null")
        su = {(r[0], r[1]): r[2] for r in self._q("src", uqq, (db,))}
        dcu = {(r[0], r[1]): r[2] for r in self._q("dst", uqq, (ddb,))}
        cbad = []
        for k, scoll in sorted(su.items()):
            dcoll = dcu.get(k)
            if not dcoll or dcoll == scoll:
                continue
            t, col = k
            detail = f"{t}.{col}: unique-key collation src({scoll}) != dst({dcoll})"
            try:
                n = self._q("src", f"select count(*) from (select 1 from"
                            f" `{db}`.`{t}` group by `{col}` collate {dcoll}"
                            f" having count(*) > 1) x")[0][0]
                if n > 0:
                    detail += (f"; {n} source groups COLLAPSE to duplicates"
                               " under the target collation (data loss)")
            except Exception:
                detail += "; collapse untestable (collation not on source)"
            cbad.append(detail)
        res.append(Result("deep", f"{db} collation",
                          "diff" if cbad else "ok",
                          "; ".join(cbad[:6]) if cbad
                          else "unique-key collations match", "",
                          "match the unique-key collation to source (or dedup"
                          " first)" if cbad else ""))

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
        dpk = {r[0] for r in self._q("dst", pkq, (ddb,))}
        both = [(t, c) for t, c in spk if t in dpk]

        def maxes(side):
            out = {}
            sdb = self._d(side, db)
            for i in range(0, len(both), 200):
                q = " union all ".join(
                    f"select '{t}', coalesce(max(`{c}`), 0)"
                    f" from `{sdb}`.`{t}`" for t, c in both[i:i + 200])
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

        res += self._deep_nullempty(db, ddb)
        res += self._deep_float(db, ddb)
        res += self._deep_unenforced_checks(db, ddb)
        res += self._check_grants(db)
        res += self._deep_ownership(db, ddb)
        res += [r for r in (self.set_aside(db),) if r]
        return res

    def _fk_orphans(self, db):
        """Rows on the target whose foreign key points at nothing: loads run
        with foreign_key_checks=0, so the key never looked."""
        ddb = self._d("dst", db)
        rows = self._q("dst",
                       "select constraint_name, table_name, column_name,"
                       " referenced_table_name, referenced_column_name"
                       " from information_schema.key_column_usage"
                       " where table_schema=%s"
                       " and referenced_table_name is not null"
                       " order by constraint_name, ordinal_position", (ddb,))
        fks = {}
        for con, t, c, rt, rc in rows:
            fk = fks.setdefault((con, t, rt), ([], []))
            fk[0].append(c)
            fk[1].append(rc)
        orphans = []
        for (con, t, rt), (cols, rcols) in sorted(fks.items()):
            if self.hop.excluded(db, str(t)):
                continue
            nn = " and ".join(f"c.`{c}` is not null" for c in cols)
            join = " and ".join(f"p.`{r}` = c.`{c}`"
                                for c, r in zip(cols, rcols))
            n = self._q("dst", f"select count(*) from `{ddb}`.`{t}` c"
                               f" where {nn} and not exists"
                               f" (select 1 from `{ddb}`.`{rt}` p"
                               f" where {join})")[0][0]
            if n:
                orphans.append(f"{t}.{con}: {n} orphan rows")
        return Result("deep", f"{db} fk", "diff" if orphans else "ok",
                      "; ".join(orphans[:5]) if orphans
                      else f"{len(fks)} fks scanned, 0 orphan rows", "",
                      "reload the child rows or delete orphans"
                      if orphans else "")

    def create_missing(self, db, log=None):
        """The bulk path's own: each missing table from the source's `show
        create table`, and the database when it is not there at all."""
        from .. import movers
        movers._my_create_missing(self.hop, db, log)

    def set_aside(self, db):
        """The target's triggers a load took off and has not put back.

        Held by a load still running - a change tail, for as long as it
        runs - they are off on purpose and go back when it stops. Held by
        one whose process is gone, they are simply missing: the
        application's cutover onto this target would run without them, and
        nothing else in the check compares a target's own triggers.
        """
        from .. import movers
        held, lost, unread = [], [], []
        there = None
        for path, alive, defs in movers.triggers_set_aside(self.hop, db):
            if defs is None:
                unread.append(str(path))
            elif alive:
                held += sorted(defs)
            else:
                if there is None:
                    there = {str(r[0]) for r in self._q(
                        "dst", "select trigger_name from information_schema"
                               ".triggers where trigger_schema = %s",
                        (self._d("dst", db),))}
                lost += [(n, str(path)) for n in sorted(defs)
                         if n not in there]
        scope = f"{db} triggers set aside"
        if lost or unread:
            said = []
            if lost:
                said.append(f"{len(lost)} triggers a load took off the"
                            " target and never put back: "
                            + ", ".join(n for n, _ in lost[:5])
                            + " - definitions in "
                            + ", ".join(sorted({p for _, p in lost})))
            if unread:
                said.append("a record of triggers taken off cannot be"
                            " read: " + ", ".join(unread))
            return Result("deep", scope, "diff", "; ".join(said), "",
                          "the next `migkit move` into this database puts"
                          " them back; or run the statements in that file")
        if held:
            return Result("deep", scope, "ok",
                          f"{len(held)} triggers are off the target while"
                          " a migkit load runs into it ("
                          + ", ".join(held[:5]) + "); they go back when it"
                          " stops")
        return None

    OWNED = (("views", "table_schema", "table_name"),
             ("routines", "routine_schema", "routine_name"),
             ("triggers", "trigger_schema", "trigger_name"),
             ("events", "event_schema", "event_name"))

    def _deep_ownership(self, db, ddb):
        """DEFINER and SQL SECURITY drift, which the schema diff cannot see.

        `_canon_ddl` strips both before comparing, deliberately: the mover
        rewrites them on every object, and leaving them in would bury every
        real schema finding under cosmetic noise. So they are reported here
        instead, where one line can describe a change made to everything.

        Measured before this existed: a view that was `app@% / DEFINER` on the
        source and `dts_migration@% / INVOKER` on the target passed the schema
        check, the object check and atlas, all three reporting ok.
        """
        from .. import ownership as _own
        rows = []
        for table, sch_col, name_col in self.OWNED:
            has_sec = table in ("views", "routines")
            sec = ", security_type" if has_sec else ""
            q = (f"select {name_col}, definer{sec} from information_schema."
                 f"{table} where {sch_col} = %s")
            try:
                a = {r[0]: "/".join(str(x) for x in r[1:])
                     for r in self._q("src", q, (db,))}
                b = {r[0]: "/".join(str(x) for x in r[1:])
                     for r in self._q("dst", q, (ddb,))}
            except Exception as e:
                return [Result("deep", f"{db} ownership", "warn",
                               f"cannot read {table}: {str(e)[:90]} -"
                               " unknown, not clean")]
            # objects missing on one side are the object check's finding, not
            # this one; saying it twice would let the two drift apart
            rows += [(f"{table[:-1]} {n}", a[n], b[n])
                     for n in sorted(set(a) & set(b))]
        if not rows:
            return [Result("deep", f"{db} ownership", "ok",
                           "no views, routines, triggers or events")]
        changes = _own.group(rows)
        if not changes:
            return [Result("deep", f"{db} ownership", "ok",
                           f"{len(rows)} objects keep their definer"
                           " and security mode")]
        note = ("one substitution across every object - a decision or"
                " something the mover did to all of them"
                if _own.systematic(changes) else
                "objects drifted individually - read each one")
        invoker = sum(len(v) for (a, b), v in changes.items()
                      if "DEFINER" in a and "INVOKER" in b)
        detail = f"{_own.total(changes)} of {len(rows)}: {_own.describe(changes)}"
        if invoker:
            # the privilege change, not just the name change
            detail += (f"; {invoker} dropped from SQL SECURITY DEFINER to"
                       " INVOKER - they now run with the caller's privileges")
        return [Result("deep", f"{db} ownership", "diff", detail, "",
                       f"{note}; recreate with the intended DEFINER before"
                       " the application depends on it")]

    # ---- checks PostgreSQL already had and MySQL did not ----

    TEXTY = ("char", "varchar", "text", "tinytext", "mediumtext", "longtext")
    FLOATY = ("float", "double")

    def _text_columns(self, db, limit=30):
        q = ("select table_name, column_name from information_schema.columns"
             " where table_schema=%s and data_type in ("
             + ",".join(["%s"] * len(self.TEXTY)) + ")"
             " and table_name not like 'migkit%%' order by 1,2")
        return self._q("src", q, (db,) + self.TEXTY)[:limit]

    def _deep_nullempty(self, db, ddb):
        """NULL and '' are different values that a mover happily swaps.

        Both sides still hold "something" in the column, so a row checksum
        can be equal on one engine's reading and not another's - and the
        application's `IS NULL` and `= ''` branches diverge. Comparing the
        two counts per column is what catches it.
        """
        bad = []
        for tbl, col in self._text_columns(db):
            q = (f"select sum(`{col}` is null), sum(`{col}` = '')"
                 f" from `%s`.`{tbl}`")
            try:
                sv = self._q("src", q % db)[0]
                dv = self._q("dst", q % ddb)[0]
            except Exception:
                continue
            s = (int(sv[0] or 0), int(sv[1] or 0))
            d = (int(dv[0] or 0), int(dv[1] or 0))
            if s != d:
                bad.append(f"{tbl}.{col}: src null/empty={s[0]}|{s[1]}"
                           f" dst={d[0]}|{d[1]}")
        if bad:
            return [Result("deep", f"{db} nullempty", "diff",
                           f"{len(bad)} text columns differ in NULL vs"
                           " empty-string split (semantic flip): "
                           + "; ".join(bad[:5]), "",
                           "normalize with NULLIF(col,'') / COALESCE per"
                           " column intent; decide which side is canonical")]
        return [Result("deep", f"{db} nullempty", "ok",
                       "NULL vs empty-string consistent on text columns")]

    def _deep_float(self, db, ddb):
        """FLOAT and DOUBLE are approximations, so a checksum over them is
        the wrong instrument: identical values can hash differently and real
        drift can hash the same. Compare the aggregate with a tolerance
        instead, which is what the value actually supports."""
        q = ("select table_name, column_name from information_schema.columns"
             " where table_schema=%s and data_type in (%s, %s)"
             " and table_name not like 'migkit%%' order by 1,2")
        cols = self._q("src", q, (db,) + self.FLOATY)[:30]
        bad = []
        for tbl, col in cols:
            agg = (f"select count(`{col}`), coalesce(sum(`{col}`),0),"
                   f" coalesce(max(abs(`{col}`)),0) from `%s`.`{tbl}`")
            try:
                sn, ss, smax = self._q("src", agg % db)[0]
                dn, ds, dmax = self._q("dst", agg % ddb)[0]
            except Exception:
                continue
            if int(sn or 0) != int(dn or 0):
                bad.append(f"{tbl}.{col}: non-null count src={sn} dst={dn}")
                continue
            ss, ds = float(ss or 0), float(ds or 0)
            # relative tolerance against the magnitude actually present, so
            # the test is meaningful for both tiny and huge columns
            scale = max(abs(ss), abs(ds), float(smax or 0), float(dmax or 0), 1.0)
            if abs(ss - ds) > scale * 1e-9:
                bad.append(f"{tbl}.{col}: sum src={ss!r} dst={ds!r}")
        if bad:
            return [Result("deep", f"{db} float", "diff",
                           f"{len(bad)} float/double columns drift beyond"
                           " tolerance: " + "; ".join(bad[:5]), "",
                           "a mover that changed the column's precision will"
                           " do this; compare the column definitions first")]
        return [Result("deep", f"{db} float", "ok",
                       f"{len(cols)} float/double columns within tolerance"
                       if cols else "no float/double columns")]

    def _deep_unenforced_checks(self, db, ddb):
        """MySQL's version of PostgreSQL's NOT VALID.

        A CHECK constraint declared `NOT ENFORCED` is in the catalog and in
        every schema diff, and enforces nothing. Counting constraints finds
        both sides equal; only reading `ENFORCED` shows that the target is
        accepting rows the source would reject.
        """
        q = ("select tc.table_name, tc.constraint_name, cc.check_clause,"
             " tc.enforced from information_schema.table_constraints tc"
             " join information_schema.check_constraints cc"
             "   on cc.constraint_schema = tc.constraint_schema"
             "  and cc.constraint_name = tc.constraint_name"
             " where tc.constraint_schema=%s and tc.constraint_type='CHECK'"
             " and tc.table_name not like 'migkit%%'")
        try:
            src = {(r[0], r[1]): (r[2], r[3]) for r in self._q("src", q, (db,))}
            dst = {(r[0], r[1]): (r[2], r[3]) for r in self._q("dst", q, (ddb,))}
        except Exception as e:
            # MySQL 5.7 parses CHECK and discards it; there is no catalog to
            # read, and saying so is more use than a silent pass
            return [Result("deep", f"{db} checks", "skip",
                           f"CHECK constraint catalog unavailable:"
                           f" {str(e).splitlines()[0][:70]}")]
        bad = []
        for k, (clause, enforced) in sorted(src.items()):
            if k not in dst:
                bad.append(f"{k[0]}.{k[1]} missing on target")
                continue
            dclause, denforced = dst[k]
            if str(denforced).upper() == "NO" and str(enforced).upper() == "YES":
                bad.append(f"{k[0]}.{k[1]} NOT ENFORCED on target"
                           " (enforced on source)")
            elif re.sub(r"\s+", "", str(clause or "")) != \
                    re.sub(r"\s+", "", str(dclause or "")):
                bad.append(f"{k[0]}.{k[1]} clause differs")
        unenforced_both = [k for k, (_, e) in src.items()
                           if str(e).upper() == "NO" and k in dst]
        if bad:
            return [Result("deep", f"{db} checks", "diff",
                           f"{len(bad)} CHECK constraints differ: "
                           + "; ".join(bad[:5]), "",
                           "ALTER TABLE ... ALTER CHECK <name> ENFORCED after"
                           " confirming the existing rows satisfy it")]
        note = (f"; {len(unenforced_both)} NOT ENFORCED on both sides"
                if unenforced_both else "")
        return [Result("deep", f"{db} checks", "ok",
                       f"{len(src)} CHECK constraints match{note}")]

    # privileges live in the grant tables, not in the schema, so neither a
    # dump/restore nor a CDC stream carries them: the data arrives and the
    # permissions do not. The symptom is an app that connects fine and then
    # cannot read its own tables, which nothing else here would catch.
    def _check_grants(self, db):
        ddb = self._d("dst", db)
        src, serr = self._grants_for_db("src", db)
        dst, derr = self._grants_for_db("dst", ddb)
        # an account that cannot read the grant tables must say so - reporting
        # a match nobody was able to look at is worse than reporting nothing.
        # warn, not skip: skip counts as ok everywhere downstream, so an
        # unreadable grant table would leave the run green.
        if serr or derr:
            where = "source" if serr else "target"
            return [Result("deep", f"{db} grants", "warn",
                           f"could not read privileges on {where}:"
                           f" {serr or derr}", "",
                           "grant SELECT on mysql.* to the checking account,"
                           " or check privileges by hand")]

        # comparing users that exist on one side only just restates the users
        # check; what is worth knowing here is whether the users that DO exist
        # on both sides carry the same rights on this database
        on_target = getattr(self, "_grant_users", {}).get("dst", set())
        # a user present on the target is comparable even when it holds
        # nothing there yet - that is a total loss of its grants, and it used
        # to be reported as "the user does not exist on target", which was
        # both wrong and pointed at the wrong fix
        shared = sorted(set(src) & on_target)
        miss, extra = [], []
        for who in shared:
            for g in sorted(src[who] - dst.get(who, set())):
                miss.append(f"{who}: {g}")
            for g in sorted(dst.get(who, set()) - src[who]):
                extra.append(f"{who}: {g}")
        only_src = sorted(set(src) - on_target)
        total = sum(len(v) for v in src.values())

        if miss or extra:
            parts = []
            if miss:
                parts.append(f"{len(miss)} grants missing on target: "
                             + "; ".join(miss[:3]))
            if extra:
                parts.append(f"{len(extra)} grants on target that the source"
                             " does not give: " + "; ".join(extra[:3]))
            if only_src:
                parts.append(f"{len(only_src)} users grant on this db at source"
                             " but do not exist on target: "
                             + ", ".join(only_src[:3]))
            return [Result("deep", f"{db} grants", "diff", "; ".join(parts), "",
                           "create the users first - migkit users create"
                           " replays their GRANTs from the source")]
        if only_src:
            return [Result("deep", f"{db} grants", "diff",
                           f"{len(only_src)} users hold grants on this db at"
                           f" source but do not exist on target:"
                           f" {', '.join(only_src)}", "",
                           "migkit users create <hop> --apply")]
        return [Result("deep", f"{db} grants", "ok",
                       f"{total} grants match on {len(shared)} users"
                       if shared else "no non-system user grants this database")]

    def _grants_for_db(self, side, dbn):
        """{user@host -> set(privileges)} granted ON this database.

        Read through SHOW GRANTS because that is what `migkit users create`
        replays when it builds the users, so the check and the repair agree on
        what a grant is. information_schema.*_PRIVILEGES would be one query
        instead of N, but it is filtered to what the connected account happens
        to be able to see - the same trap the postgres side avoids by reading
        relacl rather than information_schema.
        """
        import os
        from ..users import MYSQL_SYS
        # same knob the postgres side reads, so one setting covers both engines
        ign = MYSQL_SYS | {r.strip() for r in os.environ.get(
            "GRANTS_IGNORE_ROLES", "").split(",") if r.strip()}
        try:
            users = [(u, h) for u, h in self._q(
                side, "select user, host from mysql.user")
                if u not in ign and not u.startswith("AWS_")]
        except Exception as e:
            return {}, f"{type(e).__name__}: {str(e)[:70]}"

        # `db`.* and `db`.`tbl`, with or without the backticks MySQL prints
        # depending on version, and never `dbase` when we asked for `db`
        pat = re.compile(r"\sON\s+`?" + re.escape(dbn) + r"`?\s*\.", re.I)
        out = {}
        # every account this side has, whether or not it holds anything on
        # this database. Without it, a user that exists on the target but was
        # granted nothing there is indistinguishable from one that is not
        # there at all - and it used to be reported as the second.
        seen = getattr(self, "_grant_users", None)
        if seen is None:
            seen = self._grant_users = {}
        seen = seen.setdefault(side, set())
        seen.clear()
        store = getattr(self, "_grant_raw", None)
        if store is None:
            store = self._grant_raw = {}
        raw = store.setdefault(side, {})
        for u, h in users:
            try:
                rows = self._q(side, "show grants for %s@%s", (u, h))
            except Exception:
                # a user that was dropped mid-scan, or one this account may not
                # inspect: skip the user, do not fail the whole check
                continue
            keep = [r[0] for r in rows
                    if pat.search(r[0]) and not r[0].startswith("GRANT PROXY")]
            # `keep` already holds the statement text, so the canonical form
            # is taken from the line - indexing it again would take its first
            # character, which for a moment it did: every user's privilege set
            # came out as {'G'}
            who = f"{u}@{h}"
            seen.add(who)
            privs = {self._canon_grant(line, dbn) for line in keep}
            if privs:
                out[who] = privs
                # the server's own text, kept so the repair replays exactly
                # what it printed instead of reassembling a GRANT from the
                # canonical form and getting the quoting subtly wrong
                for line in keep:
                    raw[(who, self._canon_grant(line, dbn))] = line
        return out, ""

    @staticmethod
    def _canon_grant(text, dbn):
        """Same grant, same string on both sides.

        Two servers print the identical privilege set differently: spacing,
        backtick style, and an IDENTIFIED BY tail on older versions. Without
        this every user reads as a diff on a pair of servers that agree.

        The database name is replaced by a marker because a hop may map the
        database to a different name on the target - otherwise every single
        grant would differ for the one reason we already know about.

        Deliberately NOT uppercased: on Linux MySQL table names are
        case-sensitive, so folding case here would let CatalogProduct and
        catalogproduct compare equal. SHOW GRANTS already prints privilege
        keywords in upper case, so there is nothing left to normalise.
        """
        t = re.sub(r"\s+", " ", text.strip()).replace("`", "")
        t = re.sub(r"\s+IDENTIFIED BY.*$", "", t, flags=re.I)
        t = re.sub(r"(\sON\s+)" + re.escape(dbn) + r"(\s*\.)",
                   r"\1<db>\2", t, flags=re.I)
        return t.rstrip(";")

    def _events(self, side, db):
        """{event: (time_zone, sql_mode, CREATE EVENT)} as the server shows
        each one."""
        name = self._d(side, db)
        out = {}
        for (ev,) in self._q(side, "select event_name from"
                                   " information_schema.events where"
                                   " event_schema = %s", (name,)):
            row = self._q(side, f"show create event {self._my_ident(name)}"
                                f".{self._my_ident(ev)}")[0]
            # Event, sql_mode, time_zone, Create Event, ...
            out[str(ev)] = (str(row[2]), str(row[1]), str(row[3]))
        return out

    @staticmethod
    def _event_body(create):
        """An event's definition without what the move changes on purpose:
        who defined it, and whether it is switched on."""
        text = re.sub(r"DEFINER=\S+\s+", "", create)
        return re.sub(r"\s(ENABLE|DISABLE( ON (SLAVE|REPLICA))?)\s+(?=(COMMENT|DO)\b)",
                      " ", text)

    @staticmethod
    def _disabled(create):
        """The same CREATE EVENT, switched off: an event running on the
        target while the move still carries rows rewrites them under it."""
        text = re.sub(r"\s(ENABLE|DISABLE( ON (SLAVE|REPLICA))?)\s+(?=(COMMENT|DO)\b)",
                      " DISABLE ", create, count=1)
        return text if " DISABLE " in text else text.replace(" DO ",
                                                              " DISABLE DO ",
                                                              1)

    def _event_repair(self, db):
        """Create the events the target lacks, and redefine the ones that
        differ, switched off until cutover.

        The schema check names a missing event, and the tool that writes
        the rest of the fix DDL does not model events: measured, it called
        a pair clean where the target had no event at all, so the event was
        found and never made. Each is created in the time zone and sql_mode
        the source defined it in, since both change what it does."""
        try:
            src, dst = self._events("src", db), self._events("dst", db)
        except Exception:
            return None
        stmts, undo, made, redone = [], [], [], []

        def create(ev):
            tz, mode, ddl = ev
            return [f"SET SESSION time_zone = {self._quote_literal(tz)}",
                    f"SET SESSION sql_mode = {self._quote_literal(mode)}",
                    ddl]
        for name in sorted(src):
            q = self._my_ident(name)
            if name not in dst:
                stmts += create((src[name][0], src[name][1],
                                 self._disabled(src[name][2])))
                undo.append(f"DROP EVENT IF EXISTS {q}")
                made.append(name)
            elif self._event_body(src[name][2]) != \
                    self._event_body(dst[name][2]):
                stmts += [f"DROP EVENT {q}"] + create(
                    (src[name][0], src[name][1],
                     self._disabled(src[name][2])))
                undo += [f"DROP EVENT IF EXISTS {q}"] + create(dst[name])
                redone.append(name)
        if not stmts:
            return None
        said = []
        if made:
            said.append(f"{len(made)} events the target lacks:"
                        f" {', '.join(made[:5])}")
        if redone:
            said.append(f"{len(redone)} events defined differently:"
                        f" {', '.join(redone[:5])}")
        return RepairAction(db, "events", stmts, undo,
                            "; ".join(said) + " - created switched off;"
                            " switch them on at cutover (ALTER EVENT ..."
                            " ENABLE)")

    def _constraint_repair(self, db):
        """Turn NOT ENFORCED check constraints back on.

        MySQL's counterpart to PostgreSQL's NOT VALID, and the inverse is
        exact - `ALTER CHECK ... NOT ENFORCED` puts it back as it was, which
        is more than the PostgreSQL side can say.

        Measured: enforcing a constraint that existing rows violate fails with
        `ERROR 3819 Check constraint '...' is violated` and changes nothing,
        so no pre-scan is needed here either.
        """
        ddb = self._d("dst", db)
        q = ("select tc.table_name, tc.constraint_name, tc.enforced"
             " from information_schema.table_constraints tc"
             " where tc.constraint_schema=%s and tc.constraint_type='CHECK'"
             " and tc.table_name not like 'migkit%%'")
        try:
            src = {(r[0], r[1]): str(r[2]).upper()
                   for r in self._q("src", q, (db,))}
            dst = {(r[0], r[1]): str(r[2]).upper()
                   for r in self._q("dst", q, (ddb,))}
        except Exception:
            # MySQL 5.7 parses CHECK and discards it - no catalog to read
            return None
        turn_on = [k for k, v in sorted(dst.items())
                   if v == "NO" and src.get(k) == "YES"]
        if not turn_on:
            return None
        stmts = [f"ALTER TABLE `{tbl}` ALTER CHECK `{name}` ENFORCED;"
                 for tbl, name in turn_on]
        undo = [f"ALTER TABLE `{tbl}` ALTER CHECK `{name}` NOT ENFORCED;"
                for tbl, name in turn_on]
        return RepairAction(db, "constraints", stmts, undo,
                            f"{len(turn_on)} check constraints the target"
                            " was not enforcing")

    def _grant_repair(self, db):
        """Replay the grants the mover did not carry, with a REVOKE to undo.

        The statement replayed is the one the source server printed, with only
        the database name swapped for the target's. Reassembling a GRANT from
        the canonical comparison form would mean re-deriving the quoting, and
        the quoting is exactly what differs between servers - which is why the
        canonical form exists in the first place.

        Users that exist on one side only are left alone: that is the users
        check's finding, and `migkit users create` replays their whole grant
        set. Granting to an account that does not exist would just fail.
        """
        ddb = self._d("dst", db)
        src, serr = self._grants_for_db("src", db)
        dst, derr = self._grants_for_db("dst", ddb)
        if serr or derr:
            return None
        raw = getattr(self, "_grant_raw", {}).get("src", {})
        # an account that exists on the target holding nothing on this
        # database is exactly the case worth repairing, and `dst` only lists
        # accounts that already hold something - so the target's account list
        # decides, not its grant list
        on_target = getattr(self, "_grant_users", {}).get("dst", set())
        stmts, undo, n = [], [], 0
        for who in sorted(set(src) & on_target):
            for g in sorted(src[who] - dst.get(who, set())):
                line = raw.get((who, g))
                if not line:
                    continue
                fwd = self._retarget_grant(line, db, ddb)
                stmts.append(fwd)
                undo += self._revoke_for(fwd)
                n += 1
        if not stmts:
            return None
        return RepairAction(db, "grants", stmts, undo,
                            f"{n} grants the mover did not carry")

    @staticmethod
    def _retarget_grant(line, src_db, dst_db):
        """The source's own GRANT, pointed at the target's database name."""
        out = re.sub(r"(\sON\s+)`?" + re.escape(src_db) + r"`?(\s*\.)",
                     r"\1`" + dst_db + r"`\2", line, flags=re.I)
        return out.rstrip(";") + ";"

    @staticmethod
    def _revoke_for(grant):
        """The statements that undo one GRANT.

        `WITH GRANT OPTION` is revoked separately because MySQL will not take
        it as part of the privilege list, and leaving it granted would undo
        less than the forward statement did.
        """
        body = grant.rstrip(";")
        extra = []
        if re.search(r"\s+WITH\s+GRANT\s+OPTION\s*$", body, re.I):
            body = re.sub(r"\s+WITH\s+GRANT\s+OPTION\s*$", "", body,
                          flags=re.I)
            extra.append(re.sub(r"^GRANT\s+.*?\s+ON\s", "GRANT OPTION ON ",
                                body, flags=re.I)
                         .replace(" TO ", " FROM ", 1)
                         .replace("GRANT OPTION ON ", "REVOKE GRANT OPTION ON ",
                                  1) + ";")
        rev = re.sub(r"^GRANT\s", "REVOKE ", body, flags=re.I)
        rev = rev.replace(" TO ", " FROM ", 1)
        return [rev + ";"] + extra

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            q = ("select table_name, auto_increment from information_schema.tables"
                 " where table_schema=%s and auto_increment is not null")
            ddb = self._d("dst", db)
            src = dict(self._q("src", q, (db,), fresh=True))
            dst = dict(self._q("dst", q, (ddb,), fresh=True))
            stmts, undo, refuse = [], [], []
            for tbl, sv in sorted(src.items()):
                dv = dst.get(tbl)
                if dv == sv:
                    continue
                if dv is None:
                    refuse.append(f"{tbl}: not on target")
                    continue
                # only ever raise. Lowering a counter is how the next insert
                # collides with a row that already exists, and a target that is
                # ahead is normal while changes are still being applied.
                if dv > sv:
                    refuse.append(f"{tbl}: target {dv} > source {sv}")
                    continue
                stmts.append(f"alter table `{ddb}`.`{tbl}` auto_increment = {sv};"
                             f"  -- dst now {dv}")
                undo.append(f"alter table `{ddb}`.`{tbl}` auto_increment = {dv};")
            same = sum(1 for tbl, v in src.items() if dst.get(tbl) == v)
            note = f"{len(stmts)} counters raised, {same} already equal"
            if refuse:
                note += (f"; left alone {len(refuse)} (target not behind): "
                         + "; ".join(refuse[:4]))
            if stmts or refuse:
                actions.append(RepairAction(db, "sequences", stmts, undo, note))
        if kind in ("schema", "all"):
            # GRANT is DDL: it goes with the schema repair rather than adding
            # a choice to --kind, so a plain `migkit sync --apply` restores it
            g = self._grant_repair(db)
            if g:
                actions.append(g)
            c = self._constraint_repair(db)
            if c:
                actions.append(c)
            ev = self._event_repair(db)
            if ev:
                actions.append(ev)
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
                        # counted as keys, not as lines: a key holding a
                        # newline is one row to repair, and counting the
                        # lines reported five where there were four
                        counts.append(f"{k}={len(self._drill_keys(f))}")
                stmts = [f"resync pks for {t} ({', '.join(counts)})"]
                if which("pt-table-sync"):
                    s, tg = self.hop.source, self.hop.target
                    from ..movers import client_defaults
                    with client_defaults(s, tg) as (fs, ft):
                        p = run(["pt-table-sync", "--print",
                                 f"h={s.host},P={s.port},u={s.user},"
                                 f"F={fs},D={db},t={t}",
                                 f"h={tg.host},P={tg.port},u={tg.user},"
                                 f"F={ft},D={self._d('dst', db)}"],
                                check=False, timeout=300)
                    sql = [l for l in p.stdout.splitlines()
                           if l and not l.startswith("#")]
                    if sql:
                        stmts += [f"  {l}" for l in sql[:10]]
                        if len(sql) > 10:
                            stmts.append(f"  ... {len(sql) - 10} more")
                actions.append(RepairAction(
                    db, "rows", stmts,
                    [], f"{t}: delete extra/changed on target (saved to undo"
                        " first), reinsert from source"))
            actions += self._mapped_repairs(db, kind)
            # after the resync, never before it: a recopy takes its rows from
            # the source, which is where the broken text lives
            text = self._mojibake_repair(db)
            if text:
                actions.append(text)
        if kind in ("schema", "all"):
            act = self._schema_repair_action(db)
            if act:
                actions.append(act)
        return actions

    def _target_client(self, physical, sql):
        """Run a script on the target's database `physical` through the
        client, which handles routine and trigger bodies (DELIMITER). The
        password in the environment, not on the command line, where every
        process listing on the machine could read it."""
        tg = self.hop.target
        return run(["mysql", "-h", tg.host, "-P", str(tg.port),
                    "-u", tg.user, physical], input=sql,
                   env={"MYSQL_PWD": tg.password})

    def rehearse(self, db, action):
        """Run a schema fix and then its undo on a scratch copy of the
        target's schema, before either goes near the target itself - as
        PostgreSQL's does, on a database made for it on the target's own
        server and dropped afterwards.

        MySQL runs DDL outside any transaction, so a fix that fails part of
        the way through on the target is left half applied there. Found on
        the copy first, it is not applied at all. Returns (usable,
        sentence).
        """
        if action.kind != "schema":
            return True, ""
        import difflib
        import re as _re
        scratch = _re.sub(r"[^a-z0-9_]", "_",
                          f"migkit_rehearsal_{self.hop.name}".lower())[:64]
        q = self._quote_ident(scratch)
        try:
            self._q("dst", f"drop database if exists {q}")
            cs = self._q("dst", "select default_character_set_name,"
                                " default_collation_name from"
                                " information_schema.schemata"
                                " where schema_name = %s",
                         (self._d("dst", db),))
            self._q("dst", f"create database {q}"
                           + (f" character set {cs[0][0]} collate {cs[0][1]}"
                              if cs else ""))
        except Exception as e:
            return True, ("the undo was not rehearsed: a scratch database"
                          " could not be made on the target ("
                          + str(e).strip().splitlines()[-1][:100] + ")")
        try:
            try:
                ep = self.hop.target
                copy = run(["mysqldump", "-h", ep.host, "-P", str(ep.port),
                            "-u", ep.user, "--no-data", "--routines",
                            "--triggers", "--events", "--skip-comments",
                            "--column-statistics=0", self._d("dst", db)],
                           env={"MYSQL_PWD": ep.password})
                self._target_client(scratch, "set foreign_key_checks = 0;\n"
                                    + copy.stdout)
            except Exception as e:
                return True, ("the undo was not rehearsed: the target's"
                              " schema could not be copied ("
                              + str(e).strip().splitlines()[-1][:100] + ")")
            before = self._dump_schema("dst", db, physical=scratch)
            try:
                self._target_client(scratch, "\n".join(action.statements)
                                    + "\n")
            except Exception as e:
                return False, ("the fix fails on a copy of the target's"
                               " schema, so it was not applied: "
                               + str(e).strip().splitlines()[-1][:160])
            if not action.undo:
                return True, ("the fix applies on a copy of the target's"
                              " schema; it has no undo")
            try:
                self._target_client(scratch, "\n".join(action.undo) + "\n")
            except Exception as e:
                return True, ("the undo FAILS on a copy of the target's"
                              " schema - keep a backup before applying: "
                              + str(e).strip().splitlines()[-1][:160])
            after = self._dump_schema("dst", db, physical=scratch)
            changed = [l for l in difflib.unified_diff(
                before.splitlines(), after.splitlines(), lineterm="")
                if l[:1] in "+-" and not l.startswith(("+++", "---"))]
            if changed:
                (self.hop.report_dir(db) / "rehearsal.diff").write_text(
                    "\n".join(changed) + "\n")
                return True, (f"the undo does NOT return the schema exactly"
                              f" ({len(changed)} lines differ, in"
                              " rehearsal.diff) - keep a backup before"
                              " applying")
            return True, ("rehearsed on a copy of the target's schema: the"
                          " fix applies, and the undo returns the schema"
                          " exactly")
        finally:
            try:
                self._q("dst", f"drop database if exists {q}")
            except Exception:
                pass

    def _schema_repair_action(self, db):
        """atlas-generated DDL to align the target's objects (columns,
        indexes, PK/FK, views, routines, triggers) to the source, reverse
        DDL as undo. Structure only; data untouched."""
        if not which("atlas"):
            return None
        fwd, undo = self.migration_pair(db)
        if not fwd:
            return None
        return RepairAction(db, "schema", fwd.splitlines(),
                            (undo or "").splitlines(),
                            "DDL that aligns the target's objects with the"
                            " source (review before --apply; reverse DDL"
                            " saved to undo)")

    def apply(self, db, action):
        if action.by == "pair":
            return self.columns_pair().apply(db, action)
        if action.kind == "resnapshot":
            self._apply_resnapshot(action)
            return
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
        if action.kind == "text":
            # withheld unless MIGKIT_REPAIR_TEXT says otherwise, in which
            # case the note is the whole action and applying it does nothing.
            # One transaction: all of the repair or none of it
            if action.statements:
                conn = self._conn("dst")
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"use {self._my_ident(self._d('dst', db))}")
                        for s in action.statements:
                            cur.execute(s)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
            return
        if action.kind == "events":
            # one statement at a time over one session: an event's body is
            # full of semicolons, and the SETs before each CREATE belong to
            # the session that runs it
            conn = self._conn("dst")
            try:
                with conn.cursor() as cur:
                    cur.execute(f"use {self._my_ident(self._d('dst', db))}")
                    for s in action.statements:
                        cur.execute(s)
            finally:
                conn.close()
            return
        if action.kind in ("schema", "grants", "constraints"):
            # mysql CLI handles routine/trigger bodies (DELIMITER) correctly
            tg = self.hop.target
            ddl = "\n".join(action.statements) + "\n"
            if action.kind == "grants":
                ddl += "flush privileges;\n"
            self._target_client(self._d("dst", db), ddl)
            return
        if action.kind != "rows":
            # the same trap the postgres side had: an unhandled kind used to
            # fall through to the row path and be read as a table name
            raise RuntimeError(f"no way to apply a {action.kind!r} repair")
        t = action.statements[0].split()[3]
        # the repaired rows are the source's, and the target's triggers
        # would rewrite them as they land, as they would a load's
        with self.load_window(db, None, {t}):
            self._apply_rows_native(db, t,
                                    getattr(self, "_undo_dir", None))

    @staticmethod
    def _drill_keys(path):
        """The keys one drilldown file holds, taken apart the way the check
        put them together.

        These are `rowtext`'s encoding - `2:11`, and `1:3|2:99` for a
        composite key. The repair read them with `split("\\t")`, which is the
        very thing `rowtext.parse` was written to replace: there is no tab
        in them, so every line came back as the single string `2:11`, the
        `where` compared the key against that, and nothing matched.
        Measured on MySQL 8 with one row missing, one extra and one changed,
        `apply` returned without an error, the target was untouched, and the
        next check reported the same three differences - a repair that
        reported success and did nothing at all.

        Read by length rather than by line, so a key holding a newline is
        one key rather than two unreadable halves.
        """
        from .. import rowtext
        if not path.exists():
            return []
        try:
            return [["" if v is None else v for v in row]
                    for row in rowtext.parse_all(path.read_text())]
        except ValueError as e:
            raise SystemExit(
                f"{path} is not in the form this migkit reads ({e}). Re-run"
                " `migkit check --check data` to write it again before"
                " syncing")

    def _apply_rows_native(self, db, t, undo_dir=None):
        """This engine's own row repair, named apart from the base's
        `_apply_rows`: that one carries rows between two engines over
        the neutral contract and takes a different set of arguments,
        and two methods with one name on the same object is a trap
        waiting for whoever calls the wrong one.
        """
        d = self.hop.report_dir(db)
        ddb = self._d("dst", db)
        pks = self._pk_cols(db, t)
        cols = self._cols(db, t)

        def read(kind):
            """The keys the check wrote, taken apart the way it put them
            together.

            These lines are `rowtext`'s encoding - `2:11`, and `1:3|2:99`
            for a composite key - and this read them with `split("\\t")`,
            which is the very thing `rowtext.parse` was written to replace.
            There is no tab in them, so every line came back as the single
            string `2:11`, the `where` compared the key against that, and
            nothing matched. Measured on MySQL 8 with one row missing, one
            extra and one changed: `apply` returned without an error, the
            target was untouched, and the next check reported the same
            three differences - a repair that reported success and did
            nothing at all.
            """
            return self._drill_keys(d / f"data-{t}.{kind}")

        missing, extra, changed = read("missing"), read("extra"), read("changed")
        # keep-target preserves target-changed rows: fix only missing/extra
        if self.hop.options.get("on_conflict") == "keep-target":
            changed = []
        col = self.newer_wins_column()
        if col and changed:
            have = {side: {str(r[0]).lower() for r in self._q(
                        side, "select column_name from information_schema"
                              ".columns where table_schema = %s"
                              " and table_name = %s",
                        (db if side == "src" else ddb, t))}
                    for side in ("src", "dst")}
            if not all(col.lower() in h for h in have.values()):
                raise SystemExit(
                    f"{t}: newer_wins names {col}, and it is not on both"
                    " sides, so which row is newer cannot be told. Nothing"
                    " was repaired on this table.")
            key_cond = " and ".join(f"cast(`{c}` as char) = %s" for c in pks)

            def at(side, keys):
                name = db if side == "src" else ddb
                out = {}
                for k in keys:
                    got = self._q(side, f"select `{col}` from `{name}`.`{t}`"
                                        f" where {key_cond}", k)
                    if got:
                        out[self._key_id(k)] = got[0][0]
                return out
            changed, kept = self._keep_the_newer(t, changed, at)
            if kept:
                log_kept = d / f"data-{t}.kept-newer"
                log_kept.write_text("".join(f"{k}\n" for k in kept))
        to_delete = extra + changed
        to_copy = missing + changed
        cond = " and ".join(f"cast(`{c}` as char) = %s" for c in pks)
        collist = ", ".join(f"`{c}`" for c in cols)
        ph = ", ".join(["%s"] * len(cols))

        undo = Path(undo_dir) if undo_dir else d / "undo"
        undo.mkdir(parents=True, exist_ok=True)
        # record every touched pk's pre-repair target row so restore is exact
        sconn, dconn = self._conn("src"), self._conn("dst")
        touched = missing + extra + changed
        entries = []
        try:
            with dconn.cursor() as cur:
                cur.execute("set foreign_key_checks = 0")
                for pk in touched:
                    cur.execute(f"select {collist} from `{ddb}`.`{t}`"
                                f" where {cond}", pk)
                    got = cur.fetchall()
                    entries.append({"pk": pk, "cols": cols,
                                    "old": [list(r) for r in got] or None})
                (undo / f"rows-{t}.jsonl").write_text(
                    "".join(json.dumps(e, default=str) + "\n"
                            for e in entries))
                # pt-table-sync when available (bounded to the verified pks);
                # builtin delete+copy is the fallback
                pt_sql = self._pt_sync_sql(db, t, pks, touched)
                if pt_sql:
                    for stmt in pt_sql:
                        cur.execute(stmt)
                else:
                    for pk in to_delete:
                        cur.execute(f"delete from `{ddb}`.`{t}`"
                                    f" where {cond}", pk)
                    with sconn.cursor() as scur:
                        for pk in to_copy:
                            scur.execute(f"select {collist} from `{db}`.`{t}`"
                                         f" where {cond}", pk)
                            rows = scur.fetchall()
                            if rows:
                                cur.executemany(
                                    f"insert into `{ddb}`.`{t}` ({collist})"
                                    f" values ({ph})", rows)
            dconn.commit()
        finally:
            sconn.close()
            dconn.close()

    #: Characters that make the printed-SQL path unusable, measured on
    #: MySQL 8 with a text primary key:
    #:
    #:   a newline   `pt-table-sync --print` writes one statement that this
    #:               reads back with `splitlines()`, so half a statement was
    #:               executed: `SQL syntax ... near ''line'`
    #:   a backslash MySQL's own escape character inside a string literal,
    #:               so a `--where` built by doubling quotes alone turns
    #:               `back\slash` into `backslash` and matches no row
    #:
    #: The builtin path below sends the same keys as query parameters, where
    #: neither is a problem, so this hands over instead of guessing.
    PT_UNSAFE = ("\n", "\r", "\\")

    def _pt_sync_sql(self, db, t, pks, touched):
        if not which("pt-table-sync") \
                or not self.hop.options.get("pt_apply", True):
            return None
        if len(pks) != 1 or not touched or len(touched) > 1000:
            return None
        if any(c in str(k[0]) for k in touched for c in self.PT_UNSAFE):
            return None
        vals = ", ".join("'" + str(k[0]).replace("'", "''") + "'"
                         for k in touched)
        s, tg = self.hop.source, self.hop.target
        from ..movers import client_defaults
        with client_defaults(s, tg) as (fs, ft):
            p = run(["pt-table-sync", "--print",
                     "--where", f"`{pks[0]}` in ({vals})",
                     f"h={s.host},P={s.port},u={s.user},F={fs},"
                     f"D={db},t={t}",
                     f"h={tg.host},P={tg.port},u={tg.user},F={ft},"
                     f"D={self._d('dst', db)}"],
                    check=False, timeout=600)
        if p.returncode not in (0, 2):
            return None
        sql = [l for l in p.stdout.splitlines()
               if l and not l.startswith("#")]
        return sql or None

    def restore_rows(self, db, undo_dir):
        """Replay a complete row undo: for each touched pk, delete whatever is
        there now, then re-insert the saved pre-repair row (or leave absent)."""
        undo_dir = Path(undo_dir)
        files = sorted(undo_dir.glob("rows-*.jsonl"))
        if not files:
            return 0
        n = 0
        ddb = self._d("dst", db)
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
                        cur.execute(f"delete from `{ddb}`.`{t}` where {cond}",
                                    e["pk"])
                        if e["old"]:
                            collist = ", ".join(f"`{c}`" for c in cols)
                            ph = ", ".join(["%s"] * len(cols))
                            cur.executemany(
                                f"insert into `{ddb}`.`{t}` ({collist})"
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

        items += self._brand_rows()
        sv = self._q("src", "select version()")[0][0]
        dv = self._q("dst", "select version()")[0][0]
        items.append(self._version_row(sv, dv, parts=2))
        items += self._pooler_items()
        # each with the value to set, not only the verdict
        for name, want, lvl, fix in (
                ("log_bin", "ON", "fail",
                 "start the server with log-bin (a restart)"),
                ("binlog_format", "ROW", "fail",
                 "set global binlog_format = 'ROW'"),
                ("binlog_row_image", "FULL", "warn",
                 "set global binlog_row_image = 'FULL'"),
                # the change tail refuses without it - measured, the reader
                # otherwise cannot tell a varbinary from a varchar
                ("binlog_row_metadata", "FULL", "warn",
                 "set global binlog_row_metadata = 'FULL'")):
            v = var("src", name)
            add("pass" if str(v).upper() == want else lvl, "instance",
                f"{name}={want} on source (CDC requirement)",
                v if str(v).upper() == want else f"{v} - {fix}")
        ret = var("src", "binlog_expire_logs_seconds")
        try:
            ok = int(ret) >= 86400
        except ValueError:
            ok = False
        add("pass" if ok else "warn", "instance",
            "binlog retention at least 24h",
            ret if ok else
            f"{ret} - set global binlog_expire_logs_seconds = 604800, or"
            " on RDS call mysql.rds_set_configuration('binlog retention"
            " hours', 168)")
        # MySQL's compressed transactions the tail opens (binlog_payload);
        # MariaDB's compressed row events it cannot
        zc = str(var("src", "binlog_transaction_compression")).upper()
        if zc not in ("?", ""):
            add("pass", "instance", "compressed transactions in the binlog"
                " readable by the change tail",
                zc + (" - the tail opens them" if zc == "ON" else ""))
        zc = str(var("src", "log_bin_compress")).upper()
        if zc not in ("?", ""):
            add("pass" if zc == "OFF" else "fail", "instance",
                "log_bin_compress=OFF on source (the change tail cannot"
                " read MariaDB's compressed rows)",
                zc if zc == "OFF" else
                f"{zc} - set global log_bin_compress = OFF")
        sid = var("src", "server_id")
        add("pass" if str(sid) not in ("0", "") else "fail", "instance",
            "server_id set on source (a binlog reader needs it)",
            sid if str(sid) not in ("0", "") else
            f"{sid} - set global server_id = 1")
        gm = str(var("src", "gtid_mode")).upper()
        ge = str(var("src", "enforce_gtid_consistency")).upper()
        # "?" is a server with no such variable (MariaDB has no
        # gtid_mode), which is not a mismatch
        if gm not in ("", "?", "OFF") and ge != "ON":
            add("warn", "instance", "gtid_mode and enforce_gtid_consistency",
                f"gtid_mode {gm} with enforce_gtid_consistency {ge} -"
                " set global enforce_gtid_consistency = ON before relying"
                " on GTID positions")

        # a long-running open transaction pins InnoDB purge and stalls CDC
        # (the target lags for as long as it stays open); flag by age
        lrt = self._q("src",
                      "select count(*), coalesce(max(timestampdiff(second,"
                      " trx_started, now())),0)"
                      " from information_schema.innodb_trx")
        n, age = (lrt[0][0], int(lrt[0][1])) if lrt else (0, 0)
        add("pass" if age < 900 else "warn" if age < 3600 else "fail",
            "instance", "long-running transactions blocking CDC/purge",
            f"{n} open, oldest {age}s" if n else "none")
        backlog = self.purge_backlog("src")
        if backlog is not None:
            add("pass" if backlog < 1_000_000 else "warn", "instance",
                "undo the source has not purged yet",
                f"{backlog:,} transactions in the history list - it grows"
                " while an old snapshot stays open, and every read of a"
                " changed row walks it")
        for db in self.databases():
            cs = self._q("src", "select default_character_set_name,"
                         " default_collation_name from"
                         " information_schema.schemata"
                         " where schema_name = %s", (db,))
            cd = self._q("dst", "select default_character_set_name,"
                         " default_collation_name from"
                         " information_schema.schemata"
                         " where schema_name = %s", (self._d("dst", db),))
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
        inv = self._handwork()
        items += inv.rows() + inv.summary()
        items += self._mover_leftovers()
        items += self._bulk_path_rows()
        items += self._client_tool_versions(("mysqldump", "mysql"), dv)
        items += self._packet_items()
        items += self._collations_unknown()
        items += self._fork_objects()
        items += self._scope_items()
        return items

    #: what only MariaDB keeps, by how its catalogue says so: the table type
    #: of a sequence or a system-versioned table, and the column types MySQL
    #: has no name for
    MARIADB_ONLY = (
        ("sequences", "select table_schema, table_name from"
                      " information_schema.tables where table_type ="
                      " 'SEQUENCE'"),
        ("system-versioned tables", "select table_schema, table_name from"
                                    " information_schema.tables where"
                                    " table_type = 'SYSTEM VERSIONED'"),
        ("columns of a type MySQL does not have",
         "select table_schema, concat(table_name, '.', column_name,"
         " ' (', data_type, ')') from information_schema.columns where"
         " data_type in ('uuid', 'inet4', 'inet6')"),
    )

    #: date and time types that can hold MySQL's zero date
    ZERO_DATE_TYPES = ("date", "datetime", "timestamp")

    def zero_dates(self, side, db):
        """[(table, column, rows)] holding a date with a zero year, month or
        day - `0000-00-00`, `2020-00-15` - which a legacy `sql_mode` lets
        in and no other engine has. Read on this server, whose own
        `month()` and `dayofmonth()` answer 0 for them."""
        tables = set(self._tables(side, db))
        cols = [(str(t), str(c)) for t, c in self._q(
            side, "select table_name, column_name from"
                  " information_schema.columns where table_schema = %s"
                  " and data_type in ('date', 'datetime', 'timestamp')",
            (self._d(side, db),)) if str(t) in tables]
        out = []
        q = self._quote_ident
        for t, c in cols:
            n = self._q(side, f"select count(*) from"
                              f" {q(self._d(side, db))}.{q(t)} where"
                              f" year({q(c)}) = 0 or month({q(c)}) = 0"
                              f" or dayofmonth({q(c)}) = 0")[0][0]
            if n:
                out.append((t, c, int(n)))
        return out

    def _collations_unknown(self):
        """Collations the source's tables, columns and databases use that the
        target does not have.

        Measured, MariaDB 11 into MySQL 8.4: MariaDB's default is
        `utf8mb4_uca1400_ai_ci`, and the move stopped on `ERROR 1273:
        Unknown collation`. MySQL 8's `utf8mb4_0900_ai_ci` into 5.7 is the
        same shape. Which collation should stand in is a decision about how
        the application's text sorts and compares, so it is named rather
        than chosen."""
        item = "collations the target does not have"
        dbs = sorted(set(self.databases()))
        if not dbs:
            return []
        marks = ", ".join(["%s"] * len(dbs))
        try:
            used = {str(r[0]) for r in self._q(
                "src", "select collation_name from information_schema"
                       f".columns where table_schema in ({marks})"
                       " and collation_name is not null"
                       " union select table_collation from"
                       " information_schema.tables where table_schema in"
                       f" ({marks}) and table_collation is not null"
                       " union select default_collation_name from"
                       " information_schema.schemata where schema_name in"
                       f" ({marks})", tuple(dbs) * 3)}
            have = {str(r[0]) for r in self._q(
                "dst", "select collation_name from"
                       " information_schema.collations")}
        except Exception as e:
            return [{"level": "warn", "scope": "instance", "item": item,
                     "detail": "could not be read, so unknown rather than"
                               f" none: {str(e)[:80]}"}]
        missing = sorted(used - have)
        if missing:
            return [{"level": "fail", "scope": "instance", "item": item,
                     "detail": f"{len(missing)} used in scope and unknown to"
                               " the target, where creating a table with one"
                               " stops the move: " + ", ".join(missing[:6])}]
        return [{"level": "pass", "scope": "instance", "item": item,
                 "detail": f"all {len(used)} collations in scope exist on"
                           " the target"}]

    def _fork_objects(self):
        """What a MariaDB source holds that a MySQL target has no home for,
        named before the move rather than found after it.

        Measured on MariaDB 11 into MySQL 8.4: the move said `complete` and
        the check read `counts OK 1 tables` over a database that held a
        system-versioned table and a sequence as well - both were left out
        of every list of tables, because neither is a `BASE TABLE`. A
        system-versioned table is now carried, as a plain table with the
        rows it holds now; its history, which the application may read
        with `FOR SYSTEM_TIME`, has no home here. A sequence has none
        either, and a `uuid` or `inet6` column no type of that name."""
        src, dst = self._brands()
        if src.name != "mariadb" or dst.name == "mariadb":
            return []
        dbs = set(self.databases())
        out = []
        for what, sql in self.MARIADB_ONLY:
            try:
                got = [f"{r[0]}.{r[1]}" for r in self._q("src", sql)
                       if r[0] in dbs]
            except Exception as e:
                out.append({"level": "warn", "scope": "brand",
                            "item": f"MariaDB {what}",
                            "detail": "could not be read, so unknown rather"
                                      f" than none: {str(e)[:80]}"})
                continue
            if got:
                out.append({"level": "fail", "scope": "brand",
                            "item": f"MariaDB {what}",
                            "detail": f"{len(got)} with no home on"
                                      f" {dst.label()}: "
                                      + ", ".join(got[:6])
                                      + (" ..." if len(got) > 6 else "")})
        if not out:
            out.append({"level": "pass", "scope": "brand",
                        "item": "MariaDB-only objects",
                        "detail": "no sequences, system-versioned tables or"
                                  " MariaDB-only column types in scope"})
        return out

    def _mover_leftovers(self):
        """What a mover added to the source and did not take away.

        MySQL has no replication slot to pin WAL, so nothing here is urgent in
        the way the PostgreSQL side can be - but a leftover database or table
        is still evidence of which mover touched this server, which matters
        when two of them ran and only one is admitted to.
        """
        from .. import leftovers as _lo
        found, items = [], []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "source leftovers",
                          "item": item, "detail": str(detail)})
        try:
            for (name,) in self._q("src", "show databases"):
                found.append(("database", name))
            for (u, h) in self._q("src", "select user, host from mysql.user"):
                found.append(("user", f"{u}@{h}"))
        except Exception as e:
            add("warn", "cannot list databases or users on the source",
                f"{str(e)[:90]} - unknown, not clean")
            return items
        for db in self.databases():
            try:
                for (name,) in self._q(
                        "src", "select table_name from information_schema"
                               ".tables where table_schema = %s", (db,)):
                    found.append(("table", name))
            except Exception as e:
                add("warn", f"cannot list tables in {db}",
                    f"{str(e)[:90]} - unknown, not clean")
        by = _lo.group(found)
        if not by:
            add("pass", "no mover artifacts left in the source",
                f"{len(found)} objects examined")
            return items
        add("warn", f"{_lo.total(by)} mover artifacts left in the source",
            _lo.describe(by) + ". Drop them once every leg that used them is"
            " finished - they are on the source, so nothing on the target"
            " tells you they are there")
        return items

    def _handwork(self):
        """Inventory the work the move will not do, per `migkit.handwork`.

        Only things migkit genuinely leaves behind belong here. Routines,
        triggers and views are absent on purpose: `_dump_schema` dumps them,
        the textual diff compares them, and atlas generates the DDL to create
        a missing one - so they are carried end to end.

        Events are the boundary case, and the boundary is narrower than
        "not dumped". Measured against MySQL 8 with a source holding one event
        and a target holding none: `_dump_schema` does include it (it passes
        `--events`), and the object check names it - `event 1/0 missing: ev`.
        What did not happen was the repair. atlas, which writes the fix
        DDL, reported `atlas diff clean` for that same pair: it does not
        model MySQL events at all. migkit's own schema repair makes them now
        (`_event_repair`), switched off, so what is left by hand is
        switching them on at cutover.
        """
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                # Row-based replication finds the row to update through a
                # primary key or a unique NOT NULL index; with neither it
                # scans the table per row, and migkit has no key to name a
                # differing row by either.
                rows = self._q("src",
                    "select t.table_name from information_schema.tables t"
                    " where t.table_schema = %s"
                    " and t.table_type in ('BASE TABLE', 'SYSTEM VERSIONED')"
                    " and not exists (select 1 from information_schema"
                    ".statistics s where s.table_schema = t.table_schema"
                    " and s.table_name = t.table_name and s.non_unique = 0"
                    " and s.nullable <> 'YES')", (db,))
                inv.add("no-row-key", db, "tables", [r[0] for r in rows])

                eng = self._q("src",
                    "select table_name, engine from information_schema.tables"
                    " where table_schema = %s and table_type in ('BASE TABLE', 'SYSTEM VERSIONED')"
                    " and engine is not null and engine <> 'InnoDB'", (db,))
                # MEMORY empties on restart and MyISAM has no transactions, so
                # neither can be handed over by a consistent snapshot
                inv.add("not-carried", db, "non-InnoDB tables",
                        [f"{r[0]} ({r[1]})" for r in eng])

                ev = self._q("src",
                    "select event_name, status from information_schema.events"
                    " where event_schema = %s", (db,))
                if ev:
                    # the schema repair makes them switched off
                    # (`_event_repair`): one enabled on the target rewrites
                    # rows while the sync is still running - the same hazard
                    # as a TTL index - so switching them on is the cutover's
                    inv.add("decide-then-apply", db,
                            "events to switch on at cutover (the schema"
                            " repair makes them switched off)",
                            [r[0] for r in ev if str(r[1]).upper() == "ENABLED"])
            except Exception as e:
                inv.unknown("no-row-key", db, f"catalogue query failed: {e}")
        self._definer_handwork(inv)
        return inv

    def _definer_handwork(self, inv):
        """Definers that will not exist on the target.

        A view or routine keeps the `DEFINER=user@host` it was created with.
        Recreating it against a target where that account is missing succeeds
        - and then fails at the moment something uses it, which is usually
        after cutover.
        """
        for db in self.databases():
            try:
                rows = self._q("src",
                    "select definer, 'routine', routine_name from"
                    " information_schema.routines where routine_schema = %s"
                    " union all select definer, 'view', table_name from"
                    " information_schema.views where table_schema = %s"
                    " union all select definer, 'trigger', trigger_name from"
                    " information_schema.triggers where trigger_schema = %s"
                    " union all select definer, 'event', event_name from"
                    " information_schema.events where event_schema = %s",
                    (db, db, db, db))
            except Exception as e:
                inv.unknown("target-prereq", db,
                            f"cannot read object definers: {e}")
                continue
            if not rows:
                continue
            try:
                have = {f"{u}@{h}" for u, h in
                        self._q("dst", "select user, host from mysql.user")}
            except Exception as e:
                inv.unknown("target-prereq", db,
                            f"cannot read mysql.user on the target to check"
                            f" definers: {e}")
                continue
            missing = sorted({f"{kind} {name} (definer {d})"
                              for d, kind, name in rows
                              if str(d).replace("`", "") not in have})
            inv.add("target-prereq", db,
                    "accounts named as DEFINER but absent on the target",
                    missing)

    def list_move_tables(self, db):
        return [("", t) for t in self._tables("src", db)]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        t = tbl if tbl else sch
        key = self.move_key(db, sch, tbl)
        ddb = self._d("dst", db)
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
                # The hop's row filter on both ends, as the PostgreSQL
                # copier does it: read only what it selects, replace only
                # what it selects. `%` doubled where the statement also
                # carries parameters, or a `like 'a%'` filter would be read
                # as a placeholder.
                rf = (self.hop.row_filter(db, t)
                      if hasattr(self.hop, "row_filter") else None)
                rfp = f" and ({rf.replace('%', '%%')})" if rf else ""
                if not intpk:
                    log(f"{key}: no single int pk, single-shot copy")
                    dcur.execute(f"delete from `{ddb}`.`{t}` where ({rf})"
                                 if rf else f"truncate `{ddb}`.`{t}`")
                    scur.execute(f"select {collist} from `{db}`.`{t}`"
                                 + (f" where ({rf})" if rf else ""))
                    while True:
                        rows = scur.fetchmany(5000)
                        if not rows:
                            break
                        dcur.executemany(
                            f"insert into `{ddb}`.`{t}` ({collist})"
                            f" values ({ph})", rows)
                    dconn.commit()
                    st["done"] = True
                    ck.save()
                    return
                mm = self._q("src", f"select coalesce(min(`{intpk}`), 0),"
                             f" coalesce(max(`{intpk}`), 0),"
                             f" min(`{intpk}`) is not null"
                             f" from `{db}`.`{t}`"
                             + (f" where ({rf})" if rf else ""))[0]
                lo, hi, has = int(mm[0]), int(mm[1]), bool(mm[2])
                last = st.get("last", lo - 1)
                while last < hi:
                    nxt = min(last + chunk, hi)
                    dcur.execute(f"delete from `{ddb}`.`{t}`"
                                 f" where `{intpk}` > %s and `{intpk}` <= %s"
                                 + rfp, (last, nxt))
                    scur.execute(f"select {collist} from `{db}`.`{t}`"
                                 f" where `{intpk}` > %s and `{intpk}` <= %s"
                                 + rfp, (last, nxt))
                    while True:
                        rows = scur.fetchmany(5000)
                        if not rows:
                            break
                        dcur.executemany(
                            f"insert into `{ddb}`.`{t}` ({collist})"
                            f" values ({ph})", rows)
                    dconn.commit()
                    last = nxt
                    st["last"] = last
                    ck.save()
                    log(f"{key}: up to {intpk}={last:,} of {hi:,}")
                # each chunk replaces its own key range, so a target row
                # outside the source's whole range was in none of them
                outside = (f"not (`{intpk}` between {lo} and {hi})" if has
                           else "true")
                if rf:
                    outside = f"({outside}) and ({rf})"
                gone = dcur.execute(f"delete from `{ddb}`.`{t}`"
                                    f" where {outside}")
                dconn.commit()
                if gone:
                    log(f"{key}: removed {gone:,} target rows the source"
                        " does not have")
                st["done"] = True
                ck.save()
        finally:
            sconn.close()
            dconn.close()

    #: a replica follows a whole server, so one serves every database of
    #: the hop (`_replica_filters` scopes it)
    REPLICATES_THE_SERVER = True

    def native_replica_unsafe(self):
        """A database the hop renames. The replica's rewrite applies to the
        database in use, not to one a statement names. Measured on 8.4
        under the plan's own filters: `alter table cx.t add column ...`
        and `create table cx.t3 ...`, run with another database in use or
        none, never reached the renamed target. The replica kept running,
        and the next row's values for the added columns were dropped,
        without an error. With the same name on both sides every table,
        view, routine, trigger and index statement arrived."""
        renamed = self._replica_filters()[0]
        if not renamed:
            return None
        return (f"the hop renames {', '.join(f'{a} to {b}' for a, b in renamed)},"
                " and the server's own replica renames only the database in"
                " use: a schema change naming its table with the database"
                " (`alter table db.t ...`) does not arrive, and the rows"
                " after it lose the columns it added, with the replica still"
                " running")

    def _replica_filters(self):
        """What a native replica of this hop may write on the target, as
        rules in the target's names: every table of the hop's databases,
        none of the tables it excludes, and each database under the name
        the hop maps it to.

        Measured on 8.4 without them: a write into a database the hop does
        not name arrived on the target, and so did a row for a table the
        hop excludes - which stopped the replica on a key the target
        already had. With them the first was not applied, and neither was
        a DROP of the excluded table. Rules name the target's databases:
        a rewrite is applied before them."""
        rewrite, do, ignore = [], [], []
        for d in self.databases():
            tdb = self._d("dst", d)
            if tdb != d:
                rewrite.append((d, tdb))
            do.append(f"{tdb}.%")
            if not getattr(self.hop, "exclude", None):
                continue
            # the tables it excludes as they are named now; a table created
            # later under an excluded pattern is not in the list
            ignore += [f"{tdb}.{t}" for t in self._all_tables("src", d)
                       if self.hop.excluded(d, t)]
        return rewrite, do, ignore

    def _replica_filter_sql(self, brand):
        """The filters as the target takes them, and the lines its
        configuration needs for them to outlive a restart. Measured: after
        the target restarted, the replica started again by itself with no
        filters at all - set for the channel or not."""
        rewrite, do, ignore = self._replica_filters()
        conf = ([f"replicate-rewrite-db = {a}->{b}" for a, b in rewrite]
                + [f"replicate-wild-do-table = {r}" for r in do]
                + [f"replicate-ignore-table = {r}" for r in ignore])
        if brand == "mariadb":
            stmts = [f"set global replicate_rewrite_db ="
                     f" '{','.join(f'{a}->{b}' for a, b in rewrite)}';"
                     ] if rewrite else []
            stmts += [f"set global replicate_wild_do_table ="
                      f" '{','.join(do)}';"]
            if ignore:
                stmts.append(f"set global replicate_ignore_table ="
                             f" '{','.join(ignore)}';")
            return stmts, conf
        parts = []
        if rewrite:
            parts.append("REPLICATE_REWRITE_DB = ("
                         + ", ".join(f"({a}, {b})" for a, b in rewrite)
                         + ")")
        parts.append("REPLICATE_WILD_DO_TABLE = ("
                     + ", ".join(f"'{r}'" for r in do) + ")")
        if ignore:
            parts.append("REPLICATE_IGNORE_TABLE = (" + ", ".join(ignore)
                         + ")")
        return [f"change replication filter {', '.join(parts)}"
                " for channel '';"], conf

    #: Appliers for migkit's replica where the target has one. Measured,
    #: 100,000 one-row transactions queued on the replica before its
    #: applier started, runs after a warm-up:
    #:
    #:     MySQL 8.4     replica_parallel_workers 1   28.8s 55.3s 51.0s
    #:                                            4   14.2s 16.4s 27.4s
    #:     MariaDB 11.8  slave_parallel_threads   0    7.6s  7.9s  9.1s  7.9s
    #:                                            4    5.0s  4.7s  5.2s  4.7s
    #:
    #: The same rows arrived either way. Both commit in the source's order
    #: with more than one (MySQL only with `replica_preserve_commit_order`,
    #: which is asked), so a reader of the target never sees a transaction
    #: before one the source committed ahead of it. MySQL has 4 by default
    #: from 8.0.27; MariaDB has none by default.
    APPLIERS = 4

    def _parallel_apply_sql(self, brand):
        """(statements, the configuration line that keeps them) giving the
        replica `APPLIERS` appliers, or ([], None) where it has more than
        one already or the target cannot be asked - once: this decides
        an option, and a plan should not wait on retries for it."""
        import pymysql
        n = self.APPLIERS
        try:
            conn = self._conn("dst", retry=False)
        except pymysql.err.MySQLError:
            return [], None
        try:
            with conn.cursor() as cur:
                if brand == "mariadb":
                    cur.execute("select @@slave_parallel_threads")
                    if int(cur.fetchone()[0]) > 0:
                        return [], None
                    # it cannot be changed while any replica on the target
                    # is running
                    cur.execute("show all slaves status")
                    cols = [d[0] for d in cur.description or ()]
                    for row in cur.fetchall():
                        row = dict(zip(cols, row))
                        if "Yes" in (
                                self._repl_field(row, self._REPL_FIELDS["io"]),
                                self._repl_field(row,
                                                 self._REPL_FIELDS["sql"])):
                            return [], None
                    return ([f"set global slave_parallel_threads = {n};"],
                            f"slave_parallel_threads = {n}")
                cur.execute("select @@replica_parallel_workers,"
                            " @@replica_preserve_commit_order")
                workers, ordered = cur.fetchone()
        except pymysql.err.MySQLError:
            return [], None
        finally:
            conn.close()
        if int(workers) > 1 or int(ordered) != 1:
            return [], None
        return ([f"set global replica_parallel_workers = {n};"],
                f"replica_parallel_workers = {n}")

    def replicate_sql(self, db, copy_data=True, secret=None, copied=None):
        """The statements that make the target a replica of the source.

        `secret` is the replication user's password. A plan printed for a
        person to run carries `CHANGE_ME` for them to replace; a plan migkit
        runs itself is given a fresh random one. It used to run the
        placeholder: `--go` made a user reachable from any host whose
        password is printed in this file.
        """
        secret = secret or "CHANGE_ME"
        s, t = self.hop.source, self.hop.target
        try:
            pos = self._binlog_position("src")
        except Exception:
            pos = None
        coords = f"file {pos[0]} pos {pos[1]}" if pos else "unknown"
        brand = self._brands()[0].name
        gtid_on, gtid_note = self._gtid_state(brand)
        exact = (copied or {}).get("exact")
        if exact:
            # the copy migkit made was a snapshot at this position, and a
            # replica started here applies exactly what came after it -
            # measured on 8.4 with GTID on: an insert and an update made
            # after the dump arrived, and the replica kept running. By file
            # and position, since the target's own GTID set does not hold
            # the source's and would ask for everything again.
            pos, gtid_on = (exact["log_file"], int(exact["log_pos"])), False
            coords = (f"the position the copy of {copied['at']} was taken"
                      f" at, file {pos[0]} pos {pos[1]}")
        elif copied:
            gtid_note += (
                f"; the copy of {copied['at']} was not taken at a position"
                " of its own, so a replica started now misses what the"
                " source changed since, and one started earlier stops on"
                " rows the copy already carried - move with --mode"
                " full+cdc instead")
        src_cmds = [
            f"create user if not exists '{self.REPL_USER}'@'%'"
            f" identified by '{secret}';",
            # a user an earlier run left keeps its old password otherwise,
            # and the replica below would be given this one
            f"alter user '{self.REPL_USER}'@'%' identified by '{secret}';",
            f"grant replication slave on *.* to '{self.REPL_USER}'@'%';",
        ]
        filters, conf = self._replica_filter_sql(brand)
        parallel, applier = self._parallel_apply_sql(brand)
        keep = ("; these scope the replica to the hop and last only until"
                " the target restarts - add them to its configuration"
                + (" (the parameter group)" if "rds.amazonaws.com"
                   in (t.host or "") else "")
                + ": " + "; ".join(conf))
        if applier:
            keep += (f"; {applier} gives the replica {self.APPLIERS}"
                     " appliers where it had one, and lasts only until the"
                     " target restarts as well")
        if "rds.amazonaws.com" in (t.host or ""):
            # a managed target takes filters from its parameter group only
            filters = parallel = []
            keep = ("; set these in the target's parameter group before"
                    " starting it, or the replica writes every database and"
                    " table of the source: " + "; ".join(conf))
            if applier:
                keep += (f"; and {applier}, or it applies with one")
            dst_cmds = [
                f"call mysql.rds_set_external_source ('{s.host}', {s.port},"
                f" '{self.REPL_USER}', '{secret}',"
                + (f" '{pos[0]}', {pos[1]}," if pos else " '', 4,")
                + " 0);",
                "call mysql.rds_start_replication;",
            ]
        elif brand == "mariadb":
            # MariaDB has never had `CHANGE REPLICATION SOURCE TO` - measured
            # on 11.8, it is ERROR 1064, a syntax error rather than a
            # difference in behaviour. Its own form is `CHANGE MASTER TO`
            # with `MASTER_USE_GTID`, which MySQL 8 rejects with the same
            # error, so the two are not interchangeable in either direction.
            # `slave_pos`, not `current_pos`: measured on 11.8, a target
            # that keeps a log of its own had its own writes (the tables
            # made for the copy) in `current_pos` as 0-52-3, asked the
            # source for that, and stopped on error 1236 - "not in the
            # master's binlog". `slave_pos` holds only what was replicated,
            # and is the same position on a target that keeps no log.
            auto = ("MASTER_USE_GTID = slave_pos" if gtid_on else
                    (f"MASTER_LOG_FILE = '{pos[0]}',"
                     f" MASTER_LOG_POS = {pos[1]}"
                     + (", MASTER_USE_GTID = no" if exact else "")
                     if pos else ""))
            dst_cmds = [
                f"change master to MASTER_HOST = '{s.host}',"
                f" MASTER_PORT = {s.port}, MASTER_USER = '{self.REPL_USER}',"
                f" MASTER_PASSWORD = '{secret}', {auto};",
                *filters,
                *parallel,
                "start slave;",
            ]
        else:
            auto = "SOURCE_AUTO_POSITION = 1" if gtid_on else \
                ((f"SOURCE_AUTO_POSITION = 0, " if exact else "")
                 + f"SOURCE_LOG_FILE = '{pos[0]}',"
                 f" SOURCE_LOG_POS = {pos[1]}" if pos else "")
            dst_cmds = [
                f"change replication source to SOURCE_HOST = '{s.host}',"
                f" SOURCE_PORT = {s.port}, SOURCE_USER = '{self.REPL_USER}',"
                f" SOURCE_PASSWORD = '{secret}',"
                # MySQL 8 only: MariaDB has no such option
                f" GET_SOURCE_PUBLIC_KEY = 1, {auto};",
                *filters,
                *parallel,
                "start replica;",
            ]
        stop = ["stop slave;", "reset slave all;"] if brand == "mariadb" \
            else ["stop replica;", "reset replica all;"]
        return {"src": src_cmds, "dst": dst_cmds,
                "drop_src": [f"drop user if exists '{self.REPL_USER}'@'%';"],
                "drop_dst": stop,
                "status": ("show slave status" if brand == "mariadb"
                           else "show replica status"),
                "note": f"written for {brand}; binlog now at {coords},"
                        f" {gtid_note},"
                        " run move first then replicate from these coords"
                        + keep}

    # `SHOW REPLICA STATUS` on MySQL 8 and `SHOW SLAVE STATUS` on MariaDB
    # answer the same facts under different column names. Asked in this
    # order so the first name present wins.
    _REPL_FIELDS = {
        "io": ("Replica_IO_Running", "Slave_IO_Running"),
        "sql": ("Replica_SQL_Running", "Slave_SQL_Running"),
        "lag": ("Seconds_Behind_Source", "Seconds_Behind_Master"),
        "host": ("Source_Host", "Master_Host"),
        "user": ("Source_User", "Master_User"),
    }

    @staticmethod
    def _repl_field(row, names):
        for n in names:
            if n in row:
                return row[n]
        return None

    def apply_replication_stmt(self, side, db, stmt):
        """MySQL's half of the contract.

        Nothing here needs bounding the way `CREATE SUBSCRIPTION` does:
        `START REPLICA` returns at once whether or not the target can reach
        the source. That is the opposite failure, and `replication_status`
        is what catches it.
        """
        return self._q(side, stmt)

    def replication_status(self, db, sql):
        """What `START REPLICA` did not say.

        It returns success whether or not the target can reach the source -
        the IO thread starts, fails, and retries behind it. Measured on
        MySQL 8 with the two servers on networks that cannot route to each
        other: `start replica` returned OK, and `Replica_IO_Running` was
        `Connecting` with `Last_IO_Error` holding the timeout. Printing
        "replication started" over that is the failure this line exists to
        stop.
        """
        rows = self._q_named("dst", sql)
        if not rows:
            return ("no replica configured on the target: the statements ran"
                    " but nothing is replicating")
        r = rows[0]
        io = self._repl_field(r, self._REPL_FIELDS["io"])
        sq = self._repl_field(r, self._REPL_FIELDS["sql"])
        lag = self._repl_field(r, self._REPL_FIELDS["lag"])
        host = self._repl_field(r, self._REPL_FIELDS["host"])
        out = [f"source {host}" if host else "source unknown",
               f"io {io}", f"sql {sq}",
               "lag unknown" if lag is None else f"lag {lag}s"]
        for key in ("Last_IO_Error", "Last_SQL_Error"):
            err = (r.get(key) or "").strip()
            if err:
                out.append(f"{key} {err[:160]}")
        if str(io) != "Yes" or str(sq) != "Yes":
            out.append("NOT replicating")
        # the filters are gone after a restart of the target, and the
        # replica starts again without them
        _, do, _ = self._replica_filters()
        have = str(r.get("Replicate_Wild_Do_Table") or "")
        missing = [x for x in do if x not in have.split(",")]
        if missing:
            out.append(f"NOT limited to this hop: no rule for"
                       f" {', '.join(missing)} - it writes every database and"
                       " table of the source")
        return ", ".join(out)

    def src_lsn(self, db):
        """Where the source is now, as its executed GTID set - the same
        question the PostgreSQL engine answers with a WAL position, under
        the same name so a caller asks every engine alike.

        None where GTID is off: a file-and-position wait needs the replica
        to be reading the source's own binary log file names, and nothing
        but GTID survives a change of which server the target replicates
        from. No position means no fence, and the caller says so.
        """
        brand = self._brands()[0].name
        on, _ = self._gtid_state(brand)
        if not on:
            return None
        sql = ("select @@gtid_binlog_pos" if brand == "mariadb"
               else "select @@global.gtid_executed")
        got = self._q("src", sql)
        pos = str(got[0][0]).strip() if got and got[0][0] is not None else ""
        return pos or None

    def fence_wait(self, db, gtids, timeout=300):
        """Block until the target has applied everything the source had
        executed at `gtids`. True = fence passed, False = timed out, None =
        nothing to fence on (no position, or the target is not replicating
        from anywhere migkit can see).

        The server waits, not migkit: `WAIT_FOR_EXECUTED_GTID_SET` (MySQL)
        or `MASTER_GTID_WAIT` (MariaDB) returns as soon as the set is
        applied. It is asked in short turns, so a read timeout on the
        connection never cuts a long wait off halfway.
        """
        if not gtids:
            return None
        brand = self._brands()[1].name
        status = ("show slave status" if brand == "mariadb"
                  else "show replica status")
        try:
            if not self._q("dst", status):
                return None
        except Exception:
            return None
        ask = ("select master_gtid_wait(%s, %s)" if brand == "mariadb"
               else "select wait_for_executed_gtid_set(%s, %s)")
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            got = self._q("dst", ask, (gtids, 5))
            if got and got[0][0] is not None and int(got[0][0]) == 0:
                return True
        return False

    def _gtid_state(self, brand):
        """(whether to replicate by GTID, what to say about it).

        MySQL has a `gtid_mode` variable that is ON or OFF. **MariaDB has no
        such variable at all** - measured, `show variables like 'gtid_mode'`
        returns zero rows there rather than a row saying OFF. Reading that
        empty result as falsy is how the plan silently decided GTID was off
        and fell back to file-and-position coordinates, on a server where
        GTID is always available once the binary log is on.
        """
        if brand == "mariadb":
            got = self._q("src", "select @@gtid_binlog_pos,"
                                 " @@gtid_current_pos")
            binlog_pos = str(got[0][0]) if got else ""
            return (True,
                    "gtid available (MariaDB has no gtid_mode to switch;"
                    f" gtid_binlog_pos is {binlog_pos or 'empty so far'})")
        got = self._q("src", "show variables like 'gtid_mode'")
        if not got:
            return (False,
                    "gtid_mode is not a variable on this server and migkit"
                    " does not know this brand's equivalent - treating GTID"
                    " as unavailable, which is the safe direction but may"
                    " not be the true one")
        on = str(got[0][1]).upper() == "ON"
        return (on, f"gtid {'ON' if on else 'OFF'}")

    def setup_target_plan(self, db):
        """The target's preparation, in the order that measures best, with
        the parts migkit does itself said as migkit's commands.

        The old plan loaded the whole schema before the data, so every
        secondary index was maintained row by row through the load
        (measured on 8: 1.150 s for 200,000 rows with three of them, 0.303
        + 0.511 s loading bare and building after), and it created the
        database as `utf8mb4` whatever the source's character set was. The
        move now creates the tables the target lacks, sets the secondary
        indexes aside and builds them once after, and keeps the target's
        triggers off; the database is created in the source's own character
        set and collation."""
        hop, tdb = self.hop.name, self._d("dst", db)
        try:
            cs = self._q("src", "select default_character_set_name,"
                                " default_collation_name from"
                                " information_schema.schemata"
                                " where schema_name = %s", (db,))
        except Exception:
            cs = []
        create = (f"create database `{tdb}` character set {cs[0][0]}"
                  f" collate {cs[0][1]};   -- the source's own" if cs else
                  f"create database `{tdb}` ...;   -- the source's character"
                  " set could not be read: use the one `show create database"
                  f" {db}` gives there")
        return [
            create,
            f"-- tables and rows: migkit move {hop} --go - it creates the"
            " tables the target lacks from the source's definitions, loads"
            " them with the secondary indexes set aside and built once after"
            " (1.41x faster, measured), and keeps the target's triggers off"
            " while it loads",
            f"-- routines, views, triggers and events: migkit schema {hop}"
            " --migration writes them as files to review and apply; events"
            f" are carried disabled (migkit sync {hop} --kind schema) and"
            " switched on at cutover",
            f"-- accounts and their grants: migkit users {hop}",
            f"-- then: migkit check {hop}",
        ]

    def migration_pair(self, db):
        from ..movers import schema_diff
        if not which("atlas"):
            return None, None
        su, tu = self._schema_urls(db)
        # a mapped table's schema is the mapping's, and aligning it with the
        # source would put back the columns the hop drops
        excludes = self._schema_excludes(db)

        def diff(a, b):
            p = schema_diff(a, b, excludes)
            text = p.stdout.strip()
            if p.returncode or "Schemas are synced" in text:
                return ""
            return text
        return diff(tu, su), diff(su, tu)

    def fetch_sample_df(self, side, db, table, limit):
        import pandas as pd
        t = table.split(".", 1)[-1]
        cols = self._cols(db, t)
        rows = self._q(side,
                       f"select * from `{self._d(side, db)}`.`{t}` limit {limit}")
        return pd.DataFrame(rows, columns=cols)

    def watch_sample(self, db):
        import time
        q = ("select coalesce(sum(table_rows), 0) from information_schema.tables"
             " where table_schema=%s")
        return {"db": db, "ts": time.time(),
                "src_rows": int(self._q("src", q, (db,))[0][0]),
                "dst_rows": int(self._q("dst", q, (self._d("dst", db),))[0][0])}
