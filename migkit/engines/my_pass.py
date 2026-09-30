"""MySQL into MySQL with no row passing through migkit (backlog R20 items 1
and 4).

The table copier read each range through the driver - a Python object a
value - tallied every row here, rendered each value again as the bulk
load's text and sent that. Here the source's own client reads the range:
each row rendered by the source, in SQL, as exactly the line the target's
bulk load reads (a tab between fields, the load's escapes, `\\N` for NULL,
bytes as hex), under one consistent snapshot that has digested the range
first; the lines go into the pinned `load data local` (`_load_feed`) 64 KB
at a time as they arrive, never a row at a time; the target digests what
it took in the same transaction, which commits only where the two numbers
and the counts agree and the load raised no warning.

Session settings only, never the server's: `unique_checks = 0` where no row
loaded can meet another it would duplicate - the table emptied for the
load, and every unique index of the target's one the source enforced on
what it was read from - and `sql_log_bin = 0` only where the hop states
its target has no replicas (`target_has_replicas: false`), since a
replica, and a point-in-time restore of the target, read that log.
"""
import threading

from ..stall import Guard

#: values a load cannot be handed as text and read back the same
UNFIT = {"geometry", "point", "linestring", "polygon", "multipoint",
         "multilinestring", "multipolygon", "geometrycollection",
         "geomcollection", "vector"}
#: bytes, rendered as hex and turned back by the load
HEX = {"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"}
#: bits, rendered as their number and cast back
NUMBER = {"bit"}
#: text that can hold none of the load's special characters
PLAIN = {"tinyint", "smallint", "mediumint", "int", "integer", "bigint",
         "decimal", "numeric", "float", "double", "real", "year", "date",
         "datetime", "timestamp", "time"}

#: each special character of the load, and what stands for it: the
#: escape first, so the escapes added after are not escaped again
_ESCAPES = ((92, "92, 92"), (9, "92, 116"), (10, "92, 110"),
            (13, "92, 114"), (0, "92, 48"))


def _u(codes):
    return f"char({codes} using utf8mb4)"


def _escaped(expr):
    for c, r in _ESCAPES:
        expr = f"replace({expr}, {_u(c)}, {_u(r)})"
    return expr


def kind(data_type):
    """How a column of `data_type` crosses: "hex", "number", "plain",
    "text", or None where it cannot."""
    t = str(data_type).lower()
    if t in UNFIT:
        return None
    if t in HEX:
        return "hex"
    if t in NUMBER:
        return "number"
    if t in PLAIN:
        return "plain"
    return "text"


def line_expr(columns):
    """The SQL rendering one row, `[(name, kind)]`, as the load's line."""
    parts = []
    for name, k in columns:
        q = "`" + name.replace("`", "``") + "`"
        if k == "hex":
            e = f"hex({q})"
        elif k == "number":
            e = f"cast({q} as unsigned)"
        elif k == "plain":
            e = f"cast({q} as char)"
        else:
            e = _escaped(f"cast({q} as char)")
        parts.append(f"coalesce({e}, {_u('92, 78')})")
    return f"concat_ws({_u(9)}, " + ", ".join(parts) + ")"


class _Guarded:
    """A stream read through a stall guard, its bytes counted."""

    def __init__(self, stream, guard):
        self.stream, self.guard, self.bytes = stream, guard, 0

    def read(self, n):
        self.guard.moved()
        got = self.stream.read(n)
        self.guard.moved()
        self.bytes += len(got)
        return got


class Different(Exception):
    """A range the target did not take as the source read it."""


class TableCopy:
    """One table's pass-through: the columns as they cross, the source
    client's statements, and a target connection each worker keeps from
    range to range."""

    def __init__(self, eng, db, table, columns, log):
        self.eng, self.db, self.table, self.log = eng, db, table, log
        self.columns = columns
        self.names = [n for n, _ in columns]
        self.kinds = [("hex" if k == "hex" else "number" if k == "number"
                       else "text") for _, k in columns]
        # what both sides' digests read, asked once for the table's copy
        self.digest_cols = eng._comparable_columns(db, eng, table, eng,
                                                   table)
        self._local = threading.local()
        self._opened, self._lock = [], threading.Lock()
        self._said = set()

    def _target(self):
        conn = getattr(self._local, "conn", None)
        if conn is None or not conn.open:
            conn = self.eng._conn("dst")
            self._local.conn = conn
            with self._lock:
                self._opened.append(conn)
        return conn

    def _say_once(self, what):
        with self._lock:
            if what in self._said:
                return
            self._said.add(what)
        self.log(what)

    def _source(self, where, order):
        """The source's client reading the range under one consistent
        snapshot: its digest line first, then the load's lines."""
        import subprocess

        from ..util import tool_env
        eng, s = self.eng, self.eng.hop.source
        qt = (f"`{self.db.replace('`', '``')}`."
              f"`{self.table.replace('`', '``')}`")
        w = f" where {where}" if where else ""
        digest = eng._range_digest_sql("src", self.db, self.table, where,
                                       self.digest_cols)
        script = ("set session time_zone = '+00:00';"
                  " set session transaction isolation level repeatable"
                  " read; start transaction with consistent snapshot,"
                  f" read only; {digest};"
                  f" select {line_expr(self.columns)} from {qt}{w}"
                  + (f" order by `{order}`" if order else "") + ";"
                  " commit;")
        args = ["mysql", "-h", s.host, "-P", str(s.port), "-u", s.user,
                "--batch", "--raw", "--skip-column-names", "--quick",
                # a row of large values as one line: the most there is
                "--max-allowed-packet=1073741824",
                "--default-character-set=utf8mb4", *eng._client_tls("src"),
                "-e", script, self.db]
        return subprocess.Popen(args, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=tool_env({"MYSQL_PWD": s.password or ""}))

    def copy(self, where, clear, order, what, whole=False):
        """The rows `where` selects, in place of what `clear` removes, in
        one transaction on the target that commits only where both
        servers' digests, the counts and the load's silence agree. Returns
        the rows copied; raises `Different` otherwise, the target as it
        was."""
        from ..engines.mysql import _load_feed
        eng = self.eng
        proc = self._source(where, order)
        conn = self._target()
        guard = Guard(what, (proc,))
        failed = None
        try:
            line = proc.stdout.readline()
            guard.moved()
            head = line.decode(errors="replace").rstrip("\n").split("\t")
            if len(head) != 2 or not head[0].isdigit():
                raise RuntimeError(self._said_by(proc) or "the source's read"
                                   " gave no digest")
            n, theirs = int(head[0]), head[1]
            with conn.cursor() as cur:
                cur.execute("set foreign_key_checks = 0")
                cur.execute("set session time_zone = '+00:00'")
                cur.execute("set session transaction isolation level read"
                            " committed")
                cur.execute("set unique_checks = "
                            + ("0" if whole and self._unique_safe() else "1"))
                self._binlog_off(cur)
                cur.execute(clear)
                got = _load_feed(cur, eng._d("dst", self.db), self.table,
                                 self.names, self.kinds,
                                 _Guarded(proc.stdout, guard))
                warned = int(cur.warning_count or 0)
                said = self._ended(proc)
                if said:
                    raise RuntimeError(said)
                cur.execute(eng._range_digest_sql("dst", self.db,
                                                  self.table, where,
                                                  self.digest_cols))
                mine = cur.fetchone()
            mine_n, mine_d = int(mine[0]), str(mine[1])
            if warned or got != n or mine_n != n or mine_d != theirs:
                conn.rollback()
                raise Different(
                    f"{got:,} of {n:,} rows taken, {warned:,} warnings"
                    if warned or got != n else
                    "both servers digest it differently")
            conn.commit()
            return n
        except Different:
            raise
        except Exception as e:  # noqa: BLE001 - the read's words first
            failed = e
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 - the connection went too
                pass
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            guard.stop()
        raise RuntimeError(f"{what}: {self._said_by(proc) or failed}"[:600]) \
            from failed

    @staticmethod
    def _said_by(proc):
        try:
            return (proc.stderr.read() or b"").decode(
                errors="replace").strip()[-300:]
        except Exception:  # noqa: BLE001 - nothing more to say
            return ""

    def _ended(self, proc):
        try:
            proc.wait(timeout=60)
        except Exception:  # noqa: BLE001 - it is killed on the way out
            return "the source's read did not end"
        if proc.returncode:
            return (self._said_by(proc)
                    or f"the source's read ended with {proc.returncode}")
        return ""

    def _unique_safe(self):
        """Whether the load may leave its unique checks to the source's:
        every unique index of the target's table is one the source's table
        enforces on the same columns, so no row read under one snapshot
        duplicates another."""
        got = self.__dict__.get("_uniques_ok")
        if got is None:
            got = self._uniques("dst") <= self._uniques("src")
            self._uniques_ok = got
        return got

    def _uniques(self, side):
        eng = self.eng
        rows = eng._q(side, "select index_name, group_concat(column_name"
                            " order by seq_in_index) from"
                            " information_schema.statistics where"
                            " table_schema = %s and table_name = %s and"
                            " non_unique = 0 group by index_name",
                      (eng._d(side, self.db), self.table))
        return {r[1] for r in rows}

    def _binlog_off(self, cur):
        """`sql_log_bin = 0` where the hop states its target has no
        replicas; said once where the target's user may not."""
        opts = self.eng.hop.options or {}
        if opts.get("target_has_replicas", True) is not False:
            return
        try:
            cur.execute("set sql_log_bin = 0")
        except Exception as e:  # noqa: BLE001 - loaded with the log, then
            self._say_once(f"{self.table}: the target's log is written for"
                           " the load all the same - its user may not turn"
                           f" it off ({str(e)[:100]})")

    def close(self):
        with self._lock:
            opened, self._opened = self._opened, []
        for conn in opened:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - it is going anyway
                pass
