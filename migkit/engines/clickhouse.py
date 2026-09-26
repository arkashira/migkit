"""ClickHouse as one side of a pair (backlog 33).

Rows are read through the HTTP driver and compared through the in-process
renderer every engine without a canonical SQL rendering shares. A table
migkit creates is a MergeTree ordered by the source's key, with every
column outside the key nullable. A MergeTree does not refuse a second row
with the same key, so a batch written again after a restart first deletes
the keys it carries, which makes the copy converge instead of doubling.
A table that was there before is keyed by its own sorting key.
"""
from .base import Engine, NeutralCopier, Result

SKIP_DBS = {"system", "information_schema", "INFORMATION_SCHEMA"}


def _unwrap(declared):
    """`Nullable(LowCardinality(String))` -> `String`: what a value is,
    without how it is stored."""
    t = str(declared).strip()
    for wrapper in ("Nullable(", "LowCardinality("):
        while t.startswith(wrapper) and t.endswith(")"):
            t = t[len(wrapper):-1].strip()
    return t


class ClickHouseEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "clickhouse"
    SQL_DIALECT = "clickhouse"
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " a ClickHouse one nobody has measured")
    OVER_NETWORK = True
    TEXT_HOLDS_BYTES = ("string", "fixedstring")

    def _client(self, side, db=None):
        import clickhouse_connect
        ep = self.hop.source if side == "src" else self.hop.target
        return clickhouse_connect.get_client(
            host=ep.host, port=int(ep.port or 8123),
            username=ep.user or "default", password=ep.password or "",
            database=self._d(side, db) if db else "default",
            secure=bool(ep.options.get("secure")),
            tz_source="server", query_limit=0, connect_timeout=15)

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    @staticmethod
    def _q(name):
        return "`" + str(name).replace("\\", "\\\\").replace("`", "\\`") + "`"

    def _qualified(self, side, db, table):
        return f"{self._q(self._d(side, db))}.{self._q(table)}"

    def _rows(self, side, db, sql, params=None, settings=None):
        client = self._client(side, db)
        try:
            return [list(r) for r in client.query(
                sql, parameters=params or {},
                settings=settings or {}).result_rows]
        finally:
            client.close()

    #: String columns hold bytes as often as text: read as bytes, and
    #: decoded here only where the column is declared text
    AS_BYTES = {"String": "bytes", "FixedString": "bytes"}

    def _values(self, columns, rows):
        classes = [c for _, c in columns]
        return [[self._value(c, v) for c, v in zip(classes, r)]
                for r in rows]

    @staticmethod
    def _value(cls, v):
        import datetime
        if isinstance(v, (bytes, bytearray)) and cls != "bytes":
            return bytes(v).decode("utf-8", "surrogateescape")
        if isinstance(v, datetime.datetime) and v.tzinfo is not None:
            # read in the server's zone: its wall clock is the value
            return v.replace(tzinfo=None)
        return v

    # ---- the contract ---------------------------------------------------

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        return sorted(r[0] for r in self._rows("src", None,
                                               "show databases")
                      if r[0] not in SKIP_DBS and not self.hop.excluded(r[0]))

    def target_missing(self, db):
        return not self._rows("dst", None, "select count() from"
                                           " system.databases where name ="
                                           " {d:String}",
                              {"d": self._d("dst", db)})[0][0]

    def prepare_target(self, db):
        if not self.target_missing(db):
            return None
        client = self._client("dst")
        try:
            client.command(f"create database {self._q(self._d('dst', db))}")
        finally:
            client.close()
        return f"created database {self._d('dst', db)}"

    def neutral_tables(self, side, db):
        return [t for (t,) in self._rows(
            side, db, "select name from system.tables where database ="
                      " {d:String} and not is_temporary and engine not in"
                      " ('View', 'MaterializedView', 'LiveView')"
                      " order by name", {"d": self._d(side, db)})
            if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        return [(n, _unwrap(t)) for n, t in self._rows(
            side, db, "select name, type from system.columns where database"
                      " = {d:String} and table = {t:String} order by"
                      " position", {"d": self._d(side, db), "t": table})]

    def neutral_key(self, side, db, table):
        got = self._rows(side, db, "select sorting_key from system.tables"
                                   " where database = {d:String} and name ="
                                   " {t:String}",
                         {"d": self._d(side, db), "t": table})
        names = {n for n, _ in self.neutral_columns(side, db, table)}
        key = [k.strip().strip("`") for k in (got[0][0] if got else "")
               .split(",") if k.strip()]
        # an expression in the sorting key is no column to read a key by
        return key if key and set(key) <= names else []

    def _read_query(self, side, db, table, columns, after, limit, where):
        from .. import canon
        names = [n for n, _ in columns]
        key = self.neutral_key(side, db, table)
        conds, params = [], {}
        if key and after is not None:
            places = []
            for i, v in enumerate(after):
                params[f"k{i}"] = canon.sql_value(v)
                places.append(f"{{k{i}:{self._param_type(v)}}}")
            conds.append(f"({', '.join(self._q(k) for k in key)}) >"
                         f" ({', '.join(places)})")
        if where:
            conds.append(f"({where})")
        sql = (f"select {', '.join(self._q(n) for n in names)} from"
               f" {self._qualified(side, db, table)}"
               + (f" where {' and '.join(conds)}" if conds else "")
               + (f" order by {', '.join(self._q(k) for k in key)}"
                  if key else "")
               + (f" limit {int(limit)}" if key and limit else ""))
        return sql, params, key, names

    @staticmethod
    def _param_type(value):
        import datetime
        from decimal import Decimal
        if isinstance(value, datetime.datetime):
            return "DateTime64(6)"
        if isinstance(value, datetime.date):
            return "Date32"
        if isinstance(value, bool):
            return "Bool"
        if isinstance(value, int):
            return "Int64"
        if isinstance(value, float):
            return "Float64"
        if isinstance(value, Decimal):
            return "Decimal(76, 38)"
        return "String"

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        sql, params, key, names = self._read_query(side, db, table, columns,
                                                   after, limit, where)
        client = self._client(side, db)
        try:
            rows = self._values(columns, client.query(
                sql, parameters=params,
                query_formats=self.AS_BYTES).result_rows)
        finally:
            client.close()
        if not rows or not key or not set(key) <= set(names):
            return rows, None
        return rows, tuple(rows[-1][names.index(k)] for k in key)

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        sql, params, _, _ = self._read_query(side, db, table, columns, None,
                                             None, where)
        client = self._client(side, db)
        try:
            with client.query_row_block_stream(
                    sql, parameters=params, query_formats=self.AS_BYTES,
                    settings={"max_block_size": int(size)}) as stream:
                for block in stream:
                    yield self._values(columns, block)
        finally:
            client.close()

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        from .. import canon
        if not key or not keys:
            return {}
        params, tuples = {}, []
        for i, k in enumerate(keys):
            places = []
            for j, v in enumerate(k):
                params[f"v{i}_{j}"] = canon.sql_value(v)
                places.append(f"{{v{i}_{j}:{self._param_type(v)}}}")
            tuples.append("(" + ", ".join(places) + ")")
        cond = (f"({', '.join(self._q(k) for k in key)}) in"
                f" ({', '.join(tuples)})")
        if where:
            cond = f"({cond}) and ({where})"
        client = self._client(side, db)
        try:
            rows = self._values(columns, client.query(
                f"select {', '.join(self._q(n) for n, _ in columns)} from"
                f" {self._qualified(side, db, table)} where {cond}",
                parameters=params, query_formats=self.AS_BYTES).result_rows)
        finally:
            client.close()
        return self._by_key_map(columns, key, rows)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 10000,
                                          where):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        return (n, str(total))

    def table_facts(self, side, db):
        out = {}
        for name, rows, size in self._rows(
                side, db, "select name, total_rows, total_bytes from"
                          " system.tables where database = {d:String}",
                {"d": self._d(side, db)}):
            out[name] = {"rows": rows, "bytes": size,
                         "key": bool(self.neutral_key(side, db, name))}
        return out

    # ---- writing --------------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        defs = []
        for col in columns:
            t = canon.ddl_type("clickhouse", col[1], col[2])
            defs.append(f"{self._q(col[0])} "
                        + (t if col[0] in key else f"Nullable({t})"))
        order = (", ".join(self._q(k) for k in key)) if key else "tuple()"
        return (f"create table {self._qualified(side, db, table)}"
                f" ({', '.join(defs)}) engine = MergeTree order by ({order})")

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a table")
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        self.neutral_create_code(side, db, ddl)
        return ddl

    def neutral_create_code(self, side, db, statement):
        self._target_only(side, "create objects")
        client = self._client(side, db)
        try:
            client.command(statement)
        finally:
            client.close()

    def neutral_write(self, side, db, table, columns, rows):
        """The keys of the batch are deleted first, then the batch
        inserted: a MergeTree keeps a second row with the same key, so a
        batch written again after a restart would double without it."""
        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        client = self._client(side, db)
        try:
            rows = self._wall_clock(client, rows)
            if key:
                at = [names.index(k) for k in key]
                params, tuples = {}, []
                for i, r in enumerate(rows):
                    places = []
                    for j, idx in enumerate(at):
                        v = canon.sql_value(r[idx])
                        params[f"v{i}_{j}"] = v
                        places.append(f"{{v{i}_{j}:{self._param_type(v)}}}")
                    tuples.append("(" + ", ".join(places) + ")")
                client.command(
                    f"delete from {self._qualified(side, db, table)} where"
                    f" ({', '.join(self._q(k) for k in key)}) in"
                    f" ({', '.join(tuples)})", parameters=params,
                    settings={"lightweight_deletes_sync": 2})
            client.insert(table,
                          [[canon.sql_value(v) for v in r] for r in rows],
                          column_names=names, database=self._d(side, db))
        finally:
            client.close()
        return len(rows)

    @staticmethod
    def _wall_clock(client, rows):
        """A time with no zone is a wall-clock reading, and the driver
        would take it as this machine's: measured on a machine at UTC+7,
        `2024-02-29 00:05` arrived as `2024-02-28 17:05`. It is handed
        over as the server's own zone's wall clock, which is how the
        server reads it back."""
        import datetime
        from zoneinfo import ZoneInfo
        zone = ZoneInfo(client.command("select timezone()"))
        return [[v.replace(tzinfo=zone) if isinstance(v, datetime.datetime)
                 and v.tzinfo is None else v for v in r] for r in rows]

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        client = self._client(side, db)
        try:
            gone = client.query(f"select count() from"
                                f" {self._qualified(side, db, table)}"
                                + (f" where {where}" if where else "")
                                ).result_rows[0][0]
            if where:
                client.command(f"delete from"
                               f" {self._qualified(side, db, table)} where"
                               f" {where}",
                               settings={"lightweight_deletes_sync": 2})
            else:
                client.command(f"truncate table"
                               f" {self._qualified(side, db, table)}")
        finally:
            client.close()
        return gone

    def run_rule(self, side, db, sql):
        return self._rows(side, db, sql, settings={"readonly": 1})

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_deep(self, db):
        """Parts a merge or an insert left broken, and mutations - the
        deletes a restart runs - that have not finished: the rows a check
        reads are not settled until they have."""
        out = []
        for side, who in (("src", "source"), ("dst", "target")):
            try:
                stuck = self._rows(
                    side, db, "select table, command, latest_fail_reason"
                              " from system.mutations where database ="
                              " {d:String} and not is_done",
                    {"d": self._d(side, db)})
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(Result("deep", f"{db} {who} mutations", "error",
                                  f"could not read the {who}'s mutations:"
                                  f" {str(e).splitlines()[0][:90]}"))
                continue
            if stuck:
                failing = [r for r in stuck if r[2]]
                out.append(Result(
                    "deep", f"{db} {who} mutations",
                    "diff" if failing else "warn",
                    f"{len(stuck)} changes still being applied on the {who}"
                    f" ({', '.join(sorted({r[0] for r in stuck})[:5])})"
                    + (f"; failing: {failing[0][2][:90]}" if failing else "")
                    + " - rows read now are not settled", "",
                    "wait for them, then check again"))
            else:
                out.append(Result("deep", f"{db} {who} mutations", "ok",
                                  f"no change still being applied on the"
                                  f" {who}"))
        return out
