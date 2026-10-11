"""ClickHouse as one side of a pair (backlog 33).

Rows are read through the HTTP driver and compared through the in-process
renderer every engine without a canonical SQL rendering shares. A table
migkit creates is a MergeTree ordered by the source's key, with every
column outside the key nullable. A MergeTree does not refuse a second row
with the same key, so a batch written again after a restart first deletes
the keys it carries, which makes the copy converge instead of doubling.
A table that was there before is keyed by its own sorting key.

Between two ClickHouse servers a move is the target's own work where it
can reach the source: it pulls each of the source's partitions, and a
follow copies again the partitions whose parts changed (`native_bulk`,
`tail_apply`); each partition is held to the source's by the fingerprint
both servers compute.
"""
import re

from .. import pieces
from .base import (Engine, NeutralCopier, Result, clauses,
                   rows_counted)

SKIP_DBS = {"system", "information_schema", "INFORMATION_SCHEMA"}


def _unwrap(declared):
    """`LowCardinality(Nullable(String))` -> `String`: what a value is,
    without how it is stored. Taken off in whatever order they are nested:
    that one is the only order ClickHouse takes the two in, and a single
    pass stripping `Nullable(` first left `Nullable(String)` behind, a type
    with no class, so the column was named and never compared."""
    t = str(declared).strip()
    peeled = True
    while peeled:
        peeled = False
        for wrapper in ("Nullable(", "LowCardinality("):
            if t.startswith(wrapper) and t.endswith(")"):
                t = t[len(wrapper):-1].strip()
                peeled = True
    return t


def _nullable(declared):
    """Whether a ClickHouse column takes NULL, whatever else wraps it."""
    t = str(declared).strip()
    while t.startswith("LowCardinality(") and t.endswith(")"):
        t = t[len("LowCardinality("):-1].strip()
    return t.startswith("Nullable(")


class ClickHouseEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "clickhouse"
    #: the family `variants` names the builds of; its version is its own
    #: (`case.FAMILY_NUMBERED`)
    ENGINE_FAMILY = "clickhouse"
    SQL_DIALECT = "clickhouse"
    #: the checks that predict what a move will meet, run before it
    #: (`assess`): rows the source holds that a read does not give
    PREFLIGHT = ("_source_unsettled",)
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " a ClickHouse one nobody has measured")
    OVER_NETWORK = True
    TEXT_HOLDS_BYTES = ("string", "fixedstring")
    #: large values - a String holds any length: sized by the server
    #: (`length`, its bytes), a wide value hashed where it lies (`MD5`) when
    #: a digest is folded here, and blocks read sized by the table's widest
    #: row so a block of wide rows is never read at once (`_block_rows`)
    LARGE_VALUES = pieces.Sql(
        "sized by the server, read in blocks cut by bytes, wide values"
        " hashed where they lie",
        length="length({c})", types=("string", "fixedstring"),
        md5="lower(hex(MD5({c})))", md5_classes=("bytes", "text"))

    def _client(self, side, db=None, wait=None):
        import clickhouse_connect
        ep = self.hop.source if side == "src" else self.hop.target
        return clickhouse_connect.get_client(
            host=ep.host, port=int(ep.port or 8123),
            username=ep.user or "default", password=ep.password or "",
            database=self._d(side, db) if db else "default",
            secure=bool(ep.options.get("secure")),
            tz_source="server", query_limit=0, connect_timeout=15,
            **({"send_receive_timeout": int(wait)} if wait else {}))

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

    def _values(self, columns, rows, wide=()):
        from .. import nanotime
        classes = [c for _, c in columns]
        at = {i for i, (n, _) in enumerate(columns) if n in wide}
        return [[nanotime.parse(v) if i in at else self._value(c, v)
                 for i, (c, v) in enumerate(zip(classes, r))]
                for r in rows]

    @staticmethod
    def _digits(declared):
        """(digits after the second, zone or None) of a `DateTime64`, or
        None for any other type."""
        import re
        m = re.match(r"\s*DateTime64\(\s*(\d+)\s*(?:,\s*'([^']*)')?",
                     str(declared))
        return (int(m.group(1)), m.group(2)) if m else None

    def _wide_times(self, side, db, table):
        """{column: `toString` of it} for the `DateTime64` columns of more
        than six digits: the driver reads them at six (measured,
        `DateTime64(9)` `...00.123456789` as `...00.123456`), the server's
        text of one has all nine. Asked once per table."""
        known = self.__dict__.setdefault("_wide_times_of", {})
        at = (side, db, str(table))
        if at not in known:
            columns = self.neutral_columns(side, db, table)
            wide = {n: f"toString({self._q(n)})" for n, t in columns
                    if (self._digits(t) or (0,))[0] > 6}
            if not columns:
                # a table not there yet is asked again once it is
                return wide
            known[at] = wide
        return known[at]

    @staticmethod
    def _value(cls, v):
        import datetime
        if isinstance(v, (list, tuple)) and cls != "bytes":
            # an Array's elements, each as a column's value is: a
            # `String` one is read as bytes (`AS_BYTES`) and decoded here
            return [ClickHouseEngine._value(cls, x) for x in v]
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
            if self.in_scope(db, t)]

    def neutral_columns(self, side, db, table):
        return [(n, _unwrap(t)) for n, t in self._rows(
            side, db, "select name, type from system.columns where database"
                      " = {d:String} and table = {t:String} order by"
                      " position", {"d": self._d(side, db), "t": table})]

    def column_facts(self, side, db, table):
        """Which columns take NULL. One that does not writes a NULL it is
        given as 0 or '' - `input_format_null_as_default`, on by default -
        so a NULL the source holds has to be counted before it is moved.
        A table migkit builds makes every column outside its key Nullable,
        and a key the source keys by holds no NULL."""
        if table is None:
            return {None: {"null": True}}
        return {n: {"null": _nullable(t)} for n, t in self._rows(
            side, db, "select name, type from system.columns where database"
                      " = {d:String} and table = {t:String}",
            {"d": self._d(side, db), "t": table})}

    def row_bucket_sql(self, side, db, table, columns, n):
        """The server's own hash of the whole row, in [0, n), for a table
        whose sorting key names no column (`ranges.keyless`)."""
        if not columns:
            return None
        return (f"cityHash64({', '.join(self._q(c) for c, _ in columns)})"
                f" % {int(n)}")

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

    def _read_query(self, side, db, table, columns, after, limit, where,
                    select=None):
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
        wide = self._wide_times(side, db, table)
        select = select or ", ".join(wide.get(n, self._q(n)) for n in names)
        sql = (f"select {select}"
               f" from {self._qualified(side, db, table)}"
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

    def _block_rows(self, side, db, table, most):
        """Rows a block of a read of this table holds at most: `most`, or
        as many as `pieces.BATCH_BYTES` holds at the table's widest row
        (`pieces.widest`) - the driver decodes a block whole."""
        return pieces.rows_for(pieces.widest(self, side, db, table),
                               max(int(most or 1000), 1))

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """In key order, in blocks sized by the table's widest row, ending
        at `limit` rows or at the row that brings the read to
        `pieces.BATCH_BYTES`: the next read goes on from its last row."""
        sql, params, key, names = self._read_query(side, db, table, columns,
                                                   after, limit, where)
        keyed = bool(key) and set(key) <= set(names)
        raw, held = [], 0
        client = self._client(side, db)
        try:
            with client.query_row_block_stream(
                    sql, parameters=params, query_formats=self.AS_BYTES,
                    settings={"max_block_size": self._block_rows(
                        side, db, table, limit)}) as stream:
                for block in stream:
                    for r in block:
                        raw.append(r)
                        held += pieces.row_bytes(r)
                        if keyed and held >= pieces.BATCH_BYTES:
                            break
                    if keyed and held >= pieces.BATCH_BYTES:
                        break
        finally:
            client.close()
        rows = self._values(columns, raw, self._wide_times(side, db, table))
        if not rows or not keyed:
            return rows, None
        return rows, tuple(rows[-1][names.index(k)] for k in key)

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        """The whole table in one pass, in batches of `size` rows or
        `pieces.BATCH_BYTES`; inside a digest folded here, each value wider
        than `canon.WIDE` read as its length and md5 (`pieces.hashing`)."""
        wide = self._wide_times(side, db, table)
        plan = (pieces.DigestPlan(self, side, db, table, columns, wide)
                if pieces.is_hashing(self) else None)
        sql, params, _, _ = self._read_query(
            side, db, table, columns, None, None, where,
            select=plan.select if plan is not None else None)
        client = self._client(side, db)
        try:
            with client.query_row_block_stream(
                    sql, parameters=params, query_formats=self.AS_BYTES,
                    settings={"max_block_size": self._block_rows(
                        side, db, table, size)}) as stream:
                for block in stream:
                    rows = plan.finish(block) if plan is not None else block
                    for batch in pieces.by_bytes(rows, most_rows=int(size)):
                        yield self._values(columns, batch, wide)
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
        wide = self._wide_times(side, db, table)
        client = self._client(side, db)
        try:
            rows = self._values(columns, client.query(
                f"select {', '.join(wide.get(n, self._q(n)) for n, _ in columns)}"
                f" from {self._qualified(side, db, table)} where {cond}",
                parameters=params, query_formats=self.AS_BYTES).result_rows,
                wide)
        finally:
            client.close()
        return self._by_key_map(columns, key, rows)

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded here, each value wider than `canon.WIDE` hashed where it
        lies (`LARGE_VALUES`): its length and md5 cross, not the value."""
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        with pieces.hashing(self):
            for batch in self.neutral_batches(side, db, table, columns,
                                              10000, where):
                k, total = canon.fold_rows(classes, batch, total)
                n += k
        return (n, str(total))

    def row_sizes(self, side, db, tables=None):
        """Measured by the server (`pieces.sql_row_sizes`): each row's
        String values by their bytes, only the numbers crossing."""
        return pieces.sql_row_sizes(self, side, db, tables)

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
        """A MergeTree ordered by the key. A column takes NULL unless it is
        in the key or the source says it does not (`rule["null"]`); one
        the source gives a default is given it. The rule is the source's:
        a column left Nullable that the source declares NOT NULL would
        take the NULL it never held."""
        from .. import canon
        defs = []
        for col in columns:
            t = canon.ddl_type("clickhouse", col[1], col[2])
            rule = (col[3] if len(col) > 3 else None) or {}
            # an Array cannot be Nullable: its NULLs are counted before the
            # move (`canon.unfit`), as an empty array is what it writes
            keeps = (col[0] in key or t.startswith("Array(")
                     or rule.get("null") is False)
            default = rule.get("default")
            defs.append(f"{self._q(col[0])} " + (t if keeps
                                                 else f"Nullable({t})")
                        + (f" default {default}" if default is not None
                           and not rule.get("identity") else ""))
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

    #: keys one statement names: each is a parameter in the request's
    #: address, and measured, a batch of the table copier's 50,000 passed
    #: the server's limit on it - the write stopped on `HTTP status 400`
    KEYS_A_STATEMENT = 1000

    def neutral_write(self, side, db, table, columns, rows):
        """The keys of the batch are deleted first, then the batch
        inserted: a MergeTree keeps a second row with the same key, so a
        batch written again after a restart would double without it. The
        keys are asked `KEYS_A_STATEMENT` at a time, and deleted only where
        the target holds one of them - a delete is a mutation of every
        part, and a copy into an empty table has nothing to delete."""
        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        client = self._client(side, db)
        try:
            # the key's values as they are, for the delete; the rows as
            # they are written, for the insert
            written = self._wall_clock(client, self._ticks(
                client, side, db, table, names, rows))
            rows = self._wall_clock(client, rows)
            at = [names.index(k) for k in key]
            q = self._qualified(side, db, table)
            for start in range(0, len(rows) if key else 0,
                               self.KEYS_A_STATEMENT):
                params, tuples = {}, []
                for i, r in enumerate(rows[start:start
                                           + self.KEYS_A_STATEMENT]):
                    places = []
                    for j, idx in enumerate(at):
                        v = canon.write_value("clickhouse",
                                              columns[idx][1], r[idx])
                        params[f"v{i}_{j}"] = v
                        places.append(f"{{v{i}_{j}:{self._param_type(v)}}}")
                    tuples.append("(" + ", ".join(places) + ")")
                cond = (f"({', '.join(self._q(k) for k in key)}) in"
                        f" ({', '.join(tuples)})")
                if client.query(f"select 1 from {q} where {cond} limit 1",
                                parameters=params).result_rows:
                    client.command(f"delete from {q} where {cond}",
                                   parameters=params,
                                   settings={"lightweight_deletes_sync": 2})
            client.insert(table,
                          [[canon.write_value("clickhouse", c, v)
                            for (_, c), v in zip(columns, r)]
                           for r in written],
                          column_names=names, database=self._d(side, db))
        finally:
            client.close()
        return len(rows)

    def _ticks(self, client, side, db, table, names, rows):
        """A time going into a `DateTime64` of more than six digits as the
        count of its ticks, every digit it carries in it: the driver
        writes a datetime at six (`nanotime`). Wall clock in the column's
        zone, or the server's, as `_wall_clock` hands the rest over."""
        import datetime
        from zoneinfo import ZoneInfo

        from .. import nanotime
        wide = {n: self._digits(t) for n, t in
                self.neutral_columns(side, db, table)
                if (self._digits(t) or (0,))[0] > 6}
        at = [(i, wide[n]) for i, n in enumerate(names) if n in wide]
        if not at:
            return rows
        server = ZoneInfo(client.command("select timezone()"))
        out = []
        for r in rows:
            r = list(r)
            for i, (digits, zone) in at:
                if isinstance(r[i], datetime.datetime):
                    r[i] = nanotime.epoch_ticks(
                        r[i], digits, ZoneInfo(zone) if zone else server)
            out.append(r)
        return out

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

    def _apply_delete(self, side, db, table, key):
        """Remove the row at that key, whether or not it is there: what a
        repair deletes, and what putting a repaired row back deletes. A
        lightweight delete the statement waits for
        (`lightweight_deletes_sync`), addressed as `neutral_rows_by_key`
        addresses a row - by the same typed values - and never without a
        key: a delete with nothing to match would empty the table."""
        from .. import canon
        self._target_only(side, "delete a row")
        if not key:
            raise SystemExit(f"{table}: a row is deleted by its key, and"
                             " none was given")
        params, tests = {}, []
        for i, (name, value) in enumerate(key.items()):
            params[f"k{i}"] = canon.sql_value(value)
            tests.append(f"{self._q(name)} = "
                         f"{{k{i}:{self._param_type(value)}}}")
        client = self._client(side, db)
        try:
            client.command(f"delete from {self._qualified(side, db, table)}"
                           f" where {' and '.join(tests)}",
                           parameters=params,
                           settings={"lightweight_deletes_sync": 2})
        finally:
            client.close()

    def repair_plan(self, db, kind):
        """The rows the last check found apart, carried across by the pair
        every engine's rows are repaired by: the source's rows written
        again and the target's own removed, each target row this touches
        recorded first, so it can be put back (`repairlog`). Only the
        tables the hop takes (`Engine._drills_in_scope`)."""
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    def run_rule(self, side, db, sql):
        return self._rows(side, db, sql, settings={"readonly": 1})

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def _mutations(self, db):
        """Mutations - the deletes a restart runs - that have not
        finished, on each side: the rows a check reads are not settled
        until they have."""
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
            # a table the hop leaves out is not the move's to settle
            stuck = self._scoped_rows(db, stuck)
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

    # ---- the deep checks that hold both catalogues to each other ---------

    #: what a table of each of these engines is: an object whose rows come
    #: from a query or from a source of its own, not rows it holds
    OBJECT_ENGINES = {"View": "view", "MaterializedView": "matview",
                      "LiveView": "view", "WindowView": "view",
                      "Dictionary": "dictionary"}
    #: a column ClickHouse computes rather than stores (or reads from the
    #: insert and never keeps)
    GENERATED = ("MATERIALIZED", "ALIAS", "EPHEMERAL")
    #: tables that read a stream, and write what they read through a view
    QUEUE_ENGINES = ("Kafka", "RabbitMQ", "NATS", "S3Queue", "AzureQueue",
                     "FileLog")
    #: text columns whose bytes the charset check reads, and temporal
    #: columns the time check samples - each read is a pass over a column
    TEXT_COLUMNS, TIME_COLUMNS, TIME_ROWS = 30, 10, 200

    @staticmethod
    def _plain(text):
        """A definition without the white space it is laid out with."""
        return " ".join(str(text or "").split())

    def _written(self, db, name, lines):
        """Every line of a finding, in a file beside the other deep
        findings; its path."""
        path = self.hop.report_dir(db) / f"deep-{name}.diff"
        path.write_text("\n".join(lines) + "\n")
        return str(path)

    def _catalogue(self, side, db):
        """{name: facts} of every table, view and dictionary of the
        database that the hop takes, from the catalogue in three reads:
        what each is made of, its columns and its data-skipping indexes."""
        ask = {"d": self._d(side, db)}
        cat = {}
        for (name, engine, part, order, prim, full, policy, create,
             query) in self._rows(
                side, db, "select name, engine, partition_key, sorting_key,"
                          " primary_key, engine_full, storage_policy,"
                          " create_table_query, as_select from system.tables"
                          " where database = {d:String} and not"
                          " is_temporary and not startsWith(name, '.inner')",
                ask):
            if self.in_scope(db, name):
                cat[str(name)] = {
                    "engine": str(engine), "partition": str(part or ""),
                    "sorting": str(order or ""), "primary": str(prim or ""),
                    "full": str(full or ""), "policy": str(policy or ""),
                    "create": str(create or ""), "query": str(query or ""),
                    "object": self.OBJECT_ENGINES.get(str(engine), ""),
                    "columns": {}, "indexes": {}}
        for table, name, typ, kind, expr, codec in self._rows(
                side, db, "select table, name, type, default_kind,"
                          " default_expression, compression_codec from"
                          " system.columns where database = {d:String}"
                          " order by table, position", ask):
            if str(table) in cat:
                cat[str(table)]["columns"][str(name)] = (
                    str(typ), str(kind or ""), str(expr or ""),
                    str(codec or ""))
        for table, name, kind, expr, grain in self._rows(
                side, db, "select table, name, type, expr, granularity from"
                          " system.data_skipping_indices where database ="
                          " {d:String}", ask):
            if str(table) in cat:
                cat[str(table)]["indexes"][str(name)] = (
                    str(kind), self._plain(expr), int(grain or 0))
        return cat

    def _stored(self, src, dst):
        """The tables (not views or dictionaries) both sides hold, by
        name."""
        return [t for t in sorted(set(src) & set(dst))
                if not src[t]["object"] and not dst[t]["object"]]

    def _both_columns(self, src, dst):
        """(table, column, the source's facts, the target's) of every
        column of a stored table that both sides hold."""
        for t in self._stored(src, dst):
            a, b = src[t]["columns"], dst[t]["columns"]
            for c in sorted(set(a) & set(b)):
                yield t, c, a[c], b[c]

    def check_deep(self, db):
        """What the two servers' catalogues say of the database held
        against each other - its columns, keys, partitions, indexes,
        views, row policies, sharding and grants - the mutations that have
        not finished, and, where a catalogue cannot show it, the values
        themselves: their text, their times, their NULLs. Only the tables
        and objects the hop takes."""
        out = self._mutations(db)
        try:
            src, dst = self._catalogue("src", db), self._catalogue("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return out + [Result("deep", f"{db} objects", "error",
                                 "could not read the catalogues:"
                                 f" {str(e).splitlines()[0][:90]}")]
        for word, check in (
                ("columns", self._deep_columns),
                ("generated", self._deep_generated),
                ("narrowing", self._deep_narrowing),
                ("float", self._deep_float),
                ("nullempty", self._deep_nullempty),
                ("keys", self._deep_keys),
                ("partitions", self._deep_partitions),
                ("indexes", self._deep_indexes),
                ("objects", self._deep_objects),
                ("matviews", self._deep_matviews),
                ("ownership", self._deep_ownership),
                ("rls", self._deep_rls),
                ("sharding", self._deep_sharding),
                ("table options", self._deep_options),
                ("boundary", self._deep_boundary),
                ("charset", self._deep_charset),
                ("render", self._deep_render),
                ("timeshift", self._deep_timeshift)):
            try:
                out += check(db, src, dst)
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(Result(
                    "deep", f"{db} {word}", "error",
                    f"could not be read: {str(e).splitlines()[0][:90]}"
                    " - unknown, not clean"))
        out += self._deep_grants(db)
        return out

    # the catalogue checks, one finding each, its scope ending in the word
    # a category is read from (`base.CATEGORIES`)

    def _deep_columns(self, db, src, dst):
        drift = []
        for t in self._stored(src, dst):
            a, b = src[t]["columns"], dst[t]["columns"]
            drift += [f"{t}.{c}: on the source only" for c in sorted(set(a)
                                                                    - set(b))]
            drift += [f"{t}.{c}: on the target only" for c in sorted(set(b)
                                                                    - set(a))]
        n = 0
        for t, c, (ta, ka, ea, ca), (tb, kb, eb, cb) in self._both_columns(
                src, dst):
            n += 1
            if ka in self.GENERATED or kb in self.GENERATED:
                continue
            if _unwrap(ta) != _unwrap(tb):
                drift.append(f"{t}.{c}: {ta} on the source, {tb} on the"
                             " target")
            elif ka != kb or self._plain(ea) != self._plain(eb):
                drift.append(f"{t}.{c}: default {self._plain(ea) or 'none'}"
                             f" on the source, {self._plain(eb) or 'none'}"
                             " on the target")
            elif ca != cb:
                drift.append(f"{t}.{c}: compressed with {ca or 'the default'}"
                             f" on the source and {cb or 'the default'} on"
                             " the target")
        if not drift:
            return [Result("deep", f"{db} columns", "ok",
                           f"{n} columns compared, type, default and codec"
                           " identical")]
        return [Result("deep", f"{db} columns", "diff",
                       f"{len(drift)} columns drift: " + clauses(drift, 4),
                       self._written(db, "columns", drift),
                       "align the target's columns with the source's before"
                       " trusting a row verdict")]

    def _deep_generated(self, db, src, dst):
        drift, n = [], 0
        for t in self._stored(src, dst):
            a, b = src[t]["columns"], dst[t]["columns"]
            for c in sorted(set(a) | set(b)):
                ka, ea = a.get(c, ("", "", "", ""))[1:3]
                kb, eb = b.get(c, ("", "", "", ""))[1:3]
                if ka not in self.GENERATED and kb not in self.GENERATED:
                    continue
                n += 1
                if (ka, self._plain(ea)) != (kb, self._plain(eb)):
                    drift.append(
                        f"{t}.{c}: " + (f"{ka} {self._plain(ea)}" if ka in
                                        self.GENERATED else "stored")
                        + " on the source, "
                        + (f"{kb} {self._plain(eb)}" if kb in self.GENERATED
                           else "stored" if c in b else "absent")
                        + " on the target")
        if not drift:
            return [Result("deep", f"{db} generated", "ok",
                           f"{n} computed columns, defined alike on both"
                           " sides" if n else "no computed column")]
        return [Result("deep", f"{db} generated", "diff",
                       f"{len(drift)} computed columns differ: "
                       + clauses(drift, 4),
                       self._written(db, "generated", drift),
                       "a column the source computes is not written, and"
                       " must be computed by the same definition on the"
                       " target")]

    def _deep_narrowing(self, db, src, dst):
        from .. import verdict
        narrow = []
        for t, c, (ta, *_), (tb, *_) in self._both_columns(src, dst):
            why = verdict.narrowing("clickhouse", _unwrap(ta), _unwrap(tb))
            if why:
                narrow.append(f"{t}.{c}: {why}")
        return [verdict.narrowing_result(db, narrow)]

    _DECIMAL = re.compile(r"^Decimal(?:(32|64|128|256))?\(\s*(\d+)"
                          r"(?:\s*,\s*(\d+))?\s*\)$")
    _DECIMAL_DIGITS = {"32": 9, "64": 18, "128": 38, "256": 76}

    @classmethod
    def _decimal(cls, declared):
        """(digits, scale) of a Decimal column, or None."""
        m = cls._DECIMAL.match(str(declared).strip())
        if not m:
            return None
        if m.group(1):
            return cls._DECIMAL_DIGITS[m.group(1)], int(m.group(2))
        return int(m.group(2)), int(m.group(3) or 0)

    def _deep_float(self, db, src, dst):
        """A column that holds fewer digits than the source's: a float of
        fewer bits, a time of coarser ticks, a decimal of a smaller scale."""
        less = []
        for t, c, (ta, *_), (tb, *_) in self._both_columns(src, dst):
            a, b = _unwrap(ta), _unwrap(tb)
            wa, wb = self._digits(a), self._digits(b)
            da, dd = self._decimal(a), self._decimal(b)
            if a.startswith("Float64") and b.startswith("Float32"):
                less.append(f"{t}.{c}: Float64 -> Float32 (about 7 digits"
                            " kept of 15)")
            elif wa and wb and wa[0] > wb[0]:
                less.append(f"{t}.{c}: {a} -> {b} (ticks of 10^-{wa[0]} s"
                            f" rounded to 10^-{wb[0]} s)")
            elif wa and b.startswith("DateTime") and not wb:
                less.append(f"{t}.{c}: {a} -> {b} (the fraction of a second"
                            " is dropped)")
            elif da and dd and dd[1] < da[1]:
                less.append(f"{t}.{c}: {a} -> {b} (scale {da[1]} ->"
                            f" {dd[1]}: rounds)")
        if less:
            return [Result("deep", f"{db} float", "diff",
                           f"{len(less)} target columns hold fewer digits"
                           " than the source's: " + clauses(less, 4),
                           self._written(db, "float", less),
                           "widen the target column to the source's"
                           " precision before loading")]
        return [Result("deep", f"{db} float", "ok",
                       "no target column holds fewer digits than the"
                       " source's")]

    def _deep_nullempty(self, db, src, dst):
        """A column that takes NULL on the source and not on the target
        stores the type's zero or '' for the NULL it is given
        (`input_format_null_as_default`): counted on the source."""
        risky = [(t, c) for t, c, (ta, *_), (tb, *_) in
                 self._both_columns(src, dst)
                 if _nullable(ta) and not _nullable(tb)]
        if not risky:
            return [Result("deep", f"{db} nullempty", "ok",
                           "every column that takes NULL on the source"
                           " takes it on the target")]
        held, unread = [], []
        for t, c in risky[:20]:
            try:
                n = int(self._rows(
                    "src", db, f"select countIf(isNull({self._q(c)})) from"
                               f" {self._qualified('src', db, t)}")[0][0])
            except Exception:  # noqa: BLE001 - one column fewer counted
                unread.append(f"{t}.{c}")
                continue
            if n:
                held.append(f"{t}.{c}: {n:,} rows hold NULL")
        left = [f"{t}.{c}" for t, c in risky]
        if held:
            return [Result("deep", f"{db} nullempty", "diff",
                           "NULL on the source arrives as 0 or '' on the"
                           f" target - {clauses(held, 4)}", "",
                           "make the target column Nullable, or decide what"
                           " those rows hold, before the move")]
        return [Result("deep", f"{db} nullempty", "warn",
                       f"{len(left)} columns take NULL on the source and not"
                       " on the target ("
                       + ", ".join(left[:4]) + ")"
                       + (f"; {len(unread)} could not be counted"
                          if unread else "; none holds one now")
                       + " - a NULL written later arrives as 0 or ''", "",
                       "make the target column Nullable")]

    def _deep_keys(self, db, src, dst):
        keyless, drift = [], []
        for t in sorted(src):
            s = src[t]
            if s["object"] or not s["engine"].endswith("MergeTree"):
                continue
            if not s["sorting"].strip():
                keyless.append(t)
            d = dst.get(t)
            if d and not d["object"] and d["engine"].endswith("MergeTree") \
                    and (self._plain(s["sorting"]) != self._plain(
                        d["sorting"]) or self._plain(s["primary"])
                         != self._plain(d["primary"])):
                drift.append(f"{t}: ordered by ({s['sorting'] or 'nothing'})"
                             f" on the source, ({d['sorting'] or 'nothing'})"
                             " on the target")
        if drift:
            return [Result("deep", f"{db} keys", "diff",
                           f"{len(drift)} tables are ordered by another key"
                           " on the target: " + clauses(drift, 4),
                           self._written(db, "keys", drift),
                           "give the target table the source's ORDER BY"
                           " and PRIMARY KEY - rows are found, merged and"
                           " repaired by them")]
        if keyless:
            return [Result("deep", f"{db} keys", "warn",
                           f"{len(keyless)} tables have no sorting key"
                           f" ({', '.join(keyless[:5])}): they are copied"
                           " and checked by whole-row hashes, and a row"
                           " cannot be named to be repaired", "",
                           "order the table by a column that identifies a"
                           " row")]
        return [Result("deep", f"{db} keys", "ok",
                       "every MergeTree table has a sorting key, the same on"
                       " both sides")]

    def _partition_rows(self, side, db):
        """{table: {partition id: rows}} of the active parts, for the
        tables the hop takes."""
        out = {}
        for t, p, n in self._rows(
                side, db, "select table, partition_id, sum(rows) from"
                          " system.parts where database = {d:String} and"
                          " active group by table, partition_id",
                {"d": self._d(side, db)}):
            if self.in_scope(db, str(t)):
                out.setdefault(str(t), {})[str(p)] = int(n)
        return out

    def _deep_partitions(self, db, src, dst):
        names = [t for t in self._stored(src, dst)
                 if src[t]["partition"] or dst[t]["partition"]]
        if not names:
            return [self._partitions_verdict(db, {}, [], "catch-all")]
        held = {side: self._partition_rows(side, db)
                for side in ("src", "dst")}
        scheme, tables = [], {}
        for t in names:
            a, b = src[t]["partition"], dst[t]["partition"]
            if self._plain(a) != self._plain(b):
                scheme.append(f"{t}: partitioned by {a or 'nothing'} on the"
                              f" source and {b or 'nothing'} on the target")
                continue
            tables[t] = tuple(
                {p: {"rows": n, "digest": None}
                 for p, n in held[side].get(t, {}).items()}
                for side in ("src", "dst"))
        return [self._partitions_verdict(db, tables, scheme, "catch-all")]

    def _deep_indexes(self, db, src, dst):
        drift, extra, n = [], 0, 0
        for t in self._stored(src, dst):
            a, b = src[t]["indexes"], dst[t]["indexes"]
            extra += len(set(b) - set(a))
            for name in sorted(a):
                n += 1
                if name not in b:
                    drift.append(f"{t}.{name}: not on the target")
                elif a[name] != b[name]:
                    drift.append(f"{t}.{name}: {a[name][0]} on {a[name][1]}"
                                 f" (granularity {a[name][2]}) on the source,"
                                 f" {b[name][0]} on {b[name][1]}"
                                 f" (granularity {b[name][2]}) on the target")
        if drift:
            return [Result("deep", f"{db} indexes", "diff",
                           f"{len(drift)} data-skipping indexes of {n} are"
                           " missing or differ on the target: "
                           + clauses(drift, 4),
                           self._written(db, "indexes", drift),
                           "add the index to the target and materialize it:"
                           " until then every read of the table scans it"
                           " whole")]
        return [Result("deep", f"{db} indexes", "ok",
                       f"{n} data-skipping indexes, the same on both sides"
                       + (f"; {extra} more on the target" if extra else ""))]

    def _functions(self, side):
        """{name: definition} of the SQL functions its users wrote."""
        return {str(n): self._plain(q) for n, q in self._rows(
            side, None, "select name, create_query from system.functions"
                        " where origin = 'SQLUserDefined'")}

    def _deep_objects(self, db, src, dst):
        """Views and dictionaries, and the functions users wrote: each one
        the hop takes (`takes`) is there on the target, and a view is
        defined by the same query."""
        lines, n = [], 0
        for kind in ("view", "dictionary"):
            held = [{t for t, c in cat.items() if c["object"] == kind
                     and self.takes(db, kind, t)} for cat in (src, dst)]
            n += len(held[0])
            lines += [f"{kind} {t}: not on the target"
                      for t in sorted(held[0] - held[1])]
            lines += [f"{kind} {t}: on the target only"
                      for t in sorted(held[1] - held[0])]
            if kind == "view":
                lines += [f"view {t}: defined by another query on the target"
                          for t in sorted(held[0] & held[1])
                          if self._plain(src[t]["query"])
                          != self._plain(dst[t]["query"])]
        a, b = self._functions("src"), self._functions("dst")
        mine = [f for f in a if self.takes(db, "function", f)]
        n += len(mine)
        lines += [f"function {f}: not on the target" for f in sorted(mine)
                  if f not in b]
        lines += [f"function {f}: defined another way on the target"
                  for f in sorted(mine) if f in b and a[f] != b[f]]
        if lines:
            return [Result("deep", f"{db} objects", "diff",
                           f"{len(lines)} of {n} views, dictionaries and"
                           " functions are missing or differ on the target: "
                           + clauses(lines, 4),
                           self._written(db, "objects", lines),
                           "create them on the target - a data move carries"
                           " none")]
        return [Result("deep", f"{db} objects", "ok",
                       f"{n} views, dictionaries and functions, each on the"
                       " target and defined alike")]

    def _deep_matviews(self, db, src, dst):
        """Materialized views: each is on the target, over the same query
        and into the same table. A view fires only for the rows inserted
        after it exists, so one made late leaves what came before out."""
        lines = []
        held = [{t for t, c in cat.items() if c["object"] == "matview"
                 and self.takes(db, "matview", t)} for cat in (src, dst)]
        lines += [f"{t}: not on the target" for t in sorted(held[0]
                                                           - held[1])]
        lines += [f"{t}: on the target only" for t in sorted(held[1]
                                                            - held[0])]
        for t in sorted(held[0] & held[1]):
            if self._plain(src[t]["query"]) != self._plain(dst[t]["query"]):
                lines.append(f"{t}: defined by another query on the target")
            into = [self._into(cat[t]["create"]) for cat in (src, dst)]
            if into[0] and into[1] and into[0] != into[1]:
                lines.append(f"{t}: writes into {into[0]} on the source and"
                             f" {into[1]} on the target")
        if lines:
            return [Result("deep", f"{db} matviews", "diff",
                           f"{len(lines)} materialized views are missing or"
                           " differ on the target: " + clauses(lines, 4),
                           self._written(db, "matviews", lines),
                           "create them on the target after its tables hold"
                           " the rows - a view only sees rows inserted once"
                           " it exists")]
        return [Result("deep", f"{db} matviews", "ok",
                       f"{len(held[0])} materialized views, each on the"
                       " target over the same query")]

    @staticmethod
    def _into(create):
        """The table a materialized view writes into, as its statement
        names it (`TO table`), without its database - or ''."""
        m = re.search(r"\bTO\s+((?:`[^`]+`|[\w$]+)(?:\.(?:`[^`]+`|[\w$]+))?)",
                      str(create))
        return m.group(1).split(".")[-1].strip("`") if m else ""

    _DEFINER = re.compile(r"DEFINER\s*=\s*(\S+)", re.I)
    _SECURITY = re.compile(r"SQL\s+SECURITY\s+(\w+)", re.I)

    def _definition_owner(self, create):
        """`definer/security` of a view as its statement says it, or ''."""
        a, b = self._DEFINER.search(create), self._SECURITY.search(create)
        return "/".join(m.group(1) for m in (a, b) if m)

    def _deep_ownership(self, db, src, dst):
        """The account a view runs with, which a definition compared as
        text does not show."""
        from .. import ownership
        rows = [(f"{src[t]['object']} {t}",
                 self._definition_owner(src[t]["create"]),
                 self._definition_owner(dst[t]["create"]))
                for t in sorted(set(src) & set(dst))
                if src[t]["object"] in ("view", "matview")
                and dst[t]["object"] in ("view", "matview")]
        if not rows:
            return [Result("deep", f"{db} ownership", "ok",
                           "no view or materialized view on both sides")]
        changes = ownership.group(rows)
        if not changes:
            return [Result("deep", f"{db} ownership", "ok",
                           f"{len(rows)} views keep their definer and"
                           " security mode")]
        return [Result("deep", f"{db} ownership", "diff",
                       f"{ownership.total(changes)} of {len(rows)}: "
                       + ownership.describe(changes), "",
                       "recreate them with the intended DEFINER and SQL"
                       " SECURITY before the application depends on them")]

    def _deep_rls(self, db, src, dst):
        def policies(side):
            return {(str(t), str(n)): (self._plain(f), bool(r), bool(all_),
                                       tuple(sorted(a or [])),
                                       tuple(sorted(x or [])))
                    for t, n, f, r, all_, a, x in self._rows(
                        side, db, "select table, short_name, select_filter,"
                                  " is_restrictive, apply_to_all,"
                                  " apply_to_list, apply_to_except from"
                                  " system.row_policies where database ="
                                  " {d:String}", {"d": self._d(side, db)})
                    if self.in_scope(db, str(t))}
        a, b = policies("src"), policies("dst")
        lines = [f"{t}: policy {n} is not on the target (its rows are open"
                 " to everyone the grants allow)" for t, n in sorted(a)
                 if (t, n) not in b]
        lines += [f"{t}: policy {n} filters another way on the target"
                  for t, n in sorted(a) if (t, n) in b and a[(t, n)]
                  != b[(t, n)]]
        lines += [f"{t}: policy {n} is on the target only"
                  for t, n in sorted(b) if (t, n) not in a]
        if lines:
            return [Result("deep", f"{db} rls", "diff",
                           f"{len(lines)} row policies differ: "
                           + clauses(lines, 4), self._written(db, "rls",
                                                              lines),
                           "create the policies on the target before anyone"
                           " but the move reads it")]
        return [Result("deep", f"{db} rls", "ok",
                       f"{len(a)} row policies, the same on both sides"
                       if a else "no row policy")]

    @staticmethod
    def _engine_args(full, engine):
        """The arguments of `engine(...)` in an engine definition, split
        at the commas outside quotes and brackets, each stripped of its
        quotes; [] where the engine is not named."""
        at = str(full).find(engine + "(")
        if at < 0:
            return []
        out, depth, quote, cur = [], 0, "", ""
        for ch in str(full)[at + len(engine) + 1:]:
            if quote:
                cur += ch
                quote = "" if ch == quote else quote
            elif ch in "'`\"":
                cur, quote = cur + ch, ch
            elif ch in "([":
                depth, cur = depth + 1, cur + ch
            elif ch in ")]" and depth:
                depth, cur = depth - 1, cur + ch
            elif ch == ")":
                break
            elif ch == "," and not depth:
                out.append(cur.strip())
                cur = ""
            else:
                cur += ch
        out.append(cur.strip())
        return [a.strip("'`\"") if a[:1] in "'`\"" else a for a in out]

    def _deep_sharding(self, db, src, dst):
        """Distributed tables: each is on the target, over a cluster the
        target defines, sharded by the same key."""
        def ways(cat):
            out = {}
            for t, c in cat.items():
                if c["engine"] == "Distributed":
                    args = self._engine_args(c["full"], "Distributed")
                    out[t] = (args[0], self._plain(args[3])) \
                        if len(args) > 3 else (args[0] if args else "?", "")
            return out
        a, b = ways(src), ways(dst)
        lines = [f"{t}: not on the target" for t in sorted(a) if t not in b]
        for t in sorted(a):
            if t in b and a[t] != b[t]:
                lines.append(f"{t}: cluster {a[t][0]} sharded by"
                             f" {a[t][1] or 'nothing'} on the source,"
                             f" cluster {b[t][0]} sharded by"
                             f" {b[t][1] or 'nothing'} on the target")
        for cluster in sorted({c for c, _ in b.values() if c != "?"}):
            if not int(self._rows(
                    "dst", db, "select count() from system.clusters where"
                               " cluster = {c:String}",
                    {"c": cluster})[0][0]):
                lines.append(f"cluster {cluster} is not defined on the"
                             " target, so the tables over it read nothing")
        if lines:
            return [Result("deep", f"{db} sharding", "diff",
                           f"{len(lines)} Distributed tables differ: "
                           + clauses(lines, 4),
                           self._written(db, "sharding", lines),
                           "define the cluster in the target's configuration"
                           " and create the tables over it")]
        return [Result("deep", f"{db} sharding", "ok",
                       f"{len(a)} Distributed tables, the same cluster and"
                       " sharding key on both sides" if a
                       else "no Distributed table")]

    def _options(self, facts):
        """(engine, TTL, SETTINGS, storage policy) of a table as its
        definition says them."""
        full = facts["full"]
        ttl = re.search(r"\bTTL\b(.*?)(?=\bSETTINGS\b|\bCOMMENT\b|$)", full,
                        re.S)
        settings = re.search(r"\bSETTINGS\b(.*?)(?=\bCOMMENT\b|$)", full,
                             re.S)
        return (facts["engine"], self._plain(ttl.group(1)) if ttl else "",
                self._plain(settings.group(1)) if settings else "",
                facts["policy"])

    def _deep_options(self, db, src, dst):
        """A table's engine, its time to live (which deletes rows), its
        storage policy and its settings."""
        diff, soft = [], []
        for t in self._stored(src, dst):
            (ea, ta, sa, pa), (eb, tb, sb, pb) = (self._options(src[t]),
                                                  self._options(dst[t]))
            if ea != eb:
                diff.append(f"{t}: engine {ea} on the source, {eb} on the"
                            " target")
            if ta != tb:
                diff.append(f"{t}: TTL {ta or 'none'} on the source,"
                            f" {tb or 'none'} on the target")
            if pa != pb:
                soft.append(f"{t}: storage policy {pa} -> {pb}")
            if sa != sb:
                soft.append(f"{t}: settings {sa or 'none'} -> {sb or 'none'}")
        if diff:
            return [Result("deep", f"{db} table options", "diff",
                           f"{len(diff)} options differ: "
                           + clauses(diff + soft, 4),
                           self._written(db, "options", diff + soft),
                           "give the target table the source's engine and"
                           " TTL: a TTL that is missing keeps rows the"
                           " source deletes, one that is added deletes rows"
                           " the source keeps",
                           category="structure.collection-options")]
        if soft:
            return [Result("deep", f"{db} table options", "warn",
                           f"{len(soft)} settings differ: "
                           + clauses(soft, 4),
                           self._written(db, "options", soft), "",
                           category="structure.collection-options")]
        return [Result("deep", f"{db} table options", "ok",
                       "engine, TTL, storage policy and settings alike on"
                       " both sides",
                       category="structure.collection-options")]

    def _deep_boundary(self, db, src, dst):
        """A target whose largest key runs past the source's: rows written
        on it by something else, or applied twice."""
        keys = []
        for t in self._stored(src, dst):
            key = [k.strip().strip("`") for k in src[t]["sorting"].split(",")
                   if k.strip()]
            col = src[t]["columns"].get(key[0]) if len(key) == 1 else None
            if col and re.match(r"U?Int\d+$", _unwrap(col[0])) \
                    and dst[t]["engine"].endswith("MergeTree"):
                keys.append((t, key[0]))
        if not keys:
            return [Result("deep", f"{db} boundary", "ok",
                           "no table keyed by one integer column to"
                           " boundary-check")]
        ahead, behind = [], 0

        def top(side, t, k):
            return int(self._rows(
                side, db, f"select coalesce(max({self._q(k)}), 0) from"
                          f" {self._qualified(side, db, t)}")[0][0])
        for t, k in keys:
            a, b = top("src", t, k), top("dst", t, k)
            if b > a:
                ahead.append(f"{t} src_max={a} dst_max={b}")
            elif b < a:
                behind += 1
        if ahead:
            return [Result("deep", f"{db} boundary", "diff",
                           f"target max(key) AHEAD of source on"
                           f" {len(ahead)}: {'; '.join(ahead[:4])}", "",
                           "writes landing on the target or applied twice,"
                           " find the writer before cutover")]
        return [Result("deep", f"{db} boundary", "ok",
                       f"max(key) checked on {len(keys)} tables, none ahead"
                       " of the source"
                       + (f"; {behind} behind (still copying)"
                          if behind else ""))]

    def _text_columns(self, src, dst):
        """(table, column) of the String columns both sides hold, in the
        tables' order."""
        return [(t, c) for t, c, (ta, *_), (tb, *_) in
                self._both_columns(src, dst)
                if _unwrap(ta) == "String" and _unwrap(tb) == "String"]

    def _deep_charset(self, db, src, dst):
        """A String is bytes, and meant as UTF-8: bytes that are not, and
        the replacement character a lossy conversion leaves, are counted
        on both sides for the first text columns of the tables the hop
        takes - one pass over each column."""
        cols = self._text_columns(src, dst)
        asked, bad = cols[:self.TEXT_COLUMNS], []
        read = 0
        def counted(side, t, c):
            return self._rows(
                side, db, f"select countIf(not isValidUTF8({self._q(c)})),"
                          f" sum(countSubstrings({self._q(c)}, char(239,"
                          f" 191, 189))) from {self._qualified(side, db, t)}",
                settings={"max_execution_time": 300})[0]
        for t, c in asked:
            try:
                a, b = counted("src", t, c), counted("dst", t, c)
            except Exception:  # noqa: BLE001 - one column fewer read
                continue
            read += 1
            if int(b[0] or 0) > int(a[0] or 0):
                bad.append(f"{t}.{c}: {int(b[0]) - int(a[0]):,} more values"
                           " that are not UTF-8 on the target")
            if int(b[1] or 0) > int(a[1] or 0):
                bad.append(f"{t}.{c}: {int(b[1]) - int(a[1]):,} more U+FFFD"
                           " replacement characters on the target")
        said = (f"{read} of {len(cols)} text columns read"
                if len(cols) > len(asked) or read < len(asked)
                else f"{read} text columns read")
        if bad:
            return [Result("deep", f"{db} charset", "diff",
                           f"{said}: " + clauses(bad, 4), "",
                           "load the affected rows again over a connection"
                           " that sends UTF-8")]
        return [Result("deep", f"{db} charset", "ok",
                       f"{said}: no excess of bytes that are not UTF-8 or of"
                       " replacement characters on the target")]

    #: the types whose text depends on the server that renders them - a
    #: float's digits, a decimal's trailing zeros, a map's or a tuple's
    #: layout, an enum's name, an address, a time's ticks
    EXOTIC = ("Float", "Decimal", "Map", "Tuple", "Nested", "JSON", "Object",
              "Enum", "IPv", "Array", "DateTime64")
    #: columns of those types whose rows the render check samples, and rows
    #: a column
    RENDER_COLUMNS, RENDER_ROWS = 10, 200

    def _deep_render(self, db, src, dst):
        """A value that the two servers render as other text: the first
        rows of the exotic-typed columns, read by key, as each server
        writes them (`toString`). A float, a decimal or a map is text
        whose digits and layout belong to the server's version and its
        settings, and a fingerprint that renders it there cannot tell
        which column it was."""
        cols = [(t, c) for t, c, (ta, *_), (tb, *_) in
                self._both_columns(src, dst)
                if _unwrap(ta).startswith(self.EXOTIC)
                and _unwrap(tb).startswith(self.EXOTIC)]
        drifts, audited = [], 0
        for t, c in cols[:self.RENDER_COLUMNS]:
            key = [k.strip().strip("`") for k in src[t]["sorting"].split(",")
                   if k.strip()]
            if len(key) != 1 or key[0] not in src[t]["columns"]:
                continue

            def rows(side, t=t, c=c, key=key):
                return {str(r[0]): r[1] for r in self._rows(
                    side, db, f"select {self._q(key[0])}, toString("
                              f"{self._q(c)}) from"
                              f" {self._qualified(side, db, t)} where"
                              f" {self._q(c)} is not null order by"
                              f" {self._q(key[0])} limit {self.RENDER_ROWS}")}
            a, b = rows("src"), rows("dst")
            audited += 1
            for k in a:
                if k in b and a[k] != b[k]:
                    drifts.append(f"{t}.{c}: renders differently (source"
                                  f" {a[k][:40]!r}, target {b[k][:40]!r})")
                    break
        if drifts:
            return [Result("deep", f"{db} render", "diff",
                           f"{len(drifts)} columns: " + clauses(drifts, 4),
                           self._written(db, "render", drifts),
                           "compare the two servers' versions and their"
                           " output settings (`check params`)")]
        return [Result("deep", f"{db} render", "ok",
                       f"{audited} exotic-typed columns sampled, rendered"
                       " alike" if audited else "no exotic-typed column")]

    def _epoch_seconds(self, column, declared):
        """A time column as whole seconds since the epoch, whatever its
        ticks: the instant, not the wall clock it is shown as."""
        if _unwrap(declared).startswith("DateTime64"):
            return f"intDiv(toUnixTimestamp64Milli({self._q(column)}), 1000)"
        return f"toUnixTimestamp({self._q(column)})"

    def _deep_timeshift(self, db, src, dst):
        """A time column every row of which the target holds shifted by a
        zone's offset: the first rows of each, read by key, as epoch
        seconds, which a faithful copy holds equal."""
        from .. import verdict
        cols = [(t, c) for t, c, (ta, *_), (tb, *_) in
                self._both_columns(src, dst)
                if _unwrap(ta).startswith("DateTime")
                and _unwrap(tb).startswith("DateTime")]
        shifts = []
        for t, c in cols[:self.TIME_COLUMNS]:
            key = [k.strip().strip("`") for k in src[t]["sorting"].split(",")
                   if k.strip()]
            if len(key) != 1 or key[0] not in src[t]["columns"]:
                continue

            def rows(side, cat, t=t, c=c, key=key):
                at = self._epoch_seconds(c, cat[t]["columns"][c][0])
                return {str(r[0]): r[1] for r in self._rows(
                    side, db, f"select {self._q(key[0])}, {at}"
                              f" from {self._qualified(side, db, t)} where"
                              f" {self._q(c)} is not null order by"
                              f" {self._q(key[0])} limit {self.TIME_ROWS}")
                        if r[1] is not None}
            line = verdict.uniform_shift(f"{t}.{c}", rows("src", src),
                                         rows("dst", dst))
            if line:
                shifts.append(line)
        return [verdict.timeshift_result(db, shifts)]

    def _grants(self, side, db):
        """{grantee: {(privilege, table or '*', column, partial revoke,
        grant option)}} that reach the database's tables the hop takes:
        on the database, on a table of it, or on the whole server (`*.*`),
        from the server's grants."""
        out = {}
        for user, role, access, base, table, column, partial, option in \
                self._rows(
                    side, db, "select user_name, role_name, access_type,"
                              " database, table, column, is_partial_revoke,"
                              " grant_option from system.grants where"
                              " database = {d:String} or isNull(database)",
                    {"d": self._d(side, db)}):
            if table is not None and not self.in_scope(db, str(table)):
                continue
            who = f"user {user}" if user else f"role {role}"
            out.setdefault(who, set()).add(
                (str(access), str(table or "*") if base is not None
                 else "*.*", str(column or ""), bool(partial), bool(option)))
        return out

    def _accounts(self, side):
        """{'user name', 'role name'} the server holds."""
        return ({f"user {n}" for (n,) in self._rows(
            side, None, "select name from system.users")}
            | {f"role {n}" for (n,) in self._rows(
                side, None, "select name from system.roles")})

    def _deep_grants(self, db):
        """What each account both sides hold may do to this database's
        tables - the grants a data move does not carry."""
        from ..users import CH_SYS, NOT_CARRIED, carried
        if not carried(self.hop):
            return [Result("deep", f"{db} grants", "skip", NOT_CARRIED)]
        try:
            src, dst = self._grants("src", db), self._grants("dst", db)
            there = self._accounts("dst")
        except Exception as e:  # noqa: BLE001 - said, as a warning
            return [Result("deep", f"{db} grants", "warn",
                           "could not read the grants:"
                           f" {str(e).splitlines()[0][:90]} - unknown, not"
                           " clean")]
        mine = {w: g for w, g in src.items()
                if w.split(" ", 1)[1] not in CH_SYS}
        absent = sorted(w for w in mine if w not in there)
        miss, extra = [], []
        for who in sorted(set(mine) & there):
            held = dst.get(who, set())
            miss += [f"{who}: {g[0]} on {g[1]}" for g in
                     sorted(mine[who] - held)]
            extra += [f"{who}: {g[0]} on {g[1]}" for g in
                      sorted(held - mine[who])]
        if miss or extra or absent:
            parts = []
            if miss:
                parts.append(f"{len(miss)} grants missing on the target: "
                             + "; ".join(miss[:3]))
            if extra:
                parts.append(f"{len(extra)} grants on the target the source"
                             " does not give: " + "; ".join(extra[:3]))
            if absent:
                parts.append(f"{len(absent)} accounts hold grants here at"
                             " the source and do not exist on the target: "
                             + ", ".join(absent[:3]))
            return [Result("deep", f"{db} grants", "diff", "; ".join(parts),
                           "", "migkit users create replays the source's"
                               " accounts and grants")]
        return [Result("deep", f"{db} grants", "ok",
                       f"{sum(len(g) for g in mine.values())} grants match"
                       f" on {len(mine)} accounts" if mine
                       else "no account but the server's own is granted"
                       " here")]

    # ---- server-side fingerprints ---------------------------------------

    @staticmethod
    def _key_args(key):
        """A partition key as `partitionId`'s arguments: a tuple's members
        one by one - measured, `partitionId(k, toYYYYMM(at))` gives the
        table's own partition id and `partitionId((k, ...))` another."""
        text = str(key or "").strip()
        if not (text.startswith("(") and text.endswith(")")):
            return text
        depth, commas = 0, False
        for i, ch in enumerate(text):
            depth += ch == "("
            depth -= ch == ")"
            if depth == 0 and i < len(text) - 1:
                return text
            commas = commas or (ch == "," and depth == 1)
        return text[1:-1] if commas else text

    def _partitioned_by(self, side, db, table):
        got = self._rows(side, db, "select partition_key from system.tables"
                                   " where database = {d:String} and name ="
                                   " {t:String}",
                         {"d": self._d(side, db), "t": table})
        return str(got[0][0] or "").strip() if got else ""

    def _fingerprints(self, db, table, only=None):
        """({partition: (rows, digest)} of the source, the same of the
        target), each computed by its own server - no row leaves either.

        A partition is the source's: the target's rows are grouped by the
        source's partition key through `partitionId`, whatever the target
        is partitioned by. A row is hashed as the text of the tuple of its
        columns, in the source's order, where a string is quoted and NULL
        is not - a NULL and the string 'NULL' do not hash alike - and the
        hashes are summed, so a row there twice counts twice. Types that
        render differently (a target made nullable, another time zone)
        differ here and are settled by the row comparison."""
        columns = self.neutral_columns("src", db, table)
        key = self._partitioned_by("src", db, table)
        row = ("sum(cityHash64(toString(tuple("
               + ", ".join(self._q(n) for n, _ in columns) + "))))")
        out = []
        for side in ("src", "dst"):
            if not key:
                pid = "'all'"
            elif side == "src" or self._partitioned_by(side, db,
                                                        table) == key:
                pid = "_partition_id"
            else:
                pid = f"partitionId({self._key_args(key)})"
            where = ""
            params = {}
            if only is not None:
                where = f" where {pid} in {{p:Array(String)}}"
                params["p"] = sorted(only)
            got = self._rows(side, db,
                             f"select {pid} as p, count(), {row} from"
                             f" {self._qualified(side, db, table)}{where}"
                             f" group by p", params)
            out.append({str(p): (int(n), str(h)) for p, n, h in got})
        return out[0], out[1]

    def _renamed_or_mapped(self, db, table):
        """Whether the hop gives this table another name on the target or
        maps its columns: the fingerprints and the pull read the one name
        and every column on both sides."""
        from ..movers import _renamed
        parts = self.hop.parts_of(table)
        return bool(self.hop.column_rules(db, *parts)
                    or _renamed(self.hop, db, parts))

    def _read_through_rules(self, db, table):
        """Whether the hop reads this table through a rule that the
        servers' fingerprints do not read - a row filter, another name,
        mapped columns. The pair compares such a table row by row, through
        the rule."""
        return bool(self.hop.row_filter(db, *self.hop.parts_of(table))
                    or self._renamed_or_mapped(db, table))

    def check_data(self, db, table=None, stream=None):
        """Every table's partitions compared by fingerprints each server
        computes; only a table where they differ - or cannot be computed on
        a side - is compared row by row through the pair. Each finding
        names its table as the source does (`Result.table`), says how that
        table came out (`Result.tables`) and, where the rows were walked
        one by one, counts the rows apart (`Result.rows_apart`)."""
        tables = [table] if table else self.neutral_tables("src", db)
        same, rest = [], []
        for t in tables:
            if self._read_through_rules(db, t):
                rest.append(t)
                continue
            try:
                s, d = self._fingerprints(db, t)
            except Exception:  # noqa: BLE001 - the rows will say
                rest.append(t)
                continue
            if s == d:
                same.append(Result("data", f"{db}.{t}", "ok",
                                   f"{sum(n for n, _ in s.values()):,} rows"
                                   f" in {len(s)} partitions, each one's"
                                   " fingerprint equal on both servers",
                                   table=t, tables={t: "ok"}))
                if stream:
                    stream(f"{t}: ok")
            else:
                rest.append(t)
        out = list(same)
        if rest:
            pair = self._as_pair()
            for t in rest:
                out += [self._counted(db, t, r)
                        for r in pair.check_data(db, t, stream)]
        return out

    @staticmethod
    def _counted(db, table, found):
        """A table's finding from the pair's row comparison, naming its
        table, how it came out, and - where the pair walked every row and
        said how many are missing, extra and changed - those counts."""
        import re
        if not str(found.scope).startswith(f"{db}."):
            return found
        found.table = found.table or table
        found.tables = {table: found.status}
        got = re.search(r"\bmissing=(\d+) extra=(\d+) changed=(\d+)",
                        found.detail)
        if found.status == "diff" and got:
            found.rows_apart = rows_counted(table, *map(int, got.groups()))
        return found

    # ---- settings, access, snapshot, delta ----------------------------------

    #: settings that change what a value is read or written as, or whether
    #: a write lands: a server in another time zone renders every DateTime
    #: differently, and deduplication drops a batch inserted twice
    CRITICAL_PARAMS = ("server.timezone", "session_timezone",
                       "date_time_input_format", "join_use_nulls",
                       "insert_deduplicate", "input_format_null_as_default",
                       "max_partitions_per_insert_block",
                       "data_type_default_nullable", "union_default_mode",
                       "mutations_sync", "insert_quorum")

    def check_params(self, db):
        """The session's settings as the account reads them and the
        server's own, both sides, through the report every engine's go
        through."""
        def pull(side):
            try:
                got = {str(n): str(v) for n, v in self._rows(
                    side, None, "select name, value from system.settings")}
                got.update({f"server.{n}": str(v) for n, v in self._rows(
                    side, None, "select name, value from"
                                " system.server_settings")})
                got["server.timezone"] = str(self._rows(
                    side, None, "select timezone()")[0][0])
                return {n: v for n, v in got.items()
                        if not any(w in n.lower() for w in
                                   ("password", "secret", "key_"))}
            except Exception as e:  # noqa: BLE001 - said by the report
                return {self.UNREADABLE: str(e).splitlines()[0][:80]}
        return self._param_result(
            db, pull("src"), pull("dst"), self.CRITICAL_PARAMS,
            "set the target's server time zone and these settings as the"
            " source has them before cutover")

    def snapshot_state(self, db, state_dir, kind="all"):
        """Every target table frozen as it stands - the server links its
        parts under a name, which costs no copy until they change - with
        its definition and rows beside the names of the frozen parts. A
        server that will not freeze (a managed one) has its tables'
        definitions and rows recorded, and says why."""
        import json
        import re
        name = "migkit-" + re.sub(r"[^A-Za-z0-9_-]", "-", state_dir.name)
        out = {"freeze": name, "tables": {}}
        client = self._client("dst", db)
        try:
            for t in self.neutral_tables("dst", db):
                q = self._qualified("dst", db, t)
                entry = {
                    "create": client.command(f"show create table {q}"),
                    "rows": int(client.command(f"select count() from {q}"))}
                try:
                    got = client.query(
                        f"alter table {q} freeze with name '{name}'",
                        settings={"alter_partition_verbose_result": 1})
                    entry["frozen"] = sorted({str(r[2]) for r in
                                              got.result_rows})
                except Exception as e:  # noqa: BLE001 - recorded
                    entry["not_frozen"] = str(e).splitlines()[0][:160]
                out["tables"][t] = entry
        finally:
            client.close()
        (state_dir / "dst-tables.json").write_text(
            json.dumps(out, indent=2, sort_keys=True, default=str))

    def _parts(self, side, db, table=None):
        """{table: {partition: signature}} of the active parts: a partition
        written, merged or mutated since has another signature."""
        out = {}
        for t, p, n, h in self._rows(
                side, db, "select table, partition_id, sum(rows),"
                          " sum(cityHash64(name)) from system.parts where"
                          " database = {d:String} and active"
                          + (" and table = {t:String}" if table else "")
                          + " group by table, partition_id",
                {"d": self._d(side, db), "t": table or ""}):
            out.setdefault(str(t), {})[str(p)] = f"{n}|{h}"
        return out

    def delta_verify(self, db, limit=20000, log=None):
        """Only the partitions written, merged or mutated on either side
        since the last clean run, compared by the fingerprints each server
        computes. The first run has no baseline and compares them all. A
        partition gone from the source must be gone from the target too.
        The baseline moves only when every changed partition matched."""
        import json
        state = self.hop.report_dir(db) / "delta-parts.json"
        prev = json.loads(state.read_text()) if state.exists() else None
        now = {"src": self._parts("src", db), "dst": self._parts("dst", db)}
        # the target's parts are its own partitions; changes there are
        # found by table, then compared by the source's partitions
        res, clean, compared = [], True, 0
        for t in self.neutral_tables("src", db):
            if self._read_through_rules(db, t):
                clean = False
                res.append(Result(
                    "delta", f"{db}.{t}", "error",
                    "the hop reads this table through a row filter, another"
                    " name or mapped columns, which the partitions'"
                    " fingerprints do not read: `check` compares it row by"
                    " row, through them"))
                continue
            src_now = now["src"].get(t, {})
            if prev is None:
                only = None
            else:
                was = prev["src"].get(t, {})
                only = {p for p in set(src_now) | set(was)
                        if src_now.get(p) != was.get(p)}
                if now["dst"].get(t, {}) != prev["dst"].get(t, {}):
                    only = None
                if only is not None and not only:
                    continue
            try:
                s, d = self._fingerprints(db, t, only)
            except Exception as e:  # noqa: BLE001 - not a match
                clean = False
                res.append(Result("delta", f"{db}.{t}", "error",
                                  "the fingerprints could not be computed:"
                                  f" {str(e).splitlines()[0][:100]}"))
                continue
            bad = sorted(p for p in set(s) | set(d) if s.get(p) != d.get(p))
            compared += len(set(s) | set(d))
            clean = clean and not bad
            what = "every partition" if only is None else \
                f"{len(only)} changed partition(s)"
            res.append(Result(
                "delta", f"{db}.{t}", "diff" if bad else "ok",
                f"{what}: " + (f"{len(bad)} differ ({', '.join(bad[:6])})"
                               if bad else "equal on both servers")))
            if log:
                log(f"{t}: {'DIFF' if bad else 'ok'}")
        if clean:
            state.write_text(json.dumps(now))
        res.insert(0, Result(
            "delta", db, "ok" if clean else "diff",
            f"{compared} partition(s) compared, baseline"
            f" {'advanced' if clean else 'NOT advanced'}"))
        return res

    # ---- the target pulls from the source, a partition at a time ----------

    #: copies of one partition before it is left as still changing on the
    #: source while it was copied
    PULL_ATTEMPTS = 3
    #: the most and the least a block the target builds while it pulls may
    #: hold, whatever its memory allows between them
    BLOCK_MOST, BLOCK_LEAST = 256 * 2 ** 20, 16 * 2 ** 20
    #: seconds one partition's pull may take before the connection gives up
    PULL_WAIT = 6 * 3600
    #: rows a batch of the table copier reads, where a table goes by it
    COPIER_CHUNK = 500_000

    @staticmethod
    def _lit(text):
        return "'" + str(text).replace("\\", "\\\\").replace("'", "\\'") + "'"

    def _hidden(self, text):
        """`text` with either side's password taken out: a pull names the
        source's in the statement the target runs."""
        from ..util import without_secret
        text = str(text)
        for secret in (self.hop.source.password, self.hop.target.password):
            text = without_secret(text, secret)
        return text

    def _reach(self):
        """How the target reaches the source, asked of the target itself:
        {"fn", "address", "timeout_ms"}, or {"why"} where it cannot.

        The address is the source endpoint's `address_from_target` where
        the target knows the source by another name than migkit does, and
        otherwise the source's host at the port it takes its own protocol
        on, which the source is asked for. The target asks the server at
        that address for its id, and only the source's is taken: an
        address that leads back to the target (`127.0.0.1`, seen from the
        target, is the target) or to a third server is refused rather
        than copied from."""
        got = self.__dict__.get("_reached")
        if got is not None:
            return got
        import time
        s = self.hop.source
        fn = "remoteSecure" if s.options.get("secure") else "remote"
        address = str(s.options.get("address_from_target") or "")
        try:
            src_id = str(self._rows("src", None, "select serverUUID()")[0][0])
            dst_id = str(self._rows("dst", None, "select serverUUID()")[0][0])
            if not address:
                port = self._rows(
                    "src", None, "select getServerPort({p:String})",
                    {"p": "tcp_port_secure" if fn == "remoteSecure"
                     else "tcp_port"})[0][0]
                address = f"{s.host}:{port}"
            began = time.monotonic()
            seen = str(self._rows(
                "dst", None, f"select serverUUID() from {fn}("
                f"{self._lit(address)}, 'system', 'one',"
                f" {self._lit(s.user or 'default')},"
                f" {self._lit(s.password or '')})",
                settings={"connect_timeout_with_failover_ms": 5000,
                          "connect_timeout_with_failover_secure_ms": 5000}
            )[0][0])
            took = (time.monotonic() - began) * 1000
        except Exception as e:  # noqa: BLE001 - the reason is the answer
            got = {"why": "the target cannot reach the source at"
                          f" {address or s.host}: "
                          + self._hidden(str(e).splitlines()[0])[:160]}
        else:
            if seen == src_id:
                # the default allows 50 ms to connect, which a link between
                # regions does not make: ten times the round trip measured
                got = {"fn": fn, "address": address,
                       "timeout_ms": max(1000, int(10 * took))}
            elif seen == dst_id:
                got = {"why": f"{address}, seen from the target, is the"
                              " target itself"}
            else:
                got = {"why": f"{address}, seen from the target, is a"
                              " server other than the source"}
        self.__dict__["_reached"] = got
        return got

    def _remote(self, reach, db, table):
        s = self.hop.source
        return (f"{reach['fn']}({self._lit(reach['address'])},"
                f" {self._lit(db)}, {self._lit(table)},"
                f" {self._lit(s.user or 'default')},"
                f" {self._lit(s.password or '')})")

    def _engine_of(self, side, db, table):
        got = self._rows(side, db, "select engine from system.tables where"
                                   " database = {d:String} and name ="
                                   " {t:String}",
                         {"d": self._d(side, db), "t": table})
        return str(got[0][0]) if got else ""

    def _insertable(self, db, table):
        """The source's columns an insert takes: not one the table computes
        (`MATERIALIZED`, `ALIAS`, `EPHEMERAL`), which the target computes
        again by the same definition."""
        return [n for n, kind in self._rows(
            "src", db, "select name, default_kind from system.columns where"
                       " database = {d:String} and table = {t:String} order"
                       " by position", {"d": db, "t": table})
            if kind not in ("MATERIALIZED", "ALIAS", "EPHEMERAL")]

    def _comparable(self, db, table):
        """Whether the two tables' fingerprints can be held to each other:
        every column of one type on both sides, as a value is (a column the
        target keeps nullable, or low-cardinality, renders the same)."""
        def types(side):
            return sorted((n, _unwrap(t)) for n, t in self._rows(
                side, db, "select name, type from system.columns where"
                          " database = {d:String} and table = {t:String}",
                {"d": self._d(side, db), "t": table}))
        return types("src") == types("dst")

    #: the target pulling a partition at a time, and the table copier: the
    #: order measured on 24.8 - 500,000 rows pulled in 0.6s where the
    #: table copier reads and writes every row through migkit
    PULL, COPIER = "pull", "builtin"

    @classmethod
    def _ways(cls):
        """The ways one table of a ClickHouse move goes, climbed per table
        in `decide`: pulled by the target where the table keeps partitions
        on both sides and the target reaches the source (the proof, asked
        of the target once for the move), the table copier otherwise."""
        from ..decide import Need, Rung
        return (
            Rung(cls.PULL, "the target pulling from the source",
                 needs=(
                     Need("partitions on the source",
                          lambda f: f["kind"].endswith("MergeTree"),
                          lambda f: f"a {f['kind'] or 'view'} table keeps"
                                    " no partitions to pull by"),
                     Need("partitions on the target",
                          lambda f: f["there"] is None
                          or f["there"].endswith("MergeTree"),
                          "the target's table keeps no partitions"),
                     Need("a definition the target can take",
                          lambda f: f["there"] is not None
                          or not f["kind"].startswith("Replicated"),
                          "its definition names the source's replication"
                          " path, which the target does not take")),
                 prove=lambda f: f["reach"].get("why"),
                 because="no row through migkit, and each partition held"
                         " to the source's by the two servers'"
                         " fingerprints"),
            Rung(cls.COPIER, "the table copier",
                 gives=frozenset({"resume by chunk"})))

    def _pull_plan(self, db, tables):
        """([tables the target pulls], [(table, why it goes by the table
        copier instead)], {table: the facts its climb read}), each table
        climbed through `_ways`."""
        from ..decide import Facts, climb
        there = ({} if self.target_missing(db) else
                 {t: self._engine_of("dst", db, t)
                  for t in self.neutral_tables("dst", db)})
        pulled, copied, facts = [], [], {}
        for t in tables:
            facts[t] = Facts({"there": there.get(t)}, reach=self._reach,
                             kind=lambda t=t: self._engine_of("src", db, t))
            got = climb(t, self._ways(), facts[t])
            if got.path == self.PULL:
                pulled.append(t)
            else:
                copied.append((t, got.reason))
        return pulled, copied, facts

    def _made_from_source(self, db, table):
        """The target table made by the source's own definition - engine,
        partitioning, order, time to live, settings - under the target's
        database name."""
        import re
        text = str(self._rows("src", db, "show create table"
                              f" {self._qualified('src', db, table)}")[0][0])
        name = r"(?:`(?:[^`\\]|\\.)*`|[A-Za-z_][A-Za-z0-9_]*)"
        made = re.sub(rf"^CREATE TABLE {name}\.{name}",
                      lambda m: "CREATE TABLE "
                      + self._qualified("dst", db, table), text, count=1)
        self.neutral_create_code("dst", db, made)

    def _partition_order(self, db, table):
        """The source's partitions in the order of their key's values -
        numbers by their size, not by their text."""
        import re

        def natural(text):
            return [(0, int(p), "") if p.isdigit() else (1, 0, p)
                    for p in re.findall(r"\d+|\D+", str(text))]
        got = self._rows("src", db, "select partition_id, any(partition)"
                                    " from system.parts where database ="
                                    " {d:String} and table = {t:String} and"
                                    " active group by partition_id",
                         {"d": db, "t": table})
        return [str(p) for p, _ in sorted(got, key=lambda r: natural(r[1]))]

    def _pull_settings(self, db, table, reach):
        """The session's settings for one table's pull, from migkit's
        sizing: as many insert threads as the move may run at once, each
        building blocks no larger than a quarter of the memory the target
        has (its `max_memory_usage`, or what its host has free) divided
        among them, and of as many rows as the source's rows' width puts
        in one. Blocks land as the source holds them
        (`optimize_on_insert = 0`): a replacing or collapsing table merges
        a block it takes whole, and the partition would arrive with fewer
        rows than the source's. Nothing is set on the server."""
        threads = max(1, int(getattr(self.hop, "workers_most", 0)
                             or self.hop.workers or 1))
        room = []
        for sql in ("select toUInt64(value) from system.settings where"
                    " name = 'max_memory_usage'",
                    "select toUInt64(value) from system.asynchronous_metrics"
                    " where metric = 'OSMemoryAvailable'"):
            try:
                got = self._rows("dst", None, sql)
                if got and int(got[0][0]) > 0:
                    room.append(int(got[0][0]))
            except Exception:  # noqa: BLE001 - one reading fewer
                pass
        block = self.BLOCK_MOST
        if room:
            block = max(self.BLOCK_LEAST,
                        min(block, min(room) // (4 * threads)))
        width = self._rows("src", db, "select sum(data_uncompressed_bytes)"
                                      " / greatest(sum(rows), 1) from"
                                      " system.parts where database ="
                                      " {d:String} and table = {t:String}"
                                      " and active",
                           {"d": db, "t": table})[0][0] or 64
        settings = {"max_insert_threads": threads, "max_threads": threads,
                    "min_insert_block_size_bytes": block,
                    "min_insert_block_size_rows": max(
                        1024, int(block // max(1.0, float(width)))),
                    "optimize_on_insert": 0}
        if reach and "fn" in reach:
            settings["connect_timeout_with_failover_ms"] = reach["timeout_ms"]
            settings["connect_timeout_with_failover_secure_ms"] = \
                reach["timeout_ms"]
        return settings

    def _pull_wheres(self, db, table, pid):
        """What one partition is pulled by: itself, or - where the target
        is partitioned another way and this partition spans more of the
        target's than one insert may write (`max_partitions_per_insert_block`,
        measured: more stops the insert with `Too many partitions`) - that
        many of the target's partitions a statement, each list asked of
        the source."""
        whole = f"_partition_id = {self._lit(pid)}"
        src_key = self._partitioned_by("src", db, table)
        dst_key = self._partitioned_by("dst", db, table)
        if not dst_key or dst_key == src_key:
            return [whole]
        most = int(self._rows("dst", None, "select value from system.settings"
                                           " where name ="
                                           " 'max_partitions_per_insert_block'"
                              )[0][0] or 0)
        args = self._key_args(dst_key)
        ids = [str(r[0]) for r in self._rows(
            "src", db, f"select distinct partitionId({args}) as p from"
                       f" {self._qualified('src', db, table)} where {whole}"
                       " order by p")]
        if not most or len(ids) <= most:
            return [whole]
        return [f"{whole} and partitionId({args}) in ("
                + ", ".join(self._lit(i) for i in ids[at:at + most]) + ")"
                for at in range(0, len(ids), most)]

    def _clear_partition(self, db, table, pid):
        """The target's rows of one of the source's partitions taken away:
        the partition dropped where the target is partitioned alike, its
        rows deleted where not, the table emptied where the source keeps
        one partition."""
        key = self._partitioned_by("src", db, table)
        q = self._qualified("dst", db, table)
        client = self._client("dst", db)
        try:
            if not key:
                client.command(f"truncate table {q}")
            elif self._partitioned_by("dst", db, table) == key:
                client.command(f"alter table {q} drop partition id"
                               f" {self._lit(pid)}")
            else:
                client.command(f"alter table {q} delete where partitionId("
                               f"{self._key_args(key)}) = {self._lit(pid)}",
                               settings={"mutations_sync": 2})
        finally:
            client.close()

    def _carry_rows(self, db, table, where, settings):
        """One partition's rows read from the source and written to the
        target through migkit, where the target cannot reach the source.
        Strings cross as the bytes they hold, times as the moments they
        are."""
        cols = self._insertable(db, table)
        src = self._client("src", db, wait=self.PULL_WAIT)
        dst = self._client("dst", db, wait=self.PULL_WAIT)
        keep = {k: v for k, v in settings.items()
                if k in ("optimize_on_insert", "insert_deduplication_token")}
        try:
            with src.query_row_block_stream(
                    f"select {', '.join(self._q(c) for c in cols)} from"
                    f" {self._qualified('src', db, table)} where {where}",
                    query_formats=self.AS_BYTES) as stream:
                for block in stream:
                    dst.insert(table, [list(r) for r in block],
                               column_names=cols,
                               database=self._d("dst", db), settings=keep)
        finally:
            src.close()
            dst.close()

    def _next_copy(self, db, table, pid):
        """The number of this copy of one partition, counted with the hop,
        so neither a move run again nor the follow ever reuses a token."""
        import json
        path = self.hop.report_dir(db) / "pulled-partitions.json"
        try:
            state = json.loads(path.read_text())
        except (OSError, ValueError):
            state = {}
        mine = state.setdefault(table, {})
        mine[pid] = n = int(mine.get(pid, 0)) + 1
        path.write_text(json.dumps(state))
        return n

    def _pull_partition(self, db, table, pid, reach, settings, comparable):
        """(how it ended, rows) for one of the source's partitions, copied
        to the target and held to the source's: "equal", "differs" (the
        source did not change and the target still does not match it) or
        "changing" (`PULLED` says each in words).

        Its rows on the target are taken away, then pulled by the target
        (`remote()`), or carried through migkit where the target cannot
        reach the source; then both servers fingerprint the partition
        (`_fingerprints`) and the two must match. Not the parts' own hashes
        in `system.parts`: those hash the files, and a logical copy writes
        other files for the same rows - they match only after a copy of
        the files themselves.

        Each statement carries an `insert_deduplication_token` made of the
        partition, the source's parts as they stood when the copy began,
        and the copy's number, counted up every time this partition is
        copied again. A table that deduplicates (a replicated one, or one
        with `non_replicated_deduplication_window`) drops a block whose
        token it has seen - or, with no token, whose content it has seen.
        Measured on 24.8, a month's rows deleted from a target partitioned
        by day and copied again: under the copy before's token 624 of 696
        rows came back, with no token none. The token alone does not make a
        resend exact - an `INSERT ... SELECT` over several threads cuts
        other blocks each time, and ClickHouse deduplicates one only with
        `ORDER BY ALL` and one stream - so a statement whose answer was
        lost is not sent again: the partition is fingerprinted, and one
        that does not match is cleared and copied again under the next
        number. A source that changed during the copy is copied again, up
        to `PULL_ATTEMPTS` times."""
        import hashlib
        import time

        from ..util import is_transient
        cols = self._insertable(db, table)
        names = ", ".join(self._q(c) for c in cols)
        into = self._qualified("dst", db, table)
        wheres = self._pull_wheres(db, table, pid)
        for _ in range(self.PULL_ATTEMPTS):
            before = self._parts("src", db, table).get(table, {}).get(pid)
            n = self._next_copy(db, table, pid)
            self._clear_partition(db, table, pid)
            lost = None
            for i, where in enumerate(wheres):
                token = hashlib.sha1(
                    f"{self.hop.name}|{db}.{table}|{pid}|{i}|{before}|{n}"
                    .encode()).hexdigest()
                said = {**settings, "insert_deduplication_token": token}
                try:
                    if "fn" in reach:
                        client = self._client("dst", db, wait=self.PULL_WAIT)
                        try:
                            client.command(
                                f"insert into {into} ({names}) select"
                                f" {names} from"
                                f" {self._remote(reach, db, table)} where"
                                f" {where}", settings=said)
                        finally:
                            client.close()
                    else:
                        self._carry_rows(db, table, where, said)
                except Exception as e:  # noqa: BLE001 - classified here
                    if not is_transient(e):
                        # the partition was cleared and nothing of it is
                        # kept as copied: another way may carry the table
                        from .. import decide
                        raise decide.Blocked(
                            f"{table}: partition {pid} could not be copied:"
                            f" {self._hidden(str(e).splitlines()[0])[:200]}")
                    lost = e
                    time.sleep(2)
                    break
            s, d = self._fingerprints(db, table, only={pid})
            a, b = s.get(pid), d.get(pid)
            if not comparable:
                a, b = a and a[0], b and b[0]
            if a == b:
                return "equal", (s.get(pid) or (0, ""))[0]
            if lost is None and self._parts("src", db, table).get(
                    table, {}).get(pid) == before:
                return "differs", 0
        return "changing", 0

    #: how a partition's copy ended (`_pull_partition`), in words
    PULLED = {"differs": "differs from the source after the copy, and the"
                         " source did not change",
              "changing": f"was still changing on the source after"
                          f" {PULL_ATTEMPTS} copies"}

    @staticmethod
    def _held(comparable):
        return ("each partition's fingerprint equal on both servers"
                if comparable else
                "counted alike on both servers (the target's column types"
                " differ, so the check compares the rows)")

    def _pull_table(self, db, table, reach, log):
        """Every partition of one table: those the target already holds as
        the source does are left (the two servers' fingerprints say so),
        the target's rows of a partition the source does not hold taken
        away, the rest copied in the order of the partition key."""
        comparable = self._comparable(db, table)
        s, d = self._fingerprints(db, table)
        for pid in sorted(set(d) - set(s)):
            self._clear_partition(db, table, pid)
        settings = self._pull_settings(db, table, reach)
        copied, rows, equal, changing = 0, 0, 0, []
        for pid in self._partition_order(db, table):
            if comparable and s.get(pid) == d.get(pid):
                equal += 1
                continue
            how, n = self._pull_partition(db, table, pid, reach, settings,
                                          comparable)
            if how == "differs":
                raise SystemExit(f"{table}: partition {pid}"
                                 f" {self.PULLED[how]}")
            if how == "equal":
                copied, rows = copied + 1, rows + n
            else:
                changing.append(pid)
        if log:
            log(f"{table}: {copied:,} partitions pulled by the target from"
                f" the source, {rows:,} rows, {self._held(comparable)}"
                + (f"; {equal:,} already equal" if equal else "")
                + (f"; {len(changing)} still changing on the source"
                   f" ({', '.join(changing[:6])}) - the check or the follow"
                   " carries them" if changing else ""))
        return copied, equal, changing

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table pulled by the target from the source, no row through
        migkit: `INSERT ... SELECT` from `remote()` (`remoteSecure()` where
        the source takes TLS), one of the source's partitions a statement,
        in the order of the partition key - one insert is refused past
        `max_partitions_per_insert_block` of the target's partitions, and a
        partition is also the unit a copy resumes by and is verified by
        (`_pull_partition`). What the target already holds as the source
        does is left as it is. The source is read as the hop's own account
        reads it; its password travels in the statement to the target,
        which hides it in its own logs (measured on 24.8: `[HIDDEN]` in
        `system.query_log` and in its errors), and migkit prints neither.

        Chosen table by table (`_ways`, climbed in `decide`) where the
        target reaches the source (`_reach`, asked of the target); where it
        does not, or a table keeps no partitions, or it is replicated and
        not on the target yet, the table goes by the table copier as
        before, and the plan says why. A pull that fails as it runs - the
        target refusing `remote()`, a privilege it lacks, the source out of
        its reach - is switched from by itself to the table copier and said
        (`decide.attempt`); the copier empties the table before it writes,
        so the partitions pulled before are not doubled. A partition that
        differs from the source after its copy is the answer, and stops
        the move: never switched from. A table the target lacks is made by
        the source's own definition."""
        pulled, copied, facts = self._pull_plan(db, tables)
        reach = self._reach() if pulled else {}
        steps = ([f"{t}: pulled by the target from the source, a partition"
                  " at a time" for t in pulled]
                 + [f"{t}: copied table by table - {why}"
                    for t, why in copied])
        if not go:
            return steps
        self.prepare_target(db)
        there = set(self.neutral_tables("dst", db))
        for t in list(shape_only) + pulled:
            if t not in there and not self._engine_of(
                    "src", db, t).startswith("Replicated"):
                self._made_from_source(db, t)
        ck = []

        def run(choice, t):
            if choice.path == self.PULL:
                return self._pull_table(db, t, reach, log)
            if not ck:
                from ..cli import _Checkpoint
                ck.append(_Checkpoint(self.hop.report_dir(db)
                                      / "move-rows.json"))
                ck[0].clear()
                ck[0].discard()
            return self.move_table(db, "", t, self.COPIER_CHUNK, ck[0], log)
        for t in pulled + [t for t, _ in copied]:
            self._attempt(t, self._ways(),
                          lambda choice, t=t: run(choice, t), facts[t],
                          log=log)
        return steps

    # ---- following the source by the partitions that changed ---------------

    #: seconds between two looks at the source's parts once caught up
    FOLLOW_EVERY = 2
    CHANGE_POINT_READS_ONLY = True

    def stream_identity(self, side, db):
        return {"server": str(self._rows(side, None,
                                         "select serverUUID()")[0][0])}

    def change_point(self, side, db):
        """Now, as the source holds it: the server's own clock, then each
        table's active parts by partition (`_parts`). Read in that order,
        every write committed before the time is among the parts. Reading
        them holds nothing and costs the source nothing."""
        at = str(self._rows(side, None, "select toString(now64(6))")[0][0])
        return {"at": at, "parts": self._parts(side, db)}

    def log_position(self, side, db):
        return self.change_point(side, db)

    @staticmethod
    def position_reached(have, want):
        """Whether the follow has settled every partition as the source
        held it at `want`'s time: its own position is the time of the last
        look after which every changed partition matched."""
        if not have or not want:
            return None
        return str(have.get("at") or "") >= str(want.get("at") or "")

    def tail_start(self, db, token_path):
        """The position the follow begins at, taken before the copy it
        follows: a partition written while the copy runs has other parts
        by the time the follow looks, and is compared again."""
        from .. import tailctl
        self._refuse_follow(db)
        if token_path.exists():
            return False
        tailctl.begin(self, db, token_path, self.change_point("src", db))
        return True

    def tail_seed(self, db, token_path, point):
        from .. import tailctl
        tailctl.begin(self, db, token_path, point)

    @staticmethod
    def _changed(was, now):
        """{table: [partitions whose parts are not what they were]} - a
        write, a merge, a mutation or a drop gives a partition other parts
        (their names carry the blocks and the mutation)."""
        out = {}
        for t in set(was) | set(now):
            a, b = was.get(t, {}), now.get(t, {})
            moved = sorted(p for p in set(a) | set(b) if a.get(p) != b.get(p))
            if moved:
                out[t] = moved
        return out

    def _follow_round(self, db, token, point, reach, log):
        """(every changed partition settled, partitions copied, the parts
        as far as they are settled) for one look at the source."""
        settled, went = True, 0
        parts = {t: dict(v) for t, v in (token.get("parts") or {}).items()}
        on_src = set(self.neutral_tables("src", db))
        there = set(self.neutral_tables("dst", db))
        for t, moved in sorted(self._changed(token.get("parts") or {},
                                             point["parts"]).items()):
            if not self.in_scope(db, t):
                continue
            if t not in on_src:
                log(f"{t} is no longer on the source; its rows on the"
                    " target are left as they are - a table dropped is the"
                    " operator's to drop")
                parts.pop(t, None)
                continue
            if t not in there:
                if self._engine_of("src", db, t).startswith("Replicated"):
                    log(f"{t} is new on the source and replicated: make it"
                        " on the target to have it followed")
                    settled = False
                    continue
                self._made_from_source(db, t)
                there.add(t)
            comparable = self._comparable(db, t)
            s, d = self._fingerprints(db, t, only=set(moved))
            settings = None
            for pid in moved:
                if comparable and s.get(pid) == d.get(pid):
                    self._settle(parts, point, t, pid)
                    continue
                if pid not in s:
                    self._clear_partition(db, t, pid)
                    self._settle(parts, point, t, pid)
                    went += 1
                    continue
                settings = settings or self._pull_settings(db, t, reach)
                how, _ = self._pull_partition(db, t, pid, reach, settings,
                                              comparable)
                went += 1
                if how == "equal":
                    self._settle(parts, point, t, pid)
                else:
                    settled = False
                    log(f"{t}: partition {pid} {self.PULLED[how]}")
        return settled, went, parts

    @staticmethod
    def _settle(parts, point, table, pid):
        now = point["parts"].get(table, {}).get(pid)
        if now is None:
            parts.get(table, {}).pop(pid, None)
        else:
            parts.setdefault(table, {})[pid] = now

    def _cannot_follow(self, db):
        """[(table, why)] of the source's tables the hop takes that a
        follow by partitions would carry wrongly: a partition that differs
        is copied again whole, under its own name, with every row and every
        kind of change in it - so a table the hop filters, renames, maps
        the columns of, or carries only some changes of (`mapping.ops`,
        `Hop.carries`) cannot be followed this way
        (`movers.stream_cannot_carry`)."""
        from .. import movers
        return movers.stream_cannot_carry(self.hop, db,
                                          self.neutral_tables("src", db))

    def _refuse_follow(self, db):
        """Stop, before anything is copied or followed, at a hop a follow by
        partitions cannot hold (`_cannot_follow`)."""
        refused = self._cannot_follow(db)
        if refused:
            raise SystemExit(
                f"{db}: " + "; ".join(f"{t} ({why})" for t, why in refused[:6])
                + (f" and {len(refused) - 6} more" if len(refused) > 6
                   else "")
                + " - a follow by partitions copies a changed partition"
                " again whole, under its own name, with every row and every"
                " kind of change in it, so it cannot hold the table to what"
                " the hop asks. Nothing was followed: leave these tables out"
                " of the hop, or carry them another way")

    def tail_apply(self, db, go, token_path, log):
        """Keep the target following the source by the partitions that
        changed: each round the source's parts are read, every partition
        whose parts are not those of the saved position is fingerprinted
        on both servers, and one that differs is copied again - pulled by
        the target, or carried through migkit where the target cannot
        reach the source - and held to the source's again
        (`_pull_partition`). One the source no longer holds is taken from
        the target. A ClickHouse table keeps no log of its rows' changes;
        a partition is what changes, and what is compared.

        The position moves partition by partition as each matches, and its
        time only once every changed one has, so the fence
        (`position_reached`) waits for all of them."""
        from .. import tailctl
        self._refuse_follow(db)
        reach = {}

        def look(token):
            if not reach:
                reach.update(self._reach())
                if "why" in reach:
                    log(f"{reach['why']}; changed partitions are carried"
                        " through migkit")
            point = self.change_point("src", db)
            settled, went, parts = self._follow_round(db, token, point,
                                                      reach, log)
            return ({"at": point["at"] if settled else token.get("at"),
                     "parts": parts}, went, settled)

        def behind(token):
            moved = (self._changed(token.get("parts") or {},
                                   self._parts("src", db)) if token else {})
            return (f"{sum(len(v) for v in moved.values()):,} partitions"
                    " changed on the source since the saved position"
                    + "".join(f"; {t} {len(v)}"
                              for t, v in sorted(moved.items())))
        tailctl.follow(self, db, go, token_path, log, "partitions", look,
                       behind, self.FOLLOW_EVERY)

    # the fence and the confirm are the pair's, through the position above

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    # ---- what the servers can give a move ------------------------------------

    def capacity(self, side, db):
        """The server's CPUs, its free query slots and the queries running
        now, as it reports them itself. Its CPUs are the threads it gives
        one query (`max_threads`, `auto(N)` where left to the server,
        which counts its cores - measured on 24.8, no metric names them)."""
        import re
        out = {}
        for key, sql in (
                ("cpus", "select value from system.settings where name ="
                         " 'max_threads'"),
                ("running", "select toUInt32(value) from system.metrics"
                            " where metric = 'Query'"),
                ("limit", "select toUInt32(value) from"
                          " system.server_settings where name ="
                          " 'max_concurrent_queries'")):
            try:
                got = self._rows(side, None, sql)
                n = re.findall(r"\d+", str(got[0][0])) if got else []
                if n and int(n[-1]) > 0:
                    out[key] = int(n[-1])
            except Exception:  # noqa: BLE001 - not answered, left out
                pass
        if "limit" in out:
            out["free_connections"] = max(0, out.pop("limit")
                                          - out.get("running", 0))
        return out

    # ---- the assessment: what the servers say before anything moves -------

    def _server_versions(self):
        """(the source's version, the target's) as each server says it."""
        def one(side):
            try:
                return str(self._rows(side, None, "select version()")[0][0])
            except Exception:  # noqa: BLE001 - unknown, said by the report
                return None
        return (one("src"), one("dst"))

    def _brand_probes(self):
        """What each server says of itself: its version, and the build it
        names (`variants.SIGNATURES`) - the one number a managed build or a
        distributor's fork may spell its own way."""
        def one(side):
            out = {}
            for key, sql in (("version", "select version()"),
                             ("version_full", "select value from"
                                              " system.build_options where"
                                              " name = 'VERSION_FULL'")):
                try:
                    out[key] = str(self._rows(side, None, sql)[0][0])
                except Exception:  # noqa: BLE001 - a field fewer
                    pass
            return out
        return (one("src"), one("dst"))

    def _source_unsettled(self, db):
        """Rows the source holds that a read now does not give: parts
        detached from their table (a failed merge, a part that did not
        verify, one taken off by hand) hold rows no query sees and no copy
        carries, and a mutation that has not finished leaves the rows
        read as they were halfway."""
        scope = f"{db} source settled"
        try:
            detached = self._scoped_rows(db, self._rows(
                "src", db, "select table, count() from system.detached_parts"
                           " where database = {d:String} group by table",
                {"d": self._d("src", db)}))
            pending = self._scoped_rows(db, self._rows(
                "src", db, "select table, command, latest_fail_reason from"
                           " system.mutations where database = {d:String}"
                           " and not is_done", {"d": self._d("src", db)}))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return Result("deep", scope, "error",
                          "could not read the source's parts and mutations:"
                          f" {str(e).splitlines()[0][:90]}")
        if detached:
            return Result(
                "deep", scope, "diff",
                f"{sum(int(n) for _, n in detached)} parts of"
                f" {len(detached)} tables are detached"
                f" ({', '.join(str(t) for t, _ in detached[:4])}): the rows"
                " in them are in no query's answer, and a copy leaves them"
                " behind", "",
                "attach them (ALTER TABLE ... ATTACH PARTITION) or drop"
                " them, then move")
        if pending:
            named = ", ".join(sorted({str(r[0]) for r in pending})[:4])
            return Result(
                "deep", scope, "warn",
                f"{len(pending)} changes are still being applied on the"
                f" source ({named}) - rows read now are not settled", "",
                "wait for them, then move")
        return Result("deep", scope, "ok",
                      "no part detached from a table, no change still being"
                      " applied")

    def _assess_extra(self):
        """What ClickHouse's two servers say about being moved between:
        whether the target deletes by key (a copy written again, a
        repair), whether it reaches the source to pull from it, whether
        any replicated table of either is read-only; and the work the move
        leaves for hands (`_handwork`)."""
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "instance", "item": item,
                          "detail": str(detail)})

        def said(e):
            return str(e).splitlines()[0][:90] if str(e) else type(e).__name__
        try:
            version = str(self._rows("dst", None, "select version()")[0][0])
            major, minor = (int(x) for x in version.split(".")[:2])
            add("pass" if (major, minor) >= (23, 3) else "warn",
                "target deletes by key",
                f"{version}: "
                + ("a lightweight delete is available" if (major, minor)
                   >= (23, 3) else "a lightweight delete is experimental"
                   " before 23.3")
                + " - a batch written again and a repair delete the rows"
                  " they replace with it")
        except Exception as e:  # noqa: BLE001 - unknown, not clean
            add("warn", "target deletes by key",
                f"could not be read: {said(e)} - unknown, not clean")
        if self.hop.source.configured() and self.hop.target.configured():
            reach = self._reach()
            if "fn" in reach:
                add("pass", "target reaches the source",
                    f"{reach['address']}: partitions are pulled by the"
                    " target, no row through migkit")
            else:
                add("warn", "target reaches the source",
                    f"{reach.get('why')}: rows go through migkit, table by"
                    " table")
        for side, who in (("src", "source"), ("dst", "target")):
            try:
                n = int(self._rows(
                    side, None, "select count() from system.replicas where"
                                " is_readonly or is_session_expired")[0][0])
                add("warn" if n else "pass", f"{who} replicas writable",
                    f"{n} replicated tables are read-only or have lost"
                    " their session" if n else "no replicated table is"
                    " read-only")
            except Exception as e:  # noqa: BLE001 - unknown, not clean
                add("warn", f"{who} replicas writable",
                    f"could not be read: {said(e)} - unknown, not clean")
        inv = self._handwork()
        return items + inv.rows() + inv.summary()

    def _handwork(self):
        """The work the move leaves for hands, per `migkit.handwork`: what
        a copy of the tables does not make on the target."""
        from .. import handwork
        inv = handwork.Inventory()
        try:
            dbs = self.databases()
        except Exception as e:  # noqa: BLE001 - nothing was looked at
            inv.unknown("target-prereq", "instance",
                        f"the databases could not be listed: "
                        f"{str(e).splitlines()[0][:90]}")
            return inv
        queues = ", ".join(f"'{e}'" for e in self.QUEUE_ENGINES)
        for db in dbs:
            ask = {"d": self._d("src", db)}

            def probe(kind, what, sql, take=lambda r: str(r[0]),
                      taken=None, ask=ask, db=db):
                try:
                    rows = self._rows("src", db, sql, ask)
                except Exception as e:  # noqa: BLE001 - never read as none
                    inv.unknown(kind, db, f"{what}: "
                                f"{str(e).splitlines()[0][:80]}")
                    return
                # only what the hop takes (`takes`, `in_scope`)
                inv.add(kind, db, what, [
                    take(r) for r in rows
                    if (self.takes(db, taken, str(r[0])) if taken
                        else self.in_scope(db, str(r[0])))])
            probe("no-row-key", "MergeTree tables with no sorting key",
                  "select name from system.tables where database ="
                  " {d:String} and endsWith(engine, 'MergeTree') and"
                  " sorting_key = ''", take=lambda r: str(r[0]))
            probe("server-side-code", "views and materialized views",
                  "select name, engine from system.tables where database ="
                  " {d:String} and engine in ('View', 'MaterializedView',"
                  " 'LiveView', 'WindowView')", taken="view")
            probe("not-carried", "dictionaries (loaded from their own"
                                 " sources)",
                  "select name from system.tables where database ="
                  " {d:String} and engine = 'Dictionary'")
            probe("not-carried", "row policies",
                  "select table, short_name from system.row_policies where"
                  " database = {d:String}",
                  take=lambda r: f"{r[0]}.{r[1]}")
            probe("target-prereq", "Distributed tables (the cluster must be"
                                   " defined on the target)",
                  "select name from system.tables where database ="
                  " {d:String} and engine = 'Distributed'")
            probe("decide-then-apply", "tables that read a stream (made on"
                                       " the target they would compete for"
                                       " it)",
                  "select name from system.tables where database ="
                  f" {{d:String}} and engine in ({queues})")
        try:
            inv.add("server-side-code", "instance", "SQL functions",
                    sorted(self._functions("src")))
        except Exception as e:  # noqa: BLE001 - never read as none
            inv.unknown("server-side-code", "instance",
                        f"SQL functions: {str(e).splitlines()[0][:80]}")
        return inv

    def stream_writers(self, db):
        """Besides migkit's own tail, what writes into the target's tables
        of this database: a table that reads a stream into others through
        a view, and inserts running now from an account other than
        migkit's. None can be paused from here."""
        out = super().stream_writers(db)
        if self.target_missing(db):
            # a database not made yet has nothing writing into it
            return out
        ask = {"d": self._d("dst", db)}
        queues = ", ".join(f"'{e}'" for e in self.QUEUE_ENGINES)
        for name, engine in self._scoped_rows(db, self._rows(
                "dst", db, "select name, engine from system.tables where"
                           f" database = {{d:String}} and engine in"
                           f" ({queues}) and notEmpty(dependencies_table)",
                ask)):
            out.append((f"{engine} table {name} reading a stream into the"
                        " tables it feeds", False))
        for user, n in self._rows(
                "dst", db, "select user, count() from system.processes where"
                           " query_kind = 'Insert' and current_database ="
                           " {d:String} and user != {u:String} group by user",
                {**ask, "u": self.hop.target.user or "default"}):
            out.append((f"{n} insert(s) running now from {user}", False))
        return out

    # ---- scheduled jobs: views that refresh themselves -------------------

    def refreshes(self, side, db):
        """[(name, runs, its query)] of the database's materialized views
        that refresh on a schedule (`REFRESH EVERY`): the only job
        ClickHouse runs by itself. `runs` is False for one stopped
        (`SYSTEM STOP VIEW`); a view kept by inserts is no job."""
        ask = {"d": self._d(side, db)}
        queries = {str(n): str(q or "") for n, q in self._rows(
            side, db, "select name, as_select from system.tables where"
                      " database = {d:String} and engine ="
                      " 'MaterializedView'", ask)}
        return [(str(v), str(st) != "Disabled", queries.get(str(v), ""))
                for v, st in self._rows(
                    side, db, "select view, status from"
                              " system.view_refreshes where database ="
                              " {d:String} order by view", ask)]

    def set_refresh(self, side, db, name, on):
        """Let a view refresh again, or stop it refreshing - on either
        side, as the cutover switches a job off on the source before it is
        frozen and on on the target at the flip."""
        client = self._client(side, db)
        try:
            client.command(f"system {'start' if on else 'stop'} view"
                           f" {self._qualified(side, db, name)}")
        finally:
            client.close()

    def every_table(self, side, db):
        """The names of every table in the database, the hop's or not: a
        job's text is read against all of them to tell the tables it
        writes that the hop takes from those it does not."""
        return [str(t) for (t,) in self._rows(
            side, db, "select name from system.tables where database ="
                      " {d:String} and not is_temporary",
            {"d": self._d(side, db)})]

    def functions(self, side):
        """The names of the SQL functions its users wrote."""
        return sorted(self._functions(side))

    # ---- the schema other engines are given, and made here ------------------

    def neutral_views(self, side, db):
        """[(name, the query)] of the views the hop takes, as ClickHouse
        keeps them. A materialized view is not here: it writes rows as
        they are inserted, which no other engine's view does."""
        return [(str(n), str(q)) for n, q in self._rows(
            side, db, "select name, as_select from system.tables where"
                      " database = {d:String} and engine = 'View' order by"
                      " name", {"d": self._d(side, db)})
            if self.takes(db, "view", str(n))]

    def neutral_column_rules(self, side, db, table):
        """{column: {"null", "default", "identity"}}: whether a column
        takes NULL, the default it is given when an insert leaves it out
        (a computed column is no default), and - none is numbered by the
        server - no identity."""
        return {str(n): {
            "null": _nullable(t),
            "default": str(expr) if kind == "DEFAULT" and expr else None,
            "identity": False}
            for n, t, kind, expr in self._rows(
                side, db, "select name, type, default_kind,"
                          " default_expression from system.columns where"
                          " database = {d:String} and table = {t:String}",
                {"d": self._d(side, db), "t": table})}

    def default_works(self, side, db, expr, typ=None):
        """Whether the server evaluates `expr` as it is - a constant or a
        function, never a column - asked as a read that writes nothing."""
        try:
            self.run_rule(side, None, f"select ({expr})")
            return True
        except Exception:  # noqa: BLE001 - not a default it takes
            return False

    #: the classes of value an index over a range can skip by (`minmax`);
    #: every other is skipped by a bloom filter
    ORDERED = ("integer", "decimal", "float", "timestamp", "date", "time",
               "boolean")

    def neutral_indexes(self, side, db, table):
        """[(name, unique, [columns], carried)] of the table's data-skipping
        indexes. ClickHouse has no secondary index that refuses a
        duplicate: none is unique. One over an expression is not carried
        as it is."""
        known = {n for n, _ in self.neutral_columns(side, db, table)}
        out = []
        for name, expr in self._rows(
                side, db, "select name, expr from system.data_skipping_indices"
                          " where database = {d:String} and table = {t:String}"
                          " order by name",
                {"d": self._d(side, db), "t": table}):
            cols = [c.strip().strip("`") for c in
                    str(expr).strip().strip("()").split(",")]
            plain = bool(cols) and all(c in known for c in cols)
            out.append((str(name), False, cols if plain else [], plain))
        return out

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        """A data-skipping index over the columns: a min-max range for
        numbers, dates and times, a bloom filter for the rest. It skips
        granules a read would not need; it enforces nothing, so a unique
        index of the source is made as one that is not."""
        from .. import canon
        have = dict(self.neutral_columns(side, db, table))
        ordered = all(canon.type_class(self.CANON_ENGINE, have.get(c, ""))
                      in self.ORDERED for c in columns)
        return (f"alter table {self._qualified(side, db, table)} add index"
                f" {self._q(name)} ({', '.join(self._q(c) for c in columns)})"
                f" type {'minmax' if ordered else 'bloom_filter(0.025)'}"
                " granularity 4")

    def execute_ddl(self, side, db, sql):
        """One statement that changes the schema of the target. An index
        added is then built for the rows the table already holds
        (`materialize index`), which `add index` alone does not do."""
        self._target_only(side, "change a schema")
        client = self._client(side, db)
        try:
            client.command(sql)
            added = re.match(
                r"\s*alter table (\S+) add index (`(?:[^`\\]|\\.)*`)", sql,
                re.I)
            if added:
                client.command(f"alter table {added.group(1)} materialize"
                               f" index {added.group(2)}",
                               settings={"mutations_sync": 2})
        finally:
            client.close()

    def create_missing(self, db, log=None):
        """The tables the target lacks, made before the table copier fills
        them: by the source's own definition (`_made_from_source`) - its
        engine, partitioning, order, time to live and settings - where the
        hop renames and maps nothing of the table and the definition does
        not name the source's replication path; the rest by the pair, as
        the copier builds a table from its columns."""
        log = log or (lambda m: None)
        made = self.prepare_target(db)
        if made:
            log(made)
        there = set(self.neutral_tables("dst", db))
        for t in self.neutral_tables("src", db):
            kind = self._engine_of("src", db, t)
            if t in there or self._renamed_or_mapped(db, t) \
                    or not kind.endswith("MergeTree") \
                    or kind.startswith("Replicated"):
                continue
            self._made_from_source(db, t)
            log(f"{t}: not on the target, made as the source made it")
        self._as_pair().create_missing(db, log)
