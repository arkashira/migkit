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
from .. import pieces
from .base import Engine, NeutralCopier, Result

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
    SQL_DIALECT = "clickhouse"
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
            if not self.hop.excluded(db, t)]

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
        from .. import canon
        defs = []
        for col in columns:
            t = canon.ddl_type("clickhouse", col[1], col[2])
            # an Array cannot be Nullable: its NULLs are counted before the
            # move (`canon.unfit`), as an empty array is what it writes
            defs.append(f"{self._q(col[0])} "
                        + (t if col[0] in key or t.startswith("Array(")
                           else f"Nullable({t})"))
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

    def run_rule(self, side, db, sql):
        return self._rows(side, db, sql, settings={"readonly": 1})

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

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

    def check_data(self, db, table=None, stream=None):
        """Every table's partitions compared by fingerprints each server
        computes; only a table where they differ - or cannot be computed on
        a side - is compared row by row through the pair."""
        tables = [table] if table else self.neutral_tables("src", db)
        same, rest = [], []
        for t in tables:
            if self.hop.masks(db, t):
                # each server fingerprints the values as they are: the
                # pair takes the source through the hop's transforms
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
                                   " fingerprint equal on both servers"))
                if stream:
                    stream(f"{t}: ok")
            else:
                rest.append(t)
        out = list(same)
        if rest:
            pair = self._as_pair()
            for t in rest:
                out += pair.check_data(db, t, stream)
        return out

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
            if self.hop.excluded(db, t):
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
