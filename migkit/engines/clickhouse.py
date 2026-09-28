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
        wide = self._wide_times(side, db, table)
        sql = (f"select {', '.join(wide.get(n, self._q(n)) for n in names)}"
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

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        sql, params, key, names = self._read_query(side, db, table, columns,
                                                   after, limit, where)
        client = self._client(side, db)
        try:
            rows = self._values(columns, client.query(
                sql, parameters=params,
                query_formats=self.AS_BYTES).result_rows,
                self._wide_times(side, db, table))
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
                wide = self._wide_times(side, db, table)
                for block in stream:
                    yield self._values(columns, block, wide)
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
            # the key's values as they are, for the delete; the rows as
            # they are written, for the insert
            written = self._wall_clock(client, self._ticks(
                client, side, db, table, names, rows))
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
                          [[canon.sql_value(v) for v in r] for r in written],
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

    def _parts(self, side, db):
        """{table: {partition: signature}} of the active parts: a partition
        written, merged or mutated since has another signature."""
        out = {}
        for t, p, n, h in self._rows(
                side, db, "select table, partition_id, sum(rows),"
                          " sum(cityHash64(name)) from system.parts where"
                          " database = {d:String} and active group by"
                          " table, partition_id",
                {"d": self._d(side, db)}):
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
