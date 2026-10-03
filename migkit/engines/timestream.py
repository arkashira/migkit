"""Amazon Timestream for LiveAnalytics as a side of a pair (backlog
P0-CLOUDS), through its query and write APIs.

A hop's database is a Timestream database (`timestream://region/db`),
a table one of its tables. A record is a row: its dimensions (text), its
measure's name, its time (nanoseconds) and its measures' values - a
multi-measure record's each a column, as `DESCRIBE` names them. A
record is kept by its dimensions, measure name and time, which is the
row's key. Rows are read by the service's SQL a page at a time (its
`NextToken`), in time order, and folded here: its SQL has no hash that
is proved to render a row as migkit does.

Written by `WriteRecords`, 100 records a call, each at a version above
any before it, so a record written again replaces itself; a record the
service refuses (outside the memory store's window, where magnetic store
writes are off) stops the write and says why. A table made from another
engine's rows needs a timestamp column named `time`: its text columns
are its dimensions and the rest its measures.

Timestream deletes no record, and keeps no change log: nothing can be
followed - a record written late, or rewritten at a higher version, lands
in the past where no watermark finds it - and a table is emptied by
making it again with its own settings. A Timestream move is a copy made
while nothing writes, proved by check. Timestream for InfluxDB is
InfluxDB, not this side.
"""
import time

from .base import Engine, NeutralCopier, Result

#: records one WriteRecords call takes, the service's most
RECORDS = 100
#: DESCRIBE's word for each part of a record
DIMENSION, MEASURE_NAME, TIMESTAMP = "DIMENSION", "MEASURE_NAME", "TIMESTAMP"
MEASURE_TYPES = {"integer": "BIGINT", "float": "DOUBLE",
                 "boolean": "BOOLEAN", "text": "VARCHAR",
                 "timestamp": "TIMESTAMP"}


def _lit(text):
    return "'" + str(text).replace("'", "''") + "'"


class TimestreamEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "timestream"
    FOLDED_BECAUSE = "has no hash of its own proved to render a record as"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False
    READS_BILLED = True

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _client(self, side, kind):
        held = self.__dict__.setdefault("_clients", {})
        if (side, kind) not in held:
            import boto3
            ep = self._ep(side)
            kw = {"region_name": ep.options.get("region") or ep.host or None}
            if ep.options.get("endpoint_url"):
                kw["endpoint_url"] = ep.options["endpoint_url"]
            if ep.user:
                kw.update(aws_access_key_id=ep.user,
                          aws_secret_access_key=ep.password)
            held[(side, kind)] = boto3.client(f"timestream-{kind}", **kw)
        return held[(side, kind)]

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        got = self._client("src", "write").list_databases()
        return sorted(d["DatabaseName"] for d in got.get("Databases", []))

    def _q(self, name):
        return '"' + str(name).replace('"', '""') + '"'

    def _qualified(self, side, db, table):
        return f"{self._q(self._d(side, db))}.{self._q(table)}"

    def _query(self, side, sql):
        """(columns [(name, type)], rows of values) of a query, every page
        of it."""
        client = self._client(side, "query")
        cols, out, token = None, [], None
        while True:
            got = client.query(QueryString=sql, **(
                {"NextToken": token} if token else {}))
            if cols is None:
                cols = [(c["Name"], c["Type"].get("ScalarType", "VARCHAR"))
                        for c in got.get("ColumnInfo", [])]
            for row in got.get("Rows", []):
                out.append([self._value(t, d) for (_, t), d
                            in zip(cols, row.get("Data", []))])
            token = got.get("NextToken")
            if not token:
                return cols or [], out

    @staticmethod
    def _value(scalar, datum):
        """A result's datum as the value it holds."""
        from .. import nanotime
        if datum.get("NullValue"):
            return None
        text = datum.get("ScalarValue")
        if text is None:
            return None
        t = str(scalar).upper()
        if t in ("BIGINT", "INTEGER"):
            return int(text)
        if t == "DOUBLE":
            return float(text)
        if t == "BOOLEAN":
            return str(text).lower() == "true"
        if t == "TIMESTAMP":
            return nanotime.parse(text)
        if t == "DATE":
            import datetime
            return datetime.date.fromisoformat(text)
        if t == "TIME":
            return nanotime.parse(text)
        return text

    # ---- what a table is ---------------------------------------------------

    def neutral_tables(self, side, db):
        out, token = [], None
        client = self._client(side, "write")
        while True:
            got = client.list_tables(DatabaseName=self._d(side, db),
                                     **({"NextToken": token} if token
                                        else {}))
            out += [t["TableName"] for t in got.get("Tables", [])]
            token = got.get("NextToken")
            if not token:
                break
        return sorted(t for t in out if self.in_scope(db, t))

    def _described(self, side, db, table):
        """[(column, type, what it is of a record)]."""
        held = self.__dict__.setdefault("_described_of", {})
        at = (side, self._d(side, db), table)
        if at not in held:
            cols, rows = self._query(side, "describe "
                                     + self._qualified(side, db, table))
            names = [n.lower() for n, _ in cols]
            got = []
            for r in rows:
                row = dict(zip(names, r))
                kind = next((v for k, v in row.items()
                             if "attribute" in k), "")
                got.append((row.get("column"), str(row.get("type")).lower(),
                            str(kind).upper()))
            held[at] = got
        return held[at]

    SHAPE_CACHES = ("_column_rules_of", "_ordered_of", "_described_of")

    def neutral_columns(self, side, db, table):
        return [(n, t) for n, t, _ in self._described(side, db, table)]

    def neutral_key(self, side, db, table):
        got = self._described(side, db, table)
        return ([n for n, _, k in got if k == DIMENSION]
                + [n for n, _, k in got if k == MEASURE_NAME]
                + [n for n, _, k in got if k == TIMESTAMP])

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    # ---- reading -----------------------------------------------------------

    def _select(self, side, db, table, columns, where=""):
        return (f"select {', '.join(self._q(n) for n, _ in columns)} from"
                f" {self._qualified(side, db, table)}"
                + (f" where {where}" if where else "") + " order by time")

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        _, rows = self._query(side, self._select(side, db, table, columns,
                                                 where or ""))
        for i in range(0, len(rows), size):
            yield rows[i:i + size]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or 1000, where):
            rows += batch
            if limit and len(rows) >= limit:
                return rows[:limit], None
        return rows, None

    def _match(self, side, db, table, key, values):
        """SQL naming one record by its key's values."""
        from .spanner import ns_of
        declared = dict(self.neutral_columns(side, db, table))
        parts = []
        for k, v in zip(key, values):
            q = self._q(k)
            if v is None:
                parts.append(f"{q} is null")
            elif declared.get(k) == "timestamp":
                parts.append(f"{q} = from_nanoseconds({ns_of(v)})")
            elif declared.get(k) in ("bigint", "double"):
                parts.append(f"{q} = {v!r}")
            else:
                parts.append(f"{q} = {_lit(v)}")
        return "(" + " and ".join(parts) + ")"

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        keys = list(keys)
        found = []
        for i in range(0, len(keys), 100):
            cond = " or ".join(self._match(side, db, table, key, k)
                               for k in keys[i:i + 100])
            if where:
                cond = f"({cond}) and ({where})"
            found += self.neutral_batches_all(side, db, table, columns, cond)
        return self._by_key_map(columns, key, found)

    def neutral_batches_all(self, side, db, table, columns, where):
        return self._query(side, self._select(side, db, table, columns,
                                              where))[1]

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        return canon.fold_batches(columns, self.neutral_batches(
            side, db, table, columns, 5000, where))

    # ---- writing -------------------------------------------------------------

    def _parts(self, side, db, table, columns):
        """(dimensions, measure name column or None, time column,
        measures) of a table's columns: DESCRIBE's where the table is
        there, else read off the columns' classes."""
        got = self._described(side, db, table) if table in \
            self.neutral_tables(side, db) else []
        if got:
            return ([n for n, _, k in got if k == DIMENSION],
                    next((n for n, _, k in got if k == MEASURE_NAME), None),
                    next((n for n, _, k in got if k == TIMESTAMP), "time"),
                    [(n, t) for n, t, k in got
                     if k not in (DIMENSION, MEASURE_NAME, TIMESTAMP)])
        classes = dict((c[0], c[1]) for c in columns)
        if classes.get("time") != "timestamp":
            raise SystemExit(f"{table} has no timestamp column named time,"
                             " which a Timestream record is kept at")
        dims = [n for n, c in classes.items() if c == "text"
                and n != "measure_name"]
        measures = [(n, MEASURE_TYPES[c].lower()) for n, c in classes.items()
                    if n not in dims and n not in ("time", "measure_name")
                    and c in MEASURE_TYPES]
        left = [n for n, c in classes.items() if n not in dims and n not in (
            "time", "measure_name") and c not in MEASURE_TYPES]
        if left:
            raise SystemExit(f"{table}: {', '.join(left)} holds values"
                             " Timestream keeps no measure of")
        return dims, ("measure_name" if "measure_name" in classes else None
                      ), "time", measures

    def neutral_create_sql(self, side, db, table, columns, key=()):
        import json
        dims, name, at, measures = self._parts(side, db, table, columns)
        return json.dumps({"table": table, "dimensions": dims,
                           "measure_name": name, "time": at,
                           "measures": measures})

    def neutral_create(self, side, db, table, columns, key=()):
        """The table, its memory and magnetic stores kept as the target
        side's options say (`retention`), magnetic store writes on, so a
        record older than the memory store's window is taken."""
        self._target_only(side, "create a table")
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        spec = self.neutral_create_sql(side, db, table, columns, key)
        keep = self._ep(side).options.get("retention") or {}
        self._client(side, "write").create_table(
            DatabaseName=self._d(side, db), TableName=table,
            RetentionProperties={
                "MemoryStoreRetentionPeriodInHours": int(
                    keep.get("memory_hours", 24)),
                "MagneticStoreRetentionPeriodInDays": int(
                    keep.get("magnetic_days", 3650))},
            MagneticStoreWriteProperties={"EnableMagneticStoreWrites": True})
        self.forget_shapes()
        return spec

    def _record(self, parts, table, names, row):
        from .spanner import ns_of
        dims, name_col, at, measures = parts
        values = dict(zip(names, row))
        when = values.get(at)
        if when is None:
            raise SystemExit(f"{table}: a row without its time")
        rec = {"Dimensions": [{"Name": d, "Value": str(values[d]),
                               "DimensionValueType": "VARCHAR"}
                              for d in dims if values.get(d) is not None],
               "MeasureName": str(values.get(name_col) or table)
               if name_col else table,
               "Time": str(ns_of(when)), "TimeUnit": "NANOSECONDS",
               "Version": time.time_ns() // 1000}
        out = []
        for n, t in measures:
            v = values.get(n)
            if v is None:
                continue
            if t == "timestamp":
                v = ns_of(v) // 10 ** 6
            elif t == "boolean":
                v = "true" if v else "false"
            out.append({"Name": n, "Value": repr(v) if isinstance(v, float)
                        else str(v), "Type": t.upper()})
        if measures and all(n.startswith("measure_value::")
                            for n, _ in measures):
            # a single-measure table's one value, under the column its
            # type names
            if len(out) != 1:
                raise SystemExit(f"{table}: a single-measure record holds"
                                 f" {len(out)} values")
            rec.update(MeasureValueType=out[0]["Type"],
                       MeasureValue=out[0]["Value"])
            return rec
        rec.update(MeasureValueType="MULTI", MeasureValues=out)
        return rec

    def neutral_write(self, side, db, table, columns, rows):
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        parts = self._parts(side, db, table, columns)
        records = [self._record(parts, table, names, r) for r in rows]
        client = self._client(side, "write")
        for i in range(0, len(records), RECORDS):
            try:
                client.write_records(DatabaseName=self._d(side, db),
                                     TableName=table,
                                     Records=records[i:i + RECORDS])
            except client.exceptions.RejectedRecordsException as e:
                why = (e.response.get("RejectedRecords") or [{}])[0]
                raise SystemExit(
                    f"{table}: the target refused"
                    f" {len(e.response.get('RejectedRecords') or [])}"
                    f" records: {why.get('Reason', '')[:150]}")
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        names, out = None, []
        for key, values in rows:
            row = {**values, **key}
            names = names or list(row)
            out.append([row.get(n) for n in names])
        if out:
            self.neutral_write(side, db, table, [(n, None) for n in names],
                               out)

    def _apply_deletes(self, side, db, table, keys):
        raise SystemExit(f"{table}: Timestream deletes no record")

    def neutral_empty(self, side, db, table, where=None):
        """The table made again with its own settings: Timestream deletes
        no record."""
        self._target_only(side, "empty a table")
        if where:
            raise SystemExit(f"{table}: Timestream deletes no record, so a"
                             " part of a table cannot be emptied")
        client = self._client(side, "write")
        name = self._d(side, db)
        got = client.describe_table(DatabaseName=name, TableName=table)["Table"]
        client.delete_table(DatabaseName=name, TableName=table)
        keep = {k: got[k] for k in ("RetentionProperties",
                                    "MagneticStoreWriteProperties", "Schema")
                if got.get(k)}
        client.create_table(DatabaseName=name, TableName=table, **keep)
        self.forget_shapes()
        return None

    # ---- the checks ----------------------------------------------------------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    #: how long a record is kept, whether an old one is taken, and what a
    #: table is partitioned by
    CRITICAL_PARAMS = ()

    def _settings(self, side, db):
        out = {}
        client = self._client(side, "write")
        for t in self.neutral_tables(side, db):
            got = client.describe_table(DatabaseName=self._d(side, db),
                                        TableName=t)["Table"]
            keep = got.get("RetentionProperties") or {}
            out[f"{t} memory hours"] = str(
                keep.get("MemoryStoreRetentionPeriodInHours"))
            out[f"{t} magnetic days"] = str(
                keep.get("MagneticStoreRetentionPeriodInDays"))
            out[f"{t} magnetic writes"] = str(
                (got.get("MagneticStoreWriteProperties") or {}).get(
                    "EnableMagneticStoreWrites"))
            out[f"{t} partition key"] = str(
                (got.get("Schema") or {}).get("CompositePartitionKey"))
        return out

    def check_params(self, db):
        """Each table's retention, whether it takes old records and what it
        is partitioned by, source against target: a shorter retention
        loses the records past it on its own."""
        try:
            src, dst = self._settings("src", db), self._settings("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("params", db, "error", "could not read the"
                           f" tables: {str(e).splitlines()[0][:90]}")]
        return self._param_result(db, src, dst, tuple(set(src) | set(dst)),
                                  "give the target's tables the source's"
                                  " retention and partition key")

    def snapshot_state(self, db, state_dir, kind="all"):
        """What the target held before a repair: each table's retention and
        partition key, and its records counted - the records a repair
        writes are kept by the repair's own record; Timestream's own API
        keeps no restore point of a table."""
        import json
        out = {"settings": self._settings("dst", db), "records": {}}
        for t in self.neutral_tables("dst", db):
            _, rows = self._query("dst", "select count(*) from"
                                         f" {self._qualified('dst', db, t)}")
            out["records"][t] = rows[0][0] if rows else None
        (state_dir / "dst-timestream.json").write_text(
            json.dumps(out, indent=2, sort_keys=True, default=str))

    def server_identity(self, side, db):
        ep = self._ep(side)
        out = {"name": f"{ep.options.get('region') or ep.host}"}
        try:
            got = self._client(side, "write").describe_database(
                DatabaseName=self._d(side, db))["Database"]
            out["id"] = str(got.get("Arn"))
        except Exception:  # noqa: BLE001 - a database not there yet
            out["absent"] = True
        return out

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "records read in time order and written by"
                            " version, compared record by record"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": "Timestream keeps no change log: move it"
                                    " while nothing writes"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


def _register():
    """What of the Timestream APIs is used (`tooluse`)."""
    from .. import tooluse
    unused = tooluse.UNUSED
    tooluse.register("timestream", licence="Apache-2.0", reads=(
        ("engines/timestream.py",),
        {("query", "Query"): "NextToken",
         ("write", "WriteRecords"): "write_records",
         ("write", "Version"): "Version",
         ("write", "MULTI"): "MULTI",
         ("write", "EnableMagneticStoreWrites"): "EnableMagneticStoreWrites",
         ("write", "CreateTable"): "create_table"}), options={
        ("write", "CreateBatchLoadTask"): (
            unused, "a batch load reads CSV files from S3, which a copy from"
                    " another server has none of, and reports its errors to"
                    " a bucket rather than per record"),
        ("query", "CreateScheduledQuery"): (
            unused, "a scheduled query writes into the same database it"
                    " reads, which a copy to another does not"),
    })


_register()
