"""Warehouses as one side of a pair (backlog 33): Amazon Redshift,
Snowflake and Google BigQuery.

A hop's database is a schema (a dataset, in BigQuery); the database to
connect to is the endpoint's `database` option (the project, in BigQuery).
Tables, columns and keys are read from the warehouse's own
`information_schema`, rows through its DB-API driver, and compared through
the in-process renderer every engine without a canonical SQL rendering
shares. Rows are written as every DB-API engine writes them - a key's
rows deleted and inserted again in one transaction, which converges on a
restart - except into BigQuery, which takes them through a load job.

Loading each warehouse from staged Parquet through its own bulk command,
the faster way for a large table, is still to come.

Written 2026-09-25 without a warehouse to run it against: each needs an
account this machine has none of.
"""
from .base import Engine
from .dbapi import MARK, DbapiRows, FoldsToCapitals

#: the declared type from `information_schema.columns`, with the numbers a
#: counterpart needs, in the standard's spelling
DECLARED = ("case when data_type in ('numeric', 'decimal', 'NUMBER')"
            " and numeric_precision is not null then data_type || '('"
            " || numeric_precision || ',' || coalesce(numeric_scale, 0)"
            " || ')'"
            " when character_maximum_length is not null and"
            " character_maximum_length > 0 then data_type || '('"
            " || character_maximum_length || ')'"
            " else data_type end")


class _Warehouse(DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " a warehouse one nobody has run")
    OVER_NETWORK = True
    PARAMSTYLE = "pyformat"
    LIMIT = "limit"

    def _endpoint(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _database(self, side):
        name = (self._endpoint(side).options or {}).get("database")
        if not name:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `database`, the database to connect"
                             " to; the hop's databases are its schemas")
        return name

    def databases(self):
        return list(self.hop.databases)

    def _qualified(self, side, db, table):
        return f"{self._q(self._d(side, db))}.{self._q(table)}"

    def _create_name(self, side, db, table):
        return self._qualified(side, db, str(table).split(".")[-1])

    def _catalog(self, side, db, what):
        """`information_schema.<what>`, as this warehouse names it."""
        return f"information_schema.{what}"

    def neutral_tables(self, side, db):
        return [t for (t,) in self._rows(
            side, db, "select table_name from"
                      f" {self._catalog(side, db, 'tables')} where"
                      f" table_schema = {MARK} and table_type ="
                      f" {MARK} order by 1",
            (self._d(side, db), self.BASE_TABLE))
            if not self.hop.excluded(db, t)]

    #: what `information_schema.tables` calls a table
    BASE_TABLE = "BASE TABLE"

    def neutral_columns(self, side, db, table):
        return [(n, t) for n, t in self._rows(
            side, db, f"select column_name, {DECLARED} from"
                      f" {self._catalog(side, db, 'columns')} where"
                      f" table_schema = {MARK} and table_name = {MARK}"
                      " order by ordinal_position",
            (self._d(side, db), table))]

    def neutral_key(self, side, db, table):
        """The declared primary key. A warehouse does not enforce it, so
        two rows can share it; the pair's check counts rows as well."""
        return [n for (n,) in self._rows(
            side, db, "select k.column_name from"
                      f" {self._catalog(side, db, 'table_constraints')} c"
                      f" join {self._catalog(side, db, 'key_column_usage')}"
                      " k on k.constraint_name = c.constraint_name and"
                      " k.table_schema = c.table_schema and k.table_name ="
                      " c.table_name where c.constraint_type ="
                      f" 'PRIMARY KEY' and c.table_schema = {MARK} and"
                      f" c.table_name = {MARK} order by k.ordinal_position",
            (self._d(side, db), table))]

    @staticmethod
    def _column_tail(rule, style):
        """Only `not null`: a default is an expression of the engine it
        came from, which a warehouse need not parse, and an identity is
        the source's to keep."""
        return " not null" if rule and rule.get("null") is False else ""

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)


class RedshiftEngine(_Warehouse):
    """Amazon Redshift, through the PostgreSQL driver it speaks to."""
    CANON_ENGINE = "redshift"

    def _connect(self, side, db):
        import psycopg2
        ep = self._endpoint(side)
        return psycopg2.connect(host=ep.host, port=ep.port or 5439,
                                user=ep.user, password=ep.password,
                                dbname=self._database(side),
                                connect_timeout=15)


class SnowflakeEngine(FoldsToCapitals, _Warehouse):
    """Snowflake. The endpoint's `account` option is the account
    identifier; `warehouse` and `role`, where given, are used as they
    are. Unquoted names are kept in capitals, as Oracle's are."""
    CANON_ENGINE = "snowflake"
    #: a CHAR is a VARCHAR here, kept without padding
    PADDED = ()
    SCALED = ("NUMBER", "DECIMAL", "NUMERIC")

    def _connect(self, side, db):
        try:
            import snowflake.connector
        except ImportError:
            raise SystemExit("the Snowflake driver is not installed:"
                             " pip install snowflake-connector-python")
        ep = self._endpoint(side)
        opts = ep.options or {}
        if not opts.get("account"):
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `account`, the Snowflake account"
                             " identifier")
        kw = {k: opts[k] for k in ("warehouse", "role") if opts.get(k)}
        return snowflake.connector.connect(
            account=opts["account"], user=ep.user, password=ep.password,
            database=self._database(side), login_timeout=15, **kw)

    def neutral_tables(self, side, db):
        return [t for t in (self._said(n) for (n,) in self._rows(
            side, db, "select table_name from information_schema.tables"
                      f" where table_schema = {MARK} and table_type ="
                      f" {MARK} order by 1",
            (self._owner(side, db), self.BASE_TABLE)))
            if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        """`NUMBER` at scale 0 is Snowflake's integer, and its driver
        returns it as one."""
        out = []
        for n, t in self._rows(
                side, db, f"select column_name, {DECLARED} from"
                          " information_schema.columns where table_schema ="
                          f" {MARK} and table_name = {MARK}"
                          " order by ordinal_position",
                (self._owner(side, db), self._q(table)[1:-1])):
            if t.upper().startswith("NUMBER(") and t.endswith(",0)"):
                t = "INTEGER"
            out.append((self._said(n), t))
        return out

    def neutral_key(self, side, db, table):
        """From `show primary keys`: Snowflake's `information_schema` has
        no key columns."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(f"show primary keys in table"
                        f" {self._qualified(side, db, table)}")
            names = [c[0] for c in cur.description]
            rows = cur.fetchall()
        finally:
            conn.close()
        at, seq = names.index("column_name"), names.index("key_sequence")
        return [self._said(r[at]) for r in sorted(rows, key=lambda r: r[seq])]


class BigQueryEngine(_Warehouse):
    """Google BigQuery. The endpoint's `database` option is the project,
    a hop's databases its datasets, and `location` the datasets' region
    where it is not the default; `credentials_file` a service account's
    key, where the machine's own sign-in is not the one to use.

    Rows go in through a load job rather than one insert statement per
    row, which BigQuery runs as a job each: a batch's keys are deleted,
    then the batch is loaded. The two are not one transaction; a batch cut
    off between them is missing until the copy goes on, and a restart
    writes it again."""
    CANON_ENGINE = "bigquery"

    def _client(self, side):
        try:
            from google.cloud import bigquery
        except ImportError:
            raise SystemExit("the BigQuery driver is not installed:"
                             " pip install google-cloud-bigquery")
        opts = self._endpoint(side).options or {}
        kw = {"project": self._database(side)}
        if opts.get("location"):
            kw["location"] = opts["location"]
        if opts.get("credentials_file"):
            from google.oauth2 import service_account
            kw["credentials"] = (service_account.Credentials
                                 .from_service_account_file(
                                     opts["credentials_file"]))
        return bigquery.Client(**kw)

    def _connect(self, side, db):
        from google.cloud.bigquery import dbapi
        return dbapi.connect(client=self._client(side))

    def _q(self, name):
        return "`" + str(name).replace("\\", "\\\\").replace("`", "\\`") + "`"

    def _catalog(self, side, db, what):
        return f"{self._q(self._d(side, db))}.INFORMATION_SCHEMA.{what.upper()}"

    def neutral_create_sql(self, side, db, table, columns, key=()):
        """A key is taken only as one BigQuery does not enforce."""
        sql = super().neutral_create_sql(side, db, table, columns, key)
        return sql[:-1] + " not enforced)" if key else sql

    def neutral_columns(self, side, db, table):
        """`data_type` carries its own numbers here (`NUMERIC(10, 2)`,
        `STRING(20)`); there are no separate columns for them."""
        return [(n, t) for n, t in self._rows(
            side, db, "select column_name, data_type from"
                      f" {self._catalog(side, db, 'columns')} where"
                      f" table_name = {MARK} order by ordinal_position",
            (table,))]

    def neutral_write(self, side, db, table, columns, rows):
        import datetime
        import json

        from google.cloud import bigquery

        from .. import canon, streamout
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        key = self.neutral_key(side, db, table)
        if key and all(k in names for k in key):
            at = [names.index(k) for k in key]
            one = "(" + " and ".join(f"{self._q(k)} = {MARK}"
                                     for k in key) + ")"
            conn = self._connect(side, db)
            try:
                cur = conn.cursor()
                for i in range(0, len(rows), self.DELETE_BATCH):
                    part = rows[i:i + self.DELETE_BATCH]
                    self._run(cur, f"delete from"
                                   f" {self._qualified(side, db, table)}"
                                   " where " + " or ".join([one] * len(part)),
                              [canon.sql_value(r[j]) for r in part
                               for j in at])
            finally:
                conn.close()
        client = self._client(side)
        ref = f"{self._database(side)}.{self._d(side, db)}.{table}"
        def jsonable(v):
            # an instant as its UTC reading: a DATETIME column refuses an
            # offset, and a TIMESTAMP reads one without it as UTC
            if isinstance(v, datetime.datetime) and v.tzinfo:
                v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return streamout.jsonable(v)
        # JSON as a load job reads it: bytes as base64, times as ISO 8601,
        # exact numbers as their text
        records = [json.loads(json.dumps(
            {n: v for n, v in zip(names, r) if v is not canon.ABSENT},
            default=jsonable)) for r in rows]
        job = client.load_table_from_json(
            records, ref, job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"))
        job.result()
        return len(rows)
