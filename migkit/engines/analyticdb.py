"""Alibaba AnalyticDB as a side of a pair (backlog P0-CLOUDS.more): its
MySQL flavour on the MySQL wire, its PostgreSQL flavour - a descendant of
Greenplum - on PostgreSQL's.

Both are massively parallel warehouses: a table is spread over the
nodes by a distribution clause made with it (`decide.mpp_spread`, on the
table's key, its rows and the service's own advice in `data/managed.yaml`),
and bulk rows go fastest through an external table over object storage
the nodes read in parallel (`decide.landing_load`: a landing store named
as `stage: oss://bucket/prefix` or `s3://...`, a writer for it here, the
account allowed to make the external table, the target table empty so a
repeat cannot double rows) - the table copier otherwise, and said why.

AnalyticDB for MySQL keeps a binary log per table where the table is
made or altered with `BINLOG=true`: a source is followed through it only
where every table in scope keeps one (`decide.service_follow`), refused
with the tables named otherwise. AnalyticDB for PostgreSQL decodes no
logical replication for a client: as a source it is copied and checked,
never followed; as a target the pair's tail applies into it, a row whose
distribution key changed by a delete and an insert, since an UPDATE of a
distribution key is refused.

Everything else - catalogue, reads, writes, checks, accounts, snapshots -
is the wire's engine's, through the contracts `distmysql` and `pgwire`
keep.
"""
import csv
import io
import uuid

from .. import decide
from .distmysql import OnMySQLWire
from .pgwire import PgWireEngine

#: why the ways below are owed a run (P0-CLOUDS.more)
UNPROVED_ON = ("built; not run on a real AnalyticDB for MySQL or AnalyticDB"
               " for PostgreSQL instance")
#: the cells built here and run against no such service
UNPROVED_ITEMS = (
    "analyticdb-mysql:assess.brands analyticdb-mysql:assess.capacity "
    "analyticdb-mysql:assess.engine-checks "
    "analyticdb-mysql:assess.preflight "
    "analyticdb-mysql:cutover.counters analyticdb-mysql:cutover.drain "
    "analyticdb-mysql:cutover.freeze "
    "analyticdb-mysql:cutover.read-only-target "
    "analyticdb-mysql:cutover.rollback "
    ""
    "analyticdb-mysql:move.create-missing "
    "analyticdb-mysql:move.large-values "
    "analyticdb-mysql:move.load-window "
    "analyticdb-mysql:move.partitioned analyticdb-mysql:move.sized "
    "analyticdb-mysql:move.table-copy "
    "analyticdb-mysql:operations.fencing analyticdb-mysql:schema.create "
    "analyticdb-mysql:schema.defaults "
    "analyticdb-mysql:schema.foreign-keys "
    "analyticdb-mysql:schema.indexes analyticdb-mysql:schema.routines "
    "analyticdb-mysql:schema.views analyticdb-mysql:scope.copy "
    "analyticdb-mysql:scope.counters analyticdb-mysql:scope.deep "
    "analyticdb-mysql:scope.listings analyticdb-mysql:scope.names "
    "analyticdb-mysql:scope.objects analyticdb-mysql:scope.ops "
    "analyticdb-mysql:scope.repair analyticdb-mysql:security.tls "
    "analyticdb-mysql:tail.added analyticdb-mysql:tail.ddl "
    "analyticdb-mysql:tail.follows analyticdb-mysql:tail.readded "
    "analyticdb-mysql:tail.renames analyticdb-mysql:tail.table-states "
    "analyticdb-mysql:verify.access.object-ownership "
    "analyticdb-mysql:verify.access.table-grants "
    "analyticdb-mysql:verify.confirm analyticdb-mysql:verify.delta "
    "analyticdb-mysql:verify.fence "
    "analyticdb-mysql:verify.identity.sequence-collision "
    "analyticdb-mysql:verify.identity.sequence-parity "
    "analyticdb-mysql:verify.movement.target-ahead "
    "analyticdb-mysql:verify.repair analyticdb-mysql:verify.repair-undo "
    "analyticdb-mysql:verify.structure.collection-options "
    "analyticdb-mysql:verify.structure.columns "
    "analyticdb-mysql:verify.structure.foreign-keys "
    "analyticdb-mysql:verify.structure.keys "
    "analyticdb-mysql:verify.structure.partitions "
    "analyticdb-mysql:verify.structure.triggers "
    "analyticdb-mysql:verify.structure.unvalidated-constraints "
    "analyticdb-mysql:verify.table analyticdb-mysql:verify.users "
    "analyticdb-mysql:verify.value.charset "
    "analyticdb-mysql:verify.value.collation "
    "analyticdb-mysql:verify.value.generated "
    "analyticdb-mysql:verify.value.narrowing "
    "analyticdb-mysql:verify.value.null-empty "
    "analyticdb-mysql:verify.value.one-plan "
    "analyticdb-mysql:verify.value.precision "
    "analyticdb-mysql:verify.value.rendering "
    "analyticdb-mysql:verify.value.timezone "
    "analyticdb-postgres:assess.brands "
    "analyticdb-postgres:assess.engine-checks "
    "analyticdb-postgres:assess.handwork "
    "analyticdb-postgres:assess.preflight "
    "analyticdb-postgres:assess.versions "
    "analyticdb-postgres:cutover.counters "
    "analyticdb-postgres:cutover.freeze "
    "analyticdb-postgres:cutover.read-only-target "
    ""
    "analyticdb-postgres:move.create-missing "
    "analyticdb-postgres:move.sized analyticdb-postgres:move.table-copy "
    "analyticdb-postgres:operations.fencing "
    "analyticdb-postgres:schema.create "
    "analyticdb-postgres:schema.defaults "
    "analyticdb-postgres:schema.foreign-keys "
    "analyticdb-postgres:schema.indexes "
    "analyticdb-postgres:schema.views analyticdb-postgres:scope.copy "
    "analyticdb-postgres:scope.counters analyticdb-postgres:scope.deep "
    "analyticdb-postgres:scope.listings analyticdb-postgres:scope.names "
    "analyticdb-postgres:scope.objects analyticdb-postgres:scope.ops "
    "analyticdb-postgres:scope.repair analyticdb-postgres:security.tls "
    "analyticdb-postgres:tail.follows "
    "analyticdb-postgres:tail.table-states "
    "analyticdb-postgres:verify.access.object-ownership "
    "analyticdb-postgres:verify.access.sequence-grants "
    "analyticdb-postgres:verify.access.table-grants "
    "analyticdb-postgres:verify.confirm "
    "analyticdb-postgres:verify.delta analyticdb-postgres:verify.fence "
    "analyticdb-postgres:verify.identity.sequence-collision "
    "analyticdb-postgres:verify.identity.sequence-parity "
    "analyticdb-postgres:verify.movement.target-ahead "
    "analyticdb-postgres:verify.repair "
    "analyticdb-postgres:verify.repair-undo "
    "analyticdb-postgres:verify.rows-apart "
    "analyticdb-postgres:verify.structure.columns "
    "analyticdb-postgres:verify.structure.indexes "
    "analyticdb-postgres:verify.structure.keys "
    "analyticdb-postgres:verify.structure.objects "
    "analyticdb-postgres:verify.structure.schema-diff "
    "analyticdb-postgres:verify.table analyticdb-postgres:verify.users "
    "analyticdb-postgres:verify.value.charset "
    "analyticdb-postgres:verify.value.collation "
    "analyticdb-postgres:verify.value.generated "
    "analyticdb-postgres:verify.value.narrowing "
    "analyticdb-postgres:verify.value.null-empty "
    "analyticdb-postgres:verify.value.precision "
    "analyticdb-postgres:verify.value.rendering "
    "analyticdb-postgres:verify.value.timezone")

#: the landing stores a writer here reaches: object storage on the S3 API
#: (Alibaba OSS answers it too)
SCHEMES = ("s3://", "oss://")


def landing_writer(url):
    """Whether rows can be written to `url` from this machine: a scheme on
    the S3 API and its client installed."""
    if not str(url or "").startswith(SCHEMES):
        return False
    import importlib.util
    return importlib.util.find_spec("boto3") is not None


def landing_text(value):
    """One value as the landed CSV carries it: NULL as `\\N`, a backslash
    doubled so a value `\\N` is never read as NULL, bytes as hex."""
    if value is None:
        return "\\N"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return str(value).replace("\\", "\\\\")


def land_csv(url, name, rows, options, client=None):
    """Write `rows` as one CSV object under `url`; its URL. `options`: the
    target's `stage_endpoint_url` / `stage_region` where the store is not
    the machine's own."""
    scheme, _, rest = str(url).partition("://")
    bucket, _, prefix = rest.partition("/")
    buf = io.StringIO()
    out = csv.writer(buf, lineterminator="\n")
    for row in rows:
        out.writerow([landing_text(v) for v in row])
    key = "/".join(p for p in (prefix.strip("/"),
                               f"{name}-{uuid.uuid4().hex}.csv") if p)
    if client is None:
        import boto3
        kw = {k: options[o] for k, o in (("endpoint_url",
                                          "stage_endpoint_url"),
                                         ("region_name", "stage_region"))
              if options.get(o)}
        client = boto3.client("s3", **kw)
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode())
    return f"{scheme}://{bucket}/{key}"


def advice(service, what):
    """A documented bound the catalogue gives for `service`, or None."""
    from .. import managed
    got = managed.services().get(service)
    value = (got.limits if got else {}).get(what)
    return int(value) if isinstance(value, (int, float)) else None


class LandsRows:
    """A target whose own loader reads a landing store, ranked against
    the table copier for each table (`decide.landing_load`)."""
    #: the catalogue's row for the service (`data/managed.yaml`)
    SERVICE = ""

    def _target_options(self):
        return self.hop.target.options or {}

    def _landing(self):
        return self._target_options().get("stage") or ""

    def _landing_writable(self, url):
        return landing_writer(url)

    def _may_load(self, db):
        """Whether the target's account may make what the loader needs:
        True, False, or None where its grants were not read."""
        raise NotImplementedError

    def _table_rows(self, side, db, table):
        """Rows in `table` on `side`, counted."""
        return int(self._scalar_of(side, db, "select count(*) from "
                                   + self._qualified(side, db, table)) or 0)

    def load_way(self, db, table, log=None):
        """The `Choice` for loading `table`, recorded."""
        land = self._landing()
        facts = {"landing": land, "writer": bool(land) and
                 self._landing_writable(land), "may": None, "rows": None,
                 "empty": None}
        if land and facts["writer"]:
            facts["may"] = self._may_load(db)
            facts["rows"] = self._table_rows("src", db, table)
            facts["empty"] = not self._table_rows("dst", db, table)
        choice = decide.landing_load(facts)
        decide.record(self.hop, choice, log, work=f"the load of {table}",
                      way=choice.said, reason=choice.reason)
        return choice

    def _source_columns(self, db, table):
        return [c[0] for c in self.neutral_columns("src", db, table)]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        """The table through the target's own loader where the climb
        takes it, the parent's copier otherwise. The landed rows are
        loaded in one statement into an empty table: interrupted, the
        table is no longer empty and the next run's copier reconciles it
        by key, never loading the rows twice."""
        if self.load_way(db, tbl, log).path != "landing":
            return super().move_table(db, sch, tbl, chunk, ck, log)
        names = self._source_columns(db, tbl)
        rows = self._rows("src", db, "select " + ", ".join(
            self._quoted(n) for n in names) + " from "
            + self._qualified("src", db, tbl))
        url = land_csv(self._landing(), tbl, rows, self._target_options())
        for statement in self.landing_sql(db, tbl, names, url):
            self._run_target(db, statement)
        if log:
            log(f"{tbl}: {len(rows)} rows loaded from the landing store")
        return None


def _shown_create(text):
    return " ".join(str(text or "").lower().split())


class AnalyticDBMySQLEngine(LandsRows, OnMySQLWire):
    CANON_ENGINE = "analyticdb-mysql"
    SAID = "AnalyticDB for MySQL"
    SERVICE = "alibaba-analyticdb-mysql"

    def _scalar_of(self, side, db, sql):
        got = self._q(side, sql)
        return got[0][0] if got else None

    def _rows(self, side, db, sql, args=None):
        return [tuple(r) for r in self._q(side, sql, args)]

    def _quoted(self, name):
        return "`" + str(name).replace("`", "``") + "`"

    def _qualified(self, side, db, table):
        return f"{self._quoted(self._d(side, db))}.{self._quoted(table)}"

    def _run_target(self, db, statement):
        self._q("dst", statement)

    def _may_load(self, db):
        try:
            grants = " ".join(str(r[0]) for r in self._q("dst",
                                                         "show grants"))
        except Exception:  # noqa: BLE001 - not read: said as not read
            return None
        said = grants.upper()
        return "ALL PRIVILEGES" in said or "CREATE" in said

    def landing_sql(self, db, table, names, url):
        """An OSS external table over the landed object, its rows inserted
        into the table, the external table dropped."""
        import json
        ext = f"{table}_landed_{uuid.uuid4().hex[:8]}"
        cols = ", ".join(f"{self._quoted(n)} text" for n in names)
        props = json.dumps({"endpoint": self._target_options().get(
            "stage_endpoint_url", ""), "url": url, "delimiter": ",",
            "null_value": "\\N"})
        shown = ", ".join(self._quoted(n) for n in names)
        where = self._quoted(self._d("dst", db))
        return (f"create external table {where}.{self._quoted(ext)} ({cols})"
                f" engine='OSS' table_properties='{props}'",
                f"insert into {self._qualified('dst', db, table)} ({shown})"
                f" select {shown} from {where}.{self._quoted(ext)}",
                f"drop table {where}.{self._quoted(ext)}")

    def spread_of(self, db, table, key):
        """The table's distribution clause, as the climb chose it."""
        rows = None
        try:
            rows = int(self._scalar_of("src", db, "select table_rows from"
                                       " information_schema.tables where"
                                       f" table_schema = '{self._d('src', db)}'"
                                       f" and table_name = '{table}'"))
        except Exception:  # noqa: BLE001 - not read: the climb says so
            pass
        choice = decide.mpp_spread({"key": list(key), "rows": rows,
                                    "replicated": True, "small": advice(
                                        self.SERVICE, "replicate_below_rows")})
        if choice.path == "replicate":
            return "distributed by broadcast"
        if choice.path == "hash":
            return f"distributed by hash({self._quoted(key[0])})"
        return ""

    def neutral_create_sql(self, side, db, table, columns, key=()):
        sql = super().neutral_create_sql(side, db, table, columns, key)
        clause = self.spread_of(db, table, key) if side == "dst" else ""
        return f"{sql.rstrip().rstrip(';')} {clause}".rstrip() if clause \
            else sql

    def follow_way(self, db):
        """The climb over what the source's tables keep."""
        tables = [t for t in self.neutral_tables("src", db)]
        kept = 0
        for t in tables:
            got = self._q("src", f"show create table {self._qualified('src', db, t)}")
            kept += bool(got) and "binlog=true" in _shown_create(got[0][-1])
        return decide.service_follow({"engine": self.SAID,
                                      "tables": len(tables), "binlog": kept})

    def tail_start(self, db, token_path):
        choice = self.follow_way(db)
        if choice.path is None:
            raise SystemExit(f"{db}: no way to follow {self.SAID} holds:"
                             f" {choice.reason}")
        return super().tail_start(db, token_path)


class AnalyticDBPostgresEngine(LandsRows, PgWireEngine):
    CANON_ENGINE = "analyticdb-postgres"
    SAID = "AnalyticDB for PostgreSQL"
    SERVICE = "alibaba-analyticdb-postgres"
    SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "gp_toolkit",
                      "pg_aoseg", "pg_bitmapindex")

    def _scalar_of(self, side, db, sql):
        return self._scalar(side, db, sql)

    def _quoted(self, name):
        return self._q(name)

    def _run_target(self, db, statement):
        self.execute_ddl("dst", db, statement)

    def _may_load(self, db):
        try:
            got = self._scalar("dst", db, "select has_database_privilege("
                               "current_user, current_database(), 'CREATE')")
        except Exception:  # noqa: BLE001 - not read: said as not read
            return None
        return bool(got)

    def landing_sql(self, db, table, names, url):
        """A readable external table over the landed object (the service's
        OSS protocol), its rows inserted, the external table dropped."""
        ext = self._q(f"{table}_landed_{uuid.uuid4().hex[:8]}")
        cols = ", ".join(f"{self._q(n)} text" for n in names)
        shown = ", ".join(self._q(n) for n in names)
        endpoint = self._target_options().get("stage_endpoint_url", "")
        return (f"create readable external table {ext} ({cols}) location"
                f" ('{url} endpoint={endpoint}') format 'csv'"
                " (null '\\N' escape '\\')",
                f"insert into {self._qualified('dst', db, table)} ({shown})"
                f" select {shown} from {ext}",
                f"drop external table {ext}")

    def _segments(self):
        try:
            return int(self._scalar("dst", None, "select count(*) from"
                                    " gp_segment_configuration where"
                                    " content >= 0 and role = 'p'"))
        except Exception:  # noqa: BLE001 - not read: the climb says so
            return None

    def spread_of(self, db, table, key):
        """The table's `DISTRIBUTED` clause, as the climb chose it."""
        choice = decide.mpp_spread({"key": list(key), "nodes": self._segments(),
                                    "rows": None, "replicated": True,
                                    "small": advice(self.SERVICE,
                                                    "replicate_below_rows")})
        if choice.path == "replicate":
            return "distributed replicated"
        if choice.path == "hash":
            return "distributed by (" + ", ".join(self._q(k) for k in key) \
                + ")"
        return "distributed randomly"

    def neutral_create_sql(self, side, db, table, columns, key=()):
        sql = super().neutral_create_sql(side, db, table, columns, key)
        if side != "dst":
            return sql
        return f"{sql.rstrip().rstrip(';')} {self.spread_of(db, table, key)}"

    def follow_way(self, db):
        return decide.service_follow({
            "engine": self.SAID, "logical": False,
            "logical_why": f"{self.SAID} decodes no logical replication for"
                           " a client"})

    def tail_start(self, db, token_path):
        """A source of this service is copied and checked, never followed:
        refused, with why."""
        choice = self.follow_way(db)
        if choice.path is None:
            raise SystemExit(f"{db}: no way to follow {self.SAID} holds:"
                             f" {choice.reason}")
        return super().tail_start(db, token_path)
