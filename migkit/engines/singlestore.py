"""SingleStore (formerly MemSQL) as a side of a pair (backlog
P0-CLOUDS.more): a distributed SQL database of its own on MySQL's wire.

The MySQL wire's engine is its connection, catalogue, reads and writes
(`distmysql.OnMySQLWire`). A table made on it is sharded by the first
column of its key, or kept keyless-sharded where it has none
(`decide.mpp_spread`). Rows go fastest through a pipeline that reads a
landing store (`stage: s3://bucket/prefix`) from every leaf at once, where
the account may create one and the table is empty (`decide.landing_load`);
the table copier otherwise, and said why. SingleStore keeps no binary log
and no change stream a client reads as a released feature: as a source
it is copied and checked, never followed (`decide.service_follow` holds
no rung, and the reason is said); as a target the pair's tail writes into
it by key.
"""
import uuid

from .. import decide
from .analyticdb import LandsRows, advice
from .distmysql import OnMySQLWire

#: why the ways below are owed a run (P0-CLOUDS.more)
UNPROVED_ON = "built; not run on a real SingleStore workspace"
#: the cells built here and run against no such service
UNPROVED_ITEMS = (
    "singlestore:tail.quiet "
    "assess.brands assess.capacity assess.engine-checks "
    "assess.preflight cutover.counters cutover.drain cutover.freeze "
    "cutover.read-only-target cutover.rollback "
    "move.create-missing move.large-values move.load-window "
    "move.partitioned move.sized move.table-copy operations.fencing "
    "schema.create schema.defaults schema.foreign-keys schema.indexes "
    "schema.routines schema.views scope.copy scope.counters scope.deep "
    "scope.listings scope.names scope.objects scope.ops scope.repair "
    "security.tls tail.added tail.ddl tail.follows tail.readded "
    "tail.renames tail.table-states verify.access.object-ownership "
    "verify.access.table-grants verify.confirm verify.delta "
    "verify.fence verify.identity.sequence-collision "
    "verify.identity.sequence-parity verify.movement.target-ahead "
    "verify.repair verify.repair-undo "
    "verify.structure.collection-options verify.structure.columns "
    "verify.structure.foreign-keys verify.structure.keys "
    "verify.structure.partitions verify.structure.triggers "
    "verify.structure.unvalidated-constraints verify.table verify.users "
    "verify.value.charset verify.value.collation verify.value.generated "
    "verify.value.narrowing verify.value.null-empty "
    "verify.value.one-plan verify.value.precision "
    "verify.value.rendering verify.value.timezone")


class SingleStoreEngine(LandsRows, OnMySQLWire):
    CANON_ENGINE = "singlestore"
    BRAND = ("memsql", "singlestore")
    SAID = "SingleStore"
    SERVICE = "singlestore-helios"
    DEFAULT_PORT = 3306

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
        return "ALL PRIVILEGES" in said or "CREATE PIPELINE" in said

    def landing_sql(self, db, table, names, url):
        """A pipeline over the landed object, run in the foreground to its
        end, then dropped."""
        import json
        opts = self._target_options()
        name = self._quoted(f"{table}_landed_{uuid.uuid4().hex[:8]}")
        where = url.split("://", 1)[1]
        config = json.dumps({k: opts[o] for k, o in (
            ("region", "stage_region"), ("endpoint_url", "stage_endpoint_url"))
            if opts.get(o)})
        shown = ", ".join(self._quoted(n) for n in names)
        return (f"create pipeline {name} as load data s3 '{where}' config"
                f" '{config}' credentials '{{}}' into table"
                f" {self._qualified('dst', db, table)} fields terminated by"
                " ',' optionally enclosed by '\"' escaped by '\\\\' null"
                f" defined by '\\\\N' ({shown})",
                f"start pipeline {name} foreground",
                f"drop pipeline {name}")

    def spread_of(self, db, table, key):
        """The table's shard key, as the climb chose it."""
        choice = decide.mpp_spread({"key": list(key), "rows": None,
                                    "replicated": True, "small": advice(
                                        self.SERVICE, "replicate_below_rows")})
        if choice.path == "hash":
            return "shard key (" + ", ".join(self._quoted(k) for k in key) \
                + ")"
        if choice.path == "replicate":
            return "reference"
        return "shard key ()"

    def neutral_create_sql(self, side, db, table, columns, key=()):
        sql = super().neutral_create_sql(side, db, table, columns, key)
        if side != "dst":
            return sql
        clause = self.spread_of(db, table, key)
        if clause == "reference":
            return sql.replace("create table", "create reference table", 1)
        head, _, tail = sql.rstrip().rstrip(";").rpartition(")")
        return f"{head}, {clause}){tail}"

    def follow_way(self, db):
        return decide.service_follow({
            "engine": self.SAID, "logical": False,
            "logical_why": f"{self.SAID} keeps no binary log and serves no"
                           " released change stream to a client"})

    def tail_start(self, db, token_path):
        """A source of this service is copied and checked, never followed:
        refused, with why."""
        choice = self.follow_way(db)
        if choice.path is None:
            raise SystemExit(f"{db}: no way to follow {self.SAID} holds:"
                             f" {choice.reason}")
        return super().tail_start(db, token_path)
