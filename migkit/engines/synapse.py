"""Azure Synapse Analytics' dedicated SQL pool as a side of a pair
(backlog P0-CLOUDS.more): a massively parallel warehouse answering T-SQL,
not SQL Server's engine.

It is a warehouse as the others are (`warehouse._CatalogWarehouse`): its
catalogue read through `information_schema`, its rows compared by digests
its SQL works out, the pair's tail applied into it in numbered batches.
A table made on it is spread over the pool's sixty distributions by the
hash of its key, kept whole on every node where the catalogue gives the
service's bound and the table is under it, or round-robin where it has no
key (`decide.mpp_spread`). Rows go fastest by `COPY INTO` from a landing
store in Azure storage (`stage: https://<account>.blob.core.windows.net/
<container>/<prefix>`), where a writer for it is here, the account may run
bulk operations and the table is empty (`decide.landing_load`); the table
copier otherwise, and said why. A dedicated pool keeps no change data
capture or change tracking: as a source it is copied and checked, never
followed.
"""
import uuid

from .. import decide
from .analyticdb import LandsRows, advice
from .mssql import MSSQLEngine
from .warehouse import _CatalogWarehouse

#: why the ways below are owed a run (P0-CLOUDS.more)
UNPROVED_ON = "built; not run on a real Synapse dedicated SQL pool"
#: the cells built here and run against no such service
UNPROVED_ITEMS = (
    "assess.brands assess.engine-checks assess.handwork "
    "assess.preflight assess.versions cutover.rollback "
    "move.create-missing move.large-values move.partitioned "
    "move.sized move.table-copy schema.create "
    "schema.defaults schema.views scope.copy scope.deep scope.listings "
    "scope.names scope.objects scope.ops scope.repair "
    "verify.access.object-ownership verify.access.table-grants "
    "verify.movement.target-ahead verify.repair verify.repair-undo "
    "verify.structure.collection-options verify.structure.columns "
    "verify.structure.foreign-keys verify.structure.keys "
    "verify.structure.materialized-views verify.structure.objects "
    "verify.structure.row-security verify.table verify.tables")


def landing_writer(url):
    """Whether rows can be written to `url` from this machine: Azure
    storage blob and its client installed."""
    import importlib.util
    return ".core.windows.net/" in str(url or "") and \
        importlib.util.find_spec("azure.storage.blob") is not None


class SynapseEngine(LandsRows, _CatalogWarehouse):
    CANON_ENGINE = "synapse"
    SAID = "Synapse's dedicated SQL pool"
    SERVICE = "azure-synapse"
    DIGEST_DIALECT = "mssql"
    PARAMSTYLE = "pyformat"
    LIMIT = "top"
    DEFAULT_PORT = 1433
    LARGE_VALUES = MSSQLEngine.LARGE_VALUES
    _deep_partitions = MSSQLEngine._deep_partitions

    def _connect(self, side, db):
        import pymssql
        ep = self._endpoint(side)
        return pymssql.connect(server=ep.host, port=ep.port or 1433,
                               user=ep.user, password=ep.password,
                               database=self._database(side),
                               login_timeout=15)

    def _q(self, name):
        return "[" + str(name).replace("]", "]]") + "]"

    def _quoted(self, name):
        return self._q(name)

    def _scalar_of(self, side, db, sql):
        got = self._rows(side, db, sql)
        return got[0][0] if got else None

    def _run_target(self, db, statement):
        conn = self._connect("dst", db)
        try:
            conn.cursor().execute(statement)
            conn.commit()
        finally:
            conn.close()

    def _banner(self, side, db):
        return self._rows(side, db, "select @@version")[0][0]

    def server_identity(self, side, db):
        version, name = self._rows(side, db, "select @@version,"
                                   " db_name()")[0]
        return {"version": str(version or ""), "name": str(name or "")}

    def _landing_writable(self, url):
        return landing_writer(url)

    def accounts(self, side):
        from .. import users
        return users._ms_principals(self.hop, side)

    def check_params(self, db):
        def pull(side):
            try:
                rows = self._rows(side, db,
                                  "select name, cast(value_in_use as varchar(64))"
                                  " from sys.configurations order by name")
                return {r[0].strip(): r[1].strip() for r in rows if len(r) >= 2}
            except Exception:
                return {}
        return self._param_result(
            db, pull("src"), pull("dst"), (),
            "align server settings on the target")

    def settle_target(self, db, from_source=True):
        self._target_only("dst", "refresh statistics")
        conn = self._connect("dst", db)
        try:
            conn.cursor().execute("exec sp_updatestats")
            conn.commit()
        finally:
            conn.close()
        return "refreshed the target's statistics"

    def snapshot_state(self, db, state_dir, kind="all"):
        rows = self._rows(
            "dst", db,
            "select s.name + '.' + t.name,"
            " cast(ic.last_value as bigint) from sys.identity_columns ic"
            " join sys.tables t on t.object_id = ic.object_id"
            " join sys.schemas s on s.schema_id = t.schema_id order by 1")
        (state_dir / "dst-identity.txt").write_text("".join(
            f"{t}|{v}\n" for t, v in rows if v is not None))
        if kind == "sequences":
            return
        defs = self._rows(
            "dst", db,
            "select s.name + '.' + o.name, o.type_desc, m.definition"
            " from sys.sql_modules m join sys.objects o"
            " on o.object_id = m.object_id join sys.schemas s"
            " on s.schema_id = o.schema_id order by 1")
        (state_dir / "dst-schema.sql").write_text("".join(
            f"-- {name} ({what.lower()})\n{body}\ngo\n"
            for name, what, body in defs if body is not None))

    def _may_load(self, db):
        try:
            got = self._scalar_of("dst", db, "select count(*) from"
                                  " fn_my_permissions(null, 'DATABASE') where"
                                  " permission_name in ('ADMINISTER DATABASE"
                                  " BULK OPERATIONS', 'CONTROL')")
        except Exception:  # noqa: BLE001 - not read: said as not read
            return None
        return bool(got)

    def landing_sql(self, db, table, names, url):
        """`COPY INTO` the table from the landed object."""
        shown = ", ".join(self._q(n) for n in names)
        return (f"copy into {self._qualified('dst', db, table)} ({shown})"
                f" from '{url}' with (file_type = 'CSV', fieldterminator ="
                " ',', fieldquote = '\"', credential = (identity = 'Managed"
                " Identity'))",)

    def spread_of(self, db, table, key):
        """The table's `DISTRIBUTION`, as the climb chose it."""
        choice = decide.mpp_spread({"key": list(key), "rows": None,
                                    "nodes": 60, "replicated": True,
                                    "small": advice(self.SERVICE,
                                                    "replicate_below_rows")})
        if choice.path == "replicate":
            return "with (distribution = replicate)"
        if choice.path == "hash":
            return f"with (distribution = hash({self._q(key[0])}))"
        return "with (distribution = round_robin)"

    def neutral_create_sql(self, side, db, table, columns, key=()):
        sql = super().neutral_create_sql(side, db, table, columns, key)
        if side != "dst":
            return sql
        return f"{sql.rstrip().rstrip(';')} {self.spread_of(db, table, key)}"

    def follow_way(self, db):
        return decide.service_follow({
            "engine": self.SAID, "logical": False,
            "logical_why": f"{self.SAID} keeps no change data capture or"
                           " change tracking a client reads"})

    def tail_start(self, db, token_path):
        """A source of this service is copied and checked, never followed:
        refused, with why."""
        choice = self.follow_way(db)
        if choice.path is None:
            raise SystemExit(f"{db}: no way to follow {self.SAID} holds:"
                             f" {choice.reason}")
        return super().tail_start(db, token_path)
