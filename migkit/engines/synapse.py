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
from .warehouse import _CatalogWarehouse

#: why the ways below are owed a run (P0-CLOUDS.more)
UNPROVED_ON = "built; not run on a real Synapse dedicated SQL pool"
#: the cells built here and run against no such service
UNPROVED_ITEMS = (
    "assess.brands assess.engine-checks assess.handwork "
    "assess.preflight assess.versions "
    "move.create-missing move.sized move.table-copy schema.create "
    "schema.defaults schema.views scope.copy scope.deep scope.listings "
    "scope.names scope.objects scope.ops scope.repair "
    "verify.access.object-ownership verify.access.table-grants "
    "verify.movement.target-ahead verify.repair verify.repair-undo "
    "verify.structure.collection-options verify.structure.columns "
    "verify.structure.foreign-keys verify.structure.keys "
    "verify.structure.materialized-views verify.structure.objects "
    "verify.structure.row-security verify.table verify.tables")


class SynapseEngine(LandsRows, _CatalogWarehouse):
    CANON_ENGINE = "synapse"
    SAID = "Synapse's dedicated SQL pool"
    SERVICE = "azure-synapse"
    DIGEST_DIALECT = "mssql"
    PARAMSTYLE = "pyformat"
    LIMIT = "top"
    DEFAULT_PORT = 1433

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
        """A writer for an Azure storage landing: its client installed and
        the URL in Azure storage's form."""
        import importlib.util
        return ".core.windows.net/" in str(url) and \
            importlib.util.find_spec("azure.storage.blob") is not None

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
