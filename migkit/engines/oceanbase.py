"""OceanBase as a side of a pair (backlog P0-CLOUDS), in both tenant modes.

MySQL mode (`OceanBaseEngine`) is a distributed service on MySQL's wire
(`distmysql`): the MySQL engine's connection, catalogue, reads and writes,
its digest worked out by OceanBase's SQL where proved on each table. Its
changes are its binlog service's: OceanBase writes no MySQL binlog of its
own, but its binlog service turns the tenant's commit log into one, which
OBProxy serves to a MySQL binlog reader as MySQL does (`SHOW MASTER
STATUS`, `SHOW BINARY LOGS`, the dump protocol) once the proxy is told
where the service is (`binlog_service_ip`). Reached through such a proxy,
the MySQL engine's own tail follows it. The service writes whole row
images (`binlog_row_image` FULL is asked of the tenant); the columns'
names are read from the catalogue as the reader needs them.

Oracle mode (`OceanBaseOracleEngine`) speaks Oracle's SQL over the same
wire: tables, columns and keys from Oracle's catalogue views (`ALL_TABLES`,
`ALL_TAB_COLUMNS`, `ALL_CONSTRAINTS`, the declared types as the Oracle
engine writes them), rows read and written through the one DB-API reader,
a schema's unquoted names in capitals as Oracle keeps them. Its rows are
folded here: Oracle's hash functions are not proved to render as migkit
does there. Its changes are not followed: OceanBase's binlog service
serves MySQL-mode tenants only, and an Oracle-mode tenant's changes leave
only through OceanBase's own change-data library, which has no reader
here. The community edition has no Oracle mode, so this side has not been
run against a server: what it does is OceanBase's documented SQL.
"""
from .base import Engine
from .dbapi import MARK, DbapiRows, FoldsToCapitals
from .distmysql import OnMySQLWire


class OceanBaseEngine(OnMySQLWire):
    CANON_ENGINE = "oceanbase"
    BRAND = ("oceanbase",)
    SAID = "OceanBase (MySQL mode)"
    #: its binlog service writes whole images; the names come from the
    #: catalogue
    BINLOG_FULL = ("binlog_row_image",)

    def _identity_probes(self):
        return ("select value from oceanbase.GV$OB_PARAMETERS where name ="
                " 'cluster_id' limit 1",
                "show parameters like 'cluster_id'",
                "select @@server_uuid")

    def change_point(self, side, db):
        try:
            return super().change_point(side, db)
        except (SystemExit, Exception) as e:  # noqa: BLE001 - said, with where
            raise SystemExit(
                "OceanBase answers no binlog position here"
                f" ({str(e).strip().splitlines()[0][:80] if str(e).strip() else type(e).__name__}):"
                " its binlog is written by its binlog service and served"
                " through OBProxy. Reach the tenant through a proxy that"
                " names the service (binlog_service_ip), with the binlog"
                " service running for the tenant, to follow its changes"
            ) from None

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "tables copied by key, digested by OceanBase's"
                            " SQL where proved on each table"}]
        if side == "src":
            items.append({"level": "pass", "scope": "reach",
                          "item": "changes followed through its binlog"
                                  " service",
                          "detail": "a MySQL binlog of the tenant's commit"
                                    " log, served through OBProxy"})
        return items


class OceanBaseOracleEngine(FoldsToCapitals, DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "oceanbase-oracle"
    SQL_DIALECT = "oracle"
    FOLDED_BECAUSE = ("has no hash of its own proved to render its rows as"
                      " migkit does")
    OVER_NETWORK = True
    PARAMSTYLE = "pyformat"
    LIMIT = "fetch"
    PADDED = ("CHAR", "NCHAR")
    SCALED = ("NUMBER",)

    def _connect(self, side, db):
        """A connection to the tenant (`user@tenant#cluster` through
        OBProxy), its session's times in UTC and its schema the hop's."""
        import pymysql
        ep = self.hop.source if side == "src" else self.hop.target
        conn = pymysql.connect(host=ep.host, port=int(ep.port or 2883),
                               user=ep.user, password=ep.password,
                               charset="utf8mb4", connect_timeout=15,
                               autocommit=False, **ep.mysql_tls())
        with conn.cursor() as cur:
            cur.execute("alter session set time_zone = '+00:00'")
            cur.execute("alter session set current_schema ="
                        f" {self._q(self._d(side, db))}")
        return conn

    def databases(self):
        return list(self.hop.databases)

    def neutral_tables(self, side, db):
        return [t for t in (self._said(n) for (n,) in self._rows(
            side, db, "select table_name from all_tables where owner ="
                      f" {MARK} order by 1", (self._owner(side, db),)))
            if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        from .oracle import DECLARED
        return [(self._said(n), t) for n, t in self._rows(
            side, db, f"select column_name, {DECLARED} from all_tab_columns"
                      f" where owner = {MARK} and table_name = {MARK}"
                      " order by column_id",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def neutral_key(self, side, db, table):
        return [self._said(n) for (n,) in self._rows(
            side, db, "select cc.column_name from all_constraints c join"
                      " all_cons_columns cc on cc.owner = c.owner and"
                      " cc.constraint_name = c.constraint_name where"
                      f" c.owner = {MARK} and c.table_name = {MARK} and"
                      " c.constraint_type = 'P' order by cc.position",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None,
                    "key": bool(self.neutral_key(side, db, t))}
                for t in self.neutral_tables(side, db)}

    def server_identity(self, side, db):
        got = self._rows(side, db, "select version() from dual")
        version = str(got[0][0]) if got else ""
        if "oceanbase" not in version.lower():
            raise SystemExit(
                f"the {'source' if side == 'src' else 'target'} is not an"
                f" OceanBase Oracle-mode tenant ({version[:60]})")
        ep = self.hop.source if side == "src" else self.hop.target
        return {"version": version, "id": f"{ep.host}:{ep.port}/{ep.user}"}

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

    def reach_items(self, side):
        items = [{"level": "warn", "scope": "reach",
                  "item": "copied and verified, not proved here",
                  "detail": "Oracle's catalogue and SQL over the tenant's"
                            " wire, rows folded here; the community edition"
                            " has no Oracle mode to prove it against"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": "an Oracle-mode tenant's changes leave"
                                    " only through OceanBase's own"
                                    " change-data library: move it while"
                                    " nothing writes, and prove it with"
                                    " migkit check"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")
