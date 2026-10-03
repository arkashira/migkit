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
from .base import Engine, Result
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
    checks = ("schema", "counts", "autoinc", "data")
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
            if self.in_scope(db, t)]

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
        """A sequence is the target's own, set by its own statements; rows
        go through the pair."""
        if action.kind == "sequences":
            conn = self._connect("dst", db)
            try:
                for statement in action.statements:
                    conn.cursor().execute(statement.rstrip(";"))
                conn.commit()
            finally:
                conn.close()
            return None
        return self._as_pair().apply(db, action)

    # ---- sequences ----------------------------------------------------------
    #
    # `last_number` is where a sequence starts its next cache, past every
    # value it handed out: the target's at or past the source's hands out
    # nothing the source did.

    def _sequences(self, side, db):
        """{sequence: its next cache's start} of the schema's sequences the
        hop carries (`Engine.takes`)."""
        return {self._said(n): int(last) for n, last in self._rows(
            side, db, "select sequence_name, last_number from all_sequences"
                      f" where sequence_owner = {MARK}",
            (self._owner(side, db),))
            if self.takes(db, "sequence", self._said(n))}

    def _restart_sql(self, db, name, value):
        return (f"alter sequence {self._qualified('dst', db, name)} restart"
                f" start with {int(value)}")

    def check_autoinc(self, db):
        """Every sequence of the source's on the target, at or past where
        the source's next cache begins."""
        try:
            src, dst = self._sequences("src", db), self._sequences("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("autoinc", db, "error", "could not read the"
                           f" sequences: {str(e).splitlines()[0][:90]}")]
        short = [n for n, v in sorted(src.items())
                 if dst.get(n) is None or dst[n] < v]
        if not short:
            return [Result("autoinc", f"{db} parity", "ok",
                           f"{len(src)} sequences at or past the source's")]
        return [Result(
            "autoinc", f"{db} parity", "diff",
            "; ".join(f"{n} source next {src[n]} target"
                      f" {dst.get(n, 'missing')}" for n in short[:8]), "",
            "; ".join(self._restart_sql(db, n, src[n]) for n in short[:4]
                      if n in dst))]

    # ---- settings, the deep check and statistics -------------------------

    #: what changes what a value becomes or how text compares
    CRITICAL_PARAMS = ("nls_characterset", "nls_nchar_characterset",
                       "nls_length_semantics", "nls_comp", "nls_sort",
                       "nls_date_format", "nls_timestamp_format",
                       "nls_timestamp_tz_format", "nls_numeric_characters",
                       "dbtimezone", "ob_compatibility_mode",
                       "lower_case_table_names")

    def _settings(self, side, db):
        """The database's language settings and the tenant's own
        parameters, as OceanBase keeps them."""
        try:
            out = {str(n).lower(): str(v) for n, v in self._rows(
                side, db, "select parameter, value from"
                          " nls_database_parameters")}
            out["dbtimezone"] = str(self._rows(
                side, db, "select dbtimezone from dual")[0][0])
        except Exception as e:  # noqa: BLE001 - said, as the error
            return {self.UNREADABLE: str(e).strip().splitlines()[0][:120]}
        try:
            for n, v in self._rows(side, db, "select name, value from"
                                             " gv$ob_parameters"):
                out.setdefault(str(n).lower(), str(v))
        except Exception as e:  # noqa: BLE001 - half a reading, said
            out[self.UNREADABLE] = ("the tenant's parameters could not be"
                                    f" read: {str(e).splitlines()[0][:60]}")
        return out

    def check_params(self, db):
        return self._param_result(
            db, self._settings("src", db), self._settings("dst", db),
            self.CRITICAL_PARAMS,
            "set the target tenant's parameters as the source's (its"
            " character set needs the tenant made again)")

    def check_deep(self, db):
        """Objects a load left invalid, and constraints left disabled or
        not validated, on the target: the tenant uses a table under them
        without a word."""
        owner = self._owner("dst", db)
        try:
            invalid = [r for r in self._rows(
                "dst", db, "select object_type || ' ' || object_name,"
                           " object_name, object_type from all_objects"
                           f" where owner = {MARK} and status = 'INVALID'"
                           " order by 1", (owner,))
                if self.takes(db, str(r[2]), self._said(r[1]))]
            loose = [r for r in self._rows(
                "dst", db, "select constraint_name || ' on ' || table_name"
                           " || ' (' || status || ')', table_name from"
                           f" all_constraints where owner = {MARK} and"
                           " (status = 'DISABLED' or validated ="
                           " 'NOT VALIDATED') order by 1", (owner,))
                if self.in_scope(db, self._said(r[1]))]
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target objects", "error",
                           "could not read the target's objects:"
                           f" {str(e).splitlines()[0][:90]}")]
        return [Result("deep", f"{db} target objects",
                       "diff" if invalid else "ok",
                       f"{len(invalid)} invalid:"
                       f" {', '.join(r[0] for r in invalid[:6])}"
                       if invalid else "no invalid object on the target",
                       "", "compile them again" if invalid else ""),
                Result("deep", f"{db} target constraints",
                       "diff" if loose else "ok",
                       f"{len(loose)} not enforced:"
                       f" {', '.join(r[0] for r in loose[:6])}"
                       if loose else "every constraint enabled and validated",
                       "", "alter table ... enable validate constraint ..."
                       if loose else "")]

    def settle_target(self, db, from_source=True):
        """Statistics gathered for the schema the load wrote, as the
        tenant's own package gathers them."""
        conn = self._connect("dst", db)
        try:
            self._run(conn.cursor(), "begin dbms_stats.gather_schema_stats("
                                     f"{MARK}); end;",
                      (self._owner("dst", db),))
            conn.commit()
        finally:
            conn.close()
        return "statistics gathered for the target's schema"

    def snapshot_state(self, db, state_dir, kind="all"):
        """Each sequence's next cache start where the target stands, which
        a rollback puts back (`restore_sequences`); the rows a repair
        writes are kept by the repair's own record."""
        (state_dir / "dst-ob-sequences.txt").write_text("".join(
            f"{n}|{v}\n" for n, v in sorted(self._sequences("dst",
                                                            db).items())))

    def restore_sequences(self, db, state_dir):
        path = state_dir / "dst-ob-sequences.txt"
        if not path.exists():
            return []
        out = []
        for line in path.read_text().splitlines():
            if line:
                name, value = line.rsplit("|", 1)
                out.append(self._restart_sql(db, name, int(value)) + ";")
        return out

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
