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
from .. import pieces
from .base import Engine, Result
from .dbapi import MARK, DbapiRows, FoldsToCapitals
from .distmysql import OnMySQLWire

#: why the ways below are owed a run: either mode's widest value by its
#: version, the Oracle mode's catalogue, partitions, sequences and
#: settings (P0-CLOUDS.native), proved on fakes and on the parent's
#: sandbox only
UNPROVED_ON = "built; not run on a real OceanBase tenant"
#: the ways built here and run against no such server: the whole
#: list, the matrix reads it from this source (`capabilities`)
UNPROVED_ITEMS = (
    "oceanbase:tail.quiet "
    "oceanbase:assess.brands oceanbase:assess.capacity "
    "oceanbase:assess.engine-checks oceanbase:assess.preflight "
    "oceanbase:cutover.counters oceanbase:cutover.drain "
    "oceanbase:cutover.freeze oceanbase:cutover.read-only-target "
    "oceanbase:cutover.rollback oceanbase:move.create-missing "
    "oceanbase:move.load-window oceanbase:move.partitioned "
    "oceanbase:move.sized oceanbase:operations.fencing "
    "oceanbase:schema.defaults oceanbase:schema.foreign-keys "
    "oceanbase:schema.indexes oceanbase:schema.routines "
    "oceanbase:schema.views oceanbase:scope.copy oceanbase:scope.counters "
    "oceanbase:scope.deep oceanbase:scope.listings oceanbase:scope.names "
    "oceanbase:scope.objects oceanbase:scope.ops oceanbase:scope.repair "
    "oceanbase:scope.row-filter oceanbase:tail.added oceanbase:tail.ddl "
    "oceanbase:tail.follows oceanbase:tail.readded oceanbase:tail.renames "
    "oceanbase:tail.table-states oceanbase:verify.access.object-ownership "
    "oceanbase:verify.access.table-grants oceanbase:verify.confirm "
    "oceanbase:verify.delta oceanbase:verify.fence "
    "oceanbase:verify.identity.sequence-collision "
    "oceanbase:verify.identity.sequence-parity "
    "oceanbase:verify.movement.target-ahead oceanbase:verify.repair "
    "oceanbase:verify.repair-undo "
    "oceanbase:verify.structure.collection-options "
    "oceanbase:verify.structure.columns "
    "oceanbase:verify.structure.foreign-keys "
    "oceanbase:verify.structure.keys "
    "oceanbase:verify.structure.partitions "
    "oceanbase:verify.structure.triggers "
    "oceanbase:verify.structure.unvalidated-constraints "
    "oceanbase:verify.table oceanbase:verify.value.charset "
    "oceanbase:verify.value.collation oceanbase:verify.value.generated "
    "oceanbase:verify.value.narrowing oceanbase:verify.value.null-empty "
    "oceanbase:verify.value.one-plan oceanbase:verify.value.precision "
    "oceanbase:verify.value.rendering oceanbase:verify.value.timezone "
    "oceanbase-oracle:assess.engine-checks "
    "oceanbase-oracle:cutover.freeze "
    "oceanbase-oracle:cutover.read-only-target "
    "oceanbase-oracle:move.sized oceanbase-oracle:move.table-copy "
    "oceanbase-oracle:schema.create oceanbase-oracle:scope.copy "
    "oceanbase-oracle:scope.counters oceanbase-oracle:scope.deep "
    "oceanbase-oracle:scope.listings oceanbase-oracle:scope.names "
    "oceanbase-oracle:scope.objects oceanbase-oracle:scope.repair "
    "oceanbase-oracle:scope.row-filter "
    "oceanbase-oracle:verify.identity.sequence-parity "
    "oceanbase-oracle:verify.repair oceanbase-oracle:verify.repair-undo "
    "oceanbase-oracle:verify.structure.objects "
    "oceanbase-oracle:verify.table "
    "oceanbase:move.large-values "
    "oceanbase-oracle:move.large-values "
    "oceanbase-oracle:move.partitioned "
    "oceanbase-oracle:verify.structure.partitions")


class OceanBaseEngine(OnMySQLWire):
    CANON_ENGINE = "oceanbase"
    BRAND = ("oceanbase",)
    SAID = "OceanBase (MySQL mode)"
    #: its binlog service writes whole images; the names come from the
    #: catalogue
    BINLOG_FULL = ("binlog_row_image",)

    def piece_limits(self, side, db):
        """The packet read off the tenant, as on MySQL, and the widest
        value the tenant's version stores (`decide.oceanbase_value_bound`):
        OceanBase's large types hold less than MySQL's."""
        from .. import decide
        got = list(super().piece_limits(side, db))
        rows = self._q(side, "select version()")
        most = decide.oceanbase_value_most(decide.oceanbase_value_bound(
            {"version": decide.ob_version(rows[0][0] if rows else "")}))
        if most:
            got.append(pieces.Limit("OceanBase's widest value", most, "row",
                                    read=False))
        return got

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
    #: large values: a LOB's length read by the server without reading
    #: it - a character LOB's at the four bytes a character may take, never
    #: less than it stores - and each value read whole, as the tenant's SQL
    #: reads no part of a LOB wider than a string
    LARGE_VALUES = pieces.Sql(
        "sized by the server and read whole, under the widest value the tenant stores",
        length={"blob": "dbms_lob.getlength({c})",
                "clob": "4 * dbms_lob.getlength({c})",
                "nclob": "4 * dbms_lob.getlength({c})"},
        types=("blob", "clob", "nclob"))

    def piece_limits(self, side, db):
        """The widest value the tenant's version stores
        (`decide.oceanbase_value_bound`), its documented quota."""
        from .. import decide
        got = self._rows(side, db, "select version() from dual")
        most = decide.oceanbase_value_most(decide.oceanbase_value_bound(
            {"version": decide.ob_version(got[0][0] if got else "")}))
        return [pieces.Limit("OceanBase's widest value", most, "row",
                             read=False)] if most else []

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
        from .. import verifiers
        return verifiers.compare_schema(self._as_pair(), db)

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
        return [self._deep_partitions(db),
                Result("deep", f"{db} target objects",
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

    def partition_scheme(self, side, db):
        """{table: {"m": method, "p": {partition: its high value}}} of the
        schema's partitioned tables, from Oracle's catalogue views as the
        tenant keeps them (`ALL_PART_TABLES`, `ALL_TAB_PARTITIONS`), the
        partitions in their order."""
        out = {}
        for t, meth, part, high in self._rows(
                side, db, "select p.table_name, k.partitioning_type,"
                          " p.partition_name, p.high_value from"
                          " all_tab_partitions p join all_part_tables k on"
                          " k.owner = p.table_owner and k.table_name ="
                          f" p.table_name where p.table_owner = {MARK}"
                          " order by p.table_name, p.partition_position",
                (self._owner(side, db),)):
            name = self._said(t)
            if self.in_scope(db, name):
                out.setdefault(name, {"m": str(meth), "p": {}})["p"][
                    str(part)] = str(high or "")
        return out

    def _deep_partitions(self, db):
        """Each partitioned table's method on both sides, and each of its
        partitions held to its own row count, read with `PARTITION (p)`
        (`Engine._partitions_verdict`)."""
        try:
            src = self.partition_scheme("src", db)
            dst = self.partition_scheme("dst", db)
            scheme, tables = [], {}
            for t in sorted(set(src) | set(dst)):
                s, d = src.get(t), dst.get(t)
                if not s or not d:
                    scheme.append(f"{t}: partitioned on the"
                                  f" {'source' if s else 'target'} only")
                    continue
                if s["m"] != d["m"]:
                    scheme.append(f"{t}: partitioned by {s['m']} on the"
                                  f" source, {d['m']} on the target")
                tables[t] = tuple(
                    {p: {"rows": int(self._rows(
                        side, db, f"select count(*) from {self._q(t)}"
                                  f" partition ({self._q(p)})")[0][0]),
                         "digest": None, "bound": e["p"][p]}
                     for p in e["p"]}
                    for side, e in (("src", s), ("dst", d)))
            return self._partitions_verdict(db, tables, scheme, "MAXVALUE")
        except Exception as e:  # noqa: BLE001 - said, and the rest runs
            return Result("deep", f"{db} partitions", "error",
                          "could not read the partitions:"
                          f" {str(e).splitlines()[-1][:120] if str(e) else type(e).__name__}")

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
