"""Huawei GaussDB (openGauss and the services built on it) as a side of a
pair (backlog P0-CLOUDS), over PostgreSQL's wire.

openGauss began as PostgreSQL 9.2 and went its own way: it takes a
PostgreSQL client and keeps `information_schema` and `pg_catalog`, so it is
read and written through the one DB-API reader PostgreSQL's other kin use
(`pgwire`), with its own types (`TINYINT` of 0 to 255, `NUMBER`,
`VARCHAR2`, an A-compatible `DATE` that is `timestamp(0)`) named in `canon`.
Its password must be kept as MD5 for a PostgreSQL client to sign in
(`password_encryption_type` 0 or 1, and `md5` in `pg_hba.conf`): its own
SHA-256 method is one libpq does not speak.

A table's digest is worked out by the server in PostgreSQL's SQL where
the table's first rows are proved to render there as here
(`provedsql`); folded here otherwise.

Its changes are read by logical decoding through the SQL functions over
an ordinary connection: a slot of migkit's own (`migkit_<hop>_<db>`), made
before the copy so nothing written after it is lost, decoded by openGauss's
own JSON plugin (`mppdb_decoding`) or by `test_decoding` where the server
has it, peeked a batch at a time in whole transactions, and consumed only
up to what the tail has applied (the next read's position) - so a tail
that stops reads again from what it did not apply. Floats are decoded at
full precision (`extra_float_digits`), bytes as hex and times in UTC. The
slot holds the source's log until it is read: it is dropped when the hop
is done with it, and until then `stream_room` names what it holds.
"""
import re

from .base import Result
from .dbapi import MARK
from .pgwire import PgWireEngine
from .provedsql import ProvedDigest

#: what a decoded batch is read with
DECODE_OPTIONS = ("'include-xids', '1', 'skip-empty-xacts', '1'")
PLUGINS = ("mppdb_decoding", "test_decoding")


def lsn_int(text):
    """A log position (`0/3000060`) as one integer that orders as they do."""
    hi, _, lo = str(text or "0/0").partition("/")
    return (int(hi, 16) << 32) | int(lo or "0", 16)


class GaussEngine(ProvedDigest, PgWireEngine):
    CANON_ENGINE = "gaussdb"
    ENGINE_FAMILY = "postgres"
    #: the managed service's port (the community edition listens on 5432)
    DEFAULT_PORT = 8000
    PROVED_DIALECT = "postgres"
    checks = ("schema", "counts", "autoinc", "data")
    CHANGE_POINT_READS_ONLY = False
    #: a slot moves on only as far as what the tail applied
    READS_AHEAD = False
    #: `bpchar` padded as PostgreSQL's is, and openGauss's `nchar`
    PADDED = ("character", "bpchar", "char", "nchar")
    SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "dbe_perf",
                      "snapshot", "db4ai", "dbe_pldebugger", "dbe_pldeveloper",
                      "dbe_sql_util", "sqladvisor", "blockchain", "cstore",
                      "pkg_service", "pg_toast")

    # ---- the proved digest -------------------------------------------------

    def _proved_rows(self, side, db, sql):
        return self._rows(side, db, sql)

    def _proved_table(self, side, db, table):
        return self._qualified(side, db, table)

    def _proved_quote(self, name):
        return self._q(name)

    # ---- the server --------------------------------------------------------

    def _psql(self, side, db, sql, statement_timeout=0):
        """`sql`'s rows as `psql -At` prints them - a row a line, its
        columns joined by `|` - which the PostgreSQL hold reads
        (`freeze.PostgresHold`)."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall() if cur.description else []
            conn.commit()
        finally:
            conn.close()
        return "\n".join("|".join("" if v is None else str(v) for v in r)
                         for r in rows)

    def _dsn(self, side, db):
        return self._d(side, db)

    def server_identity(self, side, db):
        """What it says it is: its release, which must be openGauss's or
        GaussDB's, the database's size, and the cluster's own id where the
        server gives one."""
        got = self._rows(side, db, "select version(),"
                                   " pg_database_size(current_database()),"
                                   " pg_postmaster_start_time()::text")
        version, size, began = got[0]
        if not re.search(r"(?i)opengauss|gaussdb", str(version)):
            raise SystemExit(
                f"the {'source' if side == 'src' else 'target'} is not"
                f" GaussDB ({str(version)[:60]}): name its engine postgres")
        out = {"version": str(version).split(",")[0], "bytes": int(size or 0),
               "running": str(began)}
        try:
            out["id"] = str(self._rows(side, db, "select system_identifier"
                                                 " from pg_control_system()"
                                       )[0][0])
        except Exception:  # noqa: BLE001 - a release without it
            ep = self.hop.source if side == "src" else self.hop.target
            out["id"] = f"{ep.host}:{ep.port}"
        return out

    def stream_identity(self, side, db):
        return {"slot": self._slot(db), "id": self.server_identity(
            side, db).get("id")}

    # ---- sequences ----------------------------------------------------------

    def check_autoinc(self, db):
        """Each column numbered by a sequence (`nextval(...)` its default)
        with the target's sequence past the source's highest value: behind
        it, the application's next insert there takes a number a row
        already has."""
        out = []
        for t in self.neutral_tables("src", db):
            if not self.in_scope(db, t):
                continue
            rules = self.neutral_column_rules("src", db, t)
            sch, tbl = self._split(t)
            for col, _ in self.neutral_columns("src", db, t):
                default = self._rows(
                    "src", db, "select column_default from"
                               " information_schema.columns where"
                               f" table_schema = {MARK} and table_name ="
                               f" {MARK} and column_name = {MARK}",
                    (sch, tbl, col))
                text = str((default or [[None]])[0][0] or "")
                m = re.search(r"nextval\('([^']+)'", text)
                if not m or not rules.get(col, {}).get("identity", True):
                    continue
                seq = m.group(1)
                hi = self._rows("src", db, f"select max({self._q(col)}) from"
                                           f" {self._qualified('src', db, t)}"
                                )[0][0]
                try:
                    last, called = self._rows(
                        "dst", db, f"select last_value, is_called from {seq}")[0]
                except Exception as e:  # noqa: BLE001 - said as the error
                    out.append(Result("autoinc", f"{db}.{t}", "error",
                                      f"{col}: the target's sequence {seq}"
                                      f" could not be read:"
                                      f" {str(e).splitlines()[0][:80]}"))
                    continue
                nxt = int(last) + (1 if called else 0)
                if hi is None or nxt > int(hi):
                    out.append(Result("autoinc", f"{db}.{t}", "ok",
                                      f"{col}: the target's next number {nxt}"
                                      f" is past the source's highest {hi}"))
                else:
                    out.append(Result(
                        "autoinc", f"{db}.{t}", "diff",
                        f"{col}: the target's next number {nxt} is not past"
                        f" the source's highest {hi}", "",
                        f"select setval('{seq}', {int(hi)});"))
        return out or [Result("autoinc", db, "ok",
                              "no column numbered by a sequence in scope")]

    # ---- settings, the deep check and statistics -----------------------------

    #: what changes what a value means or how a statement reads: the
    #: database's compatibility (an A-compatible database stores '' as
    #: NULL), its text, its times and its numbers
    CRITICAL_PARAMS = ("datcompatibility", "sql_compatibility",
                       "server_encoding", "lc_collate", "lc_ctype",
                       "timezone", "datestyle", "intervalstyle",
                       "behavior_compat_options", "a_format_version",
                       "a_format_dev_version", "standard_conforming_strings",
                       "nls_timestamp_format", "max_identifier_length")

    def _settings(self, side, db):
        try:
            got = {str(n): str(v) for n, v in self._rows(
                side, db, "select name, setting from pg_settings")}
        except Exception as e:  # noqa: BLE001 - said, as the error
            return {self.UNREADABLE: str(e).strip().splitlines()[0][:120]}
        try:
            got["datcompatibility"] = str(self._rows(
                side, db, "select datcompatibility from pg_database where"
                          " datname = current_database()")[0][0])
        except Exception:  # noqa: BLE001 - a release without the column
            pass
        return got

    def check_params(self, db):
        """The server's settings and the database's own compatibility, both
        sides, through the report every engine's settings go through: a
        source made A-compatible keeps '' as NULL, and a target that is not
        keeps them apart."""
        return self._param_result(
            db, self._settings("src", db), self._settings("dst", db),
            self.CRITICAL_PARAMS,
            "make the target's database as the source's (its"
            " dbcompatibility) and set its settings as the source's")

    def _invalid_indexes(self, side, db):
        return self._scoped_rows(db, [
            (f"{s}.{t}", i) for s, t, i in self._rows(
                side, db, "select n.nspname, c.relname, ic.relname from"
                          " pg_index x join pg_class c on c.oid ="
                          " x.indrelid join pg_class ic on ic.oid ="
                          " x.indexrelid join pg_namespace n on n.oid ="
                          " c.relnamespace where not x.indisvalid")])

    def check_deep(self, db):
        """The source's indexes on the target, each over the same columns
        and as unique, none of the target's left invalid by a build that
        failed; and on the source, the log carrying rows a follow reads."""
        out, lacking = [], []
        try:
            for t in self.neutral_tables("src", db):
                if not self.in_scope(db, t):
                    continue
                have = {(u, tuple(c)) for _, u, c, _ in
                        self.neutral_indexes("dst", db, t)}
                lacking += [f"{t}.{name}" for name, u, c, _ in
                            self.neutral_indexes("src", db, t)
                            if (u, tuple(c)) not in have]
            bad = self._invalid_indexes("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [self._deep_error(db, "target", e)]
        out.append(Result(
            "deep", f"{db} indexes", "diff" if lacking or bad else "ok",
            (f"{len(lacking)} of the source's indexes are not on the target"
             f" ({', '.join(lacking[:4])})" if lacking else
             f"indexes left invalid on the target: "
             f"{', '.join(f'{t}.{i}' for t, i in bad[:4])}" if bad else
             "every index of the source's is on the target, valid"), "",
            "create them on the target before the cutover" if lacking
            else "rebuild them (REINDEX)" if bad else ""))
        try:
            level = str(self._rows("src", db, "show wal_level")[0][0])
        except Exception as e:  # noqa: BLE001 - said, as the error
            out.append(self._deep_error(db, "source", e))
        else:
            if level != "logical":
                out.append(Result(
                    "deep", f"{db} source log", "warn",
                    f"the source's log carries no rows (wal_level is"
                    f" {level}): its changes cannot be followed", "",
                    "wal_level = logical in the instance's parameters (the"
                    " instance restarts to take it)"))
        return out

    def settle_target(self, db, from_source=True):
        """Every table the load wrote analysed, so the planner reads it as
        it now is rather than as an empty table."""
        tables = [t for t in self.neutral_tables("dst", db)
                  if self.in_scope(db, t)]
        if not tables:
            return None
        conn = self._connect("dst", db)
        try:
            cur = conn.cursor()
            for t in tables:
                cur.execute(f"analyze {self._qualified('dst', db, t)}")
            conn.commit()
        finally:
            conn.close()
        return f"statistics gathered on {len(tables)} tables (ANALYZE)"

    # ---- the target's state before a repair ----------------------------------

    def _sequence_values(self, side, db):
        """{schema.sequence: (last value, called)} of the sequences in the
        schemas of the tables the hop takes."""
        schemas = {self._split(t)[0] for t in self.neutral_tables(side, db)
                   if self.in_scope(db, t)}
        out = {}
        for sch, seq in self._rows(
                side, db, "select n.nspname, c.relname from pg_class c join"
                          " pg_namespace n on n.oid = c.relnamespace where"
                          " c.relkind = 'S'"):
            if sch not in schemas:
                continue
            last, called = self._rows(
                side, db, f"select last_value, is_called from"
                          f" {self._q(sch)}.{self._q(seq)}")[0]
            out[f"{sch}.{seq}"] = (int(last), bool(called))
        return out

    def snapshot_state(self, db, state_dir, kind="all"):
        """Each sequence of the target where it stands, which a rollback
        sets back (`restore_sequences`); the rows a repair writes are kept
        by the repair's own record."""
        lines = [f"{name}|{last}|{'t' if called else 'f'}" for name,
                 (last, called) in sorted(self._sequence_values(
                     "dst", db).items())]
        (state_dir / "dst-gauss-sequences.txt").write_text(
            "\n".join(lines) + ("\n" if lines else ""))

    def restore_sequences(self, db, state_dir):
        path = state_dir / "dst-gauss-sequences.txt"
        if not path.exists():
            return []
        out = []
        for line in path.read_text().splitlines():
            if line:
                name, last, called = line.rsplit("|", 2)
                sch, _, seq = name.partition(".")
                literal = f"{self._q(sch)}.{self._q(seq)}".replace("'", "''")
                out.append(f"select setval('{literal}', {int(last)},"
                           f" {'true' if called == 't' else 'false'});")
        return out

    def apply(self, db, action):
        """A sequence is the target's own, set here; rows go through the
        pair."""
        if action.kind == "sequences":
            for statement in action.statements:
                self._psql("dst", db, statement)
            return None
        return super().apply(db, action)

    # ---- logical decoding ---------------------------------------------------

    def _slot(self, db):
        name = re.sub(r"[^a-z0-9_]", "_", f"migkit_{self.hop.name}_{db}"
                      .lower())
        return name[:63]

    def own_slots(self, db):
        return [self._slot(db)]

    def _slot_row(self, db):
        got = self._rows("src", db, "select plugin, restart_lsn::text from"
                                    " pg_replication_slots where slot_name ="
                                    f" {MARK}", (self._slot(db),))
        return got[0] if got else None

    def _now_lsn(self, side, db):
        """The log's end now: openGauss spells it as 9.2 did, a later
        release as PostgreSQL 10 does."""
        from .. import decide

        def run(choice):
            fn = ("pg_current_xlog_location()" if choice.rung.name == "xlog"
                  else "pg_current_wal_lsn()")
            return str(self._rows(side, db, f"select {fn}::text")[0][0])
        return self._attempt("the log's end", (
            decide.Rung("xlog", "pg_current_xlog_location"),
            decide.Rung("wal", "pg_current_wal_lsn")), run)[1]

    def _plugin(self, db):
        """The decoding plugin: the slot's own where it is there; the
        side's `decoding`, or openGauss's own JSON plugin and then
        `test_decoding`, where a slot is to be made - each tried, the next
        taken where the server has not the first."""
        got = self._slot_row(db)
        if got:
            return str(got[0])
        said = self.hop.source.options.get("decoding")
        return str(said) if said else None

    def _make_slot(self, db):
        """The slot, made where it is not there; its plugin."""
        from .. import decide
        got = self._slot_row(db)
        if got:
            return str(got[0])
        said = self.hop.source.options.get("decoding")
        ways = tuple(decide.Rung(p, f"decoded by {p}") for p in
                     ((said,) if said else PLUGINS))

        def run(choice):
            self._rows("src", db,
                       "select * from pg_create_logical_replication_slot("
                       f"{MARK}, {MARK})", (self._slot(db), choice.rung.name))
            return choice.rung.name
        return self._attempt("the change slot", ways, run)[1]

    def change_point(self, side, db):
        """The slot, made now - before the copy - and where it begins."""
        self._refuse_unkeyed(db, "a change the log carries for such a table"
                                 " names no row another server can find")
        self._make_slot(db)
        row = self._slot_row(db)
        return {"slot": self._slot(db), "lsn": str(row[1])}

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        return {"lsn": self._now_lsn(side, db)}

    @staticmethod
    def position_reached(have, want):
        if not have or not want:
            return None
        return lsn_int(have.get("lsn")) >= lsn_int(want.get("lsn"))

    def position_lost(self, side, db, token):
        if token and self._slot_row(db) is None:
            return ("the slot migkit reads the source's changes through is"
                    " gone: what changed since is carried by nothing")
        return None

    def stream_room(self, side, db, token):
        """The log the slot holds for the tail, in bytes: kept until it is
        read, however long that takes - the source's disk is what it
        costs."""
        try:
            got = self._rows(side, db, "select pg_xlog_location_diff("
                                       "pg_current_xlog_location(),"
                                       " restart_lsn) from"
                                       " pg_replication_slots where"
                                       f" slot_name = {MARK}",
                             (self._slot(db),))
        except Exception:  # noqa: BLE001 - for /metrics only
            return None
        return {"held_bytes": int(got[0][0])} if got else None

    def _session(self, cur):
        for sql in ("set extra_float_digits = 3", "set bytea_output = 'hex'",
                    "set timezone = 'UTC'", "set datestyle = 'ISO'"):
            cur.execute(sql)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Whole transactions out of the slot, in commit order: what the
        tail applied (the position it hands back) consumed first, then a
        batch peeked - which a stopped tail reads again."""
        from .. import pgslot
        if token is None:
            return [], self.change_point(side, db)
        slot = self._slot(db)
        plugin = self._plugin(db) or PLUGINS[0]
        keys = {}
        row = self._slot_row(db)
        if row is None:
            raise SystemExit(self.position_lost(side, db, token))
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._session(cur)
            if lsn_int(token.get("lsn")) > lsn_int(row[1]):
                cur.execute("select count(*) from"
                            " pg_logical_slot_get_changes(%s, %s, null, "
                            + DECODE_OPTIONS + ")", (slot, token["lsn"]))
            # the log's end before the peek: a peek that finds nothing has
            # applied everything up to it
            end = self._now_lsn(side, db)
            cur.execute("select location::text, xid, data from"
                        " pg_logical_slot_peek_changes(%s, null, %s, "
                        + DECODE_OPTIONS + ")", (slot, int(limit)))
            rows = cur.fetchall()
            conn.commit()
        finally:
            conn.close()
        out, txn, at = [], [], None
        for location, xid, data in rows:
            text = str(data or "")
            if text.startswith("BEGIN"):
                txn = []
                continue
            if text.startswith("COMMIT"):
                out += txn
                txn, at = [], str(location)
                continue
            parsed = (pgslot.parse_line(text) if plugin == "test_decoding"
                      else mppdb_line(text))
            if parsed is None or parsed["op"] == "truncate":
                if parsed is not None:
                    raise SystemExit(
                        f"{parsed['table']} was emptied on the source, which"
                        " its log carries as no row: move the table again")
                continue
            table = self._named(db, _Named(parsed["table"]))
            if table is None:
                continue
            if table not in keys:
                keys[table] = self.neutral_key("src", db, table)
            parsed["table"] = table
            txn.append(pgslot.change(parsed, keys[table], txn=int(xid)))
        if at is None and not rows:
            # nothing decoded before the log's end as it was read: every
            # transaction up to it has been applied
            at = end
        return out, {"slot": slot, "lsn": at or token.get("lsn")}

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "tables copied by key, digested by the server"
                            " where its rendering is proved on each table"}]
        if side == "src":
            items.append({"level": "pass", "scope": "reach",
                          "item": "changes followed by logical decoding",
                          "detail": "a slot of migkit's own, read in whole"
                                    " transactions"})
        return items


class _Named:
    """A decoded row's table name, as `_named` asks for it."""

    def __init__(self, table):
        self.table = table


def mppdb_line(text):
    """One line of openGauss's `mppdb_decoding` as `pgslot.parse_line`
    gives a line of `test_decoding`'s: its table, operation, and old and
    new tuples of (name, type, raw text, quoted). Its values are SQL
    literals - a string quoted with its quotes doubled, `null` bare - and
    an UPDATE carries its key before in `old_keys_*`."""
    import json
    text = str(text or "").strip()
    if not text.startswith("{"):
        return None
    msg = json.loads(text)
    op = str(msg.get("op_type") or "").lower()
    if op not in ("insert", "update", "delete"):
        return None

    def fields(prefix):
        names = msg.get(f"{prefix}_name") or []
        types = msg.get(f"{prefix}_type") or []
        vals = msg.get(f"{prefix}_val") or []
        out = []
        for n, t, v in zip(names, types, vals):
            raw = "null" if v is None else str(v)
            quoted = raw.startswith("'") and raw.endswith("'") and \
                len(raw) >= 2
            if quoted:
                raw = raw[1:-1].replace("''", "'")
            out.append((n, t, raw, quoted))
        return out
    new = fields("columns") if op != "delete" else []
    old = fields("old_keys")
    return {"table": msg.get("table_name"), "op": op, "old": old,
            "new": new}
