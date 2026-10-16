"""CockroachDB as a side of a pair (backlog ER4), over PostgreSQL's wire.

Rows are read and written as any PostgreSQL-wire side's (`pgwire`), a
batch written by `UPSERT` - CockroachDB's own statement, one write per key
rather than a read and a write - and a table emptied in pieces, since one
statement that deletes a whole table can outgrow what a transaction may
hold. A keyless table's rows are keyed by a hidden `rowid` no other server
has, so its changes cannot be placed anywhere: such a table is copied and
compared, and its follow refused by name.

Its changes come from a changefeed: streamed to migkit's own query
(`EXPERIMENTAL CHANGEFEED FOR ... WITH updated, resolved`), which makes
nothing on the cluster - no job, no sink - and ends with the query; or
from a Kafka topic a changefeed the operator made publishes to
(`changes: {servers: ..., topics: [...]}`), in the same JSON. Either way
the change-topic reader (`changetopic`) puts them in commit order by each
change's HLC timestamp and releases them at each resolved timestamp, and
each changed row is read back `AS OF SYSTEM TIME` that resolved timestamp -
exact whatever the JSON made of a decimal or bytes, and held to what the
feed said of it. A read and a commit are on one clock, so a change the
copy's read already saw is known exactly (`mark_covers`).

A changefeed needs rangefeeds on (`kv.rangefeed.enabled`, a cluster
setting migkit does not change: `doctor` and the DBA's script say it).
"""
import json
import re
import time

from .. import pieces
from . import hooks
from .base import Result
from .dbapi import MARK
from .pgwire import PgWireEngine

#: statements a batch is written in, rows each: one UPSERT of this many
#: keeps a transaction well under what CockroachDB lets one hold
UPSERT_ROWS = 500
#: rows a delete takes out at a time where a table is emptied
DELETE_ROWS = 1000
#: what CockroachDB says of a read older than the garbage collector keeps
GC_PASSED = ("gc threshold", "gc_threshold", "must be after replica gc")


#: built on fakes; owed a run on a CockroachDB cluster (ER4)
UNPROVED_ON = ("built; not run on a real CockroachDB cluster, only on its"
               " single-node container and on fakes: the accounts, the"
               " cluster settings compared, the export and import through"
               " a stage, the snapshot's mark and large values read in"
               " pieces")
UNPROVED_ITEMS = (
    "scope.repair scope.counters scope.objects scope.names "
    "verify.access.object-ownership verify.access.sequence-grants "
    "verify.access.table-grants verify.identity.sequence-collision "
    "verify.identity.sequence-parity verify.movement.target-ahead "
    "verify.structure.foreign-keys verify.structure.indexes "
    "verify.structure.materialized-views verify.structure.schema-diff "
    "verify.structure.triggers verify.structure.unvalidated-constraints "
    "verify.value.charset verify.value.collation verify.value.generated "
    "verify.value.null-empty verify.value.precision verify.value.rendering "
    "verify.value.timezone verify.delta verify.confirm verify.repair "
    "verify.repair-undo verify.rows-apart verify.table assess.versions "
    "assess.brands assess.preflight assess.handwork schema.foreign-keys "
    "schema.views schema.routines move.sized move.create-missing "
    "cutover.counters operations.fencing verify.users move.bulk "
    "move.large-values cutover.rollback")


def account_steps_by(src, dst, passwords, make, member, grant, said):
    """(steps, skipped) of `Engine.account_steps` for an engine whose
    accounts are made by statements: `make(name, account, secret)` gives
    (run, undo, given, shown) or None where no password is known,
    `member(role, name)` and `grant(privilege, database, table, name)`
    (run, undo). Roles go before users, so a role is there when a user is
    given it; `said` is why an account is skipped."""
    steps, skipped = [], []
    passwords = passwords or {}

    def step(key, run, undo, made=False, given=False, shown=None):
        steps.append({"key": key, "shown": shown or run, "run": run,
                      "undo": undo, "made": made, "given": given})
    for key, a in sorted(src.items(), key=lambda kv: (
            kv[0].startswith("user:"), kv[0])):
        name = key.partition(":")[2]
        have = dst.get(key) or {"roles": [], "grants": []}
        if key not in dst:
            got = make(name, a, passwords.get(name) or passwords.get(key))
            if got is None:
                skipped.append((key, said))
                continue
            run, undo, given, shown = got
            step(key, run, undo, made=True, given=given, shown=shown)
        for r in a.get("roles", []):
            if r not in have["roles"]:
                step(key, *member(r, name))
        for g in a.get("grants", []):
            if g not in have["grants"]:
                priv, _, on = g.partition(" ON ")
                where, _, table = on.partition(".")
                step(key, *grant(priv, where, table, name))
    return steps, skipped


class SinklessFeed:
    """A changefeed streamed to a query: one lane, whose position is a
    resolved timestamp - started again from one with `cursor`, the feed
    sends every change after it again. The query stays open from one read
    to the next; a read from another position starts it again there."""
    KIND = "changefeed"
    LANE = "feed"

    def __init__(self, engine, db):
        self.eng, self.db = engine, db
        self.conn = self.gen = None
        self.pos = None
        self.last = None

    def lanes(self):
        return [self.LANE]

    def ends(self):
        """Now, as the cluster's own clock says - the feed begun from it
        sends what commits after."""
        self.last = self.eng._now("src", self.db)
        return {self.LANE: self.last}

    def _open(self, cursor):
        import psycopg
        self.close()
        tables = self.eng.neutral_tables("src", self.db)
        if not tables:
            raise SystemExit(f"{self.db}: no table in scope to follow")
        ep = self.eng.hop.source
        self.conn = psycopg.connect(
            host=ep.host, port=ep.port or self.eng.DEFAULT_PORT,
            user=ep.user, password=ep.password, dbname=self.db,
            connect_timeout=15, autocommit=True, **ep.libpq_tls())
        hlc_text = hooks.ask("changetopic", "hlc_text")
        names = ", ".join(f"{self.eng._q(self.db)}."
                          f"{self.eng._qualified('src', self.db, t)}"
                          for t in tables)
        sql = (f"experimental changefeed for {names} with updated,"
               " resolved = '1s', min_checkpoint_frequency = '1s',"
               f" cursor = '{hlc_text(cursor)}'")
        try:
            self.gen = self.conn.cursor().stream(sql)
            first = next(self.gen)
        except Exception as e:  # noqa: BLE001 - said in its own words
            self.close()
            text = str(e)
            if "rangefeed" in text.lower():
                raise SystemExit(
                    "the cluster has rangefeeds off, and a changefeed reads"
                    " through them: SET CLUSTER SETTING"
                    " kv.rangefeed.enabled = true (migkit does not change a"
                    " cluster's settings)") from None
            raise
        self.pos = cursor
        self._first = first

    def read(self, at, most, seconds):
        cursor = at.get(self.LANE)
        if cursor is None:
            raise SystemExit("the changefeed's position is lost; move again"
                             " with --mode full+cdc")
        if self.gen is None or self.pos != cursor:
            self._open(int(cursor))
        out, mark = [], int(cursor)
        stop = time.monotonic() + seconds
        caught = False
        while len(out) < most and time.monotonic() < stop:
            if self._first is not None:
                row, self._first = self._first, None
            else:
                row = next(self.gen)
            table, key, value = row[0], row[1], row[2]
            out.append((self.LANE, mark, key, value, table))
            if table is None:
                hlc = hooks.ask("changetopic", "hlc")
                body = json.loads(value)
                if "resolved" in body:
                    mark = hlc(body["resolved"])
                    caught = True
            else:
                caught = False
        self.pos = mark
        return out, {self.LANE: mark}, caught

    def lost(self, at):
        return None

    def room(self, at):
        """How long the cluster keeps the past a feed from `at` starts
        at: its garbage collector's window, less how far behind it is."""
        cursor = at.get(self.LANE)
        try:
            got = self.eng._rows("src", self.db, "show zone configuration"
                                                 " from range default")
            keep = int(re.search(r"gc\.ttlseconds\s*=\s*(\d+)",
                                 str(got[0][1])).group(1))
        except Exception:  # noqa: BLE001 - for /metrics only
            return None
        if cursor is None:
            return {"seconds": float(keep)}
        # an `hlc` integer is wall nanoseconds shifted ten digits
        behind = (self.eng._now("src", self.db) - int(cursor)) / 10 ** 19
        return {"seconds": max(0.0, keep - behind)}

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001 - it ends with the query
                pass
        self.conn = self.gen = None
        self._first = None


class CockroachEngine(PgWireEngine):
    CANON_ENGINE = "cockroachdb"
    DEFAULT_PORT = 26257
    SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "crdb_internal",
                      "pg_extension")
    #: the changefeed names its tables when it is opened (`SinklessFeed`):
    #: a table that joins a running tail gets its changes only once the
    #: feed is let go and opened again, from where the tail stands
    #: (`hetero.tail_apply`) - copied and then never followed before
    FOLLOWS_LISTED_TABLES = True
    #: PostgreSQL's large-value expressions: sized by the server,
    #: read and written in pieces under the side's limits (`pieces.Sizing`)
    LARGE_VALUES = pieces.Sql(
        "sized by the server, read and written in pieces, wide values hashed where they lie",
        length={"bytea": "octet_length({c})", "text": "octet_length({c})",
                "character varying": "octet_length({c})"},
        types=("bytea", "text", "character varying"),
        piece="substring({c} from {at} for {n})",
        append="{c} = {c} || {v}",
        appends=("bytea", "text", "charactervarying"),
        md5="md5({c})", md5_classes=("bytes", "text"))

    #: the cluster settings that bound a statement, a write and a row
    LIMIT_SETTINGS = (
        ("sql.conn.max_read_buffer_message_size", "sent"),
        ("kv.raft.command.max_size", "stored"),
        ("kv.raft.command.max_size", "row"),
        ("sql.guardrails.max_row_size_err", "row"))

    def piece_limits(self, side, db):
        """Each of `LIMIT_SETTINGS` read off the cluster (`SHOW CLUSTER
        SETTING`); one the account cannot read is left out."""
        out = []
        for name, bounds in self.LIMIT_SETTINGS:
            try:
                got = self._rows(side, db, f"show cluster setting {name}")
            except Exception:  # noqa: BLE001 - not readable by this account
                continue
            n = pieces.setting_bytes(got[0][0]) if got else None
            if n:
                out.append(pieces.Limit(name, n, bounds))
        return out

    def _visible(self):
        return " and is_hidden = 'NO'"

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        return [d for (d,) in self._rows(
            "src", "defaultdb", "select database_name from"
                                " [show databases]")
                if d not in ("system", "postgres", "defaultdb")
                and not self.hop.excluded(d)]

    # ---- writing ---------------------------------------------------------

    def _upsert(self, cur, side, db, table, names, rows):
        from .. import canon
        declared = dict(self.neutral_columns(side, db, table))
        classes = [canon.type_class(self.CANON_ENGINE, declared.get(n, ""))
                   or "text" for n in names]
        head = (f"upsert into {self._qualified(side, db, table)} ("
                + ", ".join(self._q(n) for n in names) + ") values ")
        one = "(" + ", ".join(["%s"] * len(names)) + ")"
        # a row past what one statement takes goes first by the same
        # upsert and its value a piece a statement; the rest in statements
        # cut by rows and by what the cluster takes in one
        sz = pieces.sizing(self, side, db)
        if pieces.wider_than(rows, sz.inline, sz.char_bytes):
            rows = pieces.written_in_pieces(
                self, cur, side, db, table, list(zip(names, classes)),
                self.neutral_key(side, db, table), rows,
                insert=head + "(" + ", ".join([MARK] * len(names)) + ")")
        most = min([(x.most - pieces.HEADROOM) // (
            pieces.SENT_FACTOR if x.bounds == "sent" else 1)
            for x in sz.limits if x.bounds in ("sent", "stored")
            and x.most > pieces.HEADROOM] or [pieces.BATCH_BYTES])
        for part in pieces.by_bytes(rows, most, UPSERT_ROWS):
            cur.execute(head + ", ".join([one] * len(part)),
                        [canon.write_value(self.CANON_ENGINE, c, v)
                         for r in part for c, v in zip(classes, r)])

    def neutral_write(self, side, db, table, columns, rows):
        """Rows by `UPSERT`, which writes each over the row of its key or
        makes it: a batch written again after a restart lands on itself."""
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        if not self.neutral_key(side, db, table):
            return super().neutral_write(side, db, table, columns, rows)
        conn = self._connect(side, db)
        try:
            self._upsert(conn.cursor(), side, db, table, names,
                         [list(r) for r in rows])
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        by_shape = {}
        for key, values in rows:
            row = {**key, **values}
            by_shape.setdefault(tuple(row), []).append(list(row.values()))
        with self._writer(side, db) as conn:
            cur = conn.cursor()
            for names, got in by_shape.items():
                self._upsert(cur, side, db, table, list(names), got)

    def neutral_empty(self, side, db, table, where=None):
        """Deleted a thousand rows at a time: a delete of the whole table
        in one statement is one transaction, which a large table outgrows."""
        self._target_only(side, "empty a table")
        gone = 0
        conn = self._connect(side, db)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            while True:
                cur.execute(f"delete from {self._qualified(side, db, table)}"
                            + (f" where ({where})" if where else "")
                            + f" limit {DELETE_ROWS}")
                n = cur.rowcount or 0
                gone += max(n, 0)
                if n < DELETE_ROWS:
                    break
        finally:
            conn.close()
        return gone

    # ---- the changefeed --------------------------------------------------

    def _now(self, side, db):
        """The cluster's clock now, as the ordering integer `hlc` makes of
        CockroachDB's timestamps."""
        hlc = hooks.ask("changetopic", "hlc")
        got = self._rows(side, db, "select cluster_logical_timestamp()")
        return hlc(str(got[0][0]))

    def _follower(self, db):
        changetopic = hooks.workflow("changetopic")
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            spec = self._changes_spec()
            via = (spec or {}).get("via") or ("kafka" if spec else
                                              "changefeed")
            if via == "kafka":
                lanes = changetopic.KafkaLanes(spec,
                                               f"{self.hop.name}-changes")
            elif via == "changefeed":
                lanes = SinklessFeed(self, db)
            else:
                raise SystemExit(f"changes: via {via} - a CockroachDB"
                                 " source is followed by a changefeed"
                                 " streamed to migkit (changefeed) or one"
                                 " publishing to Kafka (kafka)")
            held[db] = changetopic.Follower(
                changetopic.CrdbChangefeed(), lanes,
                name=lambda row, db=db: self._named(db, row),
                read_back=lambda t, rows, v, db=db: self._read_back(
                    db, t, rows, v))
        return held[db]

    def change_point(self, side, db):
        """Now: the feed's ends (a topic's, or the clock), then the
        cluster's clock - a copy begun after reads every change before."""
        self._refuse_unkeyed(db, "a changefeed names such a table's rows by"
                                 " a hidden row id no other server holds")
        f = self._follower(db)
        if isinstance(f.lanes, SinklessFeed):
            return f.point(fence=lambda: f.lanes.last)
        return f.point(fence=lambda: self._now(side, db))

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        return {"fence": self._now(side, db)}

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each row the changefeed says changed, in commit order, as the
        cluster held it at the resolved timestamp its batch ends on."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

    def snapshot_mark(self, side, db):
        return str(self._now(side, db))

    @staticmethod
    def mark_covers(side, db, mark, change):
        """A read begun after the mark reads at a later timestamp, and sees
        every commit at or below the mark."""
        txn = change.get("txn")
        if mark is None or not isinstance(txn, int):
            return None
        return txn <= int(mark)

    def _as_of(self, fence):
        hlc_text = hooks.ask("changetopic", "hlc_text")
        if fence is None:
            return ""
        return f" as of system time '{hlc_text(int(fence))}'"

    def _read_back(self, db, table, rows, version):
        """[(row, key, values or None)] of the keys the feed named, read
        as the table stood at `version` - or now, where the garbage
        collector has passed it - and at `version` held to what the feed
        said of each."""
        key = self.neutral_key("src", db, table)
        declared = dict(self.neutral_columns("src", db, table))

        def raw(row):
            vals = [row.key.get(str(i)) for i in range(len(key))]
            if len(row.key) != len(key):
                raise SystemExit(
                    f"{table}: the changefeed keys its rows by"
                    f" {len(row.key)} values, and its primary key has"
                    f" {len(key)} columns - the feed's table is not the"
                    " table here")
            return [None if v is None else
                    ("true" if v is True else "false" if v is False
                     else str(v)) for v in vals]
        keys = [raw(r) for r in rows]
        at_fence = True
        try:
            found, classes = self._key_rows("src", db, table, key, keys,
                                            cast=True, fence=version)
        except Exception as e:  # noqa: BLE001 - classified here
            if not any(w in str(e).lower() for w in GC_PASSED):
                raise
            at_fence = False
            found, classes = self._key_rows("src", db, table, key, keys,
                                            cast=True)
        typed = self._typed_keys(db, key, declared, keys, classes)
        out = []
        for r, t in zip(rows, typed):
            values = found.get(t[0])
            if at_fence and (values is None) != (r.op == "delete"):
                raise SystemExit(
                    f"{db}.{table}: the changefeed says the row keyed"
                    f" {dict(zip(key, raw(r)))} was"
                    f" {'deleted' if r.op == 'delete' else 'written'} by"
                    f" {r.version}, and the cluster as it stood then"
                    f" {'holds' if values is not None else 'does not hold'}"
                    " it: the key as the feed names it is not the key here."
                    " Nothing of this batch was applied")
            out.append((r, t[1] if values is None
                        else {k: values[k] for k in key}, values))
        return out

    def _typed_keys(self, db, key, declared, keys, classes):
        """[(key text, {column: value})]: each key as the cluster reads
        the feed's text of it, cast to the column's type."""
        changetopic = hooks.workflow("changetopic")
        out = []
        conn = self._connect("src", db)
        try:
            cur = conn.cursor()
            one = "(%s, " + ", ".join(f"%s::{declared[k]}" for k in key) + ")"
            names = ", ".join(self._q(f"k{i}") for i in range(len(key)))
            for at in range(0, len(keys), 500):
                part = keys[at:at + 500]
                cur.execute(f"select {names} from (values "
                            + ", ".join([one] * len(part))
                            + f") as v(n, {names}) order by n",
                            [v for i, vals in enumerate(part)
                             for v in [i] + list(vals)])
                for got in cur.fetchall():
                    typed = self._typed_row(key, declared, list(got))
                    out.append((tuple(changetopic.key_text(classes.get(k),
                                                           typed[k])
                                      for k in key), typed))
            conn.rollback()
        finally:
            conn.close()
        return out

    def code_routines(self, side, db):
        """Every function and procedure written in the database, as the
        stored-code conversion reads them (`storedcode`): from CockroachDB's
        `pg_proc`, which holds the user-defined functions beside its own
        built-ins (told apart by schema)."""
        from ..storedcode.routines import Param, Routine
        out = []
        for name, kind, lang, setof, ret, names, modes, types, src, made, \
                sch in self._rows(
                side, db, "select p.proname, p.prokind, l.lanname,"
                          " p.proretset, format_type(p.prorettype, null),"
                          " coalesce(p.proargnames, '{}'),"
                          " coalesce(p.proargmodes::text[], '{}'),"
                          " array(select format_type(t, null) from unnest("
                          "coalesce(p.proallargtypes, p.proargtypes::oid[]))"
                          " t), p.prosrc, pg_get_functiondef(p.oid),"
                          " n.nspname from pg_catalog.pg_proc p join"
                          " pg_catalog.pg_namespace n on n.oid ="
                          " p.pronamespace join pg_catalog.pg_language l on"
                          " l.oid = p.prolang where n.nspname not in "
                          f"({self._sys()}) and p.prokind in ('f', 'p')"
                          " order by 1"):
            if not self.takes(db, "procedure" if kind == "p" else "function",
                              f"{sch}.{name}"):
                continue
            params = []
            for i, t in enumerate(types):
                mode = {"i": "in", "o": "out", "b": "inout", "v": "in",
                        "t": "table"}.get(modes[i] if i < len(modes)
                                          else "i", "in")
                if mode != "table":
                    params.append(Param(names[i] if i < len(names) else "",
                                        t, mode))
            as_procedure = kind == "p"
            out.append(Routine(
                kind="procedure" if as_procedure else "function",
                name=str(name), params=params,
                returns=None if as_procedure else str(ret),
                returns_set=bool(setof), body=src, definition=made,
                language=str(lang), settings={"schema": sch}))
        return out

    # ---- the server --------------------------------------------------------

    def server_identity(self, side, db):
        got = self._rows(side, db, "select crdb_internal.cluster_id()::string,"
                                   " version(), crdb_internal.node_id()")
        cluster, version, node = got[0]
        if "cockroach" not in str(version).lower():
            raise SystemExit(
                f"the {'source' if side == 'src' else 'target'} is not"
                f" CockroachDB ({str(version)[:60]})")
        out = {"id": str(cluster), "running": f"node {node}",
               "version": str(version).split("(")[0].strip()}
        there = self._rows(side, db, "select count(*) from [show databases]"
                                     f" where database_name = {MARK}",
                           (self._d(side, db),))
        if not int(there[0][0]):
            out["absent"] = True
        return out

    def stream_identity(self, side, db):
        spec = self._changes_spec(side) or {}
        got = self._rows(side, db, "select crdb_internal.cluster_id()::string")
        return {"cluster": str(got[0][0]),
                "changes": ",".join(sorted(str(t) for t in (
                    spec.get("topics") or [spec.get("topic")]) if t))
                or "changefeed"}

    def rangefeeds(self, side, db):
        """Whether the cluster runs rangefeeds, which a changefeed reads
        through, or None where it cannot be asked."""
        try:
            got = self._rows(side, db, "show cluster setting"
                                       " kv.rangefeed.enabled")
        except Exception:  # noqa: BLE001 - not allowed to ask
            return None
        return str(got[0][0]).lower() in ("true", "t", "1")

    def _alone(self, side, db, sql, args=None):
        """One statement on its own connection, committed as it runs - an
        export, an import or a role cannot run inside a transaction - and
        its rows, where it gives any."""
        conn = self._connect(side, db)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, args)
                return cur.fetchall() if cur.description else []
        finally:
            conn.close()

    # ---- settings (`params`) ---------------------------------------------

    #: the cluster settings that change what a statement does, what a write
    #: may hold or what a changefeed reads, compared side by side; and the
    #: database's zone (its replicas and how long old versions are kept)
    PARAM_PREFIXES = ("sql.defaults.",)
    PARAM_NAMES = ("kv.rangefeed.enabled", "kv.raft.command.max_size",
                   "sql.guardrails.max_row_size_err",
                   "sql.guardrails.max_row_size_log",
                   "kv.closed_timestamp.target_duration",
                   "sql.stats.automatic_collection.enabled",
                   "sql.conn.max_read_buffer_message_size")

    def _settings(self, side, db):
        got = {str(n): str(v) for n, v in self._rows(
            side, db, "select variable, value from [show all cluster"
                      " settings]")
            if str(n).startswith(self.PARAM_PREFIXES)
            or str(n) in self.PARAM_NAMES}
        try:
            zone = self._rows(side, db, "show zone configuration from"
                                        " database "
                                        + self._q(self._d(side, db)))
        except Exception:  # noqa: BLE001 - the account may not read zones
            zone = None
        if zone:
            got["zone"] = " ".join(str(zone[0][-1]).replace(
                self._d(side, db), "<db>").split())
        return got

    def check_params(self, db):
        """Each of `PARAM_NAMES`, the `sql.defaults.` settings and the
        database's zone as the target has it beside the source's."""
        src, dst = self._settings("src", db), self._settings("dst", db)
        out = [Result("params", f"{db} {n}", "warn",
                      f"source {src.get(n, 'unset')}, target"
                      f" {dst.get(n, 'unset')}",
                      fix_hint=("ALTER DATABASE ... CONFIGURE ZONE USING"
                                " ... as the source's" if n == "zone" else
                                f"SET CLUSTER SETTING {n} = <the source's>"))
               for n in sorted(set(src) | set(dst))
               if src.get(n) != dst.get(n)]
        return out or [Result("params", db, "ok",
                              f"{len(src)} settings alike")]

    # ---- accounts (`users`) ----------------------------------------------

    #: the cluster's own roles, never carried
    OWN_ROLES = frozenset({"root", "admin", "public", "node"})
    ACCOUNT_PASSWORDS = ("read as the cluster keeps them (`system.users`)"
                         " and set again as that hash, which CockroachDB"
                         " takes as given")

    def accounts(self, side):
        """The roles of `side` in `Engine.accounts`' shape: a role that
        signs in (no `NOLOGIN`) a user, its password's hash as kept, the
        roles it is a member of and its grants on the hop's tables."""
        dbs = list(self.hop.databases or [])
        if not dbs:
            return {}
        first, out = dbs[0], {}
        nologin = {str(u) for (u,) in self._rows(
            side, first, "select username from system.role_options where"
                         " option = 'NOLOGIN'")}
        for name, hashed in self._rows(side, first, 'select username,'
                                                    ' "hashedPassword" from'
                                                    ' system.users'):
            name = str(name)
            if name in self.OWN_ROLES:
                continue
            login = name not in nologin
            out[f"{'user' if login else 'role'}:{name}"] = {
                "login": login, "roles": [], "grants": [],
                "hash": bytes(hashed).decode() if hashed else None,
                "attrs": {}}

        def key_of(n):
            return f"user:{n}" if f"user:{n}" in out else f"role:{n}"
        for role, member in self._rows(side, first, "select role, member"
                                                    " from"
                                                    " system.role_members"):
            if key_of(str(member)) in out and str(role) not in \
                    self.OWN_ROLES:
                out[key_of(str(member))]["roles"].append(str(role))
        for db in dbs:
            for who, sch, table, priv in self._rows(
                    side, db, "select grantee, table_schema, table_name,"
                              " privilege_type from"
                              " information_schema.table_privileges where"
                              " table_catalog = current_database()"):
                t = f"{sch}.{table}"
                if sch in self.SYSTEM_SCHEMAS or key_of(str(who)) not in out \
                        or not self.in_scope(db, t):
                    continue
                out[key_of(str(who))]["grants"].append(
                    f"{str(priv).upper()} ON {db}.{t}")
        for a in out.values():
            a["roles"], a["grants"] = (sorted(set(a["roles"])),
                                       sorted(set(a["grants"])))
        return out

    def account_steps(self, src, dst, passwords):
        """A role made with `NOLOGIN`, a user with its password - the
        passwords file's, else the hash the source keeps - its memberships
        and its grants; a user with neither is skipped, said."""
        q = self._q

        def make(name, a, secret):
            if not a.get("login"):
                return (f"create role {q(name)} with nologin",
                        f"drop role {q(name)}", False, None)
            word = secret or a.get("hash")
            if not word:
                return None
            lit = str(word).replace("'", "''")
            return (f"create role {q(name)} with login password '{lit}'",
                    f"drop role {q(name)}", bool(secret),
                    f"create role {q(name)} with login password ***")

        def member(r, name):
            return (f"grant {q(r)} to {q(name)}",
                    f"revoke {q(r)} from {q(name)}")

        def grant(priv, where, table, name):
            obj = self._qualified("dst", where, table)
            return (f"grant {priv.lower()} on table {obj} to {q(name)}",
                    f"revoke {priv.lower()} on table {obj} from {q(name)}")
        return account_steps_by(src, dst, passwords, make, member, grant,
                                "it signs in, and its password is neither"
                                " read nor in the passwords file")

    def account_run(self, what):
        self._alone("dst", (self.hop.databases or ["defaultdb"])[0], what)

    # ---- the target's state before a repair or a cutover -----------------

    def snapshot_state(self, db, state_dir, kind="all"):
        """The cluster's own time the target stands at - a rollback reads
        it back `AS OF SYSTEM TIME` that mark while the zone keeps old
        versions - and, unless only counters are repaired, each table's
        definition as the cluster gives it."""
        mark = self._rows("dst", db, "select"
                                     " cluster_logical_timestamp()::string")
        (state_dir / "dst-mark.txt").write_text(f"{mark[0][0]}\n")
        if kind == "sequences":
            return
        defs = []
        for t in self.neutral_tables("dst", db):
            got = self._rows("dst", db, "show create table "
                                        + self._qualified("dst", db, t))
            defs.append(f"{got[0][-1]};" if got else f"-- {t}: not shown")
        (state_dir / "dst-schema.sql").write_text("\n\n".join(defs) + "\n")

    # ---- the bulk way (`bulk-move`) --------------------------------------

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table by the way climbed for it (`decide.crdb_bulk_ways`):
        exported by the source into the stage and imported by the target,
        else by the table copier. The check that follows compares them."""
        steps = [f"{t}: exported into the stage and imported by the"
                 " clusters, or copied by key ranges" for t in tables]
        if not go:
            return steps
        from .. import tableway
        from ..decide import Facts, crdb_bulk_ways
        self._as_pair().create_missing(db, log)
        stage = str(self.hop.target.options.get("stage") or "")
        held = []

        def run(way, t):
            if way == "import":
                return self._imported(db, t, stage, log)
            return self.copy_by_copier(db, t, log, held)

        def facts(t):
            return Facts(stage=stage, same=self.hop.engine == "cockroachdb",
                         key=lambda: bool(self.neutral_key("src", db, t)),
                         empty=lambda: not self._rows(
                             "dst", db, "select 1 from "
                             + self._qualified("dst", db, t) + " limit 1"))
        tableway.each(self, db, tables, crdb_bulk_ways(), facts, run, log)
        return steps

    def _imported(self, db, table, stage, log):
        """One table exported by the source as CSV into a place of its own
        in the stage and imported by the target from there: the rows the
        import says it took. NULL is written as `\\N` both ways, so an
        empty string stays one."""
        import uuid
        base, _, auth = stage.partition("?")
        place = f"{base.rstrip('/')}/{self.hop.name}/{db}/{uuid.uuid4().hex}"
        auth = f"?{auth}" if auth else ""
        names = ", ".join(self._q(c) for c, _ in
                          self.neutral_columns("src", db, table))
        self._alone("src", db, f"export into csv '{place}{auth}' with"
                               " nullas = '\\N' from select " + names
                    + " from " + self._qualified("src", db, table))
        got = self._alone("dst", db, "import into "
                          + self._qualified("dst", db, table)
                          + f" ({names}) csv data ('{place}/*.csv{auth}')"
                          " with nullif = '\\N'")
        rows = int(got[0][3]) if got and len(got[0]) > 3 else 0
        log(f"{table}: {rows:,} rows imported from the stage")
        return rows

    #: `ALTER TABLE ... VALIDATE CONSTRAINT` checks the rows already there
    VALIDATE_HINT = "ALTER TABLE ... VALIDATE CONSTRAINT <name>"

    def _deep_extra(self):
        """The checks for what CockroachDB keeps that the other servers on
        this wire do not: foreign keys and constraints added without
        checking the rows there, triggers and materialized views."""
        return [
            ("fk", self._deep_foreign_keys),
            ("checks", self._deep_unvalidated),
            ("triggers", self._deep_triggers),
            ("matviews", self._deep_matviews)]

    def check_deep(self, db):
        """The shared checks (`pgwire`), the target's schema objects whole
        (`crdb_internal.invalid_objects`: a descriptor a failed schema
        change left broken reads as a table that is there and is not), and
        on the source the rangefeeds a follow needs."""
        out = self._shared_deep(db)
        try:
            bad = self._rows("dst", db, "select obj_name, error, schema_name"
                                        " from crdb_internal.invalid_objects")
            # an object the hop leaves out is the target's own
            bad = [r for r in bad
                   if self.in_scope(db, f"{r[2] or 'public'}.{r[0]}")]
        except Exception as e:  # noqa: BLE001 - said, as the error
            out.append(self._deep_error(db, "target", e))
        else:
            out.append(Result(
                "deep", f"{db} target objects", "diff" if bad else "ok",
                (f"{len(bad)} objects the cluster says are broken: "
                 + ", ".join(str(r[0]) for r in bad[:4])) if bad
                else "every schema object whole", "",
                "repair those objects before the target is used" if bad
                else ""))
        on = self.rangefeeds("src", db)
        if on is False:
            out.append(Result(
                "deep", f"{db} source changefeeds", "warn",
                "rangefeeds are off on the source, and its changes cannot"
                " be followed until they are on", "",
                "SET CLUSTER SETTING kv.rangefeed.enabled = true"))
        return out

    def reach_items(self, side):
        CrdbChangefeed = hooks.ask("changetopic", "CrdbChangefeed")
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "tables copied by key, written by UPSERT, and"
                            " compared row by row"}]
        if side != "src":
            return items
        spec = self._changes_spec()
        how = ("a changefeed publishing to Kafka" if spec
               else "a changefeed streamed to migkit, which makes nothing"
                    " on the cluster")
        items.append({"level": "pass", "scope": "reach",
                      "item": "changes followed through a changefeed",
                      "detail": f"{how}, in commit order by each resolved"
                                " timestamp; it loses"
                                f" {CrdbChangefeed.LOSES}"})
        return items
