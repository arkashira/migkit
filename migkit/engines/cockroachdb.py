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
        from ..changetopic import hlc_text
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
                from ..changetopic import hlc
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
        for i in range(0, len(rows), UPSERT_ROWS):
            part = rows[i:i + UPSERT_ROWS]
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
        from ..changetopic import hlc
        got = self._rows(side, db, "select cluster_logical_timestamp()")
        return hlc(str(got[0][0]))

    def _follower(self, db):
        from .. import changetopic
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
        from ..changetopic import hlc_text
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
        from .. import changetopic
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

    def check_deep(self, db):
        """The target's schema objects whole (`crdb_internal
        .invalid_objects`: a descriptor a failed schema change left
        broken reads as a table that is there and is not), and on the
        source the rangefeeds a follow needs."""
        out = []
        try:
            bad = self._rows("dst", db, "select obj_name, error from"
                                        " crdb_internal.invalid_objects")
        except Exception as e:  # noqa: BLE001 - said, as the error
            out.append(self._deep_error(db, "target", e))
        else:
            out.append(Result(
                "deep", f"{db} target objects", "diff" if bad else "ok",
                (f"{len(bad)} objects the cluster says are broken: "
                 + ", ".join(str(n) for n, _ in bad[:4])) if bad
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
        from ..changetopic import CrdbChangefeed
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
