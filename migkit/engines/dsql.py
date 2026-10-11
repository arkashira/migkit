"""Aurora DSQL as a side of a pair (backlog ER4), over PostgreSQL's wire.

DSQL takes a PostgreSQL client and most of its SQL, within limits it
enforces and a copy has to be written for: a write transaction holds at
most 3,000 rows and 10 MiB and lasts at most five minutes; one DDL
statement a transaction, never beside a write; an index on a table with
rows is built `ASYNC`; no TRUNCATE, no temporary tables, no triggers and
no PL/pgSQL. So a batch is written in transactions of at most 3,000 rows
and 9 MiB (`WRITE_ROWS`, `WRITE_BYTES`, the rest of the 10 left for a row's
own overhead), by `INSERT ... ON CONFLICT DO UPDATE`, each tried again
where DSQL's optimistic concurrency refused it (SQLSTATE 40001); a table
is emptied 3,000 rows at a time; a tail's batch is applied in pieces the
same size, each of which, replayed, lands on itself. A keyless table is
read in one pass, and a pass of more than five minutes is refused by
DSQL - such a table has to be given a key. Signs in with an IAM token
where the endpoint gives no password (`generate_db_connect_admin_auth
_token` for `admin`, `generate_db_connect_auth_token` for any other role).

Its changes: DSQL's CDC stream to Kinesis (`changes: {stream: <name or
ARN>, region: ...}`), read by the change-topic reader (`changetopic`).
The stream is unordered and delivers at least once, and sends no mark of
how far it has published; so the reader holds each change until it is
older than the stream's own lag - DSQL's `BehindSourceLag` metric, the
largest over the last ten minutes and a minute more for the metric's own
period, or the hop's `order_window` seconds where the metric cannot be
read - and releases them in commit order (`source.ts_ns`). The values
are taken from the record, each by its column's type; what a record
cannot carry exactly - an interval's months and days, a timetz's offset,
the infinities of a date or a timestamp - makes its table's rows read back
from DSQL as they are when the change is read, which later changes level
again (DSQL keeps no past to read at the fence).
"""
import base64
import datetime
import struct
import time
from decimal import Decimal

from .base import Engine, Result
from .pgwire import PgWireEngine

#: what one write transaction may hold, as DSQL allows it, less room
WRITE_ROWS, WRITE_BYTES = 3000, 9 * 2 ** 20
#: tries of a transaction DSQL's concurrency control refused
OCC_TRIES = 8
#: the order window where the stream's lag cannot be read, in seconds
ORDER_WINDOW = 300
#: classes a CDC record carries exactly, by the column's declared name
LOSSY = ("interval", "time with time zone", "timetz")
#: a timestamp's and a date's infinities, as DSQL's records write them
TS_INFINITE = (9223372036825200000, -9223372036832400000)
EPOCH = datetime.datetime(1970, 1, 1)


class DsqlEngine(PgWireEngine):
    CANON_ENGINE = "dsql"
    SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "sys")

    def _region(self, ep):
        got = ep.options.get("region")
        if got:
            return str(got)
        # `<cluster>.dsql.<region>.on.aws`
        parts = str(ep.host).split(".")
        if "dsql" in parts and parts.index("dsql") + 1 < len(parts):
            return parts[parts.index("dsql") + 1]
        raise SystemExit(f"{ep.host}: say the DSQL cluster's region (region:"
                         " in the side's options)")

    def _password(self, side, ep):
        """The endpoint's password, or an IAM token DSQL is signed in with,
        made again when it is ten minutes old (tokens last fifteen)."""
        if ep.password and str(ep.options.get("auth", "")) != "aws_iam":
            return ep.password
        made = self.__dict__.setdefault("_tokens", {})
        got = made.get(side)
        if got and time.time() - got[1] < 600:
            return got[0]
        import boto3
        client = boto3.client("dsql", region_name=self._region(ep))
        if (ep.user or "admin") == "admin":
            token = client.generate_db_connect_admin_auth_token(
                Hostname=ep.host, Region=self._region(ep))
        else:
            token = client.generate_db_connect_auth_token(
                Hostname=ep.host, Region=self._region(ep))
        made[side] = (token, time.time())
        return token

    # ---- writing within DSQL's limits -------------------------------------

    @staticmethod
    def _bytes(row):
        return sum(len(v) if isinstance(v, (bytes, str)) else 16
                   for v in row)

    def _pieces(self, rows):
        """`rows` in runs a DSQL transaction takes: 3,000 rows and 9 MiB."""
        part, size = [], 0
        for r in rows:
            n = self._bytes(r)
            if part and (len(part) >= WRITE_ROWS or size + n > WRITE_BYTES):
                yield part
                part, size = [], 0
            part.append(r)
            size += n
        if part:
            yield part

    def _tried(self, conn, work):
        """`work(cursor)` in one transaction, again where DSQL's optimistic
        concurrency control refused its commit (SQLSTATE 40001)."""
        wait = 0.05
        for n in range(OCC_TRIES):
            try:
                work(conn.cursor())
                conn.commit()
                return
            except Exception as e:  # noqa: BLE001 - classified here
                conn.rollback()
                if getattr(e, "pgcode", None) != "40001" or n == OCC_TRIES - 1:
                    raise
                time.sleep(wait)
                wait = min(wait * 2, 2.0)

    def neutral_write(self, side, db, table, columns, rows):
        """Rows by `INSERT ... ON CONFLICT (key) DO UPDATE`, in
        transactions DSQL takes."""
        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        key = self.neutral_key(side, db, table)
        sets = [n for n in names if n not in key]
        tail = (f" on conflict ({', '.join(self._q(k) for k in key)}) do "
                + ("update set " + ", ".join(
                    f"{self._q(n)} = excluded.{self._q(n)}" for n in sets)
                   if sets else "nothing")) if key else ""
        one = "(" + ", ".join(["%s"] * len(names)) + ")"
        head = (f"insert into {self._qualified(side, db, table)} ("
                + ", ".join(self._q(n) for n in names) + ") values ")
        conn = self._connect(side, db)
        try:
            for part in self._pieces([[canon.write_value(
                    self.CANON_ENGINE, c, v) for (_, c), v in zip(columns, r)]
                    for r in rows]):
                self._tried(conn, lambda cur, part=part: cur.execute(
                    head + ", ".join([one] * len(part)) + tail,
                    [v for r in part for v in r]))
        finally:
            conn.close()
        return len(rows)

    def neutral_apply(self, side, db, changes):
        """A tail's batch in pieces of at most 3,000 changes, each one
        transaction: DSQL takes no more in one. Each piece replayed lands
        on itself, so a batch stopped between two is applied whole again
        from its position."""
        n = 0
        for i in range(0, len(changes), WRITE_ROWS):
            n += Engine.neutral_apply(self, side, db,
                                      changes[i:i + WRITE_ROWS])
        return n

    #: a DSQL batch goes as one transaction, never in lanes side by side
    LANES_FROM = 10 ** 9

    def neutral_empty(self, side, db, table, where=None):
        """Deleted 3,000 rows a transaction, by key: DSQL has no TRUNCATE
        and takes no more rows in one."""
        self._target_only(side, "empty a table")
        key = self.neutral_key(side, db, table)
        q = self._qualified(side, db, table)
        cond = f" where ({where})" if where else ""
        if not key:
            return super().neutral_empty(side, db, table, where)
        cols = ", ".join(self._q(k) for k in key)
        gone = 0
        conn = self._connect(side, db)
        try:
            while True:
                got = [0]

                def one(cur):
                    cur.execute(f"delete from {q} where ({cols}) in (select"
                                f" {cols} from {q}{cond} limit {WRITE_ROWS})")
                    got[0] = cur.rowcount or 0
                self._tried(conn, one)
                gone += got[0]
                if got[0] < WRITE_ROWS:
                    break
        finally:
            conn.close()
        return gone

    def neutral_create_sql(self, side, db, table, columns, key=()):
        """PostgreSQL's DDL, with `json` where it would build `jsonb`,
        which DSQL does not keep."""
        ddl = super().neutral_create_sql(side, db, table, columns, key)
        return ddl.replace(" jsonb", " json")

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        """Built `ASYNC`: DSQL builds an index on a table with rows only
        so, as a job of its own (`sys.jobs`)."""
        return (f"create {'unique ' if unique else ''}index async"
                f" {self._q(name)} on {self._qualified(side, db, table)} ("
                + ", ".join(self._q(c) for c in columns) + ")")

    # ---- the CDC stream -----------------------------------------------------

    def _now_ns(self, side, db):
        got = self._rows(side, db, "select (extract(epoch from now())"
                                   " * 1000000)::bigint")
        return int(got[0][0]) * 1000

    def _lag_ns(self):
        """How far behind DSQL publishes, in nanoseconds: the stream's
        `BehindSourceLag` at its largest over the last ten minutes, and a
        minute more for the metric's own period - or the order window."""
        spec = self._changes_spec() or {}
        kept = self.__dict__.get("_lag")
        if kept and time.time() - kept[1] < 60:
            return kept[0]
        lag = None
        cluster = str(self.hop.source.host).split(".")[0]
        stream = spec.get("cdc_stream")
        if stream:
            try:
                import boto3
                cw = boto3.client("cloudwatch",
                                  region_name=self._region(self.hop.source))
                end = datetime.datetime.now(datetime.timezone.utc)
                got = cw.get_metric_statistics(
                    Namespace="AWS/AuroraDSQL", MetricName="BehindSourceLag",
                    Dimensions=[{"Name": "ClusterId", "Value": cluster},
                                {"Name": "StreamId", "Value": str(stream)}],
                    StartTime=end - datetime.timedelta(minutes=10),
                    EndTime=end, Period=60, Statistics=["Maximum"])
                points = [p["Maximum"] for p in got.get("Datapoints", [])]
                if points:
                    lag = (max(points) + 60_000) * 1_000_000
            except Exception:  # noqa: BLE001 - the window then
                lag = None
        if lag is None:
            lag = float(spec.get("order_window", ORDER_WINDOW)) * 10 ** 9
        self.__dict__["_lag"] = (int(lag), time.time())
        return int(lag)

    def _follower(self, db):
        from .. import changetopic
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            spec = self._changes_spec()
            if not spec or not spec.get("stream"):
                raise SystemExit(
                    "DSQL keeps no log a client reads: its changes are read"
                    " from the Kinesis stream its CDC stream publishes to."
                    " Say which in the source's options - changes: {stream:"
                    " <name or ARN>, region: <region>, cdc_stream: <the CDC"
                    " stream's id>}")
            held[db] = changetopic.Follower(
                changetopic.DsqlCdc(), changetopic.KinesisLanes(
                    {**spec, "region": spec.get("region")
                     or self._region(self.hop.source)},
                    f"{self.hop.name}-changes"),
                name=lambda row, db=db: self._named(db, row),
                shape=lambda t, row, db=db: self._shape(db, t, row),
                read_back=lambda t, rows, v, db=db: self._read_back(
                    db, t, rows),
                delivered=lambda db=db: self._now_ns("src", db)
                - self._lag_ns())
        return held[db]

    def change_point(self, side, db):
        self._refuse_unkeyed(db, "DSQL's stream names no key of a deleted"
                                 " row of such a table")
        # a record committed at the fence itself may be one the copy did
        # not see: a microsecond earlier keeps it
        return self._follower(db).point(
            fence=lambda: self._now_ns(side, db) - 1000)

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        return {"fence": self._now_ns(side, db)}

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each row DSQL's stream says changed, in commit order once the
        stream has published everything before it."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

    def _decoders(self, db, table):
        """{column: parse} for a table whose every column its CDC records
        carry exactly, None for one they do not."""
        known = self.__dict__.setdefault("_decoders_of", {})
        if (db, table) in known:
            return known[(db, table)]
        from .. import canon
        out = {}
        for name, declared in self.neutral_columns("src", db, table):
            low = str(declared).lower()
            base = low.split("(")[0].strip()
            if any(low.startswith(w) for w in LOSSY) or base in LOSSY:
                out = None
                break
            out[name] = _decoder(canon.type_class(self.CANON_ENGINE,
                                                  declared), low)
        known[(db, table)] = out
        return out

    def _shape(self, db, table, row):
        """(key, values) from a CDC record as the neutral reads give them,
        or None where its table holds a column the record does not carry
        exactly - or a value it carries as an infinity's stand-in."""
        dec = self._decoders(db, table)
        if dec is None:
            return None
        key = self.neutral_key("src", db, table)
        try:
            ident = {k: dec[k](row.key[k]) for k in key}
            values = None if row.values is None else {
                n: dec[n](v) if n in dec else v
                for n, v in row.values.items()}
        except _Infinite:
            return None
        return ident, values

    def _read_back(self, db, table, rows):
        """[(row, key, values or None)] read as DSQL holds them now: it
        keeps no past to read at the fence, so the rows are what later
        changes bring level again, and nothing is held to the record."""
        dec = self.__dict__.get("_decoders_of", {}).get((db, table))
        key = self.neutral_key("src", db, table)
        declared = dict(self.neutral_columns("src", db, table))
        from .. import canon, changetopic
        classes = {n: canon.type_class(self.CANON_ENGINE, t)
                   for n, t in declared.items()}
        typed = []
        for r in rows:
            ident = {}
            for k in key:
                v = r.key.get(k)
                ident[k] = (dec[k](v) if dec and k in dec
                            else _decoder(classes.get(k),
                                          str(declared[k]).lower())(v))
            typed.append(ident)
        found, _ = self._key_rows("src", db, table, key,
                                  [[canon.write_value(self.CANON_ENGINE,
                                                      classes.get(k) or "text",
                                                      t[k]) for k in key]
                                   for t in typed])
        out = []
        for r, t in zip(rows, typed):
            text = tuple(changetopic.key_text(classes.get(k), t[k])
                         for k in key)
            out.append((r, t, found.get(text)))
        return out

    # ---- the server --------------------------------------------------------

    def server_identity(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        got = self._rows(side, db, "select version()")
        return {"id": str(ep.host).split(".")[0],
                "version": str(got[0][0]).split(",")[0]}

    def stream_identity(self, side, db):
        spec = self._changes_spec(side) or {}
        ep = self.hop.source if side == "src" else self.hop.target
        return {"cluster": str(ep.host).split(".")[0],
                "changes": str(spec.get("stream") or "")}

    #: a constraint added to a table is added `NOT VALID` and checked by a
    #: job of its own
    VALIDATE_HINT = ("ALTER TABLE ASYNC ... VALIDATE CONSTRAINT <name>, and"
                     " wait for the job (sys.wait_for_job)")

    def _deep_extra(self):
        """The checks for what DSQL keeps that CockroachDB does not:
        foreign keys that can be deferred to the end of a transaction, and
        added without checking the rows there (`NOT VALID`)."""
        return [
            ("fk", self._deep_foreign_keys),
            ("deferrable", self._deep_deferrable),
            ("checks", self._deep_unvalidated)]

    def check_deep(self, db):
        """The shared checks (`pgwire`), and the index builds DSQL still
        runs on the target (`sys.jobs`): a table
        whose index is not built yet answers the queries that use it by
        reading the whole table, and a unique one does not refuse a
        duplicate yet."""
        shared = self._shared_deep(db)
        try:
            got = self._rows("dst", db, "select job_id, status from sys.jobs"
                                        " where status not in"
                                        " ('completed', 'cancelled')")
        except Exception as e:  # noqa: BLE001 - said, as the error
            return shared + [self._deep_error(db, "target", e)]
        if got:
            return shared + [Result(
                "deep", f"{db} target index builds", "warn",
                f"{len(got)} index builds still running or failed on the"
                " target", "",
                "wait for them (sys.wait_for_job), and rebuild a failed one,"
                " before the cutover")]
        return shared + [Result("deep", f"{db} target index builds", "ok",
                                "every index built")]

    def reach_items(self, side):
        from ..changetopic import DsqlCdc
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": f"tables written {WRITE_ROWS} rows and at most"
                            f" {WRITE_BYTES // 2 ** 20} MiB a transaction,"
                            " compared row by row"}]
        if side != "src":
            return items
        spec = self._changes_spec() or {}
        if not spec.get("stream"):
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": "name the Kinesis stream DSQL's CDC"
                                    " stream publishes to (the source's"
                                    " `changes`) to follow it"})
            return items
        held = ("the stream's own lag" if spec.get("cdc_stream")
                else f"{spec.get('order_window', ORDER_WINDOW)} seconds")
        items.append({"level": "pass", "scope": "reach",
                      "item": "changes followed through DSQL's CDC stream",
                      "detail": f"each change held {held} for the stream's"
                                " order, then applied in commit order; it"
                                f" loses {DsqlCdc.LOSES}"})
        return items


class _Infinite(Exception):
    """A date's or a timestamp's infinity, which a record writes as a
    stand-in number: its row is read back instead."""


def _decoder(cls, declared):
    """The parse of one column's value as DSQL's CDC records write it."""
    tz = "with time zone" in declared or declared.startswith("timestamptz")
    four = declared.split("(")[0].strip() in ("real", "float4")

    def parse(v):
        if v is None:
            return None
        if cls == "float":
            if isinstance(v, str):
                return float(v)
            f = float(v)
            # a `real` as the 32-bit float it is, as the driver reads it
            return struct.unpack("<f", struct.pack("<f", f))[0] if four else f
        if cls in ("decimal", "number"):
            return Decimal(str(v))
        if cls == "integer":
            return int(v)
        if cls == "boolean":
            return bool(v)
        if cls == "bytes":
            return base64.b64decode(v)
        if cls == "date":
            days = int(v)
            if abs(days) > 3_000_000:
                raise _Infinite
            return datetime.date(1970, 1, 1) + datetime.timedelta(days=days)
        if cls == "time":
            us = int(v)
            return (datetime.datetime.min + datetime.timedelta(
                microseconds=us)).time()
        if cls == "timestamp":
            us = int(v)
            if us in TS_INFINITE or abs(us) > 2 ** 62:
                raise _Infinite
            got = EPOCH + datetime.timedelta(microseconds=us)
            return got.replace(tzinfo=datetime.timezone.utc) if tz else got
        return v
    return parse
