"""Cassandra (and ScyllaDB) as one side of a pair (backlog 34).

A database is a keyspace. A table migkit creates is keyed by all of the
source's key columns together as its partition key, so each row is found
by its whole key and no clustering order is invented. An insert replaces
a row with the same key, so a batch written again lands on itself; a null
is left unset rather than written, since a written null is a tombstone.

Rows come back in the order of their partitions' tokens, not of their
keys, so a table is read in one pass, a page at a time, and looked up by
key one row at a time. A `timestamp` holds milliseconds: a source value
with microseconds arrives short of them, and the check says so, because
it is a difference.
"""
import json

from .base import Engine, NeutralCopier, Result


class CassandraEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "cassandra"
    SQL_DIALECT = None
    FOLDED_BECAUSE = "has no way to hash rows in the server"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False

    _sessions = {}

    def _session(self, side):
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
        ep = self.hop.source if side == "src" else self.hop.target
        at = (ep.host, ep.port or 9042, ep.user)
        if at not in self._sessions:
            auth = (PlainTextAuthProvider(ep.user, ep.password)
                    if ep.user else None)
            cluster = Cluster([ep.host], port=int(ep.port or 9042),
                              auth_provider=auth, connect_timeout=15,
                              **ep.cassandra_tls())
            self._sessions[at] = cluster.connect()
        return self._sessions[at]

    def _ks(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    @staticmethod
    def _q(name):
        return '"' + str(name).replace('"', '""') + '"'

    def _qualified(self, side, db, table):
        return f"{self._q(self._ks(side, db))}.{self._q(table)}"

    def databases(self):
        return list(self.hop.databases)

    # ---- what a table is -------------------------------------------------

    def target_missing(self, db):
        return not list(self._session("dst").execute(
            "select keyspace_name from system_schema.keyspaces where"
            " keyspace_name = %s", (self._ks("dst", db),)))

    def prepare_target(self, db):
        """The keyspace, where it is missing and the target endpoint says
        how it is replicated (`replication`). How many copies a keyspace
        keeps is the operator's to decide, not migkit's."""
        if not self.target_missing(db):
            return None
        how = self.hop.target.options.get("replication")
        if not how:
            raise SystemExit(
                f"the keyspace {self._ks('dst', db)} is not on the target,"
                " and how many copies it keeps is not migkit's to choose:"
                " create it, or give the target endpoint `replication`")
        self._session("dst").execute(
            f"create keyspace {self._q(self._ks('dst', db))} with"
            f" replication = {how}")
        return f"created keyspace {self._ks('dst', db)}"

    #: the name ScyllaDB gives a table's change log, beside the table: the
    #: server's, not the application's
    CDC_LOG = "_scylla_cdc_log"

    def neutral_tables(self, side, db):
        return sorted(r.table_name for r in self._session(side).execute(
            "select table_name from system_schema.tables where"
            " keyspace_name = %s", (self._ks(side, db),))
            if not self.hop.excluded(db, r.table_name)
            and not r.table_name.endswith(self.CDC_LOG))

    def _columns(self, side, db, table):
        return list(self._session(side).execute(
            "select column_name, type, kind, position from"
            " system_schema.columns where keyspace_name = %s and"
            " table_name = %s", (self._ks(side, db), table)))

    def neutral_columns(self, side, db, table):
        return sorted((r.column_name, r.type)
                      for r in self._columns(side, db, table))

    def neutral_key(self, side, db, table):
        rows = self._columns(side, db, table)
        order = {"partition_key": 0, "clustering": 1}
        return [r.column_name for r in sorted(
            (r for r in rows if r.kind in order),
            key=lambda r: (order[r.kind], r.position))]

    # ---- values -----------------------------------------------------------

    @staticmethod
    def _value(v):
        """What the driver hands back, as the values every engine reads:
        its own date and time types to Python's, a UUID to its text."""
        import uuid
        if v is None:
            return None
        if type(v).__name__ == "Date" and hasattr(v, "date"):
            return v.date()
        if type(v).__name__ == "Time" and hasattr(v, "time"):
            return v.time()
        if isinstance(v, uuid.UUID):
            return str(v)
        if isinstance(v, (list, tuple)) or type(v).__name__ == "SortedSet":
            # a list, a set (sorted, as the driver keeps one) or a vector:
            # the list of its elements, each read as the rest are
            return [CassandraEngine._value(x) for x in v]
        if hasattr(v, "items") and not isinstance(v, str):
            # a map (`OrderedMapSerializedKey`): the mapping, keys as text
            # are what a JSON object has
            return {(k if isinstance(k, str) else str(k)):
                    CassandraEngine._value(x) for k, x in v.items()}
        return v

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        from cassandra.query import SimpleStatement
        if where:
            raise SystemExit(f"{table} moves under a row filter, which is"
                             " SQL, and Cassandra takes none")
        from .. import pieces
        names = [n for n, _ in columns]
        # a page of as many rows as a batch's bytes hold at the table's
        # widest (a row is a mutation, 16 MB by default at most), handed on
        # in batches of `size` rows or `pieces.BATCH_BYTES`
        stmt = SimpleStatement(
            f"select {', '.join(self._q(n) for n in names)} from"
            f" {self._qualified(side, db, table)}",
            fetch_size=pieces.rows_for(pieces.widest(self, side, db, table),
                                       int(size)))
        result = self._session(side).execute(stmt)
        yield from pieces.by_bytes(([self._value(v) for v in row]
                                    for row in result), most_rows=int(size))

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """Rows come back by token, not by key: the copier reads a
        Cassandra table in one pass (`RESUMES_BY_KEY`). This serves the
        callers that ask whether a table holds anything."""
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or 1000, where):
            rows += batch
            if limit and len(rows) >= limit:
                return rows[:limit], None
        return rows, None

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        from cassandra.concurrent import execute_concurrent_with_args

        from .. import canon
        if not key or not keys:
            return {}
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and Cassandra"
                             " takes none")
        session = self._session(side)
        stmt = session.prepare(
            f"select {', '.join(self._q(n) for n, _ in columns)} from"
            f" {self._qualified(side, db, table)} where "
            + " and ".join(f"{self._q(k)} = ?" for k in key))
        got = execute_concurrent_with_args(
            session, stmt, [tuple(canon.sql_value(v) for v in kk)
                            for kk in keys], concurrency=32)
        found = []
        for ok, result in got:
            if not ok:
                raise result
            found += [[self._value(v) for v in row] for row in result]
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 5000,
                                          where):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        return (n, str(total))

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    # ---- writing ----------------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        if not key:
            raise SystemExit(f"{table} has no key, and a Cassandra table is"
                             " found by its key: give the table one, or leave"
                             " it out of this hop")
        defs = [f"{self._q(c[0])} {canon.ddl_type('cassandra', c[1], c[2])}"
                for c in columns]
        return (f"create table {self._qualified(side, db, table)}"
                f" ({', '.join(defs)}, primary key"
                f" (({', '.join(self._q(k) for k in key)})))")

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a table")
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        ddl = self.neutral_create_sql(side, db, table, columns, key)
        self._session(side).execute(ddl)
        return ddl

    def neutral_write(self, side, db, table, columns, rows):
        """An insert per row, which replaces one with the same key; a null
        left unset, which writes no tombstone."""
        from cassandra.concurrent import execute_concurrent_with_args
        from cassandra.query import UNSET_VALUE

        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        session = self._session(side)
        names = [n for n, _ in columns]
        stmt = session.prepare(
            f"insert into {self._qualified(side, db, table)}"
            f" ({', '.join(self._q(n) for n in names)}) values"
            f" ({', '.join('?' for _ in names)})")
        got = execute_concurrent_with_args(
            session, stmt, [tuple(UNSET_VALUE if v is None
                                  else canon.write_value("cassandra", c, v)
                                  for (_, c), v in zip(columns, r))
                            for r in rows], concurrency=64)
        failed = [result for ok, result in got if not ok]
        if failed:
            raise SystemExit(f"{table}: the target refused {len(failed)}"
                             f" rows: {str(failed[0])[:150]}")
        return len(rows)

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and Cassandra"
                             " takes none")
        self._session(side).execute(
            f"truncate {self._qualified(side, db, table)}")
        return None

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_deep(self, db):
        """How many copies the target's keyspace keeps (`_copies_kept`),
        and every table's cells' write times and times to live, held to
        the source's (`_times`)."""
        out = self._copies_kept(db)
        if out[0].status == "diff" and "not on the target" in out[0].detail:
            return out
        for t in self.neutral_tables("src", db):
            try:
                out.append(self._times(db, t))
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(Result("deep", f"{db}.{t} cell times", "error",
                                  "could not read the cells' times:"
                                  f" {str(e).splitlines()[0][:90]}"))
        return out

    #: rows of a table whose cells' times are compared: every row of a
    #: smaller one, of a larger the first this many in token order, which
    #: spreads them over its keys
    TIMES_SAMPLE = 5000
    #: how far a cell's write time and its time to live may differ and
    #: still be the same: a second and a minute, the tolerances ScyllaDB's
    #: own migration validator checks with (`writetimeToleranceMillis`,
    #: `ttlToleranceMillis`)
    WRITETIME_SLACK_US, TTL_SLACK_S = 1_000_000, 60

    def _times(self, db, table):
        """One table's cells held to the source's by when they were written
        and when they expire - which the data check, comparing values,
        cannot see: a row copied without its time to live lives on for good
        on the target, and one copied with a later write time wins over a
        change the source made after it. Read for a sample of the source's
        rows (`TIMES_SAMPLE`) and the same rows by key on the target; a
        time to live compared as the moment it ends, so the seconds between
        the two reads do not count against it. Counters are compared by
        their value: a counter is only ever added to, and no copy writes
        one - added again, it would double."""
        import time

        from cassandra.concurrent import execute_concurrent_with_args
        from cassandra.query import SimpleStatement
        cols = self._columns("src", db, table)
        key = self.neutral_key("src", db, table)
        counters = [c.column_name for c in cols if str(c.type) == "counter"]
        plain = [c.column_name for c in cols
                 if c.kind in ("regular", "static")]
        timed = [c.column_name for c in cols
                 if c.kind in ("regular", "static")
                 and c.column_name not in counters
                 and self._one_cell(c.type)]
        loose = [n for n in plain if n not in timed and n not in counters]
        q = self._q
        sel = (", ".join(q(n) for n in key + plain)
               + "".join(f", writetime({q(n)}), ttl({q(n)})" for n in timed))
        began = time.time()
        rows = list(self._session("src").execute(SimpleStatement(
            f"select {sel} from {self._qualified('src', db, table)} limit"
            f" {self.TIMES_SAMPLE + 1}", fetch_size=1000)))
        whole = len(rows) <= self.TIMES_SAMPLE
        rows = rows[:self.TIMES_SAMPLE]
        dst = self._session("dst")
        stmt = dst.prepare(f"select {sel} from"
                           f" {self._qualified('dst', db, table)} where "
                           + " and ".join(f"{q(k)} = ?" for k in key))
        asked = time.time()
        got = execute_concurrent_with_args(
            dst, stmt, [tuple(r[:len(key)]) for r in rows], concurrency=64)
        at = {n: i for i, n in enumerate(key + plain)}
        base = len(key) + len(plain)
        missing, cells, when, expiry, counted = 0, 0, [], [], []
        for row, (ok, result) in zip(rows, got):
            if not ok:
                raise result
            there = list(result)
            if not there:
                missing += 1
                continue
            other = there[0]
            ident = tuple(row[:len(key)])
            for i, n in enumerate(timed):
                a_wt, a_ttl = row[base + 2 * i], row[base + 2 * i + 1]
                b_wt, b_ttl = other[base + 2 * i], other[base + 2 * i + 1]
                if a_wt is None or b_wt is None:
                    # a cell on one side only: a value that differs, which
                    # the data check names
                    continue
                cells += 1
                if abs(int(a_wt) - int(b_wt)) > self.WRITETIME_SLACK_US:
                    when.append((ident, n))
                if (a_ttl is None) != (b_ttl is None) or (
                        a_ttl is not None and abs(
                            (began + a_ttl) - (asked + b_ttl))
                        > self.TTL_SLACK_S):
                    expiry.append((ident, n))
            for n in counters:
                if row[at[n]] != other[at[n]]:
                    counted.append((ident, n))
        how = ("every row" if whole
               else "a sample, the first in token order")
        read = f"{len(rows):,} rows ({how}) read on both sides"
        said = []
        if counters:
            said.append(f"{len(counters)} counter column(s) compared by"
                        " value - a counter is only ever added to, and no"
                        " copy writes one")
        if loose:
            said.append(f"{', '.join(loose[:4])} hold(s) a collection or"
                        " user type with no single write time, compared by"
                        " value only")
        if missing:
            said.append(f"{missing:,} of them not on the target (the data"
                        " check names them)")
        if not (when or expiry or counted):
            return Result("deep", f"{db}.{table} cell times", "ok",
                          f"{read}: {cells:,} cells' write times within 1 s"
                          " and times to live within 60 s of the source's"
                          + "".join(f"; {s}" for s in said))

        def few(found):
            return ", ".join(f"{k if len(k) > 1 else k[0]}.{n}"
                             for k, n in found[:4])
        bad = []
        if when:
            bad.append(f"write time off by more than 1 s on {len(when):,}"
                       f" cells ({few(when)})")
        if expiry:
            bad.append(f"time to live off by more than 60 s, or on one side"
                       f" only, on {len(expiry):,} cells ({few(expiry)})")
        if counted:
            bad.append(f"counter value differs on {len(counted):,}"
                       f" ({few(counted)})")
        return Result("deep", f"{db}.{table} cell times", "diff",
                      f"{read}: " + "; ".join(bad + said), "",
                      "copy the table again by the move's own path, which"
                      " writes each cell with its time to live and write"
                      " time; a counter only by adding to it")

    def _copies_kept(self, db):
        """How many copies the target's keyspace keeps: with one, a node
        lost is the data lost, and a cutover onto it has less than the
        source had."""
        try:
            got = list(self._session("dst").execute(
                "select replication from system_schema.keyspaces where"
                " keyspace_name = %s", (self._ks("dst", db),)))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target replication", "error",
                           "could not read the target's keyspace:"
                           f" {str(e).splitlines()[0][:90]}")]
        if not got:
            return [Result("deep", f"{db} target replication", "diff",
                           "the keyspace is not on the target")]
        how = dict(got[0].replication)
        copies = [int(v) for k, v in how.items()
                  if k not in ("class",) and str(v).isdigit()]
        least = min(copies) if copies else 0
        if least < 3:
            return [Result("deep", f"{db} target replication", "warn",
                           f"the target keeps {least} cop"
                           f"{'y' if least == 1 else 'ies'} of each row"
                           f" ({how.get('class', '').split('.')[-1]}):"
                           " a node lost can be rows lost", "",
                           "alter keyspace ... with replication = ... and"
                           " repair, before the cutover")]
        return [Result("deep", f"{db} target replication", "ok",
                       f"the target keeps {least} copies of each row")]

    # ---- settings -------------------------------------------------------

    #: what changes where a row lives or how long it stays: another
    #: partitioner places every key elsewhere, a table's default time to
    #: live expires rows the source keeps, a shorter gc grace lets a
    #: deleted row come back from a node that missed the delete; and what
    #: reads a table's changes - a target table with no change log feeds
    #: nothing that read the source's
    CRITICAL_PARAMS = ("partitioner", "default_time_to_live",
                       "gc_grace_seconds", "change_log")

    def _settings(self, side, db):
        s, out = self._session(side), {}
        try:
            for r in s.execute("select name, value from"
                               " system_views.settings"):
                out[str(r.name)] = str(r.value)
        except Exception as e:  # noqa: BLE001 - before 4.0 there is none
            out["server"] = "not answered: " + str(e).splitlines()[0][:60]
        ks = self._ks(side, db)
        for r in s.execute("select replication, durable_writes from"
                           " system_schema.keyspaces where keyspace_name ="
                           " %s", (ks,)):
            out["keyspace.replication"] = json.dumps(dict(r.replication),
                                                     sort_keys=True)
            out["keyspace.durable_writes"] = str(r.durable_writes)
        rows = [r._asdict() for r in s.execute(
            "select * from system_schema.tables where keyspace_name = %s",
            (ks,))]
        # a ScyllaDB table's change log is a table beside it, there exactly
        # while the log is on
        logged = {r["table_name"][:-len(self.CDC_LOG)] for r in rows
                  if r["table_name"].endswith(self.CDC_LOG)}
        for row in rows:
            t = row.pop("table_name")
            if self.hop.excluded(db, t) or t.endswith(self.CDC_LOG):
                continue
            for k, v in row.items():
                if k in ("keyspace_name", "id"):
                    continue
                out[f"{t}.{k}"] = json.dumps(
                    dict(v) if hasattr(v, "items") else v, sort_keys=True,
                    default=str)
            if self._scylla(side):
                out[f"{t}.change_log"] = "on" if t in logged else "off"
        return {k: v for k, v in out.items()
                if not any(w in k.lower() for w in ("password", "keystore",
                                                    "secret"))}

    def check_params(self, db):
        """The server's settings (its own virtual table), the keyspace's
        replication and every table's options, both sides, through the
        report every engine's settings go through."""
        def pull(side):
            try:
                return self._settings(side, db)
            except Exception as e:  # noqa: BLE001 - said by the report
                return {self.UNREADABLE: str(e).splitlines()[0][:80]}
        src, dst = pull("src"), pull("dst")
        crit = sorted({n for n in set(src) | set(dst)
                       if n.split(".")[-1] in self.CRITICAL_PARAMS})
        return self._param_result(
            db, src, dst, crit,
            "give the target's tables the source's time to live and gc"
            " grace, and the same partitioner, before cutover; a change log"
            " the source keeps is turned on for the target's table once its"
            " copy is done - a table whose log is on refuses the copy's"
            " writes dated at the source's write times")

    def snapshot_state(self, db, state_dir, kind="all"):
        """What the target holds before a repair: each table's definition
        and settings, and its rows counted. A snapshot of the files is
        each node's own (`nodetool snapshot`), which a client does not
        reach; that is said in the record rather than taken for done."""
        s = self._session("dst")
        out = {"no_snapshot": "a snapshot of a table's files is taken on"
                              " each node, which a client cannot reach",
               "settings": self._settings("dst", db), "rows": {}}
        for t in self.neutral_tables("dst", db):
            out["rows"][t] = s.execute(
                f"select count(*) from {self._qualified('dst', db, t)}"
            ).one()[0]
        (state_dir / "dst-tables.json").write_text(
            json.dumps(out, indent=2, sort_keys=True, default=str))

    # ---- rows copied as they are, by token ranges ---------------------------

    #: token ranges a table is read in, per worker: small enough that the
    #: workers finish together
    RANGES_PER_WORKER = 8
    LOW, HIGH = -2 ** 63, 2 ** 63 - 1

    def _made_like(self, db, table):
        """The target table made by the source's own definition - its
        clustering order, collections, types and indexes - under the
        target's keyspace name. Not with the source's change log on: a
        ScyllaDB table whose log is on refuses a write dated before the
        log's current generation began (measured on 6.2: `cdc: attempted to
        get a stream "from the past"`), and every cell the copy writes is
        dated at the source's own write time. Where the target should keep
        a log, it is turned on there after the cutover; the settings check
        names the difference until then."""
        import re
        ks = self._ks("src", db)
        cluster = self._session("src").cluster
        if ks not in cluster.metadata.keyspaces:
            # a keyspace made since this session connected: the table's
            # refresh has nothing to hang it on until the keyspace is read
            cluster.refresh_keyspace_metadata(ks)
        cluster.refresh_table_metadata(ks, table)
        text = cluster.metadata.keyspaces[ks].tables[table] \
            .export_as_string()
        into = self._q(self._ks("dst", db))
        for stmt in [x.strip() for x in text.split(";\n") if x.strip()]:
            stmt = re.sub(r"^(CREATE (?:TABLE|INDEX|CUSTOM INDEX)"
                          r"(?: IF NOT EXISTS)?[^(]*?)\s(\S+?)\.",
                          lambda m: f"{m.group(1)} {into}.", stmt, count=1)
            stmt = re.sub(r"\bON\s+\S+?\.", f"ON {into}.", stmt, count=1) \
                if stmt.upper().startswith(("CREATE INDEX",
                                            "CREATE CUSTOM INDEX")) else stmt
            stmt = re.sub(r"\s+AND\s+cdc\s*=\s*\{[^}]*\}", "", stmt,
                          flags=re.I)
            self._session("dst").execute(stmt.rstrip(";"))

    #: the types whose value is one cell, with one time to live and one
    #: write time; a collection or a user type that is not frozen is a
    #: cell a member, and has none of its own
    ONE_CELL = ("ascii", "bigint", "blob", "boolean", "counter", "date",
                "decimal", "double", "duration", "float", "inet", "int",
                "smallint", "text", "time", "timestamp", "timeuuid",
                "tinyint", "uuid", "varchar", "varint")

    @classmethod
    def _one_cell(cls, declared):
        text = str(declared).strip()
        return text in cls.ONE_CELL or text.startswith(("frozen<", "tuple<",
                                                         "vector<"))

    def _row_writer(self, db, table):
        """(the key's columns, the select list that reads a row with each
        cell's time to live and write time after its values, and
        `writes(row, stamp)`: the statements that write a row read by it
        back to the target, each cell with the time to live left to it and
        its own write time - the cells that share both in one statement).
        What has no single time - a collection or a user type not frozen -
        is written as now, or at `stamp` where one is given."""
        cols = self._columns("src", db, table)
        key = self.neutral_key("src", db, table)
        plain = [c.column_name for c in cols
                 if c.kind in ("regular", "static")]
        timed = [c.column_name for c in cols
                 if c.kind in ("regular", "static")
                 and self._one_cell(c.type)]
        q = self._q
        select = (", ".join(q(n) for n in key + plain)
                  + "".join(f", ttl({q(n)}), writetime({q(n)})"
                            for n in timed))
        dst = self._session("dst")
        target = self._qualified("dst", db, table)
        prepared = {}

        def statement(names, using):
            if (names, using) not in prepared:
                prepared[(names, using)] = dst.prepare(
                    f"insert into {target} ("
                    + ", ".join(q(n) for n in key + list(names))
                    + ") values (" + ", ".join("?" for _ in key + list(
                        names)) + ")" + using)
            return prepared[(names, using)]

        def writes(row, stamp=None):
            k = list(row[:len(key)])
            vals = dict(zip(plain, row[len(key):len(key) + len(plain)]))
            meta = row[len(key) + len(plain):]
            groups = {}
            for i, name in enumerate(timed):
                ttl, wt = meta[2 * i], meta[2 * i + 1]
                if vals.get(name) is None or wt is None:
                    continue
                groups.setdefault((ttl or 0, wt), []).append(name)
            untimed = tuple(nm for nm in plain if nm not in timed
                            and vals.get(nm) is not None)
            out = []
            if untimed or not groups:
                args = k + [vals[nm] for nm in untimed]
                out.append((statement(untimed, "") if stamp is None else
                            statement(untimed, " using timestamp ?"),
                            args if stamp is None else args + [int(stamp)]))
            for (ttl, wt), names in groups.items():
                out.append((statement(tuple(names),
                                      " using ttl ? and timestamp ?"),
                            k + [vals[nm] for nm in names]
                            + [int(ttl), int(wt)]))
            return out
        return key, select, writes

    def _table_ways(self):
        """How a table goes (`tableway.each`): the bulk loader where it can
        keep what migkit's copier keeps, the copier's token ranges
        always."""
        from ..decide import Need, Rung
        return (
            Rung("loader", "the bulk loader, by token-range splits, each"
                           " cell's time to live and write time kept",
                 gives=frozenset({"resume by chunk"}),
                 needs=(Need("the bulk loader installed",
                             lambda f: f["loader"],
                             "the bulk loader is not installed here"),
                        Need("a table it carries as the copier does",
                             lambda f: f["unfit"] is None,
                             lambda f: f["unfit"])),
                 because="Java's own driver reading and writing many"
                         " ranges at once, resumable where it stopped"),
            Rung("ranges", "the token ranges, read side by side",
                 because="each row written back with its time to live and"
                         " write time"),
        )

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table's rows copied as the source holds them, each written
        back with what is left of its time to live and its write time - so
        a row that expires on the source expires on the target at the same
        moment, and a later write on either side still wins as it would
        have - by the way climbed for the table (`_table_ways`): the bulk
        loader (`dsbulk`) or migkit's own token ranges. A table the target
        lacks is made by the source's own definition. A counter table is
        refused by name: a counter is added to, never written."""
        steps = [f"{t}: rows copied with their time to live and write"
                 " time, by token ranges" for t in tables]
        if not go:
            return steps
        from .. import dsbulk, tableway
        from ..decide import Facts
        from ..util import which
        workers = max(1, int(getattr(self.hop, "workers_most", 0)
                             or self.hop.workers or 1))
        for t in list(shape_only) + list(tables):
            if t not in self.neutral_tables("dst", db):
                self._made_like(db, t)
        for t in tables:
            if any(str(c.type) == "counter"
                   for c in self._columns("src", db, t)):
                raise SystemExit(f"{t} holds counters, which are added to"
                                 " and never written: it is not copied by"
                                 " this path")

        def run(way, t):
            if way == "loader":
                moved = dsbulk.copy_table(self, db, t, workers, log)
                if log:
                    log(f"{t}: {moved:,} rows copied by the bulk loader,"
                        " counted on the target")
                return moved
            return self._copy_by_ranges(db, t, workers, log)
        tableway.each(self, db, tables, self._table_ways(),
                      lambda t: Facts(loader=lambda: bool(which(
                          dsbulk.PROGRAM)), unfit=lambda: dsbulk.unfit(
                          self, db, t)),
                      run, log)
        return steps

    def _copy_by_ranges(self, db, t, workers, log):
        """One table by token ranges read side by side, each row written
        back with its time to live and write time; the rows copied."""
        from concurrent.futures import ThreadPoolExecutor
        cols = self._columns("src", db, t)
        part = [c.column_name for c in sorted(
            (c for c in cols if c.kind == "partition_key"),
            key=lambda c: c.position)]
        q = self._q
        _, select, writes = self._row_writer(db, t)
        read = self._session("src").prepare(
            f"select {select} from {self._qualified('src', db, t)}"
            " where token(" + ", ".join(q(n) for n in part)
            + ") >= ? and token(" + ", ".join(q(n) for n in part)
            + ") <= ?")
        dst = self._session("dst")
        target = self._qualified("dst", db, t)
        dst.execute(f"truncate {target}")
        n = workers * self.RANGES_PER_WORKER
        step = (self.HIGH - self.LOW) // n
        bounds = [(self.LOW + i * step,
                   self.HIGH if i == n - 1 else self.LOW + (i + 1) * step
                   - 1) for i in range(n)]

        def one(rng):
            from cassandra.concurrent import execute_concurrent
            moved, batch = 0, []
            for row in self._session("src").execute(read, rng):
                batch += writes(row)
                moved += 1
                if len(batch) >= 500:
                    self._sent(execute_concurrent(dst, batch,
                                                  concurrency=64), t)
                    batch = []
            if batch:
                self._sent(execute_concurrent(dst, batch,
                                              concurrency=64), t)
            return moved
        with ThreadPoolExecutor(max_workers=workers) as pool:
            moved = sum(pool.map(one, bounds))
        if log:
            log(f"{t}: {moved:,} rows copied in {n} token ranges")
        return moved

    @staticmethod
    def _sent(results, table):
        failed = [r for ok, r in results if not ok]
        if failed:
            raise SystemExit(f"{table}: the target refused {len(failed)}"
                             f" rows: {str(failed[0])[:150]}")

    # ---- following ScyllaDB's change log -------------------------------------

    #: seconds the follow stays behind the source's clock (the hop's
    #: `cdc_confidence_seconds`): a write stamped by its client a moment
    #: before it reaches a replica is still arriving until then
    CDC_CONFIDENCE = 10
    #: what a row of a change log records (`cdc$operation`)
    UPDATE, INSERT, ROW_DELETE, PARTITION_DELETE = 1, 2, 3, 4
    RANGE_FROM = (5, 6)     # a range's start, taken in / left out
    RANGE_TO = (7, 8)       # its end, the same
    CHANGE_POINT_READS_ONLY = True

    def _scylla(self, side):
        """Whether the server is ScyllaDB: it keeps its own schema tables
        beside Cassandra's."""
        got = self.__dict__.setdefault("_is_scylla", {})
        if side not in got:
            try:
                self._session(side).execute(
                    "select keyspace_name from system_schema.scylla_tables"
                    " limit 1")
                got[side] = True
            except Exception:  # noqa: BLE001 - no such table: Cassandra
                got[side] = False
        return got[side]

    def _logged(self, db):
        """The source's tables, each with its change log on - or the reason
        it cannot be followed. Cassandra writes its changes where only a
        program on every node reads them (the commit log's change files,
        which the node also stops writing once they fill), so a Cassandra
        source is refused, not polled: a poll cannot see a row written and
        deleted between two looks. On ScyllaDB a table's log is its own
        setting, which migkit does not change."""
        s = self._session("src")
        if not self._scylla("src"):
            raise SystemExit(
                f"{db}: Cassandra keeps its changes in the commit log's"
                " change files on each node, which only a program running"
                " on every node reads - there is nothing a client can"
                " follow. Stop writes on the source and move it, then prove"
                " it with migkit check")
        try:
            kept = list(s.execute("select * from"
                                  " system_schema.scylla_keyspaces where"
                                  " keyspace_name = %s", (db,)))
        except Exception:  # noqa: BLE001 - a release before tablets
            kept = []
        for r in kept:
            if getattr(r, "initial_tablets", None) is not None:
                # a tablets keyspace's log streams split and merge with its
                # tablets, listed per table where a release keeps one; no
                # reader of them is trusted until one is proved against a
                # server that keeps them, and 6.2 keeps none
                raise SystemExit(
                    f"{db} keeps its data in tablets, whose change log"
                    " streams split and merge with them, and migkit follows"
                    " only a log it has proved it reads whole. Stop writes"
                    " on the source and move it")
        # a table's log is there exactly while its log is on: the server
        # makes it with the setting and drops it without
        logs = {r.table_name for r in s.execute(
            "select table_name from system_schema.tables where"
            " keyspace_name = %s", (db,))
            if r.table_name.endswith(self.CDC_LOG)}
        out = []
        for t in self.neutral_tables("src", db):
            if any(str(c.type) == "counter"
                   for c in self._columns("src", db, t)):
                raise SystemExit(
                    f"{db}.{t} holds counters, which are only ever added to:"
                    " a change carried twice would count twice, and no copy"
                    " writes one. Stop writes on the source, then compare it"
                    " with migkit check, which holds counters to their"
                    " values")
            if t + self.CDC_LOG not in logs:
                raise SystemExit(
                    f"{db}.{t} has no change log on, and turning it on is a"
                    " change to the source's table, which migkit does not"
                    f" make: ALTER TABLE {db}.{t} WITH cdc = {{'enabled':"
                    " true}, then move again with --mode full+cdc")
            out.append(t)
        return out

    def _now_ms(self, side):
        return int(self._session(side).execute(
            "select toUnixTimestamp(now()) from system.local").one()[0])

    def _confidence(self):
        return float((self.hop.options or {}).get(
            "cdc_confidence_seconds", self.CDC_CONFIDENCE))

    def change_point(self, side, db):
        """Now less the confidence window, on the source's own clock: a
        follow started here reads again what was written just before, which
        carrying twice leaves as it is."""
        self._logged(db)
        return {"at": self._now_ms(side) - int(self._confidence() * 1000)}

    def log_position(self, side, db):
        return {"at": self._now_ms(side)}

    @staticmethod
    def position_reached(have, want):
        if not have or not want:
            return None
        return int(have.get("at", 0)) >= int(want.get("at", 0))

    def stream_identity(self, side, db):
        """The cluster a saved position belongs to, by its name: a position
        is a time on the source's clock, which a node added or replaced
        leaves where it was."""
        r = self._session(side).execute("select cluster_name from"
                                        " system.local").one()
        return {"cluster": str(r[0])}

    def server_identity(self, side, db):
        """What the cluster says it is, for the interlock on the target:
        its name and its nodes' own ids - two clusters left at the name
        their image starts with (`Test Cluster`) are told apart by their
        nodes - its release, and whether the keyspace is there yet."""
        s = self._session(side)
        me = s.execute("select cluster_name, host_id, release_version from"
                       " system.local").one()
        nodes = sorted([str(me.host_id)] + [
            str(r.host_id) for r in s.execute("select host_id from"
                                              " system.peers")])
        out = {"id": {"cluster": str(me.cluster_name),
                      "nodes": ",".join(nodes)},
               "name": str(me.cluster_name),
               "version": str(me.release_version)}
        if not list(s.execute("select keyspace_name from"
                              " system_schema.keyspaces where"
                              " keyspace_name = %s", (self._ks(side, db),))):
            out["absent"] = True
        return out

    def position_lost(self, side, db, token):
        """The log keeps a change for as long as the table's `cdc` `ttl`
        says (a day unless set): a position older than that has lost what
        was written after it."""
        if not token or not self._scylla(side):
            return None
        keep = None
        for t in self.neutral_tables(side, db):
            try:
                got = list(self._session(side).execute(
                    "select cdc from system_schema.scylla_tables where"
                    " keyspace_name = %s and table_name = %s", (db, t)))
            except Exception:  # noqa: BLE001 - kept elsewhere: not said
                return None
            for r in got:
                ttl = int(dict(r.cdc or {}).get("ttl", 86400) or 0)
                if ttl:
                    keep = ttl if keep is None else min(keep, ttl)
        if keep and self._now_ms(side) - int(token.get("at", 0)) \
                > keep * 1000:
            return (f"the change log keeps {keep:,} seconds of changes, and"
                    " the saved position is older")
        return None

    def _generations(self):
        """[(start in ms, [stream ids])] of the source's change log, oldest
        first: each generation's streams carry what was written from its
        start to the next one's."""
        import calendar
        s = self._session("src")
        out = []
        for r in sorted(s.execute(
                "select time from system_distributed"
                ".cdc_generation_timestamps where key = 'timestamps'"),
                key=lambda r: r.time):
            streams = []
            for d in s.execute("select streams from system_distributed"
                               ".cdc_streams_descriptions_v2 where time ="
                               " %s", (r.time,)):
                streams += list(d.streams or [])
            start = calendar.timegm(r.time.utctimetuple()) * 1000 \
                + r.time.microsecond // 1000
            out.append((start, streams))
        return out

    def _log_rows(self, db, table, lo, hi):
        """The rows one table's log holds for writes in (lo, hi] ms, as
        dicts, each stream read on its own (the order holds within one)."""
        import datetime

        from cassandra.concurrent import execute_concurrent_with_args
        s = self._session("src")
        stmt = s.prepare(
            f"select * from {self._qualified('src', db, table + self.CDC_LOG)}"
            ' where "cdc$stream_id" = ? and "cdc$time" > maxTimeuuid(?) and'
            ' "cdc$time" <= maxTimeuuid(?)')
        since = datetime.datetime.fromtimestamp(lo / 1000,
                                                datetime.timezone.utc)
        until = datetime.datetime.fromtimestamp(hi / 1000,
                                                datetime.timezone.utc)
        gens = self._generations()
        found = []
        for i, (start, streams) in enumerate(gens):
            end = gens[i + 1][0] if i + 1 < len(gens) else None
            if (end is not None and end <= lo) or start > hi:
                continue
            for ok, got in execute_concurrent_with_args(
                    s, stmt, [(sid, since, until) for sid in streams],
                    concurrency=64):
                if not ok:
                    raise got
                names = got.column_names
                found += [dict(zip(names, r)) for r in got]
        return found

    @staticmethod
    def _micros(timeuuid):
        """A log row's `cdc$time` as the write time it holds, in
        microseconds since the epoch."""
        return (timeuuid.time - 0x01B21DD213814000) // 10

    def _changes(self, db, table, rows):
        """What one table's log rows ask of the target, in the log's
        terms: ([(delete statement, its values)], {full key: the latest
        write time that touched it}). A row, a partition, a range of rows
        and a column set to nothing are each deleted at the write time the
        log gives them; a collection written whole is emptied just before
        it, as the server itself does."""
        key = self.neutral_key("src", db, table)
        cols = self._columns("src", db, table)
        part = [c.column_name for c in sorted(
            (c for c in cols if c.kind == "partition_key"),
            key=lambda c: c.position)]
        clus = [k for k in key if k not in part]
        whole = {c.column_name for c in cols
                 if c.kind in ("regular", "static")
                 and not self._one_cell(c.type)}
        plain = [c.column_name for c in cols
                 if c.kind in ("regular", "static")]
        q, target = self._q, self._qualified("dst", db, table)
        deletes, touched, opened = [], {}, {}
        rows = sorted(rows, key=lambda r: (r["cdc$stream_id"],
                                           r["cdc$time"].time,
                                           r["cdc$batch_seq_no"]))
        for r in rows:
            op, at = int(r["cdc$operation"]), self._micros(r["cdc$time"])
            pk = [r[n] for n in part]
            if op in (self.UPDATE, self.INSERT):
                full = tuple(r[n] for n in key)
                touched[full] = max(at, touched.get(full, 0))
                for n in plain:
                    if r.get(f"cdc$deleted_{n}"):
                        deletes.append((
                            f"delete {q(n)} from {target} using timestamp"
                            " ? where " + " and ".join(
                                f"{q(k)} = ?" for k in key),
                            [at - 1 if n in whole else at] + list(full)))
            elif op == self.ROW_DELETE:
                deletes.append((f"delete from {target} using timestamp ?"
                                " where " + " and ".join(
                                    f"{q(k)} = ?" for k in key),
                                [at] + [r[n] for n in key]))
            elif op == self.PARTITION_DELETE:
                deletes.append((f"delete from {target} using timestamp ?"
                                " where " + " and ".join(
                                    f"{q(k)} = ?" for k in part),
                                [at] + pk))
            elif op in self.RANGE_FROM:
                opened[r["cdc$stream_id"]] = r
            elif op in self.RANGE_TO:
                start = opened.pop(r["cdc$stream_id"], None)
                conds, args = [f"{q(k)} = ?" for k in part], list(pk)
                for bound, sign in ((start, (">=", ">")), (r, ("<=", "<"))):
                    if bound is None:
                        continue
                    have = [k for k in clus if bound.get(k) is not None]
                    if not have:
                        continue
                    op_ = sign[0 if int(bound["cdc$operation"]) in (5, 7)
                               else 1]
                    conds.append(f"{q(have[0])} {op_} ?" if len(have) == 1
                                 else "(" + ", ".join(q(k) for k in have)
                                 + f") {op_} ("
                                 + ", ".join("?" for _ in have) + ")")
                    args += [bound[k] for k in have]
                deletes.append((f"delete from {target} using timestamp ?"
                                f" where {' and '.join(conds)}",
                                [at] + args))
        return deletes, touched

    def _follow_table(self, db, table, rows):
        """How many changes of one table's log were carried: the deletes
        as the log gives them, then every row it touched read again from
        the source and written as `native_bulk` writes one - each cell with
        its own write time and the time to live left to it, what has no
        single time at the latest write time that touched the row. The
        target's own last-write-wins then orders what arrives, so reading
        a row after a later change, or carrying a change twice, lands the
        same; and the target's cells keep the source's write times, which
        the deep check holds them to."""
        from cassandra.concurrent import (execute_concurrent,
                                          execute_concurrent_with_args)
        if not rows:
            return 0
        if table not in self.neutral_tables("dst", db):
            self._made_like(db, table)
        dst = self._session("dst")
        deletes, touched = self._changes(db, table, rows)
        prepared = {}
        batch = []
        for text, args in deletes:
            if text not in prepared:
                prepared[text] = dst.prepare(text)
            batch.append((prepared[text], args))
        self._sent(execute_concurrent(dst, batch, concurrency=64), table)
        key, select, writes = self._row_writer(db, table)
        src = self._session("src")
        read = src.prepare(f"select {select} from"
                           f" {self._qualified('src', db, table)} where "
                           + " and ".join(f"{self._q(k)} = ?" for k in key))
        keys = list(touched)
        batch = []
        for full, (ok, got) in zip(keys, execute_concurrent_with_args(
                src, read, keys, concurrency=64)):
            if not ok:
                raise got
            for row in got:
                batch += writes(row, touched[full])
        self._sent(execute_concurrent(dst, batch, concurrency=64), table)
        return len(rows)

    def _look(self, db, token):
        """(position after, log rows carried): every table's log from the
        position up to the confidence window's edge."""
        tables = self._logged(db)
        hi = self._now_ms("src") - int(self._confidence() * 1000)
        lo = int((token or {}).get("at", hi))
        if hi <= lo:
            return {"at": lo}, 0
        went = 0
        for t in tables:
            went += self._follow_table(db, t, self._log_rows(db, t, lo, hi))
        return {"at": hi}, went

    def tail_start(self, db, token_path):
        """The position the follow begins at, taken before the copy it
        follows (`change_point`)."""
        from .. import tailctl
        if token_path.exists():
            self._logged(db)
            return False
        tailctl.begin(self, db, token_path, self.change_point("src", db))
        return True

    def tail_seed(self, db, token_path, point):
        from .. import tailctl
        tailctl.begin(self, db, token_path, point)

    def tail_apply(self, db, go, token_path, log):
        """Keep the target following a ScyllaDB source through each table's
        change log (`<table>_scylla_cdc_log`): each look reads what every
        stream of the generations in play recorded since the position, up
        to the source's clock less the confidence window, and carries it
        (`_follow_table`). A Cassandra source, a table with its log off, or
        a keyspace of tablets is refused with the reason (`_logged`)."""
        from .. import tailctl

        def look(token):
            after, went = self._look(db, token)
            return after, went, True

        def behind(token):
            if not token:
                return "no position saved yet"
            lag = self._now_ms("src") - int(token.get("at", 0))
            return f"the saved position is {lag / 1000:,.0f} s behind the" \
                   " source's clock"
        tailctl.follow(self, db, go, token_path, log, "changes", look,
                       behind, 1.0)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """The source's changes after `token` as the records any pair
        applies: every row the log touched as the source holds it now, or
        its delete where the source no longer holds it; a partition deleted
        as a delete by its partition key. A range of rows deleted has no
        key to give, and is refused rather than dropped."""
        from .. import canon
        if token is None:
            return [], self.change_point(side, db)
        tables = self._logged(db)
        hi = self._now_ms(side) - int(self._confidence() * 1000)
        lo = int(token.get("at", hi))
        if hi <= lo:
            return [], {"at": lo}
        out = []
        for t in tables:
            rows = self._log_rows(db, t, lo, hi)
            if any(int(r["cdc$operation"]) in self.RANGE_FROM for r in rows):
                raise SystemExit(
                    f"{db}.{t}: a range of rows was deleted on the source,"
                    " which a change carried to another engine cannot name"
                    " by key. Move the table again")
            # by class, as every engine's rows are found by key: a key is
            # rendered by its class, and a declared type has none
            cols = [(n, canon.type_class("cassandra", typ) or canon.OWN)
                    for n, typ in self.neutral_columns(side, db, t)]
            key = self.neutral_key(side, db, t)
            part = [c.column_name for c in self._columns(side, db, t)
                    if c.kind == "partition_key"]
            cls = dict(cols)
            full = {}
            for r in rows:
                op = int(r["cdc$operation"])
                if op == self.PARTITION_DELETE:
                    out.append(canon.change("delete", t, {
                        k: self._value(r[k]) for k in part}))
                elif op in (self.UPDATE, self.INSERT, self.ROW_DELETE):
                    k = {n: self._value(r[n]) for n in key}
                    full[tuple(canon.render_value(cls[n], k[n])
                               for n in key)] = k
            now = self.neutral_rows_by_key(
                side, db, t, cols, key,
                [tuple(k[n] for n in key) for k in full.values()]) \
                if full else {}
            names = [n for n, _ in cols]
            for ident, k in full.items():
                row = now.get(ident)
                out.append(canon.change("delete", t, k) if row is None else
                           canon.change("update", t, k,
                                        dict(zip(names, row))))
        return out, {"at": hi}

    # the fence, the confirm and the verify of only what changed are the
    # pair's, through the position above

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)
