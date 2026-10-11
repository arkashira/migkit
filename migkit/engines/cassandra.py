"""Cassandra (and ScyllaDB) as one side of a pair (backlog 34).

A database is a keyspace. A table the pair creates from another engine's is
keyed by all of the source's key columns together as its partition key, so
each row is found by its whole key and no clustering order is invented; a
table of a Cassandra source is made by the source's own definition
(`create_missing`), its indexes, triggers and views once the rows are in.
An insert replaces a row with the same key, so a batch written again lands
on itself; a null is left unset rather than written, since a written null
is a tombstone - except in a repair, which clears the cell (`replacing`).

Rows come back in the order of their partitions' tokens, not of their
keys, so a table is read in one pass, a page at a time, and looked up by
key one row at a time. A `timestamp` holds milliseconds: a source value
with microseconds arrives short of them, and the check says so, because
it is a difference.
"""
import contextlib
import json
import threading

from .base import Engine, NeutralCopier, Result

_WRITING = threading.local()


@contextlib.contextmanager
def replacing():
    """For the writes of this thread, a null is written as one - the cell
    cleared - and not left unset. A copy leaves it unset (a written null is
    a tombstone, and a row new to the target has no cell to clear); a
    repair or an undo puts a row back over one that has the cell filled."""
    was = getattr(_WRITING, "nulls", False)
    _WRITING.nulls = True
    try:
        yield
    finally:
        _WRITING.nulls = was


class CassandraEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "cassandra"
    SQL_DIALECT = None
    FOLDED_BECAUSE = "has no way to hash rows in the server"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False
    #: a table the hop renames or maps the columns of goes through the pair
    #: (`through_pair`): the own copier and the own checks below read it
    #: under the source's name
    OWN_PATHS_READ_COLUMN_MAPPING = False

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

    def _target_of(self, db, table):
        """What the hop calls a source table on the target
        (`mapping.tables`, the name rules): the table's own name where it
        renames nothing."""
        return self.hop.target_table_in(db, table)

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
            if self.in_scope(db, r.table_name)
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
        left unset, which writes no tombstone - except for a repair, which
        clears the cell (`replacing`)."""
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
        clear = getattr(_WRITING, "nulls", False)
        got = execute_concurrent_with_args(
            session, stmt, [tuple((None if clear else UNSET_VALUE)
                                  if v is None
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
        """The pair's comparison, each table that differs with the rows
        apart counted as its drilldown names them (`_counted`)."""
        pair = self._as_pair()
        return self._counted(pair, db, pair.check_data(db, table, stream))

    @staticmethod
    def _counted(pair, db, results):
        """The pair's data findings with `rows_apart` filled: for each table
        that differs, the missing, extra and changed rows its drilldown
        lists, counted key by key - exact unless the check left spans of
        the table to be repaired instead of naming every row, and then
        none, since a count of a part is not the table's. What the verdict
        holds a hop's carried changes to (`verdict.hold`)."""
        spans = set(pair._leaves_kept(db))
        for r in results:
            if r.check != "data" or r.status != "diff" or not r.table:
                continue
            leaf = pair._leaf(r.table)
            kinds = {k: len(pair._read_drill(db, leaf, k))
                     for k in ("missing", "extra", "changed")}
            if leaf in spans and not any(kinds.values()):
                continue
            r.rows_apart = {str(r.table): {**kinds,
                                           "exact": leaf not in spans}}
        return results

    def check_deep(self, db):
        """How many copies the target's keyspace keeps (`_copies_kept`);
        the triggers a load took off and has not put back (`set_aside`);
        what the keyspace holds beside its rows, the tables' columns
        among it (`_structure`); who may use the tables (`_grants`); the
        values a copy can change without a word (`_values`); and every
        table's cells' write times and times to live, held to the
        source's, and the target's newest write to the source's
        (`_times`)."""
        out = self._copies_kept(db)
        if out[0].status == "diff" and "not on the target" in out[0].detail:
            return out
        for part in (self.set_aside, self._structure, self._grants,
                     self._values):
            try:
                got = part(db)
                out += got if isinstance(got, list) else [got] if got else []
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(Result("deep", f"{db} {part.__name__[1:]}",
                                  "error", "could not be read:"
                                  f" {str(e).splitlines()[0][:90]}"))
        for t in self.neutral_tables("src", db):
            try:
                out += self._times(db, t)
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
        one - added again, it would double. The cell times Result, and the
        boundary one (`_boundary`) read from the same cells."""
        import time

        from cassandra.concurrent import execute_concurrent_with_args
        from cassandra.query import SimpleStatement
        there = self._target_of(db, table)
        if self.hop.column_rules(db, table):
            return [Result("deep", f"{db}.{table} cell times", "skip",
                           "the hop maps this table's columns, so the pair"
                           " copies it without its cells' write times and"
                           " times to live, and there is nothing to hold to"
                           " the source's")]
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
                           f" {self._qualified('dst', db, there)} where "
                           + " and ".join(f"{q(k)} = ?" for k in key))
        asked = time.time()
        got = execute_concurrent_with_args(
            dst, stmt, [tuple(r[:len(key)]) for r in rows], concurrency=64)
        at = {n: i for i, n in enumerate(key + plain)}
        base = len(key) + len(plain)
        missing, cells, when, expiry, counted = 0, 0, [], [], []
        ahead = []
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
                    if int(b_wt) > int(a_wt):
                        ahead.append((ident, n))
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
        edge = self._boundary(db, table, read, cells, ahead)
        if not (when or expiry or counted):
            return [Result("deep", f"{db}.{table} cell times", "ok",
                           f"{read}: {cells:,} cells' write times within 1 s"
                           " and times to live within 60 s of the source's"
                           + "".join(f"; {s}" for s in said), table=table),
                    edge]

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
        return [Result("deep", f"{db}.{table} cell times", "diff",
                       f"{read}: " + "; ".join(bad + said), "",
                       "copy the table again by the move's own path, which"
                       " writes each cell with its time to live and write"
                       " time; a counter only by adding to it",
                       table=table), edge]

    def _boundary(self, db, table, read, cells, ahead):
        """Whether the target holds anything newer than the source: a cell
        whose write time there is later than the source's by more than the
        slack. A copy that keeps each cell's write time (`native_bulk`)
        leaves none, so one is a write the copy did not make - the
        application, or a second mover, writing into the target - and it
        outranks the source's cell from here on: last write wins."""
        def few(found):
            return ", ".join(f"{k if len(k) > 1 else k[0]}.{n}"
                             for k, n in found[:4])
        if not ahead:
            return Result("deep", f"{db}.{table} boundary", "ok",
                          f"{read}: none of {cells:,} cells is newer on the"
                          " target than on the source", table=table)
        return Result("deep", f"{db}.{table} boundary", "diff",
                      f"{read}: {len(ahead):,} of {cells:,} cells are newer"
                      f" on the target than on the source ({few(ahead)}) -"
                      " written there after the source's, by something other"
                      " than a copy that keeps write times", "",
                      "find the writer on the target before the cutover; a"
                      " cell it wrote outranks the source's from now on",
                      table=table)

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

    # ---- what a keyspace holds beside its rows ------------------------------

    #: the kinds of object read beside the tables: the schema table and the
    #: columns read of it
    KINDS = {
        "indexes": ("system_schema.indexes", "table_name, index_name, kind,"
                    " options"),
        "triggers": ("system_schema.triggers", "table_name, trigger_name,"
                     " options"),
        "views": ("system_schema.views", "view_name, base_table_name,"
                  " where_clause, include_all_columns"),
        "types": ("system_schema.types", "type_name, field_names,"
                  " field_types"),
        "functions": ("system_schema.functions", "function_name,"
                      " argument_names, argument_types, return_type,"
                      " language, body, called_on_null_input"),
        "aggregates": ("system_schema.aggregates", "aggregate_name,"
                       " argument_types, state_func, state_type, final_func,"
                       " initcond"),
        "orders": ("system_schema.columns", "table_name, column_name,"
                   " clustering_order"),
    }
    #: everything `_drift` reports, in the order it is said
    SHAPES = ("columns", "options") + tuple(KINDS)

    def _beside(self, side, db):
        """({kind: {key: description}}, {kind: why it was not read}) of the
        keyspace's secondary indexes, triggers, materialized views, user
        types, functions and aggregates, as the cluster's schema tables
        hold them. A kind the cluster cannot be asked for is said with the
        error, and is never an empty one."""
        found, unread = {}, {}
        for kind, (table, cols) in self.KINDS.items():
            try:
                rows = list(self._session(side).execute(
                    f"select {cols} from {table} where keyspace_name = %s",
                    (self._ks(side, db),)))
            except Exception as e:  # noqa: BLE001 - said, as the error
                unread[kind] = str(e).splitlines()[0][:90]
                continue
            found[kind] = dict(self._described(kind, r) for r in rows)
        return found, unread

    @staticmethod
    def _described(kind, r):
        """(key, description) of one row of a schema table: the key is the
        name the object is known by - an index's and a trigger's with its
        table - and the description is what two sides are held to."""
        opts = _opts
        if kind == "indexes":
            o = opts(r.options)
            return ((r.table_name, r.index_name),
                    (str(r.kind), o.get("target", ""),
                     o.get("class_name", "")))
        if kind == "triggers":
            return ((r.table_name, r.trigger_name),
                    (opts(r.options).get("class", ""),))
        if kind == "orders":
            return ((r.table_name, r.column_name),
                    (str(r.clustering_order).lower(),))
        if kind == "views":
            return (r.view_name, (r.base_table_name, r.where_clause or "",
                                  bool(r.include_all_columns)))
        if kind == "types":
            return (r.type_name, tuple(zip(map(str, r.field_names or ()),
                                           map(str, r.field_types or ()))))
        if kind == "functions":
            return ((r.function_name,
                     tuple(map(str, r.argument_types or ()))),
                    (tuple(map(str, r.argument_names or ())),
                     str(r.return_type), str(r.language), str(r.body),
                     bool(r.called_on_null_input)))
        return ((r.aggregate_name, tuple(map(str, r.argument_types or ()))),
                (str(r.state_func), str(r.state_type),
                 str(r.final_func or ""), str(r.initcond or "")))

    def _drift(self, db):
        """What the target lacks or holds otherwise, of what the hop takes:
        {kind: [(object, how it differs, the statement that would make it on
        the target or None)]} for the columns, indexes, triggers,
        materialized views, and the user types, functions and aggregates the
        taken tables and the hop's `objects` need, and under `unread` the
        kinds that could not be read. Read once for the checks that report
        each kind and for the schema difference (`_structural`)."""
        tables = self.neutral_tables("src", db)
        there = {t: self._target_of(db, t) for t in tables}
        out = {k: [] for k in self.SHAPES}
        src, unread_s = self._beside("src", db)
        dst, unread_d = self._beside("dst", db)
        out["unread"] = {k: f"{side}: {why}" for side, un in
                         (("source", unread_s), ("target", unread_d))
                         for k, why in un.items()}
        for t in tables:
            theirs = self._columns("dst", db, there[t])
            if theirs and not self.hop.column_rules(db, t):
                out["columns"] += self._column_drift(db, t, there[t],
                                                     theirs)
        out["options"] = self._options_drift(db, tables, there, out["unread"])
        for (table, column), want in sorted(src.get("orders", {}).items()):
            have = dst.get("orders", {}).get((there.get(table), column))
            if table in tables and have != want and have is not None \
                    and not self.hop.column_rules(db, table):
                out["orders"].append((
                    f"{table}.{column}", f"clustered {want[0]} on the"
                    f" source, {have[0]} on the target: reads come back in"
                    " the other order, and a range read from the other end",
                    None))
        for kind in ("indexes", "triggers"):
            for (table, name), want in sorted(src.get(kind, {}).items()):
                if table not in tables or not self.takes(
                        db, kind[:-2], name, by=table):
                    continue
                have = dst.get(kind, {}).get((there[table], name))
                if have != want:
                    out[kind].append((f"{table}.{name}", self._differs(
                        have, want), self._fix(db, kind, table, name, want)
                        if have is None else None))
        back = {v: k for k, v in there.items()}
        for name, want in sorted(src.get("views", {}).items()):
            if want[0] not in tables or not self.takes(db, "view", name,
                                                       by=want[0]):
                continue
            have = dst.get("views", {}).get(name)
            if have is not None:
                have = (back.get(have[0], have[0]),) + have[1:]
            if have != want:
                out["views"].append((name, self._differs(have, want), None))
        needed = self._types_needed(db, tables, src.get("types", {}))
        for name, want in sorted(src.get("types", {}).items()):
            have = dst.get("types", {}).get(name)
            if name in needed and have != want:
                out["types"].append((name, self._differs(have, want),
                                     self._fix(db, "types", None, name, want)
                                     if have is None else None))
        for kind in ("functions", "aggregates"):
            for (name, args), want in sorted(src.get(kind, {}).items()):
                if not self.takes(db, "function", name):
                    continue
                have = dst.get(kind, {}).get((name, args))
                if have != want:
                    out[kind].append((f"{name}({', '.join(args)})",
                                      self._differs(have, want),
                                      self._fix(db, kind, None, (name, args),
                                                want) if have is None
                                      else None))
        return out

    #: the options of a table that decide what it keeps and for how long:
    #: a time to live every row is written with expires them, a shorter
    #: grace brings a deleted row back from a node that missed the delete,
    #: and a time-window compaction drops whole windows of rows by their age
    HOLDING = ("default_time_to_live", "gc_grace_seconds", "compaction")

    def _options_drift(self, db, tables, there, unread):
        """[(table.option, how it differs, the statement that sets it)] for
        the options of the taken tables that decide what they keep
        (`HOLDING`), as the settings check reads them (`_settings`)."""
        try:
            mine, theirs = (self._settings(s, db) for s in ("src", "dst"))
        except Exception as e:  # noqa: BLE001 - said, as the error
            unread["options"] = str(e).splitlines()[0][:90]
            return []
        out = []
        for t in tables:
            if self.hop.column_rules(db, t):
                continue
            for opt in self.HOLDING:
                a, b = mine.get(f"{t}.{opt}"), theirs.get(
                    f"{there[t]}.{opt}")
                if a is not None and b is not None and a != b:
                    out.append((f"{t}.{opt}", f"{a} on the source, {b} on"
                                f" the target", self._set_option(
                                    db, there[t], opt, a)))
        return out

    def _set_option(self, db, table, option, text):
        """The statement that gives a table of the target an option's value
        as the source has it (`text`, as `_settings` keeps it)."""
        value = json.loads(text)
        if isinstance(value, dict):
            value = "{" + ", ".join(f"'{k}': '{v}'"
                                    for k, v in sorted(value.items())) + "}"
        return (f"alter table {self._qualified('dst', db, table)} with"
                f" {option} = {value}")

    @staticmethod
    def _differs(have, want):
        return ("not on the target" if have is None
                else f"{want} on the source, {have} on the target")

    def _types_needed(self, db, tables, types):
        """The user types the taken tables' columns name, and those the
        types they name hold in turn."""
        import re
        text = " ".join(str(c.type) for t in tables
                        for c in self._columns("src", db, t))

        def named(n, where):
            return re.search(rf'(?<![\w"]){re.escape(n)}(?![\w"])', where)
        needed, todo = set(), [n for n in types if named(n, text)]
        while todo:
            n = todo.pop()
            if n in needed:
                continue
            needed.add(n)
            inner = " ".join(t for _, t in types.get(n, ()))
            todo += [m for m in types if m not in needed and named(m, inner)]
        return needed

    def _column_drift(self, db, table, there, theirs):
        """[(column, how, the statement that adds it)] for one table whose
        columns the target holds otherwise: a column it lacks, one of
        another type, or one that is part of the key on one side and not
        on the other."""
        mine = {c.column_name: (str(c.type), c.kind, int(c.position))
                for c in self._columns("src", db, table)}
        other = {c.column_name: (str(c.type), c.kind, int(c.position))
                 for c in theirs}
        out = []
        for n, want in sorted(mine.items()):
            have = other.get(n)
            if have == want:
                continue
            if have is None:
                out.append((f"{table}.{n}", "not on the target",
                            f"alter table {self._qualified('dst', db, there)}"
                            f" add {self._q(n)} {want[0]}"))
            elif have[0] != want[0]:
                out.append((f"{table}.{n}", f"{want[0]} on the source,"
                            f" {have[0]} on the target", None))
            else:
                out.append((f"{table}.{n}", f"{want[1]} {want[2]} on the"
                            f" source, {have[1]} {have[2]} on the target",
                            None))
        return out

    def _fix(self, db, kind, table, name, want):
        """The statement that makes one missing object on the target, in the
        target's keyspace and the table's name there."""
        ks, q = self._q(self._ks("dst", db)), self._q
        on = (f"{ks}.{q(self._target_of(db, table))}" if table else None)
        if kind == "indexes":
            _, target, klass = want
            return (f"create custom index {q(name)} on {on} ({target})"
                    f" using '{klass}'" if klass else
                    f"create index {q(name)} on {on} ({target})")
        if kind == "triggers":
            return f"create trigger {q(name)} on {on} using '{want[0]}'"
        if kind == "types":
            return (f"create type {ks}.{q(name)} ("
                    + ", ".join(f"{q(f)} {t}" for f, t in want) + ")")
        fn, args = name
        if kind == "functions":
            names, returns, language, body, on_null = want
            return (f"create or replace function {ks}.{q(fn)} ("
                    + ", ".join(f"{q(a)} {t}" for a, t in zip(names, args))
                    + ") " + ("called on null input" if on_null
                              else "returns null on null input")
                    + f" returns {returns} language {language} as $${body}$$")
        state, stype, final, init = want
        return (f"create aggregate {ks}.{q(fn)} (" + ", ".join(args)
                + f") sfunc {q(state)} stype {stype}"
                + (f" finalfunc {q(final)}" if final else "")
                + (f" initcond {init}" if init else ""))

    @staticmethod
    def _said(drift, kinds, noun, hint):
        """(status, detail, report, hint) of the findings for some kinds of
        what a keyspace holds beside its rows: the ones that differ, or
        why a kind could not be read."""
        found = [x for k in kinds for x in drift[k]]
        unread = [f"{k} ({drift['unread'][k]})" for k in kinds
                  if k in drift["unread"]]
        if unread:
            return ("error", "could not read " + ", ".join(unread), "", "")
        if not found:
            return ("ok", f"every {noun} of the taken tables is on the"
                    " target as on the source", "", "")
        return ("diff", f"{len(found)} {noun}(s) differ: " + "; ".join(
            f"{n}: {how}" for n, how, _ in found[:6])
            + (f"; and {len(found) - 6} more" if len(found) > 6 else ""),
            "", hint)

    def _structure(self, db):
        """The findings for what a keyspace holds beside its rows - columns,
        indexes, triggers, materialized views, user types, functions and
        aggregates - the target's columns narrower than the source's, and
        the schema difference as a whole (`_structural`)."""
        from .. import verdict
        drift, said = self._drift(db), self._said
        out = [
            Result("deep", f"{db} columns", *said(
                drift, ("columns",), "column", "alter the target's tables"
                " to the source's columns; a column's type cannot be"
                " altered, so a table whose column differs is copied"
                " again")),
            Result("deep", f"{db} indexes", *said(
                drift, ("indexes",), "secondary index", "make the index on"
                " the target (structural-fix.cql); built over a loaded"
                " table it is built once, from its rows")),
            Result("deep", f"{db} triggers", *said(
                drift, ("triggers",), "trigger", "make the trigger on the"
                " target after the load, so it does not fire for the copy's"
                " rows")),
            Result("deep", f"{db} matviews", *said(
                drift, ("views",), "materialized view", "create the view on"
                " the target from the source's definition (describe"
                " materialized view); the move makes the views of a table"
                " it makes, once its rows are in, and the view fills itself"
                " from its base table")),
            Result("deep", f"{db} objects", *said(
                drift, ("types", "functions", "aggregates"),
                "user type, function or aggregate", "make the type,"
                " function or aggregate on the target before the tables"
                " that use it (structural-fix.cql)")),
            Result("deep", f"{db} table options", *said(
                drift, ("options", "orders"), "table option or clustering",
                "set the option on the target (structural-fix.cql); a"
                " clustering order cannot be altered, so a table clustered"
                " otherwise is copied again"),
                category="structure.collection-options")]
        narrow = []
        for table in self.neutral_tables("src", db):
            theirs = {c.column_name: str(c.type) for c in self._columns(
                "dst", db, self._target_of(db, table))}
            if self.hop.column_rules(db, table):
                continue
            for c in self._columns("src", db, table):
                why = self._narrower(str(c.type), theirs.get(c.column_name))
                if why:
                    narrow.append(f"{table}.{c.column_name}: {why}")
        out.append(verdict.narrowing_result(db, narrow))
        out.append(self._structural(db, drift))
        return out

    #: what a column of one type loses when it is held as another, where
    #: `verdict.narrowing` (whole numbers, text with a length) says nothing
    LOSES = {("double", "float"): "a double held as a float keeps 7 digits",
             ("decimal", "float"): "a decimal held as a float keeps 7"
             " digits",
             ("decimal", "double"): "a decimal held as a double keeps 15"
             " digits",
             ("varint", "bigint"): "a varint past 64 bits is refused",
             ("varint", "int"): "a varint past 32 bits is refused",
             ("varint", "smallint"): "a varint past 16 bits is refused",
             ("varint", "tinyint"): "a varint past 8 bits is refused",
             ("bigint", "float"): "a bigint held as a float keeps 24 bits",
             ("bigint", "double"): "a bigint held as a double keeps 53"
             " bits",
             ("int", "float"): "an int held as a float keeps 24 bits",
             ("text", "ascii"): "text outside ASCII is refused",
             ("varchar", "ascii"): "text outside ASCII is refused",
             ("timestamp", "date"): "the time of day is dropped"}

    def _narrower(self, mine, theirs):
        """Why a value of the source's column can fail to fit the target's,
        or None: the shared owner for whole numbers and text lengths
        (`verdict.narrowing`), then the types of this engine it does not
        know (`LOSES`)."""
        from .. import verdict
        if theirs is None or mine == theirs:
            return None
        got = verdict.narrowing("cassandra", mine, theirs)
        if got:
            return got
        loses = self.LOSES.get((mine, theirs))
        return f"{mine} -> {theirs} ({loses})" if loses else None

    def _structural(self, db, drift):
        """The schema difference as one finding, object by object: the
        statements that make the target like the source - what a statement
        can make; a column's type, a view and the rest are said, not
        guessed - written to `structural-fix.cql` beside the evidence, and
        the statements that undo them to `structural-fix.revert.cql`."""
        d = self.hop.report_dir(db)
        fix, revert = d / "structural-fix.cql", d / "structural-fix.revert.cql"
        if drift["unread"]:
            return Result("schema", f"{db} (structural)", "error",
                          "could not read " + ", ".join(
                              f"{k} ({w})" for k, w in drift["unread"]
                              .items()))
        every = [(k, n, how, st) for k in self.SHAPES
                 for n, how, st in drift[k]]
        statements = [st for *_, st in every if st]
        for path in (fix, revert):
            path.unlink(missing_ok=True)
        if not every:
            return Result("schema", f"{db} (structural)", "ok",
                          "every column, index, trigger, view, user type,"
                          " function and aggregate of the taken tables"
                          " identical, both sides")
        d.mkdir(parents=True, exist_ok=True)
        if statements:
            fix.write_text(";\n".join(statements) + ";\n")
            undo = [u for u in map(self._undone, statements) if u]
            if undo:
                revert.write_text(";\n".join(reversed(undo)) + ";\n")
        by_hand = [f"{k} {n}: {how}" for k, n, how, st in every if not st]
        return Result(
            "schema", f"{db} (structural)", "diff",
            f"{len(every)} object(s) differ, {len(statements)} with a"
            " statement that makes the target like the source"
            + (f"; by hand: {'; '.join(by_hand[:4])}"
               + (f"; and {len(by_hand) - 4} more" if len(by_hand) > 4
                  else "") if by_hand else ""),
            str(fix) if statements else "",
            "read structural-fix.cql, then run it on the target;"
            " structural-fix.revert.cql undoes it" if statements else
            "none of these can be made by a statement: copy the table again,"
            " or make the object by hand")

    @staticmethod
    def _undone(statement):
        """The statement that undoes one `_fix` or `_column_drift` made, or
        None where none is known."""
        import re
        name = r'("(?:[^"]|"")*")'
        m = re.match(rf'(?is)alter table (\S+) add {name} ', statement)
        if m:
            return f"alter table {m.group(1)} drop {m.group(2)}"
        m = re.match(rf'(?i)create (?:custom )?index {name} on ', statement)
        if m:
            ks = re.search(r' on ("(?:[^"]|"")*")\.', statement)
            return f"drop index {ks.group(1)}.{m.group(1)}" if ks else None
        m = re.match(rf'(?i)create trigger {name} on (\S+) ', statement)
        if m:
            return f"drop trigger {m.group(1)} on {m.group(2)}"
        m = re.match(r'(?i)create type (\S+?) \(', statement)
        return f"drop type {m.group(1)}" if m else None

    # ---- who may read and write the tables ---------------------------------

    def _grants(self, db):
        """Each permission a role holds on the taken tables of the source,
        on their keyspace or on all of them, held to the target's: one
        missing there is a table the application cannot read or write after
        the cutover. Read from the cluster's own auth tables
        (`users._cs_roles`), the keyspace and the table under the names the
        target gives them."""
        from .. import users
        if not self.hop.takes_object("grants", db):
            return Result("deep", f"{db} grants", "skip",
                          "the hop's objects leave roles and their grants"
                          " out")
        mine, theirs = (users._cs_roles(self.hop, s) for s in ("src", "dst"))
        tables = {t: self._target_of(db, t)
                  for t in self.neutral_tables("src", db)}
        ks = self._ks("dst", db)
        lacking, held = [], 0
        for role, info in sorted(mine.items()):
            for grant in info["grants"]:
                perm, resource = grant.split(" ", 1)
                parts = resource.split("/")
                if parts[0] != "data" or (len(parts) > 1
                                          and parts[1] != db):
                    continue
                if len(parts) == 3:
                    if parts[2] not in tables:
                        continue
                    want = f"data/{ks}/{tables[parts[2]]}"
                else:
                    want = "data" if len(parts) == 1 else f"data/{ks}"
                held += 1
                if f"{perm} {want}" not in theirs.get(role, {}).get(
                        "grants", ()):
                    lacking.append(f"{role}: {perm} on {want}"
                                   + ("" if role in theirs else
                                      " (no such role on the target)"))
        if lacking:
            return Result(
                "deep", f"{db} grants", "diff",
                f"{len(lacking)} of {held} permissions on the taken tables"
                " are not on the target: " + "; ".join(lacking[:6])
                + (f"; and {len(lacking) - 6} more" if len(lacking) > 6
                   else ""), "",
                f"migkit users {self.hop.name} sync --apply makes the roles"
                " and their permissions on the target")
        return Result("deep", f"{db} grants", "ok",
                      f"{held} permissions on the taken tables are on the"
                      " target as on the source")

    # ---- the values a copy can change without a word ------------------------

    #: rows of a table the value checks read on both sides: every row of a
    #: smaller one, of a larger the first this many in token order
    VALUES_SAMPLE = 5000

    def _pairs(self, db, table):
        """(columns as classes, the key, the source's rows, the target's
        rows by that key, whether every row was read) for one taken table,
        or None where the target lacks it or the hop maps its columns. A row
        the target does not hold is not among the target's: the data check
        names it."""
        from .. import canon
        there = self._target_of(db, table)
        if self.hop.column_rules(db, table) \
                or not self._columns("dst", db, there):
            return None
        key = self.neutral_key("src", db, table)
        # the key and the columns these checks read: a blob is not read to
        # find a text column's replacement characters
        cols = [(n, c) for n, c in (
            (n, canon.type_class("cassandra", t) or canon.OWN)
            for n, t in self.neutral_columns("src", db, table))
            if n in key or c in ("text", "float", "timestamp")]
        if not [1 for n, _ in cols if n not in key]:
            return None
        rows, whole = [], True
        for batch in self.neutral_batches("src", db, table, cols, 1000):
            rows += batch
            if len(rows) > self.VALUES_SAMPLE:
                whole = False
                break
        rows = rows[:self.VALUES_SAMPLE]
        at = {n: i for i, (n, _) in enumerate(cols)}
        theirs = self.neutral_rows_by_key(
            "dst", db, there, cols, key,
            [tuple(r[at[k]] for k in key) for r in rows])
        return cols, key, rows, theirs, whole

    @staticmethod
    def _seconds(v):
        """A timestamp as seconds since the epoch, a naive one read as the
        UTC the driver gives."""
        import calendar
        if v.tzinfo is not None:
            return v.timestamp()
        return calendar.timegm(v.timetuple()) + v.microsecond / 1e6

    def _values(self, db):
        """Four findings read from the same rows of every taken table, on
        both sides: a null turned into an empty text or the other way
        (`nullempty`), a float that drifted past the hop's
        `float_tolerance` (`float`), replacement characters the target holds
        and the source does not (`charset`), and a timestamp column every
        row of which is shifted by one offset (`timeshift`). Each row is
        read on the source and by its key on the target; a table is read
        whole when it has no more rows than `VALUES_SAMPLE`, and else its
        first rows in token order, and the finding says so."""
        import math

        from .. import verdict
        tol = float(self.hop.options.get("float_tolerance", 1e-9))
        flipped, drifted, damaged, shifts, errors = [], [], [], [], []
        read, partial = 0, []
        for table in self.neutral_tables("src", db):
            try:
                got = self._pairs(db, table)
            except Exception as e:  # noqa: BLE001 - said, as the error
                errors.append(f"{table}: {str(e).splitlines()[0][:80]}")
                continue
            if got is None:
                continue
            cols, key, rows, theirs, whole = got
            read += len(rows)
            if not whole:
                partial.append(table)
            flips, off, worst, ruined = {}, {}, {}, {}
            moments = {n: ({}, {}) for n, c in cols if c == "timestamp"}
            for r in rows:
                ident = self._key_of(cols, key, r)
                o = theirs.get(ident)
                if o is None:
                    continue
                for i, (n, c) in enumerate(cols):
                    a, b = r[i], o[i]
                    if c == "text":
                        if (a is None and b == "") or (a == "" and b is None):
                            flips[n] = flips.get(n, 0) + 1
                        if isinstance(a, str) and isinstance(b, str) \
                                and b.count("�") > a.count("�"):
                            ruined[n] = ruined.get(n, 0) + 1
                    elif c == "float" and a is not None and b is not None:
                        a, b = float(a), float(b)
                        if math.isnan(a) != math.isnan(b) or (
                                not math.isnan(a) and abs(a - b) > tol
                                * max(abs(a), abs(b), 1.0)):
                            off[n] = off.get(n, 0) + 1
                            worst[n] = max(worst.get(n, 0.0), abs(a - b))
                    elif c == "timestamp" and a is not None \
                            and b is not None:
                        moments[n][0][ident] = self._seconds(a)
                        moments[n][1][ident] = self._seconds(b)
            flipped += [f"{table}.{n}: {k} rows null on one side and empty"
                        " on the other" for n, k in sorted(flips.items())]
            drifted += [f"{table}.{n}: {k} values drift beyond the"
                        f" tolerance (largest {worst[n]:g})"
                        for n, k in sorted(off.items())]
            damaged += [f"{table}.{n}: {k} rows hold more U+FFFD"
                        " replacement characters on the target"
                        for n, k in sorted(ruined.items())]
            shifts += [s for s in (verdict.uniform_shift(
                f"{table}.{n}", *pair) for n, pair in sorted(moments.items()))
                if s]
        if errors:
            said = ("error", "could not read the rows: "
                    + "; ".join(errors[:3]), "", "")
            return [Result("deep", f"{db} nullempty", *said),
                    Result("deep", f"{db} float", *said),
                    Result("deep", f"{db} charset", *said),
                    Result("deep", f"{db} timeshift", *said)]
        how = (f"{read:,} rows read on both sides"
               + (f" (a sample - the first {self.VALUES_SAMPLE:,} in token"
                  f" order - of {', '.join(partial[:3])}"
                  + (" and more" if len(partial) > 3 else "") + ")"
                  if partial else " (every row)"))

        def one(found, ok, hint):
            return (("diff", f"{len(found)} column(s): "
                     + "; ".join(found[:6]) + f" - {how}", "", hint)
                    if found else ("ok", f"{ok} - {how}", "", ""))
        return [
            Result("deep", f"{db} nullempty", *one(
                flipped, "no text column turned a null into an empty text"
                " or the other way", "a null is a cell not written and an"
                " empty text a written one: decide which the application"
                " means, and copy the table again by the move's own path,"
                " which keeps both as they were")),
            Result("deep", f"{db} float", *one(
                drifted, "float columns within the tolerance", "a column"
                " held as a narrower float, or a value passed through text,"
                " drifts; compare the columns' types first, or set"
                " float_tolerance to accept it")),
            Result("deep", f"{db} charset", *one(
                damaged, "no replacement characters the source does not"
                " have", "the rows were written over a connection that did"
                " not read them as UTF-8; copy them again")),
            verdict.timeshift_result(db, shifts),
        ]

    # ---- before the move: what each side is, and what it will not do --------

    ENGINE_FAMILY = "cassandra"
    #: the deep checks that predict what the move will hit, asked of the
    #: source and the target before it (`assess`)
    PREFLIGHT = ("_prerequisites",)

    def _brand_probes(self):
        """What each cluster says of itself: the release it reports
        (`system.local`), and that it is ScyllaDB where it keeps that
        engine's own schema tables (`_scylla`) - which a Cassandra cluster
        has not. Only what is read is given; a brand is named by something
        it says, never by what it does not (`variants`)."""
        def one(side):
            try:
                row = self._session(side).execute(
                    "select release_version from system.local").one()
                out = {"release_version": str(row[0])}
                if self._scylla(side):
                    out["scylla"] = "yes"
                return out
            except Exception:  # noqa: BLE001 - unidentified, said
                return {}
        return (one("src"), one("dst"))

    def _server_versions(self):
        s, d = self._brands()
        return (s.version or None, d.version or None)

    def capacity(self, side, db):
        """What the node this side's client reached says it can give a move,
        for `sizing`: `free_connections` (its limit on client connections
        less those open, where it has a limit) and `running` (requests its
        read, write and client-request pools are busy with now). A key the
        node does not answer is left out: it says nothing, which the sizing
        reads as not readable, never as idle."""
        out = {}
        s = self._session(side)
        try:
            limit = next((int(r.value) for r in s.execute(
                "select name, value from system_views.settings")
                if r.name == "native_transport_max_concurrent_connections"),
                None)
            if limit is not None and limit > 0:
                open_ = len(list(s.execute(
                    "select address from system_views.clients")))
                out["free_connections"] = max(limit - open_, 0)
        except Exception:  # noqa: BLE001 - not answered: left out
            pass
        try:
            out["running"] = sum(
                int(r.active_tasks) for r in s.execute(
                    "select name, active_tasks from system_views"
                    ".thread_pools") if str(r.name) in (
                        "ReadStage", "MutationStage",
                        "Native-Transport-Requests"))
        except Exception:  # noqa: BLE001 - not answered: left out
            pass
        return out

    def _prerequisites(self, db):
        """What the move will stop on, found before it starts: a keyspace the
        target lacks and the target endpoint gives no `replication` for (how
        many copies it keeps is not migkit's to choose), and the tables that
        hold counters, which are added to and never written."""
        said = []
        if self.target_missing(db) and not self.hop.target.options.get(
                "replication"):
            said.append(f"the keyspace {self._ks('dst', db)} is not on the"
                        " target, and how many copies it keeps is not"
                        " migkit's to choose: create it, or give the target"
                        " endpoint `replication`")
        counters = [t for t in self.neutral_tables("src", db)
                    if any(str(c.type) == "counter"
                           for c in self._columns("src", db, t))]
        if counters:
            said.append(f"{', '.join(counters[:5])} hold(s) counters, which"
                        " are added to and never written: a copy of them"
                        " would double what it adds - they are compared by"
                        " value, and set on the target by hand")
        if said:
            return Result("deep", f"{db} objects", "diff", "; ".join(said),
                          "", "settle these before the move; nothing has"
                              " been written")
        return Result("deep", f"{db} objects", "ok",
                      "the target's keyspace can be made or is there, and no"
                      " taken table holds counters")

    def _handwork(self):
        """The work the move will not do, counted and named
        (`handwork.Inventory`): the functions and aggregates of the
        keyspace, which the move does not make (the statements for them are
        in the schema check's `structural-fix.cql`), the materialized views
        over tables the target already has (it makes the views of a table
        it makes), the
        trigger classes the target's nodes must have installed before a
        trigger can be made, the tables whose change log is on at the source
        (a table is made on the target without it), and the tables that
        hold counters. A kind that could not be read is unknown, never
        none."""
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                tables = self.neutral_tables("src", db)
                found, unread = self._beside("src", db)
            except Exception as e:  # noqa: BLE001 - nobody looked
                inv.unknown("server-side-code", db,
                            f"cannot read the keyspace: "
                            f"{str(e).splitlines()[0][:100]}")
                continue
            for kind, why in unread.items():
                inv.unknown("server-side-code", db,
                            f"cannot read {kind}: {why}")
            inv.add("server-side-code", db, "functions and aggregates",
                    [f"{n[0]}({', '.join(n[1])})"
                     for k in ("functions", "aggregates")
                     for n in found.get(k, {}) if self.takes(db, "function",
                                                             n[0])])
            on_target = set(self.neutral_tables("dst", db))
            inv.add("server-side-code", db, "materialized views over tables"
                    " the target already has (the move makes a view only"
                    " with the table it makes)",
                    [n for n, v in found.get("views", {}).items()
                     if v[0] in tables and v[0] in on_target
                     and self.takes(db, "view", n, by=v[0])])
            inv.add("target-prereq", db, "trigger classes installed on the"
                    " target's nodes",
                    sorted({f"{v[0]} (on {t})" for (t, n), v in
                            found.get("triggers", {}).items()
                            if t in tables}))
            inv.add("not-carried", db, "tables holding counters",
                    [t for t in tables if any(
                        str(c.type) == "counter"
                        for c in self._columns("src", db, t))])
            if self._scylla("src"):
                logged = {r.table_name[:-len(self.CDC_LOG)] for r in
                          self._session("src").execute(
                              "select table_name from system_schema.tables"
                              " where keyspace_name = %s", (db,))
                          if r.table_name.endswith(self.CDC_LOG)}
                inv.add("decide-then-apply", db, "tables whose change log is"
                        " on at the source (made on the target without one)",
                        sorted(logged & set(tables)))
        return inv

    def _assess_extra(self):
        """What has to be true before a Cassandra move is worth starting: each
        side's schema agreed among its nodes (a statement run while they
        disagree lands on some of them), the target keyspace's copies, whether
        the tables can be followed after the copy, and the work the move will
        not do (`_handwork`)."""
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope, "item": item,
                          "detail": str(detail)})
        for side, label in (("src", "source"), ("dst", "target")):
            try:
                s = self._session(side)
                mine = str(s.execute(
                    "select schema_version from system.local").one()[0])
                peers = {str(r[0]) for r in s.execute(
                    "select schema_version from system.peers")} - {"None"}
                agreed = peers <= {mine}
                add("pass" if agreed else "fail", "instance",
                    f"{label} schema agreed among its nodes",
                    "every node reports one schema version" if agreed else
                    f"{len(peers | {mine})} schema versions among its nodes"
                    " - a table made now lands on some of them; wait for"
                    " them to agree")
            except Exception as e:  # noqa: BLE001 - unknown, not clean
                add("warn", "instance",
                    f"{label} schema agreement could not be read",
                    f"{str(e).splitlines()[0][:90]} - unknown, not clean")
        for db in self.databases():
            if self.target_missing(db):
                how = self.hop.target.options.get("replication")
                add("pass" if how else "fail", db,
                    "target keyspace replication",
                    f"made with {how}" if how else
                    "not on the target, and how many copies it keeps is not"
                    " migkit's to choose: create it, or give the target"
                    " endpoint `replication`")
            else:
                got = self._copies_kept(db)[0]
                add({"ok": "pass", "warn": "warn"}.get(got.status, "warn"),
                    db, "target keyspace replication",
                    got.detail + (f"  [fix] {got.fix_hint}"
                                  if got.fix_hint and got.status != "ok"
                                  else ""))
            try:
                self._logged(db)
                add("pass", db, "tables can be followed after the copy",
                    "every table's change log is on")
            except SystemExit as e:
                add("warn", db, "tables cannot be followed after the copy",
                    str(e).replace("\n", " ")[:300])
            except Exception as e:  # noqa: BLE001 - unknown, not clean
                add("warn", db, "whether the tables can be followed",
                    f"{str(e).splitlines()[0][:90]} - unknown, not clean")
        inv = self._handwork()
        return items + inv.rows() + inv.summary()

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
            if not self.in_scope(db, t) or t.endswith(self.CDC_LOG):
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

    #: where the indexes and triggers a table was made without are kept
    #: until its rows are in, under the database's reports
    HELD = "held-off.json"

    def _statements(self, db, table):
        """The statements that make a source table as it is, in the
        target's keyspace: the table first - its clustering order,
        collections and types - then each of its indexes and triggers. Not
        with the source's change log on (`_made_like`)."""
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
        out = []
        for stmt in [x.strip() for x in text.split(";\n") if x.strip()]:
            stmt = re.sub(r"^(CREATE (?:TABLE|INDEX|CUSTOM INDEX|TRIGGER|"
                          r"MATERIALIZED VIEW)(?: IF NOT EXISTS)?[^(]*?)\s"
                          r"(\S+?)\.",
                          lambda m: f"{m.group(1)} {into}.", stmt, count=1)
            if stmt.upper().startswith("CREATE MATERIALIZED VIEW"):
                # over the table of the target's keyspace
                stmt = re.sub(r"\bFROM\s+\S+?\.", f"FROM {into}.", stmt,
                              count=1, flags=re.I)
            stmt = re.sub(r"\bON\s+\S+?\.", f"ON {into}.", stmt, count=1) \
                if stmt.upper().startswith(("CREATE INDEX",
                                            "CREATE CUSTOM INDEX",
                                            "CREATE TRIGGER")) else stmt
            stmt = re.sub(r"\s+AND\s+cdc\s*=\s*\{[^}]*\}", "", stmt,
                          flags=re.I)
            out.append(stmt.rstrip(";"))
        return out

    def _made_like(self, db, table, held=False):
        """The target table made by the source's own definition - its
        clustering order, collections, types and indexes - under the
        target's keyspace name. Not with the source's change log on: a
        ScyllaDB table whose log is on refuses a write dated before the
        log's current generation began (measured on 6.2: `cdc: attempted to
        get a stream "from the past"`), and every cell the copy writes is
        dated at the source's own write time. Where the target should keep
        a log, it is turned on there after the cutover; the settings check
        names the difference until then.

        With `held`, the indexes and triggers are not made: an index built
        as the rows arrive is built a row at a time, and a trigger runs for
        every row the copy writes - a side effect done again on rows the
        source already did it for. They are written down first, durably
        (`HELD`), and made once the rows are in (`finish_created`)."""
        statements = self._statements(db, table)
        first, later = statements[0], statements[1:]
        if held and later:
            from .. import durable
            durable.change_json(
                self.hop.report_dir(db) / self.HELD,
                lambda kept: kept.__setitem__(self._target_of(db, table),
                                              later), kind="state")
        dst = self._session("dst")
        dst.execute(first)
        for stmt in [] if held else later:
            dst.execute(stmt)

    def _make_types(self, db, log=None):
        """The user types the taken tables use and the target's keyspace
        lacks, made from the source's, those that hold another first: a
        table whose column is of a type that is not there is refused. One
        the target has under another definition is left, and the schema
        check names it."""
        src, _ = self._beside("src", db)
        have = set(self._beside("dst", db)[0].get("types", {}))
        types = src.get("types", {})
        todo = sorted(n for n in self._types_needed(
            db, self.neutral_tables("src", db), types) if n not in have)
        while todo:
            # a type is made once the missing types it holds are
            holds = {n: {m for m in todo if m != n and m in " ".join(
                t for _, t in types[n])} for n in todo}
            ready = [n for n in todo if not holds[n]] or todo[:1]
            for n in ready:
                self._session("dst").execute(
                    self._fix(db, "types", None, n, types[n]))
                if log:
                    log(f"type {n}: not on the target, made as the source"
                        " has it")
            todo = [n for n in todo if n not in ready]

    def create_missing(self, db, log=None):
        """Make on the target the tables in scope it lacks, each by the
        source's own definition and without its indexes and triggers
        (`_made_like`): the table copier fills a table and makes none, and
        a table made from the neutral classes would be keyed by all of its
        key columns as its partition key - the clustering order the source
        chose lost. A table the hop renames or maps columns of is the
        pair's to make (`through_pair`)."""
        made = self.prepare_target(db)
        if made and log:
            log(made)
        have = set(self.neutral_tables("dst", db))
        self._make_types(db, log)
        for t in self.neutral_tables("src", db):
            if t in have or self.through_pair(db, t):
                continue
            self._made_like(db, t, held=True)
            if log:
                log(f"{t}: not on the target, made as the source has it")

    def finish_created(self, db, log=None):
        """The indexes and triggers the tables made on the target went
        without, made now their rows are in: each from the record kept
        before the table was made, one that exists already left as it is,
        so a run that stopped part way is finished by the next. The record
        goes once every one is made."""
        from .. import durable
        path = self.hop.report_dir(db) / self.HELD
        kept = durable.read_state(path, "state")
        if not kept:
            return
        have = self._on_target(db)
        dst = self._session("dst")
        failed = {}
        for table, statements in sorted(kept.items()):
            left = []
            for stmt in statements:
                if self._named_in(stmt) in have:
                    continue
                try:
                    dst.execute(stmt)
                    if log:
                        log(f"{table}: {stmt}")
                except Exception as e:  # noqa: BLE001 - kept, and said
                    left.append(stmt)
                    if log:
                        log(f"{table}: not made: {stmt[:60]}: "
                            f"{str(e).splitlines()[0][:90]}")
            if left:
                failed[table] = left
        if failed:
            durable.write_json(path, failed)
            raise SystemExit(
                f"{db}: {sum(map(len, failed.values()))} indexes or triggers"
                f" were not made on the target; they are kept in {path}"
                " and the next move makes them")
        path.unlink(missing_ok=True)

    @staticmethod
    def _named_in(stmt):
        """(kind, name) an index, trigger or view statement makes."""
        import re
        m = re.match(r'(?i)create (?:custom )?(index|trigger|materialized'
                     r' view)(?: if not exists)? ("(?:[^"]|"")*"|\S+)', stmt)
        if not m:
            return None
        kind, name = m.group(1).lower(), m.group(2)
        if kind == "materialized view":
            # a view is named with its keyspace
            name = name.rpartition(".")[2]
        return kind, _unquoted(name)

    def _on_target(self, db):
        """{(kind, name)} of the indexes, triggers and materialized views the
        target's keyspace has."""
        s, ks = self._session("dst"), self._ks("dst", db)
        have = set()
        for kind, query in (("index", "select index_name from"
                             " system_schema.indexes where keyspace_name"
                             " = %s"),
                            ("trigger", "select trigger_name from"
                             " system_schema.triggers where keyspace_name"
                             " = %s"),
                            ("materialized view", "select view_name from"
                             " system_schema.views where keyspace_name"
                             " = %s")):
            have |= {(kind, str(r[0])) for r in s.execute(query, (ks,))}
        return have

    # ---- indexes the pair reads and makes -----------------------------------

    def neutral_indexes(self, side, db, table):
        """[(name, unique, [columns], carried)] of the table's secondary
        indexes. None is unique - Cassandra has no such index. `carried` is
        True for a plain index on one column: an index over a collection's
        keys, values or entries, or a custom one (SASI, SAI), is a thing of
        this engine and another cannot be given it as it is."""
        out = []
        for r in self._session(side).execute(
                "select index_name, kind, options from system_schema.indexes"
                " where keyspace_name = %s and table_name = %s",
                (self._ks(side, db), table)):
            o = {str(k): str(v) for k, v in dict(r.options or {}).items()}
            target = o.get("target", "")
            plain = (str(r.kind).upper() != "CUSTOM"
                     and not target.endswith(")"))
            out.append((str(r.index_name), False, [_unquoted(target)],
                        plain))
        return sorted(out)

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        """The statement that makes one index here. A unique index, or one
        over several columns, is not one Cassandra has: the statement comes
        back as the reason, which `execute_ddl` refuses, and the move
        names the index it did not build."""
        if unique:
            return ("-- not carried: Cassandra has no unique index - a"
                    " row is found by its key, and nothing else is held"
                    " unique")
        if len(columns) != 1:
            return ("-- not carried: a Cassandra secondary index covers one"
                    " column")
        return (f"create index {self._q(name)} on "
                f"{self._qualified(side, db, table)} "
                f"({self._q(columns[0])})")

    def execute_ddl(self, side, db, sql):
        """Run one statement that changes this side's schema."""
        self._target_only(side, "change a schema")
        if sql.lstrip().startswith("--"):
            raise RuntimeError(sql.lstrip()[2:].strip())
        self._session(side).execute(sql)

    # ---- the load's window: what a copy must not set off --------------------

    def load_window(self, db, log=None, tables=None):
        """The triggers of the tables a load writes, off for the load
        (`_TriggerWindow`): a trigger runs for every row written, and a row
        the source already ran it for would run it again."""
        return _TriggerWindow(self, db, log, tables)

    def set_aside(self, db):
        """The triggers a load took off the target and has not put back, as
        a finding - None where no record of one is kept. Held by a load
        still running they are off on purpose; held by one whose process is
        gone they are missing, and the application's cutover onto this
        target would run without them."""
        from .. import movers
        held, lost, unread = [], [], []
        there = None
        for path, alive, defs in movers.triggers_set_aside(self.hop, db):
            if defs is None:
                unread.append(str(path))
            elif alive:
                held += sorted(defs)
            else:
                if there is None:
                    there = {f"{t}.{n}" for (t, n) in self._triggers("dst",
                                                                     db)}
                lost += [(n, str(path)) for n in sorted(defs)
                         if n not in there]
        scope = f"{db} triggers set aside"
        if lost or unread:
            said = []
            if lost:
                said.append(f"{len(lost)} triggers a load took off the"
                            " target and never put back: "
                            + ", ".join(n for n, _ in lost[:5])
                            + " - definitions in "
                            + ", ".join(sorted({p for _, p in lost})))
            if unread:
                said.append("a record of triggers taken off cannot be"
                            " read: " + ", ".join(unread))
            return Result("deep", scope, "diff", "; ".join(said), "",
                          "the next `migkit move` into this database puts"
                          " them back; or run the statements in that file")
        if held:
            return Result("deep", scope, "warn",
                          f"{len(held)} triggers are off for a load that is"
                          " still running: " + ", ".join(held[:5]), "",
                          "they go back when it stops")
        return None

    def _triggers(self, side, db):
        """{(table, name): class} of the keyspace's triggers."""
        return {(r.table_name, r.trigger_name):
                _opts(r.options).get("class", "")
                for r in self._session(side).execute(
                    "select table_name, trigger_name, options from"
                    " system_schema.triggers where keyspace_name = %s",
                    (self._ks(side, db),))}

    # ---- rows sized across the ring -----------------------------------------

    #: token ranges the ring is cut into to size a table's rows, each read
    #: for its share of `SIZE_SAMPLE`: the first rows in token order lie in
    #: one corner of the ring
    SIZE_RANGES = 8

    def row_sizes(self, side, db, tables=None):
        """{table: sizes} (`Engine.row_sizes`): the bytes a row takes as
        carried, measured on rows read from `SIZE_RANGES` token ranges
        spread over the ring, `SIZE_SAMPLE` in all - the server keeps no
        size of a row to ask for, only an estimate of a partition's mean
        (`system.size_estimates`), which says nothing of the widest. A
        table with no more rows than the sample is measured whole
        (`exact`); a larger one is a sample, and says so."""
        from cassandra.query import SimpleStatement

        from .. import pieces
        from .base import sizes_of
        out = {}
        per = max(1, -(-self.SIZE_SAMPLE // self.SIZE_RANGES))
        step = (self.HIGH - self.LOW) // self.SIZE_RANGES
        for t in (tables if tables is not None
                  else self.neutral_tables(side, db)):
            try:
                cols = self._columns(side, db, t)
                part = ", ".join(self._q(c.column_name) for c in sorted(
                    (c for c in cols if c.kind == "partition_key"),
                    key=lambda c: c.position))
                names = ", ".join(self._q(c.column_name) for c in cols)
                lengths, whole = [], True
                for i in range(self.SIZE_RANGES):
                    lo = self.LOW + i * step
                    hi = (self.HIGH if i == self.SIZE_RANGES - 1
                          else lo + step - 1)
                    got = 0
                    for row in self._session(side).execute(SimpleStatement(
                            f"select {names} from"
                            f" {self._qualified(side, db, t)} where"
                            f" token({part}) >= %s and token({part}) <= %s"
                            f" limit {per}", fetch_size=min(per, 1000)),
                            (lo, hi)):
                        lengths.append(pieces.row_bytes(
                            [self._value(v) for v in row]))
                        got += 1
                    whole = whole and got < per
                out[t] = sizes_of(lengths, exact=whole)
            except Exception:  # noqa: BLE001 - that table is not sized
                continue
        return out

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
        lacks is made by the source's own definition, its indexes and
        triggers once the rows are in (`_made_like`). A counter table is
        refused by name: a counter is added to, never written."""
        mapped = [t for t in tables if self.through_pair(db, t)]
        if mapped:
            raise SystemExit(
                f"{', '.join(mapped[:5])}: renamed, or with columns the hop"
                " maps, which the copy that keeps each cell's times does not"
                " read - move them with --via builtin, the table copier"
                " through the pair")
        steps = [f"{t}: rows copied with their time to live and write"
                 " time, by token ranges" for t in tables]
        if not go:
            return steps
        from .. import dsbulk, tableway
        from ..decide import Facts
        from ..util import which
        workers = max(1, int(getattr(self.hop, "workers_most", 0)
                             or self.hop.workers or 1))
        self._make_types(db, log)
        for t in list(shape_only) + list(tables):
            if t not in self.neutral_tables("dst", db):
                self._made_like(db, t, held=True)
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
        with self.load_window(db, log, set(tables)):
            tableway.each(self, db, tables, self._table_ways(),
                          lambda t: Facts(loader=lambda: bool(which(
                              dsbulk.PROGRAM)), unfit=lambda: dsbulk.unfit(
                              self, db, t)),
                          run, log)
        self.finish_created(db, log)
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

    def _operation(self, row):
        """The operation of the hop's `mapping.ops` a log row is: an insert
        or an update as the log records it, any delete - a row's, a
        partition's, a range's - a delete."""
        op = int(row["cdc$operation"])
        return {self.INSERT: "insert", self.UPDATE: "update"}.get(op, "delete")

    def _carried(self, db, table, rows):
        """The log rows of an operation the hop's `mapping.ops` carries for
        this table (`Hop.carries`): a hop that keeps what the source
        deletes leaves out the deletes of a row, a partition and a range,
        a hop that carries inserts alone leaves out the updates, and the
        target's row is then what the changes it carries made it. Every
        row, for a hop that says nothing of its operations."""
        from ..config import OPS
        carried = self.hop.carries(db, *self.hop.parts_of(table))
        if carried >= frozenset(OPS):
            return rows
        return [r for r in rows if self._operation(r) in carried]

    def _follow_table(self, db, table, rows):
        """How many changes of one table's log were carried: the deletes
        as the log gives them, then every row it touched read again from
        the source and written as `native_bulk` writes one - each cell with
        its own write time and the time to live left to it, what has no
        single time at the latest write time that touched the row. The
        target's own last-write-wins then orders what arrives, so reading
        a row after a later change, or carrying a change twice, lands the
        same; and the target's cells keep the source's write times, which
        the deep check holds them to. Only the operations the hop's
        `mapping.ops` carries (`_carried`)."""
        from cassandra.concurrent import (execute_concurrent,
                                          execute_concurrent_with_args)
        rows = self._carried(db, table, rows)
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
        # the target's triggers are off for as long as the tail applies:
        # a change it carries is one the source already ran them for
        with (self.load_window(db, log, None) if go
              else contextlib.nullcontext()):
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

    # ---- repair ------------------------------------------------------------

    def repair_plan(self, db, kind):
        """The rows the last check named, planned by the pair every engine
        repairs by - only the tables the hop takes (`_drills_in_scope`)."""
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        """A row repair carried out by the pair, replacing each row it
        writes (`replacing`): a column the source holds null is cleared on
        the target, which a write that leaves a null unset would not do."""
        with replacing():
            return self._as_pair().apply(db, action)

    def repair_io(self, db, head):
        """How a repair record's rows are put back (`repairlog`): a row
        before the repair goes back as it was, a column it held null
        cleared again."""
        io = super().repair_io(db, head)
        put = getattr(io, "put", None)
        if put is not None:
            def put_back(*args, **kwargs):
                with replacing():
                    return put(*args, **kwargs)
            io.put = put_back
        return io

    def _apply_upserts(self, side, db, table, rows):
        """[(key, values)] of one table and one set of columns, each an
        insert of the key and the columns it carries: a row that is there is
        written over column by column, and a column set to null is cleared -
        an update that made it null is not a column left unset. The way a
        change from another engine reaches this target."""
        from cassandra.concurrent import execute_concurrent_with_args

        from .. import canon
        self._target_only(side, "apply changes")
        if not rows:
            return
        classes = {n: canon.type_class("cassandra", t) or canon.OWN
                   for n, t in self.neutral_columns(side, db, table)}
        names = list({**rows[0][0], **rows[0][1]})
        session = self._session(side)
        stmt = session.prepare(
            f"insert into {self._qualified(side, db, table)} ("
            + ", ".join(self._q(n) for n in names) + ") values ("
            + ", ".join("?" for _ in names) + ")")
        got = execute_concurrent_with_args(
            session, stmt,
            [tuple(None if v is None else canon.write_value(
                "cassandra", classes.get(n, canon.OWN), v)
                for n, v in ((n, {**k, **vals}[n]) for n in names))
             for k, vals in rows], concurrency=64)
        self._sent(got, table)

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_delete(self, side, db, table, key):
        """Remove the row at that key, whether or not it is there: a
        delete by the whole key, which is a tombstone for the row."""
        from .. import canon
        self._target_only(side, "delete a row")
        names = self.neutral_key(side, db, table)
        session = self._session(side)
        session.execute(session.prepare(
            f"delete from {self._qualified(side, db, table)} where "
            + " and ".join(f"{self._q(k)} = ?" for k in names)),
            [canon.sql_value(key[k]) for k in names])


def _opts(mapping):
    """A schema table's options map as plain text to text."""
    return {str(k): str(v) for k, v in dict(mapping or {}).items()}


def _unquoted(name):
    """A name as the schema tables hold it: a quoted one without its
    quotes."""
    name = str(name).strip()
    if len(name) > 1 and name[0] == name[-1] == '"':
        return name[1:-1].replace('""', '"')
    return name


class _TriggerWindow:
    """The triggers of the target's tables a load writes, off for the load
    and made again after it, whether it worked or not.

    A trigger runs for every row written to its table, and the rows the
    copy writes are rows the source already ran it for: a side effect done
    twice, with the source's own values dated now. Definitions reach disk
    before anything is dropped (`setaside`), one file for each load; a load
    that died leaves its file, and the next load into the database puts
    back what it lists and the target no longer has. A tail holds the
    window for as long as it runs. Tables the load does not write keep
    their triggers.
    """

    def __init__(self, eng, db, log=None, tables=None):
        from ..movers import TRIGGERS_SET_ASIDE
        from ..setaside import SetAside
        self.eng, self.db, self.log = eng, db, log
        self.tables = (None if tables is None
                       else {str(t).rpartition(".")[2] for t in tables})
        self.defs, self.dropped = {}, []
        self.record = SetAside(eng.hop, db, TRIGGERS_SET_ASIDE)

    def __enter__(self):
        eng, q = self.eng, self.eng._q
        ks = q(eng._ks("dst", self.db))
        on = eng._triggers("dst", self.db)
        # what a load that died took off: back at the end of this one,
        # whichever tables this one writes; held by one still running: off,
        # as this load needs them, and that one puts them back
        left, stale = self.record.left_behind(
            {f"{t}.{n}" for t, n in on})
        wanted = [(t, n, c) for (t, n), c in sorted(on.items())
                  if eng.in_scope(self.db, t)
                  and (self.tables is None or t in self.tables)]
        if not wanted and not left:
            for path in stale:
                path.unlink(missing_ok=True)
            return self
        for t, n, c in wanted:
            self.defs[f"{t}.{n}"] = (f"create trigger {q(n)} on {ks}."
                                     f"{q(t)} using '{c}'")
        self.defs.update(left)
        if not self.record.save(self.defs, stale):
            raise SystemExit(
                f"{self.db}: the target's triggers could not be saved"
                " before taking them off for the load, so nothing was"
                " loaded")
        self.dropped = list(left)
        for t, n, _ in wanted:
            eng._session("dst").execute(
                f"drop trigger if exists {q(n)} on {ks}.{q(t)}")
            self.dropped.append(f"{t}.{n}")
        if self.log:
            if left:
                self.log(f"{len(left)} triggers an earlier load took off"
                         " and did not put back go back at the end of this"
                         " one: " + ", ".join(sorted(left)[:5]))
            self.log(f"{len(wanted)} triggers taken off for the load;"
                     f" definitions saved to {self.record.path}")
        return self

    def __exit__(self, exc_type, *exc):
        failed = []
        for key in self.dropped:
            try:
                self.eng._session("dst").execute(self.defs[key])
            except Exception as e:  # noqa: BLE001 - kept, and said
                failed.append(key)
                if self.log:
                    self.log(f"TRIGGER NOT PUT BACK: {key}:"
                             f" {str(e).splitlines()[0][:100]}")
        if self.dropped and self.log:
            self.log(f"{len(self.dropped) - len(failed)} of"
                     f" {len(self.dropped)} triggers put back")
        if not failed:
            self.record.done()
        if failed and exc_type is None:
            raise SystemExit(
                f"{self.db}: {len(failed)} triggers were not put back:"
                f" {', '.join(failed)}. Their definitions are in"
                f" {self.record.path}")
        return False
