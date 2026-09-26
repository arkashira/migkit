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
                              auth_provider=auth, connect_timeout=15)
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

    def neutral_tables(self, side, db):
        return sorted(r.table_name for r in self._session(side).execute(
            "select table_name from system_schema.tables where"
            " keyspace_name = %s", (self._ks(side, db),))
            if not self.hop.excluded(db, r.table_name))

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
        return v

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        from cassandra.query import SimpleStatement
        if where:
            raise SystemExit(f"{table} moves under a row filter, which is"
                             " SQL, and Cassandra takes none")
        names = [n for n, _ in columns]
        stmt = SimpleStatement(
            f"select {', '.join(self._q(n) for n in names)} from"
            f" {self._qualified(side, db, table)}", fetch_size=int(size))
        result = self._session(side).execute(stmt)
        batch = []
        for row in result:
            batch.append([self._value(v) for v in row])
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

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
        from .. import canon, rowtext
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 5000,
                                          where):
            for row in batch:
                total = canon.digest_step(total, rowtext.encode(
                    [canon.render_value(c, v) for c, v in zip(classes, row)]))
                n += 1
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
                                  else canon.sql_value(v) for v in r)
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
