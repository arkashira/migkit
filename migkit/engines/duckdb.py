"""DuckDB as one side of a pair (backlog R15), through its own library.

A file and no server, as SQLite is: the endpoint's `path` (or `host`) is
the database file, and a hop's databases are schemas in it (`main` where
none is given). Rows are compared through the in-process renderer every
engine without a canonical SQL rendering shares, so any pair with DuckDB on
either side moves, is read back as it goes, and is checked. One writer at a
time: DuckDB lets one process write a file.
"""
from .base import Engine, Result
from .dbapi import MARK, DbapiRows


class DuckDBEngine(DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "duckdb"
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer, as every"
                      " file-based engine's is")
    OVER_NETWORK = False
    WRITES_IN_PARALLEL = False
    PARAMSTYLE = "qmark"
    LIMIT = "limit"

    def _path(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        path = ep.options.get("path") or ep.host
        if not path:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `path`, the DuckDB database file")
        return str(path)

    def _connect(self, side, db):
        import duckdb
        # instants read and written as UTC shows them, as every PostgreSQL
        # session migkit opens is; the machine's own zone was DuckDB's
        # default. In the configuration, not a SET: a cursor is a
        # connection of its own and does not carry what the first was set
        # to - measured, 17:00 UTC written through one landed as 00:00
        # the source read-only: opened for writing, a file with writes still
        # in its log beside it has them written into it on close - a write
        # to the source. Read-only, the log is read and left as it was
        return duckdb.connect(self._path(side), read_only=side == "src",
                              config={"TimeZone": "UTC"})

    def _qualified(self, side, db, table):
        return f"{self._q(self._d(side, db) or 'main')}.{self._q(table)}"

    def _create_name(self, side, db, table):
        return self._qualified(side, db, str(table).split(".")[-1])

    def databases(self):
        return list(self.hop.databases) or ["main"]

    def prepare_target(self, db):
        """The schema the tables go in, made where it is not there yet."""
        conn = self._connect("dst", db)
        try:
            conn.execute("create schema if not exists"
                         f" {self._q(self._d('dst', db) or 'main')}")
        finally:
            conn.close()
        return ""

    def neutral_tables(self, side, db):
        return [n for (n,) in self._rows(
            side, db, "select table_name from information_schema.tables"
                      f" where table_schema = {MARK} and table_type ="
                      " 'BASE TABLE' order by 1",
            (self._d(side, db) or "main",))
            if not self.hop.excluded(db, n)]

    def neutral_columns(self, side, db, table):
        return [(n, t) for n, t in self._rows(
            side, db, "select column_name, data_type from"
                      " information_schema.columns where table_schema ="
                      f" {MARK} and table_name = {MARK} order by"
                      " ordinal_position",
            (self._d(side, db) or "main", str(table).split(".")[-1]))]

    def neutral_key(self, side, db, table):
        got = self._rows(
            side, db, "select constraint_column_names from"
                      " duckdb_constraints() where schema_name = "
                      f"{MARK} and table_name = {MARK} and constraint_type ="
                      " 'PRIMARY KEY'",
            (self._d(side, db) or "main", str(table).split(".")[-1]))
        return list(got[0][0]) if got else []

    def neutral_write(self, side, db, table, columns, rows):
        """A batch handed to DuckDB as an Arrow table it reads in place:
        the keys it carries deleted by one join, its rows inserted by one
        statement, in one transaction - which converges on a restart as
        an upsert does. Measured, 20,000 rows through `executemany` 2.65s,
        100,000 through Arrow 0.27s; a 500,000-row copy took 115s before.
        A column Arrow cannot type as it stands (a UUID, an integer past 64
        bits) goes as text, which DuckDB casts to the column's type."""
        import pyarrow as pa

        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        arrays = []
        for col in zip(*rows):
            values = [canon.sql_value(v) for v in col]
            try:
                arrays.append(pa.array(values))
            except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
                arrays.append(pa.array([None if v is None else str(v)
                                        for v in values], pa.string()))
        batch = pa.Table.from_arrays(
            arrays, names=[f"c{i}" for i in range(len(names))])
        quoted = self._qualified(side, db, table)
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        conn = self._connect(side, db)
        try:
            conn.register("migkit_batch", batch)
            conn.execute("begin")
            try:
                if key:
                    conn.execute(
                        f"delete from {quoted} using migkit_batch b where "
                        + " and ".join(f"{quoted}.{self._q(k)} ="
                                       f" b.c{names.index(k)}" for k in key))
                conn.execute(
                    f"insert into {quoted} ({', '.join(map(self._q, names))})"
                    " select " + ", ".join(f"c{i}" for i in range(len(names)))
                    + " from migkit_batch")
                conn.execute("commit")
            except BaseException:
                conn.execute("rollback")
                raise
            conn.unregister("migkit_batch")
        finally:
            conn.close()
        return len(rows)

    def table_facts(self, side, db):
        out = {}
        for name, rows in self._rows(
                side, db, "select table_name, estimated_size from"
                          f" duckdb_tables() where schema_name = {MARK}",
                (self._d(side, db) or "main",)):
            out[name] = {"rows": rows, "bytes": None, "key": None}
        return out

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def _schema(self, side, db):
        return self._d(side, db) or "main"

    # ---- sequences, the deep checks, a snapshot ---------------------------

    def _sequences(self, side, db):
        """{sequence: the next value it hands out}, read from the START of
        the statement DuckDB keeps it as. `last_value` is the last one
        given out in the session that gave it and the next one after the
        file is opened again - measured, 3 then 4 for the same sequence."""
        import re
        out = {}
        for name, sql in self._rows(
                side, db, "select sequence_name, sql from duckdb_sequences()"
                          f" where schema_name = {MARK}",
                (self._schema(side, db),)):
            m = re.search(r"\bSTART (?:WITH )?(-?\d+)", str(sql or ""))
            if m:
                out[name] = int(m.group(1))
        return out

    def check_autoinc(self, db):
        """Each sequence's next value on both sides: a target sequence
        behind the source's hands out numbers the copied rows already
        hold, and the application's next insert fails on its key."""
        try:
            a, b = self._sequences("src", db), self._sequences("dst", db)
        except Exception as e:  # noqa: BLE001 - said, not a match
            return [Result("autoinc", db, "error",
                           f"{e} - no sequence could be read, which is not"
                           " the same as the sequences matching")]
        behind = [f"{n} src={v} dst={b.get(n, 'missing')}"
                  for n, v in sorted(a.items())
                  if n not in b or b[n] < v]
        if behind:
            return [Result("autoinc", db, "diff", "; ".join(behind[:10]),
                           "", "create each on the target with START at"
                               " the source's next value")]
        return [Result("autoinc", db, "ok",
                       f"{len(a)} sequences, none behind the source's")]

    def _objects(self, side, db):
        """{kind: {name: definition}} - indexes, constraints and views."""
        sch = (self._schema(side, db),)
        idx = {n: str(sql or "") for n, sql in self._rows(
            side, db, "select index_name, sql from duckdb_indexes() where"
                      f" schema_name = {MARK}", sch)}
        cons = {f"{t} {typ} {txt}": txt for t, typ, txt in self._rows(
            side, db, "select table_name, constraint_type, constraint_text"
                      " from duckdb_constraints() where schema_name ="
                      f" {MARK} and constraint_type <> 'NOT NULL'", sch)}
        views = {n: " ".join(str(sql or "").split()) for n, sql in self._rows(
            side, db, "select view_name, sql from duckdb_views() where not"
                      f" internal and schema_name = {MARK}", sch)}
        return {"index": idx, "constraint": cons, "view": views}

    def check_deep(self, db):
        """Indexes, keys and views the source has and the target does not:
        a copy of the rows carries none of them."""
        out = []
        try:
            a, b = self._objects("src", db), self._objects("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} objects", "error",
                           f"could not be read: {str(e)[:100]}")]
        gone = [f"{kind} {name}" for kind in a for name in sorted(a[kind])
                if name not in b[kind]]
        out.append(Result(
            "deep", f"{db} objects", "diff" if gone else "ok",
            (f"{len(gone)} on the source and not the target: "
             + ", ".join(gone[:8])) if gone else
            f"{sum(len(v) for v in a.values())} indexes, keys and views,"
            " all on the target", "",
            "create them on the target: a copy of rows carries none"
            if gone else ""))
        return out

    def snapshot_state(self, db, state_dir, kind="all"):
        """The target's tables, sequences and views copied whole into a
        database file beside the run's state, by DuckDB itself - read as
        of one moment, so a rollback has the target as it was."""
        import duckdb
        conn = duckdb.connect(str(state_dir / "dst.duckdb"),
                              config={"TimeZone": "UTC"})
        try:
            path = self._path("dst").replace("'", "''")
            conn.execute(f"attach '{path}' as target (read_only)")
            conn.execute("copy from database target to dst")
        finally:
            conn.close()

    # ---- tables copied by DuckDB itself -----------------------------------

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table copied by DuckDB itself: the source attached to the
        target read-only and the rows put in by one `INSERT ... SELECT` in
        the source's column order, vectorised, no row through Python. A
        table the target lacks is made by the source's own statement, the
        sequences its defaults draw on first, at the source's next value.
        One transaction a table."""
        steps = [f"{t}: copied by DuckDB itself, file to file"
                 for t in tables]
        if not go or not tables:
            return steps
        ssch, dsch = self._schema("src", db), self._schema("dst", db)
        conn = self._connect("dst", db)
        try:
            path = self._path("src").replace("'", "''")
            conn.execute(f"attach '{path}' as migkit_source (read_only)")
            here = conn.execute("select current_database()").fetchone()[0]
            conn.execute(f"create schema if not exists {self._q(dsch)}")
            conn.execute(f"use {self._q(here)}.{self._q(dsch)}")
            have = {n for (n,) in conn.execute(
                "select sequence_name from duckdb_sequences() where"
                " database_name = ? and schema_name = ?",
                (here, dsch)).fetchall()}
            for name, sql in conn.execute(
                    "select sequence_name, sql from duckdb_sequences() where"
                    " database_name = 'migkit_source' and schema_name = ?",
                    (ssch,)).fetchall():
                if name not in have:
                    conn.execute(sql)
            for t in tables:
                there = conn.execute(
                    "select count(*) from duckdb_tables() where"
                    " database_name = ? and schema_name = ? and table_name"
                    " = ?", (here, dsch, t)).fetchone()[0]
                cols = ", ".join(self._q(n) for (n,) in conn.execute(
                    "select column_name from duckdb_columns() where"
                    " database_name = 'migkit_source' and schema_name = ?"
                    " and table_name = ? order by column_index",
                    (ssch, t)).fetchall())
                src = f"migkit_source.{self._q(ssch)}.{self._q(t)}"
                conn.execute("begin")
                try:
                    if there:
                        conn.execute(f"delete from {self._q(t)}")
                    else:
                        conn.execute(conn.execute(
                            "select sql from duckdb_tables() where"
                            " database_name = 'migkit_source' and"
                            " schema_name = ? and table_name = ?",
                            (ssch, t)).fetchone()[0])
                    conn.execute(f"insert into {self._q(t)} ({cols})"
                                 f" select {cols} from {src}")
                    conn.execute("commit")
                except BaseException:
                    conn.execute("rollback")
                    raise
                if log:
                    log(f"{t}: copied")
            conn.execute("use " + self._q(here))
            conn.execute("detach migkit_source")
        finally:
            conn.close()
        return steps
