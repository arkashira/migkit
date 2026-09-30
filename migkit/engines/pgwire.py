"""PostgreSQL's wire, spoken by databases that are not PostgreSQL (backlog
ER4): CockroachDB and Aurora DSQL.

Both take a PostgreSQL client, its TLS and its SQL for reading and writing
rows, and keep `information_schema` and `pg_catalog`. Neither has what
the PostgreSQL engine is built on: logical replication, `pg_dump`,
`COPY ... TO` in binary, a slot, an exported snapshot, PL/pgSQL. So they
are not the PostgreSQL engine with parts switched off - every one of its
ten thousand lines would be a question of whether it holds here - but
sides that speak the neutral contract through the one DB-API reader
(`DbapiRows`), with the PostgreSQL engine's own driver settings (decimals
in JSON kept as decimals, a `real` read as the 32-bit float it is), and
are copied, compared, repaired and followed by the pair machinery every
engine shares. Their values are PostgreSQL's (`canon.dialect`); their
rows are rendered in this process, where a rendering no server of theirs
has run cannot silently disagree with the other side's.

What each keeps of its own - how its changes are published, what one
transaction may hold, how it signs in - is in its own module.
"""
from .base import Engine, Result
from .dbapi import MARK, DbapiRows


class PgWireEngine(DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    ENGINE_FAMILY = "postgres"
    SQL_DIALECT = "postgres"
    PARAMSTYLE = "pyformat"
    LIMIT = "limit"
    #: `char(n)` comes back padded to its length; PostgreSQL's own text
    #: of it, which every comparison meets, has no padding
    PADDED = ("character", "bpchar", "char")
    OVER_NETWORK = True
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " a SQL rendering its server has not run")
    CHANGE_POINT_READS_ONLY = True
    READS_AHEAD = False
    DEFAULT_PORT = 5432
    #: schemas that are the server's, never the application's
    SYSTEM_SCHEMAS = ("pg_catalog", "information_schema")

    def _password(self, side, ep):
        return ep.password

    def _connect(self, side, db):
        import psycopg2

        from .postgres import PostgresEngine
        ep = self.hop.source if side == "src" else self.hop.target
        conn = psycopg2.connect(host=ep.host, port=ep.port or self.DEFAULT_PORT,
                                user=ep.user,
                                password=self._password(side, ep),
                                dbname=self._d(side, db), connect_timeout=15,
                                **ep.libpq_tls())
        with conn.cursor() as cur:
            # values of time with a zone read in UTC, as the PostgreSQL
            # engine's session reads them
            cur.execute("set time zone 'UTC'")
        conn.commit()
        PostgresEngine._read_json(conn)
        PostgresEngine._read_float4(conn)
        return conn

    def _q(self, name):
        return '"' + str(name).replace('"', '""') + '"'

    def _qualified(self, side, db, table):
        sch, _, tbl = str(table).rpartition(".")
        return f"{self._q(sch or 'public')}.{self._q(tbl)}"

    def _create_name(self, side, db, table):
        return self._qualified(side, db, table)

    def _quote_literal(self, value):
        return "'" + str(value).replace("'", "''") + "'"

    def _read_as(self, quoted, declared):
        """An interval as its text, which keeps its months and days apart:
        the driver's `timedelta` folds a month into thirty days."""
        from .. import canon, intervals
        if canon.type_class(self.CANON_ENGINE, declared) == canon.INTERVAL:
            return (f"{quoted}::text", intervals.parse)
        return None

    def databases(self):
        return list(self.hop.databases or ["postgres"])

    @staticmethod
    def _split(table):
        sch, _, tbl = str(table).rpartition(".")
        return sch or "public", tbl

    # ---- what a table is -------------------------------------------------

    def _visible(self):
        """The condition on `information_schema.columns` for a column the
        application sees (CockroachDB hides a keyless table's row id)."""
        return ""

    def neutral_tables(self, side, db):
        from .. import drift
        system = ", ".join(self._quote_literal(s) for s in self.SYSTEM_SCHEMAS)
        got = [f"{s}.{t}" for s, t in self._rows(
            side, db, "select table_schema, table_name from"
                      " information_schema.tables where table_type ="
                      f" 'BASE TABLE' and table_schema not in ({system})"
                      " and table_name not like 'migkit%%' order by 1, 2")]
        working = drift.transient_among(got)
        return [t for t in got if t not in working
                and not self.hop.excluded(db, *t.split(".", 1))]

    def neutral_columns(self, side, db, table):
        """[(name, declared)] in the column's order, the type as
        `format_type` writes it - `character varying(20)`, `numeric(12,2)`,
        `timestamp(3) with time zone` - the words the PostgreSQL engine
        classifies by, so both mean one thing by a type."""
        sch, tbl = self._split(table)
        cols = [n for (n,) in self._rows(
            side, db, "select column_name from information_schema.columns"
                      f" where table_schema = {MARK} and table_name = {MARK}"
                      f"{self._visible()} order by ordinal_position",
            (sch, tbl))]
        types = dict(self._rows(
            side, db, "select a.attname, format_type(a.atttypid,"
                      " a.atttypmod) from pg_catalog.pg_attribute a join"
                      " pg_catalog.pg_class c on c.oid = a.attrelid join"
                      " pg_catalog.pg_namespace n on n.oid ="
                      f" c.relnamespace where n.nspname = {MARK} and"
                      f" c.relname = {MARK} and a.attnum > 0 and not"
                      " a.attisdropped", (sch, tbl)))
        missing = [c for c in cols if c not in types]
        if missing:
            types.update(self._schema_types(side, db, sch, tbl))
        return [(c, types.get(c) or "") for c in cols]

    def _schema_types(self, side, db, sch, tbl):
        """The declared types from `information_schema` alone, its numbers
        put back as `format_type` writes them."""
        out = {}
        for name, typ, length, prec, scale, digits in self._rows(
                side, db, "select column_name, data_type,"
                          " character_maximum_length, numeric_precision,"
                          " numeric_scale, datetime_precision from"
                          " information_schema.columns where table_schema ="
                          f" {MARK} and table_name = {MARK}", (sch, tbl)):
            typ = str(typ).lower()
            if length and typ in ("character varying", "character"):
                typ = f"{typ}({length})"
            elif typ == "numeric" and prec is not None and scale is not None:
                typ = f"numeric({prec},{scale})"
            elif digits is not None and typ.startswith(("timestamp", "time ")):
                head, _, rest = typ.partition(" ")
                typ = f"{head}({digits}) {rest}".strip()
            out[name] = typ
        return out

    def neutral_key(self, side, db, table):
        sch, tbl = self._split(table)
        got = [n for (n,) in self._rows(
            side, db, "select k.column_name from"
                      " information_schema.table_constraints c join"
                      " information_schema.key_column_usage k on"
                      " k.constraint_schema = c.constraint_schema and"
                      " k.constraint_name = c.constraint_name and"
                      " k.table_name = c.table_name where c.constraint_type"
                      f" = 'PRIMARY KEY' and c.table_schema = {MARK} and"
                      f" c.table_name = {MARK} order by k.ordinal_position",
            (sch, tbl))]
        seen = {n for n, _ in self.neutral_columns(side, db, table)}
        # a key over a column the application does not see is no key of
        # its rows: another engine has nowhere to put it
        return got if all(n in seen for n in got) else []

    def column_facts(self, side, db, table):
        if table is None:
            return {None: {"null": True}}
        sch, tbl = self._split(table)
        return {str(n): {"null": str(null).upper() == "YES"}
                for n, null in self._rows(
                    side, db, "select column_name, is_nullable from"
                              " information_schema.columns where"
                              f" table_schema = {MARK} and table_name ="
                              f" {MARK}", (sch, tbl))}

    def neutral_column_rules(self, side, db, table):
        sch, tbl = self._split(table)
        out = {}
        for name, null, default, ident in self._rows(
                side, db, "select column_name, is_nullable, column_default,"
                          " is_identity from information_schema.columns"
                          f" where table_schema = {MARK} and table_name ="
                          f" {MARK}", (sch, tbl)):
            text = None if default is None else str(default)
            out[str(name)] = {
                "null": str(null).upper() == "YES",
                "default": None if text and "nextval(" in text else text,
                "identity": str(ident).upper() == "YES"
                or bool(text and ("nextval(" in text
                                  or "unique_rowid()" in text))}
        return out

    def neutral_indexes(self, side, db, table):
        """Each index but the primary key's, by `pg_index`: a partial or
        expression index is listed and not carried."""
        sch, tbl = self._split(table)
        got = {}
        for name, unique, col, whole in self._rows(
                side, db, "select i.relname, x.indisunique, a.attname,"
                          " x.indexprs is null and x.indpred is null from"
                          " pg_catalog.pg_index x join pg_catalog.pg_class i"
                          " on i.oid = x.indexrelid join pg_catalog.pg_class"
                          " t on t.oid = x.indrelid join"
                          " pg_catalog.pg_namespace n on n.oid ="
                          " t.relnamespace left join pg_catalog.pg_attribute"
                          " a on a.attrelid = t.oid and a.attnum ="
                          " any(x.indkey) where not x.indisprimary and"
                          f" n.nspname = {MARK} and t.relname = {MARK}"
                          " order by i.relname", (sch, tbl)):
            one = got.setdefault(str(name), [bool(unique), [], bool(whole)])
            if col is not None:
                one[1].append(str(col))
        return [(n, u, c, ok) for n, (u, c, ok) in sorted(got.items())]

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        return (f"create {'unique ' if unique else ''}index {self._q(name)}"
                f" on {self._qualified(side, db, table)} ("
                + ", ".join(self._q(c) for c in columns) + ")")

    def execute_ddl(self, side, db, sql):
        self.neutral_create_code(side, db, sql)

    def table_facts(self, side, db):
        out = {}
        for t in self.neutral_tables(side, db):
            out[t] = {"rows": None, "bytes": None,
                      "key": bool(self.neutral_key(side, db, t))}
        return out

    # ---- the checks, through the pair every engine is compared by ------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    # ---- following, through the pair's tail ------------------------------

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    @staticmethod
    def position_reached(have, want):
        from ..changetopic import Follower
        return Follower.reached(have, want)

    def release_changes(self):
        for f in self.__dict__.pop("_followers", {}).values():
            f.close()

    def position_lost(self, side, db, token):
        return self._follower(db).lost(token) if token else None

    def stream_room(self, side, db, token):
        return self._follower(db).room(token)

    def _changes_spec(self, side="src"):
        ep = self.hop.source if side == "src" else self.hop.target
        spec = ep.options.get("changes")
        return dict(spec) if isinstance(spec, dict) else None

    def _unkeyed(self, db):
        return [t for t in self.neutral_tables("src", db)
                if not self.neutral_key("src", db, t)]

    def _refuse_unkeyed(self, db, why):
        gone = self._unkeyed(db)
        if gone:
            raise SystemExit(
                f"{db}: {', '.join(gone[:6])}"
                f" {'has' if len(gone) == 1 else 'have'} no primary key, and"
                f" {why}: give each a primary key, or leave it out of the"
                " hop and move it with a copy")

    def _named(self, db, row):
        """The table a feed's row is of, as `neutral_tables` names it, by
        its schema and name or its name alone."""
        tables = self.__dict__.setdefault("_scope", {})
        if db not in tables or not self._match(tables[db], row.table):
            tables[db] = self.neutral_tables("src", db)
        return self._match(tables[db], row.table)

    @staticmethod
    def _match(tables, name):
        if name is None:
            return None
        name = str(name)
        if name in tables:
            return name
        leaf = name.rpartition(".")[2]
        hits = [t for t in tables if t.rpartition(".")[2] == leaf
                and (name.count(".") == 0 or name.endswith("." + t)
                     or t.endswith("." + name))]
        return hits[0] if len(hits) == 1 else None

    def _key_rows(self, side, db, table, key, keys, cast=None, fence=None):
        """{key text: row values} of the rows these keys name, and the
        columns read, each key's values cast by `cast` (`%s::<type>`)
        where the feed gave them as text; as the table stood at `fence`
        where the engine reads the past (`_as_of`)."""
        from .. import canon, changetopic
        declared = dict(self.neutral_columns(side, db, table))
        classes = {n: canon.type_class(self.CANON_ENGINE, t)
                   for n, t in declared.items()}
        names = list(declared)
        cols = ", ".join(self._read_expr(n, declared[n]) for n in names)
        found = {}
        one = "(" + ", ".join(
            f"%s::{declared[k]}" if cast else "%s" for k in key) + ")"
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            for i in range(0, len(keys), 500):
                part = keys[i:i + 500]
                sql = (f"select {cols} from {self._qualified(side, db, table)}"
                       f"{self._as_of(fence)} where ("
                       + ", ".join(self._q(k) for k in key) + ") in ("
                       + ", ".join([one] * len(part)) + ")")
                cur.execute(sql, [v for k in part for v in k])
                for got in cur.fetchall():
                    values = self._typed_row(names, declared, list(got))
                    found[tuple(changetopic.key_text(classes.get(k),
                                                     values[k])
                                for k in key)] = values
            conn.rollback()
        finally:
            conn.close()
        return found, classes

    def _as_of(self, fence):
        return ""

    def _read_expr(self, name, declared):
        got = self._read_as(self._q(name), declared)
        return f"{got[0]} as {self._q(name)}" if got else self._q(name)

    def _typed_row(self, names, declared, row):
        """A row read by `_key_rows` as the neutral reads give it."""
        from .. import canon, intervals
        out = {}
        for n, v in zip(names, row):
            if v is not None and canon.type_class(
                    self.CANON_ENGINE, declared[n]) == canon.INTERVAL:
                v = intervals.parse(v)
            elif isinstance(v, memoryview):
                v = bytes(v)
            elif isinstance(v, str) and str(declared[n]).split("(")[0] \
                    .strip().lower() in self.PADDED:
                v = v.rstrip(" ")
            out[n] = v
        return out

    def reach_items(self, side):
        return []

    def _assess_extra(self):
        return self.reach_items("src")

    def _deep_error(self, db, who, e):
        return Result("deep", f"{db} {who}", "error",
                      f"could not ask the {who}:"
                      f" {str(e).strip().splitlines()[0][:90]}")
