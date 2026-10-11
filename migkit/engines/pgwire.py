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

Both are also held to their source by what the catalogue they keep says
(`check_deep`, `check_schema`, `check_autoinc`): keys, columns, indexes,
foreign keys, sequences and their owners and grants, collation, encoding,
generated columns and the values a count cannot see - written once, over
`information_schema`, `pg_catalog` and standard SQL. A catalogue a server
does not keep (Aurora DSQL has no `pg_proc`, `pg_trigger`, `pg_matviews`,
`pg_policies`, `pg_sequence` or `pg_database`) is never read for it; what
only one of them keeps is asked in that engine's own `_deep_extra`.
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
                and self.in_scope(db, t)]

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
        """The columns the pair compares, and the whole structure - columns,
        keys, indexes and views - held to the target's in one answer."""
        found = self._as_pair().check_schema(db)
        try:
            found.append(self._structural(db))
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001 - said, as the error
            found.append(Result(
                "schema", f"{db} (structural)", "error",
                "could not read the structure to hold it to the source's: "
                f"{str(e).strip().splitlines()[0][:90]}"))
        return found

    def _structural(self, db):
        """Every structural difference between the two sides' tables and
        views, kept whole in a file of the report: what a person asks of a
        schema diff, over the catalogue both sides keep."""
        import json
        diffs = []
        for s, d in self._deep_pairs(db):
            def shape(side, t):
                rules = self.neutral_column_rules(side, db, t)
                return {
                    "columns": {n: (self._squash(ty),
                                    rules.get(n, {}).get("null"),
                                    self._squash(rules.get(n, {})
                                                 .get("default")),
                                    bool(rules.get(n, {}).get("identity")))
                                for n, ty in self.neutral_columns(side, db,
                                                                  t)},
                    "key": list(self.neutral_key(side, db, t)),
                    "indexes": {n: (bool(u), list(c)) for n, u, c, _ in
                                self.neutral_indexes(side, db, t)}}
            a, b = shape("src", s), shape("dst", d)
            for kind, noun in (("columns", "column"), ("indexes", "index")):
                for name in sorted(set(a[kind]) | set(b[kind])):
                    if a[kind].get(name) != b[kind].get(name):
                        diffs.append({
                            "table": s, "object": f"{noun} {name}",
                            "source": a[kind].get(name),
                            "target": b[kind].get(name)})
            if a["key"] != b["key"]:
                diffs.append({"table": s, "object": "primary key",
                              "source": a["key"], "target": b["key"]})
        src = dict(self.neutral_views("src", db))
        dst = dict(self.neutral_views("dst", db))
        for name in sorted(src):
            if self._squash(src[name]) != self._squash(dst.get(name)):
                diffs.append({"table": name, "object": "view",
                              "source": "defined",
                              "target": "defined differently"
                              if name in dst else None})
        path = self.hop.report_dir(db) / "structural-diff.json"
        path.write_text(json.dumps({"differences": diffs}, indent=1,
                                   default=str))
        if not diffs:
            return Result("schema", f"{db} (structural)", "ok",
                          "no structural difference in the columns, keys,"
                          " indexes and views", str(path))
        return Result(
            "schema", f"{db} (structural)", "diff",
            f"{len(diffs)} structural differences: "
            + "; ".join(f"{x['table']} {x['object']}" for x in diffs[:6])
            + (" ..." if len(diffs) > 6 else ""), str(path),
            "bring the target's structure level with the source's")

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        """The pair's comparison, each differing table with its rows apart
        counted, one by one, from the keys the drilldown named."""
        from .base import rows_counted
        pair = self._as_pair()
        got = pair.check_data(db, table, stream)
        for r in got:
            if r.status == "diff" and r.table:
                kinds = [len(pair._read_drill(db, pair._leaf(r.table), k))
                         for k in ("missing", "extra", "changed")]
                if any(kinds):
                    r.rows_apart = rows_counted(r.table, *kinds)
        return got

    # ---- following, through the pair's tail ------------------------------

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    #: its change tail is the pair's (`Engine.tail_masks`)
    TAILS_THROUGH_PAIR = True

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

    # ---- what the server says of itself, for the assessment --------------

    def _scalar(self, side, db, sql):
        """The first row of a query, or None where nothing came back."""
        got = self._rows(side, db, sql)
        return tuple(got[0]) if got else None

    #: what the assessment asks before anything moves: the rows the target
    #: has no room for (`Engine._capacity_gaps`, over this engine's own
    #: catalogue and limits)
    PREFLIGHT = ("_capacity_gaps",)

    def _server_versions(self):
        """(the source's, the target's) version as each server reports it."""
        db = (self.databases() or ["postgres"])[0]
        out = []
        for side in ("src", "dst"):
            try:
                out.append(str(self.server_identity(side, db)
                               .get("version") or "") or None)
            except SystemExit:
                raise
            except Exception:  # noqa: BLE001 - unknown, not guessed
                out.append(None)
        return tuple(out)

    def _brand_probes(self):
        """The banner and the reported `server_version` of each side, which
        a fork gives its own words in (`variants.SIGNATURES`)."""
        db = (self.databases() or ["postgres"])[0]

        def one(side):
            try:
                row = self._rows(side, db, "select version(), current_setting"
                                           "('server_version')")[0]
            except Exception:  # noqa: BLE001 - an unreadable side is unknown
                return {}
            ep = self.hop.source if side == "src" else self.hop.target
            return {"version": str(row[0]), "server_version": str(row[1]),
                    "endpoint": str(ep.host)}
        return (one("src"), one("dst"))

    def _handwork(self):
        """What the move leaves for a person (`migkit.handwork`): the tables
        with no key, which are copied and compared but cannot be followed or
        repaired by key, and the code and views kept in the database, which
        the copy of the rows does not carry."""
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                tables = self.neutral_tables("src", db)
                inv.add("no-row-key", db, "tables",
                        [t for t in tables
                         if not self.neutral_key("src", db, t)])
            except Exception as e:  # noqa: BLE001 - unknown, never zero
                inv.unknown("no-row-key", db,
                            f"catalogue query failed: {str(e)[:80]}")
            try:
                inv.add("server-side-code", db, "views",
                        [n for n, _ in self.neutral_views("src", db)])
                inv.add("server-side-code", db, "functions",
                        self._function_names("src", db))
            except Exception as e:  # noqa: BLE001 - unknown, never zero
                inv.unknown("server-side-code", db,
                            f"catalogue query failed: {str(e)[:80]}")
        return inv

    def _assess_extra(self):
        inv = self._handwork()
        return self.reach_items("src") + inv.rows() + inv.summary()

    # ---- views ---------------------------------------------------------

    def _sys(self):
        """The server's own schemas, quoted for a list."""
        return ", ".join(self._quote_literal(s) for s in self.SYSTEM_SCHEMAS)

    def neutral_views(self, side, db):
        """[(name, the view's SQL)] of the views the hop takes."""
        return [(f"{s}.{n}", str(sql)) for s, n, sql in self._rows(
            side, db, "select schemaname, viewname, definition from"
                      f" pg_catalog.pg_views where schemaname not in"
                      f" ({self._sys()}) order by 1, 2")
                if sql and self.takes(db, "view", f"{s}.{n}")]

    def _function_names(self, side, db):
        """['schema.name'] of the functions and procedures written in the
        database that the hop takes."""
        return sorted({f"{s}.{n}" for s, n in self._rows(
            side, db, "select routine_schema, routine_name from"
                      " information_schema.routines where routine_schema not"
                      f" in ({self._sys()})")
            if self.takes(db, "function", f"{s}.{n}")})

    # ---- foreign keys ----------------------------------------------------

    def _constraints(self, side, db, kinds):
        """[(table, name, kind, validated, deferrable, initially deferred,
        definition)] of the constraints of these kinds ('f', 'c', 'u') on
        the tables the hop takes."""
        marks = ", ".join(self._quote_literal(k) for k in kinds)
        return self._scoped_rows(db, self._rows(
            side, db, "select n.nspname || '.' || t.relname, c.conname,"
                      " c.contype, c.convalidated, c.condeferrable,"
                      " c.condeferred, pg_catalog.pg_get_constraintdef(c.oid)"
                      " from pg_catalog.pg_constraint c join"
                      " pg_catalog.pg_class t on t.oid = c.conrelid join"
                      " pg_catalog.pg_namespace n on n.oid = t.relnamespace"
                      f" where n.nspname not in ({self._sys()}) and"
                      f" c.contype in ({marks}) order by 1, 2"))

    def _fk_facts(self, side, db):
        """[(child, [columns], parent, [parent's columns], on delete, on
        update)] of the foreign keys of the tables the hop takes, each table
        as `neutral_tables` names it."""
        out = []
        for child, parent, cols, pcols, ondel, onupd in self._rows(
                side, db, "select cn.nspname || '.' || c.relname, pn.nspname"
                          " || '.' || p.relname, array(select a.attname from"
                          " unnest(o.conkey) with ordinality k(n, i) join"
                          " pg_catalog.pg_attribute a on a.attrelid ="
                          " o.conrelid and a.attnum = k.n order by k.i),"
                          " array(select a.attname from unnest(o.confkey)"
                          " with ordinality k(n, i) join"
                          " pg_catalog.pg_attribute a on a.attrelid ="
                          " o.confrelid and a.attnum = k.n order by k.i),"
                          " o.confdeltype, o.confupdtype from"
                          " pg_catalog.pg_constraint o join"
                          " pg_catalog.pg_class c on c.oid = o.conrelid join"
                          " pg_catalog.pg_namespace cn on cn.oid ="
                          " c.relnamespace join pg_catalog.pg_class p on"
                          " p.oid = o.confrelid join pg_catalog.pg_namespace"
                          " pn on pn.oid = p.relnamespace where o.contype ="
                          f" 'f' and cn.nspname not in ({self._sys()})"
                          " order by 1, 2"):
            if self.in_scope(db, child):
                out.append((child, list(cols), parent, list(pcols),
                            str(ondel), str(onupd)))
        return out

    def foreign_keys(self, side, db):
        """[(child, [its columns], parent, [the parent's columns])], each
        table as `neutral_tables` names it, for the copy's order and the
        target's."""
        return [(c, cols, p, pcols)
                for c, cols, p, pcols, _, _ in self._fk_facts(side, db)]

    # ---- sequences and counters ------------------------------------------

    @staticmethod
    def _regclass(text, schema):
        """(schema, name) of what a `nextval('...')` default or a serial
        sequence function says, an unqualified name taken to be in
        `schema`."""
        text = str(text).split("::")[0].strip().strip("'")
        parts, cur, quoted = [], "", False
        for ch in text:
            if ch == '"':
                quoted = not quoted
            elif ch == "." and not quoted:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        parts.append(cur)
        return (parts[-2], parts[-1]) if len(parts) > 1 \
            else (schema, parts[-1])

    def _owned_sequences(self, side, db):
        """[(schema, sequence, table, column)] of the sequences a column of
        a table the hop takes draws from - by its default, or as an identity
        column - and only those: a table the hop leaves out keeps the
        counter its own writer gives it."""
        out = []
        cols = self._rows(
            side, db, "select table_schema, table_name, column_name,"
                      " column_default, is_identity from"
                      " information_schema.columns where table_schema not in"
                      f" ({self._sys()}) and (position('nextval(' in"
                      " column_default) > 0 or is_identity = 'YES')"
                      f"{self._visible()} order by 1, 2, ordinal_position")
        for sch, tbl, col, default, ident in cols:
            if not self.in_scope(db, f"{sch}.{tbl}"):
                continue
            seq = None
            if default and "nextval(" in str(default):
                text = str(default).split("nextval(", 1)[1]
                seq = self._regclass(text.rsplit(")", 1)[0], sch)
            elif str(ident).upper() == "YES":
                got = self._rows(side, db, "select pg_get_serial_sequence("
                                           f"{MARK}, {MARK})",
                                 (f"{self._q(sch)}.{self._q(tbl)}", col))
                if got and got[0][0]:
                    seq = self._regclass(got[0][0], sch)
            if seq:
                out.append((seq[0], seq[1], f"{sch}.{tbl}", str(col)))
        return out

    def _sequence_state(self, side, db, sch, name):
        """(last value, whether it was handed out, the step) of a sequence."""
        last, called = self._rows(
            side, db, f"select last_value, is_called from {self._q(sch)}."
                      f"{self._q(name)}")[0]
        try:
            step = int(self._rows(
                side, db, "select increment from information_schema.sequences"
                          f" where sequence_schema = {MARK} and"
                          f" sequence_name = {MARK}", (sch, name))[0][0])
        except Exception:  # noqa: BLE001 - one at a time, as the default is
            step = 1
        return int(last), bool(called), step

    def _next_value(self, state):
        last, called, step = state
        return last + step if called else last

    def sequences_behind(self, db):
        """[(sequence, table, column, the column's largest value)] for each
        sequence on the target that draws for a column of a table the hop
        takes and would hand out a value the column already holds. Asked of
        the target alone, so it answers whatever engine the rows came from."""
        out = []
        for sch, name, table, col in self._owned_sequences("dst", db):
            state = self._sequence_state("dst", db, sch, name)
            if state[2] <= 0:
                continue
            top = self._rows("dst", db, f"select max({self._q(col)}) from "
                                        f"{self._qualified('dst', db, table)}"
                             )[0][0]
            if top is not None and int(top) >= self._next_value(state):
                out.append((f"{self._q(sch)}.{self._q(name)}",
                            self._qualified("dst", db, table),
                            self._q(col), int(top)))
        return out

    @staticmethod
    def _raise_past(behind):
        """The statements that move each sequence to its column's largest
        value, so the next one handed out is past every row."""
        return [f"select setval('{seq.replace(chr(39), chr(39) * 2)}',"
                f" {top})" for seq, _, _, top in behind]

    def check_autoinc(self, db):
        """Two verdicts per database. USABLE is the one that matters at
        cutover: every sequence on the target that draws for a column must
        sit past that column's largest value, or the first insert is refused
        as a duplicate. PARITY is the softer one-for-one check that the
        target continues from where the source stopped."""
        mine = {}
        try:
            for side in ("src", "dst"):
                mine[side] = {}
                for sch, name, table, col in self._owned_sequences(side, db):
                    mine[side][f"{sch}.{name}"] = (sch, name, table, col)
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001 - not asked is not clear
            return [Result("autoinc", f"{db} usable", "error",
                           "could not read the sequences: "
                           f"{str(e).strip().splitlines()[0][:90]}")]
        collide = []
        for key, (sch, name, table, col) in sorted(mine["dst"].items()):
            state = self._sequence_state("dst", db, sch, name)
            top = self._rows("dst", db,
                             f"select max({self._q(col)}) from "
                             f"{self._qualified('dst', db, table)}")[0][0]
            if top is not None and state[2] > 0 and \
                    int(top) >= self._next_value(state):
                collide.append(f"{key}: next value {self._next_value(state)}"
                               f" <= largest {table}.{col} {int(top)}")
        res = []
        fix = (f"migkit sync {self.hop.name} --db {db} --kind sequences"
               " --apply")
        if collide:
            res.append(Result(
                "autoinc", f"{db} usable", "diff",
                "sequences WILL collide on the next insert: "
                + "; ".join(collide[:8]), "",
                f"{fix}  (fix BEFORE cutover)"))
        else:
            res.append(Result(
                "autoinc", f"{db} usable", "ok",
                f"{len(mine['dst'])} sequences all clear their column's"
                " largest value, no collision" if mine["dst"]
                else "no sequence draws for a column of a table the hop"
                     " takes"))
        apart = []
        for key, (sch, name, *_) in sorted(mine["src"].items()):
            want = self._sequence_state("src", db, sch, name)
            if key not in mine["dst"]:
                apart.append(f"{key} src={want[0]} dst=MISSING")
                continue
            have = self._sequence_state("dst", db, sch, name)
            if self._next_value(have) != self._next_value(want):
                apart.append(f"{key} next src={self._next_value(want)}"
                             f" dst={self._next_value(have)}")
        if apart:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(apart[:8]), "", fix))
        else:
            res.append(Result(
                "autoinc", f"{db} parity", "ok",
                f"{len(mine['src'])} sequences continue from the source's"
                " next value"))
        return res

    def repair_plan(self, db, kind):
        """Sequences raised past the rows of their column - each with the
        statements that put it back where it stood - and, from the last
        check's drilldown, the rows to carry."""
        from .base import RepairAction
        out = []
        if kind in ("sequences", "all"):
            behind = self.sequences_behind(db)
            if behind:
                out.append(RepairAction(
                    db, "sequences", self._raise_past(behind),
                    self._set_back(db, behind),
                    f"{len(behind)} sequences raised past their column's"
                    " rows"))
        if kind in ("rows", "all"):
            out += self._as_pair().repair_plan(db, "rows")
        return out

    def _set_back(self, db, behind):
        """`setval` statements returning each sequence to its present
        value (and whether that value was handed out)."""
        out = []
        for seq, *_ in behind:
            sch, name = (p.strip('"').replace('""', '"')
                         for p in seq.split(".", 1))
            last, called, _ = self._sequence_state("dst", db, sch, name)
            out.append(f"select setval('{seq.replace(chr(39), chr(39) * 2)}',"
                       f" {last}, {'true' if called else 'false'})")
        return out

    def apply(self, db, action):
        """A sequence repair is the target's own statements, each in a
        transaction of its own; rows go through the pair."""
        if action.kind == "sequences":
            self._target_only("dst", "raise a sequence")
            for sql in action.statements:
                self._writer_run("dst", db, sql)
            return None
        return self._as_pair().apply(db, action)

    def _writer_run(self, side, db, sql):
        conn = self._connect(side, db)
        try:
            conn.autocommit = True
            conn.cursor().execute(sql)
        finally:
            conn.close()

    # ---- the deep checks: one catalogue, read the same way on both sides --

    def _deep_pairs(self, db):
        """[(source table, target table)] the hop takes, paired as the
        checks pair them - by the hop's renames - leaving out a table whose
        columns the hop maps, which the pair compares through the mapping."""
        pair = self._as_pair()
        pairs, _, _, _ = pair.match_tables(
            self.neutral_tables("src", db), self.neutral_tables("dst", db),
            pair._renamer(db))
        return [(s, d) for s, d in pairs if not self.through_pair(db, s)]

    def check_deep(self, db):
        return self._shared_deep(db)

    def _shared_deep(self, db):
        """What a count and a checksum do not say, read off the catalogue
        and a sample of rows on both sides: the keys and columns each table
        has, the indexes, the values that arrive narrowed, shifted or
        rendered otherwise, the sequences' owners and the grants."""
        pairs = self._deep_pairs(db)
        out = []
        for word, fn in self._deep_checks():
            try:
                got = fn(db, pairs)
            except SystemExit:
                raise
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(self._deep_error(db, word, e))
                continue
            out += got if isinstance(got, list) else [got]
        return out

    def _deep_checks(self):
        """[(what it is called, the check)], run in this order by
        `check_deep`."""
        return self._deep_shared() + self._deep_extra()

    def _deep_extra(self):
        """[(what it is called, the check)] only some of these servers
        have anything to say to."""
        return []

    def _deep_shared(self):
        return [("keys", self._deep_keys), ("columns", self._deep_columns),
                ("indexes", self._deep_indexes),
                ("boundary", self._deep_boundary),
                ("collation", self._deep_collation),
                ("encoding", self._deep_encoding),
                ("generated", self._deep_generated),
                ("nullempty", self._deep_nullempty),
                ("float", self._deep_float), ("render", self._deep_render),
                ("timeshift", self._deep_timeshift),
                ("ownership", self._deep_ownership),
                ("grants", self._deep_grants),
                ("seq-grants", self._deep_sequence_grants),
                ("objects", self._deep_objects)]

    def _drift(self, ok, lines, fix, file=None):
        """A deep check's answer: `ok`, the finding of a check that found
        nothing, or - where lines differ - the same finding turned into a
        difference naming them, kept whole in a file of the report."""
        if not lines:
            return ok
        if file:
            db = ok.scope.split()[0]
            path = self.hop.report_dir(db) / file
            path.write_text("\n".join(lines) + "\n")
            ok.report = str(path)
        from .base import clauses
        ok.status, ok.fix_hint = "diff", fix
        ok.detail = f"{len(lines)} differ: {clauses(lines, 6)}"
        return ok

    def _deep_keys(self, db, pairs):
        """Tables with no key: copied and compared whole, but their changes
        cannot be followed and a difference cannot be named by key."""
        keyless = [t for t in self.neutral_tables("src", db)
                   if not self.neutral_key("src", db, t)]
        if keyless:
            return Result(
                "deep", f"{db} keys", "diff",
                f"{len(keyless)} tables have no key (their changes cannot be"
                f" followed, a difference in them cannot be named by key): "
                + ", ".join(keyless[:5]), "",
                "give each a primary key before the cutover, or leave it out"
                " of the hop and move it with a copy")
        return Result("deep", f"{db} keys", "ok",
                      "every table on the source has a key")

    @staticmethod
    def _squash(text):
        return "".join(str(text or "").split()).lower()

    def _deep_columns(self, db, pairs):
        """Each column's type, nullability, default and identity, held to
        the target's - and the columns whose target type cannot hold what
        the source's can."""
        from .. import canon, verdict
        lines, narrow, seen = [], [], 0
        family = canon.dialect(self.CANON_ENGINE)
        for s, d in pairs:
            st = dict(self.neutral_columns("src", db, s))
            dt = dict(self.neutral_columns("dst", db, d))
            sr = self.neutral_column_rules("src", db, s)
            dr = self.neutral_column_rules("dst", db, d)
            for name, declared in st.items():
                if name not in dt:
                    lines.append(f"{s}.{name}: not on the target")
                    continue
                seen += 1
                a, b, bits = sr.get(name, {}), dr.get(name, {}), []
                if self._squash(declared) != self._squash(dt[name]):
                    bits.append(f"type {declared} -> {dt[name]}")
                if a.get("null") != b.get("null"):
                    bits.append("null " + ("allowed" if a.get("null")
                                           else "refused") + " -> "
                                + ("allowed" if b.get("null")
                                   else "refused"))
                if self._squash(a.get("default")) != \
                        self._squash(b.get("default")):
                    bits.append(f"default {a.get('default')} ->"
                                f" {b.get('default')}")
                if a.get("identity") != b.get("identity"):
                    bits.append("identity "
                                f"{bool(a.get('identity'))} ->"
                                f" {bool(b.get('identity'))}")
                if bits:
                    lines.append(f"{s}.{name}: " + ", ".join(bits))
                why = verdict.narrowing(family, declared, dt[name])
                if why:
                    narrow.append(f"{s}.{name}: {why}")
        found = self._drift(
            Result("deep", f"{db} columns", "ok",
                   f"{seen} columns compared, type, null, default"
                   " and identity identical"),
            lines,
            "align the target's columns with the source's",
            "deep-columns.diff")
        return [found, verdict.narrowing_result(db, narrow)]

    def _deep_indexes(self, db, pairs):
        """The secondary indexes each table has, by name, held to the
        target's; and an index the target keeps and answers no query with."""
        lines = []
        for s, d in pairs:
            have = {n: (u, tuple(c)) for n, u, c, _ in
                    self.neutral_indexes("dst", db, d)}
            for name, unique, cols, whole in self.neutral_indexes(
                    "src", db, s):
                if name not in have:
                    lines.append(f"{s}: index {name} is not on the target")
                elif whole and have[name] != (unique, tuple(cols)):
                    lines.append(f"{s}: index {name} is"
                                 f" {'unique ' if unique else ''}"
                                 f"{list(cols)} on the source and"
                                 f" {'unique ' if have[name][0] else ''}"
                                 f"{list(have[name][1])} on the target")
        broken = [f"{t}.{i}" for t, i in self._scoped_rows(db, self._rows(
            "dst", db, "select n.nspname || '.' || t.relname, i.relname from"
                       " pg_catalog.pg_index x join pg_catalog.pg_class i on"
                       " i.oid = x.indexrelid join pg_catalog.pg_class t on"
                       " t.oid = x.indrelid join pg_catalog.pg_namespace n on"
                       " n.oid = t.relnamespace where not x.indisvalid and"
                       f" n.nspname not in ({self._sys()})"))]
        lines += [f"target index {b} exists and answers no query"
                  for b in broken]
        return self._drift(
            Result("deep", f"{db} indexes", "ok",
                   "every index of the source is on the target and"
                   " answers queries"),
            lines,
            "create the missing indexes on the target, and"
            " drop and make again those that are not valid",
            "deep-indexes.diff")

    def _deep_boundary(self, db, pairs):
        """A target whose largest key is past the source's: something else
        has been writing there, or a change was applied twice."""
        from .. import canon
        ahead, checked = [], 0
        for s, d in pairs:
            key = self.neutral_key("src", db, s)
            if len(key) != 1 or key != self.neutral_key("dst", db, d):
                continue
            declared = dict(self.neutral_columns("src", db, s))[key[0]]
            if canon.type_class(self.CANON_ENGINE, declared) != "integer":
                continue
            top = []
            for side, t in (("src", s), ("dst", d)):
                got = self._rows(side, db, f"select max({self._q(key[0])})"
                                           f" from "
                                           f"{self._qualified(side, db, t)}")
                top.append(got[0][0] or 0)
            checked += 1
            if top[1] > top[0]:
                ahead.append(f"{s} src_max={top[0]} dst_max={top[1]}")
        if ahead:
            return Result("deep", f"{db} boundary", "diff",
                          f"target max(pk) AHEAD of source on {len(ahead)}: "
                          + "; ".join(ahead[:4]), "",
                          "writes landing on the target, or a change applied"
                          " twice: find the writer before cutover")
        return Result("deep", f"{db} boundary", "ok",
                      f"max(pk) checked on {checked} tables, none ahead of"
                      " the source" if checked
                      else "no single-integer-key tables to check")

    def _text_columns(self, db, pairs):
        """[(source table, target table, column)] of the character columns
        of the paired tables."""
        from .. import canon
        out = []
        for s, d in pairs:
            dst = dict(self.neutral_columns("dst", db, d))
            for name, declared in self.neutral_columns("src", db, s):
                if name in dst and canon.type_class(
                        self.CANON_ENGINE, declared) == "text":
                    out.append((s, d, name))
        return out

    def _deep_collation(self, db, pairs):
        """A text column compared in another order on the target: a key
        column whose collation folds case or accents can take two source
        rows for one."""
        def read(side):
            return {(t, c): str(coll) for t, c, coll in self._scoped_rows(
                db, self._rows(side, db, "select table_schema || '.' ||"
                                         " table_name, column_name,"
                                         " collation_name from"
                                         " information_schema.columns where"
                                         " collation_name is not null and"
                                         " table_schema not in "
                                         f"({self._sys()})"
                                         f"{self._visible()}"))}
        src, dst = read("src"), read("dst")
        lines = []
        for s, d in pairs:
            for (t, c), coll in sorted(src.items()):
                if t == s and dst.get((d, c)) != coll:
                    lines.append(f"{s}.{c}: collation {coll} on the source,"
                                 f" {dst.get((d, c)) or 'the default'} on the"
                                 " target")
            for (t, c), coll in sorted(dst.items()):
                if t == d and (s, c) not in src:
                    lines.append(f"{s}.{c}: collation {coll} on the target,"
                                 " the default on the source")
        return self._drift(
            Result("deep", f"{db} collation", "ok",
                   "the collation of every text column matches"),
            lines,
            "give the target column the source's collation,"
            " or decide which order is canonical")

    def _deep_encoding(self, db, pairs):
        """The character encoding of each database, and the replacement
        characters a lossy conversion leaves in the target's text."""
        lines = []
        enc = [self._rows(side, db, "show server_encoding")[0][0]
               for side in ("src", "dst")]
        if enc[0] != enc[1]:
            lines.append(f"server_encoding differs: src={enc[0]}"
                         f" dst={enc[1]}")
        for s, d, col in self._text_columns(db, pairs)[:30]:
            counts = []
            for side, t in (("src", s), ("dst", d)):
                c = self._q(col)
                counts.append(int(self._rows(
                    side, db, f"select coalesce(sum(char_length({c}) -"
                              f" char_length(replace({c}, chr(65533), ''))),"
                              f" 0) from {self._qualified(side, db, t)}"
                )[0][0]))
            if counts[1] > counts[0]:
                lines.append(f"{s}.{col}: {counts[1] - counts[0]} extra"
                             " U+FFFD replacement characters on the target")
        return self._drift(
            Result("deep", f"{db} encoding", "ok",
                   "encodings match, no replacement-character"
                   " excess"),
            lines,
            "load the affected rows again through a client"
            " encoding that does not lose characters")

    def _deep_generated(self, db, pairs):
        """A column generated on one side and plain on the other, or by a
        different expression, or a stored value that is not its
        expression."""
        def read(side):
            return {(t, c): str(e or "") for t, c, e in self._scoped_rows(
                db, self._rows(side, db, "select table_schema || '.' ||"
                                         " table_name, column_name,"
                                         " generation_expression from"
                                         " information_schema.columns where"
                                         " is_generated = 'ALWAYS' and"
                                         " table_schema not in "
                                         f"({self._sys()})"
                                         f"{self._visible()}"))}
        src, dst = read("src"), read("dst")
        lines = []
        for s, d in pairs:
            for (t, c), expr in sorted(src.items()):
                if t != s:
                    continue
                if (d, c) not in dst:
                    lines.append(f"{s}.{c}: generated on the source, plain"
                                 " or missing on the target")
                elif self._squash(expr) != self._squash(dst[(d, c)]):
                    lines.append(f"{s}.{c}: generation expression differs")
                else:
                    n = self._rows(
                        "dst", db, f"select count(*) from "
                                   f"{self._qualified('dst', db, d)} where "
                                   f"{self._q(c)} is distinct from ({expr})"
                    )[0][0]
                    if int(n):
                        lines.append(f"{s}.{c}: {int(n)} rows where the"
                                     " stored value is not its expression")
            for (t, c) in sorted(dst):
                if t == d and (s, c) not in src:
                    lines.append(f"{s}.{c}: generated on the target, plain"
                                 " on the source")
        return self._drift(
            Result("deep", f"{db} generated", "ok",
                   f"{len(src)} generated columns match"
                   if src else "no generated columns"),
            lines,
            "align the generation expression on the target"
            " and derive the column again")

    def _deep_nullempty(self, db, pairs):
        """NULL against the empty string, per text column: a swap of one
        for the other survives a row checksum, and changes what IS NULL and
        a unique key mean."""
        lines = []
        by_table = {}
        for s, d, col in self._text_columns(db, pairs)[:30]:
            by_table.setdefault((s, d), []).append(col)
        for (s, d), cols in by_table.items():
            def query(side, t, cols=cols):
                parts = ", ".join(
                    f"count(*) filter (where {self._q(c)} is null),"
                    f" count(*) filter (where {self._q(c)} = '')"
                    for c in cols)
                return self._rows(side, db, f"select {parts} from "
                                            f"{self._qualified(side, db, t)}"
                                  )[0]
            a, b = query("src", s), query("dst", d)
            for i, c in enumerate(cols):
                x, y = a[2 * i:2 * i + 2], b[2 * i:2 * i + 2]
                if list(x) != list(y):
                    lines.append(f"{s}.{c}: source null/empty={x[0]}/{x[1]},"
                                 f" target={y[0]}/{y[1]}")
        return self._drift(
            Result("deep", f"{db} nullempty", "ok",
                   "NULL and the empty string split the same way on"
                   " both sides"),
            lines,
            "decide which of NULL and '' is canonical for"
            " each column, and normalize the other side")

    def _sampled(self, side, db, table, key, expr, column, limit):
        """{key text: value} of the first rows by key that hold the
        column."""
        k, c = self._q(key), self._q(column)
        return {str(r[0]): r[1] for r in self._rows(
            side, db, f"select {k}::text, {expr} from "
                      f"{self._qualified(side, db, table)} where {c} is not"
                      f" null order by {k} limit {int(limit)}")}

    def _class_columns(self, db, pairs, classes, most):
        """[(source table, target table, key, column)] of the paired tables
        with a one-column key, over columns of these classes."""
        from .. import canon
        out = []
        for s, d in pairs:
            key = self.neutral_key("src", db, s)
            if len(key) != 1:
                continue
            dst = dict(self.neutral_columns("dst", db, d))
            for name, declared in self.neutral_columns("src", db, s):
                if name in dst and canon.type_class(
                        self.CANON_ENGINE, declared) in classes:
                    out.append((s, d, key[0], name))
        return out[:most]

    def _deep_float(self, db, pairs):
        """Floating-point columns that drifted past a relative tolerance:
        a widening or a text round trip changes them, and a checksum of
        the exact value is the only other thing that sees it."""
        tol = float(self.hop.options.get("float_tolerance", 1e-9))
        cols = self._class_columns(db, pairs, ("float",), 10)
        lines = []
        for s, d, key, col in cols:
            a = self._sampled("src", db, s, key, self._q(col), col, 500)
            b = self._sampled("dst", db, d, key, self._q(col), col, 500)
            worst, n = 0.0, 0
            for k, v in a.items():
                if k in b:
                    x, y = float(v), float(b[k])
                    if abs(x - y) > tol * max(abs(x), abs(y), 1.0):
                        n += 1
                        worst = max(worst, abs(x - y))
            if n:
                lines.append(f"{s}.{col}: {n} values drift beyond tolerance"
                             f" (max {worst:g})")
        return self._drift(
            Result("deep", f"{db} float", "ok",
                   "float columns within tolerance" if cols
                   else "no float columns"),
            lines,
            "the precision changed on the target: use an"
            " exact type, or set options.float_tolerance to"
            " accept it")

    def _deep_render(self, db, pairs):
        """Values of types that can print differently on two servers - an
        interval, a bit string, an enum, a range, a composite - read as text
        from both sides for a few rows."""
        lines, audited = [], 0
        odd = ("interval", "bit", "varbit", "money", "tsvector", "tsquery",
               "xml", "point", "polygon", "path", "circle", "box", "line",
               "lseg")
        for s, d in pairs:
            key = self.neutral_key("src", db, s)
            if len(key) != 1:
                continue
            dst = dict(self.neutral_columns("dst", db, d))
            for name, declared in self.neutral_columns("src", db, s)[:40]:
                base = str(declared).split("(")[0].strip().lower()
                if name not in dst or not (base in odd or any(
                        base.startswith(o) for o in odd)):
                    continue
                if audited >= 20:
                    break
                audited += 1
                text = f"{self._q(name)}::text"
                a = self._sampled("src", db, s, key[0], text, name, 5)
                b = self._sampled("dst", db, d, key[0], text, name, 5)
                for k, v in a.items():
                    if k in b and b[k] != v:
                        lines.append(f"{s}.{name}: renders differently (src"
                                     f" {v[:40]!r} dst {b[k][:40]!r})")
                        break
        return self._drift(
            Result("deep", f"{db} render", "ok",
                   f"{audited} columns of types that can print"
                   " differently sampled, rendering identical"
                   if audited else "no columns of such types"),
            lines,
            "check the type's definition and the servers'"
            " versions on the target")

    def _deep_timeshift(self, db, pairs):
        """A timestamp column every row of which the target holds shifted
        by a zone's offset: a conversion applied to the whole column, not
        rows damaged one by one."""
        from .. import verdict
        shifts = []
        for s, d, key, col in self._class_columns(db, pairs, ("timestamp",),
                                                  10):
            expr = f"extract(epoch from {self._q(col)})::float8"
            a = self._sampled("src", db, s, key, expr, col, 200)
            b = self._sampled("dst", db, d, key, expr, col, 200)
            line = verdict.uniform_shift(f"{s}.{col}", a, b)
            if line:
                shifts.append(line)
        return verdict.timeshift_result(db, shifts)

    def _owners(self, side, db):
        return {t: str(o) for t, o in self._scoped_rows(db, self._rows(
            side, db, "select schemaname || '.' || tablename, tableowner"
                      f" from pg_catalog.pg_tables where schemaname not in"
                      f" ({self._sys()})"))}

    def _deep_ownership(self, db, pairs):
        """The account that owns each table, which decides whose privileges
        its triggers and views run with and who may change it."""
        src, dst = self._owners("src", db), self._owners("dst", db)
        lines = [f"{s}: owned by {src[s]} on the source, {dst[d]} on the"
                 " target" for s, d in pairs
                 if s in src and d in dst and src[s] != dst[d]]
        return self._drift(
            Result("deep", f"{db} ownership", "ok",
                   f"{len(pairs)} tables are owned by the same"
                   " account on both sides"),
            lines,
            "alter table ... owner to the source's owner")

    def _privileges(self, side, db, kind):
        """{(object, grantee): {privilege}} of a kind of object
        (`table_privileges`, or the sequences of `usage_privileges`)."""
        if kind == "table":
            sql = ("select table_schema || '.' || table_name, grantee,"
                   " privilege_type from information_schema.table_privileges"
                   f" where table_schema not in ({self._sys()})")
        else:
            sql = ("select object_schema || '.' || object_name, grantee,"
                   " privilege_type from information_schema.usage_privileges"
                   " where object_type = 'SEQUENCE' and object_schema not in"
                   f" ({self._sys()})")
        out = {}
        for obj, who, priv in self._rows(side, db, sql):
            out.setdefault((str(obj), str(who)), set()).add(str(priv))
        return out

    def _deep_grants(self, db, pairs):
        """What each account may do with each table, held to the target's:
        a privilege the source gives and the target does not is a refusal
        the application meets after the cutover."""
        src, dst = (self._privileges(side, db, "table")
                    for side in ("src", "dst"))
        lines = []
        for s, d in pairs:
            for (obj, who), privs in sorted(src.items()):
                if obj != s:
                    continue
                lack = privs - dst.get((d, who), set())
                if lack:
                    lines.append(f"{s}: {who} lacks {', '.join(sorted(lack))}"
                                 " on the target")
        return self._drift(
            Result("deep", f"{db} grants", "ok",
                   f"the privileges on {len(pairs)} tables match"),
            lines,
            "grant the same privileges on the target",
            "deep-grants.diff")

    def _deep_sequence_grants(self, db, pairs):
        """The same for the sequences that draw for a column: an insert
        needs the right to take the next value."""
        mine = {f"{sch}.{name}" for sch, name, *_ in
                self._owned_sequences("src", db)}
        src, dst = (self._privileges(side, db, "sequence")
                    for side in ("src", "dst"))
        lines = []
        for (obj, who), privs in sorted(src.items()):
            if obj in mine:
                lack = privs - dst.get((obj, who), set())
                if lack:
                    lines.append(f"{obj}: {who} lacks "
                                 f"{', '.join(sorted(lack))} on the target")
        return self._drift(
            Result("deep", f"{db} seq-grants", "ok",
                   f"the privileges on {len(mine)} sequences match"),
            lines,
            "grant the same privileges on the target's"
            " sequences")

    # ---- checks for what only some of these servers keep ------------------

    #: what an operator runs to validate a constraint left unvalidated
    VALIDATE_HINT = "alter table ... validate constraint ..."

    def _deep_foreign_keys(self, db, pairs):
        """The foreign keys of each table, with what they do on a delete and
        an update, held to the target's: a table the target has without its
        key takes a row that points at nothing."""
        to = dict(pairs)
        have = {(c, tuple(cols), p, tuple(pc), od, ou)
                for c, cols, p, pc, od, ou in self._fk_facts("dst", db)}
        lines = []
        for c, cols, p, pc, od, ou in self._fk_facts("src", db):
            if c not in to:
                continue
            want = (to[c], tuple(cols), to.get(p, p), tuple(pc), od, ou)
            if want not in have:
                same = [h for h in have if h[:4] == want[:4]]
                lines.append(
                    f"{c}({', '.join(cols)}) -> {p}({', '.join(pc)}): "
                    + ("acts differently on delete or update on the target"
                       if same else "not on the target"))
        return self._drift(
            Result("deep", f"{db} fk", "ok",
                   "every foreign key of the source is on the target"
                   " and acts the same"),
            lines,
            "add the foreign keys to the target")

    def _deep_unvalidated(self, db, pairs):
        """Foreign key and check constraints on the target that hold for new
        rows and were never checked against the rows already there."""
        bad = [f"{t}.{name}" for t, name, _, valid, *_ in
               self._constraints("dst", db, ("f", "c")) if not valid]
        if bad:
            return Result("deep", f"{db} checks", "diff",
                          f"{len(bad)} constraints NOT VALIDATED (existing"
                          " rows unchecked): " + "; ".join(bad[:5]), "",
                          self.VALIDATE_HINT)
        return Result("deep", f"{db} checks", "ok",
                      "all foreign key and check constraints validated")

    def _deep_deferrable(self, db, pairs):
        """A foreign key the source can defer to the end of a transaction
        and the target checks at each statement: code that inserts a child
        before its parent works on one and is refused on the other."""
        to = dict(pairs)
        have = {(t, name): (df, di) for t, name, _, _, df, di, _ in
                self._constraints("dst", db, ("f",))}
        lines = []
        for t, name, _, _, df, di, _ in self._constraints("src", db, ("f",)):
            got = have.get((to.get(t, t), name))
            if t in to and got is not None and got != (df, di):
                lines.append(f"{t}.{name}: deferrable/initially deferred="
                             f"{df}/{di} on the source, {got[0]}/{got[1]} on"
                             " the target")
        return self._drift(
            Result("deep", f"{db} deferrable", "ok",
                   "foreign key deferral flags match"),
            lines,
            "alter table ... alter constraint ... to match"
            " the source")

    def _triggers(self, side, db):
        return {(t, n): str(en) for t, n, en in self._rows(
            side, db, "select ns.nspname || '.' || c.relname, g.tgname,"
                      " g.tgenabled from pg_catalog.pg_trigger g join"
                      " pg_catalog.pg_class c on c.oid = g.tgrelid join"
                      " pg_catalog.pg_namespace ns on ns.oid = c.relnamespace"
                      " where not g.tgisinternal and ns.nspname not in "
                      f"({self._sys()})")
                if self.takes(db, "trigger", f"{t}.{n}", by=t)}

    def _deep_triggers(self, db, pairs):
        """Triggers the source has that the target lacks or has switched
        off: the work they do - a derived column, an audit row - stops
        without an error."""
        to = dict(pairs)
        src, dst = self._triggers("src", db), self._triggers("dst", db)
        lines = []
        for (t, name), enabled in sorted(src.items()):
            if t not in to:
                continue
            there = dst.get((to[t], name))
            if there is None:
                lines.append(f"{t}.{name}: not on the target")
            elif there == "D" and enabled != "D":
                lines.append(f"{t}.{name}: disabled on the target")
        return self._drift(
            Result("deep", f"{db} triggers", "ok",
                   f"{len(src)} triggers of the source are on the"
                   " target and enabled"),
            lines,
            "create or enable the triggers on the target")

    def _deep_matviews(self, db, pairs):
        """Materialized views the target lacks or holds unpopulated: they
        answer from nothing."""
        def read(side):
            return {n: bool(p) for n, p in self._rows(
                side, db, "select schemaname || '.' || matviewname,"
                          " ispopulated from pg_catalog.pg_matviews where"
                          f" schemaname not in ({self._sys()})")
                    if self.takes(db, "view", n)}
        src, dst = read("src"), read("dst")
        to = dict(pairs)
        lines = []
        for name, populated in sorted(src.items()):
            there = dst.get(to.get(name, name))
            if there is None:
                lines.append(f"{name}: not on the target")
            elif populated and not there:
                lines.append(f"{name}: populated on the source, empty on the"
                             " target")
        return self._drift(
            Result("deep", f"{db} matviews", "ok",
                   f"{len(src)} materialized views are on the"
                   " target and populated"),
            lines,
            "create the view on the target and refresh it")

    # ---- tables made, objects read -----------------------------------------

    def create_missing(self, db, log=None):
        """The tables the target lacks, made from the source's definition
        before any is filled, by the pair every engine is carried by."""
        return self._as_pair().create_missing(db, log)

    def finish_created(self, db, log=None):
        """The source's indexes on those tables, once their rows are in."""
        return self._as_pair().finish_created(db, log)

    def _deep_objects(self, db, pairs):
        """The views, sequences and functions the source keeps beside its
        tables, held to the target's: one missing there is behaviour the
        application relied on, gone without an error."""
        def inventory(side):
            found = {("view", n) for n, _ in self.neutral_views(side, db)}
            found |= {("sequence", f"{s}.{q}") for s, q in self._rows(
                side, db, "select sequence_schema, sequence_name from"
                          " information_schema.sequences where"
                          f" sequence_schema not in ({self._sys()})")
                      if self.takes(db, "sequence", f"{s}.{q}")}
            found |= {("function", n) for n in self._function_names(side, db)}
            return found
        src, dst = inventory("src"), inventory("dst")
        lines = [f"{kind} {name}: not on the target"
                 for kind, name in sorted(src - dst)]
        return self._drift(
            Result("deep", f"{db} objects", "ok",
                   f"{len(src)} views, sequences and"
                   " functions of the source are on the"
                   " target"),
            lines,
            "create the missing objects on the target")

    # ---- migkit's own row, where machines of a hop keep their records ------

    #: how `shared` and `fencing` write migkit's own rows here
    OWN_DIALECT = "postgres"

    def own_name(self, side, db):
        """`migkit_origin`'s name in the database of `db` on `side`."""
        from .. import twoway
        return f"public.{twoway.TABLE}"

    def _origin_table(self, side, db):
        """`migkit_origin`, made once, on a connection of its own, where it
        is not there."""
        import threading

        from .. import twoway
        lock = self.__dict__.setdefault("_origin_lock", threading.Lock())
        made = self.__dict__.setdefault("_origin_made", set())
        with lock:
            if (side, self._d(side, db)) in made:
                return
            conn = self._connect(side, db)
            try:
                conn.autocommit = True
                conn.cursor().execute(
                    f"create table if not exists public.{twoway.TABLE}"
                    " (origin text primary key, n bigint not null default"
                    " 0, seen text)")
            finally:
                conn.close()
            made.add((side, self._d(side, db)))

    def own_table(self, side, db):
        """(a connection of its own to the database of `db` on `side`,
        `migkit_origin`'s name there), the table made where it is not."""
        self._origin_table(side, db)
        return self._connect(side, db), self.own_name(side, db)

    def reach_items(self, side):
        return []

    def _deep_error(self, db, who, e):
        return Result("deep", f"{db} {who}", "error",
                      f"could not ask the {who}:"
                      f" {str(e).strip().splitlines()[0][:90]}")
