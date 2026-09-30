"""Iceberg and Delta tables as one side of a pair (backlog 33; Leapfrog:
engine reach, item 1).

Both are Parquet files with a log saying which files are the table:
Iceberg's metadata through a catalog (a SQL database - SQLite on a disk
will do - or a REST catalog), Delta's `_delta_log` beside the files. A
hop's database is a namespace (Iceberg) or a directory under the
endpoint's root (Delta); a table is a table in it.

Read through each format's own library (pyiceberg, delta-rs) and digested
by DuckDB in this process: the table's rows handed to it as Arrow and each
rendered in its SQL (`warehouse_sql`) - the number `canon.fold_rows` gives,
without a row through Python. Measured, 200,000 rows of an integer, a
text, a decimal and a float: 0.27 s from a Delta table and 0.12 s from an
Iceberg one, where the rows folded in Python took 0.55 s and 0.57 s, and
the two numbers were the same. A column DuckDB does not render as migkit
does is folded by `canon.fold_rows` instead, and the table's first rows are
rendered both ways and held to each other before the SQL is trusted.

Written a batch to a commit, and the commit says which batch it holds -
Iceberg in its snapshot's summary properties, Delta in the commit's own
metadata and, for a tail, in the protocol's transaction action (`txn`: an
application id and a version, of which the log keeps the latest). A batch
the table's last commit says it holds already is not written again, so a
retry after a commit whose answer was lost writes nothing twice. The copy
merges a keyed batch in by its key and appends a keyless one; the tail
commits each table's changes of a batch once, and says so per table, since
neither format commits two tables in one transaction.

Iceberg keeps a table's key as its identifier fields; Delta has no key, so
migkit keeps one in the key columns' own metadata (`migkit.key`, the
column's place in it). One writer a table: a commit beside another is
retried by the library or refused. Compaction, snapshot expiry and vacuum
are the operator's: migkit leaves them alone.
"""
import json
import os
import posixpath

from .. import pieces
from .base import Engine, NeutralCopier
from .warehouse import arrow_array

#: the summary and commit metadata a batch is named in
HOP, BATCH, TOKEN = "migkit.hop", "migkit.batch", "migkit.token"


class _Lake(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    SQL_DIALECT = None
    #: the digest is DuckDB's in this process, which is where the rows are
    FOLDED_BECAUSE = "is files, read and folded in this process"
    #: files keep no order to resume by: a table is read in one pass
    RESUMES_BY_KEY = False
    #: one writer a table: two commits at once, one is refused or retried
    WRITES_IN_PARALLEL = False
    #: the tail numbers its batches, and each commit says which it holds
    EXACT_BATCHES = True
    PROOF_ROWS = 64
    #: large values - a table's files are Parquet, whose pages hold a value
    #: whole: read as the format's record batches, each made values a slice
    #: cut by bytes at a time (`_rows`) and handed on in batches of
    #: `pieces.BATCH_BYTES`; hashed where they lie by DuckDB's digest over
    #: the rows as Arrow, a wide value as its length and md5
    #: (`warehouse_sql`); one commit a batch written
    LARGE_VALUES = pieces.Way(
        "read in batches cut by bytes, wide values hashed where they lie,"
        " one commit a batch written")

    def _endpoint(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    # ---- what each format provides --------------------------------------

    def _schema(self, side, db, table):
        """[(name, declared type)] in the table's order."""
        raise NotImplementedError

    def _key_names(self, side, db, table):
        raise NotImplementedError

    def _reader(self, side, db, table, names, keys=None):
        """A RecordBatchReader over the table's current rows, `names` only;
        with `keys` ([(column, [values])] for one key column), the rows
        holding them."""
        raise NotImplementedError

    def _arrow_schema(self, side, db, table):
        raise NotImplementedError

    # ---- the contract -----------------------------------------------------

    def neutral_columns(self, side, db, table):
        return self._schema(side, db, table)

    def neutral_key(self, side, db, table):
        return list(self._key_names(side, db, table))

    def _rows(self, reader, names):
        """The reader's rows, each record batch made values a slice cut by
        bytes at a time (`pieces.arrow_rows`): a batch of wide values is
        never all Python values at once."""
        for batch in reader:
            yield from pieces.arrow_rows(batch, names)

    def _where(self, reader, names, where):
        """The reader's rows through a SQL filter, which DuckDB applies."""
        if not where:
            return reader
        import duckdb
        con = duckdb.connect(config={"TimeZone": "UTC"})
        con.register("migkit_rows", reader)
        cols = ", ".join(f'"{n}"' for n in names)
        return con.execute(f"select {cols} from migkit_rows where"
                           f" ({where})").fetch_record_batch()

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        names = [n for n, _ in columns]
        reader = self._where(self._reader(side, db, table, names), names,
                             where)
        # batches of `size` rows or `pieces.BATCH_BYTES`
        yield from pieces.by_bytes((r for rows in self._rows(reader, names)
                                    for r in rows), most_rows=int(size))

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """In key order, which files do not keep: the table is read and
        sorted, as a Parquet table is. The copier reads a lake table in one
        pass (`RESUMES_BY_KEY`); this serves the callers that ask for a row
        or two."""
        names = [n for n, _ in columns]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        rows = [r for b in self.neutral_batches(side, db, table, columns,
                                                5000, where) for r in b]
        if not key:
            return rows[:limit] if limit else rows, None
        at = [names.index(k) for k in key]
        rows.sort(key=lambda r: tuple(r[i] for i in at))
        if after is not None:
            rows = [r for r in rows if tuple(r[i] for i in at) > tuple(after)]
        rows = rows[:limit] if limit else rows
        return rows, (tuple(rows[-1][i] for i in at) if rows else None)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        """The rows whose key's canonical text is one asked for, read with
        the first key column's values handed to the format, which skips
        the files that cannot hold them."""
        from .. import canon
        if not key or not keys:
            return {}
        cls = dict(columns)
        want = {tuple(canon.render_value(cls[k], v) for k, v in zip(key, kk))
                for kk in keys}
        names = [n for n, _ in columns]
        first = sorted({kk[0] for kk in keys if kk[0] is not None},
                       key=repr)
        reader = self._where(self._reader(side, db, table, names,
                                          keys=[(key[0], first)]),
                             names, where)
        found = [r for rows in self._rows(reader, names) for r in rows
                 if self._key_of(columns, key, r) in want]
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        """(rows, digest) by DuckDB over the table's rows as Arrow, where
        it renders every column and its rendering of the first rows is
        this process's; folded by `canon.fold_rows` otherwise."""
        from .. import canon
        from . import warehouse_sql
        names = [n for n, _ in columns]
        declared = dict(self._schema(side, db, table))
        cols = [(n, c, self._duck_type(declared.get(n))) for n, c in columns]
        pick = warehouse_sql.digest_select("duckdb", cols, self._quote)
        if pick is not None and self._proved(side, db, table, cols):
            import duckdb
            con = duckdb.connect(config={"TimeZone": "UTC"})
            con.register("migkit_rows", self._reader(side, db, table, names))
            got = con.execute(f"select {pick} from migkit_rows"
                              + (f" where ({where})" if where else "")
                              ).fetchone()
            self.folded_last = False
            return int(got[0]), str(got[1])
        self.folded_last = True
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 5000,
                                          where):
            k, total = canon.fold_rows([c for _, c in columns], batch, total)
            n += k
        return n, str(total)

    folded_last = True

    @staticmethod
    def _quote(name):
        return '"' + str(name).replace('"', '""') + '"'

    @staticmethod
    def _duck_type(declared):
        """The format's type name, as `warehouse_sql`'s DuckDB rendering
        reads a declared type: its text only where it is a string."""
        base = str(declared or "").split("(")[0].strip().lower()
        return "VARCHAR" if base == "string" else str(declared or "")

    def _proved(self, side, db, table, cols):
        known = self.__dict__.setdefault("_proofs", {})
        at = (side, db, str(table), tuple(cols))
        if at in known:
            return known[at]
        import duckdb

        from . import warehouse_sql
        names = [n for n, _, _ in cols]
        con = duckdb.connect(config={"TimeZone": "UTC"})
        con.register("migkit_rows", self._reader(side, db, table, names))
        text = warehouse_sql.row_expr("duckdb", cols, self._quote)
        rows = con.execute("select " + ", ".join(map(self._quote, names))
                           + f", {text} from migkit_rows limit"
                           f" {int(self.PROOF_ROWS)}").fetchall()
        ok = warehouse_sql.agree(cols, [r[:-1] for r in rows],
                                 [r[-1] for r in rows])
        if rows:
            # an empty table proves nothing, and is asked again later
            known[at] = ok
        return ok

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    # ---- writing ----------------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from .. import canon
        out = []
        for c in columns:
            try:
                out.append([c[0], canon.ddl_type(self.CANON_ENGINE, c[1],
                                                 tuple(c[2] or ()))])
            except ValueError:
                raise SystemExit(
                    f"{table}.{c[0]}: a {c[1]} column has no"
                    f" {self.CANON_ENGINE.capitalize()} counterpart migkit"
                    " makes; leave the column out with mapping.columns")
        return json.dumps({"columns": out, "key": list(key)})

    def _arrow(self, side, db, table, names, rows):
        """The rows as an Arrow table of the table's own schema, every
        column of it, a column the batch does not carry left empty."""
        import pyarrow as pa
        schema = self._arrow_schema(side, db, table)
        at = {n: i for i, n in enumerate(names)}
        arrays = []
        for field in schema:
            i = at.get(field.name)
            values = [r[i] for r in rows] if i is not None \
                else [None] * len(rows)
            arrays.append(arrow_array(field.type, values))
        return pa.Table.from_arrays(arrays, schema=schema)

    @staticmethod
    def _fingerprint(table, names, rows, extra=""):
        from .warehouse import _Warehouse
        return _Warehouse._fingerprint(table, names, rows, extra)

    def neutral_write(self, side, db, table, columns, rows):
        """One commit a batch: merged in by the key, or appended where the
        table has none, the batch named in the commit. A batch the table's
        last commit already names is not written again."""
        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        rows = [[None if v is canon.ABSENT else v for v in r] for r in rows]
        fp = self._fingerprint(table, names, rows)
        if self._holds(side, db, table, fp):
            return len(rows)
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        self._commit_batch(side, db, table, key,
                           self._arrow(side, db, table, names, rows),
                           {HOP: self.hop.name, BATCH: fp})
        return len(rows)

    def neutral_apply(self, side, db, changes):
        """Each table's changes of the batch - collapsed to one a key - in
        one commit that names the batch's number and position. A table
        whose last commit names this very batch and position has it
        already and is passed over; the tail replays a batch after a stop,
        and a table may have committed it before the stop and another
        not."""
        from .. import canon, twoway
        self._target_only(side, "apply changes")
        seen = twoway.seen_of(twoway.batch_seen(self))
        by_table = {}
        for table, (kind, key, values) in self._net_rows(side, db, changes):
            if any(isinstance(v, canon.Added) for v in values.values()):
                raise SystemExit(f"{table}: a counter's change adds to the"
                                 " value where it stands, which only a"
                                 " two-way hop does")
            by_table.setdefault(str(self.local_table(table)).split(".")[-1],
                                []).append((kind, dict(key), dict(values)))
        props = {HOP: self.hop.name}
        if seen:
            props.update({BATCH: str(seen["batch"]),
                          TOKEN: json.dumps(seen["token"], default=str)})
        for table, rows in by_table.items():
            if seen and self._holds(side, db, table, props[BATCH],
                                    props[TOKEN]):
                continue
            self._commit_changes(side, db, table, rows, props,
                                 seen["batch"] if seen else None)
        return len(changes)

    def origin_seen(self, side, db):
        """None: a batch is committed a table at a time here, so no one
        commit says the whole batch landed - the tail goes on from its own
        saved position, and each table passes over the batch it holds
        already (`neutral_apply`)."""
        return None

    def _apply_upsert(self, side, db, table, key, values):
        """One row by its key, as a batch of one: a repair writes rows
        this way."""
        self.neutral_apply(side, db, [{"op": "update", "table": table,
                                       "key": dict(key),
                                       "values": dict(values)}])

    def _apply_delete(self, side, db, table, key):
        self.neutral_apply(side, db, [{"op": "delete", "table": table,
                                       "key": dict(key)}])

    def _full_rows(self, side, db, table, rows):
        """The upserts of a batch as whole rows: a change that carried only
        some columns takes the others from the row the table holds, since
        a file table rewrites a row whole."""
        from .. import canon
        cols = self._schema(side, db, table)
        names = [n for n, _ in cols]
        key = self.neutral_key(side, db, table)
        partial = [k for kind, k, v in rows if kind != "delete"
                   and set({**k, **v}) != set(names)]
        have = {}
        if partial:
            classes = [(n, canon.type_class(self.CANON_ENGINE, t) or "text")
                       for n, t in cols]
            have = self.neutral_rows_by_key(
                side, db, table, classes, key,
                [tuple(k[c] for c in key) for k in partial])
        out = []
        for kind, k, v in rows:
            if kind == "delete":
                continue
            row = {**k, **v}
            if set(row) != set(names):
                classes = dict((n, canon.type_class(self.CANON_ENGINE, t)
                                or "text") for n, t in cols)
                ident = tuple(canon.render_value(classes[c], k[c])
                              for c in key)
                old = have.get(ident)
                if old is not None:
                    row = {**dict(zip(names, old)), **row}
            out.append([row.get(n) for n in names])
        return names, out


class IcebergEngine(_Lake):
    """Apache Iceberg, through pyiceberg. The endpoint's `catalog` is the
    catalog's URI - `sqlite:////path/catalog.db` (or any SQL database's),
    or a REST catalog's `http(s)://` address - and `warehouse` where new
    tables' files go (a directory or an `s3://` prefix);
    `catalog_properties` passes anything else the catalog takes, and
    `catalog_name` the name its tables are kept under (`migkit`)."""
    CANON_ENGINE = "iceberg"

    @property
    def OVER_NETWORK(self):
        ep = self.hop.source
        where = str((ep.options or {}).get("warehouse") or "")
        uri = str((ep.options or {}).get("catalog") or "")
        return where.startswith("s3://") or uri.startswith("http")

    def _catalog(self, side):
        known = self.__dict__.setdefault("_catalogs", {})
        if side in known:
            return known[side]
        from pyiceberg.catalog import load_catalog
        opts = self._endpoint(side).options or {}
        uri = str(opts.get("catalog") or "")
        if not uri:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `catalog`, the Iceberg catalog's URI")
        props = {"uri": uri, **(opts.get("catalog_properties") or {})}
        props.setdefault("type", "rest" if uri.startswith("http") else "sql")
        if opts.get("warehouse"):
            where = str(opts["warehouse"])
            props["warehouse"] = (where if "://" in where
                                  else "file://" + os.path.abspath(where))
        # a SQL catalog keeps its tables under the catalog's name: the same
        # database read under another name holds nothing
        known[side] = load_catalog(str(opts.get("catalog_name") or "migkit"),
                                   **props)
        return known[side]

    def _table(self, side, db, table):
        return self._catalog(side).load_table((self._d(side, db), table))

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        return sorted(".".join(ns) for ns in
                      self._catalog("src").list_namespaces())

    def target_missing(self, db):
        from pyiceberg.exceptions import NoSuchNamespaceError
        try:
            self._catalog("dst").list_tables(self._d("dst", db))
        except NoSuchNamespaceError:
            return True
        return False

    def prepare_target(self, db):
        self._catalog("dst").create_namespace_if_not_exists(
            self._d("dst", db))
        return ""

    def side_database(self, side, db):
        """The catalog and the namespace in it: one namespace in two
        catalogs is two targets."""
        opts = self._endpoint(side).options or {}
        return f"{opts.get('catalog')}#{self._d(side, db)}"

    def server_identity(self, side, db):
        """What the catalog says (`interlock`): its name for itself, and
        whether the namespace is there yet."""
        opts = self._endpoint(side).options or {}
        return {"id": str(opts.get("catalog") or ""),
                "name": self._catalog(side).name,
                "absent": self.target_missing(db) if side == "dst" else
                not self._catalog(side).namespace_exists(self._d(side, db))}

    def neutral_tables(self, side, db):
        return sorted(ident[-1] for ident in
                      self._catalog(side).list_tables(self._d(side, db))
                      if not self.hop.excluded(db, ident[-1]))

    def _schema(self, side, db, table):
        return [(f.name, str(f.field_type))
                for f in self._table(side, db, table).schema().fields]

    def _key_names(self, side, db, table):
        schema = self._table(side, db, table).schema()
        ids = set(schema.identifier_field_ids)
        return [f.name for f in schema.fields if f.field_id in ids]

    def _arrow_schema(self, side, db, table):
        return self._table(side, db, table).schema().as_arrow()

    def _reader(self, side, db, table, names, keys=None):
        from pyiceberg.expressions import AlwaysTrue, In
        flt = AlwaysTrue()
        if keys:
            col, values = keys[0]
            flt = In(col, values) if values else AlwaysTrue()
        return self._table(side, db, table).scan(
            row_filter=flt, selected_fields=tuple(names)
        ).to_arrow_batch_reader()

    def table_facts(self, side, db):
        """Rows and bytes from the current snapshot's own totals."""
        out = {}
        for t in self.neutral_tables(side, db):
            snap = self._table(side, db, t).current_snapshot()
            size = (int(snap.summary.additional_properties.get(
                "total-files-size", 0)) if snap else 0)
            out[t] = {"rows": self._count(side, db, t), "bytes": size,
                      "key": bool(self._key_names(side, db, t))}
        return out

    def neutral_create(self, side, db, table, columns, key=()):
        """The table in the namespace, its key as its identifier fields -
        which Iceberg requires to be present in every row."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField
        self._target_only(side, "create a table")
        cat = self._catalog(side)
        ns = self._d(side, db)
        cat.create_namespace_if_not_exists(ns)
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        desc = self.neutral_create_sql(side, db, table, columns, key)
        fields = [NestedField(i + 1, name, _iceberg_type(typ),
                              required=name in key)
                  for i, (name, typ) in enumerate(json.loads(desc)
                                                  ["columns"])]
        ids = [f.field_id for f in fields if f.name in key]
        cat.create_table((ns, table), schema=Schema(
            *fields, identifier_field_ids=ids))
        return desc

    def _holds(self, side, db, table, batch, token=None):
        snap = self._table(side, db, table).current_snapshot()
        if snap is None:
            return False
        props = snap.summary.additional_properties
        return (props.get(HOP) == self.hop.name and props.get(BATCH) == batch
                and (token is None or props.get(TOKEN) == token))

    def _commit_batch(self, side, db, table, key, data, props):
        t = self._table(side, db, table)
        if key:
            t.upsert(data, join_cols=key, snapshot_properties=props)
        else:
            t.append(data, snapshot_properties=props)

    def _key_filter(self, key, keys):
        from pyiceberg.expressions import And, EqualTo, In, Or
        if len(key) == 1:
            return In(key[0], [k[key[0]] for k in keys])
        terms = [And(*[EqualTo(c, k[c]) for c in key]) if len(key) > 1
                 else EqualTo(key[0], k[key[0]]) for k in keys]
        return terms[0] if len(terms) == 1 else Or(*terms)

    def _commit_changes(self, side, db, table, rows, props, n):
        """The batch's keys taken out and its rows put back, in one
        transaction whose snapshots name the batch."""
        t = self._table(side, db, table)
        key = self._key_names(side, db, table)
        if not key:
            raise SystemExit(f"{table} has no key on the target, and a"
                             " change is applied by its key")
        names, full = self._full_rows(side, db, table, rows)
        with t.transaction() as tx:
            tx.delete(self._key_filter(key, [k for _, k, _ in rows]),
                      snapshot_properties=props)
            if full:
                tx.append(self._arrow(side, db, table, names, full),
                          snapshot_properties=props)

    def _count(self, side, db, table):
        snap = self._table(side, db, table).current_snapshot()
        return (int(snap.summary.additional_properties.get("total-records",
                                                           0))
                if snap else 0)

    def neutral_empty(self, side, db, table, where=None):
        """The rows gone, counted from the snapshots' own totals."""
        self._target_only(side, "empty a table")
        before = self._count(side, db, table)
        if not before:
            return 0
        if where:
            self._table(side, db, table).delete(where)
        else:
            self._table(side, db, table).delete()
        return before - self._count(side, db, table)


def _iceberg_type(name):
    """A pyiceberg type from its name, `decimal(12,2)` included."""
    from pyiceberg import types as it
    from .. import canon
    base = str(name).split("(")[0].strip().lower()
    simple = {"long": it.LongType, "int": it.IntegerType,
              "double": it.DoubleType, "float": it.FloatType,
              "boolean": it.BooleanType, "string": it.StringType,
              "binary": it.BinaryType, "date": it.DateType,
              "timestamp": it.TimestampType,
              "timestamptz": it.TimestamptzType, "time": it.TimeType,
              "uuid": it.UUIDType}
    if base == "decimal":
        p, s = canon.params(name) or (38, 10)
        return it.DecimalType(p, s)
    if base not in simple:
        raise SystemExit(f"a {name} column has no Iceberg counterpart"
                         " migkit makes")
    return simple[base]()


class DeltaEngine(_Lake):
    """Delta Lake, through delta-rs. The endpoint's `url` (or `path`) is
    the root the databases are directories under: a directory, or an
    `s3://` prefix (the endpoint's `user` and `password` as the access key,
    `region` and `endpoint_url` as for Parquet)."""
    CANON_ENGINE = "delta"

    def _root(self, side):
        ep = self._endpoint(side)
        url = str(ep.options.get("url") or ep.options.get("path")
                  or ep.host or "")
        if not url:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " has no url or path for its Delta tables")
        if "://" in url and not url.startswith("file://"):
            return url.rstrip("/")
        return os.path.abspath(url[7:] if url.startswith("file://") else url)

    @property
    def OVER_NETWORK(self):
        return "://" in self._root("src")

    def _storage(self, side):
        ep = self._endpoint(side)
        opts = ep.options or {}
        out = {}
        if ep.user:
            out.update(AWS_ACCESS_KEY_ID=ep.user,
                       AWS_SECRET_ACCESS_KEY=ep.password or "")
        if opts.get("region"):
            out["AWS_REGION"] = str(opts["region"])
        if opts.get("endpoint_url"):
            out["AWS_ENDPOINT_URL"] = str(opts["endpoint_url"])
            if str(opts["endpoint_url"]).startswith("http://"):
                out["AWS_ALLOW_HTTP"] = "true"
        return out or None

    def _path(self, side, db, table=None):
        return posixpath.join(self._root(side), self._d(side, db),
                              *([table] if table else []))

    def _table(self, side, db, table):
        from deltalake import DeltaTable
        return DeltaTable(self._path(side, db, table),
                          storage_options=self._storage(side))

    def _local(self, side):
        return "://" not in self._root(side)

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        root = self._root("src")
        if not self._local("src"):
            raise SystemExit("name the hop's databases: an S3 root is not"
                             " listed for them")
        return sorted(d for d in os.listdir(root)
                      if os.path.isdir(os.path.join(root, d)))

    def target_missing(self, db):
        return self._local("dst") and not os.path.isdir(self._path("dst",
                                                                   db))

    def side_database(self, side, db):
        """The directory the database's tables are under."""
        return self._path(side, db)

    def server_identity(self, side, db):
        """Where the tables are, and whether their directory is there yet
        (`interlock`); a directory of an S3 root is said to be there."""
        where = self._path(side, db)
        return {"id": os.path.realpath(where) if self._local(side)
                else where,
                "absent": self._local(side) and not os.path.isdir(where)}

    def neutral_tables(self, side, db):
        from deltalake import DeltaTable
        base = self._path(side, db)
        if self._local(side):
            names = (sorted(os.listdir(base)) if os.path.isdir(base) else [])
        else:
            import pyarrow.fs as pafs
            fs, root = pafs.FileSystem.from_uri(base)
            names = sorted(posixpath.basename(i.path) for i in
                           fs.get_file_info(pafs.FileSelector(
                               root, allow_not_found=True))
                           if i.type == pafs.FileType.Directory)
        return [n for n in names
                if DeltaTable.is_deltatable(posixpath.join(base, n),
                                            self._storage(side))
                and not self.hop.excluded(db, n)]

    def _schema(self, side, db, table):
        out = []
        for f in self._table(side, db, table).schema().fields:
            out.append((f.name, getattr(f.type, "type", None) or str(f.type)))
        return out

    def _key_names(self, side, db, table):
        """The columns whose metadata place them in migkit's key, in that
        order."""
        got = []
        for f in self._table(side, db, table).schema().fields:
            at = (f.metadata or {}).get("migkit.key")
            if at is not None:
                got.append((int(at), f.name))
        return [n for _, n in sorted(got)]

    def _arrow_schema(self, side, db, table):
        import pyarrow as pa
        return pa.schema(self._table(side, db, table).schema().to_arrow())

    def _reader(self, side, db, table, names, keys=None):
        import pyarrow.dataset as ds
        dataset = self._table(side, db, table).to_pyarrow_dataset()
        flt = None
        if keys:
            col, values = keys[0]
            flt = ds.field(col).isin(values) if values else None
        return dataset.scanner(columns=list(names), filter=flt).to_reader()

    def table_facts(self, side, db):
        import pyarrow as pa
        out = {}
        for t in self.neutral_tables(side, db):
            adds = pa.table(self._table(side, db, t).get_add_actions(
                flatten=True))
            rows = (sum(v or 0 for v in adds.column("num_records")
                        .to_pylist()) if "num_records" in adds.column_names
                    else None)
            size = (sum(v or 0 for v in adds.column("size_bytes").to_pylist())
                    if "size_bytes" in adds.column_names else None)
            out[t] = {"rows": rows, "bytes": size,
                      "key": bool(self._key_names(side, db, t))}
        return out

    def neutral_create(self, side, db, table, columns, key=()):
        """The table, its key kept in its key columns' metadata: Delta has
        no key of its own."""
        from deltalake import DeltaTable
        from deltalake.schema import Field, PrimitiveType, Schema
        self._target_only(side, "create a table")
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        desc = self.neutral_create_sql(side, db, table, columns, key)
        fields = []
        for name, typ in json.loads(desc)["columns"]:
            meta = ({"migkit.key": str(list(key).index(name))}
                    if name in key else {})
            fields.append(Field(name, PrimitiveType(typ),
                                nullable=name not in key, metadata=meta))
        if self._local(side):
            os.makedirs(self._path(side, db), exist_ok=True)
        DeltaTable.create(self._path(side, db, table), schema=Schema(fields),
                          name=table, storage_options=self._storage(side))
        return desc

    def _last(self, side, db, table):
        got = self._table(side, db, table).history(1)
        return got[0] if got else {}

    def _app(self, db, table):
        """The application id a tail's commits to this table carry."""
        return f"migkit:{self.hop.name}:{db}:{table}"

    def _holds(self, side, db, table, batch, token=None):
        """For a tail's batch, first the protocol's own answer - the
        latest version the log keeps for the hop and table, which delta-rs
        leaves to the writer to compare - then the last commit's position,
        since the same number replayed from another position is another
        attempt. For a copy's batch, the last commit's name for it."""
        if token is not None:
            have = self._table(side, db, table).transaction_version(
                self._app(db, table))
            if have is None or int(have) < int(batch):
                return False
        last = self._last(side, db, table)
        return (last.get(HOP) == self.hop.name and last.get(BATCH) == batch
                and (token is None or last.get(TOKEN) == token))

    @staticmethod
    def _on(key):
        return " and ".join(f"t.`{k}` = s.`{k}`" for k in key)

    def _commit_batch(self, side, db, table, key, data, props):
        from deltalake import CommitProperties, write_deltalake
        commit = CommitProperties(custom_metadata=props)
        if key:
            (self._table(side, db, table)
             .merge(data, predicate=self._on(key), source_alias="s",
                    target_alias="t", commit_properties=commit)
             .when_matched_update_all().when_not_matched_insert_all()
             .execute())
        else:
            write_deltalake(self._path(side, db, table), data, mode="append",
                            commit_properties=commit,
                            storage_options=self._storage(side))

    def _commit_changes(self, side, db, table, rows, props, n):
        """One MERGE: the deleted keys removed, the rest updated or put in,
        the commit carrying the batch as its transaction action's version
        for the hop and the table, and its position in its metadata."""
        import pyarrow as pa
        from deltalake import CommitProperties, Transaction
        key = self._key_names(side, db, table)
        if not key:
            raise SystemExit(f"{table} has no key on the target, and a"
                             " change is applied by its key")
        names, full = self._full_rows(side, db, table, rows)
        gone = [[k.get(c) for c in names] for kind, k, _ in rows
                if kind == "delete"]
        data = self._arrow(side, db, table, names, full + gone)
        data = data.append_column("_migkit_op", pa.array(
            ["upsert"] * len(full) + ["delete"] * len(gone), pa.string()))
        txn = ([Transaction(self._app(db, table), int(n))]
               if n is not None else None)
        cols = {f"`{c}`": f"s.`{c}`" for c in names}
        (self._table(side, db, table)
         .merge(data, predicate=self._on(key), source_alias="s",
                target_alias="t", commit_properties=CommitProperties(
                    custom_metadata=props, app_transactions=txn))
         .when_matched_delete(predicate="s._migkit_op = 'delete'")
         .when_matched_update(updates=cols,
                              predicate="s._migkit_op = 'upsert'")
         .when_not_matched_insert(updates=cols,
                                  predicate="s._migkit_op = 'upsert'")
         .execute())

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        got = self._table(side, db, table).delete(where or None)
        return int((got or {}).get("num_deleted_rows", 0))


# ---- what of each wrapped library is used, and why the rest is not --------
#
# The shape `tooluse.register` takes (backlog 0f), as `warehouse.WRAPPED`.
USED, UNUSED = "used", "unused"

WRAPPED = {
    "pyiceberg": {"licence": "Apache-2.0", "options": {
        ("Table", "upsert"): (
            USED, "a copy's batch merged in by the identifier fields, its"
                  " snapshot naming the batch"),
        ("Table", "append"): (
            USED, "a keyless table's batch, skipped where the last snapshot"
                  " already names it"),
        ("Transaction", "delete and append"): (
            USED, "a tail batch's keys out and its rows in, in one commit"
                  " naming the batch and its position"),
        ("Table", "scan to_arrow_batch_reader"): (
            USED, "rows handed to DuckDB as Arrow for the digest, a key's"
                  " files found by the scan's filter"),
        ("Catalog", "sql and rest"): (
            USED, "a SQLite or other SQL catalog, or a REST catalog, by the"
                  " endpoint's URI"),
        ("Table", "add_files"): (
            UNUSED, "every batch is written by the library, so there are no"
                    " outside files to register"),
        ("Table", "expire_snapshots"): (
            UNUSED, "housekeeping is the operator's, and migkit leaves a"
                    " table's history alone"),
        ("Table", "dynamic_partition_overwrite"): (
            UNUSED, "the tables migkit makes are not partitioned, and it"
                    " merges by the key"),
    }},
    "deltalake": {"licence": "Apache-2.0", "options": {
        ("DeltaTable", "merge"): (
            USED, "a batch merged by the key in one commit whose metadata"
                  " names it"),
        ("CommitProperties", "app_transactions"): (
            USED, "a tail batch's number as the protocol's transaction"
                  " version for the hop and table"),
        ("DeltaTable", "transaction_version"): (
            USED, "asked before a tail batch is committed, which delta-rs"
                  " leaves to the writer"),
        ("write_deltalake", "append"): (
            USED, "a keyless table's batch, skipped where the last commit"
                  " already names it"),
        ("DeltaTable", "to_pyarrow_dataset"): (
            USED, "rows handed to DuckDB lazily, a key's files skipped by"
                  " the dataset's filter"),
        ("DeltaTable", "load_cdf"): (
            UNUSED, "a Delta table as a source's change log is not built"
                    " yet, backlog item 33"),
        ("DeltaTable", "vacuum and optimize"): (
            UNUSED, "housekeeping is the operator's, and migkit leaves the"
                    " files alone"),
    }},
}


def _wrapped():
    from .warehouse import _register
    _register(WRAPPED)


_wrapped()
