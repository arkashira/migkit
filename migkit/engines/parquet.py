"""Parquet files, on a disk or under an S3 prefix, as one side of a pair
(backlog 34).

A database is a directory and a table is a directory under it: the part
files, and `_table.json` beside them saying what the table's columns are
and which of them key it. Files carry no key and no order, so the
description is where the key lives. A table read as a source is read in
one pass, a batch at a time: the files keep no order to resume by.

Values are read and written through Arrow, and compared through the same
in-process renderer every engine without a server uses. A decimal whose
source declared no precision is kept as its text, since no fixed scale
reproduces what an unconstrained numeric holds. A decimal with one is kept
as an Arrow decimal, which the tools that read Parquet expect.
"""
import hashlib
import json
import os
import posixpath

from .base import Engine, NeutralCopier, Result

#: the description of a table, beside its part files
META = "_table.json"


def _arrow(cls, numbers):
    """(Arrow type, the name `_table.json` gives it) for a neutral class."""
    import pyarrow as pa
    if cls == "integer":
        return pa.int64(), "int64"
    if cls == "decimal":
        if len(numbers) == 2 and 0 < numbers[0] <= 38:
            p, s = numbers
            return pa.decimal128(p, s), f"decimal128({p}, {s})"
        if len(numbers) == 2 and 0 < numbers[0] <= 76:
            p, s = numbers
            return pa.decimal256(p, s), f"decimal256({p}, {s})"
        return pa.string(), "decimal_text"
    if cls == "float":
        return pa.float64(), "double"
    if cls == "boolean":
        return pa.bool_(), "bool"
    if cls == "text":
        return pa.string(), "string"
    if cls == "bytes":
        return pa.binary(), "binary"
    if cls == "date":
        return pa.date32(), "date32[day]"
    if cls == "timestamp":
        return pa.timestamp("us"), "timestamp[us]"
    if cls == "time":
        return pa.time64("us"), "time64[us]"
    raise SystemExit(f"a {cls} column has no Parquet counterpart migkit"
                     " writes; leave the column out with mapping.columns")


def _declared_arrow(declared):
    """The Arrow type a `_table.json` name stands for."""
    import pyarrow as pa
    from .. import canon
    base = declared.split("(")[0].split("[")[0]
    nums = canon.params(declared)
    return {"int64": pa.int64(), "double": pa.float64(), "bool": pa.bool_(),
            "string": pa.string(), "binary": pa.binary(),
            "decimal_text": pa.string(), "date32": pa.date32(),
            "timestamp": pa.timestamp("us"), "time64": pa.time64("us"),
            "decimal128": pa.decimal128(*nums) if nums else None,
            "decimal256": pa.decimal256(*nums) if nums else None}[base]


class ParquetEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "parquet"
    SQL_DIALECT = None
    FOLDED_BECAUSE = "is files, with no server to fold them in"
    #: a key-ordered read would sort the whole table each time: one pass
    RESUMES_BY_KEY = False

    # ---- where the files are ----------------------------------------------

    def _place(self, side):
        """(filesystem, root) for a side: `url: s3://bucket/prefix` (the
        endpoint's `user` and `password` as the access key, or the
        machine's own AWS credentials; `endpoint_url` for S3-compatible
        storage), or a directory as `path`, `url` or `host`."""
        import pyarrow.fs as pafs
        ep = self.hop.source if side == "src" else self.hop.target
        url = str(ep.options.get("url") or ep.options.get("path")
                  or ep.host or "")
        if url.startswith("s3://"):
            kw = {}
            if ep.user:
                kw.update(access_key=ep.user, secret_key=ep.password)
            end = str(ep.options.get("endpoint_url") or "")
            if end:
                scheme, _, rest = end.partition("://")
                kw.update(endpoint_override=rest or scheme,
                          scheme=scheme if rest else "https")
            if ep.options.get("region"):
                kw["region"] = ep.options["region"]
            return pafs.S3FileSystem(**kw), url[5:].rstrip("/")
        path = url[7:] if url.startswith("file://") else url
        if not path:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " has no url or path for its Parquet files")
        return pafs.LocalFileSystem(), os.path.abspath(path)

    @property
    def OVER_NETWORK(self):
        ep = self.hop.source
        return str(ep.options.get("url") or ep.host or "").startswith("s3://")

    def _dir(self, side, db, table=None):
        fs, root = self._place(side)
        parts = [root, self._d(side, db)] + ([table] if table else [])
        return fs, posixpath.join(*parts)

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _listing(self, fs, path):
        import pyarrow.fs as pafs
        try:
            return fs.get_file_info(pafs.FileSelector(path,
                                                      allow_not_found=True))
        except (FileNotFoundError, OSError):
            return []

    def _meta(self, side, db, table):
        fs, d = self._dir(side, db, table)
        try:
            with fs.open_input_stream(posixpath.join(d, META)) as f:
                return json.loads(f.read())
        except (FileNotFoundError, OSError):
            return None

    def _parts(self, side, db, table):
        import pyarrow.fs as pafs
        fs, d = self._dir(side, db, table)
        return fs, sorted(i.path for i in self._listing(fs, d)
                          if i.type == pafs.FileType.File
                          and i.path.endswith(".parquet"))

    # ---- the contract ---------------------------------------------------

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        import pyarrow.fs as pafs
        fs, root = self._place("src")
        return sorted(posixpath.basename(i.path)
                      for i in self._listing(fs, root)
                      if i.type == pafs.FileType.Directory)

    def target_missing(self, db):
        import pyarrow.fs as pafs
        fs, d = self._dir("dst", db)
        return fs.get_file_info(d).type == pafs.FileType.NotFound

    def neutral_tables(self, side, db):
        import pyarrow.fs as pafs
        fs, d = self._dir(side, db)
        out = []
        for i in self._listing(fs, d):
            name = posixpath.basename(i.path)
            if i.type == pafs.FileType.Directory and \
                    self._meta(side, db, name) is not None and \
                    not self.hop.excluded(db, name):
                out.append(name)
        return sorted(out)

    def neutral_columns(self, side, db, table):
        meta = self._meta(side, db, table) or {}
        return [tuple(c) for c in meta.get("columns", [])]

    def neutral_key(self, side, db, table):
        return list((self._meta(side, db, table) or {}).get("key", []))

    def _batches(self, side, db, table, names, size=5000, where=None):
        if where:
            raise SystemExit(f"{table} moves under a row filter, which is"
                             " SQL, and Parquet files take none")
        import pyarrow.parquet as pq
        fs, parts = self._parts(side, db, table)
        for p in parts:
            with fs.open_input_file(p) as f:
                for batch in pq.ParquetFile(f).iter_batches(
                        batch_size=size, columns=list(names)):
                    cols = [batch.column(n).to_pylist() for n in names]
                    yield [list(r) for r in zip(*cols)] if cols else []

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        yield from self._batches(side, db, table, [n for n, _ in columns],
                                 size, where)

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """In key order, which files do not keep: the whole table is read
        and sorted. The copier reads a Parquet table in one pass instead
        (`RESUMES_BY_KEY`); this serves the few callers that ask for a
        row or two, and the first row of a table asked for alone does not
        sort anything."""
        names = [n for n, _ in columns]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        if after is None and limit == 1:
            for batch in self._batches(side, db, table, names, 1, where):
                if batch:
                    return batch[:1], None
            return [], None
        rows = [r for b in self._batches(side, db, table, names, 5000, where)
                for r in b]
        if not key:
            return rows, None
        at = [names.index(k) for k in key]
        rows.sort(key=lambda r: tuple(r[i] for i in at))
        if after is not None:
            rows = [r for r in rows if tuple(r[i] for i in at) > tuple(after)]
        rows = rows[:limit] if limit else rows
        return rows, (tuple(rows[-1][i] for i in at) if rows else None)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        """A pass over the table, keeping the rows whose key's canonical
        text is one asked for: the files have no index to ask."""
        from .. import canon
        if not key or not keys:
            return {}
        cls = dict(columns)
        want = {tuple(canon.render_value(cls[k], v) for k, v in zip(key, kk))
                for kk in keys}
        names = [n for n, _ in columns]
        found = []
        for batch in self._batches(side, db, table, names, 5000, where):
            for row in batch:
                if self._key_of(columns, key, row) in want:
                    found.append(row)
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon, rowtext
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self._batches(side, db, table,
                                   [name for name, _ in columns], 5000,
                                   where):
            for row in batch:
                total = canon.digest_step(total, rowtext.encode(
                    [canon.render_value(c, v) for c, v in zip(classes, row)]))
                n += 1
        return (n, str(total))

    def table_facts(self, side, db):
        """Rows and bytes from the files' own footers, without reading a
        row."""
        import pyarrow.parquet as pq
        out = {}
        for t in self.neutral_tables(side, db):
            fs, parts = self._parts(side, db, t)
            rows = size = 0
            for p in parts:
                with fs.open_input_file(p) as f:
                    rows += pq.ParquetFile(f).metadata.num_rows
                size += fs.get_file_info(p).size or 0
            out[t] = {"rows": rows, "bytes": size,
                      "key": bool(self.neutral_key(side, db, t))}
        return out

    # ---- writing --------------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        cols = [[col[0], _arrow(col[1], tuple(col[2] or ()))[1]]
                for col in columns]
        return json.dumps({"columns": cols, "key": list(key)})

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a table")
        if self._meta(side, db, table) is not None:
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        desc = self.neutral_create_sql(side, db, table, columns, key)
        fs, d = self._dir(side, db, table)
        fs.create_dir(d, recursive=True)
        with fs.open_output_stream(posixpath.join(d, META)) as f:
            f.write(desc.encode())
        return desc

    def neutral_write(self, side, db, table, columns, rows):
        """One part file per batch. A keyed table names the part by the
        first and last key it holds, so a batch written again after a
        restart replaces itself; a table with no key is copied in one pass
        from empty, and numbers its parts."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        self._target_only(side, "write rows")
        if not rows:
            return 0
        meta = self._meta(side, db, table)
        if meta is None:
            raise SystemExit(f"{table} is not described on the target yet")
        declared = dict(tuple(c) for c in meta["columns"])
        names = [n for n, _ in columns]
        schema = pa.schema([(n, _declared_arrow(declared[n])) for n in names])
        data = [[r[i] for r in rows] for i in range(len(names))]
        for i, n in enumerate(names):
            if declared[n] == "decimal_text":
                data[i] = [None if v is None else format(v, "f")
                           if not isinstance(v, str) else v for v in data[i]]
        fs, d = self._dir(side, db, table)
        key = [names.index(k) for k in meta.get("key", []) if k in names]
        if key:
            ends = repr([[rows[0][i] for i in key], [rows[-1][i] for i in key],
                         len(rows)])
            name = f"part-{hashlib.sha1(ends.encode()).hexdigest()[:16]}"
        else:
            _, have = self._parts(side, db, table)
            name = f"part-{len(have):08d}"
        pq.write_table(pa.table(data, schema=schema),
                       posixpath.join(d, name + ".parquet"), filesystem=fs)
        return len(rows)

    def neutral_empty(self, side, db, table, where=None):
        import pyarrow.parquet as pq
        self._target_only(side, "empty a table")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and Parquet"
                             " files take none")
        fs, parts = self._parts(side, db, table)
        gone = 0
        for p in parts:
            with fs.open_input_file(p) as f:
                gone += pq.ParquetFile(f).metadata.num_rows
            fs.delete_file(p)
        return gone

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_deep(self, db):
        """Every part file opens and holds the rows its footer says. A copy
        cut off while uploading leaves a file that does not."""
        import pyarrow.parquet as pq
        out = []
        for side, who in (("src", "source"), ("dst", "target")):
            bad, files = [], 0
            try:
                tables = self.neutral_tables(side, db)
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(Result("deep", f"{db} {who} files", "error",
                                  f"could not list the {who}'s files: {e}"))
                continue
            for t in tables:
                fs, parts = self._parts(side, db, t)
                for p in parts:
                    files += 1
                    try:
                        with fs.open_input_file(p) as f:
                            pf = pq.ParquetFile(f)
                            counted = sum(pf.read_row_group(g).num_rows
                                          for g in range(pf.num_row_groups))
                            if counted != pf.metadata.num_rows:
                                bad.append(posixpath.basename(p))
                    except Exception:  # noqa: BLE001 - a file that fails
                        bad.append(f"{t}/{posixpath.basename(p)}")
            if bad:
                out.append(Result("deep", f"{db} {who} files", "diff",
                                  f"{len(bad)} of {files} part files do not"
                                  f" read whole: {', '.join(bad[:5])}", "",
                                  "copy those tables again"))
            else:
                out.append(Result("deep", f"{db} {who} files", "ok",
                                  f"{files} part files, every one read"
                                  " whole"))
        return out
