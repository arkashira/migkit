"""Files described by COBOL copybooks as one side of a pair (backlog 34,
W5): VSAM and sequential extracts, IMS unloads, the files a mainframe
migration starts from.

A database is a directory and a table is a data file in it with the
copybook that describes its records: `<table>.cpy` beside `<table>.dat`
(and `<table>.json` for the table's options), or the endpoint's `tables`
naming each one:

    tables:
      customers: {file: CUST.DAT, copybook: CUST.cpy, recfm: VB,
                  codepage: cp838, key: [CUST-ID]}
      lines:     {file: ORDERS.DAT, copybook: ORDERS.cpy, record: LINE-REC,
                  segment: {field: REC-TYPE, value: L}}

`recfm` is F (the default), FB, V, VB, or ZOSMF for a file fetched in
z/OSMF's record mode; `lrecl` where a fixed record is longer than its
copybook; `record` the 01 level to read where the copybook has several;
`segment` the records of a multi-layout file that are this table's;
`redefines` which alternative of a REDEFINES a record is read by
(`{DETAIL: {by: ACCT-TYPE, when: {P: PERSONAL, B: BUSINESS}}}`); `odo: max`
where a table's entries are all stored whatever its count; `bytes` fields
of `PIC X` that hold binary; `unreadable: null` to land a value that is not
one as NULL - every one listed by the deep check - where the default stops
at the first. The endpoint's own `codepage`, `recfm` and `unreadable` are
every table's default. Values are read by `migkit.copybook`.

Files keep no order and no key: a table is read in one pass, by ranges of
whole records (`position_spans`), so a copy stopped part way resumes at the
record offset it had reached, and a range is read and folded in processes
of its own, side by side. A declared key is the target's key and what a
drilldown looks rows up by. Written, a table is its records appended in
the table's record format, a batch written again after a restart
replacing itself.
"""
import hashlib
import json
import os
import re

from .base import Engine, NeutralCopier, Result

#: the extensions a data file beside a copybook may have, tried in order
DATA_SUFFIXES = (".dat", ".bin", ".ebc", ".txt", "")
COPYBOOK_SUFFIXES = (".cpy", ".cbl", ".cob", ".copy")

_RANGE = re.compile(r"^\(?\s*record_offset\s*>=\s*(\d+)(?:\s+and\s+"
                    r"record_offset\s*<\s*(\d+))?\s*\)?$")

#: a range of a file below this many bytes is folded in this process.
#: Measured on 10 CPUs, a 16-field record of 74 bytes: 60,000 records
#: (4.4 MB) 0.63 s in one process and 0.36 s in ten; 600,000 (44 MB) 6.84
#: and 2.12 s; 3,000,000 (222 MB) 31.98 and 7.38 s - starting the others
#: costs about 0.3 s, which a few megabytes already pay for
PARALLEL_BYTES = 4 << 20


def byte_range(where):
    """(start, end or None) for the spelling `position_spans` writes; a
    filter that is anything else is SQL, which files take none of."""
    if not where:
        return 0, None
    m = _RANGE.match(str(where).strip())
    if not m:
        raise SystemExit("a row filter is SQL, and files described by a"
                         " copybook take none")
    return int(m.group(1)), int(m.group(2)) if m.group(2) else None


def spelled(start, end):
    return (f"record_offset >= {start} and record_offset < {end}"
            if end is not None else f"record_offset >= {start}")


# ---- one table, as a process of its own reads it ---------------------------

class Described:
    """A table's description made concrete: its file, its layout, its
    options. Built from a plain dict (`spec`), so a process of its own can
    build the same one."""

    def __init__(self, spec):
        from .. import copybook as cb
        self.spec = spec
        self.path = spec["path"]
        self.recfm = str(spec.get("recfm") or "F").upper()
        if self.recfm not in cb.RECFMS:
            raise SystemExit(f"{spec['table']}: recfm {self.recfm} is not"
                             f" one of {', '.join(cb.RECFMS)}")
        try:
            records = cb.records_of(spec["text"])
            want = str(spec.get("record") or "").upper()
            rec = next((r for r in records if (r.name or "").upper() == want),
                       None) if want else records[0]
            if rec is None:
                raise cb.CopybookError(
                    f"no 01 level {want}; the copybook has"
                    f" {', '.join(r.name or 'FILLER' for r in records)}")
            self.layout = cb.Layout(
                rec, spec.get("codepage") or cb.DEFAULT_CODEPAGE,
                spec.get("redefines") or {}, spec.get("odo") or "actual",
                spec.get("bytes") or (), spec.get("keep_padding"))
        except cb.CopybookError as e:
            raise SystemExit(f"{spec['table']}: its copybook cannot be read"
                             f" - {e}")
        self.lrecl = int(spec.get("lrecl") or self.layout.length)
        if self.lrecl < self.layout.length and self.recfm in ("F", "FB"):
            raise SystemExit(f"{spec['table']}: lrecl {self.lrecl} is"
                             f" shorter than its copybook's"
                             f" {self.layout.length} bytes")
        self.null_unreadable = str(spec.get("unreadable") or "stop") \
            .lower() == "null"
        self.segment = None
        seg = spec.get("segment")
        if seg:
            col = self.layout.column_of(seg.get("field", ""))
            leaf = next((lf for lf in self.layout.leaves
                         if lf.column == col), None)
            if leaf is None or leaf.before or leaf.odo:
                raise SystemExit(f"{spec['table']}: its segment field"
                                 f" {seg.get('field')} is not a field at a"
                                 " fixed place in the record")
            self.segment = (leaf, str(seg.get("value")))

    def columns(self):
        return list(self.layout.columns)

    def index_of(self, names):
        at = {c: i for i, (c, _) in enumerate(self.layout.columns)}
        missing = [n for n in names if n not in at]
        if missing:
            raise SystemExit(f"{self.spec['table']} has no column"
                             f" {', '.join(missing)}")
        return [at[n] for n in names]

    def claims(self, rec):
        """Whether a record is this table's, by its segment field."""
        if self.segment is None:
            return True
        from .. import copybook as cb
        leaf, want = self.segment
        try:
            got = leaf.decode(rec[leaf.offset:leaf.offset + leaf.size])
        except cb.Unreadable:
            return False
        return cb._matches(got, want)

    def rows(self, start=0, end=None, picked=None, findings=None):
        """(offset, values) of this table's records whose first byte lies in
        [start, end), the values `picked` (indexes) in that order. A value
        that is not one stops the read, or lands as NULL and is added to
        `findings` where the table says `unreadable: null`."""
        from .. import copybook as cb
        lay = self.layout
        with open(self.path, "rb") as f:
            try:
                for at, rec in cb.records(f, self.recfm, self.lrecl, start,
                                          end):
                    if not self.claims(rec):
                        continue
                    values, bad = lay.decode(rec)
                    if bad:
                        if not self.null_unreadable:
                            raise SystemExit(self._stop(at, bad))
                        if findings is not None:
                            findings += [(at, b) for b in bad]
                    yield at, (values if picked is None
                               else [values[i] for i in picked])
            except cb.Unreadable as e:
                if not self.null_unreadable or findings is None:
                    raise SystemExit(f"{self.spec['table']}: {e.why}")
                findings.append((None, e))

    def count(self):
        """The table's records in its file, by their framing and segment
        alone: nothing decoded."""
        from .. import copybook as cb
        if not os.path.exists(self.path):
            return 0
        with open(self.path, "rb") as f:
            return sum(1 for _, rec in cb.records(f, self.recfm, self.lrecl)
                       if self.claims(rec))

    def _stop(self, at, bad):
        first = bad[0]
        return (f"{self.spec['table']}: the record at byte {at} of"
                f" {os.path.basename(self.path)} holds a value that is not"
                f" one - {first}"
                + (f", and {len(bad) - 1} more in it" if len(bad) > 1 else "")
                + ". The table's option `unreadable: null` lands such values"
                " as NULL and the deep check lists every one")


def fold_range(spec, start, end, names, classes):
    """(rows, fold, findings) over one range of a table's file: what a
    process of its own computes, and this one too for a small range."""
    from .. import canon
    d = Described(spec)
    findings = []
    picked = d.index_of(names)
    n, total = canon.fold_rows(
        classes, (v for _, v in d.rows(start, end, picked, findings)))
    return n, total, [(at, str(b)) for at, b in findings]


class CopybookEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "copybook"
    SQL_DIALECT = None
    FOLDED_BECAUSE = "is files, with no server to fold them in"
    OVER_NETWORK = False
    #: files keep no order to resume by: a table is read by ranges of whole
    #: records instead (`position_spans`)
    RESUMES_BY_KEY = False
    #: one data file per table, appended by one writer
    WRITES_IN_PARALLEL = False

    # ---- where the tables are -------------------------------------------

    def _endpoint(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _root(self, side):
        ep = self._endpoint(side)
        path = str(ep.options.get("path") or ep.host or "")
        if path.startswith("file://"):
            path = path[7:]
        if not path or "://" in path:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `path`, a directory on this machine"
                             " holding the files and their copybooks")
        return os.path.abspath(path)

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _dir(self, side, db):
        return os.path.join(self._root(side), self._d(side, db))

    @staticmethod
    def _find(folder, stem, suffixes):
        try:
            names = {n.lower(): n for n in os.listdir(folder)}
        except OSError:
            return None
        for s in suffixes:
            got = names.get((stem + s).lower())
            if got and os.path.isfile(os.path.join(folder, got)):
                return os.path.join(folder, got)
        return None

    def _descriptions(self, side, db):
        """{table: its description as a dict}, from the endpoint's `tables`
        or the directory's copybooks, each with the endpoint's defaults."""
        ep = self._endpoint(side)
        folder = self._dir(side, db)
        said = {"codepage": ep.options.get("codepage"),
                "recfm": ep.options.get("recfm"),
                "unreadable": ep.options.get("unreadable"),
                "odo": ep.options.get("odo")}
        defaults = {k: v for k, v in said.items() if v}
        out = {}
        named = ep.options.get("tables")
        if named:
            for table, desc in named.items():
                out[str(table)] = {**defaults, **(desc or {})}
            return out
        try:
            entries = sorted(os.listdir(folder))
        except OSError:
            return {}
        for n in entries:
            stem, ext = os.path.splitext(n)
            if ext.lower() not in COPYBOOK_SUFFIXES:
                continue
            desc = dict(defaults, copybook=n)
            sidecar = self._find(folder, stem, (".json",))
            if sidecar:
                with open(sidecar) as f:
                    desc.update(json.load(f))
            if "file" not in desc:
                data = self._find(folder, stem, DATA_SUFFIXES[:-1])
                desc["file"] = os.path.basename(data) if data else stem + \
                    ".dat"
            out[stem.lower().replace("-", "_")] = desc
        return out

    def _spec(self, side, db, table):
        """The picklable description of one table: everything a process of
        its own needs to read it."""
        known = self.__dict__.setdefault("_specs", {})
        at = (side, db, str(table))
        if at in known:
            return known[at]
        desc = self._descriptions(side, db).get(str(table))
        if desc is None:
            raise SystemExit(f"{table} is not described on the"
                             f" {'source' if side == 'src' else 'target'}:"
                             " no copybook for it")
        folder = self._dir(side, db)
        book = os.path.join(folder, desc.get("copybook")
                            or f"{table}.cpy")
        try:
            with open(book, encoding="utf-8", errors="replace") as f:
                text = f.read()
        except OSError:
            raise SystemExit(f"{table}: its copybook {book} cannot be read")
        from .. import copybook as cb

        def member(name):
            got = self._find(os.path.dirname(book), name,
                             COPYBOOK_SUFFIXES + ("",))
            if not got:
                return None
            with open(got, encoding="utf-8", errors="replace") as f:
                return f.read()
        try:
            text = cb.expand_copies(cb.source_text(
                text, desc.get("source_format")), member)
        except cb.CopybookError as e:
            raise SystemExit(f"{table}: its copybook cannot be read - {e}")
        spec = {k: v for k, v in desc.items()
                if k not in ("copybook", "file", "source_format")}
        spec.update(table=str(table), text=text,
                    path=os.path.join(folder, desc.get("file")
                                      or f"{table}.dat"))
        known[at] = spec
        return spec

    def _table(self, side, db, table):
        known = self.__dict__.setdefault("_described", {})
        at = (side, db, str(table))
        if at not in known:
            known[at] = Described(self._spec(side, db, table))
        return known[at]

    # ---- the contract ---------------------------------------------------

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        root = self._root("src")
        return sorted(n for n in os.listdir(root)
                      if os.path.isdir(os.path.join(root, n)))

    def target_missing(self, db):
        return not os.path.isdir(self._dir("dst", db))

    def server_identity(self, side, db):
        """The directory itself, for the interlock on the target: which one
        it is by its device and inode - the same directory reached by two
        paths is one, and a copy onto its own source is refused - where
        it is, and the bytes its data files hold; `absent` before a
        target's is made."""
        folder = self._dir(side, db)
        try:
            st = os.stat(folder)
        except OSError:
            return {"name": folder, "absent": True}
        size = 0
        for t in self.neutral_tables(side, db):
            size += self._size(side, db, t)
        return {"id": f"{st.st_dev}:{st.st_ino}", "name": folder,
                "bytes": size}

    def neutral_tables(self, side, db):
        out = []
        for table in self._descriptions(side, db):
            if self.hop.excluded(db, table):
                continue
            if os.path.isfile(self._spec(side, db, table)["path"]):
                out.append(table)
        return sorted(out)

    def neutral_columns(self, side, db, table):
        return self._table(side, db, table).columns()

    def neutral_key(self, side, db, table):
        d = self._table(side, db, table)
        key = []
        for name in d.spec.get("key") or []:
            col = d.layout.column_of(name)
            if col is None:
                raise SystemExit(f"{table}: its key names {name}, which is"
                                 " not a field of the record")
            key.append(col)
        return key

    def _size(self, side, db, table):
        try:
            return os.path.getsize(self._table(side, db, table).path)
        except OSError:
            return 0

    def table_facts(self, side, db):
        """Bytes from the file; rows where they follow from them - a fixed
        file whose every record is the table's - and otherwise unknown
        until it is read."""
        out = {}
        for t in self.neutral_tables(side, db):
            d = self._table(side, db, t)
            size = self._size(side, db, t)
            rows = (size // d.lrecl if d.recfm in ("F", "FB")
                    and d.segment is None else None)
            out[t] = {"rows": rows, "bytes": size,
                      "key": bool(d.spec.get("key"))}
        return out

    def _starts(self, side, db, table, every, start=0, end=None):
        """Offsets in [start, end) at which ranges of about `every` of the
        table's records begin, the first being `start` itself."""
        from .. import copybook as cb
        d = self._table(side, db, table)
        size = self._size(side, db, table)
        with open(d.path, "rb") as f:
            at = cb.starts(f, d.recfm, d.lrecl, size, max(1, int(every)))
        at = [a for a in at if a >= start and (end is None or a < end)]
        if not at or at[0] != start:
            at = [start] + at
        return at

    def position_spans(self, side, db, table, rows_per):
        """Ranges of whole records, about `rows_per` each, by byte offset:
        what a copy resumes from, since the file has no key. The last is
        open-ended, so records appended after the plan are read too."""
        at = self._starts(side, db, table, rows_per)
        return [[a, spelled(a, b)] for a, b in zip(at, at[1:] + [None])]

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        """The records in the range, a batch at a time, read as they are
        handed on: the copier writes one range while nothing else of the
        file is held."""
        start, end = byte_range(where)
        d = self._table(side, db, table)
        picked = d.index_of([n for n, _ in columns])
        batch = []
        for _, v in d.rows(start, end, picked, []):
            batch.append(v)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """In key order, which files do not keep: the table is read whole
        and sorted, for the few callers that ask for a row or two - the
        copier reads it by ranges instead (`RESUMES_BY_KEY`)."""
        names = [n for n, _ in columns]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        if after is None and limit == 1:
            for batch in self.neutral_batches(side, db, table, columns, 1,
                                              where):
                return batch[:1], None
            return [], None
        rows = [r for b in self.neutral_batches(side, db, table, columns,
                                                5000, where) for r in b]
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
        """A pass over the file, keeping the rows whose key's canonical text
        is one asked for: a file has no index to ask."""
        from .. import canon
        if not key or not keys:
            return {}
        cls = dict(columns)
        want = {tuple(canon.render_value(cls[k], v) for k, v in zip(key, kk))
                for kk in keys}
        found = []
        for batch in self.neutral_batches(side, db, table, columns, 5000,
                                          where):
            found += [r for r in batch
                      if self._key_of(columns, key, r) in want]
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded over the records in the range, as every engine without a
        server folds - in processes of their own, a range each, where the
        range is big enough: the fold is a sum, so the parts add up to the
        whole in any order."""
        start, end = byte_range(where)
        names, classes = [n for n, _ in columns], [c for _, c in columns]
        spec = self._spec(side, db, table)
        parts = self._fold_parts(side, db, table, start, end)
        if len(parts) == 1:
            n, total, _ = fold_range(spec, start, end, names, classes)
            return n, str(total)
        import concurrent.futures as cf
        import multiprocessing as mp
        n = total = 0
        with cf.ProcessPoolExecutor(len(parts), mp_context=mp.get_context(
                "spawn")) as pool:
            for k, t, _ in pool.map(fold_range, *zip(*[
                    (spec, a, b, names, classes) for a, b in parts])):
                n += k
                total += t
        return n, str(total)

    def _fold_parts(self, side, db, table, start, end):
        """[(start, end)] a range is folded by: one where it is small, else
        one per CPU this machine gives, each beginning on a record."""
        from .. import sizing
        size = self._size(side, db, table)
        stop = size if end is None else min(end, size)
        span = stop - start
        cpus = sizing.host()["cpus"] or 1
        if span < PARALLEL_BYTES or cpus < 2:
            return [(start, end)]
        d = self._table(side, db, table)
        per = max(1, span // cpus // max(1, d.lrecl if d.recfm in ("F", "FB")
                                         else 64))
        at = self._starts(side, db, table, per, start, end)
        return list(zip(at, at[1:] + [end]))

    # ---- writing --------------------------------------------------------

    def _index_path(self, path):
        return path + ".migkit.json"

    def neutral_create_sql(self, side, db, table, columns, key=()):
        spec = self._spec(side, db, table)
        return json.dumps({"file": os.path.basename(spec["path"]),
                           "recfm": spec.get("recfm") or "F",
                           "columns": [c[0] for c in columns]})

    def neutral_create(self, side, db, table, columns, key=()):
        """The target's copybook says what its records are; making the table
        is making its empty file - once every column arriving is a field
        of that copybook. A file already there is never replaced."""
        self._target_only(side, "create a table")
        d = self._table(side, db, table)
        have = {c for c, _ in d.columns()}
        extra = [c[0] for c in columns if c[0] not in have]
        if extra:
            raise SystemExit(f"{table}: the target's copybook has no field"
                             f" for {', '.join(extra)}")
        if os.path.exists(d.path):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        os.makedirs(os.path.dirname(d.path), exist_ok=True)
        open(d.path, "wb").close()
        return self.neutral_create_sql(side, db, table, columns, key)

    def neutral_write(self, side, db, table, columns, rows):
        """Records appended in the table's format. A table with a key names
        each batch by the first and last keys it holds and its size, and
        keeps the byte range it took beside the file: written again after a
        restart, it replaces itself and whatever followed it, as a keyed
        batch replaces itself elsewhere. A table with no key is copied from
        empty, by ranges the copier accounts for itself.

        The batch's entry is written before its records, on the disk, and
        finished once they are (audit ST6): written after them, a crash
        between left no entry - a torn index read as none - and the batch
        was appended again after itself, its records twice. An entry left
        unfinished is a batch whose write never returned: its copy was not
        counted, it is written again, and whatever it left after its start
        is cut first."""
        from .. import copybook as cb
        self._target_only(side, "write rows")
        if not rows:
            return 0
        d = self._table(side, db, table)
        cols = [c for c, _ in d.columns()]
        at = d.index_of([n for n, _ in columns])
        derived = {i for i, (_, t) in enumerate(d.columns())
                   if t == "condition"}
        out = []
        for r in rows:
            values = [None] * len(cols)
            for i, v in zip(at, r):
                if i not in derived:
                    values[i] = v
            try:
                out.append(cb.frame(d.recfm, d.layout.encode(values),
                                    d.lrecl))
            except (cb.Unreadable, cb.CopybookError) as e:
                raise SystemExit(f"{table}: a row cannot be written into its"
                                 f" record - {e}")
        names = [n for n, _ in columns]
        key = [names.index(k) for k in self.neutral_key(side, db, table)
               if k in names]
        batch = hashlib.sha1(repr([[rows[0][i] for i in key],
                                   [rows[-1][i] for i in key], len(rows)])
                             .encode()).hexdigest()[:16] if key else None
        from .. import durable
        index_path = self._index_path(d.path)
        index = durable.read_state(index_path, "position") or []
        with open(d.path, "r+b") as f:
            f.seek(0, 2)
            end = f.tell()
            if index and index[-1][2] is None:
                # a batch whose write never finished: cut back to where it
                # began, whichever batch comes now
                f.truncate(index[-1][1])
                end = index[-1][1]
                index = index[:-1]
            for k, (name, begin, _) in enumerate(index):
                if batch and name == batch:
                    f.truncate(begin)
                    end = begin
                    index = index[:k]
                    break
            if batch:
                # where the batch begins, on the disk before its records
                durable.write_json(index_path, index + [[batch, end, None]])
            f.seek(end)
            f.write(b"".join(out))
            f.flush()
            os.fsync(f.fileno())
            stop = f.tell()
        if batch:
            durable.write_json(index_path, index + [[batch, end, stop]])
        return len(rows)

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        byte_range(where)
        if where:
            raise SystemExit(f"{table}: a range of a file is not emptied on"
                             " its own")
        d = self._table(side, db, table)
        if not os.path.exists(d.path):
            return 0
        gone = d.count()
        with open(d.path, "r+b") as f:
            f.truncate(0)
        if os.path.exists(self._index_path(d.path)):
            os.remove(self._index_path(d.path))
        return gone

    # ---- the checks, through the pair every engine compares by ---------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_deep(self, db):
        return self.deep_files("src", db) + self.deep_files("dst", db)

    def deep_files(self, side, db):
        """Every record of every table read whole: a value that is not one
        (each listed in the report directory), a file ending inside a
        record, records of a multi-layout file no table claims, and text
        that decodes to control characters - a wrong code page, or binary
        in a `PIC X` field that `bytes` should name. What a pair with this
        engine on a side asks of it too (`HeteroEngine.check_deep`)."""
        who = "source" if side == "src" else "target"
        try:
            tables = self.neutral_tables(side, db)
        except SystemExit as e:
            return [Result("deep", f"{db} {who} files", "error", str(e))]
        return self._deep(side, who, db, tables)

    def _deep(self, side, who, db, tables):
        from .. import copybook as cb
        problems, warnings, files = [], [], {}
        listed = []
        for t in tables:
            try:
                d = self._table(side, db, t)
            except SystemExit as e:
                problems.append(f"{t}: {e}")
                continue
            files.setdefault(d.path, []).append((t, d))
        for path, owners in files.items():
            name = os.path.basename(path)
            first = owners[0][1]
            unclaimed = 0
            bad = {t: [] for t, _ in owners}
            control = {t: 0 for t, _ in owners}
            try:
                with open(path, "rb") as f:
                    for at, rec in cb.records(f, first.recfm, first.lrecl):
                        mine = [(t, d) for t, d in owners if d.claims(rec)]
                        if not mine:
                            unclaimed += 1
                        for t, d in mine:
                            values, found = d.layout.decode(rec)
                            bad[t] += [(at, b) for b in found]
                            control[t] += sum(
                                1 for v, c in zip(values, d.layout.classes)
                                if c == "text" and isinstance(v, str)
                                and _controls(v))
            except cb.Unreadable as e:
                problems.append(f"{name}: {e.why}")
            for t, found in bad.items():
                if found:
                    problems.append(f"{t}: {len(found):,} values that are not"
                                    f" one - first at byte {found[0][0]}:"
                                    f" {found[0][1]}")
                    listed.append((t, found))
                if control[t]:
                    warnings.append(f"{t}: {control[t]:,} text values hold"
                                    " control characters - a wrong code"
                                    " page, or binary data in a character"
                                    " field (name it under `bytes`)")
            if unclaimed and any(d.segment for _, d in owners):
                warnings.append(f"{name}: {unclaimed:,} records no table's"
                                " segment claims, so none of them is moved")
        for t, found in listed:
            (self.hop.report_dir(db) / f"unreadable-{who}-{t}.txt").write_text(
                "".join(f"{at}\t{b.field}\t{b.raw.hex().upper()}\t{b.why}\n"
                        for at, b in found))
        scope = f"{db} {who} files"
        if problems:
            return [Result("deep", scope, "diff", "; ".join(problems[:6])
                           + ("; " + "; ".join(warnings[:3])
                              if warnings else ""), "",
                           "fix the file or its description; each value is"
                           " listed in the report directory's"
                           f" unreadable-{who}-<table>.txt")]
        if warnings:
            return [Result("deep", scope, "warn", "; ".join(warnings[:6]))]
        return [Result("deep", scope, "ok",
                       f"{len(tables)} tables, every record of every file"
                       " read whole")]

    # ---- files copied as they are ------------------------------------------

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table's data file copied as it is, byte for byte, where the
        target describes the table with the same record format and code
        page: nothing decoded and written again."""
        import shutil
        steps = [f"{t}: its file copied as it is" for t in tables]
        if not go:
            return steps
        for t in tables:
            s, d = self._table("src", db, t), self._table("dst", db, t)
            if (s.recfm, s.layout.codepage, s.lrecl) != \
                    (d.recfm, d.layout.codepage, d.lrecl) or \
                    s.layout.columns != d.layout.columns:
                raise SystemExit(f"{t}: the target describes it differently"
                                 " from the source, so its file cannot be"
                                 " copied as it is")
            os.makedirs(os.path.dirname(d.path), exist_ok=True)
            shutil.copyfile(s.path, d.path)
            if os.path.exists(self._index_path(d.path)):
                os.remove(self._index_path(d.path))
            if log:
                log(f"{t}: {os.path.getsize(d.path):,} bytes copied")
        return steps

    def snapshot_state(self, db, state_dir, kind="all"):
        """The target's data files as they stand, copied beside the run's
        state, where a rollback puts them back from, and listed with their
        sizes."""
        import shutil
        listed = {}
        into = state_dir / "dst-files"
        into.mkdir(parents=True, exist_ok=True)
        for t in self.neutral_tables("dst", db):
            d = self._table("dst", db, t)
            shutil.copyfile(d.path, into / os.path.basename(d.path))
            listed[t] = {os.path.basename(d.path): os.path.getsize(d.path)}
        (state_dir / "dst-files.json").write_text(
            json.dumps(listed, indent=2, sort_keys=True))


def _controls(text):
    return any(ord(ch) < 0x20 or 0x7F <= ord(ch) < 0xA0 for ch in text)
