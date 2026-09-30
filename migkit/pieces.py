"""Large values: one way for every engine to size, read, write and hash
them (backlog P0-PARITY; R10 and R22 did it for PostgreSQL, MySQL and
Oracle).

A value is large when one row of it is more than a batch should hold. The
same five things answer it on every engine, and only a few expressions of
a column differ between them - the engine's `LARGE_VALUES` (`Way`):

* sized where it lies - each row's bytes as its server keeps them, read
  without reading a value (`Engine.row_sizes`, `sql_row_sizes`), so what
  one worker holds is known before anything is read, and a move that could
  not hold its widest row is refused before it starts (`model.too_wide`);
* batches cut by bytes, not by rows (`by_bytes`, `fetch_until`,
  `batches`): a read ends at the row that brings it to `BATCH_BYTES`, a
  batch of a one-pass read closes before the row that would take it past
  them - a count of rows read ten thousand of whatever they held;
* read in pieces where a driver would take a value in one fetch
  (`Sql.piece`, `ReadPlan`), each piece `PIECE` bytes by the row's key;
* written in pieces where a driver would send a value as one statement's
  text (`Sql.append`, `written_in_pieces`): inserted with its first piece,
  then added to a piece a statement, in the batch's own transaction;
* hashed where it lies - a value wider than `canon.WIDE` goes into a
  digest as its length and md5, computed by the server where it has md5
  (`Sql.md5`, `DigestPlan`, `Hashed`), and here as it passes where not.

An engine whose platform bounds one row small (DynamoDB's 400 KB item,
Cassandra's mutation, Redshift's 16 MB value) declares that bound as its
reason in `decide.WAYS`; its rows are still read in batches cut by bytes
wherever the copier reads them.
"""
import hashlib
import threading

#: a value past this many bytes is read and written a piece at a time
PIECE = 8 * 2 ** 20
#: what one batch of a copy holds, or one row where one row is more
BATCH_BYTES = 64 * 2 ** 20
#: what a sizing reads at most of one table (`sample_sizes`), and at once
SAMPLE_BYTES = 1 << 30
SAMPLE_STEP_BYTES = 16 * 2 ** 20
#: a collection whose documents take no more than this is sized over every
#: one (MongoDB's `row_sizes`, which reads each to size it); above it, from
#: a sample
EXACT_UP_TO = 256 * 2 ** 20
#: a table of no more rows than this is sized over every row
#: (`sql_row_sizes`): the server reads a large value's length off its
#: root, not the value, so what a sizing costs is its rows'; above it,
#: from a sample of its pages where the engine takes one
EXACT_ROWS = 1_000_000
#: what one worker of the copier every pair shares holds (`Engine.
#: COPY_MEMORY`, the pair's and every engine's that copies through it),
#: counted from the code, not yet measured: the batch being written, the
#: next read beside it, the target's rows read back by key and a value's
#: second copy while its pieces are joined - four times the larger of a
#: batch and the widest row. `CAP_CALIBRATE=1` on
#: `tests/test_large_values_on_every_engine.py` measures it
PAIR_MEMORY = {"own": 64 * 2 ** 20, "per_row": 4.0, "batch": BATCH_BYTES}
#: rows one read of that copier asks for at most (`HeteroEngine.READ_ROWS`)
PAIR_ROWS = 50000


def row_bytes(row):
    """Bytes one row of values takes as carried: its strings (as UTF-8) and
    bytes by length, a NULL as one, anything else as eight, and one for the
    row. A long string is measured a piece at a time: encoded whole, a
    string of 100 MB was a second copy of it for each row measured."""
    n = 0
    for v in row:
        if isinstance(v, (bytes, bytearray, memoryview)):
            n += len(v)
        elif isinstance(v, str):
            n += _utf8_length(v)
        elif v is None:
            n += 1
        else:
            n += 8
    return n + 1


def _utf8_length(text, step=1 << 20):
    if text.isascii():
        return len(text)
    if len(text) <= step:
        return len(text.encode("utf-8", "surrogatepass"))
    return sum(len(text[at:at + step].encode("utf-8", "surrogatepass"))
               for at in range(0, len(text), step))


# --- batches cut by bytes --------------------------------------------------

def by_bytes(rows, most_bytes=None, most_rows=None, size=None):
    """`rows` in batches, each closed before the row that would take it
    past `most_bytes` (`BATCH_BYTES`) or `most_rows` - a row wider than
    `most_bytes` is a batch alone. Lazy: a row is held only until its
    batch is handed on."""
    size = size or row_bytes
    return _cut(((r, size(r)) for r in rows), most_bytes, most_rows)


def _cut(sized, most_bytes=None, most_rows=None):
    """`by_bytes` over (row, its bytes): each row measured once."""
    most_bytes = BATCH_BYTES if most_bytes is None else most_bytes
    batch, held = [], 0
    for r, n in sized:
        if batch and (held + n > most_bytes
                      or (most_rows and len(batch) >= most_rows)):
            yield batch
            batch, held = [], 0
        batch.append(r)
        held += n
    if batch:
        yield batch


def rows_for(largest, most_rows, most_bytes=None):
    """Rows one read may ask for when the widest of them is `largest`
    bytes: as many as `most_bytes` holds at that width, `most_rows` at the
    most, one at the least; `most_rows` where the width is not known."""
    most_rows = max(1, int(most_rows))
    most_bytes = BATCH_BYTES if most_bytes is None else most_bytes
    if not largest:
        return most_rows
    return max(1, min(most_rows, int(most_bytes) // max(int(largest), 1)))


def _fetched(cur, size, most_bytes, most_step=1024):
    """(row, its bytes) of a DB-API cursor's statement one by one, fetched
    a few at a time: one first, then as many as a quarter of `most_bytes`
    holds at the width of the last fetch's widest row - a table of wide
    rows never has many of them fetched at once, and one of narrow rows is
    fetched a thousand at a time after its first."""
    step = 1
    while True:
        got = cur.fetchmany(step)
        if not got:
            return
        widest = 1
        for r in got:
            n = size(r)
            widest = max(widest, n)
            yield r, n
        step = max(1, min(int(most_step), (most_bytes // 4) // widest))


def fetch_until(cur, size=None, most_bytes=None):
    """The rows of a cursor's statement, in order, up to the row that
    brings them to `most_bytes`: a read by key that ends there goes on
    from its last row, so a row not taken is read by the next."""
    size = size or row_bytes
    most_bytes = BATCH_BYTES if most_bytes is None else most_bytes
    out, held = [], 0
    for r, n in _fetched(cur, size, most_bytes):
        out.append(r)
        held += n
        if held >= most_bytes:
            break
    return out


def batches(cur, most_rows, size=None, most_bytes=None):
    """Every row of a cursor's statement, in batches of `most_rows` rows or
    `most_bytes` (`by_bytes`) - a one-pass read that holds a batch at a
    time."""
    most_bytes = BATCH_BYTES if most_bytes is None else most_bytes
    yield from _cut(_fetched(cur, size or row_bytes, most_bytes),
                    most_bytes, most_rows)


def widest(eng, side, db, table):
    """The widest row of one table as its engine measures it
    (`Engine.row_sizes`), asked once a run; None where it cannot say -
    what a reader that fetches a count of rows at once sizes the count by
    (`rows_for`)."""
    known = eng.__dict__.setdefault("_widest_of", {})
    at = (side, db, str(table))
    if at not in known:
        # asked again by the reads that measure it: not known yet
        known[at] = None
        try:
            got = (eng.row_sizes(side, db, [table]) or {}).get(table) or {}
        except Exception:  # noqa: BLE001 - a count of rows, then
            got = {}
        known[at] = int(got.get("largest") or 0) or None
    return known[at]


def sample_sizes(eng, side, db, table, columns, most_rows,
                 most_bytes=None):
    """(row sizes, whether they are every row) of the first `most_rows`
    rows of a table, read a few at a time - one first, then as many as
    `SAMPLE_STEP_BYTES` holds at the widest seen - and no more than
    `most_bytes` in all: only the sizes are kept. Where the engine resumes
    by a key its read goes on from each step's last row; elsewhere its
    one-pass batches are read, `most_rows` or the bytes ending it."""
    most_bytes = SAMPLE_BYTES if most_bytes is None else most_bytes
    lengths, most, held = [], 0, 0
    key = (eng.neutral_key(side, db, table)
           if getattr(eng, "RESUMES_BY_KEY", True) else [])
    names = {n for n, _ in columns}
    if key and set(key) <= names:
        after = None
        while len(lengths) < most_rows and held < most_bytes:
            step = (rows_for(most, most_rows - len(lengths),
                             SAMPLE_STEP_BYTES) if most else 1)
            rows, after = eng.neutral_read(side, db, table, columns, after,
                                           step)
            for r in rows:
                n = row_bytes(r)
                lengths.append(n)
                most, held = max(most, n), held + n
            if not rows or after is None:
                return lengths, True
        return lengths, False
    for batch in eng.neutral_batches(side, db, table, columns, 16):
        for r in batch:
            n = row_bytes(r)
            lengths.append(n)
            held += n
        if len(lengths) >= most_rows or held >= most_bytes:
            return lengths, False
    return lengths, True


# --- a value read in pieces ------------------------------------------------

def pieces_of(fetch, size, piece=None):
    """A value of `size` bytes read `piece` at a time: `fetch(at, n)` gives
    the part from `at` (1-based, as SQL counts) of `n` bytes. Ends early
    where a part comes back empty - the value is shorter than it was when
    its length was read, which the caller reads again."""
    piece = PIECE if piece is None else piece
    at = 1
    while at <= size:
        part = fetch(at, min(piece, size - at + 1))
        if not part:
            return
        yield bytes(part)
        at += len(part)


# --- a value hashed where it lies ------------------------------------------

class Hashed:
    """A value wider than `canon.WIDE` hashed where it lies, standing in
    for it in a digest: its text there is `canon.WIDE_MARK`, its length
    and its md5 - what `canon.digest_value` writes for the value itself.
    Only a digest's fold is ever handed one (`DigestPlan`); it is never a
    value written anywhere."""

    __slots__ = ("length", "md5")

    def __init__(self, length, md5):
        if isinstance(md5, (bytes, bytearray)):
            # a driver that hands every string over as its bytes
            md5 = bytes(md5).decode("ascii")
        self.length, self.md5 = int(length), str(md5).strip().lower()

    def text(self):
        from .canon import WIDE_MARK
        return f"{WIDE_MARK}{self.length}:{self.md5}"

    def __eq__(self, other):
        return isinstance(other, Hashed) and (
            self.length, self.md5) == (other.length, other.md5)

    def __hash__(self):
        return hash((self.length, self.md5))

    def __repr__(self):
        return f"Hashed({self.length}, {self.md5!r})"


def hashed_pieces(parts):
    """`Hashed` of a value given as its pieces, hashed as they pass: the
    value is never held whole."""
    h, n = hashlib.md5(usedforsecurity=False), 0
    for p in parts:
        h.update(p)
        n += len(p)
    return Hashed(n, h.hexdigest())


_HASHING = threading.local()


class hashing:
    """While in it, this thread's reads of `eng` hand a wide value its
    server hashes as `Hashed` instead of the value (`DigestPlan`): a
    digest folded here asks for that, a copy on another thread of the same
    engine never sees it."""

    def __init__(self, eng):
        self.eng = eng

    def __enter__(self):
        on = getattr(_HASHING, "engines", None)
        if on is None:
            on = _HASHING.engines = set()
        self.was = id(self.eng) in on
        on.add(id(self.eng))
        return self

    def __exit__(self, *exc):
        if not self.was:
            _HASHING.engines.discard(id(self.eng))


def is_hashing(eng):
    return id(eng) in (getattr(_HASHING, "engines", None) or ())


# --- what an engine does with them -----------------------------------------

def _base(declared):
    return str(declared or "").split("(")[0].strip().lower()


def _whole(declared):
    return "".join(str(declared or "").split()).lower()


class Way:
    """What one engine does with its large values (`Engine.LARGE_VALUES`),
    said in words for the plan and the report."""

    def __init__(self, said):
        self.said = said

    def __repr__(self):
        return f"{type(self).__name__}({self.said!r})"


class Sql(Way):
    """A SQL engine's large values as a few expressions of a quoted column
    `{c}`:

    * `length` - the value's stored bytes, which the server reads without
      reading the value: one expression, or one a declared type (its name
      in small letters, before any parenthesis) where they differ;
    * `types` - the declared types whose values can be large, by that
      name; a row's other columns count as eight bytes when it is sized;
    * `piece` - the part of a byte value from `{at}` (1-based) of `{n}`
      bytes, where a value past `PIECE` is read in pieces (`ReadPlan`);
    * `append` - the set clause adding a piece `{v}` to a value on the
      target, for the declared types in `appends` (written without
      spaces), where a value past `PIECE` is written in pieces;
    * `md5` - the value's md5 in hex, over the bytes it stores, for the
      classes in `md5_classes` - text only where the server stores it as
      UTF-8, which is what `canon.WIDE` hashes;
    * `sample` - the clause after a table's name that reads a share
      `{pct}` of it, where the server has one, for sizing a large table.
    """

    def __init__(self, said, length, types, piece=None, append=None,
                 appends=(), md5=None, md5_classes=("bytes",),
                 sample=None):
        super().__init__(said)
        self.length, self.types = length, tuple(types)
        self.piece, self.append = piece, append
        self.appends = tuple(appends)
        self.md5, self.md5_classes = md5, tuple(md5_classes)
        self.sample = sample

    def holds(self, declared):
        """Whether a column of this declared type can hold a large value."""
        return _base(declared) in self.types

    def appendable(self, declared):
        return bool(self.append) and _whole(declared) in self.appends

    def length_of(self, quoted, declared=None):
        if isinstance(self.length, dict):
            got = self.length.get(_base(declared))
            if got is None:
                return None
            return got.format(c=quoted)
        return self.length.format(c=quoted)

    def row_expr(self, quote, declared):
        """The SQL of one row's size, over `[(name, declared type)]`: the
        stored length of each column that can hold a large value, eight for
        any other, one for the row - as `row_bytes` counts a row."""
        parts = []
        for name, typ in declared:
            got = self.length_of(quote(name), typ) if self.holds(typ) \
                else None
            parts.append(f"coalesce({got}, 0)" if got else "8")
        return " + ".join(parts + ["1"])


def declared_of(eng, side, db, table):
    """{column: declared type} of a table, asked once a run and side - a
    table not there yet asked again."""
    known = eng.__dict__.setdefault("_declared_of", {})
    at = (side, db, str(table))
    if at not in known:
        got = dict(eng.neutral_columns(side, db, table))
        if not got:
            return got
        known[at] = got
    return known[at]


def sql_row_sizes(eng, side, db, tables=None):
    """{table: {"largest", "p99", "mean", "rows", "exact"}}
    (`Engine.row_sizes`) measured by the server over `Sql.row_expr`: the
    largest and the mean over every row of a table of no more than
    `EXACT_ROWS` - over a sample of a larger one's pages where the server
    takes one (`Sql.sample`), every row where not - and the 99th
    percentile from its first `SIZE_SAMPLE` rows. Only numbers cross the
    network. A table with no column that can hold a large value is sized
    from its catalogue instead: no row of it can be wide."""
    from .engines.base import sizes_of
    way = eng.LARGE_VALUES
    try:
        facts = eng.table_facts(side, db) or {}
    except Exception:  # noqa: BLE001 - sized without the catalogue's word
        facts = {}
    out = {}
    for t in (tables if tables is not None
              else eng.neutral_tables(side, db)):
        try:
            declared = list(eng.neutral_columns(side, db, t))
            if not declared:
                continue
            fact = facts.get(t) or {}
            rows = int(fact.get("rows") or 0)
            if not any(way.holds(typ) for _, typ in declared):
                most = 8 * len(declared) + 1
                mean = (int(fact["bytes"]) / rows if rows and fact.get(
                    "bytes") else most)
                out[t] = {"largest": most, "p99": most, "mean": float(mean),
                          "rows": rows, "exact": False}
                continue
            expr = way.row_expr(eng._q, declared)
            qt = eng._qualified(side, db, t)
            share = ""
            exact = not rows or rows <= EXACT_ROWS or not way.sample
            if not exact:
                pct = max(0.01, min(100.0, 100.0 * EXACT_ROWS / rows))
                share = " " + way.sample.format(pct=f"{pct:.4f}")
            most, mean, n = eng._rows(
                side, db, f"select max(s), avg(s), count(*) from (select"
                          f" {expr} as s from {qt}{share}) x")[0]
            if not n:
                continue
            pick = getattr(eng, "_select", None)
            first = [int(r[0] or 0) for r in eng._rows(
                side, db, pick(expr, qt, "", "", eng.SIZE_SAMPLE) if pick
                else f"select {expr} from {qt} limit {int(eng.SIZE_SAMPLE)}")]
            got = sizes_of(first or [int(most)], exact=exact,
                           rows=int(n) if exact else rows or int(n))
            got["largest"] = max(int(most), got["largest"])
            got["mean"] = float(mean)
            out[t] = got
        except Exception:  # noqa: BLE001 - that table is not sized
            continue
    return out


class ReadPlan:
    """How a SQL engine reads one table's rows with their large values: the
    select list of `columns`, and `finish`, which gives the rows back in
    `columns`' order. A byte value past `limit` of a column that can hold
    one is left out of its row with its length read beside it, and read
    `PIECE` at a time by the row's key (`Sql.piece`) - joined into the
    value, or, inside a digest folded here, hashed as its pieces pass
    (`Hashed`). `size` is what a fetched row holds once its values left out
    are read, for a read that ends at `BATCH_BYTES`. Without a way to read
    a piece, or a key to find the row by, the select list is the plain
    one."""

    #: what a value left out becomes: the value, or its digest's `Hashed`
    hashes = False

    def __init__(self, eng, side, db, table, columns, wide=None, key=None,
                 limit=None):
        self.eng, self.side, self.db, self.table = eng, side, db, table
        self.names = [n for n, _ in columns]
        self.limit = limit = PIECE if limit is None else limit
        wide = wide or {}
        way = getattr(eng, "LARGE_VALUES", None)
        self.exprs = [wide.get(n, eng._q(n)) for n in self.names]
        self.extra, self.split, self.key = [], [], []
        if isinstance(way, Sql) and way.piece:
            key = eng.neutral_key(side, db, table) if key is None else key
            if key and set(key) <= set(self.names):
                declared = declared_of(eng, side, db, table)
                for i, (n, cls) in enumerate(columns):
                    typ = declared.get(n)
                    if cls != "bytes" or n in wide or not way.holds(typ):
                        continue
                    q = eng._q(n)
                    ln = way.length_of(q, typ)
                    if not ln or not self._takes(i, n, cls, typ):
                        continue
                    self.exprs[i] = f"case when {ln} <= {limit} then {q} end"
                    self.extra.append(ln)
                    self.split.append((i, n))
                self.key = key if self.split else []

    def _takes(self, i, name, cls, declared):
        """Whether this plan leaves the column out past `limit`."""
        return True

    @property
    def select(self):
        return ", ".join(self.exprs + self.extra)

    def size(self, raw):
        """Bytes a row as fetched holds once its values left out are read."""
        k = len(self.names)
        n = row_bytes(raw[:k])
        if not self.hashes:
            for j in range(len(self.split)):
                got = raw[k + j]
                if got is not None and int(got) > self.limit:
                    n += int(got)
        return n

    def finish(self, raw):
        """The rows in `columns`' order, each value left out read in pieces
        by its row's key."""
        k = len(self.names)
        if not self.split:
            return [list(r[:k]) for r in raw]
        out, todo = [], []
        for r in raw:
            row = list(r[:k])
            for j, (i, n) in enumerate(self.split):
                size = r[k + j]
                if row[i] is None and size is not None \
                        and int(size) > self.limit:
                    todo.append((row, i, n, int(size)))
            out.append(row)
        if todo:
            self._read_in_pieces(todo)
        return out

    def _read_in_pieces(self, todo):
        """Each value of `todo` ([(row, index, column, size)]) read `PIECE`
        at a time on one connection. A value whose length is not what it
        was once read - the source changed under the read - is read again,
        and then once whole: never handed on cut short."""
        from . import canon
        from .engines.dbapi import MARK
        eng, way = self.eng, self.eng.LARGE_VALUES
        qt = eng._qualified(self.side, self.db, self.table)
        where = " and ".join(f"{eng._q(k)} = {MARK}" for k in self.key)
        conn = eng._connect(self.side, self.db)
        try:
            cur = conn.cursor()
            for row, i, name, size in todo:
                args = [canon.sql_value(row[self.names.index(k)])
                        for k in self.key]
                q = eng._q(name)
                ln = way.length_of(q, declared_of(
                    eng, self.side, self.db, self.table).get(name))

                def one(sql, args=args):
                    eng._run(cur, sql, args)
                    got = cur.fetchone()
                    return got[0] if got else None

                def fetch(at, n, q=q, one=one):
                    part = way.piece.format(c=q, at=int(at), n=int(n))
                    return one(f"select {part} from {qt} where {where}")
                now = None
                for _ in range(3):
                    parts = pieces_of(fetch, size)
                    value = (hashed_pieces(parts) if self.hashes
                             else b"".join(parts))
                    got = (value.length if self.hashes else len(value))
                    now = one(f"select {ln} from {qt} where {where}")
                    if now is None or int(now) == size == got:
                        break
                    size = int(now)
                else:
                    value = one(f"select {q} from {qt} where {where}")
                    value = bytes(value) if value is not None else None
                row[i] = None if now is None else value
        finally:
            conn.close()


class DigestPlan(ReadPlan):
    """The select list a digest folded here reads a table by: a value
    wider than `canon.WIDE` read as its length and md5 beside a NULL where
    the server hashes it (`Sql.md5`), and read in pieces by its row's key
    and hashed as they pass where it does not but can read a piece
    (`Sql.piece`) - each handed on as `Hashed` (`finish`), so the fold
    never holds it."""

    hashes = True

    def __init__(self, eng, side, db, table, columns, wide=None, key=None):
        from .canon import WIDE
        way = getattr(eng, "LARGE_VALUES", None)
        self.md5 = []
        if isinstance(way, Sql) and way.md5:
            declared = declared_of(eng, side, db, table)
            self.md5 = [i for i, (n, cls) in enumerate(columns)
                        if cls in way.md5_classes and not (wide or {}).get(n)
                        and way.holds(declared.get(n))
                        and way.length_of(eng._q(n), declared.get(n))]
        super().__init__(eng, side, db, table, columns, wide, key,
                         limit=WIDE)
        self.hashed = []
        if self.md5:
            declared = declared_of(eng, side, db, table)
            for i in self.md5:
                n = self.names[i]
                q = eng._q(n)
                ln = way.length_of(q, declared.get(n))
                self.exprs[i] = (f"case when {ln} > {WIDE} then null"
                                 f" else {q} end")
                self.extra += [f"case when {ln} > {WIDE} then {ln} end",
                               f"case when {ln} > {WIDE} then"
                               f" {way.md5.format(c=q)} end"]
                self.hashed.append(i)

    def _takes(self, i, name, cls, declared):
        # a column the server hashes is not read in pieces
        return i not in self.md5

    def finish(self, raw):
        k, split = len(self.names), len(self.split)
        rows = super().finish([r[:k + split] for r in raw]) \
            if self.split else [list(r[:k]) for r in raw]
        for r, row in zip(raw, rows):
            for j, i in enumerate(self.hashed):
                n, md5 = r[k + split + 2 * j], r[k + split + 2 * j + 1]
                if n is not None and md5 is not None:
                    row[i] = Hashed(n, md5)
        return rows


def wider_than(rows, piece=None):
    """Whether a row of `rows` holds a text or byte value past `piece`
    (`PIECE`)."""
    piece = PIECE if piece is None else piece
    return any(isinstance(v, (bytes, bytearray, str)) and len(v) > piece
               for r in rows for v in r)


def written_in_pieces(eng, cur, side, db, table, columns, key, rows):
    """The rows of `rows` holding a value past `PIECE` in a column the
    target adds to (`Sql.appendable`), each inserted with the first piece of
    each such value and filled by the rest, a piece a statement, on the
    caller's cursor and in its transaction; the other rows handed back for
    the caller's own insert. A table without a key to find a row by writes
    every row whole."""
    from . import canon
    from .engines.dbapi import MARK
    way = getattr(eng, "LARGE_VALUES", None)
    names = [n for n, _ in columns]
    if not isinstance(way, Sql) or not way.append or not key \
            or not set(key) <= set(names):
        return rows
    declared = declared_of(eng, side, db, table)
    grows = [i for i, n in enumerate(names)
             if way.appendable(declared.get(n))]
    if not grows:
        return rows
    quoted = eng._qualified(side, db, table)
    insert = (f"insert into {quoted}"
              f" ({', '.join(eng._q(n) for n in names)}) values"
              f" ({', '.join([MARK] * len(names))})")
    where = " and ".join(f"{eng._q(k)} = {MARK}" for k in key)
    at = [names.index(k) for k in key]
    small = []
    for r in rows:
        values = [canon.write_value(eng.CANON_ENGINE, c, v)
                  for (_, c), v in zip(columns, r)]
        big = [i for i in grows if isinstance(values[i], (bytes, bytearray,
                                                          str))
               and len(values[i]) > PIECE]
        if not big:
            small.append(r)
            continue
        first = list(values)
        for i in big:
            first[i] = values[i][:PIECE]
        eng._run(cur, insert, first)
        keyed = [values[j] for j in at]
        for i in big:
            add = way.append.format(c=eng._q(names[i]), v=MARK)
            for start in range(PIECE, len(values[i]), PIECE):
                eng._run(cur, f"update {quoted} set {add} where {where}",
                         [values[i][start:start + PIECE]] + keyed)
    return small


# --- rows of an Arrow batch ------------------------------------------------

def arrow_row_bytes(batch):
    """Bytes each row of an Arrow record batch takes as carried
    (`row_bytes`): a string or binary value by its length, read off the
    column's offsets without the value being copied out; anything else as
    eight, a NULL as one."""
    import pyarrow as pa
    import pyarrow.compute as pc
    total = pa.array([1] * batch.num_rows, pa.int64())
    for col in batch.columns:
        t = col.type
        if (pa.types.is_string(t) or pa.types.is_large_string(t)
                or pa.types.is_binary(t) or pa.types.is_large_binary(t)
                or pa.types.is_fixed_size_binary(t)):
            each = pc.fill_null(pc.cast(pc.binary_length(col), pa.int64()),
                                1)
        else:
            each = pc.if_else(pc.is_null(col), 1, 8)
        total = pc.add(total, pc.cast(each, pa.int64()))
    return total.to_pylist()


def arrow_rows(batch, names, most_bytes=None):
    """The rows of an Arrow record batch as lists of values, `names` in
    order, in slices cut by bytes (`arrow_row_bytes`): what becomes Python
    values at once is a slice, never the whole batch."""
    most_bytes = BATCH_BYTES if most_bytes is None else most_bytes
    idx = [batch.schema.get_field_index(n) for n in names]
    if not idx:
        if batch.num_rows:
            yield [[] for _ in range(batch.num_rows)]
        return
    sizes = arrow_row_bytes(batch.select(idx))
    start = 0
    while start < batch.num_rows:
        held, end = 0, start
        while end < batch.num_rows and (end == start
                                        or held + sizes[end] <= most_bytes):
            held += sizes[end]
            end += 1
        part = batch.slice(start, end - start)
        cols = [part.column(i).to_pylist() for i in idx]
        yield [list(r) for r in zip(*cols)]
        start = end
