"""A table's digest as a tree, every leaf of it in one scan a side, and
the rows that differ read only from the leaves that differ (backlog R20.9,
F1; docs/research/diff-algorithms-2026-09-28.md sections 3.4, 4.3, 9).

The digest migkit compares is a sum: count, the sum of each row's hash and
the sum of each key's hash, added up exactly (never wrapped, never XORed).
A sum over a table is the sum of the sums over any partition of it, so a
parent's digest is its children's added together. One

    select bucket, count(*), sum(row hash), sum(key hash) ... group by bucket

per side is then every leaf of the tree at once, and every level above the
leaves is additions on this machine - nothing is read again to go down a
level. What it replaced (`hetero._bisect`) digested halves of the key range
one query after another, each half re-read on both servers, and refused
every key but a single integer.

Measured (`bench/proof.py`; a PostgreSQL 16 pair in a 2-CPU VM, a table of
an integer key, md5 text, an integer, a numeric and a timestamp, 10 rows
apart - 4 changed, 3 missing, 3 extra; the table digest first, as the
check takes it, then the rows named):

    rows  table digest  then halving           then range leaves       hash
    1M    2.26s         24.3s (194 digests)    3.41s (8 of 1,000)      6.25s
    5M    12.6s         79.9s (290 digests)    17.1s (8 of 4,996)      31.1s

so the whole verify of a differing table went from 26.6s to 5.67s at 1M
and from 92.4s to 29.7s at 5M; 14,016 and 14,024 rows were read back.
Leaves by hash cost a second pass a side (the rows of the differing
leaves are found through a filter on the hash, not an index). Over a link
holding 50 ms a round trip (1M rows): 10 differences, the halving 136.8s,
the leaves 5.16s, the sketch 5.92s; 1,000 differences, the halving past
900s (stopped), the leaves 19.7s, the sketch 6.18s. The tree over equal
tables costs more than the plain digest - 3.68s against 2.77s at 1M,
16.2s against 10.9s at 5M - which is why the plain digest stays the first
pass and the tree is read only once a table differs.

The bisecting reader (`bisection`, below) over every column did the same
table in 1.97s at 1M and 10.7s at 5M, where the digest and the range
leaves took 5.12s and 23.0s: on one engine it wins, reading each engine's
own text of a value; across two engines that text differs, so it is a
rung only where both sides are one engine, and what it names is held to
migkit's digests. Asked without `-c %` it compared the keys alone and
reported 0 of the 4 changed rows.

**Leaves, by what the key allows.**
* `hash`: a bucket is the top bits of the key's hash, over the key's
  canonical text (`canon.row_expr`) - the same text on both sides whatever
  the engines, so a composite key, a text key under two collations and a
  key of any type all fall into the same leaf on both. The rows of a leaf
  are read back through a filter on the hash (one scan a side for every
  leaf that differs).
* `range`: a bucket is a range of a single integer key, identical on both
  sides; the rows of a leaf are read back by the key's index.
* `row`: a table with no key buckets by the whole row's hash, and a row
  held twice counts twice - a duplicate is carried by the count, never
  cancelled.

**Localized, then held to the totals.** The differing leaves' rows come
back as (key, row hash) and are matched by the key's canonical text; each
leaf's rows must add up to that leaf's digest on its side, and the rows
found missing, extra and changed must add up exactly to the difference of
the two tables' count and sums (`Localized.complete`). Where they do not,
the table "differs, not fully localized" - never ok.

**A sketch where the link is the limit** (`Sketch`, an invertible Bloom
lookup table with sum cells): each row adds (1, its key, its hash, its
hash squared) into three cells, one scan builds every cell in SQL, and the
two sides' cells subtracted peel down to the differing rows themselves -
bytes on the wire in proportion to the differences, not the table. A cell
is taken as one row's only when its count divides its sums and its squared
lane equals the count times the square of that row's hash, which a mix of
rows passes one time in about 2^60; a sketch that does not peel to empty
says so and the leaves are read instead, and what it did decode is held to
the totals as the leaves are.

**Chosen per table** (`PASSES`, composed by `decide.compose`): proved by
the kept leaves where the hop keeps them and the tail's position matches
(pass D, `kept`), else by one scan (pass A); leaves by range for one
integer key, else by hash; localized by the sketch where few differences
are expected and the link is slow (measured round trip), else by the
leaves (pass B) - or, where measured cheaper, by the second reader's
aggregates per group of a column the source's statistics say splits the
table (`by_groups`), or by the bisecting reader on one engine, each held
to the totals as the leaves are; confirmed against the totals, then past
the tail's position where a tail runs (pass C); and a second, independent
reading where the hop asks for two or migkit's own digest disagreed, held
key for key to the rows found (`second_reading`).
"""
import hashlib
import time
from dataclasses import dataclass, field

from . import canon, decide, render, rowtext

HEX = canon.DIGEST_HEX
BITS = canon.DIGEST_BITS
#: rows a leaf holds on average; the research's L, where the leaf digests
#: of a table are a few hundredths of a percent of it on the wire
LEAF_ROWS = 1000
#: at most 2^20 leaves a table (a million, some 40 MB of digests a side)
MOST_BITS = 20
#: rows of differing leaves held here at once, a side; more are read in
#: turns - by the key's index for range leaves, one filtered pass a turn
#: for hash leaves - so the memory is bounded and the time is not
HELD = 500_000
#: rows in differing leaves read back at most; past it the table is said
#: to differ in too many places to name, and re-copying those ranges is
#: cheaper than reading them back (backlog R18.4)
MOST_ROWS = 2_000_000
#: keys a statement asks for at once
IN_LIST = 1000
#: a cell is `(count, key sum, hash sum, square sum)`
K = 3
_SLICE = 20
_MASK = (1 << _SLICE) - 1
#: a round trip at or above this is a slow link for the sketch's choice
SLOW_RTT_MS = 20.0
#: differences expected at most for the sketch to be worth building
SKETCH_MOST = 5000


def text_hash(salt, text):
    """A row's (or key's) hash: the first 60 bits of md5(salt || text),
    the canonical digest's width (`canon.DIGEST_BITS`) behind this run's
    salt (`Engine.digest_salt`), as an integer."""
    return int(hashlib.md5((salt + text).encode(),
                           usedforsecurity=False).hexdigest()[:HEX], 16)


@dataclass(frozen=True)
class Spec:
    """How rows fall into leaves, the same on both sides."""
    kind: str               #: "hash", "range" or "row"
    bits: int = 0           #: hash and row: 2^bits leaves
    base: int = 0           #: range: the lowest key at planning
    width: int = 1          #: range: keys a leaf
    salt: str = ""

    def bucket(self, kh, key=None):
        if self.kind == "range":
            return (int(key) - self.base) // self.width
        return kh >> (BITS - self.bits)

    def bounds(self, b):
        """[lo, hi) of a range leaf."""
        lo = self.base + int(b) * self.width
        return lo, lo + self.width

    def as_dict(self):
        return {"kind": self.kind, "bits": self.bits, "base": self.base,
                "width": self.width, "salt": self.salt}

    @classmethod
    def of(cls, d):
        return cls(d["kind"], int(d.get("bits", 0)), int(d.get("base", 0)),
                   int(d.get("width", 1)), d.get("salt", ""))


def plan(rows, kind, lo=None, hi=None, salt=""):
    """The leaves for a table of about `rows` rows: `LEAF_ROWS` a leaf on
    average, at most 2^`MOST_BITS` of them."""
    leaves = max(1, -(-int(rows or 0) // LEAF_ROWS))
    if kind == "range" and lo is not None and hi is not None:
        span = int(hi) - int(lo) + 1
        leaves = min(leaves, 1 << MOST_BITS)
        return Spec("range", base=int(lo), width=max(1, -(-span // leaves)),
                    salt=salt)
    bits = min(MOST_BITS, max(0, (leaves - 1).bit_length()))
    return Spec(kind, bits=bits, salt=salt)


@dataclass
class Side:
    """One side of a comparison: the engine, the table, the columns in the
    pair's order `[(name, class)]`, the key's columns in this side's names
    and the pair's key order (`[]` without one), and the hop's row filter
    in this side's SQL."""
    eng: object
    side: str
    db: str
    table: str
    cols: list
    key: list = field(default_factory=list)
    where: str = None

    @property
    def sql(self):
        """Whether the server hashes (PostgreSQL and MySQL render canon's
        text in SQL, and so do the warehouses and DuckDB where every column
        of the table is one their SQL renders, proved on its first rows -
        `warehouse_sql.DigestsInSQL.leaf_ready`); elsewhere the rows are
        hashed in this process, as `neutral_digest` folds them there - and
        on those as well where a column's class is one whose SQL rendering
        is not the one this process writes (`canon.FOLDED_HERE`), as
        `neutral_digest` does."""
        if any(c in canon.FOLDED_HERE for _, c in self.cols):
            return False
        if self.dialect:
            if "_in_sql" not in self.__dict__:
                self.__dict__["_in_sql"] = bool(self.eng.leaf_ready(
                    self.side, self.db, self.table, self.cols))
            return self.__dict__["_in_sql"]
        return (getattr(self.eng, "CANON_ENGINE", None) in SQL_ENGINES
                and bool(getattr(self.eng, "SQL_DIALECT", None)))

    @property
    def dialect(self):
        """The warehouse dialect a side's leaves are written in
        (`warehouse_sql`), or None for PostgreSQL's and MySQL's own."""
        if getattr(self.eng, "CANON_ENGINE", None) in SQL_ENGINES:
            return None
        return getattr(self.eng, "DIGEST_DIALECT", None)

    @property
    def engine(self):
        return self.eng.CANON_ENGINE

    def key_at(self):
        names = [n for n, _ in self.cols]
        return [names.index(k) for k in self.key]


SQL_ENGINES = ("postgres", "mysql")


class Hasher:
    """(bucket, row hash, key hash) of rows read into this process, as the
    SQL computes them: each column rendered by its class (`render`), the
    row and the key encoded injectively (`rowtext.encode`), hashed behind
    the salt."""

    def __init__(self, spec, classes, key_at):
        self.spec = spec
        self.fns = [render.digest_renderer(c) for c in classes]
        self.key_at = list(key_at)

    def texts(self, values):
        return [f(v) for f, v in zip(self.fns, values)]

    def of(self, values):
        rt = self.texts(values)
        rh = text_hash(self.spec.salt, rowtext.encode(rt))
        if not self.key_at:
            return self.spec.bucket(rh), rh, rh
        kh = text_hash(self.spec.salt,
                       rowtext.encode([rt[i] for i in self.key_at]))
        key = values[self.key_at[0]] if self.spec.kind == "range" else None
        return self.spec.bucket(kh, key), rh, kh


# --- SQL ---------------------------------------------------------------------

def _h(engine, expr, salt):
    """SQL for the 60-bit hash of a text expression, behind the salt."""
    if engine == "postgres":
        # the text's UTF-8 bytes, as MySQL and this process hash them
        # (`canon.digest_expr`): a LATIN1 database's own bytes are not
        return (f"('x'||substr(md5(convert_to('{salt}'||{expr}, 'UTF8')),"
                f"1,{HEX}))::bit({BITS})::bigint")
    return (f"cast(conv(substr(md5(concat('{salt}', {expr})),1,{HEX}),16,10)"
            " as unsigned)")


def _q(s, name):
    return s.eng._quote_ident(name)


def _row_hash(s, cols, salt):
    """SQL for a row's (or a key's) salted hash on this side."""
    if s.dialect:
        return s.eng.leaf_hash(
            s.eng.leaf_row(s.side, s.db, s.table, cols), salt)
    return _h(s.engine, canon.row_expr(s.engine, cols), salt)


def _shift(s, value, bits):
    return (s.eng.leaf_shift(value, bits) if s.dialect
            else f"({value} >> {bits})")


# --- a table with no key, in buckets of its rows' hash ------------------------
# The copy of a table with no key goes by the same leaves as the check of it
# (`Spec("row")`): a bucket is the top bits of each row's hash, one node of
# the tree the check reads, so the copy, its resume and the check all find
# a row in one place (backlog P0-KEYLESS).

def bucket_columns(cols):
    """The columns a bucket is found by: those whose canonical text every
    engine writes alike in SQL - a class rendered in this process
    (`canon.FOLDED_HERE`) is left out. A bucket is a partition of the rows,
    so a column left out of finding it is still copied and checked; rows
    alike in every other column only share a bucket."""
    return [(n, c) for n, c in cols if not canon.folded_here(c)]


def bucket_where(s, spec, bucket):
    """The rows of one node of a `row` spec's tree - the top `spec.bits` of
    the hash of the row's canonical text over `bucket_columns` equal to
    `bucket` - as a predicate in side `s`'s SQL, where its server hashes
    its rows as the leaves do (`Side.sql`: PostgreSQL, MySQL, the
    warehouses' and DuckDB's dialects) or SQLite through the function
    migkit registers on its connections (`migkit_hash60`); None where the
    rows are rendered only in this process, or no column is left to find a
    bucket by."""
    cols = bucket_columns(s.cols)
    if not cols:
        return None
    if s.engine == "sqlite":
        h = (f"migkit_hash60('{spec.salt}' ||"
             f" {canon.row_expr('sqlite', cols)})")
        return f"(({h}) >> {BITS - spec.bits}) = {int(bucket)}"
    by = Side(s.eng, s.side, s.db, s.table, cols)
    if not by.sql:
        return None
    shifted = _shift(by, _row_hash(by, cols, spec.salt), BITS - spec.bits)
    return f"{shifted} = {int(bucket)}"


def _inner(s, spec, extra=None, keys=False):
    """The rows as (rh[, kh][, k0, k1 ...]): what every query here reads,
    the key's raw values where `keys` or the leaves are key ranges."""
    cls = dict(s.cols)
    parts = [f"{_row_hash(s, s.cols, spec.salt)} as rh"]
    if s.key:
        key = [(k, cls[k]) for k in s.key]
        parts.append(f"{_row_hash(s, key, spec.salt)} as kh")
        if keys or spec.kind == "range":
            parts += [f"{_q(s, k)} as k{i}" for i, k in enumerate(s.key)]
    where = [w for w in (s.where, extra) if w]
    return (f"select {', '.join(parts)} from"
            f" {s.eng._qualified(s.side, s.db, s.table)}"
            + (" where " + " and ".join(f"({w})" for w in where)
               if where else ""))


def _fenced(s, inner):
    """The inner rows as a derived table computed once per row: PostgreSQL
    pulls a plain subquery up into the query around it, and the key's hash
    is named twice there (the leaf and its sum) - `offset 0` keeps the
    subquery whole, so the md5 runs once a row. What the fence is worth
    against the pull-up is not measured yet (`bench/proof.py variants`)."""
    return f"({inner} offset 0)" if s.engine == "postgres" else f"({inner})"


def _bucket_sql(s, spec):
    kh = "kh" if s.key else "rh"
    if spec.kind != "range":
        return _shift(s, kh, BITS - spec.bits)
    lo, w = spec.base, spec.width
    if s.dialect:
        div = s.eng.leaf_div
        return (f"(case when k0 >= {lo} then {div(f'k0 - ({lo})', w)}"
                f" else -1 - {div(f'({lo - 1}) - k0', w)} end)")
    if s.engine == "postgres":
        v = "k0::numeric"
        return (f"(case when {v} >= {lo} then div({v} - ({lo}), {w})"
                f" else -1 - div(({lo - 1}) - {v}, {w}) end)::bigint")
    v = "cast(k0 as decimal(65,0))"
    return (f"(case when {v} >= {lo} then ({v} - ({lo})) div {w}"
            f" else -1 - ((({lo - 1}) - {v}) div {w}) end)")


def _scan_sql(s, spec, extra=None, only=None):
    kh = "kh" if s.key else "rh"
    # sums that do not wrap where an engine's own integer sum would
    total = s.eng.leaf_sum if s.dialect else (lambda v: f"sum({v})")
    return (f"select b, count(*), {total('rh')}, {total(kh)} from"
            f" (select {_bucket_sql(s, spec)} as b, rh"
            + (", kh" if s.key else "") +
            f" from {_fenced(s, _inner(s, spec, extra))} x) y"
            + (f" where b in ({', '.join(str(b) for b in only)})"
               if only else "") + " group by b")


def _range_pred(s, spec, buckets):
    """The key ranges of these range leaves, adjacent ones joined."""
    runs, start, last = [], None, None
    for b in sorted(buckets):
        if start is not None and b == last + 1:
            last = b
            continue
        if start is not None:
            runs.append((start, last))
        start = last = b
    if start is not None:
        runs.append((start, last))
    k = _q(s, s.key[0])
    return " or ".join(f"({k} >= {spec.bounds(a)[0]} and"
                       f" {k} < {spec.bounds(z)[1]})" for a, z in runs)


# --- reading ---------------------------------------------------------------

def _rows(s, size=10_000):
    """Every row of a side read into this process: by key in turns where
    the engine resumes by key, else in batches."""
    scope = {"where": s.where} if s.where else {}
    if s.key and getattr(s.eng, "RESUMES_BY_KEY", True):
        after = None
        while True:
            rows, after = s.eng.neutral_read(s.side, s.db, s.table, s.cols,
                                             after, size, **scope)
            yield from rows
            if not rows or after is None:
                return
    else:
        for batch in s.eng.neutral_batches(s.side, s.db, s.table, s.cols,
                                           size, **scope):
            yield from batch


class Tree:
    """{leaf: (count, row-hash sum, key-hash sum)}, and every level above
    the leaves added up here: a leaf's parent is `leaf >> 1`, up to the
    level where nothing merges any more."""

    def __init__(self, leaves=None):
        self.leaves = dict(leaves or {})

    def add(self, b, n, s, k):
        was = self.leaves.get(b, (0, 0, 0))
        self.leaves[b] = (was[0] + n, was[1] + s, was[2] + k)

    @property
    def root(self):
        n = s = k = 0
        for a, b, c in self.leaves.values():
            n, s, k = n + a, s + b, k + c
        return n, s, k

    def levels(self):
        """[{node: digest}] from the leaves up: each level its children
        added, until a level adds nothing up (at most one node either side
        of zero)."""
        out = [dict(self.leaves)]
        while any(b not in (0, -1) for b in out[-1]):
            up = {}
            for b, (n, s, k) in out[-1].items():
                p = b >> 1
                a = up.get(p, (0, 0, 0))
                up[p] = (a[0] + n, a[1] + s, a[2] + k)
            out.append(up)
        return out

    def differing(self, other):
        """The leaves whose digests differ. Compared leaf by leaf here:
        measured on 2^20 leaves with 10 differing, 0.03s, where building
        the levels to go down them from the root took 0.7s - the levels
        are for a reader that cannot hold both trees at once (a relay
        comparing a level at a time across a link), not for this one."""
        z = (0, 0, 0)
        mine, theirs = self.leaves, other.leaves
        return sorted(b for b in set(mine) | set(theirs)
                      if mine.get(b, z) != theirs.get(b, z))


def scan(s, spec, buckets=None):
    """Every leaf of one side in one pass: a `Tree`. `buckets` asks for
    those leaves only - by the key's index where the leaves are ranges of
    it, else filtered from the one pass."""
    return scan_marked(s, spec, buckets, marked=False)[0]


def scan_marked(s, spec, buckets=None, marked=True, outside=None):
    """(Tree, mark before, mark after) - the leaves as `scan` reads them,
    with what the side had made visible around the read
    (`Engine.marked_rule`), for a leaf kept up from the change stream
    after it. `outside` (lowest, highest) asks instead for the range
    leaves beyond those two, by the key's index."""
    want = None if buckets is None else set(buckets)
    if s.sql:
        tree, lo, hi = Tree(), None, None
        sqls = _scan_sqls(s, spec, want) if outside is None else [
            _scan_sql(s, spec, extra=(
                f"{_q(s, s.key[0])} < {spec.bounds(outside[0])[0]} or"
                f" {_q(s, s.key[0])} >= {spec.bounds(outside[1])[1]}"))]
        for sql in sqls:
            if marked:
                a, rows, z = s.eng.marked_rule(s.side, s.db, sql)
                lo, hi = (a, z) if lo is None else (lo, z)
            else:
                rows = s.eng.run_rule(s.side, s.db, sql)
            for b, n, r, k in rows:
                if want is None or int(b) in want:
                    tree.add(int(b), int(n), int(r), int(k))
        return tree, lo, hi
    lo = s.eng.snapshot_mark(s.side, s.db) if marked else None
    tree = Tree()
    h = Hasher(spec, [c for _, c in s.cols], s.key_at())
    for row in _rows(s):
        b, rh, kh = h.of(row)
        if outside is not None:
            if outside[0] <= b <= outside[1]:
                continue
        elif want is not None and b not in want:
            continue
        tree.add(b, 1, rh, kh)
    return tree, lo, s.eng.snapshot_mark(s.side, s.db) if marked else None


def _scan_sqls(s, spec, want):
    """The statements for these leaves: one pass for the whole table, or
    for a hash leaf set small enough to name; the key's ranges, some
    hundreds a statement, for range leaves."""
    if want is None or (spec.kind != "range" and len(want) > IN_LIST):
        return [_scan_sql(s, spec)]
    if not want:
        return []
    order = sorted(want)
    if spec.kind == "range":
        return [_scan_sql(s, spec, extra=_range_pred(s, spec,
                                                     order[i:i + 200]))
                for i in range(0, len(order), 200)]
    return [_scan_sql(s, spec, only=order)]


@dataclass
class Found:
    """The rows of some leaves on one side: keyed, {key text: (raw key,
    row hash, key hash, leaf)}; without a key, {row hash: [count, leaf]}."""
    keyed: dict = field(default_factory=dict)
    kept: dict = field(default_factory=dict)

    def sums(self):
        """{leaf: (count, row-hash sum, key-hash sum)} of what was read."""
        out = {}
        for _, rh, kh, b in self.keyed.values():
            a = out.get(b, (0, 0, 0))
            out[b] = (a[0] + 1, a[1] + rh, a[2] + kh)
        for rh, (n, b) in self.kept.items():
            a = out.get(b, (0, 0, 0))
            out[b] = (a[0] + n, a[1] + n * rh, a[2] + n * rh)
        return out


def leaf_rows(s, spec, buckets):
    """The rows of these leaves on one side (`Found`)."""
    want = set(buckets)
    out = Found()
    if not want:
        return out
    if not s.sql:
        h = Hasher(spec, [c for _, c in s.cols], s.key_at())
        at = s.key_at()
        for row in _rows(s):
            b, rh, kh = h.of(row)
            if b in want:
                _keep(out, s, [row[i] for i in at], rh, kh, b)
        return out
    order = sorted(want)
    if spec.kind == "range":
        for i in range(0, len(order), 200):
            part = order[i:i + 200]
            names = ", ".join(f"k{j}" for j in range(len(s.key)))
            sql = (f"select rh, kh, {names} from"
                   f" ({_inner(s, spec, _range_pred(s, spec, part), True)}) x")
            for rh, kh, *key in s.eng.run_rule(s.side, s.db, sql):
                rh, kh = int(rh), int(kh)
                b = spec.bucket(kh, key[0])
                if b in want:
                    _keep(out, s, key, rh, kh, b)
        return out
    for i in range(0, len(order), IN_LIST):
        part = ", ".join(str(b) for b in order[i:i + IN_LIST])
        if s.key:
            names = ", ".join(f"k{j}" for j in range(len(s.key)))
            sql = (f"select rh, kh, {names} from"
                   f" {_fenced(s, _inner(s, spec, keys=True))} x"
                   f" where {_shift(s, 'kh', BITS - spec.bits)}"
                   f" in ({part})")
            for rh, kh, *key in s.eng.run_rule(s.side, s.db, sql):
                rh, kh = int(rh), int(kh)
                _keep(out, s, key, rh, kh, spec.bucket(kh))
        else:
            sql = (f"select rh, count(*) from {_fenced(s, _inner(s, spec))} x"
                   f" where {_shift(s, 'rh', BITS - spec.bits)}"
                   f" in ({part})"
                   " group by rh")
            for rh, n in s.eng.run_rule(s.side, s.db, sql):
                rh = int(rh)
                _keep(out, s, None, rh, rh, spec.bucket(rh), int(n))
    return out


def _keep(out, s, key, rh, kh, b, n=1):
    if not s.key:
        was = out.kept.get(rh)
        out.kept[rh] = [n + (was[0] if was else 0), b]
        return
    cls = dict(s.cols)
    text = tuple(canon.render_value(cls[k], v) for k, v in zip(s.key, key))
    out.keyed[text] = (tuple(key), rh, kh, b)


# --- localizing --------------------------------------------------------------

@dataclass
class Localized:
    """What the differing leaves hold, and whether it accounts for the
    whole difference."""
    missing: list = field(default_factory=list)
    changed: list = field(default_factory=list)
    extra: list = field(default_factory=list)
    only_src: int = 0          #: without a key: rows on the source alone
    only_dst: int = 0          #: and on the target alone
    raw: dict = field(default_factory=dict)   #: key text -> raw key
    leaves: int = 0            #: leaves that differ
    read: int = 0              #: leaves read back
    rows: int = 0              #: rows read back, both sides
    complete: bool = False
    why: str = ""
    how: str = ""              #: the way it was localized, for the clause
    said: str = ""             #: what that way did, where it says more
    found: tuple = (0, 0, 0)   #: what the rows found add up to
    delta: tuple = (0, 0, 0)   #: the difference of the two tables' totals

    @property
    def differ(self):
        return len(self.missing) + len(self.changed) + len(self.extra) + \
            self.only_src + self.only_dst


def _minus(a, b):
    return tuple(x - y for x, y in zip(a, b))


def _plus(a, b):
    return tuple(x + y for x, y in zip(a, b))


def compare(fa, fb, out):
    """Rows of the same leaves on both sides matched, into `out`; returns
    what they add up to, (count, row-hash, key-hash) source less target."""
    total = (0, 0, 0)
    for text, (raw, rh, kh, _) in fa.keyed.items():
        other = fb.keyed.get(text)
        if other is None:
            out.missing.append(text)
            out.raw[text] = raw
            total = _plus(total, (1, rh, kh))
        elif other[1] != rh:
            out.changed.append(text)
            out.raw[text] = raw
            total = _plus(total, (0, rh - other[1], kh - other[2]))
    for text, (raw, rh, kh, _) in fb.keyed.items():
        if text not in fa.keyed:
            out.extra.append(text)
            out.raw[text] = raw
            total = _plus(total, (-1, -rh, -kh))
    for rh in set(fa.kept) | set(fb.kept):
        d = (fa.kept.get(rh) or [0])[0] - (fb.kept.get(rh) or [0])[0]
        if d > 0:
            out.only_src += d
        elif d < 0:
            out.only_dst -= d
        total = _plus(total, (d, d * rh, d * rh))
    return total


def localize(a, b, spec, ta, tb, held=HELD):
    """The rows that differ between two sides whose trees are `ta` and
    `tb`: only the differing leaves are read back, in turns of about
    `held` rows a side; each leaf's rows are held to its digest on its
    side, and everything found to the difference of the two roots."""
    out = Localized(delta=_minus(ta.root, tb.root), how="leaves")
    bad = ta.differing(tb)
    out.leaves = len(bad)
    z = (0, 0, 0)
    size = {x: max(ta.leaves.get(x, z)[0], tb.leaves.get(x, z)[0], 1)
            for x in bad}
    if sum(size.values()) > MOST_ROWS:
        whole = max(len(ta.leaves), len(tb.leaves))
        out.why = (f"{len(bad):,} leaves of {whole:,} differ, holding"
                   f" {sum(size.values()):,} rows - too many places to"
                   " name, and copying those ranges again costs less than"
                   " reading them back")
        return out
    turns, part, held_now = [], [], 0
    for x in bad:
        if part and held_now + size[x] > held:
            turns.append(part)
            part, held_now = [], 0
        part.append(x)
        held_now += size[x]
    if part:
        turns.append(part)
    total, short = (0, 0, 0), []
    for part in turns:
        fa, fb = both(leaf_rows, a, b, spec, part)
        out.read += len(part)
        out.rows += len(fa.keyed) + len(fb.keyed) + sum(
            n for n, _ in fa.kept.values()) + sum(
            n for n, _ in fb.kept.values())
        for side, got, tree in (("source", fa, ta), ("target", fb, tb)):
            sums = got.sums()
            for leaf in part:
                if sums.get(leaf, z) != tree.leaves.get(leaf, z):
                    short.append((side, leaf))
        total = _plus(total, compare(fa, fb, out))
    out.found = total
    if short:
        side, leaf = short[0]
        out.why = (f"{len(short)} leaves read back do not add up to their"
                   f" digest (the first on the {side}) - the table changed"
                   " between the two reads")
    elif total != out.delta:
        out.why = ("the rows found do not add up to the difference of the"
                   " two tables' totals")
    else:
        out.complete = True
    return out


# --- generations under writes (F1.d) -----------------------------------------

#: generations a check gives a source written while it is read: each reads
#: again only the leaves that still differ (`MIGKIT_GENERATIONS`)
GENERATIONS = 5
#: seconds between two generations where no tail's position is waited
#: past; the hop's `settle` where it says one
GENERATION_SECONDS = 2.0


def most_generations():
    import os
    try:
        return max(0, int(os.environ.get("MIGKIT_GENERATIONS", "")
                          or GENERATIONS))
    except ValueError:
        return GENERATIONS


def settle(before_a, before_b, now_a, now_b, which):
    """(settled, still, moving) of the leaves `which`, read at two
    generations - {leaf: digest} each side, before and now: equal now on
    both sides, differing and not moved on either side since, or differing
    and moved. What held still differs as of now; what moved is still
    being written, or still arriving."""
    z = (0, 0, 0)
    settled, still, moving = set(), set(), set()
    for x in which:
        na, nb = now_a.get(x, z), now_b.get(x, z)
        if na == nb:
            settled.add(x)
        elif na == before_a.get(x, z) and nb == before_b.get(x, z):
            still.add(x)
        else:
            moving.add(x)
    return settled, still, moving


@dataclass
class Generations:
    """What differing leaves did over the generations they were read."""
    ta: Tree                   #: the source's leaves, each as last read
    tb: Tree                   #: and the target's
    differed: int = 0          #: leaves that differed at the first read
    rounds: int = 0            #: generations read
    settled: set = field(default_factory=set)
    still: set = field(default_factory=set)
    moving: set = field(default_factory=set)

    @property
    def all_settled(self):
        return bool(self.differed) and not self.still and not self.moving

    def said(self):
        """The clause: how the leaves that differed came out."""
        if not self.rounds:
            return ""
        head = (f"; read while the source was written: {self.differed:,}"
                f" leaves that differed read again over {self.rounds}"
                f" generation{'s' if self.rounds != 1 else ''}")
        if self.all_settled:
            return head + (" - every one read equal on both sides (still"
                           " arriving, then equal); compared leaf by leaf,"
                           " each leaf as of the generation it settled in")
        parts = []
        if self.settled:
            parts.append(f"{len(self.settled):,} settled (still arriving,"
                         " then equal)")
        if self.still:
            parts.append(f"{len(self.still):,} held still and differ")
        if self.moving:
            parts.append(f"{len(self.moving):,} never settled - still being"
                         f" written after {self.rounds} generations, so no"
                         " verdict is given for their rows")
        return head + " - " + ", ".join(parts)


def generations(a, b, spec, ta, tb, wait, most=None):
    """The leaves where `ta` and `tb` differ, read again generation by
    generation until none is still moving (migration-verifier's rounds,
    over the digest tree): each generation, after `wait(n)`, reads on both
    sides only the leaves that differed and moved in the one before. A leaf
    equal on both sides has settled; one that differs and did not move on
    either side since the last generation holds still - a difference as of
    now, localized from those leaves; one still moving after `most`
    generations never settled, and is said so."""
    most = most_generations() if most is None else most
    z = (0, 0, 0)
    bad = set(ta.differing(tb))
    out = Generations(Tree(ta.leaves), Tree(tb.leaves), differed=len(bad))
    before_a = {x: ta.leaves.get(x, z) for x in bad}
    before_b = {x: tb.leaves.get(x, z) for x in bad}
    for n in range(1, most + 1):
        if not bad:
            break
        wait(n)
        na, nb = both(scan, a, b, spec, sorted(bad))
        out.rounds = n
        settled, still, moving = settle(before_a, before_b, na.leaves,
                                        nb.leaves, bad)
        for x in bad:
            for tree, now in ((out.ta, na), (out.tb, nb)):
                if now.leaves.get(x, z) == z:
                    tree.leaves.pop(x, None)
                else:
                    tree.leaves[x] = now.leaves[x]
        out.settled |= settled
        out.still |= still
        before_a = {x: na.leaves.get(x, z) for x in moving}
        before_b = {x: nb.leaves.get(x, z) for x in moving}
        bad = moving
    out.moving = set(bad) if out.rounds else set()
    if not out.rounds:
        out.still = set(ta.differing(tb))
    return out


def _generation_wait(pair, db, a):
    """What a generation waits on: past the source's position of now where
    a tail follows it (`fence_wait`), else the hop's `settle` seconds."""
    from . import tailctl
    opts = pair.hop.options or {}
    pause = float(opts.get("settle") or GENERATION_SECONDS)

    def wait(n):
        if tailctl.alive(pair.hop.report_dir(db)):
            try:
                pos = a.eng.log_position("src", db)
            except Exception:  # noqa: BLE001 - waited by the clock instead
                pos = None
            if pos is not None and pair.fence_wait(
                    db, pos, int(opts.get("fence_timeout", 300))):
                return
        time.sleep(pause)
    return wait


class Said(str):
    """A drilldown's clause, and whether what differed settled - every
    leaf that differed read equal within its generations - with the rows
    the settled leaves hold."""
    settled = False
    rows_src = rows_dst = None


def confirm(pair_side_a, pair_side_b, found, most=5000):
    """The changed rows read by key on both sides and compared value by
    value: {key text: [columns that differ]} for those that still differ,
    and how many read equal a moment later."""
    a, b = pair_side_a, pair_side_b
    keys = found.changed[:most]
    if not keys or not a.key:
        return {}, 0
    raws = [found.raw[k] for k in keys]
    ra = a.eng.neutral_rows_by_key(a.side, a.db, a.table, a.cols, a.key,
                                   raws, **({"where": a.where}
                                            if a.where else {}))
    rb = b.eng.neutral_rows_by_key(b.side, b.db, b.table, b.cols, b.key,
                                   raws, **({"where": b.where}
                                            if b.where else {}))
    cols, same = {}, 0
    for k in keys:
        x, y = ra.get(k), rb.get(k)
        if x is None or y is None:
            continue
        differ = [n for (n, c), u, (_, d), v in zip(a.cols, x, b.cols, y)
                  if canon.render_value(c, u) != canon.render_value(d, v)]
        if differ:
            cols[k] = differ
        else:
            same += 1
    return cols, same


# --- the sketch --------------------------------------------------------------

class Sketch:
    """An invertible Bloom lookup table of sum cells: `K` partitions of
    `w` cells, a row in one cell of each, picked by 20-bit slices of its
    hash. A cell is [count, key sum, hash sum, square sum]; subtraction is
    exact, so a row on both sides with one hash leaves nothing behind.

    The element is the row's hash with its key beside it - the key itself
    where it is one integer, else the key's hash - so a changed row decodes
    as the same key twice, once on each side."""

    def __init__(self, w, cells=None):
        self.w = int(w)
        self.cells = dict(cells or {})

    def at(self, rh):
        return [i * self.w + ((rh >> (_SLICE * i)) & _MASK) % self.w
                for i in range(K)]

    def add(self, c, key, rh):
        for i in self.at(rh):
            a = self.cells.get(i, (0, 0, 0, 0))
            self.cells[i] = (a[0] + c, a[1] + c * key, a[2] + c * rh,
                             a[3] + c * rh * rh)

    def minus(self, other):
        out = Sketch(self.w, self.cells)
        for i, (n, k, h, q) in other.cells.items():
            a = out.cells.get(i, (0, 0, 0, 0))
            out.cells[i] = (a[0] - n, a[1] - k, a[2] - h, a[3] - q)
        return out

    def totals(self):
        """(count, key sum, hash sum) of the table: every row is in exactly
        one cell of the first partition."""
        n = k = h = 0
        for i, (a, b, c, _) in self.cells.items():
            if i < self.w:
                n, k, h = n + a, k + b, h + c
        return n, k, h

    def _pure(self, i):
        n, k, h, q = self.cells.get(i, (0, 0, 0, 0))
        if n == 0 or h % n or k % n:
            return None
        rh = h // n
        if not 0 <= rh < (1 << BITS) or q != n * rh * rh or i not in \
                self.at(rh):
            return None
        return n, k // n, rh

    def decode(self):
        """([(count, key, row hash)], whether every cell peeled to
        empty). Counts other than one carry a row held more than once."""
        cells = dict(self.cells)
        work = Sketch(self.w)
        work.cells = cells
        out, todo = [], [i for i in cells if cells[i] != (0, 0, 0, 0)]
        while todo:
            i = todo.pop()
            got = work._pure(i)
            if got is None:
                continue
            n, key, rh = got
            out.append(got)
            for j in work.at(rh):
                a = cells.get(j, (0, 0, 0, 0))
                cells[j] = (a[0] - n, a[1] - n * key, a[2] - n * rh,
                            a[3] - n * rh * rh)
                if cells[j] != (0, 0, 0, 0):
                    todo.append(j)
        return out, all(v == (0, 0, 0, 0) for v in cells.values())


def sketch_width(expected):
    """Cells a partition for about `expected` differing rows: a changed row
    is two elements; peeling with three cells an element needs about 1.23
    cells an element, with room for a small sketch's bad luck."""
    elems = max(1, 2 * int(expected))
    need = int(elems * 1.5) + 24
    return max(16, -(-need // K))


def _int_key(s):
    return (len(s.key) == 1 and dict(s.cols).get(s.key[0]) == "integer")


def _sketch_sql(s, spec, w):
    if s.dialect:
        # a cell's squared lane passes what NUMBER(38), DECIMAL(38) and
        # HUGEINT hold within a few hundred rows: the leaves are read instead
        raise ValueError("the sums a warehouse keeps are too narrow for the"
                         " sketch's squared lane")
    e = s.engine
    kel = "k0" if _int_key(s) else ("kh" if s.key else "rh")
    inner = _inner(s, spec, keys=_int_key(s))
    # a table with no key: the row is its own key, selected once
    also = f", {kel}" if kel != "rh" else ""
    cells = [f"{i * w} + ((rh >> {_SLICE * i}) & {_MASK}) % {w}"
             for i in range(K)]
    if e == "postgres":
        named = ", ".join(f"{x} as c{i}" for i, x in enumerate(cells))
        each = ", ".join(f"(c{i})" for i in range(K))
        return (f"select c, count(*), sum({kel}::numeric), sum(rh::numeric),"
                f" sum(rh::numeric * rh) from"
                f" (select rh{also}, {named}"
                f" from ({inner} offset 0) x offset 0) y"
                f" cross join lateral (values {each}) v(c) group by c")
    pick = " ".join(f"when {i} then {x}" for i, x in enumerate(cells))
    parts = " union all ".join(f"select {i} as i" for i in range(K))
    return (f"select c, count(*), sum(cast({kel} as decimal(65,0))),"
            f" sum(cast(rh as decimal(65,0))),"
            f" sum(cast(rh as decimal(65,0)) * rh) from"
            f" (select /*+ NO_MERGE(x) */ rh{also}, case p.i {pick} end"
            " as c"
            f" from ({inner}) x cross join ({parts}) p) z group by c")


def sketch(s, spec, w):
    """One side's sketch, in one scan."""
    out = Sketch(w)
    if s.sql:
        for c, n, k, h, q in s.eng.run_rule(s.side, s.db,
                                            _sketch_sql(s, spec, w)):
            out.cells[int(c)] = (int(n), int(k), int(h), int(q))
        return out
    hs = Hasher(spec, [c for _, c in s.cols], s.key_at())
    at = s.key_at()
    for row in _rows(s):
        _, rh, kh = hs.of(row)
        out.add(1, int(row[at[0]]) if _int_key(s) else kh, rh)
    return out


def by_sketch(a, b, spec, expected):
    """The differing rows from the two sides' sketches, or (None, why)
    where the sketch does not decode or does not add up: `(Localized,
    None)` otherwise. Keys that are not one integer come back as their
    hashes and are read once more, by hash, to be named."""
    w = sketch_width(expected)
    sa, sb = sketch(a, spec, w), sketch(b, spec, w)
    diff = sa.minus(sb)
    got, whole = diff.decode()
    if not whole:
        return None, (f"the sketch of {K * w:,} cells did not peel to empty"
                      f" ({len(got):,} rows decoded)")
    out = Localized(how="sketch")
    ta, tb = sa.totals(), sb.totals()
    out.delta = (ta[0] - tb[0], ta[2] - tb[2], ta[1] - tb[1])
    found = (sum(n for n, _, _ in got), sum(n * rh for n, _, rh in got),
             sum(n * k for n, k, _ in got))
    out.found = found
    if found != out.delta:
        return None, "what the sketch decoded does not add up to the totals"
    if not a.key:
        for n, _, _ in got:
            if n > 0:
                out.only_src += n
            else:
                out.only_dst -= n
        out.complete = True
        return out, None
    plus = {k: rh for n, k, rh in got if n > 0}
    minus = {k: rh for n, k, rh in got if n < 0}
    if any(abs(n) != 1 for n, _, _ in got):
        return None, "a key decoded more than once on one side"
    if _int_key(a):
        cls = dict(a.cols)[a.key[0]]

        def name(k):
            return (canon.render_value(cls, k),), (k,)
    else:
        found_keys = _by_hash(a, spec, set(plus)) if plus else {}
        found_keys.update(_by_hash(b, spec, set(minus) - set(found_keys))
                          if minus else {})
        missing = (set(plus) | set(minus)) - set(found_keys)
        if missing:
            return None, (f"{len(missing)} decoded keys were not found"
                          " by their hash a moment later")

        def name(k):
            return found_keys[k]
    for k in plus:
        text, raw = name(k)
        out.raw[text] = raw
        (out.changed if k in minus else out.missing).append(text)
    for k in minus:
        if k not in plus:
            text, raw = name(k)
            out.raw[text] = raw
            out.extra.append(text)
    out.complete = True
    return out, None


def _by_hash(s, spec, hashes):
    """{key hash: (key text, raw key)} for these key hashes, one filtered
    read of the side."""
    out = {}
    if not hashes:
        return out
    cls = dict(s.cols)
    if not s.sql:
        h = Hasher(spec, [c for _, c in s.cols], s.key_at())
        at = s.key_at()
        for row in _rows(s):
            _, _, kh = h.of(row)
            if kh in hashes:
                raw = tuple(row[i] for i in at)
                out[kh] = (tuple(canon.render_value(cls[k], v)
                                 for k, v in zip(s.key, raw)), raw)
        return out
    order = sorted(hashes)
    names = ", ".join(f"k{j}" for j in range(len(s.key)))
    for i in range(0, len(order), IN_LIST):
        part = ", ".join(str(x) for x in order[i:i + IN_LIST])
        sql = (f"select kh, {names} from"
               f" {_fenced(s, _inner(s, spec, keys=True))} x"
               f" where kh in ({part})")
        for kh, *key in s.eng.run_rule(s.side, s.db, sql):
            raw = tuple(key)
            out[int(kh)] = (tuple(canon.render_value(cls[k], v)
                                  for k, v in zip(s.key, raw)), raw)
    return out


# --- the choice --------------------------------------------------------------

_N = decide.Need
_R = decide.Rung


def _chosen(name):
    return _N("the localizer climbed to", lambda f: f.get("localizer")
              in (None, name), "{rung} measured dearer here, or was not"
              " reached, where the localizer was climbed alone")


def _two_readings(f):
    return bool(f.get("two readings") or f.get("disagreed"))


PASSES = (
    decide.Part("prove", (
        _R("kept", "the leaves the tail kept up, only those touched lately"
                   " read again", gives=frozenset({"byte exact",
                                                   "under writes"}),
           needs=(_N("a proof kept beside the tail's position",
                     lambda f: bool(f.get("kept")),
                     "{rung} needs the hop to keep its proof (keep_proof)"
                     " and a tail whose position the kept leaves match"),),
           because="the proof kept as the data moved: only the leaves the"
                   " last changes touched are read",
           footprint=1,
           fits=lambda ch: ch["keep"].name == "kept" or
           "the kept leaves are the ones the tail keeps"),
        _R("scan", "one scan a side for the table's digest and every leaf",
           gives=frozenset({"byte exact", "keyless"}),
           because="every leaf of the digest comes out of the one scan"),
    )),
    decide.Part("leaves", (
        _R("range", "leaves by ranges of the integer key, read back by its"
                    " index", gives=frozenset({"index reads"}),
           needs=(_N("one integer key on both sides",
                     lambda f: f.get("key") == "integer",
                     "the key is not one integer on both sides, so ranges"
                     " of it are not the same rows in both"),
                  _N("both sides read by their index",
                     lambda f: bool(f.get("sql")),
                     "a side is read into this process whole, so a range"
                     " of its key is no cheaper to read back than a hash")),
           because="a range of an integer key holds the same rows on both"
                   " sides and is read back by its index"),
        _R("hash", "leaves by a hash of the key's canonical text",
           gives=frozenset({"keyless"}),
           because="the key's canonical text hashes alike on both sides,"
                   " whatever its type or collation"),
    )),
    decide.Part("localize", (
        _R("sketch", "a sketch of the differences, decoded here",
           needs=(_chosen("sketch"),
                  _N("few differences expected",
                     lambda f: f.get("expected") is not None
                     and 0 < f["expected"] <= SKETCH_MOST,
                     "{rung} needs few differences expected, and nothing"
                     " says how many there are"),
                  _N("sums wide enough for its squares",
                     lambda f: f.get("wide sums", True),
                     "a side's leaves are computed in a warehouse's SQL,"
                     " whose sums are too narrow for {rung}'s squares"),
                  _N("a link that is the limit",
                     lambda f: (f.get("rtt_ms") or 0) >= SLOW_RTT_MS,
                     "the link is not the limit (round trip under"
                     f" {SLOW_RTT_MS:g} ms), so reading the leaves costs"
                     " no more than the sketch")),
           prove=lambda f: f.get("sketch failed"),
           because="bytes in proportion to the differences, in one round"
                   " trip"),
        _R("leaf reads", "reading back only the leaves that differ",
           needs=(_chosen("leaf reads"),),
           because="only the leaves that differ are read, and each is held"
                   " to its digest"),
        _R("groups", "the groups whose aggregates differ when read a second"
                     " way, their rows read back and held to the digests",
           needs=(_chosen("groups"),
                  _N("the second reader installed for both engines",
                     lambda f: bool(f.get("row hash reader")),
                     "{rung} needs the second reader, and it is not"
                     " installed or does not read both engines"),
                  _N("a column the source's statistics say splits it",
                     lambda f: f.get("group column") is not None,
                     "no column of it is known from the source's"
                     " statistics to split it into groups, integer or"
                     " text - and only integer across two engines")),
           prove=lambda f: f.get("groups failed"),
           because="the second reader's aggregates per group rule out the"
                   " groups that agree, and only the others are read back"),
        _R("bisection", "the bisecting reader over every column, each row it"
                        " names read back and held to the digests",
           needs=(_chosen("bisection"),
                  _N("one engine on both sides",
                     lambda f: bool(f.get("same engine")),
                     "{rung} compares each engine's own text of a value,"
                     " which two engines write apart"),
                  _N("a key named alike on both sides",
                     lambda f: bool(f.get("same key")),
                     "{rung} needs the key under one name on both sides"),
                  _N("the bisecting reader installed for both engines",
                     lambda f: bool(f.get("bisection reader")),
                     "the bisecting reader is not installed, or does not"
                     " read both engines")),
           prove=lambda f: f.get("bisection failed"),
           because="measured the cheaper here, and what it names is held"
                   " to migkit's own digests"),
    )),
    decide.Part("confirm", (
        _R("generations", "what differs read again, generation by"
                          " generation, until it holds still",
           needs=(_N("the source written while it was read",
                     lambda f: bool(f.get("written")),
                     "the source's log did not move while the table was"
                     " read, or it keeps none to ask, so what differs is"
                     " not still being written"),),
           because="a source written while it is read differs in the"
                   " leaves still being written; each generation reads"
                   " again only what still differs, past the tail's"
                   " position where one runs, until it holds still"),
        _R("fenced", "held to the totals, then asked again past the tail's"
                     " position", needs=(_N("a tail running",
                                            lambda f: bool(f.get("tail")),
                                            "no tail runs, so there is no"
                                            " position to wait past"),),
           because="a difference still arriving through the tail is"
                   " waited for, not reported"),
        _R("totals", "held to the difference of the two tables' totals",
           because="the rows found must add up to the whole difference"),
    )),
    decide.Part("keep", (
        _R("kept", "leaves kept up by the tail between proofs",
           needs=(_N("a proof kept beside the tail's position",
                     lambda f: bool(f.get("kept")),
                     "{rung} needs the hop to keep its proof (keep_proof)"
                     " and a tail whose position the kept leaves match"),),
           footprint=1,
           because="each change moves the leaves it touches on both sides"),
        _R("again", "read again at the next proof",
           because="nothing is kept between proofs"),
    )),
    decide.Part("second", (
        _R("none", "no second reading",
           needs=(_N("no second reading called for",
                     lambda f: not _two_readings(f)
                     or not (f.get("row hash reader")
                             or (f.get("bisection reader")
                                 and f.get("localizer") != "bisection")),
                     "the hop asks for two readings, or migkit's own"
                     " digest disagreed"),),
           because="migkit's own digest is the proof"),
        _R("row hash", "a second reading by another reader's row hash",
           needs=(_N("two readings", _two_readings),
                  _N("the second reader installed for both engines",
                     lambda f: bool(f.get("row hash reader")),
                     "the second reader is not installed, or does not"
                     " read both engines")),
           because="an independent reading, through another library and"
                   " other SQL"),
        _R("bisection", "a second reading by bisection",
           fits=lambda ch: ch["localize"].name != "bisection" or (
               "the bisecting reader already localized the rows"),
           needs=(_N("two readings", _two_readings),
                  _N("the bisecting reader installed for both engines",
                     lambda f: bool(f.get("bisection reader")),
                     "the bisecting reader is not installed, or does not"
                     " read both engines")),
           because="an independent reading that bisects the key range"),
    )),
)


class _Localizer:
    """The localizing part's own measurements (`localize/<rung>`)."""

    def __init__(self, costs):
        self.costs = costs

    def get(self, name, default=None):
        if self.costs is None:
            return default
        return self.costs.get(f"localize/{name}", default)


def strategy(work, facts, costs=None, keep=None):
    """The passes for one table (`decide.compose` over `PASSES`).

    The localizer is climbed on its own first (`decide.climb`, on its own
    measured costs): the other passes cost what they cost whichever way
    the rows are read back, and a strategy is ranked by measurement only
    once every one of its parts has been timed - so the localizers would
    never be told apart by what they cost."""
    facts["localizer"] = None
    loc = decide.climb(work, PASSES[2].rungs, facts,
                       costs=_Localizer(costs))
    if loc.rung is not None:
        facts["localizer"] = loc.rung.name
    got = decide.compose(work, PASSES, facts, costs=costs, keep=keep)
    if got and loc.rung is not None:
        got.reasons["localize"] = loc.reason
    return got


def rtt_ms(eng, side, db, tries=3):
    """The least of a few `select 1` round trips to one side, in ms, on
    one connection where the engine keeps one: a statement's round trip,
    not a connection's setup - measured through a link holding 50 ms each
    way and back, a new connection a statement took 375-383 ms (the sign-in
    is several round trips)."""
    conn = None
    try:
        if eng.CANON_ENGINE == "postgres":
            conn = eng._conn(side, eng._d(side, db))
        elif eng.CANON_ENGINE == "mysql":
            conn = eng._conn(side)
    except Exception:  # noqa: BLE001 - timed through run_rule instead
        conn = None

    def ask():
        if conn is None:
            return eng.run_rule(side, db, "select 1")
        with conn.cursor() as cur:
            cur.execute("select 1")
            return cur.fetchall()
    best = None
    try:
        for _ in range(tries + (conn is not None)):
            began = time.monotonic()
            try:
                ask()
            except Exception:  # noqa: BLE001 - no reading, then
                return None
            took = (time.monotonic() - began) * 1000
            best = took if best is None else min(best, took)
    finally:
        if conn is not None:
            conn.close()
    return best


# --- a table that differs, localized ----------------------------------------

def sides(pair, db, src_t, dst_t, sc, dc, scope):
    """(source Side, target Side) of a pair, or None where the key cannot
    be lined up across it: a key outside the columns both render, or keyed
    on other columns on the target."""
    src, dst = pair.src_engine, pair.dst_engine
    try:
        key = list(src.neutral_key("src", db, src_t) or [])
        there = list(dst.neutral_key("dst", db, dst_t) or [])
    except Exception:  # noqa: BLE001 - the walk says why
        return None
    names = [n for n, _ in sc]
    mapped = []
    if key:
        if any(k not in names for k in key):
            return None
        mapped = [dc[names.index(k)][0] for k in key]
        if sorted(mapped) != sorted(there):
            return None
    return (Side(src, "src", db, src_t, list(sc), key, scope[0]),
            Side(dst, "dst", db, dst_t, list(dc), mapped, scope[1]))


def both(fn, a, b, *args):
    """fn on the two sides at once, each on its own connection."""
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(2) as pool:
        fa, fb = pool.submit(fn, a, *args), pool.submit(fn, b, *args)
        return fa.result(), fb.result()


def key_range(a, b):
    """(lowest, highest) integer key over both sides, or None."""
    got = []
    for s in (a, b):
        q = _q(s, s.key[0])
        rows = s.eng.run_rule(s.side, s.db, (
            f"select min({q}), max({q}) from"
            f" {s.eng._qualified(s.side, s.db, s.table)}"
            + (f" where ({s.where})" if s.where else "")))
        got += [int(v) for v in rows[0] if v is not None]
    return (min(got), max(got)) if got else None


def int_keyed(a, b):
    return (len(a.key) == 1 and dict(a.cols).get(a.key[0]) == "integer"
            and dict(b.cols).get(b.key[0]) == "integer")


def _facts(pair, db, a, b, digests, began=None):
    """What the passes' needs read for one table, each read once and only
    for a rung still in the running (`decide.Facts`): the round trip is
    timed only where few differences are expected, a reader looked for
    only where two readings are called for, and the source's log asked
    again only where its position before the digests was read (`began`):
    moved since, the source was written while it was read."""
    from . import second_reader, tailctl
    expected = None
    counts = [d[0] for d in digests] if digests else None
    if counts and counts[0] != counts[1]:
        expected = abs(int(counts[0]) - int(counts[1]))
    before = sum(len(pair._read_drill(db, pair._leaf(a.table), kind))
                 for kind in ("missing", "changed", "extra"))
    if before:
        expected = max(expected or 0, before)
    opts = pair.hop.options or {}
    engines = {pair.src_name, pair.dst_name}
    rows = max([int(d[0]) for d in digests] if digests else [0])
    return decide.Facts(
        {"key": "integer" if int_keyed(a, b) else ("other" if a.key
                                                   else None),
         "kept": False, "expected": expected, "disagreed": True,
         "sql": a.sql and b.sql,
         # a side whose leaves a warehouse's SQL computes cannot hold the
         # sketch's squared sums (`_sketch_sql`)
         "wide sums": not any(s.sql and s.dialect for s in (a, b)),
         "same engine": pair.src_name == pair.dst_name and bool(digests),
         "same key": bool(a.key) and a.key == b.key,
         "two readings": bool(opts.get("two_readings"))},
        rtt_ms=lambda: max(rtt_ms(a.eng, "src", db) or 0,
                           rtt_ms(b.eng, "dst", db) or 0),
        tail=lambda: tailctl.alive(pair.hop.report_dir(db)),
        written=lambda: _written(a, db, began),
        **{"row hash reader": lambda: bool(
            second_reader.interpreter()
            and engines <= set(second_reader.READER_TYPES)),
           "bisection reader": lambda: bisection_reader(engines),
           "group column": lambda: group_of(pair, db, a, b, rows)})


def bisection_reader(engines):
    """Whether the bisecting reader is here and reads every one of these
    engines - with its driver, without which it reads nothing and exits
    0 (measured)."""
    from .util import which
    engines = set(engines)
    return bool(which("reladiff") and engines <= set(URL_SCHEMES)
                and all(_driver(e) for e in engines))


def group_of(pair, db, a, b, rows):
    """(column, class, groups) the second reader's aggregates may split
    this table by (`second_reader.group_column`), from the source's
    statistics, or None: a column both sides call by one name, since the
    reader groups the two sides by it."""
    from . import second_reader
    try:
        cands = a.eng.group_candidates("src", db, a.table)
    except Exception:  # noqa: BLE001 - no statistics, no groups
        return None
    names = [n for n, _ in a.cols]
    theirs = [n for n, _ in b.cols]
    cands = [(c, cls, n) for c, cls, n in cands
             if c in names and theirs[names.index(c)] == c]
    return second_reader.group_column(cands, a.key,
                                      pair.src_name == pair.dst_name, rows)


def _written(a, db, began):
    """Whether the source's log moved since `began`, its position before
    the table was read: False where it keeps none to ask."""
    if began is None:
        return False
    try:
        now = a.eng.log_position("src", db)
    except Exception:  # noqa: BLE001 - nothing to tell by
        return False
    return now is not None and now != began


#: the module the bisecting reader reads each engine through: installed
#: without it, it says so and exits 0, having read nothing (measured)
DRIVERS = {"postgres": "psycopg2", "mysql": "mysql.connector"}


def _driver(engine):
    import importlib.util
    try:
        return importlib.util.find_spec(DRIVERS[engine]) is not None
    except (KeyError, ModuleNotFoundError, ValueError):
        return False


def drill(pair, db, src_t, dst_t, sc, dc, scope, rows, digests=None,
          began=None):
    """The clause naming the rows that differ in a table whose digest
    differs, found from its leaves - or None where the row walk is the
    way: a keyed table small enough to walk (`DRILL_CAP`) on a source not
    written while it was read, or a key that cannot be lined up across the
    pair. `began` is the source's log position before its digest was
    read: where it has moved since, what differs is read again by
    generations (`generations`) before a row is named, and a `Said` whose
    `settled` is true comes back where every leaf that differed settled."""
    got = sides(pair, db, src_t, dst_t, sc, dc, scope)
    if got is None:
        return None
    a, b = got
    name = pair._leaf(src_t)
    facts = _facts(pair, db, a, b, digests, began)
    if a.key and rows <= pair.DRILL_CAP and not facts["written"]:
        return None
    costs = decide.HopCosts(pair.hop, "verify passes/")
    strat = strategy(f"{db}.{name}", facts, costs)
    if not strat:
        return f"; rows not localised: {strat.why_not}"
    salt = pair.digest_salt()
    work = f"{db}.{name}"
    gens = None
    # a source written while it is read differs in leaves still being
    # written: those are read again by generations, which the sketch and
    # the bisecting reader cannot do, so the leaves localize
    live = strat.parts["confirm"].name == "generations"

    def by_leaves():
        nonlocal gens
        t0 = time.monotonic()
        kind = strat.parts["leaves"].name if a.key else "row"
        lohi = key_range(a, b) if kind == "range" else None
        if kind == "range" and lohi is None:
            kind = "hash"
        spec = plan(rows, kind, *(lohi or (None, None)), salt=salt)
        ta, tb = both(scan, a, b, spec)
        took = time.monotonic() - t0
        if live:
            gens = generations(a, b, spec, ta, tb,
                               _generation_wait(pair, db, a))
            if gens.all_settled:
                # nothing to name: an earlier run's lists go
                pair._write_drill(db, name, missing=[], changed=[], extra=[])
                out = Said(gens.said())
                out.settled = True
                out.rows_src, out.rows_dst = gens.ta.root[0], gens.tb.root[0]
                return out
            ta = Tree({x: v for x, v in gens.ta.leaves.items()
                       if x in gens.still})
            tb = Tree({x: v for x, v in gens.tb.leaves.items()
                       if x in gens.still})
        found = localize(a, b, spec, ta, tb)
        if not live:
            costs.saw("leaves/" + ("hash" if kind == "row" else kind),
                      max(rows, 1), took)
            costs.saw("localize/leaf reads", max(rows, 1),
                      time.monotonic() - t0)
        return found

    def run(choice):
        """One localizer. Where it cannot run or its rows do not add up it
        is `Blocked`, its fact set for the passes' own climb, and the next
        is taken (`decide.attempt`); what it found - differences or none -
        is the answer. The leaves read back are the last way, and a
        failure of theirs goes to the caller, which walks the rows."""
        way = choice.rung.name
        if way == "leaf reads":
            return by_leaves()
        t0 = time.monotonic()
        try:
            if way == "sketch":
                found, why = by_sketch(
                    a, b, plan(rows, "hash" if a.key else "row", salt=salt),
                    facts["expected"])
            elif way == "groups":
                found, why = by_groups(pair, db, a, b, digests, rows,
                                       facts["group column"])
            else:
                found, why = by_bisection(pair, db, a, b, digests)
        except Exception as e:  # noqa: BLE001 - a way that could not run
            if not decide.operational(e):
                raise
            found, why = None, (str(e).strip().splitlines()[-1][:120]
                                if str(e) else type(e).__name__)
        if found is None:
            facts[f"{way} failed"] = why
            raise decide.Blocked(why)
        costs.saw(f"localize/{way}", max(rows, 1), time.monotonic() - t0)
        return found
    if live:
        found = by_leaves()
    else:
        facts["localizer"] = None
        _, found = decide.attempt(work, PASSES[2].rungs, run, facts,
                                  costs=_Localizer(costs),
                                  switch=decide.only_blocked)
        # the rest of the passes, climbed with what the localizers said
        strat = strategy(work, facts, costs) or strat
    if isinstance(found, Said):
        return found
    return Said(_clause(pair, db, name, a, b, found, strat, rows, gens))


def _clause(pair, db, name, a, b, found, strat, rows=0, gens=None):
    how = (f"; found from a sketch of the differences ({found.differ:,}"
           " rows decoded)" if found.how == "sketch" else
           f"; {found.said} ({found.rows:,} rows read back)"
           if found.how == "groups" else
           f"; found by bisecting the key range ({found.rows:,} rows"
           " named, each read back and hashed)"
           if found.how == "bisection" else
           f"; found from {found.leaves:,} leaves that differ, read back"
           f" ({found.rows:,} rows)")
    whole = ("what was found adds up to the whole difference of the two"
             " tables' totals" if gens is None else
             "what was found adds up to what the leaves that held still"
             " differ by")
    if gens is not None and not gens.still:
        # every leaf still differing was still being written: no row is
        # named from leaves that would not hold still
        how = "; no row named: every leaf that differs was still moving"
    elif found.complete:
        how += "; " + whole
    else:
        how += f"; differs, not fully localized: {found.why}"
    if gens is not None:
        how += gens.said()
    cols, same = confirm(a, b, found) if found.changed else ({}, 0)
    if cols:
        first = next(iter(cols.values()))
        how += f"; the first changed row differs in {', '.join(first[:6])}"
    if same:
        how += (f"; {same:,} of the changed rows read equal by value a"
                " moment later - still arriving, or changed again")
    second = strat.parts.get("second")
    if second is not None and second.name != "none":
        how += second_reading(pair, db, a, b, second.name, found, rows)
    if not a.key:
        # nothing a repair can address by key: an earlier run's lists go
        pair._write_drill(db, name, missing=[], changed=[], extra=[])
        parts = []
        for n, where, other in ((found.only_src, "source", "target"),
                                (found.only_dst, "target", "source")):
            if n:
                parts.append(f"{n:,} row{'s' if n != 1 else ''} on the"
                             f" {where} that the {other} does not hold")
        body = "; ".join(parts) or (
            "no row named" if gens is not None and not gens.still
            else "no row differs by value")
        return (f"; {body} (the table has no key: rows are told apart by"
                " their values, and a row held twice counts twice)" + how)
    return pair._drill_clause(db, name, found.missing, found.changed,
                              found.extra, [], how)


def by_bisection(pair, db, a, b, digests):
    """(Localized, None), or (None, why): the rows the bisecting reader
    names as differing over every column, each read back by key on both
    sides and hashed as the table digest hashes it (canon's text, no
    salt), and all of them held to the difference of the two digests the
    first pass took - what it misses, or names that do not differ, fails
    the sum and the leaves are read instead."""
    if not digests:
        return None, "no digests to hold its rows to"
    p = _bisect_run(pair.hop, (pair.src_name, pair.dst_name), db, a.table,
                    b.table, a.key, a.where, ["--json"],
                    max(int(d[0]) for d in digests))
    import json
    named = set()
    for line in (p.stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("["):
            continue
        try:
            _, values = json.loads(line)
        except ValueError:
            continue
        if isinstance(values, list) and len(values) >= len(a.key):
            named.add(tuple(values[:len(a.key)]))
    if not named:
        return None, "the bisecting reader named no row"
    raws = sorted(named)
    scope = ({"where": a.where} if a.where else {},
             {"where": b.where} if b.where else {})
    ra = a.eng.neutral_rows_by_key(a.side, db, a.table, a.cols, a.key,
                                   raws, **scope[0])
    rb = b.eng.neutral_rows_by_key(b.side, db, b.table, b.cols, b.key,
                                   raws, **scope[1])
    plain = Spec("hash")
    ha = Hasher(plain, [c for _, c in a.cols], a.key_at())
    hb = Hasher(plain, [c for _, c in b.cols], b.key_at())
    out = Localized(how="bisection")
    out.rows = len(named)
    total = (0, 0)
    for text in sorted(set(ra) | set(rb)):
        x, y = ra.get(text), rb.get(text)
        rx = ha.of(x)[1] if x is not None else None
        ry = hb.of(y)[1] if y is not None else None
        key = x if x is not None else y
        at = a.key_at() if x is not None else b.key_at()
        out.raw[text] = tuple(key[i] for i in at)
        if y is None:
            out.missing.append(text)
            total = (total[0] + 1, total[1] + rx)
        elif x is None:
            out.extra.append(text)
            total = (total[0] - 1, total[1] - ry)
        elif rx != ry:
            out.changed.append(text)
            total = (total[0], total[1] + rx - ry)
    out.found = total
    out.delta = (int(digests[0][0]) - int(digests[1][0]),
                 int(digests[0][1] or 0) - int(digests[1][1] or 0))
    if total != out.delta:
        return None, ("the rows it named do not add up to the difference of"
                      " the two digests")
    out.complete = True
    return out, None


#: the address schemes the bisecting reader takes, by engine
URL_SCHEMES = {"postgres": "postgresql", "mysql": "mysql"}


def _url(engine, ep, db):
    from urllib.parse import quote
    return (f"{URL_SCHEMES[engine]}://{quote(ep.user, safe='')}:"
            f"{quote(ep.password or '', safe='')}@{ep.host}:{ep.port}/{db}")


def _bisect_run(hop, names, db, src_table, dst_table, key, where, extra,
                rows=None):
    """One run of the bisecting reader over every column of one table, its
    bisection sized by migkit (`verifiers.reladiff_rung`): both addresses
    in an owner-only file for the run, never on its command line."""
    from .util import PrivateFile, diff_run_config, run
    from .verifiers import reladiff_rung
    rung = reladiff_rung(rows, None, 2, names)
    conf = diff_run_config(
        _url(names[0], hop.source, db), src_table,
        _url(names[1], hop.target, hop.target_db(db)), dst_table)
    # every column, not only the key: without `-c %` the bisecting reader
    # compares keys alone - measured, a million rows with four changed,
    # three missing and three extra came back as "0 rows updated"
    cmd = ["reladiff", "--conf", None, "-j", str(rung["threads"]),
           "--bisection-factor", str(rung["bisection_factor"]),
           "--bisection-threshold", str(rung["bisection_threshold"]),
           "-c", "%"] + list(extra)
    for k in key:
        cmd += ["-k", k]
    if where:
        cmd += ["--where", where]
    with PrivateFile(conf, ".toml") as path:
        cmd[2] = path
        return run(cmd, check=False, timeout=3600)


def bisection_reading(hop, names, db, src_table, dst_table, key, where=None,
                      rows=None):
    """A clause with the bisecting reader's own count of what differs in
    one table - an independent reading, added to the verdict."""
    from .engines.generic import GenericEngine
    p = _bisect_run(hop, names, db, src_table, dst_table, key, where,
                    ["--stats"], rows)
    got = GenericEngine._stats(GenericEngine, p)
    if not got:
        return "; the second reading by bisection gave no numbers"
    return (f"; read a second way by bisection: {got['only_a']:,} rows"
            f" only on the source, {got['only_b']:,} only on the"
            f" target, {got['updated']:,} changed")


def second_reading(pair, db, a, b, how, found=None, rows=0):
    """A clause with an independent reading of one table: `row hash`
    through the second reader's row validation, held key for key to the
    rows found (`Engine._read_rows_twice`), `bisection` through the
    bisecting reader. What either says is added to the verdict, never put
    in its place: the table already differs by migkit's own digest."""
    pair.__dict__.setdefault("_read_twice", set()).add((db, str(a.table)))
    if how == "row hash":
        from . import second_reader
        mine = []
        if found is not None:
            for text in found.missing + found.changed + found.extra:
                raw = found.raw.get(text, ())
                mine.append(tuple("" if v is None else str(v) for v in raw))
        cls = dict(a.cols)
        both_ = [(n, c) for (n, c), (m, _) in zip(a.cols, b.cols) if n == m]
        try:
            fixed = [n for n, t in a.eng.neutral_columns("src", db, a.table)
                     if second_reader.padded(t)]
        except Exception:  # noqa: BLE001 - none left out then
            fixed = []
        got = pair._read_rows_twice(db, a.table, b.table, a.key,
                                    [cls.get(k) for k in a.key], mine,
                                    rows, both_, fixed)
        if got is None:
            return ""
        if got.status in ("error", "skip"):
            return f"; not read a second way: {got.detail}"[:400]
        return f"; {got.detail}"[:400]
    if how == "bisection":
        return bisection_reading(pair.hop, (pair.src_name, pair.dst_name),
                                 db, a.table, b.table, a.key, a.where, rows)
    return ""


def by_groups(pair, db, a, b, digests, rows, group):
    """(Localized, None), or (None, why): the second reader's aggregates
    per group of `group` (the source's statistics chose it), failures
    only; the rows of the groups that differ read back on both sides as
    the leaves read theirs, and held to the difference of the two digests
    the first pass took. A difference the groups' measures cannot see - a
    text changed to another of the same length moves no count, sum or
    length - leaves the sum short, and the leaves are read instead."""
    from . import second_reader
    if not digests:
        return None, "no digests to hold its rows to"
    got = second_reader.narrow(
        pair.hop, db, (pair.src_name, pair.dst_name),
        (pair._reader_table("src", db, a.table),
         pair._reader_table("dst", db, b.table)),
        group, where=(a.where, b.where), keep=pair.hop.report_dir(db))
    if not isinstance(got, second_reader.Narrowing):
        return None, got
    sa = Side(a.eng, a.side, a.db, a.table, a.cols, a.key,
              got.inside(a.eng, a.where))
    sb = Side(b.eng, b.side, b.db, b.table, b.cols, b.key,
              got.inside(b.eng, b.where))
    # one leaf holding every row the predicate selects, hashed as the
    # table's digest hashes it (canon's text, no salt)
    plain = Spec("hash")
    fa, fb = both(leaf_rows, sa, sb, plain, [0])
    out = Localized(how="groups", said=got.said())
    out.leaves = len(got.values) + int(got.nulls)
    out.read = out.leaves
    out.rows = len(fa.keyed) + len(fb.keyed) + sum(
        n for n, _ in fa.kept.values()) + sum(n for n, _ in fb.kept.values())
    total = compare(fa, fb, out)
    out.found = total[:2]
    out.delta = (int(digests[0][0]) - int(digests[1][0]),
                 int(digests[0][1] or 0) - int(digests[1][1] or 0))
    if out.found != out.delta:
        return None, ("the rows of the groups that differ do not add up to"
                      " the difference of the two digests - something"
                      " differs where the groups' measures cannot see")
    out.complete = True
    # the second reading of its rows reads only these groups
    pair.__dict__.setdefault("_narrowed", {})[(db, str(a.table))] = (
        got, got.said())
    return out, None
