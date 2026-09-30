"""The proof kept up as the data moves (backlog R20.8; incremental
multiset hashing, docs/research/diff-algorithms-2026-09-28.md section 8).

The digest is a sum of row hashes, so it can be kept instead of read
again: a change that turns row `old` into row `new` moves its leaf's sum
by `h(new) - h(old)`. Where a hop turns `keep_proof` on, the tail keeps
every leaf of every table it carries (`leaves`) on both sides, beside its
position:

* **seeded** once, when the tail first saves a position with nothing kept:
  the target's leaves read with the tail holding between batches (so they
  are the target at that position exactly), the source's under its marks
  (`Engine.marked_rule`). Not from the table copier's own range digests:
  a range is read in batches, each under a snapshot of its own, while its
  mark is taken once before the first (`ranges.started`) - a change
  between the mark and a later batch is in what was copied and not under
  the mark, so the tail applies it again and a number kept from those
  digests would count it twice, on both sides alike, where nothing would
  ever show it;
* **kept up by the tail** in the step that saves each position
  (`Keeper.saved`, called just before the position is written): the
  source's side from the changes as the log gave them - less the old
  row's hash from its before image, plus the new row's from its after
  image, for every change the leaf's marks do not already hold - and the
  target's from the changes as they were written. A change whose image
  does not carry every column compared (no whole before image: REPLICA
  IDENTITY DEFAULT, a binlog row image that is not FULL, a value the tail
  writes as what it added), or that the marks cannot place, marks its
  leaf "to read again" instead;
* **exact with batches**: the leaves, the position they stand at, the one
  before it and the last batch's own additions are committed in one
  SQLite transaction before the tail writes its position. A stop between
  the two leaves the kept leaves one batch ahead, and the next start
  takes that batch back out (`Keeper.__init__`); anything else out of
  step marks every leaf to read again;
* **read cold** in the background (`Keeper.cold`, every
  `MIGKIT_PROOF_COLD_SECONDS`): the leaves proved longest ago are read
  again on both sides - the target's while the tail holds, so a row
  written there by anything but the tail is seen - and the next
  generation begins;
* **the proof** (`prove`): at a position the tail has passed and holds at,
  the two kept numbers of every leaf are compared, and only the leaves
  that differ, are marked, were touched in this generation or were proved
  longer ago than `MIGKIT_PROOF_OLDEST_SECONDS` are read again, under the
  same fence; what still differs is localized from those leaves.

What the kept numbers prove, and what they do not: a leaf's two numbers
are the source and the target as each was last read, moved since by
exactly what the stream carried. A write to the target that the stream
never carried is seen by the next cold read of its leaf, not before -
which is why every leaf is read again within the oldest-proof bound.

Measured (`bench/proof.py`, a PostgreSQL 16 pair in a 2-CPU VM):
* the keeping, in this process alone: 20,000 updates carried on both
  sides in batches of 2,000 in 0.40s, 20 us a change, then 0.14s, 7.2 us
  a change, once a row the tail writes as it read it is hashed once for
  both sides (`Keeper.hashed`);
* a tail of 200,000 changes (inserts, then an update of each): 46,689
  changes a second without the kept proof, 32,207 with it. That run's
  table had a timestamp column, whose value the slot hands over as its
  text, and every leaf a change touched was marked to read again rather
  than kept - the value is now typed first (`twoway._typed`), and the
  rate with every leaf kept is not measured yet;
* the final proof of 1,000,000 rows after 10,000 scattered updates, a
  pass, then 100 more: 1.46s from the kept leaves (903 of 1,000 proved by
  their numbers, 97 read again by the key's index), against 1.74s for
  the table digest read in full on both sides and 2.56s for the one-scan
  tree. The full read grows with the table (12.6s at 5,000,000 rows); the
  kept proof with the last window's leaves. The seed read both sides once
  in 4.0s.
"""
import json
import os
import sqlite3
import threading
import time

from . import canon, leaves

FILE = "kept-leaves.sqlite"
#: classes compared in the server's own text of a value (`canon.OWN`, and
#: JSON as one engine normalises it): a row image from the log, rendered
#: here, is not that text
OPAQUE = frozenset({canon.OWN, canon.JSON_ONE})


def _env(name, default):
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return float(default)


def wanted(hop):
    """Whether the hop keeps its proof between checks."""
    return bool((hop.options or {}).get("keep_proof"))


def _tok(token):
    return json.dumps(token, sort_keys=True, default=str)


class Store:
    """The kept leaves in one SQLite file beside the tail's position:
    `meta` (salt, position, the one before it, generation), `tables` (how
    each table's leaves are made, its columns and key on both sides),
    `leaves` (both sides' three numbers, the source's marks, when it was
    last read, last touched, and whether it must be read again) and
    `last` (the last batch's additions, to take back out)."""

    def __init__(self, path):
        self.path = str(path)
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False,
                                    isolation_level=None)
        self.lock = threading.Lock()
        c = self.conn
        c.execute("pragma journal_mode=wal")
        c.execute("pragma synchronous=full")
        c.execute("create table if not exists meta (k text primary key,"
                  " v text)")
        c.execute("create table if not exists tables (name text primary"
                  " key, v text)")
        c.execute("create table if not exists leaves (name text, b integer,"
                  " v text, primary key (name, b))")
        c.execute("create table if not exists last (name text, b integer,"
                  " v text, primary key (name, b))")

    def get(self, k, default=None):
        row = self.conn.execute("select v from meta where k = ?",
                                (k,)).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, k, v):
        self.conn.execute("insert into meta values (?, ?) on conflict (k)"
                          " do update set v = excluded.v", (k, json.dumps(v)))

    def tables(self):
        return {n: json.loads(v) for n, v in
                self.conn.execute("select name, v from tables")}

    def leaves(self, name, buckets=None):
        if buckets is None:
            rows = self.conn.execute("select b, v from leaves where name = ?",
                                     (name,))
            return {int(b): _leaf(v) for b, v in rows}
        out = {}
        order = sorted(buckets)
        for i in range(0, len(order), 500):
            part = order[i:i + 500]
            rows = self.conn.execute(
                f"select b, v from leaves where name = ? and b in"
                f" ({', '.join('?' * len(part))})", [name] + part)
            out.update({int(b): _leaf(v) for b, v in rows})
        return out

    def write(self, name, got):
        self.conn.executemany(
            "insert into leaves values (?, ?, ?) on conflict (name, b)"
            " do update set v = excluded.v",
            [(name, int(b), _text(v)) for b, v in got.items()])

    def begin(self):
        self.conn.execute("begin immediate")

    def commit(self):
        self.conn.execute("commit")

    def rollback(self):
        self.conn.execute("rollback")


#: a leaf: [source count, sum, key sum, target count, sum, key sum,
#: source mark before, mark after, generation read, generation touched,
#: to be read again]
S, D, LO, HI, PROVED, TOUCHED, STALE = 0, 3, 6, 7, 8, 9, 10


def _new():
    return [0, 0, 0, 0, 0, 0, None, None, -1, -1, 1]


def _empty(t):
    """A leaf the store holds no numbers for: read empty on both sides
    when its table was seeded, under the seed's marks."""
    return [0, 0, 0, 0, 0, 0, t.lo, t.hi, t.gen, -1, 0]


def _leaf(text):
    v = json.loads(text)
    for i in range(6):
        v[i] = int(v[i])
    return v


def _text(v):
    return json.dumps([str(x) for x in v[:6]] + list(v[6:]))


class Table:
    """How one kept table's rows become leaves on each side."""

    def __init__(self, name, d):
        self.name = name
        self.src, self.dst = d["src"], d["dst"]
        self.spec = leaves.Spec.of(d["spec"])
        self.sc = [tuple(x) for x in d["sc"]]
        self.dc = [tuple(x) for x in d["dc"]]
        self.key, self.dkey = list(d["key"]), list(d["dkey"])
        self.lo, self.hi = d.get("lo"), d.get("hi")
        self.filtered = bool(d.get("filtered"))
        # a column compared in the server's own text (one engine, one
        # type), which a value from the log rendered here is not: kept by
        # reading its leaves again, never by numbers worked out here
        self.opaque = any(c in OPAQUE for _, c in self.sc + self.dc)
        self.gen = int(d.get("gen", 0))
        self.same = self.sc == self.dc
        self.hs = leaves.Hasher(self.spec, [c for _, c in self.sc],
                                [[n for n, _ in self.sc].index(k)
                                 for k in self.key])
        self.hd = leaves.Hasher(self.spec, [c for _, c in self.dc],
                                [[n for n, _ in self.dc].index(k)
                                 for k in self.dkey])

    def sides(self, pair, db):
        scope = pair._row_scope(db, self.src)
        return (leaves.Side(pair.src_engine, "src", db, self.src, self.sc,
                            self.key, scope[0]),
                leaves.Side(pair.dst_engine, "dst", db, self.dst, self.dc,
                            self.dkey, scope[1]))

    def bucket(self, target, key):
        """The leaf of a row by its key's values (a dict)."""
        names, hs = (self.dkey, self.hd) if target else (self.key, self.hs)
        cols = self.dc if target else self.sc
        cls = dict(cols)
        vals = [key[k] for k in names]
        kh = leaves.text_hash(self.spec.salt, _encoded(
            [cls[k] for k in names], vals))
        return self.spec.bucket(kh, vals[0])

    def hashed(self, target, image):
        """(row hash, key hash) of a whole row image, or None where the
        image lacks a column compared or holds a value the tail writes as
        what it added."""
        cols = self.dc if target else self.sc
        if image is None or any(n not in image for n, _ in cols):
            return None
        # a slot's text as the class it is, as the two-way check reads
        # it (`twoway._typed`): a timestamp printed by the log and one
        # read by a driver are one value
        from .twoway import _typed
        values = [_typed(c, image[n]) for n, c in cols]
        if any(isinstance(v, canon.Added) for v in values):
            return None
        try:
            _, rh, kh = (self.hd if target else self.hs).of(values)
        except (TypeError, ValueError, AttributeError):
            # a value in the log's own text that the rendering does not
            # take as it takes the driver's: read again, never guessed
            return None
        return rh, kh


def _encoded(classes, values):
    from . import render, rowtext
    return rowtext.encode([render.renderer(c)(v)
                           for c, v in zip(classes, values)])


def _images(c, key_names):
    """[(sign, row image or None, the row's key)] of one change."""
    op, key = c["op"], dict(c["key"])
    values = c.get("values") or {}
    if op == "insert":
        return [(1, values, {k: values.get(k, key.get(k))
                             for k in key_names})]
    new = {k: values.get(k, key.get(k)) for k in key_names}
    old = [(-1, c.get("before"), key)]
    if op == "delete":
        return old
    return old + [(1, values, new)]


class Keeper:
    """The tail's side of the kept proof: `read` with every batch as read,
    `saved` in the step that saves its position."""

    def __init__(self, pair, db, token_path, token, log):
        self.pair, self.db, self.log = pair, db, log
        self.where = token_path.parent
        self.store = Store(self.where / FILE)
        self.batch, self.hashed, self.unplaced = [], {}, set()
        self.cold_at = time.monotonic()
        self._tables = None
        self._align(token)

    # --- in step with the position ------------------------------------------

    def _align(self, token):
        st = self.store
        at, prev = st.get("token"), st.get("prev")
        if not st.tables() or at == _tok(token):
            return
        if prev == _tok(token):
            # committed here, and the position was never written: the
            # batch will be read and applied again, so its additions go
            back = {}
            for name, b, v in st.conn.execute("select name, b, v from last"):
                back.setdefault(name, {})[int(b)] = _leaf(v)
            st.begin()
            for name, got in back.items():
                have = st.leaves(name, got)
                for b, d in got.items():
                    leaf = have.get(b, _new())
                    for i in range(6):
                        leaf[i] -= d[i]
                    have[b] = leaf
                st.write(name, have)
            st.conn.execute("delete from last")
            st.put("token", _tok(token))
            st.put("prev", None)
            st.commit()
            self.log("kept proof: the last batch was kept before its"
                     " position was saved; taken back out, it is applied"
                     " again")
            return
        self._all_stale(token, "the kept leaves stand at another position"
                               " than the tail's")

    def _all_stale(self, token, why):
        st = self.store
        st.begin()
        for name in st.tables():
            got = st.leaves(name)
            for leaf in got.values():
                leaf[STALE] = 1
            st.write(name, got)
        st.conn.execute("delete from last")
        st.put("token", _tok(token))
        st.put("prev", None)
        st.commit()
        self.log(f"kept proof: {why} - every leaf is read again at the next"
                 " proof")

    # --- the tail's calls ----------------------------------------------------

    def read(self, changes):
        """The batch as the source's log gave it, before anything is left
        out or mapped: what the source's leaves move by."""
        self.batch = list(changes)

    def saved(self, applied, token, before):
        """The tail is about to save `token`, having written `applied`
        since `before`: the leaves moved by the batch, committed with the
        position they now stand at."""
        st = self.store
        tables = self.tables()
        if not st.get("seeded"):
            self.seed(token)
            self.batch = []
            return
        if st.get("token") != _tok(before):
            # a batch committed here and read again (its position was not
            # written), or a position moved without the kept leaves
            self._align(before)
        gen = int(st.get("generation", 0))
        moves, stale = {}, set()
        by_src = {self.pair._leaf(t.src): t for t in tables.values()}
        by_dst = {t.dst: t for t in tables.values()}
        by_dst.update({self.pair._leaf(t.dst): t for t in tables.values()})
        src = self.pair.src_engine
        marks = {}
        # {id of a row image: its hashes}, for the batch: the tail writes
        # the image it read wherever the hop maps nothing
        self.hashed, self.unplaced = {}, set()
        for c in self.batch:
            t = by_src.get(self.pair._leaf(c["table"]))
            if t is not None:
                self._move(t, c, False, moves, stale, marks, src)
        for c in applied:
            t = by_dst.get(c["table"])
            if t is not None:
                self._move(t, c, True, moves, stale, marks, src)
        self._commit(moves, stale, gen, token, before)
        self.batch, self.hashed = [], {}
        if time.monotonic() - self.cold_at >= _env(
                "MIGKIT_PROOF_COLD_SECONDS", 300):
            self.cold()

    def _move(self, t, c, target, moves, stale, marks, src):
        names = t.dkey if target else t.key
        for sign, image, key in _images(c, names):
            if t.filtered or t.opaque:
                got = None
            elif target and t.same and id(image) in self.hashed:
                # the row as written is the row as read: its hash is the
                # one the source's side already took
                got = self.hashed[id(image)]
            else:
                got = t.hashed(target, image)
                if not target and got is not None:
                    self.hashed[id(image)] = got
            try:
                b = (t.spec.bucket(got[1], key.get(names[0]))
                     if got is not None else t.bucket(target, key))
            except (KeyError, TypeError, ValueError, AttributeError):
                # a row no leaf can be named for: every leaf of its table
                # is read again, rather than one kept wrong
                self.unplaced.add(t.name)
                continue
            at = (t.name, b)
            if at in stale:
                continue
            if got is None:
                stale.add(at)
                continue
            if not target:
                if at not in marks:
                    leaf = self.store.leaves(t.name, [b]).get(b)
                    marks[at] = ((leaf[LO], leaf[HI]) if leaf is not None
                                 else (t.lo, t.hi))
                lo, hi = marks[at]
                first = src.mark_covers("src", self.db, lo, c) \
                    if lo is not None else None
                if first is True:
                    continue
                last = src.mark_covers("src", self.db, hi, c) \
                    if hi is not None else None
                if last is not False:
                    stale.add(at)
                    continue
            rh, kh = got
            m = moves.setdefault(at, [0] * 6)
            o = D if target else S
            m[o] += sign
            m[o + 1] += sign * rh
            m[o + 2] += sign * kh

    def _commit(self, moves, stale, gen, token, before):
        st = self.store
        by = {}
        for (name, b) in set(moves) | stale:
            by.setdefault(name, set()).add(b)
        with st.lock:
            st.begin()
            try:
                st.conn.execute("delete from last")
                for name in self.unplaced:
                    got = st.leaves(name)
                    for leaf in got.values():
                        leaf[STALE] = 1
                    st.write(name, got)
                for name, bs in by.items():
                    have = st.leaves(name, bs)
                    t = self.tables()[name]
                    for b in bs:
                        leaf = have.get(b) or _empty(t)
                        m = moves.get((name, b))
                        if m:
                            for i in range(6):
                                leaf[i] += m[i]
                            st.conn.execute(
                                "insert into last values (?, ?, ?)",
                                (name, b, _text(m + [None] * 5)))
                        if (name, b) in stale:
                            leaf[STALE] = 1
                        leaf[TOUCHED] = gen
                        have[b] = leaf
                    st.write(name, have)
                st.put("prev", _tok(before))
                st.put("token", _tok(token))
                st.commit()
            except BaseException:
                st.rollback()
                raise

    # --- seeding and reading again ----------------------------------------

    def tables(self):
        if self._tables is None:
            self._tables = {n: Table(n, d)
                            for n, d in self.store.tables().items()}
        return self._tables

    def seed(self, token):
        """Every carried table's leaves read on both sides, the target's
        while the tail holds at `token`."""
        pair, db, st = self.pair, self.db, self.store
        began = time.monotonic()
        salt = st.get("salt") or pair.digest_salt()
        pairs, _, _, _ = pair.match_tables(
            pair.src_engine.neutral_tables("src", db),
            pair.dst_engine.neutral_tables("dst", db), pair._rename)
        made = {}
        for src_t, dst_t in pairs:
            try:
                sc, dc, _ = pair._comparable_columns(
                    db, pair.src_engine, src_t, pair.dst_engine, dst_t)
            except Exception:  # noqa: BLE001 - not kept, said below
                continue
            got = leaves.sides(pair, db, src_t, dst_t, sc, dc,
                               pair._row_scope(db, src_t)) if sc else None
            if got is None or not got[0].key:
                continue
            a, b = got
            kind = layout(a, b)
            lohi = leaves.key_range(a, b) if kind == "range" else None
            rows = _rows(pair, db, a)
            spec = leaves.plan(rows, kind if lohi else "hash",
                               *(lohi or (None, None)), salt=salt)
            made[pair._leaf(src_t)] = {
                "src": src_t, "dst": dst_t, "spec": spec.as_dict(),
                "span": list(lohi) if lohi else None,
                "sc": sc, "dc": dc, "key": a.key, "dkey": b.key,
                # a row filter is SQL the server evaluates: whether a
                # changed row is inside it is not known here
                "filtered": bool(a.where or b.where)}
        gen = int(st.get("generation", 0))
        filled = {}
        for name, d in made.items():
            t = Table(name, d)
            a, b = t.sides(pair, db)
            tb = leaves.scan(b, t.spec)
            ta, lo, hi = leaves.scan_marked(a, t.spec)
            got = {}
            for x in set(ta.leaves) | set(tb.leaves) | set(
                    _planned(t.spec, d.get("span"))):
                leaf = _new()
                leaf[0:3] = ta.leaves.get(x, (0, 0, 0))
                leaf[3:6] = tb.leaves.get(x, (0, 0, 0))
                leaf[LO], leaf[HI] = lo, hi
                leaf[PROVED], leaf[STALE] = gen, 0
                got[x] = leaf
            filled[name] = got
            # a leaf with no row on either side was read empty under the
            # same marks: a row arriving in it is held to them too
            d["lo"], d["hi"], d["gen"] = lo, hi, gen
        with st.lock:
            st.begin()
            try:
                st.conn.execute("delete from tables")
                st.conn.execute("delete from leaves")
                st.conn.execute("delete from last")
                for name, d in made.items():
                    st.conn.execute("insert into tables values (?, ?)",
                                    (name, json.dumps(d)))
                    st.write(name, filled[name])
                st.put("salt", salt)
                st.put("seeded", True)
                st.put("generation", gen)
                st.put("gen_at", {str(gen): time.time()})
                st.put("token", _tok(token))
                st.put("prev", None)
                st.commit()
            except BaseException:
                st.rollback()
                raise
        self._tables = None
        self.log(f"kept proof: {len(made)} tables' leaves read on both"
                 f" sides in {time.monotonic() - began:.1f}s; kept up by"
                 " the tail from here")

    def cold(self, most=None):
        """A background pass: the next generation begins, and the leaves
        proved longest ago (and those marked to read again) are read on
        both sides - the target's while the tail holds, so a row written
        there by anything but the tail shows. Returns {table: leaves whose
        target read differs from what was kept}."""
        most = int(most or _env("MIGKIT_PROOF_COLD_LEAVES", 256))
        st = self.store
        self.cold_at = time.monotonic()
        gen = next_generation(st)
        picked = []
        for t in self.tables().values():
            for b, leaf in st.leaves(t.name).items():
                picked.append((0 if leaf[STALE] else 1, leaf[PROVED],
                               t.name, b))
        picked.sort()
        by = {}
        for _, _, name, b in picked[:most]:
            by.setdefault(name, set()).add(b)
        for t in self.tables().values():
            # past the ranges kept, where a row the tail never carried
            # would have no leaf to be found in: by the key's index
            by.setdefault(t.name, set()).update(
                _beyond(self.pair, self.db, st, t))
        seen = {}
        for name, bs in by.items():
            t = self.tables()[name]
            seen[name] = reread(self.pair, self.db, st, t, bs, gen)
            if seen[name]:
                self.log(f"kept proof: {len(seen[name])} leaves of {name}"
                         " read on the target differ from what the tail"
                         " kept - written there by something else; the"
                         " next proof names the rows")
        return seen


def next_generation(st):
    """A new generation begun, when kept: the leaves the stream touches
    from here are this generation's."""
    with st.lock:
        st.begin()
        gen = int(st.get("generation", 0)) + 1
        at = st.get("gen_at", {})
        at[str(gen)] = time.time()
        st.put("gen_at", {k: v for k, v in at.items()
                          if int(k) > gen - 1000})
        st.put("generation", gen)
        st.commit()
    return gen


def layout(a, b):
    """How a kept table's rows fall into leaves: the passes' `leaves`
    part (`leaves.PASSES`) - ranges of one integer key read back by its
    index, else a hash of the key."""
    facts = {"key": "integer" if leaves.int_keyed(a, b) else "other",
             "sql": a.sql and b.sql, "kept": True}
    return leaves.strategy(a.table, facts).parts["leaves"].name


def _beyond(pair, db, st, t):
    """Range leaves with rows on either side beyond the lowest and highest
    leaf kept; none for leaves by hash, where every one is kept."""
    if t.spec.kind != "range":
        return set()
    have = [b for b in st.leaves(t.name)]
    if not have:
        return set()
    a, b = t.sides(pair, db)
    edge = (min(have), max(have))
    found = set()
    for s in (a, b):
        tree, _, _ = leaves.scan_marked(s, t.spec, marked=False,
                                        outside=edge)
        found |= set(tree.leaves)
    return found


def _planned(spec, span):
    """Every leaf of a table's plan: 2^bits of a hash, the ranges between
    the lowest and highest key of a range - so the cold read comes to the
    empty ones as well, where a row written behind the tail's back
    would be."""
    if spec.kind == "range":
        if not span:
            return []
        return range(spec.bucket(0, span[0]), spec.bucket(0, span[1]) + 1)
    return range(1 << spec.bits)


def _rows(pair, db, a):
    try:
        got = (a.eng.table_facts("src", db).get(a.table) or {}).get("rows")
        if got:
            return int(got)
    except Exception:  # noqa: BLE001 - counted instead
        pass
    return int(a.eng.neutral_digest("src", db, a.table, a.cols,
                                    **({"where": a.where} if a.where
                                       else {}))[0])


def reread(pair, db, st, t, buckets, gen):
    """These leaves of `t` read again on both sides and kept as read: the
    target's as it is now, the source's with its marks. Returns the leaves
    whose target read differed from the number kept (not marked)."""
    a, b = t.sides(pair, db)
    tb = leaves.scan(b, t.spec, buckets)
    ta, lo, hi = leaves.scan_marked(a, t.spec, buckets)
    z = (0, 0, 0)
    moved = []
    with st.lock:
        st.begin()
        try:
            have = st.leaves(t.name, buckets)
            for x in buckets:
                leaf = have.get(x) or _new()
                fresh_d = tb.leaves.get(x, z)
                if not leaf[STALE] and tuple(leaf[3:6]) != fresh_d:
                    moved.append(x)
                leaf[0:3] = ta.leaves.get(x, z)
                leaf[3:6] = fresh_d
                leaf[LO], leaf[HI] = lo, hi
                leaf[PROVED], leaf[STALE] = gen, 0
                have[x] = leaf
            st.write(t.name, have)
            st.commit()
        except BaseException:
            st.rollback()
            raise
    return moved


def usable(pair, db):
    """The kept store for this hop and database where it is kept and has
    tables, else None."""
    if not wanted(pair.hop):
        return None
    path = pair.hop.report_dir(db) / FILE
    if not path.exists():
        return None
    st = Store(path)
    return st if st.tables() else None


def prove(pair, db, log=None, timeout=300, rounds=2):
    """{source table leaf: (status, detail, source rows, target rows)} for
    every kept table, proved from the kept leaves at a position the tail
    has passed and holds at - or None where the kept proof cannot stand
    here: none kept, no tail running to fence on, or the leaves out of step
    with its position.

    The leaves read again are read with the tail holding, the target's
    exactly at its position and the source's under its marks. A leaf that
    still differs is asked `rounds` more times, each after the tail has
    passed where the source is then - a change still on its way is waited
    for, not reported (pass C); what differs after that is localized from
    those leaves only."""
    from . import tailctl
    st = usable(pair, db)
    if st is None:
        return None
    where = pair.hop.report_dir(db)
    if not tailctl.alive(where):
        return None
    say = log or (lambda m: None)
    began = time.monotonic()
    tables = {n: Table(n, d) for n, d in st.tables().items()}
    oldest = _env("MIGKIT_PROOF_OLDEST_SECONDS", 86400)
    bad, why, out = {}, {}, {}
    for turn in range(rounds + 1):
        if not _fence(pair, db, where, timeout):
            return None
        try:
            token = pair._saved_token(pair._tail_path(db))
            if st.get("token") != _tok(token):
                return None
            gen = int(st.get("generation", 0))
            for name, t in tables.items():
                if turn == 0:
                    why[name] = _due(st, t, gen, oldest)
                    again = set().union(*why[name].values())
                else:
                    again = set(bad.get(name) or ())
                if again:
                    reread(pair, db, st, t, again, gen)
                kept = st.leaves(t.name, again) if again else {}
                bad[name] = sorted(
                    b for b in again
                    if tuple(kept[b][0:3]) != tuple(kept[b][3:6]))
            if turn == rounds or not any(bad.values()):
                for name, t in tables.items():
                    out[name] = _verdict(pair, db, st, t, why[name],
                                         bad[name])
                next_generation(st)
                break
        finally:
            tailctl.resume(where)
    say(f"kept proof: {len(out)} tables in"
        f" {time.monotonic() - began:.1f}s")
    return out


def _fence(pair, db, where, timeout):
    """The tail past where the source's log is now, and holding there -
    never where something else already holds it: its pause is its own to
    lift."""
    from . import tailctl
    if (where / tailctl.PAUSE).exists():
        return False
    try:
        fence = pair.src_engine.log_position("src", db)
    except Exception:  # noqa: BLE001 - no fence, no kept proof
        return False
    if fence is None or not pair.fence_wait(db, fence, timeout):
        return False
    return tailctl.pause(where, timeout)


def _due(st, t, gen, oldest):
    """{why: leaves} of `t` to read again at a proof: numbers that differ,
    marked to read again, touched in this generation, or last read longer
    ago than `oldest` seconds."""
    at = {int(k): v for k, v in (st.get("gen_at") or {}).items()}
    now = time.time()
    why = {"differ": set(), "marked": set(), "touched": set(), "old": set()}
    for b, leaf in st.leaves(t.name).items():
        if leaf[STALE]:
            why["marked"].add(b)
        elif tuple(leaf[0:3]) != tuple(leaf[3:6]):
            why["differ"].add(b)
        elif leaf[TOUCHED] >= gen:
            why["touched"].add(b)
        elif now - at.get(leaf[PROVED], 0) > oldest:
            why["old"].add(b)
    return why


def _verdict(pair, db, st, t, why, bad):
    kept = st.leaves(t.name)
    again = set().union(*why.values())
    n_src = sum(leaf[0] for leaf in kept.values())
    n_dst = sum(leaf[3] for leaf in kept.values())
    words = {"differ": "whose kept numbers differed", "marked": "marked",
             "touched": "touched since the last pass",
             "old": "last read too long ago"}
    said = (f"{len(kept) - len(again):,} of {len(kept):,} leaves proved by"
            " the numbers kept as the data moved, read again: "
            + ", ".join(f"{len(v):,} {words[k]}" for k, v in why.items()
                        if v)
            if again else f"all {len(kept):,} leaves proved by the numbers"
            " kept as the data moved, none read again")
    if not bad:
        return ("ok", f"rows {n_src:,} and every compared column equal;"
                      f" {said}", n_src, n_dst)
    a, b = t.sides(pair, db)
    ta = leaves.Tree({x: tuple(kept[x][0:3]) for x in bad})
    tb = leaves.Tree({x: tuple(kept[x][3:6]) for x in bad})
    found = leaves.localize(a, b, t.spec, ta, tb)
    clause = pair._drill_clause(
        db, pair._leaf(t.src), found.missing, found.changed, found.extra,
        [], f"; found from {len(bad):,} kept leaves that differ"
            + ("; what was found adds up to what those leaves differ by"
               if found.complete else
               f"; differs, not fully localized: {found.why}"))
    head = (f"rows src={n_src:,} dst={n_dst:,}" if n_src != n_dst else
            f"rows {n_src:,} match but {len(bad):,} leaves do not")
    return ("diff", head + clause + f"; {said}", n_src, n_dst)


def proved_db(eng, db, stream=None, with_counts=False):
    """The data check of a same-engine hop from its kept leaves, as one
    verdict for the database (and its row counts first, where the check
    merges them), where the tail keeps them for every table the check
    compares and every one of them is proved; None otherwise - the
    engine's own pass then reads the tables and names what differs."""
    from .engines.base import Result
    if not wanted(eng.hop):
        return None
    pair = eng._as_pair()
    got = prove(pair, db, log=stream)
    if not got:
        return None
    pairs, _, _, _ = pair.match_tables(
        pair.src_engine.neutral_tables("src", db),
        pair.dst_engine.neutral_tables("dst", db), pair._rename)
    names = {pair._leaf(s_) for s_, _ in pairs}
    if not names or not names <= set(got) or any(
            got[n][0] != "ok" for n in names):
        return None
    rows = sum(got[n][2] for n in names)
    out = [Result("data", db, "ok",
                  f"{len(names)} tables, {rows:,} rows, proved by the"
                  " numbers kept as the data moved; "
                  + "; ".join(f"{n}: {got[n][1]}" for n in sorted(names))
                  [:600], str(pair.hop.report_dir(db) / FILE))]
    if with_counts:
        there = sum(got[n][3] for n in names)
        out.insert(0, Result("counts", db, "ok" if rows == there else "diff",
                             f"{len(names)} tables, rows {rows:,}=={there:,}"
                             " (from the kept leaves, no scan)"))
    return out
