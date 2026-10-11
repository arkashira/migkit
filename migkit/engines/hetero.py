import json
import re
import sys
import time

from .. import fencing, pieces, tailtables
from ..decide import Rung
from .base import Engine, RepairAction, Result


def _digests_ranges(engine):
    """Whether a digest of a key range is the server's to answer: an
    engine with a SQL dialect, or a warehouse, whose digest is computed in
    its SQL where the table's columns allow (`DIGESTS_IN_SERVER`)."""
    return bool(engine.SQL_DIALECT
                or getattr(engine, "DIGESTS_IN_SERVER", False))


#: new objects between two young passes of the collector while a tail runs
GC_YOUNG = 50_000
_GC = {"tails": 0, "was": None}


def _batch_gc():
    """The garbage collector's passes, for as long as a tail applies: what
    the process held before the tail began is frozen out of them, and a
    young pass waits for `GC_YOUNG` new objects rather than 700. A batch is
    tens of thousands of change records and rows, freed by their counts as
    it is applied and never in a cycle, and every pass walked them:
    measured, the 320,000 changes of `bench/tail_rates.py` applied into
    PostgreSQL in 4.3s with the collector as it was and 2.7s with it held
    (3.3s and 2.0s of CPU). Returns what puts the collector back, once the
    last tail of the process is done."""
    import gc
    import threading
    lock = _GC.setdefault("lock", threading.Lock())
    with lock:
        if not _GC["tails"]:
            _GC["was"] = gc.get_threshold()
            gc.freeze()
            gc.set_threshold(GC_YOUNG, *_GC["was"][1:])
        _GC["tails"] += 1

    def back():
        with lock:
            _GC["tails"] -= 1
            if not _GC["tails"]:
                gc.set_threshold(*_GC["was"])
                gc.unfreeze()
    return back


class _Masks:
    """A source read as the target is to hold it: every row it hands the
    pair - a batch the copier writes, a row a check, a repair or a delta
    reads by key, the fold its digest stands for - with each column the hop
    masks taken through its transform (`Hop.masks`, `transforms.apply`).
    So the copier writes masked values, and every comparison holds the
    target to the source's values as masked rather than calling each such
    row different or leaving the column out. Mixed into the source
    engine's own class by `masking`; a table the hop does not mask reads
    as it always did, and nothing is changed on the target side.

    The four readers call one another in some engines (a batch read as
    reads), so a value is taken through its transform once: by the
    outermost of them (`_held`)."""

    def _held(self):
        import threading
        local = self.__dict__.get("_mask_local")
        if local is None:
            local = self.__dict__.setdefault("_mask_local", threading.local())
        return local

    def _inside(self):
        return getattr(self._held(), "depth", 0) > 0

    def _enter(self):
        held = self._held()
        held.depth = getattr(held, "depth", 0) + 1

    def _leave(self):
        held = self._held()
        held.depth -= 1

    def _mask_at(self, db, table, columns):
        """[(position, rule)] of the masked columns among `columns`
        ([(name, class)] or names), [] where none is masked."""
        names = [str(c[0] if isinstance(c, (tuple, list)) else c)
                 for c in columns]
        rules = self.hop.masks(db, str(table), columns=names)
        return [(i, rules[n]) for i, n in enumerate(names) if n in rules]

    def _mask_sql(self, columns, at):
        """{column: SQL} of each masked column as its transform makes it,
        in this engine's SQL - or None where its digest takes no computed
        column (`DIGESTS_COMPUTED`) or its SQL cannot compute one of them
        alike (`transforms.sql`)."""
        from .. import rowfilter, transforms
        if not getattr(self, "DIGESTS_COMPUTED", False):
            return None
        dialect = rowfilter.dialect_of(self.CANON_ENGINE)
        key = self.hop.mask_key()
        out = {}
        for i, rule in at:
            c = columns[i]
            name = str(c[0] if isinstance(c, (tuple, list)) else c)
            got = transforms.sql(rule, self._quote_ident(name), dialect, key)
            if got is None:
                return None
            out[name] = got
        return out

    def _masked_rows(self, at, rows):
        from .. import transforms
        if not at:
            return rows
        key = self.hop.mask_key()
        out = []
        for row in rows:
            got = list(row)
            for i, rule in at:
                got[i] = transforms.apply(rule, got[i], key)
            out.append(got if isinstance(row, list) else tuple(got))
        return out

    def neutral_read(self, side, db, table, columns, *args, **kw):
        if side != "src" or self._inside():
            return super().neutral_read(side, db, table, columns, *args, **kw)
        self._enter()
        try:
            rows, last = super().neutral_read(side, db, table, columns,
                                              *args, **kw)
        finally:
            self._leave()
        return self._masked_rows(self._mask_at(db, table, columns),
                                 rows), last

    def neutral_rows_by_key(self, side, db, table, columns, *args, **kw):
        if side != "src" or self._inside():
            return super().neutral_rows_by_key(side, db, table, columns,
                                               *args, **kw)
        self._enter()
        try:
            got = super().neutral_rows_by_key(side, db, table, columns,
                                              *args, **kw)
        finally:
            self._leave()
        at = self._mask_at(db, table, columns)
        if not at:
            return got
        return {k: self._masked_rows(at, [v])[0] for k, v in got.items()}

    def neutral_batches(self, side, db, table, columns, *args, **kw):
        if side != "src" or self._inside():
            yield from super().neutral_batches(side, db, table, columns,
                                               *args, **kw)
            return
        at = self._mask_at(db, table, columns)
        batches = super().neutral_batches(side, db, table, columns, *args,
                                          **kw)
        while True:
            self._enter()
            try:
                batch = next(batches, None)
            finally:
                self._leave()
            if batch is None:
                return
            yield self._masked_rows(at, batch)

    def neutral_digest(self, side, db, table, columns, where=None):
        """A masked table's digest as the target's would be: summed by the
        server over each masked column as its transform makes it, in the
        server's own SQL (`transforms.sql`, where the engine's digest takes
        a computed column and its SQL computes every transform alike);
        else its rows, masked, folded here as every engine that digests in
        this process folds them (`canon.fold_rows`). Never the server's
        digest of the values as they are."""
        at = self._mask_at(db, table, columns) \
            if side == "src" and not self._inside() else []
        if not at:
            return super().neutral_digest(side, db, table, columns,
                                          where=where)
        computed = self._mask_sql(columns, at)
        if computed is not None:
            return super().neutral_digest(side, db, table, columns,
                                          where=where, computed=computed)
        from .. import canon
        n, total = 0, 0
        classes = [c for _, c in columns]
        for batch in self.neutral_batches(
                "src", db, table, columns, 5000,
                **({"where": where} if where else {})):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        return n, total

    def masks_table(self, db, table):
        """Whether the hop masks a column of this table of the source:
        what a reading in the server's own SQL - a tree of digests, a copy
        the servers pass between them, buckets of rows' hashes - is not
        taken for, the server reading the values as they are."""
        return bool(self.hop.masks(db, str(table)))


#: {an engine's class: its class reading as the target is to hold it}
_MASKING = {}


def _own_class(engine):
    """The engine's own class, under the one `masking` gives it."""
    cls = type(engine)
    return cls.__mro__[2] if issubclass(cls, _Masks) else cls


def masking(engine, hop):
    """`engine` - a pair's source - reading as the target is to hold it
    where `hop` masks columns (`_Masks`), else as it is."""
    from ..movers import _mask_said
    if not _mask_said(hop) or isinstance(engine, _Masks):
        return engine
    base = type(engine)
    cls = _MASKING.get(base)
    if cls is None:
        cls = _MASKING[base] = type(base.__name__, (_Masks, base),
                                    {"__module__": base.__module__})
    engine.__class__ = cls
    return engine


class HeteroEngine(Engine):
    """A hop whose two sides are different database engines.

    One driver per side, built from the same registry every other hop uses,
    so the pair is configuration rather than code. What used to be here was a
    single hand-written mysql-to-postgres pipeline that refused every other
    pairing outright; the comparison below is now written once against the
    neutral contract in `migkit.engines.base` and the rendering in
    `migkit.canon`, which is what makes a second pair cost nothing.

    Verification happens **inside each server**. Both sides fold their rows
    into one number using the same function over the same canonical text, and
    only the two numbers meet - so the size of the table has nothing to do
    with the amount of data on the wire. A validator that fetches both sides'
    rows into a middle box to compare them pays for the table twice, and pays
    again every time it re-runs.

    Moving data, converting DDL and tailing changes are still the
    mysql-to-postgres paths they always were, and say so when asked for
    another pair. Comparing correctly for every pair first is deliberate:
    a mover nobody can verify is the thing this tool exists to argue against.
    """

    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = True
    # the in-process binlog tail below needs this driver; without it
    # `move --mode cdc` uses migkit's streaming pipeline instead
    tail_requires = "pymysqlreplication"

    #: large values, for any pair (`pieces`): each side sizes its rows
    #: where they lie, each side's read of a batch ends at
    #: `pieces.BATCH_BYTES` or one row and each side's own pieces carry a
    #: value past a piece; a written batch is read back and compared by
    #: digest texts, a wide value by its length and md5 - the text both
    #: servers' digests hold it to
    LARGE_VALUES = pieces.Way(
        "batches cut by bytes on both sides, values past a piece read and"
        " written in pieces where each side can, wide values compared by"
        " their length and md5")
    #: what one worker of the pair's copier holds (`pieces.PAIR_MEMORY`)
    COPY_MEMORY = pieces.PAIR_MEMORY
    #: what the pair keeps of its tables' shape (`forget_shapes`): with the
    #: engine's own, each row filter's translation and each key's classes
    #: (`_row_scope`, `_key_classes`)
    SHAPE_CACHES = Engine.SHAPE_CACHES + ("_scopes",)

    def __init__(self, hop):
        super().__init__(hop)
        self.src_name = hop.options.get("source_engine", "mysql")
        self.dst_name = hop.options.get("target_engine", "postgres")
        for role, name in (("source_engine", self.src_name),
                           ("target_engine", self.dst_name)):
            if name == "hetero":
                raise SystemExit(f"{role} cannot be 'hetero'")
        from . import engine_named
        # read as the target is to hold it, where the hop masks columns
        self.src_engine = masking(engine_named(self.src_name, hop), hop)
        self.dst_engine = engine_named(self.dst_name, hop)
        # the mover, the DDL conversion and the binlog tail below are still
        # written for this one pair; they are reached through these names and
        # refuse anything else rather than pretending
        self.my = self.src_engine if self.src_name == "mysql" else None
        self.pg = self.dst_engine if self.dst_name == "postgres" else None
        # a target that keeps no counter apart from its rows has nothing
        # for the sequence check to ask
        if not hasattr(self.dst_engine, "sequences_behind"):
            self.checks = tuple(c for c in type(self).checks
                                if c != "autoinc")
        # only a MySQL source needs the binlog driver; saying otherwise sent
        # the CLI down its fallback path for pairs that never wanted it
        if self.src_engine.CANON_ENGINE != "mysql":
            self.tail_requires = ""

    #: its tail writes each change through the hop's transforms
    #: (`_masked_change`), and its repair of rows reads its source as
    #: masked (`_Masks`)
    TAILS_THROUGH_PAIR = True
    REPAIRS_MASKED = True

    def copier_masks(self):
        """The pair's copier writes through the hop's transforms (`_Masks`)."""
        return True

    def copier_filters(self):
        """The pair's copier reads the filter in each side's SQL
        (`_row_scope`), where both sides take one."""
        return bool(self.src_engine.filter_dialect()
                    and self.dst_engine.filter_dialect())

    def _masks(self, db, src_t):
        """Whether the hop masks a column of this source table: a way the
        servers take between them, reading the values as they are, is not
        taken for it (`_Masks.masks_table`)."""
        said = getattr(self.src_engine, "masks_table", None)
        return bool(said and said(db, src_t))

    #: the classes whose value is text, which a constant, a hash or a cut
    #: may be written into (`movers.mask_refused`)
    TEXT_CLASSES = ("text", "own text", "uuid")

    def masks_refused(self, db):
        """[(table.column, why)] of the hop's masking of `db` that no way
        can apply as asked (`movers.mask_refused`), held to the source's own
        listing: its keys and the columns of a foreign key on either end,
        its columns, and the text ones. Asked before anything is written,
        by the move, the tail and a repair alike."""
        from .. import canon, movers
        rules, keys, columns, text = {}, {}, {}, {}
        src = self.src_engine
        for t in src.neutral_tables("src", db):
            got = self.hop.masks(db, t)
            if not got:
                continue
            rules[t] = got
            cols = dict(src.neutral_columns("src", db, t))
            columns[t] = set(cols)
            text[t] = {n for n, typ in cols.items() if canon.type_class(
                src.CANON_ENGINE, typ) in self.TEXT_CLASSES}
            keys[t] = set(src.neutral_key("src", db, t) or ())
        if rules:
            for child, cc, parent, pc in src.foreign_keys("src", db) or ():
                for t, cs in ((child, cc), (parent, pc)):
                    if t in rules:
                        keys[t].update(cs)
        return movers.mask_refused(self.hop, rules, keys, columns, text)

    def _masked_change(self, db, change):
        """A change with each column the hop masks taken through its
        transform (`transforms.apply`), as the copy writes the rows: the
        values written, and the image before it, which the target holds
        masked. A key is never masked (`masks_refused`)."""
        if not self.hop.masks(db, change["table"]):
            return change
        from .. import transforms
        key = self.hop.mask_key()
        out = dict(change)
        for part in ("values", "before"):
            if change.get(part):
                rules = self.hop.masks(db, change["table"],
                                       columns=list(change[part]))
                out[part] = {n: transforms.apply(rules[n], v, key)
                             if n in rules else v
                             for n, v in change[part].items()}
        return out

    def lend_log(self, log):
        """Lent to both sides too: their ways switch as they write and
        read for the pair."""
        super().lend_log(log)
        for eng in (self.src_engine, self.dst_engine):
            lend = getattr(eng, "lend_log", None)
            if lend is not None:
                lend(log)

    def _mysql_to_postgres_only(self, what):
        if self.my is None or self.pg is None:
            raise SystemExit(
                f"hetero {self.src_name}->{self.dst_name}: {what} is still"
                " written for mysql->postgres only. `check` works on this"
                " pair; this does not.")

    def databases(self):
        return self.src_engine.databases()

    def in_scope(self, db, table):
        """A table as the pair names it - the source's name, or the leaf
        its drilldown is filed under (`_leaf`) - taken where the source's
        own listing, which reads the hop's scope, holds it: `orders` read
        as itself missed an `include` of `appdb.public.orders`. A hop that
        leaves nothing out reads every table, as it always did."""
        from ..config import scoped
        if not scoped(self.hop):
            return super().in_scope(db, table)
        name = str(table)
        return any(name in (t, self._leaf(t))
                   for t in self.src_engine.neutral_tables("src", db))

    # ---- comparing, for any pair ---------------------------------------

    @staticmethod
    def _leaf(identifier):
        """The table's own name, without whatever qualifies it.

        MySQL answers `orders`; PostgreSQL answers `public.orders`. Matching
        on the last component is what lets the two lists meet without either
        engine having to know the other exists.
        """
        return str(identifier).split(".")[-1]

    def _rename(self, identifier, db=None):
        """This source table's name on the target, from the hop's mapping.

        One place, so the pairing and the message explaining it cannot
        disagree about which rename applied. Asked with the database the
        table is in, so a rule written `appdb.public.orders` - the only
        way to say which of two databases' `orders` is renamed - is read
        here as it is everywhere else; without it such a rule was passed
        over, and the table compared against the name it no longer has.
        Unmapped, the name comes back as it was given.
        """
        parts = [p for p in str(identifier).split(".") if p]
        if db in (None, ""):
            return self.hop.target_table(*parts)
        return self.hop.target_table_in(db, *parts)

    def _renamer(self, db):
        """`_rename` for the tables of one database, as `match_tables`
        takes it."""
        return lambda identifier: self._rename(identifier, db)

    @staticmethod
    def match_tables(src_ids, dst_ids, rename=None):
        """(pairs, src_only, dst_only, ambiguous) by unqualified name.

        A name that appears twice on one side - the same table in two schemas -
        is returned as ambiguous rather than resolved. Picking one would
        compare a table against a namesake and report the verdict as if it
        were about the one the operator meant.

        `rename` is the hop's mapping, as a callable from a source id to
        the name that table has on the target. Given one, a mapped table is
        paired with what it was renamed *to* rather than with its namesake -
        which is the difference between verifying a transformed target and
        reporting every renamed table as missing. Without one, nothing
        changes: the leaf match below is what every hop uses today.

        A rename pointing at a table the target does not have is returned
        in `src_only`, because that is what it is - it is neither paired
        with its old namesake (which would verify a table nobody asked
        about) nor dropped. Saying *why* it is missing is the caller's
        job.
        """
        def index(ids):
            out = {}
            for i in ids:
                out.setdefault(HeteroEngine._leaf(i), []).append(i)
            return out
        pairs, mapped_missing = [], []
        if rename:
            dst_by_leaf = index(dst_ids)
            dst_exact = {str(i) for i in dst_ids}
            taken_s, taken_d = set(), set()
            for sid in sorted(src_ids):
                want = rename(sid)
                if not want or str(want) == str(sid):
                    continue
                hit = None
                if str(want) in dst_exact:
                    hit = str(want)
                else:
                    same = dst_by_leaf.get(HeteroEngine._leaf(want), [])
                    if len(same) == 1:
                        hit = same[0]
                taken_s.add(sid)
                if hit is not None and hit not in taken_d:
                    taken_d.add(hit)
                    pairs.append((sid, hit))
                else:
                    # named a target that is not there. It must not fall
                    # back to its namesake - the operator said this table
                    # lives somewhere else, and pairing it with the old
                    # name would verify a table nobody asked about - and it
                    # must not vanish either, which is what the first
                    # version of this did.
                    mapped_missing.append(sid)
            src_ids = [i for i in src_ids if i not in taken_s]
            dst_ids = [i for i in dst_ids if i not in taken_d]
        s, d = index(src_ids), index(dst_ids)
        ambiguous = sorted(
            [v[0] for v in s.values() if len(v) > 1]
            + [v[0] for v in d.values() if len(v) > 1])
        src_only, dst_only = list(mapped_missing), []
        for leaf in sorted(set(s) | set(d)):
            sv, dv = s.get(leaf, []), d.get(leaf, [])
            if len(sv) > 1 or len(dv) > 1:
                continue
            if sv and dv:
                pairs.append((sv[0], dv[0]))
            elif sv:
                src_only.append(sv[0])
            else:
                dst_only.append(dv[0])
        return pairs, src_only, dst_only, ambiguous

    def _column_plan(self, src_table, dst_table, db, how=None):
        """The base's column plan, over this hop's two engines."""
        return self._comparable_columns(db, self.src_engine, src_table,
                                        self.dst_engine, dst_table, how)

    def check_schema(self, db, only=None):
        """Whether the two sides hold the same columns at all.

        This engine had no schema check, and the row comparison reports a
        column that exists on only one side as a footnote on a green
        verdict. Measured, a SQLite source with `secret` that the PostgreSQL
        target does not have:

            OK  main.items
                rows 2 and every compared column equal across
                sqlite/postgres (digest ...)
                columns only on the source, not compared: secret

        The sentence is true - every *compared* column was equal - and a
        migration that dropped a whole column passed. Nobody reading a green
        report has a reason to look at the tail.

        The comparison is by column name and by the class each side's
        declared type renders to, because two engines never spell a type the
        same way and the class is the only thing they can both be held to.
        """
        res = []
        src_ids = self.src_engine.neutral_tables("src", db)
        dst_ids = self.dst_engine.neutral_tables("dst", db)
        pairs, src_only, dst_only, _ = self.match_tables(src_ids, dst_ids,
                                                         self._renamer(db))
        if only is not None:
            # the tables a same-engine hop leaves to this comparison; the
            # others are its own comparers' to report
            want = {self._leaf(t) for t in only}
            pairs = [p for p in pairs if self._leaf(p[0]) in want]
            src_only = [t for t in src_only if self._leaf(t) in want]
            dst_only = []
        for t in src_only:
            res.append(Result("schema", f"{db}.{self._leaf(t)}", "diff",
                              "this table is on the source and not on the"
                              " target, so none of its rows were compared",
                              "", "create it on the target, then run the"
                                  " check again"))
        for t in dst_only:
            res.append(Result("schema", f"{db}.{self._leaf(t)}", "diff",
                              "this table is on the target and not on the"
                              " source - nothing put it there as part of"
                              " this hop", "",
                              "confirm it belongs before the cutover"))
        for src_t, dst_t in pairs:
            scope = f"{db}.{self._leaf(src_t)}"
            try:
                def declared(engine, side, table):
                    return {n: engine.canonical_type(side, db, t) for n, t
                            in engine.neutral_columns(side, db, table)}
                got = self._classify_columns(
                    self.src_engine,
                    self._mapped_types(db, src_t, declared(
                        self.src_engine, "src", src_t))[0],
                    self.dst_engine, declared(self.dst_engine, "dst", dst_t))
            except Exception as e:
                res.append(Result("schema", scope, "error",
                                  f"cannot read the columns:"
                                  f" {str(e).splitlines()[-1][:120]}", "",
                                  "both sides have to be readable before"
                                  " anything can be compared"))
                continue
            res.append(self._schema_result(
                scope, got, self._rules_drift(db, src_t, dst_t)))
        return res

    def _rules_drift(self, db, src_t, dst_t):
        """What the target does not keep of the source's column rules - a
        column the engine numbers, a default, NOT NULL - which is what the
        application meets on its first insert after cutover. Only where
        both engines can say (`neutral_column_rules`); a default is
        compared by being there, because the same default is spelled
        differently in two dialects."""
        try:
            s = self.src_engine.neutral_column_rules("src", db, src_t)
            d = self.dst_engine.neutral_column_rules("dst", db, dst_t)
        except Exception:
            return []
        if not s or not d:
            return []
        _, back = self._mapped_types(db, src_t, {n: None for n in s})
        out = []
        for target, source in sorted(back.items()):
            a, b = s.get(source), d.get(target)
            if not a or not b:
                continue
            if a.get("identity") and not b.get("identity"):
                out.append(f"{target} is numbered by the source and not by"
                           " the target - an insert that leaves it out is"
                           " refused there")
            if (a.get("default") is not None and not a.get("identity")
                    and b.get("default") is None):
                out.append(f"{target} defaults to {a['default']} on the"
                           " source and to nothing on the target - an insert"
                           " that leaves it out stores NULL")
            if a.get("null") is False and b.get("null") is True:
                out.append(f"{target} is NOT NULL on the source and takes"
                           " NULL on the target")
            if a.get("null") is True and b.get("null") is False:
                out.append(f"{target} takes NULL on the source and not on"
                           " the target - a NULL the source holds is"
                           " refused")
        try:
            forward = {v: k for k, v in back.items()}
            want = {frozenset(forward[c] for c in cols)
                    for _, unique, cols, plain in
                    self.src_engine.neutral_indexes("src", db, src_t)
                    if unique and plain and cols
                    and all(c in forward for c in cols)}
            have = {frozenset(cols) for _, unique, cols, _ in
                    self.dst_engine.neutral_indexes("dst", db, dst_t)
                    if unique}
        except Exception:
            want = have = set()
        for cols in sorted(want - have, key=sorted):
            out.append(f"unique over ({', '.join(sorted(cols))}) on the"
                       " source and not on the target - a duplicate the"
                       " source refuses is taken")
        return out

    def _schema_result(self, scope, got, drift=()):
        """`got` is what `_classify_columns` returned; `drift` is
        `_rules_drift`. Kept apart from the reading so every shape can be
        exercised without two servers."""
        parts = list(drift)[:4]
        if got["only_src"]:
            parts.append(f"{len(got['only_src'])} columns the target does"
                         " not have, so their values were not carried and"
                         " are not compared: "
                         + ", ".join(got["only_src"][:6]))
        if got["only_dst"]:
            parts.append(f"{len(got['only_dst'])} columns only the target"
                         " has: " + ", ".join(got["only_dst"][:6]))
        from .. import canon
        drift = [f"{n} is {got['src_types'][n]} on the source and"
                 f" {got['dst_types'][n]} on the target, which are not the"
                 " same kind of value"
                 for n, s, d in got["pairs"] if s is not d
                 and not canon.bson_alike(s, d, got["src_types"][n],
                                          got["dst_types"][n])]
        parts += drift[:4]
        blind = got["unreadable"]
        if parts:
            tail = ("; " + f"{len(blind)} further columns could not be read"
                           " on one side or the other, so they are not"
                           " compared either: "
                    + ", ".join(n for n, _ in blind[:4])) if blind else ""
            return Result("schema", scope, "diff", "; ".join(parts) + tail,
                          "", "align the two sides' columns before trusting"
                              " any row verdict for this table")
        if blind:
            return Result(
                "schema", scope, "warn",
                f"{len(got['pairs'])} columns match on both sides, and"
                f" {len(blind)} could not be compared across these two"
                " engines (" + ", ".join(n for n, _ in blind[:4])
                + ") - the row check does not look at those", "",
                "compare those columns by hand, or accept that they are"
                " outside what this hop verifies")
        return Result("schema", scope, "ok",
                      f"{len(got['pairs'])} columns, same names and the same"
                      " kind of value on both sides")

    def _neutral_rows(self, db, table=None, stream=None):
        """One (scope, status, detail, src_rows, dst_rows) per table.

        The single computation behind both `check_counts` and `check_data`:
        the digest query returns the count alongside the checksum, so asking
        twice would be two answers taken at two different moments about the
        same question.
        """
        src_ids = self.src_engine.neutral_tables("src", db)
        dst_ids = self.dst_engine.neutral_tables("dst", db)
        pairs, src_only, dst_only, ambiguous = self.match_tables(
            src_ids, dst_ids, self._renamer(db))
        if table:
            # one table asked about: the others, on either side, are not
            # this answer's to report
            pairs = [p for p in pairs if self._leaf(p[0]) == self._leaf(table)]
            src_only = [t for t in src_only
                        if self._leaf(t) == self._leaf(table)]
            dst_only = []
            ambiguous = [t for t in ambiguous
                         if self._leaf(t) == self._leaf(table)]
        rows = []
        for t in src_only:
            want = self._rename(t, db)
            why = f"{t} is on the source and not on the target"
            if want and str(want) != str(t):
                # the mapping is why it looks missing, and without saying so
                # the operator hunts for a table by a name it no longer has
                why = (f"{t} is mapped to {want}, which the target does"
                       " not have")
            rows.append((f"{db}.{self._leaf(t)}", "diff", why, None, None))
        for t in dst_only:
            rows.append((f"{db}.{self._leaf(t)}", "warn",
                         f"{t} is on the target and not on the source",
                         None, None))
        for t in ambiguous:
            rows.append((f"{db}.{self._leaf(t)}", "warn",
                         f"{self._leaf(t)} exists more than once on one side,"
                         " so migkit will not guess which one the other side"
                         " means - name the schema", None, None))
        # pass D: where the hop keeps its proof and the tail keeps it up,
        # the kept leaves prove the tables they hold (`kept.prove`)
        from .. import kept, leaves, verdict
        proved = (kept.prove(self, db, log=stream) or {}) if pairs and \
            kept.wanted(self.hop) else {}
        from ..throttle import Throttle
        # both sides are read in full a table at a time: each side's load is
        # asked between tables, and a replica read is held to the lag it
        # had (backlog C4)
        gates = (Throttle(1, probe=lambda: self._health("src", db)),
                 Throttle(1, probe=lambda: self._health("dst", db)))
        # where the source's log stood before any table was read: moved by
        # the time a table is found to differ, the source was written while
        # it was read, and what differs is read again by generations
        # (`leaves.generations`, backlog F1.d)
        try:
            began = self.src_engine.log_position("src", db)
        except Exception:  # noqa: BLE001 - nothing to tell by, then
            began = None
        for src_t, dst_t in pairs:
            scope = f"{db}.{self._leaf(src_t)}"
            self.__dict__.setdefault("_named", {})[scope] = str(src_t)
            how = {}
            try:
                src_cols, dst_cols, notes = self._column_plan(src_t, dst_t, db,
                                                              how)
            except Exception as e:
                rows.append((scope, "error",
                             f"cannot read the columns: {str(e)[:120]}",
                             None, None))
                continue
            tail = ("; " + "; ".join(notes)) if notes else ""
            # how much of a row the verdict compared, column by column
            # (problems D15)
            compared = verdict.compared_clause(how)
            tail += compared
            self.__dict__.setdefault("_compared", {})[scope] = compared
            if not src_cols:
                rows.append((scope, "warn",
                             "no column on this table can be compared across"
                             " these two engines" + tail, None, None))
                continue
            got = proved.get(self._leaf(src_t))
            if got is not None and leaves.strategy(
                    scope, {"kept": True}).parts["prove"].name == "kept":
                status, detail, na, nb = got
                if stream:
                    stream(f"{scope}: {status}")
                rows.append((scope, status, detail + tail, na, nb))
                continue
            try:
                wheres = self._row_scope(db, src_t)
                with gates[0].unit(), gates[1].unit():
                    a, b = self._digests(db, src_t, src_cols, dst_t,
                                         dst_cols, wheres)
                if wheres[1]:
                    # narrowing the comparison to the filter hides nothing:
                    # a target row outside it is one the move never put
                    # there, and is said
                    out, _ = self.dst_engine.neutral_digest(
                        "dst", db, dst_t, dst_cols,
                        where=f"({wheres[1]}) is not true")
                    if out:
                        tail += (f"; {out:,} rows on the target lie outside"
                                 " the hop's row filter, which the move does"
                                 " not put there")
            except SystemExit as e:
                # a filter that cannot be read through on one side: this
                # table's answer, not the whole check's
                rows.append((scope, "error", str(e)[:200], None, None))
                continue
            except Exception as e:
                rows.append((scope, "error",
                             str(e).splitlines()[-1][:120], None, None))
                continue
            # what the source itself says of the read: its own count held
            # to the rows read, and what the comparison cannot prove there
            proof, told = self._source_said(db, src_t)
            tail += told
            if a == b and proof == "diff":
                # both sides agree on rows the read did not wholly get
                if stream:
                    stream(f"{scope}: diff")
                rows.append((scope, "diff",
                             f"rows {a[0]:,} equal on both sides as read,"
                             f" and the source's own count says the read"
                             f" was short{tail}", a[0], b[0]))
                continue
            if stream:
                stream(f"{scope}: {'ok' if a == b else 'diff'}")
            if a == b:
                self._write_drill(db, self._leaf(src_t), missing=[],
                                  changed=[], extra=[], leaves=[])
                rows.append((scope, "ok",
                             f"rows {a[0]:,} and every compared column equal"
                             f" across {self.src_name}/{self.dst_name}"
                             f" (digest {a[1]}){self._where_folded()}{tail}",
                             a[0], b[0]))
                continue
            head = (f"rows src={a[0]:,} dst={b[0]:,}" if a[0] != b[0] else
                    f"rows {a[0]:,} match but the contents do not:"
                    f" digest src={a[1]} dst={b[1]}")
            # for the second reading of its rows (`check_data`)
            self.__dict__.setdefault("_differing", {})[(db, scope)] = (
                src_t, dst_t, src_cols, dst_cols, max(a[0], b[0]))
            with gates[0].unit(), gates[1].unit():
                said = self._drill(db, src_t, dst_t, src_cols, dst_cols,
                                   max(a[0], b[0]), (a, b), began)
            if getattr(said, "settled", False):
                # every leaf that differed read equal on both sides within
                # the generations a live source is given (`leaves.
                # generations`): the difference was still arriving
                self.__dict__["_differing"].pop((db, scope), None)
                rows.append((scope, "ok",
                             f"every compared column equal across"
                             f" {self.src_name}/{self.dst_name}, leaf by"
                             f" leaf{said}{tail}", said.rows_src,
                             said.rows_dst))
                continue
            rows.append((scope, "diff", head + said + tail, a[0], b[0]))
        for side, gate in zip(("source", "target"), gates):
            if gate.line() and stream:
                stream(f"# {side}: {gate.line()}")
        return rows

    def _source_said(self, db, src_t):
        """(status, words) the source adds about the whole read just made
        of `src_t`, where it has anything of its own to say: the
        application's own count held to the rows read (`read_proved` - an
        SAP service, a SaaS application), and what the comparison cannot
        prove there (`not_compared`). ("", "") where it says nothing."""
        src = self.src_engine
        status, words = "", []
        ask = getattr(src, "read_proved", None)
        if ask is not None:
            try:
                got = ask("src", db, src_t)
            except Exception as e:  # noqa: BLE001 - said, not guessed
                got = ("warn", "the source's own count could not be asked"
                               f" ({str(e).strip().splitlines()[0][:90]})"
                       if str(e).strip() else type(e).__name__)
            if got:
                status = got[0]
                words.append(got[1])
        tell = getattr(src, "not_compared", None)
        if tell is not None:
            try:
                said = tell(db, src_t)
            except Exception:  # noqa: BLE001 - nothing to add, then
                said = ""
            if said:
                words.append(f"not proved: {said}")
        return status, "".join(f"; {w}" for w in words)

    #: how the rows a differing table holds apart are found (`_drill`)
    DRILLS = (Rung("leaves", "the rows found from the leaves of its digest"
                             " tree"),
              Rung("walk", "the rows walked by key"))

    def _drill(self, db, src_t, dst_t, src_cols, dst_cols, rows=0,
               digests=None, began=None):
        """The base's walk, over this hop's two engines, through the hop's
        row filter on each side - or, for a table the walk would stop
        short in, a table with no key and a source written while it was
        read (its log moved since `began`), the rows found from the leaves
        of its digest tree (`leaves.drill`)."""
        from .. import leaves
        scope = self._row_scope(db, src_t)

        def walk():
            return self._drill_rows(db, self._leaf(src_t),
                                    self.src_engine, src_t, src_cols,
                                    self.dst_engine, dst_t, dst_cols, scope)

        if self._masks(db, src_t):
            # the leaves are hashed by each server from the values as they
            # are; the walk reads the source's rows as masked
            return walk()

        def run(choice):
            if choice.path == "walk":
                return walk()
            return leaves.drill(self, db, src_t, dst_t, src_cols, dst_cols,
                                scope, rows, digests, began)
        # leaves that cannot be read are switched from by itself to the
        # walk, and said with what it found; the table differs either way,
        # and where is what either finds
        said = []
        found = self._attempt(f"{db}.{self._leaf(src_t)}", self.DRILLS,
                              run, log=said.append)[1]
        if found is None:
            # the leaves are not the way for this table: walked
            return walk()
        return found + "".join(f"; {m}" for m in said)

    # --- the second reading, in this pair's terms (the logic is the base's)

    def _reader_names(self):
        return self.src_name, self.dst_name

    def _reader_engine(self, side):
        return self.src_engine if side == "src" else self.dst_engine

    def _reader_table(self, side, db, table):
        """The reader wants `schema.table`; MySQL's schema is its
        database."""
        if "." in str(table):
            return str(table)
        return f"{self.hop.target_db(db) if side == 'dst' else db}.{table}"

    def _reader_where(self, db, src_t):
        return self._row_scope(db, src_t)

    def _digests(self, db, src_t, src_cols, dst_t, dst_cols, wheres):
        """(the source's, the target's) digest of a table: where both
        sides are one engine with a salted digest of its own
        (`same_engine_digest` - Oracle's SHA-256 sum), that, both behind
        this run's salt; the digest every engine sums otherwise. A table
        the hop masks is summed from the source's rows as masked
        (`_Masks.neutral_digest`), never by the server over the values as
        they are."""
        src, dst = self.src_engine, self.dst_engine
        masked = self._masks(db, src_t)
        own = (_own_class(src) is _own_class(dst) and not masked
               and callable(getattr(src, "same_engine_digest", None)))
        salt = {"salt": self.digest_salt()} if own else {}

        def one(eng, side, table, cols, where):
            ask = eng.same_engine_digest if own else eng.neutral_digest
            got = ask(side, db, table, cols,
                      **({"where": where} if where else {}), **salt)
            # kept for the evidence, with what takes it again: the columns
            # and their classes, the engine, the filter (`digests`)
            from .. import digests
            digests.record(self.hop, db, table, side, got[0], got[1],
                           method=digests.OWN if own else digests.READ
                           if masked and side == "src" else digests.NEUTRAL,
                           salt=salt.get("salt"),
                           engine=getattr(eng, "CANON_ENGINE", None),
                           columns=cols, where=where)
            return got
        return (one(src, "src", src_t, src_cols, wheres[0]),
                one(dst, "dst", dst_t, dst_cols, wheres[1]))

    def _neutral_compare(self, db, table=None, stream=None):
        return [Result("data", scope, status, detail,
                       table=self._named_as(scope))
                for scope, status, detail, _, _
                in self._neutral_rows(db, table, stream)]

    def _named_as(self, scope):
        """The source's own name of the table a per-table answer's scope
        names by its leaf (`Result.table`), '' where none was read."""
        return (self.__dict__.get("_named") or {}).get(scope, "")

    def _where_folded(self):
        """A clause naming any side that had to fold its rows on this machine.

        The digest is the same number either way; what is not the same is
        where the data went to produce it. An engine with no hashing operator
        - MongoDB has none at all - sends its documents here, and a report
        that read identically for both cases would be hiding the one fact an
        operator needs to size the run.
        """
        from .. import canon
        remote, local = [], []
        for name, engine in ((self.src_name, self.src_engine),
                             (self.dst_name, self.dst_engine)):
            # a warehouse folds a table here only where its SQL could not
            # (`folded_last`, set by the digest just taken)
            if engine.CANON_ENGINE not in canon.IN_PROCESS or \
                    not getattr(engine, "folded_last", True):
                continue
            # an engine that sums in its server where the table allows it
            # (Oracle) folded nothing here when every table allowed it
            if engine.__dict__.get("digested_in_server") and \
                    not engine.__dict__.get("digested_here"):
                continue
            (remote if engine.OVER_NETWORK else local).append((name, engine))
        parts = [f"{name} {engine.FOLDED_BECAUSE}, so its rows crossed the"
                 " network to be folded here" for name, engine in remote]
        local = [name for name, _ in local]
        if local:
            parts.append(f"{', '.join(local)} was folded in this process,"
                         " which is where it runs anyway")
        return (" - " + "; ".join(parts)) if parts else ""

    def assess(self):
        """Both sides, plus what this particular pairing can and cannot do.

        A cross-engine hop has two servers and neither engine's own assess
        knows the other exists. So this runs both - labelling every row with
        which side it came from, because "server version match" means nothing
        when the two sides are different software - and then answers the
        question only the pair can answer: which of migkit's operations work
        for *this* combination.

        The capability rows are read from the classes rather than by calling
        them, so `assess` says what the pairing supports even when one of the
        servers is unreachable.
        """
        from .base import Engine
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "pair", "item": item,
                          "detail": str(detail)})
        add("pass", "engines", f"{self.src_name} -> {self.dst_name}")

        for side, name, engine in (("source", self.src_name, self.src_engine),
                                   ("target", self.dst_name,
                                    self.dst_engine)):
            other = "target" if side == "source" else "source"
            try:
                for row in self._one_side(engine, side).assess():
                    # what compares the two sides, or is about the other
                    # one, means nothing asked of one server alone; the
                    # pair's own rows below answer for the pairing
                    item = str(row.get("item", "")).lower()
                    if other in item or " match" in item:
                        continue
                    row = dict(row)
                    row["scope"] = f"{side} ({name}) {row.get('scope', '')}"
                    row["item"] = f"{side}: {row.get('item', '')}"
                    items.append(row)
            except Exception as e:
                add("warn", f"{side} ({name}) could not be assessed",
                    f"{str(e).splitlines()[-1][:110]} - unknown, not clean")

        items += self._pair_capabilities()
        try:
            dbs = self.databases()
        except Exception:
            dbs = []
        for db in dbs:
            got = self._zero_date_result(db)
            if got is not None:
                items.append({"level": "fail" if got.status == "diff"
                              else "pass", "scope": "pair",
                              "item": f"{db} zero dates",
                              "detail": got.detail})
            if self._can_move_neutrally():
                got = self._unfit_result(db)
                items.append({"level": {"diff": "fail", "ok": "pass"}.get(
                                  got.status, "warn"), "scope": "pair",
                              "item": got.scope, "detail": got.detail})
        return items

    def _zero_date_result(self, db):
        """The source's zero dates, where the target has no such date
        (B6).

        Measured, MySQL into PostgreSQL: `'0000-00-00'` comes back from the
        driver as that string, and PostgreSQL answers `date/time field value
        out of range` - the copy stops at the first one, with everything
        before it already written. Named here, by column and count, before
        the move reads a row. None where the pair cannot have them."""
        reader = getattr(self.src_engine, "zero_dates", None)
        if reader is None or getattr(self.dst_engine, "zero_dates", None):
            return None
        try:
            got = reader("src", db)
        except Exception as e:
            return Result("deep", f"{db} zero dates", "error",
                          "could not be read, so unknown rather than none:"
                          f" {str(e).splitlines()[-1][:90]}")
        if not got:
            return Result("deep", f"{db} zero dates", "ok",
                          "no zero dates in the source's date columns")
        return Result(
            "deep", f"{db} zero dates", "diff",
            f"{sum(n for _, _, n in got):,} rows hold a date with a zero"
            f" year, month or day, which {self.dst_name} has no value for:"
            + ", ".join(f" {t}.{c} {n:,}" for t, c, n in got[:5])
            + (" ..." if len(got) > 5 else "")
            + " - the copy stops at the first one", "",
            "decide what each means - NULL, or a real date - and set it on"
            " the source, or in a view the hop reads, before the move")

    def _fit_plan(self, db, src_t):
        """(checks, columns, key, where) for one source table: every
        (source column, kind, arg) `canon.unfit` says to ask before its
        values are moved into the table on the target as it stands - or as
        this pair would build it - with the columns and key to read them
        by, and the hop's row filter."""
        from .. import canon
        leaf = self._leaf(self._rename(src_t, db))
        declared = {n: self.src_engine.canonical_type("src", db, t)
                    for n, t in self.src_engine.neutral_columns("src", db,
                                                                src_t)}
        mapped, back = self._mapped_types(db, src_t, declared)
        there = ([] if self.dst_engine.target_missing(db)
                 else self.dst_engine.neutral_tables("dst", db))
        pairs, _, _, _ = self.match_tables([src_t], there, self._renamer(db))
        dst_t = pairs[0][1] if pairs else None
        facts = self.dst_engine.column_facts("dst", db, dst_t)
        mine = self.src_engine.column_facts("src", db, src_t)
        if dst_t is not None:
            dst_types = {n: self.dst_engine.canonical_type("dst", db, t)
                         for n, t in self.dst_engine.neutral_columns(
                             "dst", db, dst_t)}
        elif self.dst_engine.CREATES_ON_WRITE:
            # nothing to build, and what each value becomes is the engine's
            dst_types = {n: canon.written_as(
                self.src_engine.CANON_ENGINE, t,
                self.dst_engine.CANON_ENGINE) for n, t in mapped.items()}
        else:
            dst_types = {}
            columns, built_key = self._target_shape(db, src_t, leaf)
            for name, cls, numbers, *_ in columns:
                try:
                    dst_types[name] = canon.ddl_type(
                        self.dst_engine.CANON_ENGINE, cls, numbers,
                        name in built_key)
                except ValueError:
                    continue
        checks, classes = [], {}
        # a masked column is written as its transform makes it, not as the
        # source holds it: the target takes or refuses that on the write
        masked = self.hop.masks(db, src_t, columns=list(back.values()))
        for name, typ in sorted(mapped.items()):
            if dst_t is not None and name not in dst_types:
                continue
            if back[name] in masked:
                continue
            got = canon.unfit(self.src_engine.CANON_ENGINE, typ,
                              self.dst_engine.CANON_ENGINE,
                              dst_types.get(name),
                              facts.get(name) or facts.get(None) or {},
                              mine.get(back[name]) or {})
            if got:
                classes[back[name]] = canon.type_class(
                    self.src_engine.CANON_ENGINE, typ)
            checks += [(back[name], kind, arg) for kind, arg in got]
        key = self.src_engine.neutral_key("src", db, src_t) if checks else []
        for k in key:
            classes.setdefault(k, canon.type_class(
                self.src_engine.CANON_ENGINE, declared.get(k)))
        return (checks, sorted(classes.items()), key,
                self._row_scope(db, src_t)[0], dst_types)

    def unfit_values(self, db, src_t):
        """[(source column, kind, arg, rows, example keys)] - the values of
        one source table that would not arrive on the target as they are
        (`canon.unfit`), counted on the source. Asked once per table per
        run: a whole-database move asks every table before the first is
        copied, and the copier asks again only for a table nothing asked
        yet."""
        known = self.__dict__.setdefault("_unfit_known", {})
        if (db, src_t) in known:
            return known[(db, src_t)]
        checks, columns, key, where, _ = self._fit_plan(db, src_t)
        found = []
        if checks:
            got = self.src_engine.count_unfit("src", db, src_t, columns,
                                              checks, key, where)
            found = [(col, kind, arg, n, examples)
                     for (col, kind, arg), (n, examples)
                     in zip(checks, got) if n]
        for i, (col, kind, arg, n, examples) in enumerate(found):
            if kind == "json-number":
                held = self._json_rewrites(db, src_t, col, columns, key,
                                           where)
                if held is not None:
                    found[i] = (col, kind, arg) + held
        found = [f for f in found if f[3]]
        found += [(", ".join(cols), kind, arg, groups, examples)
                  for cols, kind, arg, groups, examples
                  in self._merged_keys(db, src_t)]
        known[(db, src_t)] = found
        return found

    def _json_rewrites(self, db, src_t, col, columns, key, where):
        """(rows, example keys) of the rows of one JSON column holding a
        number the target stores as another - asked of the target
        (`json_rewritten`) for the numbers `canon.json_candidate` picks,
        which are the only ones that can change. None where the target
        keeps every JSON number, and the count stands as the source
        gave it."""
        from .. import canon
        pred = canon.unfit_sql(self.src_engine.CANON_ENGINE, "json-number",
                               self.src_engine._quote_ident(col), None)
        scope = " and ".join(f"({p})" for p in (where, pred) if p) or None
        classes = dict(columns)
        read = [(col, "json")] + [(k, classes.get(k)) for k in key
                                  if k != col]
        held = []
        for rows in self.src_engine._every_row("src", db, src_t, read,
                                               scope):
            for r in rows:
                try:
                    got = [str(n) for n in canon._json_numbers(r[0])
                           if canon.json_candidate(n)]
                except (TypeError, ValueError):
                    continue
                if got:
                    held.append((tuple(r[1:]), got))
        changed = self.dst_engine.json_rewritten(
            "dst", db, sorted({t for _, got in held for t in got}))
        if changed is None:
            return None
        hit = [k for k, got in held if any(t in changed for t in got)]
        return len(hit), hit[:3]

    def _merged_keys(self, db, src_t):
        """(columns, kind, arg, groups, examples) for each unique key of
        the source table - its primary key and every plain unique index -
        whose distinct values the target's comparison would take for one
        key, or []. What the target compares text by is its own column's
        collation, or its database's for a table it would build
        (`key_merges`); a float key it compares by value, where -0.0 and
        0.0 are one - Cassandra alone keeps them apart. Only a key with a
        text or a float column is asked: whole numbers, dates and bytes
        compare exactly everywhere."""
        from .. import canon
        declared = {n: self.src_engine.canonical_type("src", db, t)
                    for n, t in self.src_engine.neutral_columns("src", db,
                                                                src_t)}
        _, back = self._mapped_types(db, src_t, declared)
        forward = {v: k for k, v in back.items()}
        keys = [self.src_engine.neutral_key("src", db, src_t)]
        try:
            keys += [cols for _, unique, cols, plain in
                     self.src_engine.neutral_indexes("src", db, src_t)
                     if unique and plain and cols]
        except Exception:  # noqa: BLE001 - the primary key alone
            pass
        classes = {n: canon.type_class(self.src_engine.CANON_ENGINE, t)
                   for n, t in declared.items()}
        keys = [k for k in keys if k and all(c in forward for c in k)
                and any(classes.get(c) in ("text", "float") for c in k)]
        if not keys:
            return []
        there = ([] if self.dst_engine.target_missing(db)
                 else self.dst_engine.neutral_tables("dst", db))
        pairs, _, _, _ = self.match_tables([src_t], there, self._renamer(db))
        facts = self.dst_engine.column_facts(
            "dst", db, pairs[0][1] if pairs else None)
        where = self._row_scope(db, src_t)[0]
        out = []
        for cols in keys:
            names = [forward[c] for c in cols]
            if len(set(names)) != len(names):
                continue
            coll = [((facts.get(t) or facts.get(None) or {}).get("collation")
                     if classes.get(c) == "text" else None)
                    for c, t in zip(cols, names)]

            def batches():
                # a key with a NULL in it is no key a unique index holds
                # to anything
                read = [(c, classes.get(c)) for c in cols]
                for rows in self.src_engine._every_row(
                        "src", db, src_t, read, where):
                    yield [tuple(v if classes.get(c) == "text"
                                 else canon.render_value(classes.get(c), v)
                                 for c, v in zip(cols, r)) for r in rows
                           if None not in r]
            got = self.dst_engine.key_merges(
                "dst", db, list(zip(names, coll)), batches())
            if got and got[0]:
                out.append((cols, "merge",
                            "/".join(sorted({c for c in coll if c})),
                            got[0], got[1]))
            elif any(classes.get(c) == "float" for c in cols) and \
                    self.dst_engine.CANON_ENGINE != "cassandra":
                got = self._signed_zero_keys(db, src_t, cols, classes,
                                             where)
                if got[0]:
                    out.append((cols, "signed-zero", None) + got)
        return out

    def _signed_zero_keys(self, db, src_t, cols, classes, where,
                          examples=3):
        """(groups, [[key, key], ...]) of keys that differ only by the sign
        of a zero - -0.0 against 0.0, which the source keeps as two keys
        and a target comparing by value takes for one."""
        seen, groups = {}, []
        read = [(c, classes.get(c)) for c in cols]
        for rows in self.src_engine._every_row("src", db, src_t, read,
                                               where):
            for r in rows:
                if None in r:
                    continue
                # -0.0 == 0.0 in Python as well: a key is told apart by
                # the text of its floats, which keeps the sign
                same = tuple(0.0 if isinstance(v, float) and v == 0 else v
                             for v in r)
                mine = tuple(repr(v) if isinstance(v, float) else v
                             for v in r)
                if same in seen and seen[same][0] != mine:
                    groups.append([seen[same][1], tuple(r)])
                else:
                    seen.setdefault(same, (mine, tuple(r)))
        return len(groups), groups[:examples]

    def _unfit_words(self, found, key):
        from .. import canon
        parts = []
        for col, kind, arg, n, examples in found:
            if kind in ("merge", "signed-zero"):
                shown = "; ".join(" and ".join(
                    repr(k[0]) if len(k) == 1 else repr(tuple(k))
                    for k in group) for group in examples)
                parts.append(f"{col}: {n:,} group{'s' if n != 1 else ''} of "
                             + canon.unfit_words(kind, arg, self.dst_name)
                             + (f" - {shown}" if shown else ""))
                continue
            shown = ", ".join(
                (str(k[0]) if len(k) == 1 else
                 "(" + ", ".join(str(v) for v in k) + ")")
                for k in examples)
            parts.append(
                f"{col}: {n:,} {'row holds' if n == 1 else 'rows hold'} "
                + canon.unfit_words(kind, arg, self.dst_name)
                + (f" - {', '.join(key)} {shown}" if shown else ""))
        return parts

    def _accepted(self, src_t, col):
        """Whether the hop's `accept_changes` names this column: a list of
        `table.column` patterns (shell wildcards, the table by its name on
        the source) whose values the operator has decided may arrive as
        the target keeps them - refused by default, as each is a value
        that changes on the way."""
        from fnmatch import fnmatch
        said = f"{self._leaf(src_t)}.{col}"
        return any(fnmatch(said, str(p)) for p in
                   (self.hop.options or {}).get("accept_changes") or [])

    def _unfit_tables(self, db, tables=None):
        """({source table: (found, key)}, {the same for what the hop's
        `accept_changes` let through}) with something found, over the
        tables this hop moves (or `tables`)."""
        if tables is None:
            tables = [f"{s}.{t}" if s else t
                      for s, t in self.list_move_tables(db)]
        out, let = {}, {}
        for src_t in tables:
            if not self.hop.takes_table(db, src_t):
                continue
            found = self.unfit_values(db, src_t)
            refused = [f for f in found if not self._accepted(src_t, f[0])]
            accepted = [f for f in found if f not in refused]
            key = self.src_engine.neutral_key("src", db, src_t) \
                if found else []
            if refused:
                out[src_t] = (refused, key)
            if accepted:
                let[src_t] = (accepted, key)
        return out, let

    def _unfit_result(self, db):
        """The deep check's line for it, and assess's."""
        scope = f"{db} values the target would change"
        try:
            got, let = self._unfit_tables(db)
        except SystemExit as e:
            return Result("deep", scope, "error", str(e)[:200])
        except Exception as e:  # noqa: BLE001 - said, not raised
            return Result("deep", scope, "error",
                          "could not be counted, so unknown rather than"
                          f" none: {str(e).splitlines()[-1][:120]}")
        taken = [f"{self._leaf(t)}.{p}" for t, (found, key) in let.items()
                 for p in self._unfit_words(found, key)]
        if not got and taken:
            return Result(
                "deep", scope, "warn",
                "the hop accepts these changing on the way"
                " (`accept_changes`): " + "; ".join(taken[:6])
                + (" ..." if len(taken) > 6 else ""))
        if not got:
            return Result("deep", scope, "ok",
                          f"every value the source holds arrives on"
                          f" {self.dst_name} as it is")
        parts = [f"{self._leaf(t)}.{p}" for t, (found, key) in got.items()
                 for p in self._unfit_words(found, key)]
        return Result(
            "deep", scope, "diff",
            "; ".join(parts[:6]) + (" ..." if len(parts) > 6 else ""), "",
            "decide what each of these should be - on the source, or in a"
            " view the hop reads - or give the target a column that holds"
            " them, before the move")

    def _pair_temporal_meaning(self, db):
        """Whether a temporal column records the same kind of time on the
        target as on the source - an instant, or a wall clock - across the
        two engines. The same question `_temporal_meaning` asks within one
        engine, asked of each side's own meaning of its own type: a
        PostgreSQL `timestamptz` built as a MySQL `datetime` or a SQL
        Server `datetime2` loses the offset on the way, and no digest of
        what arrived can see it."""
        from .. import canon
        mismatched, unmapped, checked = [], 0, 0
        try:
            there = ([] if self.dst_engine.target_missing(db)
                     else self.dst_engine.neutral_tables("dst", db))
            pairs, _, _, _ = self.match_tables(
                self.src_engine.neutral_tables("src", db), there,
                self._renamer(db))
            for src_t, dst_t in pairs:
                mapped, _ = self._mapped_types(db, src_t, dict(
                    self.src_engine.neutral_columns("src", db, src_t)))
                dst_types = dict(self.dst_engine.neutral_columns("dst", db,
                                                                 dst_t))
                for name, src_type in sorted(mapped.items()):
                    if name not in dst_types:
                        continue
                    a = canon.time_meaning(self.src_engine.CANON_ENGINE,
                                           src_type)
                    b = canon.time_meaning(self.dst_engine.CANON_ENGINE,
                                           dst_types[name])
                    if a is None or b is None:
                        if canon.type_class(self.src_engine.CANON_ENGINE,
                                            src_type) in ("timestamp",
                                                          "time"):
                            unmapped += 1
                        continue
                    checked += 1
                    if a != b:
                        mismatched.append((self._leaf(src_t), name,
                                           src_type, a, dst_types[name], b))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return Result("deep", f"{db} temporal meaning", "error",
                          "could not compare the temporal columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        got = self._temporal_meaning_result(db, mismatched, unmapped,
                                            checked)
        if got.status != "diff":
            return got
        # across two engines an instant is carried as its UTC wall clock
        # (every session migkit opens is pinned to UTC), so no value is
        # lost - what the column means to the application is what changed
        return Result(got.check, got.scope, "warn",
                      got.detail + "; the values were carried as the UTC"
                      " clock of each instant, so nothing is lost if the"
                      " application reads them as UTC", got.report,
                      got.fix_hint)

    def refuse_unfit(self, db, tables=None):
        """Stop before anything is copied where a value would not arrive
        as it is: rounded, cut, turned into a double or a default, or
        refused by the target halfway through the load. Named by table,
        column and kind, with how many rows and the keys of a few."""
        got, _ = self._unfit_tables(db, tables)
        if not got:
            return
        lines = [f"  {self._leaf(t)}.{p}" for t, (found, key) in got.items()
                 for p in self._unfit_words(found, key)]
        raise SystemExit(
            f"{db}: values on the source would not arrive on"
            f" {self.dst_name} as they are, so nothing was copied:\n"
            + "\n".join(lines[:20])
            + ("\n  ..." if len(lines) > 20 else "")
            + "\nDecide what each of these should be - on the source, or in"
              " a view the hop reads - or give the target a column that"
              " holds them, and move again. Where a change is what is"
              " wanted, the hop's `accept_changes` names the columns"
              " (`table.column`) that may arrive as the target keeps"
              " them.")

    def _one_side(self, engine, side):
        """`engine` looking at one side's server as both of its own.

        An engine's assess compares its two sides, and in a pair its other
        side is the other engine's server. Measured, a MySQL source's
        assess asked the PostgreSQL target for MySQL's greeting and waited
        on it - an hour per connection under the read timeout, fifteen
        seconds and retries after that was bounded - so `assess` on a
        cross-engine hop never finished. Pointed at its own server twice,
        it answers in a second; what compares the two sides is then
        dropped by the caller."""
        import dataclasses
        ep = self.hop.source if side == "source" else self.hop.target
        dbs = [self.hop.target_db(d) if side == "target" else d
               for d in (self.hop.databases or [])]
        hop = dataclasses.replace(self.hop, source=ep, target=ep,
                                  databases=dbs, db_map={})
        hop.report_dir = self.hop.report_dir
        return type(engine)(hop)

    def _health(self, side, db=None):
        """The load of whichever server this side actually is.

        A cross-engine hop has no server of its own, so a throttle here has
        nothing to ask unless it asks the engine on that side. Without this
        the pair that reads hardest - the one doing the full scan - was the
        only configuration with no brake at all.
        """
        probe = getattr(self.src_engine if side == "src" else self.dst_engine,
                        "_health", None)
        if probe is None:
            return None
        import inspect
        try:
            # PostgreSQL's asks in a database: called with the side alone
            # it raised, and the pair read with no brake from that side
            takes = len(inspect.signature(probe).parameters)
            return probe(side, db) if takes > 1 and db else probe(side)
        except Exception:
            return None

    def repair_plan(self, db, kind):
        """What `migkit sync --kind rows` would carry across the pair.

        The rows come from the last check's drilldown, so the plan describes
        what the operator was shown rather than whatever the two sides happen
        to disagree about at this moment.
        """
        out = []
        behind = getattr(self.dst_engine, "sequences_behind", None)
        if kind in ("sequences", "all") and behind is not None:
            got = behind(db)
            if got:
                out.append(RepairAction(
                    db, "sequences", self.dst_engine._raise_past(got), [],
                    f"{len(got)} sequences raised past their column's rows"))
        if kind not in ("rows", "all"):
            return out
        return out + self._rows_plan(
            db, f"from {self.src_name} to {self.dst_name}")

    def apply(self, db, action):
        """What a pair has to settle before the base can carry the rows:
        that the two engines can move rows at all, and which table on the
        target the drilldown's table means. A sequence is the target's own
        business, done by its own engine.
        """
        if action.kind == "sequences":
            return self.dst_engine.apply(db, action)
        if not self._can_move_neutrally():
            raise SystemExit(
                f"this pair cannot repair rows: {self.src_name} ->"
                f" {self.dst_name} does not implement both halves of the"
                " read/write contract, so there is nothing to carry the rows"
                " with. `migkit assess` lists what the pair can do")
        name = action.scope.split(".", 1)[1]
        found = self._drill_tables(db).get(name, {})
        if not found:
            return
        pairs, _, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db), self._renamer(db))
        match = [(s, d) for s, d in pairs if self._leaf(s) == name]
        if not match:
            raise SystemExit(f"{name} is no longer on both sides, so the"
                             " rows the last check listed cannot be placed")
        src_t, dst_t = match[0]
        src_cols, dst_cols, _ = self._column_plan(src_t, dst_t, db)
        with self.dst_engine.load_window(db, None, {dst_t}):
            self._apply_rows(db, name, self.src_engine, src_t, src_cols,
                             self.dst_engine, dst_t, dst_cols)

    def _pair_capabilities(self):
        """What this combination can do, answered from the classes.

        Separate from `assess` because it needs no server: a capability
        question is about the two engines, and turning it into "is the
        database up right now" would give it a different answer on a bad
        afternoon. It is also the part of the report worth reading when a
        side is unreachable.
        """
        from .base import Engine
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "pair", "item": item,
                          "detail": str(detail)})
        for label, ok, why in (
                ("compare", self._can_compare_neutrally(),
                 "both engines have a canonical rendering, so a digest"
                 " computed on each side is the same number"),
                ("move rows", self._can_move_neutrally(),
                 "both engines can be read from and written to"),
                ("create the target table",
                 type(self.dst_engine).neutral_create_sql
                 is not Engine.neutral_create_sql
                 or self.dst_engine.CREATES_ON_WRITE,
                 (f"{self.dst_name} makes it on the first write"
                  if self.dst_engine.CREATES_ON_WRITE
                  else f"{self.dst_name} can be told what table to build")),
                ("tail changes",
                 type(self.src_engine).neutral_changes
                 is not Engine.neutral_changes
                 and type(self.dst_engine)._apply_upsert
                 is not Engine._apply_upsert,
                 f"{self.src_name} has a change log and {self.dst_name} can"
                 " apply what comes out of it")):
            add("pass" if ok else "warn", f"this pair can {label}",
                why if ok else f"no: {why} is not true here")
        return items

    def _can_move_neutrally(self):
        """Whether both engines implement the read/write contract.

        Answered from the classes, not by calling them: a capability probe
        that opens a connection turns "can this pair move" into "is the
        database up right now", and the two questions have different answers
        and different failure messages.
        """
        return all(
            type(engine).neutral_read is not Engine.neutral_read
            and type(engine).neutral_write is not Engine.neutral_write
            for engine in (self.src_engine, self.dst_engine))

    def _move_columns(self, src_table, dst_table, db):
        """Column names to carry, and what is being left behind.

        Matched by name and **sorted**, so the two sides line up by name
        rather than by the order each server happens to list them in - the
        same rule the comparison uses, and for the same reason.

        Unlike the comparison, a column whose type has no canonical rendering
        is still moved: the rendering exists so two engines can be compared,
        and a value that cannot be compared can still be carried. What is not
        carried is a column the target does not have, and that is named.
        """
        src_types, back = self._mapped_types(db, src_table, dict(
            self.src_engine.neutral_columns("src", db, src_table)))
        try:
            dst_types = dict(self.dst_engine.neutral_columns("dst", db,
                                                             dst_table))
        except Exception:
            dst_types = {}
        notes = []
        if not dst_types and self.dst_engine.CREATES_ON_WRITE:
            # nothing to intersect with: the collection is made by the write
            # itself, so what it will hold is whatever the source has
            both = sorted(src_types)
            dst_types = dict(src_types)
        else:
            both = sorted(set(src_types) & set(dst_types))
            missing = sorted(set(src_types) - set(dst_types))
            if missing:
                notes.append("not carried, the target has no such column: "
                             + ", ".join(missing))
        from .. import canon
        src_cols, dst_cols = [], []
        for name in both:
            s = canon.type_class(self.src_engine.CANON_ENGINE,
                                 src_types[name])
            d = canon.type_class(self.dst_engine.CANON_ENGINE,
                                 dst_types[name])
            typed = canon.bson_pair(self.src_engine.CANON_ENGINE,
                                    src_types[name],
                                    self.dst_engine.CANON_ENGINE,
                                    dst_types[name])
            if typed and all(typed):
                # MongoDB on one side: what each value is written as is the
                # BSON type the other side's column holds (`bsontype`)
                s, d = typed
                if self._accepted(src_table, back.get(name, name)):
                    s, d = canon.by_value(s), canon.by_value(d)
            src_cols.append((back.get(name, name), s))
            dst_cols.append((name, d))
        return src_cols, dst_cols, notes

    @staticmethod
    def _rows_differ(sc, dc, a, b):
        """(missing, extra, changed) between rows read by key from each
        side - {key: row} in `sc` and `dc` order - compared in the one
        rendering both sides' digests render to (`canon.digest_value`): a
        value wider than `canon.WIDE` by its length and md5, as the servers
        hold it - never its hex, twice its size on each side."""
        from .. import canon

        def text(cols, row):
            return tuple(canon.digest_value(c, v)
                         for (_, c), v in zip(cols, row))
        missing = [k for k in a if k not in b]
        extra = [k for k in b if k not in a]
        changed = [k for k in a if k in b
                   and text(sc, a[k]) != text(dc, b[k])]
        return missing, extra, changed

    #: keys asked of the target at once when a batch is read back
    READ_BACK = 5000

    def _read_back_plan(self, db, src_t, dst_t, src_cols, key):
        """What a written batch is read back and compared by: (source
        columns, target columns, where each lies in a row as read, the
        key in the target's names) - or None where the two sides have no
        rendering in common for the key and a column besides it."""
        if key is None or not self._can_compare_neutrally():
            return None
        sc, dc, _ = self._comparable_columns(db, self.src_engine, src_t,
                                             self.dst_engine, dst_t)
        names = [n for n, _ in sc]
        read = [n for n, _ in src_cols]
        if (not sc or not set(key) <= set(names)
                or not set(names) <= set(read)):
            return None
        to_dst = dict(zip(names, [n for n, _ in dc]))
        src_where, dst_where = self._row_scope(db, src_t)
        return (sc, dc, [read.index(n) for n in names],
                [to_dst[k] for k in key], dst_where, src_t, src_where)

    def _batch_digest(self, db, dst_t, plan, key, rows):
        """(the batch's digest, the target's over the batch's key range),
        where the key is one integer column on both sides: one number back
        from each server instead of every row. The source's is asked of the
        source over the same range, beside the target's - folding the batch
        here took longer than reading it (measured, 7.8 of 32.7 seconds a
        million rows). A source changed since the batch was read answers a
        different number, and the batch is then compared row by row with
        what was read, which is what decides. None where it does not apply
        - a range of text holds different rows under different collations."""
        import concurrent.futures as cf

        from .. import canon
        sc, dc, at, dst_key = plan[:4]
        dst_where = plan[4] if len(plan) > 4 else None
        src_t, src_where = (plan[5], plan[6]) if len(plan) > 6 else (None,
                                                                      None)
        if (len(key) != 1 or dict(sc).get(key[0]) != "integer"
                or dict(dc).get(dst_key[0]) != "integer"
                or not _digests_ranges(self.dst_engine)):
            return None
        where = [n for n, _ in sc].index(key[0])
        ks = [r[at[where]] for r in rows if r[at[where]] is not None]
        if not ks:
            return None
        lo, hi = int(min(ks)), int(max(ks))

        def rng(eng, name, extra):
            q = eng._quote_ident(name)
            r = f"{q} >= {lo} and {q} <= {hi}"
            return f"({extra}) and {r}" if extra else r

        def theirs():
            got = self.dst_engine.neutral_digest(
                "dst", db, dst_t, dc, where=rng(self.dst_engine, dst_key[0],
                                                dst_where))
            return int(got[0]), str(got[1])

        def ours():
            # a masked table's batch is folded here, as written: the
            # source's server would sum the values as they are
            if src_t and _digests_ranges(self.src_engine) \
                    and not self._masks(db, src_t):
                got = self.src_engine.neutral_digest(
                    "src", db, src_t, sc, where=rng(self.src_engine, key[0],
                                                    src_where))
                return int(got[0]), str(got[1])
            n, total = canon.fold_rows([c for _, c in sc],
                                       ([r[i] for i in at] for r in rows))
            return n, str(total)
        with cf.ThreadPoolExecutor(2) as pool:
            a, b = pool.submit(ours), pool.submit(theirs)
            return a.result(), b.result()

    def _verify_batch(self, db, dst_t, plan, key, rows):
        """Keys of `rows` - as read from the source - whose row on the
        target is not there or does not render the same: (missing,
        changed, the columns the first changed row differs in)."""
        from .. import canon
        sc, dc, at, dst_key = plan[:4]
        mine = [[r[i] for i in at] for r in rows]
        names = [n for n, _ in sc]
        where = [names.index(k) for k in key]
        missing, changed, cols = [], [], []
        # a key into MongoDB as it was written there (`bsontype`): an
        # ObjectId kept in a JSON column is looked for as the ObjectId
        into = (dict(dc) if self.dst_engine.CANON_ENGINE == "mongodb"
                and self.src_engine.CANON_ENGINE != "mongodb" else {})
        at_key = [dst_key[key.index(names[j])] for j in where]
        for i in range(0, len(mine), self.READ_BACK):
            part = mine[i:i + self.READ_BACK]
            a = self._by_key_map(sc, key, part)
            b = self.dst_engine.neutral_rows_by_key(
                "dst", db, dst_t, dc, dst_key,
                [tuple(canon.write_value("mongodb", into[n], r[j])
                       if n in into else r[j]
                       for n, j in zip(at_key, where)) for r in part])
            gone, _, differ = self._rows_differ(sc, dc, a, b)
            missing += gone
            changed += differ
            if differ and not cols:
                k = differ[0]
                cols = [n for (n, c), x, (_, d), y in
                        zip(sc, a[k], dc, b[k])
                        if canon.render_value(c, x)
                        != canon.render_value(d, y)]
        return missing, changed, cols

    def _neutral_move(self, db, sch, tbl, chunk, ck, log, later):
        """Read from one engine, write to the other, for any pair.

        Resumable through the checkpoint the caller already keeps, by the
        target table's own key. A table with no key is read in one pass and
        the log says so - restarting that one starts it over, which is the
        honest consequence of there being nothing to resume from.
        """
        # a way of either side that fails as it writes a batch says its
        # switch in the copy's log (`Engine._attempt`)
        self.lend_log(log)
        src_t = f"{sch}.{tbl}" if sch else tbl
        leaf = self._leaf(src_t)
        if not ck.get(self.move_key(db, "", leaf)):
            # nothing of this table is on the target yet: a value it would
            # not take as it is stops the move here, before the table is
            # built or a row read
            self.refuse_unfit(db, [src_t])
        pairs, _, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db), self._renamer(db))
        match = [d for s_, d in pairs if self._leaf(s_) == leaf]
        # the name it goes by on the target, through the hop's mapping: a
        # renamed table that is not there yet is made under its new name
        want = self._leaf(self._rename(src_t, db))
        if match:
            dst_t = match[0]
        elif self.dst_engine.CREATES_ON_WRITE:
            dst_t = want
            log(f"{want}: not on the target yet;"
                f" {self.dst_name} creates it on the first write")
        else:
            dst_t = self._create_target(db, src_t, want, log)
        src_cols, dst_cols, notes = self._move_columns(src_t, dst_t, db)
        for n in notes:
            log(f"{leaf}: {n}")
        key = self.move_key(db, "", leaf)
        st = ck.setdefault(key, {})
        if st.get("done") and self._still_the_same(db, key, src_t, dst_t,
                                                    st, log):
            return
        src_where, dst_where = self._row_scope(db, src_t)
        read_scope = {"where": src_where} if src_where else {}
        from .. import claims
        if claims.way_of(self.hop, db, key) != "ranges":
            # a table every machine writes at once keeps its indexes: one
            # machine setting them aside would rebuild them under the
            # others' writes
            self.set_aside_indexes(later, self.dst_engine, db, dst_t, st,
                                   self.src_engine._rows_of("src", db, src_t),
                                   log)
        if self._move_in_ranges(db, key, src_t, dst_t, src_cols, chunk, st,
                                ck, log, src_where, dst_where):
            return
        from .. import crossmove
        timed = crossmove.whole(self, db, key, src_t, dst_t, st, ck, log)
        if timed is True:
            return
        self._as_rows(db, key, src_t, dst_t, src_cols, dst_cols, chunk, st,
                      ck, log, src_where, dst_where, read_scope)
        if timed:
            timed(int(st.get("moved") or 0))

    def _as_rows(self, db, key, src_t, dst_t, src_cols, dst_cols, chunk, st,
                 ck, log, src_where, dst_where, read_scope):
        """The table copier's own way: rows read into this process and
        written, resumed by key, a table with no key in one pass or by
        where its rows are stored."""
        from .. import canon
        leaf = self._leaf(src_t)
        after = tuple(st["last"]) if st.get("last") is not None else None
        if after is None and "spans" not in st:
            # a fresh start, or a table with no key to resume from: what the
            # target already holds is not this copy's, and writing alone left
            # it there - measured, a stray row survived the move and a
            # key-less table doubled when the move ran again. Under a row
            # filter only the rows it selects are this copy's.
            gone = self.dst_engine.neutral_empty(
                "dst", db, dst_t, **({"where": dst_where} if dst_where
                                     else {}))
            st["moved"] = 0
            if gone:
                log(f"{key}: emptied {gone:,} rows the target held before"
                    " the copy")
        moved = int(st.get("moved", 0))
        absent = 0
        from ..throttle import Throttle
        # the read side is the one under load, so that is what the gate asks
        gate = Throttle(1, probe=lambda: self._health("src", db))
        names = {n for n, _ in src_cols}
        resumable = self.src_engine.neutral_key("src", db, src_t)
        chunk = self._read_rows(db, src_t, chunk)
        from ..wording import progress
        try:
            rows_there = (self.src_engine.table_facts("src", db)
                          .get(src_t) or {}).get("rows")
        except Exception:  # noqa: BLE001 - progress without a total
            rows_there = None
        started, from_rows = time.monotonic(), moved
        if not self.src_engine.RESUMES_BY_KEY:
            resumable = []
        if not resumable or not set(resumable) <= names:
            # nothing to resume from: one pass, a batch at a time, folded
            # as it passes and held to the whole target table after
            whole = (self._read_back_plan(db, src_t, dst_t, src_cols, [])
                     if (self.hop.options or {}).get("verify_batches", True)
                     else None)
            # in parts where it can go on from them: where the source
            # stores its rows, or buckets of the rows' hash
            # (`ranges.keyless`, backlog P0-KEYLESS) - never for a table
            # the hop masks, whose buckets the servers would hash from the
            # values as they are on one side and masked on the other
            from .. import decide, ranges
            way = ranges.keyless(
                f"{db}.{leaf}", st, self.src_engine, self.dst_engine, db,
                src_t, dst_t, whole[0], whole[1], int(rows_there or 0),
                lambda: self.src_engine.position_spans("src", db, src_t,
                                                       chunk),
                self.digest_salt(), decide.HopCosts(self.hop, "keyless/")
            ) if whole and not self._masks(db, src_t) else None
            if way is not None and way.restart:
                gone = self.dst_engine.neutral_empty(
                    "dst", db, dst_t, **({"where": dst_where} if dst_where
                                         else {}))
                log(f"{key}: the copy that stopped went another way; emptied"
                    f" {gone or 0:,} rows it had put on the target")
            if way is not None and way.chunk not in (None, "one pass"):
                if way.chunk != "pages":
                    log(f"{key}: {way.strategy.said} -"
                        f" {way.strategy.reasons.get('chunk', '')}")
                self._copy_in_spans(db, key, src_t, dst_t, src_cols,
                                    dst_cols, way, whole, st, ck, log,
                                    src_where, dst_where, chunk)
                return
            for attempt in (1, 2):
                batches = self.src_engine.neutral_batches(
                    "src", db, src_t, src_cols, chunk, **read_scope)
                folded, total, moved = 0, 0, 0
                while True:
                    with gate.unit():
                        rows = next(batches, None)
                    if rows is None:
                        break
                    rows, flattened = self._flatten_absent(rows)
                    absent += flattened
                    self.dst_engine.neutral_write("dst", db, dst_t,
                                                  dst_cols, rows)
                    moved += len(rows)
                    if whole:
                        k, total = canon.fold_rows(
                            [c for _, c in whole[0]],
                            ([r[i] for i in whole[2]] for r in rows), total)
                        folded += k
                if not whole:
                    break
                there = self.dst_engine.neutral_digest(
                    "dst", db, dst_t, whole[1],
                    **({"where": dst_where} if dst_where else {}))
                if (int(there[0]), str(there[1])) == (folded, str(total)):
                    break
                if attempt == 2:
                    raise SystemExit(
                        f"{key}: copied twice in one pass each (it has no"
                        f" key), and the target holds {int(there[0]):,}"
                        f" rows where {folded:,} were copied, or the same"
                        " number holding different values. The target"
                        " changes what it is given, or another writer is"
                        " writing this table")
                log(f"{key}: the target does not hold what was copied;"
                    " emptying it and copying it again")
                self.dst_engine.neutral_empty(
                    "dst", db, dst_t, **({"where": dst_where} if dst_where
                                         else {}))
            st["moved"] = moved
            log(f"{key}: {moved:,} rows in one pass (no key to resume"
                " from, so a restart starts over)")
        plan = (self._read_back_plan(db, src_t, dst_t, src_cols, resumable)
                if resumable and set(resumable) <= names
                and (self.hop.options or {}).get("verify_batches", True)
                else None)
        def read(point):
            with gate.unit():
                return self.src_engine.neutral_read(
                    "src", db, src_t, src_cols, point, chunk, **read_scope)
        # the next batch is read while this one is written and read back:
        # the two servers work at once, and the checkpoint still moves only
        # past batches written and checked
        import concurrent.futures as cf
        ahead = cf.ThreadPoolExecutor(1)
        try:
            pending = (ahead.submit(read, after)
                       if resumable and set(resumable) <= names else None)
            while pending is not None:
                rows, last = pending.result()
                if not rows:
                    break
                pending = ahead.submit(read, last) if last is not None \
                    else None
                rows, flattened = self._flatten_absent(rows)
                absent += flattened
                self._write_checked(db, key, dst_t, dst_cols, plan,
                                    resumable, rows, log)
                moved += len(rows)
                st["moved"] = moved
                if last is None:
                    log(f"{key}: {moved:,} rows in one pass (no key to"
                        " resume from, so a restart starts over)")
                    break
                after = last
                st["last"] = list(last)
                ck.save()
                # the rate over this run's own rows, not rows an earlier
                # run carried before a restart
                log(progress(key, moved, rows_there, started,
                             time.monotonic(), since=from_rows))
        finally:
            ahead.shutdown(wait=True, cancel_futures=True)
        if absent:
            log(f"{key}: {absent:,} values were not there on the source and"
                f" landed as NULL - {self.dst_name} has no way to store"
                " \"this field is not here\" apart from \"this field is"
                " null\", so the distinction ends at this hop")
        st["done"] = True
        ck.save()


    def _copy_in_spans(self, db, key, src_t, dst_t, src_cols, dst_cols,
                       way, plan, st, ck, log, src_where, dst_where,
                       size=10_000):
        """A table with no key, in the parts `way` cuts it into
        (`ranges.keyless`), each checkpointed once it is written - a stop
        costs the part it was in. By where the source stores its rows or
        the source's own hash of them: a part read whole and written in one
        statement, so it commits whole or not at all, and the target's
        count places a stop (`spans_to_copy`). By buckets of the rows'
        canonical hash, found alike on the target: a bucket streamed in
        batches, marked begun before its first and emptied on the target
        by the same hash if a stop left it part way (`buckets_to_copy`).
        The whole table is then held to the fold of every part; where it
        does not add up and the target finds buckets too, each bucket is
        held to the source's and only those that differ are copied
        again."""
        from .. import canon, decide, ranges
        sc, dc, at = plan[0], plan[1], plan[2]
        classes = [c for _, c in sc]
        began = time.monotonic()
        how = ("by buckets of the rows' hash" if way.chunk != "pages"
               else "by where the rows are stored")

        def clear(where_there):
            return self.dst_engine.neutral_empty(
                "dst", db, dst_t, where=scoped(where_there, dst_where))

        def whole_part(where):
            rows = []
            for batch in self.src_engine.neutral_batches(
                    "src", db, src_t, src_cols, 10 ** 9,
                    where=scoped(where, src_where)):
                rows += batch
            rows, _ = self._flatten_absent(rows)
            if rows:
                self.dst_engine.neutral_write("dst", db, dst_t, dst_cols,
                                              rows)
            return canon.fold_rows(classes, ([r[i] for i in at]
                                             for r in rows))

        def streamed(start, where):
            ranges.bucket_begun(st, start, ck.save)
            n, total = 0, 0
            for batch in self.src_engine.neutral_batches(
                    "src", db, src_t, src_cols, size,
                    where=scoped(where, src_where)):
                rows, _ = self._flatten_absent(batch)
                if rows:
                    self.dst_engine.neutral_write("dst", db, dst_t,
                                                  dst_cols, rows)
                k, total = canon.fold_rows(
                    classes, ([r[i] for i in at] for r in rows), total)
                n += k
            return n, total

        def scoped(where, extra):
            return (f"({where}) and ({extra})" if where and extra
                    else where or extra or None)

        def there():
            return int(self.dst_engine.neutral_digest(
                "dst", db, dst_t, dc,
                **({"where": dst_where} if dst_where else {}))[0])

        def count_of(where):
            return int(self.src_engine.neutral_digest(
                "src", db, src_t, sc, where=scoped(where, src_where))[0])

        def restart():
            self.dst_engine.neutral_empty(
                "dst", db, dst_t, **({"where": dst_where} if dst_where
                                     else {}))
            ck.save()
        for attempt in (1, 2):
            if way.by_bucket:
                todo = ranges.buckets_to_copy(st, way.spans, clear, log, key)
            else:
                todo = [(s_, w, None) for s_, w in ranges.spans_to_copy(
                    st, [(s_, w) for s_, w, _ in way.spans], there,
                    count_of, restart, log, key)]
            for start, where, _ in todo:
                n, total = (streamed(start, where) if way.by_bucket
                            else whole_part(where))
                ranges.span_copied(st, start, n, total, ck.save)
                log(f"{key}: {len(st['spans_done'])} of {len(st['spans'])}"
                    f" parts copied (no key: {how})")
            whole = ranges.spans_total(st)
            got = self.dst_engine.neutral_digest(
                "dst", db, dst_t, dc,
                **({"where": dst_where} if dst_where else {}))
            got = (int(got[0]), str(got[1]))
            if whole is not None and got == (whole[0], str(whole[1])):
                break
            if whole is None:
                # a span counted in after a stop has no fold: the source
                # itself is asked, as it is now
                ours = self.src_engine.neutral_digest(
                    "src", db, src_t, sc,
                    **({"where": src_where} if src_where else {}))
                if got == (int(ours[0]), str(ours[1])):
                    break
            if attempt == 2:
                raise SystemExit(
                    f"{key}: copied twice {how} (it has no key), and the"
                    f" target holds {got[0]:,} rows that do not add up to"
                    " what was copied. The target changes what it is"
                    " given, or another writer is writing this table")
            if way.by_bucket:
                # held bucket by bucket to the source's: only those that
                # differ are emptied there and copied again
                bad = []
                for start, where, where_there in way.spans:
                    a = self.src_engine.neutral_digest(
                        "src", db, src_t, sc, where=scoped(where, src_where))
                    b = self.dst_engine.neutral_digest(
                        "dst", db, dst_t, dc,
                        where=scoped(where_there, dst_where))
                    if (int(a[0]), str(a[1])) != (int(b[0]), str(b[1])):
                        bad.append(start)
                log(f"{key}: the target does not hold what was copied;"
                    f" {len(bad)} of {len(way.spans)} buckets differ, and"
                    " only those are copied again")
                st["spans_done"] = [x for x in st["spans_done"]
                                    if x not in bad]
                for x in bad:
                    st["span_tally"].pop(str(x), None)
                    ranges.bucket_begun(st, x, ck.save)
                ck.save()
                continue
            log(f"{key}: the target does not hold what was copied;"
                " emptying it and copying it again")
            for k in ("spans", "spans_done", "span_tally"):
                st.pop(k, None)
        # the way's measured rate, a row, for the next table's climb
        costs = decide.HopCosts(self.hop, "keyless/")
        took = time.monotonic() - began
        for part, rung in way.strategy.names.items():
            costs.saw(f"{part}/{rung}", max(got[0], 1), took)
        st["done"] = True
        ck.save()

    def _splits(self, db, src_t, dst_t, src_cols, st, src_where,
                shared=False):
        """(its key, the source's rows) where a table is copied as ranges
        of its key - one integer key, a SQL dialect on both sides, a
        target many write at once, and large enough - or None. Where the
        hop's machines share ranges (`claims`), one machine's workers are
        no reason not to split: the others' are there too; and a table
        they decided to share (`shared`) is not held to its size again - a
        machine reading a smaller count would copy it whole beside the
        others' ranges."""
        from .. import claims, crossmove, ranges
        workers = int(getattr(self.hop, "workers", 1) or 1)
        src, dst = self.src_engine, self.dst_engine
        k = list(src.neutral_key("src", db, src_t) or [])
        # a target one writer at a time: ranges side by side waited on its
        # lock and stopped - measured, SQLite to SQLite, 500,000 rows,
        # `database is locked` in the seventh of eight ranges
        if (len(k) != 1 or not src.SQL_DIALECT
                or not dst.SQL_DIALECT or not src.RESUMES_BY_KEY
                or not getattr(dst, "WRITES_IN_PARALLEL", True)
                or dict(src_cols).get(k[0]) != "integer"):
            return None
        others = crossmove.besides_the_copier(self, db, src_t, dst_t)
        machines = claims.shared_ranges(self.hop)
        if workers < 2 and not others and not machines:
            return None
        try:
            rows = int((src.table_facts("src", db).get(src_t) or {})
                       .get("rows") or 0)
        except Exception:  # noqa: BLE001 - not split, then
            rows = 0
        if "ranges" not in st and rows <= 2 * ranges.LEAST and not shared:
            # an estimate, which a table never analysed does not have:
            # where a way besides the copier would take the ranges, or
            # other machines would, the rows are counted as far as it
            # matters
            if not others and not machines:
                return None
            rows = crossmove.rows_upto(self, db, src_t, src_where,
                                       2 * ranges.LEAST)
            if rows <= 2 * ranges.LEAST:
                return None
        return k[0], rows

    def splits_across_machines(self, db, src_t):
        """Whether the hop's machines would share `src_t`'s ranges: asked
        once, by the first machine to reach the table (`claims.way`)."""
        from .. import claims
        if not claims.shared_ranges(self.hop):
            return False
        dst_t = self._leaf(self._rename(src_t, db))
        try:
            src_cols = self._move_columns(src_t, dst_t, db)[0]
            src_where, _ = self._row_scope(db, src_t)
        except Exception:  # noqa: BLE001 - copied whole, then
            return False
        return self._splits(db, src_t, dst_t, src_cols, {},
                            src_where) is not None

    def _move_in_ranges(self, db, key, src_t, dst_t, src_cols, chunk, st,
                        ck, log, src_where, dst_where):
        """A large table with one integer key, copied as ranges of equal
        rows in processes of their own. The copier's work per row is
        Python's - reading the driver's rows, rendering, writing - and
        threads gave it nothing; processes did (measured, a million rows
        MySQL to PostgreSQL: 10.6 s in one, 8.1 s in two, 7.0 s in four,
        on a 2-CPU server). Each range empties its own span on the target,
        is copied and read back as every batch is, and is checkpointed when
        it is done; a run started again copies only the ranges not done.
        Returns whether the table was copied this way.

        Each range climbs the ways a cross-engine table goes
        (`crossmove.TableWay`, on the engine's own chooser): where it is
        not the table copier's own, the rows go server to server and a
        range the two servers do not hold alike is copied again this way.
        Where such a way holds, one worker splits the table too - one
        transaction of the whole table would restart from nothing.

        Where the hop's machines share the table's ranges, every machine's
        workers take them from one record instead (`claims.copy`), each
        written by the table copier under its claim's number."""
        import dataclasses
        import pickle

        from .. import claims, crossmove, ranges
        workers = int(getattr(self.hop, "workers", 1) or 1)
        src, dst = self.src_engine, self.dst_engine
        shared = claims.way_of(self.hop, db, key) == "ranges"
        split = self._splits(db, src_t, dst_t, src_cols, st, src_where,
                             shared)
        if split is None:
            return False
        k, rows = split
        # the hop as its fields say, without what a caller set on it here
        hop = dataclasses.replace(self.hop)
        try:
            pickle.dumps(hop)
        except Exception:  # noqa: BLE001 - one process, then
            if shared:
                raise
            return False
        q = src._quote_ident(k)
        table = src._qualified("src", db, src_t)
        got = src.run_rule("src", db, f"select min({q}), max({q}) from"
                                      f" {table}" + (f" where ({src_where})"
                                                     if src_where else ""))
        if not got or got[0][0] is None:
            if not shared:
                return False
            # nothing on the source to share: the target holds nothing of
            # it either, whichever machine gets here
            dst.neutral_empty("dst", db, dst_t,
                              **({"where": dst_where} if dst_where else {}))
            st.update(done=True, moved=0)
            ck[key] = st
            ck.save()
            return True
        lo, hi = int(got[0][0]), int(got[0][1])

        def edges():
            # shared, the ranges are cut for more workers than this
            # machine's: the machines that join later take theirs from the
            # same plan, which is made once
            every = ranges.step(rows, rows, workers * (4 if shared else 1))
            return [r[0] for r in src.run_rule("src", db, ranges.bounds_sql(
                src._quote_ident, table, k, every, src_where))]

        def outside():
            # a target row outside the source's whole range was in none of
            # the ranges
            dst_k = dict(zip([n for n, _ in src_cols],
                             [n for n, _ in self._move_columns(
                                 src_t, dst_t, db)[1]])).get(k, k)
            qd = dst._quote_ident(dst_k)
            out = f"not ({qd} >= {lo} and {qd} <= {hi})"
            gone = dst.neutral_empty(
                "dst", db, dst_t, where=f"({dst_where}) and {out}"
                if dst_where else out)
            if gone:
                log(f"{key}: removed {gone:,} target rows the source does"
                    " not have")
        if shared:
            return claims.copy(
                self, db, key, src_t, k, ck, lo, hi, edges, log, _copy_range,
                lambda a, u, token: (hop, db, src_t, dst_t, a, u,
                                     {"token": token}),
                outside)
        way = crossmove.TableWay(self, db, src_t, dst_t, log, key)
        todo = ranges.plan(st, lo, hi, edges, ck.save, table=src_t)
        st["key"] = k
        total = len(st["ranges"])
        # every range's process writes under the number this process
        # writes under, where one is armed (`fencing`)
        token = fencing.armed(self.hop)
        plain = token.plain() if token is not None else None

        def finished(item, got):
            rng, asked = item[4:6], item[6]
            moved, used, fell = got
            ranges.finished(st, rng[0], ck.save)
            if fell:
                way.fell(used, fell)
            else:
                way.took(used, moved, time.monotonic() - asked["began"])
            log(f"{key}: {k} {rng[0] + 1:,} to {rng[1]:,} copied,"
                f" {moved:,} rows ({len(st['ranges_done'])} of {total}"
                " ranges)")

        def begin(item):
            # what the source had made visible as the range is handed over,
            # before its process reads it (`ranges.already_read`); and the
            # way this range goes, climbed now on what the table's ranges
            # have measured so far - the item is handed over after this
            ranges.started(st, item[4], src.snapshot_mark("src", db),
                           ck.save)
            item[6]["way"] = way.pick()
            item[6]["began"] = time.monotonic()
        # the move's slots where a move runs; the hop's workers where the
        # table is copied on its own
        slots = (ranges.active if ranges.active.workers > 1
                 else ranges.Slots(workers))
        slots.each_process(
            [(hop, db, src_t, dst_t, a, u, {"token": plain})
             for a, u in todo],
            _copy_range, finished, begin)
        outside()
        st["done"] = True
        ck.save()
        return True

    def copy_range(self, db, src_t, dst_t, after, upto, way=None):
        """One range of `_move_in_ranges`, in the process running it:
        (rows copied, the way asked, why it fell or None). By `way` where
        that is one of `crossmove`'s, and proved by the digest on both
        servers; otherwise - or where that did not prove itself - its span
        emptied on the target, then copied and read back batch by batch."""
        from .. import crossmove
        src, dst = self.src_engine, self.dst_engine
        src_cols, dst_cols, _ = self._move_columns(src_t, dst_t, db)
        (k,) = src.neutral_key("src", db, src_t)
        dst_k = dict(zip([n for n, _ in src_cols],
                         [n for n, _ in dst_cols])).get(k, k)
        way = way or crossmove.COPIER
        fell = None
        if way != crossmove.COPIER:
            moved, fell = crossmove.range_by(self, db, src_t, dst_t, k, dst_k,
                                             after, upto, way)
            if not fell:
                return moved, way, None
        src_where, dst_where = self._row_scope(db, src_t)

        def scope(eng, name, extra):
            return crossmove.scoped(eng, name, after, upto, extra)
        dst.neutral_empty("dst", db, dst_t, where=scope(dst, dst_k,
                                                        dst_where))
        plan = (self._read_back_plan(db, src_t, dst_t, src_cols, [k])
                if (self.hop.options or {}).get("verify_batches", True)
                else None)
        label = self.move_key(db, "", self._leaf(src_t))
        where = scope(src, k, src_where)
        chunk = self._read_rows(db, src_t, 10 ** 9)
        point, moved = None, 0
        while True:
            rows, last = src.neutral_read("src", db, src_t, src_cols, point,
                                          chunk, where=where)
            if not rows:
                break
            rows, _ = self._flatten_absent(rows)
            self._write_checked(db, label, dst_t, dst_cols, plan, [k], rows,
                                lambda m: None)
            moved += len(rows)
            if last is None:
                break
            point = last
        return moved, way, fell

    def _write_checked(self, db, key, dst_t, dst_cols, plan, resumable,
                       rows, log):
        """A batch written, then read back from the target by its keys and
        compared with what was read from the source - so a move that
        finishes has been checked row for row as it went, not only by a
        `check` run afterwards. A batch that reads back different is
        written once more; if it still does, the copy stops there and
        names it. The checkpoint has not moved past the batch before, so a
        run started again begins with it."""
        from .. import failpoint
        self.dst_engine.neutral_write("dst", db, dst_t, dst_cols, rows)
        failpoint.hit("batch.written")
        if plan is None:
            return
        quick = self._batch_digest(db, dst_t, plan, resumable, rows)
        if quick is not None and quick[0] == quick[1]:
            return
        missing, changed, cols = self._verify_batch(db, dst_t, plan,
                                                    resumable, rows)
        if not (missing or changed):
            if quick is not None and quick[1][0] > quick[0][0]:
                log(f"{key}: {quick[1][0] - quick[0][0]:,} rows in this"
                    " batch's key range are only on the target - written"
                    " there by something else while the copy ran")
            return
        log(f"{key}: {len(missing) + len(changed):,} rows of a batch read"
            " back from the target different from what was written;"
            " writing it again")
        self.dst_engine.neutral_rewrite("dst", db, dst_t, dst_cols, rows)
        missing, changed, cols = self._verify_batch(db, dst_t, plan,
                                                    resumable, rows)
        if not (missing or changed):
            return
        first = (changed or missing)[0]
        raise SystemExit(
            f"{key}: written twice, {len(missing):,} rows of a batch are"
            f" not on the target and {len(changed):,} read back different"
            f" from the source - the first by key {', '.join(first)}"
            + (f", in {', '.join(cols[:6])}" if cols else "")
            + f". The target changes what {self.dst_name} is given, or"
            " cannot hold it as it is. The copy stopped here; the batches"
            " before it were read back equal")

    def _still_the_same(self, db, key, src_t, dst_t, st, log):
        """A table an earlier run finished, digested on both sides before
        it is skipped: the source may have changed since, and "done
        earlier, skip" left the target behind it without a word. The same,
        it stays done; different, it is copied again."""
        sc, dc, _ = self._comparable_columns(db, self.src_engine, src_t,
                                             self.dst_engine, dst_t)
        if not sc:
            log(f"{key}: done earlier; no column of it can be compared on"
                " both sides, so it is skipped unchecked")
            return True
        src_where, dst_where = self._row_scope(db, src_t)
        a = self.src_engine.neutral_digest(
            "src", db, src_t, sc, **({"where": src_where} if src_where
                                     else {}))
        b = self.dst_engine.neutral_digest(
            "dst", db, dst_t, dc, **({"where": dst_where} if dst_where
                                     else {}))
        if (int(a[0]), str(a[1])) == (int(b[0]), str(b[1])):
            log(f"{key}: done earlier, and both sides still hold the same"
                " rows - skipped")
            return True
        st.clear()
        log(f"{key}: done earlier, and the two sides no longer hold the"
            " same rows - copying it again")
        return False

    #: what one read of the table copier holds, at most: rows, and bytes
    #: as the source's catalogue counts them
    READ_ROWS = pieces.PAIR_ROWS
    READ_BYTES = pieces.BATCH_BYTES

    def _read_rows(self, db, src_t, chunk):
        """Rows per read: `chunk` asked for 500,000 at a time, and a read
        is held whole until it is written. Measured, a text-keyed table of
        about 250 bytes a row: 486 MB held at 500,000 rows a read - and a
        table of wide rows would hold that many times over. Capped by a
        count and by the table's own bytes a row, so what is held does not
        grow with the row either. Each read is still a resumable step, and
        ends by itself at the row that brings it to `pieces.BATCH_BYTES`
        where rows are wider than their table's average (`pieces`)."""
        per_row = None
        try:
            facts = self.src_engine.table_facts("src", db).get(src_t) or {}
            if facts.get("rows") and facts.get("bytes"):
                per_row = max(int(facts["bytes"]) // int(facts["rows"]), 1)
        except Exception:  # noqa: BLE001 - no estimate is the count alone
            per_row = None
        rows = min(int(chunk), self.READ_ROWS)
        if per_row:
            rows = min(rows, max(self.READ_BYTES // per_row, 1))
        return max(rows, 1)

    def _target_shape(self, db, src_table, leaf):
        """([(name, class, numbers)], key) for the table to build.

        Sorted by name, like everything else that has to line up across two
        engines. A column whose type has no neutral class stops the build
        rather than being guessed at - the message names it so the operator
        can write the table themselves.
        """
        from .. import canon
        # built under the target's names, without the columns the hop drops
        mapped, back = self._mapped_types(db, src_table, dict(
            self.src_engine.neutral_columns("src", db, src_table)))
        declared = list(mapped.items())
        rules = self._carried_rules(db, src_table, back)
        columns, unknown = [], []
        same = self.src_engine.CANON_ENGINE == self.dst_engine.CANON_ENGINE
        from ..storedcode import sizing
        if (self.hop.options or {}).get("types_from_data") and not same:
            # the columns whose declaration leaves the domain open, read
            # from the data in one pass (`storedcode.sizing`)
            sizing.wanted(self, db, src_table,
                          {back[n]: t for n, t in declared})
        for name, typ in sorted(declared):
            cls = canon.type_class(self.src_engine.CANON_ENGINE, typ)
            if same:
                # the same engine on both sides takes the source's own type
                # as it is written. Through the classes, a `timestamptz`
                # was built as `timestamp(6)` - the offset gone - and an
                # `int` as `bigint`
                columns.append((name, canon.OWN, (typ,),
                                rules.get(name) or {}))
                continue
            if cls is None:
                unknown.append(f"{name} ({typ})")
                continue
            if self._accepted(src_table, back[name]):
                # a MongoDB value whose type the hop lets change is built
                # as the column its value alone goes in: an ObjectId as
                # text
                cls = canon.mongo_value_class(self.src_engine.CANON_ENGINE,
                                              typ, cls)
            numbers = canon.ddl_numbers(self.src_engine.CANON_ENGINE, typ,
                                        cls)
            # with the hop's `types_from_data`, the data's size where the
            # declaration leaves the domain open, recorded with its evidence
            cls, numbers = sizing.sized(self, db, src_table, back[name], name,
                                        typ, cls, numbers)
            if cls == "decimal" and not numbers:
                numbers = self._measured_decimal(db, src_table, back[name])
            columns.append((name, cls, numbers, rules.get(name) or {}))
        if unknown:
            raise SystemExit(
                f"{leaf} is not on the target and migkit cannot build it:"
                f" no neutral class for {', '.join(unknown)}."
                " Create the table yourself and run this again - guessing a"
                " column type is how a migration arrives complete and wrong")
        try:
            forward = {v: k for k, v in back.items()}
            key = [forward.get(k, k) for k in
                   self.src_engine.neutral_key("src", db, src_table)
                   if forward.get(k, k) in {c[0] for c in columns}]
        except Exception:
            key = []
        if not same:
            # a class the target has no type for is named here, with why,
            # rather than stopping the build on the first
            none = []
            for name, cls, numbers, _ in columns:
                try:
                    canon.ddl_type(self.dst_engine.CANON_ENGINE, cls,
                                   numbers, name in key)
                except ValueError as e:
                    none.append(f"{name} ({mapped.get(name)}): {e}")
            if none:
                raise SystemExit(
                    f"{leaf} is not on the target and migkit cannot build"
                    f" it: {self.dst_name} has no column that holds these"
                    " as they are - " + "; ".join(none) + ". Create the"
                    " table yourself with the columns you want them in (a"
                    " value a column changes is then counted and refused,"
                    " or let through by the hop's `accept_changes`), and"
                    " run this again")
        return columns, key

    def _measured_decimal(self, db, src_table, column):
        """(precision, scale) to build a decimal the source declares
        without one (PostgreSQL's plain `numeric`) as, on a target whose
        decimal has a fixed scale: the target's own default, widened to the
        most digits before and after the point the source holds. () where
        the target keeps any decimal as it is given, or where even its
        widest does not hold them - then the table is built at the default
        and what does not fit is counted and refused before the move.

        The default was the whole answer: `decimal(65,10)` on MySQL, which
        rounds a longer fraction without an error in strict mode (Note
        1265) - measured, 0.12345678901234 arrived as 0.1234567890."""
        from .. import canon
        dst = self.dst_engine.CANON_ENGINE
        try:
            wide = canon._fixed_decimal(dst, canon.ddl_type(dst, "decimal"))
        except ValueError:
            return ()
        most = canon.DECIMAL_MOST.get(dst)
        if not wide or not most:
            return ()
        known = self.__dict__.setdefault("_decimal_extents", {})
        at = (db, str(src_table), column)
        if at not in known:
            known[at] = self.src_engine.decimal_extent(
                "src", db, src_table, column,
                self._row_scope(db, src_table)[0])
        whole, frac = known[at]
        scale = max(wide[1], frac)
        precision = max(wide[0], whole + scale)
        if scale > most[1] or precision > most[0]:
            return ()
        return (precision, scale)

    def create_missing(self, db, log=None):
        """The tables the target lacks, built the way the copier builds
        them, before any is filled. The copier from MySQL to PostgreSQL
        filled tables and built none: onto an empty target it stopped on
        `relation "t" does not exist`."""
        if self.dst_engine.CREATES_ON_WRITE or not self._can_compare_neutrally():
            return
        log = log or (lambda m: None)
        made = self.dst_engine.prepare_target(db)
        if made:
            log(f"{self.dst_name}: {made}")
        there = ([] if self.dst_engine.target_missing(db)
                 else self.dst_engine.neutral_tables("dst", db))
        _, src_only, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db), there, self._renamer(db))
        for src_t in src_only:
            if self.hop.takes_table(db, src_t):
                self._create_target(db, src_t,
                                    self._leaf(self._rename(src_t, db)), log)

    def finish_created(self, db, log=None):
        """The source's indexes on the tables this pair built, once their
        rows are in - the faster order, as on every other path.

        A built table had none: every query the application made after
        cutover read it whole, and a unique index the source kept was not
        there to refuse the duplicate it refuses. One over an expression, a
        predicate or a part of a column cannot be given to another engine
        as it is, and is named instead."""
        from .. import durable
        log = log or (lambda m: None)
        record = self.hop.report_dir(db) / "built-tables.json"
        built = durable.read_state(record, "state")
        if not built:
            return
        for leaf, src_t in sorted(built.items()):
            try:
                indexes = self.src_engine.neutral_indexes("src", db, src_t)
                names = [n for n, _ in
                         self.src_engine.neutral_columns("src", db, src_t)]
            except Exception as e:
                log(f"{leaf}: its indexes could not be read:"
                    f" {str(e).splitlines()[-1][:90]}")
                continue
            _, back = self._mapped_types(db, src_t, {n: None for n in names})
            forward = {v: k for k, v in back.items()}
            for name, unique, cols, plain in indexes:
                if not plain or not cols or any(c not in forward
                                                for c in cols):
                    log(f"{leaf}: index {name} not carried - it is over an"
                        " expression, a predicate, part of a column, or a"
                        " column the hop does not carry")
                    continue
                self._build_index(db, leaf, name, unique,
                                  [forward[c] for c in cols], log)
        record.unlink(missing_ok=True)

    def _build_index(self, db, leaf, name, unique, cols, log):
        """One index on the target, under the source's name, or under the
        table's name before it where that one is taken - an index name is
        the whole schema's on some engines and one table's on others."""
        for attempt in (name, f"{leaf}_{name}"[:63]):
            sql = self.dst_engine.neutral_create_index_sql(
                "dst", db, leaf, attempt, unique, cols)
            try:
                self.dst_engine.execute_ddl("dst", db, sql)
                log(f"{leaf}: {sql}")
                return
            except Exception as e:
                err = str(e).splitlines()[-1][:120] if str(e) else ""
                if "exist" in err.lower() and attempt == name:
                    continue
                log(f"{leaf}: index {name} not built: {err}")
                return

    def _carried_rules(self, db, src_table, back):
        """{target column: rule} - what `neutral_column_rules` says of the
        source's columns, with each default in the target's own SQL.

        A default is an expression in the source's dialect: `uuid()` on
        MySQL is `gen_random_uuid()` on PostgreSQL. It is translated, then
        asked of the target, and one the target does not take is left off
        and named in `self.rules_not_carried` rather than guessed at - the
        table is still built, and the check reports the default missing."""
        self.rules_not_carried = []
        try:
            got = self.src_engine.neutral_column_rules("src", db, src_table)
        except Exception:
            return {}
        out = {}
        for target, source in back.items():
            rule = dict(got.get(source) or {})
            if not rule:
                continue
            default = rule.get("default")
            if default is not None and not rule.get("identity"):
                rule["default"] = self._default_here(db, default)
                if rule["default"] is None:
                    self.rules_not_carried.append(f"{target} default"
                                                  f" {default}")
            out[target] = rule
        return out

    def _row_scope(self, db, src_table):
        """The hop's row filter for this table, in each side's SQL:
        (the source's, the target's), or (None, None) without one.

        The filter is written in the source's SQL against the source's
        column names. For the target it is translated into that engine's
        SQL, with each column under the name the hop's mapping gives it -
        the check has always read both sides through the filter, and on a
        pair the two sides do not speak the same SQL or use the same
        names. One that cannot be translated stops everything that would
        read through it, before anything is read.

        Said in the target's words by the one translation
        (`rowfilter.to_sql`); a document store takes it in SQL and reads it
        as its own query (`Engine.filter_dialect`, `rowfilter.to_mongo`).
        Where the hop says the zone the filter's times are read in, each
        side's is made zone-free by that side's own column types
        (`rowfilter.zone_free`)."""
        pred = self.hop.row_filter(db, *self.hop.parts_of(src_table))
        if not pred:
            return None, None
        # translated once a table and filter, until the source's shape
        # changes (`_shape_now`): a column lookup and a parse, measured
        # 113 us a call, made for every filtered table every batch (audit
        # SP12)
        kept = self.__dict__.setdefault("_scopes", {})
        at = ("scope", db, str(src_table), pred)
        if at not in kept:
            kept[at] = self._row_scope_of(db, src_table, pred)
        return kept[at]

    def _row_scope_of(self, db, src_table, pred):
        from .. import rowfilter
        src = self.src_engine.filter_dialect()
        dst = self.dst_engine.filter_dialect()
        if not (src and dst):
            raise SystemExit(
                f"{src_table} moves under a row filter, which is SQL, and"
                f" {self.src_name if not src else self.dst_name} takes none."
                " Drop the filter, or exclude the table from this hop")
        names = [n for n, _ in
                 self.src_engine.neutral_columns("src", db, src_table)]
        _, back = self._mapped_types(db, src_table, {n: None for n in names})
        forward = {v: k for k, v in back.items()}
        try:
            there = rowfilter.to_sql(pred, src, dst, renames=forward)
        except SystemExit as e:
            raise SystemExit(
                f"the row filter on {src_table} ({pred}) has no translation"
                f" into {self.dst_name}'s SQL"
                f" ({str(e).splitlines()[-1][:80]}), so the target cannot be"
                " read through it. Write it so both engines accept it")
        tz = self.hop.row_filter_zone(db, *self.hop.parts_of(src_table))
        if tz:
            mine = self.src_engine.time_kinds("src", db, src_table)
            try:
                theirs = self.dst_engine.time_kinds(
                    "dst", db, self._rename(src_table, db))
            except Exception:  # noqa: BLE001 - not made yet: as it will be
                theirs = {forward.get(k, k): v for k, v in mine.items()}
            pred = rowfilter.zone_free(pred, src, tz, mine)
            there = rowfilter.zone_free(there, dst, tz, theirs)
        return pred, there

    def _default_here(self, db, expr):
        """The source's default in the target's SQL, or None where it has
        no translation the target accepts."""
        src = self.src_engine.SQL_DIALECT
        dst = self.dst_engine.SQL_DIALECT
        if not (src and dst):
            return None
        said = expr
        if src != dst:
            try:
                import sqlglot
                said = sqlglot.transpile(
                    expr, read=src, write=dst,
                    unsupported_level=sqlglot.ErrorLevel.RAISE)[0]
            except Exception:
                return None
        return said if self.dst_engine.default_works("dst", db, said) \
            else None

    def _create_target(self, db, src_table, leaf, log):
        """Build the table the rows are about to land in, and say what it ran.

        A missing target used to stop the move. Creating it is what the
        operator asked for by starting one - but only when it is genuinely
        missing: `neutral_create` refuses an existing table rather than
        altering it, so a name that already means something on the target is
        never quietly redefined.

        The types come from the source's classes, widened where the class
        does not carry the source's own numbers. Wider cannot truncate;
        narrower can, so the direction of the guess is not a coin toss.
        """
        columns, key = self._target_shape(db, src_table, leaf)
        # recorded before it is made: a copy that dies after this still has
        # the table's indexes added by the next run's `finish_created`
        from .. import durable
        record = self.hop.report_dir(db) / "built-tables.json"
        built = durable.read_state(record, "state") or {}
        built[leaf] = str(src_table)
        durable.write_json(record, built)
        ddl = self.dst_engine.neutral_create("dst", db, leaf, columns, key)
        log(f"{leaf}: not on the target, created it - {ddl}")
        for lost in getattr(self, "rules_not_carried", []):
            log(f"{leaf}: not carried, the target has no equivalent: {lost}")
        if not key:
            log(f"{leaf}: created without a primary key, because the source"
                " has none - the rows still move, and a restart starts over")
        return leaf

    def _flatten_absent(self, rows):
        """Turn `canon.ABSENT` into None when the target cannot hold it.

        Counted rather than quietly converted. MongoDB keeps a missing field
        and a null field apart; a SQL column has one state for both, so the
        move is lossy in a direction nobody notices unless the number is
        printed. When the target *can* express it, nothing is touched.
        """
        from .. import canon
        if self.dst_engine.EXPRESSES_ABSENT:
            return rows, 0
        n = 0
        out = []
        for row in rows:
            if any(v is canon.ABSENT for v in row):
                n += sum(1 for v in row if v is canon.ABSENT)
                row = [None if v is canon.ABSENT else v for v in row]
            out.append(row)
        return out, n

    def _flatten_changes(self, changes):
        """The same decision, for change records rather than rows.

        One rule, two shapes: a full load carries a list of values and a tail
        carries a mapping, and both have to make the same call about a target
        that cannot store "not there".
        """
        from .. import canon
        if self.dst_engine.EXPRESSES_ABSENT:
            return changes, 0
        n = 0
        out = []
        for change in changes:
            values = change.get("values") or {}
            if any(v is canon.ABSENT for v in values.values()):
                n += sum(1 for v in values.values() if v is canon.ABSENT)
                change = dict(change)
                change["values"] = {k: (None if v is canon.ABSENT else v)
                                    for k, v in values.items()}
            out.append(change)
        return out, n

    def _can_compare_neutrally(self):
        return bool(self.src_engine.CANON_ENGINE
                    and self.dst_engine.CANON_ENGINE)

    def _url(self, side, db):
        from urllib.parse import quote
        ep = self.hop.source if side == "src" else self.hop.target
        proto = "mysql" if side == "src" else "postgresql"
        return (f"{proto}://{ep.user}:{quote(ep.password, safe='')}"
                f"@{ep.host}:{ep.port}/{db}")

    def _no_rendering(self, check):
        return Result(check, "", "error",
                      f"{self.src_name}->{self.dst_name}: one of these"
                      " engines has no canonical rendering yet, so migkit"
                      " will not claim the two sides agree")

    def _counts_rows(self, db, rows):
        """The count verdict for rows already compared, in one place.

        `check counts` and `check data --with-counts` are two readings of the
        same pass, so they are one function - a second copy would be free to
        disagree with the first about the same tables.
        """
        if not rows:
            return [Result("counts", db, "warn",
                           "no table on either side, so nothing was compared"
                           " - unknown, not clean")]
        bad = [(scope, status, detail) for scope, status, detail, a, b in rows
               if status != "ok" or a != b]
        if bad:
            # ten said in full, the rest named: a table left unsaid read as
            # one that counted alike beside ten the hop keeps apart
            # (`verdict.hold`)
            more = [scope for scope, _, _ in bad[10:]]
            return [Result("counts", scope, status, detail,
                           table=self._named_as(scope))
                    for scope, status, detail in bad[:10]] + (
                [Result("counts", db, "diff",
                        f"{len(more)} more tables count differently or"
                        " could not be counted: " + ", ".join(more[:20])
                        + (" ..." if len(more) > 20 else ""))]
                if more else [])
        total = sum(a for _, _, _, a, _ in rows if a is not None)
        return [Result("counts", db, "ok",
                       f"rows {total:,}=={total:,} across"
                       f" {self.src_name}/{self.dst_name},"
                       f" {len(rows)} tables")]

    def check_autoinc(self, db):
        """Whether the target's sequences are past its rows, asked of the
        target alone: the change tail writes each row with the source's key
        too, and a sequence does not move for a key it did not hand out, so
        the first insert after cutover would collide with a row the tail
        wrote. Skipped where the target keeps no such counter apart from
        the rows (MySQL moves its own on every insert)."""
        behind = getattr(self.dst_engine, "sequences_behind", None)
        if behind is None:
            return super().check_autoinc(db)
        got = behind(db)
        if not got:
            return [Result("autoinc", db, "ok",
                           "every sequence on the target is past the rows"
                           " of its column")]
        return [Result(
            "autoinc", db, "diff",
            f"{len(got)} sequences on the target would hand out a key a row"
            " already holds: " + ", ".join(
                f"{seq} (largest {table}.{col} is {top})"
                for seq, table, col, top in got[:4])
            + (" ..." if len(got) > 4 else ""), "",
            f"migkit sync {self.hop.name} --db {db} --kind sequences"
            " --apply")]

    def settle_target(self, db, from_source=True):
        """The target's own, with the source's counters only where the
        source is the same engine and so can be read by it."""
        return self.dst_engine.settle_target(
            db, from_source and self.src_name == self.dst_name)

    def check_deep(self, db):
        """What the target's own engine set aside for the pair's loads and
        tail and has not put back; the converted code proved by running
        it, and - where the hop names a sandbox - the schema's
        constraints, collations and sized types proved on its fixture."""
        schema = []
        try:
            proved = self.prove_converted(db, schema)
        except Exception:  # noqa: BLE001 - a pair with no SQL on one side
            proved = None
        out = [r for r in (self.dst_engine.set_aside(db),
                           self._zero_date_result(db),
                           self._unfit_result(db)
                           if self._can_move_neutrally() else None,
                           self._pair_temporal_meaning(db)
                           if self._can_compare_neutrally() else None,
                           self.dst_engine._fk_orphans(db)) if r]
        # a side that is files says whether every record of them reads
        # (`deep_files`): a value that is not one, a file cut short
        for side, eng in (("src", self.src_engine), ("dst", self.dst_engine)):
            files = getattr(eng, "deep_files", None)
            if files is not None:
                out += files(side, db)
        # the source's own statements, translated, planned and timed on
        # both engines and held to the same answers (R23, 4), where the
        # source keeps statement statistics and both sides speak SQL
        perf = []
        if getattr(self.src_engine, "workload_reads", None) is not None \
                and self.src_engine.SQL_DIALECT \
                and self.dst_engine.SQL_DIALECT:
            from .. import workload
            perf.append(workload.compare(self, db))
        return (out or super().check_deep(db)) \
            + ([proved] if proved else []) + schema + perf

    def _assess_extra(self):
        """How deep migkit reaches each side, where its engine says
        (`reach_items`): a Db2 for z/OS source is copied and verified and
        never followed, an IBM i one followed only where its tables are
        journaled with both images."""
        items = []
        for side, eng in (("src", self.src_engine), ("dst", self.dst_engine)):
            reach = getattr(eng, "reach_items", None)
            if reach is not None:
                items += reach(side)
        return items

    def load_window(self, db, log=None, tables=None):
        """The target's own, over the names the tables have there."""
        if tables is not None:
            tables = {self._leaf(self._rename(t, db)) for t in tables}
        return self.dst_engine.load_window(db, log, tables)

    def planned_checks(self):
        """A second reading, where it can be had.

        A cross-engine pair is where a second opinion earns its cost: every
        value is converted on the way, and migkit's own reading is the only
        one looking. So it runs whenever the independent reader is installed
        and speaks both engines, and never for pairs it cannot read.
        """
        more = tuple(c for c in super().planned_checks() if c != "second")
        if self._can_read_twice():
            return ("second",) + more
        return more

    def run_rule(self, side, db, sql):
        engine = self.src_engine if side == "src" else self.dst_engine
        return engine.run_rule(side, db, sql)

    def _second_pairs(self, db):
        """The tables the second reading reads, paired as the check pairs
        them - through the hop's renames."""
        pairs, _, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db), self._renamer(db))
        return pairs

    def _second_views(self, db):
        """(name, the source's query, the target's) for each view the
        source has and the target has under the name the conversion
        gives it."""
        there = self.target_names(db)
        out = []
        for name, _ in self.src_engine.neutral_views("src", db):
            leaf = self._leaf(name)
            if leaf in there:
                out.append((leaf,
                            "select * from " + self.src_engine._qualified(
                                "src", db, name),
                            "select * from " + self.dst_engine._qualified(
                                "dst", db, leaf)))
        return out

    def check_counts(self, db):
        if not self._can_compare_neutrally():
            got = self._no_rendering("counts")
            return [Result("counts", db, got.status, got.detail)]
        return self._counts_rows(db, self._neutral_rows(db))

    def check_data(self, db, table=None, stream=None, with_counts=False):
        """One comparison for every pair, including the pair reladiff used
        to handle.

        The reladiff branch that lived here was written when the digest could
        only answer *whether* two tables differ. It names *which* rows now,
        writes them where `migkit sync` can read them, and does it the same
        way for all nine combinations - so MySQL to PostgreSQL was the one
        pairing whose check produced nothing a repair could act on.

        Measured before removing it, against a real pair: reladiff has no
        MySQL driver in this installation and says so - `ERROR - No module
        named 'mysql'` - then exits 0, which this branch read as a row-level
        difference. `diff` on a table nobody compared, and no files behind
        it.
        """
        if not self._can_compare_neutrally():
            got = self._no_rendering("data")
            return [Result("data", db, got.status, got.detail)]
        rows = self._neutral_rows(db, table, stream)
        rows = self._confirm_in_flight(db, rows, stream)
        res = [Result("data", scope, status, detail,
                      table=self._named_as(scope))
               for scope, status, detail, _, _ in rows]
        res += [r for r in (self._second_rows(db, scope)
                            for scope, status, _, _, _ in rows
                            if status == "diff") if r]
        if with_counts:
            res = self._counts_rows(db, rows) + res
        return res

    def _second_rows(self, db, scope):
        """The second reading of a differing table's rows, or None - read
        value by value, floats and fixed-width text left out, and only the
        columns both sides call by the same name. A table whose rows the
        leaves found was read twice on the way (`leaves.second_reading`),
        and is not read again."""
        import json
        got = (self.__dict__.get("_differing") or {}).get((db, scope))
        if not got:
            return None
        src_t, dst_t, src_cols, dst_cols, rows = got
        try:
            key = list(self.src_engine.neutral_key("src", db, src_t))
        except Exception:  # noqa: BLE001 - no key, nothing to line up by
            return None
        classes = dict(src_cols)
        both = [(n, c) for (n, c), (m, _) in zip(src_cols, dst_cols)
                if n == m]
        from .. import second_reader as sr
        fixed = [n for n, t in self.src_engine.neutral_columns("src", db,
                                                               src_t)
                 if sr.padded(t)]
        name = self._leaf(src_t)
        mine = [tuple("" if v is None else str(v) for v in json.loads(k))
                for kind in ("missing", "extra", "changed")
                for k in self._read_drill(db, name, kind)]
        return self._second_reading(db, src_t, dst_t, key,
                                    [classes.get(k) for k in key], mine,
                                    rows, both, fixed)

    # --- the confirm pass, where migkit's own tail carries the changes ------
    # A difference found while the tail is running may be a change it has
    # read and not applied yet. Before calling it one, wait until the tail
    # has read as far as the source's log is now, then look again: what
    # converged was still arriving.

    def _tail_path(self, db):
        from .. import tailctl
        return tailctl.token_path(self.hop, db)

    def src_lsn(self, db):
        from .. import tailctl
        if not tailctl.alive(self.hop.report_dir(db), self.hop):
            return None
        try:
            return self.src_engine.log_position("src", db)
        except Exception:
            return None

    def fence_wait(self, db, lsn, timeout=300):
        """Wait until the tail's saved position has reached `lsn`: True,
        False on timeout, None where there is nothing to wait on. Looked
        at every 50 ms (`tailctl.POLL`): at once a second every fence -
        cutover, a confirm pass, the kept proof - waited half a second
        more on average (audit SP10)."""
        from .. import tailctl
        if lsn is None:
            return None
        began = time.monotonic()
        while time.monotonic() - began < timeout:
            try:
                have = self._saved_token(self._tail_path(db))
            except SystemExit:
                return None
            if have is not None:
                reached = self.src_engine.position_reached(have, lsn)
                if reached is None:
                    return None
                if reached:
                    return True
            time.sleep(tailctl.POLL)
        return False

    def _compare_pks(self, db, table, keys):
        """Walk the table again, both sides, and say what still differs."""
        pairs, _, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db), self._renamer(db))
        match = [(s_, d_) for s_, d_ in pairs if self._leaf(s_) == table]
        if not match:
            return None
        src_t, dst_t = match[0]
        src_cols, dst_cols, _ = self._column_plan(src_t, dst_t, db)
        self._drill(db, src_t, dst_t, src_cols, dst_cols)
        return tuple(self._read_drill(db, table, kind)
                     for kind in ("missing", "extra", "changed"))

    def delta_verify(self, db, limit=20000, log=None):
        """Verify only the rows the source has changed since the last clean
        pass, on any pair whose source keeps a change log that can be read
        without being moved (plan 18): the keys the log names are asked of
        both sides and compared in the one rendering both render to.

        The position moves only on a clean pass: a row found different is
        asked again next time, and a lost position is said, not stepped
        over."""
        import json

        from .. import verdict
        from .base import Result
        src = self.src_engine
        if not getattr(src, "CHANGE_POINT_READS_ONLY", False):
            return [Result(
                "delta", db, "error",
                f"{self.src_name}'s change log is read through a slot, and"
                " the slot is the tail's: reading it here would move it."
                " The tail's own confirmation covers what changed - run"
                " check while it runs")]
        from .. import durable
        state = self.hop.report_dir(db) / "pair-delta-token.json"
        token = durable.read_state(state, "position")
        if token is None:
            durable.write_json(state, src.copy_point("src", db))
            return [Result("delta", db, "ok", "baseline recorded, changes"
                           " are tracked from this point on")]
        changes, end = src.neutral_changes("src", db, token, limit)
        touched = {}
        for ch in changes:
            if not src.in_scope(db, ch["table"]):
                # a table the hop leaves out: nothing else compares it, and
                # it is not on the target to be found missing
                continue
            key = ch["key"]
            touched.setdefault(ch["table"], {})[
                tuple(sorted(key.items()))] = key
        pairs, _, _, _ = self.match_tables(
            src.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db), self._renamer(db))
        by_leaf = {self._leaf(s_): (s_, d_) for s_, d_ in pairs}
        res, clean = [], True
        for table, keyed in sorted(touched.items()):
            if self._leaf(table) not in by_leaf:
                clean = False
                res.append(Result("delta", f"{db}.{table}", "diff",
                                  f"{len(keyed)} changed rows, and the table"
                                  " is not on the target"))
                continue
            src_t, dst_t = by_leaf[self._leaf(table)]
            how = {}
            sc, dc, _ = self._comparable_columns(db, src, src_t,
                                                 self.dst_engine, dst_t, how)
            compared = verdict.compared_clause(how)
            key = sorted(next(iter(keyed.values())))
            if not sc or any(k not in dict(sc) for k in key):
                clean = False
                res.append(Result("delta", f"{db}.{table}", "skip",
                                  "the key is not among the columns these"
                                  " two engines can compare"))
                continue
            raw = [tuple(k[c] for c in key) for k in keyed.values()]
            # both sides through the hop's row filter, as the check reads
            # them: a changed row the filter leaves behind is on neither
            src_where, dst_where = self._row_scope(db, src_t)
            a = src.neutral_rows_by_key("src", db, src_t, sc, key, raw,
                                        where=src_where)
            b = self.dst_engine.neutral_rows_by_key("dst", db, dst_t, dc,
                                                    key, raw,
                                                    where=dst_where)
            missing, extra, changed = self._rows_differ(sc, dc, a, b)
            if missing or extra or changed:
                self._write_drill(db, self._leaf(table),
                                  missing=[json.dumps(list(k)) for k in
                                           missing],
                                  extra=[json.dumps(list(k)) for k in extra],
                                  changed=[json.dumps(list(k)) for k in
                                           changed])
                got, held_back = self._delta_held(Result(
                    "delta", f"{db}.{table}", "diff",
                    f"of {len(keyed)} changed rows: missing={len(missing)}"
                    f" extra={len(extra)} changed={len(changed)}{compared}",
                    str(self.hop.report_dir(db)),
                    f"migkit sync {self.hop.name} --db {db} --kind rows",
                    table=str(src_t),
                    rows_apart={str(src_t): {
                        "missing": len(missing), "extra": len(extra),
                        "changed": len(changed), "exact": True}}))
                clean = clean and not held_back
                res.append(got)
            else:
                res.append(Result("delta", f"{db}.{table}", "ok",
                                  f"{len(keyed)} changed rows verified equal"
                                  f" on both sides{compared}"))
            if log:
                log(f"{table}: {len(keyed)} changed, "
                    + self._delta_said(res[-1]))
        if clean:
            durable.write_json(state, end)
        res.insert(0, Result(
            "delta", db, "ok" if clean else "diff",
            f"{sum(len(k) for k in touched.values())} changed rows across"
            f" {len(touched)} tables since the last clean pass, position"
            f" {'advanced' if clean else 'NOT advanced'}"))
        return res

    def _write_pk_files(self, db, table, missing, extra, changed):
        self._write_drill(db, table, missing=missing, extra=extra,
                          changed=changed)

    def _confirm_in_flight(self, db, rows, stream=None):
        bad = [scope.split(".", 1)[1] for scope, status, *_ in rows
               if status == "diff" and "." in scope]
        if not bad or self.src_lsn(db) is None:
            return rows
        _, healed, how = self._resolve_inflight(db, bad, stream)
        proof = dict(h.split(": ", 1) for h in how)
        out = []
        for scope, status, detail, a, b in rows:
            leaf = scope.split(".", 1)[-1]
            if leaf in healed:
                status, detail = "ok", (f"the difference was still arriving"
                                        f" through migkit's change tail"
                                        f" ({proof.get(leaf, 'confirmed')})"
                                        + self.__dict__.get(
                                            "_compared", {}).get(scope, ""))
            out.append((scope, status, detail, a, b))
        return out

    def convert_ddl(self, db):
        """The statements that would build the target, for every source table.

        The same ones `move` runs when a table is missing, printed instead of
        executed - `neutral_create_sql` is where both come from, so what an
        operator reviews here is what will run.

        What this replaced was a sqlglot transpile followed by ten regular
        expressions rewriting `DATETIME` to `TIMESTAMP` and `TINYINT(1)` to
        `BOOLEAN` by hand. It worked for MySQL to PostgreSQL and could not
        work for anything else, and a regex over generated SQL has no way to
        tell a type name from the same letters inside a default or a comment.
        """
        if not self._can_compare_neutrally():
            raise SystemExit(
                f"{self.src_name}->{self.dst_name}: one of these engines has"
                " no type mapping in migkit, so there is no honest DDL to"
                " write for it")
        return [sql for _, sql in self.converted_objects(db)]

    #: what an encoding can hold, by the names engines give them: every
    #: Unicode character, only the Basic Multilingual Plane, one byte a
    #: character, or bytes nothing checks
    TEXT_REACH = {"utf8mb4": 3, "utf8": 3, "utf-8": 3, "utf-16": 3,
                  "utf-16le": 3, "utf-16be": 3,
                  # Oracle's names for the same
                  "al32utf8": 3, "al16utf16": 3,
                  "utf8mb3": 2, "ucs2": 2,
                  "sql_ascii": 0}
    REACH_WORDS = {3: "every Unicode character",
                   2: "only characters inside the Basic Multilingual Plane"
                      " - no emoji, no rarer scripts",
                   1: "one byte a character, a single alphabet",
                   0: "bytes as written, checked against no encoding"}

    def _zone_fingerprints(self, side, db):
        """Each side's own reading of what its zone names mean. The
        reading is the same on every engine that has one, so the base
        comparison holds two different engines to each other."""
        eng = self.src_engine if side == "src" else self.dst_engine
        return eng._zone_fingerprints(side, db)

    def check_params(self, db):
        """The settings two different engines can be held to at all:
        what their zone names mean, and what their text can hold. The
        rest of two servers' settings name different things."""
        zones = self._time_zone_rules(db)
        zones.check = "params"
        return [zones, self._text_reach(db)]

    def _text_reach(self, db):
        from .base import Result
        scope = f"{db} text encoding"
        try:
            src = self.src_engine.text_encodings("src", db)
            dst = self.dst_engine.text_encodings("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as an error
            return Result("params", scope, "error",
                          "could not read the encodings:"
                          f" {(str(e).strip().splitlines() or [''])[0][:90]}")
        if not src or not dst:
            return Result("params", scope, "skip",
                          f"{self.src_name if not src else self.dst_name}"
                          " does not say what its text is stored in")

        def reach(names):
            return min(self.TEXT_REACH.get(str(n).lower(), 1)
                       for n in names)
        need, room = reach(src), reach(dst)
        said = (f"the source stores text in {', '.join(sorted(src))} and"
                f" the target in {', '.join(sorted(dst))}")
        if room == 0 and need:
            return Result("params", scope, "warn",
                          f"{said}: the target keeps "
                          f"{self.REACH_WORDS[0]}, so text arrives as"
                          " whatever bytes the copy sent", "",
                          "create the target database in UTF-8")
        if room < need:
            return Result("params", scope, "diff",
                          f"{said}: the source can hold"
                          f" {self.REACH_WORDS[need]}, the target"
                          f" {self.REACH_WORDS[room]} - a value the target"
                          " cannot represent is refused, or stored as '?'",
                          "", "create the target's database or columns in"
                          " a full UTF-8 encoding (utf8mb4 on MySQL)")
        return Result("params", scope, "ok",
                      f"{said}: the target holds everything the source can")

    def snapshot_state(self, db, state_dir, kind="all"):
        """The target's own restore point: what is kept before a repair is
        the target's, and the target's engine knows what that is."""
        take = getattr(self.dst_engine, "snapshot_state", None)
        if take is None:
            raise SystemExit(f"{self.dst_name} keeps no restore point"
                             " migkit can take before a repair; use migkit"
                             " sync --apply, which repairs without one")
        return take(db, state_dir, kind)

    def converted_objects(self, db):
        """[(name on the target, statement)]: the tables, then the views and
        functions - what `convert_ddl` prints, with what each one makes."""
        out = []
        for src_t in self.src_engine.neutral_tables("src", db):
            leaf = self._leaf(src_t)
            columns, key = self._target_shape(db, src_t, leaf)
            out.append((leaf, self.dst_engine.neutral_create_sql(
                "dst", db, leaf, columns, key) + ";"))
        return out + [(n, sql) for n, sql, _ in self.converted_code(db)]

    def target_names(self, db):
        """What the target already has, by the names the conversion uses."""
        return ({self._leaf(t) for t in
                 self.dst_engine.neutral_tables("dst", db)}
                | {self._leaf(v) for v, _ in
                   self.dst_engine.neutral_views("dst", db)}
                | {n for n, *_ in
                   self.dst_engine.neutral_functions("dst", db)})

    def converted_code(self, db, propose=True, climb=True):
        """[(name, the target's statement or a comment, why not)] for the
        source's views, functions, procedures, triggers and events, in the
        target's SQL (backlog 39, R11): each routine by whichever candidate
        proves itself on it in the sandbox, or - where no sandbox is named
        - as the query translator carries it, the rest named, never a
        guess (`storedcode.convert`)."""
        from ..storedcode import convert
        self._db = db
        return convert.converted(self, db, propose=propose, climb=climb)

    #: the line a statement a model proposed is marked with
    PROPOSED = "-- proposed by a model, not by the translator: check holds" \
               " it to the source's answers"

    def prove_converted(self, db, schema=None):
        """Every view and routine of the source held to the target's by
        what it does (plan 20, R11): a view by its rows on the hop's own
        servers; every function, procedure, trigger and event by running
        both on the same fixture in the sandbox and comparing everything
        each did - never `ok` for one nobody ran (`storedcode.prove`).
        With `schema` (a list), the schema's own proofs on the sandbox's
        fixture are appended to it."""
        from ..storedcode import prove
        return prove.prove_database(self, db, schema)

    def setup_target_plan(self, db):
        """The steps for this pair, rather than for the pair it was written
        for.

        `migkit setup` used to refuse every combination except MySQL to
        PostgreSQL - honestly, with a sentence saying so, but a refusal all
        the same, on a command that only prints a list of steps. The steps
        are now read off what this pairing can actually do, which
        `_pair_capabilities` already answers without opening a connection.

        Nothing here runs: it is a dry run an operator reads and then carries
        out, which is why a step this pair cannot do has to be replaced by
        the reason rather than left in the list to fail later.
        """
        hop = self.hop.name
        if self.my and self.pg:
            # migkit's steps only: a program it wraps is its business, and
            # named here it was a second way to run the move that none of
            # migkit's checks or records would know about
            plan = []
            plan.append(f"migkit convert-schema {hop} --db {db}"
                        "   # the target's tables from the source's,"
                        " review then --apply")
            plan.append(f"migkit move {hop} --db {db} --go"
                        "   # resumable chunked data copy")
            plan.append(f"cross-engine CDC: migkit move {hop} --mode cdc --go"
                        "   # migkit stands up the streaming pipeline")
            return plan

        can = {row["item"]: row["level"] == "pass"
               for row in self._pair_capabilities()}
        plan = [f"# {self.src_name} -> {self.dst_name}, database {db}"]
        if not (can.get("this pair can compare")
                or can.get("this pair can move rows")):
            # nothing migkit does to rows applies here, and a list of steps
            # that cannot be taken is worse than saying so - except the
            # changes themselves, which a stream target takes as messages
            if can.get("this pair can tail changes"):
                return plan + [
                    f"-- {self.dst_name} holds a stream of changes, not a"
                    " copy of the tables: there is nothing to compare, and"
                    " the changes are what is carried",
                    f"migkit move {hop} --mode cdc --go"
                    "   # changes on the source delivered to the target",
                    f"migkit assess {hop}"
                    "   # what this pair can and cannot do, in full"]
            return plan + [
                f"-- migkit has no table-shaped path between"
                f" {self.src_name} and {self.dst_name}: they do not both"
                " speak in rows, so there is nothing for it to compare or"
                " carry",
                f"migkit assess {hop}"
                "   # what this pair can and cannot do, in full"]
        if self.dst_engine.CREATES_ON_WRITE:
            plan.append(f"-- {self.dst_name} creates the collection on the"
                        " first write, so there is nothing to make by hand")
        elif type(self.dst_engine).prepare_target is not Engine.prepare_target:
            plan.append(f"-- migkit makes the {self.dst_name} target itself"
                        " when the move starts")
        else:
            plan.append(f"create the target database on {self.dst_name}"
                        " yourself, with the encoding and collation you want"
                        " - migkit will not choose those for you")
        if can.get("this pair can create the target table"):
            plan.append(f"migkit convert-schema {hop} --db {db}"
                        "   # the DDL this pair would run, review then"
                        " --apply")
        else:
            plan.append(f"-- {self.dst_name} cannot be told what table to"
                        " build, so create the target objects yourself")
        if can.get("this pair can move rows"):
            plan.append(f"migkit move {hop} --db {db} --go"
                        "   # resumable chunked copy through the neutral"
                        " contract")
        else:
            plan.append(f"-- migkit cannot copy rows between {self.src_name}"
                        f" and {self.dst_name}: one of them has no row-shaped"
                        " read or write. Use a mover built for this pair")
        if can.get("this pair can tail changes"):
            plan.append(f"migkit move {hop} --mode cdc --go"
                        "   # changes on the source applied to the target")
        else:
            plan.append(f"-- no change stream for this pair, so plan a"
                        " cutover with writes stopped rather than a tail")
        plan.append(f"migkit check {hop} --db {db}"
                    "   # the same digest on both sides, table by table")
        return plan

    def moved_nothing(self, db):
        """Source tables with rows whose target table has none.

        The guard every same-engine path has: a bulk copy can finish without
        an error and leave the target empty. Asked of the one bulk path this
        engine has, MySQL into PostgreSQL, where the source's tables land in
        the target's `public` schema under the names the hop's mapping
        gives them. None when either side cannot be asked - not the same as
        an empty list, and the caller says so.
        """
        if not (self.my and self.pg):
            return self._moved_nothing_neutrally(db)
        try:
            # the target's own probe below reads an error as "no rows", so
            # whether it can be reached at all is asked first
            self.pg._psql("dst", db, "select 1")
            with_rows = [t for t in self.my._tables("src", db)
                         if self.my._q("src", f"select 1 from `{db}`.`{t}`"
                                              " limit 1")]
            landed = {t: "public." + self._leaf(self._rename(t, db))
                      for t in with_rows}
            present = self.pg.rows_present(db, list(landed.values()))
        except Exception:
            return None
        return sorted(t for t, name in landed.items() if name not in present)

    def _moved_nothing_neutrally(self, db):
        """The same guard for any pair, through each side's own reads: a
        source table with a row under the hop's filter, and none on the
        target under the name the hop gives it - or no such table at all,
        which on a target that makes tables on the first write is the
        same finding."""
        if not self._can_move_neutrally():
            return None
        try:
            there = ([] if self.dst_engine.target_missing(db)
                     else self.dst_engine.neutral_tables("dst", db))
            pairs, src_only, _, _ = self.match_tables(
                self.src_engine.neutral_tables("src", db), there,
                self._renamer(db))
            landed = dict(pairs)
            empty = []
            for src_t in list(landed) + list(src_only):
                src_where, dst_where = self._row_scope(db, src_t)
                if not self._a_row("src", db, src_t, src_where):
                    continue
                dst_t = landed.get(src_t)
                if not dst_t or not self._a_row("dst", db, dst_t, dst_where):
                    empty.append(src_t)
            return sorted(empty)
        except Exception:  # noqa: BLE001 - None: cannot be asked
            return None

    def _a_row(self, side, db, table, where):
        """Whether a side's table holds one row under `where`, read by
        its first column through the side's own reader."""
        from .. import canon
        eng = self.src_engine if side == "src" else self.dst_engine
        cols = eng.neutral_columns(side, db, table)[:1]
        if not cols:
            return False
        return bool(eng.neutral_read(
            side, db, table,
            [(cols[0][0], canon.comparable(eng.CANON_ENGINE, cols[0][1])[0])],
            limit=1, where=where)[0])

    def rows_held(self, db, tables):
        """The target's table under the name the hop gives each, holding a
        row within the hop's row filter: a row outside it is not the
        copy's to replace (`_neutral_move` empties only what the filter
        selects), and a table the target lacks holds none."""
        if not self._can_move_neutrally():
            return None
        try:
            if self.dst_engine.target_missing(db):
                return set()
            pairs, _, _, _ = self.match_tables(
                self.src_engine.neutral_tables("src", db),
                self.dst_engine.neutral_tables("dst", db), self._renamer(db))
            landed = dict(pairs)
            held = set()
            for sch, tbl in tables:
                src_t = f"{sch}.{tbl}" if sch else tbl
                if src_t not in landed:
                    # a table named without its schema, as `--table` takes
                    # it, is the one of that name
                    same = [k for k in landed if self._leaf(k) == tbl]
                    src_t = same[0] if len(same) == 1 else src_t
                dst_t = landed.get(src_t)
                if dst_t and self._a_row("dst", db, dst_t,
                                         self._row_scope(db, src_t)[1]):
                    held.add((sch, tbl))
            return held
        except Exception:  # noqa: BLE001 - None: cannot be asked
            return None

    def list_move_tables(self, db):
        if not (self.my and self.pg):
            if not self._can_move_neutrally():
                self._mysql_to_postgres_only("listing tables to move")
            there = ([] if self.dst_engine.target_missing(db)
                     else self.dst_engine.neutral_tables("dst", db))
            pairs, src_only, _, ambiguous = self.match_tables(
                self.src_engine.neutral_tables("src", db), there,
                self._renamer(db))
            if ambiguous:
                # one name in two schemas has one place to land on the
                # target; guessing which is how a copy overwrites the other
                raise SystemExit(
                    f"{db}: {', '.join(sorted(ambiguous)[:6])} appear under"
                    " the same name more than once, so migkit cannot tell"
                    " which target table each goes to. Rename them in the"
                    " hop's mapping, or exclude the ones not moving.")
            # a table the target does not have yet is moved too: the copier
            # creates it. Only the ones already there used to be listed, so
            # the rest were left out of the move without a word
            out = []
            for src_t in [s_ for s_, _ in pairs] + list(src_only):
                sch, _, tbl = src_t.rpartition(".")
                out.append((sch, tbl))
            return out
        return [("", t) for t in self.my._tables("src", db)]

    def capacity(self, side, db):
        return (self.src_engine if side == "src"
                else self.dst_engine).capacity(side, db)

    def probe_read(self, side, db, table, part=0, parts=1,
                   budget=16 * 2 ** 20, seconds=1.5, keep=0):
        """The shared copier reads through the source's neutral reader, so
        that is what is timed - not the source engine's own copier."""
        if side != "src":
            return None
        return Engine.probe_read(self.src_engine, side, db, table, part,
                                 parts, budget, seconds, keep)

    def io_wait(self, side, db):
        return (self.src_engine if side == "src"
                else self.dst_engine).io_wait(side, db)

    def changes_made(self, side, db):
        return (self.src_engine if side == "src"
                else self.dst_engine).changes_made(side, db)

    def row_sizes(self, side, db, tables=None):
        return (self.src_engine if side == "src"
                else self.dst_engine).row_sizes(side, db, tables)

    def link_probe(self, side, db, wait=5.0):
        return (self.src_engine if side == "src"
                else self.dst_engine).link_probe(side, db, wait)

    def load_sample(self, side, db):
        """The side's own engine's reading of its load, where it gives
        one (`machine.observe`)."""
        fn = getattr(self.src_engine if side == "src" else self.dst_engine,
                     "load_sample", None)
        return fn(side, db) if fn else None

    def move_table(self, db, sch, tbl, chunk, ck, log):
        """One table through the copier every pair shares.

        MySQL to PostgreSQL had a copier of its own, which wrote a CSV
        itself: bytes went in as their hex digits read as text, and every
        empty string as NULL - measured, 200,000 of 200,000 rows differed
        after a move that reported success. The shared copier writes through
        each engine's own writer, which takes every value by its type."""
        if not self._can_move_neutrally():
            self._mysql_to_postgres_only("moving a table")
        # before anything is read from the target: some targets are not
        # there until something makes them, and listing what a target
        # already holds is the first thing this does
        made = self.dst_engine.prepare_target(db)
        if made:
            log(f"{self.dst_name}: {made}")
        import contextlib
        with contextlib.ExitStack() as later:
            return self._neutral_move(db, sch, tbl, chunk, ck, log, later)

    def _can_tail(self, db=None):
        """Refuse, naming the pair, when changes cannot be carried - before
        a copy that a tail was meant to follow, not after it."""
        from .base import Engine
        if db is not None:
            # a filter the tail could not read through stops it here,
            # before it starts, rather than at the first change
            for t in self.src_engine.neutral_tables("src", db):
                self._row_scope(db, t)
        if type(self.src_engine).neutral_changes is Engine.neutral_changes:
            raise SystemExit(
                f"{self.src_name} has no change log migkit can read, so"
                " there is nothing to tail. Re-run `migkit move` for a fresh"
                " full load instead, and verify it with `migkit check`")
        # `neutral_apply` lives on the base class on purpose - the loop is
        # the same everywhere and only the two statements differ - so asking
        # whether *that* was overridden answers the wrong question
        if type(self.dst_engine)._apply_upsert is Engine._apply_upsert:
            raise SystemExit(
                f"{self.dst_name} cannot apply changes yet - it has no"
                " statement for writing one row by its key")
        from .. import twoway
        if twoway.settings(self.hop) is not None:
            twoway.policy(self.hop)
            twoway.refuse_unless_able(self.src_engine, self.dst_engine)

    @staticmethod
    def _saved_batch(token_path, token):
        """The number of the last batch a tail saved the position of,
        where its hop numbers them (`twoway.exact`). A position saved
        without one is given a number no earlier run's marks can hold,
        written before anything is applied: a mark left on the target by
        a run whose position was since thrown away must not pass for the
        batch after this one. That first number goes into every batch's
        mark as the run's (`run`), so a mark another run left is never
        taken for this one's however far ahead it stands
        (`twoway.committed_ahead`)."""
        import secrets

        from .. import tailctl
        env = tailctl.position(token_path) or {"token": token}
        try:
            if env.get("batch") is not None:
                return int(env["batch"])
        except (ValueError, TypeError):
            pass
        base = secrets.randbits(48)
        tailctl.keep(token_path, {**env, "batch": base, "run": base})
        return base

    def two_way_teardown(self, db, token_path, go):
        """What a two-way tail's marks left on the target, taken away: the
        table where the table was the rung, the replication origins where
        those were. [statements, run where `go`]"""
        from .. import marks
        rung = marks.kept_rung(token_path)
        if rung is None:
            return []
        fn = getattr(self.dst_engine, "mark_teardown", None)
        return fn("dst", db, rung, go) if fn else []

    def _committed_ahead(self, db, token, batch, token_path, log):
        """(position, batch) to go on from: the saved ones, or where the
        target committed batches after them before the position was saved
        - applied again, what they added to counters would be added twice.
        The one rule every engine's tail goes on by: the position saved
        after the target committed, or the target's mark of the last batch
        it committed.

        Where the batch the target committed is the one the tail's own
        state set down before sending it (`tailtables.Store`), that state
        is the batch's and stands with it; a jump past it leaves the state
        behind the position, which `Store.align` holds to."""
        from .. import marks, tailctl, twoway
        env = tailctl.position(token_path) or {"token": token}
        rung = marks.kept_rung(token_path)
        got = twoway.committed_ahead(self.dst_engine, db, batch, rung,
                                     env.get("next"), run=env.get("run"))
        if got is None:
            return token, batch
        token, ahead = got
        store = tailtables.Store.of(token_path.parent)
        gen = int(env.get("gen") or 0)
        mine = store.gen() if store.get("stream") == (
            env.get("id") or "v0") else None
        if mine == gen + 1 and (store.get("pending") or {}).get(
                "batch") == ahead:
            moved = 1
        else:
            if mine == gen + 1:
                # set down for another batch than the target committed
                store.undo()
            moved = max(1, ahead - batch)
        tailctl.keep(token_path, {
            **{k: v for k, v in env.items() if k != "next"},
            "token": token, "batch": ahead, "gen": gen + moved})
        log(f"the target had committed batch {ahead} before its position"
            " was saved here"
            + (f" ({ahead - batch} batches past it)" if ahead - batch > 1
               else "")
            + ": going on after it, not applying it again")
        return token, ahead

    @staticmethod
    def _saved_token(token_path):
        """The position a tail saved, None when there is none."""
        from .. import tailctl
        return tailctl.saved(token_path)

    def _tail_targets(self, db):
        """{source table: the target table its changes go to}, by the
        source table's whole name, and by its last part where no other
        table of the source has that.

        The same pairing the move makes, through the hop's mapping. The tail
        used to write each change to the source's own table name, so a
        table the hop renames was copied under its new name and then kept
        up to date under its old one. It was keyed by the last part alone:
        `sales.orders` and `crm.orders` both went to one target table. Two
        tables of one name go each to the target's table of its own whole
        name; where the target has none, to no table (None), and a change
        of one stops the tail before anything of its batch is applied
        (`_tail_target`).
        """
        from collections import Counter
        src = self.src_engine.neutral_tables("src", db)
        there = ([] if self.dst_engine.target_missing(db)
                 else self.dst_engine.neutral_tables("dst", db))
        pairs, src_only, _, _ = self.match_tables(src, there,
                                                  self._renamer(db))
        out = {str(s_): d for s_, d in pairs}
        for s_ in src_only:
            out[str(s_)] = self._leaf(self._rename(s_, db))
        named = Counter(self._leaf(s_) for s_ in src)
        exact = {str(d) for d in there}
        for s_ in src:
            leaf = self._leaf(s_)
            if str(s_) not in out:
                # one of two of its name the pairing would not choose
                # between (`match_tables`)
                want = str(self._rename(s_, db))
                out[str(s_)] = want if want in exact else None
            out[leaf] = None if named[leaf] > 1 else out[str(s_)]
        return out

    def _in_scope(self, db, changes):
        """The changes, with every insert or update of a row the hop's row
        filter does not select turned into a delete of it.

        A change record carries values, and the filter is SQL the source
        evaluates, so the source is asked which of the changed rows it
        selects now - one read per table per batch. Asking the row's state
        now rather than at the change converges: a row that moved into the
        filter and out again in one batch is gone from the target, as it
        is from the filter. A row the source no longer has is gone too, and
        its own delete follows. Each delete made here says so
        (`canon.FILTERED`): the source deleted nothing, and a two-way
        tail holds it to no conflict (`twoway.resolve`)."""
        from .. import canon
        by_table = {}
        for c in changes:
            if c.get("op") in ("insert", "update"):
                by_table.setdefault(c["table"], []).append(c)
        if not by_table:
            return changes
        out_of_scope = set()
        for table, rows in by_table.items():
            src_where, _ = self._row_scope(db, table)
            if not src_where:
                continue
            key = sorted(rows[0]["key"])
            keys = []
            for c in rows:
                values = c.get("values") or {}
                keys.append(tuple(values.get(k, c["key"][k]) for k in key))
            cols = self._key_classes(db, table, key)
            seen = self.src_engine.neutral_rows_by_key(
                "src", db, table, cols, key, keys, where=src_where)
            for c, k in zip(rows, keys):
                if self._key_of(cols, key, list(k)) not in seen:
                    out_of_scope.add(id(c))
        out = []
        for c in changes:
            if id(c) not in out_of_scope:
                out.append(c)
                continue
            now = {k: (c.get("values") or {}).get(k, v)
                   for k, v in c["key"].items()}
            out.append({"op": "delete", "table": c["table"], "key": now,
                        canon.FILTERED: True})
            if now != c["key"]:
                # an update that moved the key as well: the row's old
                # address goes too, as the apply would have removed it
                out.append({"op": "delete", "table": c["table"],
                            "key": dict(c["key"]), canon.FILTERED: True})
        return out

    def _key_classes(self, db, table, key):
        """[(column, class)] of a table's key on the source, asked once a
        table until its shape changes (`_shape_now`) - it was asked for
        every batch the row filter was held to (audit SP12)."""
        kept = self.__dict__.setdefault("_scopes", {})
        at = ("classes", db, str(table), tuple(key))
        if at in kept:
            return kept[at]
        try:
            from .. import canon
            classes = dict(self.src_engine.neutral_columns("src", db, table))
            cols = [(k, canon.type_class(self.src_engine.CANON_ENGINE,
                                         classes.get(k))) for k in key]
        except Exception:  # noqa: BLE001 - by the values; asked again
            return [(k, None) for k in key]
        kept[at] = cols
        return cols
    def _carried(self, db, changes):
        """The changes of an operation the hop's `mapping.ops` carries for
        its table (`Hop.carries`) - decided on the source's own operation,
        before the row filter turns an insert or an update into a delete.
        Every change where the hop says nothing of it."""
        if not (self.hop.mapping or {}).get("ops"):
            return changes
        known = {}
        out = []
        for c in changes:
            t = c["table"]
            if t not in known:
                known[t] = self.hop.carries(db, *self.hop.parts_of(t))
            if c.get("op") in known[t]:
                out.append(c)
        return out

    def _filtered_kept(self, db, changes):
        """The changes without the deletes the row filter made
        (`canon.FILTERED`) for a table whose deletes the hop does not
        carry: a row that left the filter is kept on the target as it
        was, as a row the source deleted is."""
        if not (self.hop.mapping or {}).get("ops"):
            return changes
        from .. import canon
        return [c for c in changes if not (
            c.get(canon.FILTERED) and "delete" not in self.hop.carries(
                db, *self.hop.parts_of(c["table"])))]

    def _typed_change(self, db, change, target):
        """A change with each value of a class that has a form of its own
        on the target (`canon.write_value`) in that form, as the copy
        writes it: into MongoDB by the BSON type the source column is held
        as - a `bigint` a 64-bit integer, where the driver's default was a
        32-bit 5 - out of it an ObjectId as its Extended JSON; an interval,
        a vector or a shape as the target takes one. The other values are
        handed on as they are, as they always were."""
        from .. import canon
        known = self.__dict__.setdefault("_typed_of", {})
        at = (db, change["table"])
        if at not in known:
            try:
                src_cols, dst_cols, _ = self._move_columns(
                    change["table"], target, db)
            except Exception:  # noqa: BLE001 - the change as it is
                src_cols, dst_cols = [], []
            known[at] = {s: d for (s, _), (_, d) in zip(src_cols, dst_cols)
                         if canon.has_own_form(d)}
        classes = known[at]
        if not classes:
            return change
        engine = self.dst_engine.CANON_ENGINE
        out = dict(change)
        for part in ("key", "values"):
            if change.get(part):
                out[part] = {n: canon.write_value(engine, classes[n], v)
                             if n in classes else v
                             for n, v in change[part].items()}
        return out

    def _mapped_change(self, db, change):
        """A change with `mapping.columns` applied as the copy applied it:
        dropped columns left out, renamed ones under their target names.

        Measured before: with `name` renamed to `label`, the first insert
        the tail applied stopped it on `Unknown column 'name'`, and a
        target that had both would have been written in the wrong one."""
        if not self._column_rules(db, change["table"]):
            return change
        out = dict(change)
        for part in ("key", "values", "before"):
            if change.get(part):
                out[part] = self._mapped_types(db, change["table"],
                                               change[part])[0]
        if len(out.get("key") or {}) != len(change.get("key") or {}):
            raise SystemExit(
                f"{change['table']}: mapping.columns leaves out part of its"
                " key, so a change cannot be placed on the target by it."
                " Keep the key's columns.")
        return out

    def _to_target(self, db, changes, targets, accounts=None):
        """(the source's changes as the target takes them, how many values
        landed as NULL): out of the hop's row filter turned into deletes
        (`_in_scope`), each masked column through the hop's transform
        (`_masked_change`), under the target's table and column names, each
        value in the form the target keeps (`_typed_change`,
        `_mapped_change`) - the one way the tail and a reader beside it
        (`carry`) put a change on the target. Only the operations the
        hop's `mapping.ops` carries (`_carried`); where it keeps deletes,
        a row that leaves the filter is kept too (`_filtered_kept`).
        `accounts` hears of the deletes a key moved out of the filter
        adds, of what the hop does not carry, and of each table's target
        name (`_Accounts`)."""
        if not changes:
            return [], 0
        carried = self._carried(db, changes)
        if accounts is not None:
            accounts.dropped(tailtables.NOT_CARRIED, changes, carried)
        kept = carried
        changes = self._in_scope(db, carried)
        if accounts is not None:
            accounts.grown(kept, changes)
        left = self._filtered_kept(db, changes)
        if accounts is not None:
            accounts.dropped(tailtables.NOT_CARRIED, changes, left)
        changes = left
        mapped = []
        for c in changes:
            to = self._tail_target(targets, c["table"], db)
            if accounts is not None:
                accounts.named(c["table"], to)
            mapped.append(dict(self._mapped_change(
                db, self._typed_change(db, self._masked_change(db, c), to)),
                table=to))
        return self._flatten_changes(mapped)

    def carry(self, db, changes):
        """Changes of the source applied to the target as the tail applies
        them, for a reader beside it (`tailtables`, a table's changes since
        it left, read again)."""
        from .. import canon
        bare = [{k: v for k, v in c.items() if k not in canon.FROM_THE_LOG}
                for c in changes if not canon.is_ddl(c)]
        out, _ = self._to_target(db, bare, self._tail_targets(db))
        if out:
            self.dst_engine.neutral_apply("dst", db, out)

    @staticmethod
    def _commit_cut(changes):
        """Where a batch ends at the last source commit it holds: the
        length of the batch through the last change a transaction ended
        at that says where a read goes on after it (`txn_at`), or None
        where it ends at one already, or holds none (one transaction
        larger than a read, carried in parts as before). The rest of the
        batch is read with the next: a transaction that fits a batch is
        never split across two - which a target shows as half of it for
        as long as the next batch takes (F7; GoldenGate's GROUPTRANSOPS
        ends a group at a commit the same way)."""
        if not changes or changes[-1].get("txn_end"):
            return None
        for i in range(len(changes) - 1, -1, -1):
            if changes[i].get("txn_end") and \
                    changes[i].get("txn_at") is not None:
                return i + 1
        return None

    def _tail_target(self, targets, table, db=None):
        # by its whole name, then by its last part; a table made on the
        # source after the tail began goes by its mapped name, as the move
        # would have sent it
        for k in (str(table), self._leaf(table)):
            if k not in targets:
                continue
            if targets[k] is None:
                raise SystemExit(
                    f"{table}: another table of the source has its name, and"
                    " the target has no table of its whole name, so its"
                    " changes would land in a table of the other's. Nothing"
                    " of the batch was applied: make the target's table of"
                    " that whole name, or rename one of them with"
                    " mapping.tables, and run the tail again")
            return targets[k]
        return self._leaf(self._rename(table, db))

    def _shape_gate(self, db, token_path, position, skip=(), stop=True):
        """The source's column shape the tail may apply under, or None
        where it cannot be read cheaply.

        A change log carries rows, not a DDL a reader can rely on, so the
        catalogue is asked directly: once as the tail starts and again
        before each batch, against the shape saved beside the position -
        which is how a DDL made while the tail was stopped is seen as well.
        A changed shape is accepted once the target has every column the
        source now has on the tables it touched; until then the tail stops,
        and says what changed. Only where the catalogue is one query: a
        document store's shape is a scan of every document, and its stream
        names its own drops and renames.

        Where the hop's `ddl` does not stop the tail (`stop` False), the
        tables the target lags on are handed back instead (`_shape_now`),
        and the tables of `skip` - being copied, parked or out of the tail
        (`tailtables`) - are not held to the target at all. `stop` may be a
        callable of a source table, where the hop's `ddl` says it per
        table: only the tables it is true of stop the tail.
        """
        return self._shape_now(db, token_path, position, skip, stop)[0]

    def _shape_now(self, db, token_path, position, skip=(), stop=True):
        """(the source's shape or None, {source table: (target table,
        [its columns the target does not have])}) - the tables `stop`
        stops on raising instead.

        Where the source's engine computes a digest of its catalogue
        itself (`shape_mark`), that row is asked first and the whole shape
        read only where it moved; the shape read is held to the last one by
        its fingerprint before it is compared, and saved only where it
        moved - the whole catalogue was read, compared and written back
        every batch, 23.5 ms at 2,000 tables of 20 columns before the query
        itself (audit SP8). Saved in the tail's own store
        (`tailtables.Store`), where a torn file cannot pass for "nothing
        saved" and skip the gate."""
        import hashlib
        import json as _json

        from .. import drift
        src = drift.reader(self)
        if src is None:
            return None, {}
        last = self.__dict__.setdefault("_shapes_seen", {})
        kept = last.get(db)
        mark = None
        if callable(getattr(src, "shape_mark", None)):
            try:
                mark = src.shape_mark("src", db)
            except Exception:  # noqa: BLE001 - the whole shape is read
                mark = None
        if mark is not None and kept is not None and kept[0] == mark:
            # asked before the shape it stands for was read: a change in
            # between is one more whole read, never one missed
            return kept[2], {}
        now = drift.shape(self.src_engine, "src", db)
        if now is None:
            return None, {}
        fp = hashlib.sha256(_json.dumps(now, sort_keys=True,
                                        default=str).encode()).hexdigest()
        if kept is not None and kept[1] == fp:
            last[db] = (mark, fp, now)
            return now, {}
        store = tailtables.Store.of(token_path.parent)
        saved = store.get("shape")
        if saved is not None:
            saved = {t: [tuple(c) for c in cols] for t, cols in
                     saved.items()}
        moved = drift.changes(saved, now) if saved is not None else []
        behind = {}
        if moved:
            # what was read of the shape before - the row filter's
            # translation, a key's classes - read again
            self.forget_shapes()
            skip = {self._leaf(t) for t in skip}
            behind = {t: got for t, got in
                      self._lacking(db, saved, now).items()
                      if self._leaf(t) not in skip}
            stopping = {t: got for t, got in behind.items()
                        if (stop(t) if callable(stop) else stop)}
            if stopping:
                named = [f"{d}.{n}" for d, cols in stopping.values()
                         for n in cols]
                raise SystemExit(
                    f"{db}: the source's schema changed under the tail - "
                    + "; ".join(moved[:6])
                    + (" ..." if len(moved) > 6 else "")
                    + f". The target does not have {', '.join(named[:6])}"
                    f" yet, so nothing after {str(position)[:60]} was"
                    " applied. Bring the target's schema level with the"
                    " source's, then run the tail again; it resumes from"
                    " there.")
        if moved or saved is None:
            with store.batch() as st:
                st.put("meta", "shape", now)
        last[db] = (mark, fp, now)
        return now, behind

    def _target_lacks(self, db, before, after):
        """`table.column` the source has now, on tables whose shape changed,
        that the target does not."""
        return [f"{d}.{n}" for d, cols in
                self._lacking(db, before, after).values() for n in cols]

    def _lacking(self, db, before, after):
        """{source table: (target table, [columns the hop carries there now
        that the target does not have, by their target names])}, of the
        tables whose shape changed. Through the hop's `mapping.columns`
        (`_mapped_types`): a column it keeps out or drops is not the
        target's to have, and named as missing it stopped the tail, or
        parked its table for good."""
        out = {}
        if self.dst_engine.CREATES_ON_WRITE:
            # a column arrives with the first row that has it
            return out
        targets = self._tail_targets(db)
        for t, cols in sorted(after.items()):
            if dict(cols) == dict(before.get(t, [])):
                continue
            dst_t = self._tail_target(targets, t, db)
            try:
                have = {str(n).lower() for n, _ in
                        self.dst_engine.neutral_columns("dst", db, dst_t)}
            except Exception:
                have = set()
            carried, _ = self._mapped_types(db, t, dict(cols))
            missing = [n for n in carried if str(n).lower() not in have]
            if missing:
                out[t] = (dst_t, missing)
        return out

    def _level(self, db, name):
        """Whether the target has every column the source's `name` has
        now: a parked table's target come level (`tailtables`)."""
        cols = sorted((str(n), str(t)) for n, t in
                      self.src_engine.neutral_columns("src", db, name))
        return not self._lacking(db, {}, {name: cols})

    def _additive(self, db, src_table, dst_t, missing):
        """(the statements that add `missing` to the target's `dst_t` as
        the source has them, "") - or (None, why) where one of them is not
        a change the target can take at its place in the log without a row
        coming out different. A column that takes NULL, or has a default
        that is the same value whenever it is taken, holds that value on
        both sides in the rows made before it; a NOT NULL column with no
        default, an identity, a key column or a default worked out when it
        is taken (`now()`) parks the table instead. Each statement is the
        target's own `create table` rendering of the column
        (`neutral_create_sql`), so the column is built as a copy builds
        it."""
        try:
            columns, key = self._target_shape(db, src_table,
                                              self._leaf(dst_t))
        except (Exception, SystemExit) as e:  # noqa: BLE001 - parked
            return None, (str(e).strip().splitlines() or ["?"])[0][:200]
        by = {c[0]: c for c in columns}
        out = []
        for name in missing:
            col = by.get(name)
            if col is None:
                return None, f"{name} is not a column the hop carries"
            rule = col[3] if len(col) > 3 else {}
            if name in key or rule.get("identity"):
                return None, f"{name} is part of the key or numbered"
            if rule.get("null") is False and rule.get("default") is None:
                return None, (f"{name} takes no NULL and has no default:"
                              " the rows before it hold whatever each"
                              " engine makes up")
            if not tailtables.constant_default(rule.get("default")):
                return None, (f"{name}'s default ({rule.get('default')}) is"
                              " worked out when it is taken, so the rows"
                              " before it differ on the two sides")
            made = self.dst_engine.neutral_create_sql("dst", db, dst_t,
                                                      [col], ())
            got = re.match(r"^create table (.+?) \((.*)\)$", str(made),
                           re.S | re.I)
            if not got:
                return None, (f"{self.dst_name} builds tables in a way"
                              " migkit does not turn into an added column")
            out.append(f"alter table {got.group(1)} add {got.group(2)}")
        return out, ""

    def _run_ddl(self, db, sql):
        """One statement on the target that changes its schema, and what
        the engines keep of the shape it changed forgotten."""
        try:
            self.dst_engine.execute_ddl("dst", db, sql)
        except NotImplementedError:
            self.dst_engine.neutral_create_code("dst", db, sql)
        self.dst_engine.forget_shapes()
        self.__dict__.pop("_typed_of", None)

    def target_mark(self, db):
        return self.dst_engine.target_mark(db)

    def free_bytes(self, side, db):
        eng = self.dst_engine if side == "dst" else self.src_engine
        return eng.free_bytes(side, db)

    def log_kept(self, db, grows):
        return self.dst_engine.log_kept(db, grows)

    def tail_seed(self, db, token_path, point):
        """Start the tail from `point`, a position taken earlier."""
        self._begin(db, token_path, point)

    def _begin(self, db, token_path, point):
        """A position taken afresh (`tailctl.begin`): the tail's own state
        of an earlier one cleared first (`tailtables.Store`), then the
        position with the source it belongs to, in one write."""
        from .. import tailctl
        where = token_path.parent
        # the shape last seen belonged to the stream before
        self.__dict__.pop("_shapes_seen", None)
        return tailctl.begin(
            self.src_engine, db, token_path, point,
            fresh=lambda env: tailtables.Store.of(where).reset(env["id"]))

    def tail_start(self, db, token_path):
        """Fix where the tail will begin, before the copy it follows.

        A position already saved is kept: it is older than now, and starting
        earlier only replays changes the appliers are idempotent for, where
        starting later skips them.
        """
        self._can_tail(db)
        if token_path.exists():
            return False
        point = self.src_engine.copy_point("src", db)
        self._begin(db, token_path, point)
        self._shape_gate(db, token_path, point)
        return True

    #: changes a read of the tail asks for when it is caught up, and the
    #: most it grows to while it is behind
    TAIL_BATCH, TAIL_BATCH_MOST = 1000, 16000

    def _change_reader(self, db, token_path, log):
        """What reads the tail's next batch while one is applied, where the
        source's position only says where to read from (`READS_AHEAD`);
        None where the tail reads, then applies."""
        if not getattr(self.src_engine, "READS_AHEAD", False):
            return None
        return _Reader(self.src_engine, db, token_path.parent, log,
                       self.TAIL_BATCH_MOST)

    def tail_apply(self, db, go, token_path, log):
        """Carry changes from one engine's log into the other, until stopped.

        The hand-written version of this read a MySQL binlog and wrote
        PostgreSQL INSERT statements, and could only ever do that one pair.
        This asks the source for `canon.change` records and hands them to the
        target's applier, so it works for every pair where the source has a
        change log at all - and refuses, naming the pair, where it does not.

        **Set down, applied, saved - in that order.** A batch's part of the
        tail's own state (its tables' states, what it sets aside and
        releases, its accounts) is written in one transaction before the
        target commits the batch (`tailtables.Store`); the position after
        it once the target has (`tailctl.moved`). A stop anywhere between
        leaves the position before the batch: the batch is read again and
        applied again, which the appliers are idempotent for - or, where
        batches are numbered on the target, the target's mark of the last
        batch it committed says it was, and the tail goes on after it
        (`_committed_ahead`) with the state it set down for it. Saving the
        position first would skip them instead, and a skipped change is a
        row that silently never arrives.

        **A batch ends at a source commit** (F7). Where a read can stop
        inside a transaction (`READ_ENDS_INSIDE_TRANSACTIONS`), the batch
        is cut after the last change that ended one, the position saved is
        that commit's (`txn_at`), and the rest goes into the next batch
        with what is read after it: a transaction that fits a batch is
        never half on the target. One larger than a read is carried in
        parts, as before.

        **Its tables move on their own** (`tailtables`): a table added to
        the hop's scope is copied beside the stream under its own marks,
        one taken out leaves it, and a DDL does what the hop's `ddl` says
        - stops the tail (the default, as before), parks only its table,
        or is carried at its place in the log where it can be - while
        every other table goes on.

        **It ends** when asked - ctrl-c, a service manager's stop,
        `tail.stop` (`tailctl`) - or, where the hop's `follow_until` is
        `caught_up` or a schedule started it, once it has reached where
        the source's log was when it first read to its end, and at the
        latest when the schedule's slot ends: its position saved, the way
        it would go on from next time. The loop is every engine's
        (`tailctl.run_loop`).
        """
        import json as _json
        import time as _time
        from types import SimpleNamespace
        self._can_tail()
        # read before anything connects: a file that cannot be read is the
        # answer whatever the servers would have said
        token = self._saved_token(token_path)
        from .. import tailctl as _tailctl
        if go and not token_path.exists():
            # saved before the first change is read: a tail stopped before
            # anything arrived would otherwise restart from a later "now"
            # and skip whatever came in between
            self.tail_start(db, token_path)
            token = self._saved_token(token_path)
        else:
            self._can_tail(db)
            if token:
                _tailctl.same_source(self.src_engine, db, token_path, token)
        from .. import canon, drift, failpoint, marks, schedule, tailctl, \
            twoway
        where = token_path.parent
        # how long it runs: until asked, or until caught up (a slot's run
        # ends at the slot's end at the latest)
        until = tailctl.until(self.hop,
                              scheduled=schedule.slot_end() is not None)
        # the hop's `ddl`, read before anything connects, and per table
        # where it says it so: whether a table the target lags on stops
        # the tail (`_shape_now`)
        tailtables.policy(self.hop)
        tailtables.skipped(self.hop)

        def stops(table):
            return tailtables.policy(
                self.hop, [db, *str(table).split(".")]) == "stop"
        log(f"tailing {self.src_name} -> {self.dst_name}"
            + (", until caught up" if until == "caught_up" and go
               else ", ctrl-c to stop")
            + ("" if go else " (count-only, add --go to apply)")
            + (f", resuming from {str(token)[:40]}" if token else ""))
        two_way = twoway.settings(self.hop) is not None
        # batches numbered where one applied twice would be wrong: the
        # target's mark of the last one it committed says whether the
        # position saved here is behind it
        exact = go and two_way and twoway.exact(self.hop)
        # how what it applies is marked on the target, kept in the token
        rung = marks.settle(self, db, token_path, exact, log) \
            if go and two_way else None
        # and for a target that keeps a ledger of the batches it committed
        # (`EXACT_BATCHES`, the warehouses and the lakes), whatever the hop
        exact = exact or (go and getattr(self.dst_engine, "EXACT_BATCHES",
                                         False))
        # a one-way tail numbers its batches too, where the target keeps a
        # number (F7, R19 lever 11): after a stop it goes on after the last
        # batch committed rather than applying it again
        numbered = go and not two_way and not exact and marks.settle_exact(
            self, db, token_path, log) is not None
        if numbered:
            exact, rung = True, "table"
        batch = self._saved_batch(token_path, token) if exact else 0
        if exact:
            token, batch = self._committed_ahead(db, token, batch,
                                                 token_path, log)
        # what goes round the loop with the tail: where it has saved,
        # where its next read starts (past the saved position where the
        # batch before ended at a commit short of what was read, F7), what
        # it read and did not apply yet, how large a read is, and for
        # `/metrics` when a read last came back short of its limit - the
        # end of the source's log - how much longer the source keeps what
        # the tail has not read (asked once a minute), the source's commit
        # time of the last change applied and how long it took to the
        # target (`tailctl.beat`)
        s = SimpleNamespace(
            seen=0, saved=token, read_at=token, carried=[], got=[],
            limit=self.TAIL_BATCH, caught_up=_time.time(), room=None,
            room_at=0.0, room_said=False, fence=None, through=None,
            lag=None, left=0, batch=batch, env=tailctl.position(token_path),
            prepared=False, unresolved=False, pending_rows={},
            shaped_at=_time.monotonic())
        can_cut = bool(getattr(self.src_engine,
                               "READ_ENDS_INSIDE_TRANSACTIONS", False))
        # the next batch read while this one is applied, where the source's
        # position is only where to read from (`READS_AHEAD`)
        s.ahead = self._change_reader(db, token_path, log) if go else None
        s.targets = self._tail_targets(db)
        # the changes the table copier already read are left out, and how
        # many were (`_CopiedRanges`)
        copied = _CopiedRanges(self, db, where)
        # each table's state, beside the position (`tailtables`) - held to
        # it before anything is read: a batch set down that the position
        # never passed is taken back out
        tables = tailtables.TableSet(self, db, token_path, log, s.env) \
            if go else None
        # every change read, per table: applied, or left out and why
        accounts = _Accounts(tailtables.Store.of(where))
        # the proof kept up with every position saved, where the hop keeps
        # it (`kept.Keeper`)
        from .. import kept as _kept
        keeper = (_kept.Keeper(self, db, token_path, token, log)
                  if go and _kept.wanted(self.hop) else None)
        # where the source's log carries every DDL, its catalogue is read
        # again only for a batch that holds one - and once a minute, in
        # case one came some way the log did not show (audit SP8)
        ddl_logged = getattr(self.src_engine, "LOG_CARRIES_DDL",
                             self.src_engine.CANON_ENGINE
                             in self._DDL_LOGGED)

        def said():
            return (f"{s.seen} changes"
                    + ("" if go else " seen (nothing applied)")
                    + (f"; {s.left} more left out, already in what the copy"
                       " read" if s.left else ""))

        def not_live():
            return tables.not_live() if tables is not None else set()
        # the source's shape the tail last applied under, saved beside its
        # position, so a DDL made while it was stopped is seen too
        s.known, s.lacking = self._shape_now(db, token_path, s.saved,
                                             not_live(), stops)
        # the target's triggers held off for as long as changes are applied,
        # as the servers' own replication applies them: what the source's
        # triggers wrote is in the log with the rest, and the target's
        # firing again rewrote the rows and wrote them twice
        quieted = {t for t in s.targets.values() if t}
        window = (self.dst_engine.load_window(db, log, set(quieted))
                  if go else None)
        # and for a table that joins the tail after it began, from when it
        # joins (`TableSet.quiet`): the window above was decided over the
        # tables there were then
        import contextlib
        joined = contextlib.ExitStack()
        if tables is not None and window is not None:
            def quiet(name):
                to = self._tail_target(s.targets, name, db)
                if to in quieted:
                    return
                joined.enter_context(self.dst_engine.load_window(
                    db, log, {to}))
                quieted.add(to)
            tables.quiet = quiet
        # the collector's passes held to what the batches leave (`_batch_gc`)
        gc_back = _batch_gc() if go else None

        def beat(caught_up, seen):
            got = None
            if tables is not None:
                got = tables.counts()
                got["set_aside_bytes"] = sum(tables.spilled().values())
            return got

        def beat_with(caught_up, seen):
            tailctl.beat(where, s.caught_up, s.seen, s.room, s.through,
                         s.lag, lambda: beat(caught_up, seen))

        def resolve():
            """A batch set down and sent whose commit's answer was lost
            with the connection: the target's mark says whether it
            committed - gone on after, its state standing, or taken back
            out and read again."""
            tok, b = self._committed_ahead(db, s.saved, s.batch,
                                           token_path, log)
            s.unresolved = s.prepared = False
            if b == s.batch:
                if tables is not None:
                    tables.forget()
                accounts.forget()
                return
            s.env = tailctl.position(token_path)
            if tables is None or tables._prepared is None or \
                    tables.store.gen() != int(s.env.get("gen") or 0):
                raise SystemExit(
                    f"{db}: the target committed batch {b} of this tail,"
                    " and the tail's own state was not set down for it."
                    " Nothing more was applied; start the tail again, which"
                    " holds the two together before it reads")
            tables.committed()
            accounts.committed(s.pending_rows)
            s.saved = s.read_at = tok
            s.batch = b

        def lost(e, at):
            # nothing after the saved position was kept - but a batch set
            # down and sent, which the target may have committed: where
            # batches are numbered its mark says whether (`resolve`, before
            # the next read); else it is taken back out and read again
            s.read_at, s.carried = s.saved, []
            if exact:
                self.dst_engine.__dict__.pop("_batch_seen", None)
                s.unresolved = s.unresolved or s.prepared
            if not s.unresolved:
                if tables is not None:
                    tables.forget()
                accounts.forget()
                s.prepared = False
            return s.saved

        def reached(at):
            if s.fence is None:
                s.fence = self._fence(db)
            got = (self.src_engine.position_reached(s.saved, s.fence)
                   if s.fence is not None else None)
            # a log whose positions cannot be compared ends on an empty read
            return False if got is None and s.got else got

        def look(saved_token):
            if s.unresolved:
                resolve()
                saved_token = s.saved
            if go and _time.time() - s.room_at >= 60:
                s.room_at = _time.time()
                try:
                    s.room = self.src_engine.stream_room("src", db,
                                                         saved_token)
                except Exception as e:  # noqa: BLE001
                    # a reading for /metrics does not stop the tail
                    s.room = None
                    if not s.room_said:
                        log("could not read how long the source keeps"
                            f" its log for the tail: {type(e).__name__}")
                        s.room_said = True
            if tables is not None:
                again = tables.boundary(saved_token, s.known)
                if tables.take_remapped():
                    # the hop's mapping changed under it: each table's
                    # target, columns and filter read again
                    s.targets = self._tail_targets(db)
                    self.forget_shapes()
                    self.__dict__.pop("_typed_of", None)
                now = tables.taken()
                if again or (follows_listed and now - s.taking):
                    # read ahead under the scope before it, or a feed
                    # opened over the tables there were then
                    # (`FOLLOWS_LISTED_TABLES`): read again from the saved
                    # position, the source's reader let go and opened
                    # again over the tables now
                    if s.ahead is not None:
                        s.ahead.close()
                        s.ahead = self._change_reader(db, token_path, log)
                    else:
                        self.src_engine.release_changes()
                    s.read_at, s.carried = saved_token, []
                s.taking = now
                tables.check_level(lambda name: self._level(db, name))
            asked = _time.time()
            if s.ahead is not None:
                got, read_to = s.ahead.next(s.read_at, s.limit)
            else:
                got, read_to = self.src_engine.neutral_changes(
                    "src", db, s.read_at, limit=s.limit)
            s.got = got
            short = len(got) < s.limit
            if short:
                s.caught_up = asked
                s.limit = self.TAIL_BATCH
            else:
                # behind: larger reads, so what a batch costs whatever
                # its size - the schema asked again, the position
                # written, a statement a table - is paid less often
                s.limit = min(s.limit * 2, self.TAIL_BATCH_MOST)
            changes, s.carried = s.carried + got, []
            s.read_at = token = read_to
            cut = self._commit_cut(changes) if can_cut else None
            if cut is not None:
                changes, s.carried = changes[:cut], changes[cut:]
                token = changes[-1]["txn_at"]
            rows = [c for c in changes if not canon.is_ddl(c)]
            accounts.read(rows)
            if keeper:
                keeper.read(rows)
            # an online schema change's working tables are not on the
            # target and are not the application's data
            kept = [c for c in rows if not drift.transient(c["table"])]
            accounts.dropped("not the application's", rows, kept)
            if (kept or len(rows) < len(changes)) and s.known is not None:
                # before the batch is applied: rows on both sides of a
                # DDL may be in it, and its position is not saved yet
                asked_again = (not ddl_logged
                               or len(rows) < len(changes)
                               or _time.monotonic() - s.shaped_at >= 60)
                if asked_again:
                    s.shaped_at = _time.monotonic()
                    s.known, more = self._shape_now(
                        db, token_path, saved_token, not_live(), stops)
                    s.lacking = {**s.lacking, **more}
            plan, before, work, released = self._tail_batch(
                db, changes, s.lacking, tables, two_way, saved_token,
                s.targets, copied, accounts, log)
            s.lacking = {}
            s.left += sum(accounts.now.get(tailtables.COPIED,
                                           {}).values())
            applied = [c for mapped, _ in plan for c in mapped]
            accounts.check(db, applied, saved_token)
            numbered_now = exact and bool(applied) and (
                two_way or (len(plan) == 1
                            and self._one_transaction(applied)))
            moves = go and bool(applied or token != saved_token or released)
            next_batch = s.batch + (1 if numbered_now else 0)
            if exact:
                if numbered_now:
                    self.dst_engine._batch_seen = _json.dumps(
                        {"token": token, "batch": next_batch,
                         **({"run": s.env["run"]} if s.env
                            and s.env.get("run") is not None else {}),
                         **fencing.marked(self.hop)},
                        default=str)
                    if rung not in (None, "table"):
                        # where the batch ends, written before it is
                        # applied: a rung that says only that it was
                        # committed cannot say where
                        s.env = tailctl.keep(token_path, {
                            **(s.env or {}), "rung": rung,
                            "next": {"token": token, "batch": next_batch}})
                else:
                    # in lanes side by side, or in runs a DDL cut: more
                    # than one transaction, so not one batch's number -
                    # read again after a stop, as before
                    self.dst_engine.__dict__.pop("_batch_seen", None)
            unkept = set()
            if go:
                # the batch's part of the tail's own state, set down before
                # the target commits it
                s.pending_rows = accounts.kept_rows() if moves else {}
                if tables is not None:
                    tables.prepare(
                        work, int((s.env or {}).get("gen") or 0) + 1
                        if moves else None, s.pending_rows,
                        {"batch": next_batch, "token": token}
                        if moves else None)
                    s.prepared = moves
                    if moves:
                        failpoint.hit("tail.set_down")
                for mapped, act in plan:
                    if mapped:
                        self.dst_engine.neutral_apply("dst", db, mapped)
                    if act is not None:
                        self._carry_ddl(db, act, s.targets, log)
                        unkept.add(act[1])
                if applied or plan[-1][1] is not None or before:
                    failpoint.hit("tail.applied")
            s.seen += len(applied)
            ends = [c.get("txn_ts") for c in changes
                    if c.get("txn_ts") is not None]
            if moves:
                if keeper:
                    keeper.saved(applied, token, saved_token,
                                 unkept | not_live())
                fields = {"batch": next_batch} if exact else {}
                if rung:
                    fields["rung"] = rung
                s.env = tailctl.moved(token_path, s.env, token, **fields)
                s.batch, s.saved, s.prepared = next_batch, token, False
                if tables is not None:
                    tables.committed()
                accounts.committed(s.pending_rows)
                if applied:
                    failpoint.hit("tail.saved")
                if ends:
                    s.through = max(ends)
                    s.lag = max(_time.time() - s.through, 0.0)
            elif go and tables is not None:
                tables.committed(work)
            if applied or released:
                log(said())
            return (token if moves or not go else saved_token,
                    len(applied), short and not s.carried)

        # the tables carried as the source's reader was last opened, where
        # it follows only the tables it is given (`FOLLOWS_LISTED_TABLES`)
        follows_listed = getattr(self.src_engine, "FOLLOWS_LISTED_TABLES",
                                 False)

        def started():
            if window:
                window.__enter__()
            if tables is not None:
                tables.resumed(s.saved)
            s.taking = tables.taken() if tables is not None \
                else frozenset()

        def ended():
            try:
                if gc_back is not None:
                    gc_back()
                if tables is not None:
                    tables.close()
                if s.ahead is not None:
                    s.ahead.close()
                else:
                    self.src_engine.release_changes()
            finally:
                try:
                    joined.__exit__(*sys.exc_info())
                finally:
                    if window:
                        window.__exit__(*sys.exc_info())
        tailctl.run_loop(
            where, db, look, log, "changes", hop=self.hop, go=go,
            token=token, lost=lost, reached=reached, beat_with=beat_with,
            until_=until if go else "stopped", on_start=started,
            on_end=ended, secrets=(self.hop.source.password,
                                   self.hop.target.password))

    #: source engines whose change log carries every DDL, as it happened
    #: (`LOG_CARRIES_DDL`): a MySQL binlog logs each statement, and the
    #: reader hands each over (`canon.ddl`). The rest ask their catalogue
    #: every batch, by its digest first (`_shape_now`): PostgreSQL's
    #: logical decoding carries no DDL but a TRUNCATE, SQL Server's change
    #: tables none, SQLite keeps no log; Oracle's redo and TiDB's feed
    #: carry DDL, but their readers hand on only the rows
    _DDL_LOGGED = ("mysql",)

    def _tail_batch(self, db, changes, lacking, tables, two_way, token,
                    targets, copied, accounts, log):
        """One batch as the target is to take it: ([(changes to apply, the
        DDL carried after them or None)], the statements run on the target
        for it, the tables' states the batch leaves, changes set aside
        earlier applied first).

        Cut at each DDL the batch holds into runs, each routed by the
        tables' states at its place (`tailtables.route`): a truncate the
        hop's `ddl` carries is done between the runs it falls between; a
        column the target lacks is added here, before anything of the
        batch, where it is one the target takes at any place
        (`_additive`) and the table's `ddl` says apply - its table parked
        where it is not, or where the target refuses it. `token` is where
        the batch was read from. Count-only (`tables` None) routes nothing:
        every change goes on, and a DDL is said."""
        from .. import canon, drift, twoway
        work = tables.begin() if tables is not None else None
        released, back = [], {}
        if tables is not None:
            released, back, gone = tables.release(work)
            accounts.released(back)
            if gone:
                log(f"{gone} changes held for a copy were in what it read,"
                    " and left out")
        before = []
        for src_t, (dst_t, missing) in sorted(lacking.items()):
            if work is None:
                log(f"{src_t}: the target does not have"
                    f" {', '.join(missing)} (count-only: nothing done)")
                continue
            leaf = tables.key(src_t)
            if (work.get(leaf) or {}).get("state", "live") != "live":
                continue
            # the table's own `ddl`, where the hop says it per table
            pol = tailtables.policy(self.hop, [db, *str(src_t).split(".")])
            sqls, why = (self._additive(db, src_t, dst_t, missing)
                         if pol == "apply" else
                         (None, f"the hop's ddl is {pol} for it"))
            for sql in sqls or ():
                # before any of the batch: a column added takes NULL or
                # its constant in every row, so the rows before its place
                # in the log land as they did on the source
                try:
                    self._run_ddl(db, sql)
                    before.append(sql)
                    log(f"{src_t}: carried to the target at its place in"
                        f" the log: {sql}")
                except Exception as e:  # noqa: BLE001 - parked, said
                    if not self._level(db, src_t):
                        sqls, why = None, (str(e).strip().splitlines()
                                           or [type(e).__name__])[0][:200]
                        break
            if not sqls:
                tables.park(work, leaf, token,
                            f"the target does not have"
                            f" {', '.join(missing)} ({why})")
        runs, acts = [[]], []
        for c in changes:
            if drift.transient(c["table"]):
                continue
            if not canon.is_ddl(c):
                runs[-1].append(c)
                continue
            if work is None:
                log(f"{c['table']}: {c['kind']} on the source (count-only:"
                    " nothing done)")
                continue
            act = tables.ddl(work, c, token, two_way)
            if act is not None:
                acts.append(act)
                runs.append([])
        if work is not None:
            # the DDLs the hop leaves out, never applied: counted, and said
            for (table, why), n in sorted(tables.left_ddl().items()):
                accounts.ddl_left(table, why, n)
                log(f"{table}: {why}" + (f" ({n})" if n > 1 else ""))
        plan = []
        for i, run in enumerate(runs):
            if work is not None:
                run, why_left = tables.route(work, run, token)
                for why, counts in why_left.items():
                    accounts.left(why, counts)
            if i == 0:
                run = released + run
            run, carried = copied.leave_out(run)
            accounts.left(tailtables.COPIED, carried)
            mapped, flattened = self._to_target(db, run, targets, accounts)
            if flattened:
                log(f"{flattened} values were not there on the source and"
                    f" landed as NULL - {self.dst_name} cannot store the"
                    " difference")
            if two_way and work is not None and mapped:
                # each row held to the target's as it is now: changed
                # there too, the hop's policy decides
                kept = mapped
                mapped = twoway.resolve(self, db, mapped, log)
                accounts.dropped("the target's own change kept", kept,
                                 mapped)
            plan.append((mapped, acts[i] if i < len(acts) else None))
        return plan, before, work, released

    def _one_transaction(self, changes):
        """Whether the target applies `changes` in one transaction: not in
        lanes side by side (`Engine._lanes`) - a target that keeps a ledger
        of its batches applies each whole (`EXACT_BATCHES`)."""
        dst = self.dst_engine
        return (getattr(dst, "EXACT_BATCHES", False)
                or len(changes) < getattr(dst, "LANES_FROM", 2000)
                or int(getattr(self.hop, "workers", 1) or 1) < 2
                or not getattr(dst, "WRITES_IN_PARALLEL", True))

    def _carry_ddl(self, db, act, targets, log):
        """A DDL the hop's `ddl` carries, done on the target at its place
        among the runs of a batch: a table emptied on the source emptied
        there - within the hop's row filter, as a copy empties it. Never
        where the hop does not carry the table's deletes: what the source
        takes away the target keeps (`tailtables.TableSet.ddl` leaves such
        a truncate out before it comes here)."""
        kind, src_t = act
        if kind != "truncate":
            return
        if "delete" not in self.hop.carries(db, *self.hop.parts_of(src_t)):
            log(f"{src_t}: emptied on the source; the hop does not carry its"
                " deletes, so the target keeps its rows")
            return
        dst_t = self._tail_target(targets, src_t, db)
        _, dst_where = self._row_scope(db, src_t)
        gone = self.dst_engine.neutral_empty(
            "dst", db, dst_t, **({"where": dst_where} if dst_where else {}))
        log(f"{src_t}: emptied on the source; {dst_t} emptied on the target"
            f" at the same place in the log ({gone or 0:,} rows)")

    def _fence(self, db):
        """Where the source's log is now, in the form the tail saves its
        position in, or None where that cannot be compared."""
        try:
            return self.src_engine.log_position("src", db)
        except Exception:  # noqa: BLE001 - ends on an empty read instead
            return None

    def watch_sample(self, db):
        """Rows on each side, for any pair rather than one of them.

        This was written when hetero meant MySQL to PostgreSQL and reached
        straight for `self.my` and `self.pg`. On every other pairing those
        are None, so `migkit watch` on a cross-engine hop died with
        `AttributeError: 'NoneType' object has no attribute '_tables'` - on
        the one command an operator leaves running during a cutover.

        The counts come from the same comparison `check` uses, so a pair that
        can be compared can be watched. It costs a pass per table per tick,
        which is what the single-engine watch costs too.
        """
        import time
        if self.my and self.pg:
            tables = self.my._tables("src", db)
            a = sum(self.my._q("src",
                               f"select count(*) from `{db}`.`{t}`")[0][0]
                    for t in tables)
            try:
                # the target's own names for its tables; and a count that
                # could not be taken is not a count of zero, which is what
                # this used to report
                b = sum(int(self.pg._psql(
                            "dst", db, f'select count(*) from'
                                       f' "{self._leaf(self._rename(t, db))}"'))
                        for t in tables)
            except (RuntimeError, ValueError) as e:
                return {"db": db, "ts": time.time(), "src_rows": a,
                        "error": str(e).splitlines()[-1][:120] if str(e)
                        else type(e).__name__}
            return {"db": db, "ts": time.time(), "src_rows": a,
                    "dst_rows": b}
        try:
            rows = self._neutral_rows(db)
        except Exception as e:
            return {"db": db, "ts": time.time(),
                    "error": f"{type(e).__name__}: {str(e).splitlines()[-1][:90]}"}
        src = sum(n for _, _, _, n, _ in rows if isinstance(n, int))
        dst = sum(n for _, _, _, _, n in rows if isinstance(n, int))
        return {"db": db, "ts": time.time(), "src_rows": src,
                "dst_rows": dst}


def _copy_range(item):
    """What a process of `_move_in_ranges` runs: one range, from a hop
    handed over whole - written under the number its caller holds, where
    it hands one over (`fencing`)."""
    hop, db, src_t, dst_t, after, upto, asked = item
    token = fencing.token_of(asked.get("token"))
    if token is not None:
        fencing.arm(hop, token)
    try:
        return HeteroEngine(hop).copy_range(db, src_t, dst_t, after, upto,
                                            asked.get("way"))
    finally:
        if token is not None:
            fencing.disarm(hop, token)


class _CopiedRanges:
    """What the table copier read, for the tail to leave out the changes
    it already holds (backlog R19, lever 8).

    A tail after a copy replays every change since the position taken
    before the copy, and those made while the copy ran reached the target
    already wherever their range was read after them. Each range keeps the
    source's mark taken just before its read (`ranges.started`) in the
    copy's own checkpoint, and a change a mark covers is left out
    (`ranges.already_read`) - not applied, and its position saved all the
    same. Correct because a range is copied as it is when read and every
    change after that read is applied; the changes before the read are
    superseded by it. "Before" is what the source says was visible when
    the mark was taken, never a log position (`Engine.snapshot_mark`).

    Netflix's DBLog and Debezium's incremental snapshot bracket each chunk
    with watermarks written to the source, and drop the chunk's rows that
    a change inside the window touched; this keeps the copy's rows, drops
    the changes they supersede, and writes nothing to the source.

    Measured, PostgreSQL 16 to PostgreSQL 16, 200,000 rows copied in 8
    ranges two at a time while a writer updated, deleted and inserted
    random rows, the tail started after the copy from the position taken
    before it (`test_a_tail_after_a_copy_applies_only_what_the_copy_did_
    not_read.py`): 2,621 of 6,950 changes left out (38%; 35% and 42% in
    two runs before), and the target's applier busy 0.58s where it was
    1.11s for all 7,383 of the same run with every change applied. The
    tail caught up in 7.5s against 9.8s - and in 5.9s against 6.1s, and
    10.3s against 10.3s, in the two before: on this pair reading the log
    is most of a catch-up (lever 9), and that is read either way.

    What holding a change to the copy costs is paid once an entry where it
    can be (audit SP3): each entry's plan indexed as it is read
    (`ranges.placer`) - its finished ranges were a set built again for
    every change - and a mark asked about a transaction once a batch
    (every engine's `mark_covers` answers by the change's transaction).
    And an entry retires once the tail is past every mark it holds: its
    copy done, and `RETIRE_AFTER` changes of its table in a row that its
    newest mark does not cover - a mark sees what an older one saw, and a
    log gives its transactions in the order they committed, so none after
    them is covered either; the checkpoint is kept after the move, and
    every change to a table ever copied paid for it for the tail's life.
    Retiring leaves nothing out that it would have: a change applied
    again is superseded by the ones after it, as every change after the
    read is - except where the hop's counters add, whose entries never
    retire. Measured in-process (`bench` of the audit's shape: one table
    copied in ranges, each begun under a PostgreSQL-like mark with 200
    transactions in progress, 20,000 changes in batches of 1,000): 24.3
    us a change at 2,000 ranges and 152.9 us at 20,000 before, 3.4 us at
    either now; a table the tail is past every mark of, 1.5 us a change
    over the run at 2,000 ranges, its entry retired after the first
    1,000.

    `MIGKIT_TAIL_APPLY_ALL=1` applies every change, for measuring what
    leaving them out saves."""

    #: the checkpoints the table copier keeps (`cli._move_full`,
    #: `cli._copy_routed`)
    FILES = ("move.json", "move-routed.json")
    #: changes of a table in a row that its finished copy's newest mark
    #: does not cover, after which its entry retires
    RETIRE_AFTER = 1000

    def __init__(self, eng, db, where):
        import os

        from .. import twoway
        self.eng, self.db, self.where = eng, db, where
        self.off = os.environ.get("MIGKIT_TAIL_APPLY_ALL", "") not in ("",
                                                                     "0")
        self.said, self.tables = None, {}
        # {(entry, its plan): True} once retired; {entry: changes in a row
        # its newest mark did not cover}
        self.retired, self.uncovered = set(), {}
        # a mark's answer for a transaction, for one batch
        self._answers = {}
        self.keep_all = twoway.exact(eng.hop)

    @staticmethod
    def _plan(st):
        rs = st.get("ranges") or [[None, None]]
        return (json.dumps(rs[0], default=str), len(rs),
                json.dumps(st.get("mark_before"), default=str))

    def _load(self):
        """The checkpoints as saved now in `where` - the database's report
        directory, beside the tail's position - each as its file and the
        journal after it say (`checkpoint.read_move`), read again only when
        one of them was written since."""
        from .. import checkpoint, ranges
        d = self.where
        said = []
        for name in self.FILES:
            for p in (d / name, checkpoint.journal_of(d / name)):
                try:
                    s = p.stat()
                except OSError:
                    continue
                said.append((p.name, s.st_ino, s.st_mtime_ns, s.st_size))
        if said == self.said:
            return
        self.said, self.tables = said, {}
        for name in self.FILES:
            for entry, st in (checkpoint.read_move(d / name) or {}).items():
                if isinstance(st, dict) and st.get("ranges") \
                        and st.get("table") and st.get("key") \
                        and (entry, self._plan(st)) not in self.retired:
                    seen = list((st.get("ranges_seen") or {}).values())
                    self.tables.setdefault(
                        self.eng._leaf(st["table"]), []).append(
                        (entry, st, ranges.placer(st),
                         seen[-1] if seen else st.get("mark_before")))

    def _entries(self, table):
        """The entries kept for the source's `table`, by its name as the
        log gives it - schema and all, where both name one."""
        a = str(table).split(".")
        return [got for got in self.tables.get(self.eng._leaf(table), [])
                if len(a) < 2 or len(str(got[1]["table"]).split(".")) < 2
                or str(got[1]["table"]).split(".") == a]

    @staticmethod
    def _values(st, change):
        """The values of the ranges' key the change touches: its row's,
        and the new one where it moves the key; None where the change is
        not keyed by that column alone."""
        k = st["key"]
        key = change.get("key") or {}
        if list(key) != [k]:
            return None
        moved = (change.get("values") or {}).get(k, key[k])
        return [key[k]] if moved == key[k] else [key[k], moved]

    def _covers(self, change):
        """`covers(mark)` for one change: the source's answer, asked once
        a batch for each mark and transaction."""
        src = self.eng.src_engine
        txn = change.get("txn")
        try:
            hash(txn)
        except TypeError:
            txn = None

        def covers(mark):
            if txn is None:
                return src.mark_covers("src", self.db, mark, change)
            at = (mark if isinstance(mark, str)
                  else json.dumps(mark, sort_keys=True, default=str), txn)
            if at not in self._answers:
                self._answers[at] = src.mark_covers("src", self.db, mark,
                                                    change)
            return self._answers[at]
        return covers

    def _read(self, change, ahead=None):
        """Whether every entry kept for the change's table says the copy
        holds it; `ahead(entry, st)` whether a range not begun counts."""
        from .. import ranges
        found = self._entries(change["table"])
        covers = self._covers(change)
        for entry, st, place, _ in found:
            values = self._values(st, change)
            if values is None or not ranges.already_read(
                    st, values, covers,
                    ahead=bool(ahead and ahead(entry, st)), place=place):
                return False
        return bool(found)

    def _past(self, change):
        """Count the change against every finished entry of its table that
        its newest mark does not cover, and retire the ones the tail is
        now past (the class's words)."""
        if self.keep_all:
            return
        covers = self._covers(change)
        for entry, st, _, newest in self._entries(change["table"]):
            if not st.get("done") or newest is None:
                continue
            if covers(newest) is False:
                n = self.uncovered[entry] = self.uncovered.get(entry, 0) + 1
                if n >= self.RETIRE_AFTER:
                    self.retired.add((entry, self._plan(st)))
                    self.said = None
            else:
                self.uncovered[entry] = 0

    def _copying(self, entry, st):
        """Whether the process that planned the table's ranges holds the
        move now: the hop's lease, or the table's where machines share
        the move (`cli._share_tables`)."""
        from .. import lease
        from ..state import run_state
        who = st.get("copying") or {}
        hop = self.eng.hop
        remote = run_state(hop)
        for path in (hop.report_dir() / "lease.json",
                     hop.report_dir(self.db) / f"lease-{entry}.json"):
            try:
                have = lease.holder(path, remote)
            except Exception:  # noqa: BLE001 - not known to be running
                have = None
            if have and (have.get("host"), have.get("pid")) == (
                    who.get("host"), who.get("pid")):
                return True
        return False

    def leave_out(self, changes):
        """(the changes to apply, in their order and without the
        transaction each names; {table: how many were left out})."""
        from collections import Counter

        from .. import canon

        def bare(c):
            return ({k: v for k, v in c.items()
                     if k not in canon.FROM_THE_LOG}
                    if any(k in c for k in canon.FROM_THE_LOG) else c)
        if self.off or not changes:
            return [bare(c) for c in changes], Counter()
        self._load()
        if not self.tables:
            return [bare(c) for c in changes], Counter()
        self._answers = {}
        gone = [self._read(c) for c in changes]
        # a range not begun: left to the copy only while the copy that
        # planned it runs, and only for a change visible before the
        # checkpoint was read - its read begins after that
        maybe = [i for i, g in enumerate(gone)
                 if not g and self._read(changes[i], lambda e, st: True)]
        if maybe:
            live = {}

            def ahead(entry, st):
                if entry not in live:
                    live[entry] = self._copying(entry, st)
                return live[entry]
            if any(self._read(changes[i], ahead) for i in maybe):
                src = self.eng.src_engine
                mark = src.snapshot_mark("src", self.db)
                if mark is not None:
                    self.said = None
                    self._load()
                    live.clear()
                    for i in maybe:
                        if src.mark_covers("src", self.db, mark,
                                           changes[i]) is True:
                            gone[i] = self._read(changes[i], ahead)
        for c in changes:
            self._past(c)
        self._answers = {}
        return ([bare(c) for c, g in zip(changes, gone) if not g],
                Counter(c["table"] for c, g in zip(changes, gone) if g))


class _Accounts:
    """Every change the tail reads, per table, accounted for before its
    batch is applied: read = applied + left out as already in what the
    copy read (`_CopiedRanges`) + left out as not the application's (an
    online schema change's working table) + left out where, two ways, the
    target's own change was kept (`twoway.resolve`) - less the second
    delete one change becomes when it moves a key out of the row filter
    (`_in_scope`). Each count is taken where its step says it, and a step
    between the read and the applier that loses a change it does not say
    stops the tail with the numbers, before anything of the batch is
    applied or its position saved; the batch is read again when the tail
    starts. What has run so far is kept in the tail's own store
    (`tailtables.Store`), each table's row written with the batch that
    read it (`kept_rows`) - not every table's rewritten each batch.

    A tail whose tables move on their own (`tailtables`) accounts for
    three reasons more - held for a range of a copy not landed yet, set
    aside while the table is parked, out of the hop's scope - and for the
    changes set aside earlier that a batch applies (`released`): read +
    released = applied + left out, table by table. The check that would
    have caught TiDB DM decoding an added table's rows empty and dropping
    them (tiflow #12859), and PostgreSQL before 17.5 filtering a table
    added to a publication.

    Beside the identity, two things the tail says of a table and does not
    count against it: the changes a large value the log left out as
    unchanged was kept as the target holds it in (`canon.UNCHANGED`), and
    the DDLs the hop leaves out (`ddl_left`)."""

    #: why a change read is not applied, in the words the store keeps
    WHY = (tailtables.COPIED, "not the application's",
           "the target's own change kept", tailtables.NOT_CARRIED) \
        + tailtables.WHY
    #: changes with a large value kept as the target holds it
    UNCHANGED = "a large value kept as the target holds it"

    def __init__(self, store):
        from collections import Counter
        self.store = store
        self.total = store.accounts()
        self.rows = {}
        self.now, self.split, self.back = {}, Counter(), {}
        self.came_back = Counter()
        self.said = {}

    def read(self, changes):
        from collections import Counter

        from .. import canon
        self.now = {"read": Counter(c["table"] for c in changes)}
        self.split, self.back, self.rows = Counter(), {}, {}
        self.came_back = Counter()
        self.said = {}
        kept = Counter(c["table"] for c in changes
                       if c.get(canon.UNCHANGED))
        for t, n in kept.items():
            self.said.setdefault(t, Counter())[self.UNCHANGED] += n

    def ddl_left(self, table, why, n=1):
        """`n` DDLs of `table` the hop leaves out, for `why`."""
        from collections import Counter
        self.said.setdefault(table, Counter())[f"ddl {why}"] += n

    def released(self, counts):
        """`counts` {source table: changes} set aside in an earlier batch
        and applied in this one."""
        self.came_back.update(counts)

    def left(self, why, counts):
        """`counts` {source table: changes} left out for `why`."""
        from collections import Counter
        self.now.setdefault(why, Counter()).update(counts)

    def dropped(self, why, before, after):
        """What a step that only leaves changes out left out, by the
        tables of its input and output (source or target names)."""
        from collections import Counter
        gone = Counter(c["table"] for c in before)
        gone.subtract(Counter(c["table"] for c in after))
        self.left(why, {self.back.get(t, t): n for t, n in gone.items()
                        if n > 0})

    def grown(self, before, after):
        """Deletes a step added: a key moved out of the row filter goes
        from both its places."""
        from collections import Counter
        more = Counter(c["table"] for c in after)
        more.subtract(Counter(c["table"] for c in before))
        self.split.update({t: n for t, n in more.items() if n > 0})

    def named(self, source, target):
        self.back[target] = source

    def check(self, db, changes, saved):
        """The batch's changes about to be applied, held to what was
        read; raises where a change went nowhere."""
        from collections import Counter
        applied = Counter(self.back.get(c["table"], c["table"])
                          for c in changes)
        read = self.now.get("read", Counter())
        back = self.came_back
        self.rows = {}
        for t in set(read) | set(applied) | set(back):
            gone = {w: self.now.get(w, Counter())[t] for w in self.WHY}
            out = applied[t] + sum(gone.values())
            if read[t] + back[t] + self.split[t] != out:
                raise SystemExit(
                    f"{db}.{t}: the tail read {read[t]:,} changes"
                    + (f" and took back {back[t]:,} set aside" if back[t]
                       else "")
                    + f" and accounts for {out - self.split[t]:,} -"
                    f" {applied[t]:,} to apply, {out - applied[t]:,} left"
                    " out. Nothing of the batch was applied, and the"
                    f" position stays at {str(saved)[:60]}: the batch is"
                    " read again when the tail starts")
            self.rows[t] = {"read": read[t], "applied": applied[t],
                            **({"taken back": back[t]} if back[t] else {}),
                            **{w: n for w, n in gone.items() if n}}
        for t, said in self.said.items():
            self.rows.setdefault(t, {}).update(said)

    def kept_rows(self):
        """{table: what has run, with the batch checked last added}, for
        the batch's transaction (`tailtables.TableSet.prepare`)."""
        out = {}
        for t, row in self.rows.items():
            have = dict(self.total.get(t) or {})
            for k, n in row.items():
                have[k] = have.get(k, 0) + n
            out[t] = have
        return out

    def committed(self, rows):
        """The batch's position is saved: its rows are what has run."""
        self.total.update(rows or {})
        self.rows = {}

    def forget(self):
        self.rows = {}


class _ReadAhead:
    """The tail's next read, made while the batch before it is applied.

    Measured, MySQL to PostgreSQL, 320,000 changes queued: reading them took
    9.4s and applying them 6.1s, one after the other. Only for a source
    whose position says where to read from and nothing more
    (`READS_AHEAD`): a PostgreSQL slot takes the position it is handed as
    everything before it applied, and reading ahead would move it past
    changes not applied yet. A read ahead is used only if the batch asked
    for starts where it did; after a lost connection the tail goes back to
    its saved position, and what was read ahead is dropped."""

    def __init__(self, eng, db):
        import concurrent.futures as cf
        self.eng, self.db = eng, db
        self.pool = cf.ThreadPoolExecutor(1)
        self.pending = None

    def _read(self, token, limit):
        return self.eng.neutral_changes("src", self.db, token, limit=limit)

    def next(self, token, limit):
        got = None
        if self.pending is not None:
            at, fut = self.pending
            self.pending = None
            if at == token:
                got = fut.result()
            else:
                try:
                    fut.result()
                except Exception:  # noqa: BLE001 - dropped with it
                    pass
        if got is None:
            got = self._read(token, limit)
        changes, after = got
        # read on from where this one ended, at the size it will be asked
        # for: behind, twice this one
        ask = limit if len(changes) < limit else limit * 2
        self.pending = (after, self.pool.submit(self._read, after, ask))
        return changes, after

    def close(self):
        if self.pending is not None:
            try:
                self.pending[1].result()
            except Exception:  # noqa: BLE001 - nothing is waiting for it
                pass
            self.pending = None
        self.pool.shutdown(wait=True)
        release = getattr(self.eng, "release_changes", None)
        if release is not None:
            release()


#: what a process of `_ReadProcess` keeps from one read to the next: the
#: source's engine, whose reader holds its stream open between reads
_READING = {}


def _read_changes(item):
    """What the reader's process runs: one read of the source's changes."""
    cls, hop, db, token, limit = item
    eng = _READING.get((cls, hop.name, db))
    if eng is None:
        eng = _READING[(cls, hop.name, db)] = cls(hop)
    return eng.neutral_changes("src", db, token, limit=limit)


class _ReadProcess:
    """The tail's next read made in a process of its own, while the batch
    before it is applied here.

    `_ReadAhead` reads in a thread, and a thread shares the interpreter's
    lock: the binlog is decoded in Python as the batch is applied in
    Python, so the two took turns on one core. Measured, MySQL 8.4 to
    PostgreSQL 16 on a laptop, 320,000 changes queued
    (`bench/tail_rates.py`): decoded alone in 4.1s (3.5s of CPU), applied
    alone in 2.7s (2.0s of CPU); the whole tail with the reader in a thread
    5.6s on 5.7s of CPU - one core, the sum of the two - and with it in a
    process 4.9s, 2.6s of CPU here and 4.2s there (a second run: 6.2s and
    4.9s). The reader is the limit now.

    The same reader in the same library, so nothing new is trusted to
    decode a row; what crosses is the change records, pickled - measured,
    1.1us a change there and 0.8us back here, 4.2 MB a 32,000. The same
    positions as `_ReadAhead`: a read ahead is used only where the batch
    asked for starts where it did, and the process knows nothing of what
    was applied - the tail saves the position after the apply, as it did.
    A process that stops is started again and asked from the position the
    tail asks from, which is the one it saved."""

    def __init__(self, eng, db):
        self.cls, self.hop, self.db = type(eng), eng.hop, db
        self.worker = None
        self.pending = None

    def _ask(self, token, limit):
        from ..ranges import _Worker
        if self.worker is None or not self.worker.alive():
            # measured, started, its modules loaded and its engine made in
            # 0.06-0.08s: nothing gained by starting it earlier
            self.worker = _Worker("the change reader's process")
        self.worker.send(_read_changes,
                         (self.cls, self.hop, self.db, token, limit))

    def _read(self, token, limit):
        from ..ranges import WorkerGone
        for again in (False, True):
            try:
                self._ask(token, limit)
                return self.worker.receive()
            except WorkerGone:
                self.worker = None
                if again:
                    raise

    def next(self, token, limit):
        from ..ranges import WorkerGone
        got = None
        if self.pending is not None:
            at, self.pending = self.pending, None
            try:
                answer = self.worker.receive()
            except WorkerGone:
                self.worker = None
            except Exception:  # noqa: BLE001 - raised where it was asked for
                if at == token:
                    raise
            else:
                if at == token:
                    got = answer
        if got is None:
            got = self._read(token, limit)
        changes, after = got
        # read on from where this one ended, at the size it will be asked
        # for: behind, twice this one
        try:
            self._ask(after, limit if len(changes) < limit else limit * 2)
            self.pending = after
        except WorkerGone:
            # started again at the next read, from where that one asks
            self.worker = None
        return changes, after

    def close(self):
        if self.worker is not None:
            # an answer read ahead is one nobody will read
            self.worker.close(wait=self.pending is None)
            self.worker, self.pending = None, None


#: how the tail reads a source whose position only says where to read
#: from (`READS_AHEAD`): (name, what an operator reads, footprint), in the
#: order a way never timed is tried. Each is timed while the tail is
#: behind and the faster kept (`_Reader`) - the data a decision engine
#: ranks, by seconds a change
READ_RUNGS = (
    ("python-thread", "in a thread beside the writer", 0),
    ("python-process", "in a process of its own", 1),
)


class _Reader:
    """The tail's reads on the way measured faster on this source and
    this machine.

    What is timed is the tail itself while it is behind: seconds a change
    from one read to the next, the apply included, over batches of the
    largest size (`most`). The first batch read after a change of way is
    not timed - nothing was read ahead for it. Each way is timed `TIMED`
    batches; then the cheaper is kept, a tie within two significant
    figures going to the smaller footprint, and the numbers are written
    beside the tail's position (`FILE`), so a tail started again on this
    machine starts on the way chosen - measured on a laptop, a process
    1.4x as fast in its timed batches as a thread. A process needs a
    second processor to run beside the writer on, and a hop it can be
    handed. A way that fails - its process stopping twice without an
    answer - is left for the run, and the read asked again, from the same
    position, the other way."""

    FILE = "tail-read.json"
    TIMED = 2

    def __init__(self, eng, db, where, log, most):
        self.eng, self.db, self.where, self.log = eng, db, where, log
        self.most = most
        self.names = [n for n, _, _ in READ_RUNGS if self._can(n)]
        self.costs, self.timing, self.readers = {}, {}, {}
        self.on, self.fresh, self.last = self.names[0], False, None
        self.chosen = None
        self._remembered()

    @staticmethod
    def _cpus():
        import os
        try:
            return len(os.sched_getaffinity(0))
        except AttributeError:
            return os.cpu_count() or 1

    def _can(self, name):
        if name != "python-process":
            return True
        import pickle
        try:
            # what the process is handed to read with
            pickle.dumps((type(self.eng), self.eng.hop))
        except Exception:  # noqa: BLE001 - it cannot be handed over
            return False
        return self._cpus() >= 2

    def _machine(self):
        import platform
        return {"host": platform.node(), "cpus": self._cpus()}

    def _remembered(self):
        from .. import durable
        got = durable.read_state(self.where / self.FILE, "cache")
        if not isinstance(got, dict) or \
                got.get("machine") != self._machine():
            return
        costs = {n: float(c) for n, c in
                 (got.get("seconds_per_change") or {}).items()
                 if n in self.names and c is not None}
        if got.get("read changes") in self.names and \
                set(costs) == set(self.names):
            self.costs = costs
            self.on = self.chosen = got["read changes"]

    def ranked(self):
        """[(name, seconds a change or None)], in the order the next
        choice would try them: never timed first, then the cheaper, the
        footprint deciding a tie."""
        import math
        foot = {n: f for n, _, f in READ_RUNGS}
        untried = [n for n in self.names if n not in self.costs]
        tried = sorted((n for n in self.names if n in self.costs),
                       key=lambda n: self.costs[n])
        out = []
        while tried:
            first = self.costs[tried[0]]
            digits = (-int(math.floor(math.log10(first))) + 1
                      if first > 0 else 0)
            tie = [n for n in tried
                   if round(self.costs[n], digits) == round(first, digits)]
            out += sorted(tie, key=lambda n: (foot[n], self.names.index(n)))
            tried = [n for n in tried if n not in tie]
        return [(n, self.costs.get(n)) for n in untried + out]

    def _rung(self, name):
        if name not in self.readers:
            self.readers[name] = (_ReadProcess if name == "python-process"
                                  else _ReadAhead)(self.eng, self.db)
        return self.readers[name]

    def _switch(self, name):
        if name == self.on:
            return
        was = self.readers.pop(self.on, None)
        if was is not None:
            was.close()
        self.on, self.fresh = name, True

    def _timed(self, token, now):
        if self.last is None:
            return
        name, n, after, began, skip = self.last
        self.last = None
        # a read asked again after a lost connection, a batch not read
        # while behind, or the first on its way: not a measure of it
        if skip or after != token or n < self.most or self.chosen:
            return
        seen = self.timing.setdefault(name, [0, 0.0, 0])
        seen[0] += n
        seen[1] += now - began
        seen[2] += 1
        if seen[2] < self.TIMED:
            return
        self.costs[name] = seen[1] / seen[0]
        order = self.ranked()
        if order[0][1] is None:
            self._switch(order[0][0])
            return
        self.chosen = order[0][0]
        self._switch(self.chosen)
        self._keep()
        said = dict((n, s) for n, s, _ in READ_RUNGS)
        other = [(n, c) for n, c in order[1:] if c]
        self.log(f"changes are read {said[self.chosen]}"
                 + (f": {other[0][1] / order[0][1]:.1f}x as fast on this"
                    f" source as {said[other[0][0]]}" if other else ""))

    def _keep(self):
        from .. import durable
        durable.write_json(self.where / self.FILE, {
            "read changes": self.chosen,
            "seconds_per_change": dict(self.ranked()),
            "machine": self._machine()})

    def next(self, token, limit):
        """One read, on the way standing now - and where that way cannot
        run (its process stopping without an answer), on the next, from
        the same position, by the one runtime fallback every way of
        migkit's takes (`decide.attempt`): the way failed is left for the
        run and the switch said in one line. A read the source itself
        refuses, or a connection lost, is not the way failing: it is
        raised as it is, for the tail to read again from its saved
        position."""
        import time as _time

        from .. import decide
        from ..ranges import WorkerGone
        now = _time.monotonic()
        self._timed(token, now)
        skip = [self.fresh]
        self.fresh = False
        said = {n: (s, f) for n, s, f in READ_RUNGS}
        rungs = [decide.Rung(n, f"the reader {said[n][0]}",
                             footprint=said[n][1])
                 for n, _ in self.ranked()]

        def run(choice):
            name = choice.rung.name
            if name != self.on:
                self._switch(name)
                skip[0] = True
                self.fresh = False
            return self._rung(name).next(token, limit)

        def undo(rung, error):
            # the reader that failed, dropped, and not tried again this run
            was = self.readers.pop(rung.name, None)
            if was is not None:
                try:
                    was.close()
                except Exception:  # noqa: BLE001 - it is gone already
                    pass
            if rung.name in self.names:
                self.names.remove(rung.name)
            self.costs.pop(rung.name, None)
            # a choice made stands on what is left; none made, what is
            # left is timed as before
            self.chosen = (self.names[0] if self.chosen and self.names
                           else None)
        choice, (changes, after) = decide.attempt(
            "reading the source's changes", rungs, run, keep=self.on,
            undo=undo, log=self.log,
            switch=lambda e: isinstance(e, WorkerGone))
        name = choice.rung.name
        self.on = name
        self.last = (name, len(changes), after, now, skip[0])
        return changes, after

    def close(self):
        for reader in self.readers.values():
            reader.close()
        self.readers = {}
