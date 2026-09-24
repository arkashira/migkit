import csv
import io
import re
import signal
import subprocess
import sys
import time

from ..util import tool_env
from .base import Engine, RepairAction, Result


def _stop_on_term(signum, frame):
    raise KeyboardInterrupt


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

    def __init__(self, hop):
        super().__init__(hop)
        self.src_name = hop.options.get("source_engine", "mysql")
        self.dst_name = hop.options.get("target_engine", "postgres")
        for role, name in (("source_engine", self.src_name),
                           ("target_engine", self.dst_name)):
            if name == "hetero":
                raise SystemExit(f"{role} cannot be 'hetero'")
        from . import engine_named
        self.src_engine = engine_named(self.src_name, hop)
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

    def _mysql_to_postgres_only(self, what):
        if self.my is None or self.pg is None:
            raise SystemExit(
                f"hetero {self.src_name}->{self.dst_name}: {what} is still"
                " written for mysql->postgres only. `check` works on this"
                " pair; this does not.")

    def databases(self):
        return self.src_engine.databases()

    # ---- comparing, for any pair ---------------------------------------

    @staticmethod
    def _leaf(identifier):
        """The table's own name, without whatever qualifies it.

        MySQL answers `orders`; PostgreSQL answers `public.orders`. Matching
        on the last component is what lets the two lists meet without either
        engine having to know the other exists.
        """
        return str(identifier).split(".")[-1]

    def _rename(self, identifier):
        """This source table's name on the target, from the hop's mapping.

        One place, so the pairing and the message explaining it cannot
        disagree about which rename applied.
        """
        parts = [p for p in str(identifier).split(".") if p]
        return self.hop.target_table(*parts)

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

    def _column_plan(self, src_table, dst_table, db):
        """The base's column plan, over this hop's two engines."""
        return self._comparable_columns(db, self.src_engine, src_table,
                                        self.dst_engine, dst_table)

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
                                                         self._rename)
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
        drift = [f"{n} is {got['src_types'][n]} on the source and"
                 f" {got['dst_types'][n]} on the target, which are not the"
                 " same kind of value"
                 for n, s, d in got["pairs"] if s is not d]
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
            src_ids, dst_ids, self._rename)
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
            want = self._rename(t)
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
        for src_t, dst_t in pairs:
            scope = f"{db}.{self._leaf(src_t)}"
            try:
                src_cols, dst_cols, notes = self._column_plan(src_t, dst_t, db)
            except Exception as e:
                rows.append((scope, "error",
                             f"cannot read the columns: {str(e)[:120]}",
                             None, None))
                continue
            tail = ("; " + "; ".join(notes)) if notes else ""
            if not src_cols:
                rows.append((scope, "warn",
                             "no column on this table can be compared across"
                             " these two engines" + tail, None, None))
                continue
            try:
                wheres = self._row_scope(db, src_t)
                a = self.src_engine.neutral_digest(
                    "src", db, src_t, src_cols,
                    **({"where": wheres[0]} if wheres[0] else {}))
                b = self.dst_engine.neutral_digest(
                    "dst", db, dst_t, dst_cols,
                    **({"where": wheres[1]} if wheres[1] else {}))
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
            if stream:
                stream(f"{scope}: {'ok' if a == b else 'diff'}")
            if a == b:
                self._write_drill(db, self._leaf(src_t), missing=[],
                                  changed=[], extra=[])
                rows.append((scope, "ok",
                             f"rows {a[0]:,} and every compared column equal"
                             f" across {self.src_name}/{self.dst_name}"
                             f" (digest {a[1]}){self._where_folded()}{tail}",
                             a[0], b[0]))
                continue
            head = (f"rows src={a[0]:,} dst={b[0]:,}" if a[0] != b[0] else
                    f"rows {a[0]:,} match but the contents do not:"
                    f" digest src={a[1]} dst={b[1]}")
            rows.append((scope, "diff",
                         head + self._drill(db, src_t, dst_t, src_cols,
                                            dst_cols) + tail,
                         a[0], b[0]))
        return rows

    #: how many rows per side a drilldown will walk before saying it stopped

    def _drill(self, db, src_t, dst_t, src_cols, dst_cols):
        """The base's walk, over this hop's two engines, through the hop's
        row filter on each side."""
        return self._drill_rows(db, self._leaf(src_t),
                                self.src_engine, src_t, src_cols,
                                self.dst_engine, dst_t, dst_cols,
                                self._row_scope(db, src_t))

    def _neutral_compare(self, db, table=None, stream=None):
        return [Result("data", scope, status, detail)
                for scope, status, detail, _, _
                in self._neutral_rows(db, table, stream)]

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
            if engine.CANON_ENGINE not in canon.IN_PROCESS:
                continue
            (remote if engine.OVER_NETWORK else local).append(name)
        parts = []
        if remote:
            parts.append(f"{', '.join(remote)} has no hashing operator of its"
                         " own, so its rows crossed the network to be folded"
                         " here")
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

    def _health(self, side):
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
        try:
            return probe(side)
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
            self.dst_engine.neutral_tables("dst", db), self._rename)
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
            src_cols.append((back.get(name, name), canon.type_class(
                self.src_engine.CANON_ENGINE, src_types[name])))
            dst_cols.append((name, canon.type_class(
                self.dst_engine.CANON_ENGINE, dst_types[name])))
        return src_cols, dst_cols, notes

    def _neutral_move(self, db, sch, tbl, chunk, ck, log):
        """Read from one engine, write to the other, for any pair.

        Resumable through the checkpoint the caller already keeps, by the
        target table's own key. A table with no key is read in one pass and
        the log says so - restarting that one starts it over, which is the
        honest consequence of there being nothing to resume from.
        """
        src_t = f"{sch}.{tbl}" if sch else tbl
        leaf = self._leaf(src_t)
        pairs, _, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db), self._rename)
        match = [d for s_, d in pairs if self._leaf(s_) == leaf]
        # the name it goes by on the target, through the hop's mapping: a
        # renamed table that is not there yet is made under its new name
        want = self._leaf(self._rename(src_t))
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
        key = f"{db}.{leaf}"
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        after = tuple(st["last"]) if st.get("last") is not None else None
        src_where, dst_where = self._row_scope(db, src_t)
        read_scope = {"where": src_where} if src_where else {}
        if after is None:
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
        gate = Throttle(1, probe=lambda: self._health("src"))
        while True:
            with gate.unit():
                rows, last = self.src_engine.neutral_read(
                    "src", db, src_t, src_cols, after, chunk, **read_scope)
            if not rows:
                break
            rows, flattened = self._flatten_absent(rows)
            absent += flattened
            self.dst_engine.neutral_write("dst", db, dst_t, dst_cols, rows)
            moved += len(rows)
            st["moved"] = moved
            if last is None:
                log(f"{key}: {moved:,} rows in one pass (no key to resume"
                    " from, so a restart starts over)")
                break
            after = last
            st["last"] = list(last)
            ck.save()
            log(f"{key}: {moved:,} rows")
        if absent:
            log(f"{key}: {absent:,} values were not there on the source and"
                f" landed as NULL - {self.dst_name} has no way to store"
                " \"this field is not here\" apart from \"this field is"
                " null\", so the distinction ends at this hop")
        st["done"] = True
        ck.save()

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
            columns.append((name, cls, canon.params(typ),
                            rules.get(name) or {}))
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
        return columns, key

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
            self.src_engine.neutral_tables("src", db), there, self._rename)
        for src_t in src_only:
            if not self.hop.excluded(db, *str(src_t).split(".")):
                self._create_target(db, src_t,
                                    self._leaf(self._rename(src_t)), log)

    def finish_created(self, db, log=None):
        """The source's indexes on the tables this pair built, once their
        rows are in - the faster order, as on every other path.

        A built table had none: every query the application made after
        cutover read it whole, and a unique index the source kept was not
        there to refuse the duplicate it refuses. One over an expression, a
        predicate or a part of a column cannot be given to another engine
        as it is, and is named instead."""
        import json as _json
        log = log or (lambda m: None)
        record = self.hop.report_dir(db) / "built-tables.json"
        try:
            built = _json.loads(record.read_text())
        except (OSError, ValueError):
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
        read through it, before anything is read."""
        pred = self.hop.row_filter(db, *str(src_table).split("."))
        if not pred:
            return None, None
        src = self.src_engine.SQL_DIALECT
        dst = self.dst_engine.SQL_DIALECT
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
            import sqlglot
            from sqlglot import exp
            tree = sqlglot.parse_one(pred, read=src)
            for col in tree.find_all(exp.Column):
                if col.name in forward and forward[col.name] != col.name:
                    col.set("this", exp.to_identifier(forward[col.name]))
            there = tree.sql(dialect=dst,
                             unsupported_level=sqlglot.ErrorLevel.RAISE)
        except Exception as e:
            raise SystemExit(
                f"the row filter on {src_table} ({pred}) has no translation"
                f" into {self.dst_name}'s SQL"
                f" ({str(e).splitlines()[0][:80]}), so the target cannot be"
                " read through it. Write it so both engines accept it")
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
        import json as _json
        record = self.hop.report_dir(db) / "built-tables.json"
        try:
            built = _json.loads(record.read_text())
        except (OSError, ValueError):
            built = {}
        built[leaf] = str(src_table)
        record.write_text(_json.dumps(built))
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
            return [Result("counts", scope, status, detail)
                    for scope, status, detail in bad[:10]]
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
        tail and has not put back."""
        out = [r for r in (self.dst_engine.set_aside(db),
                           self._zero_date_result(db),
                           self.dst_engine._fk_orphans(db)) if r]
        return out or super().check_deep(db)

    def load_window(self, db, log=None, tables=None):
        """The target's own, over the names the tables have there."""
        if tables is not None:
            tables = {self._leaf(self._rename(t)) for t in tables}
        return self.dst_engine.load_window(db, log, tables)

    def planned_checks(self):
        """A second reading, where it can be had.

        A cross-engine pair is where a second opinion earns its cost: every
        value is converted on the way, and migkit's own reading is the only
        one looking. So it runs whenever the independent reader is installed
        and speaks both engines, and never for pairs it cannot read.
        """
        from .. import second_reader
        more = super().planned_checks()
        if (second_reader.interpreter()
                and self.src_name in second_reader.READER_TYPES
                and self.dst_name in second_reader.READER_TYPES):
            return ("second",) + more
        return more

    def run_rule(self, side, db, sql):
        engine = self.src_engine if side == "src" else self.dst_engine
        return engine.run_rule(side, db, sql)

    def check_second(self, db):
        """Every paired table read a second way: counts, and the sum,
        minimum and maximum of every column, through another library and
        other SQL.

        Aggregates, not the reader's row hash. Measured on a MySQL and a
        PostgreSQL table holding the same values: the aggregates and a
        field-by-field comparison agreed on all of them, and the row hash
        called a row with the double `-1e-07` different - the two servers
        spell it differently before hashing. A second reading that reports
        differences which are not there is worse than none.
        """
        from .. import second_reader
        pairs, _, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db), self._rename)
        if not pairs:
            return [Result("second reading", db, "skip",
                           "no table is on both sides to read twice")]
        dst_db = self.hop.target_db(db)

        def named(ident, database):
            # the reader wants schema.table; MySQL's schema is its database
            return ident if "." in str(ident) else f"{database}.{ident}"
        tables = [f"{named(s_, db)}={named(d_, dst_db)}" for s_, d_ in pairs]
        answer = second_reader.run({
            "source": second_reader.connection(self.src_name,
                                               self.hop.source, db),
            "target": second_reader.connection(self.dst_name,
                                               self.hop.target, dst_db),
            "kind": "column", "tables": tables,
            "args": ["--count", "*", "--sum", "*", "--min", "*",
                     "--max", "*"]})
        return second_reader.findings(answer, db)

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
        res = [Result("data", scope, status, detail)
               for scope, status, detail, _, _ in rows]
        if with_counts:
            res = self._counts_rows(db, rows) + res
        return res

    # --- the confirm pass, where migkit's own tail carries the changes ------
    # A difference found while the tail is running may be a change it has
    # read and not applied yet. Before calling it one, wait until the tail
    # has read as far as the source's log is now, then look again: what
    # converged was still arriving.

    def _tail_path(self, db):
        return self.hop.report_dir(db) / "tail-token.json"

    def src_lsn(self, db):
        from .. import tailctl
        if not tailctl.alive(self.hop.report_dir(db)):
            return None
        try:
            return self.src_engine.log_position("src", db)
        except Exception:
            return None

    def fence_wait(self, db, lsn, timeout=300):
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
            time.sleep(1)
        return False

    def _compare_pks(self, db, table, keys):
        """Walk the table again, both sides, and say what still differs."""
        pairs, _, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db),
            self.dst_engine.neutral_tables("dst", db), self._rename)
        match = [(s_, d_) for s_, d_ in pairs if self._leaf(s_) == table]
        if not match:
            return None
        src_t, dst_t = match[0]
        src_cols, dst_cols, _ = self._column_plan(src_t, dst_t, db)
        self._drill(db, src_t, dst_t, src_cols, dst_cols)
        return tuple(self._read_drill(db, table, kind)
                     for kind in ("missing", "extra", "changed"))

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
                                        f" ({proof.get(leaf, 'confirmed')})")
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
        out = []
        for src_t in self.src_engine.neutral_tables("src", db):
            leaf = self._leaf(src_t)
            columns, key = self._target_shape(db, src_t, leaf)
            out.append(self.dst_engine.neutral_create_sql(
                "dst", db, leaf, columns, key) + ";")
        return out

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
            # that cannot be taken is worse than saying so
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
            return None
        try:
            # the target's own probe below reads an error as "no rows", so
            # whether it can be reached at all is asked first
            self.pg._psql("dst", db, "select 1")
            with_rows = [t for t in self.my._tables("src", db)
                         if self.my._q("src", f"select 1 from `{db}`.`{t}`"
                                              " limit 1")]
            landed = {t: "public." + self._leaf(self._rename(t))
                      for t in with_rows}
            present = self.pg.rows_present(db, list(landed.values()))
        except Exception:
            return None
        return sorted(t for t, name in landed.items() if name not in present)

    def list_move_tables(self, db):
        if not (self.my and self.pg):
            if not self._can_move_neutrally():
                self._mysql_to_postgres_only("listing tables to move")
            there = ([] if self.dst_engine.target_missing(db)
                     else self.dst_engine.neutral_tables("dst", db))
            pairs, src_only, _, ambiguous = self.match_tables(
                self.src_engine.neutral_tables("src", db), there,
                self._rename)
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

    def move_table(self, db, sch, tbl, chunk, ck, log):
        # the copier written for MySQL to PostgreSQL copies each table under
        # its source's column names; a table the hop's mapping reshapes
        # goes the way that reads the mapping
        if not (self.my and self.pg) or self.hop.column_rules(db, sch, tbl):
            if not self._can_move_neutrally():
                self._mysql_to_postgres_only("moving a table")
            # before anything is read from the target: some targets are not
            # there until something makes them, and listing what a target
            # already holds is the first thing this does
            made = self.dst_engine.prepare_target(db)
            if made:
                log(f"{self.dst_name}: {made}")
            return self._neutral_move(db, sch, tbl, chunk, ck, log)
        t = tbl or sch
        # the name the hop gives it on the target; this copier wrote every
        # table under its source name, whatever the mapping said
        dst_t = self._leaf(self._rename(t))
        key = f"{db}.{t}"
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        cols = self.my._cols(db, t)
        collist_my = ", ".join(f"`{c}`" for c in cols)
        collist_pg = ", ".join(f'"{c}"' for c in cols)
        # the hop's row filter on both ends: only the rows it selects are
        # read, and only the rows it selects are replaced
        src_where, dst_where = self._row_scope(db, t)
        my_and = f" and ({src_where.replace('%', '%%')})" if src_where \
            else ""
        pg_and = f" and ({dst_where})" if dst_where else ""
        pks = self.my._pk_cols(db, t)
        intpk = None
        if len(pks) == 1:
            r = self.my._q("src",
                           "select data_type from information_schema.columns"
                           " where table_schema=%s and table_name=%s"
                           " and column_name=%s", (db, t, pks[0]))
            if r and r[0][0] in ("tinyint", "smallint", "mediumint",
                                 "int", "bigint"):
                intpk = pks[0]

        def push(rows, pred_pg):
            buf = io.StringIO()
            w = csv.writer(buf)
            for row in rows:
                w.writerow(["" if v is None else
                            v.hex() if isinstance(v, (bytes, bytearray))
                            else v for v in row])
            tgt = self.hop.target
            env = tool_env({"PGPASSWORD": tgt.password,
                            **self.pg.replica_env(db)})
            pre = (f'delete from "{dst_t}" where ({pred_pg}){pg_and};'
                   if pred_pg else
                   f'delete from "{dst_t}" where {dst_where};' if dst_where
                   else f'truncate "{dst_t}";')
            p = subprocess.run(
                ["psql", "-h", tgt.host, "-p", str(tgt.port),
                 "-U", tgt.user, "-d", self.pg._d("dst", db), "-X", "-q",
                 "-v", "ON_ERROR_STOP=1", "-1", "-c", pre,
                 "-c", f"\\copy \"{dst_t}\" ({collist_pg}) from stdin"
                       " (format csv, null '')"],
                input=buf.getvalue(), capture_output=True, text=True, env=env)
            if p.returncode:
                raise RuntimeError(p.stderr[-300:])

        if not intpk:
            log(f"{key}: no single int pk, single-shot copy")
            rows = self.my._q("src", f"select {collist_my} from `{db}`.`{t}`"
                              + (f" where {src_where}" if src_where else ""))
            push(rows, "")
            st["done"] = True
            ck.save()
            return
        mm = self.my._q("src", f"select coalesce(min(`{intpk}`), 0),"
                        f" coalesce(max(`{intpk}`), 0),"
                        f" min(`{intpk}`) is not null"
                        f" from `{db}`.`{t}`"
                        + (f" where {src_where}" if src_where else ""))[0]
        lo, hi, has = int(mm[0]), int(mm[1]), bool(mm[2])
        last = st.get("last", lo - 1)
        while last < hi:
            nxt = min(last + chunk, hi)
            rows = self.my._q("src",
                              f"select {collist_my} from `{db}`.`{t}`"
                              f" where `{intpk}` > %s and `{intpk}` <= %s"
                              + my_and, (last, nxt))
            push(rows, f'"{intpk}" > {last} and "{intpk}" <= {nxt}')
            last = nxt
            st["last"] = last
            ck.save()
            log(f"{key}: up to {intpk}={last:,} of {hi:,}")
        # each chunk replaces its own key range, so a target row outside the
        # source's whole range was in none of them
        push([], f'not ("{intpk}" between {lo} and {hi})' if has else "true")
        st["done"] = True
        ck.save()

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

    @staticmethod
    def _saved_token(token_path):
        """The position a tail saved, None when there is none."""
        import json as _json
        if not token_path.exists():
            return None
        try:
            return _json.loads(token_path.read_text())["token"]
        except (ValueError, KeyError, TypeError):
            # starting from now instead would skip everything between the
            # point this file held and now, and say nothing
            raise SystemExit(
                f"the saved position in {token_path} cannot be read, and a"
                " tail started from anywhere else either skips changes or"
                " cannot say it did not. Remove the file and move again with"
                " --mode full+cdc")

    def _tail_targets(self, db):
        """{source table's leaf: the target table its changes go to}.

        The same pairing the move makes, through the hop's mapping. The tail
        used to write each change to the source's own table name, so a
        table the hop renames was copied under its new name and then kept
        up to date under its old one.
        """
        there = ([] if self.dst_engine.target_missing(db)
                 else self.dst_engine.neutral_tables("dst", db))
        pairs, src_only, _, _ = self.match_tables(
            self.src_engine.neutral_tables("src", db), there, self._rename)
        out = {self._leaf(s_): d for s_, d in pairs}
        for s_ in src_only:
            out[self._leaf(s_)] = self._leaf(self._rename(s_))
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
        its own delete follows."""
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
            cols = [(k, None) for k in key]
            try:
                classes = dict(self.src_engine.neutral_columns("src", db,
                                                               table))
                from .. import canon
                cols = [(k, canon.type_class(self.src_engine.CANON_ENGINE,
                                             classes.get(k)))
                        for k in key]
            except Exception:
                pass
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
            out.append({"op": "delete", "table": c["table"], "key": now})
            if now != c["key"]:
                # an update that moved the key as well: the row's old
                # address goes too, as the apply would have removed it
                out.append({"op": "delete", "table": c["table"],
                            "key": dict(c["key"])})
        return out

    def _mapped_change(self, db, change):
        """A change with `mapping.columns` applied as the copy applied it:
        dropped columns left out, renamed ones under their target names.

        Measured before: with `name` renamed to `label`, the first insert
        the tail applied stopped it on `Unknown column 'name'`, and a
        target that had both would have been written in the wrong one."""
        out = dict(change)
        for part in ("key", "values"):
            if change.get(part):
                out[part] = self._mapped_types(db, change["table"],
                                               change[part])[0]
        if len(out.get("key") or {}) != len(change.get("key") or {}):
            raise SystemExit(
                f"{change['table']}: mapping.columns leaves out part of its"
                " key, so a change cannot be placed on the target by it."
                " Keep the key's columns.")
        return out

    def _tail_target(self, targets, table):
        # a table made on the source after the tail began goes by its
        # mapped name, as the move would have sent it
        return (targets.get(self._leaf(table))
                or self._leaf(self._rename(table)))

    def _shape_gate(self, db, token_path, position):
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
        """
        import json as _json

        from .. import drift
        if drift.reader(self) is None:
            return None
        now = drift.shape(self.src_engine, "src", db)
        if now is None:
            return None
        path = token_path.parent / "tail-shape.json"
        try:
            saved = {t: [tuple(c) for c in cols] for t, cols in
                     _json.loads(path.read_text()).items()}
        except (OSError, ValueError):
            saved = None
        moved = drift.changes(saved, now) if saved is not None else []
        if moved:
            behind = self._target_lacks(db, saved, now)
            if behind:
                raise SystemExit(
                    f"{db}: the source's schema changed under the tail - "
                    + "; ".join(moved[:6])
                    + (" ..." if len(moved) > 6 else "")
                    + f". The target does not have {', '.join(behind[:6])}"
                    f" yet, so nothing after {str(position)[:60]} was"
                    " applied. Bring the target's schema level with the"
                    " source's, then run the tail again; it resumes from"
                    " there.")
        token_path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_json.dumps(now))
        return now

    def _target_lacks(self, db, before, after):
        """`table.column` the source has now, on tables whose shape changed,
        that the target does not."""
        out = []
        targets = self._tail_targets(db)
        for t, cols in sorted(after.items()):
            if dict(cols) == dict(before.get(t, [])):
                continue
            dst_t = self._tail_target(targets, t)
            try:
                have = {str(n).lower() for n, _ in
                        self.dst_engine.neutral_columns("dst", db, dst_t)}
            except Exception:
                have = set()
            out += [f"{dst_t}.{n}" for n, _ in cols
                    if str(n).lower() not in have]
        return out

    def target_mark(self, db):
        return self.dst_engine.target_mark(db)

    def tail_seed(self, db, token_path, point):
        """Start the tail from `point`, a position taken earlier."""
        import json as _json
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(_json.dumps({"token": point}))
        (token_path.parent / "tail-shape.json").unlink(missing_ok=True)

    def tail_start(self, db, token_path):
        """Fix where the tail will begin, before the copy it follows.

        A position already saved is kept: it is older than now, and starting
        earlier only replays changes the appliers are idempotent for, where
        starting later skips them.
        """
        import json as _json
        self._can_tail(db)
        if token_path.exists():
            return False
        point = self.src_engine.change_point("src", db)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(_json.dumps({"token": point}))
        (token_path.parent / "tail-shape.json").unlink(missing_ok=True)
        self._shape_gate(db, token_path, point)
        return True

    def tail_apply(self, db, go, token_path, log):
        """Carry changes from one engine's log into the other, until stopped.

        The hand-written version of this read a MySQL binlog and wrote
        PostgreSQL INSERT statements, and could only ever do that one pair.
        This asks the source for `canon.change` records and hands them to the
        target's applier, so it works for every pair where the source has a
        change log at all - and refuses, naming the pair, where it does not.

        **Applied first, token saved second.** A crash between the two
        replays changes that were already applied, which the appliers are
        idempotent for. Saving the token first would skip them instead, and a
        skipped change is a row that silently never arrives.
        """
        import json as _json
        import time as _time
        self._can_tail()
        # read before anything connects: a file that cannot be read is the
        # answer whatever the servers would have said
        token = self._saved_token(token_path)
        if go and not token_path.exists():
            # saved before the first change is read: a tail stopped before
            # anything arrived would otherwise restart from a later "now"
            # and skip whatever came in between
            self.tail_start(db, token_path)
            token = self._saved_token(token_path)
        else:
            self._can_tail(db)
        log(f"tailing {self.src_name} -> {self.dst_name}, ctrl-c to stop"
            + ("" if go else " (count-only, add --go to apply)")
            + (f", resuming from {str(token)[:40]}" if token else ""))
        from .. import drift, tailctl
        seen = 0
        saved_token = token
        targets = self._tail_targets(db)
        # the source's shape the tail last applied under, saved beside its
        # position, so a DDL made while it was stopped is seen too
        known = self._shape_gate(db, token_path, saved_token)
        running = tailctl.Running(token_path.parent) if go else None
        # the target's triggers held off for as long as changes are applied,
        # as the servers' own replication applies them: what the source's
        # triggers wrote is in the log with the rest, and the target's
        # firing again rewrote the rows and wrote them twice
        window = (self.dst_engine.load_window(db, log, set(targets.values()))
                  if go else None)
        # a service manager stops a process with SIGTERM, which ends it
        # without unwinding: taken as ctrl-c, so what the tail set aside on
        # the target goes back and its marker comes down
        term = None
        if go:
            try:
                term = signal.signal(signal.SIGTERM, _stop_on_term)
            except ValueError:
                pass
        try:
            if running:
                running.__enter__()
            if window:
                window.__enter__()
            while True:
                # between batches, never inside one: everything read so far
                # is applied and its position saved before it holds still
                tailctl.hold_if_asked(token_path.parent, log)
                changes, token = self.src_engine.neutral_changes(
                    "src", db, token, limit=1000)
                # an online schema change's working tables are not on the
                # target and are not the application's data
                changes = [c for c in changes
                           if not drift.transient(c["table"])]
                if changes and known is not None:
                    # before the batch is applied: rows on both sides of a
                    # DDL may be in it, and its position is not saved yet
                    known = self._shape_gate(db, token_path, saved_token)
                if changes:
                    changes = self._in_scope(db, changes)
                if changes:
                    changes = [dict(self._mapped_change(db, c),
                                    table=self._tail_target(targets,
                                                            c["table"]))
                               for c in changes]
                    changes, flattened = self._flatten_changes(changes)
                    if flattened:
                        log(f"{flattened} values were not there on the"
                            f" source and landed as NULL - {self.dst_name}"
                            " cannot store the difference")
                    if go:
                        self.dst_engine.neutral_apply("dst", db, changes)
                    seen += len(changes)
                    if go:
                        token_path.parent.mkdir(parents=True, exist_ok=True)
                        token_path.write_text(_json.dumps({"token": token}))
                        saved_token = token
                    log(f"{seen} changes"
                        + ("" if go else " seen (nothing applied)"))
                else:
                    if go and token != saved_token:
                        # the log moved on with nothing to apply: the new
                        # position is how far the target is, which is what
                        # a fence reads
                        token_path.write_text(_json.dumps({"token": token}))
                        saved_token = token
                    _time.sleep(1)
        except KeyboardInterrupt:
            log(f"stopped after {seen} changes; rerun to resume")
        finally:
            try:
                if window:
                    window.__exit__(*sys.exc_info())
            finally:
                if running:
                    running.__exit__(None, None, None)
                if term is not None:
                    signal.signal(signal.SIGTERM, term)

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
                                       f' "{self._leaf(self._rename(t))}"'))
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
