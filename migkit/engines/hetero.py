import csv
import io
import re
import subprocess

from ..util import run, tool_env, which
from .base import Engine, RepairAction, Result


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

    checks = ("counts", "data")
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

    @staticmethod
    def match_tables(src_ids, dst_ids):
        """(pairs, src_only, dst_only, ambiguous) by unqualified name.

        A name that appears twice on one side - the same table in two schemas -
        is returned as ambiguous rather than resolved. Picking one would
        compare a table against a namesake and report the verdict as if it
        were about the one the operator meant.
        """
        def index(ids):
            out = {}
            for i in ids:
                out.setdefault(HeteroEngine._leaf(i), []).append(i)
            return out
        s, d = index(src_ids), index(dst_ids)
        ambiguous = sorted(
            [v[0] for v in s.values() if len(v) > 1]
            + [v[0] for v in d.values() if len(v) > 1])
        pairs, src_only, dst_only = [], [], []
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
        """What to hash on each side, and everything that is not hashable.

        Returns (src_cols, dst_cols, notes). The two column lists are the
        same names in the same order - **sorted by name**, not by the order
        each server happens to report, because the row text is positional and
        two servers agreeing on a set of columns says nothing about the order
        they list them in.
        """
        from .. import canon

        def declared(engine, side, table):
            got = {}
            for name, typ in engine.neutral_columns(side, db, table):
                got[name] = typ
            return got
        src_types = declared(self.src_engine, "src", src_table)
        dst_types = declared(self.dst_engine, "dst", dst_table)
        notes = []
        both = sorted(set(src_types) & set(dst_types))
        only_src = sorted(set(src_types) - set(dst_types))
        only_dst = sorted(set(dst_types) - set(src_types))
        if only_src:
            notes.append(f"columns only on the source, not compared:"
                         f" {', '.join(only_src)}")
        if only_dst:
            notes.append(f"columns only on the target, not compared:"
                         f" {', '.join(only_dst)}")
        src_cols, dst_cols = [], []
        for name in both:
            scls, swhy = canon.comparable(self.src_engine.CANON_ENGINE,
                                          src_types[name])
            dcls, dwhy = canon.comparable(self.dst_engine.CANON_ENGINE,
                                          dst_types[name])
            if not scls or not dcls:
                notes.append(f"{name}: {swhy or dwhy}")
                continue
            src_cols.append((name, scls))
            dst_cols.append((name, dcls))
        return src_cols, dst_cols, notes

    def _neutral_rows(self, db, table=None, stream=None):
        """One (scope, status, detail, src_rows, dst_rows) per table.

        The single computation behind both `check_counts` and `check_data`:
        the digest query returns the count alongside the checksum, so asking
        twice would be two answers taken at two different moments about the
        same question.
        """
        src_ids = self.src_engine.neutral_tables("src", db)
        dst_ids = self.dst_engine.neutral_tables("dst", db)
        pairs, src_only, dst_only, ambiguous = self.match_tables(src_ids,
                                                                 dst_ids)
        if table:
            pairs = [p for p in pairs if self._leaf(p[0]) == self._leaf(table)]
            src_only = [t for t in src_only
                        if self._leaf(t) == self._leaf(table)]
        rows = []
        for t in src_only:
            rows.append((f"{db}.{self._leaf(t)}", "diff",
                         f"{t} is on the source and not on the target",
                         None, None))
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
                a = self.src_engine.neutral_digest("src", db, src_t, src_cols)
                b = self.dst_engine.neutral_digest("dst", db, dst_t, dst_cols)
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
    DRILL_CAP = 20000

    def _row_text(self, columns, row):
        from .. import canon
        return tuple(canon.render_value(cls, value)
                     for (_, cls), value in zip(columns, row))

    def _drill(self, db, src_t, dst_t, src_cols, dst_cols):
        """Which rows differ, written to the estate's files, as a clause.

        The digests disagreeing says a table is wrong; this says which rows,
        which is what a repair needs and what an operator reads first. Both
        sides are asked for the same keys rather than walked in step: two
        engines do not agree on the order of a text key, so a merge over two
        ordered reads would invent missing and extra rows in equal numbers.

        A pair that cannot be lined up says why instead of writing an empty
        list, which `sync` would read as nothing to do.
        """
        leaf = self._leaf(src_t)
        names = [n for n, _ in src_cols]
        try:
            key = list(self.src_engine.neutral_key("src", db, src_t))
            dst_key = list(self.dst_engine.neutral_key("dst", db, dst_t))
        except Exception as e:
            return f"; rows not localised: {str(e).splitlines()[-1][:80]}"
        if not key or not dst_key:
            side = "source" if not key else "target"
            return (f"; rows not localised: the {side} table has no key, so"
                    " there is nothing to line the two sides up by")
        if sorted(key) != sorted(dst_key):
            return (f"; rows not localised: the two sides key on different"
                    f" columns ({', '.join(key)} vs {', '.join(dst_key)})")
        if any(k not in names for k in key):
            return ("; rows not localised: the key is not among the columns"
                    " these two engines can compare")

        missing, changed, extra = [], [], []
        capped = []
        at = [names.index(k) for k in key]

        def walk(engine, side, table, cols, other, other_side, other_table,
                 other_cols, on_absent, on_differs):
            seen = 0
            after = None
            while seen < self.DRILL_CAP:
                got, after = engine.neutral_read(side, db, table, cols, after,
                                                 min(1000,
                                                     self.DRILL_CAP - seen))
                if not got:
                    return False
                seen += len(got)
                raw = [tuple(row[i] for i in at) for row in got]
                theirs = other.neutral_rows_by_key(other_side, db, other_table,
                                                   other_cols, key, raw)
                for row in got:
                    text = self._key_of(cols, key, row)
                    if text not in theirs:
                        on_absent(text)
                    elif on_differs is not None and self._row_text(
                            other_cols, theirs[text]) != self._row_text(cols,
                                                                       row):
                        on_differs(text)
                if after is None:
                    return False
            return True

        try:
            if walk(self.src_engine, "src", src_t, src_cols, self.dst_engine,
                    "dst", dst_t, dst_cols, missing.append, changed.append):
                capped.append("source")
            if walk(self.dst_engine, "dst", dst_t, dst_cols, self.src_engine,
                    "src", src_t, src_cols, extra.append, None):
                capped.append("target")
        except Exception as e:
            return f"; rows not localised: {str(e).splitlines()[-1][:90]}"

        import json
        self._write_drill(
            db, leaf,
            missing=[json.dumps(list(k)) for k in missing],
            changed=[json.dumps(list(k)) for k in changed],
            extra=[json.dumps(list(k)) for k in extra])
        parts = []
        for label, found in (("missing on the target", missing),
                             ("with different values", changed),
                             ("only on the target", extra)):
            if found:
                shown = ", ".join("/".join(k) for k in found[:4])
                parts.append(f"{len(found)} {label} ({shown}"
                             + (" ..." if len(found) > 4 else "") + ")")
        clause = "; " + ("; ".join(parts) if parts else
                         "no row differs, so the difference is in a column"
                         " neither side could compare")
        if capped:
            clause += (f"; stopped after {self.DRILL_CAP:,} rows on the "
                       + " and ".join(capped) + ", so there may be more")
        return clause

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
            try:
                for row in engine.assess():
                    row = dict(row)
                    row["scope"] = f"{side} ({name}) {row.get('scope', '')}"
                    row["item"] = f"{side}: {row.get('item', '')}"
                    items.append(row)
            except Exception as e:
                add("warn", f"{side} ({name}) could not be assessed",
                    f"{str(e).splitlines()[-1][:110]} - unknown, not clean")

        items += self._pair_capabilities()
        return items

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
        if kind not in ("rows", "all"):
            return []
        return self._rows_plan(
            db, f"from {self.src_name} to {self.dst_name}")

    def apply(self, db, action):
        """Carry the rows across, then remove the ones that should not exist.

        The keys come back out of the drilldown as canonical text and are
        turned into values with `canon.from_text`, the same way a change
        record from a logical decoder is - the text is the one form both
        engines agree on, and re-deriving the value per side is what lets a
        key written by one engine address a row in the other.

        Writes first and deletions last, so a repair cut off in the middle
        leaves rows that should not be there - which the next check names -
        rather than a hole nothing looks for.
        """
        import json

        from .. import canon
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
            self.dst_engine.neutral_tables("dst", db))
        match = [(s, d) for s, d in pairs if self._leaf(s) == name]
        if not match:
            raise SystemExit(f"{name} is no longer on both sides, so the"
                             " rows the last check listed cannot be placed")
        src_t, dst_t = match[0]
        src_cols, dst_cols, _ = self._column_plan(src_t, dst_t, db)
        key = list(self.src_engine.neutral_key("src", db, src_t))
        cls = {n: c for n, c in src_cols}
        if not key or any(k not in cls for k in key):
            raise SystemExit(f"{name} has no key among the compared columns,"
                             " so a row cannot be addressed on both sides")

        def values(key_text):
            return tuple(canon.from_text(cls[k], t) for k, t in zip(key,
                                                                    key_text))
        undo_dir = self.hop.report_dir(db) / "undo"
        undo_dir.mkdir(parents=True, exist_ok=True)
        undo = undo_dir / f"{name}.rows.jsonl"

        def remember(handle, key_texts):
            if not key_texts:
                return
            held = self.dst_engine.neutral_rows_by_key(
                "dst", db, dst_t, dst_cols, key, [values(k) for k in
                                                  key_texts])
            for key_text, row in held.items():
                handle.write(json.dumps({
                    "table": name, "key": list(key_text),
                    "row": dict(zip([n for n, _ in dst_cols],
                                    self._row_text(dst_cols, row)))}) + "\n")

        send = found.get("missing", []) + found.get("changed", [])
        drop = found.get("extra", [])
        with undo.open("a") as handle:
            remember(handle, found.get("changed", []))
            remember(handle, drop)
        if send:
            rows = self.src_engine.neutral_rows_by_key(
                "src", db, src_t, src_cols, key, [values(k) for k in send])
            if rows:
                self.dst_engine.neutral_write("dst", db, dst_t, dst_cols,
                                              list(rows.values()))
        for key_text in drop:
            self.dst_engine._apply_delete(
                "dst", db, dst_t, dict(zip(key, values(key_text))))

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
        src_types = dict(self.src_engine.neutral_columns("src", db,
                                                         src_table))
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
            src_cols.append((name, canon.type_class(
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
            self.dst_engine.neutral_tables("dst", db))
        match = [d for s_, d in pairs if self._leaf(s_) == leaf]
        if match:
            dst_t = match[0]
        elif self.dst_engine.CREATES_ON_WRITE:
            dst_t = leaf
            log(f"{leaf}: not on the target yet;"
                f" {self.dst_name} creates it on the first write")
        else:
            dst_t = self._create_target(db, src_t, leaf, log)
        src_cols, dst_cols, notes = self._move_columns(src_t, dst_t, db)
        for n in notes:
            log(f"{leaf}: {n}")
        key = f"{db}.{leaf}"
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        after = tuple(st["last"]) if st.get("last") is not None else None
        moved = int(st.get("moved", 0))
        absent = 0
        from ..throttle import Throttle
        # the read side is the one under load, so that is what the gate asks
        gate = Throttle(1, probe=lambda: self._health("src"))
        while True:
            with gate.unit():
                rows, last = self.src_engine.neutral_read(
                    "src", db, src_t, src_cols, after, chunk)
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
        declared = self.src_engine.neutral_columns("src", db, src_table)
        columns, unknown = [], []
        for name, typ in sorted(declared):
            cls = canon.type_class(self.src_engine.CANON_ENGINE, typ)
            if cls is None:
                unknown.append(f"{name} ({typ})")
                continue
            columns.append((name, cls, canon.params(typ)))
        if unknown:
            raise SystemExit(
                f"{leaf} is not on the target and migkit cannot build it:"
                f" no neutral class for {', '.join(unknown)}."
                " Create the table yourself and run this again - guessing a"
                " column type is how a migration arrives complete and wrong")
        try:
            key = [k for k in self.src_engine.neutral_key("src", db,
                                                          src_table)
                   if k in {n for n, _, _ in columns}]
        except Exception:
            key = []
        return columns, key

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
        ddl = self.dst_engine.neutral_create("dst", db, leaf, columns, key)
        log(f"{leaf}: not on the target, created it - {ddl}")
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
        res = [Result("data", scope, status, detail)
               for scope, status, detail, _, _ in rows]
        if with_counts:
            res = self._counts_rows(db, rows) + res
        return res

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
            plan = []
            if which("pgloader"):
                plan.append(f"pgloader mysql://user@{self.hop.source.host}"
                            f"/{db} postgresql://user@"
                            f"{self.hop.target.host}/{db}"
                            "  # schema+data+indexes in one shot")
            plan.append(f"migkit convert-schema {hop} --db {db}"
                        "   # sqlglot DDL conversion, review then --apply")
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

    def list_move_tables(self, db):
        if not (self.my and self.pg):
            if not self._can_move_neutrally():
                self._mysql_to_postgres_only("listing tables to move")
            pairs, _, _, _ = self.match_tables(
                self.src_engine.neutral_tables("src", db),
                self.dst_engine.neutral_tables("dst", db))
            out = []
            for src_t, _ in pairs:
                sch, _, tbl = src_t.rpartition(".")
                out.append((sch, tbl))
            return out
        return [("", t) for t in self.my._tables("src", db)]

    def move_table(self, db, sch, tbl, chunk, ck, log):
        if not (self.my and self.pg):
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
        key = f"{db}.{t}"
        st = ck.setdefault(key, {})
        if st.get("done"):
            log(f"{key}: done earlier, skip")
            return
        cols = self.my._cols(db, t)
        collist_my = ", ".join(f"`{c}`" for c in cols)
        collist_pg = ", ".join(f'"{c}"' for c in cols)
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
            env = tool_env({"PGPASSWORD": tgt.password})
            pre = f'delete from "{t}" where {pred_pg};' if pred_pg \
                else f'truncate "{t}";'
            p = subprocess.run(
                ["psql", "-h", tgt.host, "-p", str(tgt.port),
                 "-U", tgt.user, "-d", db, "-X", "-q",
                 "-v", "ON_ERROR_STOP=1", "-1", "-c", pre,
                 "-c", f"\\copy \"{t}\" ({collist_pg}) from stdin"
                       " (format csv, null '')"],
                input=buf.getvalue(), capture_output=True, text=True, env=env)
            if p.returncode:
                raise RuntimeError(p.stderr[-300:])

        if not intpk:
            log(f"{key}: no single int pk, single-shot copy")
            rows = self.my._q("src", f"select {collist_my} from `{db}`.`{t}`")
            push(rows, "")
            st["done"] = True
            ck.save()
            return
        mm = self.my._q("src", f"select coalesce(min(`{intpk}`), 0),"
                        f" coalesce(max(`{intpk}`), 0) from `{db}`.`{t}`")[0]
        lo, hi = int(mm[0]), int(mm[1])
        last = st.get("last", lo - 1)
        while last < hi:
            nxt = min(last + chunk, hi)
            rows = self.my._q("src",
                              f"select {collist_my} from `{db}`.`{t}`"
                              f" where `{intpk}` > %s and `{intpk}` <= %s",
                              (last, nxt))
            push(rows, f'"{intpk}" > {last} and "{intpk}" <= {nxt}')
            last = nxt
            st["last"] = last
            ck.save()
            log(f"{key}: up to {intpk}={last:,} of {hi:,}")
        st["done"] = True
        ck.save()

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
        from .base import Engine
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

        token = None
        if token_path.exists():
            try:
                token = _json.loads(token_path.read_text()).get("token")
            except Exception:
                token = None
        log(f"tailing {self.src_name} -> {self.dst_name}, ctrl-c to stop"
            + ("" if go else " (count-only, add --go to apply)")
            + (f", resuming from {str(token)[:40]}" if token else ""))
        seen = 0
        try:
            while True:
                changes, token = self.src_engine.neutral_changes(
                    "src", db, token, limit=1000)
                if changes:
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
                    log(f"{seen} changes"
                        + ("" if go else " seen (nothing applied)"))
                else:
                    _time.sleep(1)
        except KeyboardInterrupt:
            log(f"stopped after {seen} changes; rerun to resume")

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
            a = sum(self.my._q("src",
                               f"select count(*) from `{db}`.`{t}`")[0][0]
                    for t in self.my._tables("src", db))
            try:
                b = sum(int(self.pg._psql("dst", db,
                                          f'select count(*) from "{t}"'))
                        for t in self.my._tables("src", db))
            except RuntimeError:
                b = 0
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
