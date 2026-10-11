"""Oracle's deep checks of a finished move (backlog P0-PARITY.verify): the
things a copy of the rows leaves looking right while they are not - a key
that is missing, a column narrower than the source's, an index, trigger,
policy or grant that did not arrive, a value that came out shifted.

Each is one question put to both sides' catalogue (`all_*`), scoped by the
hop (`Engine.in_scope`), and answered as a finding named by its last word
(`base.CATEGORIES`). A check that cannot run says so, as an error of its
own - never as a pass (`_guarded`) - and one that read only part of what it
should says how much it left (`_cap`).

None of the statements here has run against a server yet; each is listed
in `capabilities.UNPROVED` until a sandbox run proves it.
"""
import re

from .base import Result
from .dbapi import MARK

#: how many columns a check that reads their values reads, at most
LOOK = 30
#: the data types whose values are text, approximate numbers, instants, and
#: the ones whose text depends on how they are kept - as a condition on the
#: catalogue's `data_type`, which spells a precision into the name
TEXT = "data_type in ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR')"
FLOATS = "data_type in ('BINARY_FLOAT', 'BINARY_DOUBLE', 'FLOAT')"
INSTANTS = "data_type = 'DATE' or data_type like 'TIMESTAMP%'"
RENDERED = ("data_type like 'INTERVAL%' or data_type like 'TIMESTAMP%WITH"
            " TIME ZONE'")
#: a character set that holds every character, and a name that is not one
UNICODE = ("AL32UTF8", "UTF8", "AL16UTF16", "UTFE")


def _first(error):
    return (str(error).strip().splitlines() or [""])[-1][:120]


def _portable(declared):
    """A declared Oracle type as the shared reasoning on capacity reads it
    (`verdict.narrowing`): `varchar(n)`, `char(n)`, `numeric(p,s)`; None for
    a type with no limit to compare."""
    m = re.fullmatch(r"(VARCHAR2|NVARCHAR2)\((\d+)\)", declared or "")
    if m:
        return f"varchar({m.group(2)})"
    m = re.fullmatch(r"(CHAR|NCHAR)\((\d+)\)", declared or "")
    if m:
        return f"char({m.group(2)})"
    m = re.fullmatch(r"NUMBER\((\d+),(\d+)\)", declared or "")
    if m:
        return f"numeric({m.group(1)},{m.group(2)})"
    return "numeric" if declared == "NUMBER" else None


def _same_text(a, b):
    """Two expressions the catalogue keeps as written, held equal apart
    from spacing and case outside quotes."""
    return re.sub(r"\s+", "", str(a or "")).lower() \
        == re.sub(r"\s+", "", str(b or "")).lower()


class OracleDeep:
    """The deep checks of the Oracle engine; mixed into `OracleEngine`."""

    # ---- running them ------------------------------------------------------

    def _guarded(self, db, word, fn, also=()):
        """What `fn(db)` answers - a finding or a list of them - or, when
        it could not read what it needed, an error for `word` and each of
        `also` (the other findings it makes): a check that failed is not
        one that found nothing."""
        try:
            return fn(db)
        except Exception as e:  # noqa: BLE001 - said, and the rest run
            return [Result("deep", f"{db} {w}", "error",
                           f"could not run the {w} check: {_first(e)}")
                    for w in (word, *also)]

    def _deep_more(self, db):
        """Every deep check beyond the target's objects and constraints,
        each guarded: one that fails leaves the others to run."""
        out = []
        for word, fn, also in (
                ("keys", self._deep_keys, ()),
                ("fk", self._deep_fk, ()),
                ("deferrable", self._deep_deferrable, ()),
                ("columns", self._deep_columns, ("narrowing", "generated")),
                ("collation", self._deep_collation, ()),
                ("charset", self._deep_charset, ()),
                ("nullempty", self._deep_nullempty, ()),
                ("float", self._deep_float, ()),
                ("timeshift", self._deep_timeshift, ()),
                ("render", self._deep_render, ()),
                ("boundary", self._deep_boundary, ()),
                ("triggers", self._deep_triggers, ()),
                ("indexes", self._deep_indexes, ()),
                ("matviews", self._deep_matviews, ()),
                ("rls", self._deep_rls, ()),
                ("grants", self._deep_grants, ()),
                ("seq-grants", self._deep_seq_grants, ()),
                ("ownership", self._deep_ownership, ()),
                ("options", self._deep_options, ())):
            got = self._guarded(db, word, fn, also)
            out += got if isinstance(got, list) else [got]
        return out

    def _cat(self, side, db, sql, more=()):
        """The statement's rows for the side's schema."""
        return self._rows(side, db, sql, (self._owner(side, db),) + tuple(
            more))

    def _taken(self, db, rows, at=0):
        """The rows whose table (the value at `at`) the hop takes."""
        return [r for r in rows if self.in_scope(db, self._said(str(r[at])))]

    def _cap(self, found, kept):
        """The words that say a check read `kept` of `found` columns only,
        or "" where it read them all."""
        return "" if found <= kept else (
            f"; read {kept} of {found}, the rest were not compared")

    # ---- keys --------------------------------------------------------------

    def _deep_keys(self, db):
        """Tables of the source no key names a row of: the change feed
        cannot say which row a change was of, and a row cannot be named
        when the two sides differ."""
        nopk = self._keyless("src", db)
        if nopk:
            return Result("deep", f"{db} keys", "diff",
                          f"{len(nopk)} tables have no pk/unique (their"
                          " updates and deletes cannot be followed, and"
                          " their rows cannot be verified by key): "
                          + ", ".join(nopk[:5]), "",
                          "add a primary key or unique index before"
                          " migrating")
        return Result("deep", f"{db} keys", "ok",
                      "every table on the source has a pk or unique index")

    # ---- foreign keys ------------------------------------------------------

    def _deep_fk(self, db):
        """Rows on the target whose foreign key points at nothing: a load
        that ran with the constraints off never looked. A child pointing
        into a table the hop does not take is said, not charged to it."""
        owner = self._owner("dst", db)
        orphans, outside, scanned = [], [], 0
        for child, cols, parent, pcols in sorted(
                self.foreign_keys("dst", db), key=lambda f: (f[0], f[2])):
            if not self.in_scope(db, child):
                continue
            scanned += 1
            nn = " and ".join(f"c.{self._q(c)} is not null" for c in cols)
            join = " and ".join(f"p.{self._q(r)} = c.{self._q(c)}"
                                for c, r in zip(cols, pcols))
            n = self._rows(
                "dst", db, f"select count(*) from {self._q(owner)}."
                           f"{self._q(child)} c where {nn} and not exists"
                           f" (select 1 from {self._q(owner)}."
                           f"{self._q(parent)} p where {join})")[0][0]
            if not n:
                continue
            if self.in_scope(db, parent):
                orphans.append(f"{child}({', '.join(cols)}): {int(n)}"
                               " orphan rows")
            else:
                outside.append(f"{child}({', '.join(cols)}): {int(n)} rows"
                               f" point at {parent}, which this hop does"
                               " not move")
        if orphans:
            return Result("deep", f"{db} fk", "diff",
                          "; ".join(orphans[:5])
                          + (f"; {len(outside)} into tables the hop does"
                             " not move: " + "; ".join(outside[:3])
                             if outside else ""), "",
                          "reload the child rows or delete the orphans")
        if outside:
            return Result("deep", f"{db} fk", "warn", "; ".join(outside[:5]),
                          "", "the parent is not moved by this hop: take it"
                              " too, or the rows keep pointing at nothing")
        return Result("deep", f"{db} fk", "ok",
                      f"{scanned} foreign keys scanned, 0 orphan rows")

    def _deep_deferrable(self, db):
        """A key, unique or foreign, that the source lets a transaction
        check at its end and the target checks at each statement (or the
        reverse): the application's order of writes meets the difference
        first on the target."""
        sql = ("select c.table_name, c.constraint_type, c.deferrable,"
               " c.deferred, (select listagg(cc.column_name, ',') within"
               " group (order by cc.position) from all_cons_columns cc"
               " where cc.owner = c.owner and cc.constraint_name ="
               " c.constraint_name) from all_constraints c where c.owner ="
               f" {MARK} and c.constraint_type in ('P', 'U', 'R') order by"
               " 1, 2")

        def read(side):
            return {(self._said(str(t)), str(k), str(cols)):
                    (str(d), str(i)) for t, k, d, i, cols in self._taken(
                        db, self._cat(side, db, sql))}
        src, dst = read("src"), read("dst")
        kinds = {"P": "key", "U": "unique", "R": "foreign key"}
        bad = [f"{t} {kinds[k]} ({cols}): {self._defer(src[key])} on the"
               f" source, {self._defer(dst[key])} on the target"
               for key in sorted(src) if key in dst and src[key] != dst[key]
               for t, k, cols in [key]]
        if bad:
            return Result("deep", f"{db} deferrable", "diff",
                          f"{len(bad)} constraints differ in when they are"
                          " checked: " + "; ".join(bad[:5]), "",
                          "alter table ... modify constraint ... deferrable"
                          " initially deferred, as the source has it")
        return Result("deep", f"{db} deferrable", "ok",
                      f"{len(src)} constraints are checked when the source"
                      " checks them")

    @staticmethod
    def _defer(state):
        deferrable, deferred = state
        if deferrable != "DEFERRABLE":
            return "checked at each statement"
        return ("deferrable, deferred" if deferred == "DEFERRED"
                else "deferrable, immediate")

    # ---- columns -----------------------------------------------------------

    def _of_tables(self):
        """The condition that keeps a column of `c` to a table of the
        schema - the catalogue lists a view's too - and out of migkit's
        own (`OWN_PREFIX`)."""
        return (" and exists (select 1 from all_tables t where t.owner ="
                " c.owner and t.table_name = c.table_name) and"
                f" c.table_name not like '{self.OWN_PREFIX}%' escape '\\'")

    def _column_facts(self, side, db):
        """{(table, column): (declared type, nullable, default, virtual)}
        of the schema's columns the hop takes."""
        sql = ("select c.table_name, c.column_name, "
               + self._declared() + ", c.nullable, c.data_default,"
               " c.virtual_column from all_tab_cols c where c.owner = "
               + MARK + " and c.hidden_column = 'NO'" + self._of_tables()
               + " order by c.table_name, c.column_id")
        return {(self._said(str(t)), self._said(str(c))):
                (str(d), str(n), " ".join(str(df or "").split()),
                 str(v) == "YES")
                for t, c, d, n, df, v in self._taken(
                    db, self._cat(side, db, sql))}

    @staticmethod
    def _declared():
        from .oracle import DECLARED
        return DECLARED

    def _deep_columns(self, db):
        """Columns that came out different: type, nullability, default; a
        column narrower than the source's, which cuts or overflows a value
        the source holds; one computed on a side and plain on the other."""
        src, dst = self._column_facts("src", db), \
            self._column_facts("dst", db)
        drift, narrow, gen = [], [], []
        from .. import verdict
        for key in sorted(src):
            if key not in dst:
                continue
            (sd, sn, sdef, sv), (dd, dn, ddef, dv) = src[key], dst[key]
            name = ".".join(key)
            if sv or dv:
                if sv and not dv:
                    gen.append(f"{name}: computed on the source, plain on"
                               " the target")
                elif dv and not sv:
                    gen.append(f"{name}: computed on the target, plain on"
                               " the source")
                elif not _same_text(sdef, ddef):
                    gen.append(f"{name}: the expression differs")
                continue
            if (sd, sn, sdef) != (dd, dn, ddef):
                drift.append(f"{name}: src={sd}/{'null' if sn == 'Y' else 'not null'}"
                             f"/{sdef or 'no default'} dst={dd}/"
                             f"{'null' if dn == 'Y' else 'not null'}"
                             f"/{ddef or 'no default'}")
            a, b = _portable(sd), _portable(dd)
            if a and b:
                why = verdict.narrowing("postgres", a, b)
                if why:
                    narrow.append(f"{name}: {why}")
        out = []
        if drift:
            path = self.hop.report_dir(db) / "deep-columns.diff"
            path.write_text("\n".join(drift) + "\n")
            out.append(Result("deep", f"{db} columns", "diff",
                              f"{len(drift)} columns drift (type/null/"
                              "default): " + "; ".join(
                                  d.split(":")[0] for d in drift[:4]),
                              str(path), "align the target's DDL"))
        else:
            out.append(Result("deep", f"{db} columns", "ok",
                              f"{len(src)} columns compared, type/null/"
                              "default identical"))
        out.append(verdict.narrowing_result(db, narrow))
        out.append(Result("deep", f"{db} generated",
                          "diff" if gen else "ok",
                          "; ".join(gen[:6]) if gen else
                          "no computed column differs", "",
                          "make the column computed as the source has it"
                          if gen else ""))
        return out

    def _deep_collation(self, db):
        """A column whose collation is not the source's. A key column on a
        case- or accent-insensitive collation folds distinct source values
        into one on load. A server before 12.2 keeps no column collations."""
        sql = ("select table_name, column_name, collation from all_tab_cols"
               f" where owner = {MARK} and hidden_column = 'NO' and"
               " collation is not null order by 1, 2")
        keys = ("select cc.table_name, cc.column_name from all_constraints c"
                " join all_cons_columns cc on cc.owner = c.owner and"
                " cc.constraint_name = c.constraint_name where c.owner ="
                f" {MARK} and c.constraint_type in ('P', 'U')")
        try:
            src = {(self._said(str(t)), self._said(str(c))): str(k)
                   for t, c, k in self._taken(db, self._cat("src", db, sql))}
            dst = {(self._said(str(t)), self._said(str(c))): str(k)
                   for t, c, k in self._taken(db, self._cat("dst", db, sql))}
        except Exception as e:  # noqa: BLE001 - ORA-00904 before 12.2
            if "ORA-00904" in str(e):
                return Result("deep", f"{db} collation", "skip",
                              "this server keeps no column collations (they"
                              " came with 12.2): none to compare")
            raise
        held = {(self._said(str(t)), self._said(str(c))) for t, c in
                self._cat("src", db, keys)}
        bad = [f"{'.'.join(k)}: {src[k]} on the source, {dst.get(k)} on the"
               f" target" + (" - in a key, where distinct values can fold"
                             " into one" if k in held else "")
               for k in sorted(src) if k in dst and src[k] != dst[k]]
        if bad:
            return Result("deep", f"{db} collation", "diff",
                          f"{len(bad)} columns compare text another way: "
                          + "; ".join(bad[:5]), "",
                          "make the column's collation the source's, or"
                          " settle the duplicates before the load")
        return Result("deep", f"{db} collation", "ok",
                      f"{len(src)} column collations match")

    # ---- values ------------------------------------------------------------

    def _deep_charset(self, db):
        """The character sets the two databases store text in. Text the
        target's cannot hold is cut or replaced on the way in, and what
        arrived looks present."""
        src, dst = self._settings("src", db), self._settings("dst", db)
        bad = []
        for key, name in (("nls_characterset", "database character set"),
                          ("nls_nchar_characterset",
                           "national character set")):
            a, b = str(src.get(key, "")).upper(), str(dst.get(key, "")).upper()
            if not a or not b or a == b:
                continue
            note = (" (DOWNGRADE from a set that holds every character:"
                    " characters it cannot map are replaced)"
                    if a in UNICODE and b not in UNICODE else "")
            bad.append(f"{name} {a} on the source, {b} on the target{note}")
        if bad:
            return Result("deep", f"{db} charset", "diff", "; ".join(bad),
                          "", "make the target database in the source's"
                              " character set; the set cannot be changed"
                              " under data")
        return Result("deep", f"{db} charset", "ok",
                      "the database and national character sets match")

    def _cols_where(self, db, cond):
        """[(table, column, data type)] of the source's columns of the
        tables the hop takes whose data type satisfies `cond`."""
        return [(self._said(str(t)), self._said(str(c)), str(k)) for t, c, k
                in self._taken(db, self._cat(
                    "src", db, "select c.table_name, c.column_name,"
                               " c.data_type from all_tab_columns c where"
                               f" c.owner = {MARK} and ({cond})"
                               + self._of_tables() + " order by 1, 2"))]

    def _each_column(self, db, cond, read):
        """(found, kept, bad): `read(table, column)` -> a line naming what
        differs between the sides, or None, over the first `LOOK` columns
        satisfying `cond`; a column whose read fails is counted in `found`
        and not in `kept`."""
        cols = self._cols_where(db, cond)
        bad, kept = [], 0
        for t, c, _ in cols[:LOOK]:
            try:
                line = read(t, c)
            except Exception:  # noqa: BLE001 - this column is not read
                continue
            kept += 1
            if line:
                bad.append(line)
        return len(cols), kept, bad

    def _deep_nullempty(self, db):
        """An empty string is NULL in Oracle, so a text column whose rows
        came across with fewer values than the source had held something
        that became nothing. Counted where both sides hold the same rows."""
        def read(t, c):
            q = (f"select count(*), count({self._q(c)}) from "
                 f"{self._q(self._owner('src', db))}.{self._q(t)}")
            (s,), (d,) = self._beside(db, q)
            if int(s[0]) != int(d[0]):
                return None
            if int(s[1]) != int(d[1]):
                return (f"{t}.{c}: {int(s[1])} values on the source,"
                        f" {int(d[1])} on the target")
        found, kept, bad = self._each_column(db, TEXT, read)
        if bad:
            return Result("deep", f"{db} nullempty", "diff",
                          f"{len(bad)} text columns lost values to NULL: "
                          + "; ".join(bad[:5]), "",
                          "an empty string is NULL here; decide which value"
                          " the application means before it is read")
        gap = self._cap(found, kept)
        return Result("deep", f"{db} nullempty", "warn" if gap else "ok",
                      f"{kept} text columns keep their values{gap}"
                      if found else "no text columns")

    def _deep_float(self, db):
        """Approximate numbers summed with a tolerance, since a digest of
        them is the wrong instrument: the same value can render two ways,
        and a column made less precise changes the sum."""
        def read(t, c):
            q = (f"select count({self._q(c)}), nvl(sum({self._q(c)}), 0),"
                 f" nvl(max(abs({self._q(c)})), 0) from "
                 f"{self._q(self._owner('src', db))}.{self._q(t)}")
            (s,), (d,) = self._beside(db, q)
            if int(s[0]) != int(d[0]):
                return f"{t}.{c}: non-null count src={s[0]} dst={d[0]}"
            ss, ds = float(s[1]), float(d[1])
            scale = max(abs(ss), abs(ds), float(s[2]), float(d[2]), 1.0)
            if ss != ds and not (ss != ss and ds != ds) \
                    and abs(ss - ds) > scale * 1e-9:
                return f"{t}.{c}: sum src={ss!r} dst={ds!r}"
        found, kept, bad = self._each_column(db, FLOATS, read)
        if bad:
            return Result("deep", f"{db} float", "diff",
                          f"{len(bad)} approximate-number columns drift"
                          " beyond tolerance: " + "; ".join(bad[:5]), "",
                          "a copy that changed the column's precision does"
                          " this: compare the column definitions first")
        gap = self._cap(found, kept)
        return Result("deep", f"{db} float", "warn" if gap else "ok",
                      f"{kept} approximate-number columns within tolerance"
                      f"{gap}" if found else "no approximate-number columns")

    def _single_key(self, side, db, numeric=False):
        """{table: key column} of the schema's tables whose primary key is
        one column - a number, where `numeric`."""
        kind = " and tc.data_type = 'NUMBER'" if numeric else ""
        return {self._said(str(t)): self._said(str(c)) for t, c in
                self._taken(db, self._cat(
                    side, db, "select c.table_name, cc.column_name from"
                              " all_constraints c join all_cons_columns cc"
                              " on cc.owner = c.owner and cc.constraint_name"
                              " = c.constraint_name join all_tab_columns tc"
                              " on tc.owner = c.owner and tc.table_name ="
                              " c.table_name and tc.column_name ="
                              " cc.column_name where c.owner = " + MARK
                              + f" and c.constraint_type = 'P'{kind} and 1 ="
                                " (select count(*) from all_cons_columns x"
                                " where x.owner = c.owner and"
                                " x.constraint_name = c.constraint_name)"
                              " order by 1"))}

    def _beside(self, db, sql):
        """`sql` (written for the source's schema) read on both sides."""
        src_owner, dst_owner = self._owner("src", db), self._owner("dst", db)
        return (self._rows("src", db, sql),
                self._rows("dst", db, sql.replace(
                    f'"{src_owner}".', f'"{dst_owner}".')))

    def _deep_timeshift(self, db):
        """A timestamp column every row of which came out the same number
        of seconds away: a clock applied to the whole column, not rows
        damaged one by one. Read as the instant on both sides."""
        from .. import verdict
        keys = self._single_key("src", db)
        cols = self._cols_where(db, INSTANTS)
        shifts = []
        for t, c, kind in cols[:10]:
            if t not in keys:
                continue
            at = self._q(c)
            epoch = (f"(cast(sys_extract_utc({at}) as date) - date"
                     " '1970-01-01') * 86400" if "TIME ZONE" in kind else
                     f"(cast({at} as date) - date '1970-01-01') * 86400")
            q = (f"select {self._q(keys[t])}, {epoch} from "
                 f"{self._q(self._owner('src', db))}.{self._q(t)} where "
                 f"{at} is not null order by {self._q(keys[t])} fetch first"
                 " 200 rows only")
            try:
                a, b = self._beside(db, q)
            except Exception:  # noqa: BLE001 - this column is not read
                continue
            line = verdict.uniform_shift(
                f"{t}.{c}", {str(k): v for k, v in a},
                {str(k): v for k, v in b})
            if line:
                shifts.append(line)
        return verdict.timeshift_result(db, shifts)

    def _deep_render(self, db):
        """Values whose text depends on how they are kept - an interval, a
        time with its zone's name - read as text from both sides for a few
        rows by key: a copy that kept the instant and lost the zone's
        region, or an interval's fields, differs here and nowhere else."""
        keys = self._single_key("src", db)
        drifts, audited = [], 0
        for t, c, kind in [r for r in self._cols_where(db, RENDERED)
                           if r[0] in keys][:20]:
            at, key = self._q(c), self._q(keys[t])
            show = (f"to_char({at}, 'YYYY-MM-DD HH24:MI:SS.FF9 TZR')"
                    if "TIME ZONE" in kind else f"to_char({at})")
            q = (f"select to_char({key}), {show} from "
                 f"{self._q(self._owner('src', db))}.{self._q(t)} where "
                 f"{at} is not null order by {key} fetch first 5 rows only")
            try:
                a = dict(self._rows("src", db, q))
                if not a:
                    continue
                marks = ", ".join("'" + k.replace("'", "''") + "'"
                                  for k in a)
                qd = (f"select to_char({key}), {show} from "
                      f"{self._q(self._owner('dst', db))}.{self._q(t)} where"
                      f" to_char({key}) in ({marks})")
                b = dict(self._rows("dst", db, qd))
            except Exception:  # noqa: BLE001 - this column is not read
                continue
            audited += 1
            for k, v in a.items():
                if k in b and b[k] != v:
                    drifts.append(f"{t}.{c}: renders differently (src"
                                  f" {v[:40]!r} dst {b[k][:40]!r})")
                    break
        if drifts:
            return Result("deep", f"{db} render", "diff",
                          "; ".join(drifts[:5]), "",
                          "check the column's type and the zone it keeps")
        return Result("deep", f"{db} render", "ok",
                      f"{audited} intervals and zoned times sampled,"
                      " rendering identical" if audited
                      else "no interval or zoned-time columns")

    def _deep_boundary(self, db):
        """A target whose counter is past the source's: something wrote
        there, or a change was applied twice. Compared on tables with a
        single numeric key."""
        keys = self._single_key("src", db, numeric=True)
        ahead, behind, n = [], [], 0
        for t, c in sorted(keys.items()):
            q = (f"select nvl(max({self._q(c)}), 0) from "
                 f"{self._q(self._owner('src', db))}.{self._q(t)}")
            try:
                a, b = self._beside(db, q)
                a, b = a[0][0], b[0][0]
            except Exception:  # noqa: BLE001 - this table is not read
                continue
            n += 1
            if b > a:
                ahead.append(f"{t} src_max={a} dst_max={b}")
            elif b < a:
                behind.append(t)
        if ahead:
            return Result("deep", f"{db} boundary", "diff",
                          f"target max(pk) AHEAD of source on {len(ahead)}:"
                          f" {'; '.join(ahead[:4])}", "",
                          "writes landing on the target, or a change applied"
                          " twice: find the writer before cutover")
        note = f"; {len(behind)} behind (the tail has not caught up)" \
            if behind else ""
        return Result("deep", f"{db} boundary", "ok",
                      f"max(pk) checked on {n} tables, none ahead of the"
                      f" source{note}")

    # ---- what hangs off a table --------------------------------------------

    def _deep_triggers(self, db):
        """Triggers the source has that the target lacks, and triggers the
        target holds disabled (a load turns them off and a stopped one
        leaves them so)."""
        sql = ("select trigger_name, nvl(table_name, trigger_name), status"
               f" from all_triggers where owner = {MARK} order by 1")
        src = {self._said(str(n)): str(s) for n, t, s in self._taken(
            db, self._cat("src", db, sql), 1)}
        dst = {self._said(str(n)): str(s) for n, t, s in self._taken(
            db, self._cat("dst", db, sql), 1)}
        missing = sorted(set(src) - set(dst))
        off = sorted(n for n, s in dst.items()
                     if s != "ENABLED" and src.get(n, s) == "ENABLED")
        if missing or off:
            parts = []
            if missing:
                parts.append(f"{len(missing)} missing on the target: "
                             + ", ".join(missing[:5]))
            if off:
                parts.append(f"{len(off)} disabled on the target: "
                             + ", ".join(off[:5]))
            return Result("deep", f"{db} triggers", "diff", "; ".join(parts),
                          "", "alter trigger ... enable, or make the ones"
                              " that are missing")
        return Result("deep", f"{db} triggers", "ok",
                      f"{len(src)} triggers, all there and enabled")

    def _index_sets(self, side, db):
        """({(table, unique, columns): [index names]}, [unusable]) of the
        schema's indexes the hop takes; an expression is its text."""
        rows = self._taken(db, self._cat(
            side, db, "select i.table_name, i.index_name, i.uniqueness,"
                      " i.status, c.column_position, c.column_name from"
                      " all_indexes i join all_ind_columns c on"
                      " c.index_owner = i.owner and c.index_name ="
                      " i.index_name where i.owner = " + MARK
                      + " and i.index_type not in ('LOB', 'IOT - TOP') order"
                        " by 1, 2, 5"))
        exprs = {(str(n), int(p)): " ".join(str(e or "").split())
                 for n, p, e in self._cat(
                     side, db, "select index_name, column_position,"
                               " column_expression from all_ind_expressions"
                               " where index_owner = " + MARK)}
        cols = {}
        for t, n, u, st, p, c in rows:
            one = cols.setdefault((self._said(str(t)), str(n), str(u),
                                   str(st)), [])
            one.append(exprs.get((str(n), int(p)), self._said(str(c))))
        sets, unusable = {}, []
        for (t, n, u, st), c in cols.items():
            sets.setdefault((t, u == "UNIQUE", tuple(c)), []).append(n)
            if st == "UNUSABLE":
                unusable.append(f"{t}.{self._said(n)}")
        return sets, sorted(unusable)

    def _deep_indexes(self, db):
        """Indexes the source has that the target does not (by what they
        cover, since a generated name differs), and indexes the target
        holds unusable: Oracle answers a query under one without a word,
        by reading the table."""
        src, _ = self._index_sets("src", db)
        dst, unusable = self._index_sets("dst", db)
        missing = [f"{t} {'unique ' if u else ''}({', '.join(c)})"
                   for (t, u, c) in sorted(src) if (t, u, c) not in dst]
        if missing or unusable:
            parts = []
            if missing:
                parts.append(f"{len(missing)} missing on the target: "
                             + "; ".join(missing[:4]))
            if unusable:
                parts.append(f"{len(unusable)} unusable on the target: "
                             + ", ".join(unusable[:5]))
            return Result("deep", f"{db} indexes", "diff", "; ".join(parts),
                          "", "create the index, or alter index ... rebuild")
        return Result("deep", f"{db} indexes", "ok",
                      f"{len(src)} index definitions are on the target and"
                      " usable")

    def _deep_matviews(self, db):
        """Materialized views the source has that the target lacks, and
        ones the target holds stale, unusable or never refreshed: the
        application reads old rows from them without an error."""
        sql = ("select mview_name, staleness, compile_state,"
               " last_refresh_date from all_mviews where owner = " + MARK
               + " order by 1")
        src = {self._said(str(n)) for n, *_ in self._taken(
            db, self._cat("src", db, sql))}
        dst = {self._said(str(n)): (str(s), str(c), r) for n, s, c, r
               in self._taken(db, self._cat("dst", db, sql))}
        missing = sorted(src - set(dst))
        bad = sorted(f"{n} ({s.lower()}{', never refreshed' if r is None else ''})"
                     for n, (s, c, r) in dst.items()
                     if s in ("STALE", "UNUSABLE", "UNKNOWN", "COMPILATION_ERROR")
                     or c != "VALID" or r is None)
        if missing or bad:
            parts = []
            if missing:
                parts.append(f"{len(missing)} missing on the target: "
                             + ", ".join(missing[:5]))
            if bad:
                parts.append(f"{len(bad)} not fresh on the target: "
                             + ", ".join(bad[:5]))
            return Result("deep", f"{db} matviews", "diff", "; ".join(parts),
                          "", "make the view, or dbms_mview.refresh it")
        return Result("deep", f"{db} matviews", "ok",
                      f"{len(src)} materialized views, all there and fresh"
                      if src else "no materialized views")

    def _deep_rls(self, db):
        """Row-level security policies (virtual private database) the
        source enforces and the target does not: the target answers every
        user every row."""
        sql = ("select object_name, policy_name, enable from all_policies"
               " where object_owner = " + MARK + " order by 1, 2")
        src = {(self._said(str(o)), str(p)): str(e) for o, p, e in
               self._taken(db, self._cat("src", db, sql))}
        dst = {(self._said(str(o)), str(p)): str(e) for o, p, e in
               self._taken(db, self._cat("dst", db, sql))}
        missing = [f"{o}: {p}" for o, p in sorted(src) if (o, p) not in dst]
        off = [f"{o}: {p}" for (o, p), e in sorted(dst.items())
               if e != "YES" and src.get((o, p), e) == "YES"]
        if missing or off:
            parts = []
            if missing:
                parts.append(f"{len(missing)} policies missing on the"
                             " target (its rows are visible to every"
                             " user): " + "; ".join(missing[:4]))
            if off:
                parts.append(f"{len(off)} policies disabled on the target: "
                             + "; ".join(off[:4]))
            return Result("deep", f"{db} rls", "diff", "; ".join(parts), "",
                          "dbms_rls.add_policy, as the source has it")
        return Result("deep", f"{db} rls", "ok",
                      f"{len(src)} row-level policies on the target"
                      if src else "no row-level policies")

    # ---- access ------------------------------------------------------------

    def _grants(self, side, db, kinds):
        """{(grantee, object, privilege, grantable)} on the schema's
        objects of `kinds`, as the account sees them - every grant on its
        own objects, or on any where it can read the dictionary."""
        marks = ", ".join(f"'{k}'" for k in kinds)
        sql = ("select grantee, table_name, privilege, grantable from "
               "{view} where {owner} = " + MARK
               + f" and type in ({marks}) order by 1, 2, 3")
        try:
            rows = self._cat(side, db, sql.format(view="dba_tab_privs",
                                                  owner="owner"))
        except Exception:  # noqa: BLE001 - the dictionary is not readable
            rows = self._cat(side, db, sql.format(view="all_tab_privs",
                                                  owner="table_schema"))
        return {(str(g), self._said(str(t)), str(p), str(o))
                for g, t, p, o in self._taken(db, rows, 1)}

    def _deep_grants(self, db):
        from ..users import NOT_CARRIED, carried
        if not carried(self.hop):
            return Result("deep", f"{db} grants", "skip", NOT_CARRIED)
        status, detail, fix = self._grant_words(db, ("TABLE", "VIEW"))
        return Result("deep", f"{db} grants", status, detail, "", fix)

    def _deep_seq_grants(self, db):
        from ..users import NOT_CARRIED, carried
        if not carried(self.hop):
            return Result("deep", f"{db} seq-grants", "skip", NOT_CARRIED)
        status, detail, fix = self._grant_words(db, ("SEQUENCE",))
        return Result("deep", f"{db} seq-grants", status, detail, "", fix)

    def _grant_words(self, db, kinds):
        """(status, detail, fix) of the grants on the schema's objects of
        `kinds`, the source's held to the target's."""
        src, dst = self._grants("src", db, kinds), \
            self._grants("dst", db, kinds)
        miss, extra = sorted(src - dst), sorted(dst - src)
        if not (miss or extra):
            return ("ok", f"{len(src)} grants match" if src
                    else "no grants on these objects", "")
        parts = []
        if miss:
            parts.append(f"{len(miss)} grants missing on the target: "
                         + "; ".join(f"{g}: {p} on {t}"
                                     for g, t, p, _ in miss[:3]))
        if extra:
            parts.append(f"{len(extra)} grants on the target that the"
                         " source does not give: " + "; ".join(
                             f"{g}: {p} on {t}" for g, t, p, _ in extra[:3]))
        return ("diff", "; ".join(parts),
                "migkit users create <hop> --apply replays the source's"
                " grants")

    def _deep_options(self, db):
        """What a table is kept with beyond its columns - written without
        redo (`nologging`), compressed, held read-only, free to move its
        rows - the target's held to the source's. A table the target keeps
        read-only refuses the tail's writes; one kept without redo cannot
        be recovered past the load."""
        sql = ("select table_name, logging, compression, row_movement,"
               " read_only, iot_type from all_tables where owner = " + MARK
               + " and nested = 'NO' and secondary = 'N' and temporary ="
                 " 'N' order by 1")

        def read(side):
            try:
                rows = self._cat(side, db, sql)
            except Exception as e:  # noqa: BLE001 - before 12.2: no read_only
                if "ORA-00904" not in str(e):
                    raise
                rows = self._cat(side, db,
                                 sql.replace(" read_only,", " null,"))
            return {self._said(str(t)): (str(lg), str(c), str(m),
                                         str(ro or "NO"), str(i or ""))
                    for t, lg, c, m, ro, i in self._taken(db, rows)}
        src, dst = read("src"), read("dst")
        names = ("logging", "compression", "row movement", "read-only",
                 "organisation")
        bad = [f"{t}: {names[i]} {a[i]} on the source, {dst[t][i]} on the"
               " target" for t, a in sorted(src.items()) if t in dst
               for i in range(len(names)) if a[i] != dst[t][i]]
        if bad:
            return Result("deep", f"{db} options", "diff",
                          f"{len(bad)} table options differ: "
                          + "; ".join(bad[:5]), "",
                          "alter table ... as the source has it",
                          category="structure.collection-options")
        return Result("deep", f"{db} options", "ok",
                      f"{len(src)} tables kept with the options the source"
                      " keeps them with",
                      category="structure.collection-options")

    def _deep_ownership(self, db):
        """Whose privileges a routine runs with - its owner's (definer's
        rights) or the caller's - which the object check cannot see: the
        routine is there and correct, and runs as someone else."""
        from .. import ownership as _own
        sql = ("select object_name, object_type, authid from all_procedures"
               f" where owner = {MARK} and procedure_name is null and"
               " object_type in ('FUNCTION', 'PROCEDURE', 'PACKAGE',"
               " 'TYPE') order by 1")
        src = {(str(t), self._said(str(n))): str(a) for n, t, a in
               self._taken(db, self._cat("src", db, sql))}
        dst = {(str(t), self._said(str(n))): str(a) for n, t, a in
               self._taken(db, self._cat("dst", db, sql))}
        rows = [(f"{k[0].lower()} {k[1]}", src[k], dst[k])
                for k in sorted(set(src) & set(dst))]
        if not rows:
            return Result("deep", f"{db} ownership", "ok",
                          "no routines, packages or types")
        changes = _own.group(rows)
        if not changes:
            return Result("deep", f"{db} ownership", "ok",
                          f"{len(rows)} objects keep the rights they run with")
        note = ("one substitution across every object - a decision or"
                " something the move did to all of them"
                if _own.systematic(changes) else
                "objects drifted individually - read each one")
        invoker = sum(len(v) for (a, b), v in changes.items()
                      if a == "DEFINER" and b == "CURRENT_USER")
        detail = f"{_own.total(changes)} of {len(rows)}: " \
                 f"{_own.describe(changes)}"
        if invoker:
            detail += (f"; {invoker} dropped from definer's rights to the"
                       " caller's - they now run with the caller's"
                       " privileges")
        return Result("deep", f"{db} ownership", "diff", detail, "",
                      f"{note}; recreate with the intended AUTHID before the"
                      " application depends on it")
