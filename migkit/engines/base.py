import re
import threading
import time
from dataclasses import dataclass, field

from .. import fencing
from ..pieces import BATCH_BYTES, PAIR_MEMORY, PAIR_ROWS, row_bytes

# Engine-independent name for what a finding is about.
#
# Every engine words its own checks differently - postgres calls it `encoding`,
# mysql calls it `charset`, mongo calls it `null-missing` where postgres says
# `nullempty`. They are the same three failures. Without a shared name, a
# report can only be read by someone who already knows which engine produced
# it, and nothing downstream can aggregate across hops.
#
# The mapping is keyed on the last word of the scope, falling back to the check
# family. Categories are stable: they are what external consumers match on, so
# they are renamed only with a format_version bump.
CATEGORIES = {
    # values that arrived wrong while looking present
    "charset": "value.charset",
    "encoding": "value.charset",
    "collation": "value.collation",
    "narrowing": "value.narrowing",
    "float": "value.precision",
    "nullempty": "value.null-empty",
    "null-missing": "value.null-empty",
    "timeshift": "value.timezone",
    "generated": "value.generated",
    "render": "value.rendering",
    "bson-types": "value.type-drift",
    # structure and constraints
    "objects": "structure.objects",
    "columns": "structure.columns",
    "keys": "structure.keys",
    "fk": "structure.foreign-keys",
    "checks": "structure.unvalidated-constraints",
    "deferrable": "structure.deferrable",
    "partitions": "structure.partitions",
    "matviews": "structure.materialized-views",
    "triggers": "structure.triggers",
    "indexes": "structure.indexes",
    "rls": "structure.row-security",
    "extensions": "structure.extensions",
    "capped": "structure.collection-options",
    "storage": "structure.collection-options",
    "sharding": "structure.sharding",
    "(atlas)": "structure.schema-diff",
    "(liquibase)": "structure.schema-diff",
    "(structural)": "structure.schema-diff",
    # identity and access
    "usable": "identity.sequence-collision",
    "parity": "identity.sequence-parity",
    "grants": "access.table-grants",
    "seq-grants": "access.sequence-grants",
    # who an object belongs to, and whose privileges it runs with. One
    # category for both because the consequence is the same: the object is
    # present and correct, and the account attached to it is not the one the
    # application was built around.
    "ownership": "access.object-ownership",
    # how the data moved
    "boundary": "movement.target-ahead",
}
# used when the scope carries no recognised sub-check name
CATEGORY_BY_CHECK = {
    "counts": "parity.row-count",
    "data": "parity.row-content",
    "delta": "parity.row-content",
    "autoinc": "identity.sequence-parity",
    "params": "config.behaviour",
    "schema": "structure.objects",
    "deep": "structure.objects",
}
# statuses, narrowest to widest. `warn` sits between ok and diff: something
# worth reading that is not itself a mismatch.
STATUSES = ("ok", "skip", "warn", "diff", "error")


def clauses(parts, most=10):
    """At most `most` of a finding's clauses, and how many more there are
    as one clause more: a list cut short in silence read as every
    difference there was, to a reader holding each to a hop's carried
    changes (`verdict.hold`) as well as to a person."""
    parts = list(parts)
    more = len(parts) - most
    return "; ".join(parts[:most]) + (f"; and {more} more" if more > 0
                                      else "")


def rows_counted(table, missing, extra, changed, exact=True):
    """A finding's `rows_apart` for one table: the rows apart of each
    kind, counted one by one where `exact`."""
    return {str(table): {"missing": int(missing), "extra": int(extra),
                         "changed": int(changed), "exact": bool(exact)}}


def categorize(check, scope):
    """Canonical category for a (check, scope) pair. Never raises."""
    last = str(scope).split()[-1] if str(scope).strip() else ""
    return (CATEGORIES.get(last)
            or CATEGORY_BY_CHECK.get(check)
            or f"{check}.unclassified")


@dataclass
class Result:
    check: str
    scope: str
    status: str
    detail: str = ""
    report: str = ""
    fix_hint: str = ""
    category: str = ""
    #: the table a finding about one table is about, named within the
    #: database as its source names it (`public.orders`) where the scope
    #: names it otherwise - a pair names a table by its own name alone
    #: (`HeteroEngine._leaf`) - so a rule written for one schema's table is
    #: held to it and not to another schema's of the same name
    table: str = ""
    #: the rows apart, counted, of each table the finding is about -
    #: `{table: {"missing", "extra", "changed", "exact"}}`, the table named
    #: within the database as the source names it (`public.orders`), and
    #: `exact` where every row was counted one by one. What the verdict
    #: holds to a hop's carried changes (`verdict.hold`), read here and
    #: not back out of the words; empty where the finding counted none
    rows_apart: dict = field(default_factory=dict)
    #: of a database's one answer, how each table it compared came out -
    #: `{table: "ok" | "diff" | "error" | "skip"}` - so a reader holding
    #: another tool's word table by table (`servicecheck`) knows which
    #: tables the answer covers and how, not only the ones it names
    tables: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.category:
            self.category = categorize(self.check, self.scope)


@dataclass
class RepairAction:
    scope: str
    kind: str
    statements: list = field(default_factory=list)
    undo: list = field(default_factory=list)
    note: str = ""
    #: "pair" where the pair machinery carries it out (`through_pair`)
    by: str = ""
    #: the table a row repair is for, as the check and its drilldown name
    #: it: the way it goes is climbed for it (`repairways`)
    table: str = ""


#: the kind of a repair action that carries nothing out and only says
#: what the repair leaves alone - a drilldown the last check left for a
#: table the hop no longer takes (`Engine._drills_in_scope`). Printed with
#: the plan, never applied
LEFT_OUT = "left out"


#: held while an engine draws its run's salt (`Engine.digest_salt`)
_SALT_DRAWN = threading.Lock()


def _ident(key, text):
    """A row's key as the applier tells rows apart:
    `tuple(sorted((name, text(value))))`, without the sort where the key
    is one column, as most are. With it, and a row's shape worked out once
    for each order its names come in (`_net_rows`, `_apply_net`): measured,
    the 320,000 changes of `bench/tail_rates.py` applied into PostgreSQL
    in 5.1s before and 4.3s after, and 2.4s and 2.2s of CPU with no server
    to write to."""
    if len(key) == 1:
        (n, v), = key.items()
        return ((n, text(v)),)
    return tuple(sorted((n, text(v)) for n, v in key.items()))


class Engine:
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = False
    #: whether `check_params` reads anything a database can set for itself.
    #: Where it reads only the server's, one comparison answers for every
    #: database the hop moves.
    SETTINGS_PER_DATABASE = False

    def __init__(self, hop):
        self.hop = hop

    def digest_salt(self):
        """This run's salt for the row hashes a same-engine check sums on
        both servers (`checkpoint.new_salt`): one per engine, so both sides
        of one comparison hash alike and the next run draws again. Drawn
        under a lock: the two sides are read on two threads, and two first
        asks drawing one salt each made identical tables differ -
        measured, 20 equal rows on PostgreSQL read as DIFF."""
        with _SALT_DRAWN:
            if not getattr(self, "_digest_salt", None):
                from .. import checkpoint
                self._digest_salt = checkpoint.new_salt()
            return self._digest_salt

    def databases(self):
        raise NotImplementedError

    def takes(self, db, kind, name, by=None):
        """Whether the hop takes an object an inventory lists: its table in
        scope (`by`, the table it belongs to - a trigger's, an index's - or
        its own name), and its kind carried (`Hop.takes_object` - the
        hop's `objects`). Every inventory, schema comparer and schema fix
        asks this one question of an object beside the tables."""
        if not self.in_scope(db, by if by is not None else name):
            return False
        from ..util import split_table
        eng = self.CANON_ENGINE or getattr(self.hop, "engine", "")
        schema, leaf = split_table(eng, str(name).split("(", 1)[0])
        return self.hop.takes_object(kind, db,
                                     *([schema] if schema else []), leaf)

    def in_scope(self, db, table):
        """Whether a table, named as this engine's own listings name it
        (`public.orders`, `orders`, `"v1.orders"`), is one the hop takes -
        the one question every listing this engine makes for a check, an
        inventory, a repair, a counter or a watch asks before it reads a
        table, so none of them reads one the hop leaves out (backlog
        P0-SCOPE.6)."""
        return self.hop.takes_table(
            db, table, self.CANON_ENGINE or getattr(self.hop, "engine", ""))

    def _scoped_rows(self, db, rows, at=0):
        """The rows of a catalogue listing whose `at`th value is a table
        the hop takes (`in_scope`). Every deep check, inventory and repair
        reads its tables through it, so none reads one the hop leaves out:
        a keyless or drifted table the move never took made the deep check
        say different, and a repair wrote into one (backlog P0-SCOPE.6b)."""
        return [r for r in rows if self.in_scope(db, str(r[at]))]

    def _drills_in_scope(self, db, tables, what="tables"):
        """(the tables a row repair may act on, an action saying the rest
        or None), of the tables the last check left a drilldown for.

        The files are the last check's, and a check run before the hop
        left a table out - an `exclude` added, an `include` narrowed -
        left one for a table that is now the target's own: a repair read
        from it deleted the rows the target keeps there. A table the hop
        no longer takes is never acted on, and the plan says so
        (`LEFT_OUT`)."""
        tables = list(tables)
        gone = sorted(t for t in tables if not self.in_scope(db, t))
        if not gone:
            return tables, None
        left = set(gone)
        return [t for t in tables if t not in left], RepairAction(
            db, LEFT_OUT, [], [],
            f"{len(gone)} {what} the last check found different are ones"
            f" the hop no longer takes: {', '.join(gone[:5])}"
            + (" ..." if len(gone) > 5 else "")
            + " - left as the target has them; the check that named them"
              " ran under another scope")

    def check_schema(self, db):
        raise NotImplementedError

    def check_counts(self, db):
        raise NotImplementedError

    def check_autoinc(self, db):
        return [Result("autoinc", db, "skip", "not applicable for this engine")]

    def check_deep(self, db):
        return [Result("deep", db, "skip", "no deep checks for this engine yet")]

    #: How close to the engine's own ceiling a value may get before it is
    #: worth saying out loud. Not a guess about the data - a value at 80% of
    #: a hard limit is one growth spurt from an outage.
    LOB_HEADROOM = 0.80

    def _lob_result(self, db, findings, limit_name, hint):
        """What the largest values in a table say about the move.

        `findings` is [(table, column, src_max, dst_max, hard_limit)] with
        sizes in bytes and `dst_max` None when the target has no such table
        yet.

        Two questions, and the second is the one nothing else asks. First:
        how big is the biggest value, because every mover with a LOB mode
        needs that number and gets it wrong by default - AWS DMS's limited
        LOB mode pre-allocates to `LobMaxSize` and **truncates past it with
        a warning in a log nobody reads**. Second: is the target's biggest
        value smaller than the source's, which is what that truncation looks
        like afterwards - the row counts match, the checksums differ, and
        without this the operator is left guessing which column lost what.
        """
        cut = [(t, c, s, d) for t, c, s, d in
               ((t, c, s, d) for t, c, s, d, _ in findings)
               if d is not None and s is not None and d < s]
        near = [(t, c, s, lim) for t, c, s, _, lim in findings
                if lim and s and s > lim * self.LOB_HEADROOM]
        if cut:
            worst = ", ".join(
                f"{t}.{c} {s:,} -> {d:,} bytes" for t, c, s, d in cut[:4])
            return Result(
                "deep", f"{db} lobs", "diff",
                f"{len(cut)} columns hold smaller values on the target than"
                f" on the source: {worst}"
                + (" ..." if len(cut) > 4 else "")
                + " - that is what a mover truncating past its LOB limit"
                  " leaves behind, and the row counts will not show it",
                "", hint)
        if near:
            worst = ", ".join(f"{t}.{c} {s:,} of {lim:,} bytes"
                              for t, c, s, lim in near[:4])
            return Result(
                "deep", f"{db} lobs", "warn",
                f"{len(near)} columns are within"
                f" {int((1 - self.LOB_HEADROOM) * 100)}% of the"
                f" {limit_name}: {worst}", "", hint)
        if not findings:
            return Result("deep", f"{db} lobs", "ok",
                          "no columns large enough to be stored out of line")
        biggest = max((s or 0, t, c) for t, c, s, _, _ in findings)
        return Result(
            "deep", f"{db} lobs", "ok",
            f"{len(findings)} large-value columns, biggest is"
            f" {biggest[1]}.{biggest[2]} at {biggest[0]:,} bytes"
            " - a mover with a LOB size limit needs to be set above that")

    def moved_nothing(self, db):
        """Tables the source has rows in and the target has none of.

        The guard for the failure an external mover can produce without
        saying so: it exits 0, prints its progress, and the target is empty.
        Measured for real - pgcopydb built against a newer PostgreSQL emits
        `SET transaction_timeout = 0`, an older server rejects it, and a
        whole-database clone reports each rejection separately while moving
        no rows at all.

        An existence probe rather than a count, deliberately: this is the
        "nothing arrived" case, and `migkit check` is what proves the rest.
        Returns the table names, or **None** when the engine cannot answer -
        which is not the same as an empty list, and the caller says so.
        """
        return None

    def settle_target(self, db, from_source=True):
        """Leave the target usable after a bulk load, or say nothing.

        `from_source` is False where the source is another engine, whose
        own counters this one cannot read.

        A load leaves the statistics behind it, and the planner then picks
        plans for a table it has never looked at. pgcopydb runs `VACUUM
        ANALYZE` per table as soon as that table's data and indexes are
        done, for exactly this reason; a tool that copies ten million rows
        and hands back a database that reads slowly has not finished.

        Returns a line for the log when it did something, None when there is
        nothing to do for this engine.
        """
        return None

    #: The share of a table that can change before its statistics stop being
    #: worth trusting. This is autovacuum's own `autovacuum_analyze_scale_
    #: factor`, so migkit is not inventing a threshold - it is reading the
    #: one the server already works to.
    STALE_STATS_RATIO = 0.10

    def _planner_stats_result(self, db, tables, hint, reachable=()):
        """Has the target's planner ever looked at what was just loaded.

        A bulk load leaves the statistics behind. Measured on PostgreSQL 16
        right after loading 200,000 rows: `reltuples` is -1, `last_analyze`
        and `last_autoanalyze` are both null, and `n_mod_since_analyze`
        equals the whole table.

        **Most of those heal themselves, and this used to report them
        anyway.** The load is itself the modifications autoanalyze counts,
        so a freshly loaded table is already far past its threshold - on
        5,000 rows the threshold is 50 + 0.1 * 5000 = 550 and the counter
        reads 5000. Measured: analyzed by the server, unasked, one naptime
        after the load. Reporting that as `diff` put `verdict: different` on
        a pair whose every parity check passed, for about a minute, after
        which it said `OK` with nothing changed but time - and `diff`
        everywhere else here means the two sides do not match.

        `reachable` is the tables autovacuum will get to on its own. What
        is left is the case worth a finding, and it is a real one: a table
        loaded with `autovacuum_enabled = false` - standard practice during
        a bulk load, and a standard thing to forget to undo - or a target
        with autovacuum off altogether. Measured on such a table, 75
        seconds after the load: still never analyzed, still `reltuples=-1`,
        and it stays that way.

        An engine that cannot tell the two apart passes nothing and keeps
        the old behaviour, because "I could not ask" is not "there is
        nothing there".

        `tables` is [(name, rows, analyzed, modified_since)] where
        `analyzed` is False when the server has never analyzed it and
        `modified_since` is how many rows changed since it last did.
        """
        unseen = sorted(t for t, _, analyzed, _ in tables if not analyzed)
        coming = [t for t in unseen if t in reachable]
        never = [t for t in unseen if t not in reachable]
        stale = sorted(
            t for t, rows, analyzed, modified in tables
            if analyzed and rows and modified is not None
            and modified > rows * self.STALE_STATS_RATIO)
        # said wherever the line ends up, because a table the planner has
        # not seen yet is worth knowing about even when nobody need act
        waiting = (f"; {len(coming)} more are not analyzed yet and"
                   " autovacuum will reach them without being asked"
                   if coming else "")
        if never:
            return Result(
                "deep", f"{db} statistics", "diff",
                f"{len(never)} tables the planner has no statistics for and"
                f" autovacuum will not reach: {', '.join(never[:5])}"
                + (" ..." if len(never) > 5 else "")
                + " - the rows are there and the query plans will not be"
                + waiting, "", hint)
        if stale:
            return Result(
                "deep", f"{db} statistics", "warn",
                f"{len(stale)} tables changed by more than"
                f" {int(self.STALE_STATS_RATIO * 100)}% since their"
                f" statistics were taken: {', '.join(stale[:5])}"
                + (" ..." if len(stale) > 5 else "") + waiting, "", hint)
        if coming:
            return Result(
                "deep", f"{db} statistics", "ok",
                f"{len(tables)} tables, {len(coming)} of them not analyzed"
                f" yet: {', '.join(coming[:5])}"
                + (" ..." if len(coming) > 5 else "")
                + " - autovacuum reaches these on its own within a naptime,"
                  " so this is a wait rather than a fault"
                + " (`migkit move --go` analyzes what it loads rather than"
                  " waiting)")
        return Result("deep", f"{db} statistics", "ok",
                      f"{len(tables)} tables, all analyzed since they were"
                      " last written")

    #: How many non-ASCII rows per table to look at. The classification is
    #: per value and needs no context, so a sample answers the question that
    #: matters - "does this column hold both kinds of row" - without reading
    #: a terabyte to do it.
    MOJIBAKE_SAMPLE = 2000

    #: The codecs a UTF-8 string gets read as when the connection lied about
    #: its charset. latin-1 is the classic; cp1252 is the one that produces
    #: the `â€"` everybody recognises, because it maps the C1 block to
    #: typographic characters instead of controls.
    MOJIBAKE_CODECS = ("latin-1", "cp1252")

    @classmethod
    def _double_encoded(cls, text):
        """Was this text UTF-8 that something read as single-byte and stored
        again.

        The test is the round trip, not a list of suspicious substrings:
        re-encode the characters back to the bytes they would have been, and
        see whether those bytes are valid UTF-8 that says something else.
        Genuine accented text fails it, which is the whole point - measured
        on a live column, `café`, `Ångström`, `Müller`, `Ação`, `Ça va`,
        `Ægir`, `£100` and `日本語` all come back clean, because a lone
        `é` (0xE9) is not the start of any valid UTF-8 sequence.

        Returns (codec, what it really says) or None.
        """
        for codec in cls.MOJIBAKE_CODECS:
            try:
                decoded = text.encode(codec).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
            if decoded != text:
                return codec, decoded
        return None

    def _quote_ident(self, name):
        """SQL quoting for one identifier. Doubling the quote character is
        the rule in both spellings, so only the character differs."""
        return '"' + str(name).replace('"', '""') + '"'

    @staticmethod
    def _visible_text(s):
        """What a reader actually sees, so two renderings can be compared.

        Categories rather than a hand-written list of code points: `Cf` is
        the zero-width and BOM family, `Zs` is every space that is not
        U+0020, `Cc` covers carriage return and tab, and NFC folds a
        combining accent onto the letter it decorates. All four were
        checked against `unicodedata` rather than assumed.

        Runs of whitespace collapse to one, because they render as one.
        Without that, `one\\r\\ntwo` and `one\\ntwo` come out as `one  two`
        and `one two` and the carriage return - the difference this whole
        section exists for - reads as something a reader could have seen.
        """
        import unicodedata
        out = []
        for c in unicodedata.normalize("NFC", s):
            cat = unicodedata.category(c)
            if cat == "Cf":
                continue
            out.append(" " if cat == "Zs" or (cat == "Cc" and c.isspace())
                       else c)
        return " ".join("".join(out).split())

    @classmethod
    def _invisible_difference(cls, a, b):
        """Why two values that print the same are not the same.

        Returns the reasons, or `[]` when the two are equal *or* when the
        difference is one anybody can see - a row differing `red` from
        `blue` has nothing to explain and would only crowd out the rows
        that do.
        """
        import unicodedata
        if not isinstance(a, str) or not isinstance(b, str):
            return []
        if a == b or cls._visible_text(a) != cls._visible_text(b):
            return []
        why = []
        if unicodedata.normalize("NFC", a) == unicodedata.normalize("NFC", b):
            why.append("the same text written with different code points"
                       " (NFC vs NFD)")
        if len(a) - len(a.strip()) != len(b) - len(b.strip()):
            why.append("leading or trailing whitespace")
        if ("\r" in a) != ("\r" in b):
            why.append("a carriage return on the "
                       + ("source" if "\r" in a else "target"))

        def cats(s, want):
            return {c for c in s if unicodedata.category(c) == want
                    and c != " "}
        for cat, what in (("Cf", "a zero-width character"),
                          ("Zs", "a space that is not U+0020")):
            odd = cats(a, cat) ^ cats(b, cat)
            if odd:
                why.append(what + " ("
                           + ", ".join(f"U+{ord(c):04X}" for c in sorted(odd))
                           + ")")
        if why:
            return why
        # Only now, because `str.split()` treats a non-breaking space as
        # whitespace and every reason above is more useful than this one -
        # said first, it turned "a space that is not U+0020" into a second
        # line nobody needed.
        if a.split() == b.split():
            return ["the runs of spaces, tabs or newlines differ"]
        return ["characters that render the same"]

    #: How many identical-looking pairs one report names. A section longer
    #: than the sample it explains stops being an explanation.
    INVISIBLE_CAP = 20

    def _invisible_section(self, src_df, dst_df, keys, table=None):
        """The part of a drilldown a reader could not otherwise act on.

        A count of differing rows is not a finding anybody can work with
        when the values print identically. Both engines hand their sample
        back as a dataframe, so this is written once and read by both.
        """
        def col(df, name):
            for c in df.columns:
                if str(c).lower() == str(name).lower():
                    return c
            return None

        on = [col(src_df, k) for k in keys]
        if any(c is None for c in on) or any(col(dst_df, k) is None
                                             for k in keys):
            return ""
        try:
            merged = src_df.merge(dst_df, on=on, suffixes=("__s", "__d"))
        except Exception:
            return ""
        found, more = [], 0
        for name in src_df.columns:
            if name in on or col(dst_df, name) is None:
                continue
            left, right = f"{name}__s", f"{name}__d"
            if left not in merged or right not in merged:
                continue
            for _, row in merged.iterrows():
                why = self._invisible_difference(row[left], row[right])
                if not why:
                    continue
                if len(found) >= self.INVISIBLE_CAP:
                    more += 1
                    continue
                from .. import masking
                hide = masking.keys(self.hop, table)
                key = " ".join(
                    f"{k}={masking.token(self.hop, row[k]) if hide else row[k]}"
                    for k in on)
                found.append((key, name,
                              masking.shown(self.hop, table, name, row[left]),
                              masking.shown(self.hop, table, name,
                                            row[right]), why))
        if not found:
            return ""
        out = ["", "Differences You Cannot See",
               "--------------------------",
               "These pairs print identically in the sample above. Shown"
               " escaped, so the bytes are readable.", ""]
        for key, name, a, b, why in found:
            out.append(f"  {key}  {name}")
            out.append(f"      source  {ascii(a)}")
            out.append(f"      target  {ascii(b)}")
            out.append(f"      {'; '.join(why)}")
        if more:
            out.append(f"  ... {more} more not shown")
        return "\n".join(out)

    def _quote_literal(self, value):
        """One string as a SQL literal.

        Doubling the quote is the standard and is all PostgreSQL needs.
        MySQL also reads a backslash as an escape unless the session says
        otherwise, so it overrides this - which is the reason the method
        exists at all rather than the quoting being written inline.
        """
        return "'" + str(value).replace("'", "''") + "'"

    def _extension_data_result(self, db, findings, checked, hint):
        """Rows an extension owns, which nothing else in the report looks
        at.

        The extension list and its versions are compared elsewhere and
        always have been. This is the third thing an extension brings: its
        own data. PostGIS keeps coordinate systems in `spatial_ref_sys`,
        and a custom SRID sits there among the stock entries - a restore
        does not overwrite rows the target already has, so the stock table
        wins and the custom entry is quietly absent.

        `findings` is [(extension, table, what differs)].
        """
        if findings:
            worst = ", ".join(f"{ext} {table}: {why}"
                              for ext, table, why in findings[:4])
            return Result(
                "deep", f"{db} extension data", "diff",
                f"{len(findings)} tables an extension owns differ:"
                f" {worst}"
                + (" ..." if len(findings) > 4 else "")
                + " - the extension is installed and the rows it needs are"
                  " not the same", "", hint)
        if not checked:
            return Result("deep", f"{db} extension data", "ok",
                          "no extension on the source registers data of its"
                          " own")
        return Result("deep", f"{db} extension data", "ok",
                      f"{checked} tables owned by extensions hold the same"
                      " rows on both sides")

    def _large_object_result(self, db, src_total, dst_total, dangling,
                             columns, hint, contents=None, by_value=0):
        """Whether the documents a table only points at came across.

        A PostgreSQL large object does not live in the table. The table
        holds an `oid`; the bytes live in `pg_largeobject`, and nothing
        enforces a relationship between the two. Copy the table and you
        have copied the integer.

        Measured on a pair whose table contents were **byte-identical** -
        `1contract16391,2invoice16391` on both sides - where the source
        resolved the oid to a document and the target answered `large
        object 16391 does not exist`. `counts`, `data` and `deep` all said
        the migration was correct, because the column really did hold the
        same integer.

        `dangling` is [(table.column, broken on target, resolved on
        source)]. Requiring the **source** to resolve a column before
        reporting the target is what keeps this off the many `oid` columns
        that hold something else entirely - a `regclass`, say - and would
        otherwise look broken on both sides. `contents` is (objects both
        sides hold and were compared, [oid whose bytes differ]) where they
        were read; `by_value` how many of the `columns` are integer columns
        found by what they hold (problems D11).
        """
        unsure = ("; an integer column is taken for references where at"
                  " least half its values name an object on the source"
                  f" ({by_value} found that way)")
        if dangling:
            worst = ", ".join(f"{col} {broken} of {ok} rows"
                              for col, broken, ok in dangling[:4])
            return Result(
                "deep", f"{db} large objects", "diff",
                f"{len(dangling)} columns point at large objects the target"
                f" does not have: {worst}"
                + (" ..." if len(dangling) > 4 else "")
                + " - the rows arrived and what they refer to did not",
                "", hint)
        if src_total and not dst_total:
            return Result(
                "deep", f"{db} large objects", "diff",
                f"the source holds {src_total} large objects and the target"
                " holds none - a dump restricted by schema or table drops"
                " them silently, and the oid columns still compare equal",
                "", hint)
        compared, differ = contents if contents is not None else (None, [])
        if differ:
            return Result(
                "deep", f"{db} large objects", "diff",
                f"{len(differ)} of {compared} large objects both sides hold"
                " are other bytes on the target (the first: oid "
                + ", ".join(str(o) for o in differ[:4])
                + ") - the reference arrived and the document it names is"
                " not the source's", "", hint)
        if src_total != dst_total:
            return Result(
                "deep", f"{db} large objects", "diff",
                f"{src_total} large objects on the source and {dst_total} on"
                " the target", "", hint)
        if not src_total and not columns:
            return Result("deep", f"{db} large objects", "ok",
                          "no large objects on either side")
        same = (f", every one the same bytes on both ({compared} compared"
                " a piece at a time)" if compared is not None else "")
        return Result(
            "deep", f"{db} large objects", "ok",
            f"{src_total} large objects on both sides{same}, and every"
            " column that names one on the source names one on the target"
            f" ({columns} checked){unsure}")

    def _trigger_result(self, db, disabled, quieted, hint):
        """What the target's triggers will do to a load, in both
        directions.

        The check used to look one way only: triggers **disabled** on the
        target, which is the after-cutover worry - somebody turned them off
        for the load and forgot to turn them back on. That is still the
        fault, and still a difference.

        What it never said is which triggers are **enabled**, and therefore
        which ones `move` is about to silence on the operator's behalf. It
        does silence them, deliberately - a `BEFORE INSERT` trigger setting
        `updated_at := now()` was measured rewriting every migrated row to
        the date of the migration. But work that does not happen is worth
        naming: an audit trigger records nothing for the migrated rows, and
        a denormalised counter is not maintained.

        `quieted` counts only triggers on tables a load would actually
        write - a table that exists on the target alone is not one of them,
        and mentioning it would be the kind of noise that teaches people to
        skip this line.
        """
        if disabled:
            return Result(
                "deep", f"{db} triggers", "diff",
                "disabled on target: " + ", ".join(disabled[:5]), "",
                "alter table ... enable trigger before cutover")
        if quieted:
            return Result(
                "deep", f"{db} triggers", "ok",
                f"no disabled triggers on target; {len(quieted)} enabled on"
                f" tables a load writes ({', '.join(quieted[:4])}"
                + (" ..." if len(quieted) > 4 else "")
                + ") - `migkit move` runs with session_replication_role ="
                  " replica, so these will not fire for the migrated rows",
                "", hint)
        return Result("deep", f"{db} triggers", "ok",
                      "no disabled triggers on target, and none enabled on"
                      " the tables a load would write")

    def _filtered_tables(self, side, db):
        """Tables the role migkit is connected as cannot read in full.

        Row-level security is a `WHERE` clause the server adds to every
        query, and it applies to the tool doing the verifying as readily as
        to the application. A hop configured with an application role -
        which is what people do rather than hand a migration tool superuser
        - reads a subset on **both** sides and has no way to know it.

        Returns the table names, or **None** when the engine has no such
        concept or cannot be asked; the caller must not read None as "none
        are filtered".
        """
        return None

    def _honest_about_filtering(self, result, db):
        """An `ok` reached through a filter is not an `ok`.

        Measured before this existed: with the same policy on both sides and
        five of the source's ten rows deleted from the target, `counts`
        reported `OK 1 tables, rows 5==5` and `data` reported `OK 1 tables,
        5 rows, checksums equal both sides`. Neither was wrong about what it
        compared. Both were silent about what they could not see, which made
        a half-empty target read as verified.

        Only a clean verdict is touched: a `diff` found a real difference
        inside what the role *could* see, and that difference is real
        whatever is hidden behind it.
        """
        if result.status != "ok":
            return result
        hidden = self._filtered_tables("src", db)
        if not hidden:
            return result
        return Result(
            result.check, result.scope, "warn",
            result.detail
            + f" - but the role migkit is connected as cannot read"
              f" {len(hidden)} of these tables in full"
              f" ({', '.join(hidden[:4])}"
            + (" ..." if len(hidden) > 4 else "")
            + "), so those numbers are what it was allowed to see and not"
              " what is there",
            result.report,
            "re-run as a role that bypasses row-level security - an owner"
            " without FORCE, or one with BYPASSRLS - before trusting this")

    def _insert_override(self, side, db, table, names):
        """A clause an engine needs before VALUES to write a column the
        server would otherwise generate itself. Empty for engines with no
        such concept, which is most of them."""
        return ""

    def _unwritable_columns(self, side, db, table):
        """Columns the server computes and refuses to be told.

        A repair carries a whole row, including the columns a generated
        expression owns, and has to leave those out rather than argue. An
        empty set for an engine with no such concept - which is a claim, so
        an engine that has them says so by overriding this.

        Comparing them stays right either way: a target whose expression
        differs from the source's should show up as a value difference. It
        is only the writing that has to give way.
        """
        return set()

    def _scalar(self, side, db, sql):
        """One row, as text, or **None** when this engine cannot be asked -
        which is not the same as a query that returned nothing."""
        return None

    #: How a value that is too big for the target is found, per capacity
    #: kind. One place, because the two engines spell all of these the same
    #: way - `char_length`, `octet_length` and `abs` are standard.
    def _capacity_probe(self, col, cap):
        """(the rows that do not fit, the largest value) for a capacity:
        the first is `canon.unfit_sql`'s, the one question a move across
        engines asks before it starts too."""
        from .. import canon
        kind = cap[0]
        asked = {"chars": ("chars", cap[1]), "bytes": ("bytes", cap[1]),
                 "int": ("int-range", tuple(cap[1:3])),
                 "numeric": ("digits", tuple(cap[1:3]))}.get(kind)
        if asked is None:
            raise ValueError(f"no capacity probe for kind {kind!r}")
        where = canon.unfit_sql(self.CANON_ENGINE, asked[0], col, asked[1])
        if kind == "chars":
            return where, f"max(char_length({col}))"
        if kind == "bytes":
            return where, f"max(octet_length({col}))"
        return where, f"max(abs({col}))"

    #: rows of a table read at once when the values are asked here
    UNFIT_BATCH = 5000

    def count_unfit(self, side, db, table, columns, checks, key=(),
                    where=None, examples=3):
        """[(rows, [key of an example row, ...])] for each (column, kind,
        arg) in `checks` - how many of this table's rows hold a value of
        that kind (`canon.unfit`), and which.

        Asked in the engine's own SQL where `canon.unfit_sql` has a
        predicate for it - one scan for every check of the table, and a
        second read, of at most `examples` rows, only for a check that
        found some. The rest are read into this process a batch at a time
        and asked by `canon.unfit_value`, which is what every engine
        without SQL gets. `columns` is [(name, class)] for the checked
        columns and the key."""
        from .. import canon
        out = [[0, []] for _ in checks]
        preds, here = {}, []
        classes = dict(columns)
        for i, (col, kind, arg) in enumerate(checks):
            p = canon.unfit_sql(self.CANON_ENGINE, kind,
                                self._quote_ident(col), arg,
                                classes.get(col))
            if p is None:
                here.append(i)
            elif p != "false":
                preds[i] = p
        if preds:
            at = self._qualified(side, db, table)
            scope = f" and ({where})" if where else ""
            order = list(preds)
            got = self.run_rule(side, db, "select " + ", ".join(
                f"count(case when {preds[i]} then 1 end)" for i in order)
                + f" from {at}" + (f" where {where}" if where else ""))
            for i, n in zip(order, got[0]):
                out[i][0] = int(n or 0)
                if out[i][0] and key:
                    rows = self.run_rule(
                        side, db,
                        "select " + ", ".join(self._quote_ident(k)
                                              for k in key)
                        + f" from {at} where ({preds[i]}){scope}"
                        + f" limit {int(examples)}")
                    out[i][1] = [tuple(r) for r in rows]
        if here:
            names = [n for n, _ in columns]
            place = {i: names.index(checks[i][0]) for i in here}
            keys = [names.index(k) for k in key if k in names]
            for rows in self._every_row(side, db, table, columns, where):
                for r in rows:
                    for i in here:
                        _, kind, arg = checks[i]
                        if canon.unfit_value(kind, r[place[i]], arg):
                            out[i][0] += 1
                            if len(out[i][1]) < examples and keys:
                                out[i][1].append(tuple(r[k] for k in keys))
        return [(n, ex) for n, ex in out]

    def _every_row(self, side, db, table, columns, where=None):
        """Every row of a table, a batch at a time: by its key where it
        has one among `columns` - `neutral_batches` of a keyed table is
        its first batch - and in one pass where it has none."""
        names = {n for n, _ in columns}
        key = (self.neutral_key(side, db, table) if self.RESUMES_BY_KEY
               else [])
        scope = {"where": where} if where else {}
        if not key or not set(key) <= names:
            yield from self.neutral_batches(side, db, table, columns,
                                            self.UNFIT_BATCH, **scope)
            return
        after = None
        while True:
            rows, last = self.neutral_read(side, db, table, columns, after,
                                           self.UNFIT_BATCH, **scope)
            if rows:
                yield rows
            if not rows or last is None:
                return
            after = last

    def decimal_extent(self, side, db, table, column, where=None):
        """(most digits before the point, most after it) that one decimal
        column holds, NaN and the infinities left out - what a column
        built for it on a target with a fixed scale has to keep. Read
        here from the values; an engine with SQL for it says so."""
        from decimal import Decimal
        whole = frac = 0
        for rows in self._every_row(side, db, table,
                                    [(column, "decimal")], where):
            for (v,) in rows:
                if v is None:
                    continue
                if hasattr(v, "to_decimal"):
                    v = v.to_decimal()
                v = v if isinstance(v, Decimal) else Decimal(str(v))
                if not v.is_finite():
                    continue
                t = v.as_tuple()
                frac = max(frac, -t.exponent)
                whole = max(whole, len(t.digits) + t.exponent)
        return whole, frac

    def refuse_unfit(self, db, tables=None):
        """Stop before a move where a value would not arrive on the target
        as it is. Nothing to stop for where both sides are one engine
        moving its own way; a pair of engines says (`HeteroEngine`)."""
        return None

    def json_rewritten(self, side, db, numbers):
        """The numbers of `numbers` (their JSON text) this side's JSON
        type would store as a different number, or None where it keeps
        every JSON number it is given."""
        return None

    def key_merges(self, side, db, columns, batches, examples=3):
        """(groups, [[key, key, ...], ...]) - of the keys in `batches`
        (lists of tuples, one value per column of `columns`, [(name,
        collation or None)]), how many groups this side's own comparison
        takes for one key, and a few of them. None where this side keeps
        every two distinct keys apart, which is the default: an engine
        whose text comparison can fold case, accents or trailing spaces
        says so by answering."""
        return None

    def column_facts(self, side, db, table):
        """{column: {"null": whether it takes NULL, "charset": ...}} - what
        a column is beyond its type, for `canon.unfit`. {} where this
        engine says nothing more; `table` None is a table migkit would
        build here, and the facts every column of it would have."""
        return {}

    def _capacity_gaps(self, db):
        """Rows the target has no room for, counted before anything moves.

        A narrower column on the target is not worth an argument until a row
        actually exceeds it - and then it is what stops the load halfway
        through, with half the table moved. So this does not report that
        `varchar(255)` became `varchar(50)`; it reports that **three rows**
        are longer than fifty characters and the longest is 120.
        """
        from .. import canon
        engine = self.CANON_ENGINE
        findings, narrowed, unmeasured = [], 0, 0
        try:
            if self._scalar("src", db, "select 1") is None:
                return Result("deep", f"{db} target capacity", "skip",
                              "this engine cannot be asked to count rows"
                              " against the target's limits")
            src_tables = set(self.neutral_tables("src", db))
            dst_tables = set(self.neutral_tables("dst", db))
            for table in sorted(src_tables & dst_tables):
                dst_types = dict(self.neutral_columns("dst", db, table))
                for name, src_type in self.neutral_columns("src", db, table):
                    if name not in dst_types:
                        continue
                    src_cap = canon.capacity(engine, src_type)
                    dst_cap = canon.capacity(engine, dst_types[name])
                    if src_cap and dst_cap and src_cap[0] != dst_cap[0]:
                        # a character limit and a byte limit are not
                        # comparable, and pretending otherwise is how a
                        # real overflow gets reported as fine
                        unmeasured += 1
                        continue
                    if not canon.narrower(src_cap, dst_cap):
                        continue
                    narrowed += 1
                    col = self._quote_ident(name)
                    where, worst = self._capacity_probe(col, dst_cap)
                    got = self._scalar(
                        "src", db,
                        f"select count(*), {worst} from"
                        f" {self._qualified('src', db, table)} where {where}")
                    if not got:
                        continue
                    count, biggest = got
                    if int(count or 0):
                        findings.append((table, name, src_type,
                                         dst_types[name], int(count),
                                         biggest, dst_cap))
        except Exception as e:
            return Result("deep", f"{db} target capacity", "error",
                          "could not count the rows against the target's"
                          f" limits: {str(e).splitlines()[-1][:90]}")
        return self._capacity_result(db, findings, narrowed, unmeasured)

    def _qualified(self, side, db, table):
        """A table name from `neutral_tables`, quoted and addressable from
        this engine's connection - which for some engines means carrying the
        database name and for others means not."""
        return ".".join(self._quote_ident(p) for p in table.split(".", 1))

    def _capacity_result(self, db, findings, narrowed, unmeasured):
        """The rows are the finding; the narrowing on its own is not.

        Measured on the pair this was written against: MySQL stores
        `0000-00-00` happily and `select '0000-00-00'::date` on PostgreSQL
        answers `ERROR: date/time field value out of range`. Reading a value
        the other side refuses is not something a checksum can warn about,
        because by then the load has already stopped.
        """
        if findings:
            worst = ", ".join(
                f"{t}.{c} {n} rows, largest {big} against the target's"
                f" {cap[1] if cap[0] != 'int' else cap[2]}"
                for t, c, _, _, n, big, cap in findings[:4])
            return Result(
                "deep", f"{db} target capacity", "diff",
                f"{len(findings)} columns hold values the target has no room"
                f" for: {worst}"
                + (" ..." if len(findings) > 4 else "")
                + " - the load stops on the first of them, with whatever"
                  " moved before it already on the target", "",
                "widen the target column, or decide what happens to those"
                " rows before the move rather than halfway through it")
        if unmeasured:
            return Result(
                "deep", f"{db} target capacity", "skip",
                f"{narrowed} narrower columns hold nothing too big, and"
                f" {unmeasured} pairs measure their limits in different"
                " units (characters against bytes) so migkit did not"
                " compare them")
        if not narrowed:
            return Result("deep", f"{db} target capacity", "ok",
                          "no column on the target is narrower than the"
                          " source's")
        return Result("deep", f"{db} target capacity", "ok",
                      f"{narrowed} columns are narrower on the target and no"
                      " row exceeds any of them yet")

    def _partitions_verdict(self, db, tables, scheme, catchall, unread=()):
        """The deep check's answer about partitioned tables, whichever
        engine read them.

        `tables` is `{table: (source partitions, target partitions)}` in the
        shape `verdict.partition_differences` compares, `scheme` the lines
        already found about the tables themselves (partitioned on one side
        only, a key or method that differs), `catchall` what this engine
        calls the partition rows land in when none other takes them, and
        `unread` the columns a digest could not cover. Every partition
        compared is written to `deep-partitions.diff`, with its rows and
        digest on each side, beside the other deep findings.
        """
        from .. import verdict
        lines = list(scheme)
        for table in sorted(tables):
            s, d = tables[table]
            lines += verdict.partition_differences(table, s, d)
        path = self.hop.report_dir(db) / "deep-partitions.diff"
        parts = sum(len(s) for s, _ in tables.values())
        if not tables and not scheme:
            path.unlink(missing_ok=True)
            return Result("deep", f"{db} partitions", "ok",
                          "no partitioned tables")
        if not lines:
            path.unlink(missing_ok=True)
            counted = sum(1 for s, d in tables.values() for n, p in s.items()
                          if p.get("digest") is None
                          or (d.get(n) or {}).get("digest") is None)
            return Result("deep", f"{db} partitions", "ok",
                          f"{len(tables)} partitioned tables, {parts}"
                          " partitions: schemes match and every partition"
                          " holds the same rows on both sides"
                          + (f" ({counted} compared by count only)"
                             if counted else "")
                          + (f"; not in the digest: {'; '.join(unread[:3])}"
                             if unread else ""))
        listing = []
        for table in sorted(tables):
            s, d = tables[table]
            for n in sorted(set(s) | set(d)):
                for side, got in (("src", s.get(n)), ("dst", d.get(n))):
                    listing.append(
                        f"{side} {table} {n} " + (
                            f"rows={got.get('rows')} digest={got.get('digest')}"
                            f" bound={got.get('bound') or '-'}" if got
                            else "absent"))
                listing.append("")
        path.write_text("\n".join(lines) + "\n\n" + "\n".join(listing))
        return Result("deep", f"{db} partitions", "diff",
                      "; ".join(lines[:6])
                      + (f"; ... {len(lines) - 6} more" if len(lines) > 6
                         else ""), str(path),
                      "create the partitions the target lacks, move the rows"
                      f" out of its {catchall} partition and copy again the"
                      " partitions whose rows differ, before cutover - see"
                      " deep-partitions.diff")

    #: Instants to ask every zone about. A zone is its *history*, so asking
    #: only for today's offset would miss the changes that actually break a
    #: migration - Brazil abolished DST in 2019, Iran in 2022. Measured:
    #: `America/Sao_Paulo` reads 10:00 at the 2018 probe and 09:00 at the
    #: 2020 one, so a server carrying older rules answers differently and
    #: the fingerprint says so.
    TZ_PROBES = ("1995-07-01 12:00:00", "2005-01-15 12:00:00",
                 "2015-07-01 12:00:00", "2018-01-15 12:00:00",
                 "2020-01-15 12:00:00", "2022-07-01 12:00:00",
                 "2026-07-01 12:00:00")

    def _zone_fingerprints(self, side, db):
        """{zone name: fingerprint of what it does at TZ_PROBES}, or **None**
        when this engine cannot be asked - which the caller must not read as
        an empty set of zones."""
        return None

    def _time_zone_rules(self, db):
        """Do both sides agree on what the zone names mean.

        The fingerprint is deliberately "what wall clock does this zone show
        at these instants" rather than anything engine-specific, and that
        turns out to be literally portable: measured, PostgreSQL 16 and
        MySQL 8 produce the **same** md5 for `America/New_York`
        (`789e2da8…`), `America/Sao_Paulo`, `Asia/Tehran` and `UTC`. So this
        compares across a hop between two different engines as readily as
        within one.
        """
        try:
            src = self._zone_fingerprints("src", db)
            dst = self._zone_fingerprints("dst", db)
        except Exception as e:
            return Result("deep", f"{db} time zone rules", "error",
                          "could not read the time zone rules:"
                          f" {str(e).splitlines()[-1][:90]}")
        if src is None or dst is None:
            return Result("deep", f"{db} time zone rules", "skip",
                          "this engine does not expose what its zone names"
                          " mean, so migkit cannot compare them")
        missing = sorted(set(src) - set(dst))
        drifted = sorted(n for n in set(src) & set(dst) if src[n] != dst[n])
        return self._time_zone_result(
            db, len(src), len(dst), missing, drifted,
            "load the same time zone rules on both sides and restart -"
            " the data is cached, so loading them is not enough on its own")

    def _time_zone_result(self, db, src_count, dst_count, missing, drifted,
                          hint):
        """What an unloaded or stale zone table costs, as measured.

        The official `mysql:8` image arrives with **1,795** zones loaded,
        which is why this is easy to believe is somebody else's problem.
        Emptied and restarted, the same server answers
        `convert_tz('2026-07-01 12:00:00','UTC','America/New_York')` with
        **NULL** and `show warnings` with nothing at all. The offset form
        `'+00:00'` keeps working, so a smoke test written that way passes
        while every named zone silently answers NULL - and a **stored
        generated column** built on `CONVERT_TZ` wrote NULL to disk for a
        row whose source value was present. Data, on disk, wrong, no error.
        """
        for side, count in (("source", src_count), ("target", dst_count)):
            if not count:
                return Result(
                    "deep", f"{db} time zone rules", "diff",
                    f"the {side} cannot resolve a single zone name - every"
                    " conversion by name returns NULL there, with no warning"
                    " and no error, and anything computed from one writes"
                    " NULL to disk", "", hint)
        if missing:
            return Result(
                "deep", f"{db} time zone rules", "diff",
                f"{len(missing)} zone names the target cannot resolve:"
                f" {', '.join(missing[:5])}"
                + (" ..." if len(missing) > 5 else "")
                + " - a conversion naming one of these returns NULL on the"
                  " target and the same expression worked on the source",
                "", hint)
        if drifted:
            return Result(
                "deep", f"{db} time zone rules", "warn",
                f"{len(drifted)} zones mean different things on the two"
                f" sides: {', '.join(drifted[:5])}"
                + (" ..." if len(drifted) > 5 else "")
                + " - the two servers carry different rules, so the same"
                  " conversion of the same value gives two answers", "",
                hint)
        if not src_count:
            return Result("deep", f"{db} time zone rules", "skip",
                          "neither side has any named zones to compare")
        return Result("deep", f"{db} time zone rules", "ok",
                      f"{src_count} named zones, and both sides agree about"
                      " what every one of them does")

    def _temporal_meaning_result(self, db, mismatched, unmapped, checked):
        """Whether the two sides agree on what a temporal column is *for*.

        A checksum compares the values as they now stand and will happily
        agree that `2020-11-01 01:05:00` equals `2020-11-01 01:05:00`. It
        cannot say that the source column recorded an instant and the target
        column records digits off a wall clock, because that is a fact about
        the type and not about any row.

        What it costs, measured rather than reasoned: PostgreSQL given
        `'2020-11-01 01:05:00+04'` stores `01:05:00` in a `timestamp` and
        `2020-10-31 21:05:00+00` in a `timestamptz` - four hours discarded
        from a value that named its own offset. And across a DST boundary
        two genuinely different instants, `01:30:00-04` and `01:30:00-05`,
        both become the digits `01:30:00`, after which nothing can tell them
        apart.

        The trap this exists for is that **`timestamp` means opposite things
        in the two engines**: PostgreSQL's is the wall clock and MySQL's is
        the instant. A MySQL `timestamp` landing in a PostgreSQL `timestamp`
        looks like the identity mapping and is not.

        `mismatched` is [(table, column, src_type, src_meaning, dst_type,
        dst_meaning)]; `unmapped` counts temporal-looking columns whose
        engine migkit has not measured, which is not the same as agreement.
        """
        def side(t, m):
            return f"{t} ({m})"

        if mismatched:
            worst = ", ".join(
                f"{t}.{c} {side(st, sm)} -> {side(dt, dm)}"
                for t, c, st, sm, dt, dm in mismatched[:4])
            return Result(
                "deep", f"{db} temporal meaning", "diff",
                f"{len(mismatched)} columns change what they mean between"
                f" the two sides: {worst}"
                + (" ..." if len(mismatched) > 4 else "")
                + " - one side records an instant and the other records"
                  " digits off a wall clock, so the offset is dropped on the"
                  " way across and a checksum of what arrives cannot see it",
                "",
                "give the target the type that carries the same meaning, or"
                " convert deliberately with the zone the values were"
                " actually in - converting without naming it uses whichever"
                " zone the session happened to have")
        if unmapped:
            return Result(
                "deep", f"{db} temporal meaning", "skip",
                f"{checked} temporal columns agree, and {unmapped} are on an"
                " engine whose temporal types migkit has not measured - so"
                " this says nothing about those")
        if not checked:
            return Result("deep", f"{db} temporal meaning", "skip",
                          "no temporal columns on both sides to compare")
        return Result("deep", f"{db} temporal meaning", "ok",
                      f"{checked} temporal columns record the same kind of"
                      " time on both sides")

    def _temporal_meaning(self, db):
        """Built on the neutral contract, so every engine that can list its
        columns gets this without writing any of it again."""
        from .. import canon
        mismatched, unmapped, checked = [], 0, 0
        engine = self.CANON_ENGINE
        try:
            src_tables = set(self.neutral_tables("src", db))
            dst_tables = set(self.neutral_tables("dst", db))
            for table in sorted(src_tables & dst_tables):
                dst_types = dict(self.neutral_columns("dst", db, table))
                for name, src_type in self.neutral_columns("src", db, table):
                    if name not in dst_types:
                        continue
                    src_meaning = canon.time_meaning(engine, src_type)
                    dst_meaning = canon.time_meaning(engine, dst_types[name])
                    if src_meaning is None and dst_meaning is None:
                        # a column with no measured meaning is either not
                        # temporal at all or temporal on an engine nobody
                        # measured, and those must not read the same. The
                        # canonical class already knows which it is.
                        if canon.comparable(engine, src_type)[0] in (
                                "timestamp", "date", "time"):
                            unmapped += 1
                        continue
                    checked += 1
                    if src_meaning != dst_meaning:
                        mismatched.append((table, name, src_type, src_meaning
                                           or "unknown", dst_types[name],
                                           dst_meaning or "unknown"))
        except Exception as e:
            return Result("deep", f"{db} temporal meaning", "error",
                          "could not compare the temporal columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._temporal_meaning_result(db, mismatched, unmapped,
                                             checked)

    #: Duplicate groups to name per index. Enough to act on, few enough that
    #: a table which has gone entirely wrong does not produce a report
    #: nobody can read.
    DUPLICATE_CAP = 20

    def _duplicate_hunt_result(self, db, found, checked, skipped, reason,
                               hint):
        """Rows a unique index should have refused and did not.

        This is the damage the other checks only predict. It runs **only**
        when something has already said an index cannot be trusted - a
        collation that changed underneath it, or an index that exists and
        answers nothing - because otherwise it is a sequential scan of every
        table to prove a negative.

        The one thing it must not do is ask the broken index. Measured on a
        200,001-row table holding one duplicate that its unique index never
        recorded: the planner chose `Index Only Scan using big_email` for
        `group by ... having count(*) > 1` all by itself and reported **0**
        duplicates. With index, bitmap and index-only paths disabled, the
        same query on the same data reported **1**. A hunt that trusts the
        planner here is a false negative, which is the worst thing this
        project can ship.

        `found` is [(side, table, index, columns, groups, example)].
        """
        if found:
            from .. import masking
            worst = ", ".join(
                f"{s} {t}.{c} ({cols}) {n} duplicated values, e.g. "
                + (str(masking.token(self.hop, ex))
                   if masking.keys(self.hop, t) else f"{ex}")
                for s, t, c, cols, n, ex in found[:4])
            return Result(
                "deep", f"{db} duplicate keys", "diff",
                f"{len(found)} unique indexes have duplicate rows underneath"
                f" them: {worst}"
                + (" ..." if len(found) > 4 else "")
                + " - the constraint is still there and stopped being"
                  " enforced, so nothing rejected these rows", "", hint)
        if not reason:
            return Result(
                "deep", f"{db} duplicate keys", "skip",
                "nothing has suggested an index is lying, and hunting"
                " duplicates means reading every table to prove a negative")
        if not checked:
            return Result(
                "deep", f"{db} duplicate keys", "skip",
                f"{reason}, but no unique index over a text column was found"
                " to hunt through"
                + (f" ({skipped} partial or expression indexes were not"
                   " checked)" if skipped else ""))
        return Result(
            "deep", f"{db} duplicate keys", "ok",
            f"{reason}, so {checked} unique indexes over text were read"
            " without the planner being allowed to consult them - no"
            " duplicate rows underneath any of them"
            + (f" ({skipped} partial or expression indexes were not checked)"
               if skipped else ""))

    def _mojibake_tally(self, table, columns, rows):
        """Count, per column, how many sampled values are double-encoded and
        how many are simply non-ASCII and fine.

        `rows` is an iterable of sequences lined up with `columns`, so an
        engine that reads through a driver and one that reads JSON out of a
        client both arrive here rather than each growing their own counter.

        Returns (findings, values seen).
        """
        tally = {c: [0, 0, None, None] for c in columns}
        seen = 0
        for row in rows:
            for col, value in zip(columns, row):
                if not isinstance(value, str) or value.isascii():
                    continue
                seen += 1
                said = self._double_encoded(value)
                if said:
                    tally[col][0] += 1
                    if tally[col][2] is None:
                        tally[col][2], tally[col][3] = value, said[1]
                else:
                    tally[col][1] += 1
        return ([(table, c, *tally[c]) for c in columns
                 if tally[c][0] or tally[c][1]], seen)

    def _mojibake_result(self, db, findings, scanned, hint):
        """Text that was already broken before anybody moved it.

        An application sending UTF-8 through a latin1 connection stores the
        bytes as latin1 characters: `é` becomes `Ã©`, and everything looks
        fine until a conversion or a client change. The repair is a byte
        round trip over the column.

        The finding that matters is not "this column has mojibake" - it is
        **which columns have both kinds of row**, because that is where the
        obvious repair destroys data. Measured: applying the round trip to a
        column holding a genuine `£100` fails outright in PostgreSQL
        (`invalid byte sequence for encoding "UTF8": 0xa3`), and the
        application-side version of the same fix, which passes
        `errors='replace'`, silently turns it into `�100`. Rows that
        were never broken break, and nothing says so.

        `findings` is [(table, column, suspects, clean, example, decoded)]
        counted over the sampled rows.
        """
        mixed = [f for f in findings if f[2] and f[3]]
        broken = [f for f in findings if f[2] and not f[3]]
        from .. import masking

        def example(t, c, ex, dec):
            # the text itself is the finding, and a token of it says
            # nothing, so a masked column's example is left out
            if masking.column(self.hop, t, c):
                return "(the hop masks this column's values)"
            return f"(e.g. {ex!r} is really {dec!r})"
        if mixed:
            worst = ", ".join(
                f"{t}.{c} {s} double-encoded and {k} genuinely accented"
                f" {example(t, c, ex, dec)}"
                for t, c, s, k, ex, dec in mixed[:3])
            return Result(
                "deep", f"{db} mojibake", "diff",
                f"{len(mixed)} columns hold both double-encoded and correct"
                f" text: {worst}"
                + (" ..." if len(mixed) > 3 else "")
                + " - converting the whole column repairs the first kind and"
                  " destroys the second", "",
                "repair row by row, matching only the values that re-encode"
                " to valid UTF-8 - a blanket conversion over these columns"
                " corrupts the rows that were never broken")
        if broken:
            worst = ", ".join(
                f"{t}.{c} {s} rows {example(t, c, ex, dec)}"
                for t, c, s, _, ex, dec in broken[:3])
            return Result(
                "deep", f"{db} mojibake", "diff",
                f"{len(broken)} columns are double-encoded throughout:"
                f" {worst}"
                + (" ..." if len(broken) > 3 else "")
                + " - no correctly-encoded row was found among those"
                  " sampled, so one conversion over the column fits", "",
                hint)
        if not scanned:
            return Result("deep", f"{db} mojibake", "ok",
                          "no text column holds a non-ASCII character")
        return Result(
            "deep", f"{db} mojibake", "ok",
            f"{scanned} non-ASCII values sampled across the text columns,"
            " none of them UTF-8 that was stored twice")

    #: How many broken values one pass rewrites. A text repair is one
    #: statement per row, so an uncapped plan is a transaction nobody can
    #: review. Hitting the cap is said out loud and the work converges: a
    #: repaired value no longer classifies as double-encoded, so the next
    #: pass takes the next batch rather than redoing this one.
    MOJIBAKE_REPAIR_CAP = 20000

    @staticmethod
    def _repair_text_enabled():
        """Whether the text repair carries statements or only describes
        itself.

        Off by default, and the default is the interesting half. Every other
        repair here moves the target *towards* the source; this one moves it
        away on purpose, because the source is what is broken. An automated
        reconcile loop that quietly rewrote text would leave a target that
        can never check clean again, with nobody having decided that.
        """
        import os
        return os.environ.get("MIGKIT_REPAIR_TEXT", "").strip().lower() \
            in ("1", "true", "yes", "on")

    def _mojibake_updates(self, db, table, key, columns, rows):
        """Statements that repair only the values `_double_encoded` confirms.

        `rows` lines up with `_mojibake_tally`'s except that each row starts
        with its key values: `key` names them, `columns` names the text
        columns that follow. Written here rather than per engine because the
        statement is plain SQL and the decision of *which* rows to touch is
        the whole point - an engine supplying rows cannot get it wrong.

        Two properties the tests pin:

        * a value that is not double-encoded produces no statement at all,
          which is what separates this from the blanket column conversion
          the check warns about;
        * every update matches on the **old value** as well as the key, so a
          row somebody edited between the read and the apply is skipped
          rather than overwritten with a repair of text that is no longer
          there.

        Returns (statements, undo, columns repaired, repaired, left alone,
        skipped, beyond the cap).
        """
        stmts, undo = [], []
        touched = set()
        repaired = clean = skipped = left = 0
        quoted = self._qualified("dst", db, table)
        for row in rows:
            keys, values = row[:len(key)], row[len(key):]
            if any(k is None for k in keys):
                # a key that cannot address one row cannot repair one row
                skipped += 1
                continue
            where = " and ".join(f"{self._quote_ident(k)} = "
                                 f"{self._quote_literal(v)}"
                                 for k, v in zip(key, keys))
            for col, value in zip(columns, values):
                if not isinstance(value, str) or value.isascii():
                    continue
                said = self._double_encoded(value)
                if not said:
                    # genuinely accented text. The whole reason this is a
                    # row-level repair rather than one conversion per column
                    clean += 1
                    continue
                if repaired >= self.MOJIBAKE_REPAIR_CAP:
                    left += 1
                    continue
                c = self._quote_ident(col)
                old, new = self._quote_literal(value), \
                    self._quote_literal(said[1])
                stmts.append(f"update {quoted} set {c} = {new}"
                             f" where {where} and {c} = {old};")
                undo.append(f"update {quoted} set {c} = {old}"
                            f" where {where} and {c} = {new};")
                touched.add(f"{table}.{col}")
                repaired += 1
        return stmts, undo, touched, repaired, clean, skipped, left

    #: whether this engine keeps text in character sets of its own - code
    #: pages, CCSIDs, a database character set - so text stored twice as
    #: UTF-8 is a finding of its check and a repair of its sync
    #: (`_mojibake`, `_mojibake_repair`); MySQL and PostgreSQL ask their
    #: servers in SQL of their own
    CHARSETS = False
    #: rows read of one table at most, looking for non-ASCII values to
    #: sample, where the server is not asked to pick them out - a sample,
    #: as MySQL's and PostgreSQL's are: a deep check reads every table, and
    #: a clean answer says how many values it read (`_mojibake_result`)
    MOJIBAKE_ROWS = 20_000

    def _mojibake(self, db):
        """The mojibake check through the neutral contract, for an engine
        with character sets and no sampling SQL of its own: the text
        columns of every table, read in key order until `MOJIBAKE_SAMPLE`
        non-ASCII values or `MOJIBAKE_ROWS` rows - the counting and the
        verdict the base's, as on MySQL and PostgreSQL."""
        from .. import canon
        findings, scanned = [], 0
        try:
            for t in self.neutral_tables("src", db):
                if not self.in_scope(db, t):
                    continue
                cols = [(n, "text") for n, typ in
                        self.neutral_columns("src", db, t)
                        if canon.type_class(self.CANON_ENGINE, typ) == "text"]
                if not cols:
                    continue
                picked, read = [], 0
                for rows in self._every_row("src", db, t, cols):
                    for r in rows:
                        read += 1
                        if any(isinstance(v, str) and not v.isascii()
                               for v in r):
                            picked.append(r)
                    if len(picked) >= self.MOJIBAKE_SAMPLE \
                            or read >= self.MOJIBAKE_ROWS:
                        break
                found, seen = self._mojibake_tally(
                    t, [n for n, _ in cols], picked[:self.MOJIBAKE_SAMPLE])
                findings += found
                scanned += seen
        except Exception as e:  # noqa: BLE001 - said, as the error
            return Result("deep", f"{db} mojibake", "error",
                          "could not sample the source's text columns:"
                          f" {str(e).splitlines()[-1][:90]}")
        return self._mojibake_result(
            db, findings, scanned,
            "repair it on the target (migkit sync --kind rows with"
            " MIGKIT_REPAIR_TEXT=1), or convert it once on the source")

    def _mojibake_repair(self, db):
        """The row-by-row repair the mojibake check asks for, through the
        neutral contract (`charsets.repair`: each double-encoded value on
        the target read by key, read again just before it is written, and
        written only where nothing changed it since, the old value kept
        for the undo) - for an engine with character sets and no repair
        SQL of its own. Off unless `MIGKIT_REPAIR_TEXT`, as on MySQL and
        PostgreSQL: it moves the target away from the source."""
        from .. import charsets
        p = charsets.text_plan(self, db)
        found = charsets.to_repair(self.hop, self, db, p)
        if not found:
            return None
        repaired = sum(n for n in found.values())
        enabled = self._repair_text_enabled()
        note = self._mojibake_repair_note(repaired, set(found), 0, 0, 0, [],
                                          enabled)
        return RepairAction(db, "text", [
            f"repair {n} double-encoded values of {c} on the target"
            for c, n in sorted(found.items())] if enabled else [], [],
            note)

    def _apply_text(self, db, action, log=None):
        """A text action of `_mojibake_repair` carried out."""
        from .. import charsets
        return charsets.repair(self.hop, self, db,
                               charsets.text_plan(self, db), log)

    def _mojibake_repair_note(self, repaired, columns, clean, skipped, left,
                              refused, enabled):
        """What the plan says about a repair that moves the target away from
        the source rather than towards it.

        `refused` is [(what, why)] rather than a list of names, because the
        two reasons a column is left out - the table has no primary key, and
        the broken column *is* the primary key - are not interchangeable and
        a reader who cannot tell them apart cannot act on either.
        """
        if not repaired:
            return ""
        note = (f"{repaired} double-encoded values in {len(columns)} columns"
                f" ({', '.join(sorted(columns)[:3])}"
                + (" ..." if len(columns) > 3 else "") + ")")
        if clean:
            note += (f", the {clean} genuinely accented values beside them"
                     " left alone")
        if left:
            note += (f"; at least {left} beyond the"
                     f" {self.MOJIBAKE_REPAIR_CAP} this pass carries -"
                     " re-run for the rest")
        if skipped:
            note += f"; {skipped} rows had no usable key"
        if refused:
            note += "; SKIPPED " + ", ".join(
                f"{what} ({why})" for what, why in sorted(refused)[:4])
        if not enabled:
            return (note + " - REFUSED: this rewrites target data so the"
                    " target no longer matches the source, and a later"
                    " `sync --kind rows` copies the source's broken text"
                    " back over it. Fix the source too, then set"
                    " MIGKIT_REPAIR_TEXT=1 to include these statements")
        return (note + "; each update matches the old value as well as the"
                " key, so a row changed since this was read is skipped"
                " rather than overwritten")

    def _collation_version_result(self, db, drifted, missing, checked, hint):
        """Whether the sort order an index was built under still exists.

        A text index is a list sorted by rules the operating system owns,
        not the database. When the C library changes those rules - glibc
        2.28 rewrote them wholesale, and every distribution crossed that
        line - the index is still a list, still sorted, and sorted wrong.
        Lookups walk past the row they wanted. A unique index stops
        catching duplicates, because the duplicate lands somewhere the
        search never goes.

        PostgreSQL records the version it built under and compares it on
        every connection, which is the only reason this is findable at all.
        The trap measured here is the fix people reach for: `ALTER DATABASE
        ... REFRESH COLLATION VERSION` silenced the warning **instantly**,
        without touching a single index. The alarm goes quiet and the
        indexes stay wrong, so the hint has to put the rebuild first.

        `drifted` is [(side, name, stored, actual)]; `missing` is
        [(side, name, stored)] for a locale the OS no longer provides at
        all, which is worse - those indexes cannot even be rebuilt until
        the locale is installed.
        """
        def say(side, name, stored, actual):
            return f"{side} {name} built under {stored}, now {actual}"

        if missing:
            worst = ", ".join(f"{s} {n} built under {v}"
                              for s, n, v in missing[:4])
            return Result(
                "deep", f"{db} collation versions", "diff",
                f"{len(missing)} collations the operating system no longer"
                f" provides: {worst}"
                + (" ..." if len(missing) > 4 else "")
                + " - indexes using them are sorted by rules that are not"
                  " installed, and cannot be rebuilt until they are", "",
                "install the missing locale data before touching these"
                " indexes - rebuilding without it picks a different sort"
                " order again")
        if drifted:
            worst = ", ".join(say(*d) for d in drifted[:4])
            return Result(
                "deep", f"{db} collation versions", "diff",
                f"{len(drifted)} collations changed under their indexes:"
                f" {worst}"
                + (" ..." if len(drifted) > 4 else "")
                + " - a text index sorted by the old rules can walk past the"
                  " row it wanted, and a unique index can stop catching"
                  " duplicates", "", hint)
        if not checked:
            return Result("deep", f"{db} collation versions", "skip",
                          "neither side uses a versioned collation")
        return Result("deep", f"{db} collation versions", "ok",
                      f"{checked} versioned collations, all still matching"
                      " the sort order their indexes were built under")

    def _invalid_index_result(self, db, broken, partial, total, hint):
        """Indexes that exist, answer no query, and still send a bill.

        `CREATE INDEX CONCURRENTLY` does not roll back when it fails - a
        duplicate key, a cancelled session, a `statement_timeout` set for
        ordinary queries - it leaves the index behind marked invalid. The
        planner then ignores it, so it shows up in every "unused index"
        report as something to drop, while the name it holds makes the
        rebuild that would fix it fail with `already exists`.

        Two states, both measured on PostgreSQL 16 rather than assumed, and
        they cost differently:

        * **not maintained** (a build that failed before it finished): 0
          bytes, never grows, pure name collision.
        * **maintained** (a build cancelled after it finished but before it
          was validated): 200,000 inserts grew one from 4.5 MB to 12.3 MB
          while `EXPLAIN` on the indexed column still chose a seq scan.
          Every write pays for an index no read can use.

        `total` is the valid ones, {"source": n, "target": n}.
        `broken` is [(side, table, index, maintained)]. `partial` is the
        separate case that is **not** a fault: an index on a partitioned
        parent is invalid by design until every partition's index has been
        attached, so it is reported as a warning, because one that stays
        that way means a partition is missing its index.
        """
        def name(side, table, index):
            return f"{side} {table}.{index}"

        if broken:
            worst = ", ".join(
                name(s, t, i) + (" (maintained on every write)" if m else
                                 " (not maintained - only the name is taken)")
                for s, t, i, m in broken[:4])
            return Result(
                "deep", f"{db} indexes", "diff",
                f"{len(broken)} indexes exist but answer no query: {worst}"
                + (" ..." if len(broken) > 4 else "")
                + " - a CREATE INDEX CONCURRENTLY that failed leaves this"
                  " behind and does not undo it", "", hint)
        if partial:
            worst = ", ".join(name(s, t, i) for s, t, i in partial[:4])
            return Result(
                "deep", f"{db} indexes", "warn",
                f"{len(partial)} partitioned indexes are not valid yet:"
                f" {worst}"
                + (" ..." if len(partial) > 4 else "")
                + " - normal while partitions are still being attached, and a"
                  " missing partition index if it stays this way", "",
                "ATTACH the index on every partition, or rebuild the parent"
                " index so it builds them for you")
        if not sum(total.values()):
            return Result("deep", f"{db} indexes", "skip",
                          "no indexes on either side to check")
        # per side: "3 indexes, all valid" read as a clean bill for a target
        # a killed load had left with none - measured, the schema check
        # said the three were missing while this said OK beside it
        return Result("deep", f"{db} indexes", "ok",
                      f"{total['source']} on the source and"
                      f" {total['target']} on the target, every one valid"
                      " and usable by the planner")

    def history_tables(self, side, db):
        """{lower-cased name: how this engine names it} of the tables in
        `db` an application's migration tool keeps its history in
        (`appversions`); empty where the engine cannot say."""
        return {}

    def history_rows(self, side, db, ref, columns, where=""):
        """The rows of one such table, `columns` of those satisfying
        `where`, as tuples."""
        raise NotImplementedError

    def check_app_versions(self, db):
        """Whether the application's migration tool records the same
        migrations on both sides: [Result], none where neither side keeps
        such a history."""
        from .. import appversions
        try:
            reads = {side: appversions.applied(
                lambda side=side: self.history_tables(side, db),
                lambda ref, cols, where, side=side: self.history_rows(
                    side, db, ref, cols, where))
                for side in ("src", "dst")}
        except Exception as e:  # noqa: BLE001 - a side that would not say
            return [Result("schema", f"{db} application migrations", "skip",
                           f"the migration history could not be read:"
                           f" {str(e)[:120]}")]
        return [Result("schema", f"{db} {tool} history", status, detail,
                       fix_hint="" if status == "ok" else
                       "run the application's migration tool on the side"
                       " that lacks them, or compare the history tables")
                for status, tool, detail in appversions.compare(
                    reads["src"], reads["dst"])]

    def check_params(self, db):
        return [Result("params", db, "skip",
                       "no parameter comparison for this engine yet")]

    def move_key(self, db, sch, tbl):
        """What a table's entry in the copy's checkpoint is called: the one
        name the copier writes under and the plan reads back."""
        return f"{db}.{tbl or sch}"

    def log_kept(self, db, grows):
        """(bytes, why): how much of `grows` bytes of log written by a load
        this target holds at once, or None where it cannot say."""
        return None

    def stream_identity(self, side, db):
        """What the side's change log belongs to - a server, a replica
        set - for a saved position to be held to, or None where the engine
        cannot say."""
        return None

    def server_identity(self, side, db):
        """What the side's server says it is, for the interlock on the
        target (`interlock`): {"id": which server or cluster, "running":
        the process answering now, "name": what it calls itself,
        "version", "bytes": the database's size, "absent": the database is
        not there yet}, each where the engine can say. Here, the
        change log's owner alone."""
        got = self.stream_identity(side, db)
        return {"id": got} if got else {}

    def side_database(self, side, db):
        """The database a side's connections go to for `db`: the target's
        through the hop's `db_map`, as the engine resolves it (`_d`)."""
        d = getattr(self, "_d", None)
        if d is not None:
            return d(side, db)
        return self.hop.target_db(db) if side == "dst" else db

    def rows_held(self, db, tables):
        """Of `tables` - (schema, table) as `list_move_tables` gives them -
        the ones the target already holds a row of that a copy would write
        over (within the hop's row filter), or None where it cannot be
        asked. Asked through the pair every engine copies through."""
        return self._as_pair().rows_held(db, tables)

    def position_lost(self, side, db, token):
        """Why the side's log no longer holds `token`, in words, or None
        where it does or the engine cannot say before reading."""
        return None

    def drop_lost_position(self, side, db):
        """Let go of what the server keeps for migkit's change position
        where the position is lost, so a new one can be taken (`ship`,
        after a seed the source's log did not outlast). Nothing, where the
        engine keeps nothing on the server for a position: a binary log's
        file and offset, a resume token, an SCN."""
        return None

    #: whether tables can be written side by side on this engine: a file
    #: that takes one writer at a time (SQLite) cannot
    WRITES_IN_PARALLEL = True

    def position_spans(self, side, db, table, rows_per):
        """[(start, where)] covering a table by where its rows are stored,
        about `rows_per` rows each - for a table with no key, which has
        nothing else to resume from - or None where the engine has no
        stored position to read by."""
        return None

    def row_bucket_sql(self, side, db, table, columns, n):
        """A SQL expression in [0, n) this engine computes for each row of
        `table` from the whole row with a hash of its own - the buckets a
        table with no key is copied by where the engine renders canonical
        text only in this process (`ranges.keyless`) - or None where it
        has none. The source's alone: the target's count places a stop."""
        return None

    def native_replica_unsafe(self):
        """Why the server's own replication would carry this hop wrongly,
        in words, or None."""
        return None

    def native_scope_unsafe(self):
        """Why the server's own replication cannot hold to the hop's scope
        as the hop says it - a pattern its filters have no form for - in
        words, or None. Asked of a hop that leaves anything out, by the
        ladder of change ways too (`movers.stream_ways`), which then takes
        migkit's own tail."""
        return None

    def pooler(self, side):
        """{"name", "mode", "version"} for a connection pooler the side is
        reached through, or None where none is known of. `mode` is how it
        shares server sessions - session, transaction or statement - or
        None where it would not say.

        An Amazon RDS Proxy is known by its address, whatever the engine
        behind it; an engine that can ask the pooler itself says more."""
        import re
        ep = self.hop.source if side == "src" else self.hop.target
        if re.search(r"\.proxy-[a-z0-9]+\.[a-z0-9-]+\.rds\.amazonaws\.com$",
                     ep.host or "", re.I):
            return {"name": "RDS Proxy", "mode": None, "version": None,
                    "note": "sharing server sessions between transactions,"
                            " and keeping one to a client that changes its"
                            " session",
                    "means": "every connection migkit opens there changes"
                             " its session, and holds a server session the"
                             " application could have had for as long as"
                             " it runs"}
        return None

    def _pooler_items(self):
        """`assess` rows for a side reached through a connection pooler.

        Pooling by transaction hands a client's next statement to whichever
        server session is free, so what a session holds - a setting, a
        temporary table, a session lock - does not stay with migkit's
        connection, and a setting asked for at connection start is refused
        outright (measured through PgBouncer 1.25:
        `unsupported startup parameter in options: statement_timeout`). A
        change stream through it was measured to work: a subscription
        through PgBouncer 1.25 copied and followed."""
        items = []
        for side, word in (("src", "source"), ("dst", "target")):
            try:
                got = self.pooler(side)
            except Exception:
                got = None
            if not got:
                continue
            name = " ".join(x for x in (got["name"], got.get("version"))
                            if x)
            mode = got.get("mode")
            if mode == "session":
                items.append({
                    "level": "pass", "scope": "instance",
                    "item": f"the {word} is reached through a connection"
                            " pooler",
                    "detail": f"{name}, pooling by session: each connection"
                              " keeps its own server session, as migkit's"
                              " work needs; every connection migkit holds"
                              " is one fewer for the application"})
                continue
            how = (f"pooling by {mode}" if mode else got.get("note")
                   or "in a pooling mode it tells its administrators only")
            means = got.get("means") or (
                "a session setting, a temporary table or a session lock does"
                " not stay with migkit's connection, and a setting asked for"
                " at connection start is refused")
            items.append({
                "level": "warn", "scope": "instance",
                "item": f"the {word} is reached through a connection pooler",
                "detail": f"{name}, {how}: {means}. Give the hop the"
                          " database's own address for the move and the"
                          " repair; the pooler's is for the application"})
        return items

    def text_encodings(self, side, db):
        """{encoding as the engine names it: columns or objects in it},
        for what the side's text is stored in; None where the engine
        cannot say."""
        return None

    def stream_writers(self, db):
        """[(what, pausable)] writing into this target database besides a
        repair: migkit's own change tail (which can be paused), and
        whatever replication the engine can see on the target."""
        from .. import tailctl
        if tailctl.alive(self.hop.report_dir(db)):
            return [("migkit's change tail", True)]
        return []

    #: whether `change_point` only reads: where it does, a plain full copy
    #: can note the position before itself for a later `--mode cdc`. On
    #: PostgreSQL it makes a replication slot, which a copy that was not
    #: asked to be followed has no business leaving on the source.
    CHANGE_POINT_READS_ONLY = False

    def load_window(self, db, log=None, tables=None):
        """What has to be set aside on the target while rows are loaded
        into it, as a context manager; nothing, where the engine needs
        nothing."""
        import contextlib
        return contextlib.nullcontext()

    def create_missing(self, db, log=None):
        """Make on the target the tables in scope it lacks, from the
        source's definition, before the table copier fills them - nothing,
        where the engine's copier makes its own or makes none."""
        return None

    def finish_created(self, db, log=None):
        """What `create_missing` leaves until the rows are in: keys,
        indexes and constraints, where the engine builds them after."""
        return None

    def carry_database_objects(self, db, log=None):
        """What a database holds outside its tables and a copy of the
        tables leaves behind (PostgreSQL's large objects), carried after
        them by a whole-database copy. Nothing, for an engine without."""
        return None

    def _fk_orphans(self, db):
        """The target's rows whose foreign key points at nothing, as a check
        result, or None where the engine has no foreign keys to scan."""
        return None

    def set_aside(self, db):
        """What a load took off this hop's target and has not put back, as
        a check result - None where nothing is, or the engine sets nothing
        aside."""
        return None

    def log_position(self, side, db):
        """Where this engine's change log is now, in the form its tail
        saves a position in, or None where that cannot be compared."""
        return None

    @staticmethod
    def position_reached(have, want):
        """Whether a tail at `have` has read at least as far as `want`."""
        return None

    def canonical_type(self, side, db, declared):
        """The type a cross-engine comparison classifies a column by: the
        declared one, except where the engine knows a type of the
        database's own stands for a built-in one."""
        return declared

    def target_mark(self, db):
        """Something on the target that tells later what was written before
        this moment and what after, recorded as a move begins; {} where the
        time alone has to do."""
        return {}

    def _move_began(self, db):
        """What `target_mark` recorded as the last move of this database
        began, with when; None when no move of migkit's has."""
        import json
        try:
            return json.loads((self.hop.report_dir(db) / "move-began.json")
                              .read_text())
        except (OSError, ValueError):
            return None

    def who_wrote(self, db, table, keys, began):
        """Of the rows only the target has (`keys`, as the drilldown wrote
        them), when they were written against when the move began - a
        sentence, or "" where this engine cannot say."""
        return ""

    def pause_writer(self, db, what, window):
        """Pause `what`, one of `stream_writers`' pausable writers, for a
        repair: True once it has stopped writing, False if it did not
        within `window` seconds."""
        from .. import tailctl
        if what == "migkit's change tail":
            return tailctl.pause(self.hop.report_dir(db), window)
        raise ValueError(f"{what} cannot be paused here")

    def resume_writer(self, db, what):
        from .. import tailctl
        if what == "migkit's change tail":
            return tailctl.resume(self.hop.report_dir(db))
        raise ValueError(f"{what} cannot be resumed here")

    def table_facts(self, side, db):
        """{table: {"rows": estimate or None, "key": bool}} from the
        catalogue, in one query - what the planner decides from. Empty where
        an engine has no cheap way to say."""
        return {}

    def free_bytes(self, side, db):
        """What the side's disk has free, as the server reports it, or None
        where it reports nothing - which is PostgreSQL's and MySQL's
        answer: neither will say, and a guess here would be worse than the
        plan saying nobody knows."""
        return None

    #: what a server says when it will not be written to, in every engine's
    #: words: PostgreSQL's 25006, MySQL's 1290 and 1792, MongoDB's not
    #: primary, SQL Server's 3906
    READ_ONLY_SAID = ("read-only transaction", "--read-only option",
                      "--super-read-only option", "read only transaction",
                      "notwritableprimary", "not primary", "not master",
                      "database is read-only", "is read-only")
    #: and when it has no room left
    FULL_SAID = ("no space left on device", "could not extend file",
                 "the table is full", "disk full", "out of disk space",
                 "errno: 28")

    def why_it_stopped(self, message):
        """What on the servers explains a copy that stopped with
        `message`, as a sentence, or "" where nothing is known. A program
        underneath says what broke; this says why, where the engine can
        tell. Two causes read the same on every engine and are said here:
        a target that went read-only part way - measured, a flip of
        `default_transaction_read_only` mid-move stopped it on `cannot
        execute DELETE in a read-only transaction` and nothing more - and a
        target whose disk filled."""
        said = str(message).lower()
        if any(k in said for k in self.READ_ONLY_SAID):
            return ("the target is read-only: it may have failed over and"
                    " the hop now reaches a reader, or a setting made it"
                    " read-only (default_transaction_read_only, read_only)."
                    " Point the hop at the writer - a cluster's writer"
                    " endpoint - and move again; what was copied is kept")
        if any(k in said for k in self.FULL_SAID):
            return ("the target has run out of disk space. Free space there,"
                    " or grow its storage, and move again; the ranges"
                    " already copied are kept")
        return ""

    def planned_checks(self):
        """Checks added to this hop's battery from what can be seen about
        it, on top of `checks`: the operator's own business rules, when the
        hop carries any, and a second reading of every table where the hop
        asks for two (`two_readings`) and one can be had here."""
        from .. import rules
        from .. import second_reader as sr
        more = ("rules",) if rules.configured(self.hop) else ()
        if sr.asked(self.hop) and self._can_read_twice():
            more += ("second",)
        return more

    def _second_pairs(self, db):
        """[(source table, target table)] the second reading reads: every
        table both sides have, within one engine - a table the hop renames
        paired with the name it has on the target, as the pair engine pairs
        it (`HeteroEngine.match_tables`, the rule read with its database).
        Paired by its own name, a renamed table was never read twice: the
        target has no table of that name."""
        from .hetero import HeteroEngine
        src = sorted(self.neutral_tables("src", db))
        dst = list(self.neutral_tables("dst", db))

        def rename(t):
            # as listed, a bare name kept bare: its rename is said in the
            # parts it was given, and told from it by them
            return self.hop.target_table_in(
                db, *self.hop.parts_of(t, bare_in=""))
        moved = [t for t in src if str(rename(t)) != str(t)]
        pairs, _, _, _ = HeteroEngine.match_tables(moved, dst, rename)
        taken = {d for _, d in pairs}
        there = set(dst) - taken
        return sorted(pairs + [(t, t) for t in src
                               if t not in moved and t in there])

    def _second_views(self, db):
        """(name, the source's query, the target's) for each view both
        sides have: read through what it answers, not what it is."""
        dst = {n for n, _ in self.neutral_views("dst", db)}
        return [(n, f"select * from {self._qualified('src', db, n)}",
                 f"select * from {self._qualified('dst', db, n)}")
                for n, _ in self.neutral_views("src", db) if n in dst]

    def check_second(self, db):
        """Every table read a second way, through another library and other
        SQL, in one batch: count, sum, minimum and maximum of every column
        and the aggregates each column's type allows (`MORE`), within the
        hop's tolerance where it names one; the views by what they answer;
        and, where the hop asks for two readings, every keyed table row by
        row - with random rows first where it asks for a sample, which is a
        signal and never a verdict.

        Across engines, aggregates and values, not the reader's row hash:
        measured on a MySQL and a PostgreSQL table holding the same values,
        the hash called a row with the double `-1e-07` different - the two
        servers spell it differently before hashing. A second reading that
        reports differences which are not there is worse than none.
        """
        from .. import canon
        from .. import second_reader as sr
        pairs = self._second_pairs(db)
        if not pairs:
            return [Result(sr.CHECK, db, "skip",
                           "no table is on both sides to read twice")]
        names = self._reader_names()
        same = names[0] == names[1]
        ask = sr.asked(self.hop)
        src_e = self._reader_engine("src")
        validations, samples = [], []
        for s_, d_ in pairs:
            a = self._reader_table("src", db, s_)
            both = [f"{a}={self._reader_table('dst', db, d_)}"]
            parts = str(s_).split(".")
            declared = src_e.neutral_columns("src", db, s_)
            cols = [(n, canon.type_class(names[0], t)) for n, t in declared]
            fixed = [n for n, t in declared if sr.padded(t)]
            rules = self.hop.column_rules(db, *parts) or {}
            drop = set(rules.get("drop") or []) | set(rules.get("rename")
                                                      or {})
            if rules.get("keep"):
                drop |= {n for n, _ in cols} - set(rules["keep"])
            drop = sorted(drop)
            where = self._reader_where(db, s_)
            validations += sr.aggregates(
                f"column:{a}", both, cols, same,
                self.hop.tolerance(db, *parts), drop, where=where)
            if not (ask.get("rows") or ask.get("sample")):
                continue
            key = list(src_e.neutral_key("src", db, s_))
            if not key:
                continue
            ci = ask.get("case_insensitive", False)
            if ask.get("rows"):
                v = sr.rows(f"row:{a}", both, key, cols, same, where,
                            case_insensitive=ci, drop=drop, fixed=fixed)
                validations += [v] if v else []
            if ask.get("sample"):
                v = sr.rows(f"sample:{a}", both, key, cols, same, where,
                            sample=ask["sample"], case_insensitive=ci,
                            drop=drop, fixed=fixed)
                samples += [v] if v else []
        for name, src_sql, dst_sql in self._second_views(db):
            validations.append(sr.custom_query(f"view:{name}", src_sql,
                                               dst_sql))
        job = {"source": sr.connection(names[0], self.hop.source, db),
               "target": sr.connection(names[1], self.hop.target,
                                       self.hop.target_db(db))}
        out, keep = [], self.hop.report_dir(db)
        ci = ask.get("case_insensitive", False)
        if samples:
            # first, and apart: what it finds is known before the whole
            # tables are read
            early = dict(job, validations=samples)
            out += sr.findings(sr.run(early, keep=keep), db, early, ci, same)
        job["validations"] = validations
        return out + sr.findings(sr.run(job, keep=keep), db, job, ci, same)

    def newer_wins_column(self):
        """The column `newer_wins` names, when it decides changed rows.

        A hop option rather than a choice on the command line: which column
        both sides keep up to date is a fact about the application, the
        same on every run. `--on-conflict keep-target` still keeps every
        target row it was asked to keep.
        """
        if self.hop.options.get("on_conflict") == "keep-target":
            return None
        col = self.hop.options.get("newer_wins")
        return str(col) if col else None

    def _keep_the_newer(self, table, changed, read):
        """Split changed keys into (overwrite, keep): the target's row is
        kept where its `newer_wins` column is later than the source's.

        `read(side, keys)` answers {key: value} for this table. A key whose
        value either side does not have is overwritten - the source is the
        truth whenever the rows cannot say otherwise. DTS's `ConditionCover`
        and pglogical's `last_update_wins` make the same comparison.
        """
        col = self.newer_wins_column()
        if not col or not changed:
            return list(changed), []
        src, dst = read("src", changed), read("dst", changed)
        overwrite, keep = [], []
        for k in changed:
            a, b = src.get(self._key_id(k)), dst.get(self._key_id(k))
            try:
                newer = a is not None and b is not None and b > a
            except TypeError:
                newer = False
            (keep if newer else overwrite).append(k)
        return overwrite, keep

    @staticmethod
    def _key_id(key):
        return tuple(key) if isinstance(key, (list, tuple)) else key

    def check_rules(self, db):
        from .. import rules
        return rules.check(self, db)

    def run_rule(self, side, db, sql):
        """The rows one side answers to a rule, read in a transaction that
        cannot write."""
        raise RuntimeError("business rules are SQL, and this engine does not"
                           " answer SQL")

    #: what an engine puts in a settings mapping it could not read
    UNREADABLE = "_error"

    @staticmethod
    def _fingerprint_failed(error):
        """What a column fingerprint that could not run has to say.

        Every engine's fingerprint returns the columns that differ, so an
        empty list is the sentence "no column differs" - which is the
        opposite of what happened when the query failed. One wording here
        because the callers put it straight into the report, and a reader
        comparing two engines' output should not have to know which one
        stays quiet.
        """
        return [f"(column fingerprint failed: "
                f"{str(error).splitlines()[-1][:70]})"]

    def _param_result(self, db, src, dst, critical, hint):
        """Dump every server setting from both sides to params.json (same shape
        as objects.json), then flag mismatches. Only behavior-critical settings
        (timezone, encoding, collation, sql_mode, ...) fail the check; the many
        instance-specific ones that always differ on managed databases (memory,
        paths, limits) are counted but stay ok. The full list is on disk.

        A side that could not be read has to be said out loud before any of
        that. Measured on a MongoDB started with authentication and connected
        to without credentials - which is what a managed cluster looks like -
        `getParameter` comes back `OperationFailure: Command getParameter
        requires authentication`. Both sides fail the same way, both mappings
        end up holding the same one entry, nothing differs between them, and
        this returned `ok | 1 settings, all equal both sides` about two
        servers that had said nothing at all. An empty reading on both sides
        did the same thing with `0 settings`.
        """
        import json
        names = sorted(set(src) | set(dst))
        inv = {n: {"src": src.get(n), "dst": dst.get(n)} for n in names}
        out = self.hop.report_dir(db) / "params.json"
        out.write_text(json.dumps(inv, indent=1, default=str))
        blind = []
        for side, got in (("source", src), ("target", dst)):
            if not got:
                blind.append(f"{side}: nothing came back")
            elif self.UNREADABLE in got:
                blind.append(f"{side}: {got[self.UNREADABLE]}")
        if blind:
            return [Result("params", f"{db} params", "error",
                           "the settings could not be read - "
                           + "; ".join(blind)
                           + ". Two readings that failed are not two servers"
                             " that agree", str(out),
                           "give migkit an account that can read the server's"
                           " settings on both sides, then re-run")]
        crit_lc = {c.lower() for c in critical}
        diff = [n for n in names if src.get(n) != dst.get(n)]
        crit = [n for n in diff if n.lower() in crit_lc]
        if not diff:
            return [Result("params", f"{db} params", "ok",
                           f"{len(names)} settings, all equal both sides",
                           str(out))]
        if crit:
            shown = "; ".join(f"{n} src={src.get(n)} dst={dst.get(n)}"
                              for n in crit[:12])
            extra = len(diff) - len(crit)
            tail = f" (+{extra} non-critical, see params.json)" if extra else ""
            return [Result("params", f"{db} params", "diff",
                           f"{len(crit)} behavior-critical settings differ: "
                           + shown + tail, str(out), hint)]
        return [Result("params", f"{db} params", "ok",
                       f"{len(diff)} of {len(names)} settings differ but none"
                       " behavior-critical (memory/paths/limits, see"
                       " params.json)", str(out))]

    #: The schema reading that is schema-aware rather than textual, and so
    #: gets the last word. Named for what it produces, not for what produces
    #: it: nobody driving migkit installed that program or can run it.
    AUTHORITY_SCOPE = "(fix DDL)"
    OBJECT_SCOPE = "(object changes)"

    #: `options.schema_authority`: the schema-aware reading has the last
    #: word (the default), or every reading counts. Named for what decides,
    #: not for the program behind it - an older hop's word for the default
    #: (`LEGACY_AUTHORITY`) is read as it
    AUTHORITIES = ("schema-aware", "every-reading")
    LEGACY_AUTHORITY = "atlas"

    def _atlas_authoritative(self, res):
        """When the schema-aware reading says clean, demote the noisier
        textual opinions - the native dump diff and the object-change list -
        to informational, so the database's verdict follows the reading that
        understands what a schema *is* rather than how it is spelled. The
        object inventory still stands, and so does the structural diff: that
        one compares objects rather than text and is never demoted. Opt out
        with `options.schema_authority: every-reading`."""
        said = str(self.hop.options.get("schema_authority")
                   or "schema-aware").strip().lower()
        if said not in ("schema-aware", self.LEGACY_AUTHORITY):
            return res
        if not any(r.scope.endswith(self.AUTHORITY_SCOPE) and r.status == "ok"
                   for r in res):
            return res
        for r in res:
            textual = (r.scope.endswith(self.OBJECT_SCOPE)
                       or r.scope == r.scope.split(" ")[0])  # bare "db"
            if (r.check == "schema" and r.status == "diff" and textual
                    and not getattr(r, "whole_table", False)
                    and not r.scope.endswith((self.AUTHORITY_SCOPE, "objects",
                                              "(structural)"))):
                r.status = "ok"
                r.detail = ("the schema-aware comparison reads this as clean;"
                            f" the textual difference is cosmetic ({r.detail})"
                            )[:200]
        return res

    def check_data(self, db, table=None):
        raise NotImplementedError

    def repair_plan(self, db, kind):
        """[] - but an engine with character sets of its own
        (`CHARSETS`) repairs text stored twice as UTF-8 on the target, row
        by row (`_mojibake_repair`)."""
        if self.CHARSETS and kind in ("rows", "all"):
            text = self._mojibake_repair(db)
            return [text] if text else []
        return []

    def apply(self, db, action):
        """A text action (`_mojibake_repair`) carried out; an engine that
        repairs anything else says how itself."""
        if action.kind == "text":
            return self._apply_text(db, action)
        raise SystemExit(f"{self.CANON_ENGINE} cannot apply a"
                         f" {action.kind!r} action")

    def replication_status(self, db, sql):
        """One line saying whether the native replication this engine just
        set up is actually running, read from the target.

        An engine that offers `replicate_sql` has to answer this, because
        the statements that start replication are not the thing that proves
        it works:

        * PostgreSQL's `CREATE SUBSCRIPTION` dials the source while it runs,
          so it fails loudly - but only after the connection attempt gives
          up. Measured on 16 against a target with no route to the source:
          134 seconds, and `connect_timeout` in the subscription's own
          conninfo does not shorten it (the same conninfo through plain
          libpq failed in exactly the 10 it was given).
        * MySQL's `START REPLICA` returns at once and the IO thread fails
          behind it, so the same broken pair looks like a success. The only
          place that says otherwise is `Last_IO_Error`.

        Two engines, two ways for "replication started" to be a lie, and one
        line that has to tell the truth in both.
        """
        raise NotImplementedError(
            f"{type(self).__name__} emits replicate_sql but cannot say"
            " whether replication is running; printing the statements"
            " without that is how a dead replica looks like a live one")

    def _backfill_clause(self, db, sql):
        """The warning about generated DDL that does not say what the rows
        already on the target should hold, or "". Built here so the two
        engines that generate DDL phrase it identically, and so the
        difference between them stays one table in `ddl.BEHAVIOUR` rather
        than two sentences that can drift."""
        from .. import ddl as _ddl
        found = _ddl.needs_backfill(sql)
        if not found:
            return ""
        try:
            present = self.rows_present(db, sorted({t for t, _, _ in found}))
        except Exception:
            return ""
        said = _ddl.backfill_warning(getattr(self, "ENGINE_FAMILY", ""),
                                     found, present)
        return f"; {said}" if said else ""

    def rows_present(self, db, tables):
        """Of `tables`, the ones the target actually has rows in.

        Asked because the warning it feeds is the difference between a
        linter and a check: reading generated DDL can only say a statement
        *might* be a problem, and migkit can look at the target and say
        whether it *is*. Returns an empty set when the question cannot be
        put, so the warning is withheld rather than guessed.
        """
        return set()

    def apply_replication_stmt(self, side, db, stmt):
        """Run one statement of the `replicate_sql` plan.

        Here rather than in the caller because the engine is the only thing
        that knows which of its own statements reach across to the other
        server, and therefore which ones need a bound. The caller used to
        pick by asking `hasattr(eng, "_psql")` - a method only PostgreSQL
        has - which is how the MySQL path came to crash.
        """
        raise NotImplementedError(
            f"{type(self).__name__} emits replicate_sql but cannot run it")

    def _stream_connector(self):
        """The generated source connector for this hop, or None.

        Read rather than assumed, because it decides how a row repair is
        allowed to work: a running stream is a second writer on the
        target, and a repair that writes rows directly while it runs is
        racing it.
        """
        cfg = self.hop.report_dir() / "stream" / "source-connector.json"
        if not cfg.exists():
            return None
        try:
            import json
            return json.loads(cfg.read_text()).get("config") or None
        except Exception:
            return None

    def _rows_plan(self, db, carry):
        """The `--kind rows` plan of an engine that keeps a drilldown.

        The rows come from the last check rather than from whatever the two
        sides disagree about at this moment, so the plan describes what the
        operator was shown. `carry` is how this engine says where the rows
        come from, which is the only part that differs between them.

        **When a stream is running the repair goes through it, not around
        it.** Writing the rows straight onto the target would put migkit
        and the connector on the same rows at the same time, and the row
        that survives is whichever landed last - a repair that can silently
        lose to the thing it was repairing. Debezium can be asked to read
        the table again instead, which is the same fix applied by the
        writer that is already there.
        """
        stream = self._stream_connector()
        actions = []
        tables = self._drill_tables(db)
        # a table whose leaves differ in too many places for the check to
        # name its rows: repaired by its spans or whole (`repairways`)
        for name in self._leaves_kept(db):
            tables.setdefault(name, {})
        taken, said = self._drills_in_scope(db, tables)
        if said:
            actions.append(said)
        tables = {name: tables[name] for name in taken}
        for name, named in sorted(tables.items()):
            # the rows the hop keeps by the changes it does not carry are
            # left as they are, and said
            found = self._repairable(db, name, named)
            kept = self._kept_said(named, found)
            if kept and not any(found.get(k) for k in
                                ("missing", "changed", "extra")):
                continue
            if stream:
                actions.append(self._resnapshot_action(db, name, found))
                continue
            send = found.get("missing", []) + found.get("changed", [])
            drop = found.get("extra", [])
            statements = [kept] if kept else []
            if not send and not drop:
                kept = self.repair_leaves(db, name) or {}
                parts = kept.get("leaves") or []
                what = ("spans of " + ", ".join(kept.get("key") or [])
                        if kept.get("kind") == "range" else "parts")
                statements.append(
                    f"{len(parts):,} {what} differ, holding"
                    f" {sum(max(a, b) for _, a, b in parts):,} rows - too"
                    " many places for the check to name the rows; copied"
                    " again, what the target held kept first")
                actions.append(RepairAction(
                    f"{db}.{name}", "rows", statements, [],
                    f"{len(parts):,} {what} differ and no row was named",
                    table=name))
                continue
            if send:
                statements.append(
                    f"copy {len(send)} rows {carry}: "
                    + ", ".join(self._key_text(k, self.hop, name)
                                for k in send[:6])
                    + (" ..." if len(send) > 6 else ""))
            if drop:
                statements.append(
                    f"delete {len(drop)} rows the source does not have: "
                    + ", ".join(self._key_text(k, self.hop, name)
                                for k in drop[:6])
                    + (" ..." if len(drop) > 6 else ""))
            actions.append(RepairAction(
                f"{db}.{name}", "rows", statements, [],
                f"{len(found.get('missing', []))} missing,"
                f" {len(found.get('changed', []))} changed,"
                f" {len(drop)} extra; every target row this overwrites or"
                " removes is written to the undo file first", table=name))
        return actions

    def _resnapshot_action(self, db, name, found):
        """Ask the running connector to read one table again.

        There is no undo, and pretending otherwise would be worse than
        saying so: the connector re-reads the source and the sink upserts
        what arrives, so the target ends up holding what the source holds.
        That is the repair. Nothing of the target's own is being kept.

        The cost is named because the operator is the one who pays it: a
        blocking snapshot pauses the stream for the length of the read.
        Incremental would not, and is not available without a signalling
        table on the source - measured, and written up in
        `resnapshot_message`.
        """
        from ..movers import stream_reads_again_in_place
        counts = (f"{len(found.get('missing', []))} missing,"
                  f" {len(found.get('changed', []))} changed,"
                  f" {len(found.get('extra', []))} extra")
        pause = ("The table is read in chunks between the stream's own"
                 " events, so the stream keeps running"
                 if stream_reads_again_in_place(
                     self.hop.report_dir() / "stream")
                 else "The stream pauses while the table is read")
        return RepairAction(
            f"{db}.{name}", "resnapshot", [f"execute-snapshot {name}"], [],
            f"{counts}; a stream is running on this hop, so the repair is"
            " sent through it - the connector reads the table again and the"
            " sink upserts what arrives. Writing these rows directly would"
            f" race the connector on the same keys. {pause}, and `check`"
            " afterwards is what proves it landed")

    def _apply_resnapshot(self, action):
        """Put the request on the signal topic.

        Shared rather than per engine: the pipeline is generated by one
        function for every engine that streams, so the way to talk to it
        is one function too.
        """
        from ..movers import send_resnapshot, stream_reads_again_in_place
        out = self.hop.report_dir() / "stream"
        if not (out / "docker-compose.yml").exists():
            raise RuntimeError(
                "no streaming pipeline for this hop - run"
                f" `migkit move {self.hop.name} --mode cdc --go` first")
        table = action.scope.split(".", 1)[1]
        kind = ("incremental" if stream_reads_again_in_place(out)
                else "blocking")
        return send_resnapshot(out, self.hop.name, [table], kind)

    def prepare_target(self, db):
        """Make whatever the target needs before a first write, or nothing.

        Returns a line for the log when it did something, None when there
        was nothing to do - which is the answer for every engine where the
        database is the operator's to create. PostgreSQL and MySQL will not
        have one conjured for them here: `create database` is a decision
        about where data lives, with an owner and an encoding and a
        tablespace behind it.

        SQLite is the exception, and the reason this exists: there the
        database *is* a file, the mover creates the tables in it anyway, and
        listing what is already there fails outright on a path that does not
        exist yet. Nothing that exists is ever altered by this.
        """
        return None

    def setup_target_plan(self, db):
        return []

    def watch_sample(self, db):
        """One reading for `migkit watch`, or a reason there is none.

        The empty dict this used to return was not a reading and not a
        refusal: `watch` reads `src_rows` and `dst_rows` with a default, so
        the first tick printed `src~0 dst~0`, and the second one raised
        `KeyError: 'ts'` comparing the sample against the one before it. An
        engine with nothing to show says so and the loop prints it.
        """
        import time
        return {"db": db, "ts": time.time(),
                "error": f"{type(self).__name__} has no row count to watch"
                         " - use `migkit check` for this hop"}

    def watched(self, db, src, dst):
        """What a watch sums of two sides' `{table: rows}`, as this engine's
        listings name them: (the source's tables the hop takes, the
        target's tables they land in - under the names the hop gives them
        there - and the taken tables a row filter narrows, whose source
        rows are the whole table's). Summed whole, a hop taking a few
        tables of a billion-row database read `src~1B dst~5M` and a time
        left that never came, and a busy table it leaves out said the
        source was still being written. A hop that narrows nothing is
        summed as it always was."""
        from ..util import split_table
        got = self._taken(db, src)
        if got is None:
            return src, dst, []
        taken, narrowed = got
        eng = self.CANON_ENGINE or getattr(self.hop, "engine", "")
        there = set()
        for t in taken:
            schema, name = split_table(eng, str(t))
            s2, n2 = split_table(eng, self.hop.target_table_in(
                db, *([schema] if schema else []), name), schema)
            there.add(f"{s2}.{n2}" if s2 else n2)
        return ({t: src[t] for t in taken},
                {t: n for t, n in dst.items() if t in there}, narrowed)

    def carried(self, db, facts=None):
        """(the source's `table_facts` of the tables the move carries out
        of `db`, the names of those a row filter narrows) - the one count
        of what a move carries that its plan, its time, its pieces of work
        and the rate it keeps are taken from. Every table the hop leaves
        out is taken away: summed whole, a hop taking a few tables kept a
        rate of every table's rows over its own seconds, and every later
        plan - other hops' too - was read from it. A narrowed table's rows
        and bytes are the whole table's, so a sum over it is the most the
        move carries, never what it carries. A hop that narrows nothing
        has every table, as it always had."""
        facts = self.table_facts("src", db) if facts is None else facts
        got = self._taken(db, facts or {})
        if got is None:
            return dict(facts or {}), []
        taken, narrowed = got
        return {t: facts[t] for t in taken}, narrowed

    def _taken(self, db, names):
        """([the names of `names` the hop takes], [those a row filter
        narrows]), each as this engine's listings name it (`in_scope`) - or
        None where the hop narrows nothing by name or by row, and every
        name is taken whole as it always was."""
        from ..config import scoped
        from ..util import split_table
        rules = self.hop.mapping or {}
        if not (scoped(self.hop) or rules.get("tables")
                or rules.get("where")):
            return None
        eng = self.CANON_ENGINE or getattr(self.hop, "engine", "")
        taken, narrowed = [], []
        for t in names:
            if not self.in_scope(db, t):
                continue
            taken.append(t)
            schema, name = split_table(eng, str(t))
            if self.hop.row_filter(db, *([schema] if schema else []), name):
                narrowed.append(t)
        return taken, sorted(narrowed)

    # which family of client tools this engine uses, for the version check
    ENGINE_FAMILY = ""
    #: the family whose brands `variants.SIGNATURES` tells apart, where the
    #: engine reads them and has no `ENGINE_FAMILY` (which the client-tool
    #: check, the TLS posture and the feature comparison take as the
    #: engine's own software)
    BRAND_FAMILY = ""

    def _client_tool_versions(self, tools, server_version):
        """assess rows for client tools running ahead of the target server.

        `doctor` says whether a program is installed; this says whether it can
        talk to the server it is about to be pointed at. See
        `migkit.toolversion` for the two measurements behind it.
        """
        from .. import toolversion as _tv
        from ..util import which
        got = {}
        for tool in tools:
            if not which(tool):
                continue
            # asked once per build, however often an assessment asks
            got[tool] = _tv.probe(tool)
        out = []
        for level, tool, detail in _tv.report(got, server_version,
                                              self.ENGINE_FAMILY):
            out.append({"level": level, "scope": "client tools",
                        "item": f"{tool} against this target",
                        "detail": detail})
        return out

    # (label, tool names) the client-version check should look at for this
    # engine. Empty means there are no client programs to compare.
    CLIENT_TOOLS = ()

    def _bulk_path_rows(self):
        """assess rows for the programs the move would run: their builds,
        and whether each takes every option the move passes it."""
        from .. import movers
        # the path the move would take for this hop, not the first one
        # installed: the online sync gives way where the hop cannot use it
        base = movers.for_hop(self.hop, self.hop.engine)[0]
        try:
            via, _ = movers.fitted(self.hop, self.hop.engine, base)
        except SystemExit:
            via = base
        dbs = list(self.hop.databases or self.hop.db_map or [])
        if via not in movers.PROGRAMS or not dbs:
            return []
        try:
            got = movers.bulk_path_report(self.hop, dbs[0], via)
        except Exception as e:  # noqa: BLE001 - said, as a row
            # the rest of assess still has its answers to give
            return [{"level": "warn", "scope": "bulk path",
                     "item": "the programs the move runs",
                     "detail": f"not asked ({type(e).__name__}):"
                               f" {(str(e).splitlines() or [''])[-1][:90]}"
                               " - unknown, not clean"}]
        return [{"level": level, "scope": "bulk path", "item": item,
                 "detail": detail} for level, item, detail in got]

    def _server_versions(self):
        """(source version, target version) as the engine reports them.

        (None, None) when the engine cannot say. That is reported as unknown
        rather than skipped: an operator reading `assess` should be told that
        nobody checked, not left to assume it matched.
        """
        return (None, None)

    def _brand_probes(self):
        """({source fields}, {target fields}) for identifying the software.

        Whatever the server said about itself - a version banner, an INFO
        dict, a settings row. An engine with no probe yet returns empty
        mappings, which `variants.identify` reports as an unidentified brand
        rather than as the engine's own software.
        """
        return ({}, {})

    def _brands(self):
        """(source Brand, target Brand), probed once per engine instance.

        A probe that raises is not allowed to take `assess` down with it: the
        brand comes back unidentified, which is a `warn` in the rows below.
        """
        cached = getattr(self, "_brand_cache", None)
        if cached is None:
            from .. import variants as _v
            fam = self.ENGINE_FAMILY or self.BRAND_FAMILY or ""
            try:
                s_raw, d_raw = self._brand_probes()
            except Exception:
                s_raw = d_raw = {}
            cached = (_v.identify(fam, s_raw), _v.identify(fam, d_raw))
            self._brand_cache = cached
        return cached

    def _brand_rows(self):
        """assess rows naming each side's software and its measured limits."""
        from .. import variants as _v
        return _v.rows(*self._brands())

    def _version_row(self, sv, dv, parts=1):
        """The server-version-match row, aware of what reported the numbers.

        Two versions agreeing means nothing when they came from different
        software. Measured: a Redis 7.2.4 source and a Valkey 8.1.10 target
        both report `redis_version:7.2.4`, so the naive comparison passes them
        and the report reads as a clean bill for a pairing nobody checked. A
        brand mismatch demotes this row instead of letting the numbers speak
        for software they do not describe.
        """
        from .. import variants as _v
        item = "server version match"
        if not sv or not dv:
            return {"level": "warn", "scope": "instance", "item": item,
                    "detail": (f"src {sv or '?'} / dst {dv or '?'} - a side"
                               " that would not say is unknown, not clean")}
        same = str(sv).split(".")[:parts] == str(dv).split(".")[:parts]
        why = _v.mismatch(*self._brands())
        if why:
            return {"level": "warn", "scope": "instance", "item": item,
                    "detail": f"src {sv} / dst {dv} - {why}"}
        return {"level": "pass" if same else "warn", "scope": "instance",
                "item": item, "detail": f"src {sv} / dst {dv}"}

    def change_depths(self):
        """{table: (depth, the statement that raises it or "")} of how
        deeply each source table's changes can be read (`changedepth`),
        or None where migkit has no reading of this engine's log settings
        - not a depth guessed."""
        return None

    def assess(self):
        """Pre-migration readiness. The same command on every engine.

        The parts that are true of any database live here - are the two sides
        the same version, and can the client tools on this machine actually
        talk to that server - so an engine gets a real answer the day it is
        added, and deepens from there rather than starting at "not
        implemented". Engines with more to say extend this rather than
        replacing it.
        """
        items = []

        def add(level, scope, item, detail=""):
            items.append({"level": level, "scope": scope, "item": item,
                          "detail": str(detail)})
        items += self._brand_rows()
        try:
            sv, dv = self._server_versions()
        except Exception as e:
            sv = dv = None
            add("warn", "instance", "cannot read the server versions",
                f"{str(e)[:90]} - unknown, not clean")
        else:
            items.append(self._version_row(sv, dv))
        if self.CLIENT_TOOLS:
            items += self._client_tool_versions(self.CLIENT_TOOLS, dv)
        items += self._assess_extra()
        items += self._pooler_items()
        items += self._leg_items()
        items += self._preflight_items()
        items += self._scope_items()
        return items

    def _leg_items(self):
        """Each side's connection, as migkit makes it: verified, or a
        failure where its rows cross a network without the server's
        certificate being checked - a connection anyone on the path can
        stand in the middle of (`tls.posture`). A side on this machine, or
        carried by the hop's tunnel, passes."""
        from .. import tls
        family = self.ENGINE_FAMILY or self.CANON_ENGINE
        out = []
        for side, ep in (("source", self.hop.source),
                         ("target", self.hop.target)):
            if not ep.configured():
                continue
            got = tls.posture(ep, family)
            if got is None:
                continue
            level = {"local": "pass", "verified": "pass",
                     "unknown": "warn"}.get(got["state"], "fail")
            word = {"local": "local", "verified": "verified",
                    "unverified": "NOT verified", "plain": "NOT encrypted",
                    "unknown": "not known"}[got["state"]]
            out.append({"level": level, "scope": "connection",
                        "item": f"{side} connection {word}",
                        "detail": got["detail"]})
        return out

    #: past this many tables in scope, managed services warn (DMS says so
    #: outright); a per-table plan and a per-table verdict both grow with it
    MANY_TABLES = 10_000

    def _scope_items(self):
        """What the tables in scope say about themselves, on any engine:
        how many there are, and names that differ only by case - one table
        on a target that folds case, and a copy that writes one over the
        other."""
        items = []
        try:
            dbs = self.databases()
        except Exception as e:
            # an unreachable source is said by the checks above; this one
            # says it could not look, and does not take assess down with it
            return [{"level": "warn", "scope": "instance",
                     "item": "tables in scope",
                     "detail": "could not be listed: "
                               + (str(e).strip().splitlines() or [""])[-1][:80]
                               + " - unknown, not clean"}]
        for db in dbs:
            try:
                tables = self.neutral_tables("src", db)
            except Exception as e:
                items.append({"level": "warn", "scope": db,
                              "item": "tables in scope",
                              "detail": f"could not be listed: {str(e)[:80]}"})
                continue
            items.append({"level": "warn" if len(tables) > self.MANY_TABLES
                          else "pass", "scope": db,
                          "item": "tables in scope",
                          "detail": f"{len(tables):,}"
                          + (f" - more than {self.MANY_TABLES:,}: split the"
                             " hop by schema or by database"
                             if len(tables) > self.MANY_TABLES else "")})
            folded = {}
            for t in tables:
                folded.setdefault(str(t).lower(), []).append(str(t))
            clash = [names for names in folded.values() if len(names) > 1]
            if clash:
                items.append({
                    "level": "fail", "scope": db,
                    "item": "table names that differ only by case",
                    "detail": "; ".join(" / ".join(c) for c in clash[:6])
                    + " - a target that folds case keeps one of each pair;"
                      " rename one, or map it to a new name in the hop"})
            items += self._writer_items(db)
        return items + self._include_items()

    def _writer_items(self, db):
        """Replication that already writes into this target database.

        DTS fails a task whose objects another task is writing to; two
        writers on one table race, and whichever lands last survives. What
        `stream_writers` names beside a repair is named here before the
        move, where it still costs nothing to stop. migkit's own tail is
        not a stranger, and a target that cannot be asked is said to be
        unknown rather than clear."""
        item = "no other replication writes into the target"
        if type(self).stream_writers is Engine.stream_writers:
            # nothing on this engine's target can be asked; "none found"
            # would be a claim, not an answer
            return []
        try:
            writers = self.stream_writers(db)
        except Exception as e:
            return [{"level": "warn", "scope": db, "item": item,
                     "detail": "could not ask the target: "
                               + (str(e).strip().splitlines() or [""])[-1][:80]
                               + " - unknown, not clean"}]
        foreign = [w for w, pausable in writers if not pausable]
        if not foreign:
            return [{"level": "pass", "scope": db, "item": item,
                     "detail": "none found"}]
        return [{"level": "fail", "scope": db, "item": item,
                 "detail": ", ".join(foreign[:4])
                 + " already writes into it; a move beside it races it"
                   " table by table, and whichever lands last survives."
                   " Stop it, or leave this database to it"}]

    #: Deep checks that predict what a move will **do**, as opposed to
    #: reporting what it did. These run in `assess` as well, because an
    #: answer that only arrives from the command you run afterwards is an
    #: answer that arrives too late.
    #:
    #: Measured on a pair whose target column had been narrowed and whose
    #: timestamp had lost its zone: `assess` reported 13 pass, 5 warn, 2
    #: fail and mentioned neither, while `check --deep` named both. The
    #: checks existed; the operator could only reach them after the move.
    #:
    #: Deliberately not here: the checks that compare what is *on* the
    #: target - large objects, extension data, duplicate keys, counts. The
    #: target is empty before a move, so those would report a difference
    #: every time and teach people to ignore the section.
    PREFLIGHT = ()

    #: left-out tables a database's line names before it counts the rest;
    #: the run's decision record keeps every one
    SCOPE_NAMED = 10
    #: the closest real names an entry that matches nothing is offered
    NEAREST = 3
    #: a child's rows counted at most, for a foreign key whose parent the
    #: hop leaves out - a large child costs no more than these rows read
    ORPHANS_COUNTED = 100_000
    #: foreign keys across the scope whose child rows are counted, a
    #: database
    ORPHAN_COUNTS = 20
    #: what this engine lists of what a database's objects need of its
    #: tables (`dependencies`), beside its foreign keys
    DEPENDENCY_KINDS = ()

    def _include_items(self):
        """What the hop's scope takes, read before the move and said so it
        can be checked at a glance: for each database how many of the
        source's tables the hop takes and leaves, the first it leaves
        named; for each include and exclude entry what it matched, a
        pattern said to take the tables made later as well, and one that
        matches nothing offered the closest real names; then what the
        tables it takes need of the ones it leaves (`_closure_items`).
        Every name goes to the run's decision record, and to the console
        where MIGKIT_EXPLAIN=1. An entry that takes nothing, a database
        include takes nothing of, is a table the operator believes is
        moved and checked that nothing looks at - the check, comparing
        only what is taken, would call the rest the same. Nothing is said
        for a hop that takes everything.

        The source's every table is listed once, by a twin of the hop
        that takes everything and has no target (`_unscoped`): what the
        hop takes is that list read through the one scope answer
        (`in_scope`), not a second listing."""
        from ..config import scoped
        hop = self.hop
        if not scoped(hop):
            return []
        # a pair's source is listed by the source's own engine
        lister = getattr(self, "src_engine", None) or self
        try:
            dbs = self.databases()
            whole = self._unscoped(lister)
            every = {db: [str(t) for t in whole.scope_tables("src", db)]
                     for db in dbs}
        except Exception as e:
            why = str(e).splitlines()[-1][:90] if str(e) else \
                type(e).__name__
            return [{"level": "warn", "scope": "before the move",
                     "item": "scope", "detail":
                         "could not list the source's tables, so what"
                         f" the hop's scope takes is unknown: {why}"}]
        try:
            others = [d for d in whole.databases() if d not in dbs]
        except Exception:  # noqa: BLE001 - the databases it opens stand
            others = []
        taken = {db: [t for t in got if lister.in_scope(db, t)]
                 for db, got in every.items()}
        return (self._scope_said(lister, every, taken, others)
                + self._closure_items(lister, every, taken))

    def _unscoped(self, lister):
        """`lister`'s engine on a twin of the hop that takes everything
        and has no target: what lists the source's every table, so the
        scope can say what it leaves. Its target is no server at all, so
        nothing it does can reach the hop's."""
        import dataclasses

        from ..config import Endpoint
        twin = dataclasses.replace(self.hop, include=[], exclude=[],
                                   target=Endpoint())
        return type(lister)(twin)

    def _table_parts(self, lister, table):
        """A table's name as the parts the hop's patterns are matched
        against, split the one way (`util.split_table`)."""
        from ..util import split_table
        schema, name = split_table(lister.CANON_ENGINE
                                   or getattr(self.hop, "engine", ""),
                                   str(table))
        return [schema, name] if schema else [name]

    def _first(self, names):
        """The first `SCOPE_NAMED` names, and how many more there are."""
        names = list(names)
        more = len(names) - self.SCOPE_NAMED
        return ", ".join(names[:self.SCOPE_NAMED]) + (
            f" and {more} more" if more > 0 else "")

    def _nearest(self, entry, lister, pairs, dbs):
        """' - did you mean ...?' with the closest names the source has
        for an entry that matches nothing - first the ones it names but
        for case, then the nearest by spelling (`difflib`) - or ''."""
        import difflib

        from ..config import name_matches
        known = set(dbs)
        for db, t in pairs:
            parts = [db, *self._table_parts(lister, t)]
            known.update(".".join(parts[i:]) for i in range(len(parts)))
            known.update(".".join(parts[:i]) for i in range(1, len(parts)))
        depth = len(str(entry).split("."))
        alike = sorted(n for n in known if len(n.split(".")) == depth) \
            or sorted(known)
        got = [n for n in alike if name_matches(n, entry, True)]
        if not got:
            # spelled near: the last part held to the last parts of the
            # names under the same database and schema, where there are
            # any - a long shared head makes every name look near
            head, _, leaf = str(entry).rpartition(".")
            pool = [n for n in alike if n.rpartition(".")[0].casefold()
                    == head.casefold()] or alike
            by_leaf = {}
            for n in pool:
                by_leaf.setdefault(n.rpartition(".")[2], []).append(n)
            got = [n for near in difflib.get_close_matches(
                       leaf, sorted(by_leaf), n=self.NEAREST, cutoff=0.6)
                   for n in by_leaf[near]]
        got = got[:self.NEAREST]
        return f" - did you mean {', '.join(got)}?" if got else ""

    def _scope_said(self, lister, every, taken, others=()):
        """The scope's rows (`_include_items`): each database, each entry,
        and the decision record holding every name."""
        import dataclasses

        from .. import decide
        from ..config import name_matches
        hop = self.hop
        before = "before the move"
        items = []
        record = {"work": "the hop's scope", "databases": {},
                  "left out whole": list(others), "entries": {}}
        for db, got in every.items():
            took = set(taken[db])
            left = [t for t in got if t not in took]
            record["databases"][db] = {"takes": [t for t in got
                                                 if t in took],
                                       "leaves": left}
            if hop.include and not took:
                items.append({
                    "level": "warn", "scope": before, "item": "scope",
                    "detail": f"include takes no table of database {db} -"
                              " it is opened and nothing in it is moved or"
                              " checked  [fix] name its tables in include,"
                              " or leave it out of databases"})
                continue
            items.append({
                "level": "pass", "scope": before, "item": "scope",
                "detail": f"{db}: takes {len(took)} of the source's"
                          f" {len(got)} tables"
                          + (f", leaves {len(left)} out: {self._first(left)}"
                             if left else ", leaves none out")})
        if others:
            items.append({
                "level": "pass", "scope": before, "item": "scope",
                "detail": "the source's databases left out whole: "
                          + self._first(others)})
        pairs = [(db, t) for db, got in every.items() for t in got]
        kept = {(db, t) for db, got in taken.items() for t in got}
        fold = hop.folds_case()
        for kind in ("include", "exclude"):
            for entry in [str(e) for e in getattr(hop, kind, None) or []
                          if str(e).strip()]:
                probe = dataclasses.replace(
                    hop, include=[entry] if kind == "include" else [],
                    exclude=[entry] if kind == "exclude" else [])
                hit = [(db, t) for db, t in pairs if (
                    probe.included if kind == "include" else probe.excluded)(
                        db, *self._table_parts(lister, t))]
                whole_dbs = [d for d in others
                             if name_matches(d, entry, fold)]
                named = [f"{db}.{t}" for db, t in hit] + [
                    f"{d} (the whole database)" for d in whole_dbs]
                record["entries"][f"{kind} {entry}"] = named
                pattern = any(c in entry for c in "*?[")
                later = (", and tables made later that it names: a pattern"
                         " is read again every run" if pattern else "")
                if not named:
                    near = self._nearest(entry, lister, pairs,
                                         list(every) + list(others))
                    items.append({
                        "level": "warn", "scope": before, "item": "scope",
                        "detail": (f"include names {entry!r}, which takes no"
                                   " table on the source - nothing under that"
                                   " name is moved or checked"
                                   if kind == "include" else
                                   f"exclude names {entry!r}, which leaves"
                                   " out no table on the source - a table"
                                   " it was meant to keep off the target"
                                   " would be moved")
                                  + f"{near}  [fix] correct the name or"
                                    " drop it"})
                    continue
                if kind == "include" and hit and not any(
                        x in kept for x in hit):
                    items.append({
                        "level": "warn", "scope": before, "item": "scope",
                        "detail": f"include {entry!r} takes {len(hit)}"
                                  f" tables and exclude takes every one"
                                  f" back: {self._first(named)} - nothing"
                                  " under it is moved or checked  [fix]"
                                  " drop the exclude entry, or this one"})
                    continue
                items.append({
                    "level": "pass", "scope": before, "item": "scope",
                    "detail": f"{kind} {entry!r} "
                              + ("takes" if kind == "include"
                                 else "leaves out")
                              + f" {len(named)}: {self._first(named)}"
                              + later})

        def explained(line):
            items.append({"level": "pass", "scope": before,
                          "item": "scope, in full", "detail": line})
        took = sum(len(v) for v in taken.values())
        decide.record(hop, record, explained, work="the hop's scope",
                      way=f"takes {took} of {len(pairs)} tables",
                      reason="the source's every table read through the"
                             " hop's include and exclude")
        return items

    #: each way a table the hop takes may need one it leaves out: what it
    #: is called, and what accepting it leaves on the target
    CLOSURE = {
        "foreign key": ("foreign keys", "the target holds rows pointing at"
                        " parents it does not have, and the key cannot be"
                        " made or checked there"),
        "view": ("views", "the views are not made on the target, or read"
                 " nothing there"),
        "routine": ("routines", "the routines fail on the target when they"
                    " run"),
        "trigger": ("triggers", "the triggers fail on the target when they"
                    " fire"),
        "sequence": ("sequences", "the defaults draw from a sequence the"
                     " target is not given"),
        "partition": ("partitions", "a partitioned table and its"
                      " partitions are taken apart"),
    }

    def _closure_items(self, lister, every, taken):
        """What the tables the hop takes need of the ones it leaves out,
        before the move, where this engine can list it: a foreign key from
        a taken table to a parent it leaves (with the child's rows that
        hold a key into it, counted up to `ORPHANS_COUNTED` - each points
        at nothing on the target), and a view, routine or trigger over a
        table it leaves, a default drawing from a sequence another table
        owns, a partitioned table taken apart from a partition
        (`dependencies`). Each kind a `warn` naming them, its `[fix]` both
        ways out - the tables to take, or `closure: accept` - and a pass
        saying they were accepted where the hop says so. What this engine
        cannot list is said as not checked, never as clear."""
        import dataclasses

        from .. import decide
        hop = self.hop
        before = "before the move"
        accept = str((hop.options or {}).get("closure") or "").lower() \
            == "accept"
        fks = type(lister).foreign_keys is not Engine.foreign_keys
        kinds = tuple(lister.DEPENDENCY_KINDS)
        unlisted = [self.CLOSURE[k][0] for k in self.CLOSURE
                    if k != "foreign key" and k not in kinds]
        if not fks and not kinds:
            return [{"level": "warn", "scope": before,
                     "item": "dependencies",
                     "detail": "what the tables the hop takes need of the"
                               " ones it leaves out - foreign keys, views,"
                               " routines, triggers, sequences, partitions"
                               " - not checked on this engine"}]
        leaver = dataclasses.replace(hop, include=[])
        out = []
        for db, got in every.items():
            took = set(taken[db])

            def inside(name, db=db):
                return lister.in_scope(db, name)
            crossing = {}
            try:
                for child, cols, parent, _ in (
                        lister.foreign_keys("src", db) if fks else []):
                    if str(child) in took and str(parent) != str(child) \
                            and not inside(parent):
                        crossing.setdefault("foreign key", []).append(
                            (str(parent), str(child), list(cols)))
                for kind, name, holder, needs in (
                        lister.dependencies("src", db, got) if kinds
                        else []):
                    if kind == "partition":
                        if inside(holder) != inside(needs):
                            crossing.setdefault(kind, []).append(
                                (needs if inside(holder) else holder, name,
                                 holder))
                    elif str(needs) != str(holder) and inside(holder) \
                            and not inside(needs):
                        crossing.setdefault(kind, []).append(
                            (str(needs), str(name), str(holder)))
            except Exception as e:  # noqa: BLE001 - said, never as clear
                out.append({"level": "warn", "scope": before,
                            "item": "dependencies",
                            "detail": f"{db}: what the tables the hop takes"
                                      " need of the ones it leaves out could"
                                      " not be listed: "
                                      + (str(e).strip().splitlines()
                                         or [type(e).__name__])[-1][:90]})
                continue
            said = {kind: [self._crossing_said(lister, db, kind, x, i)
                           for i, x in enumerate(found)]
                    for kind, found in crossing.items()}

            def explained(line):
                out.append({"level": "pass", "scope": before,
                            "item": "dependencies, in full",
                            "detail": line})
            for kind, lines in said.items():
                needed = sorted({x[0] for x in crossing[kind]})
                what, then = self.CLOSURE[kind]
                if accept:
                    out.append({"level": "pass", "scope": before,
                                "item": "dependencies",
                                "detail": f"{db}: {what} across the scope,"
                                          " accepted (closure: accept): "
                                          + self._first(lines)})
                    continue
                out.append({
                    "level": "warn", "scope": before, "item": "dependencies",
                    "detail": f"{db}: {what} across the scope: "
                              + self._first(lines)
                              + "  [fix] take the closure: "
                              + self._closure_fix(lister, leaver, db, needed)
                              + f"; or accept it: closure: accept in the"
                                f" hop's options - {then}"})
            if not crossing:
                checked = ["foreign keys"] if fks else []
                checked += [self.CLOSURE[k][0] for k in kinds]
                out.append({"level": "pass", "scope": before,
                            "item": "dependencies",
                            "detail": f"{db}: nothing the hop takes needs a"
                                      " table it leaves out ("
                                      + ", ".join(checked) + ")"})
            decide.record(hop, {"work": f"the closure of {db}",
                                "crossing": said}, explained,
                          work=f"the closure of {db}",
                          way="accepted" if accept and crossing else "",
                          reason=f"{sum(map(len, said.values()))} across"
                                 " the scope")
        if unlisted:
            out.append({"level": "warn", "scope": before,
                        "item": "dependencies",
                        "detail": f"{', '.join(unlisted)} that need a table"
                                  " the hop leaves out - not checked on"
                                  " this engine"})
        return out

    def _crossing_said(self, lister, db, kind, found, i):
        """One dependency across the scope in words: what needs what - a
        foreign key with the child's rows that hold a key into the parent
        left out, counted for the first `ORPHAN_COUNTS`."""
        needs, name, holder = found
        if kind == "foreign key":
            said = f"{name} ({', '.join(holder)}) -> {needs}"
            if i >= self.ORPHAN_COUNTS:
                return said
            try:
                n = lister.rows_holding("src", db, name, holder,
                                        self.ORPHANS_COUNTED)
            except Exception:  # noqa: BLE001 - the key is named, uncounted
                return said + " (its rows could not be counted)"
            if n is None:
                return said
            most = n >= self.ORPHANS_COUNTED
            return (said + f" - {'at least ' if most else ''}{n:,} of its"
                    " rows hold a key into it, each pointing at nothing on"
                    " the target")
        return {"view": f"view {name} reads {needs}",
                "routine": f"{name} names {needs}",
                "trigger": f"trigger {name} on {holder} names {needs}",
                "sequence": f"{holder} draws from {name}, which {needs}"
                            " owns",
                "partition": (f"{holder} is taken and its partition"
                              f" {needs} is not" if needs != holder else
                              f"partition {name} is taken and its table"
                              f" {holder} is not")}[kind]

    def _closure_fix(self, lister, leaver, db, needed):
        """The tables a scope must take for its closure, said as the
        change to make: added to include, or taken out of exclude where
        exclude is what leaves them."""
        dropped = [n for n in needed
                   if leaver.excluded(db, *self._table_parts(lister, n))]
        added = [n for n in needed if n not in dropped]
        parts = []
        if added and self.hop.include:
            parts.append(f"add {', '.join(f'{db}.{n}' for n in added)}"
                         " to include")
        if dropped:
            parts.append(f"take {', '.join(f'{db}.{n}' for n in dropped)}"
                         " out of exclude")
        return " and ".join(parts) or "take " + ", ".join(needed)

    def _mapping_items(self):
        """What the hop's mapping would do, read before the move.

        A mapping is the one piece of a hop whose mistakes are invisible
        afterwards. A rename that collides leaves a target holding one
        table's rows under both names; a rule that matches nothing leaves
        a table exactly where it was while the operator believes it moved.
        Neither shows up as a difference, because the check would be
        comparing the wrong pair in the first place.

        Nothing is said when there is no mapping - which is every hop that
        has not asked for one, and a pre-flight that talks about an empty
        feature is one people learn to scroll past.
        """
        hop = self.hop
        if not getattr(hop, "mapping", None):
            return []
        items = []
        for dst, srcs in sorted(hop.ambiguous_mapping().items()):
            items.append({
                "level": "fail", "scope": "before the move",
                "item": "mapping", "detail":
                    f"{len(srcs)} tables are mapped onto {dst}:"
                    f" {', '.join(srcs)} - whichever is copied second"
                    " overwrites the first, and the check would then"
                    " compare one source against a target holding the"
                    " other  [fix] give them separate names on the target"})
        ids, why = [], None
        try:
            for db in self.databases():
                # named from the database down, so a rule that names its
                # database is matched, and one naming another is not
                ids += [f"{db}.{t}" for t in self.neutral_tables("src", db)]
        except Exception as e:
            why = str(e).splitlines()[-1][:90]
        if why is not None:
            items.append({
                "level": "warn", "scope": "before the move",
                "item": "mapping", "detail":
                    "could not list the source's tables, so whether every"
                    f" mapping rule matches one is unknown: {why}"})
        else:
            for key in hop.unused_mapping(ids):
                items.append({
                    "level": "warn", "scope": "before the move",
                    "item": "mapping", "detail":
                        f"the rule for {key!r} matches nothing on the"
                        " source - that table is not renamed, not filtered,"
                        " and will move exactly as it is"
                        "  [fix] correct the name or drop the rule"})
        if not items:
            items.append({
                "level": "pass", "scope": "before the move",
                "item": "mapping", "detail":
                    f"{len(hop.table_map())} renames and"
                    f" {len((hop.mapping or {}).get('where') or {})} row"
                    " filters, every rule matching a table that exists"})
        return items

    def _preflight_items(self):
        """The predictive deep checks, as `assess` rows.

        One implementation, two places to read it. The mapping is the
        obvious one and the reason matters: a `diff` here is a finding
        about what the move will hit, so it fails the pre-flight the way
        any other `fail` does. An `error` is not a finding, it is a check
        that could not run, and it says so rather than passing.
        """
        level_of = {"diff": "fail", "warn": "warn", "error": "warn"}
        items = self._mapping_items()
        try:
            dbs = self.databases()
        except Exception:
            return items
        for db in dbs:
            for name in self.PREFLIGHT:
                fn = getattr(self, name, None)
                if fn is None:
                    continue
                try:
                    res = fn(db)
                except Exception as e:
                    items.append({
                        "level": "warn", "scope": "before the move",
                        "item": f"{name.strip('_')} on {db}",
                        "detail": f"could not run: {str(e)[:90]}"})
                    continue
                items.append({
                    "level": level_of.get(res.status, "pass"),
                    "scope": "before the move",
                    "item": res.scope,
                    "detail": res.detail
                              + (f"  [fix] {res.fix_hint}"
                                 if res.fix_hint and res.status != "ok"
                                 else "")})
        return items

    def _assess_extra(self):
        """Whatever else this engine knows to look at before a migration."""
        return []

    # ---- the cross-engine contract -------------------------------------
    #
    # Three questions, answered the same way by every engine that can answer
    # them, so a comparison between two different engines is written once
    # rather than once per pair. `migkit.canon` supplies the rendering; this
    # is how an engine is asked to apply it.
    #
    # An engine that cannot take part raises from these rather than returning
    # something empty: "this pair cannot be compared" is a sentence in the
    # report, and an empty table list would read as "compared, nothing wrong".

    # Which rendering family in `migkit.canon` this engine speaks, or "".
    CANON_ENGINE = ""

    # Whether writing to a table that does not exist creates it. A SQL
    # engine refuses, so a missing target table has to be a hard stop before
    # anything is read; a document store makes the collection on the first
    # write, and stopping there would be refusing the thing the operator
    # asked for.
    CREATES_ON_WRITE = False

    # Whether this engine can store "the field is not there" as something
    # other than NULL. Only the schemaless ones can, and a mover carrying a
    # value out of one of them has to know whether the distinction survives.
    EXPRESSES_ABSENT = False

    # Whether reading this engine's data means moving it across a network.
    # A server does; a file on this disk does not, and a report that told an
    # operator their local SQLite file had been shipped over the wire would
    # be wrong in a way that costs trust in every other line.
    OVER_NETWORK = True

    def _no_canon(self, what):
        return NotImplementedError(
            f"{type(self).__name__} cannot {what} for a cross-engine"
            " comparison yet - the pair is unsupported, not clean")

    def neutral_tables(self, side, db):
        """Table identifiers this engine's other neutral methods accept."""
        raise self._no_canon("list tables")

    def scope_tables(self, side, db):
        """Every table a hop's include and exclude entries may name:
        `neutral_tables`, and - where a table holds its rows in tables of
        its own that a move copies one by one (PostgreSQL's partitions) -
        those too, so an entry naming one is said to take it."""
        return self.neutral_tables(side, db)

    def neutral_columns(self, side, db, table):
        """[(column name, declared type)] in the order the server reports."""
        raise self._no_canon("describe columns")

    def neutral_key(self, side, db, table):
        """Columns that order the table for a resumable read, or [].

        An empty list is not a failure. It means the table has no key, which
        `neutral_read` handles by reading it in one pass - slower to restart,
        and still correct, which is the trade the operator would have made.
        """
        raise self._no_canon("find a key")

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """(rows, last_key) - values as Python objects, in key order.

        Objects rather than the canonical text: the text exists so two
        engines can *compare*, and round-tripping a value through it to
        *move* it would throw away precision the target could have held.
        `columns` carries the class of each value so the writer knows what it
        is being handed.

        `after` is the key returned last time. None starts at the beginning,
        and a table with no key returns everything in one call with a
        `last_key` of None - which the caller must treat as "there is no
        second call", not as "there is nothing left".
        """
        raise self._no_canon("read rows")

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        """{canonical key: row} for the rows carrying these key values.

        Reading by key rather than walking the two sides in step is what
        makes a cross-engine drilldown possible. Two engines do not agree on
        the order of a text key - one collation difference is enough - so a
        merge over two ordered reads would report rows missing on one side
        and extra on the other with neither being true. Asking each side for
        the same keys asks a question that has one answer.

        The mapping is keyed by the canonical text of the key columns, not by
        the values themselves, because the two engines may hand the same key
        back as different Python objects (a Decimal here, an int there) and
        the point is to line the two sides up.
        """
        raise self._no_canon("read rows by key")

    def neutral_rows_matching(self, side, db, table, columns, match, values):
        """[row] of `table` whose `match` columns hold one of `values` - the
        rows that refer to others, for a copy that follows them
        (`subset`). A read by key cannot answer it: several rows share one
        value, and its answer keeps one row a key. Here the table is read
        through in key order and the rows that match are kept; an engine
        that can ask its server for them overrides this. `columns` holds
        the `match` columns, whose classes render each side alike."""
        from .. import canon
        if not match or not values:
            return []
        classes = dict(columns)
        names = [n for n, _ in columns]
        at = [names.index(m) for m in match]
        want = {tuple(canon.render_value(classes[m], v)
                      for m, v in zip(match, value)) for value in values}
        out, after = [], None
        while True:
            rows, last = self.neutral_read(side, db, table, columns, after,
                                           5000)
            for r in rows:
                if tuple(canon.render_value(classes[match[i]], r[j])
                         for i, j in enumerate(at)) in want:
                    out.append(r)
            if last is None or not rows:
                return out
            after = last

    def _drill_path(self, db, name, kind):
        return self.hop.report_dir(db) / f"data-{name}.{kind}"

    def _write_drill(self, db, name, **kinds):
        """The rows behind the counts, one per line, in the estate's file
        names - which is what makes a repair possible at all.

        A run that finds nothing removes the file rather than leaving the
        previous run's list behind for `sync` to act on.
        """
        for kind, items in kinds.items():
            path = self._drill_path(db, name, kind)
            if items:
                path.write_text("\n".join(str(i) for i in items) + "\n")
            elif path.exists():
                path.unlink()

    def _read_drill(self, db, name, kind):
        path = self._drill_path(db, name, kind)
        if not path.exists():
            return []
        return [l for l in path.read_text().splitlines() if l]

    #: how many rows a drilldown walks per side before it stops and says so
    DRILL_CAP = 20000

    def _row_text(self, columns, row):
        from .. import canon
        return tuple(canon.render_value(cls, value)
                     for (_, cls), value in zip(columns, row))

    def _column_rules(self, db, table):
        """The hop's `mapping.columns` for a table, read once: {} where it
        maps nothing there."""
        known = self.__dict__.setdefault("_column_rules_of", {})
        at = (db, str(table))
        if at not in known:
            rules = self.hop.column_rules(db, *self.hop.parts_of(table)) or {}
            known[at] = rules if (rules.get("keep") or rules.get("drop")
                                  or rules.get("rename")) else {}
        return known[at]

    def _mapped_types(self, db, table, src_types):
        """The source's columns as the target is meant to have them:
        `mapping.columns` applied - kept or dropped, and renamed.

        Returns ({target name: source type}, {target name: source name}).
        One place, so the copy, the comparison and the schema check cannot
        disagree about which source column lands where.
        """
        rules = self._column_rules(db, table)
        if not rules:
            # no mapping for the table: every column as it is - asked once
            # a change by the tail, and building the same dict again was a
            # second of 7 for 320,000 changes
            return dict(src_types), {n: n for n in src_types}
        keep, drop = set(rules.get("keep") or []), set(rules.get("drop") or [])
        rename = rules.get("rename") or {}
        virtual, back = {}, {}
        for name, typ in src_types.items():
            if (keep and name not in keep) or name in drop:
                continue
            target = rename.get(name, name)
            virtual[target], back[target] = typ, name
        return virtual, back

    def _comparable_columns(self, db, src_engine, src_table,
                            dst_engine, dst_table, how=None):
        """What to hash on each side, and everything that is not hashable.

        Returns (src_cols, dst_cols, notes). The two column lists are the
        same names in the same order - **sorted by name**, not by the order
        each server happens to report, because the row text is positional and
        two servers agreeing on a set of columns says nothing about the order
        they list them in.

        `how`, a dict where given, is filled with every column of the pair
        by the target's name: ("exactly" | "by value" | "not at all", why)
        - what a verdict says about how much of a row it compared
        (`verdict.compared_clause`, problems D15).
        """
        from .. import canon

        def declared(engine, side, table):
            got = {}
            for name, typ in engine.neutral_columns(side, db, table):
                got[name] = engine.canonical_type(side, db, typ)
            return got
        src_declared = declared(src_engine, "src", src_table)
        src_types, back = self._mapped_types(db, src_table, src_declared)
        dst_types = declared(dst_engine, "dst", dst_table)
        got = self._classify_columns(src_engine, src_types,
                                     dst_engine, dst_types)
        notes = []
        if got["only_src"]:
            notes.append(f"columns only on the source, not compared:"
                         f" {', '.join(got['only_src'])}")
        if got["only_dst"]:
            notes.append(f"columns only on the target, not compared:"
                         f" {', '.join(got['only_dst'])}")
        notes += [f"{name}: {why}" for name, why in got["unreadable"]]
        accepted = getattr(self, "_accepted", None)
        if accepted:
            # a MongoDB field whose type the hop lets change on the way
            # (`accept_changes`) is compared by its value alone
            got["pairs"] = [
                (n, canon.by_value(s), canon.by_value(d))
                if accepted(src_table, back.get(n, n)) else (n, s, d)
                for n, s, d in got["pairs"]]
        if how is not None:
            for n, s, d in got["pairs"]:
                kind, why = got["how"][n]
                if accepted and accepted(src_table, back.get(n, n)):
                    kind, why = "by value", (
                        f"compared as {canon.as_words(s)}, its type let"
                        " change on the way (accept_changes)")
                how[n] = (kind, why)
            for n in got["only_src"]:
                how[n] = ("not at all", "only on the source")
            for n in got["only_dst"]:
                how[n] = ("not at all", "only on the target")
            for n, why in got["unreadable"]:
                how[n] = ("not at all", why)
            for n in sorted(set(src_declared) - set(back.values())):
                how.setdefault(n, ("not at all", "left out by the hop's"
                                                 " column mapping"))
        # read by the source's own name, written and compared by the target's
        src_cols = [(back.get(n, n), s) for n, s, _ in got["pairs"]]
        dst_cols = [(n, d) for n, _, d in got["pairs"]]
        return src_cols, dst_cols, notes

    @staticmethod
    def _classify_columns(src_engine, src_types, dst_engine, dst_types):
        """Every column of the pair, sorted into what can be said about it.

        Split out because two callers need the same decision and only one of
        them wants it as prose: the row comparison turns it into a footnote,
        and the schema check turns it into a verdict. Deciding it twice is
        how the footnote and the verdict come to disagree.

        `pairs` are sorted by name rather than by the order either server
        reports, because the row text is positional and two servers agreeing
        on a set of columns says nothing about the order they list them in.
        """
        from .. import canon
        out = {"pairs": [], "unreadable": [],
               "only_src": sorted(set(src_types) - set(dst_types)),
               "only_dst": sorted(set(dst_types) - set(src_types))}
        same = (src_engine.CANON_ENGINE == dst_engine.CANON_ENGINE
                and canon.renders(src_engine.CANON_ENGINE))
        for name in sorted(set(src_types) & set(dst_types)):
            scls, swhy = canon.comparable(src_engine.CANON_ENGINE,
                                          src_types[name])
            dcls, dwhy = canon.comparable(dst_engine.CANON_ENGINE,
                                          dst_types[name])
            if (not scls or not dcls) and same and \
                    str(src_types[name]).lower() == \
                    str(dst_types[name]).lower():
                # one engine on both sides, one type: its own text of the
                # value is the same text on both - measured, an interval
                # and a range left out of a same-engine comparison hid a
                # changed value behind "every compared column equal"
                scls = dcls = canon.OWN
            typed = canon.bson_pair(src_engine.CANON_ENGINE, src_types[name],
                                    dst_engine.CANON_ENGINE, dst_types[name])
            if typed is not None:
                # MongoDB on one side: each value with its BSON type, the
                # other side's as MongoDB holds that column's
                if not all(typed):
                    eng, typ = ((dst_engine.CANON_ENGINE, dst_types[name])
                                if src_engine.CANON_ENGINE == "mongodb"
                                else (src_engine.CANON_ENGINE,
                                      src_types[name]))
                    out["unreadable"].append((
                        name, f"{eng} type {typ!r} has no BSON type, so"
                        " MongoDB holds none of its values to compare"))
                    continue
                out["pairs"].append((name,) + tuple(typed))
                continue
            if not scls or not dcls:
                out["unreadable"].append((name, swhy or dwhy))
                continue
            if src_engine.CANON_ENGINE == dst_engine.CANON_ENGINE \
                    == "mongodb":
                # two MongoDB sides: each value with its BSON type, so a
                # 32-bit 5 does not meet a 64-bit one (`bsontype`)
                out["pairs"].append((name, canon.BSON, canon.BSON))
                continue
            if scls == dcls and scls in canon.FOLDED_HERE and same and \
                    str(src_types[name]).lower() == \
                    str(dst_types[name]).lower():
                # one engine and one type: its own text of the value is one
                # text on both sides, rendered where the rows are
                scls = dcls = (canon.JSON_ONE if scls == "json"
                               else canon.OWN)
            if scls == dcls == "decimal":
                scls = dcls = canon.decimal_class(
                    src_engine.CANON_ENGINE, src_types[name],
                    dst_engine.CANON_ENGINE, dst_types[name])
            if {scls, dcls} in ({"xml", "text"}, {"uuid", "text"},
                                {"inet", "text"}, {"interval", "text"},
                                {"vector", "text"}):
                # a document, a UUID or an address kept as text on one
                # side: compared as what it is, which is the same text
                # where it was the same
                scls = dcls = (scls if scls != "text" else dcls)
            if {scls, dcls} == {"text", "bytes"}:
                # a type that holds bytes as readily as text reads as
                # bytes beside a column that is bytes: rendered as text it
                # is a difference in every row, or no rendering at all
                eng, typ = ((src_engine, src_types[name]) if scls == "text"
                            else (dst_engine, dst_types[name]))
                base = str(typ).split("(")[0].strip().lower()
                if base in eng.TEXT_HOLDS_BYTES:
                    scls = dcls = "bytes"
            out["pairs"].append((name, scls, dcls))
        out["src_types"], out["dst_types"] = src_types, dst_types
        # how each compared column is compared, exactly or by value, and
        # as what (problems D15): a verdict says it for every column
        out["how"] = {name: canon.how_compared(
            src_engine.CANON_ENGINE, src_types[name], s,
            dst_engine.CANON_ENGINE, dst_types[name], d)
            for name, s, d in out["pairs"]}
        return out

    # --- the second reading's part in a drilldown (backlog 0f) --------------
    # Where the independent reader is installed and reads the pair, a table
    # whose digest differs is narrowed to the groups whose aggregates differ
    # before its rows are walked, and the rows migkit names are read again
    # the other way. Where it is not, nothing here changes anything.

    def _reader_names(self):
        """(source engine, target engine) in the second reader's terms."""
        return self.CANON_ENGINE, self.CANON_ENGINE

    def _reader_engine(self, side):
        """The engine that reads one side: `self`, within one engine."""
        return self

    def _reader_table(self, side, db, table):
        """The table as the reader names it on one side: `schema.table`."""
        return str(table)

    def _reader_where(self, db, src_t):
        """The hop's row filter on each side, in each side's SQL."""
        pred = self.hop.row_filter(db, *self.hop.parts_of(src_t))
        return pred, pred

    def _can_read_twice(self):
        from .. import second_reader as sr
        return bool(sr.interpreter()) and all(
            n in sr.READER_TYPES for n in self._reader_names())

    def group_candidates(self, side, db, table):
        """[(column, neutral class, estimated distinct values)] from the
        statistics the server keeps, for the second reading's groups;
        nothing where an engine keeps none it can say."""
        return []

    def _group_for(self, db, src_t, key, rows):
        """(column, class, groups) the second reader's aggregates may split
        a differing table by, or None."""
        from .. import second_reader as sr
        names = self._reader_names()
        try:
            cands = self._reader_engine("src").group_candidates("src", db,
                                                                src_t)
        except Exception:  # noqa: BLE001 - no statistics, no groups
            return None
        return sr.group_column(cands, key, names[0] == names[1], rows)

    def _narrowed_walk(self, db, src_t, dst_t, key, walk, same, whole,
                       rows=None):
        """(missing, extra, changed) of a differing table, found the way
        chosen for it (`second_reader.WALKS`, measured per hop): only the
        groups the second reading says differ - and the rest too unless
        migkit's own digest proves it equal - or `whole()`, the engine's
        own walk of every row.

        `walk(src_where, dst_where)` and `same(src_where, dst_where)` are
        the engine's own row compare and digest over a predicate; `rows`
        how many the table holds. Where no second reading can be had the
        table is walked whole as it always was, and nothing is said or
        kept. What was done is kept for the finding (`_narrowed_note`).
        """
        from .. import decide
        from .. import second_reader as sr
        if not self._can_read_twice():
            return whole()
        notes = self.__dict__.setdefault("_narrowed", {})
        work = f"{db}.{src_t}"
        facts = decide.Facts(
            {"reader": True},
            **{"group column": lambda: self._group_for(db, src_t, key,
                                                       rows)})
        costs = decide.HopCosts(self.hop, "verify passes/own ")
        units = max(int(rows or 0), 1)

        def run(choice):
            began = time.monotonic()
            if choice.path != "groups":
                found = whole()
                costs.saw("walk", units, time.monotonic() - began)
                notes[(db, str(src_t))] = (None, "every row walked:"
                                                 f" {choice.reason}")
                return found
            got = sr.narrow(self.hop, db, self._reader_names(),
                            (self._reader_table("src", db, src_t),
                             self._reader_table("dst", db, dst_t)),
                            facts["group column"],
                            where=self._reader_where(db, src_t),
                            keep=self.hop.report_dir(db))
            if not isinstance(got, sr.Narrowing):
                # the groups could not be read the second way: nothing was
                # decided by them, and the table's own walk is next
                raise decide.Blocked(got)
            missing, extra, changed, said, only_inside = \
                sr.walk_narrowed(got, walk, same,
                                 (self._reader_engine("src"),
                                  self._reader_engine("dst")))
            costs.saw("groups", units, time.monotonic() - began)
            notes[(db, str(src_t))] = (got if only_inside else None, said)
            return missing, extra, changed
        # a way that cannot run here switches by itself (`decide.attempt`);
        # a difference found is the answer and is returned, never switched
        # from
        return decide.attempt(work, sr.WALKS, run, facts, costs=costs,
                              switch=decide.only_blocked)[1]

    def _second_reading(self, db, src_t, dst_t, key, key_classes, mine,
                        rows, columns=(), fixed=()):
        """The second reading of a differing table's rows, by the way the
        verify passes choose for it (`leaves.PASSES`, its `second` part):
        the second reader's row reading held key for key to what migkit
        named (`_read_rows_twice`), or the bisecting reader's own count -
        or None where there is no second reading to be had, or the table
        was read twice already on the way to its rows."""
        from .. import decide, leaves
        from .. import second_reader as sr
        if (db, str(src_t)) in self.__dict__.get("_read_twice", ()):
            return None
        names = self._reader_names()
        part = next(p for p in leaves.PASSES if p.name == "second")
        facts = decide.Facts(
            {"two readings": bool(sr.asked(self.hop)), "disagreed": True,
             "localizer": None},
            **{"row hash reader": self._can_read_twice,
               "bisection reader": lambda: bool(key) and
               leaves.bisection_reader(names)})
        got = decide.climb(f"{db}.{src_t}", part.rungs, facts)
        if got.rung is None or got.rung.name == "none":
            return None
        self.__dict__.setdefault("_read_twice", set()).add((db, str(src_t)))
        if got.rung.name == "row hash":
            return self._read_rows_twice(db, src_t, dst_t, key, key_classes,
                                         mine, rows, columns, fixed)
        said = leaves.bisection_reading(
            self.hop, names, db, self._reader_table("src", db, src_t),
            self._reader_table("dst", db, dst_t), key,
            self._reader_where(db, src_t)[0], rows)
        return Result(sr.CHECK,
                      f"{db}.{self._reader_table('src', db, src_t)} rows",
                      "diff", said.lstrip("; "))

    def _narrowed_note(self, db, src_t):
        """What narrowing did for this table, or ''."""
        return (self.__dict__.get("_narrowed") or {}).get(
            (db, str(src_t)), (None, ""))[1]

    def _read_rows_twice(self, db, src_t, dst_t, key, key_classes, mine,
                         rows, columns=(), fixed=()):
        """The rows migkit named in a differing table, held against the
        second reader's own row reading of it - or None where no second
        reading can be had here.

        `mine` is the keys migkit named, each a tuple of its key's values
        as text; `rows` how many rows the table holds. Where the table was
        narrowed and every difference proved inside its groups, only those
        groups are read; otherwise the table, unless it holds more rows
        than a second row reading should pull (`ROWS_MOST`), which is said.
        """
        from .. import second_reader as sr
        if not self._can_read_twice() or not key:
            return None
        names = self._reader_names()
        same = names[0] == names[1]
        tables = (self._reader_table("src", db, src_t),
                  self._reader_table("dst", db, dst_t))
        # named as the rest of the second reading names it (`check_second`)
        scope = f"{db}.{tables[0]} rows"
        where = self._reader_where(db, src_t)
        narrowing = ((self.__dict__.get("_narrowed") or {}).get(
            (db, str(src_t))) or (None, ""))[0]
        if narrowing is not None:
            where = tuple(narrowing.inside(self._reader_engine(side), w)
                          for side, w in zip(("src", "dst"), where))
            rows = rows * (len(narrowing.values) + int(narrowing.nulls)) \
                // max(1, narrowing.groups)
        if rows > sr.ROWS_MOST:
            return Result(sr.CHECK, scope, "skip",
                          f"{rows:,} rows are more than a second row by row"
                          f" reading pulls ({sr.ROWS_MOST:,}), so the rows"
                          " named were read one way only")
        ask = sr.asked(self.hop)
        label = f"row:{tables[0]}"
        v = sr.rows(label, [f"{tables[0]}={tables[1]}"], key, columns,
                    same_engine=same, where=where,
                    case_insensitive=ask.get("case_insensitive", False),
                    fixed=fixed)
        if v is None:
            return Result(sr.CHECK, scope, "skip",
                          "no column of it but floats and fixed-width text"
                          " can be read value by value across these two"
                          " engines")
        answer = sr.run({
            "source": sr.connection(names[0], self.hop.source, db),
            "target": sr.connection(names[1], self.hop.target,
                                    self.hop.target_db(db)),
            "validations": [v]}, keep=self.hop.report_dir(db))
        ran = bool(answer.get("ok")) and sr._ran(answer, label, tables[0]) \
            and not sr._broken(answer, label, tables[0])
        by_value = all(c in ("integer", "text") for c in key_classes)
        return sr.compared(db, tables[0], mine,
                           sr.failing_keys(answer, label, tables[0]), ran,
                           same, by_value)

    def _explained(self, db, table, key, named, where):
        """A differing set small enough to show, explained column by
        column: which rows one side lacks, and for each column how many
        rows differ in it and how, written to `data-<table>.explained` and
        said in a clause. '' for a set too large or none, or where the rows
        cannot be read back: the finding stands without it.

        `named` is the keys the drilldown named, `where` a predicate
        selecting exactly those rows on either side.
        """
        from .. import second_reader as sr
        if (not named or len(named) > sr.EXPLAIN_MOST or not where
                or not hasattr(self, "fetch_sample_df")):
            return ""
        try:
            got = [self.fetch_sample_df(side, db, table, len(named) + 1,
                                        where=where)
                   for side in ("src", "dst")]
            lines = sr.explain(got[0], got[1], key, self.hop, table)
        except Exception:  # noqa: BLE001 - an explanation, not the finding
            return ""
        leaf = str(table).split(".")[-1]
        path = self.hop.report_dir(db) / f"data-{leaf}.explained"
        path.write_text("\n".join(lines) + "\n")
        said = [ln.split(" - ")[0] for ln in lines[1:]]
        return (f"; {table} column by column: " + "; ".join(said[:4])
                + (" ..." if len(said) > 4 else "") + f" ({path.name})")

    def _drill_rows(self, db, name, src_engine, src_t, src_cols,
                    dst_engine, dst_t, dst_cols, wheres=(None, None)):
        found = self._localise(db, name, src_engine, src_t, src_cols,
                               dst_engine, dst_t, dst_cols, wheres)
        if isinstance(found, str):
            return found
        return self._drill_clause(db, name, *found)

    def _localise(self, db, name, src_engine, src_t, src_cols, dst_engine,
                  dst_t, dst_cols, wheres=(None, None)):
        """(missing, changed, extra, capped) - which rows differ - or the
        clause saying why they cannot be told.

        A digest says a table is wrong; this says which rows, which is what
        a repair needs and what an operator reads first. Both sides are
        asked for the same keys rather than walked in step: two engines do
        not agree on the order of a text key, so a merge over two ordered
        reads would invent missing and extra rows in equal numbers.

        The two engines are arguments rather than `self` because the same
        walk serves a hop between two different engines and a hop within
        one - for the second, both are the same object. A pair that cannot
        be lined up says why instead of writing an empty list, which `sync`
        would read as nothing to do.
        """
        names = [n for n, _ in src_cols]
        try:
            key = list(src_engine.neutral_key("src", db, src_t))
            dst_key = list(dst_engine.neutral_key("dst", db, dst_t))
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

        def scope(side):
            # the hop's row filter in that side's SQL: a row outside it is
            # not one the two sides are meant to agree on
            w = wheres[0] if side == "src" else wheres[1]
            return {"where": w} if w else {}

        def walk(engine, side, table, cols, other, other_side, other_table,
                 other_cols, on_absent, on_differs):
            seen = 0
            after = None
            while seen < self.DRILL_CAP:
                got, after = engine.neutral_read(side, db, table, cols, after,
                                                 min(1000,
                                                     self.DRILL_CAP - seen),
                                                 **scope(side))
                if not got:
                    return False
                seen += len(got)
                raw = [tuple(row[i] for i in at) for row in got]
                theirs = other.neutral_rows_by_key(other_side, db, other_table,
                                                   other_cols, key, raw,
                                                   **scope(other_side))
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
            if walk(src_engine, "src", src_t, src_cols, dst_engine,
                    "dst", dst_t, dst_cols, missing.append, changed.append):
                capped.append("source")
            if walk(dst_engine, "dst", dst_t, dst_cols, src_engine,
                    "src", src_t, src_cols, extra.append, None):
                capped.append("target")
        except Exception as e:
            return f"; rows not localised: {str(e).splitlines()[-1][:90]}"

        return missing, changed, extra, capped

    def _drill_clause(self, db, name, missing, changed, extra, capped,
                      how=""):
        """The rows found, written to the estate's files, as a clause."""
        import json
        # counted, for the finding that names the table (`Result.rows_apart`)
        # - exact where every row was walked
        self.__dict__.setdefault("_apart", {})[(db, name)] = {
            "missing": len(missing), "extra": len(extra),
            "changed": len(changed), "exact": not capped}
        # the leaves an earlier check kept go with its lists: the leaves'
        # own drill keeps them again after this (`leaves.kept_leaves`)
        self._write_drill(
            db, name,
            missing=[json.dumps(list(k)) for k in missing],
            changed=[json.dumps(list(k)) for k in changed],
            extra=[json.dumps(list(k)) for k in extra], leaves=[])
        parts = []
        for label, found in (("missing on the target", missing),
                             ("with different values", changed),
                             ("only on the target", extra)):
            if found:
                shown = ", ".join(self._key_text(k, self.hop, name)
                                  for k in found[:4])
                parts.append(f"{len(found)} {label} ({shown}"
                             + (" ..." if len(found) > 4 else "") + ")")
        if parts and not capped:
            # every row walked: the counts are exact, which is what a hop
            # that keeps some changes off the target is held to
            # (`verdict.hold`)
            parts.append(f"missing={len(missing)} extra={len(extra)}"
                         f" changed={len(changed)}")
        blind = sum(1 for group in (missing, changed, extra)
                    for k in group if any(part is None for part in k))
        if blind:
            parts.append(
                f"{blind} of those rows have NULL in the key, so they cannot"
                " be addressed on the other side - `migkit sync` will not"
                " repair them and a re-check will keep reporting them")
        clause = "; " + ("; ".join(parts) if parts else
                         "no row differs, so the difference is in a column"
                         " neither side could compare")
        if capped:
            clause += (f"; stopped after {self.DRILL_CAP:,} rows on the "
                       + " and ".join(capped) + ", so there may be more")
        return clause + how

    @staticmethod
    def _key_text(key, hop=None, table=None):
        """A row's key, for a person to read - masked where the hop's
        `mask` says (`masking.keys`).

        `"/".join(key)` was it, and a key part can be `None`: SQLite accepts
        a NULL into a non-INTEGER `PRIMARY KEY` - measured, `create table a
        (id text primary key)` takes `insert (id, v) values (NULL, 'x')` and
        reports `id is null` - so the drilldown crashed with `TypeError:
        sequence item 0: expected str instance, NoneType found` and took the
        whole data check down with it.
        """
        from .. import bsontype, masking
        hide = hop is not None and masking.keys(hop, table)
        # a key compared with its BSON type is shown as its value
        return "/".join("NULL" if part is None
                        else masking.token(hop, part) if hide
                        else str(bsontype.shown(part)) for part in key)

    def _apply_rows(self, db, name, src_engine, src_t, src_cols,
                    dst_engine, dst_t, dst_cols, found=None):
        """Carry the listed rows across, then remove the ones that should
        not exist.

        The keys come back out of the drilldown as canonical text and are
        turned into values with `canon.from_text`, the same way a change
        record from a logical decoder is - the text is the one form both
        sides agree on, and re-deriving the value per side is what lets a
        key written by one engine address a row in the other.

        Writes first and deletions last, so a repair cut off in the middle
        leaves rows that should not be there - which the next check names -
        rather than a hole nothing looks for.

        `found` is the rows to repair, {missing, changed, extra: [key as
        canonical text]}, where the engine's check keeps them in a form of
        its own; the drilldown's files otherwise.
        """
        import json

        from .. import canon
        if found is None:
            found = self._drill_tables(db).get(name, {})
        found = self._repairable(db, src_t, self._only_named(found))
        if not any(found.get(k) for k in ("missing", "changed", "extra")):
            return
        key = list(src_engine.neutral_key("src", db, src_t))
        cls = {n: c for n, c in src_cols}
        if not key or any(k not in cls for k in key):
            raise SystemExit(f"{name} has no key among the compared columns,"
                             " so a row cannot be addressed on both sides")

        def values(key_text):
            return tuple(canon.from_text(cls[k], t)
                         for k, t in zip(key, key_text))
        undo_dir = self.hop.report_dir(db) / "undo"
        undo_dir.mkdir(parents=True, exist_ok=True)
        undo = undo_dir / f"{name}.rows.jsonl"

        def remember(handle, key_texts):
            if not key_texts:
                return
            held = dst_engine.neutral_rows_by_key(
                "dst", db, dst_t, dst_cols, key,
                [values(k) for k in key_texts])
            for key_text, row in held.items():
                handle.write(json.dumps({
                    "table": name, "key": list(key_text),
                    "row": dict(zip([n for n, _ in dst_cols],
                                    self._row_text(dst_cols, row)))}) + "\n")

        send = found.get("missing", []) + found.get("changed", [])
        drop = found.get("extra", [])
        # A key part that is NULL addresses nothing: `where id = NULL` is
        # never true, so the row would be reported as carried and nothing
        # would happen to it. Measured - `canon.from_text(str, None)` comes
        # back as `None` and goes straight into the predicate. Refused here
        # rather than attempted, because a repair that silently does nothing
        # is the one failure this whole path exists to avoid.
        unaddressable = [k for k in send + drop
                         if any(part is None for part in k)]
        if unaddressable:
            raise SystemExit(
                f"{len(unaddressable)} rows of {name} have NULL in the key"
                " (" + ", ".join(self._key_text(k, self.hop, name)
                                 for k in unaddressable[:4])
                + (" ..." if len(unaddressable) > 4 else "")
                + ") - a row with no key cannot be found on the other side,"
                  " so repairing it would report success and change nothing."
                  " Give those rows a key, or key the hop on a column that is"
                  " always filled")
        # a column with no canonical rendering was left out of the
        # comparison, and writing the row without it would put a row on the
        # target that is missing values the source has - measured on a
        # sqlite `numeric` column, whose rows came back with the column set
        # to NULL and the table still differing afterwards. Deletions are
        # unaffected: removing a row the source does not have needs no
        # rendering of anything.
        if send:
            whole = {n for n, _ in dst_engine.neutral_columns("dst", db,
                                                              dst_t)}
            dropped = sorted(whole - {n for n, _ in dst_cols})
            if dropped:
                raise SystemExit(
                    f"{name} cannot be repaired row by row: "
                    + ", ".join(dropped) + " could not be rendered, and a"
                    " row written without those columns would be a row the"
                    " source does not have. Nothing has been written."
                    " `migkit move` can re-copy the table, which carries"
                    " every column whether migkit can compare it or not")
        changed = list(found.get("changed", []))
        src_names = [n for n, _ in src_cols]
        dkey = [dst_cols[src_names.index(k)][0] for k in key]
        rows = (src_engine.neutral_rows_by_key(
            "src", db, src_t, src_cols, key, [values(k) for k in send])
            if send else {})
        if changed and self.newer_wins_column():
            # the target's row is kept where the column the hop names is
            # later there (`newer_wins`), as on PostgreSQL and MySQL
            there = dst_engine.neutral_rows_by_key(
                "dst", db, dst_t, dst_cols, dkey,
                [values(k) for k in changed])
            changed, kept = self._newer_neutral(db, name, dst_engine, dst_t,
                                                src_cols, dst_cols, changed,
                                                rows, there)
            if kept:
                path = self.hop.report_dir(db) / f"data-{name}.kept-newer"
                path.write_text("".join(json.dumps(list(k)) + "\n"
                                        for k in kept))
                keep = {tuple(k) for k in kept}
                send = [k for k in send if tuple(k) not in keep]
                rows = {k: r for k, r in rows.items() if k not in keep}
        with undo.open("a") as handle:
            remember(handle, changed)
            remember(handle, drop)
        # the statement-level record, written whole before anything lands
        # (`repairlog`): what each statement puts where, and what was there
        from .. import repairlog
        # the source's rows in the source's classes: the text the target's
        # own is held to once they have landed, which is what the check
        # compares
        after = dict(zip(rows, repairlog.canon_rows(src_cols,
                                                    list(rows.values()))))
        with repairlog.planned(
                undo_dir, repairlog.CanonIO(dst_engine, "dst", db, dst_t,
                                            dst_cols, dkey),
                after, send, (), drop) as record:
            writes = sum(1 for k in send if tuple(k) in after)
            first_delete = writes + 1
            if rows:
                dst_engine.neutral_write("dst", db, dst_t, dst_cols,
                                         list(rows.values()))
                record.applied(writes)
            # a key part of a class with a form of its own on the target
            # (an ObjectId kept in a `jsonb` key) addresses the row in that
            # form
            into = {s: d for (s, _), (_, d) in zip(src_cols, dst_cols)
                    if canon.has_own_form(d)}
            for i, key_text in enumerate(drop):
                dst_engine._apply_delete("dst", db, dst_t, {
                    k: canon.write_value(dst_engine.CANON_ENGINE, into[k], v)
                    if k in into else v
                    for k, v in zip(key, values(key_text))})
                record.applied(first_delete + i)

    def _kept_by_hop(self, db, table):
        """The kinds of row (`missing`, `extra`, `changed`) the hop keeps
        on the target for this table by the changes it does not carry
        (`verdict.kept_kinds`): a repair never removes a row the source
        deleted where deletes are not carried, never adds one it inserted
        where inserts are not, and never overwrites one it updated where
        updates are not."""
        from .. import verdict
        return verdict.kept_kinds(self.hop, db, table)

    def _repairable(self, db, table, found):
        """`found` ({kind: [keys]}) without the kinds the hop keeps
        (`_kept_by_hop`); as it is for a hop that carries every change."""
        kept = self._kept_by_hop(db, table)
        if not kept or not found:
            return found
        return {k: ([] if k in kept else v) for k, v in found.items()}

    @staticmethod
    def _kept_said(found, kept_found):
        """What a repair plan says of the rows it leaves because the hop
        keeps them, or ''."""
        from .. import verdict
        left = [(kind, len(found.get(kind) or ()))
                for _, kind, _, _ in verdict.LEFT_OUT
                if len(found.get(kind) or ()) and
                not (kept_found.get(kind) or ())]
        return "; ".join(
            f"{verdict._rows(n)} {words} left as they are - {why}"
            for kind, n in left for _, k, words, why in verdict.LEFT_OUT
            if k == kind)

    def _only_named(self, found):
        """`found` ({kind: [key as canonical text]}) narrowed to the keys a
        repair by spans leaves to be repaired by key (`repairways`); as it
        is otherwise."""
        only = self.__dict__.get("_repair_only")
        if only is None:
            return found
        return {k: [x for x in v if tuple(x) in only]
                for k, v in found.items()}

    def _newer_neutral(self, db, name, dst_engine, dst_t, src_cols, dst_cols,
                       changed, src_rows, dst_rows):
        """`_keep_the_newer` over rows read through the neutral contract:
        (overwrite, keep). The column is found by the target's name, else
        the source's, among the columns both sides render; each side's
        value is taken as its class reads it back from canonical text, so
        an instant on one engine and a wall clock in UTC on another
        compare as the times they are."""
        from .. import canon
        col = self.newer_wins_column()
        dnames = [n for n, _ in dst_cols]
        snames = [n for n, _ in src_cols]
        at = dnames.index(col) if col in dnames else (
            snames.index(col) if col in snames else None)
        if at is None:
            there = {n for n, _ in dst_engine.neutral_columns("dst", db,
                                                              dst_t)}
            if col in there:
                raise SystemExit(
                    f"{name}: newer_wins names {col}, and its values cannot"
                    " be read alike on both sides, so which row is newer"
                    " cannot be told. Nothing was repaired on this table.")
            raise SystemExit(
                f"{name}: newer_wins names {col}, and it is not on both"
                " sides, so which row is newer cannot be told. Nothing was"
                " repaired on this table.")

        def read(side, keys):
            got, cls = ((src_rows, src_cols[at][1]) if side == "src"
                        else (dst_rows, dst_cols[at][1]))
            out = {}
            for k in keys:
                row = got.get(tuple(k))
                if row is None or row[at] is None:
                    continue
                out[self._key_id(k)] = canon.from_text(
                    cls, canon.render_value(cls, row[at]))
            return out
        return self._keep_the_newer(name, changed, read)

    def repair_io(self, db, head):
        """How a repair record's rows are read and put back on this hop's
        target (`repairlog`): through the neutral contract of the engine
        the target is - this one, or the pair's target engine."""
        from .. import repairlog
        eng = getattr(self, "dst_engine", None) or self
        if head.get("form") == "snapshot":
            return repairlog.TableIO(eng, db, head["table"], head["columns"],
                                     head["key"],
                                     self.hop.report_dir(db) / "undo")
        if head.get("form") != "canon":
            raise SystemExit(f"a repair record of form {head.get('form')!r}"
                             f" cannot be put back by {self.CANON_ENGINE}")
        return repairlog.CanonIO(eng, "dst", db, head["table"],
                                 head["columns"], head["key"])

    # --- the ways a table's rows are repaired (`repairways`) ---------------

    #: whether rows read through the neutral contract are put back on this
    #: target from their canonical text (`repairlog.CanonIO`, `TableIO`) -
    #: what a span or a table copied again is undone by; where they are
    #: not, a table is repaired by the engine's own rows way alone
    ROWS_PUT_BACK = True
    #: whether a span of a table copied again lands in one transaction on
    #: this target (`repair_range`)
    ONE_TRANSACTION_A_RANGE = False

    def repair_found(self, db, table):
        """{missing, changed, extra: [key as canonical text]} the last check
        named for `table` - {} where it named none - or None where this
        engine's drilldown cannot be read so. Never the rows the hop keeps
        by the changes it does not carry (`_repairable`)."""
        return self._repairable(db, table,
                                self._drill_tables(db).get(table, {}))

    def _leaves_kept(self, db):
        """The tables the last check kept differing leaves for
        (`leaves.kept_leaves`)."""
        return sorted(p.name[len("data-"):-len(".leaves")] for p in
                      self.hop.report_dir(db).glob("data-*.leaves"))

    def repair_leaves(self, db, table):
        """The leaves the last check kept for `table`: which spans of its key
        differ and how many rows each side holds in them; None where it
        kept none."""
        import json
        got = self._read_drill(db, table, "leaves")
        return json.loads(got[0]) if got else None

    def repair_sides(self, db, table):
        """`table` as a repair reads and writes it (`repairways.Sides`): the
        pair's two tables and column plan, or this engine's own table on
        both sides; None where its key cannot be lined up across them."""
        from ..repairways import Sides
        src = getattr(self, "src_engine", None)
        if src is not None:
            pairs, _, _, _ = self.match_tables(
                src.neutral_tables("src", db),
                self.dst_engine.neutral_tables("dst", db),
                self._renamer(db))
            match = [(s, d) for s, d in pairs if self._leaf(s) == table]
            if len(match) != 1:
                return None
            (src_t, dst_t), dst = match[0], self.dst_engine
            sc, dc, _ = self._column_plan(src_t, dst_t, db)
        else:
            src = dst = self
            src_t = dst_t = table
            sc, dc, _ = self._comparable_columns(db, self, table, self, table)
        key = list(src.neutral_key("src", db, src_t) or [])
        names = [n for n, _ in sc]
        if not key or any(k not in names for k in key):
            return None
        dkey = [dc[names.index(k)][0] for k in key]
        return Sides(src, src_t, list(sc), dst, dst_t, list(dc), key, dkey)

    def repair_range_ready(self, db, sides):
        """Why a span of `sides`' key cannot be copied again here the way
        the move copies a range, or None: both sides select a span of the
        key in SQL of their own, the target to empty it and the source to
        read it."""
        for side, eng in (("source", sides.src), ("target", sides.dst)):
            if not eng.SQL_DIALECT or not eng.RESUMES_BY_KEY:
                return (f"the {side} does not read a span of the key in SQL"
                        " of its own")
        return None

    def repair_range(self, db, sides, lo, hi, log):
        """[lo, hi) of `sides`' integer key on the target replaced by the
        source's rows, the way the move's pair copies a range - that span
        emptied, then copied and read back batch by batch (`copy_range`).
        Returns the rows copied."""
        pair = self if getattr(self, "src_engine", None) is not None \
            else self._as_pair()
        return pair.copy_range(db, sides.src_t, sides.dst_t, int(lo) - 1,
                               int(hi) - 1)

    def _drill_tables(self, db):
        """Tables the last check left a drilldown for, and what it found.

        One JSON list per line, each the canonical text of a row's key. The
        reader is here rather than beside one engine because the files are
        the handover between a check and `migkit sync`: an engine that wrote
        them with its own reader would drift from the one that acts on them.
        """
        import json
        found = {}
        for path in sorted(self.hop.report_dir(db).glob("data-*.*")):
            name, _, kind = path.name[len("data-"):].rpartition(".")
            if kind not in ("missing", "changed", "extra") or not name:
                continue
            keys = [tuple(json.loads(l)) for l in
                    path.read_text().splitlines() if l]
            if keys:
                found.setdefault(name, {})[kind] = keys
        return found

    @staticmethod
    def _key_of(columns, key, row):
        """The canonical text of one row's key, for matching across engines."""
        from .. import canon
        cls = {n: c for n, c in columns}
        at = {n: i for i, (n, _) in enumerate(columns)}
        return tuple(canon.render_value(cls[k], row[at[k]]) for k in key)

    def recheck_done(self, db, table, st, log, where=None, label=None):
        """A table an earlier run finished, asked again before it is
        skipped: the source may have changed since, and "done earlier,
        skip" left a target behind it without a word. Both sides are
        digested; the same, it stays done. Different, the ranges the copy
        was made in are asked one by one and only those that differ are
        copied again - or, with no ranges to ask, the table is copied again
        whole. Returns whether it stays done."""
        label = label or table
        same = self._range_same(db, table, where)
        if same is None:
            log(f"{label}: done earlier; no column of it can be compared,"
                " so it is skipped unchecked")
            return True
        if same:
            log(f"{label}: done earlier, and both sides still hold the same"
                " rows - skipped")
            return True
        ranges = st.get("ranges")
        if ranges and st.get("key"):
            q = self._quote_ident(st["key"])
            bad = []
            for after, upto in ranges:
                rng = f"{q} > {int(after)} and {q} <= {int(upto)}"
                if not self._range_same(db, table, f"({where}) and {rng}"
                                        if where else rng):
                    bad.append(after)
            st["ranges_done"] = [a for a in st.get("ranges_done", [])
                                 if a not in bad]
            st.pop("done", None)
            log(f"{label}: done earlier, and {len(bad)} of {len(ranges)}"
                " ranges no longer match the source - copying those again")
            return False
        for k in list(st):
            del st[k]
        log(f"{label}: done earlier, and the two sides no longer hold the"
            " same rows - copying it again")
        return False

    def _range_same(self, db, table, where=None, every=None):
        """Whether the rows `where` selects digest the same on the source
        and the target of a same-engine hop, over every column both
        render; None where none can be. What a copier asks when a range it
        wrote reads back different from what it sent: two releases can
        write a value's text differently and hold the same value.

        `every` names the columns that must all be among those digested:
        where one is not, the answer is None - a digest that left a column
        out cannot say a range was copied whole."""
        sc, dc, _ = self._comparable_columns(db, self, table, self, table)
        if not sc:
            return None
        if every is not None and not set(every) <= {n for n, _ in sc}:
            return None
        return (self.neutral_digest("src", db, table, sc, where=where or None)
                == self.neutral_digest("dst", db, table, dc,
                                       where=where or None))

    def _by_key_map(self, columns, key, rows):
        return {self._key_of(columns, key, row): row for row in rows}

    @staticmethod
    def _by_key_query(quoted_table, columns, key, keys, quote, mark,
                      scope=None, engine=""):
        """The select every SQL engine here needs, written once.

        A single-column key uses `in (...)`; a composite one uses a row
        value, which PostgreSQL, MySQL and SQLite all accept. `scope` is a
        row filter in this engine's SQL: a row outside it is not there.
        `engine` names whose `canon.read_expr` each column is selected
        through.
        """
        from .. import canon
        cols = ", ".join(canon.read_expr(engine, quote(n), c)
                         for n, c in columns)
        # a key value as this engine takes it for its column's class: an
        # ObjectId kept in a `jsonb` key is looked up as its Extended JSON
        of = dict(columns)
        if len(key) == 1:
            where = f"{quote(key[0])} in ({', '.join([mark] * len(keys))})"
            args = [canon.write_value(engine, of.get(key[0]), k[0])
                    for k in keys]
        else:
            left = "(" + ", ".join(quote(k) for k in key) + ")"
            one = "(" + ", ".join([mark] * len(key)) + ")"
            where = f"{left} in ({', '.join([one] * len(keys))})"
            args = [canon.write_value(engine, of.get(n), v) for k in keys
                    for n, v in zip(key, k)]
        if scope:
            where = f"({where}) and ({scope})"
        return f"select {cols} from {quoted_table} where {where}", args

    @staticmethod
    def _where(where, after_clause="", has_args=False, percent=False):
        """` where ...` joining a resume point and a row filter, either of
        which may be absent. `percent` doubles `%` in the filter, for a
        driver that reads it as a placeholder where the statement carries
        parameters (MySQL's)."""
        if where and percent and has_args:
            where = where.replace("%", "%%")
        parts = [p for p in (after_clause, f"({where})" if where else "")
                 if p]
        return (" where " + " and ".join(parts)) if parts else ""

    def neutral_write(self, side, db, table, columns, rows):
        """Write rows read from another engine. Returns how many landed.

        Existing rows with the same key are replaced rather than duplicated,
        so a move that is interrupted and restarted converges instead of
        piling up. A table with no key cannot express that, and the engine
        says so rather than inserting twice.
        """
        raise self._no_canon("write rows")

    #: a statement answering about `{n}` x 32 bytes that do not compress,
    #: for `link_probe`; None where the engine has none
    PAYLOAD_SQL = None
    #: sizes the link is tried with, in bytes: either side of the common
    #: packet sizes (576, 1280, 1500 less headers) and on to a megabyte
    LINK_SIZES = (512, 1200, 1400, 1460, 2048, 8192, 65536, 1 << 20)

    def link_probe(self, side, db, wait=5.0):
        """How the link to one side carries data, for `doctor`: its round
        trip, the largest reply and the largest request that arrived within
        `wait` seconds, the first size that did not and which way, and a
        rate.

        A link can open, sign in and answer small questions, and never
        carry a large reply: a path that drops packets larger than it passes
        (an MTU black hole - a VPN or tunnel set too high) drops only the
        full-sized ones, and TCP's keepalives are small. Measured through
        such a path: connected in 0.04 s, `select 1` answered, and a reply
        of 6.4 KB never came. A copy there hangs. Each question is asked on
        a thread and given up on after `wait`."""
        import random
        import socket
        import string
        import threading
        import time
        ep = self.hop.source if side == "src" else self.hop.target
        out = {}

        def timed(fn):
            got = {}

            def run():
                began = time.monotonic()
                try:
                    got["value"] = fn()
                except Exception as e:  # noqa: BLE001 - said below
                    got["error"] = e
                got["took"] = time.monotonic() - began
            t = threading.Thread(target=run, daemon=True)
            t.start()
            t.join(wait)
            return None if t.is_alive() else got
        try:
            with socket.create_connection((ep.host, int(ep.port)),
                                          timeout=wait):
                pass
        except OSError as e:
            out["tcp"] = str(e)
            return out
        first = timed(lambda: self.run_rule(side, db, "select 1"))
        if first is None:
            out["stalled"] = ("connecting", None)
            return out
        if "error" in first:
            out["error"] = str(first["error"]).splitlines()[0][:200]
            return out
        rtt = [first["took"]]
        for _ in range(4):
            got = timed(lambda: self.run_rule(side, db, "select 1"))
            if got and "took" in got:
                rtt.append(got["took"])
        out["rtt_ms"] = round(min(rtt) * 1000, 1)
        if not self.PAYLOAD_SQL:
            return out
        for size in self.LINK_SIZES:
            sql = self.PAYLOAD_SQL.format(n=size // 32 + 1)
            got = timed(lambda: self.run_rule(side, db, sql))
            if got is None:
                out["stalled"] = ("replies", size)
                return out
            if "error" in got:
                out["error"] = str(got["error"]).splitlines()[0][:200]
                return out
            out["replies"] = size
            if size == self.LINK_SIZES[-1]:
                out["mb_s"] = round(size / max(got["took"], 1e-6)
                                    / 2 ** 20, 1)
        text = "".join(random.choices(string.ascii_letters + string.digits,
                                      k=self.LINK_SIZES[-1]))
        for size in self.LINK_SIZES:
            sql = f"select length('{text[:size]}')"
            got = timed(lambda: self.run_rule(side, db, sql))
            if got is None:
                out["stalled"] = ("requests", size)
                return out
            if "error" in got:
                break
            out["requests"] = size
        return out

    def leg_encryption(self, side, db):
        """{"encrypted": bool, "how": protocol and cipher} of the
        connection migkit opens to one side, as the database itself says
        of it - or None where it cannot be asked."""
        return None

    def capacity(self, side, db):
        """What this side's server can give a move, for `sizing`: any of
        `cpus`, `free_connections` (its limit less those reserved and in
        use), `running` (sessions busy now) and `replica` (it serves reads
        for a primary). A key it cannot answer is left out - {} says
        nothing, which the sizing reads as "not readable", never as
        "idle"."""
        return {}

    # --- what the resource model measures (`migkit.model`, backlog R22) --

    #: what one worker of this engine's table copier holds in memory:
    #: `own` bytes, and `per_row` times the larger of the widest row it
    #: carries (as `row_sizes` counts it) and the batch it reads at once -
    #: `batch` bytes, or the copier's own count of rows at a time
    #: (`COPY_BATCH`, `READ_ROWS`) up to its bytes (`COPY_BYTES`,
    #: `READ_BYTES`) where it has them (`model.worker_memory`). The shared
    #: copier's: a read held while it is converted and written - assumed
    #: twice over, no copier of this engine measured
    COPY_MEMORY = {"own": 64 * 2 ** 20, "per_row": 2.0,
                   "batch": 64 * 2 ** 20}
    #: whether the table copier's workers are processes of their own - so
    #: each has a CPU here - or threads of this one sharing one
    COPIES_IN_PROCESSES = True
    #: whether the table copier reads the rows in migkit's own processes -
    #: its driver's work a CPU here - or a program of the server's does
    READS_HERE = True

    #: what one worker holds where the rows pass server to server
    #: (`passes_through`), as `COPY_MEMORY` says it; None: as that
    PASS_MEMORY = None
    #: whether a range's check - both servers' digests - runs beside the
    #: next range's load (True), or before its commit (False)
    CHECKS_BESIDE = False

    def passes_through(self, db, table):
        """Whether the table copier's ranges of `table` pass server to
        server, each server digesting every range, with no row tallied
        here; False: the rows pass through migkit and are read back."""
        return False

    #: every read billed by the bytes it scans, whatever its LIMIT
    READS_BILLED = False

    def probe_read(self, side, db, table, part=0, parts=1,
                   budget=16 * 2 ** 20, seconds=1.5, keep=0):
        """One short read of `table` the way the table copier reads it:
        {"rows", "bytes", "seconds", "sample"} - the bytes as carried, and
        the first `keep` of them kept for `probe_write`. `part` of `parts`
        is which slice, so streams read side by side read apart; a slice
        this engine cannot find is None. Read-only, and bounded by `budget`
        bytes and `seconds`. Here: the neutral reader from the start, the
        first slice only. None where the engine bills every read by the
        bytes it scans (`READS_BILLED`): a probe of each table would be a
        bill of each, and the pace finds the rate while it runs."""
        import time
        if part or self.READS_BILLED:
            return None
        cols = self.neutral_columns(side, db, table)
        began, rows, size, after, kept = time.monotonic(), 0, 0, None, []
        while size < budget and time.monotonic() - began < seconds:
            got, after = self.neutral_read(side, db, table, cols, after,
                                           1000)
            for r in got:
                n = _row_bytes(r)
                size += n
                if keep and sum(map(_row_bytes, kept)) < keep:
                    kept.append(r)
            rows += len(got)
            if not got or after is None:
                break
        return {"rows": rows, "bytes": size,
                "seconds": time.monotonic() - began,
                "sample": kept, "columns": cols}

    def probe_write(self, db, table, sample, seconds=1.5, stream=0,
                    apply=True):
        """`sample` (what `probe_read` kept) written into the target the way
        the table copier writes, into a table nobody else can see and that
        is gone after, read back, and - with `apply` - taken again as
        upserts, as the change tail writes: {"rows", "bytes", "seconds",
        "read_seconds", "applied", "apply_seconds", "how"}. None where this
        engine has no such table to write into."""
        return None

    def probe_link(self, side, db, nbytes):
        """Seconds a reply of `nbytes` takes over the link to one side, less
        a reply of nothing - the server's work for it next to none
        (`PAYLOAD_SQL`). None where the engine has no such statement."""
        import time
        if not self.PAYLOAD_SQL:
            return None
        began = time.monotonic()
        self.run_rule(side, db, "select 1")
        empty = time.monotonic() - began
        began = time.monotonic()
        self.run_rule(side, db, self.PAYLOAD_SQL.format(n=nbytes // 32 + 1))
        return max(time.monotonic() - began - empty, 1e-6)

    def io_wait(self, side, db):
        """How much of the server's work waits on storage now: any of
        `io_wait` (sessions waiting on it), `active` (sessions working) and
        `io_pending` (reads, writes and flushes queued). {} where it does
        not say."""
        return {}

    def pressure(self, side, db):
        """How hard one side's server is working now, for the model and for
        each resource's limit while a move runs: `capacity`'s answer, the
        load it says it is under (`busy_ratio`, `lag_seconds` - its replay
        lag as a replica, or its replicas' behind it), and `io_wait`'s. A
        key it cannot answer is left out."""
        import inspect
        out = {}
        for fn in (lambda: self.capacity(side, db),
                   lambda: self.io_wait(side, db)):
            try:
                out.update(fn() or {})
            except Exception:  # noqa: BLE001 - a question never stops a move
                pass
        probe = getattr(self, "_health", None)
        if probe is not None:
            try:
                takes_db = len(inspect.signature(probe).parameters) > 1
                h = probe(side, db) if takes_db else probe(side)
            except Exception:  # noqa: BLE001
                h = None
            if h is not None:
                for k in ("busy_ratio", "lag_seconds"):
                    if getattr(h, k, None) is not None:
                        out[k] = getattr(h, k)
        return out

    def changes_made(self, side, db):
        """(changes, seconds): rows the server has inserted, updated and
        deleted in `db`, counted over `seconds` - what the catch-up is
        measured against. None where the engine keeps no such count."""
        return None

    #: rows of a table read to size its rows (`row_sizes`)
    SIZE_SAMPLE = 2000

    def row_sizes(self, side, db, tables=None):
        """{table: {"largest", "p99", "mean", "rows", "exact"}}: bytes a row
        takes as carried - its largest, its 99th percentile and its mean -
        what one worker holds depends on the widest row, not the table's
        average. `exact` where every row was measured. Here: the first
        `SIZE_SAMPLE` rows of each table through the neutral reader, a few
        at a time and only their sizes kept (`pieces.sample_sizes`) - read
        in one go, a sample of wide rows was two thousand of them held at
        once before the move they were to size had begun."""
        from .. import pieces
        out = {}
        for t in (tables if tables is not None
                  else self.neutral_tables(side, db)):
            try:
                cols = self.neutral_columns(side, db, t)
                lengths, whole = pieces.sample_sizes(
                    self, side, db, t, cols, self.SIZE_SAMPLE)
            except Exception:  # noqa: BLE001 - that table is not sized
                continue
            out[t] = sizes_of(lengths, exact=whole)
        return out

    #: rows from which a table copied from its start is written with its
    #: secondary indexes set aside (`table_index_window`)
    INDEX_WINDOW_FROM = 100_000

    def table_index_window(self, db, table, log):
        """A context around copying one table into this target from its
        start: its secondary indexes set aside and built once after, where
        this engine has measured that faster. `table` None: only what an
        earlier copy set aside and did not put back, built on the way out.
        Nothing, for an engine without one."""
        import contextlib
        return contextlib.nullcontext()

    def _rows_of(self, side, db, table):
        """The catalogue's row estimate for one table, or None where it
        cannot be read."""
        try:
            return (self.table_facts(side, db).get(table) or {}).get("rows")
        except Exception:  # noqa: BLE001 - an estimate, or none
            return None

    @staticmethod
    def set_aside_indexes(later, target, db, table, st, rows, log):
        """The one place a copier decides whether a table goes into
        `target` with its indexes set aside: only from its start - a range
        already copied was written with them in place, and is read back by
        them - and only where the table is large enough for a bulk build
        to repay dropping them. `later` is the ExitStack the rebuild runs
        on the way out of."""
        if (st.get("ranges_done") or st.get("spans_done")
                or st.get("last") is not None):
            return
        if not rows or int(rows) < target.INDEX_WINDOW_FROM:
            return
        later.enter_context(target.table_index_window(db, table, log))

    def neutral_rewrite(self, side, db, table, columns, rows):
        """A batch that read back different, written again the plainest way
        the engine has. An engine with a bulk path of its own that could
        store a value differently from an insert writes with inserts here;
        every other engine writes as it did the first time."""
        return self.neutral_write(side, db, table, columns, rows)
    def target_missing(self, db):
        """True when the target is not there yet and the first write makes
        it - a SQLite file, say. A copy starts from an empty list then; a
        check still says it could not open the target, because comparing
        against nothing is not the same as the target being empty.
        """
        return False

    # --- confirming a difference before calling it one -------------------
    # Written once for every engine that can compare rows by key
    # (`_compare_pks`, `_write_pk_files`) and, for the fenced proof, can say
    # where its source is (`src_lsn`) and wait for the target to get there
    # (`fence_wait`). They lived in the PostgreSQL engine; MySQL had every
    # piece they need and none of them.

    def _report(self, db):
        """Where this engine's evidence goes: the same place as every other
        engine's.

        It used to be `reports/pgdc/<hop>/<db>`, from before there was an
        estate to be consistent with, while `hop.report_dir` - which the
        base's own `params.json` for this very hop uses - is
        `reports/<hop>/<db>`. One hop wrote into two trees, and the
        drilldown an operator was told to read was not where every other
        engine puts it. Both the writer and the reader here go through this
        one method, so they move together; a check run by an older migkit
        leaves its files in the old place, and re-running the check writes
        them where `sync` now looks.
        """
        return self.hop.report_dir(db)

    def _suspect_keys(self, db, table):
        """The keys the last check wrote down as differing, all kinds."""
        d = self._report(db)
        keys = set()
        for k in ("missing", "extra", "changed"):
            f = d / f"data-{table}.{k}"
            if f.exists():
                keys |= set(f.read_text().splitlines())
        keys.discard("")
        return keys

    def _compare_pks(self, db, table, keys):
        """(missing, extra, changed) for these keys, or None when this
        engine cannot compare by key."""
        return None

    def _write_pk_files(self, db, table, missing, extra, changed):
        return None

    def _delta_held(self, result):
        """A delta verify's answer for one table's differing rows, held to
        the changes the hop carries (`verdict.hold`): rows it keeps - the
        ones the source deleted, where deletes are not carried - are a
        note, not a difference, and do not hold the verified position
        back. Returns (the answer, whether it holds the position back)."""
        from .. import verdict
        verdict.hold(self.hop, [result.__dict__])
        return result, not getattr(result, "kept_by_hop", "")

    @staticmethod
    def _delta_said(result):
        """What a delta cycle's log says of one table's answer."""
        if getattr(result, "kept_by_hop", ""):
            return "kept by the hop"
        return "clean" if result.status == "ok" else "DIFF"

    def settle_recheck(self, db, table):
        keys = self._suspect_keys(db, table)
        if not keys or len(keys) > 20000:
            return None
        cmp = self._compare_pks(db, table, keys)
        if cmp is None:
            return None
        missing, extra, changed = cmp
        self._write_pk_files(db, table, missing, extra, changed)
        return len(missing), len(extra), len(changed)

    def fenced_recheck(self, db, table, keys):
        """Convergence proof for suspect rows: capture src LSN, wait for
        the fence, re-compare. Two rounds ride out rows that stay hot;
        what survives is a real diff, not replication in flight.
        Returns (missing, extra, changed, proof) or None if unfenceable."""
        if not hasattr(self, "fence_wait") or not hasattr(self, "src_lsn"):
            return None
        proof = []
        for rnd in (1, 2):
            lsn = self.src_lsn(db)
            from ..hopkeys import opt
            ok = self.fence_wait(db, lsn,
                                 timeout=opt(self.hop, "fence_timeout"))
            if ok is None:
                return None
            proof.append(f"round {rnd}: fence lsn={lsn}"
                         f" {'passed' if ok else 'TIMEOUT'}")
            cmp = self._compare_pks(db, table, keys)
            if cmp is None:
                return None
            missing, extra, changed = cmp
            if not (missing or extra or changed):
                self._write_pk_files(db, table, [], [], [])
                return [], [], [], proof
            keys = set(missing) | set(extra) | set(changed)
            if not ok:
                break
        self._write_pk_files(db, table, missing, extra, changed)
        return missing, extra, changed, proof

    def _resolve_inflight(self, db, bad, stream=None):
        """Split DIFF tables into real diffs vs in-flight replication,
        deterministically when a fence is available, by sleep-settle as
        the fallback."""
        still, healed, how = [], [], []
        from ..hopkeys import opt
        # left out, no wait: the re-check reads again at once
        settle = opt(self.hop, "settle") or 0
        slept = False
        for t in bad:
            keys = self._suspect_keys(db, t)
            if not keys or len(keys) > 20000:
                still.append(t)
                continue
            r = self.fenced_recheck(db, t, keys)
            if r is not None:
                missing, extra, changed, proof = r
                if missing or extra or changed:
                    still.append(t)
                else:
                    healed.append(t)
                    how.append(f"{t}: {'; '.join(proof)}")
                if stream:
                    stream(f"{t}: fence {'REAL DIFF' if t in still else 'converged'}")
                continue
            if not settle:
                still.append(t)
                continue
            if not slept:
                time.sleep(settle)
                slept = True
            s = self.settle_recheck(db, t)
            if s is None or any(s):
                still.append(t)
            else:
                healed.append(t)
                how.append(f"{t}: settled after {settle}s (no fence visible)")
        return still, healed, how

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        """The whole table in one pass, `size` rows at a time: for a table
        with no key to resume from, which `neutral_read` answers in one
        piece.

        Measured, MySQL to PostgreSQL, a table keyed by text: the copy held
        220 MB at 200,000 rows and 1.33 GB at 1,600,000 - every row of the
        table in memory at once, on the way to running out of it. An engine
        that can read through a cursor the server keeps overrides this;
        here the one piece is handed on as one batch."""
        rows, _ = self.neutral_read(side, db, table, columns, None, size,
                                    **({"where": where} if where else {}))
        if rows:
            yield rows

    def neutral_views(self, side, db):
        """[(name, the view's SQL)] - what a view is defined as, for another
        engine to be given (backlog 39). [] where the engine has none."""
        return []

    def neutral_functions(self, side, db):
        """[(name, [(parameter, declared type)], returned type, the body's
        one expression)] for every function and procedure the engine's
        users wrote. The expression is None where the body is statements
        rather than one expression: SQL alone does not carry those, and
        they are listed so that the conversion names them."""
        return []

    def code_definition(self, side, db, kind, name):
        """The source's own text for a view, function or procedure, for a
        proposal to be made from; None where the engine cannot give it."""
        return None

    def code_routines(self, side, db):
        """[`storedcode.routines.Routine`] for every function, procedure,
        trigger and event the engine's users wrote - [] where the engine
        keeps none, or migkit cannot read them yet."""
        return []

    def table_ddl(self, side, db, table):
        """The statement(s) making this table as it is, in this engine's
        own SQL, for a copy on another server of the same engine."""
        raise NotImplementedError(
            f"{type(self).__name__} cannot say how a table is made")

    def foreign_keys(self, side, db):
        """[(child, [columns], parent, [parent's columns])]."""
        return []

    def dependencies(self, side, db, tables=()):
        """[(kind, name, holder, needs)] - what in the database cannot
        stand without one of its tables, each name as `neutral_tables`
        gives it, of the kinds this engine lists (`DEPENDENCY_KINDS`):
        `view` - a view (holder, itself) and a table or view it reads;
        `routine` - a function or procedure and a table its body names;
        `trigger` - a trigger, the table it is on and a table its body
        names; `sequence` - a sequence, the table whose default draws from
        it and the table that owns it; `partition` - a partition, its
        partitioned table and itself. `tables` are the source's every
        table, what a body is read for. [] where this engine lists none."""
        return []

    def rows_holding(self, side, db, table, columns, most):
        """How many of `table`'s rows hold a value in every one of
        `columns`, counted up to `most` and no further - so a large table
        costs at most `most` rows read - or None where this engine answers
        no SQL of its own. Read in a transaction that cannot write
        (`run_rule`)."""
        if self.SQL_DIALECT not in ("postgres", "mysql"):
            return None
        held = " and ".join(f"{self._quote_ident(c)} is not null"
                            for c in columns) or "1 = 1"
        got = self.run_rule(
            side, db, f"select count(*) from (select 1 from"
                      f" {self._qualified(side, db, table)} where {held}"
                      f" limit {int(most)}) held")
        return int(got[0][0]) if got else 0

    #: the words a statement names a table after, and the name: parts
    #: quoted any way an engine quotes them, joined by dots
    _NAMES_A_TABLE = re.compile(
        r"\b(?:from|join|into|update|table|references)\s+"
        r"((?:[`\"\[]?[\w$]+[`\"\]]?\s*\.\s*)*[`\"\[]?[\w$]+[`\"\]]?)",
        re.I)

    @classmethod
    def _tables_named(cls, body, tables, own=""):
        """The tables of `tables` a routine's or a view's text reads or
        writes: each name after from, join, into, update, table or
        references, matched by its own name without case - and by its
        schema too, where the text gives one and the table has one. A
        name in a comment or a string is taken all the same: a dependency
        said that is not one costs a look, one missed a broken target."""
        by_leaf = {}
        for t in tables:
            leaf = str(t).split(".")[-1].strip('`"[]').casefold()
            by_leaf.setdefault(leaf, []).append(str(t))
        found = set()
        for m in cls._NAMES_A_TABLE.finditer(str(body or "")):
            parts = [p.strip().strip('`"[]').casefold()
                     for p in m.group(1).split(".")]
            for t in by_leaf.get(parts[-1], []):
                mine = [p.strip('`"[]').casefold()
                        for p in str(t).split(".")]
                if len(parts) == 1 or len(mine) == 1 or \
                        mine[-2:] == parts[-2:]:
                    found.add(t)
        found.discard(str(own))
        return sorted(found)

    def unique_keys(self, side, db):
        """{table: [[columns of one unique key], ...]}, the primary key
        first - here only the primary key, where the engine says no more."""
        out = {}
        for t in self.neutral_tables(side, db):
            key = self.neutral_key(side, db, t)
            if key:
                out[t] = [list(key)]
        return out

    def neutral_function_sql(self, name, params, returns, body):
        """The statement creating a one-expression function here, from
        `[(parameter, this engine's type)]`, this engine's return type
        and an expression already in this engine's SQL."""
        raise NotImplementedError(
            f"{type(self).__name__} cannot create functions")

    def neutral_create_code(self, side, db, statement):
        """Run one converted `create` statement on the target. A plain
        create, never `or replace`: an object already there is the
        operator's, and the statement fails rather than replace it."""
        raise NotImplementedError(
            f"{type(self).__name__} cannot create converted objects")

    def neutral_empty(self, side, db, table, where=None):
        """Remove every row of `table` on the target, keeping the table.
        Returns how many went.

        What a copy does before it starts a table afresh, so the table ends
        up holding the source's rows and nothing else. Writing alone does
        not get there: measured through the cross-engine copier, a row the
        target already had stayed after the move, and a table without a key
        doubled when the move ran again. Only ever the target - migkit does
        not write to a source.
        """
        raise self._no_canon("empty a table")

    @staticmethod
    def _target_only(side, what):
        if side != "dst":
            raise SystemExit(f"migkit does not {what} on a source")

    #: what the SQL translator calls this engine's dialect, where it has one
    SQL_DIALECT = None

    #: why an engine that renders in this process does, as a report says it
    FOLDED_BECAUSE = "has no hashing operator of its own"

    #: whether a table can be read a key range at a time; one that cannot
    #: is copied in one pass, a batch at a time
    RESUMES_BY_KEY = True

    #: text types this engine stores as bytes, compared as bytes beside a
    #: column the other side declares bytes
    TEXT_HOLDS_BYTES = ()

    def neutral_column_rules(self, side, db, table):
        """{column: {"null": takes NULL, "default": its default as this
        engine writes it or None, "identity": numbered by the engine}} -
        {} where the engine cannot say.

        What a table migkit builds on another engine has to keep beyond the
        column types. Measured before, a MySQL table built on PostgreSQL:
        `status varchar(10) not null default 'new'` arrived as `status
        varchar(10)`, and `id int auto_increment` as `id integer`. The
        application's first insert after cutover that left out `status`
        stored NULL, and one that left out `id` was refused."""
        return {}

    def neutral_indexes(self, side, db, table):
        """[(name, unique, [columns], carried)] for the table's indexes
        other than its primary key. `carried` is False for one over an
        expression, a predicate or a part of a column, which another
        engine cannot be given as it is. [] where the engine cannot say."""
        return []

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        """The statement that makes one index here."""
        raise self._no_canon("create an index")

    def execute_ddl(self, side, db, sql):
        """Run one statement that changes this side's schema."""
        raise self._no_canon("change a schema")

    def default_works(self, side, db, expr, typ=None):
        """Whether this engine accepts `expr` as a default, asked of the
        server rather than assumed."""
        return False

    @staticmethod
    def _column_tail(rule, style):
        """` not null default ...`, as `style` writes it: `postgres`,
        `mysql` or `sqlite`."""
        if not rule:
            return ""
        out = ""
        if rule.get("identity"):
            out += {"postgres": " generated by default as identity",
                    "mysql": " auto_increment",
                    "mssql": " identity(1,1)",
                    "oracle": " generated by default as identity",
                    "db2": " generated by default as identity"
                    }.get(style, "")
        if rule.get("null") is False:
            out += " not null"
        if rule.get("default") is not None and not rule.get("identity"):
            d = rule["default"]
            # MySQL takes any expression, and any default on a TEXT or
            # BLOB column, only in parentheses
            out += f" default ({d})" if style in ("mysql", "sqlite",
                                                    "mssql") \
                else f" default {d}"
        return out

    def neutral_create_sql(self, side, db, table, columns, key=()):
        """The statement that would create this table here, without running it.

        Split from `neutral_create` so `convert-schema` can print exactly what
        the mover would execute. When the two were written separately they
        were free to disagree, and the one you read was not the one that ran.
        """
        raise self._no_canon("write a create statement")

    def neutral_create(self, side, db, table, columns, key=()):
        """Create a table to receive rows. Returns the DDL it ran.

        `columns` is [(name, class, numbers)] where `numbers` are whatever
        the source's declared type carried - a length, or a precision and
        scale. They travel because the class does not carry them and a
        narrower target would truncate.

        **An existing table is never touched.** Not altered, not dropped,
        not emptied: the one thing worse than a missing target is a target
        that used to hold something else.
        """
        raise self._no_canon("create a table")

    def neutral_changes(self, side, db, token=None, limit=1000):
        """(changes, token) from this engine's own change log.

        `token` is opaque and belongs to the engine that made it - a binlog
        file and position, a replication slot's LSN, a resume token. It is
        stored and handed back so a stopped tail continues where it was
        rather than from the top.

        Each change is a `canon.change(...)` record. An engine with no change
        log says so rather than polling the table and calling the difference
        a change: a poll cannot see a row that was inserted and deleted
        between two looks, and reporting that as "nothing happened" is the
        failure this whole contract is built to avoid.
        """
        raise self._no_canon("read a change log")

    #: whether this engine's own copier, hash and replication read
    #: `mapping.columns`. Where they copy and hash whole rows, a table whose
    #: columns the hop maps goes through the pair machinery instead - the
    #: copier, comparison, repair and change tail that pair columns by name
    #: and read the mapping in one place (`_mapped_types`).
    OWN_PATHS_READ_COLUMN_MAPPING = True

    def through_pair(self, db, table):
        """Whether this table's columns are mapped, or its name is, and
        this engine's own paths would copy and hash it whole under the
        source's name.

        A table the hop renames (`mapping.tables`) went every same-engine
        way under its own name - the bulk paths, the table copier, the
        tables made for a load - and the check then compared it under that
        name on both sides, so the table it was renamed to was never filled
        and nothing said so. The pair renames it (`HeteroEngine._rename`)
        in its copy, comparison, repair and tail alike."""
        if self.OWN_PATHS_READ_COLUMN_MAPPING:
            return False
        from ..movers import _renamed
        parts = [p for p in str(table).split(".") if p]
        return bool(self.hop.column_rules(db, *parts)
                    or _renamed(self.hop, db, parts))

    def maps_columns(self):
        """Whether any table of this hop goes through the pair machinery:
        it maps columns or renames tables (`through_pair`)."""
        from ..movers import renames_any
        return (not self.OWN_PATHS_READ_COLUMN_MAPPING
                and bool((self.hop.mapping or {}).get("columns")
                         or renames_any(self.hop)))

    def _as_pair(self, within=None):
        """This hop seen as a pair of the same engine, so the machinery
        written for any pair - the copier, the change tail - serves a
        same-engine hop too, instead of a second copy of it per engine.

        `within` is a directory under this hop's reports for the pair's own
        evidence, where it would otherwise share file names with this
        engine's and each would read the other's as its own."""
        import dataclasses

        from .hetero import HeteroEngine
        name = self.CANON_ENGINE
        hop = dataclasses.replace(
            self.hop, engine="hetero",
            options={**self.hop.options, "source_engine": name,
                     "target_engine": name})
        # the report directory is a method the tests and callers may have
        # replaced on this hop; the pair keeps writing where this one does
        own = self.hop.report_dir
        if within:
            def report_dir(db=None):
                d = own(db) / within
                d.mkdir(parents=True, exist_ok=True)
                return d
            hop.report_dir = report_dir
        else:
            hop.report_dir = own
        pair = HeteroEngine(hop)
        # the rows a repair by spans leaves to be repaired by key, where
        # this engine's rows are repaired by a pair made for the call
        # (`_only_named`); never into the pair kept for mapped columns
        if not within and "_repair_only" in self.__dict__:
            pair.__dict__["_repair_only"] = self.__dict__["_repair_only"]
        return pair

    def columns_pair(self):
        """The pair machinery that carries, compares and repairs the tables
        whose columns the hop maps (`through_pair`). Its drilldown and
        evidence are kept in their own directory: the two write the same
        file names in two formats."""
        pair = self.__dict__.get("_columns_pair")
        if pair is None:
            pair = self.__dict__["_columns_pair"] = self._as_pair(
                within="mapped-columns")
        return pair

    def _mapped_data(self, db, stream=None, note=""):
        """The data results for the tables this engine's own pass leaves to
        the pair machinery, one per table."""
        out = []
        for t in self.mapped_tables(db):
            for r in self.columns_pair().check_data(db, t, stream):
                if note:
                    r.detail = f"{r.detail}{note}"
                out.append(r)
        return out

    def _delta_compare(self, db, table, keys):
        """(missing, extra, changed) for the keys a delta cycle saw change.

        A table whose columns the hop maps is compared by the pair, which
        reads the mapping; this engine's own comparison hashes whole rows
        under the source's names, and on the target those names are not
        there. The pair walks the whole table rather than the keys, which
        is slower and still right."""
        if self.through_pair(db, table):
            return self.columns_pair()._compare_pks(
                db, str(table).rpartition(".")[2], keys)
        return self._compare_pks(db, table, keys)

    #: the files a delta verify keeps its position in, under a database's
    #: reports - one an engine; PostgreSQL's is a slot on the source
    DELTA_FILES = ("delta-pos.json", "delta-token.json",
                   "pair-delta-token.json", "delta-offsets.json",
                   "delta-seqno.json", "delta-ctver.json",
                   "delta-parts.json")
    #: the scope the delta's position was taken under, kept beside it
    DELTA_SCOPE = "delta-scope.json"

    def _delta_kept(self, db):
        """What the delta's position for `db` was taken under, {} where
        nothing says (a position from before this was kept, or none)."""
        from .. import durable
        got = durable.read_state(self.hop.report_dir(db) / self.DELTA_SCOPE,
                                 "position")
        return got if isinstance(got, dict) else {}

    def delta_covers(self, db):
        """Whether the delta's position for `db` stands for everything the
        hop takes now: taken under its scope (`Hop.scope_changed`), and,
        where it was taken again because the scope changed, a whole check
        under this scope has found the two the same since. A delta reads
        only what changed after its position - the changes of a table the
        hop left out were passed over, and a table it took in later was
        never compared whole - so the cutover's last check compares every
        table instead (`cutover._has_delta`)."""
        kept = self._delta_kept(db)
        if self.hop.scope_changed(kept):
            return False
        return not kept.get("scope_changed_from") or \
            self._whole_since(float(kept.get("at") or 0))

    def _whole_since(self, at):
        """Whether the hop's last check, taken after `at` under the hop's
        scope now, found the two the same (`verdict.json`)."""
        from .. import durable
        p = self.hop.report_dir() / "verdict.json"
        try:
            got = durable.read_state(p, "cache") or {}
            taken = p.stat().st_mtime
        except OSError:
            return False
        return (taken >= at and got.get("status") == "same"
                and not self.hop.scope_changed(got)
                and bool(got.get("scope_fingerprint")))

    def delta_forget(self, db):
        """The delta's position for `db` gone - its slot dropped where it
        keeps one on the source, its files removed - so the next verify
        takes its baseline afresh."""
        if callable(getattr(self, "delta_teardown", None)):
            self.delta_teardown(db)
        where = self.hop.report_dir(db)
        for f in self.DELTA_FILES + (self.DELTA_SCOPE,):
            (where / f).unlink(missing_ok=True)

    def delta_in_scope(self, db, limit=20000, log=None):
        """`delta_verify`, its position held to the scope it was taken
        under (backlog P0-SCOPE.6c). Kept beside every position, so a
        position taken under another scope is never gone on from: the
        PostgreSQL slot had advanced past the changes of every table the
        hop left out, and a table taken in later was then verified only
        from wherever the position stood. Where the scope changed, the
        position is dropped and taken again here, and every cycle says -
        and the cutover compares every table - until a whole check under
        the new scope finds the two the same. A position with nothing kept
        beside it predates this, and is gone on from as it always was."""
        from .. import durable
        kept = self._delta_kept(db)
        changed = self.hop.scope_changed(kept)
        if changed:
            self.delta_forget(db)
        got = list(self.delta_verify(db, limit, log))
        fp = self.hop.scope_fingerprint()
        if kept.get("scope_fingerprint") != fp:
            now = {"scope_fingerprint": fp, "at": time.time()}
            if changed:
                now["scope_changed_from"] = kept["scope_fingerprint"]
            durable.write_json(self.hop.report_dir(db) / self.DELTA_SCOPE,
                               now)
            kept = now
        if changed:
            got.append(Result(
                "delta", db, "warn",
                "the hop's scope changed since the last verified position,"
                " which was taken again here: what the hop takes now was"
                " not compared whole, and the delta sees only what changes"
                " from here", "",
                f"migkit check {self.hop.name} --db {db}"))
        elif kept.get("scope_changed_from") and not self.delta_covers(db):
            got.append(Result(
                "delta", db, "warn",
                "the hop's scope changed after the delta's first position;"
                " no whole check under the scope it has now has found the"
                " two the same since, so the delta covers only what"
                " changed", "", f"migkit check {self.hop.name} --db {db}"))
        return got

    def _mapped_schema(self, db):
        """The schema results for the tables this engine's whole-database
        comparers leave to the pair machinery: their columns, compared
        through the mapping."""
        tables = self.mapped_tables(db)
        if not tables:
            return []
        return self.columns_pair().check_schema(db, only=tables)

    def _mapped_repairs(self, db, kind):
        """The row repairs for the tables whose columns the hop maps, planned
        from the pair's own drilldown and carried out by it."""
        if kind not in ("rows", "all") or not self.maps_columns():
            return []
        acts = self.columns_pair().repair_plan(db, "rows")
        for a in acts:
            a.by = "pair"
        return acts

    def mapped_tables(self, db):
        """The source's tables, by the pair's names, that go through the
        pair machinery on this hop."""
        if not self.maps_columns():
            return []
        pair = self.columns_pair()
        return [t for t in pair.src_engine.neutral_tables("src", db)
                if self.through_pair(db, t)]

    def change_point(self, side, db):
        """A token for *now* in this engine's change log, in the shape
        `neutral_changes` takes back, read without consuming anything.

        A copy followed by a tail needs it taken **before** the copy: a tail
        that starts from wherever the log is once the copy finishes has
        skipped every change made while the rows were being read, and
        nothing downstream can tell those rows from ones that never
        changed.
        """
        raise self._no_canon("read a change log")

    def copy_point(self, side, db):
        """Where the tail that follows a copy starts: a position every
        change before which a read made after it can see. A position read
        beside a copy's reads can be past a change that is committed and
        not yet visible - the copy misses it and the tail starts after it
        (`test_the_position_is_taken_before_the_copy_reads`). Where the
        engine's `change_point` has no such change before it, it is that.
        What the tail may leave out after it is `mark_covers`'s to say."""
        return self.change_point(side, db)

    def snapshot_mark(self, side, db):
        """What this side has committed and made visible by now, as text a
        change can be held to (`mark_covers`), or None where the engine
        cannot say. Taken by the table copier before it reads a range, and
        kept with the range when it is done (`ranges.finished`).

        Not a log position: a transaction's place in the log is fixed
        before it is visible to a reader - PostgreSQL writes its commit
        record, then flushes it, then shows it; MySQL writes its binlog,
        then commits in the engine - so a read begun after a position was
        taken can still miss a transaction written before it. And a
        PostgreSQL change carries the position of the change, not of its
        commit, so a long transaction's changes sit before a position its
        commit comes after. The mark is what the server says is visible,
        which is what the read sees."""
        return None

    def mark_covers(self, side, db, mark, change):
        """Whether `change` was committed and visible when `mark` was
        taken on this side (`snapshot_mark`): True, False, or None where
        it cannot be said - the change names no transaction, or the mark
        was taken on another server. Only True lets the tail leave a
        change out."""
        return None

    def visible_now(self, side, db):
        """Return once every transaction this side's log already holds is
        visible to a read begun after - where the engine logs a commit
        before it shows it. A table that joins a running tail is copied
        only after this (`tailtables`): the tail left out its changes up to
        where it had read, and a copy that could not see one of them would
        leave it nowhere. Nothing to wait for here."""
        return None

    def marked_rule(self, side, db, sql):
        """(mark before, the rows `sql` answers, mark after): what this
        side had made visible just before the read and just after it
        (`snapshot_mark`), so a change the first covers is in what was
        read and one the second does not cover is not - and one between
        the two is not known either way. The kept proof holds a leaf read
        from the source to its changes by these (`kept`)."""
        lo = self.snapshot_mark(side, db)
        rows = self.run_rule(side, db, sql)
        return lo, rows, self.snapshot_mark(side, db)

    def stream_room(self, side, db, token):
        """How much longer the source keeps what a tail at `token` has not
        read yet, before that falls out of its log: `{"seconds": n}` or
        `{"bytes": n}`, with `held_bytes` where the log is held for it. None
        where the engine keeps no log or cannot say. The tail asks once a
        minute, for `/metrics`."""
        return None

    #: whether a change position this engine hands back only says where to
    #: read from next. False where reading from it also says everything
    #: before it was applied (a PostgreSQL slot moves on then), and the
    #: tail must not read ahead of what it applied
    READS_AHEAD = False

    #: whether a reader of its own can read this engine's log again from
    #: an older position without moving the tail's: a table that left a
    #: tail and comes back gets its changes since from there
    #: (`tailtables`). Not a PostgreSQL slot, which is read once
    REREADS_FROM_A_POSITION = False

    #: whether a read can end inside a source transaction, its position
    #: saying only where to read on from: the tail then ends a batch at
    #: the last commit it holds and reads the rest again
    #: (`hetero.tail_apply`, F7)
    READ_ENDS_INSIDE_TRANSACTIONS = False

    #: the DDLs of this engine's log that stop a tail whose hop's `ddl` is
    #: stop (`tailtables`): those the tail stopped on before there was a
    #: `ddl` - a table emptied, which was carried by nothing, and a table
    #: renamed: `RENAME TABLE products TO products_old, products_new TO
    #: products` left the target's products holding the old rows under the
    #: new table's changes, and a table renamed into the hop's scope joined
    #: it with none of its rows
    DDL_STOPS = ("truncate", "rename")

    #: the caches of this engine's own that hold a table's columns, taken
    #: away once a DDL has changed the table under them (`forget_shapes`)
    SHAPE_CACHES = ("_column_rules_of", "_ordered_of")

    def forget_shapes(self):
        """What this engine keeps of its tables' columns, forgotten: a
        column added under it is asked for again, not written without."""
        for name in self.SHAPE_CACHES:
            self.__dict__.pop(name, None)

    def release_changes(self):
        """Let go of what `neutral_changes` holds open from one read to the
        next - a stream, a connection - once the tail is done with it."""
        return None

    def neutral_apply(self, side, db, changes):
        """Apply change records. Returns how many were applied.

        Idempotent by key: an insert that is already there updates, a delete
        of a row that is gone is not an error. A tail that is restarted
        replays the changes it had already applied, and the only safe way
        through that is for replaying to be a no-op rather than a duplicate.

        The loop is here and the two statements are per engine, because the
        decision - upsert this, delete that - is the same everywhere and only
        the dialect differs. An engine that grew its own copy of the loop
        would be one bug fix away from behaving differently on one target.
        """
        from .. import decide
        lanes = self._lanes(side, db, changes)

        def run(choice):
            if choice.rung.name == "lanes":
                try:
                    self._apply_lanes(side, db, lanes)
                except Exception as e:
                    # a lane that failed - a deadlock between two, a value
                    # two lanes' rows both claimed - may leave the others
                    # committed; every change is a row's final state or its
                    # removal, so the whole batch applied again, in one
                    # transaction and in order, puts the target right
                    raise _LaneFailed(str(e).strip().splitlines()[0][:200]
                                      if str(e).strip()
                                      else type(e).__name__) from e
                return len(changes)
            with self._apply_session(side, db):
                return self._apply_each(side, db, changes)

        def undo(rung, error):
            self.__dict__["lane_retries"] = \
                self.__dict__.get("lane_retries", 0) + 1
        # the one runtime fallback (`decide.attempt`): only the lanes'
        # failure switches; one of the batch's own transaction is raised as
        # it is - a connection lost there is the tail's to read again
        return decide.attempt(
            "applying a batch", _apply_ways(), run,
            decide.Facts({"lanes": bool(lanes)}), undo=undo,
            switch=lambda e: isinstance(e, _LaneFailed))[1]

    #: changes a batch holds before it is applied in lanes side by side
    LANES_FROM = 2000

    def _lanes(self, side, db, changes):
        """The batch as lists of rows that can be applied side by side,
        each on a connection of its own, or None where it goes as one.

        A row's changes are already one (`_collapsed`); rows of one key go
        to one lane by the key's hash. A table whose rows depend on each
        other - joined to another by a foreign key, or holding a unique
        index besides its key, where one row's value can move to another -
        goes whole to one lane, in the batch's order, with every table it is
        joined to (`_ordered_tables`). An engine that cannot say which
        tables those are applies in one lane.

        What the others do: MySQL's write-set replica and GoldenGate's
        parallel apply order only the changes whose keys overlap; this is
        the same rule at the grain of a batch."""
        from .. import twoway
        from .base import Engine
        lanes = int(getattr(self.hop, "workers", 1) or 1)
        if (lanes < 2 or len(changes) < self.LANES_FROM
                # a batch whose mark says it was committed has to be one
                # transaction for the mark to say so
                or twoway.exact(self.hop)
                or not getattr(self, "WRITES_IN_PARALLEL", True)
                or type(self)._open_writer is Engine._open_writer):
            return None
        try:
            ordered = self._ordered_tables(side, db)
        except Exception:  # noqa: BLE001 - not known: one lane
            ordered = None
        if ordered is None:
            return None
        out = [[] for _ in range(lanes)]
        placed, names = {}, {}
        for table, row in self._net_rows(side, db, changes):
            name = names.get(table)
            if name is None:
                name = names[table] = self._lane_name(table)
            group = ordered.get(name)
            if group is not None:
                at = placed.setdefault(group, len(placed) % lanes)
            else:
                at = hash((table, _ident(row[1], repr))) % lanes
            out[at].append((table, row))
        out = [lane for lane in out if lane]
        return out if len(out) > 1 else None

    def _net_rows(self, side, db, changes):
        """A batch as the rows to write, in the fewest statements their
        order allows: [(table, (upsert|delete, key, values))].

        A table no other table depends on and with no unique index besides
        its key: its changes collapsed to one a key (`_collapsed`), and
        since those are then independent of each other and of everything
        else, written as one run of deletes and one of upserts a table (the
        order DMS's batch apply uses). Measured, 80,000 changes to one
        table with every tenth row deleted, the target 10 ms away: the
        batch's order broke it into 4,330 statements, 56 s.

        A table `_ordered_tables` names - joined to others by keys, or with
        a second unique index - is written change by change in the source's
        order, first: the source applied them in that order and every
        statement kept its constraints. Collapsed, a row's last change went
        where its first had been: an update of a parent, the child's update
        and delete, the parent's delete became the parent's delete before
        the child's, refused by the key on every replay. An engine that
        cannot say which tables those are has every change collapsed, in the
        order rows were first touched, as before."""
        try:
            ordered = self._ordered_tables(side, db)
        except Exception:  # noqa: BLE001 - not known: as before
            ordered = None
        if ordered is None:
            return self._collapsed(changes)
        bound, loose, names = [], [], {}
        for c in changes:
            name = names.get(c["table"])
            if name is None:
                name = names[c["table"]] = self._lane_name(c["table"])
            (bound if name in ordered else loose).append(c)
        out = [row for c in bound for row in self._collapsed([c])]
        # a row's shape - its kind, its key's names and all its names -
        # worked out once for each order its names came in, not once a row
        free, shapes = {}, {}
        for table, row in self._collapsed(loose):
            kind, key, values = row
            seen = (kind, table, tuple(key), tuple(values))
            shape = shapes.get(seen)
            if shape is None:
                shape = shapes[seen] = (kind == "delete", table,
                                        tuple(sorted(key)),
                                        tuple(sorted({**key, **values})))
            free.setdefault(shape, []).append((table, row))
        return out + [r for shape in sorted(free, key=lambda s: not s[0])
                      for r in free[shape]]

    def _lane_name(self, table):
        return str(self.local_table(table)).split(".")[-1]

    def _ordered_tables(self, side, db):
        """{table: group} for the tables whose rows must be applied in the
        batch's order - those joined by foreign keys share a group, and a
        table with a unique index besides its key has one - or None where
        this engine cannot say. Tables are named by their last part."""
        return None

    @staticmethod
    def _table_groups(edges, singles):
        """{table: group} from foreign-key edges (child, parent) and the
        tables ordered on their own: each set of tables joined by keys, one
        group."""
        root = {}

        def find(t):
            root.setdefault(t, t)
            while root[t] != t:
                root[t] = root[root[t]]
                t = root[t]
            return t
        for a, b in edges:
            ra, rb = find(a), find(b)
            if ra != rb:
                root[ra] = rb
        for t in singles:
            find(t)
        return {t: find(t) for t in root}

    def _ordered_cached(self, side, db, read):
        """`read()` - the ordered tables of `_ordered_tables` - asked again
        at most once a minute."""
        import time
        known = self.__dict__.setdefault("_ordered_of", {})
        hit = known.get((side, db))
        if hit is None or time.monotonic() - hit[0] > 60:
            hit = known[(side, db)] = (time.monotonic(), read())
        return hit[1]

    def _apply_lanes(self, side, db, lanes):
        import concurrent.futures as cf

        def one(rows):
            with self._apply_session(side, db):
                self._apply_net(side, db, rows)
        with cf.ThreadPoolExecutor(len(lanes)) as pool:
            futs = [pool.submit(one, lane) for lane in lanes]
        failed = next((f.exception() for f in futs if f.exception()), None)
        if failed is not None:
            raise failed

    def _open_writer(self, side, db):
        """A connection that writes, for engines whose appliers take one;
        None where each statement makes its own way."""
        return None

    def _apply_session(self, side, db):
        """One connection and one transaction for a batch of changes.

        Each applied change used to open a connection of its own and commit
        on it. Measured by the benchmark (`bench/run.py`), MySQL into MySQL
        at 190 rows a second: the tail was still 15 seconds behind when the
        writer stopped after ten. A batch is now one transaction on one
        connection; a batch that fails is rolled back whole, and the tail,
        whose position is saved only after the batch, replays it - which
        the appliers are idempotent for."""
        import contextlib

        @contextlib.contextmanager
        def session():
            conn = self._open_writer(side, db)
            if conn is None:
                yield
                return
            # the session is the thread's: lanes apply side by side, each
            # on a connection and a transaction of its own
            held = self._apply_held()
            held.conn = conn
            try:
                if (self.hop.options or {}).get("two_way") \
                        or self.__dict__.get("_batch_seen"):
                    # first in the transaction, so the tail reading this
                    # side the other way leaves the whole of it out - and
                    # on a one-way tail that numbers its batches, so the
                    # side says which it committed last (`marks`)
                    self.origin_mark(side, db)
                yield
                if fencing.armed(self.hop) is not None:
                    # last before the commit: a writer that lost its hold
                    # is refused by the target here (`fencing`)
                    cur = conn.cursor()
                    try:
                        fencing.hold(self, side, db, cur)
                    finally:
                        cur.close()
                conn.commit()
            except BaseException:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                held.conn = None
                conn.close()
        return session()

    #: whether this engine's change reader leaves out the transactions a
    #: two-way tail applied (`twoway`)
    READS_ORIGIN_MARK = False
    #: the dialect migkit's own rows are written in here (`shared`,
    #: `fencing`), None where the engine keeps none - it then answers
    #: `own_table(side, db)` and `own_name(side, db)` as well
    OWN_DIALECT = None

    def _origin_row(self):
        """The row of `migkit_origin` this thread's batch moves
        (`twoway.thread_origin`): the hop's own where its batches are
        numbered."""
        from .. import twoway
        return twoway.thread_origin(self.hop,
                                    bool(self.__dict__.get("_numbered")))

    def origin_mark(self, side, db):
        """Write this thread's row of `migkit_origin` in the transaction
        being applied (`twoway`). Refused where the engine has no way to."""
        raise SystemExit(f"two_way: {type(self).__name__} cannot mark what"
                         " migkit applies")

    def _apply_held(self):
        import threading
        return self.__dict__.setdefault("_apply_threads", threading.local())

    def _writer(self, side, db):
        """The connection one applied statement uses: the batch's, or one
        of its own, committed and closed after it."""
        import contextlib

        @contextlib.contextmanager
        def one():
            held = getattr(self._apply_held(), "conn", None)
            if held is not None:
                yield held
                return
            conn = self._open_writer(side, db)
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()
        return one()

    #: the work a whole table's copy is timed as, a byte of the source's
    #: table a unit, so tables of every size are held to one another
    TABLE_WORK = "table copy, a byte"

    #: every way a table reaches the target, one ladder climbed once a table
    #: (`tableway`): none here, where the engine's copier carries every
    #: table the one way it has
    TABLE_WAYS = ()
    #: the moves whose programs carry the database's tables at once, as
    #: rungs of `TABLE_WAYS` (`tableway.choose`)
    TABLE_BATCH = ()
    #: bytes from which a table repays a program started for it alone: its
    #: start and its reads of the catalogue are paid once a table, and on a
    #: small table they are most of what it costs; set, not measured -
    #: `bench/same_engine_copy.py` times every way at 1 and 5 GB
    STREAM_FROM = 256 * 2 ** 20

    def way_name(self, table):
        """A table as the table ladder names it: as `table_facts` keys it."""
        return str(table)

    def table_sizes(self, db, quick=False):
        """{table: facts} the table ladder costs a table by - its rows and
        bytes (`costmodel.unit_of`) - or None where a plan (`quick`) could
        not ask in time. Asked each time: a table made or filled since is
        what the next choice is for."""
        if quick:
            from ..movers import _within
            return _within(lambda: self._sized(db))
        return self._sized(db)

    def _sized(self, db):
        return self.table_facts("src", db)

    def masked_on_the_way(self, db):
        """The tables the hop masks on the way, which only a way that masks
        may carry."""
        return set()

    def table_way_facts(self, db, via, quick=False, known=None, sizes=None):
        """(table, fact) -> `decide.Facts` the table ladder's needs read,
        for a move by `via`; asked of the servers once a database where
        they can be, and within a plan's time where `quick`."""
        from ..decide import Facts

        def facts_for(table, fact):
            return Facts({"batch": via in self.TABLE_BATCH, "via": via,
                          **(known or {})})
        return facts_for

    @staticmethod
    def _work_key(work, table=None, run=None):
        """What a way's cost is kept under: the kind of work, the table,
        and the run's size in powers of two - a way whose cost is mostly a
        fixed number of round trips is not held to a cost a row measured
        on runs ten times as long."""
        return (work, table, 1 << (int(run).bit_length() - 1) if run else None)

    def _cheaper(self, key, ways, measured=None):
        """The name of the way to do the work `key` names (`_work_key`),
        of the `decide.Rung`s in `ways`: climbed in `decide` on this
        engine's own costs for that work (`_took`) - each tried once, in
        order, then the one that did a row in less time, a tie going to the
        one that leaves less behind. Only the way taken is timed again, so
        one that grows dearer than the other's last cost gives way to it.

        `measured` is {way: seconds a row} as measured where the ways were
        built, for work not yet timed here: where trying the dearer way
        once costs as much as the work itself (SQL Server: 27s by
        statements against 0.8s by its bulk load for 50,000 rows), it is
        not tried unless the cheaper one grows dearer than it."""
        from ..decide import climb
        return climb(key[0], ways,
                     costs=self._way_costs(key, ways, measured)).path

    def _way_costs(self, key, ways, measured=None, facts=None):
        """The costs a climb of `ways` for the work `key` names reads -
        this engine's own for that work, from `measured` until each way is
        timed here (`_took`) - with the ways and the facts their needs
        read kept for `measured_ways`."""
        from ..decide import Costs
        self.__dict__.setdefault("_way_rungs", {})[key] = tuple(ways)
        if facts is not None:
            self.__dict__.setdefault("_way_facts", {})[key] = facts
        return self.__dict__.setdefault("_way_cost", {}).setdefault(
            key, Costs(measured or {}))

    def lend_log(self, log):
        """The log a way of this engine's that fails as it runs says its
        switch through (`_attempt`) where the work has no log of its own to
        hand - a batch written, a table described: the copy's, lent by the
        copier that writes through this engine."""
        self.__dict__["_lent_log"] = log

    def _attempt(self, work, rungs, run, facts=None, log=None, undo=None,
                 switch=None, **kw):
        """`decide.attempt` for a way of this engine's own: one that fails
        as it runs is switched from by itself, and said through `log`, or
        else through the log lent (`lend_log`) - there once for each way a
        work gives up, so a table's thousand batches that each switch say
        it once. The last way that holds is switched from to nothing: what
        stops it is raised in its own words, as it was before there was a
        switch, for the callers that read a server's refusal as one."""
        from .. import decide
        said = self.__dict__.setdefault("_switches_said", set())
        fresh, left = [True], [0]

        def running(choice):
            left[0] = len(choice.ranked) - 1
            return run(choice)

        def switching(e):
            return left[0] > 0 and (switch or decide.operational)(e)

        def undone(rung, error):
            if undo is not None:
                undo(rung, error)
            fresh[0] = (work, rung.name) not in said
            said.add((work, rung.name))

        def say(line):
            if log is not None:
                log(line)
            elif fresh[0] and self.__dict__.get("_lent_log") is not None:
                self.__dict__["_lent_log"](line)
            fresh[0] = True
        return decide.attempt(work, rungs, running, facts, log=say,
                              undo=undone, switch=switching, **kw)

    def _took(self, key, way, seconds, rows):
        from ..decide import Costs
        self.__dict__.setdefault("_way_cost", {}).setdefault(
            key, Costs()).saw(way, rows, seconds)

    def _trials(self, key, ways, facts=None):
        """`_cheaper` for work whose units run side by side - the ranges of
        one table: a `decide.Trials` on the same costs `_cheaper` climbs and
        `measured_ways` reports, so a way being timed on one unit is not
        tried on every unit started meanwhile, a way that failed on the
        work is passed over for the rest of it, and `ways` may need facts
        (`facts`, read once each). Kept for the engine's life, a later
        copy of the table standing on what the first measured."""
        from ..decide import Costs, Trials
        kept = self.__dict__.setdefault("_way_trials", {})
        if key not in kept:
            kept[key] = Trials(key[0], ways, facts, costs=self.__dict__
                               .setdefault("_way_cost", {})
                               .setdefault(key, Costs()))
            self.__dict__.setdefault("_way_rungs", {})[key] = \
                kept[key].rungs
        elif facts is not None:
            kept[key].facts = facts
        self.__dict__.setdefault("_way_facts", {})[key] = kept[key].facts
        return kept[key]

    def measured_ways(self):
        """What each way of a piece of work cost here, in seconds a row,
        and the one taken now: [{"work", "table", "rows_from",
        "seconds_a_row", "chosen"}], for the decision layer to read what
        was measured. "chosen" is None while a way is still to be tried
        once."""
        from ..decide import climb
        rungs = self.__dict__.get("_way_rungs", {})
        facts = self.__dict__.get("_way_facts", {})
        out = []
        for key, at in sorted(self.__dict__.get("_way_cost", {}).items(),
                              key=lambda kv: str(kv[0])):
            ways = rungs.get(key, ())
            done = bool(ways) and all(w.name in at for w in ways)
            out.append({"work": key[0], "table": key[1],
                        "rows_from": key[2], "seconds_a_row": dict(at),
                        "chosen": climb(key[0], ways, facts.get(key),
                                        costs=at).path
                        if done else None})
        return out

    def _apply_each(self, side, db, changes):
        """Consecutive rows of one table, one kind and one set of columns
        go to the engine together, in the order the batch left them.

        Measured on PostgreSQL 16, 20,000 upserts in one transaction: one
        statement a row took 3.9s; the same rows 1,000 to a statement took
        0.06s. Only runs of rows next to each other are joined, so a parent
        written before its child still is. A key seen again ends the run:
        one statement cannot write the same row twice on every engine."""
        self._apply_net(side, db, self._net_rows(side, db, changes))
        return len(changes)

    def _apply_net(self, side, db, rows):
        """Rows (`_net_rows`) applied in runs, in their order. Named apart
        from `_apply_rows`, which carries rows between two engines."""
        import itertools

        from .. import canon
        run, shape, keys, shapes = [], None, set(), {}
        added = itertools.repeat(canon.Added)
        for table, (kind, key, values) in rows:
            adds = any(map(isinstance, values.values(), added))
            seen = (table, kind, tuple(key), tuple(values), adds)
            this = shapes.get(seen)
            if this is None:
                this = shapes[seen] = (table, kind, tuple(sorted(key)),
                                       tuple(sorted({**key, **values})),
                                       adds)
            ident = _ident(key, str)
            if run and (this != shape or ident in keys):
                self._apply_run(side, db, shape, run)
                run, keys = [], set()
            shape = this
            keys.add(ident)
            run.append((key, values))
        if run:
            self._apply_run(side, db, shape, run)

    def _apply_run(self, side, db, shape, rows):
        table, kind = shape[0], shape[1]
        if kind == "delete":
            self._apply_deletes(side, db, table, [k for k, _ in rows])
        elif len(shape) > 4 and shape[4]:
            self._apply_added(side, db, table, rows)
        else:
            self._apply_upserts(side, db, table, rows)

    def _apply_added(self, side, db, table, rows):
        """[(key, values)] where a counter's value is what it adds
        (`canon.Added`): a row the target has is moved where it stands
        (`n = n + by`, the rest set, a value it keeps - `canon.Kept` - not
        touched), in the batch's transaction, so what the target's own side
        added meanwhile is kept; a row it has not got is made with what the
        source ended with."""
        from .. import canon
        if not callable(getattr(self, "_qualified", None)):
            raise SystemExit(f"two_way.delta: {type(self).__name__} cannot"
                             " add to a value where it stands")
        local = self.local_table(table)
        at = self._qualified(side, db, local)
        q = self._quote_ident
        with self._writer(side, db) as conn:
            with conn.cursor() as cur:
                one = [self._added_row(side, db, local, k, v)
                       for k, v in rows]
                if all(one):
                    # a statement a row that the server reads and writes in
                    # one step: nothing waits on a count, and a row the
                    # target's own side makes between them is added to,
                    # not written over
                    self._piping()
                    for sql, args in one:
                        cur.execute(sql, args)
                    return
                for key, values in rows:
                    names = sorted(key)
                    where = " and ".join(f"{q(k)} = %s" for k in names)
                    found = [canon.sql_value(key[k]) for k in names]
                    cur.execute(f"select count(*) from {at} where {where}",
                                found)
                    if not cur.fetchone()[0]:
                        self._apply_upserts(side, db, table, [(key, {
                            n: (v.to if isinstance(v, canon.Added) else v)
                            for n, v in values.items()})])
                        continue
                    sets, args = [], []
                    for n, v in sorted(values.items()):
                        if n in key or isinstance(v, canon.Kept):
                            continue
                        if isinstance(v, canon.Added):
                            sets.append(f"{q(n)} = {q(n)} + %s")
                            args.append(canon.sql_value(v.by))
                        else:
                            sets.append(f"{q(n)} = %s")
                            args.append(canon.sql_value(v))
                    if sets:
                        cur.execute(f"update {at} set {', '.join(sets)}"
                                    f" where {where}", args + found)

    def _added_row(self, side, db, table, key, values):
        """(sql, args): one counter row (`_apply_added`) as one statement -
        made with what the source ended with where the target has no row,
        else each counter moved where it stands and the rest set - or None
        where this engine has no such statement."""
        return None

    def _piping(self):
        """The statements the batch sends next need not wait for each
        one's answer, where the engine can send them so."""

    def _apply_upserts(self, side, db, table, rows):
        """[(key, values)] of one table and one set of columns: one
        statement for many rows where the engine writes it; one a row
        where it does not."""
        for key, values in rows:
            self._apply_upsert(side, db, table, key, values)

    def _apply_deletes(self, side, db, table, keys):
        for key in keys:
            self._apply_delete(side, db, table, key)

    @staticmethod
    def _collapsed(changes):
        """What a batch of changes leaves each row as, in the order the rows
        were first touched: [(table, (upsert|delete, key, values))].

        A row changed ten times in a batch is written once, with what the
        last change left - the batching the managed services call batch
        apply. The batch is one transaction, so nothing between the first
        change and the last was ever visible on the target anyway. Changes
        that carry only some columns (an update the log wrote without an
        unchanged large value) are merged, later over earlier, so a column
        one change did not carry is not written as empty; a delete in
        between starts the row again. An update that moved the key leaves
        its old address and arrives at the new one."""
        from .. import canon
        net, order = {}, []

        def touch(table, kind, key, values):
            ident = (table, _ident(key, repr))
            have = net.get(ident)
            if have is None:
                order.append(ident)
                net[ident] = [kind, dict(key), dict(values)]
            elif kind == "delete":
                net[ident] = ["delete", dict(key), {}]
            elif have[0] == "upsert":
                canon.merged_values(have[2], values)
            else:
                net[ident] = ["upsert", dict(key), dict(values)]
        for c in changes:
            op = c.get("op")
            if op == "delete":
                touch(c["table"], "delete", c["key"], {})
            elif op in ("insert", "update"):
                values = c.get("values") or {}
                key = c["key"]
                if len(key) == 1:
                    (k, v), = key.items()
                    moved = k in values and values[k] != v
                else:
                    moved = any(k in values and values[k] != v
                                for k, v in key.items())
                if moved:
                    # the UPDATE changed the primary key, so the row has to
                    # leave its old address as well as arrive at the new
                    # one - in one transaction, so there is no moment with
                    # neither
                    touch(c["table"], "delete", key, {})
                    touch(c["table"], "upsert",
                          {k: values[k] for k in key}, values)
                else:
                    touch(c["table"], "upsert", key, values)
            else:
                raise ValueError(f"unknown change op {op!r} on"
                                 f" {c.get('table')!r}")
        return [(ident[0], tuple(net[ident])) for ident in order]

    def local_table(self, table):
        """A table name from another engine, in this engine's own terms.

        A change record carries the name the source used, and the two sides
        do not have to agree on what a name is: PostgreSQL says `public.t`
        and MySQL has no schemas at all, so applying the source's name
        verbatim looks for `cx.public.t` and does not find it.

        The default keeps the name whole; an engine that qualifies
        differently overrides. Matching on the last component is the same
        rule the comparison uses to pair tables, and for the same reason -
        it is the only part both sides agree on.
        """
        return table

    def _apply_upsert(self, side, db, table, key, values):
        """Write this row, replacing whatever is at that key."""
        raise self._no_canon("apply changes")

    def _apply_delete(self, side, db, table, key):
        """Remove the row at that key, whether or not it is there."""
        raise self._no_canon("apply changes")

    def neutral_digest(self, side, db, table, columns, where=None):
        """(row count, digest) over `[(name, canon class)]`.

        Computed inside the server: only the two numbers cross the network,
        whatever the size of the table. That is the whole reason the
        rendering had to be pinned down first.
        """
        raise self._no_canon("digest a table")


class Enough(Exception):
    """A probe's read has read what it was allowed: stopped on purpose."""


class Counted:
    """Where a probe's COPY-like read is written: bytes and lines counted,
    the first `keep` bytes kept whole-line, and `Enough` raised once
    `budget` bytes or `seconds` have gone."""

    def __init__(self, budget, seconds, keep=0):
        import time
        self.budget, self.seconds, self.keep = budget, seconds, keep
        self.bytes = self.rows = 0
        self.kept = bytearray()
        self.began = time.monotonic()
        self.took = None

    def write(self, data):
        import time
        data = bytes(data)
        self.bytes += len(data)
        self.rows += data.count(b"\n")
        if len(self.kept) < self.keep:
            self.kept += data[:self.keep - len(self.kept)]
        self.took = time.monotonic() - self.began
        if self.bytes >= self.budget or self.took >= self.seconds:
            raise Enough()
        return len(data)

    def said(self, columns=None):
        import time
        took = self.took if self.took is not None else (
            time.monotonic() - self.began)
        kept = bytes(self.kept)
        if kept and not kept.endswith(b"\n"):
            kept = kept[:kept.rfind(b"\n") + 1]
        return {"rows": self.rows, "bytes": self.bytes, "seconds": took,
                "sample": kept, "columns": columns}


#: bytes one row of values takes as carried (`pieces.row_bytes`)
_row_bytes = row_bytes


def sizes_of(lengths, exact=False, rows=None):
    """{"largest", "p99", "mean", "rows", "exact"} of row lengths measured
    (`Engine.row_sizes`); {} for none."""
    got = sorted(int(x) for x in lengths)
    if not got:
        return {}
    at = min(len(got) - 1, int(0.99 * (len(got) - 1) + 0.5))
    return {"largest": got[-1], "p99": got[at],
            "mean": sum(got) / len(got),
            "rows": rows if rows is not None else len(got),
            "exact": bool(exact)}


class NotHere:
    """A way an engine inherits from the engine whose wire it speaks and
    does not have: TiDB keeps no binlog to replicate from, CockroachDB no
    publication. Read on the class or on an engine it is not there -
    `hasattr` answers False and `getattr(cls, name, None)` None - so every
    caller that asks whether an engine can do a thing, and the capability
    table that reads it off the code, see that it cannot; a placeholder
    that raised when called would have read as a way it has."""

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, owner=None):
        raise AttributeError(self.name)


class _LaneFailed(RuntimeError):
    """A lane of a batch that failed, for the batch to go in one
    transaction instead (`Engine.neutral_apply`)."""


def _apply_ways():
    """How a batch of changes is applied (`Engine.neutral_apply`): in
    lanes side by side where the batch is large enough and its tables
    allow (`_lanes`), else - or where a lane fails - in one transaction,
    in the batch's order."""
    from ..decide import Need, Rung
    return (Rung("lanes", "in lanes side by side",
                 needs=(Need("lanes", lambda f: f["lanes"],
                             "the batch goes in one transaction"),)),
            Rung("one", "in one transaction, in the batch's order"))


class NeutralCopier:
    """Table-by-table copy for an engine to itself, through the one copier
    written for any pair.

    The cross-engine copier reads through `neutral_read` and writes through
    `neutral_write`/`neutral_empty`, resuming by key. An engine that speaks
    that contract has everything a same-engine copy needs, so rather than
    a second copier per engine this views the hop as a pair of the same
    engine and hands the table to that one. A fix to the copier is then a
    fix for every engine it serves.
    """
    #: what one worker holds is then the pair's copier's, whose reads are
    #: `PAIR_ROWS` rows or `BATCH_BYTES` at most (`model.worker_memory`)
    COPY_MEMORY = PAIR_MEMORY
    COPY_BATCH, COPY_BYTES = PAIR_ROWS, BATCH_BYTES

    def list_move_tables(self, db):
        return self._as_pair().list_move_tables(db)

    def move_table(self, db, sch, tbl, chunk, ck, log):
        return self._as_pair().move_table(db, sch, tbl, chunk, ck, log)

    def moved_nothing(self, db):
        return self._as_pair().moved_nothing(db)
