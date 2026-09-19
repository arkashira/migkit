from dataclasses import dataclass, field

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


class Engine:
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = False

    def __init__(self, hop):
        self.hop = hop

    def databases(self):
        raise NotImplementedError

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

    def settle_target(self, db):
        """Leave the target usable after a bulk load, or say nothing.

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

    def _planner_stats_result(self, db, tables, hint):
        """Has the target's planner ever looked at what was just loaded.

        A bulk load leaves the statistics behind. Measured on PostgreSQL 16
        right after loading 200,000 rows: `reltuples` is -1, `last_analyze`
        and `last_autoanalyze` are both null, and `n_mod_since_analyze`
        equals the whole table. Autoanalyze is threshold-driven rather than
        event-driven, so on a large table it will not run until a tenth of
        the rows have changed again - which, on a table that was migrated
        and is now only read, may be never.

        Nothing else in the check catches this: the data is correct, the
        counts match, and every structural check passes. The target is
        simply slow, and the slowness gets blamed on the engine.

        `tables` is [(name, rows, analyzed, modified_since)] where
        `analyzed` is False when the server has never analyzed it and
        `modified_since` is how many rows changed since it last did.
        """
        never = sorted(t for t, _, analyzed, _ in tables if not analyzed)
        stale = sorted(
            t for t, rows, analyzed, modified in tables
            if analyzed and rows and modified is not None
            and modified > rows * self.STALE_STATS_RATIO)
        if never:
            return Result(
                "deep", f"{db} statistics", "diff",
                f"{len(never)} tables the planner has no statistics for:"
                f" {', '.join(never[:5])}"
                + (" ..." if len(never) > 5 else "")
                + " - the rows are there and the query plans will not be",
                "", hint)
        if stale:
            return Result(
                "deep", f"{db} statistics", "warn",
                f"{len(stale)} tables changed by more than"
                f" {int(self.STALE_STATS_RATIO * 100)}% since their"
                f" statistics were taken: {', '.join(stale[:5])}"
                + (" ..." if len(stale) > 5 else ""), "", hint)
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
                             columns, hint):
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
        otherwise look broken on both sides.
        """
        unsure = ("; references kept in a plain integer column are not found"
                  " by this - the type is what makes them findable")
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
        if src_total != dst_total:
            return Result(
                "deep", f"{db} large objects", "diff",
                f"{src_total} large objects on the source and {dst_total} on"
                " the target", "", hint)
        if not src_total and not columns:
            return Result("deep", f"{db} large objects", "ok",
                          "no large objects on either side")
        return Result(
            "deep", f"{db} large objects", "ok",
            f"{src_total} large objects on both sides, and every oid column"
            f" that resolves on the source resolves on the target"
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
        kind = cap[0]
        if kind == "chars":
            return f"char_length({col}) > {cap[1]}", f"max(char_length({col}))"
        if kind == "bytes":
            return (f"octet_length({col}) > {cap[1]}",
                    f"max(octet_length({col}))")
        if kind == "int":
            return f"{col} < {cap[1]} or {col} > {cap[2]}", f"max(abs({col}))"
        if kind == "numeric":
            limit = 10 ** (cap[1] - cap[2])
            return f"abs({col}) >= {limit}", f"max(abs({col}))"
        raise ValueError(f"no capacity probe for kind {kind!r}")

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
            worst = ", ".join(
                f"{s} {t}.{c} ({cols}) {n} duplicated values, e.g. {ex}"
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
        if mixed:
            worst = ", ".join(
                f"{t}.{c} {s} double-encoded and {k} genuinely accented"
                f" (e.g. {ex!r} is really {dec!r})"
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
                f"{t}.{c} {s} rows (e.g. {ex!r} is really {dec!r})"
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
        if not total:
            return Result("deep", f"{db} indexes", "skip",
                          "no indexes on either side to check")
        return Result("deep", f"{db} indexes", "ok",
                      f"{total} indexes, all valid and usable by the planner")

    def check_params(self, db):
        return [Result("params", db, "skip",
                       "no parameter comparison for this engine yet")]

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

    def _atlas_authoritative(self, res):
        """atlas is schema-aware and the authoritative differ; when it says
        clean, demote the noisier textual opinions (native dump diff,
        liquibase) to informational so the db's verdict follows atlas. The
        precise object inventory still stands, and so does the structural
        diff - that one compares objects rather than text, so it is never
        demoted. Opt out with options.schema_authority != 'atlas'."""
        if self.hop.options.get("schema_authority", "atlas") != "atlas":
            return res
        if not any(r.scope.endswith("(atlas)") and r.status == "ok"
                   for r in res):
            return res
        for r in res:
            textual = (r.scope.endswith("(liquibase)")
                       or r.scope == r.scope.split(" ")[0])  # bare "db"
            if (r.check == "schema" and r.status == "diff" and textual
                    and not r.scope.endswith(("(atlas)", "objects",
                                              "(structural)"))):
                r.status = "ok"
                r.detail = ("atlas authoritative: clean; textual diff is"
                            f" cosmetic ({r.detail})")[:200]
        return res

    def check_data(self, db, table=None):
        raise NotImplementedError

    def repair_plan(self, db, kind):
        return []

    def _rows_plan(self, db, carry):
        """The `--kind rows` plan of an engine that keeps a drilldown.

        The rows come from the last check rather than from whatever the two
        sides disagree about at this moment, so the plan describes what the
        operator was shown. `carry` is how this engine says where the rows
        come from, which is the only part that differs between them.
        """
        actions = []
        for name, found in sorted(self._drill_tables(db).items()):
            send = found.get("missing", []) + found.get("changed", [])
            drop = found.get("extra", [])
            statements = []
            if send:
                statements.append(
                    f"copy {len(send)} rows {carry}: "
                    + ", ".join("/".join(k) for k in send[:6])
                    + (" ..." if len(send) > 6 else ""))
            if drop:
                statements.append(
                    f"delete {len(drop)} rows the source does not have: "
                    + ", ".join("/".join(k) for k in drop[:6])
                    + (" ..." if len(drop) > 6 else ""))
            actions.append(RepairAction(
                f"{db}.{name}", "rows", statements, [],
                f"{len(found.get('missing', []))} missing,"
                f" {len(found.get('changed', []))} changed,"
                f" {len(drop)} extra; every target row this overwrites or"
                " removes is written to the undo file first"))
        return actions

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

    # which family of client tools this engine uses, for the version check
    ENGINE_FAMILY = ""

    def _client_tool_versions(self, tools, server_version):
        """assess rows for client tools running ahead of the target server.

        `doctor` says whether a program is installed; this says whether it can
        talk to the server it is about to be pointed at. See
        `migkit.toolversion` for the two measurements behind it.
        """
        from .. import toolversion as _tv
        from ..util import run, which
        got = {}
        for tool in tools:
            if not which(tool):
                continue
            try:
                got[tool] = run([tool, "--version"], check=False).stdout
            except Exception:
                got[tool] = None
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
            fam = self.ENGINE_FAMILY or ""
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

    def neutral_read(self, side, db, table, columns, after=None, limit=1000):
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

    def neutral_rows_by_key(self, side, db, table, columns, key, keys):
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

    def _comparable_columns(self, db, src_engine, src_table,
                            dst_engine, dst_table):
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
        src_types = declared(src_engine, "src", src_table)
        dst_types = declared(dst_engine, "dst", dst_table)
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
            scls, swhy = canon.comparable(src_engine.CANON_ENGINE,
                                          src_types[name])
            dcls, dwhy = canon.comparable(dst_engine.CANON_ENGINE,
                                          dst_types[name])
            if not scls or not dcls:
                notes.append(f"{name}: {swhy or dwhy}")
                continue
            src_cols.append((name, scls))
            dst_cols.append((name, dcls))
        return src_cols, dst_cols, notes

    def _drill_rows(self, db, name, src_engine, src_t, src_cols,
                    dst_engine, dst_t, dst_cols):
        """Which rows differ, written to the estate's files, as a clause.

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
        import json
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
            if walk(src_engine, "src", src_t, src_cols, dst_engine,
                    "dst", dst_t, dst_cols, missing.append, changed.append):
                capped.append("source")
            if walk(dst_engine, "dst", dst_t, dst_cols, src_engine,
                    "src", src_t, src_cols, extra.append, None):
                capped.append("target")
        except Exception as e:
            return f"; rows not localised: {str(e).splitlines()[-1][:90]}"

        self._write_drill(
            db, name,
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

    def _apply_rows(self, db, name, src_engine, src_t, src_cols,
                    dst_engine, dst_t, dst_cols):
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
        """
        import json

        from .. import canon
        found = self._drill_tables(db).get(name, {})
        if not found:
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
        with undo.open("a") as handle:
            remember(handle, found.get("changed", []))
            remember(handle, drop)
        if send:
            rows = src_engine.neutral_rows_by_key(
                "src", db, src_t, src_cols, key, [values(k) for k in send])
            if rows:
                dst_engine.neutral_write("dst", db, dst_t, dst_cols,
                                         list(rows.values()))
        for key_text in drop:
            dst_engine._apply_delete("dst", db, dst_t,
                                     dict(zip(key, values(key_text))))

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

    def _by_key_map(self, columns, key, rows):
        return {self._key_of(columns, key, row): row for row in rows}

    @staticmethod
    def _by_key_query(quoted_table, columns, key, keys, quote, mark):
        """The select every SQL engine here needs, written once.

        A single-column key uses `in (...)`; a composite one uses a row
        value, which PostgreSQL, MySQL and SQLite all accept.
        """
        from .. import canon
        cols = ", ".join(quote(n) for n, _ in columns)
        if len(key) == 1:
            where = f"{quote(key[0])} in ({', '.join([mark] * len(keys))})"
            args = [canon.sql_value(k[0]) for k in keys]
        else:
            left = "(" + ", ".join(quote(k) for k in key) + ")"
            one = "(" + ", ".join([mark] * len(key)) + ")"
            where = f"{left} in ({', '.join([one] * len(keys))})"
            args = [canon.sql_value(v) for k in keys for v in k]
        return f"select {cols} from {quoted_table} where {where}", args

    def neutral_write(self, side, db, table, columns, rows):
        """Write rows read from another engine. Returns how many landed.

        Existing rows with the same key are replaced rather than duplicated,
        so a move that is interrupted and restarted converges instead of
        piling up. A table with no key cannot express that, and the engine
        says so rather than inserting twice.
        """
        raise self._no_canon("write rows")

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
        n = 0
        for c in changes:
            op = c.get("op")
            if op == "delete":
                self._apply_delete(side, db, c["table"], c["key"])
            elif op in ("insert", "update"):
                values = c.get("values") or {}
                table = c["table"]
                moved = any(k in values and values[k] != v
                            for k, v in c["key"].items())
                if moved:
                    # the UPDATE changed the primary key, so the row has to
                    # leave its old address as well as arrive at the new one.
                    # Write first, delete second: interrupted between the two
                    # leaves a duplicate, which is visible, rather than
                    # nothing, which is not.
                    self._apply_upsert(side, db, table,
                                       {k: values[k] for k in c["key"]},
                                       values)
                    self._apply_delete(side, db, table, c["key"])
                else:
                    self._apply_upsert(side, db, table, c["key"], values)
            else:
                raise ValueError(f"unknown change op {op!r} on"
                                 f" {c.get('table')!r}")
            n += 1
        return n

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

    def neutral_digest(self, side, db, table, columns):
        """(row count, digest) over `[(name, canon class)]`.

        Computed inside the server: only the two numbers cross the network,
        whatever the size of the table. That is the whole reason the
        rendering had to be pinned down first.
        """
        raise self._no_canon("digest a table")
