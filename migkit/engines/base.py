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

    def check_params(self, db):
        return [Result("params", db, "skip",
                       "no parameter comparison for this engine yet")]

    def _param_result(self, db, src, dst, critical, hint):
        """Dump every server setting from both sides to params.json (same shape
        as objects.json), then flag mismatches. Only behavior-critical settings
        (timezone, encoding, collation, sql_mode, ...) fail the check; the many
        instance-specific ones that always differ on managed databases (memory,
        paths, limits) are counted but stay ok. The full list is on disk."""
        import json
        names = sorted(set(src) | set(dst))
        inv = {n: {"src": src.get(n), "dst": dst.get(n)} for n in names}
        out = self.hop.report_dir(db) / "params.json"
        out.write_text(json.dumps(inv, indent=1, default=str))
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

    def setup_target_plan(self, db):
        return []

    def watch_sample(self, db):
        return {}

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

    def neutral_write(self, side, db, table, columns, rows):
        """Write rows read from another engine. Returns how many landed.

        Existing rows with the same key are replaced rather than duplicated,
        so a move that is interrupted and restarted converges instead of
        piling up. A table with no key cannot express that, and the engine
        says so rather than inserting twice.
        """
        raise self._no_canon("write rows")

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

    def neutral_digest(self, side, db, table, columns):
        """(row count, digest) over `[(name, canon class)]`.

        Computed inside the server: only the two numbers cross the network,
        whatever the size of the table. That is the whole reason the
        rendering had to be pinned down first.
        """
        raise self._no_canon("digest a table")
