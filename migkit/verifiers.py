"""What migkit asks of each verifier it wraps, read off the code, with
every feature it does not use given its reason (backlog 0f).

The owner's rule (2026-09-27): every wrapped program and library used
whole, smarter than on its own, and topped up where migkit's own code
pulls more out of it. For the three that verify - the independent second
reader, the bisecting comparison program and the frame comparison library
- this is the table of which of their documented features migkit uses,
which it does not and why, and where it adds to them.

The table is the tool-use registry's, every wrapped tool's one
(`tooluse`), and these three are registered in it from here. As
`capabilities.matrix` does for engines, what is *used* is never declared:
a feature is used when the token that turns it on is in the code that
drives the tool (a flag as a string, a method or a keyword by name,
`tooluse.register(reads=...)`). Only the reasons are written.
`tooluse.unexplained` is a documented feature with neither a use nor a
reason, and `tooluse.stale` a reason for a feature the code has since
started using; the test holds both empty, and holds the documented list
against the flags the installed program actually has.

Keys here are the tools' own names: this is migkit's bookkeeping, and
none of it reaches an operator.
"""
import math
import sys

from . import tooluse
from .tools import SECOND_READER_LICENCE
from .tooluse import TOPPED, UNUSED

#: per tool: its licence, the release its features are documented from,
#: where it is driven from, each documented feature with the token whose
#: presence in those files means it is used, and the reasons - the input
#: of its account in the registry, registered below
_ACCOUNTS = {
    "dvt": {
        # installed by `doctor --install` in an environment of its own,
        # at the version and licence `tools` pins it to
        "licence": SECOND_READER_LICENCE,
        "version": "8.9.3",
        "files": ("second_reader.py", "runners/second_reader.py"),
        "features": {
            "validate column": "column",
            "--count": "--count", "--sum": "--sum", "--min": "--min",
            "--max": "--max", "--avg": "avg", "--std": "std",
            "--bit_xor": "bit_xor",
            "--grouped-columns": "--grouped-columns",
            "--exclude-columns": "--exclude-columns",
            "--threshold": "--threshold", "--filters": "--filters",
            "--wildcard-include-string-len": "--wildcard-include-string-len",
            "--wildcard-include-timestamp": "--wildcard-include-timestamp",
            "--cast-to-bigint": "--cast-to-bigint",
            "validate row": "row",
            "--hash": "--hash", "--concat": "--concat",
            "--comparison-fields": "--comparison-fields",
            "--primary-keys": "--primary-keys",
            "--use-random-row": "--use-random-row",
            "--random-row-batch-size": "--random-row-batch-size",
            "--case-insensitive-match": "--case-insensitive-match",
            "--max-concat-columns": "--max-concat-columns",
            "--trim-string-pks": "--trim-string-pks",
            "--trim-whitespace": "--trim-whitespace",
            "validate schema": "schema",
            "--exclusion-columns": "--exclusion-columns",
            "--allow-list": "--allow-list",
            "--allow-list-file": "--allow-list-file",
            "validate custom-query column": "custom-query column",
            "validate custom-query row": "custom-query row",
            "--source-query": "--source-query",
            "--target-query": "--target-query",
            "--source-query-file": "--source-query-file",
            "--target-query-file": "--target-query-file",
            "--result-handler": "--result-handler",
            "--bq-result-handler": "--bq-result-handler",
            "--service-account": "--service-account",
            "--labels": "--labels",
            "--config-file": "store_validation",
            "configs run": "build_config_managers_from_yaml",
            "configs list": "list_validations",
            "configs get": "get_validation",
            "--config-file-json": "--config-file-json",
            "--config-dir": "--config-dir",
            "--config-dir-json": "--config-dir-json",
            "--kube-completions": "--kube-completions",
            "--format": "--format",
            "--filter-status": "--filter-status",
            "--run-id": "--run-id",
            "--dry-run": "--dry-run",
            "--verbose": "--verbose",
            "--log-level": "--log-level",
            "generate-table-partitions": "generate-table-partitions",
            "--partition-num": "--partition-num",
            "--parts-per-file": "--parts-per-file",
            "find-tables": "find-tables",
            "--allowed-schemas": "--allowed-schemas",
            "--include-views": "--include-views",
            "--score-cutoff": "--score-cutoff",
            "--query": "--query", "--conn": "--conn",
            "--source-conn": "-sc", "--target-conn": "-tc",
            "--tables-list": "-tbls",
            "connections add": "create_connection",
            "--connection-name": "create_connection",
            "connections list": "list_connections",
            "connections delete": "delete_connection",
            "connections describe": "describe_connection",
            "--secret-manager-type": "--secret-manager-type",
            "--secret-manager-project-id": "--secret-manager-project-id",
        },
        "unused": {
            "--cast-to-bigint": (
                "it widens int32 before a sum that would overflow it;"
                " PostgreSQL and MySQL sum integers into a wider type"
                " themselves, as their manuals say, so there is nothing"
                " left to widen on the engines it reads for migkit"),
            "--concat": (
                "the hash's input without the hash: the same normalisation"
                " and more bytes over the wire"),
            "--max-concat-columns": (
                "the reader splits a wide table on its own, per engine; no"
                " table has been measured to need another split"),
            "--trim-string-pks": "deprecated and hidden in 8.9.3",
            "--trim-whitespace": (
                "not an option in 8.9.3: its row hash drops trailing blanks"
                " always (read in its source), which every row finding"
                " says"),
            "validate schema": (
                "migkit's own schema check compares constraints, indexes,"
                " defaults and grants; the reader compares names and types"),
            "--exclusion-columns": "a schema validation option; see there",
            "--allow-list": "a schema validation option; see there",
            "--allow-list-file": "a schema validation option; see there",
            "validate custom-query row": (
                "a view has no key to line its rows up by; its aggregates"
                " are read instead"),
            "--source-query-file": (
                "the query is written inline into the batch file, which"
                " keeps it"),
            "--target-query-file": (
                "the query is written inline into the batch file, which"
                " keeps it"),
            "--result-handler": (
                "its PostgreSQL handler writes results into a table -"
                " pointed at the target, a write to the target; the runner's"
                " collector replaces every handler"),
            "--bq-result-handler": "the older name of --result-handler",
            "--service-account": (
                "only for its BigQuery result handler, which is not used"),
            "configs list": "one batch file per run, run once",
            "configs get": "one batch file per run, kept as it was run",
            "--config-file-json": (
                "one validation per file; the batch is one YAML file of"
                " all of them"),
            "--config-dir": (
                "a directory of batch files is for running them apart;"
                " a run is one batch"),
            "--config-dir-json": "see --config-dir",
            "--kube-completions": (
                "for running a directory of batches as Kubernetes jobs;"
                " a migkit run reads from one machine"),
            "--format": (
                "results come back as the frame, through the collector, and"
                " are never printed"),
            "--run-id": (
                "each run is its own process and batch file, kept in the"
                " hop's report directory"),
            "--dry-run": "the batch file already says what was read",
            "--verbose": "its log is not the operator's",
            "--log-level": "its log is not the operator's",
            "generate-table-partitions": (
                "not yet (R8): a differing table is narrowed by groups and a"
                " table too large to read row by row a second time is said;"
                " per-partition runs are the next step"),
            "--partition-num": "see generate-table-partitions",
            "--parts-per-file": "see generate-table-partitions",
            "find-tables": (
                "migkit pairs tables itself, through the hop's renames; a"
                " fuzzy name match would pair tables the hop never named"),
            "--allowed-schemas": "see find-tables",
            "--include-views": "see find-tables",
            "--score-cutoff": "see find-tables",
            "--query": (
                "a raw query through the reader is not a second reading"),
            "--conn": "see --query",
            "connections list": (
                "connections live in a directory made for one run and"
                " removed with it"),
            "connections delete": "see connections list",
            "connections describe": "see connections list",
            "--secret-manager-type": (
                "the connection is handed over in the job, through a pipe,"
                " after the hop's own secret references are resolved"),
            "--secret-manager-project-id": "see --secret-manager-type",
        },
        "topped_up": {
            "--grouped-columns": (
                "the groups that differ narrow migkit's own row compare to"
                " them, and migkit's own digest proves the rest equal - a"
                " difference the groups' measures cannot see is walked, not"
                " lost (second_reader.walk_narrowed)"),
            "--filter-status": (
                "applied in the runner: in 8.9.3 the result handler applies"
                " it, and the collector replaces the handler"),
            "--hash": (
                "within one engine only; across engines rows are read value"
                " by value with floats left out (measured false difference"
                " on -1e-07); its normalisation is said in every finding"),
            "--comparison-fields": (
                "the row reading across engines, of the columns both sides"
                " name alike"),
            "--use-random-row": (
                "run first and apart, and never a verdict: a sample that"
                " agrees is `skip`"),
            "--threshold": (
                "only from the hop's `mapping.tolerance`, and a drift inside"
                " it is said"),
            "--labels": (
                "every validation carries migkit's label, and its answer and"
                " its failure come back under it, per table"),
            "--config-file": (
                "one batch file per run, executed from the file and kept as"
                " evidence the hop's recipients can encrypt"),
            "--avg": "only on integers and exact decimals",
            "--std": "only on integers and exact decimals",
            "--bit_xor": "only on integers",
        },
    },
    "reladiff": {
        "licence": "MIT",
        "version": "0.6.0",
        "files": ("engines/generic.py",
                  "engines/mysql.py::_reladiff_table",
                  "engines/mysql.py::_reladiff_url",
                  "util.py::diff_run_config", "leaves.py::_bisect_run",
                  "leaves.py::by_bisection", "leaves.py::bisection_reading",
                  "leaves.py::bisection_reader"),
        "features": {
            "--key-columns": "-k", "--columns": "-c",
            "--update-column": "-t", "--limit": "-l",
            "--bisection-factor": "--bisection-factor",
            "--bisection-threshold": "--bisection-threshold",
            "--materialize-to-table": "-m",
            "--min-age": "--min-age", "--max-age": "--max-age",
            "--stats": "--stats", "--debug": "-d", "--json": "--json",
            "--verbose": "-v", "--version": "--version",
            "--interactive": "-i", "--case-sensitive": "--case-sensitive",
            "--assume-unique-key": "--assume-unique-key",
            "--skip-sort-results": "--skip-sort-results",
            "--sample-exclusive-rows": "--sample-exclusive-rows",
            "--materialize-all-rows": "--materialize-all-rows",
            "--table-write-limit": "--table-write-limit",
            "--allow-empty-tables": "--allow-empty-tables",
            "--threads": "-j", "--where": "--where",
            "--algorithm": "-a", "--conf": "--conf", "--run": "--run",
            "databases.connect": "connect",
        },
        "unused": {
            "--update-column": (
                "which column says a row was updated is the application's,"
                " not something migkit may assume; a change still arriving"
                " is told apart by migkit's own confirm pass"),
            "--limit": (
                "it stops at that many differences, and the rows written"
                " would read as all of them"),
            "--materialize-to-table": (
                "writes a table into a database, and a check never writes"),
            "--materialize-all-rows": "see --materialize-to-table",
            "--table-write-limit": "see --materialize-to-table",
            "--sample-exclusive-rows": (
                "writes a sample table (join algorithm only), and a check"
                " never writes"),
            "--min-age": "needs --update-column; see there",
            "--max-age": "needs --update-column; see there",
            "--debug": "its diagnostics are not the operator's",
            "--verbose": "its diagnostics are not the operator's",
            "--interactive": "stops for a person at every query",
            "--case-sensitive": (
                "column names come from the catalogue exactly as they are"),
            "--assume-unique-key": (
                "a declared key is unique already, and on a table without"
                " one it would hide rows that are there twice"),
            "--skip-sort-results": (
                "not yet measured: whether the sort costs anything next to"
                " the reading"),
            "--allow-empty-tables": (
                "not yet measured on the pairs the generic engine reads"),
            "--algorithm": (
                "its join algorithm runs inside one database, which a"
                " migration's two sides never are"),
            "--run": "the run's configuration file holds one run",
        },
        "topped_up": {
            "--threads": (
                "the throttle's current allowance where it is the only"
                " reader, not the hop's worker count - measured, it is a"
                " connection count and not a speed; the rung's two in the"
                " verify passes"),
            "--bisection-factor": (
                "sized per table by the rung below from its rows and row"
                " width when the facts give them, kept below the threshold"
                " the program otherwise refuses to start with"),
            "--bisection-threshold": "sized with the factor; see there",
            "--stats": (
                "its totals are not read (measured -1 rows unchanged); the"
                " exclusive and updated counts are"),
            "--conf": (
                "both addresses in an owner-only file for the run, not on"
                " the command line"),
        },
    },
    "datacompy": {
        "licence": "Apache-2.0",
        "version": "1.0.4",
        "files": ("second_reader.py::explain",),
        "features": {
            "PandasCompare": "PandasCompare", "join_columns": "join_columns",
            "df1_name": "df1_name", "df2_name": "df2_name",
            "report": "report", "all_mismatch": "all_mismatch",
            "df1_unq_rows": "df1_unq_rows", "df2_unq_rows": "df2_unq_rows",
            "intersect_rows": "intersect_rows", "column_stats": "column_stats",
            "matches": "matches", "abs_tol": "abs_tol", "rel_tol": "rel_tol",
            "ignore_spaces": "ignore_spaces", "ignore_case": "ignore_case",
            "cast_column_names_lower": "cast_column_names_lower",
            "sample_mismatch": "sample_mismatch",
            "all_columns_match": "all_columns_match",
            "all_rows_overlap": "all_rows_overlap",
            "count_matching_rows": "count_matching_rows",
            "intersect_columns": "intersect_columns",
            "df1_unq_columns": "df1_unq_columns",
            "df2_unq_columns": "df2_unq_columns",
            "columns_with_mismatches": "columns_with_mismatches",
            "subset": "subset", "on_index": "on_index",
            "custom_comparators": "custom_comparators",
            "sensitive_columns": "sensitive_columns",
            "build_report_data": "build_report_data",
            "intersect_rows_match": "intersect_rows_match",
            "only_join_columns": "only_join_columns",
            "hide_sensitive_columns": "hide_sensitive_columns",
            "reveal_sensitive_columns": "reveal_sensitive_columns",
            "PolarsCompare": "PolarsCompare",
            "SparkSQLCompare": "SparkSQLCompare",
            "SnowflakeCompare": "SnowflakeCompare",
        },
        "unused": {
            "report": (
                "its text opens with the library's own name and its own"
                " layout; the parts it is built from are read instead and"
                " said in migkit's words"),
            "matches": (
                "an empty all_mismatch and no rows on one side only, which"
                " the explanation reads directly"),
            "abs_tol": (
                "an explanation shows every difference; a tolerance belongs"
                " to the verdict"),
            "rel_tol": "see abs_tol",
            "ignore_spaces": "hides differences the explanation is for",
            "ignore_case": "hides differences the explanation is for",
            "cast_column_names_lower": (
                "left at its default, which is why the key names are"
                " lowered before joining"),
            "sample_mismatch": (
                "a few rows per column from all_mismatch say the same"
                " without chance"),
            "all_columns_match": "the columns come from one catalogue",
            "all_rows_overlap": "the rows on one side only are listed",
            "count_matching_rows": "the rows read and on both sides are said",
            "intersect_columns": "the columns come from one catalogue",
            "df1_unq_columns": "the columns come from one catalogue",
            "df2_unq_columns": "the columns come from one catalogue",
            "columns_with_mismatches": "column_stats says which, and how many",
            "subset": "the rows on each side only are listed",
            "on_index": "rows are joined on the table's key",
            "custom_comparators": (
                "values are compared as each side holds them; a comparator"
                " would decide what equal means, which is the verdict's"),
            "sensitive_columns": (
                "migkit's own masking tokenises the columns before they are"
                " compared, so equal stays equal and nothing is in the"
                " clear"),
            "build_report_data": "what report is built from; see there",
            "intersect_rows_match": (
                "the explanation reads each column's own match column"),
            "only_join_columns": "the frames hold more than the key",
            "hide_sensitive_columns": "see sensitive_columns",
            "reveal_sensitive_columns": "see sensitive_columns",
            "PolarsCompare": (
                "not yet: both engines hand their rows back as pandas"
                " frames"),
            "SparkSQLCompare": "no Spark session in a migkit run",
            "SnowflakeCompare": (
                "rows are read by migkit's engines, not compared inside a"
                " warehouse"),
        },
        "topped_up": {
            "all_mismatch": (
                "each differing value shown as each side holds it - the"
                " joined frame turns an integer column with a missing row"
                " into floats"),
            "column_stats": "said per column, with a few rows each",
            "PandasCompare": (
                "on frames masked first, so a masked column still lines up"
                " and still differs"),
        },
    },
}


for _name, _spec in _ACCOUNTS.items():
    tooluse.register(
        _name, licence=_spec["licence"], release=_spec["version"],
        declared={"": frozenset(_spec["features"])},
        reads=(_spec["files"], {("", f): token for f, token in
                                _spec["features"].items()}),
        options={**{("", f): (UNUSED, why)
                    for f, why in _spec["unused"].items()},
                 **{("", f): (TOPPED, how)
                    for f, how in _spec["topped_up"].items()}})


# --- the comparison program as a rung the decision engine can choose -------

#: its own defaults (0.6.0)
FACTOR, THRESHOLD = 32, 16384
#: what one leaf it downloads may weigh, which sizes the threshold
LEAF_BYTES = 8 * 1024 * 1024

#: what is known about it, measured
FACTS = (
    "the bisection factor has to be lower than the threshold, or it refuses"
    " to start - and exits 0 while it says so (0.6.0: `Incorrect param"
    " values`, exit 0)",
    "exits 0 whether the tables match, differ, do not exist or its driver"
    " is missing; only its printed counts tell them apart",
    "a JSON column has no rendering of its own: it compares each side's"
    " text of it and warns that differing encodings may read as false"
    " differences (DuckDB JSON; PostgreSQL jsonb against jsonb, where the"
    " server normalises the text, found the one real difference)",
    "on 200,000 equal rows factor 4 with threshold 16, 32 with 1,024 and"
    " its defaults answered alike, 0.38 to 0.39 s: its sizing only matters"
    " once a difference is being chased",
    "no MySQL driver in this installation (`No module named 'mysql'`)",
    "imports on Python 3.14.7; no database driver built for 3.14 was"
    " there to compare with",
)


def reladiff_rung(rows=None, table_bytes=None, threads=1, engines=(),
                  asked=False, installed=True):
    """The comparison program for one table, as data: how migkit would
    size it, how deep it would bisect, and whether it is chosen here and
    why - for the decision engine to weigh, not a decision made here.

    The threshold is the rows it downloads to compare locally at the
    bottom; sized so one download stays near `LEAF_BYTES` where the row
    width - the table's bytes over its rows - is known, its own default
    otherwise. The factor is the segments per level, kept below the
    threshold. The depth is how many levels a difference is chased down
    before the download: log base factor of rows over threshold.
    `threads` is what the throttle allows now. `chosen` is True where it
    is chosen outright, False where it is not used and None where the
    verify passes choose it per table.
    """
    threshold = THRESHOLD
    if rows and table_bytes:
        width = max(1, int(table_bytes) // int(rows))
        threshold = max(1024, min(65536, LEAF_BYTES // width))
    factor = min(FACTOR, threshold - 1)
    depth = 0
    if rows and rows > threshold:
        depth = math.ceil(math.log(rows / threshold) / math.log(factor))
    engines = tuple(engines)
    if not installed:
        chosen, reason = False, "not installed on this machine"
    elif "generic" in engines:
        chosen, reason = True, ("the only reader that speaks this engine")
    elif asked:
        chosen, reason = True, "the hop asks for it (`reladiff: true`)"
    elif engines and len(set(engines)) == 1:
        chosen, reason = None, (
            "a rung of the verify passes (`leaves.PASSES`) within one"
            " engine: the localizer where measured the cheaper, and the"
            " second reading where the row reader is not here - each row it"
            " names held to migkit's own digests, a failure climbed past")
    else:
        chosen, reason = False, (
            "two engines write a value's text apart and it compares that"
            " text; migkit's own digest renders both sides one way, names"
            " the differing rows and renders JSON, which it does not")
    facts = list(FACTS)
    if sys.version_info >= (3, 14):
        facts.append("running on Python 3.14 or later, where no comparison"
                     " has been measured")
    return {"tool": "reladiff", "bisection_factor": factor,
            "bisection_threshold": threshold, "threads": max(1, int(threads)),
            "depth": depth, "checksums_per_level": factor,
            "facts": facts, "chosen": chosen, "reason": reason}


def validation_ladder(hop=None, table=""):
    """Validation methods as decision rungs for standalone validation (P0-VALIDATE.decide)."""
    from .decide import Need, Rung
    from .util import which

    return (
        Rung(
            "digest",
            "in-database range digests",
            gives=frozenset({"checksum", "byte exact", "exact", "consistent as of"}),
            needs=(
                Need("both servers support range digest",
                     lambda f: bool(f.get("can_digest", True)),
                     "server does not support in-database digest hash"),
            ),
            because="fastest verification using native in-database checksums",
            footprint=0,
            leaves="nothing on the servers",
        ),
        Rung(
            "bisection",
            "bisecting diff localizer",
            gives=frozenset({"checksum", "localizes diffs", "resume by chunk"}),
            needs=(
                Need("bisection tool available",
                     lambda f: bool(f.get("reladiff_installed", True)),
                     "bisecting comparison tool not installed"),
                Need("key on table",
                     lambda f: bool(f.get("key", True)),
                     "bisection requires key columns"),
            ),
            because="pinpoints exact differing keys with logarithmic bisection",
            footprint=1,
            leaves="temporary comparison state on local machine",
        ),
        Rung(
            "second_reader",
            "independent second reader",
            gives=frozenset({"independent verification", "cross-engine"}),
            needs=(
                Need("second reader installed",
                     lambda f: bool(f.get("dvt_installed", False) or which("data-validation")),
                     "independent second reader not installed"),
            ),
            because="third-party independent verification engine across engines",
            footprint=1,
            leaves="validation output in local directory",
        ),
        Rung(
            "row_hash",
            "streaming row hashes",
            gives=frozenset({"carried", "keyless", "row filter", "checksum"}),
            because="neutral client-side row hash comparison",
            footprint=0,
            leaves="nothing on the servers",
        ),
        Rung(
            "count",
            "row counts and metadata",
            gives=frozenset({"count only"}),
            because="fast table row count and metadata check",
            footprint=0,
            leaves="nothing on the servers",
        ),
    )


def plan_validation(table, facts=None, need=(), costs=None, explore=True):
    """Select the best validation method for table via the shared decision engine (P0-VALIDATE.decide)."""
    from .decide import Facts, climb
    f = facts if isinstance(facts, Facts) else Facts(facts or {})
    ladder = validation_ladder(f.get("hop"), table)
    return climb(table, ladder, f, need=need, costs=costs, explore=explore)


def replan_validation(table, run_check, facts=None, need=(), costs=None, where=None, log=None):
    """Run validation with automatic replanning to fallback rungs on failure (P0-VALIDATE.decide)."""
    from .decide import Facts, attempt
    f = facts if isinstance(facts, Facts) else Facts(facts or {})
    ladder = validation_ladder(f.get("hop"), table)
    return attempt(table, ladder, run_check, facts=f, need=need, costs=costs, where=where, log=log)


INTERVALS = "change-intervals"


class ChangeIntervals:
    """A batch's source-time signatures and independently read target effects."""

    def __init__(self, target, db, store, changes, seconds, costs=None):
        import copy
        from . import decide
        self.target, self.db = target, db
        self.changes = copy.deepcopy(changes)
        self.clock = dict(store.get(INTERVALS + "-clock") or {})
        self.receipt = {"windows": [], "unknown": [], "through": None}
        hop = getattr(target, "hop", None)
        # each width's verify time kept on the hop, so the next batch
        # climbs on what this one measured
        self.costs = costs if costs is not None else (
            decide.HopCosts(hop, decide.WINDOW_COSTS) if hop is not None
            else decide.Costs())
        times = [c["txn_ts"] for c in changes if c.get("txn_ts") is not None]
        span = max(times) - min(times) if times else 0
        width, self.window = decide.change_window(
            len(changes), span or seconds, self.costs)
        self.columns, self.events = {}, []
        by = {}
        for c in self.changes:
            table = c["table"]
            ts = c.get("txn_ts")
            if ts is None or not math.isfinite(ts) or width is None:
                self.receipt["unknown"].append(table)
                continue
            if table not in self.columns:
                try:
                    self.columns[table] = target.neutral_columns("dst", db, table)
                except Exception:
                    self.receipt["unknown"].append(table)
                    continue
            width = max(width, math.ulp(ts))
            if not self.clock:
                self.clock = {"start": ts, "end": ts + width}
            if ts >= self.clock["end"]:
                start = self.clock["end"]
                steps = int((ts - start) / width)
                self.clock = {"start": start + steps * width,
                              "end": start + (steps + 1) * width}
            if ts < self.clock["start"]:
                self.receipt["unknown"].append(table)
                continue
            key = (self.clock["start"], self.clock["end"], table)
            w = by.setdefault(key, {"start": key[0], "end": key[1],
                                    "table": table, "source": [0, 0],
                                    "target": None, "status": "pending"})
            w["source"][0] += 1
            w["source"][1] += self.signature(c, self.columns[table])
            self.events.append((key, c))
        self.receipt["windows"] = list(by.values())
        self.receipt["unknown"] = sorted(set(self.receipt["unknown"]))
        self.receipt["through"] = max(times) if times else None

    @staticmethod
    def signature(change, columns):
        from . import canon, rowtext
        classes = dict(columns)
        parts = [change["op"]]
        for part in ("key", "values"):
            parts.append(part)
            for n, v in sorted(change.get(part, {}).items()):
                parts.extend((n, canon.digest_value(classes[n], v)))
        return canon.digest_step(0, rowtext.encode(parts))

    def observed(self, applied):
        """Read back the final effects; an apply acknowledgement alone is no proof."""
        import time
        from . import canon
        from .engines.base import Engine
        began, read = time.monotonic(), True
        bad, observed = set(), {}
        unknown = set(self.receipt["unknown"])
        grouped = {}
        for table, (op, key, values) in Engine._collapsed(self.changes):
            grouped.setdefault((table, tuple(sorted(key))), []).append((op, key, values))
        try:
            for (table, names), rows in grouped.items():
                cols = self.columns.get(table)
                if not cols:
                    unknown.add(table)
                    continue
                found = self.target.neutral_rows_by_key(
                    "dst", self.db, table, cols, list(names),
                    [tuple(key[n] for n in names) for _, key, _ in rows])
                for op, key, values in rows:
                    ident = tuple(canon.render_value(dict(cols)[n], key[n])
                                  for n in names)
                    row = found.get(ident)
                    observed[table, ident] = (dict(zip([n for n, _ in cols], row))
                                               if row is not None else None)
                    if op == "delete":
                        if row is not None:
                            bad.add(table)
                    elif row is None:
                        bad.add(table)
                    else:
                        selected = [(n, c) for n, c in cols
                                    if n in values or n in key]
                        expected = {**key, **values}
                        mine = canon.fold_rows(
                            [c for _, c in selected],
                            [[expected[n] for n, _ in selected]])
                        actual = dict(zip([n for n, _ in cols], row))
                        theirs = canon.fold_rows(
                            [c for _, c in selected],
                            [[actual[n] for n, _ in selected]])
                        if mine != theirs:
                            bad.add(table)
        except Exception:
            read = False
            unknown.update(c["table"] for c in self.changes)
        from collections import Counter
        def identity(c):
            classes = dict(self.columns[c["table"]])
            return (c["table"], tuple(canon.render_value(classes[n], v)
                                     for n, v in sorted(c["key"].items())))
        counts = Counter(identity(c) for _, c in self.events)
        actual = {}
        for window, c in self.events:
            table = c["table"]
            ident = identity(c)
            total = actual.setdefault(window, [0, 0])
            moved = any(n in c["values"] and c["values"][n] != v
                        for n, v in c["key"].items())
            if counts[ident] > 1 or moved:
                unknown.add(table)
                continue
            row = observed.get(ident)
            if c["op"] == "delete":
                if row is not None:
                    continue
                read = c
            elif row is None:
                continue
            else:
                read = {**c, "values": {n: row[n] for n in c["values"]}}
            total[0] += 1
            total[1] += self.signature(read, self.columns[table])
        for w in self.receipt["windows"]:
            key = (w["start"], w["end"], w["table"])
            w["target"] = actual.get(key, [0, 0])
            if w["table"] in bad:
                w["status"] = "diff"
            elif applied != len(self.changes):
                w.update(status="diff", target=None)
            elif w["table"] in unknown:
                w.update(status="pending", target=None)
            else:
                from .verdict import interval_status
                w["status"] = interval_status(w["source"], w["target"], True)
        self.receipt["unknown"] = sorted(unknown)
        if read and self.window is not None and self.window.rung is not None:
            self.costs.saw(self.window.path, len(self.changes),
                           time.monotonic() - began)
        return self.receipt


def change_intervals(hop, db):
    """Read only committed receipts, holding the SQLite generation to its position."""
    import json
    from pathlib import Path
    from . import tailctl, tailtables
    where = hop.report_dir(db)
    path = where / tailtables.STORE
    if not path.exists():
        return []
    import sqlite3
    from contextlib import closing
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        conn.execute("begin")
        meta = {k: json.loads(v) for k, v in conn.execute(
            "select k, v from meta where k = 'gen' or k like ?",
            (INTERVALS + "%",))}
        env = tailctl.position(where / tailctl.TOKEN) or {}
    if int(meta.get("gen") or 0) != int(env.get("gen") or 0):
        return [{"check": "data", "category": "data.intervals", "scope": db,
                 "status": "warn", "detail": "change windows await the tail's checkpoint"}]
    receipts = [v for k, v in meta.items() if k.startswith(INTERVALS + ":")]
    through = max((r["through"] for r in receipts
                   if r.get("through") is not None), default=None)
    windows, unknown = {}, set()
    for r in receipts:
        if r.get("scope") and r["scope"] != hop.scope_fingerprint():
            unknown.update(w["table"] for w in r["windows"])
            continue
        unknown.update(r.get("unknown") or [])
        for w in r["windows"]:
            key = (w["start"], w["end"], w["table"])
            got = windows.setdefault(key, {**w, "source": [0, 0],
                                           "target": [0, 0], "status": "ok"})
            for side in ("source", "target"):
                value = w.get(side)
                if value is None or value[1] is None:
                    if got["status"] != "diff":
                        got["status"] = "warn"
                else:
                    for i in (0, 1):
                        got[side][i] += value[i]
            if w["status"] == "diff":
                got["status"] = "diff"
            elif w["status"] != "ok" and got["status"] != "diff":
                got["status"] = "warn"
    out = []
    from . import verdict
    from datetime import datetime, timezone
    for (start, end, table), w in sorted(windows.items()):
        status = w["status"]
        if status == "ok":
            status = verdict.interval_status(w["source"], w["target"],
                                             through is not None and through >= end)
        period = ", ".join(datetime.fromtimestamp(t, timezone.utc).isoformat()
                           for t in (start, end))
        out.append({"check": "data", "category": "data.intervals",
                    "scope": f"{db}.{table} [{period})",
                    "table": table, "status": status,
                    "detail": f"source changes {w['source']}; target receipts {w['target']}",
                    "window": {"start": start, "end": end, "table": table,
                               "source": w["source"], "target": w["target"]}})
    if unknown:
        out.append({"check": "data", "category": "data.intervals", "scope": db,
                    "status": "warn", "detail": "change windows unproved for: "
                    + ", ".join(sorted(unknown))})
    return out


def each_database(hop, eng, dbs, fn, workers=None):
    """[(db, fn(db))] in the order of `dbs`, the databases read side by
    side under one read pool of the run (`ranges.ReadPool`), sized as
    `check` sizes its own: whatever a database's verify opens below it
    borrows from that budget rather than multiplying it. Each database is
    a unit of the check in the attempt ledger (`Engine.work_unit`). A database whose
    `fn` raises gives (db, the exception), and the others still answer."""
    from .ranges import ReadPool
    dbs = list(dbs)
    if not dbs:
        return []
    pool = ReadPool(hop, eng, dbs[0], max(1, int(
        workers or getattr(hop, "workers", None) or 1)))
    from . import attempts

    def unit(d):
        with attempts.work_unit(eng, "check", d, level="database"):
            return fn(d)
    try:
        futs = [pool.submit(unit, d) for d in dbs]
        out = []
        for d, f in zip(dbs, futs):
            try:
                out.append((d, f.result()))
            except Exception as e:  # noqa: BLE001 - said by the caller
                out.append((d, e))
        return out
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


# --- one schema comparer for every engine (backlog AUD-H, SC1) ----------------

def _named(table):
    return ".".join(map(str, table)) if isinstance(table, (tuple, list)) \
        else str(table)


def _side_engine(eng, side):
    return getattr(eng, "src_engine" if side == "src" else "dst_engine", eng)


def column_with_rules(name, declared, rule=None):
    """A `catalog.Column` from a neutral reader's name and type and the
    engine's `neutral_column_rules` entry for it: whether it takes NULL,
    its default, and whether the engine numbers it. A column the engine
    says nothing of is read as taking NULL, with no default and plain -
    what a name-and-type reading always assumed."""
    from .catalog import Column
    rule = rule or {}
    default = rule.get("default")
    return Column(str(name), str(declared), "", bool(rule.get("null", True)),
                  None if default is None else str(default), None,
                  "identity" if rule.get("identity") else "")


def side_catalog(eng, side, db, acc=None, max_age=0.0):
    """One side's `catalog.Catalog`, read through its engine's one-pass
    reader where it has one, else table by table from the neutral readers;
    kept in the run's probe accumulator, so a second reader of the same
    side in the run does not ask the server again and a stale one is
    read afresh. A table the server would not describe is left out whole
    - never half a table, which would read as columns the other side
    lacks - and named in the `catalog-parts` reading (`unread_tables`)."""
    from . import diagnostics, drift
    from .catalog import Catalog, Column, Index
    from .util import Probes
    acc = acc or diagnostics.accumulator(eng.hop)

    def measure():
        reader = drift.side_reader(eng, side)
        if reader is not None:
            if hasattr(reader, "shape_catalog"):
                return reader.shape_catalog(side, db)
            return drift.catalog_of(reader.column_catalog(side, db))
        one = _side_engine(eng, side)
        cat, seen = Catalog(), Probes("tables")
        for t in one.neutral_tables(side, db):
            name = _named(t)
            with seen.reading(name):
                say = getattr(one, "neutral_column_rules", None)
                rules = (say(side, db, t) if say else None) or {}
                cols = [column_with_rules(n, ty, rules.get(n))
                        for n, ty in one.neutral_columns(side, db, t)]
                key = tuple(one.neutral_key(side, db, t) or ())
                indexes = [
                    Index(str(n), tuple(cols_), bool(uniq), "" if plain
                          else "(not carried)")
                    for n, uniq, cols_, plain
                    in one.neutral_indexes(side, db, t) or []]
                cat.tables.append(name)
                cat.columns[name], cat.keys[name] = cols, key
                cat.indexes[name] = indexes
        acc.absorb("catalog-parts", f"{side}:{db}", seen, {
            "read": len(seen.read), "planned": len(seen.planned),
            "unread": seen.unread()})
        return cat
    return acc.read("catalog", f"{side}:{db}", measure, max_age)


def unread_tables(eng, side, db, takes=None):
    """{table: why} of the tables a side listed and would not describe in
    its last catalogue read (`side_catalog`); `takes` keeps the ones the
    hop moves."""
    from . import diagnostics
    got = diagnostics.accumulator(eng.hop).get("catalog-parts", f"{side}:{db}")
    value = getattr(got, "value", None) or {}
    return {t: why for t, why in (value.get("unread") or {}).items()
            if takes is None or takes(t)}


def side_traits(eng, side, db, tables, acc=None, max_age=0.0):
    """({table: {kind: value}}, {table: why}) of what a side's engine holds
    of each of `tables` beyond columns, keys and indexes - a partition key,
    a time to live, a retention, a partition count, a column family's
    collection rule (`NeutralTable.traits`) - read through its engine's
    `neutral_traits(side, db, table)` (or, where one listing gives every
    table's, `neutral_traits_all(side, db)`) where it has one, kept in the run's
    probe accumulator like a catalogue. An engine without the reader has
    none to compare, which is no claim that there are none; a table whose
    settings could not be read is in the second dict, so the comparison
    names it and does not say ok over it."""
    from . import diagnostics
    from .util import Probes
    one = _side_engine(eng, side)
    read = getattr(one, "neutral_traits", None)
    bulk = getattr(one, "neutral_traits_all", None)
    if read is None and bulk is None:
        return {}, {}
    acc = acc or diagnostics.accumulator(eng.hop)
    wanted = list(tables)

    def measure():
        got, seen = {}, Probes("table settings")
        if bulk is not None:
            every = {_named(t): v for t, v in bulk(side, db).items()}
            for name in wanted:
                got[name] = dict(every.get(name) or {})
                seen.done(name)
        else:
            ids = {_named(t): t for t in one.neutral_tables(side, db)}
            for name in wanted:
                with seen.reading(name):
                    got[name] = dict(read(side, db, ids[name]) or {})
        acc.absorb("traits-parts", f"{side}:{db}", seen)
        return {"traits": got, "unread": seen.unread()}
    got = acc.read("traits", f"{side}:{db}", measure, max_age)
    if got is None:
        why = acc.get("traits", f"{side}:{db}").why or "not read"
        return {}, {t: why for t in wanted}
    return got["traits"], got["unread"]


def _drop_unpaired_traits(src, dst, loose, pairs, same):
    """Settings read on one side only are no ground to compare on: both
    ends of a pair give up theirs, whether the two tables carry one name
    or a mapping renamed the target's. `loose` is [(a side's table, "<side>
    settings", why)] by that side's own names."""
    fold = (lambda x: x) if same else str.lower
    by_dst = {fold(t): t for t in dst}
    ahead = dict(pairs) if pairs is not None else {
        t: by_dst[fold(t)] for t in src if fold(t) in by_dst}
    back = {d: s for s, d in ahead.items()}
    for t, aspect, _ in loose:
        mine, mate = (src, ahead) if aspect.startswith("src") \
            else (dst, back)
        other = dst if mine is src else src
        for shape, name in ((mine, t), (other, mate.get(t))):
            if name in shape:
                shape[name].traits = {}


def compare_schema(eng, db, max_age=0.0, shapes=None, comparison=False,
                   takes=None):
    """`check_schema` for any engine or pair: both sides' catalogues read
    once into the neutral shape and compared by the one comparer
    (`schemaformats.compare`), so a difference is worded alike for every
    pair and a hetero pair is compared as converted. A side that cannot be
    read is an "error" naming which, never an "ok"; a table or a setting
    one side would not give is named as not compared, so an ok is never
    said over it.

    An engine whose tables are not columns (topics, say) hands its two
    sides' `schemaformats.neutral` shapes as `shapes` and gets the same
    comparer; `comparison` answers the `schemaformats.Comparison` itself
    for a caller that words the findings its own way. `takes(table)` is
    the scope the engine asks (else the hop's)."""
    from . import diagnostics, drift, schemaformats
    from .engines.base import Result
    acc = diagnostics.accumulator(eng.hop)
    one = lambda side: _side_engine(eng, side)  # noqa: E731
    name = {side: getattr(one(side), "CANON_ENGINE", "")
            for side in ("src", "dst")}
    same = name["src"] == name["dst"]
    held, loose = {"src": {}, "dst": {}}, []
    if shapes is not None:
        src, dst = shapes
    else:
        cats = {side: side_catalog(eng, side, db, acc, max_age)
                for side in ("src", "dst")}
        bad = [s for s, c in cats.items() if c is None]
        if bad:
            why = "; ".join(f"{s}: {acc.get('catalog', f'{s}:{db}').why}"
                            for s in bad)
            return [Result("schema", db, "error", f"cannot read the tables"
                           f" ({why})", "", "both sides have to be readable"
                           " before anything can be compared")]
        working = {side: drift.transient_among(cats[side].tables)
                   for side in cats}
        scoped = takes or (lambda t: eng.hop.takes_table(db, t))
        takes = {side: (lambda t, side=side: t not in working[side]
                        and scoped(t)) for side in cats}
        shape = {}
        for side in cats:
            traits, missed = side_traits(
                eng, side, db, [t for t in cats[side].tables
                                if takes[side](t)], acc, max_age)
            loose += [(t, f"{side} settings", why)
                      for t, why in missed.items()]
            shape[side] = schemaformats.neutral(name[side], cats[side],
                                                takes[side], traits)
            held[side] = unread_tables(eng, side, db, takes[side])
        src, dst = shape["src"], shape["dst"]
    pairs, renamed = None, {}
    if hasattr(eng, "match_tables") and hasattr(eng, "_renamer"):
        pairs = eng.match_tables(list(src) + list(held["src"]),
                                 list(dst) + list(held["dst"]),
                                 eng._renamer(db))[0]
        pairs = [(a, b) for a, b in pairs
                 if (a in src or a in held["src"])
                 and (b in dst or b in held["dst"])]
        for a, b in pairs:
            if a not in src or b not in dst:
                continue
            try:
                renamed[a] = {s: t for t, s in
                              eng._mapped_types(db, a, {
                                  c.name: None for c in src[a].columns
                              })[1].items() if s != t}
            except Exception:  # noqa: BLE001 - names stand as they are
                renamed[a] = {}
    _drop_unpaired_traits(src, dst, loose, pairs, same)
    got = schemaformats.compare(src, dst, same, pairs, renamed, held)
    got.unread.extend(loose)
    return got if comparison else schemaformats.results(got, db)


def engines_with_own_compare():
    """The engines whose `check_schema` is their own and does not go
    through `compare_schema` - what is left to move onto the one comparer."""
    import inspect

    from . import engines
    from .engines.base import Engine
    out = []
    for name in engines.NAMES:
        try:
            cls = engines._class_for(name)
            fn = cls.check_schema
            if fn is Engine.check_schema:
                continue
            if "compare_schema" not in inspect.getsource(fn):
                out.append(name)
        except Exception:  # noqa: BLE001 - an engine that cannot load here
            continue
    return sorted(out)


from .engines import hooks  # noqa: E402

hooks.fill("verifiers", globals())
