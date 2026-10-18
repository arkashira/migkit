"""What the repair DDL will lock, said before anyone applies it.

migkit hands the operator a `structural-fix.sql` and expects them to run it
against the target - a target that, on the run this was written for, was
serving an application at the time. The check throttles itself while
*reading*; the DDL it emits for *writing* said nothing at all about what it
would block. That asymmetry is the gap this closes.

Classification is static: the statement's shape decides its lock, from
PostgreSQL's documented behaviour. That is enough to answer "will this stop
my application", and it needs no connection, so it works while reviewing a
file. It is not the same as measuring: the definitive answer comes from
executing each statement against a throwaway copy and reading `pg_locks`,
which is what `results lockinfo` does. Where the two could disagree - an
`ADD COLUMN` whose default is volatile, a `SET NOT NULL` already backed by a
validated CHECK - this errs towards naming the heavier lock, because a
warning that turns out to be unnecessary costs a re-read and the opposite
costs an outage.
"""
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Collection, Dict, List, Optional, Tuple, Union

from . import decide


def lock_impact_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for live workload lock impact and execution strategy (Row 663: NEED15)."""
    return (
        decide.Rung(
            "immediate_execution_within_budget",
            "execute DDL immediately within lock budget when no blocking contention is observed",
            needs=(
                decide.Need(
                    "predicted wait within timeout",
                    lambda f: f.get("predicted_wait_seconds", 0.0) <= f.get("max_lock_timeout", 2.0),
                    "predicted lock wait exceeds max lock timeout",
                ),
                decide.Need(
                    "zero conflicting transactions",
                    lambda f: len(f.get("conflicting_transactions", [])) == 0,
                    "active conflicting transactions detected",
                ),
                decide.Need(
                    "queue depth clear",
                    lambda f: f.get("queue_depth", 0) == 0,
                    "waiting lock queue depth is non-zero",
                ),
            ),
            footprint=0,
            leaves="execute statement immediately within lock budget",
        ),
        decide.Rung(
            "bounded_backoff_and_retry",
            "execute statement with exponential backoff and bounded retry under transient lock pressure",
            needs=(
                decide.Need(
                    "predicted wait within total budget",
                    lambda f: f.get("predicted_wait_seconds", 0.0) <= f.get("total_timeout_budget", 30.0),
                    "predicted lock wait exceeds total budget",
                ),
                decide.Need(
                    "retries available",
                    lambda f: f.get("attempt", 0) < f.get("max_retries", 5),
                    "max retries exhausted",
                ),
            ),
            footprint=1,
            leaves="retry execution with bounded backoff under transient contention",
        ),
        decide.Rung(
            "expand_contract_phased_execution",
            "decompose high-locking statement into phased expand-contract steps",
            needs=(
                decide.Need(
                    "phased migration applicable",
                    lambda f: bool(f.get("phased_plan_available", False)),
                    "no phased expand-contract decomposition available",
                ),
                decide.Need(
                    "table rewrite required",
                    lambda f: bool(f.get("table_rewrite", False)),
                    "statement does not require table rewrite",
                ),
            ),
            footprint=2,
            leaves="decompose into phased expand-contract online steps",
        ),
        decide.Rung(
            "quarantine_and_defer_to_maintenance_window",
            "defer execution to low-traffic maintenance window due to sustained lock contention",
            needs=(
                decide.Need(
                    "deferral permitted",
                    lambda f: bool(f.get("deferral_permitted", False)),
                    "deferral to maintenance window not permitted",
                ),
                decide.Need(
                    "contention sustained",
                    lambda f: bool(f.get("sustained_contention", False)),
                    "contention is not sustained",
                ),
            ),
            footprint=3,
            leaves="defer DDL execution to scheduled maintenance window",
        ),
        decide.Rung(
            "refuse_unsafe_lock_contention",
            "refuse execution to protect live transaction throughput from lock exhaustion",
            footprint=4,
            leaves="refuse execution to protect live transaction workload",
        ),
    )


def decide_lock_impact(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the lock_impact_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("live workload lock impact", lock_impact_rungs(), fact_obj)

# PostgreSQL lock modes, least to most disruptive. The names are the ones
# pg_locks reports, so a static verdict and a measured one are comparable.
MODES = [
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
]
SEVERITY = {m: i for i, m in enumerate(MODES)}
# At and above this, concurrent statements start being blocked.
BLOCKS_WRITES = SEVERITY["ShareLock"]
BLOCKS_EVERYTHING = SEVERITY["AccessExclusiveLock"]

_WS = re.compile(r"\s+")


def _norm(stmt):
    return _WS.sub(" ", stmt.strip().rstrip(";")).lower()


# (matcher, lock mode, what it means, a safer way to write it or None)
_RULES = [
    (re.compile(r"^create\s+(unique\s+)?index\s+concurrently\b"),
     "ShareUpdateExclusiveLock",
     "builds without blocking reads or writes", None),
    (re.compile(r"^create\s+(unique\s+)?index\b"),
     "ShareLock",
     "blocks writes to the table for the whole build",
     "CREATE INDEX CONCURRENTLY (cannot run inside a transaction, and a"
     " failure leaves an invalid index to drop)"),
    (re.compile(r"^alter\s+table\b.*\bvalidate\s+constraint\b"),
     "ShareUpdateExclusiveLock",
     "scans the table without blocking reads or writes", None),
    (re.compile(r"^alter\s+table\b.*\badd\s+constraint\b.*\bnot\s+valid\b"),
     "AccessExclusiveLock",
     "brief exclusive lock, no table scan", None),
    (re.compile(r"^alter\s+table\b.*\badd\s+constraint\b.*\bforeign\s+key\b"),
     "AccessExclusiveLock",
     "exclusive lock on both tables while every row is checked",
     "ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT separately"),
    (re.compile(r"^alter\s+table\b.*\badd\s+constraint\b.*\bcheck\b"),
     "AccessExclusiveLock",
     "exclusive lock while every existing row is checked",
     "ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT separately"),
    (re.compile(r"^alter\s+table\b.*\balter\s+column\b.*\btype\b"),
     "AccessExclusiveLock",
     "rewrites the whole table under an exclusive lock", None),
    (re.compile(r"^alter\s+table\b.*\bset\s+not\s+null\b"),
     "AccessExclusiveLock",
     "exclusive lock while every row is checked for nulls",
     "add a validated CHECK (col IS NOT NULL) first; on PostgreSQL 12+ the"
     " SET NOT NULL then skips the scan"),
    (re.compile(r"^alter\s+table\b.*\badd\s+column\b.*\bdefault\b"),
     "AccessExclusiveLock",
     "brief exclusive lock; a volatile default rewrites the table", None),
    (re.compile(r"^alter\s+table\b.*\badd\s+column\b"),
     "AccessExclusiveLock",
     "brief exclusive lock, no rewrite", None),
    (re.compile(r"^(drop|truncate)\b"),
     "AccessExclusiveLock", "removes or empties the object", None),
    (re.compile(r"^alter\s+table\b"),
     "AccessExclusiveLock", "exclusive lock for the duration", None),
    # Measured: COMMENT ON takes ShareUpdateExclusiveLock, not the trivial
    # lock its harmlessness suggests. It still blocks nothing an application
    # does, but it conflicts with VACUUM and with other schema changes.
    (re.compile(r"^comment\s+on\b"),
     "ShareUpdateExclusiveLock",
     "no lock on reads or writes; conflicts with VACUUM and other DDL", None),
    (re.compile(r"^(create|grant|revoke|alter\s+sequence|select\s+setval)\b"),
     "RowExclusiveLock", "no lock on existing table data", None),
]


def classify(statement):
    """(mode, meaning, safer_alternative) for one statement."""
    s = _norm(statement)
    for pat, mode, meaning, safer in _RULES:
        if pat.match(s):
            return mode, meaning, safer
    # Unrecognised DDL is reported as the worst case rather than as harmless:
    # an unclassified statement is a reason to look, not a reason to relax.
    return ("AccessExclusiveLock",
            "not recognised - assume it blocks until checked", None)


def split(sql):
    """Statements from a script, ignoring comments and blank lines.

    Deliberately simple: the differ emits one statement per logical block and
    no dollar-quoted bodies. A script that needs real parsing should be read
    with a real parser, not by this.
    """
    out, buf = [], []
    for line in sql.splitlines():
        if not line.strip() or line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            out.append("\n".join(buf))
            buf = []
    if buf:
        out.append("\n".join(buf))
    return out


def report(sql):
    """(text, counts) describing what a repair script will lock."""
    rows = []
    counts = {"total": 0, "blocks_writes": 0, "blocks_everything": 0}
    for stmt in split(sql):
        mode, meaning, safer = classify(stmt)
        sev = SEVERITY[mode]
        counts["total"] += 1
        if sev >= BLOCKS_WRITES:
            counts["blocks_writes"] += 1
        if sev >= BLOCKS_EVERYTHING:
            counts["blocks_everything"] += 1
        rows.append((mode, meaning, safer, _norm(stmt)[:160]))
    lines = [
        "What this script locks, by statement.",
        "",
        "Static classification from each statement's shape, erring towards the",
        "heavier lock where it is ambiguous. Run it against a copy first if a",
        "definitive answer matters.",
        "",
    ]
    for mode, meaning, safer, stmt in rows:
        flag = "!!" if SEVERITY[mode] >= BLOCKS_EVERYTHING else (
            " !" if SEVERITY[mode] >= BLOCKS_WRITES else "  ")
        lines.append(f"{flag} {mode}: {meaning}")
        lines.append(f"     {stmt}")
        if safer:
            lines.append(f"     safer: {safer}")
        lines.append("")
    lines.append(f"{counts['total']} statements,"
                 f" {counts['blocks_writes']} block writes,"
                 f" {counts['blocks_everything']} block reads and writes.")
    return "\n".join(lines) + "\n", counts


def summary(counts):
    if not counts["total"]:
        return ""
    if not counts["blocks_writes"]:
        return f"{counts['total']} statements, none block concurrent access"
    return (f"{counts['blocks_everything']} of {counts['total']} statements"
            f" block reads and writes,"
            f" {counts['blocks_writes']} block writes")


class LockContentionError(Exception):
    pass


class LockBudgetExceeded(Exception):
    pass


class ScopeViolationError(Exception):
    pass


@dataclass
class LockBehavior:
    engine: str
    operation: str
    lock_mode: str
    table_rewrite: bool
    blocks_reads: bool
    blocks_writes: bool
    safe_alternative: Optional[str] = None
    algorithm: Optional[str] = None
    metadata_only: bool = False
    requires_validation: bool = False


@dataclass
class LockBudget:
    max_lock_timeout: float = 2.0
    max_retries: int = 5
    initial_backoff: float = 0.2
    backoff_factor: float = 2.0
    max_backoff: float = 5.0
    total_timeout_budget: float = 30.0

    @classmethod
    def from_facts(cls, facts: Dict[str, Any]) -> "LockBudget":
        return cls(
            max_lock_timeout=float(facts.get("max_lock_timeout", 2.0)),
            max_retries=int(facts.get("max_retries", 5)),
            initial_backoff=float(facts.get("initial_backoff", 0.2)),
            backoff_factor=float(facts.get("backoff_factor", 2.0)),
            max_backoff=float(facts.get("max_backoff", 5.0)),
            total_timeout_budget=float(facts.get("total_timeout_budget", 30.0)),
        )


@dataclass
class LockWaitPrediction:
    predicted_wait_seconds: float
    conflicting_transactions: List[Dict[str, Any]] = field(default_factory=list)
    queue_depth: int = 0
    exceeds_budget: bool = False
    recommendation: str = "proceed"


@dataclass
class MigrationStep:
    phase: str
    sql: str
    lock_budget: float = 2.0
    description: str = ""
    rollback_sql: Optional[str] = None
    is_batched: bool = False
    reversible: bool = True


@dataclass
class OnlineMigrationPlan:
    statement: str
    engine: str
    table: str
    steps: List[MigrationStep]
    estimated_impact: LockBehavior


def parse_version(v: Any) -> Optional[Tuple[int, ...]]:
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return tuple(int(x) for x in v)
    if isinstance(v, (int, float)):
        return (int(v),)
    parts = re.findall(r"\d+", str(v))
    return tuple(int(p) for p in parts) if parts else (0,)


def _norm_engine(engine: Optional[str]) -> str:
    e = (engine or "postgres").lower().strip()
    if e in ("postgresql", "pg", "postgres"):
        return "postgres"
    if e in ("mysql", "mariadb"):
        return "mysql"
    return e


def lock_timeout_sql(timeout_seconds: float, engine: str = "postgres") -> str:
    eng = _norm_engine(engine)
    if eng == "postgres":
        ms = int(max(1, timeout_seconds * 1000))
        return f"SET LOCAL lock_timeout = '{ms}ms';"
    elif eng == "mysql":
        sec = max(1, int(round(timeout_seconds)))
        return f"SET SESSION lock_wait_timeout = {sec};"
    return f"SET lock_timeout = {timeout_seconds};"


_TABLE_PATTERNS = [
    re.compile(r"alter\s+table\s+(?:only\s+)?([`\"\w\.]+)", re.I),
    re.compile(r"create\s+(?:unique\s+)?index\s+(?:concurrently\s+)?(?:if\s+not\s+exists\s+)?\w+\s+on\s+(?:only\s+)?([`\"\w\.]+)", re.I),
    re.compile(r"drop\s+table\s+(?:if\s+exists\s+)?([`\"\w\.]+)", re.I),
    re.compile(r"truncate\s+(?:table\s+)?([`\"\w\.]+)", re.I),
    re.compile(r"comment\s+on\s+table\s+([`\"\w\.]+)", re.I),
    re.compile(r"from\s+([`\"\w\.]+)", re.I),
    re.compile(r"into\s+([`\"\w\.]+)", re.I),
]


def extract_table(statement: str) -> Optional[str]:
    s = statement.strip()
    for pat in _TABLE_PATTERNS:
        m = pat.search(s)
        if m:
            raw = m.group(1).replace('"', '').replace('`', '')
            return raw
    return None


def check_scope(table: Optional[str], authorized_scope: Optional[Collection[str]]) -> None:
    if authorized_scope is None or table is None:
        return
    tbl_norm = table.strip('"').strip('`').lower()
    tbl_unq = tbl_norm.split(".")[-1]
    scope_norm = set()
    for s in authorized_scope:
        c = s.strip('"').strip('`').lower()
        scope_norm.add(c)
        scope_norm.add(c.split(".")[-1])
    if tbl_norm not in scope_norm and tbl_unq not in scope_norm:
        raise ScopeViolationError(f"Table '{table}' outside authorized scope: {sorted(list(scope_norm))}")


_VOLATILE_FUNCS = re.compile(
    r"\b(now|random|clock_timestamp|timeofday|statement_timestamp|uuid_generate_v4|gen_random_uuid)\s*\(",
    re.I
)


def probe_lock_behavior(
    statement: str,
    engine: str = "postgres",
    version: Any = None,
    runner: Optional[Any] = None,
) -> LockBehavior:
    eng = _norm_engine(engine)
    ver = parse_version(version)
    if ver is None and runner is not None:
        try:
            if eng == "postgres":
                raw = _run_sql(runner, "SHOW server_version_num;")
                if raw:
                    num = int(str(raw[0][0] if isinstance(raw[0], (list, tuple)) else raw).strip())
                    ver = (num // 10000, (num % 10000) // 100)
            elif eng == "mysql":
                raw = _run_sql(runner, "SELECT VERSION();")
                if raw:
                    ver = parse_version(raw[0][0] if isinstance(raw[0], (list, tuple)) else raw)
        except Exception:
            pass

    s = _norm(statement)
    if eng == "postgres":
        if re.search(r"^create\s+(unique\s+)?index\s+concurrently\b", s):
            return LockBehavior(
                engine="postgres",
                operation="CREATE_INDEX",
                lock_mode="ShareUpdateExclusiveLock",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=False,
                metadata_only=False,
            )
        if re.search(r"^create\s+(unique\s+)?index\b", s):
            return LockBehavior(
                engine="postgres",
                operation="CREATE_INDEX",
                lock_mode="ShareLock",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=True,
                safe_alternative="CREATE INDEX CONCURRENTLY",
            )
        if re.search(r"^drop\s+index\s+concurrently\b", s):
            return LockBehavior(
                engine="postgres",
                operation="DROP_INDEX",
                lock_mode="ShareUpdateExclusiveLock",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=False,
            )
        if re.search(r"^drop\s+index\b", s):
            return LockBehavior(
                engine="postgres",
                operation="DROP_INDEX",
                lock_mode="AccessExclusiveLock",
                table_rewrite=False,
                blocks_reads=True,
                blocks_writes=True,
                safe_alternative="DROP INDEX CONCURRENTLY",
            )
        if re.search(r"^alter\s+table\b.*\bvalidate\s+constraint\b", s):
            return LockBehavior(
                engine="postgres",
                operation="VALIDATE_CONSTRAINT",
                lock_mode="ShareUpdateExclusiveLock",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=False,
            )
        if re.search(r"^alter\s+table\b.*\badd\s+constraint\b.*\bnot\s+valid\b", s):
            return LockBehavior(
                engine="postgres",
                operation="ADD_CONSTRAINT",
                lock_mode="AccessExclusiveLock",
                table_rewrite=False,
                blocks_reads=True,
                blocks_writes=True,
                metadata_only=True,
                requires_validation=True,
            )
        if re.search(r"^alter\s+table\b.*\badd\s+constraint\b.*\bforeign\s+key\b", s):
            return LockBehavior(
                engine="postgres",
                operation="ADD_CONSTRAINT_FK",
                lock_mode="AccessExclusiveLock",
                table_rewrite=False,
                blocks_reads=True,
                blocks_writes=True,
                requires_validation=True,
                safe_alternative="ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT separately",
            )
        if re.search(r"^alter\s+table\b.*\badd\s+constraint\b.*\bcheck\b", s):
            return LockBehavior(
                engine="postgres",
                operation="ADD_CONSTRAINT_CHECK",
                lock_mode="AccessExclusiveLock",
                table_rewrite=False,
                blocks_reads=True,
                blocks_writes=True,
                requires_validation=True,
                safe_alternative="ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT separately",
            )
        if re.search(r"^alter\s+table\b.*\balter\s+column\b.*\btype\b", s):
            is_varchar_expand = bool(re.search(r"\btype\s+varchar\s*\(\s*\d+\s*\)", s))
            if is_varchar_expand and (ver is None or ver >= (9, 2)):
                return LockBehavior(
                    engine="postgres",
                    operation="ALTER_COLUMN_TYPE",
                    lock_mode="AccessExclusiveLock",
                    table_rewrite=False,
                    blocks_reads=True,
                    blocks_writes=True,
                    metadata_only=True,
                )
            return LockBehavior(
                engine="postgres",
                operation="ALTER_COLUMN_TYPE",
                lock_mode="AccessExclusiveLock",
                table_rewrite=True,
                blocks_reads=True,
                blocks_writes=True,
                safe_alternative="expand/backfill/contract: add new column, backfill in batches, drop/rename old column",
            )
        if re.search(r"^alter\s+table\b.*\bset\s+not\s+null\b", s):
            skips_scan = ver is not None and ver >= (12,)
            return LockBehavior(
                engine="postgres",
                operation="SET_NOT_NULL",
                lock_mode="AccessExclusiveLock",
                table_rewrite=False,
                blocks_reads=True,
                blocks_writes=True,
                metadata_only=skips_scan,
                requires_validation=not skips_scan,
                safe_alternative="add a validated CHECK (col IS NOT NULL) first; on PostgreSQL 12+ the SET NOT NULL then skips the scan",
            )
        if re.search(r"^alter\s+table\b.*\badd\s+column\b", s):
            has_default = bool(re.search(r"\bdefault\b", s))
            is_volatile = bool(_VOLATILE_FUNCS.search(s))
            if is_volatile:
                rewrite = True
            elif has_default:
                rewrite = ver is None or ver < (11,)
            else:
                rewrite = False
            return LockBehavior(
                engine="postgres",
                operation="ADD_COLUMN",
                lock_mode="AccessExclusiveLock",
                table_rewrite=rewrite,
                blocks_reads=True,
                blocks_writes=True,
                metadata_only=not rewrite,
            )
        if re.search(r"^alter\s+table\b.*\bdrop\s+column\b", s):
            return LockBehavior(
                engine="postgres",
                operation="DROP_COLUMN",
                lock_mode="AccessExclusiveLock",
                table_rewrite=False,
                blocks_reads=True,
                blocks_writes=True,
                metadata_only=True,
            )
        if re.search(r"^comment\s+on\b", s):
            return LockBehavior(
                engine="postgres",
                operation="COMMENT",
                lock_mode="ShareUpdateExclusiveLock",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=False,
                metadata_only=True,
            )
        if re.search(r"^(vacuum\s+full)\b", s):
            return LockBehavior(
                engine="postgres",
                operation="VACUUM_FULL",
                lock_mode="AccessExclusiveLock",
                table_rewrite=True,
                blocks_reads=True,
                blocks_writes=True,
            )
        if re.search(r"^(vacuum|analyze)\b", s):
            return LockBehavior(
                engine="postgres",
                operation="VACUUM",
                lock_mode="ShareUpdateExclusiveLock",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=False,
            )
        mode, meaning, safer = classify(statement)
        sev = SEVERITY.get(mode, BLOCKS_EVERYTHING)
        return LockBehavior(
            engine="postgres",
            operation="OTHER",
            lock_mode=mode,
            table_rewrite=(sev >= BLOCKS_EVERYTHING),
            blocks_reads=(sev >= BLOCKS_EVERYTHING),
            blocks_writes=(sev >= BLOCKS_WRITES),
            safe_alternative=safer,
        )

    elif eng == "mysql":
        if re.search(r"^(create\s+(unique\s+)?index|alter\s+table\b.*\badd\s+index\b)", s):
            return LockBehavior(
                engine="mysql",
                operation="CREATE_INDEX",
                lock_mode="NONE",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=False,
                algorithm="INPLACE",
            )
        if re.search(r"^(drop\s+index|alter\s+table\b.*\bdrop\s+index\b)", s):
            return LockBehavior(
                engine="mysql",
                operation="DROP_INDEX",
                lock_mode="NONE",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=False,
                algorithm="INPLACE",
            )
        if re.search(r"^alter\s+table\b.*\badd\s+(constraint\b.*\b)?foreign\s+key\b", s):
            return LockBehavior(
                engine="mysql",
                operation="ADD_CONSTRAINT_FK",
                lock_mode="SHARED",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=True,
                algorithm="INPLACE",
                safe_alternative="disable foreign key checks during load or add with staged validation",
            )
        if re.search(r"^alter\s+table\b.*\b(modify|change)\s+(column\s+)?", s):
            is_rename_only = bool(re.search(r"\brename\s+column\b", s))
            if is_rename_only:
                instant = ver is not None and ver >= (8, 0)
                return LockBehavior(
                    engine="mysql",
                    operation="RENAME_COLUMN",
                    lock_mode="NONE",
                    table_rewrite=False,
                    blocks_reads=False,
                    blocks_writes=False,
                    algorithm="INSTANT" if instant else "INPLACE",
                    metadata_only=True,
                )
            return LockBehavior(
                engine="mysql",
                operation="ALTER_COLUMN_TYPE",
                lock_mode="SHARED",
                table_rewrite=True,
                blocks_reads=False,
                blocks_writes=True,
                algorithm="COPY",
                safe_alternative="expand/backfill/contract: add new column, backfill in batches, switch pointers",
            )
        if re.search(r"^alter\s+table\b.*\brename\s+column\b", s):
            instant = ver is not None and ver >= (8, 0)
            return LockBehavior(
                engine="mysql",
                operation="RENAME_COLUMN",
                lock_mode="NONE",
                table_rewrite=False,
                blocks_reads=False,
                blocks_writes=False,
                algorithm="INSTANT" if instant else "INPLACE",
                metadata_only=True,
            )
        if re.search(r"^alter\s+table\b.*\badd\s+(column\s+)?", s):
            instant = ver is not None and ver >= (8, 0, 12)
            return LockBehavior(
                engine="mysql",
                operation="ADD_COLUMN",
                lock_mode="NONE",
                table_rewrite=not instant,
                blocks_reads=False,
                blocks_writes=False,
                algorithm="INSTANT" if instant else "INPLACE",
                metadata_only=instant,
            )
        if re.search(r"^alter\s+table\b.*\bdrop\s+(column\s+)?", s):
            instant = ver is not None and ver >= (8, 0, 29)
            return LockBehavior(
                engine="mysql",
                operation="DROP_COLUMN",
                lock_mode="NONE",
                table_rewrite=not instant,
                blocks_reads=False,
                blocks_writes=False,
                algorithm="INSTANT" if instant else "INPLACE",
                metadata_only=instant,
            )
        if re.search(r"^(drop|truncate)\b", s):
            return LockBehavior(
                engine="mysql",
                operation="DROP_TABLE",
                lock_mode="EXCLUSIVE",
                table_rewrite=True,
                blocks_reads=True,
                blocks_writes=True,
                algorithm="COPY",
            )
        return LockBehavior(
            engine="mysql",
            operation="OTHER",
            lock_mode="EXCLUSIVE",
            table_rewrite=True,
            blocks_reads=True,
            blocks_writes=True,
            algorithm="INPLACE",
        )

    return LockBehavior(
        engine=eng,
        operation="OTHER",
        lock_mode="EXCLUSIVE",
        table_rewrite=True,
        blocks_reads=True,
        blocks_writes=True,
    )


_PG_CONFLICTS = {
    "AccessShareLock": {"AccessExclusiveLock"},
    "RowShareLock": {"ExclusiveLock", "AccessExclusiveLock"},
    "RowExclusiveLock": {"ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"},
    "ShareUpdateExclusiveLock": {"ShareUpdateExclusiveLock", "ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"},
    "ShareLock": {"RowExclusiveLock", "ShareUpdateExclusiveLock", "ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"},
    "ShareRowExclusiveLock": {"RowExclusiveLock", "ShareUpdateExclusiveLock", "ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"},
    "ExclusiveLock": {"RowShareLock", "RowExclusiveLock", "ShareUpdateExclusiveLock", "ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"},
    "AccessExclusiveLock": set(MODES),
}

_MYSQL_CONFLICTS = {
    "NONE": {"EXCLUSIVE"},
    "SHARED": {"SHARED", "EXCLUSIVE", "WRITE"},
    "EXCLUSIVE": {"NONE", "SHARED", "EXCLUSIVE", "READ", "WRITE"},
}


def predict_lock_wait(
    active_workload: List[Dict[str, Any]],
    target_table: Optional[str] = None,
    operation: Optional[str] = None,
    budget: Optional[LockBudget] = None,
    engine: str = "postgres",
) -> LockWaitPrediction:
    eng = _norm_engine(engine)
    tbl = target_table or (extract_table(operation) if operation else None)
    tbl_norm = tbl.strip('"').strip('`').lower() if tbl else None

    our_mode = "AccessExclusiveLock" if eng == "postgres" else "EXCLUSIVE"
    if operation:
        beh = probe_lock_behavior(operation, engine=eng)
        our_mode = beh.lock_mode

    conflicting = []
    queue_waiters = 0

    for item in active_workload:
        item_tbl = item.get("table") or extract_table(item.get("query", ""))
        if item_tbl and tbl_norm:
            item_tbl_norm = item_tbl.strip('"').strip('`').lower().split(".")[-1]
            if item_tbl_norm != tbl_norm.split(".")[-1]:
                continue

        item_mode = item.get("lock_mode")
        is_conflict = False

        if eng == "postgres":
            if not item_mode:
                q = (item.get("query") or "").lower()
                if any(w in q for w in ("insert", "update", "delete")):
                    item_mode = "RowExclusiveLock"
                elif "select" in q:
                    item_mode = "AccessShareLock"
                else:
                    item_mode = "AccessExclusiveLock"
            conflicting_set = _PG_CONFLICTS.get(our_mode, set())
            if item_mode in conflicting_set or our_mode in _PG_CONFLICTS.get(item_mode, set()):
                is_conflict = True
        elif eng == "mysql":
            if not item_mode:
                q = (item.get("query") or "").lower()
                if any(w in q for w in ("insert", "update", "delete")):
                    item_mode = "WRITE"
                elif "select" in q:
                    item_mode = "READ"
                else:
                    item_mode = "EXCLUSIVE"
            conflicting_set = _MYSQL_CONFLICTS.get(our_mode, set())
            if item_mode in conflicting_set:
                is_conflict = True

        if is_conflict:
            conflicting.append(item)
            if item.get("waiting", False):
                queue_waiters += 1

    if not conflicting:
        return LockWaitPrediction(
            predicted_wait_seconds=0.0,
            conflicting_transactions=[],
            queue_depth=0,
            exceeds_budget=False,
            recommendation="proceed",
        )

    running_durations = [
        float(item.get("duration", 0.0)) for item in conflicting if not item.get("waiting", False)
    ]
    max_active = max(running_durations) if running_durations else 0.5
    predicted_wait = max_active + (queue_waiters * 0.5)

    limit = budget.max_lock_timeout if budget else 2.0
    exceeds = predicted_wait > limit
    rec = "proceed"
    if exceeds:
        rec = "abort" if (budget and predicted_wait > budget.total_timeout_budget) else "defer"

    return LockWaitPrediction(
        predicted_wait_seconds=round(predicted_wait, 2),
        conflicting_transactions=conflicting,
        queue_depth=queue_waiters,
        exceeds_budget=exceeds,
        recommendation=rec,
    )


def plan_expand_backfill_contract(
    statement: str,
    engine: str = "postgres",
    version: Any = None,
    authorized_scope: Optional[Collection[str]] = None,
    batch_size: int = 1000,
    pk_column: str = "id",
    lock_budget: float = 2.0,
) -> OnlineMigrationPlan:
    eng = _norm_engine(engine)
    s = _norm(statement)
    tbl = extract_table(statement)
    check_scope(tbl, authorized_scope)
    beh = probe_lock_behavior(statement, engine=eng, version=version)
    target_table = tbl or "t"

    m_type = re.search(r"alter\s+table\s+[`\"\w\.]+\s+alter\s+column\s+([`\"\w]+)\s+type\s+(.+?)(?:;|\s*$)", s)
    m_mysql_mod = re.search(r"alter\s+table\s+[`\"\w\.]+\s+(?:modify|change)\s+(?:column\s+)?([`\"\w]+)\s+(.+?)(?:;|\s*$)", s)

    if m_type or m_mysql_mod:
        col = (m_type.group(1) if m_type else m_mysql_mod.group(1)).strip('`" ')
        new_type = (m_type.group(2) if m_type else m_mysql_mod.group(2)).strip()
        temp_col = f"{col}_migkit_new"
        old_col = f"{col}_migkit_old"

        steps = [
            MigrationStep(
                phase="expand",
                sql=f"ALTER TABLE {target_table} ADD COLUMN {temp_col} {new_type};",
                lock_budget=lock_budget,
                description=f"Add shadow column {temp_col} without rewriting existing data",
                rollback_sql=f"ALTER TABLE {target_table} DROP COLUMN IF EXISTS {temp_col};",
            ),
            MigrationStep(
                phase="backfill",
                sql=f"UPDATE {target_table} SET {temp_col} = {col} WHERE {temp_col} IS NULL;",
                lock_budget=lock_budget,
                description=f"Backfill {temp_col} in batches of {batch_size}",
                rollback_sql=f"ALTER TABLE {target_table} DROP COLUMN IF EXISTS {temp_col};",
                is_batched=True,
            ),
            MigrationStep(
                phase="contract",
                sql=(
                    f"ALTER TABLE {target_table} RENAME COLUMN {col} TO {old_col}; "
                    f"ALTER TABLE {target_table} RENAME COLUMN {temp_col} TO {col};"
                    if eng == "postgres" else
                    f"ALTER TABLE {target_table} RENAME COLUMN {col} TO {old_col}, RENAME COLUMN {temp_col} TO {col};"
                ),
                lock_budget=lock_budget,
                description=f"Atomic cutover swap from {col} to {temp_col}",
                rollback_sql=(
                    f"ALTER TABLE {target_table} RENAME COLUMN {col} TO {temp_col}; "
                    f"ALTER TABLE {target_table} RENAME COLUMN {old_col} TO {col};"
                ),
            ),
            MigrationStep(
                phase="cleanup",
                sql=f"ALTER TABLE {target_table} DROP COLUMN {old_col};",
                lock_budget=lock_budget,
                description=f"Drop decommissioned column {old_col}",
                rollback_sql=None,
            ),
        ]
        return OnlineMigrationPlan(statement=statement, engine=eng, table=target_table, steps=steps, estimated_impact=beh)

    m_fk = re.search(r"alter\s+table\s+[`\"\w\.]+\s+add\s+constraint\s+([`\"\w]+)\s+foreign\s+key\s*\(([^)]+)\)\s*references\s+([`\"\w\.]+)\s*\(([^)]+)\)", s)
    if m_fk and eng == "postgres":
        cname = m_fk.group(1).strip('`" ')
        cols = m_fk.group(2).strip()
        reftbl = m_fk.group(3).strip()
        refcols = m_fk.group(4).strip()
        steps = [
            MigrationStep(
                phase="expand",
                sql=f"ALTER TABLE {target_table} ADD CONSTRAINT {cname} FOREIGN KEY ({cols}) REFERENCES {reftbl} ({refcols}) NOT VALID;",
                lock_budget=lock_budget,
                description=f"Attach foreign key {cname} without blocking table scan",
                rollback_sql=f"ALTER TABLE {target_table} DROP CONSTRAINT IF EXISTS {cname};",
            ),
            MigrationStep(
                phase="backfill",
                sql=f"ALTER TABLE {target_table} VALIDATE CONSTRAINT {cname};",
                lock_budget=lock_budget,
                description=f"Validate foreign key {cname} non-blocking",
                rollback_sql=f"ALTER TABLE {target_table} DROP CONSTRAINT IF EXISTS {cname};",
            ),
        ]
        return OnlineMigrationPlan(statement=statement, engine=eng, table=target_table, steps=steps, estimated_impact=beh)

    m_chk = re.search(r"alter\s+table\s+[`\"\w\.]+\s+add\s+constraint\s+([`\"\w]+)\s+check\s*\((.+)\)", s)
    if m_chk and eng == "postgres" and "not valid" not in s:
        cname = m_chk.group(1).strip('`" ')
        expr = m_chk.group(2).strip()
        steps = [
            MigrationStep(
                phase="expand",
                sql=f"ALTER TABLE {target_table} ADD CONSTRAINT {cname} CHECK ({expr}) NOT VALID;",
                lock_budget=lock_budget,
                description=f"Attach check constraint {cname} without blocking table scan",
                rollback_sql=f"ALTER TABLE {target_table} DROP CONSTRAINT IF EXISTS {cname};",
            ),
            MigrationStep(
                phase="backfill",
                sql=f"ALTER TABLE {target_table} VALIDATE CONSTRAINT {cname};",
                lock_budget=lock_budget,
                description=f"Validate check constraint {cname} non-blocking",
                rollback_sql=f"ALTER TABLE {target_table} DROP CONSTRAINT IF EXISTS {cname};",
            ),
        ]
        return OnlineMigrationPlan(statement=statement, engine=eng, table=target_table, steps=steps, estimated_impact=beh)

    m_idx = re.search(r"^create\s+(unique\s+)?index\s+([`\"\w]+)\s+on\s+[`\"\w\.]+\s*\((.+)\)", s)
    if m_idx:
        uniq = m_idx.group(1) or ""
        iname = m_idx.group(2).strip('`" ')
        cols = m_idx.group(3).strip()
        if eng == "postgres" and "concurrently" not in s:
            steps = [
                MigrationStep(
                    phase="expand",
                    sql=f"CREATE {uniq}INDEX CONCURRENTLY {iname} ON {target_table} ({cols});",
                    lock_budget=lock_budget,
                    description=f"Build index {iname} concurrently without blocking writes",
                    rollback_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {iname};",
                )
            ]
            return OnlineMigrationPlan(statement=statement, engine=eng, table=target_table, steps=steps, estimated_impact=beh)
        elif eng == "mysql":
            steps = [
                MigrationStep(
                    phase="expand",
                    sql=f"CREATE {uniq}INDEX {iname} ON {target_table} ({cols}) ALGORITHM=INPLACE LOCK=NONE;",
                    lock_budget=lock_budget,
                    description=f"Build index {iname} online without blocking DML",
                    rollback_sql=f"DROP INDEX {iname} ON {target_table};",
                )
            ]
            return OnlineMigrationPlan(statement=statement, engine=eng, table=target_table, steps=steps, estimated_impact=beh)

    steps = [
        MigrationStep(
            phase="execute",
            sql=statement if statement.strip().endswith(";") else statement + ";",
            lock_budget=lock_budget,
            description="Execute statement under lock budget",
            rollback_sql=None,
        )
    ]
    return OnlineMigrationPlan(statement=statement, engine=eng, table=target_table, steps=steps, estimated_impact=beh)


def _is_lock_contention(exc: Exception) -> bool:
    if isinstance(exc, LockContentionError):
        return True
    msg = str(exc).lower()
    patterns = (
        "lock timeout",
        "lock_timeout",
        "lock wait timeout",
        "lock_wait_timeout",
        "canceling statement due to lock timeout",
        "could not obtain lock",
        "deadlock detected",
        "deadlock found",
        "lock contention",
        "55p03",
        "40p01",
        "57014",
        "1205",
        "1213",
    )
    return any(p in msg for p in patterns)


def _run_sql(runner: Any, sql: str) -> Any:
    if callable(runner):
        return runner(sql)
    elif hasattr(runner, "cursor"):
        cur = runner.cursor()
        try:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return None
        finally:
            cur.close()
    raise TypeError(f"Unsupported runner: {type(runner)}")


def execute_with_lock_budget(
    runner: Any,
    statement: str,
    budget: Optional[LockBudget] = None,
    engine: str = "postgres",
    table: Optional[str] = None,
    active_workload_fn: Optional[Callable[[], List[Dict[str, Any]]]] = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.monotonic,
) -> Dict[str, Any]:
    b = budget or LockBudget()
    eng = _norm_engine(engine)
    tbl = table or extract_table(statement)
    t0 = clock_fn()

    if active_workload_fn is not None:
        try:
            wl = active_workload_fn()
            pred = predict_lock_wait(wl, target_table=tbl, operation=statement, budget=b, engine=eng)
            if pred.exceeds_budget:
                backoff = min(b.initial_backoff, b.max_backoff)
                sleep_fn(backoff)
        except Exception:
            pass

    for attempt in range(b.max_retries):
        elapsed = clock_fn() - t0
        if elapsed >= b.total_timeout_budget:
            raise LockBudgetExceeded(
                f"Total lock budget ({b.total_timeout_budget}s) exceeded before attempt {attempt + 1}"
            )
        timeout_sql = lock_timeout_sql(b.max_lock_timeout, engine=eng)
        try:
            _run_sql(runner, timeout_sql)
            res = _run_sql(runner, statement)
            if hasattr(runner, "commit") and callable(runner.commit):
                runner.commit()
            elif eng == "postgres":
                try:
                    _run_sql(runner, "COMMIT;")
                except Exception:
                    pass
            return {
                "status": "applied",
                "attempts": attempt + 1,
                "elapsed_seconds": round(clock_fn() - t0, 4),
                "statement": statement,
                "result": res,
            }
        except Exception as exc:
            try:
                _run_sql(runner, "ROLLBACK;")
            except Exception:
                pass

            if _is_lock_contention(exc):
                if attempt + 1 >= b.max_retries:
                    raise LockBudgetExceeded(
                        f"Lock contention: max retries ({b.max_retries}) exhausted: {exc}"
                    ) from exc
                backoff = min(b.initial_backoff * (b.backoff_factor ** attempt), b.max_backoff)
                if (clock_fn() - t0) + backoff > b.total_timeout_budget:
                    raise LockBudgetExceeded(
                        f"Lock contention: total budget ({b.total_timeout_budget}s) exceeded during backoff"
                    ) from exc
                sleep_fn(backoff)
            else:
                raise

    raise LockBudgetExceeded(f"Lock contention: exceeded max retries {b.max_retries}")


def execute_plan_with_budget(
    runner: Any,
    plan: OnlineMigrationPlan,
    budget: Optional[LockBudget] = None,
    engine: str = "postgres",
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.monotonic,
) -> Dict[str, Any]:
    b = budget or LockBudget()
    executed_steps = []
    t0 = clock_fn()

    try:
        for idx, step in enumerate(plan.steps):
            step_budget = LockBudget(
                max_lock_timeout=step.lock_budget,
                max_retries=b.max_retries,
                initial_backoff=b.initial_backoff,
                backoff_factor=b.backoff_factor,
                max_backoff=b.max_backoff,
                total_timeout_budget=max(1.0, b.total_timeout_budget - (clock_fn() - t0)),
            )
            execute_with_lock_budget(
                runner,
                step.sql,
                budget=step_budget,
                engine=engine,
                table=plan.table,
                sleep_fn=sleep_fn,
                clock_fn=clock_fn,
            )
            executed_steps.append(step)

        return {
            "status": "completed",
            "steps_executed": len(executed_steps),
            "table": plan.table,
            "elapsed_seconds": round(clock_fn() - t0, 4),
        }
    except Exception:
        for executed in reversed(executed_steps):
            if executed.rollback_sql:
                try:
                    rb_budget = LockBudget(max_lock_timeout=2.0, max_retries=2, total_timeout_budget=5.0)
                    execute_with_lock_budget(
                        runner,
                        executed.rollback_sql,
                        budget=rb_budget,
                        engine=engine,
                        table=plan.table,
                        sleep_fn=sleep_fn,
                        clock_fn=clock_fn,
                    )
                except Exception:
                    pass
        raise


def live_workload_probe_sql(engine: str = "postgres", table: Optional[str] = None) -> str:
    eng = _norm_engine(engine)
    if eng == "postgres":
        tbl_filter = f"AND c.relname = '{table}'" if table else ""
        return (
            "SELECT a.pid, a.query, "
            "EXTRACT(EPOCH FROM (now() - a.query_start)) AS duration, "
            "a.wait_event, a.wait_event_type, l.mode AS lock_mode, "
            "c.relname AS table_name, l.granted "
            "FROM pg_stat_activity a "
            "LEFT JOIN pg_locks l ON a.pid = l.pid "
            "LEFT JOIN pg_class c ON l.relation = c.oid "
            f"WHERE a.pid <> pg_backend_pid() AND a.state = 'active' {tbl_filter};"
        )
    elif eng == "mysql":
        return (
            "SELECT trx_id, trx_mysql_thread_id AS pid, trx_query AS query, "
            "TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS duration, "
            "trx_operation_state, trx_tables_locked "
            "FROM information_schema.innodb_trx;"
        )
    return ""


def probe_live_locks(runner: Any, engine: str = "postgres", table: Optional[str] = None) -> List[Dict[str, Any]]:
    sql = live_workload_probe_sql(engine=engine, table=table)
    if not sql:
        return []
    try:
        rows = _run_sql(runner, sql)
        if not rows:
            return []
        eng = _norm_engine(engine)
        out = []
        if eng == "postgres":
            for r in rows:
                out.append({
                    "pid": r[0] if len(r) > 0 else None,
                    "query": r[1] if len(r) > 1 else "",
                    "duration": float(r[2]) if len(r) > 2 and r[2] is not None else 0.0,
                    "wait_event": r[3] if len(r) > 3 else None,
                    "lock_mode": r[5] if len(r) > 5 else None,
                    "table": r[6] if len(r) > 6 else table,
                    "waiting": not bool(r[7]) if len(r) > 7 else False,
                })
        elif eng == "mysql":
            for r in rows:
                out.append({
                    "pid": r[1] if len(r) > 1 else r[0],
                    "query": r[2] if len(r) > 2 else "",
                    "duration": float(r[3]) if len(r) > 3 and r[3] is not None else 0.0,
                    "lock_mode": "EXCLUSIVE",
                    "table": table,
                    "waiting": False,
                })
        return out
    except Exception:
        return []

