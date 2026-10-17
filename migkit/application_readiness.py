"""Prove application-visible readiness beyond data equality (Row 601: SMART-N18.application-readiness).

Extends storedcode/appscan/readiness and SUPER.query-guard using `decide.Rung` and
`decide.climb(work, rungs, facts)`.

Separates three independent verification tiers:
1. Value Equality: row count parity, checksum matching, column type compatibility.
2. Object & Security Parity: application role sequence/table grants, search_path resolution,
   routine executability, trigger validity and status.
3. Application Behavior: approved representative reads, isolated write fixtures, constraint
   enforcement (CHECK, FK, UNIQUE), sequence monotonicity/privileges, routine execution,
   and empirical latency parity.

Safeguards EXPLAIN ANALYZE execution: external effects require isolated authorized
environments. Equal rows with a broken trigger, missing sequence grant, or wrong
search_path block the switch; unavailable workload evidence stays explicitly unavailable.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import math
import pathlib
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from . import checkpoint, decide, lease

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class WorkloadEvidenceStatus(str, enum.Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    PARTIAL = "PARTIAL"


class ReadinessTier(str, enum.Enum):
    VALUE_EQUALITY = "value_equality"
    OBJECT_SECURITY_PARITY = "object_security_parity"
    APPLICATION_BEHAVIOR = "application_behavior"


class ApplicationReadinessError(Exception):
    """Base error for application readiness failures."""


class UnsafeEndpointError(ApplicationReadinessError):
    """Raised when an endpoint host violates loopback or RFC1918 10.0.0.x rules."""


class UnsafeExecutionError(ApplicationReadinessError):
    """Raised when EXPLAIN ANALYZE or DML is attempted without an isolated authorized environment."""


def validate_safe_endpoint(host: str) -> str:
    """Validate that endpoint host is loopback (127.0.0.1, localhost) or 10.0.0.x."""
    cleaned = (host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host must not be empty")
    if cleaned in ("127.0.0.1", "localhost") or cleaned.startswith("10.0.0."):
        return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x permitted"
    )


# -----------------------------------------------------------------------------
# Tier 1: Value Equality Specification
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ValueEqualityVerification:
    """Value equality tier: confirms rows, schemas, and checksums match."""

    tables_checked: int
    rows_source: int
    rows_target: int
    checksum_match: bool
    columns_match: bool
    verified: bool
    details: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_readings(
        cls,
        *,
        tables_checked: int,
        rows_source: int,
        rows_target: int,
        checksum_match: bool,
        columns_match: bool,
        details: Optional[Dict[str, Any]] = None,
    ) -> ValueEqualityVerification:
        verified = (
            tables_checked > 0
            and rows_source == rows_target
            and bool(checksum_match)
            and bool(columns_match)
        )
        return cls(
            tables_checked=tables_checked,
            rows_source=rows_source,
            rows_target=rows_target,
            checksum_match=checksum_match,
            columns_match=columns_match,
            verified=verified,
            details=details or {},
        )


# -----------------------------------------------------------------------------
# Tier 2: Object & Security Parity Specification
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class TriggerSpec:
    name: str
    table_name: str
    enabled: bool
    has_valid_function: bool
    action_timing: str = "BEFORE"  # "BEFORE", "AFTER", "INSTEAD OF"
    events: Tuple[str, ...] = ("INSERT",)


@dataclass(frozen=True)
class SequenceGrantSpec:
    sequence_name: str
    role: str
    privileges: Tuple[str, ...]  # ("USAGE", "SELECT", "UPDATE")
    is_granted: bool


@dataclass(frozen=True)
class TableGrantSpec:
    table_name: str
    role: str
    privileges: Tuple[str, ...]  # ("SELECT", "INSERT", "UPDATE", "DELETE")
    is_granted: bool


@dataclass(frozen=True)
class RoutineSpec:
    routine_name: str
    valid: bool
    executable_by_role: bool
    return_type: str = "void"


@dataclass(frozen=True)
class ObjectSecurityParity:
    """Object and security tier: role grants, search_path, triggers, and routines."""

    application_role: str
    search_path: Tuple[str, ...]
    expected_search_path: Tuple[str, ...]
    search_path_valid: bool
    sequence_grants: Tuple[SequenceGrantSpec, ...]
    table_grants: Tuple[TableGrantSpec, ...]
    triggers: Tuple[TriggerSpec, ...]
    routines: Tuple[RoutineSpec, ...]
    verified: bool
    defects: Tuple[str, ...]

    @classmethod
    def evaluate(
        cls,
        *,
        application_role: str,
        search_path: Sequence[str],
        expected_search_path: Sequence[str],
        sequence_grants: Iterable[SequenceGrantSpec] = (),
        table_grants: Iterable[TableGrantSpec] = (),
        triggers: Iterable[TriggerSpec] = (),
        routines: Iterable[RoutineSpec] = (),
    ) -> ObjectSecurityParity:
        sp_actual = tuple(p.strip().lower() for p in search_path if p.strip())
        sp_expected = tuple(p.strip().lower() for p in expected_search_path if p.strip())
        sp_valid = bool(sp_actual) and (sp_actual == sp_expected or (len(sp_expected) > 0 and sp_expected[0] == sp_actual[0]))

        seq_list = tuple(sequence_grants)
        tbl_list = tuple(table_grants)
        trg_list = tuple(triggers)
        rtn_list = tuple(routines)

        defects: List[str] = []
        if not sp_valid:
            defects.append(
                f"wrong search_path: actual '{','.join(sp_actual)}' does not resolve expected '{','.join(sp_expected)}'"
            )

        for sg in seq_list:
            if not sg.is_granted:
                defects.append(
                    f"missing sequence grant '{','.join(sg.privileges)}' on '{sg.sequence_name}' for role '{sg.role}'"
                )

        for tg in tbl_list:
            if not tg.is_granted:
                defects.append(
                    f"missing table grant '{','.join(tg.privileges)}' on '{tg.table_name}' for role '{tg.role}'"
                )

        for trg in trg_list:
            if not trg.enabled:
                defects.append(f"trigger '{trg.name}' on '{trg.table_name}' is disabled")
            elif not trg.has_valid_function:
                defects.append(f"trigger '{trg.name}' on '{trg.table_name}' references broken function")

        for r in rtn_list:
            if not r.valid:
                defects.append(f"routine '{r.routine_name}' is invalid or failed compilation")
            elif not r.executable_by_role:
                defects.append(f"routine '{r.routine_name}' lacks execute grant for role '{application_role}'")

        verified = (len(defects) == 0 and sp_valid)
        return cls(
            application_role=application_role,
            search_path=sp_actual,
            expected_search_path=sp_expected,
            search_path_valid=sp_valid,
            sequence_grants=seq_list,
            table_grants=tbl_list,
            triggers=trg_list,
            routines=rtn_list,
            verified=verified,
            defects=tuple(defects),
        )


# -----------------------------------------------------------------------------
# Tier 3: Application Behavior Specification
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class WorkloadReadResult:
    query_id: str
    sql: str
    latency_ms: float
    rows_returned: int
    success: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class WorkloadWriteResult:
    fixture_id: str
    sql: str
    rows_affected: int
    success: bool
    isolated_environment_verified: bool
    rollback_verified: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class ConstraintCheckResult:
    constraint_name: str
    table_name: str
    constraint_type: str  # "CHECK", "FOREIGN_KEY", "UNIQUE", "NOT_NULL"
    violation_prevented: bool
    valid_accepted: bool
    verified: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class SequenceCheckResult:
    sequence_name: str
    nextval_succeeded: bool
    last_value: int
    target_ahead_of_source: bool
    verified: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class RoutineCheckResult:
    routine_name: str
    executed_successfully: bool
    result_matched: bool
    verified: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class LatencyParityCheck:
    query_id: str
    source_latency_ms: float
    target_latency_ms: float
    latency_ratio: float
    regression_detected: bool
    p95_source_ms: float = 0.0
    p95_target_ms: float = 0.0


@dataclass(frozen=True)
class ApplicationBehaviorVerification:
    """Application behavior tier: empirical execution under application role."""

    representative_reads: Tuple[WorkloadReadResult, ...]
    isolated_writes: Tuple[WorkloadWriteResult, ...]
    constraint_checks: Tuple[ConstraintCheckResult, ...]
    sequence_checks: Tuple[SequenceCheckResult, ...]
    routine_checks: Tuple[RoutineCheckResult, ...]
    latency_checks: Tuple[LatencyParityCheck, ...]
    verified: bool
    defects: Tuple[str, ...]

    @classmethod
    def evaluate(
        cls,
        *,
        representative_reads: Iterable[WorkloadReadResult] = (),
        isolated_writes: Iterable[WorkloadWriteResult] = (),
        constraint_checks: Iterable[ConstraintCheckResult] = (),
        sequence_checks: Iterable[SequenceCheckResult] = (),
        routine_checks: Iterable[RoutineCheckResult] = (),
        latency_checks: Iterable[LatencyParityCheck] = (),
    ) -> ApplicationBehaviorVerification:
        reads = tuple(representative_reads)
        writes = tuple(isolated_writes)
        constraints = tuple(constraint_checks)
        sequences = tuple(sequence_checks)
        routines = tuple(routine_checks)
        latencies = tuple(latency_checks)

        defects: List[str] = []
        for r in reads:
            if not r.success:
                defects.append(f"representative read '{r.query_id}' failed: {r.error}")

        for w in writes:
            if not w.success:
                defects.append(f"isolated write fixture '{w.fixture_id}' failed: {w.error}")
            elif not w.isolated_environment_verified:
                defects.append(f"write fixture '{w.fixture_id}' ran outside isolated authorized environment")
            elif not w.rollback_verified:
                defects.append(f"write fixture '{w.fixture_id}' failed cleanup/rollback verification")

        for c in constraints:
            if not c.verified or not c.violation_prevented or not c.valid_accepted:
                defects.append(f"constraint '{c.constraint_name}' on '{c.table_name}' failed validation: {c.error}")

        for s in sequences:
            if not s.verified or not s.nextval_succeeded or not s.target_ahead_of_source:
                defects.append(f"sequence '{s.sequence_name}' verification failed: {s.error}")

        for rtn in routines:
            if not rtn.verified or not rtn.executed_successfully or not rtn.result_matched:
                defects.append(f"routine behavioral check for '{rtn.routine_name}' failed: {rtn.error}")

        for lat in latencies:
            if lat.regression_detected:
                defects.append(
                    f"latency regression on query '{lat.query_id}': ratio {lat.latency_ratio:.2f}x "
                    f"target {lat.target_latency_ms:.2f}ms vs source {lat.source_latency_ms:.2f}ms"
                )

        has_any_workload = bool(reads or writes or constraints or sequences or routines)
        verified = (has_any_workload and len(defects) == 0)
        return cls(
            representative_reads=reads,
            isolated_writes=writes,
            constraint_checks=constraints,
            sequence_checks=sequences,
            routine_checks=routines,
            latency_checks=latencies,
            verified=verified,
            defects=tuple(defects),
        )


# -----------------------------------------------------------------------------
# Representative Query & EXPLAIN ANALYZE Execution Guard
# -----------------------------------------------------------------------------

_DML_PATTERN = re.compile(
    r"^\s*(INSERT\s+INTO|UPDATE\s+|DELETE\s+FROM|MERGE\s+INTO|ALTER\s+|DROP\s+|TRUNCATE\s+|CREATE\s+|CALL\s+|DO\s+)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RepresentativeQuery:
    query_id: str
    sql: str
    is_read_only: bool
    expected_columns: Tuple[str, ...] = ()
    sla_max_latency_ms: float = 100.0
    isolation_level: str = "READ COMMITTED"

    @classmethod
    def create(
        cls,
        query_id: str,
        sql: str,
        *,
        is_read_only: Optional[bool] = None,
        expected_columns: Iterable[str] = (),
        sla_max_latency_ms: float = 100.0,
    ) -> RepresentativeQuery:
        inferred_read_only = not bool(_DML_PATTERN.search(sql))
        ro = inferred_read_only if is_read_only is None else is_read_only
        return cls(
            query_id=query_id,
            sql=sql.strip(),
            is_read_only=ro,
            expected_columns=tuple(expected_columns),
            sla_max_latency_ms=sla_max_latency_ms,
        )


def explain_analyze_admission_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for EXPLAIN ANALYZE and workload admission.

    EXPLAIN ANALYZE executes SQL: external effects require isolated authorized environments.
    """
    return (
        decide.Rung(
            "admit_safe_read_query",
            "admit read-only representative query for EXPLAIN ANALYZE",
            gives=frozenset({"admitted", "read_only", "safe_execution"}),
            needs=(
                decide.Need(
                    "approved representative workload query",
                    lambda f: bool(f.get("is_approved_workload")),
                    "{rung} requires the query to be registered in approved representative catalog",
                ),
                decide.Need(
                    "read-only statement without external mutating effects",
                    lambda f: bool(f.get("is_read_only_statement")),
                    "{rung} cannot admit mutating SQL without isolated environment",
                ),
            ),
            because="read-only queries have no database state side effects under EXPLAIN ANALYZE",
            footprint=1,
            leaves="planner profile and timing metrics collected",
        ),
        decide.Rung(
            "admit_isolated_write_fixture",
            "admit mutating write fixture under isolated authorized environment",
            gives=frozenset({"admitted", "write_fixture", "isolated_execution"}),
            needs=(
                decide.Need(
                    "approved representative write fixture",
                    lambda f: bool(f.get("is_approved_workload")),
                    "{rung} requires write fixture to be registered and approved",
                ),
                decide.Need(
                    "isolated authorized environment with guaranteed rollback or scratch isolation",
                    lambda f: bool(f.get("is_isolated_environment")),
                    "{rung} requires dedicated sandbox or rollback fixture before executing mutating SQL",
                ),
                decide.Need(
                    "application role authorization",
                    lambda f: bool(f.get("has_application_role_auth")),
                    "{rung} must be authorized under application role",
                ),
            ),
            because="mutating SQL executes inside an authorized isolated sandbox with rollback verification",
            footprint=2,
            leaves="isolated fixture rows cleaned up or transaction rolled back",
        ),
        decide.Rung(
            "refuse_unauthorized_external_effects",
            "refuse mutating statement without isolated environment",
            gives=frozenset({"refused", "blocked_external_effect"}),
            needs=(
                decide.Need(
                    "statement has external effects without isolated authorized environment",
                    lambda f: bool(f.get("has_unauthorized_external_effects")),
                    "{rung} requires presence of unauthorized external effects",
                ),
            ),
            because="EXPLAIN ANALYZE executes SQL; uncontrolled DML on shared databases is blocked",
            footprint=0,
            leaves="zero side effects",
        ),
    )


def evaluate_explain_analyze_admission(
    query: RepresentativeQuery,
    *,
    is_approved: bool = True,
    is_isolated_env: bool = False,
    app_role_auth: bool = True,
) -> decide.Choice:
    """Evaluate whether a query can be executed under EXPLAIN ANALYZE."""
    has_unauthorized_effects = (not query.is_read_only) and (not is_isolated_env)
    facts_dict: Dict[str, Any] = {
        "is_approved_workload": bool(is_approved),
        "is_read_only_statement": bool(query.is_read_only),
        "is_isolated_environment": bool(is_isolated_env),
        "has_application_role_auth": bool(app_role_auth),
        "has_unauthorized_external_effects": bool(has_unauthorized_effects),
    }
    rungs = explain_analyze_admission_rungs()
    choice = decide.climb("explain analyze admission", rungs, decide.Facts(facts_dict))
    return choice


# -----------------------------------------------------------------------------
# Application Readiness Decision Ladder
# -----------------------------------------------------------------------------

def application_readiness_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for certifying application-visible readiness.

    Separates value equality, object/security parity, and application behavior.
    Equal rows with a broken trigger, missing sequence grant, or wrong search_path
    block the switch; unavailable workload evidence stays explicitly unavailable.
    """
    return (
        decide.Rung(
            "certify_application_ready",
            "certify target application-ready across all 3 tiers",
            gives=frozenset({"ready", "application_visible", "switch_authorized"}),
            needs=(
                decide.Need(
                    "tier 1 value equality verified",
                    lambda f: bool(f.get("value_equality_verified")),
                    "{rung} requires row count, schema, and checksum equality",
                ),
                decide.Need(
                    "tier 2 object and security parity verified",
                    lambda f: bool(f.get("object_security_parity_verified")),
                    "{rung} requires valid search_path, sequence grants, triggers, and routines",
                ),
                decide.Need(
                    "tier 3 application behavior verified",
                    lambda f: bool(f.get("application_behavior_verified")),
                    "{rung} requires representative reads, write fixtures, and constraints to pass",
                ),
                decide.Need(
                    "no query latency regression exceeding SLA threshold",
                    lambda f: bool(f.get("no_latency_regression")),
                    "{rung} detected query latency regression on target",
                ),
                decide.Need(
                    "empirical workload evidence available",
                    lambda f: bool(f.get("workload_evidence_available")),
                    "{rung} requires representative workload evidence to be actively measured",
                ),
            ),
            because="value equality, security parity, and empirical application behavior are all proven",
            footprint=3,
            leaves="verified switch readiness certificate",
        ),
        decide.Rung(
            "refuse_broken_trigger_or_routine",
            "refuse switch due to broken trigger or invalid routine",
            gives=frozenset({"blocked", "broken_routine_or_trigger"}),
            needs=(
                decide.Need(
                    "target has broken trigger or invalid routine",
                    lambda f: bool(f.get("has_broken_trigger_or_routine")),
                    "{rung} requires presence of broken trigger or invalid routine",
                ),
            ),
            because="equal rows with a broken trigger or invalid routine fail application execution",
            footprint=0,
            leaves="switch blocked until trigger or routine is repaired",
        ),
        decide.Rung(
            "refuse_missing_grant_or_search_path",
            "refuse switch due to missing grant or wrong search_path",
            gives=frozenset({"blocked", "security_parity_deficit"}),
            needs=(
                decide.Need(
                    "target has missing sequence/table grant or wrong search_path",
                    lambda f: bool(f.get("has_missing_grant_or_wrong_search_path")),
                    "{rung} requires presence of missing grant or wrong search_path",
                ),
            ),
            because="application role cannot write or resolve objects under target security context",
            footprint=0,
            leaves="switch blocked until grants and search_path are reconciled",
        ),
        decide.Rung(
            "refuse_constraint_or_behavior_defect",
            "refuse switch due to constraint or routine behavioral check failure",
            gives=frozenset({"blocked", "behavior_defect"}),
            needs=(
                decide.Need(
                    "target failed constraint check, write fixture, or behavioral check",
                    lambda f: bool(f.get("has_constraint_or_behavior_defect")),
                    "{rung} requires presence of constraint or behavioral defect",
                ),
            ),
            because="application data mutations or constraints failed execution on target",
            footprint=0,
            leaves="switch blocked until schema constraints or write behavior match source",
        ),
        decide.Rung(
            "refuse_latency_regression",
            "refuse switch due to intolerable query latency regression",
            gives=frozenset({"blocked", "latency_regression"}),
            needs=(
                decide.Need(
                    "target query execution exhibits latency regression over threshold",
                    lambda f: bool(f.get("has_latency_regression")),
                    "{rung} requires presence of latency regression",
                ),
            ),
            because="target query execution exceeds SLA latency threshold",
            footprint=0,
            leaves="switch held for query optimization or indexing",
        ),
        decide.Rung(
            "refuse_unavailable_workload_evidence",
            "refuse switch because workload evidence is unavailable",
            gives=frozenset({"blocked", "workload_evidence_missing"}),
            needs=(
                decide.Need(
                    "workload evidence is unavailable and cannot be assumed",
                    lambda f: bool(f.get("workload_evidence_unavailable")),
                    "{rung} requires workload evidence to be explicitly unavailable",
                ),
            ),
            because="unavailable workload evidence stays explicitly unavailable; cannot assume readiness",
            footprint=0,
            leaves="switch blocked until representative reads and writes are measured",
        ),
        decide.Rung(
            "block_value_equality_alone",
            "refuse switch when only value equality is present without behavioral verification",
            gives=frozenset({"blocked", "value_equality_alone_insufficient"}),
            needs=(
                decide.Need(
                    "value equality present but behavioral verification omitted",
                    lambda f: bool(f.get("only_value_equality_present")),
                    "{rung} requires value equality alone without behavioral proof",
                ),
            ),
            because="data equality alone cannot guarantee application-visible readiness",
            footprint=0,
            leaves="switch blocked until tier 2 and tier 3 checks are performed",
        ),
    )


# -----------------------------------------------------------------------------
# Overall Application Readiness Evaluator & Report
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ApplicationReadinessReport:
    hop_name: str
    can_switch: bool
    care_badge: CareBadge
    choice: decide.Choice
    verdict: str
    value_tier: ValueEqualityVerification
    security_tier: ObjectSecurityParity
    behavior_tier: ApplicationBehaviorVerification
    evidence_status: WorkloadEvidenceStatus
    blockers: Tuple[str, ...]
    warnings: Tuple[str, ...]
    timestamp: datetime.datetime = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))

    def render(self) -> str:
        """Render pure UNIX report with zero emojis and 4-tier care badges."""
        sep = "-" * 72
        status_text = "READY FOR CUTOVER" if self.can_switch else "SWITCH BLOCKED"
        lines = [
            sep,
            f"[{self.care_badge.value}] APPLICATION READINESS REPORT: {status_text}",
            f"Hop        : {self.hop_name}",
            f"Verdict    : {self.verdict}",
            f"Timestamp  : {self.timestamp.isoformat()}",
            f"Evidence   : {self.evidence_status.value}",
            sep,
            "TIER 1 - VALUE EQUALITY:",
            f"  Tables Checked : {self.value_tier.tables_checked}",
            f"  Source Rows    : {self.value_tier.rows_source}",
            f"  Target Rows    : {self.value_tier.rows_target}",
            f"  Checksum Match : {self.value_tier.checksum_match}",
            f"  Columns Match  : {self.value_tier.columns_match}",
            f"  Tier 1 Status  : {'PASS' if self.value_tier.verified else 'FAIL'}",
            "",
            "TIER 2 - OBJECT & SECURITY PARITY:",
            f"  Application Role: {self.security_tier.application_role}",
            f"  Search Path     : {','.join(self.security_tier.search_path)} (valid: {self.security_tier.search_path_valid})",
            f"  Sequence Grants : {len(self.security_tier.sequence_grants)} checked",
            f"  Table Grants    : {len(self.security_tier.table_grants)} checked",
            f"  Triggers        : {len(self.security_tier.triggers)} checked",
            f"  Routines        : {len(self.security_tier.routines)} checked",
            f"  Tier 2 Status   : {'PASS' if self.security_tier.verified else 'FAIL'}",
            "",
            "TIER 3 - APPLICATION BEHAVIOR:",
            f"  Representative Reads  : {len(self.behavior_tier.representative_reads)} executed",
            f"  Isolated Writes       : {len(self.behavior_tier.isolated_writes)} executed",
            f"  Constraints Checked   : {len(self.behavior_tier.constraint_checks)} checked",
            f"  Sequence Probes       : {len(self.behavior_tier.sequence_checks)} checked",
            f"  Routine Probes        : {len(self.behavior_tier.routine_checks)} checked",
            f"  Latency Parity Checks : {len(self.behavior_tier.latency_checks)} checked",
            f"  Tier 3 Status         : {'PASS' if self.behavior_tier.verified else 'FAIL'}",
            sep,
        ]

        if self.blockers:
            lines.append("BLOCKERS (ACTION REQUIRED):")
            for b in self.blockers:
                lines.append(f"  * {b}")
            lines.append(sep)

        if self.warnings:
            lines.append("WARNINGS (REVIEW):")
            for w in self.warnings:
                lines.append(f"  * {w}")
            lines.append(sep)

        return "\n".join(lines)


def evaluate_application_readiness(
    hop_name: str,
    *,
    value_tier: ValueEqualityVerification,
    security_tier: ObjectSecurityParity,
    behavior_tier: ApplicationBehaviorVerification,
    evidence_status: WorkloadEvidenceStatus = WorkloadEvidenceStatus.AVAILABLE,
    regression_threshold: float = 1.5,
) -> ApplicationReadinessReport:
    """Evaluate application readiness beyond data equality using `decide.climb`."""
    blockers: List[str] = []
    warnings: List[str] = []

    # Identify defects
    has_broken_trg_or_rtn = any(
        (not trg.enabled or not trg.has_valid_function) for trg in security_tier.triggers
    ) or any(not r.valid for r in security_tier.routines)

    has_missing_grant_or_sp = (
        not security_tier.search_path_valid
        or any(not sg.is_granted for sg in security_tier.sequence_grants)
        or any(not tg.is_granted for tg in security_tier.table_grants)
        or any(not r.executable_by_role for r in security_tier.routines)
    )

    has_constraint_defect = any(not c.verified for c in behavior_tier.constraint_checks) or any(
        not w.success for w in behavior_tier.isolated_writes
    ) or any(not s.verified for s in behavior_tier.sequence_checks)

    has_latency_regr = any(lat.regression_detected for lat in behavior_tier.latency_checks)

    is_evidence_missing = (evidence_status == WorkloadEvidenceStatus.UNAVAILABLE)
    is_value_alone = (
        value_tier.verified
        and not behavior_tier.verified
        and not has_broken_trg_or_rtn
        and not has_missing_grant_or_sp
        and not is_evidence_missing
    )

    facts_dict: Dict[str, Any] = {
        "value_equality_verified": bool(value_tier.verified),
        "object_security_parity_verified": bool(security_tier.verified),
        "application_behavior_verified": bool(behavior_tier.verified),
        "no_latency_regression": not bool(has_latency_regr),
        "workload_evidence_available": evidence_status == WorkloadEvidenceStatus.AVAILABLE,
        "has_broken_trigger_or_routine": bool(has_broken_trg_or_rtn),
        "has_missing_grant_or_wrong_search_path": bool(has_missing_grant_or_sp),
        "has_constraint_or_behavior_defect": bool(has_constraint_defect),
        "has_latency_regression": bool(has_latency_regr),
        "workload_evidence_unavailable": bool(is_evidence_missing),
        "only_value_equality_present": bool(is_value_alone),
    }

    rungs = application_readiness_rungs()
    choice = decide.climb("application readiness verification", rungs, decide.Facts(facts_dict))

    # Compile blockers and warnings
    blockers.extend(security_tier.defects)
    blockers.extend(behavior_tier.defects)
    if not value_tier.verified:
        blockers.append("Tier 1 value equality check failed")

    if is_evidence_missing:
        blockers.append("workload evidence unavailable: empirical behavioral proof required before cutover")

    can_switch = (
        choice.rung is not None
        and choice.rung.name == "certify_application_ready"
        and len(blockers) == 0
    )

    if can_switch:
        care_badge = CareBadge.PASS
        verdict = "PASS: target application-ready across value equality, security parity, and behavior"
    elif is_evidence_missing:
        care_badge = CareBadge.REVIEW
        verdict = f"REVIEW: {choice.reason}"
    else:
        care_badge = CareBadge.ACTION
        verdict = f"ACTION: {choice.reason}"

    return ApplicationReadinessReport(
        hop_name=hop_name,
        can_switch=can_switch,
        care_badge=care_badge,
        choice=choice,
        verdict=verdict,
        value_tier=value_tier,
        security_tier=security_tier,
        behavior_tier=behavior_tier,
        evidence_status=evidence_status,
        blockers=tuple(blockers),
        warnings=tuple(warnings),
    )


# -----------------------------------------------------------------------------
# Latency Regression & Measured Bounds Derivation
# -----------------------------------------------------------------------------

def derive_latency_parity(
    query_id: str,
    source_latencies_ms: Sequence[float],
    target_latencies_ms: Sequence[float],
    *,
    threshold_ratio: float = 1.5,
) -> LatencyParityCheck:
    """Derive latency parity and regression from empirical readings without magic numbers."""
    if not source_latencies_ms:
        raise ApplicationReadinessError("source_latencies_ms must not be empty")
    if not target_latencies_ms:
        raise ApplicationReadinessError("target_latencies_ms must not be empty")

    src_sorted = sorted(source_latencies_ms)
    tgt_sorted = sorted(target_latencies_ms)

    def _percentile(data: List[float], p: float) -> float:
        idx = int(math.ceil(p * len(data))) - 1
        return data[max(0, min(len(data) - 1, idx))]

    src_median = _percentile(src_sorted, 0.50)
    tgt_median = _percentile(tgt_sorted, 0.50)
    src_p95 = _percentile(src_sorted, 0.95)
    tgt_p95 = _percentile(tgt_sorted, 0.95)

    base = src_median if src_median > 0.0 else 0.001
    ratio = tgt_median / base
    regression = (ratio > threshold_ratio) or (src_p95 > 0 and (tgt_p95 / src_p95) > threshold_ratio)

    return LatencyParityCheck(
        query_id=query_id,
        source_latency_ms=src_median,
        target_latency_ms=tgt_median,
        latency_ratio=ratio,
        regression_detected=regression,
        p95_source_ms=src_p95,
        p95_target_ms=tgt_p95,
    )


# -----------------------------------------------------------------------------
# O(1) Typed Checkpoint Store & Lease Integration
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ApplicationReadinessCheckpointUnit:
    unit_id: str
    tier: ReadinessTier
    min_cursor: Any
    max_cursor: Any
    verified: bool
    completed_at: datetime.datetime


class ApplicationReadinessCheckpointStore:
    """Durable O(1) checkpoint store preserving native cursor types via `migkit.checkpoint`."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = pathlib.Path(path)
        self._units: Dict[str, ApplicationReadinessCheckpointUnit] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="ascii"))
        units_raw = raw.get("units", {})
        for uid, item in units_raw.items():
            self._units[uid] = ApplicationReadinessCheckpointUnit(
                unit_id=uid,
                tier=ReadinessTier(item["tier"]),
                min_cursor=checkpoint.deserialize_cursor(item["min_cursor"]),
                max_cursor=checkpoint.deserialize_cursor(item["max_cursor"]),
                verified=bool(item["verified"]),
                completed_at=checkpoint.deserialize_cursor(item["completed_at"]),
            )

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "units": {
                uid: {
                    "tier": u.tier.value,
                    "min_cursor": checkpoint.serialize_cursor(u.min_cursor),
                    "max_cursor": checkpoint.serialize_cursor(u.max_cursor),
                    "verified": u.verified,
                    "completed_at": checkpoint.serialize_cursor(u.completed_at),
                }
                for uid, u in self._units.items()
            }
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=True), encoding="ascii")
        tmp.replace(self.path)

    def record_unit(self, unit: ApplicationReadinessCheckpointUnit) -> None:
        self._units[unit.unit_id] = unit
        self._save()

    def is_completed_o1(self, unit_id: str) -> bool:
        return unit_id in self._units

    def get_unit(self, unit_id: str) -> Optional[ApplicationReadinessCheckpointUnit]:
        return self._units.get(unit_id)


def acquire_application_readiness_lease(
    lock_path: pathlib.Path,
    *,
    what: str = "application-readiness verification",
    term_seconds: float = 60.0,
) -> lease.Lease:
    """Acquire a mutual-exclusion hop lease using `migkit.lease.Lease`."""
    return lease.Lease(pathlib.Path(lock_path), what=what, term=term_seconds).acquire()
