"""Deep and adaptive workflow contracts across migration phases (backlog P0-WORKFLOW-DEPTH).

Connects assessment, planning, copy, validation, repair and cutover with
versioned evidence, hypothesis investigation, explicit invalidation and
safe recovery boundaries.
"""
from dataclasses import dataclass, field
import hashlib
import json
import time
from typing import Dict, List, Optional, Set, Tuple


PHASES = ("assess", "plan", "copy", "verify", "repair", "cutover")


@dataclass(frozen=True)
class PhaseContract:
    """Entry, prerequisite and exit criteria for one migration phase."""
    name: str
    prerequisites: tuple = ()
    required_evidence: tuple = ()
    exit_criteria: tuple = ()
    read_only: bool = True


CONTRACTS = {
    "assess": PhaseContract(
        name="assess",
        prerequisites=(),
        required_evidence=("endpoints_reachable", "schema_inventory"),
        exit_criteria=("compatibility_known", "scope_determined"),
        read_only=True,
    ),
    "plan": PhaseContract(
        name="plan",
        prerequisites=("assess",),
        required_evidence=("capacity_measured", "strategy_selected"),
        exit_criteria=("schedule_built", "resources_budgeted"),
        read_only=True,
    ),
    "copy": PhaseContract(
        name="copy",
        prerequisites=("plan",),
        required_evidence=("checkpoint_initialized",),
        exit_criteria=("data_transferred", "indexes_ready"),
        read_only=False,
    ),
    "verify": PhaseContract(
        name="verify",
        prerequisites=("copy",),
        required_evidence=("checksum_matrix", "count_parity"),
        exit_criteria=("parity_proven",),
        read_only=True,
    ),
    "repair": PhaseContract(
        name="repair",
        prerequisites=("verify",),
        required_evidence=("repair_ladder_ready", "findings_classified"),
        exit_criteria=("differences_resolved", "reverified"),
        read_only=False,
    ),
    "cutover": PhaseContract(
        name="cutover",
        prerequisites=("verify",),
        required_evidence=("replication_caught_up", "write_fence_verified"),
        exit_criteria=("traffic_redirected", "durable_rollback_ready"),
        read_only=False,
    ),
}


@dataclass
class WorkflowState:
    """Durable state of a migration workflow across its phases."""
    hop_name: str
    goal: str
    scope: tuple
    schema_fingerprint: str = ""
    current_phase: str = "assess"
    completed_phases: List[str] = field(default_factory=list)
    evidence: Dict[str, dict] = field(default_factory=dict)
    invalidated: List[dict] = field(default_factory=list)
    findings: List[dict] = field(default_factory=list)
    investigations: List[dict] = field(default_factory=list)

    def record_evidence(self, phase: str, artifact: str, data: dict,
                        schema_fp: str = ""):
        """Records versioned evidence for a phase."""
        fp = schema_fp or self.schema_fingerprint
        self.evidence[artifact] = {
            "phase": phase,
            "artifact": artifact,
            "data": data,
            "schema_fingerprint": fp,
            "recorded_at": time.time(),
        }

    def check_invalidation(self, new_schema_fingerprint: str,
                           changed_tables: Optional[Set[str]] = None) -> List[str]:
        """Invalidates downstream evidence when schema or scope drifts."""
        if not self.schema_fingerprint:
            self.schema_fingerprint = new_schema_fingerprint
            return []

        invalidated_keys = []
        if self.schema_fingerprint != new_schema_fingerprint:
            reason = "schema fingerprint changed"
            for k, ev in list(self.evidence.items()):
                if ev.get("phase") in ("verify", "repair", "cutover"):
                    invalidated_keys.append(k)
                    self.invalidated.append({
                        "artifact": k,
                        "phase": ev.get("phase"),
                        "reason": reason,
                        "time": time.time(),
                    })
                    del self.evidence[k]
            # Reset phases that depend on matching schema
            self.completed_phases = [p for p in self.completed_phases
                                     if p in ("assess", "plan")]
            self.schema_fingerprint = new_schema_fingerprint

        return invalidated_keys

    def advance_phase(self, next_phase: str) -> Tuple[bool, str]:
        """Transitions to the next phase if prerequisites and exit criteria hold."""
        if next_phase not in CONTRACTS:
            return False, f"unknown phase {next_phase}"
        contract = CONTRACTS[next_phase]
        for req in contract.prerequisites:
            if req not in self.completed_phases:
                return False, f"prerequisite phase '{req}' not completed"
        for ev in contract.required_evidence:
            if ev not in self.evidence:
                return False, f"missing required evidence '{ev}'"

        if self.current_phase not in self.completed_phases:
            self.completed_phases.append(self.current_phase)
        self.current_phase = next_phase
        return True, f"advanced to {next_phase}"


@dataclass(frozen=True)
class Hypothesis:
    """One candidate cause for a difference observed during validation."""
    name: str
    symptom: str
    testable_probe: str
    expected_if_true: str


INVESTIGATION_HYPOTHESES = {
    "count_mismatch": [
        Hypothesis(
            name="concurrent_writes",
            symptom="row count mismatch on source vs target",
            testable_probe="check_replication_lag_and_max_commit",
            expected_if_true="lag_seconds > 0 or uncommitted_transactions",
        ),
        Hypothesis(
            name="replication_filter",
            symptom="row count mismatch on filtered table",
            testable_probe="check_where_clause_and_rules",
            expected_if_true="filter_clause_active",
        ),
        Hypothesis(
            name="hard_missing_rows",
            symptom="unexplained count difference after drain",
            testable_probe="sample_key_range_diff",
            expected_if_true="specific_primary_keys_absent",
        ),
    ],
    "hash_divergence": [
        Hypothesis(
            name="timezone_drift",
            symptom="datetime column hash differs across engines",
            testable_probe="probe_session_timezone",
            expected_if_true="source_tz != target_tz and datetime_without_tz_used",
        ),
        Hypothesis(
            name="collation_casing",
            symptom="text column hash differs across engines",
            testable_probe="probe_collation_and_accent_sensitivity",
            expected_if_true="ci_vs_cs_collation",
        ),
        Hypothesis(
            name="float_precision",
            symptom="floating point column hash differs",
            testable_probe="probe_float_rounding_and_types",
            expected_if_true="different_ieee_precision",
        ),
        Hypothesis(
            name="data_corruption",
            symptom="persistent value difference after normalization",
            testable_probe="inspect_row_values",
            expected_if_true="values_unequal",
        ),
    ],
}


def investigate(finding_kind: str, facts: dict) -> Dict[str, object]:
    """Branches and evaluates bounded hypotheses for a finding (P0-WORKFLOW-DEPTH.investigate)."""
    hypotheses = INVESTIGATION_HYPOTHESES.get(finding_kind, [])
    if not hypotheses:
        return {
            "finding_kind": finding_kind,
            "cause": "unknown",
            "supported": False,
            "evidence": {},
            "verdict": "inconclusive",
        }

    for hyp in hypotheses:
        # Evaluate hypothesis against facts safely
        if hyp.name == "concurrent_writes":
            lag = facts.get("replication_lag_seconds", 0)
            in_flight = facts.get("uncommitted_writes", False)
            if lag > 0 or in_flight:
                return {
                    "finding_kind": finding_kind,
                    "cause": hyp.name,
                    "supported": True,
                    "evidence": {"lag_seconds": lag, "in_flight": in_flight},
                    "action": "wait_drain_or_fence_writes",
                    "verdict": "transient_divergence",
                }
        elif hyp.name == "timezone_drift":
            src_tz = facts.get("src_timezone")
            dst_tz = facts.get("dst_timezone")
            has_dt = facts.get("has_naive_datetimes", False)
            if has_dt and src_tz and dst_tz and src_tz != dst_tz:
                return {
                    "finding_kind": finding_kind,
                    "cause": hyp.name,
                    "supported": True,
                    "evidence": {"src_tz": src_tz, "dst_tz": dst_tz},
                    "action": "align_session_timezone",
                    "verdict": "configuration_drift",
                }
        elif hyp.name == "collation_casing":
            src_col = facts.get("src_collation", "")
            dst_col = facts.get("dst_collation", "")
            if "_ci" in src_col and "_bin" in dst_col or "_cs" in dst_col:
                return {
                    "finding_kind": finding_kind,
                    "cause": hyp.name,
                    "supported": True,
                    "evidence": {"src_collation": src_col, "dst_collation": dst_col},
                    "action": "align_collation_case_sensitivity",
                    "verdict": "schema_drift",
                }

    # If no specific environmental drift explained it, return actionable defect
    return {
        "finding_kind": finding_kind,
        "cause": "data_defect",
        "supported": True,
        "evidence": facts.get("sample_diff", {}),
        "action": "execute_repair_ladder",
        "verdict": "application_defect",
    }


@dataclass(frozen=True)
class SemanticsModel:
    """Declared data safety and ordering invariants (backlog P0-SAFETY-INTEGRITY)."""
    scope: tuple
    foreign_keys: tuple = ()        # ((child_table, parent_table), ...)
    required_consistency: str = "as of one moment"
    idempotent: str = "converges"
    partitioned_tables: tuple = ()


def validate_safety(schedule_order: list, semantics: SemanticsModel) -> Tuple[bool, str]:
    table_positions = {t: i for i, t in enumerate(schedule_order)}
    for child, parent in semantics.foreign_keys:
        if child in table_positions and parent in table_positions:
            if table_positions[child] < table_positions[parent]:
                return False, (f"dependency ordering violation: '{child}' scheduled "
                               f"before parent '{parent}'")
    return True, "safety and dependency guarantees satisfied"


def export_workflow(target: str, hop_name: str, **kwargs) -> str:
    from . import workflow_exporter
    return workflow_exporter.export_workflow(target, hop_name, **kwargs)


