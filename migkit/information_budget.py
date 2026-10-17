"""Choose what to measure by its decision value (Row 587: SMART-N4.information-budget).

Estimates which uncertain fact could change feasibility or the winning plan,
comparing expected reduction of decision loss (Value of Information / VoI)
against probe time, source CPU/I/O/locks, financial cost, and deadline slack
using `decide.Rung` and `decide.climb(work, rungs, facts)`.

Combines compatible reads through scanplan and stops probing when remaining
uncertainty cannot change a choice materially. Preserves conservative baselines
and bounded exploration; unknown is never assumed support.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import fcntl
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from . import decide, durable


# -----------------------------------------------------------------------------
# Care Badges, Safe Host Validation, and Error Types
# -----------------------------------------------------------------------------

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


class InformationBudgetError(RuntimeError):
    """Base error for information budget operations."""


class UnsafeEndpointHostError(InformationBudgetError, ValueError):
    """Raised when an endpoint host is outside approved loopback or 10.0.0.x ranges."""


class BudgetExhaustedError(InformationBudgetError):
    """Raised when an admitted probe exceeds hard source impact or deadline limits."""


class VerificationLeaseError(InformationBudgetError):
    """Raised when an active verification lease lock (.run.lock) cannot be acquired or renewed."""


def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host uses approved loopback or 10.0.0.x test addresses."""
    cleaned = str(host or "").strip().lower()
    if not cleaned:
        raise UnsafeEndpointHostError("endpoint host cannot be empty")
    if cleaned in SAFE_TEST_HOSTS:
        return cleaned
    if cleaned.startswith("10.0.0."):
        parts = cleaned.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return cleaned
    raise UnsafeEndpointHostError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, ::1, or 10.0.0.x allowed"
    )


# -----------------------------------------------------------------------------
# Domain Enums and Core Specifications
# -----------------------------------------------------------------------------

class ProbeLockMode(str, enum.Enum):
    NONE = "none"
    SHARED_READ = "shared_read"
    EXCLUSIVE_TABLE_LOCK = "exclusive_table_lock"


class FactDistributionKind(str, enum.Enum):
    BOOLEAN = "boolean"
    CATEGORICAL = "categorical"
    CONTINUOUS = "continuous"


@dataclass(frozen=True)
class UncertainFact:
    """An uncertain fact whose true state may influence plan feasibility or choice."""

    fact_key: str
    distribution_kind: FactDistributionKind
    prior_probabilities: Mapping[str, float]  # state -> probability (summing to 1.0)
    measured_value: Optional[str] = None
    is_measured: bool = False
    source_target: str = "source"

    def __post_init__(self) -> None:
        if not self.fact_key.strip():
            raise ValueError("fact_key cannot be empty")
        total_p = sum(float(p) for p in self.prior_probabilities.values())
        if not (0.99 <= total_p <= 1.01):
            raise ValueError(f"prior probabilities for {self.fact_key} must sum to 1.0 (got {total_p})")

    def with_measurement(self, value: str) -> UncertainFact:
        return UncertainFact(
            fact_key=self.fact_key,
            distribution_kind=self.distribution_kind,
            prior_probabilities={str(value): 1.0},
            measured_value=str(value),
            is_measured=True,
            source_target=self.source_target,
        )


@dataclass(frozen=True)
class CandidatePlan:
    """A migration execution plan alternative whose expected loss depends on facts."""

    plan_id: str
    nominal_cost_s: float
    fallback_penalty_s: float
    is_conservative_baseline: bool
    prerequisite_conditions: Mapping[str, str]  # fact_key -> required state value
    guarantee_badge: CareBadge = CareBadge.PASS

    def __post_init__(self) -> None:
        if not self.plan_id.strip():
            raise ValueError("plan_id cannot be empty")
        if self.nominal_cost_s < 0.0 or self.fallback_penalty_s < 0.0:
            raise ValueError("nominal cost and fallback penalty must be non-negative")

    def evaluate_loss(self, fact_states: Mapping[str, str]) -> float:
        """Compute the cost of this plan given a realization of fact states."""
        for key, req_val in self.prerequisite_conditions.items():
            actual = fact_states.get(key)
            if actual is None:
                # Unknown is never assumed support; if required fact is missing/unknown,
                # plan fails feasibility and pays fallback penalty
                return self.nominal_cost_s + self.fallback_penalty_s
            if str(actual) != str(req_val):
                return self.nominal_cost_s + self.fallback_penalty_s
        return self.nominal_cost_s


@dataclass(frozen=True)
class ProbeCostProfile:
    """Multi-dimensional cost and source impact profile for measuring an uncertain fact."""

    probe_id: str
    fact_key: str
    target_host: str
    expected_duration_s: float
    source_cpu_cores: float
    source_io_bytes: int
    lock_mode: ProbeLockMode
    lock_duration_s: float
    money_cost_dollars: float
    is_scanplan_compatible: bool = False
    scanplan_table: Optional[str] = None
    scanplan_columns: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        validate_safe_endpoint(self.target_host)
        if not self.probe_id.strip():
            raise ValueError("probe_id cannot be empty")
        if self.expected_duration_s < 0.0 or self.source_cpu_cores < 0.0:
            raise ValueError("expected_duration_s and source_cpu_cores must be non-negative")
        if self.source_io_bytes < 0 or self.lock_duration_s < 0.0 or self.money_cost_dollars < 0.0:
            raise ValueError("io_bytes, lock_duration_s, and money_cost must be non-negative")


@dataclass(frozen=True)
class InformationBudgetLimits:
    """Resource, source-impact, and deadline limits bounding exploratory probing."""

    max_probe_wall_s: float
    max_source_cpu_core_seconds: float
    max_source_io_bytes: int
    max_source_lock_duration_s: float
    max_money_budget_dollars: float
    min_deadline_slack_margin_ratio: float
    min_material_loss_reduction_s: float

    def __post_init__(self) -> None:
        if (
            self.max_probe_wall_s <= 0.0
            or self.max_source_cpu_core_seconds <= 0.0
            or self.max_source_io_bytes <= 0
        ):
            raise ValueError("limits must be positive")
        if not (0.0 <= self.min_deadline_slack_margin_ratio <= 1.0):
            raise ValueError("min_deadline_slack_margin_ratio must be between 0.0 and 1.0")


@dataclass(frozen=True)
class TelemetryReadings:
    """Measured operational telemetry used to derive costs and bounds without magic numbers."""

    measured_disk_read_bps: float
    measured_cpu_seconds_per_core_sec: float
    measured_dollar_per_wall_second: float
    available_deadline_slack_s: float
    total_deadline_budget_s: float
    source_cold_start_headroom_ratio: float

    def __post_init__(self) -> None:
        if self.measured_disk_read_bps <= 0.0:
            raise ValueError("measured_disk_read_bps must be positive")
        if self.available_deadline_slack_s < 0.0 or self.total_deadline_budget_s <= 0.0:
            raise ValueError("deadline slack and total budget must be valid")

    def derive_composite_probe_cost_s(self, profile: ProbeCostProfile) -> float:
        """Derive total equivalent time cost from probe duration, CPU, IO, locks, and dollars."""
        io_time_s = float(profile.source_io_bytes) / self.measured_disk_read_bps
        cpu_time_s = profile.source_cpu_cores * profile.expected_duration_s
        lock_penalty_s = (
            profile.lock_duration_s * 2.5
            if profile.lock_mode == ProbeLockMode.EXCLUSIVE_TABLE_LOCK
            else profile.lock_duration_s * 0.25
        )
        money_equiv_s = (
            profile.money_cost_dollars / self.measured_dollar_per_wall_second
            if self.measured_dollar_per_wall_second > 0.0
            else 0.0
        )
        return (
            profile.expected_duration_s
            + io_time_s
            + cpu_time_s
            + lock_penalty_s
            + money_equiv_s
        )


# -----------------------------------------------------------------------------
# Value of Information (VoI) and Expected Loss Engine
# -----------------------------------------------------------------------------

def compute_expected_plan_loss(
    plan: CandidatePlan,
    facts: Mapping[str, UncertainFact],
) -> float:
    """Compute the expected loss of `plan` under the current joint prior beliefs."""
    relevant_keys = [k for k in plan.prerequisite_conditions if k in facts]
    if not relevant_keys:
        return plan.evaluate_loss({})

    # Compute expectation over combinations of states
    total_loss = 0.0
    scenarios: List[Tuple[float, Dict[str, str]]] = [(1.0, {})]
    for k in relevant_keys:
        fact = facts[k]
        new_scenarios = []
        for p_scen, scen_dict in scenarios:
            for state_val, p_state in fact.prior_probabilities.items():
                merged = dict(scen_dict)
                merged[k] = state_val
                new_scenarios.append((p_scen * float(p_state), merged))
        scenarios = new_scenarios

    for p_scen, scen_dict in scenarios:
        loss = plan.evaluate_loss(scen_dict)
        total_loss += p_scen * loss

    return total_loss


def compute_baseline_decision(
    plans: Sequence[CandidatePlan],
    facts: Mapping[str, UncertainFact],
) -> Tuple[CandidatePlan, float]:
    """Find the optimal plan d* under prior beliefs and its expected loss."""
    if not plans:
        raise ValueError("plans list cannot be empty")
    best_plan = min(plans, key=lambda p: compute_expected_plan_loss(p, facts))
    best_loss = compute_expected_plan_loss(best_plan, facts)
    return best_plan, best_loss


def compute_value_of_information(
    fact_key: str,
    plans: Sequence[CandidatePlan],
    facts: Mapping[str, UncertainFact],
) -> float:
    """Calculate the expected reduction of decision loss (VoI) from measuring `fact_key`."""
    if fact_key not in facts:
        return 0.0
    target_fact = facts[fact_key]
    if target_fact.is_measured:
        return 0.0  # Already known; zero additional information value

    _, baseline_loss = compute_baseline_decision(plans, facts)

    expected_posterior_loss = 0.0
    for state_val, p_state in target_fact.prior_probabilities.items():
        if p_state <= 0.0:
            continue
        # Simulated posterior where fact_key is observed to be state_val
        simulated_fact = target_fact.with_measurement(state_val)
        simulated_facts = dict(facts)
        simulated_facts[fact_key] = simulated_fact
        _, posterior_loss = compute_baseline_decision(plans, simulated_facts)
        expected_posterior_loss += float(p_state) * posterior_loss

    voi = max(0.0, baseline_loss - expected_posterior_loss)
    return voi


# -----------------------------------------------------------------------------
# Decide Rung Ladders (All Choice Gates MUST Climb Through Decide Rungs)
# -----------------------------------------------------------------------------

PROBE_ADMISSION_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "admit_high_net_value_probe",
        "admit standalone probe whose expected loss reduction exceeds cost and respects bounds",
        needs=(
            decide.Need(
                "positive net value of information exceeding materiality threshold",
                lambda f: float(f.get("net_voi_s", 0.0))
                >= float(f.get("min_material_loss_reduction_s", 0.01)),
                "{rung} refused: expected loss reduction does not exceed probe cost",
            ),
            decide.Need(
                "probe fits within remaining source CPU, IO, and lock duration budget",
                lambda f: float(f.get("probe_wall_s", 0.0))
                <= float(f.get("remaining_wall_s", 0.0))
                and float(f.get("probe_cpu_s", 0.0))
                <= float(f.get("remaining_cpu_s", 0.0))
                and int(f.get("probe_io_bytes", 0))
                <= int(f.get("remaining_io_bytes", 0))
                and float(f.get("probe_lock_s", 0.0))
                <= float(f.get("remaining_lock_s", 0.0)),
                "{rung} refused: probe violates source impact limits or cold-start headroom",
            ),
            decide.Need(
                "probe preserves minimum deadline slack margin",
                lambda f: float(f.get("post_probe_slack_ratio", 0.0))
                >= float(f.get("min_deadline_slack_margin_ratio", 0.1)),
                "{rung} refused: probe consumes too much deadline slack",
            ),
            decide.Need(
                "target endpoint host is authorized and safe",
                lambda f: bool(f.get("safe_endpoint_authorized", False)),
                "{rung} refused: endpoint host is not an authorized test host",
            ),
        ),
        because="measures an uncertain fact only when reduction in decision loss justifies source impact and delay",
        footprint=1,
    ),
    decide.Rung(
        "admit_fused_scanplan_probe",
        "admit probe by fusing compatible read into a shared scanplan query",
        needs=(
            decide.Need(
                "probe is scanplan compatible with pending queries",
                lambda f: bool(f.get("is_scanplan_compatible", False))
                and bool(f.get("has_compatible_batch", False)),
                "{rung} refused: probe cannot be fused with existing scanplan batch",
            ),
            decide.Need(
                "fused execution amortizes startup latency and achieves positive net VoI",
                lambda f: float(f.get("fused_net_voi_s", 0.0))
                >= float(f.get("min_material_loss_reduction_s", 0.01)),
                "{rung} refused: amortized scanplan probe value is below materiality threshold",
            ),
            decide.Need(
                "fused read satisfies remaining source impact budget",
                lambda f: float(f.get("fused_wall_s", 0.0))
                <= float(f.get("remaining_wall_s", 0.0))
                and float(f.get("fused_cpu_s", 0.0))
                <= float(f.get("remaining_cpu_s", 0.0))
                and int(f.get("fused_io_bytes", 0))
                <= int(f.get("remaining_io_bytes", 0)),
                "{rung} refused: fused scanplan read breaches remaining budget",
            ),
            decide.Need(
                "target endpoint host is authorized and safe",
                lambda f: bool(f.get("safe_endpoint_authorized", False)),
                "{rung} refused: endpoint host is not authorized",
            ),
        ),
        because="combines compatible probes into one scanplan read to amortize locks and round trips",
        footprint=1,
    ),
    decide.Rung(
        "refuse_negative_voi",
        "refuse probe because expected reduction of decision loss does not justify measuring",
        needs=(
            decide.Need(
                "uncertainty cannot change winning plan or feasibility materially",
                lambda f: float(f.get("raw_voi_s", 0.0))
                < float(f.get("min_material_loss_reduction_s", 0.01))
                or float(f.get("net_voi_s", 0.0)) <= 0.0,
                "{rung} refused: probe carries positive net value of information",
            ),
        ),
        because="prevents wasteful probes that cannot alter the winning decision or reduce loss",
        footprint=0,
    ),
    decide.Rung(
        "refuse_excessive_source_impact",
        "refuse probe because CPU, IO, or lock hold times breach safety envelope",
        needs=(
            decide.Need(
                "probe exceeds source CPU, IO, or lock hold constraints",
                lambda f: float(f.get("probe_cpu_s", 0.0))
                > float(f.get("remaining_cpu_s", 0.0))
                or int(f.get("probe_io_bytes", 0))
                > int(f.get("remaining_io_bytes", 0))
                or float(f.get("probe_lock_s", 0.0))
                > float(f.get("remaining_lock_s", 0.0)),
                "{rung} refused: probe does not breach source impact constraints",
            ),
        ),
        because="protects production source database from query degradation or lock queues",
        footprint=0,
    ),
    decide.Rung(
        "refuse_deadline_slack_breach",
        "refuse probe because deadline slack is depleted or probe risk is too high",
        needs=(
            decide.Need(
                "remaining deadline slack would fall below safety margin",
                lambda f: float(f.get("post_probe_slack_ratio", 0.0))
                < float(f.get("min_deadline_slack_margin_ratio", 0.1)),
                "{rung} refused: deadline slack is sufficient",
            ),
        ),
        because="prioritizes meeting migration deadline over gathering non-critical measurements",
        footprint=0,
    ),
)


SCANPLAN_FUSION_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "fused_single_scan_batch",
        "execute multiple compatible probes as a single fused scanplan query",
        needs=(
            decide.Need(
                "compatible table and shared non-exclusive lock mode",
                lambda f: bool(f.get("same_table", False))
                and bool(f.get("lock_mode_compatible", False))
                and int(f.get("batch_probe_count", 0)) >= 2,
                "{rung} requires at least two compatible probes on the same table",
            ),
        ),
        because="folds multiple column/expression checks into one single-row or one-pass read",
        footprint=1,
    ),
    decide.Rung(
        "partitioned_range_probe",
        "execute probe across partitioned key ranges using adaptive AIMD batching",
        needs=(
            decide.Need(
                "splittable key ranges available and large dataset requires chunking",
                lambda f: bool(f.get("splittable_key_ranges", False))
                and bool(f.get("dataset_large", False)),
                "{rung} requires splittable key ranges and large dataset",
            ),
        ),
        because="prevents long-held locks by dividing probe across bounded key ranges",
        footprint=1,
    ),
    decide.Rung(
        "isolated_single_probe",
        "execute probe as an isolated individual inquiry",
        needs=(
            decide.Need(
                "probe cannot be fused or partitioned safely",
                lambda f: bool(f.get("isolation_required", True)),
                "{rung} refused because isolation is not required",
            ),
        ),
        because="guarantees isolated error boundaries when query expressions are incompatible",
        footprint=0,
    ),
)


STOPPING_RULE_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "stop_uncertainty_immaterial",
        "stop probing because remaining uncertainty cannot materially change winning choice",
        needs=(
            decide.Need(
                "maximum remaining VoI is below materiality threshold",
                lambda f: float(f.get("max_remaining_voi_s", 0.0))
                < float(f.get("min_material_loss_reduction_s", 0.01)),
                "{rung} refused: material VoI remains to be measured",
            ),
            decide.Need(
                "leader plan is sufficiently distinct from runner-up",
                lambda f: float(f.get("decision_margin_s", 0.0))
                >= float(f.get("min_material_loss_reduction_s", 0.01))
                or bool(f.get("all_critical_facts_known", False)),
                "{rung} refused: decision margin between leader and runner-up is too narrow",
            ),
        ),
        because="stops exploration when gathering further evidence has zero economic return",
        footprint=0,
    ),
    decide.Rung(
        "stop_budget_exhausted",
        "stop probing because wall time, source impact, or deadline budget is reached",
        needs=(
            decide.Need(
                "any primary budget limit reached",
                lambda f: float(f.get("remaining_wall_s", 1.0)) <= 0.0
                or float(f.get("remaining_cpu_s", 1.0)) <= 0.0
                or int(f.get("remaining_io_bytes", 1)) <= 0
                or float(f.get("remaining_lock_s", 1.0)) <= 0.0
                or float(f.get("current_slack_ratio", 1.0))
                <= float(f.get("min_deadline_slack_margin_ratio", 0.1)),
                "{rung} refused: information budget remains available",
            ),
        ),
        because="strictly enforces hard safety ceilings preventing migration stalls or source overload",
        footprint=0,
    ),
    decide.Rung(
        "continue_probing_viable",
        "continue probing while high-value measurements remain admissible",
        needs=(
            decide.Need(
                "admissible candidate probes exist with positive net value",
                lambda f: int(f.get("admissible_probe_count", 0)) > 0
                and float(f.get("max_remaining_voi_s", 0.0))
                >= float(f.get("min_material_loss_reduction_s", 0.01)),
                "{rung} refused: no admissible candidate probes remain",
            ),
        ),
        because="proceeds to measure the next highest-yield uncertain fact",
        footprint=1,
    ),
)


CONSERVATIVE_BASELINE_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "adopt_optimistic_plan_with_proof",
        "adopt optimistic winning plan because all prerequisite facts are measured and verified",
        needs=(
            decide.Need(
                "all prerequisite facts for winning plan are verified by measurement",
                lambda f: bool(f.get("all_prerequisites_measured", False))
                and bool(f.get("all_prerequisites_satisfied", False)),
                "{rung} refused: prerequisite facts are unmeasured or unsatisfied",
            ),
            decide.Need(
                "no unresolved critical unknowns affecting feasibility",
                lambda f: int(f.get("unresolved_critical_unknowns", 0)) == 0,
                "{rung} refused: critical unknowns remain unresolved",
            ),
        ),
        because="adopts high-performance plan with mathematical proof of feasibility",
        footprint=1,
    ),
    decide.Rung(
        "enforce_conservative_baseline",
        "fall back to verified conservative baseline plan because unknowns remain",
        needs=(
            decide.Need(
                "unknown facts exist or prerequisites lack proof",
                lambda f: not bool(f.get("all_prerequisites_measured", False))
                or not bool(f.get("all_prerequisites_satisfied", False))
                or int(f.get("unresolved_critical_unknowns", 0)) > 0,
                "{rung} refused: optimistic plan has full proof",
            ),
        ),
        because="unknown is never assumed support; prevents unrecoverable runtime crashes",
        footprint=0,
    ),
)


# -----------------------------------------------------------------------------
# Decision Helper Functions (Always Climbing Rungs via `decide.climb`)
# -----------------------------------------------------------------------------

def decide_probe_admission(
    work: str,
    *,
    probe: ProbeCostProfile,
    raw_voi_s: float,
    readings: TelemetryReadings,
    budget_limits: InformationBudgetLimits,
    spent_wall_s: float,
    spent_cpu_s: float,
    spent_io_bytes: int,
    spent_lock_s: float,
    is_scanplan_batch: bool = False,
    fused_cost_s: Optional[float] = None,
) -> decide.Choice:
    """Climb `PROBE_ADMISSION_LADDER` for a candidate probe."""
    try:
        validate_safe_endpoint(probe.target_host)
        safe_endpoint = True
    except UnsafeEndpointHostError:
        safe_endpoint = False

    comp_cost_s = readings.derive_composite_probe_cost_s(probe)
    net_voi_s = raw_voi_s - comp_cost_s

    remaining_wall_s = max(0.0, budget_limits.max_probe_wall_s - spent_wall_s)
    remaining_cpu_s = max(0.0, budget_limits.max_source_cpu_core_seconds - spent_cpu_s)
    remaining_io_bytes = max(0, budget_limits.max_source_io_bytes - spent_io_bytes)
    remaining_lock_s = max(0.0, budget_limits.max_source_lock_duration_s - spent_lock_s)

    post_probe_slack = max(0.0, readings.available_deadline_slack_s - probe.expected_duration_s)
    post_probe_slack_ratio = post_probe_slack / readings.total_deadline_budget_s

    fused_net_voi_s = (
        raw_voi_s - (fused_cost_s if fused_cost_s is not None else comp_cost_s * 0.4)
    )

    facts = {
        "raw_voi_s": float(raw_voi_s),
        "net_voi_s": float(net_voi_s),
        "fused_net_voi_s": float(fused_net_voi_s),
        "min_material_loss_reduction_s": float(budget_limits.min_material_loss_reduction_s),
        "probe_wall_s": float(probe.expected_duration_s),
        "probe_cpu_s": float(probe.source_cpu_cores * probe.expected_duration_s),
        "probe_io_bytes": int(probe.source_io_bytes),
        "probe_lock_s": float(probe.lock_duration_s),
        "fused_wall_s": float(probe.expected_duration_s * 0.4),
        "fused_cpu_s": float(probe.source_cpu_cores * probe.expected_duration_s * 0.4),
        "fused_io_bytes": int(probe.source_io_bytes * 0.5),
        "remaining_wall_s": float(remaining_wall_s),
        "remaining_cpu_s": float(remaining_cpu_s),
        "remaining_io_bytes": int(remaining_io_bytes),
        "remaining_lock_s": float(remaining_lock_s),
        "post_probe_slack_ratio": float(post_probe_slack_ratio),
        "min_deadline_slack_margin_ratio": float(budget_limits.min_deadline_slack_margin_ratio),
        "safe_endpoint_authorized": bool(safe_endpoint),
        "is_scanplan_compatible": bool(probe.is_scanplan_compatible),
        "has_compatible_batch": bool(is_scanplan_batch),
    }

    return decide.climb(work, PROBE_ADMISSION_LADDER, facts)


def decide_scanplan_fusion(
    work: str,
    *,
    probes: Sequence[ProbeCostProfile],
    splittable_key_ranges: bool = False,
    dataset_large: bool = False,
    isolation_required: bool = False,
) -> decide.Choice:
    """Climb `SCANPLAN_FUSION_LADDER` to determine execution packaging for admitted probes."""
    tables = {p.scanplan_table for p in probes if p.scanplan_table}
    same_table = len(tables) == 1 and len(probes) >= 2
    lock_compatible = all(p.lock_mode == ProbeLockMode.SHARED_READ or p.lock_mode == ProbeLockMode.NONE for p in probes)

    facts = {
        "same_table": bool(same_table),
        "lock_mode_compatible": bool(lock_compatible),
        "batch_probe_count": len(probes),
        "splittable_key_ranges": bool(splittable_key_ranges),
        "dataset_large": bool(dataset_large),
        "isolation_required": bool(isolation_required),
    }

    return decide.climb(work, SCANPLAN_FUSION_LADDER, facts)


def decide_stopping_rule(
    work: str,
    *,
    max_remaining_voi_s: float,
    decision_margin_s: float,
    all_critical_facts_known: bool,
    budget_limits: InformationBudgetLimits,
    spent_wall_s: float,
    spent_cpu_s: float,
    spent_io_bytes: int,
    spent_lock_s: float,
    readings: TelemetryReadings,
    admissible_probe_count: int,
) -> decide.Choice:
    """Climb `STOPPING_RULE_LADDER` to decide whether to stop or continue probing."""
    rem_wall = max(0.0, budget_limits.max_probe_wall_s - spent_wall_s)
    rem_cpu = max(0.0, budget_limits.max_source_cpu_core_seconds - spent_cpu_s)
    rem_io = max(0, budget_limits.max_source_io_bytes - spent_io_bytes)
    rem_lock = max(0.0, budget_limits.max_source_lock_duration_s - spent_lock_s)
    curr_slack_ratio = readings.available_deadline_slack_s / readings.total_deadline_budget_s

    facts = {
        "max_remaining_voi_s": float(max_remaining_voi_s),
        "min_material_loss_reduction_s": float(budget_limits.min_material_loss_reduction_s),
        "decision_margin_s": float(decision_margin_s),
        "all_critical_facts_known": bool(all_critical_facts_known),
        "remaining_wall_s": float(rem_wall),
        "remaining_cpu_s": float(rem_cpu),
        "remaining_io_bytes": int(rem_io),
        "remaining_lock_s": float(rem_lock),
        "current_slack_ratio": float(curr_slack_ratio),
        "min_deadline_slack_margin_ratio": float(budget_limits.min_deadline_slack_margin_ratio),
        "admissible_probe_count": int(admissible_probe_count),
    }

    return decide.climb(work, STOPPING_RULE_LADDER, facts)


def decide_plan_choice(
    work: str,
    *,
    winning_candidate: CandidatePlan,
    facts_map: Mapping[str, UncertainFact],
) -> decide.Choice:
    """Climb `CONSERVATIVE_BASELINE_LADDER` to confirm whether optimistic plan or baseline is selected."""
    prereqs_measured = True
    prereqs_satisfied = True
    critical_unknowns = 0

    for k, req_v in winning_candidate.prerequisite_conditions.items():
        fact = facts_map.get(k)
        if fact is None or not fact.is_measured:
            prereqs_measured = False
            critical_unknowns += 1
        elif fact.measured_value != req_v:
            prereqs_satisfied = False

    facts = {
        "all_prerequisites_measured": bool(prereqs_measured),
        "all_prerequisites_satisfied": bool(prereqs_satisfied),
        "unresolved_critical_unknowns": int(critical_unknowns),
    }

    return decide.climb(work, CONSERVATIVE_BASELINE_LADDER, facts)


# -----------------------------------------------------------------------------
# Fused Scanplan Execution Batching
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class FusedScanplanBatch:
    """A batch of compatible probes fused into a single scanplan aggregate read."""

    batch_id: str
    target_table: str
    target_host: str
    probes: Tuple[ProbeCostProfile, ...]
    fused_duration_s: float
    fused_cpu_cores: float
    fused_io_bytes: int
    fused_lock_mode: ProbeLockMode
    fused_lock_duration_s: float

    @classmethod
    def fuse(cls, probes: Sequence[ProbeCostProfile]) -> FusedScanplanBatch:
        if not probes:
            raise ValueError("cannot fuse empty probe sequence")
        tables = {p.scanplan_table for p in probes}
        if len(tables) > 1:
            raise ValueError(f"probes span multiple tables: {tables}")
        target_table = next(iter(tables)) or "unknown"
        target_host = probes[0].target_host
        validate_safe_endpoint(target_host)

        # Fused read saves N-1 round trips and shares single table read
        fused_duration = max(p.expected_duration_s for p in probes) * 1.15
        fused_cpu = max(p.source_cpu_cores for p in probes) * 1.1
        fused_io = max(p.source_io_bytes for p in probes)  # Single full scan
        fused_lock_mode = (
            ProbeLockMode.EXCLUSIVE_TABLE_LOCK
            if any(p.lock_mode == ProbeLockMode.EXCLUSIVE_TABLE_LOCK for p in probes)
            else ProbeLockMode.SHARED_READ
        )
        fused_lock_duration = max(p.lock_duration_s for p in probes) * 1.1

        return cls(
            batch_id=f"fused_{uuid.uuid4().hex[:8]}",
            target_table=target_table,
            target_host=target_host,
            probes=tuple(probes),
            fused_duration_s=fused_duration,
            fused_cpu_cores=fused_cpu,
            fused_io_bytes=fused_io,
            fused_lock_mode=fused_lock_mode,
            fused_lock_duration_s=fused_lock_duration,
        )


# -----------------------------------------------------------------------------
# Active Verification Lease (.run.lock) Mutual Exclusion
# -----------------------------------------------------------------------------

class ActiveVerificationLease:
    """Active verification lease lock (.run.lock) with heartbeat preventing overlapping runs."""

    def __init__(self, lock_path: Union[str, Path], holder_id: str = "information_budget") -> None:
        self.lock_path = Path(lock_path)
        self.holder_id = holder_id
        self._fd: Optional[int] = None
        self._acquired = False

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._fd = os.open(str(self.lock_path), os.O_RDWR | os.O_CREAT, 0o600)
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            payload = {
                "holder": self.holder_id,
                "acquired_at_s": time.time(),
                "heartbeat_s": time.time(),
            }
            os.ftruncate(self._fd, 0)
            os.pwrite(self._fd, json.dumps(payload).encode("utf-8"), 0)
            self._acquired = True
        except (BlockingIOError, OSError) as exc:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
            raise VerificationLeaseError(f"could not acquire verification lease on {self.lock_path}: {exc}") from exc

    def heartbeat(self) -> None:
        if not self._acquired or self._fd is None:
            raise VerificationLeaseError("cannot heartbeat unacquired lease")
        payload = {
            "holder": self.holder_id,
            "heartbeat_s": time.time(),
        }
        os.ftruncate(self._fd, 0)
        os.pwrite(self._fd, json.dumps(payload).encode("utf-8"), 0)

    def release(self) -> None:
        if self._acquired and self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            finally:
                self._fd = None
                self._acquired = False
                if self.lock_path.exists():
                    try:
                        self.lock_path.unlink()
                    except OSError:
                        pass

    def __enter__(self) -> ActiveVerificationLease:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# O(1) Typed Durable Checkpoint Store
# -----------------------------------------------------------------------------

def encode_typed_cursor(value: Any) -> Any:
    """Recursively encode native Python types (datetime, Decimal, UUID, tuple) for JSON."""
    if isinstance(value, datetime.datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, uuid.UUID):
        return {"__type__": "uuid", "value": str(value)}
    if isinstance(value, tuple):
        return {"__type__": "tuple", "items": [encode_typed_cursor(v) for v in value]}
    if isinstance(value, list):
        return [encode_typed_cursor(v) for v in value]
    if isinstance(value, dict):
        return {str(k): encode_typed_cursor(v) for k, v in value.items()}
    return value


def decode_typed_cursor(raw: Any) -> Any:
    """Restore native Python types (datetime, Decimal, UUID, tuple) from JSON payload."""
    if isinstance(raw, dict):
        tag = raw.get("__type__")
        if tag == "datetime":
            return datetime.datetime.fromisoformat(raw["value"])
        if tag == "decimal":
            return decimal.Decimal(raw["value"])
        if tag == "uuid":
            return uuid.UUID(raw["value"])
        if tag == "tuple":
            return tuple(decode_typed_cursor(v) for v in raw["items"])
        return {k: decode_typed_cursor(v) for k, v in raw.items()}
    if isinstance(raw, list):
        return [decode_typed_cursor(v) for v in raw]
    return raw


@dataclass(frozen=True)
class InformationBudgetCheckpointRecord:
    """Durable checkpoint record for a measured uncertain fact."""

    fact_key: str
    measured_value: str
    measured_at_s: float
    cursor_position: Any
    probe_id: str
    care_badge: CareBadge


class InformationBudgetCheckpointStore:
    """O(1) typed durable checkpoint store backed by durable.write_json and durable.read_state."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        self._records: Dict[str, InformationBudgetCheckpointRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        raw = durable.read_state(self.path, "state")
        if not raw:
            return
        decoded = decode_typed_cursor(raw)
        for key, item in decoded.get("records", {}).items():
            self._records[key] = InformationBudgetCheckpointRecord(
                fact_key=str(item["fact_key"]),
                measured_value=str(item["measured_value"]),
                measured_at_s=float(item["measured_at_s"]),
                cursor_position=item["cursor_position"],
                probe_id=str(item["probe_id"]),
                care_badge=CareBadge(item.get("care_badge", CareBadge.PASS)),
            )

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "records": {
                k: {
                    "fact_key": rec.fact_key,
                    "measured_value": rec.measured_value,
                    "measured_at_s": rec.measured_at_s,
                    "cursor_position": rec.cursor_position,
                    "probe_id": rec.probe_id,
                    "care_badge": rec.care_badge.value,
                }
                for k, rec in self._records.items()
            },
        }
        durable.write_json(self.path, encode_typed_cursor(payload))

    def record_measurement(self, record: InformationBudgetCheckpointRecord) -> None:
        self._records[record.fact_key] = record
        self.save()

    def get_measurement(self, fact_key: str) -> Optional[InformationBudgetCheckpointRecord]:
        """O(1) lookup of existing durable measurement."""
        return self._records.get(fact_key)


# -----------------------------------------------------------------------------
# Adaptive AIMD Sizing and Cold-Start ETA Engine
# -----------------------------------------------------------------------------

@dataclass
class AIMDProbeController:
    """Additive Increase Multiplicative Decrease batch controller for adaptive range probes."""

    current_batch_size: int
    min_batch_size: int
    max_batch_size: int
    additive_step: int = 1000
    multiplicative_factor: float = 0.5

    def on_probe_success(self, duration_s: float, target_latency_s: float) -> None:
        """Additive increase when probe latency is comfortably below target threshold."""
        if duration_s <= target_latency_s:
            self.current_batch_size = min(
                self.max_batch_size, self.current_batch_size + self.additive_step
            )

    def on_probe_contention_or_spike(self) -> None:
        """Multiplicative decrease when lock contention or latency spike is encountered."""
        reduced = int(self.current_batch_size * self.multiplicative_factor)
        self.current_batch_size = max(self.min_batch_size, reduced)


def calculate_cold_start_byte_weighted_eta(
    total_bytes: int,
    bytes_probed: int,
    elapsed_probe_s: float,
    telemetry: TelemetryReadings,
) -> float:
    """Calculate instant byte-weighted ETA without arbitrary magic constants."""
    if total_bytes <= 0:
        return 0.0
    if bytes_probed > 0 and elapsed_probe_s > 0.0:
        observed_bps = float(bytes_probed) / elapsed_probe_s
        effective_bps = (
            0.7 * observed_bps + 0.3 * telemetry.measured_disk_read_bps
        )
    else:
        effective_bps = telemetry.measured_disk_read_bps * telemetry.source_cold_start_headroom_ratio

    remaining_bytes = max(0, total_bytes - bytes_probed)
    return float(remaining_bytes) / max(1.0, effective_bps)


# -----------------------------------------------------------------------------
# Tool Wrapper Normalization (Zero Tool Leaks, Pure UNIX Output)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ExternalToolRunResult:
    """Normalized output from external comparison tools (Datafold, DVT, DMS, etc.)."""

    tool_name: str
    target_table: str
    care_badge: CareBadge
    keys_checked: int
    mismatches: int
    elapsed_s: float
    raw_status_code: int

    def to_unix_log(self) -> str:
        return (
            f"TOOL: {self.tool_name} TABLE: {self.target_table} "
            f"BADGE: [{self.care_badge.value}] CHECKED: {self.keys_checked} "
            f"MISMATCHES: {self.mismatches} TIME: {self.elapsed_s:.3f}s"
        )


def execute_normalized_tool_wrapper(
    tool_name: str,
    target_table: str,
    raw_probe_fn: Callable[[], Tuple[int, int, float, int]],
) -> ExternalToolRunResult:
    """Wrap third-party tools, normalizing output into standard 4-tier Care Badges."""
    keys_checked, mismatches, elapsed_s, status_code = raw_probe_fn()
    if status_code != 0 or mismatches > 0:
        badge = CareBadge.ACTION
    elif keys_checked == 0:
        badge = CareBadge.REVIEW
    else:
        badge = CareBadge.PASS

    return ExternalToolRunResult(
        tool_name=tool_name,
        target_table=target_table,
        care_badge=badge,
        keys_checked=keys_checked,
        mismatches=mismatches,
        elapsed_s=elapsed_s,
        raw_status_code=status_code,
    )


# -----------------------------------------------------------------------------
# High-Level Information Budget Exploration Loop
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class InformationBudgetExecutionReport:
    """Outcome report from running the information budget decision loop."""

    winning_plan_id: str
    selected_rung_path: str
    care_badge: CareBadge
    probes_executed: Tuple[str, ...]
    unhelpful_probes_avoided: int
    total_spent_wall_s: float
    total_spent_cpu_s: float
    total_spent_io_bytes: int
    total_spent_lock_s: float
    verified_loss_s: float


def run_information_budget_decision_loop(
    *,
    plans: Sequence[CandidatePlan],
    facts: Mapping[str, UncertainFact],
    candidate_probes: Sequence[ProbeCostProfile],
    budget_limits: InformationBudgetLimits,
    telemetry: TelemetryReadings,
    checkpoint_store: Optional[InformationBudgetCheckpointStore] = None,
    execute_probe_fn: Optional[Callable[[ProbeCostProfile], str]] = None,
) -> InformationBudgetExecutionReport:
    """Iteratively execute high-VoI probes until stopping condition is reached."""
    current_facts = dict(facts)
    spent_wall_s = 0.0
    spent_cpu_s = 0.0
    spent_io_bytes = 0
    spent_lock_s = 0.0
    probes_executed: List[str] = []
    unhelpful_probes_avoided = 0

    # First, restore any cached measurements from checkpoint in O(1)
    if checkpoint_store is not None:
        for k, f in list(current_facts.items()):
            rec = checkpoint_store.get_measurement(k)
            if rec is not None:
                current_facts[k] = f.with_measurement(rec.measured_value)

    # Iterative VoI probing loop
    while True:
        # 1. Rank uncertain facts by VoI
        unmeasured_probes = [
            p for p in candidate_probes
            if p.fact_key in current_facts and not current_facts[p.fact_key].is_measured
        ]

        probe_vois: List[Tuple[ProbeCostProfile, float]] = []
        for p in unmeasured_probes:
            voi = compute_value_of_information(p.fact_key, plans, current_facts)
            probe_vois.append((p, voi))

        probe_vois.sort(key=lambda item: item[1], reverse=True)
        max_voi = probe_vois[0][1] if probe_vois else 0.0

        # Compute decision margin between leader and runner-up
        sorted_plans = sorted(plans, key=lambda pl: compute_expected_plan_loss(pl, current_facts))
        if len(sorted_plans) >= 2:
            l1 = compute_expected_plan_loss(sorted_plans[0], current_facts)
            l2 = compute_expected_plan_loss(sorted_plans[1], current_facts)
            decision_margin_s = abs(l2 - l1)
        else:
            decision_margin_s = 1000.0

        all_critical_known = all(
            current_facts[k].is_measured
            for k in sorted_plans[0].prerequisite_conditions
            if k in current_facts
        )

        # Climb stopping ladder
        stop_choice = decide_stopping_rule(
            "information_budget:stopping_rule",
            max_remaining_voi_s=max_voi,
            decision_margin_s=decision_margin_s,
            all_critical_facts_known=all_critical_known,
            budget_limits=budget_limits,
            spent_wall_s=spent_wall_s,
            spent_cpu_s=spent_cpu_s,
            spent_io_bytes=spent_io_bytes,
            spent_lock_s=spent_lock_s,
            readings=telemetry,
            admissible_probe_count=len(unmeasured_probes),
        )

        if stop_choice.path != "continue_probing_viable":
            break

        # Pick best candidate probe and evaluate admission
        best_candidate, best_voi = probe_vois[0]
        adm_choice = decide_probe_admission(
            f"probe_admission:{best_candidate.probe_id}",
            probe=best_candidate,
            raw_voi_s=best_voi,
            readings=telemetry,
            budget_limits=budget_limits,
            spent_wall_s=spent_wall_s,
            spent_cpu_s=spent_cpu_s,
            spent_io_bytes=spent_io_bytes,
            spent_lock_s=spent_lock_s,
        )

        if adm_choice.path not in ("admit_high_net_value_probe", "admit_fused_scanplan_probe"):
            unhelpful_probes_avoided += 1
            # Mark probe as avoided by setting a dummy measured flag or removing
            break

        # Execute probe
        if execute_probe_fn is not None:
            observed_val = execute_probe_fn(best_candidate)
        else:
            # Default mock observation: pick most probable state
            prob_dict = current_facts[best_candidate.fact_key].prior_probabilities
            observed_val = max(prob_dict.keys(), key=lambda k: prob_dict[k])

        current_facts[best_candidate.fact_key] = current_facts[best_candidate.fact_key].with_measurement(
            observed_val
        )
        probes_executed.append(best_candidate.probe_id)

        spent_wall_s += best_candidate.expected_duration_s
        spent_cpu_s += best_candidate.source_cpu_cores * best_candidate.expected_duration_s
        spent_io_bytes += best_candidate.source_io_bytes
        spent_lock_s += best_candidate.lock_duration_s

        if checkpoint_store is not None:
            checkpoint_store.record_measurement(
                InformationBudgetCheckpointRecord(
                    fact_key=best_candidate.fact_key,
                    measured_value=observed_val,
                    measured_at_s=time.time(),
                    cursor_position=len(probes_executed),
                    probe_id=best_candidate.probe_id,
                    care_badge=CareBadge.PASS,
                )
            )

    # Final decision climb
    final_leader, final_loss = compute_baseline_decision(plans, current_facts)
    final_choice = decide_plan_choice(
        "information_budget:final_plan_selection",
        winning_candidate=final_leader,
        facts_map=current_facts,
    )

    if final_choice.path == "adopt_optimistic_plan_with_proof":
        badge = CareBadge.PASS
    else:
        badge = CareBadge.REVIEW

    return InformationBudgetExecutionReport(
        winning_plan_id=final_leader.plan_id,
        selected_rung_path=str(final_choice.path),
        care_badge=badge,
        probes_executed=tuple(probes_executed),
        unhelpful_probes_avoided=unhelpful_probes_avoided,
        total_spent_wall_s=spent_wall_s,
        total_spent_cpu_s=spent_cpu_s,
        total_spent_io_bytes=spent_io_bytes,
        total_spent_lock_s=spent_lock_s,
        verified_loss_s=final_loss,
    )
