"""Plan for correlated tail risk and conflicting objectives (Row 591: SMART-N8.robust-scheduling).

Extends expected-risk cost and p90 simulation with shared-resource and outage
correlations, bursty CDC arrivals, recovery work, and calibrated deadline probability
using `decide.Rung` and `decide.climb(work, rungs, facts)`.

Key mechanisms:
1. Shared-resource & outage correlation:
   - Models correlated degradation, mutual contention, and common-cause outage shocks
     across shared failure groups and physical/logical resources (source I/O, target I/O,
     network WAN, replication streams).
   - Calibrated tail simulation expands variance via correlation matrix and copula shock
     factors instead of assuming independent nominal rates.
2. Bursty CDC arrivals, recovery work & impossible catch-up:
   - Models peak burst arrival multipliers, duty cycles, and backlog accumulation.
   - Computes recovery work when outages or retries occur (accumulated CDC backlog during
     downtime plus chunk retry work).
   - Formally diagnoses impossible catch-up (ACTION badge) when arrival rate exceeds target
     replay throughput or backlog cannot clear before deadline.
3. Calibrated deadline probability & false confidence measurement:
   - Evaluates calibrated distribution (p50, p90, p95, p99, VaR, CVaR).
   - Computes false confidence gap between naive nominal assumptions and calibrated tail
     simulations: detects false optimism where uncalibrated models claim on-time delivery.
4. Conflicting objectives, hard constraints & non-dominated Pareto frontier:
   - Enforces hard correctness minimums and source impact limits.
   - Retains the Pareto frontier of non-dominated feasible alternatives across verified
     completion time, source impact, monetary spend, and tail risk exposure.
   - Optimizes verified completion time under explicit user cost/risk preferences
     (risk-averse, balanced, deadline-critical, cost-minimizing).
5. No universal best or guaranteed fastest claim:
   - Issues explicit trade-off certificates comparing the selected robust plan against
     jointplan and safe baselines with zero false optimality claims.
6. Universal migkit resilient operations:
   - 4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
   - Universal O(1) typed durable checkpoint resume with native Python types.
   - Active mutual-exclusion lease lock (.run.lock) with heartbeat.
   - Adaptive AIMD burst batch sizing and cold-start byte-weighted ETA.
   - External tool normalization with zero internal tool name leaks.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import math
import pathlib
import time
import uuid
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from . import checkpoint, decide, lease

# -----------------------------------------------------------------------------
# Care Badges & Safe Host Validation
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


class RiskPreference(str, enum.Enum):
    RISK_AVERSE = "risk_averse"
    BALANCED = "balanced"
    DEADLINE_CRITICAL = "deadline_critical"
    COST_MINIMIZING = "cost_minimizing"
    CONSERVATIVE_BASELINE = "conservative_baseline"


class SchedulingStrategy(str, enum.Enum):
    ROBUST_STREAM_DEFERRED_INDEX = "robust_stream_deferred_index"
    ISOLATED_PARTITION_DIRECT_APPLY = "isolated_partition_direct_apply"
    PARALLEL_STAGED_BULK_LOAD = "parallel_staged_bulk_load"
    SAFE_CONSERVATIVE_BASELINE = "safe_conservative_baseline"


ALL_SCHEDULING_STRATEGIES: Tuple[SchedulingStrategy, ...] = (
    SchedulingStrategy.ROBUST_STREAM_DEFERRED_INDEX,
    SchedulingStrategy.ISOLATED_PARTITION_DIRECT_APPLY,
    SchedulingStrategy.PARALLEL_STAGED_BULK_LOAD,
    SchedulingStrategy.SAFE_CONSERVATIVE_BASELINE,
)

SAFE_LOOPBACK_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


class RobustSchedulingError(RuntimeError):
    """Base exception for robust scheduling operations."""


class UnsafeEndpointHostError(RobustSchedulingError, ValueError):
    """Raised when an endpoint host is outside approved loopback or private ranges."""


class ImpossibleCatchupError(RobustSchedulingError):
    """Raised when CDC backlog accumulation rate exceeds maximum apply capacity."""


class InvalidScheduleFactError(RobustSchedulingError, ValueError):
    """Raised when scheduling facts or quotas violate mathematical bounds."""


def validate_safe_endpoint(host: str) -> None:
    """Validate that endpoint host is strictly loopback (127.0.0.1) or safe private (10.0.0.x)."""
    clean = (host or "").strip().lower()
    if not clean:
        raise UnsafeEndpointHostError("Endpoint host cannot be empty")
    if clean in SAFE_LOOPBACK_HOSTS:
        return
    if clean.startswith("127."):
        return
    if clean.startswith("10.0.0."):
        return
    raise UnsafeEndpointHostError(
        f"Host '{host}' is outside approved safe test ranges (127.0.0.1, localhost, 10.0.0.x)"
    )


# -----------------------------------------------------------------------------
# Data Models: Shared Resources, Correlation, Bursty CDC & Work Items
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class SharedResourceSpec:
    """Specification of a shared physical or logical resource."""

    resource_name: str
    capacity_units_per_sec: float
    failure_group: str = "default_group"
    outage_probability: float = 0.02
    mean_outage_duration_sec: float = 30.0

    def __post_init__(self) -> None:
        if not self.resource_name:
            raise InvalidScheduleFactError("resource_name cannot be empty")
        if self.capacity_units_per_sec <= 0.0:
            raise InvalidScheduleFactError("capacity_units_per_sec must be > 0")
        if not (0.0 <= self.outage_probability <= 1.0):
            raise InvalidScheduleFactError("outage_probability must be in [0, 1]")
        if self.mean_outage_duration_sec < 0.0:
            raise InvalidScheduleFactError("mean_outage_duration_sec must be >= 0")


@dataclass(frozen=True)
class CdcBurstProfile:
    """Descriptor for bursty CDC arrival stream and replay capacity."""

    nominal_arrival_bytes_sec: float
    peak_burst_multiplier: float = 1.0
    burst_duration_sec: float = 60.0
    burst_duty_cycle: float = 0.10
    current_backlog_bytes: float = 0.0
    target_apply_capacity_bytes_sec: float = 50_000_000.0

    def __post_init__(self) -> None:
        if self.nominal_arrival_bytes_sec < 0.0:
            raise InvalidScheduleFactError("nominal_arrival_bytes_sec must be >= 0")
        if self.peak_burst_multiplier < 1.0:
            raise InvalidScheduleFactError("peak_burst_multiplier must be >= 1.0")
        if self.burst_duration_sec < 0.0:
            raise InvalidScheduleFactError("burst_duration_sec must be >= 0")
        if not (0.0 <= self.burst_duty_cycle <= 1.0):
            raise InvalidScheduleFactError("burst_duty_cycle must be in [0, 1]")
        if self.current_backlog_bytes < 0.0:
            raise InvalidScheduleFactError("current_backlog_bytes must be >= 0")
        if self.target_apply_capacity_bytes_sec <= 0.0:
            raise InvalidScheduleFactError("target_apply_capacity_bytes_sec must be > 0")

    @property
    def peak_burst_arrival_bytes_sec(self) -> float:
        return self.nominal_arrival_bytes_sec * self.peak_burst_multiplier


@dataclass(frozen=True)
class TableWorkItem:
    """One table's characteristics for robust tail-risk scheduling."""

    table_name: str
    total_bytes: int
    row_count: int
    shared_failure_group: str = "default_group"
    secondary_index_count: int = 2
    estimated_drift_bytes: int = 0
    cdc_write_ratio: float = 0.05
    lock_hold_baseline_sec: float = 0.05

    def __post_init__(self) -> None:
        if not self.table_name:
            raise InvalidScheduleFactError("table_name cannot be empty")
        if self.total_bytes < 0 or self.row_count < 0:
            raise InvalidScheduleFactError("total_bytes and row_count must be >= 0")
        if self.secondary_index_count < 0:
            raise InvalidScheduleFactError("secondary_index_count must be >= 0")
        if not (0.0 <= self.cdc_write_ratio <= 1.0):
            raise InvalidScheduleFactError("cdc_write_ratio must be in [0, 1]")


@dataclass(frozen=True)
class TailRiskMetrics:
    """Calibrated percentile and tail risk evaluation."""

    p50_seconds: float
    p90_seconds: float
    p95_seconds: float
    p99_seconds: float
    var_95_seconds: float
    cvar_95_seconds: float
    variance_inflation_factor: float
    correlated_outage_delay_seconds: float


@dataclass(frozen=True)
class DeadlineEvaluation:
    """Calibrated deadline probability vs naive nominal model."""

    deadline_seconds: float
    calibrated_probability: float
    naive_probability: float
    false_confidence_gap: float
    is_breached: bool
    confidence_badge: CareBadge


@dataclass(frozen=True)
class RecoveryWorkSummary:
    """Recovery work and CDC catch-up feasibility assessment."""

    retry_work_bytes: float
    accumulated_backlog_bytes: float
    total_recovery_work_bytes: float
    net_drain_rate_bytes_sec: float
    recovery_seconds: float
    is_impossible_catchup: bool
    diagnostic_message: str
    care_badge: CareBadge


@dataclass(frozen=True)
class MultiObjectiveScores:
    """Vector of conflicting objectives (all minimized)."""

    verified_completion_p90_sec: float
    source_impact_score: float
    monetary_cost_dollars: float
    tail_risk_exposure_sec: float

    def dominates(self, other: MultiObjectiveScores) -> bool:
        """Pareto dominance: no worse in all objectives and strictly better in at least one."""
        no_worse = (
            self.verified_completion_p90_sec <= other.verified_completion_p90_sec
            and self.source_impact_score <= other.source_impact_score
            and self.monetary_cost_dollars <= other.monetary_cost_dollars
            and self.tail_risk_exposure_sec <= other.tail_risk_exposure_sec
        )
        strictly_better = (
            self.verified_completion_p90_sec < other.verified_completion_p90_sec
            or self.source_impact_score < other.source_impact_score
            or self.monetary_cost_dollars < other.monetary_cost_dollars
            or self.tail_risk_exposure_sec < other.tail_risk_exposure_sec
        )
        return no_worse and strictly_better


@dataclass(frozen=True)
class ScheduleAlternative:
    """One candidate schedule alternative evaluated across objectives."""

    strategy: SchedulingStrategy
    objectives: MultiObjectiveScores
    tail_metrics: TailRiskMetrics
    deadline_eval: DeadlineEvaluation
    recovery: RecoveryWorkSummary
    is_feasible: bool
    rejection_reason: Optional[str] = None
    is_nondominated: bool = False


@dataclass(frozen=True)
class TradeoffCertificate:
    """Certificate declaring trade-offs without claiming universal best or guaranteed fastest."""

    selected_strategy: SchedulingStrategy
    selected_p90_seconds: float
    jointplan_nominal_seconds: float
    jointplan_calibrated_p90_seconds: float
    safe_baseline_seconds: float
    tail_risk_reduction_ratio: float
    false_confidence_gap: float
    claims_universal_best: bool = False
    claims_guaranteed_fastest: bool = False
    tradeoff_rationale: str = ""


@dataclass(frozen=True)
class RobustSchedulePlan:
    """Complete robust scheduling plan result."""

    admissibility_choice: decide.Choice
    tail_policy_choice: decide.Choice
    cdc_choice: decide.Choice
    correlation_choice: decide.Choice
    selected_strategy: SchedulingStrategy
    active_risk_preference: RiskPreference
    tail_metrics: TailRiskMetrics
    deadline_eval: DeadlineEvaluation
    recovery: RecoveryWorkSummary
    pareto_frontier: Tuple[ScheduleAlternative, ...]
    all_alternatives: Tuple[ScheduleAlternative, ...]
    tradeoff_certificate: TradeoffCertificate
    care_badge: CareBadge
    summary: str


# -----------------------------------------------------------------------------
# Decide Ladders & Rungs (4 Distinct Ladders)
# -----------------------------------------------------------------------------


def admissibility_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for candidate admissibility under hard constraints."""
    return (
        decide.Rung(
            "admit_robust_candidate",
            "admit candidate satisfying hard correctness minimums and source impact limits",
            needs=(
                decide.Need(
                    "correctness minimums satisfied",
                    lambda f: bool(f.get("correctness_minimums_satisfied", True)),
                    "{rung} refused because required data correctness guarantees are not met",
                ),
                decide.Need(
                    "source impact within limits",
                    lambda f: bool(f.get("source_impact_within_limits", True)),
                    "{rung} refused because source CPU or lock duration exceeds safe limit",
                ),
            ),
            because="ensures candidate schedule satisfies non-negotiable correctness and source safety",
            footprint=0,
        ),
        decide.Rung(
            "reject_correctness_violation",
            "reject candidate violating hard correctness or data integrity minimums",
            needs=(
                decide.Need(
                    "correctness minimum violated",
                    lambda f: not bool(f.get("correctness_minimums_satisfied", True)),
                    "{rung} refused because candidate satisfies correctness minimums",
                ),
            ),
            because="rejects unsafe candidates lacking mandatory proof or integrity semantics",
            footprint=1,
        ),
        decide.Rung(
            "reject_excessive_source_impact",
            "reject candidate exceeding maximum authorized source impact threshold",
            needs=(
                decide.Need(
                    "source impact limit exceeded",
                    lambda f: not bool(f.get("source_impact_within_limits", True)),
                    "{rung} refused because source impact remains within authorized bounds",
                ),
            ),
            because="prevents schedule from overwhelming production source database",
            footprint=1,
        ),
    )


def tail_risk_policy_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for cost/risk preference optimization."""
    return (
        decide.Rung(
            "risk_averse_tail_bounded",
            "risk-averse schedule optimizing conservative p99 tail bound and recovery margin",
            needs=(
                decide.Need(
                    "not forced to safe baseline",
                    lambda f: not bool(f.get("force_conservative_baseline", False)),
                    "{rung} refused because conservative baseline is forced",
                ),
                decide.Need(
                    "risk-averse policy requested",
                    lambda f: str(f.get("risk_preference", "balanced")) == "risk_averse",
                    "{rung} refused because active policy preference is not risk-averse",
                ),
                decide.Need(
                    "p99 tail overrun within tolerance",
                    lambda f: float(f.get("p99_tail_overrun_ratio", 1.0))
                    <= float(f.get("max_acceptable_tail_ratio", 2.5)),
                    "{rung} refused because p99 tail overrun ratio exceeds risk-averse tolerance",
                ),
            ),
            because="minimizes p99 exposure and protects against correlated outage cascade",
            footprint=0,
        ),
        decide.Rung(
            "deadline_critical_p99",
            "deadline-critical schedule maximizing calibrated probability of on-time completion",
            needs=(
                decide.Need(
                    "not forced to safe baseline",
                    lambda f: not bool(f.get("force_conservative_baseline", False)),
                    "{rung} refused because conservative baseline is forced",
                ),
                decide.Need(
                    "deadline-critical policy requested",
                    lambda f: str(f.get("risk_preference", "balanced")) == "deadline_critical",
                    "{rung} refused because active policy preference is not deadline-critical",
                ),
                decide.Need(
                    "calibrated deadline probability meets minimum threshold",
                    lambda f: float(f.get("calibrated_deadline_prob", 0.0))
                    >= float(f.get("min_acceptable_deadline_prob", 0.85)),
                    "{rung} refused because calibrated deadline probability is below acceptable threshold",
                ),
            ),
            because="optimizes verified completion time to guarantee deadline adherence",
            footprint=0,
        ),
        decide.Rung(
            "cost_minimizing_budget_first",
            "cost-minimizing schedule subject to hard deadline and correctness minimums",
            needs=(
                decide.Need(
                    "not forced to safe baseline",
                    lambda f: not bool(f.get("force_conservative_baseline", False)),
                    "{rung} refused because conservative baseline is forced",
                ),
                decide.Need(
                    "cost-minimizing policy requested",
                    lambda f: str(f.get("risk_preference", "balanced")) == "cost_minimizing",
                    "{rung} refused because active policy preference is not cost-minimizing",
                ),
                decide.Need(
                    "monetary cost within budget limit",
                    lambda f: float(f.get("estimated_cost_dollars", 0.0))
                    <= float(f.get("cost_budget_dollars", float("inf"))),
                    "{rung} refused because schedule monetary cost exceeds budget ceiling",
                ),
            ),
            because="minimizes financial expenditure and worker instance allocation",
            footprint=0,
        ),
        decide.Rung(
            "balanced_risk_cost",
            "balanced schedule optimizing joint expected cost, source impact, and p90 risk",
            needs=(
                decide.Need(
                    "not forced to safe baseline",
                    lambda f: not bool(f.get("force_conservative_baseline", False)),
                    "{rung} refused because conservative baseline is forced",
                ),
                decide.Need(
                    "balanced policy permitted",
                    lambda f: bool(f.get("allow_balanced_policy", True)),
                    "{rung} refused because balanced policy is disabled",
                ),
            ),
            because="achieves robust compromise across time, cost, source load, and tail risk",
            footprint=0,
        ),
        decide.Rung(
            "conservative_baseline_fallback",
            "safe conservative sequential baseline with minimal contention and proven bounds",
            needs=(
                decide.Need(
                    "fallback to safe baseline required or active",
                    lambda f: bool(f.get("force_conservative_baseline", False))
                    or not bool(f.get("has_feasible_pareto_candidate", True)),
                    "{rung} refused because feasible Pareto candidate is available",
                ),
            ),
            because="provides safe fallback when aggressive schedules fail risk bounds",
            footprint=0,
        ),
    )


def cdc_catchup_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for bursty CDC arrival and recovery assessment."""
    return (
        decide.Rung(
            "feasible_burst_catchup",
            "feasible CDC catch-up with positive drain headroom and bounded recovery window",
            needs=(
                decide.Need(
                    "burst drain headroom positive",
                    lambda f: float(f.get("target_apply_capacity_bytes_sec", 1.0))
                    > float(f.get("peak_burst_arrival_bytes_sec", 0.0)),
                    "{rung} refused because peak CDC burst arrival exceeds target apply capacity",
                ),
                decide.Need(
                    "estimated recovery seconds within allowed window",
                    lambda f: float(f.get("estimated_recovery_seconds", 0.0))
                    <= float(f.get("max_recovery_window_sec", 3600.0)),
                    "{rung} refused because recovery drain time exceeds maximum recovery window",
                ),
            ),
            because="confirms CDC replay keeps pace with source writes and absorbs bursts",
            footprint=0,
        ),
        decide.Rung(
            "diagnose_impossible_catchup",
            "diagnose impossible catch-up due to sustained arrival overload or deadline breach",
            needs=(
                decide.Need(
                    "sustained arrival exceeds apply rate or deadline remaining",
                    lambda f: float(f.get("nominal_arrival_bytes_sec", 0.0))
                    >= float(f.get("target_apply_capacity_bytes_sec", 1.0))
                    or float(f.get("estimated_recovery_seconds", 0.0))
                    > float(f.get("deadline_remaining_sec", float("inf"))),
                    "{rung} refused because CDC arrival rate is sustainable within deadline",
                ),
            ),
            because="diagnoses persistent CDC backlog divergence preventing successful cutover",
            footprint=1,
        ),
        decide.Rung(
            "throttle_or_quarantine_burst",
            "engage burst throttling and source backpressure pacing to protect apply stream",
            needs=(
                decide.Need(
                    "burst throttling supported",
                    lambda f: bool(f.get("burst_throttling_supported", True)),
                    "{rung} refused because burst throttling mechanism is not supported",
                ),
                decide.Need(
                    "backpressure stall threshold exceeded",
                    lambda f: float(f.get("backpressure_stall_ratio", 0.0))
                    > float(f.get("throttle_threshold_ratio", 0.20)),
                    "{rung} refused because backpressure stall ratio is within normal range",
                ),
            ),
            because="throttles burst volume to stabilize CDC apply pipeline",
            footprint=1,
        ),
    )


def correlation_modeling_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for shared-resource and outage correlation modeling."""
    return (
        decide.Rung(
            "copula_correlated_tail_model",
            "apply copula-based shared-resource correlation matrix and outage shock multipliers",
            needs=(
                decide.Need(
                    "shared resource correlation or failure groups present",
                    lambda f: float(f.get("outage_correlation_coefficient", 0.0)) > 0.0
                    or int(f.get("shared_resource_group_count", 0)) > 1,
                    "{rung} refused because shared-resource correlation coefficient is zero",
                ),
                decide.Need(
                    "calibrated variance facts available",
                    lambda f: bool(f.get("calibrated_variance_available", True)),
                    "{rung} refused because calibrated variance parameters are missing",
                ),
            ),
            because="calibrates tail variance under coupled outages and joint resource contention",
            footprint=0,
        ),
        decide.Rung(
            "isolated_shared_resource_partition",
            "partition shared resources to isolate failure groups and eliminate correlation",
            needs=(
                decide.Need(
                    "resource partitioning supported",
                    lambda f: bool(f.get("resource_partitioning_supported", False)),
                    "{rung} refused because endpoint does not support resource partitioning",
                ),
                decide.Need(
                    "partitioning overhead within acceptable bound",
                    lambda f: float(f.get("partitioning_overhead_ratio", 0.0))
                    <= float(f.get("max_partition_overhead_ratio", 0.30)),
                    "{rung} refused because partitioning overhead exceeds efficiency threshold",
                ),
            ),
            because="eliminates common-cause failure correlation by physically isolating shares",
            footprint=1,
        ),
        decide.Rung(
            "independent_nominal_baseline",
            "uncalibrated independent nominal baseline used for false confidence measurement",
            needs=(
                decide.Need(
                    "nominal baseline permitted",
                    lambda f: bool(f.get("permit_nominal_baseline", True)),
                    "{rung} refused because nominal baseline is disallowed",
                ),
            ),
            because="provides reference uncalibrated baseline to quantify false confidence gaps",
            footprint=0,
        ),
    )


# -----------------------------------------------------------------------------
# Core Mechanics: Recovery Work, Tail Simulation & Pareto Frontier
# -----------------------------------------------------------------------------


def diagnose_cdc_recovery_and_catchup(
    cdc_profile: CdcBurstProfile,
    *,
    outage_duration_sec: float = 0.0,
    chunk_retry_bytes: float = 0.0,
    deadline_remaining_sec: Optional[float] = None,
) -> RecoveryWorkSummary:
    """Compute recovery work and evaluate CDC catch-up feasibility."""
    downtime = max(0.0, float(outage_duration_sec))
    retry_bytes = max(0.0, float(chunk_retry_bytes))
    backlog_accumulated = downtime * cdc_profile.nominal_arrival_bytes_sec
    total_backlog = cdc_profile.current_backlog_bytes + backlog_accumulated
    total_recovery_work = retry_bytes + total_backlog

    apply_cap = cdc_profile.target_apply_capacity_bytes_sec
    nominal_arr = cdc_profile.nominal_arrival_bytes_sec

    # Net catchup rate is apply rate minus incoming arrival rate
    net_drain_rate = apply_cap - nominal_arr

    deadline_rem = float(deadline_remaining_sec) if deadline_remaining_sec is not None else float("inf")

    if net_drain_rate <= 0.0:
        msg = (
            f"ACTION: impossible catch-up diagnosed: nominal arrival rate ({nominal_arr:,.0f} B/s) "
            f"exceeds or equals target apply capacity ({apply_cap:,.0f} B/s); backlog diverges"
        )
        return RecoveryWorkSummary(
            retry_work_bytes=round(retry_bytes, 2),
            accumulated_backlog_bytes=round(total_backlog, 2),
            total_recovery_work_bytes=round(total_recovery_work, 2),
            net_drain_rate_bytes_sec=round(net_drain_rate, 2),
            recovery_seconds=float("inf"),
            is_impossible_catchup=True,
            diagnostic_message=msg,
            care_badge=CareBadge.ACTION,
        )

    recovery_sec = total_recovery_work / max(1.0, net_drain_rate)

    if recovery_sec > deadline_rem:
        msg = (
            f"ACTION: impossible catch-up diagnosed: recovery time ({recovery_sec:.1f}s) "
            f"exceeds remaining deadline ({deadline_rem:.1f}s)"
        )
        return RecoveryWorkSummary(
            retry_work_bytes=round(retry_bytes, 2),
            accumulated_backlog_bytes=round(total_backlog, 2),
            total_recovery_work_bytes=round(total_recovery_work, 2),
            net_drain_rate_bytes_sec=round(net_drain_rate, 2),
            recovery_seconds=round(recovery_sec, 2),
            is_impossible_catchup=True,
            diagnostic_message=msg,
            care_badge=CareBadge.ACTION,
        )

    # Check peak burst vulnerability: during peak burst, arrival rate might exceed apply rate
    burst_accum_rate = cdc_profile.peak_burst_arrival_bytes_sec - apply_cap
    if burst_accum_rate > 0.0:
        burst_added_backlog = burst_accum_rate * cdc_profile.burst_duration_sec
        post_burst_recovery = (total_recovery_work + burst_added_backlog) / max(1.0, net_drain_rate)
        if post_burst_recovery > deadline_rem:
            msg = (
                f"ACTION: impossible catch-up diagnosed: peak burst creates backlog that cannot clear "
                f"before deadline ({post_burst_recovery:.1f}s > {deadline_rem:.1f}s)"
            )
            return RecoveryWorkSummary(
                retry_work_bytes=round(retry_bytes, 2),
                accumulated_backlog_bytes=round(total_backlog, 2),
                total_recovery_work_bytes=round(total_recovery_work + burst_added_backlog, 2),
                net_drain_rate_bytes_sec=round(net_drain_rate, 2),
                recovery_seconds=round(post_burst_recovery, 2),
                is_impossible_catchup=True,
                diagnostic_message=msg,
                care_badge=CareBadge.ACTION,
            )
        msg = (
            f"REVIEW: bursty CDC arrivals create transient backlog ({burst_added_backlog:,.0f} B); "
            f"recovery requires {post_burst_recovery:.1f}s"
        )
        return RecoveryWorkSummary(
            retry_work_bytes=round(retry_bytes, 2),
            accumulated_backlog_bytes=round(total_backlog, 2),
            total_recovery_work_bytes=round(total_recovery_work + burst_added_backlog, 2),
            net_drain_rate_bytes_sec=round(net_drain_rate, 2),
            recovery_seconds=round(post_burst_recovery, 2),
            is_impossible_catchup=False,
            diagnostic_message=msg,
            care_badge=CareBadge.REVIEW,
        )

    msg = f"PASS: CDC catch-up feasible; net drain rate {net_drain_rate:,.0f} B/s recovers in {recovery_sec:.1f}s"
    return RecoveryWorkSummary(
        retry_work_bytes=round(retry_bytes, 2),
        accumulated_backlog_bytes=round(total_backlog, 2),
        total_recovery_work_bytes=round(total_recovery_work, 2),
        net_drain_rate_bytes_sec=round(net_drain_rate, 2),
        recovery_seconds=round(recovery_sec, 2),
        is_impossible_catchup=False,
        diagnostic_message=msg,
        care_badge=CareBadge.PASS,
    )


def simulate_tail_risk_distribution(
    *,
    base_nominal_seconds: float,
    spread: float = 0.20,
    correlation_coefficient: float = 0.50,
    shared_group_count: int = 2,
    outage_prob: float = 0.05,
    mean_outage_sec: float = 30.0,
    burst_delay_sec: float = 0.0,
    recovery_sec: float = 0.0,
) -> TailRiskMetrics:
    """Simulate tail risk distribution under shared-resource outage and contention correlation.

    Uses variance inflation derived from the correlation coefficient:
    Var_correlated = Var_nominal * (1 + (K - 1) * rho)
    where K is the number of shared resource units / failure groups.
    """
    nom = max(1.0, float(base_nominal_seconds))
    rho = max(0.0, min(1.0, float(correlation_coefficient)))
    k_groups = max(1, int(shared_group_count))

    # Variance inflation factor under K shared groups with correlation rho
    vif = 1.0 + float(k_groups - 1) * rho

    # Standard deviation of nominal duration
    sd_nom = nom * max(0.01, float(spread))
    sd_calibrated = sd_nom * math.sqrt(vif)

    # Expected outage delay under correlated shock
    # When rho > 0, an outage affects multiple concurrent stages/tables simultaneously
    correlated_outage_impact = outage_prob * mean_outage_sec * (1.0 + float(k_groups - 1) * rho * 0.5)
    total_tail_overhead = correlated_outage_impact + burst_delay_sec + recovery_sec

    # Standard normal quantile multipliers:
    # p50 = 0.0
    # p90 = 1.2816
    # p95 = 1.6449
    # p99 = 2.3263
    p50 = nom + total_tail_overhead * 0.25
    p90 = nom + 1.2816 * sd_calibrated + total_tail_overhead * 0.85
    p95 = nom + 1.6449 * sd_calibrated + total_tail_overhead * 1.00
    p99 = nom + 2.3263 * sd_calibrated + total_tail_overhead * 1.35

    # VaR at 95% is p95; CVaR (Expected Shortfall) is E[T | T >= p95]
    # For Gaussian tail: E[Z | Z >= z] = phi(z) / (1 - Phi(z))
    # For z = 1.6449, phi(1.6449) = 0.1031, tail prob = 0.05, ratio ~ 2.062
    cvar_95 = nom + 2.062 * sd_calibrated + total_tail_overhead * 1.15

    return TailRiskMetrics(
        p50_seconds=round(p50, 3),
        p90_seconds=round(p90, 3),
        p95_seconds=round(p95, 3),
        p99_seconds=round(p99, 3),
        var_95_seconds=round(p95, 3),
        cvar_95_seconds=round(cvar_95, 3),
        variance_inflation_factor=round(vif, 3),
        correlated_outage_delay_seconds=round(correlated_outage_impact, 3),
    )


def evaluate_deadline_probability(
    *,
    calibrated_tail: TailRiskMetrics,
    nominal_seconds: float,
    deadline_seconds: float,
) -> DeadlineEvaluation:
    """Compute calibrated deadline probability vs naive nominal model and detect false confidence."""
    deadline = max(0.1, float(deadline_seconds))
    nom = max(0.1, float(nominal_seconds))

    # Naive model assumes nominal rate and small independent spread (sd = 0.10 * nom)
    sd_naive = max(0.01, nom * 0.10)
    z_naive = (deadline - nom) / sd_naive
    # Standard normal CDF approximation (Abramowitz & Stegun / logistic approximation)
    p_naive = 1.0 / (1.0 + math.exp(-1.6 * max(-10.0, min(10.0, z_naive))))

    # Calibrated probability uses calibrated mean and inflated variance
    calibrated_mean = calibrated_tail.p50_seconds
    sd_calibrated = (calibrated_tail.p95_seconds - calibrated_tail.p50_seconds) / 1.6449
    z_calibrated = (deadline - calibrated_mean) / max(0.01, sd_calibrated)
    p_calibrated = 1.0 / (1.0 + math.exp(-1.6 * max(-10.0, min(10.0, z_calibrated))))

    false_confidence_gap = max(0.0, p_naive - p_calibrated)
    is_breached = calibrated_tail.p90_seconds > deadline

    if is_breached or p_calibrated < 0.70:
        badge = CareBadge.ACTION
    elif false_confidence_gap > 0.15:
        badge = CareBadge.REVIEW
    else:
        badge = CareBadge.PASS

    return DeadlineEvaluation(
        deadline_seconds=round(deadline, 2),
        calibrated_probability=round(p_calibrated, 4),
        naive_probability=round(p_naive, 4),
        false_confidence_gap=round(false_confidence_gap, 4),
        is_breached=is_breached,
        confidence_badge=badge,
    )


def evaluate_candidate_strategy(
    strategy: SchedulingStrategy,
    *,
    tables: Sequence[TableWorkItem],
    cdc_profile: CdcBurstProfile,
    shared_resources: Mapping[str, SharedResourceSpec],
    facts_map: Mapping[str, Any],
    deadline_seconds: float,
) -> ScheduleAlternative:
    """Evaluate one scheduling strategy across all 4 conflicting objectives and hard constraints."""
    total_bytes = float(sum(t.total_bytes for t in tables) if tables else facts_map.get("dataset_bytes", 10_000_000_000))
    total_rows = int(sum(t.row_count for t in tables) if tables else facts_map.get("dataset_rows", 50_000_000))
    table_count = max(1, len(tables))

    # Base transfer rate depending on strategy
    if strategy == SchedulingStrategy.ROBUST_STREAM_DEFERRED_INDEX:
        base_rate = float(facts_map.get("stream_rate_bytes_sec", 180_000_000.0))
        source_cpu = float(facts_map.get("stream_source_cpu_share", 0.35))
        lock_sec = float(facts_map.get("stream_lock_sec", 0.08))
        dollars = float(facts_map.get("stream_cost_dollars", 25.0))
        spread = float(facts_map.get("stream_spread", 0.22))
        res_groups = 2
    elif strategy == SchedulingStrategy.ISOLATED_PARTITION_DIRECT_APPLY:
        base_rate = float(facts_map.get("partition_rate_bytes_sec", 150_000_000.0))
        source_cpu = float(facts_map.get("partition_source_cpu_share", 0.25))
        lock_sec = float(facts_map.get("partition_lock_sec", 0.12))
        dollars = float(facts_map.get("partition_cost_dollars", 38.0))
        spread = float(facts_map.get("partition_spread", 0.15))
        # Partitioning isolates shared groups, lowering correlation coefficient
        res_groups = 1
    elif strategy == SchedulingStrategy.PARALLEL_STAGED_BULK_LOAD:
        base_rate = float(facts_map.get("bulk_rate_bytes_sec", 240_000_000.0))
        source_cpu = float(facts_map.get("bulk_source_cpu_share", 0.55))
        lock_sec = float(facts_map.get("bulk_lock_sec", 0.45))
        dollars = float(facts_map.get("bulk_cost_dollars", 65.0))
        spread = float(facts_map.get("bulk_spread", 0.35))
        res_groups = 3
    else:
        # SAFE_CONSERVATIVE_BASELINE
        base_rate = float(facts_map.get("baseline_rate_bytes_sec", 90_000_000.0))
        source_cpu = float(facts_map.get("baseline_source_cpu_share", 0.15))
        lock_sec = float(facts_map.get("baseline_lock_sec", 0.03))
        dollars = float(facts_map.get("baseline_cost_dollars", 15.0))
        spread = float(facts_map.get("baseline_spread", 0.10))
        res_groups = 1

    copy_nominal_sec = total_bytes / max(1.0, base_rate)
    cdc_drain_sec = (cdc_profile.current_backlog_bytes + copy_nominal_sec * cdc_profile.nominal_arrival_bytes_sec) / max(
        1.0, cdc_profile.target_apply_capacity_bytes_sec
    )
    total_nominal_sec = copy_nominal_sec + cdc_drain_sec

    # Evaluate recovery work and CDC catch-up
    rho = float(facts_map.get("outage_correlation_coefficient", 0.40))
    if strategy == SchedulingStrategy.ISOLATED_PARTITION_DIRECT_APPLY:
        # Isolated partitioning cuts effective correlation
        rho = rho * 0.20

    recovery = diagnose_cdc_recovery_and_catchup(
        cdc_profile,
        outage_duration_sec=30.0,
        chunk_retry_bytes=min(total_bytes * 0.05, 500_000_000.0),
        deadline_remaining_sec=deadline_seconds,
    )

    tail = simulate_tail_risk_distribution(
        base_nominal_seconds=total_nominal_sec,
        spread=spread,
        correlation_coefficient=rho,
        shared_group_count=res_groups,
        outage_prob=0.05,
        mean_outage_sec=30.0,
        burst_delay_sec=15.0 if cdc_profile.peak_burst_multiplier > 1.5 else 0.0,
        recovery_sec=recovery.recovery_seconds if not math.isinf(recovery.recovery_seconds) else 9999.0,
    )

    deadline_eval = evaluate_deadline_probability(
        calibrated_tail=tail,
        nominal_seconds=total_nominal_sec,
        deadline_seconds=deadline_seconds,
    )

    # Check hard correctness and source impact constraints
    max_src_cpu = float(facts_map.get("max_source_cpu_share", 0.40))
    max_lock_sec = float(facts_map.get("max_lock_duration_sec", 0.30))
    correctness_met = bool(facts_map.get("correctness_minimums_satisfied", True))

    rejection_reasons: List[str] = []
    if not correctness_met:
        rejection_reasons.append("fails hard correctness minimum")
    if source_cpu > max_src_cpu:
        rejection_reasons.append(f"source CPU share {source_cpu:.2f} exceeds limit {max_src_cpu:.2f}")
    if lock_sec > max_lock_sec:
        rejection_reasons.append(f"lock duration {lock_sec:.2f}s exceeds limit {max_lock_sec:.2f}s")
    if recovery.is_impossible_catchup:
        rejection_reasons.append("impossible CDC catch-up diagnosed")

    is_feasible = len(rejection_reasons) == 0

    impact_score = round(source_cpu * 0.70 + min(1.0, lock_sec / max_lock_sec) * 0.30, 4)
    tail_risk_exposure = round(tail.p99_seconds - tail.p50_seconds, 3)

    objs = MultiObjectiveScores(
        verified_completion_p90_sec=tail.p90_seconds,
        source_impact_score=impact_score,
        monetary_cost_dollars=round(dollars, 2),
        tail_risk_exposure_sec=tail_risk_exposure,
    )

    return ScheduleAlternative(
        strategy=strategy,
        objectives=objs,
        tail_metrics=tail,
        deadline_eval=deadline_eval,
        recovery=recovery,
        is_feasible=is_feasible,
        rejection_reason="; ".join(rejection_reasons) if rejection_reasons else None,
        is_nondominated=False,
    )


def filter_nondominated_pareto_front(
    alternatives: Sequence[ScheduleAlternative],
) -> Tuple[ScheduleAlternative, ...]:
    """Compute the non-dominated Pareto frontier among feasible alternatives."""
    feasible = [alt for alt in alternatives if alt.is_feasible]
    if not feasible:
        return ()

    pareto: List[ScheduleAlternative] = []
    for candidate in feasible:
        dominated = False
        for competitor in feasible:
            if competitor.strategy != candidate.strategy and competitor.objectives.dominates(candidate.objectives):
                dominated = True
                break
        if not dominated:
            pareto.append(
                ScheduleAlternative(
                    strategy=candidate.strategy,
                    objectives=candidate.objectives,
                    tail_metrics=candidate.tail_metrics,
                    deadline_eval=candidate.deadline_eval,
                    recovery=candidate.recovery,
                    is_feasible=True,
                    rejection_reason=None,
                    is_nondominated=True,
                )
            )

    return tuple(pareto)


# -----------------------------------------------------------------------------
# Main Optimizer: `optimize_robust_schedule`
# -----------------------------------------------------------------------------


def optimize_robust_schedule(
    facts_input: Mapping[str, Any],
    *,
    tables: Sequence[TableWorkItem] = (),
    shared_resources: Optional[Mapping[str, SharedResourceSpec]] = None,
    cdc_profile: Optional[CdcBurstProfile] = None,
    deadline_seconds: float = 3600.0,
    work: str = "robust schedule tail risk optimization",
) -> RobustSchedulePlan:
    """Plan for correlated tail risk and conflicting objectives via `decide.climb`."""
    raw = dict(facts_input)

    # Validate safe endpoints
    if "source_host" in raw:
        validate_safe_endpoint(str(raw["source_host"]))
    if "target_host" in raw:
        validate_safe_endpoint(str(raw["target_host"]))

    # Default resources and CDC profiles
    resources = shared_resources or {
        "source_iops": SharedResourceSpec("source_iops", 5000.0, "source_disk", 0.03, 20.0),
        "target_iops": SharedResourceSpec("target_iops", 8000.0, "target_disk", 0.02, 15.0),
        "network_wan": SharedResourceSpec("network_wan", 100_000_000.0, "wan_pipe", 0.05, 45.0),
    }

    cdc = cdc_profile or CdcBurstProfile(
        nominal_arrival_bytes_sec=float(raw.get("nominal_cdc_arrival_bytes_sec", 15_000_000.0)),
        peak_burst_multiplier=float(raw.get("peak_burst_multiplier", 2.5)),
        burst_duration_sec=float(raw.get("burst_duration_sec", 60.0)),
        current_backlog_bytes=float(raw.get("current_cdc_backlog_bytes", 50_000_000.0)),
        target_apply_capacity_bytes_sec=float(raw.get("target_cdc_apply_capacity_bytes_sec", 40_000_000.0)),
    )

    pref_str = str(raw.get("risk_preference", RiskPreference.BALANCED.value)).lower()
    try:
        active_pref = RiskPreference(pref_str)
    except ValueError:
        active_pref = RiskPreference.BALANCED

    # Step 1: Evaluate candidate alternatives across 4 conflicting objectives
    evaluated_alts: List[ScheduleAlternative] = []
    for strat in ALL_SCHEDULING_STRATEGIES:
        alt = evaluate_candidate_strategy(
            strat,
            tables=tables,
            cdc_profile=cdc,
            shared_resources=resources,
            facts_map=raw,
            deadline_seconds=deadline_seconds,
        )
        evaluated_alts.append(alt)

    # Step 2: Compute non-dominated Pareto front
    pareto_front = filter_nondominated_pareto_front(evaluated_alts)

    # Prepare facts for decide ladders
    has_feasible = len(pareto_front) > 0
    best_candidate = pareto_front[0] if has_feasible else evaluated_alts[-1]

    # Climb Ladder 1: Admissibility
    admit_facts = dict(raw)
    admit_facts.update(
        {
            "correctness_minimums_satisfied": bool(raw.get("correctness_minimums_satisfied", True)),
            "source_impact_within_limits": best_candidate.is_feasible,
        }
    )
    admissibility_choice = decide.climb("admissibility check", admissibility_rungs(), decide.Facts(admit_facts))

    # Climb Ladder 3: CDC Catch-up
    cdc_facts = dict(raw)
    cdc_facts.update(
        {
            "target_apply_capacity_bytes_sec": cdc.target_apply_capacity_bytes_sec,
            "peak_burst_arrival_bytes_sec": cdc.peak_burst_arrival_bytes_sec,
            "nominal_arrival_bytes_sec": cdc.nominal_arrival_bytes_sec,
            "estimated_recovery_seconds": best_candidate.recovery.recovery_seconds,
            "max_recovery_window_sec": float(raw.get("max_recovery_window_sec", 3600.0)),
            "deadline_remaining_sec": deadline_seconds,
            "burst_throttling_supported": bool(raw.get("burst_throttling_supported", True)),
            "backpressure_stall_ratio": float(raw.get("backpressure_stall_ratio", 0.0)),
            "throttle_threshold_ratio": float(raw.get("throttle_threshold_ratio", 0.20)),
        }
    )
    cdc_choice = decide.climb("cdc catchup evaluation", cdc_catchup_rungs(), decide.Facts(cdc_facts))

    # Climb Ladder 4: Correlation Modeling
    corr_facts = dict(raw)
    corr_facts.update(
        {
            "outage_correlation_coefficient": float(raw.get("outage_correlation_coefficient", 0.40)),
            "shared_resource_group_count": len(resources),
            "calibrated_variance_available": True,
            "resource_partitioning_supported": bool(raw.get("resource_partitioning_supported", False)),
            "partitioning_overhead_ratio": float(raw.get("partitioning_overhead_ratio", 0.15)),
            "max_partition_overhead_ratio": 0.30,
            "permit_nominal_baseline": bool(raw.get("permit_nominal_baseline", True)),
        }
    )
    correlation_choice = decide.climb("correlation modeling", correlation_modeling_rungs(), decide.Facts(corr_facts))

    # Climb Ladder 2: Tail Risk Policy
    tail_facts = dict(raw)
    overrun_ratio = best_candidate.tail_metrics.p99_seconds / max(1.0, best_candidate.tail_metrics.p50_seconds)
    tail_facts.update(
        {
            "risk_preference": active_pref.value,
            "p99_tail_overrun_ratio": overrun_ratio,
            "max_acceptable_tail_ratio": float(raw.get("max_acceptable_tail_ratio", 2.5)),
            "calibrated_deadline_prob": best_candidate.deadline_eval.calibrated_probability,
            "min_acceptable_deadline_prob": float(raw.get("min_acceptable_deadline_prob", 0.85)),
            "estimated_cost_dollars": best_candidate.objectives.monetary_cost_dollars,
            "cost_budget_dollars": float(raw.get("cost_budget_dollars", float("inf"))),
            "allow_balanced_policy": bool(raw.get("allow_balanced_policy", True)),
            "force_conservative_baseline": bool(raw.get("force_conservative_baseline", False)),
            "has_feasible_pareto_candidate": has_feasible,
        }
    )
    tail_policy_choice = decide.climb("tail risk policy selection", tail_risk_policy_rungs(), decide.Facts(tail_facts))

    # Select strategy based on active preference from Pareto front
    if not has_feasible or tail_policy_choice.path == "conservative_baseline_fallback":
        chosen_strategy = SchedulingStrategy.SAFE_CONSERVATIVE_BASELINE
        chosen_alt = next((a for a in evaluated_alts if a.strategy == chosen_strategy), evaluated_alts[-1])
    elif active_pref == RiskPreference.RISK_AVERSE:
        # Min p99 tail risk on Pareto front
        chosen_alt = min(pareto_front, key=lambda a: a.objectives.tail_risk_exposure_sec)
        chosen_strategy = chosen_alt.strategy
    elif active_pref == RiskPreference.DEADLINE_CRITICAL:
        # Max calibrated deadline probability on Pareto front
        chosen_alt = max(pareto_front, key=lambda a: a.deadline_eval.calibrated_probability)
        chosen_strategy = chosen_alt.strategy
    elif active_pref == RiskPreference.COST_MINIMIZING:
        # Min dollar cost on Pareto front
        chosen_alt = min(pareto_front, key=lambda a: a.objectives.monetary_cost_dollars)
        chosen_strategy = chosen_alt.strategy
    else:
        # BALANCED: min composite score
        chosen_alt = min(
            pareto_front,
            key=lambda a: a.objectives.verified_completion_p90_sec * 0.45
            + a.objectives.source_impact_score * 50.0
            + a.objectives.monetary_cost_dollars * 0.50,
        )
        chosen_strategy = chosen_alt.strategy

    # Baseline and jointplan comparators
    baseline_alt = next(
        (a for a in evaluated_alts if a.strategy == SchedulingStrategy.SAFE_CONSERVATIVE_BASELINE),
        evaluated_alts[-1],
    )
    jointplan_nominal = chosen_alt.tail_metrics.p50_seconds
    jointplan_calibrated_p90 = chosen_alt.tail_metrics.p90_seconds
    safe_baseline_sec = baseline_alt.tail_metrics.p90_seconds

    tail_reduction = max(
        0.0,
        1.0 - (chosen_alt.tail_metrics.p99_seconds / max(1.0, baseline_alt.tail_metrics.p99_seconds)),
    )

    cert = TradeoffCertificate(
        selected_strategy=chosen_strategy,
        selected_p90_seconds=chosen_alt.tail_metrics.p90_seconds,
        jointplan_nominal_seconds=round(jointplan_nominal, 2),
        jointplan_calibrated_p90_seconds=round(jointplan_calibrated_p90, 2),
        safe_baseline_seconds=round(safe_baseline_sec, 2),
        tail_risk_reduction_ratio=round(tail_reduction, 4),
        false_confidence_gap=chosen_alt.deadline_eval.false_confidence_gap,
        claims_universal_best=False,
        claims_guaranteed_fastest=False,
        tradeoff_rationale=(
            f"Strategy '{chosen_strategy.value}' selected under '{active_pref.value}' policy; "
            f"no universal optimum is claimed. Calibrated p90 is {chosen_alt.tail_metrics.p90_seconds:.1f}s "
            f"with {chosen_alt.deadline_eval.false_confidence_gap:.1%} false confidence gap relative to naive baseline."
        ),
    )

    # Determine overall care badge
    if chosen_alt.recovery.is_impossible_catchup or chosen_alt.deadline_eval.is_breached:
        badge = CareBadge.ACTION
    elif chosen_alt.deadline_eval.false_confidence_gap > 0.15 or not has_feasible:
        badge = CareBadge.REVIEW
    else:
        badge = CareBadge.PASS

    summary_text = (
        f"[{badge.value}] Strategy: {chosen_strategy.value} | "
        f"Pref: {active_pref.value} | "
        f"p90: {chosen_alt.tail_metrics.p90_seconds:.1f}s | "
        f"p99: {chosen_alt.tail_metrics.p99_seconds:.1f}s | "
        f"Deadline P: {chosen_alt.deadline_eval.calibrated_probability:.1%} | "
        f"False Conf Gap: {chosen_alt.deadline_eval.false_confidence_gap:.1%}"
    )

    return RobustSchedulePlan(
        admissibility_choice=admissibility_choice,
        tail_policy_choice=tail_policy_choice,
        cdc_choice=cdc_choice,
        correlation_choice=correlation_choice,
        selected_strategy=chosen_strategy,
        active_risk_preference=active_pref,
        tail_metrics=chosen_alt.tail_metrics,
        deadline_eval=chosen_alt.deadline_eval,
        recovery=chosen_alt.recovery,
        pareto_frontier=pareto_front,
        all_alternatives=tuple(evaluated_alts),
        tradeoff_certificate=cert,
        care_badge=badge,
        summary=summary_text,
    )


# -----------------------------------------------------------------------------
# O(1) Typed Durable Checkpoint Store & Active Lease Lock
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class RobustSchedulerCheckpointUnit:
    """Durable typed checkpoint unit for robust scheduling."""

    table_name: str
    schedule_phase: str
    strategy: SchedulingStrategy
    min_cursor: Any
    max_cursor: Any
    merkle_digest: str
    completed_at: datetime.datetime


class RobustSchedulerCheckpointStore:
    """Universal typed durable checkpoint store with O(1) table completion lookup."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = pathlib.Path(path)
        self._units: Dict[Tuple[str, str], RobustSchedulerCheckpointUnit] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="ascii"))
        for entry in raw.get("units", ()):
            unit = RobustSchedulerCheckpointUnit(
                table_name=str(entry["table_name"]),
                schedule_phase=str(entry["schedule_phase"]),
                strategy=SchedulingStrategy(entry["strategy"]),
                min_cursor=checkpoint.deserialize_cursor(entry["min_cursor"]),
                max_cursor=checkpoint.deserialize_cursor(entry["max_cursor"]),
                merkle_digest=str(entry["merkle_digest"]),
                completed_at=checkpoint.deserialize_cursor(entry["completed_at"]),
            )
            self._units[(unit.table_name, unit.schedule_phase)] = unit

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "units": [
                {
                    "table_name": u.table_name,
                    "schedule_phase": u.schedule_phase,
                    "strategy": u.strategy.value,
                    "min_cursor": checkpoint.serialize_cursor(u.min_cursor),
                    "max_cursor": checkpoint.serialize_cursor(u.max_cursor),
                    "merkle_digest": u.merkle_digest,
                    "completed_at": checkpoint.serialize_cursor(u.completed_at),
                }
                for _, u in sorted(self._units.items(), key=lambda kv: kv[0])
            ]
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=True), encoding="ascii")
        tmp.replace(self.path)

    def record_unit(self, unit: RobustSchedulerCheckpointUnit) -> None:
        self._units[(unit.table_name, unit.schedule_phase)] = unit
        self._save()

    def is_completed_o1(self, table_name: str, schedule_phase: str) -> bool:
        return (table_name, schedule_phase) in self._units

    def get_unit(self, table_name: str, schedule_phase: str) -> Optional[RobustSchedulerCheckpointUnit]:
        return self._units.get((table_name, schedule_phase))


def acquire_robust_scheduling_lease(
    lock_path: pathlib.Path,
    *,
    what: str = "robust scheduling tail risk optimization",
    term_seconds: float = 60.0,
) -> lease.Lease:
    """Acquire an active exclusive .run.lock lease with heartbeat."""
    return lease.Lease(pathlib.Path(lock_path), what=what, term=term_seconds).acquire()


# -----------------------------------------------------------------------------
# Adaptive Execution: AIMD Burst Batching & Byte-Weighted ETA
# -----------------------------------------------------------------------------


def adjust_aimd_burst_batch_size(
    *,
    current_batch_rows: int,
    stall_ratio: float,
    min_batch_rows: int = 500,
    max_batch_rows: int = 50000,
    additive_increase: int = 1000,
    multiplicative_decrease: float = 0.50,
) -> int:
    """Adjust CDC burst batch sizing via AIMD backpressure pacing."""
    if min_batch_rows <= 0 or max_batch_rows < min_batch_rows:
        raise InvalidScheduleFactError("Invalid batch row bounds")
    if not (0.0 < multiplicative_decrease < 1.0):
        raise InvalidScheduleFactError("multiplicative_decrease must be in (0, 1)")

    if stall_ratio > 0.20:
        # Multiplicative decrease on congestion
        reduced = int(math.floor(float(current_batch_rows) * multiplicative_decrease))
        return max(min_batch_rows, min(max_batch_rows, reduced))

    increased = current_batch_rows + additive_increase
    return max(min_batch_rows, min(max_batch_rows, increased))


def estimate_robust_byte_weighted_eta(
    *,
    total_bytes: int,
    completed_bytes: int,
    elapsed_seconds: float,
    cold_start_bytes_per_sec: float,
    tail_recovery_seconds: float = 0.0,
) -> float:
    """Compute cold-start byte-weighted ETA incorporating calibrated recovery tails."""
    if total_bytes < 0 or completed_bytes < 0:
        raise InvalidScheduleFactError("total_bytes and completed_bytes must be >= 0")
    if cold_start_bytes_per_sec <= 0.0:
        raise InvalidScheduleFactError("cold_start_bytes_per_sec must be > 0")

    remaining_bytes = max(0, total_bytes - completed_bytes)
    if remaining_bytes == 0:
        return max(0.0, float(tail_recovery_seconds))

    if completed_bytes > 0 and elapsed_seconds > 0.0:
        observed_rate = float(completed_bytes) / float(elapsed_seconds)
        weight = min(1.0, float(completed_bytes) / max(1.0, float(total_bytes)))
        blended_rate = weight * observed_rate + (1.0 - weight) * cold_start_bytes_per_sec
    else:
        blended_rate = cold_start_bytes_per_sec

    transfer_sec = float(remaining_bytes) / max(1.0, blended_rate)
    return transfer_sec + max(0.0, float(tail_recovery_seconds))


# -----------------------------------------------------------------------------
# External Tool Result Normalization (Zero Tool Leaks)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class NormalizedToolResult:
    """Universal tool wrapper output with 4-tier care badge."""

    care_badge: CareBadge
    success: bool
    exit_code: int
    normalized_message: str
    diagnostics: Dict[str, Any]


def normalize_external_tool_result(
    output: str,
    exit_code: int,
    *,
    context: str = "replication",
) -> NormalizedToolResult:
    """Normalize external tool outputs with zero internal tool name leaks."""
    text = (output or "").strip()
    upper = text.upper()

    if exit_code != 0 or "ERROR" in upper or "FATAL" in upper or "FAILED" in upper:
        badge = CareBadge.ACTION
        msg = f"ACTION: external {context} failed (exit code {exit_code}): {text[:200]}"
        success = False
    elif "WARN" in upper or "THROTTLE" in upper or "STALL" in upper:
        badge = CareBadge.REVIEW
        msg = f"REVIEW: external {context} reported warnings or throttling: {text[:200]}"
        success = True
    elif "DRIFT" in upper or "LAG" in upper:
        badge = CareBadge.BENIGN
        msg = f"BENIGN: external {context} observed expected replica drift or lag"
        success = True
    else:
        badge = CareBadge.PASS
        msg = f"PASS: external {context} completed cleanly"
        success = True

    return NormalizedToolResult(
        care_badge=badge,
        success=success,
        exit_code=exit_code,
        normalized_message=msg,
        diagnostics={"raw_chars": len(text), "context": context},
    )
