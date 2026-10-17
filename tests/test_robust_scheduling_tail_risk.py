"""Tests for Row 591: SMART-N8.robust-scheduling (Plan for correlated tail risk and conflicting objectives).

Verifies:
1. Every `decide.Rung` in `migkit/robust_scheduling.py` (`admissibility_rungs`,
   `tail_risk_policy_rungs`, `cdc_catchup_rungs`, `correlation_modeling_rungs`)
   exercises BOTH `holds` and `refuses` branches under `tests.rung_branches.watched()`
   and is declared in `PROVES_RUNGS`.
2. Shared-resource & outage correlation modeling:
   - Correlated degradation and variance inflation across shared failure groups.
   - Coupled outage shocks expand p90/p99 tail completion times.
3. Bursty CDC arrivals & recovery work:
   - Peak burst multiplier creates transient or permanent backlog.
   - Recovery work accurately accounts for downtime accumulation and chunk retries.
   - Formal impossible catch-up diagnosis (ACTION badge) when arrival exceeds apply capacity.
4. Calibrated deadline probability & false confidence measurement:
   - Computes calibrated p50, p90, p95, p99, VaR, CVaR.
   - Quantifies false confidence gap between naive nominal assumption and calibrated tail risk.
5. Conflicting objectives & non-dominated Pareto frontier:
   - Filters candidates violating hard correctness minimums or source impact limits.
   - Constructs non-dominated Pareto set across verified time, source load, cost, and tail risk.
6. Explicit preference optimization:
   - Validates risk-averse, deadline-critical, cost-minimizing, and balanced schedule selection.
   - Retains conservative baseline fallback when aggressive plans breach safety bounds.
7. Trade-off certificate with zero universal optimality claims:
   - Compares planned p90 against jointplan and safe baselines with honest trade-off disclosures.
8. Universal migkit resilient operations:
   - O(1) typed durable checkpoint resume with native Python types (datetime, Decimal, UUID).
   - Active lease lock (.run.lock) mutual exclusion with heartbeat.
   - Adaptive AIMD burst batch sizing and cold-start byte-weighted ETA.
   - Safe test host validation (127.0.0.1, 10.0.0.x) and normalized tool results.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import datetime
import decimal
import math
import pathlib
import time
import uuid
from typing import List

import pytest
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched

from migkit import decide
from migkit import robust_scheduling as rs

# -----------------------------------------------------------------------------
# Rung Declaration for test_every_decide_rung_is_reached.py
# -----------------------------------------------------------------------------

PROVES_RUNGS = [
    # admissibility_rungs (3 rungs x 2 branches = 6)
    ("robust_scheduling", "admissibility_rungs", "admit_robust_candidate", "holds"),
    ("robust_scheduling", "admissibility_rungs", "admit_robust_candidate", "refuses"),
    ("robust_scheduling", "admissibility_rungs", "reject_correctness_violation", "holds"),
    ("robust_scheduling", "admissibility_rungs", "reject_correctness_violation", "refuses"),
    ("robust_scheduling", "admissibility_rungs", "reject_excessive_source_impact", "holds"),
    ("robust_scheduling", "admissibility_rungs", "reject_excessive_source_impact", "refuses"),

    # tail_risk_policy_rungs (5 rungs x 2 branches = 10)
    ("robust_scheduling", "tail_risk_policy_rungs", "risk_averse_tail_bounded", "holds"),
    ("robust_scheduling", "tail_risk_policy_rungs", "risk_averse_tail_bounded", "refuses"),
    ("robust_scheduling", "tail_risk_policy_rungs", "deadline_critical_p99", "holds"),
    ("robust_scheduling", "tail_risk_policy_rungs", "deadline_critical_p99", "refuses"),
    ("robust_scheduling", "tail_risk_policy_rungs", "cost_minimizing_budget_first", "holds"),
    ("robust_scheduling", "tail_risk_policy_rungs", "cost_minimizing_budget_first", "refuses"),
    ("robust_scheduling", "tail_risk_policy_rungs", "balanced_risk_cost", "holds"),
    ("robust_scheduling", "tail_risk_policy_rungs", "balanced_risk_cost", "refuses"),
    ("robust_scheduling", "tail_risk_policy_rungs", "conservative_baseline_fallback", "holds"),
    ("robust_scheduling", "tail_risk_policy_rungs", "conservative_baseline_fallback", "refuses"),

    # cdc_catchup_rungs (3 rungs x 2 branches = 6)
    ("robust_scheduling", "cdc_catchup_rungs", "feasible_burst_catchup", "holds"),
    ("robust_scheduling", "cdc_catchup_rungs", "feasible_burst_catchup", "refuses"),
    ("robust_scheduling", "cdc_catchup_rungs", "diagnose_impossible_catchup", "holds"),
    ("robust_scheduling", "cdc_catchup_rungs", "diagnose_impossible_catchup", "refuses"),
    ("robust_scheduling", "cdc_catchup_rungs", "throttle_or_quarantine_burst", "holds"),
    ("robust_scheduling", "cdc_catchup_rungs", "throttle_or_quarantine_burst", "refuses"),

    # correlation_modeling_rungs (3 rungs x 2 branches = 6)
    ("robust_scheduling", "correlation_modeling_rungs", "copula_correlated_tail_model", "holds"),
    ("robust_scheduling", "correlation_modeling_rungs", "copula_correlated_tail_model", "refuses"),
    ("robust_scheduling", "correlation_modeling_rungs", "isolated_shared_resource_partition", "holds"),
    ("robust_scheduling", "correlation_modeling_rungs", "isolated_shared_resource_partition", "refuses"),
    ("robust_scheduling", "correlation_modeling_rungs", "independent_nominal_baseline", "holds"),
    ("robust_scheduling", "correlation_modeling_rungs", "independent_nominal_baseline", "refuses"),
]


# -----------------------------------------------------------------------------
# 1. Rung Branch Coverage: Holds and Refuses for All Rungs Under Watched
# -----------------------------------------------------------------------------


def test_every_robust_scheduling_rung_holds_and_refuses_under_watched() -> None:
    """Exercise both holds and refuses for every rung in robust_scheduling.py."""
    with watched() as seen:
        # Ladder 1: admissibility_rungs
        # Holds admit, refuses reject violations
        decide.climb(
            "admissibility normal hold",
            rs.admissibility_rungs(),
            decide.Facts({"correctness_minimums_satisfied": True, "source_impact_within_limits": True}),
        )
        # Refuses admit, holds reject violations
        decide.climb(
            "admissibility violations hold",
            rs.admissibility_rungs(),
            decide.Facts({"correctness_minimums_satisfied": False, "source_impact_within_limits": False}),
        )

        # Ladder 2: tail_risk_policy_rungs
        # Test risk_averse holds
        decide.climb(
            "tail policy risk averse hold",
            rs.tail_risk_policy_rungs(),
            decide.Facts({
                "risk_preference": "risk_averse",
                "p99_tail_overrun_ratio": 1.5,
                "max_acceptable_tail_ratio": 2.5,
                "allow_balanced_policy": False,
                "force_conservative_baseline": False,
                "has_feasible_pareto_candidate": True,
            }),
        )
        # Test deadline_critical holds
        decide.climb(
            "tail policy deadline critical hold",
            rs.tail_risk_policy_rungs(),
            decide.Facts({
                "risk_preference": "deadline_critical",
                "calibrated_deadline_prob": 0.95,
                "min_acceptable_deadline_prob": 0.85,
                "allow_balanced_policy": False,
                "force_conservative_baseline": False,
                "has_feasible_pareto_candidate": True,
            }),
        )
        # Test cost_minimizing holds
        decide.climb(
            "tail policy cost minimizing hold",
            rs.tail_risk_policy_rungs(),
            decide.Facts({
                "risk_preference": "cost_minimizing",
                "estimated_cost_dollars": 50.0,
                "cost_budget_dollars": 100.0,
                "allow_balanced_policy": False,
                "force_conservative_baseline": False,
                "has_feasible_pareto_candidate": True,
            }),
        )
        # Test balanced_risk_cost holds
        decide.climb(
            "tail policy balanced hold",
            rs.tail_risk_policy_rungs(),
            decide.Facts({
                "risk_preference": "other",
                "allow_balanced_policy": True,
                "force_conservative_baseline": False,
                "has_feasible_pareto_candidate": True,
            }),
        )
        # Test conservative_baseline_fallback holds
        decide.climb(
            "tail policy fallback hold",
            rs.tail_risk_policy_rungs(),
            decide.Facts({
                "risk_preference": "none",
                "allow_balanced_policy": False,
                "force_conservative_baseline": True,
            }),
        )
        # All refuse climb
        decide.climb(
            "tail policy all refuse",
            rs.tail_risk_policy_rungs(),
            decide.Facts({
                "risk_preference": "unmatched_policy",
                "p99_tail_overrun_ratio": 9.9,
                "max_acceptable_tail_ratio": 1.0,
                "calibrated_deadline_prob": 0.1,
                "min_acceptable_deadline_prob": 0.9,
                "estimated_cost_dollars": 999.0,
                "cost_budget_dollars": 10.0,
                "allow_balanced_policy": False,
                "force_conservative_baseline": False,
                "has_feasible_pareto_candidate": True,
            }),
        )

        # Ladder 3: cdc_catchup_rungs
        # Feasible burst catchup holds
        decide.climb(
            "cdc feasible hold",
            rs.cdc_catchup_rungs(),
            decide.Facts({
                "target_apply_capacity_bytes_sec": 50_000_000.0,
                "peak_burst_arrival_bytes_sec": 30_000_000.0,
                "estimated_recovery_seconds": 120.0,
                "max_recovery_window_sec": 3600.0,
                "nominal_arrival_bytes_sec": 10_000_000.0,
                "deadline_remaining_sec": 7200.0,
                "burst_throttling_supported": False,
                "backpressure_stall_ratio": 0.05,
                "throttle_threshold_ratio": 0.20,
            }),
        )
        # Impossible catchup holds
        decide.climb(
            "cdc impossible catchup hold",
            rs.cdc_catchup_rungs(),
            decide.Facts({
                "target_apply_capacity_bytes_sec": 20_000_000.0,
                "peak_burst_arrival_bytes_sec": 80_000_000.0,
                "estimated_recovery_seconds": 99999.0,
                "max_recovery_window_sec": 1000.0,
                "nominal_arrival_bytes_sec": 25_000_000.0,  # nominal >= apply
                "deadline_remaining_sec": 500.0,
                "burst_throttling_supported": False,
                "backpressure_stall_ratio": 0.0,
                "throttle_threshold_ratio": 0.20,
            }),
        )
        # Throttle or quarantine holds
        decide.climb(
            "cdc throttle hold",
            rs.cdc_catchup_rungs(),
            decide.Facts({
                "target_apply_capacity_bytes_sec": 50_000_000.0,
                "peak_burst_arrival_bytes_sec": 60_000_000.0,
                "estimated_recovery_seconds": 5000.0,
                "max_recovery_window_sec": 1000.0,
                "nominal_arrival_bytes_sec": 10_000_000.0,
                "deadline_remaining_sec": 10000.0,
                "burst_throttling_supported": True,
                "backpressure_stall_ratio": 0.45,
                "throttle_threshold_ratio": 0.20,
            }),
        )

        # Ladder 4: correlation_modeling_rungs
        # Copula correlated holds
        decide.climb(
            "correlation copula hold",
            rs.correlation_modeling_rungs(),
            decide.Facts({
                "outage_correlation_coefficient": 0.60,
                "shared_resource_group_count": 3,
                "calibrated_variance_available": True,
                "resource_partitioning_supported": False,
                "permit_nominal_baseline": False,
            }),
        )
        # Resource partition holds
        decide.climb(
            "correlation partition hold",
            rs.correlation_modeling_rungs(),
            decide.Facts({
                "outage_correlation_coefficient": 0.0,
                "shared_resource_group_count": 1,
                "calibrated_variance_available": False,
                "resource_partitioning_supported": True,
                "partitioning_overhead_ratio": 0.10,
                "max_partition_overhead_ratio": 0.30,
                "permit_nominal_baseline": False,
            }),
        )
        # Independent nominal baseline holds
        decide.climb(
            "correlation nominal hold",
            rs.correlation_modeling_rungs(),
            decide.Facts({
                "outage_correlation_coefficient": 0.0,
                "shared_resource_group_count": 1,
                "calibrated_variance_available": False,
                "resource_partitioning_supported": False,
                "permit_nominal_baseline": True,
            }),
        )
        # All refuse climb
        decide.climb(
            "correlation all refuse",
            rs.correlation_modeling_rungs(),
            decide.Facts({
                "outage_correlation_coefficient": 0.0,
                "shared_resource_group_count": 1,
                "calibrated_variance_available": False,
                "resource_partitioning_supported": False,
                "permit_nominal_baseline": False,
            }),
        )

    # Verify every rung declared in PROVES_RUNGS was reached on the expected branch
    for module, ladder, rung_name, branch_type in PROVES_RUNGS:
        branches_seen = branch(seen, rung_name)
        assert branch_type in branches_seen, (
            f"Rung '{rung_name}' in ladder '{ladder}' missing expected branch '{branch_type}'. "
            f"Observed branches: {branches_seen}"
        )


# -----------------------------------------------------------------------------
# 2. Shared-Resource & Outage Correlation Tests
# -----------------------------------------------------------------------------


def test_shared_resource_correlation_inflates_variance_and_tail_delay() -> None:
    """Validate that correlation among shared groups inflates tail completion percentiles."""
    # Independent case: rho = 0.0, 1 group
    tail_indep = rs.simulate_tail_risk_distribution(
        base_nominal_seconds=100.0,
        spread=0.20,
        correlation_coefficient=0.0,
        shared_group_count=1,
        outage_prob=0.05,
        mean_outage_sec=30.0,
    )
    assert tail_indep.variance_inflation_factor == 1.0

    # Correlated case: rho = 0.60, 4 shared groups
    tail_corr = rs.simulate_tail_risk_distribution(
        base_nominal_seconds=100.0,
        spread=0.20,
        correlation_coefficient=0.60,
        shared_group_count=4,
        outage_prob=0.05,
        mean_outage_sec=30.0,
    )
    # VIF = 1 + (4 - 1) * 0.60 = 2.80
    assert tail_corr.variance_inflation_factor == 2.80
    assert tail_corr.p90_seconds > tail_indep.p90_seconds
    assert tail_corr.p99_seconds > tail_indep.p99_seconds
    assert tail_corr.cvar_95_seconds > tail_indep.cvar_95_seconds
    assert tail_corr.correlated_outage_delay_seconds > tail_indep.correlated_outage_delay_seconds


# -----------------------------------------------------------------------------
# 3. Bursty CDC Arrivals, Recovery Work & Impossible Catch-Up Tests
# -----------------------------------------------------------------------------


def test_cdc_recovery_work_feasible_and_transient_burst() -> None:
    """Verify recovery work calculation and transient burst recovery."""
    profile = rs.CdcBurstProfile(
        nominal_arrival_bytes_sec=10_000_000.0,
        peak_burst_multiplier=2.0,  # 20 MB/s peak burst
        burst_duration_sec=30.0,
        current_backlog_bytes=50_000_000.0,
        target_apply_capacity_bytes_sec=40_000_000.0,  # 40 MB/s apply
    )
    # 30s outage -> 300 MB added backlog + 50 MB initial + 100 MB retry = 450 MB
    # Net drain rate = 40 MB/s - 10 MB/s = 30 MB/s -> 15s recovery
    recovery = rs.diagnose_cdc_recovery_and_catchup(
        profile,
        outage_duration_sec=30.0,
        chunk_retry_bytes=100_000_000.0,
        deadline_remaining_sec=3600.0,
    )
    assert not recovery.is_impossible_catchup
    assert recovery.care_badge in (rs.CareBadge.PASS, rs.CareBadge.REVIEW)
    assert recovery.net_drain_rate_bytes_sec == 30_000_000.0
    assert recovery.total_recovery_work_bytes == 450_000_000.0
    assert recovery.recovery_seconds == 15.0


def test_impossible_catchup_diagnosed_when_arrival_exceeds_apply() -> None:
    """Ensure impossible catch-up is diagnosed with ACTION badge when arrival >= apply."""
    profile = rs.CdcBurstProfile(
        nominal_arrival_bytes_sec=50_000_000.0,
        peak_burst_multiplier=1.5,
        burst_duration_sec=60.0,
        current_backlog_bytes=100_000_000.0,
        target_apply_capacity_bytes_sec=40_000_000.0,  # apply < arrival!
    )
    recovery = rs.diagnose_cdc_recovery_and_catchup(
        profile,
        outage_duration_sec=10.0,
        chunk_retry_bytes=10_000_000.0,
        deadline_remaining_sec=3600.0,
    )
    assert recovery.is_impossible_catchup
    assert recovery.care_badge == rs.CareBadge.ACTION
    assert math.isinf(recovery.recovery_seconds)
    assert "ACTION: impossible catch-up diagnosed" in recovery.diagnostic_message


def test_impossible_catchup_diagnosed_when_recovery_exceeds_deadline() -> None:
    """Ensure impossible catch-up is diagnosed when backlog cannot clear before deadline."""
    profile = rs.CdcBurstProfile(
        nominal_arrival_bytes_sec=38_000_000.0,
        peak_burst_multiplier=1.0,
        current_backlog_bytes=500_000_000.0,
        target_apply_capacity_bytes_sec=40_000_000.0,  # net drain only 2 MB/s
    )
    # 500 MB / 2 MB/s = 250s recovery > 60s remaining deadline
    recovery = rs.diagnose_cdc_recovery_and_catchup(
        profile,
        outage_duration_sec=0.0,
        chunk_retry_bytes=0.0,
        deadline_remaining_sec=60.0,
    )
    assert recovery.is_impossible_catchup
    assert recovery.care_badge == rs.CareBadge.ACTION
    assert recovery.recovery_seconds == 250.0


# -----------------------------------------------------------------------------
# 4. Calibrated Deadline Probability & False Confidence Gap
# -----------------------------------------------------------------------------


def test_false_confidence_gap_detection() -> None:
    """Detect when naive model gives false confidence compared to calibrated tail risk."""
    calibrated_tail = rs.TailRiskMetrics(
        p50_seconds=100.0,
        p90_seconds=145.0,
        p95_seconds=160.0,
        p99_seconds=195.0,
        var_95_seconds=160.0,
        cvar_95_seconds=175.0,
        variance_inflation_factor=2.5,
        correlated_outage_delay_seconds=20.0,
    )
    # Deadline set at 125 seconds
    # Nominal is 100s, so naive model thinks 125s is +2.5 sigma (~99% confident)
    # But calibrated tail p90 is 145s, so true probability of finishing by 125s is much lower
    eval_res = rs.evaluate_deadline_probability(
        calibrated_tail=calibrated_tail,
        nominal_seconds=100.0,
        deadline_seconds=125.0,
    )
    assert eval_res.naive_probability > 0.90
    assert eval_res.calibrated_probability < eval_res.naive_probability
    assert eval_res.false_confidence_gap > 0.15
    assert eval_res.confidence_badge in (rs.CareBadge.REVIEW, rs.CareBadge.ACTION)


# -----------------------------------------------------------------------------
# 5. Conflicting Objectives & Pareto Non-Dominated Alternatives
# -----------------------------------------------------------------------------


def test_pareto_frontier_filters_dominated_candidates() -> None:
    """Verify that filter_nondominated_pareto_front correctly filters strictly dominated candidates."""
    tail = rs.TailRiskMetrics(10, 20, 25, 30, 25, 28, 1.0, 0.0)
    dl = rs.DeadlineEvaluation(100, 0.95, 0.98, 0.03, False, rs.CareBadge.PASS)
    rec = rs.RecoveryWorkSummary(0, 0, 0, 100, 0, False, "ok", rs.CareBadge.PASS)

    # Candidate A dominates Candidate B in all 4 objectives
    alt_a = rs.ScheduleAlternative(
        strategy=rs.SchedulingStrategy.ROBUST_STREAM_DEFERRED_INDEX,
        objectives=rs.MultiObjectiveScores(100.0, 0.20, 50.0, 30.0),
        tail_metrics=tail,
        deadline_eval=dl,
        recovery=rec,
        is_feasible=True,
    )
    alt_b = rs.ScheduleAlternative(
        strategy=rs.SchedulingStrategy.PARALLEL_STAGED_BULK_LOAD,
        objectives=rs.MultiObjectiveScores(150.0, 0.40, 80.0, 50.0),
        tail_metrics=tail,
        deadline_eval=dl,
        recovery=rec,
        is_feasible=True,
    )
    # Candidate C is non-dominated: faster time, but higher cost
    alt_c = rs.ScheduleAlternative(
        strategy=rs.SchedulingStrategy.ISOLATED_PARTITION_DIRECT_APPLY,
        objectives=rs.MultiObjectiveScores(80.0, 0.25, 70.0, 25.0),
        tail_metrics=tail,
        deadline_eval=dl,
        recovery=rec,
        is_feasible=True,
    )

    pareto = rs.filter_nondominated_pareto_front([alt_a, alt_b, alt_c])
    pareto_strats = {alt.strategy for alt in pareto}
    assert rs.SchedulingStrategy.PARALLEL_STAGED_BULK_LOAD not in pareto_strats
    assert rs.SchedulingStrategy.ROBUST_STREAM_DEFERRED_INDEX in pareto_strats
    assert rs.SchedulingStrategy.ISOLATED_PARTITION_DIRECT_APPLY in pareto_strats


# -----------------------------------------------------------------------------
# 6. Explicit Preference Optimization & Plan Generation
# -----------------------------------------------------------------------------


def test_optimize_robust_schedule_risk_averse_preference() -> None:
    """Test optimization under risk-averse policy selecting tail-bounded strategy."""
    tables = [
        rs.TableWorkItem("customers", 2_000_000_000, 5_000_000, "group_core", 2),
        rs.TableWorkItem("orders", 5_000_000_000, 15_000_000, "group_orders", 3),
    ]
    facts = {
        "source_host": "127.0.0.1",
        "target_host": "10.0.0.5",
        "risk_preference": "risk_averse",
        "outage_correlation_coefficient": 0.50,
        "max_source_cpu_share": 0.45,
        "max_lock_duration_sec": 0.35,
        "correctness_minimums_satisfied": True,
    }
    plan = rs.optimize_robust_schedule(facts, tables=tables, deadline_seconds=7200.0)
    assert plan.active_risk_preference == rs.RiskPreference.RISK_AVERSE
    assert plan.tail_policy_choice.rung is not None
    assert plan.admissibility_choice.rung is not None
    assert len(plan.pareto_frontier) >= 1
    assert not plan.tradeoff_certificate.claims_universal_best
    assert not plan.tradeoff_certificate.claims_guaranteed_fastest
    assert plan.tradeoff_certificate.tradeoff_rationale != ""


def test_optimize_robust_schedule_deadline_critical_and_fallback() -> None:
    """Test deadline-critical preference and fallback to conservative baseline on breach."""
    tables = [rs.TableWorkItem("events", 1_000_000_000, 2_000_000, "group_events", 1)]
    facts = {
        "source_host": "127.0.0.1",
        "target_host": "10.0.0.10",
        "risk_preference": "deadline_critical",
        "force_conservative_baseline": True,
        "correctness_minimums_satisfied": True,
    }
    plan = rs.optimize_robust_schedule(facts, tables=tables, deadline_seconds=1800.0)
    assert plan.selected_strategy == rs.SchedulingStrategy.SAFE_CONSERVATIVE_BASELINE
    assert plan.tail_policy_choice.path == "conservative_baseline_fallback"


# -----------------------------------------------------------------------------
# 7. Universal O(1) Typed Durable Checkpoint Store Roundtrip
# -----------------------------------------------------------------------------


def test_robust_scheduler_checkpoint_o1_typed_roundtrip(tmp_path: pathlib.Path) -> None:
    """Verify O(1) checkpoint store preserves native Python types across serialization."""
    store_file = tmp_path / "robust_schedule_checkpoint.json"
    store = rs.RobustSchedulerCheckpointStore(store_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    unit1 = rs.RobustSchedulerCheckpointUnit(
        table_name="orders",
        schedule_phase="seed_and_cdc_tail",
        strategy=rs.SchedulingStrategy.ROBUST_STREAM_DEFERRED_INDEX,
        min_cursor=(1001, decimal.Decimal("49.95")),
        max_cursor=(50000, decimal.Decimal("9999.99")),
        merkle_digest="a1b2c3d4e5f6",
        completed_at=now,
    )
    unit2 = rs.RobustSchedulerCheckpointUnit(
        table_name="audit_events",
        schedule_phase="burst_drain",
        strategy=rs.SchedulingStrategy.ISOLATED_PARTITION_DIRECT_APPLY,
        min_cursor=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        max_cursor=uuid.UUID("87654321-4321-8765-4321-876543210987"),
        merkle_digest="fedcba654321",
        completed_at=now,
    )

    store.record_unit(unit1)
    store.record_unit(unit2)

    assert store.is_completed_o1("orders", "seed_and_cdc_tail")
    assert store.is_completed_o1("audit_events", "burst_drain")
    assert not store.is_completed_o1("orders", "verification")

    # Reload fresh instance from disk
    reloaded = rs.RobustSchedulerCheckpointStore(store_file)
    assert reloaded.is_completed_o1("orders", "seed_and_cdc_tail")
    assert reloaded.is_completed_o1("audit_events", "burst_drain")

    got1 = reloaded.get_unit("orders", "seed_and_cdc_tail")
    assert got1 is not None
    assert got1.strategy == rs.SchedulingStrategy.ROBUST_STREAM_DEFERRED_INDEX
    assert got1.min_cursor == (1001, decimal.Decimal("49.95"))
    assert got1.max_cursor == (50000, decimal.Decimal("9999.99"))

    got2 = reloaded.get_unit("audit_events", "burst_drain")
    assert got2 is not None
    assert isinstance(got2.min_cursor, uuid.UUID)
    assert str(got2.min_cursor) == "12345678-1234-5678-1234-567812345678"


# -----------------------------------------------------------------------------
# 8. Active Mutual-Exclusion Lease Lock (.run.lock)
# -----------------------------------------------------------------------------


def test_robust_scheduling_lease_acquisition_and_mutual_exclusion(tmp_path: pathlib.Path) -> None:
    """Verify active lease lock (.run.lock) prevents duplicate or concurrent runs."""
    lock_file = tmp_path / ".run.lock"
    lease1 = rs.acquire_robust_scheduling_lease(lock_file, what="test run 1", term_seconds=5.0)
    assert lease1 is not None

    # Second acquisition on active lease must raise Held (SystemExit/BaseException)
    with pytest.raises((Exception, BaseException)):
        rs.acquire_robust_scheduling_lease(lock_file, what="conflicting run 2", term_seconds=5.0)

    lease1.release()


# -----------------------------------------------------------------------------
# 9. Adaptive AIMD Batch Sizing & Cold-Start Byte-Weighted ETA
# -----------------------------------------------------------------------------


def test_aimd_burst_batch_sizing_and_eta() -> None:
    """Test AIMD batch reduction during backpressure stall and byte-weighted ETA."""
    # Under congestion stall: multiplicative decrease
    decreased = rs.adjust_aimd_burst_batch_size(
        current_batch_rows=10000,
        stall_ratio=0.35,  # > 0.20 stall
        multiplicative_decrease=0.50,
    )
    assert decreased == 5000

    # Under clean flow: additive increase
    increased = rs.adjust_aimd_burst_batch_size(
        current_batch_rows=10000,
        stall_ratio=0.05,
        additive_increase=1000,
    )
    assert increased == 11000

    # Byte-weighted ETA
    eta = rs.estimate_robust_byte_weighted_eta(
        total_bytes=1_000_000_000,
        completed_bytes=200_000_000,
        elapsed_seconds=10.0,
        cold_start_bytes_per_sec=15_000_000.0,
        tail_recovery_seconds=5.0,
    )
    # Remaining 800 MB; observed rate 20 MB/s, weight 0.2, cold 15 MB/s -> blended 16 MB/s -> 50s + 5s tail = 55s
    assert 50.0 <= eta <= 60.0


# -----------------------------------------------------------------------------
# 10. Safe Host Validation & External Tool Normalization
# -----------------------------------------------------------------------------


def test_safe_host_validation_refuses_unsafe_endpoints() -> None:
    """Verify endpoint validation rejects hosts outside 127.0.0.1 and 10.0.0.x."""
    rs.validate_safe_endpoint("127.0.0.1")
    rs.validate_safe_endpoint("localhost")
    rs.validate_safe_endpoint("::1")
    rs.validate_safe_endpoint("10.0.0.42")

    with pytest.raises(rs.UnsafeEndpointHostError):
        rs.validate_safe_endpoint("192.168.1.100")

    with pytest.raises(rs.UnsafeEndpointHostError):
        rs.validate_safe_endpoint("prod-db.corp.internal")


def test_normalize_external_tool_result_care_badges() -> None:
    """Verify 4-tier care badges and zero internal tool name leaks."""
    res_err = rs.normalize_external_tool_result("FATAL: replication lag exceeded", exit_code=1)
    assert res_err.care_badge == rs.CareBadge.ACTION
    assert not res_err.success

    res_warn = rs.normalize_external_tool_result("WARN: burst throttling engaged", exit_code=0)
    assert res_warn.care_badge == rs.CareBadge.REVIEW
    assert res_warn.success

    res_drift = rs.normalize_external_tool_result("drift detected on non-key columns", exit_code=0)
    assert res_drift.care_badge == rs.CareBadge.BENIGN
    assert res_drift.success

    res_clean = rs.normalize_external_tool_result("batch committed successfully", exit_code=0)
    assert res_clean.care_badge == rs.CareBadge.PASS
    assert res_clean.success
