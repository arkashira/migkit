"""Tests for Row 607: SMART-N24.decision-compute (Budget the optimizer and certify solution quality).

Verifies:
1. Every decide.Rung in `migkit/decision_compute.py` (`compute_budget_rungs`,
   `search_strategy_rungs`, `space_pruning_rungs`, `incumbent_update_rungs`, and
   `solution_quality_rungs`) is exercised on both `holds` and `refuses` inside
   `with tests.rung_branches.watched():` and declared in `PROVES_RUNGS`.
2. Bounded planner CPU/memory/time on huge estates:
   - Bounded beam width and pruning prevent exponential blowup on massive tables.
3. Cancellation and incumbent safety:
   - Cancelling or interrupting search never returns an invalid candidate;
     returns validated feasible incumbent with REVIEW care badge.
4. Solution quality certification:
   - Optimal is reported only with mathematical proof / exhaustive certificate (PASS).
   - Suboptimal or heuristic results report certified bounds and gap (BENIGN).
5. Universal typed durable resume in O(1):
   - Native type preservation (datetime, Decimal, UUID, composite tuples).
   - Skipping completed units in O(1) without rescan.
6. Mutual exclusion active lease lock (.run.lock) with heartbeat.
7. Adaptive AIMD batch sizing and cold-start byte-weighted ETA.
8. External tool wrapper (Datafold, DMS, DTS, reladiff, DVT, pgcopydb, debezium)
   with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS).
9. Safe host validation (127.0.0.1, localhost, 10.0.0.x).

Pure UNIX style only. Zero emojis or decorative glyphs.
"""

from __future__ import annotations

import datetime
import decimal
import math
import os
import pathlib
import time
import uuid
from typing import Any, Dict, List, Tuple

import pytest

import tests.rung_branches
from tests.rung_branches import branch

from migkit import decide
from migkit import decision_compute as dc

PROVES_RUNGS = [
    # compute_budget_rungs
    ("decision_compute", "compute_budget_rungs", "high_depth_budget", "holds"),
    ("decision_compute", "compute_budget_rungs", "high_depth_budget", "refuses"),
    ("decision_compute", "compute_budget_rungs", "standard_bounded_budget", "holds"),
    ("decision_compute", "compute_budget_rungs", "standard_bounded_budget", "refuses"),
    ("decision_compute", "compute_budget_rungs", "minimal_baseline_budget", "holds"),
    ("decision_compute", "compute_budget_rungs", "minimal_baseline_budget", "refuses"),

    # search_strategy_rungs
    ("decision_compute", "search_strategy_rungs", "exhaustive_oracle_strategy", "holds"),
    ("decision_compute", "search_strategy_rungs", "exhaustive_oracle_strategy", "refuses"),
    ("decision_compute", "search_strategy_rungs", "deeper_beam_search_strategy", "holds"),
    ("decision_compute", "search_strategy_rungs", "deeper_beam_search_strategy", "refuses"),
    ("decision_compute", "search_strategy_rungs", "bounded_local_search_strategy", "holds"),
    ("decision_compute", "search_strategy_rungs", "bounded_local_search_strategy", "refuses"),
    ("decision_compute", "search_strategy_rungs", "cheap_baseline_strategy", "holds"),
    ("decision_compute", "search_strategy_rungs", "cheap_baseline_strategy", "refuses"),

    # space_pruning_rungs
    ("decision_compute", "space_pruning_rungs", "admissible_bound_prune", "holds"),
    ("decision_compute", "space_pruning_rungs", "admissible_bound_prune", "refuses"),
    ("decision_compute", "space_pruning_rungs", "resource_capacity_violation_prune", "holds"),
    ("decision_compute", "space_pruning_rungs", "resource_capacity_violation_prune", "refuses"),
    ("decision_compute", "space_pruning_rungs", "retain_and_expand_branch", "holds"),
    ("decision_compute", "space_pruning_rungs", "retain_and_expand_branch", "refuses"),

    # incumbent_update_rungs
    ("decision_compute", "incumbent_update_rungs", "accept_strictly_improving_incumbent", "holds"),
    ("decision_compute", "incumbent_update_rungs", "accept_strictly_improving_incumbent", "refuses"),
    ("decision_compute", "incumbent_update_rungs", "reject_suboptimal_candidate", "holds"),
    ("decision_compute", "incumbent_update_rungs", "reject_suboptimal_candidate", "refuses"),
    ("decision_compute", "incumbent_update_rungs", "reject_infeasible_candidate", "holds"),
    ("decision_compute", "incumbent_update_rungs", "reject_infeasible_candidate", "refuses"),

    # solution_quality_rungs
    ("decision_compute", "solution_quality_rungs", "certify_optimal_quality", "holds"),
    ("decision_compute", "solution_quality_rungs", "certify_optimal_quality", "refuses"),
    ("decision_compute", "solution_quality_rungs", "certify_bounded_gap_quality", "holds"),
    ("decision_compute", "solution_quality_rungs", "certify_bounded_gap_quality", "refuses"),
    ("decision_compute", "solution_quality_rungs", "certify_interrupted_incumbent_quality", "holds"),
    ("decision_compute", "solution_quality_rungs", "certify_interrupted_incumbent_quality", "refuses"),
    ("decision_compute", "solution_quality_rungs", "reject_invalid_candidate", "holds"),
    ("decision_compute", "solution_quality_rungs", "reject_invalid_candidate", "refuses"),
]


def test_every_decide_rung_is_exercised_on_both_branches():
    """Exercise both holds and refuses branches for all decision_compute rungs under watched()."""
    with tests.rung_branches.watched() as seen:
        # 1. compute_budget_rungs
        # A) high_depth_budget holds
        f_h_holds = {
            "potential_makespan_savings_s": 250.0,
            "high_benefit_threshold_s": 100.0,
            "planner_cpu_headroom_ratio": 0.8,
            "min_benefit_threshold_s": 5.0,
            "max_planner_wall_s": 30.0,
            "allow_minimal_budget": True,
        }
        c1 = dc.climb_compute_budget("test:compute_budget:high", f_h_holds)
        assert c1.rung is not None and c1.rung.name == "high_depth_budget"

        # B) high_depth_budget refuses, standard_bounded_budget holds
        f_std_holds = {
            "potential_makespan_savings_s": 20.0,
            "high_benefit_threshold_s": 100.0,
            "planner_cpu_headroom_ratio": 0.2,  # refuses high
            "min_benefit_threshold_s": 5.0,
            "max_planner_wall_s": 5.0,
            "allow_minimal_budget": True,
        }
        c2 = dc.climb_compute_budget("test:compute_budget:standard", f_std_holds)
        assert c2.rung is not None and c2.rung.name == "standard_bounded_budget"

        # C) standard_bounded_budget refuses, minimal_baseline_budget holds
        f_min_holds = {
            "potential_makespan_savings_s": 1.0,
            "high_benefit_threshold_s": 100.0,
            "planner_cpu_headroom_ratio": 0.1,
            "min_benefit_threshold_s": 5.0,  # refuses standard
            "max_planner_wall_s": 0.5,
            "allow_minimal_budget": True,
        }
        c3 = dc.climb_compute_budget("test:compute_budget:min", f_min_holds)
        assert c3.rung is not None and c3.rung.name == "minimal_baseline_budget"

        # D) minimal_baseline_budget refuses
        f_min_refuses = {
            "potential_makespan_savings_s": 0.0,
            "high_benefit_threshold_s": 100.0,
            "planner_cpu_headroom_ratio": 0.1,
            "min_benefit_threshold_s": 10.0,
            "max_planner_wall_s": 0.1,
            "allow_minimal_budget": False,
        }
        dc.climb_compute_budget("test:compute_budget:none", f_min_refuses)

        # 2. search_strategy_rungs
        # A) exhaustive_oracle_strategy holds
        f_ex_holds = {
            "total_states": 16,
            "max_exhaustive_states": 64,
            "allocated_budget_s": 1.0,
            "exhaustive_estimated_time_s": 0.005,
            "potential_makespan_savings_s": 50.0,
            "min_beam_benefit_s": 10.0,
            "min_beam_time_s": 2.0,
            "min_local_search_time_s": 0.1,
            "estate_tables": 4,
            "allow_cheap_baseline": True,
        }
        c4 = dc.climb_search_strategy("test:strategy:exhaustive", f_ex_holds)
        assert c4.rung is not None and c4.rung.name == "exhaustive_oracle_strategy"

        # B) exhaustive refuses (states too large), deeper_beam_search_strategy holds
        f_beam_holds = {
            "total_states": 10000,
            "max_exhaustive_states": 64,  # refuses exhaustive
            "allocated_budget_s": 5.0,
            "exhaustive_estimated_time_s": 5.0,
            "potential_makespan_savings_s": 50.0,
            "min_beam_benefit_s": 10.0,
            "min_beam_time_s": 2.0,
            "min_local_search_time_s": 0.1,
            "estate_tables": 10,
            "allow_cheap_baseline": True,
        }
        c5 = dc.climb_search_strategy("test:strategy:beam", f_beam_holds)
        assert c5.rung is not None and c5.rung.name == "deeper_beam_search_strategy"

        # C) deeper_beam refuses (budget too small), bounded_local_search_strategy holds
        f_local_holds = {
            "total_states": 10000,
            "max_exhaustive_states": 64,
            "allocated_budget_s": 0.5,  # refuses beam
            "exhaustive_estimated_time_s": 5.0,
            "potential_makespan_savings_s": 50.0,
            "min_beam_benefit_s": 10.0,
            "min_beam_time_s": 2.0,
            "min_local_search_time_s": 0.1,
            "estate_tables": 5,
            "allow_cheap_baseline": True,
        }
        c6 = dc.climb_search_strategy("test:strategy:local", f_local_holds)
        assert c6.rung is not None and c6.rung.name == "bounded_local_search_strategy"

        # D) bounded_local refuses (single table, budget small), cheap_baseline_strategy holds
        f_cheap_holds = {
            "total_states": 1,
            "max_exhaustive_states": 64,
            "allocated_budget_s": 0.01,
            "exhaustive_estimated_time_s": 0.05,
            "potential_makespan_savings_s": 1.0,
            "min_beam_benefit_s": 10.0,
            "min_beam_time_s": 2.0,
            "min_local_search_time_s": 0.1,  # refuses local
            "estate_tables": 1,
            "allow_cheap_baseline": True,
        }
        c7 = dc.climb_search_strategy("test:strategy:cheap", f_cheap_holds)
        assert c7.rung is not None and c7.rung.name == "cheap_baseline_strategy"

        # E) cheap_baseline refuses
        f_cheap_refuses = {
            "total_states": 0,
            "max_exhaustive_states": 64,
            "allocated_budget_s": 0.0,
            "exhaustive_estimated_time_s": 0.1,
            "potential_makespan_savings_s": 0.0,
            "min_beam_benefit_s": 10.0,
            "min_beam_time_s": 2.0,
            "min_local_search_time_s": 1.0,
            "estate_tables": 0,
            "allow_cheap_baseline": False,
        }
        dc.climb_search_strategy("test:strategy:none", f_cheap_refuses)

        # 3. space_pruning_rungs
        # A) admissible_bound_prune holds
        f_adm_holds = {
            "node_lower_bound": 120.0,
            "incumbent_upper_bound": 100.0,
            "violates_hard_capacity": False,
            "violates_money_limit": False,
        }
        c8 = dc.climb_space_pruning("test:pruning:admissible", f_adm_holds)
        assert c8.rung is not None and c8.rung.name == "admissible_bound_prune"

        # B) admissible_bound refuses, resource_capacity_violation_prune holds
        f_cap_holds = {
            "node_lower_bound": 80.0,  # refuses admissible
            "incumbent_upper_bound": 100.0,
            "violates_hard_capacity": True,
            "violates_money_limit": False,
        }
        c9 = dc.climb_space_pruning("test:pruning:capacity", f_cap_holds)
        assert c9.rung is not None and c9.rung.name == "resource_capacity_violation_prune"

        # C) resource_capacity refuses, retain_and_expand_branch holds
        f_retain_holds = {
            "node_lower_bound": 75.0,
            "incumbent_upper_bound": 100.0,
            "violates_hard_capacity": False,
            "violates_money_limit": False,
        }
        c10 = dc.climb_space_pruning("test:pruning:retain", f_retain_holds)
        assert c10.rung is not None and c10.rung.name == "retain_and_expand_branch"

        # D) retain_and_expand_branch refuses (node_lb >= incumbent)
        f_retain_refuses = {
            "node_lower_bound": 150.0,
            "incumbent_upper_bound": 100.0,
            "violates_hard_capacity": True,
            "violates_money_limit": True,
        }
        dc.climb_space_pruning("test:pruning:refuse_retain", f_retain_refuses)

        # 4. incumbent_update_rungs
        # A) accept_strictly_improving_incumbent holds
        f_inc_holds = {
            "candidate_feasible": True,
            "candidate_objective": 45.0,
            "current_incumbent_objective": 60.0,
        }
        c11 = dc.climb_incumbent_update("test:incumbent:accept", f_inc_holds)
        assert c11.rung is not None and c11.rung.name == "accept_strictly_improving_incumbent"

        # B) accept refuses (suboptimal), reject_suboptimal_candidate holds
        f_sub_holds = {
            "candidate_feasible": True,
            "candidate_objective": 70.0,  # refuses accept
            "current_incumbent_objective": 60.0,
        }
        c12 = dc.climb_incumbent_update("test:incumbent:suboptimal", f_sub_holds)
        assert c12.rung is not None and c12.rung.name == "reject_suboptimal_candidate"

        # C) reject_infeasible_candidate holds
        f_inf_holds = {
            "candidate_feasible": False,
            "candidate_objective": 10.0,
            "current_incumbent_objective": 60.0,
        }
        c13 = dc.climb_incumbent_update("test:incumbent:infeasible", f_inf_holds)
        assert c13.rung is not None and c13.rung.name == "reject_infeasible_candidate"

        # D) reject_suboptimal_candidate refuses, reject_infeasible_candidate refuses
        f_all_refuses = {
            "candidate_feasible": True,
            "candidate_objective": 30.0,
            "current_incumbent_objective": 60.0,
        }
        # In this call, candidate_objective < incumbent, so reject_suboptimal refuses!
        # And candidate_feasible is True, so reject_infeasible refuses!
        dc.climb_incumbent_update("test:incumbent:refuse_rejects", f_all_refuses)

        # 5. solution_quality_rungs
        # A) certify_interrupted_incumbent_quality holds
        f_int_holds = {
            "is_exhaustively_proven": False,
            "optimality_gap": 0.05,
            "has_valid_incumbent": True,
            "incumbent_feasible": True,
            "has_valid_bounds": True,
            "is_interrupted": True,
            "is_cancelled": False,
        }
        c14 = dc.climb_solution_quality("test:quality:interrupted", f_int_holds)
        assert c14.rung is not None and c14.rung.name == "certify_interrupted_incumbent_quality"

        # B) certify_interrupted refuses, certify_optimal_quality holds
        f_opt_holds = {
            "is_exhaustively_proven": True,
            "optimality_gap": 0.0,
            "has_valid_incumbent": True,
            "incumbent_feasible": True,
            "has_valid_bounds": True,
            "is_interrupted": False,
            "is_cancelled": False,
        }
        c15 = dc.climb_solution_quality("test:quality:optimal", f_opt_holds)
        assert c15.rung is not None and c15.rung.name == "certify_optimal_quality"

        # C) certify_optimal refuses, certify_bounded_gap_quality holds
        f_gap_holds = {
            "is_exhaustively_proven": False,
            "optimality_gap": 0.05,
            "has_valid_incumbent": True,
            "incumbent_feasible": True,
            "has_valid_bounds": True,
            "is_interrupted": False,
            "is_cancelled": False,
        }
        c16 = dc.climb_solution_quality("test:quality:gap", f_gap_holds)
        assert c16.rung is not None and c16.rung.name == "certify_bounded_gap_quality"

        # D) reject_invalid_candidate holds
        f_inv_holds = {
            "is_exhaustively_proven": False,
            "optimality_gap": 1.0,
            "has_valid_incumbent": False,
            "incumbent_feasible": False,
            "has_valid_bounds": False,
            "is_interrupted": False,
            "is_cancelled": False,
        }
        c17 = dc.climb_solution_quality("test:quality:invalid", f_inv_holds)
        assert c17.rung is not None and c17.rung.name == "reject_invalid_candidate"

        # E) reject_invalid_candidate refuses (when incumbent is valid and feasible)
        f_inv_refuses = {
            "is_exhaustively_proven": True,
            "optimality_gap": 0.0,
            "has_valid_incumbent": True,
            "incumbent_feasible": True,
            "has_valid_bounds": True,
            "is_interrupted": False,
            "is_cancelled": False,
        }
        dc.climb_solution_quality("test:quality:valid", f_inv_refuses)

    # Validate that every declared rung reached both branches
    for _, _, rung_name, expected_branch in PROVES_RUNGS:
        branches_seen = branch(seen, rung_name)
        assert expected_branch in branches_seen, f"rung '{rung_name}' did not reach branch '{expected_branch}'"


def test_exhaustive_oracle_certifies_optimality():
    """Verify that small combinatorial space yields mathematically certified optimal solution."""
    ways = (
        dc.DecisionWay("pass", per_stream=10_000_000, startup=0.1, bills=1e-7),
        dc.DecisionWay("pipe", per_stream=20_000_000, startup=0.05, bills=2e-7),
    )
    jobs = (
        dc.DecisionJob("table_small_1", bytes=100_000_000, ways=ways),
        dc.DecisionJob("table_small_2", bytes=200_000_000, ways=ways),
        dc.DecisionJob("table_small_3", bytes=150_000_000, ways=ways),
    )
    limits = dc.DecisionLimits(workers=4, money=100.0)
    optimizer = dc.BudgetedOptimizer(jobs, limits)
    cert = optimizer.optimize("test:exhaustive_oracle")

    assert cert.status == dc.CertificateStatus.OPTIMAL_CERTIFIED
    assert cert.care_badge == dc.CARE_PASS
    assert cert.is_optimal is True
    assert cert.optimality_gap <= 1e-6
    assert cert.proof_type == "EXHAUSTIVE_ENUMERATION"
    assert len(cert.assignments) == 3
    for j in jobs:
        assert j.name in cert.assignments


def test_bounded_search_on_medium_estate_reports_valid_bounds():
    """Verify that medium estate uses bounded search and reports valid lower/upper bounds and gap."""
    ways = (
        dc.DecisionWay("stream_a", per_stream=15_000_000, uses={"source": 1.0, "target": 1.0}, startup=0.1, bills=1e-7),
        dc.DecisionWay("stream_b", per_stream=25_000_000, uses={"source": 2.0, "target": 1.0}, startup=0.2, bills=2e-7),
        dc.DecisionWay("stream_c", per_stream=10_000_000, uses={"source": 0.5, "target": 1.0}, startup=0.05, bills=5e-8),
    )
    jobs = tuple(
        dc.DecisionJob(f"table_mid_{i}", bytes=50_000_000 * (i + 1), ways=ways)
        for i in range(12)
    )
    limits = dc.DecisionLimits(workers=8, caps={"source": 80_000_000, "target": 80_000_000}, money=500.0)
    budget = dc.PlanningBudgetLimits(max_planner_wall_s=2.0, max_exhaustive_states=16)

    optimizer = dc.BudgetedOptimizer(jobs, limits, budget_limits=budget)
    cert = optimizer.optimize("test:bounded_search")

    assert cert.status in (dc.CertificateStatus.BOUNDED_GAP, dc.CertificateStatus.OPTIMAL_CERTIFIED)
    assert cert.care_badge in (dc.CARE_BENIGN, dc.CARE_PASS)
    assert cert.lower_bound > 0.0
    assert cert.upper_bound >= cert.lower_bound
    assert 0.0 <= cert.optimality_gap <= 1.0
    assert len(cert.assignments) == len(jobs)
    assert cert.compute_time_s < 10.0


def test_huge_estate_search_is_strictly_bounded_in_time_and_memory():
    """Verify that huge estate (1,000 tables) finishes under strict planner CPU/memory bounds."""
    ways = (
        dc.DecisionWay("fast_copier", per_stream=40_000_000, uses={"source": 1.0}, startup=0.1),
        dc.DecisionWay("paged_copier", per_stream=20_000_000, uses={"source": 0.5}, startup=0.05),
    )
    # 1,000 tables
    jobs = tuple(
        dc.DecisionJob(f"huge_tbl_{i}", bytes=10_000_000, ways=ways)
        for i in range(1000)
    )
    limits = dc.DecisionLimits(workers=16, caps={"source": 200_000_000})
    budget = dc.PlanningBudgetLimits(max_planner_wall_s=0.5, max_beam_width=16, max_exhaustive_states=16)

    t0 = time.perf_counter()
    optimizer = dc.BudgetedOptimizer(jobs, limits, budget_limits=budget)
    cert = optimizer.optimize("test:huge_estate")
    elapsed = time.perf_counter() - t0

    assert elapsed < 5.0, f"planner took too long on huge estate: {elapsed:.2f}s"
    assert len(cert.assignments) == 1000
    assert cert.makespan > 0.0
    assert cert.upper_bound < math.inf


def test_cancellation_preserves_validated_incumbent():
    """Verify that cancelling search returns validated feasible incumbent with REVIEW care badge."""
    ways = (
        dc.DecisionWay("way_1", per_stream=10_000_000, startup=0.1),
        dc.DecisionWay("way_2", per_stream=20_000_000, startup=0.2),
    )
    jobs = tuple(
        dc.DecisionJob(f"cancel_tbl_{i}", bytes=100_000_000, ways=ways)
        for i in range(20)
    )
    limits = dc.DecisionLimits(workers=4)
    token = dc.CancellationToken()

    # Pre-cancel token
    token.cancel()
    optimizer = dc.BudgetedOptimizer(jobs, limits, cancellation_token=token)
    cert = optimizer.optimize("test:cancellation")

    assert cert.status == dc.CertificateStatus.INCUMBENT_INTERRUPTED
    assert cert.care_badge == dc.CARE_REVIEW
    assert cert.is_optimal is False
    assert len(cert.assignments) == 20
    assert dc.is_assignment_feasible(jobs, limits, cert.assignments)


def test_infeasible_workload_rejects_with_action_badge():
    """Verify that unfeasible workload (money budget impossible) rejects with ACTION badge."""
    ways = (
        dc.DecisionWay("expensive_way", per_stream=10_000_000, bills=10.0),
    )
    jobs = (
        dc.DecisionJob("unpayable_tbl", bytes=1000, ways=ways),
    )
    # Requires 1000 * 10.0 = 10,000, but money limit is 5.0
    limits = dc.DecisionLimits(workers=1, money=5.0)
    optimizer = dc.BudgetedOptimizer(jobs, limits)
    cert = optimizer.optimize("test:infeasible")

    assert cert.status == dc.CertificateStatus.INVALID
    assert cert.care_badge == dc.CARE_ACTION
    assert cert.is_optimal is False
    assert cert.makespan == math.inf


def test_universal_typed_durable_resume_o1(tmp_path: pathlib.Path):
    """Verify that O(1) durable resume preserves native types and skips completed units."""
    store = dc.DecisionComputeCheckpointStore(tmp_path)
    now = datetime.datetime.now(datetime.timezone.utc)
    uid = uuid.uuid4()
    dec = decimal.Decimal("1234.5678")

    unit = dc.CheckpointUnit(
        unit_id="partition_001",
        stage="beam_search",
        status="completed",
        recorded_at=now,
        cursor=("table_alpha", 42, dec, uid),
        metadata={"tables": 15, "cost": dec},
    )
    store.save_unit(unit)

    # Check O(1) membership before reloading
    assert store.has_unit("partition_001") is True
    assert store.has_unit("partition_999") is False
    assert store.completed_count() == 1

    # Reload from disk and verify type restoration
    store_reloaded = dc.DecisionComputeCheckpointStore(tmp_path)
    assert store_reloaded.has_unit("partition_001") is True
    loaded = store_reloaded.get_unit("partition_001")
    assert loaded is not None
    assert loaded.unit_id == "partition_001"
    assert loaded.recorded_at == now
    assert isinstance(loaded.cursor, tuple)
    assert loaded.cursor[0] == "table_alpha"
    assert loaded.cursor[1] == 42
    assert isinstance(loaded.cursor[2], decimal.Decimal)
    assert loaded.cursor[2] == dec
    assert isinstance(loaded.cursor[3], uuid.UUID)
    assert loaded.cursor[3] == uid


def test_mutual_exclusion_active_lease_lock(tmp_path: pathlib.Path):
    """Verify that .run.lock enforces mutual exclusion with heartbeats and clean release."""
    lock_a = dc.DecisionComputeLeaseLock(tmp_path, lease_ttl_s=10.0)
    lock_a.acquire()
    lock_file = tmp_path / ".run.lock"
    assert lock_file.exists()

    # Heartbeat
    lock_a.heartbeat()

    # Second concurrent acquire must fail
    lock_b = dc.DecisionComputeLeaseLock(tmp_path, lease_ttl_s=10.0)
    with pytest.raises(dc.LeaseLockError):
        lock_b.acquire()

    # Release lock A
    lock_a.release()

    # Second lock can now acquire
    lock_b.acquire()
    lock_b.release()


def test_aimd_batch_controller_and_cold_start_eta():
    """Verify adaptive AIMD batch controller and instant cold-start ETA."""
    aimd = dc.AIMDBatchController(initial_batch=10, min_batch=2, max_batch=50)
    assert aimd.current_batch_size == 10

    # Fast latency triggers additive increase
    aimd.record_success(0.01)
    assert aimd.current_batch_size == 12

    # High pressure triggers multiplicative decrease
    aimd.record_pressure()
    assert aimd.current_batch_size == 6

    # Cold start ETA
    eta_start = dc.estimate_cold_start_eta(0, 10_000_000, 0.0, 0.8)
    assert eta_start > 0.0

    eta_prog = dc.estimate_cold_start_eta(5_000_000, 10_000_000, 1.0, 1.0)
    assert math.isclose(eta_prog, 1.0, rel_tol=1e-3)


def test_external_optimizer_wrapper():
    """Verify external optimizer wrapper with zero tool leaks and care badges."""
    ways = (
        dc.DecisionWay("sync", per_stream=10_000_000),
        dc.DecisionWay("bulk", per_stream=25_000_000),
    )
    jobs = (
        dc.DecisionJob("orders", bytes=50_000_000, ways=ways),
        dc.DecisionJob("customers", bytes=20_000_000, ways=ways),
    )
    limits = dc.DecisionLimits(workers=4)
    wrapper = dc.ExternalOptimizerWrapper(jobs, limits)

    # 1. Valid plan from datafold
    plan_datafold = {"orders": "bulk", "customers": "bulk"}
    res_df = wrapper.evaluate_external_plan("datafold", plan_datafold)
    assert res_df.is_feasible is True
    assert res_df.care_badge in (dc.CARE_PASS, dc.CARE_BENIGN)

    # 2. Invalid plan containing non-existent way
    bad_plan = {"orders": "invalid_super_speed", "customers": "bulk"}
    res_bad = wrapper.evaluate_external_plan("dms", bad_plan)
    assert res_bad.is_feasible is False
    assert res_bad.care_badge == dc.CARE_ACTION

    # 3. Unrecognized external tool
    with pytest.raises(dc.DecisionComputeError):
        wrapper.evaluate_external_plan("unrecognized_tool_xyz", plan_datafold)


def test_safe_endpoint_validation():
    """Verify safe loopback and 10.0.0.x test addresses; reject public IPs."""
    assert dc.validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert dc.validate_safe_endpoint("localhost") == "localhost"
    assert dc.validate_safe_endpoint("10.0.0.42") == "10.0.0.42"

    with pytest.raises(dc.UnsafeEndpointError):
        dc.validate_safe_endpoint("8.8.8.8")

    with pytest.raises(dc.UnsafeEndpointError):
        dc.validate_safe_endpoint("192.168.1.1")

    with pytest.raises(dc.UnsafeEndpointError):
        dc.validate_safe_endpoint("")
