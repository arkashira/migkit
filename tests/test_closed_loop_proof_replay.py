"""Tests for Row 593: SMART-N10.closed-loop-proof (Measure and replay decision quality end to end).

Proves:
1. Every `decide.Rung` in `migkit/closed_loop_proof.py` reaches both `holds` and
   `refuses` under `tests.rung_branches.watched()` and is declared in `PROVES_RUNGS`.
2. Deterministic replay reproduces original choices exactly from cryptographically
   hashed traces of contracts, facts, model versions, alternatives, and refusals.
3. Counterfactual what-if replay evaluates alternatives in isolated sandboxes
   without duplicating live writes.
4. Small-space exhaustive oracle evaluates discrete state lattices across work
   volume, network delay, and error rate, proving safety monotonicity.
5. Metamorphic relations hold across scale monotonicity, irrelevant fact invariance,
   and refusal consistency.
6. Fault/crash injection at switch boundaries proves O(1) typed durable resume,
   zero lost changes, and zero duplicate changes.
7. Active verification lease lock (.run.lock) prevents duplicate or concurrent execution.
8. Verified completion quantiles (p50/p95), calibrated forecast intervals, and source
   impact budgets are strictly measured and reported.
9. Regret is computed strictly where comparator is empirically measured; unmeasured
   counterfactuals remain explicitly unmeasured.
10. Dynamic AIMD batch sizing, byte-weighted cold-start ETA, and safe host validation.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import datetime
import decimal
import os
import pathlib
import time
import uuid
from typing import Dict, List, Tuple

import pytest

import tests.rung_branches
from migkit import closed_loop_proof as clp
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: Read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    # replay_mode_rungs
    ("closed_loop_proof", "replay_mode_rungs", "deterministic_recorded_replay", "holds"),
    ("closed_loop_proof", "replay_mode_rungs", "deterministic_recorded_replay", "refuses"),
    ("closed_loop_proof", "replay_mode_rungs", "counterfactual_shadow_replay", "holds"),
    ("closed_loop_proof", "replay_mode_rungs", "counterfactual_shadow_replay", "refuses"),
    ("closed_loop_proof", "replay_mode_rungs", "fault_injected_verification", "holds"),
    ("closed_loop_proof", "replay_mode_rungs", "fault_injected_verification", "refuses"),
    ("closed_loop_proof", "replay_mode_rungs", "bounded_real_lane_execution", "holds"),
    ("closed_loop_proof", "replay_mode_rungs", "bounded_real_lane_execution", "refuses"),
    ("closed_loop_proof", "replay_mode_rungs", "quarantine_and_refuse_replay", "holds"),
    ("closed_loop_proof", "replay_mode_rungs", "quarantine_and_refuse_replay", "refuses"),

    # proof_verdict_rungs
    ("closed_loop_proof", "proof_verdict_rungs", "certified_closed_loop_pass", "holds"),
    ("closed_loop_proof", "proof_verdict_rungs", "certified_closed_loop_pass", "refuses"),
    ("closed_loop_proof", "proof_verdict_rungs", "measured_drift_review", "holds"),
    ("closed_loop_proof", "proof_verdict_rungs", "measured_drift_review", "refuses"),
    ("closed_loop_proof", "proof_verdict_rungs", "benign_discrepancy_tolerated", "holds"),
    ("closed_loop_proof", "proof_verdict_rungs", "benign_discrepancy_tolerated", "refuses"),
    ("closed_loop_proof", "proof_verdict_rungs", "action_required_failure", "holds"),
    ("closed_loop_proof", "proof_verdict_rungs", "action_required_failure", "refuses"),

    # comparator_regret_rungs
    ("closed_loop_proof", "comparator_regret_rungs", "optimal_choice_zero_regret", "holds"),
    ("closed_loop_proof", "comparator_regret_rungs", "optimal_choice_zero_regret", "refuses"),
    ("closed_loop_proof", "comparator_regret_rungs", "suboptimal_measured_regret", "holds"),
    ("closed_loop_proof", "comparator_regret_rungs", "suboptimal_measured_regret", "refuses"),
    ("closed_loop_proof", "comparator_regret_rungs", "unmeasured_comparator_bounded_gap", "holds"),
    ("closed_loop_proof", "comparator_regret_rungs", "unmeasured_comparator_bounded_gap", "refuses"),
    ("closed_loop_proof", "comparator_regret_rungs", "infeasible_comparator_pruned", "holds"),
    ("closed_loop_proof", "comparator_regret_rungs", "infeasible_comparator_pruned", "refuses"),

    # switch_recovery_rungs
    ("closed_loop_proof", "switch_recovery_rungs", "typed_durable_resume_o1", "holds"),
    ("closed_loop_proof", "switch_recovery_rungs", "typed_durable_resume_o1", "refuses"),
    ("closed_loop_proof", "switch_recovery_rungs", "fenced_rollback_and_retry", "holds"),
    ("closed_loop_proof", "switch_recovery_rungs", "fenced_rollback_and_retry", "refuses"),
    ("closed_loop_proof", "switch_recovery_rungs", "isolated_quarantine_block", "holds"),
    ("closed_loop_proof", "switch_recovery_rungs", "isolated_quarantine_block", "refuses"),
]


# -----------------------------------------------------------------------------
# 1. Decide Rung Coverage Proof (Watched)
# -----------------------------------------------------------------------------

def test_decide_rungs_reach_both_branches():
    """Exercise both holds and refuses branches for every rung under watched()."""
    with watched() as seen:
        # --- replay_mode_rungs ---
        # 1. deterministic_recorded_replay: holds
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": True,
                "contract_intact": True,
                "has_unresolved_unknowns": False,
                "sandbox_isolated": True,
                "allows_live_writes": False,
                "has_comparator_model": True,
                "fault_injection_authorized": False,
                "crash_points_specified": False,
                "real_lane_authorized": False,
                "source_impact_budget_ok": True,
                "quarantine_triggered": False,
            },
        )
        # deterministic_recorded_replay: refuses (corrupted trace)
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": True,
                "has_unresolved_unknowns": False,
                "sandbox_isolated": True,
                "allows_live_writes": False,
                "has_comparator_model": True,
                "fault_injection_authorized": False,
                "crash_points_specified": False,
                "real_lane_authorized": False,
                "source_impact_budget_ok": True,
                "quarantine_triggered": False,
            },
        )

        # 2. counterfactual_shadow_replay: holds
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": False,
                "has_unresolved_unknowns": True,
                "sandbox_isolated": True,
                "allows_live_writes": False,
                "has_comparator_model": True,
                "fault_injection_authorized": False,
                "crash_points_specified": False,
                "real_lane_authorized": False,
                "source_impact_budget_ok": True,
                "quarantine_triggered": False,
            },
        )
        # counterfactual_shadow_replay: refuses (live writes requested)
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": False,
                "has_unresolved_unknowns": True,
                "sandbox_isolated": True,
                "allows_live_writes": True,
                "has_comparator_model": True,
                "fault_injection_authorized": False,
                "crash_points_specified": False,
                "real_lane_authorized": False,
                "source_impact_budget_ok": True,
                "quarantine_triggered": False,
            },
        )

        # 3. fault_injected_verification: holds
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": False,
                "has_unresolved_unknowns": True,
                "sandbox_isolated": False,
                "allows_live_writes": False,
                "has_comparator_model": False,
                "fault_injection_authorized": True,
                "crash_points_specified": True,
                "real_lane_authorized": False,
                "source_impact_budget_ok": True,
                "quarantine_triggered": False,
            },
        )
        # fault_injected_verification: refuses (fault injection unauthorized)
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": False,
                "has_unresolved_unknowns": True,
                "sandbox_isolated": False,
                "allows_live_writes": False,
                "has_comparator_model": False,
                "fault_injection_authorized": False,
                "crash_points_specified": True,
                "real_lane_authorized": False,
                "source_impact_budget_ok": True,
                "quarantine_triggered": False,
            },
        )

        # 4. bounded_real_lane_execution: holds
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": False,
                "has_unresolved_unknowns": True,
                "sandbox_isolated": False,
                "allows_live_writes": False,
                "has_comparator_model": False,
                "fault_injection_authorized": False,
                "crash_points_specified": False,
                "real_lane_authorized": True,
                "source_impact_budget_ok": True,
                "quarantine_triggered": False,
            },
        )
        # bounded_real_lane_execution: refuses (budget exceeded)
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": False,
                "has_unresolved_unknowns": True,
                "sandbox_isolated": False,
                "allows_live_writes": False,
                "has_comparator_model": False,
                "fault_injection_authorized": False,
                "crash_points_specified": False,
                "real_lane_authorized": True,
                "source_impact_budget_ok": False,
                "quarantine_triggered": False,
            },
        )

        # 5. quarantine_and_refuse_replay: holds
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": False,
                "has_unresolved_unknowns": True,
                "sandbox_isolated": False,
                "allows_live_writes": False,
                "has_comparator_model": False,
                "fault_injection_authorized": False,
                "crash_points_specified": False,
                "real_lane_authorized": False,
                "source_impact_budget_ok": False,
                "quarantine_triggered": True,
            },
        )
        # quarantine_and_refuse_replay: refuses
        clp.decide_replay_mode(
            "test_replay",
            {
                "trace_intact": False,
                "contract_intact": False,
                "has_unresolved_unknowns": True,
                "sandbox_isolated": False,
                "allows_live_writes": False,
                "has_comparator_model": False,
                "fault_injection_authorized": False,
                "crash_points_specified": False,
                "real_lane_authorized": False,
                "source_impact_budget_ok": False,
                "quarantine_triggered": False,
            },
        )

        # --- proof_verdict_rungs ---
        # 1. certified_closed_loop_pass: holds
        clp.decide_proof_verdict(
            "test_verdict",
            {
                "lost_changes": 0,
                "duplicate_changes": 0,
                "p95_within_forecast": True,
                "source_impact_within_budget": True,
                "retry_waste_seconds": 0.0,
                "retry_waste_bytes": 0,
                "regret_acceptable": True,
                "is_simulation_or_shadow": False,
                "actionable_defect": False,
            },
        )
        # certified_closed_loop_pass: refuses (lost changes)
        clp.decide_proof_verdict(
            "test_verdict",
            {
                "lost_changes": 1,
                "duplicate_changes": 0,
                "p95_within_forecast": True,
                "source_impact_within_budget": True,
                "retry_waste_seconds": 0.0,
                "retry_waste_bytes": 0,
                "regret_acceptable": True,
                "is_simulation_or_shadow": False,
                "actionable_defect": True,
            },
        )

        # 2. measured_drift_review: holds
        clp.decide_proof_verdict(
            "test_verdict",
            {
                "lost_changes": 0,
                "duplicate_changes": 0,
                "p95_within_forecast": False,
                "source_impact_within_budget": True,
                "retry_waste_seconds": 1.2,
                "retry_waste_bytes": 500,
                "regret_acceptable": True,
                "is_simulation_or_shadow": False,
                "actionable_defect": False,
            },
        )
        # measured_drift_review: refuses (regret unacceptable)
        clp.decide_proof_verdict(
            "test_verdict",
            {
                "lost_changes": 0,
                "duplicate_changes": 0,
                "p95_within_forecast": False,
                "source_impact_within_budget": True,
                "retry_waste_seconds": 1.2,
                "retry_waste_bytes": 500,
                "regret_acceptable": False,
                "is_simulation_or_shadow": False,
                "actionable_defect": False,
            },
        )

        # 3. benign_discrepancy_tolerated: holds
        clp.decide_proof_verdict(
            "test_verdict",
            {
                "lost_changes": 0,
                "duplicate_changes": 0,
                "p95_within_forecast": False,
                "source_impact_within_budget": False,
                "retry_waste_seconds": 5.0,
                "retry_waste_bytes": 1000,
                "regret_acceptable": False,
                "is_simulation_or_shadow": True,
                "actionable_defect": False,
            },
        )
        # benign_discrepancy_tolerated: refuses (not simulation or shadow)
        clp.decide_proof_verdict(
            "test_verdict",
            {
                "lost_changes": 0,
                "duplicate_changes": 0,
                "p95_within_forecast": False,
                "source_impact_within_budget": False,
                "retry_waste_seconds": 5.0,
                "retry_waste_bytes": 1000,
                "regret_acceptable": False,
                "is_simulation_or_shadow": False,
                "actionable_defect": True,
            },
        )

        # 4. action_required_failure: holds
        clp.decide_proof_verdict(
            "test_verdict",
            {
                "lost_changes": 2,
                "duplicate_changes": 1,
                "p95_within_forecast": False,
                "source_impact_within_budget": False,
                "retry_waste_seconds": 10.0,
                "retry_waste_bytes": 5000,
                "regret_acceptable": False,
                "is_simulation_or_shadow": False,
                "actionable_defect": True,
            },
        )
        # action_required_failure: refuses
        clp.decide_proof_verdict(
            "test_verdict",
            {
                "lost_changes": 2,
                "duplicate_changes": 1,
                "p95_within_forecast": False,
                "source_impact_within_budget": False,
                "retry_waste_seconds": 10.0,
                "retry_waste_bytes": 5000,
                "regret_acceptable": False,
                "is_simulation_or_shadow": False,
                "actionable_defect": False,
            },
        )

        # --- comparator_regret_rungs ---
        # 1. optimal_choice_zero_regret: holds
        clp.decide_comparator_regret(
            "test_regret",
            {
                "comparator_measured": True,
                "chosen_cost": 1.0,
                "comparator_cost": 1.02,
                "tie_threshold": 0.05,
                "bounds_derived_from_facts": True,
                "comparator_infeasible": False,
            },
        )
        # optimal_choice_zero_regret: refuses (comparator not measured)
        clp.decide_comparator_regret(
            "test_regret",
            {
                "comparator_measured": False,
                "chosen_cost": 1.0,
                "comparator_cost": 1.02,
                "tie_threshold": 0.05,
                "bounds_derived_from_facts": True,
                "comparator_infeasible": False,
            },
        )

        # 2. suboptimal_measured_regret: holds
        clp.decide_comparator_regret(
            "test_regret",
            {
                "comparator_measured": True,
                "chosen_cost": 2.0,
                "comparator_cost": 1.0,
                "tie_threshold": 0.05,
                "bounds_derived_from_facts": True,
                "comparator_infeasible": False,
            },
        )
        # suboptimal_measured_regret: refuses (comparator was slower)
        clp.decide_comparator_regret(
            "test_regret",
            {
                "comparator_measured": True,
                "chosen_cost": 1.0,
                "comparator_cost": 3.0,
                "tie_threshold": 0.05,
                "bounds_derived_from_facts": True,
                "comparator_infeasible": False,
            },
        )

        # 3. unmeasured_comparator_bounded_gap: holds
        clp.decide_comparator_regret(
            "test_regret",
            {
                "comparator_measured": False,
                "chosen_cost": 1.0,
                "comparator_cost": 0.8,
                "tie_threshold": 0.05,
                "bounds_derived_from_facts": True,
                "comparator_infeasible": False,
            },
        )
        # unmeasured_comparator_bounded_gap: refuses (arbitrary magic bounds)
        clp.decide_comparator_regret(
            "test_regret",
            {
                "comparator_measured": False,
                "chosen_cost": 1.0,
                "comparator_cost": 0.8,
                "tie_threshold": 0.05,
                "bounds_derived_from_facts": False,
                "comparator_infeasible": False,
            },
        )

        # 4. infeasible_comparator_pruned: holds
        clp.decide_comparator_regret(
            "test_regret",
            {
                "comparator_measured": True,
                "chosen_cost": 1.0,
                "comparator_cost": 1.5,
                "tie_threshold": 0.05,
                "bounds_derived_from_facts": False,
                "comparator_infeasible": True,
            },
        )
        # infeasible_comparator_pruned: refuses
        clp.decide_comparator_regret(
            "test_regret",
            {
                "comparator_measured": True,
                "chosen_cost": 1.0,
                "comparator_cost": 1.5,
                "tie_threshold": 0.05,
                "bounds_derived_from_facts": False,
                "comparator_infeasible": False,
            },
        )

        # --- switch_recovery_rungs ---
        # 1. typed_durable_resume_o1: holds
        clp.decide_switch_recovery(
            "test_switch",
            {
                "checkpoint_valid": True,
                "cursor_intact": True,
                "lease_acquired": True,
                "partial_state_detected": False,
                "rollback_fence_available": True,
                "retry_budget": 3,
                "unrecoverable_error": False,
            },
        )
        # typed_durable_resume_o1: refuses (lease not acquired)
        clp.decide_switch_recovery(
            "test_switch",
            {
                "checkpoint_valid": True,
                "cursor_intact": True,
                "lease_acquired": False,
                "partial_state_detected": False,
                "rollback_fence_available": True,
                "retry_budget": 3,
                "unrecoverable_error": False,
            },
        )

        # 2. fenced_rollback_and_retry: holds
        clp.decide_switch_recovery(
            "test_switch",
            {
                "checkpoint_valid": False,
                "cursor_intact": False,
                "lease_acquired": False,
                "partial_state_detected": True,
                "rollback_fence_available": True,
                "retry_budget": 2,
                "unrecoverable_error": False,
            },
        )
        # fenced_rollback_and_retry: refuses (retry budget exhausted)
        clp.decide_switch_recovery(
            "test_switch",
            {
                "checkpoint_valid": False,
                "cursor_intact": False,
                "lease_acquired": False,
                "partial_state_detected": True,
                "rollback_fence_available": True,
                "retry_budget": 0,
                "unrecoverable_error": False,
            },
        )

        # 3. isolated_quarantine_block: holds
        clp.decide_switch_recovery(
            "test_switch",
            {
                "checkpoint_valid": False,
                "cursor_intact": False,
                "lease_acquired": False,
                "partial_state_detected": False,
                "rollback_fence_available": False,
                "retry_budget": 0,
                "unrecoverable_error": True,
            },
        )
        # isolated_quarantine_block: refuses
        clp.decide_switch_recovery(
            "test_switch",
            {
                "checkpoint_valid": False,
                "cursor_intact": False,
                "lease_acquired": False,
                "partial_state_detected": False,
                "rollback_fence_available": False,
                "retry_budget": 0,
                "unrecoverable_error": False,
            },
        )

    # Verify every rung declared in PROVES_RUNGS was reached
    for mod, ladder, r_name, br in PROVES_RUNGS:
        reached = branch(seen, r_name)
        assert br in reached, f"rung {r_name} in {ladder} did not reach branch {br} (reached: {reached})"


# -----------------------------------------------------------------------------
# 2. Deterministic Trace Replay & Counterfactual Evaluation
# -----------------------------------------------------------------------------

def test_deterministic_replay_and_tamper_detection():
    trace = clp.DecisionTraceRecord.create(
        work_name="order_table_migration",
        execution_contract={
            "goal": "zero_loss_cutover",
            "source_endpoint": "127.0.0.1:5432",
            "target_endpoint": "127.0.0.1:5433",
            "max_source_cpu_pct": 30.0,
        },
        facts_snapshot={
            "trace_intact": True,
            "contract_intact": True,
            "has_unresolved_unknowns": False,
            "has_comparator_model": True,
            "fault_injection_authorized": False,
            "crash_points_specified": False,
            "real_lane_authorized": False,
            "source_impact_budget_ok": True,
            "quarantine_triggered": False,
        },
        model_versions={
            "cost_model": "1.4.0",
            "capability_matrix": "v2026.10",
            "engine": "migkit-1.0",
        },
        chosen_rung="deterministic_recorded_replay",
        chosen_because="cryptographic digest, facts, and model versions match",
        alternatives=["deterministic_recorded_replay", "counterfactual_shadow_replay"],
        refusals={},
        unknowns=(),
        lane_type=clp.LaneType.SHADOW,
    )
    assert trace.verify_integrity() is True

    engine = clp.DeterministicReplayEngine(trace)
    res = engine.replay(lane_type=clp.LaneType.SHADOW, sandbox_isolated=True, allows_live_writes=False)
    assert res.reproduced is True
    assert res.replayed_chosen_rung == "deterministic_recorded_replay"
    assert res.counterfactual_divergence is False

    # Counterfactual what-if: simulate corrupted trace -> diverges to shadow replay
    cf_res = engine.replay(
        counterfactual_facts={"trace_intact": False},
        lane_type=clp.LaneType.SHADOW,
        sandbox_isolated=True,
    )
    assert cf_res.counterfactual_divergence is True
    assert cf_res.replayed_chosen_rung == "counterfactual_shadow_replay"

    # Tampered digest detection
    tampered_trace = clp.DecisionTraceRecord(
        record_id=trace.record_id,
        work_name=trace.work_name,
        execution_contract={"goal": "altered_contract_tampered"},
        facts_snapshot=trace.facts_snapshot,
        model_versions=trace.model_versions,
        chosen_rung=trace.chosen_rung,
        chosen_because=trace.chosen_because,
        alternatives=trace.alternatives,
        refusals=trace.refusals,
        unknowns=trace.unknowns,
        digest=trace.digest,  # old digest doesn't match altered contract
        lane_type=trace.lane_type,
        recorded_at_s=trace.recorded_at_s,
    )
    with pytest.raises(clp.TamperedTraceError):
        tampered_trace.verify_integrity()


def test_shadow_lane_prohibits_live_writes():
    trace = clp.DecisionTraceRecord.create(
        work_name="user_table_migration",
        execution_contract={"goal": "shadow_eval"},
        facts_snapshot={"trace_intact": True, "contract_intact": True, "has_unresolved_unknowns": False},
        model_versions={"engine": "migkit-1.0"},
        chosen_rung="deterministic_recorded_replay",
        chosen_because="test",
        alternatives=["deterministic_recorded_replay"],
        refusals={},
        unknowns=(),
        lane_type=clp.LaneType.SHADOW,
    )
    engine = clp.DeterministicReplayEngine(trace)
    with pytest.raises(clp.UnsafeExecutionLaneError):
        engine.replay(lane_type=clp.LaneType.SHADOW, sandbox_isolated=True, allows_live_writes=True)

    with pytest.raises(clp.UnsafeExecutionLaneError):
        engine.replay(lane_type=clp.LaneType.SIMULATION, sandbox_isolated=True, allows_live_writes=True)


# -----------------------------------------------------------------------------
# 3. Small-Space Exhaustive Oracle & Metamorphic Verifier
# -----------------------------------------------------------------------------

def test_small_space_exhaustive_oracle():
    oracle = clp.SmallSpaceExhaustiveOracle()
    res = oracle.evaluate_all_combinations()
    assert res.total_states_evaluated == 3 * 3 * 3 * 2 * 2  # 108 combinations
    assert res.is_clean is True
    assert len(res.violations) == 0


def test_metamorphic_properties():
    # 1. Scale monotonicity
    assert clp.MetamorphicVerifier.verify_scale_monotonicity(
        base_bytes=100 * 1024 * 1024,
        scaled_bytes=500 * 1024 * 1024,
        base_rate_mb_s=25.0,
    )

    # 2. Irrelevant fact invariance
    base_facts = {
        "trace_intact": True,
        "contract_intact": True,
        "has_unresolved_unknowns": False,
        "sandbox_isolated": True,
        "allows_live_writes": False,
        "has_comparator_model": True,
        "fault_injection_authorized": False,
        "crash_points_specified": False,
        "real_lane_authorized": False,
        "source_impact_budget_ok": True,
        "quarantine_triggered": False,
    }
    assert clp.MetamorphicVerifier.verify_irrelevant_fact_invariance(
        "test_work",
        base_facts,
        irrelevant_key="unrelated_operator_tag",
        irrelevant_value="datacenter_us_east",
    )

    # 3. Refusal consistency
    assert clp.MetamorphicVerifier.verify_refusal_consistency(
        "test_work",
        base_facts,
        required_fact="trace_intact",
    )


# -----------------------------------------------------------------------------
# 4. Fault/Crash Injection at Switch Boundaries & O(1) Resume
# -----------------------------------------------------------------------------

def test_fault_crash_injection_and_durable_resume_o1(tmp_path: pathlib.Path):
    store_file = tmp_path / "checkpoint.json"
    lock_dir = tmp_path / "locks"
    checkpoint_store = clp.ClosedLoopCheckpointStore(store_file)
    lease_lock = clp.ClosedLoopLeaseLock(lock_dir)

    units = [
        clp.ClosedLoopCheckpointUnit(
            unit_key=f"chunk-{i}",
            trace_record_id="trace-test-01",
            recorded_at=datetime.datetime(2026, 10, 17, 12, 0, tzinfo=datetime.timezone.utc),
            predicted_seconds=decimal.Decimal("1.25"),
            actual_seconds=decimal.Decimal("1.18"),
            unit_uuid=uuid.uuid4(),
            care_badge="PASS",
            composite_cursor=("orders", i * 1000),
            applied_items_count=1000,
        )
        for i in range(5)
    ]

    # Injected crash during state flush on unit 2
    injector = clp.FaultCrashInjector(target_crash_point=clp.SwitchCrashPoint.DURING_STATE_FLUSH)

    with lease_lock:
        with pytest.raises(clp.CrashInjectionError):
            clp.execute_engine_switch_with_crash_injection(
                checkpoint_store, lease_lock, injector, units
            )

    # Recovery: Re-open checkpoint store and lease lock
    recovered_store = clp.ClosedLoopCheckpointStore(store_file)
    assert recovered_store.count() == 0  # Crash hit before unit 0 finished recording

    # Now execute cleanly without crash injector
    clean_injector = clp.FaultCrashInjector(target_crash_point=None)
    with lease_lock:
        applied, lost, waste = clp.execute_engine_switch_with_crash_injection(
            recovered_store, lease_lock, clean_injector, units
        )
        assert applied == 5
        assert lost == 0
        assert waste == 0.0

    # Test O(1) skip idempotency
    with lease_lock:
        applied_again, lost_again, _ = clp.execute_engine_switch_with_crash_injection(
            recovered_store, lease_lock, clean_injector, units
        )
        assert applied_again == 0  # all 5 units skipped in O(1)
        assert lost_again == 0


def test_active_verification_lease_lock_mutual_exclusion(tmp_path: pathlib.Path):
    lock_dir = tmp_path / "lease_dir"
    lock1 = clp.ClosedLoopLeaseLock(lock_dir, ttl_seconds=30.0, owner_id="runner-1")
    lock2 = clp.ClosedLoopLeaseLock(lock_dir, ttl_seconds=30.0, owner_id="runner-2")

    lock1.acquire()
    lock1.heartbeat()

    # Second concurrent acquire must raise VerificationLeaseError
    with pytest.raises(clp.VerificationLeaseError):
        lock2.acquire()

    lock1.release()

    # Now lock2 can acquire cleanly
    lock2.acquire()
    lock2.release()


# -----------------------------------------------------------------------------
# 5. Metrics, Forecast Calibration, Regret, and Care Badges
# -----------------------------------------------------------------------------

def test_metrics_evaluation_and_calibrated_forecast():
    trace = clp.DecisionTraceRecord.create(
        work_name="payment_events_table",
        execution_contract={"goal": "zero_loss_cutover"},
        facts_snapshot={"trace_intact": True, "contract_intact": True, "has_unresolved_unknowns": False},
        model_versions={"engine": "migkit-1.0"},
        chosen_rung="deterministic_recorded_replay",
        chosen_because="exact match",
        alternatives=["deterministic_recorded_replay"],
        refusals={},
        unknowns=(),
        lane_type=clp.LaneType.REAL,
    )

    baseline_timings = [1.0, 1.1, 1.05, 1.2, 1.15, 1.08, 1.12, 1.18]
    actual_timings = [1.02, 1.09, 1.14, 1.07]
    comparator_timings = [1.25, 1.30, 1.28]  # comparator is slower -> regret = 0

    source_impact = clp.SourceImpactReading(
        cpu_percent=15.0,
        read_iops=1200.0,
        locks_held=2,
        active_connections=12,
        wal_bytes_per_sec=250000.0,
    )
    impact_budget = clp.SourceImpactReading(
        cpu_percent=40.0,
        read_iops=5000.0,
        locks_held=10,
        active_connections=50,
        wal_bytes_per_sec=1000000.0,
    )

    # 1. Clean PASS run
    report_pass = clp.evaluate_closed_loop_proof(
        trace,
        actual_timings,
        source_impact,
        impact_budget,
        forecast_baseline_timings=baseline_timings,
        comparator_measured_timings=comparator_timings,
        retry_waste_s=0.0,
        retry_waste_bytes=0,
        lost_changes=0,
        duplicate_changes=0,
        lane_type=clp.LaneType.REAL,
    )
    assert report_pass.care_badge == clp.CareBadge.PASS
    assert report_pass.forecast_calibrated is True
    assert report_pass.regret_s == 0.0  # chosen was faster than comparator
    summary = report_pass.render_unix_summary()
    assert "CLOSED_LOOP_PROOF PASS [REAL]" in summary
    assert "integrity: lost_changes=0 duplicate_changes=0" in summary

    # 2. REVIEW run (performance drift / retry waste)
    drifted_timings = [1.30, 1.35, 1.40]
    comparator_for_review = [1.32, 1.34, 1.36]
    report_review = clp.evaluate_closed_loop_proof(
        trace,
        drifted_timings,
        source_impact,
        impact_budget,
        forecast_baseline_timings=baseline_timings,
        comparator_measured_timings=comparator_for_review,
        retry_waste_s=1.5,
        retry_waste_bytes=2048,
        lost_changes=0,
        duplicate_changes=0,
        lane_type=clp.LaneType.REAL,
    )
    assert report_review.care_badge == clp.CareBadge.REVIEW

    # 3. BENIGN run (simulation discrepancy)
    report_benign = clp.evaluate_closed_loop_proof(
        trace,
        drifted_timings,
        source_impact,
        impact_budget,
        forecast_baseline_timings=baseline_timings,
        comparator_measured_timings=comparator_timings,
        retry_waste_s=2.0,
        retry_waste_bytes=4096,
        lost_changes=0,
        duplicate_changes=0,
        lane_type=clp.LaneType.SIMULATION,
    )
    assert report_benign.care_badge == clp.CareBadge.BENIGN

    # 4. ACTION run (lost changes)
    report_action = clp.evaluate_closed_loop_proof(
        trace,
        actual_timings,
        source_impact,
        impact_budget,
        forecast_baseline_timings=baseline_timings,
        comparator_measured_timings=comparator_timings,
        retry_waste_s=0.0,
        retry_waste_bytes=0,
        lost_changes=3,
        duplicate_changes=0,
        lane_type=clp.LaneType.REAL,
    )
    assert report_action.care_badge == clp.CareBadge.ACTION


def test_regret_measured_strictly_when_comparator_measured():
    trace = clp.DecisionTraceRecord.create(
        work_name="customers_table",
        execution_contract={"goal": "migration"},
        facts_snapshot={"trace_intact": True, "contract_intact": True, "has_unresolved_unknowns": False},
        model_versions={"engine": "migkit-1.0"},
        chosen_rung="deterministic_recorded_replay",
        chosen_because="test",
        alternatives=["deterministic_recorded_replay"],
        refusals={},
        unknowns=(),
        lane_type=clp.LaneType.REAL,
    )
    timings = [2.0, 2.1, 2.05]
    budget = clp.SourceImpactReading(50.0, 5000.0, 5, 20, 1000000.0)

    # When comparator is NOT empirically measured, regret MUST be None
    report_unmeasured = clp.evaluate_closed_loop_proof(
        trace,
        timings,
        budget,
        budget,
        forecast_baseline_timings=timings,
        comparator_measured_timings=None,
    )
    assert report_unmeasured.comparator_empirically_measured is False
    assert report_unmeasured.regret_s is None
    assert "regret: UNMEASURED" in report_unmeasured.render_unix_summary()

    # When comparator IS empirically measured and faster -> quantifiable regret
    faster_comparator = [1.0, 1.1, 1.05]
    report_measured = clp.evaluate_closed_loop_proof(
        trace,
        timings,
        budget,
        budget,
        forecast_baseline_timings=timings,
        comparator_measured_timings=faster_comparator,
    )
    assert report_measured.comparator_empirically_measured is True
    assert report_measured.regret_s is not None
    assert report_measured.regret_s > 0.90


# -----------------------------------------------------------------------------
# 6. Adaptive Execution & Safe Host Validation
# -----------------------------------------------------------------------------

def test_aimd_batch_sizing_and_byte_weighted_eta():
    # Latency under target: additive increase
    s1 = clp.adjust_aimd_batch_size(
        current_size=1000,
        measured_latency_s=0.20,
        target_latency_s=0.50,
        additive_increase=100,
    )
    assert s1 == 1100

    # Latency exceeds target: multiplicative decrease
    s2 = clp.adjust_aimd_batch_size(
        current_size=1000,
        measured_latency_s=0.80,
        target_latency_s=0.50,
        multiplicative_decrease=0.5,
    )
    assert s2 == 500

    # Byte-weighted ETA
    eta_cold = clp.estimate_byte_weighted_eta(
        completed_bytes=0,
        total_bytes=100 * 1024 * 1024,
        elapsed_seconds=0.0,
        cold_start_predicted_rate_mb_s=20.0,
    )
    assert 4.5 <= eta_cold <= 5.5

    eta_done = clp.estimate_byte_weighted_eta(
        completed_bytes=100 * 1024 * 1024,
        total_bytes=100 * 1024 * 1024,
        elapsed_seconds=5.0,
    )
    assert eta_done == 0.0


def test_safe_endpoint_validation():
    assert clp.validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert clp.validate_safe_endpoint("localhost") == "localhost"
    assert clp.validate_safe_endpoint("10.0.0.42") == "10.0.0.42"

    with pytest.raises(clp.UnsafeEndpointHostError):
        clp.validate_safe_endpoint("192.168.1.1")

    with pytest.raises(clp.UnsafeEndpointHostError):
        clp.validate_safe_endpoint("prod-db.internal.corp")

    with pytest.raises(clp.UnsafeEndpointHostError):
        clp.validate_safe_endpoint("")
