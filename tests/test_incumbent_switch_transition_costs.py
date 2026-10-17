"""Tests for Row 643: RCH35.incumbent-switch (Include transition and rollback costs when replanning).

Verifies:
1. All `decide.Rung`s in `incumbent_switch.SWITCH_LADDER` reach both `holds` and
   `refuses` under `tests.rung_branches.watched()` and are declared in `PROVES_RUNGS`.
2. Six-part transition and rollback pricing: checkpoint granularity, spool drains,
   transaction barriers, process setup, resource restore, and proof regeneration.
3. Validated incumbent protection and p90 remaining-work evaluation vs local
   instantaneous throughput (including `costmodel.CostModel` and `jointplan.Way`).
4. Marginally faster path with expensive replay never steals the job.
5. Sustained changes cross a reproducible break-even threshold without oscillation.
6. O(1) typed durable checkpoint resume (`DurableSwitchCheckpoint`) and active
   verification lease lock (`ActiveVerificationLease`, `.run.lock`).
7. Pure UNIX style formatting with 4-tier care badges (`ACTION`, `REVIEW`, `BENIGN`, `PASS`)
   and 100% pure ASCII.
"""
from __future__ import annotations

import datetime
import decimal
import math
import uuid
from pathlib import Path

import pytest

from migkit import costmodel, decide, incumbent_switch, jointplan
from migkit.incumbent_switch import (
     SWITCH_LADDER,
    ActiveLeaseConflictError,
    ActiveVerificationLease,
    BreakEvenAnalysis,
    CareBadge,
    CheckpointGranularity,
    CheckpointGranularitySpec,
    DurableSwitchCheckpoint,
    HysteresisPolicy,
    IncumbentSwitchController,
    InvalidTransitionSpecError,
    PathPerformanceProfile,
    ProcessSetupSpec,
    ProofRegenerationSpec,
    RemainingWorkSpec,
    ResourceRestoreSpec,
    SpoolDrainSpec,
    SwitchVerdict,
    TransactionBarrierSpec,
    TransitionPlanSpec,
    UnsafeEndpointHostError,
    UnvalidatedIncumbentError,
    choose_replan_rung,
    compute_break_even,
    format_switch_report,
    price_transition_and_rollback,
    validate_safe_host,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

MIB = 1024 * 1024
GIB = 1024 * MIB

PROVES_RUNGS = [
    ("incumbent_switch", "SWITCH_LADDER", "retain_incumbent", "holds"),
    ("incumbent_switch", "SWITCH_LADDER", "retain_incumbent", "refuses"),
    ("incumbent_switch", "SWITCH_LADDER", "checkpoint_fenced_switch", "holds"),
    ("incumbent_switch", "SWITCH_LADDER", "checkpoint_fenced_switch", "refuses"),
    ("incumbent_switch", "SWITCH_LADDER", "emergency_rollback_switch", "holds"),
    ("incumbent_switch", "SWITCH_LADDER", "emergency_rollback_switch", "refuses"),
]


# ---------------------------------------------------------------------------
# 1. Decision Engine Rung Branch Verification (watched() + PROVES_RUNGS)
# ---------------------------------------------------------------------------


def test_every_switch_ladder_rung_holds_and_refuses_under_watched() -> None:
    with watched() as seen:
        # 1) retain_incumbent holds when incumbent is healthy and switch is not warranted;
        #    checkpoint_fenced_switch and emergency_rollback_switch refuse.
        c_retain = choose_replan_rung(
            {
                "incumbent_healthy": True,
                "switch_warranted": False,
                "challenger_validated": False,
                "p90_savings_positive": False,
                "hysteresis_satisfied": False,
                "barrier_within_timeout": True,
                "fallback_available": False,
            }
        )
        assert c_retain.rung is not None
        assert c_retain.rung.name == "retain_incumbent"

        # 2) retain_incumbent refuses when switch_warranted=True;
        #    checkpoint_fenced_switch holds when all its needs hold.
        c_switch = choose_replan_rung(
            {
                "incumbent_healthy": True,
                "switch_warranted": True,
                "challenger_validated": True,
                "p90_savings_positive": True,
                "hysteresis_satisfied": True,
                "barrier_within_timeout": True,
                "fallback_available": True,
            }
        )
        assert c_switch.rung is not None
        assert c_switch.rung.name == "checkpoint_fenced_switch"

        # 3) emergency_rollback_switch holds when incumbent_healthy=False and fallback_available=True;
        #    retain_incumbent and checkpoint_fenced_switch refuse.
        c_emerg = choose_replan_rung(
            {
                "incumbent_healthy": False,
                "switch_warranted": True,
                "challenger_validated": True,
                "p90_savings_positive": True,
                "hysteresis_satisfied": True,
                "barrier_within_timeout": True,
                "fallback_available": True,
            }
        )
        assert c_emerg.rung is not None
        assert c_emerg.rung.name == "emergency_rollback_switch"

        # 4) All rungs refuse when incumbent_healthy=False and fallback_available=False.
        c_none = choose_replan_rung(
            {
                "incumbent_healthy": False,
                "switch_warranted": True,
                "challenger_validated": False,
                "p90_savings_positive": False,
                "hysteresis_satisfied": False,
                "barrier_within_timeout": False,
                "fallback_available": False,
            }
        )
        assert c_none.rung is None

    for rung_Obj in SWITCH_LADDER:
        branches = branch(seen, rung_Obj.name)
        assert HOLDS in branches, f"{rung_Obj.name} did not hold"
        assert REFUSES in branches, f"{rung_Obj.name} did not refuse"

    assert why(seen, "emergency_rollback_switch") == "no validated fallback way is available"


# ---------------------------------------------------------------------------
# 2. Six-Part Transition & Rollback Cost Pricing
# ---------------------------------------------------------------------------


def test_six_component_transition_and_rollback_pricing() -> None:
    incumbent = PathPerformanceProfile(
        name="incumbent_copier",
        said="the validated range copier",
        mean_bytes_per_sec=50 * MIB,
        sd_bytes_per_sec=2 * MIB,
        sample_count=10,
        validated=True,
        failure_prob=0.01,
    )
    challenger = PathPerformanceProfile(
        name="challenger_bulk",
        said="the parallel bulk loader",
        mean_bytes_per_sec=80 * MIB,
        sd_bytes_per_sec=4 * MIB,
        sample_count=10,
        validated=True,
        startup_sec=1.5,
        failure_prob=0.05,
        lost_fraction_on_fail=0.10,
    )
    work = RemainingWorkSpec(
        remaining_bytes=10 * GIB,
        remaining_rows=5_000_000,
        remaining_ranges=40,
    )

    transition = TransitionPlanSpec(
        checkpoint=CheckpointGranularitySpec(
            granularity=CheckpointGranularity.CHUNK_RANGE,
            checkpoint_interval_bytes=128 * MIB,
            uncommitted_bytes=96 * MIB,
            checkpoint_commit_latency_sec=0.5,
            wait_for_next_checkpoint=False,
        ),
        spool=SpoolDrainSpec(
            spooled_bytes=64 * MIB,
            spooled_batches=20,
            drain_rate_bytes_per_sec=32 * MIB,
            per_batch_ack_sec=0.01,
            flush_overhead_sec=0.3,
            discard_instead_of_drain=False,
        ),
        barrier=TransactionBarrierSpec(
            open_transactions=3,
            oldest_tx_remaining_sec=2.0,
            fence_round_trips=4,
            rtt_sec=0.025,
            quiesce_lock_wait_sec=0.4,
            barrier_timeout_sec=30.0,
        ),
        setup=ProcessSetupSpec(
            process_spawn_sec=1.2,
            worker_count=4,
            per_worker_warmup_sec=0.5,
            connection_handshake_sec=0.3,
            snapshot_export_import_sec=1.0,
        ),
        restore=ResourceRestoreSpec(
            dirty_target_bytes=128 * MIB,
            rollback_rate_bytes_per_sec=64 * MIB,
            index_or_constraint_reset_sec=1.5,
            session_tuning_restore_sec=0.25,
            slot_or_lease_release_sec=0.25,
            challenger_failure_prob=0.05,
            rollback_to_incumbent_sec=3.0,
        ),
        proof=ProofRegenerationSpec(
            invalidated_proof_bytes=200 * MIB,
            invalidated_ranges=10,
            rehash_rate_bytes_per_sec=100 * MIB,
            merkle_tree_rebuild_sec=0.8,
            per_range_verify_overhead_sec=0.05,
        ),
    )

    bd = price_transition_and_rollback(incumbent, challenger, transition, work)

    # Verify each of the 6 components is strictly positive and matches formula
    assert bd.checkpoint_replay_bytes == 96 * MIB
    assert bd.spool_discard_replay_bytes == 0.0
    assert bd.total_replay_bytes == 96 * MIB
    assert bd.checkpoint_cost_sec == pytest.approx(
        (96 * MIB) * challenger.seconds_per_byte_p90() + 0.5
    )
    assert bd.spool_drain_cost_sec == pytest.approx(2.0 + 0.2 + 0.3)
    assert bd.transaction_barrier_cost_sec == pytest.approx(2.0 + 0.1 + 0.4)
    assert bd.process_setup_cost_sec == pytest.approx(1.2 + 4 * 0.5 + 0.3 + 1.0 + 1.5)
    assert bd.resource_restore_cost_sec == pytest.approx(2.0 + 1.5 + 0.25 + 0.25)
    assert bd.proof_regeneration_cost_sec == pytest.approx(2.0 + 0.8 + 10 * 0.05)
    assert bd.direct_transition_cost_sec == pytest.approx(
        bd.checkpoint_cost_sec
        + bd.spool_drain_cost_sec
        + bd.transaction_barrier_cost_sec
        + bd.process_setup_cost_sec
        + bd.resource_restore_cost_sec
        + bd.proof_regeneration_cost_sec
    )
    assert bd.rollback_risk_cost_sec > 0.0
    assert bd.total_transition_and_rollback_p90_sec == pytest.approx(
        bd.direct_transition_cost_sec + bd.rollback_risk_cost_sec
    )
    assert bd.barrier_within_timeout is True


def test_checkpoint_granularity_full_table_vs_fence_wait_vs_spool_discard() -> None:
    inc = PathPerformanceProfile(name="inc", mean_bytes_per_sec=50 * MIB, sd_bytes_per_sec=1 * MIB)
    chal = PathPerformanceProfile(name="chal", mean_bytes_per_sec=60 * MIB, sd_bytes_per_sec=1 * MIB)

    # Full-table checkpoint granularity forces replay of all completed_table_bytes
    full_table_spec = TransitionPlanSpec(
        checkpoint=CheckpointGranularitySpec(
            granularity=CheckpointGranularity.FULL_TABLE,
            completed_table_bytes=5 * GIB,
            uncommitted_bytes=100 * MIB,
        ),
        spool=SpoolDrainSpec(
            spooled_bytes=128 * MIB,
            discard_instead_of_drain=True,
            flush_overhead_sec=0.05,
        ),
    )
    bd_full = price_transition_and_rollback(inc, chal, full_table_spec)
    assert bd_full.checkpoint_replay_bytes == 5 * GIB
    assert bd_full.spool_discard_replay_bytes == 128 * MIB
    assert bd_full.total_replay_bytes == 5 * GIB + 128 * MIB
    assert bd_full.spool_drain_cost_sec == pytest.approx(0.05)

    # Waiting for the next chunk fence eliminates replay bytes and pays fence wait on incumbent
    fence_spec = TransitionPlanSpec(
        checkpoint=CheckpointGranularitySpec(
            granularity=CheckpointGranularity.CHUNK_RANGE,
            checkpoint_interval_bytes=100 * MIB,
            uncommitted_bytes=75 * MIB,
            wait_for_next_checkpoint=True,
            checkpoint_commit_latency_sec=0.2,
        )
    )
    bd_fence = price_transition_and_rollback(inc, chal, fence_spec)
    assert bd_fence.checkpoint_replay_bytes == 0.0
    assert bd_fence.checkpoint_fence_wait_bytes == 25 * MIB
    assert bd_fence.checkpoint_cost_sec == pytest.approx(
        (25 * MIB) * inc.seconds_per_byte_p90() + 0.2
    )


# ---------------------------------------------------------------------------
# 3. Acceptance Proof: Marginally Faster Path with Expensive Replay Never Steals Job
# ---------------------------------------------------------------------------


def test_marginally_faster_path_with_expensive_replay_never_steals_job() -> None:
    """Acceptance criterion: A marginally faster path with expensive replay never steals the job."""
    incumbent = PathPerformanceProfile(
        name="range_cursor_incumbent",
        said="the range-checkpointed copier",
        mean_bytes_per_sec=100 * MIB,
        sd_bytes_per_sec=3 * MIB,
        local_instantaneous_bytes_per_sec=100 * MIB,
        sample_count=12,
        validated=True,
        failure_prob=0.01,
    )
    # Challenger is 12% faster on mean throughput (and 80% faster in local instantaneous burst!),
    # but switching requires replaying a monolithic table copy + spool drain + proof regeneration.
    challenger = PathPerformanceProfile(
        name="monolithic_stream_challenger",
        said="the uncheckpointed bulk stream",
        mean_bytes_per_sec=112 * MIB,
        sd_bytes_per_sec=4 * MIB,
        local_instantaneous_bytes_per_sec=180 * MIB,
        sample_count=8,
        validated=True,
        startup_sec=4.0,
        failure_prob=0.04,
    )
    # 4 GiB already copied, 2 GiB remaining
    work = RemainingWorkSpec(
        remaining_bytes=2 * GIB,
        remaining_rows=1_000_000,
        remaining_ranges=8,
        completed_bytes=4 * GIB,
    )
    expensive_replay_transition = TransitionPlanSpec(
        checkpoint=CheckpointGranularitySpec(
            granularity=CheckpointGranularity.FULL_TABLE,
            completed_table_bytes=4 * GIB,
            checkpoint_commit_latency_sec=1.0,
        ),
        spool=SpoolDrainSpec(
            spooled_bytes=256 * MIB,
            spooled_batches=32,
            drain_rate_bytes_per_sec=64 * MIB,
        ),
        barrier=TransactionBarrierSpec(
            open_transactions=2,
            oldest_tx_remaining_sec=3.0,
            quiesce_lock_wait_sec=1.0,
        ),
        setup=ProcessSetupSpec(
            process_spawn_sec=2.0,
            worker_count=4,
            per_worker_warmup_sec=0.5,
            snapshot_export_import_sec=2.0,
        ),
        restore=ResourceRestoreSpec(
            dirty_target_bytes=512 * MIB,
            rollback_rate_bytes_per_sec=128 * MIB,
            index_or_constraint_reset_sec=2.0,
            challenger_failure_prob=0.05,
            rollback_to_incumbent_sec=4.0,
        ),
        proof=ProofRegenerationSpec(
            invalidated_proof_bytes=4 * GIB,
            invalidated_ranges=16,
            rehash_rate_bytes_per_sec=256 * MIB,
            merkle_tree_rebuild_sec=1.5,
        ),
    )

    controller = IncumbentSwitchController(
        incumbent=incumbent,
        policy=HysteresisPolicy(
            min_relative_p90_savings=0.10,
            min_absolute_p90_savings_sec=5.0,
            required_sustained_windows=3,
            min_dwell_windows=1,
        ),
    )

    # Even across 10 consecutive evaluation windows, challenger NEVER steals the job
    for _ in range(10):
        ev = controller.evaluate(challenger, work, expensive_replay_transition)
        assert ev.switched is False
        assert ev.active_path_after == "range_cursor_incumbent"
        assert ev.verdict == SwitchVerdict.REFUSE_NEGATIVE_P90_SAVINGS
        assert ev.badge == CareBadge.PASS
        assert ev.decide_rung_name == "retain_incumbent"
        # Local instantaneous speedup looks tempting (1.8x), and raw execution-only p90 is lower,
        # but total p90 with replay + transition + rollback is much slower than incumbent!
        assert ev.local_instantaneous_speedup_ratio == pytest.approx(1.8)
        assert ev.challenger_exec_only_p90_sec < ev.incumbent_remaining_p90_sec
        assert ev.challenger_total_p90_sec > ev.incumbent_remaining_p90_sec
        assert ev.net_p90_savings_sec < 0.0


# ---------------------------------------------------------------------------
# 4. p90 Remaining Work vs Local Instantaneous Throughput & Unvalidated Challenger
# ---------------------------------------------------------------------------


def test_noisy_or_unvalidated_challenger_rejected_despite_high_instantaneous_rate() -> None:
    incumbent = PathPerformanceProfile(
        name="steady_incumbent",
        mean_bytes_per_sec=80 * MIB,
        sd_bytes_per_sec=2 * MIB,
        local_instantaneous_bytes_per_sec=80 * MIB,
        sample_count=15,
        validated=True,
    )
    # Noisy challenger has higher mean (95 MB/s) and huge instantaneous spike (250 MB/s),
    # but wide variance (sd=35 MB/s on 3 samples) makes its p90 slower than incumbent!
    noisy_challenger = PathPerformanceProfile(
        name="noisy_challenger",
        mean_bytes_per_sec=95 * MIB,
        sd_bytes_per_sec=35 * MIB,
        local_instantaneous_bytes_per_sec=250 * MIB,
        sample_count=3,
        validated=True,
    )
    work = RemainingWorkSpec(remaining_bytes=8 * GIB, remaining_rows=2_000_000)
    controller = IncumbentSwitchController(incumbent=incumbent)

    ev_noisy = controller.evaluate(noisy_challenger, work)
    assert ev_noisy.switched is False
    assert ev_noisy.local_instantaneous_speedup_ratio > 3.0
    assert ev_noisy.challenger_exec_only_p90_sec > ev_noisy.incumbent_remaining_p90_sec
    assert ev_noisy.verdict == SwitchVerdict.REFUSE_NEGATIVE_P90_SAVINGS

    # Unvalidated challenger is refused even if its numbers look fast
    unvalidated_challenger = PathPerformanceProfile(
        name="unvalidated_fast",
        mean_bytes_per_sec=200 * MIB,
        sd_bytes_per_sec=5 * MIB,
        sample_count=5,
        validated=False,
    )
    ev_unval = controller.evaluate(unvalidated_challenger, work)
    assert ev_unval.switched is False
    assert ev_unval.verdict == SwitchVerdict.REFUSE_UNVALIDATED_CHALLENGER
    assert ev_unval.badge == CareBadge.BENIGN


def test_integration_with_costmodel_and_jointplan_way() -> None:
    rungs = (
        decide.Rung("native_copy", "migkit native copy", native_copier=True),
        decide.Rung("direct_bulk", "direct bulk stream", external=True),
    )
    cm = costmodel.CostModel(
        work="orders",
        rungs=rungs,
        pair="postgres->postgres",
        unit={"rows": 100_000, "bytes": 100 * MIB},
        knowledge=False,
    )
    for _ in range(6):
        cm.saw("native_copy", 100_000, 2.0, {"rows": 100_000, "bytes": 100 * MIB})
        cm.saw("direct_bulk", 100_000, 0.8, {"rows": 100_000, "bytes": 100 * MIB})

    work = RemainingWorkSpec(remaining_bytes=5 * GIB, remaining_rows=5_000_000)
    inc_prof = PathPerformanceProfile.from_costmodel("native_copy", cm, work)
    chal_prof = PathPerformanceProfile.from_costmodel("direct_bulk", cm, work)

    assert inc_prof.validated is True
    assert chal_prof.validated is True
    assert chal_prof.p90_bytes_per_sec() > inc_prof.p90_bytes_per_sec()

    way = jointplan.Way(
        name="server_to_server",
        said="server to server stream",
        per_stream=120 * MIB,
        startup=1.0,
        spread=0.05,
        truth="measured",
        risk=0.02,
        lost=0.1,
    )
    way_prof = PathPerformanceProfile.from_jointplan_way(way, sample_count=8)
    assert way_prof.validated is True
    assert way_prof.mean_bytes_per_sec == pytest.approx(120 * MIB)


# ---------------------------------------------------------------------------
# 5. Acceptance Proof: Sustained Changes Cross Reproducible Break-Even Without Oscillation
# ---------------------------------------------------------------------------


def test_sustained_changes_cross_reproducible_break_even_without_oscillation() -> None:
    """Acceptance criterion: Sustained changes cross a reproducible break-even threshold without oscillation."""
    incumbent = PathPerformanceProfile(
        name="incumbent_v1",
        said="standard batched copy",
        mean_bytes_per_sec=50 * MIB,
        sd_bytes_per_sec=1.5 * MIB,
        sample_count=20,
        validated=True,
        failure_prob=0.01,
    )
    challenger = PathPerformanceProfile(
        name="challenger_v2",
        said="parallel direct pipeline",
        mean_bytes_per_sec=100 * MIB,
        sd_bytes_per_sec=3.0 * MIB,
        sample_count=20,
        validated=True,
        startup_sec=1.0,
        failure_prob=0.02,
        lost_fraction_on_fail=0.05,
    )
    transition = TransitionPlanSpec(
        checkpoint=CheckpointGranularitySpec(
            granularity=CheckpointGranularity.CHUNK_RANGE,
            checkpoint_interval_bytes=64 * MIB,
            uncommitted_bytes=32 * MIB,
            checkpoint_commit_latency_sec=0.5,
        ),
        spool=SpoolDrainSpec(spooled_bytes=32 * MIB, drain_rate_bytes_per_sec=64 * MIB),
        barrier=TransactionBarrierSpec(
            open_transactions=1, oldest_tx_remaining_sec=0.5, quiesce_lock_wait_sec=0.2
        ),
        setup=ProcessSetupSpec(process_spawn_sec=1.0, worker_count=2, per_worker_warmup_sec=0.25),
        restore=ResourceRestoreSpec(
            dirty_target_bytes=32 * MIB,
            rollback_rate_bytes_per_sec=64 * MIB,
            challenger_failure_prob=0.02,
            rollback_to_incumbent_sec=2.0,
        ),
        proof=ProofRegenerationSpec(
            invalidated_proof_bytes=64 * MIB,
            invalidated_ranges=4,
            rehash_rate_bytes_per_sec=128 * MIB,
        ),
    )
    policy = HysteresisPolicy(
        min_relative_p90_savings=0.15,
        min_absolute_p90_savings_sec=6.0,
        required_sustained_windows=3,
        min_dwell_windows=2,
        reversal_penalty_multiplier=1.5,
    )

    probe_work = RemainingWorkSpec(remaining_bytes=1 * GIB, remaining_rows=500_000)
    be_a = compute_break_even(incumbent, challenger, transition, probe_work, policy)
    be_b = compute_break_even(incumbent, challenger, transition, probe_work, policy)
    assert be_a.can_break_even is True
    assert be_a.break_even_remaining_bytes == pytest.approx(be_b.break_even_remaining_bytes)
    threshold_bytes = be_a.break_even_remaining_bytes
    assert threshold_bytes > 0

    controller = IncumbentSwitchController(incumbent=incumbent, policy=policy)

    # 1) Just below the reproducible break-even remaining bytes: never switches
    below_work = RemainingWorkSpec(remaining_bytes=threshold_bytes * 0.92, remaining_rows=100_000)
    for _ in range(5):
        ev_below = controller.evaluate(challenger, below_work, transition)
        assert ev_below.switched is False
        assert ev_below.verdict == SwitchVerdict.REFUSE_NEGATIVE_P90_SAVINGS
        assert controller.incumbent.name == "incumbent_v1"

    # 2) Above the break-even threshold, a transient 2-window improvement interrupted by
    #    a dip on window 3 resets the hysteresis streak and does NOT switch.
    above_work = RemainingWorkSpec(remaining_bytes=threshold_bytes * 1.50, remaining_rows=400_000)
    ev1 = controller.evaluate(challenger, above_work, transition)
    assert ev1.switched is False
    assert ev1.verdict == SwitchVerdict.DEFER_HYSTERESIS
    assert ev1.sustained_window_count == 1

    ev2 = controller.evaluate(challenger, above_work, transition)
    assert ev2.switched is False
    assert ev2.verdict == SwitchVerdict.DEFER_HYSTERESIS
    assert ev2.sustained_window_count == 2

    # Transient regression on window 3 resets streak
    slumped_challenger = PathPerformanceProfile(
        name="challenger_v2",
        mean_bytes_per_sec=48 * MIB,
        sd_bytes_per_sec=5 * MIB,
        sample_count=20,
        validated=True,
    )
    ev_dip = controller.evaluate(slumped_challenger, above_work, transition)
    assert ev_dip.switched is False
    assert ev_dip.sustained_window_count == 0

    # 3) Now sustain the improvement across all 3 required windows:
    #    Windows 1 & 2 defer under hysteresis; Window 3 crosses cleanly and switches!
    for expected_streak in (1, 2):
        ev_wait = controller.evaluate(challenger, above_work, transition)
        assert ev_wait.switched is False
        assert ev_wait.verdict == SwitchVerdict.DEFER_HYSTERESIS
        assert ev_wait.sustained_window_count == expected_streak

    ev_switch = controller.evaluate(challenger, above_work, transition)
    assert ev_switch.switched is True
    assert ev_switch.verdict == SwitchVerdict.SWITCH_TO_CHALLENGER
    assert ev_switch.badge == CareBadge.PASS
    assert ev_switch.active_path_after == "challenger_v2"
    assert ev_switch.decide_rung_name == "checkpoint_fenced_switch"
    assert controller.incumbent.name == "challenger_v2"

    # 4) Anti-oscillation after switch:
    #    Even if the old incumbent (`incumbent_v1`) temporarily speeds up,
    #    dwell window (`min_dwell_windows=2`) and `reversal_penalty_multiplier=1.5`
    #    prevent ping-pong oscillation back to `incumbent_v1`.
    improved_old_incumbent = PathPerformanceProfile(
        name="incumbent_v1",
        mean_bytes_per_sec=135 * MIB,
        sd_bytes_per_sec=3 * MIB,
        sample_count=20,
        validated=True,
    )
    ev_after_1 = controller.evaluate(improved_old_incumbent, above_work, transition)
    assert ev_after_1.switched is False
    assert controller.incumbent.name == "challenger_v2"
    assert len(controller.switch_history) == 1


# ---------------------------------------------------------------------------
# 6. Emergency Switch on Failed Incumbent & Barrier Timeout Refusal
# ---------------------------------------------------------------------------


def test_emergency_switch_when_incumbent_fails_and_action_when_no_fallback() -> None:
    failed_incumbent = PathPerformanceProfile(
        name="broken_incumbent",
        mean_bytes_per_sec=60 * MIB,
        sd_bytes_per_sec=2 * MIB,
        validated=True,
        healthy=False,
    )
    unvalidated_fallback = PathPerformanceProfile(
        name="unvalidated_alt",
        mean_bytes_per_sec=70 * MIB,
        sd_bytes_per_sec=2 * MIB,
        validated=False,
        healthy=True,
    )
    validated_fallback = PathPerformanceProfile(
        name="validated_alt",
        mean_bytes_per_sec=55 * MIB,
        sd_bytes_per_sec=2 * MIB,
        validated=True,
        healthy=True,
    )
    work = RemainingWorkSpec(remaining_bytes=1 * GIB, remaining_rows=100_000)
    ctrl = IncumbentSwitchController(incumbent=failed_incumbent)

    # Unvalidated fallback cannot rescue failed incumbent -> ACTION badge
    ev_fail = ctrl.evaluate(unvalidated_fallback, work)
    assert ev_fail.switched is False
    assert ev_fail.verdict == SwitchVerdict.ACTION_NO_VIABLE_PATH
    assert ev_fail.badge == CareBadge.ACTION

    # Validated fallback rescues failed incumbent immediately -> REVIEW badge
    ev_rescue = ctrl.evaluate(validated_fallback, work)
    assert ev_rescue.switched is True
    assert ev_rescue.verdict == SwitchVerdict.EMERGENCY_SWITCH_INCUMBENT_FAILED
    assert ev_rescue.badge == CareBadge.REVIEW
    assert ev_rescue.decide_rung_name == "emergency_rollback_switch"
    assert ctrl.incumbent.name == "validated_alt"


def test_transaction_barrier_timeout_refuses_switch() -> None:
    inc = PathPerformanceProfile(name="inc", mean_bytes_per_sec=40 * MIB, sd_bytes_per_sec=1 * MIB)
    chal = PathPerformanceProfile(name="chal", mean_bytes_per_sec=120 * MIB, sd_bytes_per_sec=1 * MIB)
    work = RemainingWorkSpec(remaining_bytes=10 * GIB)
    long_barrier_spec = TransitionPlanSpec(
        barrier=TransactionBarrierSpec(
            open_transactions=5,
            oldest_tx_remaining_sec=45.0,
            quiesce_lock_wait_sec=10.0,
            barrier_timeout_sec=30.0,
        )
    )
    ctrl = IncumbentSwitchController(incumbent=inc)
    ev = ctrl.evaluate(chal, work, long_barrier_spec)
    assert ev.switched is False
    assert ev.verdict == SwitchVerdict.REFUSE_BARRIER_TIMEOUT
    assert ev.badge == CareBadge.REVIEW


# ---------------------------------------------------------------------------
# 7. O(1) Typed Durable Resume, Active Verification Lease & Pure UNIX Output
# ---------------------------------------------------------------------------


def test_typed_durable_checkpoint_resume_in_o1_and_controller_persistence(
    tmp_path: Path,
) -> None:
    ckpt_file = tmp_path / "switch_state.json"
    ckpt = DurableSwitchCheckpoint(ckpt_file)

    composite_cursor = (
        datetime.datetime(2026, 10, 17, 12, 30, 0, tzinfo=datetime.timezone.utc),
        datetime.date(2026, 10, 17),
        decimal.Decimal("987654321.123456"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        ("shard_a", 42000),
    )
    ckpt.mark_unit_completed("range-001", composite_cursor)
    ckpt.mark_unit_completed("range-002", ("shard_b", 84000))

    inc = PathPerformanceProfile(name="inc_a", mean_bytes_per_sec=40 * MIB, sd_bytes_per_sec=1 * MIB)
    chal = PathPerformanceProfile(name="chal_b", mean_bytes_per_sec=120 * MIB, sd_bytes_per_sec=2 * MIB)
    work = RemainingWorkSpec(remaining_bytes=5 * GIB)
    policy = HysteresisPolicy(required_sustained_windows=2, min_dwell_windows=0)

    ctrl = IncumbentSwitchController(
        incumbent=inc, policy=policy, checkpoint=ckpt, endpoint_host="10.0.0.15"
    )
    ev1 = ctrl.evaluate(chal, work)
    assert ev1.switched is False
    assert ev1.sustained_window_count == 1

    # Reload from disk in a fresh checkpoint + controller instance
    reloaded_ckpt = DurableSwitchCheckpoint(ckpt_file)
    assert reloaded_ckpt.is_completed("range-001") is True
    assert reloaded_ckpt.is_completed("range-002") is True
    assert reloaded_ckpt.is_completed("range-003") is False
    restored_cursor = reloaded_ckpt.get_cursor("range-001")
    assert restored_cursor == composite_cursor
    assert isinstance(restored_cursor[0], datetime.datetime)
    assert isinstance(restored_cursor[1], datetime.date)
    assert isinstance(restored_cursor[2], decimal.Decimal)
    assert isinstance(restored_cursor[3], uuid.UUID)
    assert isinstance(restored_cursor[4], tuple)

    ctrl_resumed = IncumbentSwitchController(
        incumbent=inc, policy=policy, checkpoint=reloaded_ckpt, endpoint_host="127.0.0.1"
    )
    # Because window 1 was persisted in checkpoint, window 2 immediately completes hysteresis!
    ev2 = ctrl_resumed.evaluate(chal, work)
    assert ev2.switched is True
    assert ev2.active_path_after == "chal_b"


def test_active_verification_lease_mutual_exclusion_and_expiry(tmp_path: Path) -> None:
    clock = [1000.0]
    with ActiveVerificationLease(
        tmp_path, owner_id="worker-1", ttl_seconds=30.0, time_fn=lambda: clock[0]
    ) as lease1:
        assert (tmp_path / ".run.lock").exists()
        # Second worker cannot acquire while lease1 is active
        lease2 = ActiveVerificationLease(
            tmp_path, owner_id="worker-2", ttl_seconds=30.0, time_fn=lambda: clock[0]
        )
        with pytest.raises(ActiveLeaseConflictError):
            lease2.acquire()

        # Advance clock by 20s and heartbeat lease1
        clock[0] += 20.0
        lease1.heartbeat()

        # Advance clock past TTL (35s later) -> lease2 can now steal expired lock
        clock[0] += 35.0
        lease2.acquire()
        with pytest.raises(ActiveLeaseConflictError):
            lease1.heartbeat()
        lease2.release()

    assert not (tmp_path / ".run.lock").exists()


def test_safe_host_validation_and_pure_ascii_unix_report() -> None:
    assert validate_safe_host("127.0.0.1") == "127.0.0.1"
    assert validate_safe_host("10.0.0.42") == "10.0.0.42"
    with pytest.raises(UnsafeEndpointHostError):
        validate_safe_host("8.8.8.8")
    with pytest.raises(UnsafeEndpointHostError):
        validate_safe_host("")
    with pytest.raises(UnvalidatedIncumbentError):
        IncumbentSwitchController(
            incumbent=PathPerformanceProfile(name="unval", validated=False, healthy=True)
        )
    with pytest.raises(InvalidTransitionSpecError):
        CheckpointGranularitySpec(uncommitted_bytes=-1.0)

    inc = PathPerformanceProfile(name="inc", mean_bytes_per_sec=50 * MIB, sd_bytes_per_sec=2 * MIB)
    chal = PathPerformanceProfile(name="chal", mean_bytes_per_sec=52 * MIB, sd_bytes_per_sec=2 * MIB)
    ctrl = IncumbentSwitchController(incumbent=inc)
    ev = ctrl.evaluate(chal, RemainingWorkSpec(remaining_bytes=1 * GIB))
    report = format_switch_report(ev)

    assert report.isascii()
    assert "[PASS]" in report
    assert "replan_verdict=REFUSE_NEGATIVE_P90_SAVINGS" in report
    assert "transition_components:" in report

    source_text = Path(incumbent_switch.__file__).read_text(encoding="utf-8")
    assert source_text.isascii()
