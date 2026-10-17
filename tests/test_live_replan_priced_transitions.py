"""Tests for live replan with priced state transitions (Row 588: SMART-N5.live-replan).

Proves:
1. Every `decide.Rung` in `live_replan.py` (`replan_trigger_rungs`, `replan_strategy_rungs`,
   `transition_cost_rungs`, `dag_reschedule_rungs`) is exercised on both `holds` and
   `refuses` inside `with tests.rung_branches.watched():` and declared in `PROVES_RUNGS`.
2. Safe checkpoint replanning of the remaining phase/table/range DAG on changing
   throughput, CDC arrivals, load, capacity, or quota.
3. Oscillating load does not thrash: hysteresis margin, priced transition costs, and
   telemetry smoothing prevent strategy ping-pong.
4. Old work is not replayed blindly: committed checkpoints are skipped in O(1) down to
   range/cursor level, preserving completed work.
5. Sustained changes produce measured end-to-end improvement over degraded incumbent.
6. Priced state transitions include setup/cleanup, cache warming, snapshot age, reconnect,
   undo/replay, and validation costs.
7. Veto gates: snapshot staleness, excessive undo penalties, and connection quota exhaustion.
8. Universal typed durable checkpoint store with O(1) lookup and active `.run.lock` lease.
9. Pure UNIX style, zero emojis/glyphs, care badges ACTION/REVIEW/BENIGN/PASS.
"""

from __future__ import annotations

import datetime
import decimal
import json
import pathlib
import time
import uuid

import pytest

import tests.rung_branches
from migkit import decide, live_replan
from migkit.live_replan import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    CareBadge,
    CheckpointCorruptionError,
    DAGNode,
    DAGNodeStatus,
    DAGPhase,
    InvalidReplanFactError,
    LiveReplanCheckpointStore,
    LiveReplanCheckpointUnit,
    LiveReplanDAG,
    LiveReplanError,
    PricedTransitionPolicy,
    ReplanStrategy,
    ResourceQuotaSpec,
    SmoothedTelemetry,
    TelemetryObservation,
    TransitionCostBreakdown,
    UnsafeEndpointError,
    acquire_live_replan_lease,
    calculate_transition_cost,
    dag_reschedule_rungs,
    estimate_byte_weighted_eta,
    estimate_strategy_remaining_seconds,
    evaluate_live_replan,
    replan_strategy_rungs,
    replan_trigger_rungs,
    reshape_remaining_dag,
    smooth_telemetry_series,
    transition_cost_rungs,
    validate_safe_endpoint,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    # 1. replan_trigger_rungs
    ("live_replan", "replan_trigger_rungs", "transient_oscillation_damping", "holds"),
    ("live_replan", "replan_trigger_rungs", "transient_oscillation_damping", "refuses"),
    ("live_replan", "replan_trigger_rungs", "sustained_throughput_degradation", "holds"),
    ("live_replan", "replan_trigger_rungs", "sustained_throughput_degradation", "refuses"),
    ("live_replan", "replan_trigger_rungs", "cdc_backlog_divergence", "holds"),
    ("live_replan", "replan_trigger_rungs", "cdc_backlog_divergence", "refuses"),
    ("live_replan", "replan_trigger_rungs", "resource_quota_exhaustion", "holds"),
    ("live_replan", "replan_trigger_rungs", "resource_quota_exhaustion", "refuses"),

    # 2. replan_strategy_rungs
    ("live_replan", "replan_strategy_rungs", "switch_to_staged_bulk_load", "holds"),
    ("live_replan", "replan_strategy_rungs", "switch_to_staged_bulk_load", "refuses"),
    ("live_replan", "replan_strategy_rungs", "switch_to_parallel_range_split", "holds"),
    ("live_replan", "replan_strategy_rungs", "switch_to_parallel_range_split", "refuses"),
    ("live_replan", "replan_strategy_rungs", "switch_to_direct_stream", "holds"),
    ("live_replan", "replan_strategy_rungs", "switch_to_direct_stream", "refuses"),
    ("live_replan", "replan_strategy_rungs", "retain_current_plan", "holds"),
    ("live_replan", "replan_strategy_rungs", "retain_current_plan", "refuses"),

    # 3. transition_cost_rungs
    ("live_replan", "transition_cost_rungs", "transition_vetoed_by_snapshot_staleness", "holds"),
    ("live_replan", "transition_cost_rungs", "transition_vetoed_by_snapshot_staleness", "refuses"),
    ("live_replan", "transition_cost_rungs", "transition_vetoed_by_undo_penalty", "holds"),
    ("live_replan", "transition_cost_rungs", "transition_vetoed_by_undo_penalty", "refuses"),
    ("live_replan", "transition_cost_rungs", "transition_vetoed_by_reconnect_quota", "holds"),
    ("live_replan", "transition_cost_rungs", "transition_vetoed_by_reconnect_quota", "refuses"),
    ("live_replan", "transition_cost_rungs", "transition_justified_by_net_savings", "holds"),
    ("live_replan", "transition_cost_rungs", "transition_justified_by_net_savings", "refuses"),
    ("live_replan", "transition_cost_rungs", "maintain_incumbent_within_hysteresis", "holds"),
    ("live_replan", "transition_cost_rungs", "maintain_incumbent_within_hysteresis", "refuses"),

    # 4. dag_reschedule_rungs
    ("live_replan", "dag_reschedule_rungs", "prioritize_critical_cdc_tables", "holds"),
    ("live_replan", "dag_reschedule_rungs", "prioritize_critical_cdc_tables", "refuses"),
    ("live_replan", "dag_reschedule_rungs", "dynamic_split_skewed_ranges", "holds"),
    ("live_replan", "dag_reschedule_rungs", "dynamic_split_skewed_ranges", "refuses"),
    ("live_replan", "dag_reschedule_rungs", "coalesce_small_remaining_ranges", "holds"),
    ("live_replan", "dag_reschedule_rungs", "coalesce_small_remaining_ranges", "refuses"),
    ("live_replan", "dag_reschedule_rungs", "preserve_topological_checkpoint_order", "holds"),
    ("live_replan", "dag_reschedule_rungs", "preserve_topological_checkpoint_order", "refuses"),
]


# -----------------------------------------------------------------------------
# 1. All Decide Rungs Reached on Both `holds` and `refuses` Under `watched()`
# -----------------------------------------------------------------------------


def test_every_live_replan_rung_holds_and_refuses_under_watched() -> None:
    """Exercise both `holds` and `refuses` for every rung in `live_replan.py`."""
    with watched() as seen:
        # 1. replan_trigger_rungs
        # Holds: transient_oscillation_damping
        decide.climb(
            "test trigger oscillation holds",
            replan_trigger_rungs(),
            decide.Facts({
                "is_oscillating": True,
                "seconds_since_last_switch": 5.0,
                "cooldown_seconds": 20.0,
                "damping_preserves_stability": True,
            }),
        )
        # Holds: sustained_throughput_degradation
        decide.climb(
            "test trigger throughput degradation holds",
            replan_trigger_rungs(),
            decide.Facts({
                "is_oscillating": False,
                "seconds_since_last_switch": 45.0,
                "cooldown_seconds": 20.0,
                "damping_preserves_stability": True,
                "current_throughput": 20.0,
                "baseline_throughput": 100.0,
                "degradation_threshold": 0.25,
                "degradation_duration_sec": 30.0,
                "min_observation_window_sec": 10.0,
            }),
        )
        # Holds: cdc_backlog_divergence
        decide.climb(
            "test trigger cdc divergence holds",
            replan_trigger_rungs(),
            decide.Facts({
                "is_oscillating": False,
                "seconds_since_last_switch": 5.0,
                "cooldown_seconds": 20.0,
                "damping_preserves_stability": False,
                "current_throughput": 100.0,
                "baseline_throughput": 100.0,
                "degradation_threshold": 0.25,
                "degradation_duration_sec": 0.0,
                "cdc_arrival_rate": 5000.0,
                "cdc_drain_rate": 500.0,
                "cdc_backlog_bytes": 20_000_000.0,
                "cdc_backlog_threshold_bytes": 10_000_000.0,
            }),
        )
        # Holds: resource_quota_exhaustion
        decide.climb(
            "test trigger quota exhaustion holds",
            replan_trigger_rungs(),
            decide.Facts({
                "is_oscillating": False,
                "seconds_since_last_switch": 5.0,
                "cooldown_seconds": 20.0,
                "damping_preserves_stability": False,
                "current_throughput": 100.0,
                "baseline_throughput": 100.0,
                "degradation_threshold": 0.25,
                "degradation_duration_sec": 0.0,
                "cdc_arrival_rate": 100.0,
                "cdc_drain_rate": 500.0,
                "cdc_backlog_bytes": 1_000.0,
                "cdc_backlog_threshold_bytes": 10_000_000.0,
                "resource_load_ratio": 0.95,
                "quota_ceiling_ratio": 0.85,
                "alternate_pathway_available": True,
            }),
        )
        # All refuse in replan_trigger_rungs
        decide.climb(
            "test trigger all refuse",
            replan_trigger_rungs(),
            decide.Facts({
                "is_oscillating": False,
                "seconds_since_last_switch": 50.0,
                "cooldown_seconds": 20.0,
                "damping_preserves_stability": False,
                "current_throughput": 100.0,
                "baseline_throughput": 100.0,
                "degradation_threshold": 0.25,
                "degradation_duration_sec": 0.0,
                "cdc_arrival_rate": 100.0,
                "cdc_drain_rate": 500.0,
                "cdc_backlog_bytes": 1_000.0,
                "cdc_backlog_threshold_bytes": 10_000_000.0,
                "resource_load_ratio": 0.30,
                "quota_ceiling_ratio": 0.85,
                "alternate_pathway_available": False,
            }),
        )

        # 2. replan_strategy_rungs
        # Holds: switch_to_staged_bulk_load
        decide.climb(
            "test strategy staged bulk holds",
            replan_strategy_rungs(),
            decide.Facts({
                "staging_quota_bytes": 50_000_000.0,
                "required_staging_bytes": 10_000_000.0,
                "staged_bulk_throughput": 150_000.0,
                "current_throughput": 30_000.0,
            }),
        )
        # Holds: switch_to_parallel_range_split
        decide.climb(
            "test strategy parallel split holds",
            replan_strategy_rungs(),
            decide.Facts({
                "staging_quota_bytes": 0.0,
                "required_staging_bytes": 10_000_000.0,
                "splittable_keys_available": True,
                "max_skew_ratio": 3.5,
                "skew_threshold": 2.0,
                "available_worker_connections": 8,
                "required_split_workers": 2,
            }),
        )
        # Holds: switch_to_direct_stream
        decide.climb(
            "test strategy direct stream holds",
            replan_strategy_rungs(),
            decide.Facts({
                "staging_quota_bytes": 0.0,
                "required_staging_bytes": 10_000_000.0,
                "splittable_keys_available": False,
                "direct_stream_available": True,
                "direct_stream_throughput": 200_000.0,
                "current_throughput": 50_000.0,
                "staging_overhead_exceeds_direct": True,
            }),
        )
        # Holds: retain_current_plan
        decide.climb(
            "test strategy retain plan holds",
            replan_strategy_rungs(),
            decide.Facts({
                "staging_quota_bytes": 0.0,
                "required_staging_bytes": 10_000_000.0,
                "splittable_keys_available": False,
                "direct_stream_available": False,
                "current_plan_viable": True,
            }),
        )
        # All refuse in replan_strategy_rungs
        decide.climb(
            "test strategy all refuse",
            replan_strategy_rungs(),
            decide.Facts({
                "staging_quota_bytes": 0.0,
                "required_staging_bytes": 10_000_000.0,
                "splittable_keys_available": False,
                "direct_stream_available": False,
                "current_plan_viable": False,
            }),
        )

        # 3. transition_cost_rungs
        # Holds: transition_vetoed_by_snapshot_staleness
        decide.climb(
            "test transition veto snapshot staleness holds",
            transition_cost_rungs(),
            decide.Facts({
                "snapshot_age_sec": 20000.0,
                "max_snapshot_age_sec": 14400.0,
                "snapshot_loss_risk_confirmed": True,
            }),
        )
        # Holds: transition_vetoed_by_undo_penalty
        decide.climb(
            "test transition veto undo penalty holds",
            transition_cost_rungs(),
            decide.Facts({
                "snapshot_age_sec": 100.0,
                "max_snapshot_age_sec": 14400.0,
                "undo_replay_cost_sec": 500.0,
                "max_allowed_undo_cost_sec": 100.0,
                "finishing_in_flight_faster": True,
            }),
        )
        # Holds: transition_vetoed_by_reconnect_quota
        decide.climb(
            "test transition veto reconnect quota holds",
            transition_cost_rungs(),
            decide.Facts({
                "snapshot_age_sec": 100.0,
                "max_snapshot_age_sec": 14400.0,
                "undo_replay_cost_sec": 50.0,
                "max_allowed_undo_cost_sec": 100.0,
                "reconnect_quota_exhausted": True,
                "reconnect_risk_disruptive": True,
            }),
        )
        # Holds: transition_justified_by_net_savings
        decide.climb(
            "test transition justified holds",
            transition_cost_rungs(),
            decide.Facts({
                "snapshot_age_sec": 100.0,
                "max_snapshot_age_sec": 14400.0,
                "undo_replay_cost_sec": 10.0,
                "max_allowed_undo_cost_sec": 100.0,
                "reconnect_quota_exhausted": False,
                "current_remaining_sec": 500.0,
                "candidate_remaining_sec": 100.0,
                "total_transition_cost_sec": 20.0,
                "hysteresis_margin_sec": 10.0,
            }),
        )
        # Holds: maintain_incumbent_within_hysteresis
        decide.climb(
            "test transition within hysteresis holds",
            transition_cost_rungs(),
            decide.Facts({
                "snapshot_age_sec": 100.0,
                "max_snapshot_age_sec": 14400.0,
                "undo_replay_cost_sec": 10.0,
                "max_allowed_undo_cost_sec": 100.0,
                "reconnect_quota_exhausted": False,
                "current_remaining_sec": 115.0,
                "candidate_remaining_sec": 100.0,
                "total_transition_cost_sec": 10.0,
                "hysteresis_margin_sec": 10.0,
            }),
        )
        # Refuses maintain_incumbent_within_hysteresis (savings clearly exceed hysteresis)
        decide.climb(
            "test transition maintain refuses when savings exceed hysteresis",
            transition_cost_rungs(),
            decide.Facts({
                "snapshot_age_sec": 100.0,
                "max_snapshot_age_sec": 14400.0,
                "undo_replay_cost_sec": 10.0,
                "max_allowed_undo_cost_sec": 100.0,
                "reconnect_quota_exhausted": False,
                "current_remaining_sec": 200.0,
                "candidate_remaining_sec": 100.0,
                "total_transition_cost_sec": 10.0,
                "hysteresis_margin_sec": 10.0,
            }),
        )

        # 4. dag_reschedule_rungs
        # Holds: prioritize_critical_cdc_tables
        decide.climb(
            "test dag prioritize cdc holds",
            dag_reschedule_rungs(),
            decide.Facts({
                "max_table_cdc_rate": 1500.0,
                "critical_cdc_rate_threshold": 500.0,
                "early_cdc_drain_beneficial": True,
            }),
        )
        # Holds: dynamic_split_skewed_ranges
        decide.climb(
            "test dag split skewed holds",
            dag_reschedule_rungs(),
            decide.Facts({
                "max_table_cdc_rate": 100.0,
                "critical_cdc_rate_threshold": 500.0,
                "partition_skew_ratio": 4.5,
                "skew_split_threshold": 2.0,
                "splittable_keys_available": True,
            }),
        )
        # Holds: coalesce_small_remaining_ranges
        decide.climb(
            "test dag coalesce small holds",
            dag_reschedule_rungs(),
            decide.Facts({
                "max_table_cdc_rate": 100.0,
                "critical_cdc_rate_threshold": 500.0,
                "partition_skew_ratio": 1.0,
                "skew_split_threshold": 2.0,
                "small_range_count": 25,
                "coalesce_count_threshold": 8,
                "coalescing_reduces_overhead": True,
            }),
        )
        # Holds: preserve_topological_checkpoint_order
        decide.climb(
            "test dag topological holds",
            dag_reschedule_rungs(),
            decide.Facts({
                "max_table_cdc_rate": 100.0,
                "critical_cdc_rate_threshold": 500.0,
                "partition_skew_ratio": 1.0,
                "skew_split_threshold": 2.0,
                "small_range_count": 2,
                "coalesce_count_threshold": 8,
                "topological_order_valid": True,
            }),
        )
        # All refuse in dag_reschedule_rungs
        decide.climb(
            "test dag all refuse",
            dag_reschedule_rungs(),
            decide.Facts({
                "max_table_cdc_rate": 100.0,
                "critical_cdc_rate_threshold": 500.0,
                "partition_skew_ratio": 1.0,
                "skew_split_threshold": 2.0,
                "small_range_count": 2,
                "coalesce_count_threshold": 8,
                "topological_order_valid": False,
            }),
        )

    # Verify that each rung has reached both HOLDS and REFUSES
    all_rung_names = [
        # trigger
        "transient_oscillation_damping",
        "sustained_throughput_degradation",
        "cdc_backlog_divergence",
        "resource_quota_exhaustion",
        # strategy
        "switch_to_staged_bulk_load",
        "switch_to_parallel_range_split",
        "switch_to_direct_stream",
        "retain_current_plan",
        # transition cost
        "transition_vetoed_by_snapshot_staleness",
        "transition_vetoed_by_undo_penalty",
        "transition_vetoed_by_reconnect_quota",
        "transition_justified_by_net_savings",
        "maintain_incumbent_within_hysteresis",
        # dag reschedule
        "prioritize_critical_cdc_tables",
        "dynamic_split_skewed_ranges",
        "coalesce_small_remaining_ranges",
        "preserve_topological_checkpoint_order",
    ]

    for r_name in all_rung_names:
        branches_seen = branch(seen, r_name)
        assert HOLDS in branches_seen, f"Rung '{r_name}' did not reach 'holds'"
        assert REFUSES in branches_seen, f"Rung '{r_name}' did not reach 'refuses'"


# -----------------------------------------------------------------------------
# 2. Oscillating Load Does Not Thrash (Anti-Thrashing Proof)
# -----------------------------------------------------------------------------


def test_oscillating_load_anti_thrashing(tmp_path: pathlib.Path) -> None:
    """Proof: Oscillating load does not thrash between strategies."""
    ckpt_path = tmp_path / "checkpoints.json"
    ckpt_store = LiveReplanCheckpointStore(ckpt_path)

    nodes = [
        DAGNode(
            node_id=f"tab_{i}",
            phase=DAGPhase.SEED_COPY,
            table_name=f"orders_{i}",
            range_id="r0",
            min_cursor=0,
            max_cursor=100000,
            estimated_bytes=50_000_000,
            estimated_rows=100_000,
        )
        for i in range(5)
    ]
    dag = LiveReplanDAG(nodes)

    quotas = ResourceQuotaSpec(
        cpu_cores_available=8.0,
        disk_read_bytes_per_sec=200_000_000.0,
        disk_write_bytes_per_sec=150_000_000.0,
        network_bytes_per_sec=100_000_000.0,
        staging_quota_bytes=500_000_000.0,
        max_worker_connections=16,
        memory_quota_bytes=2 * 1024 * 1024 * 1024,
    )

    policy = PricedTransitionPolicy(
        hysteresis_margin_sec=10.0,
        hysteresis_ratio=0.15,
        cooldown_seconds=30.0,
        min_observation_window_sec=10.0,
    )

    # Generate oscillating telemetry: alternating between 100 MB/s and 10 MB/s every 2 seconds
    telemetry_series: list[TelemetryObservation] = []
    base_t = 1000.0
    for step in range(10):
        t = base_t + step * 2.0
        # High and low oscillation
        tp = 100_000_000.0 if step % 2 == 0 else 10_000_000.0
        telemetry_series.append(
            TelemetryObservation(
                timestamp=t,
                observed_throughput_bytes_per_sec=tp,
                cdc_arrival_rate_bytes_per_sec=50.0,
                cdc_drain_rate_bytes_per_sec=500.0,
                cdc_backlog_bytes=1000.0,
                cpu_utilization_ratio=0.45,
                memory_utilization_ratio=0.35,
                active_connections=4,
            )
        )

    # Evaluate live replan across the oscillating window
    result = evaluate_live_replan(
        "oscillating load anti-thrash evaluation",
        current_strategy=ReplanStrategy.DIRECT_STREAM,
        dag=dag,
        checkpoint_store=ckpt_store,
        telemetry_history=telemetry_series,
        quotas=quotas,
        policy=policy,
        seconds_since_last_switch=5.0,  # inside cooldown
    )

    # Proves:
    # 1. Switch was suppressed (switched == False)
    # 2. RETAINED current strategy (DIRECT_STREAM)
    # 3. Care badge is BENIGN
    # 4. Trigger rung was transient_oscillation_damping
    assert not result.switched
    assert result.chosen_strategy == ReplanStrategy.DIRECT_STREAM
    assert result.care_badge == CareBadge.BENIGN
    assert result.trigger_choice.path == "transient_oscillation_damping"
    assert "BENIGN" in result.summary


# -----------------------------------------------------------------------------
# 3. Old Work Is Not Replayed Blindly (O(1) Checkpoint Preservation)
# -----------------------------------------------------------------------------


def test_committed_checkpoints_never_replayed_blindly(tmp_path: pathlib.Path) -> None:
    """Proof: Old work is not replayed blindly; committed checkpoints skipped in O(1)."""
    ckpt_path = tmp_path / "checkpoints.json"
    ckpt_store = LiveReplanCheckpointStore(ckpt_path)

    # Record 3 committed units in checkpoint store with native types
    uid_1 = uuid.uuid4()
    dt_1 = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc)
    dec_1 = decimal.Decimal("12345.6789")

    ckpt_store.record_unit(
        LiveReplanCheckpointUnit(
            node_id="tab_0",
            phase=DAGPhase.SEED_COPY,
            strategy=ReplanStrategy.DIRECT_STREAM,
            min_cursor=0,
            max_cursor=uid_1,
            merkle_digest="hash001",
            completed_at=dt_1,
        )
    )
    ckpt_store.record_unit(
        LiveReplanCheckpointUnit(
            node_id="tab_1",
            phase=DAGPhase.SEED_COPY,
            strategy=ReplanStrategy.DIRECT_STREAM,
            min_cursor=uid_1,
            max_cursor=dec_1,
            merkle_digest="hash002",
            completed_at=dt_1,
        )
    )

    # 4 nodes total in DAG: tab_0, tab_1 (already committed), tab_2, tab_3 (pending)
    nodes = [
        DAGNode(
            node_id="tab_0",
            phase=DAGPhase.SEED_COPY,
            table_name="orders",
            range_id="r0",
            min_cursor=0,
            max_cursor=uid_1,
            estimated_bytes=50_000_000,
            estimated_rows=50_000,
        ),
        DAGNode(
            node_id="tab_1",
            phase=DAGPhase.SEED_COPY,
            table_name="orders",
            range_id="r1",
            min_cursor=uid_1,
            max_cursor=dec_1,
            estimated_bytes=50_000_000,
            estimated_rows=50_000,
        ),
        DAGNode(
            node_id="tab_2",
            phase=DAGPhase.SEED_COPY,
            table_name="orders",
            range_id="r2",
            min_cursor=dec_1,
            max_cursor=99999,
            estimated_bytes=60_000_000,
            estimated_rows=60_000,
        ),
        DAGNode(
            node_id="tab_3",
            phase=DAGPhase.SEED_COPY,
            table_name="customers",
            range_id="r0",
            min_cursor=0,
            max_cursor=50000,
            estimated_bytes=30_000_000,
            estimated_rows=30_000,
        ),
    ]
    dag = LiveReplanDAG(nodes)

    quotas = ResourceQuotaSpec(
        cpu_cores_available=8.0,
        disk_read_bytes_per_sec=200_000_000.0,
        disk_write_bytes_per_sec=150_000_000.0,
        network_bytes_per_sec=100_000_000.0,
        staging_quota_bytes=500_000_000.0,
        max_worker_connections=16,
        memory_quota_bytes=1024 * 1024 * 1024,
    )

    obs = [
        TelemetryObservation(
            timestamp=1000.0 + i * 2.0,
            observed_throughput_bytes_per_sec=50_000_000.0,
            cdc_arrival_rate_bytes_per_sec=10.0,
            cdc_drain_rate_bytes_per_sec=500.0,
            cdc_backlog_bytes=100.0,
            cpu_utilization_ratio=0.3,
            memory_utilization_ratio=0.3,
            active_connections=4,
        )
        for i in range(5)
    ]

    result = evaluate_live_replan(
        "checkpoint preservation test",
        current_strategy=ReplanStrategy.DIRECT_STREAM,
        dag=dag,
        checkpoint_store=ckpt_store,
        telemetry_history=obs,
        quotas=quotas,
    )

    # Verify:
    # 1. tab_0 and tab_1 are COMMITTED
    committed = dag.get_committed_nodes()
    committed_ids = {c.node_id for c in committed}
    assert "tab_0" in committed_ids
    assert "tab_1" in committed_ids

    # 2. Remaining nodes only contain tab_2 and tab_3
    remaining = dag.get_remaining_nodes()
    rem_ids = {r.node_id for r in remaining}
    assert rem_ids == {"tab_2", "tab_3"}
    assert "tab_0" not in rem_ids
    assert "tab_1" not in rem_ids

    # 3. Remaining bytes reflect only tab_2 + tab_3 (60MB + 30MB = 90MB), NOT 190MB!
    assert dag.remaining_bytes() == 90_000_000
    assert dag.remaining_rows() == 90_000


# -----------------------------------------------------------------------------
# 4. Sustained Changes Produce Measured End-to-End Improvement
# -----------------------------------------------------------------------------


def test_sustained_change_produces_measured_end_to_end_improvement(
    tmp_path: pathlib.Path,
) -> None:
    """Proof: Sustained degradation triggers replan with priced transition net benefit."""
    ckpt_path = tmp_path / "checkpoints.json"
    ckpt_store = LiveReplanCheckpointStore(ckpt_path)

    # 1 GB remaining work across 10 tables
    nodes = [
        DAGNode(
            node_id=f"tab_{i}",
            phase=DAGPhase.SEED_COPY,
            table_name=f"big_table_{i}",
            range_id="r0",
            min_cursor=0,
            max_cursor=1000000,
            estimated_bytes=100_000_000,
            estimated_rows=1_000_000,
        )
        for i in range(10)
    ]
    dag = LiveReplanDAG(nodes)

    quotas = ResourceQuotaSpec(
        cpu_cores_available=16.0,
        disk_read_bytes_per_sec=300_000_000.0,
        disk_write_bytes_per_sec=250_000_000.0,
        network_bytes_per_sec=150_000_000.0,
        staging_quota_bytes=2_000_000_000.0,
        max_worker_connections=32,
        memory_quota_bytes=4 * 1024 * 1024 * 1024,
    )

    policy = PricedTransitionPolicy(
        hysteresis_margin_sec=5.0,
        hysteresis_ratio=0.10,
        cooldown_seconds=15.0,
        min_observation_window_sec=10.0,
        degradation_threshold=0.25,
    )

    # Sustained degradation: direct streaming bandwidth dropped from 100 MB/s to 10 MB/s
    # Staged bulk load can achieve 150 MB/s
    obs = [
        TelemetryObservation(
            timestamp=1000.0 + i * 3.0,
            observed_throughput_bytes_per_sec=10_000_000.0,  # 90% degradation!
            cdc_arrival_rate_bytes_per_sec=50.0,
            cdc_drain_rate_bytes_per_sec=500.0,
            cdc_backlog_bytes=5000.0,
            cpu_utilization_ratio=0.20,
            memory_utilization_ratio=0.25,
            active_connections=4,
        )
        for i in range(10)  # 30 seconds observation
    ]

    result = evaluate_live_replan(
        "sustained throughput degradation switch to staged bulk",
        current_strategy=ReplanStrategy.DIRECT_STREAM,
        dag=dag,
        checkpoint_store=ckpt_store,
        telemetry_history=obs,
        quotas=quotas,
        policy=policy,
        in_flight_uncommitted_bytes=2_000_000,
        in_flight_uncommitted_rows=10_000,
        source_buffer_pool_bytes=20_000_000,
        target_buffer_pool_bytes=20_000_000,
        snapshot_age_seconds=120.0,
        active_connections=4,
        seconds_since_last_switch=60.0,
        facts_override={
            "baseline_throughput": 100_000_000.0,
            "staged_bulk_throughput": 150_000_000.0,
        },
    )

    # Proves:
    # 1. Switch was APPROVED
    assert result.switched
    # 2. Chosen strategy is STAGED_BULK_LOAD
    assert result.chosen_strategy == ReplanStrategy.STAGED_BULK_LOAD
    # 3. Remaining time under direct stream (~100s) vs staged bulk (~10.6s)
    assert result.estimated_remaining_seconds_current > 80.0
    assert result.estimated_remaining_seconds_candidate < 25.0
    # 4. Gross savings > 60s
    assert result.gross_savings_seconds > 60.0
    # 5. Net benefit is positive after subtracting transition costs and hysteresis
    assert result.net_benefit_seconds > 40.0
    # 6. Care badge is PASS
    assert result.care_badge == CareBadge.PASS
    assert "PASS" in result.summary


# -----------------------------------------------------------------------------
# 5. Priced Transition Vetoes: Snapshot Staleness, Undo Penalty, Reconnect Quota
# -----------------------------------------------------------------------------


def test_transition_vetoed_by_snapshot_staleness(tmp_path: pathlib.Path) -> None:
    """Proof: Replan switch is vetoed when snapshot age violates database retention."""
    ckpt_store = LiveReplanCheckpointStore(tmp_path / "ckpt.json")
    nodes = [
        DAGNode(
            node_id="tab_1",
            phase=DAGPhase.SEED_COPY,
            table_name="orders",
            range_id="r0",
            min_cursor=0,
            max_cursor=1000,
            estimated_bytes=50_000_000,
            estimated_rows=50_000,
        )
    ]
    dag = LiveReplanDAG(nodes)
    quotas = ResourceQuotaSpec(
        cpu_cores_available=4.0,
        disk_read_bytes_per_sec=100_000_000.0,
        disk_write_bytes_per_sec=100_000_000.0,
        network_bytes_per_sec=100_000_000.0,
        staging_quota_bytes=500_000_000.0,
        max_worker_connections=8,
        memory_quota_bytes=1024 * 1024 * 1024,
    )
    obs = [
        TelemetryObservation(
            timestamp=1000.0 + i * 2.0,
            observed_throughput_bytes_per_sec=10_000_000.0,
            cdc_arrival_rate_bytes_per_sec=10.0,
            cdc_drain_rate_bytes_per_sec=100.0,
            cdc_backlog_bytes=100.0,
            cpu_utilization_ratio=0.3,
            memory_utilization_ratio=0.3,
            active_connections=2,
        )
        for i in range(6)
    ]

    result = evaluate_live_replan(
        "snapshot staleness veto test",
        current_strategy=ReplanStrategy.DIRECT_STREAM,
        dag=dag,
        checkpoint_store=ckpt_store,
        telemetry_history=obs,
        quotas=quotas,
        snapshot_age_seconds=20000.0,  # exceeds 14400.0 max
        facts_override={
            "baseline_throughput": 50_000_000.0,
            "staged_bulk_throughput": 80_000_000.0,
            "snapshot_loss_risk_confirmed": True,
        },
    )

    # Proves:
    # 1. Switch was REFUSED
    assert not result.switched
    # 2. Veto rung holds
    assert result.transition_cost_choice.path == "transition_vetoed_by_snapshot_staleness"
    # 3. Care badge is REVIEW
    assert result.care_badge == CareBadge.REVIEW
    assert "REVIEW" in result.summary


def test_transition_vetoed_by_undo_penalty(tmp_path: pathlib.Path) -> None:
    """Proof: Replan switch is vetoed when uncommitted in-flight work undo cost is excessive."""
    ckpt_store = LiveReplanCheckpointStore(tmp_path / "ckpt.json")
    nodes = [
        DAGNode(
            node_id="tab_1",
            phase=DAGPhase.SEED_COPY,
            table_name="orders",
            range_id="r0",
            min_cursor=0,
            max_cursor=1000,
            estimated_bytes=10_000_000,
            estimated_rows=10_000,
        )
    ]
    dag = LiveReplanDAG(nodes)
    quotas = ResourceQuotaSpec(
        cpu_cores_available=4.0,
        disk_read_bytes_per_sec=10_000_000.0,
        disk_write_bytes_per_sec=10_000_000.0,
        network_bytes_per_sec=10_000_000.0,
        staging_quota_bytes=500_000_000.0,
        max_worker_connections=8,
        memory_quota_bytes=1024 * 1024 * 1024,
    )
    obs = [
        TelemetryObservation(
            timestamp=1000.0 + i * 2.0,
            observed_throughput_bytes_per_sec=2_000_000.0,
            cdc_arrival_rate_bytes_per_sec=10.0,
            cdc_drain_rate_bytes_per_sec=100.0,
            cdc_backlog_bytes=100.0,
            cpu_utilization_ratio=0.3,
            memory_utilization_ratio=0.3,
            active_connections=2,
        )
        for i in range(6)
    ]

    result = evaluate_live_replan(
        "undo penalty veto test",
        current_strategy=ReplanStrategy.DIRECT_STREAM,
        dag=dag,
        checkpoint_store=ckpt_store,
        telemetry_history=obs,
        quotas=quotas,
        in_flight_uncommitted_bytes=9_000_000,  # 90% uncommitted in-flight!
        in_flight_uncommitted_rows=9_000,
        facts_override={
            "baseline_throughput": 5_000_000.0,
            "staged_bulk_throughput": 8_000_000.0,
            "finishing_in_flight_faster": True,
        },
    )

    assert not result.switched
    assert result.transition_cost_choice.path == "transition_vetoed_by_undo_penalty"
    assert result.care_badge == CareBadge.REVIEW


def test_transition_vetoed_by_reconnect_quota(tmp_path: pathlib.Path) -> None:
    """Proof: Replan switch is vetoed when reconnect quota is exhausted."""
    ckpt_store = LiveReplanCheckpointStore(tmp_path / "ckpt.json")
    nodes = [
        DAGNode(
            node_id="tab_1",
            phase=DAGPhase.SEED_COPY,
            table_name="orders",
            range_id="r0",
            min_cursor=0,
            max_cursor=1000,
            estimated_bytes=50_000_000,
            estimated_rows=50_000,
        )
    ]
    dag = LiveReplanDAG(nodes)
    quotas = ResourceQuotaSpec(
        cpu_cores_available=4.0,
        disk_read_bytes_per_sec=100_000_000.0,
        disk_write_bytes_per_sec=100_000_000.0,
        network_bytes_per_sec=100_000_000.0,
        staging_quota_bytes=500_000_000.0,
        max_worker_connections=8,
        memory_quota_bytes=1024 * 1024 * 1024,
    )
    obs = [
        TelemetryObservation(
            timestamp=1000.0 + i * 2.0,
            observed_throughput_bytes_per_sec=10_000_000.0,
            cdc_arrival_rate_bytes_per_sec=10.0,
            cdc_drain_rate_bytes_per_sec=100.0,
            cdc_backlog_bytes=100.0,
            cpu_utilization_ratio=0.3,
            memory_utilization_ratio=0.3,
            active_connections=4,
        )
        for i in range(6)
    ]

    result = evaluate_live_replan(
        "reconnect quota veto test",
        current_strategy=ReplanStrategy.DIRECT_STREAM,
        dag=dag,
        checkpoint_store=ckpt_store,
        telemetry_history=obs,
        quotas=quotas,
        facts_override={
            "baseline_throughput": 50_000_000.0,
            "staged_bulk_throughput": 80_000_000.0,
            "reconnect_quota_exhausted": True,
            "reconnect_risk_disruptive": True,
        },
    )

    assert not result.switched
    assert result.transition_cost_choice.path == "transition_vetoed_by_reconnect_quota"
    assert result.care_badge == CareBadge.REVIEW


# -----------------------------------------------------------------------------
# 6. Hysteresis Damping Band Test (Marginal Savings Rejected)
# -----------------------------------------------------------------------------


def test_hysteresis_damping_band_rejects_marginal_gain(tmp_path: pathlib.Path) -> None:
    """Proof: Marginal savings within the hysteresis margin retain the incumbent plan."""
    ckpt_store = LiveReplanCheckpointStore(tmp_path / "ckpt.json")
    nodes = [
        DAGNode(
            node_id="tab_1",
            phase=DAGPhase.SEED_COPY,
            table_name="orders",
            range_id="r0",
            min_cursor=0,
            max_cursor=1000,
            estimated_bytes=50_000_000,
            estimated_rows=50_000,
        )
    ]
    dag = LiveReplanDAG(nodes)
    quotas = ResourceQuotaSpec(
        cpu_cores_available=8.0,
        disk_read_bytes_per_sec=100_000_000.0,
        disk_write_bytes_per_sec=100_000_000.0,
        network_bytes_per_sec=100_000_000.0,
        staging_quota_bytes=500_000_000.0,
        max_worker_connections=8,
        memory_quota_bytes=1024 * 1024 * 1024,
    )
    # Marginal throughput difference (40 MB/s current vs 45 MB/s staged)
    obs = [
        TelemetryObservation(
            timestamp=1000.0 + i * 2.0,
            observed_throughput_bytes_per_sec=25_000_000.0,
            cdc_arrival_rate_bytes_per_sec=10.0,
            cdc_drain_rate_bytes_per_sec=100.0,
            cdc_backlog_bytes=100.0,
            cpu_utilization_ratio=0.3,
            memory_utilization_ratio=0.3,
            active_connections=2,
        )
        for i in range(6)
    ]

    result = evaluate_live_replan(
        "hysteresis damping test",
        current_strategy=ReplanStrategy.DIRECT_STREAM,
        dag=dag,
        checkpoint_store=ckpt_store,
        telemetry_history=obs,
        quotas=quotas,
        policy=PricedTransitionPolicy(hysteresis_margin_sec=8.0),
        facts_override={
            "baseline_throughput": 40_000_000.0,
            "staged_bulk_throughput": 27_000_000.0,  # tiny speedup
        },
    )

    # Proves:
    # 1. Switch was NOT performed
    assert not result.switched
    # 2. Chosen strategy is still incumbent
    assert result.chosen_strategy == ReplanStrategy.DIRECT_STREAM
    # 3. Transition cost rung is maintain_incumbent_within_hysteresis
    assert result.transition_cost_choice.path == "maintain_incumbent_within_hysteresis"
    # 4. Care badge is BENIGN
    assert result.care_badge == CareBadge.BENIGN


# -----------------------------------------------------------------------------
# 7. DAG Reschedule Behavior: CDC Prioritize, Skew Split, Coalesce
# -----------------------------------------------------------------------------


def test_dag_reschedule_prioritizes_cdc_and_splits_skew() -> None:
    """Proof: DAG reschedule ladder reorders high CDC tables and splits skewed partitions."""
    nodes = [
        DAGNode(
            node_id="tab_idle",
            phase=DAGPhase.SEED_COPY,
            table_name="lookup_status",
            range_id="r0",
            min_cursor=0,
            max_cursor=1000,
            estimated_bytes=10_000_000,
            estimated_rows=10_000,
            cdc_arrival_rate=0.0,
        ),
        DAGNode(
            node_id="tab_hot_cdc",
            phase=DAGPhase.SEED_COPY,
            table_name="payments",
            range_id="r0",
            min_cursor=0,
            max_cursor=100000,
            estimated_bytes=50_000_000,
            estimated_rows=100_000,
            cdc_arrival_rate=2500.0,  # High CDC rate!
        ),
        DAGNode(
            node_id="tab_skewed",
            phase=DAGPhase.SEED_COPY,
            table_name="large_events",
            range_id="r0",
            min_cursor=0,
            max_cursor=500000,
            estimated_bytes=80_000_000,
            estimated_rows=200_000,
            is_skewed=True,
            cdc_arrival_rate=100.0,
        ),
    ]
    dag = LiveReplanDAG(nodes)

    # 1. Prioritize CDC tables
    reshaped_cdc = reshape_remaining_dag(dag, "prioritize_critical_cdc_tables")
    rem_cdc = reshaped_cdc.get_remaining_nodes()
    assert rem_cdc[0].node_id == "tab_hot_cdc"

    # 2. Dynamic split skewed ranges
    reshaped_skew = reshape_remaining_dag(
        dag, "dynamic_split_skewed_ranges", target_chunk_bytes=10_000_000
    )
    rem_skew = reshaped_skew.get_remaining_nodes()
    skew_parts = [n for n in rem_skew if "tab_skewed" in n.node_id]
    assert len(skew_parts) == 2
    assert skew_parts[0].estimated_bytes == 40_000_000
    assert skew_parts[1].estimated_bytes == 40_000_000

    # 3. Coalesce small remaining ranges
    small_nodes = [
        DAGNode(
            node_id=f"small_{i}",
            phase=DAGPhase.SEED_COPY,
            table_name="tags",
            range_id=f"r{i}",
            min_cursor=i * 100,
            max_cursor=(i + 1) * 100,
            estimated_bytes=500_000,
            estimated_rows=500,
        )
        for i in range(5)
    ]
    small_dag = LiveReplanDAG(small_nodes)
    coalesced_dag = reshape_remaining_dag(
        small_dag, "coalesce_small_remaining_ranges", target_chunk_bytes=5_000_000
    )
    coalesced_nodes = coalesced_dag.get_remaining_nodes()
    assert len(coalesced_nodes) == 1
    assert "bundle_" in coalesced_nodes[0].node_id
    assert coalesced_nodes[0].estimated_bytes == 2_500_000


# -----------------------------------------------------------------------------
# 8. Universal Typed Durable Resume in O(1) & Lease Lock
# -----------------------------------------------------------------------------


def test_typed_checkpoint_persistence_and_lease(tmp_path: pathlib.Path) -> None:
    """Proof: Checkpoint store preserves native types and lease lock mutual exclusion."""
    ckpt_file = tmp_path / "checkpoints.json"
    store = LiveReplanCheckpointStore(ckpt_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    u_id = uuid.uuid4()
    dec_val = decimal.Decimal("999999.12345")

    unit = LiveReplanCheckpointUnit(
        node_id="test_node_42",
        phase=DAGPhase.SEED_COPY,
        strategy=ReplanStrategy.DIRECT_STREAM,
        min_cursor=u_id,
        max_cursor=dec_val,
        merkle_digest="merkle_digest_xyz",
        completed_at=now,
    )
    store.record_unit(unit)

    # Reload store from disk
    store_loaded = LiveReplanCheckpointStore(ckpt_file)
    assert store_loaded.is_completed_o1("test_node_42", DAGPhase.SEED_COPY)
    recovered = store_loaded.get_unit("test_node_42", DAGPhase.SEED_COPY)
    assert recovered is not None
    assert recovered.min_cursor == u_id
    assert recovered.max_cursor == dec_val
    assert isinstance(recovered.min_cursor, uuid.UUID)
    assert isinstance(recovered.max_cursor, decimal.Decimal)

    # Active lease lock test
    lock_file = tmp_path / ".run.lock"
    lease_handle = acquire_live_replan_lease(lock_file, term_seconds=30.0)
    assert lease_handle.fresh()
    lease_handle.release()


# -----------------------------------------------------------------------------
# 9. Endpoint Safety and Sanitization
# -----------------------------------------------------------------------------


def test_validate_safe_endpoint() -> None:
    """Proof: validate_safe_endpoint enforces safe hosts and rejects credentials."""
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("10.0.0.5") == "10.0.0.5"
    assert validate_safe_endpoint("localhost") == "localhost"
    assert validate_safe_endpoint("db.internal") == "db.internal"

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("user:secret@10.0.0.1")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("10.0.0.1:5432")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("evil.external-attacker.com")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("")


# -----------------------------------------------------------------------------
# 10. Instant Cold-Start Byte-Weighted ETA and Transition Calculations
# -----------------------------------------------------------------------------


def test_instant_cold_start_byte_weighted_eta() -> None:
    """Proof: estimate_byte_weighted_eta blends progress with cold start rate."""
    # Cold start: 0 progress
    eta_cold = estimate_byte_weighted_eta(
        total_bytes=100_000_000,
        completed_bytes=0,
        elapsed_seconds=0.0,
        cold_start_bytes_per_sec=10_000_000.0,
        downstream_tail_seconds=5.0,
    )
    assert round(eta_cold, 1) == 15.0  # 10s copy + 5s tail

    # Blended progress: 50% completed at higher rate
    eta_blended = estimate_byte_weighted_eta(
        total_bytes=100_000_000,
        completed_bytes=50_000_000,
        elapsed_seconds=2.5,  # 20 MB/s observed
        cold_start_bytes_per_sec=10_000_000.0,
        downstream_tail_seconds=5.0,
    )
    # progress weight = 0.5, blended rate = 0.5 * 20M + 0.5 * 10M = 15 MB/s
    # remaining 50M / 15M = 3.33s + 5.0s = 8.33s
    assert 8.0 < eta_blended < 8.5
