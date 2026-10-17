"""Tests for optimizing verified completion across the full path (Row 589: SMART-N6.complete-path).

Proves:
1. Every decide.Rung in `complete_path.py` (`complete_path_rungs`,
   `encoding_transport_rungs`, `skew_partition_rungs`, `optimizer_search_rungs`)
   is exercised on both `holds` and `refuses` inside `with tests.rung_branches.watched():`
   and declared in `PROVES_RUNGS`.
2. Joint 10-stage model (seed, encoding/compression, transport/staging, ingestion,
   indexes/constraints, CDC drain, verification, repair, cutover, rollback): a
   faster raw copy is NOT preferred when its downstream index/CDC/proof tail is
   slower overall; real-server crossover scenarios switch paths when measured
   downstream tails shift.
3. Multi-database/hop contention, CPU/disk/network/service quotas, streaming
   backpressure with AIMD batch sizing, and skew-aware range split + small-table
   coalescing.
4. Thousands-of-table planning scalability (3,000+ tables) within bounded
   CPU/memory/wall time, retaining a safe baseline when interrupted, and reporting
   distance to a valid lower bound without claiming a global optimum.
5. Universal $O(1)$ typed checkpoint persistence and active `.run.lock` lease.
"""

from __future__ import annotations

import datetime
import decimal
import hashlib
import pathlib
import time
import uuid

import pytest

import tests.rung_branches
from migkit import complete_path, decide
from migkit.complete_path import (
    ALL_COMPLETE_PATH_STAGES,
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    BackpressureTelemetry,
    CareBadge,
    CompletePathCheckpointStore,
    CompletePathCheckpointUnit,
    CompletePathError,
    CompletePathPlan,
    CompletePathStage,
    CompletePathStrategy,
    EncodingTransportMode,
    HopContentionSpec,
    InvalidQuotaOrFactError,
    LowerBoundCertificate,
    OptimizerBudget,
    OptimizerSearchMode,
    PlannedWorkUnit,
    ResourceQuotaSpec,
    SkewHandlingMode,
    StageCostBreakdown,
    TableWorkItem,
    UnboundedCdcDrainError,
    UnsafeEndpointError,
    acquire_complete_path_lease,
    adjust_aimd_batch_size,
    complete_path_rungs,
    compute_full_path_lower_bound,
    encoding_transport_rungs,
    estimate_byte_weighted_eta,
    evaluate_strategy_stage_costs,
     optimize_complete_path,
    optimizer_search_rungs,
    plan_skew_aware_work_units,
    skew_partition_rungs,
    validate_safe_endpoint,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("complete_path", "complete_path_rungs", "direct_stream_inline_verify_deferred_index", "holds"),
    ("complete_path", "complete_path_rungs", "direct_stream_inline_verify_deferred_index", "refuses"),
    ("complete_path", "complete_path_rungs", "staged_object_bulk_load_parallel_proof", "holds"),
    ("complete_path", "complete_path_rungs", "staged_object_bulk_load_parallel_proof", "refuses"),
    ("complete_path", "complete_path_rungs", "uncompressed_fast_copy_posthoc_index_and_verify", "holds"),
    ("complete_path", "complete_path_rungs", "uncompressed_fast_copy_posthoc_index_and_verify", "refuses"),
    ("complete_path", "complete_path_rungs", "safe_baseline_chunked_serial_path", "holds"),
    ("complete_path", "complete_path_rungs", "safe_baseline_chunked_serial_path", "refuses"),
    ("complete_path", "encoding_transport_rungs", "zstd_adaptive_direct_stream", "holds"),
    ("complete_path", "encoding_transport_rungs", "zstd_adaptive_direct_stream", "refuses"),
    ("complete_path", "encoding_transport_rungs", "lz4_framed_staged_spool", "holds"),
    ("complete_path", "encoding_transport_rungs", "lz4_framed_staged_spool", "refuses"),
    ("complete_path", "encoding_transport_rungs", "uncompressed_passthrough_stream", "holds"),
    ("complete_path", "encoding_transport_rungs", "uncompressed_passthrough_stream", "refuses"),
    ("complete_path", "skew_partition_rungs", "skew_aware_split_and_small_table_coalesce", "holds"),
    ("complete_path", "skew_partition_rungs", "skew_aware_split_and_small_table_coalesce", "refuses"),
    ("complete_path", "skew_partition_rungs", "dynamic_range_split_only", "holds"),
    ("complete_path", "skew_partition_rungs", "dynamic_range_split_only", "refuses"),
    ("complete_path", "skew_partition_rungs", "uniform_table_schedule", "holds"),
    ("complete_path", "skew_partition_rungs", "uniform_table_schedule", "refuses"),
    ("complete_path", "optimizer_search_rungs", "exact_joint_branch_and_bound", "holds"),
    ("complete_path", "optimizer_search_rungs", "exact_joint_branch_and_bound", "refuses"),
    ("complete_path", "optimizer_search_rungs", "hierarchical_bucketed_joint_plan", "holds"),
    ("complete_path", "optimizer_search_rungs", "hierarchical_bucketed_joint_plan", "refuses"),
    ("complete_path", "optimizer_search_rungs", "retain_safe_baseline_on_budget_exhaustion", "holds"),
    ("complete_path", "optimizer_search_rungs", "retain_safe_baseline_on_budget_exhaustion", "refuses"),
]


# -----------------------------------------------------------------------------
# 1. All Decide Rungs Reached on Both `holds` and `refuses`
# -----------------------------------------------------------------------------


def test_every_complete_path_rung_holds_and_refuses_under_watched() -> None:
    """Exercise both `holds` and `refuses` for every rung in `complete_path.py`."""
    with watched() as seen:
        # 1. complete_path_rungs: all hold vs all refuse
        decide.climb(
            "complete path all hold",
            complete_path_rungs(),
            decide.Facts(
                {
                    "direct_streaming_available": True,
                    "deferred_index_inline_proof_viable": True,
                    "full_path_verified_seconds_inline": 120.0,
                    "full_path_verified_seconds_raw_copy": 280.0,
                    "quotas_allow_direct_stream": True,
                    "staging_storage_available": True,
                    "bulk_load_supported": True,
                    "staged_bulk_within_window": True,
                    "raw_copy_tail_competitive": True,
                    "quotas_allow_double_scan": True,
                    "safe_baseline_available": True,
                }
            ),
        )
        decide.climb(
            "complete path all refuse",
            complete_path_rungs(),
            decide.Facts(
                {
                    "direct_streaming_available": False,
                    "staging_storage_available": False,
                    "raw_copy_tail_competitive": False,
                    "quotas_allow_double_scan": False,
                    "safe_baseline_available": False,
                }
            ),
        )

        # 2. encoding_transport_rungs: all hold vs all refuse
        decide.climb(
            "encoding transport all hold",
            encoding_transport_rungs(),
            decide.Facts(
                {
                    "cpu_utilization_ratio": 0.40,
                    "cpu_compression_ceiling": 0.85,
                    "backpressure_stall_ratio": 0.10,
                    "max_direct_backpressure_ratio": 0.35,
                    "staging_quota_bytes": 50_000_000_000.0,
                    "required_spool_bytes": 5_000_000_000.0,
                    "lz4_spool_enabled": True,
                    "network_quota_bytes_per_sec": 1_000_000_000.0,
                    "raw_stream_required_bytes_per_sec": 400_000_000.0,
                    "allow_uncompressed_passthrough": True,
                }
            ),
        )
        decide.climb(
            "encoding transport all refuse",
            encoding_transport_rungs(),
            decide.Facts(
                {
                    "cpu_utilization_ratio": 0.95,
                    "cpu_compression_ceiling": 0.85,
                    "backpressure_stall_ratio": 0.70,
                    "max_direct_backpressure_ratio": 0.35,
                    "staging_quota_bytes": 100.0,
                    "required_spool_bytes": 5_000_000_000.0,
                    "lz4_spool_enabled": False,
                    "network_quota_bytes_per_sec": 10_000_000.0,
                    "raw_stream_required_bytes_per_sec": 400_000_000.0,
                    "allow_uncompressed_passthrough": False,
                }
            ),
        )

        # 3. skew_partition_rungs: all hold vs all refuse
        decide.climb(
            "skew partition all hold",
            skew_partition_rungs(),
            decide.Facts(
                {
                    "range_split_supported": True,
                    "coalesce_supported": True,
                    "max_to_median_skew_ratio": 6.0,
                    "skew_split_threshold": 2.0,
                    "table_count": 200,
                    "coalesce_table_threshold": 50,
                    "uniform_schedule_allowed": True,
                }
            ),
        )
        decide.climb(
            "skew partition all refuse",
            skew_partition_rungs(),
            decide.Facts(
                {
                    "range_split_supported": False,
                    "coalesce_supported": False,
                    "max_to_median_skew_ratio": 1.0,
                    "skew_split_threshold": 2.0,
                    "table_count": 5,
                    "coalesce_table_threshold": 50,
                    "uniform_schedule_allowed": False,
                }
            ),
        )

        # 4. optimizer_search_rungs: all hold vs all refuse
        decide.climb(
            "optimizer search all hold",
            optimizer_search_rungs(),
            decide.Facts(
                {
                    "table_count": 40,
                    "exact_max_tables": 250,
                    "estimated_state_bytes": 163_840,
                    "estimated_bucket_memory_bytes": 65_536,
                    "max_memory_bytes": 64 * 1024 * 1024,
                    "search_interrupted": False,
                    "remaining_time_budget_sec": 1.5,
                    "min_exact_budget_sec": 0.005,
                    "safe_baseline_initialized": True,
                }
            ),
        )
        decide.climb(
            "optimizer search all refuse",
            optimizer_search_rungs(),
            decide.Facts(
                {
                    "table_count": 5000,
                    "exact_max_tables": 250,
                    "estimated_state_bytes": 512 * 1024 * 1024,
                    "estimated_bucket_memory_bytes": 512 * 1024 * 1024,
                    "max_memory_bytes": 1024,
                    "search_interrupted": True,
                    "remaining_time_budget_sec": 0.0,
                    "safe_baseline_initialized": False,
                }
            ),
        )

    for _, _, rung_name, expected_branch in PROVES_RUNGS:
        reached = branch(seen, rung_name)
        assert expected_branch in reached, (
            f"Rung {rung_name} did not reach {expected_branch}; got {reached}"
        )
        if expected_branch == REFUSES:
            assert why(seen, rung_name) is not None


# -----------------------------------------------------------------------------
# 2. Faster Copy Refused When Downstream Index/CDC/Proof Tail Is Slower (Crossover)
# -----------------------------------------------------------------------------


def test_faster_raw_copy_not_preferred_when_downstream_proof_and_index_tail_slower() -> None:
    """Prove that a faster raw copy is rejected when its downstream index/CDC/proof tail is slower."""
    tables = [
        TableWorkItem(
            table_name="orders",
            total_bytes=4_000_000_000,
            row_count=20_000_000,
            secondary_index_count=6,
            constraint_count=3,
            max_partition_bytes=800_000_000,
            median_partition_bytes=200_000_000,
            has_splittable_key=True,
            estimated_drift_bytes=4_000_000,
        ),
        TableWorkItem(
            table_name="order_items",
            total_bytes=6_000_000_000,
            row_count=50_000_000,
            secondary_index_count=5,
            constraint_count=2,
            max_partition_bytes=1_200_000_000,
            median_partition_bytes=300_000_000,
            has_splittable_key=True,
            estimated_drift_bytes=2_000_000,
        ),
    ]
    quotas = ResourceQuotaSpec(
        cpu_cores_available=8.0,
        disk_read_bytes_per_sec=400_000_000.0,
        disk_write_bytes_per_sec=350_000_000.0,
        network_bytes_per_sec=500_000_000.0,
        service_requests_per_sec=2000.0,
        staging_quota_bytes=50_000_000_000.0,
        memory_quota_bytes=256 * 1024 * 1024,
    )
    facts = {
        "source_host": "10.0.0.11",
        "target_host": "10.0.0.12",
        "initial_cdc_backlog_bytes": 250_000_000.0,
        "cdc_arrival_bytes_per_sec": 35_000_000.0,
        "measured_cdc_apply_bytes_per_sec": 160_000_000.0,
    }

    plan = optimize_complete_path(facts, tables=tables, quotas=quotas)

    raw_bd = plan.candidate_breakdowns[
        CompletePathStrategy.UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY
    ]
    inline_bd = plan.candidate_breakdowns[
        CompletePathStrategy.DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX
    ]

    # Verify the core domain invariant:
    # Raw uncompressed copy has a faster copy-only phase on a fast network,
    # BUT its downstream index rebuild + CDC drain + post-hoc full verification tail
    # makes total verified completion much slower!
    assert raw_bd.copy_only_seconds < inline_bd.copy_only_seconds
    assert raw_bd.downstream_tail_seconds > inline_bd.downstream_tail_seconds
    assert raw_bd.total_verified_seconds > inline_bd.total_verified_seconds
    assert raw_bd.accumulated_cdc_backlog_bytes > inline_bd.accumulated_cdc_backlog_bytes

    # Optimizer selects the path with faster full-path verified completion
    assert (
        plan.chosen_strategy
        == CompletePathStrategy.DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX
    )
    assert plan.care_badge == CareBadge.PASS
    assert set(plan.stage_breakdown.stage_seconds.keys()) == set(
        ALL_COMPLETE_PATH_STAGES
    )


def test_real_server_crossover_to_staged_or_raw_when_topology_and_tails_shift() -> None:
    """Prove crossover behavior across real-server topology regimes."""
    unindexed_table = [
        TableWorkItem(
            table_name="append_log",
            total_bytes=2_000_000_000,
            row_count=5_000_000,
            secondary_index_count=0,
            constraint_count=0,
            max_partition_bytes=200_000_000,
            median_partition_bytes=200_000_000,
        )
    ]

    # Crossover Case 1: Direct streaming blocked across isolated network zone,
    # staging storage available -> switches to STAGED_OBJECT_BULK_LOAD_PARALLEL_PROOF
    staged_plan = optimize_complete_path(
        {
            "source_host": "127.0.0.1",
            "target_host": "10.0.0.25",
            "direct_streaming_available": False,
            "staging_storage_available": True,
            "bulk_load_supported": True,
            "cpu_utilization_ratio": 0.90,
            "cpu_compression_ceiling": 0.85,
        },
        tables=unindexed_table,
    )
    assert (
        staged_plan.chosen_strategy
        == CompletePathStrategy.STAGED_OBJECT_BULK_LOAD_PARALLEL_PROOF
    )
    assert (
        staged_plan.encoding_transport_mode
        == EncodingTransportMode.LZ4_FRAMED_STAGED_SPOOL
    )

    # Crossover Case 2: Ultra-fast local loopback with zero indexes, tiny post-hoc verify
    # factor (e.g. engine snapshot checksum already available), and direct stream disabled
    raw_crossover_plan = optimize_complete_path(
        {
            "source_host": "127.0.0.1",
            "target_host": "127.0.0.1",
            "direct_streaming_available": False,
            "staging_storage_available": False,
            "posthoc_full_verify_scan_factor": 0.02,
            "raw_copy_tail_competitive": True,
            "quotas_allow_double_scan": True,
            "cpu_utilization_ratio": 0.92,
            "lz4_spool_enabled": False,
            "network_quota_bytes_per_sec": 2_000_000_000.0,
            "raw_stream_required_bytes_per_sec": 500_000_000.0,
        },
        tables=unindexed_table,
    )
    assert (
        raw_crossover_plan.chosen_strategy
        == CompletePathStrategy.UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY
    )
    assert (
        raw_crossover_plan.encoding_transport_mode
        == EncodingTransportMode.UNCOMPRESSED_PASSTHROUGH_STREAM
    )


# -----------------------------------------------------------------------------
# 3. Contention, Quotas, Streaming Backpressure (AIMD), and Skew Split/Coalesce
# -----------------------------------------------------------------------------


def test_multi_database_and_hop_contention_scales_effective_quotas_and_runtime() -> None:
    """Prove multi-database/hop contention scales effective quotas and elongates stage times."""
    tables = [
        TableWorkItem(
            table_name="ledger",
            total_bytes=1_000_000_000,
            row_count=4_000_000,
            secondary_index_count=3,
            constraint_count=1,
        )
    ]
    base_quotas = ResourceQuotaSpec(
        cpu_cores_available=16.0,
        disk_read_bytes_per_sec=600_000_000.0,
        disk_write_bytes_per_sec=500_000_000.0,
        network_bytes_per_sec=400_000_000.0,
        service_requests_per_sec=4000.0,
        staging_quota_bytes=80_000_000_000.0,
        memory_quota_bytes=512 * 1024 * 1024,
    )
    uncontended = optimize_complete_path(
        {"source_host": "10.0.0.1", "target_host": "10.0.0.2"},
        tables=tables,
        quotas=base_quotas,
        contention=HopContentionSpec(
            source_concurrent_databases=1,
            target_concurrent_databases=1,
            shared_network_hops=1,
        ),
    )
    contended = optimize_complete_path(
        {"source_host": "10.0.0.1", "target_host": "10.0.0.2"},
        tables=tables,
        quotas=base_quotas,
        contention=HopContentionSpec(
            source_concurrent_databases=4,
            target_concurrent_databases=4,
            shared_network_hops=3,
            source_cpu_share_limit=0.5,
            target_write_share_limit=0.5,
            inter_hop_relay_overhead_ratio=0.25,
        ),
    )

    assert (
        contended.effective_quotas.cpu_cores_available
        < uncontended.effective_quotas.cpu_cores_available
    )
    assert (
        contended.effective_quotas.network_bytes_per_sec
        < uncontended.effective_quotas.network_bytes_per_sec
    )
    assert (
        contended.stage_breakdown.total_verified_seconds
        > uncontended.stage_breakdown.total_verified_seconds
    )


def test_streaming_backpressure_aimd_and_byte_weighted_eta() -> None:
    """Prove AIMD batch sizing under streaming backpressure and cold-start byte-weighted ETA."""
    congested = BackpressureTelemetry(
        producer_bytes_per_sec=300_000_000.0,
        consumer_bytes_per_sec=120_000_000.0,
        queue_buffered_bytes=90_000_000.0,
        queue_capacity_bytes=100_000_000.0,
        current_batch_rows=10_000,
        min_batch_rows=500,
        max_batch_rows=50_000,
        measured_batch_latency_sec=2.5,
        target_batch_latency_sec=1.0,
        additive_increase_rows=1000,
        multiplicative_decrease_factor=0.5,
    )
    assert congested.stall_ratio > 0.5
    assert adjust_aimd_batch_size(congested) == 5000

    healthy = BackpressureTelemetry(
        producer_bytes_per_sec=200_000_000.0,
        consumer_bytes_per_sec=220_000_000.0,
        queue_buffered_bytes=5_000_000.0,
        queue_capacity_bytes=100_000_000.0,
        current_batch_rows=5000,
        min_batch_rows=500,
        max_batch_rows=50_000,
        measured_batch_latency_sec=0.4,
        target_batch_latency_sec=1.0,
        additive_increase_rows=1000,
        multiplicative_decrease_factor=0.5,
    )
    assert healthy.stall_ratio < 0.1
    assert adjust_aimd_batch_size(healthy) == 6000

    cold_eta = estimate_byte_weighted_eta(
        total_bytes=1_000_000_000,
        completed_bytes=0,
        elapsed_seconds=0.0,
        cold_start_bytes_per_sec=100_000_000.0,
        downstream_tail_seconds=25.0,
    )
    assert cold_eta == pytest.approx(35.0)

    warm_eta = estimate_byte_weighted_eta(
        total_bytes=1_000_000_000,
        completed_bytes=500_000_000,
        elapsed_seconds=2.5,
        cold_start_bytes_per_sec=100_000_000.0,
        downstream_tail_seconds=25.0,
    )
    assert warm_eta < cold_eta


def test_skew_aware_split_and_small_table_coalesce() -> None:
    """Prove skewed giant tables are split while tiny tables are coalesced into bundles."""
    skewed_giant = TableWorkItem(
        table_name="events_hot",
        total_bytes=512 * 1024 * 1024,
        row_count=5_000_000,
        secondary_index_count=4,
        constraint_count=1,
        max_partition_bytes=400 * 1024 * 1024,
        median_partition_bytes=50 * 1024 * 1024,
        has_splittable_key=True,
    )
    tiny_tables = [
        TableWorkItem(
            table_name=f"lookup_{i:03d}",
            total_bytes=128 * 1024,
            row_count=500,
            secondary_index_count=1,
            constraint_count=1,
        )
        for i in range(60)
    ]

    units = plan_skew_aware_work_units(
        [skewed_giant, *tiny_tables],
        skew_mode=SkewHandlingMode.SKEW_AWARE_SPLIT_AND_SMALL_TABLE_COALESCE,
        target_chunk_bytes=64 * 1024 * 1024,
        coalesce_bundle_max_bytes=16 * 1024 * 1024,
        coalesce_bundle_max_tables=20,
    )

    split_units = [u for u in units if u.is_split_range]
    bundle_units = [u for u in units if u.is_coalesced_bundle]

    assert len(split_units) == 8
    assert sum(u.total_bytes for u in split_units) == skewed_giant.total_bytes
    assert len(bundle_units) == 3
    assert sum(len(u.table_names) for u in bundle_units) == 60


# -----------------------------------------------------------------------------
# 4. Thousands-of-Table Scalability, Bounded Optimizer & Valid Lower Bound
# -----------------------------------------------------------------------------


def test_thousands_of_tables_scalability_and_lower_bound_certificate() -> None:
    """Prove planning 3,000 tables runs in bounded time/memory and reports valid lower bound gap."""
    tables = [
        TableWorkItem(
            table_name=f"tenant_tbl_{i:04d}",
            total_bytes=(50_000_000 if i < 20 else 250_000),
            row_count=(250_000 if i < 20 else 1200),
            secondary_index_count=(4 if i < 20 else 1),
            constraint_count=1,
            max_partition_bytes=(40_000_000 if i < 5 else 10_000_000),
            median_partition_bytes=5_000_000,
            has_splittable_key=True,
        )
        for i in range(3000)
    ]

    budget = OptimizerBudget(
        max_wall_seconds=1.5,
        max_cpu_seconds=1.5,
        max_memory_bytes=32 * 1024 * 1024,
        max_iterations=5000,
        exact_max_tables=250,
    )

    t0 = time.perf_counter()
    plan = optimize_complete_path(
        {"source_host": "10.0.0.10", "target_host": "10.0.0.20"},
        tables=tables,
        budget=budget,
    )
    elapsed = time.perf_counter() - t0

    assert elapsed < 1.5
    assert plan.search_mode == OptimizerSearchMode.HIERARCHICAL_BUCKETED_JOINT_PLAN
    assert plan.skew_handling_mode == SkewHandlingMode.SKEW_AWARE_SPLIT_AND_SMALL_TABLE_COALESCE
    assert plan.optimizer_memory_bytes <= budget.max_memory_bytes
    assert len(plan.work_units) < 300

    # Valid lower bound certificate checks
    lb = plan.lower_bound
    assert lb.claims_global_optimum is False
    assert 0.0 < lb.lower_bound_seconds <= lb.planned_verified_seconds
    assert lb.absolute_gap_seconds == pytest.approx(
        lb.planned_verified_seconds - lb.lower_bound_seconds
    )
    assert lb.relative_gap_ratio >= 0.0
    assert lb.dominant_lower_bound_term in {
        "resource_bottleneck",
        "critical_chain",
        "cdc_cutover_floor",
    }


def test_interrupted_search_retains_safe_baseline_plan() -> None:
    """Prove interrupting the optimizer immediately returns the pre-validated safe baseline."""
    tables = [
        TableWorkItem(
            table_name="accounts",
            total_bytes=500_000_000,
            row_count=2_000_000,
            secondary_index_count=3,
            constraint_count=2,
        )
    ]
    budget = OptimizerBudget(
        max_wall_seconds=1.0,
        max_cpu_seconds=1.0,
        max_memory_bytes=16 * 1024 * 1024,
        max_iterations=100,
        interrupt_check=lambda: True,
    )

    plan = optimize_complete_path(
        {"source_host": "127.0.0.1", "target_host": "127.0.0.1"},
        tables=tables,
        budget=budget,
    )

    assert plan.interrupted is True
    assert plan.retained_safe_baseline is True
    assert (
        plan.search_mode
        == OptimizerSearchMode.RETAIN_SAFE_BASELINE_ON_BUDGET_EXHAUSTION
    )
    assert (
        plan.chosen_strategy
        == CompletePathStrategy.SAFE_BASELINE_CHUNKED_SERIAL_PATH
    )
    assert plan.care_badge == CareBadge.REVIEW
    assert plan.interruption_reason is not None and "REVIEW" in plan.interruption_reason


# -----------------------------------------------------------------------------
# 5. Durable O(1) Typed Checkpoint, Active Lease Lock & Error Guards
# -----------------------------------------------------------------------------


def test_complete_path_checkpoint_store_o1_typed_roundtrip_and_lease(
    tmp_path: pathlib.Path,
) -> None:
    """Prove O(1) typed checkpoint persistence and active `.run.lock` lease."""
    ckpt_file = tmp_path / "complete_path_ckpt.json"
    store = CompletePathCheckpointStore(ckpt_file)

    now = datetime.datetime(2026, 10, 17, 12, 30, 0, tzinfo=datetime.timezone.utc)
    u_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    unit = CompletePathCheckpointUnit(
        unit_id="unit:orders:range:1-of-4",
        stage=CompletePathStage.VERIFICATION,
        strategy=CompletePathStrategy.DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX,
        min_cursor=(now, decimal.Decimal("100.25"), u_id),
        max_cursor=(now, decimal.Decimal("999.75"), u_id),
        merkle_digest=hashlib.sha256(b"orders-range-1").hexdigest(),
        completed_at=now,
    )
    store.record_unit(unit)

    assert store.is_completed_o1("unit:orders:range:1-of-4", CompletePathStage.VERIFICATION)
    assert not store.is_completed_o1("unit:orders:range:1-of-4", CompletePathStage.CUTOVER)

    reloaded = CompletePathCheckpointStore(ckpt_file)
    loaded = reloaded.get_unit("unit:orders:range:1-of-4", CompletePathStage.VERIFICATION)
    assert loaded is not None
    assert loaded.min_cursor == (now, decimal.Decimal("100.25"), u_id)
    assert loaded.max_cursor == (now, decimal.Decimal("999.75"), u_id)
    assert loaded.completed_at == now

    lock_file = tmp_path / ".run.lock"
    with acquire_complete_path_lease(lock_file, term_seconds=30.0) as active_lease:
        assert active_lease.fresh()


def test_validation_guards_raise_explicit_exceptions() -> None:
    """Prove invalid endpoints, quotas, or non-converging CDC drain raise explicit errors."""
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("user:secret@10.0.0.1")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("production-db.example.com")

    with pytest.raises(InvalidQuotaOrFactError):
        ResourceQuotaSpec(
            cpu_cores_available=0.0,
            disk_read_bytes_per_sec=100.0,
            disk_write_bytes_per_sec=100.0,
            network_bytes_per_sec=100.0,
            service_requests_per_sec=10.0,
            staging_quota_bytes=0.0,
            memory_quota_bytes=1024,
        )

    with pytest.raises(UnboundedCdcDrainError):
        optimize_complete_path(
            {
                "source_host": "127.0.0.1",
                "target_host": "127.0.0.1",
                "dataset_bytes": 1_000_000_000.0,
                "initial_cdc_backlog_bytes": 50_000_000.0,
                "cdc_arrival_bytes_per_sec": 200_000_000.0,
                "measured_cdc_apply_bytes_per_sec": 100_000_000.0,
            }
        )
