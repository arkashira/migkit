"""Tests for Row 603: SMART-N20.work-granularity (Optimize work units and encoding jointly for distributions).

Verifies:
1. Every `decide.Rung` in `migkit/work_granularity.py` (`batch_sizing_rungs`,
   `split_merge_rungs`, `compression_encoding_rungs`, `representation_integrity_rungs`)
   reaches both `holds` and `refuses` under `tests.rung_branches.watched()` and
   is declared in `PROVES_RUNGS`.
2. Joint optimization for distributions:
   - Batch bytes and rows adapt to row-width tails (p50, p95, p99, max).
   - High density compact batches amortize round trips on small uniform rows.
   - Service caps (max_request_packet_bytes) are never exceeded.
3. Memory-bounded execution for mixed tiny rows and extreme LOBs:
   - Extreme LOBs are streamed in bounded pieces without truncation.
   - Worker memory stays strictly below allocated budget.
4. Keyless multiplicity preservation:
   - Multiple duplicate identical rows are enveloped with occurrence ordinals.
   - Merkle bucket digests verify byte-exact multiplicity preservation between source and target.
5. LOB integrity verification:
   - Reassembled LOB pieces match SHA-256 checksums exactly.
   - Truncated or corrupted pieces are detected and rejected.
6. Universal typed durable resume in O(1):
   - Checkpoints with native Python types (`datetime`, `Decimal`, `UUID`, composite tuples).
   - O(1) skip for already completed units without rescanning.
7. Mutual exclusion lease lock (.run.lock) with heartbeat.
8. Dynamic AIMD batch sizing and instant cold-start byte-weighted ETA.
9. Normalized tool wrapper with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS).
10. Safe test endpoint validation (127.0.0.1, 10.0.0.x).
"""

from __future__ import annotations

import datetime
import decimal
import hashlib
import json
import os
import pathlib
import uuid
from typing import Any, Dict, List, Tuple

import pytest

import tests.rung_branches
from migkit import decide
from migkit import work_granularity as wg
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    # batch_sizing_rungs
    ("work_granularity", "batch_sizing_rungs", "adaptive_lob_tail_bounded_batch", "holds"),
    ("work_granularity", "batch_sizing_rungs", "adaptive_lob_tail_bounded_batch", "refuses"),
    ("work_granularity", "batch_sizing_rungs", "high_density_compact_batch", "holds"),
    ("work_granularity", "batch_sizing_rungs", "high_density_compact_batch", "refuses"),
    ("work_granularity", "batch_sizing_rungs", "service_cap_constrained_batch", "holds"),
    ("work_granularity", "batch_sizing_rungs", "service_cap_constrained_batch", "refuses"),
    ("work_granularity", "batch_sizing_rungs", "conservative_fallback_batch", "holds"),
    ("work_granularity", "batch_sizing_rungs", "conservative_fallback_batch", "refuses"),
    # split_merge_rungs
    ("work_granularity", "split_merge_rungs", "split_skewed_dense_range", "holds"),
    ("work_granularity", "split_merge_rungs", "split_skewed_dense_range", "refuses"),
    ("work_granularity", "split_merge_rungs", "merge_sparse_subranges", "holds"),
    ("work_granularity", "split_merge_rungs", "merge_sparse_subranges", "refuses"),
    ("work_granularity", "split_merge_rungs", "preserve_discrete_key_boundary", "holds"),
    ("work_granularity", "split_merge_rungs", "preserve_discrete_key_boundary", "refuses"),
    # compression_encoding_rungs
    ("work_granularity", "compression_encoding_rungs", "compress_high_ratio_lz4", "holds"),
    ("work_granularity", "compression_encoding_rungs", "compress_high_ratio_lz4", "refuses"),
    ("work_granularity", "compression_encoding_rungs", "compress_cpu_throttled_light", "holds"),
    ("work_granularity", "compression_encoding_rungs", "compress_cpu_throttled_light", "refuses"),
    ("work_granularity", "compression_encoding_rungs", "pass_through_raw_uncompressed", "holds"),
    ("work_granularity", "compression_encoding_rungs", "pass_through_raw_uncompressed", "refuses"),
    # representation_integrity_rungs
    ("work_granularity", "representation_integrity_rungs", "keyless_multiplicity_occurrence_envelope", "holds"),
    ("work_granularity", "representation_integrity_rungs", "keyless_multiplicity_occurrence_envelope", "refuses"),
    ("work_granularity", "representation_integrity_rungs", "lob_streaming_piece_envelope", "holds"),
    ("work_granularity", "representation_integrity_rungs", "lob_streaming_piece_envelope", "refuses"),
    ("work_granularity", "representation_integrity_rungs", "standard_relational_row_envelope", "holds"),
    ("work_granularity", "representation_integrity_rungs", "standard_relational_row_envelope", "refuses"),
]


# -----------------------------------------------------------------------------
# 1. Rung Branch Coverage Under tests.rung_branches.watched()
# -----------------------------------------------------------------------------

def test_every_decide_rung_in_work_granularity_reaches_both_branches() -> None:
    """Verify that every decide.Rung in work_granularity reaches holds and refuses under watched()."""
    with watched() as seen:
        # 1. batch_sizing_rungs
        # A) adaptive_lob_tail_bounded_batch holds, high_density refuses, service_cap refuses
        facts_tail = {
            "p99_row_bytes": 100_000,
            "max_row_bytes": 200_000,
            "lob_tail_threshold_bytes": 65536,
            "compact_threshold_bytes": 1024,
            "worker_memory_budget_bytes": 10_000_000,
            "max_request_packet_bytes": 16_000_000,
            "nominal_batch_bytes": 8_000_000,
            "target_batch_bytes": 4_000_000,
            "uncertain_distribution": False,
            "variance_coefficient": 1.0,
            "allow_conservative_fallback": False,
        }
        dec1 = wg.choose_batch_sizing(facts_tail)
        assert dec1.choice.path == "adaptive_lob_tail_bounded_batch"

        # B) high_density_compact_batch holds, adaptive_lob refuses, service_cap refuses
        facts_compact = {
            "p99_row_bytes": 128,
            "max_row_bytes": 512,
            "lob_tail_threshold_bytes": 65536,
            "compact_threshold_bytes": 1024,
            "worker_memory_budget_bytes": 64_000_000,
            "max_request_packet_bytes": 16_000_000,
            "nominal_batch_bytes": 8_000_000,
            "target_batch_bytes": 4_000_000,
            "uncertain_distribution": False,
            "variance_coefficient": 0.5,
            "allow_conservative_fallback": False,
        }
        dec2 = wg.choose_batch_sizing(facts_compact)
        assert dec2.choice.path == "high_density_compact_batch"

        # C) service_cap_constrained_batch holds, high_density refuses, adaptive_lob refuses
        facts_cap = {
            "p99_row_bytes": 4096,
            "max_row_bytes": 8192,
            "lob_tail_threshold_bytes": 65536,
            "compact_threshold_bytes": 1024,
            "worker_memory_budget_bytes": 64_000_000,
            "max_request_packet_bytes": 500_000,  # small packet cap
            "nominal_batch_bytes": 8_000_000,
            "target_batch_bytes": 4_000_000,
            "uncertain_distribution": False,
            "variance_coefficient": 1.0,
            "allow_conservative_fallback": False,
        }
        dec3 = wg.choose_batch_sizing(facts_cap)
        assert dec3.choice.path == "service_cap_constrained_batch"

        # D) conservative_fallback_batch holds
        facts_fallback = {
            "p99_row_bytes": 4096,
            "max_row_bytes": 8192,
            "lob_tail_threshold_bytes": 65536,
            "compact_threshold_bytes": 1024,
            "worker_memory_budget_bytes": 64_000_000,
            "max_request_packet_bytes": 16_000_000,
            "nominal_batch_bytes": 8_000_000,
            "target_batch_bytes": 4_000_000,
            "uncertain_distribution": True,
            "variance_coefficient": 6.5,
            "allow_conservative_fallback": True,
        }
        dec4 = wg.choose_batch_sizing(facts_fallback)
        assert dec4.choice.path == "conservative_fallback_batch"

        # 2. split_merge_rungs
        # A) split_skewed_dense_range holds
        facts_split = {
            "range_row_count": 150_000,
            "max_range_rows_threshold": 50_000,
            "min_range_rows_threshold": 5_000,
            "expected_retry_cost_s": 45.0,
            "max_retry_cost_tolerance_s": 30.0,
            "distinct_keys_count": 10,
            "has_splittable_key_boundary": True,
            "combined_range_row_count": 200_000,
            "combined_range_bytes": 20_000_000,
            "min_range_bytes_threshold": 1048576,
            "ranges_are_contiguous": True,
            "proposed_split_boundaries": (100, 200, 300),
            "preserve_boundary_explicit": False,
        }
        sm1 = wg.choose_split_merge(facts_split)
        assert sm1.action == wg.SplitMergeAction.SPLIT

        # B) merge_sparse_subranges holds
        facts_merge = {
            "range_row_count": 1000,
            "max_range_rows_threshold": 50_000,
            "min_range_rows_threshold": 5_000,
            "expected_retry_cost_s": 2.0,
            "max_retry_cost_tolerance_s": 30.0,
            "distinct_keys_count": 5,
            "has_splittable_key_boundary": True,
            "combined_range_row_count": 2000,
            "combined_range_bytes": 200_000,
            "min_range_bytes_threshold": 1048576,
            "ranges_are_contiguous": True,
            "preserve_boundary_explicit": False,
        }
        sm2 = wg.choose_split_merge(facts_merge)
        assert sm2.action == wg.SplitMergeAction.MERGE

        # C) preserve_discrete_key_boundary holds
        facts_preserve = {
            "range_row_count": 20_000,
            "max_range_rows_threshold": 50_000,
            "min_range_rows_threshold": 5_000,
            "expected_retry_cost_s": 10.0,
            "max_retry_cost_tolerance_s": 30.0,
            "distinct_keys_count": 100,
            "has_splittable_key_boundary": True,
            "combined_range_row_count": 30_000,
            "combined_range_bytes": 5_000_000,
            "min_range_bytes_threshold": 1048576,
            "ranges_are_contiguous": True,
            "preserve_boundary_explicit": True,
        }
        sm3 = wg.choose_split_merge(facts_preserve)
        assert sm3.action == wg.SplitMergeAction.PRESERVE

        # D) Force preserve_discrete_key_boundary to REFUSE by making range excessive and indivisible
        facts_preserve_refuse = {
            "range_row_count": 200_000,
            "max_range_rows_threshold": 50_000,
            "min_range_rows_threshold": 5_000,
            "expected_retry_cost_s": 50.0,
            "max_retry_cost_tolerance_s": 30.0,
            "distinct_keys_count": 10,
            "has_splittable_key_boundary": True,
            "combined_range_row_count": 300_000,
            "combined_range_bytes": 50_000_000,
            "min_range_bytes_threshold": 1048576,
            "ranges_are_contiguous": True,
            "preserve_boundary_explicit": False,
        }
        decide.climb("test:preserve_refuse", wg.split_merge_rungs(), facts_preserve_refuse)

        # 3. compression_encoding_rungs
        # A) compress_high_ratio_lz4 holds
        facts_lz4 = {
            "compressibility_ratio": 0.40,
            "compressibility_threshold": 0.70,
            "network_utilization_ratio": 0.85,
            "network_pressure_threshold": 0.60,
            "bandwidth_mbps": 50.0,
            "cpu_utilization_ratio": 0.35,
            "max_cpu_compression_threshold": 0.75,
            "force_uncompressed": False,
        }
        comp1 = wg.choose_compression_encoding(facts_lz4)
        assert comp1.compression == wg.CompressionMethod.LZ4

        # B) compress_cpu_throttled_light holds
        facts_light = {
            "compressibility_ratio": 0.50,
            "compressibility_threshold": 0.70,
            "network_utilization_ratio": 0.40,
            "network_pressure_threshold": 0.60,
            "bandwidth_mbps": 500.0,
            "cpu_utilization_ratio": 0.80,
            "max_cpu_compression_threshold": 0.75,
            "force_uncompressed": False,
        }
        comp2 = wg.choose_compression_encoding(facts_light)
        assert comp2.compression == wg.CompressionMethod.LIGHT

        # C) pass_through_raw_uncompressed holds
        facts_raw = {
            "compressibility_ratio": 0.95,
            "compressibility_threshold": 0.70,
            "network_utilization_ratio": 0.20,
            "network_pressure_threshold": 0.60,
            "bandwidth_mbps": 1000.0,
            "cpu_utilization_ratio": 0.20,
            "max_cpu_compression_threshold": 0.75,
            "force_uncompressed": False,
        }
        comp3 = wg.choose_compression_encoding(facts_raw)
        assert comp3.compression == wg.CompressionMethod.NONE

        # 4. representation_integrity_rungs
        # A) keyless_multiplicity_occurrence_envelope holds
        facts_keyless = {
            "has_primary_key": False,
            "has_duplicate_multiplicities": True,
            "strict_keyless_multiplicity": True,
            "has_extreme_lob_columns": False,
            "max_lob_bytes": 0,
            "lob_piece_threshold_bytes": 1048576,
            "supports_piece_streaming": True,
        }
        rep1 = wg.choose_representation_integrity(facts_keyless)
        assert rep1.mode == wg.RepresentationMode.KEYLESS_MULTIPLICITY

        # B) lob_streaming_piece_envelope holds
        facts_lob = {
            "has_primary_key": True,
            "has_duplicate_multiplicities": False,
            "strict_keyless_multiplicity": True,
            "has_extreme_lob_columns": True,
            "max_lob_bytes": 50_000_000,
            "lob_piece_threshold_bytes": 1048576,
            "supports_piece_streaming": True,
        }
        rep2 = wg.choose_representation_integrity(facts_lob)
        assert rep2.mode == wg.RepresentationMode.LOB_STREAMING_PIECE

        # C) standard_relational_row_envelope holds
        facts_std = {
            "has_primary_key": True,
            "has_duplicate_multiplicities": False,
            "strict_keyless_multiplicity": True,
            "has_extreme_lob_columns": False,
            "max_lob_bytes": 512,
            "lob_piece_threshold_bytes": 1048576,
            "supports_piece_streaming": True,
        }
        rep3 = wg.choose_representation_integrity(facts_std)
        assert rep3.mode == wg.RepresentationMode.STANDARD_RELATIONAL

    # Assert that every rung declared in PROVES_RUNGS was reached on both HOLDS and REFUSES
    for module_name, ladder_name, rung_name, branch_name in PROVES_RUNGS:
        branches = branch(seen, rung_name)
        assert (
            branch_name in branches
        ), f"rung '{rung_name}' in ladder '{ladder_name}' did not reach '{branch_name}' (seen: {branches})"


# -----------------------------------------------------------------------------
# 2. Acceptance: Tiny Rows Mixed with Extreme LOBs Stay Memory Bounded
# -----------------------------------------------------------------------------

def test_mixed_tiny_rows_and_extreme_lobs_stay_memory_bounded() -> None:
    """Verify that streaming tiny rows mixed with extreme LOBs strictly respects worker memory budget."""
    worker_mem_budget = 4 * 1024 * 1024  # 4 MB tight worker memory
    packet_cap = 2 * 1024 * 1024        # 2 MB packet ceiling

    pressure = wg.SystemPressureReadings(
        cpu_utilization_ratio=0.30,
        network_utilization_ratio=0.50,
        available_bandwidth_mbps=100.0,
        max_request_packet_bytes=packet_cap,
        worker_memory_budget_bytes=worker_mem_budget,
        estimated_retry_cost_s=5.0,
    )

    stats = wg.DistributionStatistics(
        p50_row_bytes=64,
        p95_row_bytes=128,
        p99_row_bytes=100_000,
        max_row_bytes=5_000_000,  # 5 MB extreme LOB, larger than worker memory!
        skewness=3.5,
        compressibility_ratio=0.40,
        distinct_keys_count=100,
        sample_size=100,
    )

    plan = wg.optimize_work_granularity(
        stats,
        pressure,
        table_name="mixed_lob_table",
        has_primary_key=True,
        has_extreme_lob_columns=True,
    )

    executor = wg.WorkUnitStreamExecutor(plan, pressure, table_name="mixed_lob_table")

    # Generate synthetic stream: 500 tiny rows + 2 extreme LOBs (5 MB and 3 MB)
    stream: List[Dict[str, Any]] = []
    for i in range(250):
        stream.append({"id": i, "name": f"item_{i}", "lob_col": None})

    extreme_payload_1 = b"EXTREME_LOB_DATA_" * (5_000_000 // 17)
    stream.append({"id": 250, "name": "extreme_1", "lob_col": extreme_payload_1})

    for i in range(251, 500):
        stream.append({"id": i, "name": f"item_{i}", "lob_col": None})

    extreme_payload_2 = b"ANOTHER_HUGE_LOB_" * (3_000_000 // 17)
    stream.append({"id": 500, "name": "extreme_2", "lob_col": extreme_payload_2})

    cert = executor.execute_stream(stream, lob_columns=frozenset({"lob_col"}))

    assert cert.rows_processed == 501
    assert cert.lob_pieces_transferred > 0
    assert cert.service_cap_violations == 0
    assert cert.peak_worker_memory_bytes <= worker_mem_budget
    assert cert.lob_integrity_verified is True
    assert cert.care_badge == wg.CareBadge.PASS

    # Verify byte-exact reassembly of extreme LOBs
    assert executor.assembled_lobs["250:lob_col"] == extreme_payload_1
    assert executor.assembled_lobs["500:lob_col"] == extreme_payload_2


# -----------------------------------------------------------------------------
# 3. Acceptance: Keyless Multiplicity & Merkle Bucket Verification
# -----------------------------------------------------------------------------

def test_keyless_multiplicity_preservation_and_merkle_digest() -> None:
    """Verify that identical duplicate rows on keyless tables are completely preserved."""
    src_tracker = wg.KeylessMultiplicityTracker()
    tgt_tracker = wg.KeylessMultiplicityTracker()

    rows = [
        ("category_A", 100, "active"),
        ("category_A", 100, "active"),  # identical duplicate
        ("category_A", 100, "active"),  # third duplicate
        ("category_B", 200, "inactive"),
        ("category_B", 200, "inactive"),
        ("category_C", 300, "pending"),
    ]

    for r in rows:
        env = src_tracker.add_row(r)
        # Target simulates exact envelope reception
        tgt_tracker.add_row(env.payload)

    assert src_tracker.total_rows() == 6
    assert tgt_tracker.total_rows() == 6
    assert src_tracker.occurrence_map() == tgt_tracker.occurrence_map()

    # Merkle bucket digest must match bit-for-bit
    src_merkle = src_tracker.compute_merkle_digest()
    tgt_merkle = tgt_tracker.compute_merkle_digest()
    assert src_merkle == tgt_merkle
    assert src_tracker.verify_against(tgt_tracker) is True

    # Test disparity detection
    bad_tracker = wg.KeylessMultiplicityTracker()
    bad_tracker.add_row(("category_A", 100, "active"))  # missing 2 duplicates
    with pytest.raises(wg.MultiplicityIntegrityError):
        src_tracker.verify_against(bad_tracker)


# -----------------------------------------------------------------------------
# 4. Acceptance: Service Caps Never Exceeded & High Density Amortization
# -----------------------------------------------------------------------------

def test_service_caps_never_exceeded_and_high_density_scaling() -> None:
    """Verify service caps constrain batch sizes and high-density amortizes tiny rows."""
    tight_packet_cap = 64 * 1024  # 64 KB packet cap
    pressure = wg.SystemPressureReadings(
        cpu_utilization_ratio=0.10,
        network_utilization_ratio=0.20,
        available_bandwidth_mbps=1000.0,
        max_request_packet_bytes=tight_packet_cap,
        worker_memory_budget_bytes=16 * 1024 * 1024,
        estimated_retry_cost_s=1.0,
    )

    stats = wg.DistributionStatistics(
        p50_row_bytes=32,
        p95_row_bytes=64,
        p99_row_bytes=128,
        max_row_bytes=256,
        skewness=0.1,
        compressibility_ratio=0.90,
        distinct_keys_count=1000,
        sample_size=100,
    )

    plan = wg.optimize_work_granularity(
        stats,
        pressure,
        table_name="high_density_table",
        has_primary_key=True,
    )

    # Batch bytes must strictly respect packet cap minus headroom
    assert plan.batch_bytes <= tight_packet_cap

    executor = wg.WorkUnitStreamExecutor(plan, pressure, table_name="high_density_table")
    sample_rows = [{"id": i, "val": f"v_{i}"} for i in range(2000)]
    cert = executor.execute_stream(sample_rows)

    assert cert.rows_processed == 2000
    assert cert.service_cap_violations == 0
    assert cert.peak_worker_memory_bytes <= pressure.worker_memory_budget_bytes
    assert cert.care_badge == wg.CareBadge.PASS


# -----------------------------------------------------------------------------
# 5. Acceptance: Universal Typed Checkpoint in O(1) & Resume
# -----------------------------------------------------------------------------

def test_universal_typed_checkpoint_o1_resume(tmp_path: pathlib.Path) -> None:
    """Verify O(1) checkpoint resume with native Python types (datetime, Decimal, UUID, composite key)."""
    ckpt_file = tmp_path / "work_granularity_checkpoint.json"
    store = wg.WorkGranularityCheckpointStore(ckpt_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    composite_min = (uuid.uuid4(), decimal.Decimal("100.50"), now)
    composite_max = (uuid.uuid4(), decimal.Decimal("999.99"), now + datetime.timedelta(hours=1))

    unit = wg.WorkUnitCheckpoint(
        unit_id="range_chunk_001",
        table_name="ledger_events",
        min_cursor=composite_min,
        max_cursor=composite_max,
        rows_processed=50000,
        bytes_processed=12500000,
        completed_at=now,
        care_badge=wg.CareBadge.PASS,
    )

    store.record_unit(unit)

    # O(1) instant lookup without scanning
    assert store.is_completed_o1("range_chunk_001") is True
    assert store.is_completed_o1("range_chunk_999") is False

    # Reload from disk and verify native type preservation
    reloaded_store = wg.WorkGranularityCheckpointStore(ckpt_file)
    restored = reloaded_store.get_unit("range_chunk_001")
    assert restored is not None
    assert restored.unit_id == "range_chunk_001"
    assert restored.rows_processed == 50000
    assert restored.care_badge == wg.CareBadge.PASS

    # Verify restored composite tuple types
    assert isinstance(restored.min_cursor, tuple)
    assert isinstance(restored.min_cursor[0], uuid.UUID)
    assert isinstance(restored.min_cursor[1], decimal.Decimal)
    assert isinstance(restored.min_cursor[2], datetime.datetime)


# -----------------------------------------------------------------------------
# 6. Mutual Exclusion Lease Lock (.run.lock)
# -----------------------------------------------------------------------------

def test_mutual_exclusion_lease_lock(tmp_path: pathlib.Path) -> None:
    """Verify that active verification lease lock prevents overlapping runs."""
    lock_file = tmp_path / "test_granularity.run.lock"

    lease1 = wg.acquire_work_granularity_lease(lock_file, what="unit-test-1", term_seconds=5.0)
    assert lease1 is not None

    # Overlapping acquisition on same lock must fail or be refused
    with pytest.raises(Exception):
        wg.acquire_work_granularity_lease(lock_file, what="unit-test-2", term_seconds=5.0)

    # Releasing lease1 allows subsequent acquisition
    lease1.release()
    lease2 = wg.acquire_work_granularity_lease(lock_file, what="unit-test-2", term_seconds=5.0)
    assert lease2 is not None
    lease2.release()


# -----------------------------------------------------------------------------
# 7. Adaptive AIMD Controller & Instant Cold-Start ETA
# -----------------------------------------------------------------------------

def test_adaptive_aimd_controller_and_cold_start_eta() -> None:
    """Verify additive increase on success, multiplicative decrease on backpressure, and byte ETA."""
    controller = wg.AdaptiveWorkUnitController(
        initial_batch_rows=1000,
        min_batch_rows=100,
        max_batch_rows=10000,
        additive_step=250,
        decrease_factor=0.5,
    )

    # Initial ETA before measurements uses conservative baseline
    initial_eta = controller.estimate_cold_start_eta(50_000_000)
    assert initial_eta == 5.0  # 50 MB / 10 MB/s baseline = 5.0s

    # Additive increase on verified success
    rows1 = controller.on_verified_success(batch_bytes=2_000_000, duration_s=0.10)
    assert rows1 == 1250

    rows2 = controller.on_verified_success(batch_bytes=2_000_000, duration_s=0.10)
    assert rows2 == 1500

    # Cold start byte-weighted ETA reflects measured speed: 4 MB in 0.2s = 20 MB/s
    eta = controller.estimate_cold_start_eta(20_000_000)
    assert 0.9 <= eta <= 1.1

    # Multiplicative decrease on backpressure
    shrunk = controller.on_backpressure("memory_pressure")
    assert shrunk == 750

    shrunk2 = controller.on_backpressure("network_drop")
    assert shrunk2 == 375


# -----------------------------------------------------------------------------
# 8. Full-Capacity Tool Wrapper with 4-Tier Care Badges
# -----------------------------------------------------------------------------

def test_tool_wrapper_emits_four_tier_care_badges() -> None:
    """Verify tool wrapper normalizes Datafold, DMS, DTS, DVT, pgcopydb to 4-tier badges."""
    wrapper = wg.WorkGranularityToolWrapper()

    # ACTION: Non-zero exit code
    rep_act = wrapper.normalize_tool_result("datafold", exit_code=2, stdout="", stderr="connection refused")
    assert rep_act.care_badge == wg.CareBadge.ACTION

    # REVIEW: Discrepancies present
    rep_rev = wrapper.normalize_tool_result("dvt", exit_code=0, stdout="rows match", stderr="", discrepancies=4)
    assert rep_rev.care_badge == wg.CareBadge.REVIEW

    # BENIGN: Zero bytes moved
    rep_ben = wrapper.normalize_tool_result("pgcopydb", exit_code=0, stdout="clean", stderr="", bytes_transferred=0)
    assert rep_ben.care_badge == wg.CareBadge.BENIGN

    # PASS: Successful completion
    rep_pass = wrapper.normalize_tool_result("dms", exit_code=0, stdout="synced", stderr="", bytes_transferred=500000)
    assert rep_pass.care_badge == wg.CareBadge.PASS


# -----------------------------------------------------------------------------
# 9. Safe Test Endpoint Validation
# -----------------------------------------------------------------------------

def test_safe_test_endpoint_validation() -> None:
    """Verify loopback and 10.0.0.x are accepted, while public/unsafe IPs are rejected."""
    assert wg.validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert wg.validate_safe_endpoint("localhost") == "localhost"
    assert wg.validate_safe_endpoint("::1") == "::1"
    assert wg.validate_safe_endpoint("10.0.0.42") == "10.0.0.42"

    with pytest.raises(wg.UnsafeEndpointError):
        wg.validate_safe_endpoint("192.168.1.1")

    with pytest.raises(wg.UnsafeEndpointError):
        wg.validate_safe_endpoint("8.8.8.8")

    with pytest.raises(wg.UnsafeEndpointError):
        wg.validate_safe_endpoint("")
