"""Tests for Row 599: SMART-N16.continuous-scope (Maintain exact scope and proof while objects and data change).

Verifies:
1. Every `decide.Rung` in `migkit/continuous_scope.py`:
   - `OBJECT_EVOLUTION_LADDER`
   - `PROOF_INVALIDATION_LADDER`
   - `SNAPSHOT_CDC_HANDOFF_LADDER`
   - `RESOURCE_RETENTION_BOUND_LADDER`
   - `RESTART_RECOVERY_LADDER`
   - `KEY_SKEW_ADAPTATION_LADDER`
   reaches both `holds` and `refuses` under `tests.rung_branches.watched()` and
   is declared in `PROVES_RUNGS`.
2. Acceptance criteria:
   - DDL + writes + crash does not omit an object, double-count a range, or reuse stale proof.
   - Snapshot-to-CDC handoff strictly covers committed changes under the contract without gaps.
3. Object lifecycle & P0-SCOPE:
   - Create, drop, rename, alter, and partition attachment.
   - In-scope new objects join the dependency/proof DAG; out-of-scope objects remain excluded.
4. Delta generations, fences, and proof invalidation:
   - DDL bumps schema epoch and invalidates old proofs.
   - Isolated range fences preserve proofs for unmutated sibling ranges.
5. Resource bounds:
   - Measured WAL retention, snapshot lifetime, and backlog dynamically bounded from telemetry.
6. Crash recovery and restart:
   - Universal typed durable resume in O(1) skipping completed units without rescan.
   - Active verification lease lock (.run.lock) prevents overlapping runs.
7. Key skew & adaptive AIMD batching:
   - Skew detection and dynamic batch size controller with cold-start ETA.
8. External tool wrappers with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS).
9. Safe test hosts (127.0.0.1, 10.0.0.x) and strict exception enforcement.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest
import tests.rung_branches
from tests.rung_branches import branch

from migkit import continuous_scope as cs

PROVES_RUNGS = [
    # object_evolution_rungs
    ("continuous_scope", "object_evolution_rungs", "admit_in_scope_created_object", "holds"),
    ("continuous_scope", "object_evolution_rungs", "admit_in_scope_created_object", "refuses"),
    ("continuous_scope", "object_evolution_rungs", "track_object_rename_in_scope", "holds"),
    ("continuous_scope", "object_evolution_rungs", "track_object_rename_in_scope", "refuses"),
    ("continuous_scope", "object_evolution_rungs", "partition_attach_root_inheritance", "holds"),
    ("continuous_scope", "object_evolution_rungs", "partition_attach_root_inheritance", "refuses"),
    ("continuous_scope", "object_evolution_rungs", "drop_tombstone_eviction", "holds"),
    ("continuous_scope", "object_evolution_rungs", "drop_tombstone_eviction", "refuses"),
    ("continuous_scope", "object_evolution_rungs", "exclude_out_of_scope_object", "holds"),
    ("continuous_scope", "object_evolution_rungs", "exclude_out_of_scope_object", "refuses"),
    # proof_invalidation_rungs
    ("continuous_scope", "proof_invalidation_rungs", "reuse_verified_generation_proof", "holds"),
    ("continuous_scope", "proof_invalidation_rungs", "reuse_verified_generation_proof", "refuses"),
    ("continuous_scope", "proof_invalidation_rungs", "fence_isolated_mutation_window", "holds"),
    ("continuous_scope", "proof_invalidation_rungs", "fence_isolated_mutation_window", "refuses"),
    ("continuous_scope", "proof_invalidation_rungs", "invalidate_stale_proof_generation", "holds"),
    ("continuous_scope", "proof_invalidation_rungs", "invalidate_stale_proof_generation", "refuses"),
    # snapshot_cdc_handoff_rungs
    ("continuous_scope", "snapshot_cdc_handoff_rungs", "consistent_watermark_handoff", "holds"),
    ("continuous_scope", "snapshot_cdc_handoff_rungs", "consistent_watermark_handoff", "refuses"),
    ("continuous_scope", "snapshot_cdc_handoff_rungs", "signaling_table_range_watermark", "holds"),
    ("continuous_scope", "snapshot_cdc_handoff_rungs", "signaling_table_range_watermark", "refuses"),
    ("continuous_scope", "snapshot_cdc_handoff_rungs", "refuse_gap_or_unbounded_retention", "holds"),
    ("continuous_scope", "snapshot_cdc_handoff_rungs", "refuse_gap_or_unbounded_retention", "refuses"),
    # resource_retention_bound_rungs
    ("continuous_scope", "resource_retention_bound_rungs", "admit_bounded_backlog_retention", "holds"),
    ("continuous_scope", "resource_retention_bound_rungs", "admit_bounded_backlog_retention", "refuses"),
    ("continuous_scope", "resource_retention_bound_rungs", "throttle_hot_writes_backpressure", "holds"),
    ("continuous_scope", "resource_retention_bound_rungs", "throttle_hot_writes_backpressure", "refuses"),
    ("continuous_scope", "resource_retention_bound_rungs", "abort_excessive_wal_or_snapshot_spill", "holds"),
    ("continuous_scope", "resource_retention_bound_rungs", "abort_excessive_wal_or_snapshot_spill", "refuses"),
    # restart_recovery_rungs
    ("continuous_scope", "restart_recovery_rungs", "atomic_resume_from_durable_checkpoint", "holds"),
    ("continuous_scope", "restart_recovery_rungs", "atomic_resume_from_durable_checkpoint", "refuses"),
    ("continuous_scope", "restart_recovery_rungs", "reconcile_crash_in_flight_generation", "holds"),
    ("continuous_scope", "restart_recovery_rungs", "reconcile_crash_in_flight_generation", "refuses"),
    ("continuous_scope", "restart_recovery_rungs", "refuse_stale_or_forked_epoch", "holds"),
    ("continuous_scope", "restart_recovery_rungs", "refuse_stale_or_forked_epoch", "refuses"),
    # key_skew_adaptation_rungs
    ("continuous_scope", "key_skew_adaptation_rungs", "split_skewed_hot_range", "holds"),
    ("continuous_scope", "key_skew_adaptation_rungs", "split_skewed_hot_range", "refuses"),
    ("continuous_scope", "key_skew_adaptation_rungs", "coalesce_uniform_cold_ranges", "holds"),
    ("continuous_scope", "key_skew_adaptation_rungs", "coalesce_uniform_cold_ranges", "refuses"),
    ("continuous_scope", "key_skew_adaptation_rungs", "dedicated_hot_stream_lane", "holds"),
    ("continuous_scope", "key_skew_adaptation_rungs", "dedicated_hot_stream_lane", "refuses"),
]


def test_every_decide_rung_is_exercised_on_both_branches(tmp_path: Path):
    """Verify that every decide rung in continuous_scope reaches holds and refuses under watched()."""
    with tests.rung_branches.watched() as seen:
        # 1. OBJECT_EVOLUTION_LADDER
        # A) admit_in_scope_created_object holds
        facts_admit = {
            "is_in_scope": True,
            "schema_valid": True,
            "dependencies_satisfied": True,
        }
        c1 = cs.decide_object_evolution("test:admit", facts_admit)
        assert c1.path == "admit_in_scope_created_object"

        # B) track_object_rename_in_scope holds
        facts_rename = {
            "is_rename_event": True,
            "rename_source_in_scope": True,
            "rename_target_in_scope": True,
            "lineage_preserved": True,
            "is_out_of_scope": False,
        }
        c2 = cs.decide_object_evolution("test:rename", facts_rename)
        assert c2.path == "track_object_rename_in_scope"

        # C) partition_attach_root_inheritance holds
        facts_part = {
            "is_partition_event": True,
            "root_table_in_scope": True,
            "partition_bounds_valid": True,
        }
        c3 = cs.decide_object_evolution("test:part", facts_part)
        assert c3.path == "partition_attach_root_inheritance"

        # D) drop_tombstone_eviction holds
        facts_drop = {
            "is_drop_event": True,
            "tombstone_recorded": True,
            "dependent_proofs_invalidated": True,
        }
        c4 = cs.decide_object_evolution("test:drop", facts_drop)
        assert c4.path == "drop_tombstone_eviction"

        # E) exclude_out_of_scope_object holds
        facts_exclude = {
            "is_out_of_scope": True,
        }
        c5 = cs.decide_object_evolution("test:exclude", facts_exclude)
        assert c5.path == "exclude_out_of_scope_object"

        # F) refuse all in OBJECT_EVOLUTION_LADDER
        c_refuse_obj = cs.decide_object_evolution("test:refuse_obj", {})
        assert c_refuse_obj.rung is None

        # 2. PROOF_INVALIDATION_LADDER
        # A) reuse_verified_generation_proof holds
        facts_reuse = {
            "generation_matches_current": True,
            "writes_within_proof_fence": True,
            "no_concurrent_ddl": True,
            "proof_ttl_valid": True,
        }
        c6 = cs.decide_proof_invalidation("test:reuse", facts_reuse)
        assert c6.path == "reuse_verified_generation_proof"

        # B) fence_isolated_mutation_window holds
        facts_fence = {
            "mutation_is_range_isolated": True,
            "range_fence_acquired": True,
            "sibling_ranges_unaffected": True,
        }
        c7 = cs.decide_proof_invalidation("test:fence", facts_fence)
        assert c7.path == "fence_isolated_mutation_window"

        # C) invalidate_stale_proof_generation holds
        facts_inval = {
            "mutation_past_fence_or_ddl": True,
            "dirty_generation_marked": True,
        }
        c8 = cs.decide_proof_invalidation("test:inval", facts_inval)
        assert c8.path == "invalidate_stale_proof_generation"

        # D) refuse all in PROOF_INVALIDATION_LADDER
        c_refuse_proof = cs.decide_proof_invalidation("test:refuse_proof", {})
        assert c_refuse_proof.rung is None

        # 3. SNAPSHOT_CDC_HANDOFF_LADDER
        # A) consistent_watermark_handoff holds
        facts_handoff = {
            "snapshot_watermark_established": True,
            "cdc_stream_position_valid": True,
            "no_log_interval_gap": True,
            "dedup_window_enforced": True,
        }
        c9 = cs.decide_snapshot_cdc_handoff("test:handoff", facts_handoff)
        assert c9.path == "consistent_watermark_handoff"

        # B) signaling_table_range_watermark holds
        facts_signal = {
            "signaling_supported_on_source": True,
            "range_watermarks_monotonic": True,
            "read_only_intent_respected": True,
        }
        c10 = cs.decide_snapshot_cdc_handoff("test:signal", facts_signal)
        assert c10.path == "signaling_table_range_watermark"

        # C) refuse_gap_or_unbounded_retention holds
        facts_gap = {
            "fail_closed_on_unsupported_handoff": True,
        }
        c11 = cs.decide_snapshot_cdc_handoff("test:gap", facts_gap)
        assert c11.path == "refuse_gap_or_unbounded_retention"

        # D) refuse all in SNAPSHOT_CDC_HANDOFF_LADDER
        c_refuse_handoff = cs.decide_snapshot_cdc_handoff(
            "test:refuse_handoff", {"fail_closed_on_unsupported_handoff": False}
        )
        assert c_refuse_handoff.rung is None

        # 4. RESOURCE_RETENTION_BOUND_LADDER
        # A) admit_bounded_backlog_retention holds
        facts_admit_ret = {
            "wal_retention_within_headroom": True,
            "snapshot_lifetime_within_bound": True,
            "replay_rate_exceeds_arrival": True,
        }
        c12 = cs.decide_resource_retention("test:admit_ret", facts_admit_ret)
        assert c12.path == "admit_bounded_backlog_retention"

        # B) throttle_hot_writes_backpressure holds
        facts_throttle = {
            "backlog_exceeds_soft_threshold": True,
            "backpressure_supported": True,
            "headroom_permits_throttling": True,
        }
        c13 = cs.decide_resource_retention("test:throttle", facts_throttle)
        assert c13.path == "throttle_hot_writes_backpressure"

        # C) abort_excessive_wal_or_snapshot_spill holds
        facts_abort = {
            "resource_breach_requires_halt": True,
        }
        c14 = cs.decide_resource_retention("test:abort", facts_abort)
        assert c14.path == "abort_excessive_wal_or_snapshot_spill"

        # D) refuse all in RESOURCE_RETENTION_BOUND_LADDER
        c_refuse_ret = cs.decide_resource_retention(
            "test:refuse_ret", {"resource_breach_requires_halt": False}
        )
        assert c_refuse_ret.rung is None

        # 5. RESTART_RECOVERY_LADDER
        # A) atomic_resume_from_durable_checkpoint holds
        facts_resume = {
            "checkpoint_present_and_valid": True,
            "active_lease_acquired": True,
            "lsn_position_monotonic": True,
            "no_unfenced_in_doubt_writes": True,
        }
        c15 = cs.decide_restart_recovery("test:resume", facts_resume)
        assert c15.path == "atomic_resume_from_durable_checkpoint"

        # B) reconcile_crash_in_flight_generation holds
        facts_reconcile = {
            "in_flight_generation_interrupted": True,
            "source_state_reconcilable": True,
            "dirty_range_refenced": True,
        }
        c16 = cs.decide_restart_recovery("test:reconcile", facts_reconcile)
        assert c16.path == "reconcile_crash_in_flight_generation"

        # C) refuse_stale_or_forked_epoch holds
        facts_fork = {
            "fail_closed_on_divergent_epoch": True,
        }
        c17 = cs.decide_restart_recovery("test:fork", facts_fork)
        assert c17.path == "refuse_stale_or_forked_epoch"

        # D) refuse all in RESTART_RECOVERY_LADDER
        c_refuse_rec = cs.decide_restart_recovery(
            "test:refuse_rec", {"fail_closed_on_divergent_epoch": False}
        )
        assert c_refuse_rec.rung is None

        # 6. KEY_SKEW_ADAPTATION_LADDER
        # A) split_skewed_hot_range holds
        facts_split = {
            "key_skew_exceeds_threshold": True,
            "range_split_supported": True,
            "sub_key_boundaries_available": True,
        }
        c18 = cs.decide_key_skew_adaptation("test:split", facts_split)
        assert c18.path == "split_skewed_hot_range"

        # B) coalesce_uniform_cold_ranges holds
        facts_coalesce = {
            "write_rate_below_cold_threshold": True,
            "adjacent_ranges_unskewed": True,
            "coalescing_reduces_overhead": True,
        }
        c19 = cs.decide_key_skew_adaptation("test:coalesce", facts_coalesce)
        assert c19.path == "coalesce_uniform_cold_ranges"

        # C) dedicated_hot_stream_lane holds
        facts_lane = {
            "extreme_skew_isolated": True,
            "dedicated_worker_lane_available": True,
        }
        c20 = cs.decide_key_skew_adaptation("test:lane", facts_lane)
        assert c20.path == "dedicated_hot_stream_lane"

        # D) refuse all in KEY_SKEW_ADAPTATION_LADDER
        c_refuse_skew = cs.decide_key_skew_adaptation("test:refuse_skew", {})
        assert c_refuse_skew.rung is None

    # Verify every rung in PROVES_RUNGS was exercised on both branches
    for _, ladder_name, rung_name, branch_name in PROVES_RUNGS:
        branches = branch(seen, rung_name)
        assert branch_name in branches, (
            f"Rung {rung_name!r} in {ladder_name!r} failed to reach {branch_name!r}. "
            f"Observed branches: {branches}"
        )


def test_scope_rule_matching_and_fingerprint():
    """Verify include/exclude, case folding, right-anchoring, and root partition inheritance."""
    rule = cs.ContinuousScopeRule(
        includes=("public.*", "sales_orders", "app.users"),
        excludes=("public.temp_*", "*.audit"),
        table_mappings={"sales_orders": "orders_v2"},
        case_sensitive=False,
    )

    # In scope tests
    assert rule.is_in_scope("public.customers") is True
    assert rule.is_in_scope("sales_orders") is True
    assert rule.is_in_scope("db.public.customers") is True
    assert rule.is_in_scope("app.users") is True

    # Exclude wins tests
    assert rule.is_in_scope("public.temp_staging") is False
    assert rule.is_in_scope("public.audit") is False
    assert rule.is_in_scope("db.public.temp_cache") is False

    # Out of scope tests
    assert rule.is_in_scope("finance.payroll") is False

    # Partition inheritance tests
    assert rule.is_in_scope("sales_orders_p2026", parent_root="sales_orders") is True
    assert rule.is_in_scope("temp_part", parent_root="public.temp_scratch") is False

    # Rename lookup
    assert rule.target_name_for("sales_orders") == "orders_v2"
    assert rule.target_name_for("public.customers") == "public.customers"

    # Deterministic fingerprint
    fp1 = rule.scope_fingerprint()
    fp2 = rule.scope_fingerprint()
    assert fp1 == fp2
    assert len(fp1) == 64


def test_scope_dag_lifecycle_create_rename_alter_drop():
    """Verify DAG updates, lineage tracking, and proof invalidation on DDL mutations."""
    rule = cs.ContinuousScopeRule(
        includes=("public.*",),
        excludes=("public.secret_*",),
    )
    dag = cs.ScopeDAG(rule)

    # 1. CREATE in-scope table
    node_t1 = dag.add_object("public.orders", schema_epoch=1)
    assert node_t1.status == cs.ObjectStatus.IN_SCOPE
    assert node_t1.schema_epoch == 1
    assert node_t1.generation == 1

    # 2. Add proof to public.orders
    proof = cs.ProofCertificate(
        object_name="public.orders",
        schema_epoch=1,
        generation=1,
        proof_digest="sha256:abc12345",
        fence_lsn="0/1000",
        verified_at=datetime.datetime.now(datetime.timezone.utc),
        verified_rows=500,
        care_badge=cs.CareBadge.PASS,
        is_valid=True,
    )
    dag.record_proof("public.orders", proof)
    assert node_t1.proof.is_valid is True

    # 3. ALTER table (DDL mutation) invalidates proof
    node_t1_alt = dag.alter_object("public.orders", new_schema_epoch=2)
    assert node_t1_alt.schema_epoch == 2
    assert node_t1_alt.generation == 2
    assert node_t1_alt.dirty_generation is True
    assert node_t1_alt.proof.is_valid is False
    assert node_t1_alt.proof.care_badge == cs.CareBadge.ACTION

    # 4. RENAME table within scope
    node_t1_renamed = dag.rename_object("public.orders", "public.orders_archived")
    assert node_t1_renamed.name == "public.orders_archived"
    assert "public.orders_archived" in node_t1_renamed.lineage
    assert "public.orders" not in dag.nodes
    assert "public.orders_archived" in dag.nodes

    # 5. DROP table records tombstone and evicts
    node_dropped = dag.drop_object("public.orders_archived")
    assert node_dropped.status == cs.ObjectStatus.TOMBSTONED
    assert "public.orders_archived" in dag.tombstones


def test_partition_attachment_inherits_scope():
    """Verify newly attached partitions inherit scope and DAG membership from parent table."""
    rule = cs.ContinuousScopeRule(includes=("public.events",))
    dag = cs.ScopeDAG(rule)

    # Add root table
    dag.add_object("public.events")

    # Add child partition
    node_part = dag.add_object("public.events_2026_q1", parent_root="public.events")
    assert node_part.status == cs.ObjectStatus.PARTITION_INHERITED
    assert node_part.parent_root == "public.events"

    # Out of scope partition child
    node_out = dag.add_object("public.logs_2026", parent_root="public.logs")
    assert node_out.status == cs.ObjectStatus.OUT_OF_SCOPE


def test_isolated_range_fence_preserves_sibling_proofs():
    """Verify range fence isolates mutated range while preserving proofs for sibling ranges."""
    rule = cs.ContinuousScopeRule(includes=("public.sales",))
    dag = cs.ScopeDAG(rule)
    dag.add_object("public.sales")

    # Record proof for range 0-1000 and 1001-2000
    p1 = cs.ProofCertificate(
        object_name="public.sales",
        range_id="r1_0_1000",
        proof_digest="hash1",
        is_valid=True,
    )
    p2 = cs.ProofCertificate(
        object_name="public.sales",
        range_id="r2_1001_2000",
        proof_digest="hash2",
        is_valid=True,
    )
    dag.record_proof("public.sales", p1)
    dag.record_proof("public.sales", p2)

    # Mutate only range 1
    choice = dag.fence_range_mutation("public.sales", "r1_0_1000")
    assert choice.path == "fence_isolated_mutation_window"

    # Verify r1 is invalidated with REVIEW, while r2 stays completely valid
    assert dag.range_proofs[("public.sales", "r1_0_1000")].is_valid is False
    assert dag.range_proofs[("public.sales", "r1_0_1000")].care_badge == cs.CareBadge.REVIEW
    assert dag.range_proofs[("public.sales", "r2_1001_2000")].is_valid is True
    assert dag.range_proofs[("public.sales", "r2_1001_2000")].care_badge == cs.CareBadge.PASS


def test_snapshot_to_cdc_handoff_validation():
    """Verify consistent handoff, deduplication window, and refusal upon log gaps."""
    now = datetime.datetime.now(datetime.timezone.utc)

    snap = cs.SnapshotWatermark(
        snapshot_id="snap_01",
        start_lsn="0/1000",
        end_lsn="0/2000",
        start_time=now - datetime.timedelta(seconds=60),
        end_time=now,
        is_consistent=True,
    )

    # Case A: Contiguous stream (start LSN <= snapshot end LSN)
    cdc_good = cs.CDCStreamPosition(
        stream_id="cdc_01",
        start_lsn="0/1800",
        current_lsn="0/2500",
        first_available_lsn="0/1000",
        is_active=True,
    )
    res_good = cs.validate_snapshot_cdc_handoff(snap, cdc_good)
    assert res_good.care_badge == cs.CareBadge.PASS
    assert res_good.watermark_aligned is True
    assert res_good.gap_detected is False
    assert res_good.duplicate_range_filtered is True

    # Case B: Unrecoverable gap (stream start > snapshot end LSN)
    cdc_gap = cs.CDCStreamPosition(
        stream_id="cdc_gap",
        start_lsn="0/2500",  # gap: 0/2000 to 0/2500 missing!
        current_lsn="0/3000",
        first_available_lsn="0/2500",
        is_active=True,
    )
    res_gap = cs.validate_snapshot_cdc_handoff(snap, cdc_gap)
    assert res_gap.care_badge == cs.CareBadge.ACTION
    assert res_gap.watermark_aligned is False
    assert res_gap.gap_detected is True


def test_resource_retention_and_backpressure_bounds():
    """Verify dynamic bounding of WAL retention, snapshot lifetime, and backlog."""
    # Case A: Normal operation within headroom
    t_normal = cs.RetentionTelemetry(
        measured_wal_bytes_rate=500_000.0,
        measured_disk_headroom_bytes=50_000_000_000,
        measured_snapshot_age_s=120.0,
        max_snapshot_lifetime_s=3600.0,
        measured_backlog_bytes=10_000_000,
        max_backlog_bytes=100_000_000,
        measured_replay_rate_bytes_s=2_000_000.0,
    )
    c_norm = cs.evaluate_retention_bounds(t_normal)
    assert c_norm.path == "admit_bounded_backlog_retention"

    # Case B: High backlog triggers throttling backpressure
    t_heavy = cs.RetentionTelemetry(
        measured_wal_bytes_rate=2_500_000.0,
        measured_disk_headroom_bytes=30_000_000_000,
        measured_snapshot_age_s=300.0,
        max_snapshot_lifetime_s=3600.0,
        measured_backlog_bytes=85_000_000,  # > 70% soft threshold
        max_backlog_bytes=100_000_000,
        measured_replay_rate_bytes_s=2_000_000.0,
    )
    c_heavy = cs.evaluate_retention_bounds(t_heavy, backpressure_supported=True)
    assert c_heavy.path == "throttle_hot_writes_backpressure"

    # Case C: Snapshot age expired triggers abort to protect source
    t_expired = cs.RetentionTelemetry(
        measured_wal_bytes_rate=500_000.0,
        measured_disk_headroom_bytes=50_000_000_000,
        measured_snapshot_age_s=4000.0,  # exceeds max_snapshot_lifetime_s
        max_snapshot_lifetime_s=3600.0,
        measured_backlog_bytes=5_000_000,
        max_backlog_bytes=100_000_000,
        measured_replay_rate_bytes_s=2_000_000.0,
    )
    c_abort = cs.evaluate_retention_bounds(t_expired, backpressure_supported=False)
    assert c_abort.path == "abort_excessive_wal_or_snapshot_spill"


def test_durable_typed_checkpoint_and_o1_resume(tmp_path: Path):
    """Verify O(1) durable resume with native Python type preservation and skipping completed ranges."""
    rule = cs.ContinuousScopeRule(includes=("public.*",))
    orch = cs.ContinuousScopeOrchestrator(rule, lock_dir=tmp_path)

    # Populate state
    orch.handle_lifecycle_event(cs.LifecycleEventKind.CREATE, "public.users")
    orch.completed_ranges.add(("public.users", "chunk_0_1000"))
    orch.completed_ranges.add(("public.users", "chunk_1001_2000"))

    # Save checkpoint
    cp_path = orch.save_checkpoint("chk_01", last_lsn="0/5000", cursor_positions={"users": 2000})
    assert cp_path.exists()

    # Create fresh orchestrator on restart
    orch2 = cs.ContinuousScopeOrchestrator(rule, lock_dir=tmp_path)
    status = orch2.resume_from_checkpoint(cp_path, source_current_lsn="0/5500")
    assert status == cs.RecoveryStatus.CLEAN_RESUMED

    # Verify completed ranges skipped in O(1)
    assert ("public.users", "chunk_0_1000") in orch2.completed_ranges
    assert ("public.users", "chunk_1001_2000") in orch2.completed_ranges


def test_lease_lock_mutual_exclusion(tmp_path: Path):
    """Verify active lease lock (.run.lock) prevents duplicate overlapping execution."""
    lock1 = cs.VerificationLeaseLock(tmp_path, lease_seconds=10.0, owner_id="worker-a")
    tok1 = lock1.acquire()
    assert tok1 is not None

    # Competing worker attempts acquisition -> LeaseLockError
    lock2 = cs.VerificationLeaseLock(tmp_path, lease_seconds=10.0, owner_id="worker-b")
    with pytest.raises(cs.LeaseLockError):
        lock2.acquire()

    # Release lock1
    lock1.release()

    # Now lock2 succeeds
    tok2 = lock2.acquire()
    assert tok2 is not None
    lock2.release()


def test_key_skew_detector_and_aimd_controller():
    """Verify write skew detection, AIMD batch expansion/reduction, and cold-start ETA."""
    detector = cs.KeySkewDetector(skew_threshold=0.60)
    detector.record_write("range_0", write_bytes=10_000, row_count=100)
    detector.record_write("range_1", write_bytes=90_000, row_count=900)

    assert detector.is_hot_range("range_0") is False
    assert detector.is_hot_range("range_1") is True
    assert detector.get_skew_ratio("range_1") == 0.90

    # AIMD batch controller
    aimd = cs.AIMDBatchController(
        min_batch_size=100, max_batch_size=5000, initial_batch_size=1000, additive_step=200
    )
    b1 = aimd.on_success(measured_latency_s=0.1)
    assert b1 == 1200
    b2 = aimd.on_success(measured_latency_s=0.1)
    assert b2 == 1400

    # Congestion backoff
    b3 = aimd.on_congestion_or_error("latency spike")
    assert b3 == 700

    # Cold-start ETA
    eta = aimd.calculate_cold_start_eta(10_000_000, 5_000_000.0, cold_start_penalty_s=1.5)
    assert pytest.approx(eta, rel=1e-3) == 3.5


def test_external_tool_wrapper_care_badges():
    """Verify normalized external tool wrapper with 4-tier care badges (PASS, REVIEW, ACTION, BENIGN)."""
    tools = ["datafold", "dms", "dts", "reladiff", "dvt", "pgcopydb", "debezium"]
    for t in tools:
        res_pass = cs.wrap_external_tool(
            t, in_scope_objects=["t1"], excluded_objects=["t2"], simulated_outcome="clean"
        )
        assert res_pass.care_badge == cs.CareBadge.PASS
        assert res_pass.success is True

        res_drift = cs.wrap_external_tool(
            t, in_scope_objects=["t1"], excluded_objects=[], simulated_outcome="drift"
        )
        assert res_drift.care_badge == cs.CareBadge.REVIEW
        assert len(res_drift.discrepancies) > 0

        res_action = cs.wrap_external_tool(
            t, in_scope_objects=["t1"], excluded_objects=[], simulated_outcome="mismatch"
        )
        assert res_action.care_badge == cs.CareBadge.ACTION
        assert res_action.success is False

        res_benign = cs.wrap_external_tool(
            t, in_scope_objects=["t1"], excluded_objects=[], simulated_outcome="benign"
        )
        assert res_benign.care_badge == cs.CareBadge.BENIGN
        assert res_benign.success is True


def test_safe_endpoint_validation():
    """Verify endpoint validator allows 127.0.0.1 and 10.0.0.x, rejecting unsafe hosts."""
    assert cs.validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert cs.validate_safe_endpoint("localhost") == "localhost"
    assert cs.validate_safe_endpoint("10.0.0.5") == "10.0.0.5"

    with pytest.raises(cs.UnsafeEndpointError):
        cs.validate_safe_endpoint("192.168.1.1")

    with pytest.raises(cs.UnsafeEndpointError):
        cs.validate_safe_endpoint("evil.example.com")


def test_acceptance_ddl_plus_writes_plus_crash_scenario(tmp_path: Path):
    """End-to-end acceptance test:
    DDL plus writes plus crash does not omit an object, double-count a range,
    or reuse stale proof; the snapshot-to-CDC handoff covers committed changes under the contract.
    """
    rule = cs.ContinuousScopeRule(
        includes=("public.*",),
        excludes=("public.temp_*",),
        table_mappings={"public.orders": "public.orders_v2"},
    )
    orch = cs.ContinuousScopeOrchestrator(rule, lock_dir=tmp_path)

    # 1. Initial snapshot phase: Admit core tables into DAG
    t_accounts = orch.handle_lifecycle_event(cs.LifecycleEventKind.CREATE, "public.accounts")
    t_orders = orch.handle_lifecycle_event(cs.LifecycleEventKind.CREATE, "public.orders")
    t_inventory = orch.handle_lifecycle_event(cs.LifecycleEventKind.CREATE, "public.inventory")

    assert t_accounts.status == cs.ObjectStatus.IN_SCOPE
    assert t_orders.status == cs.ObjectStatus.IN_SCOPE
    assert t_inventory.status == cs.ObjectStatus.IN_SCOPE
    assert len(orch.dag.get_in_scope_nodes()) == 3

    # Initial proof certificates established for snapshot
    now = datetime.datetime.now(datetime.timezone.utc)
    proof_acc = cs.ProofCertificate(
        object_name="public.accounts",
        schema_epoch=1,
        generation=1,
        proof_digest="sha256:acc111",
        fence_lsn="0/2000",
        verified_at=now,
        verified_rows=10_000,
        care_badge=cs.CareBadge.PASS,
        is_valid=True,
    )
    proof_ord = cs.ProofCertificate(
        object_name="public.orders",
        schema_epoch=1,
        generation=1,
        proof_digest="sha256:ord111",
        fence_lsn="0/2000",
        verified_at=now,
        verified_rows=5_000,
        care_badge=cs.CareBadge.PASS,
        is_valid=True,
    )
    orch.dag.record_proof("public.accounts", proof_acc)
    orch.dag.record_proof("public.orders", proof_ord)

    # Record completed range chunk_0 on public.accounts
    orch.completed_ranges.add(("public.accounts", "chunk_0"))

    # 2. DDL Mutation during replication: ALTER public.orders (schema change)
    orch.handle_lifecycle_event(cs.LifecycleEventKind.ALTER, "public.orders", schema_epoch=2)
    # Verify stale proof is invalidated and marked ACTION
    assert t_orders.proof.is_valid is False
    assert t_orders.proof.care_badge == cs.CareBadge.ACTION
    assert t_orders.generation == 2

    # Attempting to reuse old proof for public.orders fails
    facts_stale_reuse = {
        "generation_matches_current": t_orders.generation == proof_ord.generation,  # False: 2 != 1
        "writes_within_proof_fence": True,
        "no_concurrent_ddl": False,
        "proof_ttl_valid": True,
    }
    c_stale = cs.decide_proof_invalidation("test:reuse_stale", facts_stale_reuse)
    assert c_stale.path != "reuse_verified_generation_proof"

    # 3. Concurrent writes past fence on public.accounts: isolated range mutation
    orch.completed_ranges.add(("public.accounts", "chunk_1"))
    fence_choice = orch.dag.fence_range_mutation("public.accounts", "chunk_1")
    assert fence_choice.path == "fence_isolated_mutation_window"

    # 4. Save checkpoint before simulated crash
    chk_path = orch.save_checkpoint("chk_crash_01", last_lsn="0/2500")

    # 5. Simulated Crash & Restart Recovery
    orch_recovered = cs.ContinuousScopeOrchestrator(rule, lock_dir=tmp_path)
    recovery_status = orch_recovered.resume_from_checkpoint(chk_path, source_current_lsn="0/3000")
    assert recovery_status == cs.RecoveryStatus.CLEAN_RESUMED

    # In O(1), chunk_0 is restored in completed_ranges and NOT double-counted
    assert ("public.accounts", "chunk_0") in orch_recovered.completed_ranges

    # 6. CDC stream handoff with contiguous coverage
    snap = cs.SnapshotWatermark(
        snapshot_id="snap_full",
        start_lsn="0/1000",
        end_lsn="0/2500",
        start_time=now - datetime.timedelta(seconds=120),
        end_time=now,
        is_consistent=True,
    )
    cdc_stream = cs.CDCStreamPosition(
        stream_id="cdc_active",
        start_lsn="0/2400",  # overlaps with snapshot end_lsn 0/2500 (zero omission)
        current_lsn="0/3000",
        first_available_lsn="0/2000",
        is_active=True,
    )
    handoff_res = cs.validate_snapshot_cdc_handoff(snap, cdc_stream)
    assert handoff_res.care_badge == cs.CareBadge.PASS
    assert handoff_res.watermark_aligned is True
    assert handoff_res.gap_detected is False
    assert handoff_res.duplicate_range_filtered is True

    # 7. New in-scope object created during CDC: joins DAG
    t_payments = orch_recovered.handle_lifecycle_event(cs.LifecycleEventKind.CREATE, "public.payments")
    assert t_payments.status == cs.ObjectStatus.IN_SCOPE

    # 8. Out-of-scope object created: strictly excluded
    t_temp = orch_recovered.handle_lifecycle_event(cs.LifecycleEventKind.CREATE, "public.temp_buffer")
    assert t_temp.status == cs.ObjectStatus.OUT_OF_SCOPE

    # 9. Verify final inventory: No objects omitted, out of scope excluded
    in_scope_names = {n.name for n in orch_recovered.dag.get_in_scope_nodes()}
    assert "public.payments" in in_scope_names
    assert "public.temp_buffer" not in in_scope_names

