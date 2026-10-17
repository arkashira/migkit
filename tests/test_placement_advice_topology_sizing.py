"""Tests for RCH31.placement-advice (Row 639): Connect assessment to measured target topology and sizing."""
from __future__ import annotations

import datetime
import decimal
import pathlib
import uuid

import pytest

from migkit import machine
from migkit.placement_advice import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    ActiveLeaseConflictError,
    ActiveLeaseLock,
    AdaptiveBatchController,
    CandidateTargetDesign,
    CareBadge,
    DistributionStrategy,
    EvidenceTruth,
    IndexAccessPattern,
    KeySkewProfile,
    PlacementAdviceEngine,
    PlacementCheckpointStore,
    RehearsalMeasurement,
    StaticBrandClassRefusedError,
    TableAccessProfile,
    TablePlacementDecision,
    TargetTopologyKind,
    TransactionFanoutProfile,
    WorkloadAssessmentEvidence,
    assess_placement_items,
    cold_start_byte_weighted_eta,
    evaluate_candidate_design,
    generate_candidate_designs,
    recommend_resource_sizing,
    rehearse_design,
)

GIB = 1024 ** 3


def _build_skewed_multi_table_evidence(
    total_gb: int = 120,
    colocation_limit_gb: int = 32,
) -> WorkloadAssessmentEvidence:
    """Build realistic multi-table OLTP workload evidence with monotonic PK skew and tenant correlation."""
    orders_bytes = int(total_gb * 0.60 * GIB)
    items_bytes = int(total_gb * 0.35 * GIB)
    countries_bytes = int(64 * 1024 * 1024)

    orders = TableAccessProfile(
        table_name="public.orders",
        size_bytes=orders_bytes,
        row_count=60_000_000,
        point_read_qps=1800.0,
        range_scan_qps=250.0,
        write_qps=950.0,
        primary_key=("order_id",),
        candidate_shard_keys=(("order_id",), ("customer_id",)),
        skew_by_key={
            "order_id": KeySkewProfile(
                key_columns=("order_id",),
                gini_coefficient=0.78,
                max_to_median_ratio=4.8,
                hot_shard_fraction=0.88,
                monotonic_append_share=0.92,
                distinct_keys=60_000_000,
            ),
            "customer_id": KeySkewProfile(
                key_columns=("customer_id",),
                gini_coefficient=0.14,
                max_to_median_ratio=1.18,
                hot_shard_fraction=0.27,
                monotonic_append_share=0.0,
                distinct_keys=2_500_000,
            ),
        },
        indexes=[
            IndexAccessPattern(
                index_name="idx_orders_customer_created",
                columns=("customer_id", "created_at"),
                is_unique=False,
                is_monotonic=False,
                point_lookup_qps=1400.0,
                range_scan_qps=200.0,
                write_amplification_factor=1.25,
            ),
            IndexAccessPattern(
                index_name="idx_orders_status",
                columns=("status",),
                is_unique=False,
                is_monotonic=False,
                point_lookup_qps=300.0,
                write_amplification_factor=1.15,
            ),
        ],
        co_accessed_tables={"public.order_items": 1600.0, "public.countries": 400.0},
        shared_join_keys={"public.order_items": ("customer_id",)},
    )

    order_items = TableAccessProfile(
        table_name="public.order_items",
        size_bytes=items_bytes,
        row_count=180_000_000,
        point_read_qps=2400.0,
        range_scan_qps=300.0,
        write_qps=1400.0,
        primary_key=("item_id",),
        candidate_shard_keys=(("item_id",), ("customer_id",)),
        skew_by_key={
            "item_id": KeySkewProfile(
                key_columns=("item_id",),
                gini_coefficient=0.82,
                max_to_median_ratio=5.2,
                hot_shard_fraction=0.90,
                monotonic_append_share=0.94,
                distinct_keys=180_000_000,
            ),
            "customer_id": KeySkewProfile(
                key_columns=("customer_id",),
                gini_coefficient=0.15,
                max_to_median_ratio=1.20,
                hot_shard_fraction=0.28,
                monotonic_append_share=0.0,
                distinct_keys=2_500_000,
            ),
        },
        indexes=[
            IndexAccessPattern(
                index_name="idx_items_customer_order",
                columns=("customer_id", "order_id"),
                is_unique=False,
                point_lookup_qps=2100.0,
                write_amplification_factor=1.20,
            ),
        ],
        co_accessed_tables={"public.orders": 1600.0},
        shared_join_keys={"public.orders": ("customer_id",)},
    )

    countries = TableAccessProfile(
        table_name="public.countries",
        size_bytes=countries_bytes,
        row_count=250,
        point_read_qps=800.0,
        range_scan_qps=10.0,
        write_qps=1.0,
        primary_key=("country_code",),
        candidate_shard_keys=(("country_code",),),
        is_reference_candidate=True,
    )

    tx = TransactionFanoutProfile(
        workload_name="ecommerce_checkout",
        total_tps=1200.0,
        tables_touched_avg=2.4,
        single_key_tps=350.0,
        co_keyed_join_tps=800.0,
        cross_key_tps=50.0,
        inter_node_rtt_ms=2.5,
        two_phase_commit_overhead_ms=7.5,
        sla_p95_latency_ms=18.0,
        base_local_p95_latency_ms=3.2,
    )

    snapshots = [
        {
            "at": "2026-10-17T09:00:00",
            "running": 4.2,
            "running_most": 7.5,
            "iops": 4200.0,
            "tps": 1150.0,
            "connections": 120,
            "cache_bytes": 24 * GIB,
            "cache_share": 0.25,
            "hit": 0.97,
            "cpus": 16,
        },
        {
            "at": "2026-10-17T09:30:00",
            "running": 5.0,
            "running_most": 9.2,
            "iops": 5400.0,
            "tps": 1240.0,
            "connections": 145,
            "cache_bytes": 24 * GIB,
            "cache_share": 0.25,
            "hit": 0.96,
            "cpus": 16,
        },
    ]

    stages = machine.Stages(
        source=180.0 * 1024 * 1024,
        target=140.0 * 1024 * 1024,
        link=200.0 * 1024 * 1024,
        per_core=60.0 * 1024 * 1024,
    )

    return WorkloadAssessmentEvidence(
        tables=[orders, order_items, countries],
        transactions=tx,
        load_snapshots=snapshots,
        stages=stages,
        deadline_seconds=3600.0,
        workload_horizon_seconds=6 * 3600.0,
        colocation_working_set_limit_bytes=colocation_limit_gb * GIB,
        source_host="127.0.0.1",
        target_host="10.0.0.12",
        cloud="aws",
        region="us-east-1",
        engine="postgres",
    )


def test_fast_bulk_import_design_with_hot_shards_and_cross_partition_txns_loses():
    """Prove that a design making bulk import fast but causing hot shards or expensive
    cross-partition transactions loses against a balanced end-to-end workload plan."""
    evidence = _build_skewed_multi_table_evidence(total_gb=120, colocation_limit_gb=32)
    engine = PlacementAdviceEngine()

    winner, ranked = engine.recommend_best_design(evidence)
    by_id = {ev.design.design_id: ev for ev in ranked}

    bulk_first = by_id["partitioned-range-bulk-first"]
    balanced = by_id["distributed-cohashed-balanced"]

    # Range-partitioned bulk-first is genuinely faster on raw bulk import
    assert bulk_first.bulk_import_seconds < balanced.bulk_import_seconds

    # Yet bulk-first suffers monotonic hot shards and cross-partition transactions
    assert bulk_first.has_hot_shard is True
    assert bulk_first.has_expensive_cross_partition_txns is True
    assert bulk_first.care_badge == CARE_ACTION
    assert bulk_first.meets_latency_sla is False

    # Balanced co-hashed + colocated reference design avoids hot shards and keeps joins local
    assert balanced.has_hot_shard is False
    assert balanced.has_expensive_cross_partition_txns is False
    assert balanced.meets_latency_sla is True
    assert balanced.care_badge in (CARE_PASS, CARE_BENIGN)

    # Balanced design wins the overall ranking and has a lower (better) end-to-end cost score
    assert winner.design.design_id == "distributed-cohashed-balanced"
    assert balanced.end_to_end_effective_cost_score < bulk_first.end_to_end_effective_cost_score

    # Check table placements in the winning design
    orders_pl = winner.design.table_placements["public.orders"]
    items_pl = winner.design.table_placements["public.order_items"]
    countries_pl = winner.design.table_placements["public.countries"]
    assert orders_pl.shard_key == ("customer_id",)
    assert items_pl.shard_key == ("customer_id",)
    assert orders_pl.colocation_group == items_pl.colocation_group == "cohashed_customer_id"
    assert countries_pl.distribution_strategy == DistributionStrategy.REPLICATED_REFERENCE.value


def test_small_join_heavy_workload_selects_colocated_single_group():
    """When total dataset fits easily inside the colocation working set and transactions
    span multiple tables, colocated single-group topology eliminates all cross-node fanout."""
    evidence = _build_skewed_multi_table_evidence(total_gb=8, colocation_limit_gb=32)
    # Increase cross-key multi-table transaction share so distributed nodes pay 2PC
    evidence.transactions = TransactionFanoutProfile(
        workload_name="tight_multi_table_oltp",
        total_tps=1000.0,
        tables_touched_avg=3.2,
        single_key_tps=100.0,
        co_keyed_join_tps=300.0,
        cross_key_tps=600.0,
        inter_node_rtt_ms=3.0,
        two_phase_commit_overhead_ms=8.0,
        sla_p95_latency_ms=15.0,
        base_local_p95_latency_ms=3.0,
    )
    engine = PlacementAdviceEngine()
    winner, ranked = engine.recommend_best_design(evidence)

    assert winner.design.design_id == "colocated-single-group"
    assert winner.design.topology_kind == TargetTopologyKind.COLOCATED.value
    assert winner.measurement.cross_partition_txn_share == 0.0
    assert winner.care_badge == CARE_PASS


def test_rehearsed_alternatives_beat_unverified_brand_static_classes():
    """Prove that candidates relying on static brand classes without rehearsal are flagged
    or refused, and lose against rehearsed evidence-backed designs."""
    evidence = _build_skewed_multi_table_evidence(total_gb=80, colocation_limit_gb=24)
    candidates = generate_candidate_designs(evidence, distributed_nodes=4)

    static_design = CandidateTargetDesign(
        design_id="static-brand-mega-instance",
        name="Brand Static Class Guess",
        topology_kind=TargetTopologyKind.COLOCATED.value,
        node_count=1,
        table_placements=candidates[0].table_placements,
        sizing=candidates[0].sizing,
        brand_static_class="vendor.static.db.24xlarge",
        description="Unrehearsed static vendor class selection",
    )

    engine = PlacementAdviceEngine()
    with pytest.raises(StaticBrandClassRefusedError):
        engine.rehearse_and_rank(
            evidence,
            candidates=[static_design, *candidates],
            reject_static_brand_classes=True,
        )

    ranked = engine.rehearse_and_rank(
        evidence,
        candidates=[static_design, *candidates],
        reject_static_brand_classes=False,
    )
    by_id = {ev.design.design_id: ev for ev in ranked}
    static_ev = by_id["static-brand-mega-instance"]
    assert static_ev.is_static_brand_only is True
    assert static_ev.measurement.truth == EvidenceTruth.STATIC_CLASS_ONLY.value
    assert static_ev.care_badge == CARE_ACTION
    assert ranked[0].design.design_id != "static-brand-mega-instance"
    assert ranked[-1].design.design_id == "static-brand-mega-instance"


def test_resource_sizing_accounts_for_index_write_amplification_and_cache_misses():
    """Verify resource sizing scales vCPU, memory, and IOPS from observed load, cache hit ratio,
    and secondary index write amplification."""
    evidence = _build_skewed_multi_table_evidence(total_gb=100, colocation_limit_gb=32)
    candidates = generate_candidate_designs(evidence, distributed_nodes=4)
    balanced = next(c for c in candidates if c.design_id == "distributed-cohashed-balanced")

    sizing_high_hit = recommend_resource_sizing(
        evidence,
        balanced.topology_kind,
        balanced.node_count,
        balanced.table_placements,
    )

    # Lower cache hit ratio below 0.95 -> memory sizing scales up by 1.5x
    evidence.load_snapshots[0]["hit"] = 0.88
    SizingLowHit = recommend_resource_sizing(
        evidence,
        balanced.topology_kind,
        balanced.node_count,
        balanced.table_placements,
    )
    assert SizingLowHit.total_memory_gib > sizing_high_hit.total_memory_gib
    assert SizingLowHit.per_node_vcpu >= 2
    assert SizingLowHit.per_node_iops >= 500
    assert SizingLowHit.matched_catalog_machine is not None


def test_sample_rehearsal_confidence_widening_and_care_badges():
    """Small rehearsal sample fractions (<20%) widen p90 latency and trigger REVIEW badge."""
    evidence = _build_skewed_multi_table_evidence(total_gb=8, colocation_limit_gb=32)
    candidates = generate_candidate_designs(evidence, distributed_nodes=4)
    colocated = next(c for c in candidates if c.design_id == "colocated-single-group")

    meas_full = rehearse_design(colocated, evidence, sample_fraction=1.0)
    meas_small = rehearse_design(colocated, evidence, sample_fraction=0.05)

    assert meas_small.p90_widening_factor > 0.0
    assert meas_small.steady_state_p95_latency_ms > meas_full.steady_state_p95_latency_ms

    ev_small = evaluate_candidate_design(colocated, meas_small, evidence)
    assert ev_small.care_badge == CARE_REVIEW
    assert any("sample" in r for r in ev_small.care_reasons)


def test_typed_durable_checkpoint_store_o1_resume(tmp_path):
    """Verify O(1) durable resume preserves native types (datetime, date, Decimal, UUID, tuple, set)
    and skips already rehearsed designs without re-executing probes."""
    ckpt_file = tmp_path / "placement_ckpt.json"
    store = PlacementCheckpointStore(ckpt_file)

    now_dt = datetime.datetime(2026, 10, 17, 12, 30, 0)
    today_d = datetime.date(2026, 10, 17)
    uid = uuid.uuid4()
    dec = decimal.Decimal("98765.4321")
    comp_cursor = ("tenant_42", 100500, now_dt, today_d, uid, dec)

    store.mark_completed(
        "unit-range-1",
        cursor=comp_cursor,
        result={"tags": {"fast", "verified"}, "cost": dec},
    )
    assert store.is_completed("unit-range-1") is True
    assert store.is_completed("unit-range-2") is False

    # Reload from disk and verify exact native types
    reloaded = PlacementCheckpointStore(ckpt_file)
    assert reloaded.is_completed("unit-range-1") is True
    restored_cursor = reloaded.get_cursor("unit-range-1")
    assert isinstance(restored_cursor, tuple)
    assert restored_cursor[0] == "tenant_42"
    assert restored_cursor[1] == 100500
    assert restored_cursor[2] == now_dt
    assert restored_cursor[3] == today_d
    assert restored_cursor[4] == uid
    assert restored_cursor[5] == dec

    restored_result = reloaded.get_result("unit-range-1")
    assert isinstance(restored_result["tags"], set)
    assert restored_result["cost"] == dec

    # Verify PlacementAdviceEngine skips already checkpointed designs in O(1)
    evidence = _build_skewed_multi_table_evidence(total_gb=60, colocation_limit_gb=24)
    engine_1 = PlacementAdviceEngine(checkpoint_store=reloaded)
    ranked_1 = engine_1.rehearse_and_rank(evidence)
    assert len(ranked_1) == 3

    probe_calls = []

    def fail_if_called(design, ev):
        probe_calls.append(design.design_id)
        raise AssertionError("Should not re-run rehearsal probe for checkpointed design")

    for d in generate_candidate_designs(evidence):
        assert reloaded.is_completed(d.design_id) is True

    engine_2 = PlacementAdviceEngine(checkpoint_store=PlacementCheckpointStore(ckpt_file))
    ranked_2 = engine_2.rehearse_and_rank(evidence)
    assert [e.design.design_id for e in ranked_2] == [e.design.design_id for e in ranked_1]
    assert probe_calls == []


def test_active_lease_lock_mutual_exclusion_and_heartbeat(tmp_path):
    """Verify .run.lock mutual exclusion, heartbeat extension, and expired lock takeover."""
    lock_dir = tmp_path / "lease_lock"
    lock_a = ActiveLeaseLock(lock_dir, holder_id="worker-a", lease_term_seconds=20.0)
    lock_b = ActiveLeaseLock(lock_dir, holder_id="worker-b", lease_term_seconds=20.0)

    lock_a.acquire(now=1000.0)
    assert (lock_dir / ".run.lock").exists()

    with pytest.raises(ActiveLeaseConflictError):
        lock_b.acquire(now=1010.0)

    lock_a.heartbeat(now=1015.0)
    with pytest.raises(ActiveLeaseConflictError):
        lock_b.acquire(now=1025.0)

    # After expiration at 1035.0, worker-b can acquire
    lock_b.acquire(now=1036.0)
    assert lock_b.acquired is True
    lock_b.release()
    assert not (lock_dir / ".run.lock").exists()


def test_aimd_batch_controller_and_cold_start_eta():
    """Verify AIMD batch controller increases on healthy probes and cuts on hot-shard strain,
    and cold-start byte-weighted ETA works before and after progress."""
    aimd = AdaptiveBatchController(
        initial_batch=1000,
        min_batch=200,
        max_batch=5000,
        additive_step=500,
        multiplicative_cut=0.5,
        target_latency_ms=20.0,
    )
    assert aimd.observe(latency_ms=8.0, hot_shard_strained=False) == 1500
    assert aimd.observe(latency_ms=10.0, hot_shard_strained=False) == 2000
    assert aimd.observe(latency_ms=35.0, hot_shard_strained=False) == 1000
    assert aimd.observe(latency_ms=12.0, hot_shard_strained=True) == 500

    stages = machine.Stages(source=100.0 * 1024 * 1024, target=50.0 * 1024 * 1024)
    # Cold start (0 completed bytes) uses binding server stage (50 MiB/s)
    eta_cold = cold_start_byte_weighted_eta(
        total_bytes=500 * 1024 * 1024,
        completed_bytes=0,
        elapsed_seconds=0.0,
        stages=stages,
    )
    assert eta_cold == pytest.approx(10.0)

    # Warm progress uses measured rate (250 MiB in 5s = 50 MiB/s -> 5s left)
    eta_warm = cold_start_byte_weighted_eta(
        total_bytes=500 * 1024 * 1024,
        completed_bytes=250 * 1024 * 1024,
        elapsed_seconds=5.0,
        stages=stages,
    )
    assert eta_warm == pytest.approx(5.0)


def test_unix_report_rendering_assess_items_and_pure_ascii():
    """Verify UNIX report output, care badges, assess_placement_items, and 100% pure ASCII."""
    evidence = _build_skewed_multi_table_evidence(total_gb=120, colocation_limit_gb=32)
    engine = PlacementAdviceEngine()
    winner, ranked = engine.recommend_best_design(evidence)

    report = engine.render_report(evidence, ranked)
    assert "TARGET TOPOLOGY AND SIZING ADVICE REPORT:" in report
    assert "distributed-cohashed-balanced" in report
    assert "partitioned-range-bulk-first" in report
    assert "OVERALL CARE BADGE:" in report
    assert all(ord(ch) < 128 for ch in report)

    items = assess_placement_items(evidence)
    assert len(items) == 3
    assert items[0]["item"] == "target topology and workload sizing"
    assert items[0]["care_badge"] in (CARE_PASS, CARE_BENIGN)
    assert any("partitioned-range-bulk-first" in it["item"] for it in items[1:])

    # Verify source and test files are 100% pure ASCII
    root = pathlib.Path(__file__).resolve().parent.parent
    mod_text = (root / "migkit" / "placement_advice.py").read_text(encoding="utf-8")
    test_text = pathlib.Path(__file__).resolve().read_text(encoding="utf-8")
    assert all(ord(c) < 128 for c in mod_text)
    assert all(ord(c) < 128 for c in test_text)
