import io
import time
import pytest

from migkit.pipeline_ha import (
    AdaptiveCompressionSelector,
    AdaptiveMoverThrottler,
    AtomicLeaseStore,
    CompressionChoice,
    DatabaseNode,
    DatabaseStressMetrics,
    EstateSizingEngine,
    FencingViolation,
    HistoricalWorkload,
    MoverHACoordinator,
    ParallelMultipartTransferCoordinator,
    PartRange,
    PipelinedPhaseCoordinator,
    PipelinePhase,
    compute_crc32c,
)


# --- Item 281 (LT.P1): Adaptive Auto-Scaling Mover Throttling ---

def test_throttler_scales_up_under_healthy_load():
    throttler = AdaptiveMoverThrottler(
        min_workers=1,
        max_workers=8,
        initial_workers=2,
        target_cpu=65.0,
        critical_cpu=85.0,
        step_up=2,
        cooldown_sec=0.0,
    )
    metrics = DatabaseStressMetrics(cpu_pct=35.0, io_wait_pct=10.0)
    decision = throttler.evaluate(metrics, now=100.0)
    assert decision.stress_level == "HEALTHY"
    assert decision.workers == 4
    assert throttler.get_sleep_delay() == 0.0

    decision2 = throttler.evaluate(metrics, now=101.0)
    assert decision2.workers == 6
    decision3 = throttler.evaluate(metrics, now=102.0)
    assert decision3.workers == 8
    # Should not exceed max_workers
    decision4 = throttler.evaluate(metrics, now=103.0)
    assert decision4.workers == 8


def test_throttler_emergency_throttle_under_critical_stress():
    throttler = AdaptiveMoverThrottler(
        min_workers=1,
        max_workers=16,
        initial_workers=12,
        target_cpu=70.0,
        critical_cpu=85.0,
        critical_io_wait=40.0,
        backoff_factor=0.5,
    )
    # Critical CPU triggers emergency drop
    critical_metrics = DatabaseStressMetrics(cpu_pct=92.0, io_wait_pct=15.0)
    decision = throttler.evaluate(critical_metrics, now=100.0)
    assert decision.stress_level == "CRITICAL"
    assert decision.workers == 6  # 12 * 0.5 = 6
    assert decision.sleep_delay > 0.0
    assert throttler.should_throttle() is True

    # Further critical IO wait cuts workers down to min_workers
    critical_io = DatabaseStressMetrics(cpu_pct=88.0, io_wait_pct=55.0, io_latency_ms=120.0)
    decision2 = throttler.evaluate(critical_io, now=101.0)
    assert decision2.workers == 3
    decision3 = throttler.evaluate(critical_io, now=102.0)
    assert decision3.workers == 1  # min workers floor


def test_throttler_caution_under_warning_load():
    throttler = AdaptiveMoverThrottler(
        min_workers=1,
        max_workers=8,
        initial_workers=6,
        target_cpu=65.0,
        critical_cpu=85.0,
        cooldown_sec=0.0,
    )
    warn_metrics = DatabaseStressMetrics(cpu_pct=72.0, io_wait_pct=10.0)
    decision = throttler.evaluate(warn_metrics, now=100.0)
    assert decision.stress_level == "WARNING"
    assert decision.workers == 5
    assert decision.sleep_delay > 0.0


# --- Item 284 (W1): Fast Standby Takeover (< 5s) via Atomic Heartbeat Lease Election ---

def test_fast_standby_takeover_under_five_seconds():
    store = AtomicLeaseStore()
    node_leader = MoverHACoordinator("leader_node", store, lease_term_seconds=1.5, heartbeat_interval=0.3)
    node_standby = MoverHACoordinator("standby_node", store, lease_term_seconds=1.5, heartbeat_interval=0.3)

    # Initial election: leader acquires lease
    role_lead = node_leader.tick(now=10.0)
    assert role_lead == "PROMOTED"
    assert node_leader.is_leader() is True
    assert node_leader.current_epoch() == 1

    # Standby checks, sees leader holding valid lease
    role_sb = node_standby.tick(now=10.2)
    assert role_sb == "STANDBY"
    assert node_standby.is_leader() is False

    # Leader renews at 10.5
    role_lead = node_leader.tick(now=10.5)
    assert role_lead == "LEADER"

    # Leader fails / crashes (no heartbeats after 10.5). Lease expires at 10.5 + 1.5 = 12.0
    # Standby ticks at 12.1 (1.6s after leader stopped heartbeating, well under 5s limit)
    role_sb_takeover = node_standby.tick(now=12.1)
    assert role_sb_takeover == "PROMOTED"
    assert node_standby.is_leader() is True
    assert node_standby.current_epoch() == 2
    assert node_standby.takeover_duration is not None
    assert node_standby.takeover_duration < 5.0

    # Old leader wakes up later at 12.5 and ticks -> gets demoted
    role_old_lead = node_leader.tick(now=12.5)
    assert role_old_lead == "DEMOTED"
    assert node_leader.is_leader() is False


def test_mover_ha_fencing_protection():
    store = AtomicLeaseStore()
    node1 = MoverHACoordinator("node1", store, lease_term_seconds=2.0)
    node2 = MoverHACoordinator("node2", store, lease_term_seconds=2.0)

    # node1 acquires epoch 1
    node1.tick(now=1.0)
    assert node1.current_epoch() == 1
    # Valid write with current epoch succeeds
    node1.verify_write_fenced(1)

    # Stale epoch rejected
    with pytest.raises(FencingViolation):
        node1.verify_write_fenced(0)

    # Lease expires, node2 takes over with epoch 2
    node2.tick(now=4.0)
    assert node2.current_epoch() == 2
    node2.verify_write_fenced(2)

    # Node1 write with old epoch 1 is strictly rejected
    with pytest.raises(FencingViolation):
        node1.verify_write_fenced(1, now=4.0)


# --- Item 285 (W2): Estate Discovery and Sizing Engine ---

def test_estate_discovery_and_sizing_engine():
    engine = EstateSizingEngine()
    node_pg = DatabaseNode(
        node_id="pg_prod_primary",
        engine="postgres",
        version="16.2",
        cores=8,
        ram_gb=32.0,
        disk_gb=500.0,
        table_count=45,
        total_bytes=350 * 1024**3,
    )
    workload_pg = HistoricalWorkload(
        node_id="pg_prod_primary",
        peak_qps=4500.0,
        avg_qps=2100.0,
        p95_cpu_pct=72.0,
        p95_iops=8500.0,
        p95_read_iops=6000.0,
        p95_write_iops=2500.0,
        monthly_growth_pct=6.0,
    )

    engine.discover_node(node_pg, workload_pg)
    recommendation = engine.size_node("pg_prod_primary", horizon_months=12, headroom_factor=1.3)

    assert recommendation.node_id == "pg_prod_primary"
    assert recommendation.target_cores >= 8
    assert recommendation.target_ram_gb >= 32.0
    assert recommendation.target_disk_gb > 500.0
    assert recommendation.target_iops >= int(8500 * 1.3)
    assert recommendation.recommended_concurrency >= 8
    assert recommendation.sizing_tier in ("large", "xlarge", "2xlarge", "4xlarge")
    assert recommendation.buffer_pool_gb > 10.0


def test_estate_fleet_summary():
    engine = EstateSizingEngine()
    nodes = [
        DatabaseNode("db1", "postgres", "15", 4, 16.0, 100.0, 10, 50 * 1024**3),
        DatabaseNode("db2", "mysql", "8.0", 8, 32.0, 300.0, 30, 200 * 1024**3),
    ]
    workloads = [
        HistoricalWorkload("db1", 500.0, 200.0, 45.0, 1200.0, 1000.0, 200.0),
        HistoricalWorkload("db2", 2000.0, 1100.0, 68.0, 4500.0, 3000.0, 1500.0),
    ]
    engine.discover_fleet(nodes, workloads)
    summary = engine.fleet_summary()

    assert summary["node_count"] == 2
    assert summary["total_target_cores"] >= 12
    assert summary["total_target_ram_gb"] >= 48.0
    assert len(summary["nodes"]) == 2


# --- Item 385 (R20.5): Pipelined Phase Overlapping Coordinator ---

def test_pipelined_phase_overlapping_coordinator_speedup():
    coordinator = PipelinedPhaseCoordinator(
        max_concurrent_copies=2,
        max_concurrent_indexes=2,
        max_concurrent_verifications=2,
    )
    coordinator.add_table("orders", ranges=["orders_r1", "orders_r2"], copy_duration=1.0, index_duration=1.0, verify_duration=0.5)
    coordinator.add_table("users", ranges=["users_r1"], copy_duration=1.0, index_duration=1.0, verify_duration=0.5)
    coordinator.add_table("logs", ranges=["logs_r1"], copy_duration=1.0, index_duration=1.0, verify_duration=0.5)

    sim = coordinator.simulate()
    assert sim["pipelined_duration"] < sim["sequential_duration"]
    assert sim["speedup"] > 1.3
    assert sim["tail_started"] is True
    assert coordinator.is_finished() is True


def test_pipelined_phase_scheduling_order():
    coordinator = PipelinedPhaseCoordinator()
    coordinator.add_table("t1", ranges=["r1"])
    coordinator.add_table("t2", ranges=["r2"])

    # First step schedules copy for available tables
    actions = coordinator.schedule_next_actions()
    assert ("t1", PipelinePhase.COPY_DATA.value, "all") in actions
    assert ("t2", PipelinePhase.COPY_DATA.value, "all") in actions

    # Complete copy for t1: should trigger index build and range verification for t1
    coordinator.complete_action("t1", PipelinePhase.COPY_DATA.value, "all")
    next_actions = coordinator.schedule_next_actions()
    assert ("t1", PipelinePhase.BUILD_INDEXES.value, "all") in next_actions
    assert ("t1", PipelinePhase.VERIFY_RANGE.value, "r1") in next_actions


# --- Item 386 (R20.6): Measured Adaptive Compression Selector ---

def test_adaptive_compression_selection_and_roundtrip():
    selector = AdaptiveCompressionSelector()

    # High link speed chooses fast compression (lz4)
    choice_fast = selector.select(link_speed_mb_s=250.0, host_cpu_pct=30.0)
    assert choice_fast.algorithm == "lz4"

    # Medium link speed chooses balanced compression (zstd)
    choice_medium = selector.select(link_speed_mb_s=80.0, host_cpu_pct=40.0)
    assert choice_medium.algorithm == "zstd"
    assert choice_medium.level <= 3

    # Low link speed chooses high compression
    choice_slow = selector.select(link_speed_mb_s=15.0, host_cpu_pct=40.0)
    assert choice_slow.algorithm in ("zstd", "gzip")
    assert choice_slow.level >= 6

    # Saturated CPU triggers low CPU algorithm regardless of bandwidth
    choice_high_cpu = selector.select(link_speed_mb_s=20.0, host_cpu_pct=92.0)
    assert choice_high_cpu.algorithm in ("lz4", "gzip")

    # Roundtrip compression test for each
    sample_data = b"migkit-test-payload-data-stream-repeat-" * 500
    for choice in [choice_fast, choice_medium, choice_slow, choice_high_cpu]:
        compressed = selector.compress(sample_data, choice)
        assert len(compressed) < len(sample_data)
        decompressed = selector.decompress(compressed, choice.algorithm)
        assert decompressed == sample_data


# --- Item 387 (R20.7): Parallel Ranged Multipart File Transfer ---

def test_crc32c_computation():
    data = b"123456789"
    crc = compute_crc32c(data)
    assert crc == 0xe3069283


def test_multipart_ranged_planning():
    coord = ParallelMultipartTransferCoordinator(part_size=1024 * 1024, max_workers=4)
    parts = coord.plan_parts(3500 * 1024)
    assert len(parts) == 4
    assert parts[0].start_byte == 0 and parts[0].end_byte == 1024 * 1024
    assert parts[-1].end_byte == 3500 * 1024


def test_parallel_multipart_transfer_with_streaming_checksums():
    coord = ParallelMultipartTransferCoordinator(part_size=128 * 1024, max_workers=4)
    # Generate exact 512KB sample test data
    test_data = (b"MIGKIT-MULTIPART-TEST-BLOCK-" * 20000)[: 512 * 1024]
    assert len(test_data) == 512 * 1024
    result, assembled = coord.transfer_bytes(test_data, verify=True)

    assert result.verified is True
    assert result.total_bytes == len(test_data)
    assert result.part_count == 4
    assert len(result.part_checksums) == 4
    assert assembled == test_data

    for chk in result.part_checksums:
        assert chk.bytes_read == 128 * 1024
        assert len(chk.crc32c_hex) == 8
        assert len(chk.md5_hex) == 32


def test_multipart_file_stream_transfer(tmp_path):
    coord = ParallelMultipartTransferCoordinator(part_size=64 * 1024, max_workers=3)
    source_file = tmp_path / "source.bin"
    target_file = tmp_path / "target.bin"

    data = (b"abcdefghijklmnop" * 16384)[: 200 * 1024]
    source_file.write_bytes(data)

    with open(source_file, "rb") as src, open(target_file, "w+b") as dst:
        result = coord.transfer_file_streams(src, dst, total_bytes=len(data), verify=True)

    assert result.verified is True
    assert result.total_bytes == len(data)
    assert target_file.read_bytes() == data
