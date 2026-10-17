"""Comprehensive tests for consistent snapshot-to-stream handoff mechanisms (Row 619: RCH11).

Tests:
- Explicit alternatives registration: shared snapshot, range watermark, engine-native.
- Pricing snapshot lifetime, WAL retention, and source impact.
- Signaling writes enforcement & read-only source intent (refusing write signaling).
- Engine-native refusal when unsupported rather than faking an epoch.
- Missing log interval detection before accepting copy coverage (ACTION vs PASS/BENIGN).
- Universal typed durable resume in O(1) with native types (datetime, Decimal, UUID, tuple).
- Active lease mutual exclusion (.run.lock) with heartbeat.
- Pure UNIX style output with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS).
- 100% pure ASCII, no hardcoded secrets, safe test hosts (127.0.0.1, 10.0.0.x).
"""
import datetime
import decimal
import os
import tempfile
import time
import uuid
import pytest

from migkit.snapshot_watermark import (
    ActiveLeaseConflictError,
    ActiveLeaseLock,
    BaseHandoffStrategy,
    CareBadge,
    ContinuityReport,
    EngineNativeStrategy,
    HandoffPricing,
    HandoffState,
    HandoffStrategyRegistry,
    HandoffStrategyType,
    LogContinuityVerifier,
    LogInterval,
    MissingLogIntervalError,
    RangeWatermarkStrategy,
    SharedSnapshotStrategy,
    SignalingWriteRefusedError,
    SnapshotWatermarkCoordinator,
    SnapshotWatermarkError,
    SourceIntent,
    SourceProfile,
    TableProfile,
    WatermarkNotSupportedError,
    compare_positions,
    int_to_lsn,
    parse_position,
)


def test_strategy_alternatives_registration_and_listing():
    registry = HandoffStrategyRegistry()
    strategies = registry.list_strategies()
    strategy_types = {s.strategy_type for s in strategies}

    assert HandoffStrategyType.SHARED_SNAPSHOT.value in strategy_types
    assert HandoffStrategyType.RANGE_WATERMARK.value in strategy_types
    assert HandoffStrategyType.ENGINE_NATIVE.value in strategy_types

    shared = registry.get(HandoffStrategyType.SHARED_SNAPSHOT.value)
    assert isinstance(shared, SharedSnapshotStrategy)
    assert not shared.requires_signaling_writes

    range_wm = registry.get(HandoffStrategyType.RANGE_WATERMARK.value)
    assert isinstance(range_wm, RangeWatermarkStrategy)
    assert range_wm.requires_signaling_writes

    engine_nat = registry.get(HandoffStrategyType.ENGINE_NATIVE.value)
    assert isinstance(engine_nat, EngineNativeStrategy)
    assert not engine_nat.requires_signaling_writes

    # Custom strategy registration
    class CustomStrategy(BaseHandoffStrategy):
        strategy_type = "custom_cdc"
        name = "Custom CDC"

    registry.register(CustomStrategy())
    assert registry.get("custom_cdc").name == "Custom CDC"

    with pytest.raises(KeyError):
        registry.get("non_existent_strategy")


def test_price_snapshot_lifetime_wal_retention_and_source_impact():
    source = SourceProfile(
        host="127.0.0.1",
        engine="postgres",
        wal_rate_bytes_per_sec=2.0 * 1024 * 1024,  # 2 MB/s
        disk_headroom_bytes=10 * 1024 * 1024 * 1024,  # 10 GB
        max_snapshot_lifetime_seconds=7200.0,
    )
    table = TableProfile(
        table_name="orders",
        estimated_bytes=500 * 1024 * 1024,  # 500 MB
        chunk_count=5,
    )
    copy_rate = 10.0 * 1024 * 1024  # 10 MB/s

    # Shared snapshot pricing: lifetime = 50s, WAL = 100 MB
    shared = SharedSnapshotStrategy()
    pricing_shared = shared.price(source, table, copy_rate_bytes_per_sec=copy_rate)

    assert pricing_shared.is_viable
    assert pytest.approx(pricing_shared.snapshot_lifetime_seconds, 0.1) == 50.0
    assert pricing_shared.wal_retention_bytes == 100 * 1024 * 1024
    assert pricing_shared.care_badge == CareBadge.PASS.value
    assert not pricing_shared.requires_signaling_writes
    assert pricing_shared.source_impact_score >= 0.0

    # Range watermark pricing: transient chunk lifetime = 10s, WAL = 20 MB
    range_wm = RangeWatermarkStrategy()
    pricing_range = range_wm.price(source, table, copy_rate_bytes_per_sec=copy_rate)

    assert pricing_range.is_viable
    assert pytest.approx(pricing_range.snapshot_lifetime_seconds, 0.1) == 10.0
    assert pricing_range.wal_retention_bytes == 20 * 1024 * 1024
    assert pricing_range.requires_signaling_writes
    assert pricing_range.care_badge in (CareBadge.PASS.value, CareBadge.BENIGN.value)


def test_pricing_exceeding_wal_retention_produces_action_badge():
    # Source with very tight disk headroom (50 MB)
    tight_source = SourceProfile(
        host="10.0.0.5",
        wal_rate_bytes_per_sec=5.0 * 1024 * 1024,  # 5 MB/s
        disk_headroom_bytes=50 * 1024 * 1024,  # 50 MB
    )
    large_table = TableProfile(
        table_name="audit_events",
        estimated_bytes=1000 * 1024 * 1024,  # 1 GB -> 100s copy -> 500 MB WAL
    )
    shared = SharedSnapshotStrategy()
    pricing = shared.price(tight_source, large_table, copy_rate_bytes_per_sec=10.0 * 1024 * 1024)

    assert not pricing.is_viable
    assert pricing.care_badge == CareBadge.ACTION.value
    assert "exceeds source disk headroom" in (pricing.rejection_reason or "")


def test_pricing_exceeding_max_snapshot_lifetime_produces_action_badge():
    short_source = SourceProfile(
        host="127.0.0.1",
        max_snapshot_lifetime_seconds=30.0,
    )
    table = TableProfile(
        table_name="large_logs",
        estimated_bytes=500 * 1024 * 1024,  # 500 MB -> 50s copy > 30s limit
    )
    shared = SharedSnapshotStrategy()
    pricing = shared.price(short_source, table, copy_rate_bytes_per_sec=10.0 * 1024 * 1024)

    assert not pricing.is_viable
    assert pricing.care_badge == CareBadge.ACTION.value
    assert "exceeds max allowed lifetime" in (pricing.rejection_reason or "")


def test_pricing_moderate_wal_or_long_snapshot_produces_review_badge():
    source = SourceProfile(
        host="127.0.0.1",
        wal_rate_bytes_per_sec=1.0 * 1024 * 1024,
        disk_headroom_bytes=1000 * 1024 * 1024,  # 1 GB
        max_snapshot_lifetime_seconds=10000.0,
    )
    # 700 MB WAL retention on 1 GB disk headroom = 70% (> 60% threshold)
    table = TableProfile(
        table_name="archive_records",
        estimated_bytes=7000 * 1024 * 1024,
    )
    shared = SharedSnapshotStrategy()
    pricing = shared.price(source, table, copy_rate_bytes_per_sec=10.0 * 1024 * 1024)

    assert pricing.is_viable
    assert pricing.care_badge == CareBadge.REVIEW.value
    assert any("High WAL accumulation" in r for r in pricing.care_reasons)


def test_signaling_writes_refused_on_read_only_source():
    # Test read_only=True
    ro_source = SourceProfile(
        host="10.0.0.12",
        read_only=True,
        intent="read_only",
    )
    table = TableProfile(table_name="read_only_tbl")
    strategy = RangeWatermarkStrategy()

    with pytest.raises(SignalingWriteRefusedError) as exc_info:
        strategy.validate_capability(ro_source)
    assert "refused" in str(exc_info.value).lower()
    assert "read-only" in str(exc_info.value).lower()

    # Pricing evaluation on read-only source yields ACTION badge and not viable
    pricing = strategy.price(ro_source, table)
    assert not pricing.is_viable
    assert pricing.care_badge == CareBadge.ACTION.value
    assert "read-only" in (pricing.rejection_reason or "")

    # Coordinator prepare_handoff refuses write signaling
    coordinator = SnapshotWatermarkCoordinator()
    with pytest.raises(SignalingWriteRefusedError):
        coordinator.prepare_handoff("range_watermark", ro_source, table)


def test_signaling_writes_allowed_on_read_write_source():
    rw_source = SourceProfile(
        host="127.0.0.1",
        read_only=False,
        intent="read_write",
    )
    table = TableProfile(table_name="rw_tbl", chunk_count=10)
    strategy = RangeWatermarkStrategy()

    strategy.validate_capability(rw_source)
    pricing = strategy.price(rw_source, table)
    assert pricing.is_viable
    assert pricing.care_badge in (CareBadge.PASS.value, CareBadge.BENIGN.value)

    coordinator = SnapshotWatermarkCoordinator()
    state = coordinator.prepare_handoff("range_watermark", rw_source, table)
    assert state.status == "prepared"
    assert state.strategy == "range_watermark"


def test_engine_native_refusal_when_unsupported_rather_than_faking_epoch():
    unsupported_source = SourceProfile(
        host="10.0.0.20",
        engine="custom_engine",
        supports_engine_native=False,
    )
    strategy = EngineNativeStrategy()

    with pytest.raises(WatermarkNotSupportedError) as exc_info:
        strategy.validate_capability(unsupported_source)
    assert "refusing path rather than faking an epoch" in str(exc_info.value)

    pricing = strategy.price(unsupported_source, TableProfile("test_tbl"))
    assert not pricing.is_viable
    assert pricing.care_badge == CareBadge.ACTION.value
    assert "refusing path rather than faking an epoch" in (pricing.rejection_reason or "")


def test_detect_missing_log_intervals_initial_gap_fails_coverage():
    # Snapshot cut at 0/2000, but stream earliest interval starts at 0/2500
    intervals = [
        LogInterval(start_pos="0/2500", end_pos="0/3000", engine="postgres", record_count=50),
        LogInterval(start_pos="0/3000", end_pos="0/3500", engine="postgres", record_count=50),
    ]

    report = LogContinuityVerifier.verify_continuity(
        snapshot_cut="0/2000",
        log_intervals=intervals,
        engine="postgres",
        strict=False,
    )

    assert not report.is_continuous
    assert not report.accept_coverage
    assert report.care_badge == CareBadge.ACTION.value
    assert len(report.gaps) >= 1
    assert report.gaps[0] == ("0/2000", "0/2500")
    assert any("Missing log interval detected" in r for r in report.reasons)

    with pytest.raises(MissingLogIntervalError):
        LogContinuityVerifier.verify_continuity(
            snapshot_cut="0/2000",
            log_intervals=intervals,
            engine="postgres",
            strict=True,
        )


def test_detect_missing_log_intervals_mid_stream_gap_fails_coverage():
    # Stream has a gap between 0/2500 and 0/2800
    intervals = [
        LogInterval(start_pos="0/2000", end_pos="0/2500", engine="postgres", record_count=100),
        LogInterval(start_pos="0/2800", end_pos="0/3200", engine="postgres", record_count=80),
    ]

    report = LogContinuityVerifier.verify_continuity(
        snapshot_cut="0/2000",
        log_intervals=intervals,
        engine="postgres",
    )

    assert not report.is_continuous
    assert not report.accept_coverage
    assert report.care_badge == CareBadge.ACTION.value
    assert ("0/2500", "0/2800") in report.gaps


def test_continuous_log_intervals_accepted_coverage_pass_badge():
    intervals = [
        LogInterval(start_pos="0/2000", end_pos="0/2600", engine="postgres", record_count=120, bytes_size=5000),
        LogInterval(start_pos="0/2600", end_pos="0/3000", engine="postgres", record_count=80, bytes_size=3500),
        LogInterval(start_pos="0/3000", end_pos="0/3600", engine="postgres", record_count=150, bytes_size=6000),
    ]

    report = LogContinuityVerifier.verify_continuity(
        snapshot_cut="0/2000",
        log_intervals=intervals,
        engine="postgres",
        strict=True,
    )

    assert report.is_continuous
    assert report.accept_coverage
    assert report.care_badge == CareBadge.PASS.value
    assert len(report.gaps) == 0
    assert report.total_records == 350
    assert report.total_bytes == 14500


def test_overlapping_log_intervals_benign_coverage():
    # Overlapping intervals: 0/2000-0/2600 and 0/2500-0/3000 (overlap from 0/2500 to 0/2600)
    intervals = [
        LogInterval(start_pos="0/2000", end_pos="0/2600", engine="postgres", record_count=100),
        LogInterval(start_pos="0/2500", end_pos="0/3000", engine="postgres", record_count=90),
    ]

    report = LogContinuityVerifier.verify_continuity(
        snapshot_cut="0/2000",
        log_intervals=intervals,
        engine="postgres",
    )

    assert report.is_continuous
    assert report.accept_coverage
    assert report.care_badge == CareBadge.BENIGN.value
    assert len(report.gaps) == 0
    assert len(report.overlaps) == 1
    assert ("0/2500", "0/2600") in report.overlaps


def test_generic_numeric_interval_and_high_watermark_gap_detection():
    # Snapshot cut 100, high watermark 400
    intervals = [
        LogInterval(start_pos=100, end_pos=200, engine="generic"),
        LogInterval(start_pos=200, end_pos=300, engine="generic"),
    ]

    # Latest end is 300, which is below high watermark 400
    report = LogContinuityVerifier.verify_continuity(
        snapshot_cut=100,
        log_intervals=intervals,
        high_watermark=400,
        engine="generic",
    )

    assert not report.is_continuous
    assert not report.accept_coverage
    assert report.care_badge == CareBadge.ACTION.value
    assert (300, 400) in report.gaps

    # High watermark satisfied
    intervals_complete = [
        LogInterval(start_pos=100, end_pos=250, engine="generic"),
        LogInterval(start_pos=250, end_pos=450, engine="generic"),
    ]
    report_ok = LogContinuityVerifier.verify_continuity(
        snapshot_cut=100,
        log_intervals=intervals_complete,
        high_watermark=400,
        engine="generic",
    )
    assert report_ok.is_continuous
    assert report_ok.accept_coverage
    assert report_ok.care_badge == CareBadge.PASS.value


def test_universal_typed_durable_resume_in_o1():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_file = os.path.join(tmpdir, "handoff_state.json")

        now = datetime.datetime(2026, 10, 17, 12, 30, 45, tzinfo=datetime.timezone.utc)
        record_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        amount = decimal.Decimal("1234567.89")
        composite_key = (101, "shard-A", "tenant-99")

        state = HandoffState(
            handoff_id="handoff-test-01",
            strategy="range_watermark",
            source_host="127.0.0.1",
            table_name="ledger",
            snapshot_cut="0/1600",
            stream_position="0/2400",
            status="in_progress",
            metadata={
                "created_at": now,
                "account_uuid": record_id,
                "opening_balance": amount,
                "anchor": composite_key,
            },
        )

        # Record chunk completions in O(1)
        state.mark_chunk_completed(
            cursor_id="chunk_01",
            start_key=1,
            end_key=10000,
            watermark="0/1800",
            rows_written=10000,
        )
        state.mark_chunk_completed(
            cursor_id="chunk_02",
            start_key=10001,
            end_key=20000,
            watermark="0/2100",
            rows_written=10000,
        )

        state.save(state_file)
        assert os.path.exists(state_file)

        # Load back and verify O(1) status check and native type restoration
        restored = HandoffState.load(state_file)
        assert restored.handoff_id == "handoff-test-01"
        assert restored.strategy == "range_watermark"
        assert restored.is_chunk_completed("chunk_01")
        assert restored.is_chunk_completed("chunk_02")
        assert not restored.is_chunk_completed("chunk_03")

        meta = restored.metadata
        assert isinstance(meta["created_at"], datetime.datetime)
        assert meta["created_at"] == now
        assert isinstance(meta["account_uuid"], uuid.UUID)
        assert meta["account_uuid"] == record_id
        assert isinstance(meta["opening_balance"], decimal.Decimal)
        assert meta["opening_balance"] == amount
        assert isinstance(meta["anchor"], tuple)
        assert meta["anchor"] == composite_key


def test_active_lease_lock_mutual_exclusion_and_heartbeat():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Worker 1 acquires lease
        lock1 = ActiveLeaseLock(lock_dir=tmpdir, lease_term_seconds=10.0)
        lock1.acquire()
        assert lock1.acquired
        assert os.path.exists(lock1.lock_file)

        # Worker 2 attempts to acquire lease while worker 1 is active -> conflict
        lock2 = ActiveLeaseLock(lock_dir=tmpdir, lease_term_seconds=10.0)
        with pytest.raises(ActiveLeaseConflictError) as exc_info:
            lock2.acquire()
        assert "Active lease held by" in str(exc_info.value)

        # Worker 1 heartbeats
        lock1.heartbeat()

        # Worker 1 releases
        lock1.release()
        assert not lock1.acquired

        # Worker 2 can now acquire lease
        lock2.acquire()
        assert lock2.acquired
        lock2.release()


def test_coordinator_selects_best_strategy_under_source_intent_and_constraints():
    coordinator = SnapshotWatermarkCoordinator()
    table = TableProfile(table_name="customers", estimated_bytes=100 * 1024 * 1024)

    # 1. Read-only source: should reject RangeWatermark and pick SharedSnapshot or EngineNative
    ro_source = SourceProfile(
        host="127.0.0.1",
        read_only=True,
        intent="read_only",
        supports_shared_snapshot=True,
        supports_engine_native=True,
    )
    chosen_strat, pricing = coordinator.select_best_strategy(ro_source, table)
    assert chosen_strat.strategy_type in (
        HandoffStrategyType.ENGINE_NATIVE.value,
        HandoffStrategyType.SHARED_SNAPSHOT.value,
    )
    assert not chosen_strat.requires_signaling_writes
    assert pricing.is_viable

    # 2. Source with small disk headroom where SharedSnapshot WAL retention exceeds limit,
    # but source is read-write: coordinator picks RangeWatermark
    tight_source = SourceProfile(
        host="10.0.0.8",
        read_only=False,
        intent="read_write",
        wal_rate_bytes_per_sec=10.0 * 1024 * 1024,  # 10 MB/s
        disk_headroom_bytes=50 * 1024 * 1024,  # 50 MB
        supports_engine_native=False,
        supports_range_watermark=True,
    )
    large_table = TableProfile(
        table_name="huge_tbl",
        estimated_bytes=500 * 1024 * 1024,  # 500 MB -> 50s copy -> 500 MB WAL exceeds 50 MB headroom
        chunk_count=20,  # each chunk 2.5s -> 25 MB WAL fits under 50 MB headroom
    )
    chosen_strat2, pricing2 = coordinator.select_best_strategy(
        tight_source,
        large_table,
        copy_rate_bytes_per_sec=10.0 * 1024 * 1024,
    )
    assert chosen_strat2.strategy_type == HandoffStrategyType.RANGE_WATERMARK.value
    assert pricing2.is_viable
    assert pricing2.care_badge in (CareBadge.PASS.value, CareBadge.BENIGN.value)


def test_coordinator_raises_when_no_strategy_is_viable():
    coordinator = SnapshotWatermarkCoordinator()
    # Read-only source + no shared snapshot + no engine native support
    impossible_source = SourceProfile(
        host="10.0.0.9",
        read_only=True,
        intent="read_only",
        supports_shared_snapshot=False,
        supports_engine_native=False,
    )
    table = TableProfile(table_name="t1")

    with pytest.raises(SnapshotWatermarkError) as exc_info:
        coordinator.select_best_strategy(impossible_source, table)
    assert "No viable handoff strategy available" in str(exc_info.value)


def test_pure_unix_report_readability_and_care_badges():
    coordinator = SnapshotWatermarkCoordinator()
    source = SourceProfile(host="127.0.0.1", engine="postgres", intent="read_write")
    table = TableProfile(table_name="payments", estimated_bytes=50 * 1024 * 1024)

    strategy, pricing = coordinator.select_best_strategy(source, table)
    intervals = [
        LogInterval(start_pos="0/1000", end_pos="0/2000", engine="postgres", record_count=100),
    ]
    continuity = coordinator.verify_coverage(
        snapshot_cut="0/1000",
        log_intervals=intervals,
        engine="postgres",
    )

    report_text = coordinator.render_report(
        pricing=pricing,
        source=source,
        table=table,
        continuity=continuity,
    )

    # 1. 100% pure ASCII verification
    assert report_text.isascii(), "Report must be 100% pure ASCII without unicode glyphs or emojis"

    # 2. Care badge verification
    assert "SNAPSHOT-TO-STREAM HANDOFF REPORT: PASS" in report_text
    assert "CARE BADGE" in report_text
    assert "DIMENSION" in report_text
    assert "127.0.0.1" in report_text

    # Test report when gap is detected (ACTION badge)
    gap_intervals = [
        LogInterval(start_pos="0/1500", end_pos="0/2000", engine="postgres", record_count=50),
    ]
    continuity_gap = coordinator.verify_coverage(
        snapshot_cut="0/1000",
        log_intervals=gap_intervals,
        engine="postgres",
    )
    report_gap = coordinator.render_report(
        pricing=pricing,
        source=source,
        table=table,
        continuity=continuity_gap,
    )
    assert report_gap.isascii()
    assert "SNAPSHOT-TO-STREAM HANDOFF REPORT: ACTION" in report_gap
    assert "Log Continuity" in report_gap
    assert "GAP DETECTED" in report_gap
    assert "Copy Coverage" in report_gap
    assert "REFUSED" in report_gap


def test_position_utilities():
    assert int_to_lsn(5632) == "0/1600"
    assert parse_position("0/1600") == 5632
    assert parse_position("12345") == 12345
    assert parse_position(99) == 99

    assert compare_positions("0/1600", "0/2000", engine="postgres") == -1
    assert compare_positions("0/2000", "0/2000", engine="postgres") == 0
    assert compare_positions("0/2500", "0/2000", engine="postgres") == 1

    assert compare_positions(100, 200, engine="generic") == -1
    assert compare_positions(200, 200, engine="generic") == 0
    assert compare_positions(300, 200, engine="generic") == 1
