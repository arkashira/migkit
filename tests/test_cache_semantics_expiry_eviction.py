import decimal
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from migkit.cache_semantics import (
    AccessMetadata,
    ActiveActiveCRDTResolver,
    CacheCheckpointStore,
    CacheDataType,
    CacheEndpoint,
    CacheKeyRecord,
    CacheMigrationCoordinator,
    CacheSemanticsError,
    CareBadge,
    ClientConnection,
    ClientRouteTarget,
    EvictionFidelityEvaluator,
    EvictionPolicy,
    LateFullResyncForbiddenError,
    LeaseLock,
    LeaseLockError,
    MemoryPressureMonitor,
    ReverseSyncFlushForbiddenError,
    RoutingSyncCouplingError,
    StaleClientEpochError,
    SyncMode,
    TTLCompensator,
    TTLPolicy,
    UnsafeTestHostError,
    is_safe_test_host,
    validate_test_host,
)


def test_safe_test_host_enforcement():
    assert is_safe_test_host("127.0.0.1") is True
    assert is_safe_test_host("localhost") is True
    assert is_safe_test_host("10.0.0.15") is True
    assert is_safe_test_host("192.168.1.10") is False
    assert is_safe_test_host("redis.prod.internal") is False

    with pytest.raises(UnsafeTestHostError):
        validate_test_host("prod-cache.example.com")

    with pytest.raises(UnsafeTestHostError):
        CacheEndpoint(name="bad", host="172.16.0.5", port=6379)


def test_lease_lock_mutual_exclusion_and_heartbeat(tmp_path: Path):
    lock1 = LeaseLock(tmp_path, owner_id="worker-1")
    lock2 = LeaseLock(tmp_path, owner_id="worker-2")

    with lock1:
        lock1.heartbeat()
        assert lock1.last_heartbeat is not None
        with pytest.raises(LeaseLockError):
            lock2.acquire()

    with lock2:
        assert (tmp_path / ".run.lock").exists()


def test_durable_checkpoint_o1_resume_preserves_native_types(tmp_path: Path):
    ckpt_file = tmp_path / "cache_ckpt.json"
    store = CacheCheckpointStore(ckpt_file)

    now_dt = datetime(2026, 10, 17, 10, 30, 0, tzinfo=timezone.utc)
    uid = uuid.uuid4()
    dec = decimal.Decimal("99.995")
    composite_cursor = ("slot-range-0-5460", now_dt, uid, dec)

    store.mark_completed("shard-0:batch-1", composite_cursor, keys_processed=250)
    assert store.is_completed("shard-0:batch-1") is True
    assert store.is_completed("shard-0:batch-2") is False

    reloaded = CacheCheckpointStore(ckpt_file)
    assert reloaded.is_completed("shard-0:batch-1") is True
    restored_cursor = reloaded.get_cursor_boundary("shard-0:batch-1")
    assert restored_cursor == composite_cursor
    assert isinstance(restored_cursor[1], datetime)
    assert isinstance(restored_cursor[2], uuid.UUID)
    assert isinstance(restored_cursor[3], decimal.Decimal)


def test_ttl_elapsed_time_compensation_and_near_expiry_handling():
    compensator = TTLCompensator(
        preferred_policy=TTLPolicy.ABSOLUTE_TIMESTAMP,
        near_expiry_window_ms=500,
        max_uncompensated_skew_ms=50,
        min_apply_grace_ms=10,
    )

    normal_key = CacheKeyRecord(
        key="session:100",
        data_type=CacheDataType.STRING,
        value="tok-100",
        ttl_ms=10000,
        expire_at_ms=1_010_000,
        read_mono_ms=1000,
        read_wall_ms=1_000_000,
    )
    res_normal = compensator.compute_target_expiry(
        record=normal_key,
        apply_mono_ms=1250,
        target_wall_ms=1_000_250,
        source_wall_at_apply_ms=1_000_250,
        target_supports_absttl=True,
    )
    assert res_normal.should_apply is True
    assert res_normal.expired_in_flight is False
    assert res_normal.near_expiry is False
    assert res_normal.elapsed_ms == 250
    assert res_normal.target_ttl_ms == 9750
    assert res_normal.target_expire_at_ms == 1_010_000
    assert res_normal.care_badge == CareBadge.PASS

    expired_in_transit = CacheKeyRecord(
        key="otp:200",
        data_type=CacheDataType.STRING,
        value="9912",
        ttl_ms=180,
        expire_at_ms=1_000_180,
        read_mono_ms=1000,
        read_wall_ms=1_000_000,
    )
    res_expired = compensator.compute_target_expiry(
        record=expired_in_transit,
        apply_mono_ms=1200,
        target_wall_ms=1_000_200,
        source_wall_at_apply_ms=1_000_200,
    )
    assert res_expired.should_apply is False
    assert res_expired.expired_in_flight is True
    assert res_expired.care_badge == CareBadge.BENIGN

    near_expiry_key = CacheKeyRecord(
        key="rate:300",
        data_type=CacheDataType.COUNTER,
        value=5,
        ttl_ms=400,
        expire_at_ms=1_000_400,
        read_mono_ms=1000,
        read_wall_ms=1_000_000,
    )
    res_near = compensator.compute_target_expiry(
        record=near_expiry_key,
        apply_mono_ms=1100,
        target_wall_ms=1_000_100,
        source_wall_at_apply_ms=1_000_100,
    )
    assert res_near.should_apply is True
    assert res_near.near_expiry is True
    assert res_near.target_ttl_ms == 300
    assert res_near.care_badge == CareBadge.BENIGN


def test_clock_skew_compensation_and_uncompensatable_skew_detection():
    compensator = TTLCompensator(
        preferred_policy=TTLPolicy.ABSOLUTE_TIMESTAMP,
        max_uncompensated_skew_ms=100,
        max_compensatable_skew_ms=10000,
    )
    rec = CacheKeyRecord(
        key="token:skew",
        data_type=CacheDataType.STRING,
        value="abc",
        ttl_ms=5000,
        expire_at_ms=2_005_000,
        read_mono_ms=100,
        read_wall_ms=2_000_000,
    )

    res_compensated = compensator.compute_target_expiry(
        record=rec,
        apply_mono_ms=200,
        target_wall_ms=2_002_600,
        source_wall_at_apply_ms=2_000_100,
        target_supports_absttl=True,
    )
    assert res_compensated.clock_skew_ms == 2500
    assert res_compensated.skew_compensated is True
    assert res_compensated.fidelity_preserved is True
    assert res_compensated.target_expire_at_ms == 2_005_000 + 2500
    assert res_compensated.target_ttl_ms == 4900

    res_extreme = compensator.compute_target_expiry(
        record=rec,
        apply_mono_ms=200,
        target_wall_ms=2_050_100,
        source_wall_at_apply_ms=2_000_100,
        target_supports_absttl=True,
    )
    assert res_extreme.fidelity_preserved is False
    assert res_extreme.policy_used == TTLPolicy.RELATIVE_COMPENSATED
    assert res_extreme.care_badge == CareBadge.ACTION


def test_eviction_lfu_lru_metadata_fidelity_and_unavailable_declaration():
    fid_ok = EvictionFidelityEvaluator.evaluate(
        source_policy=EvictionPolicy.ALLKEYS_LFU,
        target_policy=EvictionPolicy.ALLKEYS_LFU,
        supports_restore_idletime=True,
        supports_restore_freq=True,
    )
    assert fid_ok.preserved is True
    assert "lfu_freq" in fid_ok.carried_fields
    assert fid_ok.care_badge == CareBadge.PASS

    fid_no_freq = EvictionFidelityEvaluator.evaluate(
        source_policy=EvictionPolicy.ALLKEYS_LFU,
        target_policy=EvictionPolicy.ALLKEYS_LFU,
        supports_restore_idletime=True,
        supports_restore_freq=False,
        allow_declared_degradation=True,
    )
    assert fid_no_freq.preserved is False
    assert "lfu_freq" in fid_no_freq.unavailable_fields
    assert fid_no_freq.care_badge == CareBadge.REVIEW

    fid_unsafe_policy = EvictionFidelityEvaluator.evaluate(
        source_policy=EvictionPolicy.VOLATILE_LRU,
        target_policy=EvictionPolicy.ALLKEYS_LRU,
    )
    assert fid_unsafe_policy.preserved is False
    assert "persistent_key_immunity" in fid_unsafe_policy.unavailable_fields
    assert fid_unsafe_policy.care_badge == CareBadge.ACTION

    comp_meta = EvictionFidelityEvaluator.compensate_access_metadata(
        AccessMetadata(idletime_sec=40, freq_counter=180),
        elapsed_ms=3500,
        supports_idletime=True,
        supports_freq=True,
    )
    assert comp_meta.idletime_sec == 43
    assert comp_meta.freq_counter == 180


def test_memory_pressure_evaluation_preserves_approved_semantics():
    monitor = MemoryPressureMonitor(
        warning_ratio=0.80,
        critical_ratio=0.90,
        approve_volatile_eviction_under_pressure=True,
    )

    nominal = monitor.evaluate(
        used_memory_bytes=50,
        maxmemory_bytes=100,
        evicted_keys_delta=0,
        persistent_keys_evicted=0,
        target_policy=EvictionPolicy.VOLATILE_LRU,
    )
    assert nominal.under_pressure is False
    assert nominal.semantics_approved is True
    assert nominal.care_badge == CareBadge.PASS

    approved_evict = monitor.evaluate(
        used_memory_bytes=88,
        maxmemory_bytes=100,
        evicted_keys_delta=12,
        persistent_keys_evicted=0,
        target_policy=EvictionPolicy.VOLATILE_LRU,
    )
    assert approved_evict.under_pressure is True
    assert approved_evict.semantics_approved is True
    assert approved_evict.care_badge == CareBadge.REVIEW

    persistent_evict = monitor.evaluate(
        used_memory_bytes=95,
        maxmemory_bytes=100,
        evicted_keys_delta=5,
        persistent_keys_evicted=2,
        target_policy=EvictionPolicy.ALLKEYS_LRU,
    )
    assert persistent_evict.semantics_approved is False
    assert persistent_evict.care_badge == CareBadge.ACTION

    oom_danger = monitor.evaluate(
        used_memory_bytes=95,
        maxmemory_bytes=100,
        evicted_keys_delta=0,
        persistent_keys_evicted=0,
        target_policy=EvictionPolicy.NOEVICTION,
    )
    assert oom_danger.semantics_approved is False
    assert oom_danger.care_badge == CareBadge.ACTION


def test_active_active_crdt_conflict_resolution_across_data_types():
    c_local = CacheKeyRecord(
        key="hits:daily",
        data_type=CacheDataType.COUNTER,
        value=130,
        version_vector={"site-a": 2, "site-b": 1},
        updated_at_ms=5000,
        site_id="site-a",
        expire_at_ms=10000,
    )
    c_remote = CacheKeyRecord(
        key="hits:daily",
        data_type=CacheDataType.COUNTER,
        value=145,
        version_vector={"site-a": 1, "site-b": 2},
        updated_at_ms=5050,
        site_id="site-b",
        expire_at_ms=12000,
    )
    out_counter = ActiveActiveCRDTResolver.resolve(c_local, c_remote, base_counter_value=100)
    assert out_counter.conflict_detected is True
    assert out_counter.merged_record.value == 175
    assert out_counter.merged_record.expire_at_ms == 12000
    assert out_counter.care_badge == CareBadge.PASS

    s_local = CacheKeyRecord(
        key="cart:items",
        data_type=CacheDataType.SET,
        value={"sku-1"},
        observed_adds={"sku-1": {"tag-1"}},
        version_vector={"site-a": 2, "site-b": 1},
        updated_at_ms=6000,
        site_id="site-a",
    )
    s_remote = CacheKeyRecord(
        key="cart:items",
        data_type=CacheDataType.SET,
        value={"sku-1", "sku-2"},
        observed_adds={"sku-1": {"tag-1"}, "sku-2": {"tag-2-concurrent-add"}},
        version_vector={"site-a": 1, "site-b": 2},
        updated_at_ms=6010,
        site_id="site-b",
    )
    out_set = ActiveActiveCRDTResolver.resolve(s_local, s_remote)
    assert "sku-2" in out_set.merged_record.value
    assert out_set.resolution_rule == "crdt_orset_add_wins"

    h_local = CacheKeyRecord(
        key="user:profile",
        data_type=CacheDataType.HASH,
        value={"email": "a@example.com", "tier": "gold"},
        field_timestamps={"email": (1000, "site-a"), "tier": (2000, "site-a")},
        version_vector={"site-a": 2, "site-b": 1},
        updated_at_ms=2000,
        site_id="site-a",
    )
    h_remote = CacheKeyRecord(
        key="user:profile",
        data_type=CacheDataType.HASH,
        value={"email": "b@example.com", "locale": "en"},
        field_timestamps={"email": (2500, "site-b"), "locale": (2500, "site-b")},
        version_vector={"site-a": 1, "site-b": 2},
        updated_at_ms=2500,
        site_id="site-b",
    )
    out_hash = ActiveActiveCRDTResolver.resolve(h_local, h_remote)
    assert out_hash.merged_record.value == {
        "email": "b@example.com",
        "tier": "gold",
        "locale": "en",
    }

    t_local = CacheKeyRecord(
        key="mixed:key",
        data_type=CacheDataType.STRING,
        value="plain",
        version_vector={"site-a": 1},
        updated_at_ms=1000,
        site_id="site-a",
    )
    t_remote = CacheKeyRecord(
        key="mixed:key",
        data_type=CacheDataType.LIST,
        value=["item1"],
        version_vector={"site-b": 1},
        updated_at_ms=1100,
        site_id="site-b",
    )
    out_type = ActiveActiveCRDTResolver.resolve(t_local, t_remote)
    assert out_type.care_badge == CareBadge.REVIEW
    assert out_type.merged_record.data_type == CacheDataType.LIST


def test_separation_of_data_sync_from_client_routing_and_reconnect_after_cutover(tmp_path: Path):
    src = CacheEndpoint(
        name="redis-src",
        host="127.0.0.1",
        port=6379,
        eviction_policy=EvictionPolicy.ALLKEYS_LRU,
    )
    tgt = CacheEndpoint(
        name="redis-tgt",
        host="10.0.0.12",
        port=6379,
        eviction_policy=EvictionPolicy.ALLKEYS_LRU,
    )
    src.write_key(
        CacheKeyRecord(
            key="k:persist",
            data_type=CacheDataType.STRING,
            value="v1",
            access=AccessMetadata(idletime_sec=15, freq_counter=20),
        )
    )
    src.write_key(
        CacheKeyRecord(
            key="k:expiring",
            data_type=CacheDataType.HASH,
            value={"f": "1"},
            ttl_ms=5000,
            expire_at_ms=105000,
            access=AccessMetadata(idletime_sec=5, freq_counter=10),
        )
    )
    src.write_key(
        CacheKeyRecord(
            key="k:transit_expired",
            data_type=CacheDataType.STRING,
            value="gone",
            ttl_ms=100,
            expire_at_ms=100100,
        )
    )

    ckpt = CacheCheckpointStore(tmp_path / "ckpt.json")
    coord = CacheMigrationCoordinator(source=src, target=tgt, checkpoint_store=ckpt)

    client_auto = ClientConnection(client_id="c-auto", client_host="10.0.0.50", auto_redirect_enabled=True)
    client_pinned = ClientConnection(client_id="c-pinned", client_host="10.0.0.51", auto_redirect_enabled=False)
    coord.routing_plane.register_client(client_auto)
    coord.routing_plane.register_client(client_pinned)

    coord.sync_plane.start_initial_sync()
    coord.sync_plane.sync_batch(
        unit_id="slot:0-16383",
        cursor_boundary=("0", "16383"),
        keys_to_sync=["k:persist", "k:expiring", "k:transit_expired"],
        read_mono_ms=1000,
        apply_mono_ms=1200,
        base_wall_ms=100000,
    )

    assert coord.routing_plane.route_target == ClientRouteTarget.SOURCE
    assert tgt.promoted is False
    assert "k:transit_expired" not in tgt.keys
    assert "k:persist" in tgt.keys
    assert "k:expiring" in tgt.keys
    assert tgt.keys["k:expiring"].ttl_ms == 4800

    with pytest.raises(RoutingSyncCouplingError):
        coord.routing_plane.switch_routing_to_target(coord.sync_plane, new_epoch=1)

    badge = coord.execute_cutover(new_epoch=1)
    assert badge == CareBadge.PASS
    assert coord.routing_plane.route_target == ClientRouteTarget.TARGET
    assert tgt.promoted is True
    assert tgt.writable is True
    assert src.writable is False

    with pytest.raises(StaleClientEpochError):
        coord.routing_plane.handle_client_command("c-pinned", is_write=True, record=CacheKeyRecord(
            key="k:new", data_type=CacheDataType.STRING, value="fail"
        ))

    write_res = coord.routing_plane.handle_client_command(
        "c-auto",
        is_write=True,
        record=CacheKeyRecord(key="k:post_cutover", data_type=CacheDataType.STRING, value="live-on-target"),
    )
    assert write_res["endpoint"] == "redis-tgt"
    assert tgt.post_promotion_writes == 1
    assert "k:post_cutover" in tgt.keys
    assert "k:post_cutover" not in src.keys

    report = coord.verify_semantics(verify_wall_ms=100200)
    assert report.overall_badge == CareBadge.BENIGN
    assert report.expired_in_flight_keys == ["k:transit_expired"]
    assert report.resurrected_keys == []
    assert report.missing_keys == []
    assert report.routing_separated_from_sync is True
    text = report.format_unix_report()
    assert "[BENIGN]" in text
    assert all(ord(ch) < 128 for ch in text)


def test_forbid_late_full_resync_and_protect_new_writes_on_reverse_replication():
    src = CacheEndpoint(name="src", host="127.0.0.1", port=6379)
    tgt = CacheEndpoint(name="tgt", host="127.0.0.1", port=6380)
    src.write_key(CacheKeyRecord(key="k:base", data_type=CacheDataType.STRING, value="base"))

    coord = CacheMigrationCoordinator(source=src, target=tgt)
    coord.sync_plane.start_initial_sync()
    coord.sync_plane.sync_batch(
        unit_id="u1",
        cursor_boundary=0,
        keys_to_sync=["k:base"],
        read_mono_ms=10,
        apply_mono_ms=20,
        base_wall_ms=50000,
    )
    coord.execute_cutover(new_epoch=2)

    tgt.write_key(
        CacheKeyRecord(key="k:new_after_cutover", data_type=CacheDataType.STRING, value="must-not-lose"),
        is_client_write=True,
    )
    assert tgt.post_promotion_writes == 1

    with pytest.raises(LateFullResyncForbiddenError):
        coord.sync_plane.attempt_full_resync_from_source()

    with pytest.raises(LateFullResyncForbiddenError):
        coord.sync_plane.start_initial_sync()

    with pytest.raises(ReverseSyncFlushForbiddenError):
        coord.sync_plane.configure_reverse_replication(
            from_endpoint=src,
            to_endpoint=tgt,
            flush_destination=True,
        )

    assert "k:new_after_cutover" in tgt.keys

    rev_badge = coord.sync_plane.configure_reverse_replication(
        from_endpoint=tgt,
        to_endpoint=src,
        flush_destination=False,
    )
    assert rev_badge == CareBadge.PASS
    assert coord.sync_plane.sync_mode == SyncMode.REVERSE_REPLICATION
    assert coord.sync_plane.replication_direction == "target->source"
    assert "k:new_after_cutover" in src.keys
    assert src.keys["k:new_after_cutover"].value == "must-not-lose"


def test_resurrected_key_and_ttl_drift_flagged_as_action():
    src = CacheEndpoint(name="src", host="127.0.0.1", port=6379)
    tgt = CacheEndpoint(name="tgt", host="127.0.0.1", port=6380)

    src.write_key(
        CacheKeyRecord(
            key="k:should_expire",
            data_type=CacheDataType.STRING,
            value="temp",
            ttl_ms=2000,
            expire_at_ms=102000,
        )
    )
    coord = CacheMigrationCoordinator(source=src, target=tgt, ttl_tolerance_ms=100)
    coord.sync_plane.start_initial_sync()
    coord.sync_plane.sync_batch(
        unit_id="u-res",
        cursor_boundary=1,
        keys_to_sync=["k:should_expire"],
        read_mono_ms=100,
        apply_mono_ms=200,
        base_wall_ms=100000,
    )

    tgt.keys["k:should_expire"].ttl_ms = None
    tgt.keys["k:should_expire"].expire_at_ms = None

    report = coord.verify_semantics(verify_wall_ms=100500)
    assert "k:should_expire" in report.resurrected_keys
    assert report.overall_badge == CareBadge.ACTION


def test_relative_ttl_fallback_and_checkpoint_skip_in_sync_batch(tmp_path: Path):
    src = CacheEndpoint(name="src", host="127.0.0.1", port=6379)
    tgt = CacheEndpoint(
        name="tgt-old",
        host="10.0.0.20",
        port=6379,
        supports_restore_absttl=False,
        supports_restore_freq=False,
        eviction_policy=EvictionPolicy.VOLATILE_LFU,
    )
    src.eviction_policy = EvictionPolicy.VOLATILE_LFU
    src.write_key(
        CacheKeyRecord(
            key="k:rel",
            data_type=CacheDataType.ZSET,
            value={"m1": 1.0, "m2": 2.0},
            ttl_ms=3000,
            expire_at_ms=53000,
            access=AccessMetadata(idletime_sec=10, freq_counter=55),
        )
    )

    ckpt = CacheCheckpointStore(tmp_path / "rel_ckpt.json")
    coord = CacheMigrationCoordinator(source=src, target=tgt, checkpoint_store=ckpt)
    coord.sync_plane.start_initial_sync()

    first_out = coord.sync_plane.sync_batch(
        unit_id="batch-rel-1",
        cursor_boundary="cursor-1",
        keys_to_sync=["k:rel"],
        read_mono_ms=500,
        apply_mono_ms=800,
        base_wall_ms=50000,
    )
    assert "k:rel" in first_out
    assert first_out["k:rel"].policy_used == TTLPolicy.RELATIVE_COMPENSATED
    assert first_out["k:rel"].target_ttl_ms == 2700

    second_out = coord.sync_plane.sync_batch(
        unit_id="batch-rel-1",
        cursor_boundary="cursor-1",
        keys_to_sync=["k:rel"],
        read_mono_ms=900,
        apply_mono_ms=1200,
        base_wall_ms=50400,
    )
    assert second_out == {}

    report = coord.verify_semantics(verify_wall_ms=50300, allow_declared_degradation=True)
    assert report.fidelity.preserved is False
    assert "lfu_freq" in report.fidelity.unavailable_fields
    assert report.fidelity.care_badge == CareBadge.REVIEW


def test_zset_and_causal_dominance_in_active_active():
    z_local = CacheKeyRecord(
        key="leaderboard",
        data_type=CacheDataType.ZSET,
        value={"alice": 10.0, "bob": 20.0},
        field_timestamps={"alice": (100, "site-a"), "bob": (200, "site-a")},
        version_vector={"site-a": 2, "site-b": 1},
        updated_at_ms=200,
        site_id="site-a",
    )
    z_remote = CacheKeyRecord(
        key="leaderboard",
        data_type=CacheDataType.ZSET,
        value={"alice": 15.0, "carol": 30.0},
        field_timestamps={"alice": (250, "site-b"), "carol": (250, "site-b")},
        version_vector={"site-a": 1, "site-b": 2},
        updated_at_ms=250,
        site_id="site-b",
    )
    out_z = ActiveActiveCRDTResolver.resolve(z_local, z_remote)
    assert out_z.merged_record.value == {"alice": 15.0, "bob": 20.0, "carol": 30.0}

    dom_remote = z_remote.clone()
    dom_remote.version_vector = {"site-a": 3, "site-b": 3}
    out_dom = ActiveActiveCRDTResolver.resolve(z_local, dom_remote)
    assert out_dom.conflict_detected is False
    assert out_dom.resolution_rule == "causal_progression_remote"


def test_pure_ascii_compliance():
    repo_root = Path(__file__).resolve().parent.parent
    mod_path = repo_root / "migkit" / "cache_semantics.py"
    test_path = Path(__file__).resolve()
    for p in (mod_path, test_path):
        content = p.read_text(encoding="utf-8")
        assert all(ord(c) < 128 for c in content), f"non-ASCII character found in {p}"

