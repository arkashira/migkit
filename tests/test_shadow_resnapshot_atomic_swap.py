import os
import shutil
import tempfile
from pathlib import Path
import pytest

from migkit.shadow_resync import (
    AtomicSwapCrashError,
    CDCBuffer,
    CDCEvent,
    CDCOp,
    HeadroomConfig,
    InMemoryTableEngine,
    InsufficientHeadroomError,
    ObjectMetrics,
    ResyncPhase,
    ResyncStrategy,
    RollbackRetentionPolicy,
    ShadowResyncCoordinator,
    SwapJournal,
    ValidationFailedError,
)


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="migkit_shadow_resync_")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def engine():
    eng = InMemoryTableEngine(disk_free_bytes=500 * 1024 * 1024)
    eng.create_table(
        "orders",
        columns=["id", "customer_id", "amount", "status"],
        primary_key="id",
        rows=[
            {"id": 1, "customer_id": 101, "amount": 100, "status": "COMPLETED"},
            {"id": 2, "customer_id": 102, "amount": 250, "status": "PENDING"},
            {"id": 3, "customer_id": 103, "amount": 400, "status": "COMPLETED"},
            {"id": 4, "customer_id": 104, "amount": 50, "status": "SHIPPED"},
        ],
    )
    eng.add_foreign_key("orders", "fk_orders_customer", "customer_id", "customers", "id")
    eng.add_binding("orders", "app_binding_orders")
    return eng


def test_strategy_selection_fenced_range_repair():
    coord = ShadowResyncCoordinator()
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=10000,
        divergent_rows=20,
        divergent_ranges=[(100, 120)],
        estimated_table_bytes=10 * 1024 * 1024,
        supports_atomic_swap=True,
    )
    strategy = coord.select_strategy(metrics, available_disk_bytes=100 * 1024 * 1024)
    assert strategy == ResyncStrategy.FENCED_RANGE_REPAIR


def test_strategy_selection_shadow_resnapshot():
    coord = ShadowResyncCoordinator()
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=10000,
        divergent_rows=3000,
        estimated_table_bytes=10 * 1024 * 1024,
        supports_atomic_swap=True,
    )
    strategy = coord.select_strategy(metrics, available_disk_bytes=200 * 1024 * 1024)
    assert strategy == ResyncStrategy.SHADOW_TABLE_RESNAPSHOT


def test_strategy_selection_schema_drift():
    coord = ShadowResyncCoordinator()
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=10000,
        divergent_rows=5,
        divergent_ranges=[(1, 5)],
        estimated_table_bytes=10 * 1024 * 1024,
        supports_atomic_swap=True,
        schema_drifted=True,
    )
    strategy = coord.select_strategy(metrics, available_disk_bytes=200 * 1024 * 1024)
    assert strategy == ResyncStrategy.SHADOW_TABLE_RESNAPSHOT


def test_strategy_selection_whole_reload_when_swap_unsupported():
    coord = ShadowResyncCoordinator()
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=10000,
        divergent_rows=3000,
        estimated_table_bytes=10 * 1024 * 1024,
        supports_atomic_swap=False,
    )
    strategy = coord.select_strategy(metrics, available_disk_bytes=200 * 1024 * 1024)
    assert strategy == ResyncStrategy.WHOLE_RELOAD


def test_headroom_check_insufficient(temp_dir, engine):
    headroom_cfg = HeadroomConfig(
        headroom_factor=2.0,
        min_free_bytes=50 * 1024 * 1024,
        buffer_headroom_bytes=10 * 1024 * 1024,
    )
    coord = ShadowResyncCoordinator(headroom_config=headroom_cfg, journal_dir=temp_dir)
    engine.disk_free_bytes = 60 * 1024 * 1024
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=10000,
        divergent_rows=2000,
        estimated_table_bytes=30 * 1024 * 1024,
        supports_atomic_swap=True,
    )
    with pytest.raises(InsufficientHeadroomError) as exc_info:
        coord.coordinate_resync(engine, metrics, source_snapshot=[])
    err = exc_info.value
    assert err.available_bytes == 60 * 1024 * 1024
    assert err.required_bytes > err.available_bytes


def test_cdc_buffer_tombstone_reconciliation():
    buf = CDCBuffer(start_lsn=100)
    buf.append(
        CDCEvent(
            lsn=101,
            op=CDCOp.DELETE,
            table="orders",
            key={"id": 2},
            before={"id": 2, "amount": 250},
        )
    )
    buf.append(
        CDCEvent(
            lsn=102,
            op=CDCOp.UPDATE,
            table="orders",
            key={"id": 3},
            before={"id": 3, "amount": 400},
            after={"id": 3, "amount": 450},
        )
    )
    buf.append(
        CDCEvent(
            lsn=103,
            op=CDCOp.INSERT,
            table="orders",
            key={"id": 5},
            after={"id": 5, "customer_id": 105, "amount": 300, "status": "PENDING"},
        )
    )
    buf.append(
        CDCEvent(
            lsn=104,
            op=CDCOp.DELETE,
            table="orders",
            key={"id": 999},
            before={"id": 999},
        )
    )

    snapshot_data = {
        1: {"id": 1, "customer_id": 101, "amount": 100, "status": "COMPLETED"},
        2: {"id": 2, "customer_id": 102, "amount": 250, "status": "PENDING"},
        3: {"id": 3, "customer_id": 103, "amount": 400, "status": "COMPLETED"},
        4: {"id": 4, "customer_id": 104, "amount": 50, "status": "SHIPPED"},
    }

    reconciled = buf.reconcile(snapshot_data, key_column="id")
    assert 2 not in reconciled
    assert reconciled[3]["amount"] == 450
    assert 5 in reconciled
    assert reconciled[5]["amount"] == 300
    assert 999 not in reconciled


def test_resync_concurrent_mutations_and_new_column(temp_dir, engine):
    coord = ShadowResyncCoordinator(journal_dir=temp_dir)
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=3,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )

    source_snapshot = [
        {"id": 1, "customer_id": 101, "amount": 100, "status": "COMPLETED"},
        {"id": 2, "customer_id": 102, "amount": 250, "status": "PENDING"},
        {"id": 3, "customer_id": 103, "amount": 400, "status": "COMPLETED"},
        {"id": 4, "customer_id": 104, "amount": 50, "status": "SHIPPED"},
    ]

    concurrent_cdc = [
        CDCEvent(
            lsn=101,
            op=CDCOp.SCHEMA_CHANGE,
            table="orders",
            key={},
            schema_change={"column": "discount", "type": "int", "default": 0},
        ),
        CDCEvent(
            lsn=102,
            op=CDCOp.DELETE,
            table="orders",
            key={"id": 2},
            before={"id": 2},
        ),
        CDCEvent(
            lsn=103,
            op=CDCOp.UPDATE,
            table="orders",
            key={"id": 1},
            after={"id": 1, "customer_id": 101, "amount": 120, "status": "COMPLETED", "discount": 10},
        ),
        CDCEvent(
            lsn=104,
            op=CDCOp.INSERT,
            table="orders",
            key={"id": 5},
            after={"id": 5, "customer_id": 105, "amount": 600, "status": "NEW", "discount": 0},
        ),
    ]

    report = coord.coordinate_resync(
        engine,
        metrics,
        source_snapshot=source_snapshot,
        cdc_events=concurrent_cdc,
        start_lsn=100,
    )

    assert report.phase == ResyncPhase.SWAPPED
    assert report.strategy == ResyncStrategy.SHADOW_TABLE_RESNAPSHOT

    live_rows = {r["id"]: r for r in engine.get_rows("orders")}
    assert 2 not in live_rows
    assert live_rows[1]["amount"] == 120
    assert live_rows[1]["discount"] == 10
    assert live_rows[5]["amount"] == 600
    assert live_rows[5]["discount"] == 0
    assert live_rows[3]["discount"] == 0

    assert engine.has_foreign_key("orders", "fk_orders_customer")
    assert "app_binding_orders" in engine.get_bindings("orders")
    assert engine.table_exists(report.old_table)


def test_live_table_active_during_snapshot(temp_dir, engine):
    coord = ShadowResyncCoordinator(journal_dir=temp_dir)
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=2,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )

    live_queries_succeeded = []

    def on_snapshot_hook():
        rows = engine.get_rows("orders")
        live_queries_succeeded.append(len(rows) == 4)

    coord.on_snapshot_in_progress = on_snapshot_hook

    source_snapshot = [
        {"id": 1, "customer_id": 101, "amount": 100, "status": "COMPLETED"},
        {"id": 2, "customer_id": 102, "amount": 250, "status": "PENDING"},
        {"id": 3, "customer_id": 103, "amount": 400, "status": "COMPLETED"},
        {"id": 4, "customer_id": 104, "amount": 50, "status": "SHIPPED"},
    ]

    report = coord.coordinate_resync(
        engine,
        metrics,
        source_snapshot=source_snapshot,
        start_lsn=50,
    )
    assert report.phase == ResyncPhase.SWAPPED
    assert len(live_queries_succeeded) == 1
    assert live_queries_succeeded[0] is True


def test_validation_failure_prevents_swap(temp_dir, engine):
    coord = ShadowResyncCoordinator(journal_dir=temp_dir)
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=2,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )

    source_snapshot = [
        {"id": 1, "customer_id": 101, "amount": -99999, "status": "INVALID"},
    ]

    def validator(eng, shadow_name):
        return False, ["Invalid row count and corrupt values"]

    coord.custom_validator = validator

    with pytest.raises(ValidationFailedError):
        coord.coordinate_resync(
            engine,
            metrics,
            source_snapshot=source_snapshot,
            start_lsn=10,
        )

    rows = engine.get_rows("orders")
    assert len(rows) == 4
    assert rows[0]["amount"] == 100
    assert not engine.table_exists("_migkit_shadow_orders")


def test_fenced_range_repair_execution(temp_dir, engine):
    coord = ShadowResyncCoordinator(journal_dir=temp_dir)
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=100,
        divergent_rows=1,
        divergent_ranges=[(2, 2)],
        estimated_table_bytes=2048,
        supports_atomic_swap=True,
    )

    repair_rows = [{"id": 2, "customer_id": 102, "amount": 999, "status": "REPAIRED"}]

    report = coord.coordinate_resync(
        engine,
        metrics,
        source_snapshot=repair_rows,
        start_lsn=200,
    )

    assert report.strategy == ResyncStrategy.FENCED_RANGE_REPAIR
    live_rows = {r["id"]: r for r in engine.get_rows("orders")}
    assert live_rows[2]["amount"] == 999
    assert live_rows[2]["status"] == "REPAIRED"
    assert live_rows[1]["amount"] == 100


def test_crash_at_swap_recovery_stage_rename_live(temp_dir, engine):
    coord = ShadowResyncCoordinator(journal_dir=temp_dir)
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=2,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )

    source_snapshot = [
        {"id": 1, "customer_id": 101, "amount": 100, "status": "COMPLETED"},
        {"id": 2, "customer_id": 102, "amount": 350, "status": "UPDATED"},
        {"id": 3, "customer_id": 103, "amount": 400, "status": "COMPLETED"},
        {"id": 4, "customer_id": 104, "amount": 50, "status": "SHIPPED"},
    ]

    with pytest.raises(AtomicSwapCrashError):
        coord.coordinate_resync(
            engine,
            metrics,
            source_snapshot=source_snapshot,
            start_lsn=300,
            simulate_crash_stage="AFTER_RENAME_LIVE",
        )

    recovery = coord.recover_from_crash(engine, session_id=coord.active_session_id)
    assert recovery.recovered is True
    assert recovery.active_table_verified is True

    rows = {r["id"]: r for r in engine.get_rows("orders")}
    assert len(rows) == 4
    assert rows[2]["amount"] == 350
    assert not engine.table_exists("_migkit_shadow_orders")


def test_crash_before_swap_recovery(temp_dir, engine):
    coord = ShadowResyncCoordinator(journal_dir=temp_dir)
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=2,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )

    source_snapshot = [
        {"id": 1, "customer_id": 101, "amount": 100, "status": "COMPLETED"},
        {"id": 2, "customer_id": 102, "amount": 350, "status": "UPDATED"},
        {"id": 3, "customer_id": 103, "amount": 400, "status": "COMPLETED"},
        {"id": 4, "customer_id": 104, "amount": 50, "status": "SHIPPED"},
    ]

    with pytest.raises(AtomicSwapCrashError):
        coord.coordinate_resync(
            engine,
            metrics,
            source_snapshot=source_snapshot,
            start_lsn=400,
            simulate_crash_stage="BEFORE_SWAP",
        )

    recovery = coord.recover_from_crash(engine, session_id=coord.active_session_id)
    assert recovery.recovered is True

    rows = {r["id"]: r for r in engine.get_rows("orders")}
    assert len(rows) == 4
    assert rows[2]["amount"] == 250


def test_old_table_rollback_retention_and_rollback(temp_dir, engine):
    coord = ShadowResyncCoordinator(
        retention_policy=RollbackRetentionPolicy(retention_ttl_seconds=3600),
        journal_dir=temp_dir,
    )
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=2,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )

    source_snapshot = [
        {"id": 1, "customer_id": 101, "amount": 900, "status": "COMPLETED"},
        {"id": 2, "customer_id": 102, "amount": 900, "status": "COMPLETED"},
        {"id": 3, "customer_id": 103, "amount": 900, "status": "COMPLETED"},
        {"id": 4, "customer_id": 104, "amount": 900, "status": "COMPLETED"},
    ]

    report = coord.coordinate_resync(
        engine,
        metrics,
        source_snapshot=source_snapshot,
        start_lsn=500,
    )
    assert report.phase == ResyncPhase.SWAPPED
    old_table_name = report.old_table
    assert engine.table_exists(old_table_name)
    assert engine.get_rows("orders")[0]["amount"] == 900

    rollback_res = coord.rollback_swap(engine, session_id=report.session_id)
    assert rollback_res.rolled_back is True
    assert engine.get_rows("orders")[0]["amount"] == 100

    purged = coord.purge_retention(engine, session_id=report.session_id, force=True)
    assert purged is True
    assert not engine.table_exists(old_table_name)


def test_crash_after_rename_shadow_recovery(temp_dir, engine):
    coord = ShadowResyncCoordinator(journal_dir=temp_dir)
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=2,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )

    source_snapshot = [
        {"id": 1, "customer_id": 101, "amount": 888, "status": "COMPLETED"},
        {"id": 2, "customer_id": 102, "amount": 888, "status": "COMPLETED"},
        {"id": 3, "customer_id": 103, "amount": 888, "status": "COMPLETED"},
        {"id": 4, "customer_id": 104, "amount": 888, "status": "COMPLETED"},
    ]

    with pytest.raises(AtomicSwapCrashError):
        coord.coordinate_resync(
            engine,
            metrics,
            source_snapshot=source_snapshot,
            start_lsn=600,
            simulate_crash_stage="AFTER_RENAME_SHADOW",
        )

    recovery = coord.recover_from_crash(engine, session_id=coord.active_session_id)
    assert recovery.recovered is True
    assert recovery.active_table_verified is True
    assert recovery.final_phase == ResyncPhase.SWAPPED

    rows = {r["id"]: r for r in engine.get_rows("orders")}
    assert len(rows) == 4
    assert rows[1]["amount"] == 888


def test_complex_cdc_buffer_lifecycle():
    buf = CDCBuffer(start_lsn=200)

    buf.append(
        CDCEvent(
            lsn=150,
            op=CDCOp.INSERT,
            table="orders",
            key={"id": 99},
            after={"id": 99, "amount": 99},
        )
    )

    buf.append(
        CDCEvent(
            lsn=210,
            op=CDCOp.UPDATE,
            table="orders",
            key={"id": 10},
            after={"id": 10, "amount": 25},
        )
    )
    buf.append(
        CDCEvent(
            lsn=205,
            op=CDCOp.INSERT,
            table="orders",
            key={"id": 10},
            after={"id": 10, "amount": 20},
        )
    )
    buf.append(
        CDCEvent(
            lsn=215,
            op=CDCOp.DELETE,
            table="orders",
            key={"id": 10},
            before={"id": 10},
        )
    )

    events = buf.events()
    assert len(events) == 3
    assert [e.lsn for e in events] == [205, 210, 215]

    reconciled = buf.reconcile({}, key_column="id")
    assert 10 not in reconciled
    assert 99 not in reconciled


def test_headroom_deficit_with_allow_whole_reload_fallback(temp_dir, engine):
    headroom_cfg = HeadroomConfig(
        headroom_factor=2.0,
        min_free_bytes=50 * 1024 * 1024,
        buffer_headroom_bytes=10 * 1024 * 1024,
    )
    coord = ShadowResyncCoordinator(
        headroom_config=headroom_cfg,
        journal_dir=temp_dir,
        allow_whole_reload_fallback=True,
    )
    engine.disk_free_bytes = 60 * 1024 * 1024
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=10000,
        divergent_rows=2000,
        estimated_table_bytes=30 * 1024 * 1024,
        supports_atomic_swap=True,
    )

    snapshot_data = [
        {"id": 1, "customer_id": 101, "amount": 777, "status": "RELOADED"},
    ]

    report = coord.coordinate_resync(
        engine,
        metrics,
        source_snapshot=snapshot_data,
        start_lsn=700,
    )
    assert report.strategy == ResyncStrategy.WHOLE_RELOAD
    rows = engine.get_rows("orders")
    assert len(rows) == 1
    assert rows[0]["amount"] == 777


def test_rollback_error_when_old_table_missing(temp_dir, engine):
    from migkit.shadow_resync import RollbackError

    coord = ShadowResyncCoordinator(journal_dir=temp_dir)
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=2,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )
    report = coord.coordinate_resync(
        engine,
        metrics,
        source_snapshot=[{"id": 1, "customer_id": 101, "amount": 1, "status": "X"}],
        start_lsn=800,
    )

    engine.drop_table(report.old_table)
    with pytest.raises(RollbackError):
        coord.rollback_swap(engine, session_id=report.session_id)


def test_purge_retention_ttl_guard(temp_dir, engine):
    coord = ShadowResyncCoordinator(
        retention_policy=RollbackRetentionPolicy(retention_ttl_seconds=300),
        journal_dir=temp_dir,
    )
    metrics = ObjectMetrics(
        table_name="orders",
        total_rows=4,
        divergent_rows=2,
        estimated_table_bytes=1024,
        supports_atomic_swap=True,
    )
    report = coord.coordinate_resync(
        engine,
        metrics,
        source_snapshot=[{"id": 1, "customer_id": 101, "amount": 555, "status": "OK"}],
        start_lsn=900,
    )
    old_tbl = report.old_table
    assert engine.table_exists(old_tbl)

    purged = coord.purge_retention(engine, session_id=report.session_id, force=False)
    assert purged is False
    assert engine.table_exists(old_tbl)

    purged_forced = coord.purge_retention(engine, session_id=report.session_id, force=True)
    assert purged_forced is True
    assert not engine.table_exists(old_tbl)


def test_safe_endpoint_address_binding():
    metrics = ObjectMetrics(table_name="orders")
    endpoint_ip = "127.0.0.1"
    safe_network = "10.0.0.1"
    assert endpoint_ip.startswith("127.0.0.")
    assert safe_network.startswith("10.0.0.")

