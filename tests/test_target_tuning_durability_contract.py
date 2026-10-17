"""Tests for Target Ingest Tuning Durability Contract (Row 633: RCH25.target-tuning-contract).

Proves that:
1. Ingest tuning is treated as a bounded temporary durability contract with approved
   durability bounds (`synchronous_commit`, `fsync`, `wal_level`, `maintenance_work_mem`,
   `innodb_flush_log_at_trx_commit`, `sync_binlog`).
2. Protections are never turned off globally merely because a faster mode exists;
   session/table scopes are chosen via `migkit.decide` rungs and unauthorized global
   changes are rejected.
3. Original settings, secondary index definitions, constraints, and triggers are
   observed and durably persisted in a write-ahead restore journal before any
   protection is relaxed; primary key and unique constraint indexes are never dropped.
4. Index rebuild, constraint validation, `ANALYZE` tails, and crash recovery costs
   are priced; tuning changes whose tail costs exceed ingest savings are rejected.
5. Contract lease mutual exclusion (`.run.lock`) and lease expiration during load
   halt tuned ingest and trigger immediate restoration.
6. Crashes during load and crashes mid-restoration recover idempotently in O(1)
   and never leave production with weakened durability or disabled/unvalidated constraints.
7. `AppReadinessGate` never publishes `app-ready` while checks/settings are relaxed,
   indexes are missing or invalid (`indisvalid=false`), constraints are disabled or
   `NOT VALID` (`convalidated=false`), triggers are disabled, or `ANALYZE` is incomplete.
8. Overall measured completion (`MeasuredCompletionEvidence`) justifies each tuning change.
9. Pure UNIX ASCII reporting with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS)
   and safe test host validation (127.0.0.1, 10.0.0.x).
"""

from __future__ import annotations

import datetime
import decimal
import uuid
from pathlib import Path
from typing import List

import pytest
import tests.rung_branches

from migkit.target_tuning_contract import (
    AppReadinessBlockedError,
    AppReadinessGate,
    CareBadge,
    ContractLeaseError,
    ContractLeaseExpiredError,
    ContractPhase,
    DeferredConstraintRecord,
    DeferredIndexRecord,
    DeferredTriggerRecord,
    DurabilityBoundsPolicy,
    DurabilityBoundViolationError,
    DurableRestoreCheckpoint,
    GlobalProtectionViolationError,
    MeasuredCompletionEvidence,
    RestoreVerificationError,
    SettingScope,
    TailAndRecoveryCostModel,
    TargetTuningBackend,
    TargetTuningCoordinator,
    TuningContractLease,
    UnjustifiedTuningError,
    UnsafeTargetHostError,
    decode_typed_value,
    encode_typed_value,
    is_safe_test_host,
    select_tuning_contract_way,
)

PROVES = [
    ("target-tuning-contract.durability-bounds", "postgres"),
    ("target-tuning-contract.durability-bounds", "mysql"),
]

PROVES_RUNGS = [
    ("target_tuning_contract", "select_tuning_contract_way", "session_and_table_contract", "holds"),
    ("target_tuning_contract", "select_tuning_contract_way", "session_and_table_contract", "refuses"),
    ("target_tuning_contract", "select_tuning_contract_way", "session_parameter_contract", "holds"),
    ("target_tuning_contract", "select_tuning_contract_way", "session_parameter_contract", "refuses"),
    ("target_tuning_contract", "select_tuning_contract_way", "untuned_baseline", "holds"),
]


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def pg_backend() -> TargetTuningBackend:
    indexes = [
        DeferredIndexRecord(
            index_key="public.orders_pkey",
            schema="public",
            table="orders",
            index_name="orders_pkey",
            create_ddl="CREATE UNIQUE INDEX orders_pkey ON public.orders (id)",
            drop_ddl="DROP INDEX IF EXISTS public.orders_pkey",
            is_unique=True,
            is_primary=True,
            is_constraint_backed=True,
            estimated_rows=500000,
            estimated_bytes=200 * 1024 * 1024,
        ),
        DeferredIndexRecord(
            index_key="public.orders_code_key",
            schema="public",
            table="orders",
            index_name="orders_code_key",
            create_ddl="CREATE UNIQUE INDEX orders_code_key ON public.orders (code)",
            drop_ddl="DROP INDEX IF EXISTS public.orders_code_key",
            is_unique=True,
            is_primary=False,
            is_constraint_backed=True,
            estimated_rows=500000,
            estimated_bytes=200 * 1024 * 1024,
        ),
        DeferredIndexRecord(
            index_key="public.ix_orders_customer",
            schema="public",
            table="orders",
            index_name="ix_orders_customer",
            create_ddl="CREATE INDEX ix_orders_customer ON public.orders (customer_id)",
            drop_ddl="DROP INDEX CONCURRENTLY IF EXISTS public.ix_orders_customer",
            is_unique=False,
            is_primary=False,
            is_constraint_backed=False,
            estimated_rows=500000,
            estimated_bytes=200 * 1024 * 1024,
        ),
        DeferredIndexRecord(
            index_key="public.ix_orders_created_at",
            schema="public",
            table="orders",
            index_name="ix_orders_created_at",
            create_ddl="CREATE INDEX ix_orders_created_at ON public.orders (created_at, status)",
            drop_ddl="DROP INDEX CONCURRENTLY IF EXISTS public.ix_orders_created_at",
            is_unique=False,
            is_primary=False,
            is_constraint_backed=False,
            estimated_rows=500000,
            estimated_bytes=200 * 1024 * 1024,
        ),
        DeferredIndexRecord(
            index_key="public.ix_order_items_order_id",
            schema="public",
            table="order_items",
            index_name="ix_order_items_order_id",
            create_ddl="CREATE INDEX ix_order_items_order_id ON public.order_items (order_id)",
            drop_ddl="DROP INDEX CONCURRENTLY IF EXISTS public.ix_order_items_order_id",
            is_unique=False,
            is_primary=False,
            is_constraint_backed=False,
            estimated_rows=500000,
            estimated_bytes=200 * 1024 * 1024,
        ),
    ]
    constraints = [
        DeferredConstraintRecord(
            constraint_key="public.orders.fk_orders_customer",
            schema="public",
            table="orders",
            constraint_name="fk_orders_customer",
            constraint_type="FOREIGN KEY",
            definition_ddl="FOREIGN KEY (customer_id) REFERENCES public.customers(id)",
            originally_validated=True,
            referenced_schema="public",
            referenced_table="customers",
            estimated_rows=500000,
        ),
        DeferredConstraintRecord(
            constraint_key="public.order_items.fk_items_order",
            schema="public",
            table="order_items",
            constraint_name="fk_items_order",
            constraint_type="FOREIGN KEY",
            definition_ddl="FOREIGN KEY (order_id) REFERENCES public.orders(id)",
            originally_validated=True,
            referenced_schema="public",
            referenced_table="orders",
            estimated_rows=500000,
        ),
        DeferredConstraintRecord(
            constraint_key="public.orders.chk_orders_total_positive",
            schema="public",
            table="orders",
            constraint_name="chk_orders_total_positive",
            constraint_type="CHECK",
            definition_ddl="CHECK (total_amount >= 0)",
            originally_validated=True,
            estimated_rows=500000,
        ),
    ]
    triggers = [
        DeferredTriggerRecord(
            trigger_key="public.orders.trg_orders_audit",
            schema="public",
            table="orders",
            trigger_name="trg_orders_audit",
            originally_enabled=True,
        ),
    ]
    return TargetTuningBackend(
        engine="postgres",
        database="app",
        host="127.0.0.1",
        initial_settings={
            "synchronous_commit": "on",
            "fsync": "on",
            "wal_level": "replica",
            "maintenance_work_mem": "64MB",
            "session_replication_role": "origin",
            "autovacuum_enabled": "on",
        },
        initial_indexes=indexes,
        initial_constraints=constraints,
        initial_triggers=triggers,
        tables=["public.customers", "public.orders", "public.order_items"],
    )


# -----------------------------------------------------------------------------
# Test 1: Approved Durability Bounds & Unsafe Relaxations Refused
# -----------------------------------------------------------------------------

def test_durability_bounds_block_unsafe_relaxations(pg_backend: TargetTuningBackend, tmp_path: Path):
    """Verify fsync=off, wal_level=minimal, non-replayable sync_commit=off, and excessive memory fail."""
    policy = DurabilityBoundsPolicy(
        allow_synchronous_commit_off=True,
        allow_fsync_relaxation=False,
        allow_wal_level_minimal=False,
        max_maintenance_work_mem_mb=2048,
    )
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend, policy=policy)

    # 1. fsync=off is blocked by default durability bounds
    with pytest.raises(DurabilityBoundViolationError, match="fsync=off violates approved durability bounds"):
        coord.begin_contract(
            contract_id="c-fsync",
            proposed_settings={"fsync": ("off", SettingScope.SESSION, None)},
            total_rows=500000,
            total_bytes=500 * 1024 * 1024,
        )

    # 2. wal_level=minimal is blocked when physical standbys / PITR bounds forbid it
    with pytest.raises(DurabilityBoundViolationError, match="wal_level=minimal violates durability bounds"):
        coord.begin_contract(
            contract_id="c-wal",
            proposed_settings={"wal_level": ("minimal", SettingScope.SESSION, None)},
            total_rows=500000,
            total_bytes=500 * 1024 * 1024,
        )

    # 3. synchronous_commit=off is blocked if ranges are NOT crash-replayable from source
    with pytest.raises(DurabilityBoundViolationError, match="not marked crash-replayable"):
        coord.begin_contract(
            contract_id="c-no-replay",
            proposed_settings={"synchronous_commit": ("off", SettingScope.SESSION, None)},
            total_rows=500000,
            total_bytes=500 * 1024 * 1024,
            crash_replayable=False,
        )

    # 4. maintenance_work_mem beyond max_maintenance_work_mem_mb is blocked
    with pytest.raises(DurabilityBoundViolationError, match="outside approved bounds"):
        coord.begin_contract(
            contract_id="c-mem",
            proposed_settings={"maintenance_work_mem": ("8GB", SettingScope.SESSION, None)},
            total_rows=500000,
            total_bytes=500 * 1024 * 1024,
        )


def test_mysql_durability_bounds_block_unsafe_flush_and_binlog(tmp_path: Path):
    """Verify MySQL innodb_flush_log_at_trx_commit=0 and sync_binlog=0 are blocked by bounds."""
    my_backend = TargetTuningBackend(
        engine="mysql",
        database="shop",
        host="10.0.0.12",
        initial_settings={
            "innodb_flush_log_at_trx_commit": 1,
            "sync_binlog": 1,
            "foreign_key_checks": 1,
            "unique_checks": 1,
        },
        tables=["shop.orders"],
    )
    policy = DurabilityBoundsPolicy(
        allow_flush_log_at_trx_commit_2=True,
        allow_flush_log_at_trx_commit_0=False,
        allow_sync_binlog_0=False,
    )
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=my_backend, policy=policy)

    with pytest.raises(DurabilityBoundViolationError, match="innodb_flush_log_at_trx_commit=0"):
        coord.begin_contract(
            contract_id="my-trx0",
            proposed_settings={"innodb_flush_log_at_trx_commit": (0, SettingScope.SESSION, None)},
            total_rows=200000,
            total_bytes=200 * 1024 * 1024,
        )

    with pytest.raises(DurabilityBoundViolationError, match="sync_binlog=0"):
        coord.begin_contract(
            contract_id="my-binlog0",
            proposed_settings={"sync_binlog": (0, SettingScope.SESSION, None)},
            total_rows=200000,
            total_bytes=200 * 1024 * 1024,
        )


# -----------------------------------------------------------------------------
# Test 2: Do Not Globally Turn Off Protections Merely Because Faster Mode Exists
# -----------------------------------------------------------------------------

def test_global_protection_relaxation_rejected_when_session_scope_exists(
    pg_backend: TargetTuningBackend, tmp_path: Path
):
    """Verify GLOBAL tuning is refused when SESSION scope exists or global scope is unauthorized."""
    # Even if operator enables allow_global_scope, synchronous_commit and maintenance_work_mem
    # support SESSION scope, so turning them off globally for every client on the server is refused.
    policy = DurabilityBoundsPolicy(
        allow_global_scope=True,
        authorized_global_settings=frozenset({"synchronous_commit", "max_wal_size"}),
    )
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend, policy=policy)

    with pytest.raises(
        GlobalProtectionViolationError,
        match="supports SESSION scope; refusing to alter GLOBAL protection",
    ):
        coord.begin_contract(
            contract_id="c-global-sync",
            proposed_settings={"synchronous_commit": ("off", SettingScope.GLOBAL, None)},
            total_rows=500000,
            total_bytes=500 * 1024 * 1024,
        )

    # And a true global setting (e.g. max_wal_size) without authorization is refused
    unauth_policy = DurabilityBoundsPolicy(allow_global_scope=False)
    unauth_coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend, policy=unauth_policy)
    pg_backend.live_settings["max_wal_size"] = "1GB"
    with pytest.raises(GlobalProtectionViolationError, match="global setting max_wal_size"):
        unauth_coord.begin_contract(
            contract_id="c-global-unauth",
            proposed_settings={"max_wal_size": ("40GB", SettingScope.GLOBAL, None)},
            total_rows=500000,
            total_bytes=500 * 1024 * 1024,
        )


# -----------------------------------------------------------------------------
# Test 3: Tail & Crash-Recovery Pricing + Justification Gate
# -----------------------------------------------------------------------------

def test_tail_and_crash_recovery_pricing_rejects_unjustified_small_load(
    pg_backend: TargetTuningBackend, tmp_path: Path
):
    """Verify that when index/constraint/ANALYZE tails exceed ingest speedup, tuning is rejected."""
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend)

    # Small load: 500 rows (100 KB). Dropping 3 indexes, rebuilding them, validating 3 constraints,
    # and running ANALYZE costs far more in setup/tail overhead than the tiny ingest speedup.
    with pytest.raises(UnjustifiedTuningError, match="does not beat baseline"):
        coord.begin_contract(
            contract_id="c-tiny-unjustified",
            proposed_settings={
                "synchronous_commit": ("off", SettingScope.SESSION, None),
                "maintenance_work_mem": ("512MB", SettingScope.SESSION, None),
            },
            total_rows=500,
            total_bytes=100 * 1024,
            defer_indexes=True,
            defer_constraints=True,
            require_justified=True,
        )

    # Verify target protections were NOT touched when tuning was rejected
    assert pg_backend.live_settings["synchronous_commit"] == "on"
    assert pg_backend.live_indexes["public.ix_orders_customer"]["present"] is True
    assert pg_backend.live_constraints["public.orders.fk_orders_customer"]["present"] is True


def test_tail_pricing_selects_full_contract_for_large_bulk_load(pg_backend: TargetTuningBackend):
    """Verify `select_tuning_contract_way` climbs to `session_and_table_contract` on large loads."""
    policy = DurabilityBoundsPolicy()
    model = TailAndRecoveryCostModel()
    choice, justification = select_tuning_contract_way(
        policy=policy,
        cost_model=model,
        total_rows=2_000_000,
        total_bytes=1024 * 1024 * 1024,  # 1 GB
        secondary_index_count=3,
        fk_constraint_count=2,
        crash_replayable=True,
        maintenance_work_mem_mb=1024,
        parallel_workers=4,
    )
    assert choice.rung is not None
    assert choice.rung.name == "session_and_table_contract"
    assert justification.justified is True
    assert justification.care_badge == CareBadge.PASS
    cb = justification.cost_breakdown
    assert cb.index_rebuild_tail_seconds > 0
    assert cb.constraint_validation_tail_seconds > 0
    assert cb.analyze_tail_seconds > 0
    assert cb.crash_recovery_expected_seconds > 0
    assert cb.total_tuned_completion_seconds < cb.baseline_ingest_seconds
    assert cb.net_savings_seconds > 0


# -----------------------------------------------------------------------------
# Test 4: Observed Original Settings & Primary/Unique Indexes Never Dropped
# -----------------------------------------------------------------------------

def test_observed_original_settings_and_primary_unique_indexes_preserved(
    pg_backend: TargetTuningBackend, tmp_path: Path
):
    """Verify write-ahead journal records custom observed settings and never drops PK/unique indexes."""
    # Customize server baseline before load so restore must use observed values, not static defaults
    pg_backend.live_settings["synchronous_commit"] = "remote_write"
    pg_backend.live_settings["maintenance_work_mem"] = "192MB"

    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend)
    contract = coord.begin_contract(
        contract_id="c-observed",
        proposed_settings={
            "synchronous_commit": ("off", SettingScope.SESSION, None),
            "maintenance_work_mem": ("1024MB", SettingScope.SESSION, None),
        },
        total_rows=1_000_000,
        total_bytes=600 * 1024 * 1024,
        now=1000.0,
    )

    # Journal file exists on disk with exact observed values
    journal_file = coord.journal_path("c-observed")
    assert journal_file.exists()
    assert contract.snapshot.settings["synchronous_commit"].original_value == "remote_write"
    assert contract.snapshot.settings["maintenance_work_mem"].original_value == "192MB"

    # Primary key and unique constraint-backing indexes were NEVER dropped
    assert "public.orders_pkey" not in contract.snapshot.indexes
    assert "public.orders_code_key" not in contract.snapshot.indexes
    assert pg_backend.live_indexes["public.orders_pkey"]["present"] is True
    assert pg_backend.live_indexes["public.orders_code_key"]["present"] is True

    # Non-unique secondary indexes WERE deferred (dropped during load)
    assert "public.ix_orders_customer" in contract.snapshot.indexes
    assert pg_backend.live_indexes["public.ix_orders_customer"]["present"] is False

    # Restore brings back exact observed values ('remote_write' and '192MB'), not generic defaults
    restored = coord.restore_contract("c-observed", measured_ingest_seconds=6.5, now=1020.0)
    assert restored.is_fully_restored() is True
    assert pg_backend.live_settings["synchronous_commit"] == "remote_write"
    assert pg_backend.live_settings["maintenance_work_mem"] == "192MB"
    assert pg_backend.live_indexes["public.ix_orders_customer"]["present"] is True
    assert pg_backend.live_indexes["public.ix_orders_customer"]["indisvalid"] is True
    assert restored.measured_evidence is not None
    assert restored.measured_evidence.justified_by_measured_completion() is True


# -----------------------------------------------------------------------------
# Test 5: Never Publish App-Ready While Protections Are Disabled or Unvalidated
# -----------------------------------------------------------------------------

def test_never_publish_app_ready_while_checks_disabled_or_constraints_unvalidated(
    pg_backend: TargetTuningBackend, tmp_path: Path
):
    """Verify AppReadinessGate blocks app-ready at every incomplete sub-state."""
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend)
    contract = coord.begin_contract(
        contract_id="c-gate",
        proposed_settings={
            "synchronous_commit": ("off", SettingScope.SESSION, None),
            "maintenance_work_mem": ("512MB", SettingScope.SESSION, None),
        },
        total_rows=1_000_000,
        total_bytes=500 * 1024 * 1024,
        now=2000.0,
    )

    # 1. During ACTIVE_LOAD: settings relaxed, indexes dropped, constraints removed, triggers off
    verdict_during = AppReadinessGate.evaluate(pg_backend, [contract])
    assert verdict_during.app_ready is False
    assert verdict_during.care_badge == CareBadge.ACTION
    categories = {f.category for f in verdict_during.findings}
    assert "contract_phase" in categories
    assert "unrestored_setting" in categories
    assert "weakened_durability" in categories
    assert "missing_index" in categories
    assert "missing_constraint" in categories
    assert "disabled_trigger" in categories
    assert "missing_analyze" in categories

    with pytest.raises(AppReadinessBlockedError, match="app_ready=false"):
        AppReadinessGate.publish_app_ready(pg_backend, [contract])

    # 2. Complete full restoration
    restored = coord.restore_contract("c-gate", measured_ingest_seconds=5.0, now=2030.0)
    verdict_after = AppReadinessGate.publish_app_ready(pg_backend, [restored])
    assert verdict_after.app_ready is True
    assert verdict_after.care_badge == CareBadge.PASS

    # 3. If a constraint is present but NOT VALID (convalidated=False), app-ready is blocked
    pg_backend.live_constraints["public.orders.fk_orders_customer"]["convalidated"] = False
    with pytest.raises(AppReadinessBlockedError, match="unvalidated_constraint"):
        AppReadinessGate.publish_app_ready(pg_backend, [restored])
    pg_backend.live_constraints["public.orders.fk_orders_customer"]["convalidated"] = True

    # 4. If an index is present but invalid (indisvalid=False), app-ready is blocked
    pg_backend.live_indexes["public.ix_orders_customer"]["indisvalid"] = False
    with pytest.raises(AppReadinessBlockedError, match="invalid_index"):
        AppReadinessGate.publish_app_ready(pg_backend, [restored])
    pg_backend.live_indexes["public.ix_orders_customer"]["indisvalid"] = True

    # 5. If ANALYZE has not run on a table, app-ready is blocked
    pg_backend.analyzed_tables.discard("public.orders")
    with pytest.raises(AppReadinessBlockedError, match="missing_analyze"):
        AppReadinessGate.publish_app_ready(pg_backend, [restored])


# -----------------------------------------------------------------------------
# Test 6: Contract Lease Mutual Exclusion & Expiration Triggers Auto-Restore
# -----------------------------------------------------------------------------

def test_contract_lease_mutual_exclusion_and_expiration_auto_restore(
    pg_backend: TargetTuningBackend, tmp_path: Path
):
    """Verify concurrent lease contention fails and expired lease restores target automatically."""
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend)
    contract = coord.begin_contract(
        contract_id="c-lease",
        proposed_settings={"synchronous_commit": ("off", SettingScope.SESSION, None)},
        total_rows=1_000_000,
        total_bytes=500 * 1024 * 1024,
        lease_duration_seconds=60.0,
        now=3000.0,
    )

    # Another worker attempting to acquire a non-stale lease in the same state_dir is blocked
    rival_lease = TuningContractLease(
        state_dir=tmp_path,
        contract_id="c-rival",
        lease_duration_seconds=60.0,
        host="10.0.0.99",
        pid=999999,
    )
    with pytest.raises(ContractLeaseError, match="Active tuning contract lease held"):
        rival_lease.acquire(now=3010.0, steal_stale=True)

    # Batch 1 within lease window succeeds and records typed cursor
    ran_r1 = coord.execute_load_range(
        contract,
        range_id="range-001",
        cursor_value=(datetime.datetime(2026, 10, 17, 10, 0, tzinfo=datetime.timezone.utc), 1000),
        now=3020.0,
    )
    assert ran_r1 is True

    # Re-running range-001 skips in O(1)
    ran_r1_again = coord.execute_load_range(
        contract,
        range_id="range-001",
        cursor_value=(datetime.datetime(2026, 10, 17, 10, 0, tzinfo=datetime.timezone.utc), 1000),
        now=3025.0,
    )
    assert ran_r1_again is False

    # Batch 2 after lease expiration (now=3075.0 > expires_at=3060.0) raises ContractLeaseExpiredError
    # AND automatically restores all target protections so production is never left weakened!
    with pytest.raises(ContractLeaseExpiredError, match="lease expired"):
        coord.execute_load_range(
            contract,
            range_id="range-002",
            cursor_value=(datetime.datetime(2026, 10, 17, 11, 0, tzinfo=datetime.timezone.utc), 2000),
            now=3075.0,
        )

    # Verify emergency restoration restored all settings, indexes, constraints, triggers, and ANALYZE
    assert pg_backend.live_settings["synchronous_commit"] == "on"
    assert pg_backend.live_indexes["public.ix_orders_customer"]["present"] is True
    assert pg_backend.live_constraints["public.orders.fk_orders_customer"]["convalidated"] is True
    assert pg_backend.live_triggers["public.orders.trg_orders_audit"]["enabled"] is True
    assert AppReadinessGate.evaluate(pg_backend).app_ready is True


# -----------------------------------------------------------------------------
# Test 7: Crash During Load & Crash During Restoration Recover Cleanly in O(1)
# -----------------------------------------------------------------------------

def test_crash_during_load_and_crash_during_restoration_recover_cleanly(
    pg_backend: TargetTuningBackend, tmp_path: Path
):
    """Prove crashes during load and mid-restoration never leave production weakened."""
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend)
    contract = coord.begin_contract(
        contract_id="c-crash",
        proposed_settings={
            "synchronous_commit": ("off", SettingScope.SESSION, None),
            "maintenance_work_mem": ("1024MB", SettingScope.SESSION, None),
        },
        total_rows=1_000_000,
        total_bytes=500 * 1024 * 1024,
        now=4000.0,
    )
    coord.execute_load_range(
        contract,
        range_id="range-001",
        cursor_value=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        now=4010.0,
    )

    # Simulate process crash mid-load, followed by a second crash mid-restoration
    # right when the first VALIDATE CONSTRAINT statement runs!
    failed_once = {"triggered": False}

    def fault_inject_hook(sql: str) -> None:
        if "VALIDATE CONSTRAINT" in sql and not failed_once["triggered"]:
            failed_once["triggered"] = True
            raise RuntimeError("simulated power loss during VALIDATE CONSTRAINT")

    pg_backend.sql_hook = fault_inject_hook

    # First recovery attempt after load crash hits the simulated power loss during constraint validation
    recovery_coord_1 = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend)
    with pytest.raises(RuntimeError, match="simulated power loss during VALIDATE CONSTRAINT"):
        recovery_coord_1.recover_crashed_contracts(now=4100.0)

    # At this point mid-restoration: settings and indexes were restored, but constraints & ANALYZE are not done.
    # Readiness gate MUST refuse to publish app-ready!
    assert AppReadinessGate.evaluate(pg_backend).app_ready is False

    # Clear fault hook and run second recovery attempt
    pg_backend.sql_hook = None
    sql_count_before_resume = len(pg_backend.executed_sql)
    recovery_coord_2 = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend)
    recovered_list = recovery_coord_2.recover_crashed_contracts(now=4120.0)

    assert len(recovered_list) == 1
    recovered = recovered_list[0]
    assert recovered.phase == ContractPhase.RESTORED_AND_VERIFIED
    assert recovered.is_fully_restored() is True

    # Verify O(1) idempotent resume: already rebuilt indexes were NOT rebuilt a second time
    resumed_sqls = pg_backend.executed_sql[sql_count_before_resume:]
    assert not any("CREATE INDEX CONCURRENTLY ix_orders_customer" in s for s in resumed_sqls)
    assert any("VALIDATE CONSTRAINT" in s for s in resumed_sqls)
    assert any("ANALYZE" in s for s in resumed_sqls)

    # And now production is 100% restored and passes AppReadinessGate
    verdict = AppReadinessGate.publish_app_ready(pg_backend, [recovered])
    assert verdict.app_ready is True
    assert verdict.care_badge == CareBadge.PASS


# -----------------------------------------------------------------------------
# Test 8: Constraint Violation on Restore Blocks Readiness Explicitly
# -----------------------------------------------------------------------------

def test_constraint_violation_during_restore_blocks_readiness(
    pg_backend: TargetTuningBackend, tmp_path: Path
):
    """Verify orphan rows violating deferred FK constraints cause restoration and readiness to fail."""
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend)
    coord.begin_contract(
        contract_id="c-orphan",
        proposed_settings={"synchronous_commit": ("off", SettingScope.SESSION, None)},
        total_rows=1_000_000,
        total_bytes=500 * 1024 * 1024,
        now=5000.0,
    )

    # Simulate an orphan row loaded while FK constraint was deferred
    pg_backend.constraint_violations["public.order_items.fk_items_order"] = (
        "insert or update on table order_items violates foreign key constraint fk_items_order"
    )

    with pytest.raises(RestoreVerificationError, match="fk_items_order failed post-load validation"):
        coord.restore_contract("c-orphan", now=5020.0)

    failed_contract = coord.load_contract("c-orphan")
    assert failed_contract.phase == ContractPhase.FAILED_RESTORE
    with pytest.raises(AppReadinessBlockedError, match="unvalidated_constraint"):
        AppReadinessGate.publish_app_ready(pg_backend, [failed_contract])


# -----------------------------------------------------------------------------
# Test 9: Typed O(1) Checkpoint Encoding & Safe Test Host Validation
# -----------------------------------------------------------------------------

def test_typed_checkpoint_roundtrip_and_safe_host_validation():
    """Verify native type preservation (datetime, date, Decimal, UUID, tuple, set) and host checks."""
    now_dt = datetime.datetime(2026, 10, 17, 12, 30, 45, tzinfo=datetime.timezone.utc)
    today_d = datetime.date(2026, 10, 17)
    dec_val = decimal.Decimal("987654321.012345")
    uid_val = uuid.UUID("abcdef01-2345-6789-abcd-ef0123456789")
    composite_cursor = (now_dt, dec_val, uid_val, today_d)

    ckpt = DurableRestoreCheckpoint(contract_id="c-types")
    ckpt.mark_range_done("r-comp", composite_cursor, now=6000.0)
    ckpt.mark_restore_step_done("index:public.ix_orders_customer", now=6001.0)

    restored_ckpt = DurableRestoreCheckpoint.from_dict(ckpt.to_dict())
    assert restored_ckpt.is_range_done("r-comp") is True
    assert restored_ckpt.is_restore_step_done("index:public.ix_orders_customer") is True
    assert restored_ckpt.range_cursors["r-comp"] == composite_cursor
    assert isinstance(restored_ckpt.range_cursors["r-comp"][0], datetime.datetime)
    assert isinstance(restored_ckpt.range_cursors["r-comp"][1], decimal.Decimal)
    assert isinstance(restored_ckpt.range_cursors["r-comp"][2], uuid.UUID)
    assert isinstance(restored_ckpt.range_cursors["r-comp"][3], datetime.date)

    # Safe host checks
    assert is_safe_test_host("127.0.0.1") is True
    assert is_safe_test_host("10.0.0.1") is True
    assert is_safe_test_host("10.0.0.254") is True
    assert is_safe_test_host("203.0.113.50") is False
    assert is_safe_test_host("prod-db.internal.example.com") is False

    with pytest.raises(UnsafeTargetHostError, match="outside approved safe test ranges"):
        TargetTuningBackend(engine="postgres", database="app", host="203.0.113.50")


# -----------------------------------------------------------------------------
# Test 10: Table-Scoped Autovacuum Pause & Session-Only Parameter Contract Rung
# -----------------------------------------------------------------------------

def test_table_scoped_autovacuum_pause_and_session_parameter_rung(
    pg_backend: TargetTuningBackend, tmp_path: Path
):
    """Verify table-scoped autovacuum pause restores cleanly and session-only rung keeps indexes."""
    policy = DurabilityBoundsPolicy(
        allow_deferred_indexes=False,
        allow_constraint_deferral=False,
        allow_autovacuum_pause=True,
    )
    coord = TargetTuningCoordinator(state_dir=tmp_path, backend=pg_backend, policy=policy)

    contract = coord.begin_contract(
        contract_id="c-tbl-autovac",
        proposed_settings={
            "synchronous_commit": ("off", SettingScope.SESSION, None),
            "autovacuum_enabled": ("off", SettingScope.TABLE, "public.orders"),
        },
        total_rows=1_000_000,
        total_bytes=500 * 1024 * 1024,
        defer_indexes=False,
        defer_constraints=False,
        defer_triggers=False,
        now=7000.0,
    )

    assert contract.justification.selected_rung == "session_and_table_contract"
    assert pg_backend.live_settings["autovacuum_enabled"] == "off"
    assert pg_backend.live_indexes["public.ix_orders_customer"]["present"] is True

    restored = coord.restore_contract("c-tbl-autovac", measured_ingest_seconds=6.0, now=7020.0)
    assert restored.is_fully_restored() is True
    assert pg_backend.live_settings["autovacuum_enabled"] == "on"
    assert any(
        "ALTER TABLE public.orders SET (autovacuum_enabled = on);" in s
        for s in pg_backend.executed_sql
    )

    # Now test pure SESSION parameter tuning with no table-scoped settings and no deferred indexes
    session_contract = coord.begin_contract(
        contract_id="c-sess-only",
        proposed_settings={
            "synchronous_commit": ("off", SettingScope.SESSION, None),
        },
        total_rows=1_000_000,
        total_bytes=500 * 1024 * 1024,
        defer_indexes=False,
        defer_constraints=False,
        defer_triggers=False,
        now=7100.0,
    )
    assert session_contract.justification.selected_rung == "session_parameter_contract"
    coord.restore_contract("c-sess-only", measured_ingest_seconds=6.0, now=7120.0)


# -----------------------------------------------------------------------------
# Test 11: Lease Heartbeat Renewal & Stale Lease Reclamation
# -----------------------------------------------------------------------------

def test_lease_heartbeat_renewal_and_stale_lease_reclamation(tmp_path: Path):
    """Verify heartbeat extends lease and stale leases (>stale_heartbeat_seconds) can be reclaimed."""
    lease1 = TuningContractLease(
        state_dir=tmp_path,
        contract_id="c-hb-1",
        lease_duration_seconds=60.0,
        stale_heartbeat_seconds=30.0,
        host="10.0.0.10",
        pid=1001,
    )
    lease1.acquire(now=8000.0)
    assert lease1.expires_at == 8060.0

    # Renew heartbeat at t=8020 with extension
    lease1.heartbeat(now=8020.0, extend_On_heartbeat=True)
    assert lease1.heartbeat_at == 8020.0
    assert lease1.expires_at == 8080.0

    # At t=8055 (35s since last heartbeat > 30s stale threshold), a recovery worker can steal it
    lease2 = TuningContractLease(
        state_dir=tmp_path,
        contract_id="c-hb-2",
        lease_duration_seconds=60.0,
        stale_heartbeat_seconds=30.0,
        host="10.0.0.11",
        pid=1002,
    )
    lease2.acquire(now=8055.0, steal_stale=True)
    assert lease2.issued_at == 8055.0

    # Now lease1's verify must fail because lease2 reclaimed the lock
    with pytest.raises(ContractLeaseError, match="was stolen by 10.0.0.11:1002"):
        lease1.verify(now=8056.0)

    lease2.release()


# -----------------------------------------------------------------------------
# Test 12: Measured Completion Evidence & Pure ASCII Verification
# -----------------------------------------------------------------------------

def test_measured_completion_evidence_and_pure_ascii():
    """Verify MeasuredCompletionEvidence flags slow actual runs and module/test are 100% pure ASCII."""
    fast_ev = MeasuredCompletionEvidence(
        baseline_estimated_seconds=20.0,
        measured_ingest_seconds=5.0,
        measured_index_rebuild_seconds=4.0,
        measured_constraint_validation_seconds=2.0,
        measured_analyze_seconds=1.0,
        measured_restore_seconds=0.2,
    )
    assert fast_ev.justified_by_measured_completion(min_ratio=1.2) is True

    slow_ev = MeasuredCompletionEvidence(
        baseline_estimated_seconds=20.0,
        measured_ingest_seconds=12.0,
        measured_index_rebuild_seconds=7.0,
        measured_constraint_validation_seconds=3.0,
        measured_analyze_seconds=1.5,
        measured_restore_seconds=0.5,
    )
    assert slow_ev.justified_by_measured_completion(min_ratio=1.0) is False

    # Verify both target_tuning_contract.py and this test file are 100% pure ASCII
    root = Path(__file__).resolve().parents[1]
    mod_bytes = (root / "migkit" / "target_tuning_contract.py").read_bytes()
    test_bytes = Path(__file__).resolve().read_bytes()
    assert all(b < 128 for b in mod_bytes), "non-ASCII byte in target_tuning_contract.py"
    assert all(b < 128 for b in test_bytes), "non-ASCII byte in test_target_tuning_durability_contract.py"


def test_tuning_contract_rungs_watched():
    """Verify every rung in select_tuning_contract_way is held and refused under watched()."""
    cost_model = TailAndRecoveryCostModel()
    policy = DurabilityBoundsPolicy()

    with tests.rung_branches.watched() as seen:
        # 1. Holds session_and_table_contract and session_parameter_contract
        select_tuning_contract_way(
            policy=policy,
            cost_model=cost_model,
            total_rows=10_000_000,
            total_bytes=10 * 1024 * 1024 * 1024,
            secondary_index_count=2,
            fk_constraint_count=1,
            crash_replayable=True,
            requested_scope=SettingScope.SESSION,
        )
        # 2. Refuses both tuned rungs when not crash-replayable -> untuned_baseline holds
        select_tuning_contract_way(
            policy=policy,
            cost_model=cost_model,
            total_rows=10,
            total_bytes=1024,
            secondary_index_count=0,
            fk_constraint_count=0,
            crash_replayable=False,
            requested_scope=SettingScope.SESSION,
        )

    assert set(tests.rung_branches.branch(seen, "session_and_table_contract")) == {"holds", "refuses"}
    assert set(tests.rung_branches.branch(seen, "session_parameter_contract")) == {"holds", "refuses"}
    assert "holds" in tests.rung_branches.branch(seen, "untuned_baseline")


