"""Tests for differentiating provider-native and generic migration paths (Row 609: RCH01.provider-path).

Proves:
1. Every decide.Rung in `provider_path.provider_path_rungs` and
   `provider_path.secondary_validation_rungs` is exercised on both `holds` and
   `refuses` inside `with tests.rung_branches.watched():` and declared in
   `PROVES_RUNGS`.
2. Both provider-native and generic paths are proved for a source/target pair
   (AWS RDS/Aurora PostgreSQL homogeneous vs classic/generic; Azure SQL Managed
   Instance Log Replay Service vs logical copy).
3. Native speed never earns unsupported coverage: even when a provider-native
   path is 10x faster, omitting required views, sequences, grants, routines, or
   triggers without independent supplementation causes `decide.climb` to refuse
   the native path and select a full-coverage path.
4. Injecting an omitted view, lagging/omitted sequence, or omitted grant into
   target catalog causes independent validation and completion evaluation to
   return `ACTION` (`completed=False`), and supplementing + re-verifying reaches
   `PASS`.
5. Provider resource lifecycle transitions and ephemeral resource teardown are
   enforced before certifying `PASS`.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import pathlib
import uuid
from typing import Any, Dict

import pytest

import tests.rung_branches
from migkit import decide, lease, provider_path
from migkit.provider_path import (
     ALL_SECONDARY_KINDS,
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    CareBadge,
    GrantSpec,
    MigrationPathKind,
    PathCompletionVerdict,
    PathCoverageContract,
    ProviderPathCheckpointStore,
    ProviderPathCheckpointUnit,
    ProviderPathError,
    ProviderResourceKind,
    ProviderResourceLifecycle,
    ProviderResourceLifecycleError,
    ProviderResourceState,
    RoutineSpec,
    SecondaryObjectCatalog,
    SecondaryObjectKind,
    SecondaryValidationStrategy,
    SequenceSpec,
    TriggerSpec,
    UnsafeEndpointError,
    ViewSpec,
    acquire_provider_path_lease,
    build_path_coverage_contract,
    choose_migration_path,
    evaluate_path_completion,
    provider_path_rungs,
    secondary_validation_rungs,
    supplement_missing_secondary_objects,
    validate_safe_endpoint,
    verify_secondary_objects,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("provider_path", "provider_path_rungs", "native_backup_log_restore", "holds"),
    ("provider_path", "provider_path_rungs", "native_backup_log_restore", "refuses"),
    ("provider_path", "provider_path_rungs", "provider_logical_replication", "holds"),
    ("provider_path", "provider_path_rungs", "provider_logical_replication", "refuses"),
    ("provider_path", "provider_path_rungs", "managed_dump_restore", "holds"),
    ("provider_path", "provider_path_rungs", "managed_dump_restore", "refuses"),
    ("provider_path", "provider_path_rungs", "generic_cdc_pipeline", "holds"),
    ("provider_path", "provider_path_rungs", "generic_cdc_pipeline", "refuses"),
    ("provider_path", "secondary_validation_rungs", "independent_catalog_reconcile_and_verify", "holds"),
    ("provider_path", "secondary_validation_rungs", "independent_catalog_reconcile_and_verify", "refuses"),
    ("provider_path", "secondary_validation_rungs", "independent_catalog_verify_only", "holds"),
    ("provider_path", "secondary_validation_rungs", "independent_catalog_verify_only", "refuses"),
    ("provider_path", "secondary_validation_rungs", "refuse_unverified_coverage_gap", "holds"),
    ("provider_path", "secondary_validation_rungs", "refuse_unverified_coverage_gap", "refuses"),
]


# -----------------------------------------------------------------------------
# Fixtures & Sample Catalogs
# -----------------------------------------------------------------------------

def _sample_source_catalog() -> SecondaryObjectCatalog:
    return SecondaryObjectCatalog(
        views={
            "active_orders_v": ViewSpec(
                name="active_orders_v",
                definition_sql="SELECT id, customer_id, total_cents FROM orders WHERE status = 'ACTIVE'",
                columns=("id", "customer_id", "total_cents"),
            ),
        },
        sequences={
            "orders_id_seq": SequenceSpec(
                name="orders_id_seq",
                last_value=50420,
                increment_by=1,
                min_value=1,
            ),
        },
        grants=(
            GrantSpec(
                principal="app_rw",
                object_type="table",
                object_name="orders",
                privileges=("SELECT", "INSERT", "UPDATE"),
                with_grant_option=False,
            ),
            GrantSpec(
                principal="app_rw",
                object_type="sequence",
                object_name="orders_id_seq",
                privileges=("USAGE", "SELECT"),
                with_grant_option=False,
            ),
        ),
        routines={
            "calc_order_tax": RoutineSpec(
                name="calc_order_tax",
                kind="function",
                signature="(amount_cents bigint) RETURNS bigint",
                body_sql="BEGIN RETURN (amount_cents * 7) / 100; END",
            ),
        },
        triggers={
            "orders_audit_trg": TriggerSpec(
                name="orders_audit_trg",
                table_name="orders",
                timing="AFTER",
                events=("INSERT", "UPDATE"),
                action_sql="EXECUTE FUNCTION record_order_audit()",
                enabled=True,
            ),
        },
    )


def _base_pair_facts() -> Dict[str, Any]:
    return {
        "source_host": "10.0.0.10",
        "target_host": "10.0.0.11",
        "provider": "aws",
        "source_engine": "postgres",
        "target_engine": "postgres",
        "source_major_version": 16,
        "target_major_version": 16,
        "min_dump_target_version": 14,
        "version_compatible_for_physical": True,
        "provider_backup_log_api": True,
        "storage_or_network_path_ready": True,
        "provider_restore_privilege": True,
        "requires_selective_table_or_row_transform": False,
        "logical_replication_engine_compatible": True,
        "version_supports_logical_replication": True,
        "network_replication_reachable": True,
        "replication_privilege_and_wal": True,
        "requires_custom_column_masking_or_reshape": False,
        "dump_restore_engine_compatible": True,
        "client_network_reachable": True,
        "dump_restore_privileges": True,
        "snapshot_window_permits_dump": True,
        "requires_continuous_cdc_stream": True,
        "generic_cdc_privileges": True,
        "generic_covers_required_objects": True,
        "catalog_introspection_available": True,
    }


# -----------------------------------------------------------------------------
# 1. Rung Branch Coverage Under `tests.rung_branches.watched()`
# -----------------------------------------------------------------------------

PATH_RUNG_CASES = [
    (
        "native_backup_log_restore",
        {
            "source_engine": "mssql",
            "target_engine": "mssql",
            "provider_backup_log_api": True,
            "version_compatible_for_physical": True,
            "source_major_version": 16,
            "target_major_version": 16,
            "storage_or_network_path_ready": True,
            "provider_restore_privilege": True,
            "requires_selective_table_or_row_transform": False,
            "native_backup_covers_required_objects": True,
        },
        {
            "source_engine": "mssql",
            "target_engine": "mssql",
            "provider_backup_log_api": True,
            "version_compatible_for_physical": True,
            "source_major_version": 16,
            "target_major_version": 16,
            "storage_or_network_path_ready": True,
            "provider_restore_privilege": True,
            "requires_selective_table_or_row_transform": False,
            "native_backup_covers_required_objects": False,
            "independent_secondary_supplement_available": False,
        },
        "omits required secondary objects",
    ),
    (
        "provider_logical_replication",
        {
            "logical_replication_engine_compatible": True,
            "version_supports_logical_replication": True,
            "network_replication_reachable": True,
            "replication_privilege_and_wal": True,
            "requires_custom_column_masking_or_reshape": False,
            "logical_replication_covers_required_objects": False,
            "independent_secondary_supplement_available": True,
        },
        {
            "logical_replication_engine_compatible": True,
            "version_supports_logical_replication": True,
            "network_replication_reachable": True,
            "replication_privilege_and_wal": True,
            "requires_custom_column_masking_or_reshape": False,
            "logical_replication_covers_required_objects": False,
            "independent_secondary_supplement_available": False,
        },
        "omits views, sequences, grants, routines, or triggers without independent supplement",
    ),
    (
        "managed_dump_restore",
        {
            "dump_restore_engine_compatible": True,
            "target_major_version": 16,
            "min_dump_target_version": 14,
            "client_network_reachable": True,
            "dump_restore_privileges": True,
            "snapshot_window_permits_dump": True,
            "requires_continuous_cdc_stream": False,
        },
        {
            "dump_restore_engine_compatible": True,
            "target_major_version": 16,
            "min_dump_target_version": 14,
            "client_network_reachable": True,
            "dump_restore_privileges": True,
            "snapshot_window_permits_dump": True,
            "requires_continuous_cdc_stream": True,
        },
        "does not stream ongoing writes",
    ),
    (
        "generic_cdc_pipeline",
        {
            "client_network_reachable": True,
            "generic_cdc_privileges": True,
            "generic_covers_required_objects": True,
        },
        {
            "client_network_reachable": True,
            "generic_cdc_privileges": False,
            "generic_covers_required_objects": True,
        },
        "lacks source table/change read or target write privileges",
    ),
]

VALIDATION_RUNG_CASES = [
    (
        "independent_catalog_reconcile_and_verify",
        {
            "path_has_coverage_or_verification_gaps": True,
            "catalog_introspection_available": True,
            "secondary_ddl_apply_available": True,
        },
        {
            "path_has_coverage_or_verification_gaps": False,
            "catalog_introspection_available": True,
            "secondary_ddl_apply_available": True,
        },
        "only needed when the path omits secondary objects",
    ),
    (
        "independent_catalog_verify_only",
        {
            "path_has_unsupplemented_coverage_gaps": False,
            "catalog_introspection_available": True,
        },
        {
            "path_has_unsupplemented_coverage_gaps": True,
            "catalog_introspection_available": True,
        },
        "cannot verify alone when required secondary objects were never carried",
    ),
    (
        "refuse_unverified_coverage_gap",
        {
            "must_block_unverified_gap": True,
        },
        {
            "must_block_unverified_gap": False,
        },
        "only taken when secondary coverage cannot be supplemented or verified",
    ),
]


def test_proves_rungs_matches_all_ladders() -> None:
    expected = set()
    for rung_name, _, _, _ in PATH_RUNG_CASES:
        expected.add(("provider_path", "provider_path_rungs", rung_name, HOLDS))
        expected.add(("provider_path", "provider_path_rungs", rung_name, REFUSES))
    for rung_name, _, _, _ in VALIDATION_RUNG_CASES:
        expected.add(("provider_path", "secondary_validation_rungs", rung_name, HOLDS))
        expected.add(("provider_path", "secondary_validation_rungs", rung_name, REFUSES))
    assert set(PROVES_RUNGS) == expected


@pytest.mark.parametrize("rung_name,ok_facts,bad_facts,expected_why", PATH_RUNG_CASES)
def test_provider_path_rungs_hold_and_refuse_under_watched(
    rung_name: str,
    ok_facts: Dict[str, Any],
    bad_facts: Dict[str, Any],
    expected_why: str,
) -> None:
    rungs = {r.name: r for r in provider_path_rungs()}
    target_rung = rungs[rung_name]

    with tests.rung_branches.watched() as seen:
        choice_ok = decide.climb("test path rung holds", [target_rung], decide.Facts(ok_facts))
        assert choice_ok.rung is target_rung
        choice_bad = decide.climb("test path rung refuses", [target_rung], decide.Facts(bad_facts))
        assert choice_bad.rung is None

    assert branch(seen, rung_name) == [HOLDS, REFUSES]
    assert expected_why in (why(seen, rung_name) or "")


@pytest.mark.parametrize("rung_name,ok_facts,bad_facts,expected_why", VALIDATION_RUNG_CASES)
def test_secondary_validation_rungs_hold_and_refuse_under_watched(
    rung_name: str,
    ok_facts: Dict[str, Any],
    bad_facts: Dict[str, Any],
    expected_why: str,
) -> None:
    rungs = {r.name: r for r in secondary_validation_rungs()}
    target_rung = rungs[rung_name]

    with tests.rung_branches.watched() as seen:
        choice_ok = decide.climb(
            "test validation rung holds", [target_rung], decide.Facts(ok_facts)
        )
        assert choice_ok.rung is target_rung
        choice_bad = decide.climb(
            "test validation rung refuses", [target_rung], decide.Facts(bad_facts)
        )
        assert choice_bad.rung is None

    assert branch(seen, rung_name) == [HOLDS, REFUSES]
    assert expected_why in (why(seen, rung_name) or "")


# -----------------------------------------------------------------------------
# 2. Prove Both Native and Generic Paths + Native Speed Never Earns Unsupported Coverage
# -----------------------------------------------------------------------------

def test_prove_native_and_generic_paths_and_speed_never_earns_unsupported_coverage() -> None:
    """Prove both native and generic paths for the same source/target pair.

    Even when native paths are measured 10x faster than generic CDC, if the workload
    requires views, sequences, grants, routines, or triggers that the native path omits
    and `independent_secondary_supplement_available=False`, `choose_migration_path`
    refuses the native path and chooses `generic_cdc_pipeline`.
    """
    base = _base_pair_facts()
    # Measured throughput: native backup is 500 MB/s, logical rep is 250 MB/s, generic CDC is 50 MB/s
    base.update(
        {
            "dataset_bytes": 10 * 1024 * 1024 * 1024,  # 10 GiB
            "measured_native_backup_bytes_per_sec": 500 * 1024 * 1024,
            "measured_logical_rep_bytes_per_sec": 250 * 1024 * 1024,
            "measured_generic_cdc_bytes_per_sec": 50 * 1024 * 1024,
        }
    )

    # Case 1: Azure SQL MI / AWS RDS homogeneous full-DB backup restore with DB-scoped objects
    # -> Native backup/log restore holds and wins on speed (20.48s vs 204.8s)
    with watched() as seen_native:
        res_native = choose_migration_path(
            base,
            required_secondary=ALL_SECONDARY_KINDS,
        )
    assert res_native.chosen_path == MigrationPathKind.NATIVE_BACKUP_LOG_RESTORE
    assert res_native.validation_strategy == SecondaryValidationStrategy.INDEPENDENT_VERIFY_ONLY
    assert res_native.care_badge == CareBadge.PASS
    assert HOLDS in branch(seen_native, "native_backup_log_restore")

    # Case 2: Backup restore API unavailable (e.g., cross-VPC/on-prem to RDS), logical replication
    # is 5x faster than generic CDC (250 MB/s vs 50 MB/s), and workload requires views + sequences + grants.
    # Without independent secondary supplement, provider_logical_replication MUST be refused despite speed!
    no_backup_no_supp = dict(
        base,
        provider_backup_log_api=False,
        independent_secondary_supplement_available=False,
    )
    with watched() as seen_refuse_fast:
        res_generic = choose_migration_path(
            no_backup_no_supp,
            required_secondary=(
                SecondaryObjectKind.VIEW,
                SecondaryObjectKind.SEQUENCE,
                SecondaryObjectKind.GRANT,
            ),
        )
    assert res_generic.chosen_path == MigrationPathKind.GENERIC_CDC_PIPELINE
    assert REFUSES in branch(seen_refuse_fast, "provider_logical_replication")
    assert "omits views, sequences, grants" in (
        why(seen_refuse_fast, "provider_logical_replication") or ""
    )
    assert res_generic.care_badge == CareBadge.PASS

    # Case 3: Enable independent secondary-object supplement + verification on the same pair
    # -> provider_logical_replication now holds and attaches INDEPENDENT_SUPPLEMENT_AND_VERIFY!
    with_supp = dict(
        no_backup_no_supp,
        independent_secondary_supplement_available=True,
    )
    with watched() as seen_logical_supp:
        res_logical = choose_migration_path(
            with_supp,
            required_secondary=ALL_SECONDARY_KINDS,
        )
    assert res_logical.chosen_path == MigrationPathKind.PROVIDER_LOGICAL_REPLICATION
    assert (
        res_logical.validation_strategy
        == SecondaryValidationStrategy.INDEPENDENT_SUPPLEMENT_AND_VERIFY
    )
    assert res_logical.care_badge == CareBadge.REVIEW
    assert HOLDS in branch(seen_logical_supp, "provider_logical_replication")

    # Case 4: Row filter or column remapping required -> native backup and logical replication refused,
    # falls back to generic_cdc_pipeline
    filtered_facts = dict(
        base,
        requires_selective_table_or_row_transform=True,
        requires_custom_column_masking_or_reshape=True,
    )
    with watched() as seen_filtered:
        res_filtered = choose_migration_path(filtered_facts, required_secondary=ALL_SECONDARY_KINDS)
    assert res_filtered.chosen_path == MigrationPathKind.GENERIC_CDC_PIPELINE
    assert REFUSES in branch(seen_filtered, "native_backup_log_restore")
    assert REFUSES in branch(seen_filtered, "provider_logical_replication")


# -----------------------------------------------------------------------------
# 3. Inject Omitted View, Sequence, and Grant + Verify Independent Detection & Remediation
# -----------------------------------------------------------------------------

def test_inject_omitted_view_sequence_grant_blocks_completion_until_supplemented() -> None:
    """Inject omitted view, lagging/omitted sequence, and omitted grant on a native path."""
    src_cat = _sample_source_catalog()

    # Simulate provider logical replication target where tables arrived at LSN 0/9000,
    # but active_orders_v is omitted, orders_id_seq lags at 1 (instead of 50420),
    # and app_rw sequence USAGE grant is omitted.
    imperfect_tgt_cat = SecondaryObjectCatalog(
        views={},  # Omitted view!
        sequences={
            "orders_id_seq": SequenceSpec(
                name="orders_id_seq",
                last_value=1,  # Lagging sequence!
                increment_by=1,
                min_value=1,
            ),
        },
        grants=(
            GrantSpec(
                principal="app_rw",
                object_type="table",
                object_name="orders",
                privileges=("SELECT", "INSERT", "UPDATE"),
                with_grant_option=False,
            ),
            # Omitted sequence USAGE grant!
        ),
        routines=dict(src_cat.routines),
        triggers=dict(src_cat.triggers),
    )

    sec_report = verify_secondary_objects(src_cat, imperfect_tgt_cat)
    assert sec_report.verified is False
    assert sec_report.care_badge == CareBadge.ACTION
    defect_kinds = {(d.kind, d.defect_type) for d in sec_report.defects}
    assert (SecondaryObjectKind.VIEW, "omitted") in defect_kinds
    assert (SecondaryObjectKind.SEQUENCE, "sequence_lag") in defect_kinds
    assert (SecondaryObjectKind.GRANT, "omitted") in defect_kinds

    # Even if the provider reports replication task status = COMPLETE and LSN fence is reached,
    # evaluate_path_completion MUST reject completion with CareBadge.ACTION!
    facts = dict(
        _base_pair_facts(),
        provider_backup_log_api=False,
        independent_secondary_supplement_available=True,
    )
    selection = choose_migration_path(facts, required_secondary=ALL_SECONDARY_KINDS)
    assert selection.chosen_path == MigrationPathKind.PROVIDER_LOGICAL_REPLICATION

    lifecycle = ProviderResourceLifecycle()
    lifecycle.register(
        "dms-task-orders-01",
        ProviderResourceKind.REPLICATION_TASK,
        "aws",
        ephemeral=True,
    )
    lifecycle.transition("dms-task-orders-01", ProviderResourceState.ACTIVE)
    lifecycle.transition(
        "dms-task-orders-01",
        ProviderResourceState.COMPLETED,
        fence_position="0/9000",
    )

    verdict_blocked = evaluate_path_completion(
        path_selection=selection,
        provider_reported_complete=True,
        source_fence_position="0/9000",
        target_replayed_position="0/9000",
        primary_data_verified=True,
        source_catalog=src_cat,
        target_catalog=imperfect_tgt_cat,
        lifecycle=lifecycle,
    )
    assert verdict_blocked.completed is False
    assert verdict_blocked.care_badge == CareBadge.ACTION
    assert "secondary-object defect" in verdict_blocked.reason

    # Now supplement missing/lagging secondary objects with measured headroom and re-evaluate
    remediated_tgt_cat = supplement_missing_secondary_objects(
        src_cat,
        imperfect_tgt_cat,
        sequence_headroom=100,
    )
    assert remediated_tgt_cat.sequences["orders_id_seq"].last_value == 50520

    # Before ephemeral DMS task teardown, completion is REVIEW (data & secondary verified, resource lingering)
    verdict_review = evaluate_path_completion(
        path_selection=selection,
        provider_reported_complete=True,
        source_fence_position="0/9000",
        target_replayed_position="0/9000",
        primary_data_verified=True,
        source_catalog=src_cat,
        target_catalog=remediated_tgt_cat,
        lifecycle=lifecycle,
    )
    assert verdict_review.completed is True
    assert verdict_review.care_badge == CareBadge.REVIEW
    assert verdict_review.uncleaned_ephemeral_resources == ("dms-task-orders-01",)

    # Clean up ephemeral provider resources -> verdict reaches PASS
    cleaned = lifecycle.cleanup_all_ephemeral()
    assert cleaned == ("dms-task-orders-01",)

    verdict_pass = evaluate_path_completion(
        path_selection=selection,
        provider_reported_complete=True,
        source_fence_position="0/9000",
        target_replayed_position="0/9000",
        primary_data_verified=True,
        source_catalog=src_cat,
        target_catalog=remediated_tgt_cat,
        lifecycle=lifecycle,
    )
    assert verdict_pass.completed is True
    assert verdict_pass.care_badge == CareBadge.PASS
    assert verdict_pass.secondary_validation.verified is True
    assert verdict_pass.uncleaned_ephemeral_resources == ()


def test_routine_trigger_and_grant_option_defects_detected() -> None:
    src_cat = _sample_source_catalog()
    # Modify routine body, disable trigger, and drop WITH GRANT OPTION
    src_with_grant_opt = dataclasses.replace(
        src_cat,
        grants=(
            GrantSpec(
                principal="app_admin",
                object_type="table",
                object_name="orders",
                privileges=("SELECT", "INSERT"),
                with_grant_option=True,
            ),
        ),
    )
    bad_tgt = SecondaryObjectCatalog(
        views=dict(src_cat.views),
        sequences=dict(src_cat.sequences),
        grants=(
            GrantSpec(
                principal="app_admin",
                object_type="table",
                object_name="orders",
                privileges=("SELECT", "INSERT"),
                with_grant_option=False,
            ),
        ),
        routines={
            "calc_order_tax": RoutineSpec(
                name="calc_order_tax",
                kind="function",
                signature="(amount_cents bigint) RETURNS bigint",
                body_sql="BEGIN RETURN 0; END",  # Corrupted body
            ),
        },
        triggers={
            "orders_audit_trg": dataclasses.replace(
                src_cat.triggers["orders_audit_trg"],
                enabled=False,  # Disabled on target
            ),
        },
    )
    report = verify_secondary_objects(src_with_grant_opt, bad_tgt)
    assert report.verified is False
    assert report.care_badge == CareBadge.ACTION
    kinds_and_types = {(d.kind, d.defect_type) for d in report.defects}
    assert (SecondaryObjectKind.GRANT, "grant_deficit") in kinds_and_types
    assert (SecondaryObjectKind.ROUTINE, "definition_mismatch") in kinds_and_types
    assert (SecondaryObjectKind.TRIGGER, "disabled") in kinds_and_types


# -----------------------------------------------------------------------------
# 4. Lifecycle Guardrails, Checkpoint O(1) Typed Resume, and Lease Lock
# -----------------------------------------------------------------------------

def test_provider_resource_lifecycle_rejects_invalid_transitions() -> None:
    lc = ProviderResourceLifecycle()
    lc.register("lrs-job-1", ProviderResourceKind.LOG_REPLAY_SERVICE, "azure")
    with pytest.raises(ProviderResourceLifecycleError, match="duplicate"):
        lc.register("lrs-job-1", ProviderResourceKind.LOG_REPLAY_SERVICE, "azure")
    with pytest.raises(ProviderResourceLifecycleError, match="invalid transition"):
        lc.transition("lrs-job-1", ProviderResourceState.CLEANED_UP)


def test_o1_typed_checkpoint_and_mutual_exclusion_lease(tmp_path: pathlib.Path) -> None:
    cp_file = tmp_path / "provider_path_cp.json"
    store = ProviderPathCheckpointStore(cp_file)

    now = datetime.datetime(2026, 10, 17, 18, 45, 0, tzinfo=datetime.timezone.utc)
    lo_cursor = (uuid.UUID("11111111-2222-3333-4444-555555555555"), decimal.Decimal("10.50"))
    hi_cursor = (uuid.UUID("99999999-8888-7777-6666-555555555555"), decimal.Decimal("999.99"))

    unit = ProviderPathCheckpointUnit(
        unit_id="orders:chunk-001",
        path_kind=MigrationPathKind.GENERIC_CDC_PIPELINE,
        min_cursor=lo_cursor,
        max_cursor=hi_cursor,
        fence_position="0/9500",
        secondary_verified=True,
        completed_at=now,
    )
    store.record_unit(unit)
    assert store.is_completed_o1("orders:chunk-001") is True
    assert store.is_completed_o1("orders:chunk-002") is False

    reloaded = ProviderPathCheckpointStore(cp_file)
    restored = reloaded.get_unit("orders:chunk-001")
    assert restored is not None
    assert restored.min_cursor == lo_cursor
    assert isinstance(restored.min_cursor[0], uuid.UUID)
    assert isinstance(restored.min_cursor[1], decimal.Decimal)
    assert restored.completed_at == now

    # Verify mutual-exclusion lease via migkit.lease
    lock_path = tmp_path / ".run.lock"
    active_lease = acquire_provider_path_lease(lock_path, term_seconds=60.0)
    try:
        with pytest.raises(lease.Held):
            acquire_provider_path_lease(lock_path, term_seconds=60.0)
    finally:
        active_lease.release()


def test_endpoint_safety_validation() -> None:
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("10.0.0.25") == "10.0.0.25"
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("rds.amazonaws.com")
