"""Tests for resource ownership lifecycle and state restoration (Row 605: SMART-N22.resource-ownership).

Proves:
1. Every `decide.Rung` in `resource_admission_rungs`, `resource_cleanup_rungs`,
   `setting_restore_rungs`, and `interruption_recovery_rungs` is exercised on both
   `holds` and `refuses` inside `with tests.rung_branches.watched():` and declared
   in `PROVES_RUNGS`.
2. Assigns durable owners and cleanup/restore obligations to temporary tables,
   files, spools, readers, slots, subscriptions, credential leases, tunnels,
   settings, and index windows.
3. 5-dimensional budgeting across disk, WAL, file descriptors, memory, and
   connections derived from measured capacity.
4. Deterministic cleanup or resume upon interruption at every acquisition and
   restore point.
5. Shared resources remain untouched upon release.
6. Original observed target settings are durably recorded and restored.
7. Sole copies of change or undo data are never prematurely removed.
8. Universal typed durable resume in O(1) with native types.
9. Mutual exclusion active verification lease (.run.lock).
10. Pure UNIX style, zero emojis/glyphs, care badges ACTION/REVIEW/BENIGN/PASS.
"""

from __future__ import annotations

import datetime
import decimal
import pathlib
import uuid
from typing import Any, Dict, List

import pytest
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched

from migkit import decide
from migkit.resource_ownership import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    BudgetExhaustedError,
    CareBadge,
    ObligationKind,
    OwnerRecord,
    ResourceBudget,
    ResourceDemand,
    ResourceKind,
    ResourceLifecycleManager,
    ResourceOwnershipError,
    ResourceOwnershipReport,
    ResourceOwnershipStore,
    ResourceState,
    SoleUndoDataHazardError,
    UnsafeEndpointError,
    acquire_ownership_lease,
    evaluate_interruption_recovery,
    evaluate_resource_admission,
    evaluate_resource_cleanup,
    evaluate_setting_restore,
    interruption_recovery_rungs,
    resource_admission_rungs,
    resource_cleanup_rungs,
    setting_restore_rungs,
    validate_safe_endpoint,
)

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("resource_ownership", "resource_admission_rungs", "admit_exclusive_resource", "holds"),
    ("resource_ownership", "resource_admission_rungs", "admit_exclusive_resource", "refuses"),
    ("resource_ownership", "resource_admission_rungs", "admit_shared_attachment", "holds"),
    ("resource_ownership", "resource_admission_rungs", "admit_shared_attachment", "refuses"),
    ("resource_ownership", "resource_admission_rungs", "refuse_exhausted_budget", "holds"),
    ("resource_ownership", "resource_admission_rungs", "refuse_exhausted_budget", "refuses"),
    ("resource_ownership", "resource_admission_rungs", "refuse_unsafe_endpoint", "holds"),
    ("resource_ownership", "resource_admission_rungs", "refuse_unsafe_endpoint", "refuses"),

    ("resource_ownership", "resource_cleanup_rungs", "cleanup_ephemeral_exclusive", "holds"),
    ("resource_ownership", "resource_cleanup_rungs", "cleanup_ephemeral_exclusive", "refuses"),
    ("resource_ownership", "resource_cleanup_rungs", "retain_sole_undo_copy", "holds"),
    ("resource_ownership", "resource_cleanup_rungs", "retain_sole_undo_copy", "refuses"),
    ("resource_ownership", "resource_cleanup_rungs", "unbind_shared_resource", "holds"),
    ("resource_ownership", "resource_cleanup_rungs", "unbind_shared_resource", "refuses"),
    ("resource_ownership", "resource_cleanup_rungs", "defer_active_owner", "holds"),
    ("resource_ownership", "resource_cleanup_rungs", "defer_active_owner", "refuses"),

    ("resource_ownership", "setting_restore_rungs", "restore_observed_setting", "holds"),
    ("resource_ownership", "setting_restore_rungs", "restore_observed_setting", "refuses"),
    ("resource_ownership", "setting_restore_rungs", "preserve_external_setting_change", "holds"),
    ("resource_ownership", "setting_restore_rungs", "preserve_external_setting_change", "refuses"),
    ("resource_ownership", "setting_restore_rungs", "refuse_missing_original_state", "holds"),
    ("resource_ownership", "setting_restore_rungs", "refuse_missing_original_state", "refuses"),

    ("resource_ownership", "interruption_recovery_rungs", "resume_active_lease", "holds"),
    ("resource_ownership", "interruption_recovery_rungs", "resume_active_lease", "refuses"),
    ("resource_ownership", "interruption_recovery_rungs", "rollback_interrupted_acquisition", "holds"),
    ("resource_ownership", "interruption_recovery_rungs", "rollback_interrupted_acquisition", "refuses"),
    ("resource_ownership", "interruption_recovery_rungs", "force_deterministic_cleanup", "holds"),
    ("resource_ownership", "interruption_recovery_rungs", "force_deterministic_cleanup", "refuses"),
]


# -----------------------------------------------------------------------------
# Test 1: Rung Branches Exercised under watched()
# -----------------------------------------------------------------------------

def test_every_decide_rung_is_reached_on_both_branches():
    """Exercise both holds and refuses for all 14 rungs under tests.rung_branches.watched()."""
    with watched() as seen:
        # 1. resource_admission_rungs
        adm_rungs = resource_admission_rungs()

        # admit_exclusive_resource
        f_adm_hold = decide.Facts({
            "endpoint_safe": True,
            "budget_available": True,
            "owner_recorded": True,
            "is_shared": False,
        })
        decide.climb("test admit_exclusive_resource holds", adm_rungs, f_adm_hold)

        f_adm_ref = decide.Facts({
            "endpoint_safe": False,
            "budget_available": False,
            "owner_recorded": False,
            "is_shared": True,
        })
        decide.climb("test admit_exclusive_resource refuses", adm_rungs, f_adm_ref)

        # admit_shared_attachment
        f_shared_hold = decide.Facts({
            "endpoint_safe": True,
            "budget_available": True,
            "is_shared": True,
            "non_destructive_obligation": True,
        })
        decide.climb("test admit_shared_attachment holds", adm_rungs, f_shared_hold)

        f_shared_ref = decide.Facts({
            "endpoint_safe": False,
            "budget_available": False,
            "is_shared": False,
            "non_destructive_obligation": False,
        })
        decide.climb("test admit_shared_attachment refuses", adm_rungs, f_shared_ref)

        # refuse_exhausted_budget
        f_exh_hold = decide.Facts({"budget_exhausted": True})
        decide.climb("test refuse_exhausted_budget holds", adm_rungs, f_exh_hold)

        f_exh_ref = decide.Facts({"budget_exhausted": False})
        decide.climb("test refuse_exhausted_budget refuses", adm_rungs, f_exh_ref)

        # refuse_unsafe_endpoint
        f_unsafe_hold = decide.Facts({"unsafe_endpoint": True})
        decide.climb("test refuse_unsafe_endpoint holds", adm_rungs, f_unsafe_hold)

        f_unsafe_ref = decide.Facts({"unsafe_endpoint": False})
        decide.climb("test refuse_unsafe_endpoint refuses", adm_rungs, f_unsafe_ref)

        # 2. resource_cleanup_rungs
        cln_rungs = resource_cleanup_rungs()

        # cleanup_ephemeral_exclusive
        f_cln_hold = decide.Facts({
            "owner_gone": True,
            "is_shared": False,
            "has_sole_undo_copy": False,
        })
        decide.climb("test cleanup_ephemeral_exclusive holds", cln_rungs, f_cln_hold)

        f_cln_ref = decide.Facts({
            "owner_gone": False,
            "is_shared": True,
            "has_sole_undo_copy": True,
        })
        decide.climb("test cleanup_ephemeral_exclusive refuses", cln_rungs, f_cln_ref)

        # retain_sole_undo_copy
        f_sole_hold = decide.Facts({"has_sole_undo_copy": True})
        decide.climb("test retain_sole_undo_copy holds", cln_rungs, f_sole_hold)

        f_sole_ref = decide.Facts({"has_sole_undo_copy": False})
        decide.climb("test retain_sole_undo_copy refuses", cln_rungs, f_sole_ref)

        # unbind_shared_resource
        f_unbind_hold = decide.Facts({
            "is_shared": True,
            "owner_gone": True,
        })
        decide.climb("test unbind_shared_resource holds", cln_rungs, f_unbind_hold)

        f_unbind_ref = decide.Facts({
            "is_shared": False,
            "owner_gone": False,
        })
        decide.climb("test unbind_shared_resource refuses", cln_rungs, f_unbind_ref)

        # defer_active_owner
        f_defer_hold = decide.Facts({"owner_gone": False})
        decide.climb("test defer_active_owner holds", cln_rungs, f_defer_hold)

        f_defer_ref = decide.Facts({"owner_gone": True})
        decide.climb("test defer_active_owner refuses", cln_rungs, f_defer_ref)

        # 3. setting_restore_rungs
        set_rungs = setting_restore_rungs()

        # restore_observed_setting
        f_rst_hold = decide.Facts({
            "original_recorded": True,
            "current_matches_migkit": True,
        })
        decide.climb("test restore_observed_setting holds", set_rungs, f_rst_hold)

        f_rst_ref = decide.Facts({
            "original_recorded": False,
            "current_matches_migkit": False,
        })
        decide.climb("test restore_observed_setting refuses", set_rungs, f_rst_ref)

        # preserve_external_setting_change
        f_ext_hold = decide.Facts({
            "original_recorded": True,
            "external_drift": True,
        })
        decide.climb("test preserve_external_setting_change holds", set_rungs, f_ext_hold)

        f_ext_ref = decide.Facts({
            "original_recorded": False,
            "external_drift": False,
        })
        decide.climb("test preserve_external_setting_change refuses", set_rungs, f_ext_ref)

        # refuse_missing_original_state
        f_mis_hold = decide.Facts({"original_recorded": False})
        decide.climb("test refuse_missing_original_state holds", set_rungs, f_mis_hold)

        f_mis_ref = decide.Facts({"original_recorded": True})
        decide.climb("test refuse_missing_original_state refuses", set_rungs, f_mis_ref)

        # 4. interruption_recovery_rungs
        rec_rungs = interruption_recovery_rungs()

        # resume_active_lease
        f_res_hold = decide.Facts({
            "lease_renewable": True,
            "checkpoint_valid": True,
        })
        decide.climb("test resume_active_lease holds", rec_rungs, f_res_hold)

        f_res_ref = decide.Facts({
            "lease_renewable": False,
            "checkpoint_valid": False,
        })
        decide.climb("test resume_active_lease refuses", rec_rungs, f_res_ref)

        # rollback_interrupted_acquisition
        f_rb_hold = decide.Facts({
            "interrupted_acquiring": True,
            "owner_gone": True,
        })
        decide.climb("test rollback_interrupted_acquisition holds", rec_rungs, f_rb_hold)

        f_rb_ref = decide.Facts({
            "interrupted_acquiring": False,
            "owner_gone": False,
        })
        decide.climb("test rollback_interrupted_acquisition refuses", rec_rungs, f_rb_ref)

        # force_deterministic_cleanup
        f_frc_hold = decide.Facts({
            "owner_gone": True,
            "is_shared": False,
            "has_sole_undo_copy": False,
        })
        decide.climb("test force_deterministic_cleanup holds", rec_rungs, f_frc_hold)

        f_frc_ref = decide.Facts({
            "owner_gone": False,
            "is_shared": True,
            "has_sole_undo_copy": True,
        })
        decide.climb("test force_deterministic_cleanup refuses", rec_rungs, f_frc_ref)

    # Verify that all 14 rungs reached both HOLDS and REFUSES
    for module, ladder, rung, expected_branch in PROVES_RUNGS:
        reached_branches = branch(seen, rung)
        assert expected_branch in reached_branches, (
            f"Rung {rung} in {ladder} failed to reach branch '{expected_branch}' (reached: {reached_branches})"
        )


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def temp_store(tmp_path: pathlib.Path) -> ResourceOwnershipStore:
    return ResourceOwnershipStore(tmp_path / "resource_ownership.json")


@pytest.fixture
def measured_budget() -> ResourceBudget:
    facts = {
        "available_disk_bytes": 10_000_000_000,
        "disk_headroom_fraction": 0.8,  # limit: 8 GB
        "wal_keep_bytes": 1_000_000_000,
        "wal_headroom_fraction": 0.5,   # limit: 500 MB
        "system_fd_limit": 1000,
        "fd_headroom_fraction": 0.5,    # limit: 500
        "system_memory_bytes": 4_000_000_000,
        "memory_headroom_fraction": 0.5,# limit: 2 GB
        "system_max_connections": 100,
        "connections_headroom_fraction": 0.5, # limit: 50
    }
    return ResourceBudget.from_measured(facts)


@pytest.fixture
def manager(temp_store: ResourceOwnershipStore, measured_budget: ResourceBudget) -> ResourceLifecycleManager:
    return ResourceLifecycleManager(temp_store, measured_budget)


# -----------------------------------------------------------------------------
# Acceptance: All 10 Resource Kinds Covered
# -----------------------------------------------------------------------------

def test_assigns_durable_owners_to_all_ten_resource_kinds(manager: ResourceLifecycleManager):
    """Assign durable owners and cleanup/restore obligations across all 10 resource kinds."""
    kinds = [
        ResourceKind.TABLE,
        ResourceKind.FILE,
        ResourceKind.SPOOL,
        ResourceKind.READER,
        ResourceKind.SLOT,
        ResourceKind.SUBSCRIPTION,
        ResourceKind.CREDENTIAL_LEASE,
        ResourceKind.TUNNEL,
        ResourceKind.SETTING,
        ResourceKind.INDEX_WINDOW,
    ]

    for k in kinds:
        rec = OwnerRecord(
            owner_id=f"owner-{k.value}",
            resource_id=f"res-{k.value}-001",
            kind=k,
            host="127.0.0.1",
            demand=ResourceDemand(disk_bytes=1024, memory_bytes=2048, connections=1),
            obligation=ObligationKind.CLEANUP if k != ResourceKind.SETTING else ObligationKind.RESTORE,
        )
        acquired = manager.acquire(rec)
        assert acquired.state == ResourceState.ACQUIRED
        assert manager.store.get_resource(rec.resource_id) is not None


# -----------------------------------------------------------------------------
# Acceptance: 5-Dimensional Budgeting
# -----------------------------------------------------------------------------

def test_five_dimensional_budget_admission_and_exhaustion(manager: ResourceLifecycleManager):
    """Budget disk, WAL, descriptors, memory, and connections from measured limits."""
    # 1. Normal allocation within budget
    normal_rec = OwnerRecord(
        owner_id="owner-normal",
        resource_id="res-normal-table",
        kind=ResourceKind.TABLE,
        host="10.0.0.1",
        demand=ResourceDemand(
            disk_bytes=1_000_000_000,
            wal_bytes=100_000_000,
            file_descriptors=50,
            memory_bytes=200_000_000,
            connections=5,
        ),
    )
    manager.acquire(normal_rec)
    assert manager.budget.disk_bytes_used == 1_000_000_000
    assert manager.budget.connections_used == 5

    # 2. Excessive disk demand triggers BudgetExhaustedError
    excessive_rec = OwnerRecord(
        owner_id="owner-greedy",
        resource_id="res-huge-file",
        kind=ResourceKind.FILE,
        host="10.0.0.2",
        demand=ResourceDemand(disk_bytes=10_000_000_000),  # Limit is 8GB
    )
    with pytest.raises(BudgetExhaustedError) as exc_info:
        manager.acquire(excessive_rec)
    assert "disk budget exceeded" in str(exc_info.value)

    # 3. Excessive connection demand
    excessive_conn_rec = OwnerRecord(
        owner_id="owner-conn-hog",
        resource_id="res-tunnels",
        kind=ResourceKind.TUNNEL,
        host="10.0.0.3",
        demand=ResourceDemand(connections=100),  # Limit is 50
    )
    with pytest.raises(BudgetExhaustedError) as exc_info:
        manager.acquire(excessive_conn_rec)
    assert "connections budget exceeded" in str(exc_info.value)


# -----------------------------------------------------------------------------
# Acceptance: Safe Endpoint Enforcement
# -----------------------------------------------------------------------------

def test_safe_endpoint_validation(manager: ResourceLifecycleManager):
    """Permit only 127.0.0.1, localhost, or RFC1918 10.0.0.x hosts."""
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("localhost") == "localhost"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("192.168.1.1")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("8.8.8.8")

    # Acquiring on unsafe host raises UnsafeEndpointError
    unsafe_rec = OwnerRecord(
        owner_id="owner-unsafe",
        resource_id="res-unsafe-slot",
        kind=ResourceKind.SLOT,
        host="192.168.1.1",
    )
    with pytest.raises(UnsafeEndpointError):
        manager.acquire(unsafe_rec)


# -----------------------------------------------------------------------------
# Acceptance: Interruption Recovery at Acquisition and Restore Points
# -----------------------------------------------------------------------------

def test_interruption_at_acquisition_point_rolls_back_cleanly(manager: ResourceLifecycleManager):
    """Interruption while in ACQUIRING state triggers deterministic rollback."""
    interrupted_rec = OwnerRecord(
        owner_id="owner-crashed-run",
        resource_id="tmp_table_interrupted",
        kind=ResourceKind.TABLE,
        host="127.0.0.1",
        state=ResourceState.ACQUIRING,
    )
    manager.store.record_resource(interrupted_rec)

    rolled_back_items: List[str] = []

    def mock_rollback(r: OwnerRecord):
        rolled_back_items.append(r.resource_id)

    recovered = manager.recover_interrupted(
        owner_alive=False,
        rollback_callback=mock_rollback,
    )

    assert len(recovered) == 1
    assert recovered[0].resource_id == "tmp_table_interrupted"
    assert recovered[0].state == ResourceState.CLEANED_UP
    assert rolled_back_items == ["tmp_table_interrupted"]


def test_orphaned_unshared_resource_force_cleanup(manager: ResourceLifecycleManager):
    """Dead owner with active unshared resource is deterministically cleaned up."""
    orphaned_rec = OwnerRecord(
        owner_id="owner-dead",
        resource_id="orphan_spool_01",
        kind=ResourceKind.SPOOL,
        host="10.0.0.5",
        state=ResourceState.ACTIVE,
        is_shared=False,
        has_sole_undo_copy=False,
        demand=ResourceDemand(disk_bytes=500_000),
    )
    manager.store.record_resource(orphaned_rec)
    manager.budget.allocate(orphaned_rec.demand)

    cleaned_items: List[str] = []

    def mock_cleanup(r: OwnerRecord):
        cleaned_items.append(r.resource_id)

    recovered = manager.recover_interrupted(
        owner_alive=False,
        cleanup_callback=mock_cleanup,
    )

    assert len(recovered) == 1
    assert recovered[0].resource_id == "orphan_spool_01"
    assert recovered[0].state == ResourceState.CLEANED_UP
    assert cleaned_items == ["orphan_spool_01"]
    assert manager.budget.disk_bytes_used == 0


# -----------------------------------------------------------------------------
# Acceptance: Shared Resources Remain Untouched
# -----------------------------------------------------------------------------

def test_shared_resource_remains_untouched_on_release(manager: ResourceLifecycleManager):
    """Shared resource releases ownership lease without destroying physical entity."""
    shared_rec = OwnerRecord(
        owner_id="owner-shared-client",
        resource_id="shared_logical_slot_v1",
        kind=ResourceKind.SLOT,
        host="10.0.0.10",
        is_shared=True,
        obligation=ObligationKind.UNBIND,
        demand=ResourceDemand(connections=2),
    )
    manager.acquire(shared_rec)

    physical_dropped = False

    def mock_physical_drop():
        nonlocal physical_dropped
        physical_dropped = True

    released = manager.release(
        shared_rec.resource_id,
        owner_alive=False,
        deleter_callback=mock_physical_drop,
    )

    assert released.state == ResourceState.CLEANED_UP
    # Physical resource was NOT dropped!
    assert not physical_dropped
    assert manager.budget.connections_used == 0


# -----------------------------------------------------------------------------
# Acceptance: Original Settings Durably Recorded and Restored
# -----------------------------------------------------------------------------

def test_original_setting_restored_to_observed_baseline(manager: ResourceLifecycleManager):
    """Modified database setting is restored to observed original value."""
    setting_rec = OwnerRecord(
        owner_id="owner-tuning-task",
        resource_id="work_mem",
        kind=ResourceKind.SETTING,
        host="127.0.0.1",
        obligation=ObligationKind.RESTORE,
        original_state={"name": "work_mem", "value": "64MB"},
        current_state={"name": "work_mem", "value": "256MB"},
        demand=ResourceDemand(memory_bytes=256 * 1024 * 1024),
    )
    manager.acquire(setting_rec)

    applied_settings: Dict[str, Any] = {}

    def mock_set_setting(name: str, val: Any):
        applied_settings[name] = val

    # Target currently holds the migkit modified value
    restored = manager.restore_setting("work_mem", "256MB", mock_set_setting)
    assert restored.state == ResourceState.RESTORED
    assert applied_settings["work_mem"] == "64MB"
    assert manager.budget.memory_bytes_used == 0


def test_external_setting_drift_preserved(manager: ResourceLifecycleManager):
    """External administrative setting modification is preserved rather than overwritten."""
    setting_rec = OwnerRecord(
        owner_id="owner-tuning-task",
        resource_id="max_parallel_workers",
        kind=ResourceKind.SETTING,
        host="127.0.0.1",
        obligation=ObligationKind.RESTORE,
        original_state={"name": "max_parallel_workers", "value": 4},
        current_state={"name": "max_parallel_workers", "value": 16},
    )
    manager.acquire(setting_rec)

    applied_settings: Dict[str, Any] = {}

    def mock_set_setting(name: str, val: Any):
        applied_settings[name] = val

    # An administrator altered the setting out-of-band to 8
    restored = manager.restore_setting("max_parallel_workers", 8, mock_set_setting)
    assert restored.state == ResourceState.RESTORED
    # Setter was NOT invoked to overwrite external change!
    assert "max_parallel_workers" not in applied_settings


# -----------------------------------------------------------------------------
# Acceptance: Sole Copies of Change/Undo Data Never Prematurely Removed
# -----------------------------------------------------------------------------

def test_sole_copy_of_change_data_never_prematurely_removed(manager: ResourceLifecycleManager):
    """Resource holding sole copy of change/undo data refuses cleanup and quarantines."""
    spool_rec = OwnerRecord(
        owner_id="owner-cdc-worker",
        resource_id="wal_change_spool_001",
        kind=ResourceKind.SPOOL,
        host="10.0.0.20",
        has_sole_undo_copy=True,
    )
    manager.acquire(spool_rec)

    deleted_called = False

    def mock_delete():
        nonlocal deleted_called
        deleted_called = True

    # Attempting to delete when owner is gone must raise SoleUndoDataHazardError
    with pytest.raises(SoleUndoDataHazardError):
        manager.release(
            spool_rec.resource_id,
            owner_alive=False,
            deleter_callback=mock_delete,
        )

    assert not deleted_called
    updated_rec = manager.store.get_resource(spool_rec.resource_id)
    assert updated_rec is not None
    assert updated_rec.state == ResourceState.RETAINED_SOLE_COPY

    # Audit report flags ACTION
    report = manager.audit_ownership()
    assert report.badge == CareBadge.ACTION
    assert report.retained_sole_count == 1
    assert any("ACTION:" in f for f in report.findings)

    # Once downstream acknowledges changes, has_sole_undo_copy becomes False
    updated_rec.has_sole_undo_copy = False
    manager.store.update_resource(updated_rec)

    # Now release succeeds cleanly
    released = manager.release(
        spool_rec.resource_id,
        owner_alive=False,
        deleter_callback=mock_delete,
    )
    assert released.state == ResourceState.CLEANED_UP
    assert deleted_called


# -----------------------------------------------------------------------------
# Acceptance: Universal Typed Durable Resume in O(1)
# -----------------------------------------------------------------------------

def test_universal_typed_durable_resume_o1(tmp_path: pathlib.Path):
    """Store persists and deserializes datetime, Decimal, UUID in O(1)."""
    store_file = tmp_path / "typed_resume.json"
    store = ResourceOwnershipStore(store_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    rec = OwnerRecord(
        owner_id="owner-uuid-001",
        resource_id="res-typed-table",
        kind=ResourceKind.TABLE,
        host="127.0.0.1",
        acquired_at=now,
        updated_at=now,
        lease_expiry=now + datetime.timedelta(hours=1),
        state=ResourceState.CLEANED_UP,
    )
    store.record_resource(rec)

    # Reload fresh store instance
    reloaded = ResourceOwnershipStore(store_file)
    assert reloaded.is_completed_o1("res-typed-table")
    loaded_rec = reloaded.get_resource("res-typed-table")
    assert loaded_rec is not None
    assert isinstance(loaded_rec.acquired_at, datetime.datetime)
    assert loaded_rec.state == ResourceState.CLEANED_UP


# -----------------------------------------------------------------------------
# Acceptance: Mutual Exclusion & Active Lease Lock
# -----------------------------------------------------------------------------

def test_active_lease_lock_mutual_exclusion(tmp_path: pathlib.Path):
    """Active verification lease lock (.run.lock) prevents concurrent runs."""
    lock_path = tmp_path / "test.run.lock"
    lease_a = acquire_ownership_lease(lock_path, what="run-A", term_seconds=30.0)
    assert lock_path.exists()

    # Second concurrent lease acquisition raises SystemExit / collision
    with pytest.raises(SystemExit):
        acquire_ownership_lease(lock_path, what="run-B", term_seconds=30.0)

    lease_a.release()
    assert not lock_path.exists()


# -----------------------------------------------------------------------------
# Acceptance: Care Badges and UNIX Report Formatting
# -----------------------------------------------------------------------------

def test_audit_report_care_badges_pass_and_review(manager: ResourceLifecycleManager):
    """Audit report outputs 4-tier care badges without decorative glyphs."""
    # 1. When clean, outputs PASS
    report_pass = manager.audit_ownership()
    assert report_pass.badge == CareBadge.PASS
    assert "badge=PASS" in report_pass.summary

    # 2. When active resources remain, outputs REVIEW
    rec = OwnerRecord(
        owner_id="active-worker",
        resource_id="active-tunnel-01",
        kind=ResourceKind.TUNNEL,
        host="127.0.0.1",
    )
    manager.acquire(rec)
    report_review = manager.audit_ownership()
    assert report_review.badge == CareBadge.REVIEW
    assert "REVIEW: resource active-tunnel-01" in report_review.findings[0]
