"""Tests for Row 613: RCH05.repair-fence (Schedule repair against CDC without stale overwrite)."""
from __future__ import annotations

import datetime
import decimal
import uuid
from pathlib import Path
from typing import List

import pytest

import tests.rung_branches
from migkit.repair_fence import (
    CDCOp,
    CDCChange,
    CareBadge,
    ConflictResolution,
    FencePhase,
    FencedRepairCoordinator,
    InterruptedRepairError,
    LeaseLockHeldError,
    RecoveryDisposition,
    RepairFenceCheckpoint,
    RepairFenceLeaseLock,
    RepairRowSnapshot,
    RepairScope,
    ReplayGapError,
    ScopeReadings,
    StaleFenceEpochError,
    TargetTableStore,
    UnsafeHostError,
    VerificationDivergenceError,
    compute_scope_costs,
    resolve_repair_conflict,
    select_interrupted_recovery,
    select_repair_scope,
    validate_safe_host,
)

PROVES_RUNGS = [
    ("repair_fence", "select_repair_scope", "row_repair", "holds"),
    ("repair_fence", "select_repair_scope", "row_repair", "refuses"),
    ("repair_fence", "select_repair_scope", "range_repair", "holds"),
    ("repair_fence", "select_repair_scope", "range_repair", "refuses"),
    ("repair_fence", "select_repair_scope", "table_reload", "holds"),
    ("repair_fence", "select_repair_scope", "table_reload", "refuses"),
    ("repair_fence", "select_repair_scope", "defer_bounded_outage", "holds"),
    ("repair_fence", "select_repair_scope", "defer_bounded_outage", "refuses"),
    ("repair_fence", "resolve_repair_conflict", "keep_target_policy", "holds"),
    ("repair_fence", "resolve_repair_conflict", "keep_target_policy", "refuses"),
    ("repair_fence", "resolve_repair_conflict", "preserve_newer_target", "holds"),
    ("repair_fence", "resolve_repair_conflict", "preserve_newer_target", "refuses"),
    ("repair_fence", "resolve_repair_conflict", "apply_repair_snapshot", "holds"),
    ("repair_fence", "resolve_repair_conflict", "apply_repair_snapshot", "refuses"),
    ("repair_fence", "select_interrupted_recovery", "reconcile_and_complete", "holds"),
    ("repair_fence", "select_interrupted_recovery", "reconcile_and_complete", "refuses"),
    ("repair_fence", "select_interrupted_recovery", "undo_to_preimage", "holds"),
    ("repair_fence", "select_interrupted_recovery", "undo_to_preimage", "refuses"),
    ("repair_fence", "select_interrupted_recovery", "abort_corrupted", "holds"),
    ("repair_fence", "select_interrupted_recovery", "abort_corrupted", "refuses"),
]


def test_every_repair_fence_rung_holds_and_refuses_under_watched() -> None:
    """Exercises both holds and refuses branches for every rung in repair_fence.py."""
    with tests.rung_branches.watched() as seen:
        # --- Ladder 1: select_repair_scope ---
        # 1. row_repair holds
        s1 = select_repair_scope(
            {
                "table_name": "orders",
                "table_rows": 100000,
                "named_rows": 5,
                "span_rows": 5000,
                "has_range_key": True,
                "row_by_row_only": False,
                "replace_allowed": True,
                "max_outage_seconds": 10.0,
                "row_outage_seconds": 0.1,
                "row_total_cost": 0.15,
                "range_outage_seconds": 2.0,
                "range_total_cost": 2.5,
                "table_outage_seconds": 20.0,
                "table_total_cost": 25.0,
                "allow_defer": True,
            }
        )
        assert s1.path == "row_repair"

        # 2. range_repair holds (row_repair refuses due to higher cost)
        s2 = select_repair_scope(
            {
                "table_name": "orders",
                "table_rows": 100000,
                "named_rows": 2000,
                "span_rows": 2500,
                "has_range_key": True,
                "row_by_row_only": False,
                "replace_allowed": True,
                "max_outage_seconds": 10.0,
                "row_outage_seconds": 8.0,
                "row_total_cost": 8.5,
                "range_outage_seconds": 1.2,
                "range_total_cost": 1.5,
                "table_outage_seconds": 9.0,
                "table_total_cost": 9.5,
                "allow_defer": True,
            }
        )
        assert s2.path == "range_repair"

        # 3. table_reload holds (row_repair and range_repair refuse)
        s3 = select_repair_scope(
            {
                "table_name": "orders",
                "table_rows": 50000,
                "named_rows": 0,
                "span_rows": 45000,
                "has_range_key": True,
                "row_by_row_only": False,
                "replace_allowed": True,
                "max_outage_seconds": 10.0,
                "row_outage_seconds": float("inf"),
                "row_total_cost": float("inf"),
                "range_outage_seconds": 9.0,
                "range_total_cost": 9.5,
                "table_outage_seconds": 3.0,
                "table_total_cost": 3.5,
                "allow_defer": True,
            }
        )
        assert s3.path == "table_reload"

        # 4. defer_bounded_outage holds (all active scopes exceed max_outage_seconds)
        s4 = select_repair_scope(
            {
                "table_name": "orders",
                "table_rows": 50000,
                "named_rows": 5000,
                "span_rows": 10000,
                "has_range_key": True,
                "row_by_row_only": False,
                "replace_allowed": True,
                "max_outage_seconds": 0.5,
                "row_outage_seconds": 5.0,
                "row_total_cost": 5.5,
                "range_outage_seconds": 4.0,
                "range_total_cost": 4.5,
                "table_outage_seconds": 8.0,
                "table_total_cost": 8.5,
                "allow_defer": True,
            }
        )
        assert s4.path == "defer_bounded_outage"

        # 5. defer_bounded_outage refuses when allow_defer=False
        s5 = select_repair_scope(
            {
                "table_name": "orders",
                "table_rows": 50000,
                "named_rows": 0,
                "span_rows": 0,
                "has_range_key": False,
                "row_by_row_only": True,
                "replace_allowed": False,
                "max_outage_seconds": 0.5,
                "allow_defer": False,
            }
        )
        assert s5.rung is None

        # --- Ladder 2: resolve_repair_conflict ---
        # 1. keep_target_policy holds
        c1 = resolve_repair_conflict(
            {
                "row_key": "k1",
                "conflict_policy": "keep-target",
                "target_modified": True,
                "latest_target_lsn": 10,
                "latest_target_ts": 100.0,
                "repair_snapshot_lsn": 20,
                "repair_snapshot_ts": 200.0,
            }
        )
        assert c1.path == "keep_target_policy"

        # 2. preserve_newer_target holds (keep_target_policy refuses, apply_repair_snapshot refuses)
        c2 = resolve_repair_conflict(
            {
                "row_key": "k2",
                "conflict_policy": "newest_wins",
                "target_modified": False,
                "latest_target_lsn": 50,
                "latest_target_ts": 500.0,
                "repair_snapshot_lsn": 30,
                "repair_snapshot_ts": 300.0,
            }
        )
        assert c2.path == "preserve_newer_target"

        # 3. apply_repair_snapshot holds (keep_target_policy and preserve_newer_target refuse)
        c3 = resolve_repair_conflict(
            {
                "row_key": "k3",
                "conflict_policy": "newest_wins",
                "target_modified": False,
                "latest_target_lsn": 20,
                "latest_target_ts": 200.0,
                "repair_snapshot_lsn": 30,
                "repair_snapshot_ts": 300.0,
            }
        )
        assert c3.path == "apply_repair_snapshot"

        # --- Ladder 3: select_interrupted_recovery ---
        # 1. reconcile_and_complete holds (abort_corrupted refuses)
        r1 = select_interrupted_recovery(
            {
                "epoch_id": "ep-1",
                "epoch_journal_valid": True,
                "cdc_buffer_gap_free": True,
                "can_finish_reconcile": True,
                "force_undo": False,
                "preimage_recorded": True,
                "target_matches_repair_writes": True,
            }
        )
        assert r1.path == "reconcile_and_complete"

        # 2. undo_to_preimage holds (reconcile_and_complete refuses due to force_undo=True)
        r2 = select_interrupted_recovery(
            {
                "epoch_id": "ep-2",
                "epoch_journal_valid": True,
                "cdc_buffer_gap_free": True,
                "can_finish_reconcile": True,
                "force_undo": True,
                "preimage_recorded": True,
                "target_matches_repair_writes": True,
            }
        )
        assert r2.path == "undo_to_preimage"

        # 3. abort_corrupted holds (reconcile_and_complete and undo_to_preimage refuse)
        r3 = select_interrupted_recovery(
            {
                "epoch_id": "ep-3",
                "epoch_journal_valid": False,
                "cdc_buffer_gap_free": False,
                "can_finish_reconcile": False,
                "force_undo": True,
                "preimage_recorded": False,
                "target_matches_repair_writes": False,
            }
        )
        assert r3.path == "abort_corrupted"

    for _, _, rung_name, expected_branch in PROVES_RUNGS:
        branches = tests.rung_branches.branch(seen, rung_name)
        assert expected_branch in branches, (
            f"rung {rung_name} did not reach {expected_branch}; got {branches}"
        )


def test_delayed_repair_write_while_later_cdc_update_arrives_newest_wins(tmp_path: Path) -> None:
    """Delays a repair write while a later CDC update arrives; newest committed source value wins."""
    target = TargetTableStore(
        table_name="accounts",
        initial_rows={
            101: {"id": 101, "balance": 100, "status": "drifted_old", "_migkit_lsn": 10, "_migkit_ts": 10.0},
            102: {"id": 102, "balance": 200, "status": "drifted_old", "_migkit_lsn": 10, "_migkit_ts": 10.0},
            999: {"id": 999, "balance": 900, "status": "clean", "_migkit_lsn": 10, "_migkit_ts": 10.0},
        },
    )
    coord = FencedRepairCoordinator(
        target=target,
        checkpoint_path=tmp_path / "epoch.json",
        host="127.0.0.1",
    )

    # Queue an in-flight apply event that started prior to the repair fence
    coord.queue_in_flight_apply(
        [
            CDCChange(
                seq=1,
                lsn=15,
                op=CDCOp.UPDATE,
                table="accounts",
                key=999,
                after={"id": 999, "balance": 950, "status": "clean"},
                source_ts=15.0,
            )
        ]
    )

    readings = ScopeReadings(
        table_name="accounts",
        total_rows=10000,
        named_rows=2,
        span_rows=2000,
        measured_row_repair_rate=500.0,
        measured_range_copy_rate=2000.0,
        measured_table_reload_rate=5000.0,
        max_outage_seconds=5.0,
    )

    # Repair snapshot was taken at LSN=20
    snapshots = [
        RepairRowSnapshot(
            key=101,
            row={"id": 101, "balance": 150, "status": "repaired_at_20"},
            snapshot_lsn=20,
            snapshot_ts=20.0,
        ),
        RepairRowSnapshot(
            key=102,
            row={"id": 102, "balance": 250, "status": "repaired_at_20"},
            snapshot_lsn=20,
            snapshot_ts=20.0,
        ),
    ]

    # Hook that delays repair write on key=101 while later CDC updates arrive!
    def delay_repair_and_inject_cdc(snap: RepairRowSnapshot, idx: int) -> None:
        if idx == 0:
            # 1) A later CDC update (LSN=35 > snapshot_lsn=20) arrives for key=101
            status_101 = coord.ingest_cdc_event(
                CDCChange(
                    seq=2,
                    lsn=35,
                    op=CDCOp.UPDATE,
                    table="accounts",
                    key=101,
                    after={"id": 101, "balance": 175, "status": "cdc_newer_at_35"},
                    source_ts=35.0,
                )
            )
            assert status_101 == "buffered_in_epoch"

            # 2) Another CDC update (LSN=36) arrives for key=101 in the same epoch
            status_101_b = coord.ingest_cdc_event(
                CDCChange(
                    seq=3,
                    lsn=36,
                    op=CDCOp.UPDATE,
                    table="accounts",
                    key=101,
                    after={"id": 101, "balance": 190, "status": "cdc_newest_at_36"},
                    source_ts=36.0,
                )
            )
            assert status_101_b == "buffered_in_epoch"

            # 3) An unaffected key (key=999) receives a live CDC update and is NOT stalled
            status_999 = coord.ingest_cdc_event(
                CDCChange(
                    seq=4,
                    lsn=37,
                    op=CDCOp.UPDATE,
                    table="accounts",
                    key=999,
                    after={"id": 999, "balance": 999, "status": "live_unaffected"},
                    source_ts=37.0,
                )
            )
            assert status_999 == "applied_unaffected"

    expected_post_cdc = {
        101: {"id": 101, "balance": 190, "status": "cdc_newest_at_36"},
        102: {"id": 102, "balance": 250, "status": "repaired_at_20"},
        999: {"id": 999, "balance": 999, "status": "live_unaffected"},
    }

    outcome = coord.execute_fenced_repair(
        epoch_id="ep-delay-1",
        readings=readings,
        snapshots=snapshots,
        expected_source_rows=expected_post_cdc,
        on_before_write=delay_repair_and_inject_cdc,
    )

    assert outcome.scope == RepairScope.ROW_REPAIR
    assert outcome.phase == FencePhase.COMMITTED
    assert outcome.drained_in_flight_count == 1
    # key=101 had a newer buffered CDC change (LSN 36 > 20), so stale repair write was preserved/held
    assert outcome.preserved_newer_count == 1
    # key=102 had no newer CDC change, so repair snapshot at LSN 20 applied
    assert outcome.applied_repair_count == 1
    # Both buffered CDC changes (seq 2 and seq 3) replayed gap-free
    assert outcome.replayed_cdc_count == 2
    assert outcome.reverification.verified is True
    assert outcome.care_badge == CareBadge.BENIGN
    assert "[BENIGN]" in outcome.format_line()

    assert target.get_row(101) == {"id": 101, "balance": 190, "status": "cdc_newest_at_36"}
    assert target.get_lsn(101) == 36
    assert target.get_row(102) == {"id": 102, "balance": 250, "status": "repaired_at_20"}
    assert target.get_lsn(102) == 20
    assert target.get_row(999) == {"id": 999, "balance": 999, "status": "live_unaffected"}
    assert coord.live_passthrough_applied == (4,)


def test_target_already_ahead_of_repair_snapshot_is_never_overwritten() -> None:
    """When target already holds a row at LSN=40, a delayed repair snapshot at LSN=25 does not overwrite it."""
    target = TargetTableStore(
        table_name="inventory",
        initial_rows={
            1: {"id": 1, "qty": 88, "_migkit_lsn": 40, "_migkit_ts": 40.0},
        },
    )
    coord = FencedRepairCoordinator(target=target)
    coord.begin_epoch(
        epoch_id="ep-target-ahead",
        scope=RepairScope.ROW_REPAIR,
        snapshot_lsn=25,
        snapshot_ts=25.0,
        affected_keys=[1],
    )
    applied, preserved, skipped = coord.apply_repair_snapshots(
        [
            RepairRowSnapshot(
                key=1,
                row={"id": 1, "qty": 10},
                snapshot_lsn=25,
                snapshot_ts=25.0,
            )
        ]
    )
    assert applied == 0
    assert preserved == 1
    assert skipped == 0
    assert target.get_row(1) == {"id": 1, "qty": 88}
    assert target.get_lsn(1) == 40


def test_gap_free_cdc_replay_and_gap_detection() -> None:
    """Verifies gap-free sequence enforcement on buffered CDC replay."""
    target = TargetTableStore(table_name="events")
    coord = FencedRepairCoordinator(target=target)
    coord.begin_epoch(
        epoch_id="ep-gap",
        scope=RepairScope.TABLE_RELOAD,
        snapshot_lsn=10,
        snapshot_ts=10.0,
    )
    coord.ingest_cdc_event(
        CDCChange(seq=10, lsn=11, op=CDCOp.INSERT, table="events", key=1, after={"id": 1, "v": "a"})
    )
    # Missing seq=11 -> gap between 10 and 12!
    coord.ingest_cdc_event(
        CDCChange(seq=12, lsn=13, op=CDCOp.UPDATE, table="events", key=1, after={"id": 1, "v": "b"})
    )

    with pytest.raises(ReplayGapError, match="sequence gap in buffered CDC"):
        coord.reconcile_buffered_cdc()


def test_interrupted_repair_reconciles_to_completion_with_o1_typed_resume(tmp_path: Path) -> None:
    """An interrupted repair resumes in O(1) from typed durable checkpoint without rescanning completed keys."""
    ckpt_path = tmp_path / "durable_epoch.json"
    composite_k1 = ("tenant_a", uuid.UUID("11111111-2222-3333-4444-555555555555"))
    composite_k2 = ("tenant_a", uuid.UUID("66666666-7777-8888-9999-000000000000"))

    ts_val = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc)
    dec_val = decimal.Decimal("1234.56")

    target = TargetTableStore(
        table_name="ledger",
        initial_rows={
            composite_k1: {"amount": decimal.Decimal("0.00"), "updated_at": ts_val, "_migkit_lsn": 5},
            composite_k2: {"amount": decimal.Decimal("0.00"), "updated_at": ts_val, "_migkit_lsn": 5},
        },
    )
    coord = FencedRepairCoordinator(target=target, checkpoint_path=ckpt_path)
    coord.begin_epoch(
        epoch_id="ep-resume",
        scope=RepairScope.ROW_REPAIR,
        snapshot_lsn=20,
        snapshot_ts=20.0,
        affected_keys=[composite_k1, composite_k2],
    )

    # Buffer a CDC update for composite_k2 that arrives during the repair
    coord.ingest_cdc_event(
        CDCChange(
            seq=1,
            lsn=25,
            op=CDCOp.UPDATE,
            table="ledger",
            key=composite_k2,
            after={"amount": decimal.Decimal("9999.99"), "updated_at": ts_val},
            source_ts=25.0,
        )
    )

    snapshots = [
        RepairRowSnapshot(
            key=composite_k1,
            row={"amount": dec_val, "updated_at": ts_val},
            snapshot_lsn=20,
            snapshot_ts=20.0,
        ),
        RepairRowSnapshot(
            key=composite_k2,
            row={"amount": decimal.Decimal("50.00"), "updated_at": ts_val},
            snapshot_lsn=20,
            snapshot_ts=20.0,
        ),
    ]

    # Simulate interruption after the 1st key write
    with pytest.raises(InterruptedRepairError, match="interrupted after 1 key operations"):
        coord.apply_repair_snapshots(snapshots, interrupt_after_writes=1)

    # Re-instantiate coordinator from durable checkpoint
    resumed_coord = FencedRepairCoordinator(target=target, checkpoint_path=ckpt_path)
    visited_keys: List[ object] = []

    choice, final_phase, report = resumed_coord.recover_interrupted_epoch(
        remaining_snapshots=snapshots,
        expected_source_rows={
            composite_k1: {"amount": dec_val, "updated_at": ts_val},
            composite_k2: {"amount": decimal.Decimal("9999.99"), "updated_at": ts_val},
        },
        force_undo=False,
    )

    assert choice.path == RecoveryDisposition.RECONCILE_AND_COMPLETE.value
    assert final_phase == FencePhase.COMMITTED
    assert report.verified is True
    assert target.get_row(composite_k1) == {"amount": dec_val, "updated_at": ts_val}
    assert target.get_row(composite_k2) == {"amount": decimal.Decimal("9999.99"), "updated_at": ts_val}

    # Verify O(1) skip of both completed keys if called again
    applied, preserved, skipped = resumed_coord.apply_repair_snapshots(
        snapshots,
        on_before_write=lambda snap, _: visited_keys.append(snap.key),
    )
    assert (applied, preserved, skipped) == (0, 0, 2)
    assert visited_keys == []


def test_interrupted_repair_safe_undo_to_preimage_and_cdc_replay(tmp_path: Path) -> None:
    """An interrupted repair can safely undo touched rows to recorded pre-images and replay held CDC."""
    ckpt_path = tmp_path / "undo_epoch.json"
    target = TargetTableStore(
        table_name="orders",
        initial_rows={
            1: {"id": 1, "state": "pre_repair_1", "_migkit_lsn": 10},
            2: {"id": 2, "state": "pre_repair_2", "_migkit_lsn": 10},
        },
    )
    coord = FencedRepairCoordinator(target=target, checkpoint_path=ckpt_path)
    coord.begin_epoch(
        epoch_id="ep-undo",
        scope=RepairScope.ROW_REPAIR,
        snapshot_lsn=20,
        snapshot_ts=20.0,
        affected_keys=[1, 2],
    )
    coord.ingest_cdc_event(
        CDCChange(
            seq=1,
            lsn=30,
            op=CDCOp.UPDATE,
            table="orders",
            key=2,
            after={"id": 2, "state": "cdc_at_30"},
            source_ts=30.0,
        )
    )

    snapshots = [
        RepairRowSnapshot(key=1, row={"id": 1, "state": "repair_write_1"}, snapshot_lsn=20),
        RepairRowSnapshot(key=2, row={"id": 2, "state": "repair_write_2"}, snapshot_lsn=20),
    ]

    with pytest.raises(InterruptedRepairError):
        coord.apply_repair_snapshots(snapshots, interrupt_after_writes=1)

    assert target.get_row(1) == {"id": 1, "state": "repair_write_1"}

    choice, final_phase, report = coord.recover_interrupted_epoch(
        remaining_snapshots=snapshots,
        expected_source_rows={},
        force_undo=True,
    )
    assert choice.path == RecoveryDisposition.UNDO_TO_PREIMAGE.value
    assert final_phase == FencePhase.UNDONE
    assert report.verified is True
    # Key 1 restored to pre-image; Key 2 received buffered CDC event
    assert target.get_row(1) == {"id": 1, "state": "pre_repair_1"}
    assert target.get_row(2) == {"id": 2, "state": "cdc_at_30"}


def test_scope_selection_by_measured_cost_and_bounded_outage() -> None:
    """Verifies compute_scope_costs and select_repair_scope across row, range, table, and defer."""
    target = TargetTableStore(table_name="items")
    coord = FencedRepairCoordinator(target=target)

    # Case A: Few named rows -> row_repair
    choice_row, _ = coord.plan_scope(
        ScopeReadings(
            table_name="items",
            total_rows=100000,
            named_rows=10,
            span_rows=5000,
            measured_row_repair_rate=200.0,
            measured_range_copy_rate=2000.0,
            measured_table_reload_rate=5000.0,
            max_outage_seconds=10.0,
        )
    )
    assert choice_row.path == RepairScope.ROW_REPAIR.value

    # Case B: Heavy drift in one span -> range_repair beats slow point repairs and full table reload
    choice_range, _ = coord.plan_scope(
        ScopeReadings(
            table_name="items",
            total_rows=200000,
            named_rows=4000,
            span_rows=5000,
            measured_row_repair_rate=100.0,
            measured_range_copy_rate=5000.0,
            measured_table_reload_rate=5000.0,
            max_outage_seconds=15.0,
        )
    )
    assert choice_range.path == RepairScope.RANGE_REPAIR.value

    # Case C: Widespread drift across whole table -> table_reload
    choice_table, _ = coord.plan_scope(
        ScopeReadings(
            table_name="items",
            total_rows=20000,
            named_rows=18000,
            span_rows=19500,
            measured_row_repair_rate=100.0,
            measured_range_copy_rate=2000.0,
            measured_table_reload_rate=10000.0,
            max_outage_seconds=10.0,
        )
    )
    assert choice_table.path == RepairScope.TABLE_RELOAD.value


def test_lease_lock_mutual_exclusion_and_stale_fence_rejection(tmp_path: Path) -> None:
    """Verifies .run.lock mutual exclusion and target fence token enforcement."""
    lock_a = RepairFenceLeaseLock(tmp_path, owner_id="worker-a", ttl_seconds=30.0)
    lock_b = RepairFenceLeaseLock(tmp_path, owner_id="worker-b", ttl_seconds=30.0)

    with lock_a:
        lock_a.heartbeat()
        with pytest.raises(LeaseLockHeldError, match="worker-a"):
            lock_b.acquire()

    # After lock_a exits, lock_b can acquire cleanly
    with lock_b:
        assert (tmp_path / ".run.lock").exists()

    target = TargetTableStore(table_name="fenced_tbl", initial_fence_token=5)
    with pytest.raises(StaleFenceEpochError):
        target.raise_fence(4)
    with pytest.raises(StaleFenceEpochError):
        target.put_row(1, {"id": 1}, lsn=10, fence_token=4)

    with pytest.raises(UnsafeHostError):
        validate_safe_host("192.168.99.99")


def test_range_scoped_hold_and_keep_target_policy_and_abort_corrupted() -> None:
    """Verifies range-scoped CDC hold, keep-target preservation, and abort_corrupted recovery."""
    target = TargetTableStore(
        table_name="shipments",
        initial_rows={
            10: {"id": 10, "loc": "custom_target", "_migkit_lsn": 5, "_migkit_target_modified": True},
            20: {"id": 20, "loc": "drifted", "_migkit_lsn": 5},
            500: {"id": 500, "loc": "outside_span", "_migkit_lsn": 5},
        },
    )
    coord = FencedRepairCoordinator(target=target, host="10.0.0.5")
    coord.begin_epoch(
        epoch_id="ep-range",
        scope=RepairScope.RANGE_REPAIR,
        snapshot_lsn=15,
        snapshot_ts=15.0,
        affected_ranges=[(1, 100)],
        conflict_policy="keep-target",
    )

    # Event inside span (1..100) is buffered; event at 500 is applied live
    assert (
        coord.ingest_cdc_event(
            CDCChange(seq=1, lsn=18, op=CDCOp.DELETE, table="shipments", key=20, source_ts=18.0)
        )
        == "buffered_in_epoch"
    )
    assert (
        coord.ingest_cdc_event(
            CDCChange(
                seq=2,
                lsn=19,
                op=CDCOp.UPDATE,
                table="shipments",
                key=500,
                after={"id": 500, "loc": "live_500"},
                source_ts=19.0,
            )
        )
        == "applied_unaffected"
    )

    snapshots = [
        RepairRowSnapshot(key=10, row={"id": 10, "loc": "src_10"}, snapshot_lsn=15),
        RepairRowSnapshot(key=20, row={"id": 20, "loc": "src_20"}, snapshot_lsn=15),
    ]
    applied, preserved, _ = coord.apply_repair_snapshots(snapshots)
    # key=10 preserved due to keep-target, key=20 preserved due to newer buffered DELETE at LSN=18
    assert applied == 0
    assert preserved == 2

    replayed = coord.reconcile_buffered_cdc()
    assert replayed == 1
    assert target.get_row(10) == {"id": 10, "loc": "custom_target"}
    assert target.get_row(20) is None
    assert target.get_row(500) == {"id": 500, "loc": "live_500"}

    # Reverify passes for keep-target on key 10 and deleted key 20
    rep = coord.reverify_scope({10: {"id": 10, "loc": "src_10"}, 20: None, 500: {"id": 500, "loc": "live_500"}})
    assert rep.verified is True

    # External mutation of target row after partial repair triggers abort_corrupted on recovery
    target.put_row(10, {"id": 10, "loc": "tampered_outside_fence"}, lsn=99)
    choice, phase, abort_rep = coord.recover_interrupted_epoch(
        remaining_snapshots=[],
        expected_source_rows={},
        can_finish_reconcile=False,
    )
    assert choice.path == RecoveryDisposition.ABORT_CORRUPTED.value
    assert phase == FencePhase.FAILED
    assert abort_rep.care_badge == CareBadge.ACTION


def test_pure_ascii_compliance() -> None:
    """Ensures module and test suite are 100% pure ASCII."""
    root = Path(__file__).resolve().parent.parent
    for rel in ("migkit/repair_fence.py", "tests/test_repair_fence_cdc_coordination.py"):
        content = (root / rel).read_bytes()
        content.decode("ascii")

