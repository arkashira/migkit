import tempfile
from pathlib import Path
import pytest

from migkit.snapshot_state import (
    CDCSubworkflow,
    IncompatibleWatermarkError,
    IncrementalSnapshotSubworkflow,
    InvariantViolationError,
    Obligation,
    ObligationWipeError,
    ShrinkingCoverageError,
    SnapshotStateError,
    SnapshotSubworkflow,
    SnapshotWorkflowCoordinator,
    SubworkflowStatus,
    SubworkflowType,
    TransferInvariant,
    WorkUnit,
    WorkUnitStatus,
    decide_subworkflow_transition,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("snapshot_state", "snapshot_subworkflow_rungs", "discharge_obligation_and_advance", "holds"),
    ("snapshot_state", "snapshot_subworkflow_rungs", "discharge_obligation_and_advance", "refuses"),
    ("snapshot_state", "snapshot_subworkflow_rungs", "retain_pending_subworkflow", "holds"),
    ("snapshot_state", "snapshot_subworkflow_rungs", "retain_pending_subworkflow", "refuses"),
    ("snapshot_state", "snapshot_subworkflow_rungs", "refuse_phase_wipe", "holds"),
]


def test_work_unit_and_obligation_lifecycle():
    unit = WorkUnit(unit_id="u1", target="orders", key_range={"lo": 0, "hi": 1000})
    assert unit.status == WorkUnitStatus.PENDING
    assert unit.rows_processed == 0

    d = unit.to_dict()
    restored = WorkUnit.from_dict(d)
    assert restored.unit_id == "u1"
    assert restored.target == "orders"
    assert restored.key_range == {"lo": 0, "hi": 1000}

    obl = Obligation(
        obligation_id="obl-1",
        phase=SubworkflowType.SNAPSHOT,
        kind="table_snapshot",
        target="orders",
    )
    assert not obl.discharged
    obl.discharge(token="verified_checksum")
    assert obl.discharged
    assert obl.discharge_token == "verified_checksum"


def test_snapshot_subworkflow_partial_completion_and_restart():
    snap = SnapshotSubworkflow(subworkflow_id="snap-main")
    for i in range(100):
        snap.register_unit(f"unit-{i:03d}", target=f"chunk_{i}")

    assert snap.total_units == 100
    assert snap.completed_units == 0
    assert snap.pending_units == 100
    assert snap.coverage_pct == 0.0

    # Progress to 30%
    for i in range(30):
        snap.mark_completed(f"unit-{i:03d}", rows_processed=100)

    assert snap.completed_units == 30
    assert snap.pending_units == 70
    assert snap.coverage_pct == 30.0

    # Restart at 30% completion
    snap.restart()
    assert snap.completed_units == 30
    assert snap.pending_units == 70
    assert snap.total_units == 100
    assert snap.coverage_pct == 30.0

    # Shrinking coverage must fail
    with pytest.raises(ShrinkingCoverageError):
        snap.restart(expected_units=[f"unit-{i:03d}" for i in range(25)])

    # Expanding coverage is allowed
    expanded_units = [f"unit-{i:03d}" for i in range(100)] + ["unit-100", "unit-101"]
    snap.restart(expected_units=expanded_units)
    assert snap.total_units == 102
    assert snap.completed_units == 30
    assert snap.pending_units == 72
    assert snap.coverage_pct == 29.41

    # Finish remaining units
    for i in range(30, 102):
        snap.mark_completed(f"unit-{i:03d}" if i < 100 else f"unit-{i}", rows_processed=100)

    assert snap.completed_units == 102
    assert snap.pending_units == 0
    assert snap.coverage_pct == 100.0
    assert snap.status == SubworkflowStatus.COMPLETED


def test_incremental_snapshot_partial_completion_and_chunk_fencing():
    inc = IncrementalSnapshotSubworkflow(subworkflow_id="inc-main")
    for i in range(50):
        inc.add_chunk(
            table="orders",
            chunk_id=f"ord-chk-{i:02d}",
            low_bound=i * 1000,
            high_bound=(i + 1) * 1000,
        )

    assert inc.total_units == 50
    assert len(inc.pending_chunks) == 50
    assert inc.coverage_pct == 0.0

    # Complete 15 of 50 chunks (30%)
    for i in range(15):
        chk_id = f"ord-chk-{i:02d}"
        inc.set_chunk_watermark(chk_id, low_watermark=100 + i, high_watermark=200 + i)
        inc.complete_chunk(chk_id, rows_processed=1000)

    assert inc.completed_units == 15
    assert inc.pending_units == 35
    assert inc.coverage_pct == 30.0

    # Restart preserves 30% completed and 70% pending
    inc.restart()
    assert inc.completed_units == 15
    assert inc.pending_units == 35
    assert inc.total_units == 50
    assert inc.coverage_pct == 30.0
    assert len(inc.pending_chunks) == 35

    # Cannot silently shrink coverage
    with pytest.raises(ShrinkingCoverageError):
        inc.restart(expected_units=[f"ord-chk-{i:02d}" for i in range(10)])


def test_cdc_subworkflow_positions_and_event_obligations():
    cdc = CDCSubworkflow(subworkflow_id="cdc-main")
    cdc.advance_read("0/1000000")
    cdc.advance_persisted("0/1000000")
    cdc.advance_applied("0/0500000")

    assert cdc.read_position == "0/1000000"
    assert cdc.applied_position == "0/0500000"

    cdc.record_event_obligation("evt-1", partition="p0", position="0/0600000")
    cdc.record_event_obligation("evt-2", partition="p0", position="0/0700000")
    assert len(cdc.pending_obligations) == 2

    cdc.discharge_event_obligation("evt-1", token="applied_and_fsynced")
    assert len(cdc.pending_obligations) == 1
    assert cdc.pending_obligations[0].obligation_id == "evt-2"


def test_boot_snapshot_cannot_wipe_other_phase_obligations():
    coord = SnapshotWorkflowCoordinator()

    # Incremental snapshot has pending obligations
    for i in range(10):
        coord.incremental.add_chunk("users", f"chunk-{i}", i * 100, (i + 1) * 100)
    for i in range(3):
        coord.incremental.complete_chunk(f"chunk-{i}", rows_processed=100)

    assert coord.incremental.completed_units == 3
    assert coord.incremental.pending_units == 7
    assert len(coord.incremental.pending_obligations) == 7

    # CDC has pending event obligations
    coord.cdc.record_event_obligation("cdc-evt-1", "users", "0/2000000")
    assert len(coord.cdc.pending_obligations) == 1

    # Running boot_snapshot must NOT wipe incremental or CDC obligations
    coord.boot_snapshot(tables=["inventory", "catalog"])
    assert coord.snapshot.total_units == 2
    assert coord.incremental.completed_units == 3
    assert coord.incremental.pending_units == 7
    assert len(coord.incremental.pending_obligations) == 7
    assert len(coord.cdc.pending_obligations) == 1

    # Malicious or accidental request to wipe obligations must raise ObligationWipeError
    with pytest.raises(ObligationWipeError):
        coord.boot_snapshot(tables=["inventory"], allow_wipe_other_phases=True)


def test_phase_transition_invariants():
    coord = SnapshotWorkflowCoordinator()
    coord.boot_snapshot(tables=["t1", "t2"])

    # Attempting to transition from snapshot to CDC while snapshot has pending obligations
    # without transferring them must raise ObligationWipeError
    with pytest.raises(ObligationWipeError):
        coord.transition_phase(
            from_phase=SubworkflowType.SNAPSHOT,
            to_phase=SubworkflowType.CDC,
            transfer_obligations=False,
        )

    # Completing snapshot units allows clean transition
    coord.snapshot.mark_completed("t1", rows_processed=500, watermark="0/1500000")
    coord.snapshot.mark_completed("t2", rows_processed=500, watermark="0/1500000")
    coord.snapshot.set_watermark("high_watermark", "0/1500000")

    # Incompatible watermark: CDC starting behind snapshot watermark
    coord.cdc.advance_read("0/1000000")
    with pytest.raises(IncompatibleWatermarkError):
        coord.transition_phase(
            from_phase=SubworkflowType.SNAPSHOT,
            to_phase=SubworkflowType.CDC,
        )

    # Compatible watermark: CDC starting at or after snapshot watermark
    coord.cdc.advance_read("0/1500000")
    coord.transition_phase(
        from_phase=SubworkflowType.SNAPSHOT,
        to_phase=SubworkflowType.CDC,
    )
    assert coord.active_phase == SubworkflowType.CDC


def test_phase_transition_with_obligation_transfer():
    coord = SnapshotWorkflowCoordinator()
    coord.incremental.add_chunk("logs", "chk-1", 0, 100)
    coord.incremental.add_chunk("logs", "chk-2", 100, 200)

    # Transition incremental to CDC transferring obligations
    coord.transition_phase(
        from_phase=SubworkflowType.INCREMENTAL_SNAPSHOT,
        to_phase=SubworkflowType.CDC,
        transfer_obligations=True,
    )

    # Obligations transferred into CDC
    assert len(coord.incremental.pending_obligations) == 0
    assert len(coord.cdc.pending_obligations) == 2
    assert all(obl.phase == SubworkflowType.CDC for obl in coord.cdc.pending_obligations)


def test_durable_coordinator_save_and_recovery(tmp_path):
    coord = SnapshotWorkflowCoordinator(state_dir=tmp_path)

    coord.boot_snapshot(tables=["t_alpha", "t_beta"])
    coord.snapshot.mark_completed("t_alpha", rows_processed=1000)

    coord.incremental.add_chunk("t_gamma", "chk-01", 0, 500)
    coord.incremental.add_chunk("t_gamma", "chk-02", 500, 1000)
    coord.incremental.complete_chunk("chk-01", rows_processed=500)

    coord.cdc.advance_read("0/5000000")
    coord.cdc.record_event_obligation("evt-stream-1", "t_gamma", "0/5000100")

    coord.save()

    # Re-load from durable storage
    recovered = SnapshotWorkflowCoordinator.load(tmp_path)

    # Verify snapshot state
    assert recovered.snapshot.total_units == 2
    assert recovered.snapshot.completed_units == 1
    assert recovered.snapshot.pending_units == 1
    assert recovered.snapshot.coverage_pct == 50.0

    # Verify incremental state (50% complete)
    assert recovered.incremental.total_units == 2
    assert recovered.incremental.completed_units == 1
    assert recovered.incremental.pending_units == 1
    assert recovered.incremental.coverage_pct == 50.0

    # Verify CDC state
    assert recovered.cdc.read_position == "0/5000000"
    assert len(recovered.cdc.pending_obligations) == 1
    assert recovered.cdc.pending_obligations[0].obligation_id == "evt-stream-1"


def test_in_flight_units_reset_to_pending_on_restart():
    snap = SnapshotSubworkflow()
    snap.register_unit("u1", "t1")
    snap.register_unit("u2", "t1")
    snap.register_unit("u3", "t1")

    snap.mark_completed("u1")
    snap.mark_in_flight("u2")

    assert snap.units["u1"].status == WorkUnitStatus.COMPLETED
    assert snap.units["u2"].status == WorkUnitStatus.IN_FLIGHT
    assert snap.units["u3"].status == WorkUnitStatus.PENDING

    snap.restart()

    assert snap.units["u1"].status == WorkUnitStatus.COMPLETED
    assert snap.units["u2"].status == WorkUnitStatus.PENDING
    assert snap.units["u3"].status == WorkUnitStatus.PENDING
    assert snap.completed_units == 1
    assert snap.pending_units == 2


def test_coordinator_restart_mode_enforces_coverage():
    coord = SnapshotWorkflowCoordinator()
    for i in range(10):
        coord.snapshot.register_unit(f"t_{i}", target="db")

    for i in range(3):
        coord.snapshot.mark_completed(f"t_{i}")

    assert coord.snapshot.coverage_pct == 30.0

    coord.restart_mode(SubworkflowType.SNAPSHOT)
    assert coord.snapshot.coverage_pct == 30.0
    assert coord.snapshot.completed_units == 3
    assert coord.snapshot.pending_units == 7

    with pytest.raises(ShrinkingCoverageError):
        coord.restart_mode(SubworkflowType.SNAPSHOT, expected_units=["t_0", "t_1"])

    with pytest.raises(ValueError):
        coord.restart_mode("invalid_mode")


def test_watermark_comparisons_and_order():
    coord = SnapshotWorkflowCoordinator()
    coord.boot_snapshot(tables=["t1"])
    coord.snapshot.mark_completed("t1")
    coord.snapshot.set_watermark("high_watermark", 1000)

    # Behind: 900 < 1000
    coord.cdc.advance_read(900)
    with pytest.raises(IncompatibleWatermarkError):
        coord.transition_phase(SubworkflowType.SNAPSHOT, SubworkflowType.CDC)

    # Exactly matching: 1000 >= 1000
    coord.cdc.advance_read(1000)
    coord.transition_phase(SubworkflowType.SNAPSHOT, SubworkflowType.CDC)
    assert coord.active_phase == SubworkflowType.CDC


def test_failed_unit_recording():
    snap = SnapshotSubworkflow()
    snap.register_unit("err_unit", target="err_table")
    snap.mark_failed("err_unit", "connection timed out")

    assert snap.failed_units == 1
    assert snap.status == SubworkflowStatus.FAILED
    assert snap.units["err_unit"].error == "connection timed out"


def test_snapshot_subworkflow_rungs_under_watched():
    with watched() as seen:
        # 1. discharge_obligation_and_advance holds
        facts_ok = {
            "obligations_discharged": True,
            "coverage_non_shrinking": True,
            "watermark_monotonic": True,
        }
        c1 = decide_subworkflow_transition(facts_ok)
        assert c1.rung.name == "discharge_obligation_and_advance"

        # 2. discharge_obligation_and_advance refuses, retain_pending_subworkflow holds
        facts_pending = {
            "obligations_discharged": False,
            "coverage_non_shrinking": True,
            "watermark_monotonic": True,
        }
        c2 = decide_subworkflow_transition(facts_pending)
        assert c2.rung.name == "retain_pending_subworkflow"

        # 3. retain_pending_subworkflow refuses, refuse_phase_wipe holds
        facts_wipe = {
            "obligations_discharged": False,
            "coverage_non_shrinking": False,
            "watermark_monotonic": True,
        }
        c3 = decide_subworkflow_transition(facts_wipe)
        assert c3.rung.name == "refuse_phase_wipe"


