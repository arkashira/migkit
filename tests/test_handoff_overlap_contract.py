import json
import os
import tempfile
import pytest
from migkit.handoff_overlap import (
    HandoffContract,
    HandoffManager,
    SnapshotLifetime,
    CaptureFences,
    DedupIdentity,
    StreamEvent,
    HandoffGapError,
    HandoffFencingError,
    HandoffContractExpiredError,
    compare_source_and_target_history,
    decide_handoff_overlap,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("handoff_overlap", "handoff_overlap_rungs", "certify_gap_free_handoff", "holds"),
    ("handoff_overlap", "handoff_overlap_rungs", "certify_gap_free_handoff", "refuses"),
    ("handoff_overlap", "handoff_overlap_rungs", "reconcile_and_dedup_overlap", "holds"),
    ("handoff_overlap", "handoff_overlap_rungs", "reconcile_and_dedup_overlap", "refuses"),
    ("handoff_overlap", "handoff_overlap_rungs", "restore_checkpoint_and_reapply", "holds"),
    ("handoff_overlap", "handoff_overlap_rungs", "restore_checkpoint_and_reapply", "refuses"),
    ("handoff_overlap", "handoff_overlap_rungs", "extend_or_refresh_snapshot", "holds"),
    ("handoff_overlap", "handoff_overlap_rungs", "extend_or_refresh_snapshot", "refuses"),
    ("handoff_overlap", "handoff_overlap_rungs", "refuse_handoff_gap", "holds"),
]


def test_handoff_contract_serialization_and_atomic_persistence():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "contract.json")
        lifetime = SnapshotLifetime(
            snapshot_id="snap-pg-01",
            created_at=1700000000.0,
            status="completed",
            completed_at=1700000050.0,
            expires_at=1700003600.0,
        )
        fences = CaptureFences(
            lower_fence="0/1600",
            upper_fence="0/2400",
            engine="postgres",
        )
        dedup = DedupIdentity(
            identity_keys=["id"],
            is_keyless=False,
            track_multiplicity=True,
            strategy="commit_multiplicity",
        )
        contract = HandoffContract(
            contract_id="contract-001",
            source_db="prod_db",
            table="orders",
            snapshot_lifetime=lifetime,
            fences=fences,
            dedup_identity=dedup,
            applied_commits={"c1", "c2"},
            metadata={"priority": "high"},
        )
        contract.validate(now=1700000010.0)
        contract.save(path)

        assert os.path.exists(path)
        loaded = HandoffContract.load(path)
        assert loaded.contract_id == "contract-001"
        assert loaded.source_db == "prod_db"
        assert loaded.table == "orders"
        assert loaded.snapshot_lifetime.snapshot_id == "snap-pg-01"
        assert loaded.snapshot_lifetime.is_valid(now=1700000100.0)
        assert loaded.fences.lower_fence == "0/1600"
        assert loaded.fences.upper_fence == "0/2400"
        assert loaded.dedup_identity.identity_keys == ["id"]
        assert loaded.applied_commits == {"c1", "c2"}
        assert loaded.metadata == {"priority": "high"}


def test_capture_fences_gap_detection_and_overlap():
    fences_pg = CaptureFences(lower_fence="0/2000", upper_fence="0/4000", engine="postgres")
    assert not fences_pg.has_gap("0/1500")
    assert not fences_pg.has_gap("0/2000")
    assert fences_pg.has_gap("0/2500")
    assert fences_pg.is_in_overlap("0/2000")
    assert fences_pg.is_in_overlap("0/3000")
    assert fences_pg.is_in_overlap("0/4000")
    assert not fences_pg.is_in_overlap("0/1900")
    assert not fences_pg.is_in_overlap("0/4100")

    fences_num = CaptureFences(lower_fence=100, upper_fence=200, engine="generic")
    assert not fences_num.has_gap(90)
    assert not fences_num.has_gap(100)
    assert fences_num.has_gap(101)
    assert fences_num.is_in_overlap(150)

    bad_fences = CaptureFences(lower_fence=500, upper_fence=400, engine="generic")
    contract = HandoffContract(
        contract_id="c_bad",
        source_db="db",
        table="tbl",
        snapshot_lifetime=SnapshotLifetime("s1", status="completed"),
        fences=bad_fences,
        dedup_identity=DedupIdentity(),
    )
    with pytest.raises(HandoffFencingError):
        contract.validate()


def test_snapshot_lifetime_validation_and_expiration():
    lifetime = SnapshotLifetime(
        snapshot_id="s_exp",
        created_at=100.0,
        status="completed",
        expires_at=200.0,
    )
    assert lifetime.is_valid(now=150.0)
    assert not lifetime.is_valid(now=250.0)

    contract = HandoffContract(
        contract_id="c_exp",
        source_db="db",
        table="tbl",
        snapshot_lifetime=lifetime,
        fences=CaptureFences(lower_fence=10, upper_fence=20),
        dedup_identity=DedupIdentity(),
    )
    with pytest.raises(HandoffContractExpiredError):
        contract.validate(now=250.0)


def test_handoff_overlap_event_reconciliation_gap_free():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_file = os.path.join(tmpdir, "handoff.json")
        manager = HandoffManager.create(
            contract_id="handoff-sync",
            source_db="ecommerce",
            table="payments",
            lower_fence="0/1000",
            engine="postgres",
            identity_keys=["id"],
            state_path=state_file,
        )

        manager.apply_snapshot_record({"id": 1, "amt": 50}, commit_id="c_snap_1")
        manager.apply_snapshot_record({"id": 2, "amt": 100}, commit_id="c_snap_2")
        manager.complete_snapshot(upper_fence="0/3000")

        manager.verify_stream_handoff(stream_start="0/1000")

        ev1 = StreamEvent(position="0/1500", commit_id="c_snap_1", op="INSERT", row={"id": 1, "amt": 50})
        ev2 = StreamEvent(position="0/2500", commit_id="c3", op="INSERT", row={"id": 3, "amt": 150})
        ev3 = StreamEvent(position="0/3500", commit_id="c4", op="UPDATE", row={"id": 2, "amt": 120})

        applied1 = manager.apply_stream_event(ev1)
        applied2 = manager.apply_stream_event(ev2)
        applied3 = manager.apply_stream_event(ev3)

        assert not applied1
        assert applied2
        assert applied3
        assert manager.target_rows[1]["amt"] == 50
        assert manager.target_rows[2]["amt"] == 120
        assert manager.target_rows[3]["amt"] == 150


def test_duplicate_multiplicity_preservation_for_keyless_and_multiset():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_file = os.path.join(tmpdir, "keyless_handoff.json")
        manager = HandoffManager.create(
            contract_id="keyless-events",
            source_db="telemetry",
            table="event_log",
            lower_fence=100,
            engine="generic",
            is_keyless=True,
            state_path=state_file,
        )

        manager.apply_snapshot_record({"event": "click", "ts": 1000}, commit_id="c10")
        manager.apply_snapshot_record({"event": "click", "ts": 1000}, commit_id="c11")
        manager.complete_snapshot(upper_fence=200)

        ev1 = StreamEvent(position=110, commit_id="c10", op="INSERT", row={"event": "click", "ts": 1000})
        ev2 = StreamEvent(position=120, commit_id="c11", op="INSERT", row={"event": "click", "ts": 1000})
        ev3 = StreamEvent(position=180, commit_id="c12", op="INSERT", row={"event": "click", "ts": 1000})

        manager.apply_stream_event(ev1)
        manager.apply_stream_event(ev2)
        manager.apply_stream_event(ev3)

        row_id = manager.contract.dedup_identity.compute_identity({"event": "click", "ts": 1000})
        assert manager.target_multiplicities[row_id] == 3


def test_copy_to_follow_gap_detected_and_rejected():
    manager = HandoffManager.create(
        contract_id="gap-check",
        source_db="db",
        table="users",
        lower_fence="0/2000",
        engine="postgres",
        identity_keys=["id"],
    )
    manager.complete_snapshot(upper_fence="0/3000")

    with pytest.raises(HandoffGapError) as exc_info:
        manager.verify_stream_handoff(stream_start="0/2500")
    assert "copy-to-follow gap" in str(exc_info.value)


def test_writers_running_throughout_seed_handoff_crash_restart():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_file = os.path.join(tmpdir, "resilient_handoff.json")

        manager = HandoffManager.create(
            contract_id="writers-stress",
            source_db="prod",
            table="inventory",
            lower_fence=100,
            engine="generic",
            identity_keys=["sku"],
            state_path=state_file,
        )

        all_source_commits = []

        for i in range(1, 6):
            cid = f"commit_seed_{i}"
            ev = StreamEvent(position=80 + i, commit_id=cid, op="INSERT", row={"sku": f"item_{i}", "qty": i * 10})
            all_source_commits.append(ev)
            manager.apply_snapshot_record(ev.row, commit_id=cid)

        overlap_events = []
        for i in range(6, 11):
            cid = f"commit_overlap_{i}"
            ev = StreamEvent(position=100 + i * 5, commit_id=cid, op="INSERT", row={"sku": f"item_{i}", "qty": i * 10})
            all_source_commits.append(ev)
            overlap_events.append(ev)

        manager.complete_snapshot(upper_fence=180)
        manager.verify_stream_handoff(stream_start=100)

        manager.apply_stream_event(overlap_events[0])
        manager.apply_stream_event(overlap_events[1])
        manager.save_checkpoint()

        manager.simulate_crash()
        del manager

        recovered_manager = HandoffManager.restore(state_file)
        assert recovered_manager.contract.fences.lower_fence == 100
        assert recovered_manager.contract.fences.upper_fence == 180

        for ev in overlap_events:
            recovered_manager.apply_stream_event(ev)

        for i in range(11, 16):
            cid = f"commit_follow_{i}"
            ev = StreamEvent(position=180 + i * 5, commit_id=cid, op="INSERT", row={"sku": f"item_{i}", "qty": i * 10})
            all_source_commits.append(ev)
            recovered_manager.apply_stream_event(ev)

        audit = compare_source_and_target_history(all_source_commits, recovered_manager)
        assert audit.status == "ok"
        assert audit.certified_gap_free is True
        assert audit.total_source_commits == len(all_source_commits)
        assert audit.applied_source_commits == len(all_source_commits)
        assert audit.missing_commits == []
        assert audit.dropped_intervals == []
        assert audit.multiplicity_divergences == {}
        assert len(recovered_manager.target_rows) == 15


def test_handoff_composite_keys_and_updates_deletes():
    manager = HandoffManager.create(
        contract_id="comp-keys",
        source_db="saas",
        table="tenant_users",
        lower_fence=100,
        engine="generic",
        identity_keys=["tenant_id", "user_id"],
    )
    manager.complete_snapshot(upper_fence=200)

    manager.apply_snapshot_record({"tenant_id": "t1", "user_id": "u1", "role": "viewer"}, commit_id="c1")
    assert manager.target_rows[("t1", "u1")]["role"] == "viewer"

    manager.apply_stream_event(
        StreamEvent(position=150, commit_id="c2", op="UPDATE", row={"tenant_id": "t1", "user_id": "u1", "role": "admin"})
    )
    assert manager.target_rows[("t1", "u1")]["role"] == "admin"

    manager.apply_stream_event(
        StreamEvent(position=250, commit_id="c3", op="DELETE", row={"tenant_id": "t1", "user_id": "u1", "role": "admin"})
    )
    assert ("t1", "u1") not in manager.target_rows


def test_audit_defect_detection_on_dropped_interval_or_multiplicity():
    manager = HandoffManager.create(
        contract_id="audit-fail",
        source_db="db",
        table="tbl",
        lower_fence=10,
        engine="generic",
        identity_keys=["id"],
    )
    manager.complete_snapshot(upper_fence=50)

    c1 = StreamEvent(position=5, commit_id="c1", op="INSERT", row={"id": 1, "v": "a"})
    c2 = StreamEvent(position=20, commit_id="c2", op="INSERT", row={"id": 2, "v": "b"})
    c3 = StreamEvent(position=35, commit_id="c3", op="INSERT", row={"id": 3, "v": "c"})

    manager.apply_stream_event(c1)
    manager.apply_stream_event(c3)

    audit = compare_source_and_target_history([c1, c2, c3], manager)
    assert audit.status == "defect"
    assert audit.certified_gap_free is False
    assert "c2" in audit.missing_commits
    assert (5, 20) in audit.dropped_intervals

    manager_mult = HandoffManager.create(
        contract_id="mult-diverge",
        source_db="db",
        table="tbl",
        lower_fence=10,
        engine="generic",
        is_keyless=True,
    )
    ev = StreamEvent(position=15, commit_id="c1", op="INSERT", row={"val": "x"}, multiplicity=1)
    manager_mult.apply_stream_event(ev)
    row_id = manager_mult.contract.dedup_identity.compute_identity(ev.row)
    manager_mult.target_multiplicities[row_id] = 5

    audit_mult = compare_source_and_target_history([ev], manager_mult)
    assert audit_mult.status == "defect"
    assert audit_mult.certified_gap_free is False
    assert audit_mult.defect_class == "multiplicity_divergence"


def test_repeated_crashes_and_resumes():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_file = os.path.join(tmpdir, "multi_crash.json")
        mgr = HandoffManager.create(
            contract_id="multi-crash",
            source_db="db",
            table="orders",
            lower_fence=100,
            engine="generic",
            identity_keys=["id"],
            state_path=state_file,
        )

        commits = []
        ev1 = StreamEvent(position=50, commit_id="c1", op="INSERT", row={"id": 1, "val": 10})
        commits.append(ev1)
        mgr.apply_snapshot_record(ev1.row, commit_id=ev1.commit_id)
        mgr.save_checkpoint()
        mgr.simulate_crash()
        del mgr

        mgr = HandoffManager.restore(state_file)
        ev2 = StreamEvent(position=80, commit_id="c2", op="INSERT", row={"id": 2, "val": 20})
        commits.append(ev2)
        mgr.apply_snapshot_record(ev2.row, commit_id=ev2.commit_id)
        mgr.complete_snapshot(upper_fence=200)

        ev3 = StreamEvent(position=120, commit_id="c3", op="INSERT", row={"id": 3, "val": 30})
        commits.append(ev3)
        mgr.apply_stream_event(ev3)
        mgr.save_checkpoint()
        mgr.simulate_crash()
        del mgr

        mgr = HandoffManager.restore(state_file)
        ev4 = StreamEvent(position=150, commit_id="c4", op="INSERT", row={"id": 4, "val": 40})
        commits.append(ev4)
        mgr.apply_stream_event(ev4)

        ev5 = StreamEvent(position=250, commit_id="c5", op="INSERT", row={"id": 5, "val": 50})
        commits.append(ev5)
        mgr.apply_stream_event(ev5)
        mgr.save_checkpoint()
        mgr.simulate_crash()
        del mgr

        # Final restore
        mgr = HandoffManager.restore(state_file)
        audit = compare_source_and_target_history(commits, mgr)
        assert audit.status == "ok"
        assert audit.certified_gap_free is True
        assert len(mgr.target_rows) == 5


def test_mysql_gtid_fences_and_overlap():
    uuid = "3E11FA47-71CA-11E1-9E33-C80AA9E29B45"
    fences = CaptureFences(
        lower_fence=f"{uuid}:1-100",
        upper_fence=f"{uuid}:1-200",
        engine="mysql",
    )
    assert not fences.has_gap(f"{uuid}:1-80")
    assert not fences.has_gap(f"{uuid}:1-100")
    assert fences.has_gap(f"{uuid}:1-120")


def test_handoff_overlap_rungs_under_watched():
    with watched() as seen:
        # 1. certify_gap_free_handoff holds
        f1 = {
            "snapshot_valid": True,
            "fences_valid": True,
            "no_dropped_intervals": True,
            "multiplicities_match": True,
            "overlap_resolvable": True,
            "checkpoint_available": True,
            "can_restore_cleanly": True,
            "snapshot_refreshable": True,
        }
        c1 = decide_handoff_overlap(f1)
        assert c1.rung.name == "certify_gap_free_handoff"

        # 2. certify_gap_free_handoff refuses (multiplicities diverge), reconcile_and_dedup_overlap holds
        f2 = {
            "snapshot_valid": True,
            "fences_valid": True,
            "no_dropped_intervals": False,
            "multiplicities_match": False,
            "overlap_resolvable": True,
            "checkpoint_available": True,
            "can_restore_cleanly": True,
            "snapshot_refreshable": True,
        }
        c2 = decide_handoff_overlap(f2)
        assert c2.rung.name == "reconcile_and_dedup_overlap"

        # 3. reconcile_and_dedup_overlap refuses, restore_checkpoint_and_reapply holds
        f3 = {
            "snapshot_valid": False,
            "fences_valid": False,
            "no_dropped_intervals": False,
            "multiplicities_match": False,
            "overlap_resolvable": False,
            "checkpoint_available": True,
            "can_restore_cleanly": True,
            "snapshot_refreshable": True,
        }
        c3 = decide_handoff_overlap(f3)
        assert c3.rung.name == "restore_checkpoint_and_reapply"

        # 4. restore_checkpoint_and_reapply refuses, extend_or_refresh_snapshot holds
        f4 = {
            "snapshot_valid": False,
            "fences_valid": False,
            "no_dropped_intervals": False,
            "multiplicities_match": False,
            "overlap_resolvable": False,
            "checkpoint_available": False,
            "can_restore_cleanly": False,
            "snapshot_refreshable": True,
        }
        c4 = decide_handoff_overlap(f4)
        assert c4.rung.name == "extend_or_refresh_snapshot"

        # 5. extend_or_refresh_snapshot refuses, refuse_handoff_gap holds
        f5 = {
            "snapshot_valid": False,
            "fences_valid": False,
            "no_dropped_intervals": False,
            "multiplicities_match": False,
            "overlap_resolvable": False,
            "checkpoint_available": False,
            "can_restore_cleanly": False,
            "snapshot_refreshable": False,
        }
        c5 = decide_handoff_overlap(f5)
        assert c5.rung.name == "refuse_handoff_gap"

