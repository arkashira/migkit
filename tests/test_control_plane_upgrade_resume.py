import json
import os
import shutil
import tempfile
from pathlib import Path
import pytest

from migkit.control_upgrade import (
    AdapterCheckpoint,
    DurableControlState,
    HopDefinition,
    InvariantViolationError,
    RecoveryReport,
    StateMigrator,
    StateSchemaVersion,
    UpgradeResult,
    UpgradeRollbackError,
    export_state,
    import_state,
    is_upgrade_in_progress,
    recover_interrupted_upgrade,
)


def test_schema_version_parse_and_ordering():
    assert StateSchemaVersion.V1 < StateSchemaVersion.V2 < StateSchemaVersion.V3
    assert StateSchemaVersion.parse("1") == StateSchemaVersion.V1
    assert StateSchemaVersion.parse("v2") == StateSchemaVersion.V2
    assert StateSchemaVersion.parse(3) == StateSchemaVersion.V3
    assert str(StateSchemaVersion.V2) == "v2"
    assert StateSchemaVersion.V2 >= StateSchemaVersion.V1
    assert StateSchemaVersion.V2 <= StateSchemaVersion.V3


def _sample_v1_state():
    return {
        "version": 1,
        "hops": {
            "hop-pg": {
                "source": "postgresql://127.0.0.1:5432/sourcedb",
                "target": "postgresql://127.0.0.1:5433/targetdb",
                "tables": ["orders", "customers"],
                "options": {"batch_size": 5000},
            }
        },
        "checkpoints": {
            "orders": {
                "adapter": "postgres",
                "last_pk": 54200,
                "lsn": "0/16B3748",
                "completed_rows": 54200,
                "checksum": "abc1234",
            },
            "customers": {
                "adapter": "postgres",
                "last_pk": 12000,
                "lsn": "0/16B3748",
                "completed_rows": 12000,
                "checksum": "def5678",
            },
        },
    }


def test_forward_migration_v1_to_v2_and_v3():
    migrator = StateMigrator()
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)

    v2_state = migrator.migrate_state(v1_state, StateSchemaVersion.V2)
    assert v2_state.schema_version == StateSchemaVersion.V2
    assert "hop-pg" in v2_state.hops
    assert v2_state.hops["hop-pg"].tables == ["orders", "customers"]
    assert "hop-pg:orders" in v2_state.checkpoints
    orders_cp = v2_state.checkpoints["hop-pg:orders"]
    assert orders_cp.position["last_pk"]["val"] == 54200
    assert orders_cp.position["lsn"] == "0/16B3748"

    v3_state = migrator.migrate_state(v2_state, StateSchemaVersion.V3)
    assert v3_state.schema_version == StateSchemaVersion.V3
    assert "hop-pg" in v3_state.hops
    assert v3_state.digest is not None
    assert "hop-pg:orders" in v3_state.checkpoints
    assert v3_state.checkpoints["hop-pg:orders"].position["last_pk"]["val"] == 54200


def test_backward_migration_v3_to_v2_and_v1_downgrade():
    migrator = StateMigrator()
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v3_state = migrator.migrate_state(v1_state, StateSchemaVersion.V3)

    v2_state = migrator.migrate_state(v3_state, StateSchemaVersion.V2)
    assert v2_state.schema_version == StateSchemaVersion.V2
    assert "hop-pg" in v2_state.hops
    assert v2_state.checkpoints["hop-pg:orders"].position["last_pk"]["val"] == 54200

    v1_downgraded = migrator.migrate_state(v2_state, StateSchemaVersion.V1)
    assert v1_downgraded.schema_version == StateSchemaVersion.V1
    assert "hop-pg" in v1_downgraded.hops
    assert "orders" in v1_downgraded.checkpoints or "hop-pg:orders" in v1_downgraded.checkpoints
    cp = (
        v1_downgraded.checkpoints.get("orders")
        or v1_downgraded.checkpoints.get("hop-pg:orders")
    )
    assert cp.position.get("last_pk") == 54200 or cp.position.get("last_pk", {}).get("val") == 54200


def test_staging_on_copy_preserves_original_on_failure(tmp_path):
    migrator = StateMigrator()
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v1_state.save_directory(state_dir)

    def faulty_transform(st):
        st.hops.clear()
        return st

    migrator.register_step(StateSchemaVersion.V1, StateSchemaVersion.V2, faulty_transform, lambda s: s)

    with pytest.raises(InvariantViolationError):
        migrator.migrate_directory(state_dir, StateSchemaVersion.V2)

    persisted = DurableControlState.load_directory(state_dir)
    assert persisted.schema_version == StateSchemaVersion.V1
    assert "hop-pg" in persisted.hops
    assert not any(state_dir.parent.glob(".staging.*"))


def test_invariant_validation_rejects_lost_resume_positions(tmp_path):
    migrator = StateMigrator()
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v1_state.save_directory(state_dir)

    def wiping_positions_transform(st):
        st.schema_version = StateSchemaVersion.V2
        for cp in st.checkpoints.values():
            cp.position.clear()
        return st

    migrator.register_step(StateSchemaVersion.V1, StateSchemaVersion.V2, wiping_positions_transform, lambda s: s)

    with pytest.raises(InvariantViolationError) as exc_info:
        migrator.migrate_directory(state_dir, StateSchemaVersion.V2)

    assert "resume position" in str(exc_info.value).lower()
    persisted = DurableControlState.load_directory(state_dir)
    assert persisted.schema_version == StateSchemaVersion.V1


def test_invariant_validation_table_scope_mismatch(tmp_path):
    migrator = StateMigrator()
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v1_state.save_directory(state_dir)

    def mutate_scope_transform(st):
        st.schema_version = StateSchemaVersion.V2
        st.hops["hop-pg"].tables = ["orders"]
        return st

    migrator.register_step(StateSchemaVersion.V1, StateSchemaVersion.V2, mutate_scope_transform, lambda s: s)

    with pytest.raises(InvariantViolationError) as exc_info:
        migrator.migrate_directory(state_dir, StateSchemaVersion.V2)

    assert "table scope mutated" in str(exc_info.value)
    persisted = DurableControlState.load_directory(state_dir)
    assert persisted.schema_version == StateSchemaVersion.V1


def test_atomic_swap_and_rollback(tmp_path):
    migrator = StateMigrator()
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v1_state.save_directory(state_dir)

    result = migrator.migrate_directory(state_dir, StateSchemaVersion.V2, keep_backup=True)
    assert result.success
    persisted = DurableControlState.load_directory(state_dir)
    assert persisted.schema_version == StateSchemaVersion.V2

    migrator.rollback_directory(state_dir, result.backup_dir)
    rolled_back = DurableControlState.load_directory(state_dir)
    assert rolled_back.schema_version == StateSchemaVersion.V1
    assert "hop-pg" in rolled_back.hops


def test_dry_run_migration_validates_without_swapping(tmp_path):
    migrator = StateMigrator()
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v1_state.save_directory(state_dir)

    result = migrator.migrate_directory(state_dir, StateSchemaVersion.V2, dry_run=True)
    assert result.success
    persisted = DurableControlState.load_directory(state_dir)
    assert persisted.schema_version == StateSchemaVersion.V1


def test_idempotent_directory_migration(tmp_path):
    migrator = StateMigrator()
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v1_state.save_directory(state_dir)

    migrator.migrate_directory(state_dir, StateSchemaVersion.V2)
    res2 = migrator.migrate_directory(state_dir, StateSchemaVersion.V2)
    assert res2.success
    assert res2.from_version == StateSchemaVersion.V2
    assert res2.to_version == StateSchemaVersion.V2


def test_interrupted_upgrade_during_staging_recovers_cleanly(tmp_path):
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v1_state.save_directory(state_dir)

    stray_staging = state_dir.parent / f"{state_dir.name}.staging.fake99"
    stray_staging.mkdir()
    (stray_staging / "corrupted.tmp").write_text("incomplete write")

    assert is_upgrade_in_progress(state_dir)

    report = recover_interrupted_upgrade(state_dir)
    assert report.recovered
    assert not stray_staging.exists()
    assert not is_upgrade_in_progress(state_dir)

    persisted = DurableControlState.load_directory(state_dir)
    assert persisted.schema_version == StateSchemaVersion.V1
    assert "hop-pg" in persisted.hops


def test_interrupted_upgrade_during_swap_recovers_to_consistent_state(tmp_path):
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)

    backup_dir = state_dir.parent / f"{state_dir.name}.backup.tok123"
    v1_state.save_directory(backup_dir)

    assert not state_dir.exists()
    assert is_upgrade_in_progress(state_dir)

    report = recover_interrupted_upgrade(state_dir)
    assert report.recovered
    assert state_dir.exists()
    assert not backup_dir.exists()

    persisted = DurableControlState.load_directory(state_dir)
    assert persisted.schema_version == StateSchemaVersion.V1
    assert "hop-pg" in persisted.hops


def test_crash_during_migration_step_leaves_active_untouched(tmp_path):
    migrator = StateMigrator()
    state_dir = tmp_path / "control_state"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)
    v1_state.save_directory(state_dir)

    class SimulatedCrashError(RuntimeError):
        pass

    def crashing_step(st):
        raise SimulatedCrashError("simulated crash during step")

    migrator.register_step(StateSchemaVersion.V1, StateSchemaVersion.V2, crashing_step)

    with pytest.raises(SimulatedCrashError):
        migrator.migrate_directory(state_dir, StateSchemaVersion.V2)

    persisted = DurableControlState.load_directory(state_dir)
    assert persisted.schema_version == StateSchemaVersion.V1
    assert not any(state_dir.parent.glob(f"{state_dir.name}.staging.*"))


def test_export_and_import_preserves_hops_and_positions(tmp_path):
    export_file = tmp_path / "migkit_control_export.json"
    target_dir = tmp_path / "imported_state"

    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)

    exported_path = export_state(v1_state, export_file)
    assert exported_path.exists()

    imported_state = import_state(exported_path, target_dir, target_version=StateSchemaVersion.V2)
    assert imported_state.schema_version == StateSchemaVersion.V2
    assert "hop-pg" in imported_state.hops
    assert imported_state.hops["hop-pg"].tables == ["orders", "customers"]
    assert "hop-pg:orders" in imported_state.checkpoints
    assert imported_state.checkpoints["hop-pg:orders"].position["last_pk"]["val"] == 54200


def test_export_with_target_version_conversion(tmp_path):
    export_file = tmp_path / "migkit_export_v3.json"
    v1_raw = _sample_v1_state()
    v1_state = DurableControlState.from_dict(v1_raw)

    exported_path = export_state(v1_state, export_file, target_version=StateSchemaVersion.V3)
    assert exported_path.exists()

    raw_export = json.loads(exported_path.read_text())
    assert raw_export["state"]["schema_version"] == 3
    assert raw_export["state"]["digest"] is not None


def test_adapter_checkpoint_types_postgres_mysql_mongo():
    migrator = StateMigrator()
    state = DurableControlState(
        schema_version=StateSchemaVersion.V1,
        hops={
            "hop-multi": HopDefinition(
                hop_id="hop-multi",
                source={"engine": "mysql", "host": "10.0.0.1", "port": 3306},
                target={"engine": "postgres", "host": "10.0.0.2", "port": 5432},
                tables=["events", "stream_docs"],
            )
        },
        checkpoints={
            "events": AdapterCheckpoint(
                hop_id="hop-multi",
                table="events",
                adapter_type="mysql",
                position={"binlog_file": "binlog.00042", "binlog_pos": 987654, "gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"},
                ranges_completed=[{"chunk_id": 1, "rows": 50000}],
            ),
            "stream_docs": AdapterCheckpoint(
                hop_id="hop-multi",
                table="stream_docs",
                adapter_type="mongo",
                position={"resume_token": "8265B390000000012B0229296E04", "cluster_time": 1729150000},
                ranges_completed=[],
            ),
        },
    )

    v3_state = migrator.migrate_state(state, StateSchemaVersion.V3)
    assert v3_state.schema_version == StateSchemaVersion.V3
    ev_cp = v3_state.checkpoints["hop-multi:events"]
    assert ev_cp.adapter_type == "mysql"
    assert ev_cp.position["binlog_pos"] == 987654
    assert ev_cp.position["gtid"] == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"

    doc_cp = v3_state.checkpoints["hop-multi:stream_docs"]
    assert doc_cp.adapter_type == "mongo"
    assert doc_cp.position["resume_token"] == "8265B390000000012B0229296E04"

    v1_state = migrator.migrate_state(v3_state, StateSchemaVersion.V1)
    assert v1_state.schema_version == StateSchemaVersion.V1
    ev_cp_v1 = v1_state.checkpoints.get("events") or v1_state.checkpoints.get("hop-multi:events")
    assert ev_cp_v1.position["binlog_pos"] == 987654


def test_full_roundtrip_v1_to_v3_to_v1_preserves_identity_and_positions():
    migrator = StateMigrator()
    v1_raw = {
        "version": 1,
        "hops": {
            "hop-sync": {
                "source": "postgresql://127.0.0.1:5432/src",
                "target": "postgresql://127.0.0.1:5433/dst",
                "tables": ["inventory", "audit_log"],
                "options": {"cdc": "wal2json"},
                "transforms": {"audit_log": {"mask": ["token"]}},
            }
        },
        "checkpoints": {
            "inventory": {
                "adapter": "postgres",
                "last_pk": 99000,
                "lsn": "0/1A2B3C4",
                "completed_rows": 99000,
                "checksum": "chk99",
            },
            "audit_log": {
                "adapter": "postgres",
                "last_pk": 500,
                "lsn": "0/1A2B3C4",
                "completed_rows": 500,
                "checksum": "chk50",
            },
        },
    }

    s1 = DurableControlState.from_dict(v1_raw)
    s2 = migrator.migrate_state(s1, StateSchemaVersion.V2)
    s3 = migrator.migrate_state(s2, StateSchemaVersion.V3)
    s2_down = migrator.migrate_state(s3, StateSchemaVersion.V2)
    s1_down = migrator.migrate_state(s2_down, StateSchemaVersion.V1)

    assert s1_down.schema_version == StateSchemaVersion.V1
    assert "hop-sync" in s1_down.hops
    hop = s1_down.hops["hop-sync"]
    assert hop.tables == ["inventory", "audit_log"]
    assert hop.transforms == {"audit_log": {"mask": ["token"]}}
    inv_cp = s1_down.checkpoints.get("inventory") or s1_down.checkpoints.get("hop-sync:inventory")
    assert inv_cp.position.get("last_pk") == 99000 or inv_cp.position.get("last_pk", {}).get("val") == 99000
    assert inv_cp.position.get("lsn") == "0/1A2B3C4"
