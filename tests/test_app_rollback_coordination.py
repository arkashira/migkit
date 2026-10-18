import pytest
from tests.rung_branches import watched
from migkit.rollback import (
    AppVersionSpec,
    SchemaEvolutionStep,
    ExpandContractPhase,
    RecoveryArtifact,
    RecoveryArtifactStore,
    CurrentWriteState,
    RollbackStrategy,
    AppRollbackCoordinator,
    generate_compensation_ddl,
    verify_compensation_ddl,
    decide_app_rollback_strategy,
    decide_compensation_strategy,
    app_rollback_strategy_rungs,
    compensation_strategy_rungs,
)

PROVES_RUNGS = [
    ("rollback", "app_rollback_strategy_rungs", "traffic_shift_without_schema_change", "holds"),
    ("rollback", "app_rollback_strategy_rungs", "traffic_shift_without_schema_change", "refuses"),
    ("rollback", "app_rollback_strategy_rungs", "write_preserving_reverse_migration", "holds"),
    ("rollback", "app_rollback_strategy_rungs", "write_preserving_reverse_migration", "refuses"),
    ("rollback", "app_rollback_strategy_rungs", "forward_fix_compensation_ddl", "holds"),
    ("rollback", "app_rollback_strategy_rungs", "forward_fix_compensation_ddl", "refuses"),
    ("rollback", "app_rollback_strategy_rungs", "expand_window_safe_suppression", "holds"),
    ("rollback", "app_rollback_strategy_rungs", "expand_window_safe_suppression", "refuses"),
    ("rollback", "app_rollback_strategy_rungs", "refuse_destructive_rollback", "holds"),
    ("rollback", "app_rollback_strategy_rungs", "refuse_destructive_rollback", "refuses"),
    ("rollback", "compensation_strategy_rungs", "drop_not_null_compensation", "holds"),
    ("rollback", "compensation_strategy_rungs", "drop_not_null_compensation", "refuses"),
    ("rollback", "compensation_strategy_rungs", "set_default_compensation", "holds"),
    ("rollback", "compensation_strategy_rungs", "set_default_compensation", "refuses"),
    ("rollback", "compensation_strategy_rungs", "widen_column_compensation", "holds"),
    ("rollback", "compensation_strategy_rungs", "widen_column_compensation", "refuses"),
    ("rollback", "compensation_strategy_rungs", "retain_table_compensation", "holds"),
    ("rollback", "compensation_strategy_rungs", "retain_table_compensation", "refuses"),
    ("rollback", "compensation_strategy_rungs", "refuse_destructive_compensation", "holds"),
    ("rollback", "compensation_strategy_rungs", "refuse_destructive_compensation", "refuses"),
]


def test_recovery_artifacts_independent_of_git(tmp_path):
    store_file = tmp_path / "recovery_artifacts.json"
    store = RecoveryArtifactStore(storage_path=store_file)

    step = SchemaEvolutionStep(
        step_id="20261017_01_users",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.EXPAND,
        forward_ddl="ALTER TABLE users ADD COLUMN loyalty_tier VARCHAR(32) DEFAULT 'bronze';",
        reverse_ddl="ALTER TABLE users DROP COLUMN loyalty_tier;",
        affected_tables=("users",),
        affected_columns={"users": ("loyalty_tier",)},
    )

    artifact = store.record_evolution_step(step)
    assert artifact.step_id == "20261017_01_users"
    assert artifact.from_schema_version == "v1"
    assert artifact.to_schema_version == "v2"
    assert artifact.checksum is not None
    assert artifact.verify_checksum() is True

    # Verify immutability and persistence across simulated git purge
    store.save_to_disk()
    del store

    new_store = RecoveryArtifactStore(storage_path=store_file)
    new_store.load_from_disk()
    loaded = new_store.get("20261017_01_users")
    assert loaded is not None
    assert loaded.reverse_ddl == "ALTER TABLE users DROP COLUMN loyalty_tier;"
    assert loaded.checksum == artifact.checksum
    assert loaded.verify_checksum() is True


def test_expand_contract_window_modeling():
    coord = AppRollbackCoordinator()
    coord.register_app_version(
        AppVersionSpec(
            version_id="app-v1",
            supported_schema_versions={"v1", "v2-expand"},
            required_columns={"users": {"id", "email"}},
        )
    )
    coord.register_app_version(
        AppVersionSpec(
            version_id="app-v2",
            supported_schema_versions={"v2-expand", "v2-contract"},
            required_columns={"users": {"id", "email", "loyalty_tier"}},
        )
    )

    assert coord.is_app_compatible("app-v1", "v2-expand")[0] is True
    assert coord.is_app_compatible("app-v2", "v2-expand")[0] is True

    assert coord.is_app_compatible("app-v1", "v2-contract")[0] is False
    assert coord.is_app_compatible("app-v2", "v2-contract")[0] is True


def test_decision_reverse_migration_when_zero_writes():
    store = RecoveryArtifactStore()
    coord = AppRollbackCoordinator(artifact_store=store)

    step = SchemaEvolutionStep(
        step_id="step_add_col",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.EXPAND,
        forward_ddl="ALTER TABLE accounts ADD COLUMN flags INT DEFAULT 0;",
        reverse_ddl="ALTER TABLE accounts DROP COLUMN flags;",
        affected_tables=("accounts",),
        affected_columns={"accounts": ("flags",)},
    )
    store.record_evolution_step(step)

    coord.register_app_version(AppVersionSpec("v1", supported_schema_versions={"v1"}))
    coord.register_app_version(AppVersionSpec("v2", supported_schema_versions={"v1", "v2"}))

    write_state = CurrentWriteState(
        table_writes={"accounts": 0},
        column_writes={"accounts": {"flags": 0}},
        has_new_version_writes=False,
    )

    decision = coord.evaluate_rollback(
        current_app_version="v2",
        target_app_version="v1",
        current_schema_version="v2",
        target_schema_version="v1",
        write_state=write_state,
    )

    assert decision.strategy == RollbackStrategy.REVERSE_MIGRATION
    assert decision.allowed is True
    assert "DROP COLUMN flags" in decision.ddl_script
    assert decision.badge in ("PASS", "BENIGN")


def test_decision_forward_fix_when_new_writes_exist():
    store = RecoveryArtifactStore()
    coord = AppRollbackCoordinator(artifact_store=store)

    step = SchemaEvolutionStep(
        step_id="step_add_col",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.EXPAND,
        forward_ddl="ALTER TABLE accounts ADD COLUMN flags INT DEFAULT 0;",
        reverse_ddl="ALTER TABLE accounts DROP COLUMN flags;",
        affected_tables=("accounts",),
        affected_columns={"accounts": ("flags",)},
    )
    store.record_evolution_step(step)

    coord.register_app_version(AppVersionSpec("v1", supported_schema_versions={"v1"}))
    coord.register_app_version(AppVersionSpec("v2", supported_schema_versions={"v1", "v2"}))

    write_state = CurrentWriteState(
        table_writes={"accounts": 42},
        column_writes={"accounts": {"flags": 42}},
        has_new_version_writes=True,
    )

    decision = coord.evaluate_rollback(
        current_app_version="v2",
        target_app_version="v1",
        current_schema_version="v2",
        target_schema_version="v1",
        write_state=write_state,
    )

    assert decision.strategy == RollbackStrategy.FORWARD_FIX
    assert decision.allowed is True
    assert "DROP COLUMN flags" not in decision.ddl_script
    assert len(decision.warnings) > 0
    assert decision.badge == "REVIEW"


def test_refuse_incompatible_app_rollback_before_traffic_shifts():
    coord = AppRollbackCoordinator()
    coord.register_app_version(
        AppVersionSpec(
            version_id="v1",
            supported_schema_versions={"v1"},
            required_columns={"orders": {"id", "legacy_status"}},
        )
    )

    write_state = CurrentWriteState(
        table_writes={"orders": 100},
        has_new_version_writes=True,
    )

    can_shift, reason = coord.can_shift_traffic("v1", "v2")
    assert can_shift is False

    decision = coord.evaluate_rollback(
        current_app_version="v2",
        target_app_version="v1",
        current_schema_version="v2",
        target_schema_version="v1",
        write_state=write_state,
    )

    assert decision.strategy == RollbackStrategy.REFUSE_ROLLBACK
    assert decision.allowed is False
    assert decision.badge == "ACTION"
    assert any("traffic" in r.lower() or "incompatible" in r.lower() for r in decision.refusal_reasons)


def test_preserve_new_writes_through_proved_reverse_path():
    store = RecoveryArtifactStore()
    coord = AppRollbackCoordinator(artifact_store=store)

    step = SchemaEvolutionStep(
        step_id="step_dual_column",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.EXPAND,
        forward_ddl="ALTER TABLE users ADD COLUMN new_meta JSONB;",
        reverse_ddl="UPDATE users SET old_meta = new_meta::text WHERE new_meta IS NOT NULL; ALTER TABLE users DROP COLUMN new_meta;",
        affected_tables=("users",),
        affected_columns={"users": ("new_meta",)},
    )
    store.record_evolution_step(step, proved_reverse_preserves_writes=True)

    coord.register_app_version(AppVersionSpec("v1", supported_schema_versions={"v1"}))
    coord.register_app_version(AppVersionSpec("v2", supported_schema_versions={"v1", "v2"}))

    write_state = CurrentWriteState(
        table_writes={"users": 15},
        column_writes={"users": {"new_meta": 15}},
        has_new_version_writes=True,
    )

    decision = coord.evaluate_rollback(
        current_app_version="v2",
        target_app_version="v1",
        current_schema_version="v2",
        target_schema_version="v1",
        write_state=write_state,
    )

    assert decision.strategy == RollbackStrategy.REVERSE_MIGRATION
    assert decision.allowed is True
    assert "UPDATE users SET old_meta" in decision.ddl_script


def test_verified_compensation_ddl_generation():
    step = SchemaEvolutionStep(
        step_id="step_add_not_null",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.CONTRACT,
        forward_ddl="ALTER TABLE invoices ADD COLUMN tax_code VARCHAR(32) NOT NULL;",
        reverse_ddl="ALTER TABLE invoices DROP COLUMN tax_code;",
        affected_tables=("invoices",),
        affected_columns={"invoices": ("tax_code",)},
    )

    target_app = AppVersionSpec("v1", required_columns={"invoices": {"id"}})
    compensation = generate_compensation_ddl(step, current_schema_version="v2", target_app_spec=target_app, dialect="postgres")

    assert "tax_code" in compensation
    assert "DROP NOT NULL" in compensation or "SET DEFAULT" in compensation

    valid, violations = verify_compensation_ddl(compensation, dialect="postgres")
    assert valid is True
    assert len(violations) == 0

    unsafe_ddl = "DROP TABLE invoices;"
    valid_unsafe, violations_unsafe = verify_compensation_ddl(unsafe_ddl, dialect="postgres")
    assert valid_unsafe is False
    assert any("drop table" in v.lower() for v in violations_unsafe)


def test_manifest_export_import():
    store = RecoveryArtifactStore()
    artifact = RecoveryArtifact(
        artifact_id="art-101",
        step_id="step-101",
        from_schema_version="v1",
        to_schema_version="v2",
        forward_ddl="CREATE TABLE audit_log (id INT);",
        reverse_ddl="DROP TABLE audit_log;",
        compensation_ddl="",
        phase="EXPAND",
        checksum="hash101",
    )
    store.add(artifact)

    manifest = store.export_manifest()
    new_store = RecoveryArtifactStore()
    new_store.import_manifest(manifest)

    fetched = new_store.get("step-101")
    assert fetched is not None
    assert fetched.artifact_id == "art-101"
    assert fetched.checksum == "hash101"


def test_git_commit_state_ignored_in_favor_of_current_writes():
    store = RecoveryArtifactStore()
    coord = AppRollbackCoordinator(artifact_store=store)

    step = SchemaEvolutionStep(
        step_id="step_exp",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.EXPAND,
        forward_ddl="ALTER TABLE payments ADD COLUMN provider_ref VARCHAR(64);",
        reverse_ddl="ALTER TABLE payments DROP COLUMN provider_ref;",
        affected_tables=("payments",),
        affected_columns={"payments": ("provider_ref",)},
    )
    store.record_evolution_step(step)

    coord.register_app_version(AppVersionSpec("v1", supported_schema_versions={"v1"}))
    coord.register_app_version(AppVersionSpec("v2", supported_schema_versions={"v1", "v2"}))

    # Even if git was reverted to commit v1, live writes exist
    write_state_with_writes = CurrentWriteState(
        table_writes={"payments": 99},
        column_writes={"payments": {"provider_ref": 99}},
        has_new_version_writes=True,
    )

    decision_with_writes = coord.evaluate_rollback(
        current_app_version="v2",
        target_app_version="v1",
        current_schema_version="v2",
        target_schema_version="v1",
        write_state=write_state_with_writes,
    )
    assert decision_with_writes.strategy == RollbackStrategy.FORWARD_FIX

    # Conversely, even if git is on un-reverted v2, zero live writes allow clean reverse
    write_state_zero = CurrentWriteState(
        table_writes={"payments": 0},
        column_writes={"payments": {"provider_ref": 0}},
        has_new_version_writes=False,
    )
    decision_zero = coord.evaluate_rollback(
        current_app_version="v2",
        target_app_version="v1",
        current_schema_version="v2",
        target_schema_version="v1",
        write_state=write_state_zero,
    )
    assert decision_zero.strategy == RollbackStrategy.REVERSE_MIGRATION


def test_forbidden_columns_refuses_traffic():
    coord = AppRollbackCoordinator()
    step = SchemaEvolutionStep(
        step_id="step_forbidden",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.EXPAND,
        forward_ddl="ALTER TABLE telemetry ADD COLUMN sensitive_payload TEXT;",
        reverse_ddl="ALTER TABLE telemetry DROP COLUMN sensitive_payload;",
        affected_tables=("telemetry",),
        affected_columns={"telemetry": ("sensitive_payload",)},
    )
    coord.register_evolution_step(step)

    coord.register_app_version(
        AppVersionSpec(
            version_id="legacy-worker",
            supported_schema_versions={"v1"},
            forbidden_columns={"telemetry": {"sensitive_payload"}},
        )
    )

    compat, reason = coord.is_app_compatible("legacy-worker", "v2")
    assert compat is False
    assert "forbids" in reason.lower()


def test_irreversible_data_change_refused_without_compensation():
    store = RecoveryArtifactStore()
    coord = AppRollbackCoordinator(artifact_store=store)

    step = SchemaEvolutionStep(
        step_id="step_narrow_col",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.CONTRACT,
        forward_ddl="ALTER TABLE customers ALTER COLUMN notes TYPE VARCHAR(50);",
        reverse_ddl="ALTER TABLE customers ALTER COLUMN notes TYPE VARCHAR(255);",
        irreversible_data=True,
        irreversible_reason="truncates customer notes to 50 chars",
        affected_tables=("customers",),
        affected_columns={"customers": ("notes",)},
    )
    store.record_evolution_step(step)

    coord.register_app_version(AppVersionSpec("v1", supported_schema_versions={"v1"}))
    coord.register_app_version(AppVersionSpec("v2", supported_schema_versions={"v1", "v2"}))

    write_state = CurrentWriteState(has_new_version_writes=False)

    decision = coord.evaluate_rollback(
        current_app_version="v2",
        target_app_version="v1",
        current_schema_version="v2",
        target_schema_version="v1",
        write_state=write_state,
    )

    assert decision.strategy == RollbackStrategy.REFUSE_ROLLBACK
    assert decision.allowed is False
    assert any("irreversible" in r.lower() for r in decision.refusal_reasons)


def test_mysql_compensation_ddl_generation():
    step = SchemaEvolutionStep(
        step_id="step_mysql_nn",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.CONTRACT,
        forward_ddl="ALTER TABLE billing ADD COLUMN vat_id VARCHAR(64) NOT NULL;",
        reverse_ddl="ALTER TABLE billing DROP COLUMN vat_id;",
        affected_tables=("billing",),
        affected_columns={"billing": ("vat_id",)},
    )
    target_app = AppVersionSpec("v1")
    comp = generate_compensation_ddl(step, "v2", target_app, dialect="mysql")
    assert "MODIFY COLUMN vat_id" in comp
    assert "NULL" in comp

    valid, violations = verify_compensation_ddl(comp, dialect="mysql")
    assert valid is True
    assert len(violations) == 0


def test_unrelated_table_writes_do_not_block_safe_reverse():
    store = RecoveryArtifactStore()
    coord = AppRollbackCoordinator(artifact_store=store)

    step = SchemaEvolutionStep(
        step_id="step_isolated",
        from_version="v1",
        to_version="v2",
        phase=ExpandContractPhase.EXPAND,
        forward_ddl="ALTER TABLE isolated_tbl ADD COLUMN opt_tag VARCHAR(16);",
        reverse_ddl="ALTER TABLE isolated_tbl DROP COLUMN opt_tag;",
        affected_tables=("isolated_tbl",),
        affected_columns={"isolated_tbl": ("opt_tag",)},
    )
    store.record_evolution_step(step)

    coord.register_app_version(AppVersionSpec("v1", supported_schema_versions={"v1"}))
    coord.register_app_version(AppVersionSpec("v2", supported_schema_versions={"v1", "v2"}))

    # Live writes exist on an unrelated table, but zero on isolated_tbl
    write_state = CurrentWriteState(
        table_writes={"unrelated_tbl": 500, "isolated_tbl": 0},
        column_writes={"isolated_tbl": {"opt_tag": 0}},
        has_new_version_writes=True,
    )

    decision = coord.evaluate_rollback(
        current_app_version="v2",
        target_app_version="v1",
        current_schema_version="v2",
        target_schema_version="v1",
        write_state=write_state,
    )

    assert decision.strategy == RollbackStrategy.REVERSE_MIGRATION
    assert decision.allowed is True


def test_app_rollback_rungs_under_watched():
    with watched():
        # app_rollback_strategy_rungs
        # 1. traffic_shift_without_schema_change
        c1 = decide_app_rollback_strategy({"target_app_compatible": True, "has_schema_drift": False})
        assert c1.rung.name == "traffic_shift_without_schema_change"

        # 2. write_preserving_reverse_migration
        c2 = decide_app_rollback_strategy({"target_app_compatible": False, "reverse_ddl_available": True, "proved_reverse_preserves_writes": True})
        assert c2.rung.name == "write_preserving_reverse_migration"

        # 3. forward_fix_compensation_ddl
        c3 = decide_app_rollback_strategy({"target_app_compatible": False, "reverse_ddl_available": False, "compensation_available": True, "compensation_verified": True})
        assert c3.rung.name == "forward_fix_compensation_ddl"

        # 4. expand_window_safe_suppression
        c4 = decide_app_rollback_strategy({"target_app_compatible": False, "reverse_ddl_available": False, "compensation_available": False, "within_expand_window": True, "irreversible_data": False})
        assert c4.rung.name == "expand_window_safe_suppression"

        # 5. refuse_destructive_rollback
        c5 = decide_app_rollback_strategy({"target_app_compatible": False, "reverse_ddl_available": False, "compensation_available": False, "within_expand_window": False})
        assert c5.rung.name == "refuse_destructive_rollback"

        # 6. refuse final fallback
        c6 = decide_app_rollback_strategy({"target_app_compatible": False, "reverse_ddl_available": False, "compensation_available": False, "within_expand_window": False, "force_destructive_rollback": True})
        assert c6.rung is None

        # compensation_strategy_rungs
        # 1. drop_not_null_compensation
        k1 = decide_compensation_strategy({"has_not_null": True})
        assert k1.rung.name == "drop_not_null_compensation"

        # 2. set_default_compensation
        k2 = decide_compensation_strategy({"has_not_null": False, "has_default": True})
        assert k2.rung.name == "set_default_compensation"

        # 3. widen_column_compensation
        k3 = decide_compensation_strategy({"has_not_null": False, "has_default": False, "widen_type": True})
        assert k3.rung.name == "widen_column_compensation"

        # 4. retain_table_compensation
        k4 = decide_compensation_strategy({"has_not_null": False, "has_default": False, "widen_type": False, "affected_tables": ("tbl",)})
        assert k4.rung.name == "retain_table_compensation"

        # 5. refuse_destructive_compensation
        k5 = decide_compensation_strategy({"has_not_null": False, "has_default": False, "widen_type": False, "affected_tables": ()})
        assert k5.rung.name == "refuse_destructive_compensation"

        # 6. refuse final fallback
        k6 = decide_compensation_strategy({"has_not_null": False, "has_default": False, "widen_type": False, "affected_tables": (), "allow_destructive": True})
        assert k6.rung is None


def test_observe_live_writes_and_appversions_integration():
    class DummyEngine:
        def read_table_stats(self, tables):
            return {"users": 42}
        def current_lsn(self):
            return "0/16B3740"

    state = CurrentWriteState.from_engine(DummyEngine(), ["users"])
    assert state.has_writes_in("users") is True
    assert state.highest_lsn_or_marker == "0/16B3740"

    src_hist = {"flyway": {"version": "v1"}}
    dst_hist = {"flyway": {"version": "v1"}}
    coord = AppRollbackCoordinator.from_migration_history(src_hist, dst_hist)
    assert "flyway" in coord._app_versions

    from migkit.rollback_set import Effect, EffectDependencyDAG, RollbackSetResolver
    dag = EffectDependencyDAG()
    dag.add_effect(Effect(effect_id="eff1", target_entity="tbl", operation_type="DDL"))
    resolver = RollbackSetResolver(dag)
    effects = coord.resolve_dependent_effects(["eff1"], resolver=resolver)
    assert effects is not None
    assert "eff1" in effects.target_effect_ids
