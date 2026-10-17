"""Tests for composed mapping proof decide rungs, unswallowed predicate error, durable state, and movers/tailctl wiring (Row 625: RCH17).

Proves that:
1. mapping_proof_rungs covers all 7 branches under watched().
2. Predicate exceptions in FilterStage raise MappingPredicateEvaluationError with CareBadge.ACTION.
3. Durable CheckpointState persists and recovers via migkit.durable.write_json / read_state.
4. movers.verify_composed_bulk_load and tailctl.apply_unit invoke mapping proof.
"""
from __future__ import annotations

from pathlib import Path
import pytest

from migkit import movers, tailctl
from migkit.mapping_proof import (
    CareBadge,
    CDCChangeEvent,
    CDCExecutionEngine,
    ChangeOp,
    CheckpointState,
    EmbeddingStage,
    FilterStage,
    LossinessCategory,
    MappingDataflow,
    MappingPredicateEvaluationError,
    MappingProofEngine,
    MaskStage,
    MultiplicityKind,
    RenameStage,
    TransformStage,
    decide_mapping_proof,
    mapping_proof_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("mapping_proof", "mapping_proof_rungs", "lossless_bijection_proof", "holds"),
    ("mapping_proof", "mapping_proof_rungs", "lossless_bijection_proof", "refuses"),
    ("mapping_proof", "mapping_proof_rungs", "governed_lossy_transformation_proof", "holds"),
    ("mapping_proof", "mapping_proof_rungs", "governed_lossy_transformation_proof", "refuses"),
    ("mapping_proof", "mapping_proof_rungs", "denormalized_embedding_proof", "holds"),
    ("mapping_proof", "mapping_proof_rungs", "denormalized_embedding_proof", "refuses"),
    ("mapping_proof", "mapping_proof_rungs", "snapshot_only_fallback_proof", "holds"),
]


def test_mapping_proof_rungs_all_branches_under_watched():
    with watched() as seen:
        # 1. 100% fidelity, lossless transforms, cdc compatible -> lossless_bijection holds
        c1 = decide_mapping_proof({
            "schema_fidelity_pct": 100.0,
            "has_lossless_transforms": True,
            "is_cdc_compatible": True,
        })
        assert c1.path == "lossless_bijection_proof"

        # 2. Governed lossy transforms (95% coverage, safe coercion) -> lossless refuses, governed holds
        c2 = decide_mapping_proof({
            "schema_fidelity_pct": 95.0,
            "has_lossless_transforms": False,
            "is_transformation_safe": True,
            "is_cdc_compatible": True,
        })
        assert c2.path == "governed_lossy_transformation_proof"

        # 3. Denormalized parent-child embedding (85% coverage, unsafe coercion) -> governed refuses, denormalized holds
        c3 = decide_mapping_proof({
            "schema_fidelity_pct": 85.0,
            "has_lossless_transforms": False,
            "is_transformation_safe": False,
            "is_cdc_compatible": True,
        })
        assert c3.path == "denormalized_embedding_proof"

        # 4. Incompatible CDC streaming (e.g. batch-only snapshot) -> all refuse, snapshot_only fallback holds
        c4 = decide_mapping_proof({
            "schema_fidelity_pct": 50.0,
            "has_lossless_transforms": False,
            "is_transformation_safe": False,
            "is_cdc_compatible": False,
        })
        assert c4.path == "snapshot_only_fallback_proof"

    assert set(branch(seen, "lossless_bijection_proof")) == {HOLDS, REFUSES}
    assert set(branch(seen, "governed_lossy_transformation_proof")) == {HOLDS, REFUSES}
    assert set(branch(seen, "denormalized_embedding_proof")) == {HOLDS, REFUSES}
    assert branch(seen, "snapshot_only_fallback_proof") == [HOLDS]


def test_swallowed_predicate_error_raises_action_badge():
    """Verify that predicate evaluation error raises MappingPredicateEvaluationError with ACTION badge."""
    def _faulty_predicate(row):
        if row.get("trigger_fault"):
            raise ZeroDivisionError("predicate integer division by zero")
        return bool(row.get("active"))

    filter_stage = FilterStage(
        stage_id="filter-fault",
        predicate=_faulty_predicate,
        fields_involved=["trigger_fault", "active"],
        description="Filter with deliberate fault injection",
    )

    # 1. matches() must raise MappingPredicateEvaluationError [ACTION], NOT return False
    with pytest.raises(MappingPredicateEvaluationError) as excinfo:
        filter_stage.matches({"trigger_fault": True})
    assert excinfo.value.badge == CareBadge.ACTION
    assert excinfo.value.stage_id == "filter-fault"
    assert "Predicate evaluation failed" in str(excinfo.value)

    # 2. apply() must propagate the error, preventing corrupt rows from being silently classed as excluded
    with pytest.raises(MappingPredicateEvaluationError):
        filter_stage.apply([{"trigger_fault": True}])

    # 3. MappingDataflow.execute_load must propagate the error
    df = MappingDataflow(
        dataflow_id="test-fault-df",
        version="v1.0",
        target_entity="target_tbl",
        target_key_cols=["id"],
        source_entities=["tbl"],
    )
    df.add_stage(filter_stage)
    with pytest.raises(MappingPredicateEvaluationError):
        df.execute_load({"tbl": [{"id": 1, "trigger_fault": True}]})

    # 4. CDCExecutionEngine must propagate the error on incoming change event
    cdc = CDCExecutionEngine(df, {"tbl": []}, primary_entity_pk="id")
    with pytest.raises(MappingPredicateEvaluationError):
        cdc.apply_event(CDCChangeEvent(
            sequence_offset=1,
            entity="tbl",
            op=ChangeOp.INSERT,
            after={"id": 2, "trigger_fault": True},
        ))


def test_durable_checkpoint_state_atomic_io(tmp_path: Path):
    """Verify CheckpointState persistence via migkit.durable.write_json and read_state."""
    ckpt = CheckpointState(
        last_processed_offset=42,
        dataflow_checksum="sha256-abc12345",
        target_state={"101": {"id": 101, "name": "alice"}},
        source_snapshots={"users": {"101": {"id": 101, "raw_name": "alice"}}},
        event_count=1,
    )
    ckpt_file = tmp_path / "checkpoint.json"
    ckpt.save(ckpt_file)
    assert ckpt_file.exists()

    # Load back through durable reader
    loaded = CheckpointState.load(ckpt_file)
    assert loaded.last_processed_offset == 42
    assert loaded.dataflow_checksum == "sha256-abc12345"
    assert loaded.target_state["101"]["name"] == "alice"
    assert loaded.event_count == 1

    # Corrupt state file test
    ckpt_file.write_text("NOT_VALID_JSON{{{")
    with pytest.raises(SystemExit):  # migkit.durable.Torn
        CheckpointState.load(ckpt_file)


def test_movers_and_tailctl_wiring():
    """Verify wiring into movers bulk load and tailctl apply_unit."""
    df = MappingDataflow(
        dataflow_id="orders-stream",
        version="v2.0",
        target_entity="target_orders",
        target_key_cols=["id"],
        source_entities=["orders"],
    )
    df.add_stage(RenameStage(
        stage_id="rename-id",
        mapping={"order_num": "id", "amt": "amt"},
    ))

    # Movers bulk load verification
    source_data = {"orders": [{"order_num": 1, "amt": 50}]}
    target_data = [{"id": 1, "amt": 50}]
    res = movers.verify_composed_bulk_load(
        dataflow=df,
        source_tables=source_data,
        target_records=target_data,
    )
    assert res.is_verified() is True
    assert res.badge == CareBadge.PASS

    # Tailctl apply_unit wiring
    cdc_eng = CDCExecutionEngine(df, {"orders": []}, primary_entity_pk="order_num")
    class FakeHop:
        mapping_proof = cdc_eng
    class FakeEngine:
        hop = FakeHop()
        def apply_columns(self, side, db, cols):
            return 1

    applied_changes = []
    def fake_apply(batch):
        applied_changes.extend(batch)
        return len(batch)

    tailctl.apply_unit(
        eng=FakeEngine(),
        db="orders_db",
        at=100,
        changes=[
            {"table": "orders", "type": "insert", "row": {"order_num": 2, "amt": 99}},
        ],
        apply=fake_apply,
    )
    assert len(cdc_eng.get_target_state()) == 1
    assert cdc_eng.get_target_state()[0]["id"] == 2
