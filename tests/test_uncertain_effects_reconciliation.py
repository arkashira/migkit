"""Tests for reconciling effects at every external execution boundary (Row 600: SMART-N17.uncertain-effects).

Proves:
1. Every `decide.Rung` in `boundary_admission_rungs`, `reconciliation_strategy_rungs`,
   and `recovery_action_rungs` is exercised on both `holds` and `refuses` inside
   `with tests.rung_branches.watched():` and declared in `PROVES_RUNGS`.
2. Kill after effect but before acknowledgement on each boundary adapter:
   - External program adapter
   - Provider operation adapter
   - DDL schema boundary adapter
   - Bulk loader adapter
   - Checkpoint boundary adapter
3. No blind replay or duplicate side effects on any adapter.
4. Irreconcilable irreversible operations block their downstream dependency chain.
5. Provider idempotency tokens query provider state before repeating.
6. Epoch fencing rejects stale operation epochs.
7. Bounds derived from measured telemetry readings rather than hardcoded numbers.
8. 100% pure ASCII, care badges ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List, Optional

import pytest

import tests.rung_branches
from migkit import decide
from migkit.uncertain_effects import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    BoundaryEffectState,
    BoundaryOperationIdentity,
    BoundaryType,
    BulkLoaderBoundary,
    CareBadge,
    CheckpointBoundary,
    DdlBoundary,
    DependencyChainBlockedError,
    ExecutionBounds,
    ExternalProgramBoundary,
    FencedOutError,
    IrreconcilableEffectError,
    MeasuredExecutionTelemetry,
    PostconditionObservation,
    ProviderOperationBoundary,
    UncertainEffectsError,
    UncertainEffectsLedger,
    UnsafeBoundaryExecutionError,
    boundary_admission_rungs,
    execute_boundary_operation,
    extended_landed,
    reconciliation_strategy_rungs,
    recovery_action_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    # ladder: boundary_admission_rungs
    ("uncertain_effects", "boundary_admission_rungs", "skip_already_applied", "holds"),
    ("uncertain_effects", "boundary_admission_rungs", "skip_already_applied", "refuses"),
    ("uncertain_effects", "boundary_admission_rungs", "idempotent_provider_dispatch", "holds"),
    ("uncertain_effects", "boundary_admission_rungs", "idempotent_provider_dispatch", "refuses"),
    ("uncertain_effects", "boundary_admission_rungs", "fenced_atomic_boundary_dispatch", "holds"),
    ("uncertain_effects", "boundary_admission_rungs", "fenced_atomic_boundary_dispatch", "refuses"),
    ("uncertain_effects", "boundary_admission_rungs", "inspected_boundary_dispatch", "holds"),
    ("uncertain_effects", "boundary_admission_rungs", "inspected_boundary_dispatch", "refuses"),
    ("uncertain_effects", "boundary_admission_rungs", "refuse_unsafe_boundary", "holds"),
    ("uncertain_effects", "boundary_admission_rungs", "refuse_unsafe_boundary", "refuses"),

    # ladder: reconciliation_strategy_rungs
    ("uncertain_effects", "reconciliation_strategy_rungs", "token_reconciliation", "holds"),
    ("uncertain_effects", "reconciliation_strategy_rungs", "token_reconciliation", "refuses"),
    ("uncertain_effects", "reconciliation_strategy_rungs", "authoritative_postcondition_inspection", "holds"),
    ("uncertain_effects", "reconciliation_strategy_rungs", "authoritative_postcondition_inspection", "refuses"),
    ("uncertain_effects", "reconciliation_strategy_rungs", "compensating_reversion", "holds"),
    ("uncertain_effects", "reconciliation_strategy_rungs", "compensating_reversion", "refuses"),
    ("uncertain_effects", "reconciliation_strategy_rungs", "dependency_blocking", "holds"),
    ("uncertain_effects", "reconciliation_strategy_rungs", "dependency_blocking", "refuses"),

    # ladder: recovery_action_rungs
    ("uncertain_effects", "recovery_action_rungs", "acknowledge_applied", "holds"),
    ("uncertain_effects", "recovery_action_rungs", "acknowledge_applied", "refuses"),
    ("uncertain_effects", "recovery_action_rungs", "clean_replay", "holds"),
    ("uncertain_effects", "recovery_action_rungs", "clean_replay", "refuses"),
    ("uncertain_effects", "recovery_action_rungs", "reexecute_after_compensation", "holds"),
    ("uncertain_effects", "recovery_action_rungs", "reexecute_after_compensation", "refuses"),
    ("uncertain_effects", "recovery_action_rungs", "block_and_raise", "holds"),
    ("uncertain_effects", "recovery_action_rungs", "block_and_raise", "refuses"),
]


# =============================================================================
# 1. Rung Coverage Gate: Exercise holds and refuses on all ladders
# =============================================================================

def test_every_uncertain_effects_rung_holds_and_refuses():
    """Verify all 13 rungs across the 3 ladders reach both holds and refuses branches."""
    with watched() as seen:
        # --- Ladder 1: boundary_admission_rungs ---
        rungs_adm = boundary_admission_rungs()

        # skip_already_applied holds
        decide.climb("test:skip_holds", rungs_adm, {"observed_state": "applied"})
        # skip_already_applied refuses
        decide.climb("test:skip_refuses", rungs_adm, {"observed_state": "not started"})

        # idempotent_provider_dispatch holds
        decide.climb(
            "test:idemp_holds",
            rungs_adm,
            {"observed_state": "not started", "supports_idempotency_token": True},
        )
        # idempotent_provider_dispatch refuses
        decide.climb(
            "test:idemp_refuses",
            rungs_adm,
            {"observed_state": "not started", "supports_idempotency_token": False},
        )

        # fenced_atomic_boundary_dispatch holds
        decide.climb(
            "test:fence_holds",
            rungs_adm,
            {
                "observed_state": "not started",
                "supports_idempotency_token": False,
                "has_active_fence": True,
                "operation_epoch": 5,
                "current_fence_epoch": 4,
            },
        )
        # fenced_atomic_boundary_dispatch refuses
        decide.climb(
            "test:fence_refuses",
            rungs_adm,
            {
                "observed_state": "not started",
                "supports_idempotency_token": False,
                "has_active_fence": False,
            },
        )

        # inspected_boundary_dispatch holds
        decide.climb(
            "test:inspect_holds",
            rungs_adm,
            {
                "observed_state": "not started",
                "supports_idempotency_token": False,
                "has_active_fence": False,
                "has_postcondition_inspector": True,
            },
        )
        # inspected_boundary_dispatch refuses
        decide.climb(
            "test:inspect_refuses",
            rungs_adm,
            {
                "observed_state": "not started",
                "supports_idempotency_token": False,
                "has_active_fence": False,
                "has_postcondition_inspector": False,
            },
        )

        # refuse_unsafe_boundary holds
        decide.climb(
            "test:unsafe_holds",
            rungs_adm,
            {
                "observed_state": "not started",
                "supports_idempotency_token": False,
                "has_active_fence": False,
                "has_postcondition_inspector": False,
                "lacks_boundary_safety": True,
            },
        )
        # refuse_unsafe_boundary refuses
        decide.climb(
            "test:unsafe_refuses",
            rungs_adm,
            {
                "observed_state": "not started",
                "supports_idempotency_token": False,
                "has_active_fence": False,
                "has_postcondition_inspector": False,
                "lacks_boundary_safety": False,
            },
        )

        # --- Ladder 2: reconciliation_strategy_rungs ---
        rungs_strat = reconciliation_strategy_rungs()

        # token_reconciliation holds
        decide.climb(
            "test:strat_token_holds",
            rungs_strat,
            {"supports_idempotency_token": True, "has_idempotency_token": True},
        )
        # token_reconciliation refuses
        decide.climb(
            "test:strat_token_refuses",
            rungs_strat,
            {"supports_idempotency_token": False, "has_idempotency_token": False},
        )

        # authoritative_postcondition_inspection holds
        decide.climb(
            "test:strat_auth_holds",
            rungs_strat,
            {
                "supports_idempotency_token": False,
                "has_postcondition_inspector": True,
                "target_reachable": True,
            },
        )
        # authoritative_postcondition_inspection refuses
        decide.climb(
            "test:strat_auth_refuses",
            rungs_strat,
            {
                "supports_idempotency_token": False,
                "has_postcondition_inspector": False,
            },
        )

        # compensating_reversion holds
        decide.climb(
            "test:strat_comp_holds",
            rungs_strat,
            {
                "supports_idempotency_token": False,
                "has_postcondition_inspector": False,
                "observed_state": "partial",
                "has_compensating_action": True,
                "is_irreversible": False,
            },
        )
        # compensating_reversion refuses
        decide.climb(
            "test:strat_comp_refuses",
            rungs_strat,
            {
                "supports_idempotency_token": False,
                "has_postcondition_inspector": False,
                "observed_state": "partial",
                "has_compensating_action": False,
                "is_irreversible": True,
            },
        )

        # dependency_blocking holds
        decide.climb(
            "test:strat_block_holds",
            rungs_strat,
            {
                "supports_idempotency_token": False,
                "has_postcondition_inspector": False,
                "has_compensating_action": False,
                "is_irreconcilable": True,
            },
        )
        # dependency_blocking refuses
        decide.climb(
            "test:strat_block_refuses",
            rungs_strat,
            {
                "supports_idempotency_token": False,
                "has_postcondition_inspector": False,
                "has_compensating_action": False,
                "is_irreconcilable": False,
            },
        )

        # --- Ladder 3: recovery_action_rungs ---
        rungs_rec = recovery_action_rungs()

        # acknowledge_applied holds
        decide.climb("test:rec_ack_holds", rungs_rec, {"reconciled_state": "applied"})
        # acknowledge_applied refuses
        decide.climb("test:rec_ack_refuses", rungs_rec, {"reconciled_state": "unknown"})

        # clean_replay holds
        decide.climb(
            "test:rec_replay_holds",
            rungs_rec,
            {"reconciled_state": "not started", "tries_left": 2},
        )
        # clean_replay refuses
        decide.climb(
            "test:rec_replay_refuses",
            rungs_rec,
            {"reconciled_state": "not started", "tries_left": 0},
        )

        # reexecute_after_compensation holds
        decide.climb(
            "test:rec_comp_holds",
            rungs_rec,
            {
                "reconciled_state": "partial",
                "compensation_succeeded": True,
                "tries_left": 2,
            },
        )
        # reexecute_after_compensation refuses
        decide.climb(
            "test:rec_comp_refuses",
            rungs_rec,
            {
                "reconciled_state": "partial",
                "compensation_succeeded": False,
                "tries_left": 0,
            },
        )

        # block_and_raise holds
        decide.climb("test:rec_raise_holds", rungs_rec, {"must_block_chain": True})
        # block_and_raise refuses
        decide.climb("test:rec_raise_refuses", rungs_rec, {"must_block_chain": False})

    # Verify each declared rung in PROVES_RUNGS was reached in seen
    for mod, ladder, name, branch_type in PROVES_RUNGS:
        reached = branch(seen, name)
        assert branch_type in reached, (
            f"Rung {name} in ladder {ladder} did not reach branch {branch_type}. Reached: {reached}"
        )


# =============================================================================
# 2. Adapter Tests: Kill after effect but before acknowledgement
# =============================================================================

def test_external_program_adapter_kill_after_effect_before_ack():
    """External program executes and creates output file, but process crashes before returning ACK.

    Reconciliation inspects output artifact, confirms APPLIED, avoids blind replay.
    """
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        out_path = tf.name

    try:
        call_count = 0

        def run_program():
            nonlocal call_count
            call_count += 1
            # Step 1: Program writes output file (side effect lands)
            with open(out_path, "w") as f:
                f.write("DUMP_EXPORT_ROWS_5000\n")
            # Step 2: Simulated crash / kill before ACK packet/return
            raise ConnectionResetError("Process killed by SIGKILL before ack returned")

        adapter = ExternalProgramBoundary(
            target_endpoint="127.0.0.1:5432",
            program_name="pg_dump",
            args=["--schema=public", "--table=users"],
            artifact_path=out_path,
        )

        ledger = UncertainEffectsLedger()
        result = execute_boundary_operation(
            identity=adapter.identity,
            execute_fn=run_program,
            inspect_fn=adapter.inspect_postcondition,
            ledger=ledger,
        )

        # Result is the artifact size
        assert result == len("DUMP_EXPORT_ROWS_5000\n")
        # Proves program ran exactly once: no blind duplicate replay!
        assert call_count == 1
        rec = ledger.get_record(adapter.identity.key)
        assert rec is not None
        assert rec.status == "applied"
        assert rec.reconciled_state == BoundaryEffectState.APPLIED
        assert rec.care_badge == CareBadge.PASS
    finally:
        if os.path.exists(out_path):
            os.remove(out_path)


def test_provider_operation_adapter_idempotency_token_query():
    """Provider API operation (e.g. AWS DMS task start) drops connection after resource start.

    Reconciliation queries provider using idempotency token, confirms APPLIED, avoids duplicate.
    """
    call_count = 0
    token = "req-token-dms-start-98765"
    provider_resources: Dict[str, str] = {}

    def start_provider_task():
        nonlocal call_count
        call_count += 1
        # Provider registers token and starts task
        provider_resources[token] = "ACTIVE"
        # Network partition drops HTTP response before client sees it
        raise TimeoutError("Endpoint 10.0.0.1:443 timed out before HTTP 200 response")

    def token_query(req_token: str) -> Optional[BoundaryEffectState]:
        status = provider_resources.get(req_token)
        if status == "ACTIVE":
            return BoundaryEffectState.APPLIED
        return BoundaryEffectState.NOT_STARTED

    adapter = ProviderOperationBoundary(
        provider_name="aws_dms",
        operation_name="start_replication_task",
        resource_id="arn:aws:dms:us-east-1:dummy-account:task:MIGTASK001",
        idempotency_token=token,
    )

    ledger = UncertainEffectsLedger()
    result = execute_boundary_operation(
        identity=adapter.identity,
        execute_fn=start_provider_task,
        token_query_fn=token_query,
        ledger=ledger,
    )

    assert call_count == 1
    rec = ledger.get_record(adapter.identity.key)
    assert rec is not None
    assert rec.reconciled_state == BoundaryEffectState.APPLIED


def test_ddl_boundary_adapter_interrupted_before_ack():
    """DDL statement commits on database catalog, but connection drops before ACK.

    Catalog inspection discovers index exists, reconciles as APPLIED, avoids duplicate create failure.
    """
    call_count = 0
    db_catalog: Dict[str, str] = {}

    def execute_create_index():
        nonlocal call_count
        call_count += 1
        # DB catalog receives index
        db_catalog["idx_orders_customer"] = "INDEX orders(customer_id) VALID"
        # DB connection killed right after transaction commit
        raise RuntimeError("DB connection dropped before COMMIT ACK packet")

    def inspect_catalog() -> PostconditionObservation:
        idx_def = db_catalog.get("idx_orders_customer")
        if idx_def is not None:
            return PostconditionObservation(
                state=BoundaryEffectState.APPLIED,
                observed_value=idx_def,
                matched=True,
                details={"catalog_entry": idx_def},
                care_badge=CareBadge.PASS,
            )
        return PostconditionObservation(
            state=BoundaryEffectState.NOT_STARTED,
            care_badge=CareBadge.BENIGN,
        )

    adapter = DdlBoundary(
        target_endpoint="127.0.0.1:3306",
        ddl_statement="CREATE INDEX idx_orders_customer ON orders(customer_id)",
        schema_object_name="idx_orders_customer",
    )

    ledger = UncertainEffectsLedger()
    result = execute_boundary_operation(
        identity=adapter.identity,
        execute_fn=execute_create_index,
        inspect_fn=inspect_catalog,
        ledger=ledger,
    )

    assert result == "INDEX orders(customer_id) VALID"
    assert call_count == 1
    rec = ledger.get_record(adapter.identity.key)
    assert rec is not None
    assert rec.reconciled_state == BoundaryEffectState.APPLIED


def test_bulk_loader_adapter_kill_after_batch_before_ack():
    """Bulk loader batch loads 1000 rows into target table, worker killed before ACK.

    Postcondition inspector (extended_landed) detects 1000 rows landed.
    Reconciles as APPLIED without blind duplicate insert.
    """
    call_count = 0
    target_table_rows: List[Dict[str, Any]] = []

    def run_bulk_load():
        nonlocal call_count
        call_count += 1
        # 1000 rows inserted into target
        for i in range(1000):
            target_table_rows.append({"id": i, "batch": "b1"})
        # Worker process receives SIGTERM before writing checkpoint ack
        raise SystemExit(143)

    landed_check = extended_landed(
        want=lambda: 1000,
        held=lambda: len(target_table_rows),
        before=0,
    )

    adapter = BulkLoaderBoundary(
        target_endpoint="127.0.0.1:5432",
        table_name="orders",
        batch_id="batch_001",
        target_rows_count=1000,
        epoch=1,
    )

    ledger = UncertainEffectsLedger()
    result = execute_boundary_operation(
        identity=adapter.identity,
        execute_fn=run_bulk_load,
        inspect_fn=landed_check,
        ledger=ledger,
    )

    # 1000 rows held
    assert result == 1000
    # No duplicate replay: row count remains exactly 1000, NOT 2000!
    assert len(target_table_rows) == 1000
    assert call_count == 1


def test_checkpoint_boundary_adapter_interrupted_before_ack():
    """Checkpoint fence commit succeeds in state store, client drops before ack.

    Postcondition inspects fence epoch in ledger, reconciles as APPLIED.
    """
    call_count = 0
    state_store: Dict[str, int] = {}

    def commit_checkpoint():
        nonlocal call_count
        call_count += 1
        state_store["checkpoint:pg_to_ch"] = 42
        raise ConnectionAbortedError("Client socket closed before checkpoint ack")

    def inspect_state() -> PostconditionObservation:
        val = state_store.get("checkpoint:pg_to_ch")
        if val == 42:
            return PostconditionObservation(
                state=BoundaryEffectState.APPLIED,
                observed_value=42,
                matched=True,
                care_badge=CareBadge.PASS,
            )
        return PostconditionObservation(state=BoundaryEffectState.NOT_STARTED)

    adapter = CheckpointBoundary(
        target_endpoint="127.0.0.1:8123",
        hop_name="pg_to_ch",
        cursor_position="pos:42",
        epoch=42,
    )

    ledger = UncertainEffectsLedger()
    result = execute_boundary_operation(
        identity=adapter.identity,
        execute_fn=commit_checkpoint,
        inspect_fn=inspect_state,
        ledger=ledger,
    )

    assert result == 42
    assert call_count == 1
    assert state_store["checkpoint:pg_to_ch"] == 42


# =============================================================================
# 3. Partial Effect & Compensation vs Irreversibility
# =============================================================================

def test_bulk_loader_partial_effect_compensation_and_retry():
    """Bulk loader fails mid-batch leaving 300 rows (PARTIAL).

    Compensating action reverts 300 rows back to 0. Second attempt succeeds cleanly with 1000 rows.
    """
    call_count = 0
    target_table_rows: List[int] = []

    def execute_batch():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First attempt partially inserts 300 rows and fails
            target_table_rows.extend(range(300))
            raise RuntimeError("Disk IO error during streaming insert at row 300")
        # Second attempt completes full 1000 rows
        target_table_rows.extend(range(1000))
        return 1000

    def inspect_table() -> PostconditionObservation:
        cnt = len(target_table_rows)
        if cnt == 1000:
            return PostconditionObservation(state=BoundaryEffectState.APPLIED, observed_value=1000)
        if cnt == 0:
            return PostconditionObservation(state=BoundaryEffectState.NOT_STARTED, observed_value=0)
        return PostconditionObservation(state=BoundaryEffectState.PARTIAL, observed_value=cnt)

    def compensate_partial_batch() -> bool:
        # Revert partial rows
        target_table_rows.clear()
        return True

    identity = BoundaryOperationIdentity(
        boundary_type=BoundaryType.BULK_LOADER,
        target_endpoint="127.0.0.1:5432",
        operation_name="bulk_load:events",
        scope_id="events:chunk1",
        epoch=1,
        is_irreversible=False,
    )

    ledger = UncertainEffectsLedger()
    result = execute_boundary_operation(
        identity=identity,
        execute_fn=execute_batch,
        inspect_fn=inspect_table,
        compensate_fn=compensate_partial_batch,
        ledger=ledger,
    )

    assert result == 1000
    assert len(target_table_rows) == 1000
    assert call_count == 2
    rec = ledger.get_record(identity.key)
    assert rec is not None
    assert rec.reconciled_state == BoundaryEffectState.APPLIED


def test_irreversible_partial_effect_blocks_dependency_chain():
    """Irreversible operation (e.g. DROP COLUMN or external charge) leaves partial/unknown state.

    Execution raises IrreconcilableEffectError, refusing blind replay.
    Subsequent dependent operation is blocked with DependencyChainBlockedError.
    """
    call_count = 0

    def run_irreversible_operation():
        nonlocal call_count
        call_count += 1
        # Side effect occurs partially on endpoint
        raise RuntimeError("Failure occurred after dropping column without rollback capability")

    def inspect_state() -> PostconditionObservation:
        return PostconditionObservation(
            state=BoundaryEffectState.PARTIAL,
            details={"reason": "column dropped but dependent view corrupted"},
            care_badge=CareBadge.ACTION,
        )

    prereq_op = BoundaryOperationIdentity(
        boundary_type=BoundaryType.DDL,
        target_endpoint="10.0.0.2:3306",
        operation_name="ddl:drop_old_column",
        scope_id="users.legacy_col",
        epoch=1,
        is_irreversible=True,  # Crucial: irreversible!
    )

    ledger = UncertainEffectsLedger()

    # Step 1: Prerequisite operation must fail and refuse blind replay
    with pytest.raises(IrreconcilableEffectError) as exc_info:
        execute_boundary_operation(
            identity=prereq_op,
            execute_fn=run_irreversible_operation,
            inspect_fn=inspect_state,
            ledger=ledger,
        )

    assert "Irreconcilable effect" in str(exc_info.value)
    assert call_count == 1  # Exactly 1: no blind replay!

    # Step 2: Register dependent operation
    dependent_op = BoundaryOperationIdentity(
        boundary_type=BoundaryType.BULK_LOADER,
        target_endpoint="10.0.0.2:3306",
        operation_name="bulk_load:users",
        scope_id="users:batch1",
        epoch=1,
    )
    ledger.register_operation(dependent_op)
    ledger.add_dependency(dependent_op.key, prereq_op.key)

    # Step 3: Dependent operation MUST be blocked from running
    with pytest.raises(DependencyChainBlockedError) as dep_exc:
        execute_boundary_operation(
            identity=dependent_op,
            execute_fn=lambda: "should never run",
            ledger=ledger,
        )

    assert "blocked by prerequisite failure" in str(dep_exc.value)


# =============================================================================
# 4. Fencing & Epoch Safety
# =============================================================================

def test_epoch_fencing_rejects_stale_operation():
    """Target epoch fence has advanced beyond operation epoch. Refused with FencedOutError."""
    stale_identity = BoundaryOperationIdentity(
        boundary_type=BoundaryType.BULK_LOADER,
        target_endpoint="127.0.0.1:9042",
        operation_name="bulk_load:keyspace1.table1",
        scope_id="partition_1",
        epoch=2,  # Stale!
    )

    ledger = UncertainEffectsLedger()

    with pytest.raises(FencedOutError) as exc_info:
        execute_boundary_operation(
            identity=stale_identity,
            execute_fn=lambda: "should not run",
            current_fence_epoch=3,  # Target fence has advanced to 3!
            ledger=ledger,
        )

    assert "fenced out by active target fence 3" in str(exc_info.value)


# =============================================================================
# 5. Safety Refusal on Unfenced, Non-Idempotent, Uninspected Boundary
# =============================================================================

def test_refuse_unsafe_boundary():
    """Irreversible operation lacking idempotency, fencing, and inspection is refused."""
    unsafe_identity = BoundaryOperationIdentity(
        boundary_type=BoundaryType.EXTERNAL_PROGRAM,
        target_endpoint="10.0.0.5:22",
        operation_name="raw_remote_exec",
        scope_id="host",
        epoch=0,
        is_irreversible=True,
    )

    ledger = UncertainEffectsLedger()

    with pytest.raises(UnsafeBoundaryExecutionError) as exc:
        execute_boundary_operation(
            identity=unsafe_identity,
            execute_fn=lambda: "should not execute",
            inspect_fn=None,  # No inspection!
            ledger=ledger,
            current_fence_epoch=None,  # No fencing!
        )

    assert "Execution refused for unsafe boundary operation" in str(exc.value)


# =============================================================================
# 6. Measured Telemetry Bounds Derivation
# =============================================================================

def test_measured_execution_telemetry_derivation():
    """Telemetry readings derive timeout and retries empirically from mean + 3-sigma spread."""
    telemetry = MeasuredExecutionTelemetry(
        latency_samples_s=(0.020, 0.025, 0.030, 0.035, 0.040),
        failure_rate_samples=(0.05, 0.10, 0.12),
        target_timeout_s=5.0,
    )
    bounds = telemetry.derive_bounds()

    # Latency mean is 0.030, stdev is approx 0.0079
    assert isinstance(bounds, ExecutionBounds)
    assert 0.040 < bounds.timeout_s < 1.0
    assert bounds.max_retries >= 2
    assert bounds.base_backoff_s > 0.050


# =============================================================================
# 7. Formatting & ASCII Care Badges
# =============================================================================

def test_unix_formatting_and_care_badges():
    """Verify clean UNIX formatting and 4-tier care badges without glyphs/emojis."""
    obs = PostconditionObservation(
        state=BoundaryEffectState.APPLIED,
        observed_value=42,
        matched=True,
        care_badge=CareBadge.PASS,
    )
    line = obs.format_unix_line()
    assert line.startswith("[PASS]")
    assert "state=applied matched=1 val=42" in line
    # Must be 100% ASCII
    line.encode("ascii")

    identity = BoundaryOperationIdentity(
        boundary_type=BoundaryType.BULK_LOADER,
        target_endpoint="127.0.0.1:5432",
        operation_name="orders",
        scope_id="chunk0",
        epoch=1,
    )
    id_line = identity.format_unix_line()
    assert "type=bulk_loader target=127.0.0.1:5432" in id_line
    id_line.encode("ascii")
