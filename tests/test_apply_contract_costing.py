"""Tests for Row 612: RCH04.apply-contract (Cost transactional and batch apply under different contracts).

Proves:
1. Every decide.Rung in `apply_contract.apply_contract_rungs` and
   `apply_contract.exposure_gate_rungs` is exercised on both `holds` and
   `refuses` under `tests.rung_branches.watched()` and declared in
   `PROVES_RUNGS`.
2. Each apply mode declares explicit integrity, ordering, key, and trigger
   assumptions (`ApplyModeContract`).
3. Batch merging is ranked ahead of transactional apply only when required
   guarantees can be maintained or restored before exposure and total measured
   cost (write + constraint/trigger restore + pre-exposure validation +
   expected recovery/rollback + spread margin) is lower.
4. No-PK duplicates, cross-table transactions, and targets without unique
   enforcement cannot silently inherit transactional guarantees from a batch
   path.
"""

from __future__ import annotations

import datetime
import decimal
import pathlib
import uuid
from typing import Any, Dict

import pytest

import tests.rung_branches
from migkit import apply_contract, decide
from migkit.apply_contract import (
    APPLY_CONTRACTS,
    ApplyContractError,
    ApplyContractMode,
    ApplyTelemetryReadings,
    CareBadge,
    ExposureGateVerdict,
    IntegrityAssumption,
    KeyAssumption,
    OrderingAssumption,
    SilentGuaranteeInheritanceError,
    TriggerAssumption,
    UnmeasuredContractInputError,
    UnsafeEndpointHostError,
    acquire_apply_contract_lease,
    apply_contract_rungs,
    choose_apply_contract,
    cost_apply_contracts,
    evaluate_exposure_gate,
    exposure_gate_rungs,
    get_apply_contract,
    load_contract_checkpoint_unit,
    save_contract_checkpoint,
    simulate_contract_execution,
    validate_safe_endpoint,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    ("apply_contract", "apply_contract_rungs", "batch_net_merge", "holds"),
    ("apply_contract", "apply_contract_rungs", "batch_net_merge", "refuses"),
    ("apply_contract", "apply_contract_rungs", "staged_restored_batch", "holds"),
    ("apply_contract", "apply_contract_rungs", "staged_restored_batch", "refuses"),
    ("apply_contract", "apply_contract_rungs", "transactional_ordered", "holds"),
    ("apply_contract", "apply_contract_rungs", "transactional_ordered", "refuses"),
    ("apply_contract", "apply_contract_rungs", "keyless_multiset_contract", "holds"),
    ("apply_contract", "apply_contract_rungs", "keyless_multiset_contract", "refuses"),
    ("apply_contract", "apply_contract_rungs", "native_endpoint_contract", "holds"),
    ("apply_contract", "apply_contract_rungs", "native_endpoint_contract", "refuses"),
    ("apply_contract", "exposure_gate_rungs", "block_silent_guarantee_inheritance", "holds"),
    ("apply_contract", "exposure_gate_rungs", "block_silent_guarantee_inheritance", "refuses"),
    ("apply_contract", "exposure_gate_rungs", "restore_and_verify_before_exposure", "holds"),
    ("apply_contract", "exposure_gate_rungs", "restore_and_verify_before_exposure", "refuses"),
    ("apply_contract", "exposure_gate_rungs", "expose_verified_contract", "holds"),
    ("apply_contract", "exposure_gate_rungs", "expose_verified_contract", "refuses"),
]


# -----------------------------------------------------------------------------
# Helpers & Fixtures
# -----------------------------------------------------------------------------

def _sample_readings(
    *,
    raw_changes: int = 10_000,
    distinct_keys: int = 1_500,
    tables: int = 2,
    row_samples: tuple[float, ...] = (0.0010, 0.0012, 0.0011),
    batch_samples: tuple[float, ...] = (0.00010, 0.00012, 0.00011),
    startup_samples: tuple[float, ...] = (0.05, 0.06, 0.055),
    restore_samples: tuple[float, ...] = (0.20, 0.25, 0.22),
    val_samples: tuple[float, ...] = (0.00005, 0.00006, 0.000055),
    reconcile_samples: tuple[float, ...] = (0.30, 0.35, 0.32),
    batch_failures: int = 0,
    batch_trials: int = 20,
    txn_failures: int = 0,
    txn_trials: int = 20,
) -> ApplyTelemetryReadings:
    return ApplyTelemetryReadings(
        raw_change_count=raw_changes,
        distinct_key_count=distinct_keys,
        table_count=tables,
        row_apply_sec_per_row_samples=row_samples,
        batch_write_sec_per_row_samples=batch_samples,
        batch_startup_sec_samples=startup_samples,
        constraint_restore_sec_samples=restore_samples,
        validation_sec_per_row_samples=val_samples,
        rollback_reconcile_sec_samples=reconcile_samples,
        observed_batch_failures=batch_failures,
        observed_batch_trials=batch_trials,
        observed_txn_failures=txn_failures,
        observed_txn_trials=txn_trials,
    )


# -----------------------------------------------------------------------------
# 1. Decide Rung Ladders Exercise Both Holds and Refuses Under watched()
# -----------------------------------------------------------------------------

APPLY_RUNG_CASES = [
    (
        "batch_net_merge",
        {
            "has_primary_or_unique_key": True,
            "has_no_pk_duplicates": False,
            "target_enforces_unique_key": True,
            "cross_table_transaction": False,
            "requires_intermediate_trigger_events": False,
            "batch_net_merge_cost_advantage": True,
        },
        {
            "has_primary_or_unique_key": False,
            "has_no_pk_duplicates": True,
            "target_enforces_unique_key": True,
            "cross_table_transaction": False,
            "requires_intermediate_trigger_events": False,
            "batch_net_merge_cost_advantage": True,
        },
        "no-PK duplicate rows that key-collapsed batching would fold",
    ),
    (
        "staged_restored_batch",
        {
            "supports_isolated_staging_before_exposure": True,
            "has_primary_or_unique_key": True,
            "has_no_pk_duplicates": False,
            "target_enforces_unique_key": False,
            "pre_exposure_unique_proof": True,
            "cross_table_transaction": True,
            "all_coupled_tables_in_epoch": True,
            "requires_intermediate_trigger_events": False,
            "staged_restored_cost_advantage": True,
        },
        {
            "supports_isolated_staging_before_exposure": False,
            "has_primary_or_unique_key": True,
            "has_no_pk_duplicates": False,
            "target_enforces_unique_key": True,
        },
        "cannot isolate staged batches before reader exposure",
    ),
    (
        "transactional_ordered",
        {
            "transactional_dml_supported": True,
            "has_primary_or_unique_key": True,
        },
        {
            "transactional_dml_supported": False,
            "has_primary_or_unique_key": True,
        },
        "does not support interactive transactional DML",
    ),
    (
        "keyless_multiset_contract",
        {
            "has_primary_or_unique_key": False,
            "has_no_pk_duplicates": True,
            "supports_multiset_multiplicity_apply": True,
            "cross_table_transaction": False,
            "multiset_validation_enabled": True,
        },
        {
            "has_primary_or_unique_key": True,
            "has_no_pk_duplicates": False,
            "supports_multiset_multiplicity_apply": True,
            "cross_table_transaction": False,
            "multiset_validation_enabled": True,
        },
        "has an enforced unique key and no duplicate rows",
    ),
    (
        "native_endpoint_contract",
        {
            "is_native_or_non_enforcing_endpoint": True,
            "cross_table_transaction": False,
            "has_no_pk_duplicates": False,
            "has_idempotency_version_token": True,
            "native_validation_enabled": True,
        },
        {
            "is_native_or_non_enforcing_endpoint": False,
            "cross_table_transaction": False,
            "has_no_pk_duplicates": False,
            "has_idempotency_version_token": True,
            "native_validation_enabled": True,
        },
        "not a native non-enforcing endpoint",
    ),
]


EXPOSURE_GATE_CASES = [
    (
        "block_silent_guarantee_inheritance",
        {"silent_inheritance_violation": True},
        {"silent_inheritance_violation": False},
        "no silent guarantee inheritance violation was detected",
    ),
    (
        "restore_and_verify_before_exposure",
        {
            "silent_inheritance_violation": False,
            "requires_pre_exposure_restoration": True,
            "pre_exposure_verification_completed": False,
        },
        {
            "silent_inheritance_violation": False,
            "requires_pre_exposure_restoration": False,
            "pre_exposure_verification_completed": True,
        },
        "pre-exposure restoration is either unnecessary or already completed",
    ),
    (
        "expose_verified_contract",
        {
            "silent_inheritance_violation": False,
            "requires_pre_exposure_restoration": True,
            "pre_exposure_verification_completed": True,
        },
        {
            "silent_inheritance_violation": False,
            "requires_pre_exposure_restoration": True,
            "pre_exposure_verification_completed": False,
        },
        "pre-exposure constraint restoration or validation has not completed",
    ),
]


def test_proves_rungs_matches_all_ladders() -> None:
    expected = set()
    for rung_name, _, _, _ in APPLY_RUNG_CASES:
        expected.add(("apply_contract", "apply_contract_rungs", rung_name, HOLDS))
        expected.add(("apply_contract", "apply_contract_rungs", rung_name, REFUSES))
    for rung_name, _, _, _ in EXPOSURE_GATE_CASES:
        expected.add(("apply_contract", "exposure_gate_rungs", rung_name, HOLDS))
        expected.add(("apply_contract", "exposure_gate_rungs", rung_name, REFUSES))
    assert set(PROVES_RUNGS) == expected

    from tests import test_every_decide_rung_is_reached as rung_guard

    mod_path = pathlib.Path(apply_contract.__file__)
    test_path = pathlib.Path(__file__)
    mod_text = mod_path.read_text(encoding="utf-8")
    test_text = test_path.read_text(encoding="utf-8")
    assert all(ord(c) < 128 for c in mod_text)
    assert all(ord(c) < 128 for c in test_text)

    built = rung_guard.outside({"apply_contract": mod_text})
    decls = {"tests/test_apply_contract_costing.py": (list(PROVES_RUNGS), True)}
    unreached = rung_guard.unreached_outside(built, decls, {})
    assert not unreached, f"unreached rungs in apply_contract: {unreached}"


@pytest.mark.parametrize("rung_name,ok_facts,bad_facts,expected_why", APPLY_RUNG_CASES)
def test_apply_contract_rungs_hold_and_refuse_under_watched(
    rung_name: str,
    ok_facts: Dict[str, Any],
    bad_facts: Dict[str, Any],
    expected_why: str,
) -> None:
    rungs = {r.name: r for r in apply_contract_rungs()}
    target_rung = rungs[rung_name]

    with tests.rung_branches.watched() as seen:
        kept, out = decide.prune([target_rung], dict(ok_facts))
        assert kept == [target_rung], out
        kept_bad, out_bad = decide.prune([target_rung], dict(bad_facts))
        assert not kept_bad and out_bad[0][0] is target_rung

    assert branch(seen, rung_name) == [HOLDS, REFUSES]
    assert expected_why in out_bad[0][1]
    assert why(seen, rung_name) == out_bad[0][1]


@pytest.mark.parametrize("rung_name,ok_facts,bad_facts,expected_why", EXPOSURE_GATE_CASES)
def test_exposure_gate_rungs_hold_and_refuse_under_watched(
    rung_name: str,
    ok_facts: Dict[str, Any],
    bad_facts: Dict[str, Any],
    expected_why: str,
) -> None:
    rungs = {r.name: r for r in exposure_gate_rungs()}
    target_rung = rungs[rung_name]

    with tests.rung_branches.watched() as seen:
        kept, out = decide.prune([target_rung], dict(ok_facts))
        assert kept == [target_rung], out
        kept_bad, out_bad = decide.prune([target_rung], dict(bad_facts))
        assert not kept_bad and out_bad[0][0] is target_rung

    assert branch(seen, rung_name) == [HOLDS, REFUSES]
    assert expected_why in out_bad[0][1]
    assert why(seen, rung_name) == out_bad[0][1]


# -----------------------------------------------------------------------------
# 2. Explicit Contract Assumptions (Integrity, Ordering, Key, Trigger)
# -----------------------------------------------------------------------------

def test_each_apply_mode_declares_integrity_ordering_key_and_trigger_assumptions() -> None:
    assert set(APPLY_CONTRACTS.keys()) == set(ApplyContractMode)

    txn = get_apply_contract(ApplyContractMode.TRANSACTIONAL_ORDERED)
    assert txn.integrity == IntegrityAssumption.SOURCE_TRANSACTION_ATOMIC
    assert txn.ordering == OrderingAssumption.COMMIT_ORDER
    assert txn.key_assumption == KeyAssumption.KEY_OR_FULL_ROW_MATCH
    assert txn.trigger_assumption == TriggerAssumption.FIRES_IN_COMMIT_ORDER
    assert txn.preserves_cross_table_atomicity_in_flight is True
    assert txn.preserves_no_pk_duplicates is True
    assert txn.fires_intermediate_triggers is True

    net = get_apply_contract(ApplyContractMode.BATCH_NET_MERGE)
    assert net.integrity == IntegrityAssumption.SINGLE_TABLE_NET_BATCH
    assert net.ordering == OrderingAssumption.PER_KEY_NET
    assert net.key_assumption == KeyAssumption.ENFORCED_UNIQUE_KEY
    assert net.trigger_assumption == TriggerAssumption.NET_STATE_ONLY
    assert net.requires_primary_or_unique_key is True
    assert net.requires_target_unique_enforcement is True
    assert net.preserves_no_pk_duplicates is False
    assert net.preserves_cross_table_atomicity_in_flight is False

    keyless = get_apply_contract(ApplyContractMode.KEYLESS_MULTISET_CONTRACT)
    assert keyless.key_assumption == KeyAssumption.KEYLESS_MULTISET_MULTIPLICITY
    assert keyless.preserves_no_pk_duplicates is True
    assert keyless.requires_post_batch_validation is True

    native = get_apply_contract(ApplyContractMode.NATIVE_ENDPOINT_CONTRACT)
    assert native.integrity == IntegrityAssumption.IDEMPOTENT_EVENTUAL_BATCH
    assert native.can_restore_guarantees_before_exposure is False
    assert "consistent" in native.guarantee_map()
    assert txn.as_dict()["mode"] == "transactional_ordered"


# -----------------------------------------------------------------------------
# 3. Spread-Bounded Costing Including Recovery, Restoration, and Validation
# -----------------------------------------------------------------------------

def test_batch_merging_ranked_by_measured_write_validation_and_recovery_cost() -> None:
    # Scenario A: Hot key-collapse workload (10,000 changes over 1,000 keys), low batch failure rate
    hot_readings = _sample_readings(
        raw_changes=10_000,
        distinct_keys=1_000,
        tables=1,
        batch_failures=0,
        batch_trials=25,
    )
    facts_ok = {
        "has_primary_or_unique_key": True,
        "has_no_pk_duplicates": False,
        "target_enforces_unique_key": True,
        "cross_table_transaction": False,
        "requires_intermediate_trigger_events": False,
        "transactional_dml_supported": True,
    }
    with watched() as seen_a:
        dec_hot = choose_apply_contract(facts_ok, hot_readings)
    assert dec_hot.selected_mode == ApplyContractMode.BATCH_NET_MERGE
    assert dec_hot.care_badge == CareBadge.PASS
    assert dec_hot.cost_breakdown is not None
    assert (
        dec_hot.cost_breakdown.total_expected_seconds
        < dec_hot.all_costs[ApplyContractMode.TRANSACTIONAL_ORDERED.value].total_expected_seconds
    )
    assert HOLDS in branch(seen_a, "batch_net_merge")
    assert all(ord(c) < 128 for c in dec_hot.format_unix_line())

    # Scenario B: High batch failure probability + heavy validation/reconcile cost makes
    # batch_net_merge more expensive than transactional_ordered!
    risky_readings = _sample_readings(
        raw_changes=2_000,
        distinct_keys=1_900,
        tables=4,
        row_samples=(0.0005, 0.0006),
        batch_samples=(0.0004, 0.0005),
        startup_samples=(0.8, 0.9),
        val_samples=(0.0004, 0.0005),
        reconcile_samples=(2.5, 3.0),
        batch_failures=8,
        batch_trials=10,
        txn_failures=0,
        txn_trials=20,
    )
    with watched() as seen_b:
        dec_risky = choose_apply_contract(facts_ok, risky_readings)
    assert dec_risky.selected_mode == ApplyContractMode.TRANSACTIONAL_ORDERED
    assert REFUSES in branch(seen_b, "batch_net_merge")
    assert "exceeds transactional apply" in (why(seen_b, "batch_net_merge") or "")


def test_unmeasured_or_invalid_readings_raise_explicit_error() -> None:
    with pytest.raises(UnmeasuredContractInputError, match="raw_change_count"):
        _sample_readings(raw_changes=0)
    with pytest.raises(UnmeasuredContractInputError, match="at least one reading"):
        _sample_readings(row_samples=())
    with pytest.raises(UnmeasuredContractInputError, match="must be finite"):
        cost_apply_contracts(_sample_readings(row_samples=(-0.01,)))
    with pytest.raises(UnmeasuredContractInputError, match="cannot exceed"):
        _sample_readings(batch_failures=5, batch_trials=2)


# -----------------------------------------------------------------------------
# 4. Acceptance Evidence: No-PK Duplicates, Cross-Table Txns, Non-Unique Target
# -----------------------------------------------------------------------------

def test_no_pk_duplicates_cannot_silently_inherit_transactional_guarantees_from_batch() -> None:
    """Proves No-PK duplicates refuse key-collapsed batching and require keyless_multiset_contract."""
    readings = _sample_readings(raw_changes=500, distinct_keys=0, tables=1)
    no_pk_facts = {
        "has_primary_or_unique_key": False,
        "has_no_pk_duplicates": True,
        "target_enforces_unique_key": False,
        "cross_table_transaction": False,
        "supports_multiset_multiplicity_apply": True,
        "multiset_validation_enabled": True,
        "transactional_dml_supported": True,
        "supports_single_row_cursor_match": False,
    }

    with watched() as seen:
        decision = choose_apply_contract(no_pk_facts, readings)
    assert decision.selected_mode == ApplyContractMode.KEYLESS_MULTISET_CONTRACT
    assert REFUSES in branch(seen, "batch_net_merge")
    assert REFUSES in branch(seen, "staged_restored_batch")
    assert HOLDS in branch(seen, "keyless_multiset_contract")

    # Attempting to expose batch_net_merge on no-PK duplicates is blocked with ACTION badge
    with pytest.raises(SilentGuaranteeInheritanceError, match="no-PK duplicate"):
        evaluate_exposure_gate(
            ApplyContractMode.BATCH_NET_MERGE,
            no_pk_facts,
            claimed_transactional_guarantees=True,
            raise_on_violation=True,
        )

    # Sandbox proof: two identical rows in audit_log; source deletes ONLY one copy.
    # Forcing BATCH_NET_MERGE deletes BOTH copies (data loss!) and triggers ACTION gate block.
    initial = {"audit_log": [{"acct": 100, "amt": 50}, {"acct": 100, "amt": 50}]}
    changes = [{"table": "audit_log", "op": "delete", "match": {"acct": 100, "amt": 50}, "single_row": True}]

    forced_bad = simulate_contract_execution(
        changes,
        workload_facts=no_pk_facts,
        readings=readings,
        initial_rows_by_table=initial,
        force_mode=ApplyContractMode.BATCH_NET_MERGE,
        claimed_transactional_guarantees=True,
    )
    assert forced_bad.matches_source_truth is False
    assert len(forced_bad.exposed_rows_by_table["audit_log"]) == 0
    assert len(forced_bad.source_expected_rows_by_table["audit_log"]) == 1
    assert forced_bad.exposure_gate.verdict == ExposureGateVerdict.BLOCK_SILENT_GUARANTEE_INHERITANCE
    assert forced_bad.care_badge == CareBadge.ACTION

    # Running with chosen contract (KEYLESS_MULTISET_CONTRACT) preserves exact duplicate multiplicity (1 row left)
    valid_run = simulate_contract_execution(
        changes,
        workload_facts=no_pk_facts,
        readings=readings,
        initial_rows_by_table=initial,
    )
    assert valid_run.mode == ApplyContractMode.KEYLESS_MULTISET_CONTRACT
    assert valid_run.matches_source_truth is True
    assert len(valid_run.exposed_rows_by_table["audit_log"]) == 1


def test_cross_table_transactions_cannot_silently_inherit_transactional_guarantees() -> None:
    """Proves cross-table transactions refuse un-restored batch merging and preserve atomicity."""
    readings = _sample_readings(raw_changes=5_000, distinct_keys=1_000, tables=2)

    # Cross-table FK cascade with statement order dependency: batch_net_merge must refuse
    cross_table_facts = {
        "has_primary_or_unique_key": True,
        "has_no_pk_duplicates": False,
        "target_enforces_unique_key": True,
        "cross_table_transaction": True,
        "cross_table_order_dependency": True,
        "restores_guarantees_before_exposure": False,
        "supports_isolated_staging_before_exposure": False,
        "transactional_dml_supported": True,
    }
    with watched() as seen:
        decision = choose_apply_contract(cross_table_facts, readings)
    assert decision.selected_mode == ApplyContractMode.TRANSACTIONAL_ORDERED
    assert REFUSES in branch(seen, "batch_net_merge")
    assert REFUSES in branch(seen, "staged_restored_batch")
    assert HOLDS in branch(seen, "transactional_ordered")

    # Exposure gate blocks batch_net_merge from claiming transactional guarantees on cross-table txn
    gate = evaluate_exposure_gate(
        ApplyContractMode.BATCH_NET_MERGE,
        cross_table_facts,
        claimed_transactional_guarantees=True,
    )
    assert gate.verdict == ExposureGateVerdict.BLOCK_SILENT_GUARANTEE_INHERITANCE
    assert gate.care_badge == CareBadge.ACTION
    assert any("cross-table" in v for v in gate.violations)

    # When isolated staging fences all coupled tables in one pre-exposure epoch,
    # staged_restored_batch is admitted and includes constraint restoration cost
    staged_cross_facts = dict(
        cross_table_facts,
        supports_isolated_staging_before_exposure=True,
        all_coupled_tables_in_epoch=True,
    )
    dec_staged = choose_apply_contract(staged_cross_facts, readings)
    assert dec_staged.selected_mode == ApplyContractMode.STAGED_RESTORED_BATCH
    assert dec_staged.cost_breakdown is not None
    assert dec_staged.cost_breakdown.constraint_and_trigger_restore_seconds > 0.0


def test_target_without_unique_enforcement_cannot_silently_inherit_transactional_guarantees() -> None:
    """Proves a target without unique key enforcement refuses batch_net_merge and requires proof or native contract."""
    readings = _sample_readings(raw_changes=4_000, distinct_keys=800, tables=1)

    non_unique_target_facts = {
        "has_primary_or_unique_key": True,
        "has_no_pk_duplicates": False,
        "target_enforces_unique_key": False,
        "pre_exposure_unique_proof": False,
        "supports_isolated_staging_before_exposure": False,
        "transactional_dml_supported": False,
        "is_native_or_non_enforcing_endpoint": True,
        "has_idempotency_version_token": True,
        "native_validation_enabled": True,
    }
    with watched() as seen:
        decision = choose_apply_contract(non_unique_target_facts, readings)
    assert decision.selected_mode == ApplyContractMode.NATIVE_ENDPOINT_CONTRACT
    assert REFUSES in branch(seen, "batch_net_merge")
    assert REFUSES in branch(seen, "staged_restored_batch")
    assert HOLDS in branch(seen, "native_endpoint_contract")

    # Neither batch_net_merge nor native_endpoint_contract can silently claim transactional guarantees
    with pytest.raises(SilentGuaranteeInheritanceError, match="unique"):
        evaluate_exposure_gate(
            ApplyContractMode.BATCH_NET_MERGE,
            non_unique_target_facts,
            claimed_transactional_guarantees=True,
            raise_on_violation=True,
        )
    with pytest.raises(SilentGuaranteeInheritanceError, match="eventual/token contract only"):
        evaluate_exposure_gate(
            ApplyContractMode.NATIVE_ENDPOINT_CONTRACT,
            non_unique_target_facts,
            claimed_transactional_guarantees=True,
            raise_on_violation=True,
        )

    # Sandbox proof: existing row id=1 on a target without unique enforcement.
    # Forcing BATCH_NET_MERGE appends a duplicate id=1 row instead of updating in place!
    initial = {"orders": [{"id": 1, "status": "open"}]}
    changes = [{"table": "orders", "op": "update", "key": {"id": 1}, "values": {"id": 1, "status": "paid"}}]
    forced_non_unique = simulate_contract_execution(
        changes,
        workload_facts=non_unique_target_facts,
        readings=readings,
        initial_rows_by_table=initial,
        force_mode=ApplyContractMode.BATCH_NET_MERGE,
        claimed_transactional_guarantees=True,
    )
    assert forced_non_unique.matches_source_truth is False
    assert len(forced_non_unique.exposed_rows_by_table["orders"]) == 2
    assert forced_non_unique.care_badge == CareBadge.ACTION


# -----------------------------------------------------------------------------
# 5. Durable O(1) Typed Checkpoint & Mutual Exclusion Lease via Existing Owners
# -----------------------------------------------------------------------------

def test_o1_typed_checkpoint_and_lease_via_durable_and_lease_owners(
    tmp_path: pathlib.Path,
) -> None:
    ckpt_file = tmp_path / "apply_contract_ckpt.json"
    composite_cursor = (
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        decimal.Decimal("4200.75"),
        datetime.datetime(2026, 10, 17, 15, 0, 0, tzinfo=datetime.timezone.utc),
    )
    save_contract_checkpoint(
        ckpt_file,
        unit_id="shard-01",
        mode=ApplyContractMode.BATCH_NET_MERGE,
        cursor_key=composite_cursor,
        expected_cost_seconds=1.234,
    )

    loaded = load_contract_checkpoint_unit(ckpt_file, "shard-01")
    assert loaded is not None
    assert loaded["mode"] == ApplyContractMode.BATCH_NET_MERGE
    assert loaded["cursor_key"] == composite_cursor
    assert isinstance(loaded["cursor_key"][0], uuid.UUID)
    assert isinstance(loaded["cursor_key"][1], decimal.Decimal)
    assert isinstance(loaded["cursor_key"][2], datetime.datetime)
    assert load_contract_checkpoint_unit(ckpt_file, "shard-99") is None

    # Lease mutual exclusion via migkit.lease._FileRecord
    lock_file = tmp_path / ".run.lock"
    rec1 = acquire_apply_contract_lease(lock_file, holder="worker-a", ttl_seconds=30.0, now_epoch=1000.0)
    assert rec1["holder"] == "worker-a"
    with pytest.raises(ApplyContractError, match="actively held by worker-a"):
        acquire_apply_contract_lease(lock_file, holder="worker-b", ttl_seconds=30.0, now_epoch=1010.0)

    # After TTL lapse, worker-b can take over the lease
    rec2 = acquire_apply_contract_lease(lock_file, holder="worker-b", ttl_seconds=30.0, now_epoch=1040.0)
    assert rec2["holder"] == "worker-b"

    # Safe endpoint check
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("10.0.0.25") == "10.0.0.25"
    with pytest.raises(UnsafeEndpointHostError):
        validate_safe_endpoint("prod-db.internal.corp")
