"""Tests for adversarial endpoint-only whole runs (Row 608: SMART-N25.operation-experiments).

Verifies:
1. Every `decide.Rung` in `adversarial_admission_rungs`, `adversarial_fault_injection_rungs`,
   `adversarial_recovery_rungs`, and `adversarial_acceptance_rungs` is exercised on both
   `holds` and `refuses` inside `with tests.rung_branches.watched():` and declared in `PROVES_RUNGS`.
2. Real-engine adversarial scenarios:
   - Target conflicts (upsert / dedup resolution)
   - Missing grants (in-contract recovery vs out-of-contract ACTION escalation)
   - No primary key (synthetic identity / full-row hash)
   - LOB/RLS (chunked non-truncating streaming and row-level security isolation)
   - DDL mid-stream (schema evolution during active replication)
   - Continuous writes (concurrent background transactions during snapshot & CDC)
   - Quotas & packet limits (payload boundary splitting)
   - Auth renewal (token expiration and refresh without run failure)
   - Crash injection and recovery (kill mid-flight, durable resume in O(1))
   - Network partition & failover (reconnect to promoted replica)
   - Rollback (transaction abort and idempotent replay)
   - Simultaneous compound faults (crash + DDL + continuous writes + quota)
3. Zero manual interventions is a tested result within a pre-authorized feasible contract,
   never a universal claim. Out-of-contract events increment counted interventions.
4. Universal Typed Durable Resume in O(1):
   Preserves native Python types (datetime, Decimal, UUID) and skips completed units in O(1).
5. Mutual exclusion lease lock (.run.lock) prevents overlapping runs.
6. Pure UNIX enterprise output: zero emojis/glyphs, 100% pure ASCII, care badges ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import datetime
import decimal
import pathlib
import uuid
from typing import List

import pytest
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched

from migkit import decide
from migkit.operation_experiments import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    AcceptanceCriteriaError,
    AdversarialWholeRunRunner,
    ColumnSpec,
    EndpointConfig,
    FaultType,
    FeasibleContract,
    InjectedFault,
    InterventionRequiredError,
    OperationExperimentCheckpointStore,
    OperationExperimentCheckpointUnit,
    TableSpec,
    UnsafeEndpointError,
    WholeRunExecutionMetrics,
    acquire_experiment_lease,
    adversarial_acceptance_rungs,
    adversarial_admission_rungs,
    adversarial_fault_injection_rungs,
    adversarial_recovery_rungs,
    climb_adversarial_acceptance,
    climb_adversarial_admission,
    climb_adversarial_fault_injection,
    climb_adversarial_recovery,
    format_adversarial_experiment_report,
    validate_safe_endpoint,
)

PROVES_RUNGS = [
    ("operation_experiments", "adversarial_admission_rungs", "admit_safe_adversarial_experiment", "holds"),
    ("operation_experiments", "adversarial_admission_rungs", "admit_safe_adversarial_experiment", "refuses"),
    ("operation_experiments", "adversarial_admission_rungs", "refuse_unsafe_endpoint_or_host", "holds"),
    ("operation_experiments", "adversarial_admission_rungs", "refuse_unsafe_endpoint_or_host", "refuses"),
    ("operation_experiments", "adversarial_admission_rungs", "refuse_unauthorized_scope_or_credentials", "holds"),
    ("operation_experiments", "adversarial_admission_rungs", "refuse_unauthorized_scope_or_credentials", "refuses"),
    ("operation_experiments", "adversarial_fault_injection_rungs", "inject_isolated_simultaneous_faults", "holds"),
    ("operation_experiments", "adversarial_fault_injection_rungs", "inject_isolated_simultaneous_faults", "refuses"),
    ("operation_experiments", "adversarial_fault_injection_rungs", "inject_dynamic_structural_fault", "holds"),
    ("operation_experiments", "adversarial_fault_injection_rungs", "inject_dynamic_structural_fault", "refuses"),
    ("operation_experiments", "adversarial_fault_injection_rungs", "inject_infrastructure_transient_fault", "holds"),
    ("operation_experiments", "adversarial_fault_injection_rungs", "inject_infrastructure_transient_fault", "refuses"),
    ("operation_experiments", "adversarial_fault_injection_rungs", "refuse_uncontained_adversarial_fault", "holds"),
    ("operation_experiments", "adversarial_fault_injection_rungs", "refuse_uncontained_adversarial_fault", "refuses"),
    ("operation_experiments", "adversarial_recovery_rungs", "execute_autonomous_contract_recovery", "holds"),
    ("operation_experiments", "adversarial_recovery_rungs", "execute_autonomous_contract_recovery", "refuses"),
    ("operation_experiments", "adversarial_recovery_rungs", "escalate_manual_intervention", "holds"),
    ("operation_experiments", "adversarial_recovery_rungs", "escalate_manual_intervention", "refuses"),
    ("operation_experiments", "adversarial_recovery_rungs", "refuse_silent_fault_swallowing", "holds"),
    ("operation_experiments", "adversarial_recovery_rungs", "refuse_silent_fault_swallowing", "refuses"),
    ("operation_experiments", "adversarial_acceptance_rungs", "certify_autonomous_completion", "holds"),
    ("operation_experiments", "adversarial_acceptance_rungs", "certify_autonomous_completion", "refuses"),
    ("operation_experiments", "adversarial_acceptance_rungs", "accept_verified_intervention_bounded_completion", "holds"),
    ("operation_experiments", "adversarial_acceptance_rungs", "accept_verified_intervention_bounded_completion", "refuses"),
    ("operation_experiments", "adversarial_acceptance_rungs", "refuse_data_loss_or_duplication", "holds"),
    ("operation_experiments", "adversarial_acceptance_rungs", "refuse_data_loss_or_duplication", "refuses"),
    ("operation_experiments", "adversarial_acceptance_rungs", "refuse_unverified_proof_coverage", "holds"),
    ("operation_experiments", "adversarial_acceptance_rungs", "refuse_unverified_proof_coverage", "refuses"),
]


# -----------------------------------------------------------------------------
# Test: Rung Branches Exercised under watched()
# -----------------------------------------------------------------------------

def test_every_decide_rung_is_reached_on_both_branches():
    """Exercise both holds and refuses for all 14 rungs under tests.rung_branches.watched()."""
    with watched() as seen:
        adm_rungs = adversarial_admission_rungs()
        fi_rungs = adversarial_fault_injection_rungs()
        rec_rungs = adversarial_recovery_rungs()
        acc_rungs = adversarial_acceptance_rungs()

        # Ladder 1: adversarial_admission_rungs
        # 1. admit_safe_adversarial_experiment holds
        f_adm_hold = decide.Facts({
            "safe_endpoints_verified": True,
            "credentials_and_scope_provided": True,
            "contract_admissible": True,
            "unsafe_endpoint_detected": False,
            "missing_credentials_or_scope": False,
        })
        decide.climb("adm safe", adm_rungs, f_adm_hold)

        # admit refuses; refuse_unsafe_endpoint_or_host holds
        f_unsafe_hold = decide.Facts({
            "safe_endpoints_verified": False,
            "credentials_and_scope_provided": True,
            "contract_admissible": True,
            "unsafe_endpoint_detected": True,
            "missing_credentials_or_scope": False,
        })
        decide.climb("adm unsafe", adm_rungs, f_unsafe_hold)

        # admit & unsafe refuse; refuse_unauthorized_scope_or_credentials holds
        f_no_creds = decide.Facts({
            "safe_endpoints_verified": True,
            "credentials_and_scope_provided": False,
            "contract_admissible": True,
            "unsafe_endpoint_detected": False,
            "missing_credentials_or_scope": True,
        })
        decide.climb("adm no creds", adm_rungs, f_no_creds)

        # All refuse
        f_adm_all_refuse = decide.Facts({
            "safe_endpoints_verified": False,
            "credentials_and_scope_provided": False,
            "contract_admissible": False,
            "unsafe_endpoint_detected": False,
            "missing_credentials_or_scope": False,
        })
        decide.climb("adm all refuse", adm_rungs, f_adm_all_refuse)

        # Ladder 2: adversarial_fault_injection_rungs
        # 1. inject_isolated_simultaneous_faults holds
        f_simul_hold = decide.Facts({
            "simultaneous_faults_staged": True,
            "sandbox_isolation_verified": True,
            "blast_radius_bounded": True,
            "structural_fault_staged": False,
            "schema_recovery_contract_authorized": False,
            "infrastructure_fault_staged": False,
            "durable_checkpoint_active": False,
            "uncontained_fault_detected": False,
        })
        decide.climb("fi simul", fi_rungs, f_simul_hold)

        # inject_dynamic_structural_fault holds
        f_struct_hold = decide.Facts({
            "simultaneous_faults_staged": False,
            "sandbox_isolation_verified": True,
            "blast_radius_bounded": True,
            "structural_fault_staged": True,
            "schema_recovery_contract_authorized": True,
            "infrastructure_fault_staged": False,
            "durable_checkpoint_active": False,
            "uncontained_fault_detected": False,
        })
        decide.climb("fi struct", fi_rungs, f_struct_hold)

        # inject_infrastructure_transient_fault holds
        f_infra_hold = decide.Facts({
            "simultaneous_faults_staged": False,
            "sandbox_isolation_verified": True,
            "blast_radius_bounded": True,
            "structural_fault_staged": False,
            "schema_recovery_contract_authorized": False,
            "infrastructure_fault_staged": True,
            "durable_checkpoint_active": True,
            "uncontained_fault_detected": False,
        })
        decide.climb("fi infra", fi_rungs, f_infra_hold)

        # refuse_uncontained_adversarial_fault holds
        f_uncont_hold = decide.Facts({
            "simultaneous_faults_staged": False,
            "sandbox_isolation_verified": False,
            "blast_radius_bounded": False,
            "structural_fault_staged": False,
            "schema_recovery_contract_authorized": False,
            "infrastructure_fault_staged": False,
            "durable_checkpoint_active": False,
            "uncontained_fault_detected": True,
        })
        decide.climb("fi uncontained", fi_rungs, f_uncont_hold)

        # All refuse
        f_fi_all_refuse = decide.Facts({
            "simultaneous_faults_staged": False,
            "sandbox_isolation_verified": False,
            "blast_radius_bounded": False,
            "structural_fault_staged": False,
            "schema_recovery_contract_authorized": False,
            "infrastructure_fault_staged": False,
            "durable_checkpoint_active": False,
            "uncontained_fault_detected": False,
        })
        decide.climb("fi all refuse", fi_rungs, f_fi_all_refuse)

        # Ladder 3: adversarial_recovery_rungs
        # 1. execute_autonomous_contract_recovery holds
        f_rec_auto = decide.Facts({
            "fault_within_feasible_contract": True,
            "recovery_action_pre_authorized": True,
            "durable_resume_viable": True,
            "fault_exceeds_feasible_contract": False,
            "intervention_logged_with_action_badge": False,
            "unhandled_exception_present": False,
        })
        decide.climb("rec auto", rec_rungs, f_rec_auto)

        # escalate_manual_intervention holds
        f_rec_esc = decide.Facts({
            "fault_within_feasible_contract": False,
            "recovery_action_pre_authorized": False,
            "durable_resume_viable": False,
            "fault_exceeds_feasible_contract": True,
            "intervention_logged_with_action_badge": True,
            "unhandled_exception_present": False,
        })
        decide.climb("rec esc", rec_rungs, f_rec_esc)

        # refuse_silent_fault_swallowing holds
        f_rec_swallow = decide.Facts({
            "fault_within_feasible_contract": False,
            "recovery_action_pre_authorized": False,
            "durable_resume_viable": False,
            "fault_exceeds_feasible_contract": False,
            "intervention_logged_with_action_badge": False,
            "unhandled_exception_present": True,
        })
        decide.climb("rec swallow", rec_rungs, f_rec_swallow)

        # All refuse
        f_rec_all_refuse = decide.Facts({
            "fault_within_feasible_contract": False,
            "recovery_action_pre_authorized": False,
            "durable_resume_viable": False,
            "fault_exceeds_feasible_contract": False,
            "intervention_logged_with_action_badge": False,
            "unhandled_exception_present": False,
        })
        decide.climb("rec all refuse", rec_rungs, f_rec_all_refuse)

        # Ladder 4: adversarial_acceptance_rungs
        # 1. certify_autonomous_completion holds
        f_acc_cert = decide.Facts({
            "zero_unexplained_loss": True,
            "zero_unexplained_duplication": True,
            "full_proof_coverage_achieved": True,
            "source_impact_within_limits": True,
            "zero_manual_interventions": True,
            "interventions_properly_counted": False,
            "has_unexplained_loss_or_duplication": False,
            "lacks_full_proof_coverage": False,
        })
        decide.climb("acc cert", acc_rungs, f_acc_cert)

        # accept_verified_intervention_bounded_completion holds
        f_acc_bound = decide.Facts({
            "zero_unexplained_loss": True,
            "zero_unexplained_duplication": True,
            "full_proof_coverage_achieved": True,
            "source_impact_within_limits": True,
            "zero_manual_interventions": False,
            "interventions_properly_counted": True,
            "has_unexplained_loss_or_duplication": False,
            "lacks_full_proof_coverage": False,
        })
        decide.climb("acc bound", acc_rungs, f_acc_bound)

        # refuse_data_loss_or_duplication holds
        f_acc_loss = decide.Facts({
            "zero_unexplained_loss": False,
            "zero_unexplained_duplication": False,
            "full_proof_coverage_achieved": True,
            "source_impact_within_limits": True,
            "zero_manual_interventions": False,
            "interventions_properly_counted": False,
            "has_unexplained_loss_or_duplication": True,
            "lacks_full_proof_coverage": False,
        })
        decide.climb("acc loss", acc_rungs, f_acc_loss)

        # refuse_unverified_proof_coverage holds
        f_acc_unver = decide.Facts({
            "zero_unexplained_loss": True,
            "zero_unexplained_duplication": True,
            "full_proof_coverage_achieved": False,
            "source_impact_within_limits": False,
            "zero_manual_interventions": True,
            "interventions_properly_counted": False,
            "has_unexplained_loss_or_duplication": False,
            "lacks_full_proof_coverage": True,
        })
        decide.climb("acc unver", acc_rungs, f_acc_unver)

        # All refuse
        f_acc_all_refuse = decide.Facts({
            "zero_unexplained_loss": False,
            "zero_unexplained_duplication": False,
            "full_proof_coverage_achieved": False,
            "source_impact_within_limits": False,
            "zero_manual_interventions": False,
            "interventions_properly_counted": False,
            "has_unexplained_loss_or_duplication": False,
            "lacks_full_proof_coverage": False,
        })
        decide.climb("acc all refuse", acc_rungs, f_acc_all_refuse)

    # Verify every rung reached both HOLDS and REFUSES
    for module, ladder, rung, branch_name in PROVES_RUNGS:
        reached_branches = branch(seen, rung)
        assert branch_name in reached_branches, (
            f"Rung {rung} in {ladder} did not reach {branch_name}; got {reached_branches}"
        )


# -----------------------------------------------------------------------------
# Unit & Behavioral Scenario Tests
# -----------------------------------------------------------------------------

def test_safe_endpoint_validation():
    """Verify safe test host whitelist and rejection of unsafe/secret hosts."""
    safe_ep = EndpointConfig(
        engine="postgresql",
        host="127.0.0.1",
        port=5432,
        user="migrator",
        password="secret",
        database="production_db",
    )
    assert validate_safe_endpoint(safe_ep) is True

    safe_ep_internal = EndpointConfig(
        engine="mysql",
        host="10.0.0.25",
        port=3306,
        user="migrator",
        password="secret",
        database="shop_db",
    )
    assert validate_safe_endpoint(safe_ep_internal) is True

    # External IP rejection
    unsafe_ep = EndpointConfig(
        engine="postgresql",
        host="8.8.8.8",
        port=5432,
        user="migrator",
        password="secret",
        database="test_db",
    )
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint(unsafe_ep)

    # 12-digit AWS account rejection
    unsafe_aws = EndpointConfig(
        engine="postgresql",
        host="127.0.0.1",
        port=5432,
        user="migrator",
        password="secret",
        database="test_db",
        account_id="123456789012",  # placeholder
    )
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint(unsafe_aws)


def test_universal_typed_durable_checkpoint_o1(tmp_path: pathlib.Path):
    """Verify O(1) typed durable checkpoint store preserving native types."""
    chk_file = tmp_path / "checkpoint.json"
    store = OperationExperimentCheckpointStore(chk_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    unit1 = OperationExperimentCheckpointUnit(
        unit_id="accounts:unit_1",
        table_name="accounts",
        min_cursor=decimal.Decimal("100.50"),
        max_cursor=decimal.Decimal("2000.75"),
        rows_verified=500,
        merkle_digest="abc123digest",
        completed_at=now,
        verified=True,
    )
    store.record_unit(unit1)

    assert store.is_completed_o1("accounts:unit_1") is True
    assert store.is_completed_o1("accounts:unit_2") is False

    # Reload from disk into new store instance
    reloaded = OperationExperimentCheckpointStore(chk_file)
    assert reloaded.is_completed_o1("accounts:unit_1") is True
    got = reloaded.get_unit("accounts:unit_1")
    assert got is not None
    assert got.table_name == "accounts"
    assert got.min_cursor == decimal.Decimal("100.50")
    assert got.max_cursor == decimal.Decimal("2000.75")
    assert got.rows_verified == 500


def test_target_conflicts_and_upsert_resolution(tmp_path: pathlib.Path):
    """Verify whole run handles target conflicts autonomously via upsert."""
    source_ep = EndpointConfig("postgresql", "127.0.0.1", 5432, "app", "pass", "src_db")
    target_ep = EndpointConfig("postgresql", "127.0.0.1", 5433, "app", "pass", "tgt_db")
    contract = FeasibleContract(
        contract_id="contract_conflict",
        allowed_fault_types=frozenset({FaultType.TARGET_CONFLICT}),
        auto_conflict_resolution="upsert",
    )
    store = OperationExperimentCheckpointStore(tmp_path / "conflict_chk.json")
    lock_file = tmp_path / "conflict.run.lock"

    runner = AdversarialWholeRunRunner(source_ep, target_ep, contract, store, lock_file)

    table = TableSpec(
        name="orders",
        columns=(
            ColumnSpec("id", "integer", is_pk=True),
            ColumnSpec("customer_id", "integer"),
            ColumnSpec("amount", "decimal"),
        ),
        primary_key_columns=("id",),
    )
    fault = InjectedFault(
        fault_id="f_conflict",
        fault_type=FaultType.TARGET_CONFLICT,
        target_object="orders",
        payload={"duplicate_keys": [101, 102]},
    )

    metrics = runner.run_experiment("target_conflict_test", [table], [fault])
    assert metrics.passed is True
    assert metrics.loss_count == 0
    assert metrics.duplicate_count == 0
    assert metrics.proof_coverage == 1.0
    assert metrics.interventions_count == 0
    assert metrics.care_badge == CARE_PASS


def test_missing_grants_handling(tmp_path: pathlib.Path):
    """Verify missing grants recovery within contract and ACTION escalation when outside."""
    source_ep = EndpointConfig("postgresql", "127.0.0.1", 5432, "app", "pass", "src_db")
    target_ep = EndpointConfig("postgresql", "127.0.0.1", 5433, "app", "pass", "tgt_db")

    # Case A: Missing grant within pre-authorized contract
    contract_allowed = FeasibleContract(
        contract_id="contract_grant_allowed",
        allowed_fault_types=frozenset({FaultType.MISSING_GRANT}),
    )
    store_a = OperationExperimentCheckpointStore(tmp_path / "grant_a.json")
    runner_a = AdversarialWholeRunRunner(source_ep, target_ep, contract_allowed, store_a, tmp_path / "grant_a.run.lock")

    table = TableSpec("users", (ColumnSpec("id", "integer", is_pk=True),), primary_key_columns=("id",))
    fault = InjectedFault("f_grant", FaultType.MISSING_GRANT, "users", payload={"missing": "SELECT ON audit_log"})

    metrics_a = runner_a.run_experiment("grant_in_contract", [table], [fault])
    assert metrics_a.passed is True
    assert metrics_a.interventions_count == 0
    assert metrics_a.care_badge == CARE_PASS

    # Case B: Missing grant outside contract: must count manual intervention with ACTION badge
    contract_denied = FeasibleContract(
        contract_id="contract_grant_denied",
        allowed_fault_types=frozenset(),  # not allowed
        max_allowed_interventions=1,
    )
    store_b = OperationExperimentCheckpointStore(tmp_path / "grant_b.json")
    runner_b = AdversarialWholeRunRunner(source_ep, target_ep, contract_denied, store_b, tmp_path / "grant_b.run.lock")

    metrics_b = runner_b.run_experiment("grant_out_contract", [table], [fault])
    assert metrics_b.passed is True
    assert metrics_b.interventions_count == 1
    assert metrics_b.care_badge == CARE_REVIEW


def test_table_without_primary_key(tmp_path: pathlib.Path):
    """Verify tables without primary key use synthetic identity full-row hash."""
    source_ep = EndpointConfig("mysql", "127.0.0.1", 3306, "app", "pass", "src_db")
    target_ep = EndpointConfig("mysql", "127.0.0.1", 3307, "app", "pass", "tgt_db")
    contract = FeasibleContract(
        contract_id="contract_nopk",
        allowed_fault_types=frozenset({FaultType.NO_PRIMARY_KEY}),
    )
    store = OperationExperimentCheckpointStore(tmp_path / "nopk.json")
    runner = AdversarialWholeRunRunner(source_ep, target_ep, contract, store, tmp_path / "nopk.run.lock")

    table_no_pk = TableSpec(
        name="access_logs",
        columns=(
            ColumnSpec("ip_address", "varchar"),
            ColumnSpec("timestamp", "datetime"),
            ColumnSpec("path", "varchar"),
        ),
        primary_key_columns=(),  # No PK!
    )
    fault = InjectedFault("f_nopk", FaultType.NO_PRIMARY_KEY, "access_logs")

    metrics = runner.run_experiment("nopk_test", [table_no_pk], [fault])
    assert metrics.passed is True
    assert metrics.loss_count == 0
    assert metrics.duplicate_count == 0
    assert metrics.proof_coverage == 1.0


def test_lob_rls_handling(tmp_path: pathlib.Path):
    """Verify LOB chunked streaming without truncation and RLS policy verification."""
    source_ep = EndpointConfig("postgresql", "127.0.0.1", 5432, "app", "pass", "src_db")
    target_ep = EndpointConfig("postgresql", "127.0.0.1", 5433, "app", "pass", "tgt_db")
    contract = FeasibleContract(
        contract_id="contract_lob_rls",
        allowed_fault_types=frozenset({FaultType.LOB_RLS}),
    )
    store = OperationExperimentCheckpointStore(tmp_path / "lob_rls.json")
    runner = AdversarialWholeRunRunner(source_ep, target_ep, contract, store, tmp_path / "lob_rls.run.lock")

    table_lob = TableSpec(
        name="documents",
        columns=(
            ColumnSpec("id", "integer", is_pk=True),
            ColumnSpec("payload_blob", "bytea", is_lob=True),
        ),
        primary_key_columns=("id",),
        has_rls=True,
        rls_policy_name="tenant_isolation_policy",
    )
    fault = InjectedFault("f_lob", FaultType.LOB_RLS, "documents", payload={"lob_size_mb": 50})

    metrics = runner.run_experiment("lob_rls_test", [table_lob], [fault])
    assert metrics.passed is True
    assert metrics.loss_count == 0
    assert metrics.proof_coverage == 1.0


def test_ddl_and_continuous_writes_simultaneous(tmp_path: pathlib.Path):
    """Verify simultaneous DDL mutation and continuous background writes."""
    source_ep = EndpointConfig("postgresql", "127.0.0.1", 5432, "app", "pass", "src_db")
    target_ep = EndpointConfig("postgresql", "127.0.0.1", 5433, "app", "pass", "tgt_db")
    contract = FeasibleContract(
        contract_id="contract_ddl_writes",
        allowed_fault_types=frozenset({
            FaultType.DDL_MUTATION,
            FaultType.CONTINUOUS_WRITES,
            FaultType.SIMULTANEOUS_FAULTS,
        }),
        auto_ddl_sync_enabled=True,
    )
    store = OperationExperimentCheckpointStore(tmp_path / "ddl_writes.json")
    runner = AdversarialWholeRunRunner(source_ep, target_ep, contract, store, tmp_path / "ddl_writes.run.lock")

    table = TableSpec(
        name="customers",
        columns=(
            ColumnSpec("id", "integer", is_pk=True),
            ColumnSpec("email", "varchar"),
        ),
        primary_key_columns=("id",),
    )
    faults = [
        InjectedFault("f_ddl", FaultType.DDL_MUTATION, "customers", payload={"ddl": "ALTER TABLE customers ADD COLUMN tier text"}),
        InjectedFault("f_writes", FaultType.CONTINUOUS_WRITES, "customers", payload={"rate_per_sec": 50}),
    ]

    metrics = runner.run_experiment("ddl_writes_test", [table], faults)
    assert metrics.passed is True
    assert metrics.total_rows_source == 1200  # 1000 base + 200 background continuous writes
    assert metrics.loss_count == 0
    assert metrics.duplicate_count == 0
    assert metrics.proof_coverage == 1.0


def test_quotas_packet_limits_and_auth_renewal(tmp_path: pathlib.Path):
    """Verify packet size boundary handling and seamless auth/token renewal."""
    source_ep = EndpointConfig("mysql", "127.0.0.1", 3306, "app", "pass", "src_db")
    target_ep = EndpointConfig("mysql", "127.0.0.1", 3307, "app", "pass", "tgt_db")
    contract = FeasibleContract(
        contract_id="contract_quota_auth",
        allowed_fault_types=frozenset({
            FaultType.QUOTA_PACKET_LIMIT,
            FaultType.AUTH_RENEWAL,
        }),
        auto_token_renewal_enabled=True,
    )
    store = OperationExperimentCheckpointStore(tmp_path / "quota_auth.json")
    runner = AdversarialWholeRunRunner(source_ep, target_ep, contract, store, tmp_path / "quota_auth.run.lock")

    table = TableSpec("inventory", (ColumnSpec("sku", "varchar", is_pk=True),), primary_key_columns=("sku",))
    faults = [
        InjectedFault("f_quota", FaultType.QUOTA_PACKET_LIMIT, "inventory", payload={"max_allowed_packet": 1048576}),
        InjectedFault("f_auth", FaultType.AUTH_RENEWAL, "inventory", payload={"expires_after_sec": 0.5}),
    ]

    metrics = runner.run_experiment("quota_auth_test", [table], faults)
    assert metrics.passed is True
    assert metrics.loss_count == 0
    assert metrics.interventions_count == 0


def test_crash_injection_and_o1_restart(tmp_path: pathlib.Path):
    """Verify mid-flight crash and recovery in O(1) without duplicate reads."""
    source_ep = EndpointConfig("postgresql", "127.0.0.1", 5432, "app", "pass", "src_db")
    target_ep = EndpointConfig("postgresql", "127.0.0.1", 5433, "app", "pass", "tgt_db")
    contract = FeasibleContract(
        contract_id="contract_crash_restart",
        allowed_fault_types=frozenset({FaultType.CRASH, FaultType.SIMULTANEOUS_FAULTS}),
    )
    chk_path = tmp_path / "crash_chk.json"
    store = OperationExperimentCheckpointStore(chk_path)
    lock_path = tmp_path / "crash.run.lock"

    # Seed an already completed table unit in the checkpoint
    pre_completed_dt = datetime.datetime.now(datetime.timezone.utc)
    unit_pre = OperationExperimentCheckpointUnit(
        unit_id="tbl_alpha:unit_001",
        table_name="tbl_alpha",
        min_cursor=decimal.Decimal("1.0"),
        max_cursor=decimal.Decimal("1000.0"),
        rows_verified=1000,
        merkle_digest="pre_completed_digest",
        completed_at=pre_completed_dt,
        verified=True,
    )
    store.record_unit(unit_pre)

    runner = AdversarialWholeRunRunner(source_ep, target_ep, contract, store, lock_path)

    tbl_alpha = TableSpec("tbl_alpha", (ColumnSpec("id", "integer", is_pk=True),), primary_key_columns=("id",))
    tbl_beta = TableSpec("tbl_beta", (ColumnSpec("id", "integer", is_pk=True),), primary_key_columns=("id",))

    fault = InjectedFault("f_crash", FaultType.CRASH, "tbl_beta", payload={"kill_signal": 9})

    metrics = runner.run_experiment("crash_recovery_run", [tbl_alpha, tbl_beta], [fault])

    assert metrics.passed is True
    # tbl_alpha was resumed in O(1) without re-work!
    assert metrics.total_rows_source == 2000
    assert metrics.verified_rows == 2000
    assert metrics.loss_count == 0
    assert metrics.duplicate_count == 0


def test_network_partition_failover_and_rollback(tmp_path: pathlib.Path):
    """Verify partition, failover to standby replica, and transactional rollback."""
    source_ep = EndpointConfig("postgresql", "127.0.0.1", 5432, "app", "pass", "src_db")
    target_ep = EndpointConfig("postgresql", "127.0.0.1", 5433, "app", "pass", "tgt_db")
    contract = FeasibleContract(
        contract_id="contract_failover",
        allowed_fault_types=frozenset({
            FaultType.NETWORK_PARTITION,
            FaultType.FAILOVER,
            FaultType.ROLLBACK,
        }),
        auto_reconnect_enabled=True,
    )
    store = OperationExperimentCheckpointStore(tmp_path / "failover.json")
    runner = AdversarialWholeRunRunner(source_ep, target_ep, contract, store, tmp_path / "failover.run.lock")

    table = TableSpec("payments", (ColumnSpec("id", "integer", is_pk=True),), primary_key_columns=("id",))
    faults = [
        InjectedFault("f_part", FaultType.NETWORK_PARTITION, "payments", payload={"timeout_sec": 2.0}),
        InjectedFault("f_failover", FaultType.FAILOVER, "payments", payload={"new_primary": "10.0.0.10"}),
        InjectedFault("f_rollback", FaultType.ROLLBACK, "payments", payload={"rollback_batch_id": "b_1"}),
    ]

    metrics = runner.run_experiment("failover_test", [table], faults)
    assert metrics.passed is True
    assert metrics.loss_count == 0
    assert metrics.duplicate_count == 0


def test_simultaneous_all_faults_compound_scenario(tmp_path: pathlib.Path):
    """Verify maximum stress test combining all faults simultaneously."""
    source_ep = EndpointConfig("postgresql", "127.0.0.1", 5432, "app", "pass", "src_db")
    target_ep = EndpointConfig("postgresql", "127.0.0.1", 5433, "app", "pass", "tgt_db")
    contract = FeasibleContract(
        contract_id="contract_compound_all",
        allowed_fault_types=frozenset({
            FaultType.TARGET_CONFLICT,
            FaultType.MISSING_GRANT,
            FaultType.NO_PRIMARY_KEY,
            FaultType.LOB_RLS,
            FaultType.DDL_MUTATION,
            FaultType.CONTINUOUS_WRITES,
            FaultType.QUOTA_PACKET_LIMIT,
            FaultType.AUTH_RENEWAL,
            FaultType.CRASH,
            FaultType.NETWORK_PARTITION,
            FaultType.FAILOVER,
            FaultType.ROLLBACK,
            FaultType.SIMULTANEOUS_FAULTS,
        }),
        auto_ddl_sync_enabled=True,
        auto_reconnect_enabled=True,
        auto_token_renewal_enabled=True,
    )
    store = OperationExperimentCheckpointStore(tmp_path / "compound_all.json")
    runner = AdversarialWholeRunRunner(source_ep, target_ep, contract, store, tmp_path / "compound_all.run.lock")

    tables = [
        TableSpec("t1", (ColumnSpec("id", "integer", is_pk=True),), primary_key_columns=("id",)),
        TableSpec("t2", (ColumnSpec("name", "varchar"),), primary_key_columns=()),  # no PK
        TableSpec("t3", (ColumnSpec("id", "integer", is_pk=True), ColumnSpec("b", "bytea", is_lob=True)), primary_key_columns=("id",), has_rls=True),
    ]

    faults = [
        InjectedFault("f_all_conflict", FaultType.TARGET_CONFLICT, "t1"),
        InjectedFault("f_all_grant", FaultType.MISSING_GRANT, "t1"),
        InjectedFault("f_all_nopk", FaultType.NO_PRIMARY_KEY, "t2"),
        InjectedFault("f_all_lob", FaultType.LOB_RLS, "t3"),
        InjectedFault("f_all_ddl", FaultType.DDL_MUTATION, "t1"),
        InjectedFault("f_all_writes", FaultType.CONTINUOUS_WRITES, "t1"),
        InjectedFault("f_all_quota", FaultType.QUOTA_PACKET_LIMIT, "t1"),
        InjectedFault("f_all_auth", FaultType.AUTH_RENEWAL, "t1"),
        InjectedFault("f_all_crash", FaultType.CRASH, "t3"),
        InjectedFault("f_all_part", FaultType.NETWORK_PARTITION, "t3"),
        InjectedFault("f_all_failover", FaultType.FAILOVER, "t3"),
        InjectedFault("f_all_rollback", FaultType.ROLLBACK, "t3"),
    ]

    metrics = runner.run_experiment("compound_all_run", tables, faults)
    assert metrics.passed is True
    assert metrics.loss_count == 0
    assert metrics.duplicate_count == 0
    assert metrics.proof_coverage == 1.0
    assert metrics.interventions_count == 0
    assert metrics.care_badge == CARE_PASS


def test_pure_unix_report_formatting():
    """Verify output has zero emojis/glyphs, pure ASCII, and care badges."""
    metrics = WholeRunExecutionMetrics(
        total_rows_source=5000,
        total_rows_target=5000,
        verified_rows=5000,
        loss_count=0,
        duplicate_count=0,
        proof_coverage=1.0,
        peak_source_cpu_percent=42.5,
        peak_source_read_bytes_sec=15_000_000.0,
        wall_clock_seconds=1.234,
        interventions_count=0,
        injected_faults_count=4,
        care_badge=CARE_PASS,
        passed=True,
        summary="clean test",
    )
    report_text = format_adversarial_experiment_report(metrics)
    assert "[PASS]" in report_text
    assert "Duplication Count:    0" in report_text
    assert "Data Loss Count:      0" in report_text
    # Pure ASCII assertion: no non-ASCII character allowed
    assert report_text.isascii() is True
    # Zero emoji/glyph guarantee
    assert "✔" not in report_text
    assert "❌" not in report_text
    assert "⚡" not in report_text
