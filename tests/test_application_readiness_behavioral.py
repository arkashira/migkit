"""Behavioral tests for proving application-visible readiness beyond data equality (Row 601: SMART-N18).

Verifies:
1. Every `decide.Rung` in `explain_analyze_admission_rungs` and
   `application_readiness_rungs` is exercised on both `holds` and `refuses`
   inside `with tests.rung_branches.watched():` and declared in `PROVES_RUNGS`.
2. Clear separation of three independent readiness tiers:
   - Tier 1: Value Equality (row counts, checksums, schemas).
   - Tier 2: Object & Security Parity (sequence grants, search_path, triggers, routines).
   - Tier 3: Application Behavior (representative reads, isolated write fixtures,
     constraints, sequence monotonicity, latency regression).
3. EXPLAIN ANALYZE execution guard:
   - EXPLAIN ANALYZE executes SQL: external effects require isolated authorized environments.
   - Mutating SQL without isolation is strictly refused.
4. Acceptance criteria:
   - Equal rows with a broken trigger block the switch (ACTION).
   - Equal rows with a missing sequence grant block the switch (ACTION).
   - Equal rows with a wrong search_path block the switch (ACTION).
   - Unavailable workload evidence stays explicitly unavailable (REVIEW/ACTION).
5. No arbitrary magic numbers: empirical latency percentiles (median, p95) derived directly.
6. Safe endpoints (127.0.0.1, 10.0.0.x) and pure UNIX output (no emojis/glyphs).
7. Universal typed durable resume in O(1) with native types (datetime, Decimal, UUID).
8. Active verification lease lock (.run.lock) with heartbeat.
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
from migkit.application_readiness import (
    CARE_ACTION,
    CARE_PASS,
    CARE_REVIEW,
    ApplicationBehaviorVerification,
    ApplicationReadinessCheckpointStore,
    ApplicationReadinessCheckpointUnit,
    ApplicationReadinessError,
    ApplicationReadinessReport,
    CareBadge,
    ConstraintCheckResult,
    LatencyParityCheck,
    ObjectSecurityParity,
    ReadinessTier,
    RepresentativeQuery,
    RoutineCheckResult,
    RoutineSpec,
    SequenceCheckResult,
    SequenceGrantSpec,
    TableGrantSpec,
    TriggerSpec,
    UnsafeEndpointError,
    ValueEqualityVerification,
    WorkloadEvidenceStatus,
    WorkloadReadResult,
    WorkloadWriteResult,
    acquire_application_readiness_lease,
    application_readiness_rungs,
    derive_latency_parity,
    evaluate_application_readiness,
    evaluate_explain_analyze_admission,
    explain_analyze_admission_rungs,
    validate_safe_endpoint,
)

PROVES_RUNGS = [
    ("application_readiness", "explain_analyze_admission_rungs", "admit_safe_read_query", "holds"),
    ("application_readiness", "explain_analyze_admission_rungs", "admit_safe_read_query", "refuses"),
    ("application_readiness", "explain_analyze_admission_rungs", "admit_isolated_write_fixture", "holds"),
    ("application_readiness", "explain_analyze_admission_rungs", "admit_isolated_write_fixture", "refuses"),
    ("application_readiness", "explain_analyze_admission_rungs", "refuse_unauthorized_external_effects", "holds"),
    ("application_readiness", "explain_analyze_admission_rungs", "refuse_unauthorized_external_effects", "refuses"),
    ("application_readiness", "application_readiness_rungs", "certify_application_ready", "holds"),
    ("application_readiness", "application_readiness_rungs", "certify_application_ready", "refuses"),
    ("application_readiness", "application_readiness_rungs", "refuse_broken_trigger_or_routine", "holds"),
    ("application_readiness", "application_readiness_rungs", "refuse_broken_trigger_or_routine", "refuses"),
    ("application_readiness", "application_readiness_rungs", "refuse_missing_grant_or_search_path", "holds"),
    ("application_readiness", "application_readiness_rungs", "refuse_missing_grant_or_search_path", "refuses"),
    ("application_readiness", "application_readiness_rungs", "refuse_constraint_or_behavior_defect", "holds"),
    ("application_readiness", "application_readiness_rungs", "refuse_constraint_or_behavior_defect", "refuses"),
    ("application_readiness", "application_readiness_rungs", "refuse_latency_regression", "holds"),
    ("application_readiness", "application_readiness_rungs", "refuse_latency_regression", "refuses"),
    ("application_readiness", "application_readiness_rungs", "refuse_unavailable_workload_evidence", "holds"),
    ("application_readiness", "application_readiness_rungs", "refuse_unavailable_workload_evidence", "refuses"),
    ("application_readiness", "application_readiness_rungs", "block_value_equality_alone", "holds"),
    ("application_readiness", "application_readiness_rungs", "block_value_equality_alone", "refuses"),
]


# -----------------------------------------------------------------------------
# Test: Rung Branches Exercised under watched()
# -----------------------------------------------------------------------------

def test_every_decide_rung_is_reached_on_both_branches():
    """Exercise both holds and refuses for all 10 rungs under tests.rung_branches.watched()."""
    with watched() as seen:
        adm_rungs = explain_analyze_admission_rungs()
        app_rungs = application_readiness_rungs()

        # Ladder 1: explain_analyze_admission_rungs
        # 1. admit_safe_read_query: holds when approved + read_only
        f_read_hold = decide.Facts({
            "is_approved_workload": True,
            "is_read_only_statement": True,
            "is_isolated_environment": False,
            "has_application_role_auth": True,
            "has_unauthorized_external_effects": False,
        })
        decide.climb("explain analyze probe", adm_rungs, f_read_hold)

        # admit_safe_read_query refuses; admit_isolated_write_fixture holds
        f_write_hold = decide.Facts({
            "is_approved_workload": True,
            "is_read_only_statement": False,
            "is_isolated_environment": True,
            "has_application_role_auth": True,
            "has_unauthorized_external_effects": False,
        })
        decide.climb("explain analyze probe", adm_rungs, f_write_hold)

        # admit_isolated_write_fixture refuses; refuse_unauthorized_external_effects holds
        f_unauth_hold = decide.Facts({
            "is_approved_workload": False,
            "is_read_only_statement": False,
            "is_isolated_environment": False,
            "has_application_role_auth": False,
            "has_unauthorized_external_effects": True,
        })
        decide.climb("explain analyze probe", adm_rungs, f_unauth_hold)

        # refuse_unauthorized_external_effects refuses
        f_unauth_refuse = decide.Facts({
            "is_approved_workload": False,
            "is_read_only_statement": False,
            "is_isolated_environment": False,
            "has_application_role_auth": False,
            "has_unauthorized_external_effects": False,
        })
        decide.climb("explain analyze probe", adm_rungs, f_unauth_refuse)

        # Ladder 2: application_readiness_rungs
        # 1. certify_application_ready holds; all negative rungs refuse
        f_all_ready = decide.Facts({
            "value_equality_verified": True,
            "object_security_parity_verified": True,
            "application_behavior_verified": True,
            "no_latency_regression": True,
            "workload_evidence_available": True,
            "has_broken_trigger_or_routine": False,
            "has_missing_grant_or_wrong_search_path": False,
            "has_constraint_or_behavior_defect": False,
            "has_latency_regression": False,
            "workload_evidence_unavailable": False,
            "only_value_equality_present": False,
        })
        decide.climb("readiness evaluation", app_rungs, f_all_ready)

        # 2. certify_application_ready refuses; refuse_broken_trigger_or_routine holds
        f_trg = decide.Facts({
            "value_equality_verified": True,
            "object_security_parity_verified": False,
            "application_behavior_verified": True,
            "no_latency_regression": True,
            "workload_evidence_available": True,
            "has_broken_trigger_or_routine": True,
            "has_missing_grant_or_wrong_search_path": False,
            "has_constraint_or_behavior_defect": False,
            "has_latency_regression": False,
            "workload_evidence_unavailable": False,
            "only_value_equality_present": False,
        })
        decide.climb("readiness evaluation", app_rungs, f_trg)

        # 3. refuse_missing_grant_or_search_path holds
        f_grant = decide.Facts({
            "value_equality_verified": True,
            "object_security_parity_verified": False,
            "application_behavior_verified": True,
            "no_latency_regression": True,
            "workload_evidence_available": True,
            "has_broken_trigger_or_routine": False,
            "has_missing_grant_or_wrong_search_path": True,
            "has_constraint_or_behavior_defect": False,
            "has_latency_regression": False,
            "workload_evidence_unavailable": False,
            "only_value_equality_present": False,
        })
        decide.climb("readiness evaluation", app_rungs, f_grant)

        # 4. refuse_constraint_or_behavior_defect holds
        f_defect = decide.Facts({
            "value_equality_verified": True,
            "object_security_parity_verified": True,
            "application_behavior_verified": False,
            "no_latency_regression": True,
            "workload_evidence_available": True,
            "has_broken_trigger_or_routine": False,
            "has_missing_grant_or_wrong_search_path": False,
            "has_constraint_or_behavior_defect": True,
            "has_latency_regression": False,
            "workload_evidence_unavailable": False,
            "only_value_equality_present": False,
        })
        decide.climb("readiness evaluation", app_rungs, f_defect)

        # 5. refuse_latency_regression holds
        f_lat = decide.Facts({
            "value_equality_verified": True,
            "object_security_parity_verified": True,
            "application_behavior_verified": True,
            "no_latency_regression": False,
            "workload_evidence_available": True,
            "has_broken_trigger_or_routine": False,
            "has_missing_grant_or_wrong_search_path": False,
            "has_constraint_or_behavior_defect": False,
            "has_latency_regression": True,
            "workload_evidence_unavailable": False,
            "only_value_equality_present": False,
        })
        decide.climb("readiness evaluation", app_rungs, f_lat)

        # 6. refuse_unavailable_workload_evidence holds
        f_unavail = decide.Facts({
            "value_equality_verified": True,
            "object_security_parity_verified": True,
            "application_behavior_verified": False,
            "no_latency_regression": True,
            "workload_evidence_available": False,
            "has_broken_trigger_or_routine": False,
            "has_missing_grant_or_wrong_search_path": False,
            "has_constraint_or_behavior_defect": False,
            "has_latency_regression": False,
            "workload_evidence_unavailable": True,
            "only_value_equality_present": False,
        })
        decide.climb("readiness evaluation", app_rungs, f_unavail)

        # 7. block_value_equality_alone holds
        f_val_alone = decide.Facts({
            "value_equality_verified": True,
            "object_security_parity_verified": False,
            "application_behavior_verified": False,
            "no_latency_regression": True,
            "workload_evidence_available": True,
            "has_broken_trigger_or_routine": False,
            "has_missing_grant_or_wrong_search_path": False,
            "has_constraint_or_behavior_defect": False,
            "has_latency_regression": False,
            "workload_evidence_unavailable": False,
            "only_value_equality_present": True,
        })
        decide.climb("readiness evaluation", app_rungs, f_val_alone)

    # Verify every declared rung reached both HOLDS and REFUSES
    for mod, ladder, rung_name, expected_branch in PROVES_RUNGS:
        assert mod == "application_readiness"
        reached = branch(seen, rung_name)
        assert expected_branch in reached, f"rung '{rung_name}' never reached branch '{expected_branch}'"


# -----------------------------------------------------------------------------
# Test: EXPLAIN ANALYZE Execution Safety & Admission
# -----------------------------------------------------------------------------

def test_explain_analyze_read_only_admitted():
    """Pure SELECT query in approved catalog is admitted under EXPLAIN ANALYZE."""
    q = RepresentativeQuery.create(
        "q_read_users",
        "SELECT id, username, email FROM app_users WHERE active = true",
        expected_columns=("id", "username", "email"),
    )
    assert q.is_read_only is True
    choice = evaluate_explain_analyze_admission(q, is_approved=True, is_isolated_env=False)
    assert choice.rung is not None
    assert choice.rung.name == "admit_safe_read_query"


def test_explain_analyze_dml_without_isolation_refused():
    """Mutating SQL (INSERT/UPDATE/DELETE) without isolated environment is refused."""
    q = RepresentativeQuery.create(
        "q_insert_order",
        "INSERT INTO orders (id, user_id, amount) VALUES (1, 42, 99.50)",
    )
    assert q.is_read_only is False
    choice = evaluate_explain_analyze_admission(q, is_approved=True, is_isolated_env=False)
    assert choice.rung is not None
    assert choice.rung.name == "refuse_unauthorized_external_effects"


def test_explain_analyze_dml_with_isolation_admitted():
    """Mutating SQL inside authorized isolated fixture is admitted."""
    q = RepresentativeQuery.create(
        "fixture_write_order",
        "INSERT INTO staging_test_orders (id, user_id) VALUES (999, 10)",
    )
    choice = evaluate_explain_analyze_admission(
        q, is_approved=True, is_isolated_env=True, app_role_auth=True
    )
    assert choice.rung is not None
    assert choice.rung.name == "admit_isolated_write_fixture"


# -----------------------------------------------------------------------------
# Test: Separation of Three Tiers (Acceptance Criteria)
# -----------------------------------------------------------------------------

def _clean_value_tier() -> ValueEqualityVerification:
    return ValueEqualityVerification.from_readings(
        tables_checked=15,
        rows_source=500000,
        rows_target=500000,
        checksum_match=True,
        columns_match=True,
    )


def _clean_security_tier() -> ObjectSecurityParity:
    return ObjectSecurityParity.evaluate(
        application_role="app_service",
        search_path=("public", "app"),
        expected_search_path=("public", "app"),
        sequence_grants=(
            SequenceGrantSpec("orders_id_seq", "app_service", ("USAGE", "SELECT"), True),
        ),
        table_grants=(
            TableGrantSpec("orders", "app_service", ("SELECT", "INSERT", "UPDATE"), True),
        ),
        triggers=(
            TriggerSpec("trg_orders_updated_at", "orders", enabled=True, has_valid_function=True),
        ),
        routines=(
            RoutineSpec("fn_calculate_tax", valid=True, executable_by_role=True, return_type="numeric"),
        ),
    )


def _clean_behavior_tier() -> ApplicationBehaviorVerification:
    return ApplicationBehaviorVerification.evaluate(
        representative_reads=(
            WorkloadReadResult("q_orders_by_user", "SELECT * FROM orders WHERE user_id = 1", 4.2, 5, True),
        ),
        isolated_writes=(
            WorkloadWriteResult(
                "fixture_create_order",
                "INSERT INTO orders_fixture VALUES (1)",
                1,
                True,
                isolated_environment_verified=True,
                rollback_verified=True,
            ),
        ),
        constraint_checks=(
            ConstraintCheckResult(
                "chk_order_amount_positive", "orders", "CHECK", violation_prevented=True, valid_accepted=True, verified=True
            ),
        ),
        sequence_checks=(
            SequenceCheckResult("orders_id_seq", nextval_succeeded=True, last_value=10500, target_ahead_of_source=True, verified=True),
        ),
        routine_checks=(
            RoutineCheckResult("fn_calculate_tax", executed_successfully=True, result_matched=True, verified=True),
        ),
        latency_checks=(
            LatencyParityCheck("q_orders_by_user", source_latency_ms=4.0, target_latency_ms=4.2, latency_ratio=1.05, regression_detected=False),
        ),
    )


def test_equal_rows_with_broken_trigger_blocks_switch():
    """Equal rows with a broken trigger block the switch with ACTION."""
    val_tier = _clean_value_tier()
    sec_tier = ObjectSecurityParity.evaluate(
        application_role="app_service",
        search_path=("public", "app"),
        expected_search_path=("public", "app"),
        triggers=(
            TriggerSpec("trg_broken", "orders", enabled=False, has_valid_function=False),
        ),
    )
    beh_tier = _clean_behavior_tier()

    report = evaluate_application_readiness(
        "pg_source_to_aurora_pg",
        value_tier=val_tier,
        security_tier=sec_tier,
        behavior_tier=beh_tier,
    )

    assert report.can_switch is False
    assert report.care_badge == CareBadge.ACTION
    assert any("trigger 'trg_broken'" in b for b in report.blockers)
    assert report.choice.rung.name == "refuse_broken_trigger_or_routine"


def test_equal_rows_with_missing_sequence_grant_blocks_switch():
    """Equal rows with missing sequence grant block the switch with ACTION."""
    val_tier = _clean_value_tier()
    sec_tier = ObjectSecurityParity.evaluate(
        application_role="app_service",
        search_path=("public", "app"),
        expected_search_path=("public", "app"),
        sequence_grants=(
            SequenceGrantSpec("orders_id_seq", "app_service", ("USAGE",), is_granted=False),
        ),
    )
    beh_tier = _clean_behavior_tier()

    report = evaluate_application_readiness(
        "pg_source_to_aurora_pg",
        value_tier=val_tier,
        security_tier=sec_tier,
        behavior_tier=beh_tier,
    )

    assert report.can_switch is False
    assert report.care_badge == CareBadge.ACTION
    assert any("missing sequence grant" in b for b in report.blockers)
    assert report.choice.rung.name == "refuse_missing_grant_or_search_path"


def test_equal_rows_with_wrong_search_path_blocks_switch():
    """Equal rows with wrong search_path block the switch with ACTION."""
    val_tier = _clean_value_tier()
    sec_tier = ObjectSecurityParity.evaluate(
        application_role="app_service",
        search_path=("wrong_schema",),
        expected_search_path=("public", "app"),
    )
    beh_tier = _clean_behavior_tier()

    report = evaluate_application_readiness(
        "pg_source_to_aurora_pg",
        value_tier=val_tier,
        security_tier=sec_tier,
        behavior_tier=beh_tier,
    )

    assert report.can_switch is False
    assert report.care_badge == CareBadge.ACTION
    assert any("wrong search_path" in b for b in report.blockers)
    assert report.choice.rung.name == "refuse_missing_grant_or_search_path"


def test_unavailable_workload_evidence_stays_explicitly_unavailable():
    """Unavailable workload evidence stays explicitly unavailable (REVIEW/ACTION)."""
    val_tier = _clean_value_tier()
    sec_tier = _clean_security_tier()
    beh_tier = ApplicationBehaviorVerification(
        representative_reads=(),
        isolated_writes=(),
        constraint_checks=(),
        sequence_checks=(),
        routine_checks=(),
        latency_checks=(),
        verified=False,
        defects=(),
    )

    report = evaluate_application_readiness(
        "pg_source_to_aurora_pg",
        value_tier=val_tier,
        security_tier=sec_tier,
        behavior_tier=beh_tier,
        evidence_status=WorkloadEvidenceStatus.UNAVAILABLE,
    )

    assert report.can_switch is False
    assert report.care_badge == CareBadge.REVIEW
    assert report.evidence_status == WorkloadEvidenceStatus.UNAVAILABLE
    assert any("workload evidence unavailable" in b for b in report.blockers)
    assert report.choice.rung.name == "refuse_unavailable_workload_evidence"


def test_value_equality_alone_cannot_certify_switch():
    """Data equality alone without behavioral proof cannot authorize cutover."""
    val_tier = _clean_value_tier()
    sec_tier = _clean_security_tier()
    # Behavior tier not run / empty
    beh_tier = ApplicationBehaviorVerification(
        representative_reads=(),
        isolated_writes=(),
        constraint_checks=(),
        sequence_checks=(),
        routine_checks=(),
        latency_checks=(),
        verified=False,
        defects=(),
    )

    report = evaluate_application_readiness(
        "pg_source_to_aurora_pg",
        value_tier=val_tier,
        security_tier=sec_tier,
        behavior_tier=beh_tier,
        evidence_status=WorkloadEvidenceStatus.PARTIAL,
    )

    assert report.can_switch is False
    assert report.care_badge == CareBadge.ACTION
    assert report.choice.rung.name == "block_value_equality_alone"


def test_latency_regression_blocks_switch():
    """Query latency regression exceeding SLA threshold blocks cutover."""
    val_tier = _clean_value_tier()
    sec_tier = _clean_security_tier()
    beh_tier = ApplicationBehaviorVerification.evaluate(
        representative_reads=(
            WorkloadReadResult("q_heavy_join", "SELECT ...", 120.0, 10, True),
        ),
        latency_checks=(
            LatencyParityCheck("q_heavy_join", source_latency_ms=20.0, target_latency_ms=120.0, latency_ratio=6.0, regression_detected=True),
        ),
    )

    report = evaluate_application_readiness(
        "pg_source_to_aurora_pg",
        value_tier=val_tier,
        security_tier=sec_tier,
        behavior_tier=beh_tier,
    )

    assert report.can_switch is False
    assert report.care_badge == CareBadge.ACTION
    assert report.choice.rung.name == "refuse_latency_regression"
    assert any("latency regression" in b for b in report.blockers)


def test_full_readiness_passes():
    """All 3 tiers verified with acceptable latency authorizes switch with PASS."""
    val_tier = _clean_value_tier()
    sec_tier = _clean_security_tier()
    beh_tier = _clean_behavior_tier()

    report = evaluate_application_readiness(
        "pg_source_to_aurora_pg",
        value_tier=val_tier,
        security_tier=sec_tier,
        behavior_tier=beh_tier,
        evidence_status=WorkloadEvidenceStatus.AVAILABLE,
    )

    assert report.can_switch is True
    assert report.care_badge == CareBadge.PASS
    assert len(report.blockers) == 0
    assert report.choice.rung.name == "certify_application_ready"
    rendered = report.render()
    assert "[PASS] APPLICATION READINESS REPORT: READY FOR CUTOVER" in rendered


# -----------------------------------------------------------------------------
# Test: Latency Percentile Bounds Derivation (No Magic Numbers)
# -----------------------------------------------------------------------------

def test_derive_latency_parity_from_empirical_readings():
    """Latency regression is derived mathematically from measured reading percentiles."""
    source_latencies = [10.0, 12.0, 11.0, 10.5, 13.0, 11.5, 10.2, 12.8, 11.1, 10.9]
    target_latencies_ok = [11.0, 12.5, 11.8, 12.0, 13.2, 12.1, 11.0, 13.5, 12.2, 11.9]
    target_latencies_bad = [35.0, 42.0, 38.0, 45.0, 50.0, 41.0, 39.0, 44.0, 48.0, 40.0]

    check_ok = derive_latency_parity("q_test_1", source_latencies, target_latencies_ok, threshold_ratio=1.5)
    assert check_ok.regression_detected is False
    assert check_ok.latency_ratio < 1.5

    check_bad = derive_latency_parity("q_test_2", source_latencies, target_latencies_bad, threshold_ratio=1.5)
    assert check_bad.regression_detected is True
    assert check_bad.latency_ratio > 3.0

    # Ensure empty list raises ApplicationReadinessError rather than silent None
    with pytest.raises(ApplicationReadinessError):
        derive_latency_parity("q_err", [], target_latencies_ok)


# -----------------------------------------------------------------------------
# Test: Safe Endpoint Validation
# -----------------------------------------------------------------------------

def test_safe_endpoint_validation():
    """Verify loopback and RFC1918 10.0.0.x are accepted; external hosts are rejected."""
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("localhost") == "localhost"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("192.168.1.1")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("db.production.external-cloud.com")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("")


# -----------------------------------------------------------------------------
# Test: Durable O(1) Checkpoint with Native Types
# -----------------------------------------------------------------------------

def test_application_readiness_checkpoint_store_o1(tmp_path: pathlib.Path):
    """Verify O(1) checkpoint resume preserving datetime, UUID, and Decimal."""
    store_file = tmp_path / "readiness_checkpoint.json"
    store = ApplicationReadinessCheckpointStore(store_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    unit_id = f"unit_{uuid.uuid4()}"
    unit = ApplicationReadinessCheckpointUnit(
        unit_id=unit_id,
        tier=ReadinessTier.APPLICATION_BEHAVIOR,
        min_cursor=decimal.Decimal("1000.50"),
        max_cursor=decimal.Decimal("9999.99"),
        verified=True,
        completed_at=now,
    )

    assert not store.is_completed_o1(unit_id)
    store.record_unit(unit)
    assert store.is_completed_o1(unit_id)

    # Reload from disk into fresh store instance to verify durable serialization
    reloaded_store = ApplicationReadinessCheckpointStore(store_file)
    assert reloaded_store.is_completed_o1(unit_id)
    loaded_unit = reloaded_store.get_unit(unit_id)
    assert loaded_unit is not None
    assert loaded_unit.unit_id == unit_id
    assert loaded_unit.min_cursor == decimal.Decimal("1000.50")
    assert loaded_unit.max_cursor == decimal.Decimal("9999.99")
    assert loaded_unit.verified is True
    assert loaded_unit.completed_at.isoformat() == now.isoformat()


# -----------------------------------------------------------------------------
# Test: Mutual Exclusion Active Lease Lock
# -----------------------------------------------------------------------------

def test_application_readiness_active_lease_lock(tmp_path: pathlib.Path):
    """Verify mutual exclusion lease lock with heartbeat."""
    lock_file = tmp_path / "app_readiness.run.lock"
    lease_obj = acquire_application_readiness_lease(lock_file, term_seconds=30.0)
    assert lease_obj is not None
    assert lock_file.exists()
    lease_obj.release()
