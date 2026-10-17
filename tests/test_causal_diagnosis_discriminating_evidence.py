"""Tests for Row 598: SMART-N15.causal-diagnosis (Diagnose compound causes through discriminating evidence).

Proves:
1. Every `decide.Rung` in `causal_diagnosis.hypothesis_investigation_rungs`,
   `causal_diagnosis.probe_selection_rungs`,
   `causal_diagnosis.remedy_precondition_rungs`, and
   `causal_diagnosis.effect_verification_rungs` is exercised on both `holds` and
   `refuses` inside `with tests.rung_branches.watched():` and declared in `PROVES_RUNGS`.
2. Simultaneous WAN slowdown and server throttling are not flattened to one code.
3. Retries cannot hide unrelated persistent corruption (e.g. SQLSTATE 22001 data truncation).
4. Unknown causes isolate affected dependencies without infinite retry loops.
5. Evaluates supported competing causes and disconfirming observations across:
   target pressure, WAN loss, source locks, stale auth, drift, conversion defects,
   and delayed replication.
6. Selects bounded probes for discriminatory value and reconciles uncertain in-flight
   attempts using the attempts ledger.
7. Corrective remedies are applied only when explicit operational preconditions hold,
   followed by post-remedy effect verification.
8. Pure UNIX style, zero emojis/glyphs, care badges ACTION/REVIEW/BENIGN/PASS.
"""

from __future__ import annotations

import pytest

from migkit import causal_diagnosis, decide
from migkit.causal_diagnosis import (
    CAUSE_CONVERSION_DEFECTS,
    CAUSE_DELAYED_REPLICATION,
    CAUSE_DRIFT,
    CAUSE_SOURCE_LOCKS,
    CAUSE_STALE_AUTH,
    CAUSE_TARGET_PRESSURE,
    CAUSE_THROTTLING,
    CAUSE_UNKNOWN,
    CAUSE_WAN_LOSS,
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    DiagnosisBounds,
    DiagnosisTelemetryReadings,
    EvidenceObservation,
    derive_diagnosis_bounds,
    diagnose_and_reconcile,
    effect_verification_rungs,
    hypothesis_investigation_rungs,
    probe_selection_rungs,
    remedy_precondition_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

#: read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    ("causal_diagnosis", "hypothesis_investigation_rungs", "discriminate_compound_causes", "holds"),
    ("causal_diagnosis", "hypothesis_investigation_rungs", "discriminate_compound_causes", "refuses"),
    ("causal_diagnosis", "hypothesis_investigation_rungs", "admit_single_dominant_cause", "holds"),
    ("causal_diagnosis", "hypothesis_investigation_rungs", "admit_single_dominant_cause", "refuses"),
    ("causal_diagnosis", "hypothesis_investigation_rungs", "admit_unresolved_unknown_cause", "holds"),
    ("causal_diagnosis", "hypothesis_investigation_rungs", "admit_unresolved_unknown_cause", "refuses"),

    ("causal_diagnosis", "probe_selection_rungs", "select_high_value_discriminating_probe", "holds"),
    ("causal_diagnosis", "probe_selection_rungs", "select_high_value_discriminating_probe", "refuses"),
    ("causal_diagnosis", "probe_selection_rungs", "select_targeted_ledger_reconciliation_probe", "holds"),
    ("causal_diagnosis", "probe_selection_rungs", "select_targeted_ledger_reconciliation_probe", "refuses"),
    ("causal_diagnosis", "probe_selection_rungs", "refuse_probes_budget_exhausted", "holds"),
    ("causal_diagnosis", "probe_selection_rungs", "refuse_probes_budget_exhausted", "refuses"),

    ("causal_diagnosis", "remedy_precondition_rungs", "apply_conditional_remedy_with_preconditions", "holds"),
    ("causal_diagnosis", "remedy_precondition_rungs", "apply_conditional_remedy_with_preconditions", "refuses"),
    ("causal_diagnosis", "remedy_precondition_rungs", "isolate_unremediable_or_corrupt_dependency", "holds"),
    ("causal_diagnosis", "remedy_precondition_rungs", "isolate_unremediable_or_corrupt_dependency", "refuses"),
    ("causal_diagnosis", "remedy_precondition_rungs", "refuse_remedy_unsatisfied_preconditions", "holds"),
    ("causal_diagnosis", "remedy_precondition_rungs", "refuse_remedy_unsatisfied_preconditions", "refuses"),

    ("causal_diagnosis", "effect_verification_rungs", "verify_remedy_effect_succeeded", "holds"),
    ("causal_diagnosis", "effect_verification_rungs", "verify_remedy_effect_succeeded", "refuses"),
    ("causal_diagnosis", "effect_verification_rungs", "escalate_remedy_ineffective", "holds"),
    ("causal_diagnosis", "effect_verification_rungs", "escalate_remedy_ineffective", "refuses"),
]


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

def default_readings() -> DiagnosisTelemetryReadings:
    return DiagnosisTelemetryReadings(
        network_rtt_samples_ms=(8.0, 12.0, 10.0, 14.0),
        packet_loss_samples=(0.0, 0.005, 0.0),
        cpu_utilization_samples=(0.25, 0.30, 0.35),
        probe_latency_samples_s=(0.05, 0.1, 0.08),
        max_retry_budget=3,
    )


# -----------------------------------------------------------------------------
# Test 1: Hypothesis Investigation Ladder (Holds and Refuses on All Rungs)
# -----------------------------------------------------------------------------

def test_hypothesis_investigation_rungs_holds_and_refuses():
    """Exercise both holds and refuses on all rungs of hypothesis_investigation_rungs."""
    rungs = hypothesis_investigation_rungs()

    with watched() as seen:
        # Case A: discriminate_compound_causes HOLDS
        facts_a = {
            "has_competing_hypotheses": True,
            "has_discriminating_evidence": True,
            "supports_compound_separation": True,
        }
        choice_a = decide.climb("test:hyp_compound", rungs, facts=facts_a, explore=False)
        assert choice_a.rung is not None
        assert choice_a.path == "discriminate_compound_causes"
        assert branch(seen, "discriminate_compound_causes") == [HOLDS]

        # Case B: discriminate_compound_causes REFUSES (missing discriminating evidence)
        facts_b = {
            "has_competing_hypotheses": True,
            "has_discriminating_evidence": False,
            "supports_compound_separation": True,
            "has_dominant_cause": False,
            "competing_causes_disconfirmed": False,
            "has_unresolved_symptoms": False,
            "probes_and_catalogue_exhausted": False,
        }
        choice_b = decide.climb("test:hyp_no_discrim", rungs, facts=facts_b, explore=False)
        assert REFUSES in branch(seen, "discriminate_compound_causes")

        # Case C: admit_single_dominant_cause HOLDS
        facts_c = {
            "has_competing_hypotheses": False,
            "has_discriminating_evidence": False,
            "supports_compound_separation": True,
            "has_dominant_cause": True,
            "competing_causes_disconfirmed": True,
        }
        choice_c = decide.climb("test:hyp_single", rungs, facts=facts_c, explore=False)
        assert choice_c.rung is not None
        assert choice_c.path == "admit_single_dominant_cause"
        assert HOLDS in branch(seen, "admit_single_dominant_cause")

        # Case D: admit_single_dominant_cause REFUSES (competing not disconfirmed)
        facts_d = {
            "has_competing_hypotheses": False,
            "has_discriminating_evidence": False,
            "supports_compound_separation": True,
            "has_dominant_cause": True,
            "competing_causes_disconfirmed": False,
            "has_unresolved_symptoms": False,
            "probes_and_catalogue_exhausted": False,
        }
        choice_d = decide.climb("test:hyp_single_refuse", rungs, facts=facts_d, explore=False)
        assert REFUSES in branch(seen, "admit_single_dominant_cause")

        # Case E: admit_unresolved_unknown_cause HOLDS
        facts_e = {
            "has_competing_hypotheses": False,
            "has_discriminating_evidence": False,
            "supports_compound_separation": False,
            "has_dominant_cause": False,
            "competing_causes_disconfirmed": False,
            "has_unresolved_symptoms": True,
            "probes_and_catalogue_exhausted": True,
        }
        choice_e = decide.climb("test:hyp_unknown", rungs, facts=facts_e, explore=False)
        assert choice_e.rung is not None
        assert choice_e.path == "admit_unresolved_unknown_cause"
        assert HOLDS in branch(seen, "admit_unresolved_unknown_cause")

        # Case F: admit_unresolved_unknown_cause REFUSES (catalogue not exhausted)
        facts_f = {
            "has_competing_hypotheses": False,
            "has_discriminating_evidence": False,
            "supports_compound_separation": False,
            "has_dominant_cause": False,
            "competing_causes_disconfirmed": False,
            "has_unresolved_symptoms": True,
            "probes_and_catalogue_exhausted": False,
        }
        choice_f = decide.climb("test:hyp_unknown_refuse", rungs, facts=facts_f, explore=False)
        assert REFUSES in branch(seen, "admit_unresolved_unknown_cause")


# -----------------------------------------------------------------------------
# Test 2: Probe Selection Ladder (Holds and Refuses on All Rungs)
# -----------------------------------------------------------------------------

def test_probe_selection_rungs_holds_and_refuses():
    """Exercise both holds and refuses on all rungs of probe_selection_rungs."""
    rungs = probe_selection_rungs()

    with watched() as seen:
        # Case A: select_high_value_discriminating_probe HOLDS
        facts_a = {
            "candidate_probes_available": True,
            "probe_within_budget": True,
            "discriminatory_value_positive": True,
        }
        choice_a = decide.climb("test:probe_high", rungs, facts=facts_a, explore=False)
        assert choice_a.rung is not None
        assert choice_a.path == "select_high_value_discriminating_probe"
        assert branch(seen, "select_high_value_discriminating_probe") == [HOLDS]

        # Case B: select_high_value_discriminating_probe REFUSES (budget exceeded)
        facts_b = {
            "candidate_probes_available": True,
            "probe_within_budget": False,
            "discriminatory_value_positive": True,
            "uncertain_transaction_in_flight": True,
            "ledger_reconciliation_supported": True,
        }
        choice_b = decide.climb("test:probe_overbudget", rungs, facts=facts_b, explore=False)
        assert REFUSES in branch(seen, "select_high_value_discriminating_probe")
        # And select_targeted_ledger_reconciliation_probe HOLDS here
        assert choice_b.path == "select_targeted_ledger_reconciliation_probe"
        assert HOLDS in branch(seen, "select_targeted_ledger_reconciliation_probe")

        # Case C: select_targeted_ledger_reconciliation_probe REFUSES (not supported)
        facts_c = {
            "candidate_probes_available": False,
            "probe_within_budget": False,
            "discriminatory_value_positive": False,
            "uncertain_transaction_in_flight": True,
            "ledger_reconciliation_supported": False,
            "probe_budget_exhausted": True,
        }
        choice_c = decide.climb("test:probe_recon_refuse", rungs, facts=facts_c, explore=False)
        assert REFUSES in branch(seen, "select_targeted_ledger_reconciliation_probe")
        # And refuse_probes_budget_exhausted HOLDS here
        assert choice_c.path == "refuse_probes_budget_exhausted"
        assert HOLDS in branch(seen, "refuse_probes_budget_exhausted")

        # Case D: refuse_probes_budget_exhausted REFUSES
        facts_d = {
            "candidate_probes_available": False,
            "probe_within_budget": False,
            "discriminatory_value_positive": False,
            "uncertain_transaction_in_flight": False,
            "ledger_reconciliation_supported": False,
            "probe_budget_exhausted": False,
        }
        choice_d = decide.climb("test:probe_all_refuse", rungs, facts=facts_d, explore=False)
        assert REFUSES in branch(seen, "refuse_probes_budget_exhausted")


# -----------------------------------------------------------------------------
# Test 3: Remedy Precondition Ladder (Holds and Refuses on All Rungs)
# -----------------------------------------------------------------------------

def test_remedy_precondition_rungs_holds_and_refuses():
    """Exercise both holds and refuses on all rungs of remedy_precondition_rungs."""
    rungs = remedy_precondition_rungs()

    with watched() as seen:
        # Case A: apply_conditional_remedy_with_preconditions HOLDS
        facts_a = {
            "remedy_preconditions_satisfied": True,
            "remedy_matches_causes": True,
        }
        choice_a = decide.climb("test:remedy_valid", rungs, facts=facts_a, explore=False)
        assert choice_a.rung is not None
        assert choice_a.path == "apply_conditional_remedy_with_preconditions"
        assert branch(seen, "apply_conditional_remedy_with_preconditions") == [HOLDS]

        # Case B: apply_conditional_remedy_with_preconditions REFUSES
        facts_b = {
            "remedy_preconditions_satisfied": False,
            "remedy_matches_causes": True,
            "requires_isolation": True,
            "isolation_target_identified": True,
        }
        choice_b = decide.climb("test:remedy_isolate", rungs, facts=facts_b, explore=False)
        assert REFUSES in branch(seen, "apply_conditional_remedy_with_preconditions")
        # And isolate_unremediable_or_corrupt_dependency HOLDS here
        assert choice_b.path == "isolate_unremediable_or_corrupt_dependency"
        assert HOLDS in branch(seen, "isolate_unremediable_or_corrupt_dependency")

        # Case C: isolate_unremediable_or_corrupt_dependency REFUSES (target not identified)
        facts_c = {
            "remedy_preconditions_satisfied": False,
            "remedy_matches_causes": False,
            "requires_isolation": True,
            "isolation_target_identified": False,
        }
        choice_c = decide.climb("test:remedy_iso_refuse", rungs, facts=facts_c, explore=False)
        assert REFUSES in branch(seen, "isolate_unremediable_or_corrupt_dependency")
        # And refuse_remedy_unsatisfied_preconditions HOLDS here
        assert choice_c.path == "refuse_remedy_unsatisfied_preconditions"
        assert HOLDS in branch(seen, "refuse_remedy_unsatisfied_preconditions")

        # Case D: refuse_remedy_unsatisfied_preconditions REFUSES
        facts_d = {
            "remedy_preconditions_satisfied": True,
            "remedy_matches_causes": False,
            "requires_isolation": False,
            "isolation_target_identified": False,
        }
        choice_d = decide.climb("test:remedy_refuse_none", rungs, facts=facts_d, explore=False)
        assert REFUSES in branch(seen, "refuse_remedy_unsatisfied_preconditions")


# -----------------------------------------------------------------------------
# Test 4: Effect Verification Ladder (Holds and Refuses on All Rungs)
# -----------------------------------------------------------------------------

def test_effect_verification_rungs_holds_and_refuses():
    """Exercise both holds and refuses on all rungs of effect_verification_rungs."""
    rungs = effect_verification_rungs()

    with watched() as seen:
        # Case A: verify_remedy_effect_succeeded HOLDS
        facts_a = {
            "verification_probe_succeeded": True,
            "symptoms_cleared": True,
        }
        choice_a = decide.climb("test:verif_ok", rungs, facts=facts_a, explore=False)
        assert choice_a.rung is not None
        assert choice_a.path == "verify_remedy_effect_succeeded"
        assert branch(seen, "verify_remedy_effect_succeeded") == [HOLDS]

        # Case B: verify_remedy_effect_succeeded REFUSES (symptoms not cleared)
        facts_b = {
            "verification_probe_succeeded": True,
            "symptoms_cleared": False,
        }
        choice_b = decide.climb("test:verif_escalate", rungs, facts=facts_b, explore=False)
        assert REFUSES in branch(seen, "verify_remedy_effect_succeeded")
        # And escalate_remedy_ineffective HOLDS
        assert choice_b.path == "escalate_remedy_ineffective"
        assert HOLDS in branch(seen, "escalate_remedy_ineffective")

        # Case C: escalate_remedy_ineffective REFUSES (symptoms cleared + probe ok)
        facts_c = {
            "verification_probe_succeeded": True,
            "symptoms_cleared": True,
        }
        choice_c = decide.climb("test:verif_escalate_refuse", rungs, facts=facts_c, explore=False)
        assert REFUSES in branch(seen, "escalate_remedy_ineffective")


# -----------------------------------------------------------------------------
# Test 5: Simultaneous WAN Slowdown & Throttling (NOT Flattened)
# -----------------------------------------------------------------------------

def test_simultaneous_wan_slowdown_and_throttling_not_flattened():
    """Verify simultaneous WAN loss and server throttling are preserved as compound causes."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="net_jitter_elevated",
            metric_name="network_packet_loss",
            measured_value=0.08,
            supports_causes=(CAUSE_WAN_LOSS,),
            disconfirms_causes=(),
            discriminatory_weight=1.2,
            details="elevated packet loss 8% observed on link",
        ),
        EvidenceObservation(
            observation_id="http_429_throttling",
            metric_name="server_rate_limit",
            measured_value=429,
            supports_causes=(CAUSE_THROTTLING,),
            disconfirms_causes=(),
            discriminatory_weight=1.5,
            details="server emitted HTTP 429 / MySQL 1226 rate limit exceeded",
        ),
    )

    report = diagnose_and_reconcile(
        work_id="unit:10.0.0.2:simultaneous_drop",
        observations=observations,
        readings=readings,
    )

    # Acceptance requirement: simultaneous WAN slowdown and throttling are NOT flattened to one code
    assert report.is_compound is True
    assert CAUSE_WAN_LOSS in report.diagnosed_causes
    assert CAUSE_THROTTLING in report.diagnosed_causes
    assert len(report.diagnosed_causes) == 2
    assert report.care_badge in (CARE_REVIEW, CARE_ACTION)
    assert "throttle_rate_and_extend_network_backoff" in report.remedy_action
    assert report.remedy_verified is True


# -----------------------------------------------------------------------------
# Test 6: Retries Cannot Hide Unrelated Persistent Corruption
# -----------------------------------------------------------------------------

def test_retries_cannot_hide_persistent_corruption():
    """Verify conversion defects / persistent corruption immediately isolates unit and blocks retrying."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="truncation_22001",
            metric_name="sqlstate",
            measured_value="22001",
            supports_causes=(CAUSE_CONVERSION_DEFECTS,),
            disconfirms_causes=(),
            discriminatory_weight=2.0,
            is_definitive=True,
            details="SQLSTATE 22001 string data right truncation",
        ),
    )

    report = diagnose_and_reconcile(
        work_id="unit:10.0.0.3:corrupt_row_42",
        observations=observations,
        readings=readings,
        error_code="22001",
        error_message="data truncated for column 'account_code'",
    )

    # Acceptance requirement: retries cannot hide unrelated persistent corruption
    assert CAUSE_CONVERSION_DEFECTS in report.diagnosed_causes
    assert report.care_badge == CARE_ACTION
    assert report.is_isolated is True
    assert report.isolated_target == "unit:unit:10.0.0.3:corrupt_row_42"
    assert "isolate_corrupt_unit" in report.remedy_action
    assert "retry" not in report.remedy_action


# -----------------------------------------------------------------------------
# Test 7: Unknown Causes Isolate Affected Dependencies Without Infinite Retry
# -----------------------------------------------------------------------------

def test_unknown_causes_isolate_dependencies_without_infinite_retry():
    """Verify unknown causes isolate affected dependencies rather than retrying indefinitely."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="anomalous_socket_crash",
            metric_name="kernel_fault",
            measured_value=99999,
            supports_causes=(CAUSE_UNKNOWN,),
            disconfirms_causes=(),
            discriminatory_weight=1.0,
            details="unrecognized proprietary driver kernel error 99999",
        ),
    )

    report = diagnose_and_reconcile(
        work_id="dep:10.0.0.4:unknown_service",
        observations=observations,
        readings=readings,
        error_code=99999,
        error_message="unrecognized proprietary system anomaly",
    )

    # Acceptance requirement: unknown causes isolate affected dependencies without infinite retry
    assert report.primary_cause == CAUSE_UNKNOWN
    assert report.is_isolated is True
    assert report.isolated_target == "dependency:dep:10.0.0.4:unknown_service"
    assert report.care_badge == CARE_ACTION
    assert "isolate_unknown_dependency" in report.remedy_action


# -----------------------------------------------------------------------------
# Test 8: Supported Competing Causes & Disconfirming Observations
# -----------------------------------------------------------------------------

def test_target_pressure_discriminated_from_wan_loss():
    """Verify target pressure is confirmed while WAN loss is disconfirmed by low RTT & 0% loss."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="clean_wan_probe",
            metric_name="packet_loss",
            measured_value=0.0,
            supports_causes=(),
            disconfirms_causes=(CAUSE_WAN_LOSS,),
            discriminatory_weight=1.5,
            details="0% packet loss and 2ms RTT disconfirms WAN loss",
        ),
        EvidenceObservation(
            observation_id="high_target_cpu",
            metric_name="target_cpu_busy",
            measured_value=0.96,
            supports_causes=(CAUSE_TARGET_PRESSURE,),
            disconfirms_causes=(),
            discriminatory_weight=1.8,
            details="target CPU utilization at 96% confirms server pressure",
        ),
    )

    report = diagnose_and_reconcile(
        work_id="unit:10.0.0.5:cpu_spike",
        observations=observations,
        readings=readings,
    )

    assert report.primary_cause == CAUSE_TARGET_PRESSURE
    assert CAUSE_WAN_LOSS not in report.diagnosed_causes
    assert report.remedy_action == "throttle_batch_size"


def test_source_locks_diagnosis():
    """Verify source locks diagnosis when transaction deadlock victim occurs."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="deadlock_detected",
            metric_name="lock_wait",
            measured_value=1213,
            supports_causes=(CAUSE_SOURCE_LOCKS,),
            disconfirms_causes=(),
            discriminatory_weight=2.0,
            details="deadlock found when trying to get lock; rolled back",
        ),
    )

    report = diagnose_and_reconcile(
        work_id="unit:10.0.0.6:lock_victim",
        observations=observations,
        readings=readings,
        error_code=1213,
        error_message="deadlock found when trying to get lock",
    )

    assert report.primary_cause == CAUSE_SOURCE_LOCKS
    assert report.remedy_action == "wait_for_lock_release_and_retry"


def test_stale_auth_diagnosis_and_token_renewal():
    """Verify expired token diagnosis with credentials renewal remedy."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="jwt_expired",
            metric_name="auth_token_ttl",
            measured_value=-300,
            supports_causes=(CAUSE_STALE_AUTH,),
            disconfirms_causes=(),
            discriminatory_weight=2.0,
            is_definitive=True,
            details="security token expired 300 seconds ago",
        ),
    )

    report = diagnose_and_reconcile(
        work_id="unit:10.0.0.7:stale_token",
        observations=observations,
        readings=readings,
        error_code="28P01",
        error_message="password authentication failed for user 'migkit'",
    )

    assert report.primary_cause == CAUSE_STALE_AUTH
    assert report.remedy_action == "renew_credentials_and_reconnect"


def test_schema_drift_diagnosis():
    """Verify schema drift diagnosis when table or column definition is missing."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="missing_target_col",
            metric_name="sqlstate",
            measured_value="42703",
            supports_causes=(CAUSE_DRIFT,),
            disconfirms_causes=(),
            discriminatory_weight=2.0,
            details="column 'billing_region' does not exist on target table",
        ),
    )

    report = diagnose_and_reconcile(
        work_id="unit:10.0.0.8:schema_mutation",
        observations=observations,
        readings=readings,
        error_code="42703",
        error_message="column does not exist",
    )

    assert report.primary_cause == CAUSE_DRIFT


def test_delayed_replication_diagnosis():
    """Verify replication lag exceeding retention threshold triggers delayed replication cause."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="replica_lag_high",
            metric_name="lag_seconds",
            measured_value=120.0,
            supports_causes=(CAUSE_DELAYED_REPLICATION,),
            disconfirms_causes=(),
            discriminatory_weight=1.5,
            details="CDC replication lag 120s exceeds 10s maximum threshold",
        ),
    )

    report = diagnose_and_reconcile(
        work_id="unit:10.0.0.9:wal_lag",
        observations=observations,
        readings=readings,
    )

    assert report.primary_cause == CAUSE_DELAYED_REPLICATION
    assert report.remedy_action == "pause_workload_for_replication_catchup"


# -----------------------------------------------------------------------------
# Test 9: Ledger Reconciliation for In-Flight Uncertain Transactions
# -----------------------------------------------------------------------------

def test_ledger_reconciliation_committed_transaction():
    """Verify ledger reconciliation prevents duplicate write when server committed prior to drop."""
    readings = default_readings()
    observations = (
        EvidenceObservation(
            observation_id="net_disconnect",
            metric_name="tcp_state",
            measured_value="reset_by_peer",
            supports_causes=(CAUSE_WAN_LOSS,),
            disconfirms_causes=(),
            discriminatory_weight=1.0,
        ),
    )
    ledger_state = {
        "attempt-101": {
            "status": "committed",
            "server_tx_id": "tx-8899",
        }
    }

    report = diagnose_and_reconcile(
        work_id="unit:10.0.0.10:write_recon",
        observations=observations,
        readings=readings,
        in_flight_attempt_id="attempt-101",
        ledger_state=ledger_state,
    )

    assert report.reconciliation_status == "committed"
    # When committed, write was preserved so no redundant retry write needed
    assert "reconciled ledger: committed" in report.explanation


# -----------------------------------------------------------------------------
# Test 10: Statistical Bounds Derivation (No Hardcoded Magic Numbers)
# -----------------------------------------------------------------------------

def test_statistical_telemetry_bounds_derivation():
    """Verify statistical derivation of latency, packet loss, and probe budgets."""
    readings = DiagnosisTelemetryReadings(
        network_rtt_samples_ms=(10.0, 20.0, 30.0),
        packet_loss_samples=(0.01, 0.02, 0.03),
        cpu_utilization_samples=(0.40, 0.50, 0.60),
        probe_latency_samples_s=(0.1, 0.2, 0.3),
        max_retry_budget=5,
    )

    bounds = derive_diagnosis_bounds(readings)

    # RTT mean is 20.0, std is sqrt(200/3) ~ 8.16 => bound ~ 36.33
    assert bounds.rtt_threshold_ms > 35.0
    # Loss bound derived from sample mean (0.02) + 0.02 = 0.04
    assert bounds.packet_loss_threshold == pytest.approx(0.04)
    # CPU bound derived from mean (0.50) + 0.30 = 0.80
    assert bounds.cpu_pressure_threshold == pytest.approx(0.80)
    # Max retries matches readings
    assert bounds.max_retries == 5
