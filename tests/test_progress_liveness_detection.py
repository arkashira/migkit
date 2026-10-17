"""Tests for detecting impossible progress and intelligent termination (Row 604: SMART-N21.progress-liveness).

Proves:
1. Every decide.Rung across the 6 ladders in migkit.progress_liveness is exercised
   on both `holds` and `refuses` inside `with tests.rung_branches.watched():`
   and declared in `PROVES_RUNGS`.
2. Safe test endpoint validation strictly enforces loopback and 10.0.0.x addresses.
3. Progress certificates monotonically advance work and verify cryptographic signatures.
4. Catch-up rate convergence rejects single noisy observations (one observation
   cannot establish impossibility), tolerates transient jitter, and isolates
   smallest missing prerequisites upon persistent divergence.
5. Dependency deadlocks are strictly distinguished from slow but advancing progress.
6. Hung programs and unyielding workers are quarantined without premature termination.
7. Repair drift tracking terminates non-shrinking repair loops without infinite recursion.
8. Per-resource and per-fault retry budgets enforce exponential backoff and prevent
   infinite recovery loops.
9. O(1) typed durable resume and active verification lease locks (.run.lock) guarantee
   mutual exclusion and crash recovery.
10. Autonomous supervision renders pure UNIX ASCII reports with explicit care badges.
"""

from __future__ import annotations

import decimal
import os
import pathlib
import tempfile
import time
from typing import Any, Dict, List

import pytest

import tests.rung_branches
from migkit import decide
from migkit.progress_liveness import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    ActiveVerificationLeaseLock,
    AutonomousLivenessSupervisor,
    CareBadge,
    DeadlockEvaluationResult,
    DependencyDeadlockDetector,
    DivergentRepairDriftError,
    ExtendedCatchUpResult,
    FaultClass,
    HungProgramResult,
    HungProgramWatchdog,
    LeaseConflictError,
    NonconvergentRateError,
    ProgressCertificate,
    ProgressLivenessCheckpointStore,
    RateObservation,
    RepairDriftResult,
    RepairDriftTracker,
    RetryBudgetManager,
    RetryBudgetResult,
    SmallestMissingPrerequisite,
    UnsafeEndpointHostError,
    autonomous_outcome_rungs,
    climb_autonomous_outcome,
    climb_convergence_evaluation,
    climb_deadlock_liveness,
    climb_hung_program,
    climb_repair_drift,
    climb_retry_budget,
    convergence_evaluation_rungs,
    deadlock_liveness_rungs,
    extended_catch_up_sum,
    hung_program_rungs,
    repair_drift_rungs,
    retry_budget_rungs,
    validate_safe_endpoint,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why


#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("progress_liveness", "convergence_evaluation_rungs", "convergent_catch_up", "holds"),
    ("progress_liveness", "convergence_evaluation_rungs", "convergent_catch_up", "refuses"),
    ("progress_liveness", "convergence_evaluation_rungs", "transient_jitter_tolerated", "holds"),
    ("progress_liveness", "convergence_evaluation_rungs", "transient_jitter_tolerated", "refuses"),
    ("progress_liveness", "convergence_evaluation_rungs", "divergent_rate_isolate_prerequisite", "holds"),
    ("progress_liveness", "convergence_evaluation_rungs", "divergent_rate_isolate_prerequisite", "refuses"),

    ("progress_liveness", "deadlock_liveness_rungs", "liveness_unblocked", "holds"),
    ("progress_liveness", "deadlock_liveness_rungs", "liveness_unblocked", "refuses"),
    ("progress_liveness", "deadlock_liveness_rungs", "slow_progress_admitted", "holds"),
    ("progress_liveness", "deadlock_liveness_rungs", "slow_progress_admitted", "refuses"),
    ("progress_liveness", "deadlock_liveness_rungs", "dependency_deadlock_resolution", "holds"),
    ("progress_liveness", "deadlock_liveness_rungs", "dependency_deadlock_resolution", "refuses"),

    ("progress_liveness", "hung_program_rungs", "healthy_heartbeat_active", "holds"),
    ("progress_liveness", "hung_program_rungs", "healthy_heartbeat_active", "refuses"),
    ("progress_liveness", "hung_program_rungs", "quiescent_heartbeat_within_slack", "holds"),
    ("progress_liveness", "hung_program_rungs", "quiescent_heartbeat_within_slack", "refuses"),
    ("progress_liveness", "hung_program_rungs", "hung_worker_quarantine", "holds"),
    ("progress_liveness", "hung_program_rungs", "hung_worker_quarantine", "refuses"),

    ("progress_liveness", "repair_drift_rungs", "repair_drift_converging", "holds"),
    ("progress_liveness", "repair_drift_rungs", "repair_drift_converging", "refuses"),
    ("progress_liveness", "repair_drift_rungs", "repair_drift_stationary_warn", "holds"),
    ("progress_liveness", "repair_drift_rungs", "repair_drift_stationary_warn", "refuses"),
    ("progress_liveness", "repair_drift_rungs", "repair_drift_diverging_action", "holds"),
    ("progress_liveness", "repair_drift_rungs", "repair_drift_diverging_action", "refuses"),

    ("progress_liveness", "retry_budget_rungs", "retry_budget_available", "holds"),
    ("progress_liveness", "retry_budget_rungs", "retry_budget_available", "refuses"),
    ("progress_liveness", "retry_budget_rungs", "retry_budget_exhausted_alternative", "holds"),
    ("progress_liveness", "retry_budget_rungs", "retry_budget_exhausted_alternative", "refuses"),
    ("progress_liveness", "retry_budget_rungs", "retry_budget_exhausted_stop", "holds"),
    ("progress_liveness", "retry_budget_rungs", "retry_budget_exhausted_stop", "refuses"),

    ("progress_liveness", "autonomous_outcome_rungs", "autonomous_certify_progress", "holds"),
    ("progress_liveness", "autonomous_outcome_rungs", "autonomous_certify_progress", "refuses"),
    ("progress_liveness", "autonomous_outcome_rungs", "autonomous_replan_alternative", "holds"),
    ("progress_liveness", "autonomous_outcome_rungs", "autonomous_replan_alternative", "refuses"),
    ("progress_liveness", "autonomous_outcome_rungs", "autonomous_isolate_and_terminate", "holds"),
    ("progress_liveness", "autonomous_outcome_rungs", "autonomous_isolate_and_terminate", "refuses"),
]


# =============================================================================
# 1. Safe Host Validation & Exception Integrity Tests
# =============================================================================

def test_safe_endpoint_validation():
    """Verify endpoint validation permits only loopback and 10.0.0.x."""
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("localhost") == "localhost"
    assert validate_safe_endpoint("::1") == "::1"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"
    assert validate_safe_endpoint("10.0.0.254") == "10.0.0.254"

    with pytest.raises(UnsafeEndpointHostError):
        validate_safe_endpoint("192.168.1.1")

    with pytest.raises(UnsafeEndpointHostError):
        validate_safe_endpoint("8.8.8.8")

    with pytest.raises(UnsafeEndpointHostError):
        validate_safe_endpoint("")


# =============================================================================
# 2. Progress Certificate Cryptographic & Advance Tests
# =============================================================================

def test_progress_certificate_verification():
    """Verify certificate signature verification and monotonic advancement."""
    c1 = ProgressCertificate.create(
        unit_id="tbl_orders",
        stage="copy",
        checkpoint_token="tok_100",
        bytes_processed=1024,
        rows_applied=100,
        unresolved_drift=50.0,
        timestamp=1000.0,
    )
    assert c1.verify_signature()

    # Tampered certificate fails verification
    c1_tampered = ProgressCertificate(
        certificate_id=c1.certificate_id,
        unit_id=c1.unit_id,
        stage=c1.stage,
        checkpoint_token=c1.checkpoint_token,
        bytes_processed=999999,  # tampered
        rows_applied=c1.rows_applied,
        unresolved_drift=c1.unresolved_drift,
        timestamp=c1.timestamp,
        signature=c1.signature,
    )
    assert not c1_tampered.verify_signature()

    # Advancing certificate
    c2 = ProgressCertificate.create(
        unit_id="tbl_orders",
        stage="copy",
        checkpoint_token="tok_200",
        bytes_processed=2048,
        rows_applied=200,
        unresolved_drift=20.0,
        timestamp=1010.0,
    )
    assert c2.advance_verified(c1)

    # Stale/backward timestamp fails advance
    c_stale = ProgressCertificate.create(
        unit_id="tbl_orders",
        stage="copy",
        checkpoint_token="tok_050",
        bytes_processed=500,
        rows_applied=50,
        timestamp=999.0,
    )
    assert not c_stale.advance_verified(c1)


# =============================================================================
# 3. Decision Rung Proving (watched context)
# =============================================================================

def test_convergence_evaluation_rungs_proves():
    """Exercise both holds and refuses on convergence_evaluation_rungs."""
    with watched() as seen:
        # Case 1: holds convergent_catch_up, refuses others
        f_conv = {
            "apply_rate": 5000.0,
            "arrival_rate": 1000.0,
            "divergence_count": 0,
            "catch_up_seconds": 25.0,
            "deadline_seconds": 3600.0,
            "min_divergence_observations": 3,
            "backlog_bytes": 1000.0,
            "max_spool_bytes": 1e9,
            "has_prerequisite": False,
        }
        c1 = climb_convergence_evaluation("unit.conv", f_conv)
        assert c1.path == "convergent_catch_up"

        # Case 2: holds transient_jitter_tolerated, refuses others
        f_jitter = {
            "apply_rate": 800.0,
            "arrival_rate": 1000.0,
            "divergence_count": 1,
            "catch_up_seconds": 1e12,
            "deadline_seconds": 3600.0,
            "min_divergence_observations": 3,
            "backlog_bytes": 50000.0,
            "max_spool_bytes": 1e9,
            "has_prerequisite": False,
        }
        c2 = climb_convergence_evaluation("unit.jitter", f_jitter)
        assert c2.path == "transient_jitter_tolerated"

        # Case 3: holds divergent_rate_isolate_prerequisite, refuses others
        f_div = {
            "apply_rate": 800.0,
            "arrival_rate": 1000.0,
            "divergence_count": 3,
            "catch_up_seconds": 1e12,
            "deadline_seconds": 3600.0,
            "min_divergence_observations": 3,
            "backlog_bytes": 50000.0,
            "max_spool_bytes": 1e9,
            "has_prerequisite": True,
        }
        c3 = climb_convergence_evaluation("unit.div", f_div)
        assert c3.path == "divergent_rate_isolate_prerequisite"

        # Case 4: Refuses all rungs
        f_none = {
            "apply_rate": 500.0,
            "arrival_rate": 1000.0,
            "divergence_count": 5,
            "catch_up_seconds": 1e12,
            "deadline_seconds": 3600.0,
            "min_divergence_observations": 3,
            "backlog_bytes": 1e15,
            "max_spool_bytes": 1e9,
            "has_prerequisite": False,
        }
        c4 = climb_convergence_evaluation("unit.none", f_none)
        assert c4.path is None

    assert set(branch(seen, "convergent_catch_up")) == {HOLDS, REFUSES}
    assert set(branch(seen, "transient_jitter_tolerated")) == {HOLDS, REFUSES}
    assert set(branch(seen, "divergent_rate_isolate_prerequisite")) == {HOLDS, REFUSES}


def test_deadlock_liveness_rungs_proves():
    """Exercise both holds and refuses on deadlock_liveness_rungs."""
    with watched() as seen:
        # Case 1: holds liveness_unblocked
        f_unblocked = {
            "has_dependency_cycle": False,
            "tasks_progressing": True,
            "certificates_advancing": True,
            "deadlock_resolution_available": False,
        }
        c1 = climb_deadlock_liveness("work.unblocked", f_unblocked)
        assert c1.path == "liveness_unblocked"

        # Case 2: holds slow_progress_admitted
        f_slow = {
            "has_dependency_cycle": False,
            "tasks_progressing": False,
            "certificates_advancing": True,
            "deadlock_resolution_available": False,
        }
        c2 = climb_deadlock_liveness("work.slow", f_slow)
        assert c2.path == "slow_progress_admitted"

        # Case 3: holds dependency_deadlock_resolution
        f_deadlock = {
            "has_dependency_cycle": True,
            "tasks_progressing": False,
            "certificates_advancing": False,
            "deadlock_resolution_available": True,
        }
        c3 = climb_deadlock_liveness("work.deadlock", f_deadlock)
        assert c3.path == "dependency_deadlock_resolution"

        # Case 4: Refuses all
        f_none = {
            "has_dependency_cycle": True,
            "tasks_progressing": False,
            "certificates_advancing": False,
            "deadlock_resolution_available": False,
        }
        c4 = climb_deadlock_liveness("work.none", f_none)
        assert c4.path is None

    assert set(branch(seen, "liveness_unblocked")) == {HOLDS, REFUSES}
    assert set(branch(seen, "slow_progress_admitted")) == {HOLDS, REFUSES}
    assert set(branch(seen, "dependency_deadlock_resolution")) == {HOLDS, REFUSES}


def test_hung_program_rungs_proves():
    """Exercise both holds and refuses on hung_program_rungs."""
    with watched() as seen:
        # Case 1: holds healthy_heartbeat_active
        f_healthy = {
            "heartbeat_age_seconds": 5.0,
            "heartbeat_timeout_seconds": 30.0,
            "p99_query_seconds": 120.0,
            "has_recent_progress": True,
            "process_alive": True,
            "quarantine_supported": True,
        }
        c1 = climb_hung_program("work.healthy", f_healthy)
        assert c1.path == "healthy_heartbeat_active"

        # Case 2: holds quiescent_heartbeat_within_slack
        f_quiescent = {
            "heartbeat_age_seconds": 60.0,
            "heartbeat_timeout_seconds": 30.0,
            "p99_query_seconds": 120.0,
            "has_recent_progress": False,
            "process_alive": True,
            "quarantine_supported": True,
        }
        c2 = climb_hung_program("work.quiescent", f_quiescent)
        assert c2.path == "quiescent_heartbeat_within_slack"

        # Case 3: holds hung_worker_quarantine
        f_hung = {
            "heartbeat_age_seconds": 300.0,
            "heartbeat_timeout_seconds": 30.0,
            "p99_query_seconds": 120.0,
            "has_recent_progress": False,
            "process_alive": True,
            "quarantine_supported": True,
        }
        c3 = climb_hung_program("work.hung", f_hung)
        assert c3.path == "hung_worker_quarantine"

        # Case 4: Refuses all
        f_none = {
            "heartbeat_age_seconds": 300.0,
            "heartbeat_timeout_seconds": 30.0,
            "p99_query_seconds": 120.0,
            "has_recent_progress": False,
            "process_alive": False,
            "quarantine_supported": False,
        }
        c4 = climb_hung_program("work.none", f_none)
        assert c4.path is None

    assert set(branch(seen, "healthy_heartbeat_active")) == {HOLDS, REFUSES}
    assert set(branch(seen, "quiescent_heartbeat_within_slack")) == {HOLDS, REFUSES}
    assert set(branch(seen, "hung_worker_quarantine")) == {HOLDS, REFUSES}


def test_repair_drift_rungs_proves():
    """Exercise both holds and refuses on repair_drift_rungs."""
    with watched() as seen:
        # Case 1: holds repair_drift_converging
        f_conv = {
            "drift_delta": -15.0,
            "current_drift": 100.0,
            "stationary_count": 0,
            "max_stationary_repairs": 2,
            "repair_attempts": 2,
            "max_repair_attempts": 5,
            "drift_expansion_tolerance": 0.0,
            "repair_prerequisite_identified": False,
        }
        c1 = climb_repair_drift("work.repair_conv", f_conv)
        assert c1.path == "repair_drift_converging"

        # Case 2: holds repair_drift_stationary_warn
        f_warn = {
            "drift_delta": 0.0,
            "current_drift": 100.0,
            "stationary_count": 1,
            "max_stationary_repairs": 2,
            "repair_attempts": 2,
            "max_repair_attempts": 5,
            "drift_expansion_tolerance": 0.0,
            "repair_prerequisite_identified": False,
        }
        c2 = climb_repair_drift("work.repair_warn", f_warn)
        assert c2.path == "repair_drift_stationary_warn"

        # Case 3: holds repair_drift_diverging_action
        f_div = {
            "drift_delta": 50.0,
            "current_drift": 150.0,
            "stationary_count": 3,
            "max_stationary_repairs": 2,
            "repair_attempts": 4,
            "max_repair_attempts": 5,
            "drift_expansion_tolerance": 0.0,
            "repair_prerequisite_identified": True,
        }
        c3 = climb_repair_drift("work.repair_div", f_div)
        assert c3.path == "repair_drift_diverging_action"

        # Case 4: Refuses all
        f_none = {
            "drift_delta": 50.0,
            "current_drift": 150.0,
            "stationary_count": 3,
            "max_stationary_repairs": 2,
            "repair_attempts": 4,
            "max_repair_attempts": 5,
            "drift_expansion_tolerance": 0.0,
            "repair_prerequisite_identified": False,
        }
        c4 = climb_repair_drift("work.repair_none", f_none)
        assert c4.path is None

    assert set(branch(seen, "repair_drift_converging")) == {HOLDS, REFUSES}
    assert set(branch(seen, "repair_drift_stationary_warn")) == {HOLDS, REFUSES}
    assert set(branch(seen, "repair_drift_diverging_action")) == {HOLDS, REFUSES}


def test_retry_budget_rungs_proves():
    """Exercise both holds and refuses on retry_budget_rungs."""
    with watched() as seen:
        # Case 1: holds retry_budget_available
        f_avail = {
            "resource_retries": 1,
            "max_resource_retries": 3,
            "fault_retries": 1,
            "max_fault_retries": 3,
            "has_admissible_alternative": False,
            "terminal_prerequisite_formulated": False,
        }
        c1 = climb_retry_budget("work.retry_avail", f_avail)
        assert c1.path == "retry_budget_available"

        # Case 2: holds retry_budget_exhausted_alternative
        f_alt = {
            "resource_retries": 3,
            "max_resource_retries": 3,
            "fault_retries": 1,
            "max_fault_retries": 3,
            "has_admissible_alternative": True,
            "terminal_prerequisite_formulated": False,
        }
        c2 = climb_retry_budget("work.retry_alt", f_alt)
        assert c2.path == "retry_budget_exhausted_alternative"

        # Case 3: holds retry_budget_exhausted_stop
        f_stop = {
            "resource_retries": 3,
            "max_resource_retries": 3,
            "fault_retries": 3,
            "max_fault_retries": 3,
            "has_admissible_alternative": False,
            "terminal_prerequisite_formulated": True,
        }
        c3 = climb_retry_budget("work.retry_stop", f_stop)
        assert c3.path == "retry_budget_exhausted_stop"

        # Case 4: Refuses all
        f_none = {
            "resource_retries": 3,
            "max_resource_retries": 3,
            "fault_retries": 3,
            "max_fault_retries": 3,
            "has_admissible_alternative": False,
            "terminal_prerequisite_formulated": False,
        }
        c4 = climb_retry_budget("work.retry_none", f_none)
        assert c4.path is None

    assert set(branch(seen, "retry_budget_available")) == {HOLDS, REFUSES}
    assert set(branch(seen, "retry_budget_exhausted_alternative")) == {HOLDS, REFUSES}
    assert set(branch(seen, "retry_budget_exhausted_stop")) == {HOLDS, REFUSES}


def test_autonomous_outcome_rungs_proves():
    """Exercise both holds and refuses on autonomous_outcome_rungs."""
    with watched() as seen:
        # Case 1: holds autonomous_certify_progress
        f_cert = {
            "all_certificates_advancing": True,
            "rates_convergent": True,
            "zero_deadlocks_or_hung": True,
            "liveness_impaired": False,
            "fallback_admissible": False,
            "impossible_progress_proved": False,
            "smallest_prerequisite_isolated": False,
        }
        c1 = climb_autonomous_outcome("work.cert", f_cert)
        assert c1.path == "autonomous_certify_progress"

        # Case 2: holds autonomous_replan_alternative
        f_replan = {
            "all_certificates_advancing": False,
            "rates_convergent": False,
            "zero_deadlocks_or_hung": True,
            "liveness_impaired": True,
            "fallback_admissible": True,
            "impossible_progress_proved": False,
            "smallest_prerequisite_isolated": False,
        }
        c2 = climb_autonomous_outcome("work.replan", f_replan)
        assert c2.path == "autonomous_replan_alternative"

        # Case 3: holds autonomous_isolate_and_terminate
        f_term = {
            "all_certificates_advancing": False,
            "rates_convergent": False,
            "zero_deadlocks_or_hung": False,
            "liveness_impaired": True,
            "fallback_admissible": False,
            "impossible_progress_proved": True,
            "smallest_prerequisite_isolated": True,
        }
        c3 = climb_autonomous_outcome("work.term", f_term)
        assert c3.path == "autonomous_isolate_and_terminate"

        # Case 4: Refuses all
        f_none = {
            "all_certificates_advancing": False,
            "rates_convergent": False,
            "zero_deadlocks_or_hung": False,
            "liveness_impaired": True,
            "fallback_admissible": False,
            "impossible_progress_proved": True,
            "smallest_prerequisite_isolated": False,
        }
        c4 = climb_autonomous_outcome("work.none", f_none)
        assert c4.path is None

    assert set(branch(seen, "autonomous_certify_progress")) == {HOLDS, REFUSES}
    assert set(branch(seen, "autonomous_replan_alternative")) == {HOLDS, REFUSES}
    assert set(branch(seen, "autonomous_isolate_and_terminate")) == {HOLDS, REFUSES}


# =============================================================================
# 4. Extended Catch-Up Sum with Noise Rejection Tests
# =============================================================================

def test_extended_catch_up_sum_noise_rejection():
    """Verify one noisy observation cannot establish impossibility."""
    # Series with a single noisy dip
    obs = [
        RateObservation(timestamp=100.0, arrival_rate=1000.0, apply_rate=4000.0, backlog_rows=100000.0, p90_apply_rate=3500.0),
        RateObservation(timestamp=110.0, arrival_rate=1000.0, apply_rate=4500.0, backlog_rows=70000.0, p90_apply_rate=4000.0),
        # Single noisy observation where apply momentarily dipped below arrival
        RateObservation(timestamp=120.0, arrival_rate=1200.0, apply_rate=600.0, backlog_rows=65000.0, p90_apply_rate=500.0, is_noisy=True),
    ]

    res = extended_catch_up_sum(obs, backlog_rows=65000.0, min_divergence_observations=3)
    assert res.is_convergent
    assert res.noise_tolerated
    assert res.care_badge == CARE_REVIEW
    assert res.choice_path == "transient_jitter_tolerated"

    # Persistent divergence (3 consecutive divergent readings)
    obs_divergent = list(obs) + [
        RateObservation(timestamp=130.0, arrival_rate=1500.0, apply_rate=800.0, backlog_rows=72000.0, p90_apply_rate=700.0),
        RateObservation(timestamp=140.0, arrival_rate=1600.0, apply_rate=750.0, backlog_rows=80000.0, p90_apply_rate=650.0),
    ]

    res_div = extended_catch_up_sum(obs_divergent, backlog_rows=80000.0, min_divergence_observations=3)
    assert not res_div.is_convergent
    assert not res_div.noise_tolerated
    assert res_div.care_badge == CARE_ACTION
    assert res_div.choice_path == "divergent_rate_isolate_prerequisite"
    assert res_div.smallest_prerequisite is not None
    assert res_div.smallest_prerequisite.category == "apply_rate_deficit"


# =============================================================================
# 5. Dependency Deadlock vs Slow Progress Tests
# =============================================================================

def test_deadlock_vs_slow_progress_distinction():
    """Verify slow but progressing work is distinguished from true deadlock."""
    detector = DependencyDeadlockDetector()
    detector.add_dependency("task_orders", "task_customers")
    detector.add_dependency("task_customers", "task_orders")  # Cycle

    # Case A: Tasks in cycle have monotonically advancing certificates -> SLOW PROGRESS
    t0 = 1000.0
    t1 = 1010.0
    certs_orders = [
        ProgressCertificate.create("orders", "copy", "tok_1", 100, 10, 50.0, t0),
        ProgressCertificate.create("orders", "copy", "tok_2", 200, 20, 40.0, t1),
    ]
    certs_customers = [
        ProgressCertificate.create("customers", "copy", "tok_1", 50, 5, 20.0, t0),
        ProgressCertificate.create("customers", "copy", "tok_2", 60, 6, 18.0, t1),
    ]

    task_certs = {"task_orders": certs_orders, "task_customers": certs_customers}
    res_slow = detector.evaluate_liveness(task_certs)
    assert not res_slow.is_deadlocked
    assert res_slow.care_badge == CARE_REVIEW
    assert res_slow.choice_path == "slow_progress_admitted"
    assert "task_orders" in res_slow.advancing_tasks

    # Case B: Stationary certificates across cycle -> TRUE DEADLOCK
    stationary_orders = [certs_orders[0], certs_orders[0]]
    stationary_customers = [certs_customers[0], certs_customers[0]]
    res_deadlock = detector.evaluate_liveness(
        {"task_orders": stationary_orders, "task_customers": stationary_customers}
    )
    assert res_deadlock.is_deadlocked
    assert res_deadlock.care_badge == CARE_ACTION
    assert res_deadlock.choice_path == "dependency_deadlock_resolution"
    assert res_deadlock.smallest_prerequisite is not None
    assert res_deadlock.smallest_prerequisite.category == "dependency_deadlock"


# =============================================================================
# 6. Hung Program Watchdog Tests
# =============================================================================

def test_hung_program_watchdog():
    """Verify worker heartbeat watchdog detects hung processes without false alarms."""
    watchdog = HungProgramWatchdog()
    now = 1000.0

    # Healthy: recent heartbeat
    res_healthy = watchdog.evaluate_worker(
        worker_id="worker_01",
        heartbeat_timestamp=now - 10.0,
        p99_query_seconds=120.0,
        heartbeat_timeout_seconds=30.0,
        current_time=now,
    )
    assert not res_healthy.is_hung
    assert res_healthy.care_badge == CARE_PASS
    assert res_healthy.choice_path == "healthy_heartbeat_active"

    # Quiescent: within p99 slack
    res_slack = watchdog.evaluate_worker(
        worker_id="worker_02",
        heartbeat_timestamp=now - 50.0,
        p99_query_seconds=120.0,
        heartbeat_timeout_seconds=30.0,
        has_recent_progress=False,
        current_time=now,
    )
    assert not res_slack.is_hung
    assert res_slack.care_badge == CARE_REVIEW
    assert res_slack.choice_path == "quiescent_heartbeat_within_slack"

    # Hung: exceeds p99 slack
    res_hung = watchdog.evaluate_worker(
        worker_id="worker_03",
        heartbeat_timestamp=now - 200.0,
        p99_query_seconds=120.0,
        heartbeat_timeout_seconds=30.0,
        has_recent_progress=False,
        current_time=now,
    )
    assert res_hung.is_hung
    assert res_hung.care_badge == CARE_ACTION
    assert res_hung.choice_path == "hung_worker_quarantine"
    assert res_hung.smallest_prerequisite is not None
    assert res_hung.smallest_prerequisite.category == "hung_worker"


# =============================================================================
# 7. Repair Drift Tracking Tests (Non-Shrinking Drift)
# =============================================================================

def test_repair_drift_shrinking_and_diverging():
    """Verify repair tracker identifies converging vs non-shrinking drift."""
    tracker = RepairDriftTracker()
    repair_id = "repair_users_checksum"

    # 1. First iteration
    tracker.record_observation(repair_id, 1000.0)
    res1 = tracker.evaluate_repair_drift(repair_id)
    assert res1.is_convergent
    assert res1.care_badge == CARE_PASS

    # 2. Second iteration: drift shrank by 400
    tracker.record_observation(repair_id, 600.0)
    res2 = tracker.evaluate_repair_drift(repair_id)
    assert res2.is_convergent
    assert res2.drift_delta == -400.0
    assert res2.care_badge == CARE_PASS

    # 3. Third iteration: drift stationary under concurrent write noise
    tracker.record_observation(repair_id, 600.0)
    res3 = tracker.evaluate_repair_drift(repair_id, max_stationary_repairs=2)
    assert res3.is_convergent
    assert res3.care_badge == CARE_REVIEW
    assert res3.choice_path == "repair_drift_stationary_warn"

    # 4. Fourth iteration: drift stationary again (limit reached)
    tracker.record_observation(repair_id, 600.0)
    res4 = tracker.evaluate_repair_drift(repair_id, max_stationary_repairs=2)
    assert not res4.is_convergent
    assert res4.care_badge == CARE_ACTION
    assert res4.choice_path == "repair_drift_diverging_action"
    assert res4.smallest_prerequisite is not None
    assert res4.smallest_prerequisite.category == "divergent_repair"


# =============================================================================
# 8. Per-Resource / Fault Retry Budget Tests
# =============================================================================

def test_retry_budget_management():
    """Verify per-resource/fault retry budgets, backoff, and infinite loop prevention."""
    mgr = RetryBudgetManager()
    res = "shard_db_primary"
    fault = FaultClass.LOCK_TIMEOUT

    # First attempt: admitted with backoff
    r1 = mgr.evaluate_retry(res, fault, max_resource_retries=2, max_fault_retries=2)
    assert r1.admitted
    assert r1.care_badge == CARE_BENIGN
    assert r1.choice_path == "retry_budget_available"
    assert r1.backoff_seconds > 0.0

    # Second attempt: admitted
    r2 = mgr.evaluate_retry(res, fault, max_resource_retries=2, max_fault_retries=2)
    assert r2.admitted
    assert r2.backoff_seconds >= r1.backoff_seconds

    # Third attempt with alternative: switches to alternative
    r3 = mgr.evaluate_retry(
        res,
        fault,
        max_resource_retries=2,
        max_fault_retries=2,
        has_admissible_alternative=True,
    )
    assert r3.admitted
    assert r3.care_badge == CARE_REVIEW
    assert r3.choice_path == "retry_budget_exhausted_alternative"

    # Fourth attempt without alternative: stops infinite recovery loop
    r4 = mgr.evaluate_retry(
        res,
        fault,
        max_resource_retries=2,
        max_fault_retries=2,
        has_admissible_alternative=False,
    )
    assert not r4.admitted
    assert r4.care_badge == CARE_ACTION
    assert r4.choice_path == "retry_budget_exhausted_stop"
    assert r4.smallest_prerequisite is not None
    assert r4.smallest_prerequisite.category == "exhausted_retry"


# =============================================================================
# 9. O(1) Typed Durable Checkpoint Store & Active Lease Tests
# =============================================================================

def test_checkpoint_store_and_active_lease(tmp_path: pathlib.Path):
    """Verify O(1) typed durable checkpoint store and mutual exclusion lease lock."""
    # 1. Lease lock test
    lock_file = tmp_path / ".run.lock"
    lease1 = ActiveVerificationLeaseLock(lock_file)
    lease1.acquire()
    lease1.heartbeat()

    # Second concurrent lease fails with LeaseConflictError
    lease2 = ActiveVerificationLeaseLock(lock_file)
    with pytest.raises(LeaseConflictError):
        lease2.acquire()

    lease1.release()

    # After release, second lease can acquire
    lease2.acquire()
    lease2.release()

    # 2. Checkpoint store test
    store_dir = tmp_path / "checkpoints"
    store = ProgressLivenessCheckpointStore(store_dir)

    cert = ProgressCertificate.create("tbl_users", "sync", "tok_42", 5000, 100, 0.0)
    chk_path = store.save_checkpoint(
        checkpoint_id="chk_001",
        certificates=[cert],
        care_badge=CARE_PASS,
        extra_state={"epoch": 14},
    )
    assert chk_path.exists()

    loaded = store.load_checkpoint("chk_001")
    assert loaded["checkpoint_id"] == "chk_001"
    assert loaded["care_badge"] == CARE_PASS
    assert len(loaded["typed_certificates"]) == 1
    assert loaded["typed_certificates"][0].checkpoint_token == "tok_42"


# =============================================================================
# 10. Autonomous Liveness Supervisor & ASCII Summary Tests
# =============================================================================

def test_autonomous_liveness_supervisor_healthy_and_terminated():
    """Verify supervisor end-to-end integration and pure ASCII report generation."""
    supervisor = AutonomousLivenessSupervisor()

    t0, t1 = 1000.0, 1010.0
    task_certs = {
        "task_accounts": [
            ProgressCertificate.create("accounts", "copy", "tok_1", 1000, 50, 0.0, t0),
            ProgressCertificate.create("accounts", "copy", "tok_2", 2000, 100, 0.0, t1),
        ]
    }
    rate_obs = [
        RateObservation(timestamp=t0, arrival_rate=100.0, apply_rate=500.0, backlog_rows=1000.0, p90_apply_rate=450.0),
        RateObservation(timestamp=t1, arrival_rate=100.0, apply_rate=550.0, backlog_rows=500.0, p90_apply_rate=500.0),
    ]

    # Healthy run
    out_healthy = supervisor.supervise(
        task_certificates=task_certs,
        rate_observations=rate_obs,
        backlog_rows=500.0,
    )
    assert out_healthy.status == "healthy"
    assert out_healthy.care_badge == CARE_PASS
    report = out_healthy.render_report()
    assert "[PASS]" in report
    assert "Overall Status   : HEALTHY" in report

    # Terminated run: add unresolvable deadlock
    supervisor.deadlock_detector.add_dependency("task_accounts", "task_accounts")
    stationary_certs = {
        "task_accounts": [task_certs["task_accounts"][0], task_certs["task_accounts"][0]]
    }
    out_term = supervisor.supervise(
        task_certificates=stationary_certs,
        rate_observations=rate_obs,
        backlog_rows=500.0,
    )
    assert out_term.status == "terminated"
    assert out_term.care_badge == CARE_ACTION
    assert len(out_term.prerequisites) > 0
    report_term = out_term.render_report()
    assert "[ACTION]" in report_term
    assert "Overall Status   : TERMINATED" in report_term
    assert "SMALLEST MISSING PREREQUISITES:" in report_term
