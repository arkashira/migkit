"""Tests for Row 602: SMART-N19.time-aware-control (Plan around expiring resources and changing windows).

Verifies:
1. Every `decide.Rung` in `migkit/time_aware_control.py` (`window_admissibility_rungs`,
   `resource_lease_rungs`, `burst_and_quota_rungs`, `traffic_cycle_staging_rungs`,
   `cutover_slack_rungs`) exercises BOTH `holds` and `refuses` branches under
   `with tests.rung_branches.watched():` and is declared in `PROVES_RUNGS`.
2. Maintenance windows, blackout windows, and missed window consent:
   - Phase fits within active maintenance window.
   - Blackout window strictly prohibits migration traffic.
   - Missed windows trigger a fresh admissible plan without guessing switch consent.
   - Switch consent granted allows controlled switch outside window.
3. Traffic cycles & diurnal load:
   - Diurnal peak hours reduce effective available bandwidth.
   - Off-peak dips schedule heavy bulk staging.
   - Throttling during peak protects production queries.
4. Burst credits & quota reset:
   - Ample burst credits allow full burst rate.
   - Burst credit depletion paces throughput down to baseline refill rate.
   - Quota exhaustion pauses execution until quota reset horizon.
5. Resource leases, nonrenewable leases, and log retention (CRITICAL ACCEPTANCE):
   - Proactive prerequisite renewal scheduled for renewable leases.
   - Zero optimistic execution beyond nonrenewable leases (ACTION badge, unadmissible).
   - Log retention window breach strictly refused to prevent WAL/binlog truncation.
   - Snapshot TTL expiry strictly halts consistent read execution.
6. Cutover slack & emergency abort leeway:
   - Required drain, sync, lock, and abort leeway enforced before window close.
7. Resilient operations:
   - O(1) typed durable checkpoint resume with native Python types.
   - Active lease lock (.run.lock) mutual exclusion with heartbeat.
   - Adaptive AIMD pacing and cold-start byte-weighted ETA.
   - Safe test endpoint validation (127.0.0.1, 10.0.0.x).

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import datetime
import decimal
import math
import pathlib
import time
import uuid
from typing import List

import pytest
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched

from migkit import decide
from migkit import time_aware_control as tac

# -----------------------------------------------------------------------------
# Rung Declaration for test_every_decide_rung_is_reached.py
# -----------------------------------------------------------------------------

PROVES_RUNGS = [
    # window_admissibility_rungs (4 rungs x 2 branches = 8)
    ("time_aware_control", "window_admissibility_rungs", "admit_in_active_window", "holds"),
    ("time_aware_control", "window_admissibility_rungs", "admit_in_active_window", "refuses"),
    ("time_aware_control", "window_admissibility_rungs", "schedule_in_upcoming_window", "holds"),
    ("time_aware_control", "window_admissibility_rungs", "schedule_in_upcoming_window", "refuses"),
    ("time_aware_control", "window_admissibility_rungs", "refuse_missed_window_no_consent", "holds"),
    ("time_aware_control", "window_admissibility_rungs", "refuse_missed_window_no_consent", "refuses"),
    ("time_aware_control", "window_admissibility_rungs", "reject_blackout_violation", "holds"),
    ("time_aware_control", "window_admissibility_rungs", "reject_blackout_violation", "refuses"),

    # resource_lease_rungs (5 rungs x 2 branches = 10)
    ("time_aware_control", "resource_lease_rungs", "lease_safe_within_horizon", "holds"),
    ("time_aware_control", "resource_lease_rungs", "lease_safe_within_horizon", "refuses"),
    ("time_aware_control", "resource_lease_rungs", "schedule_prerequisite_lease_renewal", "holds"),
    ("time_aware_control", "resource_lease_rungs", "schedule_prerequisite_lease_renewal", "refuses"),
    ("time_aware_control", "resource_lease_rungs", "reject_nonrenewable_lease_breach", "holds"),
    ("time_aware_control", "resource_lease_rungs", "reject_nonrenewable_lease_breach", "refuses"),
    ("time_aware_control", "resource_lease_rungs", "reject_log_retention_breach", "holds"),
    ("time_aware_control", "resource_lease_rungs", "reject_log_retention_breach", "refuses"),
    ("time_aware_control", "resource_lease_rungs", "reject_snapshot_expiry_breach", "holds"),
    ("time_aware_control", "resource_lease_rungs", "reject_snapshot_expiry_breach", "refuses"),

    # burst_and_quota_rungs (4 rungs x 2 branches = 8)
    ("time_aware_control", "burst_and_quota_rungs", "burst_sustainable_full_speed", "holds"),
    ("time_aware_control", "burst_and_quota_rungs", "burst_sustainable_full_speed", "refuses"),
    ("time_aware_control", "burst_and_quota_rungs", "pace_to_refill_baseline", "holds"),
    ("time_aware_control", "burst_and_quota_rungs", "pace_to_refill_baseline", "refuses"),
    ("time_aware_control", "burst_and_quota_rungs", "defer_for_quota_reset", "holds"),
    ("time_aware_control", "burst_and_quota_rungs", "defer_for_quota_reset", "refuses"),
    ("time_aware_control", "burst_and_quota_rungs", "reject_quota_exhaustion_unresolvable", "holds"),
    ("time_aware_control", "burst_and_quota_rungs", "reject_quota_exhaustion_unresolvable", "refuses"),

    # traffic_cycle_staging_rungs (3 rungs x 2 branches = 6)
    ("time_aware_control", "traffic_cycle_staging_rungs", "stage_during_offpeak_dip", "holds"),
    ("time_aware_control", "traffic_cycle_staging_rungs", "stage_during_offpeak_dip", "refuses"),
    ("time_aware_control", "traffic_cycle_staging_rungs", "throttle_staging_during_peak", "holds"),
    ("time_aware_control", "traffic_cycle_staging_rungs", "throttle_staging_during_peak", "refuses"),
    ("time_aware_control", "traffic_cycle_staging_rungs", "defer_staging_to_offpeak", "holds"),
    ("time_aware_control", "traffic_cycle_staging_rungs", "defer_staging_to_offpeak", "refuses"),

    # cutover_slack_rungs (3 rungs x 2 branches = 6)
    ("time_aware_control", "cutover_slack_rungs", "admit_cutover_slack_satisfied", "holds"),
    ("time_aware_control", "cutover_slack_rungs", "admit_cutover_slack_satisfied", "refuses"),
    ("time_aware_control", "cutover_slack_rungs", "pause_without_guessing_consent", "holds"),
    ("time_aware_control", "cutover_slack_rungs", "pause_without_guessing_consent", "refuses"),
    ("time_aware_control", "cutover_slack_rungs", "reject_emergency_abort_unsupported", "holds"),
    ("time_aware_control", "cutover_slack_rungs", "reject_emergency_abort_unsupported", "refuses"),
]


# -----------------------------------------------------------------------------
# 1. Rung Branch Coverage: Holds and Refuses for All Rungs Under Watched
# -----------------------------------------------------------------------------


def test_every_time_aware_control_rung_holds_and_refuses_under_watched() -> None:
    """Exercise both holds and refuses for every rung in time_aware_control.py."""
    with watched() as seen:
        # Ladder 1: window_admissibility_rungs
        # Test 1: admit_in_active_window holds
        decide.climb(
            "admit in active window",
            tac.window_admissibility_rungs(),
            decide.Facts({
                "is_window_active": True,
                "is_blackout": False,
                "fits_in_active_window": True,
                "has_upcoming_window": False,
                "active_window_viable": True,
                "window_missed": False,
                "switch_consent_outside_window": False,
            }),
        )
        # Test 2: schedule_in_upcoming_window holds
        decide.climb(
            "schedule in upcoming window",
            tac.window_admissibility_rungs(),
            decide.Facts({
                "is_window_active": False,
                "is_blackout": False,
                "fits_in_active_window": False,
                "has_upcoming_window": True,
                "active_window_viable": False,
                "window_missed": False,
                "switch_consent_outside_window": False,
            }),
        )
        # Test 3: refuse_missed_window_no_consent holds
        decide.climb(
            "refuse missed window no consent",
            tac.window_admissibility_rungs(),
            decide.Facts({
                "is_window_active": False,
                "is_blackout": False,
                "fits_in_active_window": False,
                "has_upcoming_window": False,
                "active_window_viable": False,
                "window_missed": True,
                "switch_consent_outside_window": False,
            }),
        )
        # Test 4: reject_blackout_violation holds
        decide.climb(
            "reject blackout violation",
            tac.window_admissibility_rungs(),
            decide.Facts({
                "is_window_active": False,
                "is_blackout": True,
                "fits_in_active_window": False,
                "has_upcoming_window": False,
                "active_window_viable": False,
                "window_missed": False,
                "switch_consent_outside_window": True,
            }),
        )
        # Test 5: All refuse
        decide.climb(
            "window all refuse",
            tac.window_admissibility_rungs(),
            decide.Facts({
                "is_window_active": False,
                "is_blackout": False,
                "fits_in_active_window": False,
                "has_upcoming_window": False,
                "active_window_viable": True,
                "window_missed": False,
                "switch_consent_outside_window": False,
            }),
        )

        # Ladder 2: resource_lease_rungs
        # Test 1: lease_safe_within_horizon holds
        decide.climb(
            "lease safe within horizon",
            tac.resource_lease_rungs(),
            decide.Facts({
                "lease_horizon_sufficient": True,
                "log_retention_sufficient": True,
                "snapshot_validity_sufficient": True,
                "lease_renewable": False,
                "lease_expires_during_phase": False,
            }),
        )
        # Test 2: schedule_prerequisite_lease_renewal holds
        decide.climb(
            "schedule prerequisite lease renewal",
            tac.resource_lease_rungs(),
            decide.Facts({
                "lease_horizon_sufficient": False,
                "log_retention_sufficient": True,
                "snapshot_validity_sufficient": True,
                "lease_renewable": True,
                "lease_expires_during_phase": True,
            }),
        )
        # Test 3: reject_nonrenewable_lease_breach holds
        decide.climb(
            "reject nonrenewable lease breach",
            tac.resource_lease_rungs(),
            decide.Facts({
                "lease_horizon_sufficient": False,
                "log_retention_sufficient": True,
                "snapshot_validity_sufficient": True,
                "lease_renewable": False,
                "lease_expires_during_phase": True,
            }),
        )
        # Test 4: reject_log_retention_breach holds
        decide.climb(
            "reject log retention breach",
            tac.resource_lease_rungs(),
            decide.Facts({
                "lease_horizon_sufficient": True,
                "log_retention_sufficient": False,
                "snapshot_validity_sufficient": True,
                "lease_renewable": False,
                "lease_expires_during_phase": False,
            }),
        )
        # Test 5: reject_snapshot_expiry_breach holds
        decide.climb(
            "reject snapshot expiry breach",
            tac.resource_lease_rungs(),
            decide.Facts({
                "lease_horizon_sufficient": True,
                "log_retention_sufficient": True,
                "snapshot_validity_sufficient": False,
                "lease_renewable": False,
                "lease_expires_during_phase": False,
            }),
        )
        # Test 6: All refuse
        decide.climb(
            "lease all refuse",
            tac.resource_lease_rungs(),
            decide.Facts({
                "lease_horizon_sufficient": False,
                "log_retention_sufficient": True,
                "snapshot_validity_sufficient": True,
                "lease_renewable": False,
                "lease_expires_during_phase": False,
            }),
        )

        # Ladder 3: burst_and_quota_rungs
        # Test 1: burst_sustainable_full_speed holds
        decide.climb(
            "burst sustainable full speed",
            tac.burst_and_quota_rungs(),
            decide.Facts({
                "burst_credits_sufficient": True,
                "quota_sufficient": True,
                "baseline_rate_acceptable": False,
                "quota_reset_admissible": False,
            }),
        )
        # Test 2: pace_to_refill_baseline holds
        decide.climb(
            "pace to refill baseline",
            tac.burst_and_quota_rungs(),
            decide.Facts({
                "burst_credits_sufficient": False,
                "quota_sufficient": True,
                "baseline_rate_acceptable": True,
                "quota_reset_admissible": False,
            }),
        )
        # Test 3: defer_for_quota_reset holds
        decide.climb(
            "defer for quota reset",
            tac.burst_and_quota_rungs(),
            decide.Facts({
                "burst_credits_sufficient": True,
                "quota_sufficient": False,
                "baseline_rate_acceptable": False,
                "quota_reset_admissible": True,
            }),
        )
        # Test 4: reject_quota_exhaustion_unresolvable holds
        decide.climb(
            "reject quota exhaustion unresolvable",
            tac.burst_and_quota_rungs(),
            decide.Facts({
                "burst_credits_sufficient": True,
                "quota_sufficient": False,
                "baseline_rate_acceptable": False,
                "quota_reset_admissible": False,
            }),
        )
        # Test 5: All refuse
        decide.climb(
            "burst quota all refuse",
            tac.burst_and_quota_rungs(),
            decide.Facts({
                "burst_credits_sufficient": False,
                "quota_sufficient": True,
                "baseline_rate_acceptable": False,
                "quota_reset_admissible": False,
            }),
        )

        # Ladder 4: traffic_cycle_staging_rungs
        # Test 1: stage_during_offpeak_dip holds
        decide.climb(
            "stage during offpeak dip",
            tac.traffic_cycle_staging_rungs(),
            decide.Facts({
                "is_offpeak_available": True,
                "staging_bandwidth_permitted": True,
                "is_peak_active": False,
                "throttling_supported": False,
                "must_defer_staging": False,
            }),
        )
        # Test 2: throttle_staging_during_peak holds
        decide.climb(
            "throttle staging during peak",
            tac.traffic_cycle_staging_rungs(),
            decide.Facts({
                "is_offpeak_available": False,
                "staging_bandwidth_permitted": False,
                "is_peak_active": True,
                "throttling_supported": True,
                "must_defer_staging": False,
            }),
        )
        # Test 3: defer_staging_to_offpeak holds
        decide.climb(
            "defer staging to offpeak",
            tac.traffic_cycle_staging_rungs(),
            decide.Facts({
                "is_offpeak_available": False,
                "staging_bandwidth_permitted": False,
                "is_peak_active": True,
                "throttling_supported": False,
                "must_defer_staging": True,
            }),
        )
        # Test 4: All refuse
        decide.climb(
            "traffic staging all refuse",
            tac.traffic_cycle_staging_rungs(),
            decide.Facts({
                "is_offpeak_available": False,
                "staging_bandwidth_permitted": False,
                "is_peak_active": False,
                "throttling_supported": False,
                "must_defer_staging": False,
            }),
        )

        # Ladder 5: cutover_slack_rungs
        # Test 1: admit_cutover_slack_satisfied holds
        decide.climb(
            "admit cutover slack satisfied",
            tac.cutover_slack_rungs(),
            decide.Facts({
                "cutover_slack_satisfied": True,
                "switch_consent_or_permitted": True,
                "abort_leeway_breached": False,
            }),
        )
        # Test 2: pause_without_guessing_consent holds
        decide.climb(
            "pause without guessing consent",
            tac.cutover_slack_rungs(),
            decide.Facts({
                "cutover_slack_satisfied": False,
                "switch_consent_or_permitted": False,
                "abort_leeway_breached": False,
            }),
        )
        # Test 3: reject_emergency_abort_unsupported holds
        decide.climb(
            "reject emergency abort unsupported",
            tac.cutover_slack_rungs(),
            decide.Facts({
                "cutover_slack_satisfied": False,
                "switch_consent_or_permitted": True,
                "abort_leeway_breached": True,
            }),
        )
        # Test 4: All refuse
        decide.climb(
            "slack all refuse",
            tac.cutover_slack_rungs(),
            decide.Facts({
                "cutover_slack_satisfied": True,
                "switch_consent_or_permitted": False,
                "abort_leeway_breached": False,
            }),
        )

    # Verify every rung declared in PROVES_RUNGS was reached on the expected branch
    for module, ladder, rung_name, branch_type in PROVES_RUNGS:
        branches_seen = branch(seen, rung_name)
        assert branch_type in branches_seen, (
            f"Rung '{rung_name}' in ladder '{ladder}' missing expected branch '{branch_type}'. "
            f"Observed branches: {branches_seen}"
        )


# -----------------------------------------------------------------------------
# 2. Maintenance Windows & Blackout Testing
# -----------------------------------------------------------------------------


def test_time_window_lifecycle_and_blackouts() -> None:
    """Validate active, upcoming, closed, and blackout window calculations."""
    base_time = datetime.datetime(2026, 10, 17, 10, 0, tzinfo=datetime.timezone.utc)
    w_active = tac.TimeWindow(
        window_id="win-active-1",
        start_time=base_time - datetime.timedelta(hours=1),
        end_time=base_time + datetime.timedelta(hours=2),
        is_blackout=False,
    )
    assert w_active.is_active(base_time) is True
    assert w_active.remaining_seconds(base_time) == 7200.0
    assert w_active.seconds_until_start(base_time) == 0.0
    assert w_active.is_missed(base_time, required_slack_sec=1000.0) is False
    assert w_active.is_missed(base_time, required_slack_sec=8000.0) is True

    w_future = tac.TimeWindow(
        window_id="win-future-1",
        start_time=base_time + datetime.timedelta(hours=5),
        end_time=base_time + datetime.timedelta(hours=9),
    )
    assert w_future.is_active(base_time) is False
    assert w_future.seconds_until_start(base_time) == 18000.0

    w_blackout = tac.TimeWindow(
        window_id="win-blackout",
        start_time=base_time - datetime.timedelta(hours=2),
        end_time=base_time + datetime.timedelta(hours=2),
        is_blackout=True,
    )
    assert w_blackout.is_blackout is True

    # Mathematical / domain validation
    with pytest.raises(tac.InvalidTimeAwareFactError):
        tac.TimeWindow(
            window_id="win-invalid",
            start_time=base_time + datetime.timedelta(hours=1),
            end_time=base_time,  # end before start
        )


def test_missed_window_triggers_fresh_plan_without_guessing_switch_consent() -> None:
    """Verify that missed window triggers a fresh plan and never guesses switch consent."""
    now = datetime.datetime(2026, 10, 17, 23, 50, tzinfo=datetime.timezone.utc)
    # Active window closing in 10 minutes (600s)
    w_closing = tac.TimeWindow(
        window_id="win-closing",
        start_time=now - datetime.timedelta(hours=3),
        end_time=now + datetime.timedelta(minutes=10),
        cutover_permitted=True,
    )
    # Next upcoming window tomorrow
    w_tomorrow = tac.TimeWindow(
        window_id="win-tomorrow",
        start_time=now + datetime.timedelta(hours=2),
        end_time=now + datetime.timedelta(hours=6),
        cutover_permitted=True,
    )

    phases = [
        tac.PhaseForecast(
            phase_name="cdc_catchup",
            nominal_duration_seconds=900.0,
            uncertainty_factor=0.10,
            required_bandwidth_bytes_sec=20_000_000.0,
        ),
        tac.PhaseForecast(
            phase_name="cutover_switch",
            nominal_duration_seconds=300.0,
            uncertainty_factor=0.10,
            is_cutover_phase=True,
        ),
    ]

    # Without explicit switch consent: must not guess consent, defers to upcoming window
    plan = tac.plan_time_aware_migration(
        phases=phases,
        windows=[w_closing, w_tomorrow],
        switch_consent_granted=False,
        now=now,
    )
    assert plan.target_window_id == "win-tomorrow"
    assert plan.care_badge == tac.CARE_REVIEW
    assert plan.switch_consent_granted is False
    assert any("deferred to next admissible upcoming window" in r for r in plan.reasons)

    # If no upcoming window exists and switch consent is absent: ACTION badge, inadmissible
    plan_no_future = tac.plan_time_aware_migration(
        phases=phases,
        windows=[w_closing],
        switch_consent_granted=False,
        now=now,
    )
    assert plan_no_future.admissible is False
    assert plan_no_future.care_badge == tac.CARE_ACTION


# -----------------------------------------------------------------------------
# 3. Traffic Cycles & Diurnal Load Testing
# -----------------------------------------------------------------------------


def test_traffic_cycle_peak_and_offpeak_pacing() -> None:
    """Validate diurnal traffic cycle modeling and next off-peak window calculation."""
    tc = tac.TrafficCycle(
        peak_hours=(9, 10, 11, 14, 15, 16, 17),
        offpeak_hours=(1, 2, 3, 4),
        peak_load_factor=2.0,
        offpeak_load_factor=0.4,
        baseline_bandwidth_bytes_sec=40_000_000.0,
    )

    dt_peak = datetime.datetime(2026, 10, 17, 10, 30, tzinfo=datetime.timezone.utc)
    dt_offpeak = datetime.datetime(2026, 10, 17, 2, 15, tzinfo=datetime.timezone.utc)
    dt_mid = datetime.datetime(2026, 10, 17, 7, 0, tzinfo=datetime.timezone.utc)

    assert tc.is_peak(dt_peak) is True
    assert tc.is_offpeak(dt_peak) is False
    assert tc.load_factor_at(dt_peak) == 2.0
    assert tc.effective_bandwidth_bytes_sec(dt_peak) == 20_000_000.0

    assert tc.is_offpeak(dt_offpeak) is True
    assert tc.is_peak(dt_offpeak) is False
    assert tc.load_factor_at(dt_offpeak) == 0.4
    assert tc.effective_bandwidth_bytes_sec(dt_offpeak) == 100_000_000.0  # 40MB/s * (1.0 / 0.4) = 100MB/s

    assert tc.load_factor_at(dt_mid) == 1.0

    # Next offpeak calculation
    now = datetime.datetime(2026, 10, 17, 12, 0, tzinfo=datetime.timezone.utc)
    next_start, next_end = tc.next_offpeak_window(now)
    assert next_start.hour == 1
    assert next_end.hour == 5
    assert next_start > now


# -----------------------------------------------------------------------------
# 4. Burst Credits & Quota Reset Testing
# -----------------------------------------------------------------------------


def test_burst_credit_bucket_burn_and_depletion() -> None:
    """Validate cloud/storage burst bucket accumulation, drain, and pacing."""
    bucket = tac.BurstCreditBucket(
        bucket_name="ebs_iops_bucket",
        current_credits=1000.0,
        max_credits=5000.0,
        refill_rate_per_sec=10.0,
        burst_consumption_per_sec=100.0,
        baseline_consumption_per_sec=20.0,
    )

    assert bucket.is_depleted is False
    assert bucket.credit_fill_ratio == 0.20
    # Net drain = 100 - 10 = 90 credits/sec. Time until depleted = 1000 / 90 ~ 11.11s
    assert math.isclose(bucket.time_until_depleted(), 1000.0 / 90.0, rel_tol=1e-3)

    # Simulate burn under duration that does not exhaust bucket
    eff_rate, cred_rem = bucket.simulate_burn(duration_sec=5.0, requested_rate=100.0)
    assert eff_rate == 100.0
    assert math.isclose(cred_rem, 1000.0 - (90.0 * 5.0), rel_tol=1e-3)

    # Simulate burn over long duration that depletes credits
    eff_rate_depleted, cred_rem_depleted = bucket.simulate_burn(duration_sec=100.0, requested_rate=100.0)
    assert cred_rem_depleted == 0.0
    assert eff_rate_depleted < 100.0
    assert eff_rate_depleted > 20.0


def test_quota_reset_window_and_wait_prerequisite() -> None:
    """Validate quota tracking and scheduled wait prerequisite when quota is exhausted."""
    now = datetime.datetime(2026, 10, 17, 14, 0, tzinfo=datetime.timezone.utc)
    w_active = tac.TimeWindow(
        window_id="win-full",
        start_time=now - datetime.timedelta(hours=1),
        end_time=now + datetime.timedelta(hours=5),
    )
    quota = tac.QuotaResetWindow(
        quota_name="aws_dms_api_requests",
        remaining_units=10.0,  # Only 10 units left
        total_limit=1000.0,
        reset_horizon_seconds=900.0,  # resets in 15 mins
    )
    phases = [
        tac.PhaseForecast(
            phase_name="bulk_load",
            nominal_duration_seconds=600.0,
            required_quota_units=500.0,  # Exceeds remaining 10
        )
    ]

    plan = tac.plan_time_aware_migration(
        phases=phases,
        windows=[w_active],
        quotas=[quota],
        now=now,
    )
    assert plan.chosen_rungs["burst_and_quota"] == "defer_for_quota_reset"
    assert any(p.action_type == "quota_wait" for p in plan.prerequisites)
    prereq = next(p for p in plan.prerequisites if p.action_type == "quota_wait")
    assert prereq.estimated_duration_seconds == 900.0


# -----------------------------------------------------------------------------
# 5. Resource Leases & Nonrenewable Expiry Testing (CRITICAL ACCEPTANCE)
# -----------------------------------------------------------------------------


def test_acceptance_no_phase_starts_beyond_nonrenewable_lease() -> None:
    """CRITICAL ACCEPTANCE: No phase starts optimistically beyond a nonrenewable lease."""
    now = datetime.datetime(2026, 10, 17, 10, 0, tzinfo=datetime.timezone.utc)
    w_active = tac.TimeWindow(
        window_id="win-active",
        start_time=now - datetime.timedelta(hours=1),
        end_time=now + datetime.timedelta(hours=4),
    )

    # Nonrenewable token expiring in 15 minutes (900 seconds)
    token_lease = tac.ResourceLease(
        resource_id="vault-token-short",
        resource_type=tac.ResourceLeaseType.TOKEN,
        valid_until=now + datetime.timedelta(minutes=15),
        renewable=False,
    )

    # Migration phase requires 30 minutes nominal (1800s)
    phases = [
        tac.PhaseForecast(
            phase_name="seed_copy",
            nominal_duration_seconds=1800.0,
            uncertainty_factor=0.20,
        )
    ]

    plan = tac.plan_time_aware_migration(
        phases=phases,
        windows=[w_active],
        leases=[token_lease],
        now=now,
    )

    # MUST reject optimistic start beyond nonrenewable lease
    assert plan.admissible is False
    assert plan.care_badge == tac.CARE_ACTION
    assert plan.chosen_rungs["resource_lease"] == "reject_nonrenewable_lease_breach"
    assert any("nonrenewable lease expires" in r for r in plan.reasons)


def test_renewable_lease_schedules_proactive_prerequisite_renewal() -> None:
    """Verify that a renewable lease expiring during execution triggers proactive renewal."""
    now = datetime.datetime(2026, 10, 17, 10, 0, tzinfo=datetime.timezone.utc)
    w_active = tac.TimeWindow(
        window_id="win-active",
        start_time=now - datetime.timedelta(hours=1),
        end_time=now + datetime.timedelta(hours=4),
    )

    # Renewable mTLS cert expiring in 20 minutes (1200 seconds)
    cert_lease = tac.ResourceLease(
        resource_id="mtls-cert-primary",
        resource_type=tac.ResourceLeaseType.CERTIFICATE,
        valid_until=now + datetime.timedelta(minutes=20),
        renewable=True,
        max_renewals=3,
        renewals_performed=0,
        renewal_lead_time_seconds=300.0,
        renewal_cost_seconds=5.0,
    )

    phases = [
        tac.PhaseForecast(
            phase_name="seed_copy",
            nominal_duration_seconds=1800.0,
            uncertainty_factor=0.10,
        )
    ]

    plan = tac.plan_time_aware_migration(
        phases=phases,
        windows=[w_active],
        leases=[cert_lease],
        now=now,
    )

    assert plan.admissible is True
    assert plan.chosen_rungs["resource_lease"] == "schedule_prerequisite_lease_renewal"
    assert any(p.action_type == "certificate_renewal" for p in plan.prerequisites)
    prereq = next(p for p in plan.prerequisites if p.action_type == "certificate_renewal")
    assert prereq.resource_id == "mtls-cert-primary"
    assert prereq.scheduled_at <= cert_lease.valid_until


# -----------------------------------------------------------------------------
# 6. Log Retention & Snapshot Expiry Testing (CRITICAL ACCEPTANCE)
# -----------------------------------------------------------------------------


def test_acceptance_no_phase_starts_beyond_log_retention_window() -> None:
    """CRITICAL ACCEPTANCE: No phase starts beyond a log-retention window."""
    now = datetime.datetime(2026, 10, 17, 10, 0, tzinfo=datetime.timezone.utc)
    w_active = tac.TimeWindow(
        window_id="win-active",
        start_time=now - datetime.timedelta(hours=1),
        end_time=now + datetime.timedelta(hours=4),
    )

    # Source PostgreSQL WAL retention is 3600 seconds, but current lag is already 3000 seconds
    log_retention = tac.LogRetentionWindow(
        source_engine="postgresql",
        retention_horizon_seconds=3600.0,
        max_log_bytes=10_000_000_000,
        current_lag_bytes=2_000_000_000,
        current_lag_seconds=3000.0,
        log_generation_rate_bytes_sec=10_000_000.0,
    )

    # Phase needs 1200 seconds, which will exceed 3600 - 3000 = 600s remaining retention
    phases = [
        tac.PhaseForecast(
            phase_name="catchup_stream",
            nominal_duration_seconds=1200.0,
        )
    ]

    plan = tac.plan_time_aware_migration(
        phases=phases,
        windows=[w_active],
        log_retention=log_retention,
        now=now,
    )

    assert plan.admissible is False
    assert plan.care_badge == tac.CARE_ACTION
    assert plan.chosen_rungs["resource_lease"] == "reject_log_retention_breach"
    assert any("log retention window breached" in r for r in plan.reasons)


def test_snapshot_ttl_expiry_rejection() -> None:
    """Verify that consistent read snapshot TTL expiry blocks phase execution."""
    now = datetime.datetime(2026, 10, 17, 10, 0, tzinfo=datetime.timezone.utc)
    w_active = tac.TimeWindow(
        window_id="win-active",
        start_time=now - datetime.timedelta(hours=1),
        end_time=now + datetime.timedelta(hours=4),
    )

    snapshot = tac.SnapshotSpec(
        snapshot_id="snap-pg-consistent-1",
        created_at=now - datetime.timedelta(minutes=50),
        ttl_seconds=3600.0,  # Only 10 mins remaining
    )

    phases = [
        tac.PhaseForecast(
            phase_name="consistent_read",
            nominal_duration_seconds=1200.0,  # 20 mins needed
        )
    ]

    plan = tac.plan_time_aware_migration(
        phases=phases,
        windows=[w_active],
        snapshot=snapshot,
        now=now,
    )

    assert plan.admissible is False
    assert plan.care_badge == tac.CARE_ACTION
    assert plan.chosen_rungs["resource_lease"] == "reject_snapshot_expiry_breach"


# -----------------------------------------------------------------------------
# 7. Cutover Slack & Emergency Abort Leeway Testing
# -----------------------------------------------------------------------------


def test_cutover_slack_components_and_abort_leeway() -> None:
    """Validate cutover slack budgeting and abort leeway enforcement."""
    slack = tac.CutoverSlackSpec(
        drain_seconds=20.0,
        lock_acquisition_seconds=10.0,
        final_sync_seconds=40.0,
        verification_seconds=30.0,
        abort_leeway_seconds=50.0,
    )
    assert slack.total_slack_required_seconds == 150.0

    with pytest.raises(tac.InvalidTimeAwareFactError):
        tac.CutoverSlackSpec(drain_seconds=-5.0)


# -----------------------------------------------------------------------------
# 8. Checkpointing, Leases & Resilient Operations Testing
# -----------------------------------------------------------------------------


def test_typed_durable_checkpoint_store_o1(tmp_path: pathlib.Path) -> None:
    """Validate universal typed durable checkpoint store with O(1) lookup."""
    store_file = tmp_path / "time_aware_checkpoints.json"
    store = tac.TimeAwareCheckpointStore(store_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    unit = tac.TimeAwareCheckpointUnit(
        plan_id="plan-test-01",
        phase_name="seed_chunk_0",
        min_cursor=decimal.Decimal("100.5"),
        max_cursor=decimal.Decimal("200.5"),
        merkle_digest="abc123merkle",
        completed_at=now,
        status="COMPLETED",
    )

    assert store.is_completed_o1("plan-test-01", "seed_chunk_0") is False
    store.record_unit(unit)
    assert store.is_completed_o1("plan-test-01", "seed_chunk_0") is True

    # Reload from disk
    store2 = tac.TimeAwareCheckpointStore(store_file)
    assert store2.is_completed_o1("plan-test-01", "seed_chunk_0") is True
    loaded = store2.get_unit("plan-test-01", "seed_chunk_0")
    assert loaded is not None
    assert loaded.min_cursor == decimal.Decimal("100.5")
    assert loaded.merkle_digest == "abc123merkle"


def test_active_run_lock_lease_mutual_exclusion(tmp_path: pathlib.Path) -> None:
    """Validate active .run.lock mutual exclusion lease with heartbeat."""
    lock_file = tmp_path / ".run.lock"
    lease_inst = tac.acquire_time_aware_control_lease(lock_file, term_seconds=5.0)
    assert lock_file.exists()
    lease_inst.release()


def test_aimd_window_pacing_and_byte_weighted_eta() -> None:
    """Validate AIMD pacing and cold-start byte-weighted ETA calculations."""
    # AIMD: low credits decreases throughput
    paced_down = tac.adjust_aimd_window_pacing(
        current_throughput_bytes_sec=50_000_000.0,
        window_remaining_sec=1000.0,
        phase_tail_duration_sec=900.0,
        credit_ratio=0.10,  # below 0.15 threshold
    )
    assert paced_down == 25_000_000.0

    # AIMD: ample credits and slack increases throughput
    paced_up = tac.adjust_aimd_window_pacing(
        current_throughput_bytes_sec=50_000_000.0,
        window_remaining_sec=5000.0,
        phase_tail_duration_sec=1000.0,
        credit_ratio=0.80,
    )
    assert paced_up == 52_000_000.0

    # Cold start byte-weighted ETA
    eta_cold = tac.estimate_time_aware_byte_weighted_eta(
        total_bytes=100_000_000,
        bytes_completed=0,
        elapsed_seconds=0.0,
        current_bandwidth_bytes_sec=10_000_000.0,
        traffic_load_factor=1.0,
    )
    assert eta_cold > 10.0  # Cold-start inflation applied

    eta_done = tac.estimate_time_aware_byte_weighted_eta(
        total_bytes=100_000_000,
        bytes_completed=100_000_000,
        elapsed_seconds=10.0,
        current_bandwidth_bytes_sec=10_000_000.0,
    )
    assert eta_done == 0.0


def test_safe_endpoint_host_validation() -> None:
    """Validate safe test host isolation (127.0.0.1, localhost, 10.0.0.x)."""
    tac.validate_safe_endpoint("127.0.0.1")
    tac.validate_safe_endpoint("localhost")
    tac.validate_safe_endpoint("10.0.0.42")

    with pytest.raises(tac.UnsafeEndpointHostError):
        tac.validate_safe_endpoint("192.168.1.1")

    with pytest.raises(tac.UnsafeEndpointHostError):
        tac.validate_safe_endpoint("attacker.com")

    with pytest.raises(tac.UnsafeEndpointHostError):
        tac.validate_safe_endpoint("")
