"""Tests for WAL and spool retention controller (backlog RCH16, row 624).

Proactively prevents unbounded WAL and spool risk.
Verifies forecasting, proactive interventions (speed copy, throttle safe stages,
expand capacity, truthful stop/reseed), finite spool limitations during multi-hour
outages, O(1) durable resume, and mutual exclusion.
"""

import datetime
import decimal
import json
import shutil
import tempfile
import time
import uuid
from pathlib import Path

import pytest
import tests.rung_branches

from migkit import decide
from migkit.retention_controller import (
    ACTION_EXPAND_CAPACITY,
    ACTION_SPEED_COPY,
    ACTION_STEADY,
    ACTION_STOP_AND_RESEED,
    ACTION_THROTTLE_SAFE_STAGES,
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    DurableRetentionCheckpoint,
    RetentionAction,
    RetentionController,
    RetentionForecaster,
    RetentionMetrics,
)

PROVES_RUNGS = [
    ("retention_action", "stop_and_reseed", "holds"),
    ("retention_action", "stop_and_reseed", "refuses"),
    ("retention_action", "expand_capacity", "holds"),
    ("retention_action", "expand_capacity", "refuses"),
    ("retention_action", "throttle_safe_stages", "holds"),
    ("retention_action", "throttle_safe_stages", "refuses"),
    ("retention_action", "speed_copy", "holds"),
    ("retention_action", "speed_copy", "refuses"),
    ("retention_action", "steady", "holds"),
]


@pytest.fixture
def tmp_dir():
    d = Path(tempfile.mkdtemp(prefix="test_retention_ctrl_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_retention_forecasting_steady_state_and_drain():
    # Arrival matches consumption: steady state
    m_steady = RetentionMetrics(
        arrival_rate_bytes_sec=10 * 1024 * 1024,
        consumption_rate_bytes_sec=10 * 1024 * 1024,
        current_retained_bytes=100 * 1024 * 1024,
        free_disk_bytes=100 * 1024 * 1024 * 1024,
        spool_capacity_bytes=1024 * 1024 * 1024,
        snapshot_age_sec=3600.0,
        recovery_window_sec=86400.0,
        source_host="127.0.0.1",
        target_host="10.0.0.2",
    )
    f_steady = RetentionForecaster.forecast(m_steady)
    assert f_steady.net_growth_rate_bytes_sec == 0.0
    assert f_steady.drain_rate_bytes_sec == 0.0
    assert f_steady.care_badge == CARE_PASS
    assert f_steady.time_to_spool_exhaustion_sec is None
    assert f_steady.time_to_disk_exhaustion_sec is None
    assert f_steady.projected_retained_bytes_1h == 100 * 1024 * 1024

    # Consumption exceeds arrival: actively draining
    m_drain = RetentionMetrics(
        arrival_rate_bytes_sec=5 * 1024 * 1024,
        consumption_rate_bytes_sec=15 * 1024 * 1024,
        current_retained_bytes=200 * 1024 * 1024,
        free_disk_bytes=80 * 1024 * 1024 * 1024,
        spool_capacity_bytes=1024 * 1024 * 1024,
        snapshot_age_sec=7200.0,
        recovery_window_sec=86400.0,
        source_host="127.0.0.1",
        target_host="10.0.0.3",
    )
    f_drain = RetentionForecaster.forecast(m_drain)
    assert f_drain.drain_rate_bytes_sec == 10 * 1024 * 1024
    assert f_drain.care_badge == CARE_BENIGN
    assert "draining" in f_drain.care_reason.lower()


def test_retention_forecasting_growth_and_time_to_exhaustion():
    # Arrival exceeds consumption: spool accumulation
    arrival = 20 * 1024 * 1024  # 20 MB/s
    consumption = 10 * 1024 * 1024  # 10 MB/s
    net = 10 * 1024 * 1024  # 10 MB/s growth
    spool_cap = 500 * 1024 * 1024  # 500 MB capacity
    current = 200 * 1024 * 1024  # 200 MB used -> 300 MB available
    # Available = 300 MB / 10 MB/s = 30 seconds to spool exhaustion!

    m = RetentionMetrics(
        arrival_rate_bytes_sec=arrival,
        consumption_rate_bytes_sec=consumption,
        current_retained_bytes=current,
        free_disk_bytes=10 * 1024 * 1024 * 1024,
        spool_capacity_bytes=spool_cap,
        snapshot_age_sec=1800.0,
        recovery_window_sec=86400.0,
        source_host="127.0.0.1",
        target_host="10.0.0.2",
    )
    f = RetentionForecaster.forecast(m)
    assert f.net_growth_rate_bytes_sec == net
    assert pytest.approx(f.time_to_spool_exhaustion_sec, 0.1) == 30.0
    assert f.care_badge == CARE_ACTION
    assert "breach forecast" in f.care_reason.lower()


def test_finite_spool_is_not_guarantee_against_unlimited_outage():
    # Explicitly recognize that finite spool is not a guarantee against unlimited outage
    arrival = 1 * 1024 * 1024  # 1 MB/s
    spool_cap = 100 * 1024 * 1024  # 100 MB finite capacity
    current = 10 * 1024 * 1024  # 10 MB used -> 90 MB available

    # 90 MB / 1 MB/s = 90 seconds tolerance
    m = RetentionMetrics(
        arrival_rate_bytes_sec=arrival,
        consumption_rate_bytes_sec=0.0,  # Full target outage
        current_retained_bytes=current,
        free_disk_bytes=50 * 1024 * 1024 * 1024,
        spool_capacity_bytes=spool_cap,
        snapshot_age_sec=600.0,
        recovery_window_sec=86400.0,
        is_target_outage=True,
        outage_duration_sec=300.0,  # Outage has lasted 300s, exceeding 90s tolerance!
        source_host="127.0.0.1",
        target_host="10.0.0.5",
    )
    f = RetentionForecaster.forecast(m)
    assert f.unlimited_outage_risk is True
    assert pytest.approx(f.finite_spool_outage_tolerance_sec, 0.1) == 90.0
    assert f.care_badge == CARE_ACTION
    assert "exceeded finite spool tolerance" in f.care_reason.lower()


def test_decide_engine_retention_action_rungs():
    # Rung 1: stop_and_reseed when stop is required
    act1, ch1 = decide.retention_action({"stop_required": True, "drain_needed": True})
    assert act1 == ACTION_STOP_AND_RESEED
    assert "stop truthfully" in ch1.reason or "stop truthfully" in ch1.rung.said

    # Rung 2: expand_capacity when authorized expansion is available
    act2, ch2 = decide.retention_action({
        "stop_required": False,
        "expansion_available": 10 * 1024 * 1024 * 1024,
        "drain_needed": True,
    })
    assert act2 == ACTION_EXPAND_CAPACITY
    assert "authorized" in ch2.rung.said or "authorized" in ch2.rung.because

    # Rung 3: throttle_safe_stages when safe stages can be throttled
    act3, ch3 = decide.retention_action({
        "stop_required": False,
        "expansion_available": 0,
        "throttle_safe": True,
        "drain_needed": True,
    })
    assert act3 == ACTION_THROTTLE_SAFE_STAGES
    assert "throttle" in ch3.rung.said or "throttle" in ch3.rung.because

    # Rung 4: speed_copy when target throughput can be increased
    act4, ch4 = decide.retention_action({
        "stop_required": False,
        "expansion_available": 0,
        "throttle_safe": False,
        "can_speed": True,
        "drain_needed": True,
    })
    assert act4 == ACTION_SPEED_COPY
    assert "pace" in ch4.rung.said or "pace" in ch4.rung.because

    # Rung 5: steady when safe headroom and no drain needed
    act5, ch5 = decide.retention_action({
        "stop_required": False,
        "drain_needed": False,
    })
    assert act5 == ACTION_STEADY
    assert "steady" in ch5.rung.said or "steady" in ch5.rung.because


def test_proactive_controller_decisions(tmp_dir):
    ctrl = RetentionController(hop_name="test-hop", base_dir=tmp_dir)

    # 1. Steady
    m_steady = RetentionMetrics(
        arrival_rate_bytes_sec=5 * 1024 * 1024,
        consumption_rate_bytes_sec=6 * 1024 * 1024,
        current_retained_bytes=10 * 1024 * 1024,
        free_disk_bytes=50 * 1024 * 1024 * 1024,
        spool_capacity_bytes=1024 * 1024 * 1024,
        snapshot_age_sec=300.0,
        recovery_window_sec=86400.0,
    )
    act_steady = ctrl.decide_action(m_steady)
    assert act_steady.action_type == ACTION_STEADY
    assert act_steady.recommended_pace_multiplier == 1.0

    # 2. Speed copy when target lags but is capable of faster consumption
    m_lag = RetentionMetrics(
        arrival_rate_bytes_sec=12 * 1024 * 1024,
        consumption_rate_bytes_sec=8 * 1024 * 1024,
        current_retained_bytes=200 * 1024 * 1024,
        free_disk_bytes=40 * 1024 * 1024 * 1024,
        spool_capacity_bytes=1024 * 1024 * 1024,
        snapshot_age_sec=1200.0,
        recovery_window_sec=86400.0,
        can_speed_copy=True,
        throttle_safe_active=True,
    )
    act_speed = ctrl.decide_action(m_lag)
    assert act_speed.action_type == ACTION_SPEED_COPY
    assert act_speed.recommended_pace_multiplier == 1.5

    # 3. Throttle safe background stages
    m_throttle = RetentionMetrics(
        arrival_rate_bytes_sec=15 * 1024 * 1024,
        consumption_rate_bytes_sec=8 * 1024 * 1024,
        current_retained_bytes=350 * 1024 * 1024,
        free_disk_bytes=20 * 1024 * 1024 * 1024,
        spool_capacity_bytes=1024 * 1024 * 1024,
        snapshot_age_sec=1800.0,
        recovery_window_sec=86400.0,
        can_speed_copy=False,
        throttle_safe_active=False,
    )
    act_throttle = ctrl.decide_action(m_throttle)
    assert act_throttle.action_type == ACTION_THROTTLE_SAFE_STAGES
    assert act_throttle.recommended_pace_multiplier == 0.5

    # 4. Request authorized capacity expansion
    m_expand = RetentionMetrics(
        arrival_rate_bytes_sec=25 * 1024 * 1024,
        consumption_rate_bytes_sec=10 * 1024 * 1024,
        current_retained_bytes=700 * 1024 * 1024,
        free_disk_bytes=15 * 1024 * 1024 * 1024,
        spool_capacity_bytes=1024 * 1024 * 1024,
        snapshot_age_sec=2400.0,
        recovery_window_sec=86400.0,
        authorized_expansion_bytes=5 * 1024 * 1024 * 1024,
        can_speed_copy=False,
        throttle_safe_active=True,
    )
    act_expand = ctrl.decide_action(m_expand)
    assert act_expand.action_type == ACTION_EXPAND_CAPACITY
    assert act_expand.authorized_bytes_to_request == 5 * 1024 * 1024 * 1024


def test_slow_target_plus_multi_hour_outage_stops_truthfully(tmp_dir):
    ctrl = RetentionController(hop_name="prod-hop", base_dir=tmp_dir)

    # Multi-hour target outage (consumption = 0, outage duration = 14400s = 4h)
    # Available disk is close to safe headroom boundary (600 MB free vs 512 MB safe headroom)
    m_outage = RetentionMetrics(
        arrival_rate_bytes_sec=2 * 1024 * 1024,  # 2 MB/s
        consumption_rate_bytes_sec=0.0,  # 0 MB/s target outage
        current_retained_bytes=980 * 1024 * 1024,  # 980 MB
        free_disk_bytes=550 * 1024 * 1024,  # Only 550 MB free (< safe headroom + delta)
        spool_capacity_bytes=1024 * 1024 * 1024,  # 1024 MB capacity
        safe_disk_headroom_bytes=512 * 1024 * 1024,  # 512 MB required safe headroom
        snapshot_age_sec=14400.0,
        recovery_window_sec=86400.0,
        is_target_outage=True,
        outage_duration_sec=14400.0,
        source_host="127.0.0.1",
        target_host="10.0.0.2",
    )

    act = ctrl.decide_action(m_outage)
    assert act.action_type == ACTION_STOP_AND_RESEED
    assert act.stop_truthfully is True
    assert act.care_badge == CARE_ACTION
    assert "irreversible" in act.stop_reason.lower() or "exceeded" in act.stop_reason.lower()

    # Create checkpoint recording the truthful stop without acknowledging unretained source changes
    chk = ctrl.checkpoint(
        checkpoint_dir=tmp_dir,
        last_durable_lsn="0/1A00000",
        snapshot_lsn="0/1000000",
        unretained_source_changes=True,
        reseed_reason=act.stop_reason,
        stop_truthfully=True,
        retained_bytes=m_outage.current_retained_bytes,
        spool_bytes=m_outage.current_retained_bytes,
    )

    # Verification: unretained source changes are NEVER acknowledged
    assert chk.source_changes_acknowledged is False
    assert chk.unretained_source_changes_detected is True
    assert chk.requires_reseed is True
    assert chk.stop_truthfully is True


def test_snapshot_restart_within_retention_resumes_in_o1(tmp_dir):
    ctrl = RetentionController(hop_name="resume-hop", base_dir=tmp_dir)

    # Save a valid durable checkpoint
    chk = ctrl.checkpoint(
        checkpoint_dir=tmp_dir,
        last_durable_lsn="0/2B00000",
        snapshot_lsn="0/1000000",
        unretained_source_changes=False,
        stop_truthfully=False,
        typed_cursors={"user_id": 42000, "txn_id": "tx-999"},
    )
    assert chk.source_changes_acknowledged is True

    # Source still holds logs starting at 0/2000000 (which is <= 0/2B00000)
    can_resume, code, details = ctrl.evaluate_resume(
        checkpoint_path=tmp_dir,
        current_source_min_lsn="0/2000000",
        current_recovery_window_sec=86400.0,
    )
    assert can_resume is True
    assert code == "resume_valid"
    assert details["last_durable_lsn"] == "0/2B00000"
    assert details["typed_cursors"] == {"user_id": 42000, "txn_id": "tx-999"}


def test_snapshot_restart_past_retention_demands_reseed_truthfully(tmp_dir):
    ctrl = RetentionController(hop_name="reseed-hop", base_dir=tmp_dir)

    # Checkpoint was taken at 0/2B00000
    ctrl.checkpoint(
        checkpoint_dir=tmp_dir,
        last_durable_lsn="0/2B00000",
        snapshot_lsn="0/1000000",
        unretained_source_changes=False,
        stop_truthfully=False,
    )

    # Outage was so long that source purged WAL up to 0/3000000
    # Earliest source LSN (0/3000000) > last durable LSN (0/2B00000)!
    can_resume, code, details = ctrl.evaluate_resume(
        checkpoint_path=tmp_dir,
        current_source_min_lsn="0/3000000",
        current_recovery_window_sec=86400.0,
    )
    assert can_resume is False
    assert code == "reseed_required"
    assert "source purged logs past durable position" in details["reason"].lower()


def test_universal_typed_durable_checkpoint_o1(tmp_dir):
    ctrl = RetentionController(hop_name="typed-hop", base_dir=tmp_dir)

    test_date = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc)
    test_decimal = decimal.Decimal("12345.6789")
    test_uuid = uuid.uuid4()
    test_tuple = ("orders", "range_001", 1000)
    test_set = ["col_a", "col_b"]

    cursors = {
        "created_at": test_date,
        "balance": test_decimal,
        "token": test_uuid,
        "range_spec": test_tuple,
        "columns": test_set,
        "row_count": 987654321,
    }

    chk = ctrl.checkpoint(
        checkpoint_dir=tmp_dir,
        last_durable_lsn="1/5000000",
        snapshot_lsn="1/0000000",
        typed_cursors=cursors,
    )

    # Load back and verify O(1) type fidelity
    loaded = ctrl.load_checkpoint(tmp_dir)
    assert loaded is not None
    assert loaded.last_durable_lsn == "1/5000000"

    tc = loaded.typed_cursors
    assert isinstance(tc["created_at"], datetime.datetime)
    assert tc["created_at"] == test_date

    assert isinstance(tc["balance"], decimal.Decimal)
    assert tc["balance"] == test_decimal

    assert isinstance(tc["token"], uuid.UUID)
    assert tc["token"] == test_uuid

    assert isinstance(tc["range_spec"], tuple)
    assert tc["range_spec"] == test_tuple

    assert tc["row_count"] == 987654321


def test_mutual_exclusion_active_lease_run_lock(tmp_dir):
    ctrl1 = RetentionController(hop_name="lease-hop", base_dir=tmp_dir)
    ctrl2 = RetentionController(hop_name="lease-hop", base_dir=tmp_dir)

    # ctrl1 acquires active lease lock
    ok1 = ctrl1.acquire_lease(tmp_dir, timeout_sec=0.5)
    assert ok1 is True
    assert (tmp_dir / ".run.lock").is_file()

    # ctrl2 attempts to acquire same lease; must be refused
    ok2 = ctrl2.acquire_lease(tmp_dir, timeout_sec=0.2)
    assert ok2 is False

    # Heartbeat renewal
    ctrl1.renew_lease()
    lock_content = (tmp_dir / ".run.lock").read_text(encoding="ascii")
    assert "lease-hop" in lock_content
    assert "active" in lock_content

    # Release lease
    ctrl1.release_lease()

    # Now ctrl2 can acquire
    ok2_again = ctrl2.acquire_lease(tmp_dir, timeout_sec=0.5)
    assert ok2_again is True
    ctrl2.release_lease()


def test_pure_unix_terse_output_and_badges(tmp_dir):
    ctrl = RetentionController(hop_name="unix-hop", base_dir=tmp_dir)

    metrics = RetentionMetrics(
        arrival_rate_bytes_sec=12 * 1024 * 1024,
        consumption_rate_bytes_sec=15 * 1024 * 1024,
        current_retained_bytes=50 * 1024 * 1024,
        free_disk_bytes=100 * 1024 * 1024 * 1024,
        spool_capacity_bytes=1024 * 1024 * 1024,
        snapshot_age_sec=1200.0,
        recovery_window_sec=86400.0,
        source_host="127.0.0.1",
        target_host="10.0.0.2",
    )
    forecast = ctrl.forecast(metrics)
    action = ctrl.decide_action(metrics, forecast)

    report = ctrl.format_status_report(metrics, forecast, action)

    # 1. Must be 100% pure ASCII
    for ch in report:
        assert ord(ch) < 128, f"Non-ASCII character found: {ch!r}"

    # 2. Must contain valid care badge
    assert any(badge in report for badge in ("[PASS]", "[REVIEW]", "[BENIGN]", "[ACTION]"))

    # 3. Must be terse, UNIX enterprise style
    assert "RETENTION:" in report
    assert "arrival=" in report
    assert "cons=" in report
    assert "spool=" in report
    assert "disk_free=" in report


def test_retention_action_rungs_watched():
    with tests.rung_branches.watched() as seen:
        decide.retention_action({
            "stop_required": True,
            "expansion_available": 100,
            "throttle_safe": True,
            "can_speed": True,
            "drain_needed": True,
        })
        decide.retention_action({
            "stop_required": False,
            "expansion_available": 0,
            "throttle_safe": False,
            "can_speed": False,
            "drain_needed": False,
        })
    for name in ("stop_and_reseed", "expand_capacity", "throttle_safe_stages", "speed_copy"):
        assert set(tests.rung_branches.branch(seen, name)) == {"holds", "refuses"}
    assert "holds" in tests.rung_branches.branch(seen, "steady")

