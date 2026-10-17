"""Tests for Row 614: RCH06.online-confirmation (Classify transient versus persistent divergence by positions).

Verifies:
1. Every `decide.Rung` in `migkit/online_confirmation.py` (`CONFIRMATION_STRATEGY_LADDER`,
   `DIVERGENCE_DISPOSITION_LADDER`, and `REPAIR_GATE_LADDER`) reaches both `holds` and `refuses`
   (or `holds` for fallback rungs with no needs) under `tests.rung_branches.watched()`.
2. Position-based confirmation (LSN, GTID sets, SCN, composite tuples) and generation-based
   settle passes classify candidate rows accurately:
   - In-flight replication lag settles when target catches up (`SETTLED_CONVERGED`, `PASS`).
   - Deleted-on-both rows settle cleanly (`SETTLED_DELETED_ON_BOTH`, `PASS`).
   - Delayed/open transactions stay pending (`PENDING_DELAYED_TRANSACTION`, `BENIGN`) and do not
     create false corruption.
   - Continuously changing hot rows stay pending (`PENDING_CONTINUOUS_MUTATION`, `BENIGN`) and
     are never repaired automatically after an arbitrary sleep.
   - Confirmed stationary or post-watermark divergence is flagged (`PERSISTENT_DRIFT`, `ACTION`)
     and never suppressed.
3. All selected proof units are counted in full denominators (`total_selected_units`,
   `settled_units`, `pending_units`, `persistent_drift_units`, `expired_units`) and online
   comparison evidence expires deterministically based on measured window/watermark/retention facts.
4. O(1) typed durable checkpoint resume (`datetime`, `Decimal`, `UUID`, composite tuple cursors),
   active verification lease lock (`.run.lock`) with heartbeat, adaptive AIMD batch sizing with
   instant cold-start byte-weighted ETA, Merkle bucket diffing, and normalized external tool
   wrapping with zero internal tool leaks.
"""

from __future__ import annotations

import datetime
import decimal
import uuid
from pathlib import Path

import pytest
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

from migkit import online_confirmation as oc

PROVES_RUNGS = [
    # CONFIRMATION_STRATEGY_LADDER
    ("online_confirmation", "CONFIRMATION_STRATEGY_LADDER", "position_watermark_fence", "holds"),
    ("online_confirmation", "CONFIRMATION_STRATEGY_LADDER", "position_watermark_fence", "refuses"),
    ("online_confirmation", "CONFIRMATION_STRATEGY_LADDER", "generation_settle_pass", "holds"),
    ("online_confirmation", "CONFIRMATION_STRATEGY_LADDER", "generation_settle_pass", "refuses"),
    ("online_confirmation", "CONFIRMATION_STRATEGY_LADDER", "hold_pending_no_sleep_repair", "holds"),
    # DIVERGENCE_DISPOSITION_LADDER
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "evidence_expired", "holds"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "evidence_expired", "refuses"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "settled_deleted_on_both", "holds"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "settled_deleted_on_both", "refuses"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "settled_converged", "holds"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "settled_converged", "refuses"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "pending_delayed_transaction", "holds"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "pending_delayed_transaction", "refuses"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "pending_continuous_mutation", "holds"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "pending_continuous_mutation", "refuses"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "persistent_drift", "holds"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "persistent_drift", "refuses"),
    ("online_confirmation", "DIVERGENCE_DISPOSITION_LADDER", "pending_behind_watermark", "holds"),
    # REPAIR_GATE_LADDER
    ("online_confirmation", "REPAIR_GATE_LADDER", "reverify_required_expired", "holds"),
    ("online_confirmation", "REPAIR_GATE_LADDER", "reverify_required_expired", "refuses"),
    ("online_confirmation", "REPAIR_GATE_LADDER", "no_repair_needed", "holds"),
    ("online_confirmation", "REPAIR_GATE_LADDER", "no_repair_needed", "refuses"),
    ("online_confirmation", "REPAIR_GATE_LADDER", "eligible_confirmed_drift", "holds"),
    ("online_confirmation", "REPAIR_GATE_LADDER", "eligible_confirmed_drift", "refuses"),
    ("online_confirmation", "REPAIR_GATE_LADDER", "hold_pending_do_not_repair", "holds"),
]


# -----------------------------------------------------------------------------
# 1. Decision Engine Rung Branch Verification (watched)
# -----------------------------------------------------------------------------

def test_all_online_confirmation_rungs_hold_and_refuse_under_watched() -> None:
    wm_position = oc.MeasuredWatermarkFacts(
        source_host="127.0.0.1",
        target_host="10.0.0.12",
        source_high_watermark="0/16B3748",
        target_applied_watermark="0/16B3750",
        measured_apply_lag_s=0.1,
        measured_window_scan_s=0.5,
        measured_retention_horizon_s=120.0,
        tail_alive=True,
        supports_position_fence=True,
        supports_generation_reread=True,
        observed_at_s=100.0,
    )
    wm_generation_only = oc.MeasuredWatermarkFacts(
        source_host="127.0.0.1",
        target_host="10.0.0.12",
        source_high_watermark=None,
        target_applied_watermark=None,
        measured_apply_lag_s=0.2,
        measured_window_scan_s=0.5,
        measured_retention_horizon_s=120.0,
        tail_alive=False,
        supports_position_fence=False,
        supports_generation_reread=True,
        observed_at_s=100.0,
    )

    with watched() as seen:
        # A. Exercise CONFIRMATION_STRATEGY_LADDER:
        # 1) position_watermark_fence holds
        s_pos = oc.select_confirmation_strategy(
            "orders:strategy",
            wm_position,
            generations_observed=2,
            evidence_age_s=0.2,
        )
        assert s_pos.path == "position_watermark_fence"

        # 2) position_watermark_fence refuses -> generation_settle_pass holds
        s_gen = oc.select_confirmation_strategy(
            "orders:strategy_gen",
            wm_generation_only,
            generations_observed=2,
            evidence_age_s=0.2,
        )
        assert s_gen.path == "generation_settle_pass"

        # 3) both position_watermark_fence and generation_settle_pass refuse -> hold_pending_no_sleep_repair holds
        s_hold = oc.select_confirmation_strategy(
            "orders:strategy_hold",
            wm_generation_only,
            generations_observed=1,
            evidence_age_s=0.2,
        )
        assert s_hold.path == "hold_pending_no_sleep_repair"

        # B. Exercise DIVERGENCE_DISPOSITION_LADDER and REPAIR_GATE_LADDER across all states:

        # Case 1: Evidence expired -> evidence_expired holds; reverify_required_expired holds
        trace_expired = oc.CandidateDivergenceTrace(
            unit_id="u-exp",
            table="public.orders",
            key=101,
            observations=(
                oc.RowVersionObservation(
                    generation=1,
                    observed_at_s=10.0,
                    source_present=True,
                    target_present=True,
                    source_fingerprint="fp-a",
                    target_fingerprint="fp-b",
                ),
            ),
        )
        dec_exp = oc.classify_candidate_divergence(trace_expired, wm_position, now_s=200.0)
        assert dec_exp.divergence_state == oc.DivergenceState.EVIDENCE_EXPIRED
        assert dec_exp.repair_eligibility == oc.RepairEligibility.REVERIFY_REQUIRED_EXPIRED
        assert dec_exp.care_badge == oc.CareBadge.REVIEW

        # Case 2: Deleted on both sides -> settled_deleted_on_both holds; no_repair_needed holds
        trace_del_both = oc.CandidateDivergenceTrace(
            unit_id="u-del",
            table="public.orders",
            key=102,
            observations=(
                oc.RowVersionObservation(
                    generation=1,
                    observed_at_s=100.0,
                    source_present=False,
                    target_present=True,
                    source_fingerprint=None,
                    target_fingerprint="fp-old",
                ),
                oc.RowVersionObservation(
                    generation=2,
                    observed_at_s=100.2,
                    source_present=False,
                    target_present=False,
                    source_fingerprint=None,
                    target_fingerprint=None,
                ),
            ),
        )
        dec_del = oc.classify_candidate_divergence(trace_del_both, wm_position, now_s=100.3)
        assert dec_del.divergence_state == oc.DivergenceState.SETTLED_DELETED_ON_BOTH
        assert dec_del.repair_eligibility == oc.RepairEligibility.NO_REPAIR_NEEDED
        assert dec_del.care_badge == oc.CareBadge.PASS

        # Case 3: Converged equal values -> settled_converged holds; no_repair_needed holds
        trace_conv = oc.CandidateDivergenceTrace(
            unit_id="u-conv",
            table="public.orders",
            key=103,
            observations=(
                oc.RowVersionObservation(
                    generation=1,
                    observed_at_s=100.0,
                    source_present=True,
                    target_present=True,
                    source_fingerprint="fp-v2",
                    target_fingerprint="fp-v1",
                ),
                oc.RowVersionObservation(
                    generation=2,
                    observed_at_s=100.2,
                    source_present=True,
                    target_present=True,
                    source_fingerprint="fp-v2",
                    target_fingerprint="fp-v2",
                ),
            ),
        )
        dec_conv = oc.classify_candidate_divergence(trace_conv, wm_position, now_s=100.3)
        assert dec_conv.divergence_state == oc.DivergenceState.SETTLED_CONVERGED
        assert dec_conv.repair_eligibility == oc.RepairEligibility.NO_REPAIR_NEEDED
        assert dec_conv.care_badge == oc.CareBadge.PASS

        # Case 4: Delayed transaction -> pending_delayed_transaction holds; hold_pending_do_not_repair holds
        trace_delayed_txn = oc.CandidateDivergenceTrace(
            unit_id="u-txn",
            table="public.orders",
            key=104,
            observations=(
                oc.RowVersionObservation(
                    generation=1,
                    observed_at_s=100.0,
                    source_present=True,
                    target_present=False,
                    source_fingerprint="fp-uncommitted",
                    target_fingerprint=None,
                    source_row_position="0/16B3748",
                    target_applied_position="0/16B3750",
                    source_open_txn_positions=("0/16B3740",),
                ),
            ),
        )
        dec_txn = oc.classify_candidate_divergence(trace_delayed_txn, wm_position, now_s=100.2)
        assert dec_txn.divergence_state == oc.DivergenceState.PENDING_DELAYED_TRANSACTION
        assert dec_txn.repair_eligibility == oc.RepairEligibility.HOLD_PENDING_DO_NOT_REPAIR
        assert dec_txn.care_badge == oc.CareBadge.BENIGN

        # Case 5: Continuously mutating row -> pending_continuous_mutation holds; hold_pending_do_not_repair holds
        trace_mutating = oc.CandidateDivergenceTrace(
            unit_id="u-mut",
            table="public.orders",
            key=105,
            observations=(
                oc.RowVersionObservation(
                    generation=1,
                    observed_at_s=100.0,
                    source_present=True,
                    target_present=True,
                    source_fingerprint="fp-m1",
                    target_fingerprint="fp-m0",
                    source_row_position="0/16B3740",
                    target_applied_position="0/16B3750",
                ),
                oc.RowVersionObservation(
                    generation=2,
                    observed_at_s=100.2,
                    source_present=True,
                    target_present=True,
                    source_fingerprint="fp-m2",
                    target_fingerprint="fp-m1",
                    source_row_position="0/16B3755",
                    target_applied_position="0/16B3750",
                ),
            ),
        )
        dec_mut = oc.classify_candidate_divergence(trace_mutating, wm_position, now_s=100.3)
        assert dec_mut.divergence_state == oc.DivergenceState.PENDING_CONTINUOUS_MUTATION
        assert dec_mut.repair_eligibility == oc.RepairEligibility.HOLD_PENDING_DO_NOT_REPAIR
        assert dec_mut.care_badge == oc.CareBadge.BENIGN

        # Case 6: Confirmed persistent drift -> persistent_drift holds; eligible_confirmed_drift holds
        trace_drift = oc.CandidateDivergenceTrace(
            unit_id="u-drift",
            table="public.orders",
            key=106,
            observations=(
                oc.RowVersionObservation(
                    generation=1,
                    observed_at_s=100.0,
                    source_present=True,
                    target_present=True,
                    source_fingerprint="fp-src",
                    target_fingerprint="fp-dst-corrupt",
                    source_row_position="0/16B3700",
                    target_applied_position="0/16B3750",
                    changed_columns=("amount",),
                ),
            ),
        )
        dec_drift = oc.classify_candidate_divergence(trace_drift, wm_position, now_s=100.2)
        assert dec_drift.divergence_state == oc.DivergenceState.PERSISTENT_DRIFT
        assert dec_drift.repair_eligibility == oc.RepairEligibility.ELIGIBLE_CONFIRMED_DRIFT
        assert dec_drift.care_badge == oc.CareBadge.ACTION

        # Case 7: Lagging behind watermark (target < source row pos, single pass) -> pending_behind_watermark holds
        wm_lagging = oc.MeasuredWatermarkFacts(
            source_host="127.0.0.1",
            target_host="10.0.0.12",
            source_high_watermark="0/16B3800",
            target_applied_watermark="0/16B3600",
            measured_apply_lag_s=0.5,
            measured_window_scan_s=0.5,
            measured_retention_horizon_s=120.0,
            tail_alive=True,
            supports_position_fence=True,
            supports_generation_reread=True,
            observed_at_s=100.0,
        )
        trace_lag = oc.CandidateDivergenceTrace(
            unit_id="u-lag",
            table="public.orders",
            key=107,
            observations=(
                oc.RowVersionObservation(
                    generation=1,
                    observed_at_s=100.0,
                    source_present=True,
                    target_present=True,
                    source_fingerprint="fp-new",
                    target_fingerprint="fp-old",
                    source_row_position="0/16B3750",
                    target_applied_position="0/16B3600",
                ),
            ),
        )
        dec_lag = oc.classify_candidate_divergence(trace_lag, wm_lagging, now_s=100.2)
        assert dec_lag.divergence_state == oc.DivergenceState.PENDING_BEHIND_WATERMARK
        assert dec_lag.repair_eligibility == oc.RepairEligibility.HOLD_PENDING_DO_NOT_REPAIR
        assert dec_lag.care_badge == oc.CareBadge.BENIGN

    # Verify every declared rung branch was reached under watched()
    for _, _, rung_name, expected_branch in PROVES_RUNGS:
        assert expected_branch in branch(seen, rung_name), (
            f"rung {rung_name!r} did not reach branch {expected_branch!r}; seen={branch(seen, rung_name)}"
        )
    assert why(seen, "position_watermark_fence") is not None
    assert why(seen, "eligible_confirmed_drift") is not None


# -----------------------------------------------------------------------------
# 2. Acceptance Scenarios: Continuously Changing, Deleted-on-Both, Delayed Txns,
#    Lag Without False Corruption, and Persistent Drift Never Suppressed
# -----------------------------------------------------------------------------

def test_continuously_changing_deleted_on_both_and_delayed_transactions_settle_without_false_corruption() -> None:
    wm_t1 = oc.MeasuredWatermarkFacts(
        source_host="127.0.0.1",
        target_host="10.0.0.21",
        source_high_watermark="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-150",
        target_applied_watermark="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-120",
        measured_apply_lag_s=0.4,
        measured_window_scan_s=0.6,
        measured_retention_horizon_s=180.0,
        observed_at_s=500.0,
    )

    # 1. Replication lag at t1: target at GTID 1-120, row modified at GTID 1-140 -> stays pending, no false corruption
    lag_trace_t1 = oc.CandidateDivergenceTrace(
        unit_id="unit-1",
        table="sales.invoices",
        key=9001,
        observations=(
            oc.RowVersionObservation(
                generation=1,
                observed_at_s=500.0,
                source_present=True,
                target_present=True,
                source_fingerprint="sha256:v2",
                target_fingerprint="sha256:v1",
                source_row_position="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-140",
                target_applied_position="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-120",
            ),
        ),
    )
    dec_lag_t1 = oc.classify_candidate_divergence(lag_trace_t1, wm_t1, now_s=500.2)
    assert dec_lag_t1.divergence_state == oc.DivergenceState.PENDING_BEHIND_WATERMARK
    assert not dec_lag_t1.is_persistent_drift
    with pytest.raises(oc.AutomaticRepairOfUncertainRowRefusedError, match="never repaired after an arbitrary sleep"):
        oc.enforce_no_arbitrary_sleep_repair(dec_lag_t1)

    # 2. Target catches up to GTID 1-160: continuously changing row, deleted-on-both row,
    #    and delayed transaction all settle when generation 3 observes their converged state.
    wm_t2 = oc.MeasuredWatermarkFacts(
        source_host="127.0.0.1",
        target_host="10.0.0.21",
        source_high_watermark="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-160",
        target_applied_watermark="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-160",
        measured_apply_lag_s=0.05,
        measured_window_scan_s=0.6,
        measured_retention_horizon_s=180.0,
        observed_at_s=501.0,
    )

    hot_row_settled = oc.CandidateDivergenceTrace(
        unit_id="unit-1",
        table="sales.invoices",
        key=9002,
        observations=(
            oc.RowVersionObservation(1, 500.0, True, True, "fp:1", "fp:0", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-130"),
            oc.RowVersionObservation(2, 500.5, True, True, "fp:2", "fp:1", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-155"),
            oc.RowVersionObservation(3, 501.0, True, True, "fp:2", "fp:2", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-155"),
        ),
    )
    dec_hot = oc.classify_candidate_divergence(hot_row_settled, wm_t2, now_s=501.1)
    assert dec_hot.divergence_state == oc.DivergenceState.SETTLED_CONVERGED
    assert oc.enforce_no_arbitrary_sleep_repair(dec_hot) is False

    deleted_both_trace = oc.CandidateDivergenceTrace(
        unit_id="unit-1",
        table="sales.invoices",
        key=9003,
        observations=(
            oc.RowVersionObservation(1, 500.0, False, True, None, "fp:zombie"),
            oc.RowVersionObservation(2, 501.0, False, False, None, None),
        ),
    )
    dec_del = oc.classify_candidate_divergence(deleted_both_trace, wm_t2, now_s=501.1)
    assert dec_del.divergence_state == oc.DivergenceState.SETTLED_DELETED_ON_BOTH
    assert oc.enforce_no_arbitrary_sleep_repair(dec_del) is False

    delayed_txn_settled = oc.CandidateDivergenceTrace(
        unit_id="unit-1",
        table="sales.invoices",
        key=9004,
        observations=(
            oc.RowVersionObservation(
                1, 500.0, True, False, "fp:new", None,
                source_row_position="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-145",
                source_open_txn_positions=("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-145",),
            ),
            oc.RowVersionObservation(
                2, 501.0, True, True, "fp:new", "fp:new",
                source_row_position="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-145",
                source_open_txn_positions=(),
            ),
        ),
    )
    dec_delayed = oc.classify_candidate_divergence(delayed_txn_settled, wm_t2, now_s=501.1)
    assert dec_delayed.divergence_state == oc.DivergenceState.SETTLED_CONVERGED

    # 3. Persistent drift is never suppressed even when watermarks match or generations hold still
    persistent_drift_trace = oc.CandidateDivergenceTrace(
        unit_id="unit-1",
        table="sales.invoices",
        key=9005,
        observations=(
            oc.RowVersionObservation(
                1, 500.0, True, True, "fp:correct", "fp:corrupted",
                source_row_position="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-110",
                target_applied_position="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-160",
                changed_columns=("total_cents",),
            ),
            oc.RowVersionObservation(
                2, 501.0, True, True, "fp:correct", "fp:corrupted",
                source_row_position="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-110",
                target_applied_position="3e11fa47-71ca-11e1-9e33-c80aa9429562:1-160",
                changed_columns=("total_cents",),
            ),
        ),
    )
    dec_drift = oc.classify_candidate_divergence(persistent_drift_trace, wm_t2, now_s=501.1)
    assert dec_drift.divergence_state == oc.DivergenceState.PERSISTENT_DRIFT
    assert dec_drift.care_badge == oc.CareBadge.ACTION
    assert oc.enforce_no_arbitrary_sleep_repair(dec_drift) is True


# -----------------------------------------------------------------------------
# 3. Proof Unit Counting and Measured Evidence Expiry Tracking
# -----------------------------------------------------------------------------

def test_online_confirmation_coordinator_counts_all_proof_units_and_tracks_expiry() -> None:
    wm = oc.MeasuredWatermarkFacts(
        source_host="127.0.0.1",
        target_host="10.0.0.30",
        source_high_watermark=10_000,
        target_applied_watermark=10_500,
        measured_apply_lag_s=0.5,
        measured_window_scan_s=1.0,
        measured_round_trip_s=0.05,
        measured_retention_horizon_s=60.0,
        observed_at_s=1000.0,
    )
    # Derived TTL: active_cycle = 1.0 + 0.5 + 0.0 + 0.05 = 1.55 -> dynamic_bound = 3.10s
    assert wm.derived_evidence_ttl_s == pytest.approx(3.10)

    coord = oc.OnlineConfirmationCoordinator(wm)
    coord.register_proof_unit("u1-settled", "ledger", 1, 1000, row_count=1000, byte_weight=128_000, captured_at_s=1000.0)
    coord.register_proof_unit("u2-pending", "ledger", 1001, 2000, row_count=1000, byte_weight=128_000, captured_at_s=1000.0)
    coord.register_proof_unit("u3-drift", "ledger", 2001, 3000, row_count=1000, byte_weight=128_000, captured_at_s=1000.0)
    coord.register_proof_unit("u4-expired", "ledger", 3001, 4000, row_count=1000, byte_weight=128_000, captured_at_s=990.0)

    coord.record_candidate_trace(
        oc.CandidateDivergenceTrace(
            unit_id="u1-settled",
            table="ledger",
            key=42,
            observations=(
                oc.RowVersionObservation(1, 1000.0, False, False, None, None),
            ),
        )
    )
    coord.record_candidate_trace(
        oc.CandidateDivergenceTrace(
            unit_id="u2-pending",
            table="ledger",
            key=1042,
            observations=(
                oc.RowVersionObservation(
                    1, 1000.0, True, False, "fp-a", None,
                    source_row_position=10_000,
                    source_open_txn_positions=(9_950,),
                ),
            ),
        )
    )
    coord.record_candidate_trace(
        oc.CandidateDivergenceTrace(
            unit_id="u3-drift",
            table="ledger",
            key=2042,
            observations=(
                oc.RowVersionObservation(
                    1, 1000.0, True, True, "fp-src", "fp-dst",
                    source_row_position=9_800,
                    target_applied_position=10_500,
                ),
            ),
        )
    )

    summary = coord.evaluate_all(now_s=1001.0)
    assert summary.total_selected_units == 4
    assert summary.evaluated_units == 4
    assert summary.settled_units == 1
    assert summary.pending_units == 1
    assert summary.persistent_drift_units == 1
    assert summary.expired_units == 1
    assert summary.unevaluated_units == 0
    assert summary.total_rows_covered == 4000
    assert summary.total_bytes_covered == 512_000
    assert summary.deleted_on_both_keys == (42,)
    assert summary.pending_keys == (1042,)
    assert summary.persistent_drift_keys == (2042,)
    assert summary.expired_unit_ids == ("u4-expired",)
    assert summary.care_badge == oc.CareBadge.ACTION
    assert summary.cutover_ready is False


# -----------------------------------------------------------------------------
# 4. Resilience: O(1) Typed Durable Checkpoint, Lease Lock, AIMD, Merkle, Tool Wrapper
# -----------------------------------------------------------------------------

def test_o1_typed_durable_checkpoint_and_expiry_invalidation(tmp_path: Path) -> None:
    ckpt_file = tmp_path / "online-confirm-ckpt.json"
    ckpt = oc.OnlineConfirmationCheckpoint(ckpt_file)

    composite_start = (
        datetime.datetime(2026, 10, 17, 12, 0, tzinfo=datetime.timezone.utc),
        decimal.Decimal("1234.56"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
    )
    composite_end = (
        datetime.datetime(2026, 10, 17, 13, 0, tzinfo=datetime.timezone.utc),
        decimal.Decimal("9999.99"),
        uuid.UUID("87654321-4321-8765-4321-876543218765"),
    )

    rec = oc.CompletedConfirmationUnitRecord(
        unit_id="range-composite-1",
        table="public.events",
        cursor_start=composite_start,
        cursor_end=composite_end,
        status=oc.ProofUnitStatus.SETTLED,
        source_watermark="0/16B3748",
        confirmed_at_s=200.0,
        ttl_s=30.0,
    )
    ckpt.record_unit(rec)

    reloaded = oc.OnlineConfirmationCheckpoint(ckpt_file)
    assert reloaded.should_skip_unit("range-composite-1", now_s=215.0) is True
    # Past TTL (200 + 30 = 230s), expired evidence must not be skipped
    assert reloaded.should_skip_unit("range-composite-1", now_s=235.0) is False

    loaded_rec = reloaded.get_record("range-composite-1")
    assert loaded_rec is not None
    assert loaded_rec.cursor_start == composite_start
    assert isinstance(loaded_rec.cursor_start[0], datetime.datetime)
    assert isinstance(loaded_rec.cursor_start[1], decimal.Decimal)
    assert isinstance(loaded_rec.cursor_start[2], uuid.UUID)


def test_active_verification_lease_lock_mutual_exclusion_and_heartbeat(tmp_path: Path) -> None:
    lock1 = oc.OnlineVerificationLeaseLock(tmp_path, lease_ttl_s=10.0, owner_id="worker-1")
    lock2 = oc.OnlineVerificationLeaseLock(tmp_path, lease_ttl_s=10.0, owner_id="worker-2")

    lock1.acquire(now_s=50.0)
    try:
        with pytest.raises(oc.VerificationLeaseError, match="already held"):
            lock2.acquire(now_s=51.0)

        lock1.heartbeat(now_s=55.0)
        assert lock1.last_heartbeat_s == 55.0

        with pytest.raises(oc.VerificationLeaseError, match="expired before heartbeat"):
            lock1.heartbeat(now_s=70.0)
    finally:
        lock1.release()

    with lock2:
        assert lock2.lock_path.exists()


def test_aimd_batch_controller_merkle_diff_and_tool_wrapper_sanitization() -> None:
    controller = oc.AdaptiveConfirmationBatchController(
        initial_batch_rows=500,
        min_batch_rows=100,
        max_batch_rows=2000,
        target_latency_s=0.5,
        additive_increase_rows=250,
        multiplicative_decrease_factor=0.5,
    )
    # Cold-start byte-weighted ETA before any batch observation
    assert controller.estimate_remaining_eta_s(
        remaining_bytes=1_000_000,
        cold_start_probe_bytes_per_sec=200_000.0,
    ) == pytest.approx(5.0)

    # Fast batch -> additive increase
    assert controller.observe_batch(rows=500, byte_weight=100_000, elapsed_s=0.25) == 750
    # Slow batch -> multiplicative decrease
    assert controller.observe_batch(rows=750, byte_weight=150_000, elapsed_s=1.2) == 375

    # Merkle bucket diff isolates only divergent buckets and candidate keys
    src_buckets = {
        0: [(1, "h1"), (2, "h2")],
        1: [(3, "h3"), (4, "h4-src")],
    }
    dst_buckets = {
        0: [(1, "h1"), (2, "h2")],
        1: [(3, "h3"), (4, "h4-dst")],
    }
    div_buckets, cand_keys = oc.merkle_diff_proof_buckets(src_buckets, dst_buckets)
    assert div_buckets == (1,)
    assert cand_keys == (4,)

    # External tool wrapper strips internal tool names (reladiff, datafold, debezium, etc.)
    wm = oc.MeasuredWatermarkFacts(
        source_host="127.0.0.1",
        target_host="10.0.0.5",
        source_high_watermark=500,
        target_applied_watermark=600,
        observed_at_s=10.0,
    )
    trace = oc.CandidateDivergenceTrace(
        unit_id="u1",
        table="accounts",
        key=4,
        observations=(
            oc.RowVersionObservation(1, 10.0, True, True, "h4-src", "h4-dst", 500, 600),
        ),
    )
    dec = oc.classify_candidate_divergence(trace, wm, now_s=10.1)
    env = oc.normalize_external_tool_finding(
        "reladiff",
        table="accounts",
        raw_output="reladiff and datafold detected diff via debezium stream",
        decision=dec,
    )
    assert env.care_badge == oc.CareBadge.ACTION
    assert env.confirmed_drift_count == 1
    for forbidden in ("reladiff", "datafold", "debezium", "dms", "dts", "dvt", "pgcopydb"):
        assert forbidden not in env.normalized_message.lower()

    with pytest.raises(oc.UnsafeEndpointHostError):
        oc.validate_safe_endpoint("192.168.1.99")
