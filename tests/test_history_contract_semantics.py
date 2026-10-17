"""Tests for RCH26.history-contract (Row 634): Distinguish current-state migration from complete change history.

Verifies:
- Explicit declaration of current final state vs complete intermediate change history / SCD2.
- Selection of CDC, CT, polling, temporal tables, or periodic snapshots strictly by information content.
- Propagation of history cutoffs, deletion/tombstone semantics, and source retention into proof.
- Detection of insert-and-delete between polls and multiple updates between syncs so they can
  never be falsely certified as complete historical capture.
- Separate coverage ratios, Merkle roots, and verdicts for final-state vs history capture.
- O(1) typed durable checkpoint resume, active lease lock (.run.lock), AIMD batching, and tool wrapper.
"""

from __future__ import annotations

import datetime
import decimal
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from migkit.history_contract import (
     AdaptiveHistoryBatchController,
    AnomalyCategory,
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    CaptureCapabilityProfile,
    CaptureMechanism,
    CapturedHistoryEvent,
    CareBadge,
    DeletionSemantics,
    DeletionTombstonePolicy,
    DurableHistoryContractCheckpoint,
    HistoryContractLease,
    HistoryContractSpec,
    HistoryContractVerifier,
    HistoryCutoffSpec,
    HistoryGoal,
    HistoryToolCapabilityWrapper,
    LeaseLockError,
    MechanismIncompatibleError,
    OperationType,
    PreCutoffMode,
    RetentionWindowProof,
    SourceChangeEvent,
    UnsafeEndpointError,
    evaluate_mechanism_for_contract,
    select_capture_mechanism,
    simulate_mechanism_capture,
    validate_safe_endpoint,
    decide_history_capture_mechanism,
)
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("history_contract", "history_capture_mechanism_rungs", "wal_cdc", "holds"),
    ("history_contract", "history_capture_mechanism_rungs", "wal_cdc", "refuses"),
    ("history_contract", "history_capture_mechanism_rungs", "change_tracking", "holds"),
    ("history_contract", "history_capture_mechanism_rungs", "change_tracking", "refuses"),
    ("history_contract", "history_capture_mechanism_rungs", "temporal_tables", "holds"),
    ("history_contract", "history_capture_mechanism_rungs", "temporal_tables", "refuses"),
    ("history_contract", "history_capture_mechanism_rungs", "snapshot_polling", "holds"),
    ("history_contract", "history_capture_mechanism_rungs", "snapshot_polling", "refuses"),
    ("history_contract", "history_capture_mechanism_rungs", "refuse_incompatible_history", "holds"),
]


@pytest.fixture

def tmp_dir():
    d = Path(tempfile.mkdtemp(prefix="test_history_contract_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _all_default_candidates():
    return [
        CaptureCapabilityProfile.default_for_mechanism(
            CaptureMechanism.LOG_BASED_CDC, full_row_image=True
        ),
        CaptureCapabilityProfile.default_for_mechanism(
            CaptureMechanism.TEMPORAL_SYSTEM_VERSIONED
        ),
        CaptureCapabilityProfile.default_for_mechanism(
            CaptureMechanism.CHANGE_TRACKING_CT
        ),
        CaptureCapabilityProfile.default_for_mechanism(
            CaptureMechanism.TIMESTAMP_POLLING, soft_delete_enabled=False
        ),
        CaptureCapabilityProfile.default_for_mechanism(
            CaptureMechanism.PERIODIC_SNAPSHOT
        ),
    ]


def test_mechanism_selection_for_complete_history_vs_final_state():
    """Choose CDC, CT, polling, or snapshots only when information content satisfies the goal."""
    candidates = _all_default_candidates()

    # 1. COMPLETE_HISTORY goal must reject CT, polling, and periodic snapshots
    history_contract = HistoryContractSpec(
        contract_id="hc-complete-001",
        table_name="public.orders_audit",
        goal=HistoryGoal.COMPLETE_HISTORY,
        tombstone_policy=DeletionTombstonePolicy(
            deletion_semantics=DeletionSemantics.FULL_BEFORE_IMAGE_TOMBSTONE,
            require_before_image_on_delete=True,
            require_before_image_on_update=True,
            require_transient_lifecycle_capture=True,
        ),
        source_host="127.0.0.1",
        target_host="10.0.0.11",
    )

    decision = select_capture_mechanism(history_contract, candidates)
    assert decision.selected_mechanism in (
        CaptureMechanism.LOG_BASED_CDC,
        CaptureMechanism.TEMPORAL_SYSTEM_VERSIONED,
    )
    assert decision.care_badge == CareBadge.PASS

    by_mech = {ev.mechanism: ev for ev in decision.evaluations}
    assert by_mech[CaptureMechanism.LOG_BASED_CDC].satisfied is True
    assert by_mech[CaptureMechanism.TEMPORAL_SYSTEM_VERSIONED].satisfied is True
    assert by_mech[CaptureMechanism.CHANGE_TRACKING_CT].satisfied is False
    assert by_mech[CaptureMechanism.TIMESTAMP_POLLING].satisfied is False
    assert by_mech[CaptureMechanism.PERIODIC_SNAPSHOT].satisfied is False

    # Verify CT and Polling rejection reasons explicitly cite coalesced updates and transient lifecycles
    ct_reasons = " ".join(by_mech[CaptureMechanism.CHANGE_TRACKING_CT].rejection_reasons)
    assert "intermediate" in ct_reasons
    assert "coalesces" in ct_reasons
    assert "insert-and-delete" in ct_reasons

    # 2. If only CT and Polling are offered for COMPLETE_HISTORY, strict mode raises MechanismIncompatibleError
    inadequate_only = [
        CaptureCapabilityProfile.default_for_mechanism(CaptureMechanism.CHANGE_TRACKING_CT),
        CaptureCapabilityProfile.default_for_mechanism(CaptureMechanism.TIMESTAMP_POLLING),
    ]
    with pytest.raises(MechanismIncompatibleError):
        select_capture_mechanism(history_contract, inadequate_only, strict=True)

    non_strict_refusal = select_capture_mechanism(history_contract, inadequate_only, strict=False)
    assert non_strict_refusal.selected_mechanism is None
    assert non_strict_refusal.care_badge == CareBadge.ACTION
    assert "REFUSED" in non_strict_refusal.selection_rationale

    # 3. FINAL_STATE goal can be satisfied by CHANGE_TRACKING_CT or PERIODIC_SNAPSHOT when deletes exist,
    #    but TIMESTAMP_POLLING without soft-deletes is still rejected when source_has_hard_deletes=True
    final_state_contract = HistoryContractSpec(
        contract_id="hc-state-002",
        table_name="public.customers",
        goal=HistoryGoal.FINAL_STATE,
        source_has_hard_deletes=True,
        source_host="127.0.0.1",
        target_host="10.0.0.12",
    )
    ct_eval = evaluate_mechanism_for_contract(
        final_state_contract,
        CaptureCapabilityProfile.default_for_mechanism(CaptureMechanism.CHANGE_TRACKING_CT),
    )
    assert ct_eval.satisfied is True
    assert ct_eval.care_badge == CareBadge.PASS

    poll_no_soft_eval = evaluate_mechanism_for_contract(
        final_state_contract,
        CaptureCapabilityProfile.default_for_mechanism(
            CaptureMechanism.TIMESTAMP_POLLING, soft_delete_enabled=False
        ),
    )
    assert poll_no_soft_eval.satisfied is False
    assert any("hard deletes" in r for r in poll_no_soft_eval.rejection_reasons)

    poll_soft_eval = evaluate_mechanism_for_contract(
        final_state_contract,
        CaptureCapabilityProfile.default_for_mechanism(
            CaptureMechanism.TIMESTAMP_POLLING, soft_delete_enabled=True
        ),
    )
    assert poll_soft_eval.satisfied is True


def test_insert_and_delete_between_polls_cannot_pass_complete_history():
    """Insert-and-delete between polls satisfies final state (both sides empty)
    but MUST fail complete history capture."""
    t0 = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc)

    # Row 101 is inserted at t0+1m, updated at t0+2m, and deleted at t0+3m,
    # all within sync_window_id=1 before the poll runs at t0+5m.
    # Row 102 is inserted at t0+1m and stays live.
    source_events = [
        SourceChangeEvent(
            event_id="ev-1",
            primary_key=101,
            operation=OperationType.INSERT,
            sequence_no=1,
            lsn="0/1000",
            committed_at=t0 + datetime.timedelta(minutes=1),
            before_image=None,
            after_image={"id": 101, "status": "created", "amount": decimal.Decimal("50.00")},
            sync_window_id=1,
        ),
        SourceChangeEvent(
            event_id="ev-2",
            primary_key=101,
            operation=OperationType.UPDATE,
            sequence_no=2,
            lsn="0/1010",
            committed_at=t0 + datetime.timedelta(minutes=2),
            before_image={"id": 101, "status": "created", "amount": decimal.Decimal("50.00")},
            after_image={"id": 101, "status": "cancelled", "amount": decimal.Decimal("0.00")},
            sync_window_id=1,
        ),
        SourceChangeEvent(
            event_id="ev-3",
            primary_key=101,
            operation=OperationType.DELETE,
            sequence_no=3,
            lsn="0/1020",
            committed_at=t0 + datetime.timedelta(minutes=3),
            before_image={"id": 101, "status": "cancelled", "amount": decimal.Decimal("0.00")},
            after_image=None,
            sync_window_id=1,
        ),
        SourceChangeEvent(
            event_id="ev-4",
            primary_key=102,
            operation=OperationType.INSERT,
            sequence_no=4,
            lsn="0/1030",
            committed_at=t0 + datetime.timedelta(minutes=4),
            before_image=None,
            after_image={"id": 102, "status": "active", "amount": decimal.Decimal("125.00")},
            sync_window_id=1,
        ),
    ]

    poll_profile = CaptureCapabilityProfile.default_for_mechanism(
        CaptureMechanism.TIMESTAMP_POLLING, soft_delete_enabled=True
    )
    target_state_poll, captured_hist_poll = simulate_mechanism_capture(
        source_events, poll_profile
    )

    # Under COMPLETE_HISTORY goal, polling fails both mechanism check and event history check
    history_contract = HistoryContractSpec(
        contract_id="hc-ins-del-history",
        table_name="public.ephemeral_orders",
        goal=HistoryGoal.COMPLETE_HISTORY,
        source_host="127.0.0.1",
        target_host="10.0.0.20",
    )
    verifier_hist = HistoryContractVerifier(history_contract, poll_profile)
    proof_hist = verifier_hist.verify(source_events, target_state_poll, captured_hist_poll)

    # Final state has converged (key 101 is absent on both source and target; key 102 matches)!
    assert proof_hist.final_state.verdict == "same"
    assert proof_hist.final_state.final_state_coverage_ratio == 1.0
    assert proof_hist.final_state.care_badge == CareBadge.PASS

    # Yet history capture MUST be "different" / ACTION with 3 transient lifecycle events lost!
    assert proof_hist.history_capture.verdict == "different"
    assert proof_hist.history_capture.care_badge == CareBadge.ACTION
    assert proof_hist.history_capture.transient_insert_deletes_lost == 3
    assert proof_hist.history_capture.history_coverage_ratio == pytest.approx(0.25)
    assert proof_hist.contract_satisfied is False
    assert proof_hist.overall_verdict == "different"
    assert proof_hist.overall_care_badge == CareBadge.ACTION

    cats = [a.category for a in proof_hist.history_capture.anomalies]
    assert AnomalyCategory.TRANSIENT_INSERT_DELETE_LOST in cats

    # Under FINAL_STATE goal with soft-delete polling, final-state contract is satisfied,
    # while history_capture still honestly records the lost transient lifecycle as BENIGN!
    state_contract = HistoryContractSpec(
        contract_id="hc-ins-del-state",
        table_name="public.ephemeral_orders",
        goal=HistoryGoal.FINAL_STATE,
        source_host="127.0.0.1",
        target_host="10.0.0.20",
    )
    verifier_state = HistoryContractVerifier(state_contract, poll_profile)
    proof_state = verifier_state.verify(source_events, target_state_poll, captured_hist_poll)

    assert proof_state.final_state.verdict == "same"
    assert proof_state.history_capture.verdict == "different"
    assert proof_state.history_capture.care_badge == CareBadge.BENIGN
    assert proof_state.contract_satisfied is True
    assert proof_state.overall_verdict == "same"
    assert proof_state.overall_care_badge == CareBadge.BENIGN


def test_multiple_updates_between_syncs_cannot_pass_complete_history():
    """Multiple updates to the same key between syncs coalesce under CT/polling
    and cannot be falsely declared complete historical capture."""
    t0 = datetime.datetime(2026, 10, 17, 14, 0, 0, tzinfo=datetime.timezone.utc)

    source_events = [
        SourceChangeEvent(
            event_id="u-1",
            primary_key="acct-900",
            operation=OperationType.INSERT,
            sequence_no=10,
            lsn="0/2000",
            committed_at=t0,
            before_image=None,
            after_image={"id": "acct-900", "tier": "bronze", "balance": 100},
            sync_window_id=1,
        ),
        SourceChangeEvent(
            event_id="u-2",
            primary_key="acct-900",
            operation=OperationType.UPDATE,
            sequence_no=11,
            lsn="0/2010",
            committed_at=t0 + datetime.timedelta(seconds=10),
            before_image={"id": "acct-900", "tier": "bronze", "balance": 100},
            after_image={"id": "acct-900", "tier": "silver", "balance": 500},
            sync_window_id=2,
        ),
        SourceChangeEvent(
            event_id="u-3",
            primary_key="acct-900",
            operation=OperationType.UPDATE,
            sequence_no=12,
            lsn="0/2020",
            committed_at=t0 + datetime.timedelta(seconds=20),
            before_image={"id": "acct-900", "tier": "silver", "balance": 500},
            after_image={"id": "acct-900", "tier": "gold", "balance": 1500},
            sync_window_id=2,
        ),
        SourceChangeEvent(
            event_id="u-4",
            primary_key="acct-900",
            operation=OperationType.UPDATE,
            sequence_no=13,
            lsn="0/2030",
            committed_at=t0 + datetime.timedelta(seconds=30),
            before_image={"id": "acct-900", "tier": "gold", "balance": 1500},
            after_image={"id": "acct-900", "tier": "platinum", "balance": 5000},
            sync_window_id=2,
        ),
    ]

    # 1. Change Tracking (CT) coalesces u-2, u-3, u-4 in window 2 into a single update (u-4)
    ct_profile = CaptureCapabilityProfile.default_for_mechanism(
        CaptureMechanism.CHANGE_TRACKING_CT
    )
    ct_final_state, ct_captured = simulate_mechanism_capture(source_events, ct_profile)

    scd2_contract = HistoryContractSpec(
        contract_id="hc-scd2-multi-upd",
        table_name="dim.customer_tier_scd2",
        goal=HistoryGoal.SCD2_TIMELINE,
        tombstone_policy=DeletionTombstonePolicy(
            deletion_semantics=DeletionSemantics.SCD2_CLOSE_INTERVAL,
            require_scd2_interval_closure=True,
        ),
        source_host="127.0.0.1",
        target_host="10.0.0.21",
    )

    ct_proof = HistoryContractVerifier(scd2_contract, ct_profile).verify(
        source_events, ct_final_state, ct_captured
    )

    # Final state is 100% matched ("platinum", balance=5000), but 2 intermediate versions ("silver", "gold") were lost!
    assert ct_proof.final_state.verdict == "same"
    assert ct_proof.final_state.final_state_coverage_ratio == 1.0
    assert ct_proof.history_capture.verdict == "different"
    assert ct_proof.history_capture.coalesced_intermediate_updates_lost == 2
    assert ct_proof.contract_satisfied is False
    assert ct_proof.overall_verdict == "different"
    assert ct_proof.overall_care_badge == CareBadge.ACTION
    assert any(
        a.category == AnomalyCategory.MULTI_UPDATE_COALESCED
        and a.lost_event_ids == ("u-2", "u-3")
        for a in ct_proof.history_capture.anomalies
    )

    # 2. Log-Based CDC captures every intermediate transition and closes SCD2 intervals
    cdc_profile = CaptureCapabilityProfile.default_for_mechanism(
        CaptureMechanism.LOG_BASED_CDC, full_row_image=True
    )
    cdc_final_state, cdc_captured = simulate_mechanism_capture(source_events, cdc_profile)
    cdc_proof = HistoryContractVerifier(scd2_contract, cdc_profile).verify(
        source_events, cdc_final_state, cdc_captured
    )

    assert cdc_proof.final_state.verdict == "same"
    assert cdc_proof.history_capture.verdict == "same"
    assert cdc_proof.history_capture.history_coverage_ratio == 1.0
    assert cdc_proof.history_capture.coalesced_intermediate_updates_lost == 0
    assert cdc_proof.history_capture.unclosed_scd2_intervals == 0
    assert cdc_proof.contract_satisfied is True
    assert cdc_proof.overall_verdict == "same"
    assert cdc_proof.overall_care_badge == CareBadge.PASS
    assert cdc_proof.final_state.merkle_root != cdc_proof.history_capture.merkle_root


def test_history_cutoffs_tombstones_and_retention_proof():
    """Carry history cutoffs, deletion/tombstone semantics, and retention into proof."""
    t_cutoff = datetime.datetime(2026, 10, 17, 15, 0, 0, tzinfo=datetime.timezone.utc)

    source_events = [
        # Pre-cutoff baseline event at LSN 0/100
        SourceChangeEvent(
            event_id="pre-1",
            primary_key=1,
            operation=OperationType.INSERT,
            sequence_no=1,
            lsn="0/100",
            committed_at=t_cutoff - datetime.timedelta(hours=2),
            before_image=None,
            after_image={"id": 1, "v": "v0"},
            sync_window_id=0,
        ),
        # Post-cutoff update and delete
        SourceChangeEvent(
            event_id="post-1",
            primary_key=1,
            operation=OperationType.UPDATE,
            sequence_no=2,
            lsn="0/500",
            committed_at=t_cutoff + datetime.timedelta(minutes=5),
            before_image={"id": 1, "v": "v0"},
            after_image={"id": 1, "v": "v1"},
            sync_window_id=1,
        ),
        SourceChangeEvent(
            event_id="post-2",
            primary_key=1,
            operation=OperationType.DELETE,
            sequence_no=3,
            lsn="0/550",
            committed_at=t_cutoff + datetime.timedelta(minutes=10),
            before_image={"id": 1, "v": "v1"},
            after_image=None,
            sync_window_id=1,
        ),
    ]

    # Case A: WINDOWED_HISTORY with SNAPSHOT_BASELINE_ONLY before cutoff LSN 0/500
    # and valid retention window
    valid_retention = RetentionWindowProof(
        source_retention_seconds=86400.0,
        max_sync_interval_seconds=300.0,
        source_oldest_available_lsn="0/400",
        required_start_lsn="0/500",
    )
    windowed_contract = HistoryContractSpec(
        contract_id="hc-windowed-cutoff",
        table_name="public.ledger_entries",
        goal=HistoryGoal.WINDOWED_HISTORY,
        cutoff=HistoryCutoffSpec(
            cutoff_lsn="0/500",
            pre_cutoff_mode=PreCutoffMode.SNAPSHOT_BASELINE_ONLY,
        ),
        tombstone_policy=DeletionTombstonePolicy(
            deletion_semantics=DeletionSemantics.FULL_BEFORE_IMAGE_TOMBSTONE,
            require_before_image_on_delete=True,
        ),
        retention_proof=valid_retention,
    )

    cdc_full = CaptureCapabilityProfile.default_for_mechanism(
        CaptureMechanism.LOG_BASED_CDC, full_row_image=True
    )
    # Only post-cutoff transitions (seq 2 and 3) are in the CDC stream
    _, post_captured = simulate_mechanism_capture(source_events[1:], cdc_full)
    proof_ok = HistoryContractVerifier(windowed_contract, cdc_full).verify(
        source_events, {}, post_captured
    )
    assert proof_ok.contract_satisfied is True
    assert proof_ok.history_capture.pre_cutoff_baseline_events == 1
    assert proof_ok.history_capture.total_post_cutoff_transitions == 2
    assert proof_ok.history_capture.captured_post_cutoff_transitions == 2

    # Case B: Missing before-image on DELETE tombstone fails FULL_BEFORE_IMAGE_TOMBSTONE policy
    cdc_key_only = CaptureCapabilityProfile.default_for_mechanism(
        CaptureMechanism.LOG_BASED_CDC, full_row_image=False
    )
    _, key_only_captured = simulate_mechanism_capture(source_events[1:], cdc_key_only)
    proof_no_before = HistoryContractVerifier(windowed_contract, cdc_key_only).verify(
        source_events, {}, key_only_captured
    )
    assert proof_no_before.contract_satisfied is False
    assert proof_no_before.history_capture.missing_before_images == 1
    assert any(
        a.category == AnomalyCategory.BEFORE_IMAGE_MISSING_ON_DELETE
        for a in proof_no_before.history_capture.anomalies
    )

    # Case C: Retention LSN gap (source purged past required_start_lsn) fails proof
    breached_retention = RetentionWindowProof(
        source_retention_seconds=3600.0,
        max_sync_interval_seconds=600.0,
        source_oldest_available_lsn="0/900",
        required_start_lsn="0/500",
    )
    breached_contract = HistoryContractSpec(
        contract_id="hc-retention-breach",
        table_name="public.ledger_entries",
        goal=HistoryGoal.WINDOWED_HISTORY,
        cutoff=HistoryCutoffSpec(cutoff_lsn="0/500"),
        retention_proof=breached_retention,
    )
    proof_breach = HistoryContractVerifier(breached_contract, cdc_full).verify(
        source_events, {}, post_captured
    )
    assert proof_breach.contract_satisfied is False
    assert proof_breach.history_capture.retention_gap_detected is True
    assert proof_breach.overall_care_badge == CareBadge.ACTION


def test_zombie_rows_and_unclosed_scd2_intervals_detected():
    """Verify hard-delete omission leaves zombie rows on target and broken SCD2 closure is caught."""
    t0 = datetime.datetime(2026, 10, 17, 16, 0, 0, tzinfo=datetime.timezone.utc)
    source_events = [
        SourceChangeEvent(
            event_id="z-1",
            primary_key=501,
            operation=OperationType.INSERT,
            sequence_no=1,
            lsn="0/100",
            committed_at=t0,
            after_image={"id": 501, "name": "temp_user"},
            sync_window_id=1,
        ),
        SourceChangeEvent(
            event_id="z-2",
            primary_key=501,
            operation=OperationType.DELETE,
            sequence_no=2,
            lsn="0/200",
            committed_at=t0 + datetime.timedelta(minutes=10),
            before_image={"id": 501, "name": "temp_user"},
            after_image=None,
            sync_window_id=2,
        ),
    ]

    # Timestamp polling without soft-delete misses the DELETE in window 2, leaving key 501 as a zombie row!
    poll_no_del = CaptureCapabilityProfile.default_for_mechanism(
        CaptureMechanism.TIMESTAMP_POLLING, soft_delete_enabled=False
    )
    target_state, captured_hist = simulate_mechanism_capture(source_events, poll_no_del)
    assert 501 in target_state

    contract = HistoryContractSpec(
        contract_id="hc-zombie-check",
        table_name="public.users",
        goal=HistoryGoal.FINAL_STATE,
        source_has_hard_deletes=True,
    )
    proof = HistoryContractVerifier(contract, poll_no_del).verify(
        source_events, target_state, captured_hist
    )
    assert proof.final_state.zombie_keys_on_target == 1
    assert proof.final_state.verdict == "different"
    assert proof.final_state.care_badge == CareBadge.ACTION
    assert proof.contract_satisfied is False

    # Now verify broken SCD2 interval closure detection (e.g. previous version left with is_current=True)
    scd2_contract = HistoryContractSpec(
        contract_id="hc-scd2-broken",
        table_name="dim.users_scd2",
        goal=HistoryGoal.SCD2_TIMELINE,
        tombstone_policy=DeletionTombstonePolicy(
            deletion_semantics=DeletionSemantics.SCD2_CLOSE_INTERVAL,
            require_scd2_interval_closure=True,
        ),
    )
    cdc_prof = CaptureCapabilityProfile.default_for_mechanism(CaptureMechanism.LOG_BASED_CDC)
    broken_scd2_history = [
        CapturedHistoryEvent(
            primary_key=501,
            operation=OperationType.INSERT,
            sequence_no=1,
            lsn="0/100",
            committed_at=t0,
            after_image={"id": 501, "name": "temp_user"},
            is_tombstone=False,
            effective_from=t0,
            effective_to=None,  # BUG: unclosed interval after DELETE!
            is_current=True,    # BUG: still marked current after DELETE!
            sync_window_id=1,
        ),
        CapturedHistoryEvent(
            primary_key=501,
            operation=OperationType.DELETE,
            sequence_no=2,
            lsn="0/200",
            committed_at=t0 + datetime.timedelta(minutes=10),
            before_image={"id": 501, "name": "temp_user"},
            is_tombstone=True,
            effective_from=t0 + datetime.timedelta(minutes=10),
            effective_to=t0 + datetime.timedelta(minutes=10),
            is_current=False,
            sync_window_id=2,
        ),
    ]
    scd2_proof = HistoryContractVerifier(scd2_contract, cdc_prof).verify(
        source_events, {}, broken_scd2_history
    )
    assert scd2_proof.final_state.verdict == "same"
    assert scd2_proof.history_capture.unclosed_scd2_intervals == 1
    assert scd2_proof.history_capture.verdict == "different"
    assert scd2_proof.contract_satisfied is False

    report = scd2_proof.format_unix_report()
    assert f"[{CARE_ACTION}] HISTORY_CONTRACT:" in report
    assert f"[{CARE_PASS}] FINAL_STATE_VERDICT:" in report
    assert f"[{CARE_ACTION}] HISTORY_CAPTURE_VERDICT:" in report
    assert all(ord(ch) < 128 for ch in report)


def test_o1_durable_typed_checkpoint_and_active_lease_lock(tmp_dir: Path):
    """Verify O(1) typed durable resume preserving datetime/Decimal/UUID/tuple and .run.lock lease."""
    cp = DurableHistoryContractCheckpoint(session_id="sess-634", checkpoint_dir=tmp_dir)
    cursor_val = (
        datetime.datetime(2026, 10, 17, 18, 30, 0, tzinfo=datetime.timezone.utc),
        decimal.Decimal("99999.1234"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
    )
    cp.record_window_completed(
        1,
        merkle_root="abc123merkle",
        durable_lsn="0/8888",
        typed_cursor=cursor_val,
        final_state_verdict="same",
        history_verdict="same",
    )
    cp.save()

    loaded = DurableHistoryContractCheckpoint.load(tmp_dir, "sess-634")
    assert loaded.is_window_completed(1, expected_merkle="abc123merkle") is True
    assert loaded.is_window_completed(1, expected_merkle="wrong_merkle") is False
    assert loaded.is_window_completed(2) is False
    assert loaded.last_durable_lsn == "0/8888"
    assert isinstance(loaded.last_durable_cursor, tuple)
    assert isinstance(loaded.last_durable_cursor[0], datetime.datetime)
    assert isinstance(loaded.last_durable_cursor[1], decimal.Decimal)
    assert isinstance(loaded.last_durable_cursor[2], uuid.UUID)
    assert loaded.last_durable_cursor == cursor_val

    # Mutual exclusion lease lock (.run.lock)
    with HistoryContractLease(tmp_dir, ttl_seconds=30.0) as lease1:
        assert (tmp_dir / ".run.lock").is_file()
        lease1.heartbeat()
        with pytest.raises(LeaseLockError):
            HistoryContractLease(tmp_dir, ttl_seconds=30.0).acquire()

    assert not (tmp_dir / ".run.lock").exists()


def test_adaptive_aimd_batching_tool_wrapper_and_safe_hosts():
    """Verify AIMD batch sizing, byte-weighted ETA, external tool wrapper, and host validation."""
    ctrl = AdaptiveHistoryBatchController(
        current_batch_events=1000,
        additive_increase=500,
        multiplicative_decrease=0.5,
        target_latency_ms=200.0,
    )
    # Cold-start ETA before any batches
    eta_cold = ctrl.estimate_eta_seconds(50 * 1024 * 1024)
    assert eta_cold == pytest.approx(2.0)

    # Fast batch -> additive increase
    assert ctrl.record_batch(1000, 5 * 1024 * 1024, latency_ms=100.0) == 1500
    # Slow/throttled batch -> multiplicative decrease
    assert ctrl.record_batch(1500, 5 * 1024 * 1024, latency_ms=450.0) == 750
    assert ctrl.estimate_eta_seconds(0) == 0.0
    assert ctrl.estimate_eta_seconds(10 * 1024 * 1024) > 0.0

    # External tool capability wrapper
    res_debezium = HistoryToolCapabilityWrapper.evaluate_tool_for_goal(
        "debezium", HistoryGoal.COMPLETE_HISTORY
    )
    assert res_debezium.care_badge == CareBadge.PASS
    assert res_debezium.supports_complete_history is True

    res_datafold_hist = HistoryToolCapabilityWrapper.evaluate_tool_for_goal(
        "datafold_diff", HistoryGoal.COMPLETE_HISTORY
    )
    assert res_datafold_hist.care_badge == CareBadge.ACTION
    assert res_datafold_hist.status == "refused_capability_mismatch"

    res_datafold_state = HistoryToolCapabilityWrapper.evaluate_tool_for_goal(
        "datafold_diff", HistoryGoal.FINAL_STATE
    )
    assert res_datafold_state.care_badge == CareBadge.PASS

    # Sanitized runner failure (no internal tool exception leak)
    def _failing_runner():
        raise RuntimeError("internal vendor secret stacktrace")

    res_err = HistoryToolCapabilityWrapper.evaluate_tool_for_goal(
        "dms_cdc", HistoryGoal.COMPLETE_HISTORY, runner=_failing_runner
    )
    assert res_err.care_badge == CareBadge.ACTION
    assert res_err.status == "normalized_adapter_error"
    assert "secret stacktrace" not in res_err.summary

    # Safe host check
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("192.168.1.100")


def test_out_of_order_before_image_update_pre_cutoff_archive_and_state_drift():
    """Verify out-of-order sequences, missing update before-images, pre-cutoff archive enforcement,
    CT version expiry, and final-state drift/missing keys."""
    t_cutoff = datetime.datetime(2026, 10, 17, 20, 0, 0, tzinfo=datetime.timezone.utc)

    source_events = [
        SourceChangeEvent(
            event_id="e-pre",
            primary_key=77,
            operation=OperationType.INSERT,
            sequence_no=1,
            lsn="0/0100",
            committed_at=t_cutoff - datetime.timedelta(minutes=30),
            after_image={"id": 77, "val": "initial"},
            sync_window_id=0,
        ),
        SourceChangeEvent(
            event_id="e-upd1",
            primary_key=77,
            operation=OperationType.UPDATE,
            sequence_no=2,
            lsn="0/0200",
            committed_at=t_cutoff + datetime.timedelta(minutes=5),
            before_image={"id": 77, "val": "initial"},
            after_image={"id": 77, "val": "v2"},
            sync_window_id=1,
        ),
        SourceChangeEvent(
            event_id="e-upd2",
            primary_key=77,
            operation=OperationType.UPDATE,
            sequence_no=3,
            lsn="0/0300",
            committed_at=t_cutoff + datetime.timedelta(minutes=10),
            before_image={"id": 77, "val": "v2"},
            after_image={"id": 77, "val": "v3"},
            sync_window_id=2,
        ),
        SourceChangeEvent(
            event_id="e-missing-live",
            primary_key=88,
            operation=OperationType.INSERT,
            sequence_no=4,
            lsn="0/0400",
            committed_at=t_cutoff + datetime.timedelta(minutes=12),
            after_image={"id": 88, "val": "must_exist"},
            sync_window_id=2,
        ),
    ]

    # Contract requiring FULL_ARCHIVE_REQUIRED prior to cutoff and before_image on UPDATE
    ct_expired_retention = RetentionWindowProof(
        source_retention_seconds=7200.0,
        max_sync_interval_seconds=600.0,
        source_min_valid_version=50,
        last_synced_version=40,
    )
    contract = HistoryContractSpec(
        contract_id="hc-edge-anomalies",
        table_name="public.audit_trail",
        goal=HistoryGoal.COMPLETE_HISTORY,
        cutoff=HistoryCutoffSpec(
            cutoff_lsn="0/0200",
            pre_cutoff_mode=PreCutoffMode.FULL_ARCHIVE_REQUIRED,
        ),
        tombstone_policy=DeletionTombstonePolicy(
            require_before_image_on_update=True,
        ),
        retention_proof=ct_expired_retention,
    )

    # Target has out-of-order history (seq 3 before seq 2), missing before_image on seq 2,
    # missing pre-cutoff seq 1, drifted value on key 77, and missing key 88
    captured_out_of_order = [
        CapturedHistoryEvent(
            primary_key=77,
            operation=OperationType.UPDATE,
            sequence_no=3,
            lsn="0/0300",
            committed_at=t_cutoff + datetime.timedelta(minutes=10),
            before_image={"id": 77, "val": "v2"},
            after_image={"id": 77, "val": "v3"},
            sync_window_id=2,
        ),
        CapturedHistoryEvent(
            primary_key=77,
            operation=OperationType.UPDATE,
            sequence_no=2,
            lsn="0/0200",
            committed_at=t_cutoff + datetime.timedelta(minutes=5),
            before_image=None,  # Missing required before-image on UPDATE
            after_image={"id": 77, "val": "v2"},
            sync_window_id=1,
        ),
    ]
    drifted_target_state = {
        77: {"id": 77, "val": "DRIFTED_VALUE"},
        # key 88 is missing
    }

    cdc_prof = CaptureCapabilityProfile.default_for_mechanism(CaptureMechanism.LOG_BASED_CDC)
    proof = HistoryContractVerifier(contract, cdc_prof).verify(
        source_events, drifted_target_state, captured_out_of_order
    )

    assert proof.final_state.verdict == "different"
    assert proof.final_state.mismatched_live_keys == 1
    assert proof.final_state.missing_live_keys == 1
    fs_cats = {a.category for a in proof.final_state.anomalies}
    assert AnomalyCategory.FINAL_STATE_VALUE_DRIFT in fs_cats
    assert AnomalyCategory.FINAL_STATE_MISSING_KEY in fs_cats

    assert proof.history_capture.verdict == "different"
    assert proof.history_capture.out_of_order_transitions == 1
    assert proof.history_capture.missing_before_images == 1
    assert proof.history_capture.retention_gap_detected is True
    hc_cats = {a.category for a in proof.history_capture.anomalies}
    assert AnomalyCategory.PRE_CUTOFF_ARCHIVE_MISSING in hc_cats
    assert AnomalyCategory.BEFORE_IMAGE_MISSING_ON_UPDATE in hc_cats
    assert AnomalyCategory.OUT_OF_ORDER_HISTORY_SEQUENCE in hc_cats
    assert AnomalyCategory.RETENTION_WINDOW_GAP in hc_cats


def test_history_capture_mechanism_rungs_reachability():
    """Prove all branches of history_capture_mechanism_rungs under watched()."""
    with watched():
        # 1. wal_cdc holds
        c1 = decide_history_capture_mechanism({
            "has_wal_cdc": True,
            "retention_window_valid": True,
        })
        assert c1.rung.name == "wal_cdc"

        # 2. wal_cdc refuses, change_tracking holds
        c2 = decide_history_capture_mechanism({
            "has_wal_cdc": False,
            "has_change_tracking": True,
            "version_tracking_enabled": True,
        })
        assert c2.rung.name == "change_tracking"

        # 3. change_tracking refuses, temporal_tables holds
        c3 = decide_history_capture_mechanism({
            "has_wal_cdc": False,
            "has_change_tracking": False,
            "has_temporal_tables": True,
        })
        assert c3.rung.name == "temporal_tables"

        # 4. temporal_tables refuses, snapshot_polling holds
        c4 = decide_history_capture_mechanism({
            "has_wal_cdc": False,
            "has_change_tracking": False,
            "has_temporal_tables": False,
            "is_final_state_goal": True,
        })
        assert c4.rung.name == "snapshot_polling"

        # 5. snapshot_polling refuses, refuse_incompatible_history holds
        c5 = decide_history_capture_mechanism({
            "has_wal_cdc": False,
            "has_change_tracking": False,
            "has_temporal_tables": False,
            "is_final_state_goal": False,
        })
        assert c5.rung.name == "refuse_incompatible_history"


