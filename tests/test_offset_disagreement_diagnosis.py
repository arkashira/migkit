import pytest

from migkit.offset_diag import (
    INTERVAL_ALREADY_APPLIED,
    INTERVAL_ALREADY_RETAINED,
    INTERVAL_MISSING_LOG,
    INTERVAL_UNKNOWN,
    BoundedReseedRequirement,
    Interval,
    LogEvidence,
    NaiveRuleVerdict,
    OffsetReconciliation,
    OffsetState,
    ReconciliationResult,
    ReconstructionCertificate,
    SlotState,
    format_lsn,
    parse_lsn,
    decide_offset_reconciliation,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("offset_diag", "offset_reconciliation_rungs", "resume_from_consistent_offset", "holds"),
    ("offset_diag", "offset_reconciliation_rungs", "resume_from_consistent_offset", "refuses"),
    ("offset_diag", "offset_reconciliation_rungs", "fast_forward_safe_reconstructed", "holds"),
    ("offset_diag", "offset_reconciliation_rungs", "fast_forward_safe_reconstructed", "refuses"),
    ("offset_diag", "offset_reconciliation_rungs", "bounded_reseed_and_refuse", "holds"),
]


def test_parse_and_format_lsn():
    assert parse_lsn("0/16B3748") == 0x16B3748
    assert parse_lsn("1/0") == 0x100000000
    assert parse_lsn(123456) == 123456
    assert parse_lsn("123456") == 123456
    assert parse_lsn("0x16B3748") == 0x16B3748
    assert parse_lsn(None) is None

    assert format_lsn(0x16B3748) == "0/16B3748"
    assert format_lsn(0x100000000) == "1/0"
    assert format_lsn(None) is None

    with pytest.raises(ValueError):
        parse_lsn("not-an-lsn")


def test_offset_in_sync_with_slot():
    lsn = "0/16B3748"
    offset = OffsetState(offset_lsn=lsn, timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn=lsn,
        confirmed_flush_lsn=lsn,
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    res = OffsetReconciliation(offset, slot).reconcile()
    assert res.status == "in_sync"
    assert res.is_safe()
    assert not res.needs_reseed()
    assert res.certificate is not None
    assert res.certificate.certified
    assert res.certificate.resume_lsn == parse_lsn(lsn)
    assert res.reseed_requirement is None
    assert len(res.retained_intervals()) == 1
    assert res.retained_intervals()[0].kind == INTERVAL_ALREADY_RETAINED
    assert res.retained_intervals()[0].start_lsn == parse_lsn(lsn)
    assert res.retained_intervals()[0].end_lsn == parse_lsn("0/16B4000")


def test_offset_ahead_of_slot_fast_forward_certificate():
    offset = OffsetState(offset_lsn="0/16B3A00", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3600",
        confirmed_flush_lsn="0/16B3800",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    res = OffsetReconciliation.diagnose(offset, slot)
    assert res.status == "already_applied_ahead"
    assert res.is_safe()
    assert not res.needs_reseed()
    assert res.certificate is not None
    assert res.certificate.certified
    assert res.certificate.status == "fast_forward_safe"
    assert res.certificate.resume_lsn == parse_lsn("0/16B3A00")
    assert res.reseed_requirement is None

    applied = res.applied_intervals()
    assert len(applied) >= 1
    assert any(i.start_lsn == parse_lsn("0/16B3800") and i.end_lsn == parse_lsn("0/16B3A00") for i in applied)

    retained = res.retained_intervals()
    assert any(i.start_lsn == parse_lsn("0/16B3A00") and i.end_lsn == parse_lsn("0/16B4000") for i in retained)


def test_offset_behind_slot_missing_log_without_evidence_rejects_naive_and_requires_reseed():
    offset = OffsetState(offset_lsn="0/16B3400", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3800",
        confirmed_flush_lsn="0/16B3800",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    res = OffsetReconciliation(offset, slot, evidence=[]).reconcile()
    assert res.status == "bounded_reseed_required"
    assert not res.is_safe()
    assert res.needs_reseed()
    assert res.certificate is None

    assert res.naive_verdict is not None
    assert res.naive_verdict.rejected
    assert "silently drops" in res.naive_verdict.rejection_reason or "missing" in res.naive_verdict.rejection_reason

    assert res.reseed_requirement is not None
    assert res.reseed_requirement.required
    assert res.reseed_requirement.divergence_type == "missing_log_gap"
    assert res.reseed_requirement.lower_bound_lsn == parse_lsn("0/16B3400")
    assert res.reseed_requirement.upper_bound_lsn == parse_lsn("0/16B3800")

    missing = res.missing_intervals()
    assert len(missing) == 1
    assert not missing[0].reconstructable
    assert missing[0].start_lsn == parse_lsn("0/16B3400")
    assert missing[0].end_lsn == parse_lsn("0/16B3800")


def test_offset_behind_slot_missing_log_with_archive_produces_reconstruction_certificate():
    offset = OffsetState(offset_lsn="0/16B3400", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3800",
        confirmed_flush_lsn="0/16B3800",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    evidence = [
        LogEvidence(
            source_name="wal_archive",
            min_lsn="0/16B3200",
            max_lsn="0/16B3900",
            timeline_id=1,
            continuous=True,
        )
    ]
    res = OffsetReconciliation(offset, slot, evidence=evidence).reconcile()
    assert res.status == "reconstructed_recovery"
    assert res.is_safe()
    assert not res.needs_reseed()
    assert res.reseed_requirement is None

    assert res.naive_verdict is not None
    assert res.naive_verdict.rejected

    assert res.certificate is not None
    assert res.certificate.certified
    assert res.certificate.status == "certified_reconstruction"
    assert res.certificate.resume_lsn == parse_lsn("0/16B3400")
    assert "wal_archive" in res.certificate.evidence_sources

    missing = res.missing_intervals()
    assert len(missing) == 1
    assert missing[0].reconstructable
    assert missing[0].evidence_source == "wal_archive"


def test_offset_behind_slot_with_partial_evidence_requires_bounded_reseed():
    offset = OffsetState(offset_lsn="0/16B3000", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3800",
        confirmed_flush_lsn="0/16B3800",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    evidence = [
        LogEvidence(
            source_name="ring_buffer",
            min_lsn="0/16B3400",
            max_lsn="0/16B3800",
            timeline_id=1,
            continuous=True,
        )
    ]
    res = OffsetReconciliation(offset, slot, evidence=evidence).reconcile()
    assert res.status == "bounded_reseed_required"
    assert res.needs_reseed()
    assert res.reseed_requirement is not None
    assert res.reseed_requirement.lower_bound_lsn == parse_lsn("0/16B3000")
    assert res.reseed_requirement.upper_bound_lsn == parse_lsn("0/16B3400")

    missing = res.missing_intervals()
    assert any(not i.reconstructable for i in missing)
    assert any(i.reconstructable for i in missing)


def test_offset_behind_confirmed_flush_but_within_retained_wal():
    offset = OffsetState(offset_lsn="0/16B3400", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3200",
        confirmed_flush_lsn="0/16B3800",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    res = OffsetReconciliation(offset, slot).reconcile()
    assert res.status == "within_retained_slot"
    assert res.is_safe()
    assert not res.needs_reseed()
    assert res.naive_verdict is not None
    assert res.naive_verdict.rejected
    assert res.certificate is not None
    assert res.certificate.certified
    assert res.certificate.resume_lsn == parse_lsn("0/16B3400")


def test_failover_timeline_divergence_offset_before_split():
    offset = OffsetState(offset_lsn="0/16B3400", timeline_id=1, system_id="pg-cluster")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3200",
        confirmed_flush_lsn="0/16B3200",
        current_wal_lsn="0/16B4000",
        timeline_id=2,
        system_id="pg-cluster",
    )
    res = OffsetReconciliation(
        offset,
        slot,
        failover_divergence_lsn="0/16B3600",
    ).reconcile()
    assert res.is_safe()
    assert not res.needs_reseed()
    assert res.certificate is not None
    assert res.certificate.certified
    assert res.certificate.resume_lsn == parse_lsn("0/16B3400")


def test_failover_timeline_divergence_offset_past_split_rejects_naive_and_requires_reseed():
    offset = OffsetState(offset_lsn="0/16B3800", timeline_id=1, system_id="pg-cluster")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3600",
        confirmed_flush_lsn="0/16B3600",
        current_wal_lsn="0/16B4000",
        timeline_id=2,
        system_id="pg-cluster",
    )
    res = OffsetReconciliation(
        offset,
        slot,
        failover_divergence_lsn="0/16B3600",
    ).reconcile()
    assert res.status == "failover_divergence"
    assert not res.is_safe()
    assert res.needs_reseed()
    assert res.naive_verdict is not None
    assert res.naive_verdict.rejected
    assert res.reseed_requirement is not None
    assert res.reseed_requirement.divergence_type == "timeline_divergence"
    assert res.reseed_requirement.lower_bound_lsn == parse_lsn("0/16B3600")
    assert res.reseed_requirement.upper_bound_lsn == parse_lsn("0/16B3800")

    unknowns = res.unknown_intervals()
    assert len(unknowns) >= 1
    assert any(i.start_lsn == parse_lsn("0/16B3600") and i.end_lsn == parse_lsn("0/16B3800") for i in unknowns)


def test_failover_system_id_mismatch():
    offset = OffsetState(offset_lsn="0/16B3400", system_id="cluster-primary-A")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3400",
        system_id="cluster-primary-B",
    )
    res = OffsetReconciliation(offset, slot).reconcile()
    assert res.status == "system_id_mismatch"
    assert res.needs_reseed()
    assert res.naive_verdict is not None
    assert res.naive_verdict.rejected
    assert res.reseed_requirement is not None
    assert res.reseed_requirement.divergence_type == "system_id_mismatch"


def test_stale_offset_store_backup_without_archive():
    offset = OffsetState(
        offset_lsn="0/16B3100",
        timeline_id=1,
        system_id="pg-prod-1",
        is_restored_backup=True,
    )
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3800",
        confirmed_flush_lsn="0/16B3800",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    res = OffsetReconciliation(offset, slot).reconcile()
    assert res.status == "stale_backup_divergence"
    assert res.needs_reseed()
    assert res.naive_verdict.rejected
    assert res.reseed_requirement.divergence_type == "stale_backup"
    assert res.reseed_requirement.lower_bound_lsn == parse_lsn("0/16B3100")
    assert res.reseed_requirement.upper_bound_lsn == parse_lsn("0/16B3800")


def test_stale_source_rollback_behind_offset():
    offset = OffsetState(offset_lsn="0/16B4500", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3000",
        confirmed_flush_lsn="0/16B3500",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    res = OffsetReconciliation(offset, slot).reconcile()
    assert res.status == "source_rollback_detected"
    assert res.needs_reseed()
    assert res.reseed_requirement.divergence_type == "source_rollback"
    assert res.unknown_intervals()


def test_wal_status_lost_triggers_reseed_unless_archived():
    offset = OffsetState(offset_lsn="0/16B3500", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3500",
        confirmed_flush_lsn="0/16B3500",
        current_wal_lsn="0/16B4000",
        wal_status="lost",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    res_no_arch = OffsetReconciliation(offset, slot).reconcile()
    assert res_no_arch.needs_reseed()
    assert res_no_arch.reseed_requirement.divergence_type == "wal_lost"

    evidence = [
        LogEvidence(
            source_name="wal_archive",
            min_lsn="0/16B3500",
            max_lsn="0/16B4000",
            timeline_id=1,
        )
    ]
    res_with_arch = OffsetReconciliation(offset, slot, evidence=evidence).reconcile()
    assert res_with_arch.is_safe()
    assert res_with_arch.certificate.certified


def test_multiple_evidence_chaining():
    offset = OffsetState(offset_lsn="0/16B3000", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3800",
        confirmed_flush_lsn="0/16B3800",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    evidence = [
        LogEvidence(
            source_name="ring_buffer",
            min_lsn="0/16B3000",
            max_lsn="0/16B3400",
            timeline_id=1,
        ),
        LogEvidence(
            source_name="wal_archive",
            min_lsn="0/16B3400",
            max_lsn="0/16B3800",
            timeline_id=1,
        ),
    ]
    res = OffsetReconciliation(offset, slot, evidence=evidence).reconcile()
    assert res.is_safe()
    assert res.certificate.certified
    assert "ring_buffer" in res.certificate.evidence_sources
    assert "wal_archive" in res.certificate.evidence_sources
    assert len(res.certificate.replay_plan) >= 2


def test_dict_input_and_serialization():
    offset_dict = {
        "offset_lsn": "0/16B3600",
        "timeline_id": 1,
        "system_id": "sys-1",
    }
    slot_dict = {
        "slot_name": "slot1",
        "restart_lsn": "0/16B3600",
        "confirmed_flush_lsn": "0/16B3600",
        "current_wal_lsn": "0/16B4000",
        "timeline_id": 1,
        "system_id": "sys-1",
    }
    res = OffsetReconciliation.diagnose(offset_dict, slot_dict)
    assert res.is_safe()
    d = res.to_dict()
    assert d["status"] == "in_sync"
    assert "summary" in d
    assert "intervals" in d
    assert "OFFSET_DIAG" in res.summary


def test_convenience_methods():
    offset = OffsetState(offset_lsn="0/16B3400", timeline_id=1, system_id="pg-prod-1")
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3800",
        confirmed_flush_lsn="0/16B3800",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    recon = OffsetReconciliation(offset, slot)
    intervals = recon.classify_intervals()
    assert len(intervals) >= 1
    naive = recon.evaluate_naive_rule()
    assert naive is not None
    assert naive.rejected
    cert = recon.produce_certificate()
    assert cert is None
    reseed = recon.produce_reseed_requirement()
    assert reseed is not None
    assert reseed.required


def test_stale_target_watermark_backup():
    offset = OffsetState(
        offset_lsn="0/16B3A00",
        timeline_id=1,
        system_id="pg-prod-1",
        target_watermark_lsn="0/16B3400",
    )
    slot = SlotState(
        slot_name="migkit_cdc",
        restart_lsn="0/16B3400",
        confirmed_flush_lsn="0/16B3A00",
        current_wal_lsn="0/16B4000",
        timeline_id=1,
        system_id="pg-prod-1",
    )
    res = OffsetReconciliation(offset, slot).reconcile()
    assert res.status == "stale_target_backup"
    assert res.needs_reseed()
    assert res.reseed_requirement.divergence_type == "stale_target_backup"
    assert res.reseed_requirement.lower_bound_lsn == parse_lsn("0/16B3400")
    assert res.reseed_requirement.upper_bound_lsn == parse_lsn("0/16B3A00")


def test_multiple_interleaved_gaps_in_evidence():
    offset = OffsetState(offset_lsn=100, timeline_id=1)
    slot = SlotState(
        slot_name="s",
        restart_lsn=500,
        confirmed_flush_lsn=500,
        current_wal_lsn=600,
        timeline_id=1,
    )
    evidence = [
        LogEvidence(source_name="ev1", min_lsn=150, max_lsn=200, timeline_id=1),
        LogEvidence(source_name="ev2", min_lsn=300, max_lsn=350, timeline_id=1),
    ]
    res = OffsetReconciliation(offset, slot, evidence=evidence).reconcile()
    assert res.status == "bounded_reseed_required"
    assert res.needs_reseed()
    missing = res.missing_intervals()
    assert len(missing) == 5
    assert not missing[0].reconstructable and missing[0].start_lsn == 100 and missing[0].end_lsn == 150
    assert missing[1].reconstructable and missing[1].start_lsn == 150 and missing[1].end_lsn == 200
    assert not missing[2].reconstructable and missing[2].start_lsn == 200 and missing[2].end_lsn == 300
    assert missing[3].reconstructable and missing[3].start_lsn == 300 and missing[3].end_lsn == 350
    assert not missing[4].reconstructable and missing[4].start_lsn == 350 and missing[4].end_lsn == 500


def test_evidence_with_wrong_timeline_or_non_continuous():
    offset = OffsetState(offset_lsn=100, timeline_id=2)
    slot = SlotState(
        slot_name="s",
        restart_lsn=300,
        confirmed_flush_lsn=300,
        timeline_id=2,
    )
    evidence = [
        LogEvidence(source_name="arch_t1", min_lsn=100, max_lsn=300, timeline_id=1),
        LogEvidence(source_name="broken_t2", min_lsn=100, max_lsn=300, timeline_id=2, continuous=False),
    ]
    res = OffsetReconciliation(offset, slot, evidence=evidence).reconcile()
    assert res.status == "bounded_reseed_required"
    assert res.needs_reseed()


def test_suggested_scope_propagation():
    offset = OffsetState(offset_lsn="0/16B3000", timeline_id=1)
    slot = SlotState(
        slot_name="s",
        restart_lsn="0/16B3500",
        timeline_id=1,
    )
    custom_scope = ["public.users", "public.accounts"]
    res = OffsetReconciliation(offset, slot, suggested_scope=custom_scope).reconcile()
    assert res.needs_reseed()
    assert res.reseed_requirement.suggested_scope == custom_scope


def test_interval_and_dataclass_properties():
    iv = Interval(kind=INTERVAL_MISSING_LOG, start_lsn=0x16B3400, end_lsn=0x16B3800)
    assert iv.start_lsn_formatted == "0/16B3400"
    assert iv.end_lsn_formatted == "0/16B3800"
    assert iv.length == 0x400

    cert = ReconstructionCertificate(
        certified=True,
        status="certified",
        resume_lsn=0x16B3400,
    )
    assert cert.resume_lsn_formatted == "0/16B3400"
    cd = cert.to_dict()
    assert cd["certified"]
    assert cd["resume_lsn"] == 0x16B3400

    reseed = BoundedReseedRequirement(
        required=True,
        divergence_type="missing_log_gap",
        reason="gap",
        lower_bound_lsn=0x16B3400,
        upper_bound_lsn=0x16B3800,
    )
    assert reseed.lower_bound_lsn_formatted == "0/16B3400"
    assert reseed.upper_bound_lsn_formatted == "0/16B3800"
    rd = reseed.to_dict()
    assert rd["required"]
    assert rd["lower_bound_lsn_formatted"] == "0/16B3400"

    naive = NaiveRuleVerdict(naive_lsn=0x16B3800, rejected=True, rejection_reason="skip")
    assert naive.naive_lsn_formatted == "0/16B3800"
    nd = naive.to_dict()
    assert nd["rejected"]


def test_offset_reconciliation_rungs_under_watched():
    with watched() as seen:
        # 1. resume_from_consistent_offset holds
        f1 = {
            "offset_matches_slot": True,
            "timeline_and_system_match": True,
            "reconstruction_certified": True,
        }
        c1 = decide_offset_reconciliation(f1)
        assert c1.rung.name == "resume_from_consistent_offset"

        # 2. resume_from_consistent_offset refuses, fast_forward_safe_reconstructed holds
        f2 = {
            "offset_matches_slot": False,
            "timeline_and_system_match": True,
            "reconstruction_certified": True,
        }
        c2 = decide_offset_reconciliation(f2)
        assert c2.rung.name == "fast_forward_safe_reconstructed"

        # 3. fast_forward_safe_reconstructed refuses, bounded_reseed_and_refuse holds
        f3 = {
            "offset_matches_slot": False,
            "timeline_and_system_match": True,
            "reconstruction_certified": False,
        }
        c3 = decide_offset_reconciliation(f3)
        assert c3.rung.name == "bounded_reseed_and_refuse"

