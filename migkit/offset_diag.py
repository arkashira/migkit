import dataclasses
from typing import Any, Dict, List, Optional, Tuple, Union

from . import decide


def offset_reconciliation_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for diagnosing offset-store vs slot disagreement (Row 654: NEED06)."""
    return (
        decide.Rung(
            "resume_from_consistent_offset",
            "resume directly when offset store and slot LSN are in exact consistent agreement",
            needs=(
                decide.Need(
                    "offset and slot confirmed LSN are identical",
                    lambda f: bool(f.get("offset_matches_slot", True)),
                    "offset store position differs from replication slot position",
                ),
                decide.Need(
                    "timeline and system identity match source",
                    lambda f: bool(f.get("timeline_and_system_match", True)),
                    "timeline or system identity has diverged from source",
                ),
            ),
            footprint=0,
            leaves="direct resume from verified consistent offset",
        ),
        decide.Rung(
            "fast_forward_safe_reconstructed",
            "fast-forward safely when reconstruction certificate proves all intermediate writes applied",
            needs=(
                decide.Need(
                    "timeline and system identity match source",
                    lambda f: bool(f.get("timeline_and_system_match", True)),
                    "timeline or system identity has diverged from source",
                ),
                decide.Need(
                    "reconstruction certificate proves safe fast-forward without missing writes",
                    lambda f: bool(f.get("reconstruction_certified", True)),
                    "reconstruction certificate missing or failed to prove gap safety",
                ),
            ),
            footprint=1,
            leaves="safe fast-forward resume with verified reconstruction certificate",
        ),
        decide.Rung(
            "bounded_reseed_and_refuse",
            "refuse silent skipping and demand bounded reseed when offset disagreement cannot be reconstructed",
            footprint=2,
            leaves="blocked resume with explicit bounded reseed requirement",
        ),
    )


def decide_offset_reconciliation(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the offset_reconciliation_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("offset disagreement reconciliation", offset_reconciliation_rungs(), fact_obj)


INTERVAL_MISSING_LOG = "missing-log"
INTERVAL_ALREADY_RETAINED = "already-retained"
INTERVAL_ALREADY_APPLIED = "already-applied"
INTERVAL_UNKNOWN = "unknown"


def parse_lsn(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        if "/" in val:
            parts = val.split("/")
            if len(parts) == 2:
                try:
                    return (int(parts[0], 16) << 32) + int(parts[1], 16)
                except ValueError:
                    pass
        if val.startswith(("0x", "0X")):
            try:
                return int(val, 16)
            except ValueError:
                pass
        try:
            return int(val)
        except ValueError:
            pass
    raise ValueError(f"invalid LSN format: {val!r}")


def format_lsn(val: Optional[int]) -> Optional[str]:
    if val is None:
        return None
    return f"{val >> 32:X}/{val & 0xFFFFFFFF:X}"


@dataclasses.dataclass(frozen=True)
class Interval:
    kind: str
    start_lsn: int
    end_lsn: int
    reconstructable: bool = False
    detail: str = ""
    evidence_source: Optional[str] = None

    @property
    def start_lsn_formatted(self) -> str:
        return format_lsn(self.start_lsn) or "0/0"

    @property
    def end_lsn_formatted(self) -> str:
        return format_lsn(self.end_lsn) or "0/0"

    @property
    def length(self) -> int:
        return max(0, self.end_lsn - self.start_lsn)


@dataclasses.dataclass
class SlotState:
    slot_name: str
    restart_lsn: Union[int, str]
    confirmed_flush_lsn: Optional[Union[int, str]] = None
    current_wal_lsn: Optional[Union[int, str]] = None
    timeline_id: int = 1
    system_id: Optional[str] = None
    wal_status: str = "normal"
    active: bool = False
    plugin: str = "pgoutput"


@dataclasses.dataclass
class OffsetState:
    offset_lsn: Union[int, str]
    timeline_id: int = 1
    system_id: Optional[str] = None
    applied_lsn: Optional[Union[int, str]] = None
    source_uri: Optional[str] = None
    backup_timestamp: Optional[float] = None
    is_restored_backup: bool = False
    target_watermark_lsn: Optional[Union[int, str]] = None


@dataclasses.dataclass
class LogEvidence:
    source_name: str
    min_lsn: Union[int, str]
    max_lsn: Union[int, str]
    timeline_id: int = 1
    continuous: bool = True
    segments: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class NaiveRuleVerdict:
    naive_lsn: int
    rejected: bool
    rejection_reason: str

    @property
    def naive_lsn_formatted(self) -> str:
        return format_lsn(self.naive_lsn) or "0/0"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "naive_lsn": self.naive_lsn,
            "naive_lsn_formatted": self.naive_lsn_formatted,
            "rejected": self.rejected,
            "rejection_reason": self.rejection_reason,
        }


@dataclasses.dataclass
class ReconstructionCertificate:
    certified: bool
    status: str
    resume_lsn: int
    reconstructed_intervals: List[Interval] = dataclasses.field(default_factory=list)
    retained_intervals: List[Interval] = dataclasses.field(default_factory=list)
    applied_intervals: List[Interval] = dataclasses.field(default_factory=list)
    evidence_sources: List[str] = dataclasses.field(default_factory=list)
    replay_plan: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    proof: str = ""

    @property
    def resume_lsn_formatted(self) -> str:
        return format_lsn(self.resume_lsn) or "0/0"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "certified": self.certified,
            "status": self.status,
            "resume_lsn": self.resume_lsn,
            "resume_lsn_formatted": self.resume_lsn_formatted,
            "reconstructed_intervals": [dataclasses.asdict(i) for i in self.reconstructed_intervals],
            "retained_intervals": [dataclasses.asdict(i) for i in self.retained_intervals],
            "applied_intervals": [dataclasses.asdict(i) for i in self.applied_intervals],
            "evidence_sources": self.evidence_sources,
            "replay_plan": self.replay_plan,
            "proof": self.proof,
        }


@dataclasses.dataclass
class BoundedReseedRequirement:
    required: bool
    divergence_type: str
    reason: str
    missing_intervals: List[Interval] = dataclasses.field(default_factory=list)
    lower_bound_lsn: Optional[int] = None
    upper_bound_lsn: Optional[int] = None
    suggested_scope: List[str] = dataclasses.field(default_factory=lambda: ["*"])
    proof: str = ""

    @property
    def lower_bound_lsn_formatted(self) -> Optional[str]:
        return format_lsn(self.lower_bound_lsn) if self.lower_bound_lsn is not None else None

    @property
    def upper_bound_lsn_formatted(self) -> Optional[str]:
        return format_lsn(self.upper_bound_lsn) if self.upper_bound_lsn is not None else None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "required": self.required,
            "divergence_type": self.divergence_type,
            "reason": self.reason,
            "missing_intervals": [dataclasses.asdict(i) for i in self.missing_intervals],
            "lower_bound_lsn": self.lower_bound_lsn,
            "lower_bound_lsn_formatted": self.lower_bound_lsn_formatted,
            "upper_bound_lsn": self.upper_bound_lsn,
            "upper_bound_lsn_formatted": self.upper_bound_lsn_formatted,
            "suggested_scope": self.suggested_scope,
            "proof": self.proof,
        }


@dataclasses.dataclass
class ReconciliationResult:
    status: str
    offset_lsn: int
    slot_restart_lsn: int
    slot_confirmed_flush_lsn: int
    intervals: List[Interval] = dataclasses.field(default_factory=list)
    certificate: Optional[ReconstructionCertificate] = None
    reseed_requirement: Optional[BoundedReseedRequirement] = None
    naive_verdict: Optional[NaiveRuleVerdict] = None
    summary: str = ""

    def is_safe(self) -> bool:
        return self.certificate is not None and self.certificate.certified and self.reseed_requirement is None

    def needs_reseed(self) -> bool:
        return self.reseed_requirement is not None and self.reseed_requirement.required

    def can_reconstruct(self) -> bool:
        return self.certificate is not None and self.certificate.certified

    def missing_intervals(self) -> List[Interval]:
        return [i for i in self.intervals if i.kind == INTERVAL_MISSING_LOG]

    def retained_intervals(self) -> List[Interval]:
        return [i for i in self.intervals if i.kind == INTERVAL_ALREADY_RETAINED]

    def applied_intervals(self) -> List[Interval]:
        return [i for i in self.intervals if i.kind == INTERVAL_ALREADY_APPLIED]

    def unknown_intervals(self) -> List[Interval]:
        return [i for i in self.intervals if i.kind == INTERVAL_UNKNOWN]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "offset_lsn": self.offset_lsn,
            "offset_lsn_formatted": format_lsn(self.offset_lsn),
            "slot_restart_lsn": self.slot_restart_lsn,
            "slot_restart_lsn_formatted": format_lsn(self.slot_restart_lsn),
            "slot_confirmed_flush_lsn": self.slot_confirmed_flush_lsn,
            "slot_confirmed_flush_lsn_formatted": format_lsn(self.slot_confirmed_flush_lsn),
            "intervals": [dataclasses.asdict(i) for i in self.intervals],
            "certificate": self.certificate.to_dict() if self.certificate else None,
            "reseed_requirement": self.reseed_requirement.to_dict() if self.reseed_requirement else None,
            "naive_verdict": self.naive_verdict.to_dict() if self.naive_verdict else None,
            "summary": self.summary,
        }


class OffsetReconciliation:
    def __init__(
        self,
        offset: Union[OffsetState, Dict[str, Any]],
        slot: Union[SlotState, Dict[str, Any]],
        evidence: Optional[List[Union[LogEvidence, Dict[str, Any]]]] = None,
        failover_divergence_lsn: Optional[Union[int, str]] = None,
        suggested_scope: Optional[List[str]] = None,
    ):
        if isinstance(offset, dict):
            self.offset = OffsetState(**offset)
        else:
            self.offset = offset

        if isinstance(slot, dict):
            self.slot = SlotState(**slot)
        else:
            self.slot = slot

        norm_evidence: List[LogEvidence] = []
        if evidence:
            for ev in evidence:
                if isinstance(ev, dict):
                    norm_evidence.append(LogEvidence(**ev))
                else:
                    norm_evidence.append(ev)
        self.evidence = norm_evidence
        self.failover_divergence_lsn = parse_lsn(failover_divergence_lsn)
        self.suggested_scope = suggested_scope or ["*"]

    @classmethod
    def diagnose(
        cls,
        offset: Union[OffsetState, Dict[str, Any]],
        slot: Union[SlotState, Dict[str, Any]],
        evidence: Optional[List[Union[LogEvidence, Dict[str, Any]]]] = None,
        failover_divergence_lsn: Optional[Union[int, str]] = None,
        suggested_scope: Optional[List[str]] = None,
    ) -> ReconciliationResult:
        recon = cls(
            offset=offset,
            slot=slot,
            evidence=evidence,
            failover_divergence_lsn=failover_divergence_lsn,
            suggested_scope=suggested_scope,
        )
        return recon.reconcile()

    def classify_intervals(self) -> List[Interval]:
        return self.reconcile().intervals

    def evaluate_naive_rule(self) -> Optional[NaiveRuleVerdict]:
        return self.reconcile().naive_verdict

    def produce_certificate(self) -> Optional[ReconstructionCertificate]:
        return self.reconcile().certificate

    def produce_reseed_requirement(self) -> Optional[BoundedReseedRequirement]:
        return self.reconcile().reseed_requirement

    def _slice_missing_evidence(
        self, start: int, end: int, timeline_id: int
    ) -> Tuple[List[Interval], List[str]]:
        if start >= end:
            return [], []

        ev_ranges: List[Tuple[int, int, str]] = []
        for ev in self.evidence:
            if not ev.continuous:
                continue
            if ev.timeline_id != timeline_id:
                continue
            e_min = parse_lsn(ev.min_lsn)
            e_max = parse_lsn(ev.max_lsn)
            if e_min is None or e_max is None or e_min >= e_max:
                continue
            if e_max <= start or e_min >= end:
                continue
            ev_ranges.append((max(start, e_min), min(end, e_max), ev.source_name))

        ev_ranges.sort(key=lambda x: (x[0], x[1]))

        merged: List[Tuple[int, int, List[str]]] = []
        for s, e, src in ev_ranges:
            if not merged:
                merged.append((s, e, [src]))
            else:
                last_s, last_e, srcs = merged[-1]
                if s <= last_e:
                    new_e = max(last_e, e)
                    new_srcs = srcs if src in srcs else srcs + [src]
                    merged[-1] = (last_s, new_e, new_srcs)
                else:
                    merged.append((s, e, [src]))

        intervals: List[Interval] = []
        sources_used: List[str] = []
        cursor = start

        for m_start, m_end, srcs in merged:
            if cursor < m_start:
                intervals.append(
                    Interval(
                        kind=INTERVAL_MISSING_LOG,
                        start_lsn=cursor,
                        end_lsn=m_start,
                        reconstructable=False,
                        detail="uncovered gap in secondary evidence",
                    )
                )
            for src in srcs:
                if src not in sources_used:
                    sources_used.append(src)
            intervals.append(
                Interval(
                    kind=INTERVAL_MISSING_LOG,
                    start_lsn=m_start,
                    end_lsn=m_end,
                    reconstructable=True,
                    evidence_source=",".join(srcs),
                    detail=f"bridged by evidence: {','.join(srcs)}",
                )
            )
            cursor = m_end

        if cursor < end:
            intervals.append(
                Interval(
                    kind=INTERVAL_MISSING_LOG,
                    start_lsn=cursor,
                    end_lsn=end,
                    reconstructable=False,
                    detail="uncovered gap in secondary evidence",
                )
            )

        return intervals, sources_used

    def _finish_result(self, res: ReconciliationResult) -> ReconciliationResult:
        facts = {
            "offset_matches_slot": bool(res.offset_lsn == res.slot_confirmed_flush_lsn or res.offset_lsn == res.slot_restart_lsn),
            "timeline_and_system_match": bool(
                self.offset.timeline_id == self.slot.timeline_id
                and (not self.offset.system_id or not self.slot.system_id or self.offset.system_id == self.slot.system_id)
            ),
            "reconstruction_certified": bool(res.certificate and res.certificate.certified),
        }
        decide_offset_reconciliation(facts)
        return res

    def reconcile(self) -> ReconciliationResult:
        res = self._do_reconcile()
        return self._finish_result(res)

    def _do_reconcile(self) -> ReconciliationResult:
        offset_lsn = parse_lsn(self.offset.offset_lsn)
        slot_restart = parse_lsn(self.slot.restart_lsn)
        slot_flush = parse_lsn(self.slot.confirmed_flush_lsn)
        if slot_flush is None:
            slot_flush = slot_restart
        current_wal = parse_lsn(self.slot.current_wal_lsn)
        if current_wal is None:
            current_wal = max(offset_lsn or 0, slot_restart or 0, slot_flush or 0)

        if offset_lsn is None or slot_restart is None or slot_flush is None:
            raise ValueError("offset_lsn and slot restart_lsn must be non-null")

        naive_lsn = max(offset_lsn, slot_flush)

        # 1. System ID mismatch
        if (
            self.offset.system_id
            and self.slot.system_id
            and self.offset.system_id != self.slot.system_id
        ):
            unknown_iv = Interval(
                kind=INTERVAL_UNKNOWN,
                start_lsn=min(offset_lsn, slot_restart),
                end_lsn=max(offset_lsn, current_wal),
                detail=f"system identifier mismatch: offset({self.offset.system_id}) != slot({self.slot.system_id})",
            )
            naive = NaiveRuleVerdict(
                naive_lsn=naive_lsn,
                rejected=True,
                rejection_reason=f"system identifier mismatch: {self.offset.system_id} != {self.slot.system_id}",
            )
            reseed = BoundedReseedRequirement(
                required=True,
                divergence_type="system_id_mismatch",
                reason=f"cluster identity mismatch: offset ({self.offset.system_id}) vs slot ({self.slot.system_id})",
                missing_intervals=[unknown_iv],
                lower_bound_lsn=min(offset_lsn, slot_restart),
                upper_bound_lsn=max(offset_lsn, current_wal),
                suggested_scope=self.suggested_scope,
                proof="disjoint cluster system identifiers preclude interval continuation",
            )
            summary = (
                f"OFFSET_DIAG status=system_id_mismatch offset={format_lsn(offset_lsn)} "
                f"slot={format_lsn(slot_restart)} naive_rule=rejected reseed=required"
            )
            return ReconciliationResult(
                status="system_id_mismatch",
                offset_lsn=offset_lsn,
                slot_restart_lsn=slot_restart,
                slot_confirmed_flush_lsn=slot_flush,
                intervals=[unknown_iv],
                reseed_requirement=reseed,
                naive_verdict=naive,
                summary=summary,
            )

        # 2. Source rollback behind offset
        if current_wal < offset_lsn:
            unknown_iv = Interval(
                kind=INTERVAL_UNKNOWN,
                start_lsn=current_wal,
                end_lsn=offset_lsn,
                detail=f"source current WAL ({format_lsn(current_wal)}) is behind offset ({format_lsn(offset_lsn)})",
            )
            naive = NaiveRuleVerdict(
                naive_lsn=naive_lsn,
                rejected=True,
                rejection_reason="source current WAL is behind offset store; possible source rollback",
            )
            reseed = BoundedReseedRequirement(
                required=True,
                divergence_type="source_rollback",
                reason="source WAL truncated or restored to point before consumer offset",
                missing_intervals=[unknown_iv],
                lower_bound_lsn=current_wal,
                upper_bound_lsn=offset_lsn,
                suggested_scope=self.suggested_scope,
                proof="source cluster WAL head is chronologically behind target applied offset",
            )
            summary = (
                f"OFFSET_DIAG status=source_rollback_detected offset={format_lsn(offset_lsn)} "
                f"source_wal={format_lsn(current_wal)} naive_rule=rejected reseed=required"
            )
            return ReconciliationResult(
                status="source_rollback_detected",
                offset_lsn=offset_lsn,
                slot_restart_lsn=slot_restart,
                slot_confirmed_flush_lsn=slot_flush,
                intervals=[unknown_iv],
                reseed_requirement=reseed,
                naive_verdict=naive,
                summary=summary,
            )

        # 2b. Target restore / rollback behind offset
        target_wm = parse_lsn(self.offset.target_watermark_lsn)
        if target_wm is not None and target_wm < offset_lsn:
            unknown_iv = Interval(
                kind=INTERVAL_UNKNOWN,
                start_lsn=target_wm,
                end_lsn=offset_lsn,
                detail=f"target watermark ({format_lsn(target_wm)}) is behind offset store ({format_lsn(offset_lsn)})",
            )
            naive = NaiveRuleVerdict(
                naive_lsn=naive_lsn,
                rejected=True,
                rejection_reason="target database watermark is behind offset store; target restore/rollback detected",
            )
            reseed = BoundedReseedRequirement(
                required=True,
                divergence_type="stale_target_backup",
                reason="target database restored to point prior to offset store state",
                missing_intervals=[unknown_iv],
                lower_bound_lsn=target_wm,
                upper_bound_lsn=offset_lsn,
                suggested_scope=self.suggested_scope,
                proof="target watermark lags durable offset store watermark",
            )
            summary = (
                f"OFFSET_DIAG status=stale_target_backup offset={format_lsn(offset_lsn)} "
                f"target_wm={format_lsn(target_wm)} naive_rule=rejected reseed=required"
            )
            return ReconciliationResult(
                status="stale_target_backup",
                offset_lsn=offset_lsn,
                slot_restart_lsn=slot_restart,
                slot_confirmed_flush_lsn=slot_flush,
                intervals=[unknown_iv],
                reseed_requirement=reseed,
                naive_verdict=naive,
                summary=summary,
            )

        # 3. Failover / timeline divergence
        if (
            self.slot.timeline_id != self.offset.timeline_id
            or self.failover_divergence_lsn is not None
        ):
            div_lsn = (
                self.failover_divergence_lsn
                if self.failover_divergence_lsn is not None
                else slot_restart
            )
            if offset_lsn > div_lsn:
                unknown_iv = Interval(
                    kind=INTERVAL_UNKNOWN,
                    start_lsn=div_lsn,
                    end_lsn=offset_lsn,
                    detail=f"transactions applied on timeline {self.offset.timeline_id} past divergence point {format_lsn(div_lsn)}",
                )
                naive = NaiveRuleVerdict(
                    naive_lsn=naive_lsn,
                    rejected=True,
                    rejection_reason=f"naive choose-larger-LSN ignores timeline divergence at {format_lsn(div_lsn)}",
                )
                reseed = BoundedReseedRequirement(
                    required=True,
                    divergence_type="timeline_divergence",
                    reason=f"consumer applied phantom transactions past failover divergence LSN {format_lsn(div_lsn)}",
                    missing_intervals=[unknown_iv],
                    lower_bound_lsn=div_lsn,
                    upper_bound_lsn=offset_lsn,
                    suggested_scope=self.suggested_scope,
                    proof="timeline split created divergent history discarded by promoted standby",
                )
                summary = (
                    f"OFFSET_DIAG status=failover_divergence offset={format_lsn(offset_lsn)} "
                    f"divergence={format_lsn(div_lsn)} naive_rule=rejected reseed=required"
                )
                return ReconciliationResult(
                    status="failover_divergence",
                    offset_lsn=offset_lsn,
                    slot_restart_lsn=slot_restart,
                    slot_confirmed_flush_lsn=slot_flush,
                    intervals=[unknown_iv],
                    reseed_requirement=reseed,
                    naive_verdict=naive,
                    summary=summary,
                )

        # 4. Stale restored backup offset store
        if self.offset.is_restored_backup and offset_lsn < slot_restart:
            missing_ivs, used_sources = self._slice_missing_evidence(
                offset_lsn, slot_restart, self.slot.timeline_id
            )
            has_gap = any(not i.reconstructable for i in missing_ivs)
            if has_gap or not missing_ivs:
                uncovered = [i for i in missing_ivs if not i.reconstructable]
                min_uncovered = uncovered[0].start_lsn if uncovered else offset_lsn
                max_uncovered = uncovered[-1].end_lsn if uncovered else slot_restart
                naive = NaiveRuleVerdict(
                    naive_lsn=naive_lsn,
                    rejected=True,
                    rejection_reason="stale backup offset store has purged WAL interval lacking continuous archive",
                )
                reseed = BoundedReseedRequirement(
                    required=True,
                    divergence_type="stale_backup",
                    reason="offset store restored from stale backup with unrecoverable log gap",
                    missing_intervals=missing_ivs
                    or [
                        Interval(
                            kind=INTERVAL_MISSING_LOG,
                            start_lsn=offset_lsn,
                            end_lsn=slot_restart,
                            reconstructable=False,
                            detail="stale backup log gap",
                        )
                    ],
                    lower_bound_lsn=min_uncovered,
                    upper_bound_lsn=max_uncovered,
                    suggested_scope=self.suggested_scope,
                    proof="stale backup restore leaves unbridgeable gap to slot restart position",
                )
                summary = (
                    f"OFFSET_DIAG status=stale_backup_divergence offset={format_lsn(offset_lsn)} "
                    f"slot_restart={format_lsn(slot_restart)} naive_rule=rejected reseed=required"
                )
                return ReconciliationResult(
                    status="stale_backup_divergence",
                    offset_lsn=offset_lsn,
                    slot_restart_lsn=slot_restart,
                    slot_confirmed_flush_lsn=slot_flush,
                    intervals=missing_ivs,
                    reseed_requirement=reseed,
                    naive_verdict=naive,
                    summary=summary,
                )

        # 5. Slot WAL status lost
        if self.slot.wal_status == "lost":
            end_req = max(current_wal, slot_flush)
            missing_ivs, used_sources = self._slice_missing_evidence(
                offset_lsn, end_req, self.slot.timeline_id
            )
            has_gap = any(not i.reconstructable for i in missing_ivs)
            if has_gap or not missing_ivs:
                naive = NaiveRuleVerdict(
                    naive_lsn=naive_lsn,
                    rejected=True,
                    rejection_reason="replication slot WAL status is lost",
                )
                reseed = BoundedReseedRequirement(
                    required=True,
                    divergence_type="wal_lost",
                    reason="replication slot WAL status lost and secondary evidence incomplete",
                    missing_intervals=missing_ivs,
                    lower_bound_lsn=offset_lsn,
                    upper_bound_lsn=end_req,
                    suggested_scope=self.suggested_scope,
                    proof="server purged WAL past slot retention boundary",
                )
                summary = (
                    f"OFFSET_DIAG status=wal_lost_divergence offset={format_lsn(offset_lsn)} "
                    f"wal_status=lost naive_rule=rejected reseed=required"
                )
                return ReconciliationResult(
                    status="wal_lost_divergence",
                    offset_lsn=offset_lsn,
                    slot_restart_lsn=slot_restart,
                    slot_confirmed_flush_lsn=slot_flush,
                    intervals=missing_ivs,
                    reseed_requirement=reseed,
                    naive_verdict=naive,
                    summary=summary,
                )

        # 6. Offset behind slot restart (WAL purged from server slot)
        if offset_lsn < slot_restart:
            missing_ivs, used_sources = self._slice_missing_evidence(
                offset_lsn, slot_restart, self.slot.timeline_id
            )
            retained_iv = Interval(
                kind=INTERVAL_ALREADY_RETAINED,
                start_lsn=slot_restart,
                end_lsn=current_wal,
                detail="retained by replication slot on primary",
            )
            all_ivs = list(missing_ivs) + [retained_iv]
            has_uncovered = any(not i.reconstructable for i in missing_ivs)

            if has_uncovered or not missing_ivs:
                uncovered = [i for i in missing_ivs if not i.reconstructable]
                min_uncovered = uncovered[0].start_lsn if uncovered else offset_lsn
                max_uncovered = uncovered[-1].end_lsn if uncovered else slot_restart
                naive = NaiveRuleVerdict(
                    naive_lsn=naive_lsn,
                    rejected=True,
                    rejection_reason=f"naive choose-larger-LSN chooses {format_lsn(naive_lsn)} and silently drops missing log interval [{format_lsn(min_uncovered)}, {format_lsn(max_uncovered)}]",
                )
                reseed = BoundedReseedRequirement(
                    required=True,
                    divergence_type="missing_log_gap",
                    reason=f"missing WAL interval [{format_lsn(min_uncovered)}, {format_lsn(max_uncovered)}] purged from slot and absent from secondary evidence",
                    missing_intervals=uncovered,
                    lower_bound_lsn=min_uncovered,
                    upper_bound_lsn=max_uncovered,
                    suggested_scope=self.suggested_scope,
                    proof="reconstruction failed due to gaps in available log evidence",
                )
                summary = (
                    f"OFFSET_DIAG status=bounded_reseed_required offset={format_lsn(offset_lsn)} "
                    f"slot_restart={format_lsn(slot_restart)} gap=[{format_lsn(min_uncovered)}..{format_lsn(max_uncovered)}] "
                    f"naive_rule=rejected reseed=required"
                )
                return ReconciliationResult(
                    status="bounded_reseed_required",
                    offset_lsn=offset_lsn,
                    slot_restart_lsn=slot_restart,
                    slot_confirmed_flush_lsn=slot_flush,
                    intervals=all_ivs,
                    reseed_requirement=reseed,
                    naive_verdict=naive,
                    summary=summary,
                )

            replay_plan: List[Dict[str, Any]] = []
            for iv in missing_ivs:
                replay_plan.append(
                    {
                        "source": iv.evidence_source,
                        "start_lsn": iv.start_lsn,
                        "start_lsn_formatted": iv.start_lsn_formatted,
                        "end_lsn": iv.end_lsn,
                        "end_lsn_formatted": iv.end_lsn_formatted,
                    }
                )
            if slot_restart < current_wal:
                replay_plan.append(
                    {
                        "source": "replication_slot",
                        "start_lsn": slot_restart,
                        "start_lsn_formatted": format_lsn(slot_restart),
                        "end_lsn": current_wal,
                        "end_lsn_formatted": format_lsn(current_wal),
                    }
                )

            cert = ReconstructionCertificate(
                certified=True,
                status="certified_reconstruction",
                resume_lsn=offset_lsn,
                reconstructed_intervals=missing_ivs,
                retained_intervals=[retained_iv],
                evidence_sources=used_sources,
                replay_plan=replay_plan,
                proof=f"interval [{format_lsn(offset_lsn)}..{format_lsn(slot_restart)}] bridged by secondary evidence; safe to replay from {format_lsn(offset_lsn)}",
            )
            naive = NaiveRuleVerdict(
                naive_lsn=naive_lsn,
                rejected=True,
                rejection_reason=f"naive choose-larger-LSN would jump to {format_lsn(naive_lsn)} omitting interval [{format_lsn(offset_lsn)}, {format_lsn(slot_restart)}]; reconstructed from evidence instead",
            )
            summary = (
                f"OFFSET_DIAG status=reconstructed_recovery offset={format_lsn(offset_lsn)} "
                f"slot_restart={format_lsn(slot_restart)} bridged={','.join(used_sources)} cert=certified"
            )
            return ReconciliationResult(
                status="reconstructed_recovery",
                offset_lsn=offset_lsn,
                slot_restart_lsn=slot_restart,
                slot_confirmed_flush_lsn=slot_flush,
                intervals=all_ivs,
                certificate=cert,
                naive_verdict=naive,
                summary=summary,
            )

        # 7. Offset behind confirmed_flush, but within retained WAL
        if offset_lsn < slot_flush:
            retained_replay = Interval(
                kind=INTERVAL_ALREADY_RETAINED,
                start_lsn=offset_lsn,
                end_lsn=slot_flush,
                detail="retained in slot WAL; unapplied or rewound by consumer",
            )
            retained_head = Interval(
                kind=INTERVAL_ALREADY_RETAINED,
                start_lsn=slot_flush,
                end_lsn=current_wal,
                detail="retained in slot WAL ahead of flush",
            )
            all_ivs = [retained_replay, retained_head]
            naive = NaiveRuleVerdict(
                naive_lsn=naive_lsn,
                rejected=True,
                rejection_reason=f"naive choose-larger-LSN chooses {format_lsn(naive_lsn)} and skips already-retained interval [{format_lsn(offset_lsn)}, {format_lsn(slot_flush)}]",
            )
            replay_plan = [
                {
                    "source": "replication_slot",
                    "start_lsn": offset_lsn,
                    "start_lsn_formatted": format_lsn(offset_lsn),
                    "end_lsn": current_wal,
                    "end_lsn_formatted": format_lsn(current_wal),
                }
            ]
            cert = ReconstructionCertificate(
                certified=True,
                status="within_retained_slot",
                resume_lsn=offset_lsn,
                retained_intervals=all_ivs,
                replay_plan=replay_plan,
                proof=f"slot retains WAL from {format_lsn(slot_restart)} <= {format_lsn(offset_lsn)}; safe to stream from slot at {format_lsn(offset_lsn)}",
            )
            summary = (
                f"OFFSET_DIAG status=within_retained_slot offset={format_lsn(offset_lsn)} "
                f"slot_flush={format_lsn(slot_flush)} cert=certified"
            )
            return ReconciliationResult(
                status="within_retained_slot",
                offset_lsn=offset_lsn,
                slot_restart_lsn=slot_restart,
                slot_confirmed_flush_lsn=slot_flush,
                intervals=all_ivs,
                certificate=cert,
                naive_verdict=naive,
                summary=summary,
            )

        # 8. Offset in sync with confirmed_flush
        if offset_lsn == slot_flush:
            applied_iv = Interval(
                kind=INTERVAL_ALREADY_APPLIED,
                start_lsn=slot_restart,
                end_lsn=offset_lsn,
                detail="applied to target and confirmed flushed in slot",
            )
            retained_iv = Interval(
                kind=INTERVAL_ALREADY_RETAINED,
                start_lsn=offset_lsn,
                end_lsn=current_wal,
                detail="retained in slot WAL awaiting consumption",
            )
            all_ivs = [applied_iv, retained_iv] if slot_restart < offset_lsn else [retained_iv]
            cert = ReconstructionCertificate(
                certified=True,
                status="in_sync",
                resume_lsn=offset_lsn,
                applied_intervals=[applied_iv] if slot_restart < offset_lsn else [],
                retained_intervals=[retained_iv],
                replay_plan=[
                    {
                        "source": "replication_slot",
                        "start_lsn": offset_lsn,
                        "start_lsn_formatted": format_lsn(offset_lsn),
                        "end_lsn": current_wal,
                        "end_lsn_formatted": format_lsn(current_wal),
                    }
                ],
                proof=f"offset and slot confirmed flush match at {format_lsn(offset_lsn)}",
            )
            naive = NaiveRuleVerdict(
                naive_lsn=naive_lsn,
                rejected=False,
                rejection_reason="offset and slot flush are aligned",
            )
            summary = (
                f"OFFSET_DIAG status=in_sync offset={format_lsn(offset_lsn)} "
                f"slot={format_lsn(slot_flush)} cert=ok"
            )
            return ReconciliationResult(
                status="in_sync",
                offset_lsn=offset_lsn,
                slot_restart_lsn=slot_restart,
                slot_confirmed_flush_lsn=slot_flush,
                intervals=all_ivs,
                certificate=cert,
                naive_verdict=naive,
                summary=summary,
            )

        # 9. Offset ahead of confirmed_flush
        applied_flush = Interval(
            kind=INTERVAL_ALREADY_APPLIED,
            start_lsn=slot_restart,
            end_lsn=slot_flush,
            detail="applied and flushed to slot",
        )
        applied_ahead = Interval(
            kind=INTERVAL_ALREADY_APPLIED,
            start_lsn=slot_flush,
            end_lsn=offset_lsn,
            detail="applied by consumer, pending slot flush advance",
        )
        retained_head = Interval(
            kind=INTERVAL_ALREADY_RETAINED,
            start_lsn=offset_lsn,
            end_lsn=current_wal,
            detail="retained in slot WAL awaiting consumption",
        )
        all_ivs = [applied_flush, applied_ahead, retained_head]
        cert = ReconstructionCertificate(
            certified=True,
            status="fast_forward_safe",
            resume_lsn=offset_lsn,
            applied_intervals=[applied_flush, applied_ahead],
            retained_intervals=[retained_head],
            replay_plan=[
                {
                    "source": "replication_slot",
                    "start_lsn": offset_lsn,
                    "start_lsn_formatted": format_lsn(offset_lsn),
                    "end_lsn": current_wal,
                    "end_lsn_formatted": format_lsn(current_wal),
                }
            ],
            proof=f"interval [{format_lsn(slot_flush)}..{format_lsn(offset_lsn)}] already applied to target; slot can safely fast-forward",
        )
        naive = NaiveRuleVerdict(
            naive_lsn=naive_lsn,
            rejected=False,
            rejection_reason="offset is ahead and safe to fast-forward",
        )
        summary = (
            f"OFFSET_DIAG status=already_applied_ahead offset={format_lsn(offset_lsn)} "
            f"slot_flush={format_lsn(slot_flush)} advance_to={format_lsn(offset_lsn)} cert=ok"
        )
        return ReconciliationResult(
            status="already_applied_ahead",
            offset_lsn=offset_lsn,
            slot_restart_lsn=slot_restart,
            slot_confirmed_flush_lsn=slot_flush,
            intervals=all_ivs,
            certificate=cert,
            naive_verdict=naive,
            summary=summary,
        )
