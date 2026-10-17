"""Classify transient versus persistent divergence by positions (Row 614: RCH06.online-confirmation).

Extends delta, generation, and watermark fence logic with position-based online
confirmation where possible:
- Selects confirmation mechanisms via `decide.Rung` and `decide.climb(work, rungs, facts)`.
- Classifies candidate row and leaf differences into transient in-flight lag (settled),
  deleted-on-both-sides convergence (settled), delayed open-transaction or watermark
  holdout (uncertain/pending), continuously mutating rows (uncertain/pending), or
  confirmed persistent divergence (persistent drift).
- Uncertain rows stay pending and are never repaired automatically after an arbitrary sleep.
- Counts all selected proof units (total, settled, pending, persistent_drift, expired)
  and tracks expiry of online comparison evidence based on measured window, watermark,
  and retention facts rather than hardcoded magic numbers.
- Integrates O(1) typed durable checkpoint resume, active mutual-exclusion lease lock
  (`.run.lock`) with heartbeat, adaptive AIMD batch sizing with cold-start byte-weighted
  ETA, Merkle bucket diffing over proof units, and a normalized external tool wrapper
  with zero internal tool leaks.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import fcntl
import hashlib
import json
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable


# -----------------------------------------------------------------------------
# Care Badges, Safe Host Validation, and Core Enums
# -----------------------------------------------------------------------------

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


class OnlineConfirmationError(RuntimeError):
    """Base error for online confirmation and position-based divergence classification."""


class UnsafeEndpointHostError(OnlineConfirmationError, ValueError):
    """Raised when an endpoint host is outside approved local or 10.0.0.x test ranges."""


class AutomaticRepairOfUncertainRowRefusedError(OnlineConfirmationError):
    """Raised when a caller attempts automatic repair on a pending/uncertain row or expired proof."""


class EvidenceExpiredError(OnlineConfirmationError):
    """Raised when expired online comparison evidence is used to certify readiness or trigger repair."""


class VerificationLeaseError(OnlineConfirmationError):
    """Raised when an active verification lease lock (.run.lock) cannot be acquired or renewed."""


def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host uses approved loopback or 10.0.0.x test addresses."""
    cleaned = str(host or "").strip().lower()
    if not cleaned:
        raise UnsafeEndpointHostError("endpoint host cannot be empty")
    if cleaned in SAFE_TEST_HOSTS:
        return cleaned
    if cleaned.startswith("10.0.0."):
        parts = cleaned.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return cleaned
    raise UnsafeEndpointHostError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, ::1, or 10.0.0.x allowed"
    )


class ConfirmationStrategyKind(str, enum.Enum):
    """How online divergence candidates are confirmed against replication progress."""

    POSITION_WATERMARK_FENCE = "position_watermark_fence"
    GENERATION_SETTLE_PASS = "generation_settle_pass"
    HOLD_PENDING_NO_SLEEP_REPAIR = "hold_pending_no_sleep_repair"


class DivergenceState(str, enum.Enum):
    """Classification of a candidate row or leaf difference."""

    SETTLED_CONVERGED = "settled_converged"
    SETTLED_DELETED_ON_BOTH = "settled_deleted_on_both"
    PENDING_DELAYED_TRANSACTION = "pending_delayed_transaction"
    PENDING_CONTINUOUS_MUTATION = "pending_continuous_mutation"
    PENDING_BEHIND_WATERMARK = "pending_behind_watermark"
    PERSISTENT_DRIFT = "persistent_drift"
    EVIDENCE_EXPIRED = "evidence_expired"


class RepairEligibility(str, enum.Enum):
    """Whether a candidate row is eligible for repair."""

    NO_REPAIR_NEEDED = "no_repair_needed"
    HOLD_PENDING_DO_NOT_REPAIR = "hold_pending_do_not_repair"
    ELIGIBLE_CONFIRMED_DRIFT = "eligible_confirmed_drift"
    REVERIFY_REQUIRED_EXPIRED = "reverify_required_expired"


class ProofUnitStatus(str, enum.Enum):
    """Lifecycle status of a selected online comparison proof unit."""

    SETTLED = "settled"
    PENDING = "pending"
    PERSISTENT_DRIFT = "persistent_drift"
    EXPIRED = "expired"


# -----------------------------------------------------------------------------
# Position Parsing and Monotonic Watermark Algebra (LSN / GTID / SCN / Numeric)
# -----------------------------------------------------------------------------

def parse_position_token(pos: Any) -> Optional[Tuple[int, ...]]:
    """Parse LSN ('X/Y'), numeric SCN/offset, or tuple into a comparable integer tuple."""
    if pos is None:
        return None
    if isinstance(pos, tuple) and all(isinstance(x, int) for x in pos):
        return pos
    if isinstance(pos, int) and not isinstance(pos, bool):
        return (pos,)
    if isinstance(pos, float):
        if not math.isfinite(pos):
            return None
        return (int(pos * 1_000_000),)
    if isinstance(pos, str):
        s = pos.strip()
        if not s:
            return None
        if "/" in s:
            parts = s.split("/")
            if len(parts) == 2:
                return (int(parts[0], 16), int(parts[1], 16))
        if s.startswith(("0x", "0X")):
            return (int(s, 16),)
        if ":" in s and "-" not in s:
            parts = s.split(":")
            if all(p.isdigit() for p in parts):
                return tuple(int(p) for p in parts)
        if s.isdigit():
            return (int(s),)
    return None


def _normalize_gtid_intervals(intervals: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not intervals:
        return []
    ordered = sorted(intervals, key=lambda item: (item[0], item[1]))
    merged: List[Tuple[int, int]] = [ordered[0]]
    for lo, hi in ordered[1:]:
        prev_lo, prev_hi = merged[-1]
        if lo <= prev_hi + 1:
            merged[-1] = (prev_lo, max(prev_hi, hi))
        else:
            merged.append((lo, hi))
    return merged


def parse_gtid_set(gtid_str: str) -> Dict[str, List[Tuple[int, int]]]:
    """Parse a MySQL/MariaDB GTID set string into normalized per-UUID intervals."""
    if not gtid_str or not isinstance(gtid_str, str):
        return {}
    result: Dict[str, List[Tuple[int, int]]] = {}
    cleaned = "".join(gtid_str.split()).lower()
    for entry in cleaned.split(","):
        if not entry or ":" not in entry:
            continue
        parts = entry.split(":")
        source_id = parts[0]
        intervals: List[Tuple[int, int]] = []
        for seg in parts[1:]:
            if "-" in seg:
                lo_s, _, hi_s = seg.partition("-")
                if lo_s.isdigit() and hi_s.isdigit():
                    intervals.append((int(lo_s), int(hi_s)))
            elif seg.isdigit():
                val = int(seg)
                intervals.append((val, val))
        if intervals:
            result.setdefault(source_id, []).extend(intervals)
    for source_id in list(result.keys()):
        result[source_id] = _normalize_gtid_intervals(result[source_id])
    return result


def position_covers(applied_pos: Any, required_pos: Any) -> bool:
    """Return True if `applied_pos` has reached or passed `required_pos`."""
    if required_pos is None:
        return True
    if applied_pos is None:
        return False
    if isinstance(required_pos, str) and isinstance(applied_pos, str):
        if ("-" in required_pos or "-" in applied_pos) and ":" in required_pos:
            req_gtid = parse_gtid_set(required_pos)
            app_gtid = parse_gtid_set(applied_pos)
            if not req_gtid:
                return False
            for uid, req_ivs in req_gtid.items():
                app_ivs = app_gtid.get(uid, [])
                for r_lo, r_hi in req_ivs:
                    if not any(a_lo <= r_lo and r_hi <= a_hi for a_lo, a_hi in app_ivs):
                        return False
            return True
    pa = parse_position_token(applied_pos)
    pr = parse_position_token(required_pos)
    if pa is None or pr is None:
        return False
    return pa >= pr


# -----------------------------------------------------------------------------
# Measured Window & Watermark Facts and Observations
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class MeasuredWatermarkFacts:
    """Measured replication, transaction, and retention facts from both endpoints.

    All bounds (evidence TTL, settle window, generation limits) are derived from these
    measured readings rather than arbitrary hardcoded constants.
    """

    source_host: str = "127.0.0.1"
    target_host: str = "127.0.0.1"
    source_high_watermark: Optional[Any] = None
    target_applied_watermark: Optional[Any] = None
    oldest_open_source_txn_position: Optional[Any] = None
    oldest_open_source_txn_age_s: float = 0.0
    measured_apply_lag_s: float = 0.0
    measured_round_trip_s: float = 0.01
    measured_window_scan_s: float = 0.5
    measured_retention_horizon_s: float = 300.0
    tail_alive: bool = True
    supports_position_fence: bool = True
    supports_generation_reread: bool = True
    observed_at_s: float = 0.0

    def __post_init__(self) -> None:
        validate_safe_endpoint(self.source_host)
        validate_safe_endpoint(self.target_host)
        if self.measured_retention_horizon_s <= 0:
            raise ValueError("measured_retention_horizon_s must be positive")
        if self.measured_window_scan_s <= 0:
            raise ValueError("measured_window_scan_s must be positive")

    @property
    def target_caught_up_to_high_watermark(self) -> bool:
        if not self.supports_position_fence:
            return False
        if self.source_high_watermark is None or self.target_applied_watermark is None:
            return False
        return position_covers(self.target_applied_watermark, self.source_high_watermark)

    @property
    def derived_evidence_ttl_s(self) -> float:
        """Derive evidence expiry horizon from measured retention, scan window, and lag."""
        # Evidence expires before log/tombstone retention rolls over, bounded by
        # measured scan and lag dynamics.
        active_cycle = (
            self.measured_window_scan_s
            + self.measured_apply_lag_s
            + self.oldest_open_source_txn_age_s
            + self.measured_round_trip_s
        )
        # Never exceed measured retention horizon; if active cycle is shorter, bound by
        # the smaller of retention horizon and 4x active cycle (minimum 2x scan window).
        floor_window = self.measured_window_scan_s + self.measured_round_trip_s
        dynamic_bound = max(floor_window, active_cycle * 2.0)
        return min(self.measured_retention_horizon_s, dynamic_bound)


@dataclass(frozen=True)
class RowVersionObservation:
    """One observation of a candidate key across source and target at a generation/position."""

    generation: int
    observed_at_s: float
    source_present: bool
    target_present: bool
    source_fingerprint: Optional[str]
    target_fingerprint: Optional[str]
    source_row_position: Optional[Any] = None
    target_applied_position: Optional[Any] = None
    source_open_txn_positions: Tuple[Any, ...] = ()
    changed_columns: Tuple[str, ...] = ()

    @property
    def both_absent(self) -> bool:
        return not self.source_present and not self.target_present

    @property
    def values_equal(self) -> bool:
        if self.both_absent:
            return True
        if not self.source_present or not self.target_present:
            return False
        return (
            self.source_fingerprint is not None
            and self.source_fingerprint == self.target_fingerprint
        )

    @property
    def delayed_by_open_txn(self) -> bool:
        """True if the row's change or pending commit is held by an uncommitted source transaction."""
        if not self.source_open_txn_positions:
            return False
        if self.source_row_position is None:
            return True
        for txn_pos in self.source_open_txn_positions:
            if position_covers(self.source_row_position, txn_pos) or position_covers(
                txn_pos, self.source_row_position
            ):
                return True
        return False

    @property
    def target_passed_row_position(self) -> bool:
        if self.source_row_position is None or self.target_applied_position is None:
            return False
        return position_covers(self.target_applied_position, self.source_row_position)


@dataclass(frozen=True)
class CandidateDivergenceTrace:
    """Observation history for one candidate key or leaf across confirmation passes."""

    unit_id: str
    table: str
    key: Any
    observations: Tuple[RowVersionObservation, ...]

    def __post_init__(self) -> None:
        if not self.observations:
            raise ValueError(f"CandidateDivergenceTrace for {self.unit_id!r} requires at least 1 observation")

    @property
    def latest(self) -> RowVersionObservation:
        return self.observations[-1]

    @property
    def generations_observed(self) -> int:
        return len(self.observations)

    @property
    def source_moved_across_passes(self) -> bool:
        if len(self.observations) < 2:
            return False
        prev, cur = self.observations[-2], self.observations[-1]
        return (
            prev.source_present != cur.source_present
            or prev.source_fingerprint != cur.source_fingerprint
            or prev.source_row_position != cur.source_row_position
        )

    @property
    def target_moved_across_passes(self) -> bool:
        if len(self.observations) < 2:
            return False
        prev, cur = self.observations[-2], self.observations[-1]
        return (
            prev.target_present != cur.target_present
            or prev.target_fingerprint != cur.target_fingerprint
        )

    @property
    def held_still_on_both_sides(self) -> bool:
        """True when at least 2 observations exist and neither side's row state moved in the last step."""
        if len(self.observations) < 2:
            return False
        return not self.source_moved_across_passes and not self.target_moved_across_passes


# -----------------------------------------------------------------------------
# Decision Engine Ladders (`decide.Rung` + `decide.climb`)
# -----------------------------------------------------------------------------

CONFIRMATION_STRATEGY_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "position_watermark_fence",
        "position-based confirmation against source watermark and target applied position",
        needs=(
            decide.Need(
                "endpoints expose comparable source and target replication positions",
                lambda f: bool(f.get("supports_position_fence"))
                and f.get("source_high_watermark") is not None
                and f.get("target_applied_watermark") is not None,
                "{rung} needs comparable source high watermark and target applied position",
            ),
            decide.Need(
                "replication tail is active and evidence window has not expired",
                lambda f: bool(f.get("tail_alive")) and not bool(f.get("evidence_expired")),
                "{rung} needs an active tail and fresh comparison evidence within measured retention",
            ),
        ),
        because="proves whether target apply has crossed the exact source commit position of each candidate row",
    ),
    decide.Rung(
        "generation_settle_pass",
        "generation-by-generation reread confirmation of differing leaves and keys",
        needs=(
            decide.Need(
                "endpoints support generation rereads within the evidence window",
                lambda f: bool(f.get("supports_generation_reread"))
                and not bool(f.get("evidence_expired"))
                and int(f.get("generations_observed", 0)) >= 2,
                "{rung} needs at least two generation rereads before expiry",
            ),
        ),
        because="distinguishes still-moving leaves from stationary drift when position watermarks are unavailable",
    ),
    decide.Rung(
        "hold_pending_no_sleep_repair",
        "hold uncertain rows pending without arbitrary sleep or automatic repair",
        needs=(),
        because="uncertain or unconfirmed differences must stay pending and never trigger blind sleep-based repair",
    ),
)


DIVERGENCE_DISPOSITION_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "evidence_expired",
        "comparison evidence expired past measured watermark and retention window",
        needs=(
            decide.Need(
                "comparison evidence age exceeds measured TTL bound",
                lambda f: bool(f.get("evidence_expired")),
                "evidence is still within the measured watermark and retention window",
            ),
        ),
        because="expired online evidence cannot certify convergence or authorize repair",
    ),
    decide.Rung(
        "settled_deleted_on_both",
        "row deleted on both source and target and confirmed absent",
        needs=(
            decide.Need(
                "row is absent on both source and target at confirmation",
                lambda f: bool(f.get("both_absent")) and not bool(f.get("delayed_by_open_txn")),
                "row is still present on at least one side or held by an open transaction",
            ),
        ),
        because="concurrent or replicated deletes converged to absence on both sides",
    ),
    decide.Rung(
        "settled_converged",
        "row values converged to equality on both source and target",
        needs=(
            decide.Need(
                "both sides hold identical canonical row fingerprints",
                lambda f: bool(f.get("values_equal"))
                and not bool(f.get("both_absent"))
                and not bool(f.get("delayed_by_open_txn")),
                "row fingerprints still differ, row is absent on both, or an open transaction holds it",
            ),
        ),
        because="in-flight replication change arrived and canonical row values now match",
    ),
    decide.Rung(
        "pending_delayed_transaction",
        "candidate row held by a delayed or uncommitted source transaction",
        needs=(
            decide.Need(
                "an open or delayed source transaction covers the candidate key or watermark",
                lambda f: bool(f.get("delayed_by_open_txn")),
                "no delayed or uncommitted source transaction holds this row",
            ),
        ),
        because="delayed transactions must commit and flush before divergence can be judged",
    ),
    decide.Rung(
        "pending_continuous_mutation",
        "candidate row is continuously changing across confirmation generations",
        needs=(
            decide.Need(
                "row fingerprint or position moved between consecutive confirmation reads",
                lambda f: bool(f.get("continuously_changing")),
                "row held still across confirmation reads",
            ),
        ),
        because="a hot key mutating across every generation is in motion, not confirmed persistent corruption",
    ),
    decide.Rung(
        "persistent_drift",
        "confirmed persistent divergence after watermark fence or stationary generations",
        needs=(
            decide.Need(
                "target apply passed source row position or row held still across generations while differing",
                lambda f: bool(f.get("position_confirmed_past_row"))
                or bool(f.get("stationary_across_generations")),
                "target has not yet passed the source position and row has not held still across generations",
            ),
        ),
        because="target has already applied past the row's commit position (or both sides held still) and values still differ",
    ),
    decide.Rung(
        "pending_behind_watermark",
        "uncertain divergence waiting for target apply watermark or next generation",
        needs=(),
        because="lagging target or single-pass observation stays pending and is never repaired after an arbitrary sleep",
    ),
)


REPAIR_GATE_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "reverify_required_expired",
        "refuse repair and require fresh verification because comparison evidence expired",
        needs=(
            decide.Need(
                "evidence is expired",
                lambda f: f.get("divergence_state") == DivergenceState.EVIDENCE_EXPIRED.value
                or bool(f.get("evidence_expired")),
                "comparison evidence has not expired",
            ),
        ),
        because="stale online comparison evidence cannot drive target mutations",
    ),
    decide.Rung(
        "no_repair_needed",
        "no repair needed because candidate row settled on both sides",
        needs=(
            decide.Need(
                "candidate row settled by value equality or deletion on both sides",
                lambda f: f.get("divergence_state")
                in (
                    DivergenceState.SETTLED_CONVERGED.value,
                    DivergenceState.SETTLED_DELETED_ON_BOTH.value,
                ),
                "candidate row has not settled to equality or mutual deletion",
            ),
        ),
        because="settled rows require zero target writes",
    ),
    decide.Rung(
        "eligible_confirmed_drift",
        "authorize fenced repair for position-confirmed or stationary persistent drift",
        needs=(
            decide.Need(
                "divergence is confirmed persistent drift with fresh evidence and no open txn",
                lambda f: f.get("divergence_state") == DivergenceState.PERSISTENT_DRIFT.value
                and not bool(f.get("evidence_expired"))
                and not bool(f.get("delayed_by_open_txn"))
                and not bool(f.get("continuously_changing")),
                "row is still pending, continuously mutating, delayed by an open txn, or expired",
            ),
        ),
        because="only confirmed persistent drift with fresh position/generation proof may be repaired",
    ),
    decide.Rung(
        "hold_pending_do_not_repair",
        "hold row pending and forbid automatic repair after arbitrary sleep",
        needs=(),
        because="uncertain rows stay pending until position or generation evidence settles them",
    ),
)


def select_confirmation_strategy(
    work: str,
    watermark_facts: MeasuredWatermarkFacts,
    *,
    generations_observed: int = 1,
    evidence_age_s: float = 0.0,
) -> decide.Choice:
    """Choose the online confirmation strategy via `decide.climb(work, rungs, facts)`."""
    ttl_s = watermark_facts.derived_evidence_ttl_s
    expired = evidence_age_s > ttl_s
    facts = decide.Facts(
        {
            "supports_position_fence": watermark_facts.supports_position_fence,
            "source_high_watermark": watermark_facts.source_high_watermark,
            "target_applied_watermark": watermark_facts.target_applied_watermark,
            "tail_alive": watermark_facts.tail_alive,
            "supports_generation_reread": watermark_facts.supports_generation_reread,
            "generations_observed": int(generations_observed),
            "evidence_age_s": float(evidence_age_s),
            "evidence_ttl_s": float(ttl_s),
            "evidence_expired": bool(expired),
        }
    )
    return decide.climb(work, CONFIRMATION_STRATEGY_LADDER, facts)


@dataclass(frozen=True)
class RowConfirmationDecision:
    """Full decision record for a single candidate divergence trace."""

    unit_id: str
    table: str
    key: Any
    strategy_kind: ConfirmationStrategyKind
    divergence_state: DivergenceState
    repair_eligibility: RepairEligibility
    care_badge: CareBadge
    evidence_age_s: float
    evidence_ttl_s: float
    evidence_expired: bool
    generations_observed: int
    changed_columns: Tuple[str, ...]
    strategy_choice: decide.Choice
    disposition_choice: decide.Choice
    repair_choice: decide.Choice
    summary: str

    @property
    def is_settled(self) -> bool:
        return self.divergence_state in (
            DivergenceState.SETTLED_CONVERGED,
            DivergenceState.SETTLED_DELETED_ON_BOTH,
        )

    @property
    def is_pending(self) -> bool:
        return self.divergence_state in (
            DivergenceState.PENDING_DELAYED_TRANSACTION,
            DivergenceState.PENDING_CONTINUOUS_MUTATION,
            DivergenceState.PENDING_BEHIND_WATERMARK,
        )

    @property
    def is_persistent_drift(self) -> bool:
        return self.divergence_state == DivergenceState.PERSISTENT_DRIFT


def classify_candidate_divergence(
    trace: CandidateDivergenceTrace,
    watermark_facts: MeasuredWatermarkFacts,
    *,
    now_s: Optional[float] = None,
) -> RowConfirmationDecision:
    """Classify a candidate row or leaf divergence trace by positions and generations."""
    latest = trace.latest
    eval_time_s = float(now_s if now_s is not None else max(latest.observed_at_s, watermark_facts.observed_at_s))
    first_observed_s = float(trace.observations[0].observed_at_s)
    evidence_age_s = max(0.0, eval_time_s - first_observed_s)
    ttl_s = watermark_facts.derived_evidence_ttl_s
    expired = evidence_age_s > ttl_s

    work_prefix = f"{trace.table}:{trace.unit_id}:{trace.key}"
    strategy_choice = select_confirmation_strategy(
        f"{work_prefix}:strategy",
        watermark_facts,
        generations_observed=trace.generations_observed,
        evidence_age_s=evidence_age_s,
    )
    strategy_kind = ConfirmationStrategyKind(
        strategy_choice.path or ConfirmationStrategyKind.HOLD_PENDING_NO_SLEEP_REPAIR.value
    )

    # Determine position-based and generation-based confirmation facts
    row_pos = latest.source_row_position if latest.source_row_position is not None else watermark_facts.source_high_watermark
    target_pos = (
        latest.target_applied_position
        if latest.target_applied_position is not None
        else watermark_facts.target_applied_watermark
    )

    open_txn_blocked = latest.delayed_by_open_txn
    if not open_txn_blocked and watermark_facts.oldest_open_source_txn_position is not None and row_pos is not None:
        # If an open source transaction started at or before the row/high watermark,
        # and the row has not converged, check whether the open txn holds this row's range.
        if latest.source_open_txn_positions:
            open_txn_blocked = True

    continuously_changing = (
        trace.generations_observed >= 2
        and (trace.source_moved_across_passes or trace.target_moved_across_passes)
        and not latest.values_equal
        and not latest.both_absent
    )

    position_confirmed_past_row = (
        strategy_kind == ConfirmationStrategyKind.POSITION_WATERMARK_FENCE
        and row_pos is not None
        and target_pos is not None
        and position_covers(target_pos, row_pos)
        and not open_txn_blocked
        and not continuously_changing
        and not latest.values_equal
        and not latest.both_absent
    )

    stationary_across_generations = (
        trace.generations_observed >= 2
        and trace.held_still_on_both_sides
        and not open_txn_blocked
        and not latest.values_equal
        and not latest.both_absent
    )

    disposition_facts = decide.Facts(
        {
            "evidence_expired": bool(expired),
            "both_absent": bool(latest.both_absent),
            "values_equal": bool(latest.values_equal),
            "delayed_by_open_txn": bool(open_txn_blocked),
            "continuously_changing": bool(continuously_changing),
            "position_confirmed_past_row": bool(position_confirmed_past_row),
            "stationary_across_generations": bool(stationary_across_generations),
        }
    )
    disposition_choice = decide.climb(
        f"{work_prefix}:disposition",
        DIVERGENCE_DISPOSITION_LADDER,
        disposition_facts,
    )
    div_state = DivergenceState(
        disposition_choice.path or DivergenceState.PENDING_BEHIND_WATERMARK.value
    )

    repair_facts = decide.Facts(
        {
            "divergence_state": div_state.value,
            "evidence_expired": bool(expired),
            "delayed_by_open_txn": bool(open_txn_blocked),
            "continuously_changing": bool(continuously_changing),
        }
    )
    repair_choice = decide.climb(
        f"{work_prefix}:repair",
        REPAIR_GATE_LADDER,
        repair_facts,
    )
    repair_elig = RepairEligibility(
        repair_choice.path or RepairEligibility.HOLD_PENDING_DO_NOT_REPAIR.value
    )

    if div_state == DivergenceState.PERSISTENT_DRIFT:
        badge = CareBadge.ACTION
    elif div_state == DivergenceState.EVIDENCE_EXPIRED:
        badge = CareBadge.REVIEW
    elif div_state in (
        DivergenceState.PENDING_DELAYED_TRANSACTION,
        DivergenceState.PENDING_CONTINUOUS_MUTATION,
        DivergenceState.PENDING_BEHIND_WATERMARK,
    ):
        badge = CareBadge.BENIGN
    else:
        badge = CareBadge.PASS

    summary = (
        f"[{badge.value}] {trace.table} key={trace.key} state={div_state.value} "
        f"strategy={strategy_kind.value} repair={repair_elig.value} "
        f"generations={trace.generations_observed} age={evidence_age_s:.2f}s/{ttl_s:.2f}s"
    )

    return RowConfirmationDecision(
        unit_id=trace.unit_id,
        table=trace.table,
        key=trace.key,
        strategy_kind=strategy_kind,
        divergence_state=div_state,
        repair_eligibility=repair_elig,
        care_badge=badge,
        evidence_age_s=evidence_age_s,
        evidence_ttl_s=ttl_s,
        evidence_expired=expired,
        generations_observed=trace.generations_observed,
        changed_columns=latest.changed_columns,
        strategy_choice=strategy_choice,
        disposition_choice=disposition_choice,
        repair_choice=repair_choice,
        summary=summary,
    )


def enforce_no_arbitrary_sleep_repair(decision: RowConfirmationDecision) -> bool:
    """Enforce that only confirmed persistent drift with fresh evidence can be repaired.

    Raises `EvidenceExpiredError` if the comparison evidence has expired, or
    `AutomaticRepairOfUncertainRowRefusedError` if the row is pending/uncertain.
    Returns True if repair is authorized, False if no repair is needed (settled).
    """
    if decision.repair_eligibility == RepairEligibility.REVERIFY_REQUIRED_EXPIRED:
        raise EvidenceExpiredError(
            f"cannot repair {decision.table} key={decision.key}: online comparison evidence "
            f"expired ({decision.evidence_age_s:.2f}s > {decision.evidence_ttl_s:.2f}s)"
        )
    if decision.repair_eligibility == RepairEligibility.HOLD_PENDING_DO_NOT_REPAIR:
        raise AutomaticRepairOfUncertainRowRefusedError(
            f"refusing automatic repair of uncertain row {decision.table} key={decision.key} "
            f"in state {decision.divergence_state.value}: uncertain rows stay pending, "
            f"never repaired after an arbitrary sleep"
        )
    if decision.repair_eligibility == RepairEligibility.NO_REPAIR_NEEDED:
        return False
    return True


# -----------------------------------------------------------------------------
# Proof Unit Accounting & Evidence Expiry Ledger
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class SelectedProofUnit:
    """One selected online comparison unit (table range, hash bucket, or change window)."""

    unit_id: str
    table: str
    range_start: Any
    range_end: Any
    row_count: int
    byte_weight: int
    captured_source_watermark: Any
    captured_at_s: float
    ttl_s: float

    def is_expired(self, now_s: float) -> bool:
        return max(0.0, float(now_s) - self.captured_at_s) > self.ttl_s


@dataclass(frozen=True)
class ProofUnitAccountingSummary:
    """Complete denominator and status breakdown of all selected proof units."""

    total_selected_units: int
    evaluated_units: int
    settled_units: int
    pending_units: int
    persistent_drift_units: int
    expired_units: int
    unevaluated_units: int
    total_rows_covered: int
    total_bytes_covered: int
    settled_keys: Tuple[Any, ...]
    deleted_on_both_keys: Tuple[Any, ...]
    pending_keys: Tuple[Any, ...]
    persistent_drift_keys: Tuple[Any, ...]
    expired_unit_ids: Tuple[str, ...]
    care_badge: CareBadge
    cutover_ready: bool
    summary_line: str


class OnlineConfirmationCoordinator:
    """Coordinates position-based online divergence confirmation, proof unit counting, and expiry."""

    def __init__(self, watermark_facts: MeasuredWatermarkFacts) -> None:
        self.watermark_facts = watermark_facts
        self._selected_units: Dict[str, SelectedProofUnit] = {}
        self._unit_traces: Dict[str, List[CandidateDivergenceTrace]] = {}
        self._unit_decisions: Dict[str, List[RowConfirmationDecision]] = {}

    def register_proof_unit(
        self,
        unit_id: str,
        table: str,
        range_start: Any,
        range_end: Any,
        *,
        row_count: int,
        byte_weight: int,
        captured_source_watermark: Optional[Any] = None,
        captured_at_s: Optional[float] = None,
        ttl_s: Optional[float] = None,
    ) -> SelectedProofUnit:
        """Register a selected proof unit into the denominator before evaluation."""
        if not unit_id:
            raise ValueError("unit_id cannot be empty")
        obs_s = float(captured_at_s if captured_at_s is not None else self.watermark_facts.observed_at_s)
        bound_ttl = float(ttl_s if ttl_s is not None else self.watermark_facts.derived_evidence_ttl_s)
        wm = (
            captured_source_watermark
            if captured_source_watermark is not None
            else self.watermark_facts.source_high_watermark
        )
        unit = SelectedProofUnit(
            unit_id=unit_id,
            table=table,
            range_start=range_start,
            range_end=range_end,
            row_count=max(0, int(row_count)),
            byte_weight=max(0, int(byte_weight)),
            captured_source_watermark=wm,
            captured_at_s=obs_s,
            ttl_s=bound_ttl,
        )
        self._selected_units[unit_id] = unit
        self._unit_traces.setdefault(unit_id, [])
        return unit

    def record_candidate_trace(self, trace: CandidateDivergenceTrace) -> None:
        """Attach a candidate divergence trace to a registered proof unit."""
        if trace.unit_id not in self._selected_units:
            raise KeyError(f"proof unit {trace.unit_id!r} is not registered in selected proof units")
        self._unit_traces[trace.unit_id].append(trace)

    def evaluate_all(self, *, now_s: Optional[float] = None) -> ProofUnitAccountingSummary:
        """Evaluate all selected proof units, classify every candidate trace, and check expiry."""
        eval_s = float(now_s if now_s is not None else self.watermark_facts.observed_at_s)
        settled_units = 0
        pending_units = 0
        drift_units = 0
        expired_units = 0
        evaluated_units = 0

        settled_keys: List[Any] = []
        deleted_both_keys: List[Any] = []
        pending_keys: List[Any] = []
        drift_keys: List[Any] = []
        expired_ids: List[str] = []

        self._unit_decisions.clear()

        for unit_id, unit in self._selected_units.items():
            traces = self._unit_traces.get(unit_id, [])
            evaluated_units += 1
            unit_expired = unit.is_expired(eval_s)
            decisions: List[RowConfirmationDecision] = []

            for tr in traces:
                dec = classify_candidate_divergence(
                    tr,
                    self.watermark_facts,
                    now_s=eval_s,
                )
                decisions.append(dec)
                if dec.divergence_state == DivergenceState.SETTLED_CONVERGED:
                    settled_keys.append(dec.key)
                elif dec.divergence_state == DivergenceState.SETTLED_DELETED_ON_BOTH:
                    settled_keys.append(dec.key)
                    deleted_both_keys.append(dec.key)
                elif dec.is_pending:
                    pending_keys.append(dec.key)
                elif dec.is_persistent_drift:
                    drift_keys.append(dec.key)

            self._unit_decisions[unit_id] = decisions

            if unit_expired or any(d.evidence_expired for d in decisions):
                expired_units += 1
                expired_ids.append(unit_id)
            elif any(d.is_persistent_drift for d in decisions):
                drift_units += 1
            elif any(d.is_pending for d in decisions):
                pending_units += 1
            else:
                settled_units += 1

        total_units = len(self._selected_units)
        unevaluated = max(0, total_units - evaluated_units)
        total_rows = sum(u.row_count for u in self._selected_units.values())
        total_bytes = sum(u.byte_weight for u in self._selected_units.values())

        if drift_units > 0:
            badge = CareBadge.ACTION
        elif expired_units > 0 or unevaluated > 0:
            badge = CareBadge.REVIEW
        elif pending_units > 0:
            badge = CareBadge.BENIGN
        else:
            badge = CareBadge.PASS

        cutover_ready = (
            total_units > 0
            and settled_units == total_units
            and drift_units == 0
            and pending_units == 0
            and expired_units == 0
            and unevaluated == 0
        )

        summary_line = (
            f"[{badge.value}] units={total_units} settled={settled_units} "
            f"pending={pending_units} drift={drift_units} expired={expired_units} "
            f"rows={total_rows} cutover_ready={cutover_ready}"
        )

        return ProofUnitAccountingSummary(
            total_selected_units=total_units,
            evaluated_units=evaluated_units,
            settled_units=settled_units,
            pending_units=pending_units,
            persistent_drift_units=drift_units,
            expired_units=expired_units,
            unevaluated_units=unevaluated,
            total_rows_covered=total_rows,
            total_bytes_covered=total_bytes,
            settled_keys=tuple(settled_keys),
            deleted_on_both_keys=tuple(deleted_both_keys),
            pending_keys=tuple(pending_keys),
            persistent_drift_keys=tuple(drift_keys),
            expired_unit_ids=tuple(expired_ids),
            care_badge=badge,
            cutover_ready=cutover_ready,
            summary_line=summary_line,
        )

    def decisions_for_unit(self, unit_id: str) -> Tuple[RowConfirmationDecision, ...]:
        return tuple(self._unit_decisions.get(unit_id, ()))


# -----------------------------------------------------------------------------
# Universal Typed Durable Resume in O(1)
# -----------------------------------------------------------------------------

def encode_typed_value(val: Any) -> Any:
    """Encode native Python types (datetime, date, Decimal, UUID, tuple) for durable JSON."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "val": [encode_typed_value(x) for x in val]}
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, dict):
        return {str(k): encode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [encode_typed_value(x) for x in val]
    return val


def decode_typed_value(val: Any) -> Any:
    """Restore native Python types from typed JSON envelopes."""
    if isinstance(val, dict):
        t = val.get("__type__")
        if t == "datetime":
            return datetime.datetime.fromisoformat(val["val"])
        if t == "date":
            return datetime.date.fromisoformat(val["val"])
        if t == "decimal":
            return decimal.Decimal(val["val"])
        if t == "uuid":
            return uuid.UUID(val["val"])
        if t == "tuple":
            return tuple(decode_typed_value(x) for x in val["val"])
        return {k: decode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [decode_typed_value(x) for x in val]
    return val


@dataclass(frozen=True)
class CompletedConfirmationUnitRecord:
    """Typed durable record for a confirmed proof unit."""

    unit_id: str
    table: str
    cursor_start: Any
    cursor_end: Any
    status: ProofUnitStatus
    source_watermark: Any
    confirmed_at_s: float
    ttl_s: float

    def is_valid_at(self, now_s: float) -> bool:
        if self.status != ProofUnitStatus.SETTLED:
            return False
        return max(0.0, float(now_s) - self.confirmed_at_s) <= self.ttl_s


class OnlineConfirmationCheckpoint:
    """O(1) typed durable checkpoint store that skips settled, non-expired units without rescanning."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        self._records: Dict[str, CompletedConfirmationUnitRecord] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        decoded = decode_typed_value(raw)
        units = decoded.get("units", {})
        self._records = {}
        for uid, item in units.items():
            self._records[uid] = CompletedConfirmationUnitRecord(
                unit_id=str(item["unit_id"]),
                table=str(item["table"]),
                cursor_start=item["cursor_start"],
                cursor_end=item["cursor_end"],
                status=ProofUnitStatus(item["status"]),
                source_watermark=item["source_watermark"],
                confirmed_at_s=float(item["confirmed_at_s"]),
                ttl_s=float(item["ttl_s"]),
            )

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "units": {
                uid: {
                    "unit_id": rec.unit_id,
                    "table": rec.table,
                    "cursor_start": rec.cursor_start,
                    "cursor_end": rec.cursor_end,
                    "status": rec.status.value,
                    "source_watermark": rec.source_watermark,
                    "confirmed_at_s": rec.confirmed_at_s,
                    "ttl_s": rec.ttl_s,
                }
                for uid, rec in self._records.items()
            },
        }
        durable.write_json(self.path, encode_typed_value(payload))

    def record_unit(self, record: CompletedConfirmationUnitRecord) -> None:
        self._records[record.unit_id] = record
        self.save()

    def should_skip_unit(self, unit_id: str, *, now_s: float) -> bool:
        """O(1) hash lookup checking whether a proof unit is settled and still fresh."""
        rec = self._records.get(unit_id)
        if rec is None:
            return False
        return rec.is_valid_at(now_s)

    def get_record(self, unit_id: str) -> Optional[CompletedConfirmationUnitRecord]:
        return self._records.get(unit_id)


# -----------------------------------------------------------------------------
# Mutual Exclusion & Active Lease Lock (.run.lock)
# -----------------------------------------------------------------------------

class OnlineVerificationLeaseLock:
    """Mutual exclusion lock (.run.lock) with PID and monotonic heartbeat."""

    def __init__(
        self,
        lock_dir: Union[str, Path],
        *,
        lease_ttl_s: float,
        owner_id: str = "online-confirmation",
    ) -> None:
        if lease_ttl_s <= 0:
            raise ValueError("lease_ttl_s must be positive")
        self.lock_dir = Path(lock_dir)
        self.lock_path = self.lock_dir / ".run.lock"
        self.lease_ttl_s = float(lease_ttl_s)
        self.owner_id = str(owner_id)
        self._fd: Optional[int] = None
        self.acquired_at_s: Optional[float] = None
        self.last_heartbeat_s: Optional[float] = None

    def acquire(self, *, now_s: Optional[float] = None) -> None:
        if self._fd is not None:
            raise VerificationLeaseError("verification lease lock is already held by this instance")
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise VerificationLeaseError(
                f"active verification lease lock already held at {self.lock_path}"
            ) from exc
        ts = float(now_s if now_s is not None else time.monotonic())
        self._fd = fd
        self.acquired_at_s = ts
        self.last_heartbeat_s = ts
        self._write_metadata(ts)

    def _write_metadata(self, ts: float) -> None:
        if self._fd is None:
            raise VerificationLeaseError("lease lock is not open")
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "owner_id": self.owner_id,
                "acquired_at_s": self.acquired_at_s,
                "heartbeat_at_s": ts,
                "lease_ttl_s": self.lease_ttl_s,
            },
            sort_keys=True,
        ).encode("utf-8")
        os.write(self._fd, payload)
        os.fsync(self._fd)

    def heartbeat(self, *, now_s: Optional[float] = None) -> None:
        if self._fd is None or self.last_heartbeat_s is None:
            raise VerificationLeaseError("cannot heartbeat an unacquired verification lease")
        ts = float(now_s if now_s is not None else time.monotonic())
        if ts - self.last_heartbeat_s > self.lease_ttl_s:
            raise VerificationLeaseError(
                f"verification lease expired before heartbeat ({ts - self.last_heartbeat_s:.2f}s > {self.lease_ttl_s:.2f}s)"
            )
        self.last_heartbeat_s = ts
        self._write_metadata(ts)

    def release(self) -> None:
        if self._fd is None:
            return
        fd = self._fd
        self._fd = None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> OnlineVerificationLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# Adaptive Execution: AIMD Batch Sizing, Cold-Start Byte-Weighted ETA, Merkle Diff
# -----------------------------------------------------------------------------

class AdaptiveConfirmationBatchController:
    """Dynamic AIMD batch sizing and instant cold-start byte-weighted ETA."""

    def __init__(
        self,
        *,
        initial_batch_rows: int,
        min_batch_rows: int,
        max_batch_rows: int,
        target_latency_s: float,
        additive_increase_rows: int,
        multiplicative_decrease_factor: float = 0.5,
    ) -> None:
        if min_batch_rows <= 0 or max_batch_rows < min_batch_rows:
            raise ValueError("invalid min/max batch row bounds")
        if target_latency_s <= 0:
            raise ValueError("target_latency_s must be positive")
        if not (0.0 < multiplicative_decrease_factor < 1.0):
            raise ValueError("multiplicative_decrease_factor must be in (0, 1)")
        self.min_batch_rows = int(min_batch_rows)
        self.max_batch_rows = int(max_batch_rows)
        self.current_batch_rows = max(self.min_batch_rows, min(self.max_batch_rows, int(initial_batch_rows)))
        self.target_latency_s = float(target_latency_s)
        self.additive_increase_rows = max(1, int(additive_increase_rows))
        self.multiplicative_decrease_factor = float(multiplicative_decrease_factor)
        self._ewma_bytes_per_sec: Optional[float] = None

    def observe_batch(self, *, rows: int, byte_weight: int, elapsed_s: float) -> int:
        elapsed = max(1e-6, float(elapsed_s))
        rate = float(max(1, byte_weight)) / elapsed
        if self._ewma_bytes_per_sec is None:
            self._ewma_bytes_per_sec = rate
        else:
            self._ewma_bytes_per_sec = 0.3 * rate + 0.7 * self._ewma_bytes_per_sec

        if elapsed <= self.target_latency_s:
            self.current_batch_rows = min(
                self.max_batch_rows,
                self.current_batch_rows + self.additive_increase_rows,
            )
        else:
            reduced = int(self.current_batch_rows * self.multiplicative_decrease_factor)
            self.current_batch_rows = max(self.min_batch_rows, reduced)
        return self.current_batch_rows

    def estimate_remaining_eta_s(
        self,
        *,
        remaining_bytes: int,
        cold_start_probe_bytes_per_sec: float,
    ) -> float:
        """Instant cold-start byte-weighted ETA before warm batch history accumulates."""
        if remaining_bytes <= 0:
            return 0.0
        effective_rate = (
            self._ewma_bytes_per_sec
            if self._ewma_bytes_per_sec is not None
            else float(cold_start_probe_bytes_per_sec)
        )
        if effective_rate <= 0:
            raise ValueError("effective byte rate must be positive")
        return float(remaining_bytes) / effective_rate


def merkle_diff_proof_buckets(
    source_buckets: Dict[int, Sequence[Tuple[Any, str]]],
    target_buckets: Dict[int, Sequence[Tuple[Any, str]]],
) -> Tuple[Tuple[int, ...], Tuple[Any, ...]]:
    """Compute Merkle bucket digests and return (divergent_bucket_ids, candidate_keys_in_divergent_buckets)."""
    all_bucket_ids = sorted(set(source_buckets.keys()) | set(target_buckets.keys()))
    divergent_buckets: List[int] = []
    candidate_keys: List[Any] = []

    def _bucket_digest(rows: Sequence[Tuple[Any, str]]) -> str:
        h = hashlib.sha256()
        for k, fp in sorted(rows, key=lambda item: str(item[0])):
            h.update(f"{k}:{fp}\n".encode("utf-8"))
        return h.hexdigest()

    for b_id in all_bucket_ids:
        s_rows = source_buckets.get(b_id, ())
        t_rows = target_buckets.get(b_id, ())
        if _bucket_digest(s_rows) != _bucket_digest(t_rows):
            divergent_buckets.append(b_id)
            s_map = dict(s_rows)
            t_map = dict(t_rows)
            for k in sorted(set(s_map.keys()) | set(t_map.keys()), key=str):
                if s_map.get(k) != t_map.get(k):
                    candidate_keys.append(k)

    return tuple(divergent_buckets), tuple(candidate_keys)


# -----------------------------------------------------------------------------
# Full-Capacity External Tool Wrapper (Zero Internal Tool Leaks)
# -----------------------------------------------------------------------------

_INTERNAL_TOOL_NAMES: FrozenSet[str] = frozenset(
    {"datafold", "dms", "dts", "reladiff", "dvt", "pgcopydb", "debezium"}
)


@dataclass(frozen=True)
class NormalizedConfirmationEnvelope:
    """Normalized migkit confirmation envelope with zero internal tool name leaks."""

    table: str
    care_badge: CareBadge
    divergence_state: DivergenceState
    confirmed_drift_count: int
    pending_count: int
    settled_count: int
    normalized_message: str


def normalize_external_tool_finding(
    raw_tool_name: str,
    *,
    table: str,
    raw_output: str,
    decision: RowConfirmationDecision,
) -> NormalizedConfirmationEnvelope:
    """Wrap external verifier/replication findings into migkit's normalized confirmation format."""
    sanitized = str(raw_output or "")
    for tool_token in _INTERNAL_TOOL_NAMES:
        sanitized = sanitized.replace(tool_token, "migkit-verifier")
        sanitized = sanitized.replace(tool_token.upper(), "MIGKIT-VERIFIER")
        sanitized = sanitized.replace(tool_token.capitalize(), "Migkit-Verifier")

    settled = 1 if decision.is_settled else 0
    pending = 1 if decision.is_pending else 0
    drift = 1 if decision.is_persistent_drift else 0

    msg = (
        f"[{decision.care_badge.value}] {table}: state={decision.divergence_state.value} "
        f"strategy={decision.strategy_kind.value} repair={decision.repair_eligibility.value}"
    )
    return NormalizedConfirmationEnvelope(
        table=table,
        care_badge=decision.care_badge,
        divergence_state=decision.divergence_state,
        confirmed_drift_count=drift,
        pending_count=pending,
        settled_count=settled,
        normalized_message=msg,
    )
