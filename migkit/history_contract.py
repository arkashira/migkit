"""Distinguish current-state migration from complete change history (Row 634: RCH26.history-contract).

Declares whether a migration or replication hop requires current final state
(snapshot/state-convergence) or every intermediate version/event (complete CDC,
audit history, or SCD Type 2 timeline).

Selects CDC, Change Tracking (CT), timestamp polling, temporal system-versioned
tables, or periodic snapshots only when their information content satisfies the
declared goal. Carries history cutoffs, deletion/tombstone semantics, and source
retention windows into formal verification proof.

Ensures that insert-and-delete lifecycles between polls and multiple updates to
the same key between syncs cannot be falsely certified as complete historical
capture, and keeps final-state coverage and history-capture coverage strictly
separate in verdicts and metrics.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import copy
import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import os
import socket
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union


# -----------------------------------------------------------------------------
# Care Badges and Core Enums
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


class HistoryGoal(str, enum.Enum):
    """Declared migration or synchronization contract goal."""

    FINAL_STATE = "final_state"
    COMPLETE_HISTORY = "complete_history"
    SCD2_TIMELINE = "scd2_timeline"
    WINDOWED_HISTORY = "windowed_history"


class CaptureMechanism(str, enum.Enum):
    """Candidate data capture and synchronization mechanisms."""

    LOG_BASED_CDC = "log_based_cdc"
    TEMPORAL_SYSTEM_VERSIONED = "temporal_system_versioned"
    CHANGE_TRACKING_CT = "change_tracking_ct"
    TIMESTAMP_POLLING = "timestamp_polling"
    PERIODIC_SNAPSHOT = "periodic_snapshot"


class DeletionSemantics(str, enum.Enum):
    """How deletions are represented and retained across the hop."""

    HARD_DELETE_OMITTED = "hard_delete_omitted"
    KEY_ONLY_TOMBSTONE = "key_only_tombstone"
    FULL_BEFORE_IMAGE_TOMBSTONE = "full_before_image_tombstone"
    SOFT_DELETE_FLAG = "soft_delete_flag"
    SCD2_CLOSE_INTERVAL = "scd2_close_interval"


class PreCutoffMode(str, enum.Enum):
    """How state prior to the declared history cutoff is treated in proof."""

    SNAPSHOT_BASELINE_ONLY = "snapshot_baseline_only"
    FULL_ARCHIVE_REQUIRED = "full_archive_required"
    EXCLUDED_BEFORE_CUTOFF = "excluded_before_cutoff"


class OperationType(str, enum.Enum):
    """Normalized DML change operation types."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SNAPSHOT_SEED = "SNAPSHOT_SEED"


class AnomalyCategory(str, enum.Enum):
    """Specific history-vs-state anomalies isolated during verification."""

    TRANSIENT_INSERT_DELETE_LOST = "transient_insert_delete_lost"
    MULTI_UPDATE_COALESCED = "multi_update_coalesced"
    MISSING_DELETE_TOMBSTONE = "missing_delete_tombstone"
    UNCLOSED_SCD2_INTERVAL = "unclosed_scd2_interval"
    BEFORE_IMAGE_MISSING_ON_UPDATE = "before_image_missing_on_update"
    BEFORE_IMAGE_MISSING_ON_DELETE = "before_image_missing_on_delete"
    OUT_OF_ORDER_HISTORY_SEQUENCE = "out_of_order_history_sequence"
    RETENTION_WINDOW_GAP = "retention_window_gap"
    PRE_CUTOFF_ARCHIVE_MISSING = "pre_cutoff_archive_missing"
    ZOMBIE_LIVE_ROW_ON_TARGET = "zombie_live_row_on_target"
    FINAL_STATE_VALUE_DRIFT = "final_state_value_drift"
    FINAL_STATE_MISSING_KEY = "final_state_missing_key"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class HistoryContractError(Exception):
    """Base exception for history contract violations."""


class MechanismIncompatibleError(HistoryContractError):
    """Raised when a capture mechanism's information content cannot satisfy the goal."""


class LeaseLockError(HistoryContractError):
    """Raised when mutual exclusion lease (.run.lock) cannot be acquired or renewed."""


class UnsafeEndpointError(HistoryContractError):
    """Raised when a non-test or unsafe host endpoint is supplied in test validation."""


# -----------------------------------------------------------------------------
# Host Safety Validation
# -----------------------------------------------------------------------------

def validate_safe_endpoint(host: str) -> str:
    """Ensure endpoint uses safe loopback or RFC1918 10.0.0.x addresses."""
    cleaned = (host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host cannot be empty")
    if cleaned in ("127.0.0.1", "localhost") or cleaned.startswith("10.0.0."):
        return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x allowed"
    )


# -----------------------------------------------------------------------------
# Typed JSON Serialization Helpers (O(1) Checkpoint Support)
# -----------------------------------------------------------------------------

def _encode_typed_value(val: Any) -> Any:
    """Recursively encode native Python types (datetime, Decimal, UUID, tuple) for JSON."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "val": [_encode_typed_value(x) for x in val]}
    if isinstance(val, set):
        return {"__type__": "set", "val": [_encode_typed_value(x) for x in sorted(val, key=str)]}
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, dict):
        return {str(k): _encode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_encode_typed_value(x) for x in val]
    return val


def _decode_typed_hook(obj: Dict[str, Any]) -> Any:
    """Restore native Python types from typed JSON envelopes."""
    t = obj.get("__type__")
    if t == "datetime":
        return datetime.datetime.fromisoformat(obj["val"])
    if t == "date":
        return datetime.date.fromisoformat(obj["val"])
    if t == "decimal":
        return decimal.Decimal(obj["val"])
    if t == "uuid":
        return uuid.UUID(obj["val"])
    if t == "tuple":
        return tuple(obj["val"])
    if t == "set":
        return set(obj["val"])
    return obj


def _parse_lsn_tuple(lsn: Optional[str]) -> Optional[Tuple[int, ...]]:
    """Parse PostgreSQL-style 'X/Y' or numeric LSN string into comparable integer tuple."""
    if lsn is None:
        return None
    s = str(lsn).strip()
    if not s:
        return None
    if "/" in s:
        parts = s.split("/")
        return tuple(int(p, 16) for p in parts)
    if s.startswith("0x") or s.startswith("0X"):
        return (int(s, 16),)
    if ":" in s:
        return tuple(int(p, 16) if any(c in p for c in "abcdefABCDEF") else int(p) for p in s.split(":"))
    return (int(s),)


def compare_lsn(a: Optional[str], b: Optional[str]) -> int:
    """Compare two LSN strings; returns -1 if a < b, 0 if a == b, 1 if a > b."""
    ta = _parse_lsn_tuple(a)
    tb = _parse_lsn_tuple(b)
    if ta is None or tb is None:
        raise ValueError(f"cannot compare None LSN: a={a!r}, b={b!r}")
    if ta < tb:
        return -1
    if ta > tb:
        return 1
    return 0


# -----------------------------------------------------------------------------
# Domain Models: Contract, Cutoffs, Tombstones, Retention, and Capabilities
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class HistoryCutoffSpec:
    """Explicit temporal/positional cutoff separating baseline state from tracked history."""

    cutoff_timestamp: Optional[datetime.datetime] = None
    cutoff_lsn: Optional[str] = None
    cutoff_version: Optional[int] = None
    pre_cutoff_mode: PreCutoffMode = PreCutoffMode.SNAPSHOT_BASELINE_ONLY

    def is_event_after_cutoff(
        self,
        event_ts: Optional[datetime.datetime] = None,
        event_lsn: Optional[str] = None,
        event_version: Optional[int] = None,
    ) -> bool:
        """Return True if the event falls on or after the configured history cutoff."""
        if self.cutoff_lsn is not None and event_lsn is not None:
            return compare_lsn(event_lsn, self.cutoff_lsn) >= 0
        if self.cutoff_version is not None and event_version is not None:
            return event_version >= self.cutoff_version
        if self.cutoff_timestamp is not None and event_ts is not None:
            return event_ts >= self.cutoff_timestamp
        return True

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed_value(dataclasses.asdict(self))


@dataclasses.dataclass(frozen=True)
class DeletionTombstonePolicy:
    """Contract rules for how deletes and transient lifecycles must be preserved."""

    deletion_semantics: DeletionSemantics = DeletionSemantics.KEY_ONLY_TOMBSTONE
    tombstone_retention_seconds: float = 86400.0
    require_before_image_on_delete: bool = False
    require_before_image_on_update: bool = False
    require_transient_lifecycle_capture: bool = True
    require_scd2_interval_closure: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed_value(dataclasses.asdict(self))


@dataclasses.dataclass(frozen=True)
class RetentionWindowProof:
    """Source log/change retention evidence evaluated against sync intervals and cutoffs."""

    source_retention_seconds: float
    max_sync_interval_seconds: float
    source_oldest_available_lsn: Optional[str] = None
    required_start_lsn: Optional[str] = None
    source_oldest_available_ts: Optional[datetime.datetime] = None
    required_start_ts: Optional[datetime.datetime] = None
    source_min_valid_version: Optional[int] = None
    last_synced_version: Optional[int] = None
    safety_margin_ratio: float = 2.0

    def evaluate(self) -> Tuple[bool, CareBadge, List[str]]:
        """Verify that source retention covers the required starting point and sync cadence."""
        reasons: List[str] = []
        is_valid = True
        badge = CareBadge.PASS

        if self.source_retention_seconds <= 0:
            is_valid = False
            badge = CareBadge.ACTION
            reasons.append("source_retention_seconds must be positive")

        if self.max_sync_interval_seconds >= self.source_retention_seconds:
            is_valid = False
            badge = CareBadge.ACTION
            reasons.append(
                f"max_sync_interval_seconds ({self.max_sync_interval_seconds:.0f}s) >= "
                f"source_retention_seconds ({self.source_retention_seconds:.0f}s): "
                "changes expire between syncs"
            )
        elif (
            self.max_sync_interval_seconds * self.safety_margin_ratio
            > self.source_retention_seconds
        ):
            if badge == CareBadge.PASS:
                badge = CareBadge.REVIEW
            reasons.append(
                f"sync interval ({self.max_sync_interval_seconds:.0f}s) leaves less than "
                f"{self.safety_margin_ratio:.1f}x headroom within retention "
                f"({self.source_retention_seconds:.0f}s)"
            )

        if (
            self.source_oldest_available_lsn is not None
            and self.required_start_lsn is not None
        ):
            if compare_lsn(self.source_oldest_available_lsn, self.required_start_lsn) > 0:
                is_valid = False
                badge = CareBadge.ACTION
                reasons.append(
                    f"LSN retention gap: oldest available LSN {self.source_oldest_available_lsn} "
                    f"> required start LSN {self.required_start_lsn}"
                )

        if (
            self.source_oldest_available_ts is not None
            and self.required_start_ts is not None
        ):
            if self.source_oldest_available_ts > self.required_start_ts:
                is_valid = False
                badge = CareBadge.ACTION
                reasons.append(
                    f"timestamp retention gap: oldest available {self.source_oldest_available_ts.isoformat()} "
                    f"> required start {self.required_start_ts.isoformat()}"
                )

        if (
            self.source_min_valid_version is not None
            and self.last_synced_version is not None
        ):
            if self.source_min_valid_version > self.last_synced_version:
                is_valid = False
                badge = CareBadge.ACTION
                reasons.append(
                    f"change tracking version expired: SYS_CHANGE_MIN_VALID_VERSION "
                    f"({self.source_min_valid_version}) > last_synced_version ({self.last_synced_version})"
                )

        if not reasons:
            reasons.append("retention window covers required cutoff and sync interval")

        return is_valid, badge, reasons

    def to_dict(self) -> Dict[str, Any]:
        valid, badge, reasons = self.evaluate()
        d = _encode_typed_value(dataclasses.asdict(self))
        d["is_valid"] = valid
        d["care_badge"] = badge.value
        d["reasons"] = reasons
        return d


@dataclasses.dataclass(frozen=True)
class CaptureCapabilityProfile:
    """Information-theoretic capability profile of a capture mechanism."""

    mechanism: CaptureMechanism
    captures_intermediate_versions: bool
    coalesces_updates_between_syncs: bool
    captures_transient_insert_delete: bool
    captures_hard_deletes: bool
    provides_before_image_on_update: bool
    provides_before_image_on_delete: bool
    preserves_commit_order: bool
    supports_scd2_interval_closure: bool
    deletion_semantics: DeletionSemantics
    retention_seconds: float = 86400.0
    poll_or_sync_interval_seconds: float = 60.0
    description: str = ""

    @classmethod
    def default_for_mechanism(
        cls,
        mechanism: CaptureMechanism,
        *,
        full_row_image: bool = True,
        soft_delete_enabled: bool = False,
        retention_seconds: float = 86400.0,
        poll_or_sync_interval_seconds: float = 60.0,
    ) -> "CaptureCapabilityProfile":
        """Build canonical capability profile for standard capture mechanisms."""
        if mechanism == CaptureMechanism.LOG_BASED_CDC:
            return cls(
                mechanism=mechanism,
                captures_intermediate_versions=True,
                coalesces_updates_between_syncs=False,
                captures_transient_insert_delete=True,
                captures_hard_deletes=True,
                provides_before_image_on_update=full_row_image,
                provides_before_image_on_delete=full_row_image,
                preserves_commit_order=True,
                supports_scd2_interval_closure=True,
                deletion_semantics=(
                    DeletionSemantics.FULL_BEFORE_IMAGE_TOMBSTONE
                    if full_row_image
                    else DeletionSemantics.KEY_ONLY_TOMBSTONE
                ),
                retention_seconds=retention_seconds,
                poll_or_sync_interval_seconds=poll_or_sync_interval_seconds,
                description="Transaction log CDC stream with per-event row images",
            )
        if mechanism == CaptureMechanism.TEMPORAL_SYSTEM_VERSIONED:
            return cls(
                mechanism=mechanism,
                captures_intermediate_versions=True,
                coalesces_updates_between_syncs=False,
                captures_transient_insert_delete=True,
                captures_hard_deletes=True,
                provides_before_image_on_update=True,
                provides_before_image_on_delete=True,
                preserves_commit_order=True,
                supports_scd2_interval_closure=True,
                deletion_semantics=DeletionSemantics.SCD2_CLOSE_INTERVAL,
                retention_seconds=retention_seconds,
                poll_or_sync_interval_seconds=poll_or_sync_interval_seconds,
                description="System-versioned temporal history table with valid_from/valid_to",
            )
        if mechanism == CaptureMechanism.CHANGE_TRACKING_CT:
            # SQL Server Change Tracking (CT) records only net changed keys + operation
            # since last version. Intermediate updates are coalesced, and before-images
            # are not stored in the CT side table.
            return cls(
                mechanism=mechanism,
                captures_intermediate_versions=False,
                coalesces_updates_between_syncs=True,
                captures_transient_insert_delete=False,
                captures_hard_deletes=True,
                provides_before_image_on_update=False,
                provides_before_image_on_delete=False,
                preserves_commit_order=False,
                supports_scd2_interval_closure=False,
                deletion_semantics=DeletionSemantics.KEY_ONLY_TOMBSTONE,
                retention_seconds=retention_seconds,
                poll_or_sync_interval_seconds=poll_or_sync_interval_seconds,
                description="Net-key change tracking (CT); coalesces updates and omits intermediate states",
            )
        if mechanism == CaptureMechanism.TIMESTAMP_POLLING:
            return cls(
                mechanism=mechanism,
                captures_intermediate_versions=False,
                coalesces_updates_between_syncs=True,
                captures_transient_insert_delete=False,
                captures_hard_deletes=soft_delete_enabled,
                provides_before_image_on_update=False,
                provides_before_image_on_delete=False,
                preserves_commit_order=False,
                supports_scd2_interval_closure=False,
                deletion_semantics=(
                    DeletionSemantics.SOFT_DELETE_FLAG
                    if soft_delete_enabled
                    else DeletionSemantics.HARD_DELETE_OMITTED
                ),
                retention_seconds=retention_seconds,
                poll_or_sync_interval_seconds=poll_or_sync_interval_seconds,
                description="High-watermark timestamp polling; coalesces updates and misses hard deletes",
            )
        if mechanism == CaptureMechanism.PERIODIC_SNAPSHOT:
            return cls(
                mechanism=mechanism,
                captures_intermediate_versions=False,
                coalesces_updates_between_syncs=True,
                captures_transient_insert_delete=False,
                captures_hard_deletes=True,
                provides_before_image_on_update=False,
                provides_before_image_on_delete=False,
                preserves_commit_order=False,
                supports_scd2_interval_closure=False,
                deletion_semantics=DeletionSemantics.KEY_ONLY_TOMBSTONE,
                retention_seconds=retention_seconds,
                poll_or_sync_interval_seconds=poll_or_sync_interval_seconds,
                description="Periodic full snapshot comparison; loses intra-interval transitions",
            )
        raise ValueError(f"unsupported CaptureMechanism: {mechanism!r}")


@dataclasses.dataclass(frozen=True)
class HistoryContractSpec:
    """Declared migration contract specifying state vs history requirements."""

    contract_id: str
    table_name: str
    goal: HistoryGoal
    cutoff: HistoryCutoffSpec = dataclasses.field(default_factory=HistoryCutoffSpec)
    tombstone_policy: DeletionTombstonePolicy = dataclasses.field(
        default_factory=DeletionTombstonePolicy
    )
    retention_proof: Optional[RetentionWindowProof] = None
    source_has_hard_deletes: bool = True
    source_host: str = "127.0.0.1"
    target_host: str = "10.0.0.10"

    def __post_init__(self) -> None:
        validate_safe_endpoint(self.source_host)
        validate_safe_endpoint(self.target_host)

    @property
    def requires_intermediate_history(self) -> bool:
        return self.goal in (
            HistoryGoal.COMPLETE_HISTORY,
            HistoryGoal.SCD2_TIMELINE,
            HistoryGoal.WINDOWED_HISTORY,
        )


# -----------------------------------------------------------------------------
# Mechanism Suitability & Selection Engine
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class CandidateEvaluation:
    """Evaluation of a single capture mechanism against a HistoryContractSpec."""

    mechanism: CaptureMechanism
    satisfied: bool
    care_badge: CareBadge
    information_content_score: int
    rejection_reasons: Tuple[str, ...]
    warnings: Tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class MechanismSelectionDecision:
    """Result of evaluating and selecting a capture mechanism for a contract."""

    contract_id: str
    declared_goal: HistoryGoal
    selected_mechanism: Optional[CaptureMechanism]
    selected_profile: Optional[CaptureCapabilityProfile]
    care_badge: CareBadge
    evaluations: Tuple[CandidateEvaluation, ...]
    selection_rationale: str

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed_value(dataclasses.asdict(self))


def evaluate_mechanism_for_contract(
    contract: HistoryContractSpec,
    profile: CaptureCapabilityProfile,
) -> CandidateEvaluation:
    """Check whether a capture mechanism's information content satisfies the contract."""
    rejections: List[str] = []
    warnings: List[str] = []

    # 1. Retention vs sync interval check
    if profile.poll_or_sync_interval_seconds >= profile.retention_seconds:
        rejections.append(
            f"sync_interval ({profile.poll_or_sync_interval_seconds:.0f}s) >= "
            f"retention ({profile.retention_seconds:.0f}s)"
        )

    if contract.retention_proof is not None:
        ret_ok, ret_badge, ret_reasons = contract.retention_proof.evaluate()
        if not ret_ok:
            rejections.extend(ret_reasons)
        elif ret_badge == CareBadge.REVIEW:
            warnings.extend(ret_reasons)

    # 2. Check HistoryGoal requirements
    if contract.requires_intermediate_history:
        if not profile.captures_intermediate_versions:
            rejections.append(
                f"mechanism '{profile.mechanism.value}' does not capture intermediate row versions "
                f"required by goal '{contract.goal.value}'"
            )
        if profile.coalesces_updates_between_syncs:
            rejections.append(
                f"mechanism '{profile.mechanism.value}' coalesces multiple updates between syncs, "
                "violating complete history capture"
            )
        if (
            contract.tombstone_policy.require_transient_lifecycle_capture
            and not profile.captures_transient_insert_delete
        ):
            rejections.append(
                f"mechanism '{profile.mechanism.value}' loses insert-and-delete lifecycles "
                "occurring between polls/syncs"
            )
        if not profile.preserves_commit_order:
            rejections.append(
                f"mechanism '{profile.mechanism.value}' does not preserve transaction commit order"
            )
        if (
            contract.goal == HistoryGoal.SCD2_TIMELINE
            or contract.tombstone_policy.require_scd2_interval_closure
        ) and not profile.supports_scd2_interval_closure:
            rejections.append(
                f"mechanism '{profile.mechanism.value}' cannot close SCD2 effective intervals accurately"
            )

    # 3. Check before-image requirements
    if (
        contract.tombstone_policy.require_before_image_on_update
        and not profile.provides_before_image_on_update
    ):
        rejections.append(
            f"mechanism '{profile.mechanism.value}' omits before-image on UPDATE required by contract"
        )
    if (
        contract.tombstone_policy.require_before_image_on_delete
        and not profile.provides_before_image_on_delete
    ):
        rejections.append(
            f"mechanism '{profile.mechanism.value}' omits before-image on DELETE required by contract"
        )

    # 4. Check deletion / tombstone semantics even for FINAL_STATE
    if contract.source_has_hard_deletes and not profile.captures_hard_deletes:
        rejections.append(
            f"mechanism '{profile.mechanism.value}' misses hard deletes on source, leaving zombie rows"
        )

    if (
        contract.cutoff.pre_cutoff_mode == PreCutoffMode.FULL_ARCHIVE_REQUIRED
        and profile.mechanism != CaptureMechanism.TEMPORAL_SYSTEM_VERSIONED
    ):
        # Log-based CDC alone only has retained log horizon unless paired with temporal archive
        if profile.mechanism == CaptureMechanism.LOG_BASED_CDC:
            warnings.append(
                "FULL_ARCHIVE_REQUIRED before cutoff requires verifying retention covers table birth LSN"
            )
        else:
            rejections.append(
                f"mechanism '{profile.mechanism.value}' cannot satisfy FULL_ARCHIVE_REQUIRED prior to cutoff"
            )

    # Calculate information content score (prefer minimal sufficient overhead for FINAL_STATE,
    # and highest fidelity for COMPLETE_HISTORY / SCD2_TIMELINE)
    fidelity_points = (
        (30 if profile.captures_intermediate_versions else 0)
        + (25 if not profile.coalesces_updates_between_syncs else 0)
        + (20 if profile.captures_transient_insert_delete else 0)
        + (10 if profile.captures_hard_deletes else 0)
        + (8 if profile.provides_before_image_on_update else 0)
        + (7 if profile.provides_before_image_on_delete else 0)
    )

    satisfied = len(rejections) == 0
    if not satisfied:
        badge = CareBadge.ACTION
    elif warnings:
        badge = CareBadge.REVIEW
    else:
        badge = CareBadge.PASS

    return CandidateEvaluation(
        mechanism=profile.mechanism,
        satisfied=satisfied,
        care_badge=badge,
        information_content_score=fidelity_points,
        rejection_reasons=tuple(rejections),
        warnings=tuple(warnings),
    )


def select_capture_mechanism(
    contract: HistoryContractSpec,
    candidates: Sequence[CaptureCapabilityProfile],
    *,
    strict: bool = False,
) -> MechanismSelectionDecision:
    """Select a capture mechanism whose information content satisfies the declared goal.

    Never selects CT, timestamp polling, or periodic snapshots when the declared goal
    requires every intermediate version/event or transient insert-and-delete capture.
    """
    if not candidates:
        raise ValueError("candidates list cannot be empty")

    profile_by_mech: Dict[CaptureMechanism, CaptureCapabilityProfile] = {}
    evaluations: List[CandidateEvaluation] = []

    for prof in candidates:
        profile_by_mech[prof.mechanism] = prof
        evaluations.append(evaluate_mechanism_for_contract(contract, prof))

    valid_evals = [ev for ev in evaluations if ev.satisfied]
    if not valid_evals:
        if strict:
            reasons_joined = "; ".join(
                f"{ev.mechanism.value}: {', '.join(ev.rejection_reasons)}"
                for ev in evaluations
            )
            raise MechanismIncompatibleError(
                f"no candidate mechanism satisfies goal '{contract.goal.value}' "
                f"for contract '{contract.contract_id}': {reasons_joined}"
            )
        return MechanismSelectionDecision(
            contract_id=contract.contract_id,
            declared_goal=contract.goal,
            selected_mechanism=None,
            selected_profile=None,
            care_badge=CareBadge.ACTION,
            evaluations=tuple(evaluations),
            selection_rationale=(
                f"REFUSED: zero candidate mechanisms satisfy declared goal '{contract.goal.value}'"
            ),
        )

    # Sort valid candidates:
    # - First prefer CareBadge.PASS over CareBadge.REVIEW
    # - If goal requires intermediate history, prefer highest information_content_score
    # - If goal is FINAL_STATE only, prefer mechanisms that satisfy final-state convergence
    #   cleanly (CDC/CT/Snapshot/Polling that meet delete semantics) ordered by deterministic rank
    def _sort_key(ev: CandidateEvaluation) -> Tuple[int, int]:
        badge_rank = 0 if ev.care_badge == CareBadge.PASS else 1
        if contract.requires_intermediate_history:
            return (badge_rank, -ev.information_content_score)
        return (badge_rank, -ev.information_content_score)

    valid_evals.sort(key=_sort_key)
    chosen_eval = valid_evals[0]
    chosen_prof = profile_by_mech[chosen_eval.mechanism]

    return MechanismSelectionDecision(
        contract_id=contract.contract_id,
        declared_goal=contract.goal,
        selected_mechanism=chosen_eval.mechanism,
        selected_profile=chosen_prof,
        care_badge=chosen_eval.care_badge,
        evaluations=tuple(evaluations),
        selection_rationale=(
            f"selected '{chosen_eval.mechanism.value}' for goal '{contract.goal.value}' "
            f"(info_score={chosen_eval.information_content_score}, badge={chosen_eval.care_badge.value})"
        ),
    )


# -----------------------------------------------------------------------------
# Event Stream, Poll Batch, and SCD2 Timeline Models
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class SourceChangeEvent:
    """Single committed mutation on the source database."""

    event_id: str
    primary_key: Any
    operation: OperationType
    sequence_no: int
    lsn: str
    committed_at: datetime.datetime
    before_image: Optional[Dict[str, Any]] = None
    after_image: Optional[Dict[str, Any]] = None
    sync_window_id: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed_value(dataclasses.asdict(self))


@dataclasses.dataclass(frozen=True)
class CapturedHistoryEvent:
    """Single event or version record captured by the migration pipeline on target."""

    primary_key: Any
    operation: OperationType
    sequence_no: int
    lsn: Optional[str] = None
    committed_at: Optional[datetime.datetime] = None
    before_image: Optional[Dict[str, Any]] = None
    after_image: Optional[Dict[str, Any]] = None
    is_tombstone: bool = False
    is_baseline_seed: bool = False
    effective_from: Optional[datetime.datetime] = None
    effective_to: Optional[datetime.datetime] = None
    is_current: Optional[bool] = None
    sync_window_id: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed_value(dataclasses.asdict(self))


@dataclasses.dataclass(frozen=True)
class HistoryAnomaly:
    """Concrete discrepancy detected in either final-state or history capture."""

    category: AnomalyCategory
    primary_key: Any
    sync_window_id: int
    care_badge: CareBadge
    affects_final_state: bool
    affects_history_contract: bool
    detail: str
    lost_event_ids: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed_value(dataclasses.asdict(self))


# -----------------------------------------------------------------------------
# Separate Final-State and History-Capture Coverage & Verdicts
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class FinalStateCoverage:
    """Coverage and verdict exclusively for current final-state convergence."""

    total_expected_live_keys: int
    matched_live_keys: int
    mismatched_live_keys: int
    missing_live_keys: int
    zombie_keys_on_target: int
    total_deleted_keys_verified: int
    final_state_coverage_ratio: float
    verdict: str
    care_badge: CareBadge
    merkle_root: str
    anomalies: Tuple[HistoryAnomaly, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed_value(dataclasses.asdict(self))


@dataclasses.dataclass(frozen=True)
class HistoryCaptureCoverage:
    """Coverage and verdict exclusively for intermediate change history & SCD2."""

    total_post_cutoff_transitions: int
    captured_post_cutoff_transitions: int
    pre_cutoff_baseline_events: int
    coalesced_intermediate_updates_lost: int
    transient_insert_deletes_lost: int
    missing_delete_tombstones: int
    unclosed_scd2_intervals: int
    missing_before_images: int
    out_of_order_transitions: int
    retention_gap_detected: bool
    history_coverage_ratio: float
    verdict: str
    care_badge: CareBadge
    merkle_root: str
    anomalies: Tuple[HistoryAnomaly, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed_value(dataclasses.asdict(self))


@dataclasses.dataclass(frozen=True)
class DualHistoryContractProof:
    """Combined proof carrying separate final-state and history verdicts."""

    contract: HistoryContractSpec
    mechanism: CaptureMechanism
    mechanism_evaluation: CandidateEvaluation
    final_state: FinalStateCoverage
    history_capture: HistoryCaptureCoverage
    overall_verdict: str
    overall_care_badge: CareBadge
    contract_satisfied: bool
    generated_at: float = dataclasses.field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract_id": self.contract.contract_id,
            "table_name": self.contract.table_name,
            "declared_goal": self.contract.goal.value,
            "mechanism": self.mechanism.value,
            "mechanism_satisfied": self.mechanism_evaluation.satisfied,
            "cutoff": self.contract.cutoff.to_dict(),
            "tombstone_policy": self.contract.tombstone_policy.to_dict(),
            "retention_proof": (
                self.contract.retention_proof.to_dict()
                if self.contract.retention_proof is not None
                else None
            ),
            "final_state": self.final_state.to_dict(),
            "history_capture": self.history_capture.to_dict(),
            "overall_verdict": self.overall_verdict,
            "overall_care_badge": self.overall_care_badge.value,
            "contract_satisfied": self.contract_satisfied,
            "generated_at": self.generated_at,
        }

    def format_unix_report(self) -> str:
        """Render pure ASCII UNIX operator report with explicit 4-tier care badges."""
        fs = self.final_state
        hc = self.history_capture
        lines = [
            f"[{self.overall_care_badge.value}] HISTORY_CONTRACT: "
            f"id={self.contract.contract_id} table={self.contract.table_name} "
            f"goal={self.contract.goal.value} mechanism={self.mechanism.value} "
            f"verdict={self.overall_verdict} satisfied={str(self.contract_satisfied).lower()}",
            f"  [{fs.care_badge.value}] FINAL_STATE_VERDICT: verdict={fs.verdict} "
            f"coverage={fs.final_state_coverage_ratio * 100.0:.1f}% "
            f"live_matched={fs.matched_live_keys}/{fs.total_expected_live_keys} "
            f"mismatched={fs.mismatched_live_keys} missing={fs.missing_live_keys} "
            f"zombies={fs.zombie_keys_on_target} merkle={fs.merkle_root[:16]}",
            f"  [{hc.care_badge.value}] HISTORY_CAPTURE_VERDICT: verdict={hc.verdict} "
            f"coverage={hc.history_coverage_ratio * 100.0:.1f}% "
            f"captured={hc.captured_post_cutoff_transitions}/{hc.total_post_cutoff_transitions} "
            f"coalesced_lost={hc.coalesced_intermediate_updates_lost} "
            f"transient_ins_del_lost={hc.transient_insert_deletes_lost} "
            f"missing_tombstones={hc.missing_delete_tombstones} "
            f"unclosed_scd2={hc.unclosed_scd2_intervals} "
            f"retention_gap={str(hc.retention_gap_detected).lower()} merkle={hc.merkle_root[:16]}",
        ]
        if not self.mechanism_evaluation.satisfied:
            for reason in self.mechanism_evaluation.rejection_reasons:
                lines.append(f"  [ACTION] MECHANISM_REJECTED: {reason}")
        for anom in fs.anomalies:
            lines.append(
                f"  [{anom.care_badge.value}] FINAL_STATE_ANOMALY: "
                f"cat={anom.category.value} key={anom.primary_key} window={anom.sync_window_id} "
                f"detail={anom.detail}"
            )
        for anom in hc.anomalies:
            lines.append(
                f"  [{anom.care_badge.value}] HISTORY_ANOMALY: "
                f"cat={anom.category.value} key={anom.primary_key} window={anom.sync_window_id} "
                f"detail={anom.detail}"
            )
        return "\n".join(lines)


# -----------------------------------------------------------------------------
# Merkle Digest Calculation for Separate State vs History Proof
# -----------------------------------------------------------------------------

def compute_merkle_root(items: Sequence[str]) -> str:
    """Compute deterministic SHA-256 Merkle root over canonical leaf strings."""
    if not items:
        return hashlib.sha256(b"EMPTY_MERKLE_TREE").hexdigest()
    layer = [hashlib.sha256(item.encode("ascii", errors="replace")).hexdigest() for item in items]
    while len(layer) > 1:
        next_layer: List[str] = []
        for i in range(0, len(layer), 2):
            left = layer[i]
            right = layer[i + 1] if i + 1 < len(layer) else left
            combined = f"{left}:{right}".encode("ascii")
            next_layer.append(hashlib.sha256(combined).hexdigest())
        layer = next_layer
    return layer[0]


# -----------------------------------------------------------------------------
# Simulation Helpers: How Polling / CT / CDC Observe a Source Event Stream
# -----------------------------------------------------------------------------

def simulate_mechanism_capture(
    source_events: Sequence[SourceChangeEvent],
    profile: CaptureCapabilityProfile,
) -> Tuple[Dict[Any, Dict[str, Any]], List[CapturedHistoryEvent]]:
    """Simulate what a capture mechanism observes from a sequence of source events.

    Demonstrates why polling and CT coalesce multi-updates within a sync window
    and drop transient insert-then-delete lifecycles between polls, whereas
    LOG_BASED_CDC and TEMPORAL_SYSTEM_VERSIONED preserve every transition.
    """
    ordered_source = sorted(source_events, key=lambda e: e.sequence_no)

    if profile.mechanism in (
        CaptureMechanism.LOG_BASED_CDC,
        CaptureMechanism.TEMPORAL_SYSTEM_VERSIONED,
    ):
        final_state: Dict[Any, Dict[str, Any]] = {}
        captured: List[CapturedHistoryEvent] = []
        active_Version_by_key: Dict[Any, CapturedHistoryEvent] = {}

        for ev in ordered_source:
            pk = ev.primary_key
            if ev.operation in (OperationType.INSERT, OperationType.SNAPSHOT_SEED):
                if ev.after_image is not None:
                    final_state[pk] = copy.deepcopy(ev.after_image)
                cap = CapturedHistoryEvent(
                    primary_key=pk,
                    operation=ev.operation,
                    sequence_no=ev.sequence_no,
                    lsn=ev.lsn,
                    committed_at=ev.committed_at,
                    before_image=None,
                    after_image=copy.deepcopy(ev.after_image),
                    is_tombstone=False,
                    is_baseline_seed=(ev.operation == OperationType.SNAPSHOT_SEED),
                    effective_from=ev.committed_at,
                    effective_to=None,
                    is_current=True,
                    sync_window_id=ev.sync_window_id,
                )
                captured.append(cap)
                active_Version_by_key[pk] = cap

            elif ev.operation == OperationType.UPDATE:
                if pk in active_Version_by_key:
                    prev = active_Version_by_key[pk]
                    idx = captured.index(prev)
                    captured[idx] = dataclasses.replace(
                        prev,
                        effective_to=ev.committed_at,
                        is_current=False,
                    )
                if ev.after_image is not None:
                    final_state[pk] = copy.deepcopy(ev.after_image)
                cap = CapturedHistoryEvent(
                    primary_key=pk,
                    operation=OperationType.UPDATE,
                    sequence_no=ev.sequence_no,
                    lsn=ev.lsn,
                    committed_at=ev.committed_at,
                    before_image=(
                        copy.deepcopy(ev.before_image)
                        if profile.provides_before_image_on_update
                        else None
                    ),
                    after_image=copy.deepcopy(ev.after_image),
                    is_tombstone=False,
                    effective_from=ev.committed_at,
                    effective_to=None,
                    is_current=True,
                    sync_window_id=ev.sync_window_id,
                )
                captured.append(cap)
                active_Version_by_key[pk] = cap

            elif ev.operation == OperationType.DELETE:
                if pk in active_Version_by_key:
                    prev = active_Version_by_key.pop(pk)
                    idx = captured.index(prev)
                    captured[idx] = dataclasses.replace(
                        prev,
                        effective_to=ev.committed_at,
                        is_current=False,
                    )
                final_state.pop(pk, None)
                cap = CapturedHistoryEvent(
                    primary_key=pk,
                    operation=OperationType.DELETE,
                    sequence_no=ev.sequence_no,
                    lsn=ev.lsn,
                    committed_at=ev.committed_at,
                    before_image=(
                        copy.deepcopy(ev.before_image)
                        if profile.provides_before_image_on_delete
                        else None
                    ),
                    after_image=None,
                    is_tombstone=True,
                    effective_from=ev.committed_at,
                    effective_to=ev.committed_at,
                    is_current=False,
                    sync_window_id=ev.sync_window_id,
                )
                captured.append(cap)

        return final_state, captured

    # Coalescing mechanisms: CHANGE_TRACKING_CT, TIMESTAMP_POLLING, PERIODIC_SNAPSHOT
    windows: Dict[int, List[SourceChangeEvent]] = {}
    for ev in ordered_source:
        windows.setdefault(ev.sync_window_id, []).append(ev)

    final_state = {}
    captured = []
    existed_before_window: Set[Any] = set()

    for win_id in sorted(windows.keys()):
        win_events = windows[win_id]
        by_key: Dict[Any, List[SourceChangeEvent]] = {}
        for ev in win_events:
            by_key.setdefault(ev.primary_key, []).append(ev)

        for pk, k_events in by_key.items():
            was_present_at_window_start = pk in existed_before_window
            first_ev = k_events[0]
            last_ev = k_events[-1]

            # Update running live state across all events in window
            live_row: Optional[Dict[str, Any]] = final_state.get(pk)
            for ev in k_events:
                if ev.operation in (OperationType.INSERT, OperationType.SNAPSHOT_SEED, OperationType.UPDATE):
                    live_row = copy.deepcopy(ev.after_image)
                elif ev.operation == OperationType.DELETE:
                    live_row = None

            if live_row is not None:
                # Row is live at the end of the poll/sync window:
                # All intermediate updates within this window are coalesced into 1 net event!
                final_state[pk] = live_row
                existed_before_window.add(pk)
                net_op = (
                    OperationType.UPDATE
                    if was_present_at_window_start
                    else first_ev.operation
                )
                captured.append(
                    CapturedHistoryEvent(
                        primary_key=pk,
                        operation=net_op,
                        sequence_no=last_ev.sequence_no,
                        lsn=last_ev.lsn,
                        committed_at=last_ev.committed_at,
                        before_image=None,
                        after_image=copy.deepcopy(live_row),
                        is_tombstone=False,
                        is_baseline_seed=(net_op == OperationType.SNAPSHOT_SEED),
                        effective_from=last_ev.committed_at,
                        effective_to=None,
                        is_current=True,
                        sync_window_id=win_id,
                    )
                )
            else:
                # Row is deleted at the end of the poll/sync window
                if not was_present_at_window_start:
                    # Inserted and deleted within the same poll/sync window!
                    # Polling, Periodic Snapshot, and CT net-state lookup see NOTHING!
                    final_state.pop(pk, None)
                else:
                    # Row existed prior to window and was deleted during window
                    existed_before_window.discard(pk)
                    if profile.captures_hard_deletes:
                        final_state.pop(pk, None)
                        captured.append(
                            CapturedHistoryEvent(
                                primary_key=pk,
                                operation=OperationType.DELETE,
                                sequence_no=last_ev.sequence_no,
                                lsn=last_ev.lsn,
                                committed_at=last_ev.committed_at,
                                before_image=None,
                                after_image=None,
                                is_tombstone=True,
                                effective_from=last_ev.committed_at,
                                effective_to=last_ev.committed_at,
                                is_current=False,
                                sync_window_id=win_id,
                            )
                        )
                    else:
                        # Timestamp polling without soft deletes misses the delete completely,
                        # leaving a stale zombie row on the target!
                        pass

    return final_state, captured


# -----------------------------------------------------------------------------
# Core Verification Engine: Separate Final-State vs Complete-History Proof
# -----------------------------------------------------------------------------

class HistoryContractVerifier:
    """Verifies final-state convergence and complete history capture with separate proofs."""

    def __init__(
        self,
        contract: HistoryContractSpec,
        profile: CaptureCapabilityProfile,
    ) -> None:
        self.contract = contract
        self.profile = profile

    @staticmethod
    def compute_expected_final_state(
        source_events: Sequence[SourceChangeEvent],
    ) -> Tuple[Dict[Any, Dict[str, Any]], Set[Any]]:
        """Derive expected final live rows and deleted keys from ordered source events."""
        live: Dict[Any, Dict[str, Any]] = {}
        deleted_keys: Set[Any] = set()
        for ev in sorted(source_events, key=lambda e: e.sequence_no):
            pk = ev.primary_key
            if ev.operation in (
                OperationType.INSERT,
                OperationType.SNAPSHOT_SEED,
                OperationType.UPDATE,
            ):
                if ev.after_image is not None:
                    live[pk] = copy.deepcopy(ev.after_image)
                deleted_keys.discard(pk)
            elif ev.operation == OperationType.DELETE:
                live.pop(pk, None)
                deleted_keys.add(pk)
        return live, deleted_keys

    def verify(
        self,
        source_events: Sequence[SourceChangeEvent],
        target_final_state: Dict[Any, Dict[str, Any]],
        captured_history: Sequence[CapturedHistoryEvent],
    ) -> DualHistoryContractProof:
        """Execute dual verification returning separate final-state and history verdicts."""
        mech_eval = evaluate_mechanism_for_contract(self.contract, self.profile)

        expected_live, expected_deleted = self.compute_expected_final_state(source_events)

        # ---------------------------------------------------------------------
        # 1. Final-State Verification (Strictly Current State Convergence)
        # ---------------------------------------------------------------------
        fs_anomalies: List[HistoryAnomaly] = []
        matched_live = 0
        mismatched_live = 0
        missing_live = 0
        zombie_keys = 0

        for pk, exp_row in sorted(expected_live.items(), key=lambda x: str(x[0])):
            if pk not in target_final_state:
                missing_live += 1
                fs_anomalies.append(
                    HistoryAnomaly(
                        category=AnomalyCategory.FINAL_STATE_MISSING_KEY,
                        primary_key=pk,
                        sync_window_id=-1,
                        care_badge=CareBadge.ACTION,
                        affects_final_state=True,
                        affects_history_contract=True,
                        detail=f"live key {pk!r} missing from target final state",
                    )
                )
            elif target_final_state[pk] != exp_row:
                mismatched_live += 1
                fs_anomalies.append(
                    HistoryAnomaly(
                        category=AnomalyCategory.FINAL_STATE_VALUE_DRIFT,
                        primary_key=pk,
                        sync_window_id=-1,
                        care_badge=CareBadge.ACTION,
                        affects_final_state=True,
                        affects_history_contract=True,
                        detail=(
                            f"live key {pk!r} value drift: expected={exp_row!r} "
                            f"observed={target_final_state[pk]!r}"
                        ),
                    )
                )
            else:
                matched_live += 1

        verified_deleted = 0
        for pk in sorted(expected_deleted, key=str):
            if pk in target_final_state:
                zombie_keys += 1
                fs_anomalies.append(
                    HistoryAnomaly(
                        category=AnomalyCategory.ZOMBIE_LIVE_ROW_ON_TARGET,
                        primary_key=pk,
                        sync_window_id=-1,
                        care_badge=CareBadge.ACTION,
                        affects_final_state=True,
                        affects_history_contract=True,
                        detail=f"deleted key {pk!r} remains live on target (zombie row)",
                    )
                )
            else:
                verified_deleted += 1

        # Also check any unexpected keys on target not in expected_live
        for pk in sorted(target_final_state.keys(), key=str):
            if pk not in expected_live and pk not in expected_deleted:
                zombie_keys += 1
                fs_anomalies.append(
                    HistoryAnomaly(
                        category=AnomalyCategory.ZOMBIE_LIVE_ROW_ON_TARGET,
                        primary_key=pk,
                        sync_window_id=-1,
                        care_badge=CareBadge.ACTION,
                        affects_final_state=True,
                        affects_history_contract=True,
                        detail=f"unexpected phantom/zombie key {pk!r} present on target",
                    )
                )

        total_fs_denom = len(expected_live) + zombie_keys
        if total_fs_denom == 0:
            fs_coverage_ratio = 1.0
        else:
            fs_coverage_ratio = float(matched_live) / float(total_fs_denom)

        if mismatched_live == 0 and missing_live == 0 and zombie_keys == 0:
            fs_verdict = "same"
            fs_badge = CareBadge.PASS
        else:
            fs_verdict = "different"
            fs_badge = CareBadge.ACTION

        fs_leaves = [
            f"{pk}:{json.dumps(_encode_typed_value(row), sort_keys=True)}"
            for pk, row in sorted(target_final_state.items(), key=lambda x: str(x[0]))
        ]
        fs_merkle = compute_merkle_root(fs_leaves)

        final_state_cov = FinalStateCoverage(
            total_expected_live_keys=len(expected_live),
            matched_live_keys=matched_live,
            mismatched_live_keys=mismatched_live,
            missing_live_keys=missing_live,
            zombie_keys_on_target=zombie_keys,
            total_deleted_keys_verified=verified_deleted,
            final_state_coverage_ratio=fs_coverage_ratio,
            verdict=fs_verdict,
            care_badge=fs_badge,
            merkle_root=fs_merkle,
            anomalies=tuple(fs_anomalies),
        )

        # ---------------------------------------------------------------------
        # 2. History-Capture Verification (Intermediate Transitions, Cutoffs, Tombstones)
        # ---------------------------------------------------------------------
        hc_anomalies: List[HistoryAnomaly] = []
        ordered_source = sorted(source_events, key=lambda e: e.sequence_no)

        post_cutoff_source: List[SourceChangeEvent] = []
        pre_cutoff_source: List[SourceChangeEvent] = []
        for ev in ordered_source:
            if self.contract.cutoff.is_event_after_cutoff(
                event_ts=ev.committed_at,
                event_lsn=ev.lsn,
                event_version=ev.sequence_no,
            ):
                post_cutoff_source.append(ev)
            else:
                pre_cutoff_source.append(ev)

        # Check pre-cutoff mode compliance
        captured_by_seq: Dict[int, CapturedHistoryEvent] = {
            c.sequence_no: c for c in captured_history
        }

        if (
            self.contract.cutoff.pre_cutoff_mode == PreCutoffMode.FULL_ARCHIVE_REQUIRED
            and pre_cutoff_source
        ):
            missing_pre = [
                ev for ev in pre_cutoff_source if ev.sequence_no not in captured_by_seq
            ]
            if missing_pre:
                hc_anomalies.append(
                    HistoryAnomaly(
                        category=AnomalyCategory.PRE_CUTOFF_ARCHIVE_MISSING,
                        primary_key=missing_pre[0].primary_key,
                        sync_window_id=missing_pre[0].sync_window_id,
                        care_badge=(
                            CareBadge.ACTION
                            if self.contract.requires_intermediate_history
                            else CareBadge.BENIGN
                        ),
                        affects_final_state=False,
                        affects_history_contract=True,
                        detail=(
                            f"{len(missing_pre)} pre-cutoff archive transitions missing "
                            "under FULL_ARCHIVE_REQUIRED policy"
                        ),
                        lost_event_ids=tuple(ev.event_id for ev in missing_pre),
                    )
                )

        # Group post-cutoff source events by (sync_window_id, primary_key) to detect:
        # (a) transient insert-and-delete between polls/syncs
        # (b) coalesced multiple updates between syncs
        # (c) missing delete tombstones
        # (d) missing before-images
        # (e) unclosed SCD2 intervals
        anomaly_badge = (
            CareBadge.ACTION
            if self.contract.requires_intermediate_history
            else CareBadge.BENIGN
        )

        coalesced_updates_lost = 0
        transient_ins_del_lost = 0
        missing_tombstones = 0
        unclosed_scd2 = 0
        missing_before_images = 0
        captured_transitions = 0

        windows_by_key: Dict[Tuple[int, Any], List[SourceChangeEvent]] = {}
        for ev in post_cutoff_source:
            windows_by_key.setdefault((ev.sync_window_id, ev.primary_key), []).append(ev)

        for (win_id, pk), k_events in sorted(
            windows_by_key.items(), key=lambda x: (x[0][0], str(x[0][1]))
        ):
            win_captured = [
                c
                for c in captured_history
                if c.sync_window_id == win_id and c.primary_key == pk
            ]
            captured_seqs = {c.sequence_no for c in win_captured}

            for ev in k_events:
                if ev.sequence_no in captured_seqs:
                    captured_transitions += 1

            # Check (a): Transient Insert-and-Delete within the same poll/sync window
            ops = [ev.operation for ev in k_events]
            if (
                OperationType.INSERT in ops
                and OperationType.DELETE in ops
                and ops.index(OperationType.INSERT) < len(ops) - 1 - ops[::-1].index(OperationType.DELETE)
            ):
                missing_in_lifecycle = [
                    ev for ev in k_events if ev.sequence_no not in captured_seqs
                ]
                if missing_in_lifecycle:
                    transient_ins_del_lost += len(missing_in_lifecycle)
                    hc_anomalies.append(
                        HistoryAnomaly(
                            category=AnomalyCategory.TRANSIENT_INSERT_DELETE_LOST,
                            primary_key=pk,
                            sync_window_id=win_id,
                            care_badge=anomaly_badge,
                            affects_final_state=False,
                            affects_history_contract=True,
                            detail=(
                                f"key {pk!r} inserted and deleted within sync_window={win_id}; "
                                f"{len(missing_in_lifecycle)}/{len(k_events)} lifecycle transitions "
                                "lost between polls"
                            ),
                            lost_event_ids=tuple(ev.event_id for ev in missing_in_lifecycle),
                        )
                    )
                    continue

            # Check (b): Multiple updates between syncs coalesced into a single state
            update_And_insert_events = [
                ev
                for ev in k_events
                if ev.operation in (OperationType.INSERT, OperationType.UPDATE)
            ]
            if len(update_And_insert_events) >= 2:
                missing_intermediate = [
                    ev
                    for ev in update_And_insert_events
                    if ev.sequence_no not in captured_seqs
                ]
                if missing_intermediate:
                    coalesced_updates_lost += len(missing_intermediate)
                    hc_anomalies.append(
                        HistoryAnomaly(
                            category=AnomalyCategory.MULTI_UPDATE_COALESCED,
                            primary_key=pk,
                            sync_window_id=win_id,
                            care_badge=anomaly_badge,
                            affects_final_state=False,
                            affects_history_contract=True,
                            detail=(
                                f"key {pk!r} had {len(update_And_insert_events)} writes in "
                                f"sync_window={win_id}; {len(missing_intermediate)} intermediate "
                                "version(s) coalesced and lost"
                            ),
                            lost_event_ids=tuple(ev.event_id for ev in missing_intermediate),
                        )
                    )

            # Check (c): Delete tombstone capture
            for ev in k_events:
                if ev.operation == OperationType.DELETE:
                    cap_del = captured_by_seq.get(ev.sequence_no)
                    if cap_del is None or not cap_del.is_tombstone:
                        missing_tombstones += 1
                        hc_anomalies.append(
                            HistoryAnomaly(
                                category=AnomalyCategory.MISSING_DELETE_TOMBSTONE,
                                primary_key=pk,
                                sync_window_id=win_id,
                                care_badge=anomaly_badge,
                                affects_final_state=False,
                                affects_history_contract=True,
                                detail=(
                                    f"DELETE on key {pk!r} (seq={ev.sequence_no}) missing "
                                    "explicit tombstone in captured history"
                                ),
                                lost_event_ids=(ev.event_id,),
                            )
                        )
                    elif (
                        self.contract.tombstone_policy.require_before_image_on_delete
                        and cap_del.before_image is None
                    ):
                        missing_before_images += 1
                        hc_anomalies.append(
                            HistoryAnomaly(
                                category=AnomalyCategory.BEFORE_IMAGE_MISSING_ON_DELETE,
                                primary_key=pk,
                                sync_window_id=win_id,
                                care_badge=anomaly_badge,
                                affects_final_state=False,
                                affects_history_contract=True,
                                detail=(
                                    f"DELETE tombstone on key {pk!r} (seq={ev.sequence_no}) "
                                    "missing required before_image payload"
                                ),
                                lost_event_ids=(ev.event_id,),
                            )
                        )

                elif ev.operation == OperationType.UPDATE:
                    cap_upd = captured_by_seq.get(ev.sequence_no)
                    if (
                        cap_upd is not None
                        and self.contract.tombstone_policy.require_before_image_on_update
                        and cap_upd.before_image is None
                    ):
                        missing_before_images += 1
                        hc_anomalies.append(
                            HistoryAnomaly(
                                category=AnomalyCategory.BEFORE_IMAGE_MISSING_ON_UPDATE,
                                primary_key=pk,
                                sync_window_id=win_id,
                                care_badge=anomaly_badge,
                                affects_final_state=False,
                                affects_history_contract=True,
                                detail=(
                                    f"UPDATE on key {pk!r} (seq={ev.sequence_no}) "
                                    "missing required before_image payload"
                                ),
                                lost_event_ids=(ev.event_id,),
                            )
                        )

        # Check (e): SCD2 timeline interval closure and ordering across per-key history
        captured_by_key: Dict[Any, List[CapturedHistoryEvent]] = {}
        for c in captured_history:
            captured_by_key.setdefault(c.primary_key, []).append(c)

        out_of_order = 0
        for pk, c_list in sorted(captured_by_key.items(), key=lambda x: str(x[0])):
            seqs = [c.sequence_no for c in c_list]
            if seqs != sorted(seqs):
                out_of_order += 1
                hc_anomalies.append(
                    HistoryAnomaly(
                        category=AnomalyCategory.OUT_OF_ORDER_HISTORY_SEQUENCE,
                        primary_key=pk,
                        sync_window_id=c_list[-1].sync_window_id,
                        care_badge=anomaly_badge,
                        affects_final_state=False,
                        affects_history_contract=True,
                        detail=f"captured history for key {pk!r} is out of commit order: {seqs}",
                    )
                )

            if (
                self.contract.goal == HistoryGoal.SCD2_TIMELINE
                or self.contract.tombstone_policy.require_scd2_interval_closure
            ):
                non_tombstone_versions = [c for c in c_list if not c.is_tombstone]
                for idx, ver in enumerate(non_tombstone_versions[:-1]):
                    if ver.is_current is True or ver.effective_to is None:
                        unclosed_scd2 += 1
                        hc_anomalies.append(
                            HistoryAnomaly(
                                category=AnomalyCategory.UNCLOSED_SCD2_INTERVAL,
                                primary_key=pk,
                                sync_window_id=ver.sync_window_id,
                                care_badge=anomaly_badge,
                                affects_final_state=False,
                                affects_history_contract=True,
                                detail=(
                                    f"superseded SCD2 version seq={ver.sequence_no} for key {pk!r} "
                                    "has unclosed effective_to or is_current=True"
                                ),
                            )
                        )
                # If last event for pk was a DELETE, no version should remain is_current=True
                if c_list and c_list[-1].operation == OperationType.DELETE:
                    still_open = [
                        v for v in non_tombstone_versions if v.is_current is True or v.effective_to is None
                    ]
                    if still_open:
                        unclosed_scd2 += len(still_open)
                        hc_anomalies.append(
                            HistoryAnomaly(
                                category=AnomalyCategory.UNCLOSED_SCD2_INTERVAL,
                                primary_key=pk,
                                sync_window_id=c_list[-1].sync_window_id,
                                care_badge=anomaly_badge,
                                affects_final_state=False,
                                affects_history_contract=True,
                                detail=(
                                    f"deleted key {pk!r} still has {len(still_open)} open SCD2 "
                                    "version(s) with is_current=True or effective_to=None"
                                ),
                            )
                        )

        # Check (f): Retention window gap
        retention_gap = False
        if self.contract.retention_proof is not None:
            ret_ok, _, ret_reasons = self.contract.retention_proof.evaluate()
            if not ret_ok:
                retention_gap = True
                hc_anomalies.append(
                    HistoryAnomaly(
                        category=AnomalyCategory.RETENTION_WINDOW_GAP,
                        primary_key="*",
                        sync_window_id=-1,
                        care_badge=CareBadge.ACTION,
                        affects_final_state=False,
                        affects_history_contract=True,
                        detail="; ".join(ret_reasons),
                    )
                )

        total_hc_denom = len(post_cutoff_source)
        if total_hc_denom == 0:
            hc_ratio = 1.0 if not retention_gap else 0.0
        else:
            # Penalize any missing before-images, unclosed SCD2 intervals, or out-of-order sequences
            effective_valid = max(
                0,
                captured_transitions - missing_before_images - unclosed_scd2 - out_of_order,
            )
            hc_ratio = float(effective_valid) / float(total_hc_denom)

        has_history_defects = (
            captured_transitions < total_hc_denom
            or coalesced_updates_lost > 0
            or transient_ins_del_lost > 0
            or missing_tombstones > 0
            or unclosed_scd2 > 0
            or missing_before_images > 0
            or out_of_order > 0
            or retention_gap
            or any(
                a.category == AnomalyCategory.PRE_CUTOFF_ARCHIVE_MISSING
                for a in hc_anomalies
            )
        )

        if not has_history_defects:
            hc_verdict = "same"
            hc_badge = CareBadge.PASS
        else:
            hc_verdict = "different"
            hc_badge = (
                CareBadge.ACTION
                if self.contract.requires_intermediate_history
                else CareBadge.BENIGN
            )

        hc_leaves = [
            f"{c.sequence_no}:{c.primary_key}:{c.operation.value}:"
            f"{json.dumps(_encode_typed_value(c.after_image), sort_keys=True)}:"
            f"{c.is_tombstone}:{c.is_current}"
            for c in captured_history
        ]
        hc_merkle = compute_merkle_root(hc_leaves)

        history_cov = HistoryCaptureCoverage(
            total_post_cutoff_transitions=total_hc_denom,
            captured_post_cutoff_transitions=captured_transitions,
            pre_cutoff_baseline_events=len(pre_cutoff_source),
            coalesced_intermediate_updates_lost=coalesced_updates_lost,
            transient_insert_deletes_lost=transient_ins_del_lost,
            missing_delete_tombstones=missing_tombstones,
            unclosed_scd2_intervals=unclosed_scd2,
            missing_before_images=missing_before_images,
            out_of_order_transitions=out_of_order,
            retention_gap_detected=retention_gap,
            history_coverage_ratio=hc_ratio,
            verdict=hc_verdict,
            care_badge=hc_badge,
            merkle_root=hc_merkle,
            anomalies=tuple(hc_anomalies),
        )

        # ---------------------------------------------------------------------
        # 3. Overall Contract Verdict (Goal-Driven Without Conflating Metrics)
        # ---------------------------------------------------------------------
        if self.contract.requires_intermediate_history:
            contract_satisfied = (
                mech_eval.satisfied
                and final_state_cov.verdict == "same"
                and history_cov.verdict == "same"
            )
            if contract_satisfied:
                overall_verdict = "same"
                overall_badge = (
                    CareBadge.REVIEW
                    if mech_eval.care_badge == CareBadge.REVIEW
                    else CareBadge.PASS
                )
            else:
                overall_verdict = "different"
                overall_badge = CareBadge.ACTION
        else:
            # Declared goal is FINAL_STATE only:
            # Final-state convergence + retention validity + hard-delete capture decide satisfaction.
            # Coalesced intermediate updates or transient insert-delete between polls remain
            # visible in history_capture metrics as BENIGN, never falsely called complete history,
            # while allowing the final-state migration goal to pass when final state matches.
            ret_ok = not retention_gap
            contract_satisfied = (
                mech_eval.satisfied
                and final_state_cov.verdict == "same"
                and ret_ok
            )
            if contract_satisfied:
                overall_verdict = "same"
                overall_badge = (
                    CareBadge.BENIGN
                    if history_cov.verdict == "different"
                    else CareBadge.PASS
                )
            else:
                overall_verdict = "different"
                overall_badge = CareBadge.ACTION

        return DualHistoryContractProof(
            contract=self.contract,
            mechanism=self.profile.mechanism,
            mechanism_evaluation=mech_eval,
            final_state=final_state_cov,
            history_capture=history_cov,
            overall_verdict=overall_verdict,
            overall_care_badge=overall_badge,
            contract_satisfied=contract_satisfied,
        )


# -----------------------------------------------------------------------------
# Universal Typed Durable Resume in O(1)
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class DurableHistoryContractCheckpoint:
    """Universal Typed Durable Resume in O(1) for history contract verification."""

    session_id: str
    checkpoint_dir: Path
    completed_windows: Dict[int, Dict[str, Any]] = dataclasses.field(default_factory=dict)
    last_durable_lsn: Optional[str] = None
    last_durable_cursor: Any = None
    state_version: int = 1

    def _checkpoint_file(self) -> Path:
        return self.checkpoint_dir / f"history_contract_{self.session_id}.json"

    def is_window_completed(self, window_id: int, expected_merkle: Optional[str] = None) -> bool:
        """O(1) lookup to skip already-verified sync windows without rescanning."""
        entry = self.completed_windows.get(window_id)
        if entry is None:
            return False
        if expected_merkle is not None and entry.get("merkle_root") != expected_merkle:
            return False
        return True

    def record_window_completed(
        self,
        window_id: int,
        *,
        merkle_root: str,
        durable_lsn: Optional[str] = None,
        typed_cursor: Any = None,
        final_state_verdict: str = "same",
        history_verdict: str = "same",
    ) -> None:
        """Record completed sync window with native type preservation."""
        self.completed_windows[window_id] = {
            "window_id": window_id,
            "merkle_root": merkle_root,
            "durable_lsn": durable_lsn,
            "typed_cursor": typed_cursor,
            "final_state_verdict": final_state_verdict,
            "history_verdict": history_verdict,
            "completed_at": time.time(),
        }
        if durable_lsn is not None:
            self.last_durable_lsn = durable_lsn
        if typed_cursor is not None:
            self.last_durable_cursor = typed_cursor

    def save(self) -> Path:
        """Atomically persist checkpoint preserving datetime, Decimal, UUID, and composite tuple keys."""
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "session_id": self.session_id,
            "state_version": self.state_version,
            "last_durable_lsn": self.last_durable_lsn,
            "last_durable_cursor": _encode_typed_value(self.last_durable_cursor),
            "completed_windows": {
                str(k): _encode_typed_value(v) for k, v in self.completed_windows.items()
            },
            "updated_at": time.time(),
        }
        target = self._checkpoint_file()
        tmp = self.checkpoint_dir / f".history_contract_{self.session_id}.{uuid.uuid4().hex[:8]}.tmp"
        raw = json.dumps(payload, indent=2, ensure_ascii=True)
        try:
            tmp.write_text(raw, encoding="ascii")
            os.replace(tmp, target)
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
        return target

    @classmethod
    def load(
        cls,
        checkpoint_dir: Union[str, Path],
        session_id: str,
    ) -> "DurableHistoryContractCheckpoint":
        """Load durable checkpoint in O(1) restoring exact native Python types."""
        cp_dir = Path(checkpoint_dir)
        target = cp_dir / f"history_contract_{session_id}.json"
        if not target.is_file():
            return cls(session_id=session_id, checkpoint_dir=cp_dir)
        raw = target.read_text(encoding="ascii")
        data = json.loads(raw, object_hook=_decode_typed_hook)
        windows_raw = data.get("completed_windows", {})
        completed_windows = {int(k): v for k, v in windows_raw.items()}
        return cls(
            session_id=session_id,
            checkpoint_dir=cp_dir,
            completed_windows=completed_windows,
            last_durable_lsn=data.get("last_durable_lsn"),
            last_durable_cursor=data.get("last_durable_cursor"),
            state_version=int(data.get("state_version", 1)),
        )


# -----------------------------------------------------------------------------
# Mutual Exclusion & Active Verification Lease Lock (.run.lock)
# -----------------------------------------------------------------------------

class HistoryContractLease:
    """Active verification lease lock (.run.lock) with heartbeat preventing overlapping runs."""

    def __init__(self, lock_dir: Union[str, Path], ttl_seconds: float = 30.0) -> None:
        self.lock_dir = Path(lock_dir)
        self.lock_file = self.lock_dir / ".run.lock"
        self.ttl_seconds = ttl_seconds
        self.owner_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.acquired = False

    def acquire(self) -> "HistoryContractLease":
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        now = time.time()

        if self.lock_file.exists():
            try:
                data = json.loads(self.lock_file.read_text(encoding="ascii"))
                hb = float(data.get("heartbeat", 0.0))
                owner = data.get("owner", "unknown")
                if now - hb < self.ttl_seconds:
                    raise LeaseLockError(
                        f"active verification lease held by '{owner}' "
                        f"(age={now - hb:.1f}s < ttl={self.ttl_seconds:.1f}s)"
                    )
            except (json.JSONDecodeError, OSError, ValueError) as exc:
                if isinstance(exc, LeaseLockError):
                    raise
                raise LeaseLockError(f"corrupt active lease file {self.lock_file}: {exc}") from exc

        self._write_lease(now)
        self.acquired = True
        return self

    def heartbeat(self) -> None:
        if not self.acquired or not self.lock_file.exists():
            raise LeaseLockError("cannot heartbeat: lease not currently held")
        data = json.loads(self.lock_file.read_text(encoding="ascii"))
        if data.get("owner") != self.owner_id:
            raise LeaseLockError("cannot heartbeat: lease ownership lost")
        self._write_lease(time.time())

    def _write_lease(self, timestamp: float) -> None:
        payload = {
            "owner": self.owner_id,
            "pid": os.getpid(),
            "heartbeat": timestamp,
            "ttl": self.ttl_seconds,
        }
        tmp = self.lock_dir / f".run.lock.{uuid.uuid4().hex[:8]}.tmp"
        try:
            tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="ascii")
            os.replace(tmp, self.lock_file)
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    def release(self) -> None:
        if not self.acquired:
            return
        if self.lock_file.exists():
            try:
                data = json.loads(self.lock_file.read_text(encoding="ascii"))
                if data.get("owner") == self.owner_id:
                    self.lock_file.unlink(missing_ok=True)
            except (json.JSONDecodeError, OSError):
                pass
        self.acquired = False

    def __enter__(self) -> "HistoryContractLease":
        return self.acquire()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# Adaptive AIMD Batch Sizing & Byte-Weighted Cold-Start ETA
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class AdaptiveHistoryBatchController:
    """Dynamic AIMD batch sizing and instant cold-start byte-weighted ETA."""

    current_batch_events: int = 1000
    min_batch_events: int = 100
    max_batch_events: int = 50000
    additive_increase: int = 500
    multiplicative_decrease: float = 0.5
    target_latency_ms: float = 250.0
    cold_start_throughput_bytes_sec: float = 25 * 1024 * 1024  # 25 MB/s baseline
    observed_bytes_processed: int = 0
    observed_elapsed_seconds: float = 0.0

    def record_batch(
        self,
        events_processed: int,
        bytes_processed: int,
        latency_ms: float,
        *,
        throttled_or_error: bool = False,
    ) -> int:
        """Adjust batch size via AIMD and update byte-weighted throughput."""
        if events_processed > 0 and bytes_processed > 0 and latency_ms > 0:
            self.observed_bytes_processed += bytes_processed
            self.observed_elapsed_seconds += latency_ms / 1000.0

        if throttled_or_error or latency_ms > self.target_latency_ms:
            reduced = int(self.current_batch_events * self.multiplicative_decrease)
            self.current_batch_events = max(self.min_batch_events, reduced)
        else:
            increased = self.current_batch_events + self.additive_increase
            self.current_batch_events = min(self.max_batch_events, increased)

        return self.current_batch_events

    def estimate_eta_seconds(self, remaining_bytes: int) -> float:
        """Compute instant byte-weighted ETA blending cold-start baseline with observed rate."""
        if remaining_bytes <= 0:
            return 0.0
        if self.observed_elapsed_seconds <= 0.0 or self.observed_bytes_processed <= 0:
            rate = self.cold_start_throughput_bytes_sec
        else:
            observed_rate = float(self.observed_bytes_processed) / self.observed_elapsed_seconds
            # Weight observed rate increasingly as more bytes are verified
            weight = min(1.0, float(self.observed_bytes_processed) / float(10 * 1024 * 1024))
            rate = (weight * observed_rate) + ((1.0 - weight) * self.cold_start_throughput_bytes_sec)
        return float(remaining_bytes) / max(1.0, rate)


# -----------------------------------------------------------------------------
# Full-Capacity External Tool Wrapper (Zero Internal Tool Leaks)
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class NormalizedToolHistoryResult:
    """Normalized Migkit envelope for external replication/verification tools."""

    tool_name: str
    mapped_mechanism: CaptureMechanism
    supports_complete_history: bool
    supports_final_state: bool
    care_badge: CareBadge
    status: str
    summary: str


class HistoryToolCapabilityWrapper:
    """Wraps external tools (Debezium, DMS, DTS, Datafold, DVT, reladiff, pgcopydb)
    with zero internal tool exception or branding leaks."""

    TOOL_CAPABILITY_MATRIX: Dict[str, Tuple[CaptureMechanism, bool, bool]] = {
        "debezium": (CaptureMechanism.LOG_BASED_CDC, True, True),
        "dms_cdc": (CaptureMechanism.LOG_BASED_CDC, True, True),
        "dts_cdc": (CaptureMechanism.LOG_BASED_CDC, True, True),
        "fivetran_history_scd2": (CaptureMechanism.LOG_BASED_CDC, True, True),
        "sqlserver_ct": (CaptureMechanism.CHANGE_TRACKING_CT, False, True),
        "pgcopydb_snapshot": (CaptureMechanism.PERIODIC_SNAPSHOT, False, True),
        "datafold_diff": (CaptureMechanism.PERIODIC_SNAPSHOT, False, True),
        "dvt_table_compare": (CaptureMechanism.PERIODIC_SNAPSHOT, False, True),
        "reladiff_compare": (CaptureMechanism.PERIODIC_SNAPSHOT, False, True),
    }

    @classmethod
    def evaluate_tool_for_goal(
        cls,
        tool_name: str,
        goal: HistoryGoal,
        runner: Optional[Callable[[], Dict[str, Any]]] = None,
    ) -> NormalizedToolHistoryResult:
        """Verify tool capability against declared HistoryGoal and sanitize execution errors."""
        key = (tool_name or "").strip().lower()
        if key not in cls.TOOL_CAPABILITY_MATRIX:
            return NormalizedToolHistoryResult(
                tool_name="migkit_history_adapter",
                mapped_mechanism=CaptureMechanism.PERIODIC_SNAPSHOT,
                supports_complete_history=False,
                supports_final_state=False,
                care_badge=CareBadge.ACTION,
                status="unsupported_adapter",
                summary="requested external adapter is not registered in capability matrix",
            )

        mech, supports_hist, supports_state = cls.TOOL_CAPABILITY_MATRIX[key]
        needs_hist = goal in (
            HistoryGoal.COMPLETE_HISTORY,
            HistoryGoal.SCD2_TIMELINE,
            HistoryGoal.WINDOWED_HISTORY,
        )

        if needs_hist and not supports_hist:
            return NormalizedToolHistoryResult(
                tool_name=key,
                mapped_mechanism=mech,
                supports_complete_history=supports_hist,
                supports_final_state=supports_state,
                care_badge=CareBadge.ACTION,
                status="refused_capability_mismatch",
                summary=(
                    f"adapter mapped to '{mech.value}' proves final state only and "
                    f"cannot satisfy history goal '{goal.value}'"
                ),
            )

        if runner is not None:
            try:
                raw_out = runner()
                ok = bool(raw_out.get("ok", True))
                return NormalizedToolHistoryResult(
                    tool_name=key,
                    mapped_mechanism=mech,
                    supports_complete_history=supports_hist,
                    supports_final_state=supports_state,
                    care_badge=CareBadge.PASS if ok else CareBadge.ACTION,
                    status="completed" if ok else "verification_mismatch",
                    summary=str(raw_out.get("summary", "normalized execution completed")),
                )
            except Exception as exc:
                # Sanitize internal tool tracebacks/names to prevent leaks
                err_class = type(exc).__name__
                return NormalizedToolHistoryResult(
                    tool_name=key,
                    mapped_mechanism=mech,
                    supports_complete_history=supports_hist,
                    supports_final_state=supports_state,
                    care_badge=CareBadge.ACTION,
                    status="normalized_adapter_error",
                    summary=f"normalized adapter execution failure ({err_class})",
                )

        return NormalizedToolHistoryResult(
            tool_name=key,
            mapped_mechanism=mech,
            supports_complete_history=supports_hist,
            supports_final_state=supports_state,
            care_badge=CareBadge.PASS,
            status="capable",
            summary=f"adapter '{key}' ({mech.value}) satisfies goal '{goal.value}'",
        )
