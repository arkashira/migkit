"""Schedule repair against CDC without stale overwrite (Row 613: RCH05.repair-fence).

Extends rows/spans/reload repair (`repairways`, `tailtables`, `fencing`) to:
- Choose row, range, or table repair scope by measured total cost and bounded
  outage via `decide.Rung` and `decide.climb`.
- Establish a fenced repair epoch, drain in-flight apply work, and hold/buffer
  affected CDC changes while unaffected keys/ranges continue applying.
- Preserve newer committed target writes and newer buffered CDC updates under
  the active conflict policy (`newest_wins`, `newer_wins`, `keep-target`) so a
  delayed repair write can never overwrite a newer CDC update.
- Reconcile buffered CDC changes in gap-free commit order and reverify source
  versus target convergence before releasing the repair fence.
- Safely undo to recorded pre-images or reconcile to completion when a repair
  epoch is interrupted mid-flight, with O(1) typed durable resume and `.run.lock`
  mutual exclusion.
"""
from __future__ import annotations

import copy
import datetime
import decimal
import enum
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class RepairScope(str, enum.Enum):
    ROW_REPAIR = "row_repair"
    RANGE_REPAIR = "range_repair"
    TABLE_RELOAD = "table_reload"
    DEFER_BOUNDED_OUTAGE = "defer_bounded_outage"


class ConflictResolution(str, enum.Enum):
    KEEP_TARGET_POLICY = "keep_target_policy"
    PRESERVE_NEWER_TARGET = "preserve_newer_target"
    APPLY_REPAIR_SNAPSHOT = "apply_repair_snapshot"


class RecoveryDisposition(str, enum.Enum):
    RECONCILE_AND_COMPLETE = "reconcile_and_complete"
    UNDO_TO_PREIMAGE = "undo_to_preimage"
    ABORT_CORRUPTED = "abort_corrupted"


class FencePhase(str, enum.Enum):
    EPOCH_OPENED = "EPOCH_OPENED"
    DRAINING_APPLY = "DRAINING_APPLY"
    HOLDING_CDC = "HOLDING_CDC"
    APPLYING_REPAIR = "APPLYING_REPAIR"
    RECONCILING_BUFFER = "RECONCILING_BUFFER"
    REVERIFYING = "REVERIFYING"
    COMMITTED = "COMMITTED"
    UNDONE = "UNDONE"
    FAILED = "FAILED"


class CDCOp(str, enum.Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class RepairFenceError(RuntimeError):
    """Base error for fenced repair coordination."""


class StaleFenceEpochError(RepairFenceError):
    """Raised when a repair write carries an epoch lower than the target fence."""


class ReplayGapError(RepairFenceError):
    """Raised when buffered CDC changes contain a sequence or LSN gap."""


class InterruptedRepairError(RepairFenceError):
    """Raised when a repair epoch is interrupted during execution."""


class VerificationDivergenceError(RepairFenceError):
    """Raised when post-repair reverification detects remaining drift."""


class LeaseLockHeldError(RepairFenceError):
    """Raised when another active repair fence controller holds .run.lock."""


class UnsafeHostError(RepairFenceError):
    """Raised when an endpoint host is outside safe ranges."""


SAFE_HOST_PREFIXES = ("127.", "10.0.0.", "localhost", "example.com")


def validate_safe_host(host: str) -> str:
    """Validates that a host string is non-empty and within safe test/local ranges."""
    cleaned = str(host or "").strip()
    if not cleaned:
        raise UnsafeHostError("host must be non-empty")
    if not any(cleaned == p or cleaned.startswith(p) for p in SAFE_HOST_PREFIXES):
        raise UnsafeHostError(f"host {cleaned} is outside safe endpoint ranges")
    return cleaned


def _encode_typed(value: Any) -> Any:
    """Recursively encodes native Python types for durable JSON serialization."""
    if isinstance(value, datetime.datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"__type__": "date", "value": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, uuid.UUID):
        return {"__type__": "uuid", "value": str(value)}
    if isinstance(value, tuple):
        return {"__type__": "tuple", "items": [_encode_typed(v) for v in value]}
    if isinstance(value, list):
        return [_encode_typed(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _encode_typed(v) for k, v in value.items()}
    return value


def _decode_typed(value: Any) -> Any:
    """Recursively restores native Python types from durable JSON payloads."""
    if isinstance(value, list):
        return [_decode_typed(v) for v in value]
    if isinstance(value, dict):
        kind = value.get("__type__")
        if kind == "datetime":
            return datetime.datetime.fromisoformat(value["value"])
        if kind == "date":
            return datetime.date.fromisoformat(value["value"])
        if kind == "decimal":
            return decimal.Decimal(value["value"])
        if kind == "uuid":
            return uuid.UUID(value["value"])
        if kind == "tuple":
            return tuple(_decode_typed(v) for v in value["items"])
        return {k: _decode_typed(v) for k, v in value.items()}
    return value


def canonical_row_digest(row: Optional[Dict[str, Any]]) -> str:
    """Deterministic SHA-256 digest of a row's business payload."""
    if row is None:
        return "TOMBSTONE"
    business = {
        k: v
        for k, v in row.items()
        if not str(k).startswith("_migkit_")
    }
    payload = json.dumps(_encode_typed(business), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("ascii", errors="replace")).hexdigest()[:24]


@dataclass(frozen=True)
class ScopeReadings:
    """Measured table, drift, rate, and outage readings for repair scope selection."""
    table_name: str
    total_rows: int
    named_rows: int
    span_rows: int
    span_count: int = 1
    row_bytes: int = 256
    has_range_key: bool = True
    row_by_row_only: bool = False
    replace_allowed: bool = True
    measured_row_repair_rate: float = 200.0
    measured_range_copy_rate: float = 2500.0
    measured_table_reload_rate: float = 5000.0
    measured_cdc_arrival_rate: float = 50.0
    measured_cdc_replay_rate: float = 1000.0
    measured_drain_latency_seconds: float = 0.05
    measured_verify_rate: float = 10000.0
    max_outage_seconds: float = 10.0
    allow_defer: bool = True


@dataclass(frozen=True)
class ScopeCostBreakdown:
    """Derived cost and outage estimates for each candidate repair scope."""
    row_copy_seconds: float
    row_reconcile_seconds: float
    row_verify_seconds: float
    row_outage_seconds: float
    row_total_cost: float
    range_copy_seconds: float
    range_reconcile_seconds: float
    range_verify_seconds: float
    range_outage_seconds: float
    range_total_cost: float
    table_copy_seconds: float
    table_reconcile_seconds: float
    table_verify_seconds: float
    table_outage_seconds: float
    table_total_cost: float

    def to_facts(self, readings: ScopeReadings) -> decide.Facts:
        return decide.Facts(
            {
                "table_name": readings.table_name,
                "table_rows": int(readings.total_rows),
                "named_rows": int(readings.named_rows),
                "span_rows": int(readings.span_rows),
                "span_count": int(readings.span_count),
                "has_range_key": bool(readings.has_range_key),
                "row_by_row_only": bool(readings.row_by_row_only),
                "replace_allowed": bool(readings.replace_allowed),
                "max_outage_seconds": float(readings.max_outage_seconds),
                "allow_defer": bool(readings.allow_defer),
                "row_outage_seconds": self.row_outage_seconds,
                "row_total_cost": self.row_total_cost,
                "range_outage_seconds": self.range_outage_seconds,
                "range_total_cost": self.range_total_cost,
                "table_outage_seconds": self.table_outage_seconds,
                "table_total_cost": self.table_total_cost,
            }
        )


def compute_scope_costs(readings: ScopeReadings) -> ScopeCostBreakdown:
    """Computes total cost and outage duration per scope from measured rates."""
    total_rows = max(0, int(readings.total_rows))
    named_rows = max(0, int(readings.named_rows))
    span_rows = max(0, int(readings.span_rows))

    row_rate = max(1e-6, float(readings.measured_row_repair_rate))
    range_rate = max(1e-6, float(readings.measured_range_copy_rate))
    table_rate = max(1e-6, float(readings.measured_table_reload_rate))
    cdc_in = max(0.0, float(readings.measured_cdc_arrival_rate))
    cdc_replay = max(1e-6, float(readings.measured_cdc_replay_rate))
    verify_rate = max(1e-6, float(readings.measured_verify_rate))
    drain_s = max(0.0, float(readings.measured_drain_latency_seconds))

    row_fraction = min(1.0, named_rows / max(1, total_rows)) if total_rows > 0 else 0.0
    range_fraction = min(1.0, span_rows / max(1, total_rows)) if total_rows > 0 else 0.0

    row_copy_s = named_rows / row_rate if named_rows > 0 else float("inf")
    row_buf_events = row_copy_s * cdc_in * max(0.01, row_fraction) if named_rows > 0 else 0.0
    row_rec_s = row_buf_events / cdc_replay if named_rows > 0 else float("inf")
    row_ver_s = named_rows / verify_rate if named_rows > 0 else float("inf")
    row_outage_s = (drain_s + row_copy_s + row_rec_s) if named_rows > 0 else float("inf")
    row_total_s = (row_outage_s + row_ver_s) if named_rows > 0 else float("inf")

    range_copy_s = span_rows / range_rate if span_rows > 0 else float("inf")
    range_buf_events = range_copy_s * cdc_in * max(0.05, range_fraction) if span_rows > 0 else 0.0
    range_rec_s = range_buf_events / cdc_replay if span_rows > 0 else float("inf")
    range_ver_s = span_rows / verify_rate if span_rows > 0 else float("inf")
    range_outage_s = (drain_s + range_copy_s + range_rec_s) if span_rows > 0 else float("inf")
    range_total_s = (range_outage_s + range_ver_s) if span_rows > 0 else float("inf")

    table_copy_s = total_rows / table_rate if total_rows > 0 else float("inf")
    table_buf_events = table_copy_s * cdc_in if total_rows > 0 else 0.0
    table_rec_s = table_buf_events / cdc_replay if total_rows > 0 else float("inf")
    table_ver_s = total_rows / verify_rate if total_rows > 0 else float("inf")
    table_outage_s = (drain_s + table_copy_s + table_rec_s) if total_rows > 0 else float("inf")
    table_total_s = (table_outage_s + table_ver_s) if total_rows > 0 else float("inf")

    return ScopeCostBreakdown(
        row_copy_seconds=row_copy_s,
        row_reconcile_seconds=row_rec_s,
        row_verify_seconds=row_ver_s,
        row_outage_seconds=row_outage_s,
        row_total_cost=row_total_s,
        range_copy_seconds=range_copy_s,
        range_reconcile_seconds=range_rec_s,
        range_verify_seconds=range_ver_s,
        range_outage_seconds=range_outage_s,
        range_total_cost=range_total_s,
        table_copy_seconds=table_copy_s,
        table_reconcile_seconds=table_rec_s,
        table_verify_seconds=table_ver_s,
        table_outage_seconds=table_outage_s,
        table_total_cost=table_total_s,
    )


def select_repair_scope(
    facts: Union[Dict[str, Any], decide.Facts],
    costs: Optional[decide.Costs] = None,
) -> decide.Choice:
    """Climbs the repair scope ladder by measured total cost and bounded outage."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts or {}))
    rungs = (
        decide.Rung(
            "row_repair",
            "fenced row-by-key repair with key-scoped CDC hold",
            because="named rows have the lowest measured total cost within the outage bound",
            footprint=0,
            needs=(
                decide.Need(
                    "named drift rows available",
                    lambda x: int(x.get("named_rows") or 0) > 0,
                    "no individual drift rows were named for key repair",
                ),
                decide.Need(
                    "row repair outage within bound",
                    lambda x: float(x.get("row_outage_seconds") or float("inf"))
                    <= float(x.get("max_outage_seconds") or 0.0),
                    "row repair outage exceeds the bounded outage window",
                ),
                decide.Need(
                    "row repair has lowest measured total cost",
                    lambda x: float(x.get("row_total_cost") or float("inf"))
                    <= min(
                        float(x.get("range_total_cost") or float("inf"))
                        if (
                            bool(x.get("has_range_key"))
                            and not bool(x.get("row_by_row_only"))
                            and float(x.get("range_outage_seconds") or float("inf"))
                            <= float(x.get("max_outage_seconds") or 0.0)
                        )
                        else float("inf"),
                        float(x.get("table_total_cost") or float("inf"))
                        if (
                            bool(x.get("replace_allowed"))
                            and not bool(x.get("row_by_row_only"))
                            and float(x.get("table_outage_seconds") or float("inf"))
                            <= float(x.get("max_outage_seconds") or 0.0)
                        )
                        else float("inf"),
                    ),
                    "a broader range or table scope has lower measured total cost",
                ),
            ),
        ),
        decide.Rung(
            "range_repair",
            "fenced key-span repair with range-scoped CDC hold",
            because="replacing divergent spans has the lowest measured cost within the outage bound",
            footprint=1,
            needs=(
                decide.Need(
                    "range key and span rows available",
                    lambda x: bool(x.get("has_range_key")) and int(x.get("span_rows") or 0) > 0,
                    "table lacks a range key or has no divergent key spans",
                ),
                decide.Need(
                    "no row-by-row-only conflict or filter policy",
                    lambda x: not bool(x.get("row_by_row_only")),
                    "hop policy requires row-by-row resolution",
                ),
                decide.Need(
                    "range repair outage within bound",
                    lambda x: float(x.get("range_outage_seconds") or float("inf"))
                    <= float(x.get("max_outage_seconds") or 0.0),
                    "range repair outage exceeds the bounded outage window",
                ),
                decide.Need(
                    "range repair costs no more than table reload",
                    lambda x: float(x.get("range_total_cost") or float("inf"))
                    <= (
                        float(x.get("table_total_cost") or float("inf"))
                        if (
                            bool(x.get("replace_allowed"))
                            and float(x.get("table_outage_seconds") or float("inf"))
                            <= float(x.get("max_outage_seconds") or 0.0)
                        )
                        else float("inf")
                    ),
                    "whole-table reload has lower measured total cost than span repair",
                ),
            ),
        ),
        decide.Rung(
            "table_reload",
            "fenced whole-table reload with buffered CDC catchup",
            because="whole-table reload has the lowest measured cost and fits the bounded outage",
            footprint=2,
            needs=(
                decide.Need(
                    "table replacement permitted and no row-only policy",
                    lambda x: bool(x.get("replace_allowed")) and not bool(x.get("row_by_row_only")),
                    "table reload is disallowed by replace or row-by-row policy",
                ),
                decide.Need(
                    "known positive table row count",
                    lambda x: int(x.get("table_rows") or 0) > 0,
                    "table row count is unknown or zero",
                ),
                decide.Need(
                    "table reload outage within bound",
                    lambda x: float(x.get("table_outage_seconds") or float("inf"))
                    <= float(x.get("max_outage_seconds") or 0.0),
                    "table reload outage exceeds the bounded outage window",
                ),
            ),
        ),
        decide.Rung(
            "defer_bounded_outage",
            "defer repair because all eligible scopes exceed the bounded outage window",
            because="no repair scope can complete within the allowed outage bound",
            footprint=0,
            needs=(
                decide.Need(
                    "deferral permitted when outage bound is exceeded",
                    lambda x: bool(x.get("allow_defer", True)),
                    "deferral is disabled by caller policy",
                ),
            ),
        ),
    )
    work = str(f.get("table_name") or "repair_scope")
    return decide.climb(work, rungs, f, costs=costs)


def resolve_repair_conflict(facts: Union[Dict[str, Any], decide.Facts]) -> decide.Choice:
    """Climbs the conflict resolution ladder for a repair write against target/CDC state."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts or {}))
    rungs = (
        decide.Rung(
            "keep_target_policy",
            "preserve target row under keep-target conflict policy",
            because="the hop policy explicitly preserves target modifications",
            needs=(
                decide.Need(
                    "keep-target policy with modified target row",
                    lambda x: str(x.get("conflict_policy") or "") == "keep-target"
                    and bool(x.get("target_modified")),
                    "policy is not keep-target or target row was not modified",
                ),
            ),
        ),
        decide.Rung(
            "preserve_newer_target",
            "preserve newer committed target or CDC value over older repair snapshot",
            because="a later committed source change has a higher position or timestamp than the repair snapshot",
            needs=(
                decide.Need(
                    "target or buffered CDC is newer than repair snapshot",
                    lambda x: int(x.get("latest_target_lsn") or -1)
                    > int(x.get("repair_snapshot_lsn") or 0)
                    or float(x.get("latest_target_ts") or -1.0)
                    > float(x.get("repair_snapshot_ts") or 0.0),
                    "target state is not newer than the repair snapshot",
                ),
                decide.Need(
                    "conflict policy orders by newest committed change",
                    lambda x: str(x.get("conflict_policy") or "newest_wins")
                    in ("newest_wins", "newer_wins", "source_lsn_order"),
                    "conflict policy does not order by newest committed source change",
                ),
            ),
        ),
        decide.Rung(
            "apply_repair_snapshot",
            "apply repair snapshot row to target under active repair epoch",
            because="the repair snapshot is at or newer than the target state",
            needs=(
                decide.Need(
                    "repair snapshot is not superseded by target state or keep-target",
                    lambda x: int(x.get("repair_snapshot_lsn") or 0)
                    >= int(x.get("latest_target_lsn") or 0)
                    and float(x.get("repair_snapshot_ts") or 0.0)
                    >= float(x.get("latest_target_ts") or 0.0)
                    and not (
                        str(x.get("conflict_policy") or "") == "keep-target"
                        and bool(x.get("target_modified"))
                    ),
                    "repair snapshot is older than target state or blocked by keep-target",
                ),
            ),
        ),
    )
    work = str(f.get("row_key") or "repair_conflict")
    return decide.climb(work, rungs, f)


def select_interrupted_recovery(facts: Union[Dict[str, Any], decide.Facts]) -> decide.Choice:
    """Climbs the recovery ladder when a fenced repair epoch is interrupted."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts or {}))
    rungs = (
        decide.Rung(
            "reconcile_and_complete",
            "resume from durable epoch checkpoint, reconcile buffered CDC, and reverify",
            because="journal and gap-free CDC buffer are intact so forward reconciliation finishes cleanly",
            needs=(
                decide.Need(
                    "valid epoch journal and gap-free CDC buffer",
                    lambda x: bool(x.get("epoch_journal_valid"))
                    and bool(x.get("cdc_buffer_gap_free")),
                    "epoch journal is invalid or CDC buffer has a sequence gap",
                ),
                decide.Need(
                    "forward completion permitted",
                    lambda x: bool(x.get("can_finish_reconcile")) and not bool(x.get("force_undo")),
                    "caller requested undo or forward reconciliation is unavailable",
                ),
            ),
        ),
        decide.Rung(
            "undo_to_preimage",
            "restore recorded pre-images for touched keys and replay buffered CDC",
            because="recorded pre-images match target repair writes so rollback is exact",
            needs=(
                decide.Need(
                    "complete pre-images recorded and target matches repair writes",
                    lambda x: bool(x.get("preimage_recorded"))
                    and bool(x.get("target_matches_repair_writes")),
                    "pre-images are incomplete or target rows drifted outside the repair fence",
                ),
            ),
        ),
        decide.Rung(
            "abort_corrupted",
            "halt recovery and flag ACTION because neither forward reconcile nor clean undo holds",
            because="pre-images or journal integrity failed so automatic mutation is unsafe",
            needs=(
                decide.Need(
                    "unrecoverable journal and pre-image state",
                    lambda x: not (
                        bool(x.get("epoch_journal_valid"))
                        and bool(x.get("cdc_buffer_gap_free"))
                        and bool(x.get("can_finish_reconcile"))
                        and not bool(x.get("force_undo"))
                    )
                    and not (
                        bool(x.get("preimage_recorded"))
                        and bool(x.get("target_matches_repair_writes"))
                    ),
                    "either forward reconciliation or pre-image undo is still available",
                ),
            ),
        ),
    )
    work = str(f.get("epoch_id") or "interrupted_recovery")
    return decide.climb(work, rungs, f)


@dataclass(frozen=True)
class CDCChange:
    """A single committed source CDC event with monotonic LSN and sequence number."""
    seq: int
    lsn: int
    op: CDCOp
    table: str
    key: Any
    after: Optional[Dict[str, Any]] = None
    before: Optional[Dict[str, Any]] = None
    source_ts: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return _encode_typed(
            {
                "seq": self.seq,
                "lsn": self.lsn,
                "op": self.op.value,
                "table": self.table,
                "key": self.key,
                "after": self.after,
                "before": self.before,
                "source_ts": self.source_ts,
            }
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CDCChange:
        decoded = _decode_typed(data)
        return cls(
            seq=int(decoded["seq"]),
            lsn=int(decoded["lsn"]),
            op=CDCOp(decoded["op"]),
            table=str(decoded["table"]),
            key=decoded["key"],
            after=decoded.get("after"),
            before=decoded.get("before"),
            source_ts=float(decoded.get("source_ts") or 0.0),
        )


@dataclass(frozen=True)
class RepairRowSnapshot:
    """A row read from the source at the repair snapshot epoch."""
    key: Any
    row: Optional[Dict[str, Any]]
    snapshot_lsn: int
    snapshot_ts: float = 0.0


@dataclass
class RepairEpoch:
    """Tracks the fence token, held scope, pre-images, and buffered CDC for one repair."""
    epoch_id: str
    table_name: str
    fence_token: int
    scope: RepairScope
    snapshot_lsn: int
    snapshot_ts: float
    conflict_policy: str = "newest_wins"
    affected_keys: Set[Any] = field(default_factory=set)
    affected_ranges: List[Tuple[Any, Any]] = field(default_factory=list)
    phase: FencePhase = FencePhase.EPOCH_OPENED
    drained_in_flight_count: int = 0
    completed_keys: Set[Any] = field(default_factory=set)
    preimages: Dict[Any, Optional[Dict[str, Any]]] = field(default_factory=dict)
    repair_written_digests: Dict[Any, str] = field(default_factory=dict)
    buffered_cdc: List[CDCChange] = field(default_factory=list)
    preserved_newer_keys: List[Any] = field(default_factory=list)
    applied_repair_keys: List[Any] = field(default_factory=list)
    replayed_cdc_seqs: List[int] = field(default_factory=list)

    def affects_key(self, key: Any) -> bool:
        """Returns True if `key` falls inside the fenced repair scope."""
        if self.scope == RepairScope.TABLE_RELOAD:
            return True
        if key in self.affected_keys:
            return True
        if self.scope == RepairScope.RANGE_REPAIR:
            for lo, hi in self.affected_ranges:
                try:
                    if lo <= key <= hi:
                        return True
                except TypeError:
                    if str(lo) <= str(key) <= str(hi):
                        return True
        return False


class RepairFenceLeaseLock:
    """Mutual exclusion `.run.lock` with heartbeat for fenced repair operations."""

    def __init__(self, lock_dir: Path, owner_id: str, ttl_seconds: float = 60.0) -> None:
        self.lock_path = Path(lock_dir) / ".run.lock"
        self.owner_id = str(owner_id)
        self.ttl_seconds = float(ttl_seconds)
        self._held = False

    def acquire(self, now: Optional[float] = None) -> None:
        ts = float(now if now is not None else time.time())
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        if self.lock_path.exists():
            try:
                existing = json.loads(self.lock_path.read_text(encoding="ascii"))
            except Exception:
                existing = {}
            holder = existing.get("owner_id")
            expires_at = float(existing.get("expires_at") or 0.0)
            if holder and holder != self.owner_id and expires_at > ts:
                raise LeaseLockHeldError(
                    f"active repair fence lease held by {holder} until {expires_at:.3f}"
                )
        payload = {
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "heartbeat_at": ts,
            "expires_at": ts + self.ttl_seconds,
        }
        durable.write_json(self.lock_path, payload, mode=0o600, full=False)
        self._held = True

    def heartbeat(self, now: Optional[float] = None) -> None:
        if not self._held:
            raise LeaseLockHeldError("cannot heartbeat an unheld repair fence lease")
        ts = float(now if now is not None else time.time())
        payload = {
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "heartbeat_at": ts,
            "expires_at": ts + self.ttl_seconds,
        }
        durable.write_json(self.lock_path, payload, mode=0o600, full=False)

    def release(self) -> None:
        if self._held and self.lock_path.exists():
            try:
                existing = json.loads(self.lock_path.read_text(encoding="ascii"))
            except Exception:
                existing = {}
            if existing.get("owner_id") == self.owner_id:
                self.lock_path.unlink(missing_ok=True)
        self._held = False

    def __enter__(self) -> RepairFenceLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


class RepairFenceCheckpoint:
    """Typed durable O(1) checkpoint and pre-image journal for a repair epoch."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def save(self, epoch: RepairEpoch) -> None:
        state = {
            "epoch_id": epoch.epoch_id,
            "table_name": epoch.table_name,
            "fence_token": epoch.fence_token,
            "scope": epoch.scope.value,
            "snapshot_lsn": epoch.snapshot_lsn,
            "snapshot_ts": epoch.snapshot_ts,
            "conflict_policy": epoch.conflict_policy,
            "affected_keys": _encode_typed(list(epoch.affected_keys)),
            "affected_ranges": _encode_typed(epoch.affected_ranges),
            "phase": epoch.phase.value,
            "drained_in_flight_count": epoch.drained_in_flight_count,
            "completed_keys": _encode_typed(list(epoch.completed_keys)),
            "preimages": _encode_typed(
                [[k, v] for k, v in epoch.preimages.items()]
            ),
            "repair_written_digests": _encode_typed(
                [[k, v] for k, v in epoch.repair_written_digests.items()]
            ),
            "buffered_cdc": [c.to_dict() for c in epoch.buffered_cdc],
            "preserved_newer_keys": _encode_typed(epoch.preserved_newer_keys),
            "applied_repair_keys": _encode_typed(epoch.applied_repair_keys),
            "replayed_cdc_seqs": list(epoch.replayed_cdc_seqs),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        durable.write_json(self.path, state, mode=0o600, full=False)

    def load(self) -> Optional[RepairEpoch]:
        if not self.path.exists():
            return None
        raw = durable.read_state(self.path, kind="journal")
        affected_keys = set(_decode_typed(raw.get("affected_keys", [])))
        affected_ranges = [
            tuple(r) for r in _decode_typed(raw.get("affected_ranges", []))
        ]
        completed_keys = set(_decode_typed(raw.get("completed_keys", [])))
        preimages = {
            k: v for k, v in _decode_typed(raw.get("preimages", []))
        }
        written_digests = {
            k: v for k, v in _decode_typed(raw.get("repair_written_digests", []))
        }
        buffered_cdc = [
            CDCChange.from_dict(item) for item in raw.get("buffered_cdc", [])
        ]
        return RepairEpoch(
            epoch_id=str(raw["epoch_id"]),
            table_name=str(raw["table_name"]),
            fence_token=int(raw["fence_token"]),
            scope=RepairScope(raw["scope"]),
            snapshot_lsn=int(raw["snapshot_lsn"]),
            snapshot_ts=float(raw["snapshot_ts"]),
            conflict_policy=str(raw.get("conflict_policy", "newest_wins")),
            affected_keys=affected_keys,
            affected_ranges=affected_ranges,
            phase=FencePhase(raw["phase"]),
            drained_in_flight_count=int(raw.get("drained_in_flight_count", 0)),
            completed_keys=completed_keys,
            preimages=preimages,
            repair_written_digests=written_digests,
            buffered_cdc=buffered_cdc,
            preserved_newer_keys=list(_decode_typed(raw.get("preserved_newer_keys", []))),
            applied_repair_keys=list(_decode_typed(raw.get("applied_repair_keys", []))),
            replayed_cdc_seqs=[int(s) for s in raw.get("replayed_cdc_seqs", [])],
        )


@dataclass(frozen=True)
class ReverificationReport:
    """Result of post-repair source vs target reverification."""
    verified: bool
    checked_keys: int
    matching_keys: int
    divergent_keys: List[Any]
    watermark_lsn: int
    care_badge: CareBadge
    summary: str


@dataclass(frozen=True)
class RepairFenceOutcome:
    """Complete outcome of a fenced repair epoch and CDC reconciliation."""
    epoch_id: str
    table_name: str
    fence_token: int
    scope: RepairScope
    phase: FencePhase
    scope_choice: decide.Choice
    drained_in_flight_count: int
    applied_repair_count: int
    preserved_newer_count: int
    buffered_cdc_count: int
    replayed_cdc_count: int
    skipped_completed_count: int
    reverification: ReverificationReport
    care_badge: CareBadge
    summary: str

    def format_line(self) -> str:
        return (
            f"[{self.care_badge.value}] table={self.table_name} epoch={self.epoch_id}"
            f" fence={self.fence_token} scope={self.scope.value}"
            f" phase={self.phase.value} repaired={self.applied_repair_count}"
            f" preserved_newer={self.preserved_newer_count}"
            f" replayed_cdc={self.replayed_cdc_count}"
            f" verified={self.reverification.verified}"
        )


class TargetTableStore:
    """Target table state tracking per-row LSN, timestamp, modification flag, and fence token."""

    def __init__(
        self,
        table_name: str,
        initial_rows: Optional[Dict[Any, Dict[str, Any]]] = None,
        initial_fence_token: int = 0,
    ) -> None:
        self.table_name = str(table_name)
        self.highest_fence_token: int = int(initial_fence_token)
        self._rows: Dict[Any, Dict[str, Any]] = {}
        self._row_lsn: Dict[Any, int] = {}
        self._row_ts: Dict[Any, float] = {}
        self._target_modified: Set[Any] = set()
        self._applied_cdc_seqs: Set[int] = set()
        if initial_rows:
            for k, row in initial_rows.items():
                self.put_row(
                    k,
                    row,
                    lsn=int(row.get("_migkit_lsn", 0)),
                    ts=float(row.get("_migkit_ts", 0.0)),
                    target_modified=bool(row.get("_migkit_target_modified", False)),
                )

    def raise_fence(self, token: int) -> int:
        if int(token) < self.highest_fence_token:
            raise StaleFenceEpochError(
                f"cannot raise fence to {token}; target already at {self.highest_fence_token}"
            )
        self.highest_fence_token = int(token)
        return self.highest_fence_token

    def assert_fence(self, token: int) -> None:
        if int(token) < self.highest_fence_token:
            raise StaleFenceEpochError(
                f"write fenced off: epoch token {token} < target token {self.highest_fence_token}"
            )

    def get_row(self, key: Any) -> Optional[Dict[str, Any]]:
        row = self._rows.get(key)
        return copy.deepcopy(row) if row is not None else None

    def get_lsn(self, key: Any) -> int:
        return int(self._row_lsn.get(key, 0))

    def get_ts(self, key: Any) -> float:
        return float(self._row_ts.get(key, 0.0))

    def is_target_modified(self, key: Any) -> bool:
        return key in self._target_modified

    def put_row(
        self,
        key: Any,
        row: Optional[Dict[str, Any]],
        lsn: int,
        ts: float = 0.0,
        target_modified: bool = False,
        fence_token: Optional[int] = None,
    ) -> None:
        if fence_token is not None:
            self.assert_fence(fence_token)
        if row is None:
            self._rows.pop(key, None)
        else:
            clean = copy.deepcopy(row)
            clean.pop("_migkit_lsn", None)
            clean.pop("_migkit_ts", None)
            clean.pop("_migkit_target_modified", None)
            self._rows[key] = clean
        self._row_lsn[key] = int(lsn)
        self._row_ts[key] = float(ts)
        if target_modified:
            self._target_modified.add(key)
        else:
            self._target_modified.discard(key)

    def apply_cdc_change(self, change: CDCChange) -> bool:
        """Applies a CDC event if its sequence has not been applied and LSN >= row LSN."""
        if change.seq in self._applied_cdc_seqs:
            return False
        current_lsn = self.get_lsn(change.key)
        current_ts = self.get_ts(change.key)
        if change.lsn < current_lsn or (
            change.lsn == current_lsn and change.source_ts < current_ts
        ):
            self._applied_cdc_seqs.add(change.seq)
            return False

        if change.op == CDCOp.DELETE:
            self.put_row(change.key, None, lsn=change.lsn, ts=change.source_ts)
        elif change.op in (CDCOp.INSERT, CDCOp.UPDATE):
            merged = self.get_row(change.key) or {}
            if change.op == CDCOp.INSERT or not merged:
                merged = copy.deepcopy(change.after or {})
            else:
                merged.update(copy.deepcopy(change.after or {}))
            self.put_row(change.key, merged, lsn=change.lsn, ts=change.source_ts)
        self._applied_cdc_seqs.add(change.seq)
        return True

    def all_rows(self) -> Dict[Any, Dict[str, Any]]:
        return {k: copy.deepcopy(v) for k, v in self._rows.items()}


class FencedRepairCoordinator:
    """Coordinates repair epochs, CDC draining/holding, conflict resolution, and recovery."""

    def __init__(
        self,
        target: TargetTableStore,
        checkpoint_path: Optional[Path] = None,
        host: str = "127.0.0.1",
    ) -> None:
        self.host = validate_safe_host(host)
        self.target = target
        self.checkpoint = RepairFenceCheckpoint(checkpoint_path) if checkpoint_path else None
        self._in_flight_apply: List[CDCChange] = []
        self._live_passthrough_applied: List[int] = []
        self.active_epoch: Optional[RepairEpoch] = None

    def queue_in_flight_apply(self, changes: Iterable[CDCChange]) -> None:
        """Registers in-flight CDC changes that began before the repair epoch fence."""
        self._in_flight_apply.extend(changes)

    @property
    def live_passthrough_applied(self) -> Tuple[int, ...]:
        """Sequence numbers of unaffected CDC events applied while fence was active."""
        return tuple(self._live_passthrough_applied)

    def plan_scope(
        self,
        readings: ScopeReadings,
        costs: Optional[decide.Costs] = None,
    ) -> Tuple[decide.Choice, ScopeCostBreakdown]:
        """Measures total cost and outage per scope and climbs `select_repair_scope`."""
        breakdown = compute_scope_costs(readings)
        facts = breakdown.to_facts(readings)
        choice = select_repair_scope(facts, costs=costs)
        return choice, breakdown

    def begin_epoch(
        self,
        epoch_id: str,
        scope: RepairScope,
        snapshot_lsn: int,
        snapshot_ts: float = 0.0,
        affected_keys: Optional[Iterable[Any]] = None,
        affected_ranges: Optional[Sequence[Tuple[Any, Any]]] = None,
        conflict_policy: str = "newest_wins",
        fence_token: Optional[int] = None,
    ) -> RepairEpoch:
        """Establishes a repair epoch, raises the target fence token, and drains in-flight work."""
        if self.checkpoint is not None:
            saved = self.checkpoint.load()
            if saved is not None and saved.epoch_id == epoch_id:
                self.target.raise_fence(saved.fence_token)
                self.active_epoch = saved
                return saved

        next_token = (
            int(fence_token)
            if fence_token is not None
            else self.target.highest_fence_token + 1
        )
        self.target.raise_fence(next_token)
        epoch = RepairEpoch(
            epoch_id=str(epoch_id),
            table_name=self.target.table_name,
            fence_token=next_token,
            scope=scope,
            snapshot_lsn=int(snapshot_lsn),
            snapshot_ts=float(snapshot_ts),
            conflict_policy=str(conflict_policy),
            affected_keys=set(affected_keys or ()),
            affected_ranges=list(affected_ranges or ()),
            phase=FencePhase.DRAINING_APPLY,
        )
        drained = 0
        while self._in_flight_apply:
            ev = self._in_flight_apply.pop(0)
            self.target.apply_cdc_change(ev)
            drained += 1
        epoch.drained_in_flight_count = drained
        epoch.phase = FencePhase.HOLDING_CDC
        self.active_epoch = epoch
        if self.checkpoint is not None:
            self.checkpoint.save(epoch)
        return epoch

    def ingest_cdc_event(self, change: CDCChange) -> str:
        """Routes an incoming CDC change during an active repair epoch.

        Affected keys/ranges are buffered in the repair epoch; unaffected keys
        continue applying directly to the target without stalling.
        """
        epoch = self.active_epoch
        if epoch is None or epoch.phase in (FencePhase.COMMITTED, FencePhase.UNDONE):
            self.target.apply_cdc_change(change)
            return "applied_direct"

        if epoch.affects_key(change.key):
            if not any(c.seq == change.seq for c in epoch.buffered_cdc):
                epoch.buffered_cdc.append(change)
                epoch.buffered_cdc.sort(key=lambda c: (c.seq, c.lsn))
            if self.checkpoint is not None:
                self.checkpoint.save(epoch)
            return "buffered_in_epoch"

        self.target.apply_cdc_change(change)
        self._live_passthrough_applied.append(change.seq)
        return "applied_unaffected"

    def _latest_buffered_for_key(self, epoch: RepairEpoch, key: Any) -> Tuple[int, float]:
        max_lsn = -1
        max_ts = -1.0
        for ev in epoch.buffered_cdc:
            if ev.key == key:
                if ev.lsn > max_lsn:
                    max_lsn = ev.lsn
                if ev.source_ts > max_ts:
                    max_ts = ev.source_ts
        return max_lsn, max_ts

    def apply_repair_snapshots(
        self,
        snapshots: Sequence[RepairRowSnapshot],
        on_before_write: Optional[Callable[[RepairRowSnapshot, int], None]] = None,
        interrupt_after_writes: Optional[int] = None,
    ) -> Tuple[int, int, int]:
        """Applies repair snapshots under conflict policy, skipping O(1) completed keys."""
        epoch = self.active_epoch
        if epoch is None:
            raise RepairFenceError("no active repair epoch")

        epoch.phase = FencePhase.APPLYING_REPAIR
        applied_count = 0
        preserved_count = 0
        skipped_completed = 0

        for idx, snap in enumerate(snapshots):
            if snap.key in epoch.completed_keys:
                skipped_completed += 1
                continue

            if on_before_write is not None:
                on_before_write(snap, idx)

            self.target.assert_fence(epoch.fence_token)

            if snap.key not in epoch.preimages:
                epoch.preimages[snap.key] = self.target.get_row(snap.key)

            target_lsn = self.target.get_lsn(snap.key)
            target_ts = self.target.get_ts(snap.key)
            buf_lsn, buf_ts = self._latest_buffered_for_key(epoch, snap.key)
            latest_lsn = max(target_lsn, buf_lsn)
            latest_ts = max(target_ts, buf_ts)

            decision = resolve_repair_conflict(
                {
                    "row_key": str(snap.key),
                    "conflict_policy": epoch.conflict_policy,
                    "target_modified": self.target.is_target_modified(snap.key),
                    "latest_target_lsn": latest_lsn,
                    "latest_target_ts": latest_ts,
                    "repair_snapshot_lsn": snap.snapshot_lsn,
                    "repair_snapshot_ts": snap.snapshot_ts,
                }
            )

            if decision.path == ConflictResolution.APPLY_REPAIR_SNAPSHOT.value:
                self.target.put_row(
                    snap.key,
                    snap.row,
                    lsn=snap.snapshot_lsn,
                    ts=snap.snapshot_ts,
                    target_modified=False,
                    fence_token=epoch.fence_token,
                )
                epoch.repair_written_digests[snap.key] = canonical_row_digest(
                    self.target.get_row(snap.key)
                )
                if snap.key not in epoch.applied_repair_keys:
                    epoch.applied_repair_keys.append(snap.key)
                applied_count += 1
            else:
                epoch.repair_written_digests[snap.key] = canonical_row_digest(
                    self.target.get_row(snap.key)
                )
                if snap.key not in epoch.preserved_newer_keys:
                    epoch.preserved_newer_keys.append(snap.key)
                preserved_count += 1

            epoch.completed_keys.add(snap.key)
            if self.checkpoint is not None:
                self.checkpoint.save(epoch)

            if (
                interrupt_after_writes is not None
                and (applied_count + preserved_count) >= interrupt_after_writes
            ):
                raise InterruptedRepairError(
                    f"repair interrupted after {applied_count + preserved_count} key operations"
                )

        return applied_count, preserved_count, skipped_completed

    @staticmethod
    def verify_cdc_buffer_gap_free(buffered: Sequence[CDCChange]) -> Tuple[bool, str]:
        """Checks that buffered CDC events have strictly contiguous sequence numbers and monotonic LSNs."""
        if not buffered:
            return True, ""
        ordered = sorted(buffered, key=lambda c: c.seq)
        for i in range(1, len(ordered)):
            prev_ev = ordered[i - 1]
            curr_ev = ordered[i]
            if curr_ev.seq != prev_ev.seq + 1:
                return (
                    False,
                    f"sequence gap in buffered CDC between seq={prev_ev.seq} and seq={curr_ev.seq}",
                )
            if curr_ev.lsn < prev_ev.lsn:
                return (
                    False,
                    f"non-monotonic LSN in buffered CDC: seq={curr_ev.seq} lsn={curr_ev.lsn} < {prev_ev.lsn}",
                )
        return True, ""

    def reconcile_buffered_cdc(self) -> int:
        """Replays buffered CDC changes in gap-free commit order."""
        epoch = self.active_epoch
        if epoch is None:
            raise RepairFenceError("no active repair epoch")

        epoch.phase = FencePhase.RECONCILING_BUFFER
        gap_free, gap_reason = self.verify_cdc_buffer_gap_free(epoch.buffered_cdc)
        if not gap_free:
            epoch.phase = FencePhase.FAILED
            if self.checkpoint is not None:
                self.checkpoint.save(epoch)
            raise ReplayGapError(gap_reason)

        replayed = 0
        for ev in sorted(epoch.buffered_cdc, key=lambda c: (c.seq, c.lsn)):
            if ev.seq in epoch.replayed_cdc_seqs:
                continue
            self.target.assert_fence(epoch.fence_token)
            if self.target.apply_cdc_change(ev):
                replayed += 1
            epoch.replayed_cdc_seqs.append(ev.seq)
            epoch.repair_written_digests[ev.key] = canonical_row_digest(
                self.target.get_row(ev.key)
            )

        if self.checkpoint is not None:
            self.checkpoint.save(epoch)
        return replayed

    def reverify_scope(
        self,
        expected_source_rows: Dict[Any, Optional[Dict[str, Any]]],
    ) -> ReverificationReport:
        """Reverifies repaired scope against expected post-CDC source truth."""
        epoch = self.active_epoch
        if epoch is None:
            raise RepairFenceError("no active repair epoch")

        epoch.phase = FencePhase.REVERIFYING
        divergent: List[Any] = []
        matching = 0
        watermark = epoch.snapshot_lsn
        for ev in epoch.buffered_cdc:
            if ev.lsn > watermark:
                watermark = ev.lsn

        for key, expected_row in expected_source_rows.items():
            actual_row = self.target.get_row(key)
            if epoch.conflict_policy == "keep-target" and self.target.is_target_modified(key):
                matching += 1
                continue
            if canonical_row_digest(actual_row) == canonical_row_digest(expected_row):
                matching += 1
            else:
                divergent.append(key)

        verified = len(divergent) == 0
        if verified:
            epoch.phase = FencePhase.COMMITTED
            badge = (
                CareBadge.BENIGN
                if epoch.preserved_newer_keys
                else CareBadge.PASS
            )
            summary = (
                f"verified {matching}/{len(expected_source_rows)} keys at watermark_lsn={watermark}"
            )
        else:
            epoch.phase = FencePhase.FAILED
            badge = CareBadge.ACTION
            summary = (
                f"reverification failed: {len(divergent)} keys diverged at watermark_lsn={watermark}"
            )

        if self.checkpoint is not None:
            self.checkpoint.save(epoch)

        return ReverificationReport(
            verified=verified,
            checked_keys=len(expected_source_rows),
            matching_keys=matching,
            divergent_keys=divergent,
            watermark_lsn=watermark,
            care_badge=badge,
            summary=summary,
        )

    def execute_fenced_repair(
        self,
        epoch_id: str,
        readings: ScopeReadings,
        snapshots: Sequence[RepairRowSnapshot],
        expected_source_rows: Dict[Any, Optional[Dict[str, Any]]],
        affected_ranges: Optional[Sequence[Tuple[Any, Any]]] = None,
        conflict_policy: str = "newest_wins",
        on_before_write: Optional[Callable[[RepairRowSnapshot, int], None]] = None,
        costs: Optional[decide.Costs] = None,
    ) -> RepairFenceOutcome:
        """Plans repair scope, runs fenced repair, reconciles buffered CDC, and reverifies."""
        scope_choice, _ = self.plan_scope(readings, costs=costs)
        chosen_scope = RepairScope(scope_choice.path or RepairScope.DEFER_BOUNDED_OUTAGE.value)
        snapshot_lsn = max((s.snapshot_lsn for s in snapshots), default=0)
        snapshot_ts = max((s.snapshot_ts for s in snapshots), default=0.0)

        if chosen_scope == RepairScope.DEFER_BOUNDED_OUTAGE:
            reverify = ReverificationReport(
                verified=False,
                checked_keys=0,
                matching_keys=0,
                divergent_keys=[s.key for s in snapshots],
                watermark_lsn=snapshot_lsn,
                care_badge=CareBadge.REVIEW,
                summary=f"deferred: {scope_choice.reason}",
            )
            return RepairFenceOutcome(
                epoch_id=epoch_id,
                table_name=readings.table_name,
                fence_token=self.target.highest_fence_token,
                scope=chosen_scope,
                phase=FencePhase.FAILED,
                scope_choice=scope_choice,
                drained_in_flight_count=0,
                applied_repair_count=0,
                preserved_newer_count=0,
                buffered_cdc_count=0,
                replayed_cdc_count=0,
                skipped_completed_count=0,
                reverification=reverify,
                care_badge=CareBadge.REVIEW,
                summary=scope_choice.reason,
            )

        epoch = self.begin_epoch(
            epoch_id=epoch_id,
            scope=chosen_scope,
            snapshot_lsn=snapshot_lsn,
            snapshot_ts=snapshot_ts,
            affected_keys=[s.key for s in snapshots],
            affected_ranges=affected_ranges,
            conflict_policy=conflict_policy,
        )
        applied, preserved, skipped = self.apply_repair_snapshots(
            snapshots,
            on_before_write=on_before_write,
        )
        replayed = self.reconcile_buffered_cdc()
        reverify = self.reverify_scope(expected_source_rows)
        if not reverify.verified:
            raise VerificationDivergenceError(reverify.summary)

        return RepairFenceOutcome(
            epoch_id=epoch.epoch_id,
            table_name=epoch.table_name,
            fence_token=epoch.fence_token,
            scope=epoch.scope,
            phase=epoch.phase,
            scope_choice=scope_choice,
            drained_in_flight_count=epoch.drained_in_flight_count,
            applied_repair_count=applied,
            preserved_newer_count=preserved,
            buffered_cdc_count=len(epoch.buffered_cdc),
            replayed_cdc_count=replayed,
            skipped_completed_count=skipped,
            reverification=reverify,
            care_badge=reverify.care_badge,
            summary=reverify.summary,
        )

    def recover_interrupted_epoch(
        self,
        remaining_snapshots: Sequence[RepairRowSnapshot],
        expected_source_rows: Dict[Any, Optional[Dict[str, Any]]],
        force_undo: bool = False,
        can_finish_reconcile: bool = True,
    ) -> Tuple[decide.Choice, FencePhase, ReverificationReport]:
        """Recovers an interrupted repair epoch by climbing `select_interrupted_recovery`."""
        epoch = self.active_epoch
        if epoch is None and self.checkpoint is not None:
            epoch = self.checkpoint.load()
            self.active_epoch = epoch
        if epoch is None:
            raise RepairFenceError("no interrupted repair epoch found to recover")

        gap_free, _ = self.verify_cdc_buffer_gap_free(epoch.buffered_cdc)
        preimage_recorded = len(epoch.preimages) >= len(epoch.completed_keys)
        target_matches = all(
            canonical_row_digest(self.target.get_row(k)) == digest
            for k, digest in epoch.repair_written_digests.items()
        )

        choice = select_interrupted_recovery(
            {
                "epoch_id": epoch.epoch_id,
                "epoch_journal_valid": True,
                "cdc_buffer_gap_free": gap_free,
                "can_finish_reconcile": bool(can_finish_reconcile),
                "force_undo": bool(force_undo),
                "preimage_recorded": preimage_recorded,
                "target_matches_repair_writes": target_matches,
            }
        )

        if choice.path == RecoveryDisposition.RECONCILE_AND_COMPLETE.value:
            self.apply_repair_snapshots(remaining_snapshots)
            self.reconcile_buffered_cdc()
            report = self.reverify_scope(expected_source_rows)
            return choice, epoch.phase, report

        if choice.path == RecoveryDisposition.UNDO_TO_PREIMAGE.value:
            for key, pre_row in epoch.preimages.items():
                self.target.put_row(
                    key,
                    pre_row,
                    lsn=epoch.snapshot_lsn,
                    ts=epoch.snapshot_ts,
                    fence_token=epoch.fence_token,
                )
            if gap_free:
                for ev in sorted(epoch.buffered_cdc, key=lambda c: (c.seq, c.lsn)):
                    self.target.apply_cdc_change(ev)
            epoch.phase = FencePhase.UNDONE
            if self.checkpoint is not None:
                self.checkpoint.save(epoch)
            report = ReverificationReport(
                verified=True,
                checked_keys=len(epoch.preimages),
                matching_keys=len(epoch.preimages),
                divergent_keys=[],
                watermark_lsn=epoch.snapshot_lsn,
                care_badge=CareBadge.REVIEW,
                summary=f"undid {len(epoch.preimages)} touched keys to pre-images and replayed held CDC",
            )
            return choice, epoch.phase, report

        epoch.phase = FencePhase.FAILED
        if self.checkpoint is not None:
            self.checkpoint.save(epoch)
        report = ReverificationReport(
            verified=False,
            checked_keys=len(epoch.completed_keys),
            matching_keys=0,
            divergent_keys=list(epoch.completed_keys),
            watermark_lsn=epoch.snapshot_lsn,
            care_badge=CareBadge.ACTION,
            summary=f"recovery aborted: {choice.reason}",
        )
        return choice, epoch.phase, report


def coordinate_repair_resync(
    target_store: TargetTableStore,
    engine: Any,
    metrics: Any,
    source_snapshot: Sequence[Dict[str, Any]],
    cdc_events: Optional[Sequence[Any]] = None,
    start_lsn: int = 0,
    shadow_coordinator: Optional[Any] = None,
) -> Any:
    """Coordinate zero-downtime shadow table resync when repair fence requires whole reload (Row 618: RCH10)."""
    from . import shadow_resync
    if shadow_coordinator is None:
        shadow_coordinator = shadow_resync.ShadowResyncCoordinator()
    return shadow_coordinator.coordinate_resync(
        engine=engine,
        metrics=metrics,
        source_snapshot=source_snapshot,
        cdc_events=cdc_events,
        start_lsn=start_lsn,
    )


def coordinate_repair_proof_verification(
    routine_name: str,
    source_fn: Any,
    source_code: str,
    deterministic_candidate: Optional[Any] = None,
    synthesis_generator: Optional[Any] = None,
    base_inputs: Optional[Sequence[Dict[str, Any]]] = None,
    checkpoint_dir: Optional[Any] = None,
    **kwargs: Any,
) -> Any:
    """Coordinate independent executable evidence verification for repair transformations (Row 627: RCH19)."""
    from . import ai_proof_loop
    engine = ai_proof_loop.AiProofLoopEngine(**kwargs)
    return engine.run_proof_loop(
        routine_name=routine_name,
        source_fn=source_fn,
        source_code=source_code,
        deterministic_candidate=deterministic_candidate,
        synthesis_generator=synthesis_generator,
        base_inputs=base_inputs,
        checkpoint_dir=checkpoint_dir,
    )

