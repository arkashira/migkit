"""Consistent snapshot-to-stream handoff mechanisms (Row 619: RCH11).

Provides:
- Strategy alternatives registry: shared snapshot, range watermark (low/high watermark),
  and engine-native handoff.
- Pricing engine for snapshot lifetime, WAL retention, and source impact.
- Signaling writes enforcement and read-only source intent protection (refusing write
  signaling on read-only sources rather than faking an epoch).
- Missing log interval detection before accepting copy coverage.
- Universal typed durable resume in O(1) with lease locking (.run.lock).
- Pure UNIX style output with 4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
"""
from __future__ import annotations

import datetime
import decimal
import enum
import json
import math
import os
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from migkit import decide, durable
from migkit.verifiers import gtid_is_subset, lsn_compare, lsn_to_int


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class SnapshotWatermarkError(RuntimeError):
    """Base error for snapshot-to-stream handoff operations."""
    pass


class SignalingWriteRefusedError(SnapshotWatermarkError):
    """Raised when signaling writes are attempted on a read-only source."""
    pass


class MissingLogIntervalError(SnapshotWatermarkError):
    """Raised when log intervals have gaps or missing continuity."""
    pass


class WatermarkNotSupportedError(SnapshotWatermarkError):
    """Raised when watermark or handoff support is unavailable for an engine."""
    pass


class HandoffCostExceededError(SnapshotWatermarkError):
    """Raised when snapshot lifetime or WAL retention exceeds hard budget limits."""
    pass


class ActiveLeaseConflictError(SnapshotWatermarkError):
    """Raised when another active worker holds the verification/handoff lease."""
    pass


class CorruptLockError(SnapshotWatermarkError):
    """Raised when an existing lease/lock file cannot be parsed, to prevent clobbering."""
    pass


class LockAcquisitionError(SnapshotWatermarkError):
    """Raised when lease/lock file cannot be read or acquired."""
    pass


# ---------------------------------------------------------------------------
# Enums and Care Badges
# ---------------------------------------------------------------------------


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class HandoffStrategyType(str, enum.Enum):
    SHARED_SNAPSHOT = "shared_snapshot"
    RANGE_WATERMARK = "range_watermark"
    ENGINE_NATIVE = "engine_native"


class SourceIntent(str, enum.Enum):
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"


class EngineType(str, enum.Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    GENERIC = "generic"


# ---------------------------------------------------------------------------
# Position Utilities
# ---------------------------------------------------------------------------


def int_to_lsn(val: int) -> str:
    """Format integer as uppercase PostgreSQL LSN string."""
    hi = (val >> 32) & 0xFFFFFFFF
    lo = val & 0xFFFFFFFF
    return f"{hi:X}/{lo:X}"


def parse_position(val: Any) -> Any:
    """Parse positions into comparable numbers or strings."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        v = val.strip()
        if "/" in v:
            parsed = lsn_to_int(v)
            if parsed is not None:
                return parsed
        if v.isdigit():
            return int(v)
        return v
    return val


def compare_positions(a: Any, b: Any, engine: str = "generic") -> int:
    """Compare two position markers across engines.
    
    Returns:
      -1 if a < b
       0 if a == b
       1 if a > b
    """
    if a is None and b is None:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1

    engine_norm = str(engine).lower()
    if engine_norm == "postgres" or (isinstance(a, str) and "/" in a) or (isinstance(b, str) and "/" in b):
        return lsn_compare(a, b)

    if engine_norm == "mysql" and isinstance(a, str) and ":" in a:
        if a == b:
            return 0
        a_sub = gtid_is_subset(a, b)
        b_sub = gtid_is_subset(b, a)
        if a_sub and not b_sub:
            return -1
        if b_sub and not a_sub:
            return 1
        return 0

    pa = parse_position(a)
    pb = parse_position(b)
    if isinstance(pa, (int, float)) and isinstance(pb, (int, float)):
        if pa < pb:
            return -1
        if pa > pb:
            return 1
        return 0

    sa, sb = str(a), str(b)
    if sa < sb:
        return -1
    if sa > sb:
        return 1
    return 0


# ---------------------------------------------------------------------------
# Profiles & Data Models
# ---------------------------------------------------------------------------


@dataclass
class SourceProfile:
    host: str = "127.0.0.1"
    port: int = 5432
    engine: str = "postgres"
    read_only: bool = False
    intent: str = "read_write"
    wal_rate_bytes_per_sec: float = 2.0 * 1024 * 1024  # 2 MB/s
    disk_headroom_bytes: int = 50 * 1024 * 1024 * 1024  # 50 GB
    max_snapshot_lifetime_seconds: float = 14400.0  # 4 hours
    max_slot_wal_keep_size_bytes: Optional[int] = None
    supports_shared_snapshot: bool = True
    supports_range_watermark: bool = True
    supports_engine_native: bool = True
    active_connections: int = 10
    max_connections: int = 100

    @property
    def is_read_only(self) -> bool:
        return bool(self.read_only or str(self.intent).lower() == SourceIntent.READ_ONLY.value)


@dataclass
class TableProfile:
    table_name: str
    estimated_rows: int = 100_000
    estimated_bytes: int = 50 * 1024 * 1024  # 50 MB
    has_primary_key: bool = True
    primary_key_columns: List[str] = field(default_factory=lambda: ["id"])
    chunk_count: int = 1


@dataclass
class HandoffPricing:
    strategy: str
    snapshot_lifetime_seconds: float
    wal_retention_bytes: int
    source_impact_score: float  # 0 to 100
    requires_signaling_writes: bool
    estimated_disk_usage_fraction: float
    care_badge: str = CareBadge.PASS.value
    care_reasons: List[str] = field(default_factory=list)
    is_viable: bool = True
    rejection_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LogInterval:
    start_pos: Any
    end_pos: Any
    engine: str = "generic"
    record_count: int = 0
    bytes_size: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_pos": self.start_pos,
            "end_pos": self.end_pos,
            "engine": self.engine,
            "record_count": self.record_count,
            "bytes_size": self.bytes_size,
            "metadata": self.metadata,
        }


@dataclass
class ContinuityReport:
    is_continuous: bool
    accept_coverage: bool
    care_badge: str
    gaps: List[Tuple[Any, Any]] = field(default_factory=list)
    overlaps: List[Tuple[Any, Any]] = field(default_factory=list)
    earliest_pos: Optional[Any] = None
    latest_pos: Optional[Any] = None
    total_records: int = 0
    total_bytes: int = 0
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_continuous": self.is_continuous,
            "accept_coverage": self.accept_coverage,
            "care_badge": self.care_badge,
            "gaps": self.gaps,
            "overlaps": self.overlaps,
            "earliest_pos": self.earliest_pos,
            "latest_pos": self.latest_pos,
            "total_records": self.total_records,
            "total_bytes": self.total_bytes,
            "reasons": self.reasons,
        }


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


class BaseHandoffStrategy:
    """Abstract base for snapshot-to-stream handoff mechanisms."""

    strategy_type: str = "base"
    name: str = "Base Strategy"
    description: str = "Base snapshot-to-stream handoff strategy"
    requires_signaling_writes: bool = False

    def validate_capability(self, source: SourceProfile) -> None:
        """Validate engine and source support. Refuse path if not supported."""
        pass

    def price(
        self,
        source: SourceProfile,
        table: TableProfile,
        copy_rate_bytes_per_sec: float = 10.0 * 1024 * 1024,  # 10 MB/s
    ) -> HandoffPricing:
        """Calculate costs for snapshot lifetime, WAL retention, and source impact."""
        raise NotImplementedError


class SharedSnapshotStrategy(BaseHandoffStrategy):
    """Shared snapshot handoff (e.g., PostgreSQL pg_export_snapshot).
    
    Zero signaling writes; maintains read-only transaction snapshot across parallel workers.
    Holds WAL retention and long-running snapshot lifetime.
    """

    strategy_type = HandoffStrategyType.SHARED_SNAPSHOT.value
    name = "Shared Snapshot"
    description = "Exported read transaction snapshot across parallel readers without signaling writes"
    requires_signaling_writes = False

    def validate_capability(self, source: SourceProfile) -> None:
        if not source.supports_shared_snapshot:
            raise WatermarkNotSupportedError(
                "Shared snapshot not supported by source engine/configuration"
            )

    def price(
        self,
        source: SourceProfile,
        table: TableProfile,
        copy_rate_bytes_per_sec: float = 10.0 * 1024 * 1024,
    ) -> HandoffPricing:
        reasons: List[str] = []
        is_viable = True
        rejection_reason = None

        if not source.supports_shared_snapshot:
            return HandoffPricing(
                strategy=self.strategy_type,
                snapshot_lifetime_seconds=0.0,
                wal_retention_bytes=0,
                source_impact_score=100.0,
                requires_signaling_writes=False,
                estimated_disk_usage_fraction=0.0,
                care_badge=CareBadge.ACTION.value,
                care_reasons=["Shared snapshot capability unsupported by source"],
                is_viable=False,
                rejection_reason="Shared snapshot capability unsupported",
            )

        rate = max(copy_rate_bytes_per_sec, 1024.0)
        lifetime = table.estimated_bytes / rate
        wal_retention = int(lifetime * source.wal_rate_bytes_per_sec)

        headroom = max(source.disk_headroom_bytes, 1)
        disk_fraction = wal_retention / headroom

        # Impact score: connections holding open transaction + table bloat risk
        conn_ratio = source.active_connections / max(source.max_connections, 1)
        base_impact = 25.0 + (conn_ratio * 25.0) + min(lifetime / 360.0, 40.0)
        impact_score = min(max(base_impact, 0.0), 100.0)

        # Check hard limits
        if source.max_slot_wal_keep_size_bytes and wal_retention > source.max_slot_wal_keep_size_bytes:
            is_viable = False
            rejection_reason = (
                f"WAL retention {wal_retention} bytes exceeds max_slot_wal_keep_size "
                f"({source.max_slot_wal_keep_size_bytes} bytes)"
            )
            reasons.append(rejection_reason)

        if wal_retention > source.disk_headroom_bytes:
            is_viable = False
            rejection_reason = (
                f"WAL retention {wal_retention} bytes exceeds source disk headroom "
                f"({source.disk_headroom_bytes} bytes)"
            )
            reasons.append(rejection_reason)

        if lifetime > source.max_snapshot_lifetime_seconds:
            is_viable = False
            rejection_reason = (
                f"Snapshot lifetime {lifetime:.1f}s exceeds max allowed lifetime "
                f"({source.max_snapshot_lifetime_seconds:.1f}s)"
            )
            reasons.append(rejection_reason)

        # Determine care badge
        if not is_viable:
            badge = CareBadge.ACTION.value
        elif disk_fraction > 0.6 or lifetime > 3600.0 or impact_score > 70.0:
            badge = CareBadge.REVIEW.value
            if disk_fraction > 0.6:
                reasons.append(f"High WAL accumulation: {disk_fraction*100:.1f}% of disk headroom")
            if lifetime > 3600.0:
                reasons.append(f"Long-lived snapshot ({lifetime/3600.0:.2f}h) risks vacuum freeze lag")
            if impact_score > 70.0:
                reasons.append(f"Elevated source impact score: {impact_score:.1f}/100")
        elif disk_fraction > 0.2:
            badge = CareBadge.BENIGN.value
            reasons.append(f"Moderate WAL retention: {disk_fraction*100:.1f}% of disk headroom")
        else:
            badge = CareBadge.PASS.value
            reasons.append("Shared snapshot cost envelope well within safe margins")

        return HandoffPricing(
            strategy=self.strategy_type,
            snapshot_lifetime_seconds=lifetime,
            wal_retention_bytes=wal_retention,
            source_impact_score=impact_score,
            requires_signaling_writes=False,
            estimated_disk_usage_fraction=disk_fraction,
            care_badge=badge,
            care_reasons=reasons,
            is_viable=is_viable,
            rejection_reason=rejection_reason,
        )


class RangeWatermarkStrategy(BaseHandoffStrategy):
    """Range watermark handoff (e.g., Debezium incremental snapshot / DBLog).
    
    Splits tables into chunks and writes signaling low/high watermarks.
    Stream events within low/high watermarks are deduplicated/reconciled.
    REQUIRES signaling writes. If source is read-only, this strategy MUST BE REFUSED.
    """

    strategy_type = HandoffStrategyType.RANGE_WATERMARK.value
    name = "Range Watermark"
    description = "Chunked low/high watermark handoff using signaling writes and stream reconciliation"
    requires_signaling_writes = True

    def validate_capability(self, source: SourceProfile) -> None:
        if source.is_read_only:
            raise SignalingWriteRefusedError(
                "Signaling writes required by range watermark strategy are refused: "
                "source is marked read-only"
            )
        if not source.supports_range_watermark:
            raise WatermarkNotSupportedError(
                "Range watermark capability unsupported by source engine/configuration"
            )

    def price(
        self,
        source: SourceProfile,
        table: TableProfile,
        copy_rate_bytes_per_sec: float = 10.0 * 1024 * 1024,
    ) -> HandoffPricing:
        reasons: List[str] = []

        # If source is read-only, strictly refuse
        if source.is_read_only:
            reason = "Signaling writes required by range watermark strategy are refused: source is marked read-only"
            return HandoffPricing(
                strategy=self.strategy_type,
                snapshot_lifetime_seconds=0.0,
                wal_retention_bytes=0,
                source_impact_score=100.0,
                requires_signaling_writes=True,
                estimated_disk_usage_fraction=0.0,
                care_badge=CareBadge.ACTION.value,
                care_reasons=[reason],
                is_viable=False,
                rejection_reason=reason,
            )

        if not source.supports_range_watermark:
            reason = "Range watermark capability unsupported by source engine/configuration"
            return HandoffPricing(
                strategy=self.strategy_type,
                snapshot_lifetime_seconds=0.0,
                wal_retention_bytes=0,
                source_impact_score=100.0,
                requires_signaling_writes=True,
                estimated_disk_usage_fraction=0.0,
                care_badge=CareBadge.ACTION.value,
                care_reasons=[reason],
                is_viable=False,
                rejection_reason=reason,
            )

        chunks = max(table.chunk_count, 1)
        bytes_per_chunk = table.estimated_bytes / chunks
        rate = max(copy_rate_bytes_per_sec, 1024.0)
        chunk_duration = bytes_per_chunk / rate

        # Range watermark only holds transient snapshot per chunk
        snapshot_lifetime = chunk_duration
        wal_retention = int(chunk_duration * source.wal_rate_bytes_per_sec)

        headroom = max(source.disk_headroom_bytes, 1)
        disk_fraction = wal_retention / headroom

        # Impact score: signal write overhead + stream buffer reconciliation
        impact_score = min(20.0 + (chunks * 0.5), 65.0)

        badge = CareBadge.PASS.value
        reasons.append("Chunked range watermark eliminates long-lived snapshot lock")
        if chunks > 50:
            badge = CareBadge.BENIGN.value
            reasons.append(f"{chunks} chunks require repeated signal writes")

        return HandoffPricing(
            strategy=self.strategy_type,
            snapshot_lifetime_seconds=snapshot_lifetime,
            wal_retention_bytes=wal_retention,
            source_impact_score=impact_score,
            requires_signaling_writes=True,
            estimated_disk_usage_fraction=disk_fraction,
            care_badge=badge,
            care_reasons=reasons,
            is_viable=True,
            rejection_reason=None,
        )


class EngineNativeStrategy(BaseHandoffStrategy):
    """Engine-native handoff (e.g., MySQL GTID consistent snapshot, TiDB CDC resolved TS).
    
    Zero signaling writes; uses engine-level snapshot-point coordinate.
    If engine lacks native watermark/slot coordination, refuses that path rather than faking an epoch.
    """

    strategy_type = HandoffStrategyType.ENGINE_NATIVE.value
    name = "Engine Native"
    description = "Engine-native snapshot coordinate handoff without signaling writes"
    requires_signaling_writes = False

    def validate_capability(self, source: SourceProfile) -> None:
        if not source.supports_engine_native:
            raise WatermarkNotSupportedError(
                "Engine-native handoff not supported: refusing path rather than faking an epoch"
            )

    def price(
        self,
        source: SourceProfile,
        table: TableProfile,
        copy_rate_bytes_per_sec: float = 10.0 * 1024 * 1024,
    ) -> HandoffPricing:
        reasons: List[str] = []

        if not source.supports_engine_native:
            reason = "Engine-native handoff unsupported: refusing path rather than faking an epoch"
            return HandoffPricing(
                strategy=self.strategy_type,
                snapshot_lifetime_seconds=0.0,
                wal_retention_bytes=0,
                source_impact_score=100.0,
                requires_signaling_writes=False,
                estimated_disk_usage_fraction=0.0,
                care_badge=CareBadge.ACTION.value,
                care_reasons=[reason],
                is_viable=False,
                rejection_reason=reason,
            )

        rate = max(copy_rate_bytes_per_sec, 1024.0)
        lifetime = table.estimated_bytes / rate
        wal_retention = int(lifetime * source.wal_rate_bytes_per_sec)

        headroom = max(source.disk_headroom_bytes, 1)
        disk_fraction = wal_retention / headroom
        impact_score = 15.0  # Engine native is lightweight and consistent

        is_viable = True
        rejection_reason = None

        if wal_retention > source.disk_headroom_bytes:
            is_viable = False
            rejection_reason = f"WAL retention {wal_retention} bytes exceeds disk headroom"
            reasons.append(rejection_reason)
            badge = CareBadge.ACTION.value
        elif disk_fraction > 0.5:
            badge = CareBadge.REVIEW.value
            reasons.append(f"WAL retention at {disk_fraction*100:.1f}% of headroom")
        else:
            badge = CareBadge.PASS.value
            reasons.append("Engine native handoff provides optimal zero-signaling coordination")

        return HandoffPricing(
            strategy=self.strategy_type,
            snapshot_lifetime_seconds=lifetime,
            wal_retention_bytes=wal_retention,
            source_impact_score=impact_score,
            requires_signaling_writes=False,
            estimated_disk_usage_fraction=disk_fraction,
            care_badge=badge,
            care_reasons=reasons,
            is_viable=is_viable,
            rejection_reason=rejection_reason,
        )


# ---------------------------------------------------------------------------
# Strategy Registry
# ---------------------------------------------------------------------------


class HandoffStrategyRegistry:
    """Registry of explicit alternative handoff mechanisms."""

    def __init__(self) -> None:
        self._strategies: Dict[str, BaseHandoffStrategy] = {}
        self.register(SharedSnapshotStrategy())
        self.register(RangeWatermarkStrategy())
        self.register(EngineNativeStrategy())

    def register(self, strategy: BaseHandoffStrategy) -> None:
        self._strategies[strategy.strategy_type] = strategy

    def get(self, strategy_type: str) -> BaseHandoffStrategy:
        if strategy_type not in self._strategies:
            raise KeyError(f"Unknown handoff strategy: {strategy_type}")
        return self._strategies[strategy_type]

    def list_strategies(self) -> List[BaseHandoffStrategy]:
        return list(self._strategies.values())


# ---------------------------------------------------------------------------
# Missing Log Interval & Coverage Verifier
# ---------------------------------------------------------------------------


class LogContinuityVerifier:
    """Detects missing log intervals before accepting copy coverage.
    
    Verifies that stream records cover the interval from snapshot cut (or low watermark)
    to the current stream position without gaps.
    """

    @classmethod
    def verify_continuity(
        cls,
        snapshot_cut: Any,
        log_intervals: Sequence[LogInterval],
        high_watermark: Optional[Any] = None,
        engine: str = "generic",
        strict: bool = False,
    ) -> ContinuityReport:
        if not log_intervals:
            msg = "No log intervals provided: cannot verify copy coverage"
            if strict:
                raise MissingLogIntervalError(msg)
            return ContinuityReport(
                is_continuous=False,
                accept_coverage=False,
                care_badge=CareBadge.ACTION.value,
                reasons=[msg],
            )

        gaps: List[Tuple[Any, Any]] = []
        overlaps: List[Tuple[Any, Any]] = []
        total_records = sum(iv.record_count for iv in log_intervals)
        total_bytes = sum(iv.bytes_size for iv in log_intervals)

        # Sort intervals by start_pos
        sorted_intervals = sorted(
            log_intervals,
            key=lambda iv: parse_position(iv.start_pos),
        )

        earliest = sorted_intervals[0].start_pos
        latest = sorted_intervals[-1].end_pos

        # Check 1: Gap between snapshot cut and earliest stream interval
        if snapshot_cut is not None:
            cmp_start = compare_positions(earliest, snapshot_cut, engine=engine)
            if cmp_start > 0:
                # Earliest stream interval starts strictly after snapshot cut
                gaps.append((snapshot_cut, earliest))

        # Check 2: Gaps between consecutive intervals
        for i in range(len(sorted_intervals) - 1):
            curr_end = sorted_intervals[i].end_pos
            next_start = sorted_intervals[i + 1].start_pos

            cmp_step = compare_positions(curr_end, next_start, engine=engine)
            if cmp_step < 0:
                # Gap between curr_end and next_start
                gaps.append((curr_end, next_start))
            elif cmp_step > 0:
                # Overlap between curr_end and next_start
                overlaps.append((next_start, curr_end))

        # Check 3: If high watermark is specified, ensure latest reaches it
        if high_watermark is not None:
            cmp_hi = compare_positions(latest, high_watermark, engine=engine)
            if cmp_hi < 0:
                gaps.append((latest, high_watermark))

        is_continuous = len(gaps) == 0
        accept_coverage = is_continuous
        reasons: List[str] = []

        if not is_continuous:
            badge = CareBadge.ACTION.value
            for g_start, g_end in gaps:
                reasons.append(f"Missing log interval detected: [{g_start}, {g_end}]; coverage refused")
            if strict:
                raise MissingLogIntervalError("; ".join(reasons))
        elif overlaps:
            badge = CareBadge.BENIGN.value
            reasons.append(
                f"Continuous coverage verified with {len(overlaps)} safe overlap intervals (deduplicated)"
            )
        else:
            badge = CareBadge.PASS.value
            reasons.append("Continuous unbroken log coverage verified from snapshot cut to stream head")

        return ContinuityReport(
            is_continuous=is_continuous,
            accept_coverage=accept_coverage,
            care_badge=badge,
            gaps=gaps,
            overlaps=overlaps,
            earliest_pos=earliest,
            latest_pos=latest,
            total_records=total_records,
            total_bytes=total_bytes,
            reasons=reasons,
        )


# ---------------------------------------------------------------------------
# Universal Typed Durable Resume in O(1)
# ---------------------------------------------------------------------------


def serialize_typed_value(val: Any) -> Any:
    """Recursively encode native types (datetime, Decimal, UUID, tuple, set) into typed JSON dicts."""
    if isinstance(val, tuple):
        return {"__typed__": "tuple", "val": [serialize_typed_value(x) for x in val]}
    if isinstance(val, datetime.datetime):
        return {"__typed__": "datetime", "val": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__typed__": "date", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__typed__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__typed__": "uuid", "val": str(val)}
    if isinstance(val, set):
        return {"__typed__": "set", "val": [serialize_typed_value(x) for x in val]}
    if isinstance(val, dict):
        return {k: serialize_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [serialize_typed_value(x) for x in val]
    return val


class TypedJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder preserving native types (datetime, Decimal, UUID, tuple)."""

    def default(self, obj: Any) -> Any:
        serialized = serialize_typed_value(obj)
        if serialized is not obj:
            return serialized
        return super().default(obj)


def typed_json_hook(dct: Dict[str, Any]) -> Any:
    """Custom JSON hook reconstructing native types."""
    if "__typed__" in dct:
        kind = dct["__typed__"]
        val = dct["val"]
        if kind == "datetime":
            return datetime.datetime.fromisoformat(val)
        if kind == "date":
            return datetime.date.fromisoformat(val)
        if kind == "decimal":
            return decimal.Decimal(val)
        if kind == "uuid":
            return uuid.UUID(val)
        if kind == "tuple":
            return tuple(val)
        if kind == "set":
            return set(val)
    return dct


@dataclass
class RangeCursor:
    cursor_id: str
    start_key: Any
    end_key: Any
    completed: bool = False
    watermark: Optional[Any] = None
    rows_written: int = 0
    updated_at: float = field(default_factory=time.time)


@dataclass
class HandoffState:
    handoff_id: str
    strategy: str
    source_host: str
    table_name: str
    snapshot_cut: Optional[Any] = None
    stream_position: Optional[Any] = None
    status: str = "initialized"
    cursors: Dict[str, RangeCursor] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)

    def is_chunk_completed(self, cursor_id: str) -> bool:
        """O(1) status check for range cursor."""
        cursor = self.cursors.get(cursor_id)
        return bool(cursor and cursor.completed)

    def mark_chunk_completed(
        self,
        cursor_id: str,
        start_key: Any,
        end_key: Any,
        watermark: Optional[Any] = None,
        rows_written: int = 0,
    ) -> None:
        """Record chunk completion in O(1)."""
        self.cursors[cursor_id] = RangeCursor(
            cursor_id=cursor_id,
            start_key=start_key,
            end_key=end_key,
            completed=True,
            watermark=watermark,
            rows_written=rows_written,
            updated_at=time.time(),
        )
        self.updated_at = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "handoff_id": self.handoff_id,
            "strategy": self.strategy,
            "source_host": self.source_host,
            "table_name": self.table_name,
            "snapshot_cut": self.snapshot_cut,
            "stream_position": self.stream_position,
            "status": self.status,
            "cursors": {
                k: {
                    "cursor_id": v.cursor_id,
                    "start_key": v.start_key,
                    "end_key": v.end_key,
                    "completed": v.completed,
                    "watermark": v.watermark,
                    "rows_written": v.rows_written,
                    "updated_at": v.updated_at,
                }
                for k, v in self.cursors.items()
            },
            "metadata": self.metadata,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> HandoffState:
        cursors: Dict[str, RangeCursor] = {}
        for k, v in data.get("cursors", {}).items():
            cursors[k] = RangeCursor(
                cursor_id=v["cursor_id"],
                start_key=v["start_key"],
                end_key=v["end_key"],
                completed=v.get("completed", False),
                watermark=v.get("watermark"),
                rows_written=v.get("rows_written", 0),
                updated_at=v.get("updated_at", 0.0),
            )
        return cls(
            handoff_id=data["handoff_id"],
            strategy=data["strategy"],
            source_host=data["source_host"],
            table_name=data["table_name"],
            snapshot_cut=data.get("snapshot_cut"),
            stream_position=data.get("stream_position"),
            status=data.get("status", "initialized"),
            cursors=cursors,
            metadata=data.get("metadata", {}),
            updated_at=data.get("updated_at", 0.0),
        )

    def save(self, filepath: str) -> None:
        """Atomic durable save with type preservation."""
        typed_dict = serialize_typed_value(self.to_dict())
        content = json.dumps(typed_dict, indent=2)
        durable.write_atomic(filepath, content.encode("utf-8"), full=False)

    @classmethod
    def load(cls, filepath: str) -> HandoffState:
        """Load state and restore native types."""
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f, object_hook=typed_json_hook)
        return cls.from_dict(data)


class ActiveLeaseLock:
    """Mutual exclusion active lease lock (.run.lock) with heartbeat."""

    def __init__(self, lock_dir: str, lease_term_seconds: float = 30.0) -> None:
        self.lock_dir = lock_dir
        self.lock_file = os.path.join(lock_dir, ".run.lock")
        self.lease_term_seconds = float(lease_term_seconds)
        self.holder_id = f"{os.getpid()}@{os.uname().nodename}:{time.time()}"
        self.acquired = False

    def acquire(self) -> None:
        now = time.time()
        os.makedirs(self.lock_dir, exist_ok=True)

        if os.path.exists(self.lock_file):
            try:
                with open(self.lock_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                expires = float(data.get("expires", 0))
                holder = data.get("holder", "unknown")
                if now < expires and holder != self.holder_id:
                    raise ActiveLeaseConflictError(
                        f"Active lease held by {holder} until {expires} (remaining {expires - now:.1f}s)"
                    )
            except json.JSONDecodeError as exc:
                raise CorruptLockError(f"Corrupt lease file at {self.lock_file}: {exc}") from exc
            except OSError as exc:
                raise LockAcquisitionError(f"Cannot read lease file at {self.lock_file}: {exc}") from exc

        payload = {
            "holder": self.holder_id,
            "acquired_at": now,
            "expires": now + self.lease_term_seconds,
            "pid": os.getpid(),
        }
        durable.write_atomic(self.lock_file, json.dumps(payload).encode("utf-8"), full=False)
        self.acquired = True

    def heartbeat(self) -> None:
        if not self.acquired:
            raise ActiveLeaseConflictError("Cannot heartbeat unacquired lease")
        now = time.time()
        payload = {
            "holder": self.holder_id,
            "acquired_at": now,
            "expires": now + self.lease_term_seconds,
            "pid": os.getpid(),
        }
        durable.write_atomic(self.lock_file, json.dumps(payload).encode("utf-8"), full=False)

    def release(self) -> None:
        if self.acquired:
            try:
                if os.path.exists(self.lock_file):
                    os.unlink(self.lock_file)
            except OSError:
                pass
            self.acquired = False

    def __enter__(self) -> ActiveLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# ---------------------------------------------------------------------------
# Coordinator & Reporting
# ---------------------------------------------------------------------------


def snapshot_handoff_rungs() -> Tuple[decide.Rung, ...]:
    """Decide ladder for snapshot-to-stream handoff mechanisms (Row 619: RCH11)."""
    return (
        decide.Rung(
            "zero_impact_logical_export",
            "logical export from physical standby or isolated replica",
            needs=(
                decide.Need(
                    "source is isolated replica or standby",
                    lambda f: bool(f.get("is_standby_or_replica")),
                    "{rung} requires source node to be an isolated standby or replica",
                ),
                decide.Need(
                    "snapshot export supported",
                    lambda f: bool(f.get("supports_snapshot_export")),
                    "{rung} requires engine snapshot export capability",
                ),
            ),
            because="isolates snapshot load entirely away from production primary",
        ),
        decide.Rung(
            "consistent_primary_export",
            "consistent transactional snapshot export from primary",
            needs=(
                decide.Need(
                    "snapshot export supported",
                    lambda f: bool(f.get("supports_snapshot_export")),
                    "{rung} requires engine snapshot export capability",
                ),
                decide.Need(
                    "primary headroom available",
                    lambda f: float(f.get("primary_cpu_headroom_pct", 100.0)) >= 25.0,
                    "{rung} requires at least 25% primary CPU headroom",
                ),
            ),
            because="consistent transactional snapshot export with bounded impact",
        ),
        decide.Rung(
            "keyset_chunked_handoff",
            "keyset-chunked incremental copy with bounded lock duration",
            needs=(
                decide.Need(
                    "table has monotonic or indexed key",
                    lambda f: bool(f.get("has_indexed_key", True)),
                    "{rung} requires table to have an indexed primary or unique key",
                ),
            ),
            because="keyset-chunked copy bounds lock duration and avoids long-lived transactions",
        ),
        decide.Rung(
            "bounded_table_scan_fallback",
            "fallback bounded table scan",
            because="fallback table scan when advanced handoff strategies are inadmissible",
        ),
    )


def decide_snapshot_handoff(facts: Dict[str, Any]) -> decide.Choice:
    """Climbs snapshot_handoff_rungs decide ladder."""
    return decide.climb("snapshot handoff selection", snapshot_handoff_rungs(), facts)


class SnapshotWatermarkCoordinator:
    """Orchestrates strategy selection, pricing, execution, and coverage verification."""

    def __init__(self, registry: Optional[HandoffStrategyRegistry] = None) -> None:
        self.registry = registry or HandoffStrategyRegistry()

    def evaluate_all_strategies(
        self,
        source: SourceProfile,
        table: TableProfile,
        copy_rate_bytes_per_sec: float = 10.0 * 1024 * 1024,
    ) -> List[HandoffPricing]:
        """Price all registered alternatives."""
        results = []
        for strat in self.registry.list_strategies():
            results.append(strat.price(source, table, copy_rate_bytes_per_sec))
        return results

    def select_best_strategy(
        self,
        source: SourceProfile,
        table: TableProfile,
        copy_rate_bytes_per_sec: float = 10.0 * 1024 * 1024,
    ) -> Tuple[BaseHandoffStrategy, HandoffPricing]:
        """Select optimal viable strategy by climbing snapshot_handoff_rungs decide ladder."""
        evals = self.evaluate_all_strategies(source, table, copy_rate_bytes_per_sec)
        viable_map = {p.strategy: p for p in evals if p.is_viable}

        if not viable_map:
            reasons = [f"{p.strategy}: {p.rejection_reason or p.care_reasons}" for p in evals]
            raise SnapshotWatermarkError(
                f"No viable handoff strategy available. Evaluations: {'; '.join(reasons)}"
            )

        facts = {
            "is_standby_or_replica": bool(getattr(source, "is_standby", False) or getattr(source, "is_replica", False) or source.is_read_only),
            "supports_snapshot_export": bool(getattr(source, "supports_snapshot_export", True) and (source.supports_shared_snapshot or source.supports_engine_native)),
            "primary_cpu_headroom_pct": float(getattr(source, "primary_cpu_headroom_pct", 100.0)),
            "has_indexed_key": bool(getattr(table, "has_primary_key", False) or getattr(table, "primary_key", None) or getattr(table, "indexed_columns", None)),
        }
        choice = decide_snapshot_handoff(facts)

        strategy_map = {
            "zero_impact_logical_export": "pg_physical_standby_export",
            "consistent_primary_export": "pg_consistent_export_snapshot",
            "keyset_chunked_handoff": "keyset_chunked_handoff",
            "bounded_table_scan_fallback": "single_transaction_read_only",
        }
        target_strat = strategy_map.get(choice.path, "single_transaction_read_only")

        if target_strat in viable_map:
            chosen_pricing = viable_map[target_strat]
        else:
            viable = list(viable_map.values())
            viable.sort(key=lambda p: (p.source_impact_score, p.snapshot_lifetime_seconds))
            chosen_pricing = viable[0]

        strategy = self.registry.get(chosen_pricing.strategy)
        return strategy, chosen_pricing

    def prepare_handoff(
        self,
        strategy_type: str,
        source: SourceProfile,
        table: TableProfile,
    ) -> HandoffState:
        """Validate capability and initialize handoff state."""
        strat = self.registry.get(strategy_type)
        strat.validate_capability(source)

        handoff_id = f"handoff-{uuid.uuid4().hex[:8]}"
        state = HandoffState(
            handoff_id=handoff_id,
            strategy=strategy_type,
            source_host=source.host,
            table_name=table.table_name,
            status="prepared",
            metadata={
                "engine": source.engine,
                "read_only": source.is_read_only,
                "requires_signaling_writes": strat.requires_signaling_writes,
            },
        )
        return state

    def verify_coverage(
        self,
        snapshot_cut: Any,
        log_intervals: Sequence[LogInterval],
        high_watermark: Optional[Any] = None,
        engine: str = "generic",
        strict: bool = False,
    ) -> ContinuityReport:
        """Verify stream continuity from snapshot cut and accept/refuse copy coverage."""
        return LogContinuityVerifier.verify_continuity(
            snapshot_cut=snapshot_cut,
            log_intervals=log_intervals,
            high_watermark=high_watermark,
            engine=engine,
            strict=strict,
        )

    def render_report(
        self,
        pricing: HandoffPricing,
        source: SourceProfile,
        table: TableProfile,
        continuity: Optional[ContinuityReport] = None,
    ) -> str:
        """Pure UNIX style report with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS)."""
        lines = []
        lines.append("=" * 80)
        overall_badge = pricing.care_badge
        if continuity and continuity.care_badge == CareBadge.ACTION.value:
            overall_badge = CareBadge.ACTION.value
        elif continuity and overall_badge == CareBadge.PASS.value:
            overall_badge = continuity.care_badge

        lines.append(f"SNAPSHOT-TO-STREAM HANDOFF REPORT: {overall_badge}")
        lines.append("=" * 80)
        source_mode = "read_only" if source.is_read_only else "read_write"
        lines.append(
            f"Strategy: {pricing.strategy} | Table: {table.table_name} | "
            f"Source: {source.host} ({source_mode}) | Engine: {source.engine}"
        )
        lines.append("-" * 80)
        lines.append(f"{'DIMENSION':<22} {'VALUE':<14} {'CARE BADGE':<12} {'REASON'}")
        lines.append("-" * 80)

        # Snapshot lifetime
        life_str = f"{pricing.snapshot_lifetime_seconds:.1f}s"
        life_badge = CareBadge.REVIEW.value if pricing.snapshot_lifetime_seconds > 3600.0 else CareBadge.PASS.value
        life_reason = "transient read" if pricing.snapshot_lifetime_seconds < 120.0 else "extended duration"
        lines.append(f"{'Snapshot Lifetime':<22} {life_str:<14} {life_badge:<12} {life_reason}")

        # WAL Retention
        wal_mb = pricing.wal_retention_bytes / (1024 * 1024)
        wal_str = f"{wal_mb:.1f} MB"
        wal_badge = CareBadge.ACTION.value if not pricing.is_viable and "WAL" in (pricing.rejection_reason or "") else (
            CareBadge.REVIEW.value if pricing.estimated_disk_usage_fraction > 0.5 else CareBadge.PASS.value
        )
        wal_reason = f"{pricing.estimated_disk_usage_fraction*100:.1f}% disk headroom"
        lines.append(f"{'WAL Retention':<22} {wal_str:<14} {wal_badge:<12} {wal_reason}")

        # Source Impact
        impact_str = f"{pricing.source_impact_score:.1f} / 100"
        impact_badge = CareBadge.REVIEW.value if pricing.source_impact_score > 70.0 else CareBadge.PASS.value
        impact_reason = "low connection/lock contention" if pricing.source_impact_score <= 50.0 else "moderate load"
        lines.append(f"{'Source Impact':<22} {impact_str:<14} {impact_badge:<12} {impact_reason}")

        # Signaling writes
        sig_str = "REQUIRED" if pricing.requires_signaling_writes else "NONE"
        if pricing.requires_signaling_writes and source.is_read_only:
            sig_badge = CareBadge.ACTION.value
            sig_reason = "source is read-only; writes refused"
        else:
            sig_badge = CareBadge.PASS.value
            sig_reason = "allowed or not needed"
        lines.append(f"{'Signaling Writes':<22} {sig_str:<14} {sig_badge:<12} {sig_reason}")

        # Continuity & coverage
        if continuity is not None:
            cont_str = "CONTINUOUS" if continuity.is_continuous else "GAP DETECTED"
            cont_badge = continuity.care_badge
            cont_reason = "; ".join(continuity.reasons[:1]) if continuity.reasons else "zero gaps"
            lines.append(f"{'Log Continuity':<22} {cont_str:<14} {cont_badge:<12} {cont_reason}")

            cov_str = "ACCEPTED" if continuity.accept_coverage else "REFUSED"
            cov_badge = CareBadge.PASS.value if continuity.accept_coverage else CareBadge.ACTION.value
            cov_reason = "coverage verified complete" if continuity.accept_coverage else "gaps block cutover"
            lines.append(f"{'Copy Coverage':<22} {cov_str:<14} {cov_badge:<12} {cov_reason}")

        lines.append("-" * 80)
        lines.append(f"OVERALL STATUS: {overall_badge}")
        lines.append("=" * 80)
        return "\n".join(lines)


HandoffStrategyCoordinator = SnapshotWatermarkCoordinator

