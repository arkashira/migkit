import copy
import dataclasses
import decimal
import hashlib
import json
import os
import socket
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable, lease
from .lease import ActiveLease


class ReverseContractError(Exception):
    pass


class ZeroLossFailbackIneligibleError(ReverseContractError):
    def __init__(self, message: str, findings: Optional[Sequence[Any]] = None):
        super().__init__(message)
        self.findings = list(findings or [])


class FencePreparationError(ReverseContractError):
    pass


class LeaseLockError(ReverseContractError):
    pass


class TypeIncompatibilityError(ReverseContractError):
    pass


class MappingInvertibilityError(ReverseContractError):
    pass


class KeylessReplicationError(ReverseContractError):
    pass


class SequenceOverlapError(ReverseContractError):
    pass


class DDLReplicationError(ReverseContractError):
    pass


class UncapturedWriteIntervalError(ReverseContractError):
    pass


class UnauthorizedTargetError(ReverseContractError):
    pass


class CareBadge(str, Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class FallbackStrategy(str, Enum):
    ORIGINAL_SOURCE_FALLBACK = "ORIGINAL_SOURCE_FALLBACK"
    THIRD_TARGET_FALL_FORWARD = "THIRD_TARGET_FALL_FORWARD"
    DISALLOWED = "DISALLOWED"


class ReverseCaptureState(str, Enum):
    UNARMED = "UNARMED"
    PREPARING = "PREPARING"
    ARMED_AT_FENCE = "ARMED_AT_FENCE"
    ACTIVE = "ACTIVE"
    STALE = "STALE"
    FAILED = "FAILED"


class ReplicaIdentity(str, Enum):
    DEFAULT = "DEFAULT"
    NOTHING = "NOTHING"
    FULL = "FULL"
    INDEX = "INDEX"


@dataclass(frozen=True)
class ContractFinding:
    badge: CareBadge
    code: str
    category: str
    object_name: str
    message: str
    action_required: Optional[str] = None

    def format_unix(self) -> str:
        base = f"[{self.badge.value}] {self.category}.{self.code} object={self.object_name}: {self.message}"
        if self.action_required:
            base += f" (action: {self.action_required})"
        return base


@dataclass(frozen=True)
class TypeSpec:
    table: str
    column: str
    source_type: str
    target_type: str
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None
    custom_rules: Dict[str, Any] = field(default_factory=dict)

    def object_name(self) -> str:
        return f"{self.table}.{self.column}"


@dataclass(frozen=True)
class ColumnMappingSpec:
    target_table: str
    target_column: str
    source_table: str
    source_column: Optional[str] = None
    is_identity: bool = True
    transform_expr: Optional[str] = None
    reverse_expr: Optional[str] = None
    is_invertible: bool = True
    is_synthetic: bool = False
    has_source_default: bool = False
    is_dropped: bool = False
    is_masked: bool = False

    def object_name(self) -> str:
        return f"{self.target_table}.{self.target_column} -> {self.source_table}.{self.source_column or '<none>'}"


@dataclass(frozen=True)
class TableKeySpec:
    table_name: str
    primary_key_columns: Tuple[str, ...] = ()
    unique_indexes: Tuple[Tuple[str, ...], ...] = ()
    replica_identity: ReplicaIdentity = ReplicaIdentity.DEFAULT
    has_keyless_updates: bool = False
    supports_synthetic_tuple_id: bool = False

    @property
    def has_primary_or_unique_key(self) -> bool:
        return bool(self.primary_key_columns or self.unique_indexes)


@dataclass(frozen=True)
class SequenceRangeSpec:
    sequence_name: str
    table_name: str
    column_name: str
    source_allocated_max: int
    target_next_val: int
    reservation_gap: int = 1000000
    allocation_partition: Optional[str] = None


@dataclass(frozen=True)
class DDLEventSpec:
    event_id: str
    object_type: str
    object_name: str
    ddl_sql: str
    applied_post_promotion: bool = False
    captured_in_reverse_cdc: bool = True
    applied_to_fallback_target: bool = False
    is_destructive: bool = False
    nullable_addition: bool = False


@dataclass(frozen=True)
class WriteInterval:
    interval_id: str
    start_lsn: int
    end_lsn: int
    start_timestamp: float
    end_timestamp: float
    covered_by_reverse_capture: bool = True
    writer_client_ids: Tuple[str, ...] = ()
    transaction_count: int = 0


@dataclass(frozen=True)
class TargetEndpointSpec:
    endpoint_id: str
    strategy: FallbackStrategy
    host: str
    port: int
    database: str
    is_authorized: bool = True
    read_only_fenced: bool = True
    storage_headroom_bytes: int = 100000000
    schema_version: str = "1.0.0"
    is_healthy: bool = True
    replication_lag_seconds: float = 0.0
    sla_max_lag_seconds: float = 60.0


def fallback_target_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for selecting and qualifying reverse failback target endpoints.

    Evaluates measured target health, write-fencing (read_only), and replication lag against SLA.
    """
    return (
        decide.Rung(
            "original_source_fallback",
            "fallback to write-fenced original source database",
            gives=frozenset({"target_fallback", "original_source", "zero_loss"}),
            needs=(
                decide.Need(
                    "healthy",
                    lambda f: bool(f.get("healthy")),
                    "{rung} requires target endpoint to be healthy",
                ),
                decide.Need(
                    "read_only",
                    lambda f: bool(f.get("read_only")),
                    "{rung} requires target endpoint to be write-fenced and read-only",
                ),
                decide.Need(
                    "lag_under_sla",
                    lambda f: float(f.get("replication_lag_seconds", 0.0)) <= float(f.get("sla_max_lag_seconds", 60.0)),
                    "{rung} requires replication lag within SLA bound",
                ),
            ),
            because="original source preserves identity and schema with zero divergence",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "third_target_fall_forward",
            "fall forward to authorized third target",
            gives=frozenset({"target_fallback", "third_target", "fall_forward"}),
            needs=(
                decide.Need(
                    "healthy",
                    lambda f: bool(f.get("healthy")),
                    "{rung} requires target endpoint to be healthy",
                ),
                decide.Need(
                    "read_only",
                    lambda f: bool(f.get("read_only")),
                    "{rung} requires target endpoint to be write-fenced and read-only",
                ),
                decide.Need(
                    "lag_under_sla",
                    lambda f: float(f.get("replication_lag_seconds", 0.0)) <= float(f.get("sla_max_lag_seconds", 60.0)),
                    "{rung} requires replication lag within SLA bound",
                ),
            ),
            because="fall forward routes replication to clean independent destination",
            footprint=1,
            leaves="nothing",
        ),
    )


class TypeReverseContract:
    """Evaluates type fidelity in the reverse (target -> source/fallback) path."""

    _LOSSY_CONVERSIONS = {
        ("double", "float"): ("precision loss: 64-bit float into 32-bit float", CareBadge.ACTION),
        ("double precision", "real"): ("precision loss: double precision into real", CareBadge.ACTION),
        ("numeric", "float"): ("exact decimal into floating point representation", CareBadge.ACTION),
        ("decimal", "float"): ("exact decimal into floating point representation", CareBadge.ACTION),
        ("json", "text"): ("json validation and path query lost without schema check", CareBadge.BENIGN),
        ("jsonb", "text"): ("binary json capabilities stripped down to text", CareBadge.BENIGN),
        ("timestamptz", "timestamp"): ("timezone offset stripped in reverse destination", CareBadge.ACTION),
        ("timestamp with time zone", "timestamp without time zone"): (
            "timezone offset lost in reverse conversion",
            CareBadge.ACTION,
        ),
        ("uuid", "varchar(32)"): ("uuid delimiter stripped in compact varchar", CareBadge.BENIGN),
        ("uuid", "int"): ("incompatible type family: uuid cannot map to int", CareBadge.ACTION),
        ("blob", "text"): ("binary byte stream cast into text encoding", CareBadge.ACTION),
    }

    def evaluate(self, specs: Sequence[TypeSpec]) -> List[ContractFinding]:
        findings: List[ContractFinding] = []
        for s in specs:
            src = s.source_type.strip().lower()
            tgt = s.target_type.strip().lower()

            if "enum" in tgt or "enum" in src or "target_enum_values" in s.custom_rules:
                tgt_vals = set(s.custom_rules.get("target_enum_values", []))
                src_vals = set(s.custom_rules.get("source_enum_values", []))
                if tgt_vals and src_vals and not tgt_vals.issubset(src_vals):
                    missing = sorted(list(tgt_vals - src_vals))
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.ACTION,
                            code="ENUM_VALUE_DISPARITY",
                            category="type",
                            object_name=s.object_name(),
                            message=f"target enum includes new values {missing} absent in source enum",
                            action_required="synchronize source enum variants before claiming failback",
                        )
                    )
                    continue

            if src == tgt:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="EXACT_TYPE_MATCH",
                        category="type",
                        object_name=s.object_name(),
                        message=f"symmetric type {src} verified",
                    )
                )
                continue

            pair = (tgt, src)
            if pair in self._LOSSY_CONVERSIONS:
                msg, badge = self._LOSSY_CONVERSIONS[pair]
                action = "implement explicit bidirectional type serializer" if badge == CareBadge.ACTION else None
                findings.append(
                    ContractFinding(
                        badge=badge,
                        code="LOSSY_REVERSE_TYPE",
                        category="type",
                        object_name=s.object_name(),
                        message=f"target {tgt} to source {src}: {msg}",
                        action_required=action,
                    )
                )
                continue

            if tgt.startswith("varchar") and src.startswith("varchar"):
                try:
                    tgt_len = int(tgt.split("(")[1].split(")")[0])
                    src_len = int(src.split("(")[1].split(")")[0])
                    if tgt_len > src_len:
                        findings.append(
                            ContractFinding(
                                badge=CareBadge.ACTION,
                                code="STRING_TRUNCATION_RISK",
                                category="type",
                                object_name=s.object_name(),
                                message=f"target varchar({tgt_len}) exceeds source capacity varchar({src_len})",
                                action_required=f"widen source column to varchar({tgt_len}) before failback",
                            )
                        )
                        continue
                    else:
                        findings.append(
                            ContractFinding(
                                badge=CareBadge.PASS,
                                code="SAFE_STRING_SUBSET",
                                category="type",
                                object_name=s.object_name(),
                                message=f"target varchar({tgt_len}) fits inside source varchar({src_len})",
                            )
                        )
                        continue
                except (IndexError, ValueError):
                    pass

            if "enum" in tgt or "enum" in src:
                tgt_vals = set(s.custom_rules.get("target_enum_values", []))
                src_vals = set(s.custom_rules.get("source_enum_values", []))
                if tgt_vals and src_vals and not tgt_vals.issubset(src_vals):
                    missing = sorted(list(tgt_vals - src_vals))
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.ACTION,
                            code="ENUM_VALUE_DISPARITY",
                            category="type",
                            object_name=s.object_name(),
                            message=f"target enum includes new values {missing} absent in source enum",
                            action_required="synchronize source enum variants before claiming failback",
                        )
                    )
                    continue

            findings.append(
                ContractFinding(
                    badge=CareBadge.BENIGN,
                    code="COMPATIBLE_TYPE_CONVERSION",
                    category="type",
                    object_name=s.object_name(),
                    message=f"conversion {tgt} -> {src} accepted under coercion rules",
                )
            )
        return findings


class MappingReverseContract:
    """Evaluates whether column transformations can be cleanly inverted."""

    def evaluate(self, specs: Sequence[ColumnMappingSpec]) -> List[ContractFinding]:
        findings: List[ContractFinding] = []
        for s in specs:
            obj = s.object_name()
            if s.is_masked:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="IRREVERSIBLE_MASKED_COLUMN",
                        category="mapping",
                        object_name=obj,
                        message="one-way masked or hashed column loses original plaintext during forward migration",
                        action_required="failback requires source-of-truth lookup or token vault to unmask",
                    )
                )
                continue

            if s.is_dropped:
                if s.has_source_default:
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.BENIGN,
                            code="DROPPED_COLUMN_HAS_DEFAULT",
                            category="mapping",
                            object_name=obj,
                            message="dropped column in target has default value in source; reverse insert safe",
                        )
                    )
                else:
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.ACTION,
                            code="DROPPED_NOT_NULL_NO_DEFAULT",
                            category="mapping",
                            object_name=obj,
                            message="column dropped on target is non-nullable and lacks default on source",
                            action_required="assign default value on source column or configure synthetic fallback",
                        )
                    )
                continue

            if s.is_identity:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="IDENTITY_MAPPING",
                        category="mapping",
                        object_name=obj,
                        message="direct 1-to-1 bijective identity mapping verified",
                    )
                )
                continue

            if not s.is_invertible:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="NON_INVERTIBLE_TRANSFORM",
                        category="mapping",
                        object_name=obj,
                        message=f"forward expression '{s.transform_expr}' has no verified reverse mapping",
                        action_required="define explicit reverse transform expression or preserve raw values",
                    )
                )
                continue

            findings.append(
                ContractFinding(
                    badge=CareBadge.PASS,
                    code="INVERTIBLE_TRANSFORM_VERIFIED",
                    category="mapping",
                    object_name=obj,
                    message=f"bidirectional transform verified: rev('{s.reverse_expr}')",
                )
            )
        return findings


class KeyReverseContract:
    """Evaluates row identity and sequence reservation to prevent overwrite/collision."""

    def evaluate_keys(self, specs: Sequence[TableKeySpec]) -> List[ContractFinding]:
        findings: List[ContractFinding] = []
        for s in specs:
            obj = s.table_name
            if s.has_primary_or_unique_key:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="KEYED_ROW_IDENTITY",
                        category="key",
                        object_name=obj,
                        message=f"deterministic identity verified: {s.primary_key_columns or s.unique_indexes}",
                    )
                )
                continue

            if s.has_keyless_updates:
                if s.replica_identity == ReplicaIdentity.FULL:
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.BENIGN,
                            code="KEYLESS_REPLICA_IDENTITY_FULL",
                            category="key",
                            object_name=obj,
                            message="keyless table has REPLICA IDENTITY FULL; before-image available for CDC",
                        )
                    )
                elif s.supports_synthetic_tuple_id:
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.REVIEW,
                            code="KEYLESS_SYNTHETIC_TUPLE_ID",
                            category="key",
                            object_name=obj,
                            message="keyless table relies on synthetic tuple hash for deduplication",
                        )
                    )
                else:
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.ACTION,
                            code="KEYLESS_UPDATE_AMBIGUITY",
                            category="key",
                            object_name=obj,
                            message="table lacks primary key and replica identity is not FULL; updates risk ambiguous row matching",
                            action_required="set REPLICA IDENTITY FULL or add primary key constraint before failback",
                        )
                    )
            else:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.BENIGN,
                        code="KEYLESS_APPEND_ONLY",
                        category="key",
                        object_name=obj,
                        message="keyless table has no in-flight updates/deletes; append-only replay supported",
                    )
                )
        return findings

    def evaluate_sequences(self, specs: Sequence[SequenceRangeSpec]) -> List[ContractFinding]:
        findings: List[ContractFinding] = []
        for s in specs:
            obj = f"{s.table_name}.{s.column_name} ({s.sequence_name})"
            if s.allocation_partition:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="SEQUENCE_PARTITIONED",
                        category="key",
                        object_name=obj,
                        message=f"sequence partition configured: {s.allocation_partition}",
                    )
                )
                continue

            if s.target_next_val <= s.source_allocated_max:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="SEQUENCE_COLLISION_RISK",
                        category="key",
                        object_name=obj,
                        message=(
                            f"target next_val {s.target_next_val} <= source allocated max {s.source_allocated_max}; "
                            "reverse writes will collide with existing source primary keys"
                        ),
                        action_required=f"re-seed target sequence above {s.source_allocated_max + s.reservation_gap}",
                    )
                )
                continue

            gap = s.target_next_val - s.source_allocated_max
            if gap < s.reservation_gap:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.REVIEW,
                        code="SEQUENCE_GAP_NARROW",
                        category="key",
                        object_name=obj,
                        message=f"sequence reservation gap {gap} is below target threshold {s.reservation_gap}",
                        action_required="increase sequence offset margin to absorb burst writes",
                    )
                )
            else:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="SEQUENCE_FENCE_SAFE",
                        category="key",
                        object_name=obj,
                        message=f"safe sequence reservation gap {gap} >= {s.reservation_gap}",
                    )
                )
        return findings


class DDLReverseContract:
    """Evaluates post-promotion DDL events and schema changes."""

    def evaluate(self, events: Sequence[DDLEventSpec]) -> List[ContractFinding]:
        findings: List[ContractFinding] = []
        for ev in events:
            obj = f"{ev.object_type} {ev.object_name}"
            if not ev.applied_post_promotion:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="PRE_PROMOTION_DDL_ALIGNED",
                        category="ddl",
                        object_name=obj,
                        message="schema element established prior to promotion",
                    )
                )
                continue

            if ev.is_destructive:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="DESTRUCTIVE_POST_PROMOTION_DDL",
                        category="ddl",
                        object_name=obj,
                        message=f"destructive schema change '{ev.ddl_sql}' executed after promotion",
                        action_required="reconstruct dropped object on source or obtain explicit waiver",
                    )
                )
                continue

            if not ev.captured_in_reverse_cdc:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="NEW_OBJECT_UNREPRESENTED_IN_REVERSE",
                        category="ddl",
                        object_name=obj,
                        message="post-promotion object is missing in reverse CDC replication stream table whitelist",
                        action_required="add object to reverse capture connector schema filter",
                    )
                )
                continue

            if not ev.applied_to_fallback_target:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="SCHEMA_DRIFT_FALLBACK_DESTINATION",
                        category="ddl",
                        object_name=obj,
                        message="post-promotion DDL not yet mirrored onto fallback destination database",
                        action_required="replay DDL on fallback target before triggering cutover/failback",
                    )
                )
                continue

            if ev.nullable_addition:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="SAFE_SCHEMA_EXTENSION",
                        category="ddl",
                        object_name=obj,
                        message="backward-compatible nullable column added post-promotion",
                    )
                )
            else:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.BENIGN,
                        code="POST_PROMOTION_DDL_SYNCHRONIZED",
                        category="ddl",
                        object_name=obj,
                        message="DDL change captured in reverse CDC and verified on fallback target",
                    )
                )
        return findings


class CommitOrderingReverseContract:
    """Evaluates transaction boundaries, monotonic timestamps, and write interval gaps."""

    def evaluate(self, intervals: Sequence[WriteInterval], reverse_armed_lsn: int) -> List[ContractFinding]:
        findings: List[ContractFinding] = []
        if not intervals:
            findings.append(
                ContractFinding(
                    badge=CareBadge.PASS,
                    code="NO_TARGET_WRITES_DETECTED",
                    category="commit_ordering",
                    object_name="transaction_log",
                    message="no write intervals recorded post-promotion",
                )
            )
            return findings

        sorted_intervals = sorted(intervals, key=lambda x: x.start_lsn)
        last_end_lsn: Optional[int] = None
        last_end_ts: Optional[float] = None

        for iv in sorted_intervals:
            obj = f"interval_{iv.interval_id}[{iv.start_lsn}..{iv.end_lsn}]"

            if iv.start_lsn < reverse_armed_lsn:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="WRITES_BEFORE_REVERSE_ARMED",
                        category="commit_ordering",
                        object_name=obj,
                        message=(
                            f"interval start LSN {iv.start_lsn} precedes reverse capture arming LSN {reverse_armed_lsn}; "
                            "writes occurred before reverse stream was initialized"
                        ),
                        action_required="refuse zero-loss failback; writes in uncaptured interval are unrecoverable",
                    )
                )

            if not iv.covered_by_reverse_capture:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="UNCAPTURED_WRITE_INTERVAL",
                        category="commit_ordering",
                        object_name=obj,
                        message="write interval was skipped or dropped by reverse CDC pipeline",
                        action_required="resync write interval from target WAL/binlog before failback",
                    )
                )

            if last_end_lsn is not None and iv.start_lsn > last_end_lsn:
                gap = iv.start_lsn - last_end_lsn
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="COMMIT_LSN_GAP",
                        category="commit_ordering",
                        object_name=obj,
                        message=f"detected LSN gap of {gap} between {last_end_lsn} and {iv.start_lsn}",
                        action_required="verify target engine transaction logs for dropped transactions",
                    )
                )

            if last_end_ts is not None and iv.start_timestamp < last_end_ts:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.REVIEW,
                        code="COMMIT_TIMESTAMP_CLOCK_SKEW",
                        category="commit_ordering",
                        object_name=obj,
                        message=f"timestamp moved backwards from {last_end_ts} to {iv.start_timestamp}",
                        action_required="inspect NTP synchronization across target cluster writers",
                    )
                )

            last_end_lsn = iv.end_lsn
            last_end_ts = iv.end_timestamp

        if not any(f.badge == CareBadge.ACTION for f in findings):
            findings.append(
                ContractFinding(
                    badge=CareBadge.PASS,
                    code="COMMIT_ORDERING_CONTINUOUS",
                    category="commit_ordering",
                    object_name="transaction_log",
                    message="continuous monotonic commit ordering verified across all write intervals",
                )
            )
        return findings


class FencePreparationContract:
    """Ensures reverse capture is armed at the forward fence *before* new target writes."""

    def __init__(self):
        self.state = ReverseCaptureState.UNARMED
        self.armed_fence_lsn: Optional[int] = None
        self.armed_timestamp: Optional[float] = None
        self.target_opened_for_writes: bool = False
        self.target_opened_timestamp: Optional[float] = None

    def arm_at_forward_fence(self, fence_lsn: int, target_is_write_fenced: bool) -> None:
        if not target_is_write_fenced:
            self.state = ReverseCaptureState.STALE
            raise FencePreparationError("refusing to arm reverse capture: target is not write-fenced")

        self.state = ReverseCaptureState.ARMED_AT_FENCE
        self.armed_fence_lsn = fence_lsn
        self.armed_timestamp = time.time()

    def open_target_writes(self) -> None:
        if self.state != ReverseCaptureState.ARMED_AT_FENCE and self.state != ReverseCaptureState.ACTIVE:
            self.state = ReverseCaptureState.STALE
            raise FencePreparationError(
                f"cannot open target writes: reverse capture is not armed (current state: {self.state})"
            )
        self.target_opened_for_writes = True
        self.target_opened_timestamp = time.time()
        self.state = ReverseCaptureState.ACTIVE

    def evaluate(self) -> List[ContractFinding]:
        findings: List[ContractFinding] = []
        if self.state == ReverseCaptureState.UNARMED:
            findings.append(
                ContractFinding(
                    badge=CareBadge.ACTION,
                    code="REVERSE_CAPTURE_UNARMED",
                    category="fence",
                    object_name="forward_fence",
                    message="reverse capture has not been armed at the forward fence",
                    action_required="arm reverse capture at forward fence before permitting target promotion writes",
                )
            )
        elif self.state == ReverseCaptureState.STALE:
            findings.append(
                ContractFinding(
                    badge=CareBadge.ACTION,
                    code="REVERSE_CAPTURE_STALE",
                    category="fence",
                    object_name="forward_fence",
                    message="target received writes before reverse capture was armed; uncaptured write window exists",
                    action_required="zero-loss failback strictly impossible; rebuild baseline or take outage",
                )
            )
        elif self.state == ReverseCaptureState.ARMED_AT_FENCE:
            findings.append(
                ContractFinding(
                    badge=CareBadge.PASS,
                    code="REVERSE_CAPTURE_ARMED_AT_FENCE",
                    category="fence",
                    object_name="forward_fence",
                    message=f"reverse capture armed at fence LSN {self.armed_fence_lsn} prior to target writes",
                )
            )
        elif self.state == ReverseCaptureState.ACTIVE:
            findings.append(
                ContractFinding(
                    badge=CareBadge.PASS,
                    code="REVERSE_CAPTURE_ACTIVE",
                    category="fence",
                    object_name="forward_fence",
                    message="reverse capture is actively streaming target changes",
                )
            )
        return findings


class FallbackTargetSelector:
    """Chooses original-source fallback or third-target fall-forward where authorized."""

    def choose_target(
        self,
        strategy: FallbackStrategy,
        endpoint: TargetEndpointSpec,
        authorized_tokens: Optional[Set[str]] = None,
        token: Optional[str] = None,
        telemetry: Optional[Dict[str, Any]] = None,
    ) -> List[ContractFinding]:
        findings: List[ContractFinding] = []
        obj = f"{endpoint.host}:{endpoint.port}/{endpoint.database}"

        if not endpoint.host or not endpoint.host.strip():
            findings.append(
                ContractFinding(
                    badge=CareBadge.ACTION,
                    code="UNSAFE_TARGET_HOST",
                    category="target",
                    object_name=obj,
                    message="target endpoint host must not be empty",
                    action_required="specify valid target host",
                )
            )
            return findings

        if strategy == FallbackStrategy.DISALLOWED:
            findings.append(
                ContractFinding(
                    badge=CareBadge.ACTION,
                    code="FAILBACK_STRATEGY_DISALLOWED",
                    category="target",
                    object_name=obj,
                    message="failback has been explicitly marked DISALLOWED for this migration hop",
                    action_required="enable fallback strategy in migration configuration",
                )
            )
            return findings

        target_rung_name = (
            "original_source_fallback"
            if strategy == FallbackStrategy.ORIGINAL_SOURCE_FALLBACK
            else "third_target_fall_forward"
        )
        candidate_rungs = tuple(r for r in fallback_target_rungs() if r.name == target_rung_name)
        facts = {
            "healthy": (
                telemetry.get("healthy")
                if telemetry and "healthy" in telemetry
                else getattr(endpoint, "is_healthy", True)
            ),
            "read_only": (
                telemetry.get("read_only")
                if telemetry and "read_only" in telemetry
                else getattr(endpoint, "read_only_fenced", True)
            ),
            "replication_lag_seconds": (
                telemetry.get("replication_lag_seconds")
                if telemetry and "replication_lag_seconds" in telemetry
                else getattr(endpoint, "replication_lag_seconds", 0.0)
            ),
            "sla_max_lag_seconds": (
                telemetry.get("sla_max_lag_seconds")
                if telemetry and "sla_max_lag_seconds" in telemetry
                else getattr(endpoint, "sla_max_lag_seconds", 60.0)
            ),
        }
        choice = decide.climb("fallback_target", candidate_rungs, facts)

        if choice.rung is None:
            refused_reason = choice.why_not or (choice.passed[0][1] if choice.passed else "target qualification refused")
            if not facts["healthy"]:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="TARGET_UNHEALTHY",
                        category="target",
                        object_name=obj,
                        message=f"target endpoint health check failed: {refused_reason}",
                        action_required="restore target endpoint health before failback",
                    )
                )
            elif not facts["read_only"]:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="SOURCE_NOT_READ_ONLY_FENCED",
                        category="target",
                        object_name=obj,
                        message="original source database is not read-only fenced; out-of-band writes may conflict",
                        action_required="apply read-only fence to original source database",
                    )
                )
            elif facts["replication_lag_seconds"] > facts["sla_max_lag_seconds"]:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="REPLICATION_LAG_EXCEEDS_SLA",
                        category="target",
                        object_name=obj,
                        message=f"target replication lag ({facts['replication_lag_seconds']}s) exceeds SLA threshold ({facts['sla_max_lag_seconds']}s)",
                        action_required="wait for target replication catch-up before failback",
                    )
                )
            else:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.ACTION,
                        code="TARGET_QUALIFICATION_REFUSED",
                        category="target",
                        object_name=obj,
                        message=f"target endpoint refused by decision ladder: {refused_reason}",
                        action_required="satisfy target qualification criteria",
                    )
                )
        else:
            if strategy == FallbackStrategy.ORIGINAL_SOURCE_FALLBACK:
                findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="ORIGINAL_SOURCE_FALLBACK_AUTHORIZED",
                        category="target",
                        object_name=obj,
                        message="original source database verified and write-fenced for reverse replication",
                    )
                )
            elif strategy == FallbackStrategy.THIRD_TARGET_FALL_FORWARD:
                tokens = authorized_tokens or set()
                is_valid_auth = endpoint.is_authorized or (token is not None and token in tokens)
                if not is_valid_auth:
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.ACTION,
                            code="UNAUTHORIZED_FALL_FORWARD_TARGET",
                            category="target",
                            object_name=obj,
                            message="third-target fall-forward destination lacks explicit authorization",
                            action_required="authorize destination endpoint in security policy with valid token",
                        )
                    )
                else:
                    findings.append(
                        ContractFinding(
                            badge=CareBadge.REVIEW,
                            code="THIRD_TARGET_FALL_FORWARD_AUTHORIZED",
                            category="target",
                            object_name=obj,
                            message="third-target fall-forward selected; operational review required before cutover",
                        )
                    )

        if endpoint.storage_headroom_bytes < 50 * 1024 * 1024:
            findings.append(
                ContractFinding(
                    badge=CareBadge.ACTION,
                    code="INSUFFICIENT_STORAGE_HEADROOM",
                    category="target",
                    object_name=obj,
                    message=f"storage headroom {endpoint.storage_headroom_bytes}B is below safe 50MB threshold",
                    action_required="expand destination disk headroom before proceeding",
                )
            )
        else:
            findings.append(
                ContractFinding(
                    badge=CareBadge.BENIGN,
                    code="HEADROOM_SUFFICIENT",
                    category="target",
                    object_name=obj,
                    message=f"storage headroom verified: {endpoint.storage_headroom_bytes}B",
                )
            )

        return findings


@dataclass
class FailbackPreflightReport:
    session_id: str
    timestamp: float
    overall_badge: CareBadge
    can_claim_zero_loss: bool
    failback_ready: bool
    fallback_strategy: FallbackStrategy
    findings: List[ContractFinding] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)

    def assert_zero_loss_eligible(self) -> None:
        if not self.can_claim_zero_loss:
            actions = [f.format_unix() for f in self.findings if f.badge == CareBadge.ACTION]
            err_msg = "zero-loss failback claim refused due to blocking defects:\n" + "\n".join(actions)
            raise ZeroLossFailbackIneligibleError(err_msg, findings=self.findings)

    def format_unix(self) -> str:
        lines = [
            f"=== PREFLIGHT REVERSIBILITY FAILBACK CONTRACT REPORT ===",
            f"session_id: {self.session_id}",
            f"timestamp: {self.timestamp:.3f}",
            f"overall_badge: [{self.overall_badge.value}]",
            f"zero_loss_eligible: {self.can_claim_zero_loss}",
            f"failback_ready: {self.failback_ready}",
            f"strategy: {self.fallback_strategy.value}",
            "--- FINDINGS ---",
        ]
        for f in self.findings:
            lines.append(f.format_unix())
        lines.append("--- END OF REPORT ---")
        return "\n".join(lines)


class ReverseContractLease:
    """Active verification lease lock (.run.lock) backed by migkit.lease.ActiveLease."""

    def __init__(self, lock_dir: Union[str, Path], ttl_seconds: float = 30.0):
        self.lock_dir = Path(lock_dir)
        self.lock_file = self.lock_dir / ".run.lock"
        self.ttl_seconds = float(ttl_seconds)
        self._lease = ActiveLease(
            self.lock_file,
            what="reverse contract verification",
            term=self.ttl_seconds,
        )
        self.owner_id = self._lease.me
        self.acquired = False

    def acquire(self) -> "ReverseContractLease":
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._lease.acquire()
            self.acquired = True
            return self
        except lease.Held as exc:
            owner = "unknown"
            if self.lock_file.exists():
                try:
                    data = json.loads(self.lock_file.read_text(encoding="utf-8"))
                    owner = data.get("holder") or data.get("owner") or "unknown"
                except Exception:
                    pass
            raise LeaseLockError(
                f"active lease locked by owner '{owner}'"
            ) from exc

    def heartbeat(self) -> None:
        if not self.acquired:
            raise LeaseLockError("cannot heartbeat: lease not held")
        try:
            held = self._lease._renew_once()
            if not held:
                raise LeaseLockError("cannot heartbeat: lease stolen by another owner")
        except Exception as e:
            if isinstance(e, LeaseLockError):
                raise
            raise LeaseLockError(f"lease unreadable during heartbeat: {e}") from e

    def release(self) -> None:
        if not self.acquired:
            return
        self._lease.release()
        self.acquired = False

    def __enter__(self) -> "ReverseContractLease":
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


@dataclass
class DurableReverseCheckpoint:
    """Universal Typed Durable Resume in O(1) across evaluation cycles."""

    session_id: str
    checkpoint_dir: Path
    verified_hashes: Dict[str, str] = field(default_factory=dict)
    last_cursor_lsn: int = 0
    state_version: int = 1

    def _state_file(self) -> Path:
        return self.checkpoint_dir / f"reverse_contract_{self.session_id}.json"

    def record_verified(self, category: str, object_name: str, payload_hash: str) -> None:
        key = f"{category}:{object_name}"
        self.verified_hashes[key] = payload_hash

    def is_verified(self, category: str, object_name: str, current_hash: str) -> bool:
        key = f"{category}:{object_name}"
        return self.verified_hashes.get(key) == current_hash

    def save(self) -> None:
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "session_id": self.session_id,
            "last_cursor_lsn": self.last_cursor_lsn,
            "state_version": self.state_version,
            "verified_hashes": self.verified_hashes,
            "updated_at": time.time(),
        }
        durable.write_json(self._state_file(), payload, indent=2)

    @classmethod
    def load(cls, checkpoint_dir: Union[str, Path], session_id: str) -> "DurableReverseCheckpoint":
        d = Path(checkpoint_dir)
        sf = d / f"reverse_contract_{session_id}.json"
        data = durable.read_state(sf, kind="state")
        if not data or not isinstance(data, dict):
            return cls(session_id=session_id, checkpoint_dir=d)
        return cls(
            session_id=session_id,
            checkpoint_dir=d,
            verified_hashes=data.get("verified_hashes", {}),
            last_cursor_lsn=data.get("last_cursor_lsn", 0),
            state_version=data.get("state_version", 1),
        )


class ReverseContractAssessor:
    """Orchestrates comprehensive preflight reversibility verification before claiming failback."""

    def __init__(
        self,
        type_contract: Optional[TypeReverseContract] = None,
        mapping_contract: Optional[MappingReverseContract] = None,
        key_contract: Optional[KeyReverseContract] = None,
        ddl_contract: Optional[DDLReverseContract] = None,
        ordering_contract: Optional[CommitOrderingReverseContract] = None,
        fence_contract: Optional[FencePreparationContract] = None,
        target_selector: Optional[FallbackTargetSelector] = None,
    ):
        self.type_contract = type_contract or TypeReverseContract()
        self.mapping_contract = mapping_contract or MappingReverseContract()
        self.key_contract = key_contract or KeyReverseContract()
        self.ddl_contract = ddl_contract or DDLReverseContract()
        self.ordering_contract = ordering_contract or CommitOrderingReverseContract()
        self.fence_contract = fence_contract or FencePreparationContract()
        self.target_selector = target_selector or FallbackTargetSelector()

    def assess(
        self,
        session_id: str,
        strategy: FallbackStrategy,
        endpoint: TargetEndpointSpec,
        type_specs: Sequence[TypeSpec] = (),
        mapping_specs: Sequence[ColumnMappingSpec] = (),
        key_specs: Sequence[TableKeySpec] = (),
        sequence_specs: Sequence[SequenceRangeSpec] = (),
        ddl_events: Sequence[DDLEventSpec] = (),
        write_intervals: Sequence[WriteInterval] = (),
        checkpoint: Optional[DurableReverseCheckpoint] = None,
        authorization_token: Optional[str] = None,
        authorized_tokens: Optional[Set[str]] = None,
    ) -> FailbackPreflightReport:
        all_findings: List[ContractFinding] = []

        # Target Endpoint & Strategy
        target_findings = self.target_selector.choose_target(
            strategy=strategy,
            endpoint=endpoint,
            authorized_tokens=authorized_tokens,
            token=authorization_token,
        )
        all_findings.extend(target_findings)

        # Fence Preparation
        fence_findings = self.fence_contract.evaluate()
        all_findings.extend(fence_findings)

        # Type Contract with O(1) Checkpoint Skip
        unverified_types: List[TypeSpec] = []
        for ts in type_specs:
            spec_hash = hashlib.sha256(
                f"{ts.source_type}:{ts.target_type}:{ts.precision}:{ts.scale}".encode("utf-8")
            ).hexdigest()
            if checkpoint and checkpoint.is_verified("type", ts.object_name(), spec_hash):
                all_findings.append(
                    ContractFinding(
                        badge=CareBadge.PASS,
                        code="CHECKPOINTED_TYPE_VERIFIED",
                        category="type",
                        object_name=ts.object_name(),
                        message="type verified from O(1) durable checkpoint",
                    )
                )
            else:
                unverified_types.append(ts)

        if unverified_types:
            type_res = self.type_contract.evaluate(unverified_types)
            all_findings.extend(type_res)
            if checkpoint:
                for ts, f in zip(unverified_types, type_res):
                    if f.badge in (CareBadge.PASS, CareBadge.BENIGN):
                        spec_hash = hashlib.sha256(
                            f"{ts.source_type}:{ts.target_type}:{ts.precision}:{ts.scale}".encode("utf-8")
                        ).hexdigest()
                        checkpoint.record_verified("type", ts.object_name(), spec_hash)

        # Mapping Contract
        map_findings = self.mapping_contract.evaluate(mapping_specs)
        all_findings.extend(map_findings)

        # Key & Sequence Contract
        key_findings = self.key_contract.evaluate_keys(key_specs)
        all_findings.extend(key_findings)

        seq_findings = self.key_contract.evaluate_sequences(sequence_specs)
        all_findings.extend(seq_findings)

        # DDL & Schema Evolution Contract
        ddl_findings = self.ddl_contract.evaluate(ddl_events)
        all_findings.extend(ddl_findings)

        # Commit Ordering & Write Intervals
        armed_lsn = self.fence_contract.armed_fence_lsn or 0
        order_findings = self.ordering_contract.evaluate(write_intervals, reverse_armed_lsn=armed_lsn)
        all_findings.extend(order_findings)

        # Aggregate Care Badges
        has_action = any(f.badge == CareBadge.ACTION for f in all_findings)
        has_review = any(f.badge == CareBadge.REVIEW for f in all_findings)
        has_benign = any(f.badge == CareBadge.BENIGN for f in all_findings)

        if has_action:
            overall_badge = CareBadge.ACTION
        elif has_review:
            overall_badge = CareBadge.REVIEW
        elif has_benign:
            overall_badge = CareBadge.BENIGN
        else:
            overall_badge = CareBadge.PASS

        # Strict Zero-Loss Invariant
        can_claim_zero_loss = (overall_badge in (CareBadge.PASS, CareBadge.BENIGN, CareBadge.REVIEW)) and not has_action

        # Failback readiness is true only if zero-loss can be claimed and no unreviewed critical issues
        failback_ready = can_claim_zero_loss and not has_action

        summary = {
            "total_findings": len(all_findings),
            "action_count": sum(1 for f in all_findings if f.badge == CareBadge.ACTION),
            "review_count": sum(1 for f in all_findings if f.badge == CareBadge.REVIEW),
            "benign_count": sum(1 for f in all_findings if f.badge == CareBadge.BENIGN),
            "pass_count": sum(1 for f in all_findings if f.badge == CareBadge.PASS),
        }

        report = FailbackPreflightReport(
            session_id=session_id,
            timestamp=time.time(),
            overall_badge=overall_badge,
            can_claim_zero_loss=can_claim_zero_loss,
            failback_ready=failback_ready,
            fallback_strategy=strategy,
            findings=all_findings,
            summary=summary,
        )

        if checkpoint:
            if write_intervals:
                checkpoint.last_cursor_lsn = max(iv.end_lsn for iv in write_intervals)
            checkpoint.save()

        return report
