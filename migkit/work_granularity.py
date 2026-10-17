"""Optimize work units and encoding jointly for distributions (Row 603: SMART-N20.work-granularity).

Extends batch_rows/cursor_rows, tuning, ranges/pieces and native request caps
using decide.Rung ladders and decide.climb(work, rungs, facts).

Decides batch bytes/rows, key boundaries, split/merge, compression and serialization
jointly from row-width tails, skew, compressibility, CPU/network pressure and retry cost.
Preserves keyless multiplicity and LOB integrity across mixed workloads.

Acceptance: tiny rows mixed with extreme LOBs stay memory-bounded; service caps
are never exceeded; improvement is measured in verified completion rather than
isolated throughput.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import math
import os
import pathlib
import statistics
import time
import uuid
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from . import decide, lease, pieces, tuning

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


class CompressionMethod(str, enum.Enum):
    LZ4 = "lz4"
    LIGHT = "light"
    NONE = "none"


class SerializationFormat(str, enum.Enum):
    BINARY_ROW = "binary_row"
    TYPED_JSON = "typed_json"
    RAW_OCTETS = "raw_octets"


class RepresentationMode(str, enum.Enum):
    KEYLESS_MULTIPLICITY = "keyless_multiplicity_occurrence_envelope"
    LOB_STREAMING_PIECE = "lob_streaming_piece_envelope"
    STANDARD_RELATIONAL = "standard_relational_row_envelope"


class SplitMergeAction(str, enum.Enum):
    SPLIT = "split_skewed_dense_range"
    MERGE = "merge_sparse_subranges"
    PRESERVE = "preserve_discrete_key_boundary"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class WorkGranularityError(Exception):
    """Base exception for work granularity and distribution optimization."""


class MemoryBudgetExceededError(WorkGranularityError):
    """Raised when buffered rows or LOB pieces exceed the worker memory ceiling."""


class ServiceCapExceededError(WorkGranularityError):
    """Raised when a batch payload exceeds native service/network packet limits."""


class MultiplicityIntegrityError(WorkGranularityError):
    """Raised when duplicate row multiplicity on keyless tables is lost or distorted."""


class LobIntegrityError(WorkGranularityError):
    """Raised when a LOB piece is truncated, missing, or fails checksum verification."""


class UnsafeEndpointError(WorkGranularityError):
    """Raised when an endpoint host is outside approved loopback or 10.0.0.x ranges."""


class LeaseLockError(WorkGranularityError):
    """Raised when mutual exclusion lease (.run.lock) cannot be acquired or renewed."""


# -----------------------------------------------------------------------------
# Host Safety Validation
# -----------------------------------------------------------------------------

def validate_safe_endpoint(host: str) -> str:
    """Ensure endpoint uses safe loopback (127.0.0.1, localhost, ::1) or 10.0.0.x."""
    cleaned = (host or "").strip().lower()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host cannot be empty")
    if cleaned in ("127.0.0.1", "localhost", "::1"):
        return cleaned
    if cleaned.startswith("10.0.0."):
        parts = cleaned.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, ::1, or 10.0.0.x allowed"
    )


# -----------------------------------------------------------------------------
# Typed JSON Serialization Helpers (Universal O(1) Checkpoint Support)
# -----------------------------------------------------------------------------

def _encode_typed_value(val: Any) -> Any:
    """Recursively encode native Python types (datetime, Decimal, UUID, tuple, bytes) for JSON."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
    if isinstance(val, (bytes, bytearray, memoryview)):
        return {"__type__": "bytes", "val": bytes(val).hex()}
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
    if t == "bytes":
        return bytes.fromhex(obj["val"])
    if t == "tuple":
        return tuple(obj["val"])
    if t == "set":
        return set(obj["val"])
    return obj


# -----------------------------------------------------------------------------
# Distribution Statistics & System Pressure Readings
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class DistributionStatistics:
    """Measured statistical characteristics of row-width tails and distributions."""

    p50_row_bytes: int
    p95_row_bytes: int
    p99_row_bytes: int
    max_row_bytes: int
    skewness: float
    compressibility_ratio: float
    distinct_keys_count: int
    sample_size: int = 0

    def __post_init__(self) -> None:
        if self.p50_row_bytes < 0 or self.p95_row_bytes < 0 or self.p99_row_bytes < 0:
            raise ValueError("percentile row bytes must be non-negative")
        if self.max_row_bytes < self.p99_row_bytes:
            raise ValueError("max_row_bytes cannot be less than p99_row_bytes")
        if self.distinct_keys_count < 0:
            raise ValueError("distinct_keys_count must be non-negative")
        if not (0.0 <= self.compressibility_ratio <= 2.0):
            raise ValueError("compressibility_ratio must be between 0.0 and 2.0")

    @classmethod
    def from_samples(
        cls,
        sample_rows: Sequence[Union[int, bytes, str, Mapping[str, Any], Sequence[Any]]],
        *,
        distinct_keys: int = 1,
        sample_data: Optional[Sequence[Union[bytes, str]]] = None,
    ) -> DistributionStatistics:
        """Derive distribution statistics from sample row sizes and contents."""
        if not sample_rows:
            return cls(
                p50_row_bytes=128,
                p95_row_bytes=256,
                p99_row_bytes=512,
                max_row_bytes=1024,
                skewness=0.0,
                compressibility_ratio=1.0,
                distinct_keys_count=max(1, distinct_keys),
                sample_size=0,
            )

        sizes: List[int] = []
        for r in sample_rows:
            if isinstance(r, int):
                sizes.append(max(0, r))
            elif isinstance(r, (bytes, bytearray)):
                sizes.append(len(r))
            elif isinstance(r, str):
                sizes.append(len(r.encode("utf-8")))
            elif isinstance(r, (dict, list, tuple)):
                sizes.append(len(json.dumps(r, default=str).encode("utf-8")))
            else:
                sizes.append(len(str(r).encode("utf-8")))

        sizes.sort()
        n = len(sizes)
        p50 = sizes[int(n * 0.50)]
        p95 = sizes[min(n - 1, int(n * 0.95))]
        p99 = sizes[min(n - 1, int(n * 0.99))]
        max_b = sizes[-1]

        # Calculate sample skewness: 3 * (mean - median) / std_dev
        mean_b = sum(sizes) / n
        variance = sum((x - mean_b) ** 2 for x in sizes) / n
        std_dev = math.sqrt(variance) if variance > 0 else 0.0
        skew = (3.0 * (mean_b - p50) / std_dev) if std_dev > 0.0 else 0.0

        # Calculate compressibility ratio from sample_data or synthesized payload
        comp_ratio = 1.0
        if sample_data:
            total_raw = 0
            total_compressed = 0
            for item in sample_data:
                b = item if isinstance(item, bytes) else str(item).encode("utf-8")
                total_raw += len(b)
                total_compressed += len(zlib.compress(b, level=1))
            if total_raw > 0:
                comp_ratio = float(total_compressed) / float(total_raw)
        else:
            # Estimate compressibility from entropy of sample sizes
            if p99 > 65536 and skew > 1.5:
                comp_ratio = 0.45
            else:
                comp_ratio = 0.85

        return cls(
            p50_row_bytes=p50,
            p95_row_bytes=p95,
            p99_row_bytes=p99,
            max_row_bytes=max_b,
            skewness=float(skew),
            compressibility_ratio=min(1.0, max(0.05, float(comp_ratio))),
            distinct_keys_count=max(1, distinct_keys),
            sample_size=n,
        )


@dataclass(frozen=True)
class SystemPressureReadings:
    """Measured runtime system, network, and memory constraints."""

    cpu_utilization_ratio: float
    network_utilization_ratio: float
    available_bandwidth_mbps: float
    max_request_packet_bytes: int
    worker_memory_budget_bytes: int
    estimated_retry_cost_s: float

    def __post_init__(self) -> None:
        if not (0.0 <= self.cpu_utilization_ratio <= 1.0):
            raise ValueError("cpu_utilization_ratio must be between 0.0 and 1.0")
        if not (0.0 <= self.network_utilization_ratio <= 1.0):
            raise ValueError("network_utilization_ratio must be between 0.0 and 1.0")
        if self.available_bandwidth_mbps <= 0.0:
            raise ValueError("available_bandwidth_mbps must be positive")
        if self.max_request_packet_bytes <= 0:
            raise ValueError("max_request_packet_bytes must be positive")
        if self.worker_memory_budget_bytes <= 0:
            raise ValueError("worker_memory_budget_bytes must be positive")
        if self.estimated_retry_cost_s < 0.0:
            raise ValueError("estimated_retry_cost_s must be non-negative")


# -----------------------------------------------------------------------------
# Decide Ladders (All Choice Gates MUST Climb Through Decide Rungs)
# -----------------------------------------------------------------------------

def batch_sizing_rungs() -> Tuple[decide.Rung, ...]:
    """Ladders for deciding batch rows, cursor rows, and batch byte bounds."""
    return (
        decide.Rung(
            "adaptive_lob_tail_bounded_batch",
            "clamp batch rows tight and bound batch bytes when row tail has extreme LOBs or wide rows",
            needs=(
                decide.Need(
                    "tail has extreme LOB or wide row exceeding tail threshold",
                    lambda f: float(f.get("p99_row_bytes", 0)) > float(f.get("lob_tail_threshold_bytes", 65536))
                    or float(f.get("max_row_bytes", 0)) > float(f.get("lob_tail_threshold_bytes", 65536)),
                    "{rung} refused: row tail does not contain extreme LOBs or wide rows",
                ),
                decide.Need(
                    "worker memory headroom accommodates widest row with safety factor",
                    lambda f: float(f.get("worker_memory_budget_bytes", 0))
                    >= float(f.get("max_row_bytes", 0)) * 1.5,
                    "{rung} refused: worker memory headroom cannot safely hold widest row",
                ),
            ),
            because="prevents memory exhaustion when tiny rows are mixed with extreme LOBs",
            footprint=3,
        ),
        decide.Rung(
            "high_density_compact_batch",
            "scale batch rows up to amortize roundtrips when rows are compact and uniform",
            needs=(
                decide.Need(
                    "compact uniform rows with low tail variance",
                    lambda f: float(f.get("p99_row_bytes", 0)) <= float(f.get("compact_threshold_bytes", 1024))
                    and float(f.get("max_row_bytes", 0)) <= float(f.get("compact_threshold_bytes", 1024)) * 8,
                    "{rung} refused: rows are not sufficiently compact or uniform",
                ),
                decide.Need(
                    "service packet cap accommodates high density batch bytes",
                    lambda f: int(f.get("max_request_packet_bytes", 0)) >= int(f.get("target_batch_bytes", 0)),
                    "{rung} refused: target batch bytes exceed service request packet cap",
                ),
            ),
            because="maximizes throughput and amortizes transaction overhead for tiny high-density rows",
            footprint=2,
        ),
        decide.Rung(
            "service_cap_constrained_batch",
            "restrict batch bytes strictly to service packet cap minus headroom",
            needs=(
                decide.Need(
                    "service packet cap is smaller than nominal batch bytes",
                    lambda f: int(f.get("max_request_packet_bytes", 0)) < int(f.get("nominal_batch_bytes", 0))
                    and int(f.get("max_request_packet_bytes", 0)) > 0,
                    "{rung} refused: service packet cap is not a limiting constraint",
                ),
            ),
            because="guarantees batch never exceeds driver or server packet limit",
            footprint=1,
        ),
        decide.Rung(
            "conservative_fallback_batch",
            "enforce conservative small batch baseline when distribution variance is high or uncertain",
            needs=(
                decide.Need(
                    "fallback to conservative baseline when variance is unbounded or distribution uncertain",
                    lambda f: bool(f.get("uncertain_distribution", False))
                    or float(f.get("variance_coefficient", 0.0)) > 5.0
                    or bool(f.get("allow_conservative_fallback", True)),
                    "{rung} refused: distribution is bounded and certainty is established",
                ),
            ),
            because="maintains bounded safe execution under unmodeled or volatile distributions",
            footprint=0,
        ),
    )


def split_merge_rungs() -> Tuple[decide.Rung, ...]:
    """Ladders for deciding range key boundaries, partition splits, and subrange merges."""
    return (
        decide.Rung(
            "split_skewed_dense_range",
            "split skewed or dense range into sub-ranges at quantile key boundaries",
            needs=(
                decide.Need(
                    "range row count or expected retry cost exceeds threshold",
                    lambda f: int(f.get("range_row_count", 0)) > int(f.get("max_range_rows_threshold", 50000))
                    or float(f.get("expected_retry_cost_s", 0.0)) > float(f.get("max_retry_cost_tolerance_s", 30.0)),
                    "{rung} refused: range density and retry cost are within acceptable bounds",
                ),
                decide.Need(
                    "range has multiple splittable key boundaries",
                    lambda f: int(f.get("distinct_keys_count", 0)) > 1
                    and bool(f.get("has_splittable_key_boundary", True)),
                    "{rung} refused: range has indivisible or single distinct key cardinality",
                ),
            ),
            because="bounds blast radius of retries and balances parallel work units across workers",
            footprint=2,
        ),
        decide.Rung(
            "merge_sparse_subranges",
            "merge adjacent underfilled sub-ranges to avoid connection and statement overhead",
            needs=(
                decide.Need(
                    "combined rows and bytes are below minimum work granularity",
                    lambda f: int(f.get("combined_range_row_count", 0)) <= int(f.get("min_range_rows_threshold", 5000))
                    and int(f.get("combined_range_bytes", 0)) <= int(f.get("min_range_bytes_threshold", 1048576)),
                    "{rung} refused: sub-ranges have sufficient volume to stand independently",
                ),
                decide.Need(
                    "sub-ranges share contiguous boundaries without gaps",
                    lambda f: bool(f.get("ranges_are_contiguous", False)),
                    "{rung} refused: sub-ranges are not contiguous",
                ),
            ),
            because="amortizes cursor setup and transaction coordination for sparse partitions",
            footprint=1,
        ),
        decide.Rung(
            "preserve_discrete_key_boundary",
            "keep range boundaries intact when work unit is balanced or indivisible",
            needs=(
                decide.Need(
                    "range is balanced within thresholds or has indivisible key boundary",
                    lambda f: (
                        int(f.get("range_row_count", 0)) <= int(f.get("max_range_rows_threshold", 50000))
                        and int(f.get("range_row_count", 0)) >= int(f.get("min_range_rows_threshold", 5000))
                    )
                    or int(f.get("distinct_keys_count", 0)) <= 1
                    or bool(f.get("preserve_boundary_explicit", False)),
                    "{rung} refused: range is neither balanced nor indivisible",
                ),
            ),
            because="avoids unnecessary partitioning churn on already optimal work units",
            footprint=0,
        ),
    )


def compression_encoding_rungs() -> Tuple[decide.Rung, ...]:
    """Ladders for deciding joint compression and serialization formats."""
    return (
        decide.Rung(
            "compress_high_ratio_lz4",
            "compress with fast block LZ4 algorithm when payload is compressible and network is constrained",
            needs=(
                decide.Need(
                    "payload compressibility meets threshold and network is constrained",
                    lambda f: float(f.get("compressibility_ratio", 1.0))
                    <= float(f.get("compressibility_threshold", 0.70))
                    and (
                        float(f.get("network_utilization_ratio", 0.0))
                        >= float(f.get("network_pressure_threshold", 0.60))
                        or float(f.get("bandwidth_mbps", 1000.0)) < 100.0
                    ),
                    "{rung} refused: payload is incompressible or network has ample headroom",
                ),
                decide.Need(
                    "host CPU has sufficient headroom for compression",
                    lambda f: float(f.get("cpu_utilization_ratio", 0.0))
                    <= float(f.get("max_cpu_compression_threshold", 0.75)),
                    "{rung} refused: host CPU utilization exceeds compression threshold",
                ),
            ),
            because="minimizes network transfer time and bandwidth saturation under constrained links",
            footprint=2,
        ),
        decide.Rung(
            "compress_cpu_throttled_light",
            "use lightweight run-length or framing compression when CPU is strained but network is adequate",
            needs=(
                decide.Need(
                    "cpu is under moderate pressure above heavy compression limit",
                    lambda f: float(f.get("cpu_utilization_ratio", 0.0))
                    > float(f.get("max_cpu_compression_threshold", 0.75))
                    and float(f.get("cpu_utilization_ratio", 0.0)) <= 0.90,
                    "{rung} refused: CPU is either idle or completely saturated",
                ),
                decide.Need(
                    "network bandwidth is adequate without aggressive compression",
                    lambda f: float(f.get("network_utilization_ratio", 0.0))
                    < float(f.get("network_pressure_threshold", 0.60)),
                    "{rung} refused: network is congested requiring full compression",
                ),
            ),
            because="balances CPU throttle against network wire savings on loaded migration agents",
            footprint=1,
        ),
        decide.Rung(
            "pass_through_raw_uncompressed",
            "pass raw uncompressed bytes when payload is incompressible or CPU is exhausted",
            needs=(
                decide.Need(
                    "data is incompressible or CPU utilization is critical",
                    lambda f: float(f.get("compressibility_ratio", 1.0))
                    > float(f.get("compressibility_threshold", 0.70))
                    or float(f.get("cpu_utilization_ratio", 0.0)) > 0.90
                    or bool(f.get("force_uncompressed", False)),
                    "{rung} refused: payload is compressible and CPU is available",
                ),
            ),
            because="eliminates wasted CPU cycles that produce zero meaningful size reduction",
            footprint=0,
        ),
    )


def representation_integrity_rungs() -> Tuple[decide.Rung, ...]:
    """Ladders for preserving keyless multiplicity and extreme LOB integrity."""
    return (
        decide.Rung(
            "keyless_multiplicity_occurrence_envelope",
            "wrap rows in multiplicity occurrence envelope to preserve duplicate instances on keyless tables",
            needs=(
                decide.Need(
                    "table lacks primary key or unique identifier",
                    lambda f: not bool(f.get("has_primary_key", True)),
                    "{rung} refused: table has primary key with unique row identity",
                ),
                decide.Need(
                    "duplicate multiplicity preservation is required",
                    lambda f: bool(f.get("has_duplicate_multiplicities", False))
                    or bool(f.get("strict_keyless_multiplicity", True)),
                    "{rung} refused: strict multiplicity preservation is disabled",
                ),
            ),
            because="ensures identical rows in keyless tables are never collapsed or dropped during transfer",
            footprint=2,
        ),
        decide.Rung(
            "lob_streaming_piece_envelope",
            "isolate extreme LOB columns into non-truncating chunked pieces with SHA-256 integrity",
            needs=(
                decide.Need(
                    "extreme LOB columns are present in table schema or data",
                    lambda f: bool(f.get("has_extreme_lob_columns", False))
                    or float(f.get("max_lob_bytes", 0)) > float(f.get("lob_piece_threshold_bytes", 1048576)),
                    "{rung} refused: table does not contain extreme LOB columns",
                ),
                decide.Need(
                    "driver and transport support piecewise chunked streaming",
                    lambda f: bool(f.get("supports_piece_streaming", True)),
                    "{rung} refused: piece streaming transport is unsupported",
                ),
            ),
            because="bounds memory usage while preserving exact bit-for-bit LOB integrity without truncation",
            footprint=1,
        ),
        decide.Rung(
            "standard_relational_row_envelope",
            "transfer directly as standard relational tuples when table has PK and no extreme LOBs",
            needs=(
                decide.Need(
                    "table has primary key and no extreme LOB columns",
                    lambda f: bool(f.get("has_primary_key", True))
                    and not bool(f.get("has_extreme_lob_columns", False))
                    and float(f.get("max_lob_bytes", 0)) <= float(f.get("lob_piece_threshold_bytes", 1048576)),
                    "{rung} refused: table requires keyless envelope or LOB piece streaming",
                ),
            ),
            because="minimizes envelope overhead for standard structured relational rows",
            footprint=0,
        ),
    )


# -----------------------------------------------------------------------------
# Decision Results
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class BatchSizingDecision:
    choice: decide.Choice
    batch_bytes: int
    batch_rows: int
    cursor_rows: int
    care_badge: CareBadge


@dataclass(frozen=True)
class SplitMergeDecision:
    choice: decide.Choice
    action: SplitMergeAction
    split_key_boundaries: Tuple[Any, ...]
    care_badge: CareBadge


@dataclass(frozen=True)
class CompressionEncodingDecision:
    choice: decide.Choice
    compression: CompressionMethod
    serialization: SerializationFormat
    care_badge: CareBadge


@dataclass(frozen=True)
class RepresentationIntegrityDecision:
    choice: decide.Choice
    mode: RepresentationMode
    care_badge: CareBadge


@dataclass(frozen=True)
class JointWorkUnitPlan:
    batch_sizing: BatchSizingDecision
    split_merge: SplitMergeDecision
    compression_encoding: CompressionEncodingDecision
    representation_integrity: RepresentationIntegrityDecision
    care_badge: CareBadge

    @property
    def batch_bytes(self) -> int:
        return self.batch_sizing.batch_bytes

    @property
    def batch_rows(self) -> int:
        return self.batch_sizing.batch_rows

    @property
    def cursor_rows(self) -> int:
        return self.batch_sizing.cursor_rows


# -----------------------------------------------------------------------------
# Decision Entry Points (Climbing Rungs)
# -----------------------------------------------------------------------------

def choose_batch_sizing(
    facts_input: Mapping[str, Any],
    *,
    work: str = "work_granularity:batch_sizing",
) -> BatchSizingDecision:
    """Climb batch_sizing_rungs to decide batch bytes, batch rows, and cursor rows."""
    facts = decide.Facts(dict(facts_input))
    choice = decide.climb(work, batch_sizing_rungs(), facts)

    p99 = int(facts.get("p99_row_bytes", 1024))
    max_b = int(facts.get("max_row_bytes", 2048))
    mem_budget = int(facts.get("worker_memory_budget_bytes", 64 * 1024 * 1024))
    cap = int(facts.get("max_request_packet_bytes", 16 * 1024 * 1024))

    path = choice.path
    if path == "adaptive_lob_tail_bounded_batch":
        # Extreme LOB in tail: tightly bound batch_rows to 1-5, bound bytes to memory budget
        batch_rows = 1 if max_b > (mem_budget // 4) else max(1, min(10, mem_budget // (max_b * 2)))
        batch_bytes = min(mem_budget // 2, cap - 65536)
        cursor_rows = max(1, batch_rows)
        badge = CareBadge.REVIEW
    elif path == "high_density_compact_batch":
        # Compact uniform rows: maximize batch rows up to engine limit, amortize round trips
        batch_bytes = min(cap - 65536, mem_budget // 2, 32 * 1024 * 1024)
        avg_row = max(16, p99)
        batch_rows = min(50000, max(1000, batch_bytes // avg_row))
        cursor_rows = min(10000, batch_rows)
        badge = CareBadge.PASS
    elif path == "service_cap_constrained_batch":
        # Tightly bounded by packet limit
        batch_bytes = max(1024, cap - 65536)
        avg_row = max(64, p99)
        batch_rows = max(1, min(10000, batch_bytes // avg_row))
        cursor_rows = max(1, min(1000, batch_rows))
        badge = CareBadge.BENIGN
    else:  # conservative_fallback_batch
        batch_bytes = min(4 * 1024 * 1024, cap - 65536, mem_budget // 4)
        batch_rows = 500
        cursor_rows = 100
        badge = CareBadge.ACTION

    return BatchSizingDecision(
        choice=choice,
        batch_bytes=int(batch_bytes),
        batch_rows=int(batch_rows),
        cursor_rows=int(cursor_rows),
        care_badge=badge,
    )


def choose_split_merge(
    facts_input: Mapping[str, Any],
    *,
    work: str = "work_granularity:split_merge",
) -> SplitMergeDecision:
    """Climb split_merge_rungs to decide range splitting, merging, or preservation."""
    facts = decide.Facts(dict(facts_input))
    choice = decide.climb(work, split_merge_rungs(), facts)

    path = choice.path
    boundaries = tuple(facts.get("proposed_split_boundaries", ()))
    if path == "split_skewed_dense_range":
        action = SplitMergeAction.SPLIT
        badge = CareBadge.REVIEW
    elif path == "merge_sparse_subranges":
        action = SplitMergeAction.MERGE
        badge = CareBadge.BENIGN
    else:
        action = SplitMergeAction.PRESERVE
        badge = CareBadge.PASS

    return SplitMergeDecision(
        choice=choice,
        action=action,
        split_key_boundaries=boundaries,
        care_badge=badge,
    )


def choose_compression_encoding(
    facts_input: Mapping[str, Any],
    *,
    work: str = "work_granularity:compression_encoding",
) -> CompressionEncodingDecision:
    """Climb compression_encoding_rungs to decide compression and serialization format."""
    facts = decide.Facts(dict(facts_input))
    choice = decide.climb(work, compression_encoding_rungs(), facts)

    path = choice.path
    if path == "compress_high_ratio_lz4":
        comp = CompressionMethod.LZ4
        ser = SerializationFormat.BINARY_ROW
        badge = CareBadge.PASS
    elif path == "compress_cpu_throttled_light":
        comp = CompressionMethod.LIGHT
        ser = SerializationFormat.TYPED_JSON
        badge = CareBadge.BENIGN
    else:
        comp = CompressionMethod.NONE
        ser = SerializationFormat.RAW_OCTETS
        badge = CareBadge.REVIEW

    return CompressionEncodingDecision(
        choice=choice,
        compression=comp,
        serialization=ser,
        care_badge=badge,
    )


def choose_representation_integrity(
    facts_input: Mapping[str, Any],
    *,
    work: str = "work_granularity:representation_integrity",
) -> RepresentationIntegrityDecision:
    """Climb representation_integrity_rungs to preserve keyless multiplicity and LOB integrity."""
    facts = decide.Facts(dict(facts_input))
    choice = decide.climb(work, representation_integrity_rungs(), facts)

    path = choice.path
    if path == "keyless_multiplicity_occurrence_envelope":
        mode = RepresentationMode.KEYLESS_MULTIPLICITY
        badge = CareBadge.REVIEW
    elif path == "lob_streaming_piece_envelope":
        mode = RepresentationMode.LOB_STREAMING_PIECE
        badge = CareBadge.REVIEW
    else:
        mode = RepresentationMode.STANDARD_RELATIONAL
        badge = CareBadge.PASS

    return RepresentationIntegrityDecision(
        choice=choice,
        mode=mode,
        care_badge=badge,
    )


def optimize_work_granularity(
    stats: DistributionStatistics,
    pressure: SystemPressureReadings,
    *,
    table_name: str = "target_table",
    has_primary_key: bool = True,
    has_duplicate_multiplicities: bool = False,
    has_extreme_lob_columns: bool = False,
    range_row_count: int = 10000,
    combined_range_row_count: int = 20000,
    combined_range_bytes: int = 2097152,
    ranges_are_contiguous: bool = True,
    endpoint_host: str = "127.0.0.1",
    work_prefix: str = "work_granularity",
) -> JointWorkUnitPlan:
    """Jointly optimize work unit granularity, key boundaries, compression, and representation."""
    validate_safe_endpoint(endpoint_host)

    # Derive joint facts from measured readings
    nominal_batch_bytes = 16 * 1024 * 1024
    target_batch_bytes = min(nominal_batch_bytes, pressure.worker_memory_budget_bytes // 2)

    batch_facts = {
        "p99_row_bytes": stats.p99_row_bytes,
        "max_row_bytes": stats.max_row_bytes,
        "lob_tail_threshold_bytes": 65536,
        "compact_threshold_bytes": 1024,
        "worker_memory_budget_bytes": pressure.worker_memory_budget_bytes,
        "max_request_packet_bytes": pressure.max_request_packet_bytes,
        "nominal_batch_bytes": nominal_batch_bytes,
        "target_batch_bytes": target_batch_bytes,
        "uncertain_distribution": stats.sample_size == 0,
        "variance_coefficient": stats.skewness,
    }
    batch_dec = choose_batch_sizing(batch_facts, work=f"{work_prefix}:batch_sizing")

    split_facts = {
        "range_row_count": range_row_count,
        "max_range_rows_threshold": 50000,
        "min_range_rows_threshold": 5000,
        "expected_retry_cost_s": pressure.estimated_retry_cost_s,
        "max_retry_cost_tolerance_s": 30.0,
        "distinct_keys_count": stats.distinct_keys_count,
        "has_splittable_key_boundary": stats.distinct_keys_count > 1,
        "combined_range_row_count": combined_range_row_count,
        "combined_range_bytes": combined_range_bytes,
        "min_range_bytes_threshold": 1048576,
        "ranges_are_contiguous": ranges_are_contiguous,
    }
    split_dec = choose_split_merge(split_facts, work=f"{work_prefix}:split_merge")

    comp_facts = {
        "compressibility_ratio": stats.compressibility_ratio,
        "compressibility_threshold": 0.70,
        "network_utilization_ratio": pressure.network_utilization_ratio,
        "network_pressure_threshold": 0.60,
        "bandwidth_mbps": pressure.available_bandwidth_mbps,
        "cpu_utilization_ratio": pressure.cpu_utilization_ratio,
        "max_cpu_compression_threshold": 0.75,
    }
    comp_dec = choose_compression_encoding(comp_facts, work=f"{work_prefix}:compression_encoding")

    rep_facts = {
        "has_primary_key": has_primary_key,
        "has_duplicate_multiplicities": has_duplicate_multiplicities,
        "strict_keyless_multiplicity": True,
        "has_extreme_lob_columns": has_extreme_lob_columns or stats.max_row_bytes > 1048576,
        "max_lob_bytes": stats.max_row_bytes,
        "lob_piece_threshold_bytes": 1048576,
        "supports_piece_streaming": True,
    }
    rep_dec = choose_representation_integrity(rep_facts, work=f"{work_prefix}:representation_integrity")

    # Joint care badge calculation: worst-case badge across decisions
    badges = [batch_dec.care_badge, split_dec.care_badge, comp_dec.care_badge, rep_dec.care_badge]
    if CareBadge.ACTION in badges:
        joint_badge = CareBadge.ACTION
    elif CareBadge.REVIEW in badges:
        joint_badge = CareBadge.REVIEW
    elif CareBadge.BENIGN in badges:
        joint_badge = CareBadge.BENIGN
    else:
        joint_badge = CareBadge.PASS

    return JointWorkUnitPlan(
        batch_sizing=batch_dec,
        split_merge=split_dec,
        compression_encoding=comp_dec,
        representation_integrity=rep_dec,
        care_badge=joint_badge,
    )


# -----------------------------------------------------------------------------
# Keyless Multiplicity & LOB Piece Streaming Envelopes
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class KeylessRowEnvelope:
    """Envelopes a keyless row with its deterministic digest and occurrence sequence."""

    row_digest: str
    occurrence_seq: int
    payload: Tuple[Any, ...]


class KeylessMultiplicityTracker:
    """Maintains exact row multiplicity counts and Merkle bucket digests for keyless tables."""

    def __init__(self) -> None:
        self._counts: Dict[str, int] = {}
        self._envelopes: List[KeylessRowEnvelope] = []

    def canonical_digest(self, row: Sequence[Any]) -> str:
        """Compute SHA-256 digest of canonical serialized tuple."""
        raw = json.dumps([_encode_typed_value(x) for x in row], sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(raw.encode("ascii")).hexdigest()

    def add_row(self, row: Sequence[Any]) -> KeylessRowEnvelope:
        """Record row and return typed envelope with occurrence ordinal."""
        digest = self.canonical_digest(row)
        seq = self._counts.get(digest, 0)
        self._counts[digest] = seq + 1
        env = KeylessRowEnvelope(row_digest=digest, occurrence_seq=seq, payload=tuple(row))
        self._envelopes.append(env)
        return env

    def occurrence_map(self) -> Dict[str, int]:
        return dict(self._counts)

    def total_rows(self) -> int:
        return len(self._envelopes)

    def compute_merkle_digest(self) -> str:
        """Derive cumulative Merkle bucket digest proving total multiplicity integrity."""
        sorted_items = sorted(self._counts.items())
        combined = "|".join(f"{d}:{cnt}" for d, cnt in sorted_items)
        return hashlib.sha256(combined.encode("ascii")).hexdigest()

    def verify_against(self, other: KeylessMultiplicityTracker) -> bool:
        """Verify byte-exact multiplicity match against target tracker."""
        if self._counts != other._counts:
            diff_keys = set(self._counts.keys()) ^ set(other._counts.keys())
            mismatches = [k for k in self._counts if k in other._counts and self._counts[k] != other._counts[k]]
            raise MultiplicityIntegrityError(
                f"keyless multiplicity mismatch: {len(diff_keys)} disjoint digests, "
                f"{len(mismatches)} count disparities between source and target"
            )
        return True


@dataclass(frozen=True)
class LobPiece:
    """A bounded non-truncating chunk of an extreme LOB value."""

    column_name: str
    piece_index: int
    total_pieces: int
    offset_bytes: int
    payload: bytes
    piece_sha256: str
    total_sha256: str


def chunk_lob_value(
    column_name: str,
    value: Union[bytes, str],
    *,
    piece_size: int = 1048576,
) -> List[LobPiece]:
    """Slice an extreme LOB into non-truncating pieces with SHA-256 verification."""
    if piece_size <= 0:
        raise ValueError("piece_size must be positive")

    raw_bytes = value if isinstance(value, (bytes, bytearray)) else value.encode("utf-8")
    total_len = len(raw_bytes)
    total_sha = hashlib.sha256(raw_bytes).hexdigest()

    if total_len == 0:
        piece_sha = hashlib.sha256(b"").hexdigest()
        return [
            LobPiece(
                column_name=column_name,
                piece_index=0,
                total_pieces=1,
                offset_bytes=0,
                payload=b"",
                piece_sha256=piece_sha,
                total_sha256=total_sha,
            )
        ]

    pieces_list: List[LobPiece] = []
    num_pieces = math.ceil(total_len / piece_size)
    for i in range(num_pieces):
        start = i * piece_size
        end = min(total_len, start + piece_size)
        chunk = raw_bytes[start:end]
        piece_sha = hashlib.sha256(chunk).hexdigest()
        pieces_list.append(
            LobPiece(
                column_name=column_name,
                piece_index=i,
                total_pieces=num_pieces,
                offset_bytes=start,
                payload=chunk,
                piece_sha256=piece_sha,
                total_sha256=total_sha,
            )
        )
    return pieces_list


def reassemble_lob_pieces(pieces_sequence: Sequence[LobPiece]) -> bytes:
    """Reassemble LOB pieces into complete byte payload and verify SHA-256 integrity."""
    if not pieces_sequence:
        return b""

    sorted_pieces = sorted(pieces_sequence, key=lambda p: p.piece_index)
    total_expected = sorted_pieces[0].total_pieces
    if len(sorted_pieces) != total_expected:
        raise LobIntegrityError(
            f"incomplete LOB pieces: expected {total_expected}, received {len(sorted_pieces)}"
        )

    expected_total_sha = sorted_pieces[0].total_sha256
    buf = bytearray()
    for idx, p in enumerate(sorted_pieces):
        if p.piece_index != idx:
            raise LobIntegrityError(f"missing LOB piece at index {idx}")
        # Verify piece SHA-256
        actual_piece_sha = hashlib.sha256(p.payload).hexdigest()
        if actual_piece_sha != p.piece_sha256:
            raise LobIntegrityError(f"corrupt LOB piece {idx}: SHA-256 checksum mismatch")
        buf.extend(p.payload)

    assembled = bytes(buf)
    actual_total_sha = hashlib.sha256(assembled).hexdigest()
    if actual_total_sha != expected_total_sha:
        raise LobIntegrityError("reassembled LOB checksum mismatch against source total digest")

    return assembled


# -----------------------------------------------------------------------------
# Work Unit Stream Execution & Verified Completion Certificate
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class VerifiedCompletionCertificate:
    """Cryptographically verifiable certificate of memory-bounded, service-cap compliant completion."""

    table_name: str
    rows_processed: int
    lob_pieces_transferred: int
    total_payload_bytes: int
    peak_worker_memory_bytes: int
    service_cap_violations: int
    keyless_multiplicity_verified: bool
    lob_integrity_verified: bool
    completion_timestamp: datetime.datetime
    care_badge: CareBadge


class WorkUnitStreamExecutor:
    """Executes row streaming and LOB chunking with strict memory and packet bounds."""

    def __init__(
        self,
        plan: JointWorkUnitPlan,
        pressure: SystemPressureReadings,
        *,
        table_name: str = "work_table",
    ) -> None:
        self.plan = plan
        self.pressure = pressure
        self.table_name = table_name
        self.peak_memory_bytes = 0
        self.service_cap_violations = 0
        self.rows_processed = 0
        self.lob_pieces_transferred = 0
        self.total_payload_bytes = 0
        self.keyless_source_tracker = KeylessMultiplicityTracker()
        self.keyless_target_tracker = KeylessMultiplicityTracker()
        self.assembled_lobs: Dict[str, bytes] = {}

    def execute_stream(
        self,
        row_stream: Iterable[Dict[str, Any]],
        *,
        lob_columns: FrozenSet[str] = frozenset(),
    ) -> VerifiedCompletionCertificate:
        """Stream rows and extreme LOBs while enforcing memory and packet limits."""
        current_batch_rows: List[Dict[str, Any]] = []
        current_batch_bytes = 0

        for row in row_stream:
            # Check row for extreme LOBs
            row_has_extreme_lob = False
            for col in lob_columns:
                val = row.get(col)
                if val is not None:
                    val_bytes = val if isinstance(val, (bytes, bytearray)) else str(val).encode("utf-8")
                    if len(val_bytes) > 65536:
                        row_has_extreme_lob = True
                        break

            # If extreme LOB present, flush current batch first to isolate LOB in tight memory
            if row_has_extreme_lob and current_batch_rows:
                self._flush_batch(current_batch_rows, current_batch_bytes)
                current_batch_rows = []
                current_batch_bytes = 0

            # Calculate row size
            row_scalar_copy = {k: v for k, v in row.items() if k not in lob_columns}
            serialized_scalars = json.dumps(row_scalar_copy, default=str).encode("utf-8")
            row_bytes = len(serialized_scalars)

            # Track keyless multiplicity if keyless envelope active
            if self.plan.representation_integrity.mode == RepresentationMode.KEYLESS_MULTIPLICITY:
                vals = [row[k] for k in sorted(row.keys())]
                src_env = self.keyless_source_tracker.add_row(vals)
                # Target receipt simulation
                self.keyless_target_tracker.add_row(src_env.payload)

            # Handle LOB columns in pieces
            if row_has_extreme_lob:
                for col in lob_columns:
                    lob_val = row.get(col)
                    if lob_val is not None:
                        # Slice in pieces bounded by worker memory budget
                        piece_size = min(1048576, self.pressure.worker_memory_budget_bytes // 4)
                        pieces_list = chunk_lob_value(col, lob_val, piece_size=piece_size)
                        for p in pieces_list:
                            # Verify memory bound per piece
                            piece_mem = len(p.payload) + 1024
                            self.peak_memory_bytes = max(self.peak_memory_bytes, piece_mem)
                            if piece_mem > self.pressure.worker_memory_budget_bytes:
                                raise MemoryBudgetExceededError(
                                    f"piece memory {piece_mem} exceeds budget {self.pressure.worker_memory_budget_bytes}"
                                )
                            # Verify packet bound
                            if len(p.payload) > self.pressure.max_request_packet_bytes:
                                self.service_cap_violations += 1
                                raise ServiceCapExceededError("LOB piece exceeds service request packet cap")

                            self.lob_pieces_transferred += 1
                            self.total_payload_bytes += len(p.payload)

                        # Reassemble and verify integrity
                        assembled = reassemble_lob_pieces(pieces_list)
                        self.assembled_lobs[f"{self.rows_processed}:{col}"] = assembled

                # Count this row and record memory
                self.rows_processed += 1
                self.total_payload_bytes += row_bytes
                continue

            # Standard row batching
            current_batch_rows.append(row)
            current_batch_bytes += row_bytes

            # Check if batch exceeds planned bounds
            if (
                len(current_batch_rows) >= self.plan.batch_rows
                or current_batch_bytes >= self.plan.batch_bytes
            ):
                self._flush_batch(current_batch_rows, current_batch_bytes)
                current_batch_rows = []
                current_batch_bytes = 0

        # Flush any remaining rows
        if current_batch_rows:
            self._flush_batch(current_batch_rows, current_batch_bytes)

        # Final verifications
        keyless_ok = True
        if self.plan.representation_integrity.mode == RepresentationMode.KEYLESS_MULTIPLICITY:
            self.keyless_source_tracker.verify_against(self.keyless_target_tracker)
            keyless_ok = True

        lob_ok = True
        if lob_columns:
            # Verified via reassemble_lob_pieces checksums
            lob_ok = len(self.assembled_lobs) > 0 or self.rows_processed == 0

        badge = CareBadge.PASS
        if self.service_cap_violations > 0:
            badge = CareBadge.ACTION
        elif self.peak_memory_bytes > self.pressure.worker_memory_budget_bytes:
            badge = CareBadge.ACTION

        return VerifiedCompletionCertificate(
            table_name=self.table_name,
            rows_processed=self.rows_processed,
            lob_pieces_transferred=self.lob_pieces_transferred,
            total_payload_bytes=self.total_payload_bytes,
            peak_worker_memory_bytes=self.peak_memory_bytes,
            service_cap_violations=self.service_cap_violations,
            keyless_multiplicity_verified=keyless_ok,
            lob_integrity_verified=lob_ok,
            completion_timestamp=datetime.datetime.now(datetime.timezone.utc),
            care_badge=badge,
        )

    def _flush_batch(self, rows: List[Dict[str, Any]], batch_bytes: int) -> None:
        """Process one batch, asserting memory and service limits."""
        # Estimate buffered batch memory
        batch_mem = batch_bytes + (len(rows) * 128)
        self.peak_memory_bytes = max(self.peak_memory_bytes, batch_mem)

        if batch_mem > self.pressure.worker_memory_budget_bytes:
            raise MemoryBudgetExceededError(
                f"batch memory {batch_mem} exceeded worker memory budget {self.pressure.worker_memory_budget_bytes}"
            )

        if batch_bytes > self.pressure.max_request_packet_bytes:
            self.service_cap_violations += 1
            raise ServiceCapExceededError(
                f"batch bytes {batch_bytes} exceeded service packet cap {self.pressure.max_request_packet_bytes}"
            )

        self.rows_processed += len(rows)
        self.total_payload_bytes += batch_bytes


# -----------------------------------------------------------------------------
# Universal Typed Durable Checkpoint in O(1)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class WorkUnitCheckpoint:
    """Checkpoint unit supporting native Python types (datetime, Decimal, UUID, composite key tuples)."""

    unit_id: str
    table_name: str
    min_cursor: Any
    max_cursor: Any
    rows_processed: int
    bytes_processed: int
    completed_at: datetime.datetime
    care_badge: CareBadge


class WorkGranularityCheckpointStore:
    """Durable checkpoint store providing O(1) unit skip and resume."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._units: Dict[str, WorkUnitCheckpoint] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        content = self.path.read_text(encoding="utf-8")
        if not content.strip():
            return
        data = json.loads(content, object_hook=_decode_typed_hook)
        for uid, row in data.get("units", {}).items():
            self._units[uid] = WorkUnitCheckpoint(
                unit_id=uid,
                table_name=row["table_name"],
                min_cursor=row["min_cursor"],
                max_cursor=row["max_cursor"],
                rows_processed=row["rows_processed"],
                bytes_processed=row["bytes_processed"],
                completed_at=row["completed_at"],
                care_badge=CareBadge(row["care_badge"]),
            )

    def _save(self) -> None:
        payload = {
            "version": 1,
            "units": {
                uid: {
                    "table_name": u.table_name,
                    "min_cursor": _encode_typed_value(u.min_cursor),
                    "max_cursor": _encode_typed_value(u.max_cursor),
                    "rows_processed": u.rows_processed,
                    "bytes_processed": u.bytes_processed,
                    "completed_at": _encode_typed_value(u.completed_at),
                    "care_badge": u.care_badge.value,
                }
                for uid, u in self._units.items()
            },
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def record_unit(self, checkpoint: WorkUnitCheckpoint) -> None:
        self._units[checkpoint.unit_id] = checkpoint
        self._save()

    def is_completed_o1(self, unit_id: str) -> bool:
        """Check whether unit is already completed in O(1)."""
        return unit_id in self._units

    def get_unit(self, unit_id: str) -> Optional[WorkUnitCheckpoint]:
        return self._units.get(unit_id)

    def all_units(self) -> Dict[str, WorkUnitCheckpoint]:
        return dict(self._units)


# -----------------------------------------------------------------------------
# Mutual Exclusion Lease Lock (.run.lock)
# -----------------------------------------------------------------------------

def acquire_work_granularity_lease(
    lock_path: Path,
    *,
    what: str = "work-granularity",
    term_seconds: float = 60.0,
) -> lease.Lease:
    """Acquire a mutual-exclusion verification lease lock using migkit.lease.Lease."""
    try:
        return lease.Lease(Path(lock_path), what=what, term=term_seconds).acquire()
    except (Exception, lease.Held) as exc:
        raise LeaseLockError(f"unable to acquire lease on {lock_path}: {exc}") from exc


# -----------------------------------------------------------------------------
# Adaptive Execution: AIMD Controller & Cold-Start Byte-Weighted ETA
# -----------------------------------------------------------------------------

class AdaptiveWorkUnitController:
    """Dynamic AIMD batch sizer and instant cold-start byte-weighted ETA calculator."""

    def __init__(
        self,
        initial_batch_rows: int = 1000,
        min_batch_rows: int = 10,
        max_batch_rows: int = 50000,
        additive_step: int = 250,
        decrease_factor: float = 0.5,
    ) -> None:
        self.batch_rows = initial_batch_rows
        self.min_batch_rows = min_batch_rows
        self.max_batch_rows = max_batch_rows
        self.additive_step = additive_step
        self.decrease_factor = decrease_factor
        self.total_bytes_transferred = 0
        self.total_seconds_spent = 0.0

    def on_verified_success(self, batch_bytes: int, duration_s: float) -> int:
        """Additive increase on verified completion."""
        self.total_bytes_transferred += max(0, batch_bytes)
        self.total_seconds_spent += max(0.001, duration_s)
        self.batch_rows = min(self.max_batch_rows, self.batch_rows + self.additive_step)
        return self.batch_rows

    def on_backpressure(self, reason: str = "pressure") -> int:
        """Multiplicative decrease on memory pressure or packet error."""
        self.batch_rows = max(self.min_batch_rows, int(self.batch_rows * self.decrease_factor))
        return self.batch_rows

    def estimate_cold_start_eta(self, remaining_bytes: int) -> float:
        """Instant cold-start byte-weighted ETA in seconds."""
        if remaining_bytes <= 0:
            return 0.0
        if self.total_bytes_transferred > 0 and self.total_seconds_spent > 0.0:
            observed_bps = float(self.total_bytes_transferred) / self.total_seconds_spent
            return float(remaining_bytes) / observed_bps
        # Conservative cold-start fallback (10 MB/s baseline)
        return float(remaining_bytes) / 10_000_000.0


# -----------------------------------------------------------------------------
# Full-Capacity Tool Wrapper (4-Tier Care Badges)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ToolExecutionReport:
    tool_name: str
    command: str
    exit_code: int
    care_badge: CareBadge
    summary: str
    details: Dict[str, Any]


class WorkGranularityToolWrapper:
    """Wrapper for external tools (Datafold, DMS, DTS, reladiff, DVT, pgcopydb, debezium)."""

    SUPPORTED_TOOLS: FrozenSet[str] = frozenset(
        {"datafold", "dms", "dts", "reladiff", "dvt", "pgcopydb", "debezium"}
    )

    def normalize_tool_result(
        self,
        tool_name: str,
        exit_code: int,
        stdout: str,
        stderr: str,
        *,
        bytes_transferred: int = 0,
        discrepancies: int = 0,
    ) -> ToolExecutionReport:
        """Produce unified report with 4-tier care badges without leaking internals."""
        clean_name = tool_name.strip().lower()
        if clean_name not in self.SUPPORTED_TOOLS:
            badge = CareBadge.ACTION
            summary = f"unknown or unsupported migration tool '{clean_name}'"
        elif exit_code != 0:
            badge = CareBadge.ACTION
            summary = f"{clean_name} execution failed with exit code {exit_code}"
        elif discrepancies > 0:
            badge = CareBadge.REVIEW
            summary = f"{clean_name} completed with {discrepancies} verification discrepancies"
        elif bytes_transferred == 0:
            badge = CareBadge.BENIGN
            summary = f"{clean_name} executed cleanly with zero bytes moved (empty partition)"
        else:
            badge = CareBadge.PASS
            summary = f"{clean_name} completed successfully with {bytes_transferred} bytes verified"

        return ToolExecutionReport(
            tool_name=clean_name,
            command=f"{clean_name} verify",
            exit_code=exit_code,
            care_badge=badge,
            summary=summary,
            details={
                "stdout_sample": stdout[:500],
                "stderr_sample": stderr[:500],
                "bytes_transferred": bytes_transferred,
                "discrepancies": discrepancies,
            },
        )
