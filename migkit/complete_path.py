"""Optimize verified completion across the full path (Row 589: SMART-N6.complete-path).

Jointly optimizes seed, encoding/compression, transport/staging, ingestion,
indexes/constraints, CDC drain, verification, repair, cutover, and rollback
using `decide.Rung` and `decide.climb(work, rungs, facts)`.

Models downstream index/constraint and proof tails so a faster raw copy is
never preferred when its downstream proof/index/CDC tail makes total verified
completion slower. Incorporates multi-database/hop contention, CPU/disk/network/
service quotas, streaming backpressure with AIMD batch sizing, skew-aware
range split and small-table coalescing, bounded optimizer CPU/memory/time
with safe baseline retention on interruption, and distance to a valid lower
bound certificate without claiming a global optimum.

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
import pathlib
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from . import checkpoint, decide, lease

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class CompletePathStage(str, enum.Enum):
    SEED = "seed"
    ENCODING_COMPRESSION = "encoding_compression"
    TRANSPORT_STAGING = "transport_staging"
    INGESTION = "ingestion"
    INDEXES_CONSTRAINTS = "indexes_constraints"
    CDC_DRAIN = "cdc_drain"
    VERIFICATION = "verification"
    REPAIR = "repair"
    CUTOVER = "cutover"
    ROLLBACK = "rollback"


ALL_COMPLETE_PATH_STAGES: Tuple[CompletePathStage, ...] = (
    CompletePathStage.SEED,
    CompletePathStage.ENCODING_COMPRESSION,
    CompletePathStage.TRANSPORT_STAGING,
    CompletePathStage.INGESTION,
    CompletePathStage.INDEXES_CONSTRAINTS,
    CompletePathStage.CDC_DRAIN,
    CompletePathStage.VERIFICATION,
    CompletePathStage.REPAIR,
    CompletePathStage.CUTOVER,
    CompletePathStage.ROLLBACK,
)


class CompletePathStrategy(str, enum.Enum):
    DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX = (
        "direct_stream_inline_verify_deferred_index"
    )
    STAGED_OBJECT_BULK_LOAD_PARALLEL_PROOF = (
        "staged_object_bulk_load_parallel_proof"
    )
    UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY = (
        "uncompressed_fast_copy_posthoc_index_and_verify"
    )
    SAFE_BASELINE_CHUNKED_SERIAL_PATH = "safe_baseline_chunked_serial_path"


class EncodingTransportMode(str, enum.Enum):
    ZSTD_ADAPTIVE_DIRECT_STREAM = "zstd_adaptive_direct_stream"
    LZ4_FRAMED_STAGED_SPOOL = "lz4_framed_staged_spool"
    UNCOMPRESSED_PASSTHROUGH_STREAM = "uncompressed_passthrough_stream"


class SkewHandlingMode(str, enum.Enum):
    SKEW_AWARE_SPLIT_AND_SMALL_TABLE_COALESCE = (
        "skew_aware_split_and_small_table_coalesce"
    )
    DYNAMIC_RANGE_SPLIT_ONLY = "dynamic_range_split_only"
    UNIFORM_TABLE_SCHEDULE = "uniform_table_schedule"


class OptimizerSearchMode(str, enum.Enum):
    EXACT_JOINT_BRANCH_AND_BOUND = "exact_joint_branch_and_bound"
    HIERARCHICAL_BUCKETED_JOINT_PLAN = "hierarchical_bucketed_joint_plan"
    RETAIN_SAFE_BASELINE_ON_BUDGET_EXHAUSTION = (
        "retain_safe_baseline_on_budget_exhaustion"
    )


class CompletePathError(Exception):
    """Base error for complete-path planning, execution, and verification."""


class UnsafeEndpointError(CompletePathError):
    """Raised when an endpoint is unsafe or contains embedded credentials."""


class InvalidQuotaOrFactError(CompletePathError):
    """Raised when required measured throughput, quota, or table facts are invalid."""


class UnboundedCdcDrainError(CompletePathError):
    """Raised when CDC arrival rate exceeds or equals drain rate with no convergence."""


_SAFE_HOST_RE = re.compile(
    r"^(127\.\d+\.\d+\.\d+|10\.\d+\.\d+\.\d+|localhost|::1|[a-zA-Z0-9_-]+\.internal|[a-zA-Z0-9_-]+\.test|[a-zA-Z0-9_-]+\.local)$"
)


def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host is a safe test/internal address without secrets."""
    cleaned = str(host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError("Endpoint host must not be empty")
    if "@" in cleaned or ":" in cleaned.replace("::1", ""):
        raise UnsafeEndpointError(
            f"Endpoint host '{cleaned}' must not embed credentials or port"
        )
    if not _SAFE_HOST_RE.match(cleaned):
        raise UnsafeEndpointError(
            f"Endpoint host '{cleaned}' is not in the allowed safe host set"
        )
    return cleaned


# -----------------------------------------------------------------------------
# Domain Models: Quotas, Contention, Tables, Backpressure, Budgets
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class ResourceQuotaSpec:
    """Measured resource and service quotas governing the migration path."""

    cpu_cores_available: float
    disk_read_bytes_per_sec: float
    disk_write_bytes_per_sec: float
    network_bytes_per_sec: float
    service_requests_per_sec: float
    staging_quota_bytes: float
    memory_quota_bytes: int

    def __post_init__(self) -> None:
        if self.cpu_cores_available <= 0.0:
            raise InvalidQuotaOrFactError("cpu_cores_available must be > 0")
        if self.disk_read_bytes_per_sec <= 0.0 or self.disk_write_bytes_per_sec <= 0.0:
            raise InvalidQuotaOrFactError("disk read/write bytes_per_sec must be > 0")
        if self.network_bytes_per_sec <= 0.0:
            raise InvalidQuotaOrFactError("network_bytes_per_sec must be > 0")
        if self.service_requests_per_sec <= 0.0:
            raise InvalidQuotaOrFactError("service_requests_per_sec must be > 0")
        if self.staging_quota_bytes < 0.0:
            raise InvalidQuotaOrFactError("staging_quota_bytes must be >= 0")
        if self.memory_quota_bytes <= 0:
            raise InvalidQuotaOrFactError("memory_quota_bytes must be > 0")


@dataclass(frozen=True)
class HopContentionSpec:
    """Multi-database and multi-hop shared resource contention descriptors."""

    source_concurrent_databases: int = 1
    target_concurrent_databases: int = 1
    shared_network_hops: int = 1
    source_cpu_share_limit: float = 1.0
    target_write_share_limit: float = 1.0
    inter_hop_relay_overhead_ratio: float = 0.0

    def __post_init__(self) -> None:
        if self.source_concurrent_databases < 1 or self.target_concurrent_databases < 1:
            raise InvalidQuotaOrFactError("concurrent database count must be >= 1")
        if self.shared_network_hops < 1:
            raise InvalidQuotaOrFactError("shared_network_hops must be >= 1")
        if not (0.0 < self.source_cpu_share_limit <= 1.0):
            raise InvalidQuotaOrFactError("source_cpu_share_limit must be in (0, 1]")
        if not (0.0 < self.target_write_share_limit <= 1.0):
            raise InvalidQuotaOrFactError("target_write_share_limit must be in (0, 1]")
        if self.inter_hop_relay_overhead_ratio < 0.0:
            raise InvalidQuotaOrFactError("inter_hop_relay_overhead_ratio must be >= 0")

    def effective_quotas(self, base: ResourceQuotaSpec) -> ResourceQuotaSpec:
        """Compute fair-share quotas under multi-database and multi-hop contention."""
        src_div = float(self.source_concurrent_databases)
        tgt_div = float(self.target_concurrent_databases)
        hop_div = float(self.shared_network_hops) * (
            1.0 + self.inter_hop_relay_overhead_ratio
        )
        eff_cpu = (
            base.cpu_cores_available * self.source_cpu_share_limit
        ) / max(src_div, tgt_div)
        eff_read = (base.disk_read_bytes_per_sec * self.source_cpu_share_limit) / src_div
        eff_write = (
            base.disk_write_bytes_per_sec * self.target_write_share_limit
        ) / tgt_div
        eff_net = base.network_bytes_per_sec / hop_div
        eff_svc = base.service_requests_per_sec / max(src_div, tgt_div, hop_div)
        eff_stg = base.staging_quota_bytes / hop_div
        eff_mem = max(1, int(base.memory_quota_bytes // max(1, self.source_concurrent_databases)))
        return ResourceQuotaSpec(
            cpu_cores_available=max(0.01, eff_cpu),
            disk_read_bytes_per_sec=max(1.0, eff_read),
            disk_write_bytes_per_sec=max(1.0, eff_write),
            network_bytes_per_sec=max(1.0, eff_net),
            service_requests_per_sec=max(0.1, eff_svc),
            staging_quota_bytes=max(0.0, eff_stg),
            memory_quota_bytes=eff_mem,
        )


@dataclass(frozen=True)
class TableWorkItem:
    """One table's measured characteristics for complete-path optimization."""

    table_name: str
    total_bytes: int
    row_count: int
    secondary_index_count: int = 0
    constraint_count: int = 0
    max_partition_bytes: int = 0
    median_partition_bytes: int = 0
    has_splittable_key: bool = True
    estimated_drift_bytes: int = 0

    def __post_init__(self) -> None:
        if not self.table_name:
            raise InvalidQuotaOrFactError("table_name must not be empty")
        if self.total_bytes < 0 or self.row_count < 0:
            raise InvalidQuotaOrFactError("total_bytes and row_count must be >= 0")
        if self.secondary_index_count < 0 or self.constraint_count < 0:
            raise InvalidQuotaOrFactError("index and constraint counts must be >= 0")

    @property
    def skew_ratio(self) -> float:
        if self.median_partition_bytes <= 0 or self.max_partition_bytes <= 0:
            return 1.0
        return max(1.0, float(self.max_partition_bytes) / float(self.median_partition_bytes))


@dataclass(frozen=True)
class PlannedWorkUnit:
    """Output unit after skew-aware split or small-table coalescing."""

    unit_id: str
    table_names: Tuple[str, ...]
    total_bytes: int
    row_count: int
    secondary_index_count: int
    constraint_count: int
    is_split_range: bool
    is_coalesced_bundle: bool
    range_index: int = 0
    range_total: int = 1


@dataclass(frozen=True)
class BackpressureTelemetry:
    """Measured streaming producer/consumer rates and queue occupancy."""

    producer_bytes_per_sec: float
    consumer_bytes_per_sec: float
    queue_buffered_bytes: float
    queue_capacity_bytes: float
    current_batch_rows: int
    min_batch_rows: int
    max_batch_rows: int
    measured_batch_latency_sec: float
    target_batch_latency_sec: float
    additive_increase_rows: int
    multiplicative_decrease_factor: float

    @property
    def stall_ratio(self) -> float:
        if self.producer_bytes_per_sec <= 0.0:
            return 0.0
        rate_gap = max(
            0.0,
            1.0 - (self.consumer_bytes_per_sec / self.producer_bytes_per_sec),
        )
        queue_fill = (
            min(1.0, self.queue_buffered_bytes / self.queue_capacity_bytes)
            if self.queue_capacity_bytes > 0.0
            else 0.0
        )
        return min(1.0, max(rate_gap, queue_fill * rate_gap + 0.5 * queue_fill))


@dataclass(frozen=True)
class OptimizerBudget:
    """Explicit resource bounds on the complete-path optimizer search."""

    max_wall_seconds: float
    max_cpu_seconds: float
    max_memory_bytes: int
    max_iterations: int
    exact_max_tables: int = 250
    interrupt_check: Optional[Callable[[], bool]] = None

    def __post_init__(self) -> None:
        if self.max_wall_seconds <= 0.0 or self.max_cpu_seconds <= 0.0:
            raise InvalidQuotaOrFactError("Optimizer time budgets must be > 0")
        if self.max_memory_bytes <= 0 or self.max_iterations <= 0:
            raise InvalidQuotaOrFactError("Optimizer memory and iteration budgets must be > 0")


# -----------------------------------------------------------------------------
# Decide Ladders (`decide.Rung` / `decide.climb`)
# -----------------------------------------------------------------------------


def complete_path_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for joint 10-stage verified completion path."""
    return (
        decide.Rung(
            "direct_stream_inline_verify_deferred_index",
            "stream seed with adaptive compression, deferred parallel index build, and inline Merkle verification",
            needs=(
                decide.Need(
                    "direct streaming transport available between endpoints",
                    lambda f: bool(f.get("direct_streaming_available", True)),
                    "{rung} requires direct streaming connectivity between source and target",
                ),
                decide.Need(
                    "deferred index build and inline verification beat raw copy full-path time",
                    lambda f: bool(f.get("deferred_index_inline_proof_viable", True))
                    and float(f.get("full_path_verified_seconds_inline", 0.0))
                    <= float(f.get("full_path_verified_seconds_raw_copy", float("inf"))),
                    "{rung} is not preferred when its full-path verified completion time exceeds raw copy",
                ),
                decide.Need(
                    "CPU, network, and service quotas support inline streaming pipeline",
                    lambda f: bool(f.get("quotas_allow_direct_stream", True)),
                    "{rung} exceeds available CPU, network, or service request quotas",
                ),
            ),
            because="minimizes end-to-end verified completion by avoiding post-hoc full scan proof and inline index maintenance amplification",
            footprint=1,
            leaves="streaming checkpoint cursor and deferred index build manifest",
        ),
        decide.Rung(
            "staged_object_bulk_load_parallel_proof",
            "compressed staging spool with target bulk loader, parallel index build, and bucketed verification",
            needs=(
                decide.Need(
                    "staging storage and target bulk loader available",
                    lambda f: bool(f.get("staging_storage_available", False))
                    and bool(f.get("bulk_load_supported", True)),
                    "{rung} requires staging storage quota and target bulk load support",
                ),
                decide.Need(
                    "staged bulk path verified completion finishes within CDC and cutover window",
                    lambda f: bool(f.get("staged_bulk_within_window", True)),
                    "{rung} exceeds the allowed CDC drain and verified cutover window",
                ),
            ),
            because="decouples multi-hop producer/consumer contention via staged bulk ingestion and parallel index/proof phases",
            footprint=2,
            leaves="staging objects, bulk load manifest, and range checkpoint state",
        ),
        decide.Rung(
            "uncompressed_fast_copy_posthoc_index_and_verify",
            "uncompressed fast copy with inline/post-hoc index maintenance and post-copy full verification scan",
            needs=(
                decide.Need(
                    "downstream index, CDC drain, and post-hoc proof tail make raw fast copy competitive",
                    lambda f: bool(f.get("raw_copy_tail_competitive", False)),
                    "{rung} has a faster raw copy but its downstream index, CDC drain, and post-hoc proof tail is slower overall",
                ),
                decide.Need(
                    "quotas allow uncompressed transfer and second full read scan for verification",
                    lambda f: bool(f.get("quotas_allow_double_scan", False)),
                    "{rung} requires network and disk I/O quotas sufficient for uncompressed transfer and a second full verification scan",
                ),
            ),
            because="uses raw copy only when low index density and high I/O headroom keep downstream index and proof tails short",
            footprint=1,
            leaves="post-hoc verification scan state",
        ),
        decide.Rung(
            "safe_baseline_chunked_serial_path",
            "conservative chunked seed, throttled ingestion, incremental index maintenance, and per-chunk verification",
            needs=(
                decide.Need(
                    "safe baseline endpoints, privileges, and minimum quota available",
                    lambda f: bool(f.get("safe_baseline_available", True)),
                    "{rung} cannot run because minimum endpoint connectivity or quota is unavailable",
                ),
            ),
            because="provides a bounded-memory, resumable safe baseline when faster joint paths are refused or search is interrupted",
            footprint=0,
            leaves="chunk checkpoint ledger",
        ),
    )


def encoding_transport_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for joint encoding/compression and transport/staging."""
    return (
        decide.Rung(
            "zstd_adaptive_direct_stream",
            "adaptive zstd compression over direct streaming transport with backpressure pacing",
            needs=(
                decide.Need(
                    "CPU utilization ratio is below compression saturation ceiling",
                    lambda f: float(f.get("cpu_utilization_ratio", 0.5))
                    < float(f.get("cpu_compression_ceiling", 0.85)),
                    "{rung} refused because CPU utilization exceeds the compression saturation ceiling",
                ),
                decide.Need(
                    "streaming backpressure stall ratio is within direct stream stability bound",
                    lambda f: float(f.get("backpressure_stall_ratio", 0.0))
                    <= float(f.get("max_direct_backpressure_ratio", 0.35)),
                    "{rung} refused because target ingestion backpressure stalls direct streaming",
                ),
            ),
            because="reduces network and multi-hop transfer time while CPU headroom is available and stream backpressure is low",
            footprint=0,
        ),
        decide.Rung(
            "lz4_framed_staged_spool",
            "lightweight LZ4 framing with bounded staging spool to absorb backpressure and hop contention",
            needs=(
                decide.Need(
                    "staging quota bytes cover required backpressure spool buffer",
                    lambda f: float(f.get("staging_quota_bytes", 0.0))
                    >= float(f.get("required_spool_bytes", 1.0)),
                    "{rung} refused because staging quota is smaller than required spool buffer",
                ),
                decide.Need(
                    "LZ4 staged spool enabled for hop",
                    lambda f: bool(f.get("lz4_spool_enabled", True)),
                    "{rung} refused because LZ4 staged spool is disabled on this hop",
                ),
            ),
            because="absorbs target ingestion stalls and multi-hop jitter with minimal CPU overhead",
            footprint=1,
            leaves="bounded LZ4 staging spool",
        ),
        decide.Rung(
            "uncompressed_passthrough_stream",
            "uncompressed passthrough stream when CPU is saturated and network link has surplus quota",
            needs=(
                decide.Need(
                    "network quota exceeds required raw uncompressed throughput",
                    lambda f: float(f.get("network_quota_bytes_per_sec", 0.0))
                    >= float(f.get("raw_stream_required_bytes_per_sec", 1.0)),
                    "{rung} refused because network quota cannot carry uncompressed stream without throttling",
                ),
                decide.Need(
                    "uncompressed passthrough permitted by hop policy",
                    lambda f: bool(f.get("allow_uncompressed_passthrough", True)),
                    "{rung} refused because hop policy requires compressed transport",
                ),
            ),
            because="eliminates compression CPU bottleneck when link bandwidth is non-binding",
            footprint=0,
        ),
    )


def skew_partition_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for skew-aware range splitting and small-table coalescing."""
    return (
        decide.Rung(
            "skew_aware_split_and_small_table_coalesce",
            "split skewed hot tables across key ranges and coalesce small tables into bounded bundles",
            needs=(
                decide.Need(
                    "splittable keys available on skewed tables and bundle coalescing permitted",
                    lambda f: bool(f.get("range_split_supported", True))
                    and bool(f.get("coalesce_supported", True)),
                    "{rung} requires splittable key ranges and bundle coalescing support",
                ),
                decide.Need(
                    "measured partition skew or table count exceeds split/coalesce threshold",
                    lambda f: float(f.get("max_to_median_skew_ratio", 1.0))
                    >= float(f.get("skew_split_threshold", 2.0))
                    or int(f.get("table_count", 0))
                    >= int(f.get("coalesce_table_threshold", 50)),
                    "{rung} is unneeded when partition skew is flat and table count is small",
                ),
            ),
            because="eliminates straggler partition tails and amortizes per-table startup/proof overhead across thousands of tables",
            footprint=1,
        ),
        decide.Rung(
            "dynamic_range_split_only",
            "split skewed hot tables into balanced key ranges without coalescing small tables",
            needs=(
                decide.Need(
                    "range split supported on hot tables",
                    lambda f: bool(f.get("range_split_supported", True)),
                    "{rung} requires splittable key ranges on skewed tables",
                ),
                decide.Need(
                    "measured partition skew exceeds split threshold",
                    lambda f: float(f.get("max_to_median_skew_ratio", 1.0))
                    >= float(f.get("skew_split_threshold", 2.0)),
                    "{rung} is only selected when measured partition skew exceeds threshold",
                ),
            ),
            because="parallelizes skewed giant tables when table count is small enough not to need coalescing",
            footprint=1,
        ),
        decide.Rung(
            "uniform_table_schedule",
            "one scheduling unit per table in size-weighted order",
            needs=(
                decide.Need(
                    "uniform per-table scheduling enabled",
                    lambda f: bool(f.get("uniform_schedule_allowed", True)),
                    "{rung} refused because uniform per-table scheduling is disabled",
                ),
            ),
            because="preserves simple 1-to-1 table units when skew is low and table count is small",
            footprint=0,
        ),
    )


def optimizer_search_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for bounded optimizer search regime."""
    return (
        decide.Rung(
            "exact_joint_branch_and_bound",
            "exact joint branch-and-bound evaluation across full-path stage candidates",
            needs=(
                decide.Need(
                    "table count and state memory fit within exact optimization bounds",
                    lambda f: int(f.get("table_count", 0))
                    <= int(f.get("exact_max_tables", 250))
                    and int(f.get("estimated_state_bytes", 0))
                    <= int(f.get("max_memory_bytes", 64 * 1024 * 1024)),
                    "{rung} exceeds exact table count or memory budget; hierarchical bucketed search required",
                ),
                decide.Need(
                    "optimizer search not interrupted and wall/CPU budget remaining",
                    lambda f: not bool(f.get("search_interrupted", False))
                    and float(f.get("remaining_time_budget_sec", 0.0))
                    > float(f.get("min_exact_budget_sec", 0.005)),
                    "{rung} refused because optimizer budget is exhausted or search was interrupted",
                ),
            ),
            because="evaluates exact per-table joint stage interactions when table count fits tight time and memory bounds",
            footprint=0,
        ),
        decide.Rung(
            "hierarchical_bucketed_joint_plan",
            "equivalence-bucketed O(N log N) joint path optimization for thousands of tables",
            needs=(
                decide.Need(
                    "bucketed state memory and remaining time fit within optimizer budget",
                    lambda f: int(f.get("estimated_bucket_memory_bytes", 0))
                    <= int(f.get("max_memory_bytes", 64 * 1024 * 1024))
                    and float(f.get("remaining_time_budget_sec", 0.0)) > 0.0,
                    "{rung} exceeds memory or time budget for hierarchical bucketed planning",
                ),
                decide.Need(
                    "optimizer search not interrupted before bucketed pass",
                    lambda f: not bool(f.get("search_interrupted", False)),
                    "{rung} refused because search was interrupted",
                ),
            ),
            because="scales full-path optimization to thousands of tables within bounded CPU, memory, and wall time",
            footprint=0,
        ),
        decide.Rung(
            "retain_safe_baseline_on_budget_exhaustion",
            "retain pre-validated safe baseline plan when search is interrupted or budget is exhausted",
            needs=(
                decide.Need(
                    "safe baseline plan initialized and verified",
                    lambda f: bool(f.get("safe_baseline_initialized", True)),
                    "{rung} requires a pre-built safe baseline plan",
                ),
            ),
            because="guarantees a valid, safe execution plan even when optimization is interrupted immediately",
            footprint=0,
        ),
    )


# -----------------------------------------------------------------------------
# Stage Breakdown, Lower Bound Certificate & Plan Result
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class StageCostBreakdown:
    """Measured or modeled duration and bottleneck resource for each of the 10 stages."""

    stage_seconds: Dict[CompletePathStage, float]
    copy_only_seconds: float
    downstream_tail_seconds: float
    total_verified_seconds: float
    accumulated_cdc_backlog_bytes: float
    binding_resource: str

    def __post_init__(self) -> None:
        for st in ALL_COMPLETE_PATH_STAGES:
            if st not in self.stage_seconds:
                raise InvalidQuotaOrFactError(f"Missing stage cost for {st.value}")


@dataclass(frozen=True)
class LowerBoundCertificate:
    """Valid mathematical lower bound on full-path verified completion time.

    Never claims a global optimum; reports explicit absolute and relative distance
    from the planned schedule to the valid lower bound across resource bottlenecks,
    critical-chain serial work, and minimum CDC/cutover drain.
    """

    lower_bound_seconds: float
    resource_bottleneck_lb_seconds: float
    critical_chain_lb_seconds: float
    cdc_cutover_lb_seconds: float
    planned_verified_seconds: float
    absolute_gap_seconds: float
    relative_gap_ratio: float
    dominant_lower_bound_term: str
    claims_global_optimum: bool = False


@dataclass(frozen=True)
class CompletePathPlan:
    """Complete verified migration path plan produced by `optimize_complete_path`."""

    path_choice: decide.Choice
    encoding_transport_choice: decide.Choice
    skew_choice: decide.Choice
    search_choice: decide.Choice
    chosen_strategy: Optional[CompletePathStrategy]
    encoding_transport_mode: EncodingTransportMode
    skew_handling_mode: SkewHandlingMode
    search_mode: OptimizerSearchMode
    stage_breakdown: StageCostBreakdown
    candidate_breakdowns: Dict[CompletePathStrategy, StageCostBreakdown]
    work_units: Tuple[PlannedWorkUnit, ...]
    lower_bound: LowerBoundCertificate
    safe_baseline_breakdown: StageCostBreakdown
    retained_safe_baseline: bool
    interrupted: bool
    interruption_reason: Optional[str]
    optimizer_wall_seconds: float
    optimizer_cpu_seconds: float
    optimizer_memory_bytes: int
    effective_quotas: ResourceQuotaSpec
    recommended_batch_rows: int
    care_badge: CareBadge


# -----------------------------------------------------------------------------
# AIMD Backpressure Batch Sizing & Cold-Start Byte-Weighted ETA
# -----------------------------------------------------------------------------


def adjust_aimd_batch_size(telemetry: BackpressureTelemetry) -> int:
    """Adjust streaming batch size via Additive Increase / Multiplicative Decrease (AIMD)."""
    if telemetry.min_batch_rows <= 0 or telemetry.max_batch_rows < telemetry.min_batch_rows:
        raise InvalidQuotaOrFactError("Invalid min_batch_rows / max_batch_rows bounds")
    if not (0.0 < telemetry.multiplicative_decrease_factor < 1.0):
        raise InvalidQuotaOrFactError("multiplicative_decrease_factor must be in (0, 1)")
    if telemetry.additive_increase_rows < 1:
        raise InvalidQuotaOrFactError("additive_increase_rows must be >= 1")

    congested = (
        telemetry.stall_ratio > 0.25
        or telemetry.measured_batch_latency_sec > telemetry.target_batch_latency_sec
    )
    if congested:
        reduced = int(
            math.floor(
                float(telemetry.current_batch_rows)
                * telemetry.multiplicative_decrease_factor
            )
        )
        return max(telemetry.min_batch_rows, min(telemetry.max_batch_rows, reduced))

    increased = telemetry.current_batch_rows + telemetry.additive_increase_rows
    return max(telemetry.min_batch_rows, min(telemetry.max_batch_rows, increased))


def estimate_byte_weighted_eta(
    *,
    total_bytes: int,
    completed_bytes: int,
    elapsed_seconds: float,
    cold_start_bytes_per_sec: float,
    downstream_tail_seconds: float = 0.0,
) -> float:
    """Compute instant cold-start byte-weighted ETA including downstream proof/index tail."""
    if total_bytes < 0 or completed_bytes < 0:
        raise InvalidQuotaOrFactError("total_bytes and completed_bytes must be >= 0")
    if cold_start_bytes_per_sec <= 0.0:
        raise InvalidQuotaOrFactError("cold_start_bytes_per_sec must be > 0")

    remaining_bytes = max(0, total_bytes - completed_bytes)
    if remaining_bytes == 0:
        return max(0.0, float(downstream_tail_seconds))

    if completed_bytes > 0 and elapsed_seconds > 0.0:
        observed_rate = float(completed_bytes) / float(elapsed_seconds)
        progress_weight = min(1.0, float(completed_bytes) / max(1.0, float(total_bytes)))
        blended_rate = (
            progress_weight * observed_rate
            + (1.0 - progress_weight) * float(cold_start_bytes_per_sec)
        )
    else:
        blended_rate = float(cold_start_bytes_per_sec)

    return (float(remaining_bytes) / max(1.0, blended_rate)) + max(
        0.0, float(downstream_tail_seconds)
    )


# -----------------------------------------------------------------------------
# Skew-Aware Range Split & Small-Table Coalescing
# -----------------------------------------------------------------------------


def plan_skew_aware_work_units(
    tables: Sequence[TableWorkItem],
    *,
    skew_mode: SkewHandlingMode,
    target_chunk_bytes: int,
    coalesce_bundle_max_bytes: int,
    coalesce_bundle_max_tables: int,
    max_splits_per_table: int = 16,
) -> Tuple[PlannedWorkUnit, ...]:
    """Build balanced work units by splitting skewed tables and coalescing tiny tables."""
    if target_chunk_bytes <= 0 or coalesce_bundle_max_bytes <= 0 or coalesce_bundle_max_tables <= 0:
        raise InvalidQuotaOrFactError("Chunk and bundle bounds must be > 0")

    if skew_mode == SkewHandlingMode.UNIFORM_TABLE_SCHEDULE:
        ordered = sorted(tables, key=lambda t: (-t.total_bytes, t.table_name))
        return tuple(
            PlannedWorkUnit(
                unit_id=f"unit:{t.table_name}:0",
                table_names=(t.table_name,),
                total_bytes=t.total_bytes,
                row_count=t.row_count,
                secondary_index_count=t.secondary_index_count,
                constraint_count=t.constraint_count,
                is_split_range=False,
                is_coalesced_bundle=False,
                range_index=0,
                range_total=1,
            )
            for t in ordered
        )

    units: List[PlannedWorkUnit] = []
    small_candidates: List[TableWorkItem] = []

    for tbl in sorted(tables, key=lambda t: (-t.total_bytes, t.table_name)):
        should_split = (
            tbl.has_splittable_key
            and (tbl.skew_ratio >= 2.0 or tbl.total_bytes >= 2 * target_chunk_bytes)
            and tbl.total_bytes > target_chunk_bytes
        )
        if should_split:
            raw_parts = max(
                2,
                int(math.ceil(float(tbl.total_bytes) / float(target_chunk_bytes))),
                int(math.ceil(tbl.skew_ratio)),
            )
            num_parts = min(max_splits_per_table, raw_parts)
            part_bytes = int(math.ceil(float(tbl.total_bytes) / float(num_parts)))
            part_rows = int(math.ceil(float(tbl.row_count) / float(num_parts)))
            for idx in range(num_parts):
                rem_bytes = max(0, tbl.total_bytes - idx * part_bytes)
                chunk_b = min(part_bytes, rem_bytes)
                rem_rows = max(0, tbl.row_count - idx * part_rows)
                chunk_r = min(part_rows, rem_rows)
                units.append(
                    PlannedWorkUnit(
                        unit_id=f"unit:{tbl.table_name}:range:{idx + 1}-of-{num_parts}",
                        table_names=(tbl.table_name,),
                        total_bytes=chunk_b,
                        row_count=chunk_r,
                        secondary_index_count=tbl.secondary_index_count,
                        constraint_count=tbl.constraint_count,
                        is_split_range=True,
                        is_coalesced_bundle=False,
                        range_index=idx,
                        range_total=num_parts,
                    )
                )
        elif (
            skew_mode == SkewHandlingMode.SKEW_AWARE_SPLIT_AND_SMALL_TABLE_COALESCE
            and tbl.total_bytes <= coalesce_bundle_max_bytes // 2
        ):
            small_candidates.append(tbl)
        else:
            units.append(
                PlannedWorkUnit(
                    unit_id=f"unit:{tbl.table_name}:0",
                    table_names=(tbl.table_name,),
                    total_bytes=tbl.total_bytes,
                    row_count=tbl.row_count,
                    secondary_index_count=tbl.secondary_index_count,
                    constraint_count=tbl.constraint_count,
                    is_split_range=False,
                    is_coalesced_bundle=False,
                    range_index=0,
                    range_total=1,
                )
            )

    if small_candidates:
        bundle_tables: List[TableWorkItem] = []
        bundle_bytes = 0
        bundle_idx = 0

        def _flush_bundle(items: List[TableWorkItem], idx: int) -> PlannedWorkUnit:
            names = tuple(i.table_name for i in items)
            return PlannedWorkUnit(
                unit_id=f"unit:bundle:{idx}:{names[0]}..{names[-1]}",
                table_names=names,
                total_bytes=sum(i.total_bytes for i in items),
                row_count=sum(i.row_count for i in items),
                secondary_index_count=sum(i.secondary_index_count for i in items),
                constraint_count=sum(i.constraint_count for i in items),
                is_split_range=False,
                is_coalesced_bundle=len(items) > 1,
                range_index=0,
                range_total=1,
            )

        for item in small_candidates:
            if bundle_tables and (
                bundle_bytes + item.total_bytes > coalesce_bundle_max_bytes
                or len(bundle_tables) >= coalesce_bundle_max_tables
            ):
                units.append(_flush_bundle(bundle_tables, bundle_idx))
                bundle_idx += 1
                bundle_tables = []
                bundle_bytes = 0
            bundle_tables.append(item)
            bundle_bytes += item.total_bytes

        if bundle_tables:
            units.append(_flush_bundle(bundle_tables, bundle_idx))

    return tuple(units)


# -----------------------------------------------------------------------------
# Full 10-Stage Cost Evaluation & Lower Bound Certificate
# -----------------------------------------------------------------------------


def evaluate_strategy_stage_costs(
    strategy: CompletePathStrategy,
    *,
    tables: Sequence[TableWorkItem],
    work_units: Sequence[PlannedWorkUnit],
    effective_quotas: ResourceQuotaSpec,
    encoding_mode: EncodingTransportMode,
    facts_map: Mapping[str, Any],
) -> StageCostBreakdown:
    """Evaluate all 10 stages jointly for `strategy`, including downstream tails.

    Explicitly captures how an uncompressed/unindexed 'fast copy' incurs slower
    downstream index rebuilds, larger accumulated CDC backlog, and full-scan
    post-hoc verification tails compared to a joint inline-verified deferred-index path.
    """
    total_bytes = float(
        sum(t.total_bytes for t in tables)
        if tables
        else float(facts_map.get("dataset_bytes", 0.0))
    )
    total_indexes = sum(t.secondary_index_count for t in tables) if tables else int(
        facts_map.get("secondary_index_count", 4)
    )
    total_constraints = sum(t.constraint_count for t in tables) if tables else int(
        facts_map.get("constraint_count", 2)
    )
    drift_bytes = float(
        sum(t.estimated_drift_bytes for t in tables)
        if tables
        else float(facts_map.get("estimated_drift_bytes", 0.0))
    )
    unit_count = max(1, len(work_units) if work_units else len(tables) or 1)
    per_unit_startup_sec = float(facts_map.get("per_unit_startup_sec", 0.02))

    # Measured base rates bounded by effective quotas
    seed_rate = min(
        effective_quotas.disk_read_bytes_per_sec,
        float(facts_map.get("measured_seed_bytes_per_sec", effective_quotas.disk_read_bytes_per_sec)),
    )
    cpu_bytes_per_core_sec = float(
        facts_map.get("measured_cpu_bytes_per_core_sec", 120_000_000.0)
    )
    cpu_total_rate = max(1.0, effective_quotas.cpu_cores_available * cpu_bytes_per_core_sec)

    # Compression ratio and encoding CPU cost by mode and strategy
    if strategy == CompletePathStrategy.UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY:
        comp_ratio = 1.0
        encoding_cpu_factor = 0.08
    elif strategy == CompletePathStrategy.SAFE_BASELINE_CHUNKED_SERIAL_PATH:
        comp_ratio = float(facts_map.get("baseline_compression_ratio", 0.55))
        encoding_cpu_factor = 0.35
    elif encoding_mode == EncodingTransportMode.ZSTD_ADAPTIVE_DIRECT_STREAM:
        comp_ratio = float(facts_map.get("zstd_compression_ratio", 0.32))
        encoding_cpu_factor = 0.45
    elif encoding_mode == EncodingTransportMode.LZ4_FRAMED_STAGED_SPOOL:
        comp_ratio = float(facts_map.get("lz4_compression_ratio", 0.52))
        encoding_cpu_factor = 0.20
    else:
        comp_ratio = 1.0
        encoding_cpu_factor = 0.10

    wire_bytes = total_bytes * comp_ratio

    # 1. SEED stage
    max_skew = max((t.skew_ratio for t in tables), default=1.0)
    has_split = any(u.is_split_range for u in work_units)
    skew_penalty = 1.0 if has_split else min(3.0, 1.0 + 0.25 * max(0.0, max_skew - 1.0))
    seed_seconds = (
        (total_bytes / max(1.0, seed_rate)) * skew_penalty
        + float(unit_count) * per_unit_startup_sec
    )

    # 2. ENCODING_COMPRESSION stage
    encoding_seconds = (total_bytes * encoding_cpu_factor) / cpu_total_rate

    # 3. TRANSPORT_STAGING stage
    net_rate = effective_quotas.network_bytes_per_sec
    if strategy == CompletePathStrategy.STAGED_OBJECT_BULK_LOAD_PARALLEL_PROOF:
        # Write to staging + read from staging
        staging_io_rate = min(
            effective_quotas.disk_write_bytes_per_sec,
            effective_quotas.disk_read_bytes_per_sec,
        )
        transport_seconds = (wire_bytes / max(1.0, net_rate)) + (
            wire_bytes / max(1.0, staging_io_rate)
        )
    else:
        transport_seconds = wire_bytes / max(1.0, net_rate)

    # 4. INGESTION stage (including streaming backpressure penalty)
    backpressure_stall = float(facts_map.get("backpressure_stall_ratio", 0.0))
    base_ingest_rate = min(
        effective_quotas.disk_write_bytes_per_sec,
        float(
            facts_map.get(
                "measured_ingest_bytes_per_sec",
                effective_quotas.disk_write_bytes_per_sec,
            )
        ),
    )
    if strategy == CompletePathStrategy.DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX:
        # Deferred index build keeps ingestion sequential; inline Merkle hashing adds modest CPU
        ingest_multiplier = 1.05 * (1.0 + 0.3 * backpressure_stall)
    elif strategy == CompletePathStrategy.STAGED_OBJECT_BULK_LOAD_PARALLEL_PROOF:
        # Staged spool absorbs streaming backpressure stalls
        ingest_multiplier = 0.95 * (1.0 + 0.1 * backpressure_stall)
    elif strategy == CompletePathStrategy.UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY:
        # Raw copy blasts heap quickly during copy step itself
        ingest_multiplier = 0.80 * (1.0 + 0.5 * backpressure_stall)
    else:
        # Safe baseline serial chunked transactional writes with inline index maintenance
        ingest_multiplier = 1.35 * (
            1.0 + 0.12 * float(min(10, total_indexes)) + 0.25 * backpressure_stall
        )

    ingestion_seconds = (total_bytes / max(1.0, base_ingest_rate)) * ingest_multiplier

    # Pipelined overlap for seed/encoding/transport/ingestion vs staged sequential
    if strategy in (
        CompletePathStrategy.DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX,
        CompletePathStrategy.UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY,
    ):
        # Streaming pipeline: bounded by slowest pipeline stage + startup/drain fill
        pipeline_dominant = max(
            seed_seconds, encoding_seconds, transport_seconds, ingestion_seconds
        )
        pipeline_fill = 0.25 * (
            seed_seconds + encoding_seconds + transport_seconds + ingestion_seconds
            - pipeline_dominant
        )
        copy_only_seconds = pipeline_dominant + pipeline_fill
    else:
        copy_only_seconds = (
            seed_seconds + encoding_seconds + transport_seconds + ingestion_seconds
        )

    # 5. INDEXES_CONSTRAINTS stage
    index_work_bytes = total_bytes * (
        0.22 * float(min(16, total_indexes)) + 0.08 * float(min(16, total_constraints))
    )
    index_build_rate = min(
        effective_quotas.disk_write_bytes_per_sec,
        cpu_total_rate,
        float(
            facts_map.get(
                "measured_index_build_bytes_per_sec",
                effective_quotas.disk_write_bytes_per_sec,
            )
        ),
    )
    if strategy == CompletePathStrategy.DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX:
        # Parallel sort-based deferred index build on pre-ordered ranges
        indexes_seconds = (index_work_bytes * 0.45) / max(1.0, index_build_rate)
    elif strategy == CompletePathStrategy.STAGED_OBJECT_BULK_LOAD_PARALLEL_PROOF:
        indexes_seconds = (index_work_bytes * 0.50) / max(1.0, index_build_rate)
    elif strategy == CompletePathStrategy.UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY:
        # Unordered heap requires expensive random-I/O sort & full-table constraint validation tail
        random_io_penalty = float(facts_map.get("unindexed_posthoc_rebuild_penalty", 2.4))
        indexes_seconds = (index_work_bytes * random_io_penalty) / max(
            1.0, index_build_rate
        )
    else:
        # Safe baseline maintained most indexes incrementally; still validates constraints
        indexes_seconds = (index_work_bytes * 0.65) / max(1.0, index_build_rate)

    # 6. CDC_DRAIN stage (depends on copy_only_seconds + indexes_seconds!)
    initial_cdc_backlog = float(facts_map.get("initial_cdc_backlog_bytes", 0.0))
    cdc_arrival_rate = float(facts_map.get("cdc_arrival_bytes_per_sec", 0.0))
    cdc_apply_rate = min(
        effective_quotas.disk_write_bytes_per_sec,
        float(
            facts_map.get(
                "measured_cdc_apply_bytes_per_sec",
                effective_quotas.disk_write_bytes_per_sec * 0.6,
            )
        ),
    )
    pre_cdc_window_sec = copy_only_seconds + indexes_seconds
    accumulated_cdc_bytes = initial_cdc_backlog + cdc_arrival_rate * pre_cdc_window_sec
    if accumulated_cdc_bytes > 0.0:
        net_drain_rate = cdc_apply_rate - cdc_arrival_rate
        if net_drain_rate <= 0.0:
            raise UnboundedCdcDrainError(
                f"CDC apply rate ({cdc_apply_rate:.1f} B/s) <= arrival rate ({cdc_arrival_rate:.1f} B/s); drain cannot converge"
            )
        cdc_drain_seconds = accumulated_cdc_bytes / net_drain_rate
    else:
        cdc_drain_seconds = 0.0

    # 7. VERIFICATION stage
    verify_scan_rate = min(
        effective_quotas.disk_read_bytes_per_sec,
        cpu_total_rate,
        float(
            facts_map.get(
                "measured_verify_bytes_per_sec",
                effective_quotas.disk_read_bytes_per_sec,
            )
        ),
    )
    if strategy == CompletePathStrategy.DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX:
        # Inline Merkle fold during stream + delta verification only on CDC-mutated ranges
        verify_bytes = max(total_bytes * 0.05, min(total_bytes, accumulated_cdc_bytes * 1.5))
        verification_seconds = (verify_bytes / max(1.0, verify_scan_rate)) + float(
            unit_count
        ) * 0.005
    elif strategy == CompletePathStrategy.STAGED_OBJECT_BULK_LOAD_PARALLEL_PROOF:
        # Bucketed pushdown verification against staged manifest hashes
        verify_bytes = max(total_bytes * 0.15, min(total_bytes, accumulated_cdc_bytes * 2.0))
        verification_seconds = (verify_bytes / max(1.0, verify_scan_rate)) + float(
            unit_count
        ) * 0.008
    elif strategy == CompletePathStrategy.UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY:
        # Must re-scan 100% of source and target tables post-hoc to prove byte parity
        posthoc_scan_factor = float(facts_map.get("posthoc_full_verify_scan_factor", 2.0))
        verification_seconds = (
            (total_bytes * posthoc_scan_factor) / max(1.0, verify_scan_rate)
            + float(unit_count) * 0.02
        )
    else:
        # Per-chunk verification along serial baseline
        verification_seconds = (total_bytes * 0.45) / max(1.0, verify_scan_rate) + float(
            unit_count
        ) * 0.01

    # 8. REPAIR stage
    repair_rate = max(1.0, min(effective_quotas.disk_write_bytes_per_sec, net_rate) * 0.5)
    repair_fence_sec = float(facts_map.get("repair_fence_overhead_sec", 0.5)) if drift_bytes > 0 else 0.0
    repair_seconds = (drift_bytes / repair_rate) + repair_fence_sec

    # 9. CUTOVER stage
    cutover_seconds = float(facts_map.get("measured_cutover_fence_seconds", 2.0))

    # 10. ROLLBACK readiness & verification stage
    rollback_seconds = float(facts_map.get("measured_rollback_readiness_seconds", 1.0))

    downstream_tail_seconds = (
        indexes_seconds
        + cdc_drain_seconds
        + verification_seconds
        + repair_seconds
        + cutover_seconds
        + rollback_seconds
    )
    total_verified_seconds = copy_only_seconds + downstream_tail_seconds

    stage_map: Dict[CompletePathStage, float] = {
        CompletePathStage.SEED: round(seed_seconds, 6),
        CompletePathStage.ENCODING_COMPRESSION: round(encoding_seconds, 6),
        CompletePathStage.TRANSPORT_STAGING: round(transport_seconds, 6),
        CompletePathStage.INGESTION: round(ingestion_seconds, 6),
        CompletePathStage.INDEXES_CONSTRAINTS: round(indexes_seconds, 6),
        CompletePathStage.CDC_DRAIN: round(cdc_drain_seconds, 6),
        CompletePathStage.VERIFICATION: round(verification_seconds, 6),
        CompletePathStage.REPAIR: round(repair_seconds, 6),
        CompletePathStage.CUTOVER: round(cutover_seconds, 6),
        CompletePathStage.ROLLBACK: round(rollback_seconds, 6),
    }

    bottlenecks = {
        "disk_read": seed_seconds + verification_seconds * 0.5,
        "cpu": encoding_seconds + verification_seconds * 0.5,
        "network": transport_seconds,
        "disk_write": ingestion_seconds + indexes_seconds + cdc_drain_seconds + repair_seconds,
    }
    binding_resource = max(bottlenecks.items(), key=lambda kv: kv[1])[0]

    return StageCostBreakdown(
        stage_seconds=stage_map,
        copy_only_seconds=round(copy_only_seconds, 6),
        downstream_tail_seconds=round(downstream_tail_seconds, 6),
        total_verified_seconds=round(total_verified_seconds, 6),
        accumulated_cdc_backlog_bytes=round(accumulated_cdc_bytes, 3),
        binding_resource=binding_resource,
    )


def compute_full_path_lower_bound(
    *,
    tables: Sequence[TableWorkItem],
    work_units: Sequence[PlannedWorkUnit],
    effective_quotas: ResourceQuotaSpec,
    facts_map: Mapping[str, Any],
    planned_verified_seconds: float,
) -> LowerBoundCertificate:
    """Compute a provably valid lower bound on verified completion across all 10 stages.

    Never claims a global optimum; provides an explicit lower bound certificate from:
    1. Resource bottleneck bound (minimum mandatory read, write, network, and CPU work).
    2. Critical-chain single-unit serial bound (largest post-split unit + mandatory tail).
    3. Minimum CDC drain + cutover + rollback serial floor.
    """
    total_bytes = float(
        sum(t.total_bytes for t in tables)
        if tables
        else float(facts_map.get("dataset_bytes", 0.0))
    )
    total_indexes = sum(t.secondary_index_count for t in tables) if tables else int(
        facts_map.get("secondary_index_count", 4)
    )
    total_constraints = sum(t.constraint_count for t in tables) if tables else int(
        facts_map.get("constraint_count", 2)
    )
    best_comp_ratio = min(
        1.0,
        float(facts_map.get("zstd_compression_ratio", 0.32)),
        float(facts_map.get("lz4_compression_ratio", 0.52)),
    )

    # 1. Resource bottleneck lower bound
    min_read_sec = total_bytes / max(1.0, effective_quotas.disk_read_bytes_per_sec)
    min_net_sec = (total_bytes * best_comp_ratio) / max(
        1.0, effective_quotas.network_bytes_per_sec
    )
    min_index_bytes = total_bytes * (
        0.22 * float(min(16, total_indexes)) + 0.08 * float(min(16, total_constraints))
    ) * 0.45
    min_write_sec = (total_bytes + min_index_bytes) / max(
        1.0, effective_quotas.disk_write_bytes_per_sec
    )
    resource_lb = max(min_read_sec, min_net_sec, min_write_sec)

    # 2. Critical-chain lower bound on largest single irreducible unit + cutover/rollback
    max_unit_bytes = float(
        max((u.total_bytes for u in work_units), default=int(total_bytes))
    )
    cutover_sec = float(facts_map.get("measured_cutover_fence_seconds", 2.0))
    rollback_sec = float(facts_map.get("measured_rollback_readiness_seconds", 1.0))
    unit_chain_sec = (
        max_unit_bytes / max(1.0, effective_quotas.disk_read_bytes_per_sec)
        + (max_unit_bytes * best_comp_ratio)
        / max(1.0, effective_quotas.network_bytes_per_sec)
        + max_unit_bytes / max(1.0, effective_quotas.disk_write_bytes_per_sec)
        + cutover_sec
        + rollback_sec
    )

    # 3. Minimum CDC drain + cutover + rollback bound
    initial_cdc = float(facts_map.get("initial_cdc_backlog_bytes", 0.0))
    cdc_arrival = float(facts_map.get("cdc_arrival_bytes_per_sec", 0.0))
    cdc_apply = min(
        effective_quotas.disk_write_bytes_per_sec,
        float(
            facts_map.get(
                "measured_cdc_apply_bytes_per_sec",
                effective_quotas.disk_write_bytes_per_sec * 0.6,
            )
        ),
    )
    min_copy_window = max(min_read_sec, min_net_sec, min_write_sec)
    min_cdc_bytes = initial_cdc + cdc_arrival * min_copy_window
    net_drain = max(1.0, cdc_apply - cdc_arrival)
    cdc_cutover_lb = min_copy_window + (min_cdc_bytes / net_drain) + cutover_sec + rollback_sec

    terms = {
        "resource_bottleneck": resource_lb,
        "critical_chain": unit_chain_sec,
        "cdc_cutover_floor": cdc_cutover_lb,
    }
    dominant_name, raw_lb = max(terms.items(), key=lambda kv: kv[1])
    valid_lb = min(planned_verified_seconds, max(0.0, raw_lb))
    abs_gap = max(0.0, planned_verified_seconds - valid_lb)
    rel_gap = abs_gap / valid_lb if valid_lb > 0.0 else 0.0

    return LowerBoundCertificate(
        lower_bound_seconds=round(valid_lb, 6),
        resource_bottleneck_lb_seconds=round(resource_lb, 6),
        critical_chain_lb_seconds=round(unit_chain_sec, 6),
        cdc_cutover_lb_seconds=round(cdc_cutover_lb, 6),
        planned_verified_seconds=round(planned_verified_seconds, 6),
        absolute_gap_seconds=round(abs_gap, 6),
        relative_gap_ratio=round(rel_gap, 6),
        dominant_lower_bound_term=dominant_name,
        claims_global_optimum=False,
    )


# -----------------------------------------------------------------------------
# Joint Complete-Path Optimizer (`optimize_complete_path`)
# -----------------------------------------------------------------------------


def optimize_complete_path(
    facts_input: Mapping[str, Any],
    *,
    tables: Sequence[TableWorkItem] = (),
    quotas: Optional[ResourceQuotaSpec] = None,
    contention: Optional[HopContentionSpec] = None,
    backpressure: Optional[BackpressureTelemetry] = None,
    budget: Optional[OptimizerBudget] = None,
    work: str = "complete path verified optimization",
) -> CompletePathPlan:
    """Jointly optimize verified completion across all 10 stages via `decide.climb`.

    1. Validates safe endpoints and computes effective quotas under multi-DB/hop contention.
    2. Constructs the safe baseline plan first so any interruption retains a valid plan.
    3. Climbs `optimizer_search_rungs`, `skew_partition_rungs`, `encoding_transport_rungs`,
       and `complete_path_rungs` using measured end-to-end verified completion costs.
    4. Bounds CPU/memory/wall time and computes a valid lower-bound distance certificate.
    """
    t0_wall = time.perf_counter()
    t0_cpu = time.process_time()

    raw = dict(facts_input)
    if "source_host" in raw:
        validate_safe_endpoint(str(raw["source_host"]))
    if "target_host" in raw:
        validate_safe_endpoint(str(raw["target_host"]))

    base_quotas = quotas or ResourceQuotaSpec(
        cpu_cores_available=float(raw.get("cpu_cores_available", 8.0)),
        disk_read_bytes_per_sec=float(raw.get("disk_read_bytes_per_sec", 500_000_000.0)),
        disk_write_bytes_per_sec=float(raw.get("disk_write_bytes_per_sec", 400_000_000.0)),
        network_bytes_per_sec=float(raw.get("network_bytes_per_sec", 250_000_000.0)),
        service_requests_per_sec=float(raw.get("service_requests_per_sec", 2000.0)),
        staging_quota_bytes=float(raw.get("staging_quota_bytes", 100_000_000_000.0)),
        memory_quota_bytes=int(raw.get("memory_quota_bytes", 256 * 1024 * 1024)),
    )
    hop_contention = contention or HopContentionSpec(
        source_concurrent_databases=int(raw.get("source_concurrent_databases", 1)),
        target_concurrent_databases=int(raw.get("target_concurrent_databases", 1)),
        shared_network_hops=int(raw.get("shared_network_hops", 1)),
        source_cpu_share_limit=float(raw.get("source_cpu_share_limit", 1.0)),
        target_write_share_limit=float(raw.get("target_write_share_limit", 1.0)),
        inter_hop_relay_overhead_ratio=float(raw.get("inter_hop_relay_overhead_ratio", 0.0)),
    )
    eff_quotas = hop_contention.effective_quotas(base_quotas)

    opt_budget = budget or OptimizerBudget(
        max_wall_seconds=float(raw.get("optimizer_max_wall_seconds", 2.0)),
        max_cpu_seconds=float(raw.get("optimizer_max_cpu_seconds", 2.0)),
        max_memory_bytes=int(raw.get("optimizer_max_memory_bytes", eff_quotas.memory_quota_bytes)),
        max_iterations=int(raw.get("optimizer_max_iterations", 10_000)),
        exact_max_tables=int(raw.get("exact_max_tables", 250)),
    )

    if backpressure is not None:
        raw.setdefault("backpressure_stall_ratio", backpressure.stall_ratio)
        recommended_batch = adjust_aimd_batch_size(backpressure)
    else:
        recommended_batch = int(raw.get("default_batch_rows", 5000))

    table_count = len(tables) if tables else int(raw.get("table_count", 1))
    max_skew = max(
        (t.skew_ratio for t in tables),
        default=float(raw.get("max_to_median_skew_ratio", 1.0)),
    )

    # Step 1: Always build the safe baseline first before search can be interrupted
    safe_units = (
        plan_skew_aware_work_units(
            tables,
            skew_mode=SkewHandlingMode.UNIFORM_TABLE_SCHEDULE,
            target_chunk_bytes=int(raw.get("target_chunk_bytes", 64 * 1024 * 1024)),
            coalesce_bundle_max_bytes=int(raw.get("coalesce_bundle_max_bytes", 16 * 1024 * 1024)),
            coalesce_bundle_max_tables=int(raw.get("coalesce_bundle_max_tables", 32)),
        )
        if tables
        else ()
    )
    safe_baseline_breakdown = evaluate_strategy_stage_costs(
        CompletePathStrategy.SAFE_BASELINE_CHUNKED_SERIAL_PATH,
        tables=tables,
        work_units=safe_units,
        effective_quotas=eff_quotas,
        encoding_mode=EncodingTransportMode.LZ4_FRAMED_STAGED_SPOOL,
        facts_map=raw,
    )

    # Step 2: Check optimizer budget / interruption state and climb `optimizer_search_rungs`
    elapsed_wall = time.perf_counter() - t0_wall
    elapsed_cpu = time.process_time() - t0_cpu
    interrupted_by_cb = bool(opt_budget.interrupt_check and opt_budget.interrupt_check())
    interrupted_by_flag = bool(raw.get("search_interrupted", False))
    est_exact_bytes = max(256, table_count * 4096)
    est_bucket_bytes = max(256, min(table_count, 64) * 2048)
    rem_wall = max(0.0, opt_budget.max_wall_seconds - elapsed_wall)
    rem_cpu = max(0.0, opt_budget.max_cpu_seconds - elapsed_cpu)
    rem_budget_sec = min(rem_wall, rem_cpu)

    is_interrupted = (
        interrupted_by_cb
        or interrupted_by_flag
        or rem_budget_sec <= 0.0
        or est_bucket_bytes > opt_budget.max_memory_bytes
    )

    search_facts_dict = dict(raw)
    search_facts_dict.update(
        {
            "table_count": table_count,
            "exact_max_tables": opt_budget.exact_max_tables,
            "estimated_state_bytes": int(
                raw.get("estimated_state_bytes", est_exact_bytes)
            ),
            "estimated_bucket_memory_bytes": int(
                raw.get("estimated_bucket_memory_bytes", est_bucket_bytes)
            ),
            "max_memory_bytes": opt_budget.max_memory_bytes,
            "search_interrupted": is_interrupted,
            "remaining_time_budget_sec": float(
                raw.get("remaining_time_budget_sec", rem_budget_sec)
            ),
            "safe_baseline_initialized": bool(
                raw.get("safe_baseline_initialized", True)
            ),
        }
    )
    search_choice = decide.climb(
        "optimizer search regime",
        optimizer_search_rungs(),
        decide.Facts(search_facts_dict),
    )
    search_mode = (
        OptimizerSearchMode(search_choice.path)
        if search_choice.rung is not None
        else OptimizerSearchMode.RETAIN_SAFE_BASELINE_ON_BUDGET_EXHAUSTION
    )

    # Step 3: Climb `skew_partition_rungs`
    skew_facts_dict = dict(raw)
    skew_facts_dict.update(
        {
            "table_count": table_count,
            "max_to_median_skew_ratio": max_skew,
            "range_split_supported": bool(
                raw.get(
                    "range_split_supported",
                    all(t.has_splittable_key for t in tables) if tables else True,
                )
            ),
            "coalesce_supported": bool(raw.get("coalesce_supported", True)),
            "uniform_schedule_allowed": bool(raw.get("uniform_schedule_allowed", True)),
        }
    )
    skew_choice = decide.climb(
        "skew and partition handling",
        skew_partition_rungs(),
        decide.Facts(skew_facts_dict),
    )
    skew_mode = (
        SkewHandlingMode(skew_choice.path)
        if skew_choice.rung is not None
        else SkewHandlingMode.UNIFORM_TABLE_SCHEDULE
    )

    planned_units = (
        plan_skew_aware_work_units(
            tables,
            skew_mode=skew_mode,
            target_chunk_bytes=int(raw.get("target_chunk_bytes", 64 * 1024 * 1024)),
            coalesce_bundle_max_bytes=int(raw.get("coalesce_bundle_max_bytes", 16 * 1024 * 1024)),
            coalesce_bundle_max_tables=int(raw.get("coalesce_bundle_max_tables", 32)),
        )
        if tables
        else ()
    )

    # Step 4: Climb `encoding_transport_rungs`
    total_bytes = float(
        sum(t.total_bytes for t in tables)
        if tables
        else float(raw.get("dataset_bytes", 100_000_000.0))
    )
    enc_facts_dict = dict(raw)
    enc_facts_dict.setdefault("staging_quota_bytes", eff_quotas.staging_quota_bytes)
    enc_facts_dict.setdefault("required_spool_bytes", min(total_bytes * 0.25, 10_000_000_000.0))
    enc_facts_dict.setdefault("network_quota_bytes_per_sec", eff_quotas.network_bytes_per_sec)
    enc_facts_dict.setdefault(
        "raw_stream_required_bytes_per_sec",
        min(eff_quotas.disk_write_bytes_per_sec, eff_quotas.disk_read_bytes_per_sec),
    )
    enc_choice = decide.climb(
        "encoding and transport selection",
        encoding_transport_rungs(),
        decide.Facts(enc_facts_dict),
    )
    enc_mode = (
        EncodingTransportMode(enc_choice.path)
        if enc_choice.rung is not None
        else EncodingTransportMode.LZ4_FRAMED_STAGED_SPOOL
    )

    # Step 5: Evaluate candidate full-path strategies across all 10 stages
    # If search was interrupted or budget exhausted, retain the safe baseline immediately.
    if search_mode == OptimizerSearchMode.RETAIN_SAFE_BASELINE_ON_BUDGET_EXHAUSTION:
        baseline_only_facts = dict(raw)
        baseline_only_facts.update(
            {
                "direct_streaming_available": False,
                "staging_storage_available": False,
                "raw_copy_tail_competitive": False,
                "quotas_allow_double_scan": False,
                "safe_baseline_available": bool(
                    raw.get("safe_baseline_available", True)
                ),
            }
        )
        path_choice = decide.climb(
            work, complete_path_rungs(), decide.Facts(baseline_only_facts)
        )
        chosen_strat = (
            CompletePathStrategy(path_choice.path)
            if path_choice.rung is not None
            else None
        )
        lb_cert = compute_full_path_lower_bound(
            tables=tables,
            work_units=safe_units,
            effective_quotas=eff_quotas,
            facts_map=raw,
            planned_verified_seconds=safe_baseline_breakdown.total_verified_seconds,
        )
        reason = (
            "REVIEW: optimizer search interrupted or budget exceeded; retained validated safe baseline plan"
            if chosen_strat is not None
            else "ACTION: optimizer interrupted and safe baseline was refused"
        )
        return CompletePathPlan(
            path_choice=path_choice,
            encoding_transport_choice=enc_choice,
            skew_choice=skew_choice,
            search_choice=search_choice,
            chosen_strategy=chosen_strat,
            encoding_transport_mode=enc_mode,
            skew_handling_mode=skew_mode,
            search_mode=search_mode,
            stage_breakdown=safe_baseline_breakdown,
            candidate_breakdowns={
                CompletePathStrategy.SAFE_BASELINE_CHUNKED_SERIAL_PATH: safe_baseline_breakdown
            },
            work_units=safe_units,
            lower_bound=lb_cert,
            safe_baseline_breakdown=safe_baseline_breakdown,
            retained_safe_baseline=True,
            interrupted=True,
            interruption_reason=reason,
            optimizer_wall_seconds=round(time.perf_counter() - t0_wall, 6),
            optimizer_cpu_seconds=round(time.process_time() - t0_cpu, 6),
            optimizer_memory_bytes=est_bucket_bytes,
            effective_quotas=eff_quotas,
            recommended_batch_rows=recommended_batch,
            care_badge=CareBadge.REVIEW if chosen_strat is not None else CareBadge.ACTION,
        )

    # Otherwise, evaluate all 4 strategies jointly across the 10 stages.
    # For hierarchical bucketed mode on thousands of tables, evaluate using representative
    # aggregated stats (already O(1) after single O(N) pass over `tables` and `planned_units`).
    candidate_breakdowns: Dict[CompletePathStrategy, StageCostBreakdown] = {}
    costs = decide.Costs()
    for strat in CompletePathStrategy:
        bd = evaluate_strategy_stage_costs(
            strat,
            tables=tables,
            work_units=planned_units,
            effective_quotas=eff_quotas,
            encoding_mode=enc_mode,
            facts_map=raw,
        )
        candidate_breakdowns[strat] = bd
        costs.saw(strat.value, 1, bd.total_verified_seconds)

    inline_bd = candidate_breakdowns[
        CompletePathStrategy.DIRECT_STREAM_INLINE_VERIFY_DEFERRED_INDEX
    ]
    staged_bd = candidate_breakdowns[
        CompletePathStrategy.STAGED_OBJECT_BULK_LOAD_PARALLEL_PROOF
    ]
    raw_copy_bd = candidate_breakdowns[
        CompletePathStrategy.UNCOMPRESSED_FAST_COPY_POSTHOC_INDEX_AND_VERIFY
    ]

    path_facts_dict = dict(raw)
    path_facts_dict.setdefault(
        "full_path_verified_seconds_inline", inline_bd.total_verified_seconds
    )
    path_facts_dict.setdefault(
        "full_path_verified_seconds_raw_copy", raw_copy_bd.total_verified_seconds
    )
    path_facts_dict.setdefault(
        "deferred_index_inline_proof_viable",
        inline_bd.total_verified_seconds <= raw_copy_bd.total_verified_seconds,
    )
    path_facts_dict.setdefault(
        "raw_copy_tail_competitive",
        raw_copy_bd.total_verified_seconds < inline_bd.total_verified_seconds
        and raw_copy_bd.total_verified_seconds < staged_bd.total_verified_seconds,
    )
    path_facts_dict.setdefault(
        "quotas_allow_double_scan",
        eff_quotas.network_bytes_per_sec >= eff_quotas.disk_write_bytes_per_sec,
    )
    path_facts_dict.setdefault(
        "staging_storage_available",
        eff_quotas.staging_quota_bytes >= max(1.0, total_bytes * 0.35),
    )

    path_choice = decide.climb(
        work,
        complete_path_rungs(),
        decide.Facts(path_facts_dict),
        costs=costs,
    )

    if path_choice.rung is None:
        lb_cert = compute_full_path_lower_bound(
            tables=tables,
            work_units=planned_units,
            effective_quotas=eff_quotas,
            facts_map=raw,
            planned_verified_seconds=safe_baseline_breakdown.total_verified_seconds,
        )
        return CompletePathPlan(
            path_choice=path_choice,
            encoding_transport_choice=enc_choice,
            skew_choice=skew_choice,
            search_choice=search_choice,
            chosen_strategy=None,
            encoding_transport_mode=enc_mode,
            skew_handling_mode=skew_mode,
            search_mode=search_mode,
            stage_breakdown=safe_baseline_breakdown,
            candidate_breakdowns=candidate_breakdowns,
            work_units=planned_units,
            lower_bound=lb_cert,
            safe_baseline_breakdown=safe_baseline_breakdown,
            retained_safe_baseline=False,
            interrupted=False,
            interruption_reason="ACTION: all complete-path rungs were refused",
            optimizer_wall_seconds=round(time.perf_counter() - t0_wall, 6),
            optimizer_cpu_seconds=round(time.process_time() - t0_cpu, 6),
            optimizer_memory_bytes=est_bucket_bytes,
            effective_quotas=eff_quotas,
            recommended_batch_rows=recommended_batch,
            care_badge=CareBadge.ACTION,
        )

    chosen_strategy = CompletePathStrategy(path_choice.path)
    chosen_breakdown = candidate_breakdowns[chosen_strategy]
    lb_cert = compute_full_path_lower_bound(
        tables=tables,
        work_units=planned_units,
        effective_quotas=eff_quotas,
        facts_map=raw,
        planned_verified_seconds=chosen_breakdown.total_verified_seconds,
    )

    mem_used = (
        est_exact_bytes
        if search_mode == OptimizerSearchMode.EXACT_JOINT_BRANCH_AND_BOUND
        else est_bucket_bytes
    )
    badge = (
        CareBadge.PASS
        if chosen_strategy != CompletePathStrategy.SAFE_BASELINE_CHUNKED_SERIAL_PATH
        else CareBadge.BENIGN
    )

    return CompletePathPlan(
        path_choice=path_choice,
        encoding_transport_choice=enc_choice,
        skew_choice=skew_choice,
        search_choice=search_choice,
        chosen_strategy=chosen_strategy,
        encoding_transport_mode=enc_mode,
        skew_handling_mode=skew_mode,
        search_mode=search_mode,
        stage_breakdown=chosen_breakdown,
        candidate_breakdowns=candidate_breakdowns,
        work_units=planned_units,
        lower_bound=lb_cert,
        safe_baseline_breakdown=safe_baseline_breakdown,
        retained_safe_baseline=(
            chosen_strategy == CompletePathStrategy.SAFE_BASELINE_CHUNKED_SERIAL_PATH
        ),
        interrupted=False,
        interruption_reason=None,
        optimizer_wall_seconds=round(time.perf_counter() - t0_wall, 6),
        optimizer_cpu_seconds=round(time.process_time() - t0_cpu, 6),
        optimizer_memory_bytes=mem_used,
        effective_quotas=eff_quotas,
        recommended_batch_rows=recommended_batch,
        care_badge=badge,
    )


# -----------------------------------------------------------------------------
# O(1) Typed Durable Checkpoint Store & Active Lease Lock
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class CompletePathCheckpointUnit:
    """Durable typed checkpoint record for one `(unit_id, stage)` pair."""

    unit_id: str
    stage: CompletePathStage
    strategy: CompletePathStrategy
    min_cursor: Any
    max_cursor: Any
    merkle_digest: str
    completed_at: datetime.datetime


class CompletePathCheckpointStore:
    """Universal typed durable checkpoint store with $O(1)$ unit completion lookup."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = pathlib.Path(path)
        self._units: Dict[Tuple[str, str], CompletePathCheckpointUnit] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="ascii"))
        for entry in raw.get("units", ()):
            unit = CompletePathCheckpointUnit(
                unit_id=str(entry["unit_id"]),
                stage=CompletePathStage(entry["stage"]),
                strategy=CompletePathStrategy(entry["strategy"]),
                min_cursor=checkpoint.deserialize_cursor(entry["min_cursor"]),
                max_cursor=checkpoint.deserialize_cursor(entry["max_cursor"]),
                merkle_digest=str(entry["merkle_digest"]),
                completed_at=checkpoint.deserialize_cursor(entry["completed_at"]),
            )
            self._units[(unit.unit_id, unit.stage.value)] = unit

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "units": [
                {
                    "unit_id": u.unit_id,
                    "stage": u.stage.value,
                    "strategy": u.strategy.value,
                    "min_cursor": checkpoint.serialize_cursor(u.min_cursor),
                    "max_cursor": checkpoint.serialize_cursor(u.max_cursor),
                    "merkle_digest": u.merkle_digest,
                    "completed_at": checkpoint.serialize_cursor(u.completed_at),
                }
                for _, u in sorted(self._units.items(), key=lambda kv: kv[0])
            ]
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(payload, sort_keys=True, ensure_ascii=True), encoding="ascii"
        )
        tmp.replace(self.path)

    def record_unit(self, unit: CompletePathCheckpointUnit) -> None:
        self._units[(unit.unit_id, unit.stage.value)] = unit
        self._save()

    def is_completed_o1(self, unit_id: str, stage: CompletePathStage) -> bool:
        return (unit_id, stage.value) in self._units

    def get_unit(
        self, unit_id: str, stage: CompletePathStage
    ) -> Optional[CompletePathCheckpointUnit]:
        return self._units.get((unit_id, stage.value))


def acquire_complete_path_lease(
    lock_path: pathlib.Path,
    *,
    what: str = "complete-path optimization and execution",
    term_seconds: float = 60.0,
) -> lease.Lease:
    """Acquire an exclusive `.run.lock` lease with heartbeat via `migkit.lease.Lease`."""
    return lease.Lease(pathlib.Path(lock_path), what=what, term=term_seconds).acquire()
