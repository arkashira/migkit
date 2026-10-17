"""Replan remaining work with priced state transitions (Row 588: SMART-N5.live-replan).

Safe checkpoint replanning of the remaining phase/table/range DAG on changing
throughput, CDC arrivals, load, capacity, or quota using `decide.Rung` and
`decide.climb(work, rungs, facts)`.

Includes setup/cleanup, cache warming, snapshot age, reconnect, undo/replay, and
validation costs. Switches strategy or topology only if conservative remaining-work
savings exceed total transition cost plus hysteresis. Preserves committed checkpoints
with O(1) skip. Damps transient fluctuations to prevent thrashing under oscillating load,
and delivers measured end-to-end improvement under sustained shifts.

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
    Set,
    Tuple,
)

from . import checkpoint, decide, lease
from .sizing import AIMDChunkSizer, aimd_adjust_chunk, aimd_chunk_size

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class ReplanStrategy(str, enum.Enum):
    DIRECT_STREAM = "direct_stream"
    STAGED_BULK_LOAD = "staged_bulk_load"
    PARALLEL_RANGE_SPLIT = "parallel_range_split"
    RETAIN_CURRENT_PLAN = "retain_current_plan"


class DAGPhase(str, enum.Enum):
    SCHEMA = "schema"
    SEED_COPY = "seed_copy"
    CDC_CATCHUP = "cdc_catchup"
    VERIFY = "verify"
    CUTOVER = "cutover"


class DAGNodeStatus(str, enum.Enum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    COMMITTED = "committed"


class LiveReplanError(Exception):
    """Base exception for live replan operations."""


class UnsafeEndpointError(LiveReplanError):
    """Raised when an endpoint host is unsafe or contains embedded secrets."""


class InvalidReplanFactError(LiveReplanError):
    """Raised when required telemetry, quota, or DAG facts are missing or invalid."""


class CheckpointCorruptionError(LiveReplanError):
    """Raised when checkpoint state is corrupted or fails integrity check."""


_SAFE_HOST_RE = re.compile(
    r"^(127\.\d+\.\d+\.\d+|10\.\d+\.\d+\.\d+|localhost|::1|[a-zA-Z0-9_-]+\.internal|[a-zA-Z0-9_-]+\.test|[a-zA-Z0-9_-]+\.local)$"
)


def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host is a safe internal/test address without secrets."""
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
# Domain Models: Quotas, Telemetry, Transition Costs, DAG
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class ResourceQuotaSpec:
    """Measured resource and service quotas governing live replan."""

    cpu_cores_available: float
    disk_read_bytes_per_sec: float
    disk_write_bytes_per_sec: float
    network_bytes_per_sec: float
    staging_quota_bytes: float
    max_worker_connections: int
    memory_quota_bytes: int

    def __post_init__(self) -> None:
        if self.cpu_cores_available <= 0.0:
            raise InvalidReplanFactError("cpu_cores_available must be > 0")
        if self.disk_read_bytes_per_sec <= 0.0 or self.disk_write_bytes_per_sec <= 0.0:
            raise InvalidReplanFactError("disk read/write bytes_per_sec must be > 0")
        if self.network_bytes_per_sec <= 0.0:
            raise InvalidReplanFactError("network_bytes_per_sec must be > 0")
        if self.staging_quota_bytes < 0.0:
            raise InvalidReplanFactError("staging_quota_bytes must be >= 0")
        if self.max_worker_connections <= 0:
            raise InvalidReplanFactError("max_worker_connections must be > 0")
        if self.memory_quota_bytes <= 0:
            raise InvalidReplanFactError("memory_quota_bytes must be > 0")


@dataclass(frozen=True)
class TelemetryObservation:
    """Point-in-time measured telemetry reading."""

    timestamp: float
    observed_throughput_bytes_per_sec: float
    cdc_arrival_rate_bytes_per_sec: float
    cdc_drain_rate_bytes_per_sec: float
    cdc_backlog_bytes: float
    cpu_utilization_ratio: float
    memory_utilization_ratio: float
    active_connections: int

    def __post_init__(self) -> None:
        if self.timestamp < 0.0:
            raise InvalidReplanFactError("timestamp must be >= 0")
        if self.observed_throughput_bytes_per_sec < 0.0:
            raise InvalidReplanFactError("observed_throughput_bytes_per_sec must be >= 0")
        if self.cdc_arrival_rate_bytes_per_sec < 0.0 or self.cdc_drain_rate_bytes_per_sec < 0.0:
            raise InvalidReplanFactError("cdc arrival/drain rates must be >= 0")
        if self.cdc_backlog_bytes < 0.0:
            raise InvalidReplanFactError("cdc_backlog_bytes must be >= 0")
        if not (0.0 <= self.cpu_utilization_ratio <= 1.0):
            raise InvalidReplanFactError("cpu_utilization_ratio must be in [0, 1]")
        if not (0.0 <= self.memory_utilization_ratio <= 1.0):
            raise InvalidReplanFactError("memory_utilization_ratio must be in [0, 1]")
        if self.active_connections < 0:
            raise InvalidReplanFactError("active_connections must be >= 0")


@dataclass(frozen=True)
class SmoothedTelemetry:
    """Smoothed telemetry across observations to detect trends and filter oscillation."""

    mean_throughput_bytes_per_sec: float
    smoothed_throughput_bytes_per_sec: float
    mean_cdc_arrival_rate: float
    mean_cdc_drain_rate: float
    current_cdc_backlog_bytes: float
    mean_cpu_ratio: float
    mean_memory_ratio: float
    throughput_variance: float
    is_oscillating: bool
    observation_count: int
    duration_seconds: float


def smooth_telemetry_series(
    observations: Sequence[TelemetryObservation],
    *,
    oscillation_variance_ratio_threshold: float = 0.25,
    ema_alpha: float = 0.35,
) -> SmoothedTelemetry:
    """Compute moving average, exponential smoothing, and oscillation metric."""
    if not observations:
        raise InvalidReplanFactError("observations sequence must not be empty")

    n = len(observations)
    t_throughputs = [o.observed_throughput_bytes_per_sec for o in observations]
    mean_tp = sum(t_throughputs) / float(n)

    if n > 1:
        variance = sum((x - mean_tp) ** 2 for x in t_throughputs) / float(n - 1)
        std_dev = math.sqrt(variance)
        rel_variance = std_dev / max(1.0, mean_tp)
    else:
        variance = 0.0
        rel_variance = 0.0

    # EMA calculation
    ema = observations[0].observed_throughput_bytes_per_sec
    for obs in observations[1:]:
        ema = ema_alpha * obs.observed_throughput_bytes_per_sec + (1.0 - ema_alpha) * ema

    mean_cdc_arr = sum(o.cdc_arrival_rate_bytes_per_sec for o in observations) / float(n)
    mean_cdc_drn = sum(o.cdc_drain_rate_bytes_per_sec for o in observations) / float(n)
    mean_cpu = sum(o.cpu_utilization_ratio for o in observations) / float(n)
    mean_mem = sum(o.memory_utilization_ratio for o in observations) / float(n)
    duration = max(0.0, observations[-1].timestamp - observations[0].timestamp)

    is_osc = (n >= 3) and (rel_variance >= oscillation_variance_ratio_threshold)

    return SmoothedTelemetry(
        mean_throughput_bytes_per_sec=mean_tp,
        smoothed_throughput_bytes_per_sec=ema,
        mean_cdc_arrival_rate=mean_cdc_arr,
        mean_cdc_drain_rate=mean_cdc_drn,
        current_cdc_backlog_bytes=observations[-1].cdc_backlog_bytes,
        mean_cpu_ratio=mean_cpu,
        mean_memory_ratio=mean_mem,
        throughput_variance=variance,
        is_oscillating=is_osc,
        observation_count=n,
        duration_seconds=duration,
    )


@dataclass(frozen=True)
class TransitionCostBreakdown:
    """Exact priced costs of switching migration state and strategy."""

    setup_cleanup_sec: float
    cache_warming_sec: float
    snapshot_age_sec: float
    reconnect_sec: float
    undo_replay_sec: float
    validation_sec: float

    def __post_init__(self) -> None:
        for f_name in (
            "setup_cleanup_sec",
            "cache_warming_sec",
            "snapshot_age_sec",
            "reconnect_sec",
            "undo_replay_sec",
            "validation_sec",
        ):
            if getattr(self, f_name) < 0.0:
                raise InvalidReplanFactError(f"{f_name} must be >= 0")

    @property
    def total_transition_cost_sec(self) -> float:
        return (
            self.setup_cleanup_sec
            + self.cache_warming_sec
            + self.snapshot_age_sec
            + self.reconnect_sec
            + self.undo_replay_sec
            + self.validation_sec
        )


@dataclass(frozen=True)
class PricedTransitionPolicy:
    """Hysteresis, damping, and safety limits for priced replanning."""

    hysteresis_margin_sec: float = 8.0
    hysteresis_ratio: float = 0.15
    cooldown_seconds: float = 20.0
    max_snapshot_age_sec: float = 14400.0
    max_allowed_undo_ratio: float = 0.45
    min_observation_window_sec: float = 10.0
    degradation_threshold: float = 0.25

    def __post_init__(self) -> None:
        if self.hysteresis_margin_sec < 0.0:
            raise InvalidReplanFactError("hysteresis_margin_sec must be >= 0")
        if not (0.0 <= self.hysteresis_ratio <= 1.0):
            raise InvalidReplanFactError("hysteresis_ratio must be in [0, 1]")
        if self.cooldown_seconds < 0.0:
            raise InvalidReplanFactError("cooldown_seconds must be >= 0")
        if self.max_snapshot_age_sec <= 0.0:
            raise InvalidReplanFactError("max_snapshot_age_sec must be > 0")


def calculate_transition_cost(
    current_strategy: ReplanStrategy,
    candidate_strategy: ReplanStrategy,
    *,
    in_flight_uncommitted_bytes: int,
    in_flight_uncommitted_rows: int,
    source_buffer_pool_bytes: int,
    target_buffer_pool_bytes: int,
    snapshot_age_seconds: float,
    active_connections: int,
    quotas: ResourceQuotaSpec,
    facts_map: Mapping[str, Any],
) -> TransitionCostBreakdown:
    """Derive concrete priced state transition cost components from measured parameters."""
    if in_flight_uncommitted_bytes < 0 or in_flight_uncommitted_rows < 0:
        raise InvalidReplanFactError("in-flight work must be >= 0")
    if source_buffer_pool_bytes < 0 or target_buffer_pool_bytes < 0:
        raise InvalidReplanFactError("buffer pool bytes must be >= 0")
    if snapshot_age_seconds < 0.0:
        raise InvalidReplanFactError("snapshot_age_seconds must be >= 0")
    if active_connections < 0:
        raise InvalidReplanFactError("active_connections must be >= 0")

    if current_strategy == candidate_strategy:
        return TransitionCostBreakdown(
            setup_cleanup_sec=0.0,
            cache_warming_sec=0.0,
            snapshot_age_sec=0.0,
            reconnect_sec=0.0,
            undo_replay_sec=0.0,
            validation_sec=0.0,
        )

    # 1. Setup / Cleanup cost
    base_setup = float(facts_map.get("base_setup_cleanup_sec", 1.5))
    if candidate_strategy == ReplanStrategy.STAGED_BULK_LOAD:
        # Requires allocating staging space, pipe structures, and bulk load descriptors
        staging_alloc_sec = max(
            0.5,
            float(in_flight_uncommitted_bytes) / max(1.0, quotas.disk_write_bytes_per_sec * 0.5),
        )
        setup_cleanup = base_setup + staging_alloc_sec
    elif candidate_strategy == ReplanStrategy.PARALLEL_RANGE_SPLIT:
        # Requires partitioning key spaces and worker queue setup
        worker_setup_sec = float(active_connections) * 0.15
        setup_cleanup = base_setup + worker_setup_sec
    else:
        # Tearing down staging or parallel pools
        setup_cleanup = base_setup

    # 2. Cache warming cost
    # When switching strategies, caches on source/target suffer cold penalties
    cache_miss_rate = float(facts_map.get("cache_miss_ratio_on_switch", 0.08))
    cold_bytes = float(source_buffer_pool_bytes + target_buffer_pool_bytes) * cache_miss_rate
    warming_rate = max(1.0, min(quotas.disk_read_bytes_per_sec, quotas.disk_write_bytes_per_sec))
    cache_warming = cold_bytes / warming_rate

    # 3. Snapshot age cost
    # Older snapshots carry WAL retention risk and vacuum delay penalty
    wal_overhead_factor = float(facts_map.get("wal_retention_penalty_factor", 0.0002))
    snapshot_age_cost = snapshot_age_seconds * wal_overhead_factor

    # 4. Reconnect cost
    # Closing sessions, TCP/TLS renegotiation, auth handshake, search_path init
    reconnect_time_per_conn = float(facts_map.get("reconnect_sec_per_connection", 0.12))
    reconnect_cost = float(active_connections) * reconnect_time_per_conn

    # 5. Undo / Replay cost
    # Uncommitted in-flight progress must be cleanly rolled back to the last committed checkpoint
    undo_rate = max(1.0, quotas.disk_write_bytes_per_sec * 0.75)
    undo_rollback_sec = float(in_flight_uncommitted_bytes) / undo_rate
    # Replay penalty for any CDC delta accumulated while uncommitted
    cdc_rate = float(facts_map.get("cdc_arrival_rate", 0.0))
    cdc_replay_sec = (cdc_rate * undo_rollback_sec) / max(1.0, quotas.network_bytes_per_sec)
    undo_replay = undo_rollback_sec + cdc_replay_sec

    # 6. Validation cost
    # Boundary checksum and checkpoint consistency check post-reconfiguration
    validation_rate = max(1.0, min(quotas.disk_read_bytes_per_sec, quotas.network_bytes_per_sec))
    validation_bytes = float(facts_map.get("validation_boundary_bytes", 500_000.0))
    validation_cost = validation_bytes / validation_rate

    return TransitionCostBreakdown(
        setup_cleanup_sec=round(setup_cleanup, 6),
        cache_warming_sec=round(cache_warming, 6),
        snapshot_age_sec=round(snapshot_age_cost, 6),
        reconnect_sec=round(reconnect_cost, 6),
        undo_replay_sec=round(undo_replay, 6),
        validation_sec=round(validation_cost, 6),
    )


# -----------------------------------------------------------------------------
# Phase / Table / Range DAG Models
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class DAGNode:
    """One scheduled unit of migration work in the phase/table/range DAG."""

    node_id: str
    phase: DAGPhase
    table_name: str
    range_id: str
    min_cursor: Any
    max_cursor: Any
    estimated_bytes: int
    estimated_rows: int
    dependencies: Tuple[str, ...] = ()
    status: DAGNodeStatus = DAGNodeStatus.PENDING
    is_skewed: bool = False
    cdc_arrival_rate: float = 0.0

    def __post_init__(self) -> None:
        if not self.node_id:
            raise InvalidReplanFactError("node_id must not be empty")
        if not self.table_name:
            raise InvalidReplanFactError("table_name must not be empty")
        if self.estimated_bytes < 0 or self.estimated_rows < 0:
            raise InvalidReplanFactError("estimated bytes and rows must be >= 0")
        if self.cdc_arrival_rate < 0.0:
            raise InvalidReplanFactError("cdc_arrival_rate must be >= 0")


class LiveReplanDAG:
    """DAG of migration work units with O(1) committed checkpoint lookup and reschedule."""

    def __init__(self, nodes: Iterable[DAGNode]) -> None:
        self._nodes: Dict[str, DAGNode] = {n.node_id: n for n in nodes}
        self._validate_acyclic()

    def _validate_acyclic(self) -> None:
        visited: Dict[str, int] = {}  # 0: visiting, 1: visited

        def visit(nid: str) -> None:
            state = visited.get(nid)
            if state == 0:
                raise InvalidReplanFactError(f"Cycle detected in DAG at node {nid}")
            if state == 1:
                return
            visited[nid] = 0
            for dep in self._nodes[nid].dependencies:
                if dep in self._nodes:
                    visit(dep)
            visited[nid] = 1

        for nid in self._nodes:
            if nid not in visited:
                visit(nid)

    @property
    def all_nodes(self) -> Tuple[DAGNode, ...]:
        return tuple(self._nodes.values())

    def get_node(self, node_id: str) -> Optional[DAGNode]:
        return self._nodes.get(node_id)

    def mark_committed(self, node_id: str) -> None:
        """Mark a node as committed (O(1))."""
        node = self._nodes.get(node_id)
        if node is not None:
            self._nodes[node_id] = dataclasses.replace(
                node, status=DAGNodeStatus.COMMITTED
            )

    def mark_in_flight(self, node_id: str) -> None:
        node = self._nodes.get(node_id)
        if node is not None:
            self._nodes[node_id] = dataclasses.replace(
                node, status=DAGNodeStatus.IN_FLIGHT
            )

    def get_committed_nodes(self) -> Tuple[DAGNode, ...]:
        return tuple(n for n in self._nodes.values() if n.status == DAGNodeStatus.COMMITTED)

    def get_in_flight_nodes(self) -> Tuple[DAGNode, ...]:
        return tuple(n for n in self._nodes.values() if n.status == DAGNodeStatus.IN_FLIGHT)

    def get_remaining_nodes(self) -> Tuple[DAGNode, ...]:
        """Return non-committed nodes in topological order."""
        visited: Set[str] = set()
        ordered: List[DAGNode] = []

        def visit(n: DAGNode) -> None:
            if n.node_id in visited:
                return
            for dep_id in n.dependencies:
                dep_node = self._nodes.get(dep_id)
                if dep_node is not None:
                    visit(dep_node)
            visited.add(n.node_id)
            if n.status != DAGNodeStatus.COMMITTED:
                ordered.append(n)

        for n in self._nodes.values():
            visit(n)
        return tuple(ordered)

    def remaining_bytes(self) -> int:
        return sum(n.estimated_bytes for n in self.get_remaining_nodes())

    def remaining_rows(self) -> int:
        return sum(n.estimated_rows for n in self.get_remaining_nodes())


# -----------------------------------------------------------------------------
# Decide Ladders: Trigger, Strategy, Transition Cost, DAG Reschedule
# -----------------------------------------------------------------------------


def replan_trigger_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for evaluating whether to initiate replanning."""
    return (
        decide.Rung(
            "transient_oscillation_damping",
            "suppress replan because oscillating variance or cooldown is active",
            needs=(
                decide.Need(
                    "oscillation detected or replan cooldown active",
                    lambda f: bool(f.get("is_oscillating", False))
                    or float(f.get("seconds_since_last_switch", 999.0))
                    < float(f.get("cooldown_seconds", 20.0)),
                    "{rung} is unneeded when load variance is stable and cooldown expired",
                ),
                decide.Need(
                    "damping preserves execution stability against thrashing",
                    lambda f: bool(f.get("damping_preserves_stability", True)),
                    "{rung} refused because damping does not preserve stability",
                ),
            ),
            because="prevents wasteful strategy thrashing and connection churn under oscillating load",
            footprint=0,
        ),
        decide.Rung(
            "sustained_throughput_degradation",
            "trigger replan due to sustained throughput degradation beyond observation threshold",
            needs=(
                decide.Need(
                    "measured throughput dropped below degradation threshold",
                    lambda f: float(f.get("current_throughput", 1.0))
                    < float(f.get("baseline_throughput", 1.0))
                    * (1.0 - float(f.get("degradation_threshold", 0.25))),
                    "{rung} refused because throughput is within normal variance of baseline",
                ),
                decide.Need(
                    "throughput degradation is sustained beyond observation window",
                    lambda f: float(f.get("degradation_duration_sec", 0.0))
                    >= float(f.get("min_observation_window_sec", 10.0)),
                    "{rung} refused because degradation was transient or not sustained",
                ),
                decide.Need(
                    "replan cooldown has expired",
                    lambda f: float(f.get("seconds_since_last_switch", 999.0))
                    >= float(f.get("cooldown_seconds", 20.0)),
                    "{rung} refused because recent switch cooldown has not expired",
                ),
            ),
            because="responds to genuine long-term bandwidth, CPU, or lock bottlenecks",
            footprint=1,
        ),
        decide.Rung(
            "cdc_backlog_divergence",
            "trigger replan because CDC arrival rate exceeds drain capacity and backlog expands",
            needs=(
                decide.Need(
                    "CDC arrival rate exceeds drain rate",
                    lambda f: float(f.get("cdc_arrival_rate", 0.0))
                    > float(f.get("cdc_drain_rate", 1.0)),
                    "{rung} refused because CDC drain rate exceeds arrivals",
                ),
                decide.Need(
                    "CDC backlog bytes exceed critical safety threshold",
                    lambda f: float(f.get("cdc_backlog_bytes", 0.0))
                    >= float(f.get("cdc_backlog_threshold_bytes", 10_000_000.0)),
                    "{rung} refused because CDC backlog is within safe bounds",
                ),
            ),
            because="prevents unbounded replication lag from jeopardizing cutover deadline",
            footprint=1,
        ),
        decide.Rung(
            "resource_quota_exhaustion",
            "trigger replan because operating load exceeds resource quota ceiling",
            needs=(
                decide.Need(
                    "resource load ratio exceeds quota ceiling",
                    lambda f: float(f.get("resource_load_ratio", 0.0))
                    >= float(f.get("quota_ceiling_ratio", 0.85)),
                    "{rung} refused because resource headroom is within quota limits",
                ),
                decide.Need(
                    "alternate pathway with lower resource footprint is viable",
                    lambda f: bool(f.get("alternate_pathway_available", True)),
                    "{rung} refused because no lower-footprint alternative exists",
                ),
            ),
            because="sheds load before hitting hard kernel or cloud service rate limits",
            footprint=1,
        ),
    )


def replan_strategy_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for selecting the candidate remaining-work strategy."""
    return (
        decide.Rung(
            "switch_to_staged_bulk_load",
            "switch remaining work to compressed staged spool with target bulk loader",
            needs=(
                decide.Need(
                    "staging storage quota is available",
                    lambda f: float(f.get("staging_quota_bytes", 0.0))
                    >= float(f.get("required_staging_bytes", 1.0)),
                    "{rung} requires sufficient staging storage quota",
                ),
                decide.Need(
                    "staged bulk load throughput beats degraded direct stream",
                    lambda f: float(f.get("staged_bulk_throughput", 0.0))
                    > float(f.get("current_throughput", 0.0)),
                    "{rung} refused because staged bulk load is not faster than current throughput",
                ),
            ),
            because="decouples producer/consumer rate mismatch via fast local staging ingestion",
            footprint=2,
            leaves="staging objects and bulk load descriptors",
        ),
        decide.Rung(
            "switch_to_parallel_range_split",
            "switch remaining work to dynamic fine-grained range splitting with parallel workers",
            needs=(
                decide.Need(
                    "splittable keys and high partition skew present",
                    lambda f: bool(f.get("splittable_keys_available", True))
                    and float(f.get("max_skew_ratio", 1.0))
                    >= float(f.get("skew_threshold", 2.0)),
                    "{rung} requires splittable keys and high partition skew",
                ),
                decide.Need(
                    "worker and connection quota allow increased concurrency",
                    lambda f: int(f.get("available_worker_connections", 0))
                    >= int(f.get("required_split_workers", 2)),
                    "{rung} refused because connection quota limits parallel workers",
                ),
            ),
            because="parallelizes straggler partitions and eliminates tail skew bottleneck",
            footprint=1,
            leaves="fine range partition manifests",
        ),
        decide.Rung(
            "switch_to_direct_stream",
            "switch remaining work back to low-latency direct streaming",
            needs=(
                decide.Need(
                    "direct streaming connectivity and target write headroom restored",
                    lambda f: bool(f.get("direct_stream_available", True))
                    and float(f.get("direct_stream_throughput", 0.0))
                    > float(f.get("current_throughput", 0.0)),
                    "{rung} requires direct stream connectivity and superior throughput",
                ),
                decide.Need(
                    "staging overhead exceeds direct streaming transfer time",
                    lambda f: bool(f.get("staging_overhead_exceeds_direct", True)),
                    "{rung} refused because staging remains more efficient than direct stream",
                ),
            ),
            because="eliminates staging I/O double-hop when network and target have recovered",
            footprint=0,
        ),
        decide.Rung(
            "retain_current_plan",
            "retain current execution plan as safe incumbent baseline",
            needs=(
                decide.Need(
                    "current plan is viable and safe",
                    lambda f: bool(f.get("current_plan_viable", True)),
                    "{rung} cannot retain broken or unexecutable plan",
                ),
            ),
            because="preserves execution momentum and avoids unnecessary reconfiguration",
            footprint=0,
        ),
    )


def transition_cost_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for evaluating priced state transition and hysteresis."""
    return (
        decide.Rung(
            "transition_vetoed_by_snapshot_staleness",
            "veto transition because required snapshot age exceeds database retention limit",
            needs=(
                decide.Need(
                    "snapshot age exceeds safe database retention horizon",
                    lambda f: float(f.get("snapshot_age_sec", 0.0))
                    > float(f.get("max_snapshot_age_sec", 14400.0)),
                    "{rung} refused because snapshot age is safely within database retention limits",
                ),
                decide.Need(
                    "veto protects against snapshot too old or replication slot loss",
                    lambda f: bool(f.get("snapshot_loss_risk_confirmed", True)),
                    "{rung} refused because snapshot loss risk is not present",
                ),
            ),
            because="guarantees snapshot consistency and prevents catastrophic transaction rollback",
            footprint=0,
        ),
        decide.Rung(
            "transition_vetoed_by_undo_penalty",
            "veto transition because in-flight uncommitted work and undo penalty wipes out savings",
            needs=(
                decide.Need(
                    "uncommitted undo and replay cost exceeds allowable savings fraction",
                    lambda f: float(f.get("undo_replay_cost_sec", 0.0))
                    > float(f.get("max_allowed_undo_cost_sec", 0.0)),
                    "{rung} refused because undo cost is within acceptable bounds",
                ),
                decide.Need(
                    "completing in-flight chunk is faster than aborting and rolling back",
                    lambda f: bool(f.get("finishing_in_flight_faster", True)),
                    "{rung} refused because aborting is faster than completing in-flight work",
                ),
            ),
            because="protects already-performed chunk progress from wasteful abandonment",
            footprint=0,
        ),
        decide.Rung(
            "transition_vetoed_by_reconnect_quota",
            "veto transition because connection rate limit or pool quota prohibits reconnects",
            needs=(
                decide.Need(
                    "reconnect rate limit or connection quota is exhausted",
                    lambda f: bool(f.get("reconnect_quota_exhausted", False)),
                    "{rung} refused because connection quota has available headroom",
                ),
                decide.Need(
                    "reconnect failure would disrupt active replication lease",
                    lambda f: bool(f.get("reconnect_risk_disruptive", True)),
                    "{rung} refused because reconnect is non-disruptive",
                ),
            ),
            because="prevents connection pool starvation and lease forfeiture",
            footprint=0,
        ),
        decide.Rung(
            "transition_justified_by_net_savings",
            "approve transition because conservative remaining savings exceed transition cost plus hysteresis",
            needs=(
                decide.Need(
                    "conservative remaining-work savings exceed total transition cost plus hysteresis",
                    lambda f: (
                        float(f.get("current_remaining_sec", 0.0))
                        - float(f.get("candidate_remaining_sec", 0.0))
                    )
                    > (
                        float(f.get("total_transition_cost_sec", 0.0))
                        + float(f.get("hysteresis_margin_sec", 0.0))
                    ),
                    "{rung} refused because remaining work savings do not exceed transition cost and hysteresis",
                ),
                decide.Need(
                    "snapshot age is within safe WAL retention limit",
                    lambda f: float(f.get("snapshot_age_sec", 0.0))
                    <= float(f.get("max_snapshot_age_sec", 14400.0)),
                    "{rung} refused because snapshot age exceeds database retention limit",
                ),
                decide.Need(
                    "undo penalty does not wipe out candidate advantage",
                    lambda f: float(f.get("undo_replay_cost_sec", 0.0))
                    <= float(f.get("max_allowed_undo_cost_sec", float("inf"))),
                    "{rung} refused because undo penalty exceeds allowable limit",
                ),
            ),
            because="switches only when measured end-to-end completion time is strictly reduced",
            footprint=1,
        ),
        decide.Rung(
            "maintain_incumbent_within_hysteresis",
            "retain incumbent plan because savings are within or below the hysteresis damping margin",
            needs=(
                decide.Need(
                    "savings do not exceed transition cost plus hysteresis",
                    lambda f: (
                        float(f.get("current_remaining_sec", 0.0))
                        - float(f.get("candidate_remaining_sec", 0.0))
                    )
                    <= (
                        float(f.get("total_transition_cost_sec", 0.0))
                        + float(f.get("hysteresis_margin_sec", 0.0))
                    ),
                    "{rung} refused because savings clearly exceed the hysteresis damping band",
                ),
            ),
            because="damps minor fluctuations and guarantees stability when gains are marginal or absent",
            footprint=0,
        ),
    )


def dag_reschedule_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for reshaping the remaining phase/table/range DAG."""
    return (
        decide.Rung(
            "prioritize_critical_cdc_tables",
            "reorder remaining DAG to prioritize high-volume CDC tables",
            needs=(
                decide.Need(
                    "remaining tables have active high-volume CDC arrivals",
                    lambda f: float(f.get("max_table_cdc_rate", 0.0))
                    >= float(f.get("critical_cdc_rate_threshold", 500.0)),
                    "{rung} refused because no remaining table has high-volume CDC arrivals",
                ),
                decide.Need(
                    "early drain reduces cutover backlog risk",
                    lambda f: bool(f.get("early_cdc_drain_beneficial", True)),
                    "{rung} refused because early CDC drain does not reduce cutover backlog risk",
                ),
            ),
            because="drains fast-moving CDC streams first to minimize tail catchup time",
            footprint=1,
        ),
        decide.Rung(
            "dynamic_split_skewed_ranges",
            "split skewed remaining table ranges into balanced sub-ranges",
            needs=(
                decide.Need(
                    "partition skew exceeds split threshold",
                    lambda f: float(f.get("partition_skew_ratio", 1.0))
                    >= float(f.get("skew_split_threshold", 2.0)),
                    "{rung} refused because partition skew is below split threshold",
                ),
                decide.Need(
                    "splittable key ranges available",
                    lambda f: bool(f.get("splittable_keys_available", True)),
                    "{rung} refused because keys are not splittable",
                ),
            ),
            because="prevents straggler partitions from dragging out remaining DAG execution",
            footprint=1,
        ),
        decide.Rung(
            "coalesce_small_remaining_ranges",
            "coalesce small remaining ranges and tables into bundled execution units",
            needs=(
                decide.Need(
                    "small range count exceeds coalesce threshold",
                    lambda f: int(f.get("small_range_count", 0))
                    >= int(f.get("coalesce_count_threshold", 8)),
                    "{rung} refused because small range count is below coalesce threshold",
                ),
                decide.Need(
                    "coalescing reduces per-chunk setup and teardown overhead",
                    lambda f: bool(f.get("coalescing_reduces_overhead", True)),
                    "{rung} refused because coalescing does not reduce dispatch overhead",
                ),
            ),
            because="amortizes setup and validation costs across thousands of small tables/ranges",
            footprint=1,
        ),
        decide.Rung(
            "preserve_topological_checkpoint_order",
            "preserve standard topological order for remaining DAG units",
            needs=(
                decide.Need(
                    "topological order is valid and unblocked",
                    lambda f: bool(f.get("topological_order_valid", True)),
                    "{rung} refused because topological order is invalid or blocked",
                ),
            ),
            because="preserves deterministic dependency ordering for clean checkpoint progress",
            footprint=0,
        ),
    )


# -----------------------------------------------------------------------------
# O(1) Typed Durable Checkpoint Store & Active Lease Lock
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class LiveReplanCheckpointUnit:
    """Durable typed checkpoint record for one `(node_id, phase)` tuple."""

    node_id: str
    phase: DAGPhase
    strategy: ReplanStrategy
    min_cursor: Any
    max_cursor: Any
    merkle_digest: str
    completed_at: datetime.datetime


class LiveReplanCheckpointStore:
    """Universal typed durable checkpoint store with O(1) unit completion lookup."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = pathlib.Path(path)
        self._units: Dict[Tuple[str, str], LiveReplanCheckpointUnit] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="ascii"))
        except Exception as e:
            raise CheckpointCorruptionError(
                f"Failed to load checkpoint store at {self.path}: {e}"
            ) from e

        for entry in raw.get("units", ()):
            unit = LiveReplanCheckpointUnit(
                node_id=str(entry["node_id"]),
                phase=DAGPhase(entry["phase"]),
                strategy=ReplanStrategy(entry["strategy"]),
                min_cursor=checkpoint.deserialize_cursor(entry["min_cursor"]),
                max_cursor=checkpoint.deserialize_cursor(entry["max_cursor"]),
                merkle_digest=str(entry["merkle_digest"]),
                completed_at=checkpoint.deserialize_cursor(entry["completed_at"]),
            )
            self._units[(unit.node_id, unit.phase.value)] = unit

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "units": [
                {
                    "node_id": u.node_id,
                    "phase": u.phase.value,
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

    def record_unit(self, unit: LiveReplanCheckpointUnit) -> None:
        self._units[(unit.node_id, unit.phase.value)] = unit
        self._save()

    def is_completed_o1(self, node_id: str, phase: DAGPhase) -> bool:
        """O(1) lookup to verify if node is already completed."""
        return (node_id, phase.value) in self._units

    def get_unit(
        self, node_id: str, phase: DAGPhase
    ) -> Optional[LiveReplanCheckpointUnit]:
        return self._units.get((node_id, phase.value))

    def completed_count(self) -> int:
        return len(self._units)


def acquire_live_replan_lease(
    lock_path: pathlib.Path,
    *,
    what: str = "live replan priced state transitions",
    term_seconds: float = 60.0,
) -> lease.Lease:
    """Acquire an exclusive `.run.lock` lease with heartbeat via `migkit.lease.Lease`."""
    return lease.Lease(pathlib.Path(lock_path), what=what, term=term_seconds).acquire()


# -----------------------------------------------------------------------------
# Work Estimation, Reschedule & Live Replan Engine
# -----------------------------------------------------------------------------


def estimate_byte_weighted_eta(
    total_bytes: int,
    completed_bytes: int,
    elapsed_seconds: float,
    cold_start_bytes_per_sec: float,
    downstream_tail_seconds: float = 0.0,
) -> float:
    """Compute instant cold-start byte-weighted ETA."""
    if total_bytes < 0 or completed_bytes < 0:
        raise InvalidReplanFactError("total_bytes and completed_bytes must be >= 0")
    if cold_start_bytes_per_sec <= 0.0:
        raise InvalidReplanFactError("cold_start_bytes_per_sec must be > 0")

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


def estimate_strategy_remaining_seconds(
    strategy: ReplanStrategy,
    dag: LiveReplanDAG,
    *,
    telemetry: SmoothedTelemetry,
    quotas: ResourceQuotaSpec,
    facts_map: Mapping[str, Any],
) -> float:
    """Estimate conservative remaining time for remaining DAG under a specific strategy."""
    rem_bytes = dag.remaining_bytes()
    if rem_bytes == 0:
        return 0.0

    if strategy == ReplanStrategy.DIRECT_STREAM:
        eff_rate = min(
            quotas.network_bytes_per_sec,
            quotas.disk_write_bytes_per_sec,
            max(1.0, telemetry.smoothed_throughput_bytes_per_sec),
        )
        base_time = float(rem_bytes) / eff_rate
        # In direct stream, high CDC arrivals may add downstream catchup time
        cdc_net_rate = max(0.0, telemetry.mean_cdc_arrival_rate - telemetry.mean_cdc_drain_rate)
        cdc_catchup = (cdc_net_rate * base_time) / max(1.0, quotas.disk_write_bytes_per_sec * 0.8)
        return round(base_time + cdc_catchup, 6)

    elif strategy == ReplanStrategy.STAGED_BULK_LOAD:
        # Decoupled staging: write to spool then bulk load
        staged_rate = float(facts_map.get("staged_bulk_throughput", quotas.disk_write_bytes_per_sec * 1.5))
        base_time = float(rem_bytes) / max(1.0, staged_rate)
        staged_overhead = float(facts_map.get("staged_bulk_tail_sec", 4.0))
        return round(base_time + staged_overhead, 6)

    elif strategy == ReplanStrategy.PARALLEL_RANGE_SPLIT:
        # Concurrency scales throughput across workers up to connection quota
        workers = min(
            int(facts_map.get("available_worker_connections", 4)),
            quotas.max_worker_connections,
        )
        speedup = max(1.0, math.sqrt(float(workers)) * 1.3)
        base_rate = max(1.0, telemetry.smoothed_throughput_bytes_per_sec)
        eff_rate = min(quotas.network_bytes_per_sec, base_rate * speedup)
        return round(float(rem_bytes) / eff_rate, 6)

    else:  # RETAIN_CURRENT_PLAN
        cur_rate = max(1.0, telemetry.smoothed_throughput_bytes_per_sec)
        return round(float(rem_bytes) / cur_rate, 6)


def reshape_remaining_dag(
    dag: LiveReplanDAG,
    choice: str,
    *,
    split_threshold: float = 2.0,
    target_chunk_bytes: int = 5_000_000,
) -> LiveReplanDAG:
    """Reshape remaining DAG units according to the chosen DAG reschedule rung."""
    rem_nodes = dag.get_remaining_nodes()
    committed_nodes = dag.get_committed_nodes()

    if choice == "prioritize_critical_cdc_tables":
        # Sort remaining nodes descending by CDC arrival rate
        reordered = sorted(rem_nodes, key=lambda n: (-n.cdc_arrival_rate, -n.estimated_bytes, n.node_id))
        all_nodes = list(committed_nodes) + reordered
        return LiveReplanDAG(all_nodes)

    elif choice == "dynamic_split_skewed_ranges":
        reshaped: List[DAGNode] = []
        for n in rem_nodes:
            if n.is_skewed and n.estimated_bytes > target_chunk_bytes * 2:
                # Split skewed node into 2 balanced sub-ranges
                half_bytes = n.estimated_bytes // 2
                half_rows = n.estimated_rows // 2
                part_a = DAGNode(
                    node_id=f"{n.node_id}_p0",
                    phase=n.phase,
                    table_name=n.table_name,
                    range_id=f"{n.range_id}_0",
                    min_cursor=n.min_cursor,
                    max_cursor=f"{n.max_cursor}_split",
                    estimated_bytes=half_bytes,
                    estimated_rows=half_rows,
                    dependencies=n.dependencies,
                    status=DAGNodeStatus.PENDING,
                    is_skewed=False,
                    cdc_arrival_rate=n.cdc_arrival_rate * 0.5,
                )
                part_b = DAGNode(
                    node_id=f"{n.node_id}_p1",
                    phase=n.phase,
                    table_name=n.table_name,
                    range_id=f"{n.range_id}_1",
                    min_cursor=f"{n.max_cursor}_split",
                    max_cursor=n.max_cursor,
                    estimated_bytes=n.estimated_bytes - half_bytes,
                    estimated_rows=n.estimated_rows - half_rows,
                    dependencies=n.dependencies,
                    status=DAGNodeStatus.PENDING,
                    is_skewed=False,
                    cdc_arrival_rate=n.cdc_arrival_rate * 0.5,
                )
                reshaped.extend([part_a, part_b])
            else:
                reshaped.append(n)
        return LiveReplanDAG(list(committed_nodes) + reshaped)

    elif choice == "coalesce_small_remaining_ranges":
        # Group small pending ranges of the same table
        small_nodes: List[DAGNode] = []
        regular_nodes: List[DAGNode] = []
        for n in rem_nodes:
            if n.estimated_bytes < target_chunk_bytes and not n.dependencies:
                small_nodes.append(n)
            else:
                regular_nodes.append(n)

        coalesced: List[DAGNode] = []
        if len(small_nodes) >= 2:
            bundle_bytes = sum(n.estimated_bytes for n in small_nodes)
            bundle_rows = sum(n.estimated_rows for n in small_nodes)
            bundle_id = f"bundle_{small_nodes[0].node_id}"
            coalesced.append(
                DAGNode(
                    node_id=bundle_id,
                    phase=small_nodes[0].phase,
                    table_name=small_nodes[0].table_name,
                    range_id="coalesced_range",
                    min_cursor=small_nodes[0].min_cursor,
                    max_cursor=small_nodes[-1].max_cursor,
                    estimated_bytes=bundle_bytes,
                    estimated_rows=bundle_rows,
                    dependencies=(),
                    status=DAGNodeStatus.PENDING,
                    is_skewed=False,
                    cdc_arrival_rate=sum(n.cdc_arrival_rate for n in small_nodes),
                )
            )
        else:
            coalesced.extend(small_nodes)

        return LiveReplanDAG(list(committed_nodes) + regular_nodes + coalesced)

    else:  # preserve_topological_checkpoint_order
        return dag


@dataclass(frozen=True)
class LiveReplanResult:
    """Comprehensive outcome of a live replan evaluation."""

    switched: bool
    current_strategy: ReplanStrategy
    chosen_strategy: ReplanStrategy
    trigger_choice: decide.Choice
    strategy_choice: decide.Choice
    transition_cost_choice: decide.Choice
    dag_reschedule_choice: decide.Choice
    transition_cost_breakdown: TransitionCostBreakdown
    estimated_remaining_seconds_current: float
    estimated_remaining_seconds_candidate: float
    gross_savings_seconds: float
    net_benefit_seconds: float
    hysteresis_applied_sec: float
    rescheduled_dag: LiveReplanDAG
    care_badge: CareBadge
    summary: str


def evaluate_live_replan(
    work: str,
    *,
    current_strategy: ReplanStrategy,
    dag: LiveReplanDAG,
    checkpoint_store: LiveReplanCheckpointStore,
    telemetry_history: Sequence[TelemetryObservation],
    quotas: ResourceQuotaSpec,
    policy: PricedTransitionPolicy = PricedTransitionPolicy(),
    in_flight_uncommitted_bytes: int = 0,
    in_flight_uncommitted_rows: int = 0,
    source_buffer_pool_bytes: int = 10_000_000,
    target_buffer_pool_bytes: int = 10_000_000,
    snapshot_age_seconds: float = 60.0,
    active_connections: int = 4,
    seconds_since_last_switch: float = 60.0,
    facts_override: Optional[Mapping[str, Any]] = None,
) -> LiveReplanResult:
    """Perform live replanning with priced state transitions and safe checkpointing."""
    if not work:
        raise InvalidReplanFactError("work string must not be empty")

    raw_facts: Dict[str, Any] = dict(facts_override or {})

    # Step 1: Checkpoint preservation check (O(1))
    for node in dag.all_nodes:
        if checkpoint_store.is_completed_o1(node.node_id, node.phase):
            dag.mark_committed(node.node_id)

    # Step 2: Smoothing & Oscillation detection
    smoothed = smooth_telemetry_series(telemetry_history)

    # Step 3: Trigger evaluation via decide.climb
    baseline_tp = float(raw_facts.get("baseline_throughput", max(1.0, smoothed.mean_throughput_bytes_per_sec)))
    current_tp = smoothed.smoothed_throughput_bytes_per_sec
    degradation_duration = smoothed.duration_seconds

    trigger_facts_dict: Dict[str, Any] = {
        "is_oscillating": smoothed.is_oscillating,
        "seconds_since_last_switch": seconds_since_last_switch,
        "cooldown_seconds": policy.cooldown_seconds,
        "damping_preserves_stability": True,
        "current_throughput": current_tp,
        "baseline_throughput": baseline_tp,
        "degradation_threshold": policy.degradation_threshold,
        "degradation_duration_sec": degradation_duration,
        "min_observation_window_sec": policy.min_observation_window_sec,
        "cdc_arrival_rate": smoothed.mean_cdc_arrival_rate,
        "cdc_drain_rate": smoothed.mean_cdc_drain_rate,
        "cdc_backlog_bytes": smoothed.current_cdc_backlog_bytes,
        "cdc_backlog_threshold_bytes": float(raw_facts.get("cdc_backlog_threshold_bytes", 10_000_000.0)),
        "resource_load_ratio": max(smoothed.mean_cpu_ratio, smoothed.mean_memory_ratio),
        "quota_ceiling_ratio": float(raw_facts.get("quota_ceiling_ratio", 0.85)),
        "alternate_pathway_available": bool(raw_facts.get("alternate_pathway_available", True)),
    }
    trigger_facts_dict.update(raw_facts)

    trigger_choice = decide.climb(
        work,
        replan_trigger_rungs(),
        decide.Facts(trigger_facts_dict),
    )

    # If trigger is transient_oscillation_damping or None, stay on current plan
    if trigger_choice.rung is None or trigger_choice.path == "transient_oscillation_damping":
        costs_zero = calculate_transition_cost(
            current_strategy,
            current_strategy,
            in_flight_uncommitted_bytes=0,
            in_flight_uncommitted_rows=0,
            source_buffer_pool_bytes=source_buffer_pool_bytes,
            target_buffer_pool_bytes=target_buffer_pool_bytes,
            snapshot_age_seconds=snapshot_age_seconds,
            active_connections=active_connections,
            quotas=quotas,
            facts_map=raw_facts,
        )
        cur_rem = estimate_strategy_remaining_seconds(
            current_strategy, dag, telemetry=smoothed, quotas=quotas, facts_map=raw_facts
        )
        badge = CareBadge.BENIGN if trigger_choice.rung is not None else CareBadge.ACTION
        summary = (
            f"BENIGN: Replan suppressed by transient oscillation damping; retained {current_strategy.value}"
            if trigger_choice.rung is not None
            else "ACTION: All replan trigger rungs refused"
        )
        dummy_choice = decide.climb(work, replan_strategy_rungs(), decide.Facts({"current_plan_viable": True}))
        dummy_trans = decide.climb(work, transition_cost_rungs(), decide.Facts({
            "current_remaining_sec": cur_rem,
            "candidate_remaining_sec": cur_rem,
            "total_transition_cost_sec": 0.0,
            "hysteresis_margin_sec": policy.hysteresis_margin_sec,
        }))
        dummy_dag = decide.climb(work, dag_reschedule_rungs(), decide.Facts({"topological_order_valid": True}))

        return LiveReplanResult(
            switched=False,
            current_strategy=current_strategy,
            chosen_strategy=current_strategy,
            trigger_choice=trigger_choice,
            strategy_choice=dummy_choice,
            transition_cost_choice=dummy_trans,
            dag_reschedule_choice=dummy_dag,
            transition_cost_breakdown=costs_zero,
            estimated_remaining_seconds_current=cur_rem,
            estimated_remaining_seconds_candidate=cur_rem,
            gross_savings_seconds=0.0,
            net_benefit_seconds=0.0,
            hysteresis_applied_sec=policy.hysteresis_margin_sec,
            rescheduled_dag=dag,
            care_badge=badge,
            summary=summary,
        )

    # Step 4: Strategy selection via decide.climb
    rem_bytes = dag.remaining_bytes()
    strat_facts_dict: Dict[str, Any] = {
        "staging_quota_bytes": quotas.staging_quota_bytes,
        "required_staging_bytes": float(rem_bytes * 0.35),
        "staged_bulk_throughput": float(raw_facts.get("staged_bulk_throughput", quotas.disk_write_bytes_per_sec * 1.5)),
        "current_throughput": current_tp,
        "splittable_keys_available": bool(raw_facts.get("splittable_keys_available", True)),
        "max_skew_ratio": float(raw_facts.get("max_skew_ratio", 1.0)),
        "skew_threshold": float(raw_facts.get("skew_threshold", 2.0)),
        "available_worker_connections": int(raw_facts.get("available_worker_connections", quotas.max_worker_connections)),
        "required_split_workers": 2,
        "direct_stream_available": bool(raw_facts.get("direct_stream_available", True)),
        "direct_stream_throughput": float(raw_facts.get("direct_stream_throughput", quotas.network_bytes_per_sec)),
        "staging_overhead_exceeds_direct": bool(raw_facts.get("staging_overhead_exceeds_direct", True)),
        "current_plan_viable": bool(raw_facts.get("current_plan_viable", True)),
    }
    strat_facts_dict.update(raw_facts)

    strategy_choice = decide.climb(
        work,
        replan_strategy_rungs(),
        decide.Facts(strat_facts_dict),
    )

    if strategy_choice.rung is None:
        raise LiveReplanError("All replan strategy rungs were refused")

    _STRATEGY_RUNGS_TO_ENUM = {
        "switch_to_staged_bulk_load": ReplanStrategy.STAGED_BULK_LOAD,
        "switch_to_parallel_range_split": ReplanStrategy.PARALLEL_RANGE_SPLIT,
        "switch_to_direct_stream": ReplanStrategy.DIRECT_STREAM,
        "retain_current_plan": ReplanStrategy.RETAIN_CURRENT_PLAN,
    }
    candidate_strategy = _STRATEGY_RUNGS_TO_ENUM.get(
        strategy_choice.path, ReplanStrategy.RETAIN_CURRENT_PLAN
    )

    # Step 5: Transition cost & Hysteresis gate via decide.climb
    transition_cost = calculate_transition_cost(
        current_strategy,
        candidate_strategy,
        in_flight_uncommitted_bytes=in_flight_uncommitted_bytes,
        in_flight_uncommitted_rows=in_flight_uncommitted_rows,
        source_buffer_pool_bytes=source_buffer_pool_bytes,
        target_buffer_pool_bytes=target_buffer_pool_bytes,
        snapshot_age_seconds=snapshot_age_seconds,
        active_connections=active_connections,
        quotas=quotas,
        facts_map=raw_facts,
    )

    cur_remaining_sec = estimate_strategy_remaining_seconds(
        current_strategy, dag, telemetry=smoothed, quotas=quotas, facts_map=raw_facts
    )
    cand_remaining_sec = estimate_strategy_remaining_seconds(
        candidate_strategy, dag, telemetry=smoothed, quotas=quotas, facts_map=raw_facts
    )

    gross_savings = max(0.0, cur_remaining_sec - cand_remaining_sec)
    hysteresis_sec = max(
        policy.hysteresis_margin_sec,
        policy.hysteresis_ratio * transition_cost.total_transition_cost_sec,
    )
    max_undo_allowed = gross_savings * policy.max_allowed_undo_ratio

    trans_facts_dict: Dict[str, Any] = {
        "current_remaining_sec": cur_remaining_sec,
        "candidate_remaining_sec": cand_remaining_sec,
        "total_transition_cost_sec": transition_cost.total_transition_cost_sec,
        "hysteresis_margin_sec": hysteresis_sec,
        "snapshot_age_sec": snapshot_age_seconds,
        "max_snapshot_age_sec": policy.max_snapshot_age_sec,
        "snapshot_loss_risk_confirmed": bool(raw_facts.get("snapshot_loss_risk_confirmed", True)),
        "undo_replay_cost_sec": transition_cost.undo_replay_sec,
        "max_allowed_undo_cost_sec": max_undo_allowed,
        "finishing_in_flight_faster": bool(raw_facts.get("finishing_in_flight_faster", True)),
        "reconnect_quota_exhausted": bool(raw_facts.get("reconnect_quota_exhausted", False)),
        "reconnect_risk_disruptive": bool(raw_facts.get("reconnect_risk_disruptive", True)),
    }
    trans_facts_dict.update(raw_facts)

    cost_choice = decide.climb(
        work,
        transition_cost_rungs(),
        decide.Facts(trans_facts_dict),
    )

    if cost_choice.rung is None:
        raise LiveReplanError("All transition cost rungs were refused")

    approved_switch = (
        cost_choice.path == "transition_justified_by_net_savings"
        and candidate_strategy != current_strategy
        and candidate_strategy != ReplanStrategy.RETAIN_CURRENT_PLAN
    )

    final_strategy = candidate_strategy if approved_switch else current_strategy
    net_benefit = (
        (gross_savings - transition_cost.total_transition_cost_sec - hysteresis_sec)
        if approved_switch
        else 0.0
    )

    # Step 6: DAG Reschedule via decide.climb
    rem_nodes = dag.get_remaining_nodes()
    max_cdc = max([n.cdc_arrival_rate for n in rem_nodes] or [0.0])
    small_count = sum(1 for n in rem_nodes if n.estimated_bytes < 5_000_000)
    has_skew = any(n.is_skewed for n in rem_nodes)

    dag_facts_dict: Dict[str, Any] = {
        "max_table_cdc_rate": max_cdc,
        "critical_cdc_rate_threshold": float(raw_facts.get("critical_cdc_rate_threshold", 500.0)),
        "early_cdc_drain_beneficial": bool(raw_facts.get("early_cdc_drain_beneficial", True)),
        "partition_skew_ratio": float(raw_facts.get("max_skew_ratio", 2.5 if has_skew else 1.0)),
        "skew_split_threshold": float(raw_facts.get("skew_threshold", 2.0)),
        "splittable_keys_available": bool(raw_facts.get("splittable_keys_available", True)),
        "small_range_count": small_count,
        "coalesce_count_threshold": int(raw_facts.get("coalesce_count_threshold", 8)),
        "coalescing_reduces_overhead": bool(raw_facts.get("coalescing_reduces_overhead", True)),
        "topological_order_valid": bool(raw_facts.get("topological_order_valid", True)),
    }
    dag_facts_dict.update(raw_facts)

    dag_choice = decide.climb(
        work,
        dag_reschedule_rungs(),
        decide.Facts(dag_facts_dict),
    )

    if dag_choice.rung is None:
        raise LiveReplanError("All DAG reschedule rungs were refused")

    rescheduled = reshape_remaining_dag(dag, dag_choice.path)

    # Care badge determination
    if approved_switch:
        badge = CareBadge.PASS
        summary = (
            f"PASS: Switched from {current_strategy.value} to {final_strategy.value}; "
            f"net savings {net_benefit:,.1f}s after transition cost {transition_cost.total_transition_cost_sec:,.1f}s "
            f"and hysteresis {hysteresis_sec:,.1f}s"
        )
    elif cost_choice.path.startswith("transition_vetoed"):
        badge = CareBadge.REVIEW
        summary = (
            f"REVIEW: Replan to {candidate_strategy.value} vetoed by {cost_choice.path}; "
            f"retained {current_strategy.value}"
        )
    else:
        badge = CareBadge.BENIGN
        summary = (
            f"BENIGN: Candidate savings {gross_savings:,.1f}s within hysteresis {hysteresis_sec:,.1f}s; "
            f"retained {current_strategy.value}"
        )

    return LiveReplanResult(
        switched=approved_switch,
        current_strategy=current_strategy,
        chosen_strategy=final_strategy,
        trigger_choice=trigger_choice,
        strategy_choice=strategy_choice,
        transition_cost_choice=cost_choice,
        dag_reschedule_choice=dag_choice,
        transition_cost_breakdown=transition_cost,
        estimated_remaining_seconds_current=cur_remaining_sec,
        estimated_remaining_seconds_candidate=cand_remaining_sec,
        gross_savings_seconds=round(gross_savings, 6),
        net_benefit_seconds=round(net_benefit, 6),
        hysteresis_applied_sec=round(hysteresis_sec, 6),
        rescheduled_dag=rescheduled,
        care_badge=badge,
        summary=summary,
    )
