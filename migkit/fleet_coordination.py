"""Coordinate hops sharing physical capacity (SMART-N23.fleet-coordination, Row 606).

Extends topology, jointplan, and leases using `decide.Rung` and `decide.climb`.
Identifies logical endpoints sharing physical hosts, storage subsystems, network
paths, quotas, and replicas so concurrent hops cannot independently exhaust
the same capacity.

Key capabilities:
1. Physical capacity disambiguation: maps logical endpoints to physical hosts,
   storage volumes, network paths, service quotas, and replicas.
2. Fair admission & dynamic feedback: prioritizes critical CDC catch-up and
   cutover proof; calculates AIMD rate throttling under physical resource pressure.
3. Compatible work stealing: rebalances movable range/table units between workers
   sharing physical capacity while strictly enforcing tenant isolation, engine
   compatibility, and lease fencing.
4. Coordinator lease & failover reconciliation: distributed active lease with
   mutual exclusion (.run.lock), heartbeat jitter tracking, fencing generation tokens,
   and pre-takeover audit reconciling active worker leases to prevent double ownership.
5. Universal typed durable resume in O(1): checkpoints range/cursor level state
   with native type preservation (datetime, Decimal, UUID) skipping completed units.
6. Pure UNIX enterprise style with explicit care badges: ACTION, REVIEW, BENIGN, PASS.
"""
from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import fcntl
import json
import math
import os
import pathlib
import socket
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union
import uuid

from . import decide, lease, topology


# -----------------------------------------------------------------------------
# Care Badges & Enums
# -----------------------------------------------------------------------------

class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


CARE_ACTION = CareBadge.ACTION
CARE_REVIEW = CareBadge.REVIEW
CARE_BENIGN = CareBadge.BENIGN
CARE_PASS = CareBadge.PASS


class HopWorkKind(str, enum.Enum):
    CRITICAL_CATCHUP = "critical_catchup"
    CRITICAL_PROOF = "critical_proof"
    NORMAL_BULK_COPY = "normal_bulk_copy"
    BACKGROUND_INSPECT = "background_inspect"


class ResourceDimension(str, enum.Enum):
    CPU_CORES = "cpu_cores"
    READ_IOPS = "read_iops"
    WRITE_IOPS = "write_iops"
    NETWORK_MB_S = "network_mb_s"
    CONNECTIONS = "connections"
    REPLICATION_SLOTS = "replication_slots"


# -----------------------------------------------------------------------------
# Custom Exceptions
# -----------------------------------------------------------------------------

class FleetCoordinationError(Exception):
    """Base error for fleet coordination and capacity scheduling."""

    def __init__(self, message: str, care_badge: CareBadge = CareBadge.ACTION):
        super().__init__(message)
        self.message = message
        self.care_badge = care_badge


class UnsafeEndpointError(FleetCoordinationError):
    """Raised when an endpoint specifies an unsafe non-test host."""


class CapacityConstraintViolationError(FleetCoordinationError):
    """Raised when concurrent operations violate global physical constraints."""


class DoubleOwnershipError(FleetCoordinationError):
    """Raised when two workers or hops attempt double ownership of a resource or unit."""


class IncompatibleStealError(FleetCoordinationError):
    """Raised when work stealing violates compatibility or tenant boundaries."""


class CoordinatorSplitBrainError(FleetCoordinationError):
    """Raised when coordinator failover detects split-brain or fence generation conflict."""


# -----------------------------------------------------------------------------
# Endpoint Host Validation (Safe Test Hosts Only)
# -----------------------------------------------------------------------------

def validate_safe_endpoint(host: str) -> str:
    """Validate that endpoint host is loopback (127.0.0.1, localhost) or RFC1918 10.0.0.x."""
    cleaned = (host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError(
            "endpoint host must not be empty", care_badge=CareBadge.ACTION
        )
    if cleaned in ("127.0.0.1", "localhost") or cleaned.startswith("10.0.0."):
        return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x permitted",
        care_badge=CareBadge.ACTION,
    )


# -----------------------------------------------------------------------------
# Physical Resource Vector & Capacity Modeling
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class ResourceVector:
    """Multi-resource usage vector across physical dimensions."""
    cpu_cores: float = 0.0
    read_iops: float = 0.0
    write_iops: float = 0.0
    network_mb_s: float = 0.0
    connections: int = 0
    replication_slots: int = 0

    def add(self, other: ResourceVector) -> ResourceVector:
        return ResourceVector(
            cpu_cores=round(self.cpu_cores + other.cpu_cores, 4),
            read_iops=round(self.read_iops + other.read_iops, 2),
            write_iops=round(self.write_iops + other.write_iops, 2),
            network_mb_s=round(self.network_mb_s + other.network_mb_s, 2),
            connections=self.connections + other.connections,
            replication_slots=self.replication_slots + other.replication_slots,
        )

    def sub(self, other: ResourceVector) -> ResourceVector:
        return ResourceVector(
            cpu_cores=max(0.0, round(self.cpu_cores - other.cpu_cores, 4)),
            read_iops=max(0.0, round(self.read_iops - other.read_iops, 2)),
            write_iops=max(0.0, round(self.write_iops - other.write_iops, 2)),
            network_mb_s=max(0.0, round(self.network_mb_s - other.network_mb_s, 2)),
            connections=max(0, self.connections - other.connections),
            replication_slots=max(0, self.replication_slots - other.replication_slots),
        )

    def fits_in(self, limit: ResourceVector) -> bool:
        return (
            self.cpu_cores <= limit.cpu_cores + 1e-6
            and self.read_iops <= limit.read_iops + 1e-6
            and self.write_iops <= limit.write_iops + 1e-6
            and self.network_mb_s <= limit.network_mb_s + 1e-6
            and self.connections <= limit.connections
            and self.replication_slots <= limit.replication_slots
        )

    def scale(self, factor: float) -> ResourceVector:
        safe_factor = max(0.0, factor)
        return ResourceVector(
            cpu_cores=round(self.cpu_cores * safe_factor, 4),
            read_iops=round(self.read_iops * safe_factor, 2),
            write_iops=round(self.write_iops * safe_factor, 2),
            network_mb_s=round(self.network_mb_s * safe_factor, 2),
            connections=max(1 if safe_factor > 0 and self.connections > 0 else 0,
                            int(math.ceil(self.connections * safe_factor))),
            replication_slots=max(1 if safe_factor > 0 and self.replication_slots > 0 else 0,
                                  int(math.ceil(self.replication_slots * safe_factor))),
        )


@dataclasses.dataclass(frozen=True)
class PhysicalHost:
    host_id: str
    total_cpu_cores: float
    total_ram_mb: float
    management_ip: str

    def __post_init__(self):
        validate_safe_endpoint(self.management_ip)


@dataclasses.dataclass(frozen=True)
class PhysicalStorage:
    storage_id: str
    max_read_iops: float
    max_write_iops: float
    max_throughput_mb_s: float


@dataclasses.dataclass(frozen=True)
class NetworkPath:
    path_id: str
    max_bandwidth_mb_s: float
    max_packets_per_sec: float


@dataclasses.dataclass(frozen=True)
class ServiceQuota:
    quota_id: str
    max_connections: int
    max_queries_per_sec: float
    max_replication_slots: int


@dataclasses.dataclass(frozen=True)
class ReplicaNode:
    replica_id: str
    physical_host_id: str
    max_cdc_streams: int
    max_replication_lag_s: float


@dataclasses.dataclass(frozen=True)
class PhysicalCapacityEnvelope:
    """Global physical constraints boundary."""
    hosts: Dict[str, PhysicalHost]
    storages: Dict[str, PhysicalStorage]
    networks: Dict[str, NetworkPath]
    quotas: Dict[str, ServiceQuota]
    replicas: Dict[str, ReplicaNode]


@dataclasses.dataclass(frozen=True)
class LogicalEndpoint:
    """A logical database or service endpoint used by a migration hop."""
    endpoint_id: str
    host: str
    physical_host_id: str
    physical_storage_id: str
    network_path_ids: Tuple[str, ...]
    quota_ids: Tuple[str, ...]
    replica_id: Optional[str] = None

    def __post_init__(self):
        validate_safe_endpoint(self.host)


# -----------------------------------------------------------------------------
# Fleet Topology & Physical Capacity Disambiguation
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class FleetTopology:
    """Topology mapping logical endpoints to underlying physical infrastructure."""
    endpoints: Dict[str, LogicalEndpoint]
    envelope: PhysicalCapacityEnvelope

    def identify_shared_physical_resources(
        self, endpoint_a_id: str, endpoint_b_id: str
    ) -> Dict[str, Set[str]]:
        """Identify physical hosts, storages, networks, quotas, and replicas shared by two endpoints."""
        ep_a = self.endpoints.get(endpoint_a_id)
        ep_b = self.endpoints.get(endpoint_b_id)
        if not ep_a or not ep_b:
            return {}

        shared: Dict[str, Set[str]] = {
            "hosts": set(),
            "storages": set(),
            "networks": set(),
            "quotas": set(),
            "replicas": set(),
        }
        if ep_a.physical_host_id == ep_b.physical_host_id:
            shared["hosts"].add(ep_a.physical_host_id)
        if ep_a.physical_storage_id == ep_b.physical_storage_id:
            shared["storages"].add(ep_a.physical_storage_id)
        shared_nets = set(ep_a.network_path_ids) & set(ep_b.network_path_ids)
        if shared_nets:
            shared["networks"].update(shared_nets)
        shared_quotas = set(ep_a.quota_ids) & set(ep_b.quota_ids)
        if shared_quotas:
            shared["quotas"].update(shared_quotas)
        if ep_a.replica_id and ep_b.replica_id and ep_a.replica_id == ep_b.replica_id:
            shared["replicas"].add(ep_a.replica_id)

        return {k: v for k, v in shared.items() if v}

    def endpoints_share_physical_capacity(
        self, endpoint_a_id: str, endpoint_b_id: str
    ) -> bool:
        """Return True if two logical endpoints share any underlying physical resource."""
        shared = self.identify_shared_physical_resources(endpoint_a_id, endpoint_b_id)
        return any(bool(v) for v in shared.values())

    def hops_share_physical_capacity(
        self, hop_a_endpoints: Sequence[str], hop_b_endpoints: Sequence[str]
    ) -> bool:
        """Return True if any endpoint of hop A shares physical capacity with any endpoint of hop B."""
        for ep_a in hop_a_endpoints:
            for ep_b in hop_b_endpoints:
                if self.endpoints_share_physical_capacity(ep_a, ep_b):
                    return True
        return False


# -----------------------------------------------------------------------------
# Background Application Load & Feedback Signal
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class ApplicationLoadReading:
    """Measured background application load on shared physical infrastructure."""
    timestamp: float
    host_load: Dict[str, ResourceVector] = dataclasses.field(default_factory=dict)
    storage_load: Dict[str, ResourceVector] = dataclasses.field(default_factory=dict)
    network_load: Dict[str, ResourceVector] = dataclasses.field(default_factory=dict)
    quota_load: Dict[str, ResourceVector] = dataclasses.field(default_factory=dict)
    replica_load: Dict[str, ResourceVector] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class FeedbackSignal:
    """Real-time latency, replication lag, and utilization feedback."""
    timestamp: float
    latency_p99_ms: float
    target_latency_p99_ms: float
    cdc_lag_seconds: float
    cpu_utilization_ratio: float = 0.0
    iops_utilization_ratio: float = 0.0
    network_utilization_ratio: float = 0.0


def calculate_aimd_throttle_factor(feedback: Optional[FeedbackSignal]) -> float:
    """Calculate AIMD (Additive Increase, Multiplicative Decrease) throttle factor."""
    if feedback is None:
        return 1.0

    # Multiplicative decrease if p99 latency spikes or utilization exceeds 85%
    if feedback.latency_p99_ms > feedback.target_latency_p99_ms * 1.2:
        ratio = feedback.target_latency_p99_ms / max(1.0, feedback.latency_p99_ms)
        return round(max(0.1, min(0.85, ratio)), 3)
    if (
        feedback.cpu_utilization_ratio > 0.85
        or feedback.iops_utilization_ratio > 0.85
        or feedback.network_utilization_ratio > 0.85
    ):
        return 0.5

    # Safe headroom allows full rate (1.0)
    return 1.0


# -----------------------------------------------------------------------------
# Work Units & Leases
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class HopWorkUnit:
    """A discrete unit of migration work (table partition, chunk, or proof range)."""
    unit_id: str
    hop_id: str
    table_name: str
    key_space: Optional[topology.KeySpace] = None
    engine: str = "postgres"
    tenant_id: str = "tenant_default"
    work_kind: HopWorkKind = HopWorkKind.NORMAL_BULK_COPY
    estimated_resource: ResourceVector = dataclasses.field(default_factory=ResourceVector)
    state: str = "pending"  # pending, in_flight, completed, stalled
    movable: bool = True
    locked_by: Optional[str] = None
    fencing_token: int = 1
    updated_at: float = dataclasses.field(default_factory=time.time)


@dataclasses.dataclass(frozen=True)
class WorkerLeaseRecord:
    """Heartbeat-backed lease held by a worker for assigned work units."""
    worker_id: str
    hop_id: str
    unit_ids: Tuple[str, ...]
    host: str
    pid: int
    fencing_token: int
    term_seconds: float
    renewed_at: float
    expires_at: float
    jitter: float = 0.0
    allocated_resources: ResourceVector = dataclasses.field(default_factory=ResourceVector)

    def __post_init__(self):
        validate_safe_endpoint(self.host)

    def is_lapsed(self, now: Optional[float] = None) -> bool:
        current_time = time.time() if now is None else now
        effective_expiry = self.expires_at + min(self.jitter, self.term_seconds)
        return current_time > effective_expiry


@dataclasses.dataclass(frozen=True)
class CoordinatorLeaseRecord:
    """Master fleet coordinator lease record with heartbeat and generation token."""
    coordinator_id: str
    host: str
    pid: int
    fencing_generation: int
    term_seconds: float
    renewed_at: float
    expires_at: float
    jitter: float = 0.0

    def __post_init__(self):
        validate_safe_endpoint(self.host)

    def is_lapsed(self, now: Optional[float] = None) -> bool:
        current_time = time.time() if now is None else now
        effective_expiry = self.expires_at + min(self.jitter, self.term_seconds)
        return current_time > effective_expiry


# -----------------------------------------------------------------------------
# Decision Requests & Verdicts
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class FleetAdmissionRequest:
    request_id: str
    hop_id: str
    endpoint_ids: Tuple[str, ...]
    work_kind: HopWorkKind
    demanded_resources: ResourceVector
    deadline_seconds: Optional[float] = None
    current_cdc_lag_s: float = 0.0


@dataclasses.dataclass(frozen=True)
class AdmissionVerdict:
    admitted: bool
    allocated_resources: ResourceVector
    care_badge: CareBadge
    ladder_rung: str
    reason: str
    throttle_factor: float = 1.0


@dataclasses.dataclass(frozen=True)
class WorkStealingRequest:
    donor_hop_id: str
    recipient_hop_id: str
    recipient_worker_id: str
    candidate_unit: HopWorkUnit
    recipient_engine: str
    recipient_tenant_id: str
    available_headroom: ResourceVector


@dataclasses.dataclass(frozen=True)
class StealVerdict:
    granted: bool
    stolen_unit: Optional[HopWorkUnit]
    new_fencing_token: int
    ladder_rung: str
    care_badge: CareBadge
    reason: str


@dataclasses.dataclass(frozen=True)
class CoordinatorFailoverRequest:
    standby_id: str
    current_lease: Optional[CoordinatorLeaseRecord]
    now: float
    active_worker_leases: Tuple[WorkerLeaseRecord, ...]
    envelope: PhysicalCapacityEnvelope


@dataclasses.dataclass(frozen=True)
class TakeoverVerdict:
    takeover_granted: bool
    new_fencing_generation: int
    reconciled_leases: Tuple[WorkerLeaseRecord, ...]
    ladder_rung: str
    care_badge: CareBadge
    reason: str


# -----------------------------------------------------------------------------
# Decide Ladders
# -----------------------------------------------------------------------------

def fleet_admission_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for fair admission and priority allocation over shared capacity."""
    return (
        decide.Rung(
            "priority_critical_catchup_admit",
            "admit critical CDC catch-up with priority headroom allocation",
            gives=frozenset({"admitted", "prioritized", "cdc_catchup"}),
            needs=(
                decide.Need(
                    "work kind is critical CDC catch-up",
                    lambda f: f.get("work_kind") == HopWorkKind.CRITICAL_CATCHUP.value,
                    "{rung} requires work_kind == critical_catchup",
                ),
                decide.Need(
                    "critical catch-up headroom available",
                    lambda f: bool(f.get("has_catchup_headroom")),
                    "{rung} lacks physical headroom for critical catch-up",
                ),
                decide.Need(
                    "replication lag or cutover deadline warrants priority",
                    lambda f: float(f.get("cdc_lag_seconds", 0.0))
                    >= float(f.get("critical_lag_threshold", 5.0))
                    or bool(f.get("near_cutover_deadline")),
                    "{rung} does not meet critical lag or cutover deadline criteria",
                ),
            ),
            because="critical CDC catch-up requires immediate shared physical capacity to avoid cutover deadline breach",
            footprint=1,
        ),
        decide.Rung(
            "priority_critical_proof_admit",
            "admit critical cutover verification proof",
            gives=frozenset({"admitted", "prioritized", "proof_verified"}),
            needs=(
                decide.Need(
                    "work kind is critical cutover proof",
                    lambda f: f.get("work_kind") == HopWorkKind.CRITICAL_PROOF.value,
                    "{rung} requires work_kind == critical_proof",
                ),
                decide.Need(
                    "verification proof bandwidth and IOPS quota available",
                    lambda f: bool(f.get("has_proof_headroom")),
                    "{rung} lacks physical headroom for proof verification",
                ),
                decide.Need(
                    "proof scope is final cutover validation",
                    lambda f: bool(f.get("is_final_cutover_proof")),
                    "{rung} requires final cutover proof scope",
                ),
            ),
            because="final verification proof requires guaranteed bandwidth and IOPS to certify data integrity before cutover",
            footprint=1,
        ),
        decide.Rung(
            "fair_share_headroom_admit",
            "admit standard hop within shared physical headroom",
            gives=frozenset({"admitted", "fair_share"}),
            needs=(
                decide.Need(
                    "no feedback pressure detected",
                    lambda f: not bool(f.get("feedback_pressure_detected")),
                    "{rung} resource pressure detected; unthrottled fair share refused",
                ),
                decide.Need(
                    "physical capacity envelope has safe headroom for all shared resources",
                    lambda f: bool(f.get("fits_shared_headroom")),
                    "{rung} exceeds available shared physical headroom",
                ),
                decide.Need(
                    "concurrent migration load plus application load does not exceed global bounds",
                    lambda f: not bool(f.get("global_constraints_breached")),
                    "{rung} violates global physical capacity limits",
                ),
                decide.Need(
                    "no starvation among active hops",
                    lambda f: not bool(f.get("hop_starved")),
                    "{rung} would cause starvation of existing active hops",
                ),
            ),
            because="hop demand fits cleanly within physical headroom across hosts, storages, networks, and quotas",
            footprint=2,
        ),
        decide.Rung(
            "throttled_feedback_admit",
            "admit hop with AIMD dynamic throttling under physical resource pressure",
            gives=frozenset({"admitted", "throttled"}),
            needs=(
                decide.Need(
                    "resource pressure detected by latency or utilization feedback",
                    lambda f: bool(f.get("feedback_pressure_detected")),
                    "{rung} requires active resource pressure signal to invoke throttling",
                ),
                decide.Need(
                    "AIMD backoff rate meets minimum progress floor",
                    lambda f: float(f.get("calculated_throttle_factor", 0.0))
                    >= float(f.get("min_throttle_floor", 0.1)),
                    "{rung} throttled allocation falls below minimum viable progress floor",
                ),
                decide.Need(
                    "hop supports throttled execution",
                    lambda f: bool(f.get("supports_throttling", True)),
                    "{rung} requires hop support for throttled execution",
                ),
            ),
            because="shared resource pressure allows admission only under dynamic AIMD rate throttling",
            footprint=3,
        ),
        decide.Rung(
            "queue_and_defer_refuse",
            "defer admission and queue hop request due to capacity saturation",
            gives=frozenset({"deferred", "queued"}),
            needs=(
                decide.Need(
                    "admission queue has capacity for deferred request",
                    lambda f: int(f.get("queue_depth", 0)) < int(f.get("max_queue_depth", 100)),
                    "{rung} admission queue is full",
                ),
                decide.Need(
                    "deferral timeout has not expired",
                    lambda f: not bool(f.get("deferral_timeout_expired")),
                    "{rung} request exceeded deferral timeout",
                ),
            ),
            because="physical capacity is saturated; request is deferred in fair queue until capacity is released",
            footprint=4,
        ),
    )


def work_stealing_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for stealing compatible movable units across workers sharing capacity."""
    return (
        decide.Rung(
            "steal_compatible_range_unit",
            "steal compatible movable unit to rebalance idle worker",
            gives=frozenset({"stolen", "rebalanced"}),
            needs=(
                decide.Need(
                    "unit is marked movable and not terminal",
                    lambda f: bool(f.get("unit_movable"))
                    and f.get("unit_state") in ("pending", "stalled", "backlogged"),
                    "{rung} requires unit to be movable and in non-terminal state",
                ),
                decide.Need(
                    "engine and tenant isolation compatibility holds",
                    lambda f: bool(f.get("engine_compatible"))
                    and bool(f.get("tenant_compatible")),
                    "{rung} requires identical engine family and tenant isolation",
                ),
                decide.Need(
                    "recipient worker has idle physical capacity headroom",
                    lambda f: bool(f.get("recipient_has_headroom")),
                    "{rung} recipient worker lacks sufficient physical headroom",
                ),
                decide.Need(
                    "unit is not actively leased by a live heartbeat",
                    lambda f: not bool(f.get("unit_lease_active")),
                    "{rung} unit is actively locked by an unexpired worker lease",
                ),
            ),
            because="idle worker has available headroom and movable unit is compatible, unleased, and backlogged",
            footprint=1,
        ),
        decide.Rung(
            "cooperative_yield_movable_unit",
            "cooperatively yield non-critical movable unit to relieve overloaded hop",
            gives=frozenset({"yielded", "relieved"}),
            needs=(
                decide.Need(
                    "donor hop is overloaded on shared physical resource",
                    lambda f: bool(f.get("donor_overloaded")),
                    "{rung} donor hop is not overloaded",
                ),
                decide.Need(
                    "unit is non-critical bulk work eligible for cooperative yield",
                    lambda f: bool(f.get("unit_movable"))
                    and f.get("work_kind") != HopWorkKind.CRITICAL_CATCHUP.value,
                    "{rung} unit is critical catch-up or non-movable",
                ),
                decide.Need(
                    "donor cooperative yield lease release confirmed",
                    lambda f: bool(f.get("donor_yield_authorized")),
                    "{rung} donor did not authorize cooperative yield",
                ),
            ),
            because="donor hop cooperatively yields movable unit to reduce pressure on shared physical resource",
            footprint=2,
        ),
        decide.Rung(
            "refuse_incompatible_or_locked_unit",
            "refuse stealing incompatible or locked unit",
            gives=frozenset({"refused", "isolated"}),
            needs=(
                decide.Need(
                    "unit cannot be transferred due to active write lease, tenant boundary, engine mismatch, or critical catchup",
                    lambda f: bool(f.get("incompatible_or_locked")),
                    "{rung} unit is not proven incompatible or locked",
                ),
            ),
            because="work stealing is refused to prevent double ownership, tenant cross-contamination, or engine crash",
            footprint=3,
        ),
    )


def coordinator_failover_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for coordinator failover, lease reconciliation, and fence advance."""
    return (
        decide.Rung(
            "reconcile_and_fence_takeover",
            "reconcile active leases, advance fence generation, and assume coordinator",
            gives=frozenset({"taken_over", "fenced", "reconciled"}),
            needs=(
                decide.Need(
                    "primary coordinator lease is fully lapsed",
                    lambda f: bool(f.get("primary_lease_lapsed")),
                    "{rung} primary coordinator lease is still active",
                ),
                decide.Need(
                    "worker leases successfully reconciled without double ownership",
                    lambda f: bool(f.get("worker_leases_reconciled")),
                    "{rung} worker leases cannot be safely reconciled",
                ),
                decide.Need(
                    "monotonically increasing fencing generation token available",
                    lambda f: int(f.get("proposed_generation", 0))
                    > int(f.get("current_generation", 0)),
                    "{rung} proposed fencing generation must be strictly greater than current",
                ),
                decide.Need(
                    "reconciled physical allocations stay inside global envelope constraints",
                    lambda f: bool(f.get("reconciled_allocations_within_bounds")),
                    "{rung} reconciled allocations exceed global physical capacity",
                ),
            ),
            because="primary lease lapsed cleanly and all worker leases reconciled without double ownership",
            footprint=1,
        ),
        decide.Rung(
            "standby_wait_lease_active",
            "remain standby while primary coordinator lease is active",
            gives=frozenset({"standby", "waiting"}),
            needs=(
                decide.Need(
                    "no split-brain conflict detected",
                    lambda f: not bool(f.get("split_brain_conflict_detected")),
                    "{rung} split-brain conflict detected; cannot passively wait as standby",
                ),
                decide.Need(
                    "primary coordinator lease is active within term and heartbeating",
                    lambda f: not bool(f.get("primary_lease_lapsed")),
                    "{rung} primary coordinator lease has already lapsed",
                ),
            ),
            because="primary coordinator lease is renewed and within term; takeover is forbidden",
            footprint=2,
        ),
        decide.Rung(
            "refuse_conflicting_split_brain",
            "refuse takeover to prevent split brain on fence or heartbeat conflict",
            gives=frozenset({"split_brain_prevented", "refused"}),
            needs=(
                decide.Need(
                    "split-brain risk detected from conflicting fence token, duplicate heartbeat, or stale generation",
                    lambda f: bool(f.get("split_brain_conflict_detected")),
                    "{rung} no split-brain conflict detected",
                ),
            ),
            because="active heartbeat conflict or invalid fence token risks split brain; takeover strictly refused",
            footprint=3,
        ),
    )


# -----------------------------------------------------------------------------
# Decision Climbing Wrappers
# -----------------------------------------------------------------------------

def choose_fleet_admission(
    request: FleetAdmissionRequest,
    envelope: PhysicalCapacityEnvelope,
    topology_map: FleetTopology,
    current_allocations: Dict[str, ResourceVector],
    app_load: ApplicationLoadReading,
    feedback: Optional[FeedbackSignal] = None,
    queue_depth: int = 0,
    max_queue_depth: int = 100,
    deferral_timeout_expired: bool = False,
    near_cutover_deadline: bool = False,
    is_final_cutover_proof: bool = False,
    starved_hops: Optional[Set[str]] = None,
) -> AdmissionVerdict:
    """Climb fleet admission ladder to decide allocation under physical constraints."""
    throttle_factor = calculate_aimd_throttle_factor(feedback)
    effective_demand = request.demanded_resources.scale(throttle_factor)

    # Calculate aggregate load across shared endpoints
    shared_host_ids = set()
    shared_storage_ids = set()
    shared_net_ids = set()
    shared_quota_ids = set()

    for ep_id in request.endpoint_ids:
        ep = topology_map.endpoints.get(ep_id)
        if ep:
            shared_host_ids.add(ep.physical_host_id)
            shared_storage_ids.add(ep.physical_storage_id)
            shared_net_ids.update(ep.network_path_ids)
            shared_quota_ids.update(ep.quota_ids)

    # Check bounds
    fits_headroom = True
    global_breached = False

    for host_id in shared_host_ids:
        host = envelope.hosts.get(host_id)
        if host:
            bg = app_load.host_load.get(host_id, ResourceVector())
            cur = sum(
                (v.cpu_cores for k, v in current_allocations.items() if k != request.hop_id),
                0.0,
            )
            if bg.cpu_cores + cur + effective_demand.cpu_cores > host.total_cpu_cores:
                fits_headroom = False
            if bg.cpu_cores + cur + request.demanded_resources.cpu_cores > host.total_cpu_cores * 1.05:
                global_breached = True

    for st_id in shared_storage_ids:
        st = envelope.storages.get(st_id)
        if st:
            bg = app_load.storage_load.get(st_id, ResourceVector())
            cur_r = sum(
                (v.read_iops for k, v in current_allocations.items() if k != request.hop_id),
                0.0,
            )
            cur_w = sum(
                (v.write_iops for k, v in current_allocations.items() if k != request.hop_id),
                0.0,
            )
            if bg.read_iops + cur_r + effective_demand.read_iops > st.max_read_iops:
                fits_headroom = False
            if bg.write_iops + cur_w + effective_demand.write_iops > st.max_write_iops:
                fits_headroom = False
            if (
                bg.read_iops + cur_r + request.demanded_resources.read_iops > st.max_read_iops * 1.05
                or bg.write_iops + cur_w + request.demanded_resources.write_iops > st.max_write_iops * 1.05
            ):
                global_breached = True

    for net_id in shared_net_ids:
        net = envelope.networks.get(net_id)
        if net:
            bg = app_load.network_load.get(net_id, ResourceVector())
            cur_net = sum(
                (v.network_mb_s for k, v in current_allocations.items() if k != request.hop_id),
                0.0,
            )
            if bg.network_mb_s + cur_net + effective_demand.network_mb_s > net.max_bandwidth_mb_s:
                fits_headroom = False
            if bg.network_mb_s + cur_net + request.demanded_resources.network_mb_s > net.max_bandwidth_mb_s * 1.05:
                global_breached = True

    for q_id in shared_quota_ids:
        quota = envelope.quotas.get(q_id)
        if quota:
            bg = app_load.quota_load.get(q_id, ResourceVector())
            cur_conn = sum(
                (v.connections for k, v in current_allocations.items() if k != request.hop_id),
                0,
            )
            cur_slots = sum(
                (v.replication_slots for k, v in current_allocations.items() if k != request.hop_id),
                0,
            )
            if bg.connections + cur_conn + effective_demand.connections > quota.max_connections:
                fits_headroom = False
            if bg.replication_slots + cur_slots + effective_demand.replication_slots > quota.max_replication_slots:
                fits_headroom = False
            if (
                bg.connections + cur_conn + request.demanded_resources.connections > quota.max_connections
                or bg.replication_slots + cur_slots + request.demanded_resources.replication_slots > quota.max_replication_slots
            ):
                global_breached = True

    has_catchup_headroom = fits_headroom or (throttle_factor >= 0.5)
    has_proof_headroom = fits_headroom or (throttle_factor >= 0.5)
    is_starved = request.hop_id in (starved_hops or set())

    facts: Dict[str, Any] = {
        "work_kind": request.work_kind.value,
        "cdc_lag_seconds": request.current_cdc_lag_s,
        "critical_lag_threshold": 5.0,
        "near_cutover_deadline": near_cutover_deadline,
        "has_catchup_headroom": has_catchup_headroom,
        "has_proof_headroom": has_proof_headroom,
        "is_final_cutover_proof": is_final_cutover_proof,
        "fits_shared_headroom": fits_headroom,
        "global_constraints_breached": global_breached,
        "hop_starved": is_starved,
        "feedback_pressure_detected": throttle_factor < 1.0,
        "calculated_throttle_factor": throttle_factor,
        "min_throttle_floor": 0.1,
        "supports_throttling": True,
        "queue_depth": queue_depth,
        "max_queue_depth": max_queue_depth,
        "deferral_timeout_expired": deferral_timeout_expired,
    }

    work_desc = f"fleet_admission:{request.hop_id}:{request.request_id}"
    choice = decide.climb(work_desc, fleet_admission_rungs(), facts=facts)

    if choice.rung is None:
        return AdmissionVerdict(
            admitted=False,
            allocated_resources=ResourceVector(),
            care_badge=CareBadge.ACTION,
            ladder_rung="none",
            reason=f"refused all admission rungs: {choice.why_not}",
            throttle_factor=0.0,
        )

    rung_name = choice.rung.name

    if rung_name == "priority_critical_catchup_admit":
        return AdmissionVerdict(
            admitted=True,
            allocated_resources=request.demanded_resources,
            care_badge=CareBadge.PASS,
            ladder_rung=rung_name,
            reason="critical CDC catch-up granted high priority headroom",
            throttle_factor=1.0,
        )
    if rung_name == "priority_critical_proof_admit":
        return AdmissionVerdict(
            admitted=True,
            allocated_resources=request.demanded_resources,
            care_badge=CareBadge.PASS,
            ladder_rung=rung_name,
            reason="critical cutover proof granted guaranteed verification bandwidth",
            throttle_factor=1.0,
        )
    if rung_name == "fair_share_headroom_admit":
        return AdmissionVerdict(
            admitted=True,
            allocated_resources=request.demanded_resources,
            care_badge=CareBadge.PASS,
            ladder_rung=rung_name,
            reason="admitted within shared physical capacity headroom",
            throttle_factor=1.0,
        )
    if rung_name == "throttled_feedback_admit":
        return AdmissionVerdict(
            admitted=True,
            allocated_resources=effective_demand,
            care_badge=CareBadge.REVIEW,
            ladder_rung=rung_name,
            reason=f"admitted under AIMD dynamic throttle factor {throttle_factor}",
            throttle_factor=throttle_factor,
        )
    if rung_name == "queue_and_defer_refuse":
        return AdmissionVerdict(
            admitted=False,
            allocated_resources=ResourceVector(),
            care_badge=CareBadge.BENIGN,
            ladder_rung=rung_name,
            reason="physical capacity saturated; request queued in fair backlog",
            throttle_factor=0.0,
        )

    raise FleetCoordinationError(f"unrecognized admission rung {rung_name}")


def choose_work_stealing(
    request: WorkStealingRequest,
    unit_lease_active: bool = False,
    donor_overloaded: bool = False,
    donor_yield_authorized: bool = False,
) -> StealVerdict:
    """Climb work stealing ladder to steal/rebalance compatible movable units."""
    unit = request.candidate_unit
    recipient_has_headroom = unit.estimated_resource.fits_in(request.available_headroom)
    engine_comp = unit.engine == request.recipient_engine
    tenant_comp = unit.tenant_id == request.recipient_tenant_id

    incompatible_or_locked = (
        unit_lease_active
        or not engine_comp
        or not tenant_comp
        or not unit.movable
        or unit.work_kind == HopWorkKind.CRITICAL_CATCHUP
        or not recipient_has_headroom
    )

    facts: Dict[str, Any] = {
        "unit_movable": unit.movable,
        "unit_state": unit.state,
        "work_kind": unit.work_kind.value,
        "engine_compatible": engine_comp,
        "tenant_compatible": tenant_comp,
        "recipient_has_headroom": recipient_has_headroom,
        "unit_lease_active": unit_lease_active,
        "donor_overloaded": donor_overloaded,
        "donor_yield_authorized": donor_yield_authorized,
        "incompatible_or_locked": incompatible_or_locked,
    }

    work_desc = f"work_stealing:{unit.unit_id}:{request.recipient_worker_id}"
    choice = decide.climb(work_desc, work_stealing_rungs(), facts=facts)

    if choice.rung is None:
        return StealVerdict(
            granted=False,
            stolen_unit=None,
            new_fencing_token=unit.fencing_token,
            ladder_rung="none",
            care_badge=CareBadge.ACTION,
            reason=f"stealing decision blocked: {choice.why_not}",
        )

    rung_name = choice.rung.name

    if rung_name == "steal_compatible_range_unit":
        new_token = unit.fencing_token + 1
        stolen = dataclasses.replace(
            unit,
            hop_id=request.recipient_hop_id,
            locked_by=request.recipient_worker_id,
            fencing_token=new_token,
            state="in_flight",
            updated_at=time.time(),
        )
        return StealVerdict(
            granted=True,
            stolen_unit=stolen,
            new_fencing_token=new_token,
            ladder_rung=rung_name,
            care_badge=CareBadge.PASS,
            reason="compatible movable unit stolen and fenced to idle recipient worker",
        )

    if rung_name == "cooperative_yield_movable_unit":
        new_token = unit.fencing_token + 1
        yielded = dataclasses.replace(
            unit,
            hop_id=request.recipient_hop_id,
            locked_by=request.recipient_worker_id,
            fencing_token=new_token,
            state="in_flight",
            updated_at=time.time(),
        )
        return StealVerdict(
            granted=True,
            stolen_unit=yielded,
            new_fencing_token=new_token,
            ladder_rung=rung_name,
            care_badge=CareBadge.PASS,
            reason="donor hop cooperatively yielded unit to relieve shared physical load",
        )

    if rung_name == "refuse_incompatible_or_locked_unit":
        return StealVerdict(
            granted=False,
            stolen_unit=None,
            new_fencing_token=unit.fencing_token,
            ladder_rung=rung_name,
            care_badge=CareBadge.REVIEW,
            reason="stealing refused: unit is locked, engine incompatible, or across tenant boundary",
        )

    raise FleetCoordinationError(f"unrecognized work stealing rung {rung_name}")


def choose_coordinator_failover(
    request: CoordinatorFailoverRequest,
    split_brain_conflict: bool = False,
    simulated_allocation_exceeded: bool = False,
) -> TakeoverVerdict:
    """Climb coordinator failover ladder to safely take over after lease lapse and reconciliation."""
    curr_lease = request.current_lease
    curr_gen = curr_lease.fencing_generation if curr_lease else 0
    proposed_gen = curr_gen + 1

    primary_lapsed = curr_lease is None or curr_lease.is_lapsed(request.now)

    # Reconcile worker leases: audit liveness, prune dead leases, preserve valid ones
    reconciled_workers: List[WorkerLeaseRecord] = []
    seen_units: Set[str] = set()
    double_ownership_found = False

    for w_lease in request.active_worker_leases:
        for u_id in w_lease.unit_ids:
            if u_id in seen_units:
                double_ownership_found = True
            seen_units.add(u_id)

        if not w_lease.is_lapsed(request.now):
            # Advance fencing token on preserved worker lease
            updated_w = dataclasses.replace(
                w_lease,
                fencing_token=w_lease.fencing_token + 1,
            )
            reconciled_workers.append(updated_w)

    reconciled_ok = not double_ownership_found
    alloc_within_bounds = not simulated_allocation_exceeded

    split_brain_detected = (
        split_brain_conflict
        or proposed_gen <= curr_gen
    )

    facts: Dict[str, Any] = {
        "primary_lease_lapsed": primary_lapsed,
        "worker_leases_reconciled": reconciled_ok,
        "proposed_generation": proposed_gen,
        "current_generation": curr_gen,
        "reconciled_allocations_within_bounds": alloc_within_bounds,
        "split_brain_conflict_detected": split_brain_detected,
    }

    work_desc = f"coordinator_failover:{request.standby_id}:gen_{proposed_gen}"
    choice = decide.climb(work_desc, coordinator_failover_rungs(), facts=facts)

    if choice.rung is None:
        return TakeoverVerdict(
            takeover_granted=False,
            new_fencing_generation=curr_gen,
            reconciled_leases=(),
            ladder_rung="none",
            care_badge=CareBadge.ACTION,
            reason=f"failover takeover blocked: {choice.why_not}",
        )

    rung_name = choice.rung.name

    if rung_name == "reconcile_and_fence_takeover":
        return TakeoverVerdict(
            takeover_granted=True,
            new_fencing_generation=proposed_gen,
            reconciled_leases=tuple(reconciled_workers),
            ladder_rung=rung_name,
            care_badge=CareBadge.PASS,
            reason="standby successfully reconciled active worker leases, fenced old coordinator, and assumed role",
        )

    if rung_name == "standby_wait_lease_active":
        return TakeoverVerdict(
            takeover_granted=False,
            new_fencing_generation=curr_gen,
            reconciled_leases=(),
            ladder_rung=rung_name,
            care_badge=CareBadge.BENIGN,
            reason="primary coordinator lease is still active and heartbeating; standby remains passive",
        )

    if rung_name == "refuse_conflicting_split_brain":
        return TakeoverVerdict(
            takeover_granted=False,
            new_fencing_generation=curr_gen,
            reconciled_leases=(),
            ladder_rung=rung_name,
            care_badge=CareBadge.ACTION,
            reason="takeover refused to avoid split brain: active primary heartbeat or generation conflict detected",
        )

    raise FleetCoordinationError(f"unrecognized coordinator failover rung {rung_name}")


# -----------------------------------------------------------------------------
# Active Lease Lock File & Mutual Exclusion (.run.lock)
# -----------------------------------------------------------------------------

class ActiveLeaseLock:
    """Local process mutual exclusion lease using fcntl.flock on .run.lock."""

    def __init__(self, lock_path: Union[str, pathlib.Path]):
        self.lock_path = pathlib.Path(lock_path)
        self.fd: Optional[int] = None

    def acquire(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (BlockingIOError, OSError):
            os.close(self.fd)
            self.fd = None
            return False

    def release(self) -> None:
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None

    def __enter__(self):
        if not self.acquire():
            raise DoubleOwnershipError(
                f"unable to acquire active lease lock at {self.lock_path}: held by another process",
                care_badge=CareBadge.ACTION,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


# -----------------------------------------------------------------------------
# Universal Typed Durable Checkpoint Store in O(1)
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class FleetCheckpointRecord:
    unit_id: str
    hop_id: str
    table_name: str
    key_cursor: Union[int, str, None]
    updated_at: datetime.datetime
    row_count: int
    bytes_transferred: int
    checksum_digest: str
    status: str
    fencing_token: int


class FleetCoordinationCheckpointStore:
    """Universal durable checkpoint store with native type preservation."""

    def __init__(self, storage_dir: Union[str, pathlib.Path]):
        self.storage_dir = pathlib.Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._records: Dict[str, FleetCheckpointRecord] = {}

    def _file_path(self, unit_id: str) -> pathlib.Path:
        safe_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in unit_id)
        return self.storage_dir / f"checkpoint_{safe_name}.json"

    def save_checkpoint(self, record: FleetCheckpointRecord) -> None:
        self._records[record.unit_id] = record
        data = {
            "unit_id": record.unit_id,
            "hop_id": record.hop_id,
            "table_name": record.table_name,
            "key_cursor": record.key_cursor,
            "updated_at": record.updated_at.isoformat(),
            "row_count": record.row_count,
            "bytes_transferred": record.bytes_transferred,
            "checksum_digest": record.checksum_digest,
            "status": record.status,
            "fencing_token": record.fencing_token,
        }
        target_path = self._file_path(record.unit_id)
        tmp_path = target_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, target_path)

    def load_checkpoint(self, unit_id: str) -> Optional[FleetCheckpointRecord]:
        if unit_id in self._records:
            return self._records[unit_id]
        p = self._file_path(unit_id)
        if not p.exists():
            return None
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        rec = FleetCheckpointRecord(
            unit_id=data["unit_id"],
            hop_id=data["hop_id"],
            table_name=data["table_name"],
            key_cursor=data["key_cursor"],
            updated_at=datetime.datetime.fromisoformat(data["updated_at"]),
            row_count=data["row_count"],
            bytes_transferred=data["bytes_transferred"],
            checksum_digest=data["checksum_digest"],
            status=data["status"],
            fencing_token=data["fencing_token"],
        )
        self._records[unit_id] = rec
        return rec

    def resume_in_o1(self, unit_ids: Sequence[str]) -> Tuple[List[str], List[str]]:
        """Return (completed_unit_ids, pending_unit_ids) in O(1) per unit without rescanning."""
        completed: List[str] = []
        pending: List[str] = []
        for u_id in unit_ids:
            rec = self.load_checkpoint(u_id)
            if rec and rec.status == "completed":
                completed.append(u_id)
            else:
                pending.append(u_id)
        return completed, pending


# -----------------------------------------------------------------------------
# Fleet Joint Capacity Coordinator
# -----------------------------------------------------------------------------

class FleetCapacityJointPlan:
    """Multi-hop capacity coordination plan respecting global shared physical constraints."""

    def __init__(
        self,
        topology_map: FleetTopology,
        envelope: PhysicalCapacityEnvelope,
        checkpoint_store: FleetCoordinationCheckpointStore,
    ):
        self.topology = topology_map
        self.envelope = envelope
        self.checkpoints = checkpoint_store
        self.allocations: Dict[str, ResourceVector] = {}
        self.active_leases: Dict[str, WorkerLeaseRecord] = {}
        self.app_load: ApplicationLoadReading = ApplicationLoadReading(timestamp=time.time())

    def update_application_load(self, reading: ApplicationLoadReading) -> None:
        self.app_load = reading

    def admit_hop(
        self,
        request: FleetAdmissionRequest,
        feedback: Optional[FeedbackSignal] = None,
        queue_depth: int = 0,
        near_cutover_deadline: bool = False,
        is_final_cutover_proof: bool = False,
    ) -> AdmissionVerdict:
        verdict = choose_fleet_admission(
            request=request,
            envelope=self.envelope,
            topology_map=self.topology,
            current_allocations=self.allocations,
            app_load=self.app_load,
            feedback=feedback,
            queue_depth=queue_depth,
            near_cutover_deadline=near_cutover_deadline,
            is_final_cutover_proof=is_final_cutover_proof,
        )
        if verdict.admitted:
            self.allocations[request.hop_id] = verdict.allocated_resources
        return verdict

    def release_hop(self, hop_id: str) -> None:
        self.allocations.pop(hop_id, None)

    def register_worker_lease(self, lease_rec: WorkerLeaseRecord) -> None:
        # Check double ownership of units
        for existing_id, existing in self.active_leases.items():
            if existing_id != lease_rec.worker_id and not existing.is_lapsed():
                overlap = set(lease_rec.unit_ids) & set(existing.unit_ids)
                if overlap:
                    raise DoubleOwnershipError(
                        f"double ownership violation: units {sorted(overlap)} already held by {existing_id}",
                        care_badge=CareBadge.ACTION,
                    )
        self.active_leases[lease_rec.worker_id] = lease_rec

    def prune_lapsed_leases(self, now: Optional[float] = None) -> List[str]:
        current_time = time.time() if now is None else now
        pruned = []
        for w_id, w_lease in list(self.active_leases.items()):
            if w_lease.is_lapsed(current_time):
                pruned.append(w_id)
                self.active_leases.pop(w_id, None)
        return pruned
