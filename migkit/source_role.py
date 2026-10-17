"""Source role and replica valid snapshot/position discovery and routing (Row 623: RCH15).

Discovers primary and replica roles, replication lineage, portable positions
(PostgreSQL LSN, MySQL GTID/binlog, MSSQL LSN), slot failover support, and
replay lag. Decides safe reading locations for assessment, snapshot, CDC,
and verification workloads without data loss.

Includes replica failover and lag in proof freshness and cost calculation,
avoiding blanket source-load optimization. Prevents change skipping and
incompatible timeline comparisons across failovers, and dynamically
invalidates affected decisions upon role changes.

UNIX style only. Zero emojis or decorative glyphs.
Badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import copy
import dataclasses
import enum
import fcntl
import hashlib
import json
import logging
import os
import re
import socket
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Care Badges and Status Enums
# -----------------------------------------------------------------------------

class CareBadge(str, enum.Enum):
    PASS = "PASS"
    BENIGN = "BENIGN"
    REVIEW = "REVIEW"
    ACTION = "ACTION"


class NodeRole(str, enum.Enum):
    PRIMARY = "PRIMARY"
    REPLICA = "REPLICA"
    STANDBY = "STANDBY"
    PROMOTED_PRIMARY = "PROMOTED_PRIMARY"
    DEMOTED_REPLICA = "DEMOTED_REPLICA"
    UNKNOWN = "UNKNOWN"


class EngineDialect(str, enum.Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MARIADB = "mariadb"
    MSSQL = "mssql"
    ORACLE = "oracle"
    GENERIC = "generic"


class WorkloadStage(str, enum.Enum):
    ASSESSMENT = "ASSESSMENT"
    SNAPSHOT = "SNAPSHOT"
    CDC = "CDC"
    VERIFICATION = "VERIFICATION"


class SlotFailoverStatus(str, enum.Enum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    UNSUPPORTED = "UNSUPPORTED"
    DEGRADED = "DEGRADED"


class ReplayState(str, enum.Enum):
    STREAMING = "STREAMING"
    PAUSED = "PAUSED"
    CATCHING_UP = "CATCHING_UP"
    DISCONNECTED = "DISCONNECTED"
    STALLED = "STALLED"
    UNKNOWN = "UNKNOWN"


class TimelineStatus(str, enum.Enum):
    MATCHES_PRIMARY = "MATCHES_PRIMARY"
    DIVERGED = "DIVERGED"
    AHEAD = "AHEAD"
    UNKNOWN = "UNKNOWN"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class SourceRoleError(Exception):
    """Base exception for source role discovery and routing."""
    pass


class InvalidatedDecisionError(SourceRoleError):
    """Raised when an operation attempts to use an invalidated decision."""
    pass


class TimelineDivergenceError(SourceRoleError):
    """Raised when a replica or failover node diverged from primary timeline."""
    pass


class ChangeSkipError(SourceRoleError):
    """Raised when a gap exists between snapshot end and tail start positions."""
    pass


class UnsafeWorkloadReadError(SourceRoleError):
    """Raised when a workload cannot be safely read from the requested node."""
    pass


class LeaseAcquisitionError(SourceRoleError):
    """Raised when active verification lease cannot be acquired."""
    pass


# -----------------------------------------------------------------------------
# Portable Positions and Timeline Tracking
# -----------------------------------------------------------------------------

def parse_lsn_int(lsn_str_or_int: Union[str, int]) -> int:
    """Parses Postgres or standard LSN to integer bytes."""
    if isinstance(lsn_str_or_int, int):
        return lsn_str_or_int
    text = str(lsn_str_or_int).strip()
    if "/" in text:
        hi, _, lo = text.partition("/")
        return (int(hi, 16) << 32) | int(lo or "0", 16)
    if text.startswith("0x") or text.startswith("0X"):
        return int(text, 16)
    return int(text)


def format_lsn_str(n: int) -> str:
    """Formats integer bytes to standard Postgres hex LSN X/Y."""
    n = int(n)
    return f"{n >> 32:X}/{n & 0xFFFFFFFF:X}"


def parse_gtid_intervals(gtid_str: str) -> Dict[str, List[Tuple[int, int]]]:
    """Parses MySQL GTID set string (e.g. 'uuid1:1-10:15-20,uuid2:1-5') into intervals."""
    result: Dict[str, List[Tuple[int, int]]] = {}
    if not gtid_str or not gtid_str.strip():
        return result
    for server_part in gtid_str.strip().split(","):
        server_part = server_part.strip()
        if not server_part:
            continue
        parts = server_part.split(":")
        if len(parts) < 2:
            continue
        sid = parts[0].strip().lower()
        intervals: List[Tuple[int, int]] = []
        for interval_str in parts[1:]:
            interval_str = interval_str.strip()
            if not interval_str:
                continue
            if "-" in interval_str:
                start_s, _, end_s = interval_str.partition("-")
                try:
                    intervals.append((int(start_s), int(end_s)))
                except ValueError:
                    pass
            else:
                try:
                    val = int(interval_str)
                    intervals.append((val, val))
                except ValueError:
                    pass
        if intervals:
            # Normalize and merge intervals
            intervals.sort(key=lambda x: x[0])
            merged: List[Tuple[int, int]] = []
            for start, end in intervals:
                if not merged:
                    merged.append((start, end))
                else:
                    prev_start, prev_end = merged[-1]
                    if start <= prev_end + 1:
                        merged[-1] = (prev_start, max(prev_end, end))
                    else:
                        merged.append((start, end))
            result[sid] = merged
    return result


def gtid_total_tx_count(intervals_map: Dict[str, List[Tuple[int, int]]]) -> int:
    """Computes total transactions executed across all GTID intervals."""
    total = 0
    for sid, intervals in intervals_map.items():
        for start, end in intervals:
            total += max(0, end - start + 1)
    return total


def is_gtid_subset(subset: Dict[str, List[Tuple[int, int]]], superset: Dict[str, List[Tuple[int, int]]]) -> bool:
    """Returns True if subset is completely contained within superset."""
    for sid, sub_intervals in subset.items():
        if sid not in superset:
            return False
        sup_intervals = superset[sid]
        for sub_start, sub_end in sub_intervals:
            # Check if [sub_start, sub_end] is covered by any interval in sup_intervals
            covered = any(sup_start <= sub_start and sub_end <= sup_end for sup_start, sup_end in sup_intervals)
            if not covered:
                return False
    return True


@dataclasses.dataclass(frozen=True)
class PortablePosition:
    """Engine-agnostic portable replication position with timeline tracking."""
    engine: EngineDialect
    byte_offset: int
    raw_position: str
    timeline_id: int = 1
    gtid_intervals: Dict[str, List[Tuple[int, int]]] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_postgres(cls, lsn: Union[str, int], timeline_id: int = 1) -> "PortablePosition":
        val_int = parse_lsn_int(lsn)
        val_str = format_lsn_str(val_int)
        return cls(
            engine=EngineDialect.POSTGRESQL,
            byte_offset=val_int,
            raw_position=val_str,
            timeline_id=int(timeline_id),
        )

    @classmethod
    def from_mysql(
        cls,
        gtid_set: str = "",
        binlog_file: str = "",
        binlog_pos: int = 0,
        timeline_id: int = 1,
    ) -> "PortablePosition":
        intervals = parse_gtid_intervals(gtid_set)
        tx_count = gtid_total_tx_count(intervals)
        raw = gtid_set.strip() if gtid_set.strip() else f"{binlog_file}:{binlog_pos}"
        return cls(
            engine=EngineDialect.MYSQL,
            byte_offset=binlog_pos if not tx_count else tx_count,
            raw_position=raw,
            timeline_id=int(timeline_id),
            gtid_intervals=intervals,
        )

    @classmethod
    def from_generic(cls, pos: int, raw_str: str = "", timeline_id: int = 1) -> "PortablePosition":
        return cls(
            engine=EngineDialect.GENERIC,
            byte_offset=int(pos),
            raw_position=raw_str or str(pos),
            timeline_id=int(timeline_id),
        )

    def is_compatible_timeline(self, other: "PortablePosition") -> bool:
        """Checks if two positions reside on the exact same timeline."""
        return self.timeline_id == other.timeline_id

    def distance_to(self, other: "PortablePosition") -> int:
        """Returns distance from self to other. Positive if other is ahead."""
        if not self.is_compatible_timeline(other):
            raise TimelineDivergenceError(
                f"cannot compute position distance across divergent timelines: {self.timeline_id} vs {other.timeline_id}"
            )
        if self.engine == EngineDialect.MYSQL and self.gtid_intervals and other.gtid_intervals:
            self_tx = gtid_total_tx_count(self.gtid_intervals)
            other_tx = gtid_total_tx_count(other.gtid_intervals)
            return other_tx - self_tx
        return other.byte_offset - self.byte_offset

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, PortablePosition):
            return NotImplemented
        if self.timeline_id != other.timeline_id:
            return self.timeline_id < other.timeline_id
        if self.engine == EngineDialect.MYSQL and self.gtid_intervals and other.gtid_intervals:
            return is_gtid_subset(self.gtid_intervals, other.gtid_intervals) and self.gtid_intervals != other.gtid_intervals
        return self.byte_offset < other.byte_offset

    def __le__(self, other: Any) -> bool:
        return self == other or self < other

    def __gt__(self, other: Any) -> bool:
        if not isinstance(other, PortablePosition):
            return NotImplemented
        return other < self

    def __ge__(self, other: Any) -> bool:
        return self == other or self > other

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PortablePosition):
            return False
        if self.timeline_id != other.timeline_id or self.engine != other.engine:
            return False
        if self.engine == EngineDialect.MYSQL and self.gtid_intervals and other.gtid_intervals:
            return self.gtid_intervals == other.gtid_intervals
        return self.byte_offset == other.byte_offset

    def to_dict(self) -> Dict[str, Any]:
        return {
            "engine": self.engine.value,
            "byte_offset": self.byte_offset,
            "raw_position": self.raw_position,
            "timeline_id": self.timeline_id,
            "gtid_intervals": {k: [list(pair) for pair in v] for k, v in self.gtid_intervals.items()},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PortablePosition":
        gtid_data = data.get("gtid_intervals") or {}
        normalized_gtid = {k: [(p[0], p[1]) for p in v] for k, v in gtid_data.items()}
        return cls(
            engine=EngineDialect(data.get("engine", "generic")),
            byte_offset=int(data.get("byte_offset", 0)),
            raw_position=str(data.get("raw_position", "")),
            timeline_id=int(data.get("timeline_id", 1)),
            gtid_intervals=normalized_gtid,
        )


@dataclasses.dataclass
class TimelineBranch:
    """Historical timeline branch switch point."""
    timeline_id: int
    parent_timeline_id: Optional[int]
    switch_position: Optional[PortablePosition]
    switch_timestamp: float = dataclasses.field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timeline_id": self.timeline_id,
            "parent_timeline_id": self.parent_timeline_id,
            "switch_position": self.switch_position.to_dict() if self.switch_position else None,
            "switch_timestamp": self.switch_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TimelineBranch":
        pos_data = data.get("switch_position")
        return cls(
            timeline_id=int(data["timeline_id"]),
            parent_timeline_id=int(data["parent_timeline_id"]) if data.get("parent_timeline_id") is not None else None,
            switch_position=PortablePosition.from_dict(pos_data) if pos_data else None,
            switch_timestamp=float(data.get("switch_timestamp", 0.0)),
        )


# -----------------------------------------------------------------------------
# Node Endpoint and Cluster Topology
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class NodeEndpoint:
    """Discovered node state, replication lineage, and replication capabilities."""
    node_id: str
    host: str
    port: int
    role: NodeRole
    engine: EngineDialect
    cluster_id: str
    timeline_id: int = 1
    upstream_node_id: Optional[str] = None
    is_read_only: bool = False
    current_position: Optional[PortablePosition] = None
    replay_position: Optional[PortablePosition] = None
    replay_lag_bytes: int = 0
    replay_lag_seconds: float = 0.0
    replay_state: ReplayState = ReplayState.STREAMING
    slot_failover_status: SlotFailoverStatus = SlotFailoverStatus.DISABLED
    supports_replica_cdc: bool = False
    supports_snapshot_export: bool = True
    last_seen_timestamp: float = dataclasses.field(default_factory=time.time)
    generation: int = 1

    def is_primary(self) -> bool:
        return self.role in (NodeRole.PRIMARY, NodeRole.PROMOTED_PRIMARY)

    def is_replica(self) -> bool:
        return self.role in (NodeRole.REPLICA, NodeRole.STANDBY, NodeRole.DEMOTED_REPLICA)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "role": self.role.value,
            "engine": self.engine.value,
            "cluster_id": self.cluster_id,
            "timeline_id": self.timeline_id,
            "upstream_node_id": self.upstream_node_id,
            "is_read_only": self.is_read_only,
            "current_position": self.current_position.to_dict() if self.current_position else None,
            "replay_position": self.replay_position.to_dict() if self.replay_position else None,
            "replay_lag_bytes": self.replay_lag_bytes,
            "replay_lag_seconds": self.replay_lag_seconds,
            "replay_state": self.replay_state.value,
            "slot_failover_status": self.slot_failover_status.value,
            "supports_replica_cdc": self.supports_replica_cdc,
            "supports_snapshot_export": self.supports_snapshot_export,
            "last_seen_timestamp": self.last_seen_timestamp,
            "generation": self.generation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeEndpoint":
        cur_pos = data.get("current_position")
        rep_pos = data.get("replay_position")
        return cls(
            node_id=str(data["node_id"]),
            host=str(data["host"]),
            port=int(data["port"]),
            role=NodeRole(data.get("role", "UNKNOWN")),
            engine=EngineDialect(data.get("engine", "generic")),
            cluster_id=str(data.get("cluster_id", "default")),
            timeline_id=int(data.get("timeline_id", 1)),
            upstream_node_id=data.get("upstream_node_id"),
            is_read_only=bool(data.get("is_read_only", False)),
            current_position=PortablePosition.from_dict(cur_pos) if cur_pos else None,
            replay_position=PortablePosition.from_dict(rep_pos) if rep_pos else None,
            replay_lag_bytes=int(data.get("replay_lag_bytes", 0)),
            replay_lag_seconds=float(data.get("replay_lag_seconds", 0.0)),
            replay_state=ReplayState(data.get("replay_state", "STREAMING")),
            slot_failover_status=SlotFailoverStatus(data.get("slot_failover_status", "DISABLED")),
            supports_replica_cdc=bool(data.get("supports_replica_cdc", False)),
            supports_snapshot_export=bool(data.get("supports_snapshot_export", True)),
            last_seen_timestamp=float(data.get("last_seen_timestamp", 0.0)),
            generation=int(data.get("generation", 1)),
        )


@dataclasses.dataclass
class TopologySnapshot:
    """Full cluster replication topology snapshot."""
    cluster_id: str
    primary_node_id: Optional[str]
    nodes: Dict[str, NodeEndpoint] = dataclasses.field(default_factory=dict)
    timeline_branches: List[TimelineBranch] = dataclasses.field(default_factory=list)
    generation: int = 1
    updated_at: float = dataclasses.field(default_factory=time.time)

    def get_primary(self) -> Optional[NodeEndpoint]:
        if self.primary_node_id and self.primary_node_id in self.nodes:
            return self.nodes[self.primary_node_id]
        for node in self.nodes.values():
            if node.is_primary():
                return node
        return None

    def get_replicas(self) -> List[NodeEndpoint]:
        return [node for node in self.nodes.values() if node.is_replica()]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "primary_node_id": self.primary_node_id,
            "nodes": {nid: n.to_dict() for nid, n in self.nodes.items()},
            "timeline_branches": [b.to_dict() for b in self.timeline_branches],
            "generation": self.generation,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TopologySnapshot":
        nodes = {nid: NodeEndpoint.from_dict(nd) for nid, nd in (data.get("nodes") or {}).items()}
        branches = [TimelineBranch.from_dict(bd) for bd in (data.get("timeline_branches") or [])]
        return cls(
            cluster_id=str(data.get("cluster_id", "default")),
            primary_node_id=data.get("primary_node_id"),
            nodes=nodes,
            timeline_branches=branches,
            generation=int(data.get("generation", 1)),
            updated_at=float(data.get("updated_at", 0.0)),
        )


# -----------------------------------------------------------------------------
# Proof Freshness and Cost Models
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class FreshnessModel:
    """Verification and assessment proof freshness evaluation."""
    lag_seconds: float
    query_duration_sec: float
    total_freshness_age_sec: float
    freshness_sla_sec: float
    is_fresh: bool

    @classmethod
    def evaluate(cls, lag_seconds: float, query_duration_sec: float, freshness_sla_sec: float) -> "FreshnessModel":
        total_age = max(0.0, lag_seconds) + max(0.0, query_duration_sec)
        is_fresh = total_age <= freshness_sla_sec
        return cls(
            lag_seconds=lag_seconds,
            query_duration_sec=query_duration_sec,
            total_freshness_age_sec=total_age,
            freshness_sla_sec=freshness_sla_sec,
            is_fresh=is_fresh,
        )


@dataclasses.dataclass(frozen=True)
class CostModel:
    """Cost-benefit trade-off of reading from replica vs primary."""
    source_primary_headroom_pct: float
    primary_penalty_cost: float
    replica_lag_wait_cost: float
    replica_failover_risk_cost: float
    total_replica_cost: float
    total_primary_cost: float
    is_replica_cost_effective: bool

    @classmethod
    def evaluate(
        cls,
        primary_cpu_pct: float,
        replica_lag_seconds: float,
        slot_failover_status: SlotFailoverStatus,
        workload_duration_estimate_sec: float = 10.0,
    ) -> "CostModel":
        headroom = max(0.0, 100.0 - primary_cpu_pct)
        # If primary has abundant headroom, offloading to replica has lower utility
        primary_penalty = 10.0 if primary_cpu_pct < 60.0 else (primary_cpu_pct - 50.0) * 1.5
        # Lag wait cost scales with lag time
        lag_cost = replica_lag_seconds * 2.0
        # If slot failover is disabled/unsupported, high risk penalty
        failover_risk = 50.0 if slot_failover_status != SlotFailoverStatus.ENABLED else 5.0

        total_replica = lag_cost + failover_risk + (workload_duration_estimate_sec * 0.5)
        total_primary = primary_penalty + 5.0

        is_effective = total_replica < total_primary
        return cls(
            source_primary_headroom_pct=headroom,
            primary_penalty_cost=round(primary_penalty, 2),
            replica_lag_wait_cost=round(lag_cost, 2),
            replica_failover_risk_cost=round(failover_risk, 2),
            total_replica_cost=round(total_replica, 2),
            total_primary_cost=round(total_primary, 2),
            is_replica_cost_effective=is_effective,
        )


# -----------------------------------------------------------------------------
# Workload Routing Decision
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class WorkloadRoutingDecision:
    """Outcome of workload read location decision with safety guarantees."""
    decision_id: str
    stage: WorkloadStage
    target_node_id: str
    fallback_node_id: Optional[str]
    badge: CareBadge
    allowed: bool
    reason: str
    generation: int
    cluster_id: str
    node_role: NodeRole
    timeline_id: int
    watermark_position: Optional[PortablePosition] = None
    snapshot_position: Optional[PortablePosition] = None
    freshness_model: Optional[FreshnessModel] = None
    cost_model: Optional[CostModel] = None
    timestamp: float = dataclasses.field(default_factory=time.time)
    invalidated: bool = False
    invalidation_reason: Optional[str] = None

    def invalidate(self, reason: str) -> None:
        self.invalidated = True
        self.invalidation_reason = reason
        self.allowed = False
        self.badge = CareBadge.ACTION

    def check_valid(self) -> None:
        if self.invalidated:
            raise InvalidatedDecisionError(
                f"decision {self.decision_id} invalidated: {self.invalidation_reason}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "stage": self.stage.value,
            "target_node_id": self.target_node_id,
            "fallback_node_id": self.fallback_node_id,
            "badge": self.badge.value,
            "allowed": self.allowed,
            "reason": self.reason,
            "generation": self.generation,
            "cluster_id": self.cluster_id,
            "node_role": self.node_role.value,
            "timeline_id": self.timeline_id,
            "watermark_position": self.watermark_position.to_dict() if self.watermark_position else None,
            "snapshot_position": self.snapshot_position.to_dict() if self.snapshot_position else None,
            "freshness_model": dataclasses.asdict(self.freshness_model) if self.freshness_model else None,
            "cost_model": dataclasses.asdict(self.cost_model) if self.cost_model else None,
            "timestamp": self.timestamp,
            "invalidated": self.invalidated,
            "invalidation_reason": self.invalidation_reason,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkloadRoutingDecision":
        wm_data = data.get("watermark_position")
        snap_data = data.get("snapshot_position")
        fresh_data = data.get("freshness_model")
        cost_data = data.get("cost_model")
        return cls(
            decision_id=str(data["decision_id"]),
            stage=WorkloadStage(data["stage"]),
            target_node_id=str(data["target_node_id"]),
            fallback_node_id=data.get("fallback_node_id"),
            badge=CareBadge(data.get("badge", "ACTION")),
            allowed=bool(data.get("allowed", False)),
            reason=str(data.get("reason", "")),
            generation=int(data.get("generation", 1)),
            cluster_id=str(data.get("cluster_id", "default")),
            node_role=NodeRole(data.get("node_role", "UNKNOWN")),
            timeline_id=int(data.get("timeline_id", 1)),
            watermark_position=PortablePosition.from_dict(wm_data) if wm_data else None,
            snapshot_position=PortablePosition.from_dict(snap_data) if snap_data else None,
            freshness_model=FreshnessModel(**fresh_data) if fresh_data else None,
            cost_model=CostModel(**cost_data) if cost_data else None,
            timestamp=float(data.get("timestamp", 0.0)),
            invalidated=bool(data.get("invalidated", False)),
            invalidation_reason=data.get("invalidation_reason"),
        )


# -----------------------------------------------------------------------------
# Policy and Routing Coordinator
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class SourceRolePolicy:
    """Operational guardrails for workload offloading to replicas."""
    max_assessment_lag_sec: float = 60.0
    max_snapshot_lag_sec: float = 30.0
    max_snapshot_lag_bytes: int = 64 * 1024 * 1024
    max_cdc_lag_sec: float = 5.0
    max_cdc_lag_bytes: int = 10 * 1024 * 1024
    max_verification_lag_sec: float = 15.0
    verification_freshness_sla_sec: float = 20.0
    require_slot_failover_for_replica_cdc: bool = True
    require_snapshot_export_for_replica_snapshot: bool = True
    require_watermark_fencing_for_verification: bool = True
    primary_cpu_threshold_pct: float = 75.0


class SourceRoleCoordinator:
    """Discovers source roles, decides workload reading location, handles failover."""

    def __init__(
        self,
        cluster_id: str,
        policy: Optional[SourceRolePolicy] = None,
        topology: Optional[TopologySnapshot] = None,
    ):
        self.cluster_id = cluster_id
        self.policy = policy or SourceRolePolicy()
        self.topology = topology or TopologySnapshot(cluster_id=cluster_id, primary_node_id=None)
        self.active_decisions: Dict[str, WorkloadRoutingDecision] = {}
        self._counter = 0

    def _next_decision_id(self, stage: WorkloadStage) -> str:
        self._counter += 1
        return f"DEC-{stage.value}-{self.topology.generation}-{self._counter:04d}"

    def register_node(self, node: NodeEndpoint) -> None:
        """Registers or updates a discovered node in the cluster topology."""
        node.cluster_id = self.cluster_id
        node.generation = self.topology.generation
        self.topology.nodes[node.node_id] = node
        if node.is_primary():
            self.topology.primary_node_id = node.node_id
        self.topology.updated_at = time.time()

    def record_timeline_branch(
        self,
        timeline_id: int,
        parent_timeline_id: Optional[int],
        switch_position: Optional[PortablePosition],
    ) -> None:
        """Records a timeline fork or branch point."""
        branch = TimelineBranch(
            timeline_id=timeline_id,
            parent_timeline_id=parent_timeline_id,
            switch_position=switch_position,
        )
        self.topology.timeline_branches.append(branch)

    # -------------------------------------------------------------------------
    # Workload Evaluation
    # -------------------------------------------------------------------------

    def evaluate_assessment(
        self,
        preferred_node_id: Optional[str] = None,
    ) -> WorkloadRoutingDecision:
        """Decides safe reading location for schema and metadata assessment."""
        primary = self.topology.get_primary()
        dec_id = self._next_decision_id(WorkloadStage.ASSESSMENT)

        target_node: Optional[NodeEndpoint] = None
        if preferred_node_id and preferred_node_id in self.topology.nodes:
            target_node = self.topology.nodes[preferred_node_id]
        elif self.topology.get_replicas():
            # Pick replica with least lag
            target_node = min(self.topology.get_replicas(), key=lambda n: n.replay_lag_seconds)
        else:
            target_node = primary

        if not target_node:
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.ASSESSMENT,
                target_node_id="none",
                fallback_node_id=None,
                badge=CareBadge.ACTION,
                allowed=False,
                reason="no healthy primary or replica discovered for assessment",
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=NodeRole.UNKNOWN,
                timeline_id=1,
            )

        if target_node.is_primary():
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.ASSESSMENT,
                target_node_id=target_node.node_id,
                fallback_node_id=None,
                badge=CareBadge.PASS,
                allowed=True,
                reason="reading assessment directly from primary",
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=target_node.role,
                timeline_id=target_node.timeline_id,
            )

        # Replica evaluation
        if target_node.replay_lag_seconds > self.policy.max_assessment_lag_sec:
            fallback_id = primary.node_id if primary else None
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.ASSESSMENT,
                target_node_id=primary.node_id if primary else target_node.node_id,
                fallback_node_id=fallback_id,
                badge=CareBadge.REVIEW,
                allowed=True if primary else False,
                reason=(
                    f"replica {target_node.node_id} lag {target_node.replay_lag_seconds:.1f}s "
                    f"exceeds max {self.policy.max_assessment_lag_sec:.1f}s; routing assessment to primary"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role if primary else target_node.role,
                timeline_id=primary.timeline_id if primary else target_node.timeline_id,
            )

        decision = WorkloadRoutingDecision(
            decision_id=dec_id,
            stage=WorkloadStage.ASSESSMENT,
            target_node_id=target_node.node_id,
            fallback_node_id=primary.node_id if primary else None,
            badge=CareBadge.BENIGN if target_node.replay_lag_seconds > 0 else CareBadge.PASS,
            allowed=True,
            reason=(
                f"reading assessment from replica {target_node.node_id} "
                f"(lag {target_node.replay_lag_seconds:.1f}s within limit)"
            ),
            generation=self.topology.generation,
            cluster_id=self.cluster_id,
            node_role=target_node.role,
            timeline_id=target_node.timeline_id,
        )
        self.active_decisions[dec_id] = decision
        return decision

    def evaluate_snapshot(
        self,
        preferred_node_id: Optional[str] = None,
        primary_cpu_pct: float = 40.0,
    ) -> WorkloadRoutingDecision:
        """Decides safe reading location for initial table snapshot export."""
        primary = self.topology.get_primary()
        dec_id = self._next_decision_id(WorkloadStage.SNAPSHOT)

        target_node: Optional[NodeEndpoint] = None
        if preferred_node_id and preferred_node_id in self.topology.nodes:
            target_node = self.topology.nodes[preferred_node_id]
        elif self.topology.get_replicas():
            # Choose lowest lag replica
            target_node = min(self.topology.get_replicas(), key=lambda n: n.replay_lag_bytes)
        else:
            target_node = primary

        if not target_node:
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.SNAPSHOT,
                target_node_id="none",
                fallback_node_id=None,
                badge=CareBadge.ACTION,
                allowed=False,
                reason="no target node available for snapshot",
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=NodeRole.UNKNOWN,
                timeline_id=1,
            )

        if target_node.is_primary():
            decision = WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.SNAPSHOT,
                target_node_id=target_node.node_id,
                fallback_node_id=None,
                badge=CareBadge.PASS,
                allowed=True,
                reason="consistent snapshot export on primary",
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=target_node.role,
                timeline_id=target_node.timeline_id,
                snapshot_position=target_node.current_position,
            )
            self.active_decisions[dec_id] = decision
            return decision

        # Replica snapshot safety checks
        # 1. Timeline check
        if primary and target_node.timeline_id != primary.timeline_id:
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.SNAPSHOT,
                target_node_id=primary.node_id,
                fallback_node_id=target_node.node_id,
                badge=CareBadge.ACTION,
                allowed=True,
                reason=(
                    f"replica {target_node.node_id} timeline {target_node.timeline_id} diverged from "
                    f"primary {primary.timeline_id}; cannot export snapshot; routed to primary"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role,
                timeline_id=primary.timeline_id,
                snapshot_position=primary.current_position,
            )

        # 2. Snapshot export capability
        if self.policy.require_snapshot_export_for_replica_snapshot and not target_node.supports_snapshot_export:
            fallback_id = primary.node_id if primary else None
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.SNAPSHOT,
                target_node_id=primary.node_id if primary else target_node.node_id,
                fallback_node_id=fallback_id,
                badge=CareBadge.ACTION,
                allowed=True if primary else False,
                reason=(
                    f"replica {target_node.node_id} lacks verified snapshot export capability; "
                    f"falling back to primary"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role if primary else target_node.role,
                timeline_id=primary.timeline_id if primary else target_node.timeline_id,
                snapshot_position=primary.current_position if primary else None,
            )

        # 3. Replay lag check (both time and bytes)
        if (
            target_node.replay_lag_seconds > self.policy.max_snapshot_lag_sec
            or target_node.replay_lag_bytes > self.policy.max_snapshot_lag_bytes
        ):
            fallback_id = primary.node_id if primary else None
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.SNAPSHOT,
                target_node_id=primary.node_id if primary else target_node.node_id,
                fallback_node_id=fallback_id,
                badge=CareBadge.REVIEW,
                allowed=True if primary else False,
                reason=(
                    f"replica {target_node.node_id} lag ({target_node.replay_lag_seconds:.1f}s, "
                    f"{target_node.replay_lag_bytes}B) exceeds safe snapshot threshold; "
                    f"routed to primary to avoid WAL retention overrun"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role if primary else target_node.role,
                timeline_id=primary.timeline_id if primary else target_node.timeline_id,
                snapshot_position=primary.current_position if primary else None,
            )

        # Cost model evaluation (avoid blanket source-load optimization)
        cost = CostModel.evaluate(
            primary_cpu_pct=primary_cpu_pct,
            replica_lag_seconds=target_node.replay_lag_seconds,
            slot_failover_status=target_node.slot_failover_status,
            workload_duration_estimate_sec=60.0,
        )

        badge = CareBadge.PASS if target_node.replay_lag_seconds == 0 else CareBadge.BENIGN
        decision = WorkloadRoutingDecision(
            decision_id=dec_id,
            stage=WorkloadStage.SNAPSHOT,
            target_node_id=target_node.node_id,
            fallback_node_id=primary.node_id if primary else None,
            badge=badge,
            allowed=True,
            reason=(
                f"snapshot export safe on replica {target_node.node_id} "
                f"(lag {target_node.replay_lag_seconds:.1f}s, position {target_node.replay_position.raw_position if target_node.replay_position else 'unknown'})"
            ),
            generation=self.topology.generation,
            cluster_id=self.cluster_id,
            node_role=target_node.role,
            timeline_id=target_node.timeline_id,
            snapshot_position=target_node.replay_position or target_node.current_position,
            cost_model=cost,
        )
        self.active_decisions[dec_id] = decision
        return decision

    def evaluate_cdc(
        self,
        preferred_node_id: Optional[str] = None,
    ) -> WorkloadRoutingDecision:
        """Decides safe reading location for CDC change streaming."""
        primary = self.topology.get_primary()
        dec_id = self._next_decision_id(WorkloadStage.CDC)

        target_node: Optional[NodeEndpoint] = None
        if preferred_node_id and preferred_node_id in self.topology.nodes:
            target_node = self.topology.nodes[preferred_node_id]
        else:
            # Default to primary for CDC safety unless explicitly configured
            target_node = primary

        if not target_node:
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.CDC,
                target_node_id="none",
                fallback_node_id=None,
                badge=CareBadge.ACTION,
                allowed=False,
                reason="no target node available for CDC tail",
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=NodeRole.UNKNOWN,
                timeline_id=1,
            )

        if target_node.is_primary():
            decision = WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.CDC,
                target_node_id=target_node.node_id,
                fallback_node_id=None,
                badge=CareBadge.PASS,
                allowed=True,
                reason="CDC stream anchored on primary logical replication slot",
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=target_node.role,
                timeline_id=target_node.timeline_id,
                watermark_position=target_node.current_position,
            )
            self.active_decisions[dec_id] = decision
            return decision

        # Replica CDC evaluation (e.g. PG16+ standby logical decoding)
        # 1. Engine replica CDC capability
        if not target_node.supports_replica_cdc:
            fallback_id = primary.node_id if primary else None
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.CDC,
                target_node_id=primary.node_id if primary else target_node.node_id,
                fallback_node_id=fallback_id,
                badge=CareBadge.ACTION,
                allowed=True if primary else False,
                reason=(
                    f"node {target_node.node_id} does not support replica CDC; "
                    f"routed to primary to prevent change loss"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role if primary else target_node.role,
                timeline_id=primary.timeline_id if primary else target_node.timeline_id,
                watermark_position=primary.current_position if primary else None,
            )

        # 2. Slot failover support
        if (
            self.policy.require_slot_failover_for_replica_cdc
            and target_node.slot_failover_status != SlotFailoverStatus.ENABLED
        ):
            fallback_id = primary.node_id if primary else None
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.CDC,
                target_node_id=primary.node_id if primary else target_node.node_id,
                fallback_node_id=fallback_id,
                badge=CareBadge.ACTION,
                allowed=True if primary else False,
                reason=(
                    f"replica {target_node.node_id} slot failover status is {target_node.slot_failover_status.value}; "
                    f"failover would permanently drop slot and lose changes; routed to primary"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role if primary else target_node.role,
                timeline_id=primary.timeline_id if primary else target_node.timeline_id,
                watermark_position=primary.current_position if primary else None,
            )

        # 3. Lag limits for CDC
        if (
            target_node.replay_lag_seconds > self.policy.max_cdc_lag_sec
            or target_node.replay_lag_bytes > self.policy.max_cdc_lag_bytes
        ):
            fallback_id = primary.node_id if primary else None
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.CDC,
                target_node_id=primary.node_id if primary else target_node.node_id,
                fallback_node_id=fallback_id,
                badge=CareBadge.REVIEW,
                allowed=True if primary else False,
                reason=(
                    f"replica CDC lag ({target_node.replay_lag_seconds:.1f}s) exceeds safe threshold "
                    f"{self.policy.max_cdc_lag_sec:.1f}s; routed to primary"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role if primary else target_node.role,
                timeline_id=primary.timeline_id if primary else target_node.timeline_id,
                watermark_position=primary.current_position if primary else None,
            )

        decision = WorkloadRoutingDecision(
            decision_id=dec_id,
            stage=WorkloadStage.CDC,
            target_node_id=target_node.node_id,
            fallback_node_id=primary.node_id if primary else None,
            badge=CareBadge.PASS,
            allowed=True,
            reason=(
                f"replica CDC permitted on {target_node.node_id} with slot failover enabled "
                f"and lag {target_node.replay_lag_seconds:.1f}s"
            ),
            generation=self.topology.generation,
            cluster_id=self.cluster_id,
            node_role=target_node.role,
            timeline_id=target_node.timeline_id,
            watermark_position=target_node.replay_position,
        )
        self.active_decisions[dec_id] = decision
        return decision

    def evaluate_verification(
        self,
        target_watermark: Optional[PortablePosition] = None,
        preferred_node_id: Optional[str] = None,
        query_duration_estimate_sec: float = 5.0,
        primary_cpu_pct: float = 40.0,
    ) -> WorkloadRoutingDecision:
        """Decides safe reading location for verification, diffing, and proof freshness."""
        primary = self.topology.get_primary()
        dec_id = self._next_decision_id(WorkloadStage.VERIFICATION)

        target_node: Optional[NodeEndpoint] = None
        if preferred_node_id and preferred_node_id in self.topology.nodes:
            target_node = self.topology.nodes[preferred_node_id]
        elif self.topology.get_replicas():
            # Candidate replica
            target_node = min(self.topology.get_replicas(), key=lambda n: n.replay_lag_seconds)
        else:
            target_node = primary

        if not target_node:
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.VERIFICATION,
                target_node_id="none",
                fallback_node_id=None,
                badge=CareBadge.ACTION,
                allowed=False,
                reason="no node available for verification read",
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=NodeRole.UNKNOWN,
                timeline_id=1,
            )

        if target_node.is_primary():
            freshness = FreshnessModel.evaluate(
                lag_seconds=0.0,
                query_duration_sec=query_duration_estimate_sec,
                freshness_sla_sec=self.policy.verification_freshness_sla_sec,
            )
            decision = WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.VERIFICATION,
                target_node_id=target_node.node_id,
                fallback_node_id=None,
                badge=CareBadge.PASS,
                allowed=True,
                reason="reading verification directly from primary with zero lag",
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=target_node.role,
                timeline_id=target_node.timeline_id,
                watermark_position=target_node.current_position,
                freshness_model=freshness,
            )
            self.active_decisions[dec_id] = decision
            return decision

        # Replica verification checks
        # 1. Timeline check
        if primary and target_node.timeline_id != primary.timeline_id:
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.VERIFICATION,
                target_node_id=primary.node_id,
                fallback_node_id=target_node.node_id,
                badge=CareBadge.ACTION,
                allowed=True,
                reason=(
                    f"replica {target_node.node_id} timeline {target_node.timeline_id} diverged from primary; "
                    f"cannot compare incompatible timelines; routed to primary"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role,
                timeline_id=primary.timeline_id,
                watermark_position=primary.current_position,
            )

        # 2. Watermark fencing
        if self.policy.require_watermark_fencing_for_verification and target_watermark:
            replica_pos = target_node.replay_position or target_node.current_position
            if replica_pos and replica_pos < target_watermark:
                fallback_id = primary.node_id if primary else None
                return WorkloadRoutingDecision(
                    decision_id=dec_id,
                    stage=WorkloadStage.VERIFICATION,
                    target_node_id=primary.node_id if primary else target_node.node_id,
                    fallback_node_id=fallback_id,
                    badge=CareBadge.ACTION,
                    allowed=True if primary else False,
                    reason=(
                        f"replica {target_node.node_id} position {replica_pos.raw_position} has not replayed up "
                        f"to target watermark {target_watermark.raw_position}; reading would cause false divergence; "
                        f"routed to primary"
                    ),
                    generation=self.topology.generation,
                    cluster_id=self.cluster_id,
                    node_role=primary.role if primary else target_node.role,
                    timeline_id=primary.timeline_id if primary else target_node.timeline_id,
                    watermark_position=primary.current_position if primary else None,
                )

        # 3. Freshness model evaluation
        freshness = FreshnessModel.evaluate(
            lag_seconds=target_node.replay_lag_seconds,
            query_duration_sec=query_duration_estimate_sec,
            freshness_sla_sec=self.policy.verification_freshness_sla_sec,
        )
        if not freshness.is_fresh:
            fallback_id = primary.node_id if primary else None
            return WorkloadRoutingDecision(
                decision_id=dec_id,
                stage=WorkloadStage.VERIFICATION,
                target_node_id=primary.node_id if primary else target_node.node_id,
                fallback_node_id=fallback_id,
                badge=CareBadge.REVIEW,
                allowed=True if primary else False,
                reason=(
                    f"verification freshness age {freshness.total_freshness_age_sec:.1f}s exceeds SLA "
                    f"{freshness.freshness_sla_sec:.1f}s on replica {target_node.node_id}; routed to primary"
                ),
                generation=self.topology.generation,
                cluster_id=self.cluster_id,
                node_role=primary.role if primary else target_node.role,
                timeline_id=primary.timeline_id if primary else target_node.timeline_id,
                watermark_position=primary.current_position if primary else None,
                freshness_model=freshness,
            )

        # 4. Cost model evaluation
        cost = CostModel.evaluate(
            primary_cpu_pct=primary_cpu_pct,
            replica_lag_seconds=target_node.replay_lag_seconds,
            slot_failover_status=target_node.slot_failover_status,
            workload_duration_estimate_sec=query_duration_estimate_sec,
        )

        badge = CareBadge.PASS if target_node.replay_lag_seconds == 0 else CareBadge.BENIGN
        decision = WorkloadRoutingDecision(
            decision_id=dec_id,
            stage=WorkloadStage.VERIFICATION,
            target_node_id=target_node.node_id,
            fallback_node_id=primary.node_id if primary else None,
            badge=badge,
            allowed=True,
            reason=(
                f"verification safe on replica {target_node.node_id} with freshness "
                f"{freshness.total_freshness_age_sec:.1f}s and verified watermark"
            ),
            generation=self.topology.generation,
            cluster_id=self.cluster_id,
            node_role=target_node.role,
            timeline_id=target_node.timeline_id,
            watermark_position=target_node.replay_position or target_node.current_position,
            freshness_model=freshness,
            cost_model=cost,
        )
        self.active_decisions[dec_id] = decision
        return decision

    # -------------------------------------------------------------------------
    # Failover, Timeline Divergence & Change-Skip Verification
    # -------------------------------------------------------------------------

    def notify_role_change(self, node_id: str, new_role: NodeRole) -> List[str]:
        """Notifies coordinator that a node role has changed, invalidating affected decisions."""
        if node_id not in self.topology.nodes:
            return []
        node = self.topology.nodes[node_id]
        old_role = node.role
        if old_role == new_role:
            return []

        node.role = new_role
        self.topology.generation += 1
        node.generation = self.topology.generation
        self.topology.updated_at = time.time()

        if new_role in (NodeRole.PRIMARY, NodeRole.PROMOTED_PRIMARY):
            self.topology.primary_node_id = node_id

        # Invalidate affected decisions
        invalidated_ids: List[str] = []
        for dec_id, dec in list(self.active_decisions.items()):
            if not dec.invalidated and (dec.target_node_id == node_id or dec.generation < self.topology.generation):
                dec.invalidate(
                    f"node {node_id} role changed from {old_role.value} to {new_role.value} (generation bump)"
                )
                invalidated_ids.append(dec_id)

        return invalidated_ids

    def notify_failover(
        self,
        new_primary_id: str,
        old_primary_id: Optional[str] = None,
        new_timeline_id: Optional[int] = None,
        switch_position: Optional[PortablePosition] = None,
    ) -> List[str]:
        """Handles primary failover and timeline advancement, invalidating previous decisions."""
        self.topology.generation += 1
        self.topology.updated_at = time.time()

        if old_primary_id and old_primary_id in self.topology.nodes:
            old_node = self.topology.nodes[old_primary_id]
            old_node.role = NodeRole.DEMOTED_REPLICA
            old_node.generation = self.topology.generation

        if new_primary_id in self.topology.nodes:
            new_node = self.topology.nodes[new_primary_id]
            new_node.role = NodeRole.PROMOTED_PRIMARY
            new_node.generation = self.topology.generation
            if new_timeline_id is not None:
                new_node.timeline_id = new_timeline_id
            self.topology.primary_node_id = new_primary_id

        if new_timeline_id is not None:
            self.record_timeline_branch(
                timeline_id=new_timeline_id,
                parent_timeline_id=new_timeline_id - 1 if new_timeline_id > 1 else None,
                switch_position=switch_position,
            )

        # Invalidate all active decisions across the cluster
        invalidated_ids: List[str] = []
        for dec_id, dec in list(self.active_decisions.items()):
            if not dec.invalidated:
                dec.invalidate(
                    f"cluster failover to new primary {new_primary_id} on timeline {new_timeline_id or 'same'}"
                )
                invalidated_ids.append(dec_id)

        return invalidated_ids

    def check_snapshot_to_tail_continuity(
        self,
        snapshot_position: PortablePosition,
        tail_start_position: PortablePosition,
    ) -> Tuple[bool, CareBadge, str]:
        """Ensures replica lag and primary failover between snapshot and tail cannot skip changes.

        Returns (is_continuous, care_badge, message).
        """
        # 1. Timeline check
        if not snapshot_position.is_compatible_timeline(tail_start_position):
            # Check if tail start position is on a child timeline that branched after snapshot position
            for branch in self.topology.timeline_branches:
                if (
                    branch.timeline_id == tail_start_position.timeline_id
                    and branch.parent_timeline_id == snapshot_position.timeline_id
                    and branch.switch_position is not None
                ):
                    # Valid if snapshot position is before or at switch point
                    if snapshot_position <= branch.switch_position:
                        # Timeline branched, but snapshot is within common history
                        break
            else:
                return (
                    False,
                    CareBadge.ACTION,
                    (
                        f"incompatible timeline branches: snapshot on tl{snapshot_position.timeline_id} "
                        f"vs tail on tl{tail_start_position.timeline_id}; cannot compare divergent timelines"
                    ),
                )

        # 2. Position continuity: tail_start must be <= snapshot_position (so no writes missed)
        # If tail_start > snapshot_position, an irreversible gap exists where writes are lost!
        if tail_start_position > snapshot_position:
            gap = snapshot_position.distance_to(tail_start_position)
            return (
                False,
                CareBadge.ACTION,
                (
                    f"change skip detected: tail starts at {tail_start_position.raw_position} "
                    f"which is {gap} bytes/tx ahead of snapshot position {snapshot_position.raw_position}; "
                    f"changes would be skipped"
                ),
            )

        overlap = tail_start_position.distance_to(snapshot_position)
        return (
            True,
            CareBadge.PASS,
            (
                f"snapshot-to-tail continuity verified: tail starts at {tail_start_position.raw_position} "
                f"with {overlap} bytes/tx safe overlap before snapshot {snapshot_position.raw_position}"
            ),
        )

    # -------------------------------------------------------------------------
    # UNIX Report Formatting
    # -------------------------------------------------------------------------

    def format_unix_report(self) -> str:
        """Formats cluster topology and decision routing in enterprise UNIX style."""
        lines = [
            "-------------------------------------------------------------------------------",
            f"CLUSTER REPLICATION TOPOLOGY: {self.cluster_id} (generation {self.topology.generation})",
            "-------------------------------------------------------------------------------",
            f"{'NODE_ID':<15} {'ROLE':<16} {'TIMELINE':<10} {'LAG_SEC':<10} {'SLOT_FAILOVER':<15} {'STATE':<12}",
            "-" * 79,
        ]
        for nid, node in sorted(self.topology.nodes.items()):
            lines.append(
                f"{node.node_id:<15} {node.role.value:<16} {node.timeline_id:<10} "
                f"{node.replay_lag_seconds:<10.1f} {node.slot_failover_status.value:<15} {node.replay_state.value:<12}"
            )
        lines.append("-" * 79)
        lines.append(f"{'DECISION_ID':<20} {'STAGE':<14} {'TARGET_NODE':<14} {'BADGE':<8} {'ALLOWED':<8} REASON")
        lines.append("-" * 79)
        for dec_id, dec in sorted(self.active_decisions.items()):
            allow_str = "YES" if dec.allowed else "NO"
            lines.append(
                f"{dec.decision_id:<20} {dec.stage.value:<14} {dec.target_node_id:<14} "
                f"[{dec.badge.value}]   {allow_str:<8} {dec.reason[:60]}"
            )
        lines.append("-" * 79)
        return "\n".join(lines)


# -----------------------------------------------------------------------------
# Durable Checkpoint & Lease Lock
# -----------------------------------------------------------------------------

class SourceRoleLease:
    """Active mutual exclusion lease with heartbeat for cluster role decisions."""

    def __init__(self, lock_dir: Union[str, Path], term_seconds: float = 30.0):
        self.lock_dir = Path(lock_dir)
        self.term_seconds = term_seconds
        self.lock_file = self.lock_dir / ".run.lock"
        self._fd: Optional[int] = None
        self._held = False

    def acquire(self) -> bool:
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(str(self.lock_file), os.O_RDWR | os.O_CREAT, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            payload = json.dumps({
                "pid": os.getpid(),
                "host": socket.gethostname(),
                "acquired_at": time.time(),
                "term_seconds": self.term_seconds,
            })
            os.ftruncate(fd, 0)
            os.write(fd, payload.encode("utf-8"))
            self._fd = fd
            self._held = True
            return True
        except (BlockingIOError, OSError) as e:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
            return False

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
        self._held = False

    def is_held(self) -> bool:
        return self._held


class SourceRoleCheckpoint:
    """Atomic typed serialization and O(1) durable resume for role coordinator."""

    @staticmethod
    def save(coordinator: SourceRoleCoordinator, file_path: Union[str, Path]) -> None:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "cluster_id": coordinator.cluster_id,
            "topology": coordinator.topology.to_dict(),
            "policy": dataclasses.asdict(coordinator.policy),
            "decisions": {did: d.to_dict() for did, d in coordinator.active_decisions.items()},
            "saved_at": time.time(),
        }
        raw_json = json.dumps(data, indent=2, sort_keys=True)
        tmp = tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False)
        try:
            tmp.write(raw_json)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp.close()
            os.replace(tmp.name, path)
        finally:
            if os.path.exists(tmp.name):
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass

    @staticmethod
    def load(file_path: Union[str, Path]) -> SourceRoleCoordinator:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"source role checkpoint not found: {file_path}")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        cluster_id = data["cluster_id"]
        policy = SourceRolePolicy(**data["policy"])
        topology = TopologySnapshot.from_dict(data["topology"])
        coord = SourceRoleCoordinator(cluster_id=cluster_id, policy=policy, topology=topology)
        for did, d_data in (data.get("decisions") or {}).items():
            coord.active_decisions[did] = WorkloadRoutingDecision.from_dict(d_data)
        return coord
