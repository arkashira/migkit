from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import decide


class AdaptiveBoundsError(Exception):
    pass


class InfeasibleBoundsError(AdaptiveBoundsError):
    pass


def get_default_worker_bounds(
    min_w: Optional[int] = None,
    start_w: Optional[int] = None,
    max_w: Optional[int] = None,
) -> Tuple[int, int, int]:
    env_min = os.environ.get("MIGKIT_MIN_WORKERS")
    resolved_min = int(env_min) if env_min else (min_w if min_w is not None else 1)

    cpu = os.cpu_count() or 4
    env_start = os.environ.get("MIGKIT_START_WORKERS")
    resolved_start = int(env_start) if env_start else (start_w if start_w is not None else min(2, cpu))

    env_max = os.environ.get("MIGKIT_MAX_WORKERS")
    resolved_max = int(env_max) if env_max else (max_w if max_w is not None else max(8, cpu * 2))

    return resolved_min, max(resolved_min, resolved_start), max(resolved_start, resolved_max)


def get_default_batch_bounds(
    min_b: Optional[int] = None,
    start_b: Optional[int] = None,
    max_b: Optional[int] = None,
) -> Tuple[int, int, int]:
    env_min = os.environ.get("MIGKIT_MIN_BATCH_SIZE")
    resolved_min = int(env_min) if env_min else (min_b if min_b is not None else 100)

    env_start = os.environ.get("MIGKIT_START_BATCH_SIZE")
    resolved_start = int(env_start) if env_start else (start_b if start_b is not None else 1000)

    env_max = os.environ.get("MIGKIT_MAX_BATCH_SIZE")
    resolved_max = int(env_max) if env_max else (max_b if max_b is not None else 50000)

    return resolved_min, max(resolved_min, resolved_start), max(resolved_start, resolved_max)


class CutoverStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    ABORTED = "aborted"


@dataclass(frozen=True)
class AdaptiveBoundsEnvelope:
    min_workers: int = 1
    start_workers: int = 2
    max_workers: int = 8
    min_batch_size: int = 100
    start_batch_size: int = 1000
    max_batch_size: int = 50000
    max_connections_per_worker: int = 2
    name: str = "default"

    def __post_init__(self) -> None:
        if self.min_workers < 1:
            raise InfeasibleBoundsError(
                f"Infeasible bounds: min_workers ({self.min_workers}) must be >= 1"
            )
        if self.start_workers < self.min_workers:
            raise InfeasibleBoundsError(
                f"Infeasible bounds: start_workers ({self.start_workers}) < min_workers ({self.min_workers})"
            )
        if self.max_workers < self.start_workers:
            raise InfeasibleBoundsError(
                f"Infeasible bounds: max_workers ({self.max_workers}) < start_workers ({self.start_workers})"
            )
        if self.min_batch_size < 1:
            raise InfeasibleBoundsError(
                f"Infeasible bounds: min_batch_size ({self.min_batch_size}) must be >= 1"
            )
        if self.start_batch_size < self.min_batch_size:
            raise InfeasibleBoundsError(
                f"Infeasible bounds: start_batch_size ({self.start_batch_size}) < min_batch_size ({self.min_batch_size})"
            )
        if self.max_batch_size < self.start_batch_size:
            raise InfeasibleBoundsError(
                f"Infeasible bounds: max_batch_size ({self.max_batch_size}) < start_batch_size ({self.start_batch_size})"
            )

    def is_within(self, workers: int) -> bool:
        return self.min_workers <= workers <= self.max_workers

    def clamp(self, workers: int) -> int:
        return max(self.min_workers, min(workers, self.max_workers))

    def clamp_batch(self, batch_size: int) -> int:
        return max(self.min_batch_size, min(batch_size, self.max_batch_size))


    def to_dict(self) -> Dict[str, Any]:
        return {
            "min_workers": self.min_workers,
            "start_workers": self.start_workers,
            "max_workers": self.max_workers,
            "min_batch_size": self.min_batch_size,
            "start_batch_size": self.start_batch_size,
            "max_batch_size": self.max_batch_size,
            "max_connections_per_worker": self.max_connections_per_worker,
            "name": self.name,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AdaptiveBoundsEnvelope:
        return cls(
            min_workers=int(data.get("min_workers", 1)),
            start_workers=int(data.get("start_workers", 2)),
            max_workers=int(data.get("max_workers", 8)),
            min_batch_size=int(data.get("min_batch_size", 100)),
            start_batch_size=int(data.get("start_batch_size", 1000)),
            max_batch_size=int(data.get("max_batch_size", 50000)),
            max_connections_per_worker=int(data.get("max_connections_per_worker", 2)),
            name=str(data.get("name", "default")),
        )

    @classmethod
    def from_environment(
        cls,
        min_workers: Optional[int] = None,
        start_workers: Optional[int] = None,
        max_workers: Optional[int] = None,
        min_batch_size: Optional[int] = None,
        start_batch_size: Optional[int] = None,
        max_batch_size: Optional[int] = None,
        name: str = "env_adaptive",
    ) -> AdaptiveBoundsEnvelope:
        min_w, start_w, max_w = get_default_worker_bounds(min_workers, start_workers, max_workers)
        min_b, start_b, max_b = get_default_batch_bounds(min_batch_size, start_batch_size, max_batch_size)
        return cls(
            min_workers=min_w,
            start_workers=start_w,
            max_workers=max_w,
            min_batch_size=min_b,
            start_batch_size=start_b,
            max_batch_size=max_b,
            name=name,
        )

    @classmethod
    def from_topology(
        cls,
        topology: Any,
        connections_per_worker: int = 2,
        name: str = "topology_envelope",
    ) -> AdaptiveBoundsEnvelope:
        eff_max = getattr(topology, "effective_max_workers", None)
        max_w = max(1, eff_max(connections_per_worker) if callable(eff_max) else 8)
        start_w = max(1, min(max_w, max_w // 2 or 1))
        min_w = 1
        return cls(
            min_workers=min_w,
            start_workers=start_w,
            max_workers=max_w,
            name=name,
        )


@dataclass(frozen=True)
class ClusterNode:
    node_id: str
    host: str
    port: int = 5432
    max_connections: int = 50
    cpu_cores: int = 4
    status: str = "active"

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "max_connections": self.max_connections,
            "cpu_cores": self.cpu_cores,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ClusterNode:
        return cls(
            node_id=str(data["node_id"]),
            host=str(data["host"]),
            port=int(data.get("port", 5432)),
            max_connections=int(data.get("max_connections", 50)),
            cpu_cores=int(data.get("cpu_cores", 4)),
            status=str(data.get("status", "active")),
        )


@dataclass(frozen=True)
class ShardInfo:
    shard_id: str
    node_id: Optional[str] = None
    key_range: Optional[Tuple[Optional[str], Optional[str]]] = None
    hash_mod: Optional[Tuple[int, int]] = None
    active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "node_id": self.node_id,
            "key_range": list(self.key_range) if self.key_range else None,
            "hash_mod": list(self.hash_mod) if self.hash_mod else None,
            "active": self.active,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ShardInfo:
        kr = data.get("key_range")
        key_range = (kr[0], kr[1]) if kr else None
        hm = data.get("hash_mod")
        hash_mod = (hm[0], hm[1]) if hm else None
        return cls(
            shard_id=str(data["shard_id"]),
            node_id=data.get("node_id"),
            key_range=key_range,
            hash_mod=hash_mod,
            active=bool(data.get("active", True)),
        )


@dataclass
class ClusterTopology:
    cluster_id: str
    nodes: Dict[str, ClusterNode] = field(default_factory=dict)
    max_cluster_workers: Optional[int] = None
    shards: Dict[str, ShardInfo] = field(default_factory=dict)

    def total_connections(self) -> int:
        return sum(n.max_connections for n in self.nodes.values() if n.status == "active")

    def effective_max_workers(self, connections_per_worker: int = 2) -> int:
        conns = self.total_connections()
        cpu_bound = (os.cpu_count() or 4) * 4
        conn_cap = conns // max(1, connections_per_worker) if conns > 0 else cpu_bound
        if self.max_cluster_workers is not None:
            return min(self.max_cluster_workers, conn_cap)
        return conn_cap

    def is_endpoint_reachable(self, endpoint: str) -> bool:
        norm = endpoint.strip().lower()
        for node in self.nodes.values():
            if node.status != "active":
                continue
            if node.endpoint.lower() == norm:
                return True
            if node.host.lower() == norm or f"{node.host.lower()}:{node.port}" == norm:
                return True
            if node.node_id.lower() == norm:
                return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "nodes": {nid: node.to_dict() for nid, node in self.nodes.items()},
            "max_cluster_workers": self.max_cluster_workers,
            "shards": {sid: s.to_dict() for sid, s in self.shards.items()},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ClusterTopology:
        nodes = {
            nid: ClusterNode.from_dict(ndata)
            for nid, ndata in data.get("nodes", {}).items()
        }
        shards = {
            sid: ShardInfo.from_dict(sdata)
            for sid, sdata in data.get("shards", {}).items()
        }
        return cls(
            cluster_id=str(data.get("cluster_id", "default")),
            nodes=nodes,
            max_cluster_workers=data.get("max_cluster_workers"),
            shards=shards,
        )


@dataclass
class PartitionRoute:
    partition_id: str
    target_endpoint: str
    source_endpoint: Optional[str] = None
    key_range: Optional[Tuple[Optional[str], Optional[str]]] = None
    hash_bucket: Optional[Tuple[int, int]] = None
    worker_budget: int = 1
    is_custom: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "target_endpoint": self.target_endpoint,
            "source_endpoint": self.source_endpoint,
            "key_range": list(self.key_range) if self.key_range else None,
            "hash_bucket": list(self.hash_bucket) if self.hash_bucket else None,
            "worker_budget": self.worker_budget,
            "is_custom": self.is_custom,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PartitionRoute:
        kr = data.get("key_range")
        key_range = (kr[0], kr[1]) if kr else None
        hb = data.get("hash_bucket")
        hash_bucket = (hb[0], hb[1]) if hb else None
        return cls(
            partition_id=str(data["partition_id"]),
            target_endpoint=str(data["target_endpoint"]),
            source_endpoint=data.get("source_endpoint"),
            key_range=key_range,
            hash_bucket=hash_bucket,
            worker_budget=int(data.get("worker_budget", 1)),
            is_custom=bool(data.get("is_custom", False)),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class PartitionProgress:
    partition_id: str
    cursor: Optional[str] = None
    processed_rows: int = 0
    bytes_count: int = 0
    status: str = "in_progress"
    last_updated: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "cursor": self.cursor,
            "processed_rows": self.processed_rows,
            "bytes_count": self.bytes_count,
            "status": self.status,
            "last_updated": self.last_updated,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PartitionProgress:
        return cls(
            partition_id=str(data["partition_id"]),
            cursor=data.get("cursor"),
            processed_rows=int(data.get("processed_rows", 0)),
            bytes_count=int(data.get("bytes_count", 0)),
            status=str(data.get("status", "in_progress")),
            last_updated=float(data.get("last_updated", 0.0)),
        )


@dataclass
class CutoverIteration:
    iteration_id: str
    target_stage: str = "cutover"
    status: CutoverStatus = CutoverStatus.IN_PROGRESS
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "iteration_id": self.iteration_id,
            "target_stage": self.target_stage,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CutoverIteration:
        return cls(
            iteration_id=str(data["iteration_id"]),
            target_stage=str(data.get("target_stage", "cutover")),
            status=CutoverStatus(data.get("status", "in_progress")),
            started_at=float(data.get("started_at", 0.0)),
            completed_at=data.get("completed_at"),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class ShardSplitRecord:
    parent_id: str
    child_ids: List[str]
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "parent_id": self.parent_id,
            "child_ids": list(self.child_ids),
            "timestamp": self.timestamp,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ShardSplitRecord:
        return cls(
            parent_id=str(data["parent_id"]),
            child_ids=list(data.get("child_ids", [])),
            timestamp=float(data.get("timestamp", 0.0)),
            metadata=dict(data.get("metadata", {})),
        )


class PartitionRoutingJournal:
    def __init__(
        self,
        envelope: Optional[AdaptiveBoundsEnvelope] = None,
        topology: Optional[ClusterTopology] = None,
        journal_path: Optional[str] = None,
        routes: Optional[Dict[str, PartitionRoute]] = None,
        inactive_routes: Optional[Dict[str, PartitionRoute]] = None,
        progress: Optional[Dict[str, PartitionProgress]] = None,
        cutover_iterations: Optional[List[CutoverIteration]] = None,
        split_history: Optional[List[ShardSplitRecord]] = None,
    ) -> None:
        self.envelope = envelope or AdaptiveBoundsEnvelope()
        self.topology = topology
        self.journal_path = journal_path
        self.routes: Dict[str, PartitionRoute] = dict(routes or {})
        self.inactive_routes: Dict[str, PartitionRoute] = dict(inactive_routes or {})
        self.progress: Dict[str, PartitionProgress] = dict(progress or {})
        self.cutover_iterations: List[CutoverIteration] = list(cutover_iterations or [])
        self.split_history: List[ShardSplitRecord] = list(split_history or [])

    def set_route(self, route: PartitionRoute) -> None:
        self.routes[route.partition_id] = route

    def get_route(self, partition_id: str) -> Optional[PartitionRoute]:
        return self.routes.get(partition_id) or self.inactive_routes.get(partition_id)

    def is_partition_active(self, partition_id: str) -> bool:
        return partition_id in self.routes

    def record_progress(
        self,
        partition_id: str,
        cursor: Optional[str] = None,
        processed_rows: int = 0,
        bytes_count: int = 0,
        status: str = "in_progress",
    ) -> None:
        existing = self.progress.get(partition_id)
        if existing:
            total_rows = existing.processed_rows + processed_rows
            total_bytes = existing.bytes_count + bytes_count
            existing.cursor = cursor if cursor is not None else existing.cursor
            existing.processed_rows = total_rows
            existing.bytes_count = total_bytes
            existing.status = status
            existing.last_updated = time.time()
        else:
            self.progress[partition_id] = PartitionProgress(
                partition_id=partition_id,
                cursor=cursor,
                processed_rows=processed_rows,
                bytes_count=bytes_count,
                status=status,
                last_updated=time.time(),
            )

    def get_progress(self, partition_id: str) -> Optional[PartitionProgress]:
        return self.progress.get(partition_id)

    def get_worker_budget(self, partition_id: str) -> int:
        r = self.routes.get(partition_id)
        return r.worker_budget if r else 0

    def get_total_worker_budget(self) -> int:
        return sum(r.worker_budget for r in self.routes.values())

    def scale_worker_budgets(self, allocations: Dict[str, int]) -> None:
        total = 0
        for pid, budget in allocations.items():
            if budget < 1:
                raise InfeasibleBoundsError(f"Worker budget for partition {pid} must be >= 1")
            total += budget

        if not self.envelope.is_within(total):
            raise InfeasibleBoundsError(
                f"Total worker budget {total} falls outside envelope "
                f"[{self.envelope.min_workers}, {self.envelope.max_workers}]"
            )

        if self.topology:
            effective_cap = self.topology.effective_max_workers(
                self.envelope.max_connections_per_worker
            )
            if total > effective_cap:
                raise InfeasibleBoundsError(
                    f"Total worker budget {total} exceeds cluster capacity {effective_cap}"
                )

        for pid, budget in allocations.items():
            if pid in self.routes:
                self.routes[pid].worker_budget = budget

    def apply_shard_split(
        self, parent_partition_id: str, child_routes: List[PartitionRoute]
    ) -> ShardSplitRecord:
        parent = self.routes.get(parent_partition_id)
        if parent is None:
            parent = self.inactive_routes.get(parent_partition_id)
            if parent is None:
                parent = PartitionRoute(
                    partition_id=parent_partition_id,
                    target_endpoint=child_routes[0].target_endpoint if child_routes else "",
                    is_custom=False,
                )

        projected_routes = dict(self.routes)
        if parent_partition_id in projected_routes:
            del projected_routes[parent_partition_id]

        for child in child_routes:
            if parent.is_custom and not child.is_custom:
                child.is_custom = True
            projected_routes[child.partition_id] = child

        total_workers = sum(r.worker_budget for r in projected_routes.values())
        if self.topology:
            cluster_cap = self.topology.effective_max_workers(
                self.envelope.max_connections_per_worker
            )
            if total_workers > cluster_cap:
                raise InfeasibleBoundsError(
                    f"Post-split worker requirement ({total_workers}) exceeds cluster capacity ({cluster_cap})"
                )

        if not self.envelope.is_within(total_workers):
            raise InfeasibleBoundsError(
                f"Post-split worker requirement ({total_workers}) outside bounds "
                f"[{self.envelope.min_workers}, {self.envelope.max_workers}]"
            )

        if parent_partition_id in self.routes:
            self.inactive_routes[parent_partition_id] = self.routes.pop(parent_partition_id)

        for child in child_routes:
            self.routes[child.partition_id] = child

        split_rec = ShardSplitRecord(
            parent_id=parent_partition_id,
            child_ids=[c.partition_id for c in child_routes],
            timestamp=time.time(),
        )
        self.split_history.append(split_rec)
        return split_rec

    def recheck_feasibility(
        self, topology: Optional[ClusterTopology] = None
    ) -> bool:
        topo = topology or self.topology
        if not topo:
            return True

        cluster_cap = topo.effective_max_workers(self.envelope.max_connections_per_worker)
        if cluster_cap < self.envelope.min_workers:
            raise InfeasibleBoundsError(
                f"Cluster capacity ({cluster_cap} workers) cannot support minimum envelope "
                f"({self.envelope.min_workers} workers)"
            )

        for route in self.routes.values():
            if not topo.is_endpoint_reachable(route.target_endpoint):
                raise InfeasibleBoundsError(
                    f"Partition route {route.partition_id} targets unreachable endpoint "
                    f"'{route.target_endpoint}'"
                )
            if route.source_endpoint and not topo.is_endpoint_reachable(route.source_endpoint):
                raise InfeasibleBoundsError(
                    f"Partition route {route.partition_id} references unreachable source endpoint "
                    f"'{route.source_endpoint}'"
                )

        total_workers = sum(r.worker_budget for r in self.routes.values())
        if total_workers > cluster_cap:
            raise InfeasibleBoundsError(
                f"Total worker budget ({total_workers}) exceeds updated cluster capacity ({cluster_cap})"
            )

        return True

    def update_topology_and_recheck(self, new_topology: ClusterTopology) -> bool:
        self.recheck_feasibility(topology=new_topology)
        self.topology = new_topology
        return True

    def begin_cutover(
        self,
        iteration_id: str,
        target_stage: str = "cutover",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CutoverIteration:
        iteration = CutoverIteration(
            iteration_id=iteration_id,
            target_stage=target_stage,
            status=CutoverStatus.IN_PROGRESS,
            started_at=time.time(),
            metadata=dict(metadata or {}),
        )
        self.cutover_iterations.append(iteration)
        return iteration

    def commit_cutover(self, iteration_id: str) -> None:
        for it in self.cutover_iterations:
            if it.iteration_id == iteration_id:
                it.status = CutoverStatus.COMMITTED
                it.completed_at = time.time()
                return
        raise AdaptiveBoundsError(f"Cutover iteration {iteration_id} not found")

    def rollback_cutover(self, iteration_id: str, reason: str = "") -> None:
        for it in self.cutover_iterations:
            if it.iteration_id == iteration_id:
                it.status = CutoverStatus.ROLLED_BACK
                it.completed_at = time.time()
                it.metadata["rollback_reason"] = reason
                return
        raise AdaptiveBoundsError(f"Cutover iteration {iteration_id} not found")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "envelope": self.envelope.to_dict(),
            "topology": self.topology.to_dict() if self.topology else None,
            "routes": {pid: r.to_dict() for pid, r in self.routes.items()},
            "inactive_routes": {pid: r.to_dict() for pid, r in self.inactive_routes.items()},
            "progress": {pid: p.to_dict() for pid, p in self.progress.items()},
            "cutover_iterations": [it.to_dict() for it in self.cutover_iterations],
            "split_history": [sh.to_dict() for sh in self.split_history],
        }

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], journal_path: Optional[str] = None
    ) -> PartitionRoutingJournal:
        envelope = AdaptiveBoundsEnvelope.from_dict(data["envelope"])
        topo_data = data.get("topology")
        topology = ClusterTopology.from_dict(topo_data) if topo_data else None

        routes = {
            pid: PartitionRoute.from_dict(rdata)
            for pid, rdata in data.get("routes", {}).items()
        }
        inactive_routes = {
            pid: PartitionRoute.from_dict(rdata)
            for pid, rdata in data.get("inactive_routes", {}).items()
        }
        progress = {
            pid: PartitionProgress.from_dict(pdata)
            for pid, pdata in data.get("progress", {}).items()
        }
        cutover_iterations = [
            CutoverIteration.from_dict(idata)
            for idata in data.get("cutover_iterations", [])
        ]
        split_history = [
            ShardSplitRecord.from_dict(sdata)
            for sdata in data.get("split_history", [])
        ]

        return cls(
            envelope=envelope,
            topology=topology,
            journal_path=journal_path,
            routes=routes,
            inactive_routes=inactive_routes,
            progress=progress,
            cutover_iterations=cutover_iterations,
            split_history=split_history,
        )

    def persist(self, path: Optional[str] = None) -> None:
        target_path = path or self.journal_path
        if not target_path:
            raise AdaptiveBoundsError("Cannot persist: no journal_path configured")
        self.journal_path = target_path

        p = Path(target_path)
        p.parent.mkdir(parents=True, exist_ok=True)

        payload = json.dumps(self.to_dict(), indent=2)
        tmp_fd, tmp_file = tempfile.mkstemp(dir=p.parent, prefix="journal_", suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, target_path)
        except Exception:
            if os.path.exists(tmp_file):
                os.unlink(tmp_file)
            raise

    def save(self, path: Optional[str] = None) -> None:
        self.persist(path)

    @classmethod
    def load(cls, path: str) -> PartitionRoutingJournal:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Journal file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data, journal_path=path)


# Decide ladders for Adaptive Bounds and Partition Routing

def _prove_topology_capacity_constrained(facts: decide.Facts) -> bool:
    return bool(facts.get("topology_constrained") and not facts.get("refused"))


def _prove_latency_aimd_regulated(facts: decide.Facts) -> bool:
    return bool(facts.get("aimd_regulated") and not facts.get("refused"))


def _prove_conservative_cold_start(facts: decide.Facts) -> bool:
    return bool(facts.get("cold_start") and not facts.get("refused"))


def _prove_partition_throughput_optimized(facts: decide.Facts) -> bool:
    return bool(facts.get("throughput_optimized") and not facts.get("refused"))


def _prove_refuse_infeasible_overcommit(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_infeasible_overcommit"))


def adaptive_bounds_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "topology_capacity_constrained",
            "constrain worker count strictly within physical cluster connection and node capacity",
            needs=(
                decide.Need(
                    "topology capacity boundaries verified",
                    _prove_topology_capacity_constrained,
                    "topology capacity constraint not satisfied",
                ),
            ),
        ),
        decide.Rung(
            "latency_aimd_regulated",
            "adaptively regulate batch size and concurrency based on real-time query latency feedback",
            needs=(
                decide.Need(
                    "aimd latency regulation active",
                    _prove_latency_aimd_regulated,
                    "aimd latency regulation inactive",
                ),
            ),
        ),
        decide.Rung(
            "conservative_cold_start",
            "initialize migration worker allocation with minimum safe baseline prior to scale-up",
            needs=(
                decide.Need(
                    "conservative cold start certified",
                    _prove_conservative_cold_start,
                    "cold start safety criteria not met",
                ),
            ),
        ),
        decide.Rung(
            "partition_throughput_optimized",
            "scale batch sizing and worker allocation for maximum throughput across balanced partitions",
            needs=(
                decide.Need(
                    "partition throughput optimization active",
                    _prove_partition_throughput_optimized,
                    "partition throughput optimization inactive",
                ),
            ),
        ),
        decide.Rung(
            "refuse_infeasible_overcommit",
            "refuse envelope allocation when requested workers or batches violate feasibility boundaries",
            needs=(
                decide.Need(
                    "infeasible overcommit refused",
                    _prove_refuse_infeasible_overcommit,
                    "infeasible overcommit detected",
                ),
            ),
        ),
    )


def decide_adaptive_bounds(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("adaptive bounds envelope", adaptive_bounds_rungs(), fact_obj)


def _prove_direct_endpoint_routing(facts: decide.Facts) -> bool:
    return bool(facts.get("direct_routing") and not facts.get("refused"))


def _prove_replica_read_offload(facts: decide.Facts) -> bool:
    return bool(facts.get("replica_offload") and not facts.get("refused"))


def _prove_split_partition_rebalancing(facts: decide.Facts) -> bool:
    return bool(facts.get("split_rebalancing") and not facts.get("refused"))


def _prove_coalesce_quiesced_partition(facts: decide.Facts) -> bool:
    return bool(facts.get("coalesce_quiesced") and not facts.get("refused"))


def _prove_refuse_unreachable_endpoint_route(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_unreachable_endpoint"))


def partition_routing_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "direct_endpoint_routing",
            "route migration partition directly to primary shard leader endpoint",
            needs=(
                decide.Need(
                    "direct endpoint reachable",
                    _prove_direct_endpoint_routing,
                    "direct leader endpoint unreachable",
                ),
            ),
        ),
        decide.Rung(
            "replica_read_offload",
            "offload read verification queries to healthy follower read replicas",
            needs=(
                decide.Need(
                    "replica read offload available",
                    _prove_replica_read_offload,
                    "replica offload unavailable or lag exceeded",
                ),
            ),
        ),
        decide.Rung(
            "split_partition_rebalancing",
            "dynamically route across split child partitions during active cutover",
            needs=(
                decide.Need(
                    "split partition rebalancing certified",
                    _prove_split_partition_rebalancing,
                    "split partition routing not required",
                ),
            ),
        ),
        decide.Rung(
            "coalesce_quiesced_partition",
            "coalesce routing paths when parent partitions complete and quiesce",
            needs=(
                decide.Need(
                    "partition coalescing certified",
                    _prove_coalesce_quiesced_partition,
                    "partition coalescing criteria not met",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unreachable_endpoint_route",
            "refuse partition routing when destination node or endpoint is disconnected or unverified",
            needs=(
                decide.Need(
                    "unreachable route refused",
                    _prove_refuse_unreachable_endpoint_route,
                    "unreachable endpoint route detected",
                ),
            ),
        ),
    )


def decide_partition_routing(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("partition routing strategy", partition_routing_rungs(), fact_obj)

