"""Connect assessment to measured target topology and sizing (Row 639: RCH31.placement-advice).

Extends machine and sizing advice with:
- Table and secondary index access patterns, write amplification, and global index fanout.
- Key distribution skew (Gini, max-to-median ratio, monotonic range append hot spots).
- Distributed locality (co-accessed table affinity, shared correlation keys, reference tables).
- Transaction fanout (single-shard vs cross-partition 2PC coordination and RTT cost).
- Candidate target designs (colocated, distributed, partitioned, hybrid) and resource sizing
  derived from measured/rehearsed workload evidence rather than brand-specific static classes.
- End-to-end workload evaluation proving that a design with fast bulk import that causes
  hot shards or expensive cross-partition transactions loses to a balanced end-to-end plan.
- Universal typed durable resume in O(1), active lease lock (.run.lock), AIMD adaptive
  rehearsal batch sizing, byte-weighted cold-start ETA, and 4-tier care badges
  (ACTION, REVIEW, BENIGN, PASS).
"""
from __future__ import annotations

import datetime
import decimal
import enum
import json
import math
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable, machine, rehearsal


# -----------------------------------------------------------------------------
# Decision Engine Rungs (Row 639: RCH31)
# -----------------------------------------------------------------------------

def placement_topology_rungs(facts: Optional[decide.Facts] = None) -> Tuple[decide.Rung, ...]:
    """Ladder for selecting candidate target placement topology based on measured workload evidence."""
    return (
        decide.Rung(
            "colocated_topology",
            "colocate tables to eliminate cross-node distributed 2PC transaction latency",
            needs=(
                decide.Need("total_bytes_fits_node", lambda f: bool(f.get("total_bytes_fits_node"))),
                decide.Need("high_cross_table_transactions", lambda f: bool(f.get("high_cross_table_transactions"))),
            ),
        ),
        decide.Rung(
            "partitioned_topology",
            "partition by monotonic range or time key with localized partition pruning",
            needs=(
                decide.Need("has_range_partition_key", lambda f: bool(f.get("has_range_partition_key"))),
                decide.Need("low_cross_partition_fanout", lambda f: bool(f.get("low_cross_partition_fanout"))),
            ),
        ),
        decide.Rung(
            "hybrid_topology",
            "hybrid topology: colocate reference tables and distribute high-volume sharded tables",
            needs=(
                decide.Need("has_reference_tables", lambda f: bool(f.get("has_reference_tables"))),
                decide.Need("has_distributed_sharded_tables", lambda f: bool(f.get("has_distributed_sharded_tables"))),
            ),
        ),
        decide.Rung(
            "distributed_hash_topology",
            "distribute large tables across cluster nodes using uniform hash keys",
        ),
    )


def decide_placement_topology(facts: Union[Dict[str, Any], decide.Facts]) -> decide.Choice:
    """Evaluates target placement topology via decide ladder."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(facts)
    return decide.climb("placement_topology", placement_topology_rungs(f), f)


CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class TargetTopologyKind(str, enum.Enum):
    COLOCATED = "colocated"
    DISTRIBUTED = "distributed"
    PARTITIONED = "partitioned"
    HYBRID = "hybrid_colocated_distributed"


class DistributionStrategy(str, enum.Enum):
    NONE = "none"
    COLOCATED_GROUP = "colocated_group"
    HASH = "hash"
    RANGE = "range"
    REPLICATED_REFERENCE = "replicated_reference"


class EvidenceTruth(str, enum.Enum):
    REHEARSED = "rehearsed"
    MEASURED = "measured"
    OBSERVED = "observed"
    STATIC_CLASS_ONLY = "static_class_only"


class PlacementAdviceError(RuntimeError):
    """Base error for placement advice and topology sizing operations."""


class ActiveLeaseConflictError(PlacementAdviceError):
    """Raised when another active worker holds the placement advice lease."""


class StaticBrandClassRefusedError(PlacementAdviceError):
    """Raised when an unverified static brand class is forced without rehearsal evidence."""


@dataclass(frozen=True)
class IndexAccessPattern:
    """Access and maintenance profile of a table index."""
    index_name: str
    columns: Tuple[str, ...]
    is_unique: bool = False
    is_monotonic: bool = False
    point_lookup_qps: float = 0.0
    range_scan_qps: float = 0.0
    write_amplification_factor: float = 1.15

    def aligned_with_shard_key(self, shard_key: Sequence[str]) -> bool:
        if not shard_key or not self.columns:
            return False
        sk = tuple(str(c) for c in shard_key)
        cols = tuple(str(c) for c in self.columns)
        return cols[: len(sk)] == sk or set(sk).issubset(set(cols))


@dataclass(frozen=True)
class KeySkewProfile:
    """Measured key cardinality, skew, and hot-spot behavior for a candidate key."""
    key_columns: Tuple[str, ...]
    gini_coefficient: float = 0.0
    max_to_median_ratio: float = 1.0
    hot_shard_fraction: float = 0.0
    monotonic_append_share: float = 0.0
    distinct_keys: int = 10000

    def is_hot_spotted(
        self,
        strategy: str = DistributionStrategy.HASH.value,
        partition_count: int = 1,
    ) -> bool:
        if partition_count <= 1:
            return False
        if strategy == DistributionStrategy.RANGE.value and self.monotonic_append_share >= 0.45:
            return True
        return (
            self.gini_coefficient >= 0.50
            or self.max_to_median_ratio >= 3.0
            or self.hot_shard_fraction >= 0.45
        )

    def effective_hot_shard_share(
        self,
        strategy: str,
        partition_count: int,
    ) -> float:
        n = max(1, int(partition_count))
        if n == 1:
            return 1.0
        uniform = 1.0 / n
        if strategy == DistributionStrategy.RANGE.value:
            base = max(self.hot_shard_fraction, self.monotonic_append_share, uniform)
            ratio_share = min(0.98, uniform * max(1.0, self.max_to_median_ratio))
            return min(0.99, max(base, ratio_share, uniform + 0.65 * self.gini_coefficient))
        if strategy == DistributionStrategy.HASH.value:
            ratio_share = min(0.95, uniform * max(1.0, self.max_to_median_ratio))
            return min(0.98, max(self.hot_shard_fraction, ratio_share, uniform + 0.50 * self.gini_coefficient))
        return uniform


@dataclass
class TableAccessProfile:
    """Workload and schema evidence for a single table."""
    table_name: str
    size_bytes: int
    row_count: int
    point_read_qps: float = 0.0
    range_scan_qps: float = 0.0
    write_qps: float = 0.0
    bulk_import_bytes: Optional[int] = None
    primary_key: Tuple[str, ...] = ("id",)
    candidate_shard_keys: Tuple[Tuple[str, ...], ...] = (("id",),)
    skew_by_key: Dict[str, KeySkewProfile] = field(default_factory=dict)
    indexes: List[IndexAccessPattern] = field(default_factory=list)
    co_accessed_tables: Dict[str, float] = field(default_factory=dict)
    shared_join_keys: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    is_reference_candidate: bool = False

    @property
    def effective_import_bytes(self) -> int:
        return int(self.bulk_import_bytes if self.bulk_import_bytes is not None else self.size_bytes)

    @property
    def total_qps(self) -> float:
        return float(self.point_read_qps + self.range_scan_qps + self.write_qps)

    def skew_for(self, shard_key: Sequence[str]) -> KeySkewProfile:
        key_tuple = tuple(str(c) for c in (shard_key or self.primary_key))
        joined = ",".join(key_tuple)
        if joined in self.skew_by_key:
            return self.skew_by_key[joined]
        if len(key_tuple) == 1 and key_tuple[0] in self.skew_by_key:
            return self.skew_by_key[key_tuple[0]]
        return KeySkewProfile(key_columns=key_tuple)


@dataclass(frozen=True)
class TransactionFanoutProfile:
    """Workload transaction locality and multi-table coordination characteristics."""
    workload_name: str = "oltp_mixed"
    total_tps: float = 500.0
    tables_touched_avg: float = 2.0
    single_key_tps: float = 200.0
    co_keyed_join_tps: float = 250.0
    cross_key_tps: float = 50.0
    inter_node_rtt_ms: float = 2.0
    two_phase_commit_overhead_ms: float = 6.5
    sla_p95_latency_ms: float = 20.0
    base_local_p95_latency_ms: float = 3.5


@dataclass
class WorkloadAssessmentEvidence:
    """Aggregated assessment evidence linking source observations to target topology sizing."""
    tables: List[TableAccessProfile]
    transactions: TransactionFanoutProfile = field(default_factory=TransactionFanoutProfile)
    load_snapshots: List[Dict[str, Any]] = field(default_factory=list)
    stages: Optional[machine.Stages] = None
    deadline_seconds: Optional[float] = None
    workload_horizon_seconds: float = 3600.0 * 6.0
    colocation_working_set_limit_bytes: int = 32 * 1024 ** 3
    source_host: str = "127.0.0.1"
    target_host: str = "10.0.0.10"
    cloud: str = "aws"
    region: str = "us-east-1"
    engine: str = "postgres"

    @property
    def total_size_bytes(self) -> int:
        return sum(int(t.size_bytes) for t in self.tables)

    @property
    def total_import_bytes(self) -> int:
        return sum(int(t.effective_import_bytes) for t in self.tables)


@dataclass(frozen=True)
class TablePlacementDecision:
    """Placement and sharding decision for one table within a candidate design."""
    table_name: str
    topology_kind: str
    distribution_strategy: str
    shard_key: Tuple[str, ...]
    partition_count: int
    colocation_group: Optional[str] = None
    estimated_hot_shard_load_factor: float = 1.0
    cross_partition_index_fanout: int = 1
    rationale: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ResourceSizingSpec:
    """Evidence-backed target resource sizing without brand-specific static lock-in."""
    node_count: int
    per_node_vcpu: int
    per_node_memory_gib: float
    per_node_iops: int
    per_node_network_mb_s: float
    total_vcpu: int
    total_memory_gib: float
    total_iops: int
    recommended_connections_per_node: int
    matched_catalog_machine: Optional[str] = None
    sizing_rationale: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CandidateTargetDesign:
    """A candidate colocated, distributed, partitioned, or hybrid target topology design."""
    design_id: str
    name: str
    topology_kind: str
    node_count: int
    table_placements: Dict[str, TablePlacementDecision]
    sizing: ResourceSizingSpec
    brand_static_class: Optional[str] = None
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "design_id": self.design_id,
            "name": self.name,
            "topology_kind": self.topology_kind,
            "node_count": self.node_count,
            "table_placements": {k: v.to_dict() for k, v in self.table_placements.items()},
            "sizing": self.sizing.to_dict(),
            "brand_static_class": self.brand_static_class,
            "description": self.description,
        }


@dataclass(frozen=True)
class RehearsalMeasurement:
    """Measured or simulated rehearsal telemetry for a candidate target design."""
    design_id: str
    bulk_import_rate_bytes_sec: float
    bulk_import_seconds: float
    steady_state_tps_capacity: float
    steady_state_p95_latency_ms: float
    max_shard_cpu_utilization: float
    median_shard_cpu_utilization: float
    cross_partition_txn_share: float
    distributed_2pc_latency_penalty_ms: float
    global_index_write_stall_ms: float = 0.0
    hot_shard_table: str = ""
    truth: str = EvidenceTruth.REHEARSED.value
    sample_fraction: float = 1.0
    p90_widening_factor: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DesignEvaluation:
    """End-to-end scoring and care badge classification for a candidate target design."""
    design: CandidateTargetDesign
    measurement: RehearsalMeasurement
    bulk_import_seconds: float
    steady_state_workload_seconds: float
    hot_shard_penalty_seconds: float
    cross_partition_fanout_penalty_seconds: float
    index_amplification_penalty_seconds: float
    end_to_end_effective_cost_score: float
    meets_import_deadline: bool
    meets_latency_sla: bool
    has_hot_shard: bool
    has_expensive_cross_partition_txns: bool
    is_static_brand_only: bool
    care_badge: str
    care_reasons: List[str] = field(default_factory=list)
    recommendation_summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "design": self.design.to_dict(),
            "measurement": self.measurement.to_dict(),
            "bulk_import_seconds": self.bulk_import_seconds,
            "steady_state_workload_seconds": self.steady_state_workload_seconds,
            "hot_shard_penalty_seconds": self.hot_shard_penalty_seconds,
            "cross_partition_fanout_penalty_seconds": self.cross_partition_fanout_penalty_seconds,
            "index_amplification_penalty_seconds": self.index_amplification_penalty_seconds,
            "end_to_end_effective_cost_score": self.end_to_end_effective_cost_score,
            "meets_import_deadline": self.meets_import_deadline,
            "meets_latency_sla": self.meets_latency_sla,
            "has_hot_shard": self.has_hot_shard,
            "has_expensive_cross_partition_txns": self.has_expensive_cross_partition_txns,
            "is_static_brand_only": self.is_static_brand_only,
            "care_badge": self.care_badge,
            "care_reasons": list(self.care_reasons),
            "recommendation_summary": self.recommendation_summary,
        }


# ---------------------------------------------------------------------------
# Typed JSON Encoding for O(1) Durable Resume
# ---------------------------------------------------------------------------


def _encode_typed(val: Any) -> Any:
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "val": [_encode_typed(x) for x in val]}
    if isinstance(val, (set, frozenset)):
        return {"__type__": "set", "val": [_encode_typed(x) for x in sorted(val, key=repr)]}
    if isinstance(val, dict):
        return {str(k): _encode_typed(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_encode_typed(x) for x in val]
    return val


def _decode_typed(val: Any) -> Any:
    if isinstance(val, dict):
        t = val.get("__type__")
        if t == "datetime":
            return datetime.datetime.fromisoformat(val["val"])
        if t == "date":
            return datetime.date.fromisoformat(val["val"])
        if t == "decimal":
            return decimal.Decimal(val["val"])
        if t == "uuid":
            return uuid.UUID(val["val"])
        if t == "tuple":
            return tuple(_decode_typed(x) for x in val["val"])
        if t == "set":
            return set(_decode_typed(x) for x in val["val"])
        return {k: _decode_typed(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_decode_typed(x) for x in val]
    return val


class PlacementCheckpointStore:
    """Universal typed durable checkpoint store supporting O(1) unit completion skip."""

    def __init__(self, state_path: Union[str, Path]) -> None:
        self.state_path = Path(state_path)
        self._completed: Dict[str, Dict[str, Any]] = {}
        self._cursors: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self.state_path.exists():
            return
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            decoded = _decode_typed(raw)
            if isinstance(decoded, dict):
                self._completed = dict(decoded.get("completed") or {})
                self._cursors = dict(decoded.get("cursors") or {})
        except (OSError, ValueError):
            self._completed = {}
            self._cursors = {}

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = _encode_typed({
            "completed": self._completed,
            "cursors": self._cursors,
            "updated_at": time.time(),
        })
        durable.write_atomic(
            self.state_path,
            json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
            full=False,
        )

    def is_completed(self, unit_id: str) -> bool:
        """O(1) hash lookup for completed design/range rehearsal units."""
        return str(unit_id) in self._completed

    def mark_completed(self, unit_id: str, cursor: Any = None, result: Any = None) -> None:
        key = str(unit_id)
        self._completed[key] = {
            "cursor": cursor,
            "result": result,
            "completed_at": time.time(),
        }
        if cursor is not None:
            self._cursors[key] = cursor
        self.save()

    def get_cursor(self, unit_id: str) -> Any:
        return self._cursors.get(str(unit_id))

    def get_result(self, unit_id: str) -> Any:
        entry = self._completed.get(str(unit_id))
        return entry.get("result") if isinstance(entry, dict) else None

    @property
    def completed_ids(self) -> Set[str]:
        return set(self._completed.keys())


# ---------------------------------------------------------------------------
# Mutual Exclusion & Active Lease Lock (.run.lock)
# ---------------------------------------------------------------------------


class ActiveLeaseLock:
    """Active lease lock (.run.lock) with heartbeat preventing duplicate or overlapping runs."""

    def __init__(
        self,
        lock_dir: Union[str, Path],
        holder_id: Optional[str] = None,
        lease_term_seconds: float = 30.0,
    ) -> None:
        self.lock_dir = Path(lock_dir)
        self.lock_file = self.lock_dir / ".run.lock"
        self.holder_id = holder_id or f"placement-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.lease_term_seconds = float(lease_term_seconds)
        self.acquired = False

    def acquire(self, now: Optional[float] = None) -> None:
        ts = time.time() if now is None else float(now)
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        if self.lock_file.exists():
            try:
                data = json.loads(self.lock_file.read_text(encoding="utf-8"))
                expires = float(data.get("expires", 0.0))
                holder = str(data.get("holder", "unknown"))
                if ts < expires and holder != self.holder_id:
                    raise ActiveLeaseConflictError(
                        f"Active lease held by {holder} until {expires:.1f} "
                        f"(remaining {expires - ts:.1f}s)"
                    )
            except (OSError, ValueError):
                pass
        payload = {
            "holder": self.holder_id,
            "acquired_at": ts,
            "expires": ts + self.lease_term_seconds,
            "pid": os.getpid(),
        }
        durable.write_atomic(
            self.lock_file,
            json.dumps(payload).encode("utf-8"),
            full=False,
        )
        self.acquired = True

    def heartbeat(self, now: Optional[float] = None) -> None:
        if not self.acquired:
            raise ActiveLeaseConflictError("Cannot heartbeat unacquired lease")
        ts = time.time() if now is None else float(now)
        payload = {
            "holder": self.holder_id,
            "acquired_at": ts,
            "expires": ts + self.lease_term_seconds,
            "pid": os.getpid(),
        }
        durable.write_atomic(
            self.lock_file,
            json.dumps(payload).encode("utf-8"),
            full=False,
        )

    def release(self) -> None:
        if self.acquired:
            try:
                if self.lock_file.exists():
                    self.lock_file.unlink()
            except OSError:
                pass
            self.acquired = False

    def __enter__(self) -> ActiveLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# ---------------------------------------------------------------------------
# Adaptive AIMD Batch Sizing & Cold-Start Byte-Weighted ETA
# ---------------------------------------------------------------------------


class AdaptiveBatchController:
    """Dynamic AIMD batch sizing controller for workload rehearsal probes."""

    def __init__(
        self,
        initial_batch: int = 1000,
        min_batch: int = 100,
        max_batch: int = 50000,
        additive_step: int = 500,
        multiplicative_cut: float = 0.5,
        target_latency_ms: float = 25.0,
    ) -> None:
        self.batch_size = max(min_batch, min(int(initial_batch), max_batch))
        self.min_batch = int(min_batch)
        self.max_batch = int(max_batch)
        self.additive_step = int(additive_step)
        self.multiplicative_cut = float(multiplicative_cut)
        self.target_latency_ms = float(target_latency_ms)

    def observe(self, latency_ms: float, hot_shard_strained: bool = False) -> int:
        if hot_shard_strained or latency_ms > self.target_latency_ms:
            self.batch_size = max(
                self.min_batch,
                int(math.floor(self.batch_size * self.multiplicative_cut)),
            )
        else:
            self.batch_size = min(self.max_batch, self.batch_size + self.additive_step)
        return self.batch_size


def cold_start_byte_weighted_eta(
    total_bytes: int,
    completed_bytes: int,
    elapsed_seconds: float,
    stages: Optional[machine.Stages] = None,
    default_rate_bytes_sec: float = 50.0 * 1024 * 1024,
) -> float:
    """Instant cold-start byte-weighted ETA in seconds."""
    remaining = max(0, int(total_bytes) - int(completed_bytes))
    if remaining == 0:
        return 0.0
    if completed_bytes > 0 and elapsed_seconds > 0:
        measured_rate = completed_bytes / elapsed_seconds
        return remaining / max(1.0, measured_rate)
    if stages is not None:
        servers = stages.servers()
        if servers:
            bound = min(servers.values())
            if bound and bound > 0:
                return remaining / bound
    return remaining / max(1.0, float(default_rate_bytes_sec))


# ---------------------------------------------------------------------------
# Resource Sizing & Candidate Topology Generation
# ---------------------------------------------------------------------------


def _select_shared_correlation_key(table: TableAccessProfile) -> Tuple[str, ...]:
    """Identify the best correlation/join key that minimizes skew and cross-node joins."""
    if table.shared_join_keys:
        counts: Dict[Tuple[str, ...], float] = {}
        for co_table, key_cols in table.shared_join_keys.items():
            weight = float(table.co_accessed_tables.get(co_table, 1.0))
            cols = tuple(str(c) for c in key_cols)
            counts[cols] = counts.get(cols, 0.0) + weight
        best_join_key = max(counts.items(), key=lambda x: x[1])[0]
        skew = table.skew_for(best_join_key)
        if not skew.is_hot_spotted(DistributionStrategy.HASH.value, partition_count=4):
            return best_join_key

    best_key = table.primary_key
    best_score = math.inf
    for cand in table.candidate_shard_keys or (table.primary_key,):
        skew = table.skew_for(cand)
        join_bonus = -0.25 if cand in table.shared_join_keys.values() else 0.0
        score = skew.gini_coefficient + 0.5 * skew.hot_shard_fraction + join_bonus
        if score < best_score:
            best_score = score
            best_key = tuple(cand)
    return best_key


def recommend_resource_sizing(
    evidence: WorkloadAssessmentEvidence,
    topology_kind: str,
    node_count: int,
    table_placements: Dict[str, TablePlacementDecision],
) -> ResourceSizingSpec:
    """Compute evidence-backed resource sizing per node and cluster-wide."""
    n_nodes = max(1, int(node_count))
    snapshots = evidence.load_snapshots or []

    peak_running = max((float(s.get("running_most") or 0.0) for s in snapshots), default=0.0)
    peak_iops = max((float(s.get("iops") or 0.0) for s in snapshots), default=0.0)
    peak_conns = max((int(s.get("connections") or 0) for s in snapshots), default=40)
    peak_cache = max((float(s.get("cache_bytes") or 0.0) for s in snapshots), default=0.0)
    min_hit = min((float(s.get("hit")) for s in snapshots if s.get("hit") is not None), default=0.98)

    total_read_qps = sum(t.point_read_qps + t.range_scan_qps for t in evidence.tables)
    total_write_qps = sum(t.write_qps for t in evidence.tables)

    index_write_amp = 1.0
    cross_index_fanout = 1.0
    max_hot_factor = 1.0
    for t in evidence.tables:
        pl = table_placements.get(t.table_name)
        if pl is not None:
            max_hot_factor = max(max_hot_factor, pl.estimated_hot_shard_load_factor)
            cross_index_fanout = max(cross_index_fanout, float(pl.cross_partition_index_fanout))
        if t.indexes:
            amp = 1.0 + sum(max(0.0, idx.write_amplification_factor - 1.0) for idx in t.indexes)
            index_write_amp = max(index_write_amp, amp)

    workload_cores = (total_read_qps * 0.0008 + total_write_qps * 0.0018 * index_write_amp)
    base_cores = max(peak_running / machine.CPU_SHARE, workload_cores / machine.CPU_SHARE, 2.0)

    if topology_kind in (TargetTopologyKind.DISTRIBUTED.value, TargetTopologyKind.PARTITIONED.value, TargetTopologyKind.HYBRID.value):
        coordination_factor = 1.15 if cross_index_fanout <= 1.5 else 1.35
    else:
        coordination_factor = 1.0

    # A hot shard concentrates load onto one node, requiring higher per-node capacity if unmitigated
    effective_cluster_cores = base_cores * coordination_factor
    per_node_vcpu = max(2, int(math.ceil((effective_cluster_cores / n_nodes) * max(1.0, 0.65 * max_hot_factor))))
    if per_node_vcpu % 2 != 0:
        per_node_vcpu += 1

    working_set_bytes = max(peak_cache, 0.25 * evidence.total_size_bytes)
    mem_gib_total = max(4.0, (working_set_bytes / (0.50 * (1024 ** 3))))
    if min_hit < 0.95:
        mem_gib_total *= 1.5
    per_node_memory_gib = max(4.0, round(math.ceil((mem_gib_total / n_nodes) * 2.0) / 2.0, 1))

    derived_iops = max(peak_iops, total_read_qps * 0.35 + total_write_qps * 2.2 * index_write_amp)
    per_node_iops = max(500, int(math.ceil((derived_iops / n_nodes) * max(1.0, 0.6 * max_hot_factor))))

    import_mb_s = 64.0
    if evidence.deadline_seconds and evidence.deadline_seconds > 0:
        import_mb_s = max(import_mb_s, (evidence.total_import_bytes / (1024 ** 2)) / evidence.deadline_seconds)
    per_node_network_mb_s = round(max(50.0, (import_mb_s / n_nodes) * 1.25), 1)

    conns_per_node = max(25, int(math.ceil((peak_conns * machine.CONNECTION_ROOM) / max(1, n_nodes // 2))))

    matched_name = None
    try:
        cat_types = machine.machines(evidence.cloud, evidence.region) if evidence.cloud else []
    except Exception:
        cat_types = []
    if cat_types:
        fam = machine.DB_FAMILIES.get(evidence.cloud or "")
        viable = [
            m for m in cat_types
            if (fam is None or fam.search(m.name))
            and m.vcpu >= per_node_vcpu
            and m.memory_gib >= per_node_memory_gib * 0.95
            and (m.iops is None or m.iops[0] >= per_node_iops)
        ]
        if viable:
            best_m = min(
                viable,
                key=lambda m: (m.vcpu, m.memory_gib, m.price(evidence.region) or math.inf),
            )
            matched_name = best_m.name

    rationale = (
        f"{n_nodes} node(s) x ({per_node_vcpu} vCPU, {per_node_memory_gib:.1f} GiB, "
        f"{per_node_iops} IOPS, {per_node_network_mb_s:.1f} MiB/s net); "
        f"index write amp {index_write_amp:.2f}x, max shard skew factor {max_hot_factor:.2f}x"
    )
    return ResourceSizingSpec(
        node_count=n_nodes,
        per_node_vcpu=per_node_vcpu,
        per_node_memory_gib=per_node_memory_gib,
        per_node_iops=per_node_iops,
        per_node_network_mb_s=per_node_network_mb_s,
        total_vcpu=per_node_vcpu * n_nodes,
        total_memory_gib=round(per_node_memory_gib * n_nodes, 1),
        total_iops=per_node_iops * n_nodes,
        recommended_connections_per_node=conns_per_node,
        matched_catalog_machine=matched_name,
        sizing_rationale=rationale,
    )


def generate_candidate_designs(
    evidence: WorkloadAssessmentEvidence,
    distributed_nodes: int = 4,
) -> List[CandidateTargetDesign]:
    """Generate candidate colocated, distributed, partitioned, and hybrid target designs."""
    n_dist = max(2, int(distributed_nodes))
    candidates: List[CandidateTargetDesign] = []

    # 1. Candidate A: Colocated single-group design
    colocated_placements: Dict[str, TablePlacementDecision] = {}
    for t in evidence.tables:
        colocated_placements[t.table_name] = TablePlacementDecision(
            table_name=t.table_name,
            topology_kind=TargetTopologyKind.COLOCATED.value,
            distribution_strategy=DistributionStrategy.COLOCATED_GROUP.value,
            shard_key=t.primary_key,
            partition_count=1,
            colocation_group="colocated_primary",
            estimated_hot_shard_load_factor=1.0,
            cross_partition_index_fanout=1,
            rationale="all tables colocated in one tablet/group for zero cross-node join/2PC fanout",
        )
    colocated_sizing = recommend_resource_sizing(
        evidence, TargetTopologyKind.COLOCATED.value, 1, colocated_placements
    )
    candidates.append(
        CandidateTargetDesign(
            design_id="colocated-single-group",
            name="Colocated Single-Group Topology",
            topology_kind=TargetTopologyKind.COLOCATED.value,
            node_count=1,
            table_placements=colocated_placements,
            sizing=colocated_sizing,
            description="Single colocated group eliminating distributed 2PC and secondary index fanout",
        )
    )

    # 2. Candidate B: Naive Range-Partitioned / Bulk-Import-First Design (prone to monotonic hot shards
    #    and cross-partition transactions when joins are on correlation keys rather than primary range key)
    range_placements: Dict[str, TablePlacementDecision] = {}
    for t in evidence.tables:
        pk = t.primary_key
        skew = t.skew_for(pk)
        hot_share = skew.effective_hot_shard_share(DistributionStrategy.RANGE.value, n_dist)
        hot_factor = hot_share * n_dist
        unaligned_indexes = sum(1 for idx in t.indexes if not idx.aligned_with_shard_key(pk))
        fanout = 1 + unaligned_indexes * max(1, n_dist // 2)
        range_placements[t.table_name] = TablePlacementDecision(
            table_name=t.table_name,
            topology_kind=TargetTopologyKind.PARTITIONED.value,
            distribution_strategy=DistributionStrategy.RANGE.value,
            shard_key=pk,
            partition_count=n_dist,
            colocation_group=None,
            estimated_hot_shard_load_factor=round(hot_factor, 2),
            cross_partition_index_fanout=fanout,
            rationale="range-partitioned on primary key for fast sequential bulk import",
        )
    range_sizing = recommend_resource_sizing(
        evidence, TargetTopologyKind.PARTITIONED.value, n_dist, range_placements
    )
    candidates.append(
        CandidateTargetDesign(
            design_id="partitioned-range-bulk-first",
            name="Range-Partitioned Bulk-Import-First Topology",
            topology_kind=TargetTopologyKind.PARTITIONED.value,
            node_count=n_dist,
            table_placements=range_placements,
            sizing=range_sizing,
            description="Sequential range partitions maximizing raw bulk load speed without co-location",
        )
    )

    # 3. Candidate C: Balanced Locality-Aware Co-Partitioned + Colocated Hybrid Design
    balanced_placements: Dict[str, TablePlacementDecision] = {}
    small_cutoff = min(2 * 1024 ** 3, max(64 * 1024 ** 2, evidence.colocation_working_set_limit_bytes // 8))
    for t in evidence.tables:
        if t.is_reference_candidate or t.size_bytes <= small_cutoff:
            balanced_placements[t.table_name] = TablePlacementDecision(
                table_name=t.table_name,
                topology_kind=TargetTopologyKind.COLOCATED.value,
                distribution_strategy=(
                    DistributionStrategy.REPLICATED_REFERENCE.value
                    if t.is_reference_candidate
                    else DistributionStrategy.COLOCATED_GROUP.value
                ),
                shard_key=t.primary_key,
                partition_count=1,
                colocation_group="colocated_hot_dim",
                estimated_hot_shard_load_factor=1.0,
                cross_partition_index_fanout=1,
                rationale="small or reference table kept colocated/replicated for local joins",
            )
        else:
            corr_key = _select_shared_correlation_key(t)
            skew = t.skew_for(corr_key)
            hot_share = skew.effective_hot_shard_share(DistributionStrategy.HASH.value, n_dist)
            hot_factor = max(1.0, hot_share * n_dist)
            unaligned = sum(1 for idx in t.indexes if not idx.aligned_with_shard_key(corr_key))
            fanout = 1 + unaligned
            balanced_placements[t.table_name] = TablePlacementDecision(
                table_name=t.table_name,
                topology_kind=TargetTopologyKind.DISTRIBUTED.value,
                distribution_strategy=DistributionStrategy.HASH.value,
                shard_key=corr_key,
                partition_count=n_dist,
                colocation_group=f"cohashed_{','.join(corr_key)}",
                estimated_hot_shard_load_factor=round(hot_factor, 2),
                cross_partition_index_fanout=fanout,
                rationale=(
                    f"hash-distributed on correlation key ({','.join(corr_key)}) "
                    "to balance shards and keep co-accessed joins local"
                ),
            )
    balanced_sizing = recommend_resource_sizing(
        evidence, TargetTopologyKind.HYBRID.value, n_dist, balanced_placements
    )
    candidates.append(
        CandidateTargetDesign(
            design_id="distributed-cohashed-balanced",
            name="Balanced Co-Hashed Distributed + Colocated Topology",
            topology_kind=TargetTopologyKind.HYBRID.value,
            node_count=n_dist,
            table_placements=balanced_placements,
            sizing=balanced_sizing,
            description=(
                "Co-hashes large tables on shared correlation keys and colocates reference/small "
                "tables for balanced shard load and low transaction fanout"
            ),
        )
    )

    return candidates


# ---------------------------------------------------------------------------
# Rehearsal Simulator & Evidence Evaluator
# ---------------------------------------------------------------------------


def rehearse_design(
    design: CandidateTargetDesign,
    evidence: WorkloadAssessmentEvidence,
    sample_fraction: float = 1.0,
    custom_probe: Optional[Callable[[CandidateTargetDesign, WorkloadAssessmentEvidence], RehearsalMeasurement]] = None,
) -> RehearsalMeasurement:
    """Rehearse a candidate design against measured workload evidence."""
    if custom_probe is not None:
        return custom_probe(design, evidence)

    total_import = max(1, evidence.total_import_bytes)
    n_nodes = max(1, design.node_count)

    # Bulk import throughput calculation
    base_node_write_rate = 65.0 * 1024 * 1024
    if evidence.stages is not None:
        srv = evidence.stages.servers()
        if srv:
            base_node_write_rate = max(20.0 * 1024 * 1024, min(srv.values()))

    range_tables = sum(
        1 for p in design.table_placements.values()
        if p.distribution_strategy == DistributionStrategy.RANGE.value
    )
    hash_tables = sum(
        1 for p in design.table_placements.values()
        if p.distribution_strategy == DistributionStrategy.HASH.value
    )

    if range_tables > 0 and hash_tables == 0:
        # Range bulk load avoids hash-split scatter and secondary index cross-node checks during ingest
        import_scale = min(float(n_nodes) * 0.92, 6.0)
    elif hash_tables > 0:
        import_scale = min(float(n_nodes) * 0.72, 4.8)
    else:
        import_scale = 1.0

    bulk_rate = max(10.0 * 1024 * 1024, base_node_write_rate * import_scale)
    bulk_seconds = total_import / bulk_rate

    # Evaluate shard skew & hot shard utilization
    max_hot_factor = 1.0
    hot_table = ""
    total_index_fanout_ops = 0.0
    total_write_qps = 0.0

    for t in evidence.tables:
        pl = design.table_placements.get(t.table_name)
        if pl is None:
            continue
        skew = t.skew_for(pl.shard_key)
        hot_share = skew.effective_hot_shard_share(pl.distribution_strategy, pl.partition_count)
        factor = hot_share * max(1, pl.partition_count)
        if factor > max_hot_factor:
            max_hot_factor = factor
            hot_table = t.table_name

        unaligned = sum(1 for idx in t.indexes if not idx.aligned_with_shard_key(pl.shard_key))
        total_index_fanout_ops += t.write_qps * unaligned * (1 if pl.partition_count <= 1 else 2)
        total_write_qps += t.write_qps

    # Evaluate transaction fanout (cross-partition share)
    tx = evidence.transactions
    total_tx = max(1.0, tx.total_tps)
    if design.node_count <= 1 or all(p.partition_count <= 1 for p in design.table_placements.values()):
        cross_partition_share = 0.0
    else:
        # Check whether co-accessed tables share the same colocation_group or shard_key
        co_access_total = 0.0
        co_access_broken = 0.0
        for t in evidence.tables:
            pl_a = design.table_placements.get(t.table_name)
            if pl_a is None:
                continue
            for other_name, qps_weight in t.co_accessed_tables.items():
                pl_b = design.table_placements.get(other_name)
                if pl_b is None:
                    continue
                co_access_total += qps_weight
                if pl_b.distribution_strategy == DistributionStrategy.REPLICATED_REFERENCE.value:
                    continue
                if pl_a.colocation_group and pl_a.colocation_group == pl_b.colocation_group:
                    continue
                join_key = t.shared_join_keys.get(other_name)
                if (
                    pl_a.distribution_strategy == DistributionStrategy.HASH.value
                    and pl_b.distribution_strategy == DistributionStrategy.HASH.value
                    and join_key
                    and pl_a.shard_key == tuple(join_key)
                    and pl_b.shard_key == tuple(join_key)
                ):
                    continue
                co_access_broken += qps_weight

        broken_join_ratio = (co_access_broken / co_access_total) if co_access_total > 0 else 0.5
        co_keyed_cross = (tx.co_keyed_join_tps / total_tx) * broken_join_ratio
        cross_key_part = (tx.cross_key_tps / total_tx) * (1.0 - 1.0 / max(2, design.node_count))
        index_cross = min(0.35, (total_index_fanout_ops / max(1.0, total_write_qps * 4.0)) * 0.25)
        cross_partition_share = min(0.98, co_keyed_cross + cross_key_part + index_cross)

    # Compute CPU utilization across shards consistent with sized vCPU capacity
    workload_cores = 0.0
    for t in evidence.tables:
        amp = 1.0 + sum(max(0.0, idx.write_amplification_factor - 1.0) for idx in t.indexes)
        workload_cores += (t.point_read_qps + t.range_scan_qps) * 0.0008 + t.write_qps * 0.0018 * amp

    spill_multiplier = 1.0
    if design.node_count <= 1 and evidence.total_size_bytes > evidence.colocation_working_set_limit_bytes:
        spill_ratio = evidence.total_size_bytes / max(1, evidence.colocation_working_set_limit_bytes)
        spill_multiplier = min(3.0, 1.0 + 0.45 * math.log2(max(1.0, spill_ratio)))

    coord_cpu_overhead = 1.0 + 0.85 * cross_partition_share
    effective_cores_used = workload_cores * coord_cpu_overhead * spill_multiplier
    total_vcpu = max(1.0, float(design.sizing.total_vcpu))
    median_util = min(0.99, effective_cores_used / total_vcpu)
    if design.node_count <= 1 and all(p.partition_count <= 1 for p in design.table_placements.values()):
        max_util = median_util
    else:
        max_util = min(1.0, median_util * max(1.0, max_hot_factor * 0.85))
        if max_hot_factor >= 2.2:
            max_util = max(max_util, min(0.99, 0.42 * max_hot_factor))

    # Latency calculation: local latency + hot-shard queueing + 2PC & cross-partition RTT
    queueing_penalty_ms = 0.0
    if max_util > 0.70:
        queueing_penalty_ms = ((max_util - 0.70) / max(0.03, 1.0 - max_util)) * 4.5

    two_pc_penalty_ms = cross_partition_share * (
        tx.two_phase_commit_overhead_ms + tx.inter_node_rtt_ms * max(1.0, tx.tables_touched_avg)
    )
    global_idx_stall_ms = (
        (total_index_fanout_ops / max(1.0, total_write_qps)) * tx.inter_node_rtt_ms
        if design.node_count > 1 and total_write_qps > 0
        else 0.0
    )

    had_rows = sum(t.row_count for t in evidence.tables) * max(0.001, min(1.0, sample_fraction))
    now_rows = sum(t.row_count for t in evidence.tables)
    p90_widen = rehearsal.sample_spread(had_rows, now_rows)

    p95_latency_ms = (
        tx.base_local_p95_latency_ms * spill_multiplier
        + queueing_penalty_ms
        + two_pc_penalty_ms
        + global_idx_stall_ms
    ) * (1.0 + p90_widen)

    total_qps = max(1.0, sum(t.total_qps for t in evidence.tables))
    steady_tps_cap = total_qps / max(0.05, max_util)

    return RehearsalMeasurement(
        design_id=design.design_id,
        bulk_import_rate_bytes_sec=round(bulk_rate, 2),
        bulk_import_seconds=round(bulk_seconds, 2),
        steady_state_tps_capacity=round(steady_tps_cap, 2),
        steady_state_p95_latency_ms=round(p95_latency_ms, 2),
        max_shard_cpu_utilization=round(max_util, 4),
        median_shard_cpu_utilization=round(median_util, 4),
        cross_partition_txn_share=round(cross_partition_share, 4),
        distributed_2pc_latency_penalty_ms=round(two_pc_penalty_ms, 2),
        global_index_write_stall_ms=round(global_idx_stall_ms, 2),
        hot_shard_table=hot_table,
        truth=EvidenceTruth.REHEARSED.value,
        sample_fraction=float(sample_fraction),
        p90_widening_factor=round(p90_widen, 4),
    )


def evaluate_candidate_design(
    design: CandidateTargetDesign,
    measurement: RehearsalMeasurement,
    evidence: WorkloadAssessmentEvidence,
) -> DesignEvaluation:
    """Score a candidate design end-to-end across bulk import and steady-state workload."""
    tx = evidence.transactions
    horizon = max(3600.0, float(evidence.workload_horizon_seconds))

    bulk_secs = float(measurement.bulk_import_seconds)
    steady_base_secs = horizon * float(measurement.median_shard_cpu_utilization)

    # Hot shard penalty: severe when max_shard_cpu_utilization >= 0.85 or ratio to median is high
    skew_ratio = measurement.max_shard_cpu_utilization / max(0.05, measurement.median_shard_cpu_utilization)
    has_hot_shard = (
        measurement.max_shard_cpu_utilization >= 0.82
        or (design.node_count > 1 and skew_ratio >= 2.0 and measurement.max_shard_cpu_utilization >= 0.65)
    )
    hot_shard_penalty = 0.0
    if has_hot_shard:
        excess = max(0.1, measurement.max_shard_cpu_utilization - 0.60)
        hot_shard_penalty = horizon * (excess * 1.8 + 0.25 * max(0.0, skew_ratio - 1.5))

    # Cross-partition fanout penalty: captures 2PC coordination & network RTT across the workload horizon
    has_expensive_cross_txns = (
        measurement.cross_partition_txn_share >= 0.35
        or measurement.distributed_2pc_latency_penalty_ms >= tx.sla_p95_latency_ms * 0.40
    )
    fanout_penalty = (
        horizon
        * measurement.cross_partition_txn_share
        * (1.0 + measurement.distributed_2pc_latency_penalty_ms / max(1.0, tx.sla_p95_latency_ms))
    )

    index_penalty = horizon * (measurement.global_index_write_stall_ms / max(5.0, tx.sla_p95_latency_ms)) * 0.5

    is_static_only = (
        measurement.truth == EvidenceTruth.STATIC_CLASS_ONLY.value
        or bool(design.brand_static_class and measurement.truth not in (EvidenceTruth.REHEARSED.value, EvidenceTruth.MEASURED.value))
    )
    static_penalty = horizon * 0.50 if is_static_only else 0.0

    # Colocated working set overflow penalty if single-group holds far more than cache limit
    colocated_overflow_penalty = 0.0
    if (
        design.topology_kind == TargetTopologyKind.COLOCATED.value
        and evidence.total_size_bytes > evidence.colocation_working_set_limit_bytes
    ):
        overflow_factor = evidence.total_size_bytes / max(1, evidence.colocation_working_set_limit_bytes)
        colocated_overflow_penalty = horizon * 0.35 * math.log2(max(1.0, overflow_factor))

    total_score = (
        bulk_secs
        + steady_base_secs
        + hot_shard_penalty
        + fanout_penalty
        + index_penalty
        + static_penalty
        + colocated_overflow_penalty
    )

    meets_deadline = (
        True if not evidence.deadline_seconds else bulk_secs <= evidence.deadline_seconds
    )
    meets_sla = measurement.steady_state_p95_latency_ms <= tx.sla_p95_latency_ms

    reasons: List[str] = []
    badge = CareBadge.PASS.value

    if is_static_only:
        badge = CareBadge.ACTION.value
        reasons.append(
            f"static brand class ({design.brand_static_class or 'unverified'}) lacks rehearsed workload evidence"
        )
    if has_hot_shard:
        badge = CareBadge.ACTION.value
        reasons.append(
            f"hot shard saturation ({measurement.max_shard_cpu_utilization:.0%} max vs "
            f"{measurement.median_shard_cpu_utilization:.0%} median CPU on {measurement.hot_shard_table or 'primary'})"
        )
    if has_expensive_cross_txns:
        badge = CareBadge.ACTION.value
        reasons.append(
            f"expensive cross-partition transactions ({measurement.cross_partition_txn_share:.0%} cross-node share, "
            f"+{measurement.distributed_2pc_latency_penalty_ms:.1f} ms 2PC penalty)"
        )
    if not meets_sla:
        badge = CareBadge.ACTION.value
        reasons.append(
            f"steady-state p95 latency {measurement.steady_state_p95_latency_ms:.1f} ms exceeds "
            f"SLA {tx.sla_p95_latency_ms:.1f} ms"
        )
    if not meets_deadline:
        if badge != CareBadge.ACTION.value:
            badge = CareBadge.REVIEW.value
        reasons.append(
            f"bulk import {bulk_secs:.1f}s exceeds deadline {evidence.deadline_seconds:.1f}s"
        )

    if badge == CareBadge.PASS.value:
        if measurement.sample_fraction < 0.20:
            badge = CareBadge.REVIEW.value
            reasons.append(
                f"rehearsed on {measurement.sample_fraction:.1%} sample (p90 widened by {measurement.p90_widening_factor:.0%})"
            )
        elif colocated_overflow_penalty > 0:
            badge = CareBadge.REVIEW.value
            reasons.append("dataset exceeds single-group colocation working set threshold")
        elif 0.05 < measurement.cross_partition_txn_share < 0.35:
            badge = CareBadge.BENIGN.value
            reasons.append(
                f"minor cross-partition traffic ({measurement.cross_partition_txn_share:.0%}) within latency SLA"
            )
        else:
            reasons.append("balanced shard load and local transaction execution within SLA")

    summary = (
        f"{design.name} [{badge}]: score={total_score:.1f}s "
        f"(import={bulk_secs:.1f}s, steady={steady_base_secs:.1f}s, "
        f"hot_penalty={hot_shard_penalty:.1f}s, fanout_penalty={fanout_penalty:.1f}s, "
        f"p95={measurement.steady_state_p95_latency_ms:.1f}ms)"
    )

    return DesignEvaluation(
        design=design,
        measurement=measurement,
        bulk_import_seconds=round(bulk_secs, 2),
        steady_state_workload_seconds=round(steady_base_secs, 2),
        hot_shard_penalty_seconds=round(hot_shard_penalty, 2),
        cross_partition_fanout_penalty_seconds=round(fanout_penalty, 2),
        index_amplification_penalty_seconds=round(index_penalty, 2),
        end_to_end_effective_cost_score=round(total_score, 2),
        meets_import_deadline=meets_deadline,
        meets_latency_sla=meets_sla,
        has_hot_shard=has_hot_shard,
        has_expensive_cross_partition_txns=has_expensive_cross_txns,
        is_static_brand_only=is_static_only,
        care_badge=badge,
        care_reasons=reasons,
        recommendation_summary=summary,
    )


class PlacementAdviceEngine:
    """Coordinates target topology design generation, rehearsal, ranking, and reporting."""

    def __init__(
        self,
        checkpoint_store: Optional[PlacementCheckpointStore] = None,
        batch_controller: Optional[AdaptiveBatchController] = None,
    ) -> None:
        self.checkpoint_store = checkpoint_store
        self.batch_controller = batch_controller or AdaptiveBatchController()

    def rehearse_and_rank(
        self,
        evidence: WorkloadAssessmentEvidence,
        candidates: Optional[Sequence[CandidateTargetDesign]] = None,
        measurements: Optional[Dict[str, RehearsalMeasurement]] = None,
        sample_fraction: float = 1.0,
        reject_static_brand_classes: bool = False,
    ) -> List[DesignEvaluation]:
        """Rehearse candidate target designs and rank them by end-to-end workload performance."""
        designs = list(candidates) if candidates is not None else generate_candidate_designs(evidence)
        given_measurements = dict(measurements or {})
        evaluations: List[DesignEvaluation] = []

        for d in designs:
            if reject_static_brand_classes and d.brand_static_class and d.design_id not in given_measurements:
                raise StaticBrandClassRefusedError(
                    f"Candidate {d.design_id} specifies static brand class {d.brand_static_class!r} "
                    "without rehearsal evidence"
                )

            if self.checkpoint_store is not None and self.checkpoint_store.is_completed(d.design_id):
                cached_meas = self.checkpoint_store.get_result(d.design_id)
                if isinstance(cached_meas, dict):
                    meas = RehearsalMeasurement(**cached_meas)
                else:
                    meas = rehearse_design(d, evidence, sample_fraction=sample_fraction)
            elif d.design_id in given_measurements:
                meas = given_measurements[d.design_id]
                if self.checkpoint_store is not None:
                    self.checkpoint_store.mark_completed(
                        d.design_id,
                        cursor=("design", d.design_id),
                        result=meas.to_dict(),
                    )
            elif d.brand_static_class:
                # Unrehearsed static brand class: evaluate with truth=STATIC_CLASS_ONLY
                raw_meas = rehearse_design(d, evidence, sample_fraction=sample_fraction)
                meas = RehearsalMeasurement(
                    **{
                        **raw_meas.to_dict(),
                        "truth": EvidenceTruth.STATIC_CLASS_ONLY.value,
                    }
                )
            else:
                meas = rehearse_design(d, evidence, sample_fraction=sample_fraction)
                self.batch_controller.observe(
                    meas.steady_state_p95_latency_ms,
                    hot_shard_strained=(meas.max_shard_cpu_utilization >= 0.82),
                )
                if self.checkpoint_store is not None:
                    self.checkpoint_store.mark_completed(
                        d.design_id,
                        cursor=("design", d.design_id),
                        result=meas.to_dict(),
                    )

            evaluations.append(evaluate_candidate_design(d, meas, evidence))

        badge_priority = {
            CareBadge.PASS.value: 0,
            CareBadge.BENIGN.value: 1,
            CareBadge.REVIEW.value: 2,
            CareBadge.ACTION.value: 3,
        }
        evaluations.sort(
            key=lambda ev: (
                1 if ev.is_static_brand_only else 0,
                0 if ev.meets_latency_sla else 1,
                badge_priority.get(ev.care_badge, 99),
                ev.end_to_end_effective_cost_score,
            )
        )
        return evaluations

    def recommend_best_design(
        self,
        evidence: WorkloadAssessmentEvidence,
        candidates: Optional[Sequence[CandidateTargetDesign]] = None,
        measurements: Optional[Dict[str, RehearsalMeasurement]] = None,
        sample_fraction: float = 1.0,
    ) -> Tuple[DesignEvaluation, List[DesignEvaluation]]:
        ranked = self.rehearse_and_rank(
            evidence=evidence,
            candidates=candidates,
            measurements=measurements,
            sample_fraction=sample_fraction,
        )
        if not ranked:
            raise PlacementAdviceError("No candidate target designs available to evaluate")
        return ranked[0], ranked

    def render_report(
        self,
        evidence: WorkloadAssessmentEvidence,
        ranked_evaluations: Sequence[DesignEvaluation],
    ) -> str:
        """Render a pure-ASCII UNIX enterprise placement and sizing report."""
        if not ranked_evaluations:
            return "PLACEMENT ADVICE REPORT: ACTION\nNo candidate designs evaluated."

        winner = ranked_evaluations[0]
        lines: List[str] = []
        lines.append("=" * 92)
        lines.append(f"TARGET TOPOLOGY AND SIZING ADVICE REPORT: {winner.care_badge}")
        lines.append("=" * 92)
        lines.append(
            f"Source: {evidence.source_host} -> Target: {evidence.target_host} | "
            f"Engine: {evidence.engine} | Data: {evidence.total_size_bytes / (1024 ** 3):.2f} GiB | "
            f"Workload TPS: {evidence.transactions.total_tps:.1f}"
        )
        lines.append(
            f"Selected Design: {winner.design.design_id} ({winner.design.topology_kind}, "
            f"{winner.design.node_count} node(s)) | Truth: {winner.measurement.truth}"
        )
        lines.append(f"Sizing: {winner.design.sizing.sizing_rationale}")
        lines.append("-" * 92)
        lines.append(
            f"{'RANK':<5} {'DESIGN ID':<30} {'IMPORT(s)':<10} {'P95(ms)':<9} "
            f"{'MAX_CPU':<8} {'X_PART':<8} {'E2E_SCORE':<11} {'BADGE'}"
        )
        lines.append("-" * 92)
        for idx, ev in enumerate(ranked_evaluations, start=1):
            m = ev.measurement
            lines.append(
                f"{idx:<5} {ev.design.design_id:<30} {ev.bulk_import_seconds:<10.1f} "
                f"{m.steady_state_p95_latency_ms:<9.1f} {m.max_shard_cpu_utilization * 100:<7.1f}% "
                f"{m.cross_partition_txn_share * 100:<7.1f}% {ev.end_to_end_effective_cost_score:<11.1f} "
                f"{ev.care_badge}"
            )
            for reason in ev.care_reasons:
                lines.append(f"      reason: {reason}")
        lines.append("-" * 92)
        lines.append("TABLE PLACEMENT PLAN (SELECTED DESIGN):")
        for t_name, pl in sorted(winner.design.table_placements.items()):
            key_str = ",".join(pl.shard_key) if pl.shard_key else "none"
            lines.append(
                f"  - {t_name}: strategy={pl.distribution_strategy} key=({key_str}) "
                f"partitions={pl.partition_count} hot_factor={pl.estimated_hot_shard_load_factor:.2f}x "
                f"idx_fanout={pl.cross_partition_index_fanout}"
            )
        lines.append("-" * 92)
        lines.append(f"OVERALL CARE BADGE: {winner.care_badge}")
        lines.append("=" * 92)
        return "\n".join(lines)


def assess_placement_items(
    evidence: WorkloadAssessmentEvidence,
    candidates: Optional[Sequence[CandidateTargetDesign]] = None,
    measurements: Optional[Dict[str, RehearsalMeasurement]] = None,
) -> List[Dict[str, str]]:
    """Return structured assessment rows compatible with machine.assess_items."""
    engine = PlacementAdviceEngine()
    winner, ranked = engine.recommend_best_design(
        evidence=evidence,
        candidates=candidates,
        measurements=measurements,
    )
    level_map = {
        CareBadge.PASS.value: "pass",
        CareBadge.BENIGN.value: "pass",
        CareBadge.REVIEW.value: "warn",
        CareBadge.ACTION.value: "fail",
    }
    items: List[Dict[str, str]] = [
        {
            "level": level_map.get(winner.care_badge, "warn"),
            "care_badge": winner.care_badge,
            "scope": "sizing",
            "item": "target topology and workload sizing",
            "detail": (
                f"{winner.recommendation_summary}; sizing: {winner.design.sizing.sizing_rationale}"
            ),
        }
    ]
    for loser in ranked[1:]:
        items.append(
            {
                "level": "pass" if loser.care_badge in (CareBadge.PASS.value, CareBadge.BENIGN.value) else "warn",
                "care_badge": loser.care_badge,
                "scope": "sizing",
                "item": f"rehearsed alternative: {loser.design.design_id}",
                "detail": (
                    f"{loser.recommendation_summary} -- rejected: {'; '.join(loser.care_reasons)}"
                ),
            }
        )
    return items
