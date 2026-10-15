import collections
import concurrent.futures
import enum
import gzip
import hashlib
import io
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

try:
    import lz4.frame as lz4_frame
except ImportError:
    lz4_frame = None

try:
    import zstandard as zstd
except ImportError:
    zstd = None

try:
    import crc32c as _crc32c_lib
except ImportError:
    _crc32c_lib = None

_CRC32C_TABLE = None


def _init_crc32c_table():
    global _CRC32C_TABLE
    if _CRC32C_TABLE is not None:
        return _CRC32C_TABLE
    poly = 0x82F63B78
    tbl = []
    for i in range(256):
        c = i
        for _ in range(8):
            if c & 1:
                c = (c >> 1) ^ poly
            else:
                c >>= 1
        tbl.append(c)
    _CRC32C_TABLE = tbl
    return _CRC32C_TABLE


def compute_crc32c(data: bytes, initial_crc: int = 0) -> int:
    if _crc32c_lib is not None:
        return _crc32c_lib.crc32c(data, initial_crc)
    tbl = _init_crc32c_table()
    crc = initial_crc ^ 0xFFFFFFFF
    for b in data:
        crc = tbl[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


@dataclass
class DatabaseStressMetrics:
    cpu_pct: float
    io_wait_pct: float
    io_latency_ms: float = 0.0
    active_sessions_ratio: float = 0.0
    replication_lag_sec: float = 0.0


@dataclass
class ThrottleState:
    workers: int
    sleep_delay: float
    stress_level: str
    reason: str
    target_scale_factor: float


class AdaptiveMoverThrottler:
    def __init__(
        self,
        min_workers: int = 1,
        max_workers: int = 16,
        initial_workers: int = 4,
        target_cpu: float = 65.0,
        critical_cpu: float = 85.0,
        target_io_wait: float = 20.0,
        critical_io_wait: float = 40.0,
        backoff_factor: float = 0.5,
        step_up: int = 1,
        cooldown_sec: float = 1.0,
    ):
        self.min_workers = max(1, min_workers)
        self.max_workers = max(self.min_workers, max_workers)
        self.workers = max(self.min_workers, min(initial_workers, self.max_workers))
        self.target_cpu = target_cpu
        self.critical_cpu = critical_cpu
        self.target_io_wait = target_io_wait
        self.critical_io_wait = critical_io_wait
        self.backoff_factor = backoff_factor
        self.step_up = step_up
        self.cooldown_sec = cooldown_sec
        self.sleep_delay = 0.0
        self.last_adjustment = 0.0
        self.stress_history: List[Tuple[float, str, DatabaseStressMetrics]] = []
        self._lock = threading.Lock()

    def evaluate(self, metrics: DatabaseStressMetrics, now: Optional[float] = None) -> ThrottleState:
        with self._lock:
            t = time.time() if now is None else now
            is_critical_cpu = metrics.cpu_pct >= self.critical_cpu
            is_critical_io = metrics.io_wait_pct >= self.critical_io_wait or metrics.io_latency_ms >= 100.0
            is_warn_cpu = metrics.cpu_pct >= self.target_cpu
            is_warn_io = metrics.io_wait_pct >= self.target_io_wait

            if is_critical_cpu or is_critical_io:
                level = "CRITICAL"
                reasons = []
                if is_critical_cpu:
                    reasons.append(f"CPU {metrics.cpu_pct:.1f}% >= {self.critical_cpu:.1f}%")
                if is_critical_io:
                    reasons.append(f"IO wait {metrics.io_wait_pct:.1f}% / {metrics.io_latency_ms:.1f}ms")
                reason = "Emergency throttle: " + ", ".join(reasons)
                new_workers = max(self.min_workers, int(math.floor(self.workers * self.backoff_factor)))
                self.workers = new_workers
                self.sleep_delay = min(5.0, max(0.5, self.sleep_delay * 2.0 or 0.5))
                self.last_adjustment = t
                scale = float(self.workers) / float(self.max_workers)
            elif is_warn_cpu or is_warn_io:
                level = "WARNING"
                reasons = []
                if is_warn_cpu:
                    reasons.append(f"CPU {metrics.cpu_pct:.1f}%")
                if is_warn_io:
                    reasons.append(f"IO wait {metrics.io_wait_pct:.1f}%")
                reason = "Caution: " + ", ".join(reasons)
                if self.workers > self.min_workers and (t - self.last_adjustment) >= self.cooldown_sec:
                    self.workers = max(self.min_workers, self.workers - 1)
                    self.last_adjustment = t
                self.sleep_delay = min(1.0, max(0.1, self.sleep_delay or 0.1))
                scale = float(self.workers) / float(self.max_workers)
            else:
                level = "HEALTHY"
                reason = f"Normal: CPU {metrics.cpu_pct:.1f}%, IO {metrics.io_wait_pct:.1f}%"
                if (t - self.last_adjustment) >= self.cooldown_sec:
                    if self.workers < self.max_workers:
                        self.workers = min(self.max_workers, self.workers + self.step_up)
                    self.sleep_delay = max(0.0, self.sleep_delay * 0.5 - 0.05)
                    self.last_adjustment = t
                scale = float(self.workers) / float(self.max_workers)

            self.stress_history.append((t, level, metrics))
            return ThrottleState(
                workers=self.workers,
                sleep_delay=self.sleep_delay,
                stress_level=level,
                reason=reason,
                target_scale_factor=round(scale, 3),
            )

    def get_workers(self) -> int:
        with self._lock:
            return self.workers

    def get_sleep_delay(self) -> float:
        with self._lock:
            return self.sleep_delay

    def should_throttle(self) -> bool:
        with self._lock:
            return self.sleep_delay > 0.0 or self.workers == self.min_workers

    def reset(self, initial_workers: Optional[int] = None):
        with self._lock:
            self.workers = initial_workers or self.min_workers
            self.sleep_delay = 0.0
            self.last_adjustment = 0.0
            self.stress_history.clear()


@dataclass
class LeaseRecord:
    holder_id: str
    epoch: int
    acquired_at: float
    expires_at: float
    last_heartbeat: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self, now: Optional[float] = None) -> bool:
        t = time.time() if now is None else now
        return t >= self.expires_at


class FencingViolation(RuntimeError):
    pass


class AtomicLeaseStore:
    def __init__(self):
        self._record: Optional[LeaseRecord] = None
        self._lock = threading.Lock()

    def get_lease(self) -> Optional[LeaseRecord]:
        with self._lock:
            if self._record is None:
                return None
            return LeaseRecord(
                holder_id=self._record.holder_id,
                epoch=self._record.epoch,
                acquired_at=self._record.acquired_at,
                expires_at=self._record.expires_at,
                last_heartbeat=self._record.last_heartbeat,
                metadata=dict(self._record.metadata),
            )

    def try_acquire(
        self,
        candidate_id: str,
        term_sec: float,
        expected_epoch: Optional[int] = None,
        now: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Optional[LeaseRecord]]:
        with self._lock:
            t = time.time() if now is None else now
            if self._record is None or self._record.is_expired(t):
                new_epoch = (self._record.epoch + 1) if self._record else 1
                if expected_epoch is not None and expected_epoch != (new_epoch - 1):
                    return False, self._record
                self._record = LeaseRecord(
                    holder_id=candidate_id,
                    epoch=new_epoch,
                    acquired_at=t,
                    expires_at=t + term_sec,
                    last_heartbeat=t,
                    metadata=metadata or {},
                )
                return True, self._record
            if self._record.holder_id == candidate_id:
                self._record.last_heartbeat = t
                self._record.expires_at = t + term_sec
                if metadata:
                    self._record.metadata.update(metadata)
                return True, self._record
            return False, self._record

    def renew(
        self,
        holder_id: str,
        epoch: int,
        term_sec: float,
        now: Optional[float] = None,
    ) -> bool:
        with self._lock:
            t = time.time() if now is None else now
            if self._record is None:
                return False
            if self._record.holder_id == holder_id and self._record.epoch == epoch:
                if not self._record.is_expired(t):
                    self._record.last_heartbeat = t
                    self._record.expires_at = t + term_sec
                    return True
            return False

    def release(self, holder_id: str, epoch: int) -> bool:
        with self._lock:
            if self._record and self._record.holder_id == holder_id and self._record.epoch == epoch:
                self._record.expires_at = 0.0
                return True
            return False


class MoverHACoordinator:
    def __init__(
        self,
        node_id: str,
        store: AtomicLeaseStore,
        lease_term_seconds: float = 3.0,
        heartbeat_interval: float = 0.8,
        on_promoted: Optional[Callable[[int], None]] = None,
        on_demoted: Optional[Callable[[int], None]] = None,
    ):
        if lease_term_seconds >= 5.0:
            raise ValueError(f"lease_term_seconds must be < 5.0s for fast standby takeover, got {lease_term_seconds}")
        self.node_id = node_id
        self.store = store
        self.lease_term_seconds = lease_term_seconds
        self.heartbeat_interval = heartbeat_interval
        self.on_promoted = on_promoted
        self.on_demoted = on_demoted
        self.current_role = "STANDBY"
        self.epoch = 0
        self.running = False
        self.takeover_duration: Optional[float] = None
        self._last_tick_time = 0.0
        self._leader_detected_down_at: Optional[float] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def is_leader(self) -> bool:
        with self._lock:
            return self.current_role == "LEADER"

    def current_epoch(self) -> int:
        with self._lock:
            return self.epoch

    def tick(self, now: Optional[float] = None) -> str:
        with self._lock:
            t = time.time() if now is None else now
            self._last_tick_time = t
            lease = self.store.get_lease()

            if self.current_role == "LEADER":
                ok = self.store.renew(self.node_id, self.epoch, self.lease_term_seconds, now=t)
                if not ok:
                    self.current_role = "STANDBY"
                    old_epoch = self.epoch
                    self.epoch = 0
                    if self.on_demoted:
                        self.on_demoted(old_epoch)
                    return "DEMOTED"
                return "LEADER"

            if lease is None or lease.is_expired(t):
                if self._leader_detected_down_at is None and lease is not None:
                    self._leader_detected_down_at = lease.expires_at
                ok, rec = self.store.try_acquire(self.node_id, self.lease_term_seconds, now=t)
                if ok and rec and rec.holder_id == self.node_id:
                    self.current_role = "LEADER"
                    self.epoch = rec.epoch
                    if self._leader_detected_down_at is not None:
                        self.takeover_duration = max(0.0, t - self._leader_detected_down_at)
                    else:
                        self.takeover_duration = 0.0
                    if self.on_promoted:
                        self.on_promoted(self.epoch)
                    return "PROMOTED"
            else:
                self._leader_detected_down_at = None

            return "STANDBY"

    def verify_write_fenced(self, write_epoch: int, now: Optional[float] = None):
        with self._lock:
            if self.current_role != "LEADER" or self.epoch != write_epoch:
                raise FencingViolation(
                    f"Write with epoch {write_epoch} rejected. Node {self.node_id} role={self.current_role}, current epoch={self.epoch}"
                )
            lease = self.store.get_lease()
            t = now if now is not None else (self._last_tick_time if self._last_tick_time > 0 else time.time())
            if lease is None or lease.holder_id != self.node_id or lease.epoch != write_epoch or lease.is_expired(t):
                self.current_role = "STANDBY"
                raise FencingViolation(
                    f"Write rejected: lease is no longer held by node {self.node_id} (active holder: {lease.holder_id if lease else 'none'}, epoch: {lease.epoch if lease else 'none'})"
                )

    def start(self):
        with self._lock:
            if self.running:
                return
            self.running = True

        def _loop():
            while True:
                with self._lock:
                    if not self.running:
                        break
                self.tick()
                time.sleep(self.heartbeat_interval)

        self._thread = threading.Thread(target=_loop, daemon=True)
        self._thread.start()

    def stop(self):
        with self._lock:
            self.running = False
            if self.current_role == "LEADER":
                self.store.release(self.node_id, self.epoch)
                self.current_role = "STANDBY"
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)


@dataclass
class DatabaseNode:
    node_id: str
    engine: str
    version: str
    cores: int
    ram_gb: float
    disk_gb: float
    table_count: int
    total_bytes: int


@dataclass
class HistoricalWorkload:
    node_id: str
    peak_qps: float
    avg_qps: float
    p95_cpu_pct: float
    p95_iops: float
    p95_read_iops: float
    p95_write_iops: float
    monthly_growth_pct: float = 5.0


@dataclass
class TargetSizingRecommendation:
    node_id: str
    target_cores: int
    target_ram_gb: float
    target_disk_gb: float
    target_iops: int
    recommended_concurrency: int
    sizing_tier: str
    projected_growth_bytes: int
    buffer_pool_gb: float


class EstateSizingEngine:
    TIERS = [
        ("small", 2, 8, 1000),
        ("medium", 4, 16, 3000),
        ("large", 8, 32, 6000),
        ("xlarge", 16, 64, 12000),
        ("2xlarge", 32, 128, 20000),
        ("4xlarge", 64, 256, 40000),
    ]

    def __init__(self):
        self.nodes: Dict[str, DatabaseNode] = {}
        self.workloads: Dict[str, HistoricalWorkload] = {}

    def discover_node(self, node: DatabaseNode, workload: Optional[HistoricalWorkload] = None):
        self.nodes[node.node_id] = node
        if workload:
            self.workloads[node.node_id] = workload

    def discover_fleet(self, nodes: List[DatabaseNode], workloads: List[HistoricalWorkload]):
        for n in nodes:
            self.nodes[n.node_id] = n
        for w in workloads:
            self.workloads[w.node_id] = w

    def size_node(
        self,
        node_id: str,
        horizon_months: int = 12,
        headroom_factor: float = 1.3,
    ) -> TargetSizingRecommendation:
        if node_id not in self.nodes:
            raise KeyError(f"Node {node_id} not discovered in estate")
        node = self.nodes[node_id]
        workload = self.workloads.get(node_id) or HistoricalWorkload(
            node_id=node_id,
            peak_qps=100.0,
            avg_qps=50.0,
            p95_cpu_pct=50.0,
            p95_iops=500.0,
            p95_read_iops=400.0,
            p95_write_iops=100.0,
        )

        growth_mult = (1.0 + (workload.monthly_growth_pct / 100.0)) ** horizon_months
        projected_bytes = int(node.total_bytes * growth_mult)
        projected_disk_gb = math.ceil((projected_bytes / (1024**3)) * headroom_factor)
        target_disk_gb = max(math.ceil(node.disk_gb * headroom_factor), projected_disk_gb, 50)

        needed_cores = math.ceil(node.cores * (max(workload.p95_cpu_pct, 40.0) / 100.0) * headroom_factor)
        target_cores = max(2, 2 ** math.ceil(math.log2(max(1, needed_cores))))

        buffer_pool_gb = round(max(4.0, (projected_bytes / (1024**3)) * 0.25), 1)
        needed_ram = max(node.ram_gb * headroom_factor, buffer_pool_gb * 1.5)
        target_ram_gb = float(max(8, 2 ** math.ceil(math.log2(max(8, int(needed_ram))))))

        target_iops = int(max(1000.0, workload.p95_iops * headroom_factor))

        recommended_concurrency = max(1, min(target_cores * 2, max(1, node.cores)))

        tier = "small"
        for t_name, c, r, iops in self.TIERS:
            if target_cores <= c and target_ram_gb <= r:
                tier = t_name
                break
        else:
            tier = "4xlarge"

        return TargetSizingRecommendation(
            node_id=node_id,
            target_cores=target_cores,
            target_ram_gb=target_ram_gb,
            target_disk_gb=float(target_disk_gb),
            target_iops=target_iops,
            recommended_concurrency=recommended_concurrency,
            sizing_tier=tier,
            projected_growth_bytes=projected_bytes,
            buffer_pool_gb=buffer_pool_gb,
        )

    def fleet_summary(self) -> Dict[str, Any]:
        recommendations = [self.size_node(nid) for nid in self.nodes]
        total_cores = sum(r.target_cores for r in recommendations)
        total_ram = sum(r.target_ram_gb for r in recommendations)
        total_disk = sum(r.target_disk_gb for r in recommendations)
        tier_counts = collections.Counter(r.sizing_tier for r in recommendations)
        return {
            "node_count": len(self.nodes),
            "total_target_cores": total_cores,
            "total_target_ram_gb": total_ram,
            "total_target_disk_gb": total_disk,
            "tier_distribution": dict(tier_counts),
            "nodes": [r.__dict__ for r in recommendations],
        }


class PipelinePhase(str, enum.Enum):
    COPY_DATA = "COPY_DATA"
    BUILD_INDEXES = "BUILD_INDEXES"
    VERIFY_RANGE = "VERIFY_RANGE"


@dataclass
class TableWorkItem:
    table_name: str
    ranges: List[str] = field(default_factory=list)
    copy_duration: float = 1.0
    index_duration: float = 1.0
    verify_duration: float = 1.0
    copy_done: bool = False
    index_done: bool = False
    ranges_loaded: Set[str] = field(default_factory=set)
    ranges_verified: Set[str] = field(default_factory=set)


class PipelinedPhaseCoordinator:
    def __init__(
        self,
        max_concurrent_copies: int = 2,
        max_concurrent_indexes: int = 2,
        max_concurrent_verifications: int = 2,
    ):
        self.max_concurrent_copies = max_concurrent_copies
        self.max_concurrent_indexes = max_concurrent_indexes
        self.max_concurrent_verifications = max_concurrent_verifications
        self.tables: collections.OrderedDict[str, TableWorkItem] = collections.OrderedDict()
        self.active_tasks: Set[Tuple[str, str, str]] = set()
        self.completed_tasks: Set[Tuple[str, str, str]] = set()
        self.tail_started = False
        self._lock = threading.Lock()

    def add_table(
        self,
        table_name: str,
        ranges: Optional[List[str]] = None,
        copy_duration: float = 1.0,
        index_duration: float = 1.0,
        verify_duration: float = 1.0,
    ):
        with self._lock:
            r = ranges if ranges is not None else [f"{table_name}_r0"]
            self.tables[table_name] = TableWorkItem(
                table_name=table_name,
                ranges=r,
                copy_duration=copy_duration,
                index_duration=index_duration,
                verify_duration=verify_duration,
            )

    def schedule_next_actions(self) -> List[Tuple[str, str, str]]:
        with self._lock:
            scheduled = []
            active_copies = sum(1 for t, phase, _ in self.active_tasks if phase == PipelinePhase.COPY_DATA.value)
            active_indexes = sum(1 for t, phase, _ in self.active_tasks if phase == PipelinePhase.BUILD_INDEXES.value)
            active_verifies = sum(1 for t, phase, _ in self.active_tasks if phase == PipelinePhase.VERIFY_RANGE.value)

            for tname, item in self.tables.items():
                if not item.copy_done:
                    task_key = (tname, PipelinePhase.COPY_DATA.value, "all")
                    if task_key not in self.active_tasks and task_key not in self.completed_tasks:
                        if active_copies < self.max_concurrent_copies:
                            scheduled.append(task_key)
                            self.active_tasks.add(task_key)
                            active_copies += 1

                if item.copy_done and not item.index_done:
                    task_key = (tname, PipelinePhase.BUILD_INDEXES.value, "all")
                    if task_key not in self.active_tasks and task_key not in self.completed_tasks:
                        if active_indexes < self.max_concurrent_indexes:
                            scheduled.append(task_key)
                            self.active_tasks.add(task_key)
                            active_indexes += 1

                for r in item.ranges:
                    if r in item.ranges_loaded and r not in item.ranges_verified:
                        task_key = (tname, PipelinePhase.VERIFY_RANGE.value, r)
                        if task_key not in self.active_tasks and task_key not in self.completed_tasks:
                            if active_verifies < self.max_concurrent_verifications:
                                scheduled.append(task_key)
                                self.active_tasks.add(task_key)
                                active_verifies += 1

            return scheduled

    def complete_action(self, table_name: str, phase: str, target: str):
        with self._lock:
            task_key = (table_name, phase, target)
            self.active_tasks.discard(task_key)
            self.completed_tasks.add(task_key)
            item = self.tables.get(table_name)
            if not item:
                return

            if phase == PipelinePhase.COPY_DATA.value:
                item.copy_done = True
                for r in item.ranges:
                    item.ranges_loaded.add(r)
                if not self.tail_started:
                    self.tail_started = True
            elif phase == PipelinePhase.BUILD_INDEXES.value:
                item.index_done = True
            elif phase == PipelinePhase.VERIFY_RANGE.value:
                item.ranges_verified.add(target)

    def is_finished(self) -> bool:
        with self._lock:
            for item in self.tables.values():
                if not item.copy_done or not item.index_done:
                    return False
                if len(item.ranges_verified) < len(item.ranges):
                    return False
            return len(self.active_tasks) == 0

    def simulate(self, time_step: float = 0.1) -> Dict[str, float]:
        current_time = 0.0
        in_flight: Dict[Tuple[str, str, str], float] = {}
        durations: Dict[str, float] = {}

        for tname, item in self.tables.items():
            durations[(tname, PipelinePhase.COPY_DATA.value, "all")] = item.copy_duration
            durations[(tname, PipelinePhase.BUILD_INDEXES.value, "all")] = item.index_duration
            for r in item.ranges:
                durations[(tname, PipelinePhase.VERIFY_RANGE.value, r)] = item.verify_duration

        seq_time = sum(durations.values())

        while not self.is_finished() or in_flight:
            new_actions = self.schedule_next_actions()
            for action in new_actions:
                in_flight[action] = current_time + durations.get(action, 1.0)

            if not in_flight:
                break

            next_finish = min(in_flight.values())
            current_time = max(current_time, next_finish)

            finished = [act for act, end_t in in_flight.items() if end_t <= current_time]
            for act in finished:
                del in_flight[act]
                self.complete_action(act[0], act[1], act[2])

        pipelined_time = current_time
        speedup = (seq_time / pipelined_time) if pipelined_time > 0 else 1.0
        return {
            "sequential_duration": round(seq_time, 2),
            "pipelined_duration": round(pipelined_time, 2),
            "speedup": round(speedup, 2),
            "tail_started": self.tail_started,
        }


@dataclass
class CompressionChoice:
    algorithm: str
    level: int
    reason: str


class AdaptiveCompressionSelector:
    def __init__(
        self,
        default_algo: str = "zstd",
        fast_threshold_mb_s: float = 150.0,
        balanced_threshold_mb_s: float = 40.0,
        high_cpu_threshold: float = 85.0,
    ):
        self.default_algo = default_algo
        self.fast_threshold_mb_s = fast_threshold_mb_s
        self.balanced_threshold_mb_s = balanced_threshold_mb_s
        self.high_cpu_threshold = high_cpu_threshold
        self.measurements: List[Tuple[float, float, float]] = []

    def record_measurement(self, bytes_transferred: int, duration_sec: float, cpu_pct: float = 0.0):
        if duration_sec > 0:
            rate_mb_s = (bytes_transferred / (1024 * 1024)) / duration_sec
            self.measurements.append((rate_mb_s, cpu_pct, time.time()))

    def select(
        self,
        link_speed_mb_s: Optional[float] = None,
        host_cpu_pct: Optional[float] = None,
    ) -> CompressionChoice:
        if link_speed_mb_s is None:
            if self.measurements:
                recent = self.measurements[-5:]
                link_speed_mb_s = sum(m[0] for m in recent) / len(recent)
            else:
                link_speed_mb_s = 50.0

        cpu = host_cpu_pct
        if cpu is None:
            if self.measurements:
                recent = self.measurements[-5:]
                cpu = sum(m[1] for m in recent) / len(recent)
            else:
                cpu = 30.0

        if cpu >= self.high_cpu_threshold:
            if lz4_frame is not None:
                return CompressionChoice(algorithm="lz4", level=0, reason="High CPU load, prioritizing speed")
            return CompressionChoice(algorithm="gzip", level=1, reason="High CPU load, fast gzip fallback")

        if link_speed_mb_s >= self.fast_threshold_mb_s:
            if lz4_frame is not None:
                return CompressionChoice(algorithm="lz4", level=0, reason=f"High bandwidth ({link_speed_mb_s:.1f} MB/s)")
            if zstd is not None:
                return CompressionChoice(algorithm="zstd", level=1, reason=f"High bandwidth, fast zstd")
            return CompressionChoice(algorithm="gzip", level=1, reason="Fast gzip fallback")

        if link_speed_mb_s >= self.balanced_threshold_mb_s:
            if zstd is not None:
                return CompressionChoice(algorithm="zstd", level=3, reason=f"Medium bandwidth ({link_speed_mb_s:.1f} MB/s)")
            if lz4_frame is not None:
                return CompressionChoice(algorithm="lz4", level=0, reason="Medium bandwidth lz4 fallback")
            return CompressionChoice(algorithm="gzip", level=4, reason="Balanced gzip fallback")

        if zstd is not None:
            return CompressionChoice(algorithm="zstd", level=7, reason=f"Constrained link ({link_speed_mb_s:.1f} MB/s)")
        return CompressionChoice(algorithm="gzip", level=9, reason=f"Constrained link ({link_speed_mb_s:.1f} MB/s) max gzip")

    def compress(self, data: bytes, choice: Optional[CompressionChoice] = None) -> bytes:
        if choice is None:
            choice = self.select()

        algo = choice.algorithm
        if algo == "lz4" and lz4_frame is not None:
            return lz4_frame.compress(data)
        elif algo == "zstd" and zstd is not None:
            cctx = zstd.ZstdCompressor(level=choice.level)
            return cctx.compress(data)
        elif algo == "gzip":
            return gzip.compress(data, compresslevel=max(1, min(9, choice.level)))
        elif zstd is not None:
            return zstd.ZstdCompressor().compress(data)
        return gzip.compress(data)

    def decompress(self, compressed_data: bytes, algorithm: Optional[str] = None) -> bytes:
        if algorithm == "lz4" and lz4_frame is not None:
            return lz4_frame.decompress(compressed_data)
        if algorithm == "zstd" and zstd is not None:
            dctx = zstd.ZstdDecompressor()
            return dctx.decompress(compressed_data)
        if algorithm == "gzip":
            return gzip.decompress(compressed_data)

        if zstd is not None:
            try:
                return zstd.ZstdDecompressor().decompress(compressed_data)
            except Exception:
                pass
        if lz4_frame is not None:
            try:
                return lz4_frame.decompress(compressed_data)
            except Exception:
                pass
        return gzip.decompress(compressed_data)


@dataclass
class PartRange:
    part_id: int
    start_byte: int
    end_byte: int
    size: int


@dataclass
class PartChecksum:
    part_id: int
    crc32c: int
    crc32c_hex: str
    md5_hex: str
    bytes_read: int


@dataclass
class MultipartTransferResult:
    total_bytes: int
    part_count: int
    part_checksums: List[PartChecksum]
    composite_crc32c: int
    composite_md5: str
    verified: bool
    elapsed_seconds: float


class ParallelMultipartTransferCoordinator:
    def __init__(self, part_size: int = 4 * 1024 * 1024, max_workers: int = 4):
        self.part_size = max(1024, part_size)
        self.max_workers = max(1, max_workers)

    def plan_parts(self, total_bytes: int) -> List[PartRange]:
        if total_bytes <= 0:
            return [PartRange(part_id=0, start_byte=0, end_byte=0, size=0)]
        parts = []
        offset = 0
        part_id = 0
        while offset < total_bytes:
            end = min(total_bytes, offset + self.part_size)
            parts.append(PartRange(part_id=part_id, start_byte=offset, end_byte=end, size=end - offset))
            offset = end
            part_id += 1
        return parts

    @staticmethod
    def compute_part_checksum(data: bytes, part_id: int) -> PartChecksum:
        crc = compute_crc32c(data)
        md5_digest = hashlib.md5(data, usedforsecurity=False).hexdigest()
        return PartChecksum(
            part_id=part_id,
            crc32c=crc,
            crc32c_hex=f"{crc:08x}",
            md5_hex=md5_digest,
            bytes_read=len(data),
        )

    def transfer_bytes(
        self,
        source: Union[bytes, bytearray],
        verify: bool = True,
    ) -> Tuple[MultipartTransferResult, bytes]:
        t0 = time.time()
        total_len = len(source)
        parts = self.plan_parts(total_len)
        results: Dict[int, Tuple[PartChecksum, bytes]] = {}

        def _worker(p: PartRange) -> Tuple[int, PartChecksum, bytes]:
            chunk = bytes(source[p.start_byte : p.end_byte])
            chk = self.compute_part_checksum(chunk, p.part_id)
            return p.part_id, chk, chunk

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futs = [executor.submit(_worker, p) for p in parts]
            for f in concurrent.futures.as_completed(futs):
                pid, chk, chunk = f.result()
                results[pid] = (chk, chunk)

        ordered_parts = [results[i][0] for i in range(len(parts))]
        assembled = b"".join(results[i][1] for i in range(len(parts)))

        full_crc = compute_crc32c(assembled)
        full_md5 = hashlib.md5(assembled, usedforsecurity=False).hexdigest()

        expected_crc = compute_crc32c(source)
        expected_md5 = hashlib.md5(source, usedforsecurity=False).hexdigest()

        verified = (full_crc == expected_crc) and (full_md5 == expected_md5) if verify else True
        elapsed = time.time() - t0

        return (
            MultipartTransferResult(
                total_bytes=len(assembled),
                part_count=len(parts),
                part_checksums=ordered_parts,
                composite_crc32c=full_crc,
                composite_md5=full_md5,
                verified=verified,
                elapsed_seconds=round(elapsed, 4),
            ),
            assembled,
        )

    def transfer_file_streams(
        self,
        source_stream: io.BufferedReader,
        target_stream: io.BufferedWriter,
        total_bytes: int,
        verify: bool = True,
    ) -> MultipartTransferResult:
        t0 = time.time()
        parts = self.plan_parts(total_bytes)
        part_checksums: List[PartChecksum] = []
        lock = threading.Lock()

        def _read_and_write(p: PartRange) -> PartChecksum:
            with lock:
                source_stream.seek(p.start_byte)
                chunk = source_stream.read(p.size)
            chk = self.compute_part_checksum(chunk, p.part_id)
            with lock:
                target_stream.seek(p.start_byte)
                target_stream.write(chunk)
            return chk

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futs = [executor.submit(_read_and_write, p) for p in parts]
            for f in concurrent.futures.as_completed(futs):
                part_checksums.append(f.result())

        part_checksums.sort(key=lambda x: x.part_id)
        target_stream.flush()

        verified = True
        if verify:
            target_stream.seek(0)
            full_data = target_stream.read(total_bytes)
            composite_crc = compute_crc32c(full_data)
            composite_md5 = hashlib.md5(full_data, usedforsecurity=False).hexdigest()
        else:
            composite_crc = 0
            composite_md5 = ""

        elapsed = time.time() - t0
        return MultipartTransferResult(
            total_bytes=total_bytes,
            part_count=len(parts),
            part_checksums=part_checksums,
            composite_crc32c=composite_crc,
            composite_md5=composite_md5,
            verified=verified,
            elapsed_seconds=round(elapsed, 4),
        )
