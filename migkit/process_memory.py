from contextlib import contextmanager
from dataclasses import dataclass
import enum
import re
import signal
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from . import decide


def process_memory_budget_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for process and worker memory budget allocation (Row 661: NEED13)."""
    return (
        decide.Rung(
            "allocate_full_worker_pool",
            "allocate full worker pool under standard memory budget and headroom envelope",
            needs=(
                decide.Need(
                    "within total memory ceiling",
                    lambda f: f.get("projected_bytes", 0) <= f.get("total_budget_bytes", 1073741824),
                    "projected allocation exceeds total memory budget ceiling",
                ),
                decide.Need(
                    "headroom adequate",
                    lambda f: f.get("headroom_ratio", 0.0) >= f.get("min_headroom_ratio", 0.15),
                    "available memory headroom ratio inadequate",
                ),
                decide.Need(
                    "no active oom pressure",
                    lambda f: not bool(f.get("oom_pressure", False)),
                    "system reports active out-of-memory pressure",
                ),
            ),
            footprint=0,
            leaves="allocate full worker pool under standard memory envelope",
        ),
        decide.Rung(
            "throttle_batch_and_concurrency",
            "throttle worker concurrency and batch sizing to fit within available memory",
            needs=(
                decide.Need(
                    "concurrency can scale down",
                    lambda f: f.get("min_viable_workers", 1) <= f.get("available_worker_slots", 1),
                    "minimum viable worker concurrency cannot be scheduled",
                ),
                decide.Need(
                    "batch can scale down",
                    lambda f: f.get("min_viable_batch", 1) <= f.get("effective_batch_size", 100),
                    "effective batch size cannot be throttled further",
                ),
                decide.Need(
                    "within degraded budget",
                    lambda f: f.get("throttled_bytes", 0) <= f.get("total_budget_bytes", 1073741824),
                    "throttled allocation still exceeds memory budget",
                ),
            ),
            footprint=1,
            leaves="throttle worker concurrency and batch sizing to fit memory",
        ),
        decide.Rung(
            "spill_queues_to_auxiliary_storage",
            "spill driver prefetch and worker queues to auxiliary memory-mapped storage",
            needs=(
                decide.Need(
                    "queue spill supported",
                    lambda f: bool(f.get("queue_spill_supported", True)),
                    "queue spooling to auxiliary storage not supported",
                ),
                decide.Need(
                    "auxiliary storage available",
                    lambda f: bool(f.get("auxiliary_storage_available", True)),
                    "no auxiliary storage available for queue spooling",
                ),
            ),
            footprint=2,
            leaves="spill worker queues to auxiliary disk storage",
        ),
        decide.Rung(
            "quarantine_exhausted_process",
            "quarantine failed worker process and initiate state reconciliation",
            needs=(
                decide.Need(
                    "reconciliation supported",
                    lambda f: bool(f.get("reconciliation_supported", True)),
                    "state reconciliation not supported for failed worker",
                ),
                decide.Need(
                    "isolated process teardown",
                    lambda f: bool(f.get("isolated_process_teardown", True)),
                    "cannot safely tear down isolated worker process",
                ),
            ),
            footprint=3,
            leaves="quarantine failed worker process and initiate state reconciliation",
        ),
        decide.Rung(
            "refuse_memory_admission",
            "refuse worker admission to prevent fatal host out-of-memory",
            footprint=4,
            leaves="refuse worker admission to prevent fatal host out-of-memory",
        ),
    )


def decide_process_memory_budget(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the process_memory_budget_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("process memory budget allocation", process_memory_budget_rungs(), fact_obj)


class ProcessMemoryError(RuntimeError):
    pass


class MemoryBudgetExceededError(ProcessMemoryError):
    pass


class ReconciliationRequiredError(ProcessMemoryError):
    pass


class UnverifiedResumeRefusedError(ProcessMemoryError):
    pass


class MemoryCategory(enum.Enum):
    CHILD_HEAP = "child_heap"
    DECODER_BUFFER = "decoder_buffer"
    DRIVER_PREFETCH = "driver_prefetch"
    COMPRESSION = "compression"
    QUEUE = "queue"
    CONCURRENT_PROOFS = "concurrent_proofs"


class FailureClassification(enum.Enum):
    NONE = "none"
    OS_KILL_OOM = "os_kill_oom"
    DRIVER_MEMORY_ERROR = "driver_memory_error"
    THROUGHPUT_STALL = "throughput_stall"
    OTHER_ERROR = "other_error"


_CATEGORY_ALIASES = {
    "child_process_heaps": MemoryCategory.CHILD_HEAP,
    "child_heap": MemoryCategory.CHILD_HEAP,
    "decoder_buffers": MemoryCategory.DECODER_BUFFER,
    "decoder_buffer": MemoryCategory.DECODER_BUFFER,
    "driver_prefetch": MemoryCategory.DRIVER_PREFETCH,
    "prefetch": MemoryCategory.DRIVER_PREFETCH,
    "compression_buffers": MemoryCategory.COMPRESSION,
    "compression": MemoryCategory.COMPRESSION,
    "queues": MemoryCategory.QUEUE,
    "queue": MemoryCategory.QUEUE,
    "concurrent_proofs": MemoryCategory.CONCURRENT_PROOFS,
    "proof_buffers": MemoryCategory.CONCURRENT_PROOFS,
    "proofs": MemoryCategory.CONCURRENT_PROOFS,
}


def _normalize_category(cat: Union[MemoryCategory, str]) -> MemoryCategory:
    if isinstance(cat, MemoryCategory):
        return cat
    key = str(cat).strip().lower().replace("-", "_")
    if key in _CATEGORY_ALIASES:
        return _CATEGORY_ALIASES[key]
    for member in MemoryCategory:
        if member.value == key or member.name.lower() == key:
            return member
    raise ValueError(f"unknown memory category: {cat}")


@dataclass(frozen=True)
class ProcessDiagnostic:
    classification: FailureClassification
    detail: str
    exit_code: Optional[int] = None
    action_badge: str = "PASS"


ProcessFailureInfo = ProcessDiagnostic


_DRIVER_MEM_PATTERNS = re.compile(
    r"(out of memory|memoryerror|java\.lang\.outofmemoryerror|heap space|"
    r"cannot allocate memory|max memory reached|hy000: out of memory|"
    r"driver out of memory|client out of memory|server out of memory)",
    re.IGNORECASE,
)

_OS_OOM_PATTERNS = re.compile(
    r"(oom-killer|out of memory: kill process|killed process|invoked oom-killer)",
    re.IGNORECASE,
)


def classify_process_failure(
    returncode: Optional[int] = None,
    stderr: str = "",
    stdout: str = "",
    stall_duration_seconds: float = 0.0,
    stall_threshold_seconds: float = 30.0,
    throughput_bytes_per_sec: float = 0.0,
    is_active: bool = False,
    metrics: Optional[Dict[str, Any]] = None,
) -> ProcessDiagnostic:
    combined = f"{stderr}\n{stdout}".strip()

    is_sigkill = returncode in (-9, 137, -int(signal.SIGKILL))
    if is_sigkill or _OS_OOM_PATTERNS.search(combined):
        sig_str = "SIGKILL" if is_sigkill else "OOM"
        return ProcessDiagnostic(
            classification=FailureClassification.OS_KILL_OOM,
            detail=f"process terminated by {sig_str} / OOM killer ({returncode}): {combined[:200]}",
            exit_code=returncode,
            action_badge="ACTION",
        )

    if _DRIVER_MEM_PATTERNS.search(combined):
        return ProcessDiagnostic(
            classification=FailureClassification.DRIVER_MEMORY_ERROR,
            detail=f"driver memory exhaustion ({returncode}): {combined[:200]}",
            exit_code=returncode,
            action_badge="ACTION",
        )

    if (is_active or returncode is None) and stall_duration_seconds >= stall_threshold_seconds:
        if throughput_bytes_per_sec <= 0.0:
            return ProcessDiagnostic(
                classification=FailureClassification.THROUGHPUT_STALL,
                detail=f"throughput stall: 0 bytes/sec over {stall_duration_seconds:.1f}s (threshold {stall_threshold_seconds:.1f}s)",
                exit_code=returncode,
                action_badge="REVIEW",
            )

    if returncode is not None and returncode != 0:
        return ProcessDiagnostic(
            classification=FailureClassification.OTHER_ERROR,
            detail=f"process failed with exit code {returncode}: {combined[:200]}",
            exit_code=returncode,
            action_badge="REVIEW",
        )

    return ProcessDiagnostic(
        classification=FailureClassification.NONE,
        detail="execution successful",
        exit_code=returncode,
        action_badge="PASS",
    )


def estimate_worker_memory(
    row_size_bytes: int,
    batch_size: int = 1000,
    prefetch_rows: Optional[int] = None,
    queue_capacity: Optional[int] = None,
    base_heap_bytes: Optional[int] = None,
    max_prefetch_cap: Optional[int] = None,
    max_queue_cap: Optional[int] = None,
) -> Dict[MemoryCategory, int]:
    base_heap = base_heap_bytes if base_heap_bytes is not None else (8 * 1024 * 1024)
    row_b = max(1, int(row_size_bytes))
    pref_cap = max_prefetch_cap or 500
    q_cap = max_queue_cap or 100
    eff_prefetch = prefetch_rows if prefetch_rows is not None else min(pref_cap, max(1, (4 * 1024 * 1024) // row_b))
    eff_queue = queue_capacity if queue_capacity is not None else min(q_cap, max(1, (4 * 1024 * 1024) // row_b))

    return {
        MemoryCategory.CHILD_HEAP: max(base_heap, int(batch_size * row_b * 1.2)),
        MemoryCategory.DECODER_BUFFER: max(64 * 1024, int(row_b * 2)),
        MemoryCategory.DRIVER_PREFETCH: max(128 * 1024, int(eff_prefetch * row_b)),
        MemoryCategory.COMPRESSION: max(64 * 1024, int(min(batch_size * row_b, 8 * 1024 * 1024) * 0.5)),
        MemoryCategory.QUEUE: max(128 * 1024, int(eff_queue * row_b)),
    }


@dataclass
class PhaseAllocation:
    phase_name: str
    admitted: bool
    workers: int
    allocated_bytes: int
    breakdown: Dict[str, int]
    reason: str


class ProcessMemoryGovernor:
    def __init__(
        self,
        total_budget_bytes: int,
        reserve_bytes: int = 0,
    ):
        self.total_budget_bytes = int(total_budget_bytes)
        self.reserve_bytes = int(reserve_bytes)
        self._usable_budget = max(0, self.total_budget_bytes - self.reserve_bytes)
        self._lock = threading.RLock()
        self._allocated: Dict[MemoryCategory, int] = {c: 0 for c in MemoryCategory}
        self._allocations_by_owner: Dict[str, Dict[MemoryCategory, int]] = {}

    @property
    def usable_budget_bytes(self) -> int:
        return self._usable_budget

    def allocated_bytes(self, category: Optional[Union[MemoryCategory, str]] = None) -> int:
        with self._lock:
            if category is not None:
                cat = _normalize_category(category)
                return self._allocated.get(cat, 0)
            return sum(self._allocated.values())

    def available_bytes(self) -> int:
        with self._lock:
            return max(0, self._usable_budget - sum(self._allocated.values()))

    def breakdown(self) -> Dict[str, int]:
        with self._lock:
            return {c.value: self._allocated.get(c, 0) for c in MemoryCategory}

    def allocate(
        self,
        category: Union[MemoryCategory, str],
        amount_bytes: int,
        owner_id: Optional[str] = None,
    ) -> int:
        cat = _normalize_category(category)
        amt = max(0, int(amount_bytes))
        with self._lock:
            current = sum(self._allocated.values())
            if current + amt > self._usable_budget:
                raise MemoryBudgetExceededError(
                    f"budget exceeded: requested {amt} for {cat.value}, current {current}, budget {self._usable_budget}"
                )
            self._allocated[cat] = self._allocated.get(cat, 0) + amt
            if owner_id:
                owner_map = self._allocations_by_owner.setdefault(owner_id, {})
                owner_map[cat] = owner_map.get(cat, 0) + amt
            return amt

    def try_allocate(
        self,
        category: Union[MemoryCategory, str],
        amount_bytes: int,
        owner_id: Optional[str] = None,
    ) -> bool:
        cat = _normalize_category(category)
        amt = max(0, int(amount_bytes))
        with self._lock:
            current = sum(self._allocated.values())
            if current + amt > self._usable_budget:
                return False
            self._allocated[cat] = self._allocated.get(cat, 0) + amt
            if owner_id:
                owner_map = self._allocations_by_owner.setdefault(owner_id, {})
                owner_map[cat] = owner_map.get(cat, 0) + amt
            return True

    def release(
        self,
        category: Union[MemoryCategory, str],
        amount_bytes: int,
        owner_id: Optional[str] = None,
    ) -> int:
        cat = _normalize_category(category)
        amt = max(0, int(amount_bytes))
        with self._lock:
            curr = self._allocated.get(cat, 0)
            dec = min(curr, amt)
            self._allocated[cat] = curr - dec
            if owner_id and owner_id in self._allocations_by_owner:
                owner_map = self._allocations_by_owner[owner_id]
                owner_curr = owner_map.get(cat, 0)
                owner_map[cat] = max(0, owner_curr - dec)
            return dec

    def release_owner(self, owner_id: str) -> int:
        with self._lock:
            owner_map = self._allocations_by_owner.pop(owner_id, {})
            total_released = 0
            for cat, amt in owner_map.items():
                curr = self._allocated.get(cat, 0)
                dec = min(curr, amt)
                self._allocated[cat] = curr - dec
                total_released += dec
            return total_released

    @contextmanager
    def reserve(
        self,
        category: Union[MemoryCategory, str],
        amount_bytes: int,
        owner_id: Optional[str] = None,
    ):
        self.allocate(category, amount_bytes, owner_id)
        try:
            yield
        finally:
            self.release(category, amount_bytes, owner_id)

    def calculate_safe_workers(
        self,
        row_size_bytes: int,
        batch_size: int = 1000,
        prefetch_rows: Optional[int] = None,
        queue_capacity: Optional[int] = None,
        concurrent_proofs: int = 0,
        proof_memory_bytes: int = 32 * 1024 * 1024,
        max_concurrency: int = 16,
    ) -> int:
        with self._lock:
            avail = self.available_bytes()
            proof_cost = max(0, int(concurrent_proofs)) * max(0, int(proof_memory_bytes))
            if proof_cost > avail:
                return 0
            worker_req = estimate_worker_memory(
                row_size_bytes=row_size_bytes,
                batch_size=batch_size,
                prefetch_rows=prefetch_rows,
                queue_capacity=queue_capacity,
            )
            per_worker = sum(worker_req.values())
            if per_worker <= 0:
                return max_concurrency
            rem = avail - proof_cost
            safe = int(rem // per_worker)
            return max(0, min(safe, max_concurrency))

    def admit_constrained_worker(
        self,
        row_size_bytes: int,
        min_batch_size: int = 1,
        headroom_factor: Optional[float] = None,
        max_batch_ceiling: Optional[int] = None,
    ) -> Tuple[int, int]:
        with self._lock:
            avail = self.available_bytes()
            if avail <= 0:
                return 0, 0
            row_b = max(1, int(row_size_bytes))
            if row_b > avail:
                return 0, 0

            ratio = headroom_factor if headroom_factor is not None else 0.4
            ceiling = max_batch_ceiling or 100
            target_batch = max(min_batch_size, min(ceiling, int((avail * ratio) // (row_b * 1.2))))
            while target_batch >= 1:
                req = estimate_worker_memory(
                    row_size_bytes=row_size_bytes,
                    batch_size=target_batch,
                    prefetch_rows=1,
                    queue_capacity=1,
                )
                if sum(req.values()) <= avail:
                    return 1, target_batch
                target_batch //= 2

            min_req = {
                MemoryCategory.CHILD_HEAP: max(1024 * 1024, int(row_b * 1.2)),
                MemoryCategory.DECODER_BUFFER: min(row_b * 2, int(avail * 0.2)),
                MemoryCategory.DRIVER_PREFETCH: min(row_b, int(avail * 0.2)),
                MemoryCategory.COMPRESSION: min(64 * 1024, int(avail * 0.1)),
                MemoryCategory.QUEUE: min(row_b, int(avail * 0.2)),
            }
            if sum(min_req.values()) <= avail:
                return 1, 1
            return 0, 0

    def admit_phase(
        self,
        phase_name: str,
        requested_workers: int,
        row_size_bytes: int,
        batch_size: int = 1000,
        proof_workers: int = 0,
        proof_memory_bytes: int = 32 * 1024 * 1024,
    ) -> PhaseAllocation:
        with self._lock:
            safe_w = self.calculate_safe_workers(
                row_size_bytes=row_size_bytes,
                batch_size=batch_size,
                concurrent_proofs=proof_workers,
                proof_memory_bytes=proof_memory_bytes,
                max_concurrency=requested_workers,
            )
            if safe_w == 0 and requested_workers > 0:
                return PhaseAllocation(
                    phase_name=phase_name,
                    admitted=False,
                    workers=0,
                    allocated_bytes=0,
                    breakdown={},
                    reason="insufficient memory budget for phase concurrency",
                )

            worker_req = estimate_worker_memory(
                row_size_bytes=row_size_bytes,
                batch_size=batch_size,
            )
            allocated = {}
            for cat, per_w in worker_req.items():
                tot = per_w * safe_w
                if tot > 0:
                    self.allocate(cat, tot, owner_id=f"phase:{phase_name}")
                    allocated[cat.value] = tot

            if proof_workers > 0:
                proof_tot = proof_workers * proof_memory_bytes
                self.allocate(
                    MemoryCategory.CONCURRENT_PROOFS,
                    proof_tot,
                    owner_id=f"phase:{phase_name}",
                )
                allocated[MemoryCategory.CONCURRENT_PROOFS.value] = proof_tot

            total_phase_bytes = sum(allocated.values())
            return PhaseAllocation(
                phase_name=phase_name,
                admitted=True,
                workers=safe_w,
                allocated_bytes=total_phase_bytes,
                breakdown=allocated,
                reason="admitted within global memory budget",
            )


@dataclass
class TargetState:
    table: str
    reported_watermark: Optional[Any] = None
    epoch: int = 0


@dataclass
class ReconciledState:
    table: str
    safe_position: Any
    unverified_rows_discarded: int
    reconciled: bool


@dataclass
class ResumptionPlan:
    mover: str
    workers: int
    batch_size: int
    reconciled_position: Any
    unverified_rows_discarded: int
    total_allocated: int
    breakdown: Dict[str, int]


def reconcile_target_state(
    target_state: TargetState,
    verified_position: Any,
    unverified_cleanup_fn: Optional[Callable[[Any, Any], int]] = None,
) -> ReconciledState:
    if verified_position is None:
        raise UnverifiedResumeRefusedError(
            f"refusing resume: table {target_state.table} has no verified checkpoint"
        )

    reported = target_state.reported_watermark
    discarded = 0
    if reported is not None and reported > verified_position:
        if unverified_cleanup_fn is not None:
            discarded = unverified_cleanup_fn(verified_position, reported)

    return ReconciledState(
        table=target_state.table,
        safe_position=verified_position,
        unverified_rows_discarded=discarded,
        reconciled=True,
    )


class TargetReconciler:
    def __init__(self, governor: ProcessMemoryGovernor):
        self.governor = governor
        self._reconciled_state: Optional[ReconciledState] = None

    @property
    def is_reconciled(self) -> bool:
        return self._reconciled_state is not None and self._reconciled_state.reconciled

    def reconcile(
        self,
        target_state: TargetState,
        verified_position: Any,
        unverified_cleanup_fn: Optional[Callable[[Any, Any], int]] = None,
    ) -> ReconciledState:
        res = reconcile_target_state(
            target_state=target_state,
            verified_position=verified_position,
            unverified_cleanup_fn=unverified_cleanup_fn,
        )
        self._reconciled_state = res
        return res

    def adapt_execution(
        self,
        new_mover: str,
        new_workers: int,
        row_size_bytes: int,
        batch_size: int = 1000,
        proof_workers: int = 0,
        proof_memory_bytes: int = 32 * 1024 * 1024,
    ) -> ResumptionPlan:
        if not self.is_reconciled or self._reconciled_state is None:
            raise ReconciliationRequiredError(
                "target state must be reconciled against verified position before adapting execution"
            )

        safe_w = self.governor.calculate_safe_workers(
            row_size_bytes=row_size_bytes,
            batch_size=batch_size,
            concurrent_proofs=proof_workers,
            proof_memory_bytes=proof_memory_bytes,
            max_concurrency=new_workers,
        )
        is_constrained = False
        if safe_w == 0 and new_workers > 0:
            safe_w, batch_size = self.governor.admit_constrained_worker(row_size_bytes)
            is_constrained = True

        if is_constrained:
            worker_req = estimate_worker_memory(
                row_size_bytes=row_size_bytes,
                batch_size=batch_size,
                prefetch_rows=1,
                queue_capacity=1,
            )
        else:
            worker_req = estimate_worker_memory(
                row_size_bytes=row_size_bytes,
                batch_size=batch_size,
            )

        allocated = {}
        for cat, per_w in worker_req.items():
            tot = per_w * safe_w
            if tot > 0:
                self.governor.allocate(cat, tot, owner_id=f"adapted:{new_mover}")
                allocated[cat.value] = tot

        if proof_workers > 0:
            proof_tot = proof_workers * proof_memory_bytes
            self.governor.allocate(
                MemoryCategory.CONCURRENT_PROOFS,
                proof_tot,
                owner_id=f"adapted:{new_mover}",
            )
            allocated[MemoryCategory.CONCURRENT_PROOFS.value] = proof_tot

        plan = ResumptionPlan(
            mover=new_mover,
            workers=safe_w,
            batch_size=batch_size,
            reconciled_position=self._reconciled_state.safe_position,
            unverified_rows_discarded=self._reconciled_state.unverified_rows_discarded,
            total_allocated=sum(allocated.values()),
            breakdown=allocated,
        )
        self._reconciled_state = None
        return plan
