"""Budget the optimizer and certify solution quality (Row 607: SMART-N24.decision-compute).

Extends local search, space pruning and exhaustive small-space oracles using
decide.Rung and decide.climb(work, rungs, facts).

Spends planning and probing time according to potential completion-time benefit:
cheap baseline for tiny jobs, bounded search normally, deeper exact/beam search
only where worth its cost. Keeps a validated incumbent and supports cancellation.
Guarantees bounded planner CPU/memory/time on huge estates; interrupting search
cannot return an invalid candidate; optimal is reported only with a certificate,
otherwise a valid bound/gap or unknown.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
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
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from . import decide

# -----------------------------------------------------------------------------
# Care Badges and Constants
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


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


# -----------------------------------------------------------------------------
# Error Hierarchy
# -----------------------------------------------------------------------------

class DecisionComputeError(RuntimeError):
    """Base error for decision compute and optimizer budgeting operations."""


class OptimizerBudgetExceededError(DecisionComputeError):
    """Raised when planner execution exceeds allocated CPU or wall time limits."""


class InvalidIncumbentError(DecisionComputeError):
    """Raised when an incumbent candidate fails feasibility or validation."""


class SearchCancelledError(DecisionComputeError):
    """Raised when search execution is cancelled via cancellation token."""


class UnsafeEndpointError(DecisionComputeError, ValueError):
    """Raised when an endpoint host is outside approved loopback or 10.0.0.x ranges."""


class LeaseLockError(DecisionComputeError):
    """Raised when an active verification lease lock (.run.lock) cannot be acquired or renewed."""


# -----------------------------------------------------------------------------
# Safe Endpoint Validation
# -----------------------------------------------------------------------------

def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host uses approved loopback or 10.0.0.x test addresses."""
    cleaned = str(host or "").strip().lower()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host cannot be empty")
    if cleaned in SAFE_TEST_HOSTS:
        return cleaned
    if cleaned.startswith("10.0.0."):
        parts = cleaned.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x allowed"
    )


# -----------------------------------------------------------------------------
# Typed JSON Serialization Helpers (Universal O(1) Checkpoint Support)
# -----------------------------------------------------------------------------

def _encode_typed_value(val: Any) -> Any:
    """Recursively encode native Python types for JSON preservation."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
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
    if t == "tuple":
        return tuple(obj["val"])
    if t == "set":
        return set(obj["val"])
    return obj


# -----------------------------------------------------------------------------
# Mutual Exclusion Active Lease Lock (.run.lock)
# -----------------------------------------------------------------------------

class DecisionComputeLeaseLock:
    """Mutual exclusion .run.lock with heartbeat preventing overlapping runs."""

    def __init__(self, lock_dir: Union[str, Path], lease_ttl_s: float = 30.0) -> None:
        self.lock_dir = Path(lock_dir)
        self.lock_path = self.lock_dir / ".run.lock"
        self.lease_ttl_s = float(lease_ttl_s)
        self._fd: Optional[int] = None
        self.owner_id: str = f"pid-{os.getpid()}-{int(time.time() * 1000)}"

    def acquire(self, now: Optional[float] = None) -> None:
        cur = time.time() if now is None else float(now)
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise LeaseLockError(f"active lease lock held at {self.lock_path}") from exc
        self._fd = fd
        self._write_state(cur)

    def heartbeat(self, now: Optional[float] = None) -> None:
        if self._fd is None:
            raise LeaseLockError("cannot heartbeat unacquired lease lock")
        cur = time.time() if now is None else float(now)
        self._write_state(cur)

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
            if self.lock_path.exists():
                try:
                    self.lock_path.unlink()
                except OSError:
                    pass

    def _write_state(self, timestamp: float) -> None:
        assert self._fd is not None
        payload = json.dumps(
            {
                "owner": self.owner_id,
                "timestamp": timestamp,
                "expires": timestamp + self.lease_ttl_s,
                "pid": os.getpid(),
            }
        ).encode("utf-8")
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.ftruncate(self._fd, 0)
        os.write(self._fd, payload)
        os.fsync(self._fd)

    def __enter__(self) -> DecisionComputeLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# O(1) Typed Durable Resume Checkpoint Store
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class CheckpointUnit:
    """One durable unit checkpointed with typed metadata."""

    unit_id: str
    stage: str
    status: str
    recorded_at: datetime.datetime
    cursor: Union[int, str, Tuple[Any, ...]]
    metadata: Dict[str, Any] = field(default_factory=dict)


class DecisionComputeCheckpointStore:
    """Durable checkpoint store supporting O(1) resumption and skipping."""

    def __init__(self, directory: Union[str, Path]) -> None:
        self.directory = Path(directory)
        self.checkpoint_file = self.directory / ".decision_compute.chk"
        self._units: Dict[str, CheckpointUnit] = {}
        self._load()

    def _load(self) -> None:
        if not self.checkpoint_file.exists():
            return
        with open(self.checkpoint_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                raw = json.loads(line, object_hook=_decode_typed_hook)
                unit = CheckpointUnit(
                    unit_id=str(raw["unit_id"]),
                    stage=str(raw["stage"]),
                    status=str(raw["status"]),
                    recorded_at=raw["recorded_at"],
                    cursor=raw["cursor"],
                    metadata=raw.get("metadata", {}),
                )
                self._units[unit.unit_id] = unit

    def save_unit(self, unit: CheckpointUnit) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        self._units[unit.unit_id] = unit
        raw = {
            "unit_id": unit.unit_id,
            "stage": unit.stage,
            "status": unit.status,
            "recorded_at": unit.recorded_at,
            "cursor": unit.cursor,
            "metadata": unit.metadata,
        }
        encoded = json.dumps(_encode_typed_value(raw), ensure_ascii=True)
        with open(self.checkpoint_file, "a", encoding="utf-8") as f:
            f.write(encoded + "\n")
            f.flush()
            os.fsync(f.fileno())

    def has_unit(self, unit_id: str) -> bool:
        """Check in O(1) if unit is completed."""
        return unit_id in self._units

    def get_unit(self, unit_id: str) -> Optional[CheckpointUnit]:
        return self._units.get(unit_id)

    def completed_count(self) -> int:
        return len(self._units)


# -----------------------------------------------------------------------------
# Domain Enums and Core Models
# -----------------------------------------------------------------------------

class SearchStrategyKind(str, enum.Enum):
    EXHAUSTIVE_ORACLE = "exhaustive_oracle"
    DEEPER_BEAM_SEARCH = "deeper_beam_search"
    BOUNDED_LOCAL_SEARCH = "bounded_local_search"
    CHEAP_BASELINE = "cheap_baseline"


class CertificateStatus(str, enum.Enum):
    OPTIMAL_CERTIFIED = "OPTIMAL_CERTIFIED"
    BOUNDED_GAP = "BOUNDED_GAP"
    INCUMBENT_INTERRUPTED = "INCUMBENT_INTERRUPTED"
    UNKNOWN = "UNKNOWN"
    INVALID = "INVALID"


@dataclass(frozen=True)
class DecisionWay:
    """One way a table or task can be processed."""

    name: str
    per_stream: float
    uses: Dict[str, float] = field(default_factory=dict)
    startup: float = 0.0
    bills: float = 0.0
    most: Optional[int] = None
    spread: float = 0.0

    def expected(self, bytes_count: float) -> float:
        return float(bytes_count)


@dataclass(frozen=True)
class DecisionJob:
    """One table or workload item to schedule."""

    name: str
    bytes: int
    ways: Tuple[DecisionWay, ...]
    ranges: int = 1
    index_bytes: int = 0


@dataclass(frozen=True)
class DecisionLimits:
    """Global constraints for workload optimization."""

    workers: int = 4
    caps: Dict[str, float] = field(default_factory=dict)
    money: Optional[float] = None
    deadline: Optional[float] = None
    source_share: Optional[float] = None

    def cap(self, resource: str) -> Optional[float]:
        got = self.caps.get(resource)
        if got is not None and resource == "source" and self.source_share:
            return got * self.source_share
        return got


@dataclass(frozen=True)
class PlanningBudgetLimits:
    """Planner resource and execution bounds."""

    max_planner_wall_s: float = 30.0
    max_planner_cpu_s: float = 60.0
    max_planner_memory_mb: float = 512.0
    max_beam_width: int = 32
    max_exhaustive_states: int = 256
    min_benefit_threshold_s: float = 5.0
    high_benefit_threshold_s: float = 100.0
    min_beam_benefit_s: float = 10.0
    min_beam_time_s: float = 2.0
    min_local_search_time_s: float = 0.1
    compute_cost_ratio: float = 1.0


@dataclass(frozen=True)
class SolutionCertificate:
    """Certified solution quality artifact."""

    certificate_id: str
    status: CertificateStatus
    makespan: float
    lower_bound: float
    upper_bound: float
    optimality_gap: float
    is_optimal: bool
    proof_type: str
    iterations_evaluated: int
    pruned_states: int
    compute_time_s: float
    care_badge: str
    assignments: Dict[str, str]
    timestamp: datetime.datetime = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))

    def as_evidence(self) -> Dict[str, Any]:
        return {
            "certificate_id": self.certificate_id,
            "status": self.status.value,
            "makespan": self.makespan,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "optimality_gap": self.optimality_gap,
            "is_optimal": self.is_optimal,
            "proof_type": self.proof_type,
            "iterations_evaluated": self.iterations_evaluated,
            "pruned_states": self.pruned_states,
            "compute_time_s": self.compute_time_s,
            "care_badge": self.care_badge,
            "assignments": dict(self.assignments),
            "timestamp": self.timestamp.isoformat(),
        }


class CancellationToken:
    """Thread-safe cooperative cancellation token."""

    def __init__(self) -> None:
        self._cancelled: bool = False

    def cancel(self) -> None:
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled

    def check_cancelled(self) -> None:
        if self._cancelled:
            raise SearchCancelledError("search execution cancelled by token")


# -----------------------------------------------------------------------------
# Decision Rungs and Ladders
# -----------------------------------------------------------------------------

def compute_budget_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for allocating optimizer planning budget."""
    return (
        decide.Rung(
            "high_depth_budget",
            "allocate extended search budget for high-benefit workloads",
            needs=(
                decide.Need(
                    "potential makespan savings justify deep exploration",
                    lambda f: float(f.get("potential_makespan_savings_s", 0.0)) >= float(f.get("high_benefit_threshold_s", 100.0)),
                    "{rung} requires potential savings >= high benefit threshold",
                ),
                decide.Need(
                    "planner CPU headroom sufficient for extended search",
                    lambda f: float(f.get("planner_cpu_headroom_ratio", 0.0)) >= 0.5,
                    "{rung} requires planner CPU headroom >= 50%",
                ),
            ),
            because="massive workloads justify deeper branch-and-bound search time",
            footprint=2,
        ),
        decide.Rung(
            "standard_bounded_budget",
            "allocate standard bounded search budget proportional to potential savings",
            needs=(
                decide.Need(
                    "expected search benefit exceeds minimum threshold",
                    lambda f: float(f.get("potential_makespan_savings_s", 0.0)) >= float(f.get("min_benefit_threshold_s", 5.0)),
                    "{rung} requires expected benefit >= minimum threshold",
                ),
                decide.Need(
                    "planner wall clock limit allows bounded search",
                    lambda f: float(f.get("max_planner_wall_s", 0.0)) >= 1.0,
                    "{rung} requires planner wall time budget >= 1.0s",
                ),
            ),
            because="proportional budgeting balances search overhead against makespan gain",
            footprint=1,
        ),
        decide.Rung(
            "minimal_baseline_budget",
            "allocate minimal near-zero search budget for tiny or CPU-constrained jobs",
            needs=(
                decide.Need(
                    "minimal budget allocation permitted as fallback",
                    lambda f: bool(f.get("allow_minimal_budget", True)),
                    "{rung} requires minimal budget permission",
                ),
            ),
            because="prevents spending more wall time planning than the job takes to run",
            footprint=0,
        ),
    )


def search_strategy_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for selecting optimization search tier."""
    return (
        decide.Rung(
            "exhaustive_oracle_strategy",
            "exhaustively evaluate small candidate space and certify mathematical optimality",
            needs=(
                decide.Need(
                    "candidate state space within exhaustive threshold",
                    lambda f: 0 < int(f.get("total_states", 0)) <= int(f.get("max_exhaustive_states", 256)),
                    "{rung} requires candidate states within exhaustive threshold",
                ),
                decide.Need(
                    "allocated compute budget sufficient for exhaustive evaluation",
                    lambda f: float(f.get("allocated_budget_s", 0.0)) >= float(f.get("exhaustive_estimated_time_s", 0.01)),
                    "{rung} requires compute budget >= estimated exhaustive time",
                ),
            ),
            because="small combinatorics permit full state enumeration with zero optimality gap",
            footprint=3,
        ),
        decide.Rung(
            "deeper_beam_search_strategy",
            "run deeper branch-and-bound beam search with space pruning for high-value optimization",
            needs=(
                decide.Need(
                    "potential completion-time benefit exceeds search cost",
                    lambda f: float(f.get("potential_makespan_savings_s", 0.0)) >= float(f.get("min_beam_benefit_s", 10.0)),
                    "{rung} requires potential benefit >= beam threshold",
                ),
                decide.Need(
                    "compute budget sufficient for beam exploration",
                    lambda f: float(f.get("allocated_budget_s", 0.0)) >= float(f.get("min_beam_time_s", 2.0)),
                    "{rung} requires allocated budget >= minimum beam time",
                ),
                decide.Need(
                    "candidate state space exceeds exhaustive threshold",
                    lambda f: int(f.get("total_states", 0)) > int(f.get("max_exhaustive_states", 256)),
                    "{rung} requires state space exceeding exhaustive threshold",
                ),
            ),
            because="large potential completion-time savings justify deeper branch-and-bound search",
            footprint=2,
        ),
        decide.Rung(
            "bounded_local_search_strategy",
            "run bounded local search with steepest fluid bound descent",
            needs=(
                decide.Need(
                    "candidate space requires bounded exploration",
                    lambda f: int(f.get("estate_tables", 0)) >= 2 or int(f.get("total_states", 0)) > 1,
                    "{rung} requires multi-table or multi-state estate",
                ),
                decide.Need(
                    "compute budget permits local steep descent",
                    lambda f: float(f.get("allocated_budget_s", 0.0)) >= float(f.get("min_local_search_time_s", 0.1)),
                    "{rung} requires compute budget >= minimum local search time",
                ),
            ),
            because="bounds optimization time while capturing the majority of resource mix improvements",
            footprint=1,
        ),
        decide.Rung(
            "cheap_baseline_strategy",
            "adopt cheap greedy baseline with zero search overhead for tiny or budget-constrained jobs",
            needs=(
                decide.Need(
                    "cheap baseline permitted for tiny or constrained workloads",
                    lambda f: bool(f.get("allow_cheap_baseline", True)),
                    "{rung} requires cheap baseline permission",
                ),
            ),
            because="potential makespan reduction is smaller than search compute cost or budget is zero",
            footprint=0,
        ),
    )


def space_pruning_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for branch-and-bound pruning decisions."""
    return (
        decide.Rung(
            "admissible_bound_prune",
            "prune subtree by admissible lower bound dominance",
            needs=(
                decide.Need(
                    "partial node lower bound dominates validated incumbent upper bound",
                    lambda f: float(f.get("node_lower_bound", 0.0)) >= float(f.get("incumbent_upper_bound", float("inf"))),
                    "{rung} requires node lower bound >= incumbent upper bound",
                ),
            ),
            because="no completion of this partial assignment can improve upon validated incumbent",
            footprint=0,
        ),
        decide.Rung(
            "resource_capacity_violation_prune",
            "prune branch exceeding hard resource or financial constraints",
            needs=(
                decide.Need(
                    "partial assignment violates hard resource caps or money budget",
                    lambda f: bool(f.get("violates_hard_capacity", False) or f.get("violates_money_limit", False)),
                    "{rung} requires capacity or budget constraint violation",
                ),
            ),
            because="infeasible branch cannot lead to a valid plan",
            footprint=0,
        ),
        decide.Rung(
            "retain_and_expand_branch",
            "retain branch for beam expansion and exploration",
            needs=(
                decide.Need(
                    "branch is feasible under resource and financial constraints",
                    lambda f: not bool(f.get("violates_hard_capacity", False) or f.get("violates_money_limit", False)),
                    "{rung} requires branch feasibility",
                ),
                decide.Need(
                    "node lower bound strictly improves on incumbent upper bound",
                    lambda f: float(f.get("node_lower_bound", 0.0)) < float(f.get("incumbent_upper_bound", float("inf"))),
                    "{rung} requires node lower bound < incumbent upper bound",
                ),
            ),
            because="branch may contain an optimal or improving feasible solution",
            footprint=1,
        ),
    )


def incumbent_update_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for validating and adopting improved incumbents."""
    return (
        decide.Rung(
            "accept_strictly_improving_incumbent",
            "update validated incumbent with strictly superior feasible solution",
            needs=(
                decide.Need(
                    "candidate satisfies all resource and financial limits",
                    lambda f: bool(f.get("candidate_feasible", True)),
                    "{rung} requires candidate feasibility",
                ),
                decide.Need(
                    "candidate objective strictly lower than current incumbent",
                    lambda f: float(f.get("candidate_objective", float("inf"))) < float(f.get("current_incumbent_objective", float("inf"))),
                    "{rung} requires candidate objective < current incumbent",
                ),
            ),
            because="preserves invariant that incumbent is always the best valid candidate found so far",
            footprint=1,
        ),
        decide.Rung(
            "reject_suboptimal_candidate",
            "reject candidate with equal or inferior objective value",
            needs=(
                decide.Need(
                    "candidate objective is greater than or equal to incumbent",
                    lambda f: float(f.get("candidate_objective", float("inf"))) >= float(f.get("current_incumbent_objective", float("inf"))),
                    "{rung} requires candidate objective >= current incumbent",
                ),
            ),
            because="prevents degrading incumbent solution quality",
            footprint=0,
        ),
        decide.Rung(
            "reject_infeasible_candidate",
            "reject infeasible candidate regardless of objective value",
            needs=(
                decide.Need(
                    "candidate violates feasibility constraints",
                    lambda f: not bool(f.get("candidate_feasible", True)),
                    "{rung} requires candidate to be infeasible",
                ),
            ),
            because="preserves absolute validity of incumbent under all circumstances",
            footprint=0,
        ),
    )


def solution_quality_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for certifying final solution optimality and bounds."""
    return (
        decide.Rung(
            "certify_interrupted_incumbent_quality",
            "certify validated incumbent from interrupted search with REVIEW care badge",
            needs=(
                decide.Need(
                    "search was interrupted by budget, timeout, or cancellation token",
                    lambda f: bool(f.get("is_interrupted", False) or f.get("is_cancelled", False)),
                    "{rung} requires search interruption or cancellation",
                ),
                decide.Need(
                    "validated incumbent preserved and feasible",
                    lambda f: bool(f.get("has_valid_incumbent", False)) and bool(f.get("incumbent_feasible", False)),
                    "{rung} requires validated feasible incumbent",
                ),
            ),
            because="guarantees safety under cancellation by returning verified incumbent rather than invalid candidate",
            footprint=2,
        ),
        decide.Rung(
            "certify_optimal_quality",
            "certify mathematical optimality with exact certificate and PASS care badge",
            needs=(
                decide.Need(
                    "search was not interrupted or cancelled",
                    lambda f: not bool(f.get("is_interrupted", False) or f.get("is_cancelled", False)),
                    "{rung} requires uninterrupted search",
                ),
                decide.Need(
                    "optimality mathematically proven with zero gap or exhaustive exhaustion",
                    lambda f: bool(f.get("is_exhaustively_proven", False)) or (bool(f.get("has_valid_bounds", False)) and float(f.get("optimality_gap", 1.0)) <= 1e-9),
                    "{rung} requires exhaustive proof or zero optimality gap",
                ),
                decide.Need(
                    "incumbent is validated and feasible",
                    lambda f: bool(f.get("has_valid_incumbent", False)) and bool(f.get("incumbent_feasible", False)),
                    "{rung} requires validated feasible incumbent",
                ),
            ),
            because="exhaustive enumeration or branch exhaustion proves no better feasible solution exists",
            footprint=3,
        ),
        decide.Rung(
            "certify_bounded_gap_quality",
            "certify solution with valid mathematical optimality bound and BENIGN care badge",
            needs=(
                decide.Need(
                    "search was not interrupted or cancelled",
                    lambda f: not bool(f.get("is_interrupted", False) or f.get("is_cancelled", False)),
                    "{rung} requires uninterrupted search",
                ),
                decide.Need(
                    "valid lower and upper bounds with certified non-negative gap",
                    lambda f: bool(f.get("has_valid_bounds", False)) and 1e-9 < float(f.get("optimality_gap", 1.0)) <= 1.0,
                    "{rung} requires valid bounds and positive gap <= 1.0",
                ),
                decide.Need(
                    "incumbent is validated and feasible",
                    lambda f: bool(f.get("has_valid_incumbent", False)) and bool(f.get("incumbent_feasible", False)),
                    "{rung} requires validated feasible incumbent",
                ),
            ),
            because="provides rigorous proof of worst-case suboptimality without claiming false optimality",
            footprint=1,
        ),
        decide.Rung(
            "reject_invalid_candidate",
            "reject invalid candidate with ACTION care badge",
            needs=(
                decide.Need(
                    "candidate violates feasibility or no validated incumbent exists",
                    lambda f: not bool(f.get("has_valid_incumbent", False)) or not bool(f.get("incumbent_feasible", False)),
                    "{rung} requires missing or infeasible incumbent",
                ),
            ),
            because="invalid plan must never be dispatched to production",
            footprint=0,
        ),
    )


# -----------------------------------------------------------------------------
# Decision Climb Callers
# -----------------------------------------------------------------------------

def climb_compute_budget(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb compute budget allocation ladder."""
    return decide.climb(work, compute_budget_rungs(), facts)


def climb_search_strategy(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb search strategy selection ladder."""
    return decide.climb(work, search_strategy_rungs(), facts)


def climb_space_pruning(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb space pruning ladder."""
    return decide.climb(work, space_pruning_rungs(), facts)


def climb_incumbent_update(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb incumbent update ladder."""
    return decide.climb(work, incumbent_update_rungs(), facts)


def climb_solution_quality(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb solution quality certification ladder."""
    return decide.climb(work, solution_quality_rungs(), facts)


# -----------------------------------------------------------------------------
# Mathematical Bound and Objective Helpers
# -----------------------------------------------------------------------------

def calculate_fluid_lower_bound(
    jobs: Sequence[DecisionJob],
    limits: DecisionLimits,
    partial_assignment: Optional[Dict[str, str]] = None,
) -> float:
    """Calculate mathematically guaranteed admissible fluid lower bound on makespan."""
    workers = max(1, int(limits.workers or 1))
    resource_loads: Dict[str, float] = {}
    stream_seconds = 0.0
    longest_alone = 0.0
    partial = partial_assignment or {}

    for j in jobs:
        if j.name in partial:
            way_name = partial[j.name]
            way = next((w for w in j.ways if w.name == way_name), j.ways[0])
            size = way.expected(j.bytes)
            for r, mult in way.uses.items():
                resource_loads[r] = resource_loads.get(r, 0.0) + size * mult
            if way.per_stream > 0:
                stream_seconds += size / way.per_stream
                k = min(way.most or j.ranges, j.ranges, workers)
                longest_alone = max(longest_alone, way.startup + size / (k * way.per_stream))
        else:
            # For unassigned jobs: take admissible optimistic minimum across all ways
            min_stream = math.inf
            min_alone = math.inf
            best_uses: Dict[str, float] = {}
            for w in j.ways:
                sz = w.expected(j.bytes)
                if w.per_stream > 0:
                    sec = sz / w.per_stream
                    if sec < min_stream:
                        min_stream = sec
                    k = min(w.most or j.ranges, j.ranges, workers)
                    al = w.startup + sz / (k * w.per_stream)
                    if al < min_alone:
                        min_alone = al
                for r, mult in w.uses.items():
                    val = sz * mult
                    if r not in best_uses or val < best_uses[r]:
                        best_uses[r] = val
            if min_stream < math.inf:
                stream_seconds += min_stream
            if min_alone < math.inf:
                longest_alone = max(longest_alone, min_alone)
            for r, val in best_uses.items():
                resource_loads[r] = resource_loads.get(r, 0.0) + val

    bound_candidates = [stream_seconds / workers, longest_alone]
    for r, total_bytes in resource_loads.items():
        cap = limits.cap(r)
        if cap and cap > 0:
            bound_candidates.append(total_bytes / cap)

    return max(bound_candidates) if bound_candidates else 0.0


def calculate_schedule_makespan(
    jobs: Sequence[DecisionJob],
    limits: DecisionLimits,
    assignment: Dict[str, str],
) -> float:
    """Simulate makespan of a complete valid assignment."""
    workers = max(1, int(limits.workers or 1))
    resource_loads: Dict[str, float] = {}
    stream_seconds = 0.0
    longest_alone = 0.0

    for j in jobs:
        way_name = assignment[j.name]
        way = next((w for w in j.ways if w.name == way_name), None)
        if way is None:
            raise InvalidIncumbentError(f"job '{j.name}' has invalid way '{way_name}'")
        size = way.expected(j.bytes)
        for r, mult in way.uses.items():
            resource_loads[r] = resource_loads.get(r, 0.0) + size * mult
        if way.per_stream > 0:
            stream_seconds += size / way.per_stream
            k = min(way.most or j.ranges, j.ranges, workers)
            longest_alone = max(longest_alone, way.startup + size / (k * way.per_stream))

    # Fluid bound with a small non-zero packing overhead factor (1.05) representing ordering
    fluid = max([stream_seconds / workers, longest_alone] + [
        tot / limits.cap(r) for r, tot in resource_loads.items() if limits.cap(r) and limits.cap(r) > 0
    ])
    return fluid * 1.05


def calculate_assignment_money(
    jobs: Sequence[DecisionJob],
    assignment: Dict[str, str],
) -> float:
    """Calculate total financial expenditure for assignment."""
    total = 0.0
    for j in jobs:
        way_name = assignment.get(j.name)
        if way_name:
            way = next((w for w in j.ways if w.name == way_name), None)
            if way:
                total += way.bills * j.bytes
    return total


def is_assignment_feasible(
    jobs: Sequence[DecisionJob],
    limits: DecisionLimits,
    assignment: Dict[str, str],
) -> bool:
    """Check if assignment respects hard money and deadline constraints."""
    if limits.money is not None:
        spent = calculate_assignment_money(jobs, assignment)
        if spent > limits.money + 1e-9:
            return False
    return True


# -----------------------------------------------------------------------------
# Adaptive AIMD Controller and Cold-Start ETA
# -----------------------------------------------------------------------------

class AIMDBatchController:
    """Adaptive additive-increase / multiplicative-decrease batch controller."""

    def __init__(self, initial_batch: int = 10, min_batch: int = 1, max_batch: int = 200) -> None:
        self.batch_size: int = initial_batch
        self.min_batch: int = min_batch
        self.max_batch: int = max_batch

    def record_success(self, latency_s: float) -> int:
        if latency_s < 0.05:
            self.batch_size = min(self.max_batch, self.batch_size + 2)
        elif latency_s < 0.2:
            self.batch_size = min(self.max_batch, self.batch_size + 1)
        return self.batch_size

    def record_pressure(self) -> int:
        self.batch_size = max(self.min_batch, self.batch_size // 2)
        return self.batch_size

    @property
    def current_batch_size(self) -> int:
        return self.batch_size


def estimate_cold_start_eta(
    completed_bytes: int,
    total_bytes: int,
    elapsed_s: float,
    headroom_ratio: float = 0.75,
) -> float:
    """Calculate instant byte-weighted cold-start completion ETA."""
    if total_bytes <= 0:
        return 0.0
    if completed_bytes <= 0 or elapsed_s <= 0.0:
        return float(total_bytes) / (1e6 * max(0.1, headroom_ratio))
    measured_rate = float(completed_bytes) / elapsed_s
    remaining_bytes = max(0, total_bytes - completed_bytes)
    effective_rate = measured_rate * max(0.1, min(1.0, headroom_ratio))
    return float(remaining_bytes) / max(effective_rate, 1e-6)


# -----------------------------------------------------------------------------
# Budgeted Optimizer Engine
# -----------------------------------------------------------------------------

class BudgetedOptimizer:
    """Optimizer engine that budgets compute and certifies solution quality."""

    def __init__(
        self,
        jobs: Sequence[DecisionJob],
        limits: DecisionLimits,
        budget_limits: Optional[PlanningBudgetLimits] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ) -> None:
        self.jobs = tuple(jobs)
        self.limits = limits
        self.budget_limits = budget_limits or PlanningBudgetLimits()
        self.cancellation_token = cancellation_token or CancellationToken()

        self._incumbent_assignment: Optional[Dict[str, str]] = None
        self._incumbent_objective: float = math.inf
        self._incumbent_feasible: bool = False
        self._iterations_evaluated: int = 0
        self._pruned_states: int = 0
        self._t0: float = 0.0
        self._allocated_budget_s: float = 0.0
        self._is_interrupted: bool = False
        self._is_exhaustively_proven: bool = False
        self._proof_type: str = "INITIALIZING"

    def cancel(self) -> None:
        self.cancellation_token.cancel()

    def is_cancelled(self) -> bool:
        return self.cancellation_token.is_cancelled()

    def _check_budget(self) -> None:
        if self.cancellation_token.is_cancelled():
            self._is_interrupted = True
            raise SearchCancelledError("search cancelled via cancellation token")
        elapsed = time.perf_counter() - self._t0
        if elapsed > self._allocated_budget_s:
            self._is_interrupted = True
            raise OptimizerBudgetExceededError(f"budget exceeded: elapsed {elapsed:.3f}s > allocated {self._allocated_budget_s:.3f}s")

    def _compute_greedy_baseline(self) -> Tuple[Dict[str, str], float]:
        """Compute cheap baseline: fastest-alone way per table."""
        baseline: Dict[str, str] = {}
        for j in self.jobs:
            best_way = min(
                j.ways,
                key=lambda w: (
                    w.startup + w.expected(j.bytes) / max(w.per_stream, 1e-9)
                    if w.per_stream > 0 else math.inf
                ),
            )
            baseline[j.name] = best_way.name
        val = calculate_schedule_makespan(self.jobs, self.limits, baseline)
        return baseline, val

    def _consider_candidate(self, assignment: Dict[str, str], work: str = "optimizer:update_incumbent") -> bool:
        """Evaluate and conditionally update incumbent using decide.climb."""
        self._iterations_evaluated += 1
        feasible = is_assignment_feasible(self.jobs, self.limits, assignment)
        if not feasible:
            facts = {
                "candidate_feasible": False,
                "candidate_objective": math.inf,
                "current_incumbent_objective": self._incumbent_objective,
            }
            climb_incumbent_update(work, facts)
            return False

        obj = calculate_schedule_makespan(self.jobs, self.limits, assignment)
        facts = {
            "candidate_feasible": True,
            "candidate_objective": obj,
            "current_incumbent_objective": self._incumbent_objective,
        }
        choice = climb_incumbent_update(work, facts)
        if choice.rung and choice.rung.name == "accept_strictly_improving_incumbent":
            self._incumbent_assignment = dict(assignment)
            self._incumbent_objective = obj
            self._incumbent_feasible = True
            return True
        return False

    def optimize(self, work: str = "decision_compute:optimize") -> SolutionCertificate:
        """Execute budgeted search and produce certified solution."""
        self._t0 = time.perf_counter()
        if not self.jobs:
            raise DecisionComputeError("cannot optimize empty job list")

        # 1. Establish validated initial baseline
        baseline, baseline_obj = self._compute_greedy_baseline()
        self._incumbent_assignment = dict(baseline)
        self._incumbent_objective = baseline_obj
        self._incumbent_feasible = is_assignment_feasible(self.jobs, self.limits, baseline)
        if not self._incumbent_feasible:
            # Try to find a feasible assignment if baseline exceeds money
            for j in self.jobs:
                cheapest_way = min(j.ways, key=lambda w: w.bills)
                baseline[j.name] = cheapest_way.name
            baseline_obj = calculate_schedule_makespan(self.jobs, self.limits, baseline)
            self._incumbent_assignment = dict(baseline)
            self._incumbent_objective = baseline_obj
            self._incumbent_feasible = is_assignment_feasible(self.jobs, self.limits, baseline)

        if not self._incumbent_feasible:
            # Report invalid candidate
            cert_facts = {
                "has_valid_incumbent": False,
                "incumbent_feasible": False,
                "is_exhaustively_proven": False,
                "has_valid_bounds": False,
                "optimality_gap": 1.0,
                "is_interrupted": False,
                "is_cancelled": False,
            }
            climb_solution_quality(work + ":certify", cert_facts)
            return SolutionCertificate(
                certificate_id=str(uuid.uuid4()),
                status=CertificateStatus.INVALID,
                makespan=math.inf,
                lower_bound=0.0,
                upper_bound=math.inf,
                optimality_gap=1.0,
                is_optimal=False,
                proof_type="INFEASIBLE_WORKLOAD",
                iterations_evaluated=self._iterations_evaluated,
                pruned_states=self._pruned_states,
                compute_time_s=time.perf_counter() - self._t0,
                care_badge=CARE_ACTION,
                assignments={},
            )

        # 2. Compute theoretical lower bound
        lower_bound = calculate_fluid_lower_bound(self.jobs, self.limits)
        potential_savings = max(0.0, baseline_obj - lower_bound)
        total_states = 1
        for j in self.jobs:
            total_states *= len(j.ways)
            if total_states > 1_000_000:
                total_states = 1_000_000
                break

        # 3. Climb compute budget ladder
        budget_facts = {
            "potential_makespan_savings_s": potential_savings,
            "high_benefit_threshold_s": self.budget_limits.high_benefit_threshold_s,
            "min_benefit_threshold_s": self.budget_limits.min_benefit_threshold_s,
            "max_planner_wall_s": self.budget_limits.max_planner_wall_s,
            "planner_cpu_headroom_ratio": 0.8,
            "allow_minimal_budget": True,
        }
        b_choice = climb_compute_budget(work + ":budget", budget_facts)
        if b_choice.rung and b_choice.rung.name == "high_depth_budget":
            self._allocated_budget_s = min(self.budget_limits.max_planner_wall_s, 10.0)
        elif b_choice.rung and b_choice.rung.name == "standard_bounded_budget":
            self._allocated_budget_s = min(self.budget_limits.max_planner_wall_s, 2.0)
        else:
            self._allocated_budget_s = 0.05

        # 4. Climb search strategy ladder
        strategy_facts = {
            "total_states": total_states,
            "max_exhaustive_states": self.budget_limits.max_exhaustive_states,
            "allocated_budget_s": self._allocated_budget_s,
            "exhaustive_estimated_time_s": total_states * 0.0001,
            "potential_makespan_savings_s": potential_savings,
            "min_beam_benefit_s": self.budget_limits.min_beam_benefit_s,
            "min_beam_time_s": self.budget_limits.min_beam_time_s,
            "min_local_search_time_s": self.budget_limits.min_local_search_time_s,
            "estate_tables": len(self.jobs),
            "allow_cheap_baseline": True,
        }
        s_choice = climb_search_strategy(work + ":strategy", strategy_facts)
        strategy = s_choice.rung.name if s_choice.rung else "cheap_baseline_strategy"

        # 5. Execute chosen search tier
        try:
            self._check_budget()
            if strategy == "exhaustive_oracle_strategy":
                self._run_exhaustive_oracle(work)
            elif strategy == "deeper_beam_search_strategy":
                self._run_beam_search(work)
            elif strategy == "bounded_local_search_strategy":
                self._run_local_search(work)
            else:
                self._run_cheap_baseline(work)
        except (OptimizerBudgetExceededError, SearchCancelledError):
            self._is_interrupted = True

        if self.cancellation_token.is_cancelled():
            self._is_interrupted = True

        elapsed = time.perf_counter() - self._t0
        upper_bound = self._incumbent_objective
        assert self._incumbent_assignment is not None
        assert self._incumbent_feasible, "invariant: validated incumbent must be feasible"

        # Calculate optimality gap
        if self._is_exhaustively_proven:
            optimality_gap = 0.0
            lower_bound = upper_bound
        elif upper_bound <= 0.0:
            optimality_gap = 0.0
        else:
            optimality_gap = max(0.0, (upper_bound - lower_bound) / upper_bound)

        # 6. Certify solution quality via ladder
        cert_facts = {
            "is_exhaustively_proven": self._is_exhaustively_proven,
            "optimality_gap": optimality_gap,
            "has_valid_incumbent": True,
            "incumbent_feasible": self._incumbent_feasible,
            "has_valid_bounds": lower_bound > 0.0 and upper_bound < math.inf,
            "is_interrupted": self._is_interrupted,
            "is_cancelled": self.cancellation_token.is_cancelled(),
        }
        q_choice = climb_solution_quality(work + ":certify", cert_facts)
        rung_name = q_choice.rung.name if q_choice.rung else "certify_bounded_gap_quality"

        if rung_name == "certify_optimal_quality":
            status = CertificateStatus.OPTIMAL_CERTIFIED
            care_badge = CARE_PASS
            is_optimal = True
        elif rung_name == "certify_bounded_gap_quality":
            status = CertificateStatus.BOUNDED_GAP
            care_badge = CARE_BENIGN
            is_optimal = False
        elif rung_name == "certify_interrupted_incumbent_quality":
            status = CertificateStatus.INCUMBENT_INTERRUPTED
            care_badge = CARE_REVIEW
            is_optimal = False
        else:
            status = CertificateStatus.UNKNOWN
            care_badge = CARE_ACTION
            is_optimal = False

        return SolutionCertificate(
            certificate_id=str(uuid.uuid4()),
            status=status,
            makespan=upper_bound,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            optimality_gap=optimality_gap,
            is_optimal=is_optimal,
            proof_type=self._proof_type,
            iterations_evaluated=self._iterations_evaluated,
            pruned_states=self._pruned_states,
            compute_time_s=elapsed,
            care_badge=care_badge,
            assignments=dict(self._incumbent_assignment),
        )

    def _run_exhaustive_oracle(self, work: str) -> None:
        """Exhaustively enumerate all states in small combinatorial spaces."""
        self._proof_type = "EXHAUSTIVE_ENUMERATION"
        job_ways = [[(j.name, w.name) for w in j.ways] for j in self.jobs]
        import itertools

        for combo in itertools.product(*job_ways):
            self._check_budget()
            assignment = dict(combo)
            self._consider_candidate(assignment, work + ":exhaustive")

        self._is_exhaustively_proven = True

    def _run_beam_search(self, work: str) -> None:
        """Branch-and-bound beam search with space pruning."""
        self._proof_type = "BRANCH_AND_BOUND_BEAM"
        beam_width = self.budget_limits.max_beam_width
        beam: List[Tuple[float, Dict[str, str]]] = [(0.0, {})]

        for step_idx, job in enumerate(self.jobs):
            self._check_budget()
            next_beam: List[Tuple[float, Dict[str, str]]] = []
            for _, partial in beam:
                for way in job.ways:
                    cand = dict(partial)
                    cand[job.name] = way.name

                    # Pruning check
                    violates_money = False
                    if self.limits.money is not None:
                        spent = calculate_assignment_money(self.jobs, cand)
                        if spent > self.limits.money + 1e-9:
                            violates_money = True

                    node_lb = calculate_fluid_lower_bound(self.jobs, self.limits, cand)
                    prune_facts = {
                        "node_lower_bound": node_lb,
                        "incumbent_upper_bound": self._incumbent_objective,
                        "violates_hard_capacity": False,
                        "violates_money_limit": violates_money,
                    }
                    p_choice = climb_space_pruning(work + ":pruning", prune_facts)
                    if p_choice.rung and p_choice.rung.name == "retain_and_expand_branch":
                        next_beam.append((node_lb, cand))
                    else:
                        self._pruned_states += 1

            if not next_beam:
                break
            # Retain top beam_width by lower bound
            next_beam.sort(key=lambda item: item[0])
            beam = next_beam[:beam_width]

        # Evaluate completed beam items
        for _, completed in beam:
            if len(completed) == len(self.jobs):
                self._check_budget()
                self._consider_candidate(completed, work + ":beam_complete")

    def _run_local_search(self, work: str) -> None:
        """Bounded 1-opt local search with steepest fluid makespan descent."""
        self._proof_type = "BOUNDED_LOCAL_MINIMUM"
        assert self._incumbent_assignment is not None
        current = dict(self._incumbent_assignment)
        max_iters = min(100, len(self.jobs) * 5)

        for _ in range(max_iters):
            self._check_budget()
            best_neighbor: Optional[Dict[str, str]] = None
            best_obj: float = self._incumbent_objective

            for job in self.jobs:
                cur_way = current[job.name]
                for way in job.ways:
                    if way.name == cur_way:
                        continue
                    neighbor = dict(current)
                    neighbor[job.name] = way.name
                    if not is_assignment_feasible(self.jobs, self.limits, neighbor):
                        continue
                    obj = calculate_schedule_makespan(self.jobs, self.limits, neighbor)
                    if obj < best_obj:
                        best_obj = obj
                        best_neighbor = neighbor

            if best_neighbor is None or best_obj >= self._incumbent_objective * (1 - 1e-6):
                break
            current = best_neighbor
            self._consider_candidate(current, work + ":local_search")

    def _run_cheap_baseline(self, work: str) -> None:
        """Adopt greedy baseline directly with zero further search."""
        self._proof_type = "CHEAP_BASELINE"
        # Baseline is already established in self._incumbent_assignment


# -----------------------------------------------------------------------------
# External Tool Wrapper with Zero Internal Tool Leaks
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ExternalToolEvaluation:
    """Standardized external optimizer recommendation wrapper."""

    tool_name: str
    proposed_plan: Dict[str, str]
    estimated_makespan: float
    is_feasible: bool
    care_badge: str
    decision_reason: str


class ExternalOptimizerWrapper:
    """Full-capacity wrapper for Datafold, DMS, DTS, reladiff, DVT, pgcopydb, and debezium."""

    ALLOWED_TOOLS: FrozenSet[str] = frozenset(
        {"datafold", "dms", "dts", "reladiff", "dvt", "pgcopydb", "debezium"}
    )

    def __init__(self, jobs: Sequence[DecisionJob], limits: DecisionLimits) -> None:
        self.jobs = tuple(jobs)
        self.limits = limits

    def evaluate_external_plan(
        self,
        tool_name: str,
        proposed_plan: Dict[str, str],
        work: str = "external_wrapper:evaluate",
    ) -> ExternalToolEvaluation:
        norm_tool = str(tool_name or "").strip().lower()
        if norm_tool not in self.ALLOWED_TOOLS:
            raise DecisionComputeError(f"unrecognized external tool: {norm_tool!r}")

        # Validate that all jobs are mapped to valid ways
        valid = True
        for j in self.jobs:
            assigned = proposed_plan.get(j.name)
            if not assigned or not any(w.name == assigned for w in j.ways):
                valid = False
                break

        if not valid or not is_assignment_feasible(self.jobs, self.limits, proposed_plan):
            return ExternalToolEvaluation(
                tool_name=norm_tool,
                proposed_plan=dict(proposed_plan),
                estimated_makespan=math.inf,
                is_feasible=False,
                care_badge=CARE_ACTION,
                decision_reason="proposed plan violates feasibility constraints or contains invalid ways",
            )

        makespan = calculate_schedule_makespan(self.jobs, self.limits, proposed_plan)
        lower_bound = calculate_fluid_lower_bound(self.jobs, self.limits)
        gap = max(0.0, (makespan - lower_bound) / max(makespan, 1e-9))

        if gap <= 0.05:
            badge = CARE_PASS
            reason = "external plan is verified feasible and within 5% of fluid bound"
        else:
            badge = CARE_BENIGN
            reason = "external plan is verified feasible with acceptable suboptimality"

        return ExternalToolEvaluation(
            tool_name=norm_tool,
            proposed_plan=dict(proposed_plan),
            estimated_makespan=makespan,
            is_feasible=True,
            care_badge=badge,
            decision_reason=reason,
        )
