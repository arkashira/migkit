"""Detect impossible progress and terminate intelligently (Row 604: SMART-N21.progress-liveness).

Extends catch_up_sum, retries, blockers, and autonomous outcomes via decide.Rung
and decide.climb(work, rungs, facts).

Tracks progress certificates and per-resource/fault retry budgets. Detects
nonconvergent arrival/apply rates, dependency deadlocks, hung programs, and
repairs whose drift does not shrink. Chooses an admissible alternative or
isolates/stops with the smallest missing prerequisite.

Guarantees:
- No infinite recovery loops.
- Slow but progressing work differs from deadlock.
- One noisy observation cannot establish impossibility (noise rejection).
- Every decision climbs a decide.Rung ladder.
- Pure UNIX style, zero emojis, care badges ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import fcntl
import hashlib
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
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from . import decide, durable


# -----------------------------------------------------------------------------
# Care Badges, Safe Host Validation, and Error Types
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


class ProgressLivenessError(RuntimeError):
    """Base error for progress and liveness detection operations."""


class UnsafeEndpointHostError(ProgressLivenessError, ValueError):
    """Raised when an endpoint host is outside approved loopback or 10.0.0.x ranges."""


class NonconvergentRateError(ProgressLivenessError):
    """Raised when arrival rate persistently exceeds apply rate preventing convergence."""


class DependencyDeadlockError(ProgressLivenessError):
    """Raised when an unresolvable dependency cycle with stationary progress is detected."""


class HungProgramError(ProgressLivenessError):
    """Raised when an unyielding program or worker heartbeat has ceased beyond deadlines."""


class DivergentRepairDriftError(ProgressLivenessError):
    """Raised when repair operations fail to reduce data drift over successive iterations."""


class RetryBudgetExhaustedError(ProgressLivenessError):
    """Raised when per-resource or per-fault retry budgets are depleted."""


class LeaseConflictError(ProgressLivenessError):
    """Raised when an active verification lease lock cannot be acquired or held."""


def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host uses approved loopback or 10.0.0.x test addresses."""
    cleaned = str(host or "").strip().lower()
    if not cleaned:
        raise UnsafeEndpointHostError("endpoint host cannot be empty")
    if cleaned in SAFE_TEST_HOSTS:
        return cleaned
    if cleaned.startswith("10.0.0."):
        parts = cleaned.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return cleaned
    raise UnsafeEndpointHostError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, ::1, or 10.0.0.x allowed"
    )


# -----------------------------------------------------------------------------
# Domain Specifications & Smallest Missing Prerequisite
# -----------------------------------------------------------------------------

class FaultClass(str, enum.Enum):
    """Classification of operational failure modes."""

    TRANSIENT_NETWORK = "transient_network"
    LOCK_TIMEOUT = "lock_timeout"
    SCHEMA_MISMATCH = "schema_mismatch"
    RATE_LIMIT = "rate_limit"
    DATA_CORRUPTION = "data_corruption"
    DEPENDENCY_BLOCK = "dependency_block"


@dataclass(frozen=True)
class SmallestMissingPrerequisite:
    """Smallest concrete missing prerequisite required to restore liveness."""

    category: str
    resource: str
    needed: str
    current: str
    action_required: str
    care_badge: str = CARE_ACTION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "resource": self.resource,
            "needed": self.needed,
            "current": self.current,
            "action_required": self.action_required,
            "care_badge": self.care_badge,
        }


# -----------------------------------------------------------------------------
# Active Verification Lease (.run.lock)
# -----------------------------------------------------------------------------

class ActiveVerificationLeaseLock:
    """Mutual exclusion `.run.lock` with fcntl lock and heartbeat preventing
    overlapping progress liveness supervision runs."""

    def __init__(self, lock_path: Union[str, Path], lease_seconds: float = 30.0):
        self.lock_path = Path(lock_path)
        self.lease_seconds = max(1.0, float(lease_seconds))
        self._fd: Optional[int] = None
        self.owner_pid: int = os.getpid()
        self.acquired_at: float = 0.0
        self.heartbeat_at: float = 0.0

    def acquire(self) -> None:
        if self._fd is not None:
            self.heartbeat()
            return
        if not self.lock_path.parent.exists():
            durable.private_dir(self.lock_path.parent)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise LeaseConflictError(
                f"Active liveness lease lock already held at {self.lock_path}"
            ) from exc
        self._fd = fd
        now = time.time()
        self.acquired_at = now
        self.heartbeat_at = now
        self._write_metadata()

    def heartbeat(self) -> None:
        if self._fd is None:
            raise LeaseConflictError("Cannot heartbeat an unacquired lease lock")
        self.heartbeat_at = time.time()
        self._write_metadata()

    def _write_metadata(self) -> None:
        if self._fd is None:
            return
        meta = {
            "owner_pid": self.owner_pid,
            "acquired_at": self.acquired_at,
            "heartbeat_at": self.heartbeat_at,
            "lease_seconds": self.lease_seconds,
        }
        payload = (json.dumps(meta) + "\n").encode("utf-8")
        try:
            os.lseek(self._fd, 0, os.SEEK_SET)
            os.write(self._fd, payload)
            os.ftruncate(self._fd, len(payload))
        except OSError as exc:
            raise LeaseConflictError(f"Failed to record lease metadata: {exc}") from exc

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            finally:
                self._fd = None

    def __enter__(self) -> "ActiveVerificationLeaseLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


# -----------------------------------------------------------------------------
# Progress Certificates
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ProgressCertificate:
    """A cryptographic or deterministic proof of monotonically advancing work."""

    certificate_id: str
    unit_id: str
    stage: str
    checkpoint_token: str
    bytes_processed: int
    rows_applied: int
    unresolved_drift: float
    timestamp: float
    signature: str

    @classmethod
    def create(
        cls,
        unit_id: str,
        stage: str,
        checkpoint_token: str,
        bytes_processed: int,
        rows_applied: int,
        unresolved_drift: float = 0.0,
        timestamp: Optional[float] = None,
    ) -> "ProgressCertificate":
        ts = time.time() if timestamp is None else float(timestamp)
        cert_id = f"cert-{uuid.uuid4().hex[:12]}"
        raw_msg = (
            f"{cert_id}:{unit_id}:{stage}:{checkpoint_token}:{bytes_processed}:"
            f"{rows_applied}:{unresolved_drift:.6f}:{ts:.6f}"
        )
        sig = hashlib.sha256(raw_msg.encode("utf-8")).hexdigest()
        return cls(
            certificate_id=cert_id,
            unit_id=unit_id,
            stage=stage,
            checkpoint_token=checkpoint_token,
            bytes_processed=bytes_processed,
            rows_applied=rows_applied,
            unresolved_drift=unresolved_drift,
            timestamp=ts,
            signature=sig,
        )

    def verify_signature(self) -> bool:
        raw_msg = (
            f"{self.certificate_id}:{self.unit_id}:{self.stage}:{self.checkpoint_token}:"
            f"{self.bytes_processed}:{self.rows_applied}:{self.unresolved_drift:.6f}:"
            f"{self.timestamp:.6f}"
        )
        expected = hashlib.sha256(raw_msg.encode("utf-8")).hexdigest()
        return self.signature == expected

    def advance_verified(self, previous: "ProgressCertificate") -> bool:
        """Verify that work has made non-trivial forward progress since previous certificate."""
        if not self.verify_signature() or not previous.verify_signature():
            return False
        if self.timestamp <= previous.timestamp:
            return False
        bytes_grew = self.bytes_processed > previous.bytes_processed
        rows_grew = self.rows_applied > previous.rows_applied
        drift_shrank = self.unresolved_drift < previous.unresolved_drift
        token_advanced = self.checkpoint_token != previous.checkpoint_token
        return bytes_grew or rows_grew or drift_shrank or token_advanced

    def to_dict(self) -> Dict[str, Any]:
        return {
            "certificate_id": self.certificate_id,
            "unit_id": self.unit_id,
            "stage": self.stage,
            "checkpoint_token": self.checkpoint_token,
            "bytes_processed": self.bytes_processed,
            "rows_applied": self.rows_applied,
            "unresolved_drift": self.unresolved_drift,
            "timestamp": self.timestamp,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProgressCertificate":
        return cls(
            certificate_id=str(data["certificate_id"]),
            unit_id=str(data["unit_id"]),
            stage=str(data["stage"]),
            checkpoint_token=str(data["checkpoint_token"]),
            bytes_processed=int(data["bytes_processed"]),
            rows_applied=int(data["rows_applied"]),
            unresolved_drift=float(data["unresolved_drift"]),
            timestamp=float(data["timestamp"]),
            signature=str(data["signature"]),
        )


# -----------------------------------------------------------------------------
# Rate Observations & Extended Catch-Up
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class RateObservation:
    """Discrete rate measurement for source arrival and target apply rates."""

    timestamp: float
    arrival_rate: float
    apply_rate: float
    backlog_rows: float
    p90_apply_rate: float
    is_noisy: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "arrival_rate": self.arrival_rate,
            "apply_rate": self.apply_rate,
            "backlog_rows": self.backlog_rows,
            "p90_apply_rate": self.p90_apply_rate,
            "is_noisy": self.is_noisy,
        }


@dataclass
class ExtendedCatchUpResult:
    """Complete evaluation of catch-up convergence and rate feasibility."""

    is_convergent: bool
    care_badge: str
    catch_up_seconds: Optional[float]
    p90_catch_up_seconds: Optional[float]
    rate_deficit: float
    divergence_count: int
    noise_tolerated: bool
    detail: str
    choice_path: str
    smallest_prerequisite: Optional[SmallestMissingPrerequisite] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_convergent": self.is_convergent,
            "care_badge": self.care_badge,
            "catch_up_seconds": self.catch_up_seconds,
            "p90_catch_up_seconds": self.p90_catch_up_seconds,
            "rate_deficit": self.rate_deficit,
            "divergence_count": self.divergence_count,
            "noise_tolerated": self.noise_tolerated,
            "detail": self.detail,
            "choice_path": self.choice_path,
            "smallest_prerequisite": (
                self.smallest_prerequisite.to_dict()
                if self.smallest_prerequisite
                else None
            ),
        }


# -----------------------------------------------------------------------------
# Decision Rung Ladders (AST Discovery Target Functions)
# -----------------------------------------------------------------------------

def convergence_evaluation_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for evaluating catch-up rate convergence and noise rejection."""
    return (
        decide.Rung(
            "convergent_catch_up",
            "certify arrival and apply rates converge with shrinking backlog",
            needs=(
                decide.Need(
                    "apply rate exceeds arrival rate with positive net clearance",
                    lambda f: float(f.get("apply_rate", 0.0)) > float(f.get("arrival_rate", 0.0)),
                    "{rung} requires target apply rate to strictly exceed arrival rate",
                ),
                decide.Need(
                    "consecutive nonconvergent observation count is zero",
                    lambda f: int(f.get("divergence_count", 0)) == 0,
                    "{rung} requires zero unmitigated divergence observations",
                ),
                decide.Need(
                    "projected catch-up duration within deadline",
                    lambda f: float(f.get("catch_up_seconds", 0.0)) <= float(f.get("deadline_seconds", 1e9)),
                    "{rung} requires catch-up duration within deadline",
                ),
            ),
            because="certifies arrival and apply rates converge with shrinking backlog",
            footprint=0,
        ),
        decide.Rung(
            "transient_jitter_tolerated",
            "tolerate transient rate dip or noisy observation without aborting",
            needs=(
                decide.Need(
                    "divergence observations within transient noise tolerance threshold",
                    lambda f: 0 < int(f.get("divergence_count", 0)) < int(f.get("min_divergence_observations", 3)),
                    "{rung} requires non-zero divergence below persistent threshold",
                ),
                decide.Need(
                    "backlog has not exceeded hard memory or disk spool limit",
                    lambda f: float(f.get("backlog_bytes", 0.0)) <= float(f.get("max_spool_bytes", 1e12)),
                    "{rung} requires backlog within spool capacity",
                ),
            ),
            because="tolerates transient rate dip or noisy observation without aborting",
            footprint=1,
        ),
        decide.Rung(
            "divergent_rate_isolate_prerequisite",
            "isolate smallest missing prerequisite for nonconvergent arrival apply rate",
            needs=(
                decide.Need(
                    "confirmed persistent divergence over multiple observation windows",
                    lambda f: int(f.get("divergence_count", 0)) >= int(f.get("min_divergence_observations", 3)),
                    "{rung} requires persistent divergence over multiple windows",
                ),
                decide.Need(
                    "smallest missing prerequisite isolated",
                    lambda f: bool(f.get("has_prerequisite", False)),
                    "{rung} requires explicit smallest missing prerequisite",
                ),
            ),
            because="isolates smallest missing prerequisite for nonconvergent arrival apply rate",
            footprint=2,
        ),
    )


def deadlock_liveness_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for evaluating dependency deadlocks vs slow progress."""
    return (
        decide.Rung(
            "liveness_unblocked",
            "certify tasks are unblocked with acyclic dependency graph",
            needs=(
                decide.Need(
                    "dependency graph has zero cycles",
                    lambda f: not bool(f.get("has_dependency_cycle", False)),
                    "{rung} forbids dependency cycles",
                ),
                decide.Need(
                    "active tasks reporting timely progress",
                    lambda f: bool(f.get("tasks_progressing", True)),
                    "{rung} requires active tasks to report progress",
                ),
            ),
            because="certifies tasks are unblocked with acyclic dependency graph",
            footprint=0,
        ),
        decide.Rung(
            "slow_progress_admitted",
            "admit slow but progressing work distinguished from true deadlock",
            needs=(
                decide.Need(
                    "progress certificates show monotonic advancement despite slow rate",
                    lambda f: bool(f.get("certificates_advancing", False)),
                    "{rung} requires advancing progress certificates",
                ),
                decide.Need(
                    "tasks not frozen in stationary deadlock cycle",
                    lambda f: not (
                        bool(f.get("has_dependency_cycle", False))
                        and not bool(f.get("certificates_advancing", False))
                    ),
                    "{rung} forbids stationary deadlock cycle",
                ),
            ),
            because="admits slow but progressing work distinguished from true deadlock",
            footprint=1,
        ),
        decide.Rung(
            "dependency_deadlock_resolution",
            "resolve dependency deadlock or isolate smallest blocking prerequisite",
            needs=(
                decide.Need(
                    "confirmed dependency cycle with zero progress advancement",
                    lambda f: bool(f.get("has_dependency_cycle", False)) and not bool(f.get("certificates_advancing", False)),
                    "{rung} requires confirmed cycle and zero progress",
                ),
                decide.Need(
                    "deadlock resolution strategy or isolating prerequisite identified",
                    lambda f: bool(f.get("deadlock_resolution_available", False)),
                    "{rung} requires deadlock resolution or prerequisite",
                ),
            ),
            because="resolves dependency deadlock or isolates smallest blocking prerequisite",
            footprint=2,
        ),
    )


def hung_program_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for detecting hung programs and unyielding workers."""
    return (
        decide.Rung(
            "healthy_heartbeat_active",
            "certify worker heartbeat and progress are active and healthy",
            needs=(
                decide.Need(
                    "worker heartbeat within normal period",
                    lambda f: float(f.get("heartbeat_age_seconds", 0.0)) <= float(f.get("heartbeat_timeout_seconds", 30.0)),
                    "{rung} requires heartbeat age within normal timeout",
                ),
                decide.Need(
                    "progress certificate or I/O actively advancing",
                    lambda f: bool(f.get("has_recent_progress", True)),
                    "{rung} requires recent progress certificates or I/O activity",
                ),
            ),
            because="certifies worker heartbeat and progress are active and healthy",
            footprint=0,
        ),
        decide.Rung(
            "quiescent_heartbeat_within_slack",
            "permit quiescent worker within measured p99 query duration slack",
            needs=(
                decide.Need(
                    "elapsed duration within measured p99 query duration slack",
                    lambda f: float(f.get("heartbeat_age_seconds", 0.0)) <= float(f.get("p99_query_seconds", 120.0)),
                    "{rung} requires age within measured p99 query duration",
                ),
                decide.Need(
                    "process actively running in OS process table",
                    lambda f: bool(f.get("process_alive", True)),
                    "{rung} requires target process to be alive",
                ),
            ),
            because="permits quiescent worker within measured p99 query duration slack",
            footprint=1,
        ),
        decide.Rung(
            "hung_worker_quarantine",
            "quarantine hung worker and reclaim allocated resources",
            needs=(
                decide.Need(
                    "heartbeat age exceeds deadline and p99 slack without progress",
                    lambda f: float(f.get("heartbeat_age_seconds", 0.0)) > float(f.get("p99_query_seconds", 120.0)),
                    "{rung} requires heartbeat age to exceed p99 slack",
                ),
                decide.Need(
                    "quarantine or kill capability supported",
                    lambda f: bool(f.get("quarantine_supported", True)),
                    "{rung} requires worker quarantine capability",
                ),
            ),
            because="quarantines hung worker and reclaims allocated resources",
            footprint=2,
        ),
    )


def repair_drift_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for evaluating whether data repair drift is shrinking."""
    return (
        decide.Rung(
            "repair_drift_converging",
            "certify repair drift strictly shrinks between iterations",
            needs=(
                decide.Need(
                    "drift metric strictly shrinking between iterations",
                    lambda f: float(f.get("drift_delta", 0.0)) < 0.0,
                    "{rung} requires drift metric strictly shrinking",
                ),
                decide.Need(
                    "current drift within bounded convergence trajectory",
                    lambda f: float(f.get("current_drift", 0.0)) >= 0.0,
                    "{rung} requires valid current drift metric",
                ),
            ),
            because="certifies repair drift strictly shrinks between iterations",
            footprint=0,
        ),
        decide.Rung(
            "repair_drift_stationary_warn",
            "warn on stationary repair drift under transient concurrent write noise",
            needs=(
                decide.Need(
                    "drift delta is non-negative but within transient noise limit",
                    lambda f: (
                        float(f.get("drift_delta", 0.0)) >= 0.0
                        and int(f.get("stationary_count", 0)) < int(f.get("max_stationary_repairs", 2))
                    ),
                    "{rung} requires non-negative drift within stationary count limit",
                ),
                decide.Need(
                    "repair attempt budget remaining",
                    lambda f: int(f.get("repair_attempts", 0)) < int(f.get("max_repair_attempts", 5)),
                    "{rung} requires remaining repair attempts",
                ),
            ),
            because="warns on stationary repair drift under transient concurrent write noise",
            footprint=1,
        ),
        decide.Rung(
            "repair_drift_diverging_action",
            "abort nonconvergent repair loop and isolate drift prerequisite",
            needs=(
                decide.Need(
                    "drift fails to shrink across stationary limit or strictly expands",
                    lambda f: (
                        int(f.get("stationary_count", 0)) >= int(f.get("max_stationary_repairs", 2))
                        or float(f.get("drift_delta", 0.0)) > float(f.get("drift_expansion_tolerance", 0.0))
                    ),
                    "{rung} requires stationary limit exceeded or drift expansion",
                ),
                decide.Need(
                    "smallest missing repair prerequisite identified",
                    lambda f: bool(f.get("repair_prerequisite_identified", False)),
                    "{rung} requires identified repair prerequisite",
                ),
            ),
            because="aborts nonconvergent repair loop and isolates drift prerequisite",
            footprint=2,
        ),
    )


def retry_budget_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for per-resource and per-fault retry budget management."""
    return (
        decide.Rung(
            "retry_budget_available",
            "admit retry within per-resource and per-fault budget limits",
            needs=(
                decide.Need(
                    "resource retry count strictly within resource limit",
                    lambda f: int(f.get("resource_retries", 0)) < int(f.get("max_resource_retries", 3)),
                    "{rung} requires resource retries within budget",
                ),
                decide.Need(
                    "fault class retry count strictly within fault limit",
                    lambda f: int(f.get("fault_retries", 0)) < int(f.get("max_fault_retries", 3)),
                    "{rung} requires fault retries within budget",
                ),
            ),
            because="admits retry within per-resource and per-fault budget limits",
            footprint=0,
        ),
        decide.Rung(
            "retry_budget_exhausted_alternative",
            "switch to admissible alternative strategy upon retry budget exhaustion",
            needs=(
                decide.Need(
                    "resource or fault retry limit reached",
                    lambda f: (
                        int(f.get("resource_retries", 0)) >= int(f.get("max_resource_retries", 3))
                        or int(f.get("fault_retries", 0)) >= int(f.get("max_fault_retries", 3))
                    ),
                    "{rung} requires retry limit reached",
                ),
                decide.Need(
                    "admissible alternative execution path available",
                    lambda f: bool(f.get("has_admissible_alternative", False)),
                    "{rung} requires viable alternative execution path",
                ),
            ),
            because="switches to admissible alternative strategy upon retry budget exhaustion",
            footprint=1,
        ),
        decide.Rung(
            "retry_budget_exhausted_stop",
            "stop infinite recovery loop and isolate smallest missing prerequisite",
            needs=(
                decide.Need(
                    "retry budget exhausted and no admissible alternatives",
                    lambda f: (
                        (
                            int(f.get("resource_retries", 0)) >= int(f.get("max_resource_retries", 3))
                            or int(f.get("fault_retries", 0)) >= int(f.get("max_fault_retries", 3))
                        )
                        and not bool(f.get("has_admissible_alternative", False))
                    ),
                    "{rung} requires exhausted budget and no alternative",
                ),
                decide.Need(
                    "terminal isolation prerequisite formulated",
                    lambda f: bool(f.get("terminal_prerequisite_formulated", True)),
                    "{rung} requires terminal isolation prerequisite",
                ),
            ),
            because="stops infinite recovery loop and isolates smallest missing prerequisite",
            footprint=2,
        ),
    )


def autonomous_outcome_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for overall autonomous outcome determination."""
    return (
        decide.Rung(
            "autonomous_certify_progress",
            "certify healthy forward progress and convergence with PASS care badge",
            needs=(
                decide.Need(
                    "all active tasks hold valid advancing progress certificates",
                    lambda f: bool(f.get("all_certificates_advancing", False)),
                    "{rung} requires advancing certificates across all tasks",
                ),
                decide.Need(
                    "arrival and apply rates are convergent",
                    lambda f: bool(f.get("rates_convergent", False)),
                    "{rung} requires convergent rates",
                ),
                decide.Need(
                    "zero active deadlocks or hung workers",
                    lambda f: bool(f.get("zero_deadlocks_or_hung", False)),
                    "{rung} requires zero deadlocks and zero hung workers",
                ),
            ),
            because="certifies healthy forward progress and convergence with PASS care badge",
            footprint=0,
        ),
        decide.Rung(
            "autonomous_replan_alternative",
            "replan execution over admissible alternative path with REVIEW care badge",
            needs=(
                decide.Need(
                    "liveness impairment detected",
                    lambda f: bool(f.get("liveness_impaired", False)),
                    "{rung} requires detected liveness impairment",
                ),
                decide.Need(
                    "viable fallback plan verified admissible",
                    lambda f: bool(f.get("fallback_admissible", False)),
                    "{rung} requires verified admissible fallback plan",
                ),
            ),
            because="replans execution over admissible alternative path with REVIEW care badge",
            footprint=1,
        ),
        decide.Rung(
            "autonomous_isolate_and_terminate",
            "conclude impossible progress and terminate with ACTION care badge",
            needs=(
                decide.Need(
                    "impossible progress conclusively proved by evidence",
                    lambda f: bool(f.get("impossible_progress_proved", False)),
                    "{rung} requires conclusive proof of impossible progress",
                ),
                decide.Need(
                    "minimal missing prerequisite isolated with ACTION care badge",
                    lambda f: bool(f.get("smallest_prerequisite_isolated", False)),
                    "{rung} requires smallest prerequisite isolated",
                ),
            ),
            because="concludes impossible progress and terminates with ACTION care badge",
            footprint=2,
        ),
    )


# -----------------------------------------------------------------------------
# Decision Climbing Wrappers (decide.climb)
# -----------------------------------------------------------------------------

def climb_convergence_evaluation(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb convergence evaluation ladder for arrival/apply rates."""
    return decide.climb(work, convergence_evaluation_rungs(), facts)


def climb_deadlock_liveness(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb deadlock liveness ladder for dependency graph and certificates."""
    return decide.climb(work, deadlock_liveness_rungs(), facts)


def climb_hung_program(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb hung program ladder for worker heartbeats and execution times."""
    return decide.climb(work, hung_program_rungs(), facts)


def climb_repair_drift(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb repair drift ladder for data divergence reduction."""
    return decide.climb(work, repair_drift_rungs(), facts)


def climb_retry_budget(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb retry budget ladder for per-resource and per-fault limits."""
    return decide.climb(work, retry_budget_rungs(), facts)


def climb_autonomous_outcome(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb autonomous outcome ladder for global liveness state."""
    return decide.climb(work, autonomous_outcome_rungs(), facts)


# -----------------------------------------------------------------------------
# Extended Catch-Up Sum & Rate Convergence Engine
# -----------------------------------------------------------------------------

def extended_catch_up_sum(
    observations: Sequence[RateObservation],
    backlog_rows: float,
    deadline_seconds: float = 3600.0,
    min_divergence_observations: int = 3,
    max_spool_bytes: float = 1e12,
    work: str = "catch_up_sum_convergence",
) -> ExtendedCatchUpResult:
    """Extend catch_up_sum with multi-observation noise rejection and decide.climb.

    One noisy observation cannot establish impossibility: a single rate dip or
    spike is classified as transient jitter (BENIGN or REVIEW). Only when
    persistent nonconvergent observations reach `min_divergence_observations`
    is a nonconvergent failure declared and the smallest missing prerequisite
    isolated.
    """
    if not observations:
        raise NonconvergentRateError("cannot evaluate convergence with zero rate observations")

    # Count consecutive nonconvergent readings from recent tail
    divergence_count = 0
    for obs in reversed(observations):
        if obs.apply_rate <= obs.arrival_rate or obs.is_noisy:
            divergence_count += 1
        else:
            break

    latest = observations[-1]
    arrive = float(latest.arrival_rate)
    apply = float(latest.apply_rate)
    p90_apply = float(latest.p90_apply_rate)

    catch_up_sec: Optional[float] = None
    p90_sec: Optional[float] = None
    deficit = max(0.0, arrive - apply)

    if apply > arrive:
        catch_up_sec = max(0.0, backlog_rows / (apply - arrive))
    if p90_apply > arrive:
        p90_sec = max(0.0, backlog_rows / (p90_apply - arrive))

    prerequisite: Optional[SmallestMissingPrerequisite] = None
    if divergence_count >= min_divergence_observations or apply <= arrive:
        needed_rate = math.ceil(arrive * 1.25)
        prerequisite = SmallestMissingPrerequisite(
            category="apply_rate_deficit",
            resource="target_writer",
            needed=f"apply rate >= {needed_rate:,.0f} rows/s (surplus of {(needed_rate - arrive):,.0f} rows/s)",
            current=f"apply rate ~{apply:,.0f} rows/s vs arrival ~{arrive:,.0f} rows/s",
            action_required="scale target write capacity, drop non-critical target indexes, or shift run window",
            care_badge=CARE_ACTION,
        )

    facts = {
        "arrival_rate": arrive,
        "apply_rate": apply,
        "divergence_count": divergence_count,
        "min_divergence_observations": min_divergence_observations,
        "catch_up_seconds": catch_up_sec if catch_up_sec is not None else 1e12,
        "deadline_seconds": deadline_seconds,
        "backlog_bytes": float(backlog_rows) * 128.0,
        "max_spool_bytes": max_spool_bytes,
        "has_prerequisite": prerequisite is not None,
    }

    choice = climb_convergence_evaluation(work, facts)

    if choice.path == "convergent_catch_up":
        care_badge = CARE_PASS
        detail = (
            f"convergent: backlog {backlog_rows:,.0f} rows clears in ~{catch_up_sec:,.1f}s "
            f"(apply ~{apply:,.0f}/s > arrive ~{arrive:,.0f}/s)"
        )
        is_convergent = True
        noise_tolerated = False
    elif choice.path == "transient_jitter_tolerated":
        care_badge = CARE_REVIEW
        detail = (
            f"transient jitter tolerated: {divergence_count}/{min_divergence_observations} "
            f"nonconvergent observations under noisy load; continuing verification"
        )
        is_convergent = True
        noise_tolerated = True
    else:
        care_badge = CARE_ACTION
        detail = (
            f"divergent rate: arrival ~{arrive:,.0f}/s exceeds apply ~{apply:,.0f}/s over "
            f"{divergence_count} consecutive observations; impossible progress"
        )
        is_convergent = False
        noise_tolerated = False

    return ExtendedCatchUpResult(
        is_convergent=is_convergent,
        care_badge=care_badge,
        catch_up_seconds=catch_up_sec,
        p90_catch_up_seconds=p90_sec,
        rate_deficit=deficit,
        divergence_count=divergence_count,
        noise_tolerated=noise_tolerated,
        detail=detail,
        choice_path=choice.path or "none",
        smallest_prerequisite=prerequisite,
    )


# -----------------------------------------------------------------------------
# Dependency Graph & Deadlock Detection
# -----------------------------------------------------------------------------

@dataclass
class DeadlockEvaluationResult:
    """Evaluation result distinguishing true deadlock from slow progress."""

    is_deadlocked: bool
    care_badge: str
    cycles_detected: List[List[str]]
    advancing_tasks: List[str]
    stationary_tasks: List[str]
    choice_path: str
    detail: str
    smallest_prerequisite: Optional[SmallestMissingPrerequisite] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_deadlocked": self.is_deadlocked,
            "care_badge": self.care_badge,
            "cycles_detected": self.cycles_detected,
            "advancing_tasks": self.advancing_tasks,
            "stationary_tasks": self.stationary_tasks,
            "choice_path": self.choice_path,
            "detail": self.detail,
            "smallest_prerequisite": (
                self.smallest_prerequisite.to_dict()
                if self.smallest_prerequisite
                else None
            ),
        }


class DependencyDeadlockDetector:
    """Tracks task dependency graphs and verifies whether stationary cycles exist."""

    def __init__(self) -> None:
        self._graph: Dict[str, Set[str]] = collections.defaultdict(set)

    def add_dependency(self, waiter: str, blocker: str) -> None:
        """Record that `waiter` is waiting on resource or completion of `blocker`."""
        if waiter and blocker:
            self._graph[str(waiter)].add(str(blocker))

    def clear(self) -> None:
        self._graph.clear()

    def find_cycles(self) -> List[List[str]]:
        """Return all directed cycles in the wait-for dependency graph."""
        visited: Dict[str, int] = {}  # 0: unvisited, 1: visiting, 2: visited
        cycles: List[List[str]] = []
        path: List[str] = []

        def dfs(node: str) -> None:
            visited[node] = 1
            path.append(node)
            for neighbor in self._graph.get(node, ()):
                state = visited.get(neighbor, 0)
                if state == 1:
                    cycle_start = path.index(neighbor)
                    cycles.append(list(path[cycle_start:]))
                elif state == 0:
                    dfs(neighbor)
            path.pop()
            visited[node] = 2

        all_nodes = set(self._graph.keys())
        for n in self._graph.values():
            all_nodes.update(n)

        for node in sorted(all_nodes):
            if visited.get(node, 0) == 0:
                dfs(node)

        return cycles

    def evaluate_liveness(
        self,
        task_certificates: Mapping[str, Sequence[ProgressCertificate]],
        work: str = "deadlock_liveness_check",
    ) -> DeadlockEvaluationResult:
        """Evaluate dependency graph against advancing progress certificates."""
        cycles = self.find_cycles()
        advancing: List[str] = []
        stationary: List[str] = []

        for task, certs in task_certificates.items():
            if len(certs) >= 2 and certs[-1].advance_verified(certs[-2]):
                advancing.append(task)
            elif certs:
                stationary.append(task)

        has_cycle = len(cycles) > 0
        all_advancing = len(advancing) > 0 and len(stationary) == 0
        some_advancing = len(advancing) > 0

        prerequisite: Optional[SmallestMissingPrerequisite] = None
        if has_cycle:
            cycle_desc = " -> ".join(cycles[0])
            prerequisite = SmallestMissingPrerequisite(
                category="dependency_deadlock",
                resource=cycles[0][0],
                needed="acyclic dependency DAG or deferrable foreign key constraints",
                current=f"circular wait-for dependency: {cycle_desc}",
                action_required="reorder migration phases, defer FKs, or apply transaction barrier",
                care_badge=CARE_ACTION,
            )

        facts = {
            "has_dependency_cycle": has_cycle,
            "tasks_progressing": all_advancing or (not has_cycle and len(stationary) == 0),
            "certificates_advancing": some_advancing,
            "deadlock_resolution_available": prerequisite is not None,
        }

        choice = climb_deadlock_liveness(work, facts)

        if choice.path == "liveness_unblocked":
            care_badge = CARE_PASS
            is_deadlocked = False
            detail = "unblocked: zero dependency cycles, all tasks progressing"
        elif choice.path == "slow_progress_admitted":
            care_badge = CARE_REVIEW
            is_deadlocked = False
            detail = (
                f"slow progress admitted: {len(advancing)} tasks advancing monotonically; "
                f"distinguished from deadlock"
            )
        else:
            care_badge = CARE_ACTION
            is_deadlocked = True
            detail = f"deadlock detected: {len(cycles)} cycles with stationary progress"

        return DeadlockEvaluationResult(
            is_deadlocked=is_deadlocked,
            care_badge=care_badge,
            cycles_detected=cycles,
            advancing_tasks=advancing,
            stationary_tasks=stationary,
            choice_path=choice.path or "none",
            detail=detail,
            smallest_prerequisite=prerequisite,
        )


# -----------------------------------------------------------------------------
# Hung Program & Worker Watchdog
# -----------------------------------------------------------------------------

@dataclass
class HungProgramResult:
    """Watchdog evaluation for hung program or unresponsive worker."""

    is_hung: bool
    care_badge: str
    worker_id: str
    heartbeat_age_seconds: float
    choice_path: str
    detail: str
    smallest_prerequisite: Optional[SmallestMissingPrerequisite] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_hung": self.is_hung,
            "care_badge": self.care_badge,
            "worker_id": self.worker_id,
            "heartbeat_age_seconds": self.heartbeat_age_seconds,
            "choice_path": self.choice_path,
            "detail": self.detail,
            "smallest_prerequisite": (
                self.smallest_prerequisite.to_dict()
                if self.smallest_prerequisite
                else None
            ),
        }


class HungProgramWatchdog:
    """Detects hung subprocesses and unyielding query loops without premature termination."""

    def evaluate_worker(
        self,
        worker_id: str,
        heartbeat_timestamp: float,
        p99_query_seconds: float = 120.0,
        heartbeat_timeout_seconds: float = 30.0,
        has_recent_progress: bool = True,
        process_alive: bool = True,
        quarantine_supported: bool = True,
        current_time: Optional[float] = None,
        work: str = "hung_program_watchdog",
    ) -> HungProgramResult:
        now = time.time() if current_time is None else float(current_time)
        age = max(0.0, now - float(heartbeat_timestamp))

        prerequisite: Optional[SmallestMissingPrerequisite] = None
        if age > p99_query_seconds or not process_alive:
            prerequisite = SmallestMissingPrerequisite(
                category="hung_worker",
                resource=worker_id,
                needed="healthy heartbeat within p99 query duration",
                current=f"heartbeat age {age:,.1f}s exceeds p99 threshold {p99_query_seconds:,.1f}s",
                action_required="quarantine hung worker, restart connection pool, and re-enqueue batch",
                care_badge=CARE_ACTION,
            )

        facts = {
            "heartbeat_age_seconds": age,
            "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
            "p99_query_seconds": p99_query_seconds,
            "has_recent_progress": has_recent_progress,
            "process_alive": process_alive,
            "quarantine_supported": quarantine_supported,
        }

        choice = climb_hung_program(work, facts)

        if choice.path == "healthy_heartbeat_active":
            care_badge = CARE_PASS
            is_hung = False
            detail = f"healthy: worker {worker_id} heartbeat active (age {age:.1f}s)"
        elif choice.path == "quiescent_heartbeat_within_slack":
            care_badge = CARE_REVIEW
            is_hung = False
            detail = (
                f"quiescent: worker {worker_id} idle age {age:.1f}s within p99 "
                f"slack {p99_query_seconds:.1f}s; not hung"
            )
        else:
            care_badge = CARE_ACTION
            is_hung = True
            detail = (
                f"hung program: worker {worker_id} age {age:.1f}s exceeds p99 "
                f"slack {p99_query_seconds:.1f}s; quarantined"
            )

        return HungProgramResult(
            is_hung=is_hung,
            care_badge=care_badge,
            worker_id=worker_id,
            heartbeat_age_seconds=age,
            choice_path=choice.path or "none",
            detail=detail,
            smallest_prerequisite=prerequisite,
        )


# -----------------------------------------------------------------------------
# Repair Drift Monitoring (Non-Shrinking Drift Detection)
# -----------------------------------------------------------------------------

@dataclass
class RepairDriftResult:
    """Evaluation of data repair drift trajectory across iterations."""

    is_convergent: bool
    care_badge: str
    repair_id: str
    current_drift: float
    drift_delta: float
    stationary_count: int
    repair_attempts: int
    choice_path: str
    detail: str
    smallest_prerequisite: Optional[SmallestMissingPrerequisite] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_convergent": self.is_convergent,
            "care_badge": self.care_badge,
            "repair_id": self.repair_id,
            "current_drift": self.current_drift,
            "drift_delta": self.drift_delta,
            "stationary_count": self.stationary_count,
            "repair_attempts": self.repair_attempts,
            "choice_path": self.choice_path,
            "detail": self.detail,
            "smallest_prerequisite": (
                self.smallest_prerequisite.to_dict()
                if self.smallest_prerequisite
                else None
            ),
        }


class RepairDriftTracker:
    """Tracks repair iterations and aborts infinite loops when drift does not shrink."""

    def __init__(self) -> None:
        self._history: Dict[str, List[float]] = collections.defaultdict(list)

    def record_observation(self, repair_id: str, drift_value: float) -> None:
        self._history[str(repair_id)].append(max(0.0, float(drift_value)))

    def evaluate_repair_drift(
        self,
        repair_id: str,
        max_stationary_repairs: int = 2,
        max_repair_attempts: int = 5,
        drift_expansion_tolerance: float = 0.0,
        work: str = "repair_drift_evaluation",
    ) -> RepairDriftResult:
        history = self._history.get(str(repair_id), [])
        if not history:
            raise DivergentRepairDriftError(f"no repair observations recorded for {repair_id}")

        attempts = len(history)
        current = history[-1]
        if attempts == 1:
            delta = -1.0  # Initial observation considered bounded
            stationary_count = 0
        else:
            delta = current - history[-2]
            stationary_count = 0
            for i in range(len(history) - 1, 0, -1):
                if history[i] >= history[i - 1]:
                    stationary_count += 1
                else:
                    break

        prerequisite: Optional[SmallestMissingPrerequisite] = None
        if stationary_count >= max_stationary_repairs or delta > drift_expansion_tolerance or attempts >= max_repair_attempts:
            prerequisite = SmallestMissingPrerequisite(
                category="divergent_repair",
                resource=repair_id,
                needed="monotonically shrinking drift delta (delta < 0)",
                current=f"stationary count {stationary_count}/{max_stationary_repairs}, delta {delta:+.2f}, drift {current:,.0f}",
                action_required="isolate concurrent writers, check composite key collation, or switch to bulk replacement",
                care_badge=CARE_ACTION,
            )

        facts = {
            "drift_delta": delta,
            "current_drift": current,
            "stationary_count": stationary_count,
            "max_stationary_repairs": max_stationary_repairs,
            "repair_attempts": attempts,
            "max_repair_attempts": max_repair_attempts,
            "drift_expansion_tolerance": drift_expansion_tolerance,
            "repair_prerequisite_identified": prerequisite is not None,
        }

        choice = climb_repair_drift(work, facts)

        if choice.path == "repair_drift_converging":
            care_badge = CARE_PASS
            is_convergent = True
            detail = f"converging: drift shrank by {abs(delta):,.2f} to {current:,.2f}"
        elif choice.path == "repair_drift_stationary_warn":
            care_badge = CARE_REVIEW
            is_convergent = True
            detail = (
                f"stationary drift warning: {stationary_count}/{max_stationary_repairs} "
                f"attempts unchanged under write noise; retrying"
            )
        else:
            care_badge = CARE_ACTION
            is_convergent = False
            detail = (
                f"divergent repair: drift failed to shrink over {attempts} attempts "
                f"(current {current:,.2f}, delta {delta:+.2f}); infinite loop aborted"
            )

        return RepairDriftResult(
            is_convergent=is_convergent,
            care_badge=care_badge,
            repair_id=repair_id,
            current_drift=current,
            drift_delta=delta,
            stationary_count=stationary_count,
            repair_attempts=attempts,
            choice_path=choice.path or "none",
            detail=detail,
            smallest_prerequisite=prerequisite,
        )


# -----------------------------------------------------------------------------
# Per-Resource / Fault Retry Budget
# -----------------------------------------------------------------------------

@dataclass
class RetryBudgetResult:
    """Evaluation of retry budget availability and backoff calculation."""

    admitted: bool
    care_badge: str
    resource: str
    fault_class: FaultClass
    resource_retries: int
    fault_retries: int
    backoff_seconds: float
    choice_path: str
    detail: str
    smallest_prerequisite: Optional[SmallestMissingPrerequisite] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "admitted": self.admitted,
            "care_badge": self.care_badge,
            "resource": self.resource,
            "fault_class": self.fault_class.value,
            "resource_retries": self.resource_retries,
            "fault_retries": self.fault_retries,
            "backoff_seconds": self.backoff_seconds,
            "choice_path": self.choice_path,
            "detail": self.detail,
            "smallest_prerequisite": (
                self.smallest_prerequisite.to_dict()
                if self.smallest_prerequisite
                else None
            ),
        }


class RetryBudgetManager:
    """Manages per-resource and per-fault retry budgets with exponential backoff and jitter."""

    def __init__(self) -> None:
        self._resource_counts: Dict[str, int] = collections.defaultdict(int)
        self._fault_counts: Dict[str, int] = collections.defaultdict(int)

    def calculate_backoff(
        self,
        attempt: int,
        base_seconds: float = 1.0,
        max_seconds: float = 60.0,
        factor: float = 2.0,
    ) -> float:
        """Calculate bounded exponential backoff with deterministic jitter."""
        if attempt <= 0:
            return 0.0
        exp = min(max_seconds, base_seconds * (factor ** (attempt - 1)))
        # Deterministic fractional pseudo-jitter based on attempt
        jitter = (attempt * 17 % 10) / 100.0 * exp
        return min(max_seconds, exp + jitter)

    def evaluate_retry(
        self,
        resource: str,
        fault_class: Union[FaultClass, str],
        max_resource_retries: int = 3,
        max_fault_retries: int = 3,
        has_admissible_alternative: bool = False,
        work: str = "retry_budget_evaluation",
    ) -> RetryBudgetResult:
        res_key = str(resource)
        fault = fault_class if isinstance(fault_class, FaultClass) else FaultClass(fault_class)
        fault_key = fault.value

        current_res_retries = self._resource_counts[res_key]
        current_fault_retries = self._fault_counts[fault_key]

        prerequisite: Optional[SmallestMissingPrerequisite] = None
        if current_res_retries >= max_resource_retries or current_fault_retries >= max_fault_retries:
            prerequisite = SmallestMissingPrerequisite(
                category="exhausted_retry",
                resource=res_key,
                needed=f"retries < {max_resource_retries} (resource) and < {max_fault_retries} (fault)",
                current=f"retries={current_res_retries}/{max_resource_retries} (res), {current_fault_retries}/{max_fault_retries} (fault)",
                action_required="remediate root failure condition or switch to manual intervention ledger",
                care_badge=CARE_ACTION,
            )

        facts = {
            "resource_retries": current_res_retries,
            "max_resource_retries": max_resource_retries,
            "fault_retries": current_fault_retries,
            "max_fault_retries": max_fault_retries,
            "has_admissible_alternative": has_admissible_alternative,
            "terminal_prerequisite_formulated": prerequisite is not None,
        }

        choice = climb_retry_budget(work, facts)

        if choice.path == "retry_budget_available":
            self._resource_counts[res_key] += 1
            self._fault_counts[fault_key] += 1
            attempt_num = self._resource_counts[res_key]
            backoff = self.calculate_backoff(attempt_num)
            care_badge = CARE_BENIGN
            admitted = True
            detail = (
                f"retry admitted: attempt {attempt_num}/{max_resource_retries} for {res_key} "
                f"({fault.value}); backoff {backoff:.2f}s"
            )
        elif choice.path == "retry_budget_exhausted_alternative":
            backoff = 0.0
            care_badge = CARE_REVIEW
            admitted = True
            detail = (
                f"retry budget exhausted for {res_key}; switching to admissible "
                f"alternative execution path"
            )
        else:
            backoff = 0.0
            care_badge = CARE_ACTION
            admitted = False
            detail = (
                f"retry budget exhausted: resource {res_key} ({current_res_retries}/{max_resource_retries}) "
                f"and fault {fault.value} ({current_fault_retries}/{max_fault_retries}); infinite loop prevented"
            )

        return RetryBudgetResult(
            admitted=admitted,
            care_badge=care_badge,
            resource=res_key,
            fault_class=fault,
            resource_retries=current_res_retries,
            fault_retries=current_fault_retries,
            backoff_seconds=backoff,
            choice_path=choice.path or "none",
            detail=detail,
            smallest_prerequisite=prerequisite,
        )

    def reset(self, resource: Optional[str] = None) -> None:
        if resource:
            self._resource_counts.pop(str(resource), None)
        else:
            self._resource_counts.clear()
            self._fault_counts.clear()


# -----------------------------------------------------------------------------
# Typed Durable Checkpoint Store (O(1) Resume)
# -----------------------------------------------------------------------------

class ProgressLivenessCheckpointStore:
    """O(1) typed durable checkpoint store for certificates, rates, and retry states."""

    def __init__(self, directory: Union[str, Path]) -> None:
        self.directory = Path(directory)
        durable.private_dir(self.directory)

    def save_checkpoint(
        self,
        checkpoint_id: str,
        certificates: Sequence[ProgressCertificate],
        catch_up_result: Optional[ExtendedCatchUpResult] = None,
        deadlock_result: Optional[DeadlockEvaluationResult] = None,
        care_badge: str = CARE_PASS,
        extra_state: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Persist typed state checkpoint in O(1)."""
        target = self.directory / f"{checkpoint_id}.json"
        payload = {
            "checkpoint_id": str(checkpoint_id),
            "timestamp": time.time(),
            "care_badge": str(care_badge),
            "certificates": [c.to_dict() for c in certificates],
            "catch_up": catch_up_result.to_dict() if catch_up_result else None,
            "deadlock": deadlock_result.to_dict() if deadlock_result else None,
            "extra_state": extra_state or {},
        }
        durable.write_atomic(target, json.dumps(payload, indent=2))
        return target

    def load_checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """Load typed checkpoint in O(1)."""
        target = self.directory / f"{checkpoint_id}.json"
        if not target.exists():
            raise FileNotFoundError(f"checkpoint not found: {target}")
        data = json.loads(target.read_text(encoding="utf-8"))
        # Parse certificates back to typed objects
        if "certificates" in data:
            data["typed_certificates"] = [
                ProgressCertificate.from_dict(c) for c in data["certificates"]
            ]
        return data


# -----------------------------------------------------------------------------
# Autonomous Liveness Supervisor & ASCII Summary Renderer
# -----------------------------------------------------------------------------

@dataclass
class AutonomousOutcomeResult:
    """Overall autonomous progress and liveness outcome."""

    status: str
    care_badge: str
    all_advancing: bool
    rates_convergent: bool
    is_deadlocked: bool
    is_hung: bool
    drift_convergent: bool
    retry_admitted: bool
    choice_path: str
    detail: str
    prerequisites: List[SmallestMissingPrerequisite]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "care_badge": self.care_badge,
            "all_advancing": self.all_advancing,
            "rates_convergent": self.rates_convergent,
            "is_deadlocked": self.is_deadlocked,
            "is_hung": self.is_hung,
            "drift_convergent": self.drift_convergent,
            "retry_admitted": self.retry_admitted,
            "choice_path": self.choice_path,
            "detail": self.detail,
            "prerequisites": [p.to_dict() for p in self.prerequisites],
        }

    def render_report(self) -> str:
        """Render pure ASCII summary table."""
        lines = [
            "----------------------------------------------------------------------",
            "PROGRESS & LIVENESS SUPERVISION REPORT (Row 604: SMART-N21)",
            "----------------------------------------------------------------------",
            f"Overall Status   : {self.status.upper()}",
            f"Care Badge       : [{self.care_badge}]",
            f"Certificates Adv : {'YES' if self.all_advancing else 'NO'}",
            f"Rate Convergence : {'YES' if self.rates_convergent else 'NO'}",
            f"Deadlock Present : {'YES' if self.is_deadlocked else 'NO'}",
            f"Hung Worker      : {'YES' if self.is_hung else 'NO'}",
            f"Repair Drift Conv: {'YES' if self.drift_convergent else 'NO'}",
            f"Decision Choice  : {self.choice_path}",
            f"Detail           : {self.detail}",
            "----------------------------------------------------------------------",
        ]
        if self.prerequisites:
            lines.append("SMALLEST MISSING PREREQUISITES:")
            for p in self.prerequisites:
                lines.append(f"  [{p.care_badge}] {p.resource} ({p.category}):")
                lines.append(f"    Needed : {p.needed}")
                lines.append(f"    Current: {p.current}")
                lines.append(f"    Action : {p.action_required}")
            lines.append("----------------------------------------------------------------------")
        return "\n".join(lines)


class AutonomousLivenessSupervisor:
    """Supervises end-to-end progress liveness across all detection dimensions."""

    def __init__(self) -> None:
        self.deadlock_detector = DependencyDeadlockDetector()
        self.hung_watchdog = HungProgramWatchdog()
        self.drift_tracker = RepairDriftTracker()
        self.retry_manager = RetryBudgetManager()

    def supervise(
        self,
        task_certificates: Mapping[str, Sequence[ProgressCertificate]],
        rate_observations: Sequence[RateObservation],
        backlog_rows: float,
        repair_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        worker_heartbeat: Optional[float] = None,
        deadline_seconds: float = 3600.0,
        work: str = "autonomous_liveness_supervision",
    ) -> AutonomousOutcomeResult:
        """Run full autonomous progress and liveness supervision."""
        prerequisites: List[SmallestMissingPrerequisite] = []

        # 1. Rate Convergence
        catch_up = extended_catch_up_sum(
            rate_observations,
            backlog_rows=backlog_rows,
            deadline_seconds=deadline_seconds,
            work=f"{work}.rate",
        )
        if catch_up.smallest_prerequisite:
            prerequisites.append(catch_up.smallest_prerequisite)

        # 2. Deadlock Detection
        deadlock = self.deadlock_detector.evaluate_liveness(
            task_certificates,
            work=f"{work}.deadlock",
        )
        if deadlock.smallest_prerequisite:
            prerequisites.append(deadlock.smallest_prerequisite)

        # 3. Hung Program Check
        is_hung = False
        if worker_id and worker_heartbeat is not None:
            hung_res = self.hung_watchdog.evaluate_worker(
                worker_id,
                worker_heartbeat,
                work=f"{work}.hung",
            )
            is_hung = hung_res.is_hung
            if hung_res.smallest_prerequisite:
                prerequisites.append(hung_res.smallest_prerequisite)

        # 4. Repair Drift Check
        drift_conv = True
        if repair_id:
            drift_res = self.drift_tracker.evaluate_repair_drift(
                repair_id,
                work=f"{work}.drift",
            )
            drift_conv = drift_res.is_convergent
            if drift_res.smallest_prerequisite:
                prerequisites.append(drift_res.smallest_prerequisite)

        all_advancing = len(deadlock.advancing_tasks) > 0 and len(deadlock.stationary_tasks) == 0
        rates_convergent = catch_up.is_convergent
        zero_deadlocks_or_hung = not deadlock.is_deadlocked and not is_hung

        impossible = not rates_convergent or deadlock.is_deadlocked or is_hung or not drift_conv
        has_prereq = len(prerequisites) > 0

        facts = {
            "all_certificates_advancing": all_advancing,
            "rates_convergent": rates_convergent,
            "zero_deadlocks_or_hung": zero_deadlocks_or_hung,
            "liveness_impaired": impossible,
            "fallback_admissible": False,  # No fallback in default supervision
            "impossible_progress_proved": impossible,
            "smallest_prerequisite_isolated": has_prereq,
        }

        choice = climb_autonomous_outcome(work, facts)

        if choice.path == "autonomous_certify_progress":
            status = "healthy"
            care_badge = CARE_PASS
            detail = "healthy progress certified: rates convergent and certificates advancing"
        elif choice.path == "autonomous_replan_alternative":
            status = "replan"
            care_badge = CARE_REVIEW
            detail = "replan: alternative execution path admitted"
        else:
            status = "terminated"
            care_badge = CARE_ACTION
            detail = (
                f"impossible progress: execution isolated and stopped with "
                f"{len(prerequisites)} prerequisites"
            )

        return AutonomousOutcomeResult(
            status=status,
            care_badge=care_badge,
            all_advancing=all_advancing,
            rates_convergent=rates_convergent,
            is_deadlocked=deadlock.is_deadlocked,
            is_hung=is_hung,
            drift_convergent=drift_conv,
            retry_admitted=True,
            choice_path=choice.path or "none",
            detail=detail,
            prerequisites=prerequisites,
        )
