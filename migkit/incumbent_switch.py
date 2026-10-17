"""Include transition and rollback costs when replanning (Row 643: RCH35.incumbent-switch).

Extends replanning and mover/strategy switching (`decide`, `jointplan`, `costmodel`) with:
- Six-part transition and rollback cost pricing:
  1. Checkpoint granularity (`CheckpointGranularitySpec`): replay of uncommitted
     bytes/rows since the last durable cursor, chunk range, log position,
     partition epoch, or full-table boundary, or wait-to-seal latency.
  2. Spool drains (`SpoolDrainSpec`): draining or flushing in-flight buffered
     batches and spill segments before cutover to the challenger.
  3. Transaction barriers (`TransactionBarrierSpec`): waiting for open transactions,
     LSN/SCN fence round-trips, and quiesce lock waits within timeout bounds.
  4. Process setup (`ProcessSetupSpec`): cold-start process spawn, worker pool
     warm-up, connection handshakes, and snapshot export/import.
  5. Resource restore (`ResourceRestoreSpec`): truncating/rolling back uncommitted
     target staging state, resetting indexes/constraints, restoring session tuning,
     releasing slots/leases, and pricing expected rollback risk if the challenger
     fails during transition or execution.
  6. Proof regeneration (`ProofRegenerationSpec`): re-hashing invalidated bytes,
     rebuilding Merkle trees, and re-verifying range digests across changed boundaries.
- Validated incumbent protection and p90 remaining-work evaluation:
  Compares p90 completion time over remaining work (`remaining_bytes`,
  `remaining_rows`, `remaining_ranges`) rather than local instantaneous throughput.
- Hysteresis and anti-oscillation controller (`IncumbentSwitchController`):
  Requires sustained p90 advantage across `required_sustained_windows`, enforces
  minimum dwell windows and reversal penalties, and computes exact reproducible
  break-even thresholds (`BreakEvenAnalysis`).
- Decision engine integration (`SWITCH_LADDER` of `decide.Rung`s).
- O(1) typed durable checkpoint resume (`DurableSwitchCheckpoint`) and active
  verification lease lock (`ActiveVerificationLease`, `.run.lock`).
- Pure UNIX reporting with 4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
"""
from __future__ import annotations

import datetime
import decimal
import enum
import ipaddress
import json
import math
import os
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import costmodel, decide, jointplan


# ---------------------------------------------------------------------------
# Enums & Care Badges
# ---------------------------------------------------------------------------


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class CheckpointGranularity(str, enum.Enum):
    ROW_CURSOR = "ROW_CURSOR"
    CHUNK_RANGE = "CHUNK_RANGE"
    LOG_POSITION = "LOG_POSITION"
    PARTITION_EPOCH = "PARTITION_EPOCH"
    FULL_TABLE = "FULL_TABLE"


class SwitchVerdict(str, enum.Enum):
    RETAIN_INCUMBENT = "RETAIN_INCUMBENT"
    SWITCH_TO_CHALLENGER = "SWITCH_TO_CHALLENGER"
    DEFER_HYSTERESIS = "DEFER_HYSTERESIS"
    REFUSE_UNVALIDATED_CHALLENGER = "REFUSE_UNVALIDATED_CHALLENGER"
    REFUSE_NEGATIVE_P90_SAVINGS = "REFUSE_NEGATIVE_P90_SAVINGS"
    REFUSE_BARRIER_TIMEOUT = "REFUSE_BARRIER_TIMEOUT"
    REFUSE_OSCILLATION_GUARD = "REFUSE_OSCILLATION_GUARD"
    EMERGENCY_SWITCH_INCUMBENT_FAILED = "EMERGENCY_SWITCH_INCUMBENT_FAILED"
    ACTION_NO_VIABLE_PATH = "ACTION_NO_VIABLE_PATH"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class IncumbentSwitchError(Exception):
    """Base error for incumbent switch and transition pricing."""


class ActiveLeaseConflictError(IncumbentSwitchError):
    """Raised when an active .run.lock lease is already held by another run."""


class UnvalidatedIncumbentError(IncumbentSwitchError):
    """Raised when attempting to register an invalid incumbent configuration."""


class InvalidTransitionSpecError(IncumbentSwitchError):
    """Raised when transition specification parameters are negative or invalid."""


class UnsafeEndpointHostError(IncumbentSwitchError):
    """Raised when an endpoint host is outside approved safe test/private ranges."""


# ---------------------------------------------------------------------------
# Host Safety Validation
# ---------------------------------------------------------------------------

_SAFE_HOSTNAMES = {"localhost", "127.0.0.1", "::1"}


def validate_safe_host(host: str) -> str:
    """Ensure host is loopback or RFC1918 private (127.0.0.1, 10.0.0.x, etc.)."""
    cleaned = str(host or "").strip()
    if not cleaned:
        raise UnsafeEndpointHostError("Endpoint host must not be empty")
    if cleaned in _SAFE_HOSTNAMES:
        return cleaned
    try:
        addr = ipaddress.ip_address(cleaned)
    except ValueError as exc:
        raise UnsafeEndpointHostError(
            f"Host {cleaned!r} is not a literal loopback or private IP address"
        ) from exc
    if not (addr.is_loopback or addr.is_private):
        raise UnsafeEndpointHostError(
            f"Host {cleaned!r} is outside loopback/private network ranges"
        )
    return cleaned


# ---------------------------------------------------------------------------
# Decision Engine Rungs (SWITCH_LADDER)
# ---------------------------------------------------------------------------

SWITCH_LADDER = (
    decide.Rung(
        "retain_incumbent",
        "the validated incumbent kept as it runs",
        gives=frozenset({"replan_decision", "zero_transition_disruption"}),
        needs=(
            decide.Need(
                "a healthy incumbent",
                lambda f: bool(f.get("incumbent_healthy", True)),
                "the incumbent way failed or stalled",
            ),
            decide.Need(
                "no sustained break-even switch",
                lambda f: not bool(f.get("switch_warranted", False)),
                "a challenger crossed the sustained p90 break-even threshold after transition and rollback costs",
            ),
        ),
        because="keeping the validated incumbent avoids replay, drain, barrier and setup costs",
        footprint=0,
        leaves="nothing",
    ),
    decide.Rung(
        "checkpoint_fenced_switch",
        "a switch at the checkpoint fence after spool drain and barrier",
        gives=frozenset({"replan_decision", "switched_path"}),
        needs=(
            decide.Need(
                "a healthy incumbent",
                lambda f: bool(f.get("incumbent_healthy", True)),
                "the incumbent way failed or stalled and needs emergency recovery",
            ),
            decide.Need(
                "a validated challenger",
                lambda f: bool(f.get("challenger_validated", False)),
                "the challenger way has not been validated here",
            ),
            decide.Need(
                "positive p90 savings after transition and rollback",
                lambda f: bool(f.get("p90_savings_positive", False)),
                "transition and rollback costs outweigh p90 savings over remaining work",
            ),
            decide.Need(
                "sustained hysteresis window met",
                lambda f: bool(f.get("hysteresis_satisfied", False)),
                "the challenger advantage has not held across the hysteresis window",
            ),
            decide.Need(
                "transaction barrier within timeout",
                lambda f: bool(f.get("barrier_within_timeout", True)),
                "the transaction barrier exceeds the allowed timeout",
            ),
        ),
        because="sustained p90 savings over remaining work amortize transition and rollback costs",
        footprint=1,
        leaves="a fenced transition checkpoint",
    ),
    decide.Rung(
        "emergency_rollback_switch",
        "an emergency switch from a failed incumbent with target state restore",
        gives=frozenset({"replan_decision", "switched_path", "emergency_recovery"}),
        needs=(
            decide.Need(
                "an unhealthy incumbent",
                lambda f: not bool(f.get("incumbent_healthy", True)),
                "the incumbent is still healthy",
            ),
            decide.Need(
                "a viable fallback way",
                lambda f: bool(f.get("fallback_available", False)),
                "no validated fallback way is available",
            ),
        ),
        because="the incumbent failed and target state is restored to the last checkpoint for fallback",
        footprint=2,
        leaves="a rolled-back target range and replacement mover",
    ),
)


def choose_replan_rung(
    facts: Optional[ Union[Dict[str, Any], decide.Facts] ] = None,
    need: Tuple[str, ...] = (),
    costs: Any = None,
) -> decide.Choice:
    """Climb `SWITCH_LADDER` using `decide.climb`."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(facts or {})
    return decide.climb(
        "incumbent_switch",
        SWITCH_LADDER,
        facts=fact_obj,
        need=need,
        costs=costs,
        prove=True,
    )


# ---------------------------------------------------------------------------
# Transition & Rollback Pricing Specifications
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckpointGranularitySpec:
    """Prices uncommitted work replay or checkpoint boundary sealing on switch."""

    granularity: CheckpointGranularity = CheckpointGranularity.CHUNK_RANGE
    checkpoint_interval_bytes: float = 64 * 1024 * 1024
    uncommitted_bytes: float = 0.0
    uncommitted_rows: int = 0
    completed_table_bytes: float = 0.0
    checkpoint_commit_latency_sec: float = 0.25
    wait_for_next_checkpoint: bool = False

    def __post_init__(self) -> None:
        if self.checkpoint_interval_bytes < 0:
            raise InvalidTransitionSpecError("checkpoint_interval_bytes must be >= 0")
        if self.uncommitted_bytes < 0:
            raise InvalidTransitionSpecError("uncommitted_bytes must be >= 0")
        if self.completed_table_bytes < 0:
            raise InvalidTransitionSpecError("completed_table_bytes must be >= 0")
        if self.checkpoint_commit_latency_sec < 0:
            raise InvalidTransitionSpecError("checkpoint_commit_latency_sec must be >= 0")

    def effective_replay_bytes(self) -> float:
        """Bytes that must be replayed if switching without waiting for a fence."""
        if self.granularity == CheckpointGranularity.FULL_TABLE:
            return max(self.completed_table_bytes, self.uncommitted_bytes)
        if self.wait_for_next_checkpoint:
            return 0.0
        if self.uncommitted_bytes > 0:
            return float(self.uncommitted_bytes)
        if self.granularity == CheckpointGranularity.ROW_CURSOR:
            return 0.0
        return 0.5 * float(self.checkpoint_interval_bytes)

    def bytes_to_next_fence(self) -> float:
        """Remaining bytes in the current chunk/interval if waiting for the fence."""
        if not self.wait_for_next_checkpoint:
            return 0.0
        if self.granularity == CheckpointGranularity.FULL_TABLE:
            return 0.0
        if self.checkpoint_interval_bytes <= 0:
            return 0.0
        in_chunk = self.uncommitted_bytes % self.checkpoint_interval_bytes
        if in_chunk == 0 and self.uncommitted_bytes > 0:
            return 0.0
        return max(0.0, float(self.checkpoint_interval_bytes - in_chunk))


@dataclass(frozen=True)
class SpoolDrainSpec:
    """Prices draining or discarding in-flight buffered batches and spill files."""

    spooled_bytes: float = 0.0
    spooled_batches: int = 0
    drain_rate_bytes_per_sec: float = 64 * 1024 * 1024
    per_batch_ack_sec: float = 0.005
    flush_overhead_sec: float = 0.1
    discard_instead_of_drain: bool = False

    def __post_init__(self) -> None:
        if self.spooled_bytes < 0:
            raise InvalidTransitionSpecError("spooled_bytes must be >= 0")
        if self.drain_rate_bytes_per_sec <= 0:
            raise InvalidTransitionSpecError("drain_rate_bytes_per_sec must be > 0")
        if self.flush_overhead_sec < 0 or self.per_batch_ack_sec < 0:
            raise InvalidTransitionSpecError("spool overhead seconds must be >= 0")

    def drain_seconds(self) -> float:
        if self.spooled_bytes <= 0 and self.spooled_batches <= 0:
            return 0.0
        if self.discard_instead_of_drain:
            return float(self.flush_overhead_sec)
        return (
            float(self.spooled_bytes) / float(self.drain_rate_bytes_per_sec)
            + int(self.spooled_batches) * float(self.per_batch_ack_sec)
            + float(self.flush_overhead_sec)
        )

    def discarded_replay_bytes(self) -> float:
        if self.discard_instead_of_drain and self.spooled_bytes > 0:
            return float(self.spooled_bytes)
        return 0.0


@dataclass(frozen=True)
class TransactionBarrierSpec:
    """Prices transaction quiesce barriers, LSN/SCN alignment fences, and lock waits."""

    open_transactions: int = 0
    oldest_tx_remaining_sec: float = 0.0
    fence_round_trips: int = 1
    rtt_sec: float = 0.002
    quiesce_lock_wait_sec: float = 0.0
    barrier_timeout_sec: float = 60.0

    def __post_init__(self) -> None:
        if self.open_transactions < 0:
            raise InvalidTransitionSpecError("open_transactions must be >= 0")
        if self.oldest_tx_remaining_sec < 0 or self.quiesce_lock_wait_sec < 0:
            raise InvalidTransitionSpecError("barrier wait seconds must be >= 0")
        if self.barrier_timeout_sec <= 0:
            raise InvalidTransitionSpecError("barrier_timeout_sec must be > 0")

    def barrier_seconds(self) -> float:
        tx_wait = float(self.oldest_tx_remaining_sec) if self.open_transactions > 0 else 0.0
        fence_wait = max(0, int(self.fence_round_trips)) * float(self.rtt_sec)
        return tx_wait + fence_wait + float(self.quiesce_lock_wait_sec)

    def within_timeout(self) -> bool:
        return self.barrier_seconds() <= float(self.barrier_timeout_sec)


@dataclass(frozen=True)
class ProcessSetupSpec:
    """Prices challenger process startup, worker pool warm-up, and snapshot setup."""

    process_spawn_sec: float = 0.0
    worker_count: int = 1
    per_worker_warmup_sec: float = 0.0
    connection_handshake_sec: float = 0.0
    snapshot_export_import_sec: float = 0.0

    def __post_init__(self) -> None:
        if (
            self.process_spawn_sec < 0
            or self.per_worker_warmup_sec < 0
            or self.connection_handshake_sec < 0
            or self.snapshot_export_import_sec < 0
        ):
            raise InvalidTransitionSpecError("process setup seconds must be >= 0")
        if self.worker_count < 1:
            raise InvalidTransitionSpecError("worker_count must be >= 1")

    def setup_seconds(self) -> float:
        return (
            float(self.process_spawn_sec)
            + int(self.worker_count) * float(self.per_worker_warmup_sec)
            + float(self.connection_handshake_sec)
            + float(self.snapshot_export_import_sec)
        )


@dataclass(frozen=True)
class ResourceRestoreSpec:
    """Prices target cleanup, session/index restore, and rollback risk if challenger fails."""

    dirty_target_bytes: float = 0.0
    rollback_rate_bytes_per_sec: float = 128 * 1024 * 1024
    index_or_constraint_reset_sec: float = 0.0
    session_tuning_restore_sec: float = 0.0
    slot_or_lease_release_sec: float = 0.0
    challenger_failure_prob: float = 0.05
    rollback_to_incumbent_sec: float = 1.0

    def __post_init__(self) -> None:
        if self.dirty_target_bytes < 0:
            raise InvalidTransitionSpecError("dirty_target_bytes must be >= 0")
        if self.rollback_rate_bytes_per_sec <= 0:
            raise InvalidTransitionSpecError("rollback_rate_bytes_per_sec must be > 0")
        if not (0.0 <= self.challenger_failure_prob <= 1.0):
            raise InvalidTransitionSpecError("challenger_failure_prob must be in [0, 1]")
        if (
            self.index_or_constraint_reset_sec < 0
            or self.session_tuning_restore_sec < 0
            or self.slot_or_lease_release_sec < 0
            or self.rollback_to_incumbent_sec < 0
        ):
            raise InvalidTransitionSpecError("resource restore seconds must be >= 0")

    def immediate_restore_seconds(self) -> float:
        dirty_sec = (
            float(self.dirty_target_bytes) / float(self.rollback_rate_bytes_per_sec)
            if self.dirty_target_bytes > 0
            else 0.0
        )
        return (
            dirty_sec
            + float(self.index_or_constraint_reset_sec)
            + float(self.session_tuning_restore_sec)
            + float(self.slot_or_lease_release_sec)
        )


@dataclass(frozen=True)
class ProofRegenerationSpec:
    """Prices re-hashing invalidated bytes and rebuilding Merkle/digest proofs on switch."""

    invalidated_proof_bytes: float = 0.0
    invalidated_ranges: int = 0
    rehash_rate_bytes_per_sec: float = 200 * 1024 * 1024
    merkle_tree_rebuild_sec: float = 0.0
    per_range_verify_overhead_sec: float = 0.02

    def __post_init__(self) -> None:
        if self.invalidated_proof_bytes < 0 or self.invalidated_ranges < 0:
            raise InvalidTransitionSpecError("invalidated proof quantities must be >= 0")
        if self.rehash_rate_bytes_per_sec <= 0:
            raise InvalidTransitionSpecError("rehash_rate_bytes_per_sec must be > 0")
        if self.merkle_tree_rebuild_sec < 0 or self.per_range_verify_overhead_sec < 0:
            raise InvalidTransitionSpecError("proof regeneration seconds must be >= 0")

    def regeneration_seconds(self) -> float:
        rehash_sec = (
            float(self.invalidated_proof_bytes) / float(self.rehash_rate_bytes_per_sec)
            if self.invalidated_proof_bytes > 0
            else 0.0
        )
        return (
            rehash_sec
            + float(self.merkle_tree_rebuild_sec)
            + int(self.invalidated_ranges) * float(self.per_range_verify_overhead_sec)
        )


@dataclass(frozen=True)
class TransitionPlanSpec:
    """Aggregates all six transition and rollback cost specifications."""

    checkpoint: CheckpointGranularitySpec = field(default_factory=CheckpointGranularitySpec)
    spool: SpoolDrainSpec = field(default_factory=SpoolDrainSpec)
    barrier: TransactionBarrierSpec = field(default_factory=TransactionBarrierSpec)
    setup: ProcessSetupSpec = field(default_factory=ProcessSetupSpec)
    restore: ResourceRestoreSpec = field(default_factory=ResourceRestoreSpec)
    proof: ProofRegenerationSpec = field(default_factory=ProofRegenerationSpec)


# ---------------------------------------------------------------------------
# Path Throughput & Remaining Work Models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RemainingWorkSpec:
    """Describes the remaining migration work to be completed."""

    remaining_bytes: float
    remaining_rows: int = 0
    remaining_ranges: int = 1
    completed_bytes: float = 0.0
    lob_bytes: float = 0.0
    keyless: bool = False
    rtt_sec: float = 0.001
    server_load: float = 0.0

    def __post_init__(self) -> None:
        if self.remaining_bytes < 0:
            raise InvalidTransitionSpecError("remaining_bytes must be >= 0")
        if self.remaining_rows < 0 or self.remaining_ranges < 0:
            raise InvalidTransitionSpecError("remaining_rows and remaining_ranges must be >= 0")

    def as_costmodel_unit(self) -> Dict[str, Any]:
        return {
            "bytes": float(self.remaining_bytes),
            "rows": int(self.remaining_rows),
            "lob_bytes": float(self.lob_bytes),
            "keyless": bool(self.keyless),
            "rtt": float(self.rtt_sec),
            "load": float(self.server_load),
        }


@dataclass(frozen=True)
class PathPerformanceProfile:
    """Statistical throughput and reliability profile of an incumbent or challenger path.

    Evaluates p50 and p90 completion times over remaining work rather than
    relying on a single local instantaneous throughput reading.
    """

    name: str
    said: str = ""
    mean_bytes_per_sec: float = 50 * 1024 * 1024
    sd_bytes_per_sec: float = 5 * 1024 * 1024
    local_instantaneous_bytes_per_sec: Optional[float] = None
    sample_count: int = 5
    validated: bool = True
    healthy: bool = True
    startup_sec: float = 0.0
    per_1k_rows_rtt_mult: float = 0.0
    failure_prob: float = 0.02
    lost_fraction_on_fail: float = 0.05
    truth: str = costmodel.MEASURED_TRUTH

    def __post_init__(self) -> None:
        if self.mean_bytes_per_sec <= 0:
            raise InvalidTransitionSpecError(f"Path {self.name!r} mean_bytes_per_sec must be > 0")
        if self.sd_bytes_per_sec < 0:
            raise InvalidTransitionSpecError(f"Path {self.name!r} sd_bytes_per_sec must be >= 0")

    @classmethod
    def from_costmodel(
        cls,
        name: str,
        model: costmodel.CostModel,
        work: RemainingWorkSpec,
        said: str = "",
        validated: bool = True,
        healthy: bool = True,
        local_instantaneous_bytes_per_sec: Optional[float] = None,
    ) -> "PathPerformanceProfile":
        """Build a `PathPerformanceProfile` from `costmodel.CostModel` on `work`."""
        unit = work.as_costmodel_unit()
        est = model.estimate(name, unit)
        rem_bytes = max(float(work.remaining_bytes), 1.0)
        if est is None or est.mean <= 0:
            rate = 50 * 1024 * 1024
            sd_rate = 10 * 1024 * 1024
            samples = 0
            truth = costmodel.PREDICTED
        else:
            rate = rem_bytes / max(est.mean, 1e-6)
            rel_sd = est.sd / max(est.mean, 1e-6)
            sd_rate = rate * min(rel_sd, 0.75)
            samples = int(est.n)
            truth = est.truth
        fail_p = model.risk(name) if callable(getattr(model, "risk", None)) else 0.05
        return cls(
            name=name,
            said=said or name,
            mean_bytes_per_sec=rate,
            sd_bytes_per_sec=sd_rate,
            local_instantaneous_bytes_per_sec=local_instantaneous_bytes_per_sec,
            sample_count=samples,
            validated=validated and (truth == costmodel.MEASURED_TRUTH and samples > 0),
            healthy=healthy,
            failure_prob=fail_p,
            truth=truth,
        )

    @classmethod
    def from_jointplan_way(
        cls,
        way: jointplan.Way,
        sample_count: int = 5,
        validated: bool = True,
        healthy: bool = True,
        local_instantaneous_bytes_per_sec: Optional[float] = None,
    ) -> "PathPerformanceProfile":
        """Build a `PathPerformanceProfile` from a `jointplan.Way`."""
        rate = max(float(way.per_stream or 0.0), 1.0)
        sd_rate = rate * float(way.spread or 0.0)
        return cls(
            name=way.name,
            said=way.said or way.name,
            mean_bytes_per_sec=rate,
            sd_bytes_per_sec=sd_rate,
            local_instantaneous_bytes_per_sec=local_instantaneous_bytes_per_sec,
            sample_count=sample_count,
            validated=validated and (way.truth != costmodel.PREDICTED),
            healthy=healthy,
            startup_sec=float(way.startup or 0.0),
            failure_prob=float(way.risk or 0.0),
            lost_fraction_on_fail=float(way.lost or 0.5),
            truth=way.truth or costmodel.MEASURED_TRUTH,
        )

    def dof(self) -> float:
        return float(max(1, self.sample_count - 1)) if self.sample_count < 30 else math.inf

    def seconds_per_byte_p50(self) -> float:
        return 1.0 / float(self.mean_bytes_per_sec)

    def seconds_per_byte_p90(self) -> float:
        """Conservative p90 seconds per byte, widened by Student-t uncertainty and risk."""
        base_spb = 1.0 / float(self.mean_bytes_per_sec)
        rel_sd = float(self.sd_bytes_per_sec) / float(self.mean_bytes_per_sec)
        if self.truth == costmodel.PREDICTED:
            rel_sd = max(rel_sd, jointplan.MODEL_SPREAD)
        t_mult = costmodel._t_factor(self.dof())
        p90_spb = base_spb * (1.0 + jointplan.P90_SD * rel_sd * t_mult)
        risk_mult = 1.0 + float(self.failure_prob) * float(self.lost_fraction_on_fail)
        return p90_spb * risk_mult

    def p90_bytes_per_sec(self) -> float:
        return 1.0 / self.seconds_per_byte_p90()

    def estimate_remaining(self, work: RemainingWorkSpec) -> costmodel.Estimate:
        """Return a `costmodel.Estimate` for completing `work` on this path."""
        rem_bytes = float(work.remaining_bytes)
        if rem_bytes <= 0:
            return costmodel.Estimate(0.0, 0.0, self.truth, self.name, float(self.sample_count), self.dof())
        row_trips_sec = (
            (float(work.remaining_rows) / 1000.0)
            * float(work.rtt_sec)
            * float(self.per_1k_rows_rtt_mult)
        )
        mean_sec = (
            float(self.startup_sec)
            + rem_bytes * self.seconds_per_byte_p50()
            + row_trips_sec
        )
        risk_mean = costmodel.risked(mean_sec, self.failure_prob, self.lost_fraction_on_fail)
        rel_sd = float(self.sd_bytes_per_sec) / float(self.mean_bytes_per_sec)
        if self.truth == costmodel.PREDICTED:
            rel_sd = max(rel_sd, jointplan.MODEL_SPREAD)
        sd_sec = risk_mean * rel_sd
        return costmodel.Estimate(
            mean=risk_mean,
            sd=sd_sec,
            truth=self.truth,
            source=self.name,
            n=float(self.sample_count),
            dof=self.dof(),
            noise=rel_sd,
        )


# ---------------------------------------------------------------------------
# Transition & Rollback Cost Calculation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TransitionCostBreakdown:
    """Detailed six-part transition and rollback cost breakdown in seconds and bytes."""

    checkpoint_replay_bytes: float
    checkpoint_fence_wait_bytes: float
    spool_discard_replay_bytes: float
    total_replay_bytes: float
    checkpoint_cost_sec: float
    spool_drain_cost_sec: float
    transaction_barrier_cost_sec: float
    process_setup_cost_sec: float
    resource_restore_cost_sec: float
    proof_regeneration_cost_sec: float
    direct_transition_cost_sec: float
    rollback_risk_cost_sec: float
    total_transition_and_rollback_p90_sec: float
    barrier_within_timeout: bool

    def as_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_replay_bytes": self.checkpoint_replay_bytes,
            "checkpoint_fence_wait_bytes": self.checkpoint_fence_wait_bytes,
            "spool_discard_replay_bytes": self.spool_discard_replay_bytes,
            "total_replay_bytes": self.total_replay_bytes,
            "checkpoint_cost_sec": round(self.checkpoint_cost_sec, 6),
            "spool_drain_cost_sec": round(self.spool_drain_cost_sec, 6),
            "transaction_barrier_cost_sec": round(self.transaction_barrier_cost_sec, 6),
            "process_setup_cost_sec": round(self.process_setup_cost_sec, 6),
            "resource_restore_cost_sec": round(self.resource_restore_cost_sec, 6),
            "proof_regeneration_cost_sec": round(self.proof_regeneration_cost_sec, 6),
            "direct_transition_cost_sec": round(self.direct_transition_cost_sec, 6),
            "rollback_risk_cost_sec": round(self.rollback_risk_cost_sec, 6),
            "total_transition_and_rollback_p90_sec": round(
                self.total_transition_and_rollback_p90_sec, 6
            ),
            "barrier_within_timeout": self.barrier_within_timeout,
        }


def price_transition_and_rollback(
    incumbent: PathPerformanceProfile,
    challenger: PathPerformanceProfile,
    transition: TransitionPlanSpec,
    work: Optional[RemainingWorkSpec] = None,
) -> TransitionCostBreakdown:
    """Price all 6 transition components plus expected rollback risk at p90."""
    ckpt = transition.checkpoint
    spool = transition.spool
    barrier = transition.barrier
    setup = transition.setup
    restore = transition.restore
    proof = transition.proof

    ckpt_replay_bytes = ckpt.effective_replay_bytes()
    fence_wait_bytes = ckpt.bytes_to_next_fence()
    spool_replay_bytes = spool.discarded_replay_bytes()
    total_replay_bytes = ckpt_replay_bytes + spool_replay_bytes

    inc_spb_p90 = incumbent.seconds_per_byte_p90()
    chal_spb_p90 = challenger.seconds_per_byte_p90()

    # 1. Checkpoint granularity cost:
    # Either wait for incumbent to finish current chunk to fence + commit,
    # or abort current chunk and replay uncommitted bytes on the challenger.
    fence_wait_sec = fence_wait_bytes * inc_spb_p90
    replay_sec = total_replay_bytes * chal_spb_p90
    ckpt_commit_sec = float(ckpt.checkpoint_commit_latency_sec)
    checkpoint_cost_sec = fence_wait_sec + replay_sec + ckpt_commit_sec

    # 2. Spool drain cost:
    spool_drain_cost_sec = spool.drain_seconds()

    # 3. Transaction barrier cost:
    transaction_barrier_cost_sec = barrier.barrier_seconds()
    within_timeout = barrier.within_timeout()

    # 4. Process setup cost:
    process_setup_cost_sec = setup.setup_seconds() + float(challenger.startup_sec)

    # 5. Resource restore cost (immediate cleanup of dirty target state / session tuning):
    resource_restore_cost_sec = restore.immediate_restore_seconds()

    # 6. Proof regeneration cost (re-hashing invalidated bytes + Merkle tree rebuild):
    proof_regeneration_cost_sec = proof.regeneration_seconds()

    direct_transition_sec = (
        checkpoint_cost_sec
        + spool_drain_cost_sec
        + transaction_barrier_cost_sec
        + process_setup_cost_sec
        + resource_restore_cost_sec
        + proof_regeneration_cost_sec
    )

    # Rollback risk pricing: if the challenger fails during switch or run,
    # migkit must roll back dirty state, re-establish the incumbent, and redo
    # lost work on the incumbent.
    fail_p = max(float(restore.challenger_failure_prob), float(challenger.failure_prob))
    rem_bytes = float(work.remaining_bytes) if work is not None else 0.0
    lost_work_bytes = rem_bytes * float(challenger.lost_fraction_on_fail)
    wasted_challenger_sec = lost_work_bytes * chal_spb_p90
    rollback_penalty_if_failed = (
        float(restore.rollback_to_incumbent_sec)
        + resource_restore_cost_sec
        + direct_transition_sec
        + wasted_challenger_sec
    )
    rollback_risk_cost_sec = fail_p * rollback_penalty_if_failed

    total_p90_sec = direct_transition_sec + rollback_risk_cost_sec

    return TransitionCostBreakdown(
        checkpoint_replay_bytes=ckpt_replay_bytes,
        checkpoint_fence_wait_bytes=fence_wait_bytes,
        spool_discard_replay_bytes=spool_replay_bytes,
        total_replay_bytes=total_replay_bytes,
        checkpoint_cost_sec=checkpoint_cost_sec,
        spool_drain_cost_sec=spool_drain_cost_sec,
        transaction_barrier_cost_sec=transaction_barrier_cost_sec,
        process_setup_cost_sec=process_setup_cost_sec,
        resource_restore_cost_sec=resource_restore_cost_sec,
        proof_regeneration_cost_sec=proof_regeneration_cost_sec,
        direct_transition_cost_sec=direct_transition_sec,
        rollback_risk_cost_sec=rollback_risk_cost_sec,
        total_transition_and_rollback_p90_sec=total_p90_sec,
        barrier_within_timeout=within_timeout,
    )


# ---------------------------------------------------------------------------
# Hysteresis Policy & Reproducible Break-Even Analysis
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HysteresisPolicy:
    """Controls hysteresis margin, confirmation window, dwell time, and anti-oscillation."""

    min_relative_p90_savings: float = 0.10
    min_absolute_p90_savings_sec: float = 5.0
    required_sustained_windows: int = 3
    min_dwell_windows: int = 2
    reversal_penalty_multiplier: float = 1.5
    require_validated_challenger: bool = True
    min_challenger_samples: int = 2

    def __post_init__(self) -> None:
        if not (0.0 <= self.min_relative_p90_savings < 1.0):
            raise InvalidTransitionSpecError("min_relative_p90_savings must be in [0, 1)")
        if self.min_absolute_p90_savings_sec < 0:
            raise InvalidTransitionSpecError("min_absolute_p90_savings_sec must be >= 0")
        if self.required_sustained_windows < 1:
            raise InvalidTransitionSpecError("required_sustained_windows must be >= 1")
        if self.min_dwell_windows < 0:
            raise InvalidTransitionSpecError("min_dwell_windows must be >= 0")
        if self.reversal_penalty_multiplier < 1.0:
            raise InvalidTransitionSpecError("reversal_penalty_multiplier must be >= 1.0")


@dataclass(frozen=True)
class BreakEvenAnalysis:
    """Exact reproducible break-even analysis for switching from incumbent to challenger."""

    can_break_even: bool
    break_even_remaining_bytes: float
    break_even_challenger_p90_bps: float
    incumbent_p90_sec_per_byte: float
    challenger_p90_sec_per_byte: float
    transition_and_rollback_p90_sec: float
    effective_relative_margin: float
    effective_absolute_margin_sec: float
    margin_surplus_bytes: float


def compute_break_even(
    incumbent: PathPerformanceProfile,
    challenger: PathPerformanceProfile,
    transition: TransitionPlanSpec,
    work: RemainingWorkSpec,
    policy: HysteresisPolicy = HysteresisPolicy(),
    is_reversal: bool = False,
) -> BreakEvenAnalysis:
    """Compute the exact reproducible break-even remaining bytes and challenger rate."""
    penalty = policy.reversal_penalty_multiplier if is_reversal else 1.0
    eff_rel = min(0.95, policy.min_relative_p90_savings * penalty)
    eff_abs = policy.min_absolute_p90_savings_sec * penalty

    # Compute fixed/replay transition cost at zero remaining risk-work first
    zero_work = RemainingWorkSpec(
        remaining_bytes=0.0,
        remaining_rows=0,
        remaining_ranges=1,
    )
    base_breakdown = price_transition_and_rollback(
        incumbent, challenger, transition, zero_work
    )
    c_fixed = base_breakdown.total_transition_and_rollback_p90_sec

    s_inc = incumbent.seconds_per_byte_p90()
    s_chal_base = challenger.seconds_per_byte_p90()
    fail_p = max(
        float(transition.restore.challenger_failure_prob),
        float(challenger.failure_prob),
    )
    # Per-byte rollback risk contribution when remaining_bytes > 0
    s_chal_eff = s_chal_base * (1.0 + fail_p * float(challenger.lost_fraction_on_fail))

    # Break-even remaining bytes B* must satisfy both:
    # 1) Relative margin: (1 - eff_rel) * B * s_inc >= B * s_chal_eff + c_fixed
    # 2) Absolute margin: B * s_inc - (B * s_chal_eff + c_fixed) >= eff_abs
    denom_rel = (1.0 - eff_rel) * s_inc - s_chal_eff
    denom_abs = s_inc - s_chal_eff

    if denom_rel <= 0 or denom_abs <= 0:
        can_be = False
        be_bytes = math.inf
    else:
        can_be = True
        b_rel = c_fixed / denom_rel
        b_abs = (c_fixed + eff_abs) / denom_abs
        be_bytes = max(0.0, b_rel, b_abs)

    # Required challenger p90 bytes/s to break even at the current `work.remaining_bytes`
    rem = float(work.remaining_bytes)
    if rem <= 0:
        be_rate = math.inf
    else:
        inc_total_p90 = rem * s_inc
        max_allowed_switch_total = min(
            inc_total_p90 * (1.0 - eff_rel),
            inc_total_p90 - eff_abs,
        )
        # Solve for challenger p90 spb where total_switch <= max_allowed_switch_total
        # Note that replay bytes also run at challenger spb (times 1 + fail_p):
        replay_b = (
            transition.checkpoint.effective_replay_bytes()
            + transition.spool.discarded_replay_bytes()
        )
        non_chal_fixed = (
            transition.checkpoint.bytes_to_next_fence() * s_inc
            + float(transition.checkpoint.checkpoint_commit_latency_sec)
            + transition.spool.drain_seconds()
            + transition.barrier.barrier_seconds()
            + float(transition.setup.setup_seconds())
            + float(challenger.startup_sec)
            + transition.restore.immediate_restore_seconds()
            + transition.proof.regeneration_seconds()
        )
        non_chal_with_rollback = non_chal_fixed * (1.0 + fail_p) + fail_p * (
            float(transition.restore.rollback_to_incumbent_sec)
            + transition.restore.immediate_restore_seconds()
        )
        budget_for_chal = max_allowed_switch_total - non_chal_with_rollback
        effective_chal_bytes = (
            rem * (1.0 + fail_p * float(challenger.lost_fraction_on_fail))
            + replay_b * (1.0 + fail_p)
        )
        if budget_for_chal <= 0 or effective_chal_bytes <= 0:
            be_rate = math.inf
        else:
            max_spb = budget_for_chal / effective_chal_bytes
            be_rate = 1.0 / max_spb if max_spb > 0 else math.inf

    surplus = float(work.remaining_bytes) - be_bytes if can_be else -math.inf
    return BreakEvenAnalysis(
        can_break_even=can_be,
        break_even_remaining_bytes=be_bytes,
        break_even_challenger_p90_bps=be_rate,
        incumbent_p90_sec_per_byte=s_inc,
        challenger_p90_sec_per_byte=s_chal_eff,
        transition_and_rollback_p90_sec=c_fixed,
        effective_relative_margin=eff_rel,
        effective_absolute_margin_sec=eff_abs,
        margin_surplus_bytes=surplus,
    )


# ---------------------------------------------------------------------------
# Switch Evaluation Result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SwitchEvaluation:
    """Outcome of one replanning evaluation comparing incumbent vs challenger."""

    incumbent_name: str
    challenger_name: str
    active_path_after: str
    switched: bool
    verdict: SwitchVerdict
    badge: CareBadge
    incumbent_remaining_p50_sec: float
    incumbent_remaining_p90_sec: float
    challenger_exec_only_p90_sec: float
    transition_breakdown: TransitionCostBreakdown
    challenger_total_p90_sec: float
    net_p90_savings_sec: float
    relative_p90_savings: float
    local_instantaneous_speedup_ratio: float
    break_even: BreakEvenAnalysis
    sustained_window_count: int
    required_sustained_windows: int
    dwell_windows_elapsed: int
    decide_rung_name: str
    reason: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "incumbent_name": self.incumbent_name,
            "challenger_name": self.challenger_name,
            "active_path_after": self.active_path_after,
            "switched": self.switched,
            "verdict": self.verdict.value,
            "badge": self.badge.value,
            "incumbent_remaining_p50_sec": round(self.incumbent_remaining_p50_sec, 4),
            "incumbent_remaining_p90_sec": round(self.incumbent_remaining_p90_sec, 4),
            "challenger_exec_only_p90_sec": round(self.challenger_exec_only_p90_sec, 4),
            "transition_breakdown": self.transition_breakdown.as_dict(),
            "challenger_total_p90_sec": round(self.challenger_total_p90_sec, 4),
            "net_p90_savings_sec": round(self.net_p90_savings_sec, 4),
            "relative_p90_savings": round(self.relative_p90_savings, 6),
            "local_instantaneous_speedup_ratio": round(
                self.local_instantaneous_speedup_ratio, 4
            ),
            "break_even_remaining_bytes": (
                None
                if math.isinf(self.break_even.break_even_remaining_bytes)
                else round(self.break_even.break_even_remaining_bytes, 2)
            ),
            "can_break_even": self.break_even.can_break_even,
            "sustained_window_count": self.sustained_window_count,
            "required_sustained_windows": self.required_sustained_windows,
            "dwell_windows_elapsed": self.dwell_windows_elapsed,
            "decide_rung_name": self.decide_rung_name,
            "reason": self.reason,
        }


# ---------------------------------------------------------------------------
# Mutual Exclusion & Active Verification Lease Lock (.run.lock)
# ---------------------------------------------------------------------------


class ActiveVerificationLease:
    """Mutual exclusion .run.lock with heartbeat preventing concurrent replan runs."""

    LOCK_FILENAME = ".run.lock"

    def __init__(
        self,
        directory: Union[str, Path],
        owner_id: Optional[str] = None,
        ttl_seconds: float = 60.0,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self.directory = Path(directory)
        self.lock_path = self.directory / self.LOCK_FILENAME
        self.owner_id = owner_id or f"pid-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.ttl_seconds = float(ttl_seconds)
        self._time_fn = time_fn
        self._held = False

    def acquire(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        now = self._time_fn()
        if self.lock_path.exists():
            try:
                existing = json.loads(self.lock_path.read_text(encoding="utf-8"))
                heartbeat = float(existing.get("heartbeat_ts", 0.0))
                holder = existing.get("owner_id", "")
                if holder != self.owner_id and (now - heartbeat) < self.ttl_seconds:
                    raise ActiveLeaseConflictError(
                        f"Active lease lock {self.lock_path} held by {holder!r}"
                    )
            except (ValueError, OSError):
                pass
        self._write_lease(now)
        self._held = True

    def heartbeat(self) -> None:
        if not self._held:
            raise ActiveLeaseConflictError("Cannot heartbeat an unacquired lease")
        if self.lock_path.exists():
            try:
                existing = json.loads(self.lock_path.read_text(encoding="utf-8"))
                if existing.get("owner_id") != self.owner_id:
                    self._held = False
                    raise ActiveLeaseConflictError(
                        f"Lease {self.lock_path} was stolen by {existing.get('owner_id')!r}"
                    )
            except ValueError:
                pass
        self._write_lease(self._time_fn())

    def release(self) -> None:
        if not self._held:
            return
        try:
            if self.lock_path.exists():
                existing = json.loads(self.lock_path.read_text(encoding="utf-8"))
                if existing.get("owner_id") == self.owner_id:
                    self.lock_path.unlink()
        except (ValueError, OSError):
            pass
        finally:
            self._held = False

    def _write_lease(self, ts: float) -> None:
        payload = {
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "heartbeat_ts": ts,
            "ttl_seconds": self.ttl_seconds,
        }
        fd, tmp_path = tempfile.mkstemp(dir=str(self.directory), prefix=".lock_tmp_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, str(self.lock_path))
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    def __enter__(self) -> "ActiveVerificationLease":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# ---------------------------------------------------------------------------
# Universal Typed Durable Resume in O(1)
# ---------------------------------------------------------------------------


def _encode_typed(value: Any) -> Any:
    """Recursively preserve native types (datetime, date, Decimal, UUID, tuple)."""
    if isinstance(value, datetime.datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"__type__": "date", "value": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"__type__": "Decimal", "value": str(value)}
    if isinstance(value, uuid.UUID):
        return {"__type__": "UUID", "value": str(value)}
    if isinstance(value, tuple):
        return {"__type__": "tuple", "items": [_encode_typed(v) for v in value]}
    if isinstance(value, list):
        return [_encode_typed(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _encode_typed(v) for k, v in value.items()}
    return value


def _decode_typed(value: Any) -> Any:
    """Recursively restore native types from JSON-decoded structure."""
    if isinstance(value, list):
        return [_decode_typed(v) for v in value]
    if isinstance(value, dict):
        kind = value.get("__type__")
        if kind == "datetime":
            return datetime.datetime.fromisoformat(value["value"])
        if kind == "date":
            return datetime.date.fromisoformat(value["value"])
        if kind == "Decimal":
            return decimal.Decimal(value["value"])
        if kind == "UUID":
            return uuid.UUID(value["value"])
        if kind == "tuple":
            return tuple(_decode_typed(v) for v in value["items"])
        return {k: _decode_typed(v) for k, v in value.items()}
    return value


class DurableSwitchCheckpoint:
    """O(1) typed durable checkpoint for range/cursor progress and switch state."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        self.completed_units: Set[str] = set()
        self.unit_cursors: Dict[str, Any] = {}
        self.incumbent_name: Optional[str] = None
        self.dwell_windows: int = 0
        self.challenger_streaks: Dict[str, int] = {}
        self.abandoned_paths: Set[str] = set()
        self.switch_history: List[Dict[str, Any]] = []
        if self.path.exists():
            self.load()

    def is_completed(self, unit_id: str) -> bool:
        """O(1) membership test to skip already-completed range/cursor units."""
        return str(unit_id) in self.completed_units

    def mark_unit_completed(self, unit_id: str, cursor_key: Any = None) -> None:
        uid = str(unit_id)
        self.completed_units.add(uid)
        if cursor_key is not None:
            self.unit_cursors[uid] = cursor_key

    def get_cursor(self, unit_id: str) -> Any:
        return self.unit_cursors.get(str(unit_id))

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "completed_units": sorted(self.completed_units),
            "unit_cursors": _encode_typed(self.unit_cursors),
            "incumbent_name": self.incumbent_name,
            "dwell_windows": self.dwell_windows,
            "challenger_streaks": dict(self.challenger_streaks),
            "abandoned_paths": sorted(self.abandoned_paths),
            "switch_history": _encode_typed(self.switch_history),
        }
        fd, tmp_path = tempfile.mkstemp(dir=str(self.path.parent), prefix=".ckpt_tmp_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, str(self.path))
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    def load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        self.completed_units = set(raw.get("completed_units", []))
        self.unit_cursors = _decode_typed(raw.get("unit_cursors", {}))
        self.incumbent_name = raw.get("incumbent_name")
        self.dwell_windows = int(raw.get("dwell_windows", 0))
        self.challenger_streaks = {
            str(k): int(v) for k, v in (raw.get("challenger_streaks") or {}).items()
        }
        self.abandoned_paths = set(raw.get("abandoned_paths", []))
        self.switch_history = _decode_typed(raw.get("switch_history", []))


# ---------------------------------------------------------------------------
# Incumbent Switch Controller (Hysteresis + p90 Remaining-Work Replanner)
# ---------------------------------------------------------------------------


class IncumbentSwitchController:
    """Stateful replanning controller with hysteresis, validated incumbent, and transition pricing."""

    def __init__(
        self,
        incumbent: PathPerformanceProfile,
        policy: HysteresisPolicy = HysteresisPolicy(),
        checkpoint: Optional[DurableSwitchCheckpoint] = None,
        endpoint_host: str = "127.0.0.1",
    ) -> None:
        validate_safe_host(endpoint_host)
        if not incumbent.validated and incumbent.healthy:
            raise UnvalidatedIncumbentError(
                f"Initial incumbent {incumbent.name!r} must be validated"
            )
        self.endpoint_host = endpoint_host
        self.incumbent = incumbent
        self.policy = policy
        self.checkpoint = checkpoint
        self.dwell_windows: int = policy.min_dwell_windows
        self.challenger_streaks: Dict[str, int] = {}
        self.abandoned_paths: Set[str] = set()
        self.switch_history: List[SwitchEvaluation] = []

        if self.checkpoint is not None:
            if self.checkpoint.incumbent_name:
                self.dwell_windows = self.checkpoint.dwell_windows
                self.challenger_streaks = dict(self.checkpoint.challenger_streaks)
                self.abandoned_paths = set(self.checkpoint.abandoned_paths)
            else:
                self._sync_checkpoint()

    def _sync_checkpoint(self) -> None:
        if self.checkpoint is None:
            return
        self.checkpoint.incumbent_name = self.incumbent.name
        self.checkpoint.dwell_windows = self.dwell_windows
        self.checkpoint.challenger_streaks = dict(self.challenger_streaks)
        self.checkpoint.abandoned_paths = set(self.abandoned_paths)
        self.checkpoint.save()

    def evaluate(
        self,
        challenger: PathPerformanceProfile,
        work: RemainingWorkSpec,
        transition: TransitionPlanSpec = TransitionPlanSpec(),
    ) -> SwitchEvaluation:
        """Assess challenger vs validated incumbent over remaining work at p90."""
        inc = self.incumbent
        inc_est = inc.estimate_remaining(work)
        inc_p50 = inc_est.mean
        inc_p90 = float(work.remaining_bytes) * inc.seconds_per_byte_p90() + float(inc.startup_sec)

        breakdown = price_transition_and_rollback(inc, challenger, transition, work)
        chal_exec_p90 = float(work.remaining_bytes) * challenger.seconds_per_byte_p90()
        chal_total_p90 = chal_exec_p90 + breakdown.total_transition_and_rollback_p90_sec

        net_savings_p90 = inc_p90 - chal_total_p90
        rel_savings_p90 = net_savings_p90 / inc_p90 if inc_p90 > 0 else 0.0

        inst_chal = challenger.local_instantaneous_bytes_per_sec or challenger.mean_bytes_per_sec
        inst_inc = inc.local_instantaneous_bytes_per_sec or inc.mean_bytes_per_sec
        local_speedup = float(inst_chal) / max(float(inst_inc), 1e-9)

        is_reversal = challenger.name in self.abandoned_paths
        be = compute_break_even(
            inc, challenger, transition, work, self.policy, is_reversal=is_reversal
        )

        # Case 1: Incumbent has failed/stalled -> Emergency switch if fallback is viable
        if not inc.healthy:
            fallback_ok = (
                challenger.healthy
                and challenger.validated
                and breakdown.barrier_within_timeout
            )
            facts = decide.Facts(
                {
                    "incumbent_healthy": False,
                    "switch_warranted": True,
                    "challenger_validated": challenger.validated,
                    "p90_savings_positive": True,
                    "hysteresis_satisfied": True,
                    "barrier_within_timeout": breakdown.barrier_within_timeout,
                    "fallback_available": fallback_ok,
                }
            )
            choice = choose_replan_rung(facts)
            if fallback_ok and choice.rung is not None:
                self.abandoned_paths.add(inc.name)
                self.incumbent = challenger
                self.dwell_windows = 0
                self.challenger_streaks.clear()
                ev = SwitchEvaluation(
                    incumbent_name=inc.name,
                    challenger_name=challenger.name,
                    active_path_after=challenger.name,
                    switched=True,
                    verdict=SwitchVerdict.EMERGENCY_SWITCH_INCUMBENT_FAILED,
                    badge=CareBadge.REVIEW,
                    incumbent_remaining_p50_sec=inc_p50,
                    incumbent_remaining_p90_sec=inc_p90,
                    challenger_exec_only_p90_sec=chal_exec_p90,
                    transition_breakdown=breakdown,
                    challenger_total_p90_sec=chal_total_p90,
                    net_p90_savings_sec=net_savings_p90,
                    relative_p90_savings=rel_savings_p90,
                    local_instantaneous_speedup_ratio=local_speedup,
                    break_even=be,
                    sustained_window_count=1,
                    required_sustained_windows=1,
                    dwell_windows_elapsed=self.dwell_windows,
                    decide_rung_name=choice.rung.name,
                    reason=(
                        f"Incumbent {inc.name} unhealthy; emergency switch to validated "
                        f"fallback {challenger.name} after target restore"
                    ),
                )
                self._record_eval(ev)
                return ev

            ev = SwitchEvaluation(
                incumbent_name=inc.name,
                challenger_name=challenger.name,
                active_path_after=inc.name,
                switched=False,
                verdict=SwitchVerdict.ACTION_NO_VIABLE_PATH,
                badge=CareBadge.ACTION,
                incumbent_remaining_p50_sec=inc_p50,
                incumbent_remaining_p90_sec=inc_p90,
                challenger_exec_only_p90_sec=chal_exec_p90,
                transition_breakdown=breakdown,
                challenger_total_p90_sec=chal_total_p90,
                net_p90_savings_sec=net_savings_p90,
                relative_p90_savings=rel_savings_p90,
                local_instantaneous_speedup_ratio=local_speedup,
                break_even=be,
                sustained_window_count=0,
                required_sustained_windows=self.policy.required_sustained_windows,
                dwell_windows_elapsed=self.dwell_windows,
                decide_rung_name=choice.path or "none",
                reason=(
                    f"Incumbent {inc.name} failed and challenger {challenger.name} "
                    f"is not a viable validated fallback ({choice.reason})"
                ),
            )
            self._record_eval(ev)
            return ev

        # Advance dwell window on healthy incumbent
        current_dwell = self.dwell_windows
        self.dwell_windows += 1

        # Check challenger validation & sample sufficiency
        challenger_valid = (
            challenger.healthy
            and (challenger.validated or not self.policy.require_validated_challenger)
            and challenger.sample_count >= self.policy.min_challenger_samples
        )
        if not challenger_valid:
            self.challenger_streaks[challenger.name] = 0
            choice = choose_replan_rung(
                {
                    "incumbent_healthy": True,
                    "switch_warranted": False,
                    "challenger_validated": False,
                    "p90_savings_positive": False,
                    "hysteresis_satisfied": False,
                    "barrier_within_timeout": breakdown.barrier_within_timeout,
                    "fallback_available": False,
                }
            )
            ev = SwitchEvaluation(
                incumbent_name=inc.name,
                challenger_name=challenger.name,
                active_path_after=inc.name,
                switched=False,
                verdict=SwitchVerdict.REFUSE_UNVALIDATED_CHALLENGER,
                badge=CareBadge.BENIGN,
                incumbent_remaining_p50_sec=inc_p50,
                incumbent_remaining_p90_sec=inc_p90,
                challenger_exec_only_p90_sec=chal_exec_p90,
                transition_breakdown=breakdown,
                challenger_total_p90_sec=chal_total_p90,
                net_p90_savings_sec=net_savings_p90,
                relative_p90_savings=rel_savings_p90,
                local_instantaneous_speedup_ratio=local_speedup,
                break_even=be,
                sustained_window_count=0,
                required_sustained_windows=self.policy.required_sustained_windows,
                dwell_windows_elapsed=current_dwell,
                decide_rung_name=choice.rung.name if choice.rung else "retain_incumbent",
                reason=(
                    f"Refused unvalidated or under-sampled challenger {challenger.name} "
                    f"(validated={challenger.validated}, samples={challenger.sample_count})"
                ),
            )
            self._record_eval(ev)
            return ev

        # Check transaction barrier timeout
        if not breakdown.barrier_within_timeout:
            self.challenger_streaks[challenger.name] = 0
            choice = choose_replan_rung(
                {
                    "incumbent_healthy": True,
                    "switch_warranted": False,
                    "challenger_validated": True,
                    "p90_savings_positive": net_savings_p90 > 0,
                    "hysteresis_satisfied": False,
                    "barrier_within_timeout": False,
                    "fallback_available": False,
                }
            )
            ev = SwitchEvaluation(
                incumbent_name=inc.name,
                challenger_name=challenger.name,
                active_path_after=inc.name,
                switched=False,
                verdict=SwitchVerdict.REFUSE_BARRIER_TIMEOUT,
                badge=CareBadge.REVIEW,
                incumbent_remaining_p50_sec=inc_p50,
                incumbent_remaining_p90_sec=inc_p90,
                challenger_exec_only_p90_sec=chal_exec_p90,
                transition_breakdown=breakdown,
                challenger_total_p90_sec=chal_total_p90,
                net_p90_savings_sec=net_savings_p90,
                relative_p90_savings=rel_savings_p90,
                local_instantaneous_speedup_ratio=local_speedup,
                break_even=be,
                sustained_window_count=0,
                required_sustained_windows=self.policy.required_sustained_windows,
                dwell_windows_elapsed=current_dwell,
                decide_rung_name=choice.rung.name if choice.rung else "retain_incumbent",
                reason=(
                    f"Transaction barrier {breakdown.transaction_barrier_cost_sec:.2f}s "
                    f"exceeds timeout {transition.barrier.barrier_timeout_sec:.2f}s"
                ),
            )
            self._record_eval(ev)
            return ev

        # Check whether p90 savings cross the reproducible break-even margin
        crosses_margin = (
            be.can_break_even
            and float(work.remaining_bytes) >= be.break_even_remaining_bytes
            and net_savings_p90 >= be.effective_absolute_margin_sec
            and rel_savings_p90 >= be.effective_relative_margin
        )

        if not crosses_margin:
            # Reset challenger streak when it fails to beat the break-even threshold
            self.challenger_streaks[challenger.name] = 0
            choice = choose_replan_rung(
                {
                    "incumbent_healthy": True,
                    "switch_warranted": False,
                    "challenger_validated": True,
                    "p90_savings_positive": False,
                    "hysteresis_satisfied": False,
                    "barrier_within_timeout": True,
                    "fallback_available": True,
                }
            )
            verdict = (
                SwitchVerdict.REFUSE_OSCILLATION_GUARD
                if is_reversal and net_savings_p90 > 0
                else SwitchVerdict.REFUSE_NEGATIVE_P90_SAVINGS
            )
            ev = SwitchEvaluation(
                incumbent_name=inc.name,
                challenger_name=challenger.name,
                active_path_after=inc.name,
                switched=False,
                verdict=verdict,
                badge=CareBadge.PASS if net_savings_p90 <= 0 else CareBadge.BENIGN,
                incumbent_remaining_p50_sec=inc_p50,
                incumbent_remaining_p90_sec=inc_p90,
                challenger_exec_only_p90_sec=chal_exec_p90,
                transition_breakdown=breakdown,
                challenger_total_p90_sec=chal_total_p90,
                net_p90_savings_sec=net_savings_p90,
                relative_p90_savings=rel_savings_p90,
                local_instantaneous_speedup_ratio=local_speedup,
                break_even=be,
                sustained_window_count=0,
                required_sustained_windows=self.policy.required_sustained_windows,
                dwell_windows_elapsed=current_dwell,
                decide_rung_name=choice.rung.name if choice.rung else "retain_incumbent",
                reason=(
                    f"Retaining incumbent {inc.name}: p90 transition+rollback cost "
                    f"({breakdown.total_transition_and_rollback_p90_sec:.2f}s) leaves "
                    f"net p90 savings {net_savings_p90:.2f}s ({rel_savings_p90 * 100:.1f}%) "
                    f"below break-even threshold"
                ),
            )
            self._record_eval(ev)
            return ev

        # Challenger crossed the p90 break-even threshold on this observation
        streak = self.challenger_streaks.get(challenger.name, 0) + 1
        self.challenger_streaks[challenger.name] = streak

        dwell_ok = current_dwell >= self.policy.min_dwell_windows
        sustained_ok = streak >= self.policy.required_sustained_windows
        hysteresis_ok = dwell_ok and sustained_ok

        choice = choose_replan_rung(
            {
                "incumbent_healthy": True,
                "switch_warranted": hysteresis_ok,
                "challenger_validated": True,
                "p90_savings_positive": True,
                "hysteresis_satisfied": hysteresis_ok,
                "barrier_within_timeout": True,
                "fallback_available": True,
            }
        )

        if not hysteresis_ok:
            why_defer = (
                f"dwell {current_dwell}/{self.policy.min_dwell_windows}"
                if not dwell_ok
                else f"sustained {streak}/{self.policy.required_sustained_windows}"
            )
            ev = SwitchEvaluation(
                incumbent_name=inc.name,
                challenger_name=challenger.name,
                active_path_after=inc.name,
                switched=False,
                verdict=SwitchVerdict.DEFER_HYSTERESIS,
                badge=CareBadge.BENIGN,
                incumbent_remaining_p50_sec=inc_p50,
                incumbent_remaining_p90_sec=inc_p90,
                challenger_exec_only_p90_sec=chal_exec_p90,
                transition_breakdown=breakdown,
                challenger_total_p90_sec=chal_total_p90,
                net_p90_savings_sec=net_savings_p90,
                relative_p90_savings=rel_savings_p90,
                local_instantaneous_speedup_ratio=local_speedup,
                break_even=be,
                sustained_window_count=streak,
                required_sustained_windows=self.policy.required_sustained_windows,
                dwell_windows_elapsed=current_dwell,
                decide_rung_name=choice.rung.name if choice.rung else "retain_incumbent",
                reason=(
                    f"Challenger {challenger.name} crosses p90 break-even "
                    f"(net savings {net_savings_p90:.2f}s), awaiting hysteresis ({why_defer})"
                ),
            )
            self._record_eval(ev)
            return ev

        # Sustained break-even confirmed -> execute fenced switch
        self.abandoned_paths.add(inc.name)
        self.incumbent = challenger
        self.dwell_windows = 0
        self.challenger_streaks.clear()

        ev = SwitchEvaluation(
            incumbent_name=inc.name,
            challenger_name=challenger.name,
            active_path_after=challenger.name,
            switched=True,
            verdict=SwitchVerdict.SWITCH_TO_CHALLENGER,
            badge=CareBadge.PASS,
            incumbent_remaining_p50_sec=inc_p50,
            incumbent_remaining_p90_sec=inc_p90,
            challenger_exec_only_p90_sec=chal_exec_p90,
            transition_breakdown=breakdown,
            challenger_total_p90_sec=chal_total_p90,
            net_p90_savings_sec=net_savings_p90,
            relative_p90_savings=rel_savings_p90,
            local_instantaneous_speedup_ratio=local_speedup,
            break_even=be,
            sustained_window_count=streak,
            required_sustained_windows=self.policy.required_sustained_windows,
            dwell_windows_elapsed=current_dwell,
            decide_rung_name=choice.rung.name if choice.rung else "checkpoint_fenced_switch",
            reason=(
                f"Switched from {inc.name} to {challenger.name}: sustained p90 net savings "
                f"{net_savings_p90:.2f}s ({rel_savings_p90 * 100:.1f}%) across "
                f"{streak} windows after {breakdown.total_transition_and_rollback_p90_sec:.2f}s "
                f"transition+rollback cost"
            ),
        )
        self._record_eval(ev)
        return ev

    def _record_eval(self, ev: SwitchEvaluation) -> None:
        if ev.switched:
            self.switch_history.append(ev)
            if self.checkpoint is not None:
                self.checkpoint.switch_history.append(ev.as_dict())
        self._sync_checkpoint()


# ---------------------------------------------------------------------------
# Pure UNIX Text Formatting
# ---------------------------------------------------------------------------


def format_switch_report(evaluation: SwitchEvaluation) -> str:
    """Render a terse, pure-ASCII UNIX summary with 4-tier care badges."""
    bd = evaluation.transition_breakdown
    be_str = (
        "INF"
        if math.isinf(evaluation.break_even.break_even_remaining_bytes)
        else f"{evaluation.break_even.break_even_remaining_bytes:.0f}B"
    )
    lines = [
        f"[{evaluation.badge.value}] replan_verdict={evaluation.verdict.value} "
        f"incumbent={evaluation.incumbent_name} challenger={evaluation.challenger_name} "
        f"active={evaluation.active_path_after} switched={int(evaluation.switched)} "
        f"rung={evaluation.decide_rung_name}",
        f"  p90_seconds: incumbent_rem={evaluation.incumbent_remaining_p90_sec:.2f}s "
        f"challenger_exec={evaluation.challenger_exec_only_p90_sec:.2f}s "
        f"transition_rollback={bd.total_transition_and_rollback_p90_sec:.2f}s "
        f"challenger_total={evaluation.challenger_total_p90_sec:.2f}s "
        f"net_savings={evaluation.net_p90_savings_sec:.2f}s "
        f"({evaluation.relative_p90_savings * 100:.1f}%)",
        f"  transition_components: ckpt={bd.checkpoint_cost_sec:.2f}s "
        f"spool={bd.spool_drain_cost_sec:.2f}s "
        f"barrier={bd.transaction_barrier_cost_sec:.2f}s "
        f"setup={bd.process_setup_cost_sec:.2f}s "
        f"restore={bd.resource_restore_cost_sec:.2f}s "
        f"proof={bd.proof_regeneration_cost_sec:.2f}s "
        f"rollback_risk={bd.rollback_risk_cost_sec:.2f}s "
        f"replay_bytes={bd.total_replay_bytes:.0f}B",
        f"  hysteresis: window={evaluation.sustained_window_count}/"
        f"{evaluation.required_sustained_windows} "
        f"dwell={evaluation.dwell_windows_elapsed} "
        f"break_even_bytes={be_str} "
        f"local_inst_speedup={evaluation.local_instantaneous_speedup_ratio:.2f}x",
        f"  reason: {evaluation.reason}",
    ]
    return "\n".join(lines)
