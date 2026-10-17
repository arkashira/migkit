"""Compile intent into one checked execution contract (Row 584: SMART-N1.contract).

Extends existing guarantees, need, and admission using `decide.Rung` and
`decide.climb(work, rungs, facts)`:

1. Intent Compilation (`compile_execution_contract`):
   - Compiles user goal and scope into a single immutable, cryptographically bound
     `CompiledExecutionContract` specifying:
     * Required proof coverage (`proof_coverage` and `guarantees.LEVELS` vector)
     * Snapshot / fence / ordering (`snapshot_mode`, `fence_mode`, `ordering_mode`)
     * Idempotence and delivery (`idempotent`, `exactly once`)
     * Restartability (`resumable`, durable unit cursor requirements)
     * Source impact bounds (CPU fraction, connections, IOPS, read bytes/sec derived
       from measured baseline + spread readings)
     * Outage window bounds (max cutover downtime seconds derived from measured drain
       and switch latency + spread)
     * Byte and money bounds (independent ceilings derived from measured dataset
       bytes, amplification, and transfer pricing)
     * Version and identity bindings (`facts_version`, `model_version`,
       `plan_version`, and `TargetIdentity` fingerprint)
   - Enforces componentwise minimum guarantees before lexicographic ranking
     (`guarantees.ORDER` = `("correct", "consistent", "idempotent", "exactly once", "resumable")`).
   - Detects and refuses incompatible minimum requirements before ranking.

2. Contract Propagation Across Paths & Lifecycle Phases (`evaluate_path_under_contract`,
   `evaluate_fallback_under_contract`, `verify_resume_under_contract`):
   - Carries the compiled contract through `bulk`, `table`, `range`, and `native`
     execution paths.
   - Ensures runtime fallback and adversarial faster alternatives cannot drop any
     required guarantee dimension or exceed source-impact, outage, byte, or money bounds.
   - Enforces durable resume identity so a restart cannot reuse a checkpoint from an
     incompatible scope, target identity, or weaker guarantee contract.

3. Independent Pre-Boundary Admission Gate (`admit_irreversible_boundary`):
   - Runs independent admission checks via `decide.climb` before irreversible
     boundaries (`cutover`, `rollback`, `target_exposure`, `source_log_release`,
     `destructive_target_replace`).
   - Verifies target identity, bound fact/model/plan versions, proof coverage
     completion, fence/ordering invariants, and runtime byte/money/outage/source-impact
     consumption against the contract.
   - Introduces no new CLI modes and never assumes implicit authorization for
     source mutation or destructive actions.

Pure UNIX style only. Zero emojis or decorative glyphs.
4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import math
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from . import decide, durable, guarantees, lease

# -----------------------------------------------------------------------------
# Care Badges and Core Enumerations
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


class MigrationGoalKind(str, enum.Enum):
    """High-level intent modes compiled into execution contracts."""

    ZERO_LOSS_ONLINE_CUTOVER = "zero_loss_online_cutover"
    CONSISTENT_SNAPSHOT_SEED = "consistent_snapshot_seed"
    BOUNDED_WINDOW_MAINTENANCE = "bounded_window_maintenance"
    KEYLESS_EXACT_MULTISET = "keyless_exact_multiset"


class ExecutionPathClass(str, enum.Enum):
    """Execution paths through which the compiled contract is carried."""

    BULK = "bulk"
    TABLE = "table"
    RANGE = "range"
    NATIVE = "native"


class LifecycleBoundary(str, enum.Enum):
    """Execution phases and irreversible boundaries governed by the contract."""

    BULK_LOAD = "bulk_load"
    TABLE_COPY = "table_copy"
    RANGE_COPY = "range_copy"
    NATIVE_TRANSFER = "native_transfer"
    RUNTIME_FALLBACK = "runtime_fallback"
    RESUME = "resume"
    CUTOVER = "cutover"
    ROLLBACK = "rollback"


class IrreversibleBoundaryKind(str, enum.Enum):
    """Irreversible boundaries requiring independent admission checks."""

    CUTOVER_COMMIT = "cutover"
    ROLLBACK_SWITCH = "rollback"
    TARGET_EXPOSURE = "target_exposure"
    SOURCE_LOG_RELEASE = "source_log_release"


class ProofCoverageLevel(str, enum.Enum):
    """Required verification coverage depth compiled from intent and scope."""

    UNPROVED = "unproved"
    ROW_COUNTED = "counted"
    ROW_DIGEST_PROVED = "proved"
    BYTE_EXACT_AND_CATALOG = "byte exact"


class SnapshotFenceOrderingMode(str, enum.Enum):
    """Snapshot, write-fence, and commit-ordering invariant mode."""

    STRICT_EPOCH_FENCE_COMMIT_ORDER = "strict_epoch_fence_commit_order"
    SHARED_SNAPSHOT_KEY_ORDER = "shared_snapshot_key_order"
    RANGE_FENCED_CHUNK_ORDER = "range_fenced_chunk_order"
    MULTISET_EPOCH_LOG_ORDER = "multiset_epoch_log_order"


class BoundaryAdmissionVerdict(str, enum.Enum):
    """Verdict of independent admission before an irreversible boundary."""

    REFUSE_CONTRACT_BREACH = "refuse_contract_breach"
    HOLD_FOR_REVALIDATION = "hold_for_revalidation"
    ADMIT_IRREVERSIBLE_BOUNDARY = "admit_irreversible_boundary"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class ExecutionContractError(Exception):
    """Base error for execution contract compilation, propagation, and admission."""


class IncompatibleMinimumGuaranteesError(ExecutionContractError):
    """Raised when explicit minimum requirements contradict each other or scope capabilities."""


class ContractDimensionDroppedError(ExecutionContractError):
    """Raised when a path, fallback, or resume attempts to drop a required contract dimension."""


class IrreversibleBoundaryRefusedError(ExecutionContractError):
    """Raised when an irreversible boundary fails independent admission checks."""


class UnsafeEndpointError(ExecutionContractError):
    """Raised when an endpoint host is outside loopback or RFC1918 10.0.0.x ranges."""


def validate_safe_endpoint(host: str) -> str:
    """Validate that endpoint host is loopback (127.0.0.1, localhost) or 10.0.0.x."""
    cleaned = (host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host must not be empty")
    if cleaned in ("127.0.0.1", "localhost") or cleaned.startswith("10.0.0."):
        return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x permitted"
    )


# -----------------------------------------------------------------------------
# Measured Telemetry Readings & Bound Derivation (No Hardcoded Magic Numbers)
# -----------------------------------------------------------------------------

def _mean_and_spread(samples: Sequence[float], field_name: str) -> Tuple[float, float]:
    """Compute arithmetic mean and sample standard deviation from measured samples."""
    if not samples:
        raise ExecutionContractError(f"measured samples for '{field_name}' must not be empty")
    vals = [float(x) for x in samples]
    if any(math.isnan(v) or math.isinf(v) or v < 0.0 for v in vals):
        raise ExecutionContractError(
            f"measured samples for '{field_name}' must be finite non-negative numbers"
        )
    n = len(vals)
    mean = sum(vals) / float(n)
    if n < 2:
        return mean, 0.0
    variance = sum((v - mean) ** 2 for v in vals) / float(n - 1)
    return mean, math.sqrt(max(0.0, variance))


@dataclass(frozen=True)
class ContractTelemetryReadings:
    """Measured source, network, and target telemetry used to derive contract bounds."""

    dataset_bytes_samples: Tuple[int, ...]
    wire_amplification_samples: Tuple[float, ...]
    price_per_gib_usd_samples: Tuple[float, ...]
    source_cpu_headroom_ratio_samples: Tuple[float, ...]
    source_iops_headroom_samples: Tuple[float, ...]
    source_read_bytes_per_sec_samples: Tuple[float, ...]
    source_max_connections: int
    source_reserved_system_connections: int
    cutover_drain_sec_samples: Tuple[float, ...]
    cutover_switch_sec_samples: Tuple[float, ...]

    def __post_init__(self) -> None:
        _mean_and_spread(self.dataset_bytes_samples, "dataset_bytes_samples")
        _mean_and_spread(self.wire_amplification_samples, "wire_amplification_samples")
        _mean_and_spread(self.price_per_gib_usd_samples, "price_per_gib_usd_samples")
        _mean_and_spread(
            self.source_cpu_headroom_ratio_samples, "source_cpu_headroom_ratio_samples"
        )
        _mean_and_spread(self.source_iops_headroom_samples, "source_iops_headroom_samples")
        _mean_and_spread(
            self.source_read_bytes_per_sec_samples, "source_read_bytes_per_sec_samples"
        )
        _mean_and_spread(self.cutover_drain_sec_samples, "cutover_drain_sec_samples")
        _mean_and_spread(self.cutover_switch_sec_samples, "cutover_switch_sec_samples")
        if self.source_max_connections <= self.source_reserved_system_connections:
            raise ExecutionContractError(
                "source_max_connections must exceed source_reserved_system_connections"
            )


@dataclass(frozen=True)
class SourceImpactBounds:
    """Bounded source load envelope derived from measured headroom."""

    max_cpu_ratio: float
    max_iops: float
    max_read_bytes_per_sec: int
    max_worker_connections: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_cpu_ratio": round(self.max_cpu_ratio, 6),
            "max_iops": round(self.max_iops, 3),
            "max_read_bytes_per_sec": int(self.max_read_bytes_per_sec),
            "max_worker_connections": int(self.max_worker_connections),
        }


@dataclass(frozen=True)
class OutageByteMoneyBounds:
    """Compiled outage, billed-byte, and monetary cost bounds."""

    max_outage_seconds: float
    max_billed_bytes: int
    max_money_usd: float
    estimated_outage_seconds: float
    estimated_billed_bytes: int
    estimated_money_usd: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_outage_seconds": round(self.max_outage_seconds, 6),
            "max_billed_bytes": int(self.max_billed_bytes),
            "max_money_usd": round(self.max_money_usd, 6),
            "estimated_outage_seconds": round(self.estimated_outage_seconds, 6),
            "estimated_billed_bytes": int(self.estimated_billed_bytes),
            "estimated_money_usd": round(self.estimated_money_usd, 6),
        }


def derive_bounds_from_readings(
    readings: ContractTelemetryReadings,
    *,
    operator_max_outage_seconds: Optional[float] = None,
    operator_max_billed_bytes: Optional[int] = None,
    operator_max_money_usd: Optional[float] = None,
    operator_max_cpu_ratio: Optional[float] = None,
    operator_max_connections: Optional[int] = None,
) -> Tuple[SourceImpactBounds, OutageByteMoneyBounds]:
    """Derive source-impact, outage, byte, and money bounds from measured readings + spread.

    Conservative envelopes use `mean - std` for available source capacity and
    `mean + std` for expected byte volume, monetary cost, and cutover outage duration.
    When an operator supplies an explicit ceiling, the effective bound is the tighter
    of the operator ceiling and the measured envelope.
    """
    bytes_mean, bytes_std = _mean_and_spread(
        readings.dataset_bytes_samples, "dataset_bytes_samples"
    )
    amp_mean, amp_std = _mean_and_spread(
        readings.wire_amplification_samples, "wire_amplification_samples"
    )
    price_mean, price_std = _mean_and_spread(
        readings.price_per_gib_usd_samples, "price_per_gib_usd_samples"
    )
    cpu_mean, cpu_std = _mean_and_spread(
        readings.source_cpu_headroom_ratio_samples, "source_cpu_headroom_ratio_samples"
    )
    iops_mean, iops_std = _mean_and_spread(
        readings.source_iops_headroom_samples, "source_iops_headroom_samples"
    )
    read_bps_mean, read_bps_std = _mean_and_spread(
        readings.source_read_bytes_per_sec_samples, "source_read_bytes_per_sec_samples"
    )
    drain_mean, drain_std = _mean_and_spread(
        readings.cutover_drain_sec_samples, "cutover_drain_sec_samples"
    )
    switch_mean, switch_std = _mean_and_spread(
        readings.cutover_switch_sec_samples, "cutover_switch_sec_samples"
    )

    conservative_cpu = max(0.01, min(1.0, cpu_mean - cpu_std))
    if operator_max_cpu_ratio is not None:
        if operator_max_cpu_ratio <= 0.0 or operator_max_cpu_ratio > 1.0:
            raise ExecutionContractError("operator_max_cpu_ratio must be in (0.0, 1.0]")
        conservative_cpu = min(conservative_cpu, float(operator_max_cpu_ratio))

    conservative_iops = max(1.0, iops_mean - iops_std)
    conservative_read_bps = max(1, int(math.floor(read_bps_mean - read_bps_std)))

    avail_conns = max(
        1, readings.source_max_connections - readings.source_reserved_system_connections
    )
    if operator_max_connections is not None:
        if operator_max_connections < 1:
            raise ExecutionContractError("operator_max_connections must be >= 1")
        avail_conns = min(avail_conns, int(operator_max_connections))

    source_bounds = SourceImpactBounds(
        max_cpu_ratio=conservative_cpu,
        max_iops=conservative_iops,
        max_read_bytes_per_sec=conservative_read_bps,
        max_worker_connections=avail_conns,
    )

    est_bytes = int(math.ceil((bytes_mean + bytes_std) * (amp_mean + amp_std)))
    bytes_Per_gib = float(1024 ** 3)
    est_gib = float(est_bytes) / bytes_Per_gib
    est_money = est_gib * (price_mean + price_std)
    est_outage = (drain_mean + drain_std) + (switch_mean + switch_std)

    max_bytes = (
        int(operator_max_billed_bytes)
        if operator_max_billed_bytes is not None
        else est_bytes
    )
    max_money = (
        float(operator_max_money_usd)
        if operator_max_money_usd is not None
        else est_money
    )
    max_outage = (
        float(operator_max_outage_seconds)
        if operator_max_outage_seconds is not None
        else est_outage
    )

    resource_bounds = OutageByteMoneyBounds(
        max_outage_seconds=max_outage,
        max_billed_bytes=max_bytes,
        max_money_usd=max_money,
        estimated_outage_seconds=est_outage,
        estimated_billed_bytes=est_bytes,
        estimated_money_usd=est_money,
    )
    return source_bounds, resource_bounds


# -----------------------------------------------------------------------------
# Version & Target Identity Bindings
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class TargetIdentity:
    """Cryptographically bound target identity preventing misrouted execution."""

    engine: str
    host: str
    port: int
    database: str
    cluster_uid: str
    schema_fingerprint: str

    def __post_init__(self) -> None:
        validate_safe_endpoint(self.host)
        if not self.engine or not self.database or not self.cluster_uid:
            raise ExecutionContractError(
                "TargetIdentity requires non-empty engine, database, and cluster_uid"
            )
        if self.port <= 0:
            raise ExecutionContractError("TargetIdentity port must be positive")

    @property
    def identity_digest(self) -> str:
        payload = (
            f"{self.engine}|{self.host}:{self.port}|{self.database}"
            f"|{self.cluster_uid}|{self.schema_fingerprint}"
        )
        return hashlib.sha256(payload.encode("ascii")).hexdigest()[:24]

    def matches(self, other: TargetIdentity) -> bool:
        return self.identity_digest == other.identity_digest

    def to_dict(self) -> Dict[str, Any]:
        return {
            "engine": self.engine,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "cluster_uid": self.cluster_uid,
            "schema_fingerprint": self.schema_fingerprint,
            "identity_digest": self.identity_digest,
        }


@dataclass(frozen=True)
class VersionBinding:
    """Bound versions of facts, cost/risk model, and execution plan."""

    facts_version: str
    model_version: str
    plan_version: str
    source_epoch: int

    def __post_init__(self) -> None:
        if not self.facts_version or not self.model_version or not self.plan_version:
            raise ExecutionContractError(
                "VersionBinding requires non-empty facts_version, model_version, and plan_version"
            )
        if self.source_epoch < 0:
            raise ExecutionContractError("source_epoch must be non-negative")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "facts_version": self.facts_version,
            "model_version": self.model_version,
            "plan_version": self.plan_version,
            "source_epoch": self.source_epoch,
        }


# -----------------------------------------------------------------------------
# Guarantee Algebra Extension: Componentwise Minimums & Lexicographic Ranking
# -----------------------------------------------------------------------------

def parse_guarantee_minimums(
    minimums: Optional[Mapping[str, Union[str, int]]] = None,
) -> Dict[str, int]:
    """Normalize a mapping of guarantee dimension -> level name or rank into `{dim: rank}`.

    Uses `guarantees.ORDER` (`correct`, `consistent`, `idempotent`, `exactly once`,
    `resumable`) and `guarantees.LEVELS`.
    """
    out: Dict[str, int] = {dim: 0 for dim in guarantees.ORDER}
    if not minimums:
        return out
    for dim, val in minimums.items():
        if dim not in guarantees.LEVELS:
            raise IncompatibleMinimumGuaranteesError(
                f"unknown guarantee dimension {dim!r}; valid dimensions: {guarantees.ORDER}"
            )
        if isinstance(val, int):
            if val < 0 or val >= len(guarantees.LEVELS[dim]):
                raise IncompatibleMinimumGuaranteesError(
                    f"invalid rank {val} for dimension {dim!r}"
                )
            out[dim] = val
        else:
            try:
                out[dim] = guarantees.level(dim, str(val))
            except ValueError as exc:
                raise IncompatibleMinimumGuaranteesError(str(exc)) from exc
    return out


def validate_compatible_minimums(
    minimums: Mapping[str, Union[str, int]],
    scope_facts: Mapping[str, Any],
) -> Tuple[Dict[str, int], Tuple[str, ...]]:
    """Verify whether explicit minimum guarantees are mutually compatible and achievable.

    Returns `(normalized_ranks, contradictions)`. Any non-empty `contradictions`
    tuple indicates an incompatible minimum requirement that must be refused.
    """
    ranks = parse_guarantee_minimums(minimums)
    contradictions: List[str] = []

    # 1. Exactly-once delivery requires idempotent convergence or atomic effect journal
    if (
        ranks["exactly once"] >= guarantees.level("exactly once", "exactly once")
        and ranks["idempotent"] < guarantees.level("idempotent", "converges")
        and bool(scope_facts.get("forbid_idempotent_convergence", False))
    ):
        contradictions.append(
            "minimum 'exactly once' conflicts with forbidden idempotent convergence"
        )

    # 2. Consistent 'as of one moment' conflicts with non-snapshot read when source takes live writes
    if (
        ranks["consistent"] >= guarantees.level("consistent", "as of one moment")
        and not bool(scope_facts.get("supports_point_in_time_snapshot", True))
        and not bool(scope_facts.get("source_writes_frozen", False))
        and not bool(scope_facts.get("supports_cdc_catchup_fence", False))
    ):
        contradictions.append(
            "minimum 'as of one moment' consistency is unachievable without point-in-time"
            " snapshot, CDC catch-up fence, or frozen source writes"
        )

    # 3. Byte-exact proof conflicts with lossy/unapproved semantic truncation
    if (
        ranks["correct"] >= guarantees.level("correct", "byte exact")
        and bool(scope_facts.get("lossy_type_mapping_present", False))
        and not bool(scope_facts.get("approved_lossy_mapping_rule", False))
    ):
        contradictions.append(
            "minimum 'byte exact' correctness conflicts with unapproved lossy type mapping"
        )

    # 4. Resumable 'goes on' conflicts with non-checkpointable single-shot stream without cursor
    if (
        ranks["resumable"] >= guarantees.level("resumable", "goes on")
        and not bool(scope_facts.get("supports_durable_checkpoint_cursor", True))
    ):
        contradictions.append(
            "minimum 'goes on' restartability is unachievable when durable checkpoint"
            " cursors are unsupported"
        )

    # 5. Explicit capability cap on the pair
    max_reachable = scope_facts.get("pair_max_guarantees")
    if isinstance(max_reachable, Mapping):
        max_ranks = parse_guarantee_minimums(max_reachable)
        for dim in guarantees.ORDER:
            if dim in max_reachable and ranks[dim] > max_ranks[dim]:
                req_name = guarantees.LEVELS[dim][ranks[dim]]
                cap_name = guarantees.LEVELS[dim][max_ranks[dim]]
                contradictions.append(
                    f"minimum {dim}={req_name!r} exceeds pair capability ceiling {cap_name!r}"
                )

    return ranks, tuple(contradictions)


def holds_componentwise_minimums(
    candidate_levels: Mapping[str, int],
    minimum_levels: Mapping[str, int],
) -> Tuple[bool, Tuple[str, ...]]:
    """Check whether `candidate_levels` meets or exceeds `minimum_levels` on every dimension."""
    deficits: List[str] = []
    for dim in guarantees.ORDER:
        got = int(candidate_levels.get(dim, 0))
        req = int(minimum_levels.get(dim, 0))
        if got < req:
            got_name = guarantees.LEVELS[dim][got]
            req_name = guarantees.LEVELS[dim][req]
            deficits.append(
                f"{dim}: holds {got_name!r} < required minimum {req_name!r}"
            )
    return len(deficits) == 0, tuple(deficits)


def filter_and_rank_by_guarantees(
    rungs: Sequence[decide.Rung],
    minimum_levels: Mapping[str, int],
) -> Tuple[Tuple[decide.Rung, ...], Tuple[Tuple[decide.Rung, str], ...]]:
    """Enforce componentwise minimums first, then lexicographic guarantee ordering.

    1. Any rung whose declared/derived guarantee vector falls below `minimum_levels`
       on ANY dimension in `guarantees.ORDER` is rejected with its exact deficit.
    2. Among rungs satisfying all componentwise minimums, lexicographic ordering
       (`guarantees.vector`) retains those reaching the strongest achievable guarantee
       vector, or ranks them in lexicographic order.
    """
    meeting_mins: List[Tuple[decide.Rung, Dict[str, int]]] = []
    rejected: List[Tuple[decide.Rung, str]] = []

    for r in rungs:
        lv = guarantees.of([r])
        ok, deficits = holds_componentwise_minimums(lv, minimum_levels)
        if not ok:
            rejected.append((r, "; ".join(deficits)))
        else:
            meeting_mins.append((r, lv))

    if not meeting_mins:
        return (), tuple(rejected)

    # Sort descending by lexicographic guarantee vector, preserving ladder index on ties
    meeting_mins.sort(key=lambda item: guarantees.vector(item[1]), reverse=True)
    return tuple(r for r, _ in meeting_mins), tuple(rejected)


# -----------------------------------------------------------------------------
# Compiled Execution Contract Data Model
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ScopeContract:
    """In-scope tables, secondary objects, filters, and key properties."""

    tables: Tuple[str, ...]
    secondary_objects: Tuple[str, ...] = ()
    row_filter_digest: str = ""
    column_mapping_digest: str = ""
    has_keyless_tables: bool = False
    has_cross_table_fks: bool = False

    def __post_init__(self) -> None:
        if not self.tables:
            raise ExecutionContractError("ScopeContract requires at least one table in scope")

    @property
    def scope_digest(self) -> str:
        payload = json.dumps(
            {
                "tables": sorted(self.tables),
                "secondary_objects": sorted(self.secondary_objects),
                "row_filter_digest": self.row_filter_digest,
                "column_mapping_digest": self.column_mapping_digest,
                "has_keyless_tables": self.has_keyless_tables,
                "has_cross_table_fks": self.has_cross_table_fks,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("ascii")).hexdigest()[:24]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tables": list(self.tables),
            "secondary_objects": list(self.secondary_objects),
            "row_filter_digest": self.row_filter_digest,
            "column_mapping_digest": self.column_mapping_digest,
            "has_keyless_tables": self.has_keyless_tables,
            "has_cross_table_fks": self.has_cross_table_fks,
            "scope_digest": self.scope_digest,
        }


@dataclass(frozen=True)
class CompiledExecutionContract:
    """One checked execution contract compiled from intent, scope, and measured facts."""

    contract_id: str
    goal: MigrationGoalKind
    compilation_choice: decide.Choice
    scope: ScopeContract
    target_identity: TargetIdentity
    version_binding: VersionBinding
    minimum_guarantee_ranks: Dict[str, int]
    required_proof_coverage: ProofCoverageLevel
    snapshot_fence_ordering: SnapshotFenceOrderingMode
    requires_idempotent_writes: bool
    requires_exactly_once_delivery: bool
    requires_durable_restart_cursor: bool
    source_impact_bounds: SourceImpactBounds
    outage_byte_money_bounds: OutageByteMoneyBounds
    allowed_paths: Tuple[ExecutionPathClass, ...]
    explicit_source_change_authorized: bool
    care_badge: CareBadge

    @property
    def minimum_guarantee_names(self) -> Dict[str, str]:
        return {
            dim: guarantees.LEVELS[dim][self.minimum_guarantee_ranks[dim]]
            for dim in guarantees.ORDER
        }

    @property
    def minimum_guarantee_vector(self) -> Tuple[int, ...]:
        return tuple(self.minimum_guarantee_ranks[d] for d in guarantees.ORDER)

    @property
    def contract_digest(self) -> str:
        payload = json.dumps(
            {
                "goal": self.goal.value,
                "scope_digest": self.scope.scope_digest,
                "target_digest": self.target_identity.identity_digest,
                "versions": self.version_binding.to_dict(),
                "minimum_guarantees": self.minimum_guarantee_names,
                "proof_coverage": self.required_proof_coverage.value,
                "snapshot_fence_ordering": self.snapshot_fence_ordering.value,
                "source_impact": self.source_impact_bounds.to_dict(),
                "bounds": self.outage_byte_money_bounds.to_dict(),
                "explicit_source_change_authorized": self.explicit_source_change_authorized,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("ascii")).hexdigest()[:32]

    def format_unix_line(self) -> str:
        g_str = ",".join(
            f"{d}:{guarantees.LEVELS[d][self.minimum_guarantee_ranks[d]]}"
            for d in guarantees.ORDER
        )
        return (
            f"[{self.care_badge.value}] contract={self.contract_id}"
            f" goal={self.goal.value} proof={self.required_proof_coverage.value}"
            f" ordering={self.snapshot_fence_ordering.value}"
            f" guarantees=({g_str})"
            f" max_bytes={self.outage_byte_money_bounds.max_billed_bytes}"
            f" max_usd={self.outage_byte_money_bounds.max_money_usd:.4f}"
            f" max_outage_s={self.outage_byte_money_bounds.max_outage_seconds:.3f}"
        )


# -----------------------------------------------------------------------------
# Decision Engine Ladders (`decide.Rung` / `decide.climb`)
# -----------------------------------------------------------------------------

def intent_compilation_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder compiling user goal and scope into a checked execution contract profile."""
    return (
        decide.Rung(
            "zero_loss_online_cutover_contract",
            "zero-loss online cutover contract with point-in-time snapshot, CDC fence, and byte-exact proof",
            gives=frozenset(
                {"carried", "online", "consistent as of", "exact", "byte exact", "resume by key"}
            ),
            holds={
                "correct": "byte exact",
                "consistent": "as of one moment",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
            needs=(
                decide.Need(
                    "compatible explicit minimum guarantees",
                    lambda f: bool(f.get("minimums_compatible", True)),
                    "explicit minimum guarantees are mutually contradictory or exceed pair capabilities",
                ),
                decide.Need(
                    "online zero-loss goal requested",
                    lambda f: f.get("goal_kind")
                    == MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER.value,
                    "{rung} applies only when goal is zero_loss_online_cutover",
                ),
                decide.Need(
                    "point-in-time snapshot and CDC handoff fence supported",
                    lambda f: bool(f.get("supports_point_in_time_snapshot"))
                    and bool(f.get("supports_cdc_catchup_fence")),
                    "{rung} requires point-in-time snapshot and CDC catch-up fence support",
                ),
                decide.Need(
                    "measured outage, byte, money, and source impact within bounds",
                    lambda f: bool(f.get("bounds_satisfiable")),
                    "{rung} exceeds measured outage, byte, money, or source-impact bounds",
                ),
            ),
            because="compiles online zero-loss intent into strict snapshot/CDC fencing and byte-exact proof",
            footprint=1,
            leaves="a fenced snapshot and CDC handoff contract",
        ),
        decide.Rung(
            "keyless_exact_multiset_contract",
            "keyless exact-multiplicity multiset contract with epoch fence and duplicate-sensitive proof",
            gives=frozenset(
                {"carried", "keyless", "consistent as of", "exact", "byte exact", "resume by chunk"}
            ),
            holds={
                "correct": "byte exact",
                "consistent": "as of one moment",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
            needs=(
                decide.Need(
                    "compatible explicit minimum guarantees",
                    lambda f: bool(f.get("minimums_compatible", True)),
                    "explicit minimum guarantees are mutually contradictory or exceed pair capabilities",
                ),
                decide.Need(
                    "keyless multiset goal or keyless scope present",
                    lambda f: f.get("goal_kind")
                    == MigrationGoalKind.KEYLESS_EXACT_MULTISET.value
                    or bool(f.get("has_keyless_tables")),
                    "{rung} applies only when keyless tables or exact-multiset intent is present",
                ),
                decide.Need(
                    "atomic multiset effect journal and multiset fingerprint verifier available",
                    lambda f: bool(f.get("supports_multiset_effect_journal"))
                    and bool(f.get("supports_multiset_fingerprint_proof")),
                    "{rung} requires atomic multiset effect journal and duplicate-sensitive proof",
                ),
                decide.Need(
                    "measured outage, byte, money, and source impact within bounds",
                    lambda f: bool(f.get("bounds_satisfiable")),
                    "{rung} exceeds measured outage, byte, money, or source-impact bounds",
                ),
            ),
            because="preserves duplicate multiplicity and exact restartability on keyless tables",
            footprint=1,
            leaves="an atomic multiset epoch contract",
        ),
        decide.Rung(
            "consistent_snapshot_seed_contract",
            "consistent snapshot seed contract with row-digest proof and chunk/key resume",
            gives=frozenset(
                {"carried", "consistent as of", "exact", "resume by chunk", "resume by key"}
            ),
            holds={
                "correct": "proved",
                "consistent": "as of one moment",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
            needs=(
                decide.Need(
                    "compatible explicit minimum guarantees",
                    lambda f: bool(f.get("minimums_compatible", True)),
                    "explicit minimum guarantees are mutually contradictory or exceed pair capabilities",
                ),
                decide.Need(
                    "consistent snapshot seed goal requested",
                    lambda f: f.get("goal_kind")
                    == MigrationGoalKind.CONSISTENT_SNAPSHOT_SEED.value,
                    "{rung} applies only when goal is consistent_snapshot_seed",
                ),
                decide.Need(
                    "shared snapshot or frozen write window available",
                    lambda f: bool(f.get("supports_point_in_time_snapshot"))
                    or bool(f.get("source_writes_frozen")),
                    "{rung} requires a point-in-time snapshot or frozen source writes",
                ),
                decide.Need(
                    "candidate meets explicit minimum guarantees componentwise",
                    lambda f: bool(f.get("snapshot_seed_meets_minimums", True)),
                    "{rung} does not satisfy an explicit byte-exact minimum guarantee",
                ),
                decide.Need(
                    "measured outage, byte, money, and source impact within bounds",
                    lambda f: bool(f.get("bounds_satisfiable")),
                    "{rung} exceeds measured outage, byte, money, or source-impact bounds",
                ),
            ),
            because="compiles snapshot seed intent into proved row-digest verification and durable chunk resume",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "bounded_window_maintenance_contract",
            "bounded-window maintenance contract with range-fenced copy and row-digest proof",
            gives=frozenset({"carried", "exact", "resume by key", "resume by chunk"}),
            holds={
                "correct": "proved",
                "consistent": "per range",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
            needs=(
                decide.Need(
                    "compatible explicit minimum guarantees",
                    lambda f: bool(f.get("minimums_compatible", True)),
                    "explicit minimum guarantees are mutually contradictory or exceed pair capabilities",
                ),
                decide.Need(
                    "bounded maintenance window goal requested",
                    lambda f: f.get("goal_kind")
                    == MigrationGoalKind.BOUNDED_WINDOW_MAINTENANCE.value,
                    "{rung} applies only when goal is bounded_window_maintenance",
                ),
                decide.Need(
                    "no unguarded cross-table foreign keys under per-range read",
                    lambda f: not bool(f.get("has_cross_table_fks"))
                    or bool(f.get("source_writes_frozen")),
                    "{rung} cannot read coupled foreign-key tables per-range while source writes are live",
                ),
                decide.Need(
                    "candidate meets explicit minimum guarantees componentwise",
                    lambda f: bool(f.get("bounded_window_meets_minimums", True)),
                    "{rung} falls below explicit consistency or correctness minimums",
                ),
                decide.Need(
                    "measured outage, byte, money, and source impact within bounds",
                    lambda f: bool(f.get("bounds_satisfiable")),
                    "{rung} exceeds measured outage, byte, money, or source-impact bounds",
                ),
            ),
            because="compiles maintenance-window intent into range-fenced idempotent execution with proved rows",
            footprint=0,
            leaves="nothing",
        ),
    )


def contract_path_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder selecting an execution path (`bulk`, `table`, `range`, `native`) under a compiled contract.

    Every path must satisfy all componentwise minimum guarantees, scope/fence/ordering
    invariants, and source-impact/outage/byte/money envelopes before cost ranking.
    """
    return (
        decide.Rung(
            "native_contract_path",
            "provider-native snapshot and log transfer governed by compiled contract",
            gives=frozenset(
                {"carried", "consistent as of", "exact", "byte exact", "resume by chunk"}
            ),
            holds={
                "correct": "byte exact",
                "consistent": "as of one moment",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
            needs=(
                decide.Need(
                    "native path allowed by compiled contract",
                    lambda f: bool(f.get("native_path_allowed")),
                    "{rung} is not enabled for this contract scope",
                ),
                decide.Need(
                    "native path satisfies all componentwise minimum guarantees",
                    lambda f: bool(f.get("native_meets_contract_guarantees")),
                    "{rung} drops a required guarantee dimension",
                ),
                decide.Need(
                    "native path preserves secondary objects or runs independent supplement",
                    lambda f: bool(f.get("native_covers_full_scope")),
                    "{rung} omits required secondary objects or row filters without independent supplement",
                ),
                decide.Need(
                    "native path stays within source-impact, outage, byte, and money bounds",
                    lambda f: bool(f.get("native_within_contract_bounds")),
                    "{rung} exceeds contract source-impact, outage, byte, or money ceiling",
                ),
            ),
            because="native snapshot/log path satisfies every contract dimension at lowest measured transfer cost",
            footprint=1,
            leaves="a provider-native transfer job",
        ),
        decide.Rung(
            "bulk_contract_path",
            "parallel bulk stream governed by compiled contract and post-load proof",
            gives=frozenset(
                {"carried", "consistent as of", "exact", "byte exact", "resume by chunk"}
            ),
            holds={
                "correct": "byte exact",
                "consistent": "as of one moment",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
            needs=(
                decide.Need(
                    "bulk path allowed by compiled contract",
                    lambda f: bool(f.get("bulk_path_allowed")),
                    "{rung} is not enabled for this contract scope",
                ),
                decide.Need(
                    "bulk path satisfies all componentwise minimum guarantees",
                    lambda f: bool(f.get("bulk_meets_contract_guarantees")),
                    "{rung} drops a required guarantee dimension",
                ),
                decide.Need(
                    "bulk path preserves snapshot fence and ordering invariants",
                    lambda f: bool(f.get("bulk_preserves_fence_and_ordering")),
                    "{rung} cannot preserve the contract's snapshot fence or commit ordering",
                ),
                decide.Need(
                    "bulk path stays within source-impact, outage, byte, and money bounds",
                    lambda f: bool(f.get("bulk_within_contract_bounds")),
                    "{rung} exceeds contract source-impact, outage, byte, or money ceiling",
                ),
            ),
            because="bulk path with verified digest/byte-exact lift meets all contract dimensions",
            footprint=1,
            leaves="a staged bulk load session",
        ),
        decide.Rung(
            "range_contract_path",
            "key-range fenced parallel worker path governed by compiled contract",
            gives=frozenset(
                {
                    "carried",
                    "online",
                    "consistent as of",
                    "exact",
                    "byte exact",
                    "resume by key",
                    "resume by chunk",
                }
            ),
            holds={
                "correct": "byte exact",
                "consistent": "as of one moment",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
            needs=(
                decide.Need(
                    "range path allowed by compiled contract",
                    lambda f: bool(f.get("range_path_allowed")),
                    "{rung} is not enabled for this contract scope",
                ),
                decide.Need(
                    "range path satisfies all componentwise minimum guarantees",
                    lambda f: bool(f.get("range_meets_contract_guarantees")),
                    "{rung} drops a required guarantee dimension",
                ),
                decide.Need(
                    "range path preserves snapshot fence and ordering invariants",
                    lambda f: bool(f.get("range_preserves_fence_and_ordering")),
                    "{rung} cannot preserve the contract's snapshot fence or commit ordering",
                ),
                decide.Need(
                    "range path stays within source-impact, outage, byte, and money bounds",
                    lambda f: bool(f.get("range_within_contract_bounds")),
                    "{rung} exceeds contract source-impact, outage, byte, or money ceiling",
                ),
            ),
            because="range-fenced workers provide O(1) cursor resume and bounded source impact under contract",
            footprint=0,
            leaves="nothing",
            native_copier=True,
        ),
        decide.Rung(
            "table_contract_path",
            "ordered table-level transactional copy path governed by compiled contract",
            gives=frozenset(
                {"carried", "online", "consistent as of", "exact", "byte exact", "resume by key"}
            ),
            holds={
                "correct": "byte exact",
                "consistent": "as of one moment",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
            needs=(
                decide.Need(
                    "table path allowed by compiled contract",
                    lambda f: bool(f.get("table_path_allowed")),
                    "{rung} is not enabled for this contract scope",
                ),
                decide.Need(
                    "table path satisfies all componentwise minimum guarantees",
                    lambda f: bool(f.get("table_meets_contract_guarantees")),
                    "{rung} drops a required guarantee dimension",
                ),
                decide.Need(
                    "table path preserves snapshot fence and ordering invariants",
                    lambda f: bool(f.get("table_preserves_fence_and_ordering")),
                    "{rung} cannot preserve the contract's snapshot fence or commit ordering",
                ),
                decide.Need(
                    "table path stays within source-impact, outage, byte, and money bounds",
                    lambda f: bool(f.get("table_within_contract_bounds")),
                    "{rung} exceeds contract source-impact, outage, byte, or money ceiling",
                ),
            ),
            because="table-level transactional path preserves strict FK and commit ordering under contract",
            footprint=0,
            leaves="nothing",
        ),
    )


def boundary_admission_rungs() -> Tuple[decide.Rung, ...]:
    """Independent admission ladder evaluated before every irreversible boundary."""
    return (
        decide.Rung(
            "refuse_contract_breach",
            "refuse irreversible boundary due to contract breach or unheld dimension",
            needs=(
                decide.Need(
                    "hard contract breach detected at boundary",
                    lambda f: bool(f.get("hard_contract_breach")),
                    "no hard contract breach was detected at this boundary",
                ),
            ),
            because="an irreversible boundary must never be crossed when target identity, proof, or bounds fail",
            footprint=0,
        ),
        decide.Rung(
            "hold_for_revalidation",
            "hold irreversible boundary until stale facts, epochs, or pending proofs are revalidated",
            needs=(
                decide.Need(
                    "no hard contract breach",
                    lambda f: not bool(f.get("hard_contract_breach")),
                    "hard contract breach requires immediate refusal",
                ),
                decide.Need(
                    "version drift or pending proof revalidation required",
                    lambda f: bool(f.get("requires_boundary_revalidation")),
                    "all version bindings and proofs are already current",
                ),
            ),
            because="stale fact/model/plan epochs or incomplete proof certificates require revalidation before commit",
            footprint=0,
        ),
        decide.Rung(
            "admit_irreversible_boundary",
            "admit irreversible boundary with verified contract, identity, versions, and bounds",
            needs=(
                decide.Need(
                    "no hard contract breach",
                    lambda f: not bool(f.get("hard_contract_breach")),
                    "cannot admit boundary when a contract dimension or bound is breached",
                ),
                decide.Need(
                    "all version bindings, target identity, and proof coverage verified",
                    lambda f: not bool(f.get("requires_boundary_revalidation"))
                    and bool(f.get("all_boundary_invariants_verified")),
                    "boundary revalidation is still pending or invariants are unverified",
                ),
            ),
            because="every compiled contract invariant, proof certificate, and budget bound holds at the boundary",
            footprint=0,
        ),
    )


# -----------------------------------------------------------------------------
# Intent Compilation Entry Point
# -----------------------------------------------------------------------------

def compile_execution_contract(
    goal: Union[str, MigrationGoalKind],
    scope: ScopeContract,
    target_identity: TargetIdentity,
    version_binding: VersionBinding,
    readings: ContractTelemetryReadings,
    *,
    explicit_minimum_guarantees: Optional[Mapping[str, Union[str, int]]] = None,
    scope_facts: Optional[Mapping[str, Any]] = None,
    operator_max_outage_seconds: Optional[float] = None,
    operator_max_billed_bytes: Optional[int] = None,
    operator_max_money_usd: Optional[float] = None,
    operator_max_cpu_ratio: Optional[float] = None,
    operator_max_connections: Optional[int] = None,
    explicit_source_change_authorized: bool = False,
    allowed_paths: Sequence[ExecutionPathClass] = (
        ExecutionPathClass.NATIVE,
        ExecutionPathClass.BULK,
        ExecutionPathClass.RANGE,
        ExecutionPathClass.TABLE,
    ),
) -> CompiledExecutionContract:
    """Compile user goal, scope, and measured telemetry into one checked `CompiledExecutionContract`.

    Enforces:
    1. Explicit minimum guarantees are validated for compatibility before ranking;
       incompatible minimums raise `IncompatibleMinimumGuaranteesError`.
    2. Bounds for source impact, outage window, billed bytes, and monetary cost are
       derived from measured `ContractTelemetryReadings` and checked against operator
       ceilings.
    3. Climbing `intent_compilation_rungs()` via `decide.climb` selects the contract
       profile whose guarantee vector satisfies all explicit minimums componentwise.
    """
    goal_enum = (
        goal if isinstance(goal, MigrationGoalKind) else MigrationGoalKind(str(goal))
    )
    raw_facts: Dict[str, Any] = dict(scope_facts or {})
    if "source_host" in raw_facts:
        validate_safe_endpoint(str(raw_facts["source_host"]))

    # Default goal-level minimum guarantees, merged with any explicit operator minimums
    goal_default_mins: Dict[MigrationGoalKind, Dict[str, str]] = {
        MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER: {
            "correct": "byte exact",
            "consistent": "as of one moment",
            "idempotent": "converges",
            "exactly once": "exactly once",
            "resumable": "goes on",
        },
        MigrationGoalKind.KEYLESS_EXACT_MULTISET: {
            "correct": "byte exact",
            "consistent": "as of one moment",
            "idempotent": "converges",
            "exactly once": "exactly once",
            "resumable": "goes on",
        },
        MigrationGoalKind.CONSISTENT_SNAPSHOT_SEED: {
            "correct": "proved",
            "consistent": "as of one moment",
            "idempotent": "converges",
            "exactly once": "exactly once",
            "resumable": "goes on",
        },
        MigrationGoalKind.BOUNDED_WINDOW_MAINTENANCE: {
            "correct": "proved",
            "consistent": "per range",
            "idempotent": "converges",
            "exactly once": "exactly once",
            "resumable": "goes on",
        },
    }

    base_mins = parse_guarantee_minimums(goal_default_mins[goal_enum])
    explicit_ranks, contradictions = validate_compatible_minimums(
        explicit_minimum_guarantees or {}, raw_facts
    )
    combined_mins: Dict[str, int] = {
        dim: max(base_mins[dim], explicit_ranks[dim]) for dim in guarantees.ORDER
    }
    _, combined_contradictions = validate_compatible_minimums(combined_mins, raw_facts)
    all_contradictions = tuple(dict.fromkeys(contradictions + combined_contradictions))

    source_bounds, resource_bounds = derive_bounds_from_readings(
        readings,
        operator_max_outage_seconds=operator_max_outage_seconds,
        operator_max_billed_bytes=operator_max_billed_bytes,
        operator_max_money_usd=operator_max_money_usd,
        operator_max_cpu_ratio=operator_max_cpu_ratio,
        operator_max_connections=operator_max_connections,
    )

    bounds_satisfiable = (
        resource_bounds.estimated_outage_seconds <= resource_bounds.max_outage_seconds
        and resource_bounds.estimated_billed_bytes <= resource_bounds.max_billed_bytes
        and resource_bounds.estimated_money_usd <= resource_bounds.max_money_usd
        and bool(raw_facts.get("source_impact_within_envelope", True))
    )

    rungs = intent_compilation_rungs()
    rung_by_name = {r.name: r for r in rungs}

    snapshot_seed_ok, _ = holds_componentwise_minimums(
        guarantees.of([rung_by_name["consistent_snapshot_seed_contract"]]),
        combined_mins,
    )
    bounded_win_ok, _ = holds_componentwise_minimums(
        guarantees.of([rung_by_name["bounded_window_maintenance_contract"]]),
        combined_mins,
    )

    climb_facts = decide.Facts(
        {
            **raw_facts,
            "goal_kind": goal_enum.value,
            "has_keyless_tables": scope.has_keyless_tables,
            "has_cross_table_fks": scope.has_cross_table_fks,
            "minimums_compatible": len(all_contradictions) == 0,
            "bounds_satisfiable": bounds_satisfiable,
            "snapshot_seed_meets_minimums": snapshot_seed_ok,
            "bounded_window_meets_minimums": bounded_win_ok,
            "supports_point_in_time_snapshot": bool(
                raw_facts.get("supports_point_in_time_snapshot", True)
            ),
            "supports_cdc_catchup_fence": bool(
                raw_facts.get("supports_cdc_catchup_fence", True)
            ),
            "supports_multiset_effect_journal": bool(
                raw_facts.get("supports_multiset_effect_journal", True)
            ),
            "supports_multiset_fingerprint_proof": bool(
                raw_facts.get("supports_multiset_fingerprint_proof", True)
            ),
            "source_writes_frozen": bool(raw_facts.get("source_writes_frozen", False)),
        }
    )

    choice = decide.climb("execution contract intent compilation", rungs, climb_facts)

    if all_contradictions:
        raise IncompatibleMinimumGuaranteesError(
            f"incompatible minimum guarantees refused: {'; '.join(all_contradictions)}"
        )

    if choice.rung is None:
        raise ExecutionContractError(
            f"intent compilation refused for goal={goal_enum.value}: {choice.reason}"
        )

    chosen_levels = guarantees.of([choice.rung])
    ok_min, min_deficits = holds_componentwise_minimums(chosen_levels, combined_mins)
    if not ok_min:
        raise IncompatibleMinimumGuaranteesError(
            f"compiled rung {choice.path} falls below minimum guarantees: {'; '.join(min_deficits)}"
        )

    proof_cov = ProofCoverageLevel(guarantees.LEVELS["correct"][chosen_levels["correct"]])
    if choice.path == "zero_loss_online_cutover_contract":
        ordering_mode = SnapshotFenceOrderingMode.STRICT_EPOCH_FENCE_COMMIT_ORDER
    elif choice.path == "keyless_exact_multiset_contract":
        ordering_mode = SnapshotFenceOrderingMode.MULTISET_EPOCH_LOG_ORDER
    elif choice.path == "consistent_snapshot_seed_contract":
        ordering_mode = SnapshotFenceOrderingMode.SHARED_SNAPSHOT_KEY_ORDER
    else:
        ordering_mode = SnapshotFenceOrderingMode.RANGE_FENCED_CHUNK_ORDER

    contract_id = f"contract-{goal_enum.value}-{target_identity.identity_digest[:8]}"
    return CompiledExecutionContract(
        contract_id=contract_id,
        goal=goal_enum,
        compilation_choice=choice,
        scope=scope,
        target_identity=target_identity,
        version_binding=version_binding,
        minimum_guarantee_ranks=chosen_levels,
        required_proof_coverage=proof_cov,
        snapshot_fence_ordering=ordering_mode,
        requires_idempotent_writes=(
            chosen_levels["idempotent"] >= guarantees.level("idempotent", "converges")
        ),
        requires_exactly_once_delivery=(
            chosen_levels["exactly once"]
            >= guarantees.level("exactly once", "exactly once")
        ),
        requires_durable_restart_cursor=(
            chosen_levels["resumable"] >= guarantees.level("resumable", "goes on")
        ),
        source_impact_bounds=source_bounds,
        outage_byte_money_bounds=resource_bounds,
        allowed_paths=tuple(allowed_paths),
        explicit_source_change_authorized=bool(explicit_source_change_authorized),
        care_badge=CareBadge.PASS,
    )


# -----------------------------------------------------------------------------
# Carrying the Contract Across Paths, Fallback, and Resume
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class CandidatePathProfile:
    """Measured runtime and guarantee profile of a candidate execution path."""

    path_class: ExecutionPathClass
    guarantees_held: Mapping[str, Union[str, int]]
    covers_full_scope: bool
    preserves_fence_and_ordering: bool
    measured_duration_seconds: float
    measured_outage_seconds: float
    measured_billed_bytes: int
    measured_money_usd: float
    measured_source_cpu_ratio: float
    measured_source_iops: float
    measured_source_read_bytes_per_sec: int
    measured_worker_connections: int
    requires_unauthorized_source_mutation: bool = False


@dataclass(frozen=True)
class ContractPathSelection:
    """Result of selecting or falling back across execution paths under the contract."""

    choice: decide.Choice
    selected_path: Optional[ExecutionPathClass]
    selected_profile: Optional[CandidatePathProfile]
    rejected_reasons: Dict[str, Tuple[str, ...]]
    care_badge: CareBadge
    summary: str

    def format_unix_line(self) -> str:
        p_str = self.selected_path.value if self.selected_path else "none"
        return (
            f"[{self.care_badge.value}] path={p_str}"
            f" reason={self.summary}"
        )


def _evaluate_candidate_against_contract(
    contract: CompiledExecutionContract,
    profile: CandidatePathProfile,
) -> Tuple[bool, bool, Tuple[str, ...]]:
    """Verify whether `profile` satisfies all contract guarantees and resource bounds.

    Returns `(meets_guarantees, within_bounds, violations)`.
    """
    cand_ranks = parse_guarantee_minimums(profile.guarantees_held)
    meets_guarantees, g_deficits = holds_componentwise_minimums(
        cand_ranks, contract.minimum_guarantee_ranks
    )
    violations: List[str] = list(g_deficits)

    if not profile.covers_full_scope:
        violations.append(
            f"{profile.path_class.value} omits required scope or secondary objects"
        )
    if not profile.preserves_fence_and_ordering:
        violations.append(
            f"{profile.path_class.value} violates {contract.snapshot_fence_ordering.value}"
        )
    if (
        profile.requires_unauthorized_source_mutation
        and not contract.explicit_source_change_authorized
    ):
        violations.append(
            f"{profile.path_class.value} requires unauthorized source mutation"
        )

    within_bounds = True
    sb = contract.source_impact_bounds
    rb = contract.outage_byte_money_bounds

    if profile.measured_source_cpu_ratio > sb.max_cpu_ratio + 1e-9:
        within_bounds = False
        violations.append(
            f"source CPU {profile.measured_source_cpu_ratio:.3f} > bound {sb.max_cpu_ratio:.3f}"
        )
    if profile.measured_source_iops > sb.max_iops + 1e-9:
        within_bounds = False
        violations.append(
            f"source IOPS {profile.measured_source_iops:.1f} > bound {sb.max_iops:.1f}"
        )
    if profile.measured_source_read_bytes_per_sec > sb.max_read_bytes_per_sec:
        within_bounds = False
        violations.append(
            f"source read bps {profile.measured_source_read_bytes_per_sec} > bound {sb.max_read_bytes_per_sec}"
        )
    if profile.measured_worker_connections > sb.max_worker_connections:
        within_bounds = False
        violations.append(
            f"worker connections {profile.measured_worker_connections} > bound {sb.max_worker_connections}"
        )
    if profile.measured_outage_seconds > rb.max_outage_seconds + 1e-9:
        within_bounds = False
        violations.append(
            f"outage {profile.measured_outage_seconds:.3f}s > bound {rb.max_outage_seconds:.3f}s"
        )
    if profile.measured_billed_bytes > rb.max_billed_bytes:
        within_bounds = False
        violations.append(
            f"billed bytes {profile.measured_billed_bytes} > bound {rb.max_billed_bytes}"
        )
    if profile.measured_money_usd > rb.max_money_usd + 1e-9:
        within_bounds = False
        violations.append(
            f"money {profile.measured_money_usd:.4f} USD > bound {rb.max_money_usd:.4f} USD"
        )

    if profile.requires_unauthorized_source_mutation and not contract.explicit_source_change_authorized:
        within_bounds = False

    return meets_guarantees, within_bounds, tuple(violations)


def evaluate_path_under_contract(
    contract: CompiledExecutionContract,
    candidates: Mapping[ExecutionPathClass, CandidatePathProfile],
    *,
    failed_paths: Iterable[ExecutionPathClass] = (),
    work: str = "contract-governed path selection",
) -> ContractPathSelection:
    """Select the fastest valid execution path (`bulk`, `table`, `range`, `native`) under `contract`.

    Adversarial faster alternatives that drop a required guarantee dimension, omit
    scope/secondary objects, violate snapshot/fence/ordering, require unauthorized
    source changes, or exceed source-impact/outage/byte/money bounds are refused by
    `decide.climb`.
    """
    failed_set = frozenset(failed_paths)
    allowed_set = frozenset(contract.allowed_paths) - failed_set

    facts_dict: Dict[str, Any] = {}
    costs = decide.Costs()
    rejected_reasons: Dict[str, Tuple[str, ...]] = {}

    path_to_rung = {
        ExecutionPathClass.NATIVE: ("native", "native_contract_path"),
        ExecutionPathClass.BULK: ("bulk", "bulk_contract_path"),
        ExecutionPathClass.RANGE: ("range", "range_contract_path"),
        ExecutionPathClass.TABLE: ("table", "table_contract_path"),
    }

    for p_class, (prefix, rung_name) in path_to_rung.items():
        profile = candidates.get(p_class)
        is_allowed = (p_class in allowed_set) and (profile is not None)
        facts_dict[f"{prefix}_path_allowed"] = is_allowed

        if profile is None:
            facts_dict[f"{prefix}_meets_contract_guarantees"] = False
            facts_dict[f"{prefix}_covers_full_scope"] = False
            facts_dict[f"{prefix}_preserves_fence_and_ordering"] = False
            facts_dict[f"{prefix}_within_contract_bounds"] = False
            rejected_reasons[p_class.value] = ("no candidate profile supplied",)
            continue

        meets_g, within_b, violations = _evaluate_candidate_against_contract(
            contract, profile
        )
        if p_class in failed_set:
            violations = ("path failed at runtime and is excluded",) + violations

        facts_dict[f"{prefix}_meets_contract_guarantees"] = meets_g
        facts_dict[f"{prefix}_covers_full_scope"] = (
            profile.covers_full_scope and profile.preserves_fence_and_ordering
        )
        facts_dict[f"{prefix}_preserves_fence_and_ordering"] = (
            profile.preserves_fence_and_ordering and profile.covers_full_scope
        )
        facts_dict[f"{prefix}_within_contract_bounds"] = within_b

        if violations:
            rejected_reasons[p_class.value] = violations
        if profile.measured_duration_seconds > 0.0:
            costs.saw(rung_name, 1, float(profile.measured_duration_seconds))

    rungs = contract_path_rungs()
    choice = decide.climb(work, rungs, decide.Facts(facts_dict), costs=costs)

    rung_to_path = {v[1]: k for k, v in path_to_rung.items()}
    if choice.rung is None:
        return ContractPathSelection(
            choice=choice,
            selected_path=None,
            selected_profile=None,
            rejected_reasons=rejected_reasons,
            care_badge=CareBadge.ACTION,
            summary=f"no path satisfies compiled contract: {choice.reason}",
        )

    sel_path = rung_to_path[choice.path]
    sel_profile = candidates[sel_path]
    badge = CareBadge.REVIEW if failed_set else CareBadge.PASS
    return ContractPathSelection(
        choice=choice,
        selected_path=sel_path,
        selected_profile=sel_profile,
        rejected_reasons=rejected_reasons,
        care_badge=badge,
        summary=choice.reason,
    )


def evaluate_fallback_under_contract(
    contract: CompiledExecutionContract,
    candidates: Mapping[ExecutionPathClass, CandidatePathProfile],
    failed_paths: Sequence[ExecutionPathClass],
) -> ContractPathSelection:
    """Switch from failed path(s) to the next ranked path that preserves the full contract.

    Raises `ContractDimensionDroppedError` if all remaining alternatives would drop a
    required guarantee dimension or violate a contract bound.
    """
    selection = evaluate_path_under_contract(
        contract,
        candidates,
        failed_paths=failed_paths,
        work="runtime fallback path selection",
    )
    if selection.selected_path is None:
        raise ContractDimensionDroppedError(
            f"runtime fallback refused after failure of {[p.value for p in failed_paths]}: "
            f"{selection.summary}"
        )
    return selection


# -----------------------------------------------------------------------------
# Independent Pre-Boundary Admission Checks (Cutover, Rollback, Exposure, Release)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class BoundaryRuntimeState:
    """Observed state inspected independently before crossing an irreversible boundary."""

    boundary: IrreversibleBoundaryKind
    observed_target_identity: TargetIdentity
    observed_version_binding: VersionBinding
    observed_scope_digest: str
    achieved_proof_coverage: ProofCoverageLevel
    achieved_guarantees: Mapping[str, Union[str, int]]
    snapshot_fence_verified: bool
    commit_ordering_verified: bool
    consumed_outage_seconds: float
    consumed_billed_bytes: int
    consumed_money_usd: float
    observed_source_cpu_ratio: float
    observed_source_iops: float
    observed_source_read_bytes_per_sec: int
    observed_worker_connections: int
    unauthorized_source_mutation_attempted: bool = False
    rollback_compensations_verified: bool = True


@dataclass(frozen=True)
class BoundaryAdmissionReport:
    """Outcome of independent admission before an irreversible boundary."""

    boundary: IrreversibleBoundaryKind
    choice: decide.Choice
    verdict: BoundaryAdmissionVerdict
    admitted: bool
    breaches: Tuple[str, ...]
    revalidation_needed: Tuple[str, ...]
    care_badge: CareBadge

    def format_unix_line(self) -> str:
        b_str = "; ".join(self.breaches) if self.breaches else "none"
        r_str = (
            "; ".join(self.revalidation_needed) if self.revalidation_needed else "none"
        )
        return (
            f"[{self.care_badge.value}] boundary={self.boundary.value}"
            f" verdict={self.verdict.value} admitted={self.admitted}"
            f" breaches={b_str} revalidation={r_str}"
        )


def admit_irreversible_boundary(
    contract: CompiledExecutionContract,
    state: BoundaryRuntimeState,
    *,
    raise_on_refusal: bool = False,
) -> BoundaryAdmissionReport:
    """Run independent admission checks via `decide.climb` before an irreversible boundary.

    Checks:
    1. Target identity matches `contract.target_identity` (prevents writing/switching
       against the wrong cluster, database, or schema).
    2. Scope digest matches `contract.scope.scope_digest`.
    3. Componentwise guarantee minimums and required proof coverage hold.
    4. Snapshot fence and commit ordering hold; for `ROLLBACK_SWITCH`, rollback
       compensation readiness holds.
    5. Runtime consumed bytes, money, outage window, and source load stay within
       compiled bounds.
    6. No unauthorized source mutation was attempted.
    7. Bound `facts_version`, `model_version`, `plan_version`, and `source_epoch`
       match; version drift triggers `hold_for_revalidation` until re-verified.
    """
    breaches: List[str] = []
    revalidations: List[str] = []

    # 1. Target identity verification
    if not contract.target_identity.matches(state.observed_target_identity):
        breaches.append(
            f"target identity mismatch: expected {contract.target_identity.identity_digest},"
            f" observed {state.observed_target_identity.identity_digest}"
        )

    # 2. Scope digest verification
    if state.observed_scope_digest != contract.scope.scope_digest:
        breaches.append(
            f"scope digest mismatch: expected {contract.scope.scope_digest},"
            f" observed {state.observed_scope_digest}"
        )

    # 3. Guarantee vector and proof coverage verification
    achieved_ranks = parse_guarantee_minimums(state.achieved_guarantees)
    ok_g, g_deficits = holds_componentwise_minimums(
        achieved_ranks, contract.minimum_guarantee_ranks
    )
    if not ok_g:
        breaches.extend(g_deficits)

    req_proof_rank = guarantees.level("correct", contract.required_proof_coverage.value)
    got_proof_rank = guarantees.level("correct", state.achieved_proof_coverage.value)
    if got_proof_rank < req_proof_rank:
        breaches.append(
            f"proof coverage {state.achieved_proof_coverage.value!r} < required"
            f" {contract.required_proof_coverage.value!r}"
        )

    # 4. Snapshot fence, ordering, and rollback readiness
    if not state.snapshot_fence_verified:
        breaches.append("snapshot/CDC handoff fence is not verified")
    if not state.commit_ordering_verified:
        breaches.append(
            f"ordering mode {contract.snapshot_fence_ordering.value} is not verified"
        )
    if (
        state.boundary == IrreversibleBoundaryKind.ROLLBACK_SWITCH
        and not state.rollback_compensations_verified
    ):
        breaches.append("rollback compensation invariants are not verified")

    # 5. Unauthorized source mutation check
    if (
        state.unauthorized_source_mutation_attempted
        and not contract.explicit_source_change_authorized
    ):
        breaches.append("unauthorized source mutation attempted without operator approval")

    # 6. Outage, byte, money, and source impact bounds
    rb = contract.outage_byte_money_bounds
    sb = contract.source_impact_bounds
    if state.consumed_outage_seconds > rb.max_outage_seconds + 1e-9:
        breaches.append(
            f"consumed outage {state.consumed_outage_seconds:.3f}s exceeds contract"
            f" max {rb.max_outage_seconds:.3f}s"
        )
    if state.consumed_billed_bytes > rb.max_billed_bytes:
        breaches.append(
            f"consumed billed bytes {state.consumed_billed_bytes} exceeds contract"
            f" max {rb.max_billed_bytes}"
        )
    if state.consumed_money_usd > rb.max_money_usd + 1e-9:
        breaches.append(
            f"consumed money {state.consumed_money_usd:.4f} USD exceeds contract"
            f" max {rb.max_money_usd:.4f} USD"
        )
    if state.observed_source_cpu_ratio > sb.max_cpu_ratio + 1e-9:
        breaches.append(
            f"source CPU ratio {state.observed_source_cpu_ratio:.3f} exceeds contract"
            f" max {sb.max_cpu_ratio:.3f}"
        )
    if state.observed_source_iops > sb.max_iops + 1e-9:
        breaches.append(
            f"source IOPS {state.observed_source_iops:.1f} exceeds contract"
            f" max {sb.max_iops:.1f}"
        )
    if state.observed_source_read_bytes_per_sec > sb.max_read_bytes_per_sec:
        breaches.append(
            f"source read bps {state.observed_source_read_bytes_per_sec} exceeds contract"
            f" max {sb.max_read_bytes_per_sec}"
        )
    if state.observed_worker_connections > sb.max_worker_connections:
        breaches.append(
            f"worker connections {state.observed_worker_connections} exceeds contract"
            f" max {sb.max_worker_connections}"
        )

    # 7. Version binding checks (facts/model/plan/epoch)
    vb = contract.version_binding
    obs_vb = state.observed_version_binding
    if obs_vb.facts_version != vb.facts_version:
        revalidations.append(
            f"facts_version changed ({vb.facts_version} -> {obs_vb.facts_version})"
        )
    if obs_vb.model_version != vb.model_version:
        revalidations.append(
            f"model_version changed ({vb.model_version} -> {obs_vb.model_version})"
        )
    if obs_vb.plan_version != vb.plan_version:
        revalidations.append(
            f"plan_version changed ({vb.plan_version} -> {obs_vb.plan_version})"
        )
    if obs_vb.source_epoch != vb.source_epoch:
        revalidations.append(
            f"source_epoch changed ({vb.source_epoch} -> {obs_vb.source_epoch})"
        )

    facts = decide.Facts(
        {
            "hard_contract_breach": len(breaches) > 0,
            "requires_boundary_revalidation": len(breaches) == 0
            and len(revalidations) > 0,
            "all_boundary_invariants_verified": len(breaches) == 0
            and len(revalidations) == 0,
        }
    )
    rungs = boundary_admission_rungs()
    choice = decide.climb(
        f"irreversible boundary admission ({state.boundary.value})",
        rungs,
        facts,
    )
    verdict = BoundaryAdmissionVerdict(choice.path)
    admitted = verdict == BoundaryAdmissionVerdict.ADMIT_IRREVERSIBLE_BOUNDARY

    if verdict == BoundaryAdmissionVerdict.REFUSE_CONTRACT_BREACH:
        badge = CareBadge.ACTION
        if raise_on_refusal:
            raise IrreversibleBoundaryRefusedError(
                f"refused {state.boundary.value}: {'; '.join(breaches)}"
            )
    elif verdict == BoundaryAdmissionVerdict.HOLD_FOR_REVALIDATION:
        badge = CareBadge.REVIEW
        if raise_on_refusal:
            raise IrreversibleBoundaryRefusedError(
                f"held {state.boundary.value} for revalidation: {'; '.join(revalidations)}"
            )
    else:
        badge = CareBadge.PASS

    return BoundaryAdmissionReport(
        boundary=state.boundary,
        choice=choice,
        verdict=verdict,
        admitted=admitted,
        breaches=tuple(breaches),
        revalidation_needed=tuple(revalidations),
        care_badge=badge,
    )


# -----------------------------------------------------------------------------
# O(1) Typed Durable Resume & Active Lease Lock under Compiled Contract
# -----------------------------------------------------------------------------

def _encode_typed(val: Any) -> Any:
    """Encode native Python types (datetime, date, Decimal, UUID, tuple) for JSON."""
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
    if isinstance(val, dict):
        return {str(k): _encode_typed(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_encode_typed(x) for x in val]
    return val


def _decode_typed(val: Any) -> Any:
    """Restore native Python types (datetime, date, Decimal, UUID, tuple) from JSON."""
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
        return {k: _decode_typed(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_decode_typed(x) for x in val]
    return val


def save_contract_unit_checkpoint(
    checkpoint_path: Path,
    contract: CompiledExecutionContract,
    unit_id: str,
    path_class: ExecutionPathClass,
    cursor_position: Any,
    billed_bytes_delta: int,
    money_usd_delta: float,
) -> Path:
    """Persist a completed unit in O(1) bound to `contract.contract_digest`."""
    p = Path(checkpoint_path)
    state = durable.read_state(p, "state") or {}
    existing_digest = state.get("contract_digest")
    if existing_digest is not None and existing_digest != contract.contract_digest:
        raise ContractDimensionDroppedError(
            f"checkpoint at {p} is bound to contract_digest={existing_digest},"
            f" incompatible with active contract_digest={contract.contract_digest}"
        )

    units = dict(state.get("units") or {})
    units[str(unit_id)] = {
        "path_class": path_class.value,
        "cursor_position": _encode_typed(cursor_position),
        "billed_bytes_delta": int(billed_bytes_delta),
        "money_usd_delta": float(money_usd_delta),
        "scope_digest": contract.scope.scope_digest,
        "target_digest": contract.target_identity.identity_digest,
    }
    payload = {
        "contract_id": contract.contract_id,
        "contract_digest": contract.contract_digest,
        "scope_digest": contract.scope.scope_digest,
        "target_digest": contract.target_identity.identity_digest,
        "minimum_guarantees": contract.minimum_guarantee_names,
        "units": units,
    }
    return durable.write_json(p, payload)


def verify_resume_under_contract(
    checkpoint_path: Path,
    contract: CompiledExecutionContract,
    unit_id: str,
) -> Optional[Dict[str, Any]]:
    """Lookup a completed unit in O(1) and verify that contract, scope, and target bindings hold.

    Raises `ContractDimensionDroppedError` if the checkpoint was written under a
    different target identity, scope digest, or weaker guarantee contract.
    """
    p = Path(checkpoint_path)
    state = durable.read_state(p, "state")
    if not state:
        return None

    saved_target = state.get("target_digest")
    if saved_target != contract.target_identity.identity_digest:
        raise ContractDimensionDroppedError(
            f"resume refused: checkpoint target_digest={saved_target} != contract"
            f" target_digest={contract.target_identity.identity_digest}"
        )

    saved_scope = state.get("scope_digest")
    if saved_scope != contract.scope.scope_digest:
        raise ContractDimensionDroppedError(
            f"resume refused: checkpoint scope_digest={saved_scope} != contract"
            f" scope_digest={contract.scope.scope_digest}"
        )

    saved_mins = parse_guarantee_minimums(state.get("minimum_guarantees") or {})
    ok_g, deficits = holds_componentwise_minimums(
        saved_mins, contract.minimum_guarantee_ranks
    )
    if not ok_g:
        raise ContractDimensionDroppedError(
            f"resume refused: checkpoint guarantees weaker than active contract: {'; '.join(deficits)}"
        )

    units = state.get("units") or {}
    entry = units.get(str(unit_id))
    if entry is None:
        return None

    return {
        "unit_id": str(unit_id),
        "path_class": ExecutionPathClass(entry["path_class"]),
        "cursor_position": _decode_typed(entry["cursor_position"]),
        "billed_bytes_delta": int(entry["billed_bytes_delta"]),
        "money_usd_delta": float(entry["money_usd_delta"]),
    }


def acquire_contract_execution_lease(
    lock_path: Path,
    holder: str,
    contract_digest: str,
    ttl_seconds: float,
    now_epoch: float,
) -> Dict[str, Any]:
    """Acquire or renew an exclusive execution lease bound to `contract_digest` via `migkit.lease._FileRecord`."""
    if ttl_seconds <= 0.0:
        raise ExecutionContractError("ttl_seconds must be positive")
    rec = lease._FileRecord(Path(lock_path))

    def _decide(have: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if have is not None:
            current_holder = have.get("holder")
            expires = lease.lapse(have)
            if current_holder and current_holder != holder and expires > now_epoch:
                raise ExecutionContractError(
                    f"lease at {lock_path} is actively held by {current_holder}"
                )
        return {
            "holder": holder,
            "contract_digest": contract_digest,
            "renewed": float(now_epoch),
            "expires": float(now_epoch) + float(ttl_seconds),
            "jitter": 0.0,
        }

    return rec.change(_decide)
