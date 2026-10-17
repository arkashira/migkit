"""Cost transactional and batch apply under different contracts (Row 612: RCH04.apply-contract).

Declares the integrity, ordering, key, and trigger assumptions of each apply mode
and ranks batch merging only when required guarantees can be maintained or
restored before exposure:

- Explicit contract declarations (`ApplyModeContract`) covering integrity
  (single-table vs cross-table transaction atomicity, foreign-key cascade
  behavior, pre-exposure restoration), ordering (`commit_order`, `per_key_net`,
  `epoch_barrier`, `multiset_log_order`, `idempotent_token_order`), key
  assumptions (`requires_primary_or_unique_key`, `requires_target_unique_enforcement`,
  `preserves_no_pk_duplicates`), and trigger assumptions (`fires_in_order`,
  `net_state_only`, `reconciled_before_exposure`, `none`).
- Two `decide.Rung` ladders climbed via `decide.climb(work, rungs, facts)`:
  1. `apply_contract_rungs`: chooses between `batch_net_merge`,
     `staged_restored_batch`, `transactional_ordered`,
     `keyless_multiset_contract`, and `native_endpoint_contract` using measured
     write, constraint/trigger restoration, pre-exposure validation, and
     expected recovery/rollback costs.
  2. `exposure_gate_rungs`: gates exposure of an applied batch so that no-PK
     duplicates, cross-table transactions, and targets without unique
     enforcement cannot silently inherit transactional guarantees from a batch
     path.
- All bounds and cost estimates are derived from measured telemetry readings
  and their observed spread (mean + sample standard deviation margin), never
  from hardcoded magic numbers or swallowed errors.
- Pure UNIX style output with 4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import hashlib
import json
import math
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from . import decide, durable, guarantees, lease

# -----------------------------------------------------------------------------
# Care Badges and Core Enums
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


class ApplyContractMode(str, enum.Enum):
    """Apply execution modes, each governed by an explicit contract."""

    BATCH_NET_MERGE = "batch_net_merge"
    STAGED_RESTORED_BATCH = "staged_restored_batch"
    TRANSACTIONAL_ORDERED = "transactional_ordered"
    KEYLESS_MULTISET_CONTRACT = "keyless_multiset_contract"
    NATIVE_ENDPOINT_CONTRACT = "native_endpoint_contract"


class IntegrityAssumption(str, enum.Enum):
    """How referential and transactional integrity hold during and after apply."""

    SOURCE_TRANSACTION_ATOMIC = "source_transaction_atomic"
    SINGLE_TABLE_NET_BATCH = "single_table_net_batch"
    STAGED_RESTORED_BEFORE_EXPOSURE = "staged_restored_before_exposure"
    MULTISET_ATOMIC_EPOCH = "multiset_atomic_epoch"
    IDEMPOTENT_EVENTUAL_BATCH = "idempotent_eventual_batch"


class OrderingAssumption(str, enum.Enum):
    """How change ordering is preserved across statements and keys."""

    COMMIT_ORDER = "commit_order"
    PER_KEY_NET = "per_key_net"
    EPOCH_BARRIER = "epoch_barrier"
    MULTISET_LOG_ORDER = "multiset_log_order"
    IDEMPOTENT_TOKEN_ORDER = "idempotent_token_order"


class KeyAssumption(str, enum.Enum):
    """What key identity and target uniqueness enforcement the mode assumes."""

    ENFORCED_UNIQUE_KEY = "enforced_unique_key"
    VERIFIED_KEY_BEFORE_EXPOSURE = "verified_key_before_exposure"
    KEY_OR_FULL_ROW_MATCH = "key_or_full_row_match"
    KEYLESS_MULTISET_MULTIPLICITY = "keyless_multiset_multiplicity"
    ENDPOINT_TOKEN_IDENTITY = "endpoint_token_identity"


class TriggerAssumption(str, enum.Enum):
    """How target database triggers and side-effect hooks behave under the mode."""

    FIRES_IN_COMMIT_ORDER = "fires_in_commit_order"
    NET_STATE_ONLY = "net_state_only"
    RECONCILED_BEFORE_EXPOSURE = "reconciled_before_exposure"
    NO_ROW_TRIGGERS = "no_row_triggers"


class ExposureGateVerdict(str, enum.Enum):
    """Outcome of the pre-exposure guarantee inheritance check."""

    BLOCK_SILENT_GUARANTEE_INHERITANCE = "block_silent_guarantee_inheritance"
    RESTORE_AND_VERIFY_BEFORE_EXPOSURE = "restore_and_verify_before_exposure"
    EXPOSE_VERIFIED_CONTRACT = "expose_verified_contract"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class ApplyContractError(RuntimeError):
    """Base exception for apply contract costing and guarantee violations."""


class SilentGuaranteeInheritanceError(ApplyContractError):
    """Raised when a batch path attempts to silently inherit transactional guarantees."""


class UnmeasuredContractInputError(ApplyContractError):
    """Raised when required telemetry readings are missing or invalid."""


class UnsafeEndpointHostError(ApplyContractError):
    """Raised when an endpoint host is outside approved safe test ranges."""


SAFE_ENDPOINT_PREFIXES = ("127.", "10.0.0.", "localhost", "example.com")


def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host is non-empty and within safe test ranges."""
    cleaned = str(host or "").strip()
    if not cleaned:
        raise UnsafeEndpointHostError("endpoint host must not be empty")
    if not any(cleaned == p or cleaned.startswith(p) for p in SAFE_ENDPOINT_PREFIXES):
        raise UnsafeEndpointHostError(
            f"endpoint host '{cleaned}' is outside approved safe ranges"
        )
    return cleaned


# -----------------------------------------------------------------------------
# Explicit Contract Declarations per Apply Mode
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ApplyModeContract:
    """Explicit integrity, ordering, key, and trigger contract for an apply mode."""

    mode: ApplyContractMode
    integrity: IntegrityAssumption
    ordering: OrderingAssumption
    key_assumption: KeyAssumption
    trigger_assumption: TriggerAssumption
    requires_primary_or_unique_key: bool
    requires_target_unique_enforcement: bool
    preserves_no_pk_duplicates: bool
    preserves_cross_table_atomicity_in_flight: bool
    can_restore_guarantees_before_exposure: bool
    fires_intermediate_triggers: bool
    requires_post_batch_validation: bool
    guarantee_levels: Tuple[Tuple[str, str], ...]

    def guarantee_map(self) -> Dict[str, str]:
        return dict(self.guarantee_levels)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode.value,
            "integrity": self.integrity.value,
            "ordering": self.ordering.value,
            "key_assumption": self.key_assumption.value,
            "trigger_assumption": self.trigger_assumption.value,
            "requires_primary_or_unique_key": self.requires_primary_or_unique_key,
            "requires_target_unique_enforcement": self.requires_target_unique_enforcement,
            "preserves_no_pk_duplicates": self.preserves_no_pk_duplicates,
            "preserves_cross_table_atomicity_in_flight": (
                self.preserves_cross_table_atomicity_in_flight
            ),
            "can_restore_guarantees_before_exposure": (
                self.can_restore_guarantees_before_exposure
            ),
            "fires_intermediate_triggers": self.fires_intermediate_triggers,
            "requires_post_batch_validation": self.requires_post_batch_validation,
            "guarantees": self.guarantee_map(),
        }


APPLY_CONTRACTS: Dict[ApplyContractMode, ApplyModeContract] = {
    ApplyContractMode.TRANSACTIONAL_ORDERED: ApplyModeContract(
        mode=ApplyContractMode.TRANSACTIONAL_ORDERED,
        integrity=IntegrityAssumption.SOURCE_TRANSACTION_ATOMIC,
        ordering=OrderingAssumption.COMMIT_ORDER,
        key_assumption=KeyAssumption.KEY_OR_FULL_ROW_MATCH,
        trigger_assumption=TriggerAssumption.FIRES_IN_COMMIT_ORDER,
        requires_primary_or_unique_key=False,
        requires_target_unique_enforcement=False,
        preserves_no_pk_duplicates=True,
        preserves_cross_table_atomicity_in_flight=True,
        can_restore_guarantees_before_exposure=True,
        fires_intermediate_triggers=True,
        requires_post_batch_validation=False,
        guarantee_levels=(
            ("correct", "byte exact"),
            ("consistent", "as of one moment"),
            ("idempotent", "converges"),
            ("exactly once", "exactly once"),
            ("resumable", "goes on"),
        ),
    ),
    ApplyContractMode.BATCH_NET_MERGE: ApplyModeContract(
        mode=ApplyContractMode.BATCH_NET_MERGE,
        integrity=IntegrityAssumption.SINGLE_TABLE_NET_BATCH,
        ordering=OrderingAssumption.PER_KEY_NET,
        key_assumption=KeyAssumption.ENFORCED_UNIQUE_KEY,
        trigger_assumption=TriggerAssumption.NET_STATE_ONLY,
        requires_primary_or_unique_key=True,
        requires_target_unique_enforcement=True,
        preserves_no_pk_duplicates=False,
        preserves_cross_table_atomicity_in_flight=False,
        can_restore_guarantees_before_exposure=True,
        fires_intermediate_triggers=False,
        requires_post_batch_validation=True,
        guarantee_levels=(
            ("correct", "proved"),
            ("consistent", "per range"),
            ("idempotent", "converges"),
            ("exactly once", "exactly once"),
            ("resumable", "goes on"),
        ),
    ),
    ApplyContractMode.STAGED_RESTORED_BATCH: ApplyModeContract(
        mode=ApplyContractMode.STAGED_RESTORED_BATCH,
        integrity=IntegrityAssumption.STAGED_RESTORED_BEFORE_EXPOSURE,
        ordering=OrderingAssumption.EPOCH_BARRIER,
        key_assumption=KeyAssumption.VERIFIED_KEY_BEFORE_EXPOSURE,
        trigger_assumption=TriggerAssumption.RECONCILED_BEFORE_EXPOSURE,
        requires_primary_or_unique_key=True,
        requires_target_unique_enforcement=False,
        preserves_no_pk_duplicates=False,
        preserves_cross_table_atomicity_in_flight=False,
        can_restore_guarantees_before_exposure=True,
        fires_intermediate_triggers=False,
        requires_post_batch_validation=True,
        guarantee_levels=(
            ("correct", "byte exact"),
            ("consistent", "as of one moment"),
            ("idempotent", "converges"),
            ("exactly once", "exactly once"),
            ("resumable", "goes on"),
        ),
    ),
    ApplyContractMode.KEYLESS_MULTISET_CONTRACT: ApplyModeContract(
        mode=ApplyContractMode.KEYLESS_MULTISET_CONTRACT,
        integrity=IntegrityAssumption.MULTISET_ATOMIC_EPOCH,
        ordering=OrderingAssumption.MULTISET_LOG_ORDER,
        key_assumption=KeyAssumption.KEYLESS_MULTISET_MULTIPLICITY,
        trigger_assumption=TriggerAssumption.NO_ROW_TRIGGERS,
        requires_primary_or_unique_key=False,
        requires_target_unique_enforcement=False,
        preserves_no_pk_duplicates=True,
        preserves_cross_table_atomicity_in_flight=False,
        can_restore_guarantees_before_exposure=True,
        fires_intermediate_triggers=False,
        requires_post_batch_validation=True,
        guarantee_levels=(
            ("correct", "byte exact"),
            ("consistent", "per range"),
            ("idempotent", "converges"),
            ("exactly once", "exactly once"),
            ("resumable", "goes on"),
        ),
    ),
    ApplyContractMode.NATIVE_ENDPOINT_CONTRACT: ApplyModeContract(
        mode=ApplyContractMode.NATIVE_ENDPOINT_CONTRACT,
        integrity=IntegrityAssumption.IDEMPOTENT_EVENTUAL_BATCH,
        ordering=OrderingAssumption.IDEMPOTENT_TOKEN_ORDER,
        key_assumption=KeyAssumption.ENDPOINT_TOKEN_IDENTITY,
        trigger_assumption=TriggerAssumption.NO_ROW_TRIGGERS,
        requires_primary_or_unique_key=False,
        requires_target_unique_enforcement=False,
        preserves_no_pk_duplicates=False,
        preserves_cross_table_atomicity_in_flight=False,
        can_restore_guarantees_before_exposure=False,
        fires_intermediate_triggers=False,
        requires_post_batch_validation=True,
        guarantee_levels=(
            ("correct", "proved"),
            ("consistent", "per range"),
            ("idempotent", "converges"),
            ("exactly once", "exactly once"),
            ("resumable", "goes on"),
        ),
    ),
}


def get_apply_contract(mode: Union[str, ApplyContractMode]) -> ApplyModeContract:
    """Return the declared `ApplyModeContract` for `mode`."""
    resolved = ApplyContractMode(mode) if not isinstance(mode, ApplyContractMode) else mode
    return APPLY_CONTRACTS[resolved]


# -----------------------------------------------------------------------------
# Measured Telemetry Readings & Spread-Derived Costing
# -----------------------------------------------------------------------------

def _mean_and_spread(samples: Sequence[float], field_name: str) -> Tuple[float, float]:
    """Compute sample mean and sample standard deviation from measured readings."""
    if not samples:
        raise UnmeasuredContractInputError(
            f"measured sample series '{field_name}' must contain at least one reading"
        )
    cleaned: List[float] = []
    for raw in samples:
        val = float(raw)
        if math.isnan(val) or math.isinf(val) or val < 0.0:
            raise UnmeasuredContractInputError(
                f"invalid reading {raw!r} in '{field_name}': must be finite and >= 0"
            )
        cleaned.append(val)
    n = len(cleaned)
    mean = sum(cleaned) / float(n)
    if n < 2:
        return mean, 0.0
    variance = sum((x - mean) ** 2 for x in cleaned) / float(n - 1)
    return mean, math.sqrt(max(0.0, variance))


@dataclass(frozen=True)
class ApplyTelemetryReadings:
    """Measured latency, collapse, restoration, validation, and fault readings.

    All bounds and cost estimates are derived directly from these measured
    samples and their sample spread (mean + 1 sigma), avoiding arbitrary magic
    constants.
    """

    raw_change_count: int
    distinct_key_count: int
    table_count: int
    row_apply_sec_per_row_samples: Tuple[float, ...]
    batch_write_sec_per_row_samples: Tuple[float, ...]
    batch_startup_sec_samples: Tuple[float, ...]
    constraint_restore_sec_samples: Tuple[float, ...] = (0.0,)
    validation_sec_per_row_samples: Tuple[float, ...] = (0.0,)
    rollback_reconcile_sec_samples: Tuple[float, ...] = (0.0,)
    observed_batch_failures: int = 0
    observed_batch_trials: int = 0
    observed_txn_failures: int = 0
    observed_txn_trials: int = 0

    def __post_init__(self) -> None:
        if int(self.raw_change_count) <= 0:
            raise UnmeasuredContractInputError("raw_change_count must be > 0")
        if int(self.distinct_key_count) < 0:
            raise UnmeasuredContractInputError("distinct_key_count must be >= 0")
        if int(self.table_count) <= 0:
            raise UnmeasuredContractInputError("table_count must be > 0")
        if int(self.observed_batch_failures) < 0 or int(self.observed_batch_trials) < 0:
            raise UnmeasuredContractInputError("observed batch failure/trial counts must be >= 0")
        if int(self.observed_batch_failures) > int(self.observed_batch_trials):
            raise UnmeasuredContractInputError(
                "observed_batch_failures cannot exceed observed_batch_trials"
            )
        if int(self.observed_txn_failures) < 0 or int(self.observed_txn_trials) < 0:
            raise UnmeasuredContractInputError("observed txn failure/trial counts must be >= 0")
        if int(self.observed_txn_failures) > int(self.observed_txn_trials):
            raise UnmeasuredContractInputError(
                "observed_txn_failures cannot exceed observed_txn_trials"
            )
        _mean_and_spread(
            self.row_apply_sec_per_row_samples, "row_apply_sec_per_row_samples"
        )
        _mean_and_spread(
            self.batch_write_sec_per_row_samples, "batch_write_sec_per_row_samples"
        )
        _mean_and_spread(
            self.batch_startup_sec_samples, "batch_startup_sec_samples"
        )
        _mean_and_spread(
            self.constraint_restore_sec_samples, "constraint_restore_sec_samples"
        )
        _mean_and_spread(
            self.validation_sec_per_row_samples, "validation_sec_per_row_samples"
        )
        _mean_and_spread(
            self.rollback_reconcile_sec_samples, "rollback_reconcile_sec_samples"
        )

    @property
    def net_row_count(self) -> int:
        """Net rows after key collapsing (or raw_change_count if keyless)."""
        if self.distinct_key_count <= 0:
            return int(self.raw_change_count)
        return min(int(self.raw_change_count), int(self.distinct_key_count))

    @property
    def key_collapse_ratio(self) -> float:
        """Ratio of net rows to raw changes in (0, 1]. Lower means more collapsing."""
        return float(self.net_row_count) / float(self.raw_change_count)

    @property
    def batch_failure_probability(self) -> float:
        """Observed batch failure probability from measured trials."""
        if self.observed_batch_trials <= 0:
            return 0.0
        return float(self.observed_batch_failures) / float(self.observed_batch_trials)

    @property
    def txn_failure_probability(self) -> float:
        """Observed transactional failure probability from measured trials."""
        if self.observed_txn_trials <= 0:
            return 0.0
        return float(self.observed_txn_failures) / float(self.observed_txn_trials)


@dataclass(frozen=True)
class ApplyCostBreakdown:
    """Complete cost breakdown for a candidate apply mode, including recovery and validation."""

    mode: ApplyContractMode
    execution_write_seconds: float
    constraint_and_trigger_restore_seconds: float
    validation_before_exposure_seconds: float
    expected_recovery_seconds: float
    spread_margin_seconds: float
    total_expected_seconds: float
    effective_rows_written: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode.value,
            "execution_write_seconds": round(self.execution_write_seconds, 6),
            "constraint_and_trigger_restore_seconds": round(
                self.constraint_and_trigger_restore_seconds, 6
            ),
            "validation_before_exposure_seconds": round(
                self.validation_before_exposure_seconds, 6
            ),
            "expected_recovery_seconds": round(self.expected_recovery_seconds, 6),
            "spread_margin_seconds": round(self.spread_margin_seconds, 6),
            "total_expected_seconds": round(self.total_expected_seconds, 6),
            "effective_rows_written": self.effective_rows_written,
        }


def cost_apply_contracts(
    readings: ApplyTelemetryReadings,
    *,
    requires_constraint_restore: bool = False,
    requires_pre_exposure_unique_proof: bool = False,
) -> Dict[str, ApplyCostBreakdown]:
    """Compute spread-bounded total apply cost for every contract mode.

    Includes:
    1. Execution write cost (row-by-row for transactional; net-collapsed or
       raw-multiset for batch modes).
    2. Constraint and trigger restoration cost before exposure.
    3. Pre-exposure validation cost (uniqueness proof, multiset multiplicity
       verification, or post-batch digest).
    4. Expected batch recovery/rollback + transactional replay cost weighted by
       observed failure rate.
    5. Measured spread margin (1 standard deviation of the component readings).
    """
    raw_rows = float(readings.raw_change_count)
    net_rows = float(readings.net_row_count)
    tables = float(readings.table_count)

    row_mean, row_std = _mean_and_spread(
        readings.row_apply_sec_per_row_samples, "row_apply_sec_per_row_samples"
    )
    batch_mean, batch_std = _mean_and_spread(
        readings.batch_write_sec_per_row_samples, "batch_write_sec_per_row_samples"
    )
    start_mean, start_std = _mean_and_spread(
        readings.batch_startup_sec_samples, "batch_startup_sec_samples"
    )
    restore_mean, restore_std = _mean_and_spread(
        readings.constraint_restore_sec_samples, "constraint_restore_sec_samples"
    )
    val_mean, val_std = _mean_and_spread(
        readings.validation_sec_per_row_samples, "validation_sec_per_row_samples"
    )
    reconcile_mean, reconcile_std = _mean_and_spread(
        readings.rollback_reconcile_sec_samples, "rollback_reconcile_sec_samples"
    )

    p_batch = readings.batch_failure_probability
    p_txn = readings.txn_failure_probability

    # 1. Transactional ordered: applies every change in commit order, constraints
    #    and triggers fire inline; recovery only replays the failed transaction slice.
    txn_write = raw_rows * row_mean
    txn_spread = raw_rows * row_std
    txn_recovery = p_txn * (reconcile_mean + (raw_rows / max(tables, 1.0)) * row_mean)
    txn_total = txn_write + txn_recovery + txn_spread

    # 2. Batch net merge: collapses keys to net_rows, pays per-table startup,
    #    optional constraint restore, net-row validation, and full-batch rollback
    #    + transactional fallback replay on batch failure.
    net_write = (tables * start_mean) + (net_rows * batch_mean)
    net_restore = (tables * restore_mean) if requires_constraint_restore else 0.0
    net_val = net_rows * val_mean
    net_Attempt = net_write + net_restore + net_val
    net_recovery = p_batch * (net_Attempt + reconcile_mean + txn_write)
    net_spread = (
        (tables * start_std)
        + (net_rows * batch_std)
        + ((tables * restore_std) if requires_constraint_restore else 0.0)
        + (net_rows * val_std)
        + (p_batch * reconcile_std)
    )
    net_total = net_Attempt + net_recovery + net_spread

    # 3. Staged restored batch: writes net rows to staging, always pays constraint/index
    #    restoration and full uniqueness/integrity validation before exposure.
    staged_write = (tables * start_mean) + (net_rows * batch_mean)
    staged_restore = tables * restore_mean
    staged_val_rows = (
        (raw_rows + net_rows) if requires_pre_exposure_unique_proof else net_rows
    )
    staged_val = staged_val_rows * val_mean
    staged_attempt = staged_write + staged_restore + staged_val
    staged_recovery = p_batch * (staged_attempt + reconcile_mean + txn_write)
    staged_spread = (
        (tables * start_std)
        + (net_rows * batch_std)
        + (tables * restore_std)
        + (staged_val_rows * val_std)
        + (p_batch * reconcile_std)
    )
    staged_total = staged_attempt + staged_recovery + staged_spread

    # 4. Keyless multiset contract: cannot collapse keys because duplicate rows have
    #    distinct multiplicity; writes all raw_rows and validates exact multiset counts.
    multiset_write = (tables * start_mean) + (raw_rows * batch_mean)
    multiset_restore = (tables * restore_mean) if requires_constraint_restore else 0.0
    multiset_val = raw_rows * val_mean
    multiset_attempt = multiset_write + multiset_restore + multiset_val
    multiset_recovery = p_batch * (multiset_attempt + reconcile_mean + multiset_write)
    multiset_spread = (
        (tables * start_std)
        + (raw_rows * batch_std)
        + (raw_rows * val_std)
        + (p_batch * reconcile_std)
    )
    multiset_total = multiset_attempt + multiset_recovery + multiset_spread

    # 5. Native endpoint contract: idempotent batch write with post-batch validation
    #    and reconciliation cost.
    native_write = (tables * start_mean) + (net_rows * batch_mean)
    native_val = net_rows * val_mean
    native_attempt = native_write + native_val
    native_recovery = p_batch * (native_attempt + reconcile_mean)
    native_spread = (
        (tables * start_std)
        + (net_rows * batch_std)
        + (net_rows * val_std)
        + (p_batch * reconcile_std)
    )
    native_total = native_attempt + native_recovery + native_spread

    return {
        ApplyContractMode.TRANSACTIONAL_ORDERED.value: ApplyCostBreakdown(
            mode=ApplyContractMode.TRANSACTIONAL_ORDERED,
            execution_write_seconds=txn_write,
            constraint_and_trigger_restore_seconds=0.0,
            validation_before_exposure_seconds=0.0,
            expected_recovery_seconds=txn_recovery,
            spread_margin_seconds=txn_spread,
            total_expected_seconds=txn_total,
            effective_rows_written=int(raw_rows),
        ),
        ApplyContractMode.BATCH_NET_MERGE.value: ApplyCostBreakdown(
            mode=ApplyContractMode.BATCH_NET_MERGE,
            execution_write_seconds=net_write,
            constraint_and_trigger_restore_seconds=net_restore,
            validation_before_exposure_seconds=net_val,
            expected_recovery_seconds=net_recovery,
            spread_margin_seconds=net_spread,
            total_expected_seconds=net_total,
            effective_rows_written=int(net_rows),
        ),
        ApplyContractMode.STAGED_RESTORED_BATCH.value: ApplyCostBreakdown(
            mode=ApplyContractMode.STAGED_RESTORED_BATCH,
            execution_write_seconds=staged_write,
            constraint_and_trigger_restore_seconds=staged_restore,
            validation_before_exposure_seconds=staged_val,
            expected_recovery_seconds=staged_recovery,
            spread_margin_seconds=staged_spread,
            total_expected_seconds=staged_total,
            effective_rows_written=int(net_rows),
        ),
        ApplyContractMode.KEYLESS_MULTISET_CONTRACT.value: ApplyCostBreakdown(
            mode=ApplyContractMode.KEYLESS_MULTISET_CONTRACT,
            execution_write_seconds=multiset_write,
            constraint_and_trigger_restore_seconds=multiset_restore,
            validation_before_exposure_seconds=multiset_val,
            expected_recovery_seconds=multiset_recovery,
            spread_margin_seconds=multiset_spread,
            total_expected_seconds=multiset_total,
            effective_rows_written=int(raw_rows),
        ),
        ApplyContractMode.NATIVE_ENDPOINT_CONTRACT.value: ApplyCostBreakdown(
            mode=ApplyContractMode.NATIVE_ENDPOINT_CONTRACT,
            execution_write_seconds=native_write,
            constraint_and_trigger_restore_seconds=0.0,
            validation_before_exposure_seconds=native_val,
            expected_recovery_seconds=native_recovery,
            spread_margin_seconds=native_spread,
            total_expected_seconds=native_total,
            effective_rows_written=int(net_rows),
        ),
    }


# -----------------------------------------------------------------------------
# Decision Engine Ladders (decide.Rung)
# -----------------------------------------------------------------------------

def apply_contract_rungs() -> Tuple[decide.Rung, ...]:
    """The `decide.Rung` ladder for selecting an apply mode under explicit contracts.

    Every mode declares its integrity, ordering, key, and trigger assumptions as
    `decide.Need` predicates over measured facts. Batch merging is admitted only
    when all required guarantees can be maintained or restored before exposure
    and its full cost (including recovery and validation) beats transactional apply.
    """
    return (
        decide.Rung(
            "batch_net_merge",
            "net-change key-collapsed batch merge with enforced target uniqueness",
            gives=frozenset({"carried", "exact", "resume by key"}),
            needs=(
                decide.Need(
                    "primary or unique key without no-PK duplicate rows",
                    lambda f: bool(f.get("has_primary_or_unique_key"))
                    and not bool(f.get("has_no_pk_duplicates")),
                    "the table has no primary/unique key or holds no-PK duplicate rows"
                    " that key-collapsed batching would fold",
                ),
                decide.Need(
                    "target unique key enforcement on merge columns",
                    lambda f: bool(f.get("target_enforces_unique_key")),
                    "the target does not enforce uniqueness on the key, so batch upsert"
                    " cannot guarantee single-row identity",
                ),
                decide.Need(
                    "single-table scope or cross-table guarantees restored before exposure",
                    lambda f: (
                        not bool(f.get("cross_table_transaction"))
                        or (
                            bool(f.get("restores_guarantees_before_exposure"))
                            and not bool(f.get("cross_table_order_dependency"))
                        )
                    ),
                    "cross-table transaction or foreign-key cascade order cannot be"
                    " preserved by per-table net batch merging",
                ),
                decide.Need(
                    "no intermediate row-trigger side effects required",
                    lambda f: not bool(f.get("requires_intermediate_trigger_events")),
                    "target triggers require intermediate per-event transitions that"
                    " net-change batching collapses away",
                ),
                decide.Need(
                    "batch write plus restore, validation, and recovery cheaper than transactional",
                    lambda f: bool(f.get("batch_net_merge_cost_advantage", True)),
                    "batch net merge total cost including validation and expected"
                    " recovery exceeds transactional apply",
                ),
            ),
            because="net-change key collapsing lowers write volume while preserving enforced key state",
            footprint=1,
            leaves="a batch merge transaction",
        ),
        decide.Rung(
            "staged_restored_batch",
            "staged batch load with pre-exposure constraint, uniqueness, and trigger restoration",
            gives=frozenset({"carried", "exact", "consistent as of", "resume by chunk"}),
            needs=(
                decide.Need(
                    "isolated staging barrier before reader exposure",
                    lambda f: bool(f.get("supports_isolated_staging_before_exposure")),
                    "the target cannot isolate staged batches before reader exposure",
                ),
                decide.Need(
                    "deterministic key identity without no-PK duplicate ambiguity",
                    lambda f: bool(f.get("has_primary_or_unique_key"))
                    and not bool(f.get("has_no_pk_duplicates")),
                    "staged key restoration cannot disambiguate no-PK duplicate rows",
                ),
                decide.Need(
                    "target unique enforcement or pre-exposure uniqueness proof",
                    lambda f: bool(f.get("target_enforces_unique_key"))
                    or bool(f.get("pre_exposure_unique_proof")),
                    "the target neither enforces unique keys nor runs a pre-exposure"
                    " uniqueness proof query",
                ),
                decide.Need(
                    "coupled cross-table changes fenced in one pre-exposure epoch",
                    lambda f: not bool(f.get("cross_table_transaction"))
                    or bool(f.get("all_coupled_tables_in_epoch")),
                    "cross-table transaction spans tables outside the staged exposure epoch",
                ),
                decide.Need(
                    "triggers absent or reconciled before exposure",
                    lambda f: not bool(f.get("requires_intermediate_trigger_events"))
                    or bool(f.get("reconciles_triggers_before_exposure")),
                    "intermediate trigger side effects cannot be reconciled before exposure",
                ),
                decide.Need(
                    "staged write plus constraint restoration and validation within budget",
                    lambda f: bool(f.get("staged_restored_cost_advantage", True)),
                    "staged batch load plus constraint restoration and validation"
                    " costs more than transactional apply",
                ),
            ),
            because="pre-exposure staging restores constraints and uniqueness before any reader sees the batch",
            footprint=2,
            leaves="a staging table swapped at exposure barrier",
        ),
        decide.Rung(
            "transactional_ordered",
            "commit-ordered transactional row-by-row apply",
            gives=frozenset({"carried", "exact", "byte exact", "consistent as of", "resume by key"}),
            needs=(
                decide.Need(
                    "interactive transactional DML on target",
                    lambda f: bool(f.get("transactional_dml_supported", True)),
                    "the target endpoint does not support interactive transactional DML",
                ),
                decide.Need(
                    "row identity via primary key or exact single-row match",
                    lambda f: bool(f.get("has_primary_or_unique_key", True))
                    or bool(f.get("supports_single_row_cursor_match", False)),
                    "the table has no key and the target cannot address a single duplicate row",
                ),
            ),
            because="commit-ordered transactional execution preserves cross-table atomicity, triggers, and constraints in flight",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "keyless_multiset_contract",
            "keyless multiset exact-multiplicity batch contract with count-delta verification",
            gives=frozenset({"carried", "keyless", "byte exact", "resume by chunk"}),
            needs=(
                decide.Need(
                    "keyless table or duplicate rows requiring multiset preservation",
                    lambda f: not bool(f.get("has_primary_or_unique_key"))
                    or bool(f.get("has_no_pk_duplicates")),
                    "the table has an enforced unique key and no duplicate rows",
                ),
                decide.Need(
                    "exact-multiplicity mutation and atomic effect journal on target",
                    lambda f: bool(f.get("supports_multiset_multiplicity_apply")),
                    "the target cannot apply exact-multiplicity changes with an atomic"
                    " effect journal",
                ),
                decide.Need(
                    "no unguarded cross-table foreign-key cascade",
                    lambda f: not bool(f.get("cross_table_transaction")),
                    "keyless multiset batching cannot preserve cross-table cascade atomicity",
                ),
                decide.Need(
                    "post-batch multiset multiplicity verification enabled",
                    lambda f: bool(f.get("multiset_validation_enabled", True)),
                    "keyless multiset apply requires post-batch multiplicity verification",
                ),
            ),
            because="keyless duplicate rows require an explicit multiset multiplicity contract",
            footprint=1,
            leaves="an atomic multiset effect journal entry",
        ),
        decide.Rung(
            "native_endpoint_contract",
            "provider-native non-relational idempotent batch contract with post-batch validation",
            gives=frozenset({"carried", "exact", "resume by chunk"}),
            needs=(
                decide.Need(
                    "native or non-unique-enforcing endpoint contract declared",
                    lambda f: bool(f.get("is_native_or_non_enforcing_endpoint")),
                    "the target is a relational transactional engine, not a native"
                    " non-enforcing endpoint",
                ),
                decide.Need(
                    "no uncompensated cross-table transaction atomicity demanded",
                    lambda f: not bool(f.get("cross_table_transaction")),
                    "a native non-transactional endpoint cannot execute cross-table"
                    " atomic transactions",
                ),
                decide.Need(
                    "no no-PK duplicate collapse under endpoint token identity",
                    lambda f: not bool(f.get("has_no_pk_duplicates")),
                    "native endpoint token identity cannot preserve unkeyed duplicate rows",
                ),
                decide.Need(
                    "idempotency token and post-batch validation enabled",
                    lambda f: bool(f.get("has_idempotency_version_token"))
                    and bool(f.get("native_validation_enabled", True)),
                    "native endpoint contract requires an idempotency version token and"
                    " post-batch validation",
                ),
            ),
            because="native endpoints operate under an explicit idempotent batch contract without false relational guarantees",
            footprint=1,
            leaves="an idempotent batch token and validation receipt",
        ),
    )


def exposure_gate_rungs() -> Tuple[decide.Rung, ...]:
    """The `decide.Rung` ladder gating whether an apply path may expose its state.

    Ensures that no-PK duplicates, cross-table transactions, and targets without
    unique enforcement cannot silently inherit transactional guarantees from a
    batch path.
    """
    return (
        decide.Rung(
            "block_silent_guarantee_inheritance",
            "block exposure because batch path violates or silently claims unheld transactional guarantees",
            needs=(
                decide.Need(
                    "unheld transactional guarantee or unverified batch invariant detected",
                    lambda f: bool(f.get("silent_inheritance_violation")),
                    "no silent guarantee inheritance violation was detected",
                ),
            ),
            because="a batch or non-enforcing path must never silently inherit transactional guarantees",
        ),
        decide.Rung(
            "restore_and_verify_before_exposure",
            "hold exposure until constraints, uniqueness, and multiplicity proofs complete",
            needs=(
                decide.Need(
                    "no silent guarantee inheritance violation",
                    lambda f: not bool(f.get("silent_inheritance_violation")),
                    "silent guarantee inheritance violation requires immediate block",
                ),
                decide.Need(
                    "pre-exposure constraint restoration or uniqueness/multiplicity verification pending",
                    lambda f: bool(f.get("requires_pre_exposure_restoration"))
                    and not bool(f.get("pre_exposure_verification_completed")),
                    "pre-exposure restoration is either unnecessary or already completed",
                ),
            ),
            because="batch merging may expose rows only after required guarantees are restored and proved",
        ),
        decide.Rung(
            "expose_verified_contract",
            "expose committed state under its verified apply contract",
            needs=(
                decide.Need(
                    "no silent guarantee inheritance violation",
                    lambda f: not bool(f.get("silent_inheritance_violation")),
                    "cannot expose state when silent guarantee inheritance is violated",
                ),
                decide.Need(
                    "all required pre-exposure restorations and proofs completed",
                    lambda f: not bool(f.get("requires_pre_exposure_restoration"))
                    or bool(f.get("pre_exposure_verification_completed")),
                    "pre-exposure constraint restoration or validation has not completed",
                ),
            ),
            because="every declared integrity, ordering, key, and trigger condition is satisfied",
        ),
    )


# -----------------------------------------------------------------------------
# Contract Selection & Silent Inheritance Verification
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ApplyContractDecision:
    """Result of choosing and costing an apply mode contract."""

    choice: decide.Choice
    selected_mode: Optional[ApplyContractMode]
    contract: Optional[ApplyModeContract]
    cost_breakdown: Optional[ApplyCostBreakdown]
    all_costs: Dict[str, ApplyCostBreakdown]
    care_badge: CareBadge
    summary: str

    def format_unix_line(self) -> str:
        mode_str = self.selected_mode.value if self.selected_mode else "none"
        cost_str = (
            f"{self.cost_breakdown.total_expected_seconds:.4f}s"
            if self.cost_breakdown is not None
            else "n/a"
        )
        return (
            f"[{self.care_badge.value}] mode={mode_str} expected_cost={cost_str}"
            f" reason={self.summary}"
        )


def choose_apply_contract(
    facts: Union[Mapping[str, Any], decide.Facts],
    readings: Optional[ApplyTelemetryReadings] = None,
    *,
    need: Sequence[str] = (),
) -> ApplyContractDecision:
    """Choose and cost the apply contract via `decide.climb`.

    Ranks batch merging ahead of transactional apply only when:
    1. Every required integrity, ordering, key, and trigger assumption holds or
       is restored before exposure, AND
    2. Full measured cost (write + constraint/trigger restore + validation +
       expected recovery + spread margin) is lower than transactional apply.
    """
    raw_facts: Dict[str, Any] = dict(facts)
    cost_map_obj: Dict[str, ApplyCostBreakdown] = {}
    costs_for_decide: Optional[decide.Costs] = None

    if readings is not None:
        needs_restore = bool(raw_facts.get("cross_table_transaction")) and bool(
            raw_facts.get("restores_guarantees_before_exposure")
        )
        needs_unique_proof = not bool(
            raw_facts.get("target_enforces_unique_key", True)
        ) and bool(raw_facts.get("pre_exposure_unique_proof"))
        cost_map_obj = cost_apply_contracts(
            readings,
            requires_constraint_restore=needs_restore,
            requires_pre_exposure_unique_proof=needs_unique_proof,
        )
        txn_cost = cost_map_obj[
            ApplyContractMode.TRANSACTIONAL_ORDERED.value
        ].total_expected_seconds
        net_cost = cost_map_obj[
            ApplyContractMode.BATCH_NET_MERGE.value
        ].total_expected_seconds
        staged_cost = cost_map_obj[
            ApplyContractMode.STAGED_RESTORED_BATCH.value
        ].total_expected_seconds

        raw_facts.setdefault("batch_net_merge_cost_advantage", net_cost < txn_cost)
        raw_facts.setdefault("staged_restored_cost_advantage", staged_cost < txn_cost)

        costs_for_decide = decide.Costs(
            {
                mode_name: breakdown.total_expected_seconds
                for mode_name, breakdown in cost_map_obj.items()
            }
        )

    f = facts if isinstance(facts, decide.Facts) else decide.Facts(raw_facts)
    for k, v in raw_facts.items():
        if k not in f:
            f[k] = v

    rungs = apply_contract_rungs()
    choice = decide.climb(
        "apply mode contract",
        rungs,
        f,
        need=tuple(need),
        costs=costs_for_decide,
    )

    if choice.path is None:
        return ApplyContractDecision(
            choice=choice,
            selected_mode=None,
            contract=None,
            cost_breakdown=None,
            all_costs=cost_map_obj,
            care_badge=CareBadge.ACTION,
            summary=f"no apply contract satisfies workload guarantees: {choice.reason}",
        )

    selected_mode = ApplyContractMode(choice.path)
    contract = get_apply_contract(selected_mode)
    chosen_cost = cost_map_obj.get(selected_mode.value)

    if selected_mode in (
        ApplyContractMode.KEYLESS_MULTISET_CONTRACT,
        ApplyContractMode.NATIVE_ENDPOINT_CONTRACT,
        ApplyContractMode.STAGED_RESTORED_BATCH,
    ):
        badge = CareBadge.REVIEW if contract.requires_post_batch_validation else CareBadge.PASS
    else:
        badge = CareBadge.PASS

    return ApplyContractDecision(
        choice=choice,
        selected_mode=selected_mode,
        contract=contract,
        cost_breakdown=chosen_cost,
        all_costs=cost_map_obj,
        care_badge=badge,
        summary=choice.reason,
    )


@dataclass(frozen=True)
class ExposureGateReport:
    """Audit report from evaluating whether a candidate apply path can expose state."""

    choice: decide.Choice
    verdict: ExposureGateVerdict
    proposed_mode: ApplyContractMode
    contract: ApplyModeContract
    violations: Tuple[str, ...]
    restoration_steps_required: Tuple[str, ...]
    care_badge: CareBadge

    def format_unix_line(self) -> str:
        v_str = "; ".join(self.violations) if self.violations else "none"
        return (
            f"[{self.care_badge.value}] mode={self.proposed_mode.value}"
            f" verdict={self.verdict.value} violations={v_str}"
        )


def detect_guarantee_inheritance_violations(
    proposed_mode: Union[str, ApplyContractMode],
    workload_facts: Mapping[str, Any],
    *,
    claimed_transactional_guarantees: bool = False,
) -> Tuple[List[str], List[str]]:
    """Detect any attempt by a batch or non-enforcing path to silently inherit transactional guarantees.

    Specifically checks:
    1. No-PK duplicates (`has_no_pk_duplicates` or `not has_primary_or_unique_key`)
       on key-collapsed or token-collapsed batch paths.
    2. Cross-table transactions (`cross_table_transaction`) on per-table batch
       paths without pre-exposure atomic epoch restoration, or with cross-table
       cascade order dependencies.
    3. Target without unique enforcement (`not target_enforces_unique_key`)
       using batch upsert/merge without pre-exposure uniqueness proof, or
       claiming transactional uniqueness guarantees.
    4. Required intermediate row triggers (`requires_intermediate_trigger_events`)
       on net-change batch paths that suppress intermediate transitions.
    5. Any non-transactional mode claiming `claimed_transactional_guarantees=True`
       without completing pre-exposure restoration and verification.
    """
    contract = get_apply_contract(proposed_mode)
    mode = contract.mode
    violations: List[str] = []
    restorations: List[str] = []

    has_pk = bool(workload_facts.get("has_primary_or_unique_key", True))
    has_no_pk_dups = bool(workload_facts.get("has_no_pk_duplicates", False))
    cross_table = bool(workload_facts.get("cross_table_transaction", False))
    cross_order_dep = bool(workload_facts.get("cross_table_order_dependency", False))
    target_unique = bool(workload_facts.get("target_enforces_unique_key", True))
    pre_unique_proof = bool(workload_facts.get("pre_exposure_unique_proof", False))
    restores_before_exp = bool(workload_facts.get("restores_guarantees_before_exposure", False))
    all_coupled_in_epoch = bool(workload_facts.get("all_coupled_tables_in_epoch", False))
    needs_triggers = bool(workload_facts.get("requires_intermediate_trigger_events", False))
    reconciles_triggers = bool(workload_facts.get("reconciles_triggers_before_exposure", False))

    # 1. No-PK duplicates check
    if (not has_pk or has_no_pk_dups) and not contract.preserves_no_pk_duplicates:
        violations.append(
            f"{mode.value} cannot preserve no-PK duplicate rows (requires"
            " keyless_multiset_contract or transactional cursor match)"
        )

    # 2. Cross-table transactions check
    if cross_table and not contract.preserves_cross_table_atomicity_in_flight:
        if mode == ApplyContractMode.BATCH_NET_MERGE:
            if not restores_before_exp or cross_order_dep:
                violations.append(
                    "batch_net_merge splits cross-table transactions across per-table"
                    " runs without restoring cross-table atomicity before exposure"
                )
            else:
                restorations.append("verify deferred foreign-key constraints before commit")
        elif mode == ApplyContractMode.STAGED_RESTORED_BATCH:
            if not all_coupled_in_epoch:
                violations.append(
                    "staged_restored_batch does not fence all coupled cross-table"
                    " changes in one atomic exposure epoch"
                )
            else:
                restorations.append("atomic multi-table staging swap and FK validation")
        else:
            violations.append(
                f"{mode.value} does not support cross-table transaction atomicity"
            )

    # 3. Target without unique enforcement check
    if not target_unique:
        if contract.requires_target_unique_enforcement:
            violations.append(
                f"{mode.value} requires target unique key enforcement; a target without"
                " unique enforcement cannot silently inherit transactional key uniqueness"
            )
        elif mode == ApplyContractMode.STAGED_RESTORED_BATCH:
            if not pre_unique_proof:
                violations.append(
                    "staged_restored_batch on a target without unique enforcement"
                    " requires pre_exposure_unique_proof before exposure"
                )
            else:
                restorations.append("run pre-exposure duplicate-key verification query")
        elif mode == ApplyContractMode.TRANSACTIONAL_ORDERED and claimed_transactional_guarantees:
            if not pre_unique_proof:
                violations.append(
                    "target lacks unique constraint enforcement; transactional apply"
                    " cannot claim enforced unique-key guarantees without uniqueness proof"
                )

    # 4. Trigger assumption check
    if needs_triggers and not contract.fires_intermediate_triggers:
        if mode == ApplyContractMode.STAGED_RESTORED_BATCH and reconciles_triggers:
            restorations.append("reconcile derived trigger state before exposure")
        else:
            violations.append(
                f"{mode.value} suppresses intermediate state transitions required by"
                " target triggers"
            )

    # 5. Claiming transactional guarantees from a batch/native/keyless path
    if claimed_transactional_guarantees and mode != ApplyContractMode.TRANSACTIONAL_ORDERED:
        if not contract.can_restore_guarantees_before_exposure:
            violations.append(
                f"{mode.value} cannot inherit transactional guarantees (eventual/token"
                " contract only)"
            )
        elif mode == ApplyContractMode.BATCH_NET_MERGE and (
            cross_table or not target_unique or has_no_pk_dups or not has_pk
        ):
            violations.append(
                "batch_net_merge cannot silently inherit transactional guarantees"
                " under cross-table, no-PK duplicate, or non-unique-enforcing conditions"
            )
        elif contract.requires_post_batch_validation:
            restorations.append("complete post-batch validation before exposing batch")

    if contract.requires_post_batch_validation and not violations:
        if "complete post-batch validation before exposing batch" not in restorations:
            restorations.append("complete post-batch validation before exposing batch")

    return violations, restorations


def evaluate_exposure_gate(
    proposed_mode: Union[str, ApplyContractMode],
    workload_facts: Union[Mapping[str, Any], decide.Facts],
    *,
    claimed_transactional_guarantees: bool = False,
    raise_on_violation: bool = False,
) -> ExposureGateReport:
    """Evaluate the exposure gate via `decide.climb` over `exposure_gate_rungs()`.

    Blocks any batch path that would silently inherit unheld transactional
    guarantees on no-PK duplicates, cross-table transactions, or targets
    without unique enforcement.
    """
    raw = dict(workload_facts)
    contract = get_apply_contract(proposed_mode)
    violations, restorations = detect_guarantee_inheritance_violations(
        contract.mode,
        raw,
        claimed_transactional_guarantees=claimed_transactional_guarantees,
    )

    raw.setdefault("silent_inheritance_violation", bool(violations))
    raw.setdefault("requires_pre_exposure_restoration", bool(restorations))
    raw.setdefault(
        "pre_exposure_verification_completed",
        bool(raw.get("pre_exposure_verification_completed", False)),
    )

    f = workload_facts if isinstance(workload_facts, decide.Facts) else decide.Facts(raw)
    for k, v in raw.items():
        if k not in f:
            f[k] = v

    rungs = exposure_gate_rungs()
    choice = decide.climb("batch exposure guarantee gate", rungs, f)
    verdict = ExposureGateVerdict(choice.path)

    if verdict == ExposureGateVerdict.BLOCK_SILENT_GUARANTEE_INHERITANCE:
        badge = CareBadge.ACTION
        if raise_on_violation:
            raise SilentGuaranteeInheritanceError(
                f"blocked {contract.mode.value}: {'; '.join(violations or [choice.reason])}"
            )
    elif verdict == ExposureGateVerdict.RESTORE_AND_VERIFY_BEFORE_EXPOSURE:
        badge = CareBadge.REVIEW
    else:
        badge = CareBadge.PASS

    return ExposureGateReport(
        choice=choice,
        verdict=verdict,
        proposed_mode=contract.mode,
        contract=contract,
        violations=tuple(violations),
        restoration_steps_required=tuple(restorations),
        care_badge=badge,
    )


# -----------------------------------------------------------------------------
# Sandbox Execution Simulation Proving No-PK, Cross-Table, and Non-Unique Behavior
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class SandboxApplyResult:
    """Outcome of applying a transaction batch under a chosen contract in sandbox."""

    mode: ApplyContractMode
    exposed_rows_by_table: Dict[str, Tuple[Dict[str, Any], ...]]
    source_expected_rows_by_table: Dict[str, Tuple[Dict[str, Any], ...]]
    matches_source_truth: bool
    exposure_gate: ExposureGateReport
    cost_breakdown: ApplyCostBreakdown
    care_badge: CareBadge


def simulate_contract_execution(
    changes: Sequence[Dict[str, Any]],
    *,
    workload_facts: Mapping[str, Any],
    readings: ApplyTelemetryReadings,
    initial_rows_by_table: Optional[Mapping[str, Sequence[Dict[str, Any]]]] = None,
    force_mode: Optional[ApplyContractMode] = None,
    claimed_transactional_guarantees: bool = False,
    endpoint_host: str = "127.0.0.1",
) -> SandboxApplyResult:
    """Execute changes against an in-memory sandbox under the selected or forced contract.

    Demonstrates why batch net-merging corrupts state if forced onto no-PK
    duplicates, cross-table cascades, or non-unique-enforcing targets, and proves
    that `evaluate_exposure_gate` and `choose_apply_contract` prevent silent
    guarantee inheritance.
    """
    validate_safe_endpoint(endpoint_host)

    decision = choose_apply_contract(workload_facts, readings)
    mode = force_mode if force_mode is not None else decision.selected_mode
    if mode is None:
        raise ApplyContractError(f"no valid apply contract for workload: {decision.summary}")

    # Compute reference truth via commit-ordered transactional execution
    truth_tables: Dict[str, List[Dict[str, Any]]] = {
        t: [dict(r) for r in rows]
        for t, rows in (initial_rows_by_table or {}).items()
    }
    for change in changes:
        table = str(change["table"])
        op = str(change["op"])
        tbl_rows = truth_tables.setdefault(table, [])
        if op == "insert":
            tbl_rows.append(dict(change["values"]))
        elif op == "delete":
            match_pred = dict(change.get("key") or change.get("match") or {})
            limit_one = bool(change.get("single_row", True))
            removed = 0
            kept_rows: List[Dict[str, Any]] = []
            for r in tbl_rows:
                if all(r.get(k) == v for k, v in match_pred.items()) and (
                    not limit_one or removed == 0
                ):
                    removed += 1
                else:
                    kept_rows.append(r)
            truth_tables[table] = kept_rows
        elif op == "update":
            match_pred = dict(change.get("key") or change.get("match") or {})
            limit_one = bool(change.get("single_row", True))
            updated = 0
            for r in tbl_rows:
                if all(r.get(k) == v for k, v in match_pred.items()) and (
                    not limit_one or updated == 0
                ):
                    r.update(dict(change["values"]))
                    updated += 1
        else:
            raise ApplyContractError(f"unsupported operation '{op}' in change stream")

    # Execute under `mode`
    actual_tables: Dict[str, List[Dict[str, Any]]] = {
        t: [dict(r) for r in rows]
        for t, rows in (initial_rows_by_table or {}).items()
    }
    target_enforces_unique = bool(workload_facts.get("target_enforces_unique_key", True))

    if mode in (
        ApplyContractMode.TRANSACTIONAL_ORDERED,
        ApplyContractMode.KEYLESS_MULTISET_CONTRACT,
    ):
        actual_tables = {t: [dict(r) for r in rows] for t, rows in truth_tables.items()}
        pre_verified = True
    elif mode == ApplyContractMode.STAGED_RESTORED_BATCH:
        actual_tables = {t: [dict(r) for r in rows] for t, rows in truth_tables.items()}
        pre_verified = bool(workload_facts.get("pre_exposure_unique_proof", target_enforces_unique))
    else:
        # BATCH_NET_MERGE or NATIVE_ENDPOINT_CONTRACT collapses by key per table
        pre_verified = False
        by_table: Dict[str, List[Dict[str, Any]]] = {}
        for c in changes:
            by_table.setdefault(str(c["table"]), []).append(c)
        for table, tbl_changes in by_table.items():
            tbl_rows = actual_tables.setdefault(table, [])
            net_by_key: Dict[Tuple[Tuple[str, Any], ...], Tuple[str, Dict[str, Any]]] = {}
            for c in tbl_changes:
                raw_key = dict(c.get("key") or c.get("match") or c.get("values") or {})
                k_tuple = tuple(sorted(raw_key.items()))
                op = str(c["op"])
                if op == "delete":
                    net_by_key[k_tuple] = ("delete", raw_key)
                else:
                    merged_val = dict(raw_key)
                    merged_val.update(dict(c.get("values") or {}))
                    net_by_key[k_tuple] = ("upsert", merged_val)
            for k_tuple, (kind, payload) in net_by_key.items():
                key_dict = dict(k_tuple)
                if kind == "delete":
                    # Batch delete removes ALL rows matching key_dict (corrupts no-PK duplicates)
                    actual_tables[table] = [
                        r for r in actual_tables[table]
                        if not all(r.get(k) == v for k, v in key_dict.items())
                    ]
                else:
                    if target_enforces_unique:
                        matched = False
                        for r in actual_tables[table]:
                            if all(r.get(k) == v for k, v in key_dict.items()):
                                r.update(payload)
                                matched = True
                        if not matched:
                            actual_tables[table].append(dict(payload))
                    else:
                        # Target without unique enforcement appends on batch load without dedup
                        actual_tables[table].append(dict(payload))

    gate_facts = dict(workload_facts)
    gate_facts["pre_exposure_verification_completed"] = pre_verified
    gate = evaluate_exposure_gate(
        mode,
        gate_facts,
        claimed_transactional_guarantees=claimed_transactional_guarantees,
        raise_on_violation=False,
    )

    exposed_frozen = {t: tuple( rows ) for t, rows in actual_tables.items()}
    truth_frozen = {t: tuple( rows ) for t, rows in truth_tables.items()}
    matches = exposed_frozen == truth_frozen

    cost_breakdown = decision.all_costs[mode.value]
    if gate.care_badge == CareBadge.ACTION or not matches:
        badge = CareBadge.ACTION
    elif gate.care_badge == CareBadge.REVIEW:
        badge = CareBadge.REVIEW
    else:
        badge = CareBadge.PASS

    return SandboxApplyResult(
        mode=mode,
        exposed_rows_by_table=exposed_frozen,
        source_expected_rows_by_table=truth_frozen,
        matches_source_truth=matches,
        exposure_gate=gate,
        cost_breakdown=cost_breakdown,
        care_badge=badge,
    )


# -----------------------------------------------------------------------------
# O(1) Typed Checkpoint & Lease Integration via Existing Owners (durable, lease)
# -----------------------------------------------------------------------------

def _encode_typed(val: Any) -> Any:
    """Encode native Python types (datetime, Decimal, UUID, tuple) for durable JSON."""
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
    """Restore native Python types (datetime, Decimal, UUID, tuple) from JSON."""
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


def save_contract_checkpoint(
    path: Path,
    unit_id: str,
    mode: ApplyContractMode,
    cursor_key: Any,
    expected_cost_seconds: float,
) -> Path:
    """Record a completed apply unit in O(1) using `migkit.durable.write_json`."""
    existing = durable.read_state(Path(path), "state") or {"units": {}}
    units = dict(existing.get("units") or {})
    units[str(unit_id)] = {
        "mode": mode.value,
        "cursor_key": _encode_typed(cursor_key),
        "expected_cost_seconds": float(expected_cost_seconds),
    }
    payload = {"units": units}
    return durable.write_json(Path(path), payload)


def load_contract_checkpoint_unit(
    path: Path,
    unit_id: str,
) -> Optional[Dict[str, Any]]:
    """Lookup a completed unit in O(1) via `migkit.durable.read_state`."""
    state = durable.read_state(Path(path), "state")
    if not state:
        return None
    units = state.get("units") or {}
    entry = units.get(str(unit_id))
    if entry is None:
        return None
    return {
        "mode": ApplyContractMode(entry["mode"]),
        "cursor_key": _decode_typed(entry["cursor_key"]),
        "expected_cost_seconds": float(entry["expected_cost_seconds"]),
    }


def acquire_apply_contract_lease(
    lock_path: Path,
    holder: str,
    ttl_seconds: float,
    now_epoch: float,
) -> Dict[str, Any]:
    """Acquire or renew an exclusive lease via `migkit.lease._FileRecord`."""
    rec = lease._FileRecord(Path(lock_path))

    def _decide(have: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if have is not None:
            current_holder = have.get("holder")
            expires = lease.lapse(have)
            if current_holder and current_holder != holder and expires > now_epoch:
                raise ApplyContractError(
                    f"lease at {lock_path} is actively held by {current_holder}"
                )
        return {
            "holder": holder,
            "renewed": float(now_epoch),
            "expires": float(now_epoch) + float(ttl_seconds),
            "jitter": 0.0,
        }

    return rec.change(_decide)
