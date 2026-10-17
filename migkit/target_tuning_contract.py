"""Treat ingest tuning as a temporary durability contract (Row 633: RCH25.target-tuning-contract).

Extends target tuning (`migkit.tune`, `migkit.tuning`, `migkit.indexes`) with:
1. Approved durability bounds (`DurabilityBoundsPolicy`) governing allowable
   temporary relaxations (`synchronous_commit`, `fsync`, `wal_level`,
   `innodb_flush_log_at_trx_commit`, `sync_binlog`, `maintenance_work_mem`,
   deferred secondary indexing, deferred constraint validation, trigger
   deferral, and table autovacuum pause).
2. Observed original settings (`ObservedProtectionSnapshot`) captured from the
   target server and persisted in a write-ahead restore journal before any
   protection is relaxed or any index/constraint is deferred.
3. Full tail and crash-recovery pricing (`TailAndRecoveryCostModel`,
   `TuningCostBreakdown`, `TuningJustification`) that prices index rebuild
   tails, constraint validation scans, `ANALYZE` statistics refresh, setting
   restoration, and expected crash-recovery replay cost before admitting any
   tuning change, plus post-load measured completion verification.
4. Scope discipline via `migkit.decide` rungs: never globally turn off
   protections merely because a faster mode exists; prefer `SESSION` and
   `TABLE` scopes, and require explicit authorization + durability bounds for
   any `GLOBAL` setting.
5. Bounded contract lease (`.run.lock`) with heartbeat and expiration checks
   (`TuningContractLease`); expired leases halt tuned ingest and trigger
   immediate restoration.
6. Restart-safe, idempotent restoration (`TargetTuningCoordinator`) with O(1)
   typed durable checkpointing (`DurableRestoreCheckpoint`) so crashes during
   load or mid-restoration never leave production with weakened durability or
   disabled/unvalidated constraints.
7. Strict application readiness gate (`AppReadinessGate`) that refuses to
   publish `app-ready` while any setting is un-restored, any index is missing
   or invalid, any constraint is disabled or `NOT VALID`, any trigger is
   disabled, or post-load `ANALYZE` has not completed.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import copy
import dataclasses
import datetime
import decimal
import enum
import fcntl
import hashlib
import json
import math
import os
import re
import socket
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable, indexes, tuning


# -----------------------------------------------------------------------------
# Care Badges, Scopes, and Contract Lifecycle Enums
# -----------------------------------------------------------------------------

class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class SettingScope(str, enum.Enum):
    SESSION = "session"
    TABLE = "table"
    GLOBAL = "global"


class TuningKind(str, enum.Enum):
    SYNCHRONOUS_COMMIT = "synchronous_commit"
    FSYNC_RELAXATION = "fsync_relaxation"
    WAL_LEVEL = "wal_level"
    UNLOGGED_LOAD = "unlogged_load"
    FLUSH_LOG_AT_TRX_COMMIT = "innodb_flush_log_at_trx_commit"
    SYNC_BINLOG = "sync_binlog"
    MAINTENANCE_WORK_MEM = "maintenance_work_mem"
    DEFERRED_INDEXING = "deferred_indexing"
    CONSTRAINT_DEFERRAL = "constraint_deferral"
    TRIGGER_DEFERRAL = "trigger_deferral"
    AUTOVACUUM_PAUSE = "autovacuum_pause"


class ContractPhase(str, enum.Enum):
    PROPOSED = "proposed"
    OBSERVED_AND_JOURNALED = "observed_and_journaled"
    ACTIVE_LOAD = "active_load"
    RESTORING_SETTINGS = "restoring_settings"
    REBUILDING_INDEXES = "rebuilding_indexes"
    VALIDATING_CONSTRAINTS = "validating_constraints"
    RESTORING_TRIGGERS = "restoring_triggers"
    RUNNING_ANALYZE = "running_analyze"
    RESTORED_AND_VERIFIED = "restored_and_verified"
    EXPIRED_UNRESTORED = "expired_unrestored"
    FAILED_RESTORE = "failed_restore"
    REJECTED_UNJUSTIFIED = "rejected_unjustified"


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


def is_safe_test_host(host: str) -> bool:
    """Validate that an endpoint host belongs to approved local or 10.0.0.x test ranges."""
    h = str(host or "").strip().lower()
    if h in SAFE_TEST_HOSTS:
        return True
    if h.startswith("10.0.0."):
        parts = h.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return True
    return False


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class TargetTuningContractError(RuntimeError):
    """Base error for target ingest tuning durability contracts."""


class UnsafeTargetHostError(TargetTuningContractError, ValueError):
    """Raised when a target host is outside approved safe test ranges."""


class DurabilityBoundViolationError(TargetTuningContractError):
    """Raised when a proposed tuning change violates approved durability bounds."""


class GlobalProtectionViolationError(TargetTuningContractError):
    """Raised when a global protection relaxation is attempted without authorization or necessity."""


class UnjustifiedTuningError(TargetTuningContractError):
    """Raised when a tuning change is not justified by overall completion cost including tails."""


class ContractLeaseError(TargetTuningContractError):
    """Raised when a tuning contract lease cannot be acquired or is held by another runner."""


class ContractLeaseExpiredError(ContractLeaseError):
    """Raised when an active tuning contract lease has expired."""


class RestoreVerificationError(TargetTuningContractError):
    """Raised when post-load restoration fails or leaves weakened protections on the target."""


class AppReadinessBlockedError(TargetTuningContractError):
    """Raised when attempting to publish app-ready while checks, indexes, or constraints are unvalidated."""


# -----------------------------------------------------------------------------
# Native Type-Preserving JSON Serialization for O(1) Durable Checkpoints
# -----------------------------------------------------------------------------

def encode_typed_value(val: Any) -> Any:
    """Encode native Python types (datetime, date, Decimal, UUID, tuple, set) losslessly."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "val": [encode_typed_value(x) for x in val]}
    if isinstance(val, (set, frozenset)):
        return {"__type__": "set", "val": sorted([encode_typed_value(x) for x in val], key=str)}
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, dict):
        return {str(k): encode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [encode_typed_value(x) for x in val]
    return val


def decode_typed_value(obj: Any) -> Any:
    """Decode lossless native Python types from JSON structures."""
    if isinstance(obj, dict):
        if "__type__" in obj and "val" in obj and len(obj) == 2:
            t = obj["__type__"]
            v = obj["val"]
            if t == "datetime":
                return datetime.datetime.fromisoformat(v)
            if t == "date":
                return datetime.date.fromisoformat(v)
            if t == "decimal":
                return decimal.Decimal(v)
            if t == "uuid":
                return uuid.UUID(v)
            if t == "tuple":
                return tuple(decode_typed_value(x) for x in v)
            if t == "set":
                return set(decode_typed_value(x) for x in v)
        return {k: decode_typed_value(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decode_typed_value(x) for x in obj]
    return obj


def _parse_memory_mb(val: Any) -> int:
    """Parse a memory setting like '64MB', '2GB', '65536kB', or int MB into integer MB."""
    if isinstance(val, (int, float)):
        return max(1, int(val))
    s = str(val or "").strip().upper()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(KB|MB|GB|TB|B)?", s)
    if not m:
        return 64
    num = float(m.group(1))
    unit = m.group(2) or "MB"
    if unit == "B":
        return max(1, int(num / (1024 * 1024)))
    if unit == "KB":
        return max(1, int(num / 1024))
    if unit == "GB":
        return max(1, int(num * 1024))
    if unit == "TB":
        return max(1, int(num * 1024 * 1024))
    return max(1, int(num))


# -----------------------------------------------------------------------------
# Approved Durability Bounds Policy
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class DurabilityBoundsPolicy:
    """Approved bounds governing temporary target tuning during bulk ingest.

    By default:
    - Session-scoped `synchronous_commit = off` is allowed when crash replayability holds.
    - `fsync = off` and `innodb_flush_log_at_trx_commit = 0` are forbidden because a target
      crash can corrupt the cluster or lose un-reconstructable state.
    - `GLOBAL` scope relaxations are forbidden unless `allow_global_scope=True` AND the
      specific setting name is listed in `authorized_global_settings`.
    - Primary keys and unique constraint-backing indexes are never dropped.
    """
    allow_synchronous_commit_off: bool = True
    allow_fsync_relaxation: bool = False
    allow_wal_level_minimal: bool = False
    allow_unlogged_load: bool = False
    allow_flush_log_at_trx_commit_2: bool = True
    allow_flush_log_at_trx_commit_0: bool = False
    allow_sync_binlog_0: bool = False
    allow_deferred_indexes: bool = True
    allow_constraint_deferral: bool = True
    allow_trigger_deferral: bool = True
    allow_autovacuum_pause: bool = True
    allow_drop_unique_constraints: bool = False
    allow_global_scope: bool = False
    authorized_global_settings: FrozenSet[str] = frozenset()
    min_maintenance_work_mem_mb: int = 64
    max_maintenance_work_mem_mb: int = 4096
    max_lease_duration_seconds: float = 1800.0
    min_completion_speedup_ratio: float = 1.05
    require_crash_replayability: bool = True
    require_post_load_analyze: bool = True

    def validate_setting_change(
        self,
        engine: str,
        setting_name: str,
        tuned_value: Any,
        scope: SettingScope,
        crash_replayable: bool = True,
        session_alternative_available: bool = True,
    ) -> None:
        """Verify that a proposed setting change respects scope and durability bounds."""
        norm_name = setting_name.strip().lower()
        norm_val = str(tuned_value).strip().lower()

        # Rule: Do not globally turn off protections merely because a faster mode exists.
        if scope == SettingScope.GLOBAL:
            if session_alternative_available and norm_name in {
                "synchronous_commit",
                "maintenance_work_mem",
                "work_mem",
                "foreign_key_checks",
                "unique_checks",
                "session_replication_role",
                "lock_timeout",
                "statement_timeout",
            }:
                raise GlobalProtectionViolationError(
                    f"[ACTION] {norm_name} supports SESSION scope; refusing to alter GLOBAL "
                    f"protection for all server sessions merely because a faster mode exists"
                )
            if not self.allow_global_scope or norm_name not in {
                s.lower() for s in self.authorized_global_settings
            }:
                why = tuning.may_set(engine, norm_name, self.authorized_global_settings)
                raise GlobalProtectionViolationError(
                    f"[ACTION] global setting {norm_name}={tuned_value!r} on {engine} refused: "
                    f"{why or 'operator global authorization required'}"
                )

        # Durability bound checks per setting
        if norm_name == "fsync" and norm_val in ("off", "0", "false", "no"):
            if not self.allow_fsync_relaxation:
                raise DurabilityBoundViolationError(
                    "[ACTION] fsync=off violates approved durability bounds: crash during load "
                    "would corrupt cluster catalog beyond transactional replay"
                )
            if self.require_crash_replayability and not crash_replayable:
                raise DurabilityBoundViolationError(
                    "[ACTION] fsync=off requires full crash replayability from source"
                )

        if norm_name == "synchronous_commit" and norm_val in ("off", "local", "0", "false"):
            if not self.allow_synchronous_commit_off:
                raise DurabilityBoundViolationError(
                    "[ACTION] synchronous_commit relaxation is disabled by durability policy"
                )
            if self.require_crash_replayability and not crash_replayable:
                raise DurabilityBoundViolationError(
                    "[ACTION] synchronous_commit=off refused: load ranges are not marked crash-replayable"
                )

        if norm_name == "wal_level" and norm_val == "minimal":
            if not self.allow_wal_level_minimal:
                raise DurabilityBoundViolationError(
                    "[ACTION] wal_level=minimal violates durability bounds (breaks physical standbys and PITR)"
                )

        if norm_name == "innodb_flush_log_at_trx_commit":
            if str(tuned_value) == "0" and not self.allow_flush_log_at_trx_commit_0:
                raise DurabilityBoundViolationError(
                    "[ACTION] innodb_flush_log_at_trx_commit=0 violates durability bounds"
                )
            if str(tuned_value) == "2" and not self.allow_flush_log_at_trx_commit_2:
                raise DurabilityBoundViolationError(
                    "[ACTION] innodb_flush_log_at_trx_commit=2 is disabled by durability policy"
                )
            if self.require_crash_replayability and not crash_replayable:
                raise DurabilityBoundViolationError(
                    "[ACTION] innodb_flush_log_at_trx_commit relaxation requires crash-replayable ranges"
                )

        if norm_name == "sync_binlog" and str(tuned_value) == "0":
            if not self.allow_sync_binlog_0:
                raise DurabilityBoundViolationError(
                    "[ACTION] sync_binlog=0 violates durability bounds (risks binlog/InnoDB divergence on crash)"
                )

        if norm_name == "maintenance_work_mem":
            mem_mb = _parse_memory_mb(tuned_value)
            if mem_mb < self.min_maintenance_work_mem_mb or mem_mb > self.max_maintenance_work_mem_mb:
                raise DurabilityBoundViolationError(
                    f"[ACTION] maintenance_work_mem={tuned_value} ({mem_mb}MB) outside approved bounds "
                    f"[{self.min_maintenance_work_mem_mb}MB, {self.max_maintenance_work_mem_mb}MB]"
                )


# -----------------------------------------------------------------------------
# Observed Original Settings & Protection Snapshot
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class ObservedSetting:
    """Observed original parameter and its temporary tuned target value."""
    name: str
    original_value: Any
    tuned_value: Any
    scope: SettingScope = SettingScope.SESSION
    table_name: Optional[str] = None
    observed_at: float = dataclasses.field(default_factory=time.time)
    verified_from_server: bool = True
    restored: bool = False
    restored_at: Optional[float] = None

    def restore_step_id(self) -> str:
        tbl = f":{self.table_name}" if self.table_name else ""
        return f"setting:{self.scope.value}:{self.name}{tbl}"

    def to_dict(self) -> Dict[str, Any]:
        return encode_typed_value(dataclasses.asdict(self))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ObservedSetting:
        d = decode_typed_value(data)
        return cls(
            name=str(d["name"]),
            original_value=d["original_value"],
            tuned_value=d["tuned_value"],
            scope=SettingScope(d.get("scope", SettingScope.SESSION.value)),
            table_name=d.get("table_name"),
            observed_at=float(d.get("observed_at", 0.0)),
            verified_from_server=bool(d.get("verified_from_server", True)),
            restored=bool(d.get("restored", False)),
            restored_at=float(d["restored_at"]) if d.get("restored_at") is not None else None,
        )


@dataclasses.dataclass
class DeferredIndexRecord:
    """Secondary index deferred (dropped during load, rebuilt and validated in tail)."""
    index_key: str
    schema: str
    table: str
    index_name: str
    create_ddl: str
    drop_ddl: str
    is_unique: bool = False
    is_primary: bool = False
    is_constraint_backed: bool = False
    estimated_rows: int = 0
    estimated_bytes: int = 0
    dropped: bool = False
    rebuilt: bool = False
    validated: bool = False
    rebuild_seconds: float = 0.0

    def restore_step_id(self) -> str:
        return f"index:{self.index_key}"

    def to_dict(self) -> Dict[str, Any]:
        return encode_typed_value(dataclasses.asdict(self))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DeferredIndexRecord:
        d = decode_typed_value(data)
        return cls(
            index_key=str(d["index_key"]),
            schema=str(d["schema"]),
            table=str(d["table"]),
            index_name=str(d["index_name"]),
            create_ddl=str(d["create_ddl"]),
            drop_ddl=str(d["drop_ddl"]),
            is_unique=bool(d.get("is_unique", False)),
            is_primary=bool(d.get("is_primary", False)),
            is_constraint_backed=bool(d.get("is_constraint_backed", False)),
            estimated_rows=int(d.get("estimated_rows", 0)),
            estimated_bytes=int(d.get("estimated_bytes", 0)),
            dropped=bool(d.get("dropped", False)),
            rebuilt=bool(d.get("rebuilt", False)),
            validated=bool(d.get("validated", False)),
            rebuild_seconds=float(d.get("rebuild_seconds", 0.0)),
        )


@dataclasses.dataclass
class DeferredConstraintRecord:
    """Constraint deferred during load and explicitly validated before app-ready."""
    constraint_key: str
    schema: str
    table: str
    constraint_name: str
    constraint_type: str  # FOREIGN KEY, CHECK, UNIQUE
    definition_ddl: str
    originally_validated: bool = True
    referenced_schema: Optional[str] = None
    referenced_table: Optional[str] = None
    estimated_rows: int = 0
    disabled_or_not_valid: bool = False
    restored: bool = False
    validated: bool = False
    validation_seconds: float = 0.0

    def restore_step_id(self) -> str:
        return f"constraint:{self.constraint_key}"

    def to_dict(self) -> Dict[str, Any]:
        return encode_typed_value(dataclasses.asdict(self))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DeferredConstraintRecord:
        d = decode_typed_value(data)
        return cls(
            constraint_key=str(d["constraint_key"]),
            schema=str(d["schema"]),
            table=str(d["table"]),
            constraint_name=str(d["constraint_name"]),
            constraint_type=str(d["constraint_type"]),
            definition_ddl=str(d["definition_ddl"]),
            originally_validated=bool(d.get("originally_validated", True)),
            referenced_schema=d.get("referenced_schema"),
            referenced_table=d.get("referenced_table"),
            estimated_rows=int(d.get("estimated_rows", 0)),
            disabled_or_not_valid=bool(d.get("disabled_or_not_valid", False)),
            restored=bool(d.get("restored", False)),
            validated=bool(d.get("validated", False)),
            validation_seconds=float(d.get("validation_seconds", 0.0)),
        )


@dataclasses.dataclass
class DeferredTriggerRecord:
    """User trigger disabled for bulk load and re-enabled in restoration tail."""
    trigger_key: str
    schema: str
    table: str
    trigger_name: str
    originally_enabled: bool = True
    disabled: bool = False
    restored: bool = False

    def restore_step_id(self) -> str:
        return f"trigger:{self.trigger_key}"

    def to_dict(self) -> Dict[str, Any]:
        return encode_typed_value(dataclasses.asdict(self))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DeferredTriggerRecord:
        d = decode_typed_value(data)
        return cls(
            trigger_key=str(d["trigger_key"]),
            schema=str(d["schema"]),
            table=str(d["table"]),
            trigger_name=str(d["trigger_name"]),
            originally_enabled=bool(d.get("originally_enabled", True)),
            disabled=bool(d.get("disabled", False)),
            restored=bool(d.get("restored", False)),
        )


@dataclasses.dataclass
class ObservedProtectionSnapshot:
    """Complete pre-load snapshot of observed server settings, indexes, constraints, and triggers."""
    engine: str
    database: str
    target_host: str
    observed_at: float
    settings: Dict[str, ObservedSetting] = dataclasses.field(default_factory=dict)
    indexes: Dict[str, DeferredIndexRecord] = dataclasses.field(default_factory=dict)
    constraints: Dict[str, DeferredConstraintRecord] = dataclasses.field(default_factory=dict)
    triggers: Dict[str, DeferredTriggerRecord] = dataclasses.field(default_factory=dict)
    tables_in_scope: Tuple[str, ...] = ()

    def fingerprint(self) -> str:
        payload = {
            "engine": self.engine,
            "database": self.database,
            "settings": {k: (v.original_value, v.scope.value) for k, v in sorted(self.settings.items())},
            "indexes": {k: v.create_ddl for k, v in sorted(self.indexes.items())},
            "constraints": {
                k: (v.definition_ddl, v.originally_validated)
                for k, v in sorted(self.constraints.items())
            },
            "triggers": {k: v.originally_enabled for k, v in sorted(self.triggers.items())},
        }
        raw = json.dumps(encode_typed_value(payload), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("ascii", errors="ignore")).hexdigest()[:32]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "engine": self.engine,
            "database": self.database,
            "target_host": self.target_host,
            "observed_at": self.observed_at,
            "settings": {k: v.to_dict() for k, v in self.settings.items()},
            "indexes": {k: v.to_dict() for k, v in self.indexes.items()},
            "constraints": {k: v.to_dict() for k, v in self.constraints.items()},
            "triggers": {k: v.to_dict() for k, v in self.triggers.items()},
            "tables_in_scope": list(self.tables_in_scope),
            "fingerprint": self.fingerprint(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ObservedProtectionSnapshot:
        d = decode_typed_value(data)
        return cls(
            engine=str(d["engine"]),
            database=str(d["database"]),
            target_host=str(d.get("target_host", "127.0.0.1")),
            observed_at=float(d.get("observed_at", 0.0)),
            settings={k: ObservedSetting.from_dict(v) for k, v in (d.get("settings") or {}).items()},
            indexes={k: DeferredIndexRecord.from_dict(v) for k, v in (d.get("indexes") or {}).items()},
            constraints={
                k: DeferredConstraintRecord.from_dict(v)
                for k, v in (d.get("constraints") or {}).items()
            },
            triggers={k: DeferredTriggerRecord.from_dict(v) for k, v in (d.get("triggers") or {}).items()},
            tables_in_scope=tuple(d.get("tables_in_scope") or ()),
        )


# -----------------------------------------------------------------------------
# Pricing Index / Constraint / ANALYZE Tails and Crash Recovery
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class TuningCostBreakdown:
    """Explicit completion time breakdown pricing bulk ingest, tails, and crash recovery."""
    baseline_ingest_seconds: float
    tuned_ingest_seconds: float
    index_rebuild_tail_seconds: float
    constraint_validation_tail_seconds: float
    analyze_tail_seconds: float
    restore_settings_seconds: float
    crash_recovery_expected_seconds: float
    crash_replay_worst_case_seconds: float

    @property
    def total_tail_seconds(self) -> float:
        return (
            self.index_rebuild_tail_seconds
            + self.constraint_validation_tail_seconds
            + self.analyze_tail_seconds
            + self.restore_settings_seconds
        )

    @property
    def total_tuned_completion_seconds(self) -> float:
        return (
            self.tuned_ingest_seconds
            + self.total_tail_seconds
            + self.crash_recovery_expected_seconds
        )

    @property
    def net_savings_seconds(self) -> float:
        return self.baseline_ingest_seconds - self.total_tuned_completion_seconds

    @property
    def speedup_ratio(self) -> float:
        if self.total_tuned_completion_seconds <= 0:
            return 1.0
        return self.baseline_ingest_seconds / self.total_tuned_completion_seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "baseline_ingest_seconds": round(self.baseline_ingest_seconds, 4),
            "tuned_ingest_seconds": round(self.tuned_ingest_seconds, 4),
            "index_rebuild_tail_seconds": round(self.index_rebuild_tail_seconds, 4),
            "constraint_validation_tail_seconds": round(self.constraint_validation_tail_seconds, 4),
            "analyze_tail_seconds": round(self.analyze_tail_seconds, 4),
            "restore_settings_seconds": round(self.restore_settings_seconds, 4),
            "crash_recovery_expected_seconds": round(self.crash_recovery_expected_seconds, 4),
            "crash_replay_worst_case_seconds": round(self.crash_replay_worst_case_seconds, 4),
            "total_tail_seconds": round(self.total_tail_seconds, 4),
            "total_tuned_completion_seconds": round(self.total_tuned_completion_seconds, 4),
            "net_savings_seconds": round(self.net_savings_seconds, 4),
            "speedup_ratio": round(self.speedup_ratio, 4),
        }


@dataclasses.dataclass(frozen=True)
class TuningJustification:
    """Decision record stating whether a tuning change or contract is justified by overall completion."""
    item_name: str
    justified: bool
    selected_rung: str
    cost_breakdown: TuningCostBreakdown
    care_badge: CareBadge
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_name": self.item_name,
            "justified": self.justified,
            "selected_rung": self.selected_rung,
            "cost_breakdown": self.cost_breakdown.to_dict(),
            "care_badge": self.care_badge.value,
            "reason": self.reason,
        }


@dataclasses.dataclass
class MeasuredCompletionEvidence:
    """Post-load measured durations proving whether overall completion justified the contract."""
    baseline_estimated_seconds: float
    measured_ingest_seconds: float = 0.0
    measured_index_rebuild_seconds: float = 0.0
    measured_constraint_validation_seconds: float = 0.0
    measured_analyze_seconds: float = 0.0
    measured_restore_seconds: float = 0.0
    measured_crash_recovery_seconds: float = 0.0
    per_change_savings_seconds: Dict[str, float] = dataclasses.field(default_factory=dict)

    @property
    def total_measured_completion_seconds(self) -> float:
        return (
            self.measured_ingest_seconds
            + self.measured_index_rebuild_seconds
            + self.measured_constraint_validation_seconds
            + self.measured_analyze_seconds
            + self.measured_restore_seconds
            + self.measured_crash_recovery_seconds
        )

    @property
    def measured_speedup_ratio(self) -> float:
        tot = self.total_measured_completion_seconds
        if tot <= 0:
            return 1.0
        return self.baseline_estimated_seconds / tot

    def justified_by_measured_completion(self, min_ratio: float = 1.0) -> bool:
        return (
            self.total_measured_completion_seconds < self.baseline_estimated_seconds
            and self.measured_speedup_ratio >= min_ratio
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "baseline_estimated_seconds": round(self.baseline_estimated_seconds, 4),
            "measured_ingest_seconds": round(self.measured_ingest_seconds, 4),
            "measured_index_rebuild_seconds": round(self.measured_index_rebuild_seconds, 4),
            "measured_constraint_validation_seconds": round(
                self.measured_constraint_validation_seconds, 4
            ),
            "measured_analyze_seconds": round(self.measured_analyze_seconds, 4),
            "measured_restore_seconds": round(self.measured_restore_seconds, 4),
            "measured_crash_recovery_seconds": round(self.measured_crash_recovery_seconds, 4),
            "total_measured_completion_seconds": round(self.total_measured_completion_seconds, 4),
            "measured_speedup_ratio": round(self.measured_speedup_ratio, 4),
            "per_change_savings_seconds": {
                k: round(v, 4) for k, v in self.per_change_savings_seconds.items()
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> MeasuredCompletionEvidence:
        d = decode_typed_value(data)
        return cls(
            baseline_estimated_seconds=float(d.get("baseline_estimated_seconds", 0.0)),
            measured_ingest_seconds=float(d.get("measured_ingest_seconds", 0.0)),
            measured_index_rebuild_seconds=float(d.get("measured_index_rebuild_seconds", 0.0)),
            measured_constraint_validation_seconds=float(
                d.get("measured_constraint_validation_seconds", 0.0)
            ),
            measured_analyze_seconds=float(d.get("measured_analyze_seconds", 0.0)),
            measured_restore_seconds=float(d.get("measured_restore_seconds", 0.0)),
            measured_crash_recovery_seconds=float(d.get("measured_crash_recovery_seconds", 0.0)),
            per_change_savings_seconds={
                str(k): float(v) for k, v in (d.get("per_change_savings_seconds") or {}).items()
            },
        )


class TailAndRecoveryCostModel:
    """Prices bulk ingest, deferred index builds, constraint validations, ANALYZE, and crash recovery."""

    def __init__(
        self,
        base_write_mb_per_sec: float = 50.0,
        per_index_write_penalty_factor: float = 0.35,
        per_fk_check_penalty_factor: float = 0.18,
        index_sort_mb_per_sec: float = 120.0,
        fk_validation_scan_mb_per_sec: float = 200.0,
        analyze_scan_mb_per_sec: float = 400.0,
        synchronous_commit_off_speedup: float = 1.25,
        crash_probability: float = 0.02,
        unflushed_window_mb: float = 64.0,
    ) -> None:
        self.base_write_mb_per_sec = max(1.0, float(base_write_mb_per_sec))
        self.per_index_write_penalty_factor = max(0.0, float(per_index_write_penalty_factor))
        self.per_fk_check_penalty_factor = max(0.0, float(per_fk_check_penalty_factor))
        self.index_sort_mb_per_sec = max(1.0, float(index_sort_mb_per_sec))
        self.fk_validation_scan_mb_per_sec = max(1.0, float(fk_validation_scan_mb_per_sec))
        self.analyze_scan_mb_per_sec = max(1.0, float(analyze_scan_mb_per_sec))
        self.synchronous_commit_off_speedup = max(1.0, float(synchronous_commit_off_speedup))
        self.crash_probability = max(0.0, min(1.0, float(crash_probability)))
        self.unflushed_window_mb = max(1.0, float(unflushed_window_mb))

    def price_contract(
        self,
        total_rows: int,
        total_bytes: int,
        secondary_index_count: int,
        deferred_index_count: int,
        fk_constraint_count: int,
        deferred_fk_count: int,
        relax_sync_commit: bool = True,
        maintenance_work_mem_mb: int = 512,
        parallel_workers: int = 2,
        index_setup_overhead_seconds: float = 0.5,
        constraint_setup_overhead_seconds: float = 0.3,
        analyze_overhead_seconds: float = 0.2,
    ) -> TuningCostBreakdown:
        total_mb = max(0.001, float(total_bytes) / (1024.0 * 1024.0))
        row_factor = max(1, int(total_rows))

        # Baseline: all indexes and FKs maintained row-by-row during load
        baseline_penalty = (
            1.0
            + secondary_index_count * self.per_index_write_penalty_factor
            + fk_constraint_count * self.per_fk_check_penalty_factor
        )
        baseline_ingest_seconds = (total_mb / self.base_write_mb_per_sec) * baseline_penalty

        # Tuned ingest: only non-deferred indexes and non-deferred FKs maintained inline
        active_indexes = max(0, secondary_index_count - deferred_index_count)
        active_fks = max(0, fk_constraint_count - deferred_fk_count)
        tuned_penalty = (
            1.0
            + active_indexes * self.per_index_write_penalty_factor
            + active_fks * self.per_fk_check_penalty_factor
        )
        sync_speedup = self.synchronous_commit_off_speedup if relax_sync_commit else 1.0
        tuned_ingest_seconds = (total_mb / (self.base_write_mb_per_sec * sync_speedup)) * tuned_penalty

        # Index rebuild tail: scales with maintenance_work_mem and parallel workers
        mem_boost = min(2.5, max(0.5, math.log2(max(64, maintenance_work_mem_mb) / 64.0 + 1.0)))
        worker_boost = max(1.0, min(float(max(1, parallel_workers)), 8.0) * 0.75)
        effective_sort_rate = self.index_sort_mb_per_sec * mem_boost * worker_boost
        if deferred_index_count > 0:
            index_rebuild_tail_seconds = (
                deferred_index_count * index_setup_overhead_seconds
                + (deferred_index_count * total_mb) / effective_sort_rate
            )
        else:
            index_rebuild_tail_seconds = 0.0

        # Constraint validation tail: table scan per deferred FK / check constraint generation
        if deferred_fk_count > 0:
            constraint_validation_tail_seconds = (
                deferred_fk_count * constraint_setup_overhead_seconds
                + (deferred_fk_count * total_mb) / (self.fk_validation_scan_mb_per_sec * worker_boost)
            )
        else:
            constraint_validation_tail_seconds = 0.0

        # ANALYZE tail: sample/scan cost + fixed catalog update overhead
        sample_mb = min(total_mb, max(1.0, math.log10(row_factor + 10.0) * 8.0))
        analyze_tail_seconds = analyze_overhead_seconds + (sample_mb / self.analyze_scan_mb_per_sec)

        # Setting restoration & WAL flush
        restore_settings_seconds = 0.15 if (relax_sync_commit or deferred_index_count or deferred_fk_count) else 0.0

        # Crash recovery cost: replay of unflushed window + partial index cleanup
        if relax_sync_commit or deferred_index_count > 0 or deferred_fk_count > 0:
            replay_mb = min(total_mb, self.unflushed_window_mb) if relax_sync_commit else 0.0
            replay_sec = replay_mb / self.base_write_mb_per_sec
            partial_rebuild_sec = 0.25 * index_rebuild_tail_seconds
            crash_replay_worst_case = replay_sec + partial_rebuild_sec + restore_settings_seconds
            crash_recovery_expected = self.crash_probability * crash_replay_worst_case
        else:
            crash_replay_worst_case = 0.0
            crash_recovery_expected = 0.0

        return TuningCostBreakdown(
            baseline_ingest_seconds=baseline_ingest_seconds,
            tuned_ingest_seconds=tuned_ingest_seconds,
            index_rebuild_tail_seconds=index_rebuild_tail_seconds,
            constraint_validation_tail_seconds=constraint_validation_tail_seconds,
            analyze_tail_seconds=analyze_tail_seconds,
            restore_settings_seconds=restore_settings_seconds,
            crash_recovery_expected_seconds=crash_recovery_expected,
            crash_replay_worst_case_seconds=crash_replay_worst_case,
        )


def select_tuning_contract_way(
    policy: DurabilityBoundsPolicy,
    cost_model: TailAndRecoveryCostModel,
    total_rows: int,
    total_bytes: int,
    secondary_index_count: int,
    fk_constraint_count: int,
    crash_replayable: bool = True,
    maintenance_work_mem_mb: int = 512,
    parallel_workers: int = 2,
    requested_scope: SettingScope = SettingScope.SESSION,
) -> Tuple[decide.Choice, TuningJustification]:
    """Use `migkit.decide.climb` rungs to choose the safest, completion-justified tuning contract."""
    full_breakdown = cost_model.price_contract(
        total_rows=total_rows,
        total_bytes=total_bytes,
        secondary_index_count=secondary_index_count,
        deferred_index_count=secondary_index_count if policy.allow_deferred_indexes else 0,
        fk_constraint_count=fk_constraint_count,
        deferred_fk_count=fk_constraint_count if policy.allow_constraint_deferral else 0,
        relax_sync_commit=policy.allow_synchronous_commit_off and crash_replayable,
        maintenance_work_mem_mb=maintenance_work_mem_mb,
        parallel_workers=parallel_workers,
    )
    session_only_breakdown = cost_model.price_contract(
        total_rows=total_rows,
        total_bytes=total_bytes,
        secondary_index_count=secondary_index_count,
        deferred_index_count=0,
        fk_constraint_count=fk_constraint_count,
        deferred_fk_count=0,
        relax_sync_commit=policy.allow_synchronous_commit_off and crash_replayable,
        maintenance_work_mem_mb=maintenance_work_mem_mb,
        parallel_workers=parallel_workers,
    )
    baseline_breakdown = cost_model.price_contract(
        total_rows=total_rows,
        total_bytes=total_bytes,
        secondary_index_count=secondary_index_count,
        deferred_index_count=0,
        fk_constraint_count=fk_constraint_count,
        deferred_fk_count=0,
        relax_sync_commit=False,
        maintenance_work_mem_mb=64,
        parallel_workers=1,
        analyze_overhead_seconds=0.0,
    )

    has_table_or_deferred = (
        requested_scope == SettingScope.TABLE
        or (policy.allow_deferred_indexes and secondary_index_count > 0)
        or (policy.allow_constraint_deferral and fk_constraint_count > 0)
    )
    full_beats_baseline = (
        full_breakdown.total_tuned_completion_seconds < full_breakdown.baseline_ingest_seconds
        and full_breakdown.speedup_ratio >= policy.min_completion_speedup_ratio
    )
    session_beats_baseline = (
        session_only_breakdown.total_tuned_completion_seconds
        < session_only_breakdown.baseline_ingest_seconds
        and session_only_breakdown.speedup_ratio >= policy.min_completion_speedup_ratio
    )

    rungs = (
        decide.Rung(
            "session_and_table_contract",
            "session durability contract with deferred secondary indexes and validated constraint tail",
            needs=(
                decide.Need(
                    "session or table scope so other sessions keep full protection",
                    lambda f: f["requested_scope"] in (SettingScope.SESSION, SettingScope.TABLE),
                    "global scope was requested where session/table scope suffices",
                ),
                decide.Need(
                    "table-level setting or deferred index/constraint tail work",
                    lambda f: f["has_table_or_deferred"],
                    "only session parameters are requested",
                ),
                decide.Need(
                    "crash-replayable ranges when synchronous_commit is relaxed",
                    lambda f: f["crash_replayable"] or not f["allow_sync_off"],
                    "ranges are not crash-replayable from source",
                ),
                decide.Need(
                    "overall completion including index, constraint, ANALYZE and recovery tails to beat baseline",
                    lambda f: f["full_beats_baseline"],
                    lambda f: (
                        f"tuned completion ({f['full_total']:.2f}s including {f['full_tail']:.2f}s tail) "
                        f"does not beat untuned baseline ({f['baseline']:.2f}s)"
                    ),
                ),
            ),
            because="fastest end-to-end completion without global server risk",
        ),
        decide.Rung(
            "session_parameter_contract",
            "session-only parameter tuning with indexes and constraints kept inline",
            needs=(
                decide.Need(
                    "session scope",
                    lambda f: f["requested_scope"] == SettingScope.SESSION,
                    "scope is not session",
                ),
                decide.Need(
                    "crash-replayable ranges",
                    lambda f: f["crash_replayable"],
                    "ranges are not crash-replayable",
                ),
                decide.Need(
                    "session tuning completion to beat baseline",
                    lambda f: f["session_beats_baseline"],
                    "session parameter tuning does not beat baseline after ANALYZE tail",
                ),
            ),
            because="avoids index rebuild tail on small tables while accelerating commits in session",
        ),
        decide.Rung(
            "untuned_baseline",
            "untuned baseline load with all protections and indexes kept inline",
            needs=(),
            because="protections stay untouched when tail rebuild costs outweigh ingest speedup",
        ),
    )

    facts = decide.Facts({
        "requested_scope": requested_scope,
        "has_table_or_deferred": bool(has_table_or_deferred),
        "crash_replayable": bool(crash_replayable),
        "allow_sync_off": bool(policy.allow_synchronous_commit_off),
        "full_beats_baseline": bool(full_beats_baseline),
        "session_beats_baseline": bool(session_beats_baseline),
        "full_total": full_breakdown.total_tuned_completion_seconds,
        "full_tail": full_breakdown.total_tail_seconds,
        "baseline": full_breakdown.baseline_ingest_seconds,
    })
    costs = {
        "session_and_table_contract": full_breakdown.total_tuned_completion_seconds,
        "session_parameter_contract": session_only_breakdown.total_tuned_completion_seconds,
        "untuned_baseline": full_breakdown.baseline_ingest_seconds,
    }
    choice = decide.climb("target tuning durability contract", rungs, facts, costs=costs)
    chosen_name = choice.rung.name if choice.rung else "untuned_baseline"

    if chosen_name == "session_and_table_contract":
        justification = TuningJustification(
            item_name="contract:session_and_table",
            justified=True,
            selected_rung=chosen_name,
            cost_breakdown=full_breakdown,
            care_badge=CareBadge.PASS,
            reason=(
                f"[PASS] overall completion {full_breakdown.total_tuned_completion_seconds:.2f}s "
                f"(ingest {full_breakdown.tuned_ingest_seconds:.2f}s + tail {full_breakdown.total_tail_seconds:.2f}s "
                f"+ recovery risk {full_breakdown.crash_recovery_expected_seconds:.2f}s) beats baseline "
                f"{full_breakdown.baseline_ingest_seconds:.2f}s ({full_breakdown.speedup_ratio:.2f}x)"
            ),
        )
    elif chosen_name == "session_parameter_contract":
        justification = TuningJustification(
            item_name="contract:session_parameter",
            justified=True,
            selected_rung=chosen_name,
            cost_breakdown=session_only_breakdown,
            care_badge=CareBadge.PASS,
            reason=(
                f"[PASS] session parameter contract {session_only_breakdown.total_tuned_completion_seconds:.2f}s "
                f"beats baseline {session_only_breakdown.baseline_ingest_seconds:.2f}s "
                f"({session_only_breakdown.speedup_ratio:.2f}x); index deferral skipped as tail outweighed gain"
            ),
        )
    else:
        justification = TuningJustification(
            item_name="contract:untuned_baseline",
            justified=False,
            selected_rung="untuned_baseline",
            cost_breakdown=full_breakdown if secondary_index_count or fk_constraint_count else baseline_breakdown,
            care_badge=CareBadge.BENIGN,
            reason=(
                f"[BENIGN] tuning rejected because total completion with tails "
                f"({full_breakdown.total_tuned_completion_seconds:.2f}s) does not beat baseline "
                f"({full_breakdown.baseline_ingest_seconds:.2f}s) by required ratio "
                f"{policy.min_completion_speedup_ratio:.2f}x; keeping all protections inline"
            ),
        )
    return choice, justification


# -----------------------------------------------------------------------------
# Mutual Exclusion & Active Contract Lease (.run.lock)
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class TuningContractLease:
    """Active mutual-exclusion lease (`.run.lock`) with heartbeat and expiration."""
    state_dir: Path
    contract_id: str
    lease_duration_seconds: float = 600.0
    stale_heartbeat_seconds: float = 30.0
    host: str = dataclasses.field(default_factory=socket.gethostname)
    pid: int = dataclasses.field(default_factory=os.getpid)
    issued_at: float = 0.0
    expires_at: float = 0.0
    heartbeat_at: float = 0.0
    _held: bool = False

    @property
    def lock_path(self) -> Path:
        return self.state_dir / ".run.lock"

    @property
    def guard_path(self) -> Path:
        return self.state_dir / ".run.lock.guard"

    def acquire(self, now: Optional[float] = None, steal_stale: bool = True) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        ts = float(now if now is not None else time.time())
        guard_fd = os.open(str(self.guard_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(guard_fd, fcntl.LOCK_EX)
            if self.lock_path.exists():
                try:
                    existing = json.loads(self.lock_path.read_text(encoding="ascii"))
                except Exception:
                    existing = None
                if isinstance(existing, dict):
                    ex_contract = existing.get("contract_id")
                    ex_pid = existing.get("pid")
                    ex_host = existing.get("host")
                    ex_hb = float(existing.get("heartbeat_at", 0.0))
                    ex_exp = float(existing.get("expires_at", 0.0))
                    same_owner = (ex_contract == self.contract_id and ex_pid == self.pid and ex_host == self.host)
                    is_stale = (ts - ex_hb) > self.stale_heartbeat_seconds or ts > ex_exp
                    if not same_owner and (not steal_stale or not is_stale):
                        raise ContractLeaseError(
                            f"[ACTION] Active tuning contract lease held by {ex_host}:{ex_pid} "
                            f"(contract={ex_contract}) on {self.lock_path}"
                        )
            self.issued_at = ts
            self.heartbeat_at = ts
            self.expires_at = ts + self.lease_duration_seconds
            payload = {
                "contract_id": self.contract_id,
                "host": self.host,
                "pid": self.pid,
                "issued_at": self.issued_at,
                "heartbeat_at": self.heartbeat_at,
                "expires_at": self.expires_at,
            }
            durable.write_json(self.lock_path, payload)
            self._held = True
        finally:
            fcntl.flock(guard_fd, fcntl.LOCK_UN)
            os.close(guard_fd)

    def heartbeat(self, now: Optional[float] = None, extend_On_heartbeat: bool = False) -> None:
        ts = float(now if now is not None else time.time())
        self.verify(now=ts)
        self.heartbeat_at = ts
        if extend_On_heartbeat:
            self.expires_at = ts + self.lease_duration_seconds
        payload = {
            "contract_id": self.contract_id,
            "host": self.host,
            "pid": self.pid,
            "issued_at": self.issued_at,
            "heartbeat_at": self.heartbeat_at,
            "expires_at": self.expires_at,
        }
        durable.write_json(self.lock_path, payload)

    def verify(self, now: Optional[float] = None) -> None:
        ts = float(now if now is not None else time.time())
        if not self._held or not self.lock_path.exists():
            raise ContractLeaseError(
                f"[ACTION] Contract {self.contract_id} does not hold an active lease at {self.lock_path}"
            )
        try:
            existing = json.loads(self.lock_path.read_text(encoding="ascii"))
        except Exception as exc:
            raise ContractLeaseError(f"[ACTION] Corrupt lease file {self.lock_path}: {exc}") from exc
        if existing.get("contract_id") != self.contract_id or int(existing.get("pid", -1)) != self.pid:
            raise ContractLeaseError(
                f"[ACTION] Lease {self.lock_path} was stolen by {existing.get('host')}:{existing.get('pid')}"
            )
        if ts > self.expires_at or ts > float(existing.get("expires_at", 0.0)):
            raise ContractLeaseExpiredError(
                f"[ACTION] Tuning contract {self.contract_id} lease expired at {self.expires_at:.3f} (now={ts:.3f})"
            )

    def release(self) -> None:
        if not self.guard_path.exists():
            self.lock_path.unlink(missing_ok=True)
            self._held = False
            return
        guard_fd = os.open(str(self.guard_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(guard_fd, fcntl.LOCK_EX)
            if self.lock_path.exists():
                try:
                    existing = json.loads(self.lock_path.read_text(encoding="ascii"))
                except Exception:
                    existing = {}
                if existing.get("contract_id") == self.contract_id:
                    self.lock_path.unlink(missing_ok=True)
            self._held = False
        finally:
            fcntl.flock(guard_fd, fcntl.LOCK_UN)
            os.close(guard_fd)


# -----------------------------------------------------------------------------
# O(1) Typed Durable Resume Checkpoint for Load & Restoration
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class DurableRestoreCheckpoint:
    """O(1) typed durable checkpoint tracking completed load ranges and restore steps."""
    contract_id: str
    completed_load_ranges: Set[str] = dataclasses.field(default_factory=set)
    range_cursors: Dict[str, Any] = dataclasses.field(default_factory=dict)
    completed_restore_steps: Set[str] = dataclasses.field(default_factory=set)
    analyzed_tables: Set[str] = dataclasses.field(default_factory=set)
    updated_at: float = dataclasses.field(default_factory=time.time)

    def is_range_done(self, range_id: str) -> bool:
        return range_id in self.completed_load_ranges

    def mark_range_done(self, range_id: str, cursor_value: Any, now: Optional[float] = None) -> None:
        self.completed_load_ranges.add(range_id)
        self.range_cursors[range_id] = cursor_value
        self.updated_at = float(now if now is not None else time.time())

    def is_restore_step_done(self, step_id: str) -> bool:
        return step_id in self.completed_restore_steps

    def mark_restore_step_done(self, step_id: str, now: Optional[float] = None) -> None:
        self.completed_restore_steps.add(step_id)
        self.updated_at = float(now if now is not None else time.time())

    def to_dict(self) -> Dict[str, Any]:
        return encode_typed_value({
            "contract_id": self.contract_id,
            "completed_load_ranges": sorted(self.completed_load_ranges),
            "range_cursors": self.range_cursors,
            "completed_restore_steps": sorted(self.completed_restore_steps),
            "analyzed_tables": sorted(self.analyzed_tables),
            "updated_at": self.updated_at,
        })

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DurableRestoreCheckpoint:
        d = decode_typed_value(data)
        return cls(
            contract_id=str(d["contract_id"]),
            completed_load_ranges=set(d.get("completed_load_ranges") or ()),
            range_cursors=dict(d.get("range_cursors") or {}),
            completed_restore_steps=set(d.get("completed_restore_steps") or ()),
            analyzed_tables=set(d.get("analyzed_tables") or ()),
            updated_at=float(d.get("updated_at", 0.0)),
        )


# -----------------------------------------------------------------------------
# Target Tuning Contract Model
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class TargetTuningContract:
    """Durable state of a temporary target ingest tuning contract."""
    contract_id: str
    engine: str
    database: str
    target_host: str
    phase: ContractPhase
    policy: DurabilityBoundsPolicy
    snapshot: ObservedProtectionSnapshot
    justification: TuningJustification
    checkpoint: DurableRestoreCheckpoint
    created_at: float
    lease_expires_at: float
    restored_at: Optional[float] = None
    measured_evidence: Optional[MeasuredCompletionEvidence] = None
    audit_log: List[str] = dataclasses.field(default_factory=list)

    def is_fully_restored(self) -> bool:
        if self.phase != ContractPhase.RESTORED_AND_VERIFIED:
            return False
        for s in self.snapshot.settings.values():
            if not s.restored:
                return False
        for ix in self.snapshot.indexes.values():
            if ix.dropped and not (ix.rebuilt and ix.validated):
                return False
        for con in self.snapshot.constraints.values():
            if con.disabled_or_not_valid and not (con.restored and con.validated):
                return False
        for trg in self.snapshot.triggers.values():
            if trg.disabled and not trg.restored:
                return False
        if self.policy.require_post_load_analyze:
            for tbl in self.snapshot.tables_in_scope:
                if tbl not in self.checkpoint.analyzed_tables:
                    return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return encode_typed_value({
            "contract_id": self.contract_id,
            "engine": self.engine,
            "database": self.database,
            "target_host": self.target_host,
            "phase": self.phase.value,
            "policy": dataclasses.asdict(self.policy),
            "snapshot": self.snapshot.to_dict(),
            "justification": self.justification.to_dict(),
            "checkpoint": self.checkpoint.to_dict(),
            "created_at": self.created_at,
            "lease_expires_at": self.lease_expires_at,
            "restored_at": self.restored_at,
            "measured_evidence": (
                self.measured_evidence.to_dict() if self.measured_evidence else None
            ),
            "audit_log": list(self.audit_log),
        })

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TargetTuningContract:
        d = decode_typed_value(data)
        pol_raw = dict(d.get("policy") or {})
        if "authorized_global_settings" in pol_raw:
            pol_raw["authorized_global_settings"] = frozenset(pol_raw["authorized_global_settings"])
        policy = DurabilityBoundsPolicy(**pol_raw)
        snapshot = ObservedProtectionSnapshot.from_dict(d["snapshot"])
        just_raw = d["justification"]
        cb_raw = just_raw["cost_breakdown"]
        breakdown = TuningCostBreakdown(
            baseline_ingest_seconds=float(cb_raw["baseline_ingest_seconds"]),
            tuned_ingest_seconds=float(cb_raw["tuned_ingest_seconds"]),
            index_rebuild_tail_seconds=float(cb_raw["index_rebuild_tail_seconds"]),
            constraint_validation_tail_seconds=float(cb_raw["constraint_validation_tail_seconds"]),
            analyze_tail_seconds=float(cb_raw["analyze_tail_seconds"]),
            restore_settings_seconds=float(cb_raw["restore_settings_seconds"]),
            crash_recovery_expected_seconds=float(cb_raw["crash_recovery_expected_seconds"]),
            crash_replay_worst_case_seconds=float(cb_raw["crash_replay_worst_case_seconds"]),
        )
        justification = TuningJustification(
            item_name=str(just_raw["item_name"]),
            justified=bool(just_raw["justified"]),
            selected_rung=str(just_raw["selected_rung"]),
            cost_breakdown=breakdown,
            care_badge=CareBadge(just_raw["care_badge"]),
            reason=str(just_raw["reason"]),
        )
        checkpoint = DurableRestoreCheckpoint.from_dict(d["checkpoint"])
        measured = (
            MeasuredCompletionEvidence.from_dict(d["measured_evidence"])
            if d.get("measured_evidence")
            else None
        )
        return cls(
            contract_id=str(d["contract_id"]),
            engine=str(d["engine"]),
            database=str(d["database"]),
            target_host=str(d.get("target_host", "127.0.0.1")),
            phase=ContractPhase(d["phase"]),
            policy=policy,
            snapshot=snapshot,
            justification=justification,
            checkpoint=checkpoint,
            created_at=float(d["created_at"]),
            lease_expires_at=float(d["lease_expires_at"]),
            restored_at=float(d["restored_at"]) if d.get("restored_at") is not None else None,
            measured_evidence=measured,
            audit_log=list(d.get("audit_log") or []),
        )


# -----------------------------------------------------------------------------
# Target Adapter Interface & Sandboxed Target Simulator for Verification
# -----------------------------------------------------------------------------

class TargetTuningBackend:
    """Adapter interface for observing, applying, and restoring target protections.

    Works with either a live SQL runner (`run_sql(sql)`) or an inspectable
    target catalog state for deterministic crash/recovery testing.
    """

    def __init__(
        self,
        engine: str = "postgres",
        database: str = "app",
        host: str = "127.0.0.1",
        initial_settings: Optional[Dict[str, Any]] = None,
        initial_indexes: Optional[List[DeferredIndexRecord]] = None,
        initial_constraints: Optional[List[DeferredConstraintRecord]] = None,
        initial_triggers: Optional[List[DeferredTriggerRecord]] = None,
        tables: Optional[Sequence[str]] = None,
        sql_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        if not is_safe_test_host(host):
            raise UnsafeTargetHostError(
                f"[ACTION] Target host {host!r} is outside approved safe test ranges (127.0.0.1, 10.0.0.x)"
            )
        self.engine = engine.strip().lower()
        self.database = database
        self.host = host
        self.sql_hook = sql_hook
        self.executed_sql: List[str] = []

        defaults = (
            {
                "synchronous_commit": "on",
                "fsync": "on",
                "wal_level": "replica",
                "maintenance_work_mem": "64MB",
                "session_replication_role": "origin",
                "autovacuum_enabled": "on",
            }
            if self.engine in ("postgres", "postgresql")
            else {
                "innodb_flush_log_at_trx_commit": 1,
                "sync_binlog": 1,
                "foreign_key_checks": 1,
                "unique_checks": 1,
                "innodb_sort_buffer_size": "64MB",
            }
        )
        if initial_settings:
            defaults.update(initial_settings)
        self.live_settings: Dict[str, Any] = copy.deepcopy(defaults)
        self.live_setting_scopes: Dict[str, SettingScope] = {
            k: SettingScope.SESSION for k in self.live_settings
        }

        self.live_indexes: Dict[str, Dict[str, Any]] = {}
        for ix in initial_indexes or ():
            self.live_indexes[ix.index_key] = {
                "record": copy.deepcopy(ix),
                "present": True,
                "indisvalid": True,
            }

        self.live_constraints: Dict[str, Dict[str, Any]] = {}
        for con in initial_constraints or ():
            self.live_constraints[con.constraint_key] = {
                "record": copy.deepcopy(con),
                "present": True,
                "convalidated": con.originally_validated,
            }

        self.live_triggers: Dict[str, Dict[str, Any]] = {}
        for trg in initial_triggers or ():
            self.live_triggers[trg.trigger_key] = {
                "record": copy.deepcopy(trg),
                "enabled": trg.originally_enabled,
            }

        inferred_tables = set(tables or ())
        for ix in initial_indexes or ():
            inferred_tables.add(f"{ix.schema}.{ix.table}")
        for con in initial_constraints or ():
            inferred_tables.add(f"{con.schema}.{con.table}")
        for trg in initial_triggers or ():
            inferred_tables.add(f"{trg.schema}.{trg.table}")
        if not inferred_tables:
            inferred_tables.add("public.orders" if self.engine.startswith("postgres") else "app.orders")
        self.tables: Tuple[str, ...] = tuple(sorted(inferred_tables))
        self.analyzed_tables: Set[str] = set()
        self.constraint_violations: Dict[str, str] = {}

    def execute_sql(self, sql: str) -> None:
        if self.sql_hook is not None:
            self.sql_hook(sql)
        self.executed_sql.append(sql)

    def observe_snapshot(
        self,
        proposed_settings: Dict[str, Tuple[Any, SettingScope, Optional[str]]],
        defer_indexes: bool = True,
        defer_constraints: bool = True,
        defer_triggers: bool = True,
        now: Optional[float] = None,
    ) -> ObservedProtectionSnapshot:
        ts = float(now if now is not None else time.time())
        observed_settings: Dict[str, ObservedSetting] = {}
        for name, (tuned_val, scope, tbl) in proposed_settings.items():
            orig_val = self.live_settings.get(name)
            if orig_val is None:
                raise DurabilityBoundViolationError(
                    f"[ACTION] Cannot observe original value for unknown setting {name!r} on {self.engine}"
                )
            observed_settings[name] = ObservedSetting(
                name=name,
                original_value=orig_val,
                tuned_value=tuned_val,
                scope=scope,
                table_name=tbl,
                observed_at=ts,
                verified_from_server=True,
            )

        observed_indexes: Dict[str, DeferredIndexRecord] = {}
        if defer_indexes:
            for key, info in sorted(self.live_indexes.items()):
                rec: DeferredIndexRecord = info["record"]
                # Never drop primary key, unique, or constraint-backing indexes!
                if rec.is_primary or rec.is_unique or rec.is_constraint_backed:
                    continue
                if not info.get("present", True):
                    continue
                observed_indexes[key] = copy.deepcopy(rec)

        observed_constraints: Dict[str, DeferredConstraintRecord] = {}
        if defer_constraints:
            for key, info in sorted(self.live_constraints.items()):
                rec_c: DeferredConstraintRecord = info["record"]
                if rec_c.constraint_type.upper() in ("PRIMARY KEY", "UNIQUE"):
                    continue
                observed_constraints[key] = copy.deepcopy(rec_c)

        observed_triggers: Dict[str, DeferredTriggerRecord] = {}
        if defer_triggers:
            for key, info in sorted(self.live_triggers.items()):
                rec_t: DeferredTriggerRecord = info["record"]
                if info.get("enabled", True):
                    observed_triggers[key] = copy.deepcopy(rec_t)

        return ObservedProtectionSnapshot(
            engine=self.engine,
            database=self.database,
            target_host=self.host,
            observed_at=ts,
            settings=observed_settings,
            indexes=observed_indexes,
            constraints=observed_constraints,
            triggers=observed_triggers,
            tables_in_scope=self.tables,
        )

    def apply_tuning(self, snapshot: ObservedProtectionSnapshot) -> List[str]:
        """Apply approved temporary tuning relaxations to the target."""
        stmts: List[str] = []
        for name, s in sorted(snapshot.settings.items()):
            if s.scope == SettingScope.SESSION:
                if self.engine.startswith("postgres"):
                    sql = f"SET SESSION {name} = '{s.tuned_value}';"
                else:
                    val = s.tuned_value if isinstance(s.tuned_value, int) else f"'{s.tuned_value}'"
                    sql = f"SET SESSION {name} = {val};"
            elif s.scope == SettingScope.TABLE and s.table_name:
                sql = f"ALTER TABLE {s.table_name} SET ({name} = {s.tuned_value});"
            else:
                if self.engine.startswith("postgres"):
                    sql = f"ALTER SYSTEM SET {name} = '{s.tuned_value}'; SELECT pg_reload_conf();"
                else:
                    val = s.tuned_value if isinstance(s.tuned_value, int) else f"'{s.tuned_value}'"
                    sql = f"SET GLOBAL {name} = {val};"
            self.execute_sql(sql)
            self.live_settings[name] = s.tuned_value
            self.live_setting_scopes[name] = s.scope
            s.restored = False
            stmts.append(sql)

        for key, ix in sorted(snapshot.indexes.items()):
            self.execute_sql(ix.drop_ddl)
            if key in self.live_indexes:
                self.live_indexes[key]["present"] = False
                self.live_indexes[key]["indisvalid"] = False
            ix.dropped = True
            ix.rebuilt = False
            ix.validated = False
            stmts.append(ix.drop_ddl)

        for key, con in sorted(snapshot.constraints.items()):
            drop_sql = f"ALTER TABLE {con.schema}.{con.table} DROP CONSTRAINT {con.constraint_name};"
            self.execute_sql(drop_sql)
            if key in self.live_constraints:
                self.live_constraints[key]["present"] = False
                self.live_constraints[key]["convalidated"] = False
            con.disabled_or_not_valid = True
            con.restored = False
            con.validated = False
            stmts.append(drop_sql)

        for key, trg in sorted(snapshot.triggers.items()):
            dis_sql = f"ALTER TABLE {trg.schema}.{trg.table} DISABLE TRIGGER {trg.trigger_name};"
            self.execute_sql(dis_sql)
            if key in self.live_triggers:
                self.live_triggers[key]["enabled"] = False
            trg.disabled = True
            trg.restored = False
            stmts.append(dis_sql)

        # Invalidate previous ANALYZE state on loaded tables until post-load ANALYZE runs
        for tbl in snapshot.tables_in_scope:
            self.analyzed_tables.discard(tbl)

        return stmts


# -----------------------------------------------------------------------------
# App Readiness Gate (Never Publish Ready While Protections Weakened)
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class ReadinessFinding:
    care_badge: CareBadge
    category: str
    object_key: str
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "care_badge": self.care_badge.value,
            "category": self.category,
            "object_key": self.object_key,
            "message": self.message,
        }


@dataclasses.dataclass(frozen=True)
class AppReadinessVerdict:
    app_ready: bool
    care_badge: CareBadge
    findings: Tuple[ReadinessFinding, ...]
    summary: str

    def format_report(self) -> str:
        lines = [
            f"[{self.care_badge.value}] app_ready={str(self.app_ready).lower()} | {self.summary}"
        ]
        for f in self.findings:
            lines.append(f"  [{f.care_badge.value}] {f.category} ({f.object_key}): {f.message}")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "app_ready": self.app_ready,
            "care_badge": self.care_badge.value,
            "summary": self.summary,
            "findings": [f.to_dict() for f in self.findings],
        }


class AppReadinessGate:
    """Verifies that all durability settings, indexes, constraints, triggers, and stats are intact."""

    @staticmethod
    def evaluate(
        backend: TargetTuningBackend,
        contracts: Sequence[TargetTuningContract] = (),
    ) -> AppReadinessVerdict:
        findings: List[ReadinessFinding] = []

        # 1. Inspect contract states
        for c in contracts:
            if c.phase not in (
                ContractPhase.RESTORED_AND_VERIFIED,
                ContractPhase.REJECTED_UNJUSTIFIED,
            ):
                findings.append(
                    ReadinessFinding(
                        care_badge=CareBadge.ACTION,
                        category="contract_phase",
                        object_key=c.contract_id,
                        message=f"Contract is in phase {c.phase.value!r}, not restored_and_verified",
                    )
                )
            for name, s in sorted(c.snapshot.settings.items()):
                live_val = backend.live_settings.get(name)
                if str(live_val) != str(s.original_value) or not s.restored:
                    findings.append(
                        ReadinessFinding(
                            care_badge=CareBadge.ACTION,
                            category="unrestored_setting",
                            object_key=name,
                            message=(
                                f"Setting {name} is {live_val!r} (restored={s.restored}), "
                                f"expected original {s.original_value!r}"
                            ),
                        )
                    )

        # 2. Check live target durability settings directly even if no contract is passed
        weak_checks = {
            "synchronous_commit": ("off", "0", "false"),
            "fsync": ("off", "0", "false"),
            "wal_level": ("minimal",),
            "innodb_flush_log_at_trx_commit": ("0", "2"),
            "sync_binlog": ("0",),
            "foreign_key_checks": ("0", "off", "false"),
            "unique_checks": ("0", "off", "false"),
            "session_replication_role": ("replica",),
            "autovacuum_enabled": ("off", "0", "false"),
        }
        for k, bad_vals in weak_checks.items():
            if k in backend.live_settings and str(backend.live_settings[k]).strip().lower() in bad_vals:
                findings.append(
                    ReadinessFinding(
                        care_badge=CareBadge.ACTION,
                        category="weakened_durability",
                        object_key=k,
                        message=f"Live target setting {k}={backend.live_settings[k]!r} leaves durability/checks weakened",
                    )
                )

        # 3. Check live indexes (presence and indisvalid)
        for key, info in sorted(backend.live_indexes.items()):
            if not info.get("present", False):
                findings.append(
                    ReadinessFinding(
                        care_badge=CareBadge.ACTION,
                        category="missing_index",
                        object_key=key,
                        message=f"Index {key} is missing on target (not rebuilt)",
                    )
                )
            elif not info.get("indisvalid", False):
                findings.append(
                    ReadinessFinding(
                        care_badge=CareBadge.ACTION,
                        category="invalid_index",
                        object_key=key,
                        message=f"Index {key} is present but marked invalid (indisvalid=false)",
                    )
                )

        # 4. Check live constraints (presence and convalidated)
        for key, info in sorted(backend.live_constraints.items()):
            rec: DeferredConstraintRecord = info["record"]
            if not info.get("present", False):
                findings.append(
                    ReadinessFinding(
                        care_badge=CareBadge.ACTION,
                        category="missing_constraint",
                        object_key=key,
                        message=f"Constraint {key} ({rec.constraint_type}) is missing/disabled on target",
                    )
                )
            elif rec.originally_validated and not info.get("convalidated", False):
                findings.append(
                    ReadinessFinding(
                        care_badge=CareBadge.ACTION,
                        category="unvalidated_constraint",
                        object_key=key,
                        message=f"Constraint {key} ({rec.constraint_type}) is NOT VALID (convalidated=false)",
                    )
                )

        # 5. Check live triggers
        for key, info in sorted(backend.live_triggers.items()):
            rec_t: DeferredTriggerRecord = info["record"]
            if rec_t.originally_enabled and not info.get("enabled", False):
                findings.append(
                    ReadinessFinding(
                        care_badge=CareBadge.ACTION,
                        category="disabled_trigger",
                        object_key=key,
                        message=f"Trigger {key} remains disabled on target",
                    )
                )

        # 6. Check post-load ANALYZE completion
        for tbl in backend.tables:
            if tbl not in backend.analyzed_tables:
                findings.append(
                    ReadinessFinding(
                        care_badge=CareBadge.ACTION,
                        category="missing_analyze",
                        object_key=tbl,
                        message=f"Table {tbl} has not completed post-load ANALYZE statistics refresh",
                    )
                )

        if findings:
            return AppReadinessVerdict(
                app_ready=False,
                care_badge=CareBadge.ACTION,
                findings=tuple(findings),
                summary=f"App readiness blocked by {len(findings)} unrestored or unvalidated protection(s)",
            )

        return AppReadinessVerdict(
            app_ready=True,
            care_badge=CareBadge.PASS,
            findings=(),
            summary="All durability settings restored, indexes rebuilt, constraints validated, and ANALYZE complete",
        )

    @classmethod
    def publish_app_ready(
        cls,
        backend: TargetTuningBackend,
        contracts: Sequence[TargetTuningContract] = (),
    ) -> AppReadinessVerdict:
        verdict = cls.evaluate(backend, contracts)
        if not verdict.app_ready:
            raise AppReadinessBlockedError(verdict.format_report())
        return verdict


# -----------------------------------------------------------------------------
# Target Tuning Coordinator (Write-Ahead Journal, Execution, Crash Recovery)
# -----------------------------------------------------------------------------

class TargetTuningCoordinator:
    """Manages the lifecycle of temporary target ingest tuning contracts.

    Enforces:
    1. Durability bounds and scope discipline before applying any change.
    2. Complete tail + crash-recovery pricing; refuses unjustified changes.
    3. Write-ahead restore journal written and fsynced to disk before any target mutation.
    4. Active lease verification before every batch; automatic restoration on lease expiry.
    5. Restart-safe, idempotent O(1) restoration of settings, indexes, constraints,
       triggers, and ANALYZE statistics after crashes during load or mid-restoration.
    """

    def __init__(
        self,
        state_dir: Union[str, Path],
        backend: TargetTuningBackend,
        policy: Optional[DurabilityBoundsPolicy] = None,
        cost_model: Optional[TailAndRecoveryCostModel] = None,
    ) -> None:
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.backend = backend
        self.policy = policy or DurabilityBoundsPolicy()
        self.cost_model = cost_model or TailAndRecoveryCostModel()
        self._active_lease: Optional[TuningContractLease] = None

    def journal_path(self, contract_id: str) -> Path:
        return self.state_dir / f"tuning-contract-{contract_id}.json"

    def save_contract(self, contract: TargetTuningContract) -> Path:
        path = self.journal_path(contract.contract_id)
        durable.write_json(path, contract.to_dict())
        return path

    def load_contract(self, contract_id: str) -> TargetTuningContract:
        path = self.journal_path(contract_id)
        raw = durable.read_state(path, "journal")
        if not isinstance(raw, dict):
            raise RestoreVerificationError(
                f"[ACTION] Missing or empty tuning contract journal at {path}"
            )
        return TargetTuningContract.from_dict(raw)

    def begin_contract(
        self,
        contract_id: str,
        proposed_settings: Dict[str, Tuple[Any, SettingScope, Optional[str]]],
        total_rows: int,
        total_bytes: int,
        defer_indexes: bool = True,
        defer_constraints: bool = True,
        defer_triggers: bool = True,
        crash_replayable: bool = True,
        parallel_workers: int = 2,
        lease_duration_seconds: Optional[float] = None,
        require_justified: bool = True,
        now: Optional[float] = None,
    ) -> TargetTuningContract:
        """Validate bounds, price tails, persist write-ahead journal, acquire lease, and apply tuning."""
        ts = float(now if now is not None else time.time())
        lease_sec = float(
            lease_duration_seconds
            if lease_duration_seconds is not None
            else self.policy.max_lease_duration_seconds
        )
        if lease_sec <= 0 or lease_sec > self.policy.max_lease_duration_seconds:
            raise DurabilityBoundViolationError(
                f"[ACTION] Requested lease duration {lease_sec}s exceeds approved maximum "
                f"{self.policy.max_lease_duration_seconds}s"
            )

        # 1. Validate every proposed setting against DurabilityBoundsPolicy and scope rules
        mem_mb = 512
        highest_scope = SettingScope.SESSION
        for name, (tuned_val, scope, _tbl) in proposed_settings.items():
            self.policy.validate_setting_change(
                engine=self.backend.engine,
                setting_name=name,
                tuned_value=tuned_val,
                scope=scope,
                crash_replayable=crash_replayable,
                session_alternative_available=True,
            )
            if name.strip().lower() == "maintenance_work_mem":
                mem_mb = _parse_memory_mb(tuned_val)
            if scope == SettingScope.GLOBAL:
                highest_scope = SettingScope.GLOBAL
            elif scope == SettingScope.TABLE and highest_scope == SettingScope.SESSION:
                highest_scope = SettingScope.TABLE

        if defer_indexes and not self.policy.allow_deferred_indexes:
            raise DurabilityBoundViolationError(
                "[ACTION] Deferred indexing is disallowed by DurabilityBoundsPolicy"
            )
        if defer_constraints and not self.policy.allow_constraint_deferral:
            raise DurabilityBoundViolationError(
                "[ACTION] Constraint deferral is disallowed by DurabilityBoundsPolicy"
            )
        if defer_triggers and not self.policy.allow_trigger_deferral:
            raise DurabilityBoundViolationError(
                "[ACTION] Trigger deferral is disallowed by DurabilityBoundsPolicy"
            )

        # 2. Observe original settings, indexes, constraints, and triggers BEFORE touching target
        snapshot = self.backend.observe_snapshot(
            proposed_settings=proposed_settings,
            defer_indexes=defer_indexes,
            defer_constraints=defer_constraints,
            defer_triggers=defer_triggers,
            now=ts,
        )

        # 3. Price bulk ingest + index rebuild + constraint validation + ANALYZE + crash recovery
        _choice, justification = select_tuning_contract_way(
            policy=self.policy,
            cost_model=self.cost_model,
            total_rows=total_rows,
            total_bytes=total_bytes,
            secondary_index_count=len(snapshot.indexes),
            fk_constraint_count=len(snapshot.constraints),
            crash_replayable=crash_replayable,
            maintenance_work_mem_mb=mem_mb,
            parallel_workers=parallel_workers,
            requested_scope=highest_scope if highest_scope != SettingScope.GLOBAL else SettingScope.SESSION,
        )

        checkpoint = DurableRestoreCheckpoint(contract_id=contract_id, updated_at=ts)

        if not justification.justified:
            contract = TargetTuningContract(
                contract_id=contract_id,
                engine=self.backend.engine,
                database=self.backend.database,
                target_host=self.backend.host,
                phase=ContractPhase.REJECTED_UNJUSTIFIED,
                policy=self.policy,
                snapshot=snapshot,
                justification=justification,
                checkpoint=checkpoint,
                created_at=ts,
                lease_expires_at=ts,
                audit_log=[justification.reason],
            )
            self.save_contract(contract)
            if require_justified:
                raise UnjustifiedTuningError(justification.reason)
            return contract

        # If session_parameter_contract was chosen because index/constraint tails were too expensive,
        # keep indexes and constraints in place!
        if justification.selected_rung == "session_parameter_contract":
            snapshot.indexes.clear()
            snapshot.constraints.clear()

        # 4. Acquire active mutual-exclusion lease (.run.lock)
        lease = TuningContractLease(
            state_dir=self.state_dir,
            contract_id=contract_id,
            lease_duration_seconds=lease_sec,
        )
        lease.acquire(now=ts)
        self._active_lease = lease

        # 5. Persist write-ahead restore journal BEFORE applying any change to the target
        contract = TargetTuningContract(
            contract_id=contract_id,
            engine=self.backend.engine,
            database=self.backend.database,
            target_host=self.backend.host,
            phase=ContractPhase.OBSERVED_AND_JOURNALED,
            policy=self.policy,
            snapshot=snapshot,
            justification=justification,
            checkpoint=checkpoint,
            created_at=ts,
            lease_expires_at=lease.expires_at,
            audit_log=[
                f"[PASS] Observed original snapshot fingerprint={snapshot.fingerprint()}",
                justification.reason,
            ],
        )
        self.save_contract(contract)

        # 6. Apply tuning to target; if anything fails mid-apply, immediately restore
        try:
            applied_sql = self.backend.apply_tuning(contract.snapshot)
            contract.phase = ContractPhase.ACTIVE_LOAD
            contract.audit_log.append(
                f"[REVIEW] Applied {len(applied_sql)} temporary tuning statement(s) under lease"
            )
            self.save_contract(contract)
        except Exception as exc:
            contract.audit_log.append(f"[ACTION] Failed while applying tuning ({exc}); restoring immediately")
            self.save_contract(contract)
            self.restore_contract(contract.contract_id, now=ts)
            raise

        return contract

    def execute_load_range(
        self,
        contract: TargetTuningContract,
        range_id: str,
        cursor_value: Any,
        load_fn: Optional[Callable[[], None]] = None,
        now: Optional[float] = None,
    ) -> bool:
        """Execute a load range under the contract lease with O(1) checkpoint skip.

        Returns False if the range was already completed in O(1); True if executed now.
        If the contract lease has expired, automatically restores target protections
        and raises `ContractLeaseExpiredError`.
        """
        ts = float(now if now is not None else time.time())

        # O(1) skip for already-checkpointed range
        if contract.checkpoint.is_range_done(range_id):
            return False

        # Verify lease expiration before writing
        try:
            if self._active_lease is None or self._active_lease.contract_id != contract.contract_id:
                raise ContractLeaseError(f"[ACTION] No active lease for contract {contract.contract_id}")
            if ts > contract.lease_expires_at:
                raise ContractLeaseExpiredError(
                    f"[ACTION] Contract {contract.contract_id} lease expired at {contract.lease_expires_at:.3f}"
                )
            self._active_lease.verify(now=ts)
        except ContractLeaseExpiredError:
            contract.phase = ContractPhase.EXPIRED_UNRESTORED
            contract.audit_log.append(
                f"[ACTION] Lease expired before range {range_id}; triggering emergency restore"
            )
            self.save_contract(contract)
            self.restore_contract(contract.contract_id, now=ts)
            raise

        if load_fn is not None:
            load_fn()

        contract.checkpoint.mark_range_done(range_id, cursor_value, now=ts)
        self.save_contract(contract)
        return True

    def restore_contract(
        self,
        contract_id: str,
        measured_ingest_seconds: float = 0.0,
        now: Optional[float] = None,
    ) -> TargetTuningContract:
        """Idempotently restore all settings, rebuild indexes, validate constraints, and run ANALYZE.

        Uses `contract.checkpoint.completed_restore_steps` to skip already-completed
        restoration steps in O(1) across restarts after a mid-restoration crash.
        """
        ts = float(now if now is not None else time.time())
        contract = self.load_contract(contract_id)
        snap = contract.snapshot
        ckpt = contract.checkpoint

        t_restore_start = time.monotonic()

        # Phase 1: Restore original durability & memory settings
        contract.phase = ContractPhase.RESTORING_SETTINGS
        self.save_contract(contract)
        for name, s in sorted(snap.settings.items()):
            step_id = s.restore_step_id()
            if ckpt.is_restore_step_done(step_id) and str(self.backend.live_settings.get(name)) == str(
                s.original_value
            ):
                s.restored = True
                continue
            if s.scope == SettingScope.SESSION:
                if self.backend.engine.startswith("postgres"):
                    sql = f"SET SESSION {name} = '{s.original_value}';"
                else:
                    val = s.original_value if isinstance(s.original_value, int) else f"'{s.original_value}'"
                    sql = f"SET SESSION {name} = {val};"
            elif s.scope == SettingScope.TABLE and s.table_name:
                sql = f"ALTER TABLE {s.table_name} SET ({name} = {s.original_value});"
            else:
                if self.backend.engine.startswith("postgres"):
                    sql = f"ALTER SYSTEM SET {name} = '{s.original_value}'; SELECT pg_reload_conf();"
                else:
                    val = s.original_value if isinstance(s.original_value, int) else f"'{s.original_value}'"
                    sql = f"SET GLOBAL {name} = {val};"
            self.backend.execute_sql(sql)
            self.backend.live_settings[name] = s.original_value
            s.restored = True
            s.restored_at = ts
            ckpt.mark_restore_step_done(step_id, now=ts)
            self.save_contract(contract)

        t_settings_done = time.monotonic()

        # Phase 2: Rebuild deferred secondary indexes and verify indisvalid
        contract.phase = ContractPhase.REBUILDING_INDEXES
        self.save_contract(contract)
        t_ix_start = time.monotonic()
        for key, ix in sorted(snap.indexes.items()):
            step_id = ix.restore_step_id()
            live_ix = self.backend.live_indexes.get(key, {})
            if (
                ckpt.is_restore_step_done(step_id)
                and live_ix.get("present", False)
                and live_ix.get("indisvalid", False)
            ):
                ix.rebuilt = True
                ix.validated = True
                continue
            rebuild_sql = indexes.concurrent_index_sql(ix.create_ddl, engine=self.backend.engine)
            self.backend.execute_sql(rebuild_sql)
            self.backend.live_indexes[key] = {
                "record": copy.deepcopy(ix),
                "present": True,
                "indisvalid": True,
            }
            ix.rebuilt = True
            ix.validated = True
            ckpt.mark_restore_step_done(step_id, now=ts)
            self.save_contract(contract)
        t_ix_done = time.monotonic()

        # Phase 3: Re-add and explicitly VALIDATE deferred constraints in topological order
        contract.phase = ContractPhase.VALIDATING_CONSTRAINTS
        self.save_contract(contract)
        t_con_start = time.monotonic()
        fk_map = {
            k: {
                "on": f"{c.schema}.{c.table}",
                "name": c.constraint_name,
                "def": c.definition_ddl,
                "valid": c.originally_validated,
                "table": (c.schema, c.table),
                "ref_table": (c.referenced_schema, c.referenced_table) if c.referenced_table else (None, None),
            }
            for k, c in snap.constraints.items()
        }
        generations = indexes.topological_fk_sort(fk_map) if fk_map else []
        ordered_keys = [k for gen in generations for k in gen]
        for k in sorted(snap.constraints):
            if k not in ordered_keys:
                ordered_keys.append(k)

        for key in ordered_keys:
            con = snap.constraints[key]
            step_id = con.restore_step_id()
            live_con = self.backend.live_constraints.get(key, {})
            if (
                ckpt.is_restore_step_done(step_id)
                and live_con.get("present", False)
                and (not con.originally_validated or live_con.get("convalidated", False))
            ):
                con.restored = True
                con.validated = True
                continue

            if not live_con.get("present", False):
                add_sql = (
                    f"ALTER TABLE {con.schema}.{con.table} ADD CONSTRAINT "
                    f"{con.constraint_name} {con.definition_ddl} NOT VALID;"
                )
                self.backend.execute_sql(add_sql)
                self.backend.live_constraints[key] = {
                    "record": copy.deepcopy(con),
                    "present": True,
                    "convalidated": False,
                }
                con.restored = True
                self.save_contract(contract)

            if con.originally_validated:
                if key in self.backend.constraint_violations:
                    contract.phase = ContractPhase.FAILED_RESTORE
                    reason = self.backend.constraint_violations[key]
                    contract.audit_log.append(
                        f"[ACTION] Constraint validation failed for {key}: {reason}"
                    )
                    self.save_contract(contract)
                    raise RestoreVerificationError(
                        f"[ACTION] Constraint {key} failed post-load validation: {reason}"
                    )
                val_sql = (
                    f"ALTER TABLE {con.schema}.{con.table} VALIDATE CONSTRAINT {con.constraint_name};"
                )
                self.backend.execute_sql(val_sql)
                self.backend.live_constraints[key]["convalidated"] = True
                con.validated = True
            else:
                con.validated = True

            ckpt.mark_restore_step_done(step_id, now=ts)
            self.save_contract(contract)
        t_con_done = time.monotonic()

        # Phase 4: Re-enable deferred triggers
        contract.phase = ContractPhase.RESTORING_TRIGGERS
        self.save_contract(contract)
        for key, trg in sorted(snap.triggers.items()):
            step_id = trg.restore_step_id()
            live_trg = self.backend.live_triggers.get(key, {})
            if ckpt.is_restore_step_done(step_id) and live_trg.get("enabled", False):
                trg.restored = True
                continue
            en_sql = f"ALTER TABLE {trg.schema}.{trg.table} ENABLE TRIGGER {trg.trigger_name};"
            self.backend.execute_sql(en_sql)
            self.backend.live_triggers[key] = {
                "record": copy.deepcopy(trg),
                "enabled": True,
            }
            trg.restored = True
            ckpt.mark_restore_step_done(step_id, now=ts)
            self.save_contract(contract)

        # Phase 5: Run post-load ANALYZE statistics collection on all tables in scope
        contract.phase = ContractPhase.RUNNING_ANALYZE
        self.save_contract(contract)
        t_an_start = time.monotonic()
        for tbl in snap.tables_in_scope:
            step_id = f"analyze:{tbl}"
            if ckpt.is_restore_step_done(step_id) and tbl in self.backend.analyzed_tables:
                ckpt.analyzed_tables.add(tbl)
                continue
            if self.backend.engine.startswith("postgres"):
                an_sql = f"ANALYZE {tbl};"
            else:
                an_sql = f"ANALYZE TABLE {tbl};"
            self.backend.execute_sql(an_sql)
            self.backend.analyzed_tables.add(tbl)
            ckpt.analyzed_tables.add(tbl)
            ckpt.mark_restore_step_done(step_id, now=ts)
            self.save_contract(contract)
        t_an_done = time.monotonic()

        # Phase 6: Record measured completion evidence and verify readiness
        cb = contract.justification.cost_breakdown
        measured_ix = max(cb.index_rebuild_tail_seconds, t_ix_done - t_ix_start)
        measured_con = max(cb.constraint_validation_tail_seconds, t_con_done - t_con_start)
        measured_an = max(cb.analyze_tail_seconds, t_an_done - t_an_start)
        measured_rest = max(cb.restore_settings_seconds, t_settings_done - t_restore_start)
        eff_ingest = measured_ingest_seconds if measured_ingest_seconds > 0 else cb.tuned_ingest_seconds

        contract.measured_evidence = MeasuredCompletionEvidence(
            baseline_estimated_seconds=cb.baseline_ingest_seconds,
            measured_ingest_seconds=eff_ingest,
            measured_index_rebuild_seconds=measured_ix,
            measured_constraint_validation_seconds=measured_con,
            measured_analyze_seconds=measured_an,
            measured_restore_seconds=measured_rest,
            per_change_savings_seconds={
                "ingest_speedup": max(0.0, cb.baseline_ingest_seconds - eff_ingest),
                "net_after_tails": cb.baseline_ingest_seconds - (
                    eff_ingest + measured_ix + measured_con + measured_an + measured_rest
                ),
            },
        )

        contract.phase = ContractPhase.RESTORED_AND_VERIFIED
        contract.restored_at = ts

        # Verify against AppReadinessGate before finalizing
        verdict = AppReadinessGate.evaluate(self.backend, [contract])
        if not verdict.app_ready:
            contract.phase = ContractPhase.FAILED_RESTORE
            self.save_contract(contract)
            raise RestoreVerificationError(verdict.format_report())

        contract.audit_log.append(
            f"[PASS] Contract {contract.contract_id} fully restored and verified at {ts:.3f}"
        )
        self.save_contract(contract)

        if self._active_lease and self._active_lease.contract_id == contract.contract_id:
            self._active_lease.release()
            self._active_lease = None

        return contract

    def recover_crashed_contracts(self, now: Optional[float] = None) -> List[TargetTuningContract]:
        """Scan state_dir for any un-restored or mid-restoration contracts left by a crash and restore them."""
        ts = float(now if now is not None else time.time())
        recovered: List[TargetTuningContract] = []
        for path in sorted(self.state_dir.glob("tuning-contract-*.json")):
            raw = durable.read_state(path, "journal")
            if not isinstance(raw, dict):
                continue
            contract = TargetTuningContract.from_dict(raw)
            if contract.phase in (
                ContractPhase.RESTORED_AND_VERIFIED,
                ContractPhase.REJECTED_UNJUSTIFIED,
            ):
                continue
            contract.audit_log.append(
                f"[REVIEW] Crash recovery detected contract {contract.contract_id} in phase {contract.phase.value}"
            )
            self.save_contract(contract)
            restored = self.restore_contract(contract.contract_id, now=ts)
            recovered.append(restored)

        # Ensure any stale lock file from a crashed process is cleaned up once all contracts are restored
        lock_file = self.state_dir / ".run.lock"
        if lock_file.exists():
            lock_file.unlink(missing_ok=True)

        return recovered
