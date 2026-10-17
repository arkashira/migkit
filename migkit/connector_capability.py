"""Model schema drift and limitations per connector version (Row 640: RCH32.connector-capability).

Snapshots feature matrices by (engine, service, version, connector, mode) and
distinguishes:
- added columns (`ADD_COLUMN`)
- deleted columns (`DELETE_COLUMN`)
- renamed columns (`RENAME_COLUMN`)
- type-changed columns (`CHANGE_COLUMN_TYPE`, separating widening from narrowing)
- old-row backfill (`OLD_ROW_BACKFILL`)
- new-table discovery (`NEW_TABLE_DISCOVERY`)

Enforces:
1. Unknown, preview-only, or contradictory documentation claims never infer
   "yes" (`auto_admit=False`); they trigger bounded sandbox probes and record
   owned capability debt in a durable ledger.
2. A connector that supports `ADD_COLUMN` inline but requires a full refresh
   (`REQUIRES_TABLE_RESYNC`) for `RENAME_COLUMN` cannot auto-admit `RENAME_COLUMN`.
3. Stale connector or engine version metadata (version drift, digest mismatch,
   or expired snapshot TTL) invalidates any prior admission decision.
4. All admission choices climb `migkit.decide` rungs (`decide_drift_admission`),
   persist state in O(1) typed durable checkpoints via `migkit.durable`, and
   coordinate under an active mutual-exclusion `.run.lock` lease.

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
import os
import re
import socket
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable
from .schema_drift import BackfillTask, check_type_widening, parse_type_signature


# -----------------------------------------------------------------------------
# Care Badges and Core Enums
# -----------------------------------------------------------------------------

class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class ReplicationMode(str, enum.Enum):
    CDC_LOG = "cdc_log"
    CDC_CHANGE_TRACKING = "cdc_change_tracking"
    INCREMENTAL_CURSOR = "incremental_cursor"
    SNAPSHOT_AND_STREAM = "snapshot_and_stream"
    BATCH_SNAPSHOT = "batch_snapshot"


class DriftOperation(str, enum.Enum):
    ADD_COLUMN = "add_column"
    DELETE_COLUMN = "delete_column"
    RENAME_COLUMN = "rename_column"
    CHANGE_COLUMN_TYPE = "change_column_type"
    OLD_ROW_BACKFILL = "old_row_backfill"
    NEW_TABLE_DISCOVERY = "new_table_discovery"


class SupportDisposition(str, enum.Enum):
    AUTO_INLINE = "auto_inline"
    REQUIRES_BACKFILL_TASK = "requires_backfill_task"
    REQUIRES_SCHEMA_EPOCH_FENCE = "requires_schema_epoch_fence"
    REQUIRES_TABLE_RESYNC = "requires_table_resync"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"
    CONTRADICTORY = "contradictory"


class DocReleaseStatus(str, enum.Enum):
    GA = "ga"
    PREVIEW = "preview"
    LEGACY = "legacy"
    ROADMAP = "roadmap"
    MARKETING = "marketing"
    INFERRED = "inferred"


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

class ConnectorCapabilityError(RuntimeError):
    """Base error for connector capability modeling and schema drift admission."""

    def __init__(self, message: str, care_badge: CareBadge = CareBadge.ACTION):
        super().__init__(message)
        self.message = message
        self.care_badge = care_badge


class UnsafeProbeHostError(ConnectorCapabilityError, ValueError):
    """Raised when a bounded probe target host is outside approved safe test ranges."""


class StaleCapabilityMetadataError(ConnectorCapabilityError):
    """Raised when attempting to execute a decision whose connector or engine metadata is stale."""


class CapabilityLeaseError(ConnectorCapabilityError):
    """Raised when a `.run.lock` mutual-exclusion lease is held by another active runner."""


class CapabilityLeaseExpiredError(CapabilityLeaseError):
    """Raised when an active `.run.lock` lease expires before operation completion."""


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


# -----------------------------------------------------------------------------
# Version Parsing and Matching
# -----------------------------------------------------------------------------

def parse_version_tuple(version_str: str) -> Tuple[int, ...]:
    """Parse a dotted version string like '15.4.1' or 'v2.4.0-rc1' into integer components."""
    raw = str(version_str or "").strip()
    if not raw:
        return (0,)
    nums = [int(tok) for tok in re.findall(r"\d+", raw)]
    return tuple(nums) if nums else (0,)


def compare_versions(v1: str, v2: str) -> int:
    """Compare two version strings: returns -1 if v1 < v2, 0 if equal, 1 if v1 > v2."""
    t1 = list(parse_version_tuple(v1))
    t2 = list(parse_version_tuple(v2))
    max_len = max(len(t1), len(t2))
    t1.extend([0] * (max_len - len(t1)))
    t2.extend([0] * (max_len - len(t2)))
    if t1 < t2:
        return -1
    if t1 > t2:
        return 1
    return 0


def version_satisfies(version: str, spec: str) -> bool:
    """Evaluate whether `version` satisfies a range spec like '>=2.2.0,<3.0.0' or '*' or '2.4.0'."""
    clean_spec = str(spec or "*").strip()
    if not clean_spec or clean_spec == "*":
        return True
    for part in clean_spec.split(","):
        token = part.strip()
        if not token:
            continue
        if token.startswith(">="):
            if compare_versions(version, token[2:].strip()) < 0:
                return False
        elif token.startswith("<="):
            if compare_versions(version, token[2:].strip()) > 0:
                return False
        elif token.startswith(">"):
            if compare_versions(version, token[1:].strip()) <= 0:
                return False
        elif token.startswith("<"):
            if compare_versions(version, token[1:].strip()) >= 0:
                return False
        elif token.startswith("=="):
            if compare_versions(version, token[2:].strip()) != 0:
                return False
        elif token.startswith("!="):
            if compare_versions(version, token[2:].strip()) == 0:
                return False
        else:
            if compare_versions(version, token) != 0:
                return False
    return True


# -----------------------------------------------------------------------------
# Feature Matrix Snapshot Data Structures
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ConnectorMatrixCoordinate:
    """Exact coordinate identifying an engine, service, version, connector, and mode."""
    engine: str
    service: str
    engine_version: str
    connector: str
    connector_version: str
    mode: ReplicationMode

    def normalized(self) -> "ConnectorMatrixCoordinate":
        return ConnectorMatrixCoordinate(
            engine=self.engine.strip().lower(),
            service=self.service.strip().lower(),
            engine_version=self.engine_version.strip(),
            connector=self.connector.strip().lower(),
            connector_version=self.connector_version.strip(),
            mode=ReplicationMode(self.mode),
        )

    def to_dict(self) -> Dict[str, str]:
        c = self.normalized()
        return {
            "engine": c.engine,
            "service": c.service,
            "engine_version": c.engine_version,
            "connector": c.connector,
            "connector_version": c.connector_version,
            "mode": c.mode.value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectorMatrixCoordinate":
        return cls(
            engine=str(data["engine"]),
            service=str(data["service"]),
            engine_version=str(data["engine_version"]),
            connector=str(data["connector"]),
            connector_version=str(data["connector_version"]),
            mode=ReplicationMode(data["mode"]),
        )


@dataclass(frozen=True)
class DocSourceClaim:
    """A single documentation or release-note claim regarding a drift operation."""
    source_id: str
    operation: DriftOperation
    claimed_disposition: SupportDisposition
    release_status: DocReleaseStatus = DocReleaseStatus.GA
    engine_version_spec: str = "*"
    connector_version_spec: str = "*"
    widening_only: bool = False
    backfill_historical_rows: Optional[bool] = None
    discovers_new_tables_automatically: Optional[bool] = None
    preserves_column_identity_on_rename: bool = False
    url_or_ref: str = ""
    region_or_edition: str = "global"
    notes: str = ""

    def applies_to(self, coord: ConnectorMatrixCoordinate) -> bool:
        return (
            version_satisfies(coord.engine_version, self.engine_version_spec)
            and version_satisfies(coord.connector_version, self.connector_version_spec)
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "operation": self.operation.value,
            "claimed_disposition": self.claimed_disposition.value,
            "release_status": self.release_status.value,
            "engine_version_spec": self.engine_version_spec,
            "connector_version_spec": self.connector_version_spec,
            "widening_only": self.widening_only,
            "backfill_historical_rows": self.backfill_historical_rows,
            "discovers_new_tables_automatically": self.discovers_new_tables_automatically,
            "preserves_column_identity_on_rename": self.preserves_column_identity_on_rename,
            "url_or_ref": self.url_or_ref,
            "region_or_edition": self.region_or_edition,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class CapabilityCell:
    """Resolved support cell for a single `DriftOperation` at a matrix coordinate."""
    operation: DriftOperation
    disposition: SupportDisposition
    release_status: DocReleaseStatus = DocReleaseStatus.GA
    widening_only: bool = False
    backfills_old_rows_inline: bool = False
    auto_discovers_new_tables: bool = False
    preserves_rename_without_resync: bool = False
    requires_probe: bool = False
    contradiction_reasons: Tuple[str, ...] = ()
    source_refs: Tuple[str, ...] = ()
    notes: str = ""

    @property
    def is_ambiguous_or_unverified(self) -> bool:
        return (
            self.disposition in (SupportDisposition.UNKNOWN, SupportDisposition.CONTRADICTORY)
            or self.release_status in (
                DocReleaseStatus.PREVIEW,
                DocReleaseStatus.ROADMAP,
                DocReleaseStatus.MARKETING,
                DocReleaseStatus.INFERRED,
            )
            or self.requires_probe
            or bool(self.contradiction_reasons)
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation": self.operation.value,
            "disposition": self.disposition.value,
            "release_status": self.release_status.value,
            "widening_only": self.widening_only,
            "backfills_old_rows_inline": self.backfills_old_rows_inline,
            "auto_discovers_new_tables": self.auto_discovers_new_tables,
            "preserves_rename_without_resync": self.preserves_rename_without_resync,
            "requires_probe": self.requires_probe,
            "contradiction_reasons": list(self.contradiction_reasons),
            "source_refs": list(self.source_refs),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ConnectorCapabilitySnapshot:
    """Immutable point-in-time feature matrix snapshot for a connector coordinate."""
    coordinate: ConnectorMatrixCoordinate
    cells: Dict[DriftOperation, CapabilityCell]
    captured_at: float
    max_metadata_age_seconds: float = 86400.0
    snapshot_id: str = field(default_factory=lambda: f"snap-{uuid.uuid4().hex[:12]}")

    @property
    def digest(self) -> str:
        payload = {
            "coordinate": self.coordinate.to_dict(),
            "cells": {op.value: self.cells[op].to_dict() for op in sorted(self.cells, key=lambda x: x.value)},
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def cell_for(self, operation: DriftOperation) -> CapabilityCell:
        op = DriftOperation(operation)
        if op in self.cells:
            return self.cells[op]
        return CapabilityCell(
            operation=op,
            disposition=SupportDisposition.UNKNOWN,
            release_status=DocReleaseStatus.INFERRED,
            requires_probe=True,
            contradiction_reasons=(f"no documented capability entry for {op.value}",),
            notes="missing from snapshot matrix; never inferred as supported",
        )

    def is_stale(
        self,
        *,
        now: Optional[float] = None,
        live_coordinate: Optional[ConnectorMatrixCoordinate] = None,
        expected_digest: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        current_time = time.time() if now is None else float(now)
        age = current_time - self.captured_at
        if age > self.max_metadata_age_seconds:
            return (
                True,
                f"snapshot metadata age {age:.1f}s exceeds max_metadata_age_seconds {self.max_metadata_age_seconds:.1f}s",
            )
        if live_coordinate is not None:
            norm_self = self.coordinate.normalized()
            norm_live = live_coordinate.normalized()
            if norm_self.connector != norm_live.connector:
                return (
                    True,
                    f"connector changed from {norm_self.connector!r} to {norm_live.connector!r}",
                )
            if compare_versions(norm_self.connector_version, norm_live.connector_version) != 0:
                return (
                    True,
                    f"connector_version drifted from {norm_self.connector_version!r} to {norm_live.connector_version!r}",
                )
            if norm_self.engine != norm_live.engine or norm_self.service != norm_live.service:
                return (
                    True,
                    f"engine/service drifted from {norm_self.engine}/{norm_self.service} "
                    f"to {norm_live.engine}/{norm_live.service}",
                )
            if compare_versions(norm_self.engine_version, norm_live.engine_version) != 0:
                return (
                    True,
                    f"engine_version drifted from {norm_self.engine_version!r} to {norm_live.engine_version!r}",
                )
            if norm_self.mode != norm_live.mode:
                return (
                    True,
                    f"replication mode drifted from {norm_self.mode.value!r} to {norm_live.mode.value!r}",
                )
        if expected_digest is not None and expected_digest != self.digest:
            return (
                True,
                f"snapshot digest mismatch: expected {expected_digest[:12]}, got {self.digest[:12]}",
            )
        return False, None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "coordinate": self.coordinate.to_dict(),
            "captured_at": self.captured_at,
            "max_metadata_age_seconds": self.max_metadata_age_seconds,
            "digest": self.digest,
            "cells": {op.value: cell.to_dict() for op, cell in self.cells.items()},
        }


# -----------------------------------------------------------------------------
# Compiling Claims into a Feature Matrix Snapshot (Never Inferring "Yes")
# -----------------------------------------------------------------------------

def synthesize_capability_cell(
    coordinate: ConnectorMatrixCoordinate,
    operation: DriftOperation,
    claims: Sequence[DocSourceClaim],
) -> CapabilityCell:
    """Resolve documentation claims for a single operation without ever inferring 'yes'.

    Rules:
    - Zero matching claims -> `SupportDisposition.UNKNOWN`, `requires_probe=True`.
    - Multiple matching claims with conflicting dispositions, conflicting backfill
      guarantees, or conflicting rename identity -> `SupportDisposition.CONTRADICTORY`,
      `requires_probe=True`.
    - Any claim whose `release_status` is `PREVIEW`, `ROADMAP`, `MARKETING`, or
      `INFERRED` cannot yield unconditional `AUTO_INLINE` without a probe
      (`requires_probe=True`).
    """
    matching = [
        c for c in claims
        if c.operation == operation and c.applies_to(coordinate)
    ]
    if not matching:
        return CapabilityCell(
            operation=operation,
            disposition=SupportDisposition.UNKNOWN,
            release_status=DocReleaseStatus.INFERRED,
            requires_probe=True,
            contradiction_reasons=(
                f"no documentation claim covers {operation.value} for "
                f"{coordinate.connector}@{coordinate.connector_version} on "
                f"{coordinate.engine}/{coordinate.service}@{coordinate.engine_version} ({coordinate.mode.value})",
            ),
            notes="undocumented capability triggers bounded probe and owned debt",
        )

    dispositions = {c.claimed_disposition for c in matching}
    backfill_flags = {c.backfill_historical_rows for c in matching if c.backfill_historical_rows is not None}
    discovery_flags = {
        c.discovers_new_tables_automatically
        for c in matching
        if c.discovers_new_tables_automatically is not None
    }
    rename_flags = {c.preserves_column_identity_on_rename for c in matching}
    widening_flags = {c.widening_only for c in matching}
    source_refs = tuple(dict.fromkeys(c.source_id for c in matching))

    contradictions: List[str] = []
    if len(dispositions) > 1:
        disp_names = ", ".join(sorted(d.value for d in dispositions))
        contradictions.append(
            f"conflicting support dispositions across sources ({', '.join(source_refs)}): {disp_names}"
        )
    if len(backfill_flags) > 1:
        contradictions.append(
            f"conflicting old-row backfill claims across sources ({', '.join(source_refs)})"
        )
    if len(discovery_flags) > 1:
        contradictions.append(
            f"conflicting new-table discovery claims across sources ({', '.join(source_refs)})"
        )
    if operation == DriftOperation.RENAME_COLUMN and len(rename_flags) > 1:
        contradictions.append(
            f"conflicting column-rename identity claims across sources ({', '.join(source_refs)})"
        )
    if operation == DriftOperation.CHANGE_COLUMN_TYPE and len(widening_flags) > 1:
        contradictions.append(
            f"conflicting type-change widening constraints across sources ({', '.join(source_refs)})"
        )

    if contradictions:
        return CapabilityCell(
            operation=operation,
            disposition=SupportDisposition.CONTRADICTORY,
            release_status=matching[0].release_status,
            widening_only=any(widening_flags),
            backfills_old_rows_inline=False,
            auto_discovers_new_tables=False,
            preserves_rename_without_resync=False,
            requires_probe=True,
            contradiction_reasons=tuple(contradictions),
            source_refs=source_refs,
            notes="contradictory documentation claims refuse auto-admission and require bounded probe",
        )

    chosen = matching[0]
    non_ga = chosen.release_status != DocReleaseStatus.GA
    requires_probe = (
        non_ga
        or chosen.claimed_disposition in (SupportDisposition.UNKNOWN, SupportDisposition.CONTRADICTORY)
    )
    reasons: List[str] = []
    if non_ga:
        reasons.append(
            f"claim from {chosen.source_id} has non-GA release status {chosen.release_status.value!r}"
        )

    return CapabilityCell(
        operation=operation,
        disposition=chosen.claimed_disposition,
        release_status=chosen.release_status,
        widening_only=chosen.widening_only,
        backfills_old_rows_inline=bool(chosen.backfill_historical_rows),
        auto_discovers_new_tables=bool(chosen.discovers_new_tables_automatically),
        preserves_rename_without_resync=bool(chosen.preserves_column_identity_on_rename),
        requires_probe=requires_probe,
        contradiction_reasons=tuple(reasons),
        source_refs=source_refs,
        notes=chosen.notes,
    )


def build_snapshot_from_claims(
    coordinate: ConnectorMatrixCoordinate,
    claims: Sequence[DocSourceClaim],
    *,
    captured_at: Optional[float] = None,
    max_metadata_age_seconds: float = 86400.0,
) -> ConnectorCapabilitySnapshot:
    """Compile a full 6-operation `ConnectorCapabilitySnapshot` for `coordinate` from `claims`."""
    norm_coord = coordinate.normalized()
    ts = time.time() if captured_at is None else float(captured_at)
    cells: Dict[DriftOperation, CapabilityCell] = {}
    for op in DriftOperation:
        cells[op] = synthesize_capability_cell(norm_coord, op, claims)
    return ConnectorCapabilitySnapshot(
        coordinate=norm_coord,
        cells=cells,
        captured_at=ts,
        max_metadata_age_seconds=max_metadata_age_seconds,
    )


# -----------------------------------------------------------------------------
# Bounded Sandbox Probes for Unknown / Contradictory Capabilities
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ProbeBudget:
    """Strict resource ceiling for a bounded schema-drift capability probe."""
    max_probe_rows: int = 32
    max_probe_seconds: float = 5.0
    max_statements: int = 8
    target_host: str = "127.0.0.1"

    def validate(self) -> None:
        if not is_safe_test_host(self.target_host):
            raise UnsafeProbeHostError(
                f"bounded probe refused on non-test host {self.target_host!r}; "
                "only 127.0.0.1/localhost/::1 and 10.0.0.x are permitted",
                care_badge=CareBadge.ACTION,
            )
        if self.max_probe_rows <= 0 or self.max_probe_seconds <= 0 or self.max_statements <= 0:
            raise ConnectorCapabilityError(
                "probe budget limits (rows, seconds, statements) must all be positive",
                care_badge=CareBadge.ACTION,
            )


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of a bounded sandbox probe for a connector schema-drift operation."""
    probe_id: str
    coordinate: ConnectorMatrixCoordinate
    operation: DriftOperation
    passed: bool
    observed_disposition: SupportDisposition
    rows_probed: int
    statements_executed: int
    elapsed_seconds: float
    preserved_existing_rows: bool
    preserved_column_identity: bool
    backfilled_old_rows: bool
    discovered_new_table: bool
    failure_reason: Optional[str] = None
    executed_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "probe_id": self.probe_id,
            "coordinate": self.coordinate.to_dict(),
            "operation": self.operation.value,
            "passed": self.passed,
            "observed_disposition": self.observed_disposition.value,
            "rows_probed": self.rows_probed,
            "statements_executed": self.statements_executed,
            "elapsed_seconds": self.elapsed_seconds,
            "preserved_existing_rows": self.preserved_existing_rows,
            "preserved_column_identity": self.preserved_column_identity,
            "backfilled_old_rows": self.backfilled_old_rows,
            "discovered_new_table": self.discovered_new_table,
            "failure_reason": self.failure_reason,
            "executed_at": self.executed_at,
        }


def execute_bounded_probe(
    coordinate: ConnectorMatrixCoordinate,
    operation: DriftOperation,
    budget: ProbeBudget,
    probe_runner: Optional[Callable[[ConnectorMatrixCoordinate, DriftOperation, ProbeBudget], Dict[str, Any]]] = None,
) -> ProbeResult:
    """Run a bounded probe on a safe test endpoint to falsify or confirm an unknown capability.

    If `probe_runner` is None or raises an exception, or if the probe exceeds
    `budget`, the result is `passed=False` with `SupportDisposition.UNKNOWN`
    and an explicit `failure_reason`. Never infers success without proof.
    """
    budget.validate()
    norm_coord = coordinate.normalized()
    op = DriftOperation(operation)
    probe_id = f"probe-{uuid.uuid4().hex[:10]}"

    if probe_runner is None:
        return ProbeResult(
            probe_id=probe_id,
            coordinate=norm_coord,
            operation=op,
            passed=False,
            observed_disposition=SupportDisposition.UNKNOWN,
            rows_probed=0,
            statements_executed=0,
            elapsed_seconds=0.0,
            preserved_existing_rows=False,
            preserved_column_identity=False,
            backfilled_old_rows=False,
            discovered_new_table=False,
            failure_reason="no sandbox probe runner supplied; refusing to infer support",
        )

    started = time.monotonic()
    try:
        raw = probe_runner(norm_coord, op, budget)
    except Exception as exc:  # noqa: BLE001
        elapsed = time.monotonic() - started
        return ProbeResult(
            probe_id=probe_id,
            coordinate=norm_coord,
            operation=op,
            passed=False,
            observed_disposition=SupportDisposition.UNSUPPORTED,
            rows_probed=0,
            statements_executed=0,
            elapsed_seconds=elapsed,
            preserved_existing_rows=False,
            preserved_column_identity=False,
            backfilled_old_rows=False,
            discovered_new_table=False,
            failure_reason=f"probe execution failed: {exc}",
        )

    elapsed = float(raw.get("elapsed_seconds", time.monotonic() - started))
    rows_probed = int(raw.get("rows_probed", 0))
    stmts = int(raw.get("statements_executed", 0))

    if rows_probed > budget.max_probe_rows:
        return ProbeResult(
            probe_id=probe_id,
            coordinate=norm_coord,
            operation=op,
            passed=False,
            observed_disposition=SupportDisposition.UNKNOWN,
            rows_probed=rows_probed,
            statements_executed=stmts,
            elapsed_seconds=elapsed,
            preserved_existing_rows=False,
            preserved_column_identity=False,
            backfilled_old_rows=False,
            discovered_new_table=False,
            failure_reason=f"probe exceeded row budget ({rows_probed} > {budget.max_probe_rows})",
        )
    if stmts > budget.max_statements:
        return ProbeResult(
            probe_id=probe_id,
            coordinate=norm_coord,
            operation=op,
            passed=False,
            observed_disposition=SupportDisposition.UNKNOWN,
            rows_probed=rows_probed,
            statements_executed=stmts,
            elapsed_seconds=elapsed,
            preserved_existing_rows=False,
            preserved_column_identity=False,
            backfilled_old_rows=False,
            discovered_new_table=False,
            failure_reason=f"probe exceeded statement budget ({stmts} > {budget.max_statements})",
        )
    if elapsed > budget.max_probe_seconds:
        return ProbeResult(
            probe_id=probe_id,
            coordinate=norm_coord,
            operation=op,
            passed=False,
            observed_disposition=SupportDisposition.UNKNOWN,
            rows_probed=rows_probed,
            statements_executed=stmts,
            elapsed_seconds=elapsed,
            preserved_existing_rows=False,
            preserved_column_identity=False,
            backfilled_old_rows=False,
            discovered_new_table=False,
            failure_reason=f"probe exceeded time budget ({elapsed:.2f}s > {budget.max_probe_seconds:.2f}s)",
        )

    preserved_rows = bool(raw.get("preserved_existing_rows", False))
    preserved_identity = bool(raw.get("preserved_column_identity", False))
    backfilled = bool(raw.get("backfilled_old_rows", False))
    discovered = bool(raw.get("discovered_new_table", False))
    disp_val = raw.get("observed_disposition", SupportDisposition.UNKNOWN)
    observed_disp = SupportDisposition(disp_val)

    passed = bool(raw.get("passed", False)) and preserved_rows
    if op == DriftOperation.RENAME_COLUMN and not preserved_identity:
        passed = False
    if op == DriftOperation.OLD_ROW_BACKFILL and not backfilled:
        passed = False
    if op == DriftOperation.NEW_TABLE_DISCOVERY and not discovered:
        passed = False

    failure_reason = raw.get("failure_reason")
    if not passed and not failure_reason:
        failure_reason = (
            f"probe did not prove safe inline execution for {op.value} "
            f"(disposition={observed_disp.value}, preserved_rows={preserved_rows}, "
            f"preserved_identity={preserved_identity})"
        )

    return ProbeResult(
        probe_id=probe_id,
        coordinate=norm_coord,
        operation=op,
        passed=passed,
        observed_disposition=observed_disp,
        rows_probed=rows_probed,
        statements_executed=stmts,
        elapsed_seconds=elapsed,
        preserved_existing_rows=preserved_rows,
        preserved_column_identity=preserved_identity,
        backfilled_old_rows=backfilled,
        discovered_new_table=discovered,
        failure_reason=failure_reason,
    )


# -----------------------------------------------------------------------------
# Owned Capability Debt Ledger
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class OwnedCapabilityDebt:
    """Explicit owned debt item created whenever a capability is unknown, contradictory, or refused."""
    debt_id: str
    owner: str
    coordinate: ConnectorMatrixCoordinate
    table: str
    operation: DriftOperation
    category: str
    care_badge: CareBadge
    summary: str
    remediation: str
    contradiction_reasons: Tuple[str, ...] = ()
    probe_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)

    def format_line(self) -> str:
        return (
            f"[{self.care_badge.value}] debt_id={self.debt_id} owner={self.owner} "
            f"connector={self.coordinate.connector}@{self.coordinate.connector_version} "
            f"engine={self.coordinate.engine}/{self.coordinate.service}@{self.coordinate.engine_version} "
            f"mode={self.coordinate.mode.value} table={self.table} op={self.operation.value} "
            f"category={self.category} summary={self.summary} remediation={self.remediation}"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "debt_id": self.debt_id,
            "owner": self.owner,
            "coordinate": self.coordinate.to_dict(),
            "table": self.table,
            "operation": self.operation.value,
            "category": self.category,
            "care_badge": self.care_badge.value,
            "summary": self.summary,
            "remediation": self.remediation,
            "contradiction_reasons": list(self.contradiction_reasons),
            "probe_id": self.probe_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OwnedCapabilityDebt":
        return cls(
            debt_id=str(data["debt_id"]),
            owner=str(data["owner"]),
            coordinate=ConnectorMatrixCoordinate.from_dict(data["coordinate"]),
            table=str(data["table"]),
            operation=DriftOperation(data["operation"]),
            category=str(data["category"]),
            care_badge=CareBadge(data["care_badge"]),
            summary=str(data["summary"]),
            remediation=str(data["remediation"]),
            contradiction_reasons=tuple(data.get("contradiction_reasons", [])),
            probe_id=data.get("probe_id"),
            created_at=float(data.get("created_at", 0.0)),
        )


# -----------------------------------------------------------------------------
# Schema Drift Event and Decision Result
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class SchemaDriftEvent:
    """A schema change observed on the source stream or schema diff."""
    event_id: str
    table: str
    operation: DriftOperation
    column_name: Optional[str] = None
    new_column_name: Optional[str] = None
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    nullable: bool = True
    default_value: Optional[Any] = None
    requires_historical_backfill: bool = False
    estimated_rows: int = 1000
    cursor_position: Optional[Any] = None


@dataclass(frozen=True)
class DriftAdmissionDecision:
    """Decision record for a schema drift event bound to connector/version snapshot metadata."""
    decision_id: str
    event_id: str
    table: str
    operation: DriftOperation
    coordinate: ConnectorMatrixCoordinate
    snapshot_id: str
    snapshot_digest: str
    snapshot_captured_at: float
    max_metadata_age_seconds: float
    selected_rung: str
    auto_admit: bool
    requires_table_resync: bool
    requires_backfill_task: bool
    care_badge: CareBadge
    reason: str
    backfill_task: Optional[BackfillTask] = None
    probe_result: Optional[ProbeResult] = None
    debt_record: Optional[OwnedCapabilityDebt] = None
    decided_at: float = field(default_factory=time.time)

    def validate_freshness(
        self,
        *,
        live_coordinate: ConnectorMatrixCoordinate,
        current_snapshot: Optional[ConnectorCapabilitySnapshot] = None,
        now: Optional[float] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Check whether this decision remains valid for the live connector/engine metadata."""
        ts = time.time() if now is None else float(now)
        age = ts - self.snapshot_captured_at
        if age > self.max_metadata_age_seconds:
            return (
                False,
                f"stale capability snapshot: age {age:.1f}s > {self.max_metadata_age_seconds:.1f}s",
            )
        norm_bound = self.coordinate.normalized()
        norm_live = live_coordinate.normalized()
        if norm_bound.connector != norm_live.connector:
            return (
                False,
                f"connector changed from {norm_bound.connector!r} to {norm_live.connector!r}",
            )
        if compare_versions(norm_bound.connector_version, norm_live.connector_version) != 0:
            return (
                False,
                f"connector_version changed from {norm_bound.connector_version!r} to {norm_live.connector_version!r}",
            )
        if norm_bound.engine != norm_live.engine or norm_bound.service != norm_live.service:
            return (
                False,
                f"engine/service changed from {norm_bound.engine}/{norm_bound.service} "
                f"to {norm_live.engine}/{norm_live.service}",
            )
        if compare_versions(norm_bound.engine_version, norm_live.engine_version) != 0:
            return (
                False,
                f"engine_version changed from {norm_bound.engine_version!r} to {norm_live.engine_version!r}",
            )
        if norm_bound.mode != norm_live.mode:
            return (
                False,
                f"replication mode changed from {norm_bound.mode.value!r} to {norm_live.mode.value!r}",
            )
        if current_snapshot is not None and current_snapshot.digest != self.snapshot_digest:
            return (
                False,
                f"feature matrix digest changed from {self.snapshot_digest[:12]} to {current_snapshot.digest[:12]}",
            )
        return True, None

    def ensure_fresh(
        self,
        *,
        live_coordinate: ConnectorMatrixCoordinate,
        current_snapshot: Optional[ConnectorCapabilitySnapshot] = None,
        now: Optional[float] = None,
    ) -> None:
        ok, why = self.validate_freshness(
            live_coordinate=live_coordinate,
            current_snapshot=current_snapshot,
            now=now,
        )
        if not ok:
            raise StaleCapabilityMetadataError(
                f"[ACTION] decision {self.decision_id} for {self.table}.{self.operation.value} "
                f"invalidated by stale metadata: {why}",
                care_badge=CareBadge.ACTION,
            )

    def format_line(self) -> str:
        return (
            f"[{self.care_badge.value}] table={self.table} op={self.operation.value} "
            f"connector={self.coordinate.connector}@{self.coordinate.connector_version} "
            f"rung={self.selected_rung} auto_admit={self.auto_admit} "
            f"resync={self.requires_table_resync} backfill={self.requires_backfill_task} "
            f"reason={self.reason}"
        )


# -----------------------------------------------------------------------------
# Decision Engine Rung Ladder (`decide.Rung`)
# -----------------------------------------------------------------------------

def decide_drift_admission(
    snapshot: ConnectorCapabilitySnapshot,
    event: SchemaDriftEvent,
    *,
    live_coordinate: Optional[ConnectorMatrixCoordinate] = None,
    now: Optional[float] = None,
    probe_result: Optional[ProbeResult] = None,
    allow_backfill_task: bool = True,
    allow_table_resync_plan: bool = True,
    debt_owner: str = "migration-team",
) -> Tuple[decide.Choice, DriftAdmissionDecision]:
    """Climb the `migkit.decide` ladder to determine how a connector handles a schema drift event.

    Rungs:
    1. `auto_inline_admission`: Connector natively supports the drift operation inline
       with GA documentation, fresh metadata, compatible type change (if `CHANGE_COLUMN_TYPE`),
       and no unfulfilled old-row backfill requirement.
    2. `inline_with_backfill_admission`: Connector supports the schema addition or
       table discovery online when paired with a bounded backfill/bootstrap task.
    3. `probed_sandbox_admission`: Documentation was unknown, preview, or contradictory,
       and a bounded probe on a safe test endpoint proved inline preservation.
    4. `table_resync_required`: Connector requires a full table refresh/resync for this
       operation (e.g., `RENAME_COLUMN` or narrowing type change); auto-admission is
       refused (`auto_admit=False`) and a fenced table resync plan + debt is emitted.
    5. `refuse_and_record_debt`: Fallback rung when metadata is stale, the operation is
       unsupported, or unknown/contradictory docs lack a passing probe; blocks auto-admission
       and records owned debt.
    """
    effective_live = (live_coordinate or snapshot.coordinate).normalized()
    stale, stale_reason = snapshot.is_stale(now=now, live_coordinate=effective_live)
    cell = snapshot.cell_for(event.operation)
    backfill_cell = snapshot.cell_for(DriftOperation.OLD_ROW_BACKFILL)

    is_widening = True
    widening_detail: Optional[str] = None
    if event.operation == DriftOperation.CHANGE_COLUMN_TYPE:
        if event.old_type and event.new_type:
            is_widening, widening_detail = check_type_widening(event.old_type, event.new_type)
        else:
            is_widening = False

    type_change_compatible = True
    if event.operation == DriftOperation.CHANGE_COLUMN_TYPE:
        if cell.widening_only and not is_widening:
            type_change_compatible = False

    rename_safe_inline = True
    if event.operation == DriftOperation.RENAME_COLUMN:
        rename_safe_inline = (
            cell.disposition == SupportDisposition.AUTO_INLINE
            and cell.preserves_rename_without_resync
        )

    docs_unambiguous_ga = not cell.is_ambiguous_or_unverified
    inline_supported = (
        cell.disposition == SupportDisposition.AUTO_INLINE
        and type_change_compatible
        and rename_safe_inline
    )

    needs_old_row_backfill = (
        event.requires_historical_backfill
        or (event.operation == DriftOperation.ADD_COLUMN and event.default_value is not None)
        or event.operation == DriftOperation.OLD_ROW_BACKFILL
    )
    inline_backfill_satisfied = (
        not needs_old_row_backfill
        or (
            cell.backfills_old_rows_inline
            and backfill_cell.disposition == SupportDisposition.AUTO_INLINE
            and not backfill_cell.is_ambiguous_or_unverified
        )
    )

    backfill_task_supported = (
        allow_backfill_task
        and event.operation in (
            DriftOperation.ADD_COLUMN,
            DriftOperation.OLD_ROW_BACKFILL,
            DriftOperation.NEW_TABLE_DISCOVERY,
        )
        and cell.disposition in (
            SupportDisposition.AUTO_INLINE,
            SupportDisposition.REQUIRES_BACKFILL_TASK,
            SupportDisposition.REQUIRES_SCHEMA_EPOCH_FENCE,
        )
        and backfill_cell.disposition in (
            SupportDisposition.AUTO_INLINE,
            SupportDisposition.REQUIRES_BACKFILL_TASK,
        )
    )

    probe_passed = (
        probe_result is not None
        and probe_result.passed
        and probe_result.operation == event.operation
        and compare_versions(
            probe_result.coordinate.connector_version,
            effective_live.connector_version,
        ) == 0
    )

    resync_applicable = (
        allow_table_resync_plan
        and (
            cell.disposition == SupportDisposition.REQUIRES_TABLE_RESYNC
            or (
                event.operation == DriftOperation.CHANGE_COLUMN_TYPE
                and not type_change_compatible
                and cell.disposition in (
                    SupportDisposition.AUTO_INLINE,
                    SupportDisposition.REQUIRES_TABLE_RESYNC,
                )
            )
        )
    )

    rungs = (
        decide.Rung(
            "auto_inline_admission",
            "automatic inline schema drift admission",
            needs=(
                decide.Need(
                    "fresh connector and engine version metadata",
                    lambda f: f["metadata_fresh"],
                    lambda f: f"metadata is stale: {f['stale_reason']}",
                ),
                decide.Need(
                    "unambiguous GA documentation for connector version",
                    lambda f: f["docs_unambiguous_ga"],
                    "documentation is unknown, preview-only, or contradictory",
                ),
                decide.Need(
                    "inline connector support without table resync",
                    lambda f: f["inline_supported"],
                    lambda f: f"connector disposition for {f['operation']} is {f['disposition']} (type_ok={f['type_ok']}, rename_ok={f['rename_ok']})",
                ),
                decide.Need(
                    "old-row backfill satisfied inline when required",
                    lambda f: f["inline_backfill_satisfied"],
                    "historical rows require an explicit backfill task",
                ),
            ),
            because="connector version supports the schema drift inline with zero resync or backfill gap",
        ),
        decide.Rung(
            "inline_with_backfill_admission",
            "inline schema drift admission paired with bounded old-row backfill task",
            needs=(
                decide.Need(
                    "fresh connector and engine version metadata",
                    lambda f: f["metadata_fresh"],
                    lambda f: f"metadata is stale: {f['stale_reason']}",
                ),
                decide.Need(
                    "unambiguous GA documentation for connector version",
                    lambda f: f["docs_unambiguous_ga"],
                    "documentation is unknown, preview-only, or contradictory",
                ),
                decide.Need(
                    "connector support for online schema addition with asynchronous backfill task",
                    lambda f: f["backfill_task_supported"],
                    "operation cannot be completed via online addition + backfill task",
                ),
            ),
            because="new stream events carry the column while historical rows are filled by a bounded backfill task",
        ),
        decide.Rung(
            "probed_sandbox_admission",
            "schema drift admission verified by bounded sandbox probe",
            needs=(
                decide.Need(
                    "fresh connector and engine version metadata",
                    lambda f: f["metadata_fresh"],
                    lambda f: f"metadata is stale: {f['stale_reason']}",
                ),
                decide.Need(
                    "ambiguous or undocumented capability eligible for bounded probe",
                    lambda f: not f["docs_unambiguous_ga"],
                    "capability is already unambiguously documented",
                ),
                decide.Need(
                    "passing bounded sandbox probe for exact connector version",
                    lambda f: f["probe_passed"],
                    "bounded sandbox probe did not pass or was not run",
                ),
            ),
            because="bounded sandbox probe empirically proved row and schema preservation for this connector build",
        ),
        decide.Rung(
            "table_resync_required",
            "refuse inline auto-admission and schedule fenced table resync",
            needs=(
                decide.Need(
                    "fresh connector and engine version metadata",
                    lambda f: f["metadata_fresh"],
                    lambda f: f"metadata is stale: {f['stale_reason']}",
                ),
                decide.Need(
                    "unambiguous GA documentation requiring table resync",
                    lambda f: f["docs_unambiguous_ga"],
                    "documentation is unknown or contradictory",
                ),
                decide.Need(
                    "drift operation handled by full table refresh on this connector",
                    lambda f: f["resync_applicable"],
                    "operation is not recoverable via table resync or resync is disabled",
                ),
            ),
            because="connector cannot apply this drift inline without full refresh; refusing silent auto-admission",
        ),
        decide.Rung(
            "refuse_and_record_debt",
            "refuse schema drift auto-admission and record owned capability debt",
            needs=(),
            because="unknown, contradictory, stale, or unsupported connector capability never infers yes",
        ),
    )

    facts = decide.Facts({
        "metadata_fresh": not stale,
        "stale_reason": stale_reason or "",
        "docs_unambiguous_ga": bool(docs_unambiguous_ga),
        "inline_supported": bool(inline_supported),
        "inline_backfill_satisfied": bool(inline_backfill_satisfied),
        "backfill_task_supported": bool(backfill_task_supported),
        "probe_passed": bool(probe_passed),
        "resync_applicable": bool(resync_applicable),
        "operation": event.operation.value,
        "disposition": cell.disposition.value,
        "type_ok": bool(type_change_compatible),
        "rename_ok": bool(rename_safe_inline),
    })

    costs = {
        "auto_inline_admission": 0.01,
        "inline_with_backfill_admission": 0.25,
        "probed_sandbox_admission": 0.50,
        "table_resync_required": 10.0,
        "refuse_and_record_debt": 100.0,
    }
    choice = decide.climb("connector schema drift capability", rungs, facts, costs=costs)
    chosen_name = choice.rung.name if choice.rung else "refuse_and_record_debt"

    decision_id = f"dec-{uuid.uuid4().hex[:10]}"
    decided_ts = time.time() if now is None else float(now)

    if chosen_name == "auto_inline_admission":
        detail = f" ({widening_detail})" if widening_detail else ""
        decision = DriftAdmissionDecision(
            decision_id=decision_id,
            event_id=event.event_id,
            table=event.table,
            operation=event.operation,
            coordinate=effective_live,
            snapshot_id=snapshot.snapshot_id,
            snapshot_digest=snapshot.digest,
            snapshot_captured_at=snapshot.captured_at,
            max_metadata_age_seconds=snapshot.max_metadata_age_seconds,
            selected_rung=chosen_name,
            auto_admit=True,
            requires_table_resync=False,
            requires_backfill_task=False,
            care_badge=CareBadge.PASS,
            reason=(
                f"connector {effective_live.connector}@{effective_live.connector_version} "
                f"supports {event.operation.value} inline{detail}"
            ),
            decided_at=decided_ts,
        )
    elif chosen_name == "inline_with_backfill_admission":
        bf_task = BackfillTask(
            task_id=f"bf-{event.event_id}",
            table=event.table,
            column_name=event.column_name or "__table_bootstrap__",
            default_value=event.default_value,
        )
        decision = DriftAdmissionDecision(
            decision_id=decision_id,
            event_id=event.event_id,
            table=event.table,
            operation=event.operation,
            coordinate=effective_live,
            snapshot_id=snapshot.snapshot_id,
            snapshot_digest=snapshot.digest,
            snapshot_captured_at=snapshot.captured_at,
            max_metadata_age_seconds=snapshot.max_metadata_age_seconds,
            selected_rung=chosen_name,
            auto_admit=True,
            requires_table_resync=False,
            requires_backfill_task=True,
            care_badge=CareBadge.REVIEW,
            reason=(
                f"connector {effective_live.connector}@{effective_live.connector_version} "
                f"admits {event.operation.value} with bounded backfill task {bf_task.task_id}"
            ),
            backfill_task=bf_task,
            decided_at=decided_ts,
        )
    elif chosen_name == "probed_sandbox_admission":
        debt = OwnedCapabilityDebt(
            debt_id=f"debt-{event.event_id}",
            owner=debt_owner,
            coordinate=effective_live,
            table=event.table,
            operation=event.operation,
            category="probed_undocumented_capability",
            care_badge=CareBadge.REVIEW,
            summary=(
                f"{event.operation.value} admitted after passing bounded probe "
                f"{probe_result.probe_id if probe_result else 'none'}; official docs remain {cell.disposition.value}"
            ),
            remediation="pin connector version and promote probe evidence into verified capability matrix",
            contradiction_reasons=cell.contradiction_reasons,
            probe_id=probe_result.probe_id if probe_result else None,
            created_at=decided_ts,
        )
        decision = DriftAdmissionDecision(
            decision_id=decision_id,
            event_id=event.event_id,
            table=event.table,
            operation=event.operation,
            coordinate=effective_live,
            snapshot_id=snapshot.snapshot_id,
            snapshot_digest=snapshot.digest,
            snapshot_captured_at=snapshot.captured_at,
            max_metadata_age_seconds=snapshot.max_metadata_age_seconds,
            selected_rung=chosen_name,
            auto_admit=True,
            requires_table_resync=False,
            requires_backfill_task=bool(
                probe_result and not probe_result.backfilled_old_rows and needs_old_row_backfill
            ),
            care_badge=CareBadge.REVIEW,
            reason=(
                f"bounded probe {probe_result.probe_id if probe_result else ''} proved "
                f"{event.operation.value} on {effective_live.connector}@{effective_live.connector_version}"
            ),
            probe_result=probe_result,
            debt_record=debt,
            decided_at=decided_ts,
        )
    elif chosen_name == "table_resync_required":
        debt = OwnedCapabilityDebt(
            debt_id=f"debt-{event.event_id}",
            owner=debt_owner,
            coordinate=effective_live,
            table=event.table,
            operation=event.operation,
            category="requires_table_resync",
            care_badge=CareBadge.ACTION,
            summary=(
                f"connector {effective_live.connector}@{effective_live.connector_version} "
                f"cannot auto-admit {event.operation.value} inline; requires full table refresh"
            ),
            remediation=f"schedule shadow resnapshot or fenced table refresh for {event.table}",
            contradiction_reasons=cell.contradiction_reasons,
            created_at=decided_ts,
        )
        decision = DriftAdmissionDecision(
            decision_id=decision_id,
            event_id=event.event_id,
            table=event.table,
            operation=event.operation,
            coordinate=effective_live,
            snapshot_id=snapshot.snapshot_id,
            snapshot_digest=snapshot.digest,
            snapshot_captured_at=snapshot.captured_at,
            max_metadata_age_seconds=snapshot.max_metadata_age_seconds,
            selected_rung=chosen_name,
            auto_admit=False,
            requires_table_resync=True,
            requires_backfill_task=False,
            care_badge=CareBadge.ACTION,
            reason=(
                f"auto-admission refused: {effective_live.connector}@{effective_live.connector_version} "
                f"requires full table refresh for {event.operation.value}"
            ),
            debt_record=debt,
            decided_at=decided_ts,
        )
    else:
        if stale:
            cat = "stale_metadata"
            why_msg = f"stale connector/version metadata: {stale_reason}"
        elif cell.disposition == SupportDisposition.CONTRADICTORY:
            cat = "contradictory_docs"
            why_msg = (
                f"contradictory documentation for {event.operation.value}: "
                f"{'; '.join(cell.contradiction_reasons)}"
            )
        elif cell.disposition == SupportDisposition.UNKNOWN or cell.is_ambiguous_or_unverified:
            cat = "unknown_or_unproved_docs"
            probe_note = (
                f"; probe failed ({probe_result.failure_reason})"
                if probe_result is not None and not probe_result.passed
                else "; bounded probe required"
            )
            why_msg = (
                f"unknown or unverified capability for {event.operation.value} "
                f"(status={cell.release_status.value}){probe_note}"
            )
        else:
            cat = "unsupported_drift"
            why_msg = (
                f"connector {effective_live.connector}@{effective_live.connector_version} "
                f"does not support {event.operation.value} (disposition={cell.disposition.value})"
            )

        debt = OwnedCapabilityDebt(
            debt_id=f"debt-{event.event_id}",
            owner=debt_owner,
            coordinate=effective_live,
            table=event.table,
            operation=event.operation,
            category=cat,
            care_badge=CareBadge.ACTION,
            summary=why_msg,
            remediation=(
                "refresh connector capability snapshot or run bounded sandbox probe before admitting drift"
                if stale or "docs" in cat
                else f"halt inline DDL for {event.table} or upgrade connector"
            ),
            contradiction_reasons=cell.contradiction_reasons,
            probe_id=probe_result.probe_id if probe_result else None,
            created_at=decided_ts,
        )
        decision = DriftAdmissionDecision(
            decision_id=decision_id,
            event_id=event.event_id,
            table=event.table,
            operation=event.operation,
            coordinate=effective_live,
            snapshot_id=snapshot.snapshot_id,
            snapshot_digest=snapshot.digest,
            snapshot_captured_at=snapshot.captured_at,
            max_metadata_age_seconds=snapshot.max_metadata_age_seconds,
            selected_rung="refuse_and_record_debt",
            auto_admit=False,
            requires_table_resync=cell.disposition == SupportDisposition.REQUIRES_TABLE_RESYNC,
            requires_backfill_task=False,
            care_badge=CareBadge.ACTION,
            reason=f"auto-admission refused ({cat}): {why_msg}",
            probe_result=probe_result,
            debt_record=debt,
            decided_at=decided_ts,
        )

    return choice, decision


# -----------------------------------------------------------------------------
# Mutual Exclusion Lease (.run.lock) and O(1) Typed Durable Checkpoint
# -----------------------------------------------------------------------------

@dataclass
class ConnectorCapabilityLease:
    """Active mutual-exclusion lease (`.run.lock`) with heartbeat and expiration."""
    state_dir: Path
    runner_id: str = field(default_factory=lambda: f"runner-{uuid.uuid4().hex[:8]}")
    ttl_seconds: float = 60.0
    _fd: Optional[int] = field(default=None, init=False, repr=False)
    _acquired_at: float = field(default=0.0, init=False)
    _expires_at: float = field(default=0.0, init=False)

    @property
    def lock_path(self) -> Path:
        return Path(self.state_dir) / ".run.lock"

    def acquire(self, now: Optional[float] = None) -> None:
        if self._fd is not None:
            self.release()
        durable.private_dir(self.state_dir)
        ts = time.time() if now is None else float(now)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise CapabilityLeaseError(
                f"connector capability lease {self.lock_path} is held by another active process",
                care_badge=CareBadge.ACTION,
            ) from exc

        self._fd = fd
        self._acquired_at = ts
        self._expires_at = ts + self.ttl_seconds
        self._write_payload(ts)

    def _write_payload(self, ts: float) -> None:
        if self._fd is None:
            return
        payload = {
            "runner_id": self.runner_id,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "acquired_at": self._acquired_at,
            "heartbeat_at": ts,
            "expires_at": self._expires_at,
        }
        raw = json.dumps(payload, sort_keys=True).encode("utf-8")
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.write(self._fd, raw)
        os.fsync(self._fd)

    def heartbeat(self, now: Optional[float] = None) -> None:
        self.check_active(now=now)
        ts = time.time() if now is None else float(now)
        self._expires_at = ts + self.ttl_seconds
        self._write_payload(ts)

    def check_active(self, now: Optional[float] = None) -> None:
        if self._fd is None:
            raise CapabilityLeaseError(
                "connector capability lease is not held",
                care_badge=CareBadge.ACTION,
            )
        ts = time.time() if now is None else float(now)
        if ts > self._expires_at:
            raise CapabilityLeaseExpiredError(
                f"connector capability lease expired at {self._expires_at:.2f} (now={ts:.2f})",
                care_badge=CareBadge.ACTION,
            )

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
            try:
                self.lock_path.unlink()
            except OSError:
                pass

    def __enter__(self) -> "ConnectorCapabilityLease":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


class ConnectorCapabilityCheckpoint:
    """Universal typed durable checkpoint in O(1) for drift decisions, probes, and debt."""

    def __init__(self, state_dir: Path):
        self.state_dir = durable.private_dir(Path(state_dir))
        self.path = self.state_dir / "connector_capability_state.json"
        self.completed_events: Dict[str, Dict[str, Any]] = {}
        self.cursor_watermarks: Dict[str, Any] = {}
        self.debt_items: Dict[str, OwnedCapabilityDebt] = {}
        self._load()

    def _load(self) -> None:
        raw = durable.read_state(self.path, "state")
        if not raw:
            return
        decoded = decode_typed_value(raw)
        self.completed_events = dict(decoded.get("completed_events", {}))
        self.cursor_watermarks = dict(decoded.get("cursor_watermarks", {}))
        self.debt_items = {
            k: OwnedCapabilityDebt.from_dict(v)
            for k, v in decoded.get("debt_items", {}).items()
        }

    def save(self) -> None:
        payload = {
            "completed_events": encode_typed_value(self.completed_events),
            "cursor_watermarks": encode_typed_value(self.cursor_watermarks),
            "debt_items": {k: v.to_dict() for k, v in self.debt_items.items()},
        }
        durable.write_atomic(
            self.path,
            json.dumps(payload, sort_keys=True, indent=2).encode("utf-8"),
        )

    def is_completed(self, event_id: str, snapshot_digest: str) -> bool:
        """O(1) lookup checking if `event_id` was already evaluated under `snapshot_digest`."""
        entry = self.completed_events.get(event_id)
        if not entry:
            return False
        return entry.get("snapshot_digest") == snapshot_digest

    def record_decision(
        self,
        decision: DriftAdmissionDecision,
        *,
        cursor_position: Optional[Any] = None,
    ) -> None:
        self.completed_events[decision.event_id] = {
            "decision_id": decision.decision_id,
            "table": decision.table,
            "operation": decision.operation.value,
            "selected_rung": decision.selected_rung,
            "auto_admit": decision.auto_admit,
            "requires_table_resync": decision.requires_table_resync,
            "requires_backfill_task": decision.requires_backfill_task,
            "care_badge": decision.care_badge.value,
            "snapshot_digest": decision.snapshot_digest,
            "coordinate": decision.coordinate.to_dict(),
            "cursor_position": cursor_position,
            "decided_at": decision.decided_at,
        }
        if cursor_position is not None:
            self.cursor_watermarks[decision.table] = cursor_position
        if decision.debt_record is not None:
            self.debt_items[decision.debt_record.debt_id] = decision.debt_record
        self.save()


# -----------------------------------------------------------------------------
# Reference Connector Capability Catalog & Coordinator
# -----------------------------------------------------------------------------

def default_reference_claims() -> List[DocSourceClaim]:
    """Reference capability claims modeling real-world connector version matrices.

    Covers:
    1. Managed Lakehouse Ingest (`lakeflow-connect` on `mssql` / `postgres`):
       - Supports `ADD_COLUMN` inline (`AUTO_INLINE`),
       - Requires explicit backfill task for historical defaults on older versions,
       - Requires full table refresh (`REQUIRES_TABLE_RESYNC`) for `RENAME_COLUMN`
         and narrowing type changes (`widening_only=True`),
       - Supports `NEW_TABLE_DISCOVERY` in v2.4+ (`AUTO_INLINE`).
    2. Incremental Policy Sync (`policy-sync-connector` on `postgres` / `mysql`):
       - Supports `ADD_COLUMN` (`REQUIRES_BACKFILL_TASK`),
       - Treats `RENAME_COLUMN` as drop+add requiring full refresh (`REQUIRES_TABLE_RESYNC`),
       - Supports `DELETE_COLUMN` via soft-nullify (`AUTO_INLINE`).
    """
    return [
        # lakeflow-connect v2.4.0 on mssql/rds CDC_CHANGE_TRACKING
        DocSourceClaim(
            source_id="S100-lakeflow-mssql-ga",
            operation=DriftOperation.ADD_COLUMN,
            claimed_disposition=SupportDisposition.AUTO_INLINE,
            release_status=DocReleaseStatus.GA,
            engine_version_spec=">=2019",
            connector_version_spec=">=2.4.0,<3.0.0",
            backfill_historical_rows=True,
            url_or_ref="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-overview",
            notes="nullable and default columns added online in v2.4+",
        ),
        DocSourceClaim(
            source_id="S100-lakeflow-mssql-ga",
            operation=DriftOperation.DELETE_COLUMN,
            claimed_disposition=SupportDisposition.AUTO_INLINE,
            release_status=DocReleaseStatus.GA,
            engine_version_spec=">=2019",
            connector_version_spec=">=2.4.0,<3.0.0",
            notes="deleted columns retained as NULL in target history",
        ),
        DocSourceClaim(
            source_id="S101-lakeflow-faq-rename",
            operation=DriftOperation.RENAME_COLUMN,
            claimed_disposition=SupportDisposition.REQUIRES_TABLE_RESYNC,
            release_status=DocReleaseStatus.GA,
            engine_version_spec=">=2019",
            connector_version_spec=">=2.0.0,<3.0.0",
            preserves_column_identity_on_rename=False,
            url_or_ref="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/faq",
            notes="column rename requires a full table refresh to avoid drop+add data loss",
        ),
        DocSourceClaim(
            source_id="S101-lakeflow-faq-type",
            operation=DriftOperation.CHANGE_COLUMN_TYPE,
            claimed_disposition=SupportDisposition.AUTO_INLINE,
            release_status=DocReleaseStatus.GA,
            engine_version_spec=">=2019",
            connector_version_spec=">=2.4.0,<3.0.0",
            widening_only=True,
            notes="widening type changes apply inline; incompatible/narrowing changes require table refresh",
        ),
        DocSourceClaim(
            source_id="S100-lakeflow-mssql-backfill",
            operation=DriftOperation.OLD_ROW_BACKFILL,
            claimed_disposition=SupportDisposition.AUTO_INLINE,
            release_status=DocReleaseStatus.GA,
            engine_version_spec=">=2019",
            connector_version_spec=">=2.4.0,<3.0.0",
            backfill_historical_rows=True,
        ),
        DocSourceClaim(
            source_id="S100-lakeflow-mssql-discovery",
            operation=DriftOperation.NEW_TABLE_DISCOVERY,
            claimed_disposition=SupportDisposition.AUTO_INLINE,
            release_status=DocReleaseStatus.GA,
            engine_version_spec=">=2019",
            connector_version_spec=">=2.4.0,<3.0.0",
            discovers_new_tables_automatically=True,
        ),
    ]


class ConnectorCapabilityCoordinator:
    """Evaluates single or batched schema drift events against versioned connector snapshots."""

    def __init__(
        self,
        state_dir: Path,
        snapshot: ConnectorCapabilitySnapshot,
        *,
        debt_owner: str = "migration-team",
        probe_budget: Optional[ProbeBudget] = None,
        probe_runner: Optional[
            Callable[[ConnectorMatrixCoordinate, DriftOperation, ProbeBudget], Dict[str, Any]]
        ] = None,
    ):
        self.state_dir = Path(state_dir)
        self.snapshot = snapshot
        self.debt_owner = debt_owner
        self.probe_budget = probe_budget or ProbeBudget()
        self.probe_runner = probe_runner
        self.checkpoint = ConnectorCapabilityCheckpoint(self.state_dir)
        self.decisions: Dict[str, DriftAdmissionDecision] = {}

    def evaluate_event(
        self,
        event: SchemaDriftEvent,
        *,
        live_coordinate: Optional[ConnectorMatrixCoordinate] = None,
        now: Optional[float] = None,
        run_probe_if_needed: bool = True,
    ) -> DriftAdmissionDecision:
        """Evaluate a schema drift event under an active `.run.lock` lease with O(1) resume."""
        with ConnectorCapabilityLease(self.state_dir) as lease:
            lease.check_active(now=now)
            effective_live = (live_coordinate or self.snapshot.coordinate).normalized()
            stale, _ = self.snapshot.is_stale(now=now, live_coordinate=effective_live)

            if (
                not stale
                and self.checkpoint.is_completed(event.event_id, self.snapshot.digest)
                and event.event_id in self.decisions
            ):
                cached = self.decisions[event.event_id]
                cached.ensure_fresh(
                    live_coordinate=effective_live,
                    current_snapshot=self.snapshot,
                    now=now,
                )
                return cached

            cell = self.snapshot.cell_for(event.operation)
            probe_res: Optional[ProbeResult] = None
            if (
                not stale
                and cell.is_ambiguous_or_unverified
                and run_probe_if_needed
                and self.probe_runner is not None
            ):
                probe_res = execute_bounded_probe(
                    effective_live,
                    event.operation,
                    self.probe_budget,
                    self.probe_runner,
                )

            _, decision = decide_drift_admission(
                self.snapshot,
                event,
                live_coordinate=effective_live,
                now=now,
                probe_result=probe_res,
                debt_owner=self.debt_owner,
            )
            self.decisions[event.event_id] = decision
            if not stale:
                self.checkpoint.record_decision(
                    decision,
                    cursor_position=event.cursor_position,
                )
            elif decision.debt_record is not None:
                self.checkpoint.debt_items[decision.debt_record.debt_id] = decision.debt_record
                self.checkpoint.save()
            return decision

    def evaluate_batch(
        self,
        events: Sequence[SchemaDriftEvent],
        *,
        live_coordinate: Optional[ConnectorMatrixCoordinate] = None,
        now: Optional[float] = None,
    ) -> Tuple[bool, List[DriftAdmissionDecision]]:
        """Evaluate a batch of drift events; batch auto-admission holds only if every event auto-admits."""
        results: List[DriftAdmissionDecision] = []
        for ev in events:
            dec = self.evaluate_event(ev, live_coordinate=live_coordinate, now=now)
            results.append(dec)
        batch_auto_admit = all(d.auto_admit for d in results)
        return batch_auto_admit, results
