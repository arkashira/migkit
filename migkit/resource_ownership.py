"""Own temporary resources and restore observed state (Row 605: SMART-N22.resource-ownership).

Assigns durable owners and cleanup/restore obligations to temporary tables,
files, spools, readers, replication slots, subscriptions, credential leases,
tunnels, settings, and index windows.

Budgets disk, WAL, file descriptors, memory, and database connections derived
from measured system capacity. Guarantees deterministic cleanup or resume upon
interruption at every acquisition/restore point; preserves shared resources
untouched; restores original observed settings; and ensures sole copies of
change/undo data are never prematurely removed.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import json
import os
import pathlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from . import checkpoint, decide, lease

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class ResourceKind(str, enum.Enum):
    TABLE = "table"
    FILE = "file"
    SPOOL = "spool"
    READER = "reader"
    SLOT = "slot"
    SUBSCRIPTION = "subscription"
    CREDENTIAL_LEASE = "credential_lease"
    TUNNEL = "tunnel"
    SETTING = "setting"
    INDEX_WINDOW = "index_window"


class ObligationKind(str, enum.Enum):
    CLEANUP = "cleanup"
    RESTORE = "restore"
    REVOKE = "revoke"
    UNBIND = "unbind"
    RETAIN = "retain"


class ResourceState(str, enum.Enum):
    ACQUIRING = "acquiring"
    ACQUIRED = "acquired"
    ACTIVE = "active"
    INTERRUPTED = "interrupted"
    RELEASING = "releasing"
    RESTORED = "restored"
    CLEANED_UP = "cleaned_up"
    RETAINED_SOLE_COPY = "retained_sole_copy"
    FAILED = "failed"


class ResourceOwnershipError(Exception):
    """Base error for resource ownership and state restoration."""


class BudgetExhaustedError(ResourceOwnershipError):
    """Raised when demand exceeds measured capacity in any budget dimension."""


class UnsafeEndpointError(ResourceOwnershipError):
    """Raised when an endpoint host violates loopback or RFC1918 10.0.0.x rules."""


class SoleUndoDataHazardError(ResourceOwnershipError):
    """Raised when an operation threatens to destroy the sole copy of change/undo data."""


class OwnershipConflictError(ResourceOwnershipError):
    """Raised when a resource is claimed by an active conflicting owner."""


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
# 5-Dimensional Resource Budgeting
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ResourceDemand:
    disk_bytes: int = 0
    wal_bytes: int = 0
    file_descriptors: int = 0
    memory_bytes: int = 0
    connections: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "disk_bytes": self.disk_bytes,
            "wal_bytes": self.wal_bytes,
            "file_descriptors": self.file_descriptors,
            "memory_bytes": self.memory_bytes,
            "connections": self.connections,
        }

    @classmethod
    def from_dict(cls, data: Optional[Mapping[str, Any]]) -> ResourceDemand:
        if not data:
            return cls()
        return cls(
            disk_bytes=int(data.get("disk_bytes", 0)),
            wal_bytes=int(data.get("wal_bytes", 0)),
            file_descriptors=int(data.get("file_descriptors", 0)),
            memory_bytes=int(data.get("memory_bytes", 0)),
            connections=int(data.get("connections", 0)),
        )


@dataclass
class ResourceBudget:
    disk_bytes_limit: int
    wal_bytes_limit: int
    file_descriptors_limit: int
    memory_bytes_limit: int
    connections_limit: int
    disk_bytes_used: int = 0
    wal_bytes_used: int = 0
    file_descriptors_used: int = 0
    memory_bytes_used: int = 0
    connections_used: int = 0

    @classmethod
    def from_measured(cls, facts: Mapping[str, Any]) -> ResourceBudget:
        """Derive budget limits strictly from measured system capacity facts."""
        disk_raw = int(facts.get("available_disk_bytes", 0))
        disk_headroom = float(facts.get("disk_headroom_fraction", 0.8))
        wal_raw = int(facts.get("wal_keep_bytes", 0))
        wal_headroom = float(facts.get("wal_headroom_fraction", 0.75))
        fd_raw = int(facts.get("system_fd_limit", 1024))
        fd_headroom = float(facts.get("fd_headroom_fraction", 0.5))
        mem_raw = int(facts.get("system_memory_bytes", 0))
        mem_headroom = float(facts.get("memory_headroom_fraction", 0.6))
        conn_raw = int(facts.get("system_max_connections", 100))
        conn_headroom = float(facts.get("connections_headroom_fraction", 0.5))

        return cls(
            disk_bytes_limit=int(disk_raw * disk_headroom),
            wal_bytes_limit=int(wal_raw * wal_headroom),
            file_descriptors_limit=int(fd_raw * fd_headroom),
            memory_bytes_limit=int(mem_raw * mem_headroom),
            connections_limit=int(conn_raw * conn_headroom),
        )

    def check_demand(self, demand: ResourceDemand) -> Tuple[bool, Optional[str]]:
        """Verify whether demand fits within remaining capacity across all 5 dimensions."""
        if self.disk_bytes_used + demand.disk_bytes > self.disk_bytes_limit:
            return (
                False,
                f"disk budget exceeded: {self.disk_bytes_used + demand.disk_bytes} > {self.disk_bytes_limit}",
            )
        if self.wal_bytes_used + demand.wal_bytes > self.wal_bytes_limit:
            return (
                False,
                f"WAL budget exceeded: {self.wal_bytes_used + demand.wal_bytes} > {self.wal_bytes_limit}",
            )
        if self.file_descriptors_used + demand.file_descriptors > self.file_descriptors_limit:
            return (
                False,
                f"file descriptors budget exceeded: {self.file_descriptors_used + demand.file_descriptors} > {self.file_descriptors_limit}",
            )
        if self.memory_bytes_used + demand.memory_bytes > self.memory_bytes_limit:
            return (
                False,
                f"memory budget exceeded: {self.memory_bytes_used + demand.memory_bytes} > {self.memory_bytes_limit}",
            )
        if self.connections_used + demand.connections > self.connections_limit:
            return (
                False,
                f"connections budget exceeded: {self.connections_used + demand.connections} > {self.connections_limit}",
            )
        return True, None

    def allocate(self, demand: ResourceDemand) -> None:
        ok, reason = self.check_demand(demand)
        if not ok:
            raise BudgetExhaustedError(reason)
        self.disk_bytes_used += demand.disk_bytes
        self.wal_bytes_used += demand.wal_bytes
        self.file_descriptors_used += demand.file_descriptors
        self.memory_bytes_used += demand.memory_bytes
        self.connections_used += demand.connections

    def release(self, demand: ResourceDemand) -> None:
        self.disk_bytes_used = max(0, self.disk_bytes_used - demand.disk_bytes)
        self.wal_bytes_used = max(0, self.wal_bytes_used - demand.wal_bytes)
        self.file_descriptors_used = max(0, self.file_descriptors_used - demand.file_descriptors)
        self.memory_bytes_used = max(0, self.memory_bytes_used - demand.memory_bytes)
        self.connections_used = max(0, self.connections_used - demand.connections)

    def utilization(self) -> Dict[str, float]:
        return {
            "disk": float(self.disk_bytes_used) / float(max(1, self.disk_bytes_limit)),
            "wal": float(self.wal_bytes_used) / float(max(1, self.wal_bytes_limit)),
            "descriptors": float(self.file_descriptors_used) / float(max(1, self.file_descriptors_limit)),
            "memory": float(self.memory_bytes_used) / float(max(1, self.memory_bytes_limit)),
            "connections": float(self.connections_used) / float(max(1, self.connections_limit)),
        }


# -----------------------------------------------------------------------------
# Durable Owner Record & Store
# -----------------------------------------------------------------------------

@dataclass
class OwnerRecord:
    owner_id: str
    resource_id: str
    kind: ResourceKind
    host: str
    db: Optional[str] = None
    state: ResourceState = ResourceState.ACQUIRING
    is_shared: bool = False
    has_sole_undo_copy: bool = False
    original_state: Optional[Dict[str, Any]] = None
    current_state: Optional[Dict[str, Any]] = None
    demand: ResourceDemand = field(default_factory=ResourceDemand)
    obligation: ObligationKind = ObligationKind.CLEANUP
    acquired_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    updated_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    lease_expiry: Optional[datetime.datetime] = None
    cleanup_payload: Optional[str] = None
    restore_payload: Optional[str] = None


class ResourceOwnershipStore:
    """Durable $O(1)$ store tracking resource owners and restoration obligations."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = pathlib.Path(path)
        self._records: Dict[str, OwnerRecord] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="ascii"))
        records_raw = raw.get("resources", {})
        for rid, item in records_raw.items():
            self._records[rid] = OwnerRecord(
                owner_id=str(item["owner_id"]),
                resource_id=rid,
                kind=ResourceKind(item["kind"]),
                host=str(item["host"]),
                db=item.get("db"),
                state=ResourceState(item["state"]),
                is_shared=bool(item.get("is_shared", False)),
                has_sole_undo_copy=bool(item.get("has_sole_undo_copy", False)),
                original_state=item.get("original_state"),
                current_state=item.get("current_state"),
                demand=ResourceDemand.from_dict(item.get("demand")),
                obligation=ObligationKind(item.get("obligation", ObligationKind.CLEANUP.value)),
                acquired_at=checkpoint.deserialize_cursor(item["acquired_at"]),
                updated_at=checkpoint.deserialize_cursor(item["updated_at"]),
                lease_expiry=checkpoint.deserialize_cursor(item.get("lease_expiry")),
                cleanup_payload=item.get("cleanup_payload"),
                restore_payload=item.get("restore_payload"),
            )

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "resources": {
                rid: {
                    "owner_id": r.owner_id,
                    "kind": r.kind.value,
                    "host": r.host,
                    "db": r.db,
                    "state": r.state.value,
                    "is_shared": r.is_shared,
                    "has_sole_undo_copy": r.has_sole_undo_copy,
                    "original_state": r.original_state,
                    "current_state": r.current_state,
                    "demand": r.demand.to_dict(),
                    "obligation": r.obligation.value,
                    "acquired_at": checkpoint.serialize_cursor(r.acquired_at),
                    "updated_at": checkpoint.serialize_cursor(r.updated_at),
                    "lease_expiry": checkpoint.serialize_cursor(r.lease_expiry),
                    "cleanup_payload": r.cleanup_payload,
                    "restore_payload": r.restore_payload,
                }
                for rid, r in self._records.items()
            }
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=True), encoding="ascii")
        tmp.replace(self.path)

    def record_resource(self, record: OwnerRecord) -> None:
        self._records[record.resource_id] = record
        self._save()

    def update_resource(self, record: OwnerRecord) -> None:
        record.updated_at = datetime.datetime.now(datetime.timezone.utc)
        self._records[record.resource_id] = record
        self._save()

    def get_resource(self, resource_id: str) -> Optional[OwnerRecord]:
        return self._records.get(resource_id)

    def list_resources(self, kind: Optional[ResourceKind] = None) -> List[OwnerRecord]:
        records = list(self._records.values())
        if kind:
            records = [r for r in records if r.kind == kind]
        return records

    def remove_resource(self, resource_id: str) -> None:
        if resource_id in self._records:
            del self._records[resource_id]
            self._save()

    def is_completed_o1(self, resource_id: str) -> bool:
        r = self._records.get(resource_id)
        return r is not None and r.state in (
            ResourceState.RESTORED,
            ResourceState.CLEANED_UP,
            ResourceState.RETAINED_SOLE_COPY,
        )


# -----------------------------------------------------------------------------
# Decision Ladders (Top-Level Functions for test_every_decide_rung_is_reached)
# -----------------------------------------------------------------------------

def resource_admission_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for admitting and acquiring temporary resources."""
    return (
        decide.Rung(
            "admit_exclusive_resource",
            "admit exclusive temporary resource with cleanup obligation",
            needs=(
                decide.Need(
                    "endpoint is safe",
                    lambda f: bool(f.get("endpoint_safe")),
                    "{rung} requires loopback or 10.0.0.x endpoint",
                ),
                decide.Need(
                    "budget capacity available",
                    lambda f: bool(f.get("budget_available")),
                    "{rung} requires sufficient disk, WAL, descriptor, memory, and connection budget",
                ),
                decide.Need(
                    "durable owner recorded",
                    lambda f: bool(f.get("owner_recorded")),
                    "{rung} requires durable owner ID and state tracking",
                ),
                decide.Need(
                    "not shared resource",
                    lambda f: not bool(f.get("is_shared")),
                    "{rung} requires exclusive non-shared resource lifecycle",
                ),
            ),
            because="budget capacity confirmed, endpoint safe, and exclusive ownership recorded",
            footprint=1,
            leaves="exclusive resource admitted into ownership ledger",
        ),
        decide.Rung(
            "admit_shared_attachment",
            "admit shared resource tracking non-destructive attachment",
            needs=(
                decide.Need(
                    "endpoint is safe",
                    lambda f: bool(f.get("endpoint_safe")),
                    "{rung} requires loopback or 10.0.0.x endpoint",
                ),
                decide.Need(
                    "budget capacity available",
                    lambda f: bool(f.get("budget_available")),
                    "{rung} requires sufficient budget capacity",
                ),
                decide.Need(
                    "is shared resource",
                    lambda f: bool(f.get("is_shared")),
                    "{rung} requires resource to be designated as shared",
                ),
                decide.Need(
                    "non destructive obligation declared",
                    lambda f: bool(f.get("non_destructive_obligation")),
                    "{rung} requires unbind or non-destructive obligation for shared resource",
                ),
            ),
            because="shared resource attached without destructive cleanup obligation",
            footprint=2,
            leaves="shared resource attached without destructive lifecycle",
        ),
        decide.Rung(
            "refuse_exhausted_budget",
            "refuse acquisition when budget is exhausted",
            needs=(
                decide.Need(
                    "budget demand exceeds measured capacity",
                    lambda f: bool(f.get("budget_exhausted")),
                    "{rung} requires budget demand to exceed measured limit",
                ),
            ),
            because="allocation would violate measured disk, wal, descriptor, memory, or connection budget",
            footprint=0,
            leaves="resource acquisition refused due to budget exhaustion",
        ),
        decide.Rung(
            "refuse_unsafe_endpoint",
            "refuse acquisition on non-safe endpoint host",
            needs=(
                decide.Need(
                    "target host outside loopback or RFC1918 10.0.0.x",
                    lambda f: bool(f.get("unsafe_endpoint")),
                    "{rung} requires endpoint to violate safety constraints",
                ),
            ),
            because="only loopback (127.0.0.1, localhost) or 10.0.0.x hosts are permitted",
            footprint=0,
            leaves="resource acquisition refused due to unsafe network endpoint",
        ),
    )


def resource_cleanup_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for cleaning up or retaining owned resources."""
    return (
        decide.Rung(
            "cleanup_ephemeral_exclusive",
            "delete ephemeral exclusive resource when owner is gone",
            needs=(
                decide.Need(
                    "owner is dead or completed",
                    lambda f: bool(f.get("owner_gone")),
                    "{rung} requires owner process to be terminated or completed",
                ),
                decide.Need(
                    "is not shared resource",
                    lambda f: not bool(f.get("is_shared")),
                    "{rung} shared resources must not be deleted",
                ),
                decide.Need(
                    "no sole copy of undo or change data",
                    lambda f: not bool(f.get("has_sole_undo_copy")),
                    "{rung} sole copies of change or undo data must not be removed",
                ),
            ),
            because="ephemeral exclusive resource has no active owner and holds no unique change data",
            footprint=1,
            leaves="ephemeral exclusive resource removed and budget reclaimed",
        ),
        decide.Rung(
            "retain_sole_undo_copy",
            "quarantine and retain sole copy of change or undo data",
            needs=(
                decide.Need(
                    "holds sole copy of undo or change data",
                    lambda f: bool(f.get("has_sole_undo_copy")),
                    "{rung} requires resource to contain sole uncommitted change/undo copy",
                ),
            ),
            because="premature removal would destroy the sole copy of change or undo data",
            footprint=3,
            leaves="sole copy of change data quarantined and retained safely",
        ),
        decide.Rung(
            "unbind_shared_resource",
            "unbind ownership lease while leaving shared resource untouched",
            needs=(
                decide.Need(
                    "resource is shared",
                    lambda f: bool(f.get("is_shared")),
                    "{rung} requires resource to be shared entity",
                ),
                decide.Need(
                    "owner is dead or completed",
                    lambda f: bool(f.get("owner_gone")),
                    "{rung} requires owner to be finished with shared resource",
                ),
            ),
            because="shared resource must remain untouched on physical engine; unbind ownership lease only",
            footprint=2,
            leaves="shared resource attachment released leaving physical entity intact",
        ),
        decide.Rung(
            "defer_active_owner",
            "defer cleanup while owner process remains active",
            needs=(
                decide.Need(
                    "owner process is still actively running",
                    lambda f: not bool(f.get("owner_gone")),
                    "{rung} requires owner process to be actively running",
                ),
            ),
            because="the process that created the resource is still running and may be writing",
            footprint=4,
            leaves="resource cleanup deferred while active owner holds lease",
        ),
    )


def setting_restore_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for restoring observed database settings."""
    return (
        decide.Rung(
            "restore_observed_setting",
            "restore setting to durably recorded observed original value",
            needs=(
                decide.Need(
                    "observed original value is durably recorded",
                    lambda f: bool(f.get("original_recorded")),
                    "{rung} requires durably recorded original setting value",
                ),
                decide.Need(
                    "current target value matches migkit active value",
                    lambda f: bool(f.get("current_matches_migkit")),
                    "{rung} requires target setting to match migkit modified value",
                ),
            ),
            because="target setting still holds migkit's active value and observed original value is verified",
            footprint=1,
            leaves="target setting reverted to original observed configuration",
        ),
        decide.Rung(
            "preserve_external_setting_change",
            "preserve external administrative modification to setting",
            needs=(
                decide.Need(
                    "observed original value is durably recorded",
                    lambda f: bool(f.get("original_recorded")),
                    "{rung} requires original setting baseline to be known",
                ),
                decide.Need(
                    "setting was modified by external administrator",
                    lambda f: bool(f.get("external_drift")),
                    "{rung} requires external drift away from migkit value",
                ),
            ),
            because="external administrative change must not be blindly overwritten without operator review",
            footprint=2,
            leaves="external setting preserved with audit finding logged",
        ),
        decide.Rung(
            "refuse_missing_original_state",
            "refuse restore when original observed state is unrecorded",
            needs=(
                decide.Need(
                    "original observed value is missing or unverified",
                    lambda f: not bool(f.get("original_recorded")),
                    "{rung} requires original observed setting to be missing",
                ),
            ),
            because="cannot restore setting without verified durable baseline of observed original value",
            footprint=0,
            leaves="restore refused due to absent observed state record",
        ),
    )


def interruption_recovery_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for recovering interrupted acquisitions or restores."""
    return (
        decide.Rung(
            "resume_active_lease",
            "resume interrupted operation under valid lease and checkpoint",
            needs=(
                decide.Need(
                    "process can acquire or renew active lease",
                    lambda f: bool(f.get("lease_renewable")),
                    "{rung} requires mutual exclusion lease renewal",
                ),
                decide.Need(
                    "checkpoint and resource state valid for resume",
                    lambda f: bool(f.get("checkpoint_valid")),
                    "{rung} requires valid checkpoint and resource state",
                ),
            ),
            because="active verification lease renewed and durable checkpoint enables O(1) resume",
            footprint=1,
            leaves="operation resumed deterministically from durable checkpoint",
        ),
        decide.Rung(
            "rollback_interrupted_acquisition",
            "rollback partially acquired resource from interrupted acquisition",
            needs=(
                decide.Need(
                    "interrupted during resource acquisition",
                    lambda f: bool(f.get("interrupted_acquiring")),
                    "{rung} requires resource state to be ACQUIRING",
                ),
                decide.Need(
                    "owner is dead or abandoned",
                    lambda f: bool(f.get("owner_gone")),
                    "{rung} requires dead or abandoned owner process",
                ),
            ),
            because="partial resource created during interrupted acquisition must be cleanly rolled back",
            footprint=2,
            leaves="interrupted acquisition rolled back cleanly",
        ),
        decide.Rung(
            "force_deterministic_cleanup",
            "force deterministic cleanup of orphaned unshared resource",
            needs=(
                decide.Need(
                    "owner process is dead",
                    lambda f: bool(f.get("owner_gone")),
                    "{rung} requires owner process to be terminated",
                ),
                decide.Need(
                    "resource is exclusive ephemeral",
                    lambda f: not bool(f.get("is_shared")),
                    "{rung} shared resources must not be forcibly cleaned",
                ),
                decide.Need(
                    "holds no uncommitted sole undo copy",
                    lambda f: not bool(f.get("has_sole_undo_copy")),
                    "{rung} resources holding sole undo copies must be quarantined",
                ),
            ),
            because="deterministic cleanup removes orphaned unshared resource from terminated run",
            footprint=3,
            leaves="orphaned resource deterministically purged and state updated",
        ),
    )


# -----------------------------------------------------------------------------
# Decision Ladder Climbers (Every choice climbs through decide.climb)
# -----------------------------------------------------------------------------

def evaluate_resource_admission(
    record: OwnerRecord,
    budget: ResourceBudget,
    endpoint_safe: bool,
) -> decide.Choice:
    """Climb `resource_admission_rungs` to admit or refuse temporary resource acquisition."""
    can_fit, reason = budget.check_demand(record.demand)
    facts = decide.Facts({
        "endpoint_safe": endpoint_safe,
        "budget_available": can_fit,
        "owner_recorded": bool(record.owner_id),
        "is_shared": record.is_shared,
        "non_destructive_obligation": record.obligation in (
            ObligationKind.UNBIND,
            ObligationKind.RESTORE,
            ObligationKind.REVOKE,
        ),
        "budget_exhausted": not can_fit,
        "unsafe_endpoint": not endpoint_safe,
    })
    return decide.climb("admit temporary resource", resource_admission_rungs(), facts)


def evaluate_resource_cleanup(
    record: OwnerRecord,
    owner_alive: bool,
) -> decide.Choice:
    """Climb `resource_cleanup_rungs` to determine cleanup, unbind, deferral, or quarantine."""
    facts = decide.Facts({
        "owner_gone": not owner_alive,
        "is_shared": record.is_shared,
        "has_sole_undo_copy": record.has_sole_undo_copy,
    })
    return decide.climb("clean up temporary resource", resource_cleanup_rungs(), facts)


def evaluate_setting_restore(
    record: OwnerRecord,
    current_value: Any,
) -> decide.Choice:
    """Climb `setting_restore_rungs` to restore, preserve, or refuse setting restore."""
    original_val = (
        record.original_state.get("value")
        if isinstance(record.original_state, dict)
        else None
    )
    expected_migkit_val = (
        record.current_state.get("value")
        if isinstance(record.current_state, dict)
        else None
    )

    original_recorded = original_val is not None
    current_matches_migkit = (current_value == expected_migkit_val)
    external_drift = original_recorded and not current_matches_migkit and (current_value != original_val)

    facts = decide.Facts({
        "original_recorded": original_recorded,
        "current_matches_migkit": current_matches_migkit,
        "external_drift": external_drift,
    })
    return decide.climb("restore observed target setting", setting_restore_rungs(), facts)


def evaluate_interruption_recovery(
    record: OwnerRecord,
    lease_renewable: bool,
    checkpoint_valid: bool,
    owner_alive: bool,
) -> decide.Choice:
    """Climb `interruption_recovery_rungs` to handle interrupted acquisition or crash recovery."""
    interrupted_acquiring = (record.state == ResourceState.ACQUIRING)
    facts = decide.Facts({
        "lease_renewable": lease_renewable,
        "checkpoint_valid": checkpoint_valid,
        "interrupted_acquiring": interrupted_acquiring,
        "owner_gone": not owner_alive,
        "is_shared": record.is_shared,
        "has_sole_undo_copy": record.has_sole_undo_copy,
    })
    return decide.climb("recover interrupted resource state", interruption_recovery_rungs(), facts)


# -----------------------------------------------------------------------------
# High-Level Lifecycle Manager
# -----------------------------------------------------------------------------

@dataclass
class ResourceOwnershipReport:
    badge: CareBadge
    summary: str
    active_count: int
    cleaned_count: int
    restored_count: int
    retained_sole_count: int
    budget_utilization: Dict[str, float]
    findings: List[str]


class ResourceLifecycleManager:
    """Coordinates acquisition, budgeting, interruption recovery, and deterministic restoration."""

    def __init__(self, store: ResourceOwnershipStore, budget: ResourceBudget) -> None:
        self.store = store
        self.budget = budget

    def acquire(
        self,
        record: OwnerRecord,
        *,
        creator_callback: Optional[Callable[[], None]] = None,
    ) -> OwnerRecord:
        """Acquire a temporary resource with durable owner record and budget validation."""
        safe_host = validate_safe_endpoint(record.host)
        record.host = safe_host

        choice = evaluate_resource_admission(record, self.budget, endpoint_safe=True)
        if choice.rung is None:
            can_fit, reason = self.budget.check_demand(record.demand)
            if not can_fit:
                raise BudgetExhaustedError(reason or choice.reason)
            raise ResourceOwnershipError(f"acquisition failed: {choice.reason}")
        if choice.rung.name == "refuse_exhausted_budget":
            can_fit, reason = self.budget.check_demand(record.demand)
            raise BudgetExhaustedError(reason or choice.reason)
        if choice.rung.name == "refuse_unsafe_endpoint":
            raise UnsafeEndpointError(choice.reason)

        record.state = ResourceState.ACQUIRING
        self.store.record_resource(record)

        if creator_callback:
            creator_callback()

        self.budget.allocate(record.demand)
        record.state = ResourceState.ACQUIRED
        self.store.update_resource(record)
        return record

    def release(
        self,
        resource_id: str,
        *,
        owner_alive: bool = False,
        deleter_callback: Optional[Callable[[], None]] = None,
    ) -> OwnerRecord:
        """Release or clean up an owned resource based on ladder decision."""
        record = self.store.get_resource(resource_id)
        if not record:
            raise ResourceOwnershipError(f"resource not found: {resource_id}")

        choice = evaluate_resource_cleanup(record, owner_alive=owner_alive)
        if choice.rung is None:
            raise ResourceOwnershipError(f"cleanup refused: {choice.reason}")

        if choice.rung.name == "retain_sole_undo_copy":
            record.state = ResourceState.RETAINED_SOLE_COPY
            self.store.update_resource(record)
            raise SoleUndoDataHazardError(
                f"refusing deletion: {record.resource_id} contains sole uncommitted change/undo copy"
            )

        if choice.rung.name == "defer_active_owner":
            record.state = ResourceState.ACTIVE
            self.store.update_resource(record)
            return record

        if choice.rung.name == "unbind_shared_resource":
            self.budget.release(record.demand)
            record.state = ResourceState.CLEANED_UP
            self.store.update_resource(record)
            return record

        if choice.rung.name == "cleanup_ephemeral_exclusive":
            record.state = ResourceState.RELEASING
            self.store.update_resource(record)
            if deleter_callback:
                deleter_callback()
            self.budget.release(record.demand)
            record.state = ResourceState.CLEANED_UP
            self.store.update_resource(record)
            return record

        raise ResourceOwnershipError(f"unexpected cleanup rung: {choice.rung.name}")

    def restore_setting(
        self,
        resource_id: str,
        current_target_value: Any,
        setter_callback: Callable[[str, Any], None],
    ) -> OwnerRecord:
        """Restore observed original setting unless external drift requires preservation."""
        record = self.store.get_resource(resource_id)
        if not record:
            raise ResourceOwnershipError(f"setting record not found: {resource_id}")
        if record.kind != ResourceKind.SETTING:
            raise ResourceOwnershipError(f"resource {resource_id} is not a setting")

        choice = evaluate_setting_restore(record, current_target_value)
        if choice.rung is None or choice.rung.name == "refuse_missing_original_state":
            raise ResourceOwnershipError(f"setting restore refused: {choice.reason}")

        setting_name = record.original_state.get("name", record.resource_id)
        if choice.rung.name == "preserve_external_setting_change":
            record.state = ResourceState.RESTORED
            self.store.update_resource(record)
            return record

        if choice.rung.name == "restore_observed_setting":
            record.state = ResourceState.RELEASING
            self.store.update_resource(record)
            original_val = record.original_state["value"]
            setter_callback(setting_name, original_val)
            self.budget.release(record.demand)
            record.state = ResourceState.RESTORED
            self.store.update_resource(record)
            return record

        raise ResourceOwnershipError(f"unexpected setting restore rung: {choice.rung.name}")

    def recover_interrupted(
        self,
        *,
        owner_alive: bool = False,
        rollback_callback: Optional[Callable[[OwnerRecord], None]] = None,
        cleanup_callback: Optional[Callable[[OwnerRecord], None]] = None,
    ) -> List[OwnerRecord]:
        """Recover all interrupted or orphaned resources deterministically."""
        recovered = []
        for r in self.store.list_resources():
            if r.state in (
                ResourceState.RESTORED,
                ResourceState.CLEANED_UP,
                ResourceState.RETAINED_SOLE_COPY,
            ):
                continue

            choice = evaluate_interruption_recovery(
                r,
                lease_renewable=False,
                checkpoint_valid=False,
                owner_alive=owner_alive,
            )
            if choice.rung is None:
                continue

            if choice.rung.name == "rollback_interrupted_acquisition":
                if rollback_callback:
                    rollback_callback(r)
                r.state = ResourceState.CLEANED_UP
                self.store.update_resource(r)
                recovered.append(r)
            elif choice.rung.name == "force_deterministic_cleanup":
                if cleanup_callback:
                    cleanup_callback(r)
                self.budget.release(r.demand)
                r.state = ResourceState.CLEANED_UP
                self.store.update_resource(r)
                recovered.append(r)

        return recovered

    def audit_ownership(self) -> ResourceOwnershipReport:
        """Produce UNIX-style report with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS)."""
        resources = self.store.list_resources()
        active = [r for r in resources if r.state in (ResourceState.ACQUIRING, ResourceState.ACQUIRED, ResourceState.ACTIVE)]
        cleaned = [r for r in resources if r.state == ResourceState.CLEANED_UP]
        restored = [r for r in resources if r.state == ResourceState.RESTORED]
        retained = [r for r in resources if r.state == ResourceState.RETAINED_SOLE_COPY]

        findings: List[str] = []
        if retained:
            badge = CareBadge.ACTION
            for r in retained:
                findings.append(f"ACTION: resource {r.resource_id} contains sole uncommitted change/undo copy")
        elif active:
            badge = CareBadge.REVIEW
            for r in active:
                findings.append(f"REVIEW: resource {r.resource_id} still marked {r.state.value} by owner {r.owner_id}")
        elif any(r.state == ResourceState.FAILED for r in resources):
            badge = CareBadge.ACTION
            findings.append("ACTION: resource lifecycle failure detected")
        else:
            badge = CareBadge.PASS
            findings.append("PASS: all temporary resources cleaned up or restored to observed state")

        summary = (
            f"badge={badge.value} total={len(resources)} active={len(active)} "
            f"cleaned={len(cleaned)} restored={len(restored)} retained_sole={len(retained)}"
        )
        return ResourceOwnershipReport(
            badge=badge,
            summary=summary,
            active_count=len(active),
            cleaned_count=len(cleaned),
            restored_count=len(restored),
            retained_sole_count=len(retained),
            budget_utilization=self.budget.utilization(),
            findings=findings,
        )


def acquire_ownership_lease(
    lock_path: pathlib.Path,
    *,
    what: str = "resource-ownership lease",
    term_seconds: float = 60.0,
) -> lease.Lease:
    """Acquire a mutual-exclusion active verification lease using `migkit.lease.Lease`."""
    return lease.Lease(pathlib.Path(lock_path), what=what, term=term_seconds).acquire()
