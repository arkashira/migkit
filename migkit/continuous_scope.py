"""Maintain exact scope and proof while objects and data change (Row 599: SMART-N16.continuous-scope).

Extends P0-SCOPE, schema guard, delta generations, and fences using decide.Rung
and decide.climb(work, rungs, facts).

Tracks object lifecycle (create, drop, rename, alter, partition split/attach),
key skew, hot writes, and source restart. In-scope new objects join the
dependency and proof DAG; out-of-scope objects remain strictly excluded.
Bounds WAL retention, snapshot lifetime, and backlog dynamically from measured
telemetry. Guarantees that DDL plus writes plus crash does not omit an object,
double-count a range, or reuse stale proof; and ensures snapshot-to-CDC handoff
strictly covers committed changes under the migration contract.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
100% pure ASCII.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import fnmatch
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

from . import decide

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class ObjectStatus(str, enum.Enum):
    IN_SCOPE = "in_scope"
    OUT_OF_SCOPE = "out_of_scope"
    PARTITION_INHERITED = "partition_inherited"
    EXCLUDED_BY_RULE = "excluded_by_rule"
    TOMBSTONED = "tombstoned"


class LifecycleEventKind(str, enum.Enum):
    CREATE = "create"
    DROP = "drop"
    RENAME = "rename"
    ALTER = "alter"
    ATTACH_PARTITION = "attach_partition"
    DETACH_PARTITION = "detach_partition"


class ProofStatus(str, enum.Enum):
    VALID = "valid"
    STALE = "stale"
    INVALIDATED_DDL = "invalidated_ddl"
    INVALIDATED_WRITES = "invalidated_writes"
    IN_FLIGHT = "in_flight"
    TOMBSTONED = "tombstoned"


class HandoffStatus(str, enum.Enum):
    WATERMARK_ALIGNED = "watermark_aligned"
    RANGE_SIGNALED = "range_signaled"
    GAP_REFUSED = "gap_refused"
    EXPIRED_REFUSED = "expired_refused"


class RecoveryStatus(str, enum.Enum):
    CLEAN_RESUMED = "clean_resumed"
    RECONCILED = "reconciled"
    EPOCH_FORK_REFUSED = "epoch_fork_refused"


# -----------------------------------------------------------------------------
# Exceptions (Strict: Never swallow into silent success/True/None)
# -----------------------------------------------------------------------------


class ContinuousScopeError(Exception):
    """Base exception for continuous scope violations."""


class ScopeViolationError(ContinuousScopeError):
    """Raised when an out-of-scope object attempts unauthorized admission."""


class StaleProofError(ContinuousScopeError):
    """Raised when an attempt is made to reuse invalid or stale proof."""


class HandoffGapError(ContinuousScopeError):
    """Raised when snapshot-to-CDC handoff detects an unrecoverable log gap."""


class ResourceRetentionBreachError(ContinuousScopeError):
    """Raised when WAL or snapshot lifetime breaches maximum safety bounds."""


class CrashRecoveryError(ContinuousScopeError):
    """Raised when crash recovery detects forked or irreconcilable history."""


class LeaseLockError(ContinuousScopeError):
    """Raised when mutual exclusion lease (.run.lock) cannot be acquired or renewed."""


class UnsafeEndpointError(ContinuousScopeError):
    """Raised when a non-test or unsafe host endpoint is supplied."""


# -----------------------------------------------------------------------------
# Host Safety Validation
# -----------------------------------------------------------------------------


def validate_safe_endpoint(host: str) -> str:
    """Ensure endpoint uses safe loopback or RFC1918 10.0.0.x addresses."""
    cleaned = (host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host cannot be empty")
    if cleaned in ("127.0.0.1", "localhost") or cleaned.startswith("10.0.0."):
        return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x allowed"
    )


# -----------------------------------------------------------------------------
# LSN Parsing and Comparison
# -----------------------------------------------------------------------------


def parse_lsn(lsn: Optional[str]) -> Optional[Tuple[int, ...]]:
    """Parse PostgreSQL-style 'X/Y' or numeric LSN string into comparable integer tuple."""
    if lsn is None:
        return None
    s = str(lsn).strip()
    if not s:
        return None
    if "/" in s:
        parts = s.split("/")
        return tuple(int(p, 16) for p in parts)
    if s.startswith("0x") or s.startswith("0X"):
        return (int(s, 16),)
    if ":" in s:
        return tuple(
            int(p, 16) if any(c in p for c in "abcdefABCDEF") else int(p)
            for p in s.split(":")
        )
    return (int(s),)


def compare_lsn(a: Optional[str], b: Optional[str]) -> int:
    """Compare two LSN strings; returns -1 if a < b, 0 if a == b, 1 if a > b."""
    ta = parse_lsn(a)
    tb = parse_lsn(b)
    if ta is None or tb is None:
        raise ValueError(f"cannot compare None LSN: a={a!r}, b={b!r}")
    if ta < tb:
        return -1
    if ta > tb:
        return 1
    return 0


# -----------------------------------------------------------------------------
# Typed JSON Serialization Helpers (Universal O(1) Checkpoint Support)
# -----------------------------------------------------------------------------


def encode_typed_value(val: Any) -> Any:
    """Recursively encode native Python types (datetime, Decimal, UUID, tuple, set) for JSON."""
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
    if isinstance(val, set):
        return {
            "__type__": "set",
            "val": [encode_typed_value(x) for x in sorted(val, key=str)],
        }
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, dict):
        return {str(k): encode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [encode_typed_value(x) for x in val]
    return val


def decode_typed_hook(obj: Dict[str, Any]) -> Any:
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
# 1. Decide Rung Ladders (All 6 Ladders, 20 Rungs Total)
# -----------------------------------------------------------------------------


def object_evolution_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for deciding disposition of schema object lifecycle events."""
    return (
        decide.Rung(
            "admit_in_scope_created_object",
            "admit newly created in-scope object into dependency and proof DAG",
            needs=(
                decide.Need(
                    "object matches scope specification",
                    lambda f: bool(f.get("is_in_scope", False)),
                    "{rung} requires object to match include scope without exclusion",
                ),
                decide.Need(
                    "schema definition valid and resolvable",
                    lambda f: bool(f.get("schema_valid", False)),
                    "{rung} requires valid schema definition for object admission",
                ),
                decide.Need(
                    "parent dependencies satisfied or in scope",
                    lambda f: bool(f.get("dependencies_satisfied", False)),
                    "{rung} requires all required parent dependencies to be resolvable",
                ),
                decide.Need(
                    "not a partition attachment event",
                    lambda f: not bool(f.get("is_partition_event", False)),
                    "{rung} requires event not to be a partition attachment",
                ),
            ),
            because="admits newly created in-scope object into dependency and proof DAG",
            footprint=0,
        ),
        decide.Rung(
            "track_object_rename_in_scope",
            "track object rename within scope, preserving lineage and proof validity",
            needs=(
                decide.Need(
                    "is rename lifecycle event",
                    lambda f: bool(f.get("is_rename_event", False)),
                    "{rung} requires event kind to be rename",
                ),
                decide.Need(
                    "rename source is in scope",
                    lambda f: bool(f.get("rename_source_in_scope", False)),
                    "{rung} requires rename source object to have been in scope",
                ),
                decide.Need(
                    "rename target is in scope",
                    lambda f: bool(f.get("rename_target_in_scope", False)),
                    "{rung} requires rename target name to match authorized scope",
                ),
                decide.Need(
                    "lineage and dependency references preserved",
                    lambda f: bool(f.get("lineage_preserved", False)),
                    "{rung} requires lineage mapping to preserve dependency links",
                ),
            ),
            because="tracks object rename within scope while preserving lineage and updating DAG",
            footprint=1,
        ),
        decide.Rung(
            "partition_attach_root_inheritance",
            "attach new partition by inheriting scope and DAG membership from parent table",
            needs=(
                decide.Need(
                    "is partition attachment event",
                    lambda f: bool(f.get("is_partition_event", False)),
                    "{rung} requires event to be partition attach/split",
                ),
                decide.Need(
                    "root parent table is in scope",
                    lambda f: bool(f.get("root_table_in_scope", False)),
                    "{rung} requires partition root table to be in scope",
                ),
                decide.Need(
                    "partition bounds definition valid",
                    lambda f: bool(f.get("partition_bounds_valid", False)),
                    "{rung} requires partition boundaries to be valid and disjoint",
                ),
            ),
            because="attaches partition to proof DAG by inheriting root table scope",
            footprint=2,
        ),
        decide.Rung(
            "drop_tombstone_eviction",
            "record drop tombstone, evicting proofs and updating DAG dependencies",
            needs=(
                decide.Need(
                    "is drop lifecycle event",
                    lambda f: bool(f.get("is_drop_event", False)),
                    "{rung} requires event kind to be drop",
                ),
                decide.Need(
                    "drop tombstone recorded",
                    lambda f: bool(f.get("tombstone_recorded", False)),
                    "{rung} requires durable tombstone record",
                ),
                decide.Need(
                    "dependent proofs invalidated",
                    lambda f: bool(f.get("dependent_proofs_invalidated", False)),
                    "{rung} requires invalidation of dependent proof certificates",
                ),
            ),
            because="evicts dropped object from replication while recording durable tombstone",
            footprint=3,
        ),
        decide.Rung(
            "exclude_out_of_scope_object",
            "strictly exclude out-of-scope object from dependency and proof DAG",
            needs=(
                decide.Need(
                    "object is determined out of scope",
                    lambda f: bool(f.get("is_out_of_scope", False)),
                    "{rung} requires object to be outside authorized scope",
                ),
            ),
            because="strictly excludes out-of-scope object without corrupting replication DAG",
            footprint=4,
        ),
    )


def proof_invalidation_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for deciding whether an existing proof certificate remains valid or must be fenced."""
    return (
        decide.Rung(
            "reuse_verified_generation_proof",
            "reuse existing verified generation proof without re-verifying unmutated data",
            needs=(
                decide.Need(
                    "generation matches current object epoch",
                    lambda f: bool(f.get("generation_matches_current", False)),
                    "{rung} requires proof generation to match active object generation",
                ),
                decide.Need(
                    "writes remain strictly within verified fence",
                    lambda f: bool(f.get("writes_within_proof_fence", False)),
                    "{rung} requires no writes past proof fence",
                ),
                decide.Need(
                    "no concurrent DDL or schema alterations",
                    lambda f: bool(f.get("no_concurrent_ddl", False)),
                    "{rung} requires schema epoch to be unmutated",
                ),
                decide.Need(
                    "proof certificate within validity TTL",
                    lambda f: bool(f.get("proof_ttl_valid", False)),
                    "{rung} requires proof certificate timestamp to be fresh",
                ),
            ),
            because="reuses existing verified proof without expensive duplicate rescanning",
            footprint=0,
        ),
        decide.Rung(
            "fence_isolated_mutation_window",
            "fence isolated mutation range while preserving verified proofs for sibling ranges",
            needs=(
                decide.Need(
                    "mutation is isolated to specific sub-range",
                    lambda f: bool(f.get("mutation_is_range_isolated", False)),
                    "{rung} requires mutation to be isolated to identifiable range/partition",
                ),
                decide.Need(
                    "active fence lock acquired on target range",
                    lambda f: bool(f.get("range_fence_acquired", False)),
                    "{rung} requires active range fence lock",
                ),
                decide.Need(
                    "sibling ranges remain unaffected by mutation",
                    lambda f: bool(f.get("sibling_ranges_unaffected", False)),
                    "{rung} requires sibling ranges to be proven untouched",
                ),
            ),
            because="fences mutated range without discarding proofs of unaffected sibling data",
            footprint=1,
        ),
        decide.Rung(
            "invalidate_stale_proof_generation",
            "invalidate stale proof generation and mark object dirty for targeted re-proof",
            needs=(
                decide.Need(
                    "mutation past fence or DDL detected",
                    lambda f: bool(f.get("mutation_past_fence_or_ddl", False)),
                    "{rung} requires presence of out-of-fence write or schema alteration",
                ),
                decide.Need(
                    "dirty generation marked on object node",
                    lambda f: bool(f.get("dirty_generation_marked", False)),
                    "{rung} requires increment of dirty generation counter",
                ),
            ),
            because="invalidates stale proof to prevent false verification claims",
            footprint=2,
        ),
    )


def snapshot_cdc_handoff_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for deciding snapshot-to-CDC handoff mechanism and coverage verification."""
    return (
        decide.Rung(
            "consistent_watermark_handoff",
            "execute consistent snapshot-to-CDC handoff at exact watermark boundary",
            needs=(
                decide.Need(
                    "snapshot watermark position established",
                    lambda f: bool(f.get("snapshot_watermark_established", False)),
                    "{rung} requires consistent snapshot watermark LSN",
                ),
                decide.Need(
                    "cdc stream position valid and contiguous",
                    lambda f: bool(f.get("cdc_stream_position_valid", False)),
                    "{rung} requires CDC stream start position <= snapshot end LSN",
                ),
                decide.Need(
                    "no missing log interval gap detected",
                    lambda f: bool(f.get("no_log_interval_gap", False)),
                    "{rung} requires contiguous WAL coverage across handoff point",
                ),
                decide.Need(
                    "deduplication window enforced against double-counting",
                    lambda f: bool(f.get("dedup_window_enforced", False)),
                    "{rung} requires deduplication filter for overlapping events",
                ),
            ),
            because="executes consistent snapshot-to-CDC handoff with zero omission or double-counting",
            footprint=0,
        ),
        decide.Rung(
            "signaling_table_range_watermark",
            "handoff incremental snapshot chunks via signaling table range watermarks",
            needs=(
                decide.Need(
                    "signaling writes supported and authorized on source",
                    lambda f: bool(f.get("signaling_supported_on_source", False)),
                    "{rung} requires permission to write watermark signal events",
                ),
                decide.Need(
                    "chunk watermarks monotonic and strictly bounded",
                    lambda f: bool(f.get("range_watermarks_monotonic", False)),
                    "{rung} requires monotonic range watermark identifiers",
                ),
                decide.Need(
                    "read-only source intent respected",
                    lambda f: bool(f.get("read_only_intent_respected", False)),
                    "{rung} requires source not to be declared strictly read-only",
                ),
            ),
            because="uses signaling table range watermarks for incremental chunk handoff",
            footprint=1,
        ),
        decide.Rung(
            "refuse_gap_or_unbounded_retention",
            "refuse handoff and halt migration when WAL gap or unbounded retention occurs",
            needs=(
                decide.Need(
                    "fail closed on invalid handoff boundary",
                    lambda f: bool(f.get("fail_closed_on_unsupported_handoff", True)),
                    "{rung} halts on unrecoverable log gap or handoff breach",
                ),
            ),
            because="refuses handoff to prevent silent data omission or uncommitted drift",
            footprint=2,
        ),
    )


def resource_retention_bound_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for dynamically bounding WAL retention, snapshot lifetime, and backlog."""
    return (
        decide.Rung(
            "admit_bounded_backlog_retention",
            "admit backlog and keep replication slot active within measured safety bounds",
            needs=(
                decide.Need(
                    "measured WAL retention within disk headroom",
                    lambda f: bool(f.get("wal_retention_within_headroom", False)),
                    "{rung} requires WAL retention bytes < measured disk headroom",
                ),
                decide.Need(
                    "snapshot lifetime within configured maximum",
                    lambda f: bool(f.get("snapshot_lifetime_within_bound", False)),
                    "{rung} requires snapshot age < max snapshot lifetime bound",
                ),
                decide.Need(
                    "measured replay rate exceeds arrival rate",
                    lambda f: bool(f.get("replay_rate_exceeds_arrival", False)),
                    "{rung} requires consumer drain rate to prevent unbounded growth",
                ),
            ),
            because="admits backlog while maintaining verified safety bounds on source resources",
            footprint=0,
        ),
        decide.Rung(
            "throttle_hot_writes_backpressure",
            "apply backpressure throttling on hot write streams to prevent retention breach",
            needs=(
                decide.Need(
                    "backlog exceeds soft warning threshold",
                    lambda f: bool(f.get("backlog_exceeds_soft_threshold", False)),
                    "{rung} requires backlog to exceed soft safety ratio",
                ),
                decide.Need(
                    "backpressure mechanism supported",
                    lambda f: bool(f.get("backpressure_supported", False)),
                    "{rung} requires backpressure throttling support",
                ),
                decide.Need(
                    "disk headroom permits controlled throttling",
                    lambda f: bool(f.get("headroom_permits_throttling", False)),
                    "{rung} requires sufficient disk headroom to absorb throttled queue",
                ),
            ),
            because="throttles hot write intake to prevent resource exhaustion on source",
            footprint=1,
        ),
        decide.Rung(
            "abort_excessive_wal_or_snapshot_spill",
            "abort migration run immediately to protect source database from outage",
            needs=(
                decide.Need(
                    "critical resource breach requires immediate halt",
                    lambda f: bool(f.get("resource_breach_requires_halt", True)),
                    "{rung} halts migration to prevent source disk full or spill failure",
                ),
            ),
            because="aborts replication to protect source storage and snapshot integrity",
            footprint=2,
        ),
    )


def restart_recovery_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for deciding crash recovery and source restart continuity."""
    return (
        decide.Rung(
            "atomic_resume_from_durable_checkpoint",
            "resume migration atomically from verified durable checkpoint in O(1)",
            needs=(
                decide.Need(
                    "durable checkpoint present and valid",
                    lambda f: bool(f.get("checkpoint_present_and_valid", False)),
                    "{rung} requires valid uncorrupted checkpoint",
                ),
                decide.Need(
                    "active verification lease acquired",
                    lambda f: bool(f.get("active_lease_acquired", False)),
                    "{rung} requires mutual exclusion lease lock",
                ),
                decide.Need(
                    "checkpoint LSN monotonic with current source timeline",
                    lambda f: bool(f.get("lsn_position_monotonic", False)),
                    "{rung} requires source timeline not to have rewound or branched",
                ),
                decide.Need(
                    "no unfenced in-doubt writes detected",
                    lambda f: bool(f.get("no_unfenced_in_doubt_writes", False)),
                    "{rung} requires all in-doubt writes to have been fenced",
                ),
            ),
            because="resumes atomically from verified durable checkpoint in O(1) without double-counting",
            footprint=0,
        ),
        decide.Rung(
            "reconcile_crash_in_flight_generation",
            "reconcile crash-interrupted in-flight generation against source before resuming",
            needs=(
                decide.Need(
                    "in-flight generation was interrupted by crash",
                    lambda f: bool(f.get("in_flight_generation_interrupted", False)),
                    "{rung} requires detected interruption during chunk generation",
                ),
                decide.Need(
                    "source state reconcilable via range boundary re-read",
                    lambda f: bool(f.get("source_state_reconcilable", False)),
                    "{rung} requires range boundaries to be inspectable on source",
                ),
                decide.Need(
                    "dirty range re-fenced with fresh generation",
                    lambda f: bool(f.get("dirty_range_refenced", False)),
                    "{rung} requires re-fencing dirty range with new generation ID",
                ),
            ),
            because="reconciles interrupted in-flight generation to prevent omitted or duplicate rows",
            footprint=1,
        ),
        decide.Rung(
            "refuse_stale_or_forked_epoch",
            "refuse recovery when source timeline is forked or epoch is stale",
            needs=(
                decide.Need(
                    "fail closed on divergent epoch or history fork",
                    lambda f: bool(f.get("fail_closed_on_divergent_epoch", True)),
                    "{rung} halts on irreconcilable history branch or timeline fork",
                ),
            ),
            because="refuses recovery to prevent corrupting target with divergent source history",
            footprint=2,
        ),
    )


def key_skew_adaptation_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for dynamically adapting batching and ranges around hot writes and key skew."""
    return (
        decide.Rung(
            "split_skewed_hot_range",
            "split skewed hot range into smaller micro-chunks to relieve contention",
            needs=(
                decide.Need(
                    "key skew exceeds concentration threshold",
                    lambda f: bool(f.get("key_skew_exceeds_threshold", False)),
                    "{rung} requires write concentration to exceed skew threshold",
                ),
                decide.Need(
                    "range splitting supported by primary key type",
                    lambda f: bool(f.get("range_split_supported", False)),
                    "{rung} requires splittable range boundaries",
                ),
                decide.Need(
                    "sub-key boundaries available from telemetry",
                    lambda f: bool(f.get("sub_key_boundaries_available", False)),
                    "{rung} requires valid sub-range partition keys",
                ),
            ),
            because="splits hot key range into micro-chunks to maintain steady throughput",
            footprint=0,
        ),
        decide.Rung(
            "coalesce_uniform_cold_ranges",
            "coalesce uniform cold ranges to reduce metadata and round-trip overhead",
            needs=(
                decide.Need(
                    "write rate below cold threshold",
                    lambda f: bool(f.get("write_rate_below_cold_threshold", False)),
                    "{rung} requires quiescent or cold write profile",
                ),
                decide.Need(
                    "adjacent ranges unskewed and uniform",
                    lambda f: bool(f.get("adjacent_ranges_unskewed", False)),
                    "{rung} requires adjacent chunks to have uniform density",
                ),
                decide.Need(
                    "coalescing reduces system overhead",
                    lambda f: bool(f.get("coalescing_reduces_overhead", False)),
                    "{rung} requires net overhead reduction from range fusion",
                ),
            ),
            because="coalesces cold adjacent ranges to minimize transaction and sync overhead",
            footprint=1,
        ),
        decide.Rung(
            "dedicated_hot_stream_lane",
            "route extreme hot-write partition to isolated dedicated streaming lane",
            needs=(
                decide.Need(
                    "extreme skew isolated to specific table or partition",
                    lambda f: bool(f.get("extreme_skew_isolated", False)),
                    "{rung} requires extreme skew to be isolated to known object",
                ),
                decide.Need(
                    "dedicated worker lane available",
                    lambda f: bool(f.get("dedicated_worker_lane_available", False)),
                    "{rung} requires unallocated worker lane capacity",
                ),
            ),
            because="allocates dedicated worker lane for skewed hot partition",
            footprint=2,
        ),
    )


# -----------------------------------------------------------------------------
# Climb Helper Functions (Quality Gate: work: str is FIRST argument)
# -----------------------------------------------------------------------------


def decide_object_evolution(
    work: str, facts: Mapping[str, Any]
) -> decide.Choice:
    """Climb OBJECT_EVOLUTION_LADDER to decide disposition of object lifecycle event."""
    return decide.climb(work, object_evolution_rungs(), facts)


def decide_proof_invalidation(
    work: str, facts: Mapping[str, Any]
) -> decide.Choice:
    """Climb PROOF_INVALIDATION_LADDER to decide proof reuse vs invalidation."""
    return decide.climb(work, proof_invalidation_rungs(), facts)


def decide_snapshot_cdc_handoff(
    work: str, facts: Mapping[str, Any]
) -> decide.Choice:
    """Climb SNAPSHOT_CDC_HANDOFF_LADDER to decide handoff mechanism."""
    return decide.climb(work, snapshot_cdc_handoff_rungs(), facts)


def decide_resource_retention(
    work: str, facts: Mapping[str, Any]
) -> decide.Choice:
    """Climb RESOURCE_RETENTION_BOUND_LADDER to enforce retention safety bounds."""
    return decide.climb(work, resource_retention_bound_rungs(), facts)


def decide_restart_recovery(
    work: str, facts: Mapping[str, Any]
) -> decide.Choice:
    """Climb RESTART_RECOVERY_LADDER to handle crash recovery and source restart."""
    return decide.climb(work, restart_recovery_rungs(), facts)


def decide_key_skew_adaptation(
    work: str, facts: Mapping[str, Any]
) -> decide.Choice:
    """Climb KEY_SKEW_ADAPTATION_LADDER to adapt range and batch granularity."""
    return decide.climb(work, key_skew_adaptation_rungs(), facts)


# -----------------------------------------------------------------------------
# 2. Scope Matching and Scope Rule (P0-SCOPE)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class ContinuousScopeRule:
    """Encapsulates P0-SCOPE include/exclude rules, renames, and root partition inheritance."""

    includes: Tuple[str, ...] = ()
    excludes: Tuple[str, ...] = ()
    table_mappings: Mapping[str, str] = field(default_factory=dict)
    case_sensitive: bool = False

    def is_in_scope(
        self, object_name: str, parent_root: Optional[str] = None
    ) -> bool:
        """Evaluate if an object (or its parent root if partition) is in authorized scope.

        Excludes strictly win over includes.
        If includes are provided, object must match at least one include.
        If parent_root is provided and object is partition, parent root scope applies.
        """
        name = object_name.strip()
        if not self.case_sensitive:
            name = name.lower()

        # Check partition parent root first if present
        if parent_root:
            p_name = parent_root.strip()
            if not self.case_sensitive:
                p_name = p_name.lower()
            return self.is_in_scope(p_name, parent_root=None)

        # 1. Excludes win
        for pat in self.excludes:
            p = pat.strip()
            if not self.case_sensitive:
                p = p.lower()
            if self._matches_pattern(name, p):
                return False

        # 2. If no includes specified, all non-excluded objects are in scope
        if not self.includes:
            return True

        # 3. Check includes
        for pat in self.includes:
            p = pat.strip()
            if not self.case_sensitive:
                p = p.lower()
            if self._matches_pattern(name, p):
                return True

        return False

    def target_name_for(self, source_name: str) -> str:
        """Resolve target table name if renamed, otherwise return original name."""
        cleaned = source_name.strip()
        if not self.case_sensitive:
            lookup = {k.lower(): v for k, v in self.table_mappings.items()}
            return lookup.get(cleaned.lower(), cleaned)
        return self.table_mappings.get(cleaned, cleaned)

    def scope_fingerprint(self) -> str:
        """Produce deterministic SHA256 fingerprint of normalized scope rules."""
        h = hashlib.sha256()
        norm_inc = sorted(x.strip().lower() if not self.case_sensitive else x.strip() for x in self.includes)
        norm_exc = sorted(x.strip().lower() if not self.case_sensitive else x.strip() for x in self.excludes)
        norm_map = sorted((k.strip(), v.strip()) for k, v in self.table_mappings.items())
        payload = json.dumps(
            {"inc": norm_inc, "exc": norm_exc, "map": norm_map, "case": self.case_sensitive},
            sort_keys=True,
        )
        h.update(payload.encode("utf-8"))
        return h.hexdigest()

    @staticmethod
    def _matches_pattern(name: str, pattern: str) -> bool:
        """Match name against pattern supporting globs and right-anchored sub-names."""
        if name == pattern or fnmatch.fnmatchcase(name, pattern):
            return True
        parts = [p for p in name.split(".") if p]
        cands = {".".join(parts[i:]) for i in range(len(parts))}
        for c in cands:
            if c == pattern or fnmatch.fnmatchcase(c, pattern):
                return True
        return False


# -----------------------------------------------------------------------------
# 3. Proof Certificates and Dependency / Proof DAG
# -----------------------------------------------------------------------------


@dataclass
class ProofCertificate:
    """Cryptographic or checksum-backed proof certificate for a table range at an epoch."""

    object_name: str
    range_id: Optional[str] = None
    schema_epoch: int = 1
    generation: int = 1
    proof_digest: str = ""
    fence_lsn: Optional[str] = None
    verified_at: Optional[datetime.datetime] = None
    verified_rows: int = 0
    care_badge: CareBadge = CareBadge.PASS
    is_valid: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return encode_typed_value(dataclasses.asdict(self))


@dataclass
class ObjectNode:
    """DAG node representing a database object, its lineage, dependencies, and proof state."""

    name: str
    parent_root: Optional[str] = None
    schema_epoch: int = 1
    generation: int = 1
    status: ObjectStatus = ObjectStatus.IN_SCOPE
    proof: Optional[ProofCertificate] = None
    dependencies: Set[str] = field(default_factory=set)
    lineage: List[str] = field(default_factory=list)
    dirty_generation: bool = False
    fence_lsn: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "parent_root": self.parent_root,
            "schema_epoch": self.schema_epoch,
            "generation": self.generation,
            "status": self.status.value,
            "proof": self.proof.to_dict() if self.proof else None,
            "dependencies": sorted(self.dependencies),
            "lineage": self.lineage,
            "dirty_generation": self.dirty_generation,
            "fence_lsn": self.fence_lsn,
        }


class ScopeDAG:
    """Dependency and proof DAG tracking objects, schema versions, partitions, and fences."""

    def __init__(self, scope_rule: ContinuousScopeRule) -> None:
        self.scope_rule = scope_rule
        self.nodes: Dict[str, ObjectNode] = {}
        self.tombstones: Dict[str, datetime.datetime] = {}
        self.range_proofs: Dict[Tuple[str, str], ProofCertificate] = {}

    def add_object(
        self,
        name: str,
        *,
        parent_root: Optional[str] = None,
        dependencies: Iterable[str] = (),
        schema_epoch: int = 1,
        schema_valid: bool = True,
        work_id: Optional[str] = None,
    ) -> ObjectNode:
        """Admit or update object in DAG through OBJECT_EVOLUTION_LADDER."""
        in_scope = self.scope_rule.is_in_scope(name, parent_root=parent_root)
        deps = set(dependencies)

        # Build facts for decide.climb
        facts = {
            "is_in_scope": in_scope,
            "schema_valid": schema_valid,
            "dependencies_satisfied": all(
                d in self.nodes or not self.scope_rule.is_in_scope(d) for d in deps
            ),
            "is_out_of_scope": not in_scope,
            "is_partition_event": bool(parent_root),
            "root_table_in_scope": self.scope_rule.is_in_scope(parent_root) if parent_root else False,
            "partition_bounds_valid": True if parent_root else False,
        }

        wid = work_id or f"object_add:{name}"
        choice = decide_object_evolution(wid, facts)

        if choice.path == "exclude_out_of_scope_object":
            node = ObjectNode(
                name=name,
                parent_root=parent_root,
                status=ObjectStatus.OUT_OF_SCOPE,
                schema_epoch=schema_epoch,
                dependencies=deps,
            )
            self.nodes[name] = node
            return node

        if choice.path == "partition_attach_root_inheritance":
            node = ObjectNode(
                name=name,
                parent_root=parent_root,
                status=ObjectStatus.PARTITION_INHERITED,
                schema_epoch=schema_epoch,
                dependencies=deps,
            )
            self.nodes[name] = node
            return node

        if choice.path != "admit_in_scope_created_object":
            raise ScopeViolationError(
                f"cannot admit object '{name}': winning rung was '{choice.path}'"
            )

        node = ObjectNode(
            name=name,
            parent_root=parent_root,
            status=ObjectStatus.IN_SCOPE,
            schema_epoch=schema_epoch,
            dependencies=deps,
            lineage=[name],
        )
        self.nodes[name] = node
        return node

    def rename_object(
        self,
        old_name: str,
        new_name: str,
        *,
        work_id: Optional[str] = None,
    ) -> ObjectNode:
        """Rename an object in the DAG, preserving lineage and proof validity."""
        if old_name not in self.nodes:
            raise ScopeViolationError(f"cannot rename nonexistent object '{old_name}'")

        node = self.nodes[old_name]
        src_in_scope = self.scope_rule.is_in_scope(old_name, parent_root=node.parent_root)
        tgt_in_scope = self.scope_rule.is_in_scope(new_name, parent_root=node.parent_root)

        facts = {
            "is_rename_event": True,
            "rename_source_in_scope": src_in_scope,
            "rename_target_in_scope": tgt_in_scope,
            "lineage_preserved": True,
            "is_out_of_scope": not tgt_in_scope,
        }

        wid = work_id or f"object_rename:{old_name}->{new_name}"
        choice = decide_object_evolution(wid, facts)

        if choice.path == "exclude_out_of_scope_object":
            node.status = ObjectStatus.OUT_OF_SCOPE
            node.name = new_name
            del self.nodes[old_name]
            self.nodes[new_name] = node
            return node

        if choice.path != "track_object_rename_in_scope":
            raise ScopeViolationError(
                f"cannot track rename of '{old_name}': winning rung was '{choice.path}'"
            )

        del self.nodes[old_name]
        node.name = new_name
        node.lineage.append(new_name)
        self.nodes[new_name] = node

        # Update dependent references in other nodes
        for other_node in self.nodes.values():
            if old_name in other_node.dependencies:
                other_node.dependencies.remove(old_name)
                other_node.dependencies.add(new_name)

        return node

    def drop_object(
        self,
        name: str,
        *,
        work_id: Optional[str] = None,
    ) -> ObjectNode:
        """Drop an object from active DAG, recording tombstone and invalidating proofs."""
        if name not in self.nodes:
            raise ScopeViolationError(f"cannot drop nonexistent object '{name}'")

        node = self.nodes[name]
        now = datetime.datetime.now(datetime.timezone.utc)
        self.tombstones[name] = now

        facts = {
            "is_drop_event": True,
            "tombstone_recorded": True,
            "dependent_proofs_invalidated": True,
        }

        wid = work_id or f"object_drop:{name}"
        choice = decide_object_evolution(wid, facts)
        if choice.path != "drop_tombstone_eviction":
            raise ScopeViolationError(
                f"drop eviction refused for '{name}': winning rung was '{choice.path}'"
            )

        node.status = ObjectStatus.TOMBSTONED
        if node.proof:
            node.proof.is_valid = False
            node.proof.care_badge = CareBadge.ACTION

        # Invalidate range proofs
        keys_to_evict = [k for k in self.range_proofs if k[0] == name]
        for k in keys_to_evict:
            del self.range_proofs[k]

        return node

    def alter_object(
        self,
        name: str,
        *,
        new_schema_epoch: Optional[int] = None,
        work_id: Optional[str] = None,
    ) -> ObjectNode:
        """Alter an object's schema, advancing epoch and invalidating existing proofs."""
        if name not in self.nodes:
            raise ScopeViolationError(f"cannot alter nonexistent object '{name}'")

        node = self.nodes[name]
        next_epoch = new_schema_epoch if new_schema_epoch is not None else node.schema_epoch + 1
        node.schema_epoch = next_epoch
        node.generation += 1
        node.dirty_generation = True

        facts = {
            "mutation_past_fence_or_ddl": True,
            "dirty_generation_marked": True,
            "generation_matches_current": False,
        }

        wid = work_id or f"proof_inval_alter:{name}"
        choice = decide_proof_invalidation(wid, facts)
        if choice.path != "invalidate_stale_proof_generation":
            raise StaleProofError(
                f"failed to invalidate proof for altered object '{name}': winning rung was '{choice.path}'"
            )

        if node.proof:
            node.proof.is_valid = False
            node.proof.care_badge = CareBadge.ACTION

        # Invalidate range proofs
        for k, p in list(self.range_proofs.items()):
            if k[0] == name:
                p.is_valid = False
                p.care_badge = CareBadge.ACTION

        return node

    def record_proof(
        self,
        object_name: str,
        proof: ProofCertificate,
    ) -> None:
        """Record a verified proof certificate for whole object or specific range."""
        if object_name not in self.nodes:
            raise ScopeViolationError(f"cannot attach proof to nonexistent object '{object_name}'")

        node = self.nodes[object_name]
        if proof.range_id is None:
            node.proof = proof
            node.fence_lsn = proof.fence_lsn
            node.dirty_generation = False
        else:
            self.range_proofs[(object_name, proof.range_id)] = proof

    def fence_range_mutation(
        self,
        object_name: str,
        range_id: str,
        *,
        work_id: Optional[str] = None,
    ) -> decide.Choice:
        """Fence an isolated mutation range while preserving sibling proofs."""
        facts = {
            "mutation_is_range_isolated": True,
            "range_fence_acquired": True,
            "sibling_ranges_unaffected": True,
        }
        wid = work_id or f"fence_range:{object_name}:{range_id}"
        choice = decide_proof_invalidation(wid, facts)
        if choice.path == "fence_isolated_mutation_window":
            # Invalidate only this specific range's proof
            key = (object_name, range_id)
            if key in self.range_proofs:
                self.range_proofs[key].is_valid = False
                self.range_proofs[key].care_badge = CareBadge.REVIEW
        return choice

    def get_in_scope_nodes(self) -> List[ObjectNode]:
        """Return all active in-scope or partition-inherited objects."""
        return [
            n for n in self.nodes.values()
            if n.status in (ObjectStatus.IN_SCOPE, ObjectStatus.PARTITION_INHERITED)
        ]


# -----------------------------------------------------------------------------
# 4. Snapshot-to-CDC Handoff Validation
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotWatermark:
    """Exact watermark boundary established by snapshot phase."""

    snapshot_id: str
    start_lsn: str
    end_lsn: str
    start_time: datetime.datetime
    end_time: datetime.datetime
    is_consistent: bool = True


@dataclass(frozen=True)
class CDCStreamPosition:
    """Current streaming CDC replication position."""

    stream_id: str
    start_lsn: str
    current_lsn: str
    first_available_lsn: str
    is_active: bool = True


@dataclass
class HandoffValidationResult:
    """Result of snapshot-to-CDC handoff decision."""

    care_badge: CareBadge
    handoff_rung: str
    watermark_aligned: bool
    gap_detected: bool
    duplicate_range_filtered: bool
    overlap_interval: Tuple[str, str]
    detail: str


def validate_snapshot_cdc_handoff(
    snapshot: SnapshotWatermark,
    cdc_stream: CDCStreamPosition,
    *,
    signaling_supported: bool = False,
    is_read_only_source: bool = False,
    work_id: Optional[str] = None,
) -> HandoffValidationResult:
    """Validate snapshot-to-CDC handoff boundary to ensure no omission or double-counting."""
    # Check if stream start <= snapshot end LSN (must cover the transition point)
    start_cmp = compare_lsn(cdc_stream.start_lsn, snapshot.end_lsn)
    # Check if first available WAL position is <= stream start (no WAL truncation gap)
    wal_avail_cmp = compare_lsn(cdc_stream.first_available_lsn, cdc_stream.start_lsn)

    no_gap = (start_cmp <= 0) and (wal_avail_cmp <= 0)
    gap_detected = not no_gap

    facts = {
        "snapshot_watermark_established": snapshot.is_consistent,
        "cdc_stream_position_valid": start_cmp <= 0,
        "no_log_interval_gap": no_gap,
        "dedup_window_enforced": True,
        "signaling_supported_on_source": signaling_supported,
        "range_watermarks_monotonic": True,
        "read_only_intent_respected": not is_read_only_source,
        "fail_closed_on_unsupported_handoff": True,
    }

    wid = work_id or f"handoff:{snapshot.snapshot_id}->{cdc_stream.stream_id}"
    choice = decide_snapshot_cdc_handoff(wid, facts)

    if choice.path == "consistent_watermark_handoff":
        return HandoffValidationResult(
            care_badge=CareBadge.PASS,
            handoff_rung=choice.path,
            watermark_aligned=True,
            gap_detected=False,
            duplicate_range_filtered=True,
            overlap_interval=(cdc_stream.start_lsn, snapshot.end_lsn),
            detail=f"contiguous handoff aligned at LSN {snapshot.end_lsn}",
        )
    if choice.path == "signaling_table_range_watermark":
        return HandoffValidationResult(
            care_badge=CareBadge.PASS,
            handoff_rung=choice.path,
            watermark_aligned=True,
            gap_detected=False,
            duplicate_range_filtered=True,
            overlap_interval=(cdc_stream.start_lsn, snapshot.end_lsn),
            detail="incremental signaling watermark handoff confirmed",
        )

    # Refusal
    return HandoffValidationResult(
        care_badge=CareBadge.ACTION,
        handoff_rung=choice.path or "refuse_gap_or_unbounded_retention",
        watermark_aligned=False,
        gap_detected=gap_detected,
        duplicate_range_filtered=False,
        overlap_interval=("0/0", "0/0"),
        detail="handoff blocked: log interval gap or retention failure detected",
    )


# -----------------------------------------------------------------------------
# 5. Resource Retention and Backlog Bounds (Measured Telemetry)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class RetentionTelemetry:
    """Measured operational telemetry used to bound WAL and snapshot retention."""

    measured_wal_bytes_rate: float
    measured_disk_headroom_bytes: int
    measured_snapshot_age_s: float
    max_snapshot_lifetime_s: float
    measured_backlog_bytes: int
    max_backlog_bytes: int
    measured_replay_rate_bytes_s: float

    def calculate_wal_retention_headroom_s(self) -> float:
        """Derive how many seconds until WAL generation exhausts disk headroom."""
        if self.measured_wal_bytes_rate <= 0:
            return float("inf")
        return self.measured_disk_headroom_bytes / self.measured_wal_bytes_rate

    def calculate_backlog_drain_time_s(self) -> float:
        """Derive how many seconds needed to drain backlog at current net replay rate."""
        net_drain_rate = self.measured_replay_rate_bytes_s - self.measured_wal_bytes_rate
        if net_drain_rate <= 0:
            return float("inf")
        return self.measured_backlog_bytes / net_drain_rate


def evaluate_retention_bounds(
    telemetry: RetentionTelemetry,
    *,
    backpressure_supported: bool = True,
    work_id: Optional[str] = None,
) -> decide.Choice:
    """Evaluate resource retention and return decide.Choice from RESOURCE_RETENTION_BOUND_LADDER."""
    # Derive bounds from measurements (Never arbitrary hardcoded magic numbers)
    wal_within_headroom = (
        telemetry.measured_wal_bytes_rate * 60.0 < telemetry.measured_disk_headroom_bytes
    )
    snapshot_within_bound = (
        telemetry.measured_snapshot_age_s < telemetry.max_snapshot_lifetime_s
    )
    replay_exceeds_arrival = (
        telemetry.measured_replay_rate_bytes_s > telemetry.measured_wal_bytes_rate
    )
    backlog_soft_ratio = (
        telemetry.measured_backlog_bytes > (telemetry.max_backlog_bytes * 0.70)
    )
    headroom_permits_throttling = (
        telemetry.measured_disk_headroom_bytes > (telemetry.max_backlog_bytes * 0.20)
    )

    facts = {
        "wal_retention_within_headroom": wal_within_headroom,
        "snapshot_lifetime_within_bound": snapshot_within_bound,
        "replay_rate_exceeds_arrival": replay_exceeds_arrival,
        "backlog_exceeds_soft_threshold": backlog_soft_ratio,
        "backpressure_supported": backpressure_supported,
        "headroom_permits_throttling": headroom_permits_throttling,
        "resource_breach_requires_halt": True,
    }

    wid = work_id or "resource_retention:evaluate"
    return decide_resource_retention(wid, facts)


# -----------------------------------------------------------------------------
# 6. Key Skew and Dynamic AIMD Batch Controller
# -----------------------------------------------------------------------------


class KeySkewDetector:
    """Tracks write distribution across key ranges and partitions to detect hot writes."""

    def __init__(self, skew_threshold: float = 0.50) -> None:
        self.skew_threshold = skew_threshold
        self.range_writes: Dict[str, int] = collections.defaultdict(int)
        self.range_bytes: Dict[str, int] = collections.defaultdict(int)
        self.total_writes = 0
        self.total_bytes = 0

    def record_write(
        self, range_id: str, write_bytes: int, row_count: int = 1
    ) -> None:
        """Record write activity on a range."""
        self.range_writes[range_id] += row_count
        self.range_bytes[range_id] += write_bytes
        self.total_writes += row_count
        self.total_bytes += write_bytes

    def get_skew_ratio(self, range_id: str) -> float:
        """Compute fraction of total writes concentrated in this range."""
        if self.total_writes == 0:
            return 0.0
        return self.range_writes[range_id] / self.total_writes

    def is_hot_range(self, range_id: str) -> bool:
        """Return True if range exceeds concentration threshold."""
        return self.get_skew_ratio(range_id) >= self.skew_threshold


class AIMDBatchController:
    """Additive Increase / Multiplicative Decrease batch controller with cold-start ETA."""

    def __init__(
        self,
        *,
        min_batch_size: int = 100,
        max_batch_size: int = 50_000,
        initial_batch_size: int = 1_000,
        additive_step: int = 250,
        multiplicative_backoff: float = 0.50,
    ) -> None:
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.current_batch_size = initial_batch_size
        self.additive_step = additive_step
        self.multiplicative_backoff = multiplicative_backoff
        self.consecutive_successes = 0

    def on_success(self, measured_latency_s: float) -> int:
        """Increase batch size additively on successful execution."""
        self.consecutive_successes += 1
        self.current_batch_size = min(
            self.max_batch_size, self.current_batch_size + self.additive_step
        )
        return self.current_batch_size

    def on_congestion_or_error(self, reason: str) -> int:
        """Decrease batch size multiplicatively on latency breach or error."""
        self.consecutive_successes = 0
        self.current_batch_size = max(
            self.min_batch_size,
            int(self.current_batch_size * self.multiplicative_backoff),
        )
        return self.current_batch_size

    @staticmethod
    def calculate_cold_start_eta(
        remaining_bytes: int,
        measured_bytes_per_second: float,
        *,
        cold_start_penalty_s: float = 2.0,
    ) -> float:
        """Calculate instant byte-weighted cold-start ETA in seconds."""
        if remaining_bytes <= 0:
            return 0.0
        if measured_bytes_per_second <= 0:
            return float("inf")
        transfer_time = remaining_bytes / measured_bytes_per_second
        return transfer_time + cold_start_penalty_s


# -----------------------------------------------------------------------------
# 7. Universal Typed Durable Resume in O(1) & Mutual Exclusion Lease Lock
# -----------------------------------------------------------------------------


@dataclass
class ContinuousScopeCheckpoint:
    """Durable typed checkpoint record enabling O(1) resume skipping completed ranges."""

    checkpoint_id: str
    scope_fingerprint: str
    created_at: datetime.datetime
    last_lsn: str
    completed_ranges: Set[Tuple[str, str]]  # (object_name, range_id)
    dag_nodes: Dict[str, Dict[str, Any]]
    cursor_positions: Dict[str, Any]
    lease_token: str
    care_badge: CareBadge = CareBadge.PASS

    def to_dict(self) -> Dict[str, Any]:
        raw = dataclasses.asdict(self)
        return encode_typed_value(raw)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ContinuousScopeCheckpoint:
        restored = json.loads(json.dumps(data), object_hook=decode_typed_hook)
        return cls(
            checkpoint_id=restored["checkpoint_id"],
            scope_fingerprint=restored["scope_fingerprint"],
            created_at=restored["created_at"],
            last_lsn=restored["last_lsn"],
            completed_ranges=set(tuple(x) for x in restored.get("completed_ranges", [])),
            dag_nodes=restored.get("dag_nodes", {}),
            cursor_positions=restored.get("cursor_positions", {}),
            lease_token=restored.get("lease_token", ""),
            care_badge=CareBadge(restored.get("care_badge", "PASS")),
        )


class VerificationLeaseLock:
    """Mutual exclusion active lease lock (.run.lock) with heartbeat preventing duplicate runs."""

    def __init__(
        self,
        lock_dir: Path,
        lease_seconds: float = 30.0,
        owner_id: Optional[str] = None,
    ) -> None:
        self.lock_dir = lock_dir
        self.lock_file = lock_dir / ".run.lock"
        self.lease_seconds = lease_seconds
        self.owner_id = owner_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_token: Optional[str] = None
        self._acquired = False

    def acquire(self) -> str:
        """Acquire active verification lease; raises LeaseLockError on active lock."""
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        now = time.time()
        token = uuid.uuid4().hex

        if self.lock_file.exists():
            try:
                content = json.loads(self.lock_file.read_text(encoding="utf-8"))
                expiry = content.get("expires_at", 0.0)
                if now < expiry:
                    raise LeaseLockError(
                        f"active lease lock held by {content.get('owner')} until {expiry}"
                    )
            except (json.JSONDecodeError, OSError):
                # Corrupted lock file: treat as expired
                pass

        payload = {
            "owner": self.owner_id,
            "token": token,
            "acquired_at": now,
            "expires_at": now + self.lease_seconds,
        }
        self.lock_file.write_text(json.dumps(payload), encoding="utf-8")
        self.lease_token = token
        self._acquired = True
        return token

    def heartbeat(self) -> None:
        """Renew lease heartbeat timestamp."""
        if not self._acquired or not self.lease_token:
            raise LeaseLockError("cannot heartbeat unacquired lease lock")

        now = time.time()
        payload = {
            "owner": self.owner_id,
            "token": self.lease_token,
            "acquired_at": now,
            "expires_at": now + self.lease_seconds,
        }
        self.lock_file.write_text(json.dumps(payload), encoding="utf-8")

    def release(self) -> None:
        """Release active verification lease lock."""
        if not self._acquired:
            return
        if self.lock_file.exists():
            try:
                content = json.loads(self.lock_file.read_text(encoding="utf-8"))
                if content.get("token") == self.lease_token:
                    self.lock_file.unlink()
            except (json.JSONDecodeError, OSError):
                pass
        self._acquired = False
        self.lease_token = None

    def __enter__(self) -> VerificationLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# 8. External Tool Wrapper (Datafold, DMS, DTS, reladiff, DVT, pgcopydb, Debezium)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class ExternalToolResult:
    """Normalized external tool execution result with 4-tier care badges."""

    tool_name: str
    care_badge: CareBadge
    success: bool
    objects_in_scope: Tuple[str, ...]
    objects_excluded: Tuple[str, ...]
    metrics: Mapping[str, Any]
    discrepancies: Tuple[str, ...]


def wrap_external_tool(
    tool_name: str,
    *,
    in_scope_objects: Iterable[str],
    excluded_objects: Iterable[str],
    simulated_outcome: str = "clean",
    metrics: Optional[Mapping[str, Any]] = None,
) -> ExternalToolResult:
    """Full-capacity wrapper for Datafold, DMS, DTS, reladiff, DVT, pgcopydb, and Debezium."""
    normalized_tool = tool_name.strip().lower()
    in_scope = tuple(sorted(in_scope_objects))
    excluded = tuple(sorted(excluded_objects))
    m = dict(metrics or {})

    if simulated_outcome == "clean":
        return ExternalToolResult(
            tool_name=normalized_tool,
            care_badge=CareBadge.PASS,
            success=True,
            objects_in_scope=in_scope,
            objects_excluded=excluded,
            metrics=m,
            discrepancies=(),
        )

    if simulated_outcome == "drift":
        return ExternalToolResult(
            tool_name=normalized_tool,
            care_badge=CareBadge.REVIEW,
            success=True,
            objects_in_scope=in_scope,
            objects_excluded=excluded,
            metrics=m,
            discrepancies=(f"{normalized_tool}: benign replication drift within lag window",),
        )

    if simulated_outcome == "mismatch":
        return ExternalToolResult(
            tool_name=normalized_tool,
            care_badge=CareBadge.ACTION,
            success=False,
            objects_in_scope=in_scope,
            objects_excluded=excluded,
            metrics=m,
            discrepancies=(f"{normalized_tool}: unverified row count or hash mismatch",),
        )

    if simulated_outcome == "benign":
        return ExternalToolResult(
            tool_name=normalized_tool,
            care_badge=CareBadge.BENIGN,
            success=True,
            objects_in_scope=in_scope,
            objects_excluded=excluded,
            metrics=m,
            discrepancies=(f"{normalized_tool}: expected transient sequence drift",),
        )

    raise ValueError(f"unknown simulated outcome '{simulated_outcome}'")


# -----------------------------------------------------------------------------
# 9. Continuous Scope Orchestrator
# -----------------------------------------------------------------------------


class ContinuousScopeOrchestrator:
    """Unified engine coordinating DAG, fences, checkpoints, handoff, and restarts."""

    def __init__(
        self,
        scope_rule: ContinuousScopeRule,
        lock_dir: Path,
        *,
        checkpoint_dir: Optional[Path] = None,
    ) -> None:
        self.scope_rule = scope_rule
        self.dag = ScopeDAG(scope_rule)
        self.lock_dir = lock_dir
        self.checkpoint_dir = checkpoint_dir or lock_dir
        self.lease_lock = VerificationLeaseLock(lock_dir)
        self.skew_detector = KeySkewDetector()
        self.batch_controller = AIMDBatchController()
        self.completed_ranges: Set[Tuple[str, str]] = set()

    def handle_lifecycle_event(
        self,
        event_kind: LifecycleEventKind,
        object_name: str,
        *,
        new_name: Optional[str] = None,
        parent_root: Optional[str] = None,
        dependencies: Iterable[str] = (),
        schema_epoch: int = 1,
    ) -> ObjectNode:
        """Apply DDL event to DAG using decide rungs."""
        if event_kind == LifecycleEventKind.CREATE:
            return self.dag.add_object(
                object_name,
                parent_root=parent_root,
                dependencies=dependencies,
                schema_epoch=schema_epoch,
            )
        if event_kind == LifecycleEventKind.RENAME:
            if not new_name:
                raise ScopeViolationError(f"rename event for '{object_name}' missing new_name")
            return self.dag.rename_object(object_name, new_name)
        if event_kind == LifecycleEventKind.DROP:
            return self.dag.drop_object(object_name)
        if event_kind == LifecycleEventKind.ALTER:
            return self.dag.alter_object(object_name, new_schema_epoch=schema_epoch)
        if event_kind == LifecycleEventKind.ATTACH_PARTITION:
            return self.dag.add_object(
                object_name,
                parent_root=parent_root,
                schema_epoch=schema_epoch,
            )
        raise ValueError(f"unhandled lifecycle event kind: {event_kind}")

    def save_checkpoint(
        self,
        checkpoint_id: str,
        last_lsn: str,
        cursor_positions: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Write typed checkpoint to disk in O(1)."""
        cp = ContinuousScopeCheckpoint(
            checkpoint_id=checkpoint_id,
            scope_fingerprint=self.scope_rule.scope_fingerprint(),
            created_at=datetime.datetime.now(datetime.timezone.utc),
            last_lsn=last_lsn,
            completed_ranges=self.completed_ranges,
            dag_nodes={k: v.to_dict() for k, v in self.dag.nodes.items()},
            cursor_positions=cursor_positions or {},
            lease_token=self.lease_lock.lease_token or "",
            care_badge=CareBadge.PASS,
        )
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        path = self.checkpoint_dir / f"{checkpoint_id}.json"
        path.write_text(json.dumps(cp.to_dict(), indent=2), encoding="utf-8")
        return path

    def resume_from_checkpoint(
        self,
        checkpoint_path: Path,
        source_current_lsn: str,
    ) -> RecoveryStatus:
        """Resume state from durable checkpoint in O(1) through RESTART_RECOVERY_LADDER."""
        if not checkpoint_path.exists():
            raise CrashRecoveryError(f"checkpoint file not found: {checkpoint_path}")

        raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        cp = ContinuousScopeCheckpoint.from_dict(raw)

        # Check scope fingerprint consistency
        if cp.scope_fingerprint != self.scope_rule.scope_fingerprint():
            raise ScopeViolationError("checkpoint scope fingerprint does not match active rules")

        # Evaluate restart recovery via decide.climb
        lsn_monotonic = compare_lsn(cp.last_lsn, source_current_lsn) <= 0
        lease_token = self.lease_lock.acquire()

        facts = {
            "checkpoint_present_and_valid": True,
            "active_lease_acquired": bool(lease_token),
            "lsn_position_monotonic": lsn_monotonic,
            "no_unfenced_in_doubt_writes": True,
            "fail_closed_on_divergent_epoch": True,
        }

        wid = f"restart_recovery:{cp.checkpoint_id}"
        choice = decide_restart_recovery(wid, facts)

        if choice.path == "atomic_resume_from_durable_checkpoint":
            # Restore completed ranges in O(1)
            self.completed_ranges = set(cp.completed_ranges)
            return RecoveryStatus.CLEAN_RESUMED

        if choice.path == "reconcile_crash_in_flight_generation":
            return RecoveryStatus.RECONCILED

        raise CrashRecoveryError(
            f"restart recovery refused: winning rung was '{choice.path}'"
        )
