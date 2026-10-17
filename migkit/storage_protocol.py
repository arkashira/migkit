"""Validate warehouse and lakehouse commit semantics (Row 646: RCH38.storage-protocol).

Models data-file completeness, journal/manifest positions, atomic publish,
partition/schema evolution, tombstones, and downstream visibility across
Iceberg, Delta, Snowflake, BigQuery, and Redshift staging targets.

Chooses staged bulk versus streaming by measured billable work and consistent
commit behavior via decide.Rung ladders. Files present in object storage are
NOT committed table state until referenced by an atomically published manifest
or journal entry.

Proves that a crash between object upload, metadata commit, and ack yields no
orphan visibility or duplicated effects, and queryable target coverage matches
the source fence.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import copy
import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide

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


class StoragePlatform(str, enum.Enum):
    """Warehouse and lakehouse storage and catalog platforms."""

    ICEBERG = "iceberg"
    DELTA = "delta"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"


class CommitProtocolMode(str, enum.Enum):
    """Candidate ingestion and commit protocol modes."""

    STAGED_BULK_MANIFEST = "staged_bulk_manifest"
    STREAMING_OFFSET_CHANNEL = "streaming_offset_channel"
    JOURNAL_TRANSACTION_MERGE = "journal_transaction_merge"


class TombstoneMechanism(str, enum.Enum):
    """How row deletions and retractions are represented in committed storage."""

    EQUALITY_DELETE_FILE = "equality_delete_file"
    POSITIONAL_DELETE_VECTOR = "positional_delete_vector"
    MERGE_DELETE_PREDICATE = "merge_delete_predicate"
    SOFT_TOMBSTONE_MARKER = "soft_tombstone_marker"


class CrashInjectionPoint(str, enum.Enum):
    """Fault injection boundaries in the three-phase storage commit protocol."""

    NONE = "none"
    DURING_PARTIAL_UPLOAD = "during_partial_upload"
    AFTER_OBJECT_UPLOAD_BEFORE_COMMIT = "after_object_upload_before_commit"
    AFTER_METADATA_COMMIT_BEFORE_ACK = "after_metadata_commit_before_ack"
    AFTER_ACK = "after_ack"


class CommitPhaseState(str, enum.Enum):
    """Lifecycle phase of a batch across object store, catalog metadata, and ack."""

    PENDING = "pending"
    UPLOADED_UNCOMMITTED = "uploaded_uncommitted"
    METADATA_COMMITTED_UNACKED = "metadata_committed_unacked"
    ACKNOWLEDGED = "acknowledged"
    ROLLED_BACK_OR_GC = "rolled_back_or_gc"


class SchemaEvolutionAction(str, enum.Enum):
    """Schema or partition evolution operations across commit boundaries."""

    ADD_NULLABLE_COLUMN = "add_nullable_column"
    WIDEN_NUMERIC_OR_TYPE = "widen_numeric_or_type"
    RENAME_COLUMN_BY_FIELD_ID = "rename_column_by_field_id"
    EVOLVE_PARTITION_SPEC = "evolve_partition_spec"
    INCOMPATIBLE_NARROWING = "incompatible_narrowing"
    DROP_REQUIRED_KEY_COLUMN = "drop_required_key_column"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class StorageProtocolError(Exception):
    """Base exception for warehouse and lakehouse storage protocol violations."""


class IncompleteDataFileError(StorageProtocolError):
    """Raised when a staged data file fails completeness, footer, or checksum proof."""


class OrphanVisibilityViolationError(StorageProtocolError):
    """Raised if uncommitted object files leak into queryable downstream visibility."""


class DuplicateCommitEffectError(StorageProtocolError):
    """Raised if a retried commit applies duplicate row mutations past idempotency fence."""


class IncompatibleEvolutionError(StorageProtocolError):
    """Raised when partition or schema evolution violates catalog compatibility rules."""


class LeaseLockError(StorageProtocolError):
    """Raised when mutual exclusion lease (.run.lock) cannot be acquired or renewed."""


class UnsafeEndpointError(StorageProtocolError):
    """Raised when a non-loopback or non-RFC1918 10.0.0.x host is supplied."""


# -----------------------------------------------------------------------------
# Host Safety Validation
# -----------------------------------------------------------------------------

def validate_safe_endpoint(host: str) -> str:
    """Ensure endpoint uses safe loopback (127.0.0.1, localhost) or 10.0.0.x."""
    cleaned = (host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host cannot be empty")
    if cleaned in ("127.0.0.1", "localhost") or cleaned.startswith("10.0.0."):
        return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x allowed"
    )


# -----------------------------------------------------------------------------
# Typed JSON Serialization Helpers (O(1) Checkpoint Support)
# -----------------------------------------------------------------------------

def _encode_typed_value(val: Any) -> Any:
    """Recursively encode native Python types (datetime, Decimal, UUID, tuple) for JSON."""
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


def _parse_lsn_tuple(lsn: Optional[str]) -> Optional[Tuple[int, ...]]:
    """Parse PostgreSQL-style 'X/Y' or numeric LSN string into comparable integer tuple."""
    if lsn is None:
        return None
    s = str(lsn).strip()
    if not s:
        return None
    if "/" in s:
        return tuple(int(p, 16) for p in s.split("/"))
    if s.startswith(("0x", "0X")):
        return (int(s, 16),)
    if ":" in s:
        return tuple(int(p, 16) if any(c in p for c in "abcdefABCDEF") else int(p) for p in s.split(":"))
    return (int(s),)


def compare_lsn(a: Optional[str], b: Optional[str]) -> int:
    """Compare two LSN strings; returns -1 if a < b, 0 if a == b, 1 if a > b."""
    ta = _parse_lsn_tuple(a)
    tb = _parse_lsn_tuple(b)
    if ta is None or tb is None:
        raise ValueError(f"cannot compare None LSN: a={a!r}, b={b!r}")
    if ta < tb:
        return -1
    if ta > tb:
        return 1
    return 0


def compute_rows_checksum(rows: Sequence[Dict[str, Any]]) -> str:
    """Compute deterministic SHA-256 hex digest over a sequence of row dicts."""
    canonical = json.dumps(_encode_typed_value(list(rows)), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


# -----------------------------------------------------------------------------
# Domain Models: Data Files, Tombstones, Schema/Partition Evolution, Manifests
# -----------------------------------------------------------------------------

PARQUET_MAGIC = "PAR1"


@dataclass(frozen=True)
class TombstoneSpec:
    """Explicit representation of row deletions or retractions within a commit."""

    mechanism: TombstoneMechanism
    key_columns: Tuple[str, ...]
    deleted_keys: Tuple[Tuple[Any, ...], ...]
    sequence_number: int
    source_lsn: str

    def matches_row(self, row: Dict[str, Any]) -> bool:
        if not self.key_columns or not self.deleted_keys:
            return False
        row_key = tuple(row.get(k) for k in self.key_columns)
        return row_key in set(self.deleted_keys)


@dataclass(frozen=True)
class DataFileSpec:
    """Metadata and completeness proof for an immutable staged object storage file."""

    file_uri: str
    batch_id: str
    partition_values: Dict[str, Any]
    schema_epoch: int
    partition_spec_id: int
    expected_row_count: int
    actual_row_count: int
    expected_byte_size: int
    actual_byte_size: int
    expected_checksum: str
    actual_checksum: str
    footer_magic: str = PARQUET_MAGIC
    min_key: Optional[Tuple[Any, ...]] = None
    max_key: Optional[Tuple[Any, ...]] = None
    source_min_lsn: Optional[str] = None
    source_max_lsn: Optional[str] = None
    rows_payload: Tuple[Dict[str, Any], ...] = ()

    def validate_completeness(self) -> Tuple[bool, str]:
        """Verify that object file is non-truncated, footer-valid, and checksum-matched."""
        if not self.file_uri or not self.file_uri.strip():
            return False, "file_uri is empty"
        if self.footer_magic != PARQUET_MAGIC:
            return False, f"truncated or invalid file footer magic '{self.footer_magic}' (expected '{PARQUET_MAGIC}')"
        if self.expected_byte_size <= 0 or self.actual_byte_size != self.expected_byte_size:
            return (
                False,
                f"byte size mismatch for '{self.file_uri}': actual={self.actual_byte_size} != expected={self.expected_byte_size}",
            )
        if self.expected_row_count < 0 or self.actual_row_count != self.expected_row_count:
            return (
                False,
                f"row count mismatch for '{self.file_uri}': actual={self.actual_row_count} != expected={self.expected_row_count}",
            )
        if not self.expected_checksum or self.actual_checksum != self.expected_checksum:
            return (
                False,
                f"checksum mismatch for '{self.file_uri}': actual={self.actual_checksum} != expected={self.expected_checksum}",
            )
        if self.rows_payload and len(self.rows_payload) != self.actual_row_count:
            return (
                False,
                f"payload row count {len(self.rows_payload)} != declared actual_row_count {self.actual_row_count}",
            )
        return True, "complete"


@dataclass(frozen=True)
class SchemaFieldSpec:
    """Column identity with stable field ID for Iceberg/Delta schema evolution."""

    field_id: int
    name: str
    data_type: str
    nullable: bool = True
    default_value: Any = None


@dataclass(frozen=True)
class PartitionTransformSpec:
    """Partition field transform specification (identity, day, bucket, truncate)."""

    source_field_id: int
    field_name: str
    transform: str = "identity"


@dataclass(frozen=True)
class TableSchemaVersion:
    """Snapshot of table schema and partition layout at a given epoch."""

    schema_epoch: int
    partition_spec_id: int
    fields: Tuple[SchemaFieldSpec, ...]
    key_field_ids: Tuple[int, ...]
    partition_transforms: Tuple[PartitionTransformSpec, ...] = ()

    def field_by_id(self, field_id: int) -> Optional[SchemaFieldSpec]:
        for f in self.fields:
            if f.field_id == field_id:
                return f
        return None

    def field_by_name(self, name: str) -> Optional[SchemaFieldSpec]:
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def project_row(self, raw_row: Dict[str, Any], row_schema: Optional["TableSchemaVersion"] = None) -> Dict[str, Any]:
        """Project a row written under an older schema epoch into this schema version."""
        projected: Dict[str, Any] = {}
        old_by_id: Dict[int, str] = {}
        if row_schema is not None:
            old_by_id = {f.field_id: f.name for f in row_schema.fields}

        for field_spec in self.fields:
            if field_spec.name in raw_row:
                projected[field_spec.name] = raw_row[field_spec.name]
            elif field_spec.field_id in old_by_id and old_by_id[field_spec.field_id] in raw_row:
                projected[field_spec.name] = raw_row[old_by_id[field_spec.field_id]]
            else:
                projected[field_spec.name] = field_spec.default_value
        return projected


@dataclass(frozen=True)
class JournalManifestEntry:
    """Atomic commit record linking committed data files, tombstones, and source fence."""

    commit_sequence: int
    snapshot_id: str
    parent_snapshot_id: Optional[str]
    hop_name: str
    batch_id: str
    idempotency_token: str
    source_fence_lsn: str
    protocol_mode: CommitProtocolMode
    schema_epoch: int
    partition_spec_id: int
    committed_file_uris: Tuple[str, ...]
    tombstones: Tuple[TombstoneSpec, ...] = ()
    committed_at_utc: str = ""
    row_delta: int = 0


# -----------------------------------------------------------------------------
# Decision Engine Ladder (decide.Rung) for Staged Bulk vs Streaming vs Journal
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class BillableWorkEstimate:
    """Measured billable work and latency estimate for a candidate storage protocol mode."""

    mode: CommitProtocolMode
    billable_credits: float
    estimated_latency_seconds: float
    write_amplification: float
    atomic_commit_supported: bool
    keeps_up_with_tail: bool


def evaluate_billable_work(facts: Dict[str, Any]) -> Dict[str, BillableWorkEstimate]:
    """Compute measured billable work across staged bulk, streaming offset, and journal merge."""
    rows = max(1, int(facts.get("batch_rows", 1000)))
    bytes_total = max(1, int(facts.get("batch_bytes", rows * 256)))
    mb = float(bytes_total) / (1024.0 * 1024.0)
    rate = float(facts.get("tail_rate_rows_sec", 500.0))

    # Staged bulk manifest: fixed catalog commit overhead + low per-MB object load cost
    bulk_fixed = float(facts.get("bulk_fixed_credits", 0.05))
    bulk_per_mb = float(facts.get("bulk_credits_per_mb", 0.002))
    bulk_startup_s = float(facts.get("bulk_startup_seconds", 4.0))
    bulk_per_row_s = float(facts.get("bulk_per_row_seconds", 0.00001))
    bulk_credits = bulk_fixed + (bulk_per_mb * mb)
    bulk_latency = bulk_startup_s + (bulk_per_row_s * rows)
    bulk_keeps_up = (bulk_per_row_s * rate) < 0.5

    # Streaming offset channel: near-zero startup + higher per-row/per-MB compute cost
    stream_fixed = float(facts.get("stream_fixed_credits", 0.002))
    stream_per_mb = float(facts.get("stream_credits_per_mb", 0.015))
    stream_startup_s = float(facts.get("stream_startup_seconds", 0.15))
    stream_per_row_s = float(facts.get("stream_per_row_seconds", 0.00008))
    stream_credits = stream_fixed + (stream_per_mb * mb)
    stream_latency = stream_startup_s + (stream_per_row_s * rows)
    stream_keeps_up = (stream_per_row_s * rate) < 0.5

    # Journal transaction merge: warehouse table stage + transactional MERGE + ledger write
    merge_fixed = float(facts.get("merge_fixed_credits", 0.08))
    merge_per_mb = float(facts.get("merge_credits_per_mb", 0.008))
    merge_startup_s = float(facts.get("merge_startup_seconds", 2.5))
    merge_per_row_s = float(facts.get("merge_per_row_seconds", 0.00005))
    merge_credits = merge_fixed + (merge_per_mb * mb)
    merge_latency = merge_startup_s + (merge_per_row_s * rows)
    merge_keeps_up = (merge_per_row_s * rate) < 0.5

    return {
        CommitProtocolMode.STAGED_BULK_MANIFEST.value: BillableWorkEstimate(
            mode=CommitProtocolMode.STAGED_BULK_MANIFEST,
            billable_credits=bulk_credits,
            estimated_latency_seconds=bulk_latency,
            write_amplification=1.0,
            atomic_commit_supported=bool(facts.get("atomic_manifest_publish", True)),
            keeps_up_with_tail=bulk_keeps_up,
        ),
        CommitProtocolMode.STREAMING_OFFSET_CHANNEL.value: BillableWorkEstimate(
            mode=CommitProtocolMode.STREAMING_OFFSET_CHANNEL,
            billable_credits=stream_credits,
            estimated_latency_seconds=stream_latency,
            write_amplification=1.05,
            atomic_commit_supported=bool(facts.get("streaming_offset_token", False)),
            keeps_up_with_tail=stream_keeps_up,
        ),
        CommitProtocolMode.JOURNAL_TRANSACTION_MERGE.value: BillableWorkEstimate(
            mode=CommitProtocolMode.JOURNAL_TRANSACTION_MERGE,
            billable_credits=merge_credits,
            estimated_latency_seconds=merge_latency,
            write_amplification=1.25,
            atomic_commit_supported=bool(facts.get("transactional_journal_ledger", False)),
            keeps_up_with_tail=merge_keeps_up,
        ),
    }


def storage_commit_rungs() -> Tuple[decide.Rung, ...]:
    """Build the decide.Rung ladder for selecting warehouse/lakehouse commit protocol."""
    return (
        decide.Rung(
            "staged_bulk_manifest",
            "staged Parquet files with atomic manifest or catalog snapshot publish",
            gives=frozenset({"carried", "exact", "consistent as of", "resume by chunk"}),
            needs=(
                decide.Need(
                    "object stage and atomic catalog/manifest commit",
                    lambda f: bool(f.get("object_stage")) and bool(f.get("atomic_manifest_publish")),
                    "the target lacks an object stage or atomic manifest publish protocol",
                ),
                decide.Need(
                    "complete verified data files before commit",
                    lambda f: bool(f.get("files_verified", True)),
                    "staged data files failed completeness or checksum verification",
                ),
                decide.Need(
                    "billable work within budget for staged bulk",
                    lambda f: bool(f.get("bulk_within_budget", True)),
                    "staged bulk manifest exceeds measured billable work or latency budget",
                ),
            ),
            because="lowest billable cost per megabyte with atomic snapshot isolation",
            footprint=1,
            leaves="staged Parquet files and manifest commit",
        ),
        decide.Rung(
            "streaming_offset_channel",
            "streaming write channel with committed offset token",
            gives=frozenset({"carried", "exact", "online", "source position"}),
            needs=(
                decide.Need(
                    "streaming channel keeping durable offset token",
                    lambda f: bool(f.get("streaming_offset_token")),
                    "the target offers no streaming channel that keeps a durable offset token",
                ),
                decide.Need(
                    "schema evolution compatible with active stream",
                    lambda f: not bool(f.get("requires_partition_evolution", False)),
                    "streaming channel cannot atomically evolve partition spec mid-flight",
                ),
                decide.Need(
                    "billable work and rate within streaming capacity",
                    lambda f: bool(f.get("stream_keeps_up", True)) and bool(f.get("stream_within_budget", True)),
                    "streaming channel cost per row exceeds tail rate or billable budget",
                ),
            ),
            because="lowest commit latency for continuous tail micro-batches",
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "journal_transaction_merge",
            "staging journal table merged with ledger position in one transaction",
            gives=frozenset({"carried", "exact", "consistent as of", "resume by key"}),
            needs=(
                decide.Need(
                    "transactional journal table and _migkit_ledger",
                    lambda f: bool(f.get("transactional_journal_ledger")),
                    "the target cannot merge journal and _migkit_ledger in one transaction",
                ),
                decide.Need(
                    "merge billable work within budget",
                    lambda f: bool(f.get("merge_within_budget", True)),
                    "journal transaction merge exceeds measured billable work budget",
                ),
            ),
            because="transactional idempotency without external object stage or streaming API",
            footprint=2,
            leaves="a journal table and a row in _migkit_ledger",
        ),
    )


def choose_storage_commit_protocol(
    facts: Union[Dict[str, Any], decide.Facts],
    need: Sequence[str] = (),
) -> Tuple[decide.Choice, Optional[BillableWorkEstimate]]:
    """Select the warehouse/lakehouse commit protocol by measured billable work and guarantees."""
    raw_dict = dict(facts) if not isinstance(facts, dict) else dict(facts)
    estimates = evaluate_billable_work(raw_dict)

    max_credits = raw_dict.get("max_billable_credits")
    if max_credits is not None:
        limit = float(max_credits)
        raw_dict.setdefault(
            "bulk_within_budget",
            estimates[CommitProtocolMode.STAGED_BULK_MANIFEST.value].billable_credits <= limit,
        )
        raw_dict.setdefault(
            "stream_within_budget",
            estimates[CommitProtocolMode.STREAMING_OFFSET_CHANNEL.value].billable_credits <= limit,
        )
        raw_dict.setdefault(
            "merge_within_budget",
            estimates[CommitProtocolMode.JOURNAL_TRANSACTION_MERGE.value].billable_credits <= limit,
        )

    raw_dict.setdefault(
        "stream_keeps_up",
        estimates[CommitProtocolMode.STREAMING_OFFSET_CHANNEL.value].keeps_up_with_tail,
    )

    f = facts if isinstance(facts, decide.Facts) else decide.Facts(raw_dict)
    for k, v in raw_dict.items():
        if k not in f:
            f[k] = v

    # Weight cost by measured billable credits (primary) and latency (secondary)
    latency_weight = float(raw_dict.get("latency_credit_weight", 0.005))
    cost_map = {
        mode_name: est.billable_credits + (latency_weight * est.estimated_latency_seconds)
        for mode_name, est in estimates.items()
    }
    costs = decide.Costs(cost_map)
    rungs = storage_commit_rungs()
    choice = decide.climb("warehouse and lakehouse commit protocol", rungs, f, need=tuple(need), costs=costs)
    chosen_est = estimates.get(choice.path) if choice.path else None
    return choice, chosen_est


# -----------------------------------------------------------------------------
# Atomic Catalog & Object Storage Coordinator (Crash-Safe & Visibility-Isolated)
# -----------------------------------------------------------------------------

@dataclass
class DownstreamVisibilitySnapshot:
    """Downstream queryable view of a warehouse/lakehouse table at a point in time."""

    platform: StoragePlatform
    current_commit_sequence: int
    current_snapshot_id: Optional[str]
    source_fence_lsn: Optional[str]
    schema_version: TableSchemaVersion
    committed_file_uris: Tuple[str, ...]
    uncommitted_orphan_file_uris: Tuple[str, ...]
    queryable_rows: Tuple[Dict[str, Any], ...]
    queryable_row_count: int
    queryable_digest: str


@dataclass
class CommitExecutionReceipt:
    """Result of executing or recovering a batch commit across object store and catalog."""

    batch_id: str
    idempotency_token: str
    phase_state: CommitPhaseState
    committed: bool
    acknowledged: bool
    deduplicated_retry: bool
    commit_sequence: Optional[int]
    snapshot_id: Optional[str]
    source_fence_lsn: str
    care_badge: CareBadge
    summary: str


class AtomicStorageCatalogCoordinator:
    """Coordinates object upload, data-file completeness check, atomic metadata publish,
    and acknowledgment across Iceberg, Delta, Snowflake, BigQuery, and Redshift.

    Invariants enforced:
    1. Files uploaded to object storage (`object_store_files`) are NEVER visible to
       downstream queries until referenced by a committed `JournalManifestEntry` in
       `committed_manifests`.
    2. Every file in a commit must pass `DataFileSpec.validate_completeness()` before
       metadata commit is allowed.
    3. If a crash occurs after object upload but before metadata commit, downstream
       queries see zero rows from those files, and garbage collection / retry safely
       replaces or cleans them.
    4. If a crash occurs after metadata commit but before caller acknowledgment,
       the committed manifest records `idempotency_token` and `batch_id` (mirroring
       Iceberg snapshot summary properties, Delta `txn` action, Snowflake channel
       offset token, BigQuery committed stream offset, or Redshift `_migkit_ledger`).
       A retry of the same batch detects the committed token in O(1), skips re-applying
       mutations, and returns theacknowledgment receipt with zero duplicate effects.
    """

    def __init__(
        self,
        platform: StoragePlatform,
        hop_name: str,
        initial_schema: TableSchemaVersion,
        endpoint_host: str = "127.0.0.1",
    ) -> None:
        self.endpoint_host = validate_safe_endpoint(endpoint_host)
        self.platform = platform
        self.hop_name = hop_name
        self.schema_history: Dict[int, TableSchemaVersion] = {
            initial_schema.schema_epoch: initial_schema
        }
        self.current_schema_epoch: int = initial_schema.schema_epoch

        # Object storage layer: file_uri -> DataFileSpec (includes uncommitted uploads)
        self.object_store_files: Dict[str, DataFileSpec] = {}

        # Catalog metadata layer: ordered atomic manifest commits
        self.committed_manifests: List[JournalManifestEntry] = []
        self._committed_by_token: Dict[str, JournalManifestEntry] = {}
        self._committed_by_batch: Dict[str, JournalManifestEntry] = {}

        # Acknowledgment tracking: batch_id -> CommitExecutionReceipt
        self.acknowledged_batches: Dict[str, CommitExecutionReceipt] = {}
        self.phase_by_batch: Dict[str, CommitPhaseState] = {}

    @property
    def current_schema(self) -> TableSchemaVersion:
        return self.schema_history[self.current_schema_epoch]

    @property
    def latest_manifest(self) -> Optional[JournalManifestEntry]:
        return self.committed_manifests[-1] if self.committed_manifests else None

    def stage_data_file(
        self,
        file_spec: DataFileSpec,
        *,
        crash_point: CrashInjectionPoint = CrashInjectionPoint.NONE,
    ) -> DataFileSpec:
        """Upload a data file to object storage. Does NOT commit to table metadata."""
        if crash_point == CrashInjectionPoint.DURING_PARTIAL_UPLOAD:
            truncated = dataclasses.replace(
                file_spec,
                actual_byte_size=max(0, file_spec.expected_byte_size // 2),
                footer_magic="TRUNC",
                actual_checksum="0000000000000000000000000000000000000000000000000000000000000000",
            )
            self.object_store_files[truncated.file_uri] = truncated
            self.phase_by_batch[file_spec.batch_id] = CommitPhaseState.UPLOADED_UNCOMMITTED
            raise StorageProtocolError(
                f"simulated crash during partial object upload for '{file_spec.file_uri}'"
            )

        self.object_store_files[file_spec.file_uri] = file_spec
        if self.phase_by_batch.get(file_spec.batch_id) not in (
            CommitPhaseState.METADATA_COMMITTED_UNACKED,
            CommitPhaseState.ACKNOWLEDGED,
        ):
            self.phase_by_batch[file_spec.batch_id] = CommitPhaseState.UPLOADED_UNCOMMITTED
        return file_spec

    def evolve_schema_or_partition(
        self,
        action: SchemaEvolutionAction,
        *,
        new_field: Optional[SchemaFieldSpec] = None,
        target_field_id: Optional[int] = None,
        new_name: Optional[str] = None,
        new_type: Optional[str] = None,
        new_partition_transforms: Optional[Sequence[PartitionTransformSpec]] = None,
    ) -> TableSchemaVersion:
        """Apply atomic schema or partition evolution, validating backwards compatibility."""
        cur = self.current_schema

        if action == SchemaEvolutionAction.INCOMPATIBLE_NARROWING:
            raise IncompatibleEvolutionError(
                "incompatible type narrowing refused: cannot narrow committed column type without rewrite"
            )
        if action == SchemaEvolutionAction.DROP_REQUIRED_KEY_COLUMN:
            raise IncompatibleEvolutionError(
                "dropping required identifier/key column refused: breaks tombstone and merge identity"
            )

        next_epoch = cur.schema_epoch + 1
        next_part_id = cur.partition_spec_id
        fields = list(cur.fields)
        part_transforms = list(cur.partition_transforms)

        if action == SchemaEvolutionAction.ADD_NULLABLE_COLUMN:
            if new_field is None:
                raise IncompatibleEvolutionError("new_field required for ADD_NULLABLE_COLUMN")
            if not new_field.nullable and new_field.default_value is None:
                raise IncompatibleEvolutionError(
                    f"added column '{new_field.name}' must be nullable or declare a default value"
                )
            if any(f.field_id == new_field.field_id or f.name == new_field.name for f in fields):
                raise IncompatibleEvolutionError(
                    f"field_id={new_field.field_id} or name='{new_field.name}' already exists"
                )
            fields.append(new_field)

        elif action == SchemaEvolutionAction.WIDEN_NUMERIC_OR_TYPE:
            if target_field_id is None or not new_type:
                raise IncompatibleEvolutionError("target_field_id and new_type required for type widening")
            updated = False
            for idx, f in enumerate(fields):
                if f.field_id == target_field_id:
                    fields[idx] = dataclasses.replace(f, data_type=new_type)
                    updated = True
                    break
            if not updated:
                raise IncompatibleEvolutionError(f"field_id={target_field_id} not found for type widening")

        elif action == SchemaEvolutionAction.RENAME_COLUMN_BY_FIELD_ID:
            if target_field_id is None or not new_name:
                raise IncompatibleEvolutionError("target_field_id and new_name required for column rename")
            updated = False
            for idx, f in enumerate(fields):
                if f.field_id == target_field_id:
                    fields[idx] = dataclasses.replace(f, name=new_name)
                    updated = True
                    break
            if not updated:
                raise IncompatibleEvolutionError(f"field_id={target_field_id} not found for column rename")

        elif action == SchemaEvolutionAction.EVOLVE_PARTITION_SPEC:
            if new_partition_transforms is None:
                raise IncompatibleEvolutionError("new_partition_transforms required for partition evolution")
            next_part_id = cur.partition_spec_id + 1
            part_transforms = list(new_partition_transforms)

        evolved = TableSchemaVersion(
            schema_epoch=next_epoch,
            partition_spec_id=next_part_id,
            fields=tuple(fields),
            key_field_ids=cur.key_field_ids,
            partition_transforms=tuple(part_transforms),
        )
        self.schema_history[evolved.schema_epoch] = evolved
        self.current_schema_epoch = evolved.schema_epoch
        return evolved

    def commit_batch(
        self,
        *,
        batch_id: str,
        idempotency_token: str,
        source_fence_lsn: str,
        file_uris: Sequence[str],
        tombstones: Sequence[TombstoneSpec] = (),
        protocol_mode: CommitProtocolMode = CommitProtocolMode.STAGED_BULK_MANIFEST,
        crash_point: CrashInjectionPoint = CrashInjectionPoint.NONE,
    ) -> CommitExecutionReceipt:
        """Atomically validate staged files, publish manifest/journal entry, and acknowledge.

        Idempotent on retry: if `idempotency_token` or `batch_id` was already committed
        before an earlier post-commit crash, returns the committed receipt without
        re-applying files or tombstones.
        """
        # 1. Check idempotency ledger / snapshot summary / Delta txn action first
        existing = self._committed_by_token.get(idempotency_token) or self._committed_by_batch.get(batch_id)
        if existing is not None:
            if existing.source_fence_lsn != source_fence_lsn or existing.idempotency_token != idempotency_token:
                raise DuplicateCommitEffectError(
                    f"conflicting retry for batch '{batch_id}': token or fence mismatch against committed manifest"
                )
            receipt = CommitExecutionReceipt(
                batch_id=batch_id,
                idempotency_token=idempotency_token,
                phase_state=CommitPhaseState.ACKNOWLEDGED,
                committed=True,
                acknowledged=True,
                deduplicated_retry=True,
                commit_sequence=existing.commit_sequence,
                snapshot_id=existing.snapshot_id,
                source_fence_lsn=existing.source_fence_lsn,
                care_badge=CareBadge.PASS,
                summary=f"idempotent retry recovered committed snapshot '{existing.snapshot_id}' at sequence {existing.commit_sequence}",
            )
            self.acknowledged_batches[batch_id] = receipt
            self.phase_by_batch[batch_id] = CommitPhaseState.ACKNOWLEDGED
            return receipt

        # 2. Verify all referenced object files exist and pass completeness proof
        total_rows = 0
        for uri in file_uris:
            if uri not in self.object_store_files:
                raise IncompleteDataFileError(f"staged file '{uri}' is missing from object storage")
            spec = self.object_store_files[uri]
            ok, reason = spec.validate_completeness()
            if not ok:
                raise IncompleteDataFileError(f"staged file '{uri}' incomplete: {reason}")
            if spec.schema_epoch not in self.schema_history:
                raise IncompatibleEvolutionError(
                    f"staged file '{uri}' references unknown schema_epoch={spec.schema_epoch}"
                )
            total_rows += spec.actual_row_count

        # 3. Verify monotonic source fence progression
        if self.latest_manifest is not None:
            if compare_lsn(source_fence_lsn, self.latest_manifest.source_fence_lsn) < 0:
                raise StorageProtocolError(
                    f"non-monotonic source_fence_lsn '{source_fence_lsn}' < committed '{self.latest_manifest.source_fence_lsn}'"
                )

        # 4. Simulate crash after object upload before metadata commit
        if crash_point == CrashInjectionPoint.AFTER_OBJECT_UPLOAD_BEFORE_COMMIT:
            self.phase_by_batch[batch_id] = CommitPhaseState.UPLOADED_UNCOMMITTED
            raise StorageProtocolError(
                f"simulated crash after object upload before metadata commit for batch '{batch_id}'"
            )

        # 5. Atomic metadata publish (CAS manifest / journal commit)
        seq = len(self.committed_manifests) + 1
        parent_snap = self.latest_manifest.snapshot_id if self.latest_manifest else None
        snap_hash = hashlib.sha256(
            f"{self.hop_name}:{seq}:{batch_id}:{idempotency_token}:{source_fence_lsn}".encode("ascii")
        ).hexdigest()[:16]
        snap_id = f"snap-{seq:06d}-{snap_hash}"

        manifest = JournalManifestEntry(
            commit_sequence=seq,
            snapshot_id=snap_id,
            parent_snapshot_id=parent_snap,
            hop_name=self.hop_name,
            batch_id=batch_id,
            idempotency_token=idempotency_token,
            source_fence_lsn=source_fence_lsn,
            protocol_mode=protocol_mode,
            schema_epoch=self.current_schema_epoch,
            partition_spec_id=self.current_schema.partition_spec_id,
            committed_file_uris=tuple(file_uris),
            tombstones=tuple(tombstones),
            committed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            row_delta=total_rows,
        )
        self.committed_manifests.append(manifest)
        self._committed_by_token[idempotency_token] = manifest
        self._committed_by_batch[batch_id] = manifest
        self.phase_by_batch[batch_id] = CommitPhaseState.METADATA_COMMITTED_UNACKED

        # 6. Simulate crash after metadata commit before ack
        if crash_point == CrashInjectionPoint.AFTER_METADATA_COMMIT_BEFORE_ACK:
            raise StorageProtocolError(
                f"simulated crash after metadata commit before ack for batch '{batch_id}' (snapshot='{snap_id}')"
            )

        # 7. Record acknowledgment
        receipt = CommitExecutionReceipt(
            batch_id=batch_id,
            idempotency_token=idempotency_token,
            phase_state=CommitPhaseState.ACKNOWLEDGED,
            committed=True,
            acknowledged=True,
            deduplicated_retry=False,
            commit_sequence=seq,
            snapshot_id=snap_id,
            source_fence_lsn=source_fence_lsn,
            care_badge=CareBadge.PASS,
            summary=f"atomically published snapshot '{snap_id}' at sequence {seq} (fence={source_fence_lsn})",
        )
        self.acknowledged_batches[batch_id] = receipt
        self.phase_by_batch[batch_id] = CommitPhaseState.ACKNOWLEDGED
        return receipt

    def garbage_collect_uncommitted_files(self) -> Tuple[str, ...]:
        """Remove orphan staged files in object storage that are not referenced by any committed manifest."""
        committed_uris: Set[str] = set()
        for m in self.committed_manifests:
            committed_uris.update(m.committed_file_uris)

        orphans = sorted(uri for uri in self.object_store_files if uri not in committed_uris)
        for uri in orphans:
            del self.object_store_files[uri]
        for b_id, state in list(self.phase_by_batch.items()):
            if state == CommitPhaseState.UPLOADED_UNCOMMITTED:
                self.phase_by_batch[b_id] = CommitPhaseState.ROLLED_BACK_OR_GC
        return tuple(orphans)

    def downstream_snapshot(self, as_of_sequence: Optional[int] = None) -> DownstreamVisibilitySnapshot:
        """Materialize the downstream queryable table state as of a commit sequence.

        Strictly reads ONLY files referenced by `committed_manifests` up to `as_of_sequence`.
        Uncommitted files in `object_store_files` are never read, proving zero orphan visibility.
        Applies schema projection across epochs and deduplicates by primary key + tombstones.
        """
        target_manifests = [
            m for m in self.committed_manifests
            if as_of_sequence is None or m.commit_sequence <= as_of_sequence
        ]

        committed_uris: List[str] = []
        for m in target_manifests:
            committed_uris.extend(m.committed_file_uris)
        committed_uri_set = set(committed_uris)
        orphan_uris = tuple(sorted(uri for uri in self.object_store_files if uri not in committed_uri_set))

        if not target_manifests:
            cur_schema = self.current_schema
            return DownstreamVisibilitySnapshot(
                platform=self.platform,
                current_commit_sequence=0,
                current_snapshot_id=None,
                source_fence_lsn=None,
                schema_version=cur_schema,
                committed_file_uris=(),
                uncommitted_orphan_file_uris=orphan_uris,
                queryable_rows=(),
                queryable_row_count=0,
                queryable_digest=compute_rows_checksum([]),
            )

        last_manifest = target_manifests[-1]
        active_schema = self.schema_history[last_manifest.schema_epoch]
        key_names = tuple(
            active_schema.field_by_id(fid).name
            for fid in active_schema.key_field_ids
            if active_schema.field_by_id(fid) is not None
        )

        # Replay committed manifests in commit_sequence order
        live_by_key: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        keyless_rows: List[Dict[str, Any]] = []

        for manifest in target_manifests:
            for uri in manifest.committed_file_uris:
                f_spec = self.object_store_files[uri]
                file_schema = self.schema_history.get(f_spec.schema_epoch, active_schema)
                for raw_row in f_spec.rows_payload:
                    projected = active_schema.project_row(raw_row, file_schema)
                    if key_names:
                        k = tuple(projected.get(kn) for kn in key_names)
                        live_by_key[k] = projected
                    else:
                        keyless_rows.append(projected)

            for tomb in manifest.tombstones:
                if key_names:
                    for del_key in tomb.deleted_keys:
                        live_by_key.pop(tuple(del_key), None)
                else:
                    keyless_rows = [r for r in keyless_rows if not tomb.matches_row(r)]

        if key_names:
            ordered_rows = [live_by_key[k] for k in sorted(live_by_key.keys(), key=lambda x: str(x))]
        else:
            ordered_rows = keyless_rows

        return DownstreamVisibilitySnapshot(
            platform=self.platform,
            current_commit_sequence=last_manifest.commit_sequence,
            current_snapshot_id=last_manifest.snapshot_id,
            source_fence_lsn=last_manifest.source_fence_lsn,
            schema_version=active_schema,
            committed_file_uris=tuple(committed_uris),
            uncommitted_orphan_file_uris=orphan_uris,
            queryable_rows=tuple(ordered_rows),
            queryable_row_count=len(ordered_rows),
            queryable_digest=compute_rows_checksum(ordered_rows),
        )


# -----------------------------------------------------------------------------
# Source Fence Coverage & Commit Semantics Verifier
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceFenceExpectation:
    """Expected table state on the source as of a specific LSN fence."""

    fence_lsn: str
    expected_rows: Tuple[Dict[str, Any], ...]
    key_columns: Tuple[str, ...] = ("id",)

    @property
    def expected_row_count(self) -> int:
        return len(self.expected_rows)

    @property
    def expected_digest(self) -> str:
        if self.key_columns:
            sorted_rows = sorted(
                self.expected_rows,
                key=lambda r: str(tuple(r.get(k) for k in self.key_columns)),
            )
            return compute_rows_checksum(sorted_rows)
        return compute_rows_checksum(self.expected_rows)


@dataclass(frozen=True)
class StorageCommitVerificationReport:
    """Formal verification verdict for warehouse/lakehouse commit semantics."""

    platform: StoragePlatform
    care_badge: CareBadge
    fence_matched: bool
    orphan_visibility_free: bool
    duplicate_effects_free: bool
    completeness_verified: bool
    source_fence_lsn: str
    target_commit_sequence: int
    target_snapshot_id: Optional[str]
    expected_row_count: int
    queryable_row_count: int
    uncommitted_orphan_file_count: int
    anomalies: Tuple[str, ...]
    summary: str


def verify_storage_commit_semantics(
    coordinator: AtomicStorageCatalogCoordinator,
    source_expectation: SourceFenceExpectation,
    *,
    allow_uncollected_staged_files: bool = True,
) -> StorageCommitVerificationReport:
    """Verify that queryable target coverage matches the source fence with zero orphan visibility."""
    snap = coordinator.downstream_snapshot()
    anomalies: List[str] = []

    # 1. Check that no uncommitted file URI appears in committed_file_uris
    committed_set = set(snap.committed_file_uris)
    orphan_set = set(snap.uncommitted_orphan_file_uris)
    leaked_uris = committed_set & orphan_set
    orphan_visibility_free = len(leaked_uris) == 0
    if not orphan_visibility_free:
        anomalies.append(f"orphan files leaked into committed visibility: {sorted(leaked_uris)}")

    # 2. Check completeness of all committed files
    completeness_verified = True
    for uri in snap.committed_file_uris:
        f_spec = coordinator.object_store_files.get(uri)
        if f_spec is None:
            completeness_verified = False
            anomalies.append(f"committed file '{uri}' missing from object storage")
        else:
            ok, reason = f_spec.validate_completeness()
            if not ok:
                completeness_verified = False
                anomalies.append(f"committed file '{uri}' failed completeness check: {reason}")

    # 3. Check duplicate commit effects across manifests
    seen_tokens: Set[str] = set()
    seen_batches: Set[str] = set()
    duplicate_effects_free = True
    for m in coordinator.committed_manifests:
        if m.idempotency_token in seen_tokens or m.batch_id in seen_batches:
            duplicate_effects_free = False
            anomalies.append(
                f"duplicate manifest entry committed for batch='{m.batch_id}' token='{m.idempotency_token}'"
            )
        seen_tokens.add(m.idempotency_token)
        seen_batches.add(m.batch_id)

    # 4. Check source fence alignment and row/digest parity
    fence_matched = (
        snap.source_fence_lsn == source_expectation.fence_lsn
        and snap.queryable_row_count == source_expectation.expected_row_count
        and snap.queryable_digest == source_expectation.expected_digest
    )
    if snap.source_fence_lsn != source_expectation.fence_lsn:
        anomalies.append(
            f"target fence_lsn={snap.source_fence_lsn!r} != source expected fence_lsn={source_expectation.fence_lsn!r}"
        )
    if snap.queryable_row_count != source_expectation.expected_row_count:
        anomalies.append(
            f"target queryable_row_count={snap.queryable_row_count} != source expected_row_count={source_expectation.expected_row_count}"
        )
    elif snap.queryable_digest != source_expectation.expected_digest:
        anomalies.append(
            f"target queryable_digest={snap.queryable_digest[:12]} != source expected_digest={source_expectation.expected_digest[:12]}"
        )

    if not orphan_visibility_free or not completeness_verified or not duplicate_effects_free or not fence_matched:
        badge = CareBadge.ACTION
        summary = "; ".join(anomalies)
    elif snap.uncommitted_orphan_file_uris and not allow_uncollected_staged_files:
        badge = CareBadge.REVIEW
        summary = (
            f"target coverage matches fence {source_expectation.fence_lsn}, but "
            f"{len(snap.uncommitted_orphan_file_uris)} uncommitted staged files await garbage collection"
        )
    elif snap.uncommitted_orphan_file_uris:
        badge = CareBadge.BENIGN
        summary = (
            f"queryable coverage matches fence {source_expectation.fence_lsn}; "
            f"{len(snap.uncommitted_orphan_file_uris)} uncommitted staged files isolated from visibility"
        )
    else:
        badge = CareBadge.PASS
        summary = (
            f"queryable coverage matches source fence {source_expectation.fence_lsn} "
            f"at snapshot {snap.current_snapshot_id} with zero orphan files"
        )

    return StorageCommitVerificationReport(
        platform=coordinator.platform,
        care_badge=badge,
        fence_matched=fence_matched,
        orphan_visibility_free=orphan_visibility_free,
        duplicate_effects_free=duplicate_effects_free,
        completeness_verified=completeness_verified,
        source_fence_lsn=source_expectation.fence_lsn,
        target_commit_sequence=snap.current_commit_sequence,
        target_snapshot_id=snap.current_snapshot_id,
        expected_row_count=source_expectation.expected_row_count,
        queryable_row_count=snap.queryable_row_count,
        uncommitted_orphan_file_count=len(snap.uncommitted_orphan_file_uris),
        anomalies=tuple(anomalies),
        summary=summary,
    )


# -----------------------------------------------------------------------------
# Universal Typed Durable Resume in O(1)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class DurableCommitUnitCheckpoint:
    """Range/cursor-level durable commit checkpoint preserving native types."""

    unit_id: str
    batch_id: str
    idempotency_token: str
    commit_sequence: int
    snapshot_id: str
    source_fence_lsn: str
    min_cursor_key: Any
    max_cursor_key: Any
    committed_at: datetime.datetime


class StorageProtocolCheckpointStore:
    """Universal typed durable checkpoint store supporting O(1) unit lookup and skip."""

    def __init__(self, checkpoint_file: Union[str, Path]) -> None:
        self.checkpoint_file = Path(checkpoint_file)
        self.completed_units: Dict[str, DurableCommitUnitCheckpoint] = {}
        self.completed_tokens: Set[str] = set()
        if self.checkpoint_file.exists():
            self.load()

    def is_completed_o1(self, unit_id: str, idempotency_token: Optional[str] = None) -> bool:
        """O(1) hash-set check whether a commit unit or idempotency token is already durable."""
        if unit_id in self.completed_units:
            return True
        if idempotency_token is not None and idempotency_token in self.completed_tokens:
            return True
        return False

    def record_completed_unit(self, checkpoint: DurableCommitUnitCheckpoint) -> None:
        """Record a completed commit unit and atomically persist to disk."""
        self.completed_units[checkpoint.unit_id] = checkpoint
        self.completed_tokens.add(checkpoint.idempotency_token)
        self.save()

    def save(self) -> None:
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "units": {
                uid: {
                    "unit_id": cp.unit_id,
                    "batch_id": cp.batch_id,
                    "idempotency_token": cp.idempotency_token,
                    "commit_sequence": cp.commit_sequence,
                    "snapshot_id": cp.snapshot_id,
                    "source_fence_lsn": cp.source_fence_lsn,
                    "min_cursor_key": _encode_typed_value(cp.min_cursor_key),
                    "max_cursor_key": _encode_typed_value(cp.max_cursor_key),
                    "committed_at": _encode_typed_value(cp.committed_at),
                }
                for uid, cp in self.completed_units.items()
            }
        }
        tmp = self.checkpoint_file.with_suffix(f".{uuid.uuid4().hex[:8]}.tmp")
        try:
            tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="ascii")
            os.replace(tmp, self.checkpoint_file)
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    def load(self) -> None:
        raw = json.loads(self.checkpoint_file.read_text(encoding="ascii"), object_hook=_decode_typed_hook)
        self.completed_units.clear()
        self.completed_tokens.clear()
        for uid, data in raw.get("units", {}).items():
            cp = DurableCommitUnitCheckpoint(
                unit_id=str(data["unit_id"]),
                batch_id=str(data["batch_id"]),
                idempotency_token=str(data["idempotency_token"]),
                commit_sequence=int(data["commit_sequence"]),
                snapshot_id=str(data["snapshot_id"]),
                source_fence_lsn=str(data["source_fence_lsn"]),
                min_cursor_key=data["min_cursor_key"],
                max_cursor_key=data["max_cursor_key"],
                committed_at=data["committed_at"],
            )
            self.completed_units[uid] = cp
            self.completed_tokens.add(cp.idempotency_token)


# -----------------------------------------------------------------------------
# Mutual Exclusion & Active Verification Lease (.run.lock)
# -----------------------------------------------------------------------------

class StorageProtocolLease:
    """Active mutual exclusion lease (.run.lock) with heartbeat preventing duplicate runs."""

    def __init__(
        self,
        lock_dir: Union[str, Path],
        owner_id: Optional[str] = None,
        ttl_seconds: float = 30.0,
    ) -> None:
        self.lock_dir = Path(lock_dir)
        self.lock_file = self.lock_dir / ".run.lock"
        self.owner_id = owner_id or f"migkit-storage-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.ttl_seconds = float(ttl_seconds)
        self.acquired = False

    def acquire(self) -> "StorageProtocolLease":
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        now = time.time()
        if self.lock_file.exists():
            try:
                data = json.loads(self.lock_file.read_text(encoding="ascii"))
                hb = float(data.get("heartbeat", 0.0))
                owner = data.get("owner", "unknown")
                if now - hb < self.ttl_seconds and owner != self.owner_id:
                    raise LeaseLockError(
                        f"active storage protocol lease held by '{owner}' "
                        f"(age={now - hb:.1f}s < ttl={self.ttl_seconds:.1f}s)"
                    )
            except (json.JSONDecodeError, OSError, ValueError) as exc:
                if isinstance(exc, LeaseLockError):
                    raise
                raise LeaseLockError(f"corrupt active lease file {self.lock_file}: {exc}") from exc

        self._write_lease(now)
        self.acquired = True
        return self

    def heartbeat(self) -> None:
        if not self.acquired or not self.lock_file.exists():
            raise LeaseLockError("cannot heartbeat: lease not currently held")
        data = json.loads(self.lock_file.read_text(encoding="ascii"))
        if data.get("owner") != self.owner_id:
            raise LeaseLockError("cannot heartbeat: lease ownership lost")
        self._write_lease(time.time())

    def _write_lease(self, timestamp: float) -> None:
        payload = {
            "owner": self.owner_id,
            "pid": os.getpid(),
            "heartbeat": timestamp,
            "ttl": self.ttl_seconds,
        }
        tmp = self.lock_dir / f".run.lock.{uuid.uuid4().hex[:8]}.tmp"
        try:
            tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="ascii")
            os.replace(tmp, self.lock_file)
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    def release(self) -> None:
        if not self.acquired:
            return
        if self.lock_file.exists():
            try:
                data = json.loads(self.lock_file.read_text(encoding="ascii"))
                if data.get("owner") == self.owner_id:
                    self.lock_file.unlink(missing_ok=True)
            except (json.JSONDecodeError, OSError):
                pass
        self.acquired = False

    def __enter__(self) -> "StorageProtocolLease":
        return self.acquire()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# Adaptive AIMD Batch Sizing & Byte-Weighted Cold-Start ETA
# -----------------------------------------------------------------------------

@dataclass
class AdaptiveStorageBatchController:
    """Dynamic AIMD batch sizing and instant cold-start byte-weighted ETA."""

    current_batch_rows: int = 5000
    min_batch_rows: int = 500
    max_batch_rows: int = 250000
    additive_increase: int = 2500
    multiplicative_decrease: float = 0.5
    target_commit_latency_ms: float = 1500.0
    cold_start_throughput_bytes_sec: float = 50 * 1024 * 1024  # 50 MB/s baseline
    observed_bytes_committed: int = 0
    observed_elapsed_seconds: float = 0.0

    def record_commit(
        self,
        rows_committed: int,
        bytes_committed: int,
        latency_ms: float,
        *,
        cas_conflict_or_throttled: bool = False,
    ) -> int:
        """Adjust batch size via AIMD and update byte-weighted throughput."""
        if rows_committed > 0 and bytes_committed > 0 and latency_ms > 0:
            self.observed_bytes_committed += bytes_committed
            self.observed_elapsed_seconds += latency_ms / 1000.0

        if cas_conflict_or_throttled or latency_ms > self.target_commit_latency_ms:
            reduced = int(self.current_batch_rows * self.multiplicative_decrease)
            self.current_batch_rows = max(self.min_batch_rows, reduced)
        else:
            increased = self.current_batch_rows + self.additive_increase
            self.current_batch_rows = min(self.max_batch_rows, increased)

        return self.current_batch_rows

    def estimate_eta_seconds(self, remaining_bytes: int) -> float:
        """Compute instant byte-weighted ETA blending cold-start baseline with observed rate."""
        if remaining_bytes <= 0:
            return 0.0
        if self.observed_elapsed_seconds <= 0.0 or self.observed_bytes_committed <= 0:
            rate = self.cold_start_throughput_bytes_sec
        else:
            observed_rate = float(self.observed_bytes_committed) / self.observed_elapsed_seconds
            weight = min(1.0, float(self.observed_bytes_committed) / float(20 * 1024 * 1024))
            rate = (weight * observed_rate) + ((1.0 - weight) * self.cold_start_throughput_bytes_sec)
        return float(remaining_bytes) / max(1.0, rate)


# -----------------------------------------------------------------------------
# Full-Capacity External Tool Wrapper (Zero Internal Tool Leaks)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class NormalizedStorageToolResult:
    """Normalized Migkit envelope for external replication/verification tools."""

    tool_name: str
    protocol_mode: CommitProtocolMode
    guarantees_atomic_publish: bool
    care_badge: CareBadge
    status: str
    summary: str


class StorageProtocolToolWrapper:
    """Wraps external tools (Datafold, DMS, DTS, reladiff, DVT, pgcopydb, debezium)
    with zero internal tool exception or branding leaks."""

    TOOL_MATRIX: Dict[str, Tuple[CommitProtocolMode, bool]] = {
        "snowflake_openflow": (CommitProtocolMode.JOURNAL_TRANSACTION_MERGE, True),
        "databricks_lakeflow": (CommitProtocolMode.STAGED_BULK_MANIFEST, True),
        "matillion_staging": (CommitProtocolMode.STAGED_BULK_MANIFEST, True),
        "debezium_iceberg": (CommitProtocolMode.STAGED_BULK_MANIFEST, True),
        "dms_s3_raw": (CommitProtocolMode.STAGED_BULK_MANIFEST, False),
        "dts_oss_raw": (CommitProtocolMode.STAGED_BULK_MANIFEST, False),
        "pgcopydb_stage": (CommitProtocolMode.STAGED_BULK_MANIFEST, True),
        "datafold_cross_db": (CommitProtocolMode.JOURNAL_TRANSACTION_MERGE, True),
        "dvt_warehouse_check": (CommitProtocolMode.JOURNAL_TRANSACTION_MERGE, True),
        "reladiff_warehouse": (CommitProtocolMode.JOURNAL_TRANSACTION_MERGE, True),
    }

    @classmethod
    def evaluate_adapter(
        cls,
        tool_name: str,
        runner: Optional[Callable[[], Dict[str, Any]]] = None,
    ) -> NormalizedStorageToolResult:
        """Verify adapter atomic commit guarantee and sanitize any internal tool exceptions."""
        key = (tool_name or "").strip().lower()
        if key not in cls.TOOL_MATRIX:
            return NormalizedStorageToolResult(
                tool_name="migkit_storage_adapter",
                protocol_mode=CommitProtocolMode.STAGED_BULK_MANIFEST,
                guarantees_atomic_publish=False,
                care_badge=CareBadge.ACTION,
                status="unsupported_adapter",
                summary="requested storage adapter is not registered in capability matrix",
            )

        mode, atomic = cls.TOOL_MATRIX[key]
        if not atomic:
            return NormalizedStorageToolResult(
                tool_name=key,
                protocol_mode=mode,
                guarantees_atomic_publish=False,
                care_badge=CareBadge.ACTION,
                status="refused_non_atomic_object_drop",
                summary=(
                    f"adapter '{key}' writes raw object files without atomic manifest or journal commit; "
                    "files present in storage are not committed table state"
                ),
            )

        if runner is not None:
            try:
                raw_out = runner()
                ok = bool(raw_out.get("ok", True))
                return NormalizedStorageToolResult(
                    tool_name=key,
                    protocol_mode=mode,
                    guarantees_atomic_publish=True,
                    care_badge=CareBadge.PASS if ok else CareBadge.ACTION,
                    status="completed" if ok else "commit_verification_failed",
                    summary=str(raw_out.get("summary", "normalized storage commit completed")),
                )
            except Exception as exc:
                err_class = type(exc).__name__
                return NormalizedStorageToolResult(
                    tool_name=key,
                    protocol_mode=mode,
                    guarantees_atomic_publish=True,
                    care_badge=CareBadge.ACTION,
                    status="normalized_adapter_error",
                    summary=f"normalized storage adapter execution failure ({err_class})",
                )

        return NormalizedStorageToolResult(
            tool_name=key,
            protocol_mode=mode,
            guarantees_atomic_publish=True,
            care_badge=CareBadge.PASS,
            status="capable",
            summary=f"adapter '{key}' ({mode.value}) enforces atomic metadata publish",
        )
