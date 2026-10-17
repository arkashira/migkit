"""Move event consumers and schemas with stream bytes (Row 635: RCH27.stream-metadata).

Extends streaming and Kafka migration proof beyond raw topic payload bytes to:
1. Topic configurations and partition identity (partition count, key-hash invariant,
   cleanup policy, min.insync.replicas, retention bounds).
2. Retained offset ranges and watermarks per partition.
3. Explicit capability distinction between offset-preserving paths (e.g. Cluster
   Linking mirror topics) and offset-remapped paths (e.g. MirrorMaker 2 / re-produced
   streams), rejecting direct offset copy when offsets are not preserved.
4. Consumer group offset migration with strict retention-bound validation, refusing
   offsets that fall below target log_start_offset or above target high_watermark.
5. Coordinated promotion with producer/consumer fences and mirror lag drain so that
   consumer restart after promotion has zero unaccounted replay or gap.
6. ACL policy equivalence and least-privilege verification (preserving ALLOW and DENY
   semantics without privilege widening).
7. Schema Registry compatibility, subject version completeness, and wire-format
   schema ID resolution so that topic byte equality never hides incompatible
   schema or authorization state.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import enum
import fcntl
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple, Union


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "0.0.0.0", "::1"})


# -----------------------------------------------------------------------------
# Care Badges and Core Enums
# -----------------------------------------------------------------------------

class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class OffsetCapability(str, enum.Enum):
    OFFSET_PRESERVING = "OFFSET_PRESERVING"
    OFFSET_REMAPPED = "OFFSET_REMAPPED"
    BYTE_COPY_ONLY = "BYTE_COPY_ONLY"


class CleanupPolicy(str, enum.Enum):
    DELETE = "delete"
    COMPACT = "compact"
    COMPACT_DELETE = "compact,delete"


class PromotionPhase(str, enum.Enum):
    INIT = "INIT"
    FENCE_SOURCE_PRODUCERS = "FENCE_SOURCE_PRODUCERS"
    DRAIN_MIRROR_LAG = "DRAIN_MIRROR_LAG"
    QUIESCE_ACTIVE_CONSUMERS = "QUIESCE_ACTIVE_CONSUMERS"
    TRANSLATE_AND_VERIFY_OFFSETS = "TRANSLATE_AND_VERIFY_OFFSETS"
    PROMOTE_TARGET_TOPIC = "PROMOTE_TARGET_TOPIC"
    RESUME_CONSUMERS = "RESUME_CONSUMERS"
    COMPLETED = "COMPLETED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


class ConsumerGroupState(str, enum.Enum):
    EMPTY = "EMPTY"
    QUIESCED = "QUIESCED"
    STABLE = "STABLE"
    PREPARING_REBALANCE = "PREPARING_REBALANCE"
    COMPLETING_REBALANCE = "COMPLETING_REBALANCE"
    DEAD = "DEAD"


class SchemaFormat(str, enum.Enum):
    AVRO = "AVRO"
    PROTOBUF = "PROTOBUF"
    JSON = "JSON"


class CompatibilityLevel(str, enum.Enum):
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


class AclResourceType(str, enum.Enum):
    TOPIC = "TOPIC"
    GROUP = "GROUP"
    CLUSTER = "CLUSTER"
    TRANSACTIONAL_ID = "TRANSACTIONAL_ID"
    SUBJECT = "SUBJECT"


class AclPatternType(str, enum.Enum):
    LITERAL = "LITERAL"
    PREFIXED = "PREFIXED"


class AclOperation(str, enum.Enum):
    READ = "READ"
    WRITE = "WRITE"
    CREATE = "CREATE"
    DELETE = "DELETE"
    ALTER = "ALTER"
    ALTER_CONFIGS = "ALTER_CONFIGS"
    DESCRIBE = "DESCRIBE"
    DESCRIBE_CONFIGS = "DESCRIBE_CONFIGS"
    IDEMPOTENT_WRITE = "IDEMPOTENT_WRITE"
    ALL = "ALL"


class AclPermissionType(str, enum.Enum):
    ALLOW = "ALLOW"
    DENY = "DENY"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class StreamMetadataError(Exception):
    """Base exception for stream metadata, consumer, and schema migration errors."""


class UnsafeTestHostError(StreamMetadataError):
    """Raised when a broker, registry, or client host is outside safe test ranges."""


class CapabilityMismatchError(StreamMetadataError):
    """Raised when an operation assumes offset preservation on a remapped or raw path."""


class OffsetOutOfRetentionBoundsError(StreamMetadataError):
    """Raised when a consumer offset falls outside target partition retention bounds."""


class PartitionIdentityMismatchError(StreamMetadataError):
    """Raised when topic partition count or partitioning semantics diverge."""


class PromotionFenceError(StreamMetadataError):
    """Raised when promotion is attempted without producer fence or mirror drain."""


class ActiveConsumerFenceError(StreamMetadataError):
    """Raised when offsets are migrated or promoted while consumers are still active."""


class ConsumerRestartDiscontinuityError(StreamMetadataError):
    """Raised when post-promotion consumer restart has unaccounted replay or gap."""


class SchemaCompatibilityError(StreamMetadataError):
    """Raised when schema subjects, compatibility levels, or wire IDs are incompatible."""


class AclPolicyViolationError(StreamMetadataError):
    """Raised when target ACL state lacks required rules, drops DENY, or widens privileges."""


class LeaseLockError(StreamMetadataError):
    """Raised when the mutual exclusion .run.lock cannot be acquired or is held."""


# -----------------------------------------------------------------------------
# Host Validation
# -----------------------------------------------------------------------------

def is_safe_test_host(host: str) -> bool:
    h = str(host).strip().lower()
    if ":" in h and not h.startswith("::"):
        h = h.split(":", 1)[0]
    if h in SAFE_TEST_HOSTS:
        return True
    if h.startswith("10.0.0."):
        parts = h.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return True
    return False


def validate_test_host(host: str) -> str:
    h = str(host).strip()
    if not is_safe_test_host(h):
        raise UnsafeTestHostError(
            f"unsafe host rejected: {host!r} (allowed: 127.0.0.1, localhost, 10.0.0.x)"
        )
    return h


# -----------------------------------------------------------------------------
# Mutual Exclusion Lease Lock (.run.lock) and O(1) Typed Durable Resume
# -----------------------------------------------------------------------------

class StreamMigrationLeaseLock:
    """Mutual exclusion .run.lock with heartbeat for stream metadata cutovers."""

    def __init__(self, lock_dir: Union[str, Path], lease_ttl_sec: float = 30.0) -> None:
        self.lock_dir = Path(lock_dir)
        self.lock_path = self.lock_dir / ".run.lock"
        self.lease_ttl_sec = float(lease_ttl_sec)
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
            raise LeaseLockError(f"active lease lock already held at {self.lock_path}") from exc
        self._fd = fd
        self._write_state(cur)

    def heartbeat(self, now: Optional[float] = None) -> None:
        if self._fd is None:
            raise LeaseLockError("cannot heartbeat an unacquired lease lock")
        cur = time.time() if now is None else float(now)
        self._write_state(cur)

    def _write_state(self, timestamp: float) -> None:
        assert self._fd is not None
        payload = json.dumps(
            {
                "owner_id": self.owner_id,
                "heartbeat_at": timestamp,
                "expires_at": timestamp + self.lease_ttl_sec,
            },
            sort_keys=True,
        ).encode("ascii")
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.ftruncate(self._fd, 0)
        os.write(self._fd, payload)
        os.fsync(self._fd)

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None
            if self.lock_path.exists():
                try:
                    self.lock_path.unlink()
                except OSError:
                    pass

    def __enter__(self) -> "StreamMigrationLeaseLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


@dataclasses.dataclass
class StreamMetadataCheckpointStore:
    """O(1) typed durable resume store keyed by composite cursor identifiers."""

    checkpoint_path: Path
    completed_cursors: Dict[str, Dict[str, Any]] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        self.checkpoint_path = Path(self.checkpoint_path)
        if self.checkpoint_path.exists():
            raw = json.loads(self.checkpoint_path.read_text(encoding="ascii"))
            self.completed_cursors = dict(raw.get("completed_cursors", {}))

    @staticmethod
    def make_cursor_key(kind: str, *parts: Union[str, int]) -> str:
        joined = ":".join(str(p) for p in parts)
        return f"{kind}:{joined}"

    def is_completed(self, kind: str, *parts: Union[str, int]) -> bool:
        key = self.make_cursor_key(kind, *parts)
        return key in self.completed_cursors

    def get_cursor(self, kind: str, *parts: Union[str, int]) -> Optional[Dict[str, Any]]:
        key = self.make_cursor_key(kind, *parts)
        return self.completed_cursors.get(key)

    def record_completed(
        self,
        kind: str,
        parts: Sequence[Union[str, int]],
        state: Dict[str, Any],
    ) -> str:
        key = self.make_cursor_key(kind, *parts)
        self.completed_cursors[key] = {
            "cursor_key": key,
            "state": state,
            "updated_at": time.time(),
        }
        self._persist()
        return key

    def _persist(self) -> None:
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.checkpoint_path.with_suffix(".tmp")
        payload = json.dumps(
            {"completed_cursors": self.completed_cursors},
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
        )
        tmp_path.write_text(payload, encoding="ascii")
        os.replace(str(tmp_path), str(self.checkpoint_path))


# -----------------------------------------------------------------------------
# Topic Configuration, Partition Identity, and Retained Ranges
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class PartitionRetainedRange:
    """Retained offset boundaries and payload digest for a single topic partition."""

    partition_id: int
    log_start_offset: int
    high_watermark: int
    last_stable_offset: Optional[int] = None
    record_count: int = 0
    byte_count: int = 0
    payload_sha256: str = ""
    embedded_schema_ids: Tuple[int, ...] = ()
    compacted: bool = False
    leader_epoch: int = 0

    def __post_init__(self) -> None:
        if self.partition_id < 0:
            raise ValueError(f"partition_id must be >= 0, got {self.partition_id}")
        if self.log_start_offset < 0:
            raise ValueError(f"log_start_offset must be >= 0, got {self.log_start_offset}")
        if self.high_watermark < self.log_start_offset:
            raise ValueError(
                f"high_watermark ({self.high_watermark}) cannot precede "
                f"log_start_offset ({self.log_start_offset}) on partition {self.partition_id}"
            )

    @property
    def retained_span(self) -> int:
        return self.high_watermark - self.log_start_offset

    def contains_consumer_offset(self, offset: int) -> bool:
        """A consumer offset is valid if log_start_offset <= offset <= high_watermark.

        Note: offset == high_watermark is valid and represents a consumer caught up
        to the tail of the partition waiting for the next event.
        """
        return self.log_start_offset <= offset <= self.high_watermark


@dataclasses.dataclass(frozen=True)
class TopicConfigSpec:
    """Topic-level configuration and partition topology."""

    topic_name: str
    num_partitions: int
    replication_factor: int = 3
    cleanup_policy: str = "delete"
    retention_ms: int = 604800000
    retention_bytes: int = -1
    min_insync_replicas: int = 2
    max_message_bytes: int = 1048588
    compression_type: str = "producer"
    message_timestamp_type: str = "CreateTime"
    partitioner_strategy: str = "murmur2_key_hash"
    partitions: Tuple[PartitionRetainedRange, ...] = ()
    is_mirror_topic: bool = False
    writable: bool = True
    broker_host: str = "127.0.0.1"

    def __post_init__(self) -> None:
        validate_test_host(self.broker_host)
        if self.num_partitions <= 0:
            raise ValueError(f"num_partitions must be > 0, got {self.num_partitions}")

    def partition_map(self) -> Dict[int, PartitionRetainedRange]:
        return {p.partition_id: p for p in self.partitions}


# -----------------------------------------------------------------------------
# Offset Capability and Translation Models
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class PartitionOffsetMapping:
    """Explicit offset mapping for a single partition on an OFFSET_REMAPPED path."""

    partition_id: int
    source_to_target: Dict[int, int] = dataclasses.field(default_factory=dict)
    base_source_offset: Optional[int] = None
    base_target_offset: Optional[int] = None

    def translate(self, source_offset: int) -> int:
        if source_offset in self.source_to_target:
            return self.source_to_target[source_offset]
        if self.base_source_offset is not None and self.base_target_offset is not None:
            delta = source_offset - self.base_source_offset
            return self.base_target_offset + delta
        raise CapabilityMismatchError(
            f"no offset translation entry for partition {self.partition_id} "
            f"source_offset={source_offset} on OFFSET_REMAPPED path"
        )

    def reverse_translate(self, target_offset: int) -> int:
        for src, tgt in self.source_to_target.items():
            if tgt == target_offset:
                return src
        if self.base_source_offset is not None and self.base_target_offset is not None:
            delta = target_offset - self.base_target_offset
            return self.base_source_offset + delta
        raise CapabilityMismatchError(
            f"no reverse offset translation for partition {self.partition_id} "
            f"target_offset={target_offset} on OFFSET_REMAPPED path"
        )


@dataclasses.dataclass(frozen=True)
class ReplicationLinkSpec:
    """Models the stream replication link and its offset capability."""

    link_id: str
    capability: OffsetCapability
    source_cluster_id: str
    target_cluster_id: str
    source_broker_host: str = "127.0.0.1"
    target_broker_host: str = "10.0.0.1"
    partition_mappings: Dict[Tuple[str, int], PartitionOffsetMapping] = dataclasses.field(
        default_factory=dict
    )
    mirror_lag_ByPartition: Dict[Tuple[str, int], int] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_test_host(self.source_broker_host)
        validate_test_host(self.target_broker_host)

    def total_mirror_lag(self, topic_name: Optional[str] = None) -> int:
        total = 0
        for (t_name, _pid), lag in self.mirror_lag_ByPartition.items():
            if topic_name is None or t_name == topic_name:
                total += max(0, int(lag))
        return total

    def translate_offset(
        self,
        topic_name: str,
        partition_id: int,
        source_offset: int,
        allow_remapped: bool = True,
    ) -> int:
        if self.capability == OffsetCapability.OFFSET_PRESERVING:
            return source_offset
        if self.capability == OffsetCapability.BYTE_COPY_ONLY:
            raise CapabilityMismatchError(
                f"link {self.link_id!r} has capability BYTE_COPY_ONLY; "
                f"consumer offset migration is forbidden without offset-preserving "
                f"or offset-remapped capability"
            )
        if not allow_remapped:
            raise CapabilityMismatchError(
                f"caller requested direct offset preservation on OFFSET_REMAPPED "
                f"link {self.link_id!r} for {topic_name}[{partition_id}]"
            )
        mapping = self.partition_mappings.get((topic_name, partition_id))
        if mapping is None:
            raise CapabilityMismatchError(
                f"missing PartitionOffsetMapping for {topic_name}[{partition_id}] "
                f"on OFFSET_REMAPPED link {self.link_id!r}"
            )
        return mapping.translate(source_offset)

    def reverse_translate_offset(
        self,
        topic_name: str,
        partition_id: int,
        target_offset: int,
    ) -> int:
        if self.capability == OffsetCapability.OFFSET_PRESERVING:
            return target_offset
        if self.capability == OffsetCapability.BYTE_COPY_ONLY:
            raise CapabilityMismatchError(
                f"link {self.link_id!r} has capability BYTE_COPY_ONLY; "
                f"reverse offset mapping is undefined"
            )
        mapping = self.partition_mappings.get((topic_name, partition_id))
        if mapping is None:
            raise CapabilityMismatchError(
                f"missing PartitionOffsetMapping for {topic_name}[{partition_id}] "
                f"on OFFSET_REMAPPED link {self.link_id!r}"
            )
        return mapping.reverse_translate(target_offset)


# -----------------------------------------------------------------------------
# Consumer Group Offsets and Restart Continuity Models
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class ConsumerPartitionOffset:
    """Committed consumer offset for a single topic partition."""

    topic_name: str
    partition_id: int
    committed_offset: int
    leader_epoch: int = 0
    metadata: str = ""


@dataclasses.dataclass
class ConsumerGroupSpec:
    """Consumer group state, active membership, and committed partition offsets."""

    group_id: str
    state: ConsumerGroupState
    offsets: Dict[Tuple[str, int], ConsumerPartitionOffset] = dataclasses.field(
        default_factory=dict
    )
    active_member_hosts: List[str] = dataclasses.field(default_factory=list)
    fenced: bool = False

    def __post_init__(self) -> None:
        for host in self.active_member_hosts:
            validate_test_host(host)

    @property
    def has_active_consumers(self) -> bool:
        if len(self.active_member_hosts) > 0:
            return True
        return self.state in (
            ConsumerGroupState.STABLE,
            ConsumerGroupState.PREPARING_REBALANCE,
            ConsumerGroupState.COMPLETING_REBALANCE,
        )


@dataclasses.dataclass(frozen=True)
class TranslatedConsumerOffset:
    """Result of translating and validating a consumer group partition offset."""

    group_id: str
    topic_name: str
    partition_id: int
    source_committed_offset: int
    target_committed_offset: int
    capability_used: OffsetCapability
    target_log_start_offset: int
    target_high_watermark: int
    within_retention: bool
    badge: CareBadge
    reason: str


@dataclasses.dataclass(frozen=True)
class ConsumerRestartVerification:
    """Proof of consumer restart continuity after target promotion."""

    group_id: str
    topic_name: str
    partition_id: int
    expected_source_offset: int
    actual_target_restart_offset: int
    equivalent_source_offset: int
    unaccounted_gap: int
    unaccounted_replay: int
    approved_max_replay: int
    certified: bool
    badge: CareBadge
    detail: str


# -----------------------------------------------------------------------------
# Schema Registry Models
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class SchemaFieldSpec:
    """Canonical schema field definition for structural evolution checks."""

    name: str
    field_type: str
    required: bool = True
    has_default: bool = False


@dataclasses.dataclass(frozen=True)
class SchemaVersionSpec:
    """A registered version of a subject in Schema Registry."""

    subject: str
    version: int
    global_schema_id: int
    schema_format: SchemaFormat = SchemaFormat.AVRO
    canonical_text: str = ""
    fields: Tuple[SchemaFieldSpec, ...] = ()

    @property
    def fingerprint_sha256(self) -> str:
        basis = {
            "format": self.schema_format.value,
            "canonical_text": self.canonical_text.strip(),
            "fields": [
                {
                    "name": f.name,
                    "type": f.field_type,
                    "required": f.required,
                    "has_default": f.has_default,
                }
                for f in self.fields
            ],
        }
        encoded = json.dumps(basis, sort_keys=True, separators=(",", ":")).encode("ascii")
        return hashlib.sha256(encoded).hexdigest()


@dataclasses.dataclass(frozen=True)
class SchemaSubjectState:
    """Subject state in Schema Registry including compatibility policy and versions."""

    subject: str
    compatibility: CompatibilityLevel
    versions: Tuple[SchemaVersionSpec, ...] = ()
    registry_mode: str = "READWRITE"  # IMPORT, READWRITE, READONLY
    registry_host: str = "127.0.0.1"

    def __post_init__(self) -> None:
        validate_test_host(self.registry_host)

    def version_map(self) -> Dict[int, SchemaVersionSpec]:
        return {v.version: v for v in self.versions}

    def id_map(self) -> Dict[int, SchemaVersionSpec]:
        return {v.global_schema_id: v for v in self.versions}

    def latest_version(self) -> Optional[SchemaVersionSpec]:
        if not self.versions:
            return None
        return max(self.versions, key=lambda v: v.version)


# -----------------------------------------------------------------------------
# ACL Policy Models
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class AclBinding:
    """Kafka / Schema Registry ACL binding."""

    principal: str
    resource_type: AclResourceType
    resource_name: str
    operation: AclOperation
    permission_type: AclPermissionType = AclPermissionType.ALLOW
    pattern_type: AclPatternType = AclPatternType.LITERAL
    host: str = "127.0.0.1"

    def __post_init__(self) -> None:
        if self.host != "*":
            validate_test_host(self.host)

    def signature(self) -> Tuple[str, str, str, str, str, str, str]:
        return (
            self.principal.strip(),
            self.resource_type.value,
            self.pattern_type.value,
            self.resource_name.strip(),
            self.operation.value,
            self.permission_type.value,
            self.host.strip(),
        )


# -----------------------------------------------------------------------------
# Verification Sub-Proofs and Findings
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class MetadataFinding:
    """Single structured audit finding with a 4-tier CareBadge."""

    component: str
    badge: CareBadge
    code: str
    message: str
    details: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component": self.component,
            "badge": self.badge.value,
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclasses.dataclass(frozen=True)
class TopicAndRangeProof:
    """Verification verdict for topic configs, partition identity, and retained ranges."""

    topic_name: str
    certified: bool
    badge: CareBadge
    byte_equal: bool
    partition_identity_equal: bool
    config_compatible: bool
    retained_ranges_valid: bool
    findings: Tuple[MetadataFinding, ...] = ()


@dataclasses.dataclass(frozen=True)
class SchemaRegistryProof:
    """Verification verdict for Schema Registry subjects and payload schema IDs."""

    certified: bool
    badge: CareBadge
    subjects_checked: Tuple[str, ...]
    wire_schema_ids_verified: Tuple[int, ...]
    findings: Tuple[MetadataFinding, ...] = ()


@dataclasses.dataclass(frozen=True)
class AclPolicyProof:
    """Verification verdict for source-to-target ACL policy migration."""

    certified: bool
    badge: CareBadge
    missing_allow_count: int
    missing_deny_count: int
    unapproved_widening_count: int
    findings: Tuple[MetadataFinding, ...] = ()


@dataclasses.dataclass(frozen=True)
class StreamMigrationProof:
    """Aggregate streaming migration proof combining bytes, configs, offsets, ACLs, and schemas."""

    topic_name: str
    certified: bool
    badge: CareBadge
    capability: OffsetCapability
    byte_equal: bool
    topic_proof: TopicAndRangeProof
    schema_proof: SchemaRegistryProof
    acl_proof: AclPolicyProof
    translated_offsets: Tuple[TranslatedConsumerOffset, ...] = ()
    restart_verifications: Tuple[ConsumerRestartVerification, ...] = ()
    findings: Tuple[MetadataFinding, ...] = ()

    def format_report(self) -> str:
        lines = [
            f"[{self.badge.value}] STREAM_MIGRATION_PROOF topic={self.topic_name} "
            f"certified={self.certified} capability={self.capability.value} "
            f"byte_equal={self.byte_equal}",
            f"[{self.topic_proof.badge.value}] TOPIC_AND_RANGES "
            f"partition_identity={self.topic_proof.partition_identity_equal} "
            f"config_compatible={self.topic_proof.config_compatible} "
            f"retained_ranges={self.topic_proof.retained_ranges_valid}",
            f"[{self.schema_proof.badge.value}] SCHEMA_REGISTRY "
            f"certified={self.schema_proof.certified} "
            f"subjects={len(self.schema_proof.subjects_checked)} "
            f"wire_ids={len(self.schema_proof.wire_schema_ids_verified)}",
            f"[{self.acl_proof.badge.value}] ACL_POLICY "
            f"certified={self.acl_proof.certified} "
            f"missing_allow={self.acl_proof.missing_allow_count} "
            f"missing_deny={self.acl_proof.missing_deny_count} "
            f"widening={self.acl_proof.unapproved_widening_count}",
        ]
        for f in self.findings:
            lines.append(f"[{f.badge.value}] {f.component}:{f.code} {f.message}")
        return "\n".join(lines)


# -----------------------------------------------------------------------------
# Verification Functions
# -----------------------------------------------------------------------------

def _worst_badge(badges: Iterable[CareBadge]) -> CareBadge:
    order = {
        CareBadge.PASS: 0,
        CareBadge.BENIGN: 1,
        CareBadge.REVIEW: 2,
        CareBadge.ACTION: 3,
    }
    worst = CareBadge.PASS
    for b in badges:
        if order[b] > order[worst]:
            worst = b
    return worst


def verify_topic_and_retained_ranges(
    source: TopicConfigSpec,
    target: TopicConfigSpec,
    capability: OffsetCapability,
    strict_config: bool = False,
) -> TopicAndRangeProof:
    """Verify topic configs, partition identity, and per-partition retained ranges."""

    findings: List[MetadataFinding] = []
    partition_identity_equal = True
    config_compatible = True
    retained_ranges_valid = True
    byte_equal = True

    if source.num_partitions != target.num_partitions:
        partition_identity_equal = False
        findings.append(
            MetadataFinding(
                component="topic_identity",
                badge=CareBadge.ACTION,
                code="PARTITION_COUNT_MISMATCH",
                message=(
                    f"source topic {source.topic_name!r} has {source.num_partitions} "
                    f"partitions but target {target.topic_name!r} has {target.num_partitions}"
                ),
            )
        )

    if source.partitioner_strategy != target.partitioner_strategy:
        partition_identity_equal = False
        findings.append(
            MetadataFinding(
                component="topic_identity",
                badge=CareBadge.ACTION,
                code="PARTITIONER_STRATEGY_MISMATCH",
                message=(
                    f"partitioner strategy changed from {source.partitioner_strategy!r} "
                    f"to {target.partitioner_strategy!r}, breaking key-to-partition ordering"
                ),
            )
        )

    if source.cleanup_policy != target.cleanup_policy:
        config_compatible = False
        findings.append(
            MetadataFinding(
                component="topic_config",
                badge=CareBadge.ACTION,
                code="CLEANUP_POLICY_MISMATCH",
                message=(
                    f"cleanup.policy changed from {source.cleanup_policy!r} "
                    f"to {target.cleanup_policy!r}"
                ),
            )
        )

    if target.min_insync_replicas < source.min_insync_replicas:
        config_compatible = False
        findings.append(
            MetadataFinding(
                component="topic_config",
                badge=CareBadge.ACTION,
                code="MIN_ISR_DOWNGRADE",
                message=(
                    f"target min.insync.replicas ({target.min_insync_replicas}) is lower "
                    f"than source ({source.min_insync_replicas})"
                ),
            )
        )

    if target.max_message_bytes < source.max_message_bytes:
        config_compatible = False
        findings.append(
            MetadataFinding(
                component="topic_config",
                badge=CareBadge.ACTION,
                code="MAX_MESSAGE_BYTES_REDUCED",
                message=(
                    f"target max.message.bytes ({target.max_message_bytes}) is smaller "
                    f"than source ({source.max_message_bytes})"
                ),
            )
        )

    if target.retention_ms != -1 and (
        source.retention_ms == -1 or target.retention_ms < source.retention_ms
    ):
        badge = CareBadge.ACTION if strict_config else CareBadge.REVIEW
        if strict_config:
            config_compatible = False
        findings.append(
            MetadataFinding(
                component="topic_config",
                badge=badge,
                code="RETENTION_MS_SHORTER",
                message=(
                    f"target retention.ms ({target.retention_ms}) is shorter than "
                    f"source ({source.retention_ms})"
                ),
            )
        )
    elif target.retention_ms > source.retention_ms and source.retention_ms != -1:
        findings.append(
            MetadataFinding(
                component="topic_config",
                badge=CareBadge.BENIGN,
                code="RETENTION_MS_EXTENDED",
                message=(
                    f"target retention.ms ({target.retention_ms}) is longer than "
                    f"source ({source.retention_ms})"
                ),
            )
        )

    src_parts = source.partition_map()
    tgt_parts = target.partition_map()

    if set(src_parts.keys()) != set(tgt_parts.keys()):
        partition_identity_equal = False
        retained_ranges_valid = False
        byte_equal = False
        findings.append(
            MetadataFinding(
                component="retained_range",
                badge=CareBadge.ACTION,
                code="PARTITION_SET_MISMATCH",
                message=(
                    f"partition IDs differ: source={sorted(src_parts.keys())} "
                    f"target={sorted(tgt_parts.keys())}"
                ),
            )
        )
    else:
        for pid in sorted(src_parts.keys()):
            sp = src_parts[pid]
            tp = tgt_parts[pid]

            if sp.payload_sha256 != tp.payload_sha256 or sp.byte_count != tp.byte_count:
                byte_equal = False
                retained_ranges_valid = False
                findings.append(
                    MetadataFinding(
                        component="retained_range",
                        badge=CareBadge.ACTION,
                        code="PARTITION_PAYLOAD_MISMATCH",
                        message=(
                            f"partition {pid} payload mismatch: "
                            f"source(bytes={sp.byte_count}, sha256={sp.payload_sha256}) != "
                            f"target(bytes={tp.byte_count}, sha256={tp.payload_sha256})"
                        ),
                    )
                )

            if sp.record_count != tp.record_count:
                retained_ranges_valid = False
                findings.append(
                    MetadataFinding(
                        component="retained_range",
                        badge=CareBadge.ACTION,
                        code="PARTITION_RECORD_COUNT_MISMATCH",
                        message=(
                            f"partition {pid} record count mismatch: "
                            f"source={sp.record_count} target={tp.record_count}"
                        ),
                    )
                )

            if capability == OffsetCapability.OFFSET_PRESERVING:
                if tp.log_start_offset > sp.log_start_offset:
                    retained_ranges_valid = False
                    findings.append(
                        MetadataFinding(
                            component="retained_range",
                            badge=CareBadge.ACTION,
                            code="TARGET_RETENTION_TRUNCATED",
                            message=(
                                f"partition {pid} target log_start_offset "
                                f"({tp.log_start_offset}) > source ({sp.log_start_offset}); "
                                f"retained prefix missing on target"
                            ),
                        )
                    )
                if tp.high_watermark != sp.high_watermark:
                    retained_ranges_valid = False
                    findings.append(
                        MetadataFinding(
                            component="retained_range",
                            badge=CareBadge.ACTION,
                            code="HIGH_WATERMARK_DIVERGENCE",
                            message=(
                                f"partition {pid} high_watermark mismatch on "
                                f"OFFSET_PRESERVING path: source={sp.high_watermark} "
                                f"target={tp.high_watermark}"
                            ),
                        )
                    )

    if not findings:
        findings.append(
            MetadataFinding(
                component="topic_and_ranges",
                badge=CareBadge.PASS,
                code="TOPIC_AND_RANGES_VERIFIED",
                message=f"topic {source.topic_name!r} configs, partitions, and ranges verified",
            )
        )

    badge = _worst_badge(f.badge for f in findings)
    certified = (
        partition_identity_equal
        and config_compatible
        and retained_ranges_valid
        and byte_equal
        and badge != CareBadge.ACTION
    )
    return TopicAndRangeProof(
        topic_name=source.topic_name,
        certified=certified,
        badge=badge,
        byte_equal=byte_equal,
        partition_identity_equal=partition_identity_equal,
        config_compatible=config_compatible,
        retained_ranges_valid=retained_ranges_valid,
        findings=tuple(findings),
    )


def translate_and_validate_consumer_offsets(
    group: ConsumerGroupSpec,
    source_topic: TopicConfigSpec,
    target_topic: TopicConfigSpec,
    link: ReplicationLinkSpec,
    allow_remapped: bool = True,
    raise_on_out_of_bounds: bool = True,
) -> List[TranslatedConsumerOffset]:
    """Translate consumer group offsets across the replication link and enforce retention bounds."""

    if source_topic.num_partitions != target_topic.num_partitions:
        raise PartitionIdentityMismatchError(
            f"cannot migrate offsets for group {group.group_id!r}: partition count mismatch "
            f"({source_topic.num_partitions} vs {target_topic.num_partitions})"
        )

    src_parts = source_topic.partition_map()
    tgt_parts = target_topic.partition_map()
    results: List[TranslatedConsumerOffset] = []

    for (t_name, pid), c_off in sorted(group.offsets.items()):
        if t_name != source_topic.topic_name:
            continue
        if pid not in tgt_parts or pid not in src_parts:
            raise PartitionIdentityMismatchError(
                f"partition {pid} missing in topic metadata for {t_name!r}"
            )

        sp = src_parts[pid]
        tp = tgt_parts[pid]

        if c_off.committed_offset < sp.log_start_offset or c_off.committed_offset > sp.high_watermark:
            if raise_on_out_of_bounds:
                raise OffsetOutOfRetentionBoundsError(
                    f"source committed offset {c_off.committed_offset} for group "
                    f"{group.group_id!r} on {t_name}[{pid}] is outside source retention "
                    f"[{sp.log_start_offset}, {sp.high_watermark}]"
                )
            results.append(
                TranslatedConsumerOffset(
                    group_id=group.group_id,
                    topic_name=target_topic.topic_name,
                    partition_id=pid,
                    source_committed_offset=c_off.committed_offset,
                    target_committed_offset=-1,
                    capability_used=link.capability,
                    target_log_start_offset=tp.log_start_offset,
                    target_high_watermark=tp.high_watermark,
                    within_retention=False,
                    badge=CareBadge.ACTION,
                    reason=(
                        f"source offset {c_off.committed_offset} outside source retention "
                        f"[{sp.log_start_offset}, {sp.high_watermark}]"
                    ),
                )
            )
            continue

        target_offset = link.translate_offset(
            topic_name=t_name,
            partition_id=pid,
            source_offset=c_off.committed_offset,
            allow_remapped=allow_remapped,
        )

        within_bounds = tp.contains_consumer_offset(target_offset)
        if not within_bounds:
            msg = (
                f"translated offset {target_offset} (from source {c_off.committed_offset}) "
                f"for group {group.group_id!r} on {target_topic.topic_name}[{pid}] is outside "
                f"target retention bounds [{tp.log_start_offset}, {tp.high_watermark}]"
            )
            if raise_on_out_of_bounds:
                raise OffsetOutOfRetentionBoundsError(msg)
            results.append(
                TranslatedConsumerOffset(
                    group_id=group.group_id,
                    topic_name=target_topic.topic_name,
                    partition_id=pid,
                    source_committed_offset=c_off.committed_offset,
                    target_committed_offset=target_offset,
                    capability_used=link.capability,
                    target_log_start_offset=tp.log_start_offset,
                    target_high_watermark=tp.high_watermark,
                    within_retention=False,
                    badge=CareBadge.ACTION,
                    reason=msg,
                )
            )
            continue

        badge = (
            CareBadge.PASS
            if link.capability == OffsetCapability.OFFSET_PRESERVING
            else CareBadge.BENIGN
        )
        results.append(
            TranslatedConsumerOffset(
                group_id=group.group_id,
                topic_name=target_topic.topic_name,
                partition_id=pid,
                source_committed_offset=c_off.committed_offset,
                target_committed_offset=target_offset,
                capability_used=link.capability,
                target_log_start_offset=tp.log_start_offset,
                target_high_watermark=tp.high_watermark,
                within_retention=True,
                badge=badge,
                reason=(
                    f"offset {c_off.committed_offset} -> {target_offset} verified within "
                    f"[{tp.log_start_offset}, {tp.high_watermark}]"
                ),
            )
        )

    return results


def verify_consumer_restart(
    group_id: str,
    topic_name: str,
    target_topic: TopicConfigSpec,
    link: ReplicationLinkSpec,
    expected_source_offsets: Dict[int, int],
    actual_target_restart_offsets: Dict[int, int],
    approved_max_replay: int = 0,
    raise_on_discontinuity: bool = True,
) -> List[ConsumerRestartVerification]:
    """Verify that consumer restart on the promoted target has no unaccounted replay or gap."""

    tgt_parts = target_topic.partition_map()
    verifications: List[ConsumerRestartVerification] = []

    all_pids = sorted(set(expected_source_offsets.keys()) | set(actual_target_restart_offsets.keys()))
    for pid in all_pids:
        if pid not in expected_source_offsets or pid not in actual_target_restart_offsets:
            msg = f"missing restart offset state on partition {pid} for group {group_id!r}"
            if raise_on_discontinuity:
                raise ConsumerRestartDiscontinuityError(msg)
            verifications.append(
                ConsumerRestartVerification(
                    group_id=group_id,
                    topic_name=topic_name,
                    partition_id=pid,
                    expected_source_offset=expected_source_offsets.get(pid, -1),
                    actual_target_restart_offset=actual_target_restart_offsets.get(pid, -1),
                    equivalent_source_offset=-1,
                    unaccounted_gap=1,
                    unaccounted_replay=0,
                    approved_max_replay=approved_max_replay,
                    certified=False,
                    badge=CareBadge.ACTION,
                    detail=msg,
                )
            )
            continue

        expected_src = expected_source_offsets[pid]
        actual_tgt = actual_target_restart_offsets[pid]
        tp = tgt_parts.get(pid)
        if tp is None or not tp.contains_consumer_offset(actual_tgt):
            bounds_str = (
                f"[{tp.log_start_offset}, {tp.high_watermark}]" if tp is not None else "missing"
            )
            msg = (
                f"consumer {group_id!r} restart offset {actual_tgt} on "
                f"{topic_name}[{pid}] is outside target retention bounds {bounds_str}"
            )
            if raise_on_discontinuity:
                raise OffsetOutOfRetentionBoundsError(msg)
            verifications.append(
                ConsumerRestartVerification(
                    group_id=group_id,
                    topic_name=topic_name,
                    partition_id=pid,
                    expected_source_offset=expected_src,
                    actual_target_restart_offset=actual_tgt,
                    equivalent_source_offset=-1,
                    unaccounted_gap=1,
                    unaccounted_replay=0,
                    approved_max_replay=approved_max_replay,
                    certified=False,
                    badge=CareBadge.ACTION,
                    detail=msg,
                )
            )
            continue

        equiv_src = link.reverse_translate_offset(topic_name, pid, actual_tgt)
        gap = max(0, equiv_src - expected_src)
        raw_replay = max(0, expected_src - equiv_src)
        unaccounted_replay = max(0, raw_replay - max(0, approved_max_replay))

        if gap > 0 or unaccounted_replay > 0:
            msg = (
                f"consumer restart discontinuity for group {group_id!r} on "
                f"{topic_name}[{pid}]: expected_source={expected_src}, "
                f"actual_target={actual_tgt} (source_equiv={equiv_src}), "
                f"unaccounted_gap={gap}, unaccounted_replay={unaccounted_replay}"
            )
            if raise_on_discontinuity:
                raise ConsumerRestartDiscontinuityError(msg)
            verifications.append(
                ConsumerRestartVerification(
                    group_id=group_id,
                    topic_name=topic_name,
                    partition_id=pid,
                    expected_source_offset=expected_src,
                    actual_target_restart_offset=actual_tgt,
                    equivalent_source_offset=equiv_src,
                    unaccounted_gap=gap,
                    unaccounted_replay=unaccounted_replay,
                    approved_max_replay=approved_max_replay,
                    certified=False,
                    badge=CareBadge.ACTION,
                    detail=msg,
                )
            )
        else:
            badge = CareBadge.BENIGN if raw_replay > 0 else CareBadge.PASS
            verifications.append(
                ConsumerRestartVerification(
                    group_id=group_id,
                    topic_name=topic_name,
                    partition_id=pid,
                    expected_source_offset=expected_src,
                    actual_target_restart_offset=actual_tgt,
                    equivalent_source_offset=equiv_src,
                    unaccounted_gap=0,
                    unaccounted_replay=0,
                    approved_max_replay=approved_max_replay,
                    certified=True,
                    badge=badge,
                    detail=(
                        f"restart continuous at target offset {actual_tgt} "
                        f"(source equivalent {equiv_src}, bounded replay {raw_replay})"
                    ),
                )
            )

    return verifications


# -----------------------------------------------------------------------------
# Schema Registry Compatibility Verification
# -----------------------------------------------------------------------------

_COMPATIBILITY_STRENGTH: Dict[CompatibilityLevel, int] = {
    CompatibilityLevel.NONE: 0,
    CompatibilityLevel.BACKWARD: 1,
    CompatibilityLevel.FORWARD: 1,
    CompatibilityLevel.BACKWARD_TRANSITIVE: 2,
    CompatibilityLevel.FORWARD_TRANSITIVE: 2,
    CompatibilityLevel.FULL: 3,
    CompatibilityLevel.FULL_TRANSITIVE: 4,
}


def check_structural_schema_compatibility(
    older: SchemaVersionSpec,
    newer: SchemaVersionSpec,
    mode: CompatibilityLevel,
) -> Tuple[bool, str]:
    """Check structural field evolution between two schema versions under a compatibility mode."""

    if older.schema_format != newer.schema_format:
        return (
            False,
            f"schema format changed from {older.schema_format.value} to {newer.schema_format.value}",
        )

    old_fields = {f.name: f for f in older.fields}
    new_fields = {f.name: f for f in newer.fields}

    for fname, old_f in old_fields.items():
        if fname in new_fields and new_fields[fname].field_type != old_f.field_type:
            return (
                False,
                f"field {fname!r} changed type from {old_f.field_type!r} "
                f"to {new_fields[fname].field_type!r}",
            )

    check_backward = mode in (
        CompatibilityLevel.BACKWARD,
        CompatibilityLevel.BACKWARD_TRANSITIVE,
        CompatibilityLevel.FULL,
        CompatibilityLevel.FULL_TRANSITIVE,
    )
    check_forward = mode in (
        CompatibilityLevel.FORWARD,
        CompatibilityLevel.FORWARD_TRANSITIVE,
        CompatibilityLevel.FULL,
        CompatibilityLevel.FULL_TRANSITIVE,
    )

    if check_backward:
        for fname, new_f in new_fields.items():
            if fname not in old_fields and new_f.required and not new_f.has_default:
                return (
                    False,
                    f"BACKWARD incompatibility: newly added required field {fname!r} "
                    f"has no default value",
                )

    if check_forward:
        for fname, old_f in old_fields.items():
            if fname not in new_fields and old_f.required and not old_f.has_default:
                return (
                    False,
                    f"FORWARD incompatibility: removed required field {fname!r} "
                    f"had no default value in older schema",
                )

    return True, "compatible"


def verify_schema_registry_state(
    source_subjects: Dict[str, SchemaSubjectState],
    target_subjects: Dict[str, SchemaSubjectState],
    embedded_wire_schema_ids: Iterable[int] = (),
    schema_id_remap: Optional[Dict[int, int]] = None,
) -> SchemaRegistryProof:
    """Verify Schema Registry subjects, versions, compatibility levels, and wire schema IDs."""

    findings: List[MetadataFinding] = []
    subjects_checked: List[str] = []

    source_by_id: Dict[int, SchemaVersionSpec] = {}
    target_by_id: Dict[int, SchemaVersionSpec] = {}

    for subj_name, src_subj in sorted(source_subjects.items()):
        subjects_checked.append(subj_name)
        for v in src_subj.versions:
            source_by_id[v.global_schema_id] = v

        tgt_subj = target_subjects.get(subj_name)
        if tgt_subj is None:
            findings.append(
                MetadataFinding(
                    component="schema_registry",
                    badge=CareBadge.ACTION,
                    code="MISSING_TARGET_SUBJECT",
                    message=f"subject {subj_name!r} is missing on target Schema Registry",
                )
            )
            continue

        for v in tgt_subj.versions:
            target_by_id[v.global_schema_id] = v

        if tgt_subj.compatibility != src_subj.compatibility:
            src_rank = _COMPATIBILITY_STRENGTH[src_subj.compatibility]
            tgt_rank = _COMPATIBILITY_STRENGTH[tgt_subj.compatibility]
            if tgt_rank < src_rank or (
                src_rank == tgt_rank and tgt_subj.compatibility != src_subj.compatibility
            ):
                findings.append(
                    MetadataFinding(
                        component="schema_registry",
                        badge=CareBadge.ACTION,
                        code="SCHEMA_COMPATIBILITY_DOWNGRADE",
                        message=(
                            f"subject {subj_name!r} compatibility changed from "
                            f"{src_subj.compatibility.value} to {tgt_subj.compatibility.value}"
                        ),
                    )
                )
            else:
                findings.append(
                    MetadataFinding(
                        component="schema_registry",
                        badge=CareBadge.REVIEW,
                        code="SCHEMA_COMPATIBILITY_STRENGTHENED",
                        message=(
                            f"subject {subj_name!r} compatibility tightened from "
                            f"{src_subj.compatibility.value} to {tgt_subj.compatibility.value}"
                        ),
                    )
                )

        tgt_vmap = tgt_subj.version_map()
        for src_v in src_subj.versions:
            tgt_v = tgt_vmap.get(src_v.version)
            if tgt_v is None:
                findings.append(
                    MetadataFinding(
                        component="schema_registry",
                        badge=CareBadge.ACTION,
                        code="MISSING_SCHEMA_VERSION",
                        message=(
                            f"subject {subj_name!r} version {src_v.version} is missing on target"
                        ),
                    )
                )
                continue

            if src_v.fingerprint_sha256 != tgt_v.fingerprint_sha256:
                ok, reason = check_structural_schema_compatibility(
                    src_v, tgt_v, src_subj.compatibility
                )
                findings.append(
                    MetadataFinding(
                        component="schema_registry",
                        badge=CareBadge.ACTION,
                        code="SCHEMA_VERSION_DRIFT",
                        message=(
                            f"subject {subj_name!r} version {src_v.version} canonical schema "
                            f"differs on target ({reason})"
                        ),
                    )
                )

        sorted_tgt_versions = sorted(tgt_subj.versions, key=lambda x: x.version)
        for idx in range(1, len(sorted_tgt_versions)):
            prev_v = sorted_tgt_versions[idx - 1]
            curr_v = sorted_tgt_versions[idx]
            ok, reason = check_structural_schema_compatibility(
                prev_v, curr_v, tgt_subj.compatibility
            )
            if not ok:
                findings.append(
                    MetadataFinding(
                        component="schema_registry",
                        badge=CareBadge.ACTION,
                        code="INCOMPATIBLE_SCHEMA_EVOLUTION",
                        message=(
                            f"subject {subj_name!r} v{prev_v.version}->v{curr_v.version} "
                            f"violates {tgt_subj.compatibility.value}: {reason}"
                        ),
                    )
                )

    for subj_name, tgt_subj in sorted(target_subjects.items()):
        for v in tgt_subj.versions:
            target_by_id[v.global_schema_id] = v

    wire_ids = tuple(sorted(set(int(i) for i in embedded_wire_schema_ids)))
    for wire_id in wire_ids:
        mapped_id = schema_id_remap.get(wire_id, wire_id) if schema_id_remap else wire_id
        if schema_id_remap and mapped_id != wire_id:
            findings.append(
                MetadataFinding(
                    component="schema_registry",
                    badge=CareBadge.ACTION,
                    code="UNREWRITTEN_WIRE_SCHEMA_ID",
                    message=(
                        f"stream bytes carry embedded schema ID {wire_id}, which remaps to "
                        f"{mapped_id} on target without payload rewrite or IMPORT mode"
                    ),
                )
            )
            continue

        tgt_schema = target_by_id.get(wire_id)
        src_schema = source_by_id.get(wire_id)
        if tgt_schema is None:
            findings.append(
                MetadataFinding(
                    component="schema_registry",
                    badge=CareBadge.ACTION,
                    code="MISSING_WIRE_SCHEMA_ID",
                    message=(
                        f"embedded wire schema ID {wire_id} present in stream bytes "
                        f"does not exist in target Schema Registry"
                    ),
                )
            )
        elif src_schema is not None and src_schema.fingerprint_sha256 != tgt_schema.fingerprint_sha256:
            findings.append(
                MetadataFinding(
                    component="schema_registry",
                    badge=CareBadge.ACTION,
                    code="WIRE_SCHEMA_ID_COLLISION",
                    message=(
                        f"embedded wire schema ID {wire_id} resolves to a different schema "
                        f"definition on target (subject={tgt_schema.subject!r}) than source "
                        f"(subject={src_schema.subject!r})"
                    ),
                )
            )

    if not findings:
        findings.append(
            MetadataFinding(
                component="schema_registry",
                badge=CareBadge.PASS,
                code="SCHEMA_REGISTRY_VERIFIED",
                message="all schema subjects, compatibility policies, and wire IDs verified",
            )
        )

    badge = _worst_badge(f.badge for f in findings)
    certified = badge != CareBadge.ACTION
    return SchemaRegistryProof(
        certified=certified,
        badge=badge,
        subjects_checked=tuple(subjects_checked),
        wire_schema_ids_verified=wire_ids,
        findings=tuple(findings),
    )


# -----------------------------------------------------------------------------
# ACL Policy Verification
# -----------------------------------------------------------------------------

def verify_acl_policy(
    source_acls: Sequence[AclBinding],
    target_acls: Sequence[AclBinding],
    principal_remap: Optional[Dict[str, str]] = None,
    host_remap: Optional[Dict[str, str]] = None,
    approved_extra_target_acls: Optional[Set[Tuple[str, str, str, str, str, str, str]]] = None,
) -> AclPolicyProof:
    """Verify source ACL policy is preserved on target without privilege widening or dropped DENY."""

    remap_p = principal_remap or {}
    remap_h = host_remap or {}
    approved_extra = approved_extra_target_acls or set()

    expected_bindings: Dict[Tuple[str, str, str, str, str, str, str], AclBinding] = {}
    for acl in source_acls:
        mapped_binding = AclBinding(
            principal=remap_p.get(acl.principal, acl.principal),
            resource_type=acl.resource_type,
            resource_name=acl.resource_name,
            operation=acl.operation,
            permission_type=acl.permission_type,
            pattern_type=acl.pattern_type,
            host=remap_h.get(acl.host, acl.host),
        )
        expected_bindings[mapped_binding.signature()] = mapped_binding

    actual_bindings: Dict[Tuple[str, str, str, str, str, str, str], AclBinding] = {
        acl.signature(): acl for acl in target_acls
    }

    findings: List[MetadataFinding] = []
    missing_allow = 0
    missing_deny = 0
    widening_count = 0

    for sig, exp in sorted(expected_bindings.items()):
        if sig not in actual_bindings:
            if exp.permission_type == AclPermissionType.DENY:
                missing_deny += 1
                widening_count += 1
                findings.append(
                    MetadataFinding(
                        component="acl_policy",
                        badge=CareBadge.ACTION,
                        code="MISSING_DENY_ACL",
                        message=(
                            f"source DENY rule missing on target for principal={exp.principal!r} "
                            f"resource={exp.resource_type.value}:{exp.resource_name!r} "
                            f"op={exp.operation.value}"
                        ),
                    )
                )
            else:
                missing_allow += 1
                findings.append(
                    MetadataFinding(
                        component="acl_policy",
                        badge=CareBadge.ACTION,
                        code="MISSING_ALLOW_ACL",
                        message=(
                            f"required ALLOW rule missing on target for principal={exp.principal!r} "
                            f"resource={exp.resource_type.value}:{exp.resource_name!r} "
                            f"op={exp.operation.value}"
                        ),
                    )
                )

    for sig, act in sorted(actual_bindings.items()):
        if sig in expected_bindings or sig in approved_extra:
            continue
        if act.permission_type == AclPermissionType.ALLOW:
            widening_count += 1
            findings.append(
                MetadataFinding(
                    component="acl_policy",
                    badge=CareBadge.ACTION,
                    code="UNAPPROVED_PRIVILEGE_WIDENING",
                    message=(
                        f"unapproved target ALLOW rule widens privileges: "
                        f"principal={act.principal!r} "
                        f"resource={act.resource_type.value}:{act.resource_name!r} "
                        f"pattern={act.pattern_type.value} op={act.operation.value} "
                        f"host={act.host!r}"
                    ),
                )
            )
        else:
            findings.append(
                MetadataFinding(
                    component="acl_policy",
                    badge=CareBadge.REVIEW,
                    code="EXTRA_TARGET_DENY_ACL",
                    message=(
                        f"additional DENY rule present on target: principal={act.principal!r} "
                        f"resource={act.resource_type.value}:{act.resource_name!r} "
                        f"op={act.operation.value}"
                    ),
                )
            )

    if not findings:
        findings.append(
            MetadataFinding(
                component="acl_policy",
                badge=CareBadge.PASS,
                code="ACL_POLICY_VERIFIED",
                message="target ACL policy matches mapped source policy with zero widening",
            )
        )

    badge = _worst_badge(f.badge for f in findings)
    certified = (
        missing_allow == 0
        and missing_deny == 0
        and widening_count == 0
        and badge != CareBadge.ACTION
    )
    return AclPolicyProof(
        certified=certified,
        badge=badge,
        missing_allow_count=missing_allow,
        missing_deny_count=missing_deny,
        unapproved_widening_count=widening_count,
        findings=tuple(findings),
    )


# -----------------------------------------------------------------------------
# End-to-End Stream Migration Proof Builder
# -----------------------------------------------------------------------------

def evaluate_stream_migration_proof(
    source_topic: TopicConfigSpec,
    target_topic: TopicConfigSpec,
    link: ReplicationLinkSpec,
    consumer_groups: Sequence[ConsumerGroupSpec],
    source_subjects: Dict[str, SchemaSubjectState],
    target_subjects: Dict[str, SchemaSubjectState],
    source_acls: Sequence[AclBinding],
    target_acls: Sequence[AclBinding],
    restart_offsets_by_group: Optional[Dict[str, Dict[int, int]]] = None,
    approved_max_replay: int = 0,
    allow_remapped_offsets: bool = True,
    principal_remap: Optional[Dict[str, str]] = None,
    host_remap: Optional[Dict[str, str]] = None,
    schema_id_remap: Optional[Dict[int, int]] = None,
) -> StreamMigrationProof:
    """Build the complete streaming migration proof across bytes, configs, offsets, schemas, and ACLs.

    Crucially, even when source and target partition payload bytes are 100% identical
    (byte_equal == True), incompatible schema state, missing/widened ACL state, out-of-bounds
    consumer offsets, or restart discontinuities cause certified=False with CareBadge.ACTION.
    """

    topic_proof = verify_topic_and_retained_ranges(
        source=source_topic,
        target=target_topic,
        capability=link.capability,
    )

    embedded_ids: Set[int] = set()
    for p in source_topic.partitions:
        embedded_ids.update(p.embedded_schema_ids)
    for p in target_topic.partitions:
        embedded_ids.update(p.embedded_schema_ids)

    schema_proof = verify_schema_registry_state(
        source_subjects=source_subjects,
        target_subjects=target_subjects,
        embedded_wire_schema_ids=embedded_ids,
        schema_id_remap=schema_id_remap,
    )

    acl_proof = verify_acl_policy(
        source_acls=source_acls,
        target_acls=target_acls,
        principal_remap=principal_remap,
        host_remap=host_remap,
    )

    all_findings: List[MetadataFinding] = []
    all_findings.extend(topic_proof.findings)
    all_findings.extend(schema_proof.findings)
    all_findings.extend(acl_proof.findings)

    translated_offsets: List[TranslatedConsumerOffset] = []
    restart_verifications: List[ConsumerRestartVerification] = []

    for grp in consumer_groups:
        try:
            grp_offsets = translate_and_validate_consumer_offsets(
                group=grp,
                source_topic=source_topic,
                target_topic=target_topic,
                link=link,
                allow_remapped=allow_remapped_offsets,
                raise_on_out_of_bounds=False,
            )
            translated_offsets.extend(grp_offsets)
            for toff in grp_offsets:
                if not toff.within_retention:
                    all_findings.append(
                        MetadataFinding(
                            component="consumer_offsets",
                            badge=CareBadge.ACTION,
                            code="OFFSET_OUT_OF_RETENTION_BOUNDS",
                            message=toff.reason,
                        )
                    )
        except StreamMetadataError as exc:
            all_findings.append(
                MetadataFinding(
                    component="consumer_offsets",
                    badge=CareBadge.ACTION,
                    code="OFFSET_CAPABILITY_OR_TOPOLOGY_ERROR",
                    message=str(exc),
                )
            )

        if restart_offsets_by_group and grp.group_id in restart_offsets_by_group:
            expected_src = {
                pid: off.committed_offset
                for (t_name, pid), off in grp.offsets.items()
                if t_name == source_topic.topic_name
            }
            actual_tgt = restart_offsets_by_group[grp.group_id]
            try:
                restarts = verify_consumer_restart(
                    group_id=grp.group_id,
                    topic_name=source_topic.topic_name,
                    target_topic=target_topic,
                    link=link,
                    expected_source_offsets=expected_src,
                    actual_target_restart_offsets=actual_tgt,
                    approved_max_replay=approved_max_replay,
                    raise_on_discontinuity=False,
                )
                restart_verifications.extend(restarts)
                for rv in restarts:
                    if not rv.certified:
                        all_findings.append(
                            MetadataFinding(
                                component="consumer_restart",
                                badge=CareBadge.ACTION,
                                code="CONSUMER_RESTART_DISCONTINUITY",
                                message=rv.detail,
                            )
                        )
            except StreamMetadataError as exc:
                all_findings.append(
                    MetadataFinding(
                        component="consumer_restart",
                        badge=CareBadge.ACTION,
                        code="CONSUMER_RESTART_ERROR",
                        message=str(exc),
                    )
                )

    badge = _worst_badge(f.badge for f in all_findings)
    offsets_ok = all(t.within_retention for t in translated_offsets)
    restarts_ok = all(r.certified for r in restart_verifications)
    certified = (
        topic_proof.certified
        and schema_proof.certified
        and acl_proof.certified
        and offsets_ok
        and restarts_ok
        and badge != CareBadge.ACTION
    )

    return StreamMigrationProof(
        topic_name=source_topic.topic_name,
        certified=certified,
        badge=badge,
        capability=link.capability,
        byte_equal=topic_proof.byte_equal,
        topic_proof=topic_proof,
        schema_proof=schema_proof,
        acl_proof=acl_proof,
        translated_offsets=tuple(translated_offsets),
        restart_verifications=tuple(restart_verifications),
        findings=tuple(all_findings),
    )


# -----------------------------------------------------------------------------
# Coordinated Promotion Controller with Fences and Active Consumers
# -----------------------------------------------------------------------------

class StreamPromotionCoordinator:
    """Coordinates stream promotion with producer fences, lag drain, and active consumer quiescence."""

    def __init__(
        self,
        source_topic: TopicConfigSpec,
        target_topic: TopicConfigSpec,
        link: ReplicationLinkSpec,
        consumer_groups: Sequence[ConsumerGroupSpec],
        source_subjects: Dict[str, SchemaSubjectState],
        target_subjects: Dict[str, SchemaSubjectState],
        source_acls: Sequence[AclBinding],
        target_acls: Sequence[AclBinding],
        checkpoint_store: Optional[StreamMetadataCheckpointStore] = None,
        allow_remapped_offsets: bool = True,
        approved_max_replay: int = 0,
    ) -> None:
        self.source_topic = source_topic
        self.target_topic = target_topic
        self.link = link
        self.consumer_groups: Dict[str, ConsumerGroupSpec] = {
            g.group_id: g for g in consumer_groups
        }
        self.source_subjects = dict(source_subjects)
        self.target_subjects = dict(target_subjects)
        self.source_acls = list(source_acls)
        self.target_acls = list(target_acls)
        self.checkpoint_store = checkpoint_store
        self.allow_remapped_offsets = allow_remapped_offsets
        self.approved_max_replay = approved_max_replay

        self.phase: PromotionPhase = PromotionPhase.INIT
        self.source_producers_fenced: bool = not source_topic.writable
        self.target_promoted_writable: bool = target_topic.writable and not target_topic.is_mirror_topic
        self.committed_target_offsets: Dict[str, Dict[int, int]] = {}
        self.restarted_consumer_offsets: Dict[str, Dict[int, int]] = {}
        self.transition_ledger: List[Dict[str, Any]] = []

    def _record_transition(self, target_phase: PromotionPhase, detail: str) -> None:
        self.phase = target_phase
        self.transition_ledger.append(
            {
                "phase": target_phase.value,
                "detail": detail,
                "timestamp": time.time(),
            }
        )

    def fence_source_producers(self) -> None:
        self.source_producers_fenced = True
        self.source_topic = dataclasses.replace(self.source_topic, writable=False)
        self._record_transition(
            PromotionPhase.FENCE_SOURCE_PRODUCERS,
            f"fenced producers on source topic {self.source_topic.topic_name!r}",
        )

    def drain_mirror_lag(self) -> int:
        if not self.source_producers_fenced:
            raise PromotionFenceError(
                "cannot drain mirror lag before fencing source producers"
            )
        remaining = self.link.total_mirror_lag(self.source_topic.topic_name)
        if remaining > 0:
            raise PromotionFenceError(
                f"mirror link {self.link.link_id!r} still has {remaining} un-replicated "
                f"messages for topic {self.source_topic.topic_name!r}"
            )
        self._record_transition(
            PromotionPhase.DRAIN_MIRROR_LAG,
            f"mirror lag drained to 0 on link {self.link.link_id!r}",
        )
        return 0

    def quiesce_consumer_group(self, group_id: str) -> None:
        grp = self.consumer_groups[group_id]
        grp.active_member_hosts.clear()
        grp.state = ConsumerGroupState.QUIESCED
        grp.fenced = True

    def verify_consumers_quiesced(self) -> None:
        if self.phase not in (
            PromotionPhase.DRAIN_MIRROR_LAG,
            PromotionPhase.QUIESCE_ACTIVE_CONSUMERS,
        ):
            raise PromotionFenceError(
                f"cannot verify consumer quiescence from phase {self.phase.value}"
            )
        for gid, grp in sorted(self.consumer_groups.items()):
            if grp.has_active_consumers or not grp.fenced:
                raise ActiveConsumerFenceError(
                    f"consumer group {gid!r} is still active or unfenced "
                    f"(state={grp.state.value}, active_members={len(grp.active_member_hosts)}, "
                    f"fenced={grp.fenced}); refuse offset cutover while consumers poll"
                )
        self._record_transition(
            PromotionPhase.QUIESCE_ACTIVE_CONSUMERS,
            f"all {len(self.consumer_groups)} consumer groups fenced and quiesced",
        )

    def translate_and_stage_offsets(self) -> Dict[str, List[TranslatedConsumerOffset]]:
        if self.phase != PromotionPhase.QUIESCE_ACTIVE_CONSUMERS:
            raise PromotionFenceError(
                f"offsets can only be translated after QUIESCE_ACTIVE_CONSUMERS "
                f"(current phase: {self.phase.value})"
            )
        staged: Dict[str, List[TranslatedConsumerOffset]] = {}
        for gid, grp in sorted(self.consumer_groups.items()):
            if grp.has_active_consumers or not grp.fenced:
                raise ActiveConsumerFenceError(
                    f"consumer group {gid!r} became active during offset translation"
                )
            translated = translate_and_validate_consumer_offsets(
                group=grp,
                source_topic=self.source_topic,
                target_topic=self.target_topic,
                link=self.link,
                allow_remapped=self.allow_remapped_offsets,
                raise_on_out_of_bounds=True,
            )
            staged[gid] = translated
            self.committed_target_offsets[gid] = {}
            for item in translated:
                if self.checkpoint_store is not None and self.checkpoint_store.is_completed(
                    "consumer_offset", gid, item.topic_name, item.partition_id
                ):
                    saved = self.checkpoint_store.get_cursor(
                        "consumer_offset", gid, item.topic_name, item.partition_id
                    )
                    assert saved is not None
                    self.committed_target_offsets[gid][item.partition_id] = int(
                        saved["state"]["target_committed_offset"]
                    )
                    continue

                self.committed_target_offsets[gid][item.partition_id] = (
                    item.target_committed_offset
                )
                if self.checkpoint_store is not None:
                    self.checkpoint_store.record_completed(
                        "consumer_offset",
                        (gid, item.topic_name, item.partition_id),
                        {
                            "source_committed_offset": item.source_committed_offset,
                            "target_committed_offset": item.target_committed_offset,
                            "capability": item.capability_used.value,
                        },
                    )

        self._record_transition(
            PromotionPhase.TRANSLATE_AND_VERIFY_OFFSETS,
            f"translated and staged offsets for {len(staged)} consumer groups",
        )
        return staged

    def promote_target_topic(self) -> StreamMigrationProof:
        if self.phase != PromotionPhase.TRANSLATE_AND_VERIFY_OFFSETS:
            raise PromotionFenceError(
                f"cannot promote target topic from phase {self.phase.value}"
            )
        if not self.source_producers_fenced:
            raise PromotionFenceError("source producers must remain fenced during promotion")
        if self.link.total_mirror_lag(self.source_topic.topic_name) > 0:
            raise PromotionFenceError("mirror lag became non-zero prior to target promotion")

        proof = evaluate_stream_migration_proof(
            source_topic=self.source_topic,
            target_topic=self.target_topic,
            link=self.link,
            consumer_groups=list(self.consumer_groups.values()),
            source_subjects=self.source_subjects,
            target_subjects=self.target_subjects,
            source_acls=self.source_acls,
            target_acls=self.target_acls,
            allow_remapped_offsets=self.allow_remapped_offsets,
            approved_max_replay=self.approved_max_replay,
        )
        if not proof.certified:
            self._record_transition(
                PromotionPhase.FAILED,
                f"promotion blocked by pre-promotion proof failure: {proof.badge.value}",
            )
            if not proof.schema_proof.certified:
                raise SchemaCompatibilityError(
                    "promotion refused: schema registry proof failed despite topic state"
                )
            if not proof.acl_proof.certified:
                raise AclPolicyViolationError(
                    "promotion refused: ACL policy proof failed despite topic state"
                )
            raise StreamMetadataError("promotion refused: stream migration proof not certified")

        self.target_topic = dataclasses.replace(
            self.target_topic,
            is_mirror_topic=False,
            writable=True,
        )
        self.target_promoted_writable = True
        self._record_transition(
            PromotionPhase.PROMOTE_TARGET_TOPIC,
            f"promoted target topic {self.target_topic.topic_name!r} to writable primary",
        )
        return proof

    def resume_consumers_and_verify(
        self,
        actual_restart_offsets: Optional[Dict[str, Dict[int, int]]] = None,
    ) -> StreamMigrationProof:
        if self.phase != PromotionPhase.PROMOTE_TARGET_TOPIC:
            raise PromotionFenceError(
                f"cannot resume consumers before PROMOTE_TARGET_TOPIC "
                f"(current phase: {self.phase.value})"
            )

        restart_map = actual_restart_offsets or self.committed_target_offsets
        for gid, grp in sorted(self.consumer_groups.items()):
            expected_src = {
                pid: off.committed_offset
                for (t_name, pid), off in grp.offsets.items()
                if t_name == self.source_topic.topic_name
            }
            actual_tgt = restart_map.get(gid, {})
            verify_consumer_restart(
                group_id=gid,
                topic_name=self.source_topic.topic_name,
                target_topic=self.target_topic,
                link=self.link,
                expected_source_offsets=expected_src,
                actual_target_restart_offsets=actual_tgt,
                approved_max_replay=self.approved_max_replay,
                raise_on_discontinuity=True,
            )
            self.restarted_consumer_offsets[gid] = dict(actual_tgt)
            grp.fenced = False
            grp.state = ConsumerGroupState.STABLE

        self._record_transition(
            PromotionPhase.RESUME_CONSUMERS,
            "resumed consumers on target with verified zero unaccounted gap/replay",
        )

        final_proof = evaluate_stream_migration_proof(
            source_topic=self.source_topic,
            target_topic=self.target_topic,
            link=self.link,
            consumer_groups=list(self.consumer_groups.values()),
            source_subjects=self.source_subjects,
            target_subjects=self.target_subjects,
            source_acls=self.source_acls,
            target_acls=self.target_acls,
            restart_offsets_by_group=self.restarted_consumer_offsets,
            allow_remapped_offsets=self.allow_remapped_offsets,
            approved_max_replay=self.approved_max_replay,
        )
        self._record_transition(
            PromotionPhase.COMPLETED,
            f"promotion completed with badge {final_proof.badge.value}",
        )
        return final_proof
