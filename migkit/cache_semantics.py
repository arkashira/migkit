import decimal
import enum
import fcntl
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

SAFE_TEST_HOSTS = {"127.0.0.1", "localhost", "0.0.0.0", "::1"}


class CacheSemanticsError(Exception):
    pass


class UnsafeTestHostError(CacheSemanticsError):
    pass


class LeaseLockError(CacheSemanticsError):
    pass


class LateFullResyncForbiddenError(CacheSemanticsError):
    pass


class ReverseSyncFlushForbiddenError(CacheSemanticsError):
    pass


class RoutingSyncCouplingError(CacheSemanticsError):
    pass


class StaleClientEpochError(CacheSemanticsError):
    pass


class FidelityUnavailableError(CacheSemanticsError):
    pass


def is_safe_test_host(host: str) -> bool:
    h = str(host).strip().lower()
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
            f"unsafe host rejected: {host} (allowed: 127.0.0.1, localhost, 10.0.0.x)"
        )
    return h


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class TTLPolicy(str, enum.Enum):
    PERSISTENT = "persistent"
    ABSOLUTE_TIMESTAMP = "absolute_timestamp"
    RELATIVE_COMPENSATED = "relative_compensated"


class CacheDataType(str, enum.Enum):
    STRING = "string"
    COUNTER = "counter"
    HASH = "hash"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    STREAM = "stream"
    JSON = "json"
    HYPERLOGLOG = "hyperloglog"
    BITMAP = "bitmap"


class EvictionPolicy(str, enum.Enum):
    NOEVICTION = "noeviction"
    ALLKEYS_LRU = "allkeys-lru"
    VOLATILE_LRU = "volatile-lru"
    ALLKEYS_LFU = "allkeys-lfu"
    VOLATILE_LFU = "volatile-lfu"
    ALLKEYS_RANDOM = "allkeys-random"
    VOLATILE_RANDOM = "volatile-random"
    VOLATILE_TTL = "volatile-ttl"

    @property
    def uses_lru(self) -> bool:
        return self in (EvictionPolicy.ALLKEYS_LRU, EvictionPolicy.VOLATILE_LRU)

    @property
    def uses_lfu(self) -> bool:
        return self in (EvictionPolicy.ALLKEYS_LFU, EvictionPolicy.VOLATILE_LFU)

    @property
    def evicts_all_keys(self) -> bool:
        return self in (
            EvictionPolicy.ALLKEYS_LRU,
            EvictionPolicy.ALLKEYS_LFU,
            EvictionPolicy.ALLKEYS_RANDOM,
        )

    @property
    def evicts_volatile_only(self) -> bool:
        return self in (
            EvictionPolicy.VOLATILE_LRU,
            EvictionPolicy.VOLATILE_LFU,
            EvictionPolicy.VOLATILE_RANDOM,
            EvictionPolicy.VOLATILE_TTL,
        )


class SyncMode(str, enum.Enum):
    DISCONNECTED = "disconnected"
    INITIAL_SNAPSHOT = "initial_snapshot"
    CONTINUOUS_STREAM = "continuous_stream"
    ACTIVE_ACTIVE_CRDT = "active_active_crdt"
    PROMOTED_STANDALONE = "promoted_standalone"
    REVERSE_REPLICATION = "reverse_replication"


class ClientRouteTarget(str, enum.Enum):
    SOURCE = "source"
    FENCED = "fenced"
    TARGET = "target"
    ACTIVE_ACTIVE_LOCAL = "active_active_local"


class LeaseLock:
    def __init__(self, lock_dir: Path, owner_id: str = "cache-semantics", ttl_sec: float = 30.0):
        self.lock_dir = Path(lock_dir)
        self.lock_path = self.lock_dir / ".run.lock"
        self.owner_id = owner_id
        self.ttl_sec = ttl_sec
        self._fd: Optional[int] = None
        self.acquired_at: Optional[float] = None
        self.last_heartbeat: Optional[float] = None

    def acquire(self) -> None:
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise LeaseLockError(f"active verification lease lock held: {self.lock_path}") from exc
        now = time.time()
        self._fd = fd
        self.acquired_at = now
        self.last_heartbeat = now
        self._write_metadata()

    def heartbeat(self) -> None:
        if self._fd is None:
            raise LeaseLockError("cannot heartbeat an unacquired lease lock")
        self.last_heartbeat = time.time()
        self._write_metadata()

    def _write_metadata(self) -> None:
        if self._fd is None:
            return
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        payload = json.dumps(
            {
                "owner_id": self.owner_id,
                "pid": os.getpid(),
                "acquired_at": self.acquired_at,
                "last_heartbeat": self.last_heartbeat,
                "ttl_sec": self.ttl_sec,
            },
            sort_keys=True,
        ).encode("ascii")
        os.write(self._fd, payload)
        os.fsync(self._fd)

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None

    def __enter__(self) -> "LeaseLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


def _encode_typed_value(val: Any) -> Any:
    if isinstance(val, datetime):
        return {"__type__": "datetime", "value": val.isoformat()}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "value": str(val)}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "value": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "value": [_encode_typed_value(x) for x in val]}
    if isinstance(val, list):
        return [_encode_typed_value(x) for x in val]
    if isinstance(val, dict):
        return {str(k): _encode_typed_value(v) for k, v in val.items()}
    return val


def _decode_typed_value(val: Any) -> Any:
    if isinstance(val, dict):
        t = val.get("__type__")
        if t == "datetime":
            return datetime.fromisoformat(val["value"])
        if t == "uuid":
            return uuid.UUID(val["value"])
        if t == "decimal":
            return decimal.Decimal(val["value"])
        if t == "tuple":
            return tuple(_decode_typed_value(x) for x in val["value"])
        return {k: _decode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_decode_typed_value(x) for x in val]
    return val


class CacheCheckpointStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.completed_units: Dict[str, Dict[str, Any]] = {}
        if self.path.exists():
            self.load()

    def mark_completed(self, unit_id: str, cursor_boundary: Any, keys_processed: int) -> None:
        self.completed_units[unit_id] = {
            "cursor_boundary": _encode_typed_value(cursor_boundary),
            "keys_processed": int(keys_processed),
            "completed_at": time.time(),
        }
        self.save()

    def is_completed(self, unit_id: str) -> bool:
        return unit_id in self.completed_units

    def get_cursor_boundary(self, unit_id: str) -> Any:
        entry = self.completed_units.get(unit_id)
        if entry is None:
            return None
        return _decode_typed_value(entry["cursor_boundary"])

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(".tmp")
        payload = json.dumps(
            {"completed_units": self.completed_units},
            sort_keys=True,
            ensure_ascii=True,
        )
        tmp_path.write_text(payload, encoding="ascii")
        os.replace(str(tmp_path), str(self.path))

    def load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="ascii"))
        self.completed_units = dict(raw.get("completed_units", {}))


@dataclass
class AccessMetadata:
    idletime_sec: Optional[int] = None
    freq_counter: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "idletime_sec": self.idletime_sec,
            "freq_counter": self.freq_counter,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AccessMetadata":
        return cls(
            idletime_sec=int(data["idletime_sec"]) if data.get("idletime_sec") is not None else None,
            freq_counter=int(data["freq_counter"]) if data.get("freq_counter") is not None else None,
        )


@dataclass
class CacheKeyRecord:
    key: str
    data_type: CacheDataType
    value: Any
    ttl_ms: Optional[int] = None
    expire_at_ms: Optional[int] = None
    read_mono_ms: int = 0
    read_wall_ms: int = 0
    access: AccessMetadata = field(default_factory=AccessMetadata)
    version_vector: Dict[str, int] = field(default_factory=dict)
    updated_at_ms: int = 0
    site_id: str = "site-a"
    tombstone: bool = False
    observed_adds: Dict[str, Set[str]] = field(default_factory=dict)
    field_timestamps: Dict[str, Tuple[int, str]] = field(default_factory=dict)

    @property
    def is_persistent(self) -> bool:
        return self.ttl_ms is None and self.expire_at_ms is None

    def clone(self) -> "CacheKeyRecord":
        val_copy = json.loads(json.dumps(self.value)) if not isinstance(self.value, set) else set(self.value)
        obs_copy = {elem: set(tags) for elem, tags in self.observed_adds.items()}
        ft_copy = dict(self.field_timestamps)
        return CacheKeyRecord(
            key=self.key,
            data_type=self.data_type,
            value=val_copy,
            ttl_ms=self.ttl_ms,
            expire_at_ms=self.expire_at_ms,
            read_mono_ms=self.read_mono_ms,
            read_wall_ms=self.read_wall_ms,
            access=AccessMetadata(
                idletime_sec=self.access.idletime_sec,
                freq_counter=self.access.freq_counter,
            ),
            version_vector=dict(self.version_vector),
            updated_at_ms=self.updated_at_ms,
            site_id=self.site_id,
            tombstone=self.tombstone,
            observed_adds=obs_copy,
            field_timestamps=ft_copy,
        )


@dataclass
class CompensatedTTLResult:
    key: str
    policy_used: TTLPolicy
    should_apply: bool
    expired_in_flight: bool
    near_expiry: bool
    target_ttl_ms: Optional[int]
    target_expire_at_ms: Optional[int]
    elapsed_ms: int
    clock_skew_ms: int
    skew_compensated: bool
    fidelity_preserved: bool
    care_badge: CareBadge
    reason: str


class TTLCompensator:
    def __init__(
        self,
        preferred_policy: TTLPolicy = TTLPolicy.ABSOLUTE_TIMESTAMP,
        near_expiry_window_ms: int = 500,
        max_uncompensated_skew_ms: int = 100,
        max_compensatable_skew_ms: int = 30000,
        min_apply_grace_ms: int = 5,
    ):
        self.preferred_policy = preferred_policy
        self.near_expiry_window_ms = int(near_expiry_window_ms)
        self.max_uncompensated_skew_ms = int(max_uncompensated_skew_ms)
        self.max_compensatable_skew_ms = int(max_compensatable_skew_ms)
        self.min_apply_grace_ms = int(min_apply_grace_ms)

    def compute_target_expiry(
        self,
        record: CacheKeyRecord,
        apply_mono_ms: int,
        target_wall_ms: int,
        source_wall_at_apply_ms: Optional[int] = None,
        target_supports_absttl: bool = True,
    ) -> CompensatedTTLResult:
        elapsed_ms = max(0, int(apply_mono_ms) - int(record.read_mono_ms))
        inferred_src_wall = (
            int(source_wall_at_apply_ms)
            if source_wall_at_apply_ms is not None
            else int(record.read_wall_ms) + elapsed_ms
        )
        clock_skew_ms = int(target_wall_ms) - inferred_src_wall

        if record.is_persistent:
            return CompensatedTTLResult(
                key=record.key,
                policy_used=TTLPolicy.PERSISTENT,
                should_apply=True,
                expired_in_flight=False,
                near_expiry=False,
                target_ttl_ms=None,
                target_expire_at_ms=None,
                elapsed_ms=elapsed_ms,
                clock_skew_ms=clock_skew_ms,
                skew_compensated=False,
                fidelity_preserved=True,
                care_badge=CareBadge.PASS,
                reason="persistent key requires no TTL",
            )

        if record.ttl_ms is not None:
            initial_remaining_ms = int(record.ttl_ms)
        else:
            initial_remaining_ms = int(record.expire_at_ms or 0) - int(record.read_wall_ms)

        remaining_ms = initial_remaining_ms - elapsed_ms

        if remaining_ms <= self.min_apply_grace_ms:
            return CompensatedTTLResult(
                key=record.key,
                policy_used=TTLPolicy.RELATIVE_COMPENSATED,
                should_apply=False,
                expired_in_flight=True,
                near_expiry=True,
                target_ttl_ms=0,
                target_expire_at_ms=int(target_wall_ms),
                elapsed_ms=elapsed_ms,
                clock_skew_ms=clock_skew_ms,
                skew_compensated=True,
                fidelity_preserved=True,
                care_badge=CareBadge.BENIGN,
                reason=f"key expired in flight (initial_ttl_ms={initial_remaining_ms}, elapsed_ms={elapsed_ms})",
            )

        is_near_expiry = remaining_ms <= self.near_expiry_window_ms
        abs_skew = abs(clock_skew_ms)

        if abs_skew > self.max_compensatable_skew_ms:
            return CompensatedTTLResult(
                key=record.key,
                policy_used=TTLPolicy.RELATIVE_COMPENSATED,
                should_apply=True,
                expired_in_flight=False,
                near_expiry=is_near_expiry,
                target_ttl_ms=remaining_ms,
                target_expire_at_ms=int(target_wall_ms) + remaining_ms,
                elapsed_ms=elapsed_ms,
                clock_skew_ms=clock_skew_ms,
                skew_compensated=True,
                fidelity_preserved=False,
                care_badge=CareBadge.ACTION,
                reason=(
                    f"clock skew ({clock_skew_ms}ms) exceeds max compensatable bound "
                    f"({self.max_compensatable_skew_ms}ms); absolute TTL fidelity unavailable"
                ),
            )

        use_abs = (
            self.preferred_policy == TTLPolicy.ABSOLUTE_TIMESTAMP
            and target_supports_absttl
        )

        if use_abs:
            src_expire_at = (
                int(record.expire_at_ms)
                if record.expire_at_ms is not None
                else int(record.read_wall_ms) + initial_remaining_ms
            )
            if abs_skew > self.max_uncompensated_skew_ms:
                compensated_expire_at = src_expire_at + clock_skew_ms
                skew_comp = True
                badge = CareBadge.REVIEW if is_near_expiry else CareBadge.PASS
                reason = (
                    f"absolute timestamp compensated for clock_skew_ms={clock_skew_ms} "
                    f"and elapsed_ms={elapsed_ms}"
                )
            else:
                compensated_expire_at = src_expire_at + clock_skew_ms
                skew_comp = abs_skew > 0
                badge = CareBadge.BENIGN if is_near_expiry else CareBadge.PASS
                reason = "absolute timestamp preserved with elapsed-time and clock alignment"
            return CompensatedTTLResult(
                key=record.key,
                policy_used=TTLPolicy.ABSOLUTE_TIMESTAMP,
                should_apply=True,
                expired_in_flight=False,
                near_expiry=is_near_expiry,
                target_ttl_ms=remaining_ms,
                target_expire_at_ms=compensated_expire_at,
                elapsed_ms=elapsed_ms,
                clock_skew_ms=clock_skew_ms,
                skew_compensated=skew_comp,
                fidelity_preserved=True,
                care_badge=badge,
                reason=reason,
            )

        badge = CareBadge.BENIGN if is_near_expiry else CareBadge.PASS
        if not target_supports_absttl and self.preferred_policy == TTLPolicy.ABSOLUTE_TIMESTAMP:
            reason = "target lacks ABSTTL; fell back to elapsed-time compensated relative PTTL"
        else:
            reason = f"relative TTL compensated by elapsed_ms={elapsed_ms}"

        return CompensatedTTLResult(
            key=record.key,
            policy_used=TTLPolicy.RELATIVE_COMPENSATED,
            should_apply=True,
            expired_in_flight=False,
            near_expiry=is_near_expiry,
            target_ttl_ms=remaining_ms,
            target_expire_at_ms=int(target_wall_ms) + remaining_ms,
            elapsed_ms=elapsed_ms,
            clock_skew_ms=clock_skew_ms,
            skew_compensated=True,
            fidelity_preserved=True,
            care_badge=badge,
            reason=reason,
        )


@dataclass
class FidelityDeclaration:
    preserved: bool
    source_policy: EvictionPolicy
    target_policy: EvictionPolicy
    carried_fields: List[str]
    unavailable_fields: List[str]
    care_badge: CareBadge
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "preserved": self.preserved,
            "source_policy": self.source_policy.value,
            "target_policy": self.target_policy.value,
            "carried_fields": list(self.carried_fields),
            "unavailable_fields": list(self.unavailable_fields),
            "care_badge": self.care_badge.value,
            "reason": self.reason,
        }


class EvictionFidelityEvaluator:
    @staticmethod
    def evaluate(
        source_policy: EvictionPolicy,
        target_policy: EvictionPolicy,
        supports_restore_idletime: bool = True,
        supports_restore_freq: bool = True,
        allow_declared_degradation: bool = True,
    ) -> FidelityDeclaration:
        carried: List[str] = ["ttl", "data_type"]
        unavailable: List[str] = []

        if source_policy.evicts_volatile_only and target_policy.evicts_all_keys:
            return FidelityDeclaration(
                preserved=False,
                source_policy=source_policy,
                target_policy=target_policy,
                carried_fields=carried,
                unavailable_fields=["persistent_key_immunity"],
                care_badge=CareBadge.ACTION,
                reason=(
                    f"target policy {target_policy.value} can evict persistent keys that "
                    f"source policy {source_policy.value} protected"
                ),
            )

        if source_policy != EvictionPolicy.NOEVICTION and target_policy == EvictionPolicy.NOEVICTION:
            return FidelityDeclaration(
                preserved=False,
                source_policy=source_policy,
                target_policy=target_policy,
                carried_fields=carried,
                unavailable_fields=["eviction_under_pressure"],
                care_badge=CareBadge.REVIEW,
                reason=(
                    f"target uses noeviction while source used {source_policy.value}; "
                    "writes will fail with OOM under memory pressure unless reconfigured"
                ),
            )

        if target_policy.uses_lru:
            if supports_restore_idletime:
                carried.append("lru_idletime")
            else:
                unavailable.append("lru_idletime")

        if target_policy.uses_lfu:
            if supports_restore_freq:
                carried.append("lfu_freq")
            else:
                unavailable.append("lfu_freq")

        if unavailable:
            badge = CareBadge.REVIEW if allow_declared_degradation else CareBadge.ACTION
            return FidelityDeclaration(
                preserved=False,
                source_policy=source_policy,
                target_policy=target_policy,
                carried_fields=carried,
                unavailable_fields=unavailable,
                care_badge=badge,
                reason=(
                    f"unavailable eviction metadata fidelity for {', '.join(unavailable)} "
                    f"under target policy {target_policy.value}"
                ),
            )

        if source_policy != target_policy:
            return FidelityDeclaration(
                preserved=False,
                source_policy=source_policy,
                target_policy=target_policy,
                carried_fields=carried,
                unavailable_fields=["policy_equivalence"],
                care_badge=CareBadge.REVIEW,
                reason=f"eviction policy changed from {source_policy.value} to {target_policy.value}",
            )

        return FidelityDeclaration(
            preserved=True,
            source_policy=source_policy,
            target_policy=target_policy,
            carried_fields=carried,
            unavailable_fields=[],
            care_badge=CareBadge.PASS,
            reason=f"eviction policy {target_policy.value} and access metadata preserved",
        )

    @staticmethod
    def compensate_access_metadata(
        access: AccessMetadata,
        elapsed_ms: int,
        supports_idletime: bool = True,
        supports_freq: bool = True,
    ) -> AccessMetadata:
        new_idle: Optional[int] = None
        new_freq: Optional[int] = None
        if supports_idletime and access.idletime_sec is not None:
            new_idle = max(0, int(access.idletime_sec) + int(elapsed_ms // 1000))
        if supports_freq and access.freq_counter is not None:
            new_freq = min(255, max(0, int(access.freq_counter)))
        return AccessMetadata(idletime_sec=new_idle, freq_counter=new_freq)


@dataclass
class MemoryPressureVerdict:
    used_memory_bytes: int
    maxmemory_bytes: int
    utilization_ratio: float
    evicted_keys_delta: int
    persistent_keys_evicted: int
    under_pressure: bool
    semantics_approved: bool
    care_badge: CareBadge
    reason: str


class MemoryPressureMonitor:
    def __init__(
        self,
        warning_ratio: float = 0.80,
        critical_ratio: float = 0.92,
        approve_volatile_eviction_under_pressure: bool = True,
    ):
        self.warning_ratio = float(warning_ratio)
        self.critical_ratio = float(critical_ratio)
        self.approve_volatile_eviction_under_pressure = bool(approve_volatile_eviction_under_pressure)

    def evaluate(
        self,
        used_memory_bytes: int,
        maxmemory_bytes: int,
        evicted_keys_delta: int,
        persistent_keys_evicted: int,
        target_policy: EvictionPolicy,
    ) -> MemoryPressureVerdict:
        if maxmemory_bytes <= 0:
            return MemoryPressureVerdict(
                used_memory_bytes=used_memory_bytes,
                maxmemory_bytes=maxmemory_bytes,
                utilization_ratio=0.0,
                evicted_keys_delta=evicted_keys_delta,
                persistent_keys_evicted=persistent_keys_evicted,
                under_pressure=False,
                semantics_approved=True,
                care_badge=CareBadge.PASS,
                reason="maxmemory unbounded; no memory pressure",
            )

        ratio = float(used_memory_bytes) / float(maxmemory_bytes)
        under_pressure = ratio >= self.warning_ratio or evicted_keys_delta > 0

        if persistent_keys_evicted > 0:
            return MemoryPressureVerdict(
                used_memory_bytes=used_memory_bytes,
                maxmemory_bytes=maxmemory_bytes,
                utilization_ratio=ratio,
                evicted_keys_delta=evicted_keys_delta,
                persistent_keys_evicted=persistent_keys_evicted,
                under_pressure=True,
                semantics_approved=False,
                care_badge=CareBadge.ACTION,
                reason=f"persistent keys evicted under memory pressure (count={persistent_keys_evicted})",
            )

        if evicted_keys_delta > 0:
            if (
                self.approve_volatile_eviction_under_pressure
                and target_policy != EvictionPolicy.NOEVICTION
            ):
                return MemoryPressureVerdict(
                    used_memory_bytes=used_memory_bytes,
                    maxmemory_bytes=maxmemory_bytes,
                    utilization_ratio=ratio,
                    evicted_keys_delta=evicted_keys_delta,
                    persistent_keys_evicted=0,
                    under_pressure=True,
                    semantics_approved=True,
                    care_badge=CareBadge.REVIEW,
                    reason=(
                        f"volatile eviction occurred under pressure (evicted={evicted_keys_delta}, "
                        f"policy={target_policy.value}); approved cache semantics preserved"
                    ),
                )
            return MemoryPressureVerdict(
                used_memory_bytes=used_memory_bytes,
                maxmemory_bytes=maxmemory_bytes,
                utilization_ratio=ratio,
                evicted_keys_delta=evicted_keys_delta,
                persistent_keys_evicted=0,
                under_pressure=True,
                semantics_approved=False,
                care_badge=CareBadge.ACTION,
                reason=f"unapproved key eviction during migration (evicted={evicted_keys_delta})",
            )

        if ratio >= self.critical_ratio and target_policy == EvictionPolicy.NOEVICTION:
            return MemoryPressureVerdict(
                used_memory_bytes=used_memory_bytes,
                maxmemory_bytes=maxmemory_bytes,
                utilization_ratio=ratio,
                evicted_keys_delta=0,
                persistent_keys_evicted=0,
                under_pressure=True,
                semantics_approved=False,
                care_badge=CareBadge.ACTION,
                reason=(
                    f"critical memory pressure ({ratio:.2%}) with noeviction policy; "
                    "imminent OOM write rejection"
                ),
            )

        if ratio >= self.warning_ratio:
            return MemoryPressureVerdict(
                used_memory_bytes=used_memory_bytes,
                maxmemory_bytes=maxmemory_bytes,
                utilization_ratio=ratio,
                evicted_keys_delta=0,
                persistent_keys_evicted=0,
                under_pressure=True,
                semantics_approved=True,
                care_badge=CareBadge.REVIEW,
                reason=f"elevated memory utilization ({ratio:.2%}) within configured policy",
            )

        return MemoryPressureVerdict(
            used_memory_bytes=used_memory_bytes,
            maxmemory_bytes=maxmemory_bytes,
            utilization_ratio=ratio,
            evicted_keys_delta=0,
            persistent_keys_evicted=0,
            under_pressure=False,
            semantics_approved=True,
            care_badge=CareBadge.PASS,
            reason=f"memory utilization nominal ({ratio:.2%})",
        )


@dataclass
class ActiveActiveMergeOutcome:
    key: str
    merged_record: CacheKeyRecord
    conflict_detected: bool
    resolution_rule: str
    care_badge: CareBadge
    details: str


class ActiveActiveCRDTResolver:
    @staticmethod
    def _vc_dominates(a: Dict[str, int], b: Dict[str, int]) -> bool:
        sites = set(a.keys()) | set(b.keys())
        strictly_greater = False
        for s in sites:
            va = a.get(s, 0)
            vb = b.get(s, 0)
            if va < vb:
                return False
            if va > vb:
                strictly_greater = True
        return strictly_greater

    @staticmethod
    def _vc_equal(a: Dict[str, int], b: Dict[str, int]) -> bool:
        sites = set(a.keys()) | set(b.keys())
        return all(a.get(s, 0) == b.get(s, 0) for s in sites)

    @staticmethod
    def _vc_merge(a: Dict[str, int], b: Dict[str, int]) -> Dict[str, int]:
        sites = set(a.keys()) | set(b.keys())
        return {s: max(a.get(s, 0), b.get(s, 0)) for s in sites}

    @classmethod
    def resolve(
        cls,
        local: CacheKeyRecord,
        remote: CacheKeyRecord,
        base_counter_value: int = 0,
    ) -> ActiveActiveMergeOutcome:
        merged_vc = cls._vc_merge(local.version_vector, remote.version_vector)

        if cls._vc_equal(local.version_vector, remote.version_vector) and (
            local.updated_at_ms == remote.updated_at_ms and local.site_id == remote.site_id
        ):
            rec = local.clone()
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=False,
                resolution_rule="idempotent_duplicate",
                care_badge=CareBadge.PASS,
                details="identical causal version",
            )

        if cls._vc_dominates(remote.version_vector, local.version_vector):
            rec = remote.clone()
            rec.version_vector = merged_vc
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=False,
                resolution_rule="causal_progression_remote",
                care_badge=CareBadge.PASS,
                details="remote version causally dominates local version",
            )

        if cls._vc_dominates(local.version_vector, remote.version_vector):
            rec = local.clone()
            rec.version_vector = merged_vc
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=False,
                resolution_rule="causal_progression_local",
                care_badge=CareBadge.PASS,
                details="local version causally dominates remote version",
            )

        lww_winner = (
            remote
            if (remote.updated_at_ms, remote.site_id) > (local.updated_at_ms, local.site_id)
            else local
        )

        if local.tombstone or remote.tombstone:
            rec = lww_winner.clone()
            rec.version_vector = merged_vc
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=True,
                resolution_rule="concurrent_tombstone_lww",
                care_badge=CareBadge.BENIGN,
                details=f"concurrent write vs delete resolved by LWW ({lww_winner.site_id})",
            )

        if local.data_type != remote.data_type:
            rec = lww_winner.clone()
            rec.version_vector = merged_vc
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=True,
                resolution_rule="type_conflict_lww_alert",
                care_badge=CareBadge.REVIEW,
                details=(
                    f"concurrent incompatible types ({local.data_type.value} vs "
                    f"{remote.data_type.value}); selected LWW winner {lww_winner.site_id}"
                ),
            )

        dtype = local.data_type

        if dtype == CacheDataType.COUNTER:
            delta_local = int(local.value) - int(base_counter_value)
            delta_remote = int(remote.value) - int(base_counter_value)
            merged_val = int(base_counter_value) + delta_local + delta_remote
            rec = lww_winner.clone()
            rec.value = merged_val
            rec.version_vector = merged_vc
            rec.expire_at_ms = cls._merge_expiry(local, remote)
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=True,
                resolution_rule="crdt_pn_counter_add",
                care_badge=CareBadge.PASS,
                details=f"PN-counter merged deltas ({delta_local:+d}, {delta_remote:+d}) -> {merged_val}",
            )

        if dtype == CacheDataType.SET:
            local_set = set(local.value)
            remote_set = set(remote.value)
            all_elems = local_set | remote_set | set(local.observed_adds.keys()) | set(remote.observed_adds.keys())
            merged_set: Set[str] = set()
            merged_obs: Dict[str, Set[str]] = {}
            for elem in all_elems:
                l_tags = local.observed_adds.get(elem, {f"init:{elem}"} if elem in local_set else set())
                r_tags = remote.observed_adds.get(elem, {f"init:{elem}"} if elem in remote_set else set())
                surviving_tags: Set[str] = set()
                if elem in local_set:
                    surviving_tags |= l_tags
                if elem in remote_set:
                    surviving_tags |= r_tags
                for tag in l_tags - r_tags:
                    if elem in local_set:
                        surviving_tags.add(tag)
                for tag in r_tags - l_tags:
                    if elem in remote_set:
                        surviving_tags.add(tag)
                if surviving_tags:
                    merged_set.add(elem)
                    merged_obs[elem] = surviving_tags

            rec = lww_winner.clone()
            rec.value = sorted(merged_set)
            rec.observed_adds = merged_obs
            rec.version_vector = merged_vc
            rec.expire_at_ms = cls._merge_expiry(local, remote)
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=True,
                resolution_rule="crdt_orset_add_wins",
                care_badge=CareBadge.PASS,
                details="OR-Set concurrent add/remove resolved with add-wins semantics",
            )

        if dtype == CacheDataType.HASH:
            local_dict = dict(local.value)
            remote_dict = dict(remote.value)
            all_fields = set(local_dict.keys()) | set(remote_dict.keys())
            merged_dict: Dict[str, Any] = {}
            merged_ft: Dict[str, Tuple[int, str]] = {}
            for f in all_fields:
                l_meta = local.field_timestamps.get(f, (local.updated_at_ms, local.site_id))
                r_meta = remote.field_timestamps.get(f, (remote.updated_at_ms, remote.site_id))
                if f in local_dict and f not in remote_dict:
                    if f not in remote.field_timestamps or l_meta >= r_meta:
                        merged_dict[f] = local_dict[f]
                        merged_ft[f] = l_meta
                elif f in remote_dict and f not in local_dict:
                    if f not in local.field_timestamps or r_meta >= l_meta:
                        merged_dict[f] = remote_dict[f]
                        merged_ft[f] = r_meta
                else:
                    if r_meta > l_meta:
                        merged_dict[f] = remote_dict[f]
                        merged_ft[f] = r_meta
                    else:
                        merged_dict[f] = local_dict[f]
                        merged_ft[f] = l_meta

            rec = lww_winner.clone()
            rec.value = merged_dict
            rec.field_timestamps = merged_ft
            rec.version_vector = merged_vc
            rec.expire_at_ms = cls._merge_expiry(local, remote)
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=True,
                resolution_rule="crdt_hash_field_lww",
                care_badge=CareBadge.PASS,
                details="Hash merged per field via LWW timestamps",
            )

        if dtype == CacheDataType.ZSET:
            local_z = dict(local.value)
            remote_z = dict(remote.value)
            all_members = set(local_z.keys()) | set(remote_z.keys())
            merged_z: Dict[str, float] = {}
            for m in all_members:
                l_meta = local.field_timestamps.get(m, (local.updated_at_ms, local.site_id))
                r_meta = remote.field_timestamps.get(m, (remote.updated_at_ms, remote.site_id))
                if m in local_z and m in remote_z:
                    merged_z[m] = float(remote_z[m] if r_meta > l_meta else local_z[m])
                elif m in local_z and (m not in remote.field_timestamps or l_meta >= r_meta):
                    merged_z[m] = float(local_z[m])
                elif m in remote_z and (m not in local.field_timestamps or r_meta >= l_meta):
                    merged_z[m] = float(remote_z[m])

            rec = lww_winner.clone()
            rec.value = merged_z
            rec.version_vector = merged_vc
            rec.expire_at_ms = cls._merge_expiry(local, remote)
            return ActiveActiveMergeOutcome(
                key=local.key,
                merged_record=rec,
                conflict_detected=True,
                resolution_rule="crdt_zset_member_lww",
                care_badge=CareBadge.PASS,
                details="ZSet merged per member score via LWW",
            )

        rec = lww_winner.clone()
        rec.version_vector = merged_vc
        rec.expire_at_ms = cls._merge_expiry(local, remote)
        return ActiveActiveMergeOutcome(
            key=local.key,
            merged_record=rec,
            conflict_detected=True,
            resolution_rule="crdt_register_lww",
            care_badge=CareBadge.BENIGN,
            details=f"LWW register resolved in favor of {lww_winner.site_id}",
        )

    @staticmethod
    def _merge_expiry(local: CacheKeyRecord, remote: CacheKeyRecord) -> Optional[int]:
        if local.is_persistent and remote.is_persistent:
            return None
        if local.expire_at_ms is not None and remote.expire_at_ms is not None:
            return max(local.expire_at_ms, remote.expire_at_ms)
        winner = (
            remote
            if (remote.updated_at_ms, remote.site_id) > (local.updated_at_ms, local.site_id)
            else local
        )
        return winner.expire_at_ms


@dataclass
class CacheEndpoint:
    name: str
    host: str
    port: int
    role: str = "master"
    writable: bool = True
    promoted: bool = False
    promotion_epoch: int = 0
    synced_up_to_offset: int = 0
    repl_offset: int = 0
    post_promotion_writes: int = 0
    eviction_policy: EvictionPolicy = EvictionPolicy.VOLATILE_LRU
    used_memory_bytes: int = 10 * 1024 * 1024
    maxmemory_bytes: int = 100 * 1024 * 1024
    evicted_keys_count: int = 0
    persistent_keys_evicted: int = 0
    supports_restore_absttl: bool = True
    supports_restore_idletime: bool = True
    supports_restore_freq: bool = True
    clock_offset_ms: int = 0
    keys: Dict[str, CacheKeyRecord] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_test_host(self.host)

    def wall_clock_ms(self, base_wall_ms: int) -> int:
        return int(base_wall_ms) + int(self.clock_offset_ms)

    def write_key(
        self,
        record: CacheKeyRecord,
        is_client_write: bool = True,
    ) -> None:
        if not self.writable:
            raise CacheSemanticsError(f"READONLY endpoint '{self.name}' rejected write to key '{record.key}'")
        self.keys[record.key] = record.clone()
        self.repl_offset += 1
        if is_client_write and self.promoted:
            self.post_promotion_writes += 1

    def delete_key(self, key: str) -> bool:
        if not self.writable:
            raise CacheSemanticsError(f"READONLY endpoint '{self.name}' rejected delete of key '{key}'")
        existed = self.keys.pop(key, None) is not None
        if existed:
            self.repl_offset += 1
            if self.promoted:
                self.post_promotion_writes += 1
        return existed

    def purge_expired(self, current_wall_ms: int) -> List[str]:
        expired: List[str] = []
        node_wall = self.wall_clock_ms(current_wall_ms)
        for k, rec in list(self.keys.items()):
            if rec.expire_at_ms is not None and rec.expire_at_ms <= node_wall:
                expired.append(k)
                del self.keys[k]
        return expired


@dataclass
class ClientConnection:
    client_id: str
    client_host: str
    connected_endpoint: str = "source"
    known_epoch: int = 0
    auto_redirect_enabled: bool = True
    last_seen_offset: int = 0

    def __post_init__(self) -> None:
        validate_test_host(self.client_host)


class DataSyncPlane:
    def __init__(
        self,
        source: CacheEndpoint,
        target: CacheEndpoint,
        compensator: Optional[TTLCompensator] = None,
        checkpoint_store: Optional[CacheCheckpointStore] = None,
    ):
        self.source = source
        self.target = target
        self.compensator = compensator or TTLCompensator()
        self.checkpoint_store = checkpoint_store
        self.sync_mode: SyncMode = SyncMode.DISCONNECTED
        self.replication_direction: str = "source->target"
        self.ttl_outcomes: Dict[str, CompensatedTTLResult] = {}

    def start_initial_sync(self) -> None:
        if self.target.promoted or self.target.post_promotion_writes > 0:
            raise LateFullResyncForbiddenError(
                f"forbid initial full-resync onto promoted/writable target '{self.target.name}' "
                f"(promoted={self.target.promoted}, post_promotion_writes={self.target.post_promotion_writes})"
            )
        self.target.role = "replica"
        self.target.writable = False
        self.sync_mode = SyncMode.INITIAL_SNAPSHOT
        self.replication_direction = "source->target"

    def attempt_full_resync_from_source(self) -> None:
        if (
            self.target.promoted
            or self.target.writable
            or self.target.post_promotion_writes > 0
            or self.sync_mode in (SyncMode.PROMOTED_STANDALONE, SyncMode.REVERSE_REPLICATION)
        ):
            raise LateFullResyncForbiddenError(
                f"late FULLRESYNC from '{self.source.name}' forbidden: target '{self.target.name}' "
                f"is promoted (epoch={self.target.promotion_epoch}, writable={self.target.writable}, "
                f"post_promotion_writes={self.target.post_promotion_writes})"
            )
        self.target.keys.clear()
        self.sync_mode = SyncMode.INITIAL_SNAPSHOT

    def sync_batch(
        self,
        unit_id: str,
        cursor_boundary: Any,
        keys_to_sync: List[str],
        read_mono_ms: int,
        apply_mono_ms: int,
        base_wall_ms: int,
    ) -> Dict[str, CompensatedTTLResult]:
        if self.checkpoint_store is not None and self.checkpoint_store.is_completed(unit_id):
            return {}

        if self.replication_direction != "source->target":
            raise CacheSemanticsError(
                f"cannot run forward sync_batch when direction is '{self.replication_direction}'"
            )

        src_wall_at_read = self.source.wall_clock_ms(base_wall_ms)
        elapsed_ms = max(0, int(apply_mono_ms) - int(read_mono_ms))
        src_wall_at_apply = src_wall_at_read + elapsed_ms
        tgt_wall_at_apply = self.target.wall_clock_ms(base_wall_ms + elapsed_ms)

        batch_outcomes: Dict[str, CompensatedTTLResult] = {}
        for k in keys_to_sync:
            src_rec = self.source.keys.get(k)
            if src_rec is None:
                self.target.keys.pop(k, None)
                continue

            sampled = src_rec.clone()
            sampled.read_mono_ms = int(read_mono_ms)
            sampled.read_wall_ms = int(src_wall_at_read)

            ttl_res = self.compensator.compute_target_expiry(
                record=sampled,
                apply_mono_ms=apply_mono_ms,
                target_wall_ms=tgt_wall_at_apply,
                source_wall_at_apply_ms=src_wall_at_apply,
                target_supports_absttl=self.target.supports_restore_absttl,
            )
            self.ttl_outcomes[k] = ttl_res
            batch_outcomes[k] = ttl_res

            if not ttl_res.should_apply:
                self.target.keys.pop(k, None)
                continue

            comp_access = EvictionFidelityEvaluator.compensate_access_metadata(
                access=sampled.access,
                elapsed_ms=elapsed_ms,
                supports_idletime=self.target.supports_restore_idletime,
                supports_freq=self.target.supports_restore_freq,
            )
            dst_rec = sampled.clone()
            dst_rec.ttl_ms = ttl_res.target_ttl_ms
            dst_rec.expire_at_ms = ttl_res.target_expire_at_ms
            dst_rec.access = comp_access
            self.target.keys[k] = dst_rec

        self.target.synced_up_to_offset = self.source.repl_offset
        self.target.repl_offset = self.source.repl_offset
        self.sync_mode = SyncMode.CONTINUOUS_STREAM

        if self.checkpoint_store is not None:
            self.checkpoint_store.mark_completed(
                unit_id=unit_id,
                cursor_boundary=cursor_boundary,
                keys_processed=len(keys_to_sync),
            )
        return batch_outcomes

    def promote_target_detach_sync(self, new_epoch: int) -> None:
        self.target.role = "master"
        self.target.writable = True
        self.target.promoted = True
        self.target.promotion_epoch = int(new_epoch)
        self.sync_mode = SyncMode.PROMOTED_STANDALONE
        self.replication_direction = "detached"

    def configure_reverse_replication(
        self,
        from_endpoint: CacheEndpoint,
        to_endpoint: CacheEndpoint,
        flush_destination: bool = False,
    ) -> CareBadge:
        if to_endpoint.promoted or to_endpoint.post_promotion_writes > 0:
            raise ReverseSyncFlushForbiddenError(
                f"reverse replication cannot target promoted writer '{to_endpoint.name}' "
                f"(post_promotion_writes={to_endpoint.post_promotion_writes}); would overwrite new writes"
            )
        if from_endpoint is not self.target or to_endpoint is not self.source:
            raise ReverseSyncFlushForbiddenError(
                "reverse replication must flow strictly from promoted target to fenced source"
            )
        if not self.source.writable and self.target.promoted:
            pass
        else:
            raise ReverseSyncFlushForbiddenError(
                "cannot start reverse replication before source is fenced and target is promoted"
            )
        if flush_destination and self.target.post_promotion_writes > 0 and to_endpoint is self.target:
            raise ReverseSyncFlushForbiddenError("forbid flushing new writes on promoted target")

        self.source.role = "replica"
        self.source.writable = False
        self.replication_direction = "target->source"
        self.sync_mode = SyncMode.REVERSE_REPLICATION

        for k, rec in self.target.keys.items():
            self.source.keys[k] = rec.clone()
        for k in list(self.source.keys.keys()):
            if k not in self.target.keys:
                del self.source.keys[k]

        self.source.synced_up_to_offset = self.target.repl_offset
        self.source.repl_offset = self.target.repl_offset
        return CareBadge.PASS


class ClientRoutingPlane:
    def __init__(self, source: CacheEndpoint, target: CacheEndpoint):
        self.source = source
        self.target = target
        self.route_target: ClientRouteTarget = ClientRouteTarget.SOURCE
        self.routing_epoch: int = 0
        self.clients: Dict[str, ClientConnection] = {}

    def register_client(self, client: ClientConnection) -> None:
        self.clients[client.client_id] = client

    def fence_source_writes(self) -> None:
        self.source.writable = False
        self.route_target = ClientRouteTarget.FENCED

    def switch_routing_to_target(self, sync_plane: DataSyncPlane, new_epoch: int) -> None:
        if not self.target.promoted or not self.target.writable or sync_plane.sync_mode != SyncMode.PROMOTED_STANDALONE:
            raise RoutingSyncCouplingError(
                "cannot switch client routing to target before target is promoted and detached from forward sync"
            )
        if self.source.writable:
            raise RoutingSyncCouplingError(
                "cannot switch client routing to target while source remains writable"
            )
        self.route_target = ClientRouteTarget.TARGET
        self.routing_epoch = int(new_epoch)

    def handle_client_command(
        self,
        client_id: str,
        is_write: bool,
        record: Optional[CacheKeyRecord] = None,
        key: Optional[str] = None,
    ) -> Dict[str, Any]:
        client = self.clients[client_id]

        if client.connected_endpoint == "source" and self.route_target == ClientRouteTarget.TARGET:
            if not client.auto_redirect_enabled:
                raise StaleClientEpochError(
                    f"MOVED/READONLY: client '{client_id}' pinned to fenced source at epoch {client.known_epoch} "
                    f"(active epoch={self.routing_epoch})"
                )
            client.connected_endpoint = "target"
            client.known_epoch = self.routing_epoch

        if self.route_target == ClientRouteTarget.FENCED and is_write:
            raise CacheSemanticsError("writes fenced during cutover transition")

        active_ep = self.target if client.connected_endpoint == "target" else self.source
        if client.known_epoch < self.routing_epoch:
            if client.auto_redirect_enabled:
                client.connected_endpoint = "target"
                client.known_epoch = self.routing_epoch
                active_ep = self.target
            else:
                raise StaleClientEpochError(
                    f"client '{client_id}' has stale epoch {client.known_epoch} < {self.routing_epoch}"
                )

        if is_write:
            assert record is not None
            active_ep.write_key(record, is_client_write=True)
            client.last_seen_offset = active_ep.repl_offset
            return {
                "endpoint": active_ep.name,
                "epoch": self.routing_epoch,
                "offset": active_ep.repl_offset,
                "status": "OK",
            }

        lookup_key = key if key is not None else (record.key if record is not None else "")
        found = active_ep.keys.get(lookup_key)
        return {
            "endpoint": active_ep.name,
            "epoch": self.routing_epoch,
            "value": found.value if found is not None else None,
            "found": found is not None,
        }


@dataclass
class CacheVerificationReport:
    overall_badge: CareBadge
    fidelity: FidelityDeclaration
    memory_verdict: MemoryPressureVerdict
    expired_in_flight_keys: List[str]
    near_expiry_keys: List[str]
    resurrected_keys: List[str]
    ttl_drift_keys: List[str]
    missing_keys: List[str]
    type_mismatches: List[str]
    access_metadata_mismatches: List[str]
    routing_separated_from_sync: bool
    late_resync_blocked: bool
    reverse_sync_safe: bool
    summary_lines: List[str]

    def format_unix_report(self) -> str:
        lines = [
            f"[{self.overall_badge.value}] cache_semantics verification report",
            f"  fidelity_preserved={self.fidelity.preserved} policy={self.fidelity.source_policy.value}->{self.fidelity.target_policy.value} badge={self.fidelity.care_badge.value}",
            f"  memory_pressure={self.memory_verdict.under_pressure} ratio={self.memory_verdict.utilization_ratio:.2%} badge={self.memory_verdict.care_badge.value}",
            f"  expired_in_flight={len(self.expired_in_flight_keys)} near_expiry={len(self.near_expiry_keys)} resurrected={len(self.resurrected_keys)} ttl_drift={len(self.ttl_drift_keys)}",
            f"  missing_keys={len(self.missing_keys)} type_mismatches={len(self.type_mismatches)} metadata_mismatches={len(self.access_metadata_mismatches)}",
            f"  routing_separated={self.routing_separated_from_sync} late_resync_blocked={self.late_resync_blocked} reverse_sync_safe={self.reverse_sync_safe}",
        ]
        for s in self.summary_lines:
            lines.append(f"  detail: {s}")
        return "\n".join(lines)


class CacheMigrationCoordinator:
    def __init__(
        self,
        source: CacheEndpoint,
        target: CacheEndpoint,
        compensator: Optional[TTLCompensator] = None,
        memory_monitor: Optional[MemoryPressureMonitor] = None,
        checkpoint_store: Optional[CacheCheckpointStore] = None,
        ttl_tolerance_ms: int = 150,
    ):
        self.source = source
        self.target = target
        self.compensator = compensator or TTLCompensator()
        self.memory_monitor = memory_monitor or MemoryPressureMonitor()
        self.sync_plane = DataSyncPlane(
            source=source,
            target=target,
            compensator=self.compensator,
            checkpoint_store=checkpoint_store,
        )
        self.routing_plane = ClientRoutingPlane(source=source, target=target)
        self.ttl_tolerance_ms = int(ttl_tolerance_ms)
        self.late_resync_blocked_count: int = 0

    def execute_cutover(self, new_epoch: int = 1) -> CareBadge:
        self.routing_plane.fence_source_writes()
        self.sync_plane.promote_target_detach_sync(new_epoch=new_epoch)
        self.routing_plane.switch_routing_to_target(self.sync_plane, new_epoch=new_epoch)
        return CareBadge.PASS

    def verify_semantics(
        self,
        verify_wall_ms: int,
        allow_declared_degradation: bool = True,
    ) -> CacheVerificationReport:
        fidelity = EvictionFidelityEvaluator.evaluate(
            source_policy=self.source.eviction_policy,
            target_policy=self.target.eviction_policy,
            supports_restore_idletime=self.target.supports_restore_idletime,
            supports_restore_freq=self.target.supports_restore_freq,
            allow_declared_degradation=allow_declared_degradation,
        )
        mem_verdict = self.memory_monitor.evaluate(
            used_memory_bytes=self.target.used_memory_bytes,
            maxmemory_bytes=self.target.maxmemory_bytes,
            evicted_keys_delta=self.target.evicted_keys_count,
            persistent_keys_evicted=self.target.persistent_keys_evicted,
            target_policy=self.target.eviction_policy,
        )

        expired_in_flight: List[str] = []
        near_expiry: List[str] = []
        resurrected: List[str] = []
        ttl_drift: List[str] = []
        missing_keys: List[str] = []
        type_mismatches: List[str] = []
        metadata_mismatches: List[str] = []
        summary: List[str] = []

        src_wall = self.source.wall_clock_ms(verify_wall_ms)
        tgt_wall = self.target.wall_clock_ms(verify_wall_ms)

        for k, outcome in self.sync_plane.ttl_outcomes.items():
            if outcome.expired_in_flight:
                expired_in_flight.append(k)
                if k in self.target.keys:
                    resurrected.append(k)
                continue
            if outcome.near_expiry:
                near_expiry.append(k)

        for k, src_rec in self.source.keys.items():
            src_expired_now = (
                src_rec.expire_at_ms is not None and src_rec.expire_at_ms <= src_wall
            )
            tgt_rec = self.target.keys.get(k)

            if src_expired_now:
                if tgt_rec is not None:
                    if tgt_rec.expire_at_ms is None:
                        resurrected.append(k)
                    elif (tgt_rec.expire_at_ms - tgt_wall) > self.ttl_tolerance_ms:
                        resurrected.append(k)
                continue

            if k in expired_in_flight:
                continue

            if tgt_rec is None:
                if mem_verdict.evicted_keys_delta > 0 and mem_verdict.semantics_approved and not src_rec.is_persistent:
                    summary.append(f"key '{k}' evicted under approved pressure policy")
                    continue
                missing_keys.append(k)
                continue

            if tgt_rec.data_type != src_rec.data_type:
                type_mismatches.append(k)

            if src_rec.is_persistent and not tgt_rec.is_persistent:
                ttl_drift.append(k)
            elif not src_rec.is_persistent and tgt_rec.is_persistent:
                resurrected.append(k)
            elif not src_rec.is_persistent and not tgt_rec.is_persistent:
                src_rem = int(src_rec.expire_at_ms or 0) - src_wall
                tgt_rem = int(tgt_rec.expire_at_ms or 0) - tgt_wall
                if abs(src_rem - tgt_rem) > self.ttl_tolerance_ms:
                    ttl_drift.append(k)

            if self.target.eviction_policy.uses_lru and self.target.supports_restore_idletime:
                if src_rec.access.idletime_sec is not None and tgt_rec.access.idletime_sec is None:
                    metadata_mismatches.append(k)
            if self.target.eviction_policy.uses_lfu and self.target.supports_restore_freq:
                if src_rec.access.freq_counter is not None and tgt_rec.access.freq_counter != src_rec.access.freq_counter:
                    metadata_mismatches.append(k)

        badges: List[CareBadge] = [fidelity.care_badge, mem_verdict.care_badge]
        for out in self.sync_plane.ttl_outcomes.values():
            badges.append(out.care_badge)

        if resurrected or ttl_drift or missing_keys or type_mismatches or metadata_mismatches:
            badges.append(CareBadge.ACTION)
        elif expired_in_flight or near_expiry:
            badges.append(CareBadge.BENIGN)

        priority = {
            CareBadge.ACTION: 4,
            CareBadge.REVIEW: 3,
            CareBadge.BENIGN: 2,
            CareBadge.PASS: 1,
        }
        overall = max(badges, key=lambda b: priority[b])

        routing_sep = (
            self.routing_plane.route_target == ClientRouteTarget.SOURCE
            if not self.target.promoted
            else self.routing_plane.route_target == ClientRouteTarget.TARGET
        )

        return CacheVerificationReport(
            overall_badge=overall,
            fidelity=fidelity,
            memory_verdict=mem_verdict,
            expired_in_flight_keys=sorted(set(expired_in_flight)),
            near_expiry_keys=sorted(set(near_expiry)),
            resurrected_keys=sorted(set(resurrected)),
            ttl_drift_keys=sorted(set(ttl_drift)),
            missing_keys=sorted(set(missing_keys)),
            type_mismatches=sorted(set(type_mismatches)),
            access_metadata_mismatches=sorted(set(metadata_mismatches)),
            routing_separated_from_sync=routing_sep,
            late_resync_blocked=self.late_resync_blocked_count > 0 or self.target.promoted,
            reverse_sync_safe=self.target.post_promotion_writes >= 0,
            summary_lines=summary,
        )
