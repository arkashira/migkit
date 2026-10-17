import enum
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class MergeAlgebraKind(str, enum.Enum):
    COMMUTATIVE_PN_COUNTER = "pn_counter"
    COMMUTATIVE_G_COUNTER = "g_counter"
    COMMUTATIVE_LWW_REGISTER = "lww_register"
    COMMUTATIVE_ADD_WINS_SET = "add_wins_set"
    COMMUTATIVE_MAX = "max"
    COMMUTATIVE_MIN = "min"
    COMMUTATIVE_SET_UNION = "set_union"
    NON_COMMUTATIVE_STRICT = "non_commutative_strict"


class ConflictType(str, enum.Enum):
    CLEAN_PROGRESSION = "clean_progression"
    CYCLIC_ECHO = "cyclic_echo"
    OUT_OF_ORDER_DELIVERY = "out_of_order_delivery"
    STALE_UPDATE_ON_TOMBSTONE = "stale_update_on_tombstone"
    DELETE_REINSERT = "delete_reinsert"
    TOMBSTONE_EXPIRED = "tombstone_expired"
    CONCURRENT_INCREMENTS = "concurrent_increments"
    COMMUTATIVE_MERGE_SUCCESS = "commutative_merge_success"
    CONCURRENT_NONCOMMUTATIVE = "concurrent_noncommutative"
    CLOCK_SKEW_ANOMALY = "clock_skew_anomaly"
    TYPE_ALGEBRA_MISMATCH = "type_algebra_mismatch"
    ABSENT_BEFORE_IMAGE = "absent_before_image"
    HEURISTIC_PRECEDENCE_REJECTED = "heuristic_precedence_rejected"


@dataclass
class OriginIdentity:
    origin_id: str
    writer_id: Optional[str] = None
    epoch: int = 1
    generation: int = 1
    priority_rank: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "origin_id": self.origin_id,
            "writer_id": self.writer_id,
            "epoch": self.epoch,
            "generation": self.generation,
            "priority_rank": self.priority_rank,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OriginIdentity":
        return cls(
            origin_id=str(data.get("origin_id", "")),
            writer_id=data.get("writer_id"),
            epoch=int(data.get("epoch", 1)),
            generation=int(data.get("generation", 1)),
            priority_rank=int(data.get("priority_rank", 0)),
        )


@dataclass
class VersionVector:
    clocks: Dict[str, int] = field(default_factory=dict)

    def clone(self) -> "VersionVector":
        return VersionVector(clocks=dict(self.clocks))

    def get(self, origin_id: str) -> int:
        return self.clocks.get(origin_id, 0)

    def increment(self, origin_id: str, amount: int = 1) -> "VersionVector":
        self.clocks[origin_id] = self.clocks.get(origin_id, 0) + amount
        return self

    def dominates(self, other: "VersionVector") -> bool:
        keys = set(self.clocks.keys()) | set(other.clocks.keys())
        greater = False
        for k in keys:
            v_self = self.clocks.get(k, 0)
            v_other = other.clocks.get(k, 0)
            if v_self < v_other:
                return False
            if v_self > v_other:
                greater = True
        return greater

    def is_descendant_or_equal(self, other: "VersionVector") -> bool:
        keys = set(self.clocks.keys()) | set(other.clocks.keys())
        for k in keys:
            if self.clocks.get(k, 0) < other.clocks.get(k, 0):
                return False
        return True

    def is_equal(self, other: "VersionVector") -> bool:
        keys = set(self.clocks.keys()) | set(other.clocks.keys())
        for k in keys:
            if self.clocks.get(k, 0) != other.clocks.get(k, 0):
                return False
        return True

    def is_concurrent(self, other: "VersionVector") -> bool:
        return not self.dominates(other) and not other.dominates(self) and not self.is_equal(other)

    def merge(self, other: "VersionVector") -> "VersionVector":
        merged: Dict[str, int] = {}
        keys = set(self.clocks.keys()) | set(other.clocks.keys())
        for k in keys:
            merged[k] = max(self.clocks.get(k, 0), other.clocks.get(k, 0))
        return VersionVector(clocks=merged)

    def to_dict(self) -> Dict[str, int]:
        return dict(self.clocks)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VersionVector":
        return cls(clocks={str(k): int(v) for k, v in data.items()})


@dataclass
class HybridLogicalClock:
    physical_ts: float
    logical_counter: int = 0
    origin_id: str = ""
    skew_bound_seconds: float = 0.5

    def to_tuple(self) -> Tuple[float, int]:
        return (self.physical_ts, self.logical_counter)

    def compare(
        self, other: "HybridLogicalClock", max_skew_seconds: Optional[float] = None
    ) -> Tuple[int, bool]:
        tolerance = (
            max_skew_seconds
            if max_skew_seconds is not None
            else max(self.skew_bound_seconds, other.skew_bound_seconds)
        )
        skew_delta = abs(self.physical_ts - other.physical_ts)
        skew_exceeded = skew_delta > tolerance

        if self.physical_ts < other.physical_ts:
            return (-1, skew_exceeded)
        elif self.physical_ts > other.physical_ts:
            return (1, skew_exceeded)
        else:
            if self.logical_counter < other.logical_counter:
                return (-1, skew_exceeded)
            elif self.logical_counter > other.logical_counter:
                return (1, skew_exceeded)
            else:
                return (0, skew_exceeded)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "physical_ts": self.physical_ts,
            "logical_counter": self.logical_counter,
            "origin_id": self.origin_id,
            "skew_bound_seconds": self.skew_bound_seconds,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HybridLogicalClock":
        return cls(
            physical_ts=float(data.get("physical_ts", 0.0)),
            logical_counter=int(data.get("logical_counter", 0)),
            origin_id=str(data.get("origin_id", "")),
            skew_bound_seconds=float(data.get("skew_bound_seconds", 0.5)),
        )


@dataclass
class BeforeImage:
    columns: Dict[str, Any]
    is_full: bool = True
    is_absent: bool = False

    def matches(self, row: Optional[Dict[str, Any]]) -> bool:
        if self.is_absent:
            return False
        if row is None:
            return False
        for k, v in self.columns.items():
            if k not in row:
                return False
            if row[k] != v:
                return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "columns": dict(self.columns),
            "is_full": self.is_full,
            "is_absent": self.is_absent,
        }


@dataclass
class Tombstone:
    table: str
    key: Dict[str, Any]
    deleted_at: float
    generation: int = 1
    version_vector: Optional[VersionVector] = None
    origin_id: str = ""
    horizon_seconds: float = 86400.0

    def is_expired(self, now: Optional[float] = None) -> bool:
        current_time = time.time() if now is None else now
        return (current_time - self.deleted_at) > self.horizon_seconds

    def is_valid_reinsert(
        self,
        new_generation: int,
        new_ts: Optional[float] = None,
        new_vector: Optional[VersionVector] = None,
    ) -> bool:
        if new_generation > self.generation:
            return True
        if new_generation == self.generation:
            if new_vector is not None and self.version_vector is not None:
                if new_vector.dominates(self.version_vector):
                    return True
            if new_ts is not None and new_ts > self.deleted_at:
                return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "key": dict(self.key),
            "deleted_at": self.deleted_at,
            "generation": self.generation,
            "version_vector": self.version_vector.to_dict() if self.version_vector else None,
            "origin_id": self.origin_id,
            "horizon_seconds": self.horizon_seconds,
        }


class TombstoneHorizonLedger:
    def __init__(self, default_horizon_seconds: float = 86400.0):
        self.default_horizon_seconds = default_horizon_seconds
        self._tombstones: Dict[Tuple[str, str], Tombstone] = {}

    @staticmethod
    def _key_token(table: str, key: Dict[str, Any]) -> Tuple[str, str]:
        canonical = json.dumps(key, sort_keys=True, default=str)
        return (table, canonical)

    def record(
        self,
        table: str,
        key: Dict[str, Any],
        deleted_at: Optional[float] = None,
        generation: int = 1,
        version_vector: Optional[VersionVector] = None,
        origin_id: str = "",
        horizon_seconds: Optional[float] = None,
    ) -> Tombstone:
        ts = time.time() if deleted_at is None else deleted_at
        h = self.default_horizon_seconds if horizon_seconds is None else horizon_seconds
        tb = Tombstone(
            table=table,
            key=dict(key),
            deleted_at=ts,
            generation=generation,
            version_vector=version_vector.clone() if version_vector else None,
            origin_id=origin_id,
            horizon_seconds=h,
        )
        token = self._key_token(table, key)
        self._tombstones[token] = tb
        return tb

    def get(self, table: str, key: Dict[str, Any]) -> Optional[Tombstone]:
        token = self._key_token(table, key)
        return self._tombstones.get(token)

    def remove(self, table: str, key: Dict[str, Any]) -> bool:
        token = self._key_token(table, key)
        if token in self._tombstones:
            del self._tombstones[token]
            return True
        return False

    def purge_expired(self, now: Optional[float] = None) -> int:
        current_time = time.time() if now is None else now
        expired = [
            k for k, tb in self._tombstones.items() if tb.is_expired(current_time)
        ]
        for k in expired:
            del self._tombstones[k]
        return len(expired)

    def count(self) -> int:
        return len(self._tombstones)


@dataclass
class PNCounter:
    p_counts: Dict[str, float] = field(default_factory=dict)
    n_counts: Dict[str, float] = field(default_factory=dict)

    def value(self) -> float:
        total_p = sum(self.p_counts.values())
        total_n = sum(self.n_counts.values())
        return total_p - total_n

    def increment(self, origin_id: str, amount: float = 1.0) -> "PNCounter":
        if amount >= 0:
            self.p_counts[origin_id] = self.p_counts.get(origin_id, 0.0) + amount
        else:
            self.decrement(origin_id, abs(amount))
        return self

    def decrement(self, origin_id: str, amount: float = 1.0) -> "PNCounter":
        if amount >= 0:
            self.n_counts[origin_id] = self.n_counts.get(origin_id, 0.0) + amount
        else:
            self.increment(origin_id, abs(amount))
        return self

    def merge(self, other: "PNCounter") -> "PNCounter":
        merged_p: Dict[str, float] = {}
        for k in set(self.p_counts.keys()) | set(other.p_counts.keys()):
            merged_p[k] = max(self.p_counts.get(k, 0.0), other.p_counts.get(k, 0.0))

        merged_n: Dict[str, float] = {}
        for k in set(self.n_counts.keys()) | set(other.n_counts.keys()):
            merged_n[k] = max(self.n_counts.get(k, 0.0), other.n_counts.get(k, 0.0))

        return PNCounter(p_counts=merged_p, n_counts=merged_n)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "p_counts": dict(self.p_counts),
            "n_counts": dict(self.n_counts),
            "value": self.value(),
        }


@dataclass
class AddWinsSet:
    entries: Set[Tuple[Any, str]] = field(default_factory=set)

    def add(self, item: Any, tag: Optional[str] = None) -> str:
        token = tag or str(uuid.uuid4())
        self.entries.add((item, token))
        return token

    def remove(self, item: Any) -> Set[str]:
        removed_tokens = set()
        to_del = set()
        for elem, token in self.entries:
            if elem == item:
                to_del.add((elem, token))
                removed_tokens.add(token)
        self.entries -= to_del
        return removed_tokens

    def elements(self) -> Set[Any]:
        return {item for item, _ in self.entries}

    def merge(self, other: "AddWinsSet") -> "AddWinsSet":
        merged_entries = set(self.entries) | set(other.entries)
        return AddWinsSet(entries=merged_entries)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "elements": sorted([str(x) for x in self.elements()]),
            "entries_count": len(self.entries),
        }


@dataclass
class ColumnMergePolicy:
    column_name: str
    algebra: MergeAlgebraKind
    data_type: str = "text"
    verifiable_clock: bool = False
    clock_column: Optional[str] = None
    group_columns: Optional[List[str]] = None

    def validate_applicability(self, value: Any) -> bool:
        if self.algebra == MergeAlgebraKind.COMMUTATIVE_PN_COUNTER:
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if self.algebra == MergeAlgebraKind.COMMUTATIVE_G_COUNTER:
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if self.algebra == MergeAlgebraKind.COMMUTATIVE_ADD_WINS_SET:
            return isinstance(value, (set, list, tuple, AddWinsSet))
        if self.algebra == MergeAlgebraKind.COMMUTATIVE_SET_UNION:
            return isinstance(value, (set, list, tuple))
        if self.algebra in (MergeAlgebraKind.COMMUTATIVE_MAX, MergeAlgebraKind.COMMUTATIVE_MIN):
            return value is not None
        if self.algebra == MergeAlgebraKind.COMMUTATIVE_LWW_REGISTER:
            return self.verifiable_clock
        return True


@dataclass
class TableConflictPolicy:
    table_name: str
    default_algebra: MergeAlgebraKind = MergeAlgebraKind.NON_COMMUTATIVE_STRICT
    columns: Dict[str, ColumnMergePolicy] = field(default_factory=dict)
    tombstone_horizon_seconds: float = 86400.0
    clock_skew_tolerance_seconds: float = 5.0
    require_before_image: bool = True
    cyclic_suppression: bool = True

    def get_column_policy(self, col: str) -> ColumnMergePolicy:
        if col in self.columns:
            return self.columns[col]
        return ColumnMergePolicy(
            column_name=col,
            algebra=self.default_algebra,
            data_type="text",
            verifiable_clock=False,
        )


@dataclass
class ConflictAuditRecord:
    audit_id: str
    table: str
    key: Dict[str, Any]
    conflict_type: str
    care_badge: str
    resolved: bool
    local_state: Optional[Dict[str, Any]] = None
    incoming_state: Optional[Dict[str, Any]] = None
    before_image: Optional[Dict[str, Any]] = None
    resolved_state: Optional[Dict[str, Any]] = None
    unresolved_columns: List[str] = field(default_factory=list)
    resolution_reason: str = ""
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "table": self.table,
            "key": dict(self.key),
            "conflict_type": self.conflict_type,
            "care_badge": self.care_badge,
            "resolved": self.resolved,
            "local_state": dict(self.local_state) if self.local_state is not None else None,
            "incoming_state": dict(self.incoming_state) if self.incoming_state is not None else None,
            "before_image": dict(self.before_image) if self.before_image is not None else None,
            "resolved_state": dict(self.resolved_state) if self.resolved_state is not None else None,
            "unresolved_columns": list(self.unresolved_columns),
            "resolution_reason": self.resolution_reason,
            "timestamp": self.timestamp,
            "metadata": dict(self.metadata),
        }


class ConflictAuditLedger:
    def __init__(self):
        self._records: List[ConflictAuditRecord] = []

    def record(self, rec: ConflictAuditRecord) -> None:
        self._records.append(rec)

    def all_records(self, table: Optional[str] = None) -> List[ConflictAuditRecord]:
        if table:
            return [r for r in self._records if r.table == table]
        return list(self._records)

    def unresolved_conflicts(self, table: Optional[str] = None) -> List[ConflictAuditRecord]:
        return [
            r
            for r in self.all_records(table)
            if not r.resolved or r.care_badge == CareBadge.ACTION.value
        ]

    def counts_by_badge(self) -> Dict[str, int]:
        counts = {
            CareBadge.ACTION.value: 0,
            CareBadge.REVIEW.value: 0,
            CareBadge.BENIGN.value: 0,
            CareBadge.PASS.value: 0,
        }
        for r in self._records:
            if r.care_badge in counts:
                counts[r.care_badge] += 1
            else:
                counts[r.care_badge] = 1
        return counts

    def clear(self) -> None:
        self._records.clear()


def reject_heuristic_server_precedence(origin_a: str, origin_b: str) -> None:
    lower_a = origin_a.lower()
    lower_b = origin_b.lower()
    heuristics = ("master", "primary", "leader", "main", "active", "standby", "slave")
    for word in heuristics:
        if (word in lower_a) or (word in lower_b):
            raise ValueError(
                f"Heuristic server name precedence rejected for origins '{origin_a}' and '{origin_b}'. "
                "Never infer business precedence from server names or arbitrary strings. "
                "Use explicit priority ranks, version vectors, or verifiable clocks."
            )


class ConflictSemanticsEngine:
    def __init__(self, node_id: str, default_tombstone_horizon_seconds: float = 86400.0):
        self.node_id = str(node_id)
        self.tombstones = TombstoneHorizonLedger(
            default_horizon_seconds=default_tombstone_horizon_seconds
        )
        self.audit_ledger = ConflictAuditLedger()
        self.policies: Dict[str, TableConflictPolicy] = {}
        self._applied_tx_digests: Set[str] = set()

    def register_policy(self, table_name: str, policy: TableConflictPolicy) -> None:
        self.policies[table_name] = policy

    def get_policy(self, table_name: str) -> TableConflictPolicy:
        if table_name in self.policies:
            return self.policies[table_name]
        return TableConflictPolicy(table_name=table_name)

    @staticmethod
    def _deterministic_tie_break(val_a: Any, val_b: Any, origin_a: str, origin_b: str) -> bool:
        reject_heuristic_server_precedence(origin_a, origin_b)
        hash_a = hashlib.sha256(f"{origin_a}:{json.dumps(val_a, sort_keys=True, default=str)}".encode()).digest()
        hash_b = hashlib.sha256(f"{origin_b}:{json.dumps(val_b, sort_keys=True, default=str)}".encode()).digest()
        return hash_a > hash_b

    def resolve_write(
        self,
        table: str,
        key: Dict[str, Any],
        operation: str,
        incoming_row: Optional[Dict[str, Any]],
        before_image: Optional[BeforeImage],
        local_row: Optional[Dict[str, Any]],
        incoming_origin: OriginIdentity,
        local_origin: Optional[OriginIdentity] = None,
        incoming_vector: Optional[VersionVector] = None,
        local_vector: Optional[VersionVector] = None,
        incoming_hlc: Optional[HybridLogicalClock] = None,
        local_hlc: Optional[HybridLogicalClock] = None,
        incoming_generation: int = 1,
        now: Optional[float] = None,
        tx_digest: Optional[str] = None,
    ) -> Tuple[Optional[Dict[str, Any]], ConflictAuditRecord]:
        current_time = time.time() if now is None else now
        policy = self.get_policy(table)
        audit_id = str(uuid.uuid4())
        op = operation.upper()

        if local_origin and incoming_origin.origin_id != local_origin.origin_id:
            reject_heuristic_server_precedence(
                incoming_origin.origin_id, local_origin.origin_id
            )

        if tx_digest:
            if tx_digest in self._applied_tx_digests:
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.CYCLIC_ECHO.value,
                    care_badge=CareBadge.BENIGN.value,
                    resolved=True,
                    local_state=local_row,
                    incoming_state=incoming_row,
                    before_image=before_image.to_dict() if before_image else None,
                    resolved_state=local_row,
                    resolution_reason="Cyclic replication echo suppressed via transaction digest.",
                    timestamp=current_time,
                    metadata={"tx_digest": tx_digest},
                )
                self.audit_ledger.record(rec)
                return (local_row, rec)

        if policy.cyclic_suppression and incoming_origin.origin_id == self.node_id:
            rec = ConflictAuditRecord(
                audit_id=audit_id,
                table=table,
                key=key,
                conflict_type=ConflictType.CYCLIC_ECHO.value,
                care_badge=CareBadge.BENIGN.value,
                resolved=True,
                local_state=local_row,
                incoming_state=incoming_row,
                before_image=before_image.to_dict() if before_image else None,
                resolved_state=local_row,
                resolution_reason="Cyclic replication loop detected; incoming write originated on this node.",
                timestamp=current_time,
                metadata={"origin_id": incoming_origin.origin_id},
            )
            self.audit_ledger.record(rec)
            return (local_row, rec)

        tombstone = self.tombstones.get(table, key)

        if op == "DELETE":
            if local_row is None and tombstone is not None:
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.CLEAN_PROGRESSION.value,
                    care_badge=CareBadge.BENIGN.value,
                    resolved=True,
                    local_state=None,
                    incoming_state=None,
                    before_image=before_image.to_dict() if before_image else None,
                    resolved_state=None,
                    resolution_reason="Row already deleted; idempotent delete.",
                    timestamp=current_time,
                )
                self.audit_ledger.record(rec)
                return (None, rec)

            self.tombstones.record(
                table=table,
                key=key,
                deleted_at=current_time,
                generation=incoming_generation,
                version_vector=incoming_vector,
                origin_id=incoming_origin.origin_id,
                horizon_seconds=policy.tombstone_horizon_seconds,
            )
            if tx_digest:
                self._applied_tx_digests.add(tx_digest)
            rec = ConflictAuditRecord(
                audit_id=audit_id,
                table=table,
                key=key,
                conflict_type=ConflictType.CLEAN_PROGRESSION.value,
                care_badge=CareBadge.PASS.value,
                resolved=True,
                local_state=local_row,
                incoming_state=None,
                before_image=before_image.to_dict() if before_image else None,
                resolved_state=None,
                resolution_reason="Row successfully deleted with tombstone recorded.",
                timestamp=current_time,
            )
            self.audit_ledger.record(rec)
            return (None, rec)

        if local_row is None and tombstone is not None:
            if tombstone.is_expired(current_time):
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.TOMBSTONE_EXPIRED.value,
                    care_badge=CareBadge.ACTION.value,
                    resolved=False,
                    local_state=None,
                    incoming_state=incoming_row,
                    before_image=before_image.to_dict() if before_image else None,
                    resolved_state=None,
                    resolution_reason="Tombstone expired past horizon; cannot distinguish legal reinsert from stale zombie update.",
                    timestamp=current_time,
                    metadata={"deleted_at": tombstone.deleted_at, "horizon": tombstone.horizon_seconds},
                )
                self.audit_ledger.record(rec)
                return (None, rec)

            is_reinsert = (op == "INSERT") or tombstone.is_valid_reinsert(
                new_generation=incoming_generation,
                new_ts=incoming_hlc.physical_ts if incoming_hlc else current_time,
                new_vector=incoming_vector,
            )

            if is_reinsert:
                self.tombstones.remove(table, key)
                if tx_digest:
                    self._applied_tx_digests.add(tx_digest)
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.DELETE_REINSERT.value,
                    care_badge=CareBadge.PASS.value,
                    resolved=True,
                    local_state=None,
                    incoming_state=incoming_row,
                    before_image=before_image.to_dict() if before_image else None,
                    resolved_state=incoming_row,
                    resolution_reason="Valid delete/reinsert detected; newer generation clears prior tombstone.",
                    timestamp=current_time,
                    metadata={"prior_generation": tombstone.generation, "new_generation": incoming_generation},
                )
                self.audit_ledger.record(rec)
                return (incoming_row, rec)
            else:
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.STALE_UPDATE_ON_TOMBSTONE.value,
                    care_badge=CareBadge.BENIGN.value,
                    resolved=True,
                    local_state=None,
                    incoming_state=incoming_row,
                    before_image=before_image.to_dict() if before_image else None,
                    resolved_state=None,
                    resolution_reason="Stale update discarded against active tombstone within horizon.",
                    timestamp=current_time,
                    metadata={"tombstone_generation": tombstone.generation},
                )
                self.audit_ledger.record(rec)
                return (None, rec)

        if local_row is None and op == "INSERT":
            if tx_digest:
                self._applied_tx_digests.add(tx_digest)
            rec = ConflictAuditRecord(
                audit_id=audit_id,
                table=table,
                key=key,
                conflict_type=ConflictType.CLEAN_PROGRESSION.value,
                care_badge=CareBadge.PASS.value,
                resolved=True,
                local_state=None,
                incoming_state=incoming_row,
                before_image=before_image.to_dict() if before_image else None,
                resolved_state=incoming_row,
                resolution_reason="Clean insert into non-existent row.",
                timestamp=current_time,
            )
            self.audit_ledger.record(rec)
            return (incoming_row, rec)

        if local_vector is not None and incoming_vector is not None:
            if local_vector.dominates(incoming_vector):
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.OUT_OF_ORDER_DELIVERY.value,
                    care_badge=CareBadge.BENIGN.value,
                    resolved=True,
                    local_state=local_row,
                    incoming_state=incoming_row,
                    before_image=before_image.to_dict() if before_image else None,
                    resolved_state=local_row,
                    resolution_reason="Out-of-order write rejected; local version vector strictly dominates incoming.",
                    timestamp=current_time,
                    metadata={"local_vector": local_vector.to_dict(), "incoming_vector": incoming_vector.to_dict()},
                )
                self.audit_ledger.record(rec)
                return (local_row, rec)

            if incoming_vector.dominates(local_vector):
                if tx_digest:
                    self._applied_tx_digests.add(tx_digest)
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.CLEAN_PROGRESSION.value,
                    care_badge=CareBadge.PASS.value,
                    resolved=True,
                    local_state=local_row,
                    incoming_state=incoming_row,
                    before_image=before_image.to_dict() if before_image else None,
                    resolved_state=incoming_row,
                    resolution_reason="Incoming write causally dominates local state; applied cleanly.",
                    timestamp=current_time,
                    metadata={"local_vector": local_vector.to_dict(), "incoming_vector": incoming_vector.to_dict()},
                )
                self.audit_ledger.record(rec)
                return (incoming_row, rec)

        if before_image is not None and not before_image.is_absent:
            if before_image.matches(local_row):
                if tx_digest:
                    self._applied_tx_digests.add(tx_digest)
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.CLEAN_PROGRESSION.value,
                    care_badge=CareBadge.PASS.value,
                    resolved=True,
                    local_state=local_row,
                    incoming_state=incoming_row,
                    before_image=before_image.to_dict(),
                    resolved_state=incoming_row,
                    resolution_reason="Serial write progression verified; before-image matches current local row.",
                    timestamp=current_time,
                )
                self.audit_ledger.record(rec)
                return (incoming_row, rec)

        if before_image is None or before_image.is_absent:
            if policy.require_before_image:
                rec = ConflictAuditRecord(
                    audit_id=audit_id,
                    table=table,
                    key=key,
                    conflict_type=ConflictType.ABSENT_BEFORE_IMAGE.value,
                    care_badge=CareBadge.ACTION.value,
                    resolved=False,
                    local_state=local_row,
                    incoming_state=incoming_row,
                    before_image=None,
                    resolved_state=local_row,
                    unresolved_columns=list((incoming_row or {}).keys()),
                    resolution_reason="Before-image absent; cannot prove write concurrency or column difference safety.",
                    timestamp=current_time,
                )
                self.audit_ledger.record(rec)
                return (local_row, rec)

        return self._resolve_concurrent_columns(
            table=table,
            key=key,
            incoming_row=incoming_row or {},
            local_row=local_row or {},
            before_image=before_image,
            incoming_origin=incoming_origin,
            local_origin=local_origin or OriginIdentity(origin_id=self.node_id),
            incoming_hlc=incoming_hlc,
            local_hlc=local_hlc,
            policy=policy,
            current_time=current_time,
            tx_digest=tx_digest,
        )

    def _resolve_concurrent_columns(
        self,
        table: str,
        key: Dict[str, Any],
        incoming_row: Dict[str, Any],
        local_row: Dict[str, Any],
        before_image: Optional[BeforeImage],
        incoming_origin: OriginIdentity,
        local_origin: OriginIdentity,
        incoming_hlc: Optional[HybridLogicalClock],
        local_hlc: Optional[HybridLogicalClock],
        policy: TableConflictPolicy,
        current_time: float,
        tx_digest: Optional[str],
    ) -> Tuple[Dict[str, Any], ConflictAuditRecord]:
        audit_id = str(uuid.uuid4())
        merged_row = dict(local_row)
        unresolved_columns: List[str] = []
        skew_anomalies: List[str] = []
        type_mismatches: List[str] = []
        commutative_applied: List[str] = []
        before_cols = before_image.columns if (before_image and not before_image.is_absent) else {}

        all_keys = set(local_row.keys()) | set(incoming_row.keys())
        has_conflict = False

        for col in all_keys:
            val_local = local_row.get(col)
            val_in = incoming_row.get(col)

            if val_local == val_in:
                continue

            val_before = before_cols.get(col)
            local_changed = (val_local != val_before) if before_cols else True
            incoming_changed = (val_in != val_before) if before_cols else True

            if not local_changed and incoming_changed:
                merged_row[col] = val_in
                continue

            if local_changed and not incoming_changed:
                merged_row[col] = val_local
                continue

            has_conflict = True
            col_policy = policy.get_column_policy(col)

            if not col_policy.validate_applicability(val_in) or not col_policy.validate_applicability(val_local):
                type_mismatches.append(col)
                unresolved_columns.append(col)
                continue

            algebra = col_policy.algebra

            if algebra == MergeAlgebraKind.COMMUTATIVE_PN_COUNTER:
                if val_before is not None:
                    delta_in = val_in - val_before
                    delta_local = val_local - val_before
                    merged_row[col] = val_before + delta_local + delta_in
                else:
                    merged_row[col] = val_local + val_in
                commutative_applied.append(col)

            elif algebra == MergeAlgebraKind.COMMUTATIVE_G_COUNTER:
                merged_row[col] = max(val_local, val_in)
                commutative_applied.append(col)

            elif algebra == MergeAlgebraKind.COMMUTATIVE_MAX:
                merged_row[col] = max(val_local, val_in)
                commutative_applied.append(col)

            elif algebra == MergeAlgebraKind.COMMUTATIVE_MIN:
                merged_row[col] = min(val_local, val_in)
                commutative_applied.append(col)

            elif algebra == MergeAlgebraKind.COMMUTATIVE_SET_UNION:
                set_local = set(val_local) if isinstance(val_local, (list, tuple, set)) else {val_local}
                set_in = set(val_in) if isinstance(val_in, (list, tuple, set)) else {val_in}
                merged_set = set_local | set_in
                merged_row[col] = sorted(list(merged_set), key=str)
                commutative_applied.append(col)

            elif algebra == MergeAlgebraKind.COMMUTATIVE_ADD_WINS_Set if hasattr(MergeAlgebraKind, 'COMMUTATIVE_ADD_WINS_Set') else algebra == MergeAlgebraKind.COMMUTATIVE_ADD_WINS_SET:
                if isinstance(val_local, AddWinsSet) and isinstance(val_in, AddWinsSet):
                    merged_row[col] = val_local.merge(val_in)
                elif isinstance(val_local, (list, tuple, set)) and isinstance(val_in, (list, tuple, set)):
                    merged_row[col] = sorted(list(set(val_local) | set(val_in)), key=str)
                else:
                    merged_row[col] = val_in
                commutative_applied.append(col)

            elif algebra == MergeAlgebraKind.COMMUTATIVE_LWW_REGISTER:
                if not col_policy.verifiable_clock or incoming_hlc is None or local_hlc is None:
                    unresolved_columns.append(col)
                    continue

                cmp_val, skew_exceeded = incoming_hlc.compare(
                    local_hlc, max_skew_seconds=policy.clock_skew_tolerance_seconds
                )
                if skew_exceeded:
                    skew_anomalies.append(col)
                    unresolved_columns.append(col)
                    continue

                if cmp_val > 0:
                    merged_row[col] = val_in
                    commutative_applied.append(col)
                elif cmp_val < 0:
                    merged_row[col] = val_local
                    commutative_applied.append(col)
                else:
                    if incoming_origin.priority_rank > local_origin.priority_rank:
                        merged_row[col] = val_in
                    elif local_origin.priority_rank > incoming_origin.priority_rank:
                        merged_row[col] = val_local
                    else:
                        in_wins = self._deterministic_tie_break(
                            val_in, val_local, incoming_origin.origin_id, local_origin.origin_id
                        )
                        merged_row[col] = val_in if in_wins else val_local
                    commutative_applied.append(col)

            else:
                unresolved_columns.append(col)

        if unresolved_columns:
            if skew_anomalies:
                conf_type = ConflictType.CLOCK_SKEW_ANOMALY.value
                reason = f"Clock skew exceeded permissible threshold ({policy.clock_skew_tolerance_seconds}s) on columns: {', '.join(skew_anomalies)}."
            elif type_mismatches:
                conf_type = ConflictType.TYPE_ALGEBRA_MISMATCH.value
                reason = f"Type algebra mismatch for commutative merge on columns: {', '.join(type_mismatches)}."
            else:
                conf_type = ConflictType.CONCURRENT_NONCOMMUTATIVE.value
                reason = f"Conflicting noncommutative writes remain visible and unresolved on columns: {', '.join(unresolved_columns)}."

            rec = ConflictAuditRecord(
                audit_id=audit_id,
                table=table,
                key=key,
                conflict_type=conf_type,
                care_badge=CareBadge.ACTION.value,
                resolved=False,
                local_state=local_row,
                incoming_state=incoming_row,
                before_image=before_image.to_dict() if before_image else None,
                resolved_state=local_row,
                unresolved_columns=unresolved_columns,
                resolution_reason=reason,
                timestamp=current_time,
                metadata={"commutative_applied": commutative_applied},
            )
            self.audit_ledger.record(rec)
            return (local_row, rec)

        if tx_digest:
            self._applied_tx_digests.add(tx_digest)

        badge = CareBadge.BENIGN.value if has_conflict else CareBadge.PASS.value
        rec = ConflictAuditRecord(
            audit_id=audit_id,
            table=table,
            key=key,
            conflict_type=ConflictType.COMMUTATIVE_MERGE_SUCCESS.value if has_conflict else ConflictType.CLEAN_PROGRESSION.value,
            care_badge=badge,
            resolved=True,
            local_state=local_row,
            incoming_state=incoming_row,
            before_image=before_image.to_dict() if before_image else None,
            resolved_state=merged_row,
            unresolved_columns=[],
            resolution_reason=f"Commutative algebra safely merged columns: {', '.join(commutative_applied)}.",
            timestamp=current_time,
            metadata={"commutative_columns": commutative_applied},
        )
        self.audit_ledger.record(rec)
        return (merged_row, rec)
