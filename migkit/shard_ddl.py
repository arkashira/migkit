from __future__ import annotations

import bisect
import copy
import dataclasses
import enum
import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from migkit.reshard import hash_place
from migkit.schema_drift import (
    BLOB_RANKS,
    INT_RANKS,
    TEXT_RANKS,
    check_type_widening,
    map_type_to_dialect,
    parse_type_signature,
)
from migkit.topology import KeySpace, Proof, Route, overlaps, prove_partition, range_gaps


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class DDLProgressMode(str, enum.Enum):
    OPTIMISTIC = "OPTIMISTIC"
    PESSIMISTIC_BARRIER = "PESSIMISTIC_BARRIER"
    SERIALIZED_AFFECTED = "SERIALIZED_AFFECTED"
    REFUSED = "REFUSED"


class BarrierStatus(str, enum.Enum):
    OPEN = "OPEN"
    WAITING = "WAITING"
    CONVERGED = "CONVERGED"
    APPLIED = "APPLIED"
    ABORTED = "ABORTED"
    DEADLOCK_DETECTED = "DEADLOCK_DETECTED"


class ShardDDLError(RuntimeError):
    def __init__(self, message: str, care_badge: CareBadge = CareBadge.ACTION):
        super().__init__(message)
        self.message = message
        self.care_badge = care_badge


class UnrepresentableShardDDLError(ShardDDLError):
    pass


class ShardDeadlockError(ShardDDLError):
    pass


class DuplicateKeyPlacementError(ShardDDLError):
    pass


class RoutingPlacementError(ShardDDLError):
    pass


class ShardSplitError(ShardDDLError):
    pass


def compare_watermark(a: Any, b: Any) -> int:
    if a is None and b is None:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return -1 if a < b else (1 if a > b else 0)
    sa, sb = str(a).strip(), str(b).strip()
    if sa.isdigit() and sb.isdigit():
        ia, ib = int(sa), int(sb)
        return -1 if ia < ib else (1 if ia > ib else 0)
    return -1 if sa < sb else (1 if sa > sb else 0)


@dataclass
class ColumnSpec:
    name: str
    data_type: str = "TEXT"
    nullable: bool = True
    default: Optional[Any] = None
    is_primary_key: bool = False
    ordinal: int = 0
    generation: int = 1
    origin_shards: Set[str] = field(default_factory=set)
    dropped_by_shards: Set[str] = field(default_factory=set)

    def normalized_type(self) -> str:
        base, params, is_unsigned = parse_type_signature(self.data_type)
        param_part = f"({','.join(str(p) for p in params)})" if params else ""
        unsigned_part = " UNSIGNED" if is_unsigned else ""
        return f"{base}{param_part}{unsigned_part}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "default": self.default,
            "is_primary_key": self.is_primary_key,
            "ordinal": self.ordinal,
            "generation": self.generation,
            "origin_shards": sorted(self.origin_shards),
            "dropped_by_shards": sorted(self.dropped_by_shards),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnSpec":
        return cls(
            name=str(data["name"]),
            data_type=str(data.get("data_type", "TEXT")),
            nullable=bool(data.get("nullable", True)),
            default=data.get("default"),
            is_primary_key=bool(data.get("is_primary_key", False)),
            ordinal=int(data.get("ordinal", 0)),
            generation=int(data.get("generation", 1)),
            origin_shards=set(data.get("origin_shards", [])),
            dropped_by_shards=set(data.get("dropped_by_shards", [])),
        )


@dataclass
class ShardDDLChange:
    op_id: str
    shard_id: str
    table: str
    op_type: str
    column: Optional[str] = None
    new_column_name: Optional[str] = None
    data_type: Optional[str] = None
    nullable: Optional[bool] = None
    default: Optional[Any] = None
    primary_key: Optional[Tuple[str, ...]] = None
    position: Union[int, str] = 0
    timestamp: float = field(default_factory=time.time)

    def canonical_signature(self) -> str:
        op = self.op_type.upper()
        col = self.column or ""
        new_col = self.new_column_name or ""
        dtype = ""
        if self.data_type:
            base, params, is_unsigned = parse_type_signature(self.data_type)
            p_str = f"({','.join(str(x) for x in params)})" if params else ""
            u_str = " UNSIGNED" if is_unsigned else ""
            dtype = f"{base}{p_str}{u_str}"
        pk_part = ",".join(self.primary_key) if self.primary_key else ""
        null_part = "" if self.nullable is None else ("NULL" if self.nullable else "NOT_NULL")
        def_part = "" if self.default is None else repr(self.default)
        return f"{self.table}:{op}:{col}:{new_col}:{dtype}:{null_part}:{def_part}:{pk_part}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "op_id": self.op_id,
            "shard_id": self.shard_id,
            "table": self.table,
            "op_type": self.op_type,
            "column": self.column,
            "new_column_name": self.new_column_name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "default": self.default,
            "primary_key": list(self.primary_key) if self.primary_key else None,
            "position": self.position,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ShardDDLChange":
        pk = data.get("primary_key")
        return cls(
            op_id=str(data["op_id"]),
            shard_id=str(data["shard_id"]),
            table=str(data["table"]),
            op_type=str(data["op_type"]),
            column=data.get("column"),
            new_column_name=data.get("new_column_name"),
            data_type=data.get("data_type"),
            nullable=data.get("nullable"),
            default=data.get("default"),
            primary_key=tuple(pk) if pk is not None else None,
            position=data.get("position", 0),
            timestamp=float(data.get("timestamp", 0.0)),
        )


@dataclass
class ShardSchemaVersion:
    shard_id: str
    table: str
    version: int = 1
    columns: Dict[str, ColumnSpec] = field(default_factory=dict)
    primary_key: Tuple[str, ...] = ()
    applied_ddls: Tuple[str, ...] = ()
    rename_aliases: Dict[str, str] = field(default_factory=dict)
    position: Union[int, str] = 0

    def schema_digest(self) -> str:
        col_items = []
        for name in sorted(self.columns):
            c = self.columns[name]
            col_items.append(
                (
                    c.name,
                    c.normalized_type(),
                    bool(c.nullable),
                    repr(c.default) if c.default is not None else None,
                    bool(c.is_primary_key),
                )
            )
        payload = {
            "table": self.table,
            "primary_key": list(self.primary_key),
            "columns": col_items,
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("ascii")).hexdigest()[:24]

    def evolve(self, ddl: ShardDDLChange) -> "ShardSchemaVersion":
        op = ddl.op_type.upper()
        next_cols = {k: copy.deepcopy(v) for k, v in self.columns.items()}
        next_pk = list(self.primary_key)
        next_aliases = dict(self.rename_aliases)

        if op == "ADD_COLUMN":
            col_name = ddl.column or ""
            if not col_name:
                raise UnrepresentableShardDDLError(f"{self.table}: ADD_COLUMN missing column name")
            if col_name in next_cols:
                existing = next_cols[col_name]
                same_type = existing.normalized_type() == ColumnSpec(
                    name=col_name, data_type=ddl.data_type or "TEXT"
                ).normalized_type()
                is_wide, _ = check_type_widening(existing.data_type, ddl.data_type or existing.data_type)
                if not same_type and not is_wide:
                    raise UnrepresentableShardDDLError(
                        f"{self.table}.{col_name}: conflicting ADD_COLUMN type {ddl.data_type} vs {existing.data_type}"
                    )
                if is_wide and ddl.data_type:
                    existing.data_type = ddl.data_type
                existing.origin_shards.add(ddl.shard_id)
            else:
                nullable = True if ddl.nullable is None else bool(ddl.nullable)
                if not nullable and ddl.default is None:
                    raise UnrepresentableShardDDLError(
                        f"{self.table}.{col_name}: ADD_COLUMN NOT NULL without default is unrepresentable"
                    )
                next_cols[col_name] = ColumnSpec(
                    name=col_name,
                    data_type=ddl.data_type or "TEXT",
                    nullable=nullable,
                    default=ddl.default,
                    is_primary_key=False,
                    ordinal=len(next_cols),
                    generation=1,
                    origin_shards={ddl.shard_id},
                )

        elif op == "DROP_COLUMN":
            col_name = ddl.column or ""
            if col_name in next_pk:
                raise UnrepresentableShardDDLError(
                    f"{self.table}.{col_name}: cannot drop primary key column on shard {self.shard_id}"
                )
            next_cols.pop(col_name, None)

        elif op in ("MODIFY_COLUMN", "ALTER_TYPE"):
            col_name = ddl.column or ""
            if col_name not in next_cols:
                raise UnrepresentableShardDDLError(
                    f"{self.table}.{col_name}: column missing on shard {self.shard_id} for {op}"
                )
            cur = next_cols[col_name]
            target_type = ddl.data_type or cur.data_type
            same_type = cur.normalized_type() == ColumnSpec(name=col_name, data_type=target_type).normalized_type()
            is_wide, _ = check_type_widening(cur.data_type, target_type)
            if not same_type and not is_wide:
                raise UnrepresentableShardDDLError(
                    f"{self.table}.{col_name}: lossy or incompatible type change {cur.data_type} -> {target_type}"
                )
            if ddl.nullable is False and cur.nullable and ddl.default is None and cur.default is None:
                raise UnrepresentableShardDDLError(
                    f"{self.table}.{col_name}: tightening to NOT NULL without default is unrepresentable"
                )
            cur.data_type = target_type
            if ddl.nullable is not None:
                cur.nullable = bool(ddl.nullable)
            if ddl.default is not None:
                cur.default = ddl.default
            cur.generation += 1

        elif op == "RENAME_COLUMN":
            old_name = ddl.column or ""
            new_name = ddl.new_column_name or ""
            if not old_name or not new_name:
                raise UnrepresentableShardDDLError(f"{self.table}: RENAME_COLUMN requires old and new names")
            if old_name not in next_cols:
                raise UnrepresentableShardDDLError(
                    f"{self.table}.{old_name}: source column missing for rename on {self.shard_id}"
                )
            if new_name in next_cols and new_name != old_name:
                raise UnrepresentableShardDDLError(
                    f"{self.table}.{new_name}: target column already exists on {self.shard_id}"
                )
            col_spec = next_cols.pop(old_name)
            col_spec.name = new_name
            col_spec.generation += 1
            next_cols[new_name] = col_spec
            next_aliases[old_name] = new_name
            if old_name in next_pk:
                next_pk = [new_name if k == old_name else k for k in next_pk]

        elif op == "CHANGE_PK":
            if not ddl.primary_key:
                raise UnrepresentableShardDDLError(f"{self.table}: CHANGE_PK requires non-empty primary_key")
            for pk_col in ddl.primary_key:
                if pk_col not in next_cols:
                    raise UnrepresentableShardDDLError(
                        f"{self.table}.{pk_col}: primary key column does not exist"
                    )
            for c in next_cols.values():
                c.is_primary_key = c.name in ddl.primary_key
            next_pk = list(ddl.primary_key)

        elif op == "TRUNCATE":
            pass

        else:
            raise UnrepresentableShardDDLError(f"{self.table}: unsupported DDL op_type {op}")

        sig = ddl.canonical_signature()
        applied = self.applied_ddls + ((sig,) if sig not in self.applied_ddls else ())
        return ShardSchemaVersion(
            shard_id=self.shard_id,
            table=self.table,
            version=self.version + 1,
            columns=next_cols,
            primary_key=tuple(next_pk),
            applied_ddls=applied,
            rename_aliases=next_aliases,
            position=ddl.position,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "table": self.table,
            "version": self.version,
            "columns": {k: v.to_dict() for k, v in self.columns.items()},
            "primary_key": list(self.primary_key),
            "applied_ddls": list(self.applied_ddls),
            "rename_aliases": dict(self.rename_aliases),
            "position": self.position,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ShardSchemaVersion":
        return cls(
            shard_id=str(data["shard_id"]),
            table=str(data["table"]),
            version=int(data.get("version", 1)),
            columns={k: ColumnSpec.from_dict(v) for k, v in data.get("columns", {}).items()},
            primary_key=tuple(data.get("primary_key", [])),
            applied_ddls=tuple(data.get("applied_ddls", [])),
            rename_aliases=dict(data.get("rename_aliases", {})),
            position=data.get("position", 0),
        )


def resolve_type_supremum(type_a: str, type_b: str) -> Tuple[Optional[str], Optional[str]]:
    spec_a = ColumnSpec(name="_", data_type=type_a)
    spec_b = ColumnSpec(name="_", data_type=type_b)
    if spec_a.normalized_type() == spec_b.normalized_type():
        return type_a, None
    ab_wide, reason_ab = check_type_widening(type_a, type_b)
    if ab_wide:
        return type_b, reason_ab
    ba_wide, reason_ba = check_type_widening(type_b, type_a)
    if ba_wide:
        return type_a, reason_ba
    return None, f"incompatible types across shards: {type_a} vs {type_b}"


@dataclass
class CombinedCompatibilityResult:
    table: str
    is_compatible: bool
    progress_mode: DDLProgressMode
    care_badge: CareBadge
    combined_schema: Optional[ShardSchemaVersion]
    affected_shards: Tuple[str, ...]
    waiting_shards: Tuple[str, ...]
    target_ddl_statements: List[str] = field(default_factory=list)
    reason: str = ""
    divergent_columns: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "is_compatible": self.is_compatible,
            "progress_mode": self.progress_mode.value,
            "care_badge": self.care_badge.value,
            "combined_schema": self.combined_schema.to_dict() if self.combined_schema else None,
            "affected_shards": list(self.affected_shards),
            "waiting_shards": list(self.waiting_shards),
            "target_ddl_statements": list(self.target_ddl_statements),
            "reason": self.reason,
            "divergent_columns": list(self.divergent_columns),
        }


@dataclass
class DDLConvergenceBarrier:
    barrier_id: str
    table: str
    op_type: str
    expected_shards: Set[str]
    arrived_shards: Dict[str, ShardDDLChange] = field(default_factory=dict)
    mode: DDLProgressMode = DDLProgressMode.SERIALIZED_AFFECTED
    status: BarrierStatus = BarrierStatus.WAITING
    care_badge: CareBadge = CareBadge.REVIEW
    target_column: Optional[str] = None
    new_column_name: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    converged_at: Optional[float] = None
    timeout_seconds: float = 300.0

    @property
    def pending_shards(self) -> Tuple[str, ...]:
        return tuple(sorted(self.expected_shards - set(self.arrived_shards.keys())))

    @property
    def is_converged(self) -> bool:
        return self.expected_shards <= set(self.arrived_shards.keys())

    def record_arrival(self, shard_id: str, ddl: ShardDDLChange) -> bool:
        self.arrived_shards[shard_id] = ddl
        if self.is_converged:
            self.status = BarrierStatus.CONVERGED
            self.care_badge = CareBadge.PASS
            if self.converged_at is None:
                self.converged_at = time.time()
            return True
        return False

    def apply_shard_split(self, parent_shard: str, child_shards: Sequence[str]) -> None:
        if parent_shard not in self.expected_shards:
            return
        self.expected_shards.discard(parent_shard)
        for child in child_shards:
            self.expected_shards.add(child)
        if parent_shard in self.arrived_shards:
            parent_ddl = self.arrived_shards.pop(parent_shard)
            for child in child_shards:
                child_ddl = copy.deepcopy(parent_ddl)
                child_ddl.shard_id = child
                self.arrived_shards[child] = child_ddl
        if self.is_converged and self.status == BarrierStatus.WAITING:
            self.status = BarrierStatus.CONVERGED
            self.care_badge = CareBadge.PASS
            self.converged_at = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "barrier_id": self.barrier_id,
            "table": self.table,
            "op_type": self.op_type,
            "expected_shards": sorted(self.expected_shards),
            "arrived_shards": {k: v.to_dict() for k, v in self.arrived_shards.items()},
            "mode": self.mode.value,
            "status": self.status.value,
            "care_badge": self.care_badge.value,
            "target_column": self.target_column,
            "new_column_name": self.new_column_name,
            "created_at": self.created_at,
            "converged_at": self.converged_at,
            "timeout_seconds": self.timeout_seconds,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DDLConvergenceBarrier":
        return cls(
            barrier_id=str(data["barrier_id"]),
            table=str(data["table"]),
            op_type=str(data["op_type"]),
            expected_shards=set(data.get("expected_shards", [])),
            arrived_shards={k: ShardDDLChange.from_dict(v) for k, v in data.get("arrived_shards", {}).items()},
            mode=DDLProgressMode(data.get("mode", DDLProgressMode.SERIALIZED_AFFECTED.value)),
            status=BarrierStatus(data.get("status", BarrierStatus.WAITING.value)),
            care_badge=CareBadge(data.get("care_badge", CareBadge.REVIEW.value)),
            target_column=data.get("target_column"),
            new_column_name=data.get("new_column_name"),
            created_at=float(data.get("created_at", 0.0)),
            converged_at=data.get("converged_at"),
            timeout_seconds=float(data.get("timeout_seconds", 300.0)),
        )


@dataclass(frozen=True)
class ShardRoutingRule:
    by: str
    targets: Tuple[str, ...]
    key_columns: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    bounds: Tuple[Any, ...] = ()
    values: Dict[str, Tuple[Any, ...]] = field(default_factory=dict)
    lookup: Dict[Any, str] = field(default_factory=dict)
    tables: Dict[str, str] = field(default_factory=dict)
    default_target: Optional[str] = None

    def key_for_table(self, table: str, fallback_pk: Tuple[str, ...] = ("id",)) -> Tuple[str, ...]:
        return self.key_columns.get(table, fallback_pk)

    def place_key(self, table: str, key_tuple: Tuple[Any, ...]) -> Optional[str]:
        if not self.targets:
            return None
        by = self.by.lower()
        if by == "table":
            target = self.tables.get(table, self.default_target)
            return target if target in self.targets else None
        if not key_tuple:
            return None
        first_val = key_tuple[0]
        if by == "hash":
            idx = hash_place(tuple(str(x) for x in key_tuple), len(self.targets))
            return self.targets[idx]
        if by == "range":
            if len(self.bounds) + 1 != len(self.targets):
                return None
            idx = bisect.bisect_right(list(self.bounds), first_val)
            return self.targets[idx]
        if by == "list":
            for tgt, vals in self.values.items():
                if first_val in vals or str(first_val) in [str(v) for v in vals]:
                    return tgt if tgt in self.targets else None
            return self.default_target if self.default_target in self.targets else None
        if by == "lookup":
            tgt = self.lookup.get(first_val)
            if tgt is None:
                tgt = self.lookup.get(str(first_val), self.default_target)
            return tgt if tgt in self.targets else None
        if by == "single":
            return self.targets[0]
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "by": self.by,
            "targets": list(self.targets),
            "key_columns": {k: list(v) for k, v in self.key_columns.items()},
            "bounds": list(self.bounds),
            "values": {k: list(v) for k, v in self.values.items()},
            "lookup": {str(k): v for k, v in self.lookup.items()},
            "tables": dict(self.tables),
            "default_target": self.default_target,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ShardRoutingRule":
        return cls(
            by=str(data.get("by", "hash")),
            targets=tuple(data.get("targets", [])),
            key_columns={k: tuple(v) for k, v in data.get("key_columns", {}).items()},
            bounds=tuple(data.get("bounds", [])),
            values={k: tuple(v) for k, v in data.get("values", {}).items()},
            lookup=dict(data.get("lookup", {})),
            tables=dict(data.get("tables", {})),
            default_target=data.get("default_target"),
        )


@dataclass
class KeyspaceValidationReport:
    is_valid: bool
    care_badge: CareBadge
    overlaps_found: List[Tuple[str, str, str, Any]] = field(default_factory=list)
    routing_errors: List[str] = field(default_factory=list)
    unroutable_samples: List[Tuple[str, str, Any]] = field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "care_badge": self.care_badge.value,
            "overlaps_found": [list(x) for x in self.overlaps_found],
            "routing_errors": list(self.routing_errors),
            "unroutable_samples": [list(x) for x in self.unroutable_samples],
            "summary": self.summary,
        }


def validate_keyspace_and_routing(
    shard_keyspaces: Dict[str, Dict[str, Union[KeySpace, Dict[str, Any]]]],
    routing_rule: ShardRoutingRule,
    tables: Sequence[str] = (),
    allow_keyspace_overlap: bool = False,
) -> KeyspaceValidationReport:
    errors: List[str] = []
    unroutable: List[Tuple[str, str, Any]] = []

    if not routing_rule.targets:
        errors.append("routing rule declares zero targets")

    by = routing_rule.by.lower()
    if by not in ("hash", "range", "list", "lookup", "table", "single"):
        errors.append(f"unsupported routing strategy: {routing_rule.by}")

    if by == "range" and routing_rule.targets:
        gap_msg = range_gaps(routing_rule.bounds, routing_rule.targets)
        if gap_msg:
            errors.append(f"range routing invalid: {gap_msg}")

    if by == "list":
        seen_vals: Dict[str, str] = {}
        for tgt, vals in routing_rule.values.items():
            if tgt not in routing_rule.targets:
                errors.append(f"list routing references unknown target {tgt}")
            for v in vals:
                sv = repr(v)
                if sv in seen_vals and seen_vals[sv] != tgt:
                    errors.append(
                        f"list routing maps value {v!r} to multiple targets: {seen_vals[sv]} and {tgt}"
                    )
                seen_vals[sv] = tgt

    if by == "lookup":
        for v, tgt in routing_rule.lookup.items():
            if tgt not in routing_rule.targets:
                errors.append(f"lookup routing maps {v!r} to unknown target {tgt}")

    if by == "table":
        for tbl in tables:
            tgt = routing_rule.tables.get(tbl, routing_rule.default_target)
            if tgt is None or tgt not in routing_rule.targets:
                errors.append(f"table routing has no valid target for table {tbl}")

    normalized_spaces: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for shard_id, tbl_map in shard_keyspaces.items():
        normalized_spaces[shard_id] = {}
        for tbl, spec in (tbl_map or {}).items():
            if isinstance(spec, KeySpace):
                d: Dict[str, Any] = {}
                if spec.lo is not None or spec.hi is not None:
                    d["range"] = [spec.lo, spec.hi]
                if spec.step > 1 or spec.offset != 0:
                    d["stride"] = [spec.offset, spec.step]
                if spec.values is not None:
                    d["values"] = list(spec.values)
                if spec.prefix is not None:
                    d["prefix"] = spec.prefix
                normalized_spaces[shard_id][tbl] = d
            else:
                normalized_spaces[shard_id][tbl] = dict(spec or {})
            ks = KeySpace.of(normalized_spaces[shard_id][tbl])
            if ks.lo is not None and ks.hi is not None and ks.lo > ks.hi:
                errors.append(
                    f"shard {shard_id} table {tbl} keyspace lower bound {ks.lo!r} exceeds upper bound {ks.hi!r}"
                )
            sample_keys: List[Any] = []
            if ks.values is not None:
                sample_keys.extend(sorted(ks.values, key=repr))
            else:
                if ks.lo is not None:
                    sample_keys.append(ks.lo)
                if ks.hi is not None:
                    sample_keys.append(ks.hi)
            for sk in sample_keys:
                placed = routing_rule.place_key(tbl, (sk,))
                if placed is None:
                    unroutable.append((shard_id, tbl, sk))

    ov = overlaps(normalized_spaces, tables=tables)
    has_illegal_overlap = bool(ov) and not allow_keyspace_overlap
    if unroutable:
        for shard_id, tbl, sk in unroutable[:5]:
            errors.append(f"key {sk!r} on shard {shard_id} table {tbl} places on no target")

    is_valid = not errors and not has_illegal_overlap
    if not is_valid:
        badge = CareBadge.ACTION
        parts = []
        if has_illegal_overlap:
            parts.append(f"{len(ov)} keyspace overlaps detected")
        if errors:
            parts.append("; ".join(errors))
        summary = f"[{badge.value}] " + "; ".join(parts)
    elif ov and allow_keyspace_overlap:
        badge = CareBadge.REVIEW
        summary = f"[{badge.value}] {len(ov)} keyspace overlaps allowed under conflict policy"
    else:
        badge = CareBadge.PASS
        summary = f"[{badge.value}] aggregate keyspace and routing verified across {len(shard_keyspaces)} shards"

    return KeyspaceValidationReport(
        is_valid=is_valid,
        care_badge=badge,
        overlaps_found=ov,
        routing_errors=errors,
        unroutable_samples=unroutable,
        summary=summary,
    )


@dataclass
class ShardSplitRecord:
    parent_shard: str
    child_shards: Tuple[str, ...]
    table: str
    split_watermark: Union[int, str]
    inherited_schema_version: int
    child_keyspaces: Dict[str, Dict[str, Any]]
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "parent_shard": self.parent_shard,
            "child_shards": list(self.child_shards),
            "table": self.table,
            "split_watermark": self.split_watermark,
            "inherited_schema_version": self.inherited_schema_version,
            "child_keyspaces": self.child_keyspaces,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ShardSplitRecord":
        return cls(
            parent_shard=str(data["parent_shard"]),
            child_shards=tuple(data.get("child_shards", [])),
            table=str(data["table"]),
            split_watermark=data.get("split_watermark", 0),
            inherited_schema_version=int(data.get("inherited_schema_version", 1)),
            child_keyspaces=dict(data.get("child_keyspaces", {})),
            created_at=float(data.get("created_at", 0.0)),
        )


@dataclass
class ShardedRowEvent:
    shard_id: str
    table: str
    op: str
    row: Dict[str, Any]
    before: Optional[Dict[str, Any]] = None
    position: Union[int, str] = 0
    schema_version: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "table": self.table,
            "op": self.op,
            "row": dict(self.row),
            "before": dict(self.before) if self.before else None,
            "position": self.position,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ShardedRowEvent":
        return cls(
            shard_id=str(data["shard_id"]),
            table=str(data["table"]),
            op=str(data["op"]),
            row=dict(data.get("row", {})),
            before=dict(data["before"]) if data.get("before") else None,
            position=data.get("position", 0),
            schema_version=data.get("schema_version"),
        )


@dataclass
class PlacementVerificationReport:
    ok: bool
    care_badge: CareBadge
    total_rows_verified: int
    proof: Proof
    duplicate_source_keys: List[Tuple[str, Tuple[Any, ...], Tuple[str, ...]]] = field(default_factory=list)
    lost_columns: List[Tuple[str, str]] = field(default_factory=list)
    summary: str = ""


class ShardDDLCoordinator:
    def __init__(
        self,
        routing_rule: ShardRoutingRule,
        target_dialect: str = "postgres",
        conflict_policy: str = "refuse",
        shard_priority: Optional[Sequence[str]] = None,
        state_path: Optional[Union[str, Path]] = None,
        barrier_timeout_seconds: float = 300.0,
    ):
        self.routing_rule = routing_rule
        self.target_dialect = target_dialect.lower()
        if conflict_policy not in ("refuse", "source_priority", "last_write_wins", "highest_version"):
            raise ValueError(f"unsupported conflict_policy: {conflict_policy}")
        self.conflict_policy = conflict_policy
        self.shard_priority = list(shard_priority or [])
        self.state_path = Path(state_path) if state_path else None
        self.barrier_timeout_seconds = float(barrier_timeout_seconds)

        self.shard_schemas: Dict[str, Dict[str, ShardSchemaVersion]] = {}
        self.target_schemas: Dict[str, ShardSchemaVersion] = {}
        self.shard_keyspaces: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.shard_watermarks: Dict[str, Union[int, str]] = {}
        self.retired_shards: Set[str] = set()
        self.split_records: List[ShardSplitRecord] = []

        self.active_barriers: Dict[str, DDLConvergenceBarrier] = {}
        self.completed_barriers: List[DDLConvergenceBarrier] = []
        self.held_events: Dict[Tuple[str, str], List[ShardedRowEvent]] = {}
        self.applied_target_ddls: List[str] = []
        self.applied_target_ddl_set: Set[str] = set()

        self.target_store: Dict[str, Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]]] = {
            t: {} for t in self.routing_rule.targets
        }
        self.key_owner: Dict[Tuple[str, Tuple[Any, ...]], Tuple[str, Union[int, str], int]] = {}
        self.duplicate_key_conflicts: List[Tuple[str, Tuple[Any, ...], str, str]] = []
        self.column_history: Dict[str, Dict[str, ColumnSpec]] = {}

    def active_shards_for_table(self, table: str) -> Tuple[str, ...]:
        out = [
            s
            for s, tbls in sorted(self.shard_schemas.items())
            if s not in self.retired_shards and table in tbls
        ]
        return tuple(out)

    def register_shard_table(
        self,
        shard_id: str,
        table: str,
        columns: Sequence[ColumnSpec],
        primary_key: Sequence[str],
        keyspace: Optional[Union[KeySpace, Dict[str, Any]]] = None,
        initial_version: int = 1,
    ) -> ShardSchemaVersion:
        pk_tuple = tuple(primary_key)
        col_map: Dict[str, ColumnSpec] = {}
        for idx, c in enumerate(columns):
            spec = copy.deepcopy(c)
            spec.ordinal = idx
            spec.is_primary_key = spec.name in pk_tuple or spec.is_primary_key
            spec.origin_shards.add(shard_id)
            col_map[spec.name] = spec

        sv = ShardSchemaVersion(
            shard_id=shard_id,
            table=table,
            version=initial_version,
            columns=col_map,
            primary_key=pk_tuple,
        )
        self.shard_schemas.setdefault(shard_id, {})[table] = sv
        self.shard_watermarks.setdefault(shard_id, 0)

        if keyspace is not None:
            if isinstance(keyspace, KeySpace):
                ks_dict: Dict[str, Any] = {}
                if keyspace.lo is not None or keyspace.hi is not None:
                    ks_dict["range"] = [keyspace.lo, keyspace.hi]
                if keyspace.step > 1 or keyspace.offset != 0:
                    ks_dict["stride"] = [keyspace.offset, keyspace.step]
                if keyspace.values is not None:
                    ks_dict["values"] = list(keyspace.values)
                if keyspace.prefix is not None:
                    ks_dict["prefix"] = keyspace.prefix
                self.shard_keyspaces.setdefault(shard_id, {})[table] = ks_dict
            else:
                self.shard_keyspaces.setdefault(shard_id, {})[table] = dict(keyspace)

        hist = self.column_history.setdefault(table, {})
        for cname, cspec in col_map.items():
            if cname not in hist:
                hist[cname] = copy.deepcopy(cspec)
            else:
                hist[cname].origin_shards.add(shard_id)

        self._recompute_combined_target_schema(table)
        self._persist_if_configured()
        return sv

    def compute_combined_compatibility(
        self,
        table: str,
        pending_ddl: Optional[ShardDDLChange] = None,
    ) -> CombinedCompatibilityResult:
        active_shards = self.active_shards_for_table(table)
        if not active_shards:
            return CombinedCompatibilityResult(
                table=table,
                is_compatible=False,
                progress_mode=DDLProgressMode.REFUSED,
                care_badge=CareBadge.ACTION,
                combined_schema=None,
                affected_shards=(),
                waiting_shards=(),
                reason=f"[ACTION] no active shards registered for table {table}",
            )

        simulated_schemas: Dict[str, ShardSchemaVersion] = {
            s: copy.deepcopy(self.shard_schemas[s][table]) for s in active_shards
        }
        if pending_ddl is not None and pending_ddl.shard_id in simulated_schemas:
            try:
                simulated_schemas[pending_ddl.shard_id] = simulated_schemas[pending_ddl.shard_id].evolve(pending_ddl)
            except UnrepresentableShardDDLError as exc:
                return CombinedCompatibilityResult(
                    table=table,
                    is_compatible=False,
                    progress_mode=DDLProgressMode.REFUSED,
                    care_badge=CareBadge.ACTION,
                    combined_schema=self.target_schemas.get(table),
                    affected_shards=(pending_ddl.shard_id,),
                    waiting_shards=(),
                    reason=f"[ACTION] {exc.message}",
                )

        pks = {s: sch.primary_key for s, sch in simulated_schemas.items()}
        distinct_pks = set(pks.values())

        if pending_ddl is not None and pending_ddl.op_type.upper() == "TRUNCATE":
            barrier_key = f"{table}:TRUNCATE"
            bar = self.active_barriers.get(barrier_key)
            arrived = set(bar.arrived_shards.keys()) if bar else set()
            arrived.add(pending_ddl.shard_id)
            waiting = tuple(sorted(set(active_shards) - arrived))
            return CombinedCompatibilityResult(
                table=table,
                is_compatible=True,
                progress_mode=DDLProgressMode.SERIALIZED_AFFECTED,
                care_badge=CareBadge.PASS if not waiting else CareBadge.REVIEW,
                combined_schema=self.target_schemas.get(table),
                affected_shards=active_shards,
                waiting_shards=waiting,
                target_ddl_statements=[f"TRUNCATE TABLE {table}"] if not waiting else [],
                reason=(
                    f"[PASS] all {len(active_shards)} shards converged on TRUNCATE for {table}"
                    if not waiting
                    else f"[REVIEW] serializing affected shards for {table} TRUNCATE; waiting on { list(waiting) }"
                ),
            )

        if len(distinct_pks) > 1 or (pending_ddl and pending_ddl.op_type.upper() == "CHANGE_PK"):
            if pending_ddl and pending_ddl.op_type.upper() == "CHANGE_PK":
                matching = [
                    s for s, sch in simulated_schemas.items() if sch.primary_key == tuple(pending_ddl.primary_key or ())
                ]
                waiting = tuple(sorted(set(active_shards) - set(matching)))
                comb_cols = self._merge_columns_across_shards(table, simulated_schemas)
                if comb_cols is None:
                    return CombinedCompatibilityResult(
                        table=table,
                        is_compatible=False,
                        progress_mode=DDLProgressMode.REFUSED,
                        care_badge=CareBadge.ACTION,
                        combined_schema=self.target_schemas.get(table),
                        affected_shards=active_shards,
                        waiting_shards=waiting,
                        reason=f"[ACTION] incompatible column types during CHANGE_PK on {table}",
                    )
                combined = ShardSchemaVersion(
                    shard_id="target",
                    table=table,
                    version=(self.target_schemas[table].version + 1) if table in self.target_schemas else 1,
                    columns=comb_cols,
                    primary_key=tuple(pending_ddl.primary_key or ()),
                )
                pk_sql = f"ALTER TABLE {table} DROP CONSTRAINT {table}_pkey, ADD PRIMARY KEY ({', '.join(combined.primary_key)})"
                return CombinedCompatibilityResult(
                    table=table,
                    is_compatible=True,
                    progress_mode=DDLProgressMode.PESSIMISTIC_BARRIER,
                    care_badge=CareBadge.PASS if not waiting else CareBadge.REVIEW,
                    combined_schema=combined,
                    affected_shards=active_shards,
                    waiting_shards=waiting,
                    target_ddl_statements=[pk_sql] if not waiting else [],
                    reason=(
                        f"[PASS] primary key converged across all shards on {table}"
                        if not waiting
                        else f"[REVIEW] primary key evolution requires barrier across affected shards on {table}"
                    ),
                )

        all_col_names: List[str] = []
        for s in active_shards:
            for cname in simulated_schemas[s].columns:
                if cname not in all_col_names:
                    all_col_names.append(cname)
        if table in self.target_schemas:
            for cname in self.target_schemas[table].columns:
                if cname not in all_col_names:
                    all_col_names.append(cname)

        merged_cols: Dict[str, ColumnSpec] = {}
        divergent_cols: List[str] = []
        requires_barrier = False
        waiting_set: Set[str] = set()

        if pending_ddl and pending_ddl.op_type.upper() in ("DROP_COLUMN", "RENAME_COLUMN"):
            requires_barrier = True

        for idx, cname in enumerate(all_col_names):
            present_in = [s for s in active_shards if cname in simulated_schemas[s].columns]
            missing_in = [s for s in active_shards if cname not in simulated_schemas[s].columns]

            if not present_in:
                continue

            if missing_in:
                divergent_cols.append(cname)

            first_spec = copy.deepcopy(simulated_schemas[present_in[0]].columns[cname])
            sup_type = first_spec.data_type
            sup_nullable = first_spec.nullable or bool(missing_in)
            sup_default = first_spec.default
            sup_gen = first_spec.generation
            origins: Set[str] = set(first_spec.origin_shards) | set(present_in)

            for s in present_in[1:]:
                other = simulated_schemas[s].columns[cname]
                if other.normalized_type() != ColumnSpec(name=cname, data_type=sup_type).normalized_type():
                    divergent_cols.append(cname)
                resolved, err = resolve_type_supremum(sup_type, other.data_type)
                if resolved is None:
                    return CombinedCompatibilityResult(
                        table=table,
                        is_compatible=False,
                        progress_mode=DDLProgressMode.REFUSED,
                        care_badge=CareBadge.ACTION,
                        combined_schema=self.target_schemas.get(table),
                        affected_shards=active_shards,
                        waiting_shards=(),
                        reason=f"[ACTION] {table}.{cname}: {err}",
                        divergent_columns=sorted(set(divergent_cols)),
                    )
                sup_type = resolved
                sup_nullable = sup_nullable or other.nullable
                if sup_default is None and other.default is not None:
                    sup_default = other.default
                sup_gen = max(sup_gen, other.generation)
                origins |= other.origin_shards

            is_added_not_null = (
                pending_ddl is not None
                and pending_ddl.op_type.upper() in ("ADD_COLUMN", "MODIFY_COLUMN", "ALTER_TYPE")
                and pending_ddl.column == cname
            )
            if not first_spec.nullable and missing_in and sup_default is None and (not requires_barrier or is_added_not_null):
                return CombinedCompatibilityResult(
                    table=table,
                    is_compatible=False,
                    progress_mode=DDLProgressMode.REFUSED,
                    care_badge=CareBadge.ACTION,
                    combined_schema=self.target_schemas.get(table),
                    affected_shards=active_shards,
                    waiting_shards=tuple(missing_in),
                    reason=(
                        f"[ACTION] {table}.{cname} is NOT NULL without default on {present_in} "
                        f"while missing on {missing_in}; concurrent writes unrepresentable"
                    ),
                    divergent_columns=sorted(set(divergent_cols)),
                )

            merged_cols[cname] = ColumnSpec(
                name=cname,
                data_type=sup_type,
                nullable=sup_nullable,
                default=sup_default,
                is_primary_key=cname in simulated_schemas[active_shards[0]].primary_key,
                ordinal=idx,
                generation=sup_gen,
                origin_shards=origins,
                dropped_by_shards=set(missing_in),
            )

        if pending_ddl is not None:
            op = pending_ddl.op_type.upper()
            if op == "DROP_COLUMN":
                col = pending_ddl.column or ""
                still_holding = [s for s in active_shards if col in simulated_schemas[s].columns]
                waiting_set.update(still_holding)
                if not still_holding and col in merged_cols:
                    merged_cols.pop(col, None)
            elif op == "RENAME_COLUMN":
                old_c = pending_ddl.column or ""
                new_c = pending_ddl.new_column_name or ""
                still_old = [
                    s
                    for s in active_shards
                    if old_c in simulated_schemas[s].columns and new_c not in simulated_schemas[s].columns
                ]
                waiting_set.update(still_old)
                if not still_old and old_c in merged_cols:
                    merged_cols.pop(old_c, None)

        cur_target = self.target_schemas.get(table)
        next_ver = (cur_target.version + 1) if cur_target else 1
        pk = simulated_schemas[active_shards[0]].primary_key
        combined_schema = ShardSchemaVersion(
            shard_id="target",
            table=table,
            version=next_ver,
            columns=merged_cols,
            primary_key=pk,
        )

        stmts = self._generate_target_ddl_statements(table, cur_target, combined_schema, pending_ddl, not waiting_set)

        if requires_barrier:
            mode = DDLProgressMode.SERIALIZED_AFFECTED
            badge = CareBadge.PASS if not waiting_set else CareBadge.REVIEW
            reason = (
                f"[PASS] barrier converged for {table} across {len(active_shards)} shards"
                if not waiting_set
                else f"[REVIEW] barrier waiting on shards {sorted(waiting_set)} for {table}"
            )
            affected = active_shards
        else:
            mode = DDLProgressMode.OPTIMISTIC
            badge = CareBadge.BENIGN if divergent_cols else CareBadge.PASS
            reason = (
                f"[{badge.value}] optimistic schema progress on {table}; combined target represents all shards"
            )
            affected = (pending_ddl.shard_id,) if pending_ddl else active_shards

        return CombinedCompatibilityResult(
            table=table,
            is_compatible=True,
            progress_mode=mode,
            care_badge=badge,
            combined_schema=combined_schema,
            affected_shards=affected,
            waiting_shards=tuple(sorted(waiting_set)),
            target_ddl_statements=stmts,
            reason=reason,
            divergent_columns=sorted(set(divergent_cols)),
        )

    def _merge_columns_across_shards(
        self,
        table: str,
        schemas: Dict[str, ShardSchemaVersion],
    ) -> Optional[Dict[str, ColumnSpec]]:
        merged: Dict[str, ColumnSpec] = {}
        for s, sch in schemas.items():
            for cname, cspec in sch.columns.items():
                if cname not in merged:
                    merged[cname] = copy.deepcopy(cspec)
                else:
                    sup, _ = resolve_type_supremum(merged[cname].data_type, cspec.data_type)
                    if sup is None:
                        return None
                    merged[cname].data_type = sup
                    merged[cname].nullable = merged[cname].nullable or cspec.nullable
                    merged[cname].origin_shards |= cspec.origin_shards
        return merged

    def _generate_target_ddl_statements(
        self,
        table: str,
        old_target: Optional[ShardSchemaVersion],
        new_target: ShardSchemaVersion,
        ddl: Optional[ShardDDLChange],
        barrier_complete: bool,
    ) -> List[str]:
        stmts: List[str] = []
        if old_target is None:
            return stmts

        if ddl and ddl.op_type.upper() == "RENAME_COLUMN" and barrier_complete:
            old_c = ddl.column or ""
            new_c = ddl.new_column_name or ""
            if old_c in old_target.columns and new_c in new_target.columns:
                stmts.append(f"ALTER TABLE {table} RENAME COLUMN {old_c} TO {new_c}")
                return stmts

        for cname, cspec in new_target.columns.items():
            mapped_type = map_type_to_dialect(cspec.data_type, self.target_dialect)
            if cname not in old_target.columns:
                null_sql = "NULL" if cspec.nullable else "NOT NULL"
                def_sql = f" DEFAULT {cspec.default!r}" if cspec.default is not None else ""
                stmts.append(f"ALTER TABLE {table} ADD COLUMN {cname} {mapped_type} {null_sql}{def_sql}")
            else:
                old_spec = old_target.columns[cname]
                if old_spec.normalized_type() != cspec.normalized_type():
                    stmts.append(f"ALTER TABLE {table} ALTER COLUMN {cname} TYPE {mapped_type}")

        if barrier_complete:
            for old_cname in old_target.columns:
                if old_cname not in new_target.columns:
                    stmts.append(f"ALTER TABLE {table} DROP COLUMN {old_cname}")

        return stmts

    def _recompute_combined_target_schema(self, table: str) -> Optional[ShardSchemaVersion]:
        res = self.compute_combined_compatibility(table)
        if res.is_compatible and res.combined_schema is not None:
            self.target_schemas[table] = res.combined_schema
            self._sync_target_store_schema(table)
            for stmt in res.target_ddl_statements:
                if stmt not in self.applied_target_ddl_set:
                    self.applied_target_ddl_set.add(stmt)
                    self.applied_target_ddls.append(stmt)
            return res.combined_schema
        return self.target_schemas.get(table)

    def _sync_target_store_schema(self, table: str, allow_drop: bool = False) -> None:
        target_schema = self.target_schemas.get(table)
        if target_schema is None:
            return
        for tgt_tables in self.target_store.values():
            rows = tgt_tables.get(table, {})
            for row in rows.values():
                for cname, cspec in target_schema.columns.items():
                    if cname not in row:
                        row[cname] = cspec.default
                if allow_drop:
                    for existing_col in list(row.keys()):
                        if existing_col not in target_schema.columns:
                            row.pop(existing_col, None)

    def submit_shard_ddl(self, ddl: ShardDDLChange) -> CombinedCompatibilityResult:
        shard_id = ddl.shard_id
        table = ddl.table
        if shard_id in self.retired_shards:
            raise ShardDDLError(f"shard {shard_id} is retired after split and cannot submit DDL")
        if shard_id not in self.shard_schemas or table not in self.shard_schemas[shard_id]:
            raise ShardDDLError(f"unknown shard {shard_id} or table {table}")

        compat = self.compute_combined_compatibility(table, pending_ddl=ddl)
        if not compat.is_compatible or compat.progress_mode == DDLProgressMode.REFUSED:
            raise UnrepresentableShardDDLError(compat.reason, care_badge=CareBadge.ACTION)

        evolved = self.shard_schemas[shard_id][table].evolve(ddl)
        self.shard_schemas[shard_id][table] = evolved
        if compare_watermark(ddl.position, self.shard_watermarks.get(shard_id, 0)) > 0:
            self.shard_watermarks[shard_id] = ddl.position

        for cname, cspec in evolved.columns.items():
            hist = self.column_history.setdefault(table, {})
            if cname not in hist:
                hist[cname] = copy.deepcopy(cspec)
            else:
                sup, _ = resolve_type_supremum(hist[cname].data_type, cspec.data_type)
                if sup:
                    hist[cname].data_type = sup
                hist[cname].origin_shards.add(shard_id)

        op = ddl.op_type.upper()
        if compat.progress_mode == DDLProgressMode.OPTIMISTIC:
            if compat.combined_schema is not None:
                self.target_schemas[table] = compat.combined_schema
                self._sync_target_store_schema(table, allow_drop=False)
            for stmt in compat.target_ddl_statements:
                if stmt not in self.applied_target_ddl_set:
                    self.applied_target_ddl_set.add(stmt)
                    self.applied_target_ddls.append(stmt)
            self._check_state_convergence_barriers(table)
            self._persist_if_configured()
            return compat

        barrier_key = self._barrier_key_for_ddl(ddl)
        active_shards = set(self.active_shards_for_table(table))
        barrier = self.active_barriers.get(barrier_key)
        if barrier is None:
            barrier = DDLConvergenceBarrier(
                barrier_id=barrier_key,
                table=table,
                op_type=op,
                expected_shards=active_shards,
                mode=compat.progress_mode,
                status=BarrierStatus.WAITING,
                care_badge=CareBadge.REVIEW,
                target_column=ddl.column,
                new_column_name=ddl.new_column_name,
                timeout_seconds=self.barrier_timeout_seconds,
            )
            self.active_barriers[barrier_key] = barrier
        else:
            barrier.expected_shards = active_shards

        converged = barrier.record_arrival(shard_id, ddl)
        if converged:
            self._finalize_converged_barrier(barrier, compat)
        else:
            self._detect_cross_barrier_deadlock(table)

        self._persist_if_configured()
        return self.compute_combined_compatibility(table) if converged else compat

    def _barrier_key_for_ddl(self, ddl: ShardDDLChange) -> str:
        op = ddl.op_type.upper()
        if op in ("DROP_COLUMN", "RENAME_COLUMN"):
            return f"{ddl.table}:{op}:{ddl.column or ''}->{ddl.new_column_name or ''}"
        if op == "CHANGE_PK":
            pk_str = ",".join(ddl.primary_key or ())
            return f"{ddl.table}:CHANGE_PK:{pk_str}"
        return f"{ddl.table}:{op}"

    def _finalize_converged_barrier(
        self,
        barrier: DDLConvergenceBarrier,
        compat: CombinedCompatibilityResult,
    ) -> None:
        table = barrier.table
        if barrier.op_type == "TRUNCATE":
            for tgt_tables in self.target_store.values():
                if table in tgt_tables:
                    tgt_tables[table].clear()
            for k in [k for k in self.key_owner if k[0] == table]:
                self.key_owner.pop(k, None)
            stmt = f"TRUNCATE TABLE {table}"
            self.applied_target_ddl_set.add(stmt)
            self.applied_target_ddls.append(stmt)
        else:
            recomputed = self.compute_combined_compatibility(table)
            if recomputed.combined_schema is not None:
                if barrier.op_type == "DROP_COLUMN" and barrier.target_column:
                    recomputed.combined_schema.columns.pop(barrier.target_column, None)
                elif barrier.op_type == "RENAME_COLUMN" and barrier.target_column and barrier.new_column_name:
                    recomputed.combined_schema.columns.pop(barrier.target_column, None)
                    self._migrate_renamed_column_in_target_store(
                        table, barrier.target_column, barrier.new_column_name
                    )
                self.target_schemas[table] = recomputed.combined_schema
                self._sync_target_store_schema(table, allow_drop=(barrier.op_type == "DROP_COLUMN"))
            for stmt in compat.target_ddl_statements + recomputed.target_ddl_statements:
                if stmt not in self.applied_target_ddl_set:
                    self.applied_target_ddl_set.add(stmt)
                    self.applied_target_ddls.append(stmt)

        barrier.status = BarrierStatus.APPLIED
        barrier.care_badge = CareBadge.PASS
        self.active_barriers.pop(barrier.barrier_id, None)
        self.completed_barriers.append(barrier)
        self._drain_held_events_for_table(table)

    def _migrate_renamed_column_in_target_store(self, table: str, old_col: str, new_col: str) -> None:
        for tgt_tables in self.target_store.values():
            rows = tgt_tables.get(table, {})
            for row in rows.values():
                if old_col in row and new_col not in row:
                    row[new_col] = row.pop(old_col)
                elif old_col in row:
                    row.pop(old_col, None)

    def _check_state_convergence_barriers(self, table: str) -> None:
        active_shards = self.active_shards_for_table(table)
        if not active_shards:
            return
        digests = {self.shard_schemas[s][table].schema_digest() for s in active_shards}
        if len(digests) == 1:
            for bkey in [k for k, b in self.active_barriers.items() if b.table == table]:
                bar = self.active_barriers[bkey]
                compat = self.compute_combined_compatibility(table)
                self._finalize_converged_barrier(bar, compat)

    def _detect_cross_barrier_deadlock(self, table: str) -> None:
        table_barriers = [
            b
            for b in self.active_barriers.values()
            if b.table == table and b.status == BarrierStatus.WAITING
        ]
        if len(table_barriers) < 2:
            return
        for i, b1 in enumerate(table_barriers):
            for b2 in table_barriers[i + 1 :]:
                b1_arrived = set(b1.arrived_shards.keys())
                b1_pending = set(b1.pending_shards)
                b2_arrived = set(b2.arrived_shards.keys())
                b2_pending = set(b2.pending_shards)
                if (b1_arrived & b2_pending) and (b2_arrived & b1_pending):
                    if b1.op_type in ("DROP_COLUMN", "RENAME_COLUMN") and b2.op_type in (
                        "DROP_COLUMN",
                        "RENAME_COLUMN",
                    ):
                        if b1.target_column != b2.target_column:
                            continue
                    b1.status = BarrierStatus.DEADLOCK_DETECTED
                    b1.care_badge = CareBadge.ACTION
                    b2.status = BarrierStatus.DEADLOCK_DETECTED
                    b2.care_badge = CareBadge.ACTION
                    raise ShardDeadlockError(
                        f"[ACTION] conflicting barrier cycle on {table} between {b1.barrier_id} and {b2.barrier_id}",
                        care_badge=CareBadge.ACTION,
                    )

    def check_barrier_timeouts(self, now: Optional[float] = None) -> List[DDLConvergenceBarrier]:
        cur = time.time() if now is None else float(now)
        expired: List[DDLConvergenceBarrier] = []
        for b in self.active_barriers.values():
            if b.status == BarrierStatus.WAITING and (cur - b.created_at) > b.timeout_seconds:
                b.status = BarrierStatus.DEADLOCK_DETECTED
                b.care_badge = CareBadge.ACTION
                expired.append(b)
        return expired

    def is_shard_held(self, shard_id: str, table: str) -> bool:
        for b in self.active_barriers.values():
            if b.table == table and b.status == BarrierStatus.WAITING:
                if shard_id in b.arrived_shards:
                    return True
        return False

    def apply_row_event(self, event: ShardedRowEvent) -> Optional[str]:
        shard_id = event.shard_id
        table = event.table
        if shard_id in self.retired_shards:
            for rec in self.split_records:
                if rec.parent_shard == shard_id and rec.table == table:
                    if compare_watermark(event.position, rec.split_watermark) <= 0:
                        return None
            raise ShardSplitError(
                f"[ACTION] write from retired parent shard {shard_id} after split watermark",
                care_badge=CareBadge.ACTION,
            )

        for rec in self.split_records:
            if shard_id in rec.child_shards and rec.table == table:
                if compare_watermark(event.position, rec.split_watermark) <= 0:
                    return None

        if shard_id not in self.shard_schemas or table not in self.shard_schemas[shard_id]:
            raise ShardDDLError(f"[ACTION] unregistered shard {shard_id} or table {table}")

        if self.is_shard_held(shard_id, table):
            self.held_events.setdefault((shard_id, table), []).append(event)
            self._persist_if_configured()
            return "HELD_AT_BARRIER"

        return self._execute_row_placement(event)

    def _execute_row_placement(self, event: ShardedRowEvent) -> str:
        shard_id = event.shard_id
        table = event.table
        shard_schema = self.shard_schemas[shard_id][table]
        target_schema = self.target_schemas.get(table) or shard_schema

        normalized_row = self._project_row_to_target_schema(event.row, shard_schema, target_schema)
        pk_cols = target_schema.primary_key or shard_schema.primary_key
        if not pk_cols:
            raise RoutingPlacementError(f"[ACTION] table {table} has no primary key for exact-once placement")
        pk_tuple = tuple(normalized_row.get(k) for k in pk_cols)
        if any(v is None for v in pk_tuple):
            raise RoutingPlacementError(f"[ACTION] table {table} row has NULL primary key component: {pk_tuple}")

        route_key_cols = self.routing_rule.key_for_table(table, fallback_pk=pk_cols)
        route_key_tuple = tuple(normalized_row.get(k) for k in route_key_cols)
        target_shard = self.routing_rule.place_key(table, route_key_tuple)
        if target_shard is None:
            raise RoutingPlacementError(
                f"[ACTION] table {table} key {route_key_tuple!r} places on no target under rule {self.routing_rule.by}"
            )

        op = event.op.lower()
        owner_entry = self.key_owner.get((table, pk_tuple))

        if op in ("insert", "update", "upsert"):
            if owner_entry is not None:
                prev_shard, prev_pos, prev_ver = owner_entry
                if prev_shard != shard_id and not self._is_split_lineage(prev_shard, shard_id):
                    if not self._resolve_duplicate_key(
                        table, pk_tuple, prev_shard, prev_pos, prev_ver, shard_id, event.position, shard_schema.version
                    ):
                        return target_shard

            if event.before:
                norm_before = self._project_row_to_target_schema(event.before, shard_schema, target_schema)
                old_route_key = tuple(norm_before.get(k, normalized_row.get(k)) for k in route_key_cols)
                old_target = self.routing_rule.place_key(table, old_route_key)
                if old_target and old_target != target_shard:
                    self.target_store.setdefault(old_target, {}).setdefault(table, {}).pop(pk_tuple, None)

            for other_tgt, tgt_tables in self.target_store.items():
                if other_tgt != target_shard and table in tgt_tables:
                    tgt_tables[table].pop(pk_tuple, None)

            existing = self.target_store.setdefault(target_shard, {}).setdefault(table, {}).get(pk_tuple)
            if existing is not None:
                merged = dict(existing)
                for k, v in normalized_row.items():
                    if v is not None or k in event.row:
                        merged[k] = v
                self.target_store[target_shard][table][pk_tuple] = merged
            else:
                self.target_store.setdefault(target_shard, {}).setdefault(table, {})[pk_tuple] = normalized_row

            self.key_owner[(table, pk_tuple)] = (shard_id, event.position, shard_schema.version)

        elif op == "delete":
            for tgt_tables in self.target_store.values():
                if table in tgt_tables:
                    tgt_tables[table].pop(pk_tuple, None)
            self.key_owner.pop((table, pk_tuple), None)
        else:
            raise ShardDDLError(f"[ACTION] unsupported row operation: {event.op}")

        if compare_watermark(event.position, self.shard_watermarks.get(shard_id, 0)) > 0:
            self.shard_watermarks[shard_id] = event.position
        self._persist_if_configured()
        return target_shard

    def _project_row_to_target_schema(
        self,
        row: Dict[str, Any],
        shard_schema: ShardSchemaVersion,
        target_schema: ShardSchemaVersion,
    ) -> Dict[str, Any]:
        mapped_input: Dict[str, Any] = {}
        for k, v in row.items():
            target_col = shard_schema.rename_aliases.get(k, k)
            if target_col not in target_schema.columns and k in target_schema.columns:
                target_col = k
            mapped_input[target_col] = v

        out: Dict[str, Any] = {}
        for cname, cspec in target_schema.columns.items():
            if cname in mapped_input:
                out[cname] = mapped_input[cname]
            else:
                alias_found = False
                for s_map in self.shard_schemas.values():
                    sch = s_map.get(target_schema.table)
                    if sch:
                        for old_n, new_n in sch.rename_aliases.items():
                            if new_n == cname and old_n in mapped_input:
                                out[cname] = mapped_input[old_n]
                                alias_found = True
                                break
                            if old_n == cname and new_n in mapped_input:
                                out[cname] = mapped_input[new_n]
                                alias_found = True
                                break
                if not alias_found:
                    out[cname] = cspec.default
        return out

    def _is_split_lineage(self, old_shard: str, new_shard: str) -> bool:
        for rec in self.split_records:
            if rec.parent_shard == old_shard and new_shard in rec.child_shards:
                return True
        return False

    def _resolve_duplicate_key(
        self,
        table: str,
        pk_tuple: Tuple[Any, ...],
        prev_shard: str,
        prev_pos: Union[int, str],
        prev_ver: int,
        new_shard: str,
        new_pos: Union[int, str],
        new_ver: int,
    ) -> bool:
        self.duplicate_key_conflicts.append((table, pk_tuple, prev_shard, new_shard))
        if self.conflict_policy == "refuse":
            raise DuplicateKeyPlacementError(
                f"[ACTION] duplicate key {pk_tuple!r} on table {table} between shards {prev_shard} and {new_shard}",
                care_badge=CareBadge.ACTION,
            )
        if self.conflict_policy == "highest_version":
            if new_ver != prev_ver:
                return new_ver > prev_ver
            return compare_watermark(new_pos, prev_pos) >= 0
        if self.conflict_policy == "source_priority":
            p_prev = self.shard_priority.index(prev_shard) if prev_shard in self.shard_priority else 999999
            p_new = self.shard_priority.index(new_shard) if new_shard in self.shard_priority else 999999
            if p_new != p_prev:
                return p_new < p_prev
            return compare_watermark(new_pos, prev_pos) >= 0
        return compare_watermark(new_pos, prev_pos) >= 0

    def _drain_held_events_for_table(self, table: str) -> None:
        keys = [k for k in sorted(self.held_events) if k[1] == table]
        for k in keys:
            shard_id, tbl = k
            if self.is_shard_held(shard_id, tbl):
                continue
            events = self.held_events.pop(k, [])
            for ev in events:
                self._execute_row_placement(ev)

    def register_shard_split(
        self,
        parent_shard: str,
        child_shards: Sequence[str],
        table: str,
        child_keyspaces: Dict[str, Union[KeySpace, Dict[str, Any]]],
        split_watermark: Union[int, str],
    ) -> ShardSplitRecord:
        if parent_shard not in self.shard_schemas or table not in self.shard_schemas[parent_shard]:
            raise ShardSplitError(f"[ACTION] unknown parent shard {parent_shard} for table {table}")
        if len(child_shards) < 2:
            raise ShardSplitError("[ACTION] shard split requires at least 2 child shards")

        norm_child_ks: Dict[str, Dict[str, Any]] = {}
        for c in child_shards:
            spec = child_keyspaces.get(c)
            if spec is None:
                raise ShardSplitError(f"[ACTION] missing keyspace for split child shard {c}")
            if isinstance(spec, KeySpace):
                d: Dict[str, Any] = {}
                if spec.lo is not None or spec.hi is not None:
                    d["range"] = [spec.lo, spec.hi]
                if spec.values is not None:
                    d["values"] = list(spec.values)
                norm_child_ks[c] = {table: d}
            else:
                norm_child_ks[c] = {table: dict(spec)}

        child_ov = overlaps(norm_child_ks, tables=[table])
        if child_ov:
            raise ShardSplitError(
                f"[ACTION] split child shards have overlapping keyspaces: {child_ov}",
                care_badge=CareBadge.ACTION,
            )

        parent_ks_spec = self.shard_keyspaces.get(parent_shard, {}).get(table)
        if parent_ks_spec:
            parent_ks = KeySpace.of(parent_ks_spec)
            if parent_ks.lo is not None and parent_ks.hi is not None:
                intervals = []
                for c in child_shards:
                    cks = KeySpace.of(norm_child_ks[c][table])
                    if cks.lo is None or cks.hi is None:
                        raise ShardSplitError(f"[ACTION] child shard {c} has unbounded range inside bounded parent")
                    intervals.append((cks.lo, cks.hi, c))
                intervals.sort(key=lambda x: x[0])
                if intervals[0][0] != parent_ks.lo or intervals[-1][1] != parent_ks.hi:
                    raise ShardSplitError(
                        f"[ACTION] child keyspaces {intervals} do not match parent bounds [{parent_ks.lo}, {parent_ks.hi}]"
                    )
                for idx in range(len(intervals) - 1):
                    if intervals[idx][1] + 1 != intervals[idx + 1][0] and intervals[idx][1] != intervals[idx + 1][0]:
                        raise ShardSplitError(
                            f"[ACTION] gap between split child shards {intervals[idx]} and {intervals[idx + 1]}"
                        )

        parent_sv = self.shard_schemas[parent_shard][table]
        for c in child_shards:
            child_sv = copy.deepcopy(parent_sv)
            child_sv.shard_id = c
            child_sv.position = split_watermark
            for col in child_sv.columns.values():
                col.origin_shards.add(c)
            self.shard_schemas.setdefault(c, {})[table] = child_sv
            self.shard_keyspaces.setdefault(c, {})[table] = norm_child_ks[c][table]
            self.shard_watermarks[c] = split_watermark

        for (s_id, tbl), held_list in list(self.held_events.items()):
            if s_id == parent_shard and tbl == table:
                self.held_events.pop((s_id, tbl), None)
                for ev in held_list:
                    pk_cols = parent_sv.primary_key
                    pk_val = ev.row.get(pk_cols[0]) if pk_cols else None
                    placed_child = child_shards[0]
                    for c in child_shards:
                        if KeySpace.of(norm_child_ks[c][table]).holds(pk_val):
                            placed_child = c
                            break
                    cloned_ev = copy.deepcopy(ev)
                    cloned_ev.shard_id = placed_child
                    self.held_events.setdefault((placed_child, table), []).append(cloned_ev)

        self.retired_shards.add(parent_shard)
        for barrier in self.active_barriers.values():
            if barrier.table == table:
                barrier.apply_shard_split(parent_shard, child_shards)

        rec = ShardSplitRecord(
            parent_shard=parent_shard,
            child_shards=tuple(child_shards),
            table=table,
            split_watermark=split_watermark,
            inherited_schema_version=parent_sv.version,
            child_keyspaces={c: norm_child_ks[c][table] for c in child_shards},
        )
        self.split_records.append(rec)
        self._recompute_combined_target_schema(table)
        self._persist_if_configured()
        return rec

    def verify_placement_and_schema(self, table: str) -> PlacementVerificationReport:
        target_schema = self.target_schemas.get(table)
        lost_cols: List[Tuple[str, str]] = []
        active_shards = self.active_shards_for_table(table)

        for s in active_shards:
            sch = self.shard_schemas[s][table]
            for cname in sch.columns:
                if target_schema is None or cname not in target_schema.columns:
                    alias = sch.rename_aliases.get(cname)
                    if not (alias and target_schema and alias in target_schema.columns):
                        lost_cols.append((s, cname))

        whole_digests: Dict[str, Dict[Tuple[Any, ...], str]] = {table: {}}
        parts_digests: Dict[str, Dict[str, Dict[Tuple[Any, ...], str]]] = {
            tgt: {table: {}} for tgt in self.routing_rule.targets
        }

        pk_cols = target_schema.primary_key if target_schema else ("id",)
        route_cols = self.routing_rule.key_for_table(table, fallback_pk=pk_cols)

        for tgt, tgt_tables in self.target_store.items():
            rows = tgt_tables.get(table, {})
            for pk_tuple, row in rows.items():
                row_digest = hashlib.sha256(
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode("ascii")
                ).hexdigest()[:16]
                parts_digests.setdefault(tgt, {}).setdefault(table, {})[pk_tuple] = row_digest

        for (tbl, pk_tuple) in sorted(self.key_owner):
            if tbl != table:
                continue
            found_rows = []
            for tgt, tgt_tables in self.target_store.items():
                r = tgt_tables.get(table, {}).get(pk_tuple)
                if r is not None:
                    found_rows.append((tgt, r))
            if found_rows:
                canonical_row = found_rows[0][1]
                whole_digests[table][pk_tuple] = hashlib.sha256(
                    json.dumps(canonical_row, sort_keys=True, separators=(",", ":"), default=str).encode("ascii")
                ).hexdigest()[:16]

        class _RouteAdapter:
            def __init__(self, rule: ShardRoutingRule, store: Dict[str, Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]]]):
                self.rule = rule
                self.store = store

            def place(self, tbl: str, pk_key: Tuple[Any, ...]) -> Optional[str]:
                for tgt_tables in self.store.values():
                    row = tgt_tables.get(tbl, {}).get(pk_key)
                    if row is not None:
                        r_cols = self.rule.key_for_table(tbl, fallback_pk=pk_cols)
                        r_tuple = tuple(row.get(c) for c in r_cols)
                        return self.rule.place_key(tbl, r_tuple)
                return self.rule.place_key(tbl, pk_key)

        proof = prove_partition(whole_digests, parts_digests, route=_RouteAdapter(self.routing_rule, self.target_store))
        total_rows = sum(len(t.get(table, {})) for t in self.target_store.values())
        ok = proof.ok and not lost_cols

        if ok:
            badge = CareBadge.PASS
            summary = f"[PASS] {total_rows} rows placed once on {table}; 0 lost columns; proof: {proof.words()}"
        else:
            badge = CareBadge.ACTION
            summary = f"[ACTION] verification failed on {table}: lost_columns={lost_cols}, proof={proof.words()}"

        return PlacementVerificationReport(
            ok=ok,
            care_badge=badge,
            total_rows_verified=total_rows,
            proof=proof,
            lost_columns=lost_cols,
            summary=summary,
        )

    def to_state_dict(self) -> Dict[str, Any]:
        serialized_store: Dict[str, Dict[str, List[List[Any]]]] = {}
        for tgt, tbl_map in self.target_store.items():
            serialized_store[tgt] = {}
            for tbl, rows in tbl_map.items():
                serialized_store[tgt][tbl] = [[list(pk), row] for pk, row in rows.items()]

        return {
            "routing_rule": self.routing_rule.to_dict(),
            "target_dialect": self.target_dialect,
            "conflict_policy": self.conflict_policy,
            "shard_priority": list(self.shard_priority),
            "barrier_timeout_seconds": self.barrier_timeout_seconds,
            "shard_schemas": {
                s: {t: sv.to_dict() for t, sv in tbls.items()} for s, tbls in self.shard_schemas.items()
            },
            "target_schemas": {t: sv.to_dict() for t, sv in self.target_schemas.items()},
            "shard_keyspaces": self.shard_keyspaces,
            "shard_watermarks": self.shard_watermarks,
            "retired_shards": sorted(self.retired_shards),
            "split_records": [r.to_dict() for r in self.split_records],
            "active_barriers": {k: b.to_dict() for k, b in self.active_barriers.items()},
            "completed_barriers": [b.to_dict() for b in self.completed_barriers],
            "held_events": [
                {"shard_id": k[0], "table": k[1], "events": [e.to_dict() for e in evs]}
                for k, evs in self.held_events.items()
            ],
            "applied_target_ddls": list(self.applied_target_ddls),
            "target_store": serialized_store,
            "key_owner": [
                {"table": k[0], "pk": list(k[1]), "owner": list(v)} for k, v in self.key_owner.items()
            ],
            "column_history": {
                t: {c: spec.to_dict() for c, spec in cmap.items()} for t, cmap in self.column_history.items()
            },
        }

    def save_state(self, path: Optional[Union[str, Path]] = None) -> Path:
        target = Path(path) if path else self.state_path
        if target is None:
            raise ValueError("no state_path configured for save_state")
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.to_state_dict(), indent=2, sort_keys=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".shard_ddl_", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="ascii") as f:
                f.write(payload)
            os.replace(tmp_name, target)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        return target

    @classmethod
    def load_state(cls, path: Union[str, Path]) -> "ShardDDLCoordinator":
        p = Path(path)
        raw = json.loads(p.read_text(encoding="ascii"))
        return cls.from_state_dict(raw, state_path=p)

    @classmethod
    def from_state_dict(
        cls,
        data: Dict[str, Any],
        state_path: Optional[Union[str, Path]] = None,
    ) -> "ShardDDLCoordinator":
        coord = cls(
            routing_rule=ShardRoutingRule.from_dict(data["routing_rule"]),
            target_dialect=data.get("target_dialect", "postgres"),
            conflict_policy=data.get("conflict_policy", "refuse"),
            shard_priority=data.get("shard_priority", []),
            state_path=state_path,
            barrier_timeout_seconds=float(data.get("barrier_timeout_seconds", 300.0)),
        )
        for s, tbls in data.get("shard_schemas", {}).items():
            coord.shard_schemas[s] = {t: ShardSchemaVersion.from_dict(sv) for t, sv in tbls.items()}
        for t, sv in data.get("target_schemas", {}).items():
            coord.target_schemas[t] = ShardSchemaVersion.from_dict(sv)
        coord.shard_keyspaces = dict(data.get("shard_keyspaces", {}))
        coord.shard_watermarks = dict(data.get("shard_watermarks", {}))
        coord.retired_shards = set(data.get("retired_shards", []))
        coord.split_records = [ShardSplitRecord.from_dict(r) for r in data.get("split_records", [])]
        coord.active_barriers = {
            k: DDLConvergenceBarrier.from_dict(b) for k, b in data.get("active_barriers", {}).items()
        }
        coord.completed_barriers = [
            DDLConvergenceBarrier.from_dict(b) for b in data.get("completed_barriers", [])
        ]
        for item in data.get("held_events", []):
            key = (str(item["shard_id"]), str(item["table"]))
            coord.held_events[key] = [ShardedRowEvent.from_dict(e) for e in item.get("events", [])]
        coord.applied_target_ddls = list(data.get("applied_target_ddls", []))
        coord.applied_target_ddl_set = set(coord.applied_target_ddls)

        for tgt, tbl_map in data.get("target_store", {}).items():
            coord.target_store.setdefault(tgt, {})
            for tbl, entries in tbl_map.items():
                coord.target_store[tgt][tbl] = {tuple(pk): dict(row) for pk, row in entries}

        for entry in data.get("key_owner", []):
            coord.key_owner[(str(entry["table"]), tuple(entry["pk"]))] = (
                str(entry["owner"][0]),
                entry["owner"][1],
                int(entry["owner"][2]),
            )

        for t, cmap in data.get("column_history", {}).items():
            coord.column_history[t] = {c: ColumnSpec.from_dict(spec) for c, spec in cmap.items()}

        for t in list(coord.target_schemas):
            coord._recompute_combined_target_schema(t)
            for b in list(coord.active_barriers.values()):
                if b.table == t and b.is_converged:
                    compat = coord.compute_combined_compatibility(t)
                    coord._finalize_converged_barrier(b, compat)

        return coord

    def _persist_if_configured(self) -> None:
        if self.state_path is not None:
            self.save_state(self.state_path)
