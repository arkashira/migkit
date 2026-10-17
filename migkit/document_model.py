"""Normalized-to-document transformation and update/delete propagation verifier (Row 636: RCH28.document-model).

Provides:
- Mapping-aware verification for relational-to-document transformations:
  1:1 nested subdocuments, 1:N embedded arrays, N:1 embedded reference lookups,
  multi-level nested paths, path identity verification, type conversions,
  semantic vs unordered array ordering, and root/nested multiplicity checks.
- Target shape measurement and deterministic access strategy selection:
  INDEXED_LOOKUP, BATCHED_FETCH, or STREAM_COMPARE based on collection size,
  document byte size, change ratio, and index availability.
- Separate verification of update/delete propagation for embedded references
  and child arrays (fan-out updates, orphan child deletes, child reparenting)
  distinct from static snapshot equivalence.
- Strict failure (ACTION) when collection document counts match but nested paths
  are misplaced, embedded children are missing/orphaned, or semantically ordered
  arrays are reordered.
- Explicit concurrent-source assumptions (watermarks, in-flight keys, lag bounds).
- O(1) typed durable checkpoint resume and active verification lease lock (.run.lock).
- Pure UNIX style reporting with 4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
"""
from __future__ import annotations

import copy
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
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union


# ---------------------------------------------------------------------------
# Enums and Care Badges
# ---------------------------------------------------------------------------


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class RelationMultiplicity(str, enum.Enum):
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"
    MANY_TO_MANY = "M:N"


class OrderSemantics(str, enum.Enum):
    SEMANTIC_ORDERED = "SEMANTIC_ORDERED"
    UNORDERED_KEYED = "UNORDERED_KEYED"
    SCALAR_SET = "SCALAR_SET"


class DeletePropagationPolicy(str, enum.Enum):
    REMOVE_ELEMENT = "REMOVE_ELEMENT"
    NULLIFY_REFERENCE = "NULLIFY_REFERENCE"
    CASCADE_DELETE_DOCUMENT = "CASCADE_DELETE_DOCUMENT"
    RETAIN_HISTORICAL_SNAPSHOT = "RETAIN_HISTORICAL_SNAPSHOT"


class TypeConversionKind(str, enum.Enum):
    IDENTITY = "IDENTITY"
    DECIMAL_TO_DECIMAL128_STR = "DECIMAL_TO_DECIMAL128_STR"
    DECIMAL_TO_CENTS_INT = "DECIMAL_TO_CENTS_INT"
    DECIMAL_TO_FLOAT_TOLERANT = "DECIMAL_TO_FLOAT_TOLERANT"
    DATETIME_TO_ISO8601 = "DATETIME_TO_ISO8601"
    DATETIME_TO_EPOCH_MS = "DATETIME_TO_EPOCH_MS"
    UUID_TO_STRING = "UUID_TO_STRING"
    INT_TO_BOOL = "INT_TO_BOOL"
    NULL_TO_ABSENT = "NULL_TO_ABSENT"
    ABSENT_TO_NULL = "ABSENT_TO_NULL"
    CUSTOM = "CUSTOM"


class VerificationFetchStrategy(str, enum.Enum):
    INDEXED_LOOKUP = "INDEXED_LOOKUP"
    BATCHED_FETCH = "BATCHED_FETCH"
    STREAM_COMPARE = "STREAM_COMPARE"


class DefectCategory(str, enum.Enum):
    ROOT_MULTIPLICITY_MISMATCH = "ROOT_MULTIPLICITY_MISMATCH"
    MISSING_ROOT_DOCUMENT = "MISSING_ROOT_DOCUMENT"
    UNEXPECTED_ROOT_DOCUMENT = "UNEXPECTED_ROOT_DOCUMENT"
    PATH_IDENTITY_MISMATCH = "PATH_IDENTITY_MISMATCH"
    MISSING_EMBEDDED_CHILD = "MISSING_EMBEDDED_CHILD"
    ORPHAN_EMBEDDED_CHILD = "ORPHAN_EMBEDDED_CHILD"
    NESTED_MULTIPLICITY_MISMATCH = "NESTED_MULTIPLICITY_MISMATCH"
    SEMANTIC_ORDER_VIOLATION = "SEMANTIC_ORDER_VIOLATION"
    TYPE_CONVERSION_MISMATCH = "TYPE_CONVERSION_MISMATCH"
    TOLERATED_COERCION_DRIFT = "TOLERATED_COERCION_DRIFT"
    VALUE_MISMATCH = "VALUE_MISMATCH"
    STALE_EMBEDDED_REFERENCE = "STALE_EMBEDDED_REFERENCE"
    INCOMPLETE_REFERENCE_FANOUT = "INCOMPLETE_REFERENCE_FANOUT"
    REPARENT_PROPAGATION_FAILURE = "REPARENT_PROPAGATION_FAILURE"
    DELETE_PROPAGATION_FAILURE = "DELETE_PROPAGATION_FAILURE"
    HISTORICAL_SNAPSHOT_RETAINED = "HISTORICAL_SNAPSHOT_RETAINED"
    CONCURRENT_SOURCE_WINDOW_DRIFT = "CONCURRENT_SOURCE_WINDOW_DRIFT"
    CONCURRENT_ASSUMPTION_VIOLATION = "CONCURRENT_ASSUMPTION_VIOLATION"


class ChangeOp(str, enum.Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class PropagationKind(str, enum.Enum):
    ROOT_INSERT = "ROOT_INSERT"
    ROOT_UPDATE = "ROOT_UPDATE"
    ROOT_DELETE = "ROOT_DELETE"
    EMBEDDED_CHILD_INSERT = "EMBEDDED_CHILD_INSERT"
    EMBEDDED_CHILD_UPDATE = "EMBEDDED_CHILD_UPDATE"
    EMBEDDED_CHILD_DELETE = "EMBEDDED_CHILD_DELETE"
    EMBEDDED_CHILD_REPARENT = "EMBEDDED_CHILD_REPARENT"
    EMBEDDED_REF_FANOUT_UPDATE = "EMBEDDED_REF_FANOUT_UPDATE"
    EMBEDDED_REF_DELETE = "EMBEDDED_REF_DELETE"


class ConcurrencyMode(str, enum.Enum):
    QUIESCED_SOURCE = "QUIESCED_SOURCE"
    MVCC_CONSISTENT_CUT = "MVCC_CONSISTENT_CUT"
    WATERMARK_ALIGNED_CDC = "WATERMARK_ALIGNED_CDC"
    LIVE_CONCURRENT_WINDOW = "LIVE_CONCURRENT_WINDOW"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DocumentVerificationError(RuntimeError):
    """Base error for document model verification."""


class ActiveVerificationLeaseError(DocumentVerificationError):
    """Raised when an active verification lease lock is already held."""


# ---------------------------------------------------------------------------
# Path Resolution Utilities
# ---------------------------------------------------------------------------

_ABSENT = object()


def get_nested_path(doc: Dict[str, Any], path: str, default: Any = _ABSENT) -> Any:
    """Resolves a dot-separated path inside a nested dictionary."""
    if not path:
        return doc
    curr: Any = doc
    for part in path.split("."):
        if isinstance(curr, dict) and part in curr:
            curr = curr[part]
        else:
            return default
    return curr


def set_nested_path(doc: Dict[str, Any], path: str, value: Any) -> None:
    """Sets a dot-separated path inside a nested dictionary, creating intermediate dicts."""
    parts = path.split(".")
    curr = doc
    for part in parts[:-1]:
        if part not in curr or not isinstance(curr[part], dict):
            curr[part] = {}
        curr = curr[part]
    curr[parts[-1]] = value


def delete_nested_path(doc: Dict[str, Any], path: str) -> bool:
    """Deletes a dot-separated path if present. Returns True if removed."""
    parts = path.split(".")
    curr: Any = doc
    for part in parts[:-1]:
        if isinstance(curr, dict) and part in curr:
            curr = curr[part]
        else:
            return False
    if isinstance(curr, dict) and parts[-1] in curr:
        del curr[parts[-1]]
        return True
    return False


def flatten_leaf_paths(
    obj: Any,
    prefix: str = "",
    stop_at_arrays: bool = True,
) -> Dict[str, Any]:
    """Flattens a nested dict into dot-separated leaf paths."""
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        if not obj and prefix:
            out[prefix] = {}
            return out
        for k, v in obj.items():
            sub_path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out.update(flatten_leaf_paths(v, sub_path, stop_at_arrays=stop_at_arrays))
            elif isinstance(v, list) and not stop_at_arrays:
                for idx, item in enumerate(v):
                    out.update(flatten_leaf_paths(item, f"{sub_path}[{idx}]", stop_at_arrays=stop_at_arrays))
            else:
                out[sub_path] = v
    else:
        out[prefix] = obj
    return out


# ---------------------------------------------------------------------------
# Type Conversion Specifications
# ---------------------------------------------------------------------------


@dataclass
class TypeConversionSpec:
    kind: TypeConversionKind = TypeConversionKind.IDENTITY
    float_tolerance: float = 1e-6
    custom_converter: Optional[Callable[[Any], Any]] = None
    custom_comparator: Optional[Callable[[Any, Any], bool]] = None

    def convert(self, value: Any) -> Any:
        if value is _ABSENT:
            if self.kind == TypeConversionKind.ABSENT_TO_NULL:
                return None
            return _ABSENT
        if value is None:
            if self.kind == TypeConversionKind.NULL_TO_ABSENT:
                return _ABSENT
            return None

        if self.kind == TypeConversionKind.IDENTITY:
            return copy.deepcopy(value)
        if self.kind == TypeConversionKind.DECIMAL_TO_DECIMAL128_STR:
            if isinstance(value, decimal.Decimal):
                return f"Decimal128('{str(value)}')"
            return f"Decimal128('{decimal.Decimal(str(value))}')"
        if self.kind == TypeConversionKind.DECIMAL_TO_CENTS_INT:
            d = value if isinstance(value, decimal.Decimal) else decimal.Decimal(str(value))
            return int((d * 100).to_integral_value(rounding=decimal.ROUND_HALF_UP))
        if self.kind == TypeConversionKind.DECIMAL_TO_FLOAT_TOLERANT:
            return float(value)
        if self.kind == TypeConversionKind.DATETIME_TO_ISO8601:
            if isinstance(value, datetime.datetime):
                if value.tzinfo is None:
                    return value.isoformat() + "Z"
                return value.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
            if isinstance(value, datetime.date):
                return value.isoformat()
            return str(value)
        if self.kind == TypeConversionKind.DATETIME_TO_EPOCH_MS:
            if isinstance(value, datetime.datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=datetime.timezone.utc)
                return int(round(value.timestamp() * 1000))
            return int(value)
        if self.kind == TypeConversionKind.UUID_TO_STRING:
            if isinstance(value, uuid.UUID):
                return str(value).lower()
            return str(value).lower()
        if self.kind == TypeConversionKind.INT_TO_BOOL:
            return bool(int(value)) if isinstance(value, (int, str)) else bool(value)
        if self.kind in (TypeConversionKind.NULL_TO_ABSENT, TypeConversionKind.ABSENT_TO_NULL):
            return copy.deepcopy(value)
        if self.kind == TypeConversionKind.CUSTOM and self.custom_converter is not None:
            return self.custom_converter(value)
        return copy.deepcopy(value)

    def compare(self, expected_val: Any, actual_val: Any) -> Tuple[bool, Optional[CareBadge], str]:
        """Returns (matches, optional_badge_override, detail)."""
        if expected_val is _ABSENT and actual_val is _ABSENT:
            return True, None, "Both absent"
        if expected_val is _ABSENT and actual_val is None and self.kind == TypeConversionKind.NULL_TO_ABSENT:
            return False, CareBadge.ACTION, "Expected NULL to be omitted (absent), but target contains explicit null"
        if expected_val is None and actual_val is _ABSENT and self.kind == TypeConversionKind.ABSENT_TO_NULL:
            return False, CareBadge.ACTION, "Expected explicit null, but field is absent on target"
        if expected_val is _ABSENT or actual_val is _ABSENT:
            return False, CareBadge.ACTION, "Presence/absence mismatch"

        if self.custom_comparator is not None:
            ok = self.custom_comparator(expected_val, actual_val)
            return ok, (CareBadge.PASS if ok else CareBadge.ACTION), "Custom comparator evaluation"

        if self.kind == TypeConversionKind.DECIMAL_TO_FLOAT_TOLERANT:
            if isinstance(expected_val, (int, float, decimal.Decimal)) and isinstance(
                actual_val, (int, float, decimal.Decimal)
            ):
                if type(actual_val) is bool or type(expected_val) is bool:
                    return False, CareBadge.ACTION, "Boolean cannot substitute numeric float"
                diff = abs(float(expected_val) - float(actual_val))
                if diff == 0.0:
                    return True, CareBadge.PASS, "Exact float match"
                if diff <= self.float_tolerance:
                    return True, CareBadge.REVIEW, (
                        f"Float coercion drift {diff:.2e} within tolerance {self.float_tolerance:.2e}"
                    )
                return False, CareBadge.ACTION, (
                    f"Float drift {diff:.2e} exceeds tolerance {self.float_tolerance:.2e}"
                )

        if type(expected_val) is not type(actual_val):
            return False, CareBadge.ACTION, (
                f"Type conversion mismatch: expected type {type(expected_val).__name__} "
                f"({expected_val!r}), got {type(actual_val).__name__} ({actual_val!r})"
            )

        if expected_val == actual_val:
            return True, CareBadge.PASS, "Exact value and type match"
        return False, CareBadge.ACTION, f"Value mismatch: expected {expected_val!r}, got {actual_val!r}"


# ---------------------------------------------------------------------------
# Mapping Rules: Root Paths, Embedded Subdocuments, Nested Arrays, References
# ---------------------------------------------------------------------------


@dataclass
class FieldPathRule:
    source_column: str
    target_path: str
    conversion: TypeConversionSpec = field(default_factory=TypeConversionSpec)
    required: bool = False


@dataclass
class EmbeddedRelationRule:
    """Defines how a normalized child or reference entity embeds into a document."""

    rule_id: str
    parent_entity: str
    child_entity: str
    parent_key: str
    child_foreign_key: str
    child_identity_key: str
    target_path: str
    multiplicity: RelationMultiplicity
    field_mappings: List[FieldPathRule]
    order_semantics: OrderSemantics = OrderSemantics.SEMANTIC_ORDERED
    order_by_fields: List[str] = field(default_factory=list)
    delete_policy: DeletePropagationPolicy = DeletePropagationPolicy.REMOVE_ELEMENT
    parent_array_path: Optional[str] = None
    join_entity: Optional[str] = None
    join_parent_fk: Optional[str] = None
    join_child_fk: Optional[str] = None

    def project_child_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        doc: Dict[str, Any] = {}
        for fm in self.field_mappings:
            raw_val = row.get(fm.source_column, _ABSENT)
            conv_val = fm.conversion.convert(raw_val)
            if conv_val is not _ABSENT:
                set_nested_path(doc, fm.target_path, conv_val)
        return doc

    def get_target_identity_field(self) -> str:
        for fm in self.field_mappings:
            if fm.source_column == self.child_identity_key:
                return fm.target_path
        return self.child_identity_key

    def sort_key(self, source_row: Dict[str, Any]) -> Tuple[Any, ...]:
        cols = self.order_by_fields if self.order_by_fields else [self.child_identity_key]
        return tuple(str(source_row.get(c, "")) if not isinstance(source_row.get(c), (int, float)) else source_row.get(c) for c in cols)


@dataclass
class DocumentMappingSpec:
    """Complete mapping specification from normalized tables to a target document collection."""

    mapping_id: str
    version: str
    root_entity: str
    root_source_pk: str
    target_collection: str
    target_root_key_path: str = "_id"
    root_field_mappings: List[FieldPathRule] = field(default_factory=list)
    embedded_relations: List[EmbeddedRelationRule] = field(default_factory=list)
    filter_predicate: Optional[Callable[[Dict[str, Any]], bool]] = None

    def checksum(self) -> str:
        parts = [
            self.mapping_id,
            self.version,
            self.root_entity,
            self.root_source_pk,
            self.target_collection,
            self.target_root_key_path,
        ]
        for fm in self.root_field_mappings:
            parts.append(f"root:{fm.source_column}->{fm.target_path}:{fm.conversion.kind.value}")
        for rel in self.embedded_relations:
            rel_fields = ",".join(f"{m.source_column}->{m.target_path}:{m.conversion.kind.value}" for m in rel.field_mappings)
            parts.append(
                f"rel:{rel.rule_id}:{rel.parent_entity}->{rel.child_entity}:{rel.multiplicity.value}:"
                f"{rel.target_path}:{rel.order_semantics.value}:{rel.delete_policy.value}:[{rel_fields}]"
            )
        return hashlib.sha256("|".join(parts).encode("ascii", errors="replace")).hexdigest()

    def all_expected_leaf_paths(self) -> Dict[str, str]:
        """Maps bare leaf field names to their canonical full dot-paths for misplaced-path detection."""
        leaf_map: Dict[str, str] = {}
        for fm in self.root_field_mappings:
            leaf_name = fm.target_path.split(".")[-1]
            leaf_map[leaf_name] = fm.target_path
            leaf_map[fm.source_column] = fm.target_path
        for rel in self.embedded_relations:
            if rel.multiplicity in (RelationMultiplicity.ONE_TO_ONE, RelationMultiplicity.MANY_TO_ONE) and not rel.parent_array_path:
                for fm in rel.field_mappings:
                    full_p = f"{rel.target_path}.{fm.target_path}"
                    leaf_name = fm.target_path.split(".")[-1]
                    leaf_map[leaf_name] = full_p
                    leaf_map[fm.source_column] = full_p
        return leaf_map


# ---------------------------------------------------------------------------
# Target Shape Measurement & Access Strategy Selection
# ---------------------------------------------------------------------------


@dataclass
class MeasuredTargetShape:
    collection_name: str
    total_documents: int
    avg_document_bytes: int
    changed_root_keys_count: int = 0
    affected_embedded_refs_count: int = 0
    has_root_key_index: bool = True
    has_embedded_path_index: bool = False
    max_nested_array_len: int = 0
    network_rtt_ms: float = 1.0

    @classmethod
    def measure_from_documents(
        cls,
        collection_name: str,
        documents: Sequence[Dict[str, Any]],
        changed_root_keys_count: int = 0,
        affected_embedded_refs_count: int = 0,
        has_root_key_index: bool = True,
        has_embedded_path_index: bool = False,
        network_rtt_ms: float = 1.0,
    ) -> MeasuredTargetShape:
        total = len(documents)
        if total == 0:
            return cls(
                collection_name=collection_name,
                total_documents=0,
                avg_document_bytes=0,
                changed_root_keys_count=changed_root_keys_count,
                affected_embedded_refs_count=affected_embedded_refs_count,
                has_root_key_index=has_root_key_index,
                has_embedded_path_index=has_embedded_path_index,
                max_nested_array_len=0,
                network_rtt_ms=network_rtt_ms,
            )
        total_bytes = 0
        max_arr = 0
        for d in documents:
            encoded = json.dumps(d, default=str).encode("utf-8")
            total_bytes += len(encoded)
            for v in d.values():
                if isinstance(v, list) and len(v) > max_arr:
                    max_arr = len(v)
        return cls(
            collection_name=collection_name,
            total_documents=total,
            avg_document_bytes=max(1, total_bytes // total),
            changed_root_keys_count=changed_root_keys_count,
            affected_embedded_refs_count=affected_embedded_refs_count,
            has_root_key_index=has_root_key_index,
            has_embedded_path_index=has_embedded_path_index,
            max_nested_array_len=max_arr,
            network_rtt_ms=network_rtt_ms,
        )


@dataclass
class VerificationAccessPlan:
    strategy: VerificationFetchStrategy
    batch_size: int
    estimated_round_trips: int
    estimated_io_bytes: int
    change_ratio: float
    used_index: str
    rationale: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "batch_size": self.batch_size,
            "estimated_round_trips": self.estimated_round_trips,
            "estimated_io_bytes": self.estimated_io_bytes,
            "change_ratio": round(self.change_ratio, 6),
            "used_index": self.used_index,
            "rationale": self.rationale,
        }


def select_verification_strategy(
    shape: MeasuredTargetShape,
    indexed_lookup_max_keys: int = 64,
    indexed_lookup_max_ratio: float = 0.05,
    stream_compare_ratio_threshold: float = 0.30,
    batched_fetch_max_keys: int = 10000,
    target_batch_bytes: int = 4 * 1024 * 1024,
) -> VerificationAccessPlan:
    """Deterministically selects INDEXED_LOOKUP, BATCHED_FETCH, or STREAM_COMPARE from measured target shape."""
    total_docs = max(shape.total_documents, 0)
    avg_bytes = max(shape.avg_document_bytes, 256)

    if total_docs == 0:
        return VerificationAccessPlan(
            strategy=VerificationFetchStrategy.INDEXED_LOOKUP,
            batch_size=1,
            estimated_round_trips=1,
            estimated_io_bytes=0,
            change_ratio=0.0,
            used_index="_id" if shape.has_root_key_index else "none",
            rationale="Empty target collection (0 documents); trivial single check.",
        )

    effective_keys = shape.changed_root_keys_count
    if effective_keys == 0 and shape.affected_embedded_refs_count > 0 and shape.has_embedded_path_index:
        effective_keys = shape.affected_embedded_refs_count
        index_name = "embedded_path_idx"
        can_use_index = True
    else:
        index_name = "_id" if shape.has_root_key_index else "none"
        can_use_index = shape.has_root_key_index

    # Full snapshot verification (effective_keys == 0 means verify entire collection)
    is_full_snapshot = (effective_keys == 0) or (effective_keys >= total_docs)
    change_ratio = 1.0 if is_full_snapshot else (effective_keys / float(total_docs))

    stream_batch_size = max(10, min(5000, target_batch_bytes // avg_bytes))

    if not can_use_index:
        return VerificationAccessPlan(
            strategy=VerificationFetchStrategy.STREAM_COMPARE,
            batch_size=stream_batch_size,
            estimated_round_trips=max(1, math.ceil(total_docs / stream_batch_size)),
            estimated_io_bytes=total_docs * avg_bytes,
            change_ratio=change_ratio,
            used_index="collection_scan_cursor",
            rationale=(
                f"Target collection '{shape.collection_name}' lacks a usable key/embedded index; "
                f"selected STREAM_COMPARE to avoid O(N*K) unindexed lookups across {total_docs} documents."
            ),
        )

    if is_full_snapshot or change_ratio > stream_compare_ratio_threshold or effective_keys > batched_fetch_max_keys:
        return VerificationAccessPlan(
            strategy=VerificationFetchStrategy.STREAM_COMPARE,
            batch_size=stream_batch_size,
            estimated_round_trips=max(1, math.ceil(total_docs / stream_batch_size)),
            estimated_io_bytes=total_docs * avg_bytes,
            change_ratio=change_ratio,
            used_index=index_name,
            rationale=(
                f"High verification coverage (change_ratio={change_ratio:.1%}, keys={effective_keys or total_docs}/{total_docs}); "
                f"selected STREAM_COMPARE with cursor batch_size={stream_batch_size}."
            ),
        )

    if (
        effective_keys <= indexed_lookup_max_keys
        and change_ratio <= indexed_lookup_max_ratio
        and shape.avg_document_bytes <= 262144
    ):
        return VerificationAccessPlan(
            strategy=VerificationFetchStrategy.INDEXED_LOOKUP,
            batch_size=1,
            estimated_round_trips=effective_keys,
            estimated_io_bytes=effective_keys * avg_bytes,
            change_ratio=change_ratio,
            used_index=index_name,
            rationale=(
                f"Selective point verification ({effective_keys} keys, {change_ratio:.2%} of {total_docs} docs, "
                f"avg_bytes={shape.avg_document_bytes}) with index '{index_name}'; selected INDEXED_LOOKUP."
            ),
        )

    # Otherwise: moderate working set with available index -> BATCHED_FETCH
    adaptive_batch = max(16, min(1000, target_batch_bytes // avg_bytes))
    adaptive_batch = min(adaptive_batch, effective_keys)
    round_trips = max(1, math.ceil(effective_keys / adaptive_batch))
    return VerificationAccessPlan(
        strategy=VerificationFetchStrategy.BATCHED_FETCH,
        batch_size=adaptive_batch,
        estimated_round_trips=round_trips,
        estimated_io_bytes=effective_keys * avg_bytes,
        change_ratio=change_ratio,
        used_index=index_name,
        rationale=(
            f"Moderate changed working set ({effective_keys} keys, {change_ratio:.2%} of {total_docs} docs, "
            f"avg_bytes={shape.avg_document_bytes}) with index '{index_name}'; "
            f"selected BATCHED_FETCH (batch_size={adaptive_batch}, round_trips={round_trips})."
        ),
    )


# ---------------------------------------------------------------------------
# Explicit Concurrent-Source Assumptions
# ---------------------------------------------------------------------------


@dataclass
class ConcurrentSourceAssumptions:
    mode: ConcurrencyMode = ConcurrencyMode.QUIESCED_SOURCE
    source_watermark_lsn: Optional[int] = None
    target_applied_lsn: Optional[int] = None
    in_flight_root_keys: Set[str] = field(default_factory=set)
    max_allowed_lag_ms: float = 0.0
    observed_lag_ms: float = 0.0
    allow_in_flight_window_exclusion: bool = False
    explicit_notes: List[str] = field(default_factory=list)

    def evaluate(self) -> Tuple[CareBadge, List[str]]:
        """Evaluates concurrency invariants and returns (badge, findings)."""
        findings: List[str] = []
        badge = CareBadge.PASS

        if self.mode == ConcurrencyMode.QUIESCED_SOURCE:
            findings.append("Source assumed quiesced (read-only / zero concurrent DML during check).")
            if self.in_flight_root_keys:
                badge = CareBadge.ACTION
                findings.append(
                    f"VIOLATION: QUIESCED_SOURCE declared but {len(self.in_flight_root_keys)} in-flight root keys detected: "
                    f"{sorted(self.in_flight_root_keys)}"
                )
            if self.observed_lag_ms > 0.0:
                badge = CareBadge.ACTION
                findings.append(
                    f"VIOLATION: QUIESCED_SOURCE declared but observed replication lag is {self.observed_lag_ms:.1f}ms > 0ms."
                )

        elif self.mode == ConcurrencyMode.MVCC_CONSISTENT_CUT:
            findings.append(
                f"MVCC point-in-time snapshot cut at source_lsn={self.source_watermark_lsn}, "
                f"target_applied_lsn={self.target_applied_lsn}."
            )
            if self.source_watermark_lsn is None or self.target_applied_lsn is None:
                badge = CareBadge.ACTION
                findings.append("VIOLATION: MVCC_CONSISTENT_CUT requires both source_watermark_lsn and target_applied_lsn.")
            elif self.target_applied_lsn != self.source_watermark_lsn:
                badge = CareBadge.ACTION
                findings.append(
                    f"VIOLATION: Target applied LSN ({self.target_applied_lsn}) does not match MVCC snapshot cut "
                    f"({self.source_watermark_lsn})."
                )

        elif self.mode == ConcurrencyMode.WATERMARK_ALIGNED_CDC:
            findings.append(
                f"Watermark-aligned CDC verification: source_watermark_lsn={self.source_watermark_lsn}, "
                f"target_applied_lsn={self.target_applied_lsn}, observed_lag_ms={self.observed_lag_ms:.1f}ms "
                f"(max={self.max_allowed_lag_ms:.1f}ms)."
            )
            if self.source_watermark_lsn is None or self.target_applied_lsn is None:
                badge = CareBadge.ACTION
                findings.append("VIOLATION: WATERMARK_ALIGNED_CDC requires explicit source and target LSN watermarks.")
            elif self.target_applied_lsn < self.source_watermark_lsn:
                badge = CareBadge.ACTION
                findings.append(
                    f"VIOLATION: Target applied LSN ({self.target_applied_lsn}) lags behind source verification watermark "
                    f"({self.source_watermark_lsn})."
                )
            if self.observed_lag_ms > self.max_allowed_lag_ms:
                if badge != CareBadge.ACTION:
                    badge = CareBadge.REVIEW
                findings.append(
                    f"LAG WARNING: Observed replication lag {self.observed_lag_ms:.1f}ms exceeds allowed threshold "
                    f"{self.max_allowed_lag_ms:.1f}ms."
                )

        elif self.mode == ConcurrencyMode.LIVE_CONCURRENT_WINDOW:
            findings.append(
                f"Live concurrent source with {len(self.in_flight_root_keys)} tracked in-flight root keys; "
                f"allow_in_flight_window_exclusion={self.allow_in_flight_window_exclusion}, "
                f"observed_lag_ms={self.observed_lag_ms:.1f}ms (max={self.max_allowed_lag_ms:.1f}ms)."
            )
            if self.observed_lag_ms > self.max_allowed_lag_ms:
                badge = CareBadge.ACTION
                findings.append(
                    f"VIOLATION: Live source replication lag {self.observed_lag_ms:.1f}ms exceeds max_allowed_lag_ms "
                    f"{self.max_allowed_lag_ms:.1f}ms."
                )
            elif self.in_flight_root_keys:
                badge = CareBadge.REVIEW
                findings.append(
                    f"Concurrent writes active on keys {sorted(self.in_flight_root_keys)}; "
                    "mismatches on these keys require bounded re-verification after watermark advance."
                )

        for note in self.explicit_notes:
            findings.append(f"ASSUMPTION: {note}")

        return badge, findings


# ---------------------------------------------------------------------------
# O(1) Typed Durable Checkpoint & Active Lease Lock (.run.lock)
# ---------------------------------------------------------------------------


def _encode_typed_value(val: Any) -> Dict[str, Any]:
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "value": str(val)}
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "value": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "value": val.isoformat()}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "value": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "value": [_encode_typed_value(x) for x in val]}
    return {"__type__": "primitive", "value": val}


def _decode_typed_value(payload: Any) -> Any:
    if not isinstance(payload, dict) or "__type__" not in payload:
        return payload
    t = payload["__type__"]
    v = payload["value"]
    if t == "decimal":
        return decimal.Decimal(v)
    if t == "datetime":
        return datetime.datetime.fromisoformat(v)
    if t == "date":
        return datetime.date.fromisoformat(v)
    if t == "uuid":
        return uuid.UUID(v)
    if t == "tuple":
        return tuple(_decode_typed_value(x) for x in v)
    return v


@dataclass
class DocumentVerifyCheckpoint:
    """Universal typed durable checkpoint supporting O(1) skip of verified root keys and propagation effects."""

    mapping_checksum: str
    completed_root_keys: Set[str] = field(default_factory=set)
    completed_effect_ids: Set[str] = field(default_factory=set)
    last_cursor_key: Any = None
    updated_at: float = field(default_factory=time.time)

    def is_root_key_completed(self, root_key: str) -> bool:
        return root_key in self.completed_root_keys

    def is_effect_completed(self, effect_id: str) -> bool:
        return effect_id in self.completed_effect_ids

    def mark_root_key_completed(self, root_key: str, cursor_key: Any = None) -> None:
        self.completed_root_keys.add(root_key)
        if cursor_key is not None:
            self.last_cursor_key = cursor_key
        self.updated_at = time.time()

    def mark_effect_completed(self, effect_id: str) -> None:
        self.completed_effect_ids.add(effect_id)
        self.updated_at = time.time()

    def save(self, path: str) -> None:
        data = {
            "mapping_checksum": self.mapping_checksum,
            "completed_root_keys": sorted(self.completed_root_keys),
            "completed_effect_ids": sorted(self.completed_effect_ids),
            "last_cursor_key": _encode_typed_value(self.last_cursor_key),
            "updated_at": self.updated_at,
        }
        tmp_path = f"{path}.tmp.{os.getpid()}"
        with open(tmp_path, "w", encoding="ascii") as f:
            json.dump(data, f, ensure_ascii=True, indent=2)
        os.replace(tmp_path, path)

    @classmethod
    def load(cls, path: str, expected_checksum: Optional[str] = None) -> DocumentVerifyCheckpoint:
        with open(path, "r", encoding="ascii") as f:
            data = json.load(f)
        ck = cls(
            mapping_checksum=data["mapping_checksum"],
            completed_root_keys=set(data.get("completed_root_keys", [])),
            completed_effect_ids=set(data.get("completed_effect_ids", [])),
            last_cursor_key=_decode_typed_value(data.get("last_cursor_key")),
            updated_at=float(data.get("updated_at", 0.0)),
        )
        if expected_checksum is not None and ck.mapping_checksum != expected_checksum:
            raise DocumentVerificationError(
                f"Checkpoint mapping checksum mismatch: expected {expected_checksum}, got {ck.mapping_checksum}"
            )
        return ck


class VerificationLeaseLock:
    """Mutual exclusion active lease lock (.run.lock) preventing overlapping verification runs."""

    def __init__(self, lock_dir: str, owner_id: str = "doc-verifier", ttl_seconds: float = 60.0):
        self.lock_path = os.path.join(lock_dir, ".run.lock")
        self.owner_id = owner_id
        self.ttl_seconds = ttl_seconds
        self._acquired = False

    def acquire(self) -> None:
        now = time.time()
        if os.path.exists(self.lock_path):
            try:
                with open(self.lock_path, "r", encoding="ascii") as f:
                    existing = json.load(f)
                heartbeat = float(existing.get("heartbeat_ts", 0.0))
                existing_owner = existing.get("owner_id", "")
                if now - heartbeat < self.ttl_seconds and existing_owner != self.owner_id:
                    raise ActiveVerificationLeaseError(
                        f"Active verification lease held by '{existing_owner}' at {self.lock_path}"
                    )
            except (ValueError, OSError):
                pass
        payload = {
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "acquired_ts": now,
            "heartbeat_ts": now,
            "ttl_seconds": self.ttl_seconds,
        }
        with open(self.lock_path, "w", encoding="ascii") as f:
            json.dump(payload, f, ensure_ascii=True)
        self._acquired = True

    def heartbeat(self) -> None:
        if not self._acquired:
            raise DocumentVerificationError("Cannot heartbeat an unacquired lease lock.")
        now = time.time()
        with open(self.lock_path, "r", encoding="ascii") as f:
            payload = json.load(f)
        payload["heartbeat_ts"] = now
        with open(self.lock_path, "w", encoding="ascii") as f:
            json.dump(payload, f, ensure_ascii=True)

    def release(self) -> None:
        if self._acquired and os.path.exists(self.lock_path):
            try:
                os.remove(self.lock_path)
            except OSError:
                pass
        self._acquired = False

    def __enter__(self) -> VerificationLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# ---------------------------------------------------------------------------
# Relational Mutations & Expected Propagation Effects
# ---------------------------------------------------------------------------


@dataclass
class RelationalMutationEvent:
    seq: int
    entity: str
    op: ChangeOp
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    lsn: int = 0
    tx_id: str = "tx-0"


@dataclass
class ExpectedPropagationEffect:
    effect_id: str
    source_seq: int
    source_entity: str
    source_op: ChangeOp
    source_pk: str
    kind: PropagationKind
    affected_root_keys: List[str]
    target_path: str
    expected_before_summary: Any = None
    expected_after_summary: Any = None
    delete_policy: Optional[DeletePropagationPolicy] = None


# ---------------------------------------------------------------------------
# Normalized-to-Document Projector & Mutation Propagation Engine
# ---------------------------------------------------------------------------


class NormalizedToDocumentProjector:
    """Projects relational tables into target documents and computes expected update/delete propagation effects."""

    def __init__(self, spec: DocumentMappingSpec, entity_pks: Optional[Dict[str, str]] = None):
        self.spec = spec
        self.entity_pks: Dict[str, str] = dict(entity_pks or {})
        self.entity_pks.setdefault(spec.root_entity, spec.root_source_pk)
        for rel in spec.embedded_relations:
            self.entity_pks.setdefault(rel.child_entity, rel.child_identity_key)

    def project_snapshot(self, tables: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Projects a complete set of normalized tables into expected target documents."""
        root_rows = tables.get(self.spec.root_entity, [])
        documents: List[Dict[str, Any]] = []

        # Check if any CASCADE_DELETE_DOCUMENT rule removes a root row when a required N:1 reference is missing
        cascade_ref_rules = [
            r for r in self.spec.embedded_relations
            if r.multiplicity == RelationMultiplicity.MANY_TO_ONE
            and r.parent_entity == self.spec.root_entity
            and r.delete_policy == DeletePropagationPolicy.CASCADE_DELETE_DOCUMENT
        ]

        for r_row in root_rows:
            if self.spec.filter_predicate is not None and not self.spec.filter_predicate(r_row):
                continue

            # Check cascade delete on missing required N:1 reference
            cascade_dropped = False
            for c_rule in cascade_ref_rules:
                fk_val = r_row.get(c_rule.parent_key)
                ref_rows = tables.get(c_rule.child_entity, [])
                matched_ref = any(str(ch.get(c_rule.child_foreign_key)) == str(fk_val) for ch in ref_rows)
                if not matched_ref:
                    cascade_dropped = True
                    break
            if cascade_dropped:
                continue

            doc: Dict[str, Any] = {}
            for fm in self.spec.root_field_mappings:
                raw_val = r_row.get(fm.source_column, _ABSENT)
                conv_val = fm.conversion.convert(raw_val)
                if conv_val is not _ABSENT:
                    set_nested_path(doc, fm.target_path, conv_val)

            # First pass: direct relations on root_entity
            root_pk_val = r_row.get(self.spec.root_source_pk)
            self._embed_relations_for_parent(
                parent_entity=self.spec.root_entity,
                parent_source_row=r_row,
                target_container=doc,
                tables=tables,
            )
            documents.append(doc)

        # Deterministic sort by target root key
        documents.sort(key=lambda d: str(get_nested_path(d, self.spec.target_root_key_path, "")))
        return documents

    def _embed_relations_for_parent(
        self,
        parent_entity: str,
        parent_source_row: Dict[str, Any],
        target_container: Dict[str, Any],
        tables: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        for rel in self.spec.embedded_relations:
            if rel.parent_entity != parent_entity:
                continue
            child_rows = tables.get(rel.child_entity, [])

            if rel.multiplicity == RelationMultiplicity.ONE_TO_MANY:
                p_val = parent_source_row.get(rel.parent_key)
                matched = [c for c in child_rows if p_val is not None and str(c.get(rel.child_foreign_key)) == str(p_val)]
                matched.sort(key=rel.sort_key)
                projected_items: List[Dict[str, Any]] = []
                for c_row in matched:
                    item_doc = rel.project_child_row(c_row)
                    # Nested second-level relations (e.g., child of child)
                    self._embed_relations_for_parent(
                        parent_entity=rel.child_entity,
                        parent_source_row=c_row,
                        target_container=item_doc,
                        tables=tables,
                    )
                    projected_items.append(item_doc)
                set_nested_path(target_container, rel.target_path, projected_items)

            elif rel.multiplicity == RelationMultiplicity.ONE_TO_ONE:
                p_val = parent_source_row.get(rel.parent_key)
                matched = [c for c in child_rows if p_val is not None and str(c.get(rel.child_foreign_key)) == str(p_val)]
                if matched:
                    sub_doc = rel.project_child_row(matched[0])
                    self._embed_relations_for_parent(
                        parent_entity=rel.child_entity,
                        parent_source_row=matched[0],
                        target_container=sub_doc,
                        tables=tables,
                    )
                    set_nested_path(target_container, rel.target_path, sub_doc)
                elif rel.delete_policy == DeletePropagationPolicy.NULLIFY_REFERENCE:
                    set_nested_path(target_container, rel.target_path, None)

            elif rel.multiplicity == RelationMultiplicity.MANY_TO_ONE:
                # parent_source_row has foreign key pointing to child_entity's primary key
                fk_val = parent_source_row.get(rel.parent_key)
                matched = [c for c in child_rows if fk_val is not None and str(c.get(rel.child_foreign_key)) == str(fk_val)]
                if matched:
                    sub_doc = rel.project_child_row(matched[0])
                    self._embed_relations_for_parent(
                        parent_entity=rel.child_entity,
                        parent_source_row=matched[0],
                        target_container=sub_doc,
                        tables=tables,
                    )
                    set_nested_path(target_container, rel.target_path, sub_doc)
                elif rel.delete_policy == DeletePropagationPolicy.NULLIFY_REFERENCE:
                    set_nested_path(target_container, rel.target_path, None)

            elif rel.multiplicity == RelationMultiplicity.MANY_TO_MANY and rel.join_entity:
                p_val = parent_source_row.get(rel.parent_key)
                join_rows = tables.get(rel.join_entity, [])
                matching_child_ids = {
                    str(jr.get(rel.join_child_fk))
                    for jr in join_rows
                    if p_val is not None and str(jr.get(rel.join_parent_fk)) == str(p_val)
                }
                matched = [c for c in child_rows if str(c.get(rel.child_foreign_key)) in matching_child_ids]
                matched.sort(key=rel.sort_key)
                projected_items = [rel.project_child_row(c_row) for c_row in matched]
                set_nested_path(target_container, rel.target_path, projected_items)

    def _root_target_key_for_source_pk(self, root_row: Dict[str, Any]) -> str:
        for fm in self.spec.root_field_mappings:
            if fm. target_path == self.spec.target_root_key_path:
                return str(fm.conversion.convert(root_row.get(fm.source_column)))
        return str(root_row.get(self.spec.root_source_pk, ""))

    def apply_mutations(
        self,
        initial_tables: Dict[str, List[Dict[str, Any]]],
        mutations: Sequence[RelationalMutationEvent],
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], List[ExpectedPropagationEffect]]:
        """Applies relational mutations to normalized tables and records expected document propagation effects."""
        state: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for ent, rows in initial_tables.items():
            pk_col = self.entity_pks.get(ent, "id")
            state[ent] = {str(r[pk_col]): copy.deepcopy(r) for r in rows if pk_col in r}

        # Keep historical snapshots for rules with RETAIN_HISTORICAL_SNAPSHOT
        retained_snapshots: Dict[Tuple[str, str], Dict[str, Any]] = {}
        effects: List[ExpectedPropagationEffect] = []

        for ev in mutations:
            ent = ev.entity
            pk_col = self.entity_pks.get(ent, "id")
            ent_map = state.setdefault(ent, {})

            if ent == self.spec.root_entity:
                if ev.op == ChangeOp.INSERT and ev.after:
                    pk_str = str(ev.after[pk_col])
                    ent_map[pk_str] = copy.deepcopy(ev.after)
                    t_key = self._root_target_key_for_source_pk(ev.after)
                    effects.append(
                        ExpectedPropagationEffect(
                            effect_id=f"eff-{ev.seq}-root-ins-{t_key}",
                            source_seq=ev.seq,
                            source_entity=ent,
                            source_op=ev.op,
                            source_pk=pk_str,
                            kind=PropagationKind.ROOT_INSERT,
                            affected_root_keys=[t_key],
                            target_path=self.spec.target_root_key_path,
                            expected_after_summary=t_key,
                        )
                    )
                elif ev.op == ChangeOp.UPDATE and ev.after:
                    pk_str = str(ev.after[pk_col])
                    before_row = ent_map.get(pk_str, ev.before or {})
                    ent_map[pk_str] = copy.deepcopy(ev.after)
                    t_key = self._root_target_key_for_source_pk(ev.after)
                    effects.append(
                        ExpectedPropagationEffect(
                            effect_id=f"eff-{ev.seq}-root-upd-{t_key}",
                            source_seq=ev.seq,
                            source_entity=ent,
                            source_op=ev.op,
                            source_pk=pk_str,
                            kind=PropagationKind.ROOT_UPDATE,
                            affected_root_keys=[t_key],
                            target_path=self.spec.target_root_key_path,
                            expected_before_summary=before_row,
                            expected_after_summary=copy.deepcopy(ev.after),
                        )
                    )
                elif ev.op == ChangeOp.DELETE:
                    ref_row = ev.before or {}
                    pk_str = str(ref_row.get(pk_col, ""))
                    removed = ent_map.pop(pk_str, ref_row)
                    t_key = self._root_target_key_for_source_pk(removed) if removed else pk_str
                    effects.append(
                        ExpectedPropagationEffect(
                            effect_id=f"eff-{ev.seq}-root-del-{t_key}",
                            source_seq=ev.seq,
                            source_entity=ent,
                            source_op=ev.op,
                            source_pk=pk_str,
                            kind=PropagationKind.ROOT_DELETE,
                            affected_root_keys=[t_key],
                            target_path=self.spec.target_root_key_path,
                            expected_before_summary=removed,
                            expected_after_summary=None,
                        )
                    )
                continue

            # Mutation is on a child or referenced entity
            matching_rules = [r for r in self.spec.embedded_relations if r.child_entity == ent]
            for rel in matching_rules:
                if rel.multiplicity in (RelationMultiplicity.ONE_TO_MANY, RelationMultiplicity.ONE_TO_ONE):
                    self._record_child_mutation_effect(ev, rel, state, effects)
                elif rel.multiplicity == RelationMultiplicity.MANY_TO_ONE:
                    self._record_reference_mutation_effect(ev, rel, state, retained_snapshots, effects)

            # Now update the state table for `ent`
            if ev.op in (ChangeOp.INSERT, ChangeOp.UPDATE) and ev.after:
                c_pk = str(ev.after[pk_col])
                ent_map[c_pk] = copy.deepcopy(ev.after)
            elif ev.op == ChangeOp.DELETE and ev.before:
                c_pk = str(ev.before[pk_col])
                ent_map.pop(c_pk, None)

        updated_tables = {ent: list(rmap.values()) for ent, rmap in state.items()}
        expected_docs = self.project_snapshot(updated_tables)

        # Apply any RETAIN_HISTORICAL_SNAPSHOT overrides where reference was deleted
        if retained_snapshots:
            for doc in expected_docs:
                rk = str(get_nested_path(doc, self.spec.target_root_key_path, ""))
                for (snap_rk, snap_path), snap_val in retained_snapshots.items():
                    if snap_rk == rk and get_nested_path(doc, snap_path, _ABSENT) is _ABSENT:
                        set_nested_path(doc, snap_path, copy.deepcopy(snap_val))

        return updated_tables, expected_docs, effects

    def _find_root_keys_for_parent_row(
        self,
        parent_entity: str,
        parent_fk_value: Any,
        state: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> List[str]:
        if parent_fk_value is None:
            return []
        if parent_entity == self.spec.root_entity:
            root_row = state.get(self.spec.root_entity, {}).get(str(parent_fk_value))
            if root_row:
                return [self._root_target_key_for_source_pk(root_row)]
            return [str(parent_fk_value)]
        # Second-level parent: trace up to root_entity
        parent_rel = next((r for r in self.spec.embedded_relations if r.child_entity == parent_entity), None)
        if parent_rel:
            p_row = state.get(parent_entity, {}).get(str(parent_fk_value))
            if p_row:
                root_fk = p_row.get(parent_rel.child_foreign_key)
                return self._find_root_keys_for_parent_row(parent_rel.parent_entity, root_fk, state)
        return []

    def _record_child_mutation_effect(
        self,
        ev: RelationalMutationEvent,
        rel: EmbeddedRelationRule,
        state: Dict[str, Dict[str, Dict[str, Any]]],
        effects: List[ExpectedPropagationEffect],
    ) -> None:
        pk_col = rel.child_identity_key
        fk_col = rel.child_foreign_key
        full_path = f"{rel.parent_array_path}.{rel.target_path}" if rel.parent_array_path else rel.target_path

        if ev.op == ChangeOp.INSERT and ev.after:
            c_pk = str(ev.after.get(pk_col))
            parent_fk = ev.after.get(fk_col)
            root_keys = self._find_root_keys_for_parent_row(rel.parent_entity, parent_fk, state)
            effects.append(
                ExpectedPropagationEffect(
                    effect_id=f"eff-{ev.seq}-{rel.rule_id}-ins-{c_pk}",
                    source_seq=ev.seq,
                    source_entity=rel.child_entity,
                    source_op=ev.op,
                    source_pk=c_pk,
                    kind=PropagationKind.EMBEDDED_CHILD_INSERT,
                    affected_root_keys=root_keys,
                    target_path=full_path,
                    expected_before_summary=None,
                    expected_after_summary=rel.project_child_row(ev.after),
                )
            )
        elif ev.op == ChangeOp.UPDATE and ev.after:
            c_pk = str(ev.after.get(pk_col))
            old_row = ev.before or state.get(rel.child_entity, {}).get(c_pk, {})
            old_fk = old_row.get(fk_col)
            new_fk = ev.after.get(fk_col)
            old_roots = self._find_root_keys_for_parent_row(rel.parent_entity, old_fk, state)
            new_roots = self._find_root_keys_for_parent_row(rel.parent_entity, new_fk, state)

            if old_fk is not None and new_fk is not None and str(old_fk) != str(new_fk):
                # Child reparented across parent documents!
                combined_roots = list(dict.fromkeys(old_roots + new_roots))
                effects.append(
                    ExpectedPropagationEffect(
                        effect_id=f"eff-{ev.seq}-{rel.rule_id}-reparent-{c_pk}",
                        source_seq=ev.seq,
                        source_entity=rel.child_entity,
                        source_op=ev.op,
                        source_pk=c_pk,
                        kind=PropagationKind.EMBEDDED_CHILD_REPARENT,
                        affected_root_keys=combined_roots,
                        target_path=full_path,
                        expected_before_summary={"old_root_keys": old_roots, "child": rel.project_child_row(old_row)},
                        expected_after_summary={"new_root_keys": new_roots, "child": rel.project_child_row(ev.after)},
                    )
                )
            else:
                effects.append(
                    ExpectedPropagationEffect(
                        effect_id=f"eff-{ev.seq}-{rel.rule_id}-upd-{c_pk}",
                        source_seq=ev.seq,
                        source_entity=rel.child_entity,
                        source_op=ev.op,
                        source_pk=c_pk,
                        kind=PropagationKind.EMBEDDED_CHILD_UPDATE,
                        affected_root_keys=new_roots or old_roots,
                        target_path=full_path,
                        expected_before_summary=rel.project_child_row(old_row) if old_row else None,
                        expected_after_summary=rel.project_child_row(ev.after),
                    )
                )
        elif ev.op == ChangeOp.DELETE:
            old_row = ev.before or {}
            c_pk = str(old_row.get(pk_col, ""))
            if not old_row and c_pk:
                old_row = state.get(rel.child_entity, {}).get(c_pk, {})
            old_fk = old_row.get(fk_col)
            old_roots = self._find_root_keys_for_parent_row(rel.parent_entity, old_fk, state)
            effects.append(
                ExpectedPropagationEffect(
                    effect_id=f"eff-{ev.seq}-{rel.rule_id}-del-{c_pk}",
                    source_seq=ev.seq,
                    source_entity=rel.child_entity,
                    source_op=ev.op,
                    source_pk=c_pk,
                    kind=PropagationKind.EMBEDDED_CHILD_DELETE,
                    affected_root_keys=old_roots,
                    target_path=full_path,
                    expected_before_summary=rel.project_child_row(old_row) if old_row else None,
                    expected_after_summary=None,
                    delete_policy=rel.delete_policy,
                )
            )

    def _record_reference_mutation_effect(
        self,
        ev: RelationalMutationEvent,
        rel: EmbeddedRelationRule,
        state: Dict[str, Dict[str, Dict[str, Any]]],
        retained_snapshots: Dict[Tuple[str, str], Dict[str, Any]],
        effects: List[ExpectedPropagationEffect],
    ) -> None:
        ref_pk_col = rel.child_foreign_key
        row_ref = ev.after or ev.before or {}
        ref_pk_val = row_ref.get(ref_pk_col)
        if ref_pk_val is None:
            return
        ref_pk_str = str(ref_pk_val)

        # Find all parent rows that reference this entity
        parent_rows = state.get(rel.parent_entity, {}).values()
        referencing_parents = [p for p in parent_rows if str(p.get(rel.parent_key)) == ref_pk_str]

        affected_root_keys: List[str] = []
        parent_pk_col = self.entity_pks.get(rel.parent_entity, "id")
        for p in referencing_parents:
            rks = self._find_root_keys_for_parent_row(rel.parent_entity, p.get(parent_pk_col), state)
            for rk in rks:
                if rk not in affected_root_keys:
                    affected_root_keys.append(rk)
        affected_root_keys.sort()

        full_path = f"{rel.parent_array_path}.{rel.target_path}" if rel.parent_array_path else rel.target_path
        old_ref_row = ev.before or state.get(rel.child_entity, {}).get(ref_pk_str, {})

        if ev.op in (ChangeOp.UPDATE, ChangeOp.INSERT) and ev.after:
            effects.append(
                ExpectedPropagationEffect(
                    effect_id=f"eff-{ev.seq}-{rel.rule_id}-ref-upd-{ref_pk_str}",
                    source_seq=ev.seq,
                    source_entity=rel.child_entity,
                    source_op=ev.op,
                    source_pk=ref_pk_str,
                    kind=PropagationKind.EMBEDDED_REF_FANOUT_UPDATE,
                    affected_root_keys=affected_root_keys,
                    target_path=full_path,
                    expected_before_summary=rel.project_child_row(old_ref_row) if old_ref_row else None,
                    expected_after_summary=rel.project_child_row(ev.after),
                )
            )
        elif ev.op == ChangeOp.DELETE:
            proj_before = rel.project_child_row(old_ref_row) if old_ref_row else None
            if rel.delete_policy == DeletePropagationPolicy.RETAIN_HISTORICAL_SNAPSHOT and proj_before is not None:
                for rk in affected_root_keys:
                    retained_snapshots[(rk, full_path)] = proj_before
            effects.append(
                ExpectedPropagationEffect(
                    effect_id=f"eff-{ev.seq}-{rel.rule_id}-ref-del-{ref_pk_str}",
                    source_seq=ev.seq,
                    source_entity=rel.child_entity,
                    source_op=ev.op,
                    source_pk=ref_pk_str,
                    kind=PropagationKind.EMBEDDED_REF_DELETE,
                    affected_root_keys=affected_root_keys,
                    target_path=full_path,
                    expected_before_summary=proj_before,
                    expected_after_summary=(
                        proj_before
                        if rel.delete_policy == DeletePropagationPolicy.RETAIN_HISTORICAL_SNAPSHOT
                        else None
                    ),
                    delete_policy=rel.delete_policy,
                )
            )


# ---------------------------------------------------------------------------
# Verification Findings & Result Models
# ---------------------------------------------------------------------------


@dataclass
class DocumentVerificationDefect:
    badge: CareBadge
    category: DefectCategory
    root_key: str
    path: str
    expected_value: Any
    actual_value: Any
    reason: str
    source_effect_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "badge": self.badge.value,
            "category": self.category.value,
            "root_key": self.root_key,
            "path": self.path,
            "expected_value": str(self.expected_value),
            "actual_value": str(self.actual_value),
            "reason": self.reason,
            "source_effect_id": self.source_effect_id,
        }


@dataclass
class SnapshotVerificationResult:
    badge: CareBadge
    mapping_id: str
    mapping_checksum: str
    expected_collection_count: int
    actual_collection_count: int
    collection_count_matches: bool
    verified_documents_count: int
    skipped_via_checkpoint_count: int
    access_plan: VerificationAccessPlan
    concurrency_badge: CareBadge
    concurrency_findings: List[str]
    defects: List[DocumentVerificationDefect]

    def is_clean(self) -> bool:
        return self.badge in (CareBadge.PASS, CareBadge.BENIGN)

    def format_unix_report(self) -> str:
        lines = [
            f"DOCUMENT SNAPSHOT VERIFICATION: [{self.badge.value}]",
            f"Mapping: {self.mapping_id} | Checksum: {self.mapping_checksum[:16]}",
            f"Strategy: {self.access_plan.strategy.value} (batch_size={self.access_plan.batch_size}, "
            f"index={self.access_plan.used_index}, change_ratio={self.access_plan.change_ratio:.2%})",
            f"Strategy Rationale: {self.access_plan.rationale}",
            f"Collection Counts: expected={self.expected_collection_count} | actual={self.actual_collection_count} | "
            f"equal_counts={'YES' if self.collection_count_matches else 'NO'} | "
            f"verified={self.verified_documents_count} | checkpoint_skipped={self.skipped_via_checkpoint_count}",
            f"Concurrency Contract: [{self.concurrency_badge.value}]",
        ]
        for cf in self.concurrency_findings:
            lines.append(f"  * {cf}")
        if self.defects:
            lines.append("Snapshot Defects:")
            for d in self.defects:
                lines.append(
                    f"  [{d.badge.value}] [{d.category.value}] key={d.root_key} path={d.path}: {d.reason} "
                    f"(expected={d.expected_value!r}, actual={d.actual_value!r})"
                )
        else:
            lines.append("Zero snapshot defects detected.")
        return "\n".join(lines)


@dataclass
class PropagationVerificationResult:
    badge: CareBadge
    mapping_id: str
    mapping_checksum: str
    total_mutations_evaluated: int
    total_effects_expected: int
    verified_effects_count: int
    skipped_effects_count: int
    access_plan: VerificationAccessPlan
    concurrency_badge: CareBadge
    concurrency_findings: List[str]
    propagation_defects: List[DocumentVerificationDefect]
    post_mutation_snapshot_result: SnapshotVerificationResult

    def is_clean(self) -> bool:
        return self.badge in (CareBadge.PASS, CareBadge.BENIGN)

    def format_unix_report(self) -> str:
        lines = [
            f"EMBEDDED UPDATE/DELETE PROPAGATION VERIFICATION: [{self.badge.value}]",
            f"Mapping: {self.mapping_id} | Checksum: {self.mapping_checksum[:16]}",
            f"Mutations Evaluated: {self.total_mutations_evaluated} | Expected Effects: {self.total_effects_expected} | "
            f"Verified: {self.verified_effects_count} | Checkpoint Skipped: {self.skipped_effects_count}",
            f"Access Strategy: {self.access_plan.strategy.value} (batch_size={self.access_plan.batch_size}, "
            f"round_trips={self.access_plan.estimated_round_trips})",
            f"Concurrency Contract: [{self.concurrency_badge.value}]",
        ]
        for cf in self.concurrency_findings:
            lines.append(f"  * {cf}")
        if self.propagation_defects:
            lines.append("Propagation Defects:")
            for d in self.propagation_defects:
                eff_tag = f" effect={d.source_effect_id}" if d.source_effect_id else ""
                lines.append(
                    f"  [{d.badge.value}] [{d.category.value}] key={d.root_key} path={d.path}{eff_tag}: {d.reason} "
                    f"(expected={d.expected_value!r}, actual={d.actual_value!r})"
                )
        else:
            lines.append("All embedded update/delete propagation effects verified cleanly.")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Document Model Transform Verifier
# ---------------------------------------------------------------------------


class DocumentTransformVerifier:
    """Verifies normalized-to-document transformations for snapshots and post-update propagation."""

    def __init__(
        self,
        spec: DocumentMappingSpec,
        entity_pks: Optional[Dict[str, str]] = None,
    ):
        self.spec = spec
        self.projector = NormalizedToDocumentProjector(spec, entity_pks=entity_pks)
        self._leaf_path_directory = spec.all_expected_leaf_paths()
        self._root_rules_by_path: Dict[str, FieldPathRule] = {
            fm.target_path: fm for fm in spec.root_field_mappings
        }
        self._relations_by_path: Dict[str, EmbeddedRelationRule] = {}
        for rel in spec.embedded_relations:
            full_p = f"{rel.parent_array_path}.{rel.target_path}" if rel.parent_array_path else rel.target_path
            self._relations_by_path[full_p] = rel

    def _extract_root_key(self, doc: Dict[str, Any]) -> str:
        val = get_nested_path(doc, self.spec.target_root_key_path, _ABSENT)
        return "" if val is _ABSENT else str(val)

    def verify_snapshot(
        self,
        source_tables: Dict[str, List[Dict[str, Any]]],
        actual_target_documents: Sequence[Dict[str, Any]],
        measured_shape: Optional[MeasuredTargetShape] = None,
        concurrency: Optional[ConcurrentSourceAssumptions] = None,
        checkpoint: Optional[DocumentVerifyCheckpoint] = None,
        changed_root_keys_subset: Optional[Set[str]] = None,
    ) -> SnapshotVerificationResult:
        """Verifies normalized source tables against target documents with path, type, ordering, and multiplicity rules."""
        expected_docs = self.projector.project_snapshot(source_tables)
        return self._verify_expected_vs_actual_docs(
            expected_docs=expected_docs,
            actual_target_documents=actual_target_documents,
            measured_shape=measured_shape,
            concurrency=concurrency,
            checkpoint=checkpoint,
            changed_root_keys_subset=changed_root_keys_subset,
        )

    def _verify_expected_vs_actual_docs(
        self,
        expected_docs: List[Dict[str, Any]],
        actual_target_documents: Sequence[Dict[str, Any]],
        measured_shape: Optional[MeasuredTargetShape] = None,
        concurrency: Optional[ConcurrentSourceAssumptions] = None,
        checkpoint: Optional[DocumentVerifyCheckpoint] = None,
        changed_root_keys_subset: Optional[Set[str]] = None,
    ) -> SnapshotVerificationResult:
        concurrency_contract = concurrency or ConcurrentSourceAssumptions()
        conc_badge, conc_findings = concurrency_contract.evaluate()

        changed_cnt = len(changed_root_keys_subset) if changed_root_keys_subset is not None else 0
        shape = measured_shape or MeasuredTargetShape.measure_from_documents(
            collection_name=self.spec.target_collection,
            documents=actual_target_documents,
            changed_root_keys_count=changed_cnt,
            has_root_key_index=True,
        )
        access_plan = select_verification_strategy(shape)

        expected_collection_count = len(expected_docs)
        actual_collection_count = len(actual_target_documents)
        collection_count_matches = (expected_collection_count == actual_collection_count)

        defects: List[DocumentVerificationDefect] = []
        if conc_badge == CareBadge.ACTION:
            defects.append(
                DocumentVerificationDefect(
                    badge=CareBadge.ACTION,
                    category=DefectCategory.CONCURRENT_ASSUMPTION_VIOLATION,
                    root_key="__collection__",
                    path="__concurrency__",
                    expected_value=concurrency_contract.mode.value,
                    actual_value=conc_findings,
                    reason="; ".join(f for f in conc_findings if "VIOLATION" in f),
                )
            )

        expected_by_key: Dict[str, Dict[str, Any]] = {}
        for d in expected_docs:
            k = self._extract_root_key(d)
            expected_by_key[k] = d

        actual_by_key: Dict[str, Dict[str, Any]] = {}
        actual_key_counts: Dict[str, int] = {}
        for d in actual_target_documents:
            k = self._extract_root_key(d)
            actual_key_counts[k] = actual_key_counts.get(k, 0) + 1
            actual_by_key[k] = d

        # 1. Check duplicate root keys in target (root multiplicity violation)
        for k, cnt in sorted(actual_key_counts.items()):
            if cnt > 1:
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.ACTION,
                        category=DefectCategory.ROOT_MULTIPLICITY_MISMATCH,
                        root_key=k,
                        path=self.spec.target_root_key_path,
                        expected_value=1,
                        actual_value=cnt,
                        reason=f"Target collection has {cnt} duplicate documents for root key '{k}'",
                    )
                )

        # Determine keys to verify
        all_candidate_keys = sorted(set(expected_by_key.keys()) | set(actual_by_key.keys()))
        verified_count = 0
        skipped_count = 0

        for k in all_candidate_keys:
            if changed_root_keys_subset is not None and k not in changed_root_keys_subset:
                continue
            if checkpoint is not None and checkpoint.is_root_key_completed(k):
                skipped_count += 1
                continue

            in_flight_Downgrade = (
                concurrency_contract.mode == ConcurrencyMode.LIVE_CONCURRENT_WINDOW
                and concurrency_contract.allow_in_flight_window_exclusion
                and k in concurrency_contract.in_flight_root_keys
            )

            if k not in actual_by_key:
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.REVIEW if in_flight_Downgrade else CareBadge.ACTION,
                        category=(
                            DefectCategory.CONCURRENT_SOURCE_WINDOW_DRIFT
                            if in_flight_Downgrade
                            else DefectCategory.MISSING_ROOT_DOCUMENT
                        ),
                        root_key=k,
                        path=self.spec.target_root_key_path,
                        expected_value=expected_by_key[k],
                        actual_value=None,
                        reason=(
                            f"Root document '{k}' missing on target (in-flight concurrent key)"
                            if in_flight_Downgrade
                            else f"Expected root document '{k}' is missing from target collection"
                        ),
                    )
                )
                continue

            if k not in expected_by_key:
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.REVIEW if in_flight_Downgrade else CareBadge.ACTION,
                        category=(
                            DefectCategory.CONCURRENT_SOURCE_WINDOW_DRIFT
                            if in_flight_Downgrade
                            else DefectCategory.UNEXPECTED_ROOT_DOCUMENT
                        ),
                        root_key=k,
                        path=self.spec.target_root_key_path,
                        expected_value=None,
                        actual_value=actual_by_key[k],
                        reason=(
                            f"Unexpected root document '{k}' on target (in-flight concurrent key)"
                            if in_flight_Downgrade
                            else f"Target contains unexpected root document '{k}' not present in normalized source"
                        ),
                    )
                )
                continue

            doc_defects = self._compare_single_document(
                root_key=k,
                expected_doc=expected_by_key[k],
                actual_doc=actual_by_key[k],
            )
            if in_flight_Downgrade:
                for dd in doc_defects:
                    if dd.badge == CareBadge.ACTION:
                        dd.badge = CareBadge.REVIEW
                        dd.category = DefectCategory.CONCURRENT_SOURCE_WINDOW_DRIFT
                        dd.reason = f"[In-flight concurrent key {k}] {dd.reason}"
            defects.extend(doc_defects)
            verified_count += 1

            if checkpoint is not None and not any(d.badge == CareBadge.ACTION for d in doc_defects):
                checkpoint.mark_root_key_completed(k, cursor_key=k)

        overall_badge = self._aggregate_badge(defects, conc_badge)
        return SnapshotVerificationResult(
            badge=overall_badge,
            mapping_id=self.spec.mapping_id,
            mapping_checksum=self.spec.checksum(),
            expected_collection_count=expected_collection_count,
            actual_collection_count=actual_collection_count,
            collection_count_matches=collection_count_matches,
            verified_documents_count=verified_count,
            skipped_via_checkpoint_count=skipped_count,
            access_plan=access_plan,
            concurrency_badge=conc_badge,
            concurrency_findings=conc_findings,
            defects=defects,
        )

    def _compare_single_document(
        self,
        root_key: str,
        expected_doc: Dict[str, Any],
        actual_doc: Dict[str, Any],
    ) -> List[DocumentVerificationDefect]:
        defects: List[DocumentVerificationDefect] = []

        # Track actual leaf paths for misplaced path identity detection
        actual_flat = flatten_leaf_paths(actual_doc, stop_at_arrays=True)
        expected_flat = flatten_leaf_paths(expected_doc, stop_at_arrays=True)

        # 1. Verify root field mappings
        for fm in self.spec.root_field_mappings:
            exp_val = get_nested_path(expected_doc, fm.target_path, _ABSENT)
            act_val = get_nested_path(actual_doc, fm.target_path, _ABSENT)

            if exp_val is not _ABSENT and act_val is _ABSENT:
                # Check if value was placed at the wrong path in actual_doc!
                misplaced_at = self._find_misplaced_path(
                    fm.source_column, fm.target_path, exp_val, actual_flat, expected_flat
                )
                if misplaced_at:
                    defects.append(
                        DocumentVerificationDefect(
                            badge=CareBadge.ACTION,
                            category=DefectCategory.PATH_IDENTITY_MISMATCH,
                            root_key=root_key,
                            path=fm.target_path,
                            expected_value=f"{fm.target_path}={exp_val!r}",
                            actual_value=f"{misplaced_at}={actual_flat[misplaced_at]!r}",
                            reason=(
                                f"Field '{fm.source_column}' mapped to wrong nested path '{misplaced_at}' "
                                f"instead of required path '{fm.target_path}'"
                            ),
                        )
                    )
                else:
                    defects.append(
                        DocumentVerificationDefect(
                            badge=CareBadge.ACTION,
                            category=DefectCategory.PATH_IDENTITY_MISMATCH,
                            root_key=root_key,
                            path=fm.target_path,
                            expected_value=exp_val,
                            actual_value="<ABSENT>",
                            reason=f"Expected field at path '{fm.target_path}' is missing from target document",
                        )
                    )
                continue

            ok, badge_override, detail = fm.conversion.compare(exp_val, act_val)
            if not ok:
                cat = (
                    DefectCategory.TYPE_CONVERSION_MISMATCH
                    if "Type conversion mismatch" in detail
                    else DefectCategory.VALUE_MISMATCH
                )
                defects.append(
                    DocumentVerificationDefect(
                        badge=badge_override or CareBadge.ACTION,
                        category=cat,
                        root_key=root_key,
                        path=fm.target_path,
                        expected_value="<ABSENT>" if exp_val is _ABSENT else exp_val,
                        actual_value="<ABSENT>" if act_val is _ABSENT else act_val,
                        reason=detail,
                    )
                )
            elif badge_override == CareBadge.REVIEW:
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.REVIEW,
                        category=DefectCategory.TOLERATED_COERCION_DRIFT,
                        root_key=root_key,
                        path=fm.target_path,
                        expected_value=exp_val,
                        actual_value=act_val,
                        reason=detail,
                    )
                )

        # 2. Verify top-level embedded relations on root_entity
        for rel in self.spec.embedded_relations:
            if rel.parent_entity != self.spec.root_entity:
                continue
            defects.extend(
                self._compare_embedded_relation(
                    root_key=root_key,
                    rel=rel,
                    expected_parent=expected_doc,
                    actual_parent=actual_doc,
                    path_prefix="",
                    actual_root_flat=actual_flat,
                    expected_root_flat=expected_flat,
                )
            )

        # 3. Check for any undeclared extra root fields that weren't already reported as misplaced
        reported_misplaced_actuals = {
            str(d.actual_value).split("=", 1)[0]
            for d in defects
            if d.category == DefectCategory.PATH_IDENTITY_MISMATCH and "=" in str(d.actual_value)
        }
        for act_path, act_val in sorted(actual_flat.items()):
            if act_path in expected_flat or act_path in reported_misplaced_actuals:
                continue
            # Check if act_path is under an embedded relation path
            if any(
                act_path == rel.target_path or act_path.startswith(rel.target_path + ".")
                for rel in self.spec.embedded_relations
                if rel.parent_entity == self.spec.root_entity
            ):
                continue
            defects.append(
                DocumentVerificationDefect(
                    badge=CareBadge.ACTION,
                    category=DefectCategory.PATH_IDENTITY_MISMATCH,
                    root_key=root_key,
                    path=act_path,
                    expected_value="<ABSENT>",
                    actual_value=act_val,
                    reason=f"Target document contains unexpected or misplaced nested path '{act_path}'",
                )
            )

        return defects

    def _find_misplaced_path(
        self,
        source_col: str,
        expected_path: str,
        expected_val: Any,
        actual_flat: Dict[str, Any],
        expected_flat: Dict[str, Any],
    ) -> Optional[str]:
        leaf_key = expected_path.split(".")[-1]
        for act_p, act_v in actual_flat.items():
            if act_p in expected_flat:
                continue
            act_leaf = act_p.split(".")[-1]
            if act_leaf in (leaf_key, source_col) or act_v == expected_val:
                return act_p
        return None

    def _compare_embedded_relation(
        self,
        root_key: str,
        rel: EmbeddedRelationRule,
        expected_parent: Dict[str, Any],
        actual_parent: Dict[str, Any],
        path_prefix: str,
        actual_root_flat: Dict[str, Any],
        expected_root_flat: Dict[str, Any],
    ) -> List[DocumentVerificationDefect]:
        defects: List[DocumentVerificationDefect] = []
        full_rel_path = f"{path_prefix}.{rel.target_path}" if path_prefix else rel.target_path
        exp_sub = get_nested_path(expected_parent, rel.target_path, _ABSENT)
        act_sub = get_nested_path(actual_parent, rel.target_path, _ABSENT)

        if rel.multiplicity in (RelationMultiplicity.ONE_TO_ONE, RelationMultiplicity.MANY_TO_ONE):
            if exp_sub is _ABSENT and act_sub is _ABSENT:
                return defects
            if exp_sub is None and act_sub is None:
                return defects
            if exp_sub is _ABSENT and act_sub is not _ABSENT:
                if rel.delete_policy == DeletePropagationPolicy.RETAIN_HISTORICAL_SNAPSHOT:
                    defects.append(
                        DocumentVerificationDefect(
                            badge=CareBadge.BENIGN,
                            category=DefectCategory.HISTORICAL_SNAPSHOT_RETAINED,
                            root_key=root_key,
                            path=full_rel_path,
                            expected_value="<HISTORICAL_SNAPSHOT>",
                            actual_value=act_sub,
                            reason=f"Retained historical embedded subdocument at '{full_rel_path}' per policy",
                        )
                    )
                    return defects
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.ACTION,
                        category=DefectCategory.ORPHAN_EMBEDDED_CHILD,
                        root_key=root_key,
                        path=full_rel_path,
                        expected_value="<ABSENT>",
                        actual_value=act_sub,
                        reason=f"Embedded subdocument at '{full_rel_path}' should have been removed but remains present",
                    )
                )
                return defects
            if exp_sub is not _ABSENT and (act_sub is _ABSENT or act_sub is None):
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.ACTION,
                        category=DefectCategory.MISSING_EMBEDDED_CHILD,
                        root_key=root_key,
                        path=full_rel_path,
                        expected_value=exp_sub,
                        actual_value="<ABSENT>" if act_sub is _ABSENT else None,
                        reason=f"Missing required embedded subdocument at '{full_rel_path}'",
                    )
                )
                return defects
            if not isinstance(act_sub, dict):
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.ACTION,
                        category=DefectCategory.TYPE_CONVERSION_MISMATCH,
                        root_key=root_key,
                        path=full_rel_path,
                        expected_value=exp_sub,
                        actual_value=act_sub,
                        reason=f"Expected embedded object (dict) at '{full_rel_path}', got {type(act_sub).__name__}",
                    )
                )
                return defects

            defects.extend(
                self._compare_subdocument_fields(
                    root_key=root_key,
                    rel=rel,
                    exp_item=exp_sub,
                    act_item=act_sub,
                    item_path_prefix=full_rel_path,
                )
            )
            return defects

        # Array multiplicity (1:N or M:N)
        if exp_sub is _ABSENT:
            exp_sub = []
        if act_sub is _ABSENT or not isinstance(act_sub, list):
            defects.append(
                DocumentVerificationDefect(
                    badge=CareBadge.ACTION,
                    category=DefectCategory.MISSING_EMBEDDED_CHILD,
                    root_key=root_key,
                    path=full_rel_path,
                    expected_value=exp_sub,
                    actual_value="<ABSENT>" if act_sub is _ABSENT else act_sub,
                    reason=f"Expected embedded array at '{full_rel_path}', got {'<ABSENT>' if act_sub is _ABSENT else type(act_sub).__name__}",
                )
            )
            return defects

        id_field = rel.get_target_identity_field()
        exp_by_id: Dict[str, Tuple[int, Dict[str, Any]]] = {}
        for idx, item in enumerate(exp_sub):
            cid = str(get_nested_path(item, id_field, idx))
            exp_by_id[cid] = (idx, item)

        act_by_id: Dict[str, Tuple[int, Dict[str, Any]]] = {}
        act_id_counts: Dict[str, int] = {}
        for idx, item in enumerate(act_sub):
            if isinstance(item, dict):
                cid = str(get_nested_path(item, id_field, f"__idx_{idx}"))
            else:
                cid = str(item)
            act_id_counts[cid] = act_id_counts.get(cid, 0) + 1
            act_by_id[cid] = (idx, item)

        # Check duplicate child identities inside nested array
        for cid, cnt in sorted(act_id_counts.items()):
            if cnt > 1:
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.ACTION,
                        category=DefectCategory.NESTED_MULTIPLICITY_MISMATCH,
                        root_key=root_key,
                        path=f"{full_rel_path}[{id_field}={cid}]",
                        expected_value=1,
                        actual_value=cnt,
                        reason=f"Duplicate embedded child '{cid}' appears {cnt} times in '{full_rel_path}'",
                    )
                )

        # Check missing embedded children
        missing_cids = [cid for cid in exp_by_id if cid not in act_by_id]
        extra_cids = [cid for cid in act_by_id if cid not in exp_by_id]

        for cid in missing_cids:
            _, exp_item = exp_by_id[cid]
            defects.append(
                DocumentVerificationDefect(
                    badge=CareBadge.ACTION,
                    category=DefectCategory.MISSING_EMBEDDED_CHILD,
                    root_key=root_key,
                    path=f"{full_rel_path}[{id_field}={cid}]",
                    expected_value=exp_item,
                    actual_value="<MISSING>",
                    reason=f"Embedded child '{cid}' is missing from nested array '{full_rel_path}'",
                )
            )

        for cid in extra_cids:
            _, act_item = act_by_id[cid]
            defects.append(
                DocumentVerificationDefect(
                    badge=CareBadge.ACTION,
                    category=DefectCategory.ORPHAN_EMBEDDED_CHILD,
                    root_key=root_key,
                    path=f"{full_rel_path}[{id_field}={cid}]",
                    expected_value="<ABSENT>",
                    actual_value=act_item,
                    reason=f"Orphan or unexpected embedded child '{cid}' present in nested array '{full_rel_path}'",
                )
            )

        if len(exp_sub) != len(act_sub):
            defects.append(
                DocumentVerificationDefect(
                    badge=CareBadge.ACTION,
                    category=DefectCategory.NESTED_MULTIPLICITY_MISMATCH,
                    root_key=root_key,
                    path=f"{full_rel_path}.length",
                    expected_value=len(exp_sub),
                    actual_value=len(act_sub),
                    reason=(
                        f"Nested array '{full_rel_path}' multiplicity mismatch: "
                        f"expected {len(exp_sub)} elements, got {len(act_sub)}"
                    ),
                )
            )

        # Check semantic ordering when all expected children are present
        if rel.order_semantics == OrderSemantics.SEMANTIC_ORDERED and not missing_cids and not extra_cids and len(exp_sub) == len(act_sub):
            exp_order = [str(get_nested_path(it, id_field, i)) for i, it in enumerate(exp_sub)]
            act_order = [str(get_nested_path(it, id_field, i)) for i, it in enumerate(act_sub)]
            if exp_order != act_order:
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.ACTION,
                        category=DefectCategory.SEMANTIC_ORDER_VIOLATION,
                        root_key=root_key,
                        path=full_rel_path,
                        expected_value=exp_order,
                        actual_value=act_order,
                        reason=(
                            f"Semantically ordered array '{full_rel_path}' has reordered elements: "
                            f"expected sequence {exp_order}, got {act_order}"
                        ),
                    )
                )

        # Compare fields of each matched child element by identity key
        for cid in sorted(set(exp_by_id.keys()) & set(act_by_id.keys())):
            _, exp_item = exp_by_id[cid]
            _, act_item = act_by_id[cid]
            item_prefix = f"{full_rel_path}[{id_field}={cid}]"
            defects.extend(
                self._compare_subdocument_fields(
                    root_key=root_key,
                    rel=rel,
                    exp_item=exp_item,
                    act_item=act_item,
                    item_path_prefix=item_prefix,
                )
            )

        return defects

    def _compare_subdocument_fields(
        self,
        root_key: str,
        rel: EmbeddedRelationRule,
        exp_item: Dict[str, Any],
        act_item: Dict[str, Any],
        item_path_prefix: str,
    ) -> List[DocumentVerificationDefect]:
        defects: List[DocumentVerificationDefect] = []
        exp_item_flat = flatten_leaf_paths(exp_item, stop_at_arrays=True)
        act_item_flat = flatten_leaf_paths(act_item, stop_at_arrays=True)

        for fm in rel.field_mappings:
            full_field_path = f"{item_path_prefix}.{fm.target_path}"
            exp_v = get_nested_path(exp_item, fm.target_path, _ABSENT)
            act_v = get_nested_path(act_item, fm.target_path, _ABSENT)

            if exp_v is not _ABSENT and act_v is _ABSENT:
                misplaced = self._find_misplaced_path(
                    fm.source_column, fm.target_path, exp_v, act_item_flat, exp_item_flat
                )
                if misplaced:
                    defects.append(
                        DocumentVerificationDefect(
                            badge=CareBadge.ACTION,
                            category=DefectCategory.PATH_IDENTITY_MISMATCH,
                            root_key=root_key,
                            path=full_field_path,
                            expected_value=f"{fm.target_path}={exp_v!r}",
                            actual_value=f"{misplaced}={act_item_flat[misplaced]!r}",
                            reason=(
                                f"Embedded field '{fm.source_column}' placed at wrong sub-path '{misplaced}' "
                                f"instead of '{fm.target_path}' under '{item_path_prefix}'"
                            ),
                        )
                    )
                else:
                    defects.append(
                        DocumentVerificationDefect(
                            badge=CareBadge.ACTION,
                            category=DefectCategory.PATH_IDENTITY_MISMATCH,
                            root_key=root_key,
                            path=full_field_path,
                            expected_value=exp_v,
                            actual_value="<ABSENT>",
                            reason=f"Missing embedded field '{full_field_path}'",
                        )
                    )
                continue

            ok, badge_override, detail = fm.conversion.compare(exp_v, act_v)
            if not ok:
                cat = (
                    DefectCategory.TYPE_CONVERSION_MISMATCH
                    if "Type conversion mismatch" in detail
                    else DefectCategory.VALUE_MISMATCH
                )
                defects.append(
                    DocumentVerificationDefect(
                        badge=badge_override or CareBadge.ACTION,
                        category=cat,
                        root_key=root_key,
                        path=full_field_path,
                        expected_value="<ABSENT>" if exp_v is _ABSENT else exp_v,
                        actual_value="<ABSENT>" if act_v is _ABSENT else act_v,
                        reason=detail,
                    )
                )
            elif badge_override == CareBadge.REVIEW:
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.REVIEW,
                        category=DefectCategory.TOLERATED_COERCION_DRIFT,
                        root_key=root_key,
                        path=full_field_path,
                        expected_value=exp_v,
                        actual_value=act_v,
                        reason=detail,
                    )
                )

        # Check nested second-level relations under this child entity
        for sub_rel in self.spec.embedded_relations:
            if sub_rel.parent_entity == rel.child_entity:
                defects.extend(
                    self._compare_embedded_relation(
                        root_key=root_key,
                        rel=sub_rel,
                        expected_parent=exp_item,
                        actual_parent=act_item,
                        path_prefix=item_path_prefix,
                        actual_root_flat=act_item_flat,
                        expected_root_flat=exp_item_flat,
                    )
                )

        return defects

    def verify_update_delete_propagation(
        self,
        initial_source_tables: Dict[str, List[Dict[str, Any]]],
        mutations: Sequence[RelationalMutationEvent],
        actual_target_documents_after: Sequence[Dict[str, Any]],
        measured_shape: Optional[MeasuredTargetShape] = None,
        concurrency: Optional[ConcurrentSourceAssumptions] = None,
        checkpoint: Optional[DocumentVerifyCheckpoint] = None,
    ) -> PropagationVerificationResult:
        """Verifies update/delete propagation for embedded references and child arrays separately from snapshot equivalence."""
        concurrency_contract = concurrency or ConcurrentSourceAssumptions()
        conc_badge, conc_findings = concurrency_contract.evaluate()

        _, expected_docs_after, expected_effects = self.projector.apply_mutations(
            initial_source_tables, mutations
        )

        affected_root_keys: Set[str] = set()
        affected_ref_count = 0
        for eff in expected_effects:
            affected_root_keys.update(eff.affected_root_keys)
            if eff.kind in (PropagationKind.EMBEDDED_REF_FANOUT_UPDATE, PropagationKind.EMBEDDED_REF_DELETE):
                affected_ref_count += 1

        shape = measured_shape or MeasuredTargetShape.measure_from_documents(
            collection_name=self.spec.target_collection,
            documents=actual_target_documents_after,
            changed_root_keys_count=len(affected_root_keys),
            affected_embedded_refs_count=affected_ref_count,
            has_root_key_index=True,
        )
        access_plan = select_verification_strategy(shape)

        actual_by_key: Dict[str, Dict[str, Any]] = {
            self._extract_root_key(d): d for d in actual_target_documents_after
        }
        expected_by_key: Dict[str, Dict[str, Any]] = {
            self._extract_root_key(d): d for d in expected_docs_after
        }

        prop_defects: List[DocumentVerificationDefect] = []
        if conc_badge == CareBadge.ACTION:
            prop_defects.append(
                DocumentVerificationDefect(
                    badge=CareBadge.ACTION,
                    category=DefectCategory.CONCURRENT_ASSUMPTION_VIOLATION,
                    root_key="__collection__",
                    path="__concurrency__",
                    expected_value=concurrency_contract.mode.value,
                    actual_value=conc_findings,
                    reason="; ".join(f for f in conc_findings if "VIOLATION" in f),
                )
            )

        verified_effects = 0
        skipped_effects = 0

        for eff in expected_effects:
            if checkpoint is not None and checkpoint.is_effect_completed(eff.effect_id):
                skipped_effects += 1
                continue

            effect_defects = self._verify_single_propagation_effect(
                eff=eff,
                expected_by_key=expected_by_key,
                actual_by_key=actual_by_key,
            )
            prop_defects.extend(effect_defects)
            verified_effects += 1

            if checkpoint is not None and not any(d.badge == CareBadge.ACTION for d in effect_defects):
                checkpoint.mark_effect_completed(eff.effect_id)

        # Also run targeted post-mutation document verification on affected root keys
        post_snapshot = self._verify_expected_vs_actual_docs(
            expected_docs=expected_docs_after,
            actual_target_documents=actual_target_documents_after,
            measured_shape=shape,
            concurrency=concurrency_contract,
            checkpoint=None,
            changed_root_keys_subset=affected_root_keys if affected_root_keys else None,
        )

        # Merge any structural/ordering/path defects on affected documents not already represented
        existing_defect_sigs = {(d.root_key, d.path, d.category) for d in prop_defects}
        for sd in post_snapshot.defects:
            if (sd.root_key, sd.path, sd.category) not in existing_defect_sigs:
                prop_defects.append(sd)

        overall_badge = self._aggregate_badge(prop_defects, conc_badge)
        return PropagationVerificationResult(
            badge=overall_badge,
            mapping_id=self.spec.mapping_id,
            mapping_checksum=self.spec.checksum(),
            total_mutations_evaluated=len(mutations),
            total_effects_expected=len(expected_effects),
            verified_effects_count=verified_effects,
            skipped_effects_count=skipped_effects,
            access_plan=access_plan,
            concurrency_badge=conc_badge,
            concurrency_findings=conc_findings,
            propagation_defects=prop_defects,
            post_mutation_snapshot_result=post_snapshot,
        )

    def _verify_single_propagation_effect(
        self,
        eff: ExpectedPropagationEffect,
        expected_by_key: Dict[str, Dict[str, Any]],
        actual_by_key: Dict[str, Dict[str, Any]],
    ) -> List[DocumentVerificationDefect]:
        defects: List[DocumentVerificationDefect] = []

        if eff.kind == PropagationKind.EMBEDDED_REF_FANOUT_UPDATE:
            updated_roots: List[str] = []
            stale_roots: List[str] = []
            for rk in eff.affected_root_keys:
                act_doc = actual_by_key.get(rk)
                if act_doc is None:
                    stale_roots.append(rk)
                    continue
                act_sub = get_nested_path(act_doc, eff.target_path, _ABSENT)
                if act_sub == eff.expected_after_summary:
                    updated_roots.append(rk)
                else:
                    stale_roots.append(rk)
                    is_stale_before = (act_sub == eff.expected_before_summary)
                    defects.append(
                        DocumentVerificationDefect(
                            badge=CareBadge.ACTION,
                            category=DefectCategory.STALE_EMBEDDED_REFERENCE,
                            root_key=rk,
                            path=eff.target_path,
                            expected_value=eff.expected_after_summary,
                            actual_value=act_sub,
                            reason=(
                                f"Embedded reference '{eff.source_entity}({eff.source_pk})' at '{eff.target_path}' "
                                f"in document '{rk}' retains stale pre-update values"
                                if is_stale_before
                                else f"Embedded reference '{eff.source_entity}({eff.source_pk})' at '{eff.target_path}' "
                                f"in document '{rk}' did not propagate update cleanly"
                            ),
                            source_effect_id=eff.effect_id,
                        )
                    )
            if stale_roots and updated_roots:
                defects.append(
                    DocumentVerificationDefect(
                        badge=CareBadge.ACTION,
                        category=DefectCategory.INCOMPLETE_REFERENCE_FANOUT,
                        root_key=",".join(stale_roots),
                        path=eff.target_path,
                        expected_value=f"all {len(eff.affected_root_keys)} referencing docs updated",
                        actual_value=f"updated={updated_roots}, stale={stale_roots}",
                        reason=(
                            f"Partial reference fan-out for '{eff.source_entity}({eff.source_pk})': "
                            f"{len(updated_roots)}/{len(eff.affected_root_keys)} referencing documents updated; "
                            f"stale documents: {stale_roots}"
                        ),
                        source_effect_id=eff.effect_id,
                    )
                )

        elif eff.kind == PropagationKind.EMBEDDED_REF_DELETE:
            for rk in eff.affected_root_keys:
                act_doc = actual_by_key.get(rk)
                if eff.delete_policy == DeletePropagationPolicy.CASCADE_DELETE_DOCUMENT:
                    if act_doc is not None:
                        defects.append(
                            DocumentVerificationDefect(
                                badge=CareBadge.ACTION,
                                category=DefectCategory.DELETE_PROPAGATION_FAILURE,
                                root_key=rk,
                                path=self.spec.target_root_key_path,
                                expected_value="<DELETED_DOCUMENT>",
                                actual_value=act_doc,
                                reason=(
                                    f"Referenced '{eff.source_entity}({eff.source_pk})' was deleted with "
                                    f"CASCADE_DELETE_DOCUMENT, but root document '{rk}' still exists"
                                ),
                                source_effect_id=eff.effect_id,
                            )
                        )
                elif eff.delete_policy == DeletePropagationPolicy.NULLIFY_REFERENCE:
                    if act_doc is not None:
                        act_sub = get_nested_path(act_doc, eff.target_path, _ABSENT)
                        if act_sub is not None:
                            defects.append(
                                DocumentVerificationDefect(
                                    badge=CareBadge.ACTION,
                                    category=DefectCategory.DELETE_PROPAGATION_FAILURE,
                                    root_key=rk,
                                    path=eff.target_path,
                                    expected_value=None,
                                    actual_value=act_sub,
                                    reason=(
                                        f"Referenced '{eff.source_entity}({eff.source_pk})' was deleted with "
                                        f"NULLIFY_REFERENCE, but '{eff.target_path}' in '{rk}' was not nullified"
                                    ),
                                    source_effect_id=eff.effect_id,
                                )
                            )
                elif eff.delete_policy == DeletePropagationPolicy.REMOVE_ELEMENT:
                    if act_doc is not None:
                        act_sub = get_nested_path(act_doc, eff.target_path, _ABSENT)
                        if act_sub is not _ABSENT:
                            defects.append(
                                DocumentVerificationDefect(
                                    badge=CareBadge.ACTION,
                                    category=DefectCategory.DELETE_PROPAGATION_FAILURE,
                                    root_key=rk,
                                    path=eff.target_path,
                                    expected_value="<ABSENT>",
                                    actual_value=act_sub,
                                    reason=(
                                        f"Referenced '{eff.source_entity}({eff.source_pk})' was deleted with "
                                        f"REMOVE_ELEMENT, but '{eff.target_path}' in '{rk}' still exists"
                                    ),
                                    source_effect_id=eff.effect_id,
                                )
                            )
                elif eff.delete_policy == DeletePropagationPolicy.RETAIN_HISTORICAL_SNAPSHOT:
                    if act_doc is not None:
                        act_sub = get_nested_path(act_doc, eff.target_path, _ABSENT)
                        if act_sub == eff.expected_before_summary:
                            defects.append(
                                DocumentVerificationDefect(
                                    badge=CareBadge.BENIGN,
                                    category=DefectCategory.HISTORICAL_SNAPSHOT_RETAINED,
                                    root_key=rk,
                                    path=eff.target_path,
                                    expected_value=eff.expected_before_summary,
                                    actual_value=act_sub,
                                    reason=(
                                        f"Historical snapshot of deleted reference '{eff.source_entity}({eff.source_pk})' "
                                        f"intentionally retained in '{rk}'"
                                    ),
                                    source_effect_id=eff.effect_id,
                                )
                            )

        elif eff.kind == PropagationKind.EMBEDDED_CHILD_DELETE:
            rel = self._relations_by_path.get(eff.target_path)
            id_field = rel.get_target_identity_field() if rel else "id"
            for rk in eff.affected_root_keys:
                act_doc = actual_by_key.get(rk)
                if act_doc is None:
                    continue
                act_arr = get_nested_path(act_doc, eff.target_path, _ABSENT)
                if isinstance(act_arr, list):
                    lingering = [
                        it for it in act_arr
                        if isinstance(it, dict) and str(get_nested_path(it, id_field, "")) == eff.source_pk
                    ]
                    if lingering:
                        defects.append(
                            DocumentVerificationDefect(
                                badge=CareBadge.ACTION,
                                category=DefectCategory.ORPHAN_EMBEDDED_CHILD,
                                root_key=rk,
                                path=f"{eff.target_path}[{id_field}={eff.source_pk}]",
                                expected_value="<REMOVED>",
                                actual_value=lingering[0],
                                reason=(
                                    f"Deleted child '{eff.source_entity}({eff.source_pk})' was not removed from "
                                    f"embedded array '{eff.target_path}' in document '{rk}'"
                                ),
                                source_effect_id=eff.effect_id,
                            )
                        )

        elif eff.kind == PropagationKind.EMBEDDED_CHILD_REPARENT:
            rel = self._relations_by_path.get(eff.target_path)
            id_field = rel.get_target_identity_field() if rel else "id"
            old_roots = (eff.expected_before_summary or {}).get("old_root_keys", [])
            new_roots = (eff.expected_after_summary or {}).get("new_root_keys", [])

            for old_rk in old_roots:
                act_old = actual_by_key.get(old_rk)
                if act_old is not None:
                    arr = get_nested_path(act_old, eff.target_path, [])
                    if isinstance(arr, list) and any(
                        isinstance(it, dict) and str(get_nested_path(it, id_field, "")) == eff.source_pk
                        for it in arr
                    ):
                        defects.append(
                            DocumentVerificationDefect(
                                badge=CareBadge.ACTION,
                                category=DefectCategory.REPARENT_PROPAGATION_FAILURE,
                                root_key=old_rk,
                                path=f"{eff.target_path}[{id_field}={eff.source_pk}]",
                                expected_value="<MOVED_TO_NEW_PARENT>",
                                actual_value=eff.source_pk,
                                reason=(
                                    f"Reparented child '{eff.source_entity}({eff.source_pk})' still lingers in "
                                    f"former parent document '{old_rk}'"
                                ),
                                source_effect_id=eff.effect_id,
                            )
                        )
            for new_rk in new_roots:
                act_new = actual_by_key.get(new_rk)
                if act_new is not None:
                    arr = get_nested_path(act_new, eff.target_path, [])
                    if not isinstance(arr, list) or not any(
                        isinstance(it, dict) and str(get_nested_path(it, id_field, "")) == eff.source_pk
                        for it in arr
                    ):
                        defects.append(
                            DocumentVerificationDefect(
                                badge=CareBadge.ACTION,
                                category=DefectCategory.REPARENT_PROPAGATION_FAILURE,
                                root_key=new_rk,
                                path=f"{eff.target_path}[{id_field}={eff.source_pk}]",
                                expected_value=(eff.expected_after_summary or {}).get("child"),
                                actual_value="<MISSING>",
                                reason=(
                                    f"Reparented child '{eff.source_entity}({eff.source_pk})' was not added to "
                                    f"new parent document '{new_rk}'"
                                ),
                                source_effect_id=eff.effect_id,
                            )
                        )

        elif eff.kind == PropagationKind.ROOT_DELETE:
            for rk in eff.affected_root_keys:
                if rk in actual_by_key:
                    defects.append(
                        DocumentVerificationDefect(
                            badge=CareBadge.ACTION,
                            category=DefectCategory.DELETE_PROPAGATION_FAILURE,
                            root_key=rk,
                            path=self.spec.target_root_key_path,
                            expected_value=None,
                            actual_value=actual_by_key[rk],
                            reason=f"Deleted root row '{rk}' still exists in target collection",
                            source_effect_id=eff.effect_id,
                        )
                    )

        return defects

    @staticmethod
    def _aggregate_badge(defects: Sequence[DocumentVerificationDefect], conc_badge: CareBadge) -> CareBadge:
        badges = {d.badge for d in defects}
        badges.add(conc_badge)
        if CareBadge.ACTION in badges:
            return CareBadge.ACTION
        if CareBadge.REVIEW in badges:
            return CareBadge.REVIEW
        if CareBadge.BENIGN in badges:
            return CareBadge.BENIGN
        return CareBadge.PASS
