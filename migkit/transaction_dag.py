"""Parallel transaction DAG replay with explicit dependency guarantees (RCH03.transaction-dag).

Extends transaction DAG scheduling with row primary-key, unique-key (including
unique-key swaps), and cross-table foreign-key dependency edges, atomic barriers
for DDL, sequence, cascade, and cutover fences, decision-climbed huge transaction
handling, consistency-contract reordering control, downstream contention-driven
worker pacing, crash-atomic transaction rollback/resume in O(1), and committed-prefix
hole verification that blocks premature cutover.
"""
from __future__ import annotations

from collections import deque
import dataclasses
import datetime
import decimal
import enum
import fcntl
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union
import uuid

from . import decide, durable
from .dag_scheduler import (
    BarrierFence,
    BarrierType,
    CDCMutation,
    TableDependencyGraph,
    Transaction,
)


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class DependencyKind(str, enum.Enum):
    ROW_PRIMARY_KEY = "row_primary_key"
    UNIQUE_KEY = "unique_key"
    FOREIGN_KEY = "foreign_key"
    CASCADE_BARRIER = "cascade_barrier"
    DDL_BARRIER = "ddl_barrier"
    SEQUENCE_BARRIER = "sequence_barrier"
    CUTOVER_BARRIER = "cutover_barrier"
    CUSTOM_BARRIER = "custom_barrier"


class TransactionAtomicityError(RuntimeError):
    """Raised when a transaction fails mid-apply or cannot be applied atomically."""

    def __init__(self, message: str, tx_id: Any = None, lsn: Optional[int] = None,
                 care_badge: CareBadge = CareBadge.ACTION):
        super().__init__(message)
        self.tx_id = tx_id
        self.lsn = lsn
        self.care_badge = care_badge


class LeaseConflictError(RuntimeError):
    """Raised when another active runner holds the DAG execution lease."""


# --- Decision Engine Ladders (decide.Rung) -----------------------------------

REORDER_CONTRACT_LADDER = (
    decide.Rung(
        "dag_topological_parallel",
        "parallel out-of-order commit across independent DAG components",
        because="independent transactions with complete key and FK edges commit in parallel without violating causal order",
        footprint=0,
        leaves="nothing",
        needs=(
            decide.Need(
                "consistency contract permits independent transaction reordering",
                lambda f: f.get("consistency_contract") in (
                    "causal_dag", "read_committed_dag", "component_ordered"
                ) and not bool(f.get("strict_commit_order", False)),
                "{rung} requires a causal_dag or component_ordered consistency contract without strict_commit_order",
            ),
            decide.Need(
                "complete key and constraint metadata for dependency graph",
                lambda f: bool(f.get("schema_keys_complete", False))
                and not bool(f.get("has_untracked_triggers", False)),
                "{rung} needs complete primary, unique, and foreign key metadata with no untracked triggers",
            ),
            decide.Need(
                "measured target contention below saturation ceiling",
                lambda f: float(f.get("max_lock_wait_ratio", 0.0)) > 0.0
                and float(f.get("lock_wait_ratio", 0.0)) < float(f.get("max_lock_wait_ratio", 0.0)),
                "{rung} is refused because measured target lock contention reached the saturation ceiling",
            ),
        ),
    ),
    decide.Rung(
        "wave_barrier_commit",
        "parallel wave execution with source-ordered commit barrier",
        because="conflict-free waves execute concurrently while commits finalize in source LSN order",
        footprint=1,
        leaves="nothing",
        needs=(
            decide.Need(
                "consistency contract permits wave parallel execution",
                lambda f: f.get("consistency_contract") in (
                    "causal_dag", "snapshot_wave", "strict_commit_order"
                ) and bool(f.get("schema_keys_complete", False)),
                "{rung} requires complete schema key metadata and wave-compatible consistency contract",
            ),
            decide.Need(
                "target supports barrier-synchronized commit ordering",
                lambda f: bool(f.get("supports_commit_barrier", False)),
                "{rung} needs barrier-synchronized commit ordering on the target",
            ),
        ),
    ),
    decide.Rung(
        "serial_source_commit_order",
        "strict serial replay in source commit order",
        because="serial replay preserves exact source commit sequence when parallel reordering is disallowed",
        footprint=0,
        leaves="nothing",
        needs=(
            decide.Need(
                "monotonic source commit positions available",
                lambda f: bool(f.get("has_monotonic_lsn", False)),
                "{rung} needs monotonic source commit LSN positions",
            ),
        ),
    ),
)

HUGE_TXN_LADDER = (
    decide.Rung(
        "atomic_single_worker",
        "single-worker in-memory atomic transaction replay",
        because="the transaction fits within the measured single-connection atomic memory and lock budget",
        footprint=0,
        leaves="nothing",
        needs=(
            decide.Need(
                "target supports atomic transactions",
                lambda f: bool(f.get("target_supports_transactions", False)),
                "{rung} requires transactional target support",
            ),
            decide.Need(
                "transaction mutation count within measured atomic lock budget",
                lambda f: int(f.get("max_atomic_mutations", 0)) > 0
                and int(f.get("tx_mutations", 0)) <= int(f.get("max_atomic_mutations", 0)),
                "{rung} exceeds the target's measured per-transaction mutation lock budget",
            ),
            decide.Need(
                "transaction byte size within measured atomic memory budget",
                lambda f: int(f.get("max_atomic_bytes", 0)) > 0
                and int(f.get("tx_bytes", 0)) <= int(f.get("max_atomic_bytes", 0)),
                "{rung} exceeds the target's measured single-batch memory budget",
            ),
        ),
    ),
    decide.Rung(
        "savepoint_windowed_atomic",
        "windowed sub-batches inside one target transaction with savepoints",
        because="streaming savepoint windows inside one target transaction bounds packet memory while keeping commit atomic",
        footprint=1,
        leaves="nothing",
        needs=(
            decide.Need(
                "target supports transactions and savepoints",
                lambda f: bool(f.get("target_supports_transactions", False))
                and bool(f.get("target_supports_savepoints", False)),
                "{rung} requires target transactions and savepoints",
            ),
            decide.Need(
                "transaction bytes fit within single connection hold limit",
                lambda f: int(f.get("max_connection_hold_bytes", 0)) > 0
                and int(f.get("tx_bytes", 0)) <= int(f.get("max_connection_hold_bytes", 0)),
                "{rung} exceeds the target connection undo/WAL byte hold budget",
            ),
            decide.Need(
                "estimated transaction duration within target lock hold window",
                lambda f: float(f.get("max_tx_hold_seconds", 0.0)) > 0.0
                and float(f.get("estimated_tx_seconds", 0.0)) <= float(f.get("max_tx_hold_seconds", 0.0)),
                "{rung} would hold row locks longer than the measured target lock window",
            ),
        ),
    ),
    decide.Rung(
        "staged_journal_barrier",
        "barrier-isolated staging journal load and atomic swap apply",
        because="staging the huge transaction at an execution barrier and merging inside one target transaction preserves atomicity",
        footprint=2,
        leaves="a staging table",
        needs=(
            decide.Need(
                "target supports transactions and staging tables",
                lambda f: bool(f.get("target_supports_transactions", False))
                and bool(f.get("can_create_staging", False)),
                "{rung} needs transactional target support and staging privilege",
            ),
            decide.Need(
                "staging storage has capacity for transaction payload",
                lambda f: int(f.get("free_staging_bytes", 0)) > 0
                and int(f.get("tx_bytes", 0)) <= int(f.get("free_staging_bytes", 0)),
                "{rung} exceeds measured free staging storage capacity",
            ),
        ),
    ),
)

CONTENTION_PACE_LADDER = (
    decide.Rung(
        "backoff_on_contention",
        "multiplicative worker backoff on measured downstream contention",
        because="reducing concurrent workers relieves target lock waits, deadlocks, or latency inflation",
        footprint=0,
        leaves="nothing",
        needs=(
            decide.Need(
                "more than one worker active",
                lambda f: int(f.get("current_workers", 0)) > 1,
                "{rung} requires more than one active worker to back off",
            ),
            decide.Need(
                "measured downstream contention or deadlock observed",
                lambda f: int(f.get("deadlock_count", 0)) > 0
                or (
                    float(f.get("contention_ceiling", 0.0)) > 0.0
                    and float(f.get("lock_wait_ratio", 0.0)) > float(f.get("contention_ceiling", 0.0))
                )
                or (
                    float(f.get("latency_ceiling", 0.0)) > 0.0
                    and float(f.get("latency_inflation", 0.0)) > float(f.get("latency_ceiling", 0.0))
                ),
                "{rung} is not needed when downstream lock wait and latency are within bounds",
            ),
        ),
    ),
    decide.Rung(
        "scale_to_dag_width",
        "additive worker scale-up bounded by measured DAG ready width and connection headroom",
        because="independent DAG branches are ready and downstream lock wait and latency are below low-contention bounds",
        footprint=0,
        leaves="nothing",
        needs=(
            decide.Need(
                "zero deadlocks and low measured lock contention",
                lambda f: int(f.get("deadlock_count", 0)) == 0
                and float(f.get("low_contention_bound", 0.0)) > 0.0
                and float(f.get("lock_wait_ratio", 0.0)) <= float(f.get("low_contention_bound", 0.0))
                and float(f.get("latency_ceiling", 0.0)) > 0.0
                and float(f.get("latency_inflation", 0.0)) <= float(f.get("latency_ceiling", 0.0)),
                "{rung} requires zero deadlocks and measured contention within the low-contention bound",
            ),
            decide.Need(
                "measured DAG ready width exceeds current workers within connection headroom",
                lambda f: int(f.get("current_workers", 0)) >= 1
                and int(f.get("dag_ready_width", 0)) > int(f.get("current_workers", 0))
                and int(f.get("available_connections", 0)) > int(f.get("current_workers", 0)),
                "{rung} needs ready independent DAG branches and available target connections above current workers",
            ),
        ),
    ),
    decide.Rung(
        "hold_steady_pace",
        "hold current worker concurrency matched to downstream throughput",
        because="current worker concurrency matches the DAG width or downstream contention equilibrium",
        footprint=0,
        leaves="nothing",
        needs=(
            decide.Need(
                "at least one active worker and target connection available",
                lambda f: int(f.get("current_workers", 0)) >= 1
                and int(f.get("available_connections", 0)) >= 1,
                "{rung} needs at least one active worker and one available target connection",
            ),
        ),
    ),
)

CUTOVER_PREFIX_LADDER = (
    decide.Rung(
        "contiguous_prefix_certified",
        "certify gap-free committed prefix for cutover",
        because="every source transaction up to the cutover LSN has committed with zero holes, aborts, or pending barriers",
        footprint=0,
        leaves="nothing",
        needs=(
            decide.Need(
                "zero uncommitted holes before cutover LSN",
                lambda f: len(f.get("hole_lsns") or ()) == 0,
                "{rung} refuses cutover while uncommitted LSN holes remain in the prefix",
            ),
            decide.Need(
                "contiguous watermark reaches required cutover LSN",
                lambda f: f.get("contiguous_lsn") is not None
                and f.get("target_cutover_lsn") is not None
                and int(f["contiguous_lsn"]) >= int(f["target_cutover_lsn"]),
                "{rung} requires the contiguous committed watermark to reach target_cutover_lsn",
            ),
            decide.Need(
                "zero in-flight or aborted transactions",
                lambda f: int(f.get("in_flight_count", 0)) == 0
                and int(f.get("aborted_count", 0)) == 0,
                "{rung} requires zero in-flight or aborted transactions",
            ),
            decide.Need(
                "all pending barriers drained",
                lambda f: int(f.get("pending_barriers", 0)) == 0,
                "{rung} requires all DDL, sequence, cascade, and cutover barriers to complete first",
            ),
        ),
    ),
    decide.Rung(
        "drain_and_reconcile_holes",
        "hold cutover and drain in-flight hole transactions",
        because="uncommitted hole transactions are actively draining within the allowed cutover drain window",
        footprint=1,
        leaves="nothing",
        needs=(
            decide.Need(
                "no aborted transactions in hole range",
                lambda f: int(f.get("aborted_count", 0)) == 0,
                "{rung} cannot drain a hole caused by an aborted transaction without replay",
            ),
            decide.Need(
                "in-flight hole transactions draining within window",
                lambda f: len(f.get("hole_lsns") or ()) > 0
                and int(f.get("in_flight_count", 0)) > 0
                and float(f.get("max_drain_window_seconds", 0.0)) > 0.0
                and float(f.get("estimated_drain_seconds", 0.0))
                <= float(f.get("max_drain_window_seconds", 0.0)),
                "{rung} requires active in-flight workers able to drain holes within max_drain_window_seconds",
            ),
        ),
    ),
)


def choose_reorder_mode(facts: Union[decide.Facts, Dict[str, Any]],
                        work: str = "transaction_dag.reorder") -> decide.Choice:
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(facts)
    return decide.climb(work, REORDER_CONTRACT_LADDER, f)


def choose_huge_transaction_strategy(facts: Union[decide.Facts, Dict[str, Any]],
                                     work: str = "transaction_dag.huge_tx") -> decide.Choice:
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(facts)
    return decide.climb(work, HUGE_TXN_LADDER, f)


def choose_worker_concurrency(facts: Union[decide.Facts, Dict[str, Any]],
                              work: str = "transaction_dag.pace") -> decide.Choice:
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(facts)
    return decide.climb(work, CONTENTION_PACE_LADDER, f)


def choose_cutover_prefix_action(facts: Union[decide.Facts, Dict[str, Any]],
                                 work: str = "transaction_dag.cutover_prefix") -> decide.Choice:
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(facts)
    return decide.climb(work, CUTOVER_PREFIX_LADDER, f)


# --- Typed Value Serialization for O(1) Durable Checkpoints ------------------

def encode_typed_value(val: Any) -> Any:
    """Recursively encode native Python types (datetime, date, UUID, Decimal,
    tuple, bytes) into JSON-safe structures that round-trip losslessly."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "value": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "value": val.isoformat()}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "value": str(val)}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "value": str(val)}
    if isinstance(val, bytes):
        return {"__type__": "bytes", "value": val.hex()}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "value": [encode_typed_value(x) for x in val]}
    if isinstance(val, list):
        return [encode_typed_value(x) for x in val]
    if isinstance(val, dict):
        return {str(k): encode_typed_value(v) for k, v in val.items()}
    return val


def decode_typed_value(raw: Any) -> Any:
    """Restore native Python types encoded by `encode_typed_value`."""
    if isinstance(raw, list):
        return [decode_typed_value(x) for x in raw]
    if isinstance(raw, dict):
        kind = raw.get("__type__")
        if kind == "datetime":
            return datetime.datetime.fromisoformat(raw["value"])
        if kind == "date":
            return datetime.date.fromisoformat(raw["value"])
        if kind == "uuid":
            return uuid.UUID(raw["value"])
        if kind == "decimal":
            return decimal.Decimal(raw["value"])
        if kind == "bytes":
            return bytes.fromhex(raw["value"])
        if kind == "tuple":
            return tuple(decode_typed_value(x) for x in raw["value"])
        return {k: decode_typed_value(v) for k, v in raw.items()}
    return raw


def canonical_key_tuple(values: Sequence[Any]) -> Tuple[Any, ...]:
    """Convert a sequence of key column values into a hashable canonical tuple."""
    out = []
    for v in values:
        if isinstance(v, list):
            out.append( canonical_key_tuple(v) )
        elif isinstance(v, dict):
            out.append(tuple(sorted((str(k), canonical_key_tuple([val])[0]) for k, val in v.items())))
        else:
            out.append(v)
    return tuple(out)


# --- Schema Constraints & Dependency Edges -----------------------------------

@dataclasses.dataclass(frozen=True)
class ForeignKeyConstraint:
    child_table: str
    child_columns: Tuple[str, ...]
    parent_table: str
    parent_columns: Tuple[str, ...]
    on_delete: str = "RESTRICT"
    on_update: str = "RESTRICT"

    @property
    def is_cascade(self) -> bool:
        return self.on_delete.upper() == "CASCADE" or self.on_update.upper() == "CASCADE"


@dataclasses.dataclass(frozen=True)
class DependencyEdge:
    from_tx_id: Union[int, str]
    to_tx_id: Union[int, str]
    from_lsn: int
    to_lsn: int
    kind: DependencyKind
    table: str
    key_signature: Tuple[Any, ...] = ()
    reason: str = ""


class SchemaConstraintCatalog:
    """Holds primary key, unique key, and foreign key definitions for exact
    row/unique/FK dependency edge extraction."""

    def __init__(
        self,
        primary_keys: Optional[Dict[str, Sequence[str]]] = None,
        unique_keys: Optional[Dict[str, Union[Dict[str, Sequence[str]], Sequence[Sequence[str]]]]] = None,
        foreign_keys: Optional[Iterable[Any]] = None,
    ):
        self.primary_keys: Dict[str, Tuple[str, ...]] = {}
        if primary_keys:
            for tbl, cols in primary_keys.items():
                if isinstance(cols, str):
                    self.primary_keys[tbl] = (cols,)
                else:
                    self.primary_keys[tbl] = tuple(str(c) for c in cols)

        self.unique_keys: Dict[str, Dict[str, Tuple[str, ...]]] = {}
        if unique_keys:
            for tbl, spec in unique_keys.items():
                tbl_uks: Dict[str, Tuple[str, ...]] = {}
                if isinstance(spec, dict):
                    for idx_name, cols in spec.items():
                        col_tuple = (cols,) if isinstance(cols, str) else tuple(str(c) for c in cols)
                        tbl_uks[str(idx_name)] = col_tuple
                else:
                    for idx, cols in enumerate(spec):
                        col_tuple = (cols,) if isinstance(cols, str) else tuple(str(c) for c in cols)
                        tbl_uks[f"uk_{idx}_{'_'.join(col_tuple)}"] = col_tuple
                self.unique_keys[tbl] = tbl_uks

        self.foreign_keys: List[ForeignKeyConstraint] = []
        self.table_graph = TableDependencyGraph()
        if foreign_keys:
            for fk in foreign_keys:
                self.add_foreign_key(fk)

    def add_foreign_key(self, fk: Any) -> None:
        if isinstance(fk, ForeignKeyConstraint):
            self.foreign_keys.append(fk)
            self.table_graph.add_foreign_key((fk.child_table, fk.parent_table))
            return
        if isinstance(fk, dict):
            child = str(fk.get("child") or fk.get("child_table") or fk.get("table") or "")
            parent = str(fk.get("parent") or fk.get("parent_table") or fk.get("references") or "")
            c_cols_raw = fk.get("child_columns") or fk.get("column") or ("parent_id",)
            p_cols_raw = fk.get("parent_columns") or fk.get("ref_column") or ("id",)
            c_cols = (c_cols_raw,) if isinstance(c_cols_raw, str) else tuple(str(c) for c in c_cols_raw)
            p_cols = (p_cols_raw,) if isinstance(p_cols_raw, str) else tuple(str(c) for c in p_cols_raw)
            on_del = str(fk.get("on_delete", "RESTRICT")).upper()
            on_upd = str(fk.get("on_update", "RESTRICT")).upper()
            if child and parent:
                self.foreign_keys.append(
                    ForeignKeyConstraint(child, c_cols, parent, p_cols, on_del, on_upd)
                )
                self.table_graph.add_foreign_key((child, parent))
            return
        if isinstance(fk, (tuple, list)):
            if len(fk) >= 4:
                child, c_col, parent, p_col = str(fk[0]), fk[1], str(fk[2]), fk[3]
                c_cols = (c_col,) if isinstance(c_col, str) else tuple(str(c) for c in c_col)
                p_cols = (p_col,) if isinstance(p_col, str) else tuple(str(c) for c in p_col)
                on_del = str(fk[4]).upper() if len(fk) >= 5 else "RESTRICT"
                on_upd = str(fk[5]).upper() if len(fk) >= 6 else "RESTRICT"
                self.foreign_keys.append(
                    ForeignKeyConstraint(child, c_cols, parent, p_cols, on_del, on_upd)
                )
                self.table_graph.add_foreign_key((child, parent))
            elif len(fk) >= 2:
                child, parent = str(fk[0]), str(fk[1])
                p_pk = self.primary_keys.get(parent, ("id",))
                c_fk = tuple(f"{parent.rstrip('s')}_{c}" for c in p_pk)
                self.foreign_keys.append(
                    ForeignKeyConstraint(child, c_fk, parent, p_pk)
                )
                self.table_graph.add_foreign_key((child, parent))

    def extract_pk_tuples(self, mut: CDCMutation) -> List[Tuple[Any, ...]]:
        """Return all primary key tuples touched by `mut` (both before-image
        and after-image when a key changes)."""
        table = mut.table
        if not table:
            return []
        pk_cols = self.primary_keys.get(table)
        results: List[Tuple[Any, ...]] = []
        for source in (mut.key, mut.old_values, mut.values):
            if not isinstance(source, dict) or not source:
                continue
            if pk_cols:
                if all(c in source and source[c] is not None for c in pk_cols):
                    tup = canonical_key_tuple([source[c] for c in pk_cols])
                    if tup not in results:
                        results.append(tup)
            elif source is mut.key:
                cols = sorted(source.keys())
                tup = canonical_key_tuple([source[c] for c in cols])
                if tup not in results:
                    results.append(tup)
            elif "id" in source and source["id"] is not None:
                tup = canonical_key_tuple([source["id"]])
                if tup not in results:
                    results.append(tup)
        return results

    def extract_unique_tuples(self, mut: CDCMutation) -> List[Tuple[str, Tuple[Any, ...]]]:
        """Return `[(index_name, value_tuple)]` for both `old_values` and `values`
        so unique-key swaps and reuses across rows are linked."""
        table = mut.table
        if not table or table not in self.unique_keys:
            return []
        out: List[Tuple[str, Tuple[Any, ...]]] = []
        for idx_name, cols in self.unique_keys[table].items():
            for img in (mut.old_values, mut.values, mut.key):
                if not isinstance(img, dict):
                    continue
                if all(c in img and img[c] is not None for c in cols):
                    tup = (idx_name, canonical_key_tuple([img[c] for c in cols]))
                    if tup not in out:
                        out.append(tup)
        return out

    def extract_parent_fk_keys(self, mut: CDCMutation) -> List[Tuple[str, Tuple[str, ...], Tuple[Any, ...], bool]]:
        """When `mut` modifies a parent table referenced by foreign keys,
        return `[(parent_table, parent_cols, key_tuple, is_cascade_or_delete)]`."""
        table = mut.table
        if not table:
            return []
        parent_fks = [fk for fk in self.foreign_keys if fk.parent_table == table]
        if not parent_fks:
            return []
        out: List[Tuple[str, Tuple[str, ...], Tuple[Any, ...], bool]] = []
        op = mut.op.lower()
        for fk in parent_fks:
            is_casc = bool(mut.is_cascade or (op in ("delete", "update") and fk.is_cascade))
            for img in (mut.key, mut.old_values, mut.values):
                if not isinstance(img, dict):
                    continue
                if all(c in img and img[c] is not None for c in fk.parent_columns):
                    val_tup = canonical_key_tuple([img[c] for c in fk.parent_columns])
                    entry = (table, fk.parent_columns, val_tup, is_casc)
                    if entry not in out:
                        out.append(entry)
        return out

    def extract_child_fk_refs(self, mut: CDCMutation) -> List[Tuple[str, Tuple[str, ...], Tuple[Any, ...]]]:
        """When `mut` modifies a child table, return all referenced
        `[(parent_table, parent_cols, referenced_val_tuple)]` from both
        before-image and after-image."""
        table = mut.table
        if not table:
            return []
        child_fks = [fk for fk in self.foreign_keys if fk.child_table == table]
        if not child_fks:
            return []
        out: List[Tuple[str, Tuple[str, ...], Tuple[Any, ...]]] = []
        for fk in child_fks:
            for img in (mut.old_values, mut.values, mut.key):
                if not isinstance(img, dict):
                    continue
                if all(c in img and img[c] is not None for c in fk.child_columns):
                    val_tup = canonical_key_tuple([img[c] for c in fk.child_columns])
                    entry = (fk.parent_table, fk.parent_columns, val_tup)
                    if entry not in out:
                        out.append(entry)
        return out


# --- Transaction DAG Node & Graph Builder ------------------------------------

def _estimate_mutation_bytes(mut: CDCMutation) -> int:
    meta = mut.metadata or {}
    if "bytes" in meta and isinstance(meta["bytes"], (int, float)):
        return max(1, int(meta["bytes"]))
    payload = {
        "t": mut.table,
        "o": mut.op,
        "k": encode_typed_value(mut.key),
        "v": encode_typed_value(mut.values),
        "old": encode_typed_value(mut.old_values),
    }
    return len(json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"))


@dataclasses.dataclass
class TransactionNode:
    tx_id: Union[int, str]
    commit_lsn: int
    mutations: List[CDCMutation] = dataclasses.field(default_factory=list)
    barrier: Optional[BarrierFence] = None
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)
    estimated_bytes: int = 0

    def __post_init__(self):
        if self.estimated_bytes <= 0:
            if self.metadata and isinstance(self.metadata.get("bytes"), (int, float)):
                self.estimated_bytes = max(1, int(self.metadata["bytes"]))
            elif self.mutations:
                self.estimated_bytes = sum(_estimate_mutation_bytes(m) for m in self.mutations)
            else:
                self.estimated_bytes = 64

    @property
    def is_barrier(self) -> bool:
        return self.barrier is not None

    @property
    def tables(self) -> Set[str]:
        if self.barrier and self.barrier.table:
            return {self.barrier.table}
        return {m.table for m in self.mutations if m.table}


class TransactionDependencyDAG:
    """Explicit transaction-level dependency DAG tracking:
    - Row primary key conflicts (including PK updates)
    - Unique key conflicts and unique-key swaps across rows/transactions
    - Cross-table foreign key parent/child dependencies and cascades
    - DDL, sequence, cascade, and cutover barrier fences
    """

    def __init__(self, catalog: Optional[SchemaConstraintCatalog] = None):
        self.catalog = catalog or SchemaConstraintCatalog()
        self._nodes: Dict[Union[int, str], TransactionNode] = {}
        self._lsn_to_tx: Dict[int, Union[int, str]] = {}
        self._order: List[Union[int, str]] = []
        self._edges: List[DependencyEdge] = []
        self._preds: Dict[Union[int, str], Set[Union[int, str]]] = {}
        self._succs: Dict[Union[int, str], Set[Union[int, str]]] = {}

        self._last_pk_writer: Dict[Tuple[str, Tuple[Any, ...]], Union[int, str]] = {}
        self._last_uk_writer: Dict[Tuple[str, str, Tuple[Any, ...]], Union[int, str]] = {}
        self._last_parent_key_writer: Dict[Tuple[str, Tuple[str, ...], Tuple[Any, ...]], Union[int, str]] = {}
        self._last_child_key_touchers: Dict[Tuple[str, Tuple[str, ...], Tuple[Any, ...]], List[Union[int, str]]] = {}
        self._last_table_writers: Dict[str, List[Union[int, str]]] = {}
        self._last_table_cascade: Dict[str, Union[int, str]] = {}
        self._last_barrier_tx: Optional[Union[int, str]] = None
        self._lock = threading.RLock()

    def _add_edge(
        self,
        from_tx: Union[int, str],
        to_tx: Union[int, str],
        kind: DependencyKind,
        table: str,
        key_sig: Tuple[Any, ...] = (),
        reason: str = "",
    ) -> None:
        if from_tx == to_tx or from_tx not in self._nodes or to_tx not in self._nodes:
            return
        if from_tx in self._preds.get(to_tx, set()):
            for existing in self._edges:
                if existing.from_tx_id == from_tx and existing.to_tx_id == to_tx and existing.kind == kind:
                    return
        self._preds.setdefault(to_tx, set()).add(from_tx)
        self._succs.setdefault(from_tx, set()).add(to_tx)
        self._edges.append(
            DependencyEdge(
                from_tx_id=from_tx,
                to_tx_id=to_tx,
                from_lsn=self._nodes[from_tx].commit_lsn,
                to_lsn=self._nodes[to_tx].commit_lsn,
                kind=kind,
                table=table,
                key_signature=key_sig,
                reason=reason,
            )
        )

    def sort_transaction_mutations(self, mutations: Sequence[CDCMutation]) -> List[CDCMutation]:
        """Order mutations inside a single transaction so that:
        1. Child deletes precede parent deletes along FK edges.
        2. Unique-key freeing deletes/updates precede conflicting inserts on the same table.
        3. Parent inserts/updates precede child inserts/updates along FK edges.
        """
        if len(mutations) <= 1:
            return list(mutations)

        tables = {m.table for m in mutations if m.table}
        del_order = self.catalog.table_graph.topological_order(tables, op="delete")
        ins_order = self.catalog.table_graph.topological_order(tables, op="insert")
        del_rank = {t: i for i, t in enumerate(del_order)}
        ins_rank = {t: i for i, t in enumerate(ins_order)}

        indexed = list(enumerate(mutations))
        deletes = [(i, m) for i, m in indexed if m.op.lower() == "delete"]
        updates = [(i, m) for i, m in indexed if m.op.lower() == "update"]
        inserts = [(i, m) for i, m in indexed if m.op.lower() == "insert"]
        others = [(i, m) for i, m in indexed if m.op.lower() not in ("delete", "update", "insert")]

        deletes.sort(key=lambda item: (del_rank.get(item[1].table, len(del_rank)), item[0]))
        updates.sort(key=lambda item: (ins_rank.get(item[1].table, len(ins_rank)), item[0]))
        inserts.sort(key=lambda item: (ins_rank.get(item[1].table, len(ins_rank)), item[0]))

        return [m for _, m in (deletes + updates + inserts + others)]

    def add_node(self, node: TransactionNode) -> TransactionNode:
        with self._lock:
            tx_id = node.tx_id
            if tx_id in self._nodes:
                return self._nodes[tx_id]

            if node.mutations:
                node.mutations = self.sort_transaction_mutations(node.mutations)

            self._nodes[tx_id] = node
            self._lsn_to_tx[node.commit_lsn] = tx_id
            self._order.append(tx_id)
            self._preds.setdefault(tx_id, set())
            self._succs.setdefault(tx_id, set())

            if self._last_barrier_tx is not None:
                b_node = self._nodes[self._last_barrier_tx]
                b_kind = self._barrier_kind(b_node.barrier)
                self._add_edge(
                    self._last_barrier_tx,
                    tx_id,
                    b_kind,
                    b_node.barrier.table or "*" if b_node.barrier else "*",
                    reason="ordered after barrier fence",
                )

            if node.is_barrier and node.barrier is not None:
                b_kind = self._barrier_kind(node.barrier)
                for prior_tx in self._order[:-1]:
                    self._add_edge(
                        prior_tx,
                        tx_id,
                        b_kind,
                        node.barrier.table or "*",
                        reason=f"drain before {node.barrier.barrier_type} barrier",
                    )
                self._last_barrier_tx = tx_id
                return node

            has_table_wide_cascade = False
            cascade_tables: Set[str] = set()

            for mut in node.mutations:
                tbl = mut.table
                if tbl in self._last_table_cascade:
                    self._add_edge(
                        self._last_table_cascade[tbl],
                        tx_id,
                        DependencyKind.CASCADE_BARRIER,
                        tbl,
                        reason="ordered after table-wide FK cascade",
                    )

                for pk_tup in self.catalog.extract_pk_tuples(mut):
                    pk_key = (tbl, pk_tup)
                    prev_tx = self._last_pk_writer.get(pk_key)
                    if prev_tx is not None and prev_tx != tx_id:
                        self._add_edge(
                            prev_tx,
                            tx_id,
                            DependencyKind.ROW_PRIMARY_KEY,
                            tbl,
                            key_sig=pk_tup,
                            reason=f"same primary key on {tbl}",
                        )
                    self._last_pk_writer[pk_key] = tx_id

                for idx_name, uk_tup in self.catalog.extract_unique_tuples(mut):
                    uk_key = (tbl, idx_name, uk_tup)
                    prev_tx = self._last_uk_writer.get(uk_key)
                    if prev_tx is not None and prev_tx != tx_id:
                        self._add_edge(
                            prev_tx,
                            tx_id,
                            DependencyKind.UNIQUE_KEY,
                            tbl,
                            key_sig=(idx_name,) + uk_tup,
                            reason=f"unique key {idx_name} conflict or swap on {tbl}",
                        )
                    self._last_uk_writer[uk_key] = tx_id

                for p_tbl, p_cols, val_tup, is_casc in self.catalog.extract_parent_fk_keys(mut):
                    fk_key = (p_tbl, p_cols, val_tup)
                    prev_parent = self._last_parent_key_writer.get(fk_key)
                    if prev_parent is not None and prev_parent != tx_id:
                        self._add_edge(
                            prev_parent,
                            tx_id,
                            DependencyKind.FOREIGN_KEY,
                            p_tbl,
                            key_sig=val_tup,
                            reason=f"parent key order on {p_tbl}",
                        )
                    if mut.op.lower() in ("delete", "update"):
                        for child_tx in self._last_child_key_touchers.get(fk_key, ()):
                            if child_tx != tx_id:
                                self._add_edge(
                                    child_tx,
                                    tx_id,
                                    DependencyKind.FOREIGN_KEY,
                                    p_tbl,
                                    key_sig=val_tup,
                                    reason=f"parent {mut.op} ordered after child references on {p_tbl}",
                                )
                    self._last_parent_key_writer[fk_key] = tx_id
                    if is_casc:
                        has_table_wide_cascade = True
                        cascade_tables.update(self._descendant_tables(p_tbl))

                for p_tbl, p_cols, val_tup in self.catalog.extract_child_fk_refs(mut):
                    fk_key = (p_tbl, p_cols, val_tup)
                    prev_parent = self._last_parent_key_writer.get(fk_key)
                    if prev_parent is not None and prev_parent != tx_id:
                        self._add_edge(
                            prev_parent,
                            tx_id,
                            DependencyKind.FOREIGN_KEY,
                            mut.table,
                            key_sig=val_tup,
                            reason=f"child {mut.table} references parent {p_tbl}",
                        )
                    touchers = self._last_child_key_touchers.setdefault(fk_key, [])
                    if tx_id not in touchers:
                        touchers.append(tx_id)

                if mut.is_cascade and tbl:
                    has_table_wide_cascade = True
                    cascade_tables.update(self._descendant_tables(tbl))

                if tbl:
                    writers = self._last_table_writers.setdefault(tbl, [])
                    if tx_id not in writers:
                        writers.append(tx_id)

            if has_table_wide_cascade:
                for c_tbl in cascade_tables:
                    for prior_tx in self._last_table_writers.get(c_tbl, ()):
                        if prior_tx != tx_id:
                            self._add_edge(
                                prior_tx,
                                tx_id,
                                DependencyKind.CASCADE_BARRIER,
                                c_tbl,
                                reason=f"FK cascade fence on {c_tbl}",
                            )
                    self._last_table_cascade[c_tbl] = tx_id

            return node

    def _descendant_tables(self, root_table: str) -> Set[str]:
        visited = {root_table}
        queue = deque([root_table])
        while queue:
            curr = queue.popleft()
            for child in self.catalog.table_graph.get_children(curr):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
        return visited

    @staticmethod
    def _barrier_kind(barrier: Optional[BarrierFence]) -> DependencyKind:
        if barrier is None:
            return DependencyKind.CUSTOM_BARRIER
        btype = barrier.barrier_type
        if btype == BarrierType.DDL:
            return DependencyKind.DDL_BARRIER
        if btype == BarrierType.SEQUENCE:
            return DependencyKind.SEQUENCE_BARRIER
        if btype == BarrierType.CUTOVER:
            return DependencyKind.CUTOVER_BARRIER
        if btype == BarrierType.CASCADE:
            return DependencyKind.CASCADE_BARRIER
        return DependencyKind.CUSTOM_BARRIER

    def add_transaction(self, txn: Union[Transaction, TransactionNode, Dict[str, Any]]) -> TransactionNode:
        if isinstance(txn, TransactionNode):
            return self.add_node(txn)
        if isinstance(txn, Transaction):
            for m in txn.mutations:
                if m.tx_id is None:
                    m.tx_id = txn.tx_id
            node = TransactionNode(
                tx_id=txn.tx_id,
                commit_lsn=txn.commit_lsn,
                mutations=list(txn.mutations),
                metadata=dict(txn.metadata or {}),
            )
            return self.add_node(node)
        if isinstance(txn, dict):
            tx_id = txn.get("tx_id") or txn.get("txn") or f"tx-{txn.get('commit_lsn', txn.get('lsn', 0))}"
            commit_lsn = int(txn.get("commit_lsn", txn.get("lsn", 0)))
            raw_muts = txn.get("mutations") or txn.get("changes") or []
            muts = [m if isinstance(m, CDCMutation) else CDCMutation.from_dict(m) for m in raw_muts]
            for m in muts:
                if m.tx_id is None:
                    m.tx_id = tx_id
            node = TransactionNode(
                tx_id=tx_id,
                commit_lsn=commit_lsn,
                mutations=muts,
                metadata=dict(txn.get("metadata") or {}),
            )
            return self.add_node(node)
        raise TypeError(f"Unsupported transaction object: {type(txn)}")

    def add_barrier(self, barrier: BarrierFence) -> TransactionNode:
        tx_id = barrier.fence_id or f"barrier-{barrier.lsn}"
        node = TransactionNode(
            tx_id=tx_id,
            commit_lsn=barrier.lsn,
            mutations=[],
            barrier=barrier,
            metadata=dict(barrier.metadata or {}),
        )
        return self.add_node(node)

    @property
    def edges(self) -> List[DependencyEdge]:
        with self._lock:
            return list(self._edges)

    @property
    def nodes(self) -> List[TransactionNode]:
        with self._lock:
            return [self._nodes[tid] for tid in self._order]

    def get_node(self, tx_id: Union[int, str]) -> Optional[TransactionNode]:
        with self._lock:
            return self._nodes.get(tx_id)

    def predecessors(self, tx_id: Union[int, str]) -> Set[Union[int, str]]:
        with self._lock:
            return set(self._preds.get(tx_id, set()))

    def successors(self, tx_id: Union[int, str]) -> Set[Union[int, str]]:
        with self._lock:
            return set(self._succs.get(tx_id, set()))

    def ready_nodes(
        self,
        completed_tx_ids: Set[Union[int, str]],
        in_flight_tx_ids: Set[Union[int, str]],
        aborted_tx_ids: Optional[Set[Union[int, str]]] = None,
    ) -> List[TransactionNode]:
        """Return all nodes in source commit order whose predecessors have all
        completed and which are neither completed, in-flight, nor blocked by
        an aborted predecessor."""
        aborted = aborted_tx_ids or set()
        with self._lock:
            ready: List[TransactionNode] = []
            for tid in self._order:
                if tid in completed_tx_ids or tid in in_flight_tx_ids or tid in aborted:
                    continue
                preds = self._preds.get(tid, set())
                if preds <= completed_tx_ids:
                    ready.append(self._nodes[tid])
            return ready

    def topological_waves(self) -> List[List[TransactionNode]]:
        """Partition the DAG into conflict-free parallel waves where every
        node in wave `k` depends only on nodes in waves `< k`."""
        with self._lock:
            completed: Set[Union[int, str]] = set()
            waves: List[List[TransactionNode]] = []
            remaining = len(self._order)
            while len(completed) < remaining:
                wave = self.ready_nodes(completed, set())
                if not wave:
                    unresolved = [tid for tid in self._order if tid not in completed]
                    raise ValueError(f"Cycle detected in transaction DAG among: {unresolved}")
                waves.append(wave)
                for n in wave:
                    completed.add(n.tx_id)
            return waves


# --- Measured Downstream Contention Telemetry --------------------------------

class ContentionTelemetry:
    """Tracks downstream apply latency, lock wait time, and deadlocks to drive
    `CONTENTION_PACE_LADDER` without fixed worker counts."""

    def __init__(
        self,
        initial_workers: int = 1,
        available_connections: int = 8,
        low_contention_bound: Optional[float] = None,
        contention_ceiling: Optional[float] = None,
        latency_ceiling: Optional[float] = None,
    ):
        self.current_workers = max(1, int(initial_workers))
        self.available_connections = max(1, int(available_connections))
        self._samples: deque = deque(maxlen=64)
        self._baseline_per_mut_s: Optional[float] = None
        self._ewma_per_mut_s: Optional[float] = None
        self._deadlock_count: int = 0
        self._low_contention_bound = low_contention_bound
        self._contention_ceiling = contention_ceiling
        self._latency_ceiling = latency_ceiling
        self._lock = threading.Lock()
        self.decisions: List[decide.Choice] = []

    def record_execution(
        self,
        mutations_count: int,
        apply_seconds: float,
        lock_wait_seconds: float = 0.0,
        deadlock: bool = False,
    ) -> None:
        with self._lock:
            muts = max(1, int(mutations_count))
            dur = max(1e-6, float(apply_seconds))
            wait = max(0.0, float(lock_wait_seconds))
            per_mut = dur / muts
            self._samples.append((dur, wait, per_mut))
            if self._baseline_per_mut_s is None or per_mut < self._baseline_per_mut_s:
                self._baseline_per_mut_s = per_mut
            if self._ewma_per_mut_s is None:
                self._ewma_per_mut_s = per_mut
            else:
                self._ewma_per_mut_s = 0.3 * per_mut + 0.7 * self._ewma_per_mut_s
            if deadlock:
                self._deadlock_count += 1

    def snapshot_facts(self, dag_ready_width: int) -> decide.Facts:
        with self._lock:
            total_dur = sum(s[0] for s in self._samples)
            total_wait = sum(s[1] for s in self._samples)
            lock_ratio = (total_wait / total_dur) if total_dur > 0 else 0.0
            base = self._baseline_per_mut_s or 1e-4
            ewma = self._ewma_per_mut_s or base
            inflation = ewma / max(base, 1e-9)

            per_mut_vals = [s[2] for s in self._samples]
            if len(per_mut_vals) >= 2:
                spread = max(per_mut_vals) / max(min(per_mut_vals), 1e-9)
            else:
                spread = 1.25

            low_bound = (
                self._low_contention_bound
                if self._low_contention_bound is not None
                else max(0.05, min(0.20, 1.0 / (1.0 + spread)))
            )
            high_bound = (
                self._contention_ceiling
                if self._contention_ceiling is not None
                else max(low_bound * 2.0, min(0.50, 2.0 / (1.0 + spread)))
            )
            lat_ceil = (
                self._latency_ceiling
                if self._latency_ceiling is not None
                else max(1.5, min(3.0, 1.0 + spread))
            )

            return decide.Facts({
                "current_workers": self.current_workers,
                "available_connections": self.available_connections,
                "dag_ready_width": max(0, int(dag_ready_width)),
                "deadlock_count": self._deadlock_count,
                "lock_wait_ratio": lock_ratio,
                "latency_inflation": inflation,
                "low_contention_bound": low_bound,
                "contention_ceiling": high_bound,
                "latency_ceiling": lat_ceil,
            })

    def adjust_workers(self, dag_ready_width: int) -> Tuple[int, decide.Choice]:
        facts = self.snapshot_facts(dag_ready_width)
        choice = choose_worker_concurrency(facts)
        with self._lock:
            self.decisions.append(choice)
            if choice.path == "backoff_on_contention":
                if self._deadlock_count > 0:
                    self.current_workers = max(1, self.current_workers // 2)
                    self._deadlock_count = 0
                else:
                    ratio = float(facts.get("lock_wait_ratio", 0.0))
                    scaled = int(self.current_workers * max(0.5, 1.0 - ratio))
                    self.current_workers = max(1, min(self.current_workers - 1, scaled))
            elif choice.path == "scale_to_dag_width":
                target = min(int(facts["dag_ready_width"]), int(facts["available_connections"]))
                step = max(1, (target - self.current_workers + 1) // 2)
                self.current_workers = min(target, self.current_workers + step)
            return self.current_workers, choice


# --- Active Lease Lock & Typed Durable Checkpoint ----------------------------

class ActiveVerificationLeaseLock:
    """Mutual exclusion `.run.lock` with fcntl lock and heartbeat preventing
    overlapping transaction DAG runs."""

    def __init__(self, lock_path: Union[str, Path], lease_seconds: float = 30.0):
        self.lock_path = Path(lock_path)
        self.lease_seconds = max(1.0, float(lease_seconds))
        self._fd: Optional[int] = None
        self.owner_pid: int = os.getpid()
        self.acquired_at: float = 0.0
        self.heartbeat_at: float = 0.0

    def acquire(self) -> None:
        if self._fd is not None:
            self.heartbeat()
            return
        if not self.lock_path.parent.exists():
            durable.private_dir(self.lock_path.parent)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise LeaseConflictError(
                f"Active DAG lease lock already held at {self.lock_path}"
            ) from exc
        self._fd = fd
        now = time.time()
        self.acquired_at = now
        self.heartbeat_at = now
        self._write_metadata()

    def heartbeat(self) -> None:
        if self._fd is None:
            raise LeaseConflictError("Cannot heartbeat an unacquired lease lock")
        self.heartbeat_at = time.time()
        self._write_metadata()

    def _write_metadata(self) -> None:
        if self._fd is None:
            return
        payload = json.dumps({
            "pid": self.owner_pid,
            "acquired_at": self.acquired_at,
            "heartbeat_at": self.heartbeat_at,
            "expires_at": self.heartbeat_at + self.lease_seconds,
        }).encode("utf-8")
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
            self.lock_path.unlink(missing_ok=True)

    def __enter__(self) -> "ActiveVerificationLeaseLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


@dataclasses.dataclass(frozen=True)
class CutoverPrefixProof:
    allowed: bool
    care_badge: CareBadge
    chosen_way: Optional[str]
    reason: str
    contiguous_committed_lsn: int
    target_cutover_lsn: int
    hole_lsns: Tuple[int, ...]
    aborted_lsns: Tuple[int, ...]
    pending_barriers: Tuple[int, ...]
    choice: decide.Choice


class TypedDurableDAGCheckpoint:
    """Tracks committed prefix watermark, out-of-order completed LSNs, holes,
    aborted LSNs, and typed cursor state with O(1) skip lookup on resume."""

    def __init__(self, checkpoint_path: Optional[Union[str, Path]] = None):
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
        self._expected_lsns: Set[int] = set()
        self._committed_lsns: Set[int] = set()
        self._committed_tx_ids: Set[Union[int, str]] = set()
        self._aborted_lsns: Dict[int, str] = {}
        self._pending_barriers: Set[int] = set()
        self._contiguous_lsn: int = -1
        self._cursor_by_lsn: Dict[int, Any] = {}
        self._cursor_state: Dict[str, Any] = {}
        self._lock = threading.RLock()

        if self.checkpoint_path and self.checkpoint_path.exists():
            self.load()

    def register_expected(self, tx_id: Union[int, str], lsn: int, is_barrier: bool = False) -> None:
        with self._lock:
            self._expected_lsns.add(int(lsn))
            if is_barrier:
                self._pending_barriers.add(int(lsn))
            self._advance_contiguous_locked()

    def is_completed(self, tx_id: Union[int, str], lsn: int) -> bool:
        """O(1) lookup to skip already-committed transactions on resume."""
        with self._lock:
            if tx_id in self._committed_tx_ids or int(lsn) in self._committed_lsns:
                return True
            if self._contiguous_lsn >= 0 and int(lsn) <= self._contiguous_lsn and int(lsn) in self._expected_lsns:
                return True
            return False

    def mark_committed(
        self,
        tx_id: Union[int, str],
        lsn: int,
        cursor_key: Any = None,
        is_barrier: bool = False,
    ) -> int:
        with self._lock:
            lsn_i = int(lsn)
            self._expected_lsns.add(lsn_i)
            self._committed_lsns.add(lsn_i)
            self._committed_tx_ids.add(tx_id)
            self._aborted_lsns.pop(lsn_i, None)
            if is_barrier:
                self._pending_barriers.discard(lsn_i)
            if cursor_key is not None:
                self._cursor_by_lsn[lsn_i] = cursor_key
            self._advance_contiguous_locked()
            self.save()
            return self._contiguous_lsn

    def mark_aborted(self, tx_id: Union[int, str], lsn: int, reason: str) -> None:
        with self._lock:
            lsn_i = int(lsn)
            self._expected_lsns.add(lsn_i)
            self._aborted_lsns[lsn_i] = str(reason)
            self.save()

    def _advance_contiguous_locked(self) -> None:
        ordered = sorted(self._expected_lsns)
        for lsn in ordered:
            if lsn <= self._contiguous_lsn:
                if lsn in self._cursor_by_lsn:
                    self._cursor_state["last_cursor_key"] = self._cursor_by_lsn[lsn]
                continue
            if lsn in self._committed_lsns:
                self._contiguous_lsn = lsn
                if lsn in self._cursor_by_lsn:
                    self._cursor_state["last_cursor_key"] = self._cursor_by_lsn[lsn]
            else:
                break

    @property
    def contiguous_committed_lsn(self) -> int:
        with self._lock:
            return self._contiguous_lsn

    @property
    def committed_lsns(self) -> Set[int]:
        with self._lock:
            return set(self._committed_lsns)

    @property
    def committed_tx_ids(self) -> Set[Union[int, str]]:
        with self._lock:
            return set(self._committed_tx_ids)

    @property
    def last_cursor_key(self) -> Any:
        with self._lock:
            return self._cursor_state.get("last_cursor_key")

    def hole_lsns(self, up_to_lsn: Optional[int] = None) -> List[int]:
        """Return uncommitted LSNs that precede either `up_to_lsn` or the
        highest committed LSN (i.e. holes in the committed prefix)."""
        with self._lock:
            ceiling = up_to_lsn
            if ceiling is None:
                ceiling = max(self._committed_lsns) if self._committed_lsns else self._contiguous_lsn
            return sorted(
                lsn for lsn in self._expected_lsns
                if lsn <= ceiling and lsn not in self._committed_lsns
            )

    def verify_cutover_prefix(
        self,
        target_cutover_lsn: int,
        in_flight_count: int = 0,
        estimated_drain_seconds: float = 0.0,
        max_drain_window_seconds: float = 0.0,
    ) -> CutoverPrefixProof:
        with self._lock:
            holes = tuple(self.hole_lsns(up_to_lsn=target_cutover_lsn))
            aborted = tuple(sorted(lsn for lsn in self._aborted_lsns if lsn <= target_cutover_lsn))
            barriers = tuple(sorted(lsn for lsn in self._pending_barriers if lsn <= target_cutover_lsn))
            facts = decide.Facts({
                "hole_lsns": holes,
                "contiguous_lsn": self._contiguous_lsn,
                "target_cutover_lsn": int(target_cutover_lsn),
                "in_flight_count": int(in_flight_count),
                "aborted_count": len(aborted),
                "pending_barriers": len(barriers),
                "estimated_drain_seconds": float(estimated_drain_seconds),
                "max_drain_window_seconds": float(max_drain_window_seconds),
            })
            choice = choose_cutover_prefix_action(facts)
            if choice.path == "contiguous_prefix_certified":
                badge = CareBadge.PASS
                allowed = True
            elif choice.path == "drain_and_reconcile_holes":
                badge = CareBadge.REVIEW
                allowed = False
            else:
                badge = CareBadge.ACTION
                allowed = False
            return CutoverPrefixProof(
                allowed=allowed,
                care_badge=badge,
                chosen_way=choice.path,
                reason=choice.reason,
                contiguous_committed_lsn=self._contiguous_lsn,
                target_cutover_lsn=int(target_cutover_lsn),
                hole_lsns=holes,
                aborted_lsns=aborted,
                pending_barriers=barriers,
                choice=choice,
            )

    def save(self) -> None:
        if not self.checkpoint_path:
            return
        with self._lock:
            payload = {
                "contiguous_lsn": self._contiguous_lsn,
                "expected_lsns": sorted(self._expected_lsns),
                "committed_lsns": sorted(self._committed_lsns),
                "committed_tx_ids": encode_typed_value(list(self._committed_tx_ids)),
                "aborted_lsns": {str(k): v for k, v in self._aborted_lsns.items()},
                "pending_barriers": sorted(self._pending_barriers),
                "cursor_by_lsn": {
                    str(k): encode_typed_value(v) for k, v in self._cursor_by_lsn.items()
                },
                "cursor_state": encode_typed_value(self._cursor_state),
            }
            durable.write_json(self.checkpoint_path, payload)

    def load(self) -> None:
        if not self.checkpoint_path or not self.checkpoint_path.exists():
            return
        with self._lock:
            data = durable.read_state(self.checkpoint_path, "position")
            if not data:
                return
            self._contiguous_lsn = int(data.get("contiguous_lsn", -1))
            self._expected_lsns = {int(x) for x in data.get("expected_lsns", [])}
            self._committed_lsns = {int(x) for x in data.get("committed_lsns", [])}
            self._committed_tx_ids = set(decode_typed_value(data.get("committed_tx_ids", [])))
            self._aborted_lsns = {int(k): str(v) for k, v in (data.get("aborted_lsns") or {}).items()}
            self._pending_barriers = {int(x) for x in data.get("pending_barriers", [])}
            self._cursor_by_lsn = {
                int(k): decode_typed_value(v)
                for k, v in (data.get("cursor_by_lsn") or {}).items()
            }
            self._cursor_state = decode_typed_value(data.get("cursor_state") or {})
            self._advance_contiguous_locked()


# --- Transactional Target Store & Parallel DAG Executor ----------------------

class TransactionalTargetStore:
    """In-memory or adapter-backed transactional target enforcing primary-key,
    unique-key, and foreign-key constraints with full atomic commit/rollback,
    savepoints, staging tables, and DDL execution."""

    def __init__(self, catalog: Optional[SchemaConstraintCatalog] = None):
        self.catalog = catalog or SchemaConstraintCatalog()
        self._tables: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]] = {}
        self._schemas: Dict[str, Set[str]] = {}
        self._sequences: Dict[str, int] = {}
        self._staging_buffers: Dict[Union[int, str], List[CDCMutation]] = {}
        self._tx_snapshots: Dict[Union[int, str], Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]]] = {}
        self._savepoints: Dict[Tuple[Union[int, str], str], Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]]] = {}
        self._tx_Lock = threading.RLock()
        self.ddl_history: List[str] = []
        self.committed_transactions: List[Union[int, str]] = []
        self.rolled_back_transactions: List[Union[int, str]] = []

    def _clone_tables(self, src: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]]) -> Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]]:
        return {
            tbl: {pk: dict(row) for pk, row in rows.items()}
            for tbl, rows in src.items()
        }

    def begin_tx(self, tx_id: Union[int, str]) -> None:
        with self._tx_Lock:
            self._tx_snapshots[tx_id] = self._clone_tables(self._tables)

    def create_savepoint(self, tx_id: Union[int, str], name: str) -> None:
        with self._tx_Lock:
            if tx_id not in self._tx_snapshots:
                raise TransactionAtomicityError(f"Transaction {tx_id} not active for savepoint {name}", tx_id=tx_id)
            self._savepoints[(tx_id, name)] = self._clone_tables(self._tx_snapshots[tx_id])

    def rollback_to_savepoint(self, tx_id: Union[int, str], name: str) -> None:
        with self._tx_Lock:
            snap = self._savepoints.get((tx_id, name))
            if snap is None:
                raise TransactionAtomicityError(f"Savepoint {name} not found for tx {tx_id}", tx_id=tx_id)
            self._tx_snapshots[tx_id] = self._clone_tables(snap)

    def stage_mutations(self, tx_id: Union[int, str], mutations: Sequence[CDCMutation]) -> None:
        with self._tx_Lock:
            buf = self._staging_buffers.setdefault(tx_id, [])
            buf.extend(mutations)

    def apply_staged(self, tx_id: Union[int, str]) -> int:
        with self._tx_Lock:
            staged = self._staging_buffers.pop(tx_id, [])
            for mut in staged:
                self.apply_mutation(tx_id, mut)
            return len(staged)

    def apply_mutation(self, tx_id: Union[int, str], mut: CDCMutation) -> None:
        with self._tx_Lock:
            if tx_id not in self._tx_snapshots:
                self.begin_tx(tx_id)
            working = self._tx_snapshots[tx_id]
            tbl = mut.table
            rows = working.setdefault(tbl, {})
            op = mut.op.lower()

            pk_tuples = self.catalog.extract_pk_tuples(mut)
            if not pk_tuples:
                raise TransactionAtomicityError(
                    f"Missing primary key on {tbl} for operation {op}", tx_id=tx_id, lsn=mut.lsn
                )

            if op == "delete":
                target_pk = pk_tuples[0]
                if mut.is_cascade:
                    self._cascade_delete(working, tbl, target_pk, mut)
                rows.pop(target_pk, None)
                self._check_foreign_keys_on_delete(working, tbl, target_pk, mut.lsn, tx_id)
                return

            if op == "update":
                old_pk = pk_tuples[0]
                new_pk = pk_tuples[-1]
                existing = dict(rows.get(old_pk) or mut.old_values or {})
                if old_pk != new_pk:
                    rows.pop(old_pk, None)
                if mut.values:
                    existing.update(mut.values)
                if mut.key:
                    existing.update(mut.key)
                self._check_unique_constraints(working, tbl, new_pk, existing, mut.lsn, tx_id)
                self._check_child_foreign_keys(working, tbl, existing, mut.lsn, tx_id)
                rows[new_pk] = existing
                return

            if op == "insert":
                new_pk = pk_tuples[-1]
                row_data = {}
                if mut.key:
                    row_data.update(mut.key)
                if mut.values:
                    row_data.update(mut.values)
                if new_pk in rows:
                    raise TransactionAtomicityError(
                        f"Duplicate primary key {new_pk} on {tbl}", tx_id=tx_id, lsn=mut.lsn
                    )
                self._check_unique_constraints(working, tbl, new_pk, row_data, mut.lsn, tx_id)
                self._check_child_foreign_keys(working, tbl, row_data, mut.lsn, tx_id)
                rows[new_pk] = row_data
                return

    def _cascade_delete(
        self,
        working: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]],
        parent_table: str,
        parent_pk: Tuple[Any, ...],
        mut: CDCMutation,
    ) -> None:
        parent_row = working.get(parent_table, {}).get(parent_pk) or mut.old_values or mut.key or {}
        for fk in self.catalog.foreign_keys:
            if fk.parent_table != parent_table:
                continue
            if not all(c in parent_row for c in fk.parent_columns):
                continue
            p_val = canonical_key_tuple([parent_row[c] for c in fk.parent_columns])
            child_rows = working.get(fk.child_table, {})
            to_delete = []
            for c_pk, c_row in list(child_rows.items()):
                if all(c in c_row for c in fk.child_columns):
                    c_val = canonical_key_tuple([c_row[c] for c in fk.child_columns])
                    if c_val == p_val:
                        to_delete.append(c_pk)
            for c_pk in to_delete:
                self._cascade_delete(working, fk.child_table, c_pk, mut)
                child_rows.pop(c_pk, None)

    def _check_unique_constraints(
        self,
        working: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]],
        table: str,
        row_pk: Tuple[Any, ...],
        row_data: Dict[str, Any],
        lsn: int,
        tx_id: Union[int, str],
    ) -> None:
        uks = self.catalog.unique_keys.get(table, {})
        if not uks:
            return
        table_rows = working.get(table, {})
        for idx_name, cols in uks.items():
            if not all(c in row_data and row_data[c] is not None for c in cols):
                continue
            candidate_val = canonical_key_tuple([row_data[c] for c in cols])
            for other_pk, other_row in table_rows.items():
                if other_pk == row_pk:
                    continue
                if all(c in other_row and other_row[c] is not None for c in cols):
                    other_val = canonical_key_tuple([other_row[c] for c in cols])
                    if other_val == candidate_val:
                        raise TransactionAtomicityError(
                            f"Unique constraint {idx_name} violation on {table} for {candidate_val}",
                            tx_id=tx_id,
                            lsn=lsn,
                        )

    def _check_child_foreign_keys(
        self,
        working: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]],
        table: str,
        row_data: Dict[str, Any],
        lsn: int,
        tx_id: Union[int, str],
    ) -> None:
        for fk in self.catalog.foreign_keys:
            if fk.child_table != table:
                continue
            if not all(c in row_data and row_data[c] is not None for c in fk.child_columns):
                continue
            ref_val = canonical_key_tuple([row_data[c] for c in fk.child_columns])
            parent_rows = working.get(fk.parent_table, {})
            found = False
            for p_pk, p_row in parent_rows.items():
                if all(c in p_row for c in fk.parent_columns):
                    if canonical_key_tuple([p_row[c] for c in fk.parent_columns]) == ref_val:
                        found = True
                        break
                elif p_pk == ref_val:
                    found = True
                    break
            if not found:
                raise TransactionAtomicityError(
                    f"Foreign key violation on {table} referencing {fk.parent_table}={ref_val}",
                    tx_id=tx_id,
                    lsn=lsn,
                )

    def _check_foreign_keys_on_delete(
        self,
        working: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]],
        parent_table: str,
        deleted_pk: Tuple[Any, ...],
        lsn: int,
        tx_id: Union[int, str],
    ) -> None:
        for fk in self.catalog.foreign_keys:
            if fk.parent_table != parent_table or fk.is_cascade:
                continue
            child_rows = working.get(fk.child_table, {})
            for c_row in child_rows.values():
                if all(c in c_row and c_row[c] is not None for c in fk.child_columns):
                    c_val = canonical_key_tuple([c_row[c] for c in fk.child_columns])
                    if c_val == deleted_pk:
                        raise TransactionAtomicityError(
                            f"Foreign key restrict violation deleting {parent_table}={deleted_pk}; referenced by {fk.child_table}",
                            tx_id=tx_id,
                            lsn=lsn,
                        )

    def commit_tx(self, tx_id: Union[int, str]) -> None:
        with self._tx_Lock:
            snap = self._tx_snapshots.pop(tx_id, None)
            if snap is None:
                return
            for key in [k for k in self._savepoints if k[0] == tx_id]:
                self._savepoints.pop(key, None)
            self._staging_buffers.pop(tx_id, None)
            for tbl, tbl_rows in snap.items():
                self._tables[tbl] = tbl_rows
            self.committed_transactions.append(tx_id)

    def rollback_tx(self, tx_id: Union[int, str]) -> None:
        with self._tx_Lock:
            self._tx_snapshots.pop(tx_id, None)
            for key in [k for k in self._savepoints if k[0] == tx_id]:
                self._savepoints.pop(key, None)
            self._staging_buffers.pop(tx_id, None)
            self.rolled_back_transactions.append(tx_id)

    def execute_ddl(self, sql: str, table: Optional[str] = None, add_column: Optional[str] = None) -> None:
        with self._tx_Lock:
            self.ddl_history.append(sql)
            if table and add_column:
                self._schemas.setdefault(table, set()).add(add_column)

    def set_sequence(self, seq_name: str, value: int) -> None:
        with self._tx_Lock:
            self._sequences[seq_name] = int(value)

    def get_row(self, table: str, pk: Union[Tuple[Any, ...], Any]) -> Optional[Dict[str, Any]]:
        pk_tup = pk if isinstance(pk, tuple) else (pk,)
        with self._tx_Lock:
            row = self._tables.get(table, {}).get(pk_tup)
            return dict(row) if row is not None else None

    def table_rows(self, table: str) -> List[Dict[str, Any]]:
        with self._tx_Lock:
            rows = self._tables.get(table, {})
            return [dict(rows[k]) for k in sorted(rows.keys())]


@dataclasses.dataclass
class DAGExecutionReport:
    care_badge: CareBadge
    reorder_choice: decide.Choice
    huge_tx_choices: Dict[Union[int, str], decide.Choice]
    pace_choices: List[decide.Choice]
    committed_tx_ids: List[Union[int, str]]
    skipped_tx_ids: List[Union[int, str]]
    aborted_tx_ids: List[Union[int, str]]
    barriers_executed: int
    contiguous_committed_lsn: int
    hole_lsns: List[int]
    final_workers: int
    edges_count: int


class ParallelTransactionDAGExecutor:
    """Executes a `TransactionDependencyDAG` with explicit dependency guarantees,
    huge-transaction handling via `HUGE_TXN_LADDER`, consistency-contract
    reordering via `REORDER_CONTRACT_LADDER`, contention-driven concurrency via
    `CONTENTION_PACE_LADDER`, and crash-atomic durable resume in O(1)."""

    def __init__(
        self,
        catalog: Optional[SchemaConstraintCatalog] = None,
        store: Optional[TransactionalTargetStore] = None,
        checkpoint: Optional[TypedDurableDAGCheckpoint] = None,
        lock_path: Optional[Union[str, Path]] = None,
        telemetry: Optional[ContentionTelemetry] = None,
        facts: Optional[Union[decide.Facts, Dict[str, Any]]] = None,
        mutation_hook: Optional[Callable[[Union[int, str], int, CDCMutation], None]] = None,
    ):
        self.catalog = catalog or SchemaConstraintCatalog()
        self.store = store if store is not None else TransactionalTargetStore(self.catalog)
        self.checkpoint = checkpoint if checkpoint is not None else TypedDurableDAGCheckpoint()
        self.lock_path = Path(lock_path) if lock_path else None
        self.telemetry = telemetry if telemetry is not None else ContentionTelemetry()
        self.base_facts = dict(facts or {})
        self.mutation_hook = mutation_hook

    def _build_reorder_facts(self, nodes: Sequence[TransactionNode]) -> decide.Facts:
        monotonic = all(
            nodes[i].commit_lsn <= nodes[i + 1].commit_lsn
            for i in range(len(nodes) - 1)
        ) if len(nodes) > 1 else bool(nodes)
        snap = self.telemetry.snapshot_facts(dag_ready_width=len(nodes))
        merged = {
            "consistency_contract": "causal_dag",
            "strict_commit_order": False,
            "schema_keys_complete": True,
            "has_untracked_triggers": False,
            "supports_commit_barrier": True,
            "has_monotonic_lsn": monotonic,
            "lock_wait_ratio": snap.get("lock_wait_ratio", 0.0),
            "max_lock_wait_ratio": snap.get("contention_ceiling", 0.35),
        }
        merged.update(self.base_facts)
        return decide.Facts(merged)

    def _build_huge_tx_facts(self, node: TransactionNode) -> decide.Facts:
        mut_count = len(node.mutations)
        tx_bytes = node.estimated_bytes
        avg_mut_bytes = max(1, tx_bytes // max(1, mut_count))
        mem_budget_bytes = int(self.base_facts.get("measured_worker_memory_bytes", 256 * 1024))
        max_atomic_bytes = int(self.base_facts.get("max_atomic_bytes", mem_budget_bytes))
        max_atomic_muts = int(
            self.base_facts.get("max_atomic_mutations", max(1, max_atomic_bytes // max(avg_mut_bytes, 64)))
        )
        max_conn_bytes = int(self.base_facts.get("max_connection_hold_bytes", max_atomic_bytes * 8))
        free_staging = int(self.base_facts.get("free_staging_bytes", max_conn_bytes * 4))
        rate_muts_per_s = float(self.base_facts.get("measured_apply_rate_muts_s", 5000.0))
        est_seconds = float(
            node.metadata.get("estimated_tx_seconds", mut_count / max(rate_muts_per_s, 1.0))
        )
        max_hold_s = float(self.base_facts.get("max_tx_hold_seconds", 30.0))

        merged = {
            "target_supports_transactions": True,
            "target_supports_savepoints": True,
            "can_create_staging": True,
            "tx_mutations": mut_count,
            "tx_bytes": tx_bytes,
            "max_atomic_mutations": max_atomic_muts,
            "max_atomic_bytes": max_atomic_bytes,
            "max_connection_hold_bytes": max_conn_bytes,
            "estimated_tx_seconds": est_seconds,
            "max_tx_hold_seconds": max_hold_s,
            "free_staging_bytes": free_staging,
        }
        merged.update(self.base_facts)
        if node.metadata:
            for k in (
                "target_supports_transactions",
                "target_supports_savepoints",
                "can_create_staging",
                "max_atomic_mutations",
                "max_atomic_bytes",
                "max_connection_hold_bytes",
                "estimated_tx_seconds",
                "max_tx_hold_seconds",
                "free_staging_bytes",
            ):
                if k in node.metadata:
                    merged[k] = node.metadata[k]
        return decide.Facts(merged)

    def _execute_node(
        self,
        node: TransactionNode,
        huge_choices: Dict[Union[int, str], decide.Choice],
        choices_lock: threading.Lock,
    ) -> None:
        if node.is_barrier and node.barrier is not None:
            res = node.barrier.execute()
            meta = node.barrier.metadata or {}
            if node.barrier.barrier_type == BarrierType.DDL and meta.get("sql"):
                self.store.execute_ddl(
                    str(meta["sql"]),
                    table=node.barrier.table,
                    add_column=meta.get("add_column"),
                )
            elif node.barrier.barrier_type == BarrierType.SEQUENCE and meta.get("sequence_name") is not None:
                self.store.set_sequence(str(meta["sequence_name"]), int(meta.get("value", 0)))
            self.checkpoint.mark_committed(
                node.tx_id,
                node.commit_lsn,
                cursor_key=meta.get("cursor_key", res),
                is_barrier=True,
            )
            return

        facts = self._build_huge_tx_facts(node)
        choice = choose_huge_transaction_strategy(
            facts, work=f"transaction_dag.huge_tx:{node.tx_id}"
        )
        with choices_lock:
            huge_choices[node.tx_id] = choice

        if choice.rung is None:
            self.checkpoint.mark_aborted(node.tx_id, node.commit_lsn, choice.reason)
            raise TransactionAtomicityError(
                f"Refused non-atomic huge transaction {node.tx_id}: {choice.reason}",
                tx_id=node.tx_id,
                lsn=node.commit_lsn,
                care_badge=CareBadge.ACTION,
            )

        t0 = time.monotonic()
        self.store.begin_tx(node.tx_id)
        try:
            if choice.path == "atomic_single_worker":
                for idx, mut in enumerate(node.mutations):
                    if self.mutation_hook is not None:
                        self.mutation_hook(node.tx_id, idx, mut)
                    self.store.apply_mutation(node.tx_id, mut)
            elif choice.path == "savepoint_windowed_atomic":
                max_win = max(1, int(facts.get("max_atomic_mutations", 1)))
                total = len(node.mutations)
                pos = 0
                win_num = 0
                while pos < total:
                    sp_name = f"sp_{node.tx_id}_{win_num}"
                    self.store.create_savepoint(node.tx_id, sp_name)
                    end = min(total, pos + max_win)
                    for idx in range(pos, end):
                        mut = node.mutations[idx]
                        if self.mutation_hook is not None:
                            self.mutation_hook(node.tx_id, idx, mut)
                        self.store.apply_mutation(node.tx_id, mut)
                    pos = end
                    win_num += 1
            elif choice.path == "staged_journal_barrier":
                max_win = max(1, int(facts.get("max_atomic_mutations", 1)))
                total = len(node.mutations)
                pos = 0
                while pos < total:
                    end = min(total, pos + max_win)
                    for idx in range(pos, end):
                        mut = node.mutations[idx]
                        if self.mutation_hook is not None:
                            self.mutation_hook(node.tx_id, idx, mut)
                    self.store.stage_mutations(node.tx_id, node.mutations[pos:end])
                    pos = end
                self.store.apply_staged(node.tx_id)

            self.store.commit_tx(node.tx_id)
        except Exception as exc:
            self.store.rollback_tx(node.tx_id)
            self.checkpoint.mark_aborted(node.tx_id, node.commit_lsn, str(exc))
            raise

        elapsed = max(1e-6, time.monotonic() - t0)
        lock_wait = float(node.metadata.get("lock_wait_seconds", 0.0))
        deadlock = bool(node.metadata.get("deadlock", False))
        self.telemetry.record_execution(
            len(node.mutations),
            apply_seconds=elapsed,
            lock_wait_seconds=lock_wait,
            deadlock=deadlock,
        )
        last_key = node.metadata.get("cursor_key")
        if last_key is None and node.mutations and node.mutations[-1].key:
            last_key = node.mutations[-1].key
        self.checkpoint.mark_committed(
            node.tx_id,
            node.commit_lsn,
            cursor_key=last_key,
            is_barrier=False,
        )

    def execute(self, events: Sequence[Union[Transaction, TransactionNode, BarrierFence, Dict[str, Any]]]) -> DAGExecutionReport:
        lease_ctx = (
            ActiveVerificationLeaseLock(self.lock_path)
            if self.lock_path is not None
            else None
        )
        if lease_ctx is not None:
            lease_ctx.acquire()
        try:
            return self._execute_locked(events, lease_ctx)
        finally:
            if lease_ctx is not None:
                lease_ctx.release()

    def _execute_locked(
        self,
        events: Sequence[Union[Transaction, TransactionNode, BarrierFence, Dict[str, Any]]],
        lease_ctx: Optional[ActiveVerificationLeaseLock],
    ) -> DAGExecutionReport:
        dag = TransactionDependencyDAG(self.catalog)
        for ev in events:
            if isinstance(ev, BarrierFence):
                node = dag.add_barrier(ev)
            else:
                node = dag.add_transaction(ev)
            self.checkpoint.register_expected(node.tx_id, node.commit_lsn, is_barrier=node.is_barrier)

        all_nodes = dag.nodes
        reorder_facts = self._build_reorder_facts(all_nodes)
        reorder_choice = choose_reorder_mode(reorder_facts)
        if reorder_choice.rung is None:
            raise TransactionAtomicityError(
                f"Cannot schedule transaction DAG: {reorder_choice.reason}",
                care_badge=CareBadge.ACTION,
            )

        completed: Set[Union[int, str]] = set()
        skipped: List[Union[int, str]] = []
        committed_now: List[Union[int, str]] = []
        aborted: Set[Union[int, str]] = set()
        huge_choices: Dict[Union[int, str], decide.Choice] = {}
        choices_lock = threading.Lock()
        barriers_executed = 0

        for node in all_nodes:
            if self.checkpoint.is_completed(node.tx_id, node.commit_lsn):
                completed.add(node.tx_id)
                skipped.append(node.tx_id)

        first_error: Optional[Exception] = None

        if reorder_choice.path == "serial_source_commit_order":
            for node in all_nodes:
                if node.tx_id in completed:
                    continue
                if lease_ctx is not None:
                    lease_ctx.heartbeat()
                self.telemetry.adjust_workers(dag_ready_width=1)
                try:
                    self._execute_node(node, huge_choices, choices_lock)
                    completed.add(node.tx_id)
                    committed_now.append(node.tx_id)
                    if node.is_barrier:
                        barriers_executed += 1
                except Exception as exc:
                    aborted.add(node.tx_id)
                    first_error = exc
                    break
        else:
            waves = dag.topological_waves()
            for wave in waves:
                pending_wave = [n for n in wave if n.tx_id not in completed]
                if not pending_wave:
                    continue
                if lease_ctx is not None:
                    lease_ctx.heartbeat()
                workers, _ = self.telemetry.adjust_workers(dag_ready_width=len(pending_wave))

                if len(pending_wave) == 1 or workers == 1 or any(n.is_barrier for n in pending_wave):
                    for node in pending_wave:
                        try:
                            self._execute_node(node, huge_choices, choices_lock)
                            completed.add(node.tx_id)
                            committed_now.append(node.tx_id)
                            if node.is_barrier:
                                barriers_executed += 1
                        except Exception as exc:
                            aborted.add(node.tx_id)
                            if first_error is None:
                                first_error = exc
                else:
                    idx = 0
                    while idx < len(pending_wave):
                        batch = pending_wave[idx:idx + workers]
                        threads: List[threading.Thread] = []
                        errors: List[Tuple[Union[int, str], Exception]] = []
                        err_lock = threading.Lock()

                        def _run_one(target_node: TransactionNode) -> None:
                            try:
                                self._execute_node(target_node, huge_choices, choices_lock)
                                with err_lock:
                                    completed.add(target_node.tx_id)
                                    committed_now.append(target_node.tx_id)
                            except Exception as e:
                                with err_lock:
                                    aborted.add(target_node.tx_id)
                                    errors.append((target_node.tx_id, e))

                        for b_node in batch:
                            t = threading.Thread(target=_run_one, args=(b_node,), daemon=True)
                            threads.append(t)
                            t.start()
                        for t in threads:
                            t.join()

                        if errors and first_error is None:
                            first_error = errors[0][1]
                        idx += workers

                if first_error is not None and reorder_choice.path == "wave_barrier_commit":
                    break

        if first_error is not None:
            raise first_error

        holes = self.checkpoint.hole_lsns()
        badge = CareBadge.PASS if not holes and not aborted else CareBadge.ACTION
        return DAGExecutionReport(
            care_badge=badge,
            reorder_choice=reorder_choice,
            huge_tx_choices=huge_choices,
            pace_choices=list(self.telemetry.decisions),
            committed_tx_ids=committed_now,
            skipped_tx_ids=skipped,
            aborted_tx_ids=sorted(aborted, key=str),
            barriers_executed=barriers_executed,
            contiguous_committed_lsn=self.checkpoint.contiguous_committed_lsn,
            hole_lsns=holes,
            final_workers=self.telemetry.current_workers,
            edges_count=len(dag.edges),
        )
