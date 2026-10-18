import hashlib
import io
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


from . import decide


def lob_execution_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for LOB transaction execution path selection (Row 659: NEED11)."""
    return (
        decide.Rung(
            "inline_lob_write",
            "write LOB inline in single insert/update statement within adapter byte and memory budgets",
            needs=(
                decide.Need(
                    "within inline byte limit",
                    lambda f: f.get("lob_bytes", 0) <= f.get("max_inline_bytes", 65536),
                    "lob size exceeds inline limit",
                ),
                decide.Need(
                    "within memory budget",
                    lambda f: f.get("lob_bytes", 0) <= f.get("memory_budget", 67108864),
                    "lob size exceeds coordinator memory budget",
                ),
                decide.Need(
                    "not null constraint satisfied",
                    lambda f: bool(f.get("not_null_satisfied", True)),
                    "empty or null value violates not null constraint",
                ),
            ),
            footprint=0,
            leaves="inline LOB write in single statement",
        ),
        decide.Rung(
            "chunked_stream_write",
            "write LOB in streaming chunks protected by transactional savepoints",
            needs=(
                decide.Need(
                    "streaming supported",
                    lambda f: bool(f.get("supports_streaming", True)),
                    "target adapter does not support streaming LOB writes",
                ),
                decide.Need(
                    "savepoints supported",
                    lambda f: bool(f.get("supports_savepoints", True)),
                    "target adapter does not support transactional savepoints",
                ),
                decide.Need(
                    "within stream transaction budget",
                    lambda f: bool(f.get("within_transaction_budget", True)),
                    "exceeds stream transaction budget",
                ),
            ),
            footprint=1,
            leaves="chunked stream write with transactional savepoints",
        ),
        decide.Rung(
            "delayed_fetch_from_locator",
            "retain snapshot locator and fetch LOB on demand during target apply",
            needs=(
                decide.Need(
                    "source retains snapshot locators",
                    lambda f: bool(f.get("retains_snapshot_locators", True)),
                    "source adapter does not retain snapshot locators across transactions",
                ),
                decide.Need(
                    "active transaction identity",
                    lambda f: bool(f.get("tx_identity_active", True)),
                    "source transaction identity is inactive or expired",
                ),
                decide.Need(
                    "no concurrent row mutation",
                    lambda f: not bool(f.get("concurrent_mutation", False)),
                    "source row mutated during delayed fetch window",
                ),
            ),
            footprint=2,
            leaves="delayed fetch from source snapshot locator",
        ),
        decide.Rung(
            "stage_to_spill_storage",
            "stage oversized LOB chunk to temporary spool storage before applying",
            needs=(
                decide.Need(
                    "spill storage available",
                    lambda f: bool(f.get("spill_storage_available", True)),
                    "auxiliary spill storage not available",
                ),
                decide.Need(
                    "spill policy permits buffering",
                    lambda f: bool(f.get("spill_permitted", True)),
                    "spill policy prohibits disk buffering",
                ),
            ),
            footprint=3,
            leaves="stage oversized LOB chunk to temporary spool storage",
        ),
        decide.Rung(
            "refuse_lob_write",
            "refuse LOB write to prevent truncation or constraint corruption",
            footprint=4,
            leaves="refuse LOB write to prevent truncation or constraint corruption",
        ),
    )


def decide_lob_execution(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the lob_execution_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("lob transaction execution", lob_execution_rungs(), fact_obj)


class LobTransactionError(Exception):
    pass


class NotNullConstraintViolation(LobTransactionError):
    pass


class StaleTransactionIdentityError(LobTransactionError):
    pass


class ConcurrentLobMutationError(LobTransactionError):
    pass


class LobPathRefusedError(LobTransactionError):
    pass


class ExecutionPath(str, Enum):
    INLINE = "inline"
    STREAMED = "streamed"
    DELAYED_FETCH = "delayed_fetch"
    REFUSED = "refused"


class CompensationStatus(str, Enum):
    PENDING = "pending"
    ROLLED_BACK = "rolled_back"
    COMPENSATED = "compensated"
    FAILED = "failed"


@dataclass
class CompensationRecord:
    table: str
    key: Dict[str, Any]
    status: CompensationStatus
    step_failed: str
    error_message: str
    recoverable: bool = True
    chunks_written: int = 0
    bytes_written: int = 0
    created_at: float = field(default_factory=time.time)


class LobStreamTransactionError(LobTransactionError):
    def __init__(
        self,
        message: str,
        compensation_record: Optional[CompensationRecord] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.compensation_record = compensation_record
        self.original_error = original_error


@dataclass
class AdapterLobCapability:
    engine_name: str
    eager_materialize: bool = False
    max_inline_bytes: int = 64 * 1024
    supports_streaming: bool = True
    supports_savepoints: bool = True
    empty_string_is_null: bool = False
    retains_snapshot_locators: bool = True


@dataclass
class ColumnConstraint:
    name: str
    data_type: str = "clob"
    nullable: bool = True
    default_value: Optional[Any] = None
    check_constraints: Tuple[str, ...] = ()


@dataclass
class TransactionIdentity:
    tx_id: str
    snapshot_id: Optional[str] = None
    scn_or_lsn: Optional[int] = None
    version_hash: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    row_key: Any = None
    is_active: bool = True


@dataclass
class DelayedFetchHandle:
    identity: TransactionIdentity
    column_name: str
    fetch_fn: Callable[[TransactionIdentity], Union[str, bytes, io.IOBase]]
    mutation_detector: Optional[Callable[[TransactionIdentity], bool]] = None


@dataclass
class PathPrice:
    path: ExecutionPath
    estimated_cost: float
    round_trips: int
    peak_memory_bytes: int
    is_feasible: bool = True
    refusal_reason: Optional[str] = None


@dataclass
class LobPricing:
    recommended_path: ExecutionPath
    prices: Dict[ExecutionPath, PathPrice]


@dataclass
class WriteReceipt:
    table: str
    key: Dict[str, Any]
    column: str
    path: ExecutionPath
    bytes_written: int
    chunks_count: int
    committed: bool = True
    digest: Optional[str] = None


def probe_adapter_lob_capability(
    engine_or_adapter: Any,
    max_inline_bytes: Optional[int] = None,
) -> AdapterLobCapability:
    name = (
        engine_or_adapter
        if isinstance(engine_or_adapter, str)
        else getattr(engine_or_adapter, "ENGINE_FAMILY", getattr(engine_or_adapter, "engine", "generic"))
    ).lower()

    explicit_inline = max_inline_bytes or getattr(engine_or_adapter, "max_inline_bytes", None)

    if "oracle" in name:
        return AdapterLobCapability(
            engine_name="oracle",
            eager_materialize=False,
            max_inline_bytes=explicit_inline or 4000,
            supports_streaming=True,
            supports_savepoints=True,
            empty_string_is_null=True,
            retains_snapshot_locators=True,
        )
    if "postgres" in name:
        return AdapterLobCapability(
            engine_name="postgres",
            eager_materialize=False,
            max_inline_bytes=explicit_inline or (1024 * 1024),
            supports_streaming=True,
            supports_savepoints=True,
            empty_string_is_null=False,
            retains_snapshot_locators=True,
        )
    if "mysql" in name or "mariadb" in name or "tidb" in name:
        return AdapterLobCapability(
            engine_name="mysql",
            eager_materialize=False,
            max_inline_bytes=explicit_inline or (1024 * 1024),
            supports_streaming=True,
            supports_savepoints=True,
            empty_string_is_null=False,
            retains_snapshot_locators=False,
        )
    if "sqlite" in name:
        return AdapterLobCapability(
            engine_name="sqlite",
            eager_materialize=False,
            max_inline_bytes=explicit_inline or (1024 * 1024),
            supports_streaming=True,
            supports_savepoints=True,
            empty_string_is_null=False,
            retains_snapshot_locators=True,
        )
    return AdapterLobCapability(
        engine_name=name,
        eager_materialize=True,
        max_inline_bytes=explicit_inline or (32 * 1024),
        supports_streaming=False,
        supports_savepoints=True,
        empty_string_is_null=False,
        retains_snapshot_locators=False,
    )


class LOBTransactionCoordinator:
    def __init__(
        self,
        adapter_capability: AdapterLobCapability,
        memory_budget: int = 64 * 1024 * 1024,
        chunk_size: int = 64 * 1024,
    ):
        self.capability = adapter_capability
        self.memory_budget = max(1024, int(memory_budget))
        self.chunk_size = max(512, int(chunk_size))

    def price_paths(
        self,
        lob_size_bytes: int,
        constraint: ColumnConstraint,
        source_cap: Optional[AdapterLobCapability] = None,
    ) -> LobPricing:
        facts = {
            "lob_bytes": lob_size_bytes,
            "max_inline_bytes": self.capability.max_inline_bytes,
            "memory_budget": self.memory_budget,
            "not_null_satisfied": not (not constraint.nullable and (lob_size_bytes == 0 and self.capability.empty_string_is_null)),
            "supports_streaming": self.capability.supports_streaming,
            "supports_savepoints": self.capability.supports_savepoints,
            "within_transaction_budget": lob_size_bytes <= (self.memory_budget * 64),
            "retains_snapshot_locators": source_cap.retains_snapshot_locators if source_cap else self.capability.retains_snapshot_locators,
            "tx_identity_active": True,
            "concurrent_mutation": False,
            "spill_storage_available": True,
            "spill_permitted": True,
        }
        choice = decide_lob_execution(facts)

        prices: Dict[ExecutionPath, PathPrice] = {}

        can_inline = (
            lob_size_bytes <= self.capability.max_inline_bytes
            and lob_size_bytes <= self.memory_budget
        )
        if can_inline:
            inline_cost = 1.0 + (lob_size_bytes / max(1, self.capability.max_inline_bytes))
            prices[ExecutionPath.INLINE] = PathPrice(
                path=ExecutionPath.INLINE,
                estimated_cost=inline_cost,
                round_trips=1,
                peak_memory_bytes=lob_size_bytes,
                is_feasible=True,
            )
        else:
            reason = "LOB size exceeds max inline threshold" if lob_size_bytes > self.capability.max_inline_bytes else "LOB size exceeds memory budget"
            prices[ExecutionPath.INLINE] = PathPrice(
                path=ExecutionPath.INLINE,
                estimated_cost=float("inf"),
                round_trips=1,
                peak_memory_bytes=lob_size_bytes,
                is_feasible=False,
                refusal_reason=reason,
            )

        if self.capability.supports_streaming:
            chunks = max(1, math.ceil(lob_size_bytes / self.chunk_size))
            stream_memory = min(lob_size_bytes, self.chunk_size)
            stream_cost = 2.0 + (chunks * 0.5) + (lob_size_bytes / max(1, self.capability.max_inline_bytes))
            prices[ExecutionPath.STREAMED] = PathPrice(
                path=ExecutionPath.STREAMED,
                estimated_cost=stream_cost,
                round_trips=chunks,
                peak_memory_bytes=stream_memory,
                is_feasible=True,
            )
        else:
            prices[ExecutionPath.STREAMED] = PathPrice(
                path=ExecutionPath.STREAMED,
                estimated_cost=float("inf"),
                round_trips=0,
                peak_memory_bytes=0,
                is_feasible=False,
                refusal_reason="Adapter does not support streaming LOB writes",
            )

        delayed_source_ok = source_cap.retains_snapshot_locators if source_cap else True
        if delayed_source_ok and (can_inline or self.capability.supports_streaming):
            base = prices[ExecutionPath.INLINE] if can_inline else prices[ExecutionPath.STREAMED]
            prices[ExecutionPath.DELAYED_FETCH] = PathPrice(
                path=ExecutionPath.DELAYED_FETCH,
                estimated_cost=base.estimated_cost + 1.5,
                round_trips=base.round_trips + 1,
                peak_memory_bytes=base.peak_memory_bytes,
                is_feasible=True,
            )
        else:
            prices[ExecutionPath.DELAYED_FETCH] = PathPrice(
                path=ExecutionPath.DELAYED_FETCH,
                estimated_cost=float("inf"),
                round_trips=0,
                peak_memory_bytes=0,
                is_feasible=False,
                refusal_reason="Source does not retain snapshot locators or target cannot apply",
            )

        if choice.rung.name == "inline_lob_write" and prices[ExecutionPath.INLINE].is_feasible:
            recommended = ExecutionPath.INLINE
        elif choice.rung.name == "chunked_stream_write" and prices[ExecutionPath.STREAMED].is_feasible:
            recommended = ExecutionPath.STREAMED
        elif choice.rung.name == "delayed_fetch_from_locator" and prices[ExecutionPath.DELAYED_FETCH].is_feasible:
            recommended = ExecutionPath.DELAYED_FETCH
        else:
            feasible_paths = [p for p in (ExecutionPath.INLINE, ExecutionPath.STREAMED) if prices[p].is_feasible]
            if feasible_paths:
                recommended = min(feasible_paths, key=lambda p: prices[p].estimated_cost)
            else:
                recommended = ExecutionPath.REFUSED
                refusal_msg = prices[ExecutionPath.STREAMED].refusal_reason or prices[ExecutionPath.INLINE].refusal_reason or "No feasible path under adapter capabilities"
                prices[ExecutionPath.REFUSED] = PathPrice(
                    path=ExecutionPath.REFUSED,
                    estimated_cost=float("inf"),
                    round_trips=0,
                    peak_memory_bytes=0,
                    is_feasible=False,
                    refusal_reason=refusal_msg,
                )

        return LobPricing(recommended_path=recommended, prices=prices)

    def coordinate_write(
        self,
        target_conn: Any,
        table: str,
        key: Dict[str, Any],
        column: str,
        lob_value: Union[str, bytes, io.IOBase, DelayedFetchHandle, None],
        constraint: ColumnConstraint,
        other_cols: Optional[Dict[str, Any]] = None,
        relax_not_null: bool = False,
    ) -> WriteReceipt:
        if relax_not_null:
            raise NotNullConstraintViolation("Relaxing NOT NULL silently is prohibited")

        resolved_value = lob_value
        if isinstance(lob_value, DelayedFetchHandle):
            handle = lob_value
            if not handle.identity.is_active:
                raise StaleTransactionIdentityError(
                    f"Transaction identity {handle.identity.snapshot_id or handle.identity.tx_id} is inactive"
                )
            if handle.mutation_detector and handle.mutation_detector(handle.identity):
                raise ConcurrentLobMutationError(
                    f"Source row {key} mutated during delayed LOB fetch for {table}.{column}"
                )
            resolved_value = handle.fetch_fn(handle.identity)

        if resolved_value is None:
            if not constraint.nullable:
                raise NotNullConstraintViolation(f"Column {column} is NOT NULL; cannot write NULL")
            return self._write_inline(target_conn, table, key, column, None, other_cols)

        if resolved_value == "" or resolved_value == b"":
            if not constraint.nullable and self.capability.empty_string_is_null:
                raise NotNullConstraintViolation(
                    f"Target engine {self.capability.engine_name} treats empty string as NULL on NOT NULL column {column}"
                )
            return self._write_inline(target_conn, table, key, column, resolved_value, other_cols)

        if isinstance(resolved_value, (str, bytes, bytearray, memoryview)):
            raw_bytes = (
                resolved_value.encode("utf-8")
                if isinstance(resolved_value, str)
                else bytes(resolved_value)
            )
            pricing = self.price_paths(len(raw_bytes), constraint)
            if pricing.recommended_path == ExecutionPath.INLINE:
                return self._write_inline(
                    target_conn, table, key, column, resolved_value, other_cols
                )
            if pricing.recommended_path == ExecutionPath.STREAMED:
                stream = io.BytesIO(raw_bytes) if isinstance(resolved_value, (bytes, bytearray, memoryview)) else io.StringIO(resolved_value)
                return self._write_streamed(
                    target_conn, table, key, column, stream, constraint, other_cols
                )
            raise LobPathRefusedError(
                pricing.prices[ExecutionPath.REFUSED].refusal_reason or "Write refused"
            )

        if isinstance(resolved_value, io.IOBase) or hasattr(resolved_value, "read"):
            return self._write_streamed(
                target_conn, table, key, column, resolved_value, constraint, other_cols
            )

        raise ValueError(f"Unsupported LOB value type: {type(resolved_value)}")

    def _write_inline(
        self,
        conn: Any,
        table: str,
        key: Dict[str, Any],
        column: str,
        value: Any,
        other_cols: Optional[Dict[str, Any]],
    ) -> WriteReceipt:
        cols = {**key, **(other_cols or {}), column: value}
        col_names = list(cols.keys())
        marks = ", ".join(["?"] * len(col_names))
        sql = f"INSERT INTO {table} ({', '.join(col_names)}) VALUES ({marks})"
        conn.execute(sql, list(cols.values()))
        if hasattr(conn, "commit"):
            conn.commit()

        raw = b""
        if value is not None:
            raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        digest = hashlib.md5(raw, usedforsecurity=False).hexdigest()
        return WriteReceipt(
            table=table,
            key=key,
            column=column,
            path=ExecutionPath.INLINE,
            bytes_written=len(raw),
            chunks_count=1,
            committed=True,
            digest=digest,
        )

    def _write_streamed(
        self,
        conn: Any,
        table: str,
        key: Dict[str, Any],
        column: str,
        stream: Any,
        constraint: ColumnConstraint,
        other_cols: Optional[Dict[str, Any]],
    ) -> WriteReceipt:
        sp_name = f"lob_tx_{int(time.time() * 1000) % 1000000}"
        savepoint_active = False

        if self.capability.supports_savepoints:
            try:
                conn.execute(f"SAVEPOINT {sp_name}")
                savepoint_active = True
            except Exception as exc:
                rec = CompensationRecord(
                    table=table,
                    key=key,
                    status=CompensationStatus.FAILED,
                    step_failed="savepoint_create",
                    error_message=str(exc),
                    recoverable=False,
                )
                raise LobStreamTransactionError(
                    f"Savepoint creation failed for {table}.{column}: {exc}",
                    compensation_record=rec,
                    original_error=exc,
                )

        first_chunk = stream.read(self.chunk_size)
        is_text = isinstance(first_chunk, str)

        if first_chunk is None or (len(first_chunk) == 0 and not constraint.nullable):
            if not constraint.nullable:
                if savepoint_active:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                    conn.execute(f"RELEASE SAVEPOINT {sp_name}")
                raise NotNullConstraintViolation(f"Column {column} is NOT NULL; stream cannot be empty")

        cols = {**key, **(other_cols or {}), column: first_chunk}
        col_names = list(cols.keys())
        marks = ", ".join(["?"] * len(col_names))
        insert_sql = f"INSERT INTO {table} ({', '.join(col_names)}) VALUES ({marks})"

        try:
            conn.execute(insert_sql, list(cols.values()))
        except Exception as exc:
            if savepoint_active:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            rec = CompensationRecord(
                table=table,
                key=key,
                status=CompensationStatus.ROLLED_BACK if savepoint_active else CompensationStatus.FAILED,
                step_failed="row_insert",
                error_message=str(exc),
                recoverable=True,
            )
            raise LobStreamTransactionError(
                f"Row insert failed for {table}.{column}: {exc}",
                compensation_record=rec,
                original_error=exc,
            )

        hasher = hashlib.md5(usedforsecurity=False)
        raw_first = first_chunk.encode("utf-8") if isinstance(first_chunk, str) else bytes(first_chunk or b"")
        hasher.update(raw_first)
        total_bytes = len(raw_first)
        chunks_count = 1

        pk_clauses = " AND ".join([f"{k} = ?" for k in key.keys()])
        pk_values = list(key.values())
        if self.capability.engine_name == "sqlite" and not is_text:
            update_sql = f"UPDATE {table} SET {column} = CAST({column} || ? AS BLOB) WHERE {pk_clauses}"
        else:
            update_sql = f"UPDATE {table} SET {column} = {column} || ? WHERE {pk_clauses}"

        try:
            while True:
                chunk = stream.read(self.chunk_size)
                if not chunk:
                    break
                conn.execute(update_sql, [chunk, *pk_values])
                raw_chunk = chunk.encode("utf-8") if isinstance(chunk, str) else bytes(chunk)
                hasher.update(raw_chunk)
                total_bytes += len(raw_chunk)
                chunks_count += 1
        except Exception as exc:
            comp_status = CompensationStatus.FAILED
            if savepoint_active:
                try:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                    conn.execute(f"RELEASE SAVEPOINT {sp_name}")
                    comp_status = CompensationStatus.ROLLED_BACK
                except Exception:
                    comp_status = CompensationStatus.FAILED

            if comp_status != CompensationStatus.ROLLED_BACK:
                try:
                    delete_sql = f"DELETE FROM {table} WHERE {pk_clauses}"
                    conn.execute(delete_sql, pk_values)
                    if hasattr(conn, "commit"):
                        conn.commit()
                    comp_status = CompensationStatus.COMPENSATED
                except Exception:
                    comp_status = CompensationStatus.FAILED

            rec = CompensationRecord(
                table=table,
                key=key,
                status=comp_status,
                step_failed="stream_chunk",
                error_message=str(exc),
                recoverable=True,
                chunks_written=chunks_count,
                bytes_written=total_bytes,
            )
            raise LobStreamTransactionError(
                f"LOB stream chunk failed for {table}.{column}: {exc}",
                compensation_record=rec,
                original_error=exc,
            )

        if savepoint_active:
            conn.execute(f"RELEASE SAVEPOINT {sp_name}")
        if hasattr(conn, "commit"):
            conn.commit()

        return WriteReceipt(
            table=table,
            key=key,
            column=column,
            path=ExecutionPath.STREAMED,
            bytes_written=total_bytes,
            chunks_count=chunks_count,
            committed=True,
            digest=hasher.hexdigest(),
        )
