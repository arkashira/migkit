import errno
import json
import os
import re
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from . import decide
from . import lease


class SnapshotTxState(str, Enum):
    INITIALIZING = "INITIALIZING"
    ACTIVE = "ACTIVE"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"


def cancel_recovery_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for recovering invalid or cancelled snapshot transactions (Row 651: NEED03)."""
    return (
        decide.Rung(
            "commit_active_transaction",
            "commit cleanly when transaction is active, heartbeats valid, and no cancellation requested",
            needs=(
                decide.Need(
                    "transaction is valid and active",
                    lambda f: bool(f.get("is_active", True)),
                    "transaction is not in active state",
                ),
                decide.Need(
                    "no cancellation requested",
                    lambda f: not bool(f.get("cancel_requested", False)),
                    "cancellation was requested for this transaction",
                ),
                decide.Need(
                    "lease heartbeat is active",
                    lambda f: not bool(f.get("lease_expired", False)),
                    "lease heartbeat has expired",
                ),
            ),
            footprint=0,
            leaves="committed snapshot transaction with staged work",
        ),
        decide.Rung(
            "rollback_owned_work_and_cancel",
            "roll back owned work chunks non-destructively and transition to cancelled state",
            needs=(
                decide.Need(
                    "owned work chunks can be rolled back cleanly",
                    lambda f: bool(f.get("can_rollback", True)),
                    "work chunks cannot be safely rolled back",
                ),
            ),
            footprint=1,
            leaves="clean non-destructive rollback of owned work",
        ),
        decide.Rung(
            "quarantine_and_recover_stranded",
            "quarantine stranded or orphaned process transaction and roll back owned work",
            footprint=2,
            leaves="quarantined failed transaction with diagnostics recorded",
        ),
    )


def decide_cancel_recovery(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the cancel_recovery_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("snapshot cancel recovery", cancel_recovery_rungs(), fact_obj)


class TxValidationError(ValueError):
    pass


class TxStateError(RuntimeError):
    pass


@dataclass
class DiagnosticEvidence:
    tx_id: str
    timestamp: float
    phase: str
    error_code: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DiagnosticEvidence":
        return cls(
            tx_id=data["tx_id"],
            timestamp=float(data["timestamp"]),
            phase=data["phase"],
            error_code=data["error_code"],
            message=data["message"],
            details=dict(data.get("details") or {}),
        )


@dataclass
class SnapshotTx:
    tx_id: str
    table_name: str
    keys: Tuple[str, ...]
    filter_expr: Optional[str] = None
    state: SnapshotTxState = SnapshotTxState.INITIALIZING
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    owner_pid: Optional[int] = None
    heartbeat_at: float = field(default_factory=time.time)
    lease_timeout: float = field(default_factory=lambda: float(lease.term()))
    metadata: Dict[str, Any] = field(default_factory=dict)
    owned_chunks: Set[str] = field(default_factory=set)
    staged_records: Dict[str, List[Any]] = field(default_factory=dict)
    diagnostics: List[DiagnosticEvidence] = field(default_factory=list)

    def is_terminal(self) -> bool:
        return self.state in {
            SnapshotTxState.COMMITTED,
            SnapshotTxState.CANCELLED,
            SnapshotTxState.FAILED,
            SnapshotTxState.ROLLED_BACK,
        }

    def is_active(self) -> bool:
        return self.state == SnapshotTxState.ACTIVE

    def is_initializing(self) -> bool:
        return self.state == SnapshotTxState.INITIALIZING

    def record_heartbeat(self, timestamp: Optional[float] = None) -> None:
        now = time.time() if timestamp is None else timestamp
        self.heartbeat_at = now
        self.updated_at = now

    def is_expired(self, now: Optional[float] = None) -> bool:
        current = time.time() if now is None else now
        return (current - self.heartbeat_at) > self.lease_timeout

    def record_diagnostic(
        self, phase: str, error_code: str, message: str, details: Optional[Dict[str, Any]] = None
    ) -> DiagnosticEvidence:
        diag = DiagnosticEvidence(
            tx_id=self.tx_id,
            timestamp=time.time(),
            phase=phase,
            error_code=error_code,
            message=message,
            details=details or {},
        )
        self.diagnostics.append(diag)
        self.updated_at = time.time()
        return diag

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tx_id": self.tx_id,
            "table_name": self.table_name,
            "keys": list(self.keys),
            "filter_expr": self.filter_expr,
            "state": self.state.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "owner_pid": self.owner_pid,
            "heartbeat_at": self.heartbeat_at,
            "lease_timeout": self.lease_timeout,
            "metadata": self.metadata,
            "owned_chunks": sorted(list(self.owned_chunks)),
            "staged_records": self.staged_records,
            "diagnostics": [d.to_dict() for d in self.diagnostics],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SnapshotTx":
        return cls(
            tx_id=data["tx_id"],
            table_name=data["table_name"],
            keys=tuple(data.get("keys") or ()),
            filter_expr=data.get("filter_expr"),
            state=SnapshotTxState(data["state"]),
            created_at=float(data["created_at"]),
            updated_at=float(data["updated_at"]),
            owner_pid=data.get("owner_pid"),
            heartbeat_at=float(data.get("heartbeat_at", data["created_at"])),
            lease_timeout=float(data["lease_timeout"]) if data.get("lease_timeout") is not None else float(lease.term()),
            metadata=dict(data.get("metadata") or {}),
            owned_chunks=set(data.get("owned_chunks") or ()),
            staged_records=dict(data.get("staged_records") or {}),
            diagnostics=[DiagnosticEvidence.from_dict(d) for d in data.get("diagnostics") or ()],
        )


def _is_pid_alive(pid: Optional[int]) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as err:
        return err.errno == errno.EPERM
    return True


_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$")
_FORBIDDEN_KEYWORDS = re.compile(r"\b(DROP|TRUNCATE|DELETE|ALTER|UPDATE|INSERT|GRANT|REVOKE)\b", re.IGNORECASE)


class SnapshotTxStore:
    def __init__(
        self,
        storage_path: Optional[Path | str] = None,
        schema_catalog: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.storage_path = Path(storage_path) if storage_path else None
        self.schema_catalog = schema_catalog or {}
        self._transactions: Dict[str, SnapshotTx] = {}
        self._global_stream_offsets: Dict[str, Any] = {}
        self._diagnostics: List[DiagnosticEvidence] = []
        self._lock = threading.Lock()

    def get_global_offsets(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._global_stream_offsets)

    def set_global_offsets(self, offsets: Dict[str, Any]) -> None:
        with self._lock:
            self._global_stream_offsets = dict(offsets)

    def get_tx(self, tx_id: str) -> Optional[SnapshotTx]:
        with self._lock:
            return self._transactions.get(tx_id)

    def list_active(self) -> List[SnapshotTx]:
        with self._lock:
            return [tx for tx in self._transactions.values() if tx.state == SnapshotTxState.ACTIVE]

    def get_diagnostics(
        self, tx_id: Optional[str] = None, phase: Optional[str] = None
    ) -> List[DiagnosticEvidence]:
        with self._lock:
            res: List[DiagnosticEvidence] = []
            if tx_id:
                tx = self._transactions.get(tx_id)
                if tx:
                    res.extend(tx.diagnostics)
            else:
                res.extend(self._diagnostics)
                for tx in self._transactions.values():
                    for d in tx.diagnostics:
                        if d not in res:
                            res.append(d)

            if phase:
                res = [d for d in res if d.phase == phase]
            return res

    def begin_tx(
        self,
        tx_id: str,
        table_name: str,
        keys: Tuple[str, ...],
        filter_expr: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        owner_pid: Optional[int] = None,
        lease_timeout: Optional[float] = None,
    ) -> SnapshotTx:
        if not keys or not isinstance(keys, (tuple, list)) or len(keys) == 0:
            raise TxValidationError(f"Invalid keys for snapshot transaction {tx_id}: keys cannot be empty")

        with self._lock:
            if tx_id in self._transactions and not self._transactions[tx_id].is_terminal():
                raise TxStateError(f"Transaction {tx_id} already exists in active or in-flight state")

            pid = owner_pid if owner_pid is not None else os.getpid()
            effective_timeout = float(lease_timeout) if lease_timeout is not None else float(lease.term())
            tx = SnapshotTx(
                tx_id=tx_id,
                table_name=table_name,
                keys=tuple(keys),
                filter_expr=filter_expr,
                state=SnapshotTxState.INITIALIZING,
                owner_pid=pid,
                lease_timeout=effective_timeout,
                metadata=metadata or {},
            )
            self._transactions[tx_id] = tx
            return tx

    def validate_tx(self, tx_id: str) -> None:
        tx = self._transactions.get(tx_id)
        if not tx:
            raise KeyError(f"Transaction {tx_id} not found")

        if not tx.table_name or not isinstance(tx.table_name, str):
            diag = tx.record_diagnostic("INITIALIZATION", "INVALID_METADATA", "Invalid table name")
            self._diagnostics.append(diag)
            tx.state = SnapshotTxState.FAILED
            raise TxValidationError("Table name must be a non-empty string")

        if not _IDENTIFIER_PATTERN.match(tx.table_name):
            diag = tx.record_diagnostic("INITIALIZATION", "INVALID_METADATA", f"Invalid table identifier: {tx.table_name}")
            self._diagnostics.append(diag)
            tx.state = SnapshotTxState.FAILED
            raise TxValidationError(f"Invalid table identifier: {tx.table_name}")

        if self.schema_catalog and tx.table_name not in self.schema_catalog:
            diag = tx.record_diagnostic(
                "INITIALIZATION",
                "INVALID_TABLE",
                f"Table {tx.table_name} does not exist in schema catalog",
                {"table": tx.table_name},
            )
            self._diagnostics.append(diag)
            tx.state = SnapshotTxState.FAILED
            raise TxValidationError(f"Table {tx.table_name} does not exist in schema catalog")

        if not tx.keys:
            diag = tx.record_diagnostic("INITIALIZATION", "INVALID_KEY", "Keys cannot be empty")
            self._diagnostics.append(diag)
            tx.state = SnapshotTxState.FAILED
            raise TxValidationError("Keys cannot be empty")

        if self.schema_catalog and tx.table_name in self.schema_catalog:
            tbl_info = self.schema_catalog[tx.table_name]
            known_cols = set(tbl_info.get("columns", {}).keys()) | set(tbl_info.get("pks", ()))
            for k in tx.keys:
                if k not in known_cols:
                    diag = tx.record_diagnostic(
                        "INITIALIZATION",
                        "INVALID_KEY",
                        f"Key column '{k}' not found in schema for table '{tx.table_name}'",
                        {"key": k, "table": tx.table_name},
                    )
                    self._diagnostics.append(diag)
                    tx.state = SnapshotTxState.FAILED
                    raise TxValidationError(f"Key column '{k}' not found in table schema")

        if tx.filter_expr is not None:
            f = tx.filter_expr.strip()
            if _FORBIDDEN_KEYWORDS.search(f) or ";" in f:
                diag = tx.record_diagnostic("INITIALIZATION", "FILTER_SYNTAX_ERROR", "Disallowed SQL statement in filter")
                self._diagnostics.append(diag)
                tx.state = SnapshotTxState.FAILED
                raise TxValidationError("Disallowed SQL statement or statement terminator in filter")

            if f.count("(") != f.count(")"):
                diag = tx.record_diagnostic("INITIALIZATION", "FILTER_SYNTAX_ERROR", "Unbalanced parentheses in filter expression")
                self._diagnostics.append(diag)
                tx.state = SnapshotTxState.FAILED
                raise TxValidationError("Unbalanced parentheses in filter expression")

            if f.count("'") % 2 != 0 or f.count('"') % 2 != 0:
                diag = tx.record_diagnostic("INITIALIZATION", "FILTER_SYNTAX_ERROR", "Unbalanced quotes in filter expression")
                self._diagnostics.append(diag)
                tx.state = SnapshotTxState.FAILED
                raise TxValidationError("Unbalanced quotes in filter expression")

    def publish_tx(self, tx_id: str) -> SnapshotTx:
        with self._lock:
            tx = self._transactions.get(tx_id)
            if not tx:
                raise KeyError(f"Transaction {tx_id} not found")
            if tx.state != SnapshotTxState.INITIALIZING:
                raise TxStateError(f"Cannot publish transaction {tx_id} in state {tx.state.value}")

            self.validate_tx(tx_id)
            tx.state = SnapshotTxState.ACTIVE
            tx.updated_at = time.time()
            return tx

    def stage_chunk(self, tx_id: str, chunk_id: str, records: Optional[List[Any]] = None) -> None:
        with self._lock:
            tx = self._transactions.get(tx_id)
            if not tx:
                raise KeyError(f"Transaction {tx_id} not found")
            if tx.state != SnapshotTxState.ACTIVE:
                raise TxStateError(f"Cannot stage chunk for transaction {tx_id} in state {tx.state.value}")

            tx.owned_chunks.add(chunk_id)
            tx.staged_records[chunk_id] = list(records or [])
            tx.updated_at = time.time()

    def evaluate_filter_on_chunk(
        self, tx_id: str, chunk_id: str, filter_fn: Callable[[Any], bool]
    ) -> List[Any]:
        with self._lock:
            tx = self._transactions.get(tx_id)
            if not tx:
                raise KeyError(f"Transaction {tx_id} not found")
            if tx.state != SnapshotTxState.ACTIVE:
                raise TxStateError(f"Cannot evaluate filter on transaction {tx_id} in state {tx.state.value}")
            records = list(tx.staged_records.get(chunk_id, []))

        passed: List[Any] = []
        try:
            for row in records:
                if filter_fn(row):
                    passed.append(row)
        except Exception as err:
            with self._lock:
                diag = tx.record_diagnostic(
                    phase="FILTER_EVALUATION",
                    error_code="FILTER_EXECUTION_FAILURE",
                    message=f"Runtime error during filter evaluation: {type(err).__name__}: {str(err)}",
                    details={"chunk_id": chunk_id, "error_type": type(err).__name__},
                )
                self._diagnostics.append(diag)
                self.rollback_owned_work(tx_id)
                tx.state = SnapshotTxState.FAILED
            raise

        with self._lock:
            tx.staged_records[chunk_id] = passed
            tx.updated_at = time.time()
            return passed

    def commit_tx(self, tx_id: str) -> SnapshotTx:
        with self._lock:
            tx = self._transactions.get(tx_id)
            if not tx:
                raise KeyError(f"Transaction {tx_id} not found")
            if tx.state != SnapshotTxState.ACTIVE:
                raise TxStateError(f"Cannot commit transaction {tx_id} in state {tx.state.value}")

            facts = {
                "is_active": tx.is_active(),
                "cancel_requested": False,
                "lease_expired": tx.is_expired(),
                "can_rollback": True,
            }
            decide_cancel_recovery(facts)

            tx.state = SnapshotTxState.COMMITTING
            tx.state = SnapshotTxState.COMMITTED
            tx.updated_at = time.time()
            return tx

    def cancel_tx(self, tx_id: str, reason: str = "") -> SnapshotTx:
        with self._lock:
            tx = self._transactions.get(tx_id)
            if not tx:
                raise KeyError(f"Transaction {tx_id} not found")
            if tx.state == SnapshotTxState.COMMITTED:
                raise TxStateError(f"Cannot cancel already committed transaction {tx_id}")
            if tx.state in {SnapshotTxState.CANCELLED, SnapshotTxState.FAILED}:
                return tx

            facts = {
                "is_active": tx.is_active(),
                "cancel_requested": True,
                "lease_expired": tx.is_expired(),
                "can_rollback": True,
            }
            decide_cancel_recovery(facts)

            diag = tx.record_diagnostic(
                phase="CANCELLATION",
                error_code="CLIENT_CANCELLED",
                message=reason or "Snapshot transaction cancelled",
                details={"prior_state": tx.state.value},
            )
            self._diagnostics.append(diag)
            self.rollback_owned_work(tx_id)
            tx.state = SnapshotTxState.CANCELLED
            tx.updated_at = time.time()
            return tx

    def fail_tx(
        self,
        tx_id: str,
        phase: str,
        error_code: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> SnapshotTx:
        with self._lock:
            tx = self._transactions.get(tx_id)
            if not tx:
                raise KeyError(f"Transaction {tx_id} not found")
            if tx.state == SnapshotTxState.COMMITTED:
                raise TxStateError(f"Cannot fail already committed transaction {tx_id}")

            facts = {
                "is_active": False,
                "cancel_requested": False,
                "lease_expired": False,
                "can_rollback": True,
            }
            decide_cancel_recovery(facts)

            diag = tx.record_diagnostic(phase, error_code, message, details)
            self._diagnostics.append(diag)
            self.rollback_owned_work(tx_id)
            tx.state = SnapshotTxState.FAILED
            tx.updated_at = time.time()
            return tx

    def rollback_owned_work(self, tx_id: str) -> Dict[str, Any]:
        tx = self._transactions.get(tx_id)
        if not tx:
            return {"rolled_back_chunks": 0, "pruned_records": 0}

        chunk_count = len(tx.owned_chunks)
        record_count = sum(len(records) for records in tx.staged_records.values())

        tx.owned_chunks.clear()
        tx.staged_records.clear()
        tx.updated_at = time.time()

        return {
            "tx_id": tx_id,
            "rolled_back_chunks": chunk_count,
            "pruned_records": record_count,
        }

    def recover_stranded_transactions(
        self,
        now: Optional[float] = None,
        pid_alive_checker: Optional[Callable[[int], bool]] = None,
    ) -> List[SnapshotTx]:
        with self._lock:
            recovered: List[SnapshotTx] = []
            checker = pid_alive_checker if pid_alive_checker is not None else _is_pid_alive
            current_time = time.time() if now is None else now

            for tx in self._transactions.values():
                if tx.is_terminal():
                    continue

                is_stranded = False
                reason = ""

                if tx.owner_pid is not None and not checker(tx.owner_pid):
                    is_stranded = True
                    reason = f"Owner process {tx.owner_pid} is no longer alive"
                elif tx.is_expired(current_time):
                    is_stranded = True
                    reason = f"Lease heartbeat expired (timeout={tx.lease_timeout}s)"

                if is_stranded:
                    facts = {
                        "is_active": False,
                        "cancel_requested": False,
                        "lease_expired": True,
                        "can_rollback": True,
                    }
                    decide_cancel_recovery(facts)
                    diag = tx.record_diagnostic(
                        phase="PROCESS_DEATH",
                        error_code="ORPHANED_PROCESS_DEATH",
                        message=reason,
                        details={
                            "owner_pid": tx.owner_pid,
                            "last_heartbeat": tx.heartbeat_at,
                            "prior_state": tx.state.value,
                        },
                    )
                    self._diagnostics.append(diag)
                    self.rollback_owned_work(tx.tx_id)
                    tx.state = SnapshotTxState.FAILED
                    tx.updated_at = current_time
                    recovered.append(tx)

            return recovered

    def save_to_disk(self, path: Optional[Path | str] = None) -> None:
        target = Path(path) if path else self.storage_path
        if not target:
            raise ValueError("No storage path provided for save_to_disk")

        payload = {
            "version": 1,
            "global_stream_offsets": self.get_global_offsets(),
            "transactions": [tx.to_dict() for tx in self._transactions.values()],
            "diagnostics": [d.to_dict() for d in self._diagnostics],
        }

        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=str(target.parent), delete=False) as tf:
            json.dump(payload, tf, indent=2)
            temp_name = tf.name
        os.replace(temp_name, str(target))

    def load_from_disk(self, path: Optional[Path | str] = None) -> None:
        target = Path(path) if path else self.storage_path
        if not target or not target.exists():
            return

        with open(target, "r", encoding="utf-8") as f:
            payload = json.load(f)

        with self._lock:
            self._global_stream_offsets = dict(payload.get("global_stream_offsets") or {})
            self._transactions.clear()
            for tx_data in payload.get("transactions") or []:
                tx = SnapshotTx.from_dict(tx_data)
                self._transactions[tx.tx_id] = tx
            self._diagnostics = [
                DiagnosticEvidence.from_dict(d) for d in payload.get("diagnostics") or []
            ]
