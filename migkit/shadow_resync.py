from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable
from .decide import Need, Rung


class ShadowResyncError(Exception):
    pass


class JournalReadError(ShadowResyncError):
    """Raised when swap journal exists on disk but cannot be read or parsed."""
    pass


class InsufficientHeadroomError(ShadowResyncError):
    def __init__(self, required_bytes: int, available_bytes: int, message: str = ""):
        self.required_bytes = required_bytes
        self.available_bytes = available_bytes
        self.deficit_bytes = max(0, required_bytes - available_bytes)
        super().__init__(
            message
            or f"insufficient disk headroom: required {required_bytes}B, available {available_bytes}B (deficit {self.deficit_bytes}B)"
        )


class ValidationFailedError(ShadowResyncError):
    def __init__(self, errors: Sequence[str]):
        self.errors = list(errors)
        super().__init__(f"shadow resync validation failed: {'; '.join(self.errors)}")


class AtomicSwapCrashError(ShadowResyncError):
    def __init__(self, stage: str, message: str = ""):
        self.stage = stage
        super().__init__(message or f"simulated crash at swap stage: {stage}")


class RollbackError(ShadowResyncError):
    pass


class InvalidStrategyError(ShadowResyncError):
    pass


class ResyncStrategy(str, Enum):
    FENCED_RANGE_REPAIR = "fenced_range_repair"
    SHADOW_TABLE_RESNAPSHOT = "shadow_table_resnapshot"
    WHOLE_RELOAD = "whole_reload"


def resync_strategy_rungs() -> Tuple[Rung, ...]:
    """Strategy selection rungs for shadow resync (Row 618)."""
    return (
        Rung(
            "shadow_table_resnapshot",
            "zero-downtime shadow table resnapshot with CDC buffer catchup and atomic table swap",
            needs=(
                Need(
                    "atomic swap capability",
                    lambda f: bool(f.get("supports_atomic_swap")),
                    "target engine does not support atomic table swap",
                ),
                Need(
                    "sufficient disk headroom",
                    lambda f: bool(f.get("has_headroom")),
                    "insufficient disk headroom for shadow table creation",
                ),
                Need(
                    "unfenced divergence or schema drift",
                    lambda f: bool(f.get("schema_drifted") or not f.get("has_divergent_ranges") or not f.get("within_range_threshold")),
                    "divergence is bounded within range repair threshold and schema has not drifted",
                ),
            ),
            because="zero-downtime shadow table resnapshot with CDC buffer catchup and atomic swap",
        ),
        Rung(
            "fenced_range_repair",
            "in-place fenced repair for bounded divergence ranges",
            needs=(
                Need(
                    "divergent ranges identified",
                    lambda f: bool(f.get("has_divergent_ranges")),
                    "no divergent ranges available for selective repair",
                ),
                Need(
                    "bounded row divergence",
                    lambda f: bool(f.get("within_range_threshold")),
                    "divergent rows exceed range repair threshold",
                ),
                Need(
                    "no schema drift",
                    lambda f: not bool(f.get("schema_drifted")),
                    "schema has drifted; requires full table resnapshot",
                ),
            ),
            because="fenced range repair selectively replaces divergent key ranges without rebuilding entire table",
        ),
        Rung(
            "whole_table_reload",
            "whole table copy and reload",
            because="fallback whole reload when atomic swap or range repair are not admissible",
        ),
    )


def shadow_resync_threshold_rungs() -> Tuple[Rung, ...]:
    """Dynamic threshold and headroom envelope rungs (Row 618)."""
    return (
        Rung(
            "configured_resync_thresholds",
            "explicit thresholds configured via hop options or parameters",
            needs=(
                Need(
                    "custom threshold options configured",
                    lambda f: bool(f.get("has_custom_thresholds")),
                    "no custom shadow resync thresholds configured",
                ),
            ),
            because="operator specified custom divergence bounds, headroom multipliers, or retention TTL",
        ),
        Rung(
            "adaptive_workload_thresholds",
            "adaptive divergence and headroom thresholds scaled to table volume and host capacity",
            needs=(
                Need(
                    "table volume metrics known",
                    lambda f: bool(int(f.get("total_rows", 0)) > 0 and int(f.get("estimated_table_bytes", 0)) > 0),
                    "table row count or storage size unknown",
                ),
            ),
            because="dynamically scales range repair limit and buffer headroom to table volume",
        ),
        Rung(
            "default_resync_thresholds",
            "standard production baseline thresholds",
            because="production baseline thresholds (50 rows / 5%, 2.0x headroom, 100MB min free, 3600s TTL)",
        ),
    )


def decide_resync_thresholds(
    metrics: ObjectMetrics,
    available_disk_bytes: int,
    hop: Any = None,
    options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from . import decide
    opts = {}
    if hop is not None and hasattr(hop, "options") and isinstance(hop.options, dict):
        opts.update(hop.options)
    if options and isinstance(options, dict):
        opts.update(options)

    has_custom = bool(
        "resync_range_max_rows" in opts
        or "resync_range_ratio" in opts
        or "shadow_headroom_factor" in opts
        or "shadow_min_free_bytes" in opts
        or "shadow_buffer_headroom_bytes" in opts
        or "shadow_retention_ttl_seconds" in opts
    )
    facts = {
        "has_custom_thresholds": has_custom,
        "total_rows": metrics.total_rows,
        "estimated_table_bytes": metrics.estimated_table_bytes,
        "available_disk_bytes": available_disk_bytes,
    }
    choice = decide.climb("shadow resync thresholds", shadow_resync_threshold_rungs(), facts)
    if choice.path == "configured_resync_thresholds":
        max_rows = opts.get("resync_range_max_rows", max(50, int(opts.get("resync_range_ratio", 0.05) * max(1, metrics.total_rows))))
        factor = float(opts.get("shadow_headroom_factor", 2.0))
        min_free = int(opts.get("shadow_min_free_bytes", 100 * 1024 * 1024))
        buf_bytes = int(opts.get("shadow_buffer_headroom_bytes", 10 * 1024 * 1024))
        ttl = float(opts.get("shadow_retention_ttl_seconds", 3600.0))
    elif choice.path == "adaptive_workload_thresholds":
        max_rows = min(10000, max(50, int(0.02 * metrics.total_rows)))
        factor = 1.5 if metrics.estimated_table_bytes > 10 * 1024 * 1024 * 1024 else 2.0
        min_free = max(50 * 1024 * 1024, min(available_disk_bytes // 10, 500 * 1024 * 1024))
        buf_bytes = min(64 * 1024 * 1024, max(5 * 1024 * 1024, int(0.01 * metrics.estimated_table_bytes)))
        ttl = 3600.0
    else:
        max_rows = max(50, int(0.05 * max(1, metrics.total_rows)))
        factor = 2.0
        min_free = 100 * 1024 * 1024
        buf_bytes = 10 * 1024 * 1024
        ttl = 3600.0

    return {
        "max_range_rows": max_rows,
        "range_max_rows": max_rows,
        "range_ratio": 0.05,
        "headroom_config": HeadroomConfig(headroom_factor=factor, min_free_bytes=min_free, buffer_headroom_bytes=buf_bytes),
        "retention_policy": RollbackRetentionPolicy(retention_ttl_seconds=ttl),
    }



class ResyncPhase(str, Enum):
    INITIALIZED = "INITIALIZED"
    STRATEGY_SELECTED = "STRATEGY_SELECTED"
    HEADROOM_CHECKED = "HEADROOM_CHECKED"
    BUFFERING_CDC = "BUFFERING_CDC"
    SNAPSHOT_RUNNING = "SNAPSHOT_RUNNING"
    SNAPSHOT_COMPLETED = "SNAPSHOT_COMPLETED"
    CATCHING_UP = "CATCHING_UP"
    VALIDATING = "VALIDATING"
    VALIDATED = "VALIDATED"
    PREPARING_SWAP = "PREPARING_SWAP"
    SWAPPING = "SWAPPING"
    SWAPPED = "SWAPPED"
    ROLLED_BACK = "ROLLED_BACK"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


class CDCOp(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"


@dataclass
class CDCEvent:
    lsn: int
    op: CDCOp
    table: str
    key: Dict[str, Any]
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    schema_change: Optional[Dict[str, Any]] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class ObjectMetrics:
    table_name: str
    total_rows: int = 0
    divergent_rows: int = 0
    divergent_ranges: List[Tuple[Any, Any]] = field(default_factory=list)
    estimated_table_bytes: int = 0
    supports_atomic_swap: bool = True
    has_active_foreign_keys: bool = False
    has_dependent_views: bool = False
    schema_drifted: bool = False
    force_strategy: Optional[ResyncStrategy] = None


@dataclass
class HeadroomConfig:
    headroom_factor: float = 2.0
    min_free_bytes: int = 100 * 1024 * 1024
    buffer_headroom_bytes: int = 10 * 1024 * 1024


@dataclass
class RollbackRetentionPolicy:
    retention_ttl_seconds: float = 3600.0
    retain_old_table: bool = True
    old_table_prefix: str = "_migkit_old_"
    shadow_table_prefix: str = "_migkit_shadow_"


@dataclass
class ValidationResult:
    valid: bool
    shadow_row_count: int
    expected_row_count: int
    checksum_match: bool
    schema_valid: bool
    foreign_keys_intact: bool
    bindings_intact: bool = True
    errors: List[str] = field(default_factory=list)


@dataclass
class ResyncReport:
    session_id: str
    table_name: str
    strategy: ResyncStrategy
    phase: ResyncPhase
    shadow_table: Optional[str] = None
    old_table: Optional[str] = None
    snapshot_rows: int = 0
    buffered_events_count: int = 0
    reconciled_events_count: int = 0
    start_lsn: int = 0
    end_lsn: int = 0
    validation: Optional[ValidationResult] = None
    elapsed_seconds: float = 0.0


@dataclass
class RecoveryReport:
    session_id: str
    table_name: str
    recovered: bool
    active_table_verified: bool
    strategy_used: str
    final_phase: ResyncPhase
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RollbackReport:
    session_id: str
    table_name: str
    rolled_back: bool
    details: Dict[str, Any] = field(default_factory=dict)


class CDCBuffer:
    def __init__(self, start_lsn: int = 0):
        self.start_lsn = start_lsn
        self._events: List[CDCEvent] = []

    def append(self, event: CDCEvent) -> None:
        if event.lsn >= self.start_lsn:
            self._events.append(event)
            self._events.sort(key=lambda e: e.lsn)

    def events(self) -> List[CDCEvent]:
        return list(self._events)

    def size(self) -> int:
        return len(self._events)

    def reconcile(
        self,
        snapshot_rows: Dict[Any, Dict[str, Any]],
        key_column: str = "id",
    ) -> Dict[Any, Dict[str, Any]]:
        result = {k: copy.deepcopy(v) for k, v in snapshot_rows.items()}
        for ev in self._events:
            if ev.op == CDCOp.SCHEMA_CHANGE:
                if ev.schema_change and "column" in ev.schema_change:
                    col = ev.schema_change["column"]
                    default_val = ev.schema_change.get("default")
                    for row in result.values():
                        if col not in row:
                            row[col] = default_val
            elif ev.op == CDCOp.DELETE:
                k = ev.key.get(key_column)
                if k in result:
                    del result[k]
            elif ev.op == CDCOp.INSERT:
                if ev.after:
                    k = ev.key.get(key_column, ev.after.get(key_column))
                    result[k] = copy.deepcopy(ev.after)
            elif ev.op == CDCOp.UPDATE:
                if ev.after:
                    k = ev.key.get(key_column, ev.after.get(key_column))
                    if k in result:
                        result[k].update(copy.deepcopy(ev.after))
                    else:
                        result[k] = copy.deepcopy(ev.after)
        return result


class SwapJournal:
    STAGE_NOT_STARTED = "NOT_STARTED"
    STAGE_BEFORE_SWAP = "BEFORE_SWAP"
    STAGE_AFTER_RENAME_LIVE = "AFTER_RENAME_LIVE"
    STAGE_AFTER_RENAME_SHADOW = "AFTER_RENAME_SHADOW"
    STAGE_COMPLETED = "COMPLETED"
    STAGE_ROLLED_BACK = "ROLLED_BACK"

    def __init__(self, journal_path: Path):
        self.path = journal_path

    def write(self, state: Dict[str, Any]) -> None:
        state["updated_at"] = time.time()
        durable.write_json(self.path, state, mode=0o600, full=False)

    def read(self) -> Optional[Dict[str, Any]]:
        if not self.path.exists():
            return None
        try:
            return durable.read_state(self.path, kind="journal")
        except (durable.Torn, Exception) as e:
            raise JournalReadError(f"Failed to read swap journal at {self.path}: {e}") from e


class ShadowTableEngine:
    """Abstract base class for shadow table engines."""
    disk_free_bytes: int

    def create_table(self, name: str, columns: List[str], primary_key: str = "id", rows: Optional[List[Dict[str, Any]]] = None) -> None:
        raise NotImplementedError

    def table_exists(self, name: str) -> bool:
        raise NotImplementedError

    def drop_table(self, name: str) -> None:
        raise NotImplementedError

    def get_columns(self, name: str) -> List[str]:
        raise NotImplementedError

    def get_rows(self, name: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def set_rows(self, name: str, rows: List[Dict[str, Any]]) -> None:
        raise NotImplementedError

    def rename_table(self, old_name: str, new_name: str) -> None:
        raise NotImplementedError

    def execute_atomic_swap(self, live_table: str, shadow_table: str, old_table: str, journal: Optional[SwapJournal] = None, simulate_crash_stage: Optional[str] = None) -> None:
        raise NotImplementedError

    def repair_fenced_ranges(self, table_name: str, ranges: Sequence[Tuple[Any, Any]], rows: Sequence[Dict[str, Any]]) -> None:
        raise NotImplementedError

    def add_foreign_key(self, table: str, fk_name: str, column: str, ref_table: str, ref_column: str) -> None:
        pass

    def get_foreign_keys(self, table: str) -> List[Dict[str, Any]]:
        return []

    def has_foreign_key(self, table: str, fk_name: str) -> bool:
        return False

    def add_binding(self, table: str, binding_name: str) -> None:
        pass

    def get_bindings(self, table: str) -> Set[str]:
        return set()


class InMemoryTableEngine(ShadowTableEngine):
    def __init__(self, disk_free_bytes: int = 1024 * 1024 * 1024):
        self.disk_free_bytes = disk_free_bytes
        self._tables: Dict[str, Dict[str, Any]] = {}
        self._foreign_keys: Dict[str, List[Dict[str, Any]]] = {}
        self._bindings: Dict[str, Set[str]] = {}

    def create_table(
        self,
        name: str,
        columns: List[str],
        primary_key: str = "id",
        rows: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self._tables[name] = {
            "columns": list(columns),
            "primary_key": primary_key,
            "rows": {r[primary_key]: copy.deepcopy(r) for r in (rows or [])},
        }

    def table_exists(self, name: str) -> bool:
        return name in self._tables

    def drop_table(self, name: str) -> None:
        self._tables.pop(name, None)

    def get_columns(self, name: str) -> List[str]:
        if name not in self._tables:
            return []
        return list(self._tables[name]["columns"])

    def add_column(self, name: str, col: str, default: Any = None) -> None:
        if name in self._tables:
            if col not in self._tables[name]["columns"]:
                self._tables[name]["columns"].append(col)
                for r in self._tables[name]["rows"].values():
                    r[col] = default

    def get_rows(self, name: str) -> List[Dict[str, Any]]:
        if name not in self._tables:
            return []
        return [copy.deepcopy(r) for r in self._tables[name]["rows"].values()]

    def set_rows(self, name: str, rows: List[Dict[str, Any]]) -> None:
        if name in self._tables:
            pk = self._tables[name]["primary_key"]
            self._tables[name]["rows"] = {r[pk]: copy.deepcopy(r) for r in rows}

    def add_foreign_key(
        self,
        table: str,
        fk_name: str,
        column: str,
        ref_table: str,
        ref_column: str,
    ) -> None:
        if table not in self._foreign_keys:
            self._foreign_keys[table] = []
        self._foreign_keys[table].append(
            {
                "name": fk_name,
                "column": column,
                "ref_table": ref_table,
                "ref_column": ref_column,
            }
        )

    def has_foreign_key(self, table: str, fk_name: str) -> bool:
        return any(fk["name"] == fk_name for fk in self._foreign_keys.get(table, []))

    def add_binding(self, table: str, binding_name: str) -> None:
        if table not in self._bindings:
            self._bindings[table] = set()
        self._bindings[table].add(binding_name)

    def get_bindings(self, table: str) -> Set[str]:
        return set(self._bindings.get(table, set()))

    def rename_table(self, old_name: str, new_name: str) -> None:
        if old_name not in self._tables:
            raise KeyError(f"table {old_name} does not exist")
        self._tables[new_name] = self._tables.pop(old_name)

    def execute_atomic_swap(
        self,
        live_table: str,
        shadow_table: str,
        old_table: str,
        journal: Optional[SwapJournal] = None,
        simulate_crash_stage: Optional[str] = None,
    ) -> None:
        if simulate_crash_stage == "BEFORE_SWAP":
            if journal:
                jstate = journal.read() or {}
                jstate["stage"] = SwapJournal.STAGE_BEFORE_SWAP
                journal.write(jstate)
            raise AtomicSwapCrashError("BEFORE_SWAP")

        if journal:
            jstate = journal.read() or {}
            jstate["stage"] = SwapJournal.STAGE_AFTER_RENAME_LIVE
            journal.write(jstate)

        live_data = self._tables.pop(live_table)
        self._tables[old_table] = live_data

        if simulate_crash_stage == "AFTER_RENAME_LIVE":
            raise AtomicSwapCrashError("AFTER_RENAME_LIVE")

        if journal:
            jstate = journal.read() or {}
            jstate["stage"] = SwapJournal.STAGE_AFTER_RENAME_SHADOW
            journal.write(jstate)

        shadow_data = self._tables.pop(shadow_table)
        self._tables[live_table] = shadow_data

        if live_table in self._foreign_keys:
            pass
        if live_table in self._bindings:
            pass

        if simulate_crash_stage == "AFTER_RENAME_SHADOW":
            raise AtomicSwapCrashError("AFTER_RENAME_SHADOW")

        if journal:
            jstate = journal.read() or {}
            jstate["stage"] = SwapJournal.STAGE_COMPLETED
            journal.write(jstate)

    def repair_fenced_ranges(
        self,
        table_name: str,
        ranges: Sequence[Tuple[Any, Any]],
        rows: Sequence[Dict[str, Any]],
    ) -> None:
        if table_name not in self._tables:
            return
        pk = self._tables[table_name]["primary_key"]
        for r in rows:
            self._tables[table_name]["rows"][r[pk]] = copy.deepcopy(r)


class DBAPIShadowTableEngine(ShadowTableEngine):
    """Real DB-API 2.0 cursor execution engine for PostgreSQL, MySQL, and SQLite."""

    def __init__(self, connection: Any, dialect: str = "sqlite", disk_free_bytes: int = 1024 * 1024 * 1024):
        self.connection = connection
        self.dialect = str(dialect).lower()
        self.disk_free_bytes = disk_free_bytes
        self._foreign_keys: Dict[str, List[Dict[str, Any]]] = {}
        self._bindings: Dict[str, Set[str]] = {}

    def _q(self, ident: str) -> str:
        s = str(ident)
        if self.dialect in ("mysql", "mariadb"):
            return "`" + s.replace("`", "``") + "`"
        return '"' + s.replace('"', '""') + '"'

    def table_exists(self, name: str) -> bool:
        cur = self.connection.cursor()
        try:
            cur.execute(f"SELECT 1 FROM {self._q(name)} WHERE 1 = 0")
            return True
        except Exception:
            if hasattr(self.connection, "rollback"):
                try:
                    self.connection.rollback()
                except Exception:
                    pass
            return False
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def create_table(self, name: str, columns: List[str], primary_key: str = "id", rows: Optional[List[Dict[str, Any]]] = None) -> None:
        col_defs = []
        for c in columns:
            if c == primary_key:
                col_defs.append(f"{self._q(c)} INTEGER PRIMARY KEY" if self.dialect == "sqlite" else f"{self._q(c)} INT PRIMARY KEY")
            else:
                col_defs.append(f"{self._q(c)} TEXT")
        sql = f"CREATE TABLE {self._q(name)} ({', '.join(col_defs)})"
        cur = self.connection.cursor()
        try:
            cur.execute(sql)
            if hasattr(self.connection, "commit"):
                self.connection.commit()
            if rows:
                self.set_rows(name, rows)
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def drop_table(self, name: str) -> None:
        cur = self.connection.cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS {self._q(name)}")
            if hasattr(self.connection, "commit"):
                self.connection.commit()
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def get_columns(self, name: str) -> List[str]:
        if not self.table_exists(name):
            return []
        cur = self.connection.cursor()
        try:
            cur.execute(f"SELECT * FROM {self._q(name)} WHERE 1 = 0")
            if cur.description:
                return [d[0] for d in cur.description]
            return []
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def get_rows(self, name: str) -> List[Dict[str, Any]]:
        if not self.table_exists(name):
            return []
        cur = self.connection.cursor()
        try:
            cur.execute(f"SELECT * FROM {self._q(name)}")
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def set_rows(self, name: str, rows: List[Dict[str, Any]]) -> None:
        cur = self.connection.cursor()
        try:
            cur.execute(f"DELETE FROM {self._q(name)}")
            if rows:
                cols = list(rows[0].keys())
                col_str = ", ".join(self._q(c) for c in cols)
                val_str = ", ".join(["?"] * len(cols)) if self.dialect == "sqlite" else ", ".join(["%s"] * len(cols))
                sql = f"INSERT INTO {self._q(name)} ({col_str}) VALUES ({val_str})"
                params = [[r.get(c) for c in cols] for r in rows]
                cur.executemany(sql, params)
            if hasattr(self.connection, "commit"):
                self.connection.commit()
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def rename_table(self, old_name: str, new_name: str) -> None:
        cur = self.connection.cursor()
        try:
            if self.dialect in ("mysql", "mariadb"):
                cur.execute(f"RENAME TABLE {self._q(old_name)} TO {self._q(new_name)}")
            else:
                cur.execute(f"ALTER TABLE {self._q(old_name)} RENAME TO {self._q(new_name)}")
            if hasattr(self.connection, "commit"):
                self.connection.commit()
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def execute_atomic_swap(
        self,
        live_table: str,
        shadow_table: str,
        old_table: str,
        journal: Optional[SwapJournal] = None,
        simulate_crash_stage: Optional[str] = None,
    ) -> None:
        if simulate_crash_stage == "BEFORE_SWAP":
            if journal:
                jstate = journal.read() or {}
                jstate["stage"] = SwapJournal.STAGE_BEFORE_SWAP
                journal.write(jstate)
            raise AtomicSwapCrashError("BEFORE_SWAP")

        if journal:
            jstate = journal.read() or {}
            jstate["stage"] = SwapJournal.STAGE_AFTER_RENAME_LIVE
            journal.write(jstate)

        cur = self.connection.cursor()
        try:
            if self.dialect in ("mysql", "mariadb"):
                if simulate_crash_stage == "AFTER_RENAME_LIVE":
                    cur.execute(f"RENAME TABLE {self._q(live_table)} TO {self._q(old_table)}")
                    if hasattr(self.connection, "commit"):
                        self.connection.commit()
                    raise AtomicSwapCrashError("AFTER_RENAME_LIVE")
                cur.execute(f"RENAME TABLE {self._q(live_table)} TO {self._q(old_table)}, {self._q(shadow_table)} TO {self._q(live_table)}")
                if hasattr(self.connection, "commit"):
                    self.connection.commit()
            else:
                cur.execute(f"ALTER TABLE {self._q(live_table)} RENAME TO {self._q(old_table)}")
                if simulate_crash_stage == "AFTER_RENAME_LIVE":
                    if hasattr(self.connection, "commit"):
                        self.connection.commit()
                    raise AtomicSwapCrashError("AFTER_RENAME_LIVE")
                cur.execute(f"ALTER TABLE {self._q(shadow_table)} RENAME TO {self._q(live_table)}")
                if hasattr(self.connection, "commit"):
                    self.connection.commit()

            if simulate_crash_stage == "AFTER_RENAME_SHADOW":
                if journal:
                    jstate = journal.read() or {}
                    jstate["stage"] = SwapJournal.STAGE_AFTER_RENAME_SHADOW
                    journal.write(jstate)
                raise AtomicSwapCrashError("AFTER_RENAME_SHADOW")

            if journal:
                jstate = journal.read() or {}
                jstate["stage"] = SwapJournal.STAGE_COMPLETED
                journal.write(jstate)
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def repair_fenced_ranges(
        self,
        table_name: str,
        ranges: Sequence[Tuple[Any, Any]],
        rows: Sequence[Dict[str, Any]],
    ) -> None:
        if not rows or not self.table_exists(table_name):
            return
        cur = self.connection.cursor()
        try:
            for lo, hi in ranges:
                q = "?" if self.dialect == "sqlite" else "%s"
                cur.execute(f"DELETE FROM {self._q(table_name)} WHERE id >= {q} AND id <= {q}", (lo, hi))
            cols = list(rows[0].keys())
            col_str = ", ".join(self._q(c) for c in cols)
            val_str = ", ".join(["?"] * len(cols)) if self.dialect == "sqlite" else ", ".join(["%s"] * len(cols))
            sql = f"INSERT INTO {self._q(table_name)} ({col_str}) VALUES ({val_str})"
            cur.executemany(sql, [[r.get(c) for c in cols] for r in rows])
            if hasattr(self.connection, "commit"):
                self.connection.commit()
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def add_foreign_key(self, table: str, fk_name: str, column: str, ref_table: str, ref_column: str) -> None:
        self._foreign_keys.setdefault(table, []).append({"name": fk_name, "column": column, "ref_table": ref_table, "ref_column": ref_column})

    def get_foreign_keys(self, table: str) -> List[Dict[str, Any]]:
        return list(self._foreign_keys.get(table, []))

    def has_foreign_key(self, table: str, fk_name: str) -> bool:
        return any(fk["name"] == fk_name for fk in self._foreign_keys.get(table, []))

    def add_binding(self, table: str, binding_name: str) -> None:
        self._bindings.setdefault(table, set()).add(binding_name)

    def get_bindings(self, table: str) -> Set[str]:
        return set(self._bindings.get(table, set()))


class ShadowResyncCoordinator:
    def __init__(
        self,
        headroom_config: Optional[HeadroomConfig] = None,
        retention_policy: Optional[RollbackRetentionPolicy] = None,
        journal_dir: Optional[Path] = None,
        allow_whole_reload_fallback: bool = False,
    ):
        self.headroom_config = headroom_config or HeadroomConfig()
        self.retention_policy = retention_policy or RollbackRetentionPolicy()
        self.journal_dir = journal_dir or Path(tempfile.gettempdir())
        self.allow_whole_reload_fallback = allow_whole_reload_fallback
        self.active_session_id: Optional[str] = None
        self.on_snapshot_in_progress: Optional[Callable[[], None]] = None
        self.custom_validator: Optional[
            Callable[[Any, str], Tuple[bool, List[str]]]
        ] = None

    def select_strategy(
        self,
        metrics: ObjectMetrics,
        available_disk_bytes: int,
    ) -> ResyncStrategy:
        if metrics.force_strategy:
            return metrics.force_strategy

        thresholds = decide_resync_thresholds(metrics, available_disk_bytes)
        max_rows = thresholds.get("range_max_rows", 50)
        ratio = thresholds.get("range_ratio", 0.05)
        limit = max(max_rows, int(ratio * max(1, metrics.total_rows)))
        within_range = bool(metrics.divergent_rows <= limit)
        has_headroom, _, _ = self.check_headroom(metrics, available_disk_bytes)

        facts = {
            "supports_atomic_swap": bool(metrics.supports_atomic_swap),
            "has_headroom": bool(has_headroom or not self.allow_whole_reload_fallback),
            "schema_drifted": bool(metrics.schema_drifted),
            "within_range_threshold": within_range,
            "has_divergent_ranges": bool(metrics.divergent_ranges),
        }
        choice = decide.climb("resync strategy selection", resync_strategy_rungs(), facts)
        if choice.path == "shadow_table_resnapshot":
            return ResyncStrategy.SHADOW_TABLE_RESNAPSHOT
        elif choice.path == "fenced_range_repair":
            return ResyncStrategy.FENCED_RANGE_REPAIR
        else:
            return ResyncStrategy.WHOLE_RELOAD

    def check_headroom(
        self,
        metrics: ObjectMetrics,
        available_disk_bytes: int,
    ) -> Tuple[bool, int, int]:
        required = (
            int(metrics.estimated_table_bytes * self.headroom_config.headroom_factor)
            + self.headroom_config.buffer_headroom_bytes
            + self.headroom_config.min_free_bytes
        )
        return (available_disk_bytes >= required, required, available_disk_bytes)

    def _get_journal(self, session_id: str) -> SwapJournal:
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        return SwapJournal(self.journal_dir / f"swap_{session_id}.journal")

    def coordinate_resync(
        self,
        engine: Any,
        metrics: ObjectMetrics,
        source_snapshot: Sequence[Dict[str, Any]],
        cdc_events: Optional[Sequence[CDCEvent]] = None,
        start_lsn: int = 0,
        simulate_crash_stage: Optional[str] = None,
    ) -> ResyncReport:
        t0 = time.time()
        session_id = uuid.uuid4().hex[:12]
        self.active_session_id = session_id
        journal = self._get_journal(session_id)

        available_disk = getattr(engine, "disk_free_bytes", 1024 * 1024 * 1024)
        strategy = self.select_strategy(metrics, available_disk)

        if strategy == ResyncStrategy.SHADOW_TABLE_RESNAPSHOT:
            has_headroom, req_bytes, avail_bytes = self.check_headroom(
                metrics, available_disk
            )
            if not has_headroom:
                raise InsufficientHeadroomError(req_bytes, avail_bytes)

        if strategy == ResyncStrategy.FENCED_RANGE_REPAIR:
            if hasattr(engine, "repair_fenced_ranges"):
                engine.repair_fenced_ranges(
                    metrics.table_name, metrics.divergent_ranges, source_snapshot
                )
            return ResyncReport(
                session_id=session_id,
                table_name=metrics.table_name,
                strategy=strategy,
                phase=ResyncPhase.SWAPPED,
                snapshot_rows=len(source_snapshot),
                start_lsn=start_lsn,
                end_lsn=start_lsn,
                elapsed_seconds=time.time() - t0,
            )

        if strategy == ResyncStrategy.WHOLE_RELOAD:
            if hasattr(engine, "set_rows"):
                engine.set_rows(metrics.table_name, list(source_snapshot))
            return ResyncReport(
                session_id=session_id,
                table_name=metrics.table_name,
                strategy=strategy,
                phase=ResyncPhase.SWAPPED,
                snapshot_rows=len(source_snapshot),
                start_lsn=start_lsn,
                end_lsn=start_lsn,
                elapsed_seconds=time.time() - t0,
            )

        shadow_table = f"{self.retention_policy.shadow_table_prefix}{metrics.table_name}"
        old_table = f"{self.retention_policy.old_table_prefix}{metrics.table_name}"

        initial_journal = {
            "session_id": session_id,
            "table_name": metrics.table_name,
            "shadow_table": shadow_table,
            "old_table": old_table,
            "phase": ResyncPhase.INITIALIZED.value,
            "stage": SwapJournal.STAGE_NOT_STARTED,
            "start_lsn": start_lsn,
            "created_at": t0,
        }
        journal.write(initial_journal)

        cdc_buffer = CDCBuffer(start_lsn=start_lsn)
        if cdc_events:
            for ev in cdc_events:
                cdc_buffer.append(ev)

        live_cols = engine.get_columns(metrics.table_name)
        schema_changes = [
            ev for ev in cdc_buffer.events() if ev.op == CDCOp.SCHEMA_CHANGE
        ]
        target_cols = list(live_cols)
        for sc in schema_changes:
            if sc.schema_change and "column" in sc.schema_change:
                c = sc.schema_change["column"]
                if c not in target_cols:
                    target_cols.append(c)

        engine.create_table(
            shadow_table,
            columns=target_cols,
            primary_key="id",
            rows=None,
        )

        if self.on_snapshot_in_progress:
            self.on_snapshot_in_progress()

        raw_snapshot_map = {r["id"]: copy.deepcopy(r) for r in source_snapshot}
        for sc in schema_changes:
            if sc.schema_change and "column" in sc.schema_change:
                c = sc.schema_change["column"]
                d = sc.schema_change.get("default")
                for r in raw_snapshot_map.values():
                    if c not in r:
                        r[c] = d

        reconciled_map = cdc_buffer.reconcile(raw_snapshot_map, key_column="id")
        reconciled_rows = list(reconciled_map.values())
        engine.set_rows(shadow_table, reconciled_rows)

        validation = self._validate_shadow(
            engine, metrics.table_name, shadow_table, reconciled_rows
        )
        if not validation.valid:
            engine.drop_table(shadow_table)
            journal_state = journal.read() or {}
            journal_state["phase"] = ResyncPhase.FAILED.value
            journal.write(journal_state)
            raise ValidationFailedError(validation.errors)

        journal_state = journal.read() or {}
        journal_state["phase"] = ResyncPhase.VALIDATED.value
        journal.write(journal_state)

        engine.execute_atomic_swap(
            live_table=metrics.table_name,
            shadow_table=shadow_table,
            old_table=old_table,
            journal=journal,
            simulate_crash_stage=simulate_crash_stage,
        )

        journal_state = journal.read() or {}
        journal_state["phase"] = ResyncPhase.SWAPPED.value
        journal.write(journal_state)

        return ResyncReport(
            session_id=session_id,
            table_name=metrics.table_name,
            strategy=strategy,
            phase=ResyncPhase.SWAPPED,
            shadow_table=shadow_table,
            old_table=old_table,
            snapshot_rows=len(source_snapshot),
            buffered_events_count=cdc_buffer.size(),
            reconciled_events_count=len(reconciled_rows),
            start_lsn=start_lsn,
            end_lsn=max([ev.lsn for ev in cdc_buffer.events()] or [start_lsn]),
            validation=validation,
            elapsed_seconds=time.time() - t0,
        )

    def _validate_shadow(
        self,
        engine: Any,
        live_table: str,
        shadow_table: str,
        expected_rows: List[Dict[str, Any]],
    ) -> ValidationResult:
        if self.custom_validator:
            ok, errs = self.custom_validator(engine, shadow_table)
            return ValidationResult(
                valid=ok,
                shadow_row_count=len(engine.get_rows(shadow_table)),
                expected_row_count=len(expected_rows),
                checksum_match=ok,
                schema_valid=ok,
                foreign_keys_intact=ok,
                errors=errs,
            )

        shadow_rows = engine.get_rows(shadow_table)
        errors = []
        if len(shadow_rows) != len(expected_rows):
            errors.append(
                f"row count mismatch: shadow has {len(shadow_rows)}, expected {len(expected_rows)}"
            )

        shadow_cols = set(engine.get_columns(shadow_table))
        live_cols = set(engine.get_columns(live_table))
        if not live_cols.issubset(shadow_cols):
            errors.append(f"shadow table missing base columns: {live_cols - shadow_cols}")

        return ValidationResult(
            valid=len(errors) == 0,
            shadow_row_count=len(shadow_rows),
            expected_row_count=len(expected_rows),
            checksum_match=len(errors) == 0,
            schema_valid=len(errors) == 0,
            foreign_keys_intact=True,
            errors=errors,
        )

    def recover_from_crash(
        self,
        engine: Any,
        session_id: str,
    ) -> RecoveryReport:
        journal = self._get_journal(session_id)
        state = journal.read()
        if not state:
            return RecoveryReport(
                session_id=session_id,
                table_name="",
                recovered=False,
                active_table_verified=False,
                strategy_used="none",
                final_phase=ResyncPhase.FAILED,
                details={"error": "no journal found"},
            )

        table_name = state["table_name"]
        shadow_table = state["shadow_table"]
        old_table = state["old_table"]
        stage = state.get("stage", SwapJournal.STAGE_NOT_STARTED)

        if stage == SwapJournal.STAGE_AFTER_RENAME_LIVE:
            if engine.table_exists(shadow_table) and not engine.table_exists(table_name):
                engine.rename_table(shadow_table, table_name)
            elif not engine.table_exists(shadow_table) and engine.table_exists(old_table):
                engine.rename_table(old_table, table_name)
            state["stage"] = SwapJournal.STAGE_COMPLETED
            state["phase"] = ResyncPhase.SWAPPED.value
            journal.write(state)
            return RecoveryReport(
                session_id=session_id,
                table_name=table_name,
                recovered=True,
                active_table_verified=engine.table_exists(table_name),
                strategy_used="forward_complete_swap",
                final_phase=ResyncPhase.SWAPPED,
                details={"stage_recovered": stage},
            )

        if stage in (SwapJournal.STAGE_BEFORE_SWAP, SwapJournal.STAGE_NOT_STARTED):
            if engine.table_exists(shadow_table):
                engine.drop_table(shadow_table)
            state["phase"] = ResyncPhase.INITIALIZED.value
            journal.write(state)
            return RecoveryReport(
                session_id=session_id,
                table_name=table_name,
                recovered=True,
                active_table_verified=engine.table_exists(table_name),
                strategy_used="discard_shadow_keep_live",
                final_phase=ResyncPhase.INITIALIZED,
                details={"stage_recovered": stage},
            )

        if stage in (SwapJournal.STAGE_AFTER_RENAME_SHADOW, SwapJournal.STAGE_COMPLETED):
            state["stage"] = SwapJournal.STAGE_COMPLETED
            state["phase"] = ResyncPhase.SWAPPED.value
            journal.write(state)
            return RecoveryReport(
                session_id=session_id,
                table_name=table_name,
                recovered=True,
                active_table_verified=engine.table_exists(table_name),
                strategy_used="finalize_swap",
                final_phase=ResyncPhase.SWAPPED,
                details={"stage_recovered": stage},
            )

        return RecoveryReport(
            session_id=session_id,
            table_name=table_name,
            recovered=False,
            active_table_verified=engine.table_exists(table_name),
            strategy_used="unknown",
            final_phase=ResyncPhase.FAILED,
            details={"unhandled_stage": stage},
        )

    def rollback_swap(
        self,
        engine: Any,
        session_id: str,
    ) -> RollbackReport:
        journal = self._get_journal(session_id)
        state = journal.read()
        if not state:
            raise RollbackError(f"no journal found for session {session_id}")

        table_name = state["table_name"]
        shadow_table = state["shadow_table"]
        old_table = state["old_table"]

        if not engine.table_exists(old_table):
            raise RollbackError(f"cannot rollback: old table {old_table} does not exist")

        temp_swapped_table = f"_migkit_revert_{session_id}"
        engine.rename_table(table_name, temp_swapped_table)
        engine.rename_table(old_table, table_name)
        engine.rename_table(temp_swapped_table, shadow_table)

        state["phase"] = ResyncPhase.ROLLED_BACK.value
        state["stage"] = SwapJournal.STAGE_ROLLED_BACK
        journal.write(state)

        return RollbackReport(
            session_id=session_id,
            table_name=table_name,
            rolled_back=True,
            details={"restored_from": old_table},
        )

    def purge_retention(
        self,
        engine: Any,
        session_id: str,
        force: bool = False,
        max_age_seconds: Optional[float] = None,
    ) -> bool:
        journal = self._get_journal(session_id)
        state = journal.read()
        if not state:
            return False

        old_table = state["old_table"]
        created_at = state.get("created_at", 0)
        age = time.time() - created_at
        ttl = max_age_seconds or self.retention_policy.retention_ttl_seconds

        if force or age >= ttl:
            if engine.table_exists(old_table):
                engine.drop_table(old_table)
            state["phase"] = ResyncPhase.COMMITTED.value
            journal.write(state)
            return True

        return False
