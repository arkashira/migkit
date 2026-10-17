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

from . import durable


class ShadowResyncError(Exception):
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
        except Exception:
            return None


class InMemoryTableEngine:
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

        if not metrics.supports_atomic_swap:
            if (
                metrics.divergent_ranges
                and metrics.divergent_rows <= max(50, int(0.05 * max(1, metrics.total_rows)))
                and not metrics.schema_drifted
            ):
                return ResyncStrategy.FENCED_RANGE_REPAIR
            return ResyncStrategy.WHOLE_RELOAD

        if metrics.schema_drifted:
            return ResyncStrategy.SHADOW_TABLE_RESNAPSHOT

        if (
            metrics.divergent_ranges
            and metrics.divergent_rows <= max(50, int(0.05 * max(1, metrics.total_rows)))
        ):
            return ResyncStrategy.FENCED_RANGE_REPAIR

        if self.allow_whole_reload_fallback:
            has_headroom, _, _ = self.check_headroom(metrics, available_disk_bytes)
            if not has_headroom:
                return ResyncStrategy.WHOLE_RELOAD

        return ResyncStrategy.SHADOW_TABLE_RESNAPSHOT

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
