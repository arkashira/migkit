import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from . import durable


class SnapshotStateError(RuntimeError):
    pass


class InvariantViolationError(SnapshotStateError):
    pass


class ObligationWipeError(InvariantViolationError):
    pass


class ShrinkingCoverageError(InvariantViolationError):
    pass


class IncompatibleWatermarkError(InvariantViolationError):
    pass


class InvalidPhaseTransitionError(InvariantViolationError):
    pass


class WorkUnitStatus:
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"
    FAILED = "failed"


class SubworkflowType:
    SNAPSHOT = "snapshot"
    INCREMENTAL_SNAPSHOT = "incremental_snapshot"
    CDC = "cdc"


class SubworkflowStatus:
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    PAUSED = "paused"
    FAILED = "failed"


class TransferInvariant:
    PRESERVE_OBLIGATIONS = "preserve_obligations"
    MONOTONIC_COVERAGE = "monotonic_coverage"
    WATERMARK_ORDER = "watermark_order"
    ISOLATED_BOOT = "isolated_boot"


def _parse_watermark(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        val = val.strip()
        if "/" in val:
            parts = val.split("/", 1)
            try:
                hi = int(parts[0], 16)
                lo = int(parts[1], 16)
                return (hi << 32) + lo
            except ValueError:
                pass
        try:
            return int(val)
        except ValueError:
            try:
                return float(val)
            except ValueError:
                return val
    return str(val)


def _compare_watermarks(w1: Any, w2: Any) -> int:
    p1 = _parse_watermark(w1)
    p2 = _parse_watermark(w2)
    if isinstance(p1, (int, float)) and isinstance(p2, (int, float)):
        if p1 < p2:
            return -1
        if p1 > p2:
            return 1
        return 0
    s1, s2 = str(w1), str(w2)
    if s1 < s2:
        return -1
    if s1 > s2:
        return 1
    return 0


@dataclass
class WorkUnit:
    unit_id: str
    target: str
    key_range: Optional[Dict[str, Any]] = None
    status: str = WorkUnitStatus.PENDING
    rows_processed: int = 0
    watermark: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "unit_id": self.unit_id,
            "target": self.target,
            "key_range": self.key_range,
            "status": self.status,
            "rows_processed": self.rows_processed,
            "watermark": self.watermark,
            "metadata": self.metadata,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkUnit":
        return cls(
            unit_id=d["unit_id"],
            target=d["target"],
            key_range=d.get("key_range"),
            status=d.get("status", WorkUnitStatus.PENDING),
            rows_processed=d.get("rows_processed", 0),
            watermark=d.get("watermark"),
            metadata=d.get("metadata", {}),
            started_at=d.get("started_at"),
            completed_at=d.get("completed_at"),
            error=d.get("error"),
        )


@dataclass
class Obligation:
    obligation_id: str
    phase: str
    kind: str
    target: str
    details: Dict[str, Any] = field(default_factory=dict)
    discharged: bool = False
    discharged_at: Optional[float] = None
    discharge_token: Optional[str] = None

    def discharge(self, token: Optional[str] = None) -> None:
        self.discharged = True
        self.discharged_at = time.time()
        self.discharge_token = token

    def to_dict(self) -> Dict[str, Any]:
        return {
            "obligation_id": self.obligation_id,
            "phase": self.phase,
            "kind": self.kind,
            "target": self.target,
            "details": self.details,
            "discharged": self.discharged,
            "discharged_at": self.discharged_at,
            "discharge_token": self.discharge_token,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Obligation":
        return cls(
            obligation_id=d["obligation_id"],
            phase=d["phase"],
            kind=d["kind"],
            target=d["target"],
            details=d.get("details", {}),
            discharged=d.get("discharged", False),
            discharged_at=d.get("discharged_at"),
            discharge_token=d.get("discharge_token"),
        )


@dataclass
class DurableSubworkflow:
    subworkflow_id: str
    subworkflow_type: str
    status: str = SubworkflowStatus.NOT_STARTED
    units: Dict[str, WorkUnit] = field(default_factory=dict)
    obligations: Dict[str, Obligation] = field(default_factory=dict)
    watermarks: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def total_units(self) -> int:
        return len(self.units)

    @property
    def completed_units(self) -> int:
        return sum(1 for u in self.units.values() if u.status == WorkUnitStatus.COMPLETED)

    @property
    def pending_units(self) -> int:
        return sum(1 for u in self.units.values() if u.status in (WorkUnitStatus.PENDING, WorkUnitStatus.IN_FLIGHT))

    @property
    def failed_units(self) -> int:
        return sum(1 for u in self.units.values() if u.status == WorkUnitStatus.FAILED)

    @property
    def coverage_ratio(self) -> float:
        return self.completed_units / self.total_units if self.total_units > 0 else 0.0

    @property
    def coverage_pct(self) -> float:
        return round(self.coverage_ratio * 100.0, 2)

    @property
    def pending_obligations(self) -> List[Obligation]:
        return [o for o in self.obligations.values() if not o.discharged]

    @property
    def has_pending_obligations(self) -> bool:
        return len(self.pending_obligations) > 0

    def register_unit(
        self,
        unit_id: str,
        target: str,
        key_range: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> WorkUnit:
        if unit_id in self.units:
            return self.units[unit_id]
        unit = WorkUnit(
            unit_id=unit_id,
            target=target,
            key_range=key_range,
            metadata=metadata or {},
        )
        self.units[unit_id] = unit
        obl_id = f"obl-unit-{unit_id}"
        self.add_obligation(
            kind="unit_execution",
            target=target,
            details={"unit_id": unit_id},
            obligation_id=obl_id,
        )
        if self.status == SubworkflowStatus.NOT_STARTED:
            self.status = SubworkflowStatus.IN_PROGRESS
        return unit

    def mark_in_flight(self, unit_id: str) -> None:
        if unit_id in self.units and self.units[unit_id].status == WorkUnitStatus.PENDING:
            self.units[unit_id].status = WorkUnitStatus.IN_FLIGHT
            self.units[unit_id].started_at = time.time()

    def mark_completed(self, unit_id: str, rows_processed: int = 0, watermark: Any = None) -> None:
        if unit_id not in self.units:
            self.register_unit(unit_id, target=unit_id)
        unit = self.units[unit_id]
        unit.status = WorkUnitStatus.COMPLETED
        unit.rows_processed = rows_processed
        unit.watermark = watermark
        unit.completed_at = time.time()
        obl_id = f"obl-unit-{unit_id}"
        if obl_id in self.obligations and not self.obligations[obl_id].discharged:
            self.obligations[obl_id].discharge(token=f"unit_completed_{unit_id}")
        if self.total_units > 0 and self.completed_units == self.total_units:
            self.status = SubworkflowStatus.COMPLETED
        elif self.status == SubworkflowStatus.NOT_STARTED:
            self.status = SubworkflowStatus.IN_PROGRESS

    def mark_failed(self, unit_id: str, error: str) -> None:
        if unit_id in self.units:
            self.units[unit_id].status = WorkUnitStatus.FAILED
            self.units[unit_id].error = error
            self.status = SubworkflowStatus.FAILED

    def add_obligation(
        self,
        kind: str,
        target: str,
        details: Optional[Dict[str, Any]] = None,
        obligation_id: Optional[str] = None,
    ) -> Obligation:
        if not obligation_id:
            obligation_id = f"obl-{self.subworkflow_type}-{len(self.obligations) + 1}"
        obl = Obligation(
            obligation_id=obligation_id,
            phase=self.subworkflow_type,
            kind=kind,
            target=target,
            details=details or {},
        )
        self.obligations[obligation_id] = obl
        return obl

    def discharge_obligation(self, obligation_id: str, token: Optional[str] = None) -> Obligation:
        if obligation_id not in self.obligations:
            raise KeyError(f"Obligation {obligation_id} not found")
        self.obligations[obligation_id].discharge(token=token)
        return self.obligations[obligation_id]

    def set_watermark(self, key: str, val: Any) -> None:
        self.watermarks[key] = val

    def get_watermark(self, key: str, default: Any = None) -> Any:
        return self.watermarks.get(key, default)

    def restart(self, expected_units: Optional[Sequence[str]] = None) -> None:
        if expected_units is not None:
            exp_set = set(expected_units)
            existing_set = set(self.units.keys())
            if len(exp_set) < len(existing_set):
                raise ShrinkingCoverageError(
                    f"Restart unit count {len(exp_set)} is smaller than existing units {len(existing_set)}"
                )
            missing = existing_set - exp_set
            if missing:
                raise ShrinkingCoverageError(
                    f"Restart would silently drop existing units: {sorted(missing)[:5]}"
                )
            for uid in expected_units:
                if uid not in self.units:
                    self.register_unit(uid, target=uid)

        for u in self.units.values():
            if u.status == WorkUnitStatus.IN_FLIGHT:
                u.status = WorkUnitStatus.PENDING
                u.started_at = None

        if self.total_units > 0 and self.completed_units == self.total_units:
            self.status = SubworkflowStatus.COMPLETED
        elif self.total_units > 0:
            self.status = SubworkflowStatus.IN_PROGRESS

    def to_dict(self) -> Dict[str, Any]:
        return {
            "subworkflow_id": self.subworkflow_id,
            "subworkflow_type": self.subworkflow_type,
            "status": self.status,
            "units": {k: u.to_dict() for k, u in self.units.items()},
            "obligations": {k: o.to_dict() for k, o in self.obligations.items()},
            "watermarks": self.watermarks,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DurableSubworkflow":
        type_map = {
            SubworkflowType.SNAPSHOT: SnapshotSubworkflow,
            SubworkflowType.INCREMENTAL_SNAPSHOT: IncrementalSnapshotSubworkflow,
            SubworkflowType.CDC: CDCSubworkflow,
        }
        target_cls = cls
        if cls is DurableSubworkflow:
            target_cls = type_map.get(d.get("subworkflow_type"), DurableSubworkflow)

        kwargs = {
            "subworkflow_id": d["subworkflow_id"],
            "status": d.get("status", SubworkflowStatus.NOT_STARTED),
            "watermarks": d.get("watermarks", {}),
            "metadata": d.get("metadata", {}),
        }
        if target_cls is DurableSubworkflow:
            kwargs["subworkflow_type"] = d.get("subworkflow_type", SubworkflowType.SNAPSHOT)

        sub = target_cls(**kwargs)
        for k, u in d.get("units", {}).items():
            sub.units[k] = WorkUnit.from_dict(u)
        for k, o in d.get("obligations", {}).items():
            sub.obligations[k] = Obligation.from_dict(o)
        return sub

    def save(self, path: Optional[Union[str, Path]] = None) -> None:
        target = Path(path)
        payload = json.dumps(self.to_dict(), indent=2)
        durable.write_atomic(target, payload)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "DurableSubworkflow":
        target = Path(path)
        with open(target, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)


class SnapshotSubworkflow(DurableSubworkflow):
    def __init__(
        self,
        subworkflow_id: str = "snapshot",
        status: str = SubworkflowStatus.NOT_STARTED,
        units: Optional[Dict[str, WorkUnit]] = None,
        obligations: Optional[Dict[str, Obligation]] = None,
        watermarks: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            subworkflow_id=subworkflow_id,
            subworkflow_type=SubworkflowType.SNAPSHOT,
            status=status,
            units=units or {},
            obligations=obligations or {},
            watermarks=watermarks or {},
            metadata=metadata or {},
        )

    def initialize_tables(self, tables: Sequence[str]) -> None:
        for t in tables:
            self.register_unit(unit_id=t, target=t)


class IncrementalSnapshotSubworkflow(DurableSubworkflow):
    def __init__(
        self,
        subworkflow_id: str = "incremental_snapshot",
        status: str = SubworkflowStatus.NOT_STARTED,
        units: Optional[Dict[str, WorkUnit]] = None,
        obligations: Optional[Dict[str, Obligation]] = None,
        watermarks: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            subworkflow_id=subworkflow_id,
            subworkflow_type=SubworkflowType.INCREMENTAL_SNAPSHOT,
            status=status,
            units=units or {},
            obligations=obligations or {},
            watermarks=watermarks or {},
            metadata=metadata or {},
        )

    def add_chunk(
        self,
        table: str,
        chunk_id: str,
        low_bound: Any,
        high_bound: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> WorkUnit:
        meta = dict(metadata or {})
        meta["low_bound"] = low_bound
        meta["high_bound"] = high_bound
        return self.register_unit(
            unit_id=chunk_id,
            target=table,
            key_range={"lo": low_bound, "hi": high_bound},
            metadata=meta,
        )

    def set_chunk_watermark(self, chunk_id: str, low_watermark: Any, high_watermark: Any) -> None:
        if chunk_id in self.units:
            self.units[chunk_id].metadata["low_watermark"] = low_watermark
            self.units[chunk_id].metadata["high_watermark"] = high_watermark

    def complete_chunk(self, chunk_id: str, rows_processed: int = 0) -> None:
        self.mark_completed(chunk_id, rows_processed=rows_processed)

    @property
    def pending_chunks(self) -> List[WorkUnit]:
        return [u for u in self.units.values() if u.status in (WorkUnitStatus.PENDING, WorkUnitStatus.IN_FLIGHT)]


class CDCSubworkflow(DurableSubworkflow):
    def __init__(
        self,
        subworkflow_id: str = "cdc",
        status: str = SubworkflowStatus.NOT_STARTED,
        units: Optional[Dict[str, WorkUnit]] = None,
        obligations: Optional[Dict[str, Obligation]] = None,
        watermarks: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            subworkflow_id=subworkflow_id,
            subworkflow_type=SubworkflowType.CDC,
            status=status,
            units=units or {},
            obligations=obligations or {},
            watermarks=watermarks or {},
            metadata=metadata or {},
        )

    @property
    def read_position(self) -> Any:
        return self.get_watermark("read_position")

    @property
    def persisted_position(self) -> Any:
        return self.get_watermark("persisted_position")

    @property
    def applied_position(self) -> Any:
        return self.get_watermark("applied_position")

    @property
    def proved_position(self) -> Any:
        return self.get_watermark("proved_position")

    def advance_read(self, pos: Any, partition: str = "default") -> None:
        self.set_watermark("read_position", pos)
        self.set_watermark(f"read_position_{partition}", pos)

    def advance_persisted(self, pos: Any, partition: str = "default") -> None:
        self.set_watermark("persisted_position", pos)
        self.set_watermark(f"persisted_position_{partition}", pos)

    def advance_applied(self, pos: Any, partition: str = "default") -> None:
        self.set_watermark("applied_position", pos)
        self.set_watermark(f"applied_position_{partition}", pos)

    def advance_proved(self, pos: Any, partition: str = "default") -> None:
        self.set_watermark("proved_position", pos)
        self.set_watermark(f"proved_position_{partition}", pos)

    def record_event_obligation(
        self,
        event_id: str,
        partition: str,
        position: Any,
        details: Optional[Dict[str, Any]] = None,
    ) -> Obligation:
        d = {"partition": partition, "position": position}
        if details:
            d.update(details)
        return self.add_obligation(
            kind="cdc_event",
            target=f"{partition}:{position}",
            details=d,
            obligation_id=event_id,
        )

    def discharge_event_obligation(self, event_id: str, token: Optional[str] = None) -> Obligation:
        return self.discharge_obligation(event_id, token=token)


class SnapshotWorkflowCoordinator:
    def __init__(
        self,
        state_dir: Optional[Union[str, Path]] = None,
        snapshot: Optional[SnapshotSubworkflow] = None,
        incremental: Optional[IncrementalSnapshotSubworkflow] = None,
        cdc: Optional[CDCSubworkflow] = None,
        active_phase: str = SubworkflowType.SNAPSHOT,
    ):
        self.state_dir = Path(state_dir) if state_dir else None
        self.snapshot = snapshot or SnapshotSubworkflow()
        self.incremental = incremental or IncrementalSnapshotSubworkflow()
        self.cdc = cdc or CDCSubworkflow()
        self.active_phase = active_phase

    def boot_snapshot(
        self,
        tables: Sequence[str],
        snapshot_id: Optional[str] = None,
        allow_wipe_other_phases: bool = False,
    ) -> None:
        if allow_wipe_other_phases and (self.incremental.has_pending_obligations or self.cdc.has_pending_obligations):
            raise ObligationWipeError("Boot snapshot cannot reset or wipe pending obligations of other phases")

        for t in tables:
            self.snapshot.register_unit(unit_id=t, target=t)

        if snapshot_id:
            self.snapshot.set_watermark("snapshot_id", snapshot_id)

        self.snapshot.status = SubworkflowStatus.IN_PROGRESS
        self.active_phase = SubworkflowType.SNAPSHOT

    def transition_phase(
        self,
        from_phase: str,
        to_phase: str,
        transfer_obligations: bool = True,
    ) -> None:
        phase_map = {
            SubworkflowType.SNAPSHOT: self.snapshot,
            SubworkflowType.INCREMENTAL_SNAPSHOT: self.incremental,
            SubworkflowType.CDC: self.cdc,
        }
        if from_phase not in phase_map or to_phase not in phase_map:
            raise InvalidPhaseTransitionError(f"Invalid transition from {from_phase} to {to_phase}")

        src = phase_map[from_phase]
        dst = phase_map[to_phase]

        if src.has_pending_obligations:
            if not transfer_obligations:
                raise ObligationWipeError(
                    f"Phase {from_phase} has undischarged pending obligations; cannot transition without transferring or discharging"
                )
            for obl in list(src.pending_obligations):
                transferred = Obligation(
                    obligation_id=obl.obligation_id,
                    phase=to_phase,
                    kind=obl.kind,
                    target=obl.target,
                    details=dict(obl.details, transferred_from=from_phase),
                )
                dst.obligations[transferred.obligation_id] = transferred
                obl.discharge(token=f"transferred_to_{to_phase}")

        if from_phase in (SubworkflowType.SNAPSHOT, SubworkflowType.INCREMENTAL_SNAPSHOT) and to_phase == SubworkflowType.CDC:
            snap_hw = src.get_watermark("high_watermark") or src.get_watermark("snapshot_lsn") or src.get_watermark("snapshot_watermark")
            cdc_pos = dst.read_position or dst.get_watermark("read_position")
            if snap_hw is not None and cdc_pos is not None:
                if _compare_watermarks(cdc_pos, snap_hw) < 0:
                    raise IncompatibleWatermarkError(
                        f"CDC start position {cdc_pos} is behind snapshot watermark {snap_hw}"
                    )

        self.active_phase = to_phase

    def restart_mode(self, mode: str, expected_units: Optional[Sequence[str]] = None) -> None:
        phase_map = {
            SubworkflowType.SNAPSHOT: self.snapshot,
            SubworkflowType.INCREMENTAL_SNAPSHOT: self.incremental,
            SubworkflowType.CDC: self.cdc,
        }
        if mode not in phase_map:
            raise ValueError(f"Unknown snapshot mode {mode}")
        phase_map[mode].restart(expected_units=expected_units)

    def save(self, dir_path: Optional[Union[str, Path]] = None) -> None:
        target_dir = Path(dir_path) if dir_path else self.state_dir
        if not target_dir:
            raise ValueError("No state directory specified for durable save")
        target_dir.mkdir(parents=True, exist_ok=True)

        manifest = {
            "active_phase": self.active_phase,
            "format_version": 1,
        }
        durable.write_atomic(target_dir / "manifest.json", json.dumps(manifest, indent=2))
        self.snapshot.save(target_dir / "snapshot_state.json")
        self.incremental.save(target_dir / "incremental_state.json")
        self.cdc.save(target_dir / "cdc_state.json")

    @classmethod
    def load(cls, dir_path: Union[str, Path]) -> "SnapshotWorkflowCoordinator":
        target_dir = Path(dir_path)
        with open(target_dir / "manifest.json", "r") as f:
            manifest = json.load(f)

        snap = SnapshotSubworkflow.from_dict(
            json.loads((target_dir / "snapshot_state.json").read_text())
        )
        inc = IncrementalSnapshotSubworkflow.from_dict(
            json.loads((target_dir / "incremental_state.json").read_text())
        )
        cdc = CDCSubworkflow.from_dict(
            json.loads((target_dir / "cdc_state.json").read_text())
        )

        return cls(
            state_dir=target_dir,
            snapshot=snap,
            incremental=inc,
            cdc=cdc,
            active_phase=manifest.get("active_phase", SubworkflowType.SNAPSHOT),
        )
