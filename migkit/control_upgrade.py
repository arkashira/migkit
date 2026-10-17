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
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from . import checkpoint as cp_mod
from . import durable


class StateSchemaVersion(int, Enum):
    V1 = 1
    V2 = 2
    V3 = 3

    @classmethod
    def parse(cls, val: Any) -> "StateSchemaVersion":
        if isinstance(val, StateSchemaVersion):
            return val
        if isinstance(val, int):
            return cls(val)
        s = str(val).strip().lower()
        if s.startswith("v"):
            s = s[1:]
        return cls(int(s))

    def __str__(self) -> str:
        return f"v{self.value}"

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, StateSchemaVersion):
            return self.value < other.value
        return self.value < int(other)

    def __le__(self, other: Any) -> bool:
        if isinstance(other, StateSchemaVersion):
            return self.value <= other.value
        return self.value <= int(other)

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, StateSchemaVersion):
            return self.value > other.value
        return self.value > int(other)

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, StateSchemaVersion):
            return self.value >= other.value
        return self.value >= int(other)


class InvariantViolationError(ValueError):
    pass


class UpgradeRollbackError(RuntimeError):
    pass


@dataclass
class HopDefinition:
    hop_id: str
    source: Any
    target: Any
    tables: List[str] = field(default_factory=list)
    options: Dict[str, Any] = field(default_factory=dict)
    transforms: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hop_id": self.hop_id,
            "source": self.source,
            "target": self.target,
            "tables": list(self.tables),
            "options": dict(self.options),
            "transforms": dict(self.transforms),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], hop_id: Optional[str] = None) -> "HopDefinition":
        hid = str(data.get("hop_id") or hop_id or "")
        return cls(
            hop_id=hid,
            source=data.get("source"),
            target=data.get("target"),
            tables=list(data.get("tables") or []),
            options=dict(data.get("options") or {}),
            transforms=dict(data.get("transforms") or {}),
        )


@dataclass
class AdapterCheckpoint:
    hop_id: str
    table: str
    adapter_type: str
    position: Dict[str, Any] = field(default_factory=dict)
    ranges_completed: List[Dict[str, Any]] = field(default_factory=list)
    pending_ranges: List[Dict[str, Any]] = field(default_factory=list)
    updated_at: float = field(default_factory=time.time)
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        if self.hop_id:
            return f"{self.hop_id}:{self.table}"
        return self.table

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hop_id": self.hop_id,
            "table": self.table,
            "adapter_type": self.adapter_type,
            "position": dict(self.position),
            "ranges_completed": list(self.ranges_completed),
            "pending_ranges": list(self.pending_ranges),
            "updated_at": self.updated_at,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], key: Optional[str] = None) -> "AdapterCheckpoint":
        hop_id = str(data.get("hop_id") or "")
        table = str(data.get("table") or "")
        if not table and key:
            if ":" in key:
                hop_id, _, table = key.partition(":")
            else:
                table = key

        adapter = str(data.get("adapter_type") or data.get("adapter") or "generic")
        pos = data.get("position")
        if pos is None:
            pos = {}
            for k in ("last_pk", "lsn", "binlog_file", "binlog_pos", "gtid", "resume_token", "cluster_time"):
                if k in data:
                    pos[k] = data[k]
        elif isinstance(pos, dict):
            pos = dict(pos)
        else:
            pos = {"val": pos}

        completed = list(data.get("ranges_completed") or [])
        if not completed and any(k in data for k in ("completed_rows", "checksum", "rows", "sum")):
            completed.append({
                "rows": data.get("completed_rows", data.get("rows", 0)),
                "checksum": data.get("checksum", data.get("sum")),
            })

        return cls(
            hop_id=hop_id,
            table=table,
            adapter_type=adapter,
            position=pos,
            ranges_completed=completed,
            pending_ranges=list(data.get("pending_ranges") or []),
            updated_at=float(data.get("updated_at") or time.time()),
            details=dict(data.get("details") or {}),
        )


@dataclass
class DurableControlState:
    schema_version: StateSchemaVersion = StateSchemaVersion.V2
    migkit_version: str = "0.2.0"
    state_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    hops: Dict[str, HopDefinition] = field(default_factory=dict)
    checkpoints: Dict[str, AdapterCheckpoint] = field(default_factory=dict)
    pending_work: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    digest: Optional[str] = None

    def compute_digest(self) -> str:
        payload = {
            "schema_version": int(self.schema_version),
            "hops": {k: v.to_dict() for k, v in sorted(self.hops.items())},
            "checkpoints": {k: v.to_dict() for k, v in sorted(self.checkpoints.items())},
            "pending_work": self.pending_work,
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "schema_version": int(self.schema_version),
            "migkit_version": self.migkit_version,
            "state_id": self.state_id,
            "digest": self.digest or self.compute_digest(),
            "hops": {k: v.to_dict() for k, v in self.hops.items()},
            "checkpoints": {k: v.to_dict() for k, v in self.checkpoints.items()},
            "pending_work": dict(self.pending_work),
            "metadata": dict(self.metadata),
        }
        if self.schema_version == StateSchemaVersion.V1:
            d["version"] = 1
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DurableControlState":
        raw_version = data.get("schema_version", data.get("version", 1))
        ver = StateSchemaVersion.parse(raw_version)

        raw_hops = data.get("hops") or {}
        hops = {}
        for hid, hval in raw_hops.items():
            if isinstance(hval, HopDefinition):
                hops[hid] = hval
            elif isinstance(hval, dict):
                hops[hid] = HopDefinition.from_dict(hval, hop_id=hid)

        raw_cps = data.get("checkpoints") or {}
        checkpoints = {}
        first_hop_id = next(iter(hops.keys()), "") if hops else ""
        for k, v in raw_cps.items():
            if isinstance(v, AdapterCheckpoint):
                checkpoints[k] = v
            elif isinstance(v, dict):
                cp = AdapterCheckpoint.from_dict(v, key=k)
                if not cp.hop_id and first_hop_id:
                    cp.hop_id = first_hop_id
                target_key = cp.key if ver > StateSchemaVersion.V1 and cp.hop_id else k
                checkpoints[target_key] = cp

        state = cls(
            schema_version=ver,
            migkit_version=str(data.get("migkit_version") or "0.2.0"),
            state_id=str(data.get("state_id") or uuid.uuid4().hex),
            hops=hops,
            checkpoints=checkpoints,
            pending_work=dict(data.get("pending_work") or {}),
            metadata=dict(data.get("metadata") or {}),
            digest=data.get("digest"),
        )
        if ver == StateSchemaVersion.V3 and not state.digest:
            state.digest = state.compute_digest()
        return state

    def copy(self) -> "DurableControlState":
        return DurableControlState.from_dict(self.to_dict())

    def save_directory(self, target_dir: Union[str, Path]) -> Path:
        target_dir = Path(target_dir)
        durable.private_dir(target_dir)
        payload = self.to_dict()
        state_file = target_dir / "control_state.json"
        durable.write_json(state_file, payload, indent=2)
        meta_file = target_dir / "meta.json"
        meta_payload = {
            "schema_version": int(self.schema_version),
            "state_id": self.state_id,
            "digest": payload.get("digest"),
            "migkit_version": self.migkit_version,
            "updated_at": time.time(),
        }
        durable.write_json(meta_file, meta_payload, indent=2)
        durable.sync_dir(target_dir)
        return target_dir

    @classmethod
    def load_directory(cls, target_dir: Union[str, Path]) -> "DurableControlState":
        target_dir = Path(target_dir)
        state_file = target_dir / "control_state.json"
        if state_file.exists():
            data = durable.read_state(state_file, kind="state")
            if data is not None:
                return cls.from_dict(data)

        meta_file = target_dir / "meta.json"
        if meta_file.exists():
            data = durable.read_state(meta_file, kind="state")
            if data is not None:
                return cls.from_dict(data)

        raise FileNotFoundError(f"no control state found under {target_dir}")


@dataclass
class UpgradeResult:
    success: bool
    from_version: StateSchemaVersion
    to_version: StateSchemaVersion
    backup_dir: Optional[Path] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryReport:
    recovered: bool
    action_taken: str
    active_version: Optional[StateSchemaVersion] = None
    hops_count: int = 0
    checkpoints_count: int = 0
    cleaned_paths: List[Path] = field(default_factory=list)


class StateInvariantValidator:
    @staticmethod
    def validate(before: DurableControlState, after: DurableControlState, target_version: StateSchemaVersion) -> None:
        if after.schema_version != target_version:
            raise InvariantViolationError(
                f"target schema version mismatch: expected {target_version}, got {after.schema_version}"
            )

        if before.hops and not after.hops:
            raise InvariantViolationError("hop configuration wiped during upgrade")

        for hid, bh in before.hops.items():
            if hid not in after.hops:
                raise InvariantViolationError(f"hop definition {hid} was lost during upgrade")
            ah = after.hops[hid]
            if set(bh.tables) != set(ah.tables):
                raise InvariantViolationError(f"hop {hid} table scope mutated during upgrade")

        if before.checkpoints and not after.checkpoints:
            raise InvariantViolationError("resume positions wiped during upgrade; full resync forced")

        def _has_meaningful_position(pos: Dict[str, Any]) -> bool:
            if not pos:
                return False
            for v in pos.values():
                if v is not None and v != "":
                    return True
            return False

        for b_key, b_cp in before.checkpoints.items():
            matched_cp = after.checkpoints.get(b_key)
            if not matched_cp:
                candidates = [
                    ac for ac in after.checkpoints.values()
                    if ac.table == b_cp.table and (not b_cp.hop_id or ac.hop_id == b_cp.hop_id)
                ]
                if candidates:
                    matched_cp = candidates[0]
            if not matched_cp:
                raise InvariantViolationError(f"checkpoint for {b_key} lost during upgrade; full resync forced")

            if _has_meaningful_position(b_cp.position) and not _has_meaningful_position(matched_cp.position):
                raise InvariantViolationError(
                    f"resume position for table {b_cp.table} was cleared; resync would be forced"
                )

        if target_version == StateSchemaVersion.V3:
            if not after.digest:
                raise InvariantViolationError("V3 state requires cryptographic digest")


class StateMigrator:
    def __init__(self):
        self._steps: Dict[Tuple[StateSchemaVersion, StateSchemaVersion], Callable[[DurableControlState], DurableControlState]] = {}
        self._install_builtin_steps()

    def register_step(
        self,
        from_ver: StateSchemaVersion,
        to_ver: StateSchemaVersion,
        forward_fn: Callable[[DurableControlState], DurableControlState],
        backward_fn: Optional[Callable[[DurableControlState], DurableControlState]] = None,
    ) -> None:
        self._steps[(from_ver, to_ver)] = forward_fn
        if backward_fn:
            self._steps[(to_ver, from_ver)] = backward_fn

    def _install_builtin_steps(self) -> None:
        self.register_step(StateSchemaVersion.V1, StateSchemaVersion.V2, self._v1_to_v2, self._v2_to_v1)
        self.register_step(StateSchemaVersion.V2, StateSchemaVersion.V3, self._v2_to_v3, self._v3_to_v2)

    @staticmethod
    def _v1_to_v2(state: DurableControlState) -> DurableControlState:
        st = state.copy()
        st.schema_version = StateSchemaVersion.V2
        new_cps = {}
        first_hop = next(iter(st.hops.keys()), "hop-0") if st.hops else "hop-0"
        for k, cp in st.checkpoints.items():
            hop_id = cp.hop_id or first_hop
            cp.hop_id = hop_id
            pos = dict(cp.position)
            if "last_pk" in pos and not isinstance(pos["last_pk"], dict):
                pos["last_pk"] = cp_mod.serialize_cursor(pos["last_pk"])
            cp.position = pos
            target_key = f"{hop_id}:{cp.table}"
            new_cps[target_key] = cp
        st.checkpoints = new_cps
        return st

    @staticmethod
    def _v2_to_v1(state: DurableControlState) -> DurableControlState:
        st = state.copy()
        st.schema_version = StateSchemaVersion.V1
        new_cps = {}
        for k, cp in st.checkpoints.items():
            pos = dict(cp.position)
            if "last_pk" in pos and isinstance(pos["last_pk"], dict) and "_type" in pos["last_pk"]:
                pos["last_pk"] = cp_mod.deserialize_cursor(pos["last_pk"])
            cp.position = pos
            new_cps[cp.table] = cp
        st.checkpoints = new_cps
        return st

    @staticmethod
    def _v2_to_v3(state: DurableControlState) -> DurableControlState:
        st = state.copy()
        st.schema_version = StateSchemaVersion.V3
        for cp in st.checkpoints.values():
            if "scoped_units" not in cp.details:
                scoped = []
                for idx, r in enumerate(cp.ranges_completed, 1):
                    scoped.append(f"{cp.table}::chunk:{r.get('chunk_id', idx)}")
                cp.details["scoped_units"] = scoped
        if not st.pending_work:
            for hid in st.hops:
                st.pending_work[hid] = {"active_units": [], "pending_ranges_count": 0}
        st.digest = st.compute_digest()
        return st

    @staticmethod
    def _v3_to_v2(state: DurableControlState) -> DurableControlState:
        st = state.copy()
        st.schema_version = StateSchemaVersion.V2
        st.digest = None
        for cp in st.checkpoints.values():
            cp.details.pop("scoped_units", None)
        return st

    def _find_path(self, current: StateSchemaVersion, target: StateSchemaVersion) -> List[Tuple[StateSchemaVersion, StateSchemaVersion]]:
        if current == target:
            return []
        queue = [(current, [])]
        visited = {current}
        while queue:
            node, path = queue.pop(0)
            if node == target:
                return path
            for (f, t) in self._steps:
                if f == node and t not in visited:
                    visited.add(t)
                    queue.append((t, path + [(f, t)]))
        raise ValueError(f"no migration path from {current} to {target}")

    def migrate_state(self, state: DurableControlState, target_version: StateSchemaVersion) -> DurableControlState:
        path = self._find_path(state.schema_version, target_version)
        current_state = state.copy()
        for (f, t) in path:
            step_fn = self._steps[(f, t)]
            next_state = step_fn(current_state)
            StateInvariantValidator.validate(current_state, next_state, t)
            current_state = next_state
        return current_state

    def migrate_directory(
        self,
        state_dir: Union[str, Path],
        target_version: StateSchemaVersion,
        keep_backup: bool = False,
        dry_run: bool = False,
    ) -> UpgradeResult:
        state_dir = Path(state_dir)
        token = uuid.uuid4().hex[:8]
        parent = state_dir.parent
        staging_dir = parent / f"{state_dir.name}.staging.{token}"
        backup_dir = parent / f"{state_dir.name}.backup.{token}"
        journal_file = parent / f".{state_dir.name}.upgrade_journal.{token}"

        before_state = DurableControlState.load_directory(state_dir)
        from_version = before_state.schema_version
        if from_version == target_version:
            return UpgradeResult(success=True, from_version=from_version, to_version=target_version)

        shutil.copytree(state_dir, staging_dir)
        try:
            journal_data = {
                "token": token,
                "state_dir": str(state_dir),
                "staging_dir": str(staging_dir),
                "backup_dir": str(backup_dir),
                "from_version": int(from_version),
                "target_version": int(target_version),
                "phase": "staging",
                "timestamp": time.time(),
            }
            durable.write_json(journal_file, journal_data)

            staged_before = DurableControlState.load_directory(staging_dir)
            staged_after = self.migrate_state(staged_before, target_version)
            staged_after.save_directory(staging_dir)

            journal_data["phase"] = "validating"
            durable.write_json(journal_file, journal_data)
            StateInvariantValidator.validate(before_state, staged_after, target_version)

            if dry_run:
                shutil.rmtree(staging_dir, ignore_errors=True)
                if journal_file.exists():
                    journal_file.unlink()
                return UpgradeResult(success=True, from_version=from_version, to_version=target_version)

            journal_data["phase"] = "swapping"
            durable.write_json(journal_file, journal_data)

            os.replace(state_dir, backup_dir)
            try:
                os.replace(staging_dir, state_dir)
            except BaseException:
                if backup_dir.exists() and not state_dir.exists():
                    os.replace(backup_dir, state_dir)
                raise

            durable.sync_dir(parent)

            if journal_file.exists():
                journal_file.unlink()

            if not keep_backup:
                shutil.rmtree(backup_dir, ignore_errors=True)
                backup_ret = None
            else:
                backup_ret = backup_dir

            return UpgradeResult(
                success=True,
                from_version=from_version,
                to_version=target_version,
                backup_dir=backup_ret,
            )
        except Exception:
            shutil.rmtree(staging_dir, ignore_errors=True)
            if journal_file.exists():
                journal_file.unlink()
            raise

    def rollback_directory(self, state_dir: Union[str, Path], backup_dir: Union[str, Path]) -> None:
        state_dir = Path(state_dir)
        backup_dir = Path(backup_dir)
        if not backup_dir.exists():
            raise UpgradeRollbackError(f"backup directory {backup_dir} does not exist")
        token = uuid.uuid4().hex[:8]
        tmp_target = state_dir.parent / f"{state_dir.name}.failed.{token}"
        if state_dir.exists():
            os.replace(state_dir, tmp_target)
        try:
            os.replace(backup_dir, state_dir)
            shutil.rmtree(tmp_target, ignore_errors=True)
            durable.sync_dir(state_dir.parent)
        except BaseException:
            if tmp_target.exists() and not state_dir.exists():
                os.replace(tmp_target, state_dir)
            raise


def is_upgrade_in_progress(state_dir: Union[str, Path]) -> bool:
    state_dir = Path(state_dir)
    parent = state_dir.parent
    name = state_dir.name
    for p in parent.glob(f"{name}.staging.*"):
        if p.is_dir():
            return True
    for p in parent.glob(f"{name}.backup.*"):
        if p.is_dir():
            return True
    for p in parent.glob(f".{name}.upgrade_journal.*"):
        if p.is_file():
            return True
    return False


def recover_interrupted_upgrade(state_dir: Union[str, Path]) -> RecoveryReport:
    state_dir = Path(state_dir)
    parent = state_dir.parent
    name = state_dir.name
    cleaned: List[Path] = []
    action_taken = "verified_clean"

    for p in sorted(parent.glob(f"{name}.staging.*")):
        shutil.rmtree(p, ignore_errors=True)
        cleaned.append(p)
        action_taken = "cleaned_stray_staging"

    backup_dirs = sorted(parent.glob(f"{name}.backup.*"))
    if not state_dir.exists() and backup_dirs:
        primary_backup = backup_dirs[-1]
        os.replace(primary_backup, state_dir)
        cleaned.append(primary_backup)
        action_taken = "restored_from_backup"
        for extra in backup_dirs[:-1]:
            shutil.rmtree(extra, ignore_errors=True)
            cleaned.append(extra)
    elif state_dir.exists() and backup_dirs:
        for extra in backup_dirs:
            shutil.rmtree(extra, ignore_errors=True)
            cleaned.append(extra)
        action_taken = "cleaned_abandoned_backups"

    for jf in parent.glob(f".{name}.upgrade_journal.*"):
        try:
            jf.unlink()
            cleaned.append(jf)
        except OSError:
            pass

    durable.sync_dir(parent)

    persisted = DurableControlState.load_directory(state_dir)
    return RecoveryReport(
        recovered=True,
        action_taken=action_taken,
        active_version=persisted.schema_version,
        hops_count=len(persisted.hops),
        checkpoints_count=len(persisted.checkpoints),
        cleaned_paths=cleaned,
    )


def export_state(
    state_or_dir: Union[str, Path, DurableControlState],
    export_path: Union[str, Path],
    target_version: Optional[StateSchemaVersion] = None,
) -> Path:
    if isinstance(state_or_dir, DurableControlState):
        state = state_or_dir.copy()
    else:
        state = DurableControlState.load_directory(state_or_dir)

    if target_version and state.schema_version != target_version:
        migrator = StateMigrator()
        state = migrator.migrate_state(state, target_version)

    export_path = Path(export_path)
    bundle = {
        "bundle_format": "migkit_control_state_export",
        "exported_at": time.time(),
        "state": state.to_dict(),
    }
    durable.write_json(export_path, bundle, indent=2)
    return export_path


def import_state(
    export_path: Union[str, Path],
    target_dir: Union[str, Path],
    target_version: Optional[StateSchemaVersion] = None,
) -> DurableControlState:
    export_path = Path(export_path)
    target_dir = Path(target_dir)

    raw = durable.read_state(export_path, kind="state")
    if not isinstance(raw, dict) or "state" not in raw:
        raise ValueError(f"invalid control state export bundle at {export_path}")

    state_dict = raw["state"]
    state = DurableControlState.from_dict(state_dict)

    if target_version and state.schema_version != target_version:
        migrator = StateMigrator()
        state = migrator.migrate_state(state, target_version)

    token = uuid.uuid4().hex[:8]
    staging_dir = target_dir.parent / f"{target_dir.name}.staging.{token}"
    state.save_directory(staging_dir)

    staged = DurableControlState.load_directory(staging_dir)
    StateInvariantValidator.validate(state, staged, staged.schema_version)

    if target_dir.exists():
        backup_dir = target_dir.parent / f"{target_dir.name}.backup.{token}"
        os.replace(target_dir, backup_dir)
        try:
            os.replace(staging_dir, target_dir)
            shutil.rmtree(backup_dir, ignore_errors=True)
        except BaseException:
            if backup_dir.exists() and not target_dir.exists():
                os.replace(backup_dir, target_dir)
            raise
    else:
        os.replace(staging_dir, target_dir)

    durable.sync_dir(target_dir.parent)
    return state
