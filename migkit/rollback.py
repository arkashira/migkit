import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


class ExpandContractPhase(str, Enum):
    EXPAND = "EXPAND"
    CONTRACT = "CONTRACT"
    NEUTRAL = "NEUTRAL"


class RollbackStrategy(str, Enum):
    REVERSE_MIGRATION = "REVERSE_MIGRATION"
    FORWARD_FIX = "FORWARD_FIX"
    REFUSE_ROLLBACK = "REFUSE_ROLLBACK"


@dataclass
class AppVersionSpec:
    version_id: str
    supported_schema_versions: Set[str] = field(default_factory=set)
    required_columns: Dict[str, Set[str]] = field(default_factory=dict)
    forbidden_columns: Dict[str, Set[str]] = field(default_factory=dict)
    tolerates_unknown_columns: bool = True


@dataclass
class SchemaEvolutionStep:
    step_id: str
    from_version: str
    to_version: str
    phase: ExpandContractPhase = ExpandContractPhase.EXPAND
    forward_ddl: str = ""
    reverse_ddl: str = ""
    compensation_ddl: str = ""
    irreversible_data: bool = False
    irreversible_reason: str = ""
    affected_tables: Tuple[str, ...] = ()
    affected_columns: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    proved_reverse_preserves_writes: bool = False
    created_at: float = field(default_factory=time.time)


@dataclass(frozen=True)
class RecoveryArtifact:
    artifact_id: str
    step_id: str
    from_schema_version: str
    to_schema_version: str
    forward_ddl: str
    reverse_ddl: str
    compensation_ddl: str = ""
    phase: str = "EXPAND"
    checksum: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    proved_reverse_preserves_writes: bool = False

    def verify_checksum(self) -> bool:
        h = hashlib.sha256()
        h.update(self.step_id.encode("utf-8"))
        h.update(self.forward_ddl.encode("utf-8"))
        h.update(self.reverse_ddl.encode("utf-8"))
        return h.hexdigest() == self.checksum


@dataclass
class CurrentWriteState:
    table_writes: Dict[str, int] = field(default_factory=dict)
    column_writes: Dict[str, Dict[str, int]] = field(default_factory=dict)
    has_new_version_writes: bool = False
    active_writer_app_versions: Set[str] = field(default_factory=set)
    highest_lsn_or_marker: str = ""

    def has_writes_in(self, table: str, column: Optional[str] = None) -> bool:
        if column is not None:
            return self.column_writes.get(table, {}).get(column, 0) > 0
        if self.table_writes.get(table, 0) > 0:
            return True
        return self.has_new_version_writes


@dataclass
class RollbackDecision:
    strategy: RollbackStrategy
    allowed: bool
    target_app_version: str
    current_app_version: str
    current_schema_version: str
    target_schema_version: str
    action_plan: List[str] = field(default_factory=list)
    ddl_script: str = ""
    refusal_reasons: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    badge: str = "PASS"


class RecoveryArtifactStore:
    def __init__(self, storage_path: Optional[os.PathLike] = None):
        self.storage_path = Path(storage_path) if storage_path else None
        self._artifacts: Dict[str, RecoveryArtifact] = {}

    def _calc_checksum(self, step_id: str, forward_ddl: str, reverse_ddl: str) -> str:
        h = hashlib.sha256()
        h.update(step_id.encode("utf-8"))
        h.update(forward_ddl.encode("utf-8"))
        h.update(reverse_ddl.encode("utf-8"))
        return h.hexdigest()

    def record_evolution_step(
        self,
        step: SchemaEvolutionStep,
        proved_reverse_preserves_writes: bool = False,
        compensation_ddl: str = "",
    ) -> RecoveryArtifact:
        checksum = self._calc_checksum(step.step_id, step.forward_ddl, step.reverse_ddl)
        artifact_id = f"art-{step.step_id}-{checksum[:8]}"
        artifact = RecoveryArtifact(
            artifact_id=artifact_id,
            step_id=step.step_id,
            from_schema_version=step.from_version,
            to_schema_version=step.to_version,
            forward_ddl=step.forward_ddl,
            reverse_ddl=step.reverse_ddl,
            compensation_ddl=compensation_ddl or step.compensation_ddl,
            phase=step.phase.value if isinstance(step.phase, ExpandContractPhase) else str(step.phase),
            checksum=checksum,
            metadata={
                "affected_tables": list(step.affected_tables),
                "affected_columns": step.affected_columns,
                "irreversible_data": step.irreversible_data,
                "irreversible_reason": step.irreversible_reason,
            },
            created_at=step.created_at,
            proved_reverse_preserves_writes=proved_reverse_preserves_writes or step.proved_reverse_preserves_writes,
        )
        self._artifacts[step.step_id] = artifact
        return artifact

    def add(self, artifact: RecoveryArtifact) -> None:
        self._artifacts[artifact.step_id] = artifact

    def get(self, step_id: str) -> Optional[RecoveryArtifact]:
        return self._artifacts.get(step_id)

    def get_by_target_version(self, version: str) -> Optional[RecoveryArtifact]:
        for art in self._artifacts.values():
            if art.to_schema_version == version:
                return art
        return None

    def list_artifacts(self) -> List[RecoveryArtifact]:
        return list(self._artifacts.values())

    def save_to_disk(self) -> None:
        if not self.storage_path:
            return
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        data = {k: asdict(v) for k, v in self._artifacts.items()}
        with open(self.storage_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def load_from_disk(self) -> None:
        if not self.storage_path or not self.storage_path.exists():
            return
        with open(self.storage_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k, v in data.items():
            self._artifacts[k] = RecoveryArtifact(**v)

    def export_manifest(self) -> str:
        data = {k: asdict(v) for k, v in self._artifacts.items()}
        return json.dumps(data, sort_keys=True)

    def import_manifest(self, manifest_str: str) -> None:
        data = json.loads(manifest_str)
        for k, v in data.items():
            self._artifacts[k] = RecoveryArtifact(**v)


def _step_has_writes(
    step: Optional[SchemaEvolutionStep],
    artifact: Optional[RecoveryArtifact],
    write_state: CurrentWriteState,
) -> bool:
    affected_tables = ()
    affected_cols = {}
    if step:
        affected_tables = step.affected_tables
        affected_cols = step.affected_columns
    elif artifact:
        affected_tables = tuple(artifact.metadata.get("affected_tables", ()))
        affected_cols = artifact.metadata.get("affected_columns", {})

    if affected_tables:
        for t in affected_tables:
            if write_state.column_writes.get(t):
                cols = affected_cols.get(t, ())
                if cols:
                    if any(write_state.column_writes[t].get(c, 0) > 0 for c in cols):
                        return True
                elif write_state.table_writes.get(t, 0) > 0:
                    return True
            elif write_state.table_writes.get(t, 0) > 0:
                return True
        return False
    return write_state.has_new_version_writes or any(w > 0 for w in write_state.table_writes.values())


class AppRollbackCoordinator:
    def __init__(self, artifact_store: Optional[RecoveryArtifactStore] = None):
        self.artifact_store = artifact_store or RecoveryArtifactStore()
        self._app_versions: Dict[str, AppVersionSpec] = {}
        self._steps: Dict[str, SchemaEvolutionStep] = {}

    def register_app_version(self, app: AppVersionSpec) -> None:
        self._app_versions[app.version_id] = app

    def register_evolution_step(self, step: SchemaEvolutionStep) -> None:
        self._steps[step.step_id] = step
        self.artifact_store.record_evolution_step(step)

    def is_app_compatible(self, app_version_id: str, schema_version: str) -> Tuple[bool, str]:
        app = self._app_versions.get(app_version_id)
        if not app:
            return False, f"unregistered app version: {app_version_id}"

        artifact = self.artifact_store.get_by_target_version(schema_version)
        step = self._steps.get(artifact.step_id) if artifact else None

        affected_cols = {}
        if step:
            affected_cols = step.affected_columns
        elif artifact:
            affected_cols = artifact.metadata.get("affected_columns", {})

        if app.forbidden_columns:
            for table, forbidden in app.forbidden_columns.items():
                active_cols = set(affected_cols.get(table, ()))
                intersect = active_cols & forbidden
                if intersect:
                    return False, f"app {app_version_id} forbids columns present in schema: {intersect}"

        if app.supported_schema_versions and schema_version in app.supported_schema_versions:
            return True, "compatible"

        if artifact and artifact.phase == ExpandContractPhase.EXPAND.value:
            if app.tolerates_unknown_columns:
                if not app.supported_schema_versions or artifact.from_schema_version in app.supported_schema_versions:
                    return True, "compatible within expand window"
        return False, f"app {app_version_id} incompatible with schema {schema_version}"

    def can_shift_traffic(self, target_app_version: str, current_schema_version: str) -> Tuple[bool, str]:
        compat, msg = self.is_app_compatible(target_app_version, current_schema_version)
        if not compat:
            return False, f"cannot shift traffic: {msg}"
        return True, "traffic shift permitted"

    def evaluate_rollback(
        self,
        current_app_version: str,
        target_app_version: str,
        current_schema_version: str,
        target_schema_version: str,
        write_state: CurrentWriteState,
        dialect: str = "postgres",
    ) -> RollbackDecision:
        artifact = self.artifact_store.get_by_target_version(current_schema_version)
        target_app = self._app_versions.get(target_app_version)
        step = self._steps.get(artifact.step_id) if artifact else None
        if not step and artifact:
            step = SchemaEvolutionStep(
                step_id=artifact.step_id,
                from_version=artifact.from_schema_version,
                to_version=artifact.to_schema_version,
                forward_ddl=artifact.forward_ddl,
                reverse_ddl=artifact.reverse_ddl,
                affected_tables=tuple(artifact.metadata.get("affected_tables", ())),
                affected_columns=artifact.metadata.get("affected_columns", {}),
                irreversible_data=artifact.metadata.get("irreversible_data", False),
                irreversible_reason=artifact.metadata.get("irreversible_reason", ""),
            )

        traffic_ok, traffic_reason = self.can_shift_traffic(target_app_version, current_schema_version)
        has_writes = _step_has_writes(step, artifact, write_state)

        if not traffic_ok and has_writes:
            proved_reverse = artifact.proved_reverse_preserves_writes if artifact else False
            if not proved_reverse:
                return RollbackDecision(
                    strategy=RollbackStrategy.REFUSE_ROLLBACK,
                    allowed=False,
                    target_app_version=target_app_version,
                    current_app_version=current_app_version,
                    current_schema_version=current_schema_version,
                    target_schema_version=target_schema_version,
                    refusal_reasons=[
                        traffic_reason,
                        "destructive reverse migration would cause uncompensated data loss from live writes",
                    ],
                    badge="ACTION",
                )

        if has_writes:
            if artifact and artifact.proved_reverse_preserves_writes:
                return RollbackDecision(
                    strategy=RollbackStrategy.REVERSE_MIGRATION,
                    allowed=True,
                    target_app_version=target_app_version,
                    current_app_version=current_app_version,
                    current_schema_version=current_schema_version,
                    target_schema_version=target_schema_version,
                    action_plan=["execute proved write-preserving reverse migration", "shift traffic to target app"],
                    ddl_script=artifact.reverse_ddl,
                    badge="PASS",
                )

            comp_ddl = ""
            if step and target_app:
                comp_ddl = generate_compensation_ddl(step, current_schema_version, target_app, dialect=dialect)

            valid, violations = verify_compensation_ddl(comp_ddl, dialect=dialect) if comp_ddl else (True, [])
            if not valid:
                return RollbackDecision(
                    strategy=RollbackStrategy.REFUSE_ROLLBACK,
                    allowed=False,
                    target_app_version=target_app_version,
                    current_app_version=current_app_version,
                    current_schema_version=current_schema_version,
                    target_schema_version=target_schema_version,
                    refusal_reasons=violations,
                    badge="ACTION",
                )

            return RollbackDecision(
                strategy=RollbackStrategy.FORWARD_FIX,
                allowed=True,
                target_app_version=target_app_version,
                current_app_version=current_app_version,
                current_schema_version=current_schema_version,
                target_schema_version=target_schema_version,
                action_plan=[
                    "apply verified forward-fix compensation DDL",
                    "preserve all new-version writes without column/table drops",
                    "shift traffic safely to target app",
                ],
                ddl_script=comp_ddl,
                warnings=[
                    "new writes present in expanded schema; reverse DDL suppressed to prevent data loss",
                ],
                badge="REVIEW",
            )

        if artifact and artifact.metadata.get("irreversible_data") and not artifact.proved_reverse_preserves_writes:
            reason = artifact.metadata.get("irreversible_reason") or "irreversible schema evolution step"
            return RollbackDecision(
                strategy=RollbackStrategy.REFUSE_ROLLBACK,
                allowed=False,
                target_app_version=target_app_version,
                current_app_version=current_app_version,
                current_schema_version=current_schema_version,
                target_schema_version=target_schema_version,
                refusal_reasons=[f"irreversible data change refused: {reason}"],
                badge="ACTION",
            )

        if artifact and artifact.reverse_ddl:
            return RollbackDecision(
                strategy=RollbackStrategy.REVERSE_MIGRATION,
                allowed=True,
                target_app_version=target_app_version,
                current_app_version=current_app_version,
                current_schema_version=current_schema_version,
                target_schema_version=target_schema_version,
                action_plan=["execute clean reverse DDL", "shift traffic to target app"],
                ddl_script=artifact.reverse_ddl,
                badge="PASS",
            )

        return RollbackDecision(
            strategy=RollbackStrategy.REFUSE_ROLLBACK,
            allowed=False,
            target_app_version=target_app_version,
            current_app_version=current_app_version,
            current_schema_version=current_schema_version,
            target_schema_version=target_schema_version,
            refusal_reasons=["missing reverse recovery artifact and zero compensation path"],
            badge="ACTION",
        )


_UNSAFE_PATTERNS = [
    re.compile(r"\bdrop\s+table\b", re.IGNORECASE),
    re.compile(r"\bdrop\s+database\b", re.IGNORECASE),
    re.compile(r"\bdrop\s+schema\b", re.IGNORECASE),
    re.compile(r"\btruncate\b", re.IGNORECASE),
]


def generate_compensation_ddl(
    step: SchemaEvolutionStep,
    current_schema_version: str,
    target_app_spec: AppVersionSpec,
    dialect: str = "postgres",
) -> str:
    lines = []
    eng = dialect.lower()

    if step.affected_columns:
        for table, cols in step.affected_columns.items():
            for col in cols:
                fwd = step.forward_ddl.upper()
                if "NOT NULL" in fwd and col.upper() in fwd:
                    if "mysql" in eng or "mariadb" in eng:
                        lines.append(f"ALTER TABLE {table} MODIFY COLUMN {col} VARCHAR(255) NULL;")
                    else:
                        lines.append(f"ALTER TABLE {table} ALTER COLUMN {col} DROP NOT NULL;")
                else:
                    if "mysql" in eng or "mariadb" in eng:
                        lines.append(f"ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT '';")
                    else:
                        lines.append(f"ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT '';")

    if not lines and step.affected_tables:
        for table in step.affected_tables:
            lines.append(f"-- retain {table} without modification to preserve new writes")

    return "\n".join(lines)


def verify_compensation_ddl(ddl: str, dialect: str = "postgres") -> Tuple[bool, List[str]]:
    violations = []
    if not ddl or not ddl.strip():
        return True, []

    for stmt in ddl.split(";"):
        s = stmt.strip()
        if not s or s.startswith("--"):
            continue
        for pat in _UNSAFE_PATTERNS:
            if pat.search(s):
                violations.append(f"prohibited destructive operation detected: {s[:80]}")
    return len(violations) == 0, violations
