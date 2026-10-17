import copy
import dataclasses
import enum
import re
import time
import uuid
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Tuple, Union

from migkit.ddl_shield import (
    CDCReplicationApplier,
    ColumnSchema,
    TransactionalFence,
    VerificationSliceManager,
)
from migkit.schema_drift import (
    BackfillTask,
    INT_RANKS,
    TEXT_RANKS,
    BLOB_RANKS,
    check_type_widening,
    map_type_to_dialect,
    parse_type_signature,
)


class UnrepresentableDDLError(Exception):
    def __init__(
        self,
        message: str,
        table: Optional[str] = None,
        column: Optional[str] = None,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
        reason: Optional[str] = None,
    ):
        super().__init__(message)
        self.message = message
        self.table = table
        self.column = column
        self.source_type = source_type
        self.target_type = target_type
        self.reason = reason


class CompatibilityType(str, enum.Enum):
    BACKWARD_COMPATIBLE = "BACKWARD_COMPATIBLE"
    FORWARD_COMPATIBLE = "FORWARD_COMPATIBLE"
    BREAKING_FENCED = "BREAKING_FENCED"
    REQUIRES_BACKFILL = "REQUIRES_BACKFILL"
    REQUIRES_RESYNC = "REQUIRES_RESYNC"
    UNREPRESENTABLE = "UNREPRESENTABLE"


@dataclasses.dataclass
class ColumnIdentity:
    column_id: str
    name: str
    generation: int = 1
    data_type: str = "TEXT"
    nullable: bool = True
    default: Optional[str] = None
    is_primary_key: bool = False
    is_identity: bool = False
    created_epoch: int = 0
    dropped_epoch: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnIdentity":
        return cls(**data)


@dataclasses.dataclass
class DDLOperation:
    op_type: str
    table: str
    column: Optional[str] = None
    new_column_name: Optional[str] = None
    data_type: Optional[str] = None
    old_data_type: Optional[str] = None
    nullable: Optional[bool] = None
    old_nullable: Optional[bool] = None
    default: Optional[str] = None
    old_default: Optional[str] = None
    primary_key_columns: Optional[List[str]] = None
    old_primary_key_columns: Optional[List[str]] = None
    lsn: Optional[Union[int, str]] = None
    timestamp: float = dataclasses.field(default_factory=time.time)
    raw_sql: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DDLOperation":
        return cls(**data)


@dataclasses.dataclass
class CompatibilityVerdict:
    compatibility_type: CompatibilityType
    is_compatible: bool
    requires_fence: bool
    requires_backfill: bool
    requires_slice_invalidation: bool
    requires_resync: bool
    affected_columns: List[str] = dataclasses.field(default_factory=list)
    reason: str = ""
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = dataclasses.asdict(self)
        d["compatibility_type"] = self.compatibility_type.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompatibilityVerdict":
        d = dict(data)
        d["compatibility_type"] = CompatibilityType(d["compatibility_type"])
        return cls(**d)


@dataclasses.dataclass
class SchemaEpochState:
    epoch: int
    table_name: str
    columns: Dict[str, ColumnIdentity] = dataclasses.field(default_factory=dict)
    columns_by_id: Dict[str, ColumnIdentity] = dataclasses.field(default_factory=dict)
    primary_key: List[str] = dataclasses.field(default_factory=list)
    applied_ddl: Optional[DDLOperation] = None
    parent_epoch: Optional[int] = None
    timestamp: float = dataclasses.field(default_factory=time.time)
    lsn: Optional[Union[int, str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "epoch": self.epoch,
            "table_name": self.table_name,
            "columns": {k: v.to_dict() for k, v in self.columns.items()},
            "columns_by_id": {k: v.to_dict() for k, v in self.columns_by_id.items()},
            "primary_key": list(self.primary_key),
            "applied_ddl": self.applied_ddl.to_dict() if self.applied_ddl else None,
            "parent_epoch": self.parent_epoch,
            "timestamp": self.timestamp,
            "lsn": self.lsn,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaEpochState":
        return cls(
            epoch=data["epoch"],
            table_name=data["table_name"],
            columns={k: ColumnIdentity.from_dict(v) for k, v in data.get("columns", {}).items()},
            columns_by_id={k: ColumnIdentity.from_dict(v) for k, v in data.get("columns_by_id", {}).items()},
            primary_key=data.get("primary_key", []),
            applied_ddl=DDLOperation.from_dict(data["applied_ddl"]) if data.get("applied_ddl") else None,
            parent_epoch=data.get("parent_epoch"),
            timestamp=data.get("timestamp", time.time()),
            lsn=data.get("lsn"),
        )


@dataclasses.dataclass
class FencedEpochPlan:
    plan_id: str
    target_table: str
    from_epoch: int
    to_epoch: int
    operation: DDLOperation
    verdict: CompatibilityVerdict
    target_statements: List[str] = dataclasses.field(default_factory=list)
    requires_fence: bool = True
    invalidated_evidence: List[str] = dataclasses.field(default_factory=list)
    backfill_tasks: List[BackfillTask] = dataclasses.field(default_factory=list)
    resync_required: bool = False
    status: str = "PENDING"
    applied_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "target_table": self.target_table,
            "from_epoch": self.from_epoch,
            "to_epoch": self.to_epoch,
            "operation": self.operation.to_dict(),
            "verdict": self.verdict.to_dict(),
            "target_statements": list(self.target_statements),
            "requires_fence": self.requires_fence,
            "invalidated_evidence": list(self.invalidated_evidence),
            "backfill_tasks": [dataclasses.asdict(b) for b in self.backfill_tasks],
            "resync_required": self.resync_required,
            "status": self.status,
            "applied_at": self.applied_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FencedEpochPlan":
        bt = [BackfillTask(**b) for b in data.get("backfill_tasks", [])]
        return cls(
            plan_id=data["plan_id"],
            target_table=data["target_table"],
            from_epoch=data["from_epoch"],
            to_epoch=data["to_epoch"],
            operation=DDLOperation.from_dict(data["operation"]),
            verdict=CompatibilityVerdict.from_dict(data["verdict"]),
            target_statements=data.get("target_statements", []),
            requires_fence=data.get("requires_fence", True),
            invalidated_evidence=data.get("invalidated_evidence", []),
            backfill_tasks=bt,
            resync_required=data.get("resync_required", False),
            status=data.get("status", "PENDING"),
            applied_at=data.get("applied_at"),
        )


@dataclasses.dataclass
class ActiveSnapshotContext:
    snapshot_id: str
    table: str
    start_epoch: int
    status: str = "ACTIVE"
    created_at: float = dataclasses.field(default_factory=time.time)
    completed_at: Optional[float] = None
    concurrent_ddls: List[DDLOperation] = dataclasses.field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "table": self.table,
            "start_epoch": self.start_epoch,
            "status": self.status,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "concurrent_ddls": [op.to_dict() for op in self.concurrent_ddls],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ActiveSnapshotContext":
        return cls(
            snapshot_id=data["snapshot_id"],
            table=data["table"],
            start_epoch=data["start_epoch"],
            status=data.get("status", "ACTIVE"),
            created_at=data.get("created_at", time.time()),
            completed_at=data.get("completed_at"),
            concurrent_ddls=[DDLOperation.from_dict(op) for op in data.get("concurrent_ddls", [])],
        )


class DDLCompatibilityMatrix:
    def __init__(self, target_dialect: str = "postgres"):
        self.target_dialect = target_dialect.lower()

    def check_type_transition(
        self,
        current_type: str,
        new_type: str,
    ) -> Tuple[bool, bool, Optional[str]]:
        is_widening, widening_reason = check_type_widening(current_type, new_type)
        if is_widening:
            return True, False, widening_reason

        cur_base, cur_params, cur_unsigned = parse_type_signature(current_type)
        new_base, new_params, new_unsigned = parse_type_signature(new_type)

        if cur_base == new_base and cur_params == new_params and cur_unsigned == new_unsigned:
            return True, False, "identical type"

        if cur_base in INT_RANKS and new_base in INT_RANKS:
            if INT_RANKS[new_base] < INT_RANKS[cur_base]:
                return False, True, f"lossy integer narrowing: {current_type} -> {new_type}"
            if INT_RANKS[new_base] == INT_RANKS[cur_base] and cur_unsigned and not new_unsigned:
                return False, True, f"lossy unsigned to signed narrowing: {current_type} -> {new_type}"

        if cur_base in TEXT_RANKS and new_base in TEXT_RANKS:
            if TEXT_RANKS[new_base] < TEXT_RANKS[cur_base]:
                return False, True, f"lossy text narrowing: {current_type} -> {new_type}"
            if cur_base in ("CHAR", "VARCHAR") and new_base in ("CHAR", "VARCHAR"):
                c_len = cur_params[0] if cur_params else (255 if cur_base == "VARCHAR" else 1)
                n_len = new_params[0] if new_params else (255 if new_base == "VARCHAR" else 1)
                if n_len < c_len:
                    return False, True, f"lossy varchar length narrowing: {c_len} -> {n_len}"
            if cur_base in ("TEXT", "MEDIUMTEXT", "LONGTEXT") and new_base in ("VARCHAR", "CHAR"):
                return False, True, f"lossy text to bounded string narrowing: {current_type} -> {new_type}"

        if cur_base in ("DECIMAL", "NUMERIC") and new_base in ("DECIMAL", "NUMERIC"):
            if cur_params and new_params:
                cp, cs = cur_params if len(cur_params) == 2 else (cur_params[0], 0)
                np, ns = new_params if len(new_params) == 2 else (new_params[0], 0)
                if np < cp or ns < cs:
                    return False, True, f"lossy decimal precision/scale narrowing: {current_type} -> {new_type}"
            elif not cur_params and new_params:
                return False, True, f"lossy unbounded to bounded decimal narrowing: {current_type} -> {new_type}"

        if cur_base in ("FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION") and new_base in (
            "FLOAT",
            "REAL",
            "DOUBLE",
            "DOUBLE PRECISION",
        ):
            if cur_base in ("DOUBLE", "DOUBLE PRECISION") and new_base in ("FLOAT", "REAL"):
                return False, True, f"lossy floating point narrowing: {current_type} -> {new_type}"

        if cur_base in TEXT_RANKS and new_base in INT_RANKS:
            return False, True, f"lossy text to integer coercion: {current_type} -> {new_type}"
        if cur_base in BLOB_RANKS and (new_base in INT_RANKS or new_base in TEXT_RANKS):
            return False, True, f"lossy binary to structured coercion: {current_type} -> {new_type}"
        if cur_base == "JSON" and new_base in INT_RANKS:
            return False, True, f"lossy json to integer coercion: {current_type} -> {new_type}"

        return False, True, f"unsupported or lossy type conversion: {current_type} -> {new_type}"

    def classify(
        self,
        op: DDLOperation,
        current_columns: Dict[str, ColumnIdentity],
        primary_key: Optional[List[str]] = None,
        strict: bool = True,
    ) -> CompatibilityVerdict:
        table_name = op.table
        op_type = op.op_type.upper()

        if op_type == "ADD_COLUMN":
            col_name = op.column or ""
            if col_name in current_columns and current_columns[col_name].dropped_epoch is None:
                err_msg = f"Column '{col_name}' already exists in table '{table_name}'"
                if strict:
                    raise UnrepresentableDDLError(err_msg, table=table_name, column=col_name, reason=err_msg)
                return CompatibilityVerdict(
                    compatibility_type=CompatibilityType.UNREPRESENTABLE,
                    is_compatible=False,
                    requires_fence=True,
                    requires_backfill=False,
                    requires_slice_invalidation=True,
                    requires_resync=False,
                    affected_columns=[col_name],
                    reason=err_msg,
                    error=err_msg,
                )

            if op.nullable is False and op.default is None:
                err_msg = f"Adding NOT NULL without default to table '{table_name}' column '{col_name}' is unrepresentable"
                if strict:
                    raise UnrepresentableDDLError(err_msg, table=table_name, column=col_name, reason=err_msg)
                return CompatibilityVerdict(
                    compatibility_type=CompatibilityType.UNREPRESENTABLE,
                    is_compatible=False,
                    requires_fence=True,
                    requires_backfill=False,
                    requires_slice_invalidation=True,
                    requires_resync=False,
                    affected_columns=[col_name],
                    reason=err_msg,
                    error=err_msg,
                )

            if op.default is not None:
                return CompatibilityVerdict(
                    compatibility_type=CompatibilityType.REQUIRES_BACKFILL,
                    is_compatible=True,
                    requires_fence=True,
                    requires_backfill=True,
                    requires_slice_invalidation=True,
                    requires_resync=False,
                    affected_columns=[col_name],
                    reason=f"ADD column with default requires backfill and fence: {col_name}",
                )

            return CompatibilityVerdict(
                compatibility_type=CompatibilityType.BACKWARD_COMPATIBLE,
                is_compatible=True,
                requires_fence=True,
                requires_backfill=False,
                requires_slice_invalidation=True,
                requires_resync=False,
                affected_columns=[col_name],
                reason=f"ADD nullable column without default: {col_name}",
            )

        elif op_type == "DROP_COLUMN":
            col_name = op.column or ""
            if primary_key and col_name in primary_key:
                return CompatibilityVerdict(
                    compatibility_type=CompatibilityType.REQUIRES_RESYNC,
                    is_compatible=True,
                    requires_fence=True,
                    requires_backfill=False,
                    requires_slice_invalidation=True,
                    requires_resync=True,
                    affected_columns=[col_name],
                    reason=f"DROP primary key column requires resync: {col_name}",
                )
            return CompatibilityVerdict(
                compatibility_type=CompatibilityType.BREAKING_FENCED,
                is_compatible=True,
                requires_fence=True,
                requires_backfill=False,
                requires_slice_invalidation=True,
                requires_resync=False,
                affected_columns=[col_name],
                reason=f"DROP column fences stream and invalidates table slices: {col_name}",
            )

        elif op_type == "RENAME_COLUMN":
            col_name = op.column or ""
            new_col_name = op.new_column_name or ""
            if new_col_name in current_columns and current_columns[new_col_name].dropped_epoch is None:
                err_msg = f"Target column '{new_col_name}' already exists in table '{table_name}'"
                if strict:
                    raise UnrepresentableDDLError(err_msg, table=table_name, column=new_col_name, reason=err_msg)
                return CompatibilityVerdict(
                    compatibility_type=CompatibilityType.UNREPRESENTABLE,
                    is_compatible=False,
                    requires_fence=True,
                    requires_backfill=False,
                    requires_slice_invalidation=True,
                    requires_resync=False,
                    affected_columns=[col_name, new_col_name],
                    reason=err_msg,
                    error=err_msg,
                )
            return CompatibilityVerdict(
                compatibility_type=CompatibilityType.BREAKING_FENCED,
                is_compatible=True,
                requires_fence=True,
                requires_backfill=False,
                requires_slice_invalidation=True,
                requires_resync=False,
                affected_columns=[col_name, new_col_name],
                reason=f"Column rename identity preservation: {col_name} -> {new_col_name}",
            )

        elif op_type in ("MODIFY_COLUMN", "ALTER_TYPE"):
            col_name = op.column or ""
            cur_col = current_columns.get(col_name)
            source_type = op.old_data_type or (cur_col.data_type if cur_col else "TEXT")
            target_type = op.data_type or source_type

            is_ok, is_lossy, reason = self.check_type_transition(source_type, target_type)
            if is_lossy:
                err_msg = reason or f"Lossy type conversion from {source_type} to {target_type}"
                if strict:
                    raise UnrepresentableDDLError(
                        err_msg,
                        table=table_name,
                        column=col_name,
                        source_type=source_type,
                        target_type=target_type,
                        reason=reason,
                    )
                return CompatibilityVerdict(
                    compatibility_type=CompatibilityType.UNREPRESENTABLE,
                    is_compatible=False,
                    requires_fence=True,
                    requires_backfill=False,
                    requires_slice_invalidation=True,
                    requires_resync=False,
                    affected_columns=[col_name],
                    reason=err_msg,
                    error=err_msg,
                )

            if op.nullable is False and cur_col and cur_col.nullable is True and op.default is None:
                err_msg = f"Column '{col_name}' altered to NOT NULL without default on table '{table_name}'"
                if strict:
                    raise UnrepresentableDDLError(err_msg, table=table_name, column=col_name, reason=err_msg)
                return CompatibilityVerdict(
                    compatibility_type=CompatibilityType.UNREPRESENTABLE,
                    is_compatible=False,
                    requires_fence=True,
                    requires_backfill=False,
                    requires_slice_invalidation=True,
                    requires_resync=False,
                    affected_columns=[col_name],
                    reason=err_msg,
                    error=err_msg,
                )

            return CompatibilityVerdict(
                compatibility_type=CompatibilityType.BACKWARD_COMPATIBLE,
                is_compatible=True,
                requires_fence=True,
                requires_backfill=False,
                requires_slice_invalidation=True,
                requires_resync=False,
                affected_columns=[col_name],
                reason=f"Type modification compatible: {source_type} -> {target_type}",
            )

        elif op_type == "CHANGE_PK":
            new_pks = op.primary_key_columns
            old_pks = op.old_primary_key_columns or primary_key or []
            if not new_pks or len(new_pks) == 0:
                err_msg = f"Primary key removed without replacement on table '{table_name}'"
                if strict:
                    raise UnrepresentableDDLError(err_msg, table=table_name, reason=err_msg)
                return CompatibilityVerdict(
                    compatibility_type=CompatibilityType.UNREPRESENTABLE,
                    is_compatible=False,
                    requires_fence=True,
                    requires_backfill=False,
                    requires_slice_invalidation=True,
                    requires_resync=True,
                    affected_columns=old_pks,
                    reason=err_msg,
                    error=err_msg,
                )

            affected = list(set(new_pks + old_pks))
            return CompatibilityVerdict(
                compatibility_type=CompatibilityType.REQUIRES_RESYNC,
                is_compatible=True,
                requires_fence=True,
                requires_backfill=False,
                requires_slice_invalidation=True,
                requires_resync=True,
                affected_columns=affected,
                reason=f"Primary key changed from {old_pks} to {new_pks}",
            )

        return CompatibilityVerdict(
            compatibility_type=CompatibilityType.BREAKING_FENCED,
            is_compatible=True,
            requires_fence=True,
            requires_backfill=False,
            requires_slice_invalidation=True,
            requires_resync=False,
            affected_columns=[op.column] if op.column else [],
            reason=f"Default fenced DDL: {op_type}",
        )


class SchemaEpochPlanner:
    def __init__(
        self,
        target_dialect: str = "postgres",
        slice_manager: Optional[VerificationSliceManager] = None,
        applier: Optional[CDCReplicationApplier] = None,
        target_executor: Optional[Callable[[str], Any]] = None,
    ):
        self.target_dialect = target_dialect.lower()
        self.slice_manager = slice_manager
        self.applier = applier
        self.target_executor = target_executor
        self.compatibility_matrix = DDLCompatibilityMatrix(target_dialect=self.target_dialect)

        self.table_epochs: Dict[str, int] = {}
        self.epoch_history: Dict[str, Dict[int, SchemaEpochState]] = {}
        self.column_lineage: Dict[str, Dict[str, List[ColumnIdentity]]] = {}
        self.column_rename_map: Dict[str, Dict[str, str]] = {}
        self.active_snapshots: Dict[str, ActiveSnapshotContext] = {}
        self.applied_plans: Dict[str, FencedEpochPlan] = {}

    def register_table(
        self,
        name: str,
        columns: List[ColumnSchema],
        primary_key: Optional[List[str]] = None,
        initial_epoch: int = 1,
    ) -> SchemaEpochState:
        pk = list(primary_key) if primary_key else []
        col_map: Dict[str, ColumnIdentity] = {}
        col_by_id: Dict[str, ColumnIdentity] = {}
        lineage_map: Dict[str, List[ColumnIdentity]] = {}

        for c in columns:
            cid = f"col_{name}_{c.name}_g1_{uuid.uuid4().hex[:6]}"
            col_ident = ColumnIdentity(
                column_id=cid,
                name=c.name,
                generation=1,
                data_type=c.data_type,
                nullable=c.nullable,
                default=c.default,
                is_primary_key=c.is_primary_key or (c.name in pk),
                is_identity=c.is_identity,
                created_epoch=initial_epoch,
            )
            col_map[c.name] = col_ident
            col_by_id[cid] = col_ident
            lineage_map[c.name] = [col_ident]

        state = SchemaEpochState(
            epoch=initial_epoch,
            table_name=name,
            columns=col_map,
            columns_by_id=col_by_id,
            primary_key=pk,
            applied_ddl=None,
            parent_epoch=None,
        )

        self.table_epochs[name] = initial_epoch
        if name not in self.epoch_history:
            self.epoch_history[name] = {}
        self.epoch_history[name][initial_epoch] = state
        self.column_lineage[name] = lineage_map
        self.column_rename_map[name] = {}
        return state

    def get_current_epoch(self, table: str) -> int:
        return self.table_epochs.get(table, 1)

    def get_current_schema(self, table: str) -> SchemaEpochState:
        ep = self.get_current_epoch(table)
        return self.epoch_history[table][ep]

    def get_schema_at_epoch(self, table: str, epoch: int) -> SchemaEpochState:
        if table in self.epoch_history and epoch in self.epoch_history[table]:
            return self.epoch_history[table][epoch]
        raise KeyError(f"No schema state recorded for table '{table}' at epoch {epoch}")

    def generate_target_sql(self, op: DDLOperation, curr_state: SchemaEpochState) -> List[str]:
        target = self.target_dialect
        table = op.table
        op_type = op.op_type.upper()
        statements: List[str] = []

        if op_type == "ADD_COLUMN":
            col = op.column or ""
            raw_type = op.data_type or "TEXT"
            dtype = map_type_to_dialect(raw_type, target)
            default_part = f" DEFAULT {op.default}" if op.default is not None else ""
            null_part = " NOT NULL" if op.nullable is False else ""

            if target in ("postgres", "postgresql", "sqlite"):
                statements.append(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {dtype}{default_part}{null_part}')
            elif target in ("mysql", "mariadb"):
                statements.append(f"ALTER TABLE `{table}` ADD COLUMN `{col}` {dtype}{default_part}{null_part}")
            else:
                statements.append(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}{default_part}{null_part}")

        elif op_type == "DROP_COLUMN":
            col = op.column or ""
            if target in ("postgres", "postgresql", "sqlite"):
                statements.append(f'ALTER TABLE "{table}" DROP COLUMN "{col}"')
            elif target in ("mysql", "mariadb"):
                statements.append(f"ALTER TABLE `{table}` DROP COLUMN `{col}`")
            else:
                statements.append(f"ALTER TABLE {table} DROP COLUMN {col}")

        elif op_type == "RENAME_COLUMN":
            old_col = op.column or ""
            new_col = op.new_column_name or ""
            if target in ("postgres", "postgresql", "sqlite"):
                statements.append(f'ALTER TABLE "{table}" RENAME COLUMN "{old_col}" TO "{new_col}"')
            elif target in ("mysql", "mariadb"):
                statements.append(f"ALTER TABLE `{table}` RENAME COLUMN `{old_col}` TO `{new_col}`")
            else:
                statements.append(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")

        elif op_type in ("MODIFY_COLUMN", "ALTER_TYPE"):
            col = op.column or ""
            raw_type = op.data_type or "TEXT"
            dtype = map_type_to_dialect(raw_type, target)
            if target in ("postgres", "postgresql"):
                statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col}" TYPE {dtype} USING "{col}"::{dtype}')
            elif target in ("mysql", "mariadb"):
                statements.append(f"ALTER TABLE `{table}` MODIFY COLUMN `{col}` {dtype}")
            else:
                statements.append(f"ALTER TABLE {table} MODIFY COLUMN {col} {dtype}")

        elif op_type == "CHANGE_PK":
            new_pks = op.primary_key_columns or []
            if target in ("postgres", "postgresql"):
                statements.append(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{table}_pkey"')
                cols_quoted = ", ".join(f'"{c}"' for c in new_pks)
                statements.append(f'ALTER TABLE "{table}" ADD PRIMARY KEY ({cols_quoted})')
            elif target in ("mysql", "mariadb"):
                statements.append(f"ALTER TABLE `{table}` DROP PRIMARY KEY")
                cols_quoted = ", ".join(f"`{c}`" for c in new_pks)
                statements.append(f"ALTER TABLE `{table}` ADD PRIMARY KEY ({cols_quoted})")
            else:
                cols_quoted = ", ".join(new_pks)
                statements.append(f"ALTER TABLE {table} ADD PRIMARY KEY ({cols_quoted})")

        return statements

    def plan_ddl(self, op: DDLOperation, strict: bool = True) -> FencedEpochPlan:
        table = op.table
        if table not in self.table_epochs:
            raise KeyError(f"Table '{table}' is not registered in schema epoch planner")

        curr_epoch = self.table_epochs[table]
        curr_state = self.epoch_history[table][curr_epoch]

        verdict = self.compatibility_matrix.classify(
            op=op,
            current_columns=curr_state.columns,
            primary_key=curr_state.primary_key,
            strict=strict,
        )

        target_stmts = self.generate_target_sql(op, curr_state)
        backfill_tasks: List[BackfillTask] = []
        if verdict.requires_backfill and op.op_type.upper() == "ADD_COLUMN" and op.column:
            bt = BackfillTask(
                task_id=f"bf_{table}_{op.column}_{uuid.uuid4().hex[:8]}",
                table=table,
                column_name=op.column,
                default_value=op.default,
                key_column=curr_state.primary_key[0] if curr_state.primary_key else "id",
            )
            backfill_tasks.append(bt)

        invalidated_evidence: List[str] = []
        if self.slice_manager:
            for s in self.slice_manager.get_valid_slices(table=table):
                invalidated_evidence.append(s.slice_id)

        plan = FencedEpochPlan(
            plan_id=f"plan_{table}_e{curr_epoch + 1}_{uuid.uuid4().hex[:8]}",
            target_table=table,
            from_epoch=curr_epoch,
            to_epoch=curr_epoch + 1,
            operation=op,
            verdict=verdict,
            target_statements=target_stmts,
            requires_fence=verdict.requires_fence,
            invalidated_evidence=invalidated_evidence,
            backfill_tasks=backfill_tasks,
            resync_required=verdict.requires_resync,
        )
        return plan

    def apply_plan(
        self,
        plan: FencedEpochPlan,
        fence: Optional[TransactionalFence] = None,
        executor: Optional[Callable[[str], Any]] = None,
    ) -> FencedEpochPlan:
        table = plan.target_table
        curr_epoch = self.table_epochs.get(table, 0)
        if curr_epoch >= plan.to_epoch:
            plan.status = "SKIPPED_ALREADY_APPLIED"
            return plan

        if self.applier:
            self.applier.pause()

        exec_fn = executor or self.target_executor
        if fence:
            with fence:
                for stmt in plan.target_statements:
                    fence.execute(stmt)
        elif exec_fn:
            for stmt in plan.target_statements:
                exec_fn(stmt)

        prev_state = self.epoch_history[table][curr_epoch]
        new_cols = dict(prev_state.columns)
        new_cols_by_id = dict(prev_state.columns_by_id)
        new_pk = list(prev_state.primary_key)
        op = plan.operation
        op_type = op.op_type.upper()

        if op_type == "ADD_COLUMN":
            col_name = op.column or ""
            history_list = self.column_lineage[table].setdefault(col_name, [])
            gen = len(history_list) + 1
            cid = f"col_{table}_{col_name}_g{gen}_{uuid.uuid4().hex[:6]}"
            new_col = ColumnIdentity(
                column_id=cid,
                name=col_name,
                generation=gen,
                data_type=op.data_type or "TEXT",
                nullable=True if op.nullable is None else op.nullable,
                default=op.default,
                is_primary_key=col_name in new_pk,
                created_epoch=plan.to_epoch,
            )
            new_cols[col_name] = new_col
            new_cols_by_id[cid] = new_col
            history_list.append(new_col)

        elif op_type == "DROP_COLUMN":
            col_name = op.column or ""
            if col_name in new_cols:
                dropping = new_cols.pop(col_name)
                dropping_updated = dataclasses.replace(dropping, dropped_epoch=plan.to_epoch)
                new_cols_by_id[dropping.column_id] = dropping_updated
                lineage = self.column_lineage[table].get(col_name, [])
                for i, c in enumerate(lineage):
                    if c.column_id == dropping.column_id:
                        lineage[i] = dropping_updated

        elif op_type == "RENAME_COLUMN":
            old_name = op.column or ""
            new_name = op.new_column_name or ""
            if old_name in new_cols:
                renaming = new_cols.pop(old_name)
                renamed = dataclasses.replace(renaming, name=new_name)
                new_cols[new_name] = renamed
                new_cols_by_id[renamed.column_id] = renamed
                self.column_rename_map[table][old_name] = new_name
                history_list = self.column_lineage[table].setdefault(new_name, [])
                history_list.append(renamed)

        elif op_type in ("MODIFY_COLUMN", "ALTER_TYPE"):
            col_name = op.column or ""
            if col_name in new_cols:
                modifying = new_cols[col_name]
                mod_updated = dataclasses.replace(
                    modifying,
                    data_type=op.data_type or modifying.data_type,
                    nullable=op.nullable if op.nullable is not None else modifying.nullable,
                    default=op.default if op.default is not None else modifying.default,
                )
                new_cols[col_name] = mod_updated
                new_cols_by_id[mod_updated.column_id] = mod_updated

        elif op_type == "CHANGE_PK":
            new_pk = list(op.primary_key_columns or [])
            for c_name, c_ident in new_cols.items():
                is_pk = c_name in new_pk
                if c_ident.is_primary_key != is_pk:
                    upd = dataclasses.replace(c_ident, is_primary_key=is_pk)
                    new_cols[c_name] = upd
                    new_cols_by_id[upd.column_id] = upd

        new_state = SchemaEpochState(
            epoch=plan.to_epoch,
            table_name=table,
            columns=new_cols,
            columns_by_id=new_cols_by_id,
            primary_key=new_pk,
            applied_ddl=op,
            parent_epoch=curr_epoch,
            lsn=op.lsn,
        )

        self.table_epochs[table] = plan.to_epoch
        self.epoch_history[table][plan.to_epoch] = new_state

        actual_invalidated: List[str] = []
        if self.slice_manager:
            actual_invalidated = self.slice_manager.invalidate_table(
                table=table,
                reason=f"SCHEMA_EPOCH_{plan.to_epoch}:{op_type}",
            )
        plan.invalidated_evidence = actual_invalidated

        if self.applier:
            self.applier.resume()

        plan.status = "COMMITTED"
        plan.applied_at = time.time()
        self.applied_plans[plan.plan_id] = plan
        return plan

    def decode_change(self, change: Dict[str, Any]) -> Dict[str, Any]:
        table = change.get("table", "")
        if table not in self.table_epochs:
            return change

        event_epoch = change.get("epoch")
        if event_epoch is None:
            event_epoch = self.table_epochs[table]

        schema_at_epoch = self.get_schema_at_epoch(table, event_epoch)
        current_state = self.get_current_schema(table)

        raw_data = dict(change.get("data") or {})
        raw_key = dict(change.get("key") or {})
        decoded_data: Dict[str, Any] = {}
        col_idents: Dict[str, ColumnIdentity] = {}
        is_dropped_generation = False

        rename_map = self.column_rename_map.get(table, {})

        for col_name, val in raw_data.items():
            mapped_name = rename_map.get(col_name, col_name)
            if col_name in schema_at_epoch.columns:
                c_ident = schema_at_epoch.columns[col_name]
                col_idents[mapped_name] = c_ident

                if mapped_name in current_state.columns:
                    curr_col = current_state.columns[mapped_name]
                    if curr_col.generation != c_ident.generation:
                        is_dropped_generation = True
                        decoded_data[mapped_name] = None
                        continue
                decoded_data[mapped_name] = val
            else:
                decoded_data[mapped_name] = val

        if event_epoch < current_state.epoch:
            for curr_name, curr_col in current_state.columns.items():
                if curr_name not in decoded_data:
                    if curr_col.default is not None:
                        decoded_data[curr_name] = curr_col.default

        decoded_key: Dict[str, Any] = {}
        for k_name, val in raw_key.items():
            mapped_k = rename_map.get(k_name, k_name)
            decoded_key[mapped_k] = val

        out = dict(change)
        out["data"] = decoded_data
        out["key"] = decoded_key
        out["column_identities"] = col_idents
        out["key_columns"] = list(schema_at_epoch.primary_key)
        out["is_dropped_generation"] = is_dropped_generation
        return out

    def start_snapshot(self, snapshot_id: str, table: str) -> ActiveSnapshotContext:
        curr_epoch = self.table_epochs.get(table, 1)
        ctx = ActiveSnapshotContext(
            snapshot_id=snapshot_id,
            table=table,
            start_epoch=curr_epoch,
            status="ACTIVE",
        )
        self.active_snapshots[snapshot_id] = ctx
        return ctx

    def record_concurrent_ddl(self, snapshot_id: str, op: DDLOperation) -> None:
        if snapshot_id in self.active_snapshots:
            self.active_snapshots[snapshot_id].concurrent_ddls.append(op)

    def decode_snapshot_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        snapshot_id = row.get("snapshot_id")
        table = row.get("table", "")
        snap = self.active_snapshots.get(snapshot_id) if snapshot_id else None
        snap_epoch = snap.start_epoch if snap else self.table_epochs.get(table, 1)

        raw_data = dict(row.get("data") or {})
        current_state = self.get_current_schema(table)
        rename_map = self.column_rename_map.get(table, {})

        decoded_data: Dict[str, Any] = {}
        for col_name, val in raw_data.items():
            target_name = rename_map.get(col_name, col_name)
            decoded_data[target_name] = val

        for col_name, col_ident in current_state.columns.items():
            if col_name not in decoded_data and col_ident.default is not None:
                def_val: Any = col_ident.default
                if isinstance(def_val, str) and def_val.isdigit():
                    def_val = int(def_val)
                elif isinstance(def_val, str) and (def_val.startswith("'") and def_val.endswith("'")):
                    def_val = def_val[1:-1]
                decoded_data[col_name] = def_val

        out = dict(row)
        out["data"] = decoded_data
        out["target_coherent"] = True
        return out

    def complete_snapshot(self, snapshot_id: str) -> None:
        if snapshot_id in self.active_snapshots:
            snap = self.active_snapshots[snapshot_id]
            snap.status = "COMPLETED"
            snap.completed_at = time.time()

    def checkpoint_state(self) -> Dict[str, Any]:
        tables_data = {}
        for t, cur_ep in self.table_epochs.items():
            history = {ep: state.to_dict() for ep, state in self.epoch_history[t].items()}
            lineage = {
                c: [ident.to_dict() for ident in idents]
                for c, idents in self.column_lineage.get(t, {}).items()
            }
            renames = dict(self.column_rename_map.get(t, {}))
            tables_data[t] = {
                "current_epoch": cur_ep,
                "history": history,
                "lineage": lineage,
                "renames": renames,
            }

        snapshots_data = {sid: ctx.to_dict() for sid, ctx in self.active_snapshots.items()}
        plans_data = {pid: plan.to_dict() for pid, plan in self.applied_plans.items()}

        return {
            "target_dialect": self.target_dialect,
            "tables": tables_data,
            "snapshots": snapshots_data,
            "applied_plans": plans_data,
        }

    @classmethod
    def restore_checkpoint(
        cls,
        checkpoint: Dict[str, Any],
        slice_manager: Optional[VerificationSliceManager] = None,
        applier: Optional[CDCReplicationApplier] = None,
        target_executor: Optional[Callable[[str], Any]] = None,
    ) -> "SchemaEpochPlanner":
        planner = cls(
            target_dialect=checkpoint.get("target_dialect", "postgres"),
            slice_manager=slice_manager,
            applier=applier,
            target_executor=target_executor,
        )

        for t, tdata in checkpoint.get("tables", {}).items():
            planner.table_epochs[t] = tdata["current_epoch"]
            planner.epoch_history[t] = {
                int(ep): SchemaEpochState.from_dict(s)
                for ep, s in tdata["history"].items()
            }
            planner.column_lineage[t] = {
                c: [ColumnIdentity.from_dict(ident) for ident in idents]
                for c, idents in tdata["lineage"].items()
            }
            planner.column_rename_map[t] = dict(tdata.get("renames", {}))

        for sid, sdata in checkpoint.get("snapshots", {}).items():
            planner.active_snapshots[sid] = ActiveSnapshotContext.from_dict(sdata)

        for pid, pdata in checkpoint.get("applied_plans", {}).items():
            planner.applied_plans[pid] = FencedEpochPlan.from_dict(pdata)

        return planner
