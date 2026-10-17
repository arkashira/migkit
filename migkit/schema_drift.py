import copy
import dataclasses
import enum
import queue
import re
import threading
import time
import uuid
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Tuple, Union

from migkit.ddl_shield import (
    CDCReplicationApplier,
    ColumnSchema,
    SchemaCache,
    TableSchema,
    TransactionalFence,
    VerificationSliceManager,
)


class SchemaDriftError(Exception):
    pass


class DriftType(str, enum.Enum):
    NEW_COLUMN = "NEW_COLUMN"
    TYPE_WIDENING = "TYPE_WIDENING"
    RELAXED_NULLABILITY = "RELAXED_NULLABILITY"
    ADDED_DEFAULT = "ADDED_DEFAULT"


@dataclasses.dataclass
class DriftAction:
    drift_type: DriftType
    table: str
    column_name: str
    target_type: Optional[str] = None
    new_type: Optional[str] = None
    old_nullable: Optional[bool] = None
    new_nullable: Optional[bool] = None
    old_default: Optional[str] = None
    new_default: Optional[str] = None
    requires_backfill: bool = False
    details: Optional[str] = None


@dataclasses.dataclass
class BackfillTask:
    task_id: str
    table: str
    column_name: str
    default_value: Any
    key_column: str = "id"
    batch_size: int = 1000
    min_key: Optional[int] = None
    max_key: Optional[int] = None
    last_cursor: Optional[Any] = None
    checkpoint_path: Optional[str] = None
    status: str = "PENDING"
    rows_updated: int = 0
    created_at: float = dataclasses.field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None


@dataclasses.dataclass
class DriftResolutionResult:
    barrier_id: str
    table: str
    status: str
    drifts: List[DriftAction] = dataclasses.field(default_factory=list)
    generated_statements: List[str] = dataclasses.field(default_factory=list)
    invalidated_slices: List[str] = dataclasses.field(default_factory=list)
    backfill_tasks: List[BackfillTask] = dataclasses.field(default_factory=list)
    duration_seconds: float = 0.0
    error: Optional[str] = None


INT_RANKS: Dict[str, int] = {
    "TINYINT": 1,
    "SMALLINT": 2,
    "MEDIUMINT": 3,
    "INT": 4,
    "INTEGER": 4,
    "BIGINT": 5,
}

TEXT_RANKS: Dict[str, int] = {
    "CHAR": 1,
    "VARCHAR": 2,
    "TINYTEXT": 3,
    "TEXT": 4,
    "MEDIUMTEXT": 5,
    "LONGTEXT": 6,
}

BLOB_RANKS: Dict[str, int] = {
    "BINARY": 1,
    "VARBINARY": 2,
    "TINYBLOB": 3,
    "BLOB": 4,
    "MEDIUMBLOB": 5,
    "LONGBLOB": 6,
    "BYTEA": 6,
}


def parse_type_signature(type_str: str) -> Tuple[str, List[int], bool]:
    clean = type_str.strip().upper()
    is_unsigned = "UNSIGNED" in clean
    clean = re.sub(r"\bUNSIGNED\b", "", clean).strip()

    m = re.match(r"^([A-Z\s]+?)(?:\(([\d\s,]+)\))?$", clean)
    if not m:
        return clean, [], is_unsigned

    base = m.group(1).strip()
    param_str = m.group(2)
    params: List[int] = []
    if param_str:
        for p in param_str.split(","):
            p_clean = p.strip()
            if p_clean.isdigit():
                params.append(int(p_clean))

    if base in ("INT", "INTEGER"):
        base = "INTEGER"

    return base, params, is_unsigned


def check_type_widening(current_type: str, new_type: str) -> Tuple[bool, Optional[str]]:
    cur_base, cur_params, cur_unsigned = parse_type_signature(current_type)
    new_base, new_params, new_unsigned = parse_type_signature(new_type)

    if cur_base == new_base and cur_params == new_params and cur_unsigned == new_unsigned:
        return False, None

    if cur_base in INT_RANKS and new_base in INT_RANKS:
        cur_rank = INT_RANKS[cur_base]
        new_rank = INT_RANKS[new_base]
        if new_rank > cur_rank:
            return True, f"integer widening: {current_type} -> {new_type}"
        if new_rank == cur_rank and not cur_unsigned and new_unsigned:
            return True, f"signed to unsigned integer widening: {current_type} -> {new_type}"
        return False, None

    if cur_base in INT_RANKS and new_base in ("DECIMAL", "NUMERIC"):
        return True, f"integer to decimal widening: {current_type} -> {new_type}"

    if cur_base in ("DECIMAL", "NUMERIC") and new_base in ("DECIMAL", "NUMERIC"):
        if cur_params and not new_params:
            return True, f"bounded to unbounded numeric: {current_type} -> {new_type}"
        if len(cur_params) == 2 and len(new_params) == 2:
            cp, cs = cur_params
            np, ns = new_params
            if np >= cp and ns >= cs and (np > cp or ns > cs):
                return True, f"decimal precision/scale widening: {current_type} -> {new_type}"
        elif len(cur_params) == 1 and len(new_params) == 1:
            if new_params[0] > cur_params[0]:
                return True, f"decimal precision widening: {current_type} -> {new_type}"
        return False, None

    if cur_base in TEXT_RANKS and new_base in TEXT_RANKS:
        if cur_base in ("CHAR", "VARCHAR") and new_base in ("CHAR", "VARCHAR"):
            c_len = cur_params[0] if cur_params else 1
            n_len = new_params[0] if new_params else (255 if new_base == "VARCHAR" else 1)
            if n_len > c_len:
                return True, f"varchar length expansion: {c_len} -> {n_len}"
            if new_base == "VARCHAR" and cur_base == "CHAR" and n_len >= c_len:
                return True, f"char to varchar conversion: {current_type} -> {new_type}"
            return False, None
        if TEXT_RANKS[new_base] > TEXT_RANKS[cur_base]:
            return True, f"text size expansion: {current_type} -> {new_type}"
        return False, None

    if cur_base in ("FLOAT", "REAL") and new_base in ("DOUBLE", "DOUBLE PRECISION"):
        return True, f"float to double widening: {current_type} -> {new_type}"

    if cur_base in BLOB_RANKS and new_base in BLOB_RANKS:
        if BLOB_RANKS[new_base] > BLOB_RANKS[cur_base]:
            return True, f"blob size expansion: {current_type} -> {new_type}"
        if cur_base == "VARBINARY" and new_base == "VARBINARY":
            c_len = cur_params[0] if cur_params else 255
            n_len = new_params[0] if new_params else 255
            if n_len > c_len:
                return True, f"varbinary capacity expansion: {c_len} -> {n_len}"
        return False, None

    return False, None


def map_type_to_dialect(data_type: str, target_dialect: str) -> str:
    target = target_dialect.lower()
    base, params, is_unsigned = parse_type_signature(data_type)

    if target in ("postgres", "postgresql"):
        if base in ("TINYINT", "BOOLEAN") and params == [1]:
            return "BOOLEAN"
        if base == "TINYINT":
            return "SMALLINT"
        if base == "SMALLINT":
            return "SMALLINT"
        if base in ("MEDIUMINT", "INT", "INTEGER"):
            return "BIGINT" if is_unsigned else "INTEGER"
        if base == "BIGINT":
            return "NUMERIC(20, 0)" if is_unsigned else "BIGINT"
        if base in ("FLOAT", "REAL"):
            return "REAL"
        if base in ("DOUBLE", "DOUBLE PRECISION"):
            return "DOUBLE PRECISION"
        if base in ("DECIMAL", "NUMERIC"):
            return f"NUMERIC({params[0]}, {params[1]})" if len(params) == 2 else (f"NUMERIC({params[0]})" if params else "NUMERIC")
        if base == "VARCHAR":
            return f"VARCHAR({params[0]})" if params else "VARCHAR"
        if base == "CHAR":
            return f"CHAR({params[0]})" if params else "CHAR(1)"
        if base in ("TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"):
            return "TEXT"
        if base in ("BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "BYTEA"):
            return "BYTEA"
        if base == "DATETIME":
            return "TIMESTAMP WITHOUT TIME ZONE"
        if base == "TIMESTAMP":
            return "TIMESTAMP WITH TIME ZONE"
        if base in ("JSON", "JSONB"):
            return "JSONB"
        return data_type

    if target in ("mysql", "mariadb"):
        if base == "BOOLEAN":
            return "TINYINT(1)"
        if base == "INTEGER":
            return "INT UNSIGNED" if is_unsigned else "INT"
        if base in ("INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT"):
            return f"{base} UNSIGNED" if is_unsigned else base
        if base == "REAL":
            return "FLOAT"
        if base in ("DOUBLE PRECISION", "DOUBLE"):
            return "DOUBLE"
        if base in ("DECIMAL", "NUMERIC"):
            return f"DECIMAL({params[0]}, {params[1]})" if len(params) == 2 else (f"DECIMAL({params[0]})" if params else "DECIMAL(10, 0)")
        if base == "VARCHAR":
            return f"VARCHAR({params[0]})" if params else "VARCHAR(255)"
        if base == "CHAR":
            return f"CHAR({params[0]})" if params else "CHAR(1)"
        if base == "BYTEA":
            return "LONGBLOB"
        if base == "TIMESTAMP WITHOUT TIME ZONE":
            return "DATETIME"
        if base == "TIMESTAMP WITH TIME ZONE":
            return "TIMESTAMP"
        if base in ("JSON", "JSONB"):
            return "JSON"
        return data_type

    if target == "sqlite":
        if base in ("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "BOOLEAN"):
            return "INTEGER"
        if base in ("FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION"):
            return "REAL"
        if base in ("VARCHAR", "CHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "DATETIME", "TIMESTAMP", "JSON", "JSONB"):
            return "TEXT"
        if base in ("DECIMAL", "NUMERIC"):
            return "NUMERIC"
        if base in ("BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "BYTEA"):
            return "BLOB"
        return "TEXT"

    return data_type


def _format_default_value(default_val: Any) -> str:
    if default_val is None:
        return "NULL"
    if isinstance(default_val, bool):
        return "TRUE" if default_val else "FALSE"
    if isinstance(default_val, (int, float)):
        return str(default_val)
    s = str(default_val).strip()
    if s.upper() in ("CURRENT_TIMESTAMP", "NOW()", "NULL", "TRUE", "FALSE") or s.startswith("'"):
        return s
    return f"'{s}'"


def generate_alter_statements(
    table: str,
    drifts: List[DriftAction],
    target_dialect: str = "postgres",
    table_schema: Optional[TableSchema] = None,
) -> List[str]:
    target = target_dialect.lower()
    statements: List[str] = []

    if target in ("postgres", "postgresql"):
        for d in drifts:
            col = d.column_name
            if d.drift_type == DriftType.NEW_COLUMN:
                t_type = map_type_to_dialect(d.new_type or "TEXT", target)
                stmt = f'ALTER TABLE "{table}" ADD COLUMN "{col}" {t_type}'
                if d.new_default is not None:
                    stmt += f" DEFAULT {_format_default_value(d.new_default)}"
                if d.new_nullable is False:
                    stmt += " NOT NULL"
                statements.append(stmt)
            elif d.drift_type == DriftType.TYPE_WIDENING:
                t_type = map_type_to_dialect(d.new_type or "TEXT", target)
                statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col}" TYPE {t_type}')
            elif d.drift_type == DriftType.RELAXED_NULLABILITY:
                statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col}" DROP NOT NULL')
            elif d.drift_type == DriftType.ADDED_DEFAULT:
                def_expr = _format_default_value(d.new_default)
                statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col}" SET DEFAULT {def_expr}')

    elif target in ("mysql", "mariadb"):
        for d in drifts:
            col = d.column_name
            if d.drift_type == DriftType.NEW_COLUMN:
                t_type = map_type_to_dialect(d.new_type or "TEXT", target)
                stmt = f"ALTER TABLE `{table}` ADD COLUMN `{col}` {t_type}"
                if d.new_default is not None:
                    stmt += f" DEFAULT {_format_default_value(d.new_default)}"
                if d.new_nullable is False:
                    stmt += " NOT NULL"
                elif d.new_nullable is True:
                    stmt += " NULL"
                statements.append(stmt)
            elif d.drift_type == DriftType.TYPE_WIDENING:
                t_type = map_type_to_dialect(d.new_type or "TEXT", target)
                stmt = f"ALTER TABLE `{table}` MODIFY COLUMN `{col}` {t_type}"
                if d.new_default is not None:
                    stmt += f" DEFAULT {_format_default_value(d.new_default)}"
                if d.new_nullable is False:
                    stmt += " NOT NULL"
                elif d.new_nullable is True:
                    stmt += " NULL"
                statements.append(stmt)
            elif d.drift_type == DriftType.RELAXED_NULLABILITY:
                t_type = map_type_to_dialect(d.new_type or d.target_type or "TEXT", target)
                stmt = f"ALTER TABLE `{table}` MODIFY COLUMN `{col}` {t_type} NULL"
                if d.new_default is not None:
                    stmt += f" DEFAULT {_format_default_value(d.new_default)}"
                statements.append(stmt)
            elif d.drift_type == DriftType.ADDED_DEFAULT:
                def_expr = _format_default_value(d.new_default)
                statements.append(f"ALTER TABLE `{table}` ALTER COLUMN `{col}` SET DEFAULT {def_expr}")

    elif target == "sqlite":
        has_table_rewrite = any(
            d.drift_type in (DriftType.TYPE_WIDENING, DriftType.RELAXED_NULLABILITY, DriftType.ADDED_DEFAULT)
            for d in drifts
        )
        if has_table_rewrite and table_schema is not None:
            updated_cols: Dict[str, ColumnSchema] = copy.deepcopy(table_schema.columns)
            for d in drifts:
                if d.drift_type == DriftType.NEW_COLUMN:
                    updated_cols[d.column_name] = ColumnSchema(
                        name=d.column_name,
                        data_type=map_type_to_dialect(d.new_type or "TEXT", target),
                        nullable=d.new_nullable if d.new_nullable is not None else True,
                        default=d.new_default,
                    )
                elif d.column_name in updated_cols:
                    col_obj = updated_cols[d.column_name]
                    if d.drift_type == DriftType.TYPE_WIDENING and d.new_type:
                        col_obj.data_type = map_type_to_dialect(d.new_type, target)
                    elif d.drift_type == DriftType.RELAXED_NULLABILITY:
                        col_obj.nullable = True
                    elif d.drift_type == DriftType.ADDED_DEFAULT:
                        col_obj.default = d.new_default

            col_defs = []
            for c in updated_cols.values():
                c_sql = f'"{c.name}" {map_type_to_dialect(c.data_type, target)}'
                if c.is_primary_key:
                    c_sql += " PRIMARY KEY"
                if not c.nullable:
                    c_sql += " NOT NULL"
                if c.default is not None:
                    c_sql += f" DEFAULT {_format_default_value(c.default)}"
                col_defs.append(c_sql)

            temp_table = f"{table}_drift_new"
            common_cols = [f'"{c}"' for c in table_schema.columns if c in updated_cols]
            common_cols_str = ", ".join(common_cols)

            statements.append(f'CREATE TABLE "{temp_table}" ({", ".join(col_defs)})')
            if common_cols:
                statements.append(f'INSERT INTO "{temp_table}" ({common_cols_str}) SELECT {common_cols_str} FROM "{table}"')
            statements.append(f'DROP TABLE "{table}"')
            statements.append(f'ALTER TABLE "{temp_table}" RENAME TO "{table}"')
        else:
            for d in drifts:
                col = d.column_name
                if d.drift_type == DriftType.NEW_COLUMN:
                    t_type = map_type_to_dialect(d.new_type or "TEXT", target)
                    stmt = f'ALTER TABLE "{table}" ADD COLUMN "{col}" {t_type}'
                    if d.new_default is not None:
                        stmt += f" DEFAULT {_format_default_value(d.new_default)}"
                    if d.new_nullable is False:
                        stmt += " NOT NULL"
                    statements.append(stmt)
                elif d.drift_type == DriftType.TYPE_WIDENING:
                    t_type = map_type_to_dialect(d.new_type or "TEXT", target)
                    statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col}" {t_type}')
                elif d.drift_type == DriftType.RELAXED_NULLABILITY:
                    statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col}" DROP NOT NULL')
                elif d.drift_type == DriftType.ADDED_DEFAULT:
                    def_expr = _format_default_value(d.new_default)
                    statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col}" SET DEFAULT {def_expr}')

    return statements


class BackfillOrchestrator:
    def __init__(
        self,
        target_executor: Optional[Callable[[str], Any]] = None,
        conn: Optional[Any] = None,
        async_mode: bool = True,
    ):
        self.target_executor = target_executor
        self.conn = conn
        self.async_mode = async_mode
        self._tasks: Dict[str, BackfillTask] = {}
        self._queue: queue.Queue = queue.Queue()
        self._worker_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        if self.async_mode:
            self.start()

    def start(self) -> None:
        if self._worker_thread and self._worker_thread.is_alive():
            return
        self._stop_event.clear()
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)

    def schedule_backfill(
        self,
        table: str,
        column_name: str,
        default_value: Any,
        key_column: str = "id",
        batch_size: int = 1000,
        min_key: Optional[int] = None,
        max_key: Optional[int] = None,
        checkpoint_path: Optional[str] = None,
    ) -> BackfillTask:
        task_id = f"backfill_{uuid.uuid4().hex[:12]}"
        task = BackfillTask(
            task_id=task_id,
            table=table,
            column_name=column_name,
            default_value=default_value,
            key_column=key_column,
            batch_size=batch_size,
            min_key=min_key,
            max_key=max_key,
            checkpoint_path=checkpoint_path,
            status="PENDING",
        )
        with self._lock:
            self._tasks[task_id] = task
        if self.async_mode:
            self._queue.put(task_id)
        return task

    def execute_task(self, task: BackfillTask) -> None:
        import os
        import json
        from migkit import durable
        from migkit.sizing import AIMDChunkSizer

        task.status = "RUNNING"
        task.started_at = time.time()
        try:
            val_str = _format_default_value(task.default_value)
            key_col = task.key_column or "id"

            cursor_start = None
            if task.checkpoint_path and os.path.exists(task.checkpoint_path):
                try:
                    with open(task.checkpoint_path, "r") as f:
                        chk = json.load(f)
                    if chk.get("status") == "COMPLETED":
                        task.status = "COMPLETED"
                        task.rows_updated = chk.get("rows_updated", 0)
                        task.completed_at = time.time()
                        return
                    cursor_start = chk.get("cursor")
                except Exception:
                    cursor_start = None

            if self.conn is not None:
                cur = self.conn.cursor()
                min_k, max_k = task.min_key, task.max_key
                if min_k is None or max_k is None:
                    try:
                        cur.execute(f'SELECT MIN("{key_col}"), MAX("{key_col}") FROM "{task.table}" WHERE "{task.column_name}" IS NULL')
                        row = cur.fetchone()
                        if row and row[0] is not None and row[1] is not None:
                            min_k, max_k = row[0], row[1]
                    except Exception:
                        min_k, max_k = None, None

                if min_k is not None and max_k is not None and isinstance(min_k, (int, float)):
                    sizer = AIMDChunkSizer(initial=task.batch_size)
                    cur_key = cursor_start if cursor_start is not None else min_k
                    total_updated = 0
                    while cur_key <= max_k:
                        batch_rows = sizer.current()
                        next_key = cur_key + batch_rows
                        t0 = time.time()
                        chunk_sql = (
                            f'UPDATE "{task.table}" SET "{task.column_name}" = {val_str} '
                            f'WHERE "{task.column_name}" IS NULL '
                            f'AND "{key_col}" >= {cur_key} AND "{key_col}" < {next_key}'
                        )
                        cur.execute(chunk_sql)
                        cnt = getattr(cur, "rowcount", 0)
                        total_updated += cnt
                        if hasattr(self.conn, "commit"):
                            self.conn.commit()
                        elapsed = time.time() - t0
                        sizer.adjust(latency=elapsed, rows=cnt)
                        task.last_cursor = next_key
                        task.rows_updated = total_updated
                        if task.checkpoint_path:
                            durable.write_json(task.checkpoint_path, {
                                "task_id": task.task_id,
                                "table": task.table,
                                "column": task.column_name,
                                "cursor": next_key,
                                "rows_updated": total_updated,
                                "status": "RUNNING",
                            })
                        cur_key = next_key
                    task.rows_updated = total_updated
                else:
                    update_sql = f'UPDATE "{task.table}" SET "{task.column_name}" = {val_str} WHERE "{task.column_name}" IS NULL'
                    cur.execute(update_sql)
                    task.rows_updated = getattr(cur, "rowcount", 0)
                    if hasattr(self.conn, "commit"):
                        self.conn.commit()

            elif self.target_executor is not None:
                if task.min_key is not None and task.max_key is not None and isinstance(task.min_key, (int, float)):
                    sizer = AIMDChunkSizer(initial=task.batch_size)
                    cur_key = cursor_start if cursor_start is not None else task.min_key
                    total_updated = 0
                    while cur_key <= task.max_key:
                        batch_rows = sizer.current()
                        next_key = cur_key + batch_rows
                        t0 = time.time()
                        chunk_sql = (
                            f'UPDATE "{task.table}" SET "{task.column_name}" = {val_str} '
                            f'WHERE "{task.column_name}" IS NULL '
                            f'AND "{key_col}" >= {cur_key} AND "{key_col}" < {next_key}'
                        )
                        res = self.target_executor(chunk_sql)
                        cnt = res if isinstance(res, int) else 0
                        total_updated += cnt
                        elapsed = time.time() - t0
                        sizer.adjust(latency=elapsed, rows=cnt)
                        task.last_cursor = next_key
                        task.rows_updated = total_updated
                        if task.checkpoint_path:
                            durable.write_json(task.checkpoint_path, {
                                "task_id": task.task_id,
                                "table": task.table,
                                "column": task.column_name,
                                "cursor": next_key,
                                "rows_updated": total_updated,
                                "status": "RUNNING",
                            })
                        cur_key = next_key
                    task.rows_updated = total_updated
                else:
                    update_sql = f'UPDATE "{task.table}" SET "{task.column_name}" = {val_str} WHERE "{task.column_name}" IS NULL'
                    res = self.target_executor(update_sql)
                    if isinstance(res, int):
                        task.rows_updated = res

            task.status = "COMPLETED"
            task.completed_at = time.time()
            if task.checkpoint_path:
                durable.write_json(task.checkpoint_path, {
                    "task_id": task.task_id,
                    "table": task.table,
                    "column": task.column_name,
                    "cursor": task.last_cursor,
                    "rows_updated": task.rows_updated,
                    "status": "COMPLETED",
                })
        except Exception as exc:
            task.status = "FAILED"
            task.error = str(exc)
            task.completed_at = time.time()

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                task_id = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            with self._lock:
                task = self._tasks.get(task_id)
            if task and task.status == "PENDING":
                self.execute_task(task)
            self._queue.task_done()

    def run_all_pending(self) -> List[BackfillTask]:
        completed = []
        with self._lock:
            pending_ids = [tid for tid, t in self._tasks.items() if t.status == "PENDING"]
        for tid in pending_ids:
            with self._lock:
                task = self._tasks[tid]
            self.execute_task(task)
            completed.append(task)
        return completed

    def wait_all(self, timeout: float = 5.0) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                all_done = all(t.status in ("COMPLETED", "FAILED") for t in self._tasks.values())
            if all_done:
                return True
            time.sleep(0.02)
        return False

    def get_task(self, task_id: str) -> Optional[BackfillTask]:
        with self._lock:
            return self._tasks.get(task_id)

    def get_tasks_for_table(self, table: str) -> List[BackfillTask]:
        with self._lock:
            return [t for t in self._tasks.values() if t.table == table]


class SchemaDriftResolver:
    def __init__(
        self,
        source_dialect: str = "mysql",
        target_dialect: str = "postgres",
        schema_cache: Optional[SchemaCache] = None,
        slice_manager: Optional[VerificationSliceManager] = None,
        applier: Optional[CDCReplicationApplier] = None,
        target_executor: Optional[Callable[[str], Any]] = None,
        conn: Optional[Any] = None,
        auto_backfill: bool = True,
        backfill_batch_size: int = 1000,
        backfill_orchestrator: Optional[BackfillOrchestrator] = None,
        source_catalog_inspector: Optional[Callable[[str], Dict[str, Any]]] = None,
        source_conn: Optional[Any] = None,
        source_engine: Optional[Any] = None,
        checkpoint_dir: Optional[Union[str, Any]] = None,
    ):
        self.source_dialect = source_dialect.lower()
        self.target_dialect = target_dialect.lower()
        self.schema_cache = schema_cache if schema_cache is not None else SchemaCache()
        self.slice_manager = slice_manager if slice_manager is not None else VerificationSliceManager()
        self.applier = applier if applier is not None else CDCReplicationApplier()
        self.target_executor = target_executor
        self.conn = conn
        self.auto_backfill = auto_backfill
        self.backfill_batch_size = backfill_batch_size
        self.backfill_orchestrator = backfill_orchestrator or BackfillOrchestrator(
            target_executor=self.target_executor,
            conn=self.conn,
            async_mode=True,
        )
        self.source_catalog_inspector = source_catalog_inspector
        self.source_conn = source_conn
        self.source_engine = source_engine
        self.checkpoint_dir = checkpoint_dir
        self.resolution_history: List[DriftResolutionResult] = []

    def inspect_source_catalog(self, table: str) -> Optional[Dict[str, Any]]:
        if self.source_catalog_inspector is not None:
            try:
                res = self.source_catalog_inspector(table)
                if res:
                    return res
            except Exception:
                pass
        if self.source_conn is not None:
            res = self._probe_catalog_columns(table)
            if res:
                return res
        if self.source_engine is not None:
            res = self._probe_engine_columns(table)
            if res:
                return res
        return None

    def _probe_catalog_columns(self, table: str) -> Optional[Dict[str, Any]]:
        if self.source_conn is None:
            return None
        try:
            parts = table.split(".")
            sch = parts[0] if len(parts) > 1 else "public"
            tbl = parts[-1]
            cur = self.source_conn.cursor()
            cur.execute(
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_name = %s AND table_schema = %s",
                (tbl, sch),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            cols = {}
            for col_name, data_type, is_nullable, col_default in rows:
                cols[col_name] = {
                    "data_type": data_type.upper(),
                    "nullable": is_nullable.upper() == "YES" if isinstance(is_nullable, str) else bool(is_nullable),
                    "default": col_default,
                }
            return cols
        except Exception:
            return None

    def _probe_engine_columns(self, table: str) -> Optional[Dict[str, Any]]:
        if self.source_engine is None:
            return None
        try:
            cols = {}
            if hasattr(self.source_engine, "columns"):
                raw = self.source_engine.columns("src", getattr(self.source_engine, "db", ""), table)
                if raw:
                    for c in raw:
                        cols[c.name] = {
                            "data_type": getattr(c, "data_type", "TEXT"),
                            "nullable": getattr(c, "nullable", True),
                            "default": getattr(c, "default", None),
                        }
                    return cols
        except Exception:
            pass
        return None

    def detect_drift_from_columns(
        self,
        table: str,
        source_columns: Dict[str, Any],
    ) -> List[DriftAction]:
        target_table = self.schema_cache.get_table(table)
        drifts: List[DriftAction] = []

        if target_table is None:
            for col_name, col_meta in source_columns.items():
                col_type = col_meta.get("data_type") if isinstance(col_meta, dict) else getattr(col_meta, "data_type", "TEXT")
                nullable = col_meta.get("nullable", True) if isinstance(col_meta, dict) else getattr(col_meta, "nullable", True)
                default = col_meta.get("default") if isinstance(col_meta, dict) else getattr(col_meta, "default", None)
                drifts.append(
                    DriftAction(
                        drift_type=DriftType.NEW_COLUMN,
                        table=table,
                        column_name=col_name,
                        target_type=None,
                        new_type=col_type,
                        old_nullable=None,
                        new_nullable=nullable,
                        old_default=None,
                        new_default=default,
                        requires_backfill=default is not None,
                        details=f"initial table creation/column {col_name}",
                    )
                )
            return drifts

        for col_name, col_meta in source_columns.items():
            s_type = col_meta.get("data_type") if isinstance(col_meta, dict) else getattr(col_meta, "data_type", "TEXT")
            s_nullable = col_meta.get("nullable", True) if isinstance(col_meta, dict) else getattr(col_meta, "nullable", True)
            s_default = col_meta.get("default") if isinstance(col_meta, dict) else getattr(col_meta, "default", None)

            if col_name not in target_table.columns:
                drifts.append(
                    DriftAction(
                        drift_type=DriftType.NEW_COLUMN,
                        table=table,
                        column_name=col_name,
                        target_type=None,
                        new_type=s_type,
                        old_nullable=None,
                        new_nullable=s_nullable,
                        old_default=None,
                        new_default=s_default,
                        requires_backfill=s_default is not None,
                        details=f"new column added on source: {col_name} {s_type}",
                    )
                )
                continue

            target_col = target_table.columns[col_name]

            if target_col.nullable is False and s_nullable is True:
                drifts.append(
                    DriftAction(
                        drift_type=DriftType.RELAXED_NULLABILITY,
                        table=table,
                        column_name=col_name,
                        target_type=target_col.data_type,
                        new_type=s_type,
                        old_nullable=False,
                        new_nullable=True,
                        old_default=target_col.default,
                        new_default=s_default or target_col.default,
                        requires_backfill=False,
                        details=f"nullability relaxed for column {col_name}: NOT NULL -> NULL",
                    )
                )

            is_widening, widening_reason = check_type_widening(target_col.data_type, s_type)
            if is_widening:
                drifts.append(
                    DriftAction(
                        drift_type=DriftType.TYPE_WIDENING,
                        table=table,
                        column_name=col_name,
                        target_type=target_col.data_type,
                        new_type=s_type,
                        old_nullable=target_col.nullable,
                        new_nullable=s_nullable,
                        old_default=target_col.default,
                        new_default=s_default or target_col.default,
                        requires_backfill=False,
                        details=widening_reason,
                    )
                )

            if target_col.default is None and s_default is not None:
                drifts.append(
                    DriftAction(
                        drift_type=DriftType.ADDED_DEFAULT,
                        table=table,
                        column_name=col_name,
                        target_type=target_col.data_type,
                        new_type=s_type,
                        old_nullable=target_col.nullable,
                        new_nullable=s_nullable,
                        old_default=None,
                        new_default=s_default,
                        requires_backfill=True,
                        details=f"default added for column {col_name}: DEFAULT {s_default}",
                    )
                )

        return drifts

    def detect_drift_from_event(self, event: Dict[str, Any]) -> Tuple[Optional[str], List[DriftAction]]:
        table = event.get("table")
        ev_type = (event.get("type") or event.get("op") or "").upper()

        if "schema" in event and isinstance(event["schema"], dict):
            s_dict = event["schema"]
            tbl = table or s_dict.get("table")
            cols = s_dict.get("columns", {})
            if isinstance(cols, list):
                cols = {c["name"]: c for c in cols if "name" in c}
            if tbl and cols:
                return tbl, self.detect_drift_from_columns(tbl, cols)

        if "column_definitions" in event and isinstance(event["column_definitions"], dict):
            tbl = table or event.get("table")
            if tbl:
                return tbl, self.detect_drift_from_columns(tbl, event["column_definitions"])

        data_map = event.get("data")
        if data_map is None and ("values" in event or "key" in event):
            data_map = {**(event.get("key") or {}), **(event.get("values") or {})}

        if ev_type in ("INSERT", "UPDATE") and data_map and isinstance(data_map, dict):
            if not table:
                return None, []
            cached_tbl = self.schema_cache.get_table(table)
            if cached_tbl is None:
                return None, []

            has_potential_drift = False
            for k, val in data_map.items():
                if k not in cached_tbl.columns:
                    has_potential_drift = True
                    break
                col = cached_tbl.columns[k]
                if val is None and col.nullable is False:
                    has_potential_drift = True
                    break
                if isinstance(val, int) and (val > 2147483647 or val < -2147483648):
                    c_base, _, _ = parse_type_signature(col.data_type)
                    if c_base in ("INT", "INTEGER", "SMALLINT", "TINYINT", "MEDIUMINT"):
                        has_potential_drift = True
                        break

            if not has_potential_drift:
                return table, []

            catalog_cols = self.inspect_source_catalog(table)
            if catalog_cols:
                return table, self.detect_drift_from_columns(table, catalog_cols)

            drifts: List[DriftAction] = []
            for k, val in data_map.items():
                if k not in cached_tbl.columns:
                    inferred_type = "TEXT"
                    if isinstance(val, bool):
                        inferred_type = "BOOLEAN"
                    elif isinstance(val, int):
                        inferred_type = "BIGINT" if val > 2147483647 or val < -2147483648 else "INTEGER"
                    elif isinstance(val, float):
                        inferred_type = "DOUBLE PRECISION"
                    drifts.append(
                        DriftAction(
                            drift_type=DriftType.NEW_COLUMN,
                            table=table,
                            column_name=k,
                            target_type=None,
                            new_type=inferred_type,
                            old_nullable=None,
                            new_nullable=True,
                            old_default=None,
                            new_default=None,
                            requires_backfill=False,
                            details=f"inferred new column from CDC row payload: {k}",
                        )
                    )
                else:
                    col = cached_tbl.columns[k]
                    if val is None and col.nullable is False:
                        drifts.append(
                            DriftAction(
                                drift_type=DriftType.RELAXED_NULLABILITY,
                                table=table,
                                column_name=k,
                                target_type=col.data_type,
                                new_type=col.data_type,
                                old_nullable=False,
                                new_nullable=True,
                                old_default=col.default,
                                new_default=col.default,
                                requires_backfill=False,
                                details=f"NULL value received for NOT NULL column {k}",
                            )
                        )
                    elif isinstance(val, int) and (val > 2147483647 or val < -2147483648):
                        c_base, _, _ = parse_type_signature(col.data_type)
                        if c_base in ("INT", "INTEGER", "SMALLINT", "TINYINT", "MEDIUMINT"):
                            drifts.append(
                                DriftAction(
                                    drift_type=DriftType.TYPE_WIDENING,
                                    table=table,
                                    column_name=k,
                                    target_type=col.data_type,
                                    new_type="BIGINT",
                                    old_nullable=col.nullable,
                                    new_nullable=col.nullable,
                                    old_default=col.default,
                                    new_default=col.default,
                                    requires_backfill=False,
                                    details=f"integer overflow observed in CDC payload for {k}: widening to BIGINT",
                                )
                            )
            return table, drifts

        if ev_type in ("DDL", "SCHEMA", "SCHEMA_CHANGE") or "sql" in event:
            raw_sql = event.get("sql", "")
            if raw_sql:
                return self._parse_ddl_for_drifts(raw_sql, table)

        return table, []

    def _parse_ddl_for_drifts(self, raw_sql: str, default_table: Optional[str]) -> Tuple[Optional[str], List[DriftAction]]:
        clean = re.sub(r"\s+", " ", raw_sql).strip()
        m_tbl = re.search(r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?", clean, re.I)
        table = m_tbl.group(2) if m_tbl else default_table
        if not table:
            return None, []

        drifts: List[DriftAction] = []
        cached_tbl = self.schema_cache.get_table(table)

        m_add = re.search(
            r"ADD\s+(?:COLUMN\s+)?`?(\w+)`?\s+([A-Z0-9_\(\),\s]+?)(?:\s+DEFAULT\s+([^,;]+?))?(?:\s+(NOT\s+NULL|NULL))?(?:\s+AFTER|\s+FIRST|;|$)",
            clean,
            re.I,
        )
        if m_add and "ADD CONSTRAINT" not in clean.upper() and "ADD INDEX" not in clean.upper():
            col_name = m_add.group(1)
            raw_type = m_add.group(2).strip()
            def_val = m_add.group(3).strip() if m_add.group(3) else None
            null_clause = m_add.group(4).strip().upper() if m_add.group(4) else None
            is_nullable = False if null_clause == "NOT NULL" else True

            drifts.append(
                DriftAction(
                    drift_type=DriftType.NEW_COLUMN,
                    table=table,
                    column_name=col_name,
                    target_type=None,
                    new_type=raw_type,
                    old_nullable=None,
                    new_nullable=is_nullable,
                    old_default=None,
                    new_default=def_val,
                    requires_backfill=def_val is not None,
                    details=f"parsed DDL ADD COLUMN {col_name}",
                )
            )

        m_mod = re.search(
            r"(?:MODIFY|ALTER)\s+(?:COLUMN\s+)?`?(\w+)`?\s+(?:TYPE\s+)?(DOUBLE\s+PRECISION|[A-Z0-9_\(\)]+)(?:\s+DEFAULT\s+([^,;]+?))?(?:\s+(NOT\s+NULL|NULL))?(?:;|$)",
            clean,
            re.I,
        )
        if m_mod and not m_add:
            col_name = m_mod.group(1)
            raw_type = m_mod.group(2).strip()
            def_val = m_mod.group(3).strip() if m_mod.group(3) else None
            null_clause = m_mod.group(4).strip().upper() if m_mod.group(4) else None

            existing_col = cached_tbl.columns.get(col_name) if cached_tbl else None
            cur_type = existing_col.data_type if existing_col else "TEXT"
            cur_nullable = existing_col.nullable if existing_col else True
            cur_default = existing_col.default if existing_col else None

            is_widening, widening_reason = check_type_widening(cur_type, raw_type)
            if is_widening:
                drifts.append(
                    DriftAction(
                        drift_type=DriftType.TYPE_WIDENING,
                        table=table,
                        column_name=col_name,
                        target_type=cur_type,
                        new_type=raw_type,
                        old_nullable=cur_nullable,
                        new_nullable=False if null_clause == "NOT NULL" else (True if null_clause == "NULL" else cur_nullable),
                        old_default=cur_default,
                        new_default=def_val or cur_default,
                        requires_backfill=False,
                        details=widening_reason,
                    )
                )

            if null_clause == "NULL" and cur_nullable is False:
                drifts.append(
                    DriftAction(
                        drift_type=DriftType.RELAXED_NULLABILITY,
                        table=table,
                        column_name=col_name,
                        target_type=cur_type,
                        new_type=raw_type,
                        old_nullable=False,
                        new_nullable=True,
                        old_default=cur_default,
                        new_default=def_val or cur_default,
                        requires_backfill=False,
                        details=f"relaxed nullability via DDL on {col_name}",
                    )
                )

            if def_val and cur_default is None:
                drifts.append(
                    DriftAction(
                        drift_type=DriftType.ADDED_DEFAULT,
                        table=table,
                        column_name=col_name,
                        target_type=cur_type,
                        new_type=raw_type,
                        old_nullable=cur_nullable,
                        new_nullable=cur_nullable,
                        old_default=None,
                        new_default=def_val,
                        requires_backfill=True,
                        details=f"added default via DDL on {col_name}",
                    )
                )

        m_drop_not_null = re.search(r"ALTER\s+(?:COLUMN\s+)?`?(\w+)`?\s+DROP\s+NOT\s+NULL", clean, re.I)
        if m_drop_not_null:
            col_name = m_drop_not_null.group(1)
            existing_col = cached_tbl.columns.get(col_name) if cached_tbl else None
            drifts.append(
                DriftAction(
                    drift_type=DriftType.RELAXED_NULLABILITY,
                    table=table,
                    column_name=col_name,
                    target_type=existing_col.data_type if existing_col else "TEXT",
                    new_type=existing_col.data_type if existing_col else "TEXT",
                    old_nullable=False,
                    new_nullable=True,
                    old_default=existing_col.default if existing_col else None,
                    new_default=existing_col.default if existing_col else None,
                    requires_backfill=False,
                    details=f"DROP NOT NULL parsed for {col_name}",
                )
            )

        m_set_default = re.search(r"ALTER\s+(?:COLUMN\s+)?`?(\w+)`?\s+SET\s+DEFAULT\s+([^;]+)", clean, re.I)
        if m_set_default:
            col_name = m_set_default.group(1)
            def_val = m_set_default.group(2).strip()
            existing_col = cached_tbl.columns.get(col_name) if cached_tbl else None
            drifts.append(
                DriftAction(
                    drift_type=DriftType.ADDED_DEFAULT,
                    table=table,
                    column_name=col_name,
                    target_type=existing_col.data_type if existing_col else "TEXT",
                    new_type=existing_col.data_type if existing_col else "TEXT",
                    old_nullable=existing_col.nullable if existing_col else True,
                    new_nullable=existing_col.nullable if existing_col else True,
                    old_default=None,
                    new_default=def_val,
                    requires_backfill=True,
                    details=f"SET DEFAULT parsed for {col_name}",
                )
            )

        return table, drifts

    def generate_statements(self, table: str, drifts: List[DriftAction], target_dialect: Optional[str] = None) -> List[str]:
        dialect = (target_dialect or self.target_dialect).lower()
        cached_tbl = self.schema_cache.get_table(table)
        return generate_alter_statements(table, drifts, target_dialect=dialect, table_schema=cached_tbl)

    def execute_barrier(
        self,
        table: str,
        drifts: List[DriftAction],
        fence: Optional[TransactionalFence] = None,
        conn: Optional[Any] = None,
        target_executor: Optional[Callable[[str], Any]] = None,
    ) -> DriftResolutionResult:
        if not drifts:
            res = DriftResolutionResult(
                barrier_id=f"barrier_{uuid.uuid4().hex[:12]}",
                table=table,
                status="NO_DRIFT",
                drifts=[],
                generated_statements=[],
                invalidated_slices=[],
                backfill_tasks=[],
                duration_seconds=0.0,
            )
            return res

        start_time = time.time()
        barrier_id = f"barrier_{uuid.uuid4().hex[:12]}"
        stmts = self.generate_statements(table, drifts)

        self.applier.pause()

        active_fence = fence or TransactionalFence(
            target_executor=target_executor or self.target_executor,
            conn=conn or self.conn,
        )

        invalidated: List[str] = []
        backfill_tasks: List[BackfillTask] = []
        error_msg: Optional[str] = None
        status = "RESOLVED"

        try:
            with active_fence:
                for stmt in stmts:
                    active_fence.execute(stmt)

            self._update_schema_cache(table, drifts)

            drift_labels = ",".join(d.drift_type.value for d in drifts)
            invalidated = self.slice_manager.invalidate_table(
                table=table,
                key_range=None,
                reason=f"SCHEMA_DRIFT:{drift_labels}",
            )

            if self.auto_backfill and self.backfill_orchestrator:
                key_col = "id"
                cached = self.schema_cache.get_table(table)
                if cached:
                    pk_cols = [c.name for c in cached.columns.values() if getattr(c, "is_primary_key", False)]
                    if pk_cols:
                        key_col = pk_cols[0]
                chk_path = None
                if self.checkpoint_dir:
                    import os
                    os.makedirs(str(self.checkpoint_dir), exist_ok=True)
                for d in drifts:
                    if d.requires_backfill and d.new_default is not None:
                        if self.checkpoint_dir:
                            import os
                            chk_path = os.path.join(str(self.checkpoint_dir), f"{table}_{d.column_name}.backfill.json")
                        t = self.backfill_orchestrator.schedule_backfill(
                            table=table,
                            column_name=d.column_name,
                            default_value=d.new_default,
                            key_column=key_col,
                            batch_size=self.backfill_batch_size,
                            checkpoint_path=chk_path,
                        )
                        backfill_tasks.append(t)

        except Exception as exc:
            status = "FAILED"
            error_msg = str(exc)
            raise SchemaDriftError(f"Barrier execution failed on {table}: {exc}") from exc
        finally:
            self.applier.resume()

        duration = time.time() - start_time
        result = DriftResolutionResult(
            barrier_id=barrier_id,
            table=table,
            status=status,
            drifts=drifts,
            generated_statements=stmts,
            invalidated_slices=invalidated,
            backfill_tasks=backfill_tasks,
            duration_seconds=duration,
            error=error_msg,
        )
        self.resolution_history.append(result)
        return result

    def _update_schema_cache(self, table: str, drifts: List[DriftAction]) -> None:
        tbl = self.schema_cache.get_table(table)
        if tbl is None:
            tbl = self.schema_cache.add_table(table)

        for d in drifts:
            if d.drift_type == DriftType.NEW_COLUMN:
                tbl.columns[d.column_name] = ColumnSchema(
                    name=d.column_name,
                    data_type=d.new_type or "TEXT",
                    nullable=d.new_nullable if d.new_nullable is not None else True,
                    default=d.new_default,
                )
            elif d.drift_type == DriftType.TYPE_WIDENING:
                col = tbl.columns.get(d.column_name)
                if col:
                    if d.new_type:
                        col.data_type = d.new_type
                    if d.new_nullable is not None:
                        col.nullable = d.new_nullable
                    if d.new_default is not None:
                        col.default = d.new_default
                else:
                    tbl.columns[d.column_name] = ColumnSchema(
                        name=d.column_name,
                        data_type=d.new_type or "TEXT",
                        nullable=d.new_nullable if d.new_nullable is not None else True,
                        default=d.new_default,
                    )
            elif d.drift_type == DriftType.RELAXED_NULLABILITY:
                col = tbl.columns.get(d.column_name)
                if col:
                    col.nullable = True
            elif d.drift_type == DriftType.ADDED_DEFAULT:
                col = tbl.columns.get(d.column_name)
                if col:
                    col.default = d.new_default

    def resolve_event(self, event: Dict[str, Any], conn: Optional[Any] = None) -> Optional[DriftResolutionResult]:
        tbl, drifts = self.detect_drift_from_event(event)
        if tbl and drifts:
            return self.execute_barrier(tbl, drifts, conn=conn)
        return None

    def inspect_batch(
        self,
        eng: Any,
        db: str,
        changes: List[Dict[str, Any]],
        log: Optional[Callable[[str], None]] = None,
    ) -> List[DriftResolutionResult]:
        from migkit import decide
        results: List[DriftResolutionResult] = []
        if not changes:
            return results

        by_table: Dict[str, List[Dict[str, Any]]] = {}
        for c in changes:
            tbl = c.get("table")
            if tbl:
                by_table.setdefault(tbl, []).append(c)

        for tbl, tbl_changes in by_table.items():
            all_drifts: List[DriftAction] = []
            for ev in tbl_changes:
                t_name, drifts = self.detect_drift_from_event(ev)
                if drifts:
                    all_drifts.extend(drifts)

            if not all_drifts:
                continue

            seen_drifts: Set[Tuple[str, str, Any]] = set()
            unique_drifts: List[DriftAction] = []
            for d in all_drifts:
                dk = (d.table, d.column_name, d.drift_type)
                if dk not in seen_drifts:
                    seen_drifts.add(dk)
                    unique_drifts.append(d)

            req_backfill = any(d.requires_backfill for d in unique_drifts)
            incompatible = any(d.drift_type not in (
                DriftType.NEW_COLUMN, DriftType.TYPE_WIDENING, DriftType.RELAXED_NULLABILITY, DriftType.ADDED_DEFAULT
            ) for d in unique_drifts)

            has_key = True
            if hasattr(eng, "neutral_key"):
                try:
                    has_key = bool(eng.neutral_key("dst", db, tbl))
                except Exception:
                    has_key = True

            facts = decide.Facts({
                "has_drift": True,
                "all_inline_safe": not req_backfill and not incompatible,
                "requires_backfill": req_backfill,
                "has_key": has_key,
                "incompatible": incompatible,
            })

            rungs = schema_evolution_rungs()
            choice = decide.climb("schema_evolution", rungs, facts=facts)

            if choice.rung.name == "refuse_incompatible_schema_drift":
                why = choice.why(facts) if callable(choice.why) else choice.why
                raise SchemaDriftError(f"Incompatible schema drift on {tbl}: {why}")

            if log:
                log(f"Schema drift detected on {tbl}: executing online schema evolution via {choice.rung.name}")
            res = self.execute_barrier(tbl, unique_drifts)
            results.append(res)

        return results

    def process_cdc_stream(
        self,
        stream_events: Iterable[Dict[str, Any]],
        conn: Optional[Any] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        for event in stream_events:
            tbl, drifts = self.detect_drift_from_event(event)
            if tbl and drifts:
                barrier_res = self.execute_barrier(tbl, drifts, conn=conn or event.get("conn"))
                yield {
                    "type": "SCHEMA_DRIFT_BARRIER",
                    "barrier_id": barrier_res.barrier_id,
                    "status": barrier_res.status,
                    "table": barrier_res.table,
                    "drifts": [d.drift_type.value for d in barrier_res.drifts],
                    "statements": barrier_res.generated_statements,
                    "invalidated_slices": barrier_res.invalidated_slices,
                    "backfill_tasks": [t.task_id for t in barrier_res.backfill_tasks],
                }

            ev_type = (event.get("type") or event.get("op") or "").upper()
            if ev_type in ("INSERT", "UPDATE", "DELETE"):
                self.applier.apply_dml(event)
                yield event
            elif ev_type not in ("DDL", "SCHEMA", "SCHEMA_CHANGE"):
                yield event


def schema_evolution_rungs() -> Tuple[Any, ...]:
    from migkit import decide
    return (
        decide.Rung(
            "admit_inline_safe_evolution",
            "admit non-blocking online schema evolution directly without backfill",
            needs=(
                decide.Need(
                    "schema drift detected",
                    lambda f: bool(f.get("has_drift")),
                    "no schema drift detected",
                ),
                decide.Need(
                    "all drifts are non-blocking and safe for inline execution",
                    lambda f: bool(f.get("all_inline_safe", False)),
                    "drift requires backfill or contains incompatible operations",
                ),
                decide.Need(
                    "no unapproved incompatible drift",
                    lambda f: not bool(f.get("incompatible", False)),
                    "incompatible schema mutation detected",
                ),
            ),
        ),
        decide.Rung(
            "admit_evolution_with_batched_backfill",
            "admit schema evolution with asynchronous keyset-chunked backfill",
            needs=(
                decide.Need(
                    "schema drift detected",
                    lambda f: bool(f.get("has_drift")),
                    "no schema drift detected",
                ),
                decide.Need(
                    "drift requires default backfill",
                    lambda f: bool(f.get("requires_backfill", False)),
                    "no backfill needed",
                ),
                decide.Need(
                    "primary key or unique cursor column available for chunking",
                    lambda f: bool(f.get("has_key", True)),
                    "cannot perform keyset chunked backfill without key column",
                ),
                decide.Need(
                    "no unapproved incompatible drift",
                    lambda f: not bool(f.get("incompatible", False)),
                    "incompatible schema mutation detected",
                ),
            ),
        ),
        decide.Rung(
            "refuse_incompatible_schema_drift",
            "refuse incompatible or destructive schema drift",
            needs=(),
        ),
    )


SchemaDriftController = SchemaDriftResolver


__all__ = [
    "SchemaDriftError",
    "DriftType",
    "DriftAction",
    "BackfillTask",
    "DriftResolutionResult",
    "BackfillOrchestrator",
    "SchemaDriftResolver",
    "SchemaDriftController",
    "schema_evolution_rungs",
    "check_type_widening",
    "map_type_to_dialect",
    "generate_alter_statements",
]
