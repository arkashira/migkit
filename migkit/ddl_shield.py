import dataclasses
import re
import threading
import time
import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Tuple, Union

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .merkle import VerificationSlice, VerificationSliceManager  # noqa: F401


class DDLShieldError(Exception):
    """Base exception for online DDL shield errors."""


class DDLTranslationError(DDLShieldError):
    """Raised when DDL statement cannot be translated across dialect boundaries."""


class DDLBarrierError(DDLShieldError):
    """Raised when transactional DDL barrier execution fails."""


@dataclasses.dataclass
class ShadowTableInfo:
    tool: str
    role: str
    base_table: str
    shadow_table: str
    is_cutover: bool = False
    old_table: Optional[str] = None


@dataclasses.dataclass
class ActionInfo:
    action_type: str
    column_name: Optional[str] = None
    new_column_name: Optional[str] = None
    data_type: Optional[str] = None
    nullable: Optional[bool] = None
    default: Optional[str] = None
    is_identity: bool = False
    is_primary_key: bool = False
    position: Optional[str] = None


@dataclasses.dataclass
class DDLEvent:
    raw_sql: str
    schema: Optional[str] = None
    table: Optional[str] = None
    event_type: str = "ALTER"
    source_dialect: str = "mysql"
    gtid: Optional[str] = None
    lsn: Optional[int] = None
    timestamp: float = dataclasses.field(default_factory=time.time)
    is_shadow: bool = False
    shadow_info: Optional[ShadowTableInfo] = None
    actions: List[ActionInfo] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class TranslatedDDL:
    original_sql: str
    source_dialect: str
    target_dialect: str
    target_table: str
    target_sql_statements: List[str]
    affected_columns: List[str]
    mutation_type: str
    actions: List[ActionInfo] = dataclasses.field(default_factory=list)
    is_shadow: bool = False
    shadow_info: Optional[ShadowTableInfo] = None
    requires_barrier: bool = True


@dataclasses.dataclass
class BarrierResult:
    barrier_id: str
    status: str
    affected_table: str
    translated_statements: List[str]
    invalidated_slices: List[str]
    duration_seconds: float
    paused_at: float
    resumed_at: float
    error: Optional[str] = None


@dataclasses.dataclass
class ColumnSchema:
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[str] = None
    is_primary_key: bool = False
    is_identity: bool = False


@dataclasses.dataclass
class TableSchema:
    name: str
    columns: Dict[str, ColumnSchema] = dataclasses.field(default_factory=dict)
    primary_key: List[str] = dataclasses.field(default_factory=list)
    indexes: Dict[str, List[str]] = dataclasses.field(default_factory=dict)


class SchemaCache:
    def __init__(self):
        self.tables: Dict[str, TableSchema] = {}

    def add_table(
        self,
        name: str,
        columns: Optional[List[ColumnSchema]] = None,
        primary_key: Optional[List[str]] = None,
    ) -> TableSchema:
        tbl = TableSchema(name=name)
        if columns:
            for c in columns:
                tbl.columns[c.name] = c
        if primary_key:
            tbl.primary_key = list(primary_key)
        self.tables[name] = tbl
        return tbl

    def get_table(self, name: str) -> Optional[TableSchema]:
        return self.tables.get(name)

    def has_table(self, name: str) -> bool:
        return name in self.tables

    def drop_table(self, name: str) -> None:
        self.tables.pop(name, None)

    def rename_table(self, old_name: str, new_name: str) -> None:
        tbl = self.tables.pop(old_name, None)
        if tbl:
            tbl.name = new_name
            self.tables[new_name] = tbl

    def apply_ddl(self, translated: TranslatedDDL) -> None:
        table_name = translated.target_table
        if not table_name:
            return

        for action in translated.actions:
            if action.action_type == "CREATE_TABLE":
                tbl = self.get_table(table_name) or self.add_table(table_name)
                if action.column_name and action.data_type:
                    tbl.columns[action.column_name] = ColumnSchema(
                        name=action.column_name,
                        data_type=action.data_type,
                        nullable=action.nullable if action.nullable is not None else True,
                        default=action.default,
                        is_primary_key=action.is_primary_key,
                        is_identity=action.is_identity,
                    )
            elif action.action_type == "DROP_TABLE":
                self.drop_table(table_name)
            elif action.action_type == "RENAME_TABLE":
                new_tbl = action.new_column_name or action.column_name
                if new_tbl:
                    self.rename_table(table_name, new_tbl)
            elif action.action_type == "ADD_COLUMN":
                tbl = self.get_table(table_name) or self.add_table(table_name)
                if action.column_name:
                    tbl.columns[action.column_name] = ColumnSchema(
                        name=action.column_name,
                        data_type=action.data_type or "TEXT",
                        nullable=action.nullable if action.nullable is not None else True,
                        default=action.default,
                        is_primary_key=action.is_primary_key,
                        is_identity=action.is_identity,
                    )
            elif action.action_type == "DROP_COLUMN":
                tbl = self.get_table(table_name)
                if tbl and action.column_name:
                    tbl.columns.pop(action.column_name, None)
            elif action.action_type == "RENAME_COLUMN":
                tbl = self.get_table(table_name)
                if tbl and action.column_name and action.new_column_name:
                    col = tbl.columns.pop(action.column_name, None)
                    if col:
                        col.name = action.new_column_name
                        tbl.columns[action.new_column_name] = col
            elif action.action_type in ("MODIFY_COLUMN", "ALTER_COLUMN"):
                tbl = self.get_table(table_name) or self.add_table(table_name)
                col_name = action.column_name
                if col_name:
                    existing = tbl.columns.get(col_name)
                    if existing:
                        if action.data_type:
                            existing.data_type = action.data_type
                        if action.nullable is not None:
                            existing.nullable = action.nullable
                        if action.default is not None:
                            existing.default = action.default
                        if action.is_identity:
                            existing.is_identity = True
                    else:
                        tbl.columns[col_name] = ColumnSchema(
                            name=col_name,
                            data_type=action.data_type or "TEXT",
                            nullable=action.nullable if action.nullable is not None else True,
                            default=action.default,
                            is_primary_key=action.is_primary_key,
                            is_identity=action.is_identity,
                        )

    def clone(self) -> "SchemaCache":
        import copy
        new_cache = SchemaCache()
        new_cache.tables = copy.deepcopy(self.tables)
        return new_cache


class TransactionalFence:
    def __init__(
        self,
        target_executor: Optional[Callable[[str], Any]] = None,
        conn: Optional[Any] = None,
        dialect: str = "postgres",
        lock_timeout_s: int = 2,
        statement_timeout_s: int = 5,
    ):
        self.target_executor = target_executor
        self.conn = conn
        self.dialect = dialect.lower()
        self.lock_timeout_s = int(lock_timeout_s)
        self.statement_timeout_s = int(statement_timeout_s)
        self.in_transaction = False
        self.executed_statements: List[str] = []

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
            return False
        self.commit()
        return True

    def begin(self):
        self.in_transaction = True
        self.executed_statements.clear()
        if self.conn is not None:
            if hasattr(self.conn, "autocommit"):
                self.conn.autocommit = False
        if self.target_executor is not None:
            self.target_executor("BEGIN")
        for stmt in self._guards():
            if self.conn is not None:
                self.conn.cursor().execute(stmt)
            elif self.target_executor is not None:
                self.target_executor(stmt)

    def _guards(self) -> List[str]:
        if self.dialect in ("mysql", "mariadb"):
            return [f"SET SESSION lock_wait_timeout = {self.lock_timeout_s}"]
        return [
            f"SET LOCAL lock_timeout = '{self.lock_timeout_s}s'",
            f"SET LOCAL statement_timeout = '{self.statement_timeout_s}s'",
        ]

    def execute(self, stmt: str):
        if not self.in_transaction:
            raise RuntimeError("TransactionalFence must be begun before execute()")
        self.executed_statements.append(stmt)
        if self.conn is not None:
            cur = self.conn.cursor()
            cur.execute(stmt)
        elif self.target_executor is not None:
            self.target_executor(stmt)

    def commit(self):
        if not self.in_transaction:
            return
        if self.conn is not None:
            self.conn.commit()
        elif self.target_executor is not None:
            self.target_executor("COMMIT")
        self.in_transaction = False

    def rollback(self):
        if not self.in_transaction:
            return
        if self.conn is not None:
            self.conn.rollback()
        elif self.target_executor is not None:
            self.target_executor("ROLLBACK")
        self.in_transaction = False


class CDCReplicationApplier:
    def __init__(self, downstream: Optional[Callable[[Any], Any]] = None, drain_timeout_s: float = 30.0):
        self._cv = threading.Condition()
        self.is_paused = False
        self.pause_count = 0
        self.resume_count = 0
        self.in_flight = 0
        self.drain_timeout_s = drain_timeout_s
        self.downstream = downstream
        self.applied_events: List[Any] = []

    def pause(self) -> None:
        with self._cv:
            self.is_paused = True
            self.pause_count += 1
            if not self._cv.wait_for(lambda: self.in_flight == 0, timeout=self.drain_timeout_s):
                self.is_paused = False
                self.resume_count += 1
                self._cv.notify_all()
                raise DDLBarrierError("in-flight DML did not drain before the DDL barrier")

    def resume(self) -> None:
        with self._cv:
            self.is_paused = False
            self.resume_count += 1
            self._cv.notify_all()

    def apply_dml(self, event: Any) -> None:
        with self._cv:
            if self.is_paused:
                raise RuntimeError("Cannot apply DML: CDC replication applier is paused by DDL barrier")
            self.in_flight += 1
        try:
            if self.downstream is not None:
                self.downstream(event)
            self.applied_events.append(event)
        finally:
            with self._cv:
                self.in_flight -= 1
                self._cv.notify_all()


SHADOW_PATTERNS = [
    (re.compile(r"^_(.+?)_gho$", re.I), "gh-ost", "ghost"),
    (re.compile(r"^_(.+?)_ghc$", re.I), "gh-ost", "changelog"),
    (re.compile(r"^_(.+?)_(?:\d+_)?del$", re.I), "gh-ost", "tombstone"),
    (re.compile(r"^_gho$", re.I), "gh-ost", "ghost"),
    (re.compile(r"^_ghc$", re.I), "gh-ost", "changelog"),
    (re.compile(r"^_del$", re.I), "gh-ost", "tombstone"),
    (re.compile(r"^_{1,2}(.+?)_new$", re.I), "pt-osc", "new"),
    (re.compile(r"^_{1,2}(.+?)_old$", re.I), "pt-osc", "old"),
    (re.compile(r"^_{1,2}new$", re.I), "pt-osc", "new"),
    (re.compile(r"^_{1,2}old$", re.I), "pt-osc", "old"),
]


def parse_shadow_table(table_name: str) -> Optional[ShadowTableInfo]:
    clean = table_name.strip("`\"'[]")
    for pat, tool, role in SHADOW_PATTERNS:
        m = pat.match(clean)
        if m:
            base = m.group(1) if m.groups() else ""
            return ShadowTableInfo(
                tool=tool,
                role=role,
                base_table=base or clean,
                shadow_table=clean,
                is_cutover=False,
            )
    return None


MYSQL_TO_POSTGRES_TYPES = {
    "tinyint(1)": "BOOLEAN",
    "tinyint": "SMALLINT",
    "smallint": "SMALLINT",
    "mediumint": "INTEGER",
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "int unsigned": "BIGINT",
    "integer unsigned": "BIGINT",
    "bigint unsigned": "NUMERIC(20, 0)",
    "float": "REAL",
    "double": "DOUBLE PRECISION",
    "datetime": "TIMESTAMP WITHOUT TIME ZONE",
    "timestamp": "TIMESTAMP WITH TIME ZONE",
    "mediumtext": "TEXT",
    "longtext": "TEXT",
    "tinyblob": "BYTEA",
    "blob": "BYTEA",
    "mediumblob": "BYTEA",
    "longblob": "BYTEA",
    "json": "JSONB",
}

POSTGRES_TO_MYSQL_TYPES = {
    "boolean": "TINYINT(1)",
    "smallint": "SMALLINT",
    "integer": "INT",
    "int": "INT",
    "bigint": "BIGINT",
    "real": "FLOAT",
    "double precision": "DOUBLE",
    "timestamp without time zone": "DATETIME",
    "timestamp with time zone": "TIMESTAMP",
    "timestamp": "DATETIME",
    "timestamptz": "TIMESTAMP",
    "text": "LONGTEXT",
    "bytea": "LONGBLOB",
    "jsonb": "JSON",
    "json": "JSON",
}


def _clauses(text: str) -> List[str]:
    out, cur, quote, depth = [], [], None, 0
    for ch in text:
        if quote:
            if ch == quote:
                quote = None
        elif ch in "`'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    out.append("".join(cur))
    return [p for p in out if p.strip()]


class DDLTranslator:
    def __init__(self, source_dialect: str = "mysql", target_dialect: str = "postgres"):
        self.source_dialect = source_dialect.lower()
        self.target_dialect = target_dialect.lower()

    def map_type(self, type_str: str, source_dialect: str, target_dialect: str) -> str:
        s_lower = type_str.lower().strip()
        if source_dialect == "mysql" and target_dialect == "postgres":
            if s_lower in MYSQL_TO_POSTGRES_TYPES:
                return MYSQL_TO_POSTGRES_TYPES[s_lower]
            if s_lower.startswith("tinyint(1)"):
                return "BOOLEAN"
            if s_lower.startswith("tinyint"):
                return "SMALLINT"
            if "unsigned" in s_lower:
                if "bigint" in s_lower:
                    return "NUMERIC(20, 0)"
                if "int" in s_lower:
                    return "BIGINT"
                if "smallint" in s_lower:
                    return "INTEGER"
                if "tinyint" in s_lower:
                    return "SMALLINT"
            return type_str
        elif source_dialect == "postgres" and target_dialect == "mysql":
            if s_lower in POSTGRES_TO_MYSQL_TYPES:
                return POSTGRES_TO_MYSQL_TYPES[s_lower]
            if s_lower.startswith("serial"):
                return "INT AUTO_INCREMENT"
            if s_lower.startswith("bigserial"):
                return "BIGINT AUTO_INCREMENT"
            return type_str
        return type_str

    def translate_mysql_to_postgres(
        self,
        sql: str,
        target_table_override: Optional[str] = None,
    ) -> Tuple[str, List[str], List[ActionInfo]]:
        clean_sql = sql.strip().rstrip(";")

        rename_match = re.match(r"^RENAME\s+TABLE\s+(.+)$", clean_sql, re.I | re.S)
        if rename_match:
            pairs = _clauses(rename_match.group(1))
            statements = []
            actions = []
            table_name = None
            for pair in pairs:
                m = re.match(
                    r"^\s*(?:`?(\w+)`?\.)?`?(\w+)`?\s+TO\s+(?:`?(\w+)`?\.)?`?(\w+)`?\s*$",
                    pair,
                    re.I,
                )
                if m:
                    _, src_tbl, _, dst_tbl = m.groups()
                    statements.append(f'ALTER TABLE "{src_tbl}" RENAME TO "{dst_tbl}"')
                    actions.append(
                        ActionInfo(
                            action_type="RENAME_TABLE",
                            column_name=src_tbl,
                            new_column_name=dst_tbl,
                        )
                    )
                    if not table_name:
                        table_name = dst_tbl
            return target_table_override or table_name or "unknown", statements, actions

        alter_match = re.match(
            r"^\s*ALTER\s+TABLE\s+(?:`?(\w+)`?\.)?`?(\w+)`?\s+(.+)$",
            clean_sql,
            re.I | re.S,
        )
        if alter_match:
            _, tbl_name, rest = alter_match.groups()
            effective_table = target_table_override or tbl_name
            sub_clauses = _clauses(rest)
            statements = []
            actions = []

            for sc in sub_clauses:
                c_str = sc.strip()
                mod_m = re.match(
                    r"^MODIFY\s+(?:COLUMN\s+)?`?(\w+)`?\s+([^\s,]+(?:\s*\([^)]+\))?)\s*(.*)$",
                    c_str,
                    re.I | re.S,
                )
                if mod_m:
                    col_name, raw_type, extra = mod_m.groups()
                    extra_clean = " ".join(extra.split()).lower()
                    pg_type = self.map_type(raw_type, "mysql", "postgres")

                    is_ident = "auto_increment" in extra_clean

                    statements.append(
                        f'ALTER TABLE "{effective_table}" ALTER COLUMN "{col_name}" TYPE {pg_type} USING "{col_name}"::{pg_type}'
                    )

                    has_not_null = "not null" in extra_clean
                    has_null = "null" in extra_clean and not has_not_null
                    nullable = False if has_not_null else (True if has_null else None)

                    if has_not_null:
                        statements.append(
                            f'ALTER TABLE "{effective_table}" ALTER COLUMN "{col_name}" SET NOT NULL'
                        )
                    elif has_null:
                        statements.append(
                            f'ALTER TABLE "{effective_table}" ALTER COLUMN "{col_name}" DROP NOT NULL'
                        )

                    default_val = None
                    def_m = re.search(r"\bdefault\s+([^\s,]+)", extra, re.I)
                    if def_m:
                        default_val = def_m.group(1).rstrip(";")
                        if pg_type == "BOOLEAN":
                            if default_val in ("1", "'1'"):
                                default_val = "TRUE"
                            elif default_val in ("0", "'0'"):
                                default_val = "FALSE"
                        statements.append(
                            f'ALTER TABLE "{effective_table}" ALTER COLUMN "{col_name}" SET DEFAULT {default_val}'
                        )
                    elif "drop default" in extra_clean:
                        statements.append(
                            f'ALTER TABLE "{effective_table}" ALTER COLUMN "{col_name}" DROP DEFAULT'
                        )

                    if is_ident:
                        statements.append(
                            f'ALTER TABLE "{effective_table}" ALTER COLUMN "{col_name}" ADD GENERATED BY DEFAULT AS IDENTITY'
                        )

                    actions.append(
                        ActionInfo(
                            action_type="MODIFY_COLUMN",
                            column_name=col_name,
                            data_type=pg_type,
                            nullable=nullable,
                            default=default_val,
                            is_identity=is_ident,
                        )
                    )
                    continue

                chg_m = re.match(
                    r"^CHANGE\s+(?:COLUMN\s+)?`?(\w+)`?\s+`?(\w+)`?\s+([^\s,]+(?:\s*\([^)]+\))?)\s*(.*)$",
                    c_str,
                    re.I | re.S,
                )
                if chg_m:
                    old_col, new_col, raw_type, extra = chg_m.groups()
                    extra_clean = " ".join(extra.split()).lower()
                    pg_type = self.map_type(raw_type, "mysql", "postgres")

                    if old_col.lower() != new_col.lower():
                        statements.append(
                            f'ALTER TABLE "{effective_table}" RENAME COLUMN "{old_col}" TO "{new_col}"'
                        )
                        actions.append(
                            ActionInfo(
                                action_type="RENAME_COLUMN",
                                column_name=old_col,
                                new_column_name=new_col,
                            )
                        )

                    statements.append(
                        f'ALTER TABLE "{effective_table}" ALTER COLUMN "{new_col}" TYPE {pg_type} USING "{new_col}"::{pg_type}'
                    )

                    has_not_null = "not null" in extra_clean
                    has_null = "null" in extra_clean and not has_not_null
                    nullable = False if has_not_null else (True if has_null else None)

                    if has_not_null:
                        statements.append(
                            f'ALTER TABLE "{effective_table}" ALTER COLUMN "{new_col}" SET NOT NULL'
                        )
                    elif has_null:
                        statements.append(
                            f'ALTER TABLE "{effective_table}" ALTER COLUMN "{new_col}" DROP NOT NULL'
                        )

                    default_val = None
                    def_m = re.search(r"\bdefault\s+([^\s,]+)", extra, re.I)
                    if def_m:
                        default_val = def_m.group(1).rstrip(";")
                        statements.append(
                            f'ALTER TABLE "{effective_table}" ALTER COLUMN "{new_col}" SET DEFAULT {default_val}'
                        )

                    actions.append(
                        ActionInfo(
                            action_type="ALTER_COLUMN",
                            column_name=new_col,
                            data_type=pg_type,
                            nullable=nullable,
                            default=default_val,
                        )
                    )
                    continue

                add_m = re.match(
                    r"^ADD\s+(?:COLUMN\s+)?`?(\w+)`?\s+([^\s,]+(?:\s*\([^)]+\))?)\s*(.*)$",
                    c_str,
                    re.I | re.S,
                )
                if add_m and not re.match(
                    r"^ADD\s+(?:CONSTRAINT|INDEX|KEY|PRIMARY|UNIQUE|FOREIGN)\b",
                    c_str,
                    re.I,
                ):
                    col_name, raw_type, extra = add_m.groups()
                    extra_stripped = re.sub(r"\s+AFTER\s+`?\w+`?", "", extra, flags=re.I)
                    extra_stripped = re.sub(r"\s+FIRST\b", "", extra_stripped, flags=re.I)

                    is_auto = "auto_increment" in extra_stripped.lower()
                    extra_stripped = re.sub(
                        r"\bauto_increment\b", "", extra_stripped, flags=re.I
                    ).strip()

                    pg_type = self.map_type(raw_type, "mysql", "postgres")
                    extra_clean = " ".join(extra_stripped.split())

                    has_not_null = "not null" in extra_clean.lower()
                    has_pk = "primary key" in extra_clean.lower()

                    if is_auto:
                        if pg_type.upper() in ("BIGINT", "INT", "INTEGER"):
                            col_def = f'"{col_name}" {pg_type} GENERATED BY DEFAULT AS IDENTITY'
                            if has_not_null and "not null" not in col_def.lower():
                                col_def += " NOT NULL"
                            if has_pk:
                                col_def += " PRIMARY KEY"
                            extra_clean = re.sub(r"\bprimary\s+key\b", "", extra_clean, flags=re.I)
                            extra_clean = re.sub(r"\bnot\s+null\b", "", extra_clean, flags=re.I).strip()
                            if extra_clean:
                                col_def += f" {extra_clean}"
                        else:
                            col_def = f'"{col_name}" {pg_type} {extra_clean}'.strip()
                    else:
                        col_def = f'"{col_name}" {pg_type}'
                        if extra_clean:
                            col_def += f" {extra_clean}"

                    statements.append(f'ALTER TABLE "{effective_table}" ADD COLUMN {col_def}')
                    actions.append(
                        ActionInfo(
                            action_type="ADD_COLUMN",
                            column_name=col_name,
                            data_type=pg_type,
                            nullable=not has_not_null,
                            is_identity=is_auto,
                            is_primary_key=has_pk,
                        )
                    )
                    continue

                drop_m = re.match(r"^DROP\s+(?:COLUMN\s+)?`?(\w+)`?\s*$", c_str, re.I)
                if drop_m:
                    col_name = drop_m.group(1)
                    statements.append(f'ALTER TABLE "{effective_table}" DROP COLUMN "{col_name}"')
                    actions.append(
                        ActionInfo(
                            action_type="DROP_COLUMN",
                            column_name=col_name,
                        )
                    )
                    continue

                ren_m = re.match(r"^RENAME\s+(?:TO\s+|AS\s+)?`?(\w+)`?\s*$", c_str, re.I)
                if ren_m:
                    new_table = ren_m.group(1)
                    statements.append(f'ALTER TABLE "{effective_table}" RENAME TO "{new_table}"')
                    actions.append(
                        ActionInfo(
                            action_type="RENAME_TABLE",
                            column_name=effective_table,
                            new_column_name=new_table,
                        )
                    )
                    continue

                single_sql = f"ALTER TABLE `{effective_table}` {c_str}"
                try:
                    transpiled = sqlglot.transpile(
                        single_sql, read="mysql", write="postgres"
                    )
                    for t in transpiled:
                        statements.append(t.rstrip(";"))
                except Exception:
                    statements.append(single_sql)

            return effective_table, statements, actions

        create_m = re.match(
            r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?\s*\((.+)\)(.*)$",
            clean_sql,
            re.I | re.S,
        )
        if create_m:
            _, tbl_name, body, opts = create_m.groups()
            effective_table = target_table_override or tbl_name
            try:
                parsed = sqlglot.parse_one(clean_sql, read="mysql")
                if parsed:
                    parsed.set("properties", None)
                    if target_table_override:
                        parsed.this.this.set("this", target_table_override)
                    pg_sql = parsed.sql(dialect="postgres")
                    actions = [ActionInfo(action_type="CREATE_TABLE")]
                    return effective_table, [pg_sql], actions
            except Exception:
                pass

        drop_tbl_m = re.match(
            r"^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?\s*$",
            clean_sql,
            re.I,
        )
        if drop_tbl_m:
            _, tbl_name = drop_tbl_m.groups()
            effective_table = target_table_override or tbl_name
            return (
                effective_table,
                [f'DROP TABLE IF EXISTS "{effective_table}"'],
                [ActionInfo(action_type="DROP_TABLE", column_name=effective_table)],
            )

        trunc_m = re.match(
            r"^\s*TRUNCATE\s+(?:TABLE\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?\s*$",
            clean_sql,
            re.I,
        )
        if trunc_m:
            _, tbl_name = trunc_m.groups()
            effective_table = target_table_override or tbl_name
            return (
                effective_table,
                [f'TRUNCATE TABLE "{effective_table}"'],
                [ActionInfo(action_type="TRUNCATE", column_name=effective_table)],
            )

        try:
            transpiled = sqlglot.transpile(clean_sql, read="mysql", write="postgres")
            return "unknown", [s.rstrip(";") for s in transpiled], []
        except Exception as e:
            raise DDLTranslationError(f"Failed to translate MySQL DDL to Postgres: {e}") from e

    def translate_postgres_to_mysql(
        self,
        sql: str,
        target_table_override: Optional[str] = None,
    ) -> Tuple[str, List[str], List[ActionInfo]]:
        clean_sql = sql.strip().rstrip(";")
        alter_match = re.match(
            r"^\s*ALTER\s+TABLE\s+(?:\"?(\w+)\"?\.)?\"?(\w+)\"?\s+(.+)$",
            clean_sql,
            re.I | re.S,
        )
        if alter_match:
            _, tbl_name, rest = alter_match.groups()
            effective_table = target_table_override or tbl_name
            sub_clauses = _clauses(rest)
            statements = []
            actions = []

            for sc in sub_clauses:
                c_str = sc.strip()
                type_m = re.match(
                    r"^ALTER\s+(?:COLUMN\s+)?\"?(\w+)\"?\s+TYPE\s+([^\s,]+(?:\s*\([^)]+\))?)(?:\s+USING\s+.+)?$",
                    c_str,
                    re.I | re.S,
                )
                if type_m:
                    col_name, raw_type = type_m.groups()
                    my_type = self.map_type(raw_type, "postgres", "mysql")
                    statements.append(f"ALTER TABLE `{effective_table}` MODIFY COLUMN `{col_name}` {my_type}")
                    actions.append(
                        ActionInfo(
                            action_type="MODIFY_COLUMN",
                            column_name=col_name,
                            data_type=my_type,
                        )
                    )
                    continue

                not_null_m = re.match(
                    r"^ALTER\s+(?:COLUMN\s+)?\"?(\w+)\"?\s+SET\s+NOT\s+NULL$",
                    c_str,
                    re.I,
                )
                if not_null_m:
                    col_name = not_null_m.group(1)
                    statements.append(f"ALTER TABLE `{effective_table}` MODIFY COLUMN `{col_name}` TEXT NOT NULL")
                    actions.append(
                        ActionInfo(
                            action_type="MODIFY_COLUMN",
                            column_name=col_name,
                            nullable=False,
                        )
                    )
                    continue

                single_sql = f'ALTER TABLE "{effective_table}" {c_str}'
                try:
                    transpiled = sqlglot.transpile(single_sql, read="postgres", write="mysql")
                    for t in transpiled:
                        statements.append(t.rstrip(";"))
                except Exception:
                    statements.append(single_sql)

            return effective_table, statements, actions

        try:
            transpiled = sqlglot.transpile(clean_sql, read="postgres", write="mysql")
            return "unknown", [s.rstrip(";") for s in transpiled], []
        except Exception as e:
            raise DDLTranslationError(f"Failed to translate Postgres DDL to MySQL: {e}") from e

    def translate(
        self,
        sql: str,
        target_table_override: Optional[str] = None,
    ) -> Tuple[str, List[str], List[ActionInfo]]:
        if self.source_dialect == "mysql" and self.target_dialect in ("postgres", "postgresql"):
            return self.translate_mysql_to_postgres(sql, target_table_override=target_table_override)
        elif self.source_dialect in ("postgres", "postgresql") and self.target_dialect == "mysql":
            return self.translate_postgres_to_mysql(sql, target_table_override=target_table_override)
        else:
            try:
                transpiled = sqlglot.transpile(
                    sql, read=self.source_dialect, write=self.target_dialect
                )
                return target_table_override or "unknown", [s.rstrip(";") for s in transpiled], []
            except Exception as e:
                raise DDLTranslationError(
                    f"Unsupported dialect translation {self.source_dialect} -> {self.target_dialect}: {e}"
                ) from e


class OnlineDDLShield:
    def __init__(
        self,
        source_dialect: str = "mysql",
        target_dialect: str = "postgres",
        applier: Optional[CDCReplicationApplier] = None,
        target_executor: Optional[Callable[[str], Any]] = None,
        schema_cache: Optional[SchemaCache] = None,
        slice_manager: Optional[VerificationSliceManager] = None,
        shadow_mode: str = "intercept_and_map",
        suppress_auxiliary_shadow_tables: bool = True,
    ):
        self.source_dialect = source_dialect.lower()
        self.target_dialect = target_dialect.lower()
        self.applier = applier or CDCReplicationApplier()
        self.target_executor = target_executor
        self.schema_cache = schema_cache or SchemaCache()
        self.slice_manager = slice_manager or VerificationSliceManager()
        self.shadow_mode = shadow_mode
        self.suppress_auxiliary_shadow_tables = suppress_auxiliary_shadow_tables
        self.translator = DDLTranslator(
            source_dialect=self.source_dialect,
            target_dialect=self.target_dialect,
        )
        self.barrier_history: List[BarrierResult] = []

    def intercept_ddl(
        self,
        raw_sql: str,
        schema: Optional[str] = None,
        gtid: Optional[str] = None,
        lsn: Optional[int] = None,
    ) -> DDLEvent:
        clean = raw_sql.strip().rstrip(";")
        is_shadow = False
        shadow_info = None

        cutover_m = re.search(
            r"RENAME\s+TABLE\s+(?:`?\w+`?\.)?`?(\w+)`?\s+TO\s+(?:`?\w+`?\.)?`?(\w+)`?,\s*(?:`?\w+`?\.)?`?(\w+)`?\s+TO\s+(?:`?\w+`?\.)?`?(\w+)`?",
            clean,
            re.I,
        )
        if cutover_m:
            t1, t2, t3, t4 = cutover_m.groups()
            s2 = parse_shadow_table(t2)
            s3 = parse_shadow_table(t3)
            if s2 and s3 and t1.lower() == t4.lower():
                is_shadow = True
                shadow_info = ShadowTableInfo(
                    tool=s3.tool,
                    role="ghost" if s3.tool == "gh-ost" else "new",
                    base_table=t1,
                    shadow_table=t3,
                    is_cutover=True,
                    old_table=t2,
                )
                return DDLEvent(
                    raw_sql=raw_sql,
                    schema=schema,
                    table=t1,
                    event_type="SHADOW_CUTOVER",
                    source_dialect=self.source_dialect,
                    gtid=gtid,
                    lsn=lsn,
                    is_shadow=True,
                    shadow_info=shadow_info,
                )

        tbl_match = re.search(
            r"^\s*(?:ALTER\s+TABLE|CREATE\s+TABLE|DROP\s+TABLE|TRUNCATE\s+(?:TABLE\s+)?)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?",
            clean,
            re.I,
        )
        tbl_name = tbl_match.group(2) if tbl_match else None
        if tbl_name:
            s_info = parse_shadow_table(tbl_name)
            if s_info:
                is_shadow = True
                shadow_info = s_info

        ev_type = "ALTER"
        if re.match(r"^\s*CREATE\s+TABLE\b", clean, re.I):
            ev_type = "CREATE"
        elif re.match(r"^\s*DROP\s+TABLE\b", clean, re.I):
            ev_type = "DROP"
        elif re.match(r"^\s*TRUNCATE\b", clean, re.I):
            ev_type = "TRUNCATE"
        elif re.match(r"^\s*RENAME\s+TABLE\b", clean, re.I):
            ev_type = "RENAME"

        return DDLEvent(
            raw_sql=raw_sql,
            schema=schema,
            table=tbl_name,
            event_type=ev_type,
            source_dialect=self.source_dialect,
            gtid=gtid,
            lsn=lsn,
            is_shadow=is_shadow,
            shadow_info=shadow_info,
        )

    def translate(
        self,
        event_or_sql: Union[str, DDLEvent],
        target_table_override: Optional[str] = None,
    ) -> TranslatedDDL:
        if isinstance(event_or_sql, str):
            event = self.intercept_ddl(event_or_sql)
        else:
            event = event_or_sql

        effective_target = target_table_override
        if not effective_target and event.shadow_info and self.shadow_mode == "intercept_and_map":
            effective_target = event.shadow_info.base_table

        if event.shadow_info and event.shadow_info.is_cutover and self.shadow_mode == "intercept_and_map":
            tbl = effective_target or event.shadow_info.base_table
            return TranslatedDDL(
                original_sql=event.raw_sql,
                source_dialect=self.source_dialect,
                target_dialect=self.target_dialect,
                target_table=tbl,
                target_sql_statements=[],
                affected_columns=[],
                mutation_type="SHADOW_CUTOVER",
                actions=[ActionInfo(action_type="SHADOW_CUTOVER", column_name=tbl)],
                is_shadow=True,
                shadow_info=event.shadow_info,
                requires_barrier=True,
            )

        tbl, statements, actions = self.translator.translate(
            event.raw_sql, target_table_override=effective_target
        )

        affected_cols = [a.column_name for a in actions if a.column_name]
        mutation_type = actions[0].action_type if actions else event.event_type

        return TranslatedDDL(
            original_sql=event.raw_sql,
            source_dialect=self.source_dialect,
            target_dialect=self.target_dialect,
            target_table=tbl,
            target_sql_statements=statements,
            affected_columns=affected_cols,
            mutation_type=mutation_type,
            actions=actions,
            is_shadow=event.is_shadow,
            shadow_info=event.shadow_info,
            requires_barrier=True,
        )

    def apply_with_barrier(
        self,
        ddl: Union[str, DDLEvent, TranslatedDDL],
        fence: Optional[TransactionalFence] = None,
        target_executor: Optional[Callable[[str], Any]] = None,
        conn: Optional[Any] = None,
    ) -> BarrierResult:
        start_time = time.time()
        barrier_id = f"barrier_{uuid.uuid4().hex[:12]}"

        if isinstance(ddl, TranslatedDDL):
            translated = ddl
            event = None
        elif isinstance(ddl, DDLEvent):
            event = ddl
            translated = self.translate(event)
        else:
            event = self.intercept_ddl(ddl)
            translated = self.translate(event)

        if (
            event
            and event.is_shadow
            and event.shadow_info
            and self.suppress_auxiliary_shadow_tables
            and event.shadow_info.role in ("changelog", "tombstone")
        ):
            res = BarrierResult(
                barrier_id=barrier_id,
                status="SKIPPED",
                affected_table=translated.target_table,
                translated_statements=[],
                invalidated_slices=[],
                duration_seconds=time.time() - start_time,
                paused_at=start_time,
                resumed_at=time.time(),
            )
            self.barrier_history.append(res)
            return res

        paused_at = time.time()
        if self.applier:
            self.applier.pause()

        active_fence = fence or TransactionalFence(
            target_executor=target_executor or self.target_executor,
            conn=conn,
            dialect=self.target_dialect,
        )
        invalidated_slices: List[str] = []
        error_msg: Optional[str] = None
        status = "COMMITTED"

        try:
            with active_fence:
                for stmt in translated.target_sql_statements:
                    active_fence.execute(stmt)

            self.schema_cache.apply_ddl(translated)

            if self.slice_manager and translated.target_table:
                invalidated_slices = self.slice_manager.invalidate_table(
                    table=translated.target_table,
                    key_range=None,
                    reason=f"DDL_MUTATION:{translated.mutation_type}",
                )

        except Exception as exc:
            status = "ROLLED_BACK"
            error_msg = str(exc)
            raise DDLBarrierError(f"DDL barrier {barrier_id} rolled back: {exc}") from exc

        finally:
            resumed_at = time.time()
            if self.applier:
                self.applier.resume()

        result = BarrierResult(
            barrier_id=barrier_id,
            status=status,
            affected_table=translated.target_table,
            translated_statements=translated.target_sql_statements,
            invalidated_slices=invalidated_slices,
            duration_seconds=resumed_at - start_time,
            paused_at=paused_at,
            resumed_at=resumed_at,
            error=error_msg,
        )
        self.barrier_history.append(result)
        return result

    def process_cdc_stream(
        self,
        stream_events: Iterable[Dict[str, Any]],
    ) -> Generator[Dict[str, Any], None, None]:
        for ev in stream_events:
            ev_type = ev.get("type", "").upper()
            if ev_type == "DDL" or "sql" in ev:
                raw_sql = ev.get("sql", "")
                barrier_res = self.apply_with_barrier(
                    raw_sql,
                    conn=ev.get("conn"),
                )
                yield {
                    "type": "DDL_BARRIER",
                    "barrier_id": barrier_res.barrier_id,
                    "status": barrier_res.status,
                    "table": barrier_res.affected_table,
                    "translated_statements": barrier_res.translated_statements,
                    "invalidated_slices": barrier_res.invalidated_slices,
                }
            else:
                self.applier.apply_dml(ev)
                yield ev
